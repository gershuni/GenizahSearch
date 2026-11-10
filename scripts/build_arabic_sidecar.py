#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build ``arabic_data/arabic.db``, the Arabic dictionary sidecar (phase A: Lane).

Plan: ``docs/plans/ARABIC_DICTIONARY_LAYER_PLAN.md`` §7 phase A, with the owner's rulings of
2026-11-04 (§8): Lane first; credit and licence terms at the minimum each source requires;
Ephraim Meiri's parsed files may be used; the frequency count over every primary, non-OCR
OpenITI text of 250-650 AH, dictionaries excluded; a closed-class table.

Sources, fetched by ``fetch`` into an EMPTY directory of their own and never committed
(``arabic_data/`` is gitignored; the sidecar is a build artifact like every ``*_data/`` sidecar):

- ``emeiri/``: the manifest and the 1,024 ``data/entries/<n>.json`` shards of
  https://semitic.lang.emeiri.org (Ephraim Meiri's parse; its Lane is quranic-research's 2019
  restructuring of the laneslexicon/lexicon_xml master files, the Perseus TEI). Only the
  ``lane`` entries are read. ``pins.json`` records every file's sha256.
- ``openiti/``: the texts of OpenITI release 2025.1.9 that ``select_openiti`` names, from the
  release metadata table, each fetched from its repository at a recorded commit;
  ``manifest.json`` records commit, path and sha256 per text, and lists as ``unavailable`` the
  selected texts not found in their repository. The build recomputes the selection from
  ``metadata.tsv`` (sha256 pinned) and fails unless the manifest accounts for every selected
  text, exactly once, with at most ``MAX_UNAVAILABLE_SHARE`` unavailable.
- ``scripts/arabic_closed_class.json`` (in the repository): the closed-class table (D10).

    python scripts/build_arabic_sidecar.py fetch --dest arabic_data/download
    python scripts/build_arabic_sidecar.py build --source-dir arabic_data/download --out arabic_data/arabic.db
    python scripts/verify_arabic_sidecar.py --db arabic_data/arabic.db --expect-entries <n>

The downloads are untrusted data: read with the stdlib ``json`` module and as text only,
nothing in them is executed or used as a path. The build FAILS (exit 2) on a missing, empty or
malformed file, a sha256 that differs from the pins, a credit line that differs from the
Perseus one, a duplicate id, or a closed-class row that names a root or headword Lane lacks;
REFUSES (exit 3) to replace an existing sidecar that holds more Lane entries or frequency rows
unless ``--force``; and writes beside the target under a temporary name, renamed into place
only when complete (exit 4 on a failed write, the served file left as it was).

Tables (schema ``SCHEMA_VERSION``): ``meta`` (versions, sources and digests, credit lines, row
counts); ``lane_roots`` (one row per root key, with Lane's spellings); ``lane_entries`` (every
Lane article: headword, root key, printed page, text); ``lane_forms`` (the one-word headwords
by ``skeleton``: the form-to-root table of the plan's method v3); ``freq`` (skeleton, count,
commonest spelling); ``closed_class`` (prepositions with pronouns, demonstratives, particles,
pointing at Lane's articles). Keys come from ``shared/arabic_normalize.py``.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import datetime as _dt
import hashlib
import json
import math
import os
import re
import sqlite3
import subprocess
import sys
import time
import unicodedata
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPTS_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from shared.arabic_normalize import (  # noqa: E402
    NORMALIZER_VERSION,
    arabic_tokens,
    root_key,
    skeleton,
    strip_harakat,
)

SCHEMA_VERSION = "1.0.0"
DEFAULT_OUT = Path("arabic_data") / "arabic.db"
CLOSED_CLASS_FILE = Path(_REPO_ROOT) / "scripts" / "arabic_closed_class.json"

EMEIRI_SITE = "https://semitic.lang.emeiri.org/"
EMEIRI_SHARDS = 1024
LANE_SOURCE_NOTE = ("Ephraim Meiri's parse (semitic.lang.emeiri.org) of quranic-research/lexicon_xml (2019), "
                    "itself a restructuring of the laneslexicon/lexicon_xml master files: the Perseus Digital "
                    "Library's TEI of E. W. Lane, An Arabic-English Lexicon (London 1863-1893)")
#: The Perseus TEI header's availability statement and credit line, verbatim (plan §2.1). The
#: build checks the source's own copy against them; every Lane surface shows ``PERSEUS_CREDIT``.
PERSEUS_CREDIT = ("Text provided by Perseus Digital Library, with funding from The U.S. Department of "
                  "Education and The Max Planck Society.")
PERSEUS_AVAILABILITY = ("This text may be freely distributed, subject to the following restrictions: You "
                        "credit Perseus, as follows, whenever you use the document: " + PERSEUS_CREDIT
                        + " You leave this availability statement intact. You offer Perseus any "
                        "modifications you make.")
OPENITI_RELEASE = "2025.1.9"
OPENITI_DOI = "10.5281/zenodo.17767721"
OPENITI_METADATA_URL = "https://zenodo.org/records/17767721/files/OpenITI_metadata_2025-1-9.tsv?download=1"
OPENITI_METADATA_SHA256 = "10a025fcfc1bf96d218096f0dfe1d9416f9ff277f092c42622cb012cc4da2a61"
OPENITI_CREDIT = ("Word counts from the Open Islamicate Texts Initiative (OpenITI), release 2025.1.9, "
                  "doi:10.5281/zenodo.17767721, CC BY-NC-SA 4.0.")
#: Rows of the count kept: a reading is offered only at 2 or more (the plan's MIN_COUNT).
FREQ_MIN_STORED = 2
#: Texts D7 selects that the fetch did not find in their repository (3 of 2,819 on 2026-11-04) are
#: listed as unavailable in the manifest; more than this share and the count is not D7's.
MAX_UNAVAILABLE_SHARE = 0.005

EXIT_OK = 0
EXIT_SOURCE = 2
EXIT_REFUSED = 3
EXIT_OUTPUT = 4

DDL = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE lane_roots (
    root_key TEXT PRIMARY KEY, name TEXT NOT NULL, names_json TEXT NOT NULL, ordinal INTEGER NOT NULL,
    num_entries INTEGER NOT NULL, first_page INTEGER
);
CREATE TABLE lane_entries (
    id TEXT PRIMARY KEY, root_key TEXT NOT NULL REFERENCES lane_roots(root_key), root TEXT NOT NULL,
    ordinal INTEGER NOT NULL, headword TEXT NOT NULL, page INTEGER, pages TEXT NOT NULL, text TEXT NOT NULL
);
CREATE INDEX idx_lane_entries_root ON lane_entries(root_key, ordinal);
CREATE TABLE lane_forms (
    skeleton TEXT NOT NULL, root_key TEXT NOT NULL REFERENCES lane_roots(root_key),
    entry_id TEXT NOT NULL REFERENCES lane_entries(id), PRIMARY KEY (skeleton, entry_id)
);
CREATE INDEX idx_lane_forms_skeleton ON lane_forms(skeleton);
CREATE TABLE freq (skeleton TEXT PRIMARY KEY, count INTEGER NOT NULL, surface TEXT NOT NULL);
CREATE TABLE closed_class (
    skeleton TEXT NOT NULL, form TEXT NOT NULL, root_key TEXT NOT NULL REFERENCES lane_roots(root_key),
    entry_id TEXT REFERENCES lane_entries(id), note TEXT NOT NULL, ordinal INTEGER NOT NULL,
    PRIMARY KEY (skeleton, root_key)
);
"""
COUNTED_TABLES = ("lane_roots", "lane_entries", "lane_forms", "freq", "closed_class")


class SourceError(Exception):
    pass


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Lane (from the emeiri shards)
# ---------------------------------------------------------------------------

def _read_json(path: Path):
    if not path.is_file() or path.stat().st_size == 0:
        raise SourceError(f"missing or empty: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SourceError(f"not JSON: {path} ({exc})") from exc


def check_pins(emeiri_dir: Path) -> tuple[dict, str]:
    """Every pinned file present with its sha256; returns (pins, sha256 of pins.json)."""
    pins_path = emeiri_dir / "pins.json"
    pins = _read_json(pins_path)
    if not isinstance(pins, dict) or "manifest.json" not in pins:
        raise SourceError(f"{pins_path}: not a pins table")
    shards = [f"entries/{i}.json" for i in range(EMEIRI_SHARDS)]
    missing = [name for name in shards if name not in pins]
    if missing:
        raise SourceError(f"{pins_path}: {len(missing)} shards not pinned (first {missing[0]})")
    for name, pin in pins.items():
        path = emeiri_dir / name
        if not path.is_file():
            raise SourceError(f"missing: {path}")
        if sha256_of(path) != pin.get("sha256"):
            raise SourceError(f"sha256 differs from pins.json: {path}")
    return pins, sha256_of(pins_path)


def read_lane(emeiri_dir: Path) -> tuple[list[dict], dict]:
    """Every Lane entry of the shards, checked; and facts for ``meta``."""
    pins, pins_sha = check_pins(emeiri_dir)
    manifest = _read_json(emeiri_dir / "manifest.json")
    try:
        lane_source = manifest["sources"]["lane"]
        expected = int(manifest["counts"]["lane"])
        built_at = str(manifest["built_at"])
    except (KeyError, TypeError, ValueError) as exc:
        raise SourceError(f"manifest.json: no lane source, count or build time ({exc})") from exc
    if lane_source.get("credit") != PERSEUS_CREDIT:
        raise SourceError("manifest.json: the Lane credit line differs from the Perseus line")
    if " ".join(str(lane_source.get("statement", "")).split()) != PERSEUS_AVAILABILITY:
        raise SourceError("manifest.json: the Lane availability statement differs from the Perseus statement")
    entries: list[dict] = []
    seen: set[str] = set()
    for i in range(EMEIRI_SHARDS):
        shard = _read_json(emeiri_dir / "entries" / f"{i}.json")
        if not isinstance(shard, dict):
            raise SourceError(f"entries/{i}.json: not an object")
        for key, entry in shard.items():
            if not isinstance(entry, dict) or entry.get("source") != "lane":
                continue
            eid = entry.get("id")
            fields = (eid, entry.get("headword"), entry.get("gloss"))
            # The root may be null: the source leaves it unset on a few entries (35 in the
            # 2026-11-01 build); they are counted and pinned in meta, not guessed.
            if not all(isinstance(v, str) for v in fields) or eid != key \
                    or not isinstance(entry.get("root"), (str, type(None))):
                raise SourceError(f"entries/{i}.json: a Lane entry with a missing or odd field ({key!r})")
            if eid in seen:
                raise SourceError(f"duplicate Lane id {eid!r}")
            pages = entry.get("pages") or []
            doc = entry.get("_doc")
            if not isinstance(pages, list) or not all(isinstance(p, (str, int)) for p in pages):
                raise SourceError(f"{eid}: pages is not a list of page numbers")
            if not isinstance(doc, int):
                raise SourceError(f"{eid}: no document number")
            seen.add(eid)
            entries.append({"id": eid, "headword": entry["headword"], "root": entry["root"], "text": entry["gloss"],
                            "pages": [str(p) for p in pages], "ordinal": doc})
    if len(entries) != expected:
        raise SourceError(f"the shards hold {len(entries):,} Lane entries, the manifest says {expected:,}")
    entries.sort(key=lambda e: e["ordinal"])
    facts = {"lane_source": LANE_SOURCE_NOTE, "lane_source_url": EMEIRI_SITE, "lane_source_built_at": built_at,
             "lane_manifest_sha256": pins["manifest.json"]["sha256"], "lane_pins_sha256": pins_sha,
             "lane_credit": PERSEUS_CREDIT, "lane_availability": PERSEUS_AVAILABILITY,
             "lane_edition": str(lane_source.get("edition", ""))}
    return entries, facts


def _first_page(pages: list[str]) -> Optional[int]:
    for page in pages:
        digits = "".join(ch for ch in page if ch.isdigit())
        if digits:
            return int(digits)
    return None


#: Where a Lane root heading joins spellings: "سعو & سعى", "غو | غوى", "(دمو or دمى)".
_HEADING_SEPARATORS = re.compile(r"&|\||\bor\b|,|،")


def root_spellings(heading: str) -> list[str]:
    """The root spellings a Lane heading names, unvowelled, in the order written. A heading may
    join several ("سعو & سعى"), carry an English note in brackets, or split one word with a space
    ("سفر جل" is سفرجل): the note and the parentheses are dropped, the heading is split at
    ``_HEADING_SEPARATORS``, and the letters of each part are joined."""
    text = re.sub(r"\[[^\]]*\]", " ", strip_harakat(heading))
    out: list[str] = []
    for part in _HEADING_SEPARATORS.split(text):
        letters = "".join(re.findall("[ء-ي]+", part))
        if letters and letters not in out:
            out.append(letters)
    return out


def heading_root_key(heading: str) -> tuple[str, list[str]]:
    """(the key an entry is filed under, the heading's spellings that share it). The key is the
    first spelling's; "سعو & سعى" is one root (سعW), "ثند & ثندأ" names two and files under the
    first. A heading with no Arabic letter gives ("", [])."""
    spellings = root_spellings(heading)
    if not spellings:
        return "", []
    key = root_key(spellings[0])
    return key, [s for s in spellings if root_key(s) == key]


def lane_tables(entries: list[dict]) -> tuple[list[tuple], list[tuple], list[tuple], dict]:
    """(roots, entries, forms) rows and facts. An entry the source gives no root is left out and
    counted (``lane_entries_without_root``); a root with no Arabic letter is a source error. A
    heading that names spellings of two roots files under the first, the heading kept whole in
    ``lane_entries.root`` and counted (``lane_entries_heading_names_two_roots``)."""
    by_key: dict[str, dict] = {}
    entry_rows, form_rows = [], []
    multiword = rootless = two_roots = 0
    for e in entries:
        if e["root"] is None:
            rootless += 1
            continue
        key, names = heading_root_key(e["root"])
        if not key:
            raise SourceError(f"{e['id']}: the root {e['root']!r} has no Arabic letter")
        if len(names) < len(root_spellings(e["root"])):
            two_roots += 1
        page = _first_page(e["pages"])
        slot = by_key.setdefault(key, {"names": [], "ordinal": e["ordinal"], "n": 0, "page": page})
        for name in names:
            if name not in slot["names"]:
                slot["names"].append(name)
        slot["n"] += 1
        entry_rows.append((e["id"], key, e["root"], e["ordinal"], e["headword"], page, json.dumps(e["pages"]),
                           e["text"]))
        bare = strip_harakat(e["headword"]).strip()
        if not bare or len(bare.split()) != 1:
            multiword += 1
            continue
        form = skeleton(bare)
        if len(form) >= 2:
            form_rows.append((form, key, e["id"]))
    root_rows = [(k, v["names"][0], json.dumps(v["names"], ensure_ascii=False), v["ordinal"], v["n"], v["page"])
                 for k, v in sorted(by_key.items(), key=lambda kv: kv[1]["ordinal"])]
    form_rows = sorted(set(form_rows))
    return root_rows, entry_rows, form_rows, {"lane_headwords_not_one_word": str(multiword),
                                              "lane_entries_without_root": str(rootless),
                                              "lane_entries_heading_names_two_roots": str(two_roots),
                                              "lane_entries_in_source": str(len(entries))}


# ---------------------------------------------------------------------------
# The OpenITI count
# ---------------------------------------------------------------------------

def select_openiti(metadata_tsv: Path) -> list[dict]:
    """The D7 selection: every primary, non-OCR Arabic text dated 250-650 AH, dictionaries excluded
    (genre tags ``_MACAJIM``, ``_GHARIB`` and any "lexicograph" tag), sorted by version URI."""
    csv.field_size_limit(10 ** 8)
    rows = []
    with open(metadata_tsv, encoding="utf-8") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r["language"] != "ara" or r["status"] != "pri" or r["uncorrected_OCR"] != "False":
                continue
            try:
                date = int(r["date"])
            except ValueError:
                continue
            tags = [t for t in r["tags"].split(" :: ") if t.startswith("_")]
            if not 250 <= date <= 650:
                continue
            if any(t in ("_MACAJIM", "_GHARIB") for t in tags) or "lexicograph" in r["tags"].lower():
                continue
            rows.append({"version_uri": r["version_uri"], "date": date, "local_path": r["local_path"],
                         "tok_length": int(r["tok_length"] or 0),
                         "repo": f"{math.ceil(date / 25) * 25:04d}AH"})
    rows.sort(key=lambda r: r["version_uri"])
    return rows


def _count_file(path: str) -> tuple[Counter, dict[str, Counter]]:
    """skeleton -> count, and skeleton -> the counts of the spellings that differ from it."""
    counts: Counter = Counter()
    spelled: dict[str, Counter] = defaultdict(Counter)
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if line.startswith("#META#"):
                continue
            for tok in arabic_tokens(line):
                key = skeleton(tok)
                if key:
                    counts[key] += 1
                    if tok != key:
                        spelled[key][tok] += 1
    return counts, dict(spelled)


#: The suffixes an OpenITI text may carry in its repository, in the order ``fetch`` tries them.
OPENITI_EXTENSIONS = (".mARkdown", ".completed", ".inProgress", "")


def openiti_local_name(repo_path: str) -> str:
    """The file name ``fetch`` stores a text under, from its path in the repository."""
    return os.path.basename(repo_path).replace(".", "_") + ".txt"


def check_selection(openiti_dir: Path, manifest: dict) -> dict:
    """The manifest against D7's selection from the pinned metadata table: every selected text is
    counted or listed as unavailable, nothing outside it is counted, none twice, and few are
    unavailable. A manifest that lost rows is refused, however many tokens remain."""
    metadata = openiti_dir / "metadata.tsv"
    if not metadata.is_file() or sha256_of(metadata) != OPENITI_METADATA_SHA256:
        raise SourceError(f"missing, or differs from the pinned release: {metadata}")
    local_paths = {r["version_uri"]: r["local_path"] for r in select_openiti(metadata)}
    selected = set(local_paths)
    counted = [str(t.get("version_uri", "")) for t in manifest.get("texts") or []]
    # Each counted text is the file its own metadata row names, so two identifiers cannot share one.
    for t in manifest.get("texts") or []:
        base = local_paths.get(str(t.get("version_uri", "")))
        if base is not None and str(t.get("path", "")) not in {base + ext for ext in OPENITI_EXTENSIONS}:
            raise SourceError(f"openiti/manifest.json: {t.get('version_uri')} names the file {t.get('path')!r}, "
                              f"not its own ({base})")
    local_files = [openiti_local_name(str(t.get("path", ""))) for t in manifest.get("texts") or []]
    if len(local_files) != len(set(local_files)):
        raise SourceError("openiti/manifest.json names one file twice")
    unavailable = [str(u) for u in manifest.get("unavailable") or []]
    listed = counted + unavailable
    if len(listed) != len(set(listed)):
        raise SourceError("openiti/manifest.json names a text twice")
    outside = sorted(set(listed) - selected)
    if outside:
        raise SourceError(f"openiti/manifest.json: {len(outside)} texts outside the D7 selection "
                          f"(first {outside[0]!r})")
    left_out = sorted(selected - set(listed))
    if left_out:
        raise SourceError(f"openiti/manifest.json: {len(left_out)} selected texts neither counted nor listed "
                          f"as unavailable (first {left_out[0]})")
    if len(unavailable) > MAX_UNAVAILABLE_SHARE * len(selected):
        raise SourceError(f"openiti/manifest.json: {len(unavailable)} of {len(selected)} selected texts "
                          f"unavailable, more than {MAX_UNAVAILABLE_SHARE:.1%}")
    return {"freq_texts_selected": str(len(selected)), "freq_texts_unavailable": str(len(unavailable)),
            "freq_metadata_sha256": OPENITI_METADATA_SHA256}


def read_openiti(openiti_dir: Path, workers: int) -> tuple[list[tuple], dict]:
    """The frequency rows (skeleton, count, commonest spelling), kept at ``FREQ_MIN_STORED`` or
    more, from every text the manifest pins (sha256 checked), once the manifest is D7's selection."""
    manifest_path = openiti_dir / "manifest.json"
    manifest = _read_json(manifest_path)
    texts = manifest.get("texts") if isinstance(manifest, dict) else None
    if not texts:
        raise SourceError(f"{manifest_path}: no texts")
    selection = check_selection(openiti_dir, manifest)
    paths = []
    for t in texts:
        path = openiti_dir / openiti_local_name(str(t["path"]))
        if not path.is_file() or sha256_of(path) != t.get("sha256"):
            raise SourceError(f"missing, or sha256 differs from the manifest: {path}")
        paths.append(str(path))
    counts: Counter = Counter()
    spelled: dict[str, Counter] = defaultdict(Counter)
    if workers <= 1:
        results = map(_count_file, paths)
        pool = None
    else:
        pool = concurrent.futures.ProcessPoolExecutor(max_workers=workers)
        results = pool.map(_count_file, paths, chunksize=4)
    try:
        for file_counts, file_spelled in results:
            counts.update(file_counts)
            for key, spellings in file_spelled.items():
                spelled[key].update(spellings)
    finally:
        if pool is not None:
            pool.shutdown()
    rows = []
    for key, n in counts.items():
        if n < FREQ_MIN_STORED:
            continue
        variants = spelled.get(key, Counter())
        plain = n - sum(variants.values())
        best, best_n = key, plain
        for tok, c in variants.items():
            if c > best_n or (c == best_n and tok < best):
                best, best_n = tok, c
        rows.append((key, n, best))
    rows.sort()
    facts = {"freq_source": f"OpenITI release {OPENITI_RELEASE}", "freq_doi": OPENITI_DOI,
             "freq_credit": OPENITI_CREDIT, "freq_texts": str(len(paths)), "freq_tokens": str(sum(counts.values())),
             "freq_types": str(len(counts)), "freq_min_stored": str(FREQ_MIN_STORED),
             "freq_manifest_sha256": sha256_of(manifest_path),
             "freq_selection": "primary, non-OCR Arabic texts dated 250-650 AH; dictionaries excluded",
             **selection}
    return rows, facts


# ---------------------------------------------------------------------------
# The closed-class table
# ---------------------------------------------------------------------------

def read_closed_class(path: Path, entry_rows: list[tuple], root_rows: list[tuple]) -> tuple[list[tuple], dict]:
    """Rows of the closed-class table, each naming a Lane root (and optionally a headword under it)
    the sidecar holds; a row Lane cannot answer is a source error, not a silent gap."""
    data = _read_json(path)
    if not isinstance(data, dict) or not isinstance(data.get("forms"), list):
        raise SourceError(f"{path}: no forms list")
    roots = {r[0] for r in root_rows}
    # A headword is matched as written (vowels and all: مِنْ is not مَنْ), else by its letters.
    by_root_headword: dict[tuple[str, str], str] = {}
    for eid, key, _root, _ord, headword, *_rest in entry_rows:
        by_root_headword.setdefault((key, unicodedata.normalize("NFC", headword).strip()), eid)
    for eid, key, _root, _ord, headword, *_rest in entry_rows:
        by_root_headword.setdefault((key, strip_harakat(headword).strip()), eid)
    rows, seen = [], set()
    for n, item in enumerate(data["forms"]):
        if not isinstance(item, dict):
            raise SourceError(f"{path}: row {n} is not an object")
        form, root, note = item.get("form"), item.get("root"), item.get("note")
        if not all(isinstance(v, str) and v.strip() for v in (form, root, note)):
            raise SourceError(f"{path}: row {n} lacks a form, a root or a note")
        key, rkey = skeleton(strip_harakat(form)), root_key(strip_harakat(root))
        if rkey not in roots:
            raise SourceError(f"{path}: row {n} ({form}) names the root {root!r}, which Lane does not have")
        entry_id = None
        headword = item.get("headword")
        if headword:
            entry_id = by_root_headword.get((rkey, unicodedata.normalize("NFC", headword).strip()))
            if entry_id is None:
                raise SourceError(f"{path}: row {n} ({form}) names the headword {headword!r}, not under {root!r}")
        if (key, rkey) in seen:
            raise SourceError(f"{path}: row {n} ({form}) repeats a form and root")
        seen.add((key, rkey))
        rows.append((key, form, rkey, entry_id, note, n))
    return rows, {"closed_class_sha256": sha256_of(path)}


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

def _builder_revision() -> str:
    try:
        out = subprocess.run(["git", "rev-parse", "HEAD"], cwd=_REPO_ROOT, capture_output=True, text=True,
                             timeout=10)
        return (out.stdout.strip() or "unknown") if out.returncode == 0 else "unknown"
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def write_sidecar(path: Path, tables: dict[str, list[tuple]], facts: dict) -> dict:
    con = sqlite3.connect(str(path))
    try:
        con.execute("PRAGMA foreign_keys = ON")
        con.executescript(DDL)
        con.executemany("INSERT INTO lane_roots VALUES (?,?,?,?,?,?)", tables["lane_roots"])
        con.executemany("INSERT INTO lane_entries VALUES (?,?,?,?,?,?,?,?)", tables["lane_entries"])
        con.executemany("INSERT INTO lane_forms VALUES (?,?,?)", tables["lane_forms"])
        con.executemany("INSERT INTO freq VALUES (?,?,?)", tables["freq"])
        con.executemany("INSERT INTO closed_class VALUES (?,?,?,?,?,?)", tables["closed_class"])
        counts = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in COUNTED_TABLES}
        meta = {
            "version": SCHEMA_VERSION,
            "normalizer_version": str(NORMALIZER_VERSION),
            "built_at": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
            "builder": "scripts/build_arabic_sidecar.py",
            "builder_revision": _builder_revision(),
            **facts,
            **{f"rows_{t}": str(n) for t, n in counts.items()},
        }
        con.executemany("INSERT INTO meta VALUES (?, ?)", sorted(meta.items()))
        con.commit()
        bad = con.execute("PRAGMA foreign_key_check").fetchall()
        if bad:
            raise SourceError(f"foreign keys do not resolve: {bad[:5]}")
        con.execute("VACUUM")
    finally:
        con.close()
    return counts


def existing_counts(path: Path) -> Optional[dict]:
    """Row counts of a sidecar already at ``path``; None when there is none or it is unreadable."""
    if not path.is_file() or path.stat().st_size == 0:
        return None
    try:
        con = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    except sqlite3.Error:
        return None
    try:
        return {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("lane_entries", "freq")}
    except sqlite3.Error:
        return None
    finally:
        con.close()


def build(args: argparse.Namespace) -> int:
    source = Path(args.source_dir)
    out = Path(args.out)
    started = time.monotonic()
    try:
        entries, facts = read_lane(source / "emeiri")
        root_rows, entry_rows, form_rows, lane_facts = lane_tables(entries)
        freq_rows, freq_facts = read_openiti(source / "openiti", args.workers)
        closed_rows, closed_facts = read_closed_class(Path(args.closed_class), entry_rows, root_rows)
    except SourceError as exc:
        print(f"FAIL: {exc}")
        return EXIT_SOURCE
    except (TypeError, AttributeError, KeyError, ValueError) as exc:
        print(f"FAIL: a source is not shaped as expected ({type(exc).__name__}: {exc})")
        return EXIT_SOURCE
    facts.update(lane_facts)
    facts.update(freq_facts)
    facts.update(closed_facts)
    print(f"read     Lane entries {len(entry_rows):,}; roots {len(root_rows):,}; one-word headwords "
          f"{len(form_rows):,}; frequency rows {len(freq_rows):,} of {int(freq_facts['freq_types']):,} types "
          f"({int(freq_facts['freq_tokens']):,} tokens, {freq_facts['freq_texts']} texts); closed class "
          f"{len(closed_rows)}; {time.monotonic() - started:.0f} s")
    old = existing_counts(out)
    if old is not None and not args.force:
        fewer = {t: (old[t], n) for t, n in (("lane_entries", len(entry_rows)), ("freq", len(freq_rows)))
                 if n < old[t]}
        if fewer:
            print(f"REFUSED: {out} holds more rows than this build would write "
                  f"({', '.join(f'{t} {o:,} -> {n:,}' for t, (o, n) in fewer.items())}); pass --force to replace it")
            return EXIT_REFUSED
    tables = {"lane_roots": root_rows, "lane_entries": entry_rows, "lane_forms": form_rows, "freq": freq_rows,
              "closed_class": closed_rows}
    tmp = out.with_name(f"{out.name}.building-{os.getpid()}")
    try:
        out.parent.mkdir(parents=True, exist_ok=True)
        if tmp.exists():
            tmp.unlink()
        counts = write_sidecar(tmp, tables, facts)
        os.replace(tmp, out)
    except (sqlite3.Error, OSError, SourceError) as exc:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass
        print(f"FAIL: could not write {out}: {exc}")
        return EXIT_OUTPUT
    print(f"wrote    {out} ({out.stat().st_size:,} bytes): " + "; ".join(f"{t} {n:,}" for t, n in counts.items()))
    print("RESULT: OK")
    return EXIT_OK


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

def _get(url: str) -> bytes:
    for attempt in range(4):
        try:
            return urllib.request.urlopen(url, timeout=120).read()  # noqa: S310 -- fixed https hosts
        except OSError as exc:
            print(f"retry {url}: {exc}", file=sys.stderr)
            time.sleep(5 * (attempt + 1))
    raise SourceError(f"could not fetch {url}")


def _gh(path: str, jq: str) -> str:
    out = subprocess.run(["gh", "api", path, "--jq", jq], capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise SourceError(f"gh api {path}: {out.stderr.strip()}")
    return out.stdout


def fetch(dest: str) -> int:
    """Download the sources into an EMPTY folder: the emeiri manifest and shards (pinned in
    ``emeiri/pins.json``), the OpenITI metadata table (sha256-checked) and every text
    ``select_openiti`` names, at each repository's current commit (pinned in
    ``openiti/manifest.json``)."""
    root = Path(dest)
    if root.exists() and any(root.iterdir()):
        print(f"--dest must be an empty folder: {dest}")
        return EXIT_SOURCE
    try:
        pins = {}
        for name in ["manifest.json"] + [f"entries/{i}.json" for i in range(EMEIRI_SHARDS)]:
            data = _get(EMEIRI_SITE + "data/" + name)
            path = root / "emeiri" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            pins[name] = {"bytes": len(data), "sha256": sha256_bytes(data)}
        (root / "emeiri" / "pins.json").write_text(json.dumps(pins, indent=1), encoding="utf-8")
        metadata = _get(OPENITI_METADATA_URL)
        if sha256_bytes(metadata) != OPENITI_METADATA_SHA256:
            raise SourceError("the OpenITI metadata table differs from the pinned release")
        (root / "openiti").mkdir(parents=True, exist_ok=True)
        (root / "openiti" / "metadata.tsv").write_bytes(metadata)
        commits: dict[str, str] = {}
        trees: dict[str, set[str]] = {}
        texts = []
        unavailable = []
        for row in select_openiti(root / "openiti" / "metadata.tsv"):
            repo = row["repo"]
            if repo not in commits:
                commits[repo] = _gh(f"repos/OpenITI/{repo}/commits/master", ".sha").strip()
                trees[repo] = set(_gh(f"repos/OpenITI/{repo}/git/trees/{commits[repo]}?recursive=1",
                                      ".tree[].path").split("\n"))
            base = row["local_path"]
            found = [base + ext for ext in OPENITI_EXTENSIONS if base + ext in trees[repo]]
            if not found:
                print(f"not in its repository: {base}")
                unavailable.append(row["version_uri"])
                continue
            data = _get(f"https://raw.githubusercontent.com/OpenITI/{repo}/{commits[repo]}/{found[0]}")
            (root / "openiti" / openiti_local_name(found[0])).write_bytes(data)
            texts.append({"version_uri": row["version_uri"], "date": row["date"], "repo": repo,
                          "commit": commits[repo], "path": found[0], "bytes": len(data),
                          "sha256": sha256_bytes(data)})
        (root / "openiti" / "manifest.json").write_text(
            json.dumps({"texts": texts, "unavailable": unavailable}, ensure_ascii=False, indent=1), encoding="utf-8")
    except SourceError as exc:
        print(f"FAIL: {exc}")
        return EXIT_SOURCE
    print(f"fetched  {len(pins)} emeiri files and {len(texts)} OpenITI texts into {dest}")
    return EXIT_OK


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="command", required=True)
    f = sub.add_parser("fetch", help="download the sources into an empty folder")
    f.add_argument("--dest", required=True)
    b = sub.add_parser("build", help="build the sidecar from a fetched folder")
    b.add_argument("--source-dir", required=True, help="the folder fetch filled (emeiri/, openiti/)")
    b.add_argument("--out", default=str(DEFAULT_OUT))
    b.add_argument("--closed-class", default=str(CLOSED_CLASS_FILE))
    b.add_argument("--workers", type=int, default=max(1, min(8, (os.cpu_count() or 2) - 1)))
    b.add_argument("--force", action="store_true", help="replace a sidecar that holds more rows")
    args = ap.parse_args(argv)
    if args.command == "fetch":
        return fetch(args.dest)
    return build(args)


if __name__ == "__main__":
    sys.exit(main())
