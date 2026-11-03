#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build ``blau_data/blau.db``, the Judeo-Arabic dictionary sidecar, from the Blau site's JSON.

Source: https://blau-dict.netlify.app/ -- Joshua Blau, *A Dictionary of Mediaeval Judaeo-Arabic
Texts* (2006), digital edition release 1.1 (2026-08-25), "published with permission" of the
Academy of the Hebrew Language and the Israel Academy of Sciences and Humanities (MiDRASH / ERC).
Plan: ``docs/plans/BLAU_DICTIONARY_PLAN.md``.

Two files, fetched into an EMPTY directory of their own and never committed (``blau_data/`` is
gitignored; the sidecar is a build artifact like every other ``*_data/`` sidecar)::

    curl -L -o blau_data/download/semantic_blau.json.gz \\
        https://blau-dict.netlify.app/data/semantic_blau.json.gz
    curl -L -o blau_data/download/hebrew_bibliography.json \\
        https://blau-dict.netlify.app/data/frontmatter/hebrew_bibliography.json
    python scripts/build_blau_sidecar.py --source-dir blau_data/download --out blau_data/blau.db
    python scripts/verify_blau_sidecar.py --db blau_data/blau.db --expect-roots 3047 --expect-forms 12753

The download is untrusted data: it is read with the stdlib ``json`` module only, nothing in it is
executed, and nothing in it is used as a path. The build FAILS (exit 2) on a missing, empty,
truncated or malformed file (a cut-off gzip raises before any row is written), and REFUSES
(exit 3) to replace an existing sidecar that holds more roots or forms than the new one unless
``--force`` is given. The database is written beside the target under a temporary name and
renamed into place only once it is complete, so a failed build leaves the served file as it was.

Tables (schema ``SCHEMA_VERSION``): ``meta`` (key/value: versions, source release and digests,
row counts, build time, the credit line), ``sources`` (the bibliography), ``roots``,
``sub_entries``, ``stems``, ``forms`` (the Judeo-Arabic text, Arabic spelling, Hebrew and
English glosses, notes, flags, printed page, and the form's segments as JSON for rendering),
``citations`` (every parsed reference and dictionary citation with its source id) and
``forms_norm``, the lookup table keyed by ``shared.blau_normalize.normalize_ja`` of every root
name, sub-entry name, headword, spelling variant and quoted Judeo-Arabic phrase.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import gzip
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPTS_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from shared.blau_normalize import NORMALIZER_VERSION, normalize_arabic, normalize_ja  # noqa: E402

SCHEMA_VERSION = "1.0.0"
SOURCE_URL = "https://blau-dict.netlify.app/"
SOURCE_RELEASE = "1.1"
SOURCE_RELEASE_DATE = "2026-08-25"
SOURCE_SCHEMA_VERSION = "v6"
DICTIONARY_FILE = "semantic_blau.json.gz"
BIBLIOGRAPHY_FILE = "hebrew_bibliography.json"
DEFAULT_OUT = Path("blau_data") / "blau.db"

# The site's own credit line (its footer and about page), verbatim. Whether this is the line every
# GenizahSearch surface shows is owner ruling (a) in the plan; until then it is data in ``meta``.
CREDIT_HE = (
    "יהושע בלאו, מילון לטקסטים ערביים־יהודיים מימי הביניים, ירושלים: האקדמיה ללשון העברית "
    "והאקדמיה הלאומית הישראלית למדעים, תשס\"ו (2006) © האקדמיה ללשון העברית והאקדמיה הלאומית "
    "הישראלית למדעים. פורסם ברשות"
)
CREDIT_EN = (
    "Joshua Blau, A Dictionary of Mediaeval Judaeo-Arabic Texts, Jerusalem: The Academy of the "
    "Hebrew Language and The Israel Academy of Sciences and Humanities, 2006 © The Academy of "
    "the Hebrew Language and The Israel Academy of Sciences and Humanities. Published with permission."
)
PERMISSION_NOTE = (
    "The site states (about.html, release 1.1): the resources are made available for research and "
    "educational use only; no commercial use beyond conventional scholarly publication without "
    "written permission from the copyright holders."
)

EXIT_OK = 0
EXIT_SOURCE = 2      # missing, empty, truncated or malformed source
EXIT_REFUSED = 3     # an existing sidecar holds more rows; pass --force
EXIT_OUTPUT = 4      # could not write the output

# Segment text types, by what the form row stores (the site's js/search.js scopes, release 1.1).
_JA_HEADWORD_TYPES = frozenset({"headword_ja"})
_JA_VARIANT_TYPES = frozenset({"variant_ja"})
_ARABIC_SPELLING_TYPES = frozenset({"headword_arabic", "variant_arabic"})
_NOTE_TYPES = frozenset({"freeform_note", "morph_note", "grammar_citation"})
_JA_PHRASE_TYPES = frozenset({"freeform_note", "morph_note", "complement_ja", "hebrew_in_ja"})
_JA_FONT_PREFIX = "Hebrew-Miriam"   # the font that marks Judeo-Arabic text in Hebrew letters

DDL = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE sources (
    id TEXT PRIMARY KEY, abbreviation TEXT NOT NULL, full_reference TEXT, page INTEGER, parent TEXT
);
CREATE TABLE roots (
    id TEXT PRIMARY KEY, key TEXT NOT NULL UNIQUE, ordinal INTEGER NOT NULL,
    name_ja TEXT NOT NULL, names_json TEXT NOT NULL, name_ja_clean TEXT,
    arabic_spelling TEXT, arabic_norm TEXT, start_page_printed INTEGER,
    is_cross_reference INTEGER NOT NULL, target_id TEXT,
    num_stems INTEGER NOT NULL, num_sub_entries INTEGER NOT NULL, num_forms INTEGER NOT NULL
);
CREATE TABLE sub_entries (
    id TEXT PRIMARY KEY, root_id TEXT NOT NULL REFERENCES roots(id), ordinal INTEGER NOT NULL,
    name_ja TEXT NOT NULL, names_json TEXT NOT NULL, arabic_spelling TEXT, arabic_norm TEXT
);
CREATE TABLE stems (
    id TEXT PRIMARY KEY, root_id TEXT NOT NULL REFERENCES roots(id),
    sub_entry_id TEXT REFERENCES sub_entries(id), ordinal INTEGER NOT NULL,
    binyan TEXT, binyan_shift TEXT, government TEXT, complement_arabic TEXT, complement_ja TEXT,
    head_json TEXT
);
CREATE TABLE forms (
    id TEXT PRIMARY KEY, stem_id TEXT NOT NULL REFERENCES stems(id),
    root_id TEXT NOT NULL REFERENCES roots(id), sub_entry_id TEXT REFERENCES sub_entries(id),
    ordinal INTEGER NOT NULL, ja_text TEXT, arabic_spelling TEXT, gloss_he TEXT, gloss_en TEXT,
    notes TEXT, classical_note TEXT, cross_reference TEXT, target_ids TEXT, binyan TEXT,
    is_classical INTEGER NOT NULL, is_reconstructed INTEGER NOT NULL, is_erroneous INTEGER NOT NULL,
    page INTEGER, segments_json TEXT NOT NULL
);
CREATE TABLE citations (
    id TEXT PRIMARY KEY, form_id TEXT NOT NULL REFERENCES forms(id),
    kind TEXT NOT NULL CHECK (kind IN ('reference', 'dictionary')), reference_id TEXT,
    ordinal INTEGER NOT NULL, citation_type TEXT, source_work TEXT, source_id TEXT,
    source_ids TEXT, citation TEXT, ja_text TEXT, translation TEXT, commentary TEXT
);
CREATE TABLE forms_norm (
    norm TEXT NOT NULL, raw TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('root', 'sub_entry', 'headword', 'variant', 'phrase')),
    root_id TEXT NOT NULL REFERENCES roots(id), sub_entry_id TEXT REFERENCES sub_entries(id),
    form_id TEXT REFERENCES forms(id), word_count INTEGER NOT NULL
);
CREATE INDEX idx_forms_norm_norm ON forms_norm(norm);
CREATE INDEX idx_forms_norm_root ON forms_norm(root_id);
CREATE INDEX idx_sub_entries_root ON sub_entries(root_id);
CREATE INDEX idx_stems_root ON stems(root_id);
CREATE INDEX idx_forms_root ON forms(root_id);
CREATE INDEX idx_forms_stem ON forms(stem_id);
CREATE INDEX idx_citations_form ON citations(form_id);
CREATE INDEX idx_citations_source ON citations(source_id);
"""

COUNTED_TABLES = ("roots", "sub_entries", "stems", "forms", "citations", "forms_norm", "sources")


class SourceError(Exception):
    """The download is missing, empty, truncated or not the dictionary."""


# ---------------------------------------------------------------------------
# Reading the source
# ---------------------------------------------------------------------------

def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def read_dictionary(path: Path) -> tuple[dict, dict]:
    """The dictionary object and facts about the file. Raises SourceError on anything that is
    not a complete, well-formed v6 dictionary: a truncated gzip fails its CRC/length check at
    EOF, which ``gzip`` raises before ``json`` sees a byte."""
    if not path.is_file():
        raise SourceError(f"{path} does not exist")
    if path.stat().st_size == 0:
        raise SourceError(f"{path} is empty")
    try:
        with gzip.open(path, "rb") as fh:
            raw = fh.read()
    except (OSError, EOFError, gzip.BadGzipFile) as exc:
        raise SourceError(f"{path} is not a complete gzip file: {exc}") from exc
    if not raw:
        raise SourceError(f"{path} decompresses to nothing")
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise SourceError(f"{path} is not valid UTF-8 JSON: {exc}") from exc
    if not isinstance(data, dict) or not data:
        raise SourceError(f"{path}: expected a non-empty object keyed by entry name")
    for key, entry in data.items():
        if not isinstance(entry, dict):
            raise SourceError(f"{path}: entry {key!r} is not an object")
        if entry.get("schema_version") != SOURCE_SCHEMA_VERSION:
            raise SourceError(
                f"{path}: entry {key!r} has schema_version {entry.get('schema_version')!r}, "
                f"not {SOURCE_SCHEMA_VERSION!r}; this builder reads v6 only"
            )
        if not isinstance(entry.get("id"), str) or not entry["id"]:
            raise SourceError(f"{path}: entry {key!r} has no id")
    facts = {
        "source_sha256": sha256_of(path),
        "source_bytes": str(path.stat().st_size),
        "source_json_sha256": hashlib.sha256(raw).hexdigest(),
        "source_json_bytes": str(len(raw)),
    }
    return data, facts


def read_bibliography(path: Path) -> tuple[list, dict]:
    if not path.is_file():
        raise SourceError(f"{path} does not exist")
    if path.stat().st_size == 0:
        raise SourceError(f"{path} is empty")
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
        items = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise SourceError(f"{path} is not valid UTF-8 JSON: {exc}") from exc
    if not isinstance(items, list) or not items:
        raise SourceError(f"{path}: expected a non-empty list of bibliography entries")
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, dict) or not item.get("id") or not item.get("abbreviation"):
            raise SourceError(f"{path}: every bibliography entry needs an id and an abbreviation")
        if not isinstance(item["id"], str) or not isinstance(item["abbreviation"], str):
            raise SourceError(f"{path}: bibliography ids and abbreviations must be strings")
        if item["id"] in seen:
            raise SourceError(f"{path}: duplicate bibliography id {item['id']!r}")
        seen.add(item["id"])
    return items, {"bibliography_sha256": hashlib.sha256(raw).hexdigest()}


# ---------------------------------------------------------------------------
# Flattening the entries
# ---------------------------------------------------------------------------

def _texts(segments, types=None, font_prefix=None) -> list[str]:
    out = []
    for seg in segments or []:
        if not isinstance(seg, dict):
            continue
        text = seg.get("text")
        if not isinstance(text, str) or not text.strip():
            continue
        if types is not None and seg.get("text_type") not in types:
            continue
        if font_prefix is not None and not str(seg.get("font", "")).startswith(font_prefix):
            continue
        out.append(text.strip())
    return out


def _joined(segments, types, sep=" ") -> str | None:
    texts = _texts(segments, types)
    return sep.join(texts) if texts else None


def _opt_str(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, list):
        return " ".join(_texts(value)) or None
    return json.dumps(value, ensure_ascii=False)


def _int(value, default=0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _string_list(entry, field: str) -> list[str]:
    """``entry[field]`` as a list of non-empty strings: absent/None is empty, a bare string is one
    name, a list keeps its strings; anything else (a number, an object) is malformed input."""
    value = entry.get(field)
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        raise SourceError(f"entry {entry.get('id')!r}: {field} is {type(value).__name__}, not a list of strings")
    return [v for v in value if isinstance(v, str) and v.strip()]


def _names(entry) -> list[str]:
    return _string_list(entry, "name_ja")


def _spellings(entry) -> list[str]:
    return _string_list(entry, "arabic_spelling")


class Flattener:
    """Walks the dictionary once and fills the row lists; row order is the source's order."""

    def __init__(self):
        self.roots: list[tuple] = []
        self.sub_entries: list[tuple] = []
        self.stems: list[tuple] = []
        self.forms: list[tuple] = []
        self.citations: list[tuple] = []
        self.forms_norm: list[tuple] = []
        self.roots_unnamed = 0
        self.sub_entries_unnamed = 0
        self._norm_seen: set[tuple] = set()
        self._ids: set[str] = set()

    def _unique(self, kind: str, ident: str) -> str:
        if ident in self._ids:
            raise SourceError(f"duplicate {kind} id {ident!r}")
        self._ids.add(ident)
        return ident

    def _norm_row(self, raw: str, kind: str, root_id: str, sub_entry_id, form_id) -> None:
        norm = normalize_ja(raw)
        if not norm:
            return
        key = (norm, kind, root_id, sub_entry_id, form_id)
        if key in self._norm_seen:
            return
        self._norm_seen.add(key)
        self.forms_norm.append((norm, raw, kind, root_id, sub_entry_id, form_id, len(norm.split(" "))))

    def add_root(self, ordinal: int, key: str, entry: dict) -> None:
        root_id = self._unique("root", entry["id"])
        names = _names(entry)
        if not names:
            # A few roots in release 1.1 carry only an Arabic spelling (their JSON key is the
            # spelling in parentheses); kept, unnamed, and counted in meta.roots_unnamed.
            self.roots_unnamed += 1
        spellings = _spellings(entry)
        page = _int(entry.get("start_page_printed"), 0)
        self.roots.append((
            root_id, key, ordinal, " | ".join(names), json.dumps(names, ensure_ascii=False),
            " | ".join(n for n in (entry.get("name_ja_clean") or []) if isinstance(n, str)) or None,
            " | ".join(spellings) or None,
            " | ".join(normalize_arabic(s) for s in spellings if normalize_arabic(s)) or None,
            page, 1 if entry.get("is_cross_reference") else 0, entry.get("target_id") or None,
            0, 0, 0,   # num_stems, num_sub_entries, num_forms: filled below
        ))
        for name in names:
            self._norm_row(name, "root", root_id, None, None)
        n_stems = n_forms = 0
        for s_ord, stem in enumerate(entry.get("stems") or []):
            n_stems += 1
            n_forms += self.add_stem(s_ord, stem, root_id, None, page)
        subs = entry.get("sub_entries") or []
        for se_ord, sub in enumerate(subs):
            if not isinstance(sub, dict) or not sub.get("id"):
                raise SourceError(f"root {key!r}: sub-entry {se_ord} has no id")
            sub_id = self._unique("sub-entry", sub["id"])
            sub_names = _names(sub)
            if not sub_names:
                # 54 sub-entries in release 1.1 carry an Arabic spelling but no Hebrew-letter
                # name (lost in the site's extraction); their forms still hold Judeo-Arabic text,
                # so they are kept, unnamed, and counted in meta.sub_entries_unnamed.
                self.sub_entries_unnamed += 1
            sub_spellings = _spellings(sub)
            self.sub_entries.append((
                sub_id, root_id, se_ord, " | ".join(sub_names), json.dumps(sub_names, ensure_ascii=False),
                " | ".join(sub_spellings) or None,
                " | ".join(normalize_arabic(s) for s in sub_spellings if normalize_arabic(s)) or None,
            ))
            for name in sub_names:
                self._norm_row(name, "sub_entry", root_id, sub_id, None)
            for s_ord, stem in enumerate(sub.get("stems") or []):
                n_stems += 1
                n_forms += self.add_stem(s_ord, stem, root_id, sub_id, page)
        row = list(self.roots[-1])
        row[11], row[12], row[13] = n_stems, len(subs), n_forms
        self.roots[-1] = tuple(row)

    def add_stem(self, ordinal: int, stem: dict, root_id: str, sub_entry_id, page: int) -> int:
        if not isinstance(stem, dict) or not stem.get("id"):
            raise SourceError(f"root {root_id}: stem {ordinal} has no id")
        stem_id = self._unique("stem", stem["id"])
        head = stem.get("stem_head") or {}
        if not isinstance(head, dict):
            head = {}
        self.stems.append((
            stem_id, root_id, sub_entry_id, ordinal,
            _opt_str(head.get("binyan")), _opt_str(head.get("binyan_shift")), _opt_str(head.get("government")),
            _opt_str(head.get("complement_arabic")), _opt_str(head.get("complement_ja")),
            json.dumps(head, ensure_ascii=False) if head else None,
        ))
        for text in _texts(head.get("segments"), {"complement_ja"}, _JA_FONT_PREFIX):
            self._norm_row(text, "phrase", root_id, sub_entry_id, None)
        stem_binyan = _opt_str(head.get("binyan"))
        count = 0
        for f_ord, form in enumerate(stem.get("forms") or []):
            self.add_form(f_ord, form, stem_id, root_id, sub_entry_id, page, stem_binyan)
            count += 1
        return count

    def add_form(self, ordinal, form, stem_id, root_id, sub_entry_id, page, stem_binyan) -> None:
        if not isinstance(form, dict) or not form.get("id"):
            raise SourceError(f"stem {stem_id}: form {ordinal} has no id")
        form_id = self._unique("form", form["id"])
        segments = form.get("segments") or []
        if not isinstance(segments, list):
            raise SourceError(f"form {form_id}: segments is not a list")
        variant_head = form.get("variant_head") or {}
        if not isinstance(variant_head, dict):
            variant_head = {}
        binyan = _opt_str(variant_head.get("binyan")) or stem_binyan
        target_ids = [seg["target_id"] for seg in segments
                      if isinstance(seg, dict) and isinstance(seg.get("target_id"), str) and seg["target_id"]]
        self.forms.append((
            form_id, stem_id, root_id, sub_entry_id, ordinal,
            _joined(segments, _JA_HEADWORD_TYPES | _JA_VARIANT_TYPES, " | "),
            _joined(segments, _ARABIC_SPELLING_TYPES, " | "),
            _joined(segments, {"definition_hebrew"}), _joined(segments, {"definition_english"}),
            _joined(segments, _NOTE_TYPES), _joined(segments, {"classical"}),
            " ".join(t for t in _texts(segments, {"cross_reference"}) if t != "←") or None,
            json.dumps(target_ids) if target_ids else None, binyan,
            1 if form.get("is_classical") else 0, 1 if form.get("is_reconstructed") else 0,
            1 if form.get("is_erroneous") else 0, page,
            json.dumps({"segments": segments, "variant_head": variant_head or None,
                        "additional_notes": form.get("additional_notes")}, ensure_ascii=False),
        ))
        for text in _texts(segments, _JA_HEADWORD_TYPES):
            self._norm_row(text, "headword", root_id, sub_entry_id, form_id)
        for text in _texts(segments, _JA_VARIANT_TYPES):
            self._norm_row(text, "variant", root_id, sub_entry_id, form_id)
        for text in _texts(segments, _JA_PHRASE_TYPES, _JA_FONT_PREFIX):
            self._norm_row(text, "phrase", root_id, sub_entry_id, form_id)
        for text in _texts(variant_head.get("segments"), {"complement_ja"}, _JA_FONT_PREFIX):
            self._norm_row(text, "phrase", root_id, sub_entry_id, form_id)
        c_ord = 0
        for ref in form.get("references") or []:
            if not isinstance(ref, dict):
                continue
            parsed = ref.get("parsed")
            if isinstance(parsed, dict):
                parsed = [parsed]
            for cit in parsed or []:
                if not isinstance(cit, dict) or not cit.get("id"):
                    raise SourceError(f"form {form_id}: a parsed citation has no id")
                self.citations.append((
                    self._unique("citation", cit["id"]), form_id, "reference", ref.get("id") or None, c_ord,
                    _opt_str(cit.get("citation_type")), _opt_str(cit.get("source_work")),
                    _opt_str(cit.get("source_id")),
                    json.dumps(cit["source_ids"], ensure_ascii=False) if cit.get("source_ids") else None,
                    " ".join(_texts(cit.get("citation"))) or None, " ".join(_texts(cit.get("ja_text"))) or None,
                    " ".join(_texts(cit.get("translation"))) or None, " ".join(_texts(cit.get("commentary"))) or None,
                ))
                c_ord += 1
                for text in _texts(cit.get("ja_text"), None, _JA_FONT_PREFIX):
                    self._norm_row(text, "phrase", root_id, sub_entry_id, form_id)
        for dc in form.get("dictionary_citations") or []:
            if not isinstance(dc, dict) or not dc.get("id"):
                raise SourceError(f"form {form_id}: a dictionary citation has no id")
            self.citations.append((
                self._unique("citation", dc["id"]), form_id, "dictionary", None, c_ord, "dictionary", None,
                _opt_str(dc.get("source_id")),
                json.dumps(dc["source_ids"], ensure_ascii=False) if dc.get("source_ids") else None,
                " ".join(_texts(dc.get("segments"))) or None, None, None, None,
            ))
            c_ord += 1


def flatten(data: dict) -> Flattener:
    flat = Flattener()
    for ordinal, (key, entry) in enumerate(data.items()):
        flat.add_root(ordinal, key, entry)
    return flat


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

def _builder_revision() -> str:
    try:
        out = subprocess.run(["git", "rev-parse", "HEAD"], cwd=_REPO_ROOT, capture_output=True,
                             text=True, timeout=10)
        if out.returncode != 0:
            return "unknown"
        return out.stdout.strip() or "unknown"
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def write_sidecar(path: Path, flat: Flattener, bibliography: list, facts: dict) -> dict:
    con = sqlite3.connect(str(path))
    try:
        con.executescript(DDL)
        con.executemany("INSERT INTO sources VALUES (?, ?, ?, ?, ?)", [
            (b["id"], b["abbreviation"], b.get("full_reference"), _int(b.get("page"), 0) or None,
             (b.get("parent") or None))
            for b in bibliography
        ])
        con.executemany("INSERT INTO roots VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", flat.roots)
        con.executemany("INSERT INTO sub_entries VALUES (?,?,?,?,?,?,?)", flat.sub_entries)
        con.executemany("INSERT INTO stems VALUES (?,?,?,?,?,?,?,?,?,?)", flat.stems)
        con.executemany("INSERT INTO forms VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", flat.forms)
        con.executemany("INSERT INTO citations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", flat.citations)
        con.executemany("INSERT INTO forms_norm VALUES (?,?,?,?,?,?,?)", flat.forms_norm)
        counts = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in COUNTED_TABLES}
        # A source id the bibliography does not list (release 1.1 has one, blau-bib-000364): a fact
        # about the source, recorded so the gate can pin it rather than hide it.
        dangling = con.execute(
            "SELECT COUNT(*) FROM citations c WHERE c.source_id IS NOT NULL "
            "AND NOT EXISTS (SELECT 1 FROM sources s WHERE s.id = c.source_id)").fetchone()[0]
        meta = {
            "version": SCHEMA_VERSION,
            "source_url": SOURCE_URL,
            "source_release": SOURCE_RELEASE,
            "source_release_date": SOURCE_RELEASE_DATE,
            "source_schema_version": SOURCE_SCHEMA_VERSION,
            "normalizer_version": str(NORMALIZER_VERSION),
            "built_at": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
            "builder": "scripts/build_blau_sidecar.py",
            "builder_revision": _builder_revision(),
            "credit_he": CREDIT_HE,
            "credit_en": CREDIT_EN,
            "permission_note": PERMISSION_NOTE,
            "cross_references": str(sum(1 for r in flat.roots if r[9])),
            # Cross-reference roots the site itself could not resolve (14 in release 1.1, no
            # target_id): pinned so the gate can tell a mutated file from the source's own gaps.
            "cross_references_unresolved": str(sum(1 for r in flat.roots if r[9] and r[10] is None)),
            "roots_unnamed": str(flat.roots_unnamed),
            "sub_entries_unnamed": str(flat.sub_entries_unnamed),
            "citations_dangling_source": str(dangling),
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


def existing_counts(path: Path) -> dict | None:
    """Row counts of a sidecar already at ``path``; None when there is none, or it is unreadable
    (an unreadable file is replaced: there is nothing to protect)."""
    if not path.is_file() or path.stat().st_size == 0:
        return None
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except sqlite3.Error:
        return None
    try:
        return {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("roots", "forms")}
    except sqlite3.Error:
        return None
    finally:
        con.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--source-dir", required=True,
                    help=f"directory holding {DICTIONARY_FILE} and {BIBLIOGRAPHY_FILE}")
    ap.add_argument("--out", default=str(DEFAULT_OUT), help=f"the sidecar to write (default {DEFAULT_OUT})")
    ap.add_argument("--force", action="store_true",
                    help="replace an existing sidecar even when it holds more roots or forms")
    args = ap.parse_args(argv)

    source_dir = Path(args.source_dir)
    out = Path(args.out)
    try:
        data, facts = read_dictionary(source_dir / DICTIONARY_FILE)
        bibliography, bib_facts = read_bibliography(source_dir / BIBLIOGRAPHY_FILE)
        flat = flatten(data)
    except SourceError as exc:
        print(f"FAIL: {exc}")
        return EXIT_SOURCE
    except (TypeError, AttributeError, KeyError, ValueError) as exc:
        # Untrusted input of the wrong shape anywhere the walk did not check explicitly
        # (a scalar where a list or object is expected): still a source failure, not a traceback.
        print(f"FAIL: the download is not shaped like the dictionary ({type(exc).__name__}: {exc})")
        return EXIT_SOURCE
    facts.update(bib_facts)
    print(f"source   {source_dir / DICTIONARY_FILE} sha256 {facts['source_sha256']} "
          f"({facts['source_bytes']} bytes gz, {facts['source_json_bytes']} bytes JSON)")
    print(f"read     roots {len(flat.roots):,}; sub-entries {len(flat.sub_entries):,}; stems {len(flat.stems):,}; "
          f"forms {len(flat.forms):,}; citations {len(flat.citations):,}; lookup keys {len(flat.forms_norm):,}; "
          f"sources {len(bibliography):,}")

    old = existing_counts(out)
    if old is not None and not args.force:
        fewer = {t: (old[t], n) for t, n in (("roots", len(flat.roots)), ("forms", len(flat.forms))) if n < old[t]}
        if fewer:
            print(f"REFUSED: {out} holds more rows than this build would write "
                  f"({', '.join(f'{t} {o:,} -> {n:,}' for t, (o, n) in fewer.items())}); pass --force to replace it")
            return EXIT_REFUSED

    tmp = out.with_name(f"{out.name}.building-{os.getpid()}")
    try:
        out.parent.mkdir(parents=True, exist_ok=True)
        if tmp.exists():
            tmp.unlink()
        counts = write_sidecar(tmp, flat, bibliography, facts)
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


if __name__ == "__main__":
    sys.exit(main())
