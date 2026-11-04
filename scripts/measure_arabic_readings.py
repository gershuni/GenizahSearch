# -*- coding: utf-8 -*-
"""Measure an Arabic-dictionary layer beside Blau's dictionary, on real pages.

docs/plans/ARABIC_DICTIONARY_LAYER_PLAN.md is the plan this script measures.
Blau records what is particular to Judeo-Arabic, so an ordinary Arabic word
(אבו, לנא) has no entry, and an undotted letter is two letters (דלך is both
دلك and ذلك). The layer the plan proposes: read a Judeo-Arabic word as Arabic
(every reading its undotted letters allow), rank the readings by how common
each is in classical Arabic, find the root of the reading shown first, and
show that root's entries in Lane's *Arabic-English Lexicon* and *Lisān
al-ʿArab*. Nothing here ships; this script measures whether that works.

Inputs (read-only; ``fetch`` downloads the public ones into an EMPTY directory
of their own, as untrusted data, never executed):

- Lane: the Perseus TEI files as amended by the laneslexicon project
  (``LANE_REPO`` at ``LANE_COMMIT``), one ``<div2 type="root" n="...">`` per
  root, Arabic in Buckwalter transliteration.
- Lisān al-ʿArab: OpenITI ``0711IbnManzurIfriqi.LisanCarab.Shamela0001687-ara1``
  (``LISAN_REPO`` at ``LISAN_COMMIT``), one ``### $ <root>:`` line per root.
- A frequency count: a seeded sample of OpenITI release 2025.1.9 texts
  (``scripts/arabic_layer_openiti_sample.json`` pins each file by repository
  commit and sha256; ``pick`` rebuilds that list from the release metadata).
- Blau (``blau_data/blau.db``), the PGP Judeo-Arabic editions
  (``pgp_data/pgp.db``) and the automatic text (``Transcriptions.txt``), as
  ``scripts/measure_blau_close_spellings.py`` (PR #475) reads them: each
  automatic page is aligned word by word with the edition of the same
  manuscript (the alignment code below is a copy of that script's).

What is measured, on the aligned word pairs (``measure``):

- **P1, root against Blau.** Words the automatic text read right (the same as
  the edition) that reach exactly ONE Blau root with a clean 3-4 letter Arabic
  root spelling. Blau is the independent truth: the layer runs WITHOUT Blau and
  its top reading's root is compared with Blau's root. ``right`` / ``wrong``
  (a root shown, not Blau's) / ``none`` (nothing shown).
- **P2, the dot against the edition.** Edition words that carry a dot mark
  (U+0307, an apostrophe or geresh) on ג ד ט צ ת כ, aligned with an automatic
  word of the same letters. The layer gets the letters WITHOUT the marks; its
  top reading must have the dotted Arabic letter at every marked place.
- **P3, the words Blau lacks** (the layer's real population): how often a
  reading is shown and how often its root is in Lane or Lisān. There is no
  independent root truth for these words; ``--grading-sample`` writes a seeded
  sample for a person to grade.
- **The owner's examples** (``EXAMPLES``), each with its ranked readings.

The gate (``gate``; thresholds fixed in ``GATE`` before the first full run):
G1 P2 dot agreement under frequency ranking; G2 P1 ``right`` under frequency
ranking beats the plain-letter baseline (what an undotted letter gives today);
G3 P1 ``wrong`` stays under a ceiling. ``--ranking reversed`` / ``plain`` /
``dotted`` are deliberately broken rankings: the gate must fail on each
(exit 1).

Usage::

    python scripts/measure_arabic_readings.py fetch --dest <empty dir>
    python scripts/measure_arabic_readings.py measure --sources <that dir> --out _tmp/arabic_layer.json
    python scripts/measure_arabic_readings.py measure --sources <dir> --ranking reversed   # must exit 1

``--root-method camel`` needs CAMeL Tools and its ``morphology-db-msa-r13``
database (GPL v2) installed in the running Python; the default ``strip``
method is stdlib only.
"""
from __future__ import annotations

import argparse
import bisect
import csv
import difflib
import glob
import gzip
import hashlib
import html
import json
import math
import os
import random
import re
import sqlite3
import sys
import time
import urllib.request
from collections import Counter, defaultdict
from itertools import product
from pathlib import Path
from typing import Iterator, Optional

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from shared.arabic_normalize import (  # noqa: E402
    DOTTED,
    buckwalter_to_arabic,
    is_abbreviation,
    ja_letters,
    readings,
    root_key,
    skeleton,
    strip_harakat,
    arabic_tokens,
    strong_letters,
)
from shared.blau_normalize import candidate_stems, normalize_ja  # noqa: E402

HEADWORD_KINDS = ("root", "sub_entry", "headword", "variant")  # as shared/blau_service.py
HEADER_PREFIX = "==> "
HEADER_SUFFIX = " <=="

LANE_REPO = "laneslexicon/lexicon_xml"
LANE_COMMIT = "f3c19fb29f2cf2e12de3f97b7ce2b7a0d6682ea6"
LISAN_REPO = "OpenITI/0725AH"
LISAN_COMMIT = "056df249d6619b428a78f1d69a8ab2851b97b0a6"
LISAN_PATH = ("data/0711IbnManzurIfriqi/0711IbnManzurIfriqi.LisanCarab/"
              "0711IbnManzurIfriqi.LisanCarab.Shamela0001687-ara1.mARkdown")
LISAN_SHA256 = "34fa56710a5b701532a34cb70f8fe440b261e80ae3c40d9667d18b65be7ed86d"
OPENITI_METADATA_URL = "https://zenodo.org/records/17767721/files/OpenITI_metadata_2025-1-9.tsv?download=1"
OPENITI_METADATA_SHA256 = "10a025fcfc1bf96d218096f0dfe1d9416f9ff277f092c42622cb012cc4da2a61"
SAMPLE_MANIFEST = REPO / "scripts" / "arabic_layer_openiti_sample.json"
SAMPLE_SEED, SAMPLE_PER_TAG, SAMPLE_CAP_TOKENS = 20261104, 6, 15_000_000

# ---------------------------------------------------------------------------
# The two dictionaries' root lists (the letters, readings and keys are
# shared/arabic_normalize.py's, which the sidecar build uses too)
# ---------------------------------------------------------------------------


def lane_attribute(tag: str, name: str) -> str:
    """An attribute of a Lane XML tag, its entities decoded: Buckwalter's ``&`` (ؤ) and ``<`` (إ)
    can only stand in XML as ``&amp;`` and ``&lt;``, and converting the entity letter by letter
    would read ``&amp;`` as four Arabic letters. In the pinned master no converted name or key
    holds one: the 101 root names with one ("ytm &amp;c.") keep only their first word."""
    found = re.search(rf'\b{name}="([^"]*)"', tag)
    return html.unescape(found.group(1)) if found else ""


def lane_roots(lane_dir: str) -> tuple[dict[str, list[str]], Counter]:
    """root_key -> Lane's root names (Arabic), from every ``*.xml`` in the folder;
    root_key -> the characters of its articles (a prior: a common root has a
    long article)."""
    out: dict[str, list[str]] = defaultdict(list)
    size: Counter = Counter()
    files = sorted(glob.glob(os.path.join(lane_dir, "*.xml")))
    if not files:
        raise FileNotFoundError(f"no Lane XML in {lane_dir}")
    for path in files:
        text = Path(path).read_text(encoding="utf-8")
        tags = list(re.finditer(r'<div2\b[^>]*\btype="root"[^>]*>', text))
        for k, tag in enumerate(tags):
            name = lane_attribute(tag.group(0), "n")
            if not name.split():
                continue
            arabic = buckwalter_to_arabic(name.split()[0])
            if 1 <= len(arabic) <= 5:
                key = root_key(arabic)
                if arabic not in out[key]:
                    out[key].append(arabic)
                end = tags[k + 1].start() if k + 1 < len(tags) else len(text)
                size[key] += end - tag.end()
    return dict(out), size


def lane_forms(lane_dir: str) -> dict[str, set[str]]:
    """skeleton of a one-word Lane headword (``<entryFree key="kitaAbN">``) -> the
    root keys it is filed under: the dictionary itself as a form -> root table."""
    out: dict[str, set[str]] = defaultdict(set)
    for path in sorted(glob.glob(os.path.join(lane_dir, "*.xml"))):
        text = Path(path).read_text(encoding="utf-8")
        tags = list(re.finditer(r'<div2\b[^>]*\btype="root"[^>]*>', text))
        for k, tag in enumerate(tags):
            name = lane_attribute(tag.group(0), "n")
            if not name.split():
                continue
            root = root_key(buckwalter_to_arabic(name.split()[0]))
            end = tags[k + 1].start() if k + 1 < len(tags) else len(text)
            for raw in re.findall(r'<entryFree\b[^>]*\bkey="([^"]*)"', text[tag.end():end]):
                key = html.unescape(raw)
                if " " in key.strip():
                    continue
                bare = re.sub(r"[auioNKF~`=_^@\-]", "", key.replace("A^", "<"))
                form = skeleton(buckwalter_to_arabic(bare.replace("<", "A^")))
                if len(form) >= 2:
                    out[form].add(root)
    return dict(out)


def _proclitic_layers(pro: str) -> int:
    """How many proclitics a peeled prefix is: و/ف, then ب/ل/ك, then the article (لل is two)."""
    layers, rest = 0, pro
    if rest[:1] in ("و", "ف"):
        layers, rest = layers + 1, rest[1:]
    if rest.startswith("لل"):
        return layers + 2
    if rest[:1] in ("ب", "ل", "ك"):
        layers, rest = layers + 1, rest[1:]
    return layers + rest.startswith("ال")


def form_roots(reading: str, forms: dict[str, set[str]], prior: Counter,
               counts: Optional[Counter] = None) -> list[str]:
    """Roots of a reading through Lane's headwords: the reading, or the reading with
    a proclitic and a pronoun peeled, found as a headword. Score: half a point per
    proclitic, a point for a pronoun, less a third of the stem's log10 count in the
    sample (بقاءك is بقاء + ك, not ب + قاك); best first."""
    counts = counts or Counter()
    found: dict[str, float] = {}
    for pro in BACKOFF_PROCLITICS:
        if not reading.startswith(pro):
            continue
        for enc in BACKOFF_ENCLITICS:
            if not reading.endswith(enc):
                continue
            stem = reading[len(pro):len(reading) - len(enc)]
            if len(stem) < 2 or stem not in forms:
                continue
            score = (COST_PROCLITIC * _proclitic_layers(pro) + COST_AFFIX * bool(enc)
                     - math.log10(1 + counts.get(stem, 0)) / 3)
            for root in forms[stem]:
                if score < found.get(root, 99.0):
                    found[root] = score
    return sorted(found, key=lambda k: (found[k], -prior.get(k, 0), k))


def load_sidecar(db: str) -> tuple[dict, Counter, dict, Counter, dict, dict]:
    """From the built sidecar: Lane's roots, the prior (article characters per root), the
    headword table, the counts and spellings, and the closed-class table (skeleton -> roots)."""
    conn = _ro(db)
    try:
        lane = {key: json.loads(names) for key, names in conn.execute("SELECT root_key, names_json FROM lane_roots")}
        prior = Counter(dict(conn.execute("SELECT root_key, SUM(LENGTH(text)) FROM lane_entries GROUP BY root_key")))
        forms: dict[str, set[str]] = defaultdict(set)
        for form, key in conn.execute("SELECT skeleton, root_key FROM lane_forms"):
            forms[form].add(key)
        counts, surface = Counter(), {}
        for key, n, spelled in conn.execute("SELECT skeleton, count, surface FROM freq"):
            counts[key] = n
            surface[key] = spelled
        closed: dict[str, list[str]] = defaultdict(list)
        for key, rkey in conn.execute("SELECT skeleton, root_key FROM closed_class ORDER BY ordinal"):
            if rkey not in closed[key]:
                closed[key].append(rkey)
        return lane, prior, dict(forms), counts, surface, dict(closed)
    finally:
        conn.close()


def lisan_roots(lisan_file: str) -> tuple[dict[str, list[str]], Counter]:
    """root_key -> Lisān's entry heads (``### $ <root>:``); root_key -> the
    characters of those entries."""
    out: dict[str, list[str]] = defaultdict(list)
    size: Counter = Counter()
    key = None
    with open(lisan_file, encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("### "):
                key = None
                head = re.match(r"### \$ ([^:،.]+?)\s*(?::|$)", line)
                arabic = head.group(1).strip() if head else ""
                if 2 <= len(arabic) <= 5 and " " not in arabic:
                    key = root_key(arabic)
                    if arabic not in out[key]:
                        out[key].append(arabic)
            if key is not None:
                size[key] += len(line)
    return dict(out), size


# ---------------------------------------------------------------------------
# Frequency
# ---------------------------------------------------------------------------

def count_frequency(text_dir: str) -> tuple[Counter, dict[str, Counter]]:
    """skeleton -> count over every ``*.txt`` in the folder, and skeleton -> its
    surface spellings with counts (to show ذلك rather than the key ذلك)."""
    counts: Counter = Counter()
    surfaces: dict[str, Counter] = defaultdict(Counter)
    files = sorted(glob.glob(os.path.join(text_dir, "*.txt")))
    if not files:
        raise FileNotFoundError(f"no frequency texts in {text_dir}")
    for path in files:
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if line.startswith("#META#"):
                    continue
                for tok in arabic_tokens(line):
                    key = skeleton(tok)
                    if key:
                        counts[key] += 1
                        surfaces[key][tok] += 1
    return counts, surfaces


def load_frequency(sources: str, cache: Optional[str], extra: tuple[str, ...] = ()) -> tuple[Counter, dict[str, str]]:
    """skeleton -> count and skeleton -> its commonest surface spelling, cached as gzip JSON.
    ``extra`` folders of texts are counted too (the sample-size sensitivity run)."""
    if cache and os.path.isfile(cache):
        with gzip.open(cache, "rt", encoding="utf-8") as fh:
            data = json.load(fh)
        return Counter(data["counts"]), data["surface"]
    counts, surfaces = count_frequency(os.path.join(sources, "openiti"))
    for folder in extra:
        more, more_surfaces = count_frequency(folder)
        counts.update(more)
        for key, spellings in more_surfaces.items():
            surfaces.setdefault(key, Counter()).update(spellings)
    surface = {k: c.most_common(1)[0][0] for k, c in surfaces.items()}
    if cache:
        Path(cache).parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(cache, "wt", encoding="utf-8") as fh:
            json.dump({"counts": counts, "surface": surface}, fh, ensure_ascii=False)
    return counts, surface


#: A reading is offered only when it occurs at least this often in the sample.
MIN_COUNT = 2
RANKINGS = ("frequency", "plain", "dotted", "reversed")


#: Method v2's backoff: the clitics peeled from an unattested reading before its
#: stem is counted instead (\u0648\u062a\u0646\u0641\u0630\u0647 -> \u062a\u0646\u0641\u0630).
BACKOFF_PROCLITICS = ("", "\u0648\u0628\u0627\u0644", "\u0641\u0628\u0627\u0644", "\u0648\u0627\u0644", "\u0641\u0627\u0644", "\u0628\u0627\u0644", "\u0644\u0644", "\u0627\u0644", "\u0648\u0628", "\u0641\u0628", "\u0648\u0644", "\u0641\u0644", "\u0648", "\u0641", "\u0628",
                      "\u0644", "\u0643")
BACKOFF_ENCLITICS = ("", "\u0643\u0645\u0627", "\u0647\u0645\u0627", "\u0647\u0627", "\u0647\u0645", "\u0647\u0646", "\u0643\u0645", "\u0643\u0646", "\u0646\u0627", "\u0646\u064a", "\u0647", "\u0643", "\u064a")


def backoff_count(reading: str, counts: Counter) -> int:
    """The count of the commonest stem left when a proclitic and a pronoun are
    peeled from the reading (at least three letters left), 0 when none is counted."""
    best = 0
    for pro in BACKOFF_PROCLITICS:
        if not reading.startswith(pro):
            continue
        for enc in BACKOFF_ENCLITICS:
            if (pro or enc) and reading.endswith(enc):
                stem = reading[len(pro):len(reading) - len(enc)]
                if len(stem) >= 3:
                    best = max(best, counts.get(stem, 0))
    return best


def rank_readings(word: str, counts: Counter, ranking: str = "frequency",
                  version: str = "v1", min_count: int = MIN_COUNT) -> list[tuple[str, int]]:
    """(skeleton, count) of the word's attested readings, best first.

    ``frequency`` -- commonest first (the proposal). The broken rankings the gate
    must reject: ``plain`` -- every undotted letter read plain, as the lookup
    does today; ``dotted`` -- every dottable letter read dotted; ``reversed`` --
    rarest attested reading first.

    ``version`` "v2" adds the plural alif (``readings``) and, when no reading is
    attested as a whole word, ranks them by their stems (``backoff_count``)."""
    later = version in ("v2", "v3", "v4")
    all_readings = readings(word, plural_alif=later)
    if ranking == "plain":
        all_readings = all_readings[:1]
    elif ranking == "dotted":
        letters = ja_letters(word)
        dotted_word = "".join(f"{letter}\u0307" if letter in DOTTED else letter for letter, _ in letters)
        all_readings = readings(dotted_word, plural_alif=later)[:1]
    attested = [(r, counts.get(r, 0)) for r in all_readings if counts.get(r, 0) >= min_count]
    if not attested and later:
        attested = [(r, c) for r, c in ((r, backoff_count(r, counts)) for r in all_readings) if c >= min_count]
    if ranking == "reversed":
        return sorted(attested, key=lambda rc: rc[1])
    order = {r: i for i, r in enumerate(all_readings)}
    return sorted(attested, key=lambda rc: (-rc[1], order[rc[0]]))


# ---------------------------------------------------------------------------
# Root of a reading
# ---------------------------------------------------------------------------

PROCLITICS = ("وبال", "فبال", "وال", "فال", "بال", "كال", "لل", "ال", "وب", "فب", "ول", "فل", "وك", "فك",
              "و", "ف", "ب", "ل", "ك", "س")
ENCLITICS = ("كما", "هما", "ها", "هم", "هن", "كم", "كن", "نا", "ني", "ه", "ك", "ي")
ENDINGS = ("تما", "تموا", "ون", "ين", "ان", "ات", "تم", "تن", "وا", "ة", "ه", "ت", "ا", "ن", "ي")
PREFIXES = ("است", "مست", "ان", "ي", "ت", "ن", "ا", "أ", "م")
INFIXES = set("اويت")
#: Peeling a proclitic (conjunction, preposition, article) is cheaper than an
#: inflection, and both are cheaper than dropping a letter from inside the stem.
COST_PROCLITIC, COST_AFFIX, COST_INFIX = 0.5, 1.0, 1.5


def strip_roots(surface: str, roots: set[str], prior: Optional[Counter] = None) -> list[str]:
    """Root keys an Arabic word may come from: peel a proclitic, a pronoun, an
    inflectional ending and a verbal or nominal prefix (each optional), drop at
    most one inner long vowel or t, and keep what is a root of Lane or Lisān.
    Cheapest first; a tie goes to the root with the longer dictionary articles."""
    word = strip_harakat(surface)
    stems: list[tuple[float, str]] = []
    for pro in ("",) + PROCLITICS:
        if pro and not word.startswith(pro):
            continue
        if pro == "س" and word[1:2] not in ("ي", "ت", "ن", "أ"):
            continue  # the future marker comes only before an imperfect verb (سلامة is not سـ + لامة)
        a = word[len(pro):]
        for enc in ("",) + ENCLITICS:
            if enc and not a.endswith(enc):
                continue
            b = a[: len(a) - len(enc)] if enc else a
            for end in ("",) + ENDINGS:
                if end and not b.endswith(end):
                    continue
                c = b[: len(b) - len(end)] if end else b
                for pre in ("",) + PREFIXES:
                    if pre and not c.startswith(pre):
                        continue
                    d = c[len(pre):]
                    if len(d) >= 2:
                        cost = (COST_PROCLITIC * bool(pro)
                                + COST_AFFIX * (bool(enc) + bool(end) + bool(pre)))
                        stems.append((cost, d))
    found: dict[str, float] = {}
    for cost, stem in stems:
        candidates = [(0.0, stem)]
        if len(stem) > 3:
            candidates += [(COST_INFIX, stem[:i] + stem[i + 1:]) for i, ch in enumerate(stem) if ch in INFIXES and i]
        for extra, cand in candidates:
            if 2 <= len(cand) <= 4:
                key = root_key(cand)
                if key in roots and cost + extra < found.get(key, 99.0):
                    found[key] = cost + extra
    prior = prior or Counter()
    return sorted(found, key=lambda k: (found[k], -prior.get(k, 0), k))


class CamelRoots:
    """Roots from CAMeL Tools' analyzer over ``morphology-db-msa-r13``: the root of
    the most probable analysis (``pos_lex_logprob``). Optional dependency."""

    def __init__(self) -> None:
        from camel_tools.morphology.analyzer import Analyzer  # optional: GPL v2 database
        from camel_tools.morphology.database import MorphologyDB

        self.analyzer = Analyzer(MorphologyDB.builtin_db())
        self.cache: dict[str, list[str]] = {}

    def roots(self, surface: str) -> list[str]:
        if surface not in self.cache:
            scored: dict[str, float] = {}
            for analysis in self.analyzer.analyze(surface):
                root = str(analysis.get("root") or "")
                if not root or root in ("NTWS", "PUNC", "DIGIT", "FOREIGN"):
                    continue
                score = float(analysis.get("pos_lex_logprob") or -99.0)
                # CAMeL writes a hamza or weak radical as "#" (أبو is #.ب.#): try both
                letters = root.split(".")
                options = [("ء", "W") if ch == "#" else (ch,) for ch in letters]
                for combo in product(*options):
                    key = root_key("".join(combo).replace("W", "و"))
                    if key and score > scored.get(key, -1e9):
                        scored[key] = score
            self.cache[surface] = [k for k, _ in sorted(scored.items(), key=lambda kv: -kv[1])]
        return self.cache[surface]


# ---------------------------------------------------------------------------
# Blau, the editions, the automatic text (the alignment is
# scripts/measure_blau_close_spellings.py's, PR #475)
# ---------------------------------------------------------------------------

def _ro(path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{Path(path).resolve().as_posix()}?mode=ro", uri=True)


def load_blau(blau_db: str) -> tuple[dict[str, frozenset[str]], dict[str, str]]:
    """single-word headword-like key -> Blau root ids (a cross-reference root as its
    target); root id -> its clean root key ("" when the Arabic spelling is not a
    3-4 letter root: a noun with ة or a long vowel, several words, none)."""
    conn = _ro(blau_db)
    try:
        xref, clean = {}, {}
        for rid, is_x, target, arabic in conn.execute(
                "SELECT id, is_cross_reference, target_id, arabic_spelling FROM roots"):
            if is_x and target:
                xref[str(rid)] = str(target)
            letters = "".join(strip_harakat((arabic or "").split("|")[0]).split())
            ok = (3 <= len(letters) <= 4 and all("\u0621" <= ch <= "\u064a" for ch in letters)
                  and "ة" not in letters and "ا" not in letters[1:])
            clean[str(rid)] = root_key(letters) if ok else ""
        marks = ",".join("?" * len(HEADWORD_KINDS))
        out: dict[str, set[str]] = defaultdict(set)
        for norm, root_id in conn.execute(
                f"SELECT norm, root_id FROM forms_norm WHERE word_count = 1 AND kind IN ({marks})", HEADWORD_KINDS):
            out[str(norm)].add(xref.get(str(root_id), str(root_id)))
        # A word also reaches an entry whose several-word name begins with it ("ד\u0307לך ו"),
        # as shared/blau_service.py::NAME_PREFIX_KINDS does since PR #475.
        for norm, root_id in conn.execute(
                "SELECT norm, root_id FROM forms_norm WHERE word_count > 1 AND kind IN ('root', 'sub_entry', "
                "'headword')"):
            out[str(norm).split(" ", 1)[0]].add(xref.get(str(root_id), str(root_id)))
        return {k: frozenset(v) for k, v in out.items()}, clean
    finally:
        conn.close()


def is_subsequence(needle: str, haystack: str) -> bool:
    it = iter(haystack)
    return all(ch in it for ch in needle)


def load_editions(pgp_db: str) -> tuple[dict[int, list[str]], dict[str, set[int]]]:
    """pgpid -> the edition's words AS WRITTEN (marks kept, split on spaces);
    sys_id -> pgpids."""
    conn = _ro(pgp_db)
    try:
        words: dict[int, list[str]] = defaultdict(list)
        for pgpid, content in conn.execute(
                "SELECT pgpid, content FROM document_sources WHERE source_language = 'jrb' "
                "AND doc_relation = 'Digital Edition' ORDER BY pgpid, sequence_order, id"):
            for raw in (content or "").split():
                if normalize_ja(raw) and " " not in normalize_ja(raw):
                    words[int(pgpid)].append(raw)
        by_sys: dict[str, set[int]] = defaultdict(set)
        for pgpid, sys_id in conn.execute("SELECT document_id, sys_id FROM document_fragments"):
            if int(pgpid) in words:
                by_sys[str(sys_id)].add(int(pgpid))
        return dict(words), dict(by_sys)
    finally:
        conn.close()


def iter_pages(path: str, wanted: set[str]) -> Iterator[tuple[str, str, str]]:
    """(sys_id, header, text) for every page of a wanted sys_id, streaming."""
    sys_id = header = None
    lines: list[str] = []
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if line.startswith(HEADER_PREFIX) and line.rstrip().endswith(HEADER_SUFFIX):
                if sys_id is not None:
                    yield sys_id, header, "".join(lines)
                header = line.strip()[len(HEADER_PREFIX):-len(HEADER_SUFFIX)]
                candidate = header.split("_", 1)[0]
                sys_id = candidate if candidate in wanted else None
                lines = []
            elif sys_id is not None:
                lines.append(line)
    if sys_id is not None:
        yield sys_id, header, "".join(lines)


def raw_words(text: str) -> list[str]:
    """The page's words as written (marks kept), one Hebrew-letter word each."""
    return [w for w in text.split() if normalize_ja(w) and " " not in normalize_ja(w)]


def aligned_pairs(page: list[str], edition: list[str], min_coverage: float) -> Optional[list[tuple[int, int]]]:
    """(page index, edition index) pairs of one page aligned on the normalized
    words, or None when fewer than ``min_coverage`` of the page's words matched."""
    a = [normalize_ja(w) for w in page]
    b = [normalize_ja(w) for w in edition]
    ops = difflib.SequenceMatcher(None, a, b, autojunk=False).get_opcodes()
    matched = sum(i2 - i1 for tag, i1, i2, _j1, _j2 in ops if tag == "equal")
    if not a or matched / len(a) < min_coverage:
        return None
    pairs: list[tuple[int, int]] = []
    for k, (tag, i1, i2, j1, j2) in enumerate(ops):
        if tag == "equal":
            pairs.extend(zip(range(i1, i2), range(j1, j2)))
        elif tag == "replace" and i2 - i1 == j2 - j1 <= 2:
            before = ops[k - 1][0] if k > 0 else ""
            after = ops[k + 1][0] if k + 1 < len(ops) else ""
            if before == "equal" and after == "equal":
                pairs.extend(zip(range(i1, i2), range(j1, j2)))
    return pairs


# ---------------------------------------------------------------------------
# The measurement
# ---------------------------------------------------------------------------

#: The owner's failing examples (2026-11-04) and a few common words.
EXAMPLES = ("דלך", "ד'לך", "ודלך", "אבו", "לנא", "אלדי", "גיר", "כאן", "תם", "אכד", "אלכתאב", "קאל", "פי", "מן",
            "עלי", "אלי", "שי", "איצא", "הדא", "דאלך", "צרב")

#: The 300 documents the methods were shaped on (plan, section 6.2), pinned.
DEV_DOCUMENTS_FILE = REPO / "scripts" / "arabic_layer_dev_documents.json"


def load_dev_documents(path: str) -> set[int]:
    """The pinned development documents (PGP ids); a missing or empty list is an error, never
    an empty set that would score them."""
    ids = json.loads(Path(path).read_text(encoding="utf-8")).get("pgpids") or []
    if not ids:
        raise ValueError(f"{path}: no development documents")
    return {int(i) for i in ids}


#: Fixed before the first full run (2026-11-04); see the plan, §6.
GATE = {"G1_p2_dot_agreement_min": 0.80, "G2_p1_right_margin_over_plain_min": 0.05, "G3_p1_wrong_max": 0.30}
MAX_SHOWN = 3


def plural_alif_dropped(top: str, ed_letters: list[tuple[str, bool]]) -> str:
    """The top reading letter for letter with the page: the silent alif that v2 and later add
    after a final ו (חצ'רו = حضروا) is not one of the page's letters, so P2 compares without it."""
    if len(top) == len(ed_letters) + 1 and top.endswith("وا") and ed_letters and ed_letters[-1][0] == "ו":
        return top[:-1]
    return top


class Layer:
    """The proposed lookup, for one ranking and one root method."""

    def __init__(self, counts: Counter, surface: dict[str, str], roots: set[str], prior: Counter, ranking: str,
                 method: str, camel: Optional[CamelRoots], version: str = "v1",
                 forms: Optional[dict[str, set[str]]] = None, closed: Optional[dict[str, list[str]]] = None):
        self.counts, self.surface, self.roots, self.prior = counts, surface, roots, prior
        self.forms = forms or {}
        self.closed = closed or {}
        self.min_count = MIN_COUNT
        self.ranking, self.method, self.camel, self.version = ranking, method, camel, version
        self._cache: dict[str, tuple[list[tuple[str, int]], list[str]]] = {}

    def root_of(self, reading: str) -> list[str]:
        """Roots of a reading, found from its commonest spelling in the sample
        (which carries the hamza and the ة the skeleton folds away)."""
        spelled = self.surface.get(reading, reading)
        if self.closed:
            # v4: a closed-class word (with at most a conjunction before it) is the table's.
            for pro in ("", "و", "ف"):
                if reading.startswith(pro) and reading[len(pro):] in self.closed:
                    return self.closed[reading[len(pro):]]
        if self.method == "camel" and self.camel is not None:
            return [k for k in self.camel.roots(spelled) if k in self.roots]
        if self.version in ("v3", "v4") and self.forms:
            found = form_roots(reading, self.forms, self.prior, self.counts)
            if found:
                return found
        return strip_roots(spelled, self.roots, self.prior)

    def lookup(self, word: str) -> tuple[list[tuple[str, int]], list[str]]:
        """(ranked attested readings, the first reading's roots in Lane/Lisān)."""
        if word not in self._cache:
            ranked = rank_readings(word, self.counts, self.ranking, self.version, self.min_count)[:MAX_SHOWN]
            roots = self.root_of(ranked[0][0]) if ranked else []
            self._cache[word] = (ranked, roots)
        return self._cache[word]


def run(args: argparse.Namespace) -> dict:
    started = time.monotonic()
    sources = args.sources
    closed: dict[str, list[str]] = {}
    if args.sidecar:
        # The built artifact (scripts/build_arabic_sidecar.py): Lane only, the full count.
        lane, prior, forms, counts, surface, closed = load_sidecar(args.sidecar)
        lisan = {}
    else:
        lane, lane_size = lane_roots(os.path.join(sources, "lane"))
        lisan, lisan_size = lisan_roots(os.path.join(sources, "lisan", os.path.basename(LISAN_PATH)))
        counts, surface = load_frequency(sources, args.frequency_cache, tuple(args.frequency_extra))
        prior = lane_size + lisan_size
        forms = lane_forms(os.path.join(sources, "lane")) if args.method_version in ("v3", "v4") else {}
    roots = set(lane) | set(lisan)
    camel = CamelRoots() if args.root_method == "camel" else None
    layers = {r: Layer(counts, surface, roots, prior, r, args.root_method, camel, args.method_version, forms,
                       closed if args.method_version == "v4" else None)
              for r in RANKINGS}
    for layer in layers.values():
        layer.min_count = args.min_count
    key_roots, clean_root = load_blau(args.blau_db)
    editions, by_sys = load_editions(args.pgp_db)
    # The development documents the method was shaped on, pinned (a later seed or a newer
    # pgp.db must not re-admit them). Never scored unless asked.
    dev = load_dev_documents(args.dev_file)
    if args.only_dev:
        by_sys = {s: {p for p in ps if p in dev} for s, ps in by_sys.items()}
        by_sys = {s: ps for s, ps in by_sys.items() if ps}
    if args.limit_docs:
        keep = set(random.Random(args.seed).sample(sorted(editions), min(args.limit_docs, len(editions))))
        by_sys = {s: {p for p in ps if p in keep} for s, ps in by_sys.items()}
        by_sys = {s: ps for s, ps in by_sys.items() if ps}
    sample_rng = random.Random(args.seed)
    sample_seen: Counter = Counter()

    def keep_sample(name: str, item: dict) -> None:
        """A seeded reservoir sample, so the examples are not the corpus's first pages."""
        sample_seen[name] += 1
        if len(samples[name]) < args.samples:
            samples[name].append(item)
        else:
            k = sample_rng.randrange(sample_seen[name])
            if k < args.samples:
                samples[name][k] = item

    def blau_roots(word: str) -> frozenset[str]:
        found: set[str] = set()
        for stem in candidate_stems(word):
            found |= key_roots.get(stem, frozenset())
        return frozenset(found)

    p1 = {r: Counter() for r in RANKINGS}
    p2 = {r: Counter() for r in RANKINGS}
    p3 = Counter()
    p3_words: Counter = Counter()
    p3_context: dict[str, list[tuple[str, str, str]]] = defaultdict(list)  # word -> (context, sys_id, page)
    p1_seen: Counter = Counter()
    p2_seen: Counter = Counter()
    samples: dict[str, list] = {"p1_wrong": [], "p2_wrong": [], "p2_none": [], "p3_no_reading": []}
    pages_seen = pages_used = pairs_total = 0
    dev_pages = 0
    for sys_id, header, text in iter_pages(args.transcriptions, set(by_sys)):
        page = raw_words(text)
        if len(page) < args.min_words:
            continue
        pages_seen += 1
        best, best_doc = None, None
        for pgpid in sorted(by_sys[sys_id]):
            pairs = aligned_pairs(page, editions[pgpid], args.min_coverage)
            if pairs is not None and (best is None or len(pairs) > len(best)):
                best, best_doc = pairs, pgpid
        if best is None:
            continue
        if best_doc in dev and not (args.score_dev or args.only_dev):
            dev_pages += 1
            continue
        pages_used += 1
        pairs_total += len(best)
        edition = editions[best_doc]
        for i, j in best:
            auto_raw, ed_raw = page[i], edition[j]
            auto, ed = normalize_ja(auto_raw), normalize_ja(ed_raw)
            if len(auto) < 2:
                continue
            # P2: the edition marks a dot; the layer sees the letters without marks.
            # Not scored: a mark on ג (it marks ج or غ by the scribe's system), a
            # mark on the last letter, a word with gershayim and a word with a mark
            # after a letter that takes no dot (all mostly abbreviations and
            # numerals: אלמד׳ = al-madhkūr, ב׳כ׳ר׳).
            ed_letters = ja_letters(ed_raw)
            marked = [k for k, (letter, dotted) in enumerate(ed_letters)
                      if dotted and letter != "ג" and k < len(ed_letters) - 1]
            abbreviation = is_abbreviation(ed_raw)
            if any(dotted for _letter, dotted in ed_letters) and auto == ed:
                p2_seen["marked_words"] += 1
                if abbreviation or not marked or len(ed_letters) != len(auto):
                    p2_seen["not_scored"] += 1
                    marked = []
            if marked and auto == ed and len(ed_letters) == len(auto) and not abbreviation:
                want = {k: DOTTED[ed_letters[k][0]] for k in marked}
                for r in RANKINGS:
                    ranked, _roots = layers[r].lookup(auto)
                    if not ranked:
                        p2[r]["none"] += 1
                        if r == "frequency":
                            keep_sample("p2_none", {"edition": ed_raw, "automatic": auto_raw})
                        continue
                    top = plural_alif_dropped(ranked[0][0], ed_letters)
                    ok = len(top) == len(ed_letters) and all(top[k] == v for k, v in want.items())
                    p2[r]["right" if ok else "wrong"] += 1
                    if not ok and r == "frequency":
                        keep_sample("p2_wrong", {"edition": ed_raw, "automatic": auto_raw, "top": top,
                                                 "count": ranked[0][1]})
            if auto != ed:
                continue
            hits = blau_roots(auto)
            # P1 and P3 look the word up as the page writes it: a dot the page
            # marks (ד'לך) narrows the readings, as it would for a reader.
            if not hits:
                # P3: read right, not in Blau -- the layer's own population.
                ranked, found = layers["frequency"].lookup(auto_raw)
                p3["occurrences"] += 1
                p3["reading_shown" if ranked else "no_reading"] += 1
                if ranked and found:
                    p3["root_in_lane_or_lisan"] += 1
                if not ranked:
                    keep_sample("p3_no_reading", {"word": auto_raw})
                p3_words[auto_raw] += 1
                p3_context[auto_raw].append((" ".join(page[max(0, i - 4):i + 5]), sys_id, header))
                continue
            if len(hits) != 1:
                p1_seen["several_blau_roots"] += 1
                continue
            truth = clean_root.get(next(iter(hits)), "")
            # Blau's root is truth only when it is a root (3-4 letters, no ة, no
            # inner alef) and its strong letters (two at least) sit in the word in
            # order, in some reading: a form filed under another root (السلام under
            # خصص) is not evidence about this word.
            strong = strong_letters(truth)
            if not truth or len(strong) < 2 or not any(is_subsequence(strong, r) for r in readings(auto_raw)):
                p1_seen["truth_not_usable"] += 1
                continue
            p1_seen["scored"] += 1
            for r in RANKINGS:
                ranked, found = layers[r].lookup(auto_raw)
                outcome = "none" if not ranked or not found else ("right" if found[0] == truth else "wrong")
                p1[r][outcome] += 1
                if outcome == "wrong" and r == "frequency":
                    keep_sample("p1_wrong", {"word": auto_raw, "top": surface.get(ranked[0][0], ranked[0][0]),
                                             "root": found[0], "blau": truth,
                                             "context": " ".join(page[max(0, i - 4):i + 5])})
    examples = {}
    for word in EXAMPLES:
        ranked, found = layers["frequency"].lookup(word)
        examples[word] = {"readings": [(surface.get(r, r), c) for r, c in ranked], "roots": found[:3],
                          "lane": [lane.get(k, []) for k in found[:1]], "lisan": [lisan.get(k, []) for k in found[:1]],
                          "plain_first": rank_readings(word, counts, "plain")[:1]}
    result = {
        "inputs": {k: getattr(args, k) for k in ("blau_db", "pgp_db", "transcriptions", "sources", "root_method",
                                                 "ranking", "min_coverage", "min_words", "limit_docs", "seed",
                                                 "dev_file", "score_dev", "only_dev", "method_version",
                                                 "frequency_extra",
                                                 "sidecar")},
        "lane_root_keys": len(lane), "lisan_root_keys": len(lisan), "root_keys_either": len(roots),
        "root_keys_both": len(set(lane) & set(lisan)),
        "frequency_tokens": sum(counts.values()), "frequency_types": len(counts), "min_count": args.min_count,
        "documents_with_jrb_edition": len(editions), "dev_documents": len(dev), "dev_pages_skipped": dev_pages,
        "pages_seen": pages_seen, "pages_used": pages_used,
        "aligned_pairs": pairs_total,
        "P1_root_against_blau": {r: dict(c) for r, c in p1.items()}, "P1_population": dict(p1_seen),
        "P2_dot_against_edition": {r: dict(c) for r, c in p2.items()}, "P2_population": dict(p2_seen),
        "P3_words_blau_lacks": dict(p3), "P3_distinct_words": len(p3_words),
        "examples": examples, "samples": samples, "gate_thresholds": GATE,
        "seconds": round(time.monotonic() - started, 1),
    }
    if args.grading_sample:
        # Occurrences, not spellings: the draw is the one over every occurrence (sorted by word, so
        # the same words as before), and each row carries ITS occurrence's context and page.
        rng = random.Random(args.seed)
        population = sorted(p3_words.elements())
        chosen = rng.sample(range(len(population)), min(args.grading_sample, len(population)))
        rows = []
        for index in chosen:
            word = population[index]
            context, page_sys_id, page_header = p3_context[word][index - bisect.bisect_left(population, word)]
            ranked, found = layers["frequency"].lookup(word)
            rows.append({"word": word, "context": context, "sys_id": page_sys_id, "page": page_header,
                         "readings": " | ".join(f"{surface.get(r, r)} ({c})" for r, c in ranked),
                         "root": found[0] if found else "", "lane": " ".join(lane.get(found[0], [])) if found else "",
                         "lisan": " ".join(lisan.get(found[0], [])) if found else "", "grade": ""})
        result["grading_sample"] = rows
    return result


def _rate(counter: dict, key: str) -> float:
    total = sum(counter.values())
    return counter.get(key, 0) / total if total else 0.0


def gate(result: dict, ranking: str) -> list[str]:
    """Failures of the gate for the ranking under test (empty = pass)."""
    failures = []
    p1, p2 = result["P1_root_against_blau"], result["P2_dot_against_edition"]
    dot = _rate(p2[ranking], "right")
    if dot < GATE["G1_p2_dot_agreement_min"]:
        failures.append(f"G1 P2 dot agreement {dot:.1%} < {GATE['G1_p2_dot_agreement_min']:.0%}")
    margin = _rate(p1[ranking], "right") - _rate(p1["plain"], "right")
    if margin < GATE["G2_p1_right_margin_over_plain_min"]:
        failures.append(f"G2 P1 right {_rate(p1[ranking], 'right'):.1%} is {margin:+.1%} over the plain "
                        f"baseline (< {GATE['G2_p1_right_margin_over_plain_min']:+.0%})")
    wrong = _rate(p1[ranking], "wrong")
    if wrong > GATE["G3_p1_wrong_max"]:
        failures.append(f"G3 P1 wrong root {wrong:.1%} > {GATE['G3_p1_wrong_max']:.0%}")
    return failures


def summary(result: dict) -> str:
    lines = [f"Lane root keys {result['lane_root_keys']:,}, Lisan {result['lisan_root_keys']:,}, either "
             f"{result['root_keys_either']:,}, both {result['root_keys_both']:,}; frequency sample "
             f"{result['frequency_tokens']:,} tokens, {result['frequency_types']:,} skeletons",
             f"pages used {result['pages_used']:,} of {result['pages_seen']:,} ({result['dev_pages_skipped']:,} "
             f"pages of the {result['dev_documents']} development documents not scored); aligned pairs "
             f"{result['aligned_pairs']:,}; {result['seconds']} s"]
    for r in RANKINGS:
        p1, p2 = result["P1_root_against_blau"][r], result["P2_dot_against_edition"][r]
        shown = p2.get("right", 0) + p2.get("wrong", 0)
        lines.append(f"{r:>9}: P1 n={sum(p1.values()):,} right {_rate(p1, 'right'):.1%} wrong "
                     f"{_rate(p1, 'wrong'):.1%} none {_rate(p1, 'none'):.1%} | P2 n={sum(p2.values()):,} dot "
                     f"right {_rate(p2, 'right'):.1%} wrong {_rate(p2, 'wrong'):.1%} none {_rate(p2, 'none'):.1%}"
                     f" (right of shown {p2.get('right', 0) / shown if shown else 0:.1%})")
    p3 = result["P3_words_blau_lacks"]
    n = p3.get("occurrences", 0)
    lines.append(f"P3 words Blau lacks: {n:,} occurrences ({result['P3_distinct_words']:,} distinct); a reading "
                 f"shown {p3.get('reading_shown', 0) / n if n else 0:.1%}, its root in Lane or Lisan "
                 f"{p3.get('root_in_lane_or_lisan', 0) / n if n else 0:.1%}")
    for word, ex in result["examples"].items():
        lines.append(f"  {word}: {ex['readings']} root {ex['roots'][:1]} lane {ex['lane']} lisan {ex['lisan']}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Fetching the public sources
# ---------------------------------------------------------------------------

def _download(url: str, dest: Path, sha256: str = "") -> None:
    data = urllib.request.urlopen(url, timeout=300).read()  # noqa: S310 -- fixed https URLs only
    if sha256 and hashlib.sha256(data).hexdigest() != sha256:
        raise ValueError(f"sha256 mismatch for {url}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)


def pick_sample(metadata_tsv: str) -> list[dict]:
    """The seeded OpenITI pick: primary, non-OCR Arabic texts dated 250-650 AH of
    20,000-1,500,000 tokens, no dictionaries; at most ``SAMPLE_PER_TAG`` per
    first genre tag, ``SAMPLE_CAP_TOKENS`` in all. The repositories' file names
    are resolved by ``fetch`` (or read from the pinned manifest)."""
    csv.field_size_limit(10 ** 8)
    rows = []
    with open(metadata_tsv, encoding="utf-8") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r["language"] != "ara" or r["status"] != "pri" or r["uncorrected_OCR"] != "False":
                continue
            try:
                date, tokens = int(r["date"]), int(r["tok_length"])
            except ValueError:
                continue
            tags = [t for t in r["tags"].split(" :: ") if t.startswith("_")]
            if not (250 <= date <= 650 and 20000 <= tokens <= 1500000) or not tags:
                continue
            if any(t in ("_MACAJIM", "_GHARIB") for t in tags) or "lexicograph" in r["tags"].lower():
                continue
            rows.append({"version_uri": r["version_uri"], "date": r["date"], "tag": tags[0], "tok_length": tokens,
                         "local_path": r["local_path"], "repo": f"{math.ceil(date / 25) * 25:04d}AH"})
    rows.sort(key=lambda r: r["version_uri"])
    random.Random(SAMPLE_SEED).shuffle(rows)
    per: Counter = Counter()
    picked, total = [], 0
    for r in rows:
        if per[r["tag"]] >= SAMPLE_PER_TAG or total + r["tok_length"] > SAMPLE_CAP_TOKENS:
            continue
        per[r["tag"]] += 1
        picked.append(r)
        total += r["tok_length"]
    return picked


def fetch(dest: str) -> int:
    root = Path(dest)
    if root.exists() and any(root.iterdir()):
        print(f"--dest must be an empty directory: {dest}", file=sys.stderr)
        return 2
    manifest = json.loads(SAMPLE_MANIFEST.read_text(encoding="utf-8"))
    names = [entry["name"] for entry in manifest["lane_files"]]
    for entry in manifest["lane_files"]:
        _download(f"https://raw.githubusercontent.com/{LANE_REPO}/{LANE_COMMIT}/{entry['name']}",
                  root / "lane" / entry["name"], entry["sha256"])
    _download(f"https://raw.githubusercontent.com/{LISAN_REPO}/{LISAN_COMMIT}/{LISAN_PATH}",
              root / "lisan" / os.path.basename(LISAN_PATH), LISAN_SHA256)
    for text in manifest["texts"]:
        url = f"https://raw.githubusercontent.com/OpenITI/{text['repo']}/{text['commit']}/{text['path']}"
        _download(url, root / "openiti" / (os.path.basename(text["path"]).replace(".", "_") + ".txt"), text["sha256"])
    print(f"fetched {len(names)} Lane files, Lisan, {len(manifest['texts'])} OpenITI texts into {dest}")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    f = sub.add_parser("fetch", help="download Lane, Lisan and the pinned OpenITI sample")
    f.add_argument("--dest", required=True)
    p = sub.add_parser("pick", help="print the seeded OpenITI pick from the release metadata TSV")
    p.add_argument("--metadata", required=True)
    m = sub.add_parser("measure", help="run the measurement and the gate")
    m.add_argument("--sources", default="", help="the folder fetch filled (lane/, lisan/, openiti/)")
    m.add_argument("--sidecar", default="", help="measure the built arabic_data/arabic.db instead (Lane only)")
    m.add_argument("--min-count", type=int, default=MIN_COUNT,
                   help="a reading is offered only at this count or more in the word count")
    m.add_argument("--pgp-db", default=str(REPO / "pgp_data" / "pgp.db"))
    m.add_argument("--transcriptions", default=str(REPO / "Transcriptions.txt"))
    m.add_argument("--blau-db", default=str(REPO / "blau_data" / "blau.db"))
    m.add_argument("--frequency-cache", default="", help="gzip JSON of the counts (written when absent)")
    m.add_argument("--root-method", choices=("strip", "camel"), default="strip")
    m.add_argument("--method-version", choices=("v1", "v2", "v3", "v4"), default="v1",
                   help="v1: as the gate first judged it; v2: plural alif + stem backoff; v3: v2 + roots "
                        "from Lane's headwords (plan, section 6)")
    m.add_argument("--frequency-extra", action="append", default=[],
                   help="another folder of texts to count (sample-size sensitivity; use its own cache)")
    m.add_argument("--ranking", choices=RANKINGS, default="frequency", help="the ranking the gate judges")
    m.add_argument("--min-coverage", type=float, default=0.5)
    m.add_argument("--min-words", type=int, default=15)
    m.add_argument("--limit-docs", type=int, default=0, help="a seeded sample of documents (0 = all)")
    m.add_argument("--seed", type=int, default=20261104)
    m.add_argument("--dev-file", default=str(DEV_DOCUMENTS_FILE),
                   help="the pinned documents the method was shaped on; not scored (see --score-dev)")
    m.add_argument("--score-dev", action="store_true", help="score the development documents too")
    m.add_argument("--only-dev", action="store_true", help="score the development documents only")
    m.add_argument("--samples", type=int, default=15)
    m.add_argument("--grading-sample", type=int, default=0, help="P3 words to write for a person to grade")
    m.add_argument("--out", default="")
    args = parser.parse_args(argv)
    if args.command == "fetch":
        return fetch(args.dest)
    if args.command == "pick":
        for row in pick_sample(args.metadata):
            print(json.dumps(row, ensure_ascii=False))
        return 0
    if bool(args.sources) == bool(args.sidecar):
        print("give exactly one of --sources and --sidecar", file=sys.stderr)
        return 2
    for name in ("pgp_db", "transcriptions", "blau_db"):
        path = getattr(args, name)
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            print(f"missing or empty input: {path}", file=sys.stderr)
            return 2
    result = run(args)
    failures = gate(result, args.ranking)
    result["gate"] = {"ranking": args.ranking, "failures": failures, "passed": not failures}
    print(summary(result))
    print(f"GATE ({args.ranking}, {args.root_method}, {args.method_version}): " + ("PASS" if not failures else "FAIL: " + "; ".join(failures)))
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"written: {args.out}")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
