# -*- coding: utf-8 -*-
"""Read-only lookups in the Arabic dictionary sidecar (``arabic_data/arabic.db``): Lane's
*Arabic-English Lexicon* for a Judeo-Arabic or an Arabic word.

Phase B of docs/plans/ARABIC_DICTIONARY_LAYER_PLAN.md. scripts/build_arabic_sidecar.py builds the
file and scripts/verify_arabic_sidecar.py gates it; this module only reads it, decides nothing
about its content and never writes.

Fail-closed, as shared/blau_service.py. The service is READY only when the file is where a sidecar
lives, opens read-only, passes ``PRAGMA quick_check``, carries the schema version this reader
understands, was keyed by the normalizer this process runs (``meta.normalizer_version`` ==
``shared.arabic_normalize.NORMALIZER_VERSION``; otherwise every lookup would silently miss), holds
every table and lookup index a lookup reads, and carries the Perseus credit line (D3: a Lane card
without it must not show). Any of those failing leaves ``is_ready()`` False with the reason in
``unavailable_reason``, and ``lookup`` answers an empty result. ``is_available()`` is readiness AND
the ``LANE_DICTIONARY_ENABLED`` kill switch (default on, read live). Nothing here raises into a
page.

The lookup is the plan's method v4 (``shared.arabic_method``), the one the gate measured on this
file: a word in Hebrew letters is read as every Arabic skeleton it may be, the readings attested in
the OpenITI word count are ranked commonest first (``MAX_READINGS`` kept), and the first reading's
roots are found through the closed-class table, Lane's headwords, then stripping. A word typed in
Arabic letters is its own reading. No guessing at misread letters, and no judgement of the word's
language (owner, D11): a reader who asks for Arabic gets the Arabic readings.

Each root card holds Lane's entries under that root: the entries whose headword is the reading, or
the reading with a proclitic and a pronoun peeled, or the closed-class table's headword, first;
then the rest in Lane's order. Everything is bounded (``MAX_ROOTS``, ``MAX_ENTRIES``, ``MAX_TEXT``)
so one click never renders a page; what is left out is counted and linked on Ephraim Meiri's site,
where the entries come from (D5).
"""
from __future__ import annotations

import logging
import os
import re
import sqlite3
import threading
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import quote

from shared.arabic_method import (
    BACKOFF_ENCLITICS,
    BACKOFF_PROCLITICS,
    MIN_COUNT,
    rank_readings,
    reading_roots,
)
from shared.arabic_normalize import (
    NORMALIZER_VERSION,
    arabic_tokens,
    is_abbreviation,
    ja_letters,
    readings,
    skeleton,
    strip_harakat,
)
from shared.blau_normalize import BRACKETS, QUOTE_MARKS
from shared.thread_local_db import ThreadLocalConnection

logger = logging.getLogger(__name__)

#: The sidecar schema this reader understands; scripts/build_arabic_sidecar.py writes its own
#: ``SCHEMA_VERSION`` into ``meta.version`` and tests/test_arabic_service.py pins the two equal.
SCHEMA_VERSION = "1.0.0"
SIDECAR_DIR = "arabic_data"
SIDECAR_FILENAME = "arabic.db"
#: Ephraim Meiri's site, where the Lane entries come from (D5): an entry is ``?id=<entry id>``.
SITE_URL = "https://semitic.lang.emeiri.org/"
REQUIRED_TABLES = ("meta", "lane_roots", "lane_entries", "lane_forms", "freq", "closed_class")
#: Every table whose row count the builder records in ``meta.rows_<table>``: a table that lost rows
#: (an empty closed-class table sends דלך to the wrong root) is not served.
COUNTED_TABLES = ("lane_roots", "lane_entries", "lane_forms", "freq", "closed_class")
#: The Perseus credit line, verbatim (D3); scripts/build_arabic_sidecar.py checks the source against
#: the same words and tests/test_arabic_service.py pins the two copies equal.
PERSEUS_CREDIT = ("Text provided by Perseus Digital Library, with funding from The U.S. Department of "
                  "Education and The Max Planck Society.")
LOOKUP_INDEXES = ("idx_lane_entries_root", "idx_lane_forms_skeleton")
#: The method the gate measured (plan section 7.1) and the floor it measured at (D11: 2).
METHOD_VERSION = "v4"
MAX_READINGS = 3
MAX_ROOTS = 1  # the gate measured the first root only (P1)
MAX_ENTRIES = 3
MAX_TEXT = 600


def lane_enabled() -> bool:
    """The ``LANE_DICTIONARY_ENABLED`` kill switch, read on every call (default on)."""
    value = os.environ.get("LANE_DICTIONARY_ENABLED")
    if value is None:
        return True
    return value.strip().lower() in {"1", "true", "yes", "on"}


def entry_url(entry_id: str) -> str:
    """One Lane entry on Ephraim Meiri's site."""
    return f"{SITE_URL}?id={quote(str(entry_id), safe='')}"


def root_url(root: str) -> str:
    """Lane's entries under a root on Ephraim Meiri's site (a headword search in Lane)."""
    return f"{SITE_URL}?q={quote(str(root), safe='')}&source=lane"


def default_sidecar_path() -> Optional[str]:
    """Where the sidecar lives: the user's updated copy first (as for the Blau, FGP and PGP
    sidecars), then the repository's ``arabic_data/arabic.db``."""
    local = os.environ.get("LOCALAPPDATA", "")
    if local:
        user_path = os.path.join(local, "GenizahSearchPro", "data", SIDECAR_DIR, SIDECAR_FILENAME)
        if os.path.isfile(user_path):
            return user_path
    return str(Path(__file__).resolve().parents[1] / SIDECAR_DIR / SIDECAR_FILENAME)


# ---------------------------------------------------------------------------
# What a lookup answers: plain frozen records a renderer draws without a decision of its own.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LaneReading:
    """One Arabic reading of the word: its skeleton, its commonest spelling in the word count
    (with the hamza and the ة the skeleton folds away) and its count."""
    skeleton: str
    spelling: str
    count: int


@dataclass(frozen=True)
class LaneEntry:
    """One Lane entry: the headword, the printed page, the text (cut at ``MAX_TEXT``)."""
    entry_id: str
    headword: str
    page: Optional[int]
    text: str
    cut: bool
    url: str
    #: Its headword is the reading (or the reading's stem, or the closed-class table's headword).
    matched: bool


@dataclass(frozen=True)
class LaneRoot:
    """One root the first reading may come from, with its entries."""
    root_key: str
    name: str
    page: Optional[int]
    url: str
    entries: tuple[LaneEntry, ...]
    more_entries: int


@dataclass(frozen=True)
class LaneLookup:
    word: str
    #: "ja" (Hebrew letters) or "arabic" (a typed Arabic-script word).
    script: str
    readings: tuple[LaneReading, ...]
    roots: tuple[LaneRoot, ...]

    @property
    def found(self) -> bool:
        return bool(self.roots)

    @property
    def reading(self) -> Optional[LaneReading]:
        """The reading the root cards are for: the first."""
        return self.readings[0] if self.readings else None


#: Marks written inside one Judeo-Arabic word: the quote marks ``ja_letters`` reads as the dot,
#: and the editorial brackets of a restored letter (ד[ל]ך). Nonspacing marks (the combining dot,
#: niqqud) are dropped too.
_IN_WORD_MARKS = QUOTE_MARKS | BRACKETS


def one_ja_word(word: str) -> bool:
    """``word`` is ONE Judeo-Arabic word: no space at all, and, NFC-normalized as ``ja_letters``
    reads it, with the in-word marks dropped, a single run of Hebrew letters. A space, a maqaf
    (כל־מה) or a hyphen between letters makes two, which ``ja_letters`` would read as one (كلمة)."""
    if len(word.split()) != 1:
        return False
    text = unicodedata.normalize("NFC", word)
    kept = "".join(ch for ch in text if ch not in _IN_WORD_MARKS and unicodedata.category(ch) != "Mn")
    if any(ch.isalpha() and not "א" <= ch <= "ת" for ch in kept):
        return False  # another script joined on (דלך-كتاب): two words
    return len(re.findall("[א-ת]+", kept)) == 1


def empty_lane(word: str = "", script: str = "ja") -> LaneLookup:
    return LaneLookup(word=word, script=script, readings=(), roots=())


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _page(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None and str(value).strip() != "" else None
    except (TypeError, ValueError):
        return None


def cut_text(text: str, limit: int = MAX_TEXT) -> tuple[str, bool]:
    """``text`` up to ``limit`` characters, ended at a space where one is near, and whether
    anything was cut."""
    text = " ".join(text.split())
    if len(text) <= limit:
        return text, False
    head = text[:limit]
    space = head.rfind(" ")
    if space > limit * 0.8:
        head = head[:space]
    return head.rstrip(" ,;:") + "…", True


class _FreqView:
    """The word count for one lookup: ``.get`` like a Counter, read from ``freq`` by key and kept
    for the lookup's life. ``prefetch`` reads many keys in one query."""

    def __init__(self, conn: Any):
        self._conn = conn
        self._rows: dict[str, Optional[tuple[int, str]]] = {}

    def __bool__(self) -> bool:  # shared.arabic_method treats a falsy count as "no count"
        return True

    def prefetch(self, keys: Iterable[str]) -> None:
        wanted = [k for k in dict.fromkeys(keys) if k and k not in self._rows]
        for start in range(0, len(wanted), 500):
            chunk = wanted[start:start + 500]
            for k in chunk:
                self._rows[k] = None
            marks = ",".join("?" * len(chunk))
            for key, count, surface in self._conn.execute(
                    f"SELECT skeleton, count, surface FROM freq WHERE skeleton IN ({marks})", chunk):
                self._rows[key] = (int(count), _text(surface))

    def _row(self, key: str) -> Optional[tuple[int, str]]:
        if key not in self._rows:
            self.prefetch([key])
        return self._rows[key]

    def get(self, key: str, default: int = 0) -> int:
        row = self._row(key)
        return row[0] if row is not None else default

    def spelling(self, key: str) -> str:
        row = self._row(key)
        return row[1] if row is not None and row[1] else key


class ArabicService:
    """Read-only access to the Arabic sidecar. See the module docstring."""

    def __init__(self, db_path: Optional[str] = None, thread_safe: bool = False):
        self._conn: Any = None
        self._meta: dict[str, str] = {}
        self._roots: set[str] = set()
        self._prior: Counter = Counter()
        self._forms: dict[str, set[str]] = {}
        self._closed: dict[str, list[str]] = {}
        self.unavailable_reason = "not opened"
        self._db_path = db_path or default_sidecar_path()
        if not self._db_path:
            self.unavailable_reason = "no sidecar path"
            return
        if not os.path.isfile(self._db_path):
            self.unavailable_reason = f"no sidecar at {self._db_path}"
            logger.info("ArabicService: %s", self.unavailable_reason)
            return
        conn = None
        try:
            uri = f"file:{Path(self._db_path).resolve().as_posix()}?mode=ro"
            if thread_safe:
                conn = ThreadLocalConnection(uri, timeout=10.0)
            else:
                conn = sqlite3.connect(uri, uri=True, check_same_thread=False, timeout=10.0)
            problem, meta = _validate(conn)
            if not problem:
                self._load_tables(conn)
        except Exception as exc:  # a file that is not a database, a locked file, ...
            problem, meta = f"cannot open: {exc}", {}
        if problem:
            self.unavailable_reason = problem
            logger.warning("ArabicService: %s is not served: %s", self._db_path, problem)
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            return
        self._conn = conn
        self._meta = meta
        self.unavailable_reason = ""
        logger.info("ArabicService: serving %s (schema %s, %s Lane entries, %s frequency rows)",
                    self._db_path, meta.get("version"), meta.get("rows_lane_entries"), meta.get("rows_freq"))

    def _load_tables(self, conn: Any) -> None:
        """The small tables the method reads on every lookup, held in memory (as the gate's
        ``load_sidecar`` reads them): root keys, the article size per root (the tie-break prior),
        the headword table and the closed-class table. The word count stays on disk."""
        self._roots = {r[0] for r in conn.execute("SELECT root_key FROM lane_roots")}
        self._prior = Counter(dict(conn.execute(
            "SELECT root_key, SUM(LENGTH(text)) FROM lane_entries GROUP BY root_key").fetchall()))
        forms: dict[str, set[str]] = defaultdict(set)
        for form, key in conn.execute("SELECT skeleton, root_key FROM lane_forms"):
            forms[form].add(key)
        self._forms = dict(forms)
        closed: dict[str, list[str]] = defaultdict(list)
        for key, rkey in conn.execute("SELECT skeleton, root_key FROM closed_class ORDER BY ordinal"):
            if rkey not in closed[key]:
                closed[key].append(rkey)
        self._closed = dict(closed)

    # -- state -------------------------------------------------------------

    @property
    def db_path(self) -> Optional[str]:
        return self._db_path

    def is_ready(self) -> bool:
        """The sidecar is open and passed every check."""
        return self._conn is not None

    def is_available(self) -> bool:
        """Ready AND the kill switch is on. The only question a surface asks."""
        return self.is_ready() and lane_enabled()

    def meta(self, key: str) -> str:
        return self._meta.get(key, "")

    @property
    def credit(self) -> str:
        """The Perseus credit line (D3), verbatim; shown on every Lane card."""
        return self._meta.get("lane_credit", "")

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
            self.unavailable_reason = "closed"

    # -- the lookup ----------------------------------------------------------

    def lookup(self, word: str) -> LaneLookup:
        """What Lane has for one clicked or typed word. Hebrew letters are read as Judeo-Arabic;
        Arabic letters only are their own reading. Never raises; empty when unavailable, when the
        word is an abbreviation or has neither script, or on any error."""
        word = _text(word)
        if not word or not self.is_available():
            return empty_lane(word)
        script = "ja"
        try:
            counts = _FreqView(self._conn)
            if ja_letters(word):
                # One word only: ja_letters reads past a space or a maqaf, so "כל מה" would be كلمة.
                if is_abbreviation(word) or not one_ja_word(word):
                    return empty_lane(word)
                counts.prefetch(readings(word, plural_alif=True))
                ranked = rank_readings(word, counts, "frequency", METHOD_VERSION, MIN_COUNT)[:MAX_READINGS]
                found = tuple(LaneReading(r, counts.spelling(r), c) for r, c in ranked)
            else:
                script = "arabic"
                tokens = list(arabic_tokens(word))
                key = skeleton(tokens[0]) if len(tokens) == 1 else ""
                if not key:
                    return empty_lane(word, script)
                found = (LaneReading(key, strip_harakat(tokens[0]), counts.get(key, 0)),)
            if not found:
                return LaneLookup(word=word, script=script, readings=(), roots=())
            first = found[0]
            keys = reading_roots(first.skeleton, surface={first.skeleton: first.spelling}, roots=self._roots,
                                 prior=self._prior, forms=self._forms, counts=counts, closed=self._closed,
                                 version=METHOD_VERSION)
            roots = tuple(self._root(key, first.skeleton) for key in keys[:MAX_ROOTS])
            return LaneLookup(word=word, script=script, readings=found,
                              roots=tuple(r for r in roots if r is not None))
        except Exception:
            logger.exception("ArabicService: lookup of %r failed", word)
            return empty_lane(word, script)

    def _matched_entries(self, root_key: str, reading: str) -> set[str]:
        """The entries of ``root_key`` whose headword is the reading, or the reading with a
        proclitic and a pronoun peeled (as ``form_roots`` peels them), or whose closed-class row
        names them."""
        stems = {reading}
        for pro in BACKOFF_PROCLITICS:
            if not reading.startswith(pro):
                continue
            for enc in BACKOFF_ENCLITICS:
                if reading.endswith(enc):
                    stem = reading[len(pro):len(reading) - len(enc)]
                    if len(stem) >= 2:
                        stems.add(stem)
        marks = ",".join("?" * len(stems))
        ids = {r[0] for r in self._conn.execute(
            f"SELECT entry_id FROM lane_forms WHERE root_key = ? AND skeleton IN ({marks})",
            (root_key, *sorted(stems)))}
        closed = {reading} | {reading[1:] for pro in ("و", "ف") if reading.startswith(pro)}
        marks = ",".join("?" * len(closed))
        ids |= {r[0] for r in self._conn.execute(
            f"SELECT entry_id FROM closed_class WHERE root_key = ? AND entry_id IS NOT NULL "
            f"AND skeleton IN ({marks})", (root_key, *sorted(closed)))}
        return ids

    def _root(self, root_key: str, reading: str) -> Optional[LaneRoot]:
        row = self._conn.execute("SELECT name, first_page FROM lane_roots WHERE root_key = ?",
                                 (root_key,)).fetchone()
        if row is None:
            return None
        listing = self._conn.execute("SELECT id FROM lane_entries WHERE root_key = ? ORDER BY ordinal",
                                     (root_key,)).fetchall()
        matched = self._matched_entries(root_key, reading)
        order = [r[0] for r in listing]
        chosen = [i for i in order if i in matched] + [i for i in order if i not in matched]
        shown = chosen[:MAX_ENTRIES]
        entries = []
        for entry_id in shown:
            e = self._conn.execute("SELECT headword, page, text FROM lane_entries WHERE id = ?",
                                   (entry_id,)).fetchone()
            text, cut = cut_text(_text(e[2]))
            entries.append(LaneEntry(entry_id=entry_id, headword=_text(e[0]), page=_page(e[1]), text=text,
                                     cut=cut, url=entry_url(entry_id), matched=entry_id in matched))
        name = _text(row[0])
        return LaneRoot(root_key=root_key, name=name, page=_page(row[1]), url=root_url(name),
                        entries=tuple(entries), more_entries=max(0, len(order) - len(shown)))


def _validate(conn: Any) -> tuple[str, dict[str, str]]:
    """'' and the meta when the open file is one this reader serves; else the reason."""
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    missing = [t for t in REQUIRED_TABLES if t not in tables]
    if missing:
        return f"missing table(s): {', '.join(missing)}", {}
    meta = {str(k): ("" if v is None else str(v)) for k, v in conn.execute("SELECT key, value FROM meta")}
    if meta.get("version") != SCHEMA_VERSION:
        return f"schema version {meta.get('version')!r}; this reader understands {SCHEMA_VERSION}", {}
    if meta.get("normalizer_version") != str(NORMALIZER_VERSION):
        return (f"keyed by normalizer {meta.get('normalizer_version')!r}; this process runs "
                f"{NORMALIZER_VERSION}: rebuild the sidecar"), {}
    if meta.get("lane_credit") != PERSEUS_CREDIT:
        return "meta.lane_credit is not the Perseus credit line", {}
    # The readings are offered from MIN_COUNT up (D11); a sidecar that stores only higher counts
    # would drop readings the gate measured.
    floor = meta.get("freq_min_stored", "")
    if not floor.isdigit() or not 1 <= int(floor) <= MIN_COUNT:
        return f"meta.freq_min_stored = {floor!r}; this reader offers readings counted {MIN_COUNT} or more", {}
    indexes = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}
    absent = [i for i in LOOKUP_INDEXES if i not in indexes]
    if absent:
        return f"lookup index(es) missing: {', '.join(absent)}", {}
    for table in COUNTED_TABLES:
        expected = meta.get(f"rows_{table}", "")
        actual = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if not expected.isdigit() or int(expected) != int(actual):
            return f"meta.rows_{table} = {expected!r} but the table holds {actual} rows", {}
    check = conn.execute("PRAGMA quick_check").fetchone()
    if check is None or check[0] != "ok":
        return f"quick_check: {check[0] if check else 'no answer'}", {}
    return "", meta


# ---------------------------------------------------------------------------
# The process-wide instance (one per app, like the other sidecar services)
# ---------------------------------------------------------------------------

_default_service: Optional[ArabicService] = None
_lock = threading.Lock()


def get_arabic_service(thread_safe: bool = True) -> ArabicService:
    """The default service, created on first use (per-thread connections by default: safe under
    NiceGUI's ``run.io_bound`` pool)."""
    global _default_service
    with _lock:
        if _default_service is None:
            _default_service = ArabicService(thread_safe=thread_safe)
        return _default_service


def reset_arabic_service() -> None:
    """Forget the default service (after the sidecar file is replaced)."""
    global _default_service
    with _lock:
        if _default_service is not None:
            _default_service.close()
            _default_service = None
