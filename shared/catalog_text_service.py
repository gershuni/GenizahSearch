# -*- coding: utf-8 -*-
"""Read-only search of the catalogue text sidecar (``catalog_data/catalog_text.db``).

Plan: docs/plans/CATALOGUE_TEXT_SEARCH_PLAN.md (§4.3, §4.5). scripts/build_catalog_text_sidecar.py
builds the file and scripts/verify_catalog_text_sidecar.py gates it. This module only reads it.

Fail-closed, like shared/arabic_service.py. The service is READY only when the file is where a
sidecar lives, opens read-only, carries the schema version this reader understands, was keyed by
the normalizer this process runs (``meta.normalizer_version`` ==
``shared.catalog_text_normalize.NORMALIZER_VERSION``; otherwise every search would silently miss),
holds every table it reads, and holds as many records as its ``meta`` says. Any of those failing
leaves ``is_ready()`` False with the reason in ``unavailable_reason``, and ``search`` answers an
empty result. ``is_available()`` is readiness AND the ``CATALOG_TEXT_SEARCH_ENABLED`` flag
(default OFF until the file is on the server, decision 0004). Nothing here raises into a page.
``PRAGMA quick_check`` is the gate's job, not startup's: on a 700 MB file it costs seconds.

A search answers one hit per manuscript, ranked (plan §4.3, owner D3):

1. manuscripts found by the words as typed come before those found only through a Hebrew prefix
   (כתובה before הכתובה), and, with ``near_spellings``, those before the ones found only through a
   near spelling (כתבה, saadya; plan §4.7, ``shared/near_spellings.py``);
2. within each group, by the manuscript's best record: FTS5 bm25, with a title worth more than a
   description and a description more than a full catalogue entry (``COLUMN_WEIGHTS``), and an
   automatic translation worth less than an original (``AUTOMATIC_FACTOR``).

``include_translations=False`` leaves the automatic translations out (D2's switch). The count is
exact: every matching manuscript is ranked, and ``limit`` only bounds what is returned.

A near spelling counts only when the file holds it: each candidate is looked up in the FTS5
vocabulary (``fts5vocab``, made in the connection's own temp schema, so the read-only file is
unchanged and every thread's connection has its own).
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Optional

from shared.catalog_text_normalize import (
    NORMALIZER_VERSION,
    Term,
    can_expand,
    fts_match,
    match_spans,
    parse_query,
)
from shared.near_spellings import near_spellings as _near_spellings_of
from shared.thread_local_db import ThreadLocalConnection

logger = logging.getLogger(__name__)

#: The sidecar schema this reader understands; tests/test_catalog_text_service.py pins it equal
#: to scripts/build_catalog_text_sidecar.py's ``SCHEMA_VERSION``.
SCHEMA_VERSION = "1.0.0"
SIDECAR_DIR = "catalog_data"
SIDECAR_FILENAME = "catalog_text.db"
REQUIRED_TABLES = ("meta", "records", "catalog_fts")
#: bm25 weights of the FTS columns, in the builder's order: title, description, fulltext.
COLUMN_WEIGHTS = (3.0, 1.5, 1.0)
#: bm25 scores are negative (lower is better); an automatic translation's score is multiplied by
#: this, which moves it towards zero, behind an original that matches as well.
AUTOMATIC_FACTOR = 0.8
#: Characters of original text a snippet shows around its first marked word.
SNIPPET_CHARS = 200
#: The default bound on returned hits (the count is always exact).
DEFAULT_LIMIT = 5000
#: The records of one manuscript ``matches`` returns at most (the count is always exact). A
#: manuscript has a few dozen records; a common word may match many of them.
MATCHES_LIMIT = 30
#: At most this many words of a query get near spellings (the first ones); the rest are searched
#: as typed. Each costs up to a few tenths of a second (shared/near_spellings.py).
MAX_NEAR_WORDS = 4
#: Words whose near spellings in the file are remembered (small tuples, not the candidates).
NEAR_CACHE_SIZE = 2048
#: The inputs whose drift is reported at startup (plan §4.6): this file's ``meta`` key and a label.
#: PGP's schema version rarely changes with a refresh, its upstream commit always does.
DRIFT_KEYS = (
    ("input_fjms_version", "FJMS sidecar version"),
    ("input_pgp_upstream_commit", "PGP sidecar upstream commit"),
)


def catalog_text_enabled(default: bool = False) -> bool:
    """The ``CATALOG_TEXT_SEARCH_ENABLED`` flag, read on every call. Unset, it is *default*:
    OFF for the website (decision 0004: the file goes up before the flag), ON for the desktop
    app, whose installer carries the file (plan D5; desktop/catalog_search.py)."""
    value = os.environ.get("CATALOG_TEXT_SEARCH_ENABLED")
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def default_sidecar_path() -> Optional[str]:
    """Where the sidecar lives: the user's updated copy first (as for the other sidecars), then
    the repository's ``catalog_data/catalog_text.db``."""
    local = os.environ.get("LOCALAPPDATA", "")
    if local:
        user_path = os.path.join(local, "GenizahSearchPro", "data", SIDECAR_DIR, SIDECAR_FILENAME)
        if os.path.isfile(user_path):
            return user_path
    return str(Path(__file__).resolve().parents[1] / SIDECAR_DIR / SIDECAR_FILENAME)


@dataclass(frozen=True)
class CatalogHit:
    """One manuscript and the record that ranked it."""
    sys_id: str
    record_id: int
    source: str          # fjms | pgp | libraries
    field: str           # e.g. FreeDesc, RunningTitle, description, title
    lang: str            # he | en
    automatic: bool      # True when the record is an automatic translation
    score: float         # lower is better
    exact: bool          # found by the words as typed (False: only through a prefix or a near spelling)
    near: bool = False   # found only through a near spelling


@dataclass(frozen=True)
class CatalogResult:
    query: str
    terms: tuple[Term, ...]      # with ``near_spellings``, each word's near spellings the file holds
    hits: tuple[CatalogHit, ...]
    total: int                   # every matching manuscript, whatever ``limit`` returned
    include_translations: bool
    near_spellings: bool = False

    @property
    def truncated(self) -> bool:
        return self.total > len(self.hits)

    @property
    def near_forms(self) -> tuple[str, ...]:
        """Every near spelling searched, commonest in the file first per word, each once."""
        return tuple(dict.fromkeys(f for t in self.terms for f in t.near))


@dataclass(frozen=True)
class CatalogMatch:
    """One catalogue record of a manuscript that a query matches: where a catalogue search found
    it (``CatalogTextService.matches``)."""
    record_id: int
    source: str          # fjms | pgp | libraries
    field: str
    lang: str
    automatic: bool
    text: str            # the record's original text, whole
    spans: tuple[tuple[int, int], ...]   # the matched words in ``text`` (match_spans)
    exact: bool          # matched by the words as typed
    near: bool = False   # matched only through a near spelling


def _empty(query: str, include_translations: bool, terms: tuple[Term, ...] = (),
           near_spellings: bool = False) -> CatalogResult:
    return CatalogResult(query, terms, (), 0, include_translations, near_spellings)


class CatalogTextService:
    """Read-only access to the catalogue text sidecar. See the module docstring."""

    def __init__(self, db_path: Optional[str] = None, thread_safe: bool = False):
        self._conn: Any = None
        self._meta: dict[str, str] = {}
        # Words -> their near spellings in this file (``_near_in_file``); searches run in several
        # threads at once (run.io_bound), so the cache is read and written under its lock.
        self._near_cache: dict[str, tuple[str, ...]] = {}
        self._near_lock = threading.Lock()
        self.unavailable_reason = "not opened"
        self._db_path = db_path or default_sidecar_path()
        if not self._db_path:
            self.unavailable_reason = "no sidecar path"
            return
        if not os.path.isfile(self._db_path):
            self.unavailable_reason = f"no sidecar at {self._db_path}"
            logger.info("CatalogTextService: %s", self.unavailable_reason)
            return
        conn = None
        try:
            uri = f"file:{Path(self._db_path).resolve().as_posix()}?mode=ro"
            if thread_safe:
                conn = ThreadLocalConnection(uri, timeout=10.0)
            else:
                conn = sqlite3.connect(uri, uri=True, check_same_thread=False, timeout=10.0)
            problem, meta = _validate(conn)
        except Exception as exc:  # not a database, locked, ...
            problem, meta = f"cannot open: {exc}", {}
        if problem:
            self.unavailable_reason = problem
            logger.warning("CatalogTextService: %s is not served: %s", self._db_path, problem)
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            return
        self._conn = conn
        self._meta = meta
        self.unavailable_reason = ""
        logger.info("CatalogTextService: serving %s (schema %s, %s records, %s manuscripts)",
                    self._db_path, meta.get("version"), meta.get("rows_total"), meta.get("manuscripts_total"))

    # -- state ---------------------------------------------------------------

    @property
    def db_path(self) -> Optional[str]:
        return self._db_path

    def is_ready(self) -> bool:
        """The sidecar is open and passed every check."""
        return self._conn is not None

    def is_available(self) -> bool:
        """Ready AND the flag is on. The only question a surface asks."""
        return self.is_ready() and catalog_text_enabled()

    def meta(self, key: str) -> str:
        return self._meta.get(key, "")

    def input_drift(self, installed: dict[str, Optional[str]]) -> list[str]:
        """How the inputs this file was built from differ from the sidecars installed now (plan
        §4.6): one line per ``DRIFT_KEYS`` entry whose installed value is known and differs. The
        file is still served (a slightly old catalogue is still useful); the caller logs it."""
        out = []
        for meta_key, label in DRIFT_KEYS:
            now = installed.get(meta_key)
            built = self.meta(meta_key)
            if now and built and str(now) != built:
                out.append(f"{label}: the catalogue file was built from {built}, the installed one is {now}")
        return out

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = None
        self.unavailable_reason = "closed"

    # -- search --------------------------------------------------------------

    def search(self, query: str, include_translations: bool = True, limit: int = DEFAULT_LIMIT,
               near_spellings: bool = False) -> CatalogResult:
        """The manuscripts whose catalogue records hold every word of ``query`` (with
        ``near_spellings``, or a near spelling of it). Never raises."""
        terms = tuple(parse_query(query or ""))
        if not terms or self._conn is None:
            return _empty(query, include_translations, terms, near_spellings)
        try:
            if near_spellings:
                terms = tuple(self._with_near_spellings(terms))
            expand = can_expand(list(terms))
            exact = self._ranked(fts_match(list(terms), expand_prefixes=False), include_translations)
            ranked = list(exact)
            found = {hit.sys_id for hit in exact}
            if expand:
                for hit in self._ranked(fts_match(list(terms), expand_prefixes=True), include_translations):
                    if hit.sys_id not in found:
                        found.add(hit.sys_id)
                        ranked.append(replace(hit, exact=False))
            if any(t.near for t in terms):
                # One word: its own manuscripts are found already, so only its near spellings are
                # searched (a common word's own records are not ranked a second time). Several:
                # each word as typed or near, since a manuscript may hold one as typed and another
                # only near.
                match = fts_match(list(terms), expand_prefixes=expand, near=True, near_only=len(terms) == 1)
                for hit in self._ranked(match, include_translations):
                    if hit.sys_id not in found:
                        found.add(hit.sys_id)
                        ranked.append(replace(hit, exact=False, near=True))
        except sqlite3.Error as exc:
            logger.warning("CatalogTextService: search %r failed: %s", query, exc)
            return _empty(query, include_translations, terms, near_spellings)
        bound = max(0, int(limit)) if limit is not None else len(ranked)
        return CatalogResult(query, terms, tuple(ranked[:bound]), len(ranked), include_translations,
                             near_spellings)

    def _with_near_spellings(self, terms: tuple[Term, ...]) -> list[Term]:
        """``terms`` with each single word's near spellings that the file holds, commonest first
        (a phrase gets none: it asked for the words as typed). Only the first ``MAX_NEAR_WORDS``
        single words get them."""
        out = []
        budget = MAX_NEAR_WORDS
        for term in terms:
            if term.phrase or budget <= 0:
                out.append(term)
                continue
            budget -= 1
            near = self._near_in_file(term.keys[0])
            out.append(replace(term, near=near) if near else term)
        return out

    def _near_in_file(self, word: str) -> tuple[str, ...]:
        """*word*'s near spellings that the file holds, remembered (``NEAR_CACHE_SIZE``)."""
        with self._near_lock:
            hit = self._near_cache.get(word)
        if hit is not None:
            return hit
        candidates = _near_spellings_of(word) - {word}
        near = tuple(self._in_vocabulary(candidates)) if candidates else ()
        with self._near_lock:
            while len(self._near_cache) >= NEAR_CACHE_SIZE:
                self._near_cache.pop(next(iter(self._near_cache)))
            self._near_cache[word] = near
        return near

    def _in_vocabulary(self, candidates) -> list[str]:
        """Those of ``candidates`` the FTS index holds, the commonest (most records) first."""
        self._conn.execute(
            "CREATE VIRTUAL TABLE IF NOT EXISTS temp.catalog_vocab USING fts5vocab(main, 'catalog_fts', 'row')")
        rows = self._conn.execute(
            "SELECT term FROM temp.catalog_vocab WHERE term IN (SELECT value FROM json_each(?)) "
            "ORDER BY doc DESC, term", (json.dumps(sorted(candidates), ensure_ascii=False),))
        return [row[0] for row in rows]

    def _ranked(self, match: str, include_translations: bool) -> list[CatalogHit]:
        weights = ", ".join(str(w) for w in COLUMN_WEIGHTS)
        where = "" if include_translations else "WHERE r.automatic = 0"
        # The bm25 scores are taken in a MATERIALIZED step of their own: flattened into the join,
        # FTS5 refuses bm25() ("unable to use function bm25 in the requested context").
        # SQLite returns the other columns of the row that holds MIN(): the best record per
        # manuscript comes with its own id, source and field.
        sql = (
            "WITH h AS MATERIALIZED ("
            f"  SELECT rowid AS id, bm25(catalog_fts, {weights}) AS score "
            "  FROM catalog_fts WHERE catalog_fts MATCH ?) "
            "SELECT r.sys_id, r.id, r.source, r.field, r.lang, r.automatic, "
            f"MIN(h.score * CASE r.automatic WHEN 1 THEN {AUTOMATIC_FACTOR} ELSE 1.0 END) AS best "
            f"FROM h JOIN records r ON r.id = h.id {where} "
            "GROUP BY r.sys_id ORDER BY best, r.sys_id"
        )
        return [
            CatalogHit(sys_id=row[0], record_id=int(row[1]), source=row[2], field=row[3], lang=row[4],
                       automatic=bool(row[5]), score=float(row[6]), exact=True)
            for row in self._conn.execute(sql, (match,))
        ]

    def matches(self, sys_id: str, query: str, include_translations: bool = True,
                near_spellings: bool = False, limit: int = MATCHES_LIMIT) -> tuple[list[CatalogMatch], int]:
        """Every catalogue record of ``sys_id`` that ``query`` matches, as ``search`` matches it,
        and how many there are: the records found by the words as typed first, then through a
        Hebrew prefix, then through a near spelling, the best first in each, at most ``limit``.
        Each comes whole, with its matched words. What a manuscript opened from a catalogue search
        shows as the place it was found, whatever field holds it. Never raises."""
        terms = tuple(parse_query(query or ""))
        if not terms or self._conn is None or not sys_id:
            return [], 0
        try:
            where = "" if include_translations else " AND automatic = 0"
            ids = [r[0] for r in self._conn.execute(f"SELECT id FROM records WHERE sys_id = ?{where}",
                                                   (str(sys_id),))]
            if not ids:
                return [], 0
            if near_spellings:
                terms = tuple(self._with_near_spellings(terms))
            expand = can_expand(list(terms))
            tiers = [(False, False)] + ([(True, False)] if expand else [])
            if any(t.near for t in terms):
                tiers.append((expand, True))
            weights = ", ".join(str(w) for w in COLUMN_WEIGHTS)
            sql = (
                "WITH h AS MATERIALIZED ("
                f"  SELECT rowid AS id, bm25(catalog_fts, {weights}) AS score FROM catalog_fts "
                "  WHERE catalog_fts MATCH ? AND rowid IN (SELECT value FROM json_each(?))) "
                "SELECT r.id, r.source, r.field, r.lang, r.automatic, r.text "
                "FROM h JOIN records r ON r.id = h.id "
                f"ORDER BY h.score * CASE r.automatic WHEN 1 THEN {AUTOMATIC_FACTOR} ELSE 1.0 END, r.id"
            )
            found: list[CatalogMatch] = []
            seen: set[int] = set()
            for tier, (expand_prefixes, near) in enumerate(tiers):
                match = fts_match(list(terms), expand_prefixes=expand_prefixes, near=near)
                for row in self._conn.execute(sql, (match, json.dumps(ids))):
                    if row[0] in seen:
                        continue
                    seen.add(row[0])
                    spans, _ = match_spans(row[5], list(terms), expand_prefixes, near)
                    found.append(CatalogMatch(
                        record_id=int(row[0]), source=row[1], field=row[2], lang=row[3],
                        automatic=bool(row[4]), text=row[5], spans=tuple(spans), exact=tier == 0,
                        near=near))
        except sqlite3.Error as exc:
            logger.warning("CatalogTextService: matches %r in %s failed: %s", query, sys_id, exc)
            return [], 0
        bound = max(0, int(limit)) if limit is not None else len(found)
        return found[:bound], len(found)

    # -- display -------------------------------------------------------------

    def record_text(self, record_id: int) -> str:
        """The original text of one record ('' when there is none or the service is not ready)."""
        if self._conn is None:
            return ""
        try:
            row = self._conn.execute("SELECT text FROM records WHERE id = ?", (int(record_id),)).fetchone()
        except sqlite3.Error:
            return ""
        return row[0] if row else ""

    def record_texts(self, record_ids) -> dict[int, str]:
        """The original texts of many records at once, by id (one query, ids in file order)."""
        if self._conn is None:
            return {}
        ids = sorted({int(i) for i in record_ids})
        if not ids:
            return {}
        try:
            rows = self._conn.execute(
                "SELECT id, text FROM records WHERE id IN (SELECT value FROM json_each(?))", (json.dumps(ids),))
            return {int(r[0]): r[1] for r in rows}
        except sqlite3.Error:
            return {}

    def snippets(self, hits, result: CatalogResult) -> list[str]:
        """``snippet`` for each of ``hits``, their texts read in one query."""
        texts = self.record_texts(h.record_id for h in hits)
        return [make_snippet(texts.get(h.record_id, ""), result.terms, expand_prefixes=not h.exact, near=h.near)
                for h in hits]

    def snippet(self, hit: CatalogHit, result: CatalogResult) -> str:
        """About ``SNIPPET_CHARS`` of the hit's original text around its match (the first phrase
        match when the query has a phrase, else the first matched word), every matched word inside
        ``*...*`` (the result cards' marker, ``SearchEngine.format_snippet``). A hit found through a
        near spelling marks the near spelling."""
        return make_snippet(self.record_text(hit.record_id), result.terms, expand_prefixes=not hit.exact,
                            near=hit.near)


def make_snippet(text: str, terms: tuple[Term, ...] | list[Term], expand_prefixes: bool,
                 width: int = SNIPPET_CHARS, near: bool = False) -> str:
    """See ``CatalogTextService.snippet``. A phrase's words are marked only where the phrase
    stands (``match_spans``). A literal ``*`` in the text becomes ``∗`` (U+2217), so it can never
    open or close a marker."""
    if not text:
        return ""
    marked, anchor = match_spans(text, list(terms), expand_prefixes, near)
    start, end = 0, len(text)
    if len(text) > width:
        centre = anchor if anchor is not None else 0
        start = max(0, centre - width // 3)
        end = min(len(text), start + width)
        start = max(0, end - width)
        # Do not cut a word in two.
        while start > 0 and not text[start - 1].isspace():
            start -= 1
        while end < len(text) and not text[end].isspace():
            end += 1
    pieces: list[str] = []
    cursor = start
    for s, e in marked:
        if s < start or e > end:
            continue
        pieces.append(text[cursor:s].replace("*", "∗"))
        pieces.append("*" + text[s:e].replace("*", "∗") + "*")
        cursor = e
    pieces.append(text[cursor:end].replace("*", "∗"))
    body = "".join(pieces).replace("\n", " ").strip()
    return ("…" if start > 0 else "") + body + ("…" if end < len(text) else "")


def _validate(conn: Any) -> tuple[str, dict[str, str]]:
    """'' and the meta when the open file is one this reader serves; else the reason."""
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')")}
    missing = [t for t in REQUIRED_TABLES if t not in tables]
    if missing:
        return f"missing table(s): {', '.join(missing)}", {}
    meta = {str(k): ("" if v is None else str(v)) for k, v in conn.execute("SELECT key, value FROM meta")}
    if meta.get("version") != SCHEMA_VERSION:
        return f"schema version {meta.get('version')!r}; this reader understands {SCHEMA_VERSION}", {}
    if meta.get("normalizer_version") != str(NORMALIZER_VERSION):
        return (f"keyed by normalizer {meta.get('normalizer_version')!r}; this process runs "
                f"{NORMALIZER_VERSION}: rebuild the sidecar"), {}
    expected = meta.get("rows_total", "")
    actual = conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]
    if not expected.isdigit() or int(expected) != int(actual) or int(actual) == 0:
        return f"meta.rows_total = {expected!r} but records holds {actual} rows", {}
    return "", meta


# ---------------------------------------------------------------------------
# The process-wide instance (one per app, like the other sidecar services)
# ---------------------------------------------------------------------------

_default_service: Optional[CatalogTextService] = None
_lock = threading.Lock()


def get_catalog_text_service(thread_safe: bool = True) -> CatalogTextService:
    """The default service, created on first use (per-thread connections by default: safe under
    NiceGUI's ``run.io_bound`` pool)."""
    global _default_service
    with _lock:
        if _default_service is None:
            _default_service = CatalogTextService(thread_safe=thread_safe)
        return _default_service


def peek_catalog_text_service() -> Optional[CatalogTextService]:
    """The default service if it was opened already, else None (never opens the file: a page
    asks this on the event loop, and opening checks a 700 MB file)."""
    return _default_service


def installed_inputs() -> dict[str, Optional[str]]:
    """The installed FJMS and PGP sidecars' values for ``DRIFT_KEYS`` (None where a sidecar is
    absent or unreadable). Never raises."""
    out: dict[str, Optional[str]] = {key: None for key, _ in DRIFT_KEYS}
    try:
        from shared.fjms_service import get_fjms_service
        out["input_fjms_version"] = get_fjms_service(thread_safe=True).get_version()
    except Exception as exc:
        logger.debug("CatalogTextService: FJMS version unreadable: %s", exc)
    try:
        from shared.document_service import get_pgp_service
        out["input_pgp_upstream_commit"] = get_pgp_service().get_meta_value("upstream_commit")
    except Exception as exc:
        logger.debug("CatalogTextService: PGP upstream commit unreadable: %s", exc)
    return out


def open_and_check() -> CatalogTextService:
    """Open the process-wide service and log why it is not served, or how its inputs drifted from
    the installed sidecars (plan §4.6). For a startup hook, off the event loop. Never raises."""
    service = get_catalog_text_service()
    if not service.is_ready():
        logger.warning("Catalogue text search not served: %s", service.unavailable_reason)
        return service
    for line in service.input_drift(installed_inputs()):
        logger.warning("Catalogue text file out of date (rebuild it; plan §4.6): %s", line)
    return service


def reset_catalog_text_service() -> None:
    """Forget the default service (after the sidecar file is replaced)."""
    global _default_service
    with _lock:
        if _default_service is not None:
            _default_service.close()
            _default_service = None
