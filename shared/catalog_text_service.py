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
   (כתובה before הכתובה);
2. within each group, by the manuscript's best record: FTS5 bm25, with a title worth more than a
   description and a description more than a full catalogue entry (``COLUMN_WEIGHTS``), and an
   automatic translation worth less than an original (``AUTOMATIC_FACTOR``).

``include_translations=False`` leaves the automatic translations out (D2's switch). The count is
exact: every matching manuscript is ranked, and ``limit`` only bounds what is returned.
"""
from __future__ import annotations

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


def catalog_text_enabled() -> bool:
    """The ``CATALOG_TEXT_SEARCH_ENABLED`` flag, read on every call (default OFF)."""
    value = os.environ.get("CATALOG_TEXT_SEARCH_ENABLED")
    if value is None:
        return False
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
    exact: bool          # found by the words as typed (False: only through a Hebrew prefix)


@dataclass(frozen=True)
class CatalogResult:
    query: str
    terms: tuple[Term, ...]
    hits: tuple[CatalogHit, ...]
    total: int                   # every matching manuscript, whatever ``limit`` returned
    include_translations: bool

    @property
    def truncated(self) -> bool:
        return self.total > len(self.hits)


def _empty(query: str, include_translations: bool, terms: tuple[Term, ...] = ()) -> CatalogResult:
    return CatalogResult(query, terms, (), 0, include_translations)


class CatalogTextService:
    """Read-only access to the catalogue text sidecar. See the module docstring."""

    def __init__(self, db_path: Optional[str] = None, thread_safe: bool = False):
        self._conn: Any = None
        self._meta: dict[str, str] = {}
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

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = None
        self.unavailable_reason = "closed"

    # -- search --------------------------------------------------------------

    def search(self, query: str, include_translations: bool = True, limit: int = DEFAULT_LIMIT) -> CatalogResult:
        """The manuscripts whose catalogue records hold every word of ``query``. Never raises."""
        terms = tuple(parse_query(query or ""))
        if not terms or self._conn is None:
            return _empty(query, include_translations, terms)
        try:
            exact = self._ranked(fts_match(list(terms), expand_prefixes=False), include_translations)
            ranked = list(exact)
            if can_expand(list(terms)):
                found = {hit.sys_id for hit in exact}
                for hit in self._ranked(fts_match(list(terms), expand_prefixes=True), include_translations):
                    if hit.sys_id not in found:
                        ranked.append(replace(hit, exact=False))
        except sqlite3.Error as exc:
            logger.warning("CatalogTextService: search %r failed: %s", query, exc)
            return _empty(query, include_translations, terms)
        bound = max(0, int(limit)) if limit is not None else len(ranked)
        return CatalogResult(query, terms, tuple(ranked[:bound]), len(ranked), include_translations)

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

    def snippet(self, hit: CatalogHit, result: CatalogResult) -> str:
        """About ``SNIPPET_CHARS`` of the hit's original text around its match (the first phrase
        match when the query has a phrase, else the first matched word), every matched word inside
        ``*...*`` (the result cards' marker, ``SearchEngine.format_snippet``)."""
        return make_snippet(self.record_text(hit.record_id), result.terms, expand_prefixes=not hit.exact)


def make_snippet(text: str, terms: tuple[Term, ...] | list[Term], expand_prefixes: bool,
                 width: int = SNIPPET_CHARS) -> str:
    """See ``CatalogTextService.snippet``. A phrase's words are marked only where the phrase
    stands (``match_spans``). A literal ``*`` in the text becomes ``∗`` (U+2217), so it can never
    open or close a marker."""
    if not text:
        return ""
    marked, anchor = match_spans(text, list(terms), expand_prefixes)
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


def reset_catalog_text_service() -> None:
    """Forget the default service (after the sidecar file is replaced)."""
    global _default_service
    with _lock:
        if _default_service is not None:
            _default_service.close()
            _default_service = None
