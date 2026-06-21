# -*- coding: utf-8 -*-
"""
FGP (Friedberg Genizah Project) transcription service.

Mirrors ``shared/document_service.py::PgpService`` for a SEPARATE, gitignored
sidecar ``fgp_data/fgp_transcriptions.db`` (~387 MB, ~45K rows). The goal is to
surface FGP transcriptions as a DISTINCT, selectable source ALONGSIDE PGP in the
version chooser, in both apps (web NiceGUI + desktop PyQt6).

The FGP schema MIRRORS PGP ``document_sources`` but is flatter/denormalized:
``sys_id``, ``page_info`` and the FGP C-number live directly on each source row
(there is no ``document_fragments`` join table — ``sys_id`` was resolved at build
time, 99.94% against ``libraries.csv`` ``system_number``).

Read-only. Degrades gracefully:
  * flag off  -> ``get_fgp_sources_for_fragment()`` returns ``[]``
  * DB absent -> ``is_available()`` returns ``False``; queries return ``[]``

Thread-safe: uses per-thread SQLite connections via ``ThreadLocalConnection`` so
concurrent NiceGUI ``run.io_bound()`` calls each get their own connection.

------------------------------------------------------------------------------
⚠️  SCHEMA ASSUMPTION — VERIFY AGAINST THE REAL DB BEFORE SHIP (Phase C / FGP-09)
------------------------------------------------------------------------------
The real ``fgp_transcriptions.db`` is gitignored and was UNAVAILABLE when this
module was written, and its companion references (``fgp_data/README.md``,
``docs/plans/FGP_TRANSCRIPTIONS_INTEGRATION_PLAN.md``) are not in the repo. The
assumed schema below is derived from:
  * docs/OPEN_ISSUES.md  -> "mirrors PGP document_sources; 99.94% sys_id-resolved;
    recto/verso for 18,222 rows via FGP C-number"
  * docs/plans/FGP_CHOOSER_MILESTONE.md §4.1 -> the chooser-shaped output dict.

To absorb minor real-schema drift WITHOUT crashing, the service DISCOVERS the
actual source table + column set at connect time (see ``_discover_source_table``)
and reads every column defensively. But the assumed column NAMES — ``sys_id``,
``page_info``, ``fgp_c_number``, ``doc_relation``, ``content``, ``language``,
``source_scholar``, ``sections``, ``sequence_order``, ``id`` — MUST be confirmed
(and this file adjusted) once the real DB is in hand.
"""

import json
import logging
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from shared.thread_local_db import ThreadLocalConnection

logger = logging.getLogger(__name__)

# Default sidecar location (mirrors PgpService).
_SIDECAR_FILENAME = "fgp_transcriptions.db"
_SIDECAR_DIR = "fgp_data"

# ── Source-kind discriminator (FGP-03) ─────────────────────────────
# Shared, normalized provider tags + an attribution string. Used at EVERY
# classifier surface (web version_selector, desktop _populate_pgp_combo) so FGP
# rows never silently fold into the green "PGP" group just because they share
# PGP's 'Digital Edition' / 'Digital Translation' doc_relation values.
SOURCE_FGP = "fgp"
SOURCE_PGP = "pgp"

# Default attribution. NOTE (FGP-10): the EXACT credit/licensing text is a
# release-gated sign-off item; this is the working default per the milestone doc.
FGP_ATTRIBUTION = "FGP (Friedberg Genizah Project)"

# Documented candidate table names, most-likely first ("mirrors document_sources").
_CANDIDATE_TABLES = (
    "document_sources",
    "fgp_sources",
    "sources",
    "transcriptions",
    "fgp_transcriptions",
)
# Column names that may hold the transcription text, in preference order.
_CONTENT_COLUMNS = ("content", "transcription", "text")


def _find_project_root() -> Optional[Path]:
    """Find the project root by looking for libraries.csv up from this file."""
    current = Path(__file__).resolve().parent
    for _ in range(5):  # Up to 5 levels
        if (current / "libraries.csv").exists():
            return current
        current = current.parent
    return None


def _fgp_enabled() -> bool:
    """Return whether the shared FGP flag is enabled.

    Read from the environment on every call so the flag can be flipped without a
    restart (consistent with the project's other request-time-read flags). Lives
    in ``shared/`` so both apps share one gate; ``shared/`` must NOT import
    ``web/`` (the web app layers an optional ``WEB_FGP_ENABLED`` override on top
    via ``web/feature_flags.py``).

    Default: OFF (opt-in). Nothing surfaces until the DB is in place and the flag
    is explicitly enabled — safe for prod and keeps existing behavior unchanged.
    """
    value = os.environ.get("FGP_TRANSCRIPTIONS_ENABLED")
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


# ── Source-kind helpers (pure; FGP-03) ─────────────────────────────


def source_provider(source: Dict[str, Any]) -> str:
    """Return the normalized provider tag for a chooser source dict.

    ``'fgp'`` for FGP sources (carry ``source='fgp'`` / ``is_fgp=True``),
    ``'pgp'`` otherwise. Use this at every classifier so FGP and PGP editions
    render in distinct groups even though they share ``doc_relation`` values.
    """
    if source.get("source") == SOURCE_FGP or source.get("is_fgp"):
        return SOURCE_FGP
    return SOURCE_PGP


def source_relation_kind(source: Dict[str, Any]) -> str:
    """Normalize ``doc_relation`` into ``'edition'`` / ``'translation'`` / ``'other'``.

    Mirrors the existing substring logic (``'Edition'`` / ``'Translation'`` in
    ``doc_relation``) used by the web ``version_selector`` and the desktop
    ``_populate_pgp_combo`` classifiers, centralized so both apps agree.
    """
    rel = source.get("doc_relation") or ""
    if "Translation" in rel:
        return "translation"
    if "Edition" in rel:
        return "edition"
    return "other"


def namespaced_source_id(source: Dict[str, Any]) -> Optional[str]:
    """Return a collision-free id like ``'pgp:123'`` / ``'fgp:123'`` (FGP-03).

    Both PGP and FGP carry an integer ``id`` that would collide when merged into a
    single ``all_sources`` list. Namespacing by provider keeps selection state and
    cache keys distinct. Returns ``None`` when the source has no id.
    """
    raw = source.get("id")
    if raw is None:
        return None
    return f"{source_provider(source)}:{raw}"


def _normalize_fgp_sections(sections: Any) -> Optional[List[Dict[str, Any]]]:
    """Normalize FGP ``sections`` so canvas-based page lookup works (FGP-02).

    FGP ``sections`` are keyed ``page_num``; the shared canvas-matching code
    keys on ``canvas_num``. Copy ``page_num`` -> ``canvas_num`` where missing so
    a section can be matched by page number. Returns ``None`` for empty/invalid.
    """
    if not isinstance(sections, list):
        return None
    out: List[Dict[str, Any]] = []
    for sec in sections:
        if isinstance(sec, dict):
            sec = dict(sec)
            if "canvas_num" not in sec and "page_num" in sec:
                try:
                    sec["canvas_num"] = int(sec["page_num"])
                except (TypeError, ValueError):
                    pass
            out.append(sec)
        else:
            out.append(sec)
    return out or None


def get_fgp_section_for_page(source: Dict[str, Any], page_num: int) -> Optional[str]:
    """Return the FGP source's text for a page (1=recto, 2=verso), or ``None``.

    FGP-specific page split. Deliberately does NOT reuse
    ``document_service.get_section_for_page``: that function falls back to
    returning the FULL transcription when a page is not covered by structured
    sections and the text has no recto/verso markers — which, for FGP's faithful
    PDF->text content (no markers), would show the SAME full text on BOTH sides.
    This splitter returns ``None`` for an uncovered page instead (FGP-02), and
    never touches the shared PGP path (FGP-12).

    Rules:
      * structured ``sections`` present -> match by ``canvas_num`` (already
        normalized from ``page_num``); uncovered page -> ``None``.
      * no sections, ``page_info`` present -> show only on that side.
      * no sections, no ``page_info`` -> default to recto only (page 1).
    """
    content = (source.get("content") or "").strip()
    if not content:
        return None

    sections = source.get("sections") or []
    if sections:
        for sec in sections:
            if not isinstance(sec, dict):
                continue
            cnum = sec.get("canvas_num")
            if cnum is None:
                cnum = sec.get("page_num")
            if cnum == page_num:
                text = sec.get("text") or sec.get("content")
                return text if text else None
        # Page not covered by any section -> no content for this page.
        return None

    # No structured sections: single-sided by page_info (default: recto).
    page_info = (source.get("page_info") or "recto").lower()
    side = "verso" if "verso" in page_info else "recto"
    target = "recto" if page_num == 1 else "verso"
    return content if side == target else None


def _row_to_fgp_source(row: sqlite3.Row) -> Dict[str, Any]:
    """Map a raw FGP DB row to a chooser-shaped source dict (FGP-01/02/03).

    Output keys mirror what ``version_selector`` / ``_populate_pgp_combo`` read
    from PGP sources (``doc_relation``, ``content``, ``source_scholar``,
    ``language``, ``id``), plus the FGP discriminator + extras.
    """
    d = dict(row)

    content = ""
    for col in _CONTENT_COLUMNS:
        if d.get(col):
            content = d[col]
            break

    sections = d.get("sections")
    if isinstance(sections, str):
        try:
            sections = json.loads(sections)
        except (json.JSONDecodeError, TypeError):
            sections = None
    sections = _normalize_fgp_sections(sections)

    raw_id = d.get("id")
    out: Dict[str, Any] = {
        "source": SOURCE_FGP,
        "is_fgp": True,
        "id": raw_id,
        "uid": f"{SOURCE_FGP}:{raw_id}" if raw_id is not None else None,
        # FGP transcription text are editions; default if the column is absent.
        "doc_relation": d.get("doc_relation") or "Digital Edition",
        "language": d.get("language"),
        "content": content,
        "sections": sections,
        "page_info": d.get("page_info"),
        "source_scholar": d.get("source_scholar") or "FGP",
        "attribution": FGP_ATTRIBUTION,
        "fgp_c_number": d.get("fgp_c_number"),
        "sequence_order": d.get("sequence_order") or 0,
    }
    return out


class FgpService:
    """Service for accessing FGP transcription data from the SQLite sidecar."""

    def __init__(self, db_path: str = None, thread_safe: bool = False):
        """
        Initialize FgpService.

        Args:
            db_path: Path to fgp_transcriptions.db. If None, auto-detect from the
                LOCALAPPDATA sidecar location first, then the project root.
            thread_safe: If True, use per-thread connections (NiceGUI web app).
                Desktop app may leave this False (single-threaded).
        """
        self._conn = None  # ThreadLocalConnection or sqlite3.Connection
        self._db_path: Optional[str] = None
        self._table: Optional[str] = None
        self._columns: Set[str] = set()

        # Resolve db_path
        if db_path is None:
            # Check user-updated sidecar location first (LOCALAPPDATA), matching
            # PgpService + the path the desktop sidecar updater writes to.
            user_path = os.path.join(
                os.environ.get("LOCALAPPDATA", ""),
                "GenizahSearchPro", "data", _SIDECAR_DIR, _SIDECAR_FILENAME,
            )
            if os.path.isfile(user_path):
                db_path = user_path
            else:
                root = _find_project_root()
                if root:
                    db_path = str(root / _SIDECAR_DIR / _SIDECAR_FILENAME)

        if db_path is None:
            logger.warning("FgpService: No db_path provided and project root not found")
            return

        self._db_path = db_path
        db_file = Path(db_path)

        if not db_file.exists():
            # Expected when the (gitignored, downloaded-on-demand) DB isn't present.
            logger.info("FgpService: Sidecar database not found at %s", db_path)
            return

        try:
            uri = f"file:{db_path}?mode=ro"
            if thread_safe:
                self._conn = ThreadLocalConnection(
                    uri, row_factory=sqlite3.Row, timeout=10.0
                )
            else:
                self._conn = sqlite3.connect(
                    uri, uri=True, check_same_thread=True, timeout=10.0
                )
                self._conn.row_factory = sqlite3.Row
            self._table, self._columns = _discover_source_table(self._conn)
            if self._table is None:
                logger.warning(
                    "FgpService: connected to %s but found no source table with a "
                    "'sys_id' column; FGP sources will be empty", db_path
                )
            else:
                logger.info(
                    "FgpService: Connected to %s (table=%s)", db_path, self._table
                )
        except Exception as e:
            logger.error("FgpService: Failed to connect to %s: %s", db_path, e)
            self._conn = None

    def is_available(self) -> bool:
        """True if the sidecar DB connection is active AND a source table was found."""
        return self._conn is not None and self._table is not None

    def get_fgp_sources_for_fragment(self, sys_id: str) -> List[Dict[str, Any]]:
        """
        Get all FGP transcription sources for a fragment, chooser-shaped.

        Args:
            sys_id: The GenizahSearch system ID (== libraries.csv system_number).

        Returns:
            List of chooser-shaped FGP source dicts (see ``_row_to_fgp_source``),
            ordered by ``sequence_order`` when available. Returns ``[]`` when the
            flag is off, the DB is absent, ``sys_id`` is falsy, or on error.
        """
        if not _fgp_enabled():
            return []
        if not sys_id or not self.is_available() or "sys_id" not in self._columns:
            return []

        try:
            order = " ORDER BY sequence_order" if "sequence_order" in self._columns else ""
            cursor = self._conn.execute(
                f'SELECT * FROM "{self._table}" WHERE sys_id = ?{order}', (sys_id,)
            )
            return [_row_to_fgp_source(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error("Error getting FGP sources for fragment %s: %s", sys_id, e)
            return []

    def get_sys_ids_with_fgp_sources(self, sys_ids: List[str]) -> Set[str]:
        """
        Batch-check which sys_ids have FGP transcription sources.

        Chooser-availability helper ONLY (e.g. to show an "FGP" affordance). This
        is NOT a search/discovery signal — FGP-12 forbids touching Tantivy,
        ``get_sys_ids_with_transcriptions``, ``has_pgp`` or PGP search filters.

        Args:
            sys_ids: List of system IDs to check.

        Returns:
            Set of sys_ids that have at least one FGP source. ``set()`` when the
            flag is off, the DB is absent, or on error.
        """
        if not _fgp_enabled():
            return set()
        if not sys_ids or not self.is_available() or "sys_id" not in self._columns:
            return set()

        try:
            result_set: Set[str] = set()
            batch_size = 500  # stay under SQLite's 999 variable limit
            for i in range(0, len(sys_ids), batch_size):
                batch = sys_ids[i:i + batch_size]
                placeholders = ",".join("?" * len(batch))
                cursor = self._conn.execute(
                    f'SELECT DISTINCT sys_id FROM "{self._table}" '
                    f"WHERE sys_id IN ({placeholders})",
                    batch,
                )
                result_set.update(row["sys_id"] for row in cursor)
            return result_set
        except Exception as e:
            logger.error("Error batch checking FGP sources: %s", e)
            return set()

    def close(self):
        """Close the database connection if open."""
        if self._conn is not None:
            try:
                self._conn.close()
                logger.info("FgpService: Connection closed")
            except Exception as e:
                logger.error("FgpService.close error: %s", e)
            finally:
                self._conn = None
                self._table = None
                self._columns = set()

    def get_version(self) -> Optional[str]:
        """Get the sidecar DB version from a ``meta`` table, or ``None``."""
        if self._conn is None:
            return None
        try:
            cursor = self._conn.execute(
                "SELECT value FROM meta WHERE key = 'version'"
            )
            row = cursor.fetchone()
            return row["value"] if row else None
        except Exception:
            # meta table may not exist in the FGP DB; not an error.
            return None


def _discover_source_table(conn) -> tuple:
    """Discover the FGP source table name + its columns.

    Returns ``(table_name, columns_set)`` for the table that holds FGP sources,
    or ``(None, set())`` if none is found. Prefers the documented candidate names
    (those carrying a ``sys_id`` column); otherwise falls back to any table that
    has both ``sys_id`` and a content-ish column. Makes the service resilient to
    minor differences between the assumed and real schema.
    """
    try:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row["name"] for row in cursor.fetchall()]
    except Exception as e:
        logger.error("FgpService: could not list tables: %s", e)
        return None, set()

    def cols(table: str) -> Set[str]:
        try:
            return {row["name"] for row in conn.execute(f'PRAGMA table_info("{table}")')}
        except Exception:
            return set()

    # First pass: documented candidate names that carry sys_id.
    for table in _CANDIDATE_TABLES:
        if table in tables:
            columns = cols(table)
            if "sys_id" in columns:
                return table, columns

    # Second pass: any table with sys_id + a content-ish column.
    content_cols = set(_CONTENT_COLUMNS)
    for table in tables:
        columns = cols(table)
        if "sys_id" in columns and (content_cols & columns):
            return table, columns

    return None, set()


# ── Module-level Singleton ─────────────────────────────────────────

_default_service: Optional[FgpService] = None


def get_fgp_service(thread_safe: bool = True) -> FgpService:
    """Get or create the default FgpService singleton.

    Args:
        thread_safe: If True (default), per-thread read-only connections — safe
            for both web and desktop.

    Returns:
        FgpService instance (may have ``is_available() == False`` if the DB is
        missing or the flag is off).
    """
    global _default_service
    if _default_service is None:
        _default_service = FgpService(thread_safe=thread_safe)
    return _default_service


def reset_fgp_service():
    """Reset the singleton FgpService.

    Call after the ``fgp_transcriptions.db`` sidecar is downloaded/replaced (the
    desktop sidecar updater's post-download reset) to force re-initialization on
    next access.
    """
    global _default_service
    if _default_service is not None:
        _default_service.close()
        _default_service = None


# ── Module-level Wrapper Functions ─────────────────────────────────


def get_fgp_sources_for_fragment(sys_id: str) -> List[Dict[str, Any]]:
    """Get all FGP transcription sources for a fragment (chooser-shaped)."""
    return get_fgp_service().get_fgp_sources_for_fragment(sys_id)


def get_sys_ids_with_fgp_sources(sys_ids: List[str]) -> Set[str]:
    """Batch-check which sys_ids have FGP sources (chooser availability only)."""
    return get_fgp_service().get_sys_ids_with_fgp_sources(sys_ids)


def get_version() -> Optional[str]:
    """Get the FGP sidecar database version."""
    svc = get_fgp_service()
    return svc.get_version() if svc.is_available() else None
