# -*- coding: utf-8 -*-
"""Phase 95 LOCAL indexer (REQ-1, REQ-3, REQ-4, REQ-5, D-01..D-44).

Qt-free. Owns: PyMuPDF/python-docx/TXT extraction, LOCAL Tantivy side-index
+ LOCAL LAB side-index builders, SQLite cache (folders / processed_files /
local_pages / local_files), incremental re-extract, delete-by-uid, two-phase
commit with crash recovery.

Critical divergences from main Tantivy index (CONTEXT D-08 / D-13 / D-20 / D-21
+ RESEARCH Pitfall #2):
  - unique_id field uses tokenizer_name="raw" (main index omits this kwarg).
    Required because LOCAL deletes-by-term on rescan; main never deletes.
  - One Tantivy doc per PDF page (D-03), 20-paragraph chunk for DOCX (D-04).
  - LOCAL hits never write to shared corpus files (Seewald's prototype patched
    the shared corpus - we don't; SPEC Constraint #6).

Per CONTEXT D-02: _fix_rtl_line, _fix_rtl_page, _join_fragmented_lines are
PORTED VERBATIM from seewald_addition/genizah_make_index.py:67-105 but NEVER
invoked at runtime. They exist as a regression-prevention contract for the
future fallback path (pdfplumber/pypdf). Tests in tests/test_local_indexer.py
exercise the helpers in isolation.

HIGH-3 review fix: _delete_file uses crash-safe THREE-STEP protocol:
  1. Mark pending_delete=1 in SQLite
  2. Tantivy delete + commit
  3. SQLite final DELETE
Startup recovery calls _recover_pending_deletes() to replay steps 2-3 for
any rows left with pending_delete=1 from a previous crash.

HIGH-4 review fix (option b LOCKED): page text for LAB rebuild is read from
main LOCAL Tantivy content stored field by UID - NOT from SQLite (this plan's
schema stores metadata only).

MEDIUM-2 review fix: TXT extraction tries utf-8-sig with errors=strict; on
UnicodeDecodeError, attempts cp1255 (legacy Windows Hebrew); on both failures,
the file is marked extraction_status='encoding_error'.
"""
from __future__ import annotations

import logging
import os
import re
import sqlite3
import time
import unicodedata
from typing import Callable, Iterator, Optional

import fitz  # PyMuPDF - D-01
from docx import Document as _DocxDoc  # python-docx - D-04
import tantivy

from shared.local_sys_id import (
    is_local_sys_id,  # noqa: F401
    generate_local_sys_id,
    _canonical_filepath,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_SUPPORTED_EXTENSIONS = {".docx", ".pdf", ".txt"}
_DOCX_CHUNK_PARAGRAPHS = 20            # D-04
_SCANNED_PDF_CHAR_THRESHOLD = 50       # D-05
_EMPTY_PAGE_CHAR_THRESHOLD = 10        # D-06
_COMMIT_BATCH_SIZE = 25                # D-21
_MAX_COLLISION_RETRIES = 4             # D-19
_UNIQUE_ID_PREFIX = "LOCAL"            # D-34
_DEFAULT_TXT_ENCODING = "utf-8-sig"    # D-07 starting policy


# ---------------------------------------------------------------------------
# EncodingError (MEDIUM-2 review fix)
# ---------------------------------------------------------------------------
class EncodingError(Exception):
    """Raised by extract_txt when both utf-8-sig and cp1255 fail (MEDIUM-2)."""
    pass


# ---------------------------------------------------------------------------
# RTL helpers - DEAD CODE per D-02.
# Ported VERBATIM from seewald_addition/genizah_make_index.py:67-105.
# These helpers are NEVER invoked in v1 runtime. They exist as a
# regression-prevention contract for a future pdfplumber/pypdf fallback path.
# ---------------------------------------------------------------------------

def _rtl_ratio(text: str) -> float:  # pragma: no cover
    """Fraction of RTL chars among alpha chars. DEAD CODE per D-02."""
    alpha = [c for c in text if c.isalpha()]
    if not alpha:
        return 0.0
    rtl = sum(1 for c in alpha if unicodedata.bidirectional(c) in ("R", "AL", "AN"))
    return rtl / len(alpha)


def _fix_rtl_line(line: str) -> str:  # pragma: no cover
    """Reverse a pdfplumber mirror-reversed RTL line. DEAD CODE per D-02."""
    s = line.strip()
    if not s or _rtl_ratio(s) <= 0.4:
        return line
    lead = len(line) - len(line.lstrip())
    tail = len(line) - len(line.rstrip())
    core = s[::-1]
    return line[:lead] + core + (line[len(line) - tail:] if tail else "")


def _fix_rtl_page(text: str) -> str:  # pragma: no cover
    """Apply per-line RTL fix + re-glue punctuation. DEAD CODE per D-02."""
    if not text:
        return text
    lines = [_fix_rtl_line(ln) for ln in text.splitlines()]
    result = "\n".join(lines)
    result = re.sub(r"(\w)\s+([,.])", r"\1\2", result)
    result = re.sub(r"([,.])\s+(\w)", r"\1 \2", result)
    return result


def _join_fragmented_lines(text: str) -> str:  # pragma: no cover
    """Join pages where each word is on its own line. DEAD CODE per D-02."""
    lines = text.splitlines()
    non_empty = [line for line in lines if line.strip()]
    if len(non_empty) < 4:
        return text
    single = sum(1 for line in non_empty if len(line.split()) <= 1)
    if single / len(non_empty) < 0.60:
        return text
    paragraphs, current = [], []
    for line in lines:
        s = line.strip()
        if s:
            current.append(s)
        elif current:
            paragraphs.append(" ".join(current))
            current = []
    if current:
        paragraphs.append(" ".join(current))
    return "\n\n".join(paragraphs)


# ---------------------------------------------------------------------------
# Schema builders
# ---------------------------------------------------------------------------

def build_local_schema() -> tantivy.Schema:
    """LOCAL Tantivy side-index schema (REQ-3).

    CRITICAL DIVERGENCE: unique_id uses tokenizer_name="raw" (main index at
    genizah_core.py:5125 omits this kwarg). Required for delete_documents()
    to work on rescan - RESEARCH Pitfall #2 / tantivy-py #297.

    Without tokenizer_name="raw", writer.delete_documents("unique_id", uid)
    SILENTLY does nothing on rescans and doubles the page-row count on every
    modify. Schema-divergence comment is required per PATTERNS.md.
    """
    builder = tantivy.SchemaBuilder()
    # CRITICAL: tokenizer_name="raw" - main index at genizah_core.py:5125 omits this
    # and is rebuilt from scratch so the bug stays latent. For LOCAL where incremental
    # delete IS the central operation, raw is mandatory. tantivy-py issue #297.
    builder.add_text_field("unique_id", stored=True, tokenizer_name="raw")
    builder.add_text_field("content", stored=True, tokenizer_name="whitespace")
    builder.add_text_field("content_head", stored=False, tokenizer_name="whitespace")
    builder.add_text_field("content_tail", stored=False, tokenizer_name="whitespace")
    builder.add_text_field("line_starts", stored=False, tokenizer_name="whitespace")
    builder.add_text_field("line_ends", stored=False, tokenizer_name="whitespace")
    builder.add_text_field("source", stored=True)
    builder.add_text_field("full_header", stored=True)
    builder.add_text_field("shelfmark", stored=True)
    builder.add_text_field("scope", stored=True)
    builder.add_text_field("boundaries", stored=True)
    return builder.build()


def build_local_lab_schema() -> tantivy.Schema:
    """LOCAL LAB side-index schema (D-09).

    Same raw-tokenizer divergence on unique_id as the main LOCAL schema.
    fingerprint_dyn computed at build time using current LAB dynamic_rank_map.
    """
    builder = tantivy.SchemaBuilder()
    # CRITICAL: tokenizer_name="raw" - same divergence as build_local_schema().
    builder.add_text_field("unique_id", stored=True, tokenizer_name="raw")  # Pitfall #2
    builder.add_text_field("text_normalized", stored=True, tokenizer_name="simple")
    builder.add_text_field("text_ngram", stored=False, tokenizer_name="whitespace")
    # NOTE: main LAB uses self.LAB_FINGERPRINT_FIELD; we mirror as "fingerprint".
    builder.add_text_field("fingerprint", stored=False, tokenizer_name="simple")
    builder.add_text_field("fingerprint_dyn", stored=False, tokenizer_name="simple")
    builder.add_text_field("full_header", stored=True)
    builder.add_text_field("shelfmark", stored=True)
    builder.add_text_field("source", stored=True)
    builder.add_text_field("content", stored=True, tokenizer_name="simple")
    return builder.build()


# ---------------------------------------------------------------------------
# SQLite schema init (D-35)
# ---------------------------------------------------------------------------

def init_sqlite(db_path: str) -> sqlite3.Connection:
    """Create tables per D-35 and return an open connection.

    Tables:
      - folders: registered source folders with status tracking
      - processed_files: narrow mtime-cache; status for two-phase commit
      - local_pages: per-page UID tracking for delete-by-uid (D-20)
      - local_files: rich metadata for Browse panel + per-file status panel
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")  # Pitfall #6 mitigation
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS folders (
            folder_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            path             TEXT    UNIQUE NOT NULL,
            added_at         REAL,
            last_scanned_at  REAL,
            status           TEXT    NOT NULL DEFAULT 'active'
        );
        CREATE TABLE IF NOT EXISTS processed_files (
            filepath  TEXT    PRIMARY KEY,
            mtime     REAL,
            size      INTEGER,
            sys_id    TEXT,
            status    TEXT    NOT NULL DEFAULT 'committed'
        );
        CREATE TABLE IF NOT EXISTS local_pages (
            sys_id    TEXT    NOT NULL,
            uid       TEXT    NOT NULL,
            page_num  INTEGER NOT NULL,
            PRIMARY KEY (sys_id, page_num)
        );
        CREATE TABLE IF NOT EXISTS local_files (
            file_id           INTEGER PRIMARY KEY AUTOINCREMENT,
            sys_id            TEXT    NOT NULL UNIQUE,
            filepath          TEXT    NOT NULL,
            folder_id         INTEGER NOT NULL REFERENCES folders(folder_id),
            display_title     TEXT,
            original_filename TEXT    NOT NULL,
            file_extension    TEXT    NOT NULL,
            page_count        INTEGER NOT NULL DEFAULT 0,
            file_size_bytes   INTEGER NOT NULL,
            extraction_status TEXT    NOT NULL,
            last_indexed_at   REAL    NOT NULL,
            sha256_full       TEXT,
            error_msg         TEXT,
            pending_delete    INTEGER NOT NULL DEFAULT 0
        );
    """)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Folder overlap detection (D-17 + D-42)
# ---------------------------------------------------------------------------

def check_folder_overlap(
    candidate: str,
    existing_paths: list[str],
) -> Optional[str]:
    """Return the conflicting existing path if candidate overlaps, else None.

    Overlap = same path, ancestor, or descendant.
    Uses _canonical_filepath + os.path.normcase + os.path.commonpath per D-17.
    """
    cand = _canonical_filepath(candidate)
    for existing in existing_paths:
        exist = _canonical_filepath(existing)
        if cand == exist:
            return existing
        try:
            common = os.path.normcase(os.path.commonpath([cand, exist]))
        except ValueError:
            # Different drives on Windows
            continue
        if common == cand or common == exist:
            return existing
    return None


# ---------------------------------------------------------------------------
# UID + full_header builders (D-34)
# ---------------------------------------------------------------------------

def _make_uid(sys_id: str, page_num: int) -> str:
    """Construct the Tantivy unique_id for a LOCAL page doc (D-34)."""
    return f"{_UNIQUE_ID_PREFIX}_{sys_id}_P{page_num}"


def _make_full_header(sys_id: str, page_num: int, file_id: int) -> str:
    """Construct the full_header stored field value for a LOCAL page doc (D-34)."""
    return f"{sys_id}_LOCAL_P{page_num}_F{file_id:04d}"


# ---------------------------------------------------------------------------
# Per-format extractors
# ---------------------------------------------------------------------------

def extract_pdf_pages(
    filepath: str,
) -> Iterator[tuple[int, str, str]]:
    """Extract text page-by-page using PyMuPDF (D-01 / D-03).

    Yields (page_num, text, title) - D-03 one-doc-per-page model.

    D-06: pages with < 10 chars after strip are skipped silently.
    D-05: caller must check total chars across all yielded pages; if < 50,
          file gets status='no_text_layer'.

    D-02: RTL helpers are NOT invoked in v1 (dead code).
    """
    doc = fitz.open(filepath)
    try:
        title = (doc.metadata or {}).get("title") or os.path.basename(filepath)
        for page_num, page in enumerate(doc, start=1):
            blocks = page.get_text("blocks")
            text_parts = [b[4].strip() for b in blocks if b[6] == 0 and b[4].strip()]
            text = "\n\n".join(text_parts)
            if len(text.strip()) < _EMPTY_PAGE_CHAR_THRESHOLD:
                continue  # D-06: skip empty pages
            yield page_num, text, title
    finally:
        doc.close()


def extract_docx_pages(
    filepath: str,
) -> Iterator[tuple[int, str, str]]:
    """Extract text in 20-paragraph chunks from a DOCX file (D-04).

    Yields (chunk_num, text, title).
    D-04: fixed 20-paragraph windows (NOT Seewald's contains_page_break heuristic).
    """
    doc = _DocxDoc(filepath)
    basename = os.path.basename(filepath)
    try:
        title = doc.core_properties.title or basename
    except Exception:
        title = basename

    paragraphs = [p.text for p in doc.paragraphs]
    chunk_num = 0
    for start in range(0, max(len(paragraphs), 1), _DOCX_CHUNK_PARAGRAPHS):
        chunk_num += 1
        chunk_paras = paragraphs[start : start + _DOCX_CHUNK_PARAGRAPHS]
        text = "\n".join(p for p in chunk_paras if p.strip())
        if text.strip():
            yield chunk_num, text, title


def extract_txt(filepath: str) -> Iterator[tuple[int, str, str]]:
    """TXT extraction (D-07 + MEDIUM-2 review fix: strict utf-8-sig + cp1255 fallback;
    encoding_error on both failures - no silent replacement-character indexing).

    Encoding policy:
      1. Try utf-8-sig with errors='strict'.
      2. On UnicodeDecodeError, try cp1255 with errors='strict' (legacy Windows Hebrew).
      3. On second UnicodeDecodeError, raise EncodingError - caller (LocalIndexer._index_one_file)
         catches and sets local_files.extraction_status = 'encoding_error'. No docs emitted.

    DO NOT use errors='replace'. The replacement character (U+FFFD) silently corrupts
    the index and breaks search for the affected files (MEDIUM-2 review fix).
    """
    try:
        with open(filepath, "r", encoding="utf-8-sig", errors="strict") as f:
            text = f.read()
    except UnicodeDecodeError as utf8_err:
        try:
            with open(filepath, "r", encoding="cp1255", errors="strict") as f:
                text = f.read()
            logger.info(
                "extract_txt fallback to cp1255 for %s (utf-8-sig failed: %s)",
                filepath,
                utf8_err,
            )
        except UnicodeDecodeError as cp1255_err:
            raise EncodingError(
                f"Cannot decode {filepath} as utf-8-sig or cp1255: "
                f"utf-8 error: {utf8_err}; cp1255 error: {cp1255_err}"
            ) from cp1255_err
    yield (1, text, os.path.basename(filepath))


# ---------------------------------------------------------------------------
# LocalIndexer class
# ---------------------------------------------------------------------------

class LocalIndexer:
    """Qt-free LOCAL indexer engine (REQ-1, REQ-3, REQ-4, REQ-5).

    Owns:
      - Tantivy LOCAL side-index (build_local_schema)
      - SQLite metadata sidecar (folders / processed_files / local_pages / local_files)
      - Per-format extraction (PDF / DOCX / TXT)
      - Incremental re-extract on mtime change
      - Crash-safe delete via pending_delete two-phase protocol (HIGH-3)
      - Startup crash recovery (_recover_pending_deletes + pending inserts)
      - Folder overlap detection
      - Cooperative cancellation hooks

    Constructor:
        index_dir: Directory for LOCAL Tantivy side-index files.
        lab_index_dir: Directory for LOCAL LAB side-index files (may be unused by Plan 03).
        db_path: Path to SQLite sidecar (local_index.sqlite3 or any path).
        progress_cb: Optional callable(current_index, total, filename) for per-file progress.
        file_finished_cb: Optional callable(filename, status, pages, error_msg).
    """

    def __init__(
        self,
        index_dir: str,
        lab_index_dir: str,
        db_path: str,
        progress_cb: Optional[Callable] = None,
        file_finished_cb: Optional[Callable] = None,
    ) -> None:
        self._index_dir = index_dir
        self._lab_index_dir = lab_index_dir
        self._db_path = db_path
        self._progress_cb = progress_cb
        self._file_finished_cb = file_finished_cb

        # SQLite connection
        self._conn = init_sqlite(db_path)
        self._conn.row_factory = sqlite3.Row

        # Tantivy LOCAL side-index
        schema = build_local_schema()
        os.makedirs(index_dir, exist_ok=True)
        try:
            self._index = tantivy.Index.open(index_dir)
        except Exception:
            # Schema mismatch or missing — create fresh
            self._index = tantivy.Index(schema, path=index_dir)
        self._writer = self._index.writer(heap_size=15_000_000)

        # Batch tracking for two-phase commit (D-21)
        self._pending_filepaths: list[str] = []

        # HIGH-3 review fix: run pending-delete recovery at init
        recovered = self._recover_pending_deletes()
        if recovered:
            logger.info("LocalIndexer init: recovered %d pending deletes", recovered)

    # ------------------------------------------------------------------
    # Folder management
    # ------------------------------------------------------------------

    def add_folder(self, path: str) -> bool:
        """Normalize path, check for overlap with existing folders, and INSERT.

        Returns True if added, False if already present or overlapping.
        Raises ValueError with an explanatory message on overlap.
        """
        canonical = _canonical_filepath(path)
        existing = self._get_folder_paths()
        conflict = check_folder_overlap(canonical, existing)
        if conflict is not None:
            raise ValueError(
                f"Folder '{path}' overlaps with existing registered folder '{conflict}'"
            )
        now = time.time()
        try:
            self._conn.execute(
                "INSERT INTO folders (path, added_at, status) VALUES (?, ?, 'active')",
                (canonical, now),
            )
            self._conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Already exists (UNIQUE constraint)
            return False

    def remove_folder(self, folder_path: str) -> int:
        """Synchronous delete: remove all files in folder, then delete the folder row.

        Returns the number of files removed.
        """
        canonical = _canonical_filepath(folder_path)
        row = self._conn.execute(
            "SELECT folder_id FROM folders WHERE path = ?", (canonical,)
        ).fetchone()
        if row is None:
            return 0
        folder_id = row["folder_id"]
        # Get all sys_ids + filepaths for this folder
        files = self._conn.execute(
            "SELECT sys_id, filepath FROM local_files WHERE folder_id = ?",
            (folder_id,),
        ).fetchall()
        count = 0
        for f in files:
            self._delete_file(f["sys_id"], f["filepath"])
            count += 1
        # Commit Tantivy
        try:
            self._writer.commit()
        except Exception as exc:
            logger.warning("remove_folder: Tantivy commit failed: %s", exc)
        # Delete folder row
        self._conn.execute("DELETE FROM folders WHERE folder_id = ?", (folder_id,))
        self._conn.commit()
        return count

    def _get_folder_paths(self) -> list[str]:
        rows = self._conn.execute("SELECT path FROM folders").fetchall()
        return [r["path"] for r in rows]

    # ------------------------------------------------------------------
    # Scan
    # ------------------------------------------------------------------

    def scan_all(self, cancel_check: Callable[[], bool] = lambda: False) -> dict:
        """Scan all registered folders; return summary dict.

        Returns: {"indexed": N, "skipped": N, "errors": N, "cancelled": bool}
        """
        result = {"indexed": 0, "skipped": 0, "errors": 0, "cancelled": False}
        folders = self._conn.execute(
            "SELECT folder_id, path, status FROM folders"
        ).fetchall()

        for folder in folders:
            if cancel_check():
                result["cancelled"] = True
                break

            folder_path = folder["path"]
            folder_id = folder["folder_id"]

            # D-40: check folder availability
            if not os.path.isdir(folder_path):
                self._conn.execute(
                    "UPDATE folders SET status = 'unavailable' WHERE folder_id = ?",
                    (folder_id,),
                )
                self._conn.commit()
                logger.info("Folder '%s' unavailable — preserving existing rows", folder_path)
                continue

            # Mark folder as active + update last_scanned_at
            self._conn.execute(
                "UPDATE folders SET status = 'active', last_scanned_at = ? WHERE folder_id = ?",
                (time.time(), folder_id),
            )
            self._conn.commit()

            # Build set of current files on disk
            disk_files: dict[str, int] = {}  # canonical_path -> file_size
            for filepath, file_size in self._iterate_supported_files(
                folder_path, cancel_check
            ):
                disk_files[filepath] = file_size

            if cancel_check():
                result["cancelled"] = True
                break

            # Get cached files for this folder
            cached = {
                row["filepath"]: row
                for row in self._conn.execute(
                    "SELECT pf.filepath, pf.mtime, pf.size, pf.sys_id, pf.status "
                    "FROM processed_files pf "
                    "JOIN local_files lf ON pf.sys_id = lf.sys_id "
                    "WHERE lf.folder_id = ?",
                    (folder_id,),
                ).fetchall()
            }

            # Detect deleted files (in cache but not on disk)
            for cached_path, cached_row in list(cached.items()):
                if cached_path not in disk_files:
                    if cancel_check():
                        result["cancelled"] = True
                        break
                    sys_id = cached_row["sys_id"]
                    if sys_id:
                        self._delete_file(sys_id, cached_path)

            if result["cancelled"]:
                break

            # Index new / modified files
            total_files = len(disk_files)
            for idx, (filepath, file_size) in enumerate(disk_files.items()):
                if cancel_check():
                    result["cancelled"] = True
                    break

                cached_row = cached.get(filepath)
                try:
                    stat = os.stat(filepath)
                    mtime = stat.st_mtime
                    fsize = stat.st_size
                except OSError as exc:
                    logger.warning("scan_all: stat failed for %s: %s", filepath, exc)
                    result["errors"] += 1
                    continue

                # Check if unchanged
                if (
                    cached_row is not None
                    and cached_row["status"] == "committed"
                    and abs((cached_row["mtime"] or 0) - mtime) < 0.01
                    and (cached_row["size"] or 0) == fsize
                ):
                    result["skipped"] += 1
                    continue

                # Fire progress callback only for files actually being indexed/re-indexed
                # (not for cache hits) — matches D-23 "per file being processed" semantics
                if self._progress_cb:
                    self._progress_cb(idx, total_files, os.path.basename(filepath))

                # Need to index (new or modified)
                if cached_row is not None and cached_row["sys_id"]:
                    # Modified: delete old docs first (D-36)
                    self._delete_file(cached_row["sys_id"], filepath)

                status, pages = self._index_one_file(filepath, folder_id, cancel_check)
                if status in ("ok", "no_text_layer", "encoding_error", "unsupported"):
                    result["indexed"] += 1
                else:
                    result["errors"] += 1

                if self._file_finished_cb:
                    self._file_finished_cb(
                        os.path.basename(filepath), status, pages, ""
                    )

                # Batch commit boundary
                if len(self._pending_filepaths) >= _COMMIT_BATCH_SIZE:
                    self._commit_batch()

        # Final commit
        if not result["cancelled"] and self._pending_filepaths:
            self._commit_batch()

        return result

    def prescan_count(self, folder_path: str) -> tuple[int, int]:
        """Fast count of supported files + total bytes for D-26 ceiling dialog."""
        file_count = 0
        total_bytes = 0
        try:
            for dirpath, _dirs, files in os.walk(folder_path, followlinks=False):
                try:
                    for fname in files:
                        ext = os.path.splitext(fname)[1].lower()
                        if ext in _SUPPORTED_EXTENSIONS:
                            fpath = os.path.join(dirpath, fname)
                            try:
                                total_bytes += os.path.getsize(fpath)
                                file_count += 1
                            except OSError:
                                pass
                except OSError as exc:
                    logger.warning("prescan_count: OSError in %s: %s", dirpath, exc)
        except OSError as exc:
            logger.warning("prescan_count: OSError walking %s: %s", folder_path, exc)
        return file_count, total_bytes

    # ------------------------------------------------------------------
    # Startup recovery
    # ------------------------------------------------------------------

    def startup_recovery(self) -> dict:
        """Two-pass startup recovery (D-21 + HIGH-3 review fix).

        Pass A (HIGH-3): recover any crash-interrupted deletes (_recover_pending_deletes).
        Pass B (D-21): re-extract files with status='pending'.

        Returns: {'pending_deletes_recovered': N, 'pending_inserts_recovered': M}
        """
        # Pass A: HIGH-3 - finish any interrupted deletes
        deletes_recovered = self._recover_pending_deletes()

        # Pass B: D-21 - re-extract pending inserts
        pending_rows = self._conn.execute(
            "SELECT filepath, sys_id FROM processed_files WHERE status = 'pending'"
        ).fetchall()
        inserts_recovered = 0
        for row in pending_rows:
            filepath = row["filepath"]
            sys_id = row["sys_id"]
            if not os.path.exists(filepath):
                # File gone - clean up
                if sys_id:
                    self._delete_file(sys_id, filepath)
                else:
                    self._conn.execute(
                        "DELETE FROM processed_files WHERE filepath = ?", (filepath,)
                    )
                    self._conn.commit()
                continue
            # Delete stale data and re-extract
            if sys_id:
                self._delete_file(sys_id, filepath)
            # Find folder_id for this file
            folder_row = self._conn.execute(
                "SELECT f.folder_id FROM folders f WHERE ? LIKE f.path || '%'",
                (filepath,),
            ).fetchone()
            folder_id = folder_row["folder_id"] if folder_row else 1
            self._index_one_file(filepath, folder_id, lambda: False)
            inserts_recovered += 1

        if self._pending_filepaths:
            self._commit_batch()

        return {
            "pending_deletes_recovered": deletes_recovered,
            "pending_inserts_recovered": inserts_recovered,
        }

    def _recover_pending_deletes(self) -> int:
        """HIGH-3 review fix: replay Tantivy delete + SQLite cleanup for pending rows.

        Called at __init__ AND from startup_recovery() Pass A.
        Idempotent: re-deleting already-deleted Tantivy UIDs is a no-op.

        Returns: count of completed deletes.
        """
        pending_rows = self._conn.execute(
            "SELECT sys_id FROM local_files WHERE pending_delete = 1"
        ).fetchall()
        n = 0
        for row in pending_rows:
            sys_id = row["sys_id"]
            # Step 2: Tantivy delete-by-uid loop + commit (idempotent)
            uid_rows = self._conn.execute(
                "SELECT uid FROM local_pages WHERE sys_id = ?", (sys_id,)
            ).fetchall()
            for uid_row in uid_rows:
                uid = uid_row["uid"]
                self._writer.delete_documents("unique_id", uid)
            try:
                self._writer.commit()
            except Exception as exc:
                logger.warning(
                    "_recover_pending_deletes: Tantivy commit failed for sys_id=%s: %s",
                    sys_id, exc,
                )
                continue
            # Step 3: SQLite final cleanup
            self._conn.execute("DELETE FROM local_pages WHERE sys_id = ?", (sys_id,))
            self._conn.execute("DELETE FROM local_files WHERE sys_id = ?", (sys_id,))
            self._conn.execute(
                "DELETE FROM processed_files WHERE sys_id = ?", (sys_id,)
            )
            self._conn.commit()
            n += 1
        logger.info("HIGH-3 recovery: completed %d pending deletes", n)
        return n

    # ------------------------------------------------------------------
    # Internal indexing helpers
    # ------------------------------------------------------------------

    def _iterate_supported_files(
        self,
        folder_path: str,
        cancel_check: Callable[[], bool],
    ) -> Iterator[tuple[str, int]]:
        """Yield (canonical_filepath, file_size) for ALL files in folder.

        Note: unsupported extensions are included so _index_one_file can record
        them with extraction_status='unsupported'. The extension check happens
        inside _index_one_file, not here.
        """
        try:
            for dirpath, _dirs, files in os.walk(folder_path, followlinks=False):
                if cancel_check():
                    return
                try:
                    for fname in files:
                        fpath = os.path.join(dirpath, fname)
                        canonical = _canonical_filepath(fpath)
                        try:
                            fsize = os.path.getsize(fpath)
                        except OSError:
                            fsize = 0
                        yield canonical, fsize
                except OSError as exc:
                    logger.warning(
                        "_iterate_supported_files: OSError in %s: %s", dirpath, exc
                    )
        except OSError as exc:
            logger.warning(
                "_iterate_supported_files: OSError walking %s: %s", folder_path, exc
            )

    def _index_one_file(
        self,
        filepath: str,
        folder_id: int,
        cancel_check: Callable[[], bool],
    ) -> tuple[str, int]:
        """Index a single file: extract pages, write Tantivy docs, mark pending.

        Returns (extraction_status, page_count).
        """
        canonical = _canonical_filepath(filepath)
        ext = os.path.splitext(filepath)[1].lower()

        # Generate sys_id with collision retry
        sys_id = None
        for slot in range(_MAX_COLLISION_RETRIES):
            candidate = generate_local_sys_id(canonical, slot=slot)
            # Check for collision (another file already owns this sys_id)
            existing = self._conn.execute(
                "SELECT filepath FROM local_files WHERE sys_id = ?", (candidate,)
            ).fetchone()
            if existing is None or existing["filepath"] == canonical:
                sys_id = candidate
                break
            logger.warning(
                "_index_one_file: sys_id collision at slot %d for %s, retrying",
                slot, canonical,
            )
        if sys_id is None:
            logger.error(
                "_index_one_file: exhausted %d collision slots for %s",
                _MAX_COLLISION_RETRIES, canonical,
            )
            return ("error", 0)

        try:
            file_size = os.path.getsize(canonical)
            mtime = os.path.getmtime(canonical)
        except OSError as exc:
            logger.warning("_index_one_file: cannot stat %s: %s", canonical, exc)
            return ("error", 0)

        basename = os.path.basename(canonical)

        # Mark as pending BEFORE extraction (two-phase commit step 1)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO processed_files (filepath, mtime, size, sys_id, status)
            VALUES (?, ?, ?, ?, 'pending')
            """,
            (canonical, mtime, file_size, sys_id),
        )
        self._conn.commit()

        # Extract pages
        if ext not in _SUPPORTED_EXTENSIONS:
            return self._finish_file(
                sys_id, canonical, folder_id, basename, ext,
                0, file_size, "unsupported", None, mtime,
            )

        pages_written = 0
        extraction_status = "ok"
        error_msg = None
        display_title = basename

        try:
            if ext == ".pdf":
                pages_written, extraction_status, display_title = self._extract_and_write_pdf(
                    sys_id, canonical, folder_id, cancel_check
                )
            elif ext == ".docx":
                pages_written, extraction_status, display_title = self._extract_and_write_docx(
                    sys_id, canonical, folder_id, cancel_check
                )
            elif ext == ".txt":
                pages_written, extraction_status, display_title = self._extract_and_write_txt(
                    sys_id, canonical, folder_id
                )
        except EncodingError as exc:
            extraction_status = "encoding_error"
            error_msg = str(exc)
            logger.info("_index_one_file: encoding_error for %s: %s", canonical, exc)
        except Exception as exc:
            extraction_status = "error"
            error_msg = str(exc)
            logger.warning("_index_one_file: extraction error for %s: %s", canonical, exc)

        return self._finish_file(
            sys_id, canonical, folder_id, basename, ext,
            pages_written, file_size, extraction_status, error_msg, mtime,
            display_title=display_title,
        )

    def _finish_file(
        self,
        sys_id: str,
        filepath: str,
        folder_id: int,
        basename: str,
        ext: str,
        page_count: int,
        file_size: int,
        extraction_status: str,
        error_msg: Optional[str],
        mtime: float,
        display_title: Optional[str] = None,
    ) -> tuple[str, int]:
        """Upsert local_files row. Returns (extraction_status, page_count)."""
        now = time.time()
        self._conn.execute(
            """
            INSERT OR REPLACE INTO local_files (
                sys_id, filepath, folder_id, display_title,
                original_filename, file_extension, page_count,
                file_size_bytes, extraction_status, last_indexed_at,
                sha256_full, error_msg, pending_delete
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, 0)
            """,
            (
                sys_id, filepath, folder_id, display_title or basename,
                basename, ext, page_count,
                file_size, extraction_status, now,
                error_msg,
            ),
        )
        self._conn.commit()
        # Track for batch commit
        self._pending_filepaths.append(filepath)
        return (extraction_status, page_count)

    def _write_page_doc(
        self,
        sys_id: str,
        page_num: int,
        text: str,
        title: str,
        folder_id: int,
    ) -> str:
        """Write one Tantivy doc for a page and record in local_pages. Returns uid."""
        # Get file_id from local_files (may not be committed yet, try 0 as fallback)
        file_row = self._conn.execute(
            "SELECT file_id FROM local_files WHERE sys_id = ?", (sys_id,)
        ).fetchone()
        file_id = file_row["file_id"] if file_row else 0

        uid = _make_uid(sys_id, page_num)
        full_header = _make_full_header(sys_id, page_num, file_id)
        basename = os.path.basename(
            self._conn.execute(
                "SELECT filepath FROM processed_files WHERE sys_id = ?", (sys_id,)
            ).fetchone()["filepath"]
        )
        # Build content_head / content_tail for snippet generation
        words = text.split()
        head = " ".join(words[:50]) if words else ""
        tail = " ".join(words[-50:]) if words else ""

        doc = tantivy.Document(
            unique_id=[uid],
            content=[text],
            content_head=[head],
            content_tail=[tail],
            line_starts=[""],
            line_ends=[""],
            source=["LOCAL"],
            full_header=[full_header],
            shelfmark=[basename],
            scope=["page"],
            boundaries=[""],
        )
        self._writer.add_document(doc)

        # Track page UID in SQLite
        self._conn.execute(
            "INSERT OR REPLACE INTO local_pages (sys_id, uid, page_num) VALUES (?, ?, ?)",
            (sys_id, uid, page_num),
        )
        self._conn.commit()
        return uid

    def _extract_and_write_pdf(
        self,
        sys_id: str,
        filepath: str,
        folder_id: int,
        cancel_check: Callable[[], bool],
    ) -> tuple[int, str, str]:
        """Extract PDF pages and write Tantivy docs. Returns (pages_written, status, title)."""
        pages_written = 0
        total_chars = 0
        display_title = os.path.basename(filepath)

        for page_num, text, title in extract_pdf_pages(filepath):
            if cancel_check():
                # Rollback partial pages
                self._rollback_partial(sys_id)
                return (pages_written, "cancelled", display_title)
            display_title = title
            total_chars += len(text)
            self._write_page_doc(sys_id, page_num, text, title, folder_id)
            pages_written += 1

        if total_chars < _SCANNED_PDF_CHAR_THRESHOLD and pages_written == 0:
            return (0, "no_text_layer", display_title)

        return (pages_written, "ok", display_title)

    def _extract_and_write_docx(
        self,
        sys_id: str,
        filepath: str,
        folder_id: int,
        cancel_check: Callable[[], bool],
    ) -> tuple[int, str, str]:
        """Extract DOCX chunks and write Tantivy docs. Returns (pages_written, status, title)."""
        pages_written = 0
        display_title = os.path.basename(filepath)

        for chunk_num, text, title in extract_docx_pages(filepath):
            if cancel_check():
                self._rollback_partial(sys_id)
                return (pages_written, "cancelled", display_title)
            display_title = title
            self._write_page_doc(sys_id, chunk_num, text, title, folder_id)
            pages_written += 1

        return (pages_written, "ok", display_title)

    def _extract_and_write_txt(
        self,
        sys_id: str,
        filepath: str,
        folder_id: int,
    ) -> tuple[int, str, str]:
        """Extract TXT and write a single Tantivy doc. Returns (pages_written, status, title)."""
        pages_written = 0
        display_title = os.path.basename(filepath)

        for page_num, text, title in extract_txt(filepath):
            display_title = title
            self._write_page_doc(sys_id, page_num, text, title, folder_id)
            pages_written += 1

        return (pages_written, "ok", display_title)

    def _rollback_partial(self, sys_id: str) -> None:
        """Roll back partial page docs for a file that was cancelled mid-extraction."""
        # Remove any local_pages rows added so far for this sys_id
        uid_rows = self._conn.execute(
            "SELECT uid FROM local_pages WHERE sys_id = ?", (sys_id,)
        ).fetchall()
        for uid_row in uid_rows:
            self._writer.delete_documents("unique_id", uid_row["uid"])
        try:
            self._writer.rollback()
        except Exception:
            pass
        try:
            self._writer = self._index.writer(heap_size=15_000_000)
        except Exception:
            pass
        self._conn.execute("DELETE FROM local_pages WHERE sys_id = ?", (sys_id,))
        self._conn.execute("DELETE FROM processed_files WHERE sys_id = ?", (sys_id,))
        self._conn.commit()

    # ------------------------------------------------------------------
    # Delete (HIGH-3 three-step crash-safe protocol)
    # ------------------------------------------------------------------

    def _delete_file(self, sys_id: str, filepath: str) -> None:
        """Crash-safe three-step delete (HIGH-3 review fix).

        Step 1: Mark pending_delete=1 in SQLite (durable; recovery picks this up).
        Step 2: Tantivy delete-by-uid + commit.
        Step 3: SQLite final cleanup.

        If crash between step 1 and step 2: _recover_pending_deletes() replays steps 2-3.
        If crash between step 2 and step 3: _recover_pending_deletes() re-runs step 2
                                             (idempotent) then step 3.
        """
        # Step 1: Mark pending_delete
        self._conn.execute(
            "UPDATE local_files SET pending_delete = 1 WHERE sys_id = ?", (sys_id,)
        )
        self._conn.commit()

        # Step 2: Tantivy delete-by-uid
        uid_rows = self._conn.execute(
            "SELECT uid FROM local_pages WHERE sys_id = ?", (sys_id,)
        ).fetchall()
        for uid_row in uid_rows:
            uid = uid_row["uid"]
            self._writer.delete_documents("unique_id", uid)
        try:
            self._writer.commit()
        except Exception as exc:
            logger.warning(
                "_delete_file: Tantivy commit failed for sys_id=%s: %s", sys_id, exc
            )
            return  # Step 3 will be replayed by recovery

        # Step 3: SQLite final cleanup
        self._conn.execute("DELETE FROM local_pages WHERE sys_id = ?", (sys_id,))
        self._conn.execute("DELETE FROM local_files WHERE sys_id = ?", (sys_id,))
        self._conn.execute(
            "DELETE FROM processed_files WHERE sys_id = ?", (sys_id,)
        )
        self._conn.commit()

    # ------------------------------------------------------------------
    # Two-phase commit (D-21)
    # ------------------------------------------------------------------

    def _commit_batch(self) -> None:
        """Two-phase commit per D-21.

        Phase 1: Tantivy writer.commit() (Tantivy docs are now durable on disk).
        Phase 2: SQLite UPDATE status='committed' for all pending filepaths.
        """
        if not self._pending_filepaths:
            return
        # Phase 1: Tantivy commit
        self._writer.commit()
        # Phase 2: SQLite mark committed
        placeholders = ",".join("?" * len(self._pending_filepaths))
        self._conn.execute(
            f"UPDATE processed_files SET status = 'committed' "  # noqa: S608
            f"WHERE filepath IN ({placeholders})",
            self._pending_filepaths,
        )
        self._conn.commit()
        self._pending_filepaths.clear()

    # ------------------------------------------------------------------
    # Close
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Flush + close Tantivy writer and SQLite connection.

        Explicitly deletes the Tantivy writer and index objects to release the
        Tantivy lockfile (LockBusy prevention for subsequent open() calls in
        the same process or a different process).
        """
        if self._pending_filepaths:
            try:
                self._commit_batch()
            except Exception as exc:
                logger.warning("LocalIndexer.close: commit_batch failed: %s", exc)
        try:
            self._writer.commit()
        except Exception:
            pass
        # Delete writer first (releases Tantivy lockfile), then index object
        try:
            del self._writer
        except Exception:
            pass
        try:
            del self._index
        except Exception:
            pass
        try:
            self._conn.close()
        except Exception:
            pass
