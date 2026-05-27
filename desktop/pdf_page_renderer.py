"""Phase 99 — PDF page renderer core: PdfRenderFailure enum, DocLRU, render functions.

This module provides the SYNCHRONOUSLY-TESTABLE render core for PDFIMG-01/02/06:
  - PdfRenderFailure enum (D-04): all 8 classified failure reasons.
  - PdfRenderError exception (D-04): typed error carrying reason + detail.
  - _log_and_raise (REVIEW item 1): single logging chokepoint — logs EXACTLY ONCE
    then raises; every failure path routes through this helper.
  - _open_doc_classified (D-04 classification order): classifies every failure at
    open time before any render starts.
  - DocLRU (D-06): bounded OrderedDict LRU of open fitz.Document handles; explicitly
    .close()'d on eviction and on shutdown; best-effort (swallows close errors).
  - render_page (D-01/D-01b/D-04a): renders a single page to a memory-safe copied
    QImage; validates page bounds BEFORE any render call.
  - render_via_lru: convenience entry point for worker (Plan 02) and tests.

DESKTOP-ONLY — no shared/ split (D-08): web has no My Library.

Phase 100 wires the QThread worker, display, and timeout watchdog.
D-03 token-echo: the Plan 02 worker accepts token per request and echoes it in
  render_succeeded / render_failed signals so stale results are discardable.
"""
import enum
import logging
import os
from collections import OrderedDict
from typing import NoReturn

import fitz  # PyMuPDF — D-01; already a desktop dep via Phase 95

# Phase 97.3 R97.3-C belt-and-suspenders: silence PyMuPDF stderr noise.
# (Inherited automatically if shared.local_indexer loads first — A3 — but
# set explicitly here so this module is safe to import standalone.)
try:
    fitz.TOOLS.mupdf_display_warnings(False)
except Exception as _e:  # noqa: BLE001
    logging.getLogger(__name__).debug(
        "mupdf_display_warnings unavailable: %s", _e
    )
try:
    fitz.TOOLS.mupdf_display_errors(False)
except Exception as _e:  # noqa: BLE001
    logging.getLogger(__name__).debug(
        "mupdf_display_errors unavailable: %s", _e
    )

from PyQt6.QtGui import QImage

from genizah_core import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------------

RENDER_DPI = 200  # D-01: fixed DPI — NOT adaptive; 200 gives ~1600px wide for A4


# ---------------------------------------------------------------------------
# Failure enum + exception
# ---------------------------------------------------------------------------

class PdfRenderFailure(enum.Enum):
    """Classified failure reasons for PDF page rendering (D-04).

    Enum values are stable string identifiers — Phase 100 maps each to a
    user-visible placeholder string.

    Notes:
      TIMEOUT  — emitted by the Phase 100 UI watchdog (D-05/D-07), NOT by
                 this module. This worker never emits TIMEOUT; the ~8s QTimer
                 in the UI controller fires and shows the placeholder; the
                 late render result (if it arrives) is silently dropped.
      CANCELLED — stale / superseded request (latest-wins token logic, D-03).
    """

    MISSING_FILE = "missing-file"
    NOT_PDF = "not-pdf"
    ENCRYPTED = "encrypted"
    CORRUPT = "corrupt"
    PAGE_OUT_OF_RANGE = "page-out-of-range"
    RENDER_ERROR = "render-error"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


class PdfRenderError(Exception):
    """Raised by every render failure path (D-04).

    Carries the classified reason and a human-readable detail string.
    The Plan 02 worker catches this and routes it to the render_failed signal.
    """

    def __init__(self, reason: PdfRenderFailure, detail: str) -> None:
        super().__init__(f"{reason.value}: {detail}")
        self.reason = reason
        self.detail = detail


# ---------------------------------------------------------------------------
# Single logging chokepoint (REVIEW item 1 — Codex MEDIUM)
# ---------------------------------------------------------------------------

def _log_and_raise(reason: PdfRenderFailure, detail: str) -> NoReturn:
    """Log exactly ONE warning record then raise PdfRenderError.

    This is the ONLY place PdfRenderError is constructed and raised.
    EVERY failure classification site calls _log_and_raise — never a bare raise.
    Guarantees: (a) every failure is logged, (b) it is logged exactly once
    (no double-logging when the worker catches the error), (c) direct callers
    of render_via_lru / _open_doc_classified also get a guaranteed log record.
    """
    logger.warning("pdf render failure: %s — %s", reason.value, detail)
    raise PdfRenderError(reason, detail)


# ---------------------------------------------------------------------------
# Document open + classification (D-04 classification order)
# ---------------------------------------------------------------------------

def _open_doc_classified(filepath: str) -> fitz.Document:
    """Open a fitz.Document, classifying every error into PdfRenderFailure.

    Classification order (D-04):
      1. Existence check (os.path.exists wrapped in try/except OSError so an
         illegal/bad path — e.g. NUL bytes — classifies as MISSING_FILE).
      2. Extension check (.pdf suffix, case-insensitive).
      3. fitz.open() — any exception → CORRUPT.
      4. doc.needs_pass → ENCRYPTED (some PDFs open fine but need a password).
      Returns the opened doc on success (caller must close it eventually).

    Note: this function does NOT check page bounds — that is done in render_page
    (D-04a) just before the get_pixmap call.
    """
    # Step 1: existence (wrap in try/except so a bad path ≠ unhandled exception)
    try:
        exists = os.path.exists(filepath)
    except OSError as e:
        _log_and_raise(PdfRenderFailure.MISSING_FILE, f"{filepath}: {e}")

    if not exists:
        _log_and_raise(PdfRenderFailure.MISSING_FILE, filepath)

    # Step 2: extension (case-insensitive: .PDF / .Pdf / .pdf all accepted)
    if not filepath.lower().endswith(".pdf"):
        _log_and_raise(PdfRenderFailure.NOT_PDF, filepath)

    # Step 3: fitz.open — any exception is a corrupt/unreadable file
    try:
        doc = fitz.open(filepath)
    except Exception as e:
        _log_and_raise(PdfRenderFailure.CORRUPT, str(e))

    # Step 4: encrypted (needs_pass = True means we opened it but can't render)
    if doc.needs_pass:
        doc.close()
        _log_and_raise(PdfRenderFailure.ENCRYPTED, filepath)

    return doc


# ---------------------------------------------------------------------------
# DocLRU — bounded LRU of open fitz.Document handles (D-06)
# ---------------------------------------------------------------------------

class DocLRU:
    """Bounded LRU cache of open fitz.Document handles.

    Keyed by canonical filepath. Evicts the oldest (least recently used) handle
    when the cache grows past maxsize; calls .close() on eviction (D-06).
    close_all() is idempotent and closes every remaining handle on shutdown.

    IMPORTANT: The LRU MUST be touched only from a single thread (the render
    worker — D-09a single-owner discipline). fitz is not thread-safe for
    concurrent access; the single-owner model is the mitigation.

    Constraints (D-06):
      - Handles only — NEVER cache Page or Pixmap objects (memory blowup).
      - Evicted doc is .close()'d explicitly (best-effort; errors swallowed).
      - close_all() is safe to call multiple times (idempotent on empty cache).
    """

    def __init__(self, maxsize: int = 4) -> None:
        """Initialize with a bounded cache size (D-06 default 4, valid 2–8)."""
        self._cache: OrderedDict[str, fitz.Document] = OrderedDict()
        self._maxsize = maxsize

    def get(self, filepath: str) -> fitz.Document:
        """Return the open fitz.Document for filepath, opening it if not cached.

        On cache hit: move to MRU end.
        On cache miss: open via _open_doc_classified, insert, evict oldest if
        the cache exceeds maxsize (D-06 explicit close on eviction).

        Raises PdfRenderError (via _open_doc_classified) if the file cannot
        be opened or is classified as a failure.
        """
        doc = self._cache.get(filepath)
        if doc is not None:
            self._cache.move_to_end(filepath)
            return doc

        # Cache miss — open and classify
        doc = _open_doc_classified(filepath)
        self._cache[filepath] = doc

        # Evict oldest if over capacity (D-06)
        if len(self._cache) > self._maxsize:
            _, evicted = self._cache.popitem(last=False)  # oldest
            try:
                evicted.close()
            except Exception as e:  # noqa: BLE001
                # Best-effort close on eviction — a close() that raises must NOT
                # turn eviction into a render failure (REVIEW item 5 Codex LOW).
                logger.debug("evicted doc close failed: %s", e)

        return doc

    def close_all(self) -> None:
        """Close every cached handle and clear the cache (D-06 — called on shutdown).

        Idempotent: safe to call on an already-empty cache (Plan 02's run()
        finally calls this; it must not crash if the cache was already cleared).
        """
        for doc in self._cache.values():
            try:
                doc.close()
            except Exception as e:  # noqa: BLE001
                logger.debug("close_all: doc close failed: %s", e)
        self._cache.clear()


# ---------------------------------------------------------------------------
# Single-page render (D-01 / D-01b / D-04a)
# ---------------------------------------------------------------------------

def render_page(doc: fitz.Document, page_num: int) -> QImage:
    """Render exactly one page of an open fitz.Document to a copied QImage.

    Args:
        doc:      An open fitz.Document (from DocLRU.get or fitz.open).
        page_num: 1-based page number. fitz index = page_num - 1.

    Returns:
        A QImage independent of any C pixmap buffer (.copy() called — D-01b).

    Raises:
        PdfRenderError with PAGE_OUT_OF_RANGE if page_num is out of bounds.
        PdfRenderError with RENDER_ERROR for any unexpected render exception.
        An already-typed PdfRenderError propagates unchanged (never re-wrapped).

    Critical notes:
      D-04a: bounds check fires BEFORE any load_page / get_pixmap call.
      D-01b: img.copy() MUST be called before pix goes out of scope to avoid
             use-after-free (pix.samples is a C-backed buffer; QImage raw-buffer
             constructor is zero-copy by design).
      D-01:  fixed RENDER_DPI = 200; alpha=False → 3 bytes/px (Format_RGB888).
             Pass pix.stride, never a computed stride (MuPDF may pad rows).
    """
    idx = page_num - 1

    # D-04a: validate bounds BEFORE any render call
    # (Also rejects page_num=0 → idx=-1 since -1 < 0.)
    if not (0 <= idx < doc.page_count):
        _log_and_raise(
            PdfRenderFailure.PAGE_OUT_OF_RANGE,
            f"page_num={page_num}, page_count={doc.page_count}",
        )

    try:
        page = doc.load_page(idx)
        pix = page.get_pixmap(dpi=RENDER_DPI, colorspace=fitz.csRGB, alpha=False)
        # D-01b: copy() before pix is freed — mandatory; see Pitfall 1 in RESEARCH.md
        img = QImage(
            pix.samples,
            pix.width,
            pix.height,
            pix.stride,  # never compute stride manually — MuPDF may pad rows (Pitfall 2)
            QImage.Format.Format_RGB888,
        )
        return img.copy()
    except PdfRenderError:
        # Already-classified error — propagate unchanged (no re-wrap / no double-log)
        raise
    except Exception as e:
        _log_and_raise(PdfRenderFailure.RENDER_ERROR, str(e))


# ---------------------------------------------------------------------------
# Convenience entry point (used by worker in Plan 02 and by tests)
# ---------------------------------------------------------------------------

def render_via_lru(lru: DocLRU, filepath: str, page_num: int) -> QImage:
    """Open (or reuse cached) fitz.Document and render one page to a QImage.

    This is the synchronous, QThread-free render path.
    The Plan 02 worker calls this inside its run() loop; tests call it directly.

    Raises PdfRenderError on any failure (classified with reason + detail,
    logged exactly once via _log_and_raise before the raise).
    """
    doc = lru.get(filepath)
    return render_page(doc, page_num)
