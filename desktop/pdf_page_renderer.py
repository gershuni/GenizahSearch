"""Phase 99 — PDF page renderer core: PdfRenderFailure enum, DocLRU, render functions,
and PdfRenderWorker (off-thread queue-driven render worker).

This module provides the SYNCHRONOUSLY-TESTABLE render core for PDFIMG-01/02/06
plus the off-thread worker for PDFIMG-02 (D-09 single-owner single dedicated render
thread):

Render core (Plan 01 — synchronously testable, no QThread):
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
  - render_via_lru: convenience entry point for worker and tests.

Off-thread worker (Plan 02 — PDFIMG-02 off-thread half):
  - PdfRenderWorker(QThread): long-lived queue-driven render loop. The SINGLE thread
    that ever touches fitz/DocLRU (D-09a single-owner discipline). Exposes D-07
    tokenized signals (render_succeeded / render_failed) so the Phase 100 controller
    can discard stale results with a latest-wins token comparison (D-03).
  - _handle_request: per-request no-crash envelope — extracted into its own method
    so unit tests can call it SYNCHRONOUSLY without spinning the thread (REVIEW item 1
    Codex HIGH). Thread assertion lives in run(), NOT here.
  - stop(): cooperative shutdown only — NO terminate() (D-05: force-killing the fitz
    C call is unsafe). DocLRU.close_all() runs ONLY in run()'s finally on the render
    thread (single-owner; never from the caller thread).

NOT in this module (Phase 100 scope):
  - Generation-token COUNTER: owned by the Phase 100 UI controller; the worker only
    echoes the token passed into enqueue(). A bounded/coalescing enqueue is also a
    Phase 100 concern (REVIEW item 4 Codex MEDIUM).
  - ~8s QTimer watchdog + TIMEOUT PdfRenderFailure reason: Phase 100 UI-controller
    concern (D-05/D-07). This worker never emits TIMEOUT and never force-kills the C
    call; if the render wedges, the loop drains once the C call returns and the
    process reaps the thread if still wedged on exit.

DESKTOP-ONLY — no shared/ split (D-08): web has no My Library.
"""
import enum
import logging
import os
import queue
from collections import OrderedDict
from typing import NoReturn

from PyQt6.QtCore import QThread, pyqtSignal

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


# ---------------------------------------------------------------------------
# Sentinel used to unblock the blocking queue.get() in run()
# ---------------------------------------------------------------------------

_STOP = object()


# ---------------------------------------------------------------------------
# PdfRenderWorker — off-thread queue-driven render worker (PDFIMG-02 / D-09a)
# ---------------------------------------------------------------------------

class PdfRenderWorker(QThread):
    """Long-lived, queue-driven render worker.

    Owns the DocLRU and is the SINGLE thread that ever touches fitz (D-09
    option (a) — single dedicated render thread).

    Signal contract (D-07 tokenized signals):
      render_succeeded(token, sys_id, page_num, QImage)   — successful render
      render_failed(token, sys_id, page_num, reason, detail) — any failure

    The enum slot uses `object` (NOT `PdfRenderFailure`) because PyMuPDF
    enums are not Qt metatypes registered with QMetaType; `object` accepts
    any Python object across a queued cross-thread signal connection.

    Token discipline (D-03 latest-wins):
      The generation-token COUNTER lives in the Phase 100 UI controller.
      The worker only ECHOES the token passed into enqueue() — it does not
      generate, increment, or compare tokens. The Phase 100 controller
      compares the echoed token against its own counter and discards stale
      results. A bounded/coalescing enqueue is also a Phase 100 concern
      (REVIEW item 4 Codex MEDIUM). The internal queue is intentionally
      UNBOUNDED for Phase 99: latest-wins token echo protects what is
      DISPLAYED, not the render backlog.

    Shutdown discipline (D-05 / D-06):
      stop() is cooperative: sets _stopping, pushes the _STOP sentinel, and
      wait()s. It does NOT call terminate() — force-killing a thread mid-fitz
      C call is unsafe (D-05). If the thread does not exit within timeout_ms
      (meaning a render C call is wedged), stop() logs a warning and returns;
      the thread will drain on its own once the C call returns and the process
      reaps it on exit. DocLRU.close_all() runs ONLY in run()'s finally on
      the render thread — never from the caller thread (single-owner rule).
    """

    # D-07 tokenized signals.  `object` for the enum slot because PdfRenderFailure
    # is not a Qt metatype (REVIEW item 1 Codex HIGH in 99-PATTERNS.md Analog 1).
    render_succeeded = pyqtSignal(int, str, int, QImage)        # token, sys_id, page_num, image
    render_failed    = pyqtSignal(int, str, int, object, str)   # token, sys_id, page_num, PdfRenderFailure, detail  # noqa: E501

    def __init__(self, maxsize: int = 4) -> None:
        super().__init__()
        self._lru = DocLRU(maxsize)
        self._queue: queue.Queue = queue.Queue()
        self._stopping = False

    # ------------------------------------------------------------------
    # Public enqueue API (called from the UI thread by Phase 100 controller)
    # ------------------------------------------------------------------

    def enqueue(self, token: int, sys_id: str, page_num: int, filepath: str) -> bool:
        """Enqueue a render request.

        Returns True if the request was queued, False if the worker is stopping
        (REVIEW item 4 Codex MEDIUM — defined behavior after stop: drop + log;
        never silently queue work that can never emit a result).

        The token is generated by the Phase 100 controller and echoed verbatim
        in both result signals so stale results can be discarded. Do NOT call
        this method after stop() — the request will be dropped.
        """
        if self._stopping:
            logger.debug(
                "PdfRenderWorker.enqueue after stop — dropping token=%s sys_id=%s",
                token, sys_id,
            )
            return False
        self._queue.put((token, sys_id, page_num, filepath))
        return True

    # ------------------------------------------------------------------
    # Per-request render + emit (REVIEW item 1 Codex HIGH — extracted so
    # unit tests can call it SYNCHRONOUSLY without spinning the thread or
    # tripping the thread assertion which lives in run() only).
    # ------------------------------------------------------------------

    def _handle_request(self, item) -> None:  # noqa: ANN001
        """Handle a single render request.

        This method NEVER raises (PDFIMG-06 no-crash envelope — mirror
        LocalIndexerWorker.run). The outer run() loop that calls it therefore
        cannot die due to a bad render. There is intentionally NO thread
        assertion here — it lives in run() so unit tests can call
        _handle_request directly on the main thread.
        """
        token, sys_id, page_num, filepath = item
        try:
            img = render_via_lru(self._lru, filepath, page_num)
            self.render_succeeded.emit(token, sys_id, page_num, img)
        except PdfRenderError as e:
            # Plan 01's _log_and_raise already logged once; just emit.
            self.render_failed.emit(token, sys_id, page_num, e.reason, e.detail)
        except Exception as exc:  # noqa: BLE001 — no-crash contract; thread must survive
            logger.exception(
                "PdfRenderWorker: unexpected render error token=%s sys_id=%s",
                token, sys_id,
            )
            self.render_failed.emit(
                token, sys_id, page_num, PdfRenderFailure.RENDER_ERROR, str(exc)
            )

    # ------------------------------------------------------------------
    # run() — the long-lived queue loop (single-owner fitz/LRU discipline)
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Long-lived blocking queue loop.

        Single-owner discipline: ALL fitz access (the LRU + render) happens on
        THIS thread. The thread assertion AND DocLRU.close_all() teardown both
        live here — NOT in stop() (which runs on the caller thread — REVIEW
        item 2 Codex HIGH).

        The loop blocks on queue.get() until a request or the _STOP sentinel
        arrives. Items already in the queue AHEAD of _STOP are drained FIFO
        first; only items enqueued AFTER stop() are dropped by the enqueue
        guard. This is the intended behavior.
        """
        self._assert_worker_thread()        # single-owner guard — render thread only
        try:
            while True:
                item = self._queue.get()    # blocks until work or the _STOP sentinel
                if item is _STOP:
                    break
                self._handle_request(item)
        finally:
            self._lru.close_all()           # D-06: all docs closed ON the render thread

    # ------------------------------------------------------------------
    # Cooperative shutdown — NO terminate(), NO caller-thread LRU access
    # (REVIEW item 2 Codex HIGH; D-05)
    # ------------------------------------------------------------------

    def stop(self, timeout_ms: int = 5000) -> None:
        """Request cooperative shutdown.

        Sets the stop flag (so enqueue() drops new requests), pushes the _STOP
        sentinel to unblock the blocking queue.get(), then wait()s up to
        timeout_ms for the thread to exit.

        If the thread does not exit within timeout_ms (a wedged fitz C call
        cannot be safely force-killed — D-05), logs a warning and returns.
        The thread will drain on its own once the C call returns; the process
        reaps it on exit if still wedged.

        IMPORTANT: This method deliberately does NOT call self._lru.close_all().
        That would access fitz from the caller thread, violating the single-owner
        rule (D-09a). DocLRU teardown runs ONLY in run()'s finally on the render
        thread.
        """
        self._stopping = True
        self._queue.put(_STOP)              # unblock the blocking get()
        if not self.wait(timeout_ms):
            # A wedged C call cannot be safely killed (D-05). Log and let the
            # thread drain: it will hit _STOP and close the LRU in run()'s
            # finally once the in-flight render returns; process exit reaps it
            # if still wedged. UI is never blocked (work is off-thread).
            logger.warning(
                "PdfRenderWorker did not stop within %dms; leaving it to drain "
                "(wedged C call cannot be force-killed per D-05)",
                timeout_ms,
            )
        # Deliberately NO self._lru.close_all() here — that would touch fitz off
        # the render thread (single-owner violation). run()'s finally owns LRU
        # teardown.

    # ------------------------------------------------------------------
    # Single-owner thread assertion (RESEARCH Pitfall 3 / Open Question 2)
    # ------------------------------------------------------------------

    def _assert_worker_thread(self) -> None:
        """Assert that we are executing on the render thread (not the UI thread).

        Called ONCE at the top of run() to guard against future contributors
        accidentally calling render/LRU code from a different thread. NOT called
        in _handle_request (would break synchronous unit tests) or in stop()
        / enqueue() (which correctly run on the caller thread).
        """
        assert QThread.currentThread() is self, (  # noqa: S101
            "fitz/LRU touched off the render thread — single-owner discipline "
            "violated (D-09a / T-99-04)"
        )
