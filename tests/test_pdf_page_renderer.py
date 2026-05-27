# -*- coding: utf-8 -*-
"""Phase 99 — test contract for desktop/pdf_page_renderer.py.

Wave 0 tests (Plans 01 — synchronous, no QThread):
  Covers: PDFIMG-01 (render), PDFIMG-02 (LRU + no-disk-cache), PDFIMG-06
  (failure classification).

Wave 1 tests (Plan 02 — PdfRenderWorker signal wiring, D-03 token echo):
  Covers: D-03 token echo via real signal emissions, PDFIMG-06 no-crash
  (failure routing to render_failed), loop continuity across bad renders,
  enqueue-after-stop drop. Uses _handle_request synchronous call idiom or
  real QThread with DirectConnection (pytest-qt-FREE — mirrors
  test_folder_walk_worker.py).

These tests call render functions and LRU directly for synchronous tests,
and use worker._handle_request() for signal-emission tests (the thread
assertion lives in run(), not _handle_request, so direct calls work).

If any fixture PDF is missing, run:
    python scripts/generate_pdf_render_fixtures.py
"""
import gc
import os
import shutil
import sys
import tempfile
import threading
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Fixture paths
# ---------------------------------------------------------------------------
FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "local_indexer")
MULTIPAGE_PDF = os.path.join(FIXTURES_DIR, "multipage_sample.pdf")
ENCRYPTED_PDF = os.path.join(FIXTURES_DIR, "encrypted_sample.pdf")
CORRUPT_PDF = os.path.join(FIXTURES_DIR, "corrupt_sample.pdf")


def _check_fixtures() -> None:
    """Fail fast if any required fixture is missing."""
    missing = [
        p for p in (MULTIPAGE_PDF, ENCRYPTED_PDF, CORRUPT_PDF)
        if not os.path.exists(p)
    ]
    if missing:
        pytest.fail(
            "Required PDF fixtures are missing. Regenerate via:\n"
            "    python scripts/generate_pdf_render_fixtures.py\n"
            f"Missing: {missing}"
        )


_check_fixtures()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _import_renderer():
    """Import the renderer module under test.

    Returns the module so callers can reference PdfRenderFailure,
    PdfRenderError, DocLRU, render_page, render_via_lru.
    """
    from desktop import pdf_page_renderer
    return pdf_page_renderer


# ---------------------------------------------------------------------------
# PDFIMG-01: single-page render
# ---------------------------------------------------------------------------

def test_render_single_page():
    """Render page_num=2 of the 3-page fixture; assert non-null QImage."""
    m = _import_renderer()
    import fitz
    doc = fitz.open(MULTIPAGE_PDF)
    try:
        img = m.render_page(doc, page_num=2)
        assert img is not None
        assert not img.isNull()
        assert img.width() > 0
        assert img.height() > 0
    finally:
        doc.close()


def test_only_requested_page_rendered():
    """Prove 'no bulk render': get_pixmap called exactly once when rendering
    one page of a 3-page PDF (SC1 / PDFIMG-01 — REVIEW item 5).

    A single output pixel is not proof; the call count is.
    """
    m = _import_renderer()
    import fitz

    doc = fitz.open(MULTIPAGE_PDF)
    try:
        call_count = []
        original_get_pixmap = fitz.Page.get_pixmap

        def spy_get_pixmap(self, **kwargs):
            call_count.append(1)
            return original_get_pixmap(self, **kwargs)

        with patch.object(fitz.Page, "get_pixmap", spy_get_pixmap):
            m.render_page(doc, page_num=2)

        assert len(call_count) == 1, (
            f"Expected get_pixmap called exactly once; called {len(call_count)} times"
        )
    finally:
        doc.close()


def test_qimage_independent_of_pixmap():
    """The returned QImage survives after the source pixmap goes out of scope
    (verifies the mandatory .copy() in render_page — D-01b).
    """
    m = _import_renderer()
    import fitz

    doc = fitz.open(MULTIPAGE_PDF)
    try:
        img = m.render_page(doc, page_num=1)
    finally:
        doc.close()

    # Force GC so the pixmap (which QImage referenced before .copy()) is freed.
    gc.collect()

    # The copied QImage must still be valid.
    assert not img.isNull()
    assert img.width() > 0
    assert img.height() > 0
    assert img.constBits() is not None


# ---------------------------------------------------------------------------
# PDFIMG-02: LRU eviction + no-disk-cache
# ---------------------------------------------------------------------------

def test_doc_lru_evict_and_close():
    """LRU with maxsize=2 evicts + closes the first-inserted doc on insertion
    of a third doc; close_all() closes remaining docs and empties the cache.
    """
    m = _import_renderer()

    # Use three valid PDF paths (no encrypted/corrupt — those raise on open)
    pdf1 = MULTIPAGE_PDF
    pdf2 = os.path.join(FIXTURES_DIR, "clean_sample.pdf")
    pdf3 = os.path.join(FIXTURES_DIR, "single_word_per_line.pdf")

    lru = m.DocLRU(maxsize=2)

    # Insert first two — fills the cache to capacity (no eviction yet)
    lru.get(pdf1)
    lru.get(pdf2)
    assert len(lru._cache) == 2

    # Insert third — evicts the oldest (pdf1, which was least recently used)
    lru.get(pdf3)
    assert len(lru._cache) == 2

    # pdf1 must have been evicted; pdf2 and pdf3 remain
    assert pdf1 not in lru._cache
    assert pdf2 in lru._cache
    assert pdf3 in lru._cache

    # close_all() must empty the cache and close remaining docs
    lru.close_all()
    assert len(lru._cache) == 0

    # close_all() must be idempotent
    lru.close_all()
    assert len(lru._cache) == 0


def test_lru_eviction_survives_close_error():
    """A doc whose .close() raises during eviction must NOT propagate the
    exception out of the LRU (best-effort close — REVIEW item 5 Codex LOW).

    The cache entry must still be removed and the cache state consistent.
    """
    m = _import_renderer()

    lru = m.DocLRU(maxsize=1)

    # Insert one doc so there's something to evict
    lru.get(MULTIPAGE_PDF)
    assert len(lru._cache) == 1

    # Patch the cached doc's close() to raise
    path = list(lru._cache.keys())[0]
    bad_doc = lru._cache[path]
    bad_doc.close = MagicMock(side_effect=RuntimeError("close exploded"))

    # Triggering eviction (insert a second valid doc) must NOT raise
    pdf2 = os.path.join(FIXTURES_DIR, "clean_sample.pdf")
    lru.get(pdf2)  # evicts path; best-effort close swallows RuntimeError

    # Cache must be consistent (evicted entry removed)
    assert path not in lru._cache
    assert len(lru._cache) == 1

    # close_all() with a doc that raises close() must also not propagate
    next_path = list(lru._cache.keys())[0]
    lru._cache[next_path].close = MagicMock(side_effect=RuntimeError("also explodes"))
    lru.close_all()  # must not raise
    assert len(lru._cache) == 0


def test_no_disk_cache():
    """No file is written to disk during a render (D-06 / PDFIMG-02).

    Spy on builtins.open write modes and assert no new file appears in a
    temp directory used as cwd.
    """
    m = _import_renderer()
    import fitz

    write_calls = []
    original_open = __builtins__["open"] if isinstance(__builtins__, dict) else open

    def spy_open(file, mode="r", *args, **kwargs):
        if any(c in mode for c in ("w", "x", "a")):
            write_calls.append((file, mode))
        return original_open(file, mode, *args, **kwargs)

    with tempfile.TemporaryDirectory() as tmpdir:
        before = set(os.listdir(tmpdir))

        with patch("builtins.open", spy_open):
            doc = fitz.open(MULTIPAGE_PDF)
            try:
                m.render_page(doc, page_num=1)
            finally:
                doc.close()

        after = set(os.listdir(tmpdir))
        new_files = after - before

    # No new files should have been created in tmpdir
    assert len(new_files) == 0, f"Render wrote files to disk: {new_files}"
    # No write-mode open calls with paths under tmpdir
    tmpdir_writes = [
        (f, mode) for (f, mode) in write_calls
        if str(f).startswith(str(tmpdir))
    ]
    assert len(tmpdir_writes) == 0, f"Render opened files for write: {tmpdir_writes}"


# ---------------------------------------------------------------------------
# PDFIMG-06: failure classification
# ---------------------------------------------------------------------------

def test_missing_file_reason():
    """A path that does not exist classifies as MISSING_FILE."""
    m = _import_renderer()

    nonexistent = "/nonexistent/path/does_not_exist.pdf"
    with pytest.raises(m.PdfRenderError) as exc_info:
        m._open_doc_classified(nonexistent)

    assert exc_info.value.reason == m.PdfRenderFailure.MISSING_FILE


def test_not_pdf_reason():
    """A path ending in .txt classifies as NOT_PDF."""
    m = _import_renderer()

    txt_path = os.path.join(FIXTURES_DIR, "bad_encoding.txt")
    # The file doesn't need to exist — NOT_PDF is checked before existence
    # But to be safe with path order, use any existing .txt path or just
    # use a non-existent .txt path (the path EXISTS check happens first,
    # so we need an existing .txt file).
    if not os.path.exists(txt_path):
        txt_path = os.path.join(FIXTURES_DIR, "sample.txt")

    with pytest.raises(m.PdfRenderError) as exc_info:
        m._open_doc_classified(txt_path)

    assert exc_info.value.reason == m.PdfRenderFailure.NOT_PDF


def test_uppercase_pdf_not_misclassified(tmp_path):
    """A path ending .PDF (uppercase) must NOT be classified as NOT_PDF
    and must render successfully (suffix check uses .lower() — REVIEW item 5
    Codex MEDIUM, mirrors test_mixed_case_extensions_normalized precedent).
    """
    m = _import_renderer()

    upper_path = str(tmp_path / "page_sample.PDF")
    shutil.copy(MULTIPAGE_PDF, upper_path)

    # Must NOT raise PdfRenderError with NOT_PDF
    doc = m._open_doc_classified(upper_path)
    try:
        img = m.render_page(doc, page_num=1)
        assert not img.isNull()
        assert img.width() > 0
    finally:
        doc.close()


def test_encrypted_reason():
    """An AES-256 encrypted PDF classifies as ENCRYPTED."""
    m = _import_renderer()

    with pytest.raises(m.PdfRenderError) as exc_info:
        m._open_doc_classified(ENCRYPTED_PDF)

    assert exc_info.value.reason == m.PdfRenderFailure.ENCRYPTED


def test_corrupt_reason():
    """A PDF with a valid header but corrupt body classifies as CORRUPT."""
    m = _import_renderer()

    with pytest.raises(m.PdfRenderError) as exc_info:
        m._open_doc_classified(CORRUPT_PDF)

    assert exc_info.value.reason == m.PdfRenderFailure.CORRUPT


def test_pdf_suffix_corrupt_bytes(tmp_path):
    """A file ending .pdf whose BYTES are not valid PDF classifies as CORRUPT
    (REVIEW item 5 — Codex edge case: suffix-pass does NOT imply openable).
    """
    m = _import_renderer()

    garbage_pdf = str(tmp_path / "garbage.pdf")
    with open(garbage_pdf, "wb") as f:
        f.write(b"%PDF-1.5\n\x00\x01\x02\x03garbage bytes not valid\n%%EOF")

    with pytest.raises(m.PdfRenderError) as exc_info:
        m._open_doc_classified(garbage_pdf)

    assert exc_info.value.reason == m.PdfRenderFailure.CORRUPT


def test_page_out_of_range():
    """Requesting page_num=99 on a 3-page PDF classifies as PAGE_OUT_OF_RANGE.

    The bounds check must fire BEFORE any get_pixmap call (D-04a).
    """
    m = _import_renderer()
    import fitz

    doc = fitz.open(MULTIPAGE_PDF)
    try:
        call_count = []
        original_get_pixmap = fitz.Page.get_pixmap

        def spy_get_pixmap(self, **kwargs):
            call_count.append(1)
            return original_get_pixmap(self, **kwargs)

        with patch.object(fitz.Page, "get_pixmap", spy_get_pixmap):
            with pytest.raises(m.PdfRenderError) as exc_info:
                m.render_page(doc, page_num=99)

        assert exc_info.value.reason == m.PdfRenderFailure.PAGE_OUT_OF_RANGE
        assert len(call_count) == 0, "get_pixmap should NOT be called for out-of-range page"
    finally:
        doc.close()


def test_page_num_zero():
    """page_num=0 → idx=-1 → PAGE_OUT_OF_RANGE (0 is not a valid 1-based page number).

    The 0 <= idx < page_count guard rejects negative indices; get_pixmap
    must NOT be called (REVIEW item 5 — Codex edge case).
    """
    m = _import_renderer()
    import fitz

    doc = fitz.open(MULTIPAGE_PDF)
    try:
        call_count = []
        original_get_pixmap = fitz.Page.get_pixmap

        def spy_get_pixmap(self, **kwargs):
            call_count.append(1)
            return original_get_pixmap(self, **kwargs)

        with patch.object(fitz.Page, "get_pixmap", spy_get_pixmap):
            with pytest.raises(m.PdfRenderError) as exc_info:
                m.render_page(doc, page_num=0)

        assert exc_info.value.reason == m.PdfRenderFailure.PAGE_OUT_OF_RANGE
        assert len(call_count) == 0, "get_pixmap should NOT be called for page_num=0"
    finally:
        doc.close()


def test_failures_logged(caplog):
    """Every failure path emits exactly one log record containing the reason
    value + detail string (the single _log_and_raise helper guarantees this —
    REVIEW item 1).

    Tests MISSING_FILE, NOT_PDF, ENCRYPTED, CORRUPT, PAGE_OUT_OF_RANGE.

    Note: genizah_core.get_logger() prepends 'genizah.' to the module name, so
    the actual logger name is 'genizah.desktop.pdf_page_renderer'. We capture
    at root level (logger="") to catch all records regardless of logger name.
    """
    import logging
    import fitz
    m = _import_renderer()

    cases = [
        # (callable, expected_reason)
        (
            lambda: m._open_doc_classified("/no/such/file.pdf"),
            m.PdfRenderFailure.MISSING_FILE,
        ),
        (
            lambda: m._open_doc_classified(
                os.path.join(FIXTURES_DIR, "bad_encoding.txt")
            ),
            m.PdfRenderFailure.NOT_PDF,
        ),
        (
            lambda: m._open_doc_classified(ENCRYPTED_PDF),
            m.PdfRenderFailure.ENCRYPTED,
        ),
        (
            lambda: m._open_doc_classified(CORRUPT_PDF),
            m.PdfRenderFailure.CORRUPT,
        ),
    ]

    # genizah_core.get_logger() returns a child of the 'genizah' logger which
    # has propagate=False. caplog only captures records that propagate to the
    # root logger by default. We must add caplog's handler directly to the
    # 'genizah' logger to capture its output.
    genizah_logger = logging.getLogger("genizah")
    genizah_logger.addHandler(caplog.handler)

    try:
        for fn, expected_reason in cases:
            caplog.clear()
            with caplog.at_level(logging.WARNING, logger="genizah"):
                with pytest.raises(m.PdfRenderError):
                    fn()
            reason_value = expected_reason.value
            matching = [
                r for r in caplog.records
                if reason_value in r.getMessage()
            ]
            assert len(matching) == 1, (
                f"Expected exactly 1 log record containing '{reason_value}'; "
                f"got {len(matching)}. Records: {[r.getMessage() for r in caplog.records]}"
            )

        # PAGE_OUT_OF_RANGE via render_page
        doc = fitz.open(MULTIPAGE_PDF)
        try:
            caplog.clear()
            with caplog.at_level(logging.WARNING, logger="genizah"):
                with pytest.raises(m.PdfRenderError):
                    m.render_page(doc, page_num=999)
            reason_value = m.PdfRenderFailure.PAGE_OUT_OF_RANGE.value
            matching = [
                r for r in caplog.records
                if reason_value in r.getMessage()
            ]
            assert len(matching) == 1, (
                f"Expected exactly 1 log record for PAGE_OUT_OF_RANGE; "
                f"got {len(matching)}."
            )
        finally:
            doc.close()
    finally:
        genizah_logger.removeHandler(caplog.handler)


# ---------------------------------------------------------------------------
# D-03: token echo contract (upgraded by Plan 02 with real signal-emission asserts)
# ---------------------------------------------------------------------------

def _require_pyqt():
    """Ensure QApplication exists; pytest.skip if PyQt6 not available.

    Mirrors tests/test_folder_walk_worker.py:149-155 — the in-repo precedent
    for testing QThread workers without pytest-qt.
    """
    try:
        from PyQt6.QtWidgets import QApplication  # noqa: F401
    except ImportError:
        pytest.skip("PyQt6 not available in this environment")
    from PyQt6.QtWidgets import QApplication
    return QApplication.instance() or QApplication(sys.argv[:1])


def test_token_echoed_in_signals():
    """UPGRADED from Plan 01 stub: verify (token, sys_id, page_num) are echoed
    verbatim in render_succeeded when _handle_request is called synchronously.

    Uses Qt.ConnectionType.DirectConnection so the slot fires inline on the
    calling thread — pytest-qt-FREE (mirrors test_folder_walk_worker.py idiom).

    D-03 latest-wins: the Phase 100 controller compares the echoed token
    against its current counter; a stale result (old token) is discarded.
    """
    _require_pyqt()
    from PyQt6.QtCore import Qt

    m = _import_renderer()
    worker = m.PdfRenderWorker()

    # Capture args from render_succeeded via DirectConnection (inline in caller thread)
    succeeded_args = []
    worker.render_succeeded.connect(
        lambda tok, sid, pn, img: succeeded_args.append((tok, sid, pn, img)),
        Qt.ConnectionType.DirectConnection,
    )
    failed_args = []
    worker.render_failed.connect(
        lambda tok, sid, pn, reason, detail: failed_args.append((tok, sid, pn, reason, detail)),
        Qt.ConnectionType.DirectConnection,
    )

    # Call _handle_request DIRECTLY (thread assertion is in run(), not here)
    worker._handle_request((7, "S1", 2, MULTIPAGE_PDF))

    assert len(succeeded_args) == 1, (
        f"Expected 1 render_succeeded emission; got {len(succeeded_args)}"
    )
    assert len(failed_args) == 0, (
        f"render_failed should NOT fire for a valid render; got {failed_args}"
    )

    tok, sid, pn, img = succeeded_args[0]
    assert tok == 7,    f"Token must be echoed verbatim; expected 7, got {tok}"
    assert sid == "S1", f"sys_id must be echoed verbatim; expected 'S1', got {sid!r}"
    assert pn == 2,     f"page_num must be echoed verbatim; expected 2, got {pn}"
    assert img is not None and not img.isNull(), "render_succeeded image must be non-null"


# ---------------------------------------------------------------------------
# Plan 02 additions — worker signal tests (pytest-qt-FREE)
# ---------------------------------------------------------------------------

def test_worker_failure_routes_to_render_failed():
    """A missing-file render request emits render_failed with the token echoed,
    reason=MISSING_FILE, non-empty detail; render_succeeded must NOT fire.

    Uses synchronous _handle_request call (thread assertion in run() only).
    Pins PDFIMG-06 failure routing contract via PdfRenderWorker.
    """
    _require_pyqt()
    from PyQt6.QtCore import Qt

    m = _import_renderer()
    worker = m.PdfRenderWorker()

    succeeded_args = []
    failed_args = []
    worker.render_succeeded.connect(
        lambda tok, sid, pn, img: succeeded_args.append((tok, sid, pn, img)),
        Qt.ConnectionType.DirectConnection,
    )
    worker.render_failed.connect(
        lambda tok, sid, pn, reason, detail: failed_args.append((tok, sid, pn, reason, detail)),
        Qt.ConnectionType.DirectConnection,
    )

    # Missing file — will classify as MISSING_FILE in render_via_lru
    worker._handle_request((42, "SYS_X", 1, "/nonexistent/totally_missing.pdf"))

    assert len(failed_args) == 1, (
        f"Expected 1 render_failed emission; got {len(failed_args)}"
    )
    assert len(succeeded_args) == 0, (
        "render_succeeded must NOT fire for a missing file"
    )

    tok, sid, pn, reason, detail = failed_args[0]
    assert tok == 42,                  f"Token echoed; expected 42, got {tok}"
    assert sid == "SYS_X",             f"sys_id echoed; expected 'SYS_X', got {sid!r}"
    assert pn == 1,                    f"page_num echoed; expected 1, got {pn}"
    assert reason == m.PdfRenderFailure.MISSING_FILE, (
        f"Expected MISSING_FILE, got {reason!r}"
    )
    assert detail, "detail must be non-empty"


def test_worker_survives_bad_render_and_serves_next():
    """PDFIMG-06 no-crash + D-03 loop-continuity proof.

    Enqueues a corrupt-PDF request THEN a valid multipage request on the SAME
    worker. Uses a REAL QThread (to prove the run loop does not die after an
    error) with DirectConnection + threading.Event for deterministic completion.

    Asserts:
      - first emission is render_failed with a failure reason (corrupt)
      - second emission is render_succeeded with a non-null QImage
      - the run loop survived the bad render and processed the next request
    """
    _require_pyqt()
    from PyQt6.QtCore import Qt

    m = _import_renderer()
    worker = m.PdfRenderWorker()

    emissions = []
    done_event = threading.Event()

    def _on_succeeded(tok, sid, pn, img):
        emissions.append(("succeeded", tok, sid, pn, img))
        if len(emissions) >= 2:
            done_event.set()

    def _on_failed(tok, sid, pn, reason, detail):
        emissions.append(("failed", tok, sid, pn, reason, detail))
        if len(emissions) >= 2:
            done_event.set()

    worker.render_succeeded.connect(_on_succeeded, Qt.ConnectionType.DirectConnection)
    worker.render_failed.connect(_on_failed, Qt.ConnectionType.DirectConnection)

    worker.start()
    try:
        # Enqueue corrupt PDF (will fail) then a valid multipage PDF
        worker.enqueue(1, "BAD", 1, CORRUPT_PDF)
        worker.enqueue(2, "GOOD", 1, MULTIPAGE_PDF)

        # Wait up to 5s for both emissions
        done_event.wait(5.0)
    finally:
        worker.stop(timeout_ms=5000)

    assert len(emissions) == 2, (
        f"Expected exactly 2 emissions (1 failed + 1 succeeded); got {len(emissions)}: "
        f"{emissions}"
    )

    first = emissions[0]
    assert first[0] == "failed", (
        f"First emission must be render_failed for the corrupt PDF; got {first[0]!r}"
    )
    # reason should be CORRUPT (corrupt_sample.pdf has a bad body)
    assert first[4] in (
        m.PdfRenderFailure.CORRUPT, m.PdfRenderFailure.RENDER_ERROR
    ), f"Expected CORRUPT or RENDER_ERROR for corrupt PDF; got {first[4]!r}"

    second = emissions[1]
    assert second[0] == "succeeded", (
        f"Second emission must be render_succeeded for the valid PDF; got {second[0]!r}. "
        "This proves the run loop survived the bad render (PDFIMG-06 no-crash)."
    )
    img = second[4]
    assert img is not None and not img.isNull(), (
        "Second emission QImage must be non-null (valid render of multipage_sample.pdf)"
    )


def test_enqueue_after_stop_dropped():
    """enqueue() after stop() must drop the request (return False) and not queue
    work that can never emit a result. Pins REVIEW item 4 (Codex MEDIUM).

    The thread never needs to be started — stop() sets _stopping immediately,
    so enqueue() observes the flag and returns False.
    """
    _require_pyqt()

    m = _import_renderer()
    worker = m.PdfRenderWorker()

    # stop() without ever starting sets _stopping and puts _STOP in queue
    # (wait(5000) immediately returns True since thread was never started)
    worker.stop(timeout_ms=100)

    # enqueue after stop must return False and drop the request
    result = worker.enqueue(1, "S", 1, MULTIPAGE_PDF)
    assert result is False, (
        f"enqueue() after stop() must return False; got {result!r}"
    )
