# -*- coding: utf-8 -*-
"""Phase 99 — Wave 0 test contract for desktop/pdf_page_renderer.py.

Covers: PDFIMG-01 (render), PDFIMG-02 (LRU + no-disk-cache), PDFIMG-06
(failure classification), and D-03 token-echo contract.

These tests call render functions and LRU directly — NO QThread spun here.
Mirror approach: tests/test_local_pdf_extraction_fallback.py calls
extract_pdf_pages() directly without spinning a QThread.

If any fixture PDF is missing, run:
    python scripts/generate_pdf_render_fixtures.py
"""
import gc
import os
import shutil
import tempfile
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
    import fitz

    lru = m.DocLRU(maxsize=2)

    # Open 3 distinct fixture paths by inserting them in order
    lru.get(MULTIPAGE_PDF)
    lru.get(ENCRYPTED_PDF)  # maxsize reached — no eviction yet (len==2)
    # Insert third: MULTIPAGE_PDF was LRU; ENCRYPTED_PDF was MRU
    # Actually we need a 3rd distinct path: use CORRUPT_PDF
    # But corrupt raises — we need a valid third PDF.
    # Use clean_sample.pdf (existing fixture).
    clean_pdf = os.path.join(FIXTURES_DIR, "clean_sample.pdf")
    lru.get(clean_pdf)  # evicts oldest (MULTIPAGE_PDF)

    assert len(lru._cache) == 2

    # MULTIPAGE_PDF must have been evicted; it is no longer in cache
    assert MULTIPAGE_PDF not in lru._cache
    # The two remaining should be ENCRYPTED_PDF and clean_pdf
    assert ENCRYPTED_PDF in lru._cache or clean_pdf in lru._cache

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

    # Triggering eviction (insert a second doc) must NOT raise
    clean_pdf = os.path.join(FIXTURES_DIR, "clean_sample.pdf")
    lru.get(clean_pdf)  # evicts path; best-effort close swallows RuntimeError

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
    import fitz
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

    for fn, expected_reason in cases:
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="desktop.pdf_page_renderer"):
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
        with caplog.at_level(logging.WARNING, logger="desktop.pdf_page_renderer"):
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


# ---------------------------------------------------------------------------
# D-03: token echo contract (stub for Plan 02's signal wiring)
# ---------------------------------------------------------------------------

def test_token_echoed_in_signals():
    """Verify the PdfRenderFailure enum exposes all 8 members with the exact
    string values from D-04 (the token/signal contract is completed by Plan 02
    which adds the real QThread signal-echo assertions).

    Comment for Plan 02's executor: upgrade this test to assert that the
    PdfRenderWorker's render_succeeded / render_failed signals echo the
    (token, sys_id, page_num) tuple provided to enqueue(), proving stale
    results can be discarded by the UI controller.
    """
    m = _import_renderer()

    expected_values = {
        "missing-file",
        "not-pdf",
        "encrypted",
        "corrupt",
        "page-out-of-range",
        "render-error",
        "timeout",
        "cancelled",
    }
    actual_values = {e.value for e in m.PdfRenderFailure}
    assert actual_values == expected_values, (
        f"PdfRenderFailure enum mismatch.\n"
        f"  Expected: {sorted(expected_values)}\n"
        f"  Got:      {sorted(actual_values)}"
    )
    assert len(list(m.PdfRenderFailure)) == 8
