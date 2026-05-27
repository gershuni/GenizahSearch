"""Unit tests for desktop/pdf_image_controller.py — PdfImageController.

Tests cover:
  - Global-token monotonicity (one counter shared across all scopes)
  - Cross-surface independence (Browse + ResultDialog scopes never stranded)
  - cancel() before debounce fires (no enqueue, no stale display)
  - cancel() simulating Genizah navigation before a late render_succeeded
  - Terminal-state callback release (success / failure / watchdog timeout)
  - Same sys_id different page: per-scope latest-wins discards stale token
  - Watchdog TIMEOUT: fires placeholder + invalidates scope for late results
  - OLD watchdog does NOT time out a NEWER same-scope request (REVIEWS-R2-1)
  - discard_scope removes timer dict entries + is idempotent (REVIEWS-R2-3)
  - Latest success invokes on_image exactly once
  - Extension gate (.pdf accepted; .docx/.html/.xlsx/.csv/.txt rejected; uppercase .PDF accepted; None safe)
  - Per-reason localized placeholder map (en + he) + CANCELLED → None
  - Debounce coalesces rapid requests to one enqueue

Style: pytest-qt-FREE (uses QApplication.instance() or QApplication(sys.argv[:1])).
Mirrors tests/test_pdf_page_renderer.py _require_pyqt() helper pattern.
"""

import sys
import time
import pytest


# ---------------------------------------------------------------------------
# QApplication helper
# ---------------------------------------------------------------------------

def _require_pyqt():
    """Ensure a QApplication exists; skip test if PyQt6 is unavailable."""
    try:
        from PyQt6.QtWidgets import QApplication  # noqa: F401
    except ImportError:
        pytest.skip("PyQt6 not available in this environment")
    from PyQt6.QtWidgets import QApplication
    return QApplication.instance() or QApplication(sys.argv[:1])


# ---------------------------------------------------------------------------
# Fake worker (real QObject subclass so controller connects to real signals)
# ---------------------------------------------------------------------------

def _make_fake_worker():
    """Return a (QApplication, _FakeWorker) pair."""
    app = _require_pyqt()
    from PyQt6.QtCore import QObject, pyqtSignal

    class _FakeWorker(QObject):
        render_succeeded = pyqtSignal(int, str, int, object)
        render_failed = pyqtSignal(int, str, int, object, str)

        def __init__(self):
            super().__init__()
            self.enqueued: list = []

        def enqueue(self, token, sys_id, page_num, filepath):
            self.enqueued.append((token, sys_id, page_num, filepath))
            return True

    return app, _FakeWorker()


def _make_controller(worker, debounce_ms=0, watchdog_ms=5000):
    """Create a PdfImageController with the given worker and timing."""
    from desktop.pdf_image_controller import PdfImageController
    return PdfImageController(worker, debounce_ms=debounce_ms, watchdog_ms=watchdog_ms)


_FAKE_PDF = "/some/doc.pdf"
_SENTINEL = object()
_SENTINEL2 = object()


# ---------------------------------------------------------------------------
# Helper: process Qt events (for timer-based tests)
# ---------------------------------------------------------------------------

def _pump(app, ms=0):
    """Process pending Qt events and sleep `ms` milliseconds."""
    app.processEvents()
    if ms > 0:
        end = time.monotonic() + ms / 1000.0
        while time.monotonic() < end:
            app.processEvents()
            time.sleep(0.005)
        app.processEvents()


# ---------------------------------------------------------------------------
# Test: global-token monotonicity
# ---------------------------------------------------------------------------

def test_request_returns_global_monotonic_tokens():
    """Tokens from two different scopes come from ONE global counter and are strictly increasing."""
    app, worker = _make_fake_worker()
    ctrl = _make_controller(worker)
    placeholders = []

    t1 = ctrl.request("a", "X", 1, _FAKE_PDF, lambda img: None, placeholders.append)
    t2 = ctrl.request("b", "Y", 1, _FAKE_PDF, lambda img: None, placeholders.append)

    assert t1 is not None
    assert t2 is not None
    assert t2 > t1, f"Tokens must be strictly increasing; got t1={t1}, t2={t2}"


# ---------------------------------------------------------------------------
# Test: cross-surface independence (REVIEWS HIGH-1)
# ---------------------------------------------------------------------------

def test_cross_surface_independence():
    """Browse render in flight is NOT stranded when ResultDialog makes a request.

    Proves REVIEWS HIGH-1 is fixed: per-scope _awaiting_token means a
    ResultDialog request cannot supersede Browse's awaited token.
    """
    app, worker = _make_fake_worker()
    ctrl = _make_controller(worker, debounce_ms=0)

    browse_images = []
    browse_ph = []
    dialog_images = []
    dialog_ph = []

    dialog_scope = "dialog-test"

    # Request from Browse (token 1)
    t1 = ctrl.request("browse", "B", 1, _FAKE_PDF, browse_images.append, browse_ph.append)
    # Request from ResultDialog (token 2) — must NOT strangle Browse's awaited token
    t2 = ctrl.request(dialog_scope, "D", 1, _FAKE_PDF, dialog_images.append, dialog_ph.append)

    # Per-scope state: browse still awaits t1; dialog awaits t2
    assert ctrl._awaiting_token.get("browse") == t1, "Browse's awaited token should still be t1"
    assert ctrl._awaiting_token.get(dialog_scope) == t2, "Dialog's awaited token should be t2"

    # Pump debounce (debounce_ms=0 so fire immediately)
    ctrl._fire_pending("browse")
    ctrl._fire_pending(dialog_scope)

    # Emit render_succeeded for Browse's token — should land in browse_images, NOT dialog_images
    worker.render_succeeded.emit(t1, "B", 1, _SENTINEL)
    _pump(app)
    assert browse_images == [_SENTINEL], "Browse's on_image must receive its result"
    assert dialog_images == [], "Dialog's on_image must NOT be called for Browse's result"

    # Emit render_succeeded for dialog's token — should land in dialog_images only
    worker.render_succeeded.emit(t2, "D", 1, _SENTINEL2)
    _pump(app)
    assert dialog_images == [_SENTINEL2], "Dialog's on_image must receive its result"
    assert len(browse_images) == 1, "Browse's on_image must not be called again"


# ---------------------------------------------------------------------------
# Test: cancel() before debounce fires (REVIEWS HIGH-2)
# ---------------------------------------------------------------------------

def test_cancel_before_debounce_no_enqueue_no_callback():
    """cancel() before debounce fires: no enqueue, no stale display."""
    app, worker = _make_fake_worker()
    # Use non-zero debounce so we can cancel before it fires
    ctrl = _make_controller(worker, debounce_ms=200)

    images = []
    ph = []
    t = ctrl.request("s", "X", 1, _FAKE_PDF, images.append, ph.append)

    # Cancel before the 200ms debounce fires
    ctrl.cancel("s", silent=True)

    # Pump events past the debounce interval
    _pump(app, ms=250)

    assert worker.enqueued == [], "Worker must NOT be enqueued after cancel before debounce"
    assert ctrl._awaiting_token.get("s") is None, "Awaiting token must be cleared after cancel"

    # A late render_succeeded for the cancelled token must be discarded
    worker.render_succeeded.emit(t, "X", 1, _SENTINEL)
    _pump(app)
    assert images == [], "on_image must NOT be called after cancel"


# ---------------------------------------------------------------------------
# Test: Genizah navigation before late render_succeeded (no stale display)
# ---------------------------------------------------------------------------

def test_genizah_nav_before_success_no_stale_display():
    """cancel() simulating nav to a Genizah result: late render_succeeded is discarded."""
    app, worker = _make_fake_worker()
    ctrl = _make_controller(worker, debounce_ms=0)

    images = []
    t = ctrl.request("s", "X", 1, _FAKE_PDF, images.append, lambda s: None)
    ctrl._fire_pending("s")  # enqueue

    # Simulate navigating to a Genizah (non-PDF) result
    ctrl.cancel("s", silent=True)

    # Late render_succeeded arrives after cancel
    worker.render_succeeded.emit(t, "X", 1, _SENTINEL)
    _pump(app)
    assert images == [], "on_image must NOT be called after cancel (stale result discarded)"


# ---------------------------------------------------------------------------
# Test: terminal-state callback release (REVIEWS MEDIUM-4)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("terminal", ["success", "failure", "watchdog"])
def test_terminal_states_release_callbacks(terminal):
    """After any terminal event, _pending and _awaiting_token are None for scope."""
    app, worker = _make_fake_worker()
    ctrl = _make_controller(worker, debounce_ms=0, watchdog_ms=5000)

    ph_calls = []
    t = ctrl.request("s", "X", 1, _FAKE_PDF, lambda img: None, ph_calls.append)
    ctrl._fire_pending("s")

    if terminal == "success":
        worker.render_succeeded.emit(t, "X", 1, _SENTINEL)
        _pump(app)
    elif terminal == "failure":
        from desktop.pdf_page_renderer import PdfRenderFailure
        worker.render_failed.emit(t, "X", 1, PdfRenderFailure.RENDER_ERROR, "test")
        _pump(app)
    elif terminal == "watchdog":
        # Force watchdog timeout by calling _on_watchdog directly
        ctrl._on_watchdog("s")
        _pump(app)

    assert ctrl._pending.get("s") is None, f"_pending must be None after {terminal} terminal state"
    assert ctrl._awaiting_token.get("s") is None, f"_awaiting_token must be None after {terminal}"


# ---------------------------------------------------------------------------
# Test: same sys_id different page (per-scope latest-wins)
# ---------------------------------------------------------------------------

def test_same_sysid_different_page_discards_stale():
    """Second request for same sys_id different page supersedes the first."""
    app, worker = _make_fake_worker()
    ctrl = _make_controller(worker, debounce_ms=0)

    images = []
    ph = []
    t1 = ctrl.request("s", "X", 1, _FAKE_PDF, images.append, ph.append)
    t2 = ctrl.request("s", "X", 2, _FAKE_PDF, images.append, ph.append)

    assert ctrl._awaiting_token.get("s") == t2, "Second request must become the awaited token"

    ctrl._fire_pending("s")

    # Emit stale page-1 result — must be discarded
    worker.render_succeeded.emit(t1, "X", 1, _SENTINEL)
    _pump(app)
    assert images == [], "Stale page-1 result must be discarded"

    # Emit current page-2 result — must be displayed
    worker.render_succeeded.emit(t2, "X", 2, _SENTINEL2)
    _pump(app)
    assert images == [_SENTINEL2], "Current page-2 result must be displayed"


# ---------------------------------------------------------------------------
# Test: latest success invokes on_image exactly once
# ---------------------------------------------------------------------------

def test_latest_success_displays():
    """render_succeeded echoed with the scope's latest token invokes on_image once."""
    app, worker = _make_fake_worker()
    ctrl = _make_controller(worker, debounce_ms=0)

    images = []
    t = ctrl.request("s", "X", 1, _FAKE_PDF, images.append, lambda s: None)
    ctrl._fire_pending("s")

    worker.render_succeeded.emit(t, "X", 1, _SENTINEL)
    _pump(app)
    assert images == [_SENTINEL], "on_image should be called exactly once with the sentinel"


# ---------------------------------------------------------------------------
# Test: extension gate
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("ext", [".docx", ".html", ".xlsx", ".csv", ".txt"])
def test_non_pdf_extensions_gated_out(ext):
    """Non-PDF extensions must return None and never call worker.enqueue."""
    app, worker = _make_fake_worker()
    ctrl = _make_controller(worker)

    result = ctrl.request("s", "X", 1, f"/a/b{ext}", lambda img: None, lambda s: None)
    assert result is None, f"Extension '{ext}' must be gated out, got token {result}"
    assert worker.enqueued == [], f"worker.enqueue must not be called for '{ext}'"


def test_uppercase_pdf_accepted():
    """.PDF uppercase must be accepted by is_pdf()."""
    _require_pyqt()
    from desktop.pdf_image_controller import PdfImageController
    from unittest.mock import MagicMock
    worker = MagicMock()
    worker.render_succeeded = MagicMock()
    worker.render_succeeded.connect = MagicMock()
    worker.render_failed = MagicMock()
    worker.render_failed.connect = MagicMock()
    ctrl = PdfImageController(worker, debounce_ms=0)
    assert ctrl.is_pdf("/some/FILE.PDF") is True, ".PDF uppercase must be accepted"


def test_none_filepath_returns_none():
    """filepath=None must return None without raising."""
    app, worker = _make_fake_worker()
    ctrl = _make_controller(worker)
    result = ctrl.request("s", "X", 1, None, lambda img: None, lambda s: None)
    assert result is None, "None filepath must return None"
    assert worker.enqueued == []


# ---------------------------------------------------------------------------
# Test: per-reason localized placeholder map
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lang,expected", [
    ("en", "File not found"),
    ("he", "הקובץ לא נמצא"),
])
def test_placeholder_missing_file(lang, expected, monkeypatch):
    """MISSING_FILE maps to the correct localized string."""
    _require_pyqt()
    import genizah_core
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", lang)
    from desktop.pdf_image_controller import PdfImageController
    from desktop.pdf_page_renderer import PdfRenderFailure
    from unittest.mock import MagicMock

    worker = MagicMock()
    worker.render_succeeded.connect = MagicMock()
    worker.render_failed.connect = MagicMock()
    ctrl = PdfImageController(worker)
    result = ctrl._placeholder_for(PdfRenderFailure.MISSING_FILE)
    assert result == expected, f"Expected {expected!r}, got {result!r}"


@pytest.mark.parametrize("reason,lang,expected_partial", [
    ("ENCRYPTED", "en", "password"),
    ("ENCRYPTED", "he", "סיסמה"),
    ("CORRUPT", "en", "open"),
    ("CORRUPT", "he", "לפתוח"),
    ("PAGE_OUT_OF_RANGE", "en", "Page not found"),
    ("PAGE_OUT_OF_RANGE", "he", "עמוד"),
    ("RENDER_ERROR", "en", "display"),
    ("RENDER_ERROR", "he", "להציג"),
    ("TIMEOUT", "en", "timed out"),
    ("TIMEOUT", "he", "זמן"),
])
def test_placeholder_per_reason_localized(reason, lang, expected_partial, monkeypatch):
    """Each PdfRenderFailure reason maps to a non-empty localized placeholder."""
    _require_pyqt()
    import genizah_core
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", lang)
    from desktop.pdf_image_controller import PdfImageController
    from desktop.pdf_page_renderer import PdfRenderFailure
    from unittest.mock import MagicMock

    worker = MagicMock()
    worker.render_succeeded.connect = MagicMock()
    worker.render_failed.connect = MagicMock()
    ctrl = PdfImageController(worker)
    result = ctrl._placeholder_for(getattr(PdfRenderFailure, reason))
    assert result is not None, f"Expected placeholder for {reason}/{lang}, got None"
    assert expected_partial.lower() in result.lower(), (
        f"Expected {expected_partial!r} in {result!r} for {reason}/{lang}"
    )


def test_placeholder_cancelled_returns_none(monkeypatch):
    """PdfRenderFailure.CANCELLED must return None (silent discard, no placeholder)."""
    _require_pyqt()
    import genizah_core
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")
    from desktop.pdf_image_controller import PdfImageController
    from desktop.pdf_page_renderer import PdfRenderFailure
    from unittest.mock import MagicMock

    worker = MagicMock()
    worker.render_succeeded.connect = MagicMock()
    worker.render_failed.connect = MagicMock()
    ctrl = PdfImageController(worker)
    result = ctrl._placeholder_for(PdfRenderFailure.CANCELLED)
    assert result is None, f"CANCELLED must return None, got {result!r}"


# ---------------------------------------------------------------------------
# Test: debounce coalesces rapid requests
# ---------------------------------------------------------------------------

def test_debounce_coalesces():
    """Multiple rapid requests within debounce window coalesce to ONE enqueue."""
    app, worker = _make_fake_worker()
    debounce = 40  # ms — measurable but not too slow
    ctrl = _make_controller(worker, debounce_ms=debounce)

    ph = []
    t1 = ctrl.request("s", "X", 1, _FAKE_PDF, lambda img: None, ph.append)  # noqa: F841
    t2 = ctrl.request("s", "X", 2, _FAKE_PDF, lambda img: None, ph.append)  # noqa: F841
    t3 = ctrl.request("s", "X", 3, _FAKE_PDF, lambda img: None, ph.append)

    # Wait for the debounce timer to fire
    _pump(app, ms=debounce + 20)

    assert len(worker.enqueued) == 1, (
        f"Debounce must coalesce 3 requests into 1 enqueue; got {len(worker.enqueued)}: {worker.enqueued}"
    )
    # The enqueued item must carry the LAST token
    assert worker.enqueued[0][0] == t3, (
        f"Enqueued token must be the last token t3={t3}; got {worker.enqueued[0][0]}"
    )


# ---------------------------------------------------------------------------
# Test: watchdog fires TIMEOUT placeholder + invalidates scope for late results
# ---------------------------------------------------------------------------

def test_watchdog_fires_timeout_placeholder():
    """Watchdog fires TIMEOUT placeholder; subsequent late render_succeeded is discarded."""
    app, worker = _make_fake_worker()
    ctrl = _make_controller(worker, debounce_ms=0, watchdog_ms=30)  # 30ms watchdog

    ph = []
    images = []
    t = ctrl.request("s", "X", 1, _FAKE_PDF, images.append, ph.append)
    ctrl._fire_pending("s")  # enqueues + arms watchdog

    # Wait for watchdog to fire
    _pump(app, ms=60)

    # Watchdog should have fired TIMEOUT
    assert any("timed out" in p.lower() or "זמן" in p for p in ph), (
        f"TIMEOUT placeholder expected; got placeholders: {ph}"
    )
    assert ctrl._awaiting_token.get("s") is None, "_awaiting_token must be cleared after watchdog"

    # Late render_succeeded for the timed-out token must be discarded
    ph_before = list(ph)
    worker.render_succeeded.emit(t, "X", 1, _SENTINEL)
    _pump(app)
    assert images == [], "on_image must NOT be called after watchdog timeout"
    assert ph == ph_before, "No additional placeholder after watchdog timeout"


# ---------------------------------------------------------------------------
# Test: OLD watchdog does NOT time out a NEWER same-scope request (REVIEWS-R2-1)
# ---------------------------------------------------------------------------

def test_old_watchdog_does_not_timeout_newer_request():
    """An OLD watchdog armed for token1 must NOT fire when token2 has replaced _awaiting_token.

    Proves REVIEWS-R2-1 fix: _watchdog_token guard causes _on_watchdog to
    no-op when the armed token no longer matches the scope's awaited token.
    """
    app, worker = _make_fake_worker()
    ctrl = _make_controller(worker, debounce_ms=0, watchdog_ms=5000)

    ph_calls = []
    images = []

    # First request — token1 enqueued + watchdog armed
    t1 = ctrl.request("s", "X", 1, _FAKE_PDF, images.append, ph_calls.append)
    ctrl._fire_pending("s")  # enqueues + sets _watchdog_token["s"] = t1
    assert ctrl._watchdog_token.get("s") == t1, "_watchdog_token must be set to t1 after _fire_pending"

    # Second request — token2 supersedes token1; request() stops the prior watchdog
    t2 = ctrl.request("s", "X", 2, _FAKE_PDF, images.append, ph_calls.append)
    assert ctrl._awaiting_token.get("s") == t2, "_awaiting_token must be t2 after second request"

    # DIRECTLY invoke _on_watchdog simulating the OLD (token1) watchdog firing
    ph_before = list(ph_calls)
    ctrl._on_watchdog("s")

    # Must be a NO-OP: no TIMEOUT placeholder, _awaiting_token still == t2
    timeout_text = [p for p in ph_calls if "timed out" in p.lower() or "זמן" in p]
    assert len(timeout_text) == len([p for p in ph_before if "timed out" in p.lower() or "זמן" in p]), (
        "Old watchdog must NOT emit a TIMEOUT placeholder (REVIEWS-R2-1)"
    )
    assert ctrl._awaiting_token.get("s") == t2, (
        "Old watchdog must NOT clear the newer token (REVIEWS-R2-1)"
    )

    # The new request (token2) must still succeed: fire pending + emit render_succeeded
    ctrl._fire_pending("s")
    worker.render_succeeded.emit(t2, "X", 2, _SENTINEL2)
    _pump(app)
    assert _SENTINEL2 in images, "New request (token2) must still invoke on_image after old watchdog no-op"


# ---------------------------------------------------------------------------
# Test: discard_scope removes timer dict entries (REVIEWS-R2-3)
# ---------------------------------------------------------------------------

def test_discard_scope_removes_timer_entries():
    """discard_scope removes _debounce_timers and _watchdog_timers entries for the scope."""
    app, worker = _make_fake_worker()
    ctrl = _make_controller(worker, debounce_ms=0, watchdog_ms=5000)

    # request() lazily creates the debounce timer for scope "d"
    ctrl.request("d", "X", 1, _FAKE_PDF, lambda img: None, lambda s: None)
    assert "d" in ctrl._debounce_timers, "Debounce timer must be created after request()"

    # _fire_pending creates the watchdog timer
    ctrl._fire_pending("d")
    assert "d" in ctrl._watchdog_timers, "Watchdog timer must be created after _fire_pending()"

    # discard_scope must remove both timer entries
    ctrl.discard_scope("d")

    assert "d" not in ctrl._debounce_timers, "discard_scope must remove debounce timer entry"
    assert "d" not in ctrl._watchdog_timers, "discard_scope must remove watchdog timer entry"
    assert "d" not in ctrl._awaiting_token, "discard_scope must clear _awaiting_token"

    # Idempotent: second call must not raise
    ctrl.discard_scope("d")  # should not raise


# ---------------------------------------------------------------------------
# Test: failure reason maps to localized string + on_placeholder is called
# ---------------------------------------------------------------------------

def test_failure_maps_to_localized_placeholder_via_signal(monkeypatch):
    """render_failed signal triggers the correct localized placeholder via on_placeholder."""
    app, worker = _make_fake_worker()
    import genizah_core
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")

    ctrl = _make_controller(worker, debounce_ms=0)
    from desktop.pdf_page_renderer import PdfRenderFailure

    ph = []
    t = ctrl.request("s", "X", 1, _FAKE_PDF, lambda img: None, ph.append)
    ctrl._fire_pending("s")

    # Emit render_failed with MISSING_FILE
    worker.render_failed.emit(t, "X", 1, PdfRenderFailure.MISSING_FILE, "not found")
    _pump(app)

    assert any("not found" in p.lower() for p in ph), (
        f"Expected 'not found' placeholder; got {ph}"
    )
