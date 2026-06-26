"""DESK-08: behavioral tests for GenizahGUI sidecar coordination methods.

Tests the three sidecar reset/download coordination methods that live IN PLACE
on GenizahGUI (not extracted — RESEARCH Directive #2 crux verdict). They are
tested via GenizahGUI.__new__ (skipping __init__ so no QApplication / Tantivy
index is required — same pattern as test_telemetry_consent_ux.py).

Methods under test:
  _reset_sidecar_connections()          — lazy service resets + catalog filter
  _download_next_sidecar()              — queue pop + thread launch (or reset)
  _on_sidecar_download_finished(...)    — advances queue

SEED-020 §7 C-6: these methods had no direct test before Phase 127.
"""
from unittest.mock import MagicMock, patch


def _make_gui_coordinator():
    """Build a minimal GenizahGUI duck with only sidecar-coordinator state.

    Uses __new__ to skip __init__ (no QApplication, no Tantivy index).
    Stubs exactly the self.* attributes accessed by the three tested methods.
    """
    import genizah_app
    gui = genizah_app.GenizahGUI.__new__(genizah_app.GenizahGUI)
    gui._sidecar_download_queue = []
    gui._sidecar_data_dir = "/fake/data"
    gui._current_sidecar_download = None
    return gui


# ---------------------------------------------------------------------------
# _reset_sidecar_connections
# ---------------------------------------------------------------------------

def test_reset_sidecar_connections_calls_all_three_services():
    """_reset_sidecar_connections must reset all three sidecar services."""
    gui = _make_gui_coordinator()
    with patch("shared.document_service.reset_pgp_service") as mock_pgp, \
         patch("shared.fjms_service.reset_fjms_service") as mock_fjms, \
         patch("shared.nli_crossref_service.reset_nli_crossref_service") as mock_nli, \
         patch("genizah_app.reset_catalog_filter_sets") as mock_cat:
        gui._reset_sidecar_connections()

    mock_pgp.assert_called_once()
    mock_fjms.assert_called_once()
    mock_nli.assert_called_once()
    mock_cat.assert_called_once()


def test_reset_sidecar_connections_calls_catalog_filter_sets():
    """_reset_sidecar_connections must call reset_catalog_filter_sets for cache invalidation."""
    gui = _make_gui_coordinator()
    with patch("shared.document_service.reset_pgp_service"), \
         patch("shared.fjms_service.reset_fjms_service"), \
         patch("shared.nli_crossref_service.reset_nli_crossref_service"), \
         patch("genizah_app.reset_catalog_filter_sets") as mock_cat:
        gui._reset_sidecar_connections()

    mock_cat.assert_called_once_with()


# ---------------------------------------------------------------------------
# _download_next_sidecar — non-empty queue branch
# ---------------------------------------------------------------------------

def test_download_next_sidecar_pops_queue_and_starts_thread():
    """_download_next_sidecar must pop the first item and start a SidecarDownloadThread."""
    gui = _make_gui_coordinator()
    gui._sidecar_download_queue = [
        {"url": "https://example.com/db.zip", "subdir": "data", "name": "test.db"},
    ]

    mock_thread = MagicMock()
    with patch("genizah_app.SidecarDownloadThread", return_value=mock_thread) as mock_cls:
        gui._download_next_sidecar()

    # Queue must now be empty (item was popped)
    assert gui._sidecar_download_queue == [], (
        "_download_next_sidecar must pop the queue item"
    )
    # Thread must have been created and started
    mock_cls.assert_called_once()
    mock_thread.finished_signal.connect.assert_called_once_with(gui._on_sidecar_download_finished)
    mock_thread.start.assert_called_once()
    # The new thread must be stored on the coordinator
    assert gui._current_sidecar_download is mock_thread


def test_download_next_sidecar_advances_through_multiple_items():
    """_download_next_sidecar processes items one at a time (FIFO order)."""
    gui = _make_gui_coordinator()
    item_a = {"url": "https://a.com/a.db", "subdir": "data", "name": "a.db"}
    item_b = {"url": "https://b.com/b.db", "subdir": "data", "name": "b.db"}
    gui._sidecar_download_queue = [item_a, item_b]

    mock_thread = MagicMock()
    with patch("genizah_app.SidecarDownloadThread", return_value=mock_thread):
        gui._download_next_sidecar()

    # Only the first item should have been popped; the second remains
    assert len(gui._sidecar_download_queue) == 1
    assert gui._sidecar_download_queue[0] is item_b


# ---------------------------------------------------------------------------
# _download_next_sidecar — empty queue branch
# ---------------------------------------------------------------------------

def test_download_next_sidecar_empty_queue_calls_reset_and_returns():
    """When the queue is empty, _download_next_sidecar must call _reset_sidecar_connections."""
    gui = _make_gui_coordinator()
    gui._sidecar_download_queue = []  # already empty

    # Patch QMessageBox.information so the bare __new__ object is not used as a real QWidget.
    with patch("genizah_app.QMessageBox") as mock_qmb, \
         patch.object(gui, "_reset_sidecar_connections") as mock_reset:
        mock_qmb.information = MagicMock()
        gui._download_next_sidecar()

    mock_reset.assert_called_once()


# ---------------------------------------------------------------------------
# _on_sidecar_download_finished
# ---------------------------------------------------------------------------

def test_on_sidecar_download_finished_success_advances_queue():
    """_on_sidecar_download_finished must call _download_next_sidecar on success."""
    gui = _make_gui_coordinator()
    with patch.object(gui, "_download_next_sidecar") as mock_next:
        gui._on_sidecar_download_finished(True, "/fake/data/data/test.db", "test.db")

    mock_next.assert_called_once()


def test_on_sidecar_download_finished_failure_still_advances_queue():
    """_on_sidecar_download_finished must advance the queue even on failure."""
    gui = _make_gui_coordinator()
    with patch.object(gui, "_download_next_sidecar") as mock_next:
        gui._on_sidecar_download_finished(False, "connection error", "test.db")

    mock_next.assert_called_once()
