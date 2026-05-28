# -*- coding: utf-8 -*-
"""Phase 97 C-06 — Disk-headroom indicator tests (Wave D).

T-D-1: warning fires when (free - 2 × index_size) < 1 GB.
T-D-1: no warning when there is sufficient headroom.

Tests are RED until Phase 97 Wave D GREEN implementation lands in
desktop/my_library_tab.py (_update_disk_indicator + _disk_label).
"""
from __future__ import annotations

import os
import sys
import unittest.mock as mock

import pytest


# Headless Qt: when no display server is available (e.g. CI on Linux without
# Xvfb), Qt aborts with SIGABRT on QApplication() construction. Force the
# offscreen platform plugin before importing QApplication. Mirrors the pattern
# in tests/test_line_numbers_desktop.py.
if sys.platform.startswith("linux") and not os.environ.get("DISPLAY"):
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

try:
    from PyQt6.QtWidgets import QApplication, QWidget
    _app = QApplication.instance() or QApplication(sys.argv)
    QT_AVAILABLE = True
except ImportError:
    QT_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not QT_AVAILABLE, reason="PyQt6 not available"
)


# ---------------------------------------------------------------------------
# Helpers — reuse ceiling-test fixture pattern (test_local_ceiling_enforcement)
# ---------------------------------------------------------------------------

def _make_mock_indexer():
    m = mock.MagicMock()
    m.list_folders.return_value = []
    m.prescan_count.return_value = (0, 0)
    m.prescan_count_all.return_value = (0, 0)
    m.scan_all.return_value = {
        "indexed": 0, "skipped": 0, "errors": 0, "cancelled": False
    }
    m.startup_recovery.return_value = {
        "pending_deletes_recovered": 0,
        "pending_inserts_recovered": 0,
    }
    m.start_recovery_probe.return_value = []  # Phase 97 R-01 — no interrupted runs
    m.estimate_index_size.return_value = 0     # Phase 97 C-06 — no index yet
    return m


@pytest.fixture(autouse=True)
def _ensure_app():
    if QT_AVAILABLE:
        if QApplication.instance() is None:
            QApplication(sys.argv)
    yield


def _make_tab(mock_idx):
    """Instantiate MyLibraryTab with a mocked indexer (headless Qt)."""
    from desktop.my_library_tab import MyLibraryTab

    parent = QWidget()
    parent.searcher = mock.MagicMock()
    parent.lab_engine = mock.MagicMock()
    parent.statusBar = mock.MagicMock(return_value=mock.MagicMock())

    with (
        mock.patch("desktop.my_library_tab.LocalIndexer") as MockIdx,
        mock.patch("desktop.my_library_tab.os.makedirs"),
    ):
        MockIdx.return_value = mock_idx
        tab = MyLibraryTab(parent)
    return tab


# ---------------------------------------------------------------------------
# T-D-1a: warning fires when headroom < 1 GB
# ---------------------------------------------------------------------------

def test_warning_below_threshold():
    """C-06: low-headroom warning fires when free - 2×index < 1 GB.

    Scenario: free=512 MB, index_size=1 GB → headroom = 512 MB - 2 GB = -1.5 GB < 1 GB.
    Expected: _disk_label text contains the warning string.
    """
    mock_idx = _make_mock_indexer()
    # estimate_index_size returns 1 GB
    mock_idx.estimate_index_size.return_value = 1 * 1024 ** 3

    tab = _make_tab(mock_idx)

    # Mock shutil.disk_usage to return (total=10GB, used=9.5GB, free=512MB)
    _512_mb = 512 * 1024 * 1024
    _10_gb = 10 * 1024 ** 3
    fake_usage = mock.MagicMock()
    fake_usage.free = _512_mb
    fake_usage.total = _10_gb
    fake_usage.used = _10_gb - _512_mb

    with mock.patch("shutil.disk_usage", return_value=fake_usage):
        tab._update_disk_indicator()

    # Locale-agnostic: the warning is rendered via tr("⚠ low merge headroom"),
    # which resolves to Hebrew when CURRENT_LANG=he. Compare against the
    # translated string rather than the English literal.
    from genizah_core import tr
    warning = tr("⚠ low merge headroom")
    label_text = tab._disk_label.text()
    assert warning in label_text, (
        f"Expected low-headroom warning {warning!r} in label text; got: {label_text!r}"
    )


# ---------------------------------------------------------------------------
# T-D-1b: no warning when there is sufficient headroom
# ---------------------------------------------------------------------------

def test_no_warning_above_threshold():
    """C-06: no warning when free - 2×index >= 1 GB.

    Scenario: free=10 GB, index_size=1 GB → headroom = 10 GB - 2 GB = 8 GB >= 1 GB.
    Expected: _disk_label text does NOT contain the warning string.
    """
    mock_idx = _make_mock_indexer()
    # estimate_index_size returns 1 GB
    mock_idx.estimate_index_size.return_value = 1 * 1024 ** 3

    tab = _make_tab(mock_idx)

    # Mock shutil.disk_usage to return free=10 GB
    _10_gb = 10 * 1024 ** 3
    fake_usage = mock.MagicMock()
    fake_usage.free = _10_gb
    fake_usage.total = _10_gb * 10
    fake_usage.used = _10_gb * 9

    with mock.patch("shutil.disk_usage", return_value=fake_usage):
        tab._update_disk_indicator()

    from genizah_core import tr
    warning = tr("⚠ low merge headroom")
    label_text = tab._disk_label.text()
    assert warning not in label_text, (
        f"Expected NO low-headroom warning when headroom >= 1 GB; got: {label_text!r}"
    )
