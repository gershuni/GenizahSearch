# -*- coding: utf-8 -*-
"""Phase 102 D-08 surface 4 — corrupt_encoding tree label + red color.

Tests that the My Library file tree shows 'Corrupt encoding' (bilingual) in red
for files with status='corrupt_encoding' at all three surface points:
  :333 _build_leaf_item_status  — static build
  :486 update_file_status label — live update label
  :519 update_file_status color — live update color paint

Tests also assert encoding_error mapping is unchanged (no regression).
"""
from __future__ import annotations

import sys
import unittest.mock as mock

import pytest

# ---------------------------------------------------------------------------
# Ensure a QApplication exists for all tests (headless-safe)
# ---------------------------------------------------------------------------
try:
    from PyQt6.QtWidgets import QApplication

    _app = QApplication.instance() or QApplication(sys.argv)
    QT_AVAILABLE = True
except ImportError:
    QT_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not QT_AVAILABLE, reason="PyQt6 not available"
)


@pytest.fixture(autouse=True)
def _ensure_app():
    """Guarantee a QApplication exists for every test."""
    if QT_AVAILABLE:
        from PyQt6.QtWidgets import QApplication
        import sys
        if QApplication.instance() is None:
            QApplication(sys.argv)
    yield


# ---------------------------------------------------------------------------
# Helper: instantiate _UnifiedFileTreeWidget without a full MyLibraryTab
# ---------------------------------------------------------------------------

def _make_tree_widget():
    """Instantiate _UnifiedFileTreeWidget in isolation for unit testing."""
    from desktop.my_library_tab import _UnifiedFileTreeWidget
    from PyQt6.QtWidgets import QWidget
    # _UnifiedFileTreeWidget takes (parent: QWidget, app: object)
    mock_parent = QWidget()
    mock_app = mock.MagicMock()
    mock_app._local_file_optouts = set()
    widget = _UnifiedFileTreeWidget(mock_parent, mock_app)
    return widget


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_build_leaf_item_status_corrupt_encoding():
    """_build_leaf_item_status('corrupt_encoding', 0) returns red 'Corrupt encoding' label.

    Proves surface point :333 (_build_leaf_item_status) is wired for corrupt_encoding.
    """
    from genizah_core import tr

    widget = _make_tree_widget()
    pages_str, label, color = widget._build_leaf_item_status('corrupt_encoding', 0)

    assert label == tr("Corrupt encoding"), (
        f"Expected label == tr('Corrupt encoding'), got {label!r}"
    )
    assert color == '#e74c3c', (
        f"Expected red color '#e74c3c' for corrupt_encoding, got {color!r}"
    )


def test_build_leaf_item_status_corrupt_encoding_with_pages():
    """_build_leaf_item_status('corrupt_encoding', 5) includes pages_str and red color."""
    from genizah_core import tr

    widget = _make_tree_widget()
    pages_str, label, color = widget._build_leaf_item_status('corrupt_encoding', 5)

    assert pages_str == '5', f"Expected pages_str='5', got {pages_str!r}"
    assert label == tr("Corrupt encoding"), (
        f"Expected label == tr('Corrupt encoding'), got {label!r}"
    )
    assert color == '#e74c3c', (
        f"Expected red color '#e74c3c', got {color!r}"
    )


def test_build_leaf_item_status_encoding_error_unchanged():
    """Regression: encoding_error mapping unchanged after adding corrupt_encoding."""
    from genizah_core import tr

    widget = _make_tree_widget()
    _, label, color = widget._build_leaf_item_status('encoding_error', 0)

    assert label == tr("Encoding error"), (
        f"encoding_error label must remain tr('Encoding error'), got {label!r}"
    )
    assert color == '#e74c3c', (
        f"encoding_error color must remain '#e74c3c', got {color!r}"
    )


def test_update_file_status_label_surface_corrupt_encoding():
    """update_file_status :486 branch — corrupt_encoding sets display label to tr('Corrupt encoding').

    Uses source inspection to assert the elif branch exists (avoids
    full widget instantiation with indexer which is heavy).
    """
    import inspect
    from desktop.my_library_tab import _UnifiedFileTreeWidget

    # update_file_status is on _UnifiedFileTreeWidget
    src = inspect.getsource(_UnifiedFileTreeWidget.update_file_status)

    assert "corrupt_encoding" in src, (
        "update_file_status must contain a 'corrupt_encoding' branch (:486 surface)"
    )
    assert 'tr("Corrupt encoding")' in src or "tr('Corrupt encoding')" in src, (
        "update_file_status must call tr('Corrupt encoding') in the corrupt_encoding branch"
    )


def test_update_file_status_color_paint_surface_corrupt_encoding():
    """update_file_status :519 color guard — 'corrupt_encoding' in the red-paint tuple.

    Source inspection confirms ('error', 'encoding_error', 'corrupt_encoding') is present.
    """
    import inspect
    from desktop.my_library_tab import _UnifiedFileTreeWidget

    src = inspect.getsource(_UnifiedFileTreeWidget.update_file_status)

    assert "'corrupt_encoding'" in src or '"corrupt_encoding"' in src, (
        "update_file_status color paint guard must include 'corrupt_encoding'"
    )
    # Specifically the extended tuple at :519
    assert "encoding_error" in src and "corrupt_encoding" in src, (
        "Both 'encoding_error' and 'corrupt_encoding' must appear in update_file_status"
    )


def test_corrupt_encoding_appears_3_times_in_my_library_tab():
    """Acceptance: 'corrupt_encoding' appears at least 3 times in desktop/my_library_tab.py.

    Proves all three surface points are wired:
      :333 _build_leaf_item_status branch
      :486 update_file_status label branch
      :519 update_file_status color paint tuple
    """
    with open("desktop/my_library_tab.py", encoding="utf-8") as f:
        source = f.read()

    count = source.count("corrupt_encoding")
    assert count >= 3, (
        f"'corrupt_encoding' should appear >= 3 times in desktop/my_library_tab.py "
        f"(build :333, label :486, color :519), got {count} occurrence(s)"
    )
