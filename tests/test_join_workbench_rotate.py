# -*- coding: utf-8 -*-
"""SEED-017 (#10) — desktop Joins Lab + Compare rotate/fullscreen parity.

Mirrors the stub pattern in test_join_workbench_vs.py (pytest-qt-free) so the
CompareDialog pane handlers and the workbench anchor handlers are exercised
under a real QApplication without a full dialog.
"""
import sys
import types

import pytest

try:
    from PyQt6.QtCore import Qt  # noqa: F401
    from PyQt6.QtGui import QImage, QPixmap
    from PyQt6.QtWidgets import QApplication
    QT_AVAILABLE = True
except Exception:  # pragma: no cover - headless without PyQt6
    QT_AVAILABLE = False

pytestmark = pytest.mark.skipif(not QT_AVAILABLE, reason="PyQt6 not available")


@pytest.fixture(autouse=True)
def _ensure_app():
    if QT_AVAILABLE and QApplication.instance() is None:
        QApplication(sys.argv)


def _make_pixmap(w, h):
    img = QImage(w, h, QImage.Format.Format_RGB32)
    img.fill(0)
    return QPixmap.fromImage(img)


class TestRotatedPixmapHelper:
    def test_none_passthrough(self):
        from desktop.join_workbench import _rotated_pixmap
        assert _rotated_pixmap(None, 90) is None

    def test_zero_degrees_passthrough(self):
        from desktop.join_workbench import _rotated_pixmap
        pix = _make_pixmap(100, 50)
        assert _rotated_pixmap(pix, 0) is pix

    def test_90_swaps_dimensions(self):
        from desktop.join_workbench import _rotated_pixmap
        out = _rotated_pixmap(_make_pixmap(100, 50), 90)
        assert (out.width(), out.height()) == (50, 100)

    def test_negative_90_swaps_dimensions(self):
        from desktop.join_workbench import _rotated_pixmap
        out = _rotated_pixmap(_make_pixmap(100, 50), -90)
        assert (out.width(), out.height()) == (50, 100)

    def test_180_keeps_dimensions(self):
        from desktop.join_workbench import _rotated_pixmap
        out = _rotated_pixmap(_make_pixmap(100, 50), 180)
        assert (out.width(), out.height()) == (100, 50)


class TestCompareDialogRotation:
    def test_pane_rotate_accumulates_and_renders(self):
        from desktop.join_workbench import CompareDialog
        rendered = []
        stub = types.SimpleNamespace()
        stub._render_pane_image = lambda pd: rendered.append(pd.get("rotation"))
        pane = {"rotation": 0}
        CompareDialog._pane_rotate(stub, pane, 90)
        CompareDialog._pane_rotate(stub, pane, 90)
        CompareDialog._pane_rotate(stub, pane, -90)
        assert pane["rotation"] == 90
        assert rendered == [90, 180, 90], "render must run after each rotate"

    def test_pane_reset_view_delegates_to_set_pane_pix(self):
        from desktop.join_workbench import CompareDialog
        calls = {}
        stub = types.SimpleNamespace()
        stub._set_pane_pix = lambda pd, p: calls.setdefault("set", True)
        pane = {"rotation": 90, "full_pix": _make_pixmap(10, 10)}
        CompareDialog._pane_reset_view(stub, pane)
        assert calls.get("set"), "_pane_reset_view must delegate to _set_pane_pix (re-fit + clear rotation)"

    def test_pane_reset_view_no_pixmap_is_safe(self):
        from desktop.join_workbench import CompareDialog
        stub = types.SimpleNamespace()
        stub._set_pane_pix = lambda pd, p: (_ for _ in ()).throw(AssertionError("should not be called"))
        CompareDialog._pane_reset_view(stub, {"rotation": 90, "full_pix": None})  # no raise

    def test_render_pane_image_applies_rotation_before_scale(self):
        from desktop.join_workbench import CompareDialog
        captured = {}

        class _Lbl:
            def setPixmap(self, p):
                captured["pix"] = p

            def resize(self, s):
                captured["size"] = s

        pane = {
            "full_pix": _make_pixmap(100, 50),
            "img": _Lbl(),
            "zoom": 1.0,
            "rotation": 90,
            "zoom_lbl": None,
        }
        stub = types.SimpleNamespace()
        CompareDialog._render_pane_image(stub, pane)
        # 100x50 rotated 90 -> 50x100, scaled by zoom 1.0
        assert captured["pix"].width() == 50 and captured["pix"].height() == 100


class TestWorkbenchAnchorRotation:
    def _win(self):
        from unittest.mock import MagicMock
        from desktop.join_workbench import JoinWorkbenchWindow
        return JoinWorkbenchWindow(parent=None, app=MagicMock())

    def test_initial_rotation_is_0(self):
        win = self._win()
        assert win._rotation == 0

    def test_rotate_anchor_accumulates(self):
        win = self._win()
        win._rotate_anchor(90)
        assert win._rotation == 90
        win._rotate_anchor(-90)
        assert win._rotation == 0
        win._rotate_anchor(-90)
        assert win._rotation == -90

    def test_reset_anchor_view_resets_rotation(self):
        win = self._win()
        win._rotation = 180
        win._reset_anchor_view()
        assert win._rotation == 0

    def test_open_anchor_fullscreen_no_pixmap_is_safe(self):
        win = self._win()
        win._anchor_full_pix = None
        win._open_anchor_fullscreen()  # must not raise


def test_desktop_source_has_rotate_fullscreen_controls():
    """Static guard: both panes wire rotate/reset/fullscreen and reuse FullscreenImageWindow."""
    import pathlib
    src = pathlib.Path("desktop/join_workbench.py").read_text(encoding="utf-8")
    for name in (
        "_rotate_anchor", "_reset_anchor_view", "_open_anchor_fullscreen",
        "_pane_rotate", "_pane_reset_view", "_open_pane_fullscreen",
        "FullscreenImageWindow",
    ):
        assert name in src, f"missing {name} (desktop rotate/fullscreen parity)"
    for glyph in ("↺", "↻", "⛶"):
        assert glyph in src, f"missing rotate/fullscreen glyph {glyph!r}"
