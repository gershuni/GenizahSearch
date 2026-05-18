# -*- coding: utf-8 -*-
"""Phase 999.4 Plan 02 — Headless Qt tests for the desktop line-number gutter.

Covers D-04, D-07, D-09, D-10, D-11 of `.planning/phases/999.4-line-numbering/999.4-CONTEXT.md`.

The helper `apply_line_numbered_text(widget, html_or_text, *, source_text, is_html)`
lives in `desktop/widgets/line_number_text_edit.py`. It attaches a sibling
LineNumberArea QWidget to the body widget — Qt's text cursor cannot extend out
of its own QTextDocument into a sibling, which is what makes the D-04
copy-paste invariant structurally trivial.

These tests skip cleanly when PyQt6 is unavailable (e.g. minimal CI env).
"""
from __future__ import annotations

import sys

import pytest

pytest.importorskip("PyQt6.QtWidgets")

from PyQt6.QtCore import Qt  # noqa: E402
from PyQt6.QtWidgets import QApplication, QTextEdit, QTextBrowser  # noqa: E402

from desktop.widgets.line_number_text_edit import (  # noqa: E402
    LineNumberArea,
    apply_line_numbered_text,
    is_line_numbers_enabled,
    set_line_numbers_enabled,
    refresh_visibility,
)


# ---------- Fixtures ----------


@pytest.fixture(scope="module")
def qapp():
    """Single QApplication for the test module (Qt apps are process-singletons)."""
    app = QApplication.instance() or QApplication(sys.argv)
    yield app
    # Do not call quit — other tests in the suite may share the app.


@pytest.fixture
def textedit(qapp):
    w = QTextEdit()
    yield w
    w.deleteLater()


@pytest.fixture
def textbrowser(qapp):
    w = QTextBrowser()
    yield w
    w.deleteLater()


@pytest.fixture(autouse=True)
def _stub_config(monkeypatch):
    """Stub load_app_config / save_app_config so tests do not touch disk.

    Tests that need different config values use monkeypatch.setattr directly.
    Default: line numbers ON (mirrors D-07 default).
    """
    state: dict = {"show_line_numbers": True}

    def _load():
        return dict(state)

    def _save(payload):
        if isinstance(payload, dict):
            state.update(payload)

    monkeypatch.setattr(
        "desktop.widgets.line_number_text_edit.load_app_config", _load
    )
    monkeypatch.setattr(
        "desktop.widgets.line_number_text_edit.save_app_config", _save
    )
    yield state


# ---------- Tests ----------


def test_line_number_area_attaches_to_qtextedit(textedit):
    """Test 1: helper attaches a LineNumberArea sibling widget on first call."""
    apply_line_numbered_text(
        textedit,
        "<div>alpha<br>beta<br>gamma</div>",
        source_text="alpha\nbeta\ngamma",
        is_html=True,
    )
    area = getattr(textedit, "_line_number_area", None)
    assert area is not None, "expected _line_number_area attribute"
    assert isinstance(area, LineNumberArea), (
        f"expected LineNumberArea, got {type(area)!r}"
    )
    # Must be a child of the body widget (parent relationship enables Qt event flow)
    assert area.parent() is textedit


def test_line_number_area_line_count_matches_split(textedit):
    """Test 2 (D-10): line count == len(source_text.split('\\n')), including blanks."""
    # 4-line text
    apply_line_numbered_text(
        textedit, "html", source_text="a\nb\nc\nd", is_html=False
    )
    assert textedit._line_number_area._line_count == 4

    # 3 lines including a blank middle line
    apply_line_numbered_text(
        textedit, "html", source_text="a\n\nb", is_html=False
    )
    assert textedit._line_number_area._line_count == 3

    # single line, no newline
    apply_line_numbered_text(
        textedit, "html", source_text="single", is_html=False
    )
    assert textedit._line_number_area._line_count == 1

    # empty source => zero lines
    apply_line_numbered_text(
        textedit, "html", source_text="", is_html=False
    )
    assert textedit._line_number_area._line_count == 0

    # trailing newline: "a\nb\n".split('\n') == ['a','b',''] -> 3 lines (D-10)
    apply_line_numbered_text(
        textedit, "html", source_text="a\nb\n", is_html=False
    )
    assert textedit._line_number_area._line_count == 3


def test_paints_one_position_per_source_line_for_br_separated_html(textedit, qapp):
    """Regression (smoke-check finding 2026-05-18): when the body widget
    receives HTML with `<br>` separators inside a single block (the form
    genizah_app.py's transcription renders use), the gutter must paint one
    number per source line — NOT just one.

    Pre-fix: iterating QTextBlocks alone yielded a single block for a
    `<br>`-separated transcription, so only "1" appeared. Post-fix:
    walking QTextLayout's visual QTextLines within each block correctly
    yields one position per `<br>`.

    This test exercises the y-computation method directly (no real screen
    needed); the painter is a thin wrapper over the same positions list.
    """
    # 3 source lines packaged as a single `<br>`-separated HTML block —
    # the canonical shape highlight_text() produces.
    apply_line_numbered_text(
        textedit,
        "<p>line one<br>line two<br>line three</p>",
        source_text="line one\nline two\nline three",
        is_html=True,
    )
    # Force layout to be computed (in a headless test the document
    # layout is not always triggered automatically).
    textedit.show()
    qapp.processEvents()
    textedit.document().adjustSize()
    qapp.processEvents()

    positions = textedit._line_number_area._compute_line_positions()
    assert len(positions) == 3, (
        f"expected 3 line positions for 3-line `<br>`-separated HTML, "
        f"got {len(positions)}: {positions!r}"
    )
    # y-positions must be strictly ascending (line 2 below line 1, etc.)
    ys = [p[0] for p in positions]
    assert ys == sorted(ys) and len(set(ys)) == 3, (
        f"line y-positions must be distinct and ascending; got {ys!r}"
    )


def test_clipboard_isolation_invariant(textedit):
    """Test 3 (D-04): toPlainText() of the body returns NO gutter digits.

    The LineNumberArea is a SIBLING QWidget — not part of QTextDocument —
    so Qt's text cursor cannot reach it. Ctrl+A inside the body produces a
    selection that contains only body text.
    """
    apply_line_numbered_text(
        textedit,
        "alpha\nbeta\ngamma",
        source_text="alpha\nbeta\ngamma",
        is_html=False,
    )
    body = textedit.toPlainText()
    assert "alpha" in body
    assert "gamma" in body
    # Gutter digits 1..3 must NOT appear in the body text
    for n in ("1", "2", "3"):
        assert n not in body, (
            f"gutter digit {n!r} leaked into body text — D-04 invariant violated"
        )


def test_toggle_hides_gutter(textedit, _stub_config):
    """Test 4 (D-09): toggling show_line_numbers config hides/shows the gutter.

    We use ``isVisibleTo(parent)`` (which checks intrinsic visibility), not
    ``isVisible()`` (which additionally requires ancestor visibility — tests
    don't show the parent QTextEdit, so ``isVisible()`` always returns False).
    """
    # Default: intended visible
    _stub_config["show_line_numbers"] = True
    apply_line_numbered_text(
        textedit, "html", source_text="a\nb", is_html=False
    )
    assert textedit._line_number_area.isVisibleTo(textedit) is True

    # Flip OFF and re-apply
    _stub_config["show_line_numbers"] = False
    apply_line_numbered_text(
        textedit, "html", source_text="a\nb", is_html=False
    )
    assert textedit._line_number_area.isVisibleTo(textedit) is False

    # Flip ON again
    _stub_config["show_line_numbers"] = True
    apply_line_numbered_text(
        textedit, "html", source_text="a\nb", is_html=False
    )
    assert textedit._line_number_area.isVisibleTo(textedit) is True

    # refresh_visibility() should also respect a config flip without re-applying text
    _stub_config["show_line_numbers"] = False
    refresh_visibility(textedit)
    assert textedit._line_number_area.isVisibleTo(textedit) is False


def test_config_persistence_default_true(textedit, monkeypatch):
    """Test 5 (D-07): default ON when 'show_line_numbers' key is absent."""

    def _empty_load():
        return {}

    monkeypatch.setattr(
        "desktop.widgets.line_number_text_edit.load_app_config", _empty_load
    )
    assert is_line_numbers_enabled() is True, (
        "default must be True per D-07 when config is empty"
    )

    def _explicit_false():
        return {"show_line_numbers": False}

    monkeypatch.setattr(
        "desktop.widgets.line_number_text_edit.load_app_config", _explicit_false
    )
    assert is_line_numbers_enabled() is False


def test_set_line_numbers_persists(monkeypatch):
    """Test 5b: set_line_numbers_enabled writes through save_app_config."""
    captured: dict = {}

    def _save(payload):
        captured.update(payload)

    monkeypatch.setattr(
        "desktop.widgets.line_number_text_edit.save_app_config", _save
    )
    set_line_numbers_enabled(False)
    assert captured == {"show_line_numbers": False}
    set_line_numbers_enabled(True)
    assert captured == {"show_line_numbers": True}


def test_apply_to_qtextbrowser_too(textbrowser):
    """Test 6: helper works on QTextBrowser (used by ResultDialog)."""
    apply_line_numbered_text(
        textbrowser,
        "<div>a<br>b<br>c</div>",
        source_text="a\nb\nc",
        is_html=True,
    )
    area = getattr(textbrowser, "_line_number_area", None)
    assert area is not None
    assert isinstance(area, LineNumberArea)
    assert area._line_count == 3
    # Same D-04 invariant must hold on QTextBrowser too
    body = textbrowser.toPlainText()
    for digit in ("1", "2", "3"):
        assert digit not in body


def test_recompute_on_repeated_call(textedit):
    """Test 7 (D-11): repeated apply updates the SAME gutter — no duplicate instances."""
    apply_line_numbered_text(
        textedit, "html", source_text="a\nb", is_html=False
    )
    first_area = textedit._line_number_area
    assert first_area._line_count == 2

    apply_line_numbered_text(
        textedit, "html", source_text="a\nb\nc\nd", is_html=False
    )
    second_area = textedit._line_number_area
    assert second_area is first_area, (
        "expected the same LineNumberArea instance; helper must update in place"
    )
    assert second_area._line_count == 4

    # Numbering restarts at 1 per call (D-11) — the count is the COUNT
    # for this render; no cumulative state across calls.
    apply_line_numbered_text(
        textedit, "html", source_text="single", is_html=False
    )
    assert textedit._line_number_area._line_count == 1


def test_rtl_layout_gutter_on_right(textedit, qapp):
    """Test 8: RTL widgets place the gutter on the visual RIGHT.

    In RTL, the gutter's geometry sits at the RIGHT edge of the body widget
    (positive x near contentsRect().right() - gutter_width).
    """
    textedit.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
    textedit.resize(400, 300)
    apply_line_numbered_text(
        textedit, "html", source_text="a\nb\nc", is_html=False
    )
    area = textedit._line_number_area
    geom = area.geometry()
    # Sanity: the area widget is a child of the body widget
    assert area.parent() is textedit
    # Geometry sits within the body widget bounds and on the right half
    assert geom.width() > 0
    # In RTL, gutter is positioned at x near the right edge of the contents rect
    cr = textedit.contentsRect()
    # The gutter's x coordinate should be > half-width (i.e. on the visual right)
    assert geom.x() >= cr.width() // 2, (
        f"RTL gutter x={geom.x()} expected on the visual right of width={cr.width()}"
    )


def test_ltr_layout_gutter_on_left(textedit):
    """Test 8b: LTR widgets place the gutter on the visual LEFT (sanity check)."""
    textedit.setLayoutDirection(Qt.LayoutDirection.LeftToRight)
    textedit.resize(400, 300)
    apply_line_numbered_text(
        textedit, "html", source_text="a\nb\nc", is_html=False
    )
    area = textedit._line_number_area
    geom = area.geometry()
    assert area.parent() is textedit
    # In LTR, gutter sits at the leading (left) edge — x near 0
    cr = textedit.contentsRect()
    assert geom.x() < cr.width() // 2, (
        f"LTR gutter x={geom.x()} expected on the visual left of width={cr.width()}"
    )
