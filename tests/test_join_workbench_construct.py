# -*- coding: utf-8 -*-
"""Phase 108 regression: the Join Lab widgets must CONSTRUCT without error.

Guards the bug where ``JoinWorkbenchWindow`` crashed on open with
``AttributeError: 'JoinQueryBuilder' object has no attribute '_preview_edit'``
— ``_init_ui`` called ``add_row()`` (which runs ``_update_preview`` →
``self._preview_edit``) BEFORE the preview QLineEdit (and the modifier /
global-option checkboxes) were created. The fix moves the first ``add_row``
call to the END of ``_init_ui``.

The headless parser tests (test_join_workbench_builder/triage) never construct
the Qt widgets — they verify the compose()/parser contracts — so they could not
catch this. These tests DO construct the real widgets under a QApplication.

Style: pytest-qt-FREE (QApplication.instance() or QApplication(sys.argv)).
CI-skipped via tests/conftest.py collect_ignore_glob — like every other desktop
Qt-widget test, the QThread/QWidget teardown races with the full-suite teardown
on the headless GitHub runner (see conftest.py). These run locally on a real
display (or offscreen) where they caught the bug.
"""
from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

try:
    from PyQt6.QtWidgets import QApplication

    _app = QApplication.instance() or QApplication(sys.argv)
    QT_AVAILABLE = True
except ImportError:
    QT_AVAILABLE = False

pytestmark = pytest.mark.skipif(not QT_AVAILABLE, reason="PyQt6 not available")


@pytest.fixture(autouse=True)
def _ensure_app():
    if QT_AVAILABLE and QApplication.instance() is None:
        QApplication(sys.argv)
    yield


def test_join_query_builder_constructs_anchor_side():
    """Anchor-side builder (allow_page_position=True) builds with one blank row."""
    from desktop.join_workbench import JoinQueryBuilder

    b = JoinQueryBuilder(on_search=lambda: None, first_hint="hint",
                         allow_page_position=True)
    assert len(b.rows) == 1
    assert b.page_pos is not None          # page-position control present (RR-5)
    assert b._preview_edit.text() == ""    # the attribute that used to be missing


def test_join_query_builder_constructs_other_side():
    """Other-side builder (allow_page_position=False) has no page-position control."""
    from desktop.join_workbench import JoinQueryBuilder

    b = JoinQueryBuilder(on_search=lambda: None, first_hint="hint",
                         allow_page_position=False)
    assert len(b.rows) == 1
    assert b.page_pos is None              # RR-5: other side never exposes it
    assert b._preview_edit.text() == ""


def test_join_candidate_pane_constructs():
    """The full right-pane candidate surface builds (anchor + other builder + grid + table)."""
    from desktop.join_workbench import JoinCandidatePane

    pane = JoinCandidatePane(wb=MagicMock(), executor=None)
    assert len(pane.builder.rows) == 1
    assert len(pane.other_builder.rows) == 1
    assert pane.other_builder.page_pos is None
    # Column 0 = checkbox; 1..8 = data (adapted_decision 8)
    assert pane.table.columnCount() == 9
    # Per-row ⚙ gear button present in first row
    assert "gear" in pane.builder.rows[0]
    # No include_anchor_chk (removed in adapted_decision 11)
    assert not hasattr(pane, "include_anchor_chk")
    # Selection set present
    assert hasattr(pane, "_selected_keys")
    # Bulk bar widget present
    assert hasattr(pane, "_bulk_bar_widget")
    # Search options button present
    assert hasattr(pane.builder, "_btn_search_opts")


def test_join_workbench_window_opens():
    """Regression: JoinWorkbenchWindow opens (full _init_ui) without AttributeError.

    This is the exact path that crashed in production ("the join lab does not
    open"): JoinWorkbenchWindow.__init__ -> _init_ui -> _build_right_pane ->
    JoinCandidatePane._build_ui -> JoinQueryBuilder().
    """
    from desktop.join_workbench import JoinWorkbenchWindow

    win = JoinWorkbenchWindow(parent=None, app=MagicMock())
    assert win.windowTitle()                # "Joins Lab"
    assert win._executor is not None
    assert hasattr(win, "anchor_shelf")     # left (Phase-107) pane built


# ── Polish round 2: Feature 7 — session persistence round-trip tests ─────────


def test_join_query_builder_to_state_from_state_round_trip():
    """JoinQueryBuilder.to_state() → fresh builder.from_state() round-trips losslessly.

    Sets rows/boxes/mods/gaps/global-opts, serializes, builds a fresh builder,
    restores from state, and asserts identical state AND identical build_side_query() output.
    """
    from desktop.join_workbench import JoinQueryBuilder

    # Build a builder with known state
    b1 = JoinQueryBuilder(on_search=lambda: None, first_hint="hint",
                          allow_page_position=True)
    # Set up first row
    row0 = b1.rows[0]
    row0["boxes"][0]["edit"].setText("מילה")
    row0["mods"] = {"negation": False, "plene": True, "prefix": False,
                    "suffix": False, "wildcard_prefix": False, "wildcard_suffix": True}
    row0["start"].setChecked(True)
    row0["end"].setChecked(False)
    row0["gap"].setValue(3)

    # Add second row
    entry2 = b1.add_row()
    entry2["boxes"][0]["edit"].setText("שורה")
    entry2["mods"] = {"negation": False, "plene": False, "prefix": True,
                      "suffix": True, "wildcard_prefix": False, "wildcard_suffix": False}
    entry2["start"].setChecked(False)
    entry2["end"].setChecked(True)
    entry2["gap"].setValue(0)

    # Set global opts
    b1._global_opts = {
        "variants": True,
        "ja": False,
        "flex_spacing": True,
        "bidirectional": False,
    }
    # Set page position (index 1 = "page: start of text")
    b1.page_pos.setCurrentIndex(1)

    # Capture expected query
    expected_query = b1.build_side_query()

    # Serialize
    state = b1.to_state()
    assert len(state["rows"]) == 2
    assert state["global_opts"]["variants"] is True
    assert state["global_opts"]["flex_spacing"] is True
    assert state["page_pos_idx"] == 1

    # Build a fresh builder and restore
    b2 = JoinQueryBuilder(on_search=lambda: None, first_hint="hint",
                          allow_page_position=True)
    b2.from_state(state)

    # Assert restored state matches
    assert len(b2.rows) == 2
    assert b2.rows[0]["boxes"][0]["edit"].text() == "מילה"
    assert b2.rows[0]["mods"]["plene"] is True
    assert b2.rows[0]["mods"]["wildcard_suffix"] is True
    assert b2.rows[0]["start"].isChecked() is True
    assert b2.rows[0]["end"].isChecked() is False
    assert b2.rows[0]["gap"].value() == 3

    assert b2.rows[1]["boxes"][0]["edit"].text() == "שורה"
    assert b2.rows[1]["mods"]["prefix"] is True
    assert b2.rows[1]["mods"]["suffix"] is True
    assert b2.rows[1]["end"].isChecked() is True
    assert b2.rows[1]["gap"].value() == 0

    assert b2._global_opts["variants"] is True
    assert b2._global_opts["flex_spacing"] is True
    assert b2.page_pos.currentIndex() == 1

    # Assert build_side_query() produces identical output
    restored_query = b2.build_side_query()
    assert expected_query == restored_query, (
        f"Round-trip mismatch:\n  expected: {expected_query}\n  got: {restored_query}"
    )


def test_join_workbench_window_to_state_open_false():
    """JoinWorkbenchWindow.to_state() returns a dict with open=False when not shown."""
    from desktop.join_workbench import JoinWorkbenchWindow

    win = JoinWorkbenchWindow(parent=None, app=MagicMock())
    # Not shown (isVisible() == False by default for a QDialog not yet exec()/show()ed)
    state = win.to_state()
    assert isinstance(state, dict)
    assert "open" in state
    assert state["open"] is False   # freshly built, never shown
    assert "anchor" in state
    assert "builder" in state
    assert "triage" in state
