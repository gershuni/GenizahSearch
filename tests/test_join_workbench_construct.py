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
    assert pane.table.columnCount() == 8


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
