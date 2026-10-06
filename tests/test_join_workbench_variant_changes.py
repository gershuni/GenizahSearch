# -*- coding: utf-8 -*-
"""Joins Lab searches with Basic's Num Changes (x1-x3), whatever the last main-window
search left in the shared value (owner ruling 2026-09-28; the website's Joins Lab does
the same).

Since Num Changes acts on Variants search (25ed814e), a main-window search sets the
shared ``variant_max_changes`` to its level's value. The Joins Lab window searched
with ``app.searcher`` as it was, so after a Maximum x3 search every Joins search -- the
anchor side, the other side of the leaf, a search restored with the window -- ran at
x3 instead of Basic's x1.

A real JoinWorkbenchWindow and its real JoinCandidatePane. The SearchThread and the
other-side worker are the real classes, run on this thread (start() calls run()); the
engine is a stub that records the shared x1-x3 at each search.
"""
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.gui  # a real window: gui bucket only

from PyQt6.QtTest import QTest  # noqa: E402
from PyQt6.QtWidgets import QApplication  # noqa: E402

APP = QApplication.instance() or QApplication(sys.argv)

import desktop.join_workbench as jw  # noqa: E402

SID = "990000000010205171"
ANCHOR_SIDE = "אבגד"
OTHER_SIDE = "הוזח"


class _Engine:
    """Records (query, mode, the shared x1-x3) at each search."""

    def __init__(self, settings):
        self.settings, self.ran = settings, []

    def execute_search(self, query, mode, gap, **kw):
        self.ran.append((query, mode, self.settings.variant_max_changes))
        return [{"display": {"id": SID, "shelfmark": "T-S 1", "title": "",
                             "library_code": "CUL", "img": page},
                 "uid": f"{SID}_P{page:04d}", "full_text": "x"} for page in (3, 4)]

    def get_browse_page(self, sid, p_num=None, **kw):
        return {"text": "", "total_pages": 10, "p_num": p_num}


class _Meta:
    def get_meta_for_id(self, sid):
        return ("T-S 1", "")

    def get_library_for_id(self, sid):
        return "CUL"

    def get_thumbnail(self, sid, size=None):
        return ""


class _SyncSearchThread(jw.SearchThread):
    def start(self):
        self.run()          # same thread: its signals are delivered directly


class _SyncCrossSideWorker(jw._CrossSideWorker):
    def start(self):
        self.run()


@pytest.fixture
def lab(monkeypatch):
    monkeypatch.setattr(jw, "SearchThread", _SyncSearchThread)
    monkeypatch.setattr(jw, "_CrossSideWorker", _SyncCrossSideWorker)
    # The main window's last search was Maximum at x3; Basic is x1.
    settings = SimpleNamespace(
        variant_max_changes=3,
        variant_max_changes_by_preset={"basic": 1, "extended": 2, "maximum": 3})
    app = MagicMock()
    app.lab_engine = SimpleNamespace(settings=settings)
    app.searcher = _Engine(settings)
    app.meta_mgr = _Meta()
    app.corrections_client = None
    wb = jw.JoinWorkbenchWindow(None, app)
    wb._candidate_pane._maybe_assemble = lambda: None   # enrichment and thumbnails: not here
    yield wb, app.searcher, settings
    wb.close()
    wb.deleteLater()
    QTest.qWait(10)


def _row(text):
    return {"boxes": [text], "mods": {}, "start": False, "end": False, "gap": 0}


def _anchor_side(mode_idx):
    """The anchor-side builder: Responsa-style rows with variants on (mode 0), or a
    single-line query in Variants (2) or Fuzzy (3) -- JOINS_LAB_MODE_KEYS."""
    if mode_idx == 0:
        return {"mode_idx": 0, "rows": [_row(ANCHOR_SIDE)], "global_opts": {"variants": True}}
    return {"mode_idx": mode_idx, "single_text": ANCHOR_SIDE}


def _restore(wb, other=False):
    """JoinWorkbenchWindow.restore_state without loading the anchor (no images, no
    sidecar): the builders come back and the search is deferred to the event loop
    (restore searches when the rows hold a query: the Responsa-style builder). With
    *other*, the other side of the leaf is searched too (AND)."""
    state = {"anchor": {"sys_id": SID}, "builder": _anchor_side(0)}
    if other:
        state.update({"other_builder": {"rows": [_row(OTHER_SIDE)], "global_opts": {"variants": True}},
                      "other_enabled": True, "other_mode_idx": 0})
    wb.set_anchor = lambda res: None
    wb.restore_state(state)


def _pump_until(pred, timeout_ms=3000):
    waited = 0
    while not pred() and waited < timeout_ms:
        QTest.qWait(10)
        waited += 10


def _ran(engine):
    """(which side, mode, x1-x3) of each search."""
    return [("anchor" if ANCHOR_SIDE in q else "other" if OTHER_SIDE in q else q, mode, x)
            for q, mode, x in engine.ran]


@pytest.mark.parametrize("mode_idx,core_mode", [(0, "exact"), (2, "variants"), (3, "fuzzy")])
def test_a_joins_search_runs_with_basics_changes(lab, mode_idx, core_mode):
    wb, engine, settings = lab
    pane = wb._candidate_pane
    pane.builder.from_state(_anchor_side(mode_idx))
    pane.do_search()                         # Find Candidates
    assert _ran(engine) == [("anchor", core_mode, 1)]
    assert settings.variant_max_changes == 3, "the main window's value is put back"


def test_a_restored_joins_search_runs_with_basics_changes(lab):
    wb, engine, settings = lab
    _restore(wb)
    _pump_until(lambda: engine.ran)
    assert _ran(engine) == [("anchor", "exact", 1)]
    assert settings.variant_max_changes == 3


def test_the_other_side_of_the_leaf_runs_with_basics_changes(lab):
    wb, engine, settings = lab
    _restore(wb, other=True)
    _pump_until(lambda: len(engine.ran) >= 2)
    assert _ran(engine) == [("anchor", "exact", 1), ("other", "exact", 1)]
    assert settings.variant_max_changes == 3


def test_basics_value_is_read_at_each_search(lab):
    """A Settings change while the window is open applies to its next search."""
    wb, engine, settings = lab
    pane = wb._candidate_pane
    pane.builder.from_state(_anchor_side(2))
    pane.do_search()
    settings.variant_max_changes_by_preset = {"basic": 2, "extended": 2, "maximum": 3}
    pane.do_search()
    assert _ran(engine) == [("anchor", "variants", 1), ("anchor", "variants", 2)]
    assert settings.variant_max_changes == 3
