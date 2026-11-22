# -*- coding: utf-8 -*-
"""The owner's review of the desktop shelf picker (2026-11-21): "Select all in <node>" reads right
in Hebrew when the node's name is Latin (the name isolated, the count outside it), and the Hebrew
for what is chosen says "marked" (מסומנים), not "lads" (בחורים).

The real dialog in its multiple-choice mode on the fixture tree, in Hebrew. The four profile
variables point at scratch folders.
"""
from __future__ import annotations

import os
import time

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QApplication

import genizah_core
import desktop.widgets.shelfmark_tree_picker as shelf_picker
from shared.shelfmark_tree_service import ShelfmarkTreeService
from tests.shelfmark_tree_fixture import build_fixture

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])


@pytest.fixture(autouse=True)
def _isolated_profile(tmp_path, monkeypatch):
    for var in ("USERPROFILE", "HOME", "LOCALAPPDATA", "APPDATA"):
        folder = tmp_path / "profile" / var
        folder.mkdir(parents=True)
        monkeypatch.setenv(var, str(folder))
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "he")
    monkeypatch.delenv("SHELFMARK_TREE_ENABLED", raising=False)
    yield


@pytest.fixture
def svc(tmp_path):
    db, _libraries, _meta = build_fixture(tmp_path)
    service = ShelfmarkTreeService(str(db), thread_safe=True)
    yield service
    service.close()


def _settle(dialog, pred, timeout=8.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        _APP.processEvents()
        if dialog._view is not None and not dialog._loads.busy() and pred():
            return
    raise AssertionError("the picker did not settle")


def test_select_all_isolates_a_latin_name_in_hebrew_with_the_count_outside(svc):
    _APP.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
    dialog = shelf_picker.ShelfPickerDialog(svc, mode="multi", purpose="test", on_confirm=lambda *_a: None)
    try:
        dialog.show()
        path = tuple(n.id for n in svc.ancestors("vol:100"))
        dialog.go(path)
        whole = dialog.leaf_panel.whole
        _settle(dialog, lambda: dialog._drawn_path == path and whole.isVisibleTo(dialog))
        state = dialog._paint_state["whole"]
        name, total = state["name"], state["total"]
        assert name.isascii()                                                 # a Latin name
        text = whole.text()
        assert text == f"בחירת הכל ב־{shelf_picker._FSI}{name}{shelf_picker._PDI} ({total:,})"
        assert shelf_picker._LRM not in text and shelf_picker._RLM not in text   # no forced direction
        assert whole.layoutDirection() == Qt.LayoutDirection.RightToLeft
    finally:
        dialog.reject()
        dialog.deleteLater()
        _APP.setLayoutDirection(Qt.LayoutDirection.LeftToRight)


def test_what_is_chosen_is_marked_in_hebrew():
    assert genizah_core.tr("{n} are still chosen").format(n=28) == "28 עדיין מסומנים"
    assert genizah_core.tr("{n} manuscripts chosen").format(n=40) == "40 כתבי יד מסומנים"


@pytest.mark.parametrize("multi", [False, True])
def test_every_shelfmark_is_as_wide_as_the_longest_whatever_comes_first(svc, multi):
    # the grid's items are all the size of its first: a short first one ("705") cut the rest of
    # Schoeyen's shelfmarks to "18..."
    from types import SimpleNamespace
    kw = dict(mode="multi", purpose="test", on_confirm=lambda *_a: None) if multi else {}
    dialog = shelf_picker.ShelfPickerDialog(svc, **kw)
    try:
        dialog.resize(1240, 820)
        dialog.show()
        _APP.processEvents()
        labels = ["705", "1858/1", "1858/10", "Schoeyen, Martin 1858.12"]
        rows = [{"leaf": SimpleNamespace(id=f"p{i}", sys_id=f"99{i}", shelfmark=text), "label": text}
                for i, text in enumerate(labels)]
        panel = dialog.leaf_panel
        panel.setVisible(True)
        panel.fill({"rows": rows, "node": "n", "title": "t", "total": len(rows), "filter_key": "k", "more": 0})
        _APP.processEvents()
        grid = panel.grid
        widths = {grid.item(i).sizeHint().width() for i in range(grid.count())}
        need = max(grid.fontMetrics().horizontalAdvance(text) for text in labels)
        assert len(widths) == 1 and widths.pop() >= need                    # one width, the longest's
        for i in range(grid.count()):
            assert grid.visualItemRect(grid.item(i)).width() >= grid.fontMetrics().horizontalAdvance(labels[i])
    finally:
        dialog.reject()
        dialog.deleteLater()
