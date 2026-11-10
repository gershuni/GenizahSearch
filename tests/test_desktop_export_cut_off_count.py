# -*- coding: utf-8 -*-
"""The desktop search Excel download counts a cut-off list as the website's
does (OPEN_ISSUES P3, "Desktop Excel downloads count a cut-off list as
complete"): a list the search cut at its limit, or that Stop ended, says "N+"
on the Credits and Info sheet; a complete list says the plain number, and so
does a selection, which is counted as selected
(web/api.py::export_excel, `result_count_capped=(not _sel and ...)`).

Driven through GenizahGUI.export_results on the stub window of
tests/test_exclusion_surfaces.py, so the wiring from the window's cut-off
signal to the workbook is what is tested, not only the builder.
"""
from __future__ import annotations

import pytest
from PyQt6.QtCore import Qt

import genizah_app
import genizah_core
from tests.test_exclusion_surfaces import (_personal_state_in_tmp,  # noqa: F401
                                           slot_errors, window)
from tests.test_export_locked_file_sites import _search_export

pytestmark = pytest.mark.gui  # imports genizah_app, which imports PyQt6

openpyxl = pytest.importorskip("openpyxl")


@pytest.fixture(autouse=True)
def _english(monkeypatch):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")
    monkeypatch.setattr(genizah_app, "CURRENT_LANG", "en", raising=False)


def _result_count(path):
    ws = openpyxl.load_workbook(path)["Credits and Info"]
    for label, value in ws.iter_rows(min_col=1, max_col=2, values_only=True):
        if label == "Result count":
            return value
    raise AssertionError("no Result count row on the Credits and Info sheet")


@pytest.mark.parametrize("cutoff, expected", [
    (None, 2),                                          # no search signal (a tag search)
    ({"capped": False, "interrupted": False}, 2),       # complete
    ({"capped": True, "interrupted": False}, "2+"),     # reached the candidate limit
    ({"capped": False, "interrupted": True}, "2+"),     # Stop ended it
])
def test_the_count_says_n_plus_when_the_list_leaves_matches_out(
        window, monkeypatch, tmp_path, cutoff, expected):  # noqa: F811 (the imported fixture)
    w, out, _shown = _search_export(window, monkeypatch, tmp_path, "xlsx")
    w._search_cutoff = cutoff
    w.export_results("xlsx")
    assert _result_count(out) == expected


def test_a_selection_from_a_cut_off_list_is_counted_as_selected(
        window, monkeypatch, tmp_path):  # noqa: F811 (the imported fixture)
    w, out, _shown = _search_export(window, monkeypatch, tmp_path, "xlsx")
    w._search_cutoff = {"capped": True, "interrupted": False}
    w.results_table.item(0, w.COL_CHECKBOX).setCheckState(Qt.CheckState.Checked)
    w.export_results("xlsx")
    assert _result_count(out) == 1
