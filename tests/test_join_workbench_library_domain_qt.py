# -*- coding: utf-8 -*-
"""Desktop Joins Lab: Library and Domain filters (several values each, shown only
or hidden), the Sort control, and the library · domain line on each card (as on
the website).

Real JoinCandidatePane / CandidateCard / _EnrichWorker code under a QApplication;
the window is a MagicMock with no metadata manager, so no thumbnail thread starts.
"""
from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

try:
    from PyQt6.QtWidgets import QApplication, QLabel

    _app = QApplication.instance() or QApplication(sys.argv)
    QT_AVAILABLE = True
except ImportError:
    QT_AVAILABLE = False

pytestmark = [
    pytest.mark.skipif(not QT_AVAILABLE, reason="PyQt6 not available"),
    pytest.mark.gui,   # imports PyQt6: gui bucket only
]

# (sys_id, library, domain, domain_heb) in the search's order
ROWS = [
    ("990000000000000001", "JTS", "Liturgy", "ליטורגיה"),
    ("990000000000000002", "CUL", "Bible", "מקרא"),
    ("990000000000000003", "JTS", "Bible", "מקרא"),
    ("990000000000000004", "CUL", None, None),
]


def _candidates():
    from shared.joins_lab import normalize_candidate
    return [normalize_candidate({
        "display": {"id": sid, "shelfmark": f"T-S {i}", "title": "",
                    "library_code": lib, "img": 1},
        "uid": f"{sid}_P0001", "full_text": "x",
    }) for i, (sid, lib, _d, _h) in enumerate(ROWS)]


def _enrich(cands):
    by_sid = {sid: (d, h) for sid, _l, d, h in ROWS}
    out = {}
    for c in cands:
        d, h = by_sid[c.sys_id]
        out[c.key] = {"domain": d, "domain_heb": h} if d else {}
    return out


def _pane():
    from desktop.join_workbench import JoinCandidatePane

    wb = MagicMock()
    wb.meta_mgr = None          # no ThumbResolver thread
    wb._pick_callback = None
    wb.triage = {}
    pane = JoinCandidatePane(wb=wb, executor=None)
    pane.results = _candidates()
    pane._on_enriched(_enrich(pane.results))
    return pane


def _sids(pane):
    return [c.sys_id[-1] for c in pane.wb.filtered]


def _items(combo):
    return [(combo.itemText(i), combo.itemData(i)) for i in range(combo.count())]


def _checklist(lst):
    """A Library or Domain checklist as (text, value, checked)."""
    from PyQt6.QtCore import Qt
    return [(lst.item(i).text(), lst.item(i).data(Qt.ItemDataRole.UserRole),
             lst.item(i).checkState() == Qt.CheckState.Checked)
            for i in range(lst.count())]


def _tick(lst, values):
    from PyQt6.QtCore import Qt
    for i in range(lst.count()):
        item = lst.item(i)
        item.setCheckState(Qt.CheckState.Checked
                           if item.data(Qt.ItemDataRole.UserRole) in values
                           else Qt.CheckState.Unchecked)


def _check_libraries(pane, *codes, mode="show_only"):
    _tick(pane.lib_filter, codes)
    pane.lib_mode.setCurrentIndex(pane.lib_mode.findData(mode))
    pane.apply_filters()


def _check_domains(pane, *domains, mode="show_only"):
    _tick(pane.dom_filter, domains)
    pane.dom_mode.setCurrentIndex(pane.dom_mode.findData(mode))
    pane.apply_filters()


def _dialog_parts(dlg, label):
    """The Filter dialog's checklist and Show only / Hide choice for *label*."""
    from PyQt6.QtWidgets import QComboBox, QListWidget
    [lst] = [w for w in dlg.findChildren(QListWidget) if w.accessibleName() == label]
    [mode] = [c for c in dlg.findChildren(QComboBox) if c.accessibleName() == label]
    return lst, mode


def _dialog_button(dlg, text):
    from PyQt6.QtWidgets import QPushButton
    [btn] = [b for b in dlg.findChildren(QPushButton) if b.text() == text]
    return btn


def test_options_carry_counts_and_the_search_order_stays(monkeypatch):
    import desktop.join_workbench as jw
    pane = _pane()
    assert _checklist(pane.lib_filter) == [("CUL (2)", "CUL", False), ("JTS (2)", "JTS", False)]
    assert _checklist(pane.dom_filter) == [
        ("Bible (2)", "Bible", False), ("Liturgy (1)", "Liturgy", False)]
    for mode in (pane.lib_mode, pane.dom_mode):
        assert _items(mode) == [("Show only", "show_only"), ("Hide", "hide")]
    assert _sids(pane) == ["1", "2", "3", "4"]
    # In the Hebrew UI the labels (and their order) are Hebrew; the value stays English
    monkeypatch.setattr(jw, "CURRENT_LANG", "he")
    pane._refresh_facet_filters()
    assert _checklist(pane.dom_filter) == [
        ("ליטורגיה (1)", "Liturgy", False), ("מקרא (2)", "Bible", False)]


def test_library_and_domain_filters():
    pane = _pane()
    _check_libraries(pane, "JTS")
    assert _sids(pane) == ["1", "3"]
    _check_domains(pane, "Bible")
    assert _sids(pane) == ["3"]


def test_several_libraries_or_domains_are_shown_only():
    pane = _pane()
    _check_libraries(pane, "CUL", "JTS")
    assert _sids(pane) == ["1", "2", "3", "4"]
    _check_libraries(pane)
    _check_domains(pane, "Bible", "Liturgy")
    assert _sids(pane) == ["1", "2", "3"]               # 4 has no domain
    _check_domains(pane, "Liturgy")
    assert _sids(pane) == ["1"]


def test_hidden_libraries_are_left_out():
    pane = _pane()
    _check_libraries(pane, "JTS", mode="hide")
    assert _sids(pane) == ["2", "4"]
    _check_domains(pane, "Bible", mode="hide")          # both filters at once
    assert _sids(pane) == ["4"]
    _check_libraries(pane, mode="hide")                 # nothing checked: all
    assert _sids(pane) == ["1", "4"]


def test_hidden_domains_are_left_out_and_a_fragment_with_none_stays():
    pane = _pane()
    _check_domains(pane, "Bible", mode="hide")
    assert _sids(pane) == ["1", "4"]
    _check_domains(pane, "Bible", "Liturgy", mode="hide")
    assert _sids(pane) == ["4"]
    _check_domains(pane, mode="hide")                   # nothing checked: all
    assert _sids(pane) == ["1", "2", "3", "4"]


def test_checked_values_still_on_offer_survive_new_results():
    pane = _pane()
    _check_libraries(pane, "CUL", "JTS", mode="hide")
    _check_domains(pane, "Bible", "Liturgy", mode="hide")
    pane.results = [c for c in pane.results if c.sys_id[-1] != "1"]     # Liturgy gone
    pane._on_enriched(_enrich(pane.results))
    assert _checklist(pane.dom_filter) == [("Bible (2)", "Bible", True)]
    assert pane.dom_mode.currentData() == "hide"
    pane.results = [c for c in pane.results if c.library_code == "CUL"]  # JTS gone
    pane._on_enriched(_enrich(pane.results))
    assert _checklist(pane.lib_filter) == [("CUL (2)", "CUL", True)]
    assert pane.lib_mode.currentData() == "hide"
    assert _sids(pane) == []


def test_the_filter_dialog_checks_libraries_and_domains_and_hides_them(monkeypatch):
    """The Filter dialog's Library and Domain lists and their Show only / Hide
    choices reach the filter on Apply, and its Reset clears them."""
    from PyQt6.QtCore import Qt

    import desktop.join_workbench as jw
    pane = _pane()
    seen = {}

    def _hide_bible_show_cul(dlg):
        lib, lib_mode = _dialog_parts(dlg, "Library")
        dom, dom_mode = _dialog_parts(dlg, "Domain")
        seen["items"] = ([lib.item(i).text() for i in range(lib.count())],
                         [dom.item(i).text() for i in range(dom.count())])
        _tick(lib, {"CUL"})
        _tick(dom, {"Bible"})
        dom_mode.setCurrentIndex(dom_mode.findData("hide"))
        _dialog_button(dlg, "Apply").click()
        return 1

    monkeypatch.setattr(jw.QDialog, "exec", _hide_bible_show_cul)
    pane._open_filter_dialog()
    assert seen["items"] == (["CUL (2)", "JTS (2)"], ["Bible (2)", "Liturgy (1)"])
    assert _sids(pane) == ["4"]                         # CUL: 2 is Bible, hidden
    assert pane.lib_mode.currentData() == "show_only"
    assert pane.dom_mode.currentData() == "hide"

    def _hide_cul(dlg):
        lib, lib_mode = _dialog_parts(dlg, "Library")
        _tick(lib, {"CUL"})
        lib_mode.setCurrentIndex(lib_mode.findData("hide"))
        _dialog_button(dlg, "Apply").click()
        return 1

    monkeypatch.setattr(jw.QDialog, "exec", _hide_cul)
    pane._open_filter_dialog()
    assert _sids(pane) == ["1"]                         # JTS, and not Bible

    def _reset(dlg):
        for label in ("Library", "Domain"):
            lst, mode = _dialog_parts(dlg, label)
            seen[label] = ([lst.item(i).checkState() == Qt.CheckState.Checked
                            for i in range(lst.count())], mode.currentData())
        _dialog_button(dlg, "Reset").click()
        _dialog_button(dlg, "Apply").click()
        return 1

    monkeypatch.setattr(jw.QDialog, "exec", _reset)
    pane._open_filter_dialog()
    assert seen["Library"] == ([True, False], "hide")   # mirrored
    assert seen["Domain"] == ([True, False], "hide")
    assert _sids(pane) == ["1", "2", "3", "4"]
    for lst, mode in ((pane.lib_filter, pane.lib_mode), (pane.dom_filter, pane.dom_mode)):
        assert mode.currentData() == "show_only"
        assert not any(checked for _t, _v, checked in _checklist(lst))


def test_sort_groups_and_keeps_the_search_order_within_a_group():
    pane = _pane()
    pane.order_combo.setCurrentIndex(pane.order_combo.findData("library"))
    assert _sids(pane) == ["2", "4", "1", "3"]          # CUL then JTS
    pane.order_combo.setCurrentIndex(pane.order_combo.findData("domain"))
    order = _sids(pane)
    assert order[-1] == "4"                              # no domain: last
    assert order.index("2") < order.index("3")           # one domain keeps its order
    pane.order_combo.setCurrentIndex(pane.order_combo.findData("rank"))
    assert _sids(pane) == ["1", "2", "3", "4"]


def test_a_choice_no_longer_on_offer_is_dropped():
    pane = _pane()
    _check_libraries(pane, "CUL")
    pane._on_enriched(_enrich(pane.results))
    assert _checklist(pane.lib_filter)[0] == ("CUL (2)", "CUL", True)
    pane.results = [c for c in pane.results if c.library_code == "JTS"]
    pane._on_enriched(_enrich(pane.results))
    assert _checklist(pane.lib_filter) == [("JTS (2)", "JTS", False)]   # CUL gone: any
    assert _sids(pane) == ["1", "3"]


def test_card_shows_library_and_domain():
    pane = _pane()
    texts = [lbl.text() for lbl in pane.cards[0].findChildren(QLabel)]
    assert any(t.startswith("JTS  ·  ") for t in texts), texts
    texts = [lbl.text() for lbl in pane.cards[3].findChildren(QLabel)]
    assert "CUL" in texts, texts                         # no domain: library only


def test_enrich_worker_adds_the_first_domain():
    from desktop.join_workbench import _EnrichWorker
    cands = _candidates()
    svc = MagicMock()
    svc.get_measurement_summaries_batch.return_value = {}
    svc.get_domains_for_sys_ids.return_value = {
        ROWS[0][0]: [{"domain": "Liturgy", "domain_heb": "ליטורגיה"}, {"domain": "Law"}],
    }
    worker = _EnrichWorker(svc, cands, {})
    got = []
    worker.enriched.connect(got.append)
    worker.run()                                         # synchronously, no thread
    out = got[0]
    assert out[cands[0].key]["domain"] == "Liturgy"
    assert out[cands[0].key]["domain_heb"] == "ליטורגיה"
    assert "domain" not in out[cands[1].key]


def test_the_session_keeps_the_sort():
    from desktop.join_workbench import JoinWorkbenchWindow
    win = JoinWorkbenchWindow(parent=None, app=MagicMock())
    try:
        pane = win._candidate_pane
        pane.order_combo.setCurrentIndex(pane.order_combo.findData("domain"))
        state = win.to_state()
        assert state["order"] == "domain"
        pane.order_combo.setCurrentIndex(0)
        win.restore_state({**state, "anchor": {}})
        assert pane.order_combo.currentData() == "domain"
        win.restore_state({**state, "anchor": {}, "order": "nonsense"})
        assert pane.order_combo.currentData() == "rank"
    finally:
        win.close()


def test_the_dialog_applies_its_checks_by_value_when_the_results_changed(monkeypatch):
    """New results can rebuild the lists while the dialog is open; Apply carries
    the checked values over by value, not by row."""
    import desktop.join_workbench as jw
    pane = _pane()

    def _hide_liturgy_and_jts_as_bible_and_cul_go(dlg):
        lib, lib_mode = _dialog_parts(dlg, "Library")
        dom, dom_mode = _dialog_parts(dlg, "Domain")
        _tick(lib, {"JTS"})                             # [CUL, JTS]: row 1
        _tick(dom, {"Liturgy"})                         # [Bible, Liturgy]: row 1
        dom_mode.setCurrentIndex(dom_mode.findData("hide"))
        pane.results = [c for c in pane.results if c.sys_id[-1] in "13"]   # JTS only
        pane.results[1] = pane.results[0]               # no Bible: 1 twice
        pane._on_enriched(_enrich(pane.results))        # lists [JTS], [Liturgy]
        _dialog_button(dlg, "Apply").click()
        return 1

    monkeypatch.setattr(jw.QDialog, "exec", _hide_liturgy_and_jts_as_bible_and_cul_go)
    pane._open_filter_dialog()
    assert _checklist(pane.lib_filter) == [("JTS (2)", "JTS", True)]
    assert _checklist(pane.dom_filter) == [("Liturgy (2)", "Liturgy", True)]
    assert _sids(pane) == []


def test_clear_resets_library_domain_and_sort():
    """Clear resets the Lab: a Library/Domain choice or a Sort must not narrow or
    group the next search."""
    pane = _pane()
    _check_libraries(pane, "JTS", mode="hide")
    _check_domains(pane, "Bible", mode="hide")
    pane.order_combo.setCurrentIndex(pane.order_combo.findData("domain"))
    pane._clear_lab()
    for lst, mode in ((pane.lib_filter, pane.lib_mode), (pane.dom_filter, pane.dom_mode)):
        assert lst.count() == 0 and mode.currentData() == "show_only"
    assert pane.order_combo.currentData() == "rank"
