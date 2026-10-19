# -*- coding: utf-8 -*-
"""Desktop Joins Lab: Library and Domain filters, the Sort control, and the
library · domain line on each card (as on the website).

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


def test_options_carry_counts_and_the_search_order_stays(monkeypatch):
    import desktop.join_workbench as jw
    pane = _pane()
    assert _items(pane.lib_filter) == [
        ("any library", ""), ("CUL (2)", "CUL"), ("JTS (2)", "JTS")]
    assert _items(pane.dom_filter) == [
        ("any domain", ""), ("Bible (2)", "Bible"), ("Liturgy (1)", "Liturgy")]
    assert _sids(pane) == ["1", "2", "3", "4"]
    # In the Hebrew UI the labels (and their order) are Hebrew; the value stays English
    monkeypatch.setattr(jw, "CURRENT_LANG", "he")
    pane._refresh_facet_filters()
    assert [(t, d) for t, d in _items(pane.dom_filter)][1:] == [
        ("ליטורגיה (1)", "Liturgy"), ("מקרא (2)", "Bible")]


def test_library_and_domain_filters():
    pane = _pane()
    pane.lib_filter.setCurrentIndex(pane.lib_filter.findData("JTS"))
    pane.apply_filters()
    assert _sids(pane) == ["1", "3"]
    pane.dom_filter.setCurrentIndex(pane.dom_filter.findData("Bible"))
    pane.apply_filters()
    assert _sids(pane) == ["3"]


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


def test_a_choice_still_on_offer_survives_new_results():
    pane = _pane()
    pane.lib_filter.setCurrentIndex(pane.lib_filter.findData("CUL"))
    pane._on_enriched(_enrich(pane.results))
    assert pane.lib_filter.currentData() == "CUL"
    pane.results = [c for c in pane.results if c.library_code == "JTS"]
    pane._on_enriched(_enrich(pane.results))
    assert pane.lib_filter.currentData() == ""          # CUL gone: any library


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
