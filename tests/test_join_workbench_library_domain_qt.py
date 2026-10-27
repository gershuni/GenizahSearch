# -*- coding: utf-8 -*-
"""Desktop Joins Lab: Library and Domain filters (several values each, shown only
or hidden; Domain the FJMS tree; each counting what the other filters keep), the
Filter dialog's checklists (Select All / None, live counts), the chips row of
active filters, the Sort control, and the library · domain line on each card.

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


def _choose(pane, key, *values, mode="show_only"):
    pane.facet_chosen[key] = set(values)
    pane.facet_mode[key] = mode
    pane.apply_filters()


def _rows(pane):
    """The Library and Domain options as the Filter dialog would list them."""
    return pane._facet_rows(pane._filter_spec())


def _nested_pane():
    """Two Rabbinic Literature fragments (CUL: Aggadic Midrashim, JTS: Midrash) and
    a Piyyut one (JTS), with their places in the domain tree."""
    from shared.joins_lab import primary_domain
    pane = _pane()
    pane.results = pane.results[:3]
    domains = ["Aggadic Midrashim", "Midrash", "Piyyut"]
    libs = ["CUL", "JTS", "JTS"]
    for c, lib in zip(pane.results, libs):
        object.__setattr__(c, "library_code", lib)
    pane._on_enriched({c.key: primary_domain([{"domain": d}])
                       for c, d in zip(pane.results, domains)})
    return pane


def _checklist(tree):
    """A dialog checklist as (text, value, check state, depth), each before its branch."""
    from PyQt6.QtCore import Qt
    out = []

    def _walk(item, depth):
        out.append((item.text(0), item.data(0, Qt.ItemDataRole.UserRole),
                    item.checkState(0).name, depth))
        for i in range(item.childCount()):
            _walk(item.child(i), depth + 1)

    for i in range(tree.topLevelItemCount()):
        _walk(tree.topLevelItem(i), 0)
    return out


def _dialog_parts(dlg, label):
    """The Filter dialog's checklist and Show only / Hide choice for *label*."""
    from PyQt6.QtWidgets import QComboBox, QTreeWidget
    [tree] = [w for w in dlg.findChildren(QTreeWidget) if w.accessibleName() == label]
    [mode] = [c for c in dlg.findChildren(QComboBox) if c.accessibleName() == label]
    return tree, mode


def _dialog_button(dlg, text, facet=None):
    """A dialog button by its text; with *facet* ("Library" / "Domain"), that
    checklist's Select All / None (named for it, for screen readers too)."""
    from PyQt6.QtWidgets import QPushButton
    buttons = [b for b in dlg.findChildren(QPushButton) if b.text() == text]
    if facet is not None:
        buttons = [b for b in buttons if b.accessibleName() == f"{facet}: {text}"]
    [btn] = buttons
    return btn


def _set_check(tree, value, state):
    from PyQt6.QtCore import Qt
    from desktop.join_workbench import _tree_items
    [item] = [i for i in _tree_items(tree) if i.data(0, Qt.ItemDataRole.UserRole) == value]
    item.setCheckState(0, getattr(Qt.CheckState, state))


def _open_dialog(pane, monkeypatch, drive):
    """Open the Filter dialog with *drive(dlg)* in place of its event loop."""
    import desktop.join_workbench as jw
    monkeypatch.setattr(jw.QDialog, "exec", lambda dlg: (drive(dlg), 1)[1])
    pane._open_filter_dialog()


# --- Options, counts and filters -----------------------------------------------

def test_options_carry_counts_and_the_search_order_stays(monkeypatch):
    import desktop.join_workbench as jw
    pane = _pane()
    libs, doms = _rows(pane)
    assert libs == [("CUL", "CUL", 2, 0), ("JTS", "JTS", 2, 0)]
    assert doms == [("Bible", "Bible", 2, 0), ("Liturgy", "Liturgy", 1, 0)]
    assert _sids(pane) == ["1", "2", "3", "4"]
    # In the Hebrew UI the labels are Hebrew; the value stays English
    monkeypatch.setattr(jw, "CURRENT_LANG", "he")
    _libs, doms = _rows(pane)
    assert [(v, label) for v, label, _n, _d in doms] == [("Bible", "מקרא"), ("Liturgy", "ליטורגיה")]


def test_library_and_domain_filters():
    pane = _pane()
    _choose(pane, "libraries", "JTS")
    assert _sids(pane) == ["1", "3"]
    _choose(pane, "domains", "Bible")
    assert _sids(pane) == ["3"]


def test_several_libraries_or_domains_are_shown_only():
    pane = _pane()
    _choose(pane, "libraries", "CUL", "JTS")
    assert _sids(pane) == ["1", "2", "3", "4"]
    _choose(pane, "libraries")
    _choose(pane, "domains", "Bible", "Liturgy")
    assert _sids(pane) == ["1", "2", "3"]               # 4 has no domain


def test_hidden_libraries_and_domains_are_left_out():
    pane = _pane()
    _choose(pane, "libraries", "JTS", mode="hide")
    assert _sids(pane) == ["2", "4"]
    _choose(pane, "domains", "Bible", mode="hide")      # both filters at once
    assert _sids(pane) == ["4"]                          # no domain: not hidden
    _choose(pane, "libraries", mode="hide")              # nothing chosen: all
    assert _sids(pane) == ["1", "4"]


def test_each_filter_counts_what_the_other_filters_keep():
    pane = _pane()
    _choose(pane, "domains", "Bible")
    libs, doms = _rows(pane)
    assert [(v, n) for v, _l, n, _d in libs] == [("CUL", 1), ("JTS", 1)]
    assert [(v, n) for v, _l, n, _d in doms] == [("Bible", 2), ("Liturgy", 1)]   # its own
    _choose(pane, "libraries", "JTS", mode="hide")
    _libs, doms = _rows(pane)
    assert [(v, n) for v, _l, n, _d in doms] == [("Bible", 1), ("Liturgy", 0)]
    pane.wb.triage = {"990000000000000002": "yes"}
    pane.tri_filter.setCurrentIndex(1)                   # the other filters count too
    libs, _doms = _rows(pane)
    assert [(v, n) for v, _l, n, _d in libs] == [("CUL", 1), ("JTS", 0)]


def test_domains_are_the_fjms_tree_and_a_parent_takes_its_branch():
    pane = _nested_pane()
    _libs, doms = _rows(pane)
    assert doms == [
        ("Rabbinic Literature", "Rabbinic Literature", 2, 0), ("Midrash", "Midrash", 2, 1),
        ("Aggadic Midrashim", "Aggadic Midrashim", 1, 2),
        ("Piyut and its Interpretation", "Piyut and its Interpretation", 1, 0),
        ("Piyyut", "Piyyut", 1, 1)]
    _choose(pane, "domains", "Rabbinic Literature")
    assert _sids(pane) == ["1", "2"]
    _choose(pane, "domains", "Midrash", mode="hide")
    assert _sids(pane) == ["3"]


def test_a_choice_no_longer_among_the_results_is_dropped():
    pane = _pane()
    _choose(pane, "libraries", "CUL", "JTS", mode="hide")
    _choose(pane, "domains", "Bible", "Liturgy", mode="hide")
    pane.results = [c for c in pane.results if c.sys_id[-1] != "1"]     # Liturgy gone
    pane._on_enriched(_enrich(pane.results))
    assert pane.facet_chosen["domains"] == {"Bible"}
    assert pane.facet_mode["domains"] == "hide"
    pane.results = [c for c in pane.results if c.library_code == "CUL"]  # JTS gone
    pane._on_enriched(_enrich(pane.results))
    assert pane.facet_chosen["libraries"] == {"CUL"}
    assert _sids(pane) == []


# --- The Filter dialog -----------------------------------------------------------

def test_the_dialog_lists_libraries_and_the_domain_tree(monkeypatch):
    pane = _nested_pane()
    pane.facet_chosen["domains"] = {"Rabbinic Literature"}
    seen = {}

    def drive(dlg):
        lib, _mode = _dialog_parts(dlg, "Library")
        dom, _mode = _dialog_parts(dlg, "Domain")
        seen["lib"], seen["dom"] = _checklist(lib), _checklist(dom)
        seen["decorated"] = (lib.rootIsDecorated(), dom.rootIsDecorated())

    _open_dialog(pane, monkeypatch, drive)
    assert seen["lib"] == [("CUL (1)", "CUL", "Unchecked", 0), ("JTS (1)", "JTS", "Unchecked", 0)]
    assert seen["dom"] == [                     # a chosen parent comes with its branch
        ("Rabbinic Literature (2)", "Rabbinic Literature", "Checked", 0),
        ("Midrash (2)", "Midrash", "Checked", 1),
        ("Aggadic Midrashim (1)", "Aggadic Midrashim", "Checked", 2),
        ("Piyut and its Interpretation (1)", "Piyut and its Interpretation", "Unchecked", 0),
        ("Piyyut (1)", "Piyyut", "Unchecked", 1)]
    assert seen["decorated"] == (False, True)   # Library is a plain list


def test_the_dialog_counts_follow_its_choices_before_apply(monkeypatch):
    """Checking a domain in the dialog updates the library counts at once, and the
    other way round; a parent checked takes its branch; Apply keeps the choice."""
    pane = _nested_pane()
    seen = {}

    def drive(dlg):
        lib, lib_mode = _dialog_parts(dlg, "Library")
        dom, _dom_mode = _dialog_parts(dlg, "Domain")
        _set_check(dom, "Piyut and its Interpretation", "Checked")
        seen["piyyut"] = _checklist(dom)[-1][2]                # the branch came along
        seen["lib_after_domain"] = [t for t, *_ in _checklist(lib)]
        _set_check(lib, "CUL", "Checked")
        lib_mode.setCurrentIndex(lib_mode.findData("hide"))
        seen["dom_after_library"] = [t for t, *_ in _checklist(dom)]
        _dialog_button(dlg, "Apply").click()

    _open_dialog(pane, monkeypatch, drive)
    assert seen["piyyut"] == "Checked"
    assert seen["lib_after_domain"] == ["CUL (0)", "JTS (1)"]
    assert seen["dom_after_library"] == [
        "Rabbinic Literature (1)", "Midrash (1)", "Aggadic Midrashim (0)",
        "Piyut and its Interpretation (1)", "Piyyut (1)"]
    assert pane.facet_chosen == {"libraries": {"CUL"},
                                 "domains": {"Piyut and its Interpretation", "Piyyut"}}
    assert pane.facet_mode == {"libraries": "hide", "domains": "show_only"}
    assert _sids(pane) == ["3"]


def test_select_all_and_none_and_reset(monkeypatch):
    pane = _nested_pane()
    pane.facet_chosen["libraries"] = {"CUL"}
    pane.facet_mode["libraries"] = "hide"
    seen = {}

    def drive(dlg):
        lib, lib_mode = _dialog_parts(dlg, "Library")
        dom, dom_mode = _dialog_parts(dlg, "Domain")
        _dialog_button(dlg, "Select All", facet="Domain").click()
        seen["all"] = {state for _t, _v, state, _d in _checklist(dom)}
        _dialog_button(dlg, "Select None", facet="Domain").click()
        seen["none"] = {state for _t, _v, state, _d in _checklist(dom)}
        _dialog_button(dlg, "Select All", facet="Library").click()
        seen["lib_all"] = {state for _t, _v, state, _d in _checklist(lib)}
        _dialog_button(dlg, "Reset").click()
        seen["reset"] = ({state for _t, _v, state, _d in _checklist(lib)},
                         lib_mode.currentData())
        _dialog_button(dlg, "Apply").click()

    _open_dialog(pane, monkeypatch, drive)
    assert seen["all"] == {"Checked"} and seen["none"] == {"Unchecked"}
    assert seen["lib_all"] == {"Checked"}
    assert seen["reset"] == ({"Unchecked"}, "show_only")
    assert pane.facet_chosen == {"libraries": set(), "domains": set()}
    assert _sids(pane) == ["1", "2", "3"]


def test_the_dialog_drops_a_choice_the_new_results_lack(monkeypatch):
    """New results can come while the dialog is open; Apply keeps the checked
    values still among them."""
    pane = _pane()

    def drive(dlg):
        lib, _lib_mode = _dialog_parts(dlg, "Library")
        dom, dom_mode = _dialog_parts(dlg, "Domain")
        _set_check(lib, "JTS", "Checked")
        _set_check(dom, "Liturgy", "Checked")
        _set_check(dom, "Bible", "Checked")
        dom_mode.setCurrentIndex(dom_mode.findData("hide"))
        pane.results = [c for c in pane.results if c.sys_id[-1] in "14"]   # no Bible
        pane._on_enriched(_enrich(pane.results))
        _dialog_button(dlg, "Apply").click()

    _open_dialog(pane, monkeypatch, drive)
    assert pane.facet_chosen == {"libraries": {"JTS"}, "domains": {"Liturgy"}}
    assert _sids(pane) == []


# --- The chips row of active filters ----------------------------------------------

def _chip_texts(pane):
    from PyQt6.QtWidgets import QPushButton
    layout = pane._chips_layout
    return [layout.itemAt(i).widget().text() for i in range(layout.count())
            if isinstance(layout.itemAt(i).widget(), QPushButton)]


def test_the_chips_row_names_every_filter_and_each_chip_clears_its_own():
    pane = _nested_pane()
    assert not pane._chips_widget.isVisibleTo(pane) and pane.btn_filter.text() == "Filter ▾"
    pane.facet_chosen["domains"] = {"Rabbinic Literature", "Midrash", "Piyyut"}
    pane.facet_mode["domains"] = "hide"
    pane.facet_chosen["libraries"] = {"JTS"}
    pane.dim_chk.setChecked(True)
    pane.apply_filters()
    assert pane._chips_widget.isVisibleTo(pane)
    assert _chip_texts(pane) == [
        "Library: JTS  ×",
        "Domain · Hiding: Piyyut, Rabbinic Literature  ×",   # Midrash: under its parent
        "Has dimensions  ×",
        "Clear all"]
    assert pane.btn_filter.text() == "Filter (3) ▾"

    layout = pane._chips_layout
    layout.itemAt(0).widget().click()                         # the Library chip
    assert pane.facet_chosen["libraries"] == set()
    assert _chip_texts(pane)[0].startswith("Domain")
    assert pane.btn_filter.text() == "Filter (2) ▾"

    [clear_all] = [layout.itemAt(i).widget() for i in range(layout.count())
                   if layout.itemAt(i).widget().text() == "Clear all"]
    clear_all.click()
    assert pane.facet_chosen == {"libraries": set(), "domains": set()}
    assert pane.facet_mode["domains"] == "show_only" and not pane.dim_chk.isChecked()
    assert not pane._chips_widget.isVisibleTo(pane) and pane.btn_filter.text() == "Filter ▾"
    assert _sids(pane) == ["1", "2", "3"]


def test_a_long_choice_is_shortened_on_its_chip_and_whole_in_its_tooltip():
    pane = _pane()
    pane.filter_in.setText("T-S")
    _choose(pane, "domains", "Bible", "Liturgy", "Law")      # Law is not in the results
    chip = pane._chips_layout.itemAt(1).widget()
    assert chip.text() == "Domain: Bible, Law +1  ×"
    assert chip.toolTip() == "Domain: Bible, Law, Liturgy"
    assert pane._chips_layout.itemAt(0).widget().text() == "Text: T-S  ×"


# --- Cards, enrichment, session, Clear -----------------------------------------------

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


def test_card_shows_library_and_domain():
    pane = _pane()
    texts = [lbl.text() for lbl in pane.cards[0].findChildren(QLabel)]
    assert any(t.startswith("JTS  ·  ") for t in texts), texts
    texts = [lbl.text() for lbl in pane.cards[3].findChildren(QLabel)]
    assert "CUL" in texts, texts                         # no domain: library only


def test_enrich_worker_adds_the_first_domain_and_its_place_in_the_tree():
    from desktop.join_workbench import _EnrichWorker
    cands = _candidates()
    svc = MagicMock()
    svc.get_measurement_summaries_batch.return_value = {}
    svc.get_domains_for_sys_ids.return_value = {
        ROWS[0][0]: [{"domain": "Midrash", "domain_heb": "מדרש"}, {"domain": "Law"}],
    }
    svc.get_domain_hierarchy.return_value = {
        "Rabbinic Literature": {"parent_domain_heb": "ספרות חז\"ל", "children": []}}
    worker = _EnrichWorker(svc, cands, {})
    got = []
    worker.enriched.connect(got.append)
    worker.run()                                         # synchronously, no thread
    out = got[0]
    assert out[cands[0].key]["domain"] == "Midrash"
    assert out[cands[0].key]["domain_heb"] == "מדרש"
    assert out[cands[0].key]["domain_path"] == [
        ["Rabbinic Literature", "Rabbinic Literature", "ספרות חז\"ל"],
        ["Midrash", "Midrash", "מדרש"]]
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


def test_clear_resets_library_domain_and_sort():
    """Clear resets the Lab: a Library/Domain choice or a Sort must not narrow or
    group the next search."""
    pane = _pane()
    _choose(pane, "libraries", "JTS", mode="hide")
    _choose(pane, "domains", "Bible", mode="hide")
    pane.order_combo.setCurrentIndex(pane.order_combo.findData("domain"))
    pane._clear_lab()
    assert pane.facet_chosen == {"libraries": set(), "domains": set()}
    assert pane.facet_mode == {"libraries": "show_only", "domains": "show_only"}
    assert pane.order_combo.currentData() == "rank"
    assert not pane._chips_widget.isVisibleTo(pane)
