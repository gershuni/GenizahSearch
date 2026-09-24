# -*- coding: utf-8 -*-
"""The Composition Manuscript Viewer follows the result-list filters (user letter, A4/A5).

One rule (desktop/comp_view_filter.py) decides which manuscripts and pages the tree
shows and which the viewer walks through. Owner decisions, 2026-09-23:
  * split by filter type -- manuscript-level filters keep/drop a whole manuscript,
    the Context / MS Context text filters keep only the matching pages;
  * the viewer keeps the snapshot it opened with; its strip names the filters.

Before: filtered-out rows still reached the viewer; under any filter an unexpanded
appendix group vanished whole (its only child was a data-less placeholder);
expanding a group showed its rows unfiltered; the domain pass saw only top-level
rows; opening the viewer mid-batch truncated the list; clicking a manuscript whose
first page was filtered opened that hidden page.
"""
from types import MethodType

import pytest

from desktop import comp_view_filter as cvf

# ------------------------------------------------------------------ pure rule -------


def _host(fields=None, printed=(), local=(), opted_out=(), domains=None):
    fields = fields or {}
    domains = domains or {}
    return cvf.FilterHost(
        ms_fields=lambda it: fields.get(it["sys_id"], {}),
        sys_id=lambda it: it.get("sys_id", ""),
        is_printed=lambda sid: sid in printed,
        is_local=lambda it: it["sys_id"] in local,
        is_opted_out=lambda it: it["sys_id"] in opted_out,
        domains=lambda sid: domains.get(sid, []),
        page_texts=lambda p, ms: {"context": p.get("source_ctx", ""), "ms_context": p.get("text", ""),
                                  "shelfmark": p.get("_cell")},
    )


def _ms(sid, *ctxs):
    return {"type": "manuscript", "sys_id": sid,
            "pages": [{"uid": f"{sid}-{i}", "raw_header": f"{sid}_P{i}", "source_ctx": c,
                       "text": ""} for i, c in enumerate(ctxs)]}


def test_no_filter_keeps_every_page():
    ms = _ms("a", "x", "y")
    assert cvf.eligible_pages(ms, cvf.FilterState(), _host()) == ms["pages"]


def test_manuscript_level_filter_drops_the_whole_manuscript():
    st = cvf.FilterState(column_rules={"title": {"text": "Talmud"}})
    host = _host(fields={"a": {"title": "Talmud Taanit"}, "b": {"title": "Siddur"}})
    assert len(cvf.eligible_pages(_ms("a", "x", "y"), st, host)) == 2
    assert cvf.eligible_pages(_ms("b", "x", "y"), st, host) == []


def test_page_level_filter_keeps_only_matching_pages():
    st = cvf.FilterState(column_rules={"context": {"text": "target"}})
    pages = cvf.eligible_pages(_ms("a", "first", "the target", "last"), st, _host())
    assert [p["source_ctx"] for p in pages] == ["the target"]


def test_manuscript_shown_iff_ms_rules_pass_and_some_page_passes():
    st = cvf.FilterState(column_rules={"title": {"text": "T"}, "context": {"text": "hit"}})
    host = _host(fields={"a": {"title": "T"}})
    assert cvf.eligible_pages(_ms("a", "no", "nope"), st, host) == []


def test_exclude_rule_negates():
    st = cvf.FilterState(column_rules={"library": {"text": "CUL", "exclude": True}})
    host = _host(fields={"a": {"library": "CUL"}, "b": {"library": "JTS"}})
    assert cvf.eligible_pages(_ms("a", "x"), st, host) == []
    assert cvf.eligible_pages(_ms("b", "x"), st, host)


@pytest.mark.parametrize("state,printed_kept,plain_kept", [
    ("hide_printed", False, True), ("only_printed", True, False)])
def test_printed_state(state, printed_kept, plain_kept):
    st = cvf.FilterState(printed_state=state)
    host = _host(printed={"p"})
    assert bool(cvf.eligible_pages(_ms("p", "x"), st, host)) is printed_kept
    assert bool(cvf.eligible_pages(_ms("m", "x"), st, host)) is plain_kept


def test_local_states_and_optout():
    host = _host(local={"L", "O"}, opted_out={"O"})
    only = cvf.FilterState(local_state="only_local")
    no = cvf.FilterState(local_state="no_local")
    out = cvf.FilterState(optouts_active=True)
    assert cvf.eligible_pages(_ms("L", "x"), only, host)
    assert not cvf.eligible_pages(_ms("g", "x"), only, host)
    assert not cvf.eligible_pages(_ms("L", "x"), no, host)
    assert not cvf.eligible_pages(_ms("O", "x"), out, host)
    assert cvf.eligible_pages(_ms("L", "x"), out, host)


def test_domain_exclusions_all_excluded_and_uncategorized():
    host = _host(domains={"a": ["Bible"], "b": ["Bible", "Liturgy"]})
    st = cvf.FilterState(domain_exclusions=frozenset({"Bible"}))
    assert not cvf.eligible_pages(_ms("a", "x"), st, host)      # every domain excluded
    assert cvf.eligible_pages(_ms("b", "x"), st, host)          # one domain left
    assert cvf.eligible_pages(_ms("c", "x"), st, host)          # uncategorized kept...
    st_u = cvf.FilterState(domain_exclusions=frozenset({"Uncategorized"}))
    assert not cvf.eligible_pages(_ms("c", "x"), st_u, host)    # ...unless excluded


def test_single_page_manuscript_yields_its_page_not_the_aggregate():
    ms = _ms("a", "only")
    assert cvf.item_pages(ms) == [ms["pages"][0]]
    fallback = {"sys_id": "f", "raw_header": "h", "uid": "u"}
    assert cvf.item_pages(fallback) == [fallback]


def test_filter_reason_ids_keep_every_raw_reason():
    ms = {"pages": [{"filter_reason": "duplicate_photography"},
                    {"is_text_filtered": True}, {"is_filtered": True}]}
    assert cvf.filter_reason_ids(ms) == ("duplicate_photography", "filtered", "source_text")


# ------------------------------------------------------------------ Qt call sites ----

@pytest.fixture(scope="module")
def qt():
    import os
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PyQt6.QtWidgets import QApplication
    return QApplication.instance() or QApplication([])


class _Status:
    def __init__(self):
        self.messages = []

    def showMessage(self, msg, *_):
        self.messages.append(msg)


def _gui_stub(qt):
    """GenizahGUI's real composition filter + viewer methods on a stub with a real tree."""
    from PyQt6.QtWidgets import QTreeWidget
    from genizah_app import GenizahGUI

    class _Stub:
        comp_col_library, comp_col_shelfmark, comp_col_title = 1, 2, 3
        comp_col_sysid, comp_col_context, comp_col_ms_context, comp_col_printed = 4, 5, 6, 7
        _COMP_CATEGORY_LABELS = GenizahGUI._COMP_CATEGORY_LABELS

    s = _Stub()
    s.comp_tree = QTreeWidget()
    s.comp_tree.setColumnCount(9)
    s.comp_tree.setHeaderLabels(["Score", "Library", "Shelfmark", "Title", "ID",
                                 "Context", "MS Context", "Printed", "Src"])
    s.comp_filters = {}
    s._comp_printed_sys_ids = set()
    s._comp_printed_filter_state = "all"
    s._local_filter_state_composition = "all"
    s._local_filter_inactive_chip_visible = False
    s._local_file_optouts = set()
    s.comp_raw_items = []
    s._comp_results_from_parallels = False
    s._comp_domain_exclusions = set()
    s._comp_result_domain_map = {}
    s._comp_view_groups = []
    s.comp_tree_updating = False
    s._status = _Status()
    s.opened = []
    for name in ("_apply_comp_tree_filters", "_comp_filter_state", "_comp_filter_population",
                 "_comp_ms_sys_id", "_comp_viewer_entry", "on_comp_item_double_clicked",
                 "_comp_filter_summary", "_apply_comp_domain_exclusions",
                 "on_comp_tree_item_expanded", "_comp_shelf_cells",
                 "_comp_shelf_cells_uncached"):
        setattr(s, name, MethodType(getattr(GenizahGUI, name), s))
    s._comp_item_is_local = GenizahGUI._comp_item_is_local
    s._comp_preview_source_text = GenizahGUI._comp_preview_source_text
    s._comp_ms_display = lambda it: {"sys_id": it.get("sys_id", ""), "shelf": it.get("_shelf", ""),
                                     "library_code": it.get("_lib", ""), "title": it.get("_title", "")}
    s._get_meta_for_header = lambda h: (h.split("_P")[0], h.split("_P")[-1], "shelf", "title")
    s._comp_local_display_fields = lambda sid, shelf: (shelf, "")
    s._apply_local_filter = lambda rows, st: rows
    s._apply_local_optout_filter = lambda rows: rows
    s._show_local_filter_chip = lambda *a: None
    s.statusBar = lambda: s._status
    s._show_result_dialog = lambda lst, idx, filter_summary=None: s.opened.append(
        (lst, idx, filter_summary))
    s._sort_comp_items = lambda items: list(items)
    s._clear_comp_node_previews = lambda node: None

    def add_ms_node(parent, ms_item):
        from PyQt6.QtCore import Qt
        from PyQt6.QtWidgets import QTreeWidgetItem
        node = QTreeWidgetItem(parent)
        node.setData(0, Qt.ItemDataRole.UserRole, ms_item)
        if len(ms_item.get("pages", [])) > 1:
            for p in ms_item["pages"]:
                QTreeWidgetItem(node).setData(0, Qt.ItemDataRole.UserRole, p)
        return node

    s._add_manuscript_node = add_ms_node
    return s


def _item(sid, *ctxs, title="", **extra):
    it = _ms(sid, *ctxs)
    it["_title"] = title
    it.update(extra)
    return it


def _tree_with_groups(s, groups, *, lazy_categories=()):
    """Mirror display_comp_results: one root per category, rows or lazy groups."""
    from PyQt6.QtCore import Qt
    from PyQt6.QtWidgets import QTreeWidgetItem
    roots = {}
    s._comp_view_groups = groups
    for g in groups:
        root = roots.get(g.category)
        if root is None:
            root = roots[g.category] = QTreeWidgetItem(s.comp_tree, [g.category])
        if g.category in lazy_categories:
            grp = QTreeWidgetItem(root, ["", "", g.subgroup or ""])
            grp.setData(0, Qt.ItemDataRole.UserRole + 200, list(g.items))
            QTreeWidgetItem(grp, ["Loading..."]).setData(0, Qt.ItemDataRole.UserRole + 201,
                                                         "PLACEHOLDER")
        else:
            for it in g.items:
                s._add_manuscript_node(root, it)
    return roots


def _find(s, sid):
    from PyQt6.QtCore import Qt
    from PyQt6.QtWidgets import QTreeWidgetItemIterator
    it = QTreeWidgetItemIterator(s.comp_tree)
    while it.value():
        d = it.value().data(0, Qt.ItemDataRole.UserRole)
        if isinstance(d, dict) and d.get("sys_id") == sid:
            return it.value()
        it += 1
    raise AssertionError(sid)


@pytest.mark.gui
def test_viewer_leaves_out_what_the_filters_hide(qt):
    s = _gui_stub(qt)
    a, b = _item("a", "x", title="Talmud"), _item("b", "x", title="Siddur")
    _tree_with_groups(s, [cvf.ViewGroup(cvf.CATEGORY_MAIN, (a, b))])
    s.comp_filters = {s.comp_col_title: {"text": "Talmud"}}
    s._apply_comp_tree_filters()
    s.on_comp_item_double_clicked(_find(s, "a"), 0)
    lst, idx, summary = s.opened[-1]
    assert [e["display"]["id"] for e in lst] == ["a"]
    assert 'Talmud' in summary


@pytest.mark.gui
def test_viewer_includes_lazy_collapsed_and_undrawn_rows_with_categories(qt):
    s = _gui_stub(qt)
    main = _item("m", "x")
    undrawn = _item("u", "x")                   # in the group, never drawn (mid-batch)
    appx = _item("p", "x")
    filt = _item("f", "x")
    filt["pages"][0]["filter_reason"] = "duplicate_photography"
    groups = [cvf.ViewGroup(cvf.CATEGORY_MAIN, (main, undrawn)),
              cvf.ViewGroup(cvf.CATEGORY_APPENDIX, (appx,), subgroup="SIG"),
              cvf.ViewGroup(cvf.CATEGORY_FILTERED, (filt,), subgroup="Dup")]
    # The tree has drawn only `main` so far (a batched load in progress); the
    # recorded groups hold the undrawn row too.
    _tree_with_groups(s, [cvf.ViewGroup(cvf.CATEGORY_MAIN, (main,)), groups[1], groups[2]],
                      lazy_categories={cvf.CATEGORY_APPENDIX})
    s._comp_view_groups = groups
    s.on_comp_item_double_clicked(_find(s, "m"), 0)
    lst, idx, _ = s.opened[-1]
    assert [e["display"]["id"] for e in lst] == ["m", "u", "p", "f"]
    cats = [e["category"]["id"] for e in lst]
    assert cats == ["main", "main", "appendix", "filtered"]
    assert lst[2]["category"]["subgroup"] == "SIG"
    assert lst[3]["category"]["reasons"] == ("duplicate_photography",)


@pytest.mark.gui
def test_clicking_a_manuscript_opens_its_first_eligible_page(qt):
    s = _gui_stub(qt)
    a = _item("a", "first", "the target")
    _tree_with_groups(s, [cvf.ViewGroup(cvf.CATEGORY_MAIN, (a,))])
    s.comp_filters = {s.comp_col_context: {"text": "target"}}
    s._apply_comp_tree_filters()
    s.on_comp_item_double_clicked(_find(s, "a"), 0)
    lst, idx, _ = s.opened[-1]
    assert len(lst) == 1 and lst[idx]["uid"] == "a-1"


@pytest.mark.gui
def test_clicking_a_filtered_out_row_opens_nothing(qt):
    s = _gui_stub(qt)
    a, b = _item("a", "x", title="keep"), _item("b", "x", title="drop")
    _tree_with_groups(s, [cvf.ViewGroup(cvf.CATEGORY_MAIN, (a, b))])
    s.comp_filters = {s.comp_col_title: {"text": "keep"}}
    s.on_comp_item_double_clicked(_find(s, "b"), 0)
    assert s.opened == [] and s._status.messages


@pytest.mark.gui
def test_unexpanded_lazy_group_is_judged_by_its_items(qt):
    from PyQt6.QtCore import Qt
    s = _gui_stub(qt)
    keep, drop = _item("k", "x", title="keep"), _item("d", "x", title="drop")
    roots = _tree_with_groups(
        s, [cvf.ViewGroup(cvf.CATEGORY_APPENDIX, (keep, drop), subgroup="S"),
            cvf.ViewGroup(cvf.CATEGORY_FILTERED, (drop,), subgroup="T")],
        lazy_categories={cvf.CATEGORY_APPENDIX, cvf.CATEGORY_FILTERED})
    s.comp_filters = {s.comp_col_title: {"text": "keep"}}
    s._apply_comp_tree_filters()
    appx_group = roots[cvf.CATEGORY_APPENDIX].child(0)
    filt_group = roots[cvf.CATEGORY_FILTERED].child(0)
    assert appx_group.data(0, Qt.ItemDataRole.UserRole + 200)
    assert not appx_group.isHidden(), "a lazy group with a matching item vanished"
    assert filt_group.isHidden()


@pytest.mark.gui
def test_expanding_a_group_filters_its_new_rows(qt):
    s = _gui_stub(qt)
    keep, drop = _item("k", "x", title="keep"), _item("d", "x", title="drop")
    roots = _tree_with_groups(
        s, [cvf.ViewGroup(cvf.CATEGORY_APPENDIX, (keep, drop), subgroup="S")],
        lazy_categories={cvf.CATEGORY_APPENDIX})
    s.comp_filters = {s.comp_col_title: {"text": "keep"}}
    s._apply_comp_tree_filters()
    s.comp_tree_updating = True                      # e.g. mid-batch
    s.on_comp_tree_item_expanded(roots[cvf.CATEGORY_APPENDIX].child(0))
    assert not _find(s, "k").isHidden()
    assert _find(s, "d").isHidden(), "an expanded row ignored the active filter"
    assert s.comp_tree_updating is True, "expansion cleared the batch's updating flag"


@pytest.mark.gui
def test_domain_and_column_filters_compose_in_one_pass(qt):
    s = _gui_stub(qt)
    a, b = _item("a", "x", title="keep"), _item("b", "x", title="drop")
    _tree_with_groups(s, [cvf.ViewGroup(cvf.CATEGORY_MAIN, (a, b))])
    s._comp_result_domain_map = {"a": ["Bible"], "b": ["Liturgy"]}
    s.comp_filters = {s.comp_col_title: {"text": "keep"}}
    s._comp_domain_exclusions = {"Bible"}
    s._apply_comp_domain_exclusions()
    assert _find(s, "a").isHidden() and _find(s, "b").isHidden()
    s._comp_domain_exclusions = set()                # clearing domains...
    s._apply_comp_domain_exclusions()
    assert not _find(s, "a").isHidden()
    assert _find(s, "b").isHidden(), "clearing domains unhid a column-filtered row"


@pytest.mark.gui
def test_flat_view_entries_are_category_all(qt):
    s = _gui_stub(qt)
    a = _item("a", "x")
    _tree_with_groups(s, [cvf.ViewGroup(cvf.CATEGORY_ALL, (a,))])
    s.on_comp_item_double_clicked(_find(s, "a"), 0)
    assert s.opened[-1][0][0]["category"]["id"] == "all"
    assert s.opened[-1][2] == ""                     # no filters -> empty summary


# ------------------------------------------------------------------ ResultDialog -----

def _ctx_stub(qt, results, idx=0, summary=""):
    from PyQt6.QtWidgets import QLabel, QWidget
    from desktop.result_dialog import ResultDialog

    class _D:
        pass

    d = _D()
    d.results_context_bar = QWidget()
    d.lbl_res_category = QLabel()
    d.lbl_res_filters = QLabel()
    d.all_results = results
    d.current_result_idx = idx
    d._results_filter_summary = summary
    d._update_results_context = MethodType(ResultDialog._update_results_context, d)
    return d


@pytest.mark.gui
def test_viewer_shows_category_and_frozen_filter_strip(qt):
    cat = {"id": "filtered", "label": "Filtered", "subgroup": "Dup", "reasons": ()}
    d = _ctx_stub(qt, [{"category": cat}], summary="Filters from the results list: X")
    d._update_results_context()
    assert d.lbl_res_category.text() == "Filtered — Dup"
    assert "X" in d.lbl_res_filters.text()
    assert not d.results_context_bar.isHidden()


@pytest.mark.gui
def test_uncategorized_lists_show_nothing_new(qt):
    """Every other ResultDialog caller, and a row load_by_shelfmark appends."""
    d = _ctx_stub(qt, [{"uid": "x"}])
    d._update_results_context()
    assert d.results_context_bar.isHidden()
    mixed = _ctx_stub(qt, [{"category": {"id": "main", "label": "Main"}}, {"uid": "y"}],
                      idx=1, summary="S")
    mixed._update_results_context()
    assert mixed.results_context_bar.isHidden(), (
        "an appended non-composition result kept the composition filter strip")
    assert mixed.lbl_res_filters.text() == ""
    mixed.current_result_idx = 0                    # back on a composition entry
    mixed._update_results_context()
    assert not mixed.results_context_bar.isHidden()
    assert mixed.lbl_res_filters.text() == "S"


# ------------------------------------------------------------------ Codex #360 --------

def test_shelfmark_matches_the_visible_cell_then_page_rows():
    """The Shelfmark cell shows "(Image 3)" / "(2 matches)" / "[2v]": a filter on that
    visible text must not lose the row."""
    ms = _ms("a", "x", "y")
    ms["pages"][0]["_cell"] = "Image 1 [1r]"
    ms["pages"][1]["_cell"] = "Image 2 [2v]"
    host = _host(fields={"a": {"shelfmark": "T-S 1 (2 matches)"}})
    both = cvf.FilterState(column_rules={"shelfmark": {"text": "2 matches"}})
    assert len(cvf.eligible_pages(ms, both, host)) == 2
    one = cvf.FilterState(column_rules={"shelfmark": {"text": "2v"}})
    assert [p["uid"] for p in cvf.eligible_pages(ms, one, host)] == ["a-1"]
    none = cvf.FilterState(column_rules={"shelfmark": {"text": "ENA"}})
    assert cvf.eligible_pages(ms, none, host) == []


def test_single_page_row_is_judged_only_by_its_manuscript_cell():
    """A single-page manuscript has no page row, so an exclude rule must not be
    satisfied by a page cell that does not exist."""
    ms = _ms("a", "x")                      # page has no "_cell" -> None
    host = _host(fields={"a": {"shelfmark": "T-S 1 (Image 3)"}})
    st = cvf.FilterState(column_rules={"shelfmark": {"text": "Image 3", "exclude": True}})
    assert cvf.eligible_pages(ms, st, host) == []


@pytest.mark.gui
def test_shelf_cells_match_what_the_builders_draw(qt, monkeypatch):
    """The formatter IS what the builders draw: pin its output shapes."""
    import genizah_core
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")   # labels go through tr()
    s = _gui_stub(qt)
    one = {"type": "manuscript", "sys_id": "a", "pages": [{"uid": "u", "raw_header": "a_P3"}]}
    many = {"type": "manuscript", "sys_id": "a",
            "pages": [{"uid": "u1", "raw_header": "a_P3"}, {"uid": "u2", "raw_header": "a_P4"}]}
    assert s._comp_shelf_cells(one, "T-S 1")[0] == "T-S 1 (Image 3)"
    cell, pages = s._comp_shelf_cells(many, "T-S 1")
    assert cell == "T-S 1 (Image 3...)"
    assert pages[("u2", "a_P4")] == "Image 4"


@pytest.mark.gui
def test_tree_shelfmark_filter_uses_the_visible_cell(qt, monkeypatch):
    """Call site: GenizahGUI's host feeds the formatted cell to the rule."""
    import genizah_core
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")
    s = _gui_stub(qt)
    one = _item("a", "x", _shelf="T-S 1")        # single page -> "T-S 1 (Image 0)"
    many = _item("b", "x", "y", _shelf="T-S 2")  # pages b_P0, b_P1 -> "Image 1" row
    _tree_with_groups(s, [cvf.ViewGroup(cvf.CATEGORY_MAIN, (one, many))])
    s.comp_filters = {s.comp_col_shelfmark: {"text": "Image 0"}}
    s._apply_comp_tree_filters()
    assert not _find(s, "a").isHidden(), "filter on the visible '(Image 0)' lost the row"
    s.comp_filters = {s.comp_col_shelfmark: {"text": "Image 1"}}
    s._apply_comp_tree_filters()
    assert _find(s, "a").isHidden()
    b = _find(s, "b")
    assert not b.isHidden() and b.child(0).isHidden() and not b.child(1).isHidden()


@pytest.mark.gui
def test_back_to_results_switches_to_the_composition_tab(qt):
    from desktop.result_dialog import ResultDialog

    calls = []

    class _Host:
        composition_tab = object()

        def _set_active_tab(self, tab):
            calls.append(("tab", tab))

        def isMinimized(self):
            return False

        def raise_(self):
            calls.append("raise")

        def activateWindow(self):
            calls.append("activate")

    class _D:
        pass

    d = _D()
    d._app = _Host()
    ResultDialog._back_to_results(d)
    assert calls[0] == ("tab", _Host.composition_tab), "did not return to the Composition tab"
    assert "raise" in calls and "activate" in calls


def test_shelfmark_exclusion_drops_matching_page_rows_and_manuscripts():
    """Codex #360 round 3: "does not contain Image 4" must hide the Image 4 row even
    when the manuscript's own cell ("T-S 1 (Image 1...)") passes."""
    ms = _ms("a", "x", "y")
    ms["pages"][0]["_cell"] = "Image 1"
    ms["pages"][1]["_cell"] = "Image 4"
    host = _host(fields={"a": {"shelfmark": "T-S 1 (Image 1...)"}})
    st = cvf.FilterState(column_rules={"shelfmark": {"text": "Image 4", "exclude": True}})
    assert [p["uid"] for p in cvf.eligible_pages(ms, st, host)] == ["a-0"]
    st_ms = cvf.FilterState(column_rules={"shelfmark": {"text": "T-S 1", "exclude": True}})
    assert cvf.eligible_pages(ms, st_ms, host) == [], "excluded shelfmark kept via its pages"
