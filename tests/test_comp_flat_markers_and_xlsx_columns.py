"""Composition results: the flat-view filtered marker, and the xlsx Report View columns.

Both reported by the owner on 2026-09-24:

* With "Sort by shelfmark only" on, every group is merged into one "All Results"
  list, so a row the Filter Text routed to the Filtered group looked exactly like a
  main result -- a working Filter Text seemed to do nothing. Flat rows from the
  Filtered and Excluded groups are now marked, and the root label counts them.
* The xlsx Report View sheet wrote the shelfmark under "Library" and the library
  under "Shelfmark" (its three row builders had the two values the wrong way round);
  the Raw Data sheet was right.
"""

import os
from types import MethodType, SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtWidgets import QApplication, QTreeWidget, QTreeWidgetItem

import genizah_app
import genizah_core
from genizah_app import GenizahGUI

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only

_APP = QApplication.instance() or QApplication([])


@pytest.fixture(autouse=True)
def _english(monkeypatch):
    # Labels are asserted in English; this machine may run the app in Hebrew.
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")
    monkeypatch.setattr(genizah_app, "CURRENT_LANG", "en", raising=False)


def _ms(sid, shelf, library, title, pages=1, **extra):
    item = {
        "type": "manuscript", "sys_id": sid, "score": 10,
        "_shelf": shelf, "_library": library, "_title": title,
        "pages": [{"uid": f"{sid}_{i}", "raw_header": f"{sid}_{i}", "score": 5,
                   "source_ctx": "src", "text": "ms", **extra}
                  for i in range(1, pages + 1)],
    }
    return item


# --------------------------------------------------------------------------------------
# Flat view marker -- through the real batch loader and the real row builder.
# --------------------------------------------------------------------------------------

class _FlatHost:
    comp_col_library = 1
    comp_col_shelfmark = 2
    comp_col_title = 3
    comp_col_sysid = 4
    comp_col_printed = 7
    comp_col_src = 8

    def __init__(self):
        self.comp_tree = QTreeWidget()
        self.comp_tree.setColumnCount(10)
        self.comp_tree_updating = True
        for name in ("_start_batched_tree_load", "_process_tree_batch",
                     "_finish_batched_tree_load", "_batch_parent_is_alive",
                     "_add_manuscript_node", "_make_node_checkable",
                     "_comp_flat_marker_map", "_comp_flat_root_label",
                     "_apply_comp_flat_marker", "_get_filter_reason"):
            setattr(self, name, MethodType(getattr(GenizahGUI, name), self))

    # Stand-ins for the metadata and preview services the row builder calls.
    def _comp_ms_display(self, ms_item):
        return {"sys_id": ms_item["sys_id"], "shelf": ms_item["_shelf"],
                "library_code": ms_item["_library"], "library_full": "",
                "title": ms_item["_title"], "is_local": False}

    def _set_comp_tree_text(self, node, col, text):
        node.setText(col, str(text))

    def _comp_shelf_cells(self, ms_item, shelf):
        return shelf, {}

    def _get_meta_for_header(self, raw_header):
        return ("", "1", "", "")

    def _set_comp_node_previews(self, *a, **k):
        pass

    def _apply_comp_printed_badge(self, *a):
        pass

    def _apply_comp_witness_cell(self, *a):
        pass

    def _update_comp_filter_indicators(self):
        pass

    def _apply_comp_tree_filters(self):
        pass


def _load_flat(host, main, filtered, known):
    host._comp_flat_markers = host._comp_flat_marker_map(filtered, known)
    items = main + filtered + known
    root = QTreeWidgetItem(host.comp_tree, [host._comp_flat_root_label(len(items), host._comp_flat_markers)])
    host._start_batched_tree_load(root, items)
    return root


def test_flat_view_marks_filtered_and_excluded_rows():
    host = _FlatHost()
    main = [_ms("990001", "T-S 1", "CUL", "Main")]
    filt = [_ms("990002", "T-S 2", "CUL", "Filt", filter_reason="source_text")]
    known = [_ms("990003", "T-S 3", "CUL", "Known")]
    root = _load_flat(host, main, filt, known)

    assert root.text(0) == "All Results (3) — 1 filtered, 1 excluded"
    rows = {root.child(i).text(host.comp_col_sysid): root.child(i) for i in range(root.childCount())}
    assert rows["990001"].text(0) == "10"
    assert rows["990001"].toolTip(0) == ""
    assert rows["990002"].text(0) == "⊘ 10"
    assert rows["990002"].toolTip(0) == "Filtered result: Found in source text"
    assert rows["990002"].toolTip(host.comp_col_shelfmark) == "Filtered result: Found in source text"
    assert rows["990002"].foreground(host.comp_col_title).color().name() == "#f39c12"
    assert rows["990003"].toolTip(0) == "Excluded from the results by you"
    assert rows["990003"].text(0) == "10"
    assert rows["990002"].foreground(host.comp_col_title).color() != rows["990001"].foreground(host.comp_col_title).color()


def test_filtered_row_without_a_reason_still_gets_marked():
    host = _FlatHost()
    filt = [_ms("990002", "T-S 2", "CUL", "Filt")]  # a filtered bucket item with no flags
    root = _load_flat(host, [], filt, [])
    assert root.child(0).toolTip(0) == "Filtered result"
    assert root.text(0) == "All Results (1) — 1 filtered"


def test_marker_needs_the_same_object_not_an_equal_copy():
    host = _FlatHost()
    filt = [_ms("990002", "T-S 2", "CUL", "Filt", filter_reason="source_text")]
    host._comp_flat_markers = host._comp_flat_marker_map(filt, [])
    root = QTreeWidgetItem(host.comp_tree, ["All"])
    import copy
    host._start_batched_tree_load(root, [copy.deepcopy(filt[0])])
    assert root.child(0).toolTip(0) == ""


def test_grouped_view_leaves_rows_unmarked():
    # display_comp_results resets the map on every render; the grouped view never
    # fills it, so its batched main rows are untouched.
    host = _FlatHost()
    host._comp_flat_markers = {}
    root = QTreeWidgetItem(host.comp_tree, ["Main"])
    host._start_batched_tree_load(root, [_ms("990001", "T-S 1", "CUL", "Main")])
    assert root.child(0).text(0) == "10"


def test_add_manuscript_node_returns_the_row_it_built():
    host = _FlatHost()
    root = QTreeWidgetItem(host.comp_tree, ["r"])
    single = host._add_manuscript_node(root, _ms("990001", "T-S 1", "CUL", "One"))
    multi = host._add_manuscript_node(root, _ms("990002", "T-S 2", "CUL", "Two", pages=3))
    part = host._add_manuscript_node(root, dict(_ms("990003", "T-S 3", "CUL", "P", pages=2),
                                                 type="part", part_id="P1"))
    fallback = host._add_manuscript_node(root, dict(_ms("990004", "T-S 4", "CUL", "F"), type=""))
    assert [root.indexOfChild(n) for n in (single, multi, part, fallback)] == [0, 1, 2, 3]


# --------------------------------------------------------------------------------------
# xlsx export -- the real export_comp_report, read back with openpyxl.
# --------------------------------------------------------------------------------------

class _Line:
    def __init__(self, text):
        self._t = text

    def text(self):
        return self._t

    def toPlainText(self):
        return self._t


class _Meta:
    nli_cache = {"990001": {}, "990002": {}}
    _rows = {"990001": ("T-S 12.34", "Title one", "CUL"),
             "990002": ("ENA 555.6", "Title two", "JTS")}

    def get_meta_for_id(self, sid):
        shelf, title, _ = self._rows[sid]
        return shelf, title

    def get_library_for_id(self, sid):
        return self._rows[sid][2]

    def parse_header_smart(self, raw):
        return (raw.split("_")[0], None)

    def get_shelfmark_from_header(self, raw):
        return ""


class _ExportHost:
    _EXPORT_ACTION_BY_FMT = GenizahGUI._EXPORT_ACTION_BY_FMT

    def __init__(self, main, filtered):
        self.meta_mgr = _Meta()
        self.comp_main = main
        self.comp_appendix = {}
        self.comp_filtered_main = filtered
        self.comp_filtered_appendix = {}
        self.comp_known = []
        self.comp_title_input = _Line("Test")
        self.comp_text_area = _Line("query text")
        self.chk_comp_flat = SimpleNamespace(isChecked=lambda: False)
        self._collect_comp_items = MethodType(GenizahGUI._collect_comp_items, self)
        self.export_comp_report = MethodType(GenizahGUI.export_comp_report, self)

    def _collect_checked_comp_page_uids(self):
        return set()

    def _fetch_metadata_with_dialog(self, *a, **k):
        pass

    def _default_report_path(self, title, fallback):
        return title + ".xlsx"

    def _emit_feature_opened(self, **k):
        pass

    def _get_credit_header(self, local_only=False):
        return ""

    def _comp_export_settings_lines(self):
        return []

    def _comp_witness_total(self):
        return 0

    def _get_meta_for_header(self, raw):
        return (raw.split("_")[0], raw.split("_")[1], "", "")

    def _sort_comp_items(self, items):
        return list(items)

    def _get_filter_reason(self, item):
        return ""

    def _save_last_folder(self, *a, **k):
        pass

    def _show_export_saved_dialog(self, *a, **k):
        pass


def test_xlsx_every_sheet_puts_each_value_under_its_own_header(tmp_path, monkeypatch):
    openpyxl = pytest.importorskip("openpyxl")
    out = tmp_path / "report.xlsx"
    monkeypatch.setattr(genizah_app.QFileDialog, "getSaveFileName",
                        staticmethod(lambda *a, **k: (str(out), "")))
    monkeypatch.setattr(genizah_app.QMessageBox, "information", staticmethod(lambda *a, **k: None))
    monkeypatch.setattr(genizah_app.QMessageBox, "warning", staticmethod(lambda *a, **k: None))
    monkeypatch.setattr(genizah_app.QMessageBox, "critical",
                        staticmethod(lambda *a, **k: pytest.fail(f"export failed: {a[-1]}")))

    host = _ExportHost(
        main=[{"type": "manuscript", "sys_id": "990001", "score": 9,
               "pages": [{"raw_header": "990001_1", "score": 9, "source_ctx": "a", "text": "b"}]}],
        filtered=[{"type": "manuscript", "sys_id": "990002", "score": 4,
                   "pages": [{"raw_header": "990002_1", "score": 4, "source_ctx": "c", "text": "d"}]}],
    )
    host.export_comp_report("xlsx")
    assert out.exists()

    wb = openpyxl.load_workbook(out)
    library_of = {"990001": genizah_app.get_library_display("CUL", short=False),
                  "990002": genizah_app.get_library_display("JTS", short=False)}
    shelf_of = {"990001": "T-S 12.34", "990002": "ENA 555.6"}
    checked = 0
    for ws in wb.worksheets:
        header_row = col = None
        for row in ws.iter_rows():
            vals = [c.value for c in row]
            if "Library" in vals and "Shelfmark" in vals and "System ID" in vals:
                header_row = vals
                col = {name: vals.index(name) for name in ("System ID", "Library", "Shelfmark")}
                continue
            if header_row is None:
                continue
            sid = vals[col["System ID"]] if len(vals) > col["System ID"] else None
            if sid in shelf_of:
                assert vals[col["Library"]] == library_of[sid], (ws.title, vals)
                assert vals[col["Shelfmark"]] == shelf_of[sid], (ws.title, vals)
                checked += 1
    # Two manuscripts on both the Raw Data and the Report View sheet.
    assert checked >= 4, checked

    # Report View heads each manuscript's rows with "<shelfmark> | <title>" (Codex, #361).
    report = next(ws for ws in wb.worksheets if ws.title == "Report View")
    first_cells = {str(r[0].value) for r in report.iter_rows() if r[0].value}
    assert "T-S 12.34 | Title one" in first_cells, sorted(first_cells)
    assert "ENA 555.6 | Title two" in first_cells, sorted(first_cells)
