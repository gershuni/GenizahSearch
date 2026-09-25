# -*- coding: utf-8 -*-
"""Exclude Manuscripts is PER SURFACE: Search tab and Composition tab.

Until 2026-09-03 there was ONE list. `btn_main_exclude` (Search) and
`btn_exclude` (Composition) opened the same dialog over the same
`excluded_sys_ids` / `excluded_shelfmarks` / `excluded_raw_entries` /
`exclusion_sources`, so:

  - excluding a manuscript on one surface silently excluded it on the other;
  - `_update_exclusion_display` wrote the SAME text into BOTH status labels,
    which is what hid the coupling;
  - `_reset_search` and `_reset_composition` each wiped the other's list.

The Search tab keeps the historical un-prefixed attribute names (the saved
session schema and existing readers depend on them); Composition gets the
parallel `comp_` set. Everything routes through `_excl_get` / `_excl_set`.

Qt-free where possible, in the style of
tests/test_result_dialog_highlight_persistence.py: the methods under test are
bound onto a lightweight stub and the labels are trivial fakes.
"""

import ast
import io
import os
import sys
from types import MethodType, SimpleNamespace

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt6.QtCore import QPoint
from PyQt6.QtWidgets import (QApplication, QComboBox, QLabel, QLineEdit,
                             QMainWindow, QProgressBar, QPushButton,
                             QTableWidget)

import genizah_app as app
from genizah_app import GenizahGUI
from shared.config import Config
from shared.exclusion_service import ExclusionSource
from shared.lists_manager import ListsManager

_APP = QApplication.instance() or QApplication([])

pytestmark = pytest.mark.gui  # imports genizah_app, which imports PyQt6

APP_PY = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "genizah_app.py")


def _method_source(name):
    tree = ast.parse(io.open(APP_PY, encoding="utf-8").read())
    cls = next(n for n in tree.body
               if isinstance(n, ast.ClassDef) and n.name == "GenizahGUI")
    fn = next(n for n in cls.body
              if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
              and n.name == name)
    lines = io.open(APP_PY, encoding="utf-8").read().splitlines()
    return "\n".join(lines[fn.lineno - 1:fn.end_lineno])


class _Label:
    def __init__(self):
        self.text = ""

    def setText(self, t):
        self.text = t


class _Stub:
    _EXCLUSION_ATTRS = GenizahGUI._EXCLUSION_ATTRS
    _EXCLUSION_LABELS = GenizahGUI._EXCLUSION_LABELS
    _excl_get = GenizahGUI._excl_get
    _excl_set = GenizahGUI._excl_set
    _clear_exclusions = GenizahGUI._clear_exclusions
    _update_exclusion_display = GenizahGUI._update_exclusion_display
    set_excluded_entries = GenizahGUI.set_excluded_entries
    _remove_exclusion_source = GenizahGUI._remove_exclusion_source
    _item_matches_exclusion = GenizahGUI._item_matches_exclusion
    _apply_manual_exclusions = GenizahGUI._apply_manual_exclusions

    def __init__(self):
        self.excluded_raw_entries = []
        self.excluded_sys_ids = set()
        self.excluded_shelfmarks = set()
        self.exclusion_sources = []
        self.comp_excluded_raw_entries = []
        self.comp_excluded_sys_ids = set()
        self.comp_excluded_shelfmarks = set()
        self.comp_exclusion_sources = []
        self.lbl_main_exclude_status = _Label()
        self.lbl_exclude_status = _Label()
        self.saves = 0
        self.rerenders = 0
        self.meta_mgr = self

    # --- collaborators -----------------------------------------------------
    def _schedule_session_save(self):
        self.saves += 1

    def _rerender_with_exclusions(self):
        self.rerenders += 1

    def _normalize_shelfmark(self, s):
        return (s or "").strip().upper()

    def parse_header_smart(self, header):
        return (header or "").strip(), None

    def fetch_nli_data(self, sys_id):
        return None

    def _get_meta_for_header(self, header):
        return None, None, "", None

    @property
    def nli_cache(self):
        return {"99001": {}, "99002": {}, "99003": {}}


def _src(label, sys_ids):
    return ExclusionSource(label=label, source_type="file", source_id=label,
                           sys_ids=set(sys_ids), unresolved=[])


# --------------------------------------------------------------------------
# The two lists are independent
# --------------------------------------------------------------------------

def test_the_two_surfaces_have_distinct_attributes():
    s = _Stub._EXCLUSION_ATTRS["search"]
    c = _Stub._EXCLUSION_ATTRS["composition"]
    assert set(s) == set(c) == {"sources", "sys_ids", "shelfmarks", "raw"}
    assert not (set(s.values()) & set(c.values())), (
        "a shared attribute name would re-couple the two surfaces"
    )


def test_writing_one_surface_leaves_the_other_untouched():
    w = _Stub()
    w._excl_set("search", "sys_ids", {"99001"})
    w._excl_set("search", "raw", ["99001"])
    assert w._excl_get("composition", "sys_ids") == set()
    assert w._excl_get("composition", "raw") == []

    w._excl_set("composition", "sys_ids", {"99002"})
    assert w._excl_get("search", "sys_ids") == {"99001"}, (
        "the composition write must not reach the search list"
    )


def test_clearing_one_surface_leaves_the_other_intact():
    """The reported symptom: New on either tab wiped both lists."""
    w = _Stub()
    w._excl_set("search", "sys_ids", {"99001"})
    w._excl_set("search", "sources", [_src("a", {"99001"})])
    w._excl_set("composition", "sys_ids", {"99002"})
    w._excl_set("composition", "sources", [_src("b", {"99002"})])

    w._clear_exclusions("search")

    assert w._excl_get("search", "sys_ids") == set()
    assert w._excl_get("search", "sources") == []
    assert w._excl_get("composition", "sys_ids") == {"99002"}
    assert len(w._excl_get("composition", "sources")) == 1


# --------------------------------------------------------------------------
# Each surface labels only its own control
# --------------------------------------------------------------------------

def test_each_surface_writes_only_its_own_status_label():
    w = _Stub()
    w._excl_set("search", "sources", [_src("my list", {"99001", "99002"})])
    w._update_exclusion_display("search")
    assert "2" in w.lbl_main_exclude_status.text
    assert w.lbl_exclude_status.text == "", (
        "the Search tab must not relabel the Composition tab"
    )

    w2 = _Stub()
    w2._excl_set("composition", "sources", [_src("other", {"99009"})])
    w2._update_exclusion_display("composition")
    assert "1" in w2.lbl_exclude_status.text
    assert w2.lbl_main_exclude_status.text == ""


def test_an_empty_surface_clears_only_its_own_label():
    w = _Stub()
    w.lbl_main_exclude_status.setText("stale")
    w.lbl_exclude_status.setText("keep me")
    w._update_exclusion_display("search")
    assert w.lbl_main_exclude_status.text == ""
    assert w.lbl_exclude_status.text == "keep me"


# --------------------------------------------------------------------------
# set_excluded_entries
# --------------------------------------------------------------------------

def test_set_excluded_entries_defaults_to_composition():
    """Its only caller excludes a result and switches to the Composition tab."""
    w = _Stub()
    w.set_excluded_entries("99001\n99002")
    assert w._excl_get("composition", "sys_ids") == {"99001", "99002"}
    assert w._excl_get("search", "sys_ids") == set()
    assert "2" in w.lbl_exclude_status.text
    assert w.lbl_main_exclude_status.text == ""


def test_set_excluded_entries_can_target_search():
    w = _Stub()
    w.set_excluded_entries("99001", "search")
    assert w._excl_get("search", "raw") == ["99001"]
    assert w._excl_get("composition", "raw") == []


def test_set_excluded_entries_splits_ids_from_shelfmarks():
    w = _Stub()
    w.set_excluded_entries("99001\nT-S 12.123")
    assert w._excl_get("composition", "sys_ids") == {"99001"}
    assert w._excl_get("composition", "shelfmarks") == {"T-S 12.123".upper()}


# --------------------------------------------------------------------------
# The composition render path reads the COMPOSITION list
# --------------------------------------------------------------------------

def test_composition_filtering_ignores_the_search_exclusion_list():
    """A manuscript excluded only on the Search tab must still appear here."""
    w = _Stub()
    w._excl_set("search", "sys_ids", {"99001"})
    main = [{"raw_header": "99001"}, {"raw_header": "99002"}]

    kept, _appx, known = w._apply_manual_exclusions(main, {})

    assert [i["raw_header"] for i in kept] == ["99001", "99002"]
    assert known == []


def test_composition_filtering_honours_the_composition_list():
    w = _Stub()
    w._excl_set("composition", "sys_ids", {"99001"})
    main = [{"raw_header": "99001"}, {"raw_header": "99002"}]

    kept, _appx, known = w._apply_manual_exclusions(main, {})

    assert [i["raw_header"] for i in kept] == ["99002"]
    assert [i["raw_header"] for i in known] == ["99001"]


def test_item_matches_exclusion_reads_the_composition_list():
    w = _Stub()
    w._excl_set("composition", "sys_ids", {"99003"})
    assert w._item_matches_exclusion({"raw_header": "99003"}) is True
    assert w._item_matches_exclusion({"raw_header": "99002"}) is False


def test_part_items_check_their_folios_against_the_composition_list():
    w = _Stub()
    w._excl_set("composition", "sys_ids", {"99002"})
    part = {"type": "part", "sys_id": "99001", "folios": ["99002"],
            "raw_header": "99001"}
    assert w._item_matches_exclusion(part) is True


# --------------------------------------------------------------------------
# Per-source removal
# --------------------------------------------------------------------------

def test_removing_a_source_touches_one_surface_only():
    w = _Stub()
    w._excl_set("search", "sources", [_src("keep", {"99001"}), _src("drop", {"99002"})])
    w._excl_set("composition", "sources", [_src("drop", {"99003"})])

    w._remove_exclusion_source("drop", "search")

    assert [s.source_id for s in w._excl_get("search", "sources")] == ["keep"]
    assert [s.source_id for s in w._excl_get("composition", "sources")] == ["drop"]
    assert w.rerenders == 1, "the Search tab re-renders its results table"


def test_removing_a_composition_source_does_not_rerender_the_search_table():
    w = _Stub()
    w._excl_set("composition", "sources", [_src("drop", {"99003"})])
    w._remove_exclusion_source("drop", "composition")
    assert w._excl_get("composition", "sources") == []
    assert w.rerenders == 0


# --------------------------------------------------------------------------
# Wiring the source can't drift back to a shared list
# --------------------------------------------------------------------------

def test_each_button_binds_its_own_surface():
    src = io.open(APP_PY, encoding="utf-8").read()
    assert "lambda: self.open_exclude_dialog('search')" in src
    assert "lambda: self.open_exclude_dialog('composition')" in src
    assert "clicked.connect(self.open_exclude_dialog)" not in src, (
        "a bare connect passes Qt's bool as the surface name"
    )


def test_the_dialog_reads_and_writes_one_surface():
    s = _method_source("open_exclude_dialog")
    assert "self.excluded_sys_ids" not in s
    assert "self.exclusion_sources" not in s
    assert "self.excluded_raw_entries" not in s
    assert "_excl_get(surface" in s and "_excl_set(surface" in s


def test_the_lab_composition_path_uses_the_composition_list():
    s = _method_source("run_composition")
    assert "self._excl_get('composition', 'raw')" in s
    assert "self._excl_get('composition', 'sys_ids')" in s
    assert "self.excluded_sys_ids" not in s
    assert "self.excluded_raw_entries" not in s


def test_neither_reset_clears_the_other_surface():
    for name, own, other in (("_reset_search", "search", "composition"),
                             ("_reset_composition", "composition", "search")):
        s = _method_source(name)
        assert f"_clear_exclusions('{own}')" in s, name
        assert f"_clear_exclusions('{other}')" not in s, name
        for attr in ("self.excluded_sys_ids = set()",
                     "self.excluded_raw_entries = []",
                     "self.exclusion_sources = []"):
            assert attr not in s, f"{name} still clears the shared attribute"


# --------------------------------------------------------------------------
# Session persistence
# --------------------------------------------------------------------------

def test_the_composition_snapshot_stores_the_composition_list():
    src = io.open(APP_PY, encoding="utf-8").read()
    assert "'excluded_sys_ids': sorted(getattr(self, 'comp_excluded_sys_ids', set()))," in src
    assert "'exclusion_sources': serialize_sources(getattr(self, 'comp_exclusion_sources', []))," in src


def test_the_regular_snapshot_still_stores_the_search_list():
    src = io.open(APP_PY, encoding="utf-8").read()
    assert "'excluded_sys_ids': sorted(getattr(self, 'excluded_sys_ids', set()))," in src
    assert "'exclusion_sources': serialize_sources(getattr(self, 'exclusion_sources', []))," in src


def test_an_old_single_list_session_migrates_into_both_surfaces():
    """Sessions written before the split carry no composition exclusion keys."""
    s = _method_source("_restore_session")
    assert "self._comp_snapshot_has_own_exclusions(comp)" in s
    assert "self.comp_exclusion_sources = list(" in s
    assert "getattr(self, 'exclusion_sources', []) or [])" in s, (
        "the fallback must copy the restored SEARCH list into composition"
    )


# The legacy/post-split decision, exercised directly. Codex P1 on PR #334: the
# first version tested the VALUES, so a composition list the user had
# deliberately emptied ([] in a post-split snapshot) was read as legacy and the
# Search list was copied back over it -- re-coupling the surfaces the split had
# just separated, silently, on the next restart.

def test_a_post_split_snapshot_with_an_empty_list_is_not_treated_as_legacy():
    empty_but_deliberate = {
        'excluded_sys_ids': [],
        'excluded_shelfmarks': [],
        'excluded_raw_entries': [],
        'exclusion_sources': [],
    }
    assert GenizahGUI._comp_snapshot_has_own_exclusions(empty_but_deliberate) is True, (
        "an intentionally empty composition list must survive a restart; "
        "migrating the Search list over it re-couples the two surfaces"
    )


def test_a_pre_split_snapshot_is_treated_as_legacy():
    """Before the split, composition_search stored only these two keys."""
    legacy = {'excluded_sys_ids': ['s1'], 'excluded_shelfmarks': ['T-S 1.1']}
    assert GenizahGUI._comp_snapshot_has_own_exclusions(legacy) is False


@pytest.mark.parametrize("comp", [
    {'exclusion_sources': []},
    {'excluded_raw_entries': []},
    {'exclusion_sources': [], 'excluded_raw_entries': []},
    {'exclusion_sources': [{'id': 'x'}]},
])
def test_either_key_alone_marks_a_post_split_snapshot(comp):
    assert GenizahGUI._comp_snapshot_has_own_exclusions(comp) is True


def test_an_empty_snapshot_is_legacy():
    assert GenizahGUI._comp_snapshot_has_own_exclusions({}) is False


def test_the_composition_restore_runs_after_the_search_restore():
    """The migration reads the search list, so ordering is load-bearing."""
    s = _method_source("_restore_session")
    reg = s.index("self.exclusion_sources = deserialize_sources(reg['exclusion_sources'])")
    comp = s.index("self.comp_exclusion_sources = list(")
    assert reg < comp


# ---------------------------------------------------------------------------
# The status label when there are exclusions but no SOURCE (Codex P2, PR #334)
# ---------------------------------------------------------------------------
#
# set_excluded_entries -- the results-table "exclude this and work on it in
# Composition" action -- fills raw/sys_ids and builds no ExclusionSource. A
# restore then called _update_exclusion_display, which blanked the label while
# the manuscripts stayed excluded: results silently filtered, nothing on screen
# saying so.

class _LabelHarness:
    _EXCLUSION_ATTRS = GenizahGUI._EXCLUSION_ATTRS
    _EXCLUSION_LABELS = GenizahGUI._EXCLUSION_LABELS

    def __init__(self):
        self.lbl_main_exclude_status = QLabel()
        self.lbl_exclude_status = QLabel()
        for surface in ("search", "composition"):
            for field, attr in self._EXCLUSION_ATTRS[surface].items():
                setattr(self, attr, [] if field in ("sources", "raw") else set())
        for name in ("_excl_get", "_excl_set", "_update_exclusion_display"):
            setattr(self, name, MethodType(getattr(GenizahGUI, name), self))


@pytest.fixture
def labels():
    return _LabelHarness()


@pytest.mark.parametrize("surface,lbl_attr", [
    ("search", "lbl_main_exclude_status"),
    ("composition", "lbl_exclude_status"),
])
def test_raw_entries_with_no_source_still_report_a_count(labels, surface, lbl_attr):
    labels._excl_set(surface, 'raw', ['T-S 1.1', 'T-S 1.2'])
    labels._update_exclusion_display(surface)
    assert labels._excl_get(surface, 'sources') == []
    assert "2" in getattr(labels, lbl_attr).text(), (
        "manuscripts are excluded; a blank label says they are not"
    )


@pytest.mark.parametrize("surface,lbl_attr", [
    ("search", "lbl_main_exclude_status"),
    ("composition", "lbl_exclude_status"),
])
def test_no_sources_and_no_raw_entries_clears_the_label(labels, surface, lbl_attr):
    getattr(labels, lbl_attr).setText("stale")
    labels._update_exclusion_display(surface)
    assert getattr(labels, lbl_attr).text() == ""


def test_the_label_fallback_still_writes_only_its_own_surface(labels):
    labels._excl_set('composition', 'raw', ['T-S 1.1'])
    labels._update_exclusion_display('composition')
    assert labels.lbl_main_exclude_status.text() == "", (
        "the raw fallback must not re-couple the two labels"
    )


def test_the_search_restore_no_longer_hand_rolls_the_fallback():
    """One fallback, in the shared helper, or the surfaces drift again."""
    s = _method_source("_restore_session")
    assert "self.lbl_main_exclude_status.setText(" not in s, (
        "the restore must go through _update_exclusion_display"
    )
    assert "self._update_exclusion_display('search')" in s
    assert "self._update_exclusion_display('composition')" in s


# ===========================================================================
# The Search results table, driven for real
# ===========================================================================
#
# A real GenizahGUI (__new__ + QMainWindow.__init__, no app startup) with a
# real QTableWidget, driven through the entry points the UI uses. Everything
# with side effects elsewhere (disk, threads, network) is stubbed, and the
# personal-state files point into tmp_path.

A, B, C = "990000000000100001", "990000000000200002", "990000000000300003"


@pytest.fixture(autouse=True)
def _personal_state_in_tmp(tmp_path, monkeypatch):
    """No test in this file may reach the developer's own session, config,
    language or lists file."""
    monkeypatch.setattr(Config, "INDEX_DIR", str(tmp_path))
    monkeypatch.setattr(Config, "SESSION_FILE", str(tmp_path / "session.json"))
    monkeypatch.setattr(Config, "CONFIG_FILE", str(tmp_path / "config.pkl"))
    monkeypatch.setattr(Config, "LANGUAGE_FILE", str(tmp_path / "lang.pkl"))
    monkeypatch.setattr(ListsManager, "LISTS_FILE", str(tmp_path / "lists.pkl"))


@pytest.fixture
def slot_errors(monkeypatch):
    """An exception escaping a Qt slot (a menu action's lambda) aborts the
    process unless sys.excepthook is ours. Record it, then fail the test."""
    errors = []
    monkeypatch.setattr(sys, "excepthook", lambda et, ev, tb: errors.append(ev))
    yield errors
    assert not errors, f"exception inside a Qt slot: {errors!r}"


def _res(sid, page):
    # The shape a real Genizah hit has: NO top-level sys_id -- the id lives in
    # display['id'] and in the header.
    return {
        "display": {"id": sid, "source": "V0.8", "img": str(page),
                    "shelfmark": "", "title": ""},
        "snippet": f"*word* {sid[-2:]}/{page}",
        "full_text": "text",
        "uid": f"{sid}_{page}",
        "raw_header": f"{sid}_P{page}",
        "raw_file_hl": "",
        "highlight_pattern": "word",
        "scope": "genizah",
        "score": 1.0,
    }


class _Meta:
    def __init__(self):
        self.nli_cache = {}
        self.csv_bank = {A: {}, B: {}, C: {}}

    def parse_full_id_components(self, raw_header):
        return {"sys_id": (raw_header or "").split("_")[0], "ie_id": None,
                "p_num": "1", "fl_id": None}

    def get_meta_for_id(self, sid):
        return (f"T-S {sid[-3:]}", f"title {sid[-3:]}")

    def get_library_for_id(self, sid):
        return "CUL"


class _Header:
    def set_filter_active(self, col, on):
        pass

    def blockSignals(self, b):
        return False

    def setChecked(self, b):
        pass


@pytest.fixture
def window(slot_errors, monkeypatch):
    monkeypatch.setattr(app, "_resolve_display_title",
                        lambda sid, title, **k: title or "")
    w = GenizahGUI.__new__(GenizahGUI)
    QMainWindow.__init__(w)
    cols = dict(COL_CHECKBOX=0, COL_ACTIONS=1, COL_SYS_ID=2, COL_LIBRARY=3,
                COL_SHELF=4, COL_IMG=5, COL_TITLE=6, COL_SNIPPET=7, COL_SRC=8,
                COL_PGP=9, COL_DOMAIN=10, COL_PRINTED=11, COL_TRANSCRIPTION=12)
    for k, v in cols.items():
        setattr(w, k, v)
    w.results_table = QTableWidget()
    w.results_table.setColumnCount(13)
    w.status_label = QLabel()
    w.lbl_search_export = QLabel()
    w.lbl_main_exclude_status = QLabel()
    w.lbl_domain_filter = QLabel()
    w.btn_domain_filter = QPushButton()
    w.btn_search = QPushButton()
    w.search_progress = QProgressBar()
    w.query_input = QLineEdit()
    w.mode_combo = QComboBox()
    w.chk_search_header = _Header()
    w.meta_mgr = _Meta()
    w.export_buttons = []
    # results state
    w.last_results = []
    w.results_loaded = 0
    w.shelfmark_items_by_sid = {}
    w.title_items_by_sid = {}
    w.result_row_by_sys_id = {}
    w._res_map_by_sid = {}
    w.refinement_chain = []
    w.refinement_restrict_sys_ids = None
    w._refine_mode = False
    w._all_terms_filter = False
    w._restoring_session = False
    w.is_searching = False
    w._responsa_expanded_count = 0
    w._search_was_cancelled = False
    w.last_search_query = "word"
    w.hovered_row = -1
    w.search_thread = None
    w._pause_search = SimpleNamespace(state="idle", elapsed=lambda t: 1.0)
    w.pre_search_filters = {}
    w.pre_search_restrict_sys_ids = None
    # filter state
    w.list_filter_state = {"active": False, "mode": "in", "lists": "all"}
    w._domain_exclusions = set()
    w._has_result_domains = False
    w._result_domain_map = {}
    w._result_domain_counts = {}
    w._domain_name_map = {}
    w._post_measurement_filters = {}
    w._local_filter_state_search = "all"
    w._local_filter_inactive_chip_visible = False
    w._local_file_optouts = set()
    w.results_filters = {}
    w._printed_filter_state = "all"
    w._printed_sys_ids = set()
    w._manual_transcription_sys_ids = set()
    w._pgp_pages_by_sys_id = {}
    w._result_measurement_map = {}
    w._measurement_fetch_complete = True
    # exclusions: the right-click set and the Search tab's list
    w.word_excluded_sys_ids = set()
    w.excluded_sys_ids = set()
    w.excluded_shelfmarks = set()
    w.excluded_raw_entries = []
    w.exclusion_sources = []
    # collaborators with side effects elsewhere (disk, threads, network)
    w._save_session = lambda: None
    w._schedule_session_save = lambda: None
    w._write_pgp_badge_cell = lambda row, sid: None
    w._update_search_row_list_indicator = lambda row, res=None: None
    w._lookup_local_filepath = lambda sid: None
    w._prime_local_filepath_cache = lambda results: None
    w._update_local_filter_visibility_search = lambda: None
    w._set_local_scope_strip_visible = lambda v: None
    w._add_regular_search_to_history = lambda: None
    w._notify_search_complete = lambda n, q: None
    w._update_search_within_btn = lambda: None
    w._update_refinement_strip = lambda: None
    w._update_filter_chip_bar = lambda: None
    w._emit_search_telemetry = lambda *a, **k: None
    w._emit_feature_opened = lambda **k: None
    w._apply_pause_state = lambda *a: None
    # Production's enrichment start re-runs the visibility pass
    # (_launch_enrichment_workers._start -> _apply_results_table_filters).
    w._launch_enrichment_workers = (
        lambda results, defer=False: w._apply_results_table_filters())
    return w


def _row_of(w, sid):
    return [r for r in range(w.results_table.rowCount())
            if w.results_table.item(r, w.COL_SYS_ID).text() == sid]


def _hidden(w, sid):
    rows = _row_of(w, sid)
    assert rows, f"{sid} has no row"
    return [w.results_table.isRowHidden(r) for r in rows]


def _search(w, results):
    """A search landing: what start_search's worker delivers."""
    w.on_search_finished(list(results))


@pytest.fixture
def menus(monkeypatch):
    captured = []
    monkeypatch.setattr(app.QMenu, "exec",
                        lambda self, *a, **k: captured.append(self))
    return captured


def _menu_action(w, menus, row, text):
    y = w.results_table.rowViewportPosition(row) + 2
    assert w.results_table.rowAt(y) == row
    w._show_results_context_menu(QPoint(5, y))
    [act] = [a for a in menus[-1].actions() if a.text() == text]
    return act


# --------------------------------------------------------------------------
# The results menu acts on the manuscript that was clicked
# --------------------------------------------------------------------------

_SHELF_B = f"T-S {B[-3:]}"


@pytest.mark.parametrize("label,method,expected", [
    ("View Document", "_context_view_document", (B,)),
    ("Submit Correction...", "_context_submit_correction", (B, _SHELF_B)),
    ("Add Comment...", "_context_add_comment", (B, _SHELF_B)),
    ("View Corrections...", "_context_view_corrections", (B, _SHELF_B)),
    ("View Comments...", "_context_view_comments", (B, _SHELF_B)),
    ("Share Discovery...", "_context_share_discovery", (B, _SHELF_B)),
    ("Exclude this manuscript", "_exclude_word_search_result", (B, 1)),
])
def test_every_results_menu_action_receives_the_clicked_manuscript(
        window, menus, label, method, expected):
    """A Genizah hit has no top-level sys_id; the menu read only that key, so
    all seven actions received '' (a comment or correction was filed against
    document ''; the exclusion was a silent no-op)."""
    w = window
    w.last_results = [_res(A, 1), _res(B, 1)]
    w.load_next_batch()
    calls = []
    setattr(w, method, lambda *a: calls.append(a))
    _menu_action(w, menus, 1, app.tr(label)).trigger()
    assert calls == [expected], (
        f"{label!r} received {calls!r} for the row of {B}")
