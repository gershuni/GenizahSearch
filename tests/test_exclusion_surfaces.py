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

from PyQt6.QtCore import QObject, QPoint
from PyQt6.QtWidgets import (QApplication, QComboBox, QLabel, QLineEdit,
                             QMainWindow, QProgressBar, QPushButton,
                             QTableWidget)

import genizah_app as app
import shared.session_persistence as session_persistence
from genizah_app import GenizahGUI
from shared.config import Config
from shared.exclusion_service import ExclusionSource, serialize_sources
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
    """The migration reads the search list, so ordering is load-bearing. The
    search list is restored with the persistent preferences, which run first."""
    prefs = _method_source("_apply_persistent_session_preferences")
    assert "self.exclusion_sources = deserialize_sources(reg['exclusion_sources'])" in prefs
    s = _method_source("_restore_session")
    reg = s.index("self._apply_persistent_session_preferences(state)")
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
    """One fallback, in the shared helper, or the surfaces drift again. The
    Search label is restored with the Search list, on the persistent path."""
    s = _method_source("_restore_session")
    prefs = _method_source("_apply_persistent_session_preferences")
    assert "self.lbl_main_exclude_status.setText(" not in s + prefs, (
        "the restore must go through _update_exclusion_display"
    )
    assert "self._update_exclusion_display('search')" in prefs
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
    # Derived from INDEX_DIR once, at import.
    monkeypatch.setattr(session_persistence, "HISTORY_FILE",
                        str(tmp_path / "search_history.json"))


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
        self.csv_bank = {sid: {"shelfmark": f"T-S {sid[-3:]}"} for sid in (A, B, C)}

    def parse_full_id_components(self, raw_header):
        return {"sys_id": (raw_header or "").split("_")[0], "ie_id": None,
                "p_num": "1", "fl_id": None}

    def parse_header_smart(self, raw_header):
        return (raw_header or "").split("_")[0], "1"

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
    w.searcher = None
    w.meta_loader = None
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
    w._emit_pgp_tag_search_telemetry = lambda *a, **k: None
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


# --------------------------------------------------------------------------
# An exclusion hides every row of the manuscript, and nothing brings it back
# --------------------------------------------------------------------------

tr = app.tr
EXCLUDED = tr("excluded")


def _showing(visible, total, excluded=0):
    line = tr("Showing {} of {} results").format(visible, total)
    return line + (f" ({excluded} {EXCLUDED})" if excluded else "")


def test_right_click_exclude_hides_every_row_of_the_manuscript(window, menus):
    w = window
    _search(w, [_res(A, 1), _res(B, 1), _res(A, 2)])
    _menu_action(w, menus, _row_of(w, A)[0], tr("Exclude this manuscript")).trigger()
    assert w.word_excluded_sys_ids == {A}
    assert _hidden(w, A) == [True, True], "'Exclude this manuscript' left a row of it visible"
    assert _hidden(w, B) == [False]


def test_the_printed_cycle_does_not_bring_an_excluded_row_back(window):
    w = window
    _search(w, [_res(A, 1), _res(B, 1)])
    w._exclude_word_search_result(A, _row_of(w, A)[0])
    for state in ("hide_printed", "only_printed", "all"):
        w._open_results_filter_dialog(w.COL_PRINTED)   # the header click
        assert w._printed_filter_state == state
        assert _hidden(w, A) == [True], f"excluded row came back at Printed={state}"


def test_the_next_batch_load_does_not_bring_an_excluded_row_back(window):
    w = window
    w.last_results = [_res(A, 1), _res(B, 1), _res(A, 2), _res(C, 1)]
    w.load_next_batch(batch_size=2)
    w._exclude_word_search_result(A, _row_of(w, A)[0])
    w.load_next_batch(batch_size=2)                     # what scrolling does
    assert w.results_table.rowCount() == 4
    assert _hidden(w, A) == [True, True], (
        "the scroll load showed the excluded row again, or the manuscript's later row")
    assert w.status_label.text() == _showing(2, 4, excluded=2)


def test_an_exclusion_holds_across_searches_until_new(window):
    w = window
    _search(w, [_res(A, 1), _res(B, 1)])
    w._exclude_word_search_result(A, _row_of(w, A)[0])

    _search(w, [_res(C, 1), _res(A, 5), _res(B, 2)])   # a later search (owner ruling)
    assert _hidden(w, A) == [True], "a later search showed a manuscript excluded in an earlier one"
    assert w.status_label.text() == _showing(2, 3, excluded=1), (
        "the later search hides a row and its status line does not say so")

    w._reset_search()                                   # New
    assert w.word_excluded_sys_ids == set()
    _search(w, [_res(A, 1)])
    assert _hidden(w, A) == [False], "New must end the exclusion"
    assert w.status_label.text() == _showing(1, 1)


def test_the_status_counts_visible_rows_with_a_filter_and_an_exclusion(window):
    w = window
    w._printed_sys_ids = {C}
    w._printed_filter_state = "hide_printed"
    w.excluded_sys_ids = {A}
    _search(w, [_res(A, 1), _res(B, 1), _res(C, 1)])
    assert _hidden(w, A) == [True] and _hidden(w, C) == [True]
    assert w.status_label.text() == _showing(1, 3, excluded=1), (
        "the first number must be the rows the table shows")


# --------------------------------------------------------------------------
# The Search tab's Exclude Manuscripts list
# --------------------------------------------------------------------------

def test_editing_the_exclude_list_keeps_filtered_rows_hidden(window):
    w = window
    _search(w, [_res(A, 1), _res(B, 1), _res(C, 1)])
    w._printed_sys_ids = {C}
    w._open_results_filter_dialog(w.COL_PRINTED)        # hide_printed: C hidden
    assert _hidden(w, C) == [True]
    w.excluded_sys_ids = {A}
    w._rerender_with_exclusions()                       # the dialog's OK
    assert _hidden(w, A) == [True]
    assert _hidden(w, C) == [True], "the exclude-list re-render showed a Printed-filtered row again"


def test_editing_the_exclude_list_rewrites_the_status_when_no_row_changes(window):
    """The dialog's OK always writes the summary. An id that is not in the
    table changes no row, so the visibility pass writes nothing, and the line
    would stay whatever wrote last."""
    w = window
    _search(w, [_res(A, 1), _res(B, 1)])
    w.status_label.setText(tr("Loaded {} items.").format(2))   # metadata finished last
    w.excluded_sys_ids = {C}                                    # not in this table
    w._rerender_with_exclusions()
    assert w.status_label.text() == _showing(2, 2)


def test_the_exclude_list_applies_to_the_next_search(window):
    w = window
    w.excluded_sys_ids = {B}
    _search(w, [_res(A, 1), _res(B, 1)])
    assert _hidden(w, B) == [True], "the Search tab's Exclude list did not apply to a new search"


def test_clearing_the_last_filter_rewrites_the_status(window):
    """The pass wrote its line only while a filter was active, so the line
    that counted the rows the last filter hid stayed after it was cleared."""
    w = window
    w._printed_sys_ids = {C}
    _search(w, [_res(A, 1), _res(B, 1), _res(C, 1)])
    for state, line in (("hide_printed", _showing(2, 3)), ("only_printed", _showing(1, 3)),
                        ("all", _showing(3, 3))):
        w._open_results_filter_dialog(w.COL_PRINTED)
        assert w._printed_filter_state == state
        assert w.status_label.text() == line, f"stale status at Printed={state}"


def test_removing_the_last_exclusion_rewrites_the_status(window):
    w = window
    _search(w, [_res(A, 1), _res(B, 1)])
    w.exclusion_sources = [_src("lst", {A})]
    w.excluded_sys_ids = {A}
    w._rerender_with_exclusions()                       # the dialog's OK
    assert w.status_label.text() == _showing(1, 2, excluded=1)
    w._remove_exclusion_source("lst", "search")         # and its per-source remove
    assert _hidden(w, A) == [False]
    assert w.status_label.text() == _showing(2, 2), (
        "the status still counts an exclusion that no longer exists")


# --------------------------------------------------------------------------
# The other passes and status writers
# --------------------------------------------------------------------------

def test_domain_enrichment_does_not_bring_an_excluded_row_back(window):
    w = window
    w._domain_exclusions = {"Liturgy"}                  # remembered across searches
    _search(w, [_res(A, 1), _res(B, 1)])
    w._exclude_word_search_result(A, _row_of(w, A)[0])
    w._on_domain_enrichment_loaded({A: [{"domain": "Bible"}], B: [{"domain": "Liturgy"}]})
    assert _hidden(w, B) == [True]
    assert _hidden(w, A) == [True], "domain enrichment showed the excluded manuscript again"


def test_clearing_the_domain_filter_keeps_exclusions_and_says_so(window):
    w = window
    _search(w, [_res(A, 1), _res(B, 1), _res(C, 1)])
    w._exclude_word_search_result(A, _row_of(w, A)[0])
    w._has_result_domains = True
    w._domain_exclusions = set()                        # the Domain dialog, cleared
    w._apply_domain_exclusions()
    assert _hidden(w, A) == [True]
    assert w.status_label.text() == _showing(2, 3, excluded=1)


def test_the_domain_filter_line_counts_visible_rows_and_the_excluded(window):
    w = window
    _search(w, [_res(A, 1), _res(B, 1), _res(C, 1)])
    w._exclude_word_search_result(A, _row_of(w, A)[0])
    w._has_result_domains = True
    w._result_domain_map = {B: ["Liturgy"], C: ["Bible"]}
    w._domain_exclusions = {"Liturgy"}                  # the Domain dialog's OK
    w._apply_domain_exclusions()
    assert _hidden(w, A) == [True] and _hidden(w, B) == [True]
    assert w.status_label.text() == (
        tr("Showing {} of {} results (filtering {} domains)").format(1, 3, 1)
        + f" (1 {EXCLUDED})")


def test_a_responsa_search_line_keeps_its_expanded_term_count(window):
    w = window
    w.word_excluded_sys_ids = {A}
    first = dict(_res(A, 1), responsa_expanded_count=4)
    _search(w, [first, _res(B, 1)])
    assert w.status_label.text() == (
        tr("Showing {} of {} results (searching {} expanded terms)").format(1, 2, 4)
        + f" (1 {EXCLUDED})")


def test_a_stopped_search_line_says_its_results_are_partial(window):
    w = window
    w.word_excluded_sys_ids = {A}
    w._search_was_cancelled = True                      # Stop, with results delivered
    _search(w, [_res(A, 1), _res(B, 1)])
    assert w.status_label.text() == (
        _showing(1, 2, excluded=1) + f" ({tr('Partial results')})")


def test_a_tag_search_says_that_exclusions_hide_rows(window, monkeypatch):
    import shared.document_service as ds
    import shared.transcription_service as ts
    monkeypatch.setattr(ts, "get_sys_ids_with_manual_transcriptions", lambda ids: set())
    monkeypatch.setattr(ds, "get_pgp_urls_for_pgpids", lambda ids: {})
    w = window
    w.word_excluded_sys_ids = {A}                       # standing, from an earlier search
    w._on_tag_search_results("letters", [{"sys_id": A, "pgpid": 1},
                                         {"sys_id": B, "pgpid": 2}])
    assert _hidden(w, A) == [True]
    assert w.status_label.text() == (
        tr("Tag: {} - {} results").format("letters", 2) + " " + _showing(1, 2, excluded=1))


def test_the_all_terms_rerender_keeps_the_excluded_note(window, monkeypatch):
    w = window
    _search(w, [_res(A, 1), _res(B, 1), _res(C, 1)])
    w._exclude_word_search_result(A, _row_of(w, A)[0])
    monkeypatch.setattr(app, "compute_all_terms_filter",
                        lambda chain: {f"{A}_1", f"{B}_1"})
    monkeypatch.setattr(app, "enrich_snippet_with_chain_terms", lambda s, c, q: s)
    w.refinement_chain = [object(), object()]
    w._all_terms_filter = True
    w._apply_all_terms_filter_and_rerender()
    assert _hidden(w, A) == [True]
    assert w.status_label.text() == (
        _showing(1, 3, excluded=1) + f" ({tr('Only results with all terms')})")


def test_the_responsa_warning_timer_writes_an_honest_line(window, monkeypatch):
    timers = []
    monkeypatch.setattr(app.QTimer, "singleShot",
                        staticmethod(lambda ms, fn: timers.append((ms, fn))))
    w = window
    w.word_excluded_sys_ids = {A}
    first = dict(_res(A, 1), responsa_warning="too many terms")
    _search(w, [first, _res(B, 1)])
    [restore_status] = [fn for ms, fn in timers if ms == 5000]
    restore_status()
    assert w.status_label.text() == _showing(1, 2, excluded=1)


@pytest.mark.parametrize("finish", ["loaded", "cancelled", "already loaded"])
def test_metadata_completion_keeps_the_summary_when_rows_are_hidden(window, finish):
    w = window
    _search(w, [_res(A, 1), _res(B, 1)])
    w._exclude_word_search_result(A, _row_of(w, A)[0])
    if finish == "already loaded":
        w.meta_mgr.nli_cache = {A: {}, B: {}}
        w.start_metadata_loading([A, B])
        message = tr("Metadata already loaded for {} items.").format(2)
    else:
        w.meta_cached_count, w.meta_to_fetch_count, w.meta_progress_current = 0, 2, 2
        w.on_meta_finished(finish == "cancelled")
        message = (tr("Metadata load cancelled. Loaded {}/{}.").format(2, 2)
                   if finish == "cancelled" else tr("Loaded {} items.").format(2))
    assert w.status_label.text() == message + " " + _showing(1, 2, excluded=1), (
        "metadata completion replaced the line that says rows are hidden")


@pytest.mark.parametrize("writer", ["restore finished", "restore failed", "replay"])
def test_a_refinement_replay_leaves_the_summary_not_a_blank(window, monkeypatch, writer):
    w = window
    _search(w, [_res(A, 1), _res(B, 1)])
    w._exclude_word_search_result(A, _row_of(w, A)[0])
    if writer == "restore finished":
        w._on_replay_for_restore_finished(set())
    elif writer == "restore failed":
        w._on_replay_for_restore_error("boom")
    else:
        monkeypatch.setattr(app, "replay_chain", lambda chain, searcher, scope: set())
        w._replay_refinement_chain()
    assert w.status_label.text() == _showing(1, 2, excluded=1)


# --------------------------------------------------------------------------
# A history click keeps the standing Exclude list
# --------------------------------------------------------------------------

_HISTORY = {"domain_exclusions": [], "printed_filter": "all"}


def test_a_history_click_does_not_drop_the_current_exclude_list(window):
    w = window
    w.exclusion_sources = [_src("my list", {B})]
    w.excluded_sys_ids = {B}
    w._update_exclusion_display("search")
    label = w.lbl_main_exclude_status.text()
    w._restore_regular_search_from_state(dict(_HISTORY, excluded_sys_ids=[]), entry=None)
    _search(w, [_res(A, 1), _res(B, 1)])
    assert w.excluded_sys_ids == {B}, "the history entry replaced the Exclude list"
    assert _hidden(w, B) == [True]
    assert w.lbl_main_exclude_status.text() == label


def test_a_history_click_does_not_install_an_old_exclude_list(window):
    w = window
    w._restore_regular_search_from_state(dict(_HISTORY, excluded_sys_ids=[B]), entry=None)
    _search(w, [_res(A, 1), _res(B, 1)])
    assert w.excluded_sys_ids == set(), (
        "the history entry installed ids the label and the dialog know nothing of")
    assert _hidden(w, B) == [False]


# --------------------------------------------------------------------------
# The exclusions survive every kind of restart
# --------------------------------------------------------------------------

class _Records(list):
    def handle(self, record):
        self.append(record)


def _session_state(results=(), word=(A,), listed=(B,)):
    return {
        "version": 1,
        "regular_search": {
            "query": "word",
            "results": list(results),
            "excluded_sys_ids": list(listed),
            "excluded_raw_entries": list(listed),
            "exclusion_sources": (serialize_sources([_src("my list", set(listed))])
                                  if listed else []),
        },
        "composition_search": {},
        "word_excluded_sys_ids": list(word),
    }


@pytest.fixture
def restore(window, monkeypatch):
    """Runs the real _restore_session over `state`. It logs and swallows its
    own exceptions, so a harness gap would pass silently: fail on any."""
    import logging
    w = window
    for name in ("_update_local_filter_btn_search", "_update_local_filter_btn_composition",
                 "_update_local_filter_btn_parallels", "_honour_deferred_comp_method",
                 "_apply_default_comp_method", "_refresh_search_history",
                 "_refresh_comp_history"):
        setattr(w, name, lambda *a, **k: None)
    w._restore_comp_passage_preferences = lambda comp, absent_method=None: None
    w._passage_snapshot_must_wait = lambda comp: False
    w._display_restored_comp_snapshot = lambda comp: False
    records = _Records()
    handler = logging.Handler(level=logging.ERROR)
    handler.emit = records.handle
    app.logger.addHandler(handler)

    def run(state, restore_mode, answer=None):
        monkeypatch.setattr(app, "load_app_config", lambda: {"restore_mode": restore_mode})
        monkeypatch.setattr(session_persistence, "load_session_state", lambda: state)
        if answer is not None:
            monkeypatch.setattr(app.QMessageBox, "exec", lambda self: answer)
        w._restore_session()
        assert not records, [r.getMessage() for r in records]
        return w

    yield run
    app.logger.removeHandler(handler)


@pytest.mark.parametrize("case", ["zero-result search", "restore_mode never", "declined"])
def test_both_exclusion_sets_and_the_label_survive_a_restart(restore, case):
    if case == "zero-result search":
        w = restore(_session_state(results=()), "ask")          # nothing to restore
    elif case == "restore_mode never":
        w = restore(_session_state(results=[_res(A, 1)]), "never")
    else:
        w = restore(_session_state(results=[_res(A, 1)]), "ask",
                    answer=app.QMessageBox.StandardButton.No)
    assert w.word_excluded_sys_ids == {A}, "the right-click exclusion did not survive the restart"
    assert w.excluded_sys_ids == {B}, "the Exclude Manuscripts list did not survive the restart"
    assert "1" in w.lbl_main_exclude_status.text(), "the Exclude label says nothing is excluded"
    _search(w, [_res(A, 1), _res(B, 1), _res(C, 1)])
    assert _hidden(w, A) == [True] and _hidden(w, B) == [True]
    assert w.status_label.text() == _showing(1, 3, excluded=2)


def test_a_restored_search_hides_what_the_restored_exclusions_exclude(restore):
    w = restore(_session_state(results=[_res(A, 1), _res(B, 1), _res(C, 1)]), "always")
    assert _hidden(w, A) == [True], "the replay ran before the right-click set was restored"
    assert _hidden(w, B) == [True], "the replay ignored the Exclude Manuscripts list"
    assert _hidden(w, C) == [False]


# --------------------------------------------------------------------------
# Export
# --------------------------------------------------------------------------

NOTHING_TO_EXPORT = ("Nothing to export: every result in the table is hidden "
                     "by a filter or an exclusion.")


def _all_hidden(w):
    _search(w, [_res(A, 1), _res(B, 1), _res(A, 2)])
    for r in range(w.results_table.rowCount()):
        w.results_table.setRowHidden(r, True)
    return w


def test_the_collector_returns_nothing_when_every_row_is_hidden(window):
    w = _all_hidden(window)
    assert w._collect_sorted_results() == [], (
        "an empty visible set was replaced by every result, excluded ones included")


def test_export_with_every_row_hidden_says_so_and_writes_nothing(window, monkeypatch, tmp_path):
    w = window
    _search(w, [_res(A, 1), _res(B, 1)])
    w._exclude_word_search_result(A, _row_of(w, A)[0])
    w._exclude_word_search_result(B, _row_of(w, B)[0])
    saves, events = [], []
    monkeypatch.setattr(app.QFileDialog, "getSaveFileName",
                        staticmethod(lambda *a, **k: saves.append(a) or ("", "")))
    monkeypatch.setattr(app.QMessageBox, "information",
                        staticmethod(lambda *a, **k: events.append(("message", a[2]))))
    w._emit_feature_opened = lambda **k: events.append(("opened", k))
    w._default_report_path = lambda q, name: str(tmp_path / "never.xlsx")
    w.export_results("csv")
    assert saves == [], "offered to save an export of a table whose every row is hidden"
    # The export dialog counts as opened with or without data (MEDIUM-8),
    # so the guard comes after that event and before the save dialog.
    assert events == [("opened", {"dialog_name": "export"}),
                      ("message", tr(NOTHING_TO_EXPORT))], (
        "the nothing-to-export message is missing, or it suppressed the export-opened event")


def test_the_nothing_to_export_message_has_a_hebrew_translation():
    from shared.genizah_translations import TRANSLATIONS
    assert TRANSLATIONS.get(NOTHING_TO_EXPORT), "user-visible string without a Hebrew entry"


# Guards (green before and after): the collector's other caller, and a partial view.

def test_view_result_still_opens_the_clicked_result_when_every_row_is_hidden(window):
    w = _all_hidden(window)
    opened = []
    w._show_result_dialog = lambda results, idx: opened.append((results, idx))
    res = w.last_results[1]
    w.show_full_text_for_result(res)
    [(results, idx)] = opened
    assert results[idx] is res


def test_the_collector_returns_exactly_the_visible_rows(window):
    w = window
    _search(w, [_res(A, 1), _res(B, 1), _res(C, 1)])
    w.results_table.setRowHidden(_row_of(w, B)[0], True)
    assert [r["display"]["id"] for r in w._collect_sorted_results()] == [A, C]


# --------------------------------------------------------------------------
# "Load more results": the next batch stays reachable when nothing scrolls
# --------------------------------------------------------------------------
#
# The next batch loads on a scroll to the bottom. A table whose visible rows
# fit in the window has no scroll range, so with the first batch all excluded
# ("Showing 0 of 50") nothing further could ever load.

@pytest.fixture
def load_more(window, monkeypatch):
    """The real button, installed by the method the Search tab's builder
    calls, over a table shown at a known size, with 20-row batches."""
    w = window
    install = getattr(w, "_install_load_more_button", None)
    if install is not None:
        install()
    monkeypatch.setattr(app, "BATCH_SIZE", 20)
    w.results_table.resize(900, 400)
    w.results_table.show()
    yield w
    w.results_table.hide()


def _load_more_state(w):
    """(shown, text) once Qt has laid the rows out."""
    QApplication.processEvents()
    btn = getattr(w, "btn_load_more_results", None)
    assert btn is not None, "the Search tab has no 'Load more results' button"
    return (not btn.isHidden(), btn.text())


def _load_more_text(n):
    return tr("Load more results ({} not loaded)").format(n)


def _rows(sid, n, start=1):
    return [_res(sid, p) for p in range(start, start + n)]


def _search_in_flight(w, results):
    w.is_searching = True                   # reset_ui clears it as the run ends
    _search(w, results)


def test_load_more_appears_when_the_whole_first_batch_is_excluded(load_more):
    w = load_more
    w.excluded_sys_ids = {A}
    _search_in_flight(w, _rows(A, 20) + _rows(B, 30))
    assert w.status_label.text() == _showing(0, 50, excluded=20)
    assert _load_more_state(w) == (True, _load_more_text(30)), (
        "the first batch is all excluded and nothing can scroll: batch 2 is unreachable")


def test_load_more_appears_after_a_restore_whose_first_batch_is_excluded(load_more, restore):
    w = restore(_session_state(results=_rows(A, 50) + _rows(B, 50), word=(A,), listed=()),
                "always")
    assert w.results_table.rowCount() == 50                 # the restore's first batch
    assert _load_more_state(w) == (True, _load_more_text(50)), (
        "the restore replayed a fully excluded first batch and left nothing to load the rest")


def test_load_more_is_hidden_while_the_restore_runs(load_more):
    w = load_more
    w.excluded_sys_ids = {A}
    w._restoring_session = True
    _search(w, _rows(A, 50) + _rows(B, 30))                 # a restore's first batch is 50
    assert w.results_table.rowCount() == 50
    assert _load_more_state(w)[0] is False
    w._restoring_session = False
    w._update_load_more_button()                            # what the restore's end does
    assert _load_more_state(w) == (True, _load_more_text(30))


def test_load_more_is_hidden_when_the_table_can_scroll(load_more):
    w = load_more
    w.results_table.resize(900, 200)
    _search_in_flight(w, _rows(B, 40))
    assert _load_more_state(w)[0] is False, "the button showed while scrolling works"
    assert w.results_table.verticalScrollBar().maximum() > 0


def test_load_more_follows_the_scroll_range_when_the_window_changes(load_more):
    """Nothing but the scroll bar's rangeChanged recomputes it on a resize."""
    w = load_more
    w.results_table.resize(900, 2000)
    _search_in_flight(w, _rows(B, 40))
    assert _load_more_state(w) == (True, _load_more_text(20))
    w.results_table.resize(900, 200)
    assert _load_more_state(w)[0] is False, "stale after the table became scrollable"
    w.results_table.resize(900, 2000)
    assert _load_more_state(w) == (True, _load_more_text(20))


def test_load_more_is_hidden_when_nothing_remains(load_more):
    w = load_more
    w.excluded_sys_ids = {A}
    _search_in_flight(w, _rows(A, 5))
    assert _load_more_state(w)[0] is False


def test_a_click_loads_exactly_one_batch_and_updates_the_count(load_more):
    w = load_more
    w.results_table.resize(900, 2000)       # no batch here ever overflows the view
    w.excluded_sys_ids = {A}
    _search_in_flight(w, _rows(A, 40) + _rows(B, 30))
    assert _load_more_state(w) == (True, _load_more_text(50))
    w.btn_load_more_results.click()
    assert w.results_table.rowCount() == 40 and w.results_loaded == 40
    assert _load_more_state(w) == (True, _load_more_text(30))
    assert w.status_label.text() == _showing(0, 70, excluded=40)


class _Signal:
    def connect(self, fn):
        pass


class _IdleSearchThread:
    """Takes start_search's run and never delivers it."""

    def __init__(self, *a, **k):
        self.results_signal = self.progress_signal = self.error_signal = _Signal()

    def start(self):
        pass

    def isRunning(self):
        return False


def _start_a_search_that_never_lands(w, monkeypatch):
    """The real start_search, with a worker that delivers nothing."""
    monkeypatch.setattr(app, "SearchThread", _IdleSearchThread)
    w.searcher = SimpleNamespace(parse_query_syntax=lambda q, responsa_mode=False: (None, q))
    w.mode_combo.addItems(["literal"] * 8)
    w.mode_combo.setCurrentIndex(0)
    w.MODE_RESPONSA, w.MODE_PGP_TAGS = 2, 7         # set by the UI builder
    w.gap_input, w.exclude_input = QLineEdit(), QLineEdit()
    w.text_position_combo = QComboBox()
    w.btn_lab_mode_toggle, w.search_within_btn = QPushButton(), QPushButton()
    w._run_seq = 0
    w._pause_search.reset_for_run = lambda run_id, now: None
    w.query_input.setText("other")
    w.start_search()
    w._search_elapsed_timer.stop()
    assert w.results_table.rowCount() == 0 and w.is_searching


def test_load_more_is_hidden_while_a_search_runs(load_more, monkeypatch):
    """start_search empties the table; a click on the button then would
    render the previous run's results into the new run's table."""
    w = load_more
    w.excluded_sys_ids = {A}
    _search_in_flight(w, _rows(A, 20) + _rows(B, 30))
    assert _load_more_state(w)[0] is True
    _start_a_search_that_never_lands(w, monkeypatch)   # the next search, still running
    assert _load_more_state(w)[0] is False, (
        "the button offers the previous run's results during a search")


@pytest.mark.parametrize("ending", ["an error", "Stop"])
def test_load_more_offers_nothing_after_a_search_that_delivered_nothing(
        load_more, monkeypatch, ending):
    """A run that ends with no results -- a bad pattern, a Stop before any
    arrive -- ends in reset_ui, which recomputes the button. It counted the
    previous run's results, and a click rendered 20 of them under the new
    query ("Showing 20 of 50 results")."""
    w = load_more
    w.excluded_sys_ids = {A}
    _search_in_flight(w, _rows(A, 20) + _rows(B, 30))
    assert _load_more_state(w)[0] is True
    _start_a_search_that_never_lands(w, monkeypatch)
    if ending == "an error":
        monkeypatch.setattr(app.QMessageBox, "critical", staticmethod(lambda *a, **k: None))
        w.on_error("bad pattern")                       # the worker's error_signal
    else:
        w.stop_search()
    assert not w.is_searching
    assert _load_more_state(w)[0] is False, "the button offers the previous run's results"
    w.btn_load_more_results.click()
    assert w.results_table.rowCount() == 0, "a click rendered the previous run's rows"


def test_the_all_terms_view_shows_nothing_after_a_search_that_failed(load_more, monkeypatch):
    """The same stale results reached the table through the all-terms
    checkbox, which re-renders last_results."""
    w = load_more
    _search_in_flight(w, _rows(B, 30))
    _start_a_search_that_never_lands(w, monkeypatch)
    monkeypatch.setattr(app.QMessageBox, "critical", staticmethod(lambda *a, **k: None))
    w.on_error("bad pattern")
    w._toggle_all_terms_filter(True)
    assert w.results_table.rowCount() == 0, "the all-terms view rendered the previous run's rows"


def test_a_click_is_checked_again_and_loads_nothing_while_a_search_runs(load_more):
    """The button only shows the state; a click re-checks it. With a run
    landing its results (nothing has recomputed the button yet), a click
    loads nothing."""
    w = load_more
    w.excluded_sys_ids = {A}
    _search_in_flight(w, _rows(A, 20) + _rows(B, 30))
    assert _load_more_state(w)[0] is True
    w.is_searching = True
    w.btn_load_more_results.click()
    assert w.results_table.rowCount() == 20 and w.results_loaded == 20, (
        "a click loaded a batch while a search was running")


def _assert_bottom_centre(w):
    btn, viewport = w.btn_load_more_results, w.results_table.viewport()
    g = btn.geometry()
    assert g.width() > 0 and g.left() >= 0 and g.top() >= 0, g
    assert abs((g.left() + g.width() / 2) - viewport.width() / 2) <= 1, (
        f"not centred: {g} in a viewport {viewport.width()} wide")
    assert 0 < viewport.height() - (g.top() + g.height()) <= 16, (
        f"not at the bottom: {g} in a viewport {viewport.height()} high")


def test_load_more_sits_bottom_centre_and_follows_a_resize(load_more):
    w = load_more
    w.excluded_sys_ids = {A}
    _search_in_flight(w, _rows(A, 20) + _rows(B, 30))
    assert _load_more_state(w)[0] is True
    _assert_bottom_centre(w)
    w.results_table.resize(1300, 700)       # the scroll range stays 0: only the resize says so
    assert _load_more_state(w)[0] is True
    _assert_bottom_centre(w)


def test_load_more_fits_its_label_when_the_count_grows(load_more):
    """A later search can count many more unloaded results than the last,
    with no resize in between to place the button again."""
    w = load_more
    w.excluded_sys_ids = {A}
    _search_in_flight(w, _rows(A, 20) + _rows(B, 5))
    assert _load_more_state(w) == (True, _load_more_text(5))
    _search_in_flight(w, _rows(A, 20) + _rows(B, 1000))
    assert _load_more_state(w) == (True, _load_more_text(1000))
    btn = w.btn_load_more_results
    assert btn.width() >= btn.sizeHint().width(), "the label no longer fits the button"
    _assert_bottom_centre(w)


def test_load_more_stays_centred_when_the_table_scrolls_sideways(load_more):
    """A horizontal scroll moves every child of the viewport with it."""
    w = load_more
    w.excluded_sys_ids = {A}
    _search_in_flight(w, _rows(A, 20) + _rows(B, 30))
    w.results_table.setColumnWidth(w.COL_SNIPPET, 4000)
    QApplication.processEvents()
    bar = w.results_table.horizontalScrollBar()
    assert bar.maximum() > 0
    bar.setValue(bar.maximum())
    assert _load_more_state(w)[0] is True
    _assert_bottom_centre(w)


# --- the Search tab as the app builds it ------------------------------------

@pytest.fixture
def search_tab(window, monkeypatch):
    """create_search_tab for real, over the window fixture's collaborators,
    shown at a known size, with 20-row batches."""
    w = window
    w._zero_result_refine = False
    monkeypatch.setattr(app, "BATCH_SIZE", 20)
    panel = w.create_search_tab()
    panel.resize(1000, 700)
    panel.show()
    QApplication.processEvents()
    yield w, panel
    # The builder makes the window an event filter of the tab's widgets. The
    # tab dies before the window here (never in the app), and its widgets'
    # last events would reach a filter reading the deleted table.
    for obj in [panel, *panel.findChildren(QObject)]:
        obj.removeEventFilter(w)
    panel.hide()


def _footer(w, panel):
    top = panel.layout()
    for i in range(top.count()):
        row = top.itemAt(i).layout()
        if row is not None and row.indexOf(w.status_label) >= 0:
            return row
    raise AssertionError("no row of the Search tab holds the status label")


def test_the_built_search_tab_puts_load_more_over_the_results(search_tab):
    w, panel = search_tab
    btn = getattr(w, "btn_load_more_results", None)
    assert btn is not None, "the Search tab as built has no 'Load more results' button"
    assert btn.parentWidget() is w.results_table.viewport(), (
        "the button is not an overlay on the results viewport")
    assert _footer(w, panel).indexOf(btn) == -1, "the button is in the footer layout"
    assert btn.isHidden(), "the button shows at startup, with nothing to load"


def test_load_more_does_not_widen_the_search_footer(search_tab):
    """In the footer layout the button raised the row's minimum width by
    its whole label (734 to 1,174 px), and it shows exactly when the table
    has room to spare -- the window could no longer be as narrow."""
    w, panel = search_tab
    w.excluded_sys_ids = {A}
    _search_in_flight(w, _rows(A, 20) + _rows(B, 30))
    assert _load_more_state(w) == (True, _load_more_text(30))
    footer = _footer(w, panel)
    footer.invalidate()
    shown = footer.minimumSize().width()
    w.btn_load_more_results.hide()
    footer.invalidate()
    assert footer.minimumSize().width() == shown


@pytest.mark.parametrize("ending", ["New", "a zero-result search", "a zero-result tag search"])
def test_load_more_does_not_outlive_the_results_it_counted(load_more, ending):
    w = load_more
    w.excluded_sys_ids = {A}
    _search_in_flight(w, _rows(A, 20) + _rows(B, 30))
    assert _load_more_state(w)[0] is True
    if ending == "New":
        w._reset_search()
    elif ending == "a zero-result search":
        _search_in_flight(w, [])
    else:
        w._on_tag_search_results("letters", [])
    assert _load_more_state(w)[0] is False


def test_after_the_all_terms_view_the_count_is_what_a_click_loads_from(load_more, monkeypatch):
    """The all-terms view swaps last_results for the render and restores the
    full set; a click reads the full set, so the label must count it."""
    w = load_more
    w.excluded_sys_ids = {A}
    _search_in_flight(w, _rows(A, 25) + _rows(B, 25))
    monkeypatch.setattr(app, "compute_all_terms_filter",
                        lambda chain: {f"{A}_{p}" for p in range(1, 26)})
    monkeypatch.setattr(app, "enrich_snippet_with_chain_terms", lambda s, c, q: s)
    w.refinement_chain = [object(), object()]
    w._all_terms_filter = True
    w._apply_all_terms_filter_and_rerender()
    assert w.results_table.rowCount() == 20
    remaining = len(w.last_results) - w.results_loaded
    assert _load_more_state(w) == (True, _load_more_text(remaining)), (
        "the label counts the swapped-in view, not what a click will read")


def test_the_load_more_label_has_a_hebrew_translation():
    from shared.genizah_translations import TRANSLATIONS
    assert TRANSLATIONS.get("Load more results ({} not loaded)"), (
        "user-visible string without a Hebrew entry")
