# -*- coding: utf-8 -*-
"""Desktop surfaces of the catalogue criteria (PR 1c of
docs/plans/CATALOGUE_CRITERIA_PLAN.md, §6.3-§6.4): the Catalog tab (button,
chips, workers, hand-off, the domain tree's "No catalogue record" entry),
"Search only in..." and the main window (chips, chip removal, history
summary, saved-filter migration), and the post-search filters (D20).

Each test drives the real GenizahGUI methods (call sites, not only helpers)
on a window built without __init__, over a small real sidecar
(tests/criteria_sidecar.py).
"""
import os
import sys
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only

from PyQt6.QtCore import QObject, QTimer, pyqtSignal  # noqa: E402
from PyQt6.QtWidgets import (QApplication, QHBoxLayout, QLabel, QMainWindow,  # noqa: E402
                             QPushButton, QWidget)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from criteria_sidecar import A, B, C, D, E, F, UNIVERSE, build_criteria_sidecar  # noqa: E402

import genizah_app  # noqa: E402
import genizah_core  # noqa: E402
import desktop.dialogs_filter as dialogs_filter  # noqa: E402
from genizah_app import GenizahGUI  # noqa: E402
from shared import fjms_service  # noqa: E402
from shared.fjms_service import FjmsService  # noqa: E402

_APP = QApplication.instance() or QApplication([])

EXCEPT_ORIENTAL = {'region': {'values': ['Oriental'], 'mode': 'except'}}
LINES = {'lines': {'source': 'either', 'min': 20, 'max': 25}}
LIBRARY = {A: 'CUL', B: 'JTS', C: 'CUL', D: 'JTS', E: 'BL', F: 'CUL'}


class _Meta:
    """The MetadataManager methods these call sites use."""

    def corpus_sys_ids(self, timeout=30.0):
        return UNIVERSE

    def get_library_for_id(self, sid):
        return LIBRARY.get(sid, '')

    def get_meta_for_id(self, sid):
        return f"T-S {sid[-8:-6]}", ''


@pytest.fixture
def svc(tmp_path, monkeypatch):
    path = build_criteria_sidecar(tmp_path / "c.db")
    monkeypatch.setenv("GENIZAH_FJMS_DB_PATH", path)
    service = FjmsService(db_path=path)
    monkeypatch.setattr(fjms_service, "_default_service", service)
    for module in (genizah_core, genizah_app, dialogs_filter):
        monkeypatch.setattr(module, 'CURRENT_LANG', 'en')
    yield service
    service.close()


def _window(**attrs):
    w = GenizahGUI.__new__(GenizahGUI)
    QMainWindow.__init__(w)
    w.meta_mgr = _Meta()
    w._schedule_session_save = lambda: None
    for name, value in attrs.items():
        setattr(w, name, value)
    return w


def _catalog_window(criteria=None):
    w = _window()
    w._catalog_current_domain = w._catalog_current_author = w._catalog_current_work = None
    w._catalog_text_all, w._catalog_text_any, w._catalog_text_not = [], [], []
    w._catalog_pgp_filter = w._catalog_editions_filter = 'all'
    w._catalog_library_filter, w._catalog_library_mode = [], 'hide'
    w._catalog_criteria = dict(criteria or {})
    w._catalog_current_page, w._CATALOG_PAGE_SIZE = 0, 50
    w.pre_search_filters, w.pre_search_restrict_sys_ids = {}, None
    return w


def _run(worker):
    out = []
    worker.done.connect(out.append)
    worker.run()  # synchronously, on this thread
    return out[-1]


# -- Catalog tab ------------------------------------------------------------------

def test_the_tab_has_the_button_and_a_tree_that_cannot_collapse(svc):
    w = _window()
    panel = w.create_catalog_browse_tab()  # noqa: F841 - kept: it owns the widgets
    assert w._catalog_criteria_btn.text() == "Manuscript features…"
    assert w.catalog_domain_tree.minimumHeight() == 240, "the tree collapsed when the column overflowed"
    for gone in ('_catalog_date_from_input', '_catalog_undated_cb', '_catalog_traits_btn'):
        assert not hasattr(w, gone), f"{gone}: the date and traits controls moved into the window"


def test_both_catalog_workers_apply_the_criteria_over_the_corpus(svc):
    """D16/D22: catalogued first, then the uncatalogued, each in sys_id order;
    the library counts honour the criteria too."""
    result = _run(genizah_app._CatalogRefreshWorker(
        None, None, None, None, 0, 50, refresh_authors=False, refresh_works=False,
        meta_mgr=_Meta(), criteria=EXCEPT_ORIENTAL))
    assert [r['sys_id'] for r in result['data']['results']] == [B, C, F, D, E]
    assert result['data']['total'] == len(svc.get_filter_sys_ids(criteria=EXCEPT_ORIENTAL,
                                                                  universe=UNIVERSE))
    facets = _run(genizah_app._CatalogFacetWorker(None, None, None, None, meta_mgr=_Meta(),
                                                  criteria=EXCEPT_ORIENTAL))
    assert facets == {'JTS': 2, 'CUL': 2, 'BL': 1}


class _Recorder:
    """Stands in for a catalog worker: records its arguments, answers with ``reply``."""
    made = []
    reply = {}

    def __init__(self, parent, **kwargs):
        _Recorder.made.append(kwargs)
        self._done = []
        self.done = SimpleNamespace(connect=self._done.append)

    def start(self):
        QTimer.singleShot(0, lambda: [fn(dict(_Recorder.reply)) for fn in self._done])


def test_the_tab_refresh_passes_a_copy_of_its_criteria(svc, monkeypatch):
    _Recorder.made = []
    monkeypatch.setattr(genizah_app, '_CatalogRefreshWorker', _Recorder)
    w = _catalog_window(EXCEPT_ORIENTAL)
    w.catalog_results_table = SimpleNamespace(setRowCount=lambda n: None)
    w._catalog_count_label = SimpleNamespace(setText=lambda s: None)
    w._catalog_on_async_refresh_done = lambda result: None
    w._catalog_start_async_refresh()
    assert _Recorder.made[-1]['criteria'] == EXCEPT_ORIENTAL
    assert _Recorder.made[-1]['criteria'] is not w._catalog_criteria, "the worker gets a copy"
    assert 'date_from' not in _Recorder.made[-1], "the old date range moved into the criteria"


def test_the_library_counts_get_the_criteria(svc, monkeypatch):
    _Recorder.made = []
    monkeypatch.setattr(genizah_app, '_CatalogFacetWorker', _Recorder)

    class _Rejected:
        def __init__(self, *a, **k):
            pass

        def exec(self):
            return genizah_app.QDialog.DialogCode.Rejected
    monkeypatch.setattr(genizah_app, 'LibraryFilterDialog', _Rejected)
    w = _catalog_window(EXCEPT_ORIENTAL)
    w._open_catalog_library_dialog()
    assert _Recorder.made[-1]['criteria'] == EXCEPT_ORIENTAL


def test_search_in_these_results_carries_the_criteria_and_their_scope(svc, monkeypatch):
    """A browse list hands its criteria on unchanged (§6.3); the scope is the
    same set, uncatalogued manuscripts included (they have no region)."""
    warned = []
    monkeypatch.setattr(genizah_app.QMessageBox, 'warning', staticmethod(lambda *a, **k: warned.append(a)))
    w = _catalog_window(EXCEPT_ORIENTAL)
    filters = w._catalog_build_browse_filters()
    assert filters == {'criteria': EXCEPT_ORIENTAL}
    assert w._apply_catalog_handoff_filters(filters)
    assert w.pre_search_filters == {'criteria': EXCEPT_ORIENTAL}
    assert w.pre_search_restrict_sys_ids == {B, C, D, E, F}
    assert warned == []


def _chip_texts(layout):
    return [layout.itemAt(i).widget().text() for i in range(layout.count())
            if isinstance(layout.itemAt(i).widget(), QPushButton)]


def test_catalog_chips_one_per_criterion_and_each_removes_its_own(svc):
    w = _catalog_window(dict(EXCEPT_ORIENTAL, **LINES))
    host = QWidget()
    w._catalog_chips_layout = QHBoxLayout(host)
    w._catalog_clear_all_btn = QPushButton()
    refreshed = []
    w._catalog_start_async_refresh = lambda **k: refreshed.append(dict(w._catalog_criteria))
    w._catalog_update_chips()
    assert _chip_texts(w._catalog_chips_layout) == [
        "Region: all except Oriental  ×", "Lines: 20–25  ×"]
    assert not w._catalog_clear_all_btn.isHidden()
    w._catalog_chips_layout.itemAt(0).widget().click()
    assert w._catalog_criteria == LINES
    assert refreshed == [LINES], "removing a chip must list again"
    assert _chip_texts(w._catalog_chips_layout) == ["Lines: 20–25  ×"]


def test_the_window_sets_the_tab_criteria(svc, monkeypatch):
    class _Dialog:
        dropped_saved_criteria = []

        def __init__(self, parent, criteria):
            self.seen = criteria

        def exec(self):
            return genizah_app.QDialog.DialogCode.Accepted

        def get_criteria(self):
            return dict(LINES)
    monkeypatch.setattr(dialogs_filter, 'CriteriaDialog', _Dialog)
    w = _catalog_window()
    w._catalog_criteria_btn = QPushButton()
    refreshed = []
    w._catalog_start_async_refresh = lambda **k: refreshed.append(True)
    w._catalog_update_chips = lambda: None
    w._catalog_open_criteria_dialog()
    assert w._catalog_criteria == LINES and refreshed
    assert w._catalog_criteria_btn.text() == "Manuscript features (1)…"


def test_criteria_the_file_cannot_answer_leave_the_tab_even_on_cancel(svc, monkeypatch):
    """The window dropped them with a notice; Cancel must not keep them under the list."""
    class _Dialog:
        dropped_saved_criteria = ['lines']

        def __init__(self, parent, criteria):
            pass

        def exec(self):
            return genizah_app.QDialog.DialogCode.Rejected
    monkeypatch.setattr(dialogs_filter, 'CriteriaDialog', _Dialog)
    w = _catalog_window(dict(EXCEPT_ORIENTAL, **LINES))
    refreshed = []
    w._catalog_start_async_refresh = lambda **k: refreshed.append(dict(w._catalog_criteria))
    w._catalog_update_chips = lambda: None
    w._catalog_open_criteria_dialog()
    assert w._catalog_criteria == EXCEPT_ORIENTAL and refreshed == [EXCEPT_ORIENTAL]


def test_the_domain_tree_counts_the_manuscripts_with_no_catalogue_record(svc):
    from PyQt6.QtWidgets import QTreeWidget
    counted = []
    worker = genizah_app._CatalogUncataloguedWorker(_Meta())
    worker.counted.connect(counted.append)
    worker.run()
    assert counted == [2], "D and E have no catalogue record"
    w = _catalog_window()
    w.catalog_domain_tree = QTreeWidget()
    w.catalog_domain_tree.setColumnCount(2)
    w._catalog_add_uncatalogued_entry(2)
    w._catalog_add_uncatalogued_entry(2)  # a second answer updates, never duplicates
    items = [w.catalog_domain_tree.topLevelItem(i) for i in range(w.catalog_domain_tree.topLevelItemCount())]
    assert [(i.text(0), i.text(1)) for i in items] == [("No catalogue record", "2")]
    w._catalog_start_async_refresh = lambda **k: pytest.fail("the entry is not a domain to list")
    w._catalog_on_domain_select(items[0])


def test_rendering_the_tree_adds_the_entry(svc, monkeypatch):
    """The call site: _catalog_render_tree starts the count and adds its answer."""
    from PyQt6.QtWidgets import QTreeWidget

    class _Sync(genizah_app._CatalogUncataloguedWorker):
        def start(self):
            self.run()
    monkeypatch.setattr(genizah_app, '_CatalogUncataloguedWorker', _Sync)
    monkeypatch.setattr(genizah_app.QTimer, 'singleShot', staticmethod(lambda *a: None))
    w = _catalog_window()
    w.catalog_domain_tree = QTreeWidget()
    w.catalog_domain_tree.setColumnCount(2)
    w._catalog_render_tree({}, 5)
    tree = w.catalog_domain_tree
    assert [tree.topLevelItem(i).text(0) for i in range(tree.topLevelItemCount())] == [
        "Unclassified", "No catalogue record"]


# -- "Search only in..." and the main window ----------------------------------------

def test_search_only_in_hands_its_criteria_to_the_count(svc, monkeypatch):
    """The dialog's button sets filters['criteria'], and its count (the call
    site, FilterCountWorker) applies them over the corpus."""
    made = []

    class _Worker(QObject):
        finished = pyqtSignal(object)
        failed = pyqtSignal(str)

        def __init__(self, filters, parent=None, **kw):
            super().__init__()
            made.append((filters, kw))

        def start(self):
            pass
    monkeypatch.setattr(dialogs_filter, 'FilterCountWorker', _Worker)
    owner = QWidget()
    owner.meta_mgr = _Meta()
    dlg = dialogs_filter.PreSearchFilterDialog(owner, {'include_mode': True, 'criteria': EXCEPT_ORIENTAL})
    try:
        assert made[-1][0]['criteria'] == EXCEPT_ORIENTAL
        assert made[-1][1]['universe']() == UNIVERSE
        assert dlg.criteria_btn.text() == "Manuscript features (1)…"
        dlg._remove_criterion('region')
        assert 'criteria' not in dlg.get_filters()
    finally:
        dlg.deleteLater()
    from desktop.gui_threads import FilterCountWorker
    worker = FilterCountWorker({'include_mode': True, 'criteria': EXCEPT_ORIENTAL}, universe=UNIVERSE)
    got = []
    worker.finished.connect(got.append)
    worker.run()
    assert got == [{B, C, D, E, F}]


def _chip_window(filters):
    w = _window(pre_search_filters=filters, pre_search_restrict_sys_ids={B})
    for prefix in ('', 'comp_'):
        bar = QWidget()
        setattr(w, f'{prefix}filter_chip_bar', bar)
        setattr(w, f'{prefix}filter_chip_bar_layout', QHBoxLayout(bar))
        setattr(w, f'{prefix}filter_chip_count_label', QLabel())
    return w


class _IdleWorker(QObject):
    finished = pyqtSignal(object)
    failed = pyqtSignal(str)

    def __init__(self, *a, **k):
        super().__init__()

    def start(self):
        pass


def test_main_window_chips_and_their_removal(svc, monkeypatch):
    monkeypatch.setattr(genizah_app, 'FilterCountWorker', _IdleWorker)
    w = _chip_window({'include_mode': True, 'criteria': dict(EXCEPT_ORIENTAL, **LINES)})
    w._update_filter_chip_bar()
    texts = _chip_texts(w.filter_chip_bar_layout)
    assert texts == ["Region: all except Oriental  ×", "Lines: 20–25  ×"], texts
    w._remove_filter(('criteria', 'region'))
    assert w.pre_search_filters == {'include_mode': True, 'criteria': LINES}
    assert w._filter_lookup_pending, "the scope is recomputed for what is left"
    w._remove_filter(('criteria', 'lines'))
    assert w.pre_search_filters == {}, "the last criterion gone, no filter is left"


@pytest.mark.parametrize('include_mode,expected', [
    (True, '[include: Bible, Region: Spanish]'),
    (False, '[exclude: Bible] [include: Region: Spanish]'),
])
def test_the_history_summary_never_puts_a_criterion_under_exclude(svc, include_mode, expected):
    filters = {'include_mode': include_mode, 'domains': ['Bible'],
               'criteria': {'region': {'values': ['Spanish']}}}
    assert GenizahGUI._build_filter_summary(filters, 80) == expected


def test_a_truncated_summary_keeps_its_brackets_balanced(svc):
    from fjms_filter_sidecar import brackets_balanced
    filters = {'include_mode': False, 'domains': ['Bible', 'Liturgy', 'Halakha'],
               'criteria': {'period': {'min': 1000, 'max': 1300}, 'region': {'values': ['Spanish']}}}
    for max_len in range(20, 90):
        s = GenizahGUI._build_filter_summary(filters, max_len)
        assert len(s) <= max_len and brackets_balanced(s), (max_len, s)


# -- saved filters, as they run today (§6.4) --------------------------------------

def test_saved_dates_become_the_period_and_old_measurements_are_dropped():
    saved = {'include_mode': False, 'domains': ['Bible'], 'date_from': 1000, 'date_to': 1100,
             'include_undated': True, 'width_min': 10.0, 'measurement_material': ['Paper'],
             'script_styles': ['Square']}
    migrated, had_measurements = dialogs_filter.migrate_saved_filters(saved)
    assert migrated == {'include_mode': False, 'domains': ['Bible'], 'criteria': {
        'period': {'min': 1000, 'max': 1100, 'not_recorded': True}}}
    assert had_measurements is True
    migrated['domains'].append('Piyyut')
    assert saved['domains'] == ['Bible'], "a copy: a chip removed later must not edit the saved entry"
    assert dialogs_filter.migrate_saved_filters({'include_undated': True}) == ({}, False), (
        '"include undated" alone never filtered')
    kept = {'criteria': {'period': {'min': 1000}}, 'date_from': 900}
    assert dialogs_filter.migrate_saved_filters(kept)[0] == {'criteria': {'period': {'min': 1000}}}, (
        "a saved period wins over a stray old date")


@pytest.mark.parametrize("restore", ["_restore_regular_search_from_state",
                                     "_restore_comp_search_from_state"])
def test_a_history_entry_with_the_old_bounds_reruns_instead_of_showing_its_snapshot(
        svc, monkeypatch, restore):
    """R2: a legacy snapshot found under the old measurement bounds is not shown."""
    from PyQt6.QtWidgets import QLineEdit, QPlainTextEdit
    monkeypatch.setattr(genizah_app, 'FilterCountWorker', _IdleWorker)
    shown = []
    w = _window(query_input=QLineEdit(), comp_text_area=QPlainTextEdit(), comp_title_input=QLineEdit(),
                composition_tab=None, refinement_chain=[], pre_search_filters={},
                pre_search_restrict_sys_ids=None)
    w._set_local_scope_strip_visible = lambda v: None
    w._witness_edits_are_locked = lambda: False
    w._restore_comp_passage_preferences = lambda params: None
    w._restore_comp_conditions = lambda conditions: None
    w._update_filter_chip_bar = lambda: None
    w._set_active_tab = lambda tab: None
    w.on_search_finished = lambda results: shown.append(results)
    w._clear_compound_results = lambda: None
    w.display_comp_results = lambda *a, **k: shown.append(a)
    entry = {'query': 'word', 'pre_search_filters': {'include_mode': True, 'width_min': 10.0,
                                                     'domains': ['Bible']}}
    state = {'source_text': 'a b c', 'results': [{'display': {'id': A}}],
             'filtered_results': [{'display': {'id': A}}]}
    getattr(w, restore)(state, entry)
    assert shown == [], "a snapshot found under the old measurement bounds was shown"
    assert w.pre_search_filters == {'include_mode': True, 'domains': ['Bible']}


# -- post-search filters (D20) -----------------------------------------------------

def test_post_search_filters_keep_what_the_window_counts(svc):
    """D20: the main window's post-search set is the same answer the window's
    own total gives over the results, by the panel's rules."""
    from desktop.gui_threads import CriteriaCountWorker
    criteria = {'width': {'min': 11}, 'lines': {'min': 20, 'max': 25, 'not_recorded': True}}
    results = [{'display': {'id': sid}} for sid in (A, B, C, D)]
    w = _window(last_results=results, _post_criteria=criteria)
    w._recompute_post_criteria()
    counted = []
    worker = CriteriaCountWorker(criteria, universe=frozenset({A, B, C, D}))
    worker.counted.connect(lambda tag, total, info: counted.append(total))
    worker.run()
    assert w._post_criteria_ids == svc.get_filter_sys_ids(criteria=criteria, universe={A, B, C, D})
    assert counted == [len(w._post_criteria_ids)]
    assert w._post_criteria_error is None


def test_post_search_filters_that_cannot_run_hide_nothing_and_say_so(svc, monkeypatch):
    monkeypatch.setattr(svc, 'match_result_criteria', lambda c, ids: (_ for _ in ()).throw(
        fjms_service.FilterUnavailable('query_failed')))
    w = _window(last_results=[{'display': {'id': A}}], _post_criteria=dict(LINES))
    w._recompute_post_criteria()
    assert w._post_criteria_ids is None and w._post_criteria_error == 'query_failed'
    assert w.statusBar().currentMessage() == (
        "The measurement filters could not be applied: every result is shown.")


def test_the_enrichment_step_computes_the_post_search_set(svc, monkeypatch):
    """The call site: _launch_enrichment_workers (after every search)."""
    class _Inert(QObject):
        finished = pyqtSignal(object)

        def __init__(self, *a, **k):
            super().__init__()

        def start(self):
            pass

        def isRunning(self):
            return False
    import desktop.gui_threads as gui_threads
    monkeypatch.setattr(gui_threads, 'DomainEnrichmentWorker', _Inert)
    monkeypatch.setattr(genizah_app, 'PGPBadgeWorker', _Inert)
    monkeypatch.setattr(genizah_app, 'PrintedBadgeWorker', _Inert)
    results = [{'display': {'id': sid}} for sid in (A, B)]
    w = _window(last_results=results, _post_criteria={'region': {'values': ['Spanish']}},
                _post_criteria_ids=None, _domain_worker=None, _pgp_badge_worker=None,
                _printed_badge_worker=None, btn_measurement_filter=QPushButton())
    w._apply_results_table_filters = lambda: None
    w._launch_enrichment_workers(results, defer=False)
    assert w._post_criteria_ids == {B}
    assert w._measurement_fetch_complete and w.btn_measurement_filter.isEnabled()


def test_the_post_search_button_opens_the_panel_on_its_sections(svc, monkeypatch):
    seen = {}

    class _Dialog:
        def __init__(self, parent, criteria, **kwargs):
            seen.update(kwargs, criteria=criteria)

        def exec(self):
            return genizah_app.QDialog.DialogCode.Accepted

        def get_criteria(self):
            return {'region': {'values': ['Spanish'], 'mode': 'only'}}
    monkeypatch.setattr(dialogs_filter, 'CriteriaDialog', _Dialog)
    results = [{'display': {'id': sid}} for sid in (A, B)]
    w = _window(last_results=results, _post_criteria={}, _measurement_fetch_complete=True)
    applied = []
    w._apply_results_table_filters = lambda: applied.append(w._post_criteria_ids)
    w._open_measurement_filter_dialog()
    assert seen['sections'] == ('lines', 'material', 'measured')
    assert seen['within'] == frozenset({A, B})
    assert applied == [{B}]
