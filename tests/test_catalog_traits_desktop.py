# -*- coding: utf-8 -*-
"""Desktop Catalog tab: "Script and size" lists manuscripts by a trait (2026-11-01).

The dialog's traits must reach both workers (the list and the library counts),
show as chips, and hand on to "Search in these results" with the scope the
search runs with computed from them.
"""
import os
import sys

import pytest

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only

from PyQt6.QtCore import Qt  # noqa: E402
from PyQt6.QtWidgets import QApplication  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fjms_filter_sidecar import build_browse_sidecar  # noqa: E402

import genizah_app  # noqa: E402
import genizah_core  # noqa: E402
import desktop.dialogs_filter as dialogs_filter  # noqa: E402
from shared import fjms_service  # noqa: E402
from shared.fjms_service import FjmsService  # noqa: E402

_APP = QApplication.instance() or QApplication([])
TRAITS = {'script_places': ['Oriental'], 'width_min': 14.0}
EXPECTED = {'990002', '990003'}  # Oriental, and at least 14 cm wide


@pytest.fixture
def svc(tmp_path, monkeypatch):
    service = FjmsService(db_path=build_browse_sidecar(tmp_path / "browse.db"))
    monkeypatch.setattr(fjms_service, "_default_service", service)
    for module in (genizah_core, genizah_app, dialogs_filter):
        monkeypatch.setattr(module, 'CURRENT_LANG', 'en')
    yield service
    service.close()


def test_the_dialog_round_trips_its_traits_and_clears(svc):
    dlg = dialogs_filter.CatalogTraitsDialog(None, dict(TRAITS, line_count_max=30))
    try:
        assert dlg.get_traits() == dict(TRAITS, line_count_max=30)
        oriental = dlg._script_lists['script_places']
        dialogs_filter.PreSearchFilterDialog._check_list_item(oriental, 'Spanish', True)
        assert dlg.get_traits()['script_places'] == ['Oriental', 'Spanish']
        dlg._clear()
        assert dlg.get_traits() == {}
    finally:
        dlg.deleteLater()


def _run_worker(worker):
    out = []
    worker.done.connect(out.append)
    worker.run()  # synchronously, on this thread
    return out[-1]


def test_both_workers_apply_the_traits(svc):
    result = _run_worker(genizah_app._CatalogRefreshWorker(
        None, None, None, None, 0, 50, refresh_authors=False, refresh_works=False, traits=TRAITS))
    assert {r['sys_id'] for r in result['data']['results']} == EXPECTED
    library = {'990002': 'JTS', '990003': 'CUL'}
    facets = _run_worker(genizah_app._CatalogFacetWorker(
        None, None, None, None, meta_mgr=type('M', (), {'get_library_for_id': staticmethod(library.get)})(),
        traits=TRAITS))
    assert facets == {'JTS': 1, 'CUL': 1}


class _Host:
    """The Catalog-tab state the methods under test read, with the real methods."""

    _catalog_build_browse_filters = genizah_app.GenizahGUI._catalog_build_browse_filters
    _apply_catalog_handoff_filters = genizah_app.GenizahGUI._apply_catalog_handoff_filters
    _catalog_trait_chips = genizah_app.GenizahGUI._catalog_trait_chips

    def __init__(self, traits):
        self._catalog_current_domain = self._catalog_current_author = self._catalog_current_work = None
        self._catalog_date_from = self._catalog_date_to = None
        self._catalog_library_filter, self._catalog_library_mode = [], 'hide'
        self._catalog_traits = dict(traits)
        self.pre_search_filters, self.pre_search_restrict_sys_ids = {}, None

    def _supersede_filter_lookup(self):
        pass


def test_search_in_these_results_carries_the_traits_and_their_scope(svc):
    host = _Host(TRAITS)
    filters = host._catalog_build_browse_filters()
    assert filters == TRAITS
    assert host._apply_catalog_handoff_filters(filters)
    assert host.pre_search_filters == TRAITS
    assert host.pre_search_restrict_sys_ids == EXPECTED, "the search must run on the browsed set"


def test_trait_chips(svc):
    host = _Host(dict(TRAITS, line_count_min=20, line_count_max=30, measurement_material=['Paper']))
    assert [text for text, _k, _v in host._catalog_trait_chips()] == [
        'Script Place: Oriental', 'Width: ≥14 cm', 'Lines: 20–30', 'Material (measured): Paper']
    assert [(k, v) for _t, k, v in host._catalog_trait_chips()] == [
        ('script_places', 'Oriental'), ('width', None), ('line_count', None), ('measurement_material', 'Paper')]


def test_the_tab_refresh_passes_its_traits_to_the_worker(svc, monkeypatch):
    """The call site, not just the worker: _catalog_start_async_refresh."""
    from types import SimpleNamespace
    made = []

    class _Recorder:
        def __init__(self, parent, **kwargs):
            made.append(kwargs)
            self.done = SimpleNamespace(connect=lambda fn: None)

        def start(self):
            pass

    monkeypatch.setattr(genizah_app, '_CatalogRefreshWorker', _Recorder)
    host = _Host(TRAITS)
    host.catalog_results_table = SimpleNamespace(setRowCount=lambda n: None)
    host._catalog_count_label = SimpleNamespace(setText=lambda s: None)
    host._catalog_current_page, host._CATALOG_PAGE_SIZE = 0, 50
    host._catalog_include_undated = False
    host._catalog_text_all = host._catalog_text_any = host._catalog_text_not = []
    host._catalog_pgp_filter = host._catalog_editions_filter = 'all'
    host.meta_mgr = None
    host._catalog_on_async_refresh_done = lambda result: None
    genizah_app.GenizahGUI._catalog_start_async_refresh(host)
    assert made and made[-1]['traits'] == TRAITS
    assert made[-1]['traits'] is not host._catalog_traits, "the worker gets a copy"


def test_the_shared_script_lists_still_drive_the_search_dialog(svc, monkeypatch):
    """build_script_lists was factored out of PreSearchFilterDialog: its lists
    must still recount the dialog."""
    counts = []
    monkeypatch.setattr(dialogs_filter.PreSearchFilterDialog, '_update_count',
                        lambda self: counts.append(self.get_filters()))
    dlg = dialogs_filter.PreSearchFilterDialog(None, {})
    try:
        counts.clear()
        dlg._check_list_item(dlg._script_lists['script_places'], 'Yemenite', True)
        assert counts and counts[-1].get('script_places') == ['Yemenite']
        assert dlg._script_lists['script_places'].item(0).flags() & Qt.ItemFlag.ItemIsUserCheckable
    finally:
        dlg.deleteLater()
