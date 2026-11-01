"""The corpus list and its callers (PR 1a of docs/plans/CATALOGUE_CRITERIA_PLAN.md):
MetadataManager.corpus_sys_ids never hands over part of libraries.csv (rule 9),
and the desktop Catalog tab's worker passes it on (D16) and reports a failed
list as an error, not as no results."""
import threading
from unittest.mock import MagicMock

import pytest

from shared.fjms_service import FilterUnavailable
from shared.metadata_manager import MetadataManager


def _manager(bank, loaded):
    mgr = MetadataManager.__new__(MetadataManager)
    mgr.csv_bank = dict(bank)
    mgr._csv_bank_loaded = loaded
    mgr._csv_bank_done = threading.Event()
    return mgr


def test_corpus_sys_ids_is_the_corpus_namespace_only():
    mgr = _manager({"990001000205171": {}, "970000000001": {}, "9900020": {}}, loaded=True)
    assert mgr.corpus_sys_ids() == frozenset({"990001000205171"})


def test_corpus_sys_ids_follows_a_grown_bank():
    mgr = _manager({"990001000205171": {}}, loaded=True)
    assert len(mgr.corpus_sys_ids()) == 1
    mgr.csv_bank["990002000205171"] = {}
    assert len(mgr.corpus_sys_ids()) == 2


def test_corpus_sys_ids_never_returns_a_partial_list():
    mgr = _manager({"990001000205171": {}}, loaded=False)
    mgr._csv_bank_done.set()          # the load ended without reading the file
    with pytest.raises(FilterUnavailable) as err:
        mgr.corpus_sys_ids(timeout=0.01)
    assert err.value.reason == "corpus_unavailable"


def test_corpus_sys_ids_waits_for_the_load():
    mgr = _manager({}, loaded=False)

    def finish():
        mgr.csv_bank["990001000205171"] = {}
        mgr._csv_bank_loaded = True
        mgr._csv_bank_done.set()
    threading.Timer(0.05, finish).start()
    assert mgr.corpus_sys_ids(timeout=5) == frozenset({"990001000205171"})


def _run_worker(monkeypatch, fake, meta_mgr):
    import genizah_app
    import shared.fjms_service as fjms_mod
    monkeypatch.setattr(fjms_mod, 'get_fjms_service', lambda **_: fake)
    emitted = []
    w = genizah_app._CatalogRefreshWorker(
        None, domain=None, author=None, work=None, offset=0, limit=50,
        refresh_authors=False, refresh_works=False, meta_mgr=meta_mgr)
    w.done.connect(emitted.append)
    w.run()
    return emitted[-1]


def test_desktop_worker_passes_the_corpus(monkeypatch):
    seen = {}
    fake = MagicMock()
    fake.get_browse_results.side_effect = lambda *a, **k: seen.update(k) or {'results': [], 'total': 0}
    meta = MagicMock()
    meta.corpus_sys_ids = lambda timeout=30.0: frozenset({"990001000205171"})
    _run_worker(monkeypatch, fake, meta)
    assert seen['universe']() == frozenset({"990001000205171"})


def test_desktop_worker_reports_a_failed_list_as_an_error(monkeypatch):
    fake = MagicMock()
    fake.get_browse_results.side_effect = FilterUnavailable('query_failed', 'boom')
    result = _run_worker(monkeypatch, fake, MagicMock())
    assert result['data'] == {'results': [], 'total': 0, 'error': True}
