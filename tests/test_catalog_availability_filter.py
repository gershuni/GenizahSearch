"""SEED-023 desktop parity — catalog 'Browse by Identification' availability filters.

The desktop catalog browse now mirrors the web's 3-state PGP + Scholarly
Transcriptions filters. These pin the new desktop-side wiring:
  * ``_get_catalog_filter_sets`` builds + caches the corpus-wide sets
    (PGP link  /  PGP %Edition% ∪ FGP Digital Edition).
  * ``_CatalogRefreshWorker`` threads ``pgp_filter`` / ``editions_filter`` (and the
    resolved sys-id sets) into the shared ``get_browse_results`` ONLY when active.

GUI-marked (constructs QApplication for the QThread worker); runs in gui-tests.
"""

from unittest.mock import MagicMock

from PyQt6.QtWidgets import QApplication

import pytest

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only -- Qt in the mixed non-GUI run
# segfaults after thousands of NiceGUI/asyncio tests share the process (2026-08-21).

_app = QApplication.instance() or QApplication([])


def _reset_cache():
    import genizah_app
    genizah_app._CATALOG_FILTER_SETS['value'] = None


def test_get_catalog_filter_sets_unions_and_caches(monkeypatch):
    import genizah_app
    import shared.document_service as ds
    import shared.fgp_service as fs
    _reset_cache()
    calls = {'pgp': 0}

    def _pgp():
        calls['pgp'] += 1
        return {'1', '2'}

    monkeypatch.setattr(ds, 'get_all_pgp_link_sys_ids', _pgp)
    monkeypatch.setattr(ds, 'get_sys_ids_with_editions', lambda: ['2', '3'])  # PGP %Edition%
    monkeypatch.setattr(fs, 'get_sys_ids_with_fgp_editions', lambda: {'3', '4'})  # FGP Digital Edition

    pgp, edition = genizah_app._get_catalog_filter_sets()
    assert pgp == {'1', '2'}
    assert edition == {'2', '3', '4'}  # union of PGP editions + FGP editions

    # Second call is served from cache — no re-query.
    genizah_app._get_catalog_filter_sets()
    assert calls['pgp'] == 1
    _reset_cache()


def test_reset_invalidates_cache_for_sidecar_update(monkeypatch):
    """A sidecar DB swap must drop the cached sets so the next filtered query
    recomputes against the new data (Codex review fix)."""
    import genizah_app
    import shared.document_service as ds
    import shared.fgp_service as fs
    _reset_cache()
    calls = {'n': 0}

    def _pgp():
        calls['n'] += 1
        return {'1'}

    monkeypatch.setattr(ds, 'get_all_pgp_link_sys_ids', _pgp)
    monkeypatch.setattr(ds, 'get_sys_ids_with_editions', lambda: [])
    monkeypatch.setattr(fs, 'get_sys_ids_with_fgp_editions', lambda: set())

    genizah_app._get_catalog_filter_sets()        # computes (call 1)
    genizah_app._get_catalog_filter_sets()        # cached (no new call)
    assert calls['n'] == 1
    genizah_app.reset_catalog_filter_sets()       # sidecar swap
    genizah_app._get_catalog_filter_sets()        # recomputes (call 2)
    assert calls['n'] == 2
    _reset_cache()


def _run_worker(monkeypatch, *, pgp_filter, editions_filter):
    """Run _CatalogRefreshWorker.run() with a mocked FJMS + filter sets; return
    the kwargs the worker passed to get_browse_results."""
    import genizah_app
    import shared.fjms_service as fjms_mod

    captured = {}
    fake = MagicMock()
    fake.get_browse_results.side_effect = lambda *a, **k: captured.update(k) or {'results': [], 'total': 0}
    monkeypatch.setattr(fjms_mod, 'get_fjms_service', lambda **_: fake)
    monkeypatch.setattr(genizah_app, '_get_catalog_filter_sets', lambda: ({'P'}, {'E'}))

    w = genizah_app._CatalogRefreshWorker(
        None, domain=None, author=None, work=None, offset=0, limit=50,
        refresh_authors=False, refresh_works=False,
        pgp_filter=pgp_filter, editions_filter=editions_filter,
    )
    w.run()  # synchronous
    return captured


def test_worker_threads_filters_when_active(monkeypatch):
    k = _run_worker(monkeypatch, pgp_filter='has_pgp', editions_filter='no_edition')
    assert k['pgp_filter'] == 'has_pgp'
    assert k['pgp_sys_ids'] == {'P'}
    assert k['editions_filter'] == 'no_edition'
    assert k['edition_sys_ids'] == {'E'}


def test_worker_omits_filters_when_all(monkeypatch):
    # When both are 'all', the sets are NOT resolved and the kwargs are None.
    import genizah_app
    monkeypatch.setattr(
        genizah_app, '_get_catalog_filter_sets',
        lambda: (_ for _ in ()).throw(AssertionError('must not resolve sets when inactive')),
    )
    import shared.fjms_service as fjms_mod
    captured = {}
    fake = MagicMock()
    fake.get_browse_results.side_effect = lambda *a, **k: captured.update(k) or {'results': [], 'total': 0}
    monkeypatch.setattr(fjms_mod, 'get_fjms_service', lambda **_: fake)
    w = genizah_app._CatalogRefreshWorker(
        None, domain=None, author=None, work=None, offset=0, limit=50,
        refresh_authors=False, refresh_works=False,
        pgp_filter='all', editions_filter='all',
    )
    w.run()
    assert captured['pgp_filter'] is None
    assert captured['pgp_sys_ids'] is None
    assert captured['editions_filter'] is None
    assert captured['edition_sys_ids'] is None
