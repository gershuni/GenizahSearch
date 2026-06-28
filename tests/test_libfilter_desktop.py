"""SEED-026 desktop parity — catalog 'Browse by Identification' library filter.

Pins the new desktop-side wiring for LIBFILTER-03:
  * ``_CatalogRefreshWorker`` accepts ``library_filter`` (list[str]) AND an
    explicit ``meta_mgr`` ctor arg (OQ-2 resolution).
  * ``resolve_library_sys_ids`` is called INSIDE ``run()`` on the worker thread,
    never on the UI thread.
  * Empty / None ``library_filter`` passes ``library_codes=None`` and
    ``library_sys_ids=None`` to ``get_browse_results`` (Pitfall 5).

GUI-marked (constructs QApplication for the QThread worker); runs in gui-tests.
"""

from unittest.mock import MagicMock, patch

from PyQt6.QtWidgets import QApplication

_app = QApplication.instance() or QApplication([])


def _make_fake_meta_mgr(library_code: str = "CUL", sys_ids: list[str] | None = None):
    """Return a minimal fake MetadataManager exposing ``csv_bank``."""
    if sys_ids is None:
        sys_ids = ["990001"]
    fake = MagicMock()
    fake.csv_bank = {sid: {"library_code": library_code} for sid in sys_ids}
    return fake


def _run_worker_with_library_filter(monkeypatch, *, library_filter, meta_mgr=None):
    """Construct and run _CatalogRefreshWorker synchronously; return captured kwargs."""
    import genizah_app
    import shared.fjms_service as fjms_mod

    captured = {}
    fake_fjms = MagicMock()
    fake_fjms.get_browse_results.side_effect = (
        lambda *a, **k: captured.update(k) or {"results": [], "total": 0}
    )
    monkeypatch.setattr(fjms_mod, "get_fjms_service", lambda **_: fake_fjms)
    # PGP/editions sets are unused in these tests; monkeypatch to avoid DB calls.
    monkeypatch.setattr(
        genizah_app, "_get_catalog_filter_sets", lambda: (set(), set())
    )

    if meta_mgr is None:
        meta_mgr = _make_fake_meta_mgr()

    w = genizah_app._CatalogRefreshWorker(
        None,  # parent=None — QThread.__init__(None) is valid; fake parent raises
        domain=None,
        author=None,
        work=None,
        offset=0,
        limit=50,
        refresh_authors=False,
        refresh_works=False,
        pgp_filter="all",
        editions_filter="all",
        library_filter=library_filter,
        meta_mgr=meta_mgr,
    )
    w.run()  # synchronous; no QThread.start()
    return captured


def test_worker_threads_library_filter_into_get_browse_results(monkeypatch):
    """LIBFILTER-03: worker passes library_codes + resolved library_sys_ids to get_browse_results."""
    fake_meta = _make_fake_meta_mgr(library_code="CUL", sys_ids=["990001", "990002"])
    captured = _run_worker_with_library_filter(
        monkeypatch, library_filter=["CUL"], meta_mgr=fake_meta
    )

    assert captured.get("library_codes") == ["CUL"], (
        f"Expected library_codes=['CUL'], got {captured.get('library_codes')!r}"
    )
    lib_sys_ids = captured.get("library_sys_ids")
    assert lib_sys_ids is not None, "library_sys_ids must not be None when filter is active"
    assert "990001" in lib_sys_ids
    assert "990002" in lib_sys_ids


def test_worker_empty_library_filter_passes_none(monkeypatch):
    """Pitfall 5: empty library_filter -> library_codes=None, library_sys_ids=None (no 0-row filter)."""
    for empty_val in ([], None):
        captured = _run_worker_with_library_filter(
            monkeypatch, library_filter=empty_val
        )
        assert captured.get("library_codes") is None, (
            f"library_codes must be None for filter={empty_val!r}, "
            f"got {captured.get('library_codes')!r}"
        )
        assert captured.get("library_sys_ids") is None, (
            f"library_sys_ids must be None for filter={empty_val!r}, "
            f"got {captured.get('library_sys_ids')!r}"
        )


def test_worker_resolution_uses_shared_helper(monkeypatch):
    """OQ-2: resolution runs via resolve_library_sys_ids called on the worker thread (inside run())."""
    import genizah_app
    import shared.fjms_service as fjms_mod
    import shared.fjms_service as fjms_service_mod

    resolved_calls = []

    def _fake_resolve(library_codes, meta_mgr):
        resolved_calls.append({"library_codes": library_codes, "meta_mgr": meta_mgr})
        return {"999-fake-sid"}

    # Patch resolve_library_sys_ids in the module where it is looked up from run().
    monkeypatch.setattr(fjms_service_mod, "resolve_library_sys_ids", _fake_resolve)

    captured = {}
    fake_fjms = MagicMock()
    fake_fjms.get_browse_results.side_effect = (
        lambda *a, **k: captured.update(k) or {"results": [], "total": 0}
    )
    monkeypatch.setattr(fjms_mod, "get_fjms_service", lambda **_: fake_fjms)
    monkeypatch.setattr(
        genizah_app, "_get_catalog_filter_sets", lambda: (set(), set())
    )

    fake_meta = _make_fake_meta_mgr()
    w = genizah_app._CatalogRefreshWorker(
        None,
        domain=None,
        author=None,
        work=None,
        offset=0,
        limit=50,
        refresh_authors=False,
        refresh_works=False,
        pgp_filter="all",
        editions_filter="all",
        library_filter=["CUL"],
        meta_mgr=fake_meta,
    )
    w.run()

    assert len(resolved_calls) == 1, (
        f"resolve_library_sys_ids must be called once from run(); calls={resolved_calls}"
    )
    assert resolved_calls[0]["library_codes"] == ["CUL"]
    assert resolved_calls[0]["meta_mgr"] is fake_meta

    # The resolved set must flow through to get_browse_results
    assert captured.get("library_sys_ids") == {"999-fake-sid"}
