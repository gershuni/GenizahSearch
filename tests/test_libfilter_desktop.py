"""SEED-026 desktop parity — catalog 'Browse by Identification' library filter.

Pins the new desktop-side wiring for LIBFILTER-03:
  * ``_CatalogRefreshWorker`` accepts ``library_filter`` (list[str]) AND an
    explicit ``meta_mgr`` ctor arg (OQ-2 resolution).
  * ``resolve_library_sys_ids`` is called INSIDE ``run()`` on the worker thread,
    never on the UI thread.
  * Empty / None ``library_filter`` passes ``library_codes=None`` and
    ``library_sys_ids=None`` to ``get_browse_results`` (Pitfall 5).

GAP-G / GAP-H / FINDING 1 / FINDING 2 (129-07 gap-closure) — additional tests:
  * ``LibraryFilterDialog`` is a checkbox QDialog (no QMenu/QAction): importable
    from desktop.dialogs_filter, LOCAL not offered, OK guarded when zero checked.
  * ``library_apply_selection`` maps all-checked -> [] (show-all sentinel) and
    subset -> that subset.
  * ``_catalog_build_browse_filters`` populates filters['library'] when active.
  * Search-within and parallels-within narrow pre_search_restrict_sys_ids by
    resolve_library_sys_ids intersection.
  * ``FilterCountWorker`` (with meta_mgr) intersects filters['library'] into the
    recomputed set so chip-removal does not drop the library restriction.

GUI-marked (constructs QApplication for the QThread worker); runs in gui-tests.
"""

import pytest
from unittest.mock import MagicMock, patch

from PyQt6.QtWidgets import QApplication, QDialogButtonBox

import shared.fjms_service as _fjms_mod

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


# ── GAP-G: LibraryFilterDialog ─────────────────────────────────────────────

@pytest.mark.gui
def test_library_filter_dialog_importable():
    """GAP-G: LibraryFilterDialog is importable from desktop.dialogs_filter."""
    from desktop.dialogs_filter import LibraryFilterDialog  # noqa: F401


@pytest.mark.gui
def test_library_filter_dialog_local_not_offered():
    """GAP-G: LOCAL ('My Library') is never an option in LibraryFilterDialog."""
    from desktop.dialogs_filter import LibraryFilterDialog
    from genizah_core import LIBRARY_CODES

    dlg = LibraryFilterDialog(selected_codes=[])
    codes_in_list = []
    for i in range(dlg.list_widget.count()):
        item = dlg.list_widget.item(i)
        codes_in_list.append(item.data(256))  # Qt.ItemDataRole.UserRole = 256

    assert 'LOCAL' not in codes_in_list, "LOCAL must never appear in LibraryFilterDialog"
    # At least a few library codes must be offered
    assert len(codes_in_list) > 0, "LibraryFilterDialog must offer at least one library"

    # All library codes except LOCAL should be present
    expected_codes = {c for c in LIBRARY_CODES.keys() if c != 'LOCAL'}
    assert expected_codes == set(codes_in_list), (
        f"LibraryFilterDialog codes differ from LIBRARY_CODES-LOCAL.\n"
        f"Missing: {expected_codes - set(codes_in_list)}\n"
        f"Extra: {set(codes_in_list) - expected_codes}"
    )


@pytest.mark.gui
def test_library_filter_dialog_get_checked_codes():
    """GAP-G: get_checked_codes() returns list of checked codes."""
    from desktop.dialogs_filter import LibraryFilterDialog

    # All checked by default (empty selected_codes = all included)
    dlg = LibraryFilterDialog(selected_codes=[])
    codes = dlg.get_checked_codes()
    assert isinstance(codes, list), "get_checked_codes() must return a list"
    assert len(codes) > 0, "All-checked dialog must return non-empty codes"
    assert 'LOCAL' not in codes, "LOCAL must never be in get_checked_codes()"

    # Subset: only CUL checked
    dlg2 = LibraryFilterDialog(selected_codes=['CUL'])
    codes2 = dlg2.get_checked_codes()
    assert codes2 == ['CUL'], f"Expected ['CUL'], got {codes2!r}"


@pytest.mark.gui
def test_library_filter_dialog_no_select_none():
    """FINDING 1: LibraryFilterDialog must NOT provide a 'Select None'/deselect-all button."""
    from desktop.dialogs_filter import LibraryFilterDialog
    from PyQt6.QtWidgets import QPushButton

    dlg = LibraryFilterDialog(selected_codes=[])
    # Collect all button texts
    btn_texts = [
        b.text().lower()
        for b in dlg.findChildren(QPushButton)
    ]
    # "select none" or "deselect all" or "none" must not appear
    for text in btn_texts:
        assert 'none' not in text, (
            f"LibraryFilterDialog must NOT have a 'Select None' button; found: {text!r}"
        )
        assert 'deselect' not in text, (
            f"LibraryFilterDialog must NOT have a 'Deselect All' button; found: {text!r}"
        )


@pytest.mark.gui
def test_library_filter_dialog_ok_disabled_when_zero_checked():
    """FINDING 1 guard: OK button must be disabled when zero items are checked."""
    from desktop.dialogs_filter import LibraryFilterDialog
    from PyQt6.QtCore import Qt

    dlg = LibraryFilterDialog(selected_codes=[])
    # Uncheck all items programmatically
    for i in range(dlg.list_widget.count()):
        dlg.list_widget.item(i).setCheckState(Qt.CheckState.Unchecked)

    # The OK button must be disabled (or accept() must be a no-op — check both)
    assert not dlg.ok_button.isEnabled(), (
        "OK button must be disabled when zero libraries are checked"
    )


@pytest.mark.gui
def test_library_apply_selection_all_checked_returns_empty():
    """GAP-G mapping: all-checked => [] (show-all sentinel); subset => subset list."""
    from desktop.dialogs_filter import library_apply_selection
    from genizah_core import LIBRARY_CODES

    all_codes = [c for c in LIBRARY_CODES.keys() if c != 'LOCAL']

    # All checked -> clear filter (show all)
    result = library_apply_selection(all_codes, all_codes)
    assert result == [], (
        f"library_apply_selection(all, all) must return [] (show-all sentinel), got {result!r}"
    )

    # Strict subset -> that subset
    subset = ['CUL', 'JTS']
    result2 = library_apply_selection(subset, all_codes)
    assert set(result2) == {'CUL', 'JTS'}, (
        f"library_apply_selection(subset, all) must return the subset, got {result2!r}"
    )

    # Never called with empty (guard prevents it), but verify no crash
    # (the plan says library_apply_selection is only ever called with non-empty checked)


# ── GAP-H: search-within threading ────────────────────────────────────────

@pytest.mark.gui
def test_catalog_build_browse_filters_includes_library(monkeypatch):
    """GAP-H: _catalog_build_browse_filters adds filters['library'] when active."""
    import genizah_app

    # Build a minimal fake instance with _catalog_library_filter set
    fake_self = MagicMock()
    fake_self._catalog_library_filter = ['CUL', 'JTS']
    fake_self._catalog_current_domain = None
    fake_self._catalog_current_author = None
    fake_self._catalog_current_work = None
    fake_self._catalog_date_from = None
    fake_self._catalog_date_to = None

    result = genizah_app.GenizahGUI._catalog_build_browse_filters(fake_self)
    assert 'library' in result, (
        f"_catalog_build_browse_filters must include 'library' when _catalog_library_filter is set; got {result!r}"
    )
    assert set(result['library']) == {'CUL', 'JTS'}, (
        f"filters['library'] must be the active codes; got {result['library']!r}"
    )


@pytest.mark.gui
def test_catalog_build_browse_filters_omits_library_when_empty(monkeypatch):
    """GAP-H: _catalog_build_browse_filters omits 'library' when filter is empty (show all)."""
    import genizah_app

    fake_self = MagicMock()
    fake_self._catalog_library_filter = []
    fake_self._catalog_current_domain = None
    fake_self._catalog_current_author = None
    fake_self._catalog_current_work = None
    fake_self._catalog_date_from = None
    fake_self._catalog_date_to = None

    result = genizah_app.GenizahGUI._catalog_build_browse_filters(fake_self)
    assert 'library' not in result, (
        f"_catalog_build_browse_filters must NOT include 'library' when filter is empty; got {result!r}"
    )


# ── FINDING 2: recompute-preserves-library ─────────────────────────────────

@pytest.mark.gui
def test_filter_count_worker_intersects_library_with_meta_mgr(monkeypatch):
    """FINDING 2: FilterCountWorker with meta_mgr intersects filters['library'] into result set."""
    from gui_threads import FilterCountWorker

    # CUL sys_ids (smaller set)
    cul_sys_ids = {'990001', '990002'}
    # fjms returns a broader set that includes some non-CUL IDs
    broader_set = {'990001', '990002', '990003', '990099'}

    fake_meta = _make_fake_meta_mgr(library_code='CUL', sys_ids=list(cul_sys_ids))

    def _fake_resolve(library_codes, meta_mgr):
        return cul_sys_ids

    def _fake_get_filter_sys_ids(**kwargs):
        return broader_set

    fake_fjms_instance = MagicMock()
    fake_fjms_instance.is_available.return_value = True
    fake_fjms_instance.get_filter_sys_ids.side_effect = _fake_get_filter_sys_ids

    # Patch at the source module so the local import inside run() picks it up
    monkeypatch.setattr(_fjms_mod, 'resolve_library_sys_ids', _fake_resolve)

    emitted = []
    filters = {'library': ['CUL'], 'domains': ['some_domain']}
    worker = FilterCountWorker(filters, meta_mgr=fake_meta)

    # Patch FjmsService in shared.fjms_service (where it's imported from inside run())
    with patch('shared.fjms_service.FjmsService', return_value=fake_fjms_instance):
        worker.finished.connect(emitted.append)
        worker.run()

    assert len(emitted) == 1, "FilterCountWorker.run() must emit exactly once"
    result = emitted[0]
    assert result is not None, "FilterCountWorker must emit a non-None result set"
    # The intersection must limit to CUL-only sys_ids
    assert result == cul_sys_ids, (
        f"Recomputed set must be intersection of fjms result and library sys_ids.\n"
        f"Expected: {cul_sys_ids}\nGot: {result!r}"
    )


@pytest.mark.gui
def test_filter_count_worker_no_meta_mgr_is_safe_noop(monkeypatch):
    """FINDING 2 (no-meta_mgr path): FilterCountWorker without meta_mgr must not raise."""
    from gui_threads import FilterCountWorker

    broader_set = {'990001', '990002', '990003'}

    def _fake_get_filter_sys_ids(**kwargs):
        return broader_set

    fake_fjms_instance = MagicMock()
    fake_fjms_instance.is_available.return_value = True
    fake_fjms_instance.get_filter_sys_ids.side_effect = _fake_get_filter_sys_ids

    emitted = []
    filters = {'library': ['CUL']}
    # meta_mgr defaults to None -> library intersection must be a no-op
    worker = FilterCountWorker(filters)

    with patch('shared.fjms_service.FjmsService', return_value=fake_fjms_instance):
        worker.finished.connect(emitted.append)
        worker.run()

    assert len(emitted) == 1, "FilterCountWorker.run() must emit exactly once"
    result = emitted[0]
    # No meta_mgr -> no intersection -> result is whatever fjms returned (the broader set)
    # Must not be an exception and must not crash
    assert result == broader_set, (
        f"Without meta_mgr the library intersection must be a no-op; "
        f"expected {broader_set}, got {result!r}"
    )
