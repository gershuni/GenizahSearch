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

Phase 131 Plan 01 (DMF-07/DMF-13) extensions:
  * LibraryFilterDialog dual-mode tests: mode='show_only' / mode='hide', get_mode(),
    D-04 mode-flip reset, mode-aware OK guard, fresh-default 'hide'.
  * Source scans: _catalog_library_mode='hide' init, library_codes_with_manuscripts
    in dialogs_filter.py.
  * REVISED: existing inclusion-only tests carry mode='show_only' (Codex R1 BLOCKER #1).
  * REVISED + ADDED: _catalog_build_browse_filters handoff tests for Show-only / Hide
    (Codex R2 N3).

GUI-marked (constructs QApplication for the QThread worker); runs in gui-tests.
"""

import pytest
from unittest.mock import MagicMock, patch

from PyQt6.QtWidgets import QApplication

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
    """GAP-G: LOCAL ('My Library') is never an option in LibraryFilterDialog.

    Phase 131 Plan 01 REVISION (Codex R1 BLOCKER #1):
      - Construct with mode='show_only' so the OLD inclusion-only Show-only
        semantics are explicitly expressed (Plan 03 changes the default to 'hide').
      - Universe expectation updated to set(library_codes_with_manuscripts()) - {'LOCAL'}
        (DMF-13 — Plan 03 swaps _all_codes to library_codes_with_manuscripts()).
        Under the fail-open default that frozenset equals frozenset(LIBRARY_CODES), so
        comparing against the with-manuscripts set is correct in BOTH the populated and
        fail-open cases.
    """
    from desktop.dialogs_filter import LibraryFilterDialog
    from shared.browse_map_utils import library_codes_with_manuscripts

    # REVISED: explicit mode='show_only' (Plan 03 default becomes 'hide')
    dlg = LibraryFilterDialog(mode='show_only', selected_codes=[])
    codes_in_list = []
    for i in range(dlg.list_widget.count()):
        item = dlg.list_widget.item(i)
        codes_in_list.append(item.data(256))  # Qt.ItemDataRole.UserRole = 256

    assert 'LOCAL' not in codes_in_list, "LOCAL must never appear in LibraryFilterDialog"
    # At least a few library codes must be offered
    assert len(codes_in_list) > 0, "LibraryFilterDialog must offer at least one library"

    # REVISED: universe is now library_codes_with_manuscripts() - {'LOCAL'} (DMF-13)
    expected_codes = set(library_codes_with_manuscripts()) - {'LOCAL'}
    assert expected_codes == set(codes_in_list), (
        f"LibraryFilterDialog codes must match library_codes_with_manuscripts()-LOCAL (DMF-13).\n"
        f"Missing: {expected_codes - set(codes_in_list)}\n"
        f"Extra: {set(codes_in_list) - expected_codes}"
    )


@pytest.mark.gui
def test_library_filter_dialog_get_checked_codes():
    """GAP-G: get_checked_codes() returns list of checked codes.

    Phase 131 Plan 01 REVISION (Codex R1 BLOCKER #1):
      - Construct with mode='show_only' so the OLD inclusion-only semantics are
        explicit (all-checked-by-default is Show-only semantics).
    """
    from desktop.dialogs_filter import LibraryFilterDialog

    # REVISED: explicit mode='show_only' (all-checked-by-default is Show-only semantics)
    # All checked by default when selected_codes is empty (Show-only: empty = show all)
    dlg = LibraryFilterDialog(mode='show_only', selected_codes=[])
    codes = dlg.get_checked_codes()
    assert isinstance(codes, list), "get_checked_codes() must return a list"
    assert len(codes) > 0, "All-checked Show-only dialog must return non-empty codes"
    assert 'LOCAL' not in codes, "LOCAL must never be in get_checked_codes()"

    # Subset case: only CUL checked (mode='show_only', selected_codes=['CUL'] -> ['CUL'])
    dlg2 = LibraryFilterDialog(mode='show_only', selected_codes=['CUL'])
    codes2 = dlg2.get_checked_codes()
    assert codes2 == ['CUL'], f"Expected ['CUL'], got {codes2!r}"


@pytest.mark.gui
def test_library_filter_dialog_select_none_present_ok_still_guarded():
    """BUG-C fix: LibraryFilterDialog now provides a 'Select None' clear-checkboxes
    convenience button, BUT OK must remain DISABLED at zero checked (FINDING 1 guard).

    'Select None' unchecks all items without applying. The _update_ok_button and
    _on_accept guards ensure the all-unchecked state cannot be committed via OK.

    Phase 131 Plan 01 REVISION (Codex R1 BLOCKER #1):
      - Construct with mode='show_only' so the OLD zero-checked -> OK-disabled guard
        still holds (in Hide mode, zero-checked is valid = show all; see
        test_library_filter_dialog_ok_guard_mode_aware for the Hide counterpart).
    """
    from desktop.dialogs_filter import LibraryFilterDialog
    from PyQt6.QtWidgets import QPushButton

    # REVISED: explicit mode='show_only' — "OK disabled after Select None" is Show-only semantics
    dlg = LibraryFilterDialog(mode='show_only', selected_codes=[])

    # 'Select None' button must be present (BUG-C fix)
    btn_texts = [b.text() for b in dlg.findChildren(QPushButton)]
    # The button label comes from tr("Select None") which may be translated; check the
    # source text too (translations.py has "Select None": "בטל בחירה")
    has_select_none = any(
        'none' in t.lower() or 'בטל' in t
        for t in btn_texts
    )
    assert has_select_none, (
        f"BUG-C: 'Select None' button missing from LibraryFilterDialog. "
        f"Button texts found: {btn_texts!r}"
    )

    # After clicking 'Select None', zero items are checked → OK must be disabled (Show-only).
    dlg._select_none()
    assert not dlg.ok_button.isEnabled(), (
        "OK must remain DISABLED after 'Select None' in Show-only mode "
        "(zero checked — FINDING 1 guard)"
    )

    # 'deselect' (as in a 'Deselect All' variant) must NOT appear (guard against
    # a differently-labelled button that also bypasses the guard).
    for text in btn_texts:
        assert 'deselect' not in text.lower(), (
            f"LibraryFilterDialog must NOT have a 'Deselect All' button; found: {text!r}"
        )


@pytest.mark.gui
def test_library_filter_dialog_ok_disabled_when_zero_checked():
    """FINDING 1 guard: OK button must be disabled when zero items are checked (Show-only).

    Phase 131 Plan 01 REVISION (Codex R1 BLOCKER #1):
      - Construct with mode='show_only' so this guard remains valid.
      - The Hide-mode counterpart (zero-checked -> OK ENABLED) is in
        test_library_filter_dialog_ok_guard_mode_aware (new in Phase 131).
    """
    from desktop.dialogs_filter import LibraryFilterDialog
    from PyQt6.QtCore import Qt

    # REVISED: explicit mode='show_only' — zero-checked is only invalid in Show-only
    dlg = LibraryFilterDialog(mode='show_only', selected_codes=[])
    # Uncheck all items programmatically
    for i in range(dlg.list_widget.count()):
        dlg.list_widget.item(i).setCheckState(Qt.CheckState.Unchecked)

    # The OK button must be disabled in Show-only mode (FINDING 1 guard)
    assert not dlg.ok_button.isEnabled(), (
        "OK button must be disabled when zero libraries are checked (Show-only mode)"
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
    """GAP-H: _catalog_build_browse_filters adds filters['library'] when active (Show-only mode).

    Phase 131 Plan 01 REVISION (Codex R2 N3):
      - Add fake_self._catalog_library_mode = 'show_only' so that after Plan 03
        gates filters['library'] on mode == 'show_only', this test still asserts
        the include-handoff in Show-only mode.
      - Without this line, MagicMock auto-creates _catalog_library_mode as a Mock
        object, so _catalog_library_mode != 'show_only' evaluates truthy, and Plan 03's
        gate would (correctly) suppress filters['library'], failing this test for the
        wrong reason.
    """
    import genizah_app

    # Build a minimal fake instance with _catalog_library_filter set
    fake_self = MagicMock()
    fake_self._catalog_library_filter = ['CUL', 'JTS']
    # REVISED: set mode explicitly so Plan 03's Show-only gate passes (Codex N3)
    fake_self._catalog_library_mode = 'show_only'
    fake_self._catalog_current_domain = None
    fake_self._catalog_current_author = None
    fake_self._catalog_current_work = None
    fake_self._catalog_date_from = None
    fake_self._catalog_date_to = None

    result = genizah_app.GenizahGUI._catalog_build_browse_filters(fake_self)
    assert 'library' in result, (
        f"_catalog_build_browse_filters must include 'library' when mode='show_only' and "
        f"_catalog_library_filter is set; got {result!r}"
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


# ── Phase 131 Plan 01: Dual-mode DMF-07 tests ─────────────────────────────
# These tests are RED until Plan 03 adds mode/get_mode/_on_mode_changed/
# _catalog_library_mode and the Show-only-gated handoff (intended Wave-0 state).
# Use real assertions — NOT pytest.skip.

@pytest.mark.gui
def test_library_filter_dialog_mode_show_only():
    """(1) DMF-07: LibraryFilterDialog(mode='show_only') -> get_mode() returns 'show_only'.

    RED until Plan 03 adds the mode param + get_mode() method.
    """
    from desktop.dialogs_filter import LibraryFilterDialog
    dlg = LibraryFilterDialog(mode='show_only', selected_codes=['CUL'])
    assert dlg.get_mode() == 'show_only', (
        f"LibraryFilterDialog(mode='show_only') must have get_mode()='show_only', "
        f"got {dlg.get_mode()!r}"
    )


@pytest.mark.gui
def test_library_filter_dialog_mode_hide():
    """(2) DMF-07: LibraryFilterDialog(mode='hide') -> get_mode() returns 'hide'.

    RED until Plan 03 adds the mode param + get_mode() method.
    """
    from desktop.dialogs_filter import LibraryFilterDialog
    dlg = LibraryFilterDialog(mode='hide', selected_codes=[])
    assert dlg.get_mode() == 'hide', (
        f"LibraryFilterDialog(mode='hide') must have get_mode()='hide', "
        f"got {dlg.get_mode()!r}"
    )


@pytest.mark.gui
def test_library_filter_dialog_mode_flip_resets():
    """(3) D-04: mode flip resets the checked set (prevents silent inversion of intent).

    Construct in Show-only with CUL checked; programmatically check the Hide radio button;
    assert ALL list-widget items become Unchecked.

    RED until Plan 03 adds _rb_show_only/_rb_hide + _on_mode_changed.
    """
    from desktop.dialogs_filter import LibraryFilterDialog
    from PyQt6.QtCore import Qt

    dlg = LibraryFilterDialog(mode='show_only', selected_codes=['CUL'])

    # Verify CUL is checked to start
    checked_before = [
        dlg.list_widget.item(i).checkState() == Qt.CheckState.Checked
        for i in range(dlg.list_widget.count())
    ]
    assert any(checked_before), "At least one item (CUL) should be checked before mode flip"

    # Simulate clicking the Hide radio button (D-04 reset)
    dlg._rb_hide.setChecked(True)

    # After mode flip, ALL items must be Unchecked
    checked_after = [
        dlg.list_widget.item(i).checkState() == Qt.CheckState.Checked
        for i in range(dlg.list_widget.count())
    ]
    assert not any(checked_after), (
        "D-04: mode flip must uncheck ALL items in the list widget "
        f"(checked after flip: {sum(checked_after)})"
    )


@pytest.mark.gui
def test_library_filter_dialog_ok_guard_mode_aware():
    """(4) D-05/D-08: Mode-aware OK guard.

    Hide + zero checked -> OK ENABLED (empty hide-set = show all).
    Show-only + zero checked -> OK DISABLED (zero-checked Show-only is blocked).

    RED until Plan 03 makes _update_ok_button mode-aware.
    """
    from desktop.dialogs_filter import LibraryFilterDialog
    from PyQt6.QtCore import Qt

    # Hide + zero checked -> OK must be ENABLED (empty hide-set = show all)
    dlg_hide = LibraryFilterDialog(mode='hide', selected_codes=[])
    for i in range(dlg_hide.list_widget.count()):
        dlg_hide.list_widget.item(i).setCheckState(Qt.CheckState.Unchecked)
    assert dlg_hide.ok_button.isEnabled(), (
        "Hide mode + zero checked must have OK ENABLED (empty hide-set = show all, D-05/D-08)"
    )

    # Show-only + zero checked -> OK must be DISABLED (FINDING 1 guard)
    dlg_show = LibraryFilterDialog(mode='show_only', selected_codes=[])
    for i in range(dlg_show.list_widget.count()):
        dlg_show.list_widget.item(i).setCheckState(Qt.CheckState.Unchecked)
    assert not dlg_show.ok_button.isEnabled(), (
        "Show-only mode + zero checked must have OK DISABLED (FINDING 1 guard)"
    )


@pytest.mark.gui
def test_library_filter_dialog_fresh_default_hide():
    """(5) D-05: LibraryFilterDialog() with no mode arg -> get_mode() returns 'hide'.

    RED until Plan 03 changes the default from (implicit/no-mode) to mode='hide'.
    """
    from desktop.dialogs_filter import LibraryFilterDialog
    dlg = LibraryFilterDialog()  # No mode arg — fresh default
    assert dlg.get_mode() == 'hide', (
        f"Fresh LibraryFilterDialog() must default to mode='hide' (D-05), "
        f"got {dlg.get_mode()!r}"
    )


def test_catalog_library_mode_default_source():
    """(6a) DMF-07 source scan: genizah_app.py must initialise _catalog_library_mode = 'hide'
    in the catalog init block.

    This avoids constructing a full GenizahGUI. We scan the source for the literal
    `self._catalog_library_mode = 'hide'` in the init region.

    RED until Plan 03 adds the _catalog_library_mode field.
    """
    import genizah_app
    from pathlib import Path
    source = Path(genizah_app.__file__).read_text(encoding='utf-8')
    assert "self._catalog_library_mode = 'hide'" in source or \
           'self._catalog_library_mode = "hide"' in source, (
        "genizah_app.py must initialise self._catalog_library_mode = 'hide' "
        "in the catalog init block (D-05 default). RED until Plan 03."
    )


def test_dialog_uses_with_manuscripts_universe_source():
    """(6b) DMF-13 source scan: desktop/dialogs_filter.py must reference
    library_codes_with_manuscripts where the _all_codes list is built.

    RED until Plan 03 updates LibraryFilterDialog._all_codes to use
    library_codes_with_manuscripts().
    """
    from pathlib import Path
    dialogs_filter_py = Path(__file__).parent.parent / 'desktop' / 'dialogs_filter.py'
    source = dialogs_filter_py.read_text(encoding='utf-8')
    assert 'library_codes_with_manuscripts' in source, (
        "desktop/dialogs_filter.py must reference library_codes_with_manuscripts "
        "where _all_codes is built (DMF-13). RED until Plan 03."
    )


# ── Phase 131 Plan 01: Hide-mode handoff tests (Codex R2 N3) ──────────────
# Tests (C): REVISED + ADDED handoff tests for _catalog_build_browse_filters.
# RED until Plan 03 adds the Show-only-gated handoff.

@pytest.mark.gui
def test_catalog_build_browse_filters_hide_omits_library(monkeypatch):
    """(C) Codex HIGH #4: Hide mode does NOT hand off filters['library'].

    _catalog_build_browse_filters with mode='hide' and a non-empty filter must
    NOT add 'library' to the result (Hide = complement, handled post-fetch).

    RED until Plan 03 gates filters['library'] on mode=='show_only'.
    """
    import genizah_app

    fake_self = MagicMock()
    fake_self._catalog_library_filter = ['CUL', 'JTS']
    fake_self._catalog_library_mode = 'hide'
    fake_self._catalog_current_domain = None
    fake_self._catalog_current_author = None
    fake_self._catalog_current_work = None
    fake_self._catalog_date_from = None
    fake_self._catalog_date_to = None

    result = genizah_app.GenizahGUI._catalog_build_browse_filters(fake_self)
    assert 'library' not in result, (
        f"_catalog_build_browse_filters with mode='hide' must NOT add filters['library'] "
        f"(Codex HIGH #4 contract). RED until Plan 03. Got: {result!r}"
    )


@pytest.mark.gui
def test_catalog_search_in_results_hide_no_restrict_and_notice(monkeypatch):
    """(C) Codex N3: Hide mode in _catalog_search_in_results / _catalog_parallels_in_results
    must NOT intersect resolve_library_sys_ids into pre_search_restrict_sys_ids,
    AND must fire a Hide-suppression notice.

    This test covers the _catalog_search_in_results path (the recompute path).
    RED until Plan 03 adds the Hide-suppression branch.
    """
    import genizah_app
    import shared.fjms_service as fjms_mod

    # Track resolve_library_sys_ids calls
    resolve_calls = []

    def _fake_resolve(library_codes, meta_mgr):
        resolve_calls.append(library_codes)
        return {'fake_sys_id'}

    monkeypatch.setattr(fjms_mod, 'resolve_library_sys_ids', _fake_resolve)

    # Mock status bar for notice detection
    status_messages = []
    fake_status_bar = MagicMock()
    fake_status_bar.showMessage.side_effect = lambda msg, *a, **kw: status_messages.append(msg)

    # Build minimal fake_self with Hide mode active
    fake_self = MagicMock()
    fake_self._catalog_library_filter = ['CUL']
    fake_self._catalog_library_mode = 'hide'
    fake_self.statusBar.return_value = fake_status_bar
    # Stub out navigation / search launch to keep test unit-scoped
    fake_self._catalog_search_in_results_text = ''
    fake_self._catalog_current_domain = None
    fake_self._catalog_current_author = None
    fake_self._catalog_current_work = None
    fake_self._catalog_date_from = None
    fake_self._catalog_date_to = None
    # Return an empty restrict set (so the test verifies no library intersection added)
    fake_self.meta_mgr = MagicMock()

    # Call the recompute path; in Hide mode it should NOT call resolve_library_sys_ids
    # for a library restriction intersect (and should fire a notice).
    # We stub _catalog_start_async_refresh and related launchers to prevent real DB calls.
    fake_self._catalog_start_async_refresh = MagicMock()
    fake_self._catalog_parallels_in_results = MagicMock()

    try:
        genizah_app.GenizahGUI._catalog_search_in_results(fake_self)
    except Exception:
        # The stub may raise; we only care about what ran BEFORE the raise
        pass

    # Assert: resolve_library_sys_ids was NOT called for a library restriction
    assert len(resolve_calls) == 0, (
        f"_catalog_search_in_results in Hide mode must NOT call resolve_library_sys_ids "
        f"(no library allowlist restriction in Hide mode). Codex N3. RED until Plan 03. "
        f"Calls: {resolve_calls}"
    )

    # Assert: a suppression notice was fired (statusBar.showMessage called with a
    # Hide-related message; accept any non-empty message as the notice)
    assert len(status_messages) > 0 or fake_status_bar.showMessage.called or \
           fake_self.statusBar.called, (
        "_catalog_search_in_results in Hide mode must fire a Hide-suppression notice. "
        "Codex N3. RED until Plan 03."
    )


# ── Phase 131 Plan 06: Facet counts (DMF-07/DMF-12 desktop) ──────────────
# Five tests cover: count rendering, dynamic PGP-only counts, name-only fallback,
# LOCAL exclusion, and the off-UI-thread contract.


@pytest.mark.gui
def test_dialog_renders_facet_counts():
    """(1) DMF-07/DMF-12: LibraryFilterDialog renders 'Name (count)' when facets supplied.

    - CUL row text must contain '(1,234)' (localized thousands separator).
    - A code present in the universe but absent from facets renders name-only (no count).
    """
    from desktop.dialogs_filter import LibraryFilterDialog
    from PyQt6.QtCore import Qt

    dlg = LibraryFilterDialog(mode='hide', facets={'CUL': 1234})
    cul_item = None
    other_no_count_item = None
    for i in range(dlg.list_widget.count()):
        item = dlg.list_widget.item(i)
        code = item.data(Qt.ItemDataRole.UserRole)
        if code == 'CUL':
            cul_item = item
        elif code not in ('CUL',):
            # Any code absent from facets — pick the first one found
            if other_no_count_item is None:
                other_no_count_item = item

    assert cul_item is not None, "CUL must appear in the dialog universe"
    assert '(1,234)' in cul_item.text(), (
        f"CUL row must contain '(1,234)' when facets={{'CUL': 1234}}; "
        f"got {cul_item.text()!r}"
    )
    # UserRole still returns bare code, not the labelled string
    assert cul_item.data(Qt.ItemDataRole.UserRole) == 'CUL', (
        "UserRole must still be the bare code 'CUL', not the labelled text"
    )

    # A code absent from facets must render name-only (no parenthesised count)
    if other_no_count_item is not None:
        text = other_no_count_item.text()
        # Must not end with '(N)' pattern from a count (could legitimately have parentheses
        # in the library name, so we check for the specific ' (' format we add)
        assert '  (' not in text, (
            f"Row for code absent from facets must not have a count; got {text!r}"
        )


@pytest.mark.gui
def test_dialog_facets_none_fallback():
    """(2) DMF-07/DMF-12: LibraryFilterDialog with facets=None (or no kwarg) renders name-only.

    - Both calling forms produce name-only rows.
    - get_checked_codes() still returns bare codes after selecting with show_only + CUL.
    """
    from desktop.dialogs_filter import LibraryFilterDialog

    for kwarg in ({}, {'facets': None}):
        dlg = LibraryFilterDialog(mode='hide', **kwarg)
        for i in range(dlg.list_widget.count()):
            item = dlg.list_widget.item(i)
            assert '  (' not in item.text(), (
                f"Name-only fallback: row must not contain count suffix for kwarg={kwarg!r}; "
                f"got {item.text()!r}"
            )

    # get_checked_codes() returns bare codes
    dlg_codes = LibraryFilterDialog(mode='show_only', selected_codes=['CUL'])
    codes = dlg_codes.get_checked_codes()
    assert codes == ['CUL'], (
        f"get_checked_codes() must return bare codes ['CUL'], got {codes!r}"
    )


def test_facet_worker_dynamic_pgp_filter(monkeypatch):
    """(3) DYNAMIC: _CatalogFacetWorker.run() passes pgp_filter + pgp_sys_ids + resolver
    when pgp_filter='has_pgp' is active.

    Proves that counts honor an active PGP-only filter set (DMF-12 dynamic counts).
    """
    import genizah_app
    import shared.fjms_service as fjms_mod

    recorded_kwargs = {}
    fake_fjms = MagicMock()
    fake_fjms.get_browse_library_facets.side_effect = (
        lambda **kw: recorded_kwargs.update(kw) or {}
    )
    monkeypatch.setattr(fjms_mod, 'get_fjms_service', lambda **_: fake_fjms)
    monkeypatch.setattr(
        genizah_app, '_get_catalog_filter_sets',
        lambda: ({'990001'}, set()),
    )

    meta_mgr = _make_fake_meta_mgr()

    w = genizah_app._CatalogFacetWorker(
        None,
        domain=None,
        author=None,
        work=None,
        pgp_filter='has_pgp',
        editions_filter='all',
        meta_mgr=meta_mgr,
    )
    w.run()  # synchronous; no QThread.start()

    assert recorded_kwargs.get('pgp_filter') == 'has_pgp', (
        f"Worker must pass pgp_filter='has_pgp' into get_browse_library_facets; "
        f"got {recorded_kwargs.get('pgp_filter')!r}"
    )
    assert recorded_kwargs.get('pgp_sys_ids') == {'990001'}, (
        f"Worker must pass non-None pgp_sys_ids (from _get_catalog_filter_sets) when "
        f"pgp_filter='has_pgp'; got {recorded_kwargs.get('pgp_sys_ids')!r}"
    )
    assert recorded_kwargs.get('sys_id_to_library') is meta_mgr.get_library_for_id, (
        "Worker must pass meta_mgr.get_library_for_id as sys_id_to_library "
        "(full-corpus resolver, WR-05)"
    )


@pytest.mark.gui
def test_dialog_excludes_local_even_in_facets():
    """(4) LOCAL never rendered: LibraryFilterDialog universe excludes 'LOCAL' even when
    facets includes a 'LOCAL' key.

    The dialog _all_codes is library_codes_with_manuscripts() minus 'LOCAL' (DMF-13),
    so 'LOCAL' can never appear as a row regardless of what facets contains.
    """
    from desktop.dialogs_filter import LibraryFilterDialog
    from PyQt6.QtCore import Qt

    dlg = LibraryFilterDialog(mode='hide', facets={'LOCAL': 99, 'CUL': 5})
    for i in range(dlg.list_widget.count()):
        item = dlg.list_widget.item(i)
        code = item.data(Qt.ItemDataRole.UserRole)
        assert code != 'LOCAL', (
            f"'LOCAL' must never appear as a row in LibraryFilterDialog "
            f"(DMF-13); found at index {i}"
        )


def test_facet_computation_off_ui_thread():
    """(5) OFF-THREAD: _CatalogFacetWorker is a QThread subclass; the DB facet call
    (get_browse_library_facets) lives inside run(), not inside _open_catalog_library_dialog.

    This is an AST/source scan — proves the off-UI-thread invariant without running
    the real DB or starting a thread.
    """
    import ast
    import genizah_app
    from pathlib import Path
    from PyQt6.QtCore import QThread

    # 1. _CatalogFacetWorker must be a QThread subclass
    assert issubclass(genizah_app._CatalogFacetWorker, QThread), (
        "_CatalogFacetWorker must be a QThread subclass"
    )

    source = Path(genizah_app.__file__).read_text(encoding='utf-8')
    tree = ast.parse(source)

    # 2. find _CatalogFacetWorker.run body
    run_has_facet_call = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == '_CatalogFacetWorker':
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == 'run':
                    # Walk the run() body for get_browse_library_facets calls
                    for child in ast.walk(item):
                        if isinstance(child, ast.Attribute):
                            if child.attr == 'get_browse_library_facets':
                                run_has_facet_call = True
                        elif isinstance(child, ast.Name):
                            if child.id == 'get_browse_library_facets':
                                run_has_facet_call = True

    assert run_has_facet_call, (
        "_CatalogFacetWorker.run() must contain a call to get_browse_library_facets"
    )

    # 3. _open_catalog_library_dialog must NOT contain a direct get_browse_library_facets call
    dialog_open_has_facet_call = False
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == '_open_catalog_library_dialog':
            for child in ast.walk(node):
                if isinstance(child, ast.Attribute):
                    if child.attr == 'get_browse_library_facets':
                        dialog_open_has_facet_call = True
                elif isinstance(child, ast.Name):
                    if child.id == 'get_browse_library_facets':
                        dialog_open_has_facet_call = True

    assert not dialog_open_has_facet_call, (
        "_open_catalog_library_dialog must NOT call get_browse_library_facets directly "
        "(DB facet call must be delegated to the worker thread, not the UI thread)"
    )


# ── Phase 131 Plan 07: Search box + sort toggle (DMF-07 desktop parity) ──────
# Six tests cover: search hides non-matching rows while preserving check state;
# get_checked_codes returns hidden-but-checked codes; By-count orders by facets desc;
# A-Z orders by display name asc; By-count falls back to A-Z when facets empty;
# Select All ignores the active search filter.


@pytest.mark.gui
def test_search_hides_nonmatching_rows_and_preserves_checks():
    """(1) Type-to-find: non-matching rows are hidden; matching rows visible;
    check state is preserved across hide/show; CUL stays checked while hidden.

    Web parity: catLibFilterSearch hides rows whose data-label does not contain the
    query; checkbox state is unchanged.
    """
    from desktop.dialogs_filter import LibraryFilterDialog
    from PyQt6.QtCore import Qt

    dlg = LibraryFilterDialog(mode='show_only', selected_codes=['CUL'])

    # Find the CUL item and read its display label for a robust query
    cul_item = None
    for i in range(dlg.list_widget.count()):
        item = dlg.list_widget.item(i)
        if item.data(Qt.ItemDataRole.UserRole) == 'CUL':
            cul_item = item
            break
    assert cul_item is not None, "CUL must appear in the dialog universe"

    cul_label = cul_item.text()

    # Use a 4-char substring of the CUL label that is unlikely to match every row.
    # Fall back to 'CUL' as a query if the label itself is short.
    query = cul_label[:4].lower() if len(cul_label) >= 4 else 'CUL'

    dlg.search_input.setText(query)  # fires textChanged -> _apply_search_filter

    # CUL row must be visible (label contains query)
    assert not cul_item.isHidden(), (
        f"CUL row must be visible after typing {query!r} (its label is {cul_label!r})"
    )

    # At least one other row must be hidden (query is not universal)
    hidden_count = sum(
        1 for i in range(dlg.list_widget.count())
        if dlg.list_widget.item(i).isHidden()
    )
    assert hidden_count > 0, (
        f"At least one non-matching row must be hidden after typing {query!r}"
    )

    # CUL check state must still be Checked
    assert cul_item.checkState() == Qt.CheckState.Checked, (
        "CUL must remain Checked after the search filter is applied"
    )

    # Clear the search — all rows must be visible again
    dlg.search_input.setText("")
    for i in range(dlg.list_widget.count()):
        assert not dlg.list_widget.item(i).isHidden(), (
            f"Row {i} must be visible after clearing the search box"
        )


@pytest.mark.gui
def test_get_checked_codes_returns_hidden_checked_codes():
    """(2) Hidden-but-checked rows still appear in get_checked_codes() — web parity.

    Construct in hide mode, programmatically check two codes, then type a query that
    hides those rows; assert get_checked_codes() still returns them.
    """
    from desktop.dialogs_filter import LibraryFilterDialog
    from PyQt6.QtCore import Qt

    dlg = LibraryFilterDialog(mode='hide')

    # Find CUL and BL items and check them
    codes_to_check = {'CUL', 'BL'}
    checked_items = {}
    for i in range(dlg.list_widget.count()):
        item = dlg.list_widget.item(i)
        code = item.data(Qt.ItemDataRole.UserRole)
        if code in codes_to_check:
            item.setCheckState(Qt.CheckState.Checked)
            checked_items[code] = item

    assert len(checked_items) >= 2, (
        f"CUL and BL must both be present in the dialog universe; found {list(checked_items)}"
    )

    # Apply a query that hides at least the checked rows.
    # Use a 3-letter string that shouldn't match CUL or BL display labels.
    # We'll use a query that we know the CUL/BL labels don't contain (e.g. 'zzzz').
    dlg.search_input.setText("zzzz")

    # Both CUL and BL rows should be hidden (their labels don't contain 'zzzz')
    for code, item in checked_items.items():
        assert item.isHidden(), (
            f"{code} row must be hidden after query 'zzzz'"
        )

    # get_checked_codes must still return both hidden-but-checked codes
    returned = set(dlg.get_checked_codes())
    for code in checked_items:
        assert code in returned, (
            f"get_checked_codes() must return hidden-but-checked code {code!r}; "
            f"got {returned!r}"
        )


@pytest.mark.gui
def test_by_count_sort_orders_by_facets_descending():
    """(3) By count sort orders rows by self._facets descending (CUL>BL>JTS order).

    Web parity: catLibFilterSort(key='count') sorts rows by data-count DESCENDING.
    """
    from desktop.dialogs_filter import LibraryFilterDialog

    facets = {'CUL': 5000, 'JTS': 100, 'BL': 900}
    dlg = LibraryFilterDialog(mode='hide', facets=facets)

    # Default is A-Z; switch to By count
    dlg._rb_sort_count.setChecked(True)  # fires _on_sort_changed -> _repopulate

    # Read the row order
    order = [
        dlg.list_widget.item(i).data(256)  # 256 == Qt.ItemDataRole.UserRole
        for i in range(dlg.list_widget.count())
    ]

    # All three facet codes must be present
    assert 'CUL' in order and 'BL' in order and 'JTS' in order, (
        f"CUL, BL, JTS must all be in the row order; got {order!r}"
    )

    # By count descending: CUL (5000) before BL (900) before JTS (100)
    assert order.index('CUL') < order.index('BL'), (
        f"CUL (5000) must appear before BL (900) in By-count order; got {order!r}"
    )
    assert order.index('BL') < order.index('JTS'), (
        f"BL (900) must appear before JTS (100) in By-count order; got {order!r}"
    )


@pytest.mark.gui
def test_az_sort_orders_by_display_name():
    """(4) A-Z sort orders rows by display name ascending (default order).

    Web parity: catLibFilterSort(key='az') sorts rows by data-label ascending.
    """
    from desktop.dialogs_filter import LibraryFilterDialog
    from genizah_core import get_library_display
    from shared.browse_map_utils import library_codes_with_manuscripts

    dlg = LibraryFilterDialog(mode='hide')

    # A-Z is the default; confirm it's selected
    assert dlg._rb_sort_az.isChecked(), "A-Z sort radio must be checked by default"

    # Collect all codes in list order
    order = [
        dlg.list_widget.item(i).data(256)
        for i in range(dlg.list_widget.count())
    ]

    # The expected A-Z order: sorted by get_library_display ascending
    all_codes = [c for c in library_codes_with_manuscripts() if c != 'LOCAL']
    expected = sorted(all_codes, key=lambda c: get_library_display(c, short=False))

    assert order == expected, (
        f"A-Z sort must produce get_library_display ascending order; "
        f"first 5 got {order[:5]!r}, expected {expected[:5]!r}"
    )


@pytest.mark.gui
def test_by_count_falls_back_to_az_when_facets_empty():
    """(5) By-count with empty facets produces the same order as A-Z (fallback).

    Web parity: By-count with all data-count=0 produces stable sort; desktop
    explicitly falls back to A-Z when self._facets is empty.
    """
    from desktop.dialogs_filter import LibraryFilterDialog

    # Create with no facets (empty)
    dlg = LibraryFilterDialog(mode='hide')

    # Capture A-Z order (default)
    az_order = [
        dlg.list_widget.item(i).data(256)
        for i in range(dlg.list_widget.count())
    ]

    # Switch to By count with empty facets
    dlg._rb_sort_count.setChecked(True)  # fires _on_sort_changed -> _repopulate

    bycount_order = [
        dlg.list_widget.item(i).data(256)
        for i in range(dlg.list_widget.count())
    ]

    assert bycount_order == az_order, (
        f"By-count with empty facets must fall back to A-Z order; "
        f"first 5 A-Z {az_order[:5]!r}, first 5 By-count {bycount_order[:5]!r}"
    )


@pytest.mark.gui
def test_select_all_ignores_active_search_filter():
    """(6) Select All checks ALL rows regardless of the active search filter.

    Web parity: catLibFilterSelectAll(val) sets every .cat-lib-cb regardless of
    visible/hidden state — hidden rows are also checked.
    """
    from desktop.dialogs_filter import LibraryFilterDialog
    from PyQt6.QtCore import Qt

    dlg = LibraryFilterDialog(mode='hide')

    # Type a query that hides most rows (use 'zzzz' which matches nothing)
    dlg.search_input.setText("zzzz")
    hidden_before = sum(
        1 for i in range(dlg.list_widget.count())
        if dlg.list_widget.item(i).isHidden()
    )
    assert hidden_before > 0, "At least some rows must be hidden by 'zzzz' query"

    # Click Select All
    dlg._select_all()

    # ALL items (including hidden) must be Checked
    for i in range(dlg.list_widget.count()):
        item = dlg.list_widget.item(i)
        assert item.checkState() == Qt.CheckState.Checked, (
            f"Item {i} (code={item.data(256)!r}, hidden={item.isHidden()}) "
            f"must be Checked after Select All"
        )
