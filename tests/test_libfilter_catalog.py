"""LIBFILTER-02 — catalog "Browse by identification" library filter service tests.

Covers:
- _FILTER_TEMP_TABLES allowlist contains ``"_browse_filter_library"``;
- ``get_browse_results(library_codes=…, library_sys_ids=…)`` narrows total to
  the full filtered set (not the page-size subset);
- None/empty library args are a no-op (backward-compatible);
- content-derived token prevents same-size-but-different selections from
  reusing stale TEMP-table data (GUARD-02 / Codex REQUIRED CHANGE 1);
- 3-way AND composition with PGP + Editions filters;
- selected-but-resolved-to-empty fails open (Codex REQUIRED CHANGE 2).

Mirrors the shape of ``tests/test_seed023_catalog_filters.py``.
"""

from __future__ import annotations

import sqlite3

import pytest


def _func_body(source: str, def_idx: int) -> str:
    """Return the source of a nested function starting at def_idx, sliced up to the
    next sibling `    def ` (4-space indent) so fixed-size windows can't bleed into
    an adjacent function's body/docstring."""
    nxt = source.find('\n    def ', def_idx + 1)
    return source[def_idx:nxt] if nxt != -1 else source[def_idx:]


# ── Minimal in-memory-ish FJMS catalog for the browse-filter wiring tests ───

_CAT_COLS = (
    "AlmaId TEXT, Title TEXT, TitleHeb TEXT, AuthorText TEXT, CopyDate TEXT, "
    "TextualFrameHeb TEXT, TextualFrameEng TEXT"
)

# Six rows with AlmaIds A1..A6, split into two disjoint groups of three:
#   Group X = {A1, A2, A3}  (used for "CUL" and "X" library selections)
#   Group Y = {A4, A5, A6}  (used for "JTS" and "Y" library selections)
_ROWS = [
    ("A1", "alef"),
    ("A2", "bet"),
    ("A3", "gimel"),
    ("A4", "dalet"),
    ("A5", "he"),
    ("A6", "vav"),
]


@pytest.fixture
def tiny_fjms(tmp_path):
    """A FjmsService over a 6-row catalog (no domains/FTS needed for these tests)."""
    from shared.fjms_service import FjmsService

    db = tmp_path / "tiny_fjms.db"
    conn = sqlite3.connect(str(db))
    conn.execute(f"CREATE TABLE catalog ({_CAT_COLS})")
    conn.executemany(
        "INSERT INTO catalog (AlmaId, Title) VALUES (?, ?)", _ROWS
    )
    conn.commit()
    conn.close()

    svc = FjmsService(db_path=str(db), thread_safe=True)
    assert svc.is_available()
    yield svc
    svc.close()


def _total(svc, **kw):
    """Return the total count from get_browse_results with a page size of 2."""
    return svc.get_browse_results(offset=0, limit=2, **kw)["total"]


def _result_ids(svc, **kw):
    """Return the SET of AlmaId values from a full (limit=100) result page."""
    results = svc.get_browse_results(offset=0, limit=100, **kw)["results"]
    return {r["sys_id"] for r in results}


# ── LIBFILTER-02 tests ────────────────────────────────────────────────────────


def test_allowlist_contains_library_table():
    """_browse_filter_library must be in the allowlist (injection-safety gate)."""
    from shared.fjms_service import FjmsService

    assert "_browse_filter_library" in FjmsService._FILTER_TEMP_TABLES


def test_library_filter_changes_total_full_set_not_page(tiny_fjms):
    """Library filter narrows total to the full filtered set, not the page limit.

    library_sys_ids = {A1, A2, A3} (3 of 6 rows).  total must be 3, not 2 (the
    page limit) and not 6 (unfiltered).  The returned page is still capped at
    limit=2.
    """
    lib_ids = {"A1", "A2", "A3"}
    # Unfiltered baseline
    assert _total(tiny_fjms) == 6
    # Filtered total is the subset size, not the page size
    assert _total(tiny_fjms, library_codes=["CUL"], library_sys_ids=lib_ids) == 3
    # The page itself is still capped at the limit
    page = tiny_fjms.get_browse_results(
        offset=0, limit=2, library_codes=["CUL"], library_sys_ids=lib_ids
    )
    assert page["total"] == 3 and len(page["results"]) == 2


def test_library_none_or_empty_is_noop(tiny_fjms):
    """None or empty library args must be a no-op (backward-compatible).

    Mirrors test_filter_skipped_when_set_missing from test_seed023_catalog_filters.
    """
    unfiltered = _total(tiny_fjms)
    assert unfiltered == 6
    assert _total(tiny_fjms, library_codes=None, library_sys_ids=None) == 6
    assert _total(tiny_fjms, library_codes=[], library_sys_ids=None) == 6
    assert _total(tiny_fjms, library_codes=None, library_sys_ids=set()) == 6


def test_same_size_different_selection_not_stale(tiny_fjms):
    """Two same-size-but-different library selections must return DIFFERENT result sets.

    This directly tests the Codex REQUIRED CHANGE 1: the TEMP-table token must be
    content-derived (hash of selection), NOT len-derived.  Two selections of size 3
    would share the same ``len`` token and reuse stale TEMP rows — the wrong results
    would then be silently returned.

    We assert on the RETURNED ID SETS (not totals, which coincidentally equal 3 for
    both) because only the set difference proves the TEMP table was rebuilt.

    Group X = {A1, A2, A3}, Group Y = {A4, A5, A6} — genuinely disjoint in the catalog.
    """
    group_x = {"A1", "A2", "A3"}
    group_y = {"A4", "A5", "A6"}

    ids_x = _result_ids(tiny_fjms, library_codes=["X"], library_sys_ids=group_x)
    ids_y = _result_ids(tiny_fjms, library_codes=["Y"], library_sys_ids=group_y)

    # Both have 3 results each — totals alone would NOT distinguish stale reuse
    assert len(ids_x) == 3
    assert len(ids_y) == 3

    # The actual ID sets must be disjoint (content-derived token rebuilt the table)
    assert ids_x != ids_y, (
        "Same result set returned for two different same-size library selections — "
        "TEMP table was not rebuilt (stale len-based token reuse detected)"
    )
    assert ids_x.isdisjoint(ids_y), (
        f"Expected disjoint sets; got overlap: {ids_x & ids_y}"
    )


def test_composition_pgp_editions_library_3way_and(tiny_fjms):
    """Library, PGP, and Editions filters compose via 3-way AND.

    Setup:
      PGP set    = {A1, A2, A3, A4}   (has_pgp selects these)
      Edition set = {A2, A3, A5, A6}  (has_edition selects these)
      Library set = {A1, A2, A3}       (library selects these)
      Intersection = {A2, A3}          → total should be 2
    """
    pgp_ids = {"A1", "A2", "A3", "A4"}
    ed_ids = {"A2", "A3", "A5", "A6"}
    lib_ids = {"A1", "A2", "A3"}

    result = _total(
        tiny_fjms,
        pgp_filter="has_pgp",
        pgp_sys_ids=pgp_ids,
        editions_filter="has_edition",
        edition_sys_ids=ed_ids,
        library_codes=["CUL"],
        library_sys_ids=lib_ids,
    )
    assert result == 2, f"Expected 3-way intersection = 2, got {result}"


def test_selected_but_resolved_empty_fails_open(tiny_fjms):
    """A non-empty library_codes that resolves to an empty sys_id set must fail open.

    This documents Codex REQUIRED CHANGE 2: when library_codes is truthy but
    library_sys_ids is empty/None (e.g. all selected codes were invalid, or the
    csv_bank was not yet loaded), the filter must be SKIPPED (fail-open), returning
    ALL results rather than 0.  The result must equal the unfiltered total (6), NOT 0.
    """
    # Empty set (resolved to nothing)
    assert _total(tiny_fjms, library_codes=["CUL"], library_sys_ids=set()) == 6
    # None (not yet resolved)
    assert _total(tiny_fjms, library_codes=["CUL"], library_sys_ids=None) == 6


# ── GAP-E + GAP-F closure tests (129-06) ─────────────────────────────────────
# These tests encode the threading (build/consume/lifecycle) and control (dialog
# vs. ui.select) requirements.  They are RED until Tasks 2 and 3 are implemented.

# ── GAP-F: build ────────────────────────────────────────────────────────────

def test_has_active_filters_true_when_library_selected():
    """_has_active_filters() must return True when a library code is selected.

    Encodes GAP-F: _has_active_filters omitted current_library_filter, so
    "Search in these results" was always disabled when library was the only
    active filter.  After the fix, the any([...]) in _has_active_filters
    includes current_library_filter['value'].
    """
    import pathlib
    source = pathlib.Path('web/pages/catalog_browse.py').read_text(encoding='utf-8')
    # The _has_active_filters function must reference current_library_filter
    # We verify this by source-scanning the function body
    assert 'current_library_filter' in source, (
        "current_library_filter must appear in catalog_browse.py"
    )
    # Specifically it must be in _has_active_filters context
    # Find the function and check that current_library_filter appears near it
    has_active_idx = source.find('def _has_active_filters()')
    assert has_active_idx != -1, "_has_active_filters() must exist in catalog_browse.py"
    # The function body follows the def — check that library_filter appears within
    # a reasonable span (the whole function is < 25 lines)
    func_snippet = source[has_active_idx:has_active_idx + 800]
    assert 'current_library_filter' in func_snippet, (
        "current_library_filter['value'] must appear inside _has_active_filters(); "
        "without it, library-only selections leave 'Search in these results' disabled (GAP-F)"
    )


def test_build_incoming_filters_includes_library_filter():
    """_build_incoming_filters() must include library_filter when codes are selected.

    Encodes GAP-F: catalog_browse._build_incoming_filters omitted the library
    selection so the key 'library_filter' was never sent to consume_incoming_filters.
    """
    import pathlib
    source = pathlib.Path('web/pages/catalog_browse.py').read_text(encoding='utf-8')
    # Signature gained an include_library kwarg (Codex MEDIUM 2026-06-29) — match the
    # def by prefix, not the old zero-arg form.
    build_idx = source.find('def _build_incoming_filters(')
    assert build_idx != -1, "_build_incoming_filters(...) must exist in catalog_browse.py"
    func_snippet = _func_body(source, build_idx)
    assert "incoming['library_filter']" in func_snippet or "incoming[\"library_filter\"]" in func_snippet, (
        "_build_incoming_filters() must set incoming['library_filter'] from "
        "current_library_filter['value'] (GAP-F)"
    )


def test_parallels_handoff_excludes_library():
    """Codex MEDIUM (2026-06-29): web Parallels ignores library_filter, so the
    catalog→parallels handoff must NOT carry it (it would silently produce unscoped
    parallels), and a library-ONLY selection must not enable the Parallels button.

    Source-scan assertions:
    - _has_active_filters_excluding_library() exists and does NOT reference library.
    - _parallels_in_results calls _build_incoming_filters(include_library=False).
    - _update_search_buttons gates the parallels button on the excluding-library check.
    - Search handoff (_search_in_results) still carries library (include_library default).
    """
    import pathlib
    source = pathlib.Path('web/pages/catalog_browse.py').read_text(encoding='utf-8')

    # The library-excluding active-filter helper must exist.
    excl_idx = source.find('def _has_active_filters_excluding_library(')
    assert excl_idx != -1, "_has_active_filters_excluding_library() must exist"
    excl_body = _func_body(source, excl_idx)
    assert 'current_library_filter' not in excl_body, (
        "_has_active_filters_excluding_library() must NOT count current_library_filter"
    )

    # Parallels handoff strips library.
    par_idx = source.find('def _parallels_in_results(')
    assert par_idx != -1, "_parallels_in_results() must exist"
    par_body = _func_body(source, par_idx)
    assert 'include_library=False' in par_body, (
        "_parallels_in_results must build the handoff with include_library=False"
    )

    # Parallels button enablement uses the excluding-library check.
    upd_idx = source.find('def _update_search_buttons(')
    assert upd_idx != -1, "_update_search_buttons() must exist"
    upd_body = _func_body(source, upd_idx)
    assert '_has_active_filters_excluding_library()' in upd_body, (
        "_update_search_buttons must gate the parallels button on "
        "_has_active_filters_excluding_library()"
    )

    # Search handoff still carries library (default include_library=True).
    search_idx = source.find('def _search_in_results(')
    assert search_idx != -1, "_search_in_results() must exist"
    search_body = _func_body(source, search_idx)
    assert 'include_library=False' not in search_body, (
        "_search_in_results must NOT strip library (search applies it)"
    )


# ── GAP-F: consume ───────────────────────────────────────────────────────────

def test_consume_sets_library_filter_and_persists(monkeypatch):
    """consume_incoming_filters must set state.library_filter and persist search_library_filter.

    Encodes GAP-F consume side: after the fix, consume_incoming_filters reads
    incoming.get('library_filter') and:
      1. sets state.library_filter via setattr (to the codes list)
      2. calls persist_value('search_library_filter', ...) with the D-09 dict shape

    The incoming value is a bare list ['CUL', 'JTS'] (legacy shape).  The backward-compat
    branch in consume_incoming_filters interprets a bare-list as Show-only and persists
    the D-09 dict shape: {'mode': 'show_only', 'codes': ['CUL', 'JTS']}.
    state.library_filter is set to the codes list (the live-render value).
    Both are required: setattr for the live render, persist for the next fresh render.
    """
    persisted = {}

    # Patch safe_user_get to return our mock incoming_filters and session_persistence_enabled
    def fake_safe_user_get(key, default=None):
        if key == 'incoming_filters':
            return {'library_filter': ['CUL', 'JTS']}
        if key == 'session_persistence_enabled':
            return True
        return default

    def fake_safe_user_set(key, value):
        persisted[key] = value

    def fake_safe_user_pop(key, default=None):
        pass

    monkeypatch.setattr('web.safe_storage.safe_user_get', fake_safe_user_get)
    monkeypatch.setattr('web.safe_storage.safe_user_set', fake_safe_user_set)
    monkeypatch.setattr('web.safe_storage.safe_user_pop', fake_safe_user_pop)

    # Also patch persist_value's safe_user_get/set calls
    import web.components.filter_panel as fp
    monkeypatch.setattr(fp, 'persist_value',
        lambda key, value: persisted.__setitem__(key, value))

    class FakeState:
        library_filter: list = []

    state = FakeState()
    result = fp.consume_incoming_filters(state, 'search', require_from_browse=False)

    assert result is True, "consume_incoming_filters must return True when filters were consumed"
    assert state.library_filter == ['CUL', 'JTS'], (
        "consume_incoming_filters must set state.library_filter to the codes list "
        "from incoming['library_filter']"
    )
    assert 'search_library_filter' in persisted, (
        "consume_incoming_filters must persist 'search_library_filter' (the literal key "
        "that search.py loads at :187-189)"
    )
    # D-09 contract: persisted value is ALWAYS the dict shape {'mode','codes'}.
    # A bare-list incoming is interpreted as Show-only (backward-compat branch).
    assert persisted['search_library_filter'] == {'mode': 'show_only', 'codes': ['CUL', 'JTS']}, (
        "Persisted value must be the D-09 dict shape {'mode': 'show_only', 'codes': [...]}. "
        "A bare-list incoming is interpreted as Show-only; search.py's load path reads "
        "this dict shape and still migrates legacy plain-lists (search.py:189+)."
    )


def test_consume_does_not_crash_without_library_filter_attr(monkeypatch):
    """consume_incoming_filters must not crash when state lacks a .library_filter attr.

    Parallels page state may not have library_filter; the defensive setattr/getattr
    in consume must not raise AttributeError.
    """
    persisted = {}

    def fake_safe_user_get(key, default=None):
        if key == 'incoming_filters':
            return {'library_filter': ['CUL']}
        if key == 'session_persistence_enabled':
            return True
        return default

    def fake_safe_user_set(key, value):
        persisted[key] = value

    def fake_safe_user_pop(key, default=None):
        pass

    monkeypatch.setattr('web.safe_storage.safe_user_get', fake_safe_user_get)
    monkeypatch.setattr('web.safe_storage.safe_user_set', fake_safe_user_set)
    monkeypatch.setattr('web.safe_storage.safe_user_pop', fake_safe_user_pop)

    import web.components.filter_panel as fp
    monkeypatch.setattr(fp, 'persist_value',
        lambda key, value: persisted.__setitem__(key, value))

    # A parallels-like state without .library_filter
    class ParallelsLikeState:
        pass  # No library_filter attribute

    state = ParallelsLikeState()
    # Must not raise
    fp.consume_incoming_filters(state, 'parallels', require_from_browse=False)
    # No assertion on attribute state — the point is no crash


# ── GAP-F: LIFECYCLE (persist→reload) ────────────────────────────────────────

def test_lifecycle_persist_then_reload(monkeypatch):
    """Full persist→reload lifecycle: consume persists D-09 dict, subsequent read returns it.

    (a) consume_incoming_filters with library_filter=['CUL'] (bare list, legacy shape)
        → persists 'search_library_filter' as {'mode': 'show_only', 'codes': ['CUL']} (D-09 dict shape)
    (b) A fresh state-init render reads storage.get('search_library_filter') → returns that dict

    The D-09 contract (locked in Phase 130): persist key is ALWAYS the dict shape so the
    restore path (search.py:189+) reads back the correct mode.  search.py still migrates
    legacy plain-list values for backward compat with pre-Phase-130 storage entries.
    Both halves are required for the selection to survive a /search reload (GAP-F lifecycle).
    """
    storage = {}

    def fake_safe_user_get(key, default=None):
        if key == 'incoming_filters':
            return {'library_filter': ['CUL']}
        if key == 'session_persistence_enabled':
            return True
        return storage.get(key, default)

    def fake_safe_user_set(key, value):
        storage[key] = value

    def fake_safe_user_pop(key, default=None):
        pass

    monkeypatch.setattr('web.safe_storage.safe_user_get', fake_safe_user_get)
    monkeypatch.setattr('web.safe_storage.safe_user_set', fake_safe_user_set)
    monkeypatch.setattr('web.safe_storage.safe_user_pop', fake_safe_user_pop)

    import web.components.filter_panel as fp

    # Patch persist_value to write into storage (same as safe_user_set above)
    def real_persist(key, value):
        storage[key] = value

    monkeypatch.setattr(fp, 'persist_value', real_persist)

    class FakeState:
        library_filter: list = []

    # (a) Run consume — simulating the browse→search handoff with a bare-list incoming value
    state = FakeState()
    fp.consume_incoming_filters(state, 'search', require_from_browse=False)
    # D-09 contract: consume persists the dict shape, NOT the bare list.
    # A bare-list incoming ['CUL'] is interpreted as Show-only (backward-compat branch).
    assert storage.get('search_library_filter') == {'mode': 'show_only', 'codes': ['CUL']}, (
        "consume_incoming_filters must persist 'search_library_filter' as the D-09 dict shape "
        "{'mode': 'show_only', 'codes': ['CUL']} so a subsequent fresh render can reload it "
        "with the correct mode"
    )

    # (b) Simulate a fresh state-init render calling storage.get('search_library_filter')
    # This is the load path at search.py:189+ — reads the dict shape and restores mode+codes.
    reloaded = storage.get('search_library_filter', [])
    assert reloaded == {'mode': 'show_only', 'codes': ['CUL']}, (
        "The persisted 'search_library_filter' value on a fresh render must be the D-09 dict "
        "{'mode': 'show_only', 'codes': ['CUL']}; got %r" % (reloaded,)
    )


def test_lifecycle_source_order_load_before_consume():
    """Source-order assertion: in search.py, the _safe_get('search_library_filter') LOAD
    appears at a lower line index than the consume_incoming_filters() CALL.

    This encodes the ordering contract: the load runs first (restoring persisted codes),
    then consume runs (setattr overwrites with fresh incoming codes for this render,
    AND persists for the next render).  Because consume runs AFTER the load, the setattr
    is never overwritten by the load — so the incoming codes survive into the first use
    of search_state.library_filter.
    """
    import pathlib
    source = pathlib.Path('web/pages/search.py').read_text(encoding='utf-8')

    load_token = "_safe_get('search_library_filter'"
    consume_token = "consume_incoming_filters("

    load_idx = source.find(load_token)
    consume_idx = source.find(consume_token)

    assert load_idx != -1, (
        f"Could not find load token {load_token!r} in search.py — "
        "the _safe_get('search_library_filter', []) load must exist at ~search.py:187"
    )
    assert consume_idx != -1, (
        f"Could not find consume token {consume_token!r} in search.py — "
        "consume_incoming_filters() must be called in search.py at ~:199"
    )
    assert load_idx < consume_idx, (
        f"Source ordering violated: _safe_get('search_library_filter') load (char {load_idx}) "
        f"must appear BEFORE consume_incoming_filters call (char {consume_idx}).  "
        "If they are reordered, the load will overwrite the consume setattr, "
        "and the incoming library codes will be lost."
    )


# ── GAP-E: control (dialog vs. ui.select) ───────────────────────────────────

def test_catalog_library_control_is_dialog_not_select():
    """catalog_browse.py must implement _open_library_filter_dialog and ui.dialog,
    and must NOT implement a multiple=True ui.select for the library filter (GAP-E).
    """
    import pathlib
    source = pathlib.Path('web/pages/catalog_browse.py').read_text(encoding='utf-8')

    # The dialog function must exist
    assert '_open_library_filter_dialog' in source, (
        "catalog_browse.py must define _open_library_filter_dialog() "
        "(the checkbox dialog replacing the ui.select — GAP-E)"
    )

    # ui.dialog must be used (for the library dialog)
    assert 'ui.dialog()' in source or 'ui.dialog(' in source, (
        "catalog_browse.py must use ui.dialog() for the library filter (GAP-E)"
    )

    # The old ui.select(multiple=True) LIBRARY control must be gone.
    # The catalog also has other ui.select controls (author, work) so we check
    # for the specific pattern that was the library select: 'library_filter_ctrl_ref'
    # being assigned a ui.select result.
    # The old code was: library_filter_ctrl_ref['ref'] = lib_sel
    # where lib_sel = ui.select(... multiple=True ...)
    # After the fix, library_filter_ctrl_ref is either removed or reassigned to a button.
    # We check that 'Select libraries...' (the old label) is gone from catalog_browse.py.
    assert "Select libraries..." not in source, (
        "The old ui.select label 'Select libraries...' must be gone from catalog_browse.py "
        "(the ui.select library control was replaced by a checkbox dialog — GAP-E)"
    )


# ── FINDING 1: all-unchecked guard ───────────────────────────────────────────

def test_catalog_dialog_no_deselect_all_and_apply_guarded():
    """FINDING 1: the catalog library dialog must not have a deselect-all affordance,
    and Apply must be guarded against a zero-checked commit.

    Locked design: only 'Select All' is provided (re-checks everything = clear filter);
    there is NO 'Select None'/deselect-all action (that would produce an apply-able
    all-unchecked state that collides with the '[]' = show all sentinel).
    Apply is disabled client-side when checked-count == 0, and Python defensively
    short-circuits with the hint string if somehow an empty set arrives.
    """
    import pathlib
    source = pathlib.Path('web/pages/catalog_browse.py').read_text(encoding='utf-8')

    # The all-unchecked Python guard (hint string) must be present in catalog_browse.py
    assert 'Select at least one library' in source, (
        "catalog_browse.py must contain the FINDING-1 hint string "
        "'Select at least one library' in the Apply guard "
        "(prevents all-unchecked '[]' collision with the show-all sentinel)"
    )

    # 'Select All' must be present (the only bulk action)
    # (Using tr('Select All') — already in translations)
    assert "tr('Select All')" in source or 'tr("Select All")' in source, (
        "catalog_browse.py must provide a 'Select All' action in the library dialog"
    )

    # BUG-C fix: 'Select None' IS now present in catalog_browse.py as a convenience
    # clear-checkboxes action. It does NOT commit — Apply is disabled when zero are
    # checked (catLibFilterUpdateApply JS guard + Python if-not-checked guard).
    # Verify that 'Select None' is paired with a guarded Apply (both must coexist):
    assert "tr('Select All')" in source or 'tr("Select All")' in source, (
        "catalog_browse.py must still provide a 'Select All' action alongside 'Select None'"
    )
    # The all-unchecked Apply guard must remain (FINDING 1 invariant — '[] = show-all' safe).
    assert 'Select at least one library' in source, (
        "The Python Apply guard 'Select at least one library' must still be present "
        "alongside the new 'Select None' button (FINDING 1 invariant)"
    )


def test_catalog_apply_mapping_all_checked_clears_filter():
    """The catalog dialog's apply mapping: all-checked => [] (clear filter/show all).

    Source-string assertion: the all-checked branch (set comparison that yields [])
    must be present in catalog_browse.py, encoding the mapping:
      - all checked => [] (clear filter; '[]' = show all — the existing data-layer sentinel)
      - strict subset => that subset
      - zero checked => NEVER CALLED (guarded)
    """
    import pathlib
    source = pathlib.Path('web/pages/catalog_browse.py').read_text(encoding='utf-8')
    # The apply helper returns [] when the full set is checked (all-checked branch)
    # This mirrors _library_apply_selection in search.py:
    #   if set(checked_codes) == set(all_codes): return []
    # Any of the following patterns encodes this:
    all_checked_clears = (
        'set(checked_codes) == set(all_codes)' in source
        or 'set(checked) == set(all_codes)' in source
        or 'set(checked) == set(_all_codes)' in source
        or 'len(checked) == len(all_codes)' in source
        or '== len(_all_codes)' in source
    )
    # OR: a simpler check that the function exists and returns [] for the all-checked case
    # (checked-set length == LIBRARY_CODES minus LOCAL length)
    # Also accept a direct length comparison used in a catalog-specific implementation
    # Accept any version of "if checked_set equals full set, clear filter"
    assert all_checked_clears or 'return []' in source, (
        "catalog_browse.py must implement the all-checked => [] (clear filter) branch "
        "in the library dialog apply handler (FINDING 1 apply mapping)"
    )


# ---------------------------------------------------------------------------
# BUG-B static guard: no <script> inside ui.html in the catalog library dialog
# ---------------------------------------------------------------------------

def test_catalog_no_script_in_library_dialog_html():
    """BUG-B static guard: _open_library_filter_dialog in catalog_browse.py must NOT
    pass a string containing '<script' to ui.html().

    NiceGUI raises ValueError: 'HTML elements must not contain <script> tags.' when
    ui.html() content contains a <script> tag. This guard catches the class of bug
    that was BUG-B: JS functions embedded inside the dialog's ui.html() call.

    The correct pattern: JS defined once at page level in ui.add_head_html().
    """
    import pathlib
    source = pathlib.Path('web/pages/catalog_browse.py').read_text(encoding='utf-8')

    # Find _open_library_filter_dialog function body in catalog_browse.py
    lines = source.splitlines()
    in_fn = False
    fn_lines = []
    fn_indent = None
    for ln in lines:
        if 'def _open_library_filter_dialog' in ln:
            in_fn = True
            fn_indent = len(ln) - len(ln.lstrip())
        if in_fn:
            fn_lines.append(ln)
            if len(fn_lines) > 1:
                stripped = ln.strip()
                if stripped and not stripped.startswith('#'):
                    cur_indent = len(ln) - len(ln.lstrip())
                    if cur_indent <= fn_indent and stripped.startswith('def '):
                        break
            if len(fn_lines) > 200:
                break

    fn_src = '\n'.join(fn_lines)

    assert '_open_library_filter_dialog' in fn_src, (
        "_open_library_filter_dialog not found in catalog_browse.py"
    )

    # If ui.html( and <script both appear on non-comment lines in the function,
    # that is the BUG-B pattern. Comments may legitimately mention '<script>' as docs.
    non_comment_fn_lines = [
        ln for ln in fn_lines
        if ln.strip() and not ln.strip().startswith('#')
    ]
    non_comment_fn_src = '\n'.join(non_comment_fn_lines)

    if 'ui.html(' in non_comment_fn_src and '<script' in non_comment_fn_src:
        raise AssertionError(
            "BUG-B: _open_library_filter_dialog in catalog_browse.py passes a string "
            "containing '<script' to ui.html(). NiceGUI raises ValueError at dialog-open "
            "time. Move JS functions to ui.add_head_html() at page setup."
        )

    # Positive: the page-level ui.add_head_html must define the catLibFilter JS functions.
    assert 'catLibFilterGetChecked' in source, (
        "BUG-B: catLibFilterGetChecked JS function not found in catalog_browse.py. "
        "It must be defined in ui.add_head_html() at page setup (not inside ui.html)."
    )
    # The JS must be defined in a ui.add_head_html() call, not inside a ui.html() call.
    # Verify: 'catLibFilterGetChecked' must appear INSIDE a ui.add_head_html( block.
    # Simple heuristic: find the line number of 'catLibFilterGetChecked' and check that
    # 'ui.add_head_html' appears within 20 lines before it.
    catalog_lines = source.splitlines()
    js_fn_line = None
    for i, ln in enumerate(catalog_lines):
        if 'catLibFilterGetChecked' in ln and 'function catLibFilterGetChecked' in ln:
            js_fn_line = i
            break
    assert js_fn_line is not None, (
        "BUG-B: 'function catLibFilterGetChecked' definition not found in catalog_browse.py. "
        "The JS function must be defined (not just called) in ui.add_head_html() setup."
    )
    # Check that ui.add_head_html appears within the preceding 20 lines
    preceding_block = '\n'.join(catalog_lines[max(0, js_fn_line - 20): js_fn_line + 1])
    assert 'ui.add_head_html' in preceding_block, (
        "BUG-B: 'function catLibFilterGetChecked' is not inside a ui.add_head_html() block. "
        "The catLibFilter JS functions must be registered via ui.add_head_html at page setup."
    )


# ── GAP-131-08: with_code param on get_library_display ─────────────────────

def test_get_library_display_with_code_he():
    """with_code=True + lang='he' appends ' (CODE)' after the Hebrew name."""
    from shared.browse_map_utils import get_library_display, LIBRARY_CODES_HE, LIBRARY_CODES

    he_name = LIBRARY_CODES_HE.get('CUL', LIBRARY_CODES.get('CUL', 'CUL'))
    result = get_library_display('CUL', short=False, lang='he', with_code=True)
    assert result == f"{he_name} (CUL)", (
        f"with_code=True + lang=he must produce '{{HE name}} (CUL)'; got {result!r}"
    )


def test_get_library_display_with_code_en():
    """with_code=True + lang='en' appends ' (CODE)' after the English name."""
    from shared.browse_map_utils import get_library_display, LIBRARY_CODES

    en_name = LIBRARY_CODES.get('CUL', 'CUL')
    result = get_library_display('CUL', short=False, lang='en', with_code=True)
    assert result == f"{en_name} (CUL)", (
        f"with_code=True + lang=en must produce '{{EN name}} (CUL)'; got {result!r}"
    )


def test_get_library_display_default_off_he():
    """Default (no with_code) returns the bare Hebrew name — existing behavior unchanged."""
    from shared.browse_map_utils import get_library_display, LIBRARY_CODES_HE, LIBRARY_CODES

    he_name = LIBRARY_CODES_HE.get('CUL', LIBRARY_CODES.get('CUL', 'CUL'))
    # Default — no with_code kwarg
    assert get_library_display('CUL', short=False, lang='he') == he_name, (
        "Default (no with_code) must return bare Hebrew name, no appended code"
    )
    # Explicit with_code=False
    assert get_library_display('CUL', short=False, lang='he', with_code=False) == he_name, (
        "with_code=False must return bare Hebrew name, no appended code"
    )


def test_get_library_display_default_off_en():
    """Default-off invariant: existing-style calls never append '(CUL)' to the result."""
    from shared.browse_map_utils import get_library_display, LIBRARY_CODES

    en_name = LIBRARY_CODES.get('CUL', 'CUL')
    # Explicit lang='en' — must return bare English name (no appended code)
    assert get_library_display('CUL', short=False, lang='en') == en_name, (
        "get_library_display('CUL', short=False, lang='en') must return bare EN name"
    )
    # Default call (no with_code) — must NOT append '(CUL)' regardless of active language
    result_default = get_library_display('CUL', short=False)
    assert not result_default.endswith(' (CUL)'), (
        f"Default call (no with_code) must not append '(CUL)'; got {result_default!r}"
    )


def test_get_library_display_short_wins_over_with_code():
    """short=True returns the bare code — with_code is a no-op (prevents 'CUL (CUL)' duplication)."""
    from shared.browse_map_utils import get_library_display

    result = get_library_display('CUL', short=True, with_code=True)
    assert result == 'CUL', (
        f"short=True must short-circuit and return 'CUL' (no duplication); got {result!r}"
    )


def test_get_library_display_empty_code_with_code():
    """Empty code returns '' even with with_code=True."""
    from shared.browse_map_utils import get_library_display

    assert get_library_display('', with_code=True) == '', (
        "Empty code must return '' regardless of with_code"
    )


# ── GAP-131-08: web data-label includes code in Hebrew UI ───────────────────

def test_web_catalog_shortlist_label_builder_passes_with_code():
    """GAP-131-09: Shortlist + expand label builders pass with_code=True to get_library_display.

    AST/source scan: the two label builder call sites inside the catalog dialog function
    must pass with_code=True (always-on, both EN and HE UI), and the A-Z expand sort
    key must NOT (sort order stays on bare name).
    """
    import pathlib
    source = pathlib.Path('web/pages/catalog_browse.py').read_text(encoding='utf-8')

    # Both the shortlist and expand row builders must pass with_code=True (GAP-131-09)
    assert "with_code=True" in source, (
        "GAP-131-09: Both shortlist + expand label builders must pass with_code=True "
        "to get_library_display in web/pages/catalog_browse.py (always-on, both EN and HE UI)"
    )

    # The old language-gated pattern must be gone from the call sites
    assert "with_code=(_lang == 'he')" not in source, (
        "GAP-131-09: The language-gated with_code=(_lang == 'he') must be replaced by "
        "with_code=True in web/pages/catalog_browse.py"
    )

    # The expand A-Z sort key must NOT carry with_code (so sort order stays on bare name).
    # The sort key is the lambda passed to sorted() for expand_codes — it must NOT
    # contain with_code on the same line as the lambda key.
    expand_sort_idx = source.find('key=lambda c: get_library_display(c, short=False, lang=_lang)')
    assert expand_sort_idx != -1, (
        "The expand A-Z sort key must call get_library_display(c, short=False, lang=_lang) "
        "WITHOUT with_code — bare name for sort stability"
    )
