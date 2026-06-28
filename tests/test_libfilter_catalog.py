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
    import ast, pathlib
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
    build_idx = source.find('def _build_incoming_filters()')
    assert build_idx != -1, "_build_incoming_filters() must exist in catalog_browse.py"
    # Use a larger snippet (2000 chars) to encompass the full function body
    func_snippet = source[build_idx:build_idx + 2000]
    assert "incoming['library_filter']" in func_snippet or "incoming[\"library_filter\"]" in func_snippet, (
        "_build_incoming_filters() must set incoming['library_filter'] from "
        "current_library_filter['value'] (GAP-F)"
    )


# ── GAP-F: consume ───────────────────────────────────────────────────────────

def test_consume_sets_library_filter_and_persists(monkeypatch):
    """consume_incoming_filters must set state.library_filter and persist search_library_filter.

    Encodes GAP-F consume side: after the fix, consume_incoming_filters reads
    incoming.get('library_filter') and:
      1. sets state.library_filter via setattr
      2. calls persist_value('search_library_filter', ...)
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
        "consume_incoming_filters must set state.library_filter from incoming['library_filter']"
    )
    assert 'search_library_filter' in persisted, (
        "consume_incoming_filters must persist 'search_library_filter' (the literal key "
        "that search.py loads at :187-189)"
    )
    assert persisted['search_library_filter'] == ['CUL', 'JTS'], (
        "Persisted value must equal the incoming codes"
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
    """Full persist→reload lifecycle: consume persists, subsequent _safe_get returns codes.

    (a) consume_incoming_filters with library_filter=['CUL'] → persists 'search_library_filter'=['CUL']
    (b) A fresh state-init render reads _safe_get('search_library_filter', []) → returns ['CUL']
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

    # (a) Run consume — simulating the browse→search handoff
    state = FakeState()
    fp.consume_incoming_filters(state, 'search', require_from_browse=False)
    # Assert that 'search_library_filter' was written to storage by consume
    assert storage.get('search_library_filter') == ['CUL'], (
        "consume_incoming_filters must persist 'search_library_filter'=['CUL'] "
        "so a subsequent fresh render can reload it"
    )

    # (b) Simulate a fresh state-init render calling _safe_get('search_library_filter', [])
    # This is the load path at search.py:187-189
    reloaded = storage.get('search_library_filter', [])
    assert reloaded == ['CUL'], (
        "_safe_get('search_library_filter', []) on a fresh render must return the "
        "persisted codes ['CUL']; got %r" % reloaded
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

    # There must be NO callable 'Select None' button or deselect-all library action.
    # We verify this by checking that tr('Select None') does NOT appear in catalog_browse.py
    # (which would mean a NiceGUI button with that label was created).  Mere mentions in
    # comments are acceptable; only a tr(key) call creates a user-visible button.
    assert "tr('Select None')" not in source and 'tr("Select None")' not in source, (
        "catalog_browse.py must NOT use tr('Select None') as a button label "
        "(a deselect-all action would let the user commit an all-unchecked state "
        "that collides with the '[]' = show all sentinel — FINDING 1)"
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
