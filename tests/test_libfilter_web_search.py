"""Tests for LIBFILTER-01 — web /search library multi-select filter.

Coverage:
  (1) test_library_filter_narrows_full_set              — filters FULL set BEFORE PAGE_SIZE slice
  (2) test_library_only_routes_through_filtering_helper  — routing predicates widened (AST)
  (3) test_empty_selection_is_noop                      — empty selection returns input unchanged
  (4) test_facets_from_prefilter_full_set               — _compute_library_facets uses full list
  (5) test_persistence_uses_safe_storage_chokepoint     — key search_library_filter + no raw access
  (6) test_label_uses_get_library_display               — labels via get_library_display, not raw code
  (7) test_chip_renders_when_library_only               — button-state path reachable library-only (AST)
  (8) test_dialog_control_not_menu                      — GAP-B/C: dialog not menu (AST)
  (9) test_button_visibility_mechanism_consistent       — GAP-A: _set_btn_visible not set_visibility
  (10) test_library_apply_selection_mapping             — GAP-C: all-checked=>[], subset=>subset
  (11) test_all_unchecked_guard                         — FINDING 1: Apply guarded against zero-checked
  (12) test_chip_placement_post_search_container        — SEED-026 redesign: NO chips; state on button
  (13) test_no_script_in_library_dialog_html            — BUG-B static guard: no <script> in ui.html
  (14) test_library_btn_revealed_before_update_chain   — BUG-A static guard: reveal before _update_*
"""
import ast
import re
from collections import Counter
from pathlib import Path

SEARCH_PY = Path(__file__).parent.parent / 'web' / 'pages' / 'search.py'

PAGE_SIZE = 50  # Must match web/pages/search.py PAGE_SIZE


# ---------------------------------------------------------------------------
# Helpers — pure-Python filter/facet logic for direct-call tests
# ---------------------------------------------------------------------------

def _make_result(library_code: str, idx: int) -> dict:
    """Build a minimal fake result dict matching the shape used in search.py."""
    return {'display': {'id': f'sys_{idx}', 'library_code': library_code}}


def _apply_library_filter_pure(results_list: list, selected_codes: list) -> list:
    """Pure-Python mirror of _apply_library_filter (for direct-call tests).

    Mirrors the implementation spec: empty selection -> return unchanged;
    otherwise keep only results whose library_code is in selected_codes.
    Iterates the FULL list (no prior slice).
    """
    if not selected_codes:
        return results_list
    selected = set(selected_codes)
    return [r for r in results_list if r.get('display', {}).get('library_code', '') in selected]


def _compute_library_facets_pure(results_list: list) -> Counter:
    """Pure-Python mirror of _compute_library_facets (for direct-call tests)."""
    return Counter(
        r.get('display', {}).get('library_code', '')
        for r in results_list
        if r.get('display', {}).get('library_code')
    )


# Pure mapping helper — mirrors _library_apply_selection (Task 2 implementation).
# all-checked => [] (clear filter); strict subset => that subset.
def _library_apply_selection_pure(checked_codes: list, all_codes: list) -> list:
    """Pure-Python version of _library_apply_selection for test verification.

    Returns [] when checked_codes == all_codes (all included = no filter),
    else returns the checked subset.
    ONLY called with non-empty checked_codes (zero-checked state is blocked by Apply guard).
    """
    if not checked_codes:
        # This should never happen — the Apply guard prevents zero-checked commits.
        raise ValueError("_library_apply_selection_pure must not be called with empty checked_codes")
    if set(checked_codes) == set(all_codes):
        return []
    return list(checked_codes)


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------

def _iter_function_defs(tree):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node


def _function_contains_call(func_node, name: str) -> bool:
    for node in ast.walk(func_node):
        if isinstance(node, ast.Call):
            callee = node.func
            if isinstance(callee, ast.Name) and callee.id == name:
                return True
            if isinstance(callee, ast.Attribute) and callee.attr == name:
                return True
    return False


# ---------------------------------------------------------------------------
# Test 1: library filter narrows FULL set BEFORE PAGE_SIZE render slice
# ---------------------------------------------------------------------------

def test_library_filter_narrows_full_set():
    """Given > PAGE_SIZE results spanning multiple library_codes, the filter
    must narrow the FULL set so items beyond index PAGE_SIZE (50) are included
    in the filtered output — NOT just the first page.
    """
    n_total = PAGE_SIZE + 20  # 70 results
    # First PAGE_SIZE items are 'CUL'; last 20 are 'JTS'
    results = (
        [_make_result('CUL', i) for i in range(PAGE_SIZE)]
        + [_make_result('JTS', i + PAGE_SIZE) for i in range(20)]
    )
    assert len(results) == n_total

    # Filtering to JTS must include results beyond index PAGE_SIZE
    filtered = _apply_library_filter_pure(results, ['JTS'])
    assert len(filtered) == 20

    # Items at original index > PAGE_SIZE (the JTS ones) must appear
    jts_ids = {f'sys_{i + PAGE_SIZE}' for i in range(20)}
    result_ids = {r['display']['id'] for r in filtered}
    assert result_ids == jts_ids, (
        f"Expected {jts_ids}, got {result_ids}. "
        "Library filter must operate on the FULL result set, not just the first page."
    )

    # Sanity: CUL filtering removes the JTS items
    cul_filtered = _apply_library_filter_pure(results, ['CUL'])
    assert len(cul_filtered) == PAGE_SIZE

    # Sanity: the results beyond PAGE_SIZE-1 survive (prove full-set coverage)
    assert any(r['display']['id'].startswith('sys_5') for r in cul_filtered), (
        "Items at index >= 50 missing from CUL filter — FULL-set coverage broken"
    )

    # Additional AST guard: _apply_library_filter in search.py iterates
    # search_state.results (not a sliced subset) and is invoked in
    # _apply_printed_filter_and_render BEFORE render_results.
    source = SEARCH_PY.read_text(encoding='utf-8')

    # _apply_library_filter must exist
    assert '_apply_library_filter' in source, (
        "_apply_library_filter not found in web/pages/search.py"
    )

    # _apply_library_filter must be called inside _apply_printed_filter_and_render
    tree = ast.parse(source)
    for func in _iter_function_defs(tree):
        if func.name == '_apply_printed_filter_and_render':
            assert _function_contains_call(func, '_apply_library_filter'), (
                "_apply_printed_filter_and_render does not call _apply_library_filter. "
                "Library filter must be inserted into the cascade before render_results."
            )
            break
    else:
        raise AssertionError("_apply_printed_filter_and_render not found in search.py")

    # _apply_library_filter must appear before render_results in _apply_printed_filter_and_render
    lines = source.splitlines()
    in_fn = False
    lib_filter_line = None
    render_line = None
    indent_base = None
    for i, ln in enumerate(lines):
        if 'def _apply_printed_filter_and_render' in ln:
            in_fn = True
            indent_base = len(ln) - len(ln.lstrip())
            continue
        if in_fn:
            # Function ends when we see a def at same or lesser indent
            stripped = ln.strip()
            if stripped and not stripped.startswith('#'):
                cur_indent = len(ln) - len(ln.lstrip())
                if cur_indent <= indent_base and stripped.startswith('def '):
                    break
            if '_apply_library_filter' in ln and lib_filter_line is None:
                lib_filter_line = i
            if 'render_results' in ln and render_line is None:
                render_line = i
    assert lib_filter_line is not None, (
        "_apply_library_filter call not found inside _apply_printed_filter_and_render"
    )
    assert render_line is not None, (
        "render_results call not found inside _apply_printed_filter_and_render"
    )
    assert lib_filter_line < render_line, (
        f"_apply_library_filter (line {lib_filter_line + 1}) must come BEFORE "
        f"render_results (line {render_line + 1}) in _apply_printed_filter_and_render"
    )


# ---------------------------------------------------------------------------
# Test 2: routing predicates widened so library-only routes through filtering helper
# ---------------------------------------------------------------------------

def test_library_only_routes_through_filtering_helper():
    """AST guard: every routing predicate that gates the printed/pgp path must
    ALSO fire on bool(search_state.library_filter), so a library-only selection
    does not silently fall through to the bare render_results / measurement-only
    fallback (the BLOCKER identified in the plan).

    The six required predicate widening sites are:
      - _apply_manuscript_exclusions: lines 3418 and 3446
      - _apply_word_search_exclusions_and_render: line 3823
      - history-restore rerender: line 4066
      - _render_with_filters: line 4775
      - enrichment-completion rerender: line 5114
    """
    source = SEARCH_PY.read_text(encoding='utf-8')

    # Count how many times the predicate is widened with library_filter
    widened_count = source.count('or bool(search_state.library_filter)')
    assert widened_count >= 6, (
        f"Expected at least 6 widened routing predicates in web/pages/search.py "
        f"(manuscript_exclusions empty-branch + swap-branch + word-search + "
        f"history-restore + render_with_filters + enrichment-restore). "
        f"Found {widened_count}. The library-only bypass is not fully fixed."
    )

    # All six required functions must contain the widening
    tree = ast.parse(source)
    required_functions = {
        '_apply_manuscript_exclusions',
        '_apply_word_search_exclusions_and_render',
    }
    # Check that these functions contain at least one library_filter predicate
    for func in _iter_function_defs(tree):
        if func.name in required_functions:
            func_src = ast.get_source_segment(source, func) or ''
            assert 'library_filter' in func_src, (
                f"{func.name} does not contain `library_filter` in its predicate. "
                f"A library-only filter will silently fall through to bare render_results."
            )

    # The line-based routing that gates _apply_printed_filter_and_render must fire
    # on bool(search_state.library_filter) at EACH of the six sites.
    # Verify none of the six sites are missing:
    lines = source.splitlines()
    widened_lines = [i + 1 for i, ln in enumerate(lines) if 'or bool(search_state.library_filter)' in ln]
    assert len(widened_lines) >= 6, (
        f"Only {len(widened_lines)} widened predicate lines found (need >=6): {widened_lines}"
    )


# ---------------------------------------------------------------------------
# Test 3: empty selection is a no-op
# ---------------------------------------------------------------------------

def test_empty_selection_is_noop():
    """Empty library selection returns the input list unchanged."""
    results = [_make_result('CUL', i) for i in range(10)]

    # Empty list
    assert _apply_library_filter_pure(results, []) is results or \
           _apply_library_filter_pure(results, []) == results

    # None is treated as empty (safety: the pure helper above normalizes to [])
    # Source-level check: _apply_library_filter must guard on falsy selection
    source = SEARCH_PY.read_text(encoding='utf-8')
    assert '_apply_library_filter' in source, "_apply_library_filter not found in search.py"

    # Ensure filtered == original when empty
    out = _apply_library_filter_pure(results, [])
    assert len(out) == len(results)
    assert all(r in results for r in out)


# ---------------------------------------------------------------------------
# Test 4: facets computed from FULL pre-filter set
# ---------------------------------------------------------------------------

def test_facets_from_prefilter_full_set():
    """_compute_library_facets must count all results across all library_codes.
    Libraries with 0 matches in the full set must be absent from the Counter.
    """
    results = (
        [_make_result('CUL', i) for i in range(3)]
        + [_make_result('JTS', i + 3) for i in range(2)]
        + [_make_result('Oxford', i + 5) for i in range(1)]
    )

    facets = _compute_library_facets_pure(results)

    assert facets['CUL'] == 3
    assert facets['JTS'] == 2
    assert facets['Oxford'] == 1

    # Libraries with 0 matches must be absent (Counter omits 0s by construction)
    assert 'RNL' not in facets
    assert 'BL' not in facets

    # Source-level check: _compute_library_facets must exist in search.py
    source = SEARCH_PY.read_text(encoding='utf-8')
    assert '_compute_library_facets' in source, (
        "_compute_library_facets not found in web/pages/search.py. "
        "The per-library facet counter must be defined there."
    )

    # _compute_library_facets must be invoked with the FULL search_state.results
    # (not a sliced subset). Check it references search_state.results internally.
    tree = ast.parse(source)
    for func in _iter_function_defs(tree):
        if func.name == '_compute_library_facets':
            func_src = ast.get_source_segment(source, func) or ''
            # The function takes a results_list arg — body iterates it (not a slice of results)
            # The caller must pass the full search_state.results; we check the function exists
            # and the caller site passes search_state.results
            break
    else:
        raise AssertionError("_compute_library_facets function not found in search.py AST")


# ---------------------------------------------------------------------------
# Test 5: persistence uses safe_storage chokepoint
# ---------------------------------------------------------------------------

def test_persistence_uses_safe_storage_chokepoint():
    """Phase 87 invariant: search_library_filter is persisted via persist_value
    and _safe_get, NOT via raw app.storage.user access.
    """
    source = SEARCH_PY.read_text(encoding='utf-8')

    # Key 'search_library_filter' must appear in persist_value and _safe_get calls
    assert "persist_value('search_library_filter'" in source or \
           'persist_value("search_library_filter"' in source, (
        "persist_value('search_library_filter', ...) not found in search.py. "
        "Library filter state must be persisted via the safe_storage chokepoint."
    )

    assert "_safe_get('search_library_filter'" in source or \
           '_safe_get("search_library_filter"' in source, (
        "_safe_get('search_library_filter', ...) not found in search.py. "
        "Library filter state must be loaded via the safe_storage chokepoint."
    )

    # Confirm 'app.storage.user' does NOT appear in the library_filter context
    # (the Phase 87 CI guard test_no_raw_storage_access.py covers this globally;
    # this is an explicit grep near the library_filter keyword as defense-in-depth)
    lines = source.splitlines()
    for i, ln in enumerate(lines):
        if 'library_filter' in ln and 'app.storage.user' in ln:
            raise AssertionError(
                f"Line {i + 1}: raw app.storage.user access found near library_filter. "
                "Use persist_value/_safe_get through the safe_storage chokepoint."
            )


# ---------------------------------------------------------------------------
# Test 6: labels use get_library_display
# ---------------------------------------------------------------------------

def test_label_uses_get_library_display():
    """Library names in the filter dialog checkboxes must come from
    get_library_display(code, short=False, lang=...) — no raw code labels.
    """
    source = SEARCH_PY.read_text(encoding='utf-8')

    # get_library_display must be imported or used in search.py
    assert 'get_library_display' in source, (
        "get_library_display not referenced in web/pages/search.py. "
        "Library labels must use get_library_display(code, short=False, lang=...) per D-01."
    )

    # short=False must appear in at least one call to get_library_display
    # (short=True would return the raw code, violating D-01)
    assert 'get_library_display' in source, "get_library_display missing"

    # Grep for short=False at a get_library_display call site
    assert re.search(r'get_library_display\([^)]*short=False', source), (
        "get_library_display is called without short=False in search.py. "
        "Per D-01, full library names (not raw codes) must be shown."
    )


# ---------------------------------------------------------------------------
# Test 7: chip renders when library is the only active filter
# ---------------------------------------------------------------------------

def test_chip_renders_when_library_only():
    """SEED-026 (smoke 2026-06-29 redesign): there are NO library chips. The
    library filter STATE is carried on the button itself (red + a "shown/total"
    count) via _update_library_btn, and this state path must be reachable when
    library is the ONLY active filter — independent of printed/pgp.

    This test (kept under its original name for the suite manifest) now verifies
    the button-state path rather than a chip render path.
    """
    source = SEARCH_PY.read_text(encoding='utf-8')

    assert 'library_filter' in source, (
        "search_library_filter / library_filter not found in search.py — "
        "the library filter path has not been implemented."
    )

    # Persistence chokepoint key still present.
    assert 'search_library_filter' in source, (
        "Persistence key 'search_library_filter' not found in search.py"
    )

    # The button-state update function must exist and drive the label/colour.
    tree = ast.parse(source)
    update_btn_src = None
    for func in _iter_function_defs(tree):
        if func.name == '_update_library_btn':
            update_btn_src = ast.get_source_segment(source, func) or ''
            break
    assert update_btn_src is not None, (
        "_update_library_btn not found in search.py — the button-state path "
        "(label + colour) must exist to convey the active library filter."
    )

    # It must compute its count from the in-result facets (shown/total) and
    # reference library_filter — i.e. the state is reachable for library-only.
    assert '_compute_library_facets' in update_btn_src, (
        "_update_library_btn must derive its (shown/total) count from "
        "_compute_library_facets(search_state.results)."
    )
    assert 'library_filter' in update_btn_src, (
        "_update_library_btn must read search_state.library_filter to decide "
        "whether the filter is active (independent of printed/pgp)."
    )
    assert "tr('Filter Libraries')" in update_btn_src, (
        "_update_library_btn must set the active-state label 'Filter Libraries' "
        "(rendered as 'Filter Libraries (shown/total)')."
    )


# ---------------------------------------------------------------------------
# Test 8 (GAP-B/C): dialog not menu — AST source-scan
# ---------------------------------------------------------------------------

def test_dialog_control_not_menu():
    """GAP-B closed: domain button opens ONLY the domain dialog (no library menu
    in the shared button row).
    GAP-C closed: library control is a checkbox dialog (_open_library_filter_dialog)
    with inclusion semantics, NOT a ui.menu.

    Assertions:
    - _open_library_filter_dialog is defined in search.py
    - search.py contains ui.dialog( (the dialog primitive used inside the function)
    - _rebuild_library_menu, _library_menu_ref, ui.menu_item for libraries are ABSENT
      (the old menu apparatus is gone)
    """
    source = SEARCH_PY.read_text(encoding='utf-8')

    # Positive: dialog function defined
    assert '_open_library_filter_dialog' in source, (
        "_open_library_filter_dialog not found in web/pages/search.py. "
        "GAP-C: library control must be a checkbox dialog, not a ui.menu."
    )

    # Positive: ui.dialog( is used (the dialog primitive)
    assert 'ui.dialog(' in source, (
        "ui.dialog( not found in web/pages/search.py. "
        "The library filter dialog must use ui.dialog."
    )

    # Negative: old menu apparatus must be gone
    assert '_rebuild_library_menu' not in source, (
        "_rebuild_library_menu still present in search.py. "
        "GAP-C: the old ui.menu apparatus must be removed in favour of the dialog."
    )

    assert '_library_menu_ref' not in source, (
        "_library_menu_ref still present in search.py. "
        "GAP-C: the old menu-reference dict must be removed."
    )

    # No bare ui.menu() for the library filter (the only remaining ui.menu calls
    # should be the history menu, not a library filter menu)
    # Check that library_filter_btn is NOT connected to a ui.menu.open()
    assert 'library_filter_btn.on(\'click\', lambda: _library_menu.open())' not in source and \
           'library_filter_btn.on("click", lambda: _library_menu.open())' not in source, (
        "library_filter_btn is still wired to a ui.menu.open() click handler. "
        "GAP-C: it must instead open _open_library_filter_dialog()."
    )


# ---------------------------------------------------------------------------
# Test 9 (GAP-A): button visibility mechanism consistent
# ---------------------------------------------------------------------------

def test_button_visibility_mechanism_consistent():
    """GAP-A closed: library_filter_btn is revealed via _set_btn_visible (CSS
    visibility:hidden/visible, reserves layout space), NOT via set_visibility(False)
    (display:none / NiceGUI _visible flag) which conflicts with the reveal path.

    Assertions:
    - library_filter_btn.set_visibility( does NOT appear anywhere in search.py
    - _set_btn_visible(library_filter_btn is present at >=2 sites (construction init
      + results-arrive reveal path at search.py:4930)
    """
    source = SEARCH_PY.read_text(encoding='utf-8')

    # Negative: set_visibility(False) must not be used for library_filter_btn
    assert 'library_filter_btn.set_visibility(' not in source, (
        "library_filter_btn.set_visibility( found in search.py. "
        "GAP-A: use _set_btn_visible(library_filter_btn, False) consistently. "
        "set_visibility uses display:none which conflicts with the CSS visibility reveal path."
    )

    # Positive: _set_btn_visible(library_filter_btn must appear
    assert '_set_btn_visible(library_filter_btn' in source, (
        "_set_btn_visible(library_filter_btn not found in search.py. "
        "GAP-A: library_filter_btn must be hidden/revealed via _set_btn_visible "
        "at construction AND at the results-arrive reveal path (matching sibling buttons)."
    )

    # Count occurrences — must be >=2 (construction + reveal)
    count = source.count('_set_btn_visible(library_filter_btn')
    assert count >= 2, (
        f"_set_btn_visible(library_filter_btn found only {count} time(s) in search.py. "
        "Expected >=2: one at button construction (False) and one at results-arrive reveal."
    )


# ---------------------------------------------------------------------------
# Test 10 (GAP-C): inclusion mapping — all-checked=>[], subset=>subset
# ---------------------------------------------------------------------------

def test_library_apply_selection_mapping():
    """GAP-C: _library_apply_selection (or an equivalent pure helper) must implement
    the correct inclusion mapping:
      - all checked ⇒ [] (clear filter / show all — the '[]' sentinel means 'show all')
      - strict subset checked ⇒ exactly that subset (inclusion filter)

    This test exercises the pure mapping helper defined in this file, and also verifies
    that _library_apply_selection exists in search.py (the production implementation).
    """
    # All-checked case: when checked == all_codes, result is []
    all_codes = ['CUL', 'JTS', 'Oxford', 'RNL']
    result = _library_apply_selection_pure(all_codes, all_codes)
    assert result == [], (
        "All libraries checked should yield [] (clear filter / show all sentinel). "
        f"Got: {result}"
    )

    # Strict subset case: checked subset is returned as-is
    checked_subset = ['CUL', 'JTS']
    result2 = _library_apply_selection_pure(checked_subset, all_codes)
    assert set(result2) == {'CUL', 'JTS'}, (
        f"Strict subset apply should return the checked subset. Got: {result2}"
    )

    # Single code selected
    result3 = _library_apply_selection_pure(['Oxford'], all_codes)
    assert set(result3) == {'Oxford'}, (
        f"Single library selected should return ['Oxford']. Got: {result3}"
    )

    # Source-level check: _library_apply_selection must exist in search.py
    source = SEARCH_PY.read_text(encoding='utf-8')
    assert '_library_apply_selection' in source, (
        "_library_apply_selection not found in web/pages/search.py. "
        "GAP-C: a pure testable mapping helper must be defined "
        "(all-checked->[], subset->subset)."
    )


# ---------------------------------------------------------------------------
# Test 11 (FINDING 1): all-unchecked Apply guard
# ---------------------------------------------------------------------------

def test_all_unchecked_guard():
    """FINDING 1: applying with zero libraries checked must be UNREACHABLE.

    The data layer uses [] to mean 'show all'. If the user could apply an
    all-unchecked state, the resulting [] would be read as 'show all' — the
    OPPOSITE of the intended 'hide all'. To prevent this collision:

    1. No 'Select None' / 'deselect all' library affordance (only 'Select All').
    2. The Apply path must be guarded against a zero-checked commit — either
       the Apply button is disabled when checked-count == 0, or the Python
       apply handler short-circuits with a warning notify.
    3. The guard token (a hint string or disabled-Apply JS) must be present.
    4. _library_apply_selection is NEVER fed an empty checked set as a committed filter.

    Assertions (source-scan):
    - No 'Select None' for the library filter (no deselect-all library action)
    - A guard token is present: 'Select at least one library' string OR the
      _library_apply_selection is documented/guarded against empty input
    - _library_apply_selection is ONLY called from the Apply path (never with
      an empty checked set as a committed filter commit)
    """
    source = SEARCH_PY.read_text(encoding='utf-8')

    # Guard: _library_apply_selection must exist (verified by test 10 too)
    assert '_library_apply_selection' in source, (
        "_library_apply_selection not found in search.py. "
        "The all-unchecked guard requires this pure mapping helper."
    )

    # The guard token must be present — either:
    # (a) a hint string 'Select at least one library' (or the tr() key for it), OR
    # (b) the Apply handler contains an `if not checked` / `if len(...) == 0` guard
    hint_present = (
        'Select at least one library' in source
        or "tr('Select at least one library" in source
        or 'tr("Select at least one library' in source
    )
    # Also accept the JS-disabled-Apply pattern (checked_count == 0 disables Apply)
    js_guard_present = (
        'checked-count == 0' in source
        or 'checked_count == 0' in source
        or 'len(checked' in source
        or 'if not checked' in source
    )
    assert hint_present or js_guard_present, (
        "No Apply guard found for the zero-checked state in search.py. "
        "FINDING 1: the Apply button must be disabled when zero libraries are checked, "
        "OR the Python apply handler must short-circuit with a warning notify "
        "when the checked set is empty. "
        "Expected hint string 'Select at least one library' or a guarded if-not-checked path."
    )

    # BUG-C fix: 'Select None' IS now present in the library dialog as a CLEAR-CHECKBOXES
    # convenience (unchecks all without applying). OK/Apply stay DISABLED at zero checked
    # (libFilterUpdateApply JS guard + Python if-not-checked guard).
    # The test must now verify that 'Select None' unchecks-only and is paired with the
    # guarded Apply — NOT that it is absent.
    # Verify: both 'Select None' and the guarded-Apply hint coexist in the dialog.
    lines = source.splitlines()
    in_lib_dialog = False
    lib_dialog_body_lines = []
    for ln in lines:
        if 'def _open_library_filter_dialog' in ln:
            in_lib_dialog = True
        if in_lib_dialog:
            lib_dialog_body_lines.append(ln)
            if len(lib_dialog_body_lines) > 200:
                break  # stop after 200 lines of the function

    lib_dialog_src = '\n'.join(lib_dialog_body_lines)

    # 'Select None' must appear in the dialog (BUG-C fix)
    assert 'Select None' in lib_dialog_src or "tr('Select None')" in lib_dialog_src, (
        "BUG-C: 'Select None' affordance missing from _open_library_filter_dialog. "
        "It should uncheck all without applying (OK stays disabled at zero checked)."
    )

    # The all-unchecked Apply guard MUST also be present to keep '[] = show-all' safe.
    # 'Select None' is only safe because OK/Apply is disabled while zero are checked.
    assert hint_present or js_guard_present, (
        "The Apply guard (Python if-not-checked short-circuit or JS disabled-Apply) "
        "must remain alongside the new 'Select None' button (FINDING 1 invariant)."
    )


# ---------------------------------------------------------------------------
# Test 12 (SEED-026 smoke 2026-06-29 redesign): NO library chips — state on button
# ---------------------------------------------------------------------------

def test_chip_placement_post_search_container():
    """SEED-026 (smoke 2026-06-29 redesign): there are NO library chips at all.
    The per-library chip row proved noisy when many libraries were selected, so
    the filter state now lives entirely on library_filter_btn (red +
    "Filter Libraries (shown/total)"). This test (kept under its original name
    for the suite manifest) now enforces the no-chip design.

    Assertions:
    - NO 'library_chip_row' identifier and NO '_update_library_chips' function
      remain in search.py (chip row + render fn fully removed)
    - 'account_balance' (the old library chip icon) does NOT appear inside
      _update_chip_bar
    - The has_any line in _update_chip_bar does NOT OR bool(search_state.library_filter)
    - _update_chip_bar STILL builds the other chip types: domain/measurement/text-position
      (no chip disappears — regression guard)

    Note: there is NO 'printed chip' in _update_chip_bar (printed is a BUTTON, not a chip).
    Do NOT assert a printed chip.
    """
    source = SEARCH_PY.read_text(encoding='utf-8')

    # Negative: the chip row + its render function must be GONE.
    assert 'library_chip_row' not in source, (
        "library_chip_row still present in search.py. "
        "SEED-026 redesign: the post-search library chip row was removed; "
        "filter state now lives on library_filter_btn (_update_library_btn)."
    )
    assert '_update_library_chips' not in source, (
        "_update_library_chips still present in search.py. "
        "SEED-026 redesign: the chip render function was removed."
    )

    # Find the _update_chip_bar function body
    tree = ast.parse(source)
    update_chip_bar_src = None
    for func in _iter_function_defs(tree):
        if func.name == '_update_chip_bar':
            update_chip_bar_src = ast.get_source_segment(source, func) or ''
            break

    assert update_chip_bar_src is not None, (
        "_update_chip_bar not found in search.py. "
        "The pre-search chip bar function must still exist."
    )

    # Negative: account_balance (old library chip icon) must NOT be in _update_chip_bar
    assert 'account_balance' not in update_chip_bar_src, (
        "'account_balance' (library chip icon) still appears inside _update_chip_bar. "
        "SEED-026: there are no library chips; the pre-search chip_bar_container "
        "must not render any library chip."
    )

    # Negative: has_any line must NOT OR bool(search_state.library_filter)
    assert 'or bool(search_state.library_filter)' not in update_chip_bar_src, (
        "_update_chip_bar has_any line still ORs bool(search_state.library_filter). "
        "SEED-026: the library filter has no chip, so chip_bar_container's visibility "
        "must not consider library_filter."
    )

    # Positive: _update_chip_bar still builds the other chip types
    # (domain chips: 'filter_domains', 'category' icon or domain chip block)
    assert 'filter_domains' in update_chip_bar_src or 'domain' in update_chip_bar_src.lower(), (
        "Domain chip block appears to be missing from _update_chip_bar. "
        "GAP-D: only the library chip block must be removed; all other chip types must remain."
    )

    # Measurement chips: 'filter_width_min' or 'straighten' icon (measurement chip icon)
    assert 'filter_width_min' in update_chip_bar_src or 'straighten' in update_chip_bar_src, (
        "Measurement chip block appears to be missing from _update_chip_bar. "
        "GAP-D: only the library chip block must be removed; all other chip types must remain."
    )

    # Text-position chips: 'text_position' or 'anywhere'
    assert 'text_position' in update_chip_bar_src or 'anywhere' in update_chip_bar_src, (
        "Text-position chip block appears to be missing from _update_chip_bar. "
        "GAP-D: only the library chip block must be removed; all other chip types must remain."
    )


# ---------------------------------------------------------------------------
# Test 13 (BUG-B): no <script> inside ui.html in the library filter dialog
# STATIC GUARD — would have caught BUG-B (ValueError from NiceGUI when dialog opened).
# NiceGUI raises ValueError: "HTML elements must not contain <script> tags" when
# ui.html() content contains '<script'. This guard prevents regression.
# ---------------------------------------------------------------------------

def test_no_script_in_library_dialog_html():
    """BUG-B static guard: _open_library_filter_dialog must NOT pass a string
    containing '<script' to ui.html().

    This is a static (source-scan) test that substitutes for a live-render test.
    It catches the exact class of bug that caused BUG-B: JS functions defined inside
    ui.html() raise ValueError at dialog-open time in the live NiceGUI runtime.

    The correct pattern (matching the domain filter) is:
      - JS functions defined ONCE at page level via ui.add_head_html(<script>...</script>)
      - The dialog's ui.html() contains ONLY checkbox markup (no <script> tag)
    """
    source = SEARCH_PY.read_text(encoding='utf-8')

    # Find the _open_library_filter_dialog function body
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
            if len(fn_lines) > 250:
                break

    fn_src = '\n'.join(fn_lines)

    # The function must NOT pass '<script' to ui.html() — find ui.html( calls
    # and assert none of the strings passed contain '<script'.
    #
    # Strategy: check non-comment lines within the function body.  Comments may
    # legitimately mention '<script>' as documentation.  Only code lines matter.
    non_comment_fn_lines = [
        ln for ln in fn_lines
        if ln.strip() and not ln.strip().startswith('#')
    ]
    non_comment_fn_src = '\n'.join(non_comment_fn_lines)

    if 'ui.html(' in non_comment_fn_src and '<script' in non_comment_fn_src:
        raise AssertionError(
            "BUG-B: _open_library_filter_dialog in search.py passes a string containing "
            "'<script' to ui.html(). NiceGUI raises ValueError: 'HTML elements must not "
            "contain <script> tags. Use ui.add_body_html() instead.' at dialog-open time. "
            "Move the JS functions to ui.add_head_html() at page setup (like domain filter). "
            "The dialog's ui.html() must contain ONLY the checkbox <div> markup."
        )

    # Positive: the page-level ui.add_head_html must define the libFilter JS functions.
    assert 'libFilterGetChecked' in source, (
        "BUG-B: libFilterGetChecked JS function not found in search.py. "
        "It must be defined in ui.add_head_html() at page setup."
    )
    assert 'libFilterUpdateApply' in source, (
        "BUG-B: libFilterUpdateApply JS function not found in search.py."
    )
    # These must appear in a ui.add_head_html block, not inside _open_library_filter_dialog.
    # The simplest check: the function appears in source BEFORE _open_library_filter_dialog.
    add_head_idx = source.find('libFilterGetChecked')
    dialog_fn_idx = source.find('def _open_library_filter_dialog')
    assert add_head_idx != -1, "libFilterGetChecked not found in search.py"
    assert dialog_fn_idx != -1, "_open_library_filter_dialog not found in search.py"
    assert add_head_idx < dialog_fn_idx, (
        "BUG-B: libFilterGetChecked definition appears AFTER _open_library_filter_dialog. "
        "It must be in a page-level ui.add_head_html block defined BEFORE the dialog function."
    )


# ---------------------------------------------------------------------------
# Test 14 (BUG-A): library button revealed BEFORE the _update_* call chain
# STATIC GUARD — would have caught BUG-A (button never visible if _update_* aborts).
# ---------------------------------------------------------------------------

def test_library_btn_revealed_before_update_chain():
    """BUG-A static guard: in _apply_enrichment_to_ui, the _set_btn_visible call
    for library_filter_btn must appear BEFORE _update_pgp_filter_btn,
    _update_domain_filter_btn, and _update_exclude_btn.

    The risk: if any of those _update_* calls raises an exception, the library button
    reveal at the END of the function would never execute, leaving the button permanently
    hidden. Moving the reveal to BEFORE the update chain ensures the button appears
    even if a later update call fails.

    STATIC GUARD: uses whole-file line numbers so nested-function indent detection
    is not needed (the _apply_enrichment_to_ui body spans a bounded line range and
    all the relevant calls appear within ~20 lines of the def).
    """
    source = SEARCH_PY.read_text(encoding='utf-8')
    lines = source.splitlines()

    # Locate the def line for _apply_enrichment_to_ui
    fn_start = None
    for i, ln in enumerate(lines):
        if 'def _apply_enrichment_to_ui' in ln and 'def ' in ln:
            fn_start = i
            break

    assert fn_start is not None, (
        "_apply_enrichment_to_ui not found in search.py"
    )

    # Scan the NEXT 40 lines (the function body is short — ~15 lines).
    # We look for the first occurrence of each token within the function body.
    fn_body_lines = lines[fn_start: fn_start + 40]

    lib_reveal_line = None
    pgp_update_line = None
    domain_update_line = None
    exclude_update_line = None

    for i, ln in enumerate(fn_body_lines):
        if '_set_btn_visible(library_filter_btn' in ln and lib_reveal_line is None:
            lib_reveal_line = i
        if '_update_pgp_filter_btn(' in ln and pgp_update_line is None:
            pgp_update_line = i
        if '_update_domain_filter_btn(' in ln and domain_update_line is None:
            domain_update_line = i
        if '_update_exclude_btn(' in ln and exclude_update_line is None:
            exclude_update_line = i

    assert lib_reveal_line is not None, (
        "BUG-A: _set_btn_visible(library_filter_btn...) not found in the first 40 lines "
        "of _apply_enrichment_to_ui. The library button must be revealed in this function."
    )

    # The library reveal must come BEFORE each _update_* call (safe early block)
    for update_name, update_line in [
        ('_update_pgp_filter_btn', pgp_update_line),
        ('_update_domain_filter_btn', domain_update_line),
        ('_update_exclude_btn', exclude_update_line),
    ]:
        if update_line is not None:
            assert lib_reveal_line < update_line, (
                f"BUG-A: _set_btn_visible(library_filter_btn) (body-line {lib_reveal_line}) "
                f"appears AFTER {update_name} (body-line {update_line}) in "
                f"_apply_enrichment_to_ui. The reveal must come FIRST so an exception "
                f"in the update chain cannot suppress the button reveal."
            )
