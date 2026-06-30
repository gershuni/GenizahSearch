# -*- coding: utf-8 -*-
"""Tests for DMF dual-mode library filter — Phase 131 Plan 01 — Parallels Surface.

Coverage (mirrors the pure-mirror + AST-scan pattern of test_dual_mode_library_filter.py):

  (1)  test_apply_parallels_show_only            — Show-only: keep rows IN codes (DMF-09)
  (2)  test_apply_parallels_hide                 — Hide: keep rows NOT IN codes (DMF-09)
  (3)  test_apply_parallels_empty_returns_all    — empty codes in either mode = show all (D-05/D-08)
  (4)  test_parallels_migrate_list_to_show_only  — plain list -> show_only (DMF-09)
  (5)  test_parallels_migrate_empty_to_hide      — empty list -> hide/[] (DMF-09)
  (6)  test_parallels_migrate_dict_roundtrip     — dict shape round-trips both modes
  (7)  test_parallels_migrate_bogus_mode         — unknown mode -> 'hide' fallback
  (8)  test_parallels_local_absent_from_options  — LOCAL not in filter options (DMF-10)
  (9)  test_ast_parallels_state_fields           — AST: ParallelsState defines library_filter + library_mode
  (10) test_ast_parallels_restore_has_isinstance_branches — AST: restore has list+dict branches
  (11) test_ast_parallels_restore_sanitizes      — AST: restore calls sanitize_library_codes
  (12) test_ast_parallels_apply_local_guard      — AST: dialog/apply carries c != 'LOCAL' guard
  (13) test_ast_parallels_library_codes_with_manuscripts — AST: dialog references library_codes_with_manuscripts
  (14) test_ast_parallels_local_helper_no_search_import  — Codex N2: local helper, no search.py import
  (15) test_ast_parallels_btn_pluralized_keys    — REAL Phase-130 button keys (DMF-10)
  (16) test_ast_parallels_filter_before_export   — Codex #6: _apply_parallels_library_filter( before set_parallels_export(
"""

import ast
import re
from pathlib import Path

from shared.browse_map_utils import LIBRARY_CODES, sanitize_library_codes

PARALLELS_PY = Path(__file__).parent.parent / 'web' / 'pages' / 'parallels.py'

# Canonical valid codes — LOCAL is in LIBRARY_CODES but must be excluded from all web filter UI.
_VALID_CODES = set(LIBRARY_CODES) - {'LOCAL'}


# ---------------------------------------------------------------------------
# Pure-Python mirrors
# ---------------------------------------------------------------------------

def _make_parallels_result(library_code: str, idx: int = 0) -> dict:
    """Minimal fake parallels result dict — mirrors two possible library_code locations."""
    return {
        'library_code': library_code,
        'display': {'id': f'sys_{idx}', 'library_code': library_code},
    }


def _apply_parallels_library_filter(results_list: list, mode: str, codes: set) -> list:
    """Pure mirror of _apply_parallels_library_filter.

    Mode branch:
      - 'show_only': keep rows whose library_code (or display.library_code) IN codes;
                     empty codes = show all (D-08).
      - 'hide'     : keep rows whose library_code (or display.library_code) NOT IN codes;
                     empty codes = show all (D-05).

    Mirrors web/pages/search.py::_apply_library_filter (3830-3853).
    """
    if mode == 'show_only':
        if not codes:
            return results_list  # empty Show-only = show all
        return [r for r in results_list
                if r.get('library_code', '') in codes
                or r.get('display', {}).get('library_code', '') in codes]
    else:  # hide
        if not codes:
            return results_list  # empty Hide = show all
        return [r for r in results_list
                if r.get('library_code', '') not in codes
                and r.get('display', {}).get('library_code', '') not in codes]


def _migrate_library_filter(raw, valid_codes: set) -> dict:
    """Pure mirror of the parallels restore/migration path.

    Branches (matching parallels.py restore block after Plan 05):
      - list  (legacy): non-empty -> show_only + sanitized; empty -> hide + []
      - dict  (new shape): validated mode + sanitized codes; unknown mode -> 'hide';
               show_only + empty codes -> normalised to hide/[] (Codex HIGH fix).
      - else  (None/garbage): fresh default hide + []
    """
    def _sanitize(lst):
        if not isinstance(lst, list):
            return []
        return [c for c in lst if isinstance(c, str) and c in valid_codes and c != 'LOCAL']

    if isinstance(raw, list):
        codes = _sanitize(raw)
        if codes:
            return {'mode': 'show_only', 'codes': codes}
        return {'mode': 'hide', 'codes': []}
    if isinstance(raw, dict):
        mode = raw.get('mode', 'hide')
        if mode not in ('show_only', 'hide'):
            mode = 'hide'
        codes = _sanitize(raw.get('codes'))
        if mode == 'show_only' and not codes:
            mode = 'hide'
        return {'mode': mode, 'codes': codes}
    return {'mode': 'hide', 'codes': []}


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------

def _iter_function_defs(tree):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node


def _extract_function_lines(source: str, func_name: str, max_lines: int = 300) -> str:
    """Crude line-based extraction for nested functions not reachable by AST segment."""
    lines = source.splitlines()
    in_fn = False
    fn_lines = []
    fn_indent = None
    for ln in lines:
        if f'def {func_name}' in ln:
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
            if len(fn_lines) > max_lines:
                break
    return '\n'.join(fn_lines)


# ---------------------------------------------------------------------------
# (1) Show-only: keep rows IN codes (DMF-09)
# ---------------------------------------------------------------------------

def test_apply_parallels_show_only():
    """Show-only with codes={'CUL'} over [CUL, CUL, JTS] -> 2 CUL rows (DMF-09)."""
    results = [
        _make_parallels_result('CUL', 0),
        _make_parallels_result('CUL', 1),
        _make_parallels_result('JTS', 2),
    ]
    out = _apply_parallels_library_filter(results, 'show_only', {'CUL'})
    assert len(out) == 2, f"Expected 2 CUL rows, got {len(out)}"
    assert all(r['library_code'] == 'CUL' for r in out)

    # Multi-code
    results2 = [_make_parallels_result('CUL', 0), _make_parallels_result('RNL', 1),
                _make_parallels_result('JTS', 2)]
    out2 = _apply_parallels_library_filter(results2, 'show_only', {'CUL', 'RNL'})
    assert len(out2) == 2
    assert {r['library_code'] for r in out2} == {'CUL', 'RNL'}


# ---------------------------------------------------------------------------
# (2) Hide: keep rows NOT IN codes (DMF-09)
# ---------------------------------------------------------------------------

def test_apply_parallels_hide():
    """Hide with codes={'RNL'} over [CUL, RNL, JTS] -> CUL+JTS rows (DMF-09)."""
    results = [
        _make_parallels_result('CUL', 0),
        _make_parallels_result('RNL', 1),
        _make_parallels_result('JTS', 2),
    ]
    out = _apply_parallels_library_filter(results, 'hide', {'RNL'})
    assert len(out) == 2
    libs = {r['library_code'] for r in out}
    assert libs == {'CUL', 'JTS'}, f"Expected CUL+JTS, got {libs}"
    assert all(r['library_code'] != 'RNL' for r in out)


# ---------------------------------------------------------------------------
# (3) Empty codes in either mode = show all (D-05/D-08)
# ---------------------------------------------------------------------------

def test_apply_parallels_empty_returns_all():
    """Empty codes in Show-only and Hide both return the full list (D-05/D-08)."""
    results = [
        _make_parallels_result('CUL', 0),
        _make_parallels_result('RNL', 1),
        _make_parallels_result('JTS', 2),
    ]
    # Empty Show-only = show all
    out_show = _apply_parallels_library_filter(results, 'show_only', set())
    assert len(out_show) == 3, f"Empty Show-only must return all 3, got {len(out_show)}"

    # Empty Hide = show all
    out_hide = _apply_parallels_library_filter(results, 'hide', set())
    assert len(out_hide) == 3, f"Empty Hide must return all 3, got {len(out_hide)}"


# ---------------------------------------------------------------------------
# (4) Migrate: plain list -> show_only (DMF-09)
# ---------------------------------------------------------------------------

def test_parallels_migrate_list_to_show_only():
    """Non-empty plain list ['CUL','JTS'] -> mode='show_only', codes=['CUL','JTS'] (DMF-09)."""
    result = _migrate_library_filter(['CUL', 'JTS'], _VALID_CODES)
    assert result['mode'] == 'show_only', (
        f"Non-empty list must migrate to show_only, got {result['mode']!r}"
    )
    assert set(result['codes']) == {'CUL', 'JTS'}


# ---------------------------------------------------------------------------
# (5) Migrate: empty list -> hide/[] (DMF-09)
# ---------------------------------------------------------------------------

def test_parallels_migrate_empty_to_hide():
    """Empty plain list [] -> mode='hide', codes=[] (DMF-09)."""
    result = _migrate_library_filter([], _VALID_CODES)
    assert result['mode'] == 'hide'
    assert result['codes'] == []


# ---------------------------------------------------------------------------
# (6) Migrate: dict shape round-trips both modes
# ---------------------------------------------------------------------------

def test_parallels_migrate_dict_roundtrip():
    """Dict shape passes through both modes unchanged."""
    # Hide round-trip
    result = _migrate_library_filter({'mode': 'hide', 'codes': ['RNL']}, _VALID_CODES)
    assert result['mode'] == 'hide'
    assert result['codes'] == ['RNL']

    # Show-only round-trip
    result2 = _migrate_library_filter({'mode': 'show_only', 'codes': ['CUL', 'JTS']}, _VALID_CODES)
    assert result2['mode'] == 'show_only'
    assert set(result2['codes']) == {'CUL', 'JTS'}


# ---------------------------------------------------------------------------
# (7) Migrate: unknown mode -> 'hide' fallback
# ---------------------------------------------------------------------------

def test_parallels_migrate_bogus_mode():
    """Dict with unknown mode falls back to 'hide'."""
    result = _migrate_library_filter({'mode': 'exclude_all', 'codes': ['CUL']}, _VALID_CODES)
    assert result['mode'] == 'hide', (
        f"Unknown mode must fall back to 'hide', got {result['mode']!r}"
    )


# ---------------------------------------------------------------------------
# (8) LOCAL absent from filter options (DMF-10)
# ---------------------------------------------------------------------------

def test_parallels_local_absent_from_options():
    """LOCAL is never present in the filter options when applied to a mock LIBRARY_CODES
    that includes LOCAL (DMF-10 — pure function test, no production code)."""
    # Mock LIBRARY_CODES-like set that includes LOCAL
    mock_codes = {'CUL', 'JTS', 'RNL', 'LOCAL'}
    valid_codes_excluding_local = mock_codes - {'LOCAL'}

    # Shortlist build mirrors the dialog expand section: c != 'LOCAL' guard
    mock_facets = {'CUL': 5, 'LOCAL': 99, 'JTS': 3, 'RNL': 1}
    shortlist = [c for c in mock_facets if c in valid_codes_excluding_local and c != 'LOCAL']
    assert 'LOCAL' not in shortlist, "LOCAL must never appear in shortlist"

    # Migration from a list that includes LOCAL: LOCAL must be dropped
    result = _migrate_library_filter(['CUL', 'LOCAL', 'RNL'], valid_codes_excluding_local)
    assert 'LOCAL' not in result.get('codes', []), (
        f"Migration must drop LOCAL from codes, got {result['codes']!r}"
    )


# ---------------------------------------------------------------------------
# (9) AST: ParallelsState defines library_filter + library_mode (DMF-09)
# ---------------------------------------------------------------------------

def test_ast_parallels_state_fields():
    """ParallelsState.__init__ must define self.library_filter and self.library_mode
    with a 'hide' default (DMF-09 — D-05 fresh-user default).

    NOTE: RED until Plan 05 lands — intended Wave-0 state.
    """
    source = PARALLELS_PY.read_text(encoding='utf-8')
    # Extract ParallelsState.__init__ body (crude line scan)
    lines = source.splitlines()
    in_class = False
    in_init = False
    init_lines = []
    for ln in lines:
        if 'class ParallelsState' in ln:
            in_class = True
        if in_class and 'def __init__' in ln:
            in_init = True
        if in_init:
            init_lines.append(ln)
            if len(init_lines) > 1 and ln.strip() and not ln.strip().startswith('#'):
                # Stop at next def at same or shallower indent
                cur_indent = len(ln) - len(ln.lstrip())
                if cur_indent <= 8 and ln.strip().startswith('def ') and len(init_lines) > 5:
                    break
            if len(init_lines) > 80:
                break
    init_src = '\n'.join(init_lines)

    assert 'library_filter' in init_src, (
        "ParallelsState.__init__ must define self.library_filter (DMF-09). RED until Plan 05."
    )
    assert 'library_mode' in init_src, (
        "ParallelsState.__init__ must define self.library_mode (DMF-09). RED until Plan 05."
    )
    assert "'hide'" in init_src or '"hide"' in init_src, (
        "ParallelsState.__init__ must set library_mode='hide' (D-05 default). RED until Plan 05."
    )


# ---------------------------------------------------------------------------
# (10) AST: restore has isinstance branches (DMF-09)
# ---------------------------------------------------------------------------

def test_ast_parallels_restore_has_isinstance_branches():
    """parallels.py restore region must have isinstance(_plib_raw, list)
    AND isinstance(_plib_raw, dict) branches (D-06 migration — DMF-09).

    NOTE: RED until Plan 05 lands.
    """
    source = PARALLELS_PY.read_text(encoding='utf-8')
    assert "isinstance(_plib_raw, list)" in source or "isinstance(_plib_raw, list)" in source, (
        "parallels.py restore region must have isinstance(_plib_raw, list) branch. "
        "RED until Plan 05."
    )
    assert "isinstance(_plib_raw, dict)" in source, (
        "parallels.py restore region must have isinstance(_plib_raw, dict) branch. "
        "RED until Plan 05."
    )


# ---------------------------------------------------------------------------
# (11) AST: restore calls sanitize_library_codes (DMF-09)
# ---------------------------------------------------------------------------

def test_ast_parallels_restore_sanitizes():
    """parallels.py restore region must call sanitize_library_codes (delegates LOCAL guard).

    NOTE: RED until Plan 05 lands.
    """
    source = PARALLELS_PY.read_text(encoding='utf-8')
    assert 'sanitize_library_codes' in source, (
        "parallels.py must import and call sanitize_library_codes in the restore block. "
        "RED until Plan 05."
    )


# ---------------------------------------------------------------------------
# (12) AST: dialog/apply carries c != 'LOCAL' guard (DMF-10)
# ---------------------------------------------------------------------------

def test_ast_parallels_apply_local_guard():
    """The parallels dialog/apply function must carry the inline 'LOCAL' exclusion guard.

    NOTE: RED until Plan 05 lands.
    """
    source = PARALLELS_PY.read_text(encoding='utf-8')
    fn_src = _extract_function_lines(source, '_open_parallels_library_filter_dialog', max_lines=300)
    if not fn_src:
        # May use a different name; check the whole source
        fn_src = source
    has_local_guard = "c != 'LOCAL'" in fn_src or 'c != "LOCAL"' in fn_src
    assert has_local_guard, (
        "parallels.py dialog/apply must carry the inline 'c != LOCAL' guard (DMF-10). "
        "RED until Plan 05."
    )


# ---------------------------------------------------------------------------
# (13) AST: dialog references library_codes_with_manuscripts (DMF-13)
# ---------------------------------------------------------------------------

def test_ast_parallels_library_codes_with_manuscripts():
    """parallels.py dialog build must reference library_codes_with_manuscripts
    (DMF-13: universe = libraries with actual manuscripts).

    NOTE: RED until Plan 05 lands.
    """
    source = PARALLELS_PY.read_text(encoding='utf-8')
    assert 'library_codes_with_manuscripts' in source, (
        "parallels.py must reference library_codes_with_manuscripts (DMF-13). "
        "RED until Plan 05."
    )


# ---------------------------------------------------------------------------
# (14) Codex N2: local helper, no cross-module import of search.py's _library_apply_selection
# ---------------------------------------------------------------------------

def test_ast_parallels_local_helper_no_search_import():
    """The parallels Apply path must NOT import search.py's nested _library_apply_selection.

    Codex N2 contract: parallels defines/uses its OWN local all-selected->[] normalization
    (e.g. set(checked)==set(all_codes) sentinel) and does NOT reference the search.py-nested
    `_library_apply_selection` by an import.

    NOTE: RED until Plan 05 (the negative assertion about the IMPORT will be green even now,
    but the local-helper presence assertion is RED until Plan 05).
    """
    source = PARALLELS_PY.read_text(encoding='utf-8')

    # Negative assertion (must pass now AND after Plan 05): no cross-module import
    assert 'from web.pages.search import _library_apply_selection' not in source, (
        "parallels.py must NOT import _library_apply_selection from web.pages.search "
        "(Codex N2 contract: define a local helper instead)."
    )

    # Positive assertion (RED until Plan 05): the parallels source contains
    # its own all-selected normalization sentinel or named local helper
    has_local_norm = (
        'set(checked)' in source or
        '_library_apply_selection' in source or
        'all_selected' in source or
        'library_apply_selection' in source
    )
    assert has_local_norm, (
        "parallels.py must define/use its own all-selected->[] normalization helper "
        "(not imported from search.py). Codex N2 contract. RED until Plan 05."
    )


# ---------------------------------------------------------------------------
# (15) AST: button references REAL Phase-130 pluralized keys (DMF-10)
# ---------------------------------------------------------------------------

def test_ast_parallels_btn_pluralized_keys():
    """The parallels library filter button updater must reference the REAL
    Phase-130 pluralized template keys (NOT an invented label).

    Mirrors test_dual_mode_library_filter.py:test_ast_update_library_btn_three_states.

    NOTE: RED until Plan 05 lands.
    """
    source = PARALLELS_PY.read_text(encoding='utf-8')
    fn_src = _extract_function_lines(source, '_update_parallels_library_filter_btn', max_lines=120)
    if not fn_src:
        # May use a different name; try to find it by key strings
        fn_src = source

    assert "Showing {shown}/{total} library" in fn_src, (
        "parallels button updater must reference 'Showing {shown}/{total} library' key. "
        "RED until Plan 05."
    )
    assert "Showing {shown}/{total} libraries" in fn_src, (
        "parallels button updater must reference 'Showing {shown}/{total} libraries' key. "
        "RED until Plan 05."
    )
    assert "Hiding {n} library" in fn_src, (
        "parallels button updater must reference 'Hiding {n} library' key. "
        "RED until Plan 05."
    )
    assert "Hiding {n} libraries" in fn_src, (
        "parallels button updater must reference 'Hiding {n} libraries' key. "
        "RED until Plan 05."
    )


# ---------------------------------------------------------------------------
# (16) AST: _apply_parallels_library_filter( appears before set_parallels_export( (Codex #6)
# ---------------------------------------------------------------------------

def test_ast_parallels_filter_before_export():
    """_apply_parallels_library_filter( must appear before set_parallels_export(
    in the search-result block (finding #6 contract — export/storage rows are scoped).

    NOTE: RED until Plan 05 lands.
    """
    source = PARALLELS_PY.read_text(encoding='utf-8')

    # Check that _apply_parallels_library_filter exists
    assert '_apply_parallels_library_filter' in source, (
        "parallels.py must define _apply_parallels_library_filter function. "
        "RED until Plan 05."
    )

    # Check ordering: _apply_parallels_library_filter( must appear before set_parallels_export(
    # in the search-result handling block
    apply_pos = source.find('_apply_parallels_library_filter(')
    export_pos = source.find('set_parallels_export(')

    if apply_pos == -1:
        assert False, (
            "_apply_parallels_library_filter( not found in parallels.py. RED until Plan 05."
        )
    if export_pos == -1:
        assert False, (
            "set_parallels_export( not found in parallels.py (needed for ordering check). "
            "RED until Plan 05."
        )
    assert apply_pos < export_pos, (
        f"_apply_parallels_library_filter( (pos {apply_pos}) must appear BEFORE "
        f"set_parallels_export( (pos {export_pos}) so exports are scoped. "
        "Finding #6 contract. RED until Plan 05."
    )
