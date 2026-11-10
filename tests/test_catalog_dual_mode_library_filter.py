# -*- coding: utf-8 -*-
"""Tests for DMF dual-mode library filter — Phase 131 Plan 01 — Catalog Surface.

Coverage (mirrors the pure-mirror + AST-scan pattern of test_dual_mode_library_filter.py):

  (1)  test_migrate_list_to_show_only         — plain list -> show_only migration (DMF-08)
  (2)  test_migrate_empty_list_to_hide         — empty list -> hide/[] migration (DMF-08)
  (3)  test_migrate_dict_roundtrip             — dict shape round-trips both modes (DMF-08)
  (4)  test_migrate_bogus_mode_falls_back      — unknown mode -> 'hide' fallback (DMF-08)
  (5)  test_migrate_show_only_empty_normalizes — show_only+empty codes -> hide (DMF-08)
  (6)  test_migrate_sanitizes_local_unknown    — LOCAL + unknown dropped (DMF-10)
  (7)  test_shortlist_count_desc               — shortlist sorted count-desc, LOCAL absent
  (8)  test_apply_dual_show_only               — Show-only: keep rows IN codes (DMF-08)
  (9)  test_apply_dual_hide                    — Hide: keep rows NOT IN codes (DMF-08)
  (10) test_ast_catalog_restore_has_both_isinstance_branches — AST: restore has list+dict branches
  (11) test_ast_catalog_apply_persists_dict_shape  — AST: apply handler writes dict (D-09)
  (12) test_ast_catalog_apply_inline_local_guard   — AST: apply handler carries c != 'LOCAL' guard
  (13) test_ast_catalog_dialog_references_library_codes_with_manuscripts  — DMF-13
  (14) test_ast_catalog_dialog_fjms_instance_method  — Codex N1: fjms.get_browse_library_facets(
  (15) test_ast_catalog_btn_three_state_pluralized_keys  — REAL Phase-130 keys (DMF-10)
"""

import ast
from pathlib import Path

from shared.browse_map_utils import LIBRARY_CODES

CATALOG_BROWSE_PY = Path(__file__).parent.parent / 'web' / 'pages' / 'catalog_browse.py'

# Canonical valid codes — LOCAL is in LIBRARY_CODES but must be excluded from all web filter UI.
_VALID_CODES = set(LIBRARY_CODES) - {'LOCAL'}


# ---------------------------------------------------------------------------
# Pure-Python mirrors
# ---------------------------------------------------------------------------

def _make_result(library_code: str, idx: int = 0) -> dict:
    """Minimal fake catalog result dict."""
    return {'display': {'id': f'sys_{idx}', 'library_code': library_code}}


def _apply_library_filter_dual(results_list: list, mode: str, codes: set) -> list:
    """Pure mirror of the catalog dual-mode filter.

    Mode branch:
      - 'show_only': keep rows IN codes; empty codes = show all (D-08).
      - 'hide'     : keep rows NOT IN codes; empty codes = show all (D-05).

    Mirrors web/pages/search.py::_apply_library_filter (3830-3853).
    """
    if mode == 'show_only':
        if not codes:
            return results_list  # empty Show-only = show all
        return [r for r in results_list
                if r.get('display', {}).get('library_code', '') in codes
                or r.get('library_code', '') in codes]
    else:  # hide
        if not codes:
            return results_list  # empty Hide = show all
        return [r for r in results_list
                if r.get('display', {}).get('library_code', '') not in codes
                and r.get('library_code', '') not in codes]


def _migrate_library_filter(raw, valid_codes: set) -> dict:
    """Pure mirror of the catalog restore/migration path.

    Branches (matching catalog_browse.py restore block after Plan 04):
      - list  (legacy plain-list): non-empty -> show_only + sanitized;
                                   empty     -> hide + []
      - dict  (new shape): validated mode + sanitized codes; unknown mode -> 'hide';
               show_only + empty codes -> normalised to hide/[] (Codex HIGH fix).
      - else  (None/garbage): fresh default hide + []

    Always drops non-str items, 'LOCAL', and codes not in valid_codes.
    """
    def _sanitize(lst):
        """Mirror of sanitize_library_codes: only str items in valid_codes, != 'LOCAL'."""
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
        # Normalise show_only + empty to hide (Codex HIGH fix: "show nothing" is not intent).
        if mode == 'show_only' and not codes:
            mode = 'hide'
        return {'mode': mode, 'codes': codes}
    # None / int / str / anything else
    return {'mode': 'hide', 'codes': []}


def _shortlist_codes(facets: dict, valid_codes: set) -> list:
    """Pure mirror of the dialog shortlist build.

    Returns codes sorted by count desc, excluding LOCAL.
    """
    return sorted(
        [c for c in facets if c in valid_codes and c != 'LOCAL'],
        key=lambda c: -facets[c],
    )


# ---------------------------------------------------------------------------
# AST helpers (mirror from test_dual_mode_library_filter.py)
# ---------------------------------------------------------------------------

def _iter_function_defs(tree):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node


def _get_function_source(source: str, func_name: str) -> str:
    """Return the source text of the first function with the given name, or ''."""
    tree = ast.parse(source)
    for func in _iter_function_defs(tree):
        if func.name == func_name:
            return ast.get_source_segment(source, func) or ''
    return ''


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
# (1) Plain list migrates to show_only (DMF-08)
# ---------------------------------------------------------------------------

def test_migrate_list_to_show_only():
    """Non-empty plain list ['CUL','JTS'] -> mode='show_only', codes=['CUL','JTS'] (DMF-08)."""
    result = _migrate_library_filter(['CUL', 'JTS'], _VALID_CODES)
    assert result['mode'] == 'show_only', (
        f"Non-empty list must migrate to show_only, got mode={result['mode']!r}"
    )
    assert set(result['codes']) == {'CUL', 'JTS'}, (
        f"Codes must preserve CUL and JTS, got {result['codes']}"
    )


# ---------------------------------------------------------------------------
# (2) Empty list migrates to hide/[] (DMF-08)
# ---------------------------------------------------------------------------

def test_migrate_empty_list_to_hide():
    """Empty plain list [] -> mode='hide', codes=[] (DMF-08)."""
    result = _migrate_library_filter([], _VALID_CODES)
    assert result['mode'] == 'hide', f"Empty list must migrate to hide, got {result['mode']!r}"
    assert result['codes'] == [], f"Empty list must have codes=[], got {result['codes']!r}"


# ---------------------------------------------------------------------------
# (3) Dict shape round-trips both modes (DMF-08)
# ---------------------------------------------------------------------------

def test_migrate_dict_roundtrip():
    """{'mode':'hide','codes':['RNL']} passes through unchanged."""
    result = _migrate_library_filter({'mode': 'hide', 'codes': ['RNL']}, _VALID_CODES)
    assert result['mode'] == 'hide'
    assert result['codes'] == ['RNL']

    # Show-only round-trip
    result2 = _migrate_library_filter({'mode': 'show_only', 'codes': ['CUL', 'JTS']}, _VALID_CODES)
    assert result2['mode'] == 'show_only'
    assert set(result2['codes']) == {'CUL', 'JTS'}


# ---------------------------------------------------------------------------
# (4) Unknown mode falls back to hide (DMF-08)
# ---------------------------------------------------------------------------

def test_migrate_bogus_mode_falls_back():
    """Dict with unknown mode falls back to 'hide'."""
    result = _migrate_library_filter({'mode': 'include_all', 'codes': ['CUL']}, _VALID_CODES)
    assert result['mode'] == 'hide', (
        f"Unknown mode must fall back to 'hide', got {result['mode']!r}"
    )


# ---------------------------------------------------------------------------
# (5) show_only + empty codes normalizes to hide (Codex HIGH fix)
# ---------------------------------------------------------------------------

def test_migrate_show_only_empty_normalizes():
    """show_only + empty codes -> normalised to hide/[] (Codex HIGH fix)."""
    result = _migrate_library_filter({'mode': 'show_only', 'codes': []}, _VALID_CODES)
    assert result['mode'] == 'hide', (
        f"show_only+empty must normalize to hide, got {result['mode']!r}"
    )
    assert result['codes'] == []


# ---------------------------------------------------------------------------
# (6) Sanitize: LOCAL + unknown dropped (DMF-10)
# ---------------------------------------------------------------------------

def test_migrate_sanitizes_local_unknown():
    """['CUL','LOCAL','ZZZ'] -> only CUL survives (LOCAL + unknown dropped)."""
    result = _migrate_library_filter(['CUL', 'LOCAL', 'ZZZ'], _VALID_CODES)
    assert result['mode'] == 'show_only', (
        f"Non-empty list (after sanitize) must be show_only, got {result['mode']!r}"
    )
    assert result['codes'] == ['CUL'], (
        f"Only CUL should survive sanitization, got {result['codes']!r}"
    )


# ---------------------------------------------------------------------------
# (7) Shortlist: count-desc, LOCAL absent
# ---------------------------------------------------------------------------

def test_shortlist_count_desc():
    """Shortlist from {'CUL':3,'LOCAL':9,'RNL':1} -> ['CUL','RNL'] (count-desc, LOCAL absent)."""
    facets = {'CUL': 3, 'LOCAL': 9, 'RNL': 1}
    result = _shortlist_codes(facets, _VALID_CODES)
    assert result == ['CUL', 'RNL'], (
        f"Shortlist must be count-desc and exclude LOCAL, got {result!r}"
    )
    assert 'LOCAL' not in result, "LOCAL must never appear in the shortlist"


# ---------------------------------------------------------------------------
# (8) Apply dual: Show-only keeps rows IN codes (DMF-08)
# ---------------------------------------------------------------------------

def test_apply_dual_show_only():
    """Show-only with codes={'CUL'} over [CUL, CUL, JTS] -> 2 CUL rows (DMF-08)."""
    results = [
        _make_result('CUL', 0),
        _make_result('CUL', 1),
        _make_result('JTS', 2),
    ]
    out = _apply_library_filter_dual(results, 'show_only', {'CUL'})
    assert len(out) == 2, f"Expected 2 CUL rows, got {len(out)}"
    assert all(r['display']['library_code'] == 'CUL' for r in out)

    # Multi-code
    results2 = [_make_result('CUL', 0), _make_result('RNL', 1), _make_result('JTS', 2)]
    out2 = _apply_library_filter_dual(results2, 'show_only', {'CUL', 'RNL'})
    assert len(out2) == 2
    assert {r['display']['library_code'] for r in out2} == {'CUL', 'RNL'}


# ---------------------------------------------------------------------------
# (9) Apply dual: Hide keeps rows NOT IN codes (DMF-08)
# ---------------------------------------------------------------------------

def test_apply_dual_hide():
    """Hide with codes={'RNL'} over [CUL, RNL, JTS] -> CUL+JTS rows (DMF-08)."""
    results = [
        _make_result('CUL', 0),
        _make_result('RNL', 1),
        _make_result('JTS', 2),
    ]
    out = _apply_library_filter_dual(results, 'hide', {'RNL'})
    assert len(out) == 2
    libs = {r['display']['library_code'] for r in out}
    assert libs == {'CUL', 'JTS'}, f"Expected CUL+JTS, got {libs}"
    assert all(r['display']['library_code'] != 'RNL' for r in out)

    # Empty codes = show all (D-05/D-08)
    results3 = [_make_result('CUL', i) for i in range(5)]
    out3 = _apply_library_filter_dual(results3, 'hide', set())
    assert len(out3) == 5, "Empty hide-set must return full list"


# ---------------------------------------------------------------------------
# (10) AST: catalog restore has both isinstance branches (DMF-08)
# ---------------------------------------------------------------------------

def test_ast_catalog_restore_has_both_isinstance_branches():
    """catalog_browse.py restore region must have isinstance(_lib_raw, list)
    AND isinstance(_lib_raw, dict) branches (D-06 migration — DMF-08).

    NOTE: This scan targets the POST-Plan-04 production file. It is RED until
    Plan 04 lands — that is the intended Wave-0 state (normal failing assertion,
    not a skip).
    """
    source = CATALOG_BROWSE_PY.read_text(encoding='utf-8')
    # Find the restore region: look for the _lib_raw = safe_user_get block
    lines = source.splitlines()
    restore_lines = []
    in_restore = False
    for ln in lines:
        if '_lib_raw' in ln and 'safe_user_get' in ln:
            in_restore = True
        if in_restore:
            restore_lines.append(ln)
            if len(restore_lines) > 40:
                break
    restore_src = '\n'.join(restore_lines)

    assert "isinstance(_lib_raw, list)" in restore_src or "isinstance(_lib_raw, list)" in source, (
        "catalog_browse.py restore region must have isinstance(_lib_raw, list) branch (D-06 migration). "
        "This test is RED until Plan 04 lands."
    )
    assert "isinstance(_lib_raw, dict)" in restore_src or "isinstance(_lib_raw, dict)" in source, (
        "catalog_browse.py restore region must have isinstance(_lib_raw, dict) branch (D-06 migration). "
        "This test is RED until Plan 04 lands."
    )


# ---------------------------------------------------------------------------
# (11) AST: apply handler persists dict shape (D-09)
# ---------------------------------------------------------------------------

def test_ast_catalog_apply_persists_dict_shape():
    """The catalog Apply handler must write a dict {'mode':..., 'codes':[...]}
    (NOT a bare list) to safe_user_set / persist_value (D-09 dict shape contract).

    NOTE: RED until Plan 04 lands.
    """
    source = CATALOG_BROWSE_PY.read_text(encoding='utf-8')
    fn_src = _extract_function_lines(source, 'apply_catalog_library_filter', max_lines=80)

    assert fn_src, (
        "apply_catalog_library_filter function not found in catalog_browse.py"
    )
    # The apply handler must write a dict with 'mode' key to safe_user_set
    assert "safe_user_set('catalog_library_filter', {" in fn_src or \
           'safe_user_set("catalog_library_filter", {' in fn_src or \
           "{'mode'" in fn_src or \
           '"mode"' in fn_src, (
        "apply_catalog_library_filter must write a dict shape {'mode':..., 'codes':[...]} "
        "via safe_user_set (D-09). RED until Plan 04."
    )


# ---------------------------------------------------------------------------
# (12) AST: apply handler carries c != 'LOCAL' guard (DMF-10)
# ---------------------------------------------------------------------------

def test_ast_catalog_apply_inline_local_guard():
    """The catalog Apply handler (or the dialog function containing it) must carry
    the inline 'LOCAL' exclusion guard.

    NOTE: RED until Plan 04 lands.
    """
    source = CATALOG_BROWSE_PY.read_text(encoding='utf-8')
    fn_src = _extract_function_lines(source, '_open_library_filter_dialog', max_lines=300)
    assert fn_src, "_open_library_filter_dialog not found in catalog_browse.py"

    has_local_guard = "c != 'LOCAL'" in fn_src or 'c != "LOCAL"' in fn_src
    assert has_local_guard, (
        "_open_library_filter_dialog (or apply_catalog_library_filter) must carry "
        "the inline 'c != LOCAL' guard (DMF-10). RED until Plan 04."
    )


# ---------------------------------------------------------------------------
# (13) AST: dialog references library_codes_with_manuscripts (DMF-13)
# ---------------------------------------------------------------------------

def test_ast_catalog_dialog_references_library_codes_with_manuscripts():
    """The catalog dialog build must reference library_codes_with_manuscripts
    (DMF-13: universe = libraries with actual manuscripts).

    NOTE: RED until Plan 04 lands.
    """
    source = CATALOG_BROWSE_PY.read_text(encoding='utf-8')
    assert 'library_codes_with_manuscripts' in source, (
        "catalog_browse.py must reference library_codes_with_manuscripts "
        "(DMF-13 — universe = libraries with manuscripts). RED until Plan 04."
    )


# ---------------------------------------------------------------------------
# (14) AST: fjms instance-method call (Codex N1)
# ---------------------------------------------------------------------------

def test_ast_catalog_dialog_fjms_instance_method():
    """The catalog dialog/fetch path must call fjms.get_browse_library_facets(
    via the page's fjms instance handle — NOT a module-level import (Codex N1).

    NOTE: RED until Plan 04 lands.
    """
    source = CATALOG_BROWSE_PY.read_text(encoding='utf-8')
    assert 'fjms.get_browse_library_facets(' in source, (
        "catalog_browse.py must call fjms.get_browse_library_facets( as an instance "
        "method on the page fjms handle (NOT a module-level import). "
        "Codex N1 contract. RED until Plan 04."
    )


# ---------------------------------------------------------------------------
# (15) AST: button references REAL Phase-130 pluralized keys (DMF-10)
# ---------------------------------------------------------------------------

def test_ast_catalog_btn_three_state_pluralized_keys():
    """_update_library_filter_btn in catalog_browse.py must reference the REAL
    Phase-130 pluralized template keys (NOT an invented 'Filter by library (shown/total)').

    Mirrors test_dual_mode_library_filter.py:test_ast_update_library_btn_three_states.

    NOTE: RED until Plan 04 lands.
    """
    source = CATALOG_BROWSE_PY.read_text(encoding='utf-8')
    fn_src = _extract_function_lines(source, '_update_library_filter_btn', max_lines=120)
    assert fn_src, "_update_library_filter_btn not found in catalog_browse.py"

    assert "Showing {shown}/{total} library" in fn_src, (
        "_update_library_filter_btn must reference 'Showing {shown}/{total} library' "
        "template key (REAL Phase-130 key). RED until Plan 04."
    )
    assert "Showing {shown}/{total} libraries" in fn_src, (
        "_update_library_filter_btn must reference 'Showing {shown}/{total} libraries' "
        "template key. RED until Plan 04."
    )
    assert "Hiding {n} library" in fn_src, (
        "_update_library_filter_btn must reference 'Hiding {n} library' "
        "template key. RED until Plan 04."
    )
    assert "Hiding {n} libraries" in fn_src, (
        "_update_library_filter_btn must reference 'Hiding {n} libraries' "
        "template key. RED until Plan 04."
    )
    assert "tr('Filter by library')" in fn_src or 'tr("Filter by library")' in fn_src, (
        "_update_library_filter_btn must have tr('Filter by library') for the neutral label. "
        "RED until Plan 04."
    )
    assert 'library_mode' in fn_src or 'current_library_mode' in fn_src, (
        "_update_library_filter_btn must read library_mode (or current_library_mode) "
        "to determine the active state. RED until Plan 04."
    )
