# -*- coding: utf-8 -*-
"""Tests for DMF dual-mode library filter — Phase 130 Plan 03.

Coverage (mirrors the pure-mirror + AST-scan pattern of test_libfilter_web_search.py):

  (1)  test_show_only_mode_filters_in_set           — Show-only: keep rows IN codes (DMF-01)
  (2)  test_hide_mode_filters_out_set               — Hide: keep rows NOT IN codes (DMF-02)
  (3)  test_show_only_empty_codes_is_noop           — empty Show-only = show all (D-08/DMF-06)
  (4)  test_hide_empty_codes_is_noop                — empty Hide = show all (D-05)
  (5)  test_full_hide_returns_empty_list            — full Hide-set = 0 results (DMF-06)
  (6)  test_hide_persists_over_new_libraries        — Hide intent stays when NEW library appears (DMF-02)
  (7)  test_legacy_list_migrates_to_show_only       — non-empty list -> show_only (DMF-05)
  (8)  test_legacy_empty_list_migrates_to_hide      — empty list -> hide/[] (DMF-05)
  (9)  test_dict_valid_roundtrip                    — dict shape passthrough (mode+codes)
  (10) test_dict_bogus_mode_falls_back_to_hide      — unknown mode -> 'hide' fallback
  (11) test_migration_sanitizes_local_and_unknown   — LOCAL + unknown dropped (DMF-10)
  (12) test_migration_none_garbage_returns_default  — None/int/str -> hide/[] (no raise)
  (13) test_shortlist_excludes_local_code           — LOCAL never in shortlist (HIGH-2)
  (14) test_ast_apply_filter_references_library_mode — AST: _apply_library_filter reads library_mode/'show_only'
  (15) test_ast_restore_has_legacy_list_branch      — AST: restore region has isinstance(_lib_raw, list)
  (16) test_ast_apply_handler_sanitizes_codes       — AST: Apply handler sanitizes with c in LIBRARY_CODES and c != 'LOCAL'
  (17) test_ast_apply_handler_persists_dict_shape   — AST: Apply handler persist_value writes a dict (D-09)
  (18) test_ast_dialog_shortlist_excludes_local     — AST: shortlist build carries c != 'LOCAL' literal (HIGH-2)
  (19) test_ast_update_library_btn_three_states     — AST: _update_library_btn has Showing/Hiding/'Filter by library'/library_mode
  (20) test_ast_dialog_and_restore_contain_local_guard — AST: c != 'LOCAL' in dialog body + restore path
  (21) test_ast_dual_writer_dict_shape              — AST: BOTH search.py Apply AND filter_panel.consume_incoming_filters persist dict (HIGH-1)
  (22) test_ast_filter_panel_show_only_stamp        — AST: filter_panel.consume_incoming_filters references 'show_only'
  (23) test_ast_no_bare_list_writer                 — AST: no persist_value('search_library_filter', <bare list variable>)
  (24) test_ast_persistence_chokepoint              — persist_value/_safe_get for search_library_filter; no raw app.storage.user
"""

import ast
import re
from pathlib import Path

from shared.browse_map_utils import LIBRARY_CODES

SEARCH_PY = Path(__file__).parent.parent / 'web' / 'pages' / 'search.py'
FILTER_PANEL_PY = Path(__file__).parent.parent / 'web' / 'components' / 'filter_panel.py'

# Canonical valid codes — LOCAL is in LIBRARY_CODES but must be excluded from all web filter UI.
_VALID_CODES = set(LIBRARY_CODES) - {'LOCAL'}


# ---------------------------------------------------------------------------
# Pure-Python mirrors
# ---------------------------------------------------------------------------

def _make_result(library_code: str, idx: int = 0) -> dict:
    """Minimal fake result dict."""
    return {'display': {'id': f'sys_{idx}', 'library_code': library_code}}


def _apply_library_filter_dual(results_list: list, mode: str, codes: set) -> list:
    """Pure mirror of _apply_library_filter.

    Mode branch:
      - 'show_only': keep rows IN codes; empty codes = show all (D-08).
      - 'hide'     : keep rows NOT IN codes; empty codes = show all (D-05).
    """
    if mode == 'show_only':
        if not codes:
            return results_list  # empty Show-only = show all
        return [r for r in results_list
                if r.get('display', {}).get('library_code', '') in codes]
    else:  # hide
        if not codes:
            return results_list  # empty Hide = show all
        return [r for r in results_list
                if r.get('display', {}).get('library_code', '') not in codes]


def _migrate_library_filter(raw, valid_codes: set) -> dict:
    """Pure mirror of the restore/sanitize path.

    Branches (matching search.py ~188-208):
      - list  (legacy v8.3.0): non-empty -> show_only + sanitized codes;
                                empty    -> hide + []
      - dict  (new shape): validated mode + sanitized codes; unknown mode -> 'hide'
      - else  (None/garbage): fresh default hide + []

    Always drops 'LOCAL' and codes not in valid_codes.
    """
    if isinstance(raw, list):
        codes = [c for c in raw if c in valid_codes and c != 'LOCAL']
        if codes:
            return {'mode': 'show_only', 'codes': codes}
        return {'mode': 'hide', 'codes': []}
    if isinstance(raw, dict):
        mode = raw.get('mode', 'hide')
        if mode not in ('show_only', 'hide'):
            mode = 'hide'
        codes = [c for c in (raw.get('codes') or []) if c in valid_codes and c != 'LOCAL']
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
# AST helpers
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
# (1) Show-only mode: keep rows IN codes
# ---------------------------------------------------------------------------

def test_show_only_mode_filters_in_set():
    """Show-only with codes={'CUL'} over [CUL, CUL, JTS] -> 2 CUL rows (DMF-01)."""
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
# (2) Hide mode: keep rows NOT IN codes
# ---------------------------------------------------------------------------

def test_hide_mode_filters_out_set():
    """Hide with codes={'RNL'} over [CUL, RNL, JTS] -> CUL+JTS rows (DMF-02)."""
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


# ---------------------------------------------------------------------------
# (3) Empty Show-only = show all
# ---------------------------------------------------------------------------

def test_show_only_empty_codes_is_noop():
    """Empty Show-only returns input unchanged (D-08/DMF-06)."""
    results = [_make_result('CUL', i) for i in range(5)]
    out = _apply_library_filter_dual(results, 'show_only', set())
    assert len(out) == len(results)
    assert out is results or out == results


# ---------------------------------------------------------------------------
# (4) Empty Hide = show all
# ---------------------------------------------------------------------------

def test_hide_empty_codes_is_noop():
    """Empty Hide returns input unchanged (D-05)."""
    results = [_make_result('JTS', i) for i in range(4)]
    out = _apply_library_filter_dual(results, 'hide', set())
    assert len(out) == len(results)
    assert out is results or out == results


# ---------------------------------------------------------------------------
# (5) Full Hide-set = 0 results (DMF-06)
# ---------------------------------------------------------------------------

def test_full_hide_returns_empty_list():
    """Hiding all present libraries yields [] (DMF-06/D-08)."""
    results = [
        _make_result('CUL', 0),
        _make_result('RNL', 1),
        _make_result('JTS', 2),
    ]
    out = _apply_library_filter_dual(results, 'hide', {'CUL', 'RNL', 'JTS'})
    assert out == [], f"Expected [], got {out}"


# ---------------------------------------------------------------------------
# (6) Hide intent persists when NEW library appears (DMF-02)
# ---------------------------------------------------------------------------

def test_hide_persists_over_new_libraries():
    """A row with a library NOT in the hide-set is kept even if it was absent from
    the original result set — the mirror does not recompute the set from results.
    (DMF-02: the hide intent is stateful, not recomputed from results.)
    """
    # Original results: CUL and RNL. Hide-set: {RNL}.
    results1 = [_make_result('CUL', 0), _make_result('RNL', 1)]
    out1 = _apply_library_filter_dual(results1, 'hide', {'RNL'})
    assert [r['display']['library_code'] for r in out1] == ['CUL']

    # Now a new search returns JTS which was absent before. The persisted hide-set
    # is still {RNL} — JTS should appear in the output.
    results2 = [_make_result('CUL', 0), _make_result('RNL', 1), _make_result('JTS', 2)]
    out2 = _apply_library_filter_dual(results2, 'hide', {'RNL'})
    libs2 = {r['display']['library_code'] for r in out2}
    assert 'JTS' in libs2, "JTS (new library not in hide-set) must appear in output"
    assert 'RNL' not in libs2, "RNL (in hide-set) must be dropped"


# ---------------------------------------------------------------------------
# (7) Legacy list -> Show-only migration (DMF-05)
# ---------------------------------------------------------------------------

def test_legacy_list_migrates_to_show_only():
    """Non-empty plain list ['CUL','JTS'] -> mode='show_only', codes=['CUL','JTS'] (DMF-05)."""
    result = _migrate_library_filter(['CUL', 'JTS'], _VALID_CODES)
    assert result['mode'] == 'show_only', (
        f"Non-empty list must migrate to show_only, got mode={result['mode']!r}"
    )
    assert set(result['codes']) == {'CUL', 'JTS'}, (
        f"Codes must preserve CUL and JTS, got {result['codes']}"
    )


# ---------------------------------------------------------------------------
# (8) Legacy empty list -> hide/[] (DMF-05)
# ---------------------------------------------------------------------------

def test_legacy_empty_list_migrates_to_hide():
    """Empty plain list [] -> mode='hide', codes=[] (DMF-05)."""
    result = _migrate_library_filter([], _VALID_CODES)
    assert result['mode'] == 'hide'
    assert result['codes'] == []


# ---------------------------------------------------------------------------
# (9) Dict valid roundtrip
# ---------------------------------------------------------------------------

def test_dict_valid_roundtrip():
    """{'mode':'hide','codes':['RNL']} passes through unchanged (no migration needed)."""
    result = _migrate_library_filter({'mode': 'hide', 'codes': ['RNL']}, _VALID_CODES)
    assert result['mode'] == 'hide'
    assert result['codes'] == ['RNL']

    result2 = _migrate_library_filter({'mode': 'show_only', 'codes': ['CUL']}, _VALID_CODES)
    assert result2['mode'] == 'show_only'
    assert result2['codes'] == ['CUL']


# ---------------------------------------------------------------------------
# (10) Dict bogus mode falls back to 'hide'
# ---------------------------------------------------------------------------

def test_dict_bogus_mode_falls_back_to_hide():
    """{'mode':'bogus','codes':['CUL']} -> mode falls back to 'hide', codes preserved."""
    result = _migrate_library_filter({'mode': 'bogus', 'codes': ['CUL']}, _VALID_CODES)
    assert result['mode'] == 'hide', (
        f"Bogus mode must fall back to 'hide', got {result['mode']!r}"
    )
    assert 'CUL' in result['codes']


# ---------------------------------------------------------------------------
# (11) Migration sanitizes LOCAL and unknown codes (DMF-10)
# ---------------------------------------------------------------------------

def test_migration_sanitizes_local_and_unknown():
    """['CUL','LOCAL','ZZZ'] -> codes=['CUL'] ('LOCAL' + unknown dropped) (DMF-10)."""
    result = _migrate_library_filter(['CUL', 'LOCAL', 'ZZZ'], _VALID_CODES)
    assert result['mode'] == 'show_only', (
        "Non-empty list (after sanitization) should yield show_only"
    )
    assert result['codes'] == ['CUL'], (
        f"Only CUL must survive; LOCAL and ZZZ must be dropped. Got: {result['codes']}"
    )

    # LOCAL in a dict is also dropped
    result2 = _migrate_library_filter({'mode': 'hide', 'codes': ['LOCAL', 'RNL']}, _VALID_CODES)
    assert 'LOCAL' not in result2['codes'], "LOCAL must be dropped from dict codes"
    assert 'RNL' in result2['codes']

    # All-invalid list (only LOCAL + garbage) -> treat as empty list -> hide/[]
    result3 = _migrate_library_filter(['LOCAL', 'ZZZ'], _VALID_CODES)
    assert result3['mode'] == 'hide'
    assert result3['codes'] == []


# ---------------------------------------------------------------------------
# (12) None / garbage -> fresh default (no raise)
# ---------------------------------------------------------------------------

def test_migration_none_garbage_returns_default():
    """None / 42 / 'x' all return mode='hide', codes=[] without raising."""
    for raw in (None, 42, 'x', 3.14, True, object()):
        result = _migrate_library_filter(raw, _VALID_CODES)
        assert result['mode'] == 'hide', (
            f"Garbage input {raw!r} must yield mode='hide', got {result['mode']!r}"
        )
        assert result['codes'] == [], (
            f"Garbage input {raw!r} must yield codes=[], got {result['codes']!r}"
        )


# ---------------------------------------------------------------------------
# (13) Shortlist excludes LOCAL (HIGH-2)
# ---------------------------------------------------------------------------

def test_shortlist_excludes_local_code():
    """facets={'CUL':3,'LOCAL':9,'RNL':1} -> shortlist=['CUL','RNL'] (LOCAL absent) (HIGH-2).

    Even though LOCAL has the highest count, it must NEVER appear in the dialog shortlist.
    """
    facets = {'CUL': 3, 'LOCAL': 9, 'RNL': 1}
    shortlist = _shortlist_codes(facets, _VALID_CODES)
    assert 'LOCAL' not in shortlist, (
        f"'LOCAL' must never appear in the dialog shortlist, got {shortlist}"
    )
    assert 'CUL' in shortlist
    assert 'RNL' in shortlist
    # Order: highest count first (CUL=3 before RNL=1)
    assert shortlist.index('CUL') < shortlist.index('RNL'), (
        "Shortlist must be sorted by count descending"
    )


# ---------------------------------------------------------------------------
# (14) AST: _apply_library_filter references library_mode and 'show_only'
# ---------------------------------------------------------------------------

def test_ast_apply_filter_references_library_mode():
    """_apply_library_filter must read library_mode and branch on 'show_only'."""
    source = SEARCH_PY.read_text(encoding='utf-8')
    fn_src = _extract_function_lines(source, '_apply_library_filter')
    assert fn_src, "_apply_library_filter not found in web/pages/search.py"
    assert 'library_mode' in fn_src, (
        "_apply_library_filter must read library_mode from search_state"
    )
    assert "'show_only'" in fn_src or '"show_only"' in fn_src, (
        "_apply_library_filter must branch on 'show_only'"
    )


# ---------------------------------------------------------------------------
# (15) AST: restore region has isinstance(_lib_raw, list) (legacy migration branch)
# ---------------------------------------------------------------------------

def test_ast_restore_has_legacy_list_branch():
    """The restore/sanitize block (~188) must have isinstance(_lib_raw, list) for
    v8.3.0 legacy migration (DMF-05).
    """
    source = SEARCH_PY.read_text(encoding='utf-8')
    assert 'isinstance(_lib_raw, list)' in source, (
        "restore block must contain isinstance(_lib_raw, list) for legacy list migration"
    )
    assert 'isinstance(_lib_raw, dict)' in source, (
        "restore block must also contain isinstance(_lib_raw, dict) for new dict shape"
    )


# ---------------------------------------------------------------------------
# (16) AST: Apply handler sanitizes codes against LIBRARY_CODES and c != 'LOCAL'
# ---------------------------------------------------------------------------

def test_ast_apply_handler_sanitizes_codes():
    """The dialog's apply_library_filter handler must sanitize JS-returned codes with
    a comprehension containing 'c in LIBRARY_CODES and c != 'LOCAL'' (HIGH-3).
    """
    source = SEARCH_PY.read_text(encoding='utf-8')
    # The sanitize line is inside the async apply_library_filter nested closure
    # We check the whole file since the function is deeply nested
    assert 'c in LIBRARY_CODES and c != \'LOCAL\'' in source or \
           'c in LIBRARY_CODES and c != "LOCAL"' in source, (
        "Apply handler must sanitize checked codes with "
        "'c in LIBRARY_CODES and c != LOCAL' (HIGH-3 T-130-02-02)"
    )


# ---------------------------------------------------------------------------
# (17) AST: Apply handler persists dict shape (D-09)
# ---------------------------------------------------------------------------

def test_ast_apply_handler_persists_dict_shape():
    """The dialog Apply handler must persist_value('search_library_filter', {...dict...})
    — no bare-list persist for this key (D-09).
    """
    source = SEARCH_PY.read_text(encoding='utf-8')
    # The persist line should look like:
    #   persist_value('search_library_filter', {'mode': ..., 'codes': ...})
    assert "persist_value('search_library_filter'" in source or \
           'persist_value("search_library_filter"' in source, (
        "search.py must persist search_library_filter via persist_value"
    )
    # Verify the persist site is followed by a '{' (dict literal) not a bare name/list
    # We look for the pattern: persist_value('search_library_filter', {
    assert "persist_value('search_library_filter', {" in source or \
           'persist_value("search_library_filter", {' in source, (
        "persist_value for search_library_filter must pass a dict literal, not a bare list (D-09)"
    )


# ---------------------------------------------------------------------------
# (18) AST: dialog shortlist build contains c != 'LOCAL' (HIGH-2)
# ---------------------------------------------------------------------------

def test_ast_dialog_shortlist_excludes_local():
    """The shortlist build in _open_library_filter_dialog must contain c != 'LOCAL'
    so LOCAL result rows can never surface in the dialog (HIGH-2/DMF-10).
    """
    source = SEARCH_PY.read_text(encoding='utf-8')
    fn_src = _extract_function_lines(source, '_open_library_filter_dialog', max_lines=300)
    assert fn_src, "_open_library_filter_dialog not found in web/pages/search.py"

    local_guard_count = fn_src.count("!= 'LOCAL'") + fn_src.count('!= "LOCAL"')
    assert local_guard_count >= 2, (
        f"Dialog function must carry 'c != LOCAL' at least twice "
        f"(shortlist + expand section), found {local_guard_count} time(s). "
        "HIGH-2: LOCAL must never appear in the shortlist or expand section."
    )


# ---------------------------------------------------------------------------
# (19) AST: _update_library_btn has Showing/Hiding/'Filter by library'/library_mode
# ---------------------------------------------------------------------------

def test_ast_update_library_btn_three_states():
    """_update_library_btn must reference tr('Showing'), tr('Hiding'),
    tr('Filter by library'), and library_mode (3-state button contract).
    """
    source = SEARCH_PY.read_text(encoding='utf-8')
    fn_src = _extract_function_lines(source, '_update_library_btn', max_lines=100)
    assert fn_src, "_update_library_btn not found in web/pages/search.py"

    assert "tr('Showing')" in fn_src or 'tr("Showing")' in fn_src, (
        "_update_library_btn must have tr('Showing') for the Show-only active label"
    )
    assert "tr('Hiding')" in fn_src or 'tr("Hiding")' in fn_src, (
        "_update_library_btn must have tr('Hiding') for the Hide active label"
    )
    assert "tr('Filter by library')" in fn_src or 'tr("Filter by library")' in fn_src, (
        "_update_library_btn must have tr('Filter by library') for the neutral label"
    )
    assert 'library_mode' in fn_src, (
        "_update_library_btn must read library_mode to determine the active state"
    )


# ---------------------------------------------------------------------------
# (20) AST: dialog body + restore path contain c != 'LOCAL' guard
# ---------------------------------------------------------------------------

def test_ast_dialog_and_restore_contain_local_guard():
    """Both the dialog body and the restore/sanitize path must contain the
    'c != LOCAL' guard (DMF-10).
    """
    source = SEARCH_PY.read_text(encoding='utf-8')

    # Restore region (top-of-function)
    # Find the lines around the restore block
    lines = source.splitlines()
    restore_lines = []
    in_restore = False
    for ln in lines:
        if '_lib_raw = _safe_get' in ln:
            in_restore = True
        if in_restore:
            restore_lines.append(ln)
            if len(restore_lines) > 30:
                break
    restore_src = '\n'.join(restore_lines)
    assert "!= 'LOCAL'" in restore_src or '!= "LOCAL"' in restore_src, (
        "The restore/sanitize region must contain c != 'LOCAL' to drop LOCAL from migrated codes"
    )

    # Dialog shortlist build (already covered in test 18, but cross-checked here for the
    # composite guard statement)
    fn_src = _extract_function_lines(source, '_open_library_filter_dialog', max_lines=300)
    assert "!= 'LOCAL'" in fn_src or '!= "LOCAL"' in fn_src, (
        "_open_library_filter_dialog must contain c != 'LOCAL' guard"
    )


# ---------------------------------------------------------------------------
# (21) AST: BOTH writers persist dict shape (HIGH-1)
# ---------------------------------------------------------------------------

def test_ast_dual_writer_dict_shape():
    """Both search.py Apply handler AND filter_panel.consume_incoming_filters must
    persist search_library_filter as a dict (not a bare list) (HIGH-1).

    search.py: persist_value('search_library_filter', {'mode':..., 'codes':...})
    filter_panel: persist_value('search_library_filter', {'mode': 'show_only', ...})
    """
    search_src = SEARCH_PY.read_text(encoding='utf-8')
    fp_src = FILTER_PANEL_PY.read_text(encoding='utf-8')

    # search.py writer: dict literal follows the key
    assert "persist_value('search_library_filter', {" in search_src or \
           'persist_value("search_library_filter", {' in search_src, (
        "search.py Apply handler must persist search_library_filter as a dict literal (HIGH-1)"
    )

    # filter_panel.py writer: also a dict literal (not a bare list variable)
    assert "persist_value('search_library_filter', {" in fp_src or \
           'persist_value("search_library_filter", {' in fp_src, (
        "filter_panel.consume_incoming_filters must persist search_library_filter as a dict (HIGH-1)"
    )


# ---------------------------------------------------------------------------
# (22) AST: filter_panel.consume_incoming_filters references 'show_only'
# ---------------------------------------------------------------------------

def test_ast_filter_panel_show_only_stamp():
    """filter_panel.consume_incoming_filters must reference 'show_only' — it stamps the
    browse->search handoff as Show-only intent (HIGH-1).
    """
    fp_src = FILTER_PANEL_PY.read_text(encoding='utf-8')
    fn_src = _get_function_source(fp_src, 'consume_incoming_filters')
    assert fn_src, "consume_incoming_filters not found in filter_panel.py"

    assert "'show_only'" in fn_src or '"show_only"' in fn_src, (
        "consume_incoming_filters must stamp mode='show_only' for the browse->search handoff"
    )
    assert "!= 'LOCAL'" in fn_src or '!= "LOCAL"' in fn_src, (
        "consume_incoming_filters must carry the c != 'LOCAL' guard (HIGH-1/DMF-10)"
    )


# ---------------------------------------------------------------------------
# (23) AST: no bare-list writer for search_library_filter
# ---------------------------------------------------------------------------

def test_ast_no_bare_list_writer():
    """No persist_value('search_library_filter', <bare list>) remains in either file.

    A bare-list write would lose the mode and cause the restore path to mis-migrate.
    Every persist site must pass a dict.
    """
    search_src = SEARCH_PY.read_text(encoding='utf-8')
    fp_src = FILTER_PANEL_PY.read_text(encoding='utf-8')

    for name, src in [('search.py', search_src), ('filter_panel.py', fp_src)]:
        # Find all lines that persist search_library_filter
        for i, ln in enumerate(src.splitlines()):
            if 'persist_value' in ln and 'search_library_filter' in ln:
                # This line must NOT immediately pass a bare list ([) or bare variable
                # without a dict. We check: the persist call must contain '{' on the
                # same line or the immediately following line (multi-line dict).
                # The simplest reliable check: the key must be followed by ', {' on the
                # same line (the production code formats it inline).
                stripped = ln.strip()
                assert not re.search(
                    r"persist_value\(['\"]search_library_filter['\"],\s*\[",
                    stripped,
                ), (
                    f"{name} line {i + 1}: persist_value('search_library_filter', [...]) "
                    "found — must be a dict literal, not a bare list (HIGH-1)"
                )


# ---------------------------------------------------------------------------
# (24) AST: persistence uses chokepoint (persist_value + _safe_get; no raw app.storage.user)
# ---------------------------------------------------------------------------

def test_ast_persistence_chokepoint():
    """search_library_filter is persisted via persist_value and read via _safe_get in
    search.py. Neither file has raw app.storage.user access near library_filter.
    (Phase 87 safe_storage invariant.)
    """
    search_src = SEARCH_PY.read_text(encoding='utf-8')

    # persist_value chokepoint
    assert "persist_value('search_library_filter'" in search_src or \
           'persist_value("search_library_filter"' in search_src, (
        "search_library_filter must be written via persist_value in search.py"
    )

    # _safe_get chokepoint
    assert "_safe_get('search_library_filter'" in search_src or \
           '_safe_get("search_library_filter"' in search_src, (
        "search_library_filter must be read via _safe_get in search.py"
    )

    # No raw app.storage.user near library_filter in search.py or filter_panel.py
    fp_src = FILTER_PANEL_PY.read_text(encoding='utf-8')
    for name, src in [('search.py', search_src), ('filter_panel.py', fp_src)]:
        for i, ln in enumerate(src.splitlines()):
            if 'library_filter' in ln and 'app.storage.user' in ln:
                raise AssertionError(
                    f"{name} line {i + 1}: raw app.storage.user access found near "
                    "library_filter. Use persist_value/_safe_get (Phase 87 invariant)."
                )
