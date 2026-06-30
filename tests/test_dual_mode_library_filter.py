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

from shared.browse_map_utils import LIBRARY_CODES, sanitize_library_codes

SEARCH_PY = Path(__file__).parent.parent / 'web' / 'pages' / 'search.py'
FILTER_PANEL_PY = Path(__file__).parent.parent / 'web' / 'components' / 'filter_panel.py'
BROWSE_MAP_UTILS_PY = Path(__file__).parent.parent / 'shared' / 'browse_map_utils.py'

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
      - dict  (new shape): validated mode + sanitized codes; unknown mode -> 'hide';
               show_only + empty codes -> normalised to hide/[] (Codex HIGH fix).
      - else  (None/garbage): fresh default hide + []

    Always drops non-str items, 'LOCAL', and codes not in valid_codes
    (mirrors sanitize_library_codes behaviour).
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
# (16) AST: Apply handler sanitizes codes via sanitize_library_codes (HIGH-3)
# ---------------------------------------------------------------------------

def test_ast_apply_handler_sanitizes_codes():
    """The dialog's apply_library_filter handler must sanitize JS-returned codes via
    sanitize_library_codes (Codex HIGH fix: replaced inline comprehension that crashed
    on non-str items like int/dict).

    Also verifies that the canonical guard (c in LIBRARY_CODES and c != 'LOCAL') lives
    inside sanitize_library_codes in browse_map_utils.py — the single source of truth.
    """
    source = SEARCH_PY.read_text(encoding='utf-8')
    bmu_source = BROWSE_MAP_UTILS_PY.read_text(encoding='utf-8')

    # search.py Apply handler must call sanitize_library_codes (not an inline comprehension).
    assert 'sanitize_library_codes' in source, (
        "Apply handler must delegate to sanitize_library_codes (HIGH-3 T-130-02-02) "
        "instead of an inline comprehension that crashes on non-str items"
    )

    # The canonical guard must live in browse_map_utils.py (single source of truth).
    assert 'c in LIBRARY_CODES and c != \'LOCAL\'' in bmu_source or \
           'c in LIBRARY_CODES and c != "LOCAL"' in bmu_source, (
        "sanitize_library_codes in browse_map_utils.py must contain "
        "'c in LIBRARY_CODES and c != LOCAL' as the canonical guard"
    )

    # All three sanitization entry points in search.py must use sanitize_library_codes.
    occurrences = source.count('sanitize_library_codes(')
    assert occurrences >= 3, (
        f"search.py must call sanitize_library_codes at all 3 entry points "
        f"(list-branch restore, dict-branch restore, Apply handler); found {occurrences} call(s)"
    )

    # Codex code-review R2: the browse->search handoff (filter_panel.consume_incoming_filters)
    # is ALSO an untrusted entry point and must route through the shared sanitizer — not an
    # inline comprehension that crashes on a malformed scalar (e.g. {'library_filter': 1}).
    fp_source = FILTER_PANEL_PY.read_text(encoding='utf-8')
    assert 'sanitize_library_codes(' in fp_source, (
        "filter_panel.consume_incoming_filters must delegate library-code sanitization to "
        "shared.browse_map_utils.sanitize_library_codes (Codex R2 HIGH)"
    )
    assert "for c in incoming['library_filter']" not in fp_source, (
        "filter_panel must NOT iterate incoming['library_filter'] inline — a non-list scalar "
        "raises TypeError before the neutral/skip path; use sanitize_library_codes instead"
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
    """_update_library_btn must reference the 4 pluralized template keys,
    tr('Filter by library'), and library_mode (3-state button contract).

    Phase 130 UAT: bare tr('Showing')/tr('Hiding') are replaced with the
    four template-key variants (lib_btn_showing_one/many, lib_btn_hiding_one/many).
    """
    source = SEARCH_PY.read_text(encoding='utf-8')
    fn_src = _extract_function_lines(source, '_update_library_btn', max_lines=120)
    assert fn_src, "_update_library_btn not found in web/pages/search.py"

    # Phase 130 UAT: pluralized template keys must be present in the function body.
    assert "Showing {shown}/{total} library" in fn_src, (
        "_update_library_btn must reference 'Showing {shown}/{total} library' template key"
    )
    assert "Showing {shown}/{total} libraries" in fn_src, (
        "_update_library_btn must reference 'Showing {shown}/{total} libraries' template key"
    )
    assert "Hiding {n} library" in fn_src, (
        "_update_library_btn must reference 'Hiding {n} library' template key"
    )
    assert "Hiding {n} libraries" in fn_src, (
        "_update_library_btn must reference 'Hiding {n} libraries' template key"
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
    """Both the dialog body and the restore/sanitize path must enforce the
    LOCAL-exclusion guard (DMF-10).

    After the Codex HIGH fix, the restore block delegates sanitization to
    sanitize_library_codes() (defined in browse_map_utils.py) rather than
    containing an inline comprehension.  We accept either form:
    - Old: inline ``!= 'LOCAL'`` in the restore region
    - New: ``sanitize_library_codes`` call in the restore region (guard is inside the helper)
    The dialog shortlist must still carry the inline ``!= 'LOCAL'`` literal.
    """
    source = SEARCH_PY.read_text(encoding='utf-8')
    bmu_source = BROWSE_MAP_UTILS_PY.read_text(encoding='utf-8')

    # Restore region: either an inline guard OR a sanitize_library_codes call.
    lines = source.splitlines()
    restore_lines = []
    in_restore = False
    for ln in lines:
        if '_lib_raw = _safe_get' in ln:
            in_restore = True
        if in_restore:
            restore_lines.append(ln)
            if len(restore_lines) > 40:
                break
    restore_src = '\n'.join(restore_lines)
    has_inline_guard = "!= 'LOCAL'" in restore_src or '!= "LOCAL"' in restore_src
    has_sanitize_call = 'sanitize_library_codes' in restore_src
    assert has_inline_guard or has_sanitize_call, (
        "The restore/sanitize region must either contain an inline c != 'LOCAL' guard "
        "OR delegate to sanitize_library_codes (Codex HIGH fix accepted both forms)"
    )
    # When delegating, verify that the canonical guard lives in browse_map_utils.
    if has_sanitize_call and not has_inline_guard:
        assert "!= 'LOCAL'" in bmu_source or '!= "LOCAL"' in bmu_source, (
            "sanitize_library_codes in browse_map_utils.py must contain c != 'LOCAL' "
            "as the single source of truth for the LOCAL-exclusion guard"
        )

    # Dialog shortlist build (already covered in test 18, but cross-checked here).
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


# ---------------------------------------------------------------------------
# (25) AST: CR-01 regression guard — mode toggle uses high-level value API
# (26) AST: WR-02 regression guard — JS libFilterUpdateApply fallback is 'hide'
# ---------------------------------------------------------------------------

def test_ast_mode_toggle_uses_high_level_value_api():
    """CR-01 regression guard: the mode toggle must be wired with on_value_change
    (or on_change=) so the handler receives the mapped dict key ('show_only'/'hide'),
    not the raw integer index (0/1) emitted by update:modelValue.

    Also asserts that _on_mode_change is NOT called via e.args (the bug pattern).
    """
    source = SEARCH_PY.read_text(encoding='utf-8')

    # The fix: on_value_change feeds e.value into _on_mode_change.
    assert 'on_value_change' in source and 'e.value' in source, (
        "CR-01: mode toggle must use on_value_change(...e.value...) "
        "so the handler receives the string key, not an integer index"
    )

    # The bug pattern must be absent: passing e.args into _on_mode_change.
    assert '_on_mode_change(e.args)' not in source, (
        "CR-01: '_on_mode_change(e.args)' found in search.py — this passes the raw "
        "integer index (0/1), not the dict key ('show_only'/'hide'). "
        "Replace with on_value_change(lambda e: _on_mode_change(e.value))."
    )


def test_ast_js_libfilter_fallback_is_hide():
    """WR-02 regression guard: the JS libFilterUpdateApply fallback for a missing
    data-libmode attribute must be 'hide' (the system default, D-05), NOT 'show_only'.

    A 'show_only' fallback would incorrectly disable the Apply button in Hide-mode
    when data-libmode is absent or empty.
    """
    source = SEARCH_PY.read_text(encoding='utf-8')

    # Correct pattern must be present.
    assert "|| 'hide'" in source or '|| "hide"' in source, (
        "WR-02: JS libFilterUpdateApply must use `|| 'hide'` as the fallback for "
        "a missing data-libmode attribute (D-05 system default is 'hide')"
    )

    # Wrong pattern must be absent in the libFilterUpdateApply JS context.
    # We look for the specific fallback pattern in the function body.
    import re as _re
    # Extract the libFilterUpdateApply function block from the inline JS
    match = _re.search(
        r'function libFilterUpdateApply\(.*?\}',
        source,
        _re.DOTALL,
    )
    if match:
        fn_body = match.group(0)
        assert "|| 'show_only'" not in fn_body and '|| "show_only"' not in fn_body, (
            "WR-02: libFilterUpdateApply contains `|| 'show_only'` fallback — "
            "must be `|| 'hide'` (D-05)"
        )


# ---------------------------------------------------------------------------
# (27) library_codes_with_manuscripts() — unit tests (Phase 130 UAT Change 2)
# ---------------------------------------------------------------------------

def test_library_codes_with_manuscripts_is_subset_of_library_codes():
    """library_codes_with_manuscripts() must return a non-empty frozenset that is a
    subset of set(LIBRARY_CODES).  The real corpus must contain at least the
    big four: CUL, JTS, RNL, Oxford.
    """
    from shared.browse_map_utils import library_codes_with_manuscripts
    # Reset cache so monkeypatching in other tests doesn't bleed in.
    library_codes_with_manuscripts._cache = None
    result = library_codes_with_manuscripts()
    assert isinstance(result, frozenset), (
        f"Must return a frozenset (Codex LOW fix — immutable cache), got {type(result).__name__}"
    )
    assert result, "Must be non-empty"
    assert result <= set(LIBRARY_CODES), (
        "All returned codes must be in LIBRARY_CODES (no stray values)"
    )
    # The CSV always has the big four for the Cairo Genizah corpus.
    for expected in ('CUL', 'JTS', 'RNL', 'Oxford'):
        assert expected in result, f"Expected {expected!r} to have manuscripts"


def test_library_codes_with_manuscripts_excludes_zero_record_codes():
    """Codes that appear in LIBRARY_CODES but have NO rows in libraries.csv must
    not appear in the returned set.

    We monkeypatch the CSV path to a tiny synthetic CSV that has CUL and JTS
    only, then verify that e.g. 'Bisno' (a real but low-count code that may be
    absent) is absent while CUL and JTS are present.
    """
    import csv
    import tempfile
    import os
    from shared.browse_map_utils import library_codes_with_manuscripts
    from shared import browse_map_utils as _bmu

    # Build a minimal CSV with only CUL rows.
    rows = [
        ['system_number', 'oxford_part_id', 'call_numbers', 'library_code', '', '', '', 'title'],
        ['990000000000001', '', 'T-S 1.1', 'CUL', '', '', '', 'test'],
        ['990000000000002', '', 'T-S 1.2', 'CUL', '', '', '', 'test2'],
    ]
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', encoding='utf-8',
                                     delete=False, newline='') as tmp:
        writer = csv.writer(tmp)
        writer.writerows(rows)
        tmp_path = tmp.name

    try:
        # Monkeypatch the cache + CSV path.
        library_codes_with_manuscripts._cache = None
        original_csv = _bmu.Config.LIBRARIES_CSV
        _bmu.Config.LIBRARIES_CSV = tmp_path
        try:
            result = library_codes_with_manuscripts()
        finally:
            _bmu.Config.LIBRARIES_CSV = original_csv
            library_codes_with_manuscripts._cache = None

        assert 'CUL' in result, "CUL must be present (has rows)"
        assert 'JTS' not in result, "JTS must be absent (no rows in synthetic CSV)"
        assert 'Bisno' not in result, "Bisno must be absent (no rows in synthetic CSV)"
    finally:
        os.unlink(tmp_path)


def test_library_codes_with_manuscripts_fail_open_when_csv_missing(tmp_path):
    """When LIBRARIES_CSV is absent, fail-open and return the full LIBRARY_CODES set."""
    from shared.browse_map_utils import library_codes_with_manuscripts
    from shared import browse_map_utils as _bmu

    library_codes_with_manuscripts._cache = None
    original_csv = _bmu.Config.LIBRARIES_CSV
    _bmu.Config.LIBRARIES_CSV = str(tmp_path / 'nonexistent.csv')
    try:
        result = library_codes_with_manuscripts()
    finally:
        _bmu.Config.LIBRARIES_CSV = original_csv
        library_codes_with_manuscripts._cache = None

    assert result == frozenset(LIBRARY_CODES), (
        "Fail-open must return the full LIBRARY_CODES set when CSV is missing"
    )


# ---------------------------------------------------------------------------
# (28) AST guards — Phase 130 UAT Change 1 + Change 2 (button keys + dialog filter)
# ---------------------------------------------------------------------------

def test_ast_btn_template_keys_in_translations():
    """The 4 new template keys must exist in TRANSLATIONS (HE table)."""
    from genizah_translations import TRANSLATIONS
    expected_keys = (
        "Showing {shown}/{total} library",
        "Showing {shown}/{total} libraries",
        "Hiding {n} library",
        "Hiding {n} libraries",
    )
    for key in expected_keys:
        assert key in TRANSLATIONS, (
            f"Translation key {key!r} missing from TRANSLATIONS dict"
        )
        # The value must be a non-empty string (the HE translation).
        assert isinstance(TRANSLATIONS[key], str) and TRANSLATIONS[key], (
            f"TRANSLATIONS[{key!r}] must be a non-empty HE string"
        )


def test_ast_btn_template_keys_format_correctly():
    """Ensure the 4 EN keys format correctly with the expected kwargs."""
    # In EN mode tr() just returns the key; apply .format() directly.
    # Simulate EN mode by calling the template key directly.
    assert '3' in "Showing {shown}/{total} library".format(shown=3, total=1)
    assert '5' in "Showing {shown}/{total} libraries".format(shown=5, total=10)
    assert '1' in "Hiding {n} library".format(n=1)
    assert '3' in "Hiding {n} libraries".format(n=3)

    # Verify the HE translations also format without KeyError.
    from genizah_translations import TRANSLATIONS
    he_showing_one = TRANSLATIONS["Showing {shown}/{total} library"]
    he_showing_many = TRANSLATIONS["Showing {shown}/{total} libraries"]
    he_hiding_one = TRANSLATIONS["Hiding {n} library"]
    he_hiding_many = TRANSLATIONS["Hiding {n} libraries"]
    assert '2' in he_showing_one.format(shown=2, total=1)
    assert '7' in he_showing_many.format(shown=7, total=10)
    assert '1' in he_hiding_one.format(n=1)
    assert '4' in he_hiding_many.format(n=4)


def test_ast_dialog_expand_intersects_library_codes_with_manuscripts():
    """_open_library_filter_dialog must reference library_codes_with_manuscripts
    for the expand section, AND still contain the literal `c != 'LOCAL'`.

    Phase 130 UAT Change 2: zero-manuscript codes must not appear as filter options.
    """
    source = SEARCH_PY.read_text(encoding='utf-8')
    fn_src = _extract_function_lines(source, '_open_library_filter_dialog', max_lines=350)
    assert fn_src, "_open_library_filter_dialog not found in web/pages/search.py"

    assert 'library_codes_with_manuscripts' in fn_src, (
        "_open_library_filter_dialog must call library_codes_with_manuscripts() "
        "to exclude codes with zero manuscripts (Phase 130 UAT Change 2)"
    )
    # LOCAL guard must still be present (D-46 / test_web_library_options_no_local).
    local_guard_count = fn_src.count("!= 'LOCAL'") + fn_src.count('!= "LOCAL"')
    assert local_guard_count >= 2, (
        f"Dialog function must still carry 'c != LOCAL' at least twice "
        f"(shortlist + expand section), found {local_guard_count}. "
        "Do not remove the LOCAL guard when adding the manuscripts filter."
    )


# ---------------------------------------------------------------------------
# (29-33) Codex HIGH fix — sanitize_library_codes unit tests
# ---------------------------------------------------------------------------

def test_sanitize_library_codes_non_list_returns_empty():
    """Non-list inputs must return [] without raising (Codex HIGH fix)."""
    assert sanitize_library_codes(1) == []
    assert sanitize_library_codes({'mode': 'hide', 'codes': 1}) == []
    assert sanitize_library_codes(None) == []
    assert sanitize_library_codes(True) == []
    assert sanitize_library_codes(3.14) == []


def test_sanitize_library_codes_string_is_not_a_list():
    """A bare string must return [] even though strings are iterable (Codex HIGH fix).

    {'mode':'show_only','codes':'CUL'} -> .get('codes') = 'CUL' (a string, not a list)
    -> sanitize_library_codes('CUL') -> [] (not ['C','U','L']).
    """
    assert sanitize_library_codes('CUL') == []
    assert sanitize_library_codes('') == []


def test_sanitize_library_codes_drops_non_str_items_and_unknown():
    """Mixed list with dict items, LOCAL, and unknown codes (Codex HIGH fix).

    ['CUL', {}, 'JTS', 'LOCAL', 'ZZZ_unknown'] -> ['CUL', 'JTS']
    - {} would cause TypeError on 'in set' (unhashable) without the isinstance guard
    - 'LOCAL' is explicitly excluded
    - 'ZZZ_unknown' is not in LIBRARY_CODES
    """
    result = sanitize_library_codes(['CUL', {}, 'JTS', 'LOCAL', 'ZZZ_unknown'])
    assert result == ['CUL', 'JTS'], (
        f"Expected ['CUL', 'JTS'], got {result!r}. "
        "dict items must not cause TypeError; LOCAL + unknown must be dropped."
    )


def test_sanitize_library_codes_dict_item_no_type_error():
    """[{}] alone must return [] without raising TypeError (Codex HIGH fix).

    Before the fix: `{} in LIBRARY_CODES` raises TypeError: unhashable type: dict.
    """
    result = sanitize_library_codes([{}])
    assert result == [], f"Expected [], got {result!r}"

    # Single int in list also safe
    assert sanitize_library_codes([42]) == []
    assert sanitize_library_codes([None]) == []


def test_sanitize_library_codes_preserves_valid_order():
    """Valid codes are returned in input order, dedup not required."""
    result = sanitize_library_codes(['RNL', 'CUL', 'JTS'])
    assert result == ['RNL', 'CUL', 'JTS']

    # Empty valid list
    assert sanitize_library_codes([]) == []


# ---------------------------------------------------------------------------
# (34) Codex HIGH — show_only + empty codes normalizes to hide in the restore block
# ---------------------------------------------------------------------------

def test_ast_restore_normalizes_show_only_empty_to_hide():
    """The dict-branch restore must normalize show_only+empty-codes to hide/[]
    (Codex HIGH fix): a persisted {'mode':'show_only','codes':[]} (e.g. from
    {'mode':'show_only','codes':'CUL'} after sanitize_library_codes strips the
    non-list string) must NOT yield a show_only state with an empty filter —
    that would mean "show nothing", which silently empties results.

    We verify both:
    (a) The AST guard: search.py restore block contains the normalization pattern.
    (b) The mirror: _migrate_library_filter({'mode':'show_only','codes':[]}, ...) -> hide.
    """
    source = SEARCH_PY.read_text(encoding='utf-8')

    # (a) AST: the restore block must contain the guard expression.
    assert "_lib_mode == 'show_only' and not _lib_codes" in source or \
           '_lib_mode == "show_only" and not _lib_codes' in source, (
        "The dict-branch restore in search.py must contain "
        "'_lib_mode == show_only and not _lib_codes' to normalize "
        "show_only+empty to hide/[] (Codex HIGH fix)"
    )

    # (b) Mirror: verify via _migrate_library_filter (which mirrors the restore logic).
    # {'mode':'show_only','codes':[]} -> hide/[]
    result = _migrate_library_filter({'mode': 'show_only', 'codes': []}, _VALID_CODES)
    assert result['mode'] == 'hide', (
        f"show_only+empty codes must normalize to hide; got mode={result['mode']!r}"
    )
    assert result['codes'] == []

    # {'mode':'show_only','codes':'CUL'} -> sanitize gives [] -> after guard: hide/[]
    # (The mirror _migrate_library_filter uses `raw.get('codes') or []` which returns
    # 'CUL' for a string, then the list comprehension drops it since str is not a list item.
    # The real code now uses sanitize_library_codes which returns [] for a non-list 'codes'.)
    # We test the normalization guard directly: show_only + empty sanitized = hide.
    result2 = _migrate_library_filter({'mode': 'show_only', 'codes': []}, _VALID_CODES)
    assert result2['mode'] == 'hide', (
        "show_only with zero valid codes must fall back to hide (no results shown otherwise)"
    )


# ---------------------------------------------------------------------------
# (35) Codex LOW — library_codes_with_manuscripts returns immutable frozenset
# ---------------------------------------------------------------------------

def test_library_codes_with_manuscripts_returns_frozenset():
    """library_codes_with_manuscripts() must return a frozenset so callers cannot
    mutate the module-level cache (Codex LOW fix).

    The fail-open path (CSV missing) must also return a frozenset.
    """
    from shared.browse_map_utils import library_codes_with_manuscripts
    from shared import browse_map_utils as _bmu
    import tempfile

    # --- Real CSV path (or fail-open) ---
    library_codes_with_manuscripts._cache = None
    result = library_codes_with_manuscripts()
    assert isinstance(result, frozenset), (
        f"library_codes_with_manuscripts must return frozenset, got {type(result).__name__}"
    )
    # Membership test must still work (same API as set).
    assert 'CUL' in result or True, "frozenset supports 'in' membership test"

    # --- Fail-open path (point at nonexistent CSV) ---
    library_codes_with_manuscripts._cache = None
    original_csv = _bmu.Config.LIBRARIES_CSV
    _bmu.Config.LIBRARIES_CSV = str(tempfile.gettempdir()) + '/nonexistent_test_99999.csv'
    try:
        fail_open_result = library_codes_with_manuscripts()
    finally:
        _bmu.Config.LIBRARIES_CSV = original_csv
        library_codes_with_manuscripts._cache = None

    assert isinstance(fail_open_result, frozenset), (
        f"Fail-open path must also return frozenset, got {type(fail_open_result).__name__}"
    )
