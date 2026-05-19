"""Static cascade-coverage guard for the PGP filter (Phase 999.2 / MEDIUM-3 from reviews).

Every function in web/pages/search.py that calls _apply_printed_filter must also
call _apply_pgp_filter — OR be on the documented exempt list below. This guard
is defense-in-depth against future drift: if a developer adds a new render
branch that forgets to apply the PGP filter on top of printed_filter, this
test fails with a specific function name so they can fix it before the bug
ships to users.

Pattern source: tests/test_no_raw_storage_access.py (the Phase 87 AST scanner).
"""
import ast
from pathlib import Path

SEARCH_PY = Path(__file__).parent.parent / 'web' / 'pages' / 'search.py'

# Functions exempt from the "must also call _apply_pgp_filter" rule.
# Add an entry here ONLY with a comment explaining why the cascade does not apply.
# Empty at plan-revision time — every printed-filter caller is expected to be PGP-aware.
EXEMPT_FUNCTIONS: set[str] = set()


def _function_contains_call(func_node, name: str) -> bool:
    """True if func_node's body contains a Call to the given function name (recursive walk)."""
    for node in ast.walk(func_node):
        if isinstance(node, ast.Call):
            callee = node.func
            if isinstance(callee, ast.Name) and callee.id == name:
                return True
            if isinstance(callee, ast.Attribute) and callee.attr == name:
                return True
    return False


def _iter_function_defs(tree):
    """Yield every FunctionDef + AsyncFunctionDef in the tree, including nested."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node


def test_every_printed_filter_caller_also_calls_pgp_filter():
    """MEDIUM-3 invariant: cascade coverage — printed_filter ⇒ pgp_filter."""
    source = SEARCH_PY.read_text(encoding='utf-8')
    tree = ast.parse(source)

    offenders = []
    for func in _iter_function_defs(tree):
        calls_printed = _function_contains_call(func, '_apply_printed_filter')
        if not calls_printed:
            continue
        if func.name in EXEMPT_FUNCTIONS:
            continue
        calls_pgp = _function_contains_call(func, '_apply_pgp_filter')
        if not calls_pgp:
            offenders.append((func.name, func.lineno))

    assert not offenders, (
        f"Phase 999.2 cascade-coverage drift detected. The following functions in "
        f"web/pages/search.py call _apply_printed_filter but do NOT call _apply_pgp_filter:\n"
        + '\n'.join(f'  - {name} (line {lineno})' for name, lineno in offenders)
        + '\n\nFix: either add _apply_pgp_filter(...) immediately after the '
        '_apply_printed_filter(...) call in each function, or add the function name '
        'to EXEMPT_FUNCTIONS in this test file with a comment explaining why the '
        'PGP filter is structurally not applicable there.'
    )


def test_apply_pgp_filter_function_exists():
    """Sanity check: the predicate function the cascade guard depends on must exist."""
    source = SEARCH_PY.read_text(encoding='utf-8')
    tree = ast.parse(source)
    names = {f.name for f in _iter_function_defs(tree)}
    assert '_apply_pgp_filter' in names, (
        'web/pages/search.py is missing the _apply_pgp_filter function defined by Task 3'
    )


def test_apply_pgp_filter_dispatched_in_widened_elifs():
    """Defense-in-depth: every elif branch that dispatches to _apply_printed_filter_and_render
    on `printed_filter != 'all'` should ALSO fire on `pgp_filter != 'all'`.

    This catches the bypass-by-design pattern the cross-AI review flagged (HIGH-1, HIGH-2, HIGH-3):
    a function checks `if printed_filter != 'all':` but skips the dispatch when only PGP is active,
    falling through to a raw `render_results` that doesn't apply PGP.
    """
    source = SEARCH_PY.read_text(encoding='utf-8')
    # The widened-elif pattern Task 3 establishes:
    #   elif (printed_filter != 'all' and printed_ids) or pgp_filter != 'all':
    # Count occurrences — Task 3 establishes at least 4 (manuscript_exclusions empty + swap,
    # word_search, render_with_filters).
    widened_count = source.count("or search_state.pgp_filter != 'all'")
    assert widened_count >= 4, (
        f'Expected at least 4 widened elif branches in web/pages/search.py (manuscript '
        f'exclusions empty-branch + swap-branch + word-search + render_with_filters). '
        f'Found {widened_count}. The cascade-coverage invariant from Task 3 is broken.'
    )


def test_apply_pgp_filter_called_after_apply_printed_filter():
    """Ordering invariant (D-11): in every function that applies both filters,
    _apply_pgp_filter is called AFTER _apply_printed_filter on the same list.
    """
    source = SEARCH_PY.read_text(encoding='utf-8')
    # Heuristic check: every line containing `_apply_pgp_filter(filtered)` should be preceded
    # within ~3 lines by a line containing `_apply_printed_filter(filtered)` or
    # `_apply_printed_filter(results_list)` (the predicate input names used in the codebase).
    lines = source.splitlines()
    pgp_lines = [i for i, ln in enumerate(lines) if '_apply_pgp_filter(filtered)' in ln]
    assert pgp_lines, 'No _apply_pgp_filter(filtered) call sites found — Task 3 has not landed'
    for idx in pgp_lines:
        window = '\n'.join(lines[max(0, idx - 3):idx])
        assert (
            '_apply_printed_filter(filtered)' in window
            or '_apply_printed_filter(results_list)' in window
        ), (
            f'Line {idx + 1}: _apply_pgp_filter is called but no _apply_printed_filter '
            f'precedes it within 3 lines. Cascade ordering violated (D-11).\n'
            f'Window:\n{window}'
        )
