# -*- coding: utf-8 -*-
"""Phase 95 D-46 — static AST guard: no web/pages/*.py iterates LIBRARY_CODES
without filtering out 'LOCAL'.

Mirrors tests/test_pgp_filter_cascade.py AST scanner pattern; scans every web
page module instead of a single file.

Rationale: genizah_core.LIBRARY_CODES gains the 'LOCAL' entry in Phase 95
(D-13). Any web page that builds a library-filter dropdown from LIBRARY_CODES
without skipping code == 'LOCAL' would expose 'My Library' as a web filter
option — violating D-30 and the web-only out-of-scope boundary from the SPEC.

This guard makes such a regression fail CI instantly.
"""
import ast
import pathlib

WEB_PAGES_DIR = pathlib.Path(__file__).parent.parent / "web" / "pages"

# No exemptions today. Add function names here only if a future consumer has
# a legitimate reason to iterate ALL library codes including LOCAL (e.g. an
# admin-only debug page). Document the reason alongside the exemption.
EXEMPT_FUNCTIONS: set[str] = set()


def _function_contains_library_codes_iteration(func_node) -> bool:
    """Heuristic: function body contains a reference to LIBRARY_CODES."""
    for node in ast.walk(func_node):
        # Direct name lookup: LIBRARY_CODES
        if isinstance(node, ast.Name) and node.id == "LIBRARY_CODES":
            return True
        # Attribute access: module.LIBRARY_CODES
        if isinstance(node, ast.Attribute) and node.attr == "LIBRARY_CODES":
            return True
    return False


def _function_contains_local_guard(func_node) -> bool:
    """Heuristic: function body contains a string comparison against 'LOCAL'.

    Catches patterns like:
      if code == 'LOCAL': continue
      if code != 'LOCAL': ...
      [... for code in LIBRARY_CODES if code != 'LOCAL']
      options = [(c, n) for c, n in LIBRARY_CODES.items() if c != 'LOCAL']
    """
    for node in ast.walk(func_node):
        if isinstance(node, ast.Compare):
            # `something == 'LOCAL'` or `something != 'LOCAL'`
            for operand in (node.left, *node.comparators):
                if isinstance(operand, ast.Constant) and operand.value == "LOCAL":
                    return True
        elif isinstance(node, ast.Constant) and node.value == "LOCAL":
            # Looser fallback: 'LOCAL' string literal anywhere in the function
            # (catches dict-key omission patterns and `in {'LOCAL', ...}` sets).
            return True
    return False


def _iter_function_defs(tree):
    """Yield all function and async-function definitions in the AST tree."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node


def test_no_web_page_iterates_library_codes_without_local_guard():
    """D-46: every web/pages/*.py function that iterates LIBRARY_CODES must
    also contain a guard filtering out code == 'LOCAL'.

    Fails CI if a new library-filter consumer is added without the guard,
    preventing 'My Library' from ever appearing as a web filter option.
    """
    offenders = []
    for py_file in WEB_PAGES_DIR.rglob("*.py"):
        try:
            source = py_file.read_text(encoding="utf-8")
            tree = ast.parse(source)
        except (SyntaxError, OSError):
            continue
        for func in _iter_function_defs(tree):
            if func.name in EXEMPT_FUNCTIONS:
                continue
            if not _function_contains_library_codes_iteration(func):
                continue
            if not _function_contains_local_guard(func):
                offenders.append(
                    (
                        str(py_file.relative_to(WEB_PAGES_DIR.parent.parent)),
                        func.name,
                        func.lineno,
                    )
                )
    assert not offenders, (
        "Phase 95 D-46 violation — web/pages/*.py functions iterate "
        "LIBRARY_CODES without a 'LOCAL' guard:\n"
        + "\n".join(
            f"  {f}:{lineno} in {name}()"
            for f, name, lineno in offenders
        )
    )


def test_exempt_functions_set_is_defined():
    """Structural guard: EXEMPT_FUNCTIONS set must exist (even if empty).

    Prevents the guard from being silently deleted and replaced with a
    hard-coded pass-through.
    """
    assert isinstance(EXEMPT_FUNCTIONS, set)


def test_web_pages_dir_exists():
    """Sanity check: web/pages/ directory must exist for the scanner to work."""
    assert WEB_PAGES_DIR.is_dir(), (
        f"web/pages/ directory not found at {WEB_PAGES_DIR}. "
        "The AST scanner has nothing to scan."
    )
