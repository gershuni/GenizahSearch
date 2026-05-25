# -*- coding: utf-8 -*-
"""Phase 97 F-06 — RTL invariant AST guard.

Asserts that the three new Wave C extractors (extract_html_pages,
extract_xlsx_pages, extract_csv_pages) in shared/local_indexer.py
contain ZERO calls to _fix_rtl_line or _fix_rtl_page.

Rationale (CONTEXT F-06 + RESEARCH Issue #2): lxml.html, openpyxl, and
Python csv all produce logical-order Hebrew strings already. Applying
Phase 95's _fix_rtl_line / _fix_rtl_page (designed for PDF mirror-reversal)
to HTML/XLSX/CSV would CORRUPT already-correct Hebrew text. These helpers are
DEAD CODE per local_indexer.py D-02 — they must never be called from the
new format extractors.

This test is a permanent CI guard. Any future PR that accidentally wires
_fix_rtl_* into the new extractors will fail here first.
"""
from __future__ import annotations

import ast
import pathlib

LOCAL_INDEXER_PY = pathlib.Path(__file__).resolve().parent.parent / "shared" / "local_indexer.py"


def _iter_function_defs(tree):
    """Yield every FunctionDef + AsyncFunctionDef in the tree, including nested."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node


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


def test_format_rtl_invariant_no_fix_rtl_in_new_extractors():
    """F-06: HTML/XLSX/CSV extractors MUST NOT call Phase 95 _fix_rtl_* helpers.

    Also asserts all three extractor functions exist (RED in Task 1;
    GREEN after Task 2 adds the implementations).
    """
    source = LOCAL_INDEXER_PY.read_text(encoding="utf-8")
    tree = ast.parse(source)
    target_functions = {"extract_html_pages", "extract_xlsx_pages", "extract_csv_pages"}
    found = {f.name: f for f in _iter_function_defs(tree) if f.name in target_functions}

    # First: all 3 functions must exist (RED guard — fails until Task 2 adds them)
    assert len(found) == 3, (
        f"Expected all 3 Wave C extractors to exist in shared/local_indexer.py, "
        f"found: {set(found)}. Missing: {target_functions - set(found)}"
    )

    # Second: none of them may call _fix_rtl_line or _fix_rtl_page
    offenders = []
    for fname, func in found.items():
        for forbidden in ("_fix_rtl_line", "_fix_rtl_page"):
            if _function_contains_call(func, forbidden):
                offenders.append((fname, forbidden, func.lineno))

    assert not offenders, (
        "F-06 violation: format extractor calls Phase 95 PDF mirror-reversal helper. "
        "HTML/XLSX/CSV strings are already in logical order — applying _fix_rtl_* "
        "would CORRUPT Hebrew text. Offenders: "
        + str(offenders)
    )
