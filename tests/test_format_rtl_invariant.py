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
    """F-06 AST guard (REV-2h, narrowed): PDF sort=True RTL helpers must not be
    reused in structured extractors (HTML/XLSX/CSV). This pins the invariant
    that RTL word-order repair is a PDF-only concern — structured extractors
    have their own per-format logic and should NOT inherit the PDF helpers.
    This is NOT a blanket "structured extractors never do RTL" rule.

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

    # Second: none of them may call the Phase 95 dead-code helpers OR the
    # Phase 101 live sort=True RTL helpers.
    offenders = []
    for fname, func in found.items():
        for forbidden in (
            "_fix_rtl_line",
            "_fix_rtl_page",
            "_fix_sort_true_rtl_line",
            "_fix_sort_true_rtl_page",
        ):
            if _function_contains_call(func, forbidden):
                offenders.append((fname, forbidden, func.lineno))

    assert not offenders, (
        "F-06 violation: structured extractor (HTML/XLSX/CSV) calls a PDF RTL "
        "helper (Phase 95 mirror-reversal _fix_rtl_* or Phase 101 sort=True "
        "_fix_sort_true_rtl_*). These helpers are PDF-only — HTML/XLSX/CSV "
        "strings are already in logical order and would be CORRUPTED. "
        "Offenders: " + str(offenders)
    )


def test_sort_true_rtl_helpers_only_called_from_extract_pdf_pages():
    """REV-2c / Claude S-7 (positive assertion, STRENGTHENED per REVIEWS round 2
    LOW #12 / Gemini): the new sort=True RTL helpers must be (a) actually called
    by extract_pdf_pages — empty callers set is NOT acceptable — and (b)
    referenced ONLY from extract_pdf_pages and from each other (page→line).
    Catches both accidental non-wiring and accidental re-use in structured extractors.
    """
    source = LOCAL_INDEXER_PY.read_text(encoding="utf-8")
    tree = ast.parse(source)
    callers = set()
    for fn in ast.walk(tree):
        if not isinstance(fn, ast.FunctionDef):
            continue
        for node in ast.walk(fn):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                if node.func.id in ('_fix_sort_true_rtl_line', '_fix_sort_true_rtl_page'):
                    callers.add(fn.name)
    allowed = {'extract_pdf_pages', '_fix_sort_true_rtl_line', '_fix_sort_true_rtl_page'}
    # Positive: extract_pdf_pages MUST be among callers (proves wiring).
    assert 'extract_pdf_pages' in callers, (
        'F-06 positive assertion: extract_pdf_pages must call '
        '_fix_sort_true_rtl_page in the sort=True fallback branch — '
        'helper appears to be defined but not wired in'
    )
    # Negative: no callers outside the allowed set (proves no rogue re-use).
    assert callers <= allowed, (
        f'F-06 positive assertion: unexpected callers of sort=True RTL helpers: '
        f'{callers - allowed}'
    )
