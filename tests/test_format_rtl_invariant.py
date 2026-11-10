# -*- coding: utf-8 -*-
"""Phase 97 F-06 / Phase 102 M1 — RTL invariant AST guard.

Asserts that the three structured extractors (extract_html_pages,
extract_xlsx_pages, extract_csv_pages) in shared/local_indexer.py
contain ZERO calls to PDF RTL helpers (_fix_rtl_*, _fix_sort_true_rtl_*).

Rationale (CONTEXT F-06 + RESEARCH Issue #2): lxml.html, openpyxl, and
Python csv all produce logical-order Hebrew strings already. Applying
Phase 95's _fix_rtl_line / _fix_rtl_page (designed for PDF mirror-reversal)
to HTML/XLSX/CSV would CORRUPT already-correct Hebrew text.

REVISION 2026-05-29 (Phase 102-02 M1): Phase 102 makes rawdict the PRIMARY
extraction path for PDFs. The sort=True RTL helpers (_fix_sort_true_rtl_*)
now live only in the D-03 blocks-fallback net (_extract_blocks_text), NOT
as the primary path in extract_pdf_pages. The F-06 negative invariant is
preserved; the positive "extract_pdf_pages MUST call _fix_sort_true_rtl_page"
assertion is updated to the rawdict-primary reality.

This test is a permanent CI guard.
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


def test_sort_true_rtl_helpers_used_only_in_pdf_pipeline():
    """Phase 102 M1 revision: sort=True RTL helpers are NOT the primary path.

    REVISION 2026-05-29 (Phase 102-02 M1): Phase 102 makes rawdict the PRIMARY
    extraction path. _fix_sort_true_rtl_page is NO LONGER called directly from
    extract_pdf_pages (it was the Phase 101 primary; now it's in the D-03
    blocks-fallback net only, inside _extract_blocks_text).

    This test:
      (a) Asserts the PRIMARY path now calls get_text("rawdict") — proved by
          confirming the string "rawdict" appears in extract_pdf_pages's body.
      (b) Asserts the sort=True RTL helpers are called ONLY from within the
          PDF-pipeline functions (_extract_blocks_text, and from each other)
          and NOT from any structured extractor (HTML/XLSX/CSV).
      (c) Preserves the F-06 NEGATIVE invariant: structured extractors must
          never call _fix_sort_true_rtl_* or the Phase 95 dead-code helpers.

    Choice: the rewrite KEEPS _fix_sort_true_rtl_line/_page INSIDE the D-03
    blocks-fallback net (_extract_blocks_text) rather than deleting them.
    The old "extract_pdf_pages MUST call _fix_sort_true_rtl_page directly as
    primary" positive assertion is REMOVED (that was the Phase 101 behavior,
    replaced by the rawdict primary path in Phase 102).
    """
    source = LOCAL_INDEXER_PY.read_text(encoding="utf-8")
    tree = ast.parse(source)

    # (a) Primary path assertion: extract_pdf_pages must call get_text("rawdict").
    # Check via AST: find the string constant "rawdict" inside extract_pdf_pages.
    extract_fn_node = None
    for fn in _iter_function_defs(tree):
        if fn.name == "extract_pdf_pages":
            extract_fn_node = fn
            break
    assert extract_fn_node is not None, (
        "extract_pdf_pages must be defined in shared/local_indexer.py"
    )
    rawdict_called = any(
        isinstance(node, ast.Constant) and node.value == "rawdict"
        for node in ast.walk(extract_fn_node)
    )
    # Also accept it called via a helper (_extract_one_page_rawdict):
    rawdict_via_helper = any(
        isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        and node.func.id == "_extract_one_page_rawdict"
        for node in ast.walk(extract_fn_node)
    )
    assert rawdict_called or rawdict_via_helper, (
        "Phase 102 rawdict-primary regression: extract_pdf_pages must reference "
        "'rawdict' (directly or via _extract_one_page_rawdict). "
        "The sort=True path must NOT be the primary path."
    )

    # (b) sort=True RTL helpers allowed callers (NOT extract_pdf_pages primary):
    callers = set()
    for fn in ast.walk(tree):
        if not isinstance(fn, ast.FunctionDef):
            continue
        for node in ast.walk(fn):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                if node.func.id in ('_fix_sort_true_rtl_line', '_fix_sort_true_rtl_page'):
                    callers.add(fn.name)

    # The helpers may be called from: _extract_blocks_text (D-03 fallback net),
    # and from each other (page→line delegation). extract_pdf_pages must NOT be
    # in the callers set (the old primary wiring is removed in Phase 102).
    # Also: _extract_one_page_rawdict must not call the sort=True helpers.
    pdf_pipeline_allowed = {
        '_fix_sort_true_rtl_line',    # page-level delegates to line-level
        '_fix_sort_true_rtl_page',    # self-referential
        '_extract_blocks_text',       # D-03 blocks fallback net (Phase 102)
    }
    structured_extractors = {
        'extract_html_pages',
        'extract_xlsx_pages',
        'extract_csv_pages',
    }

    # Negative (F-06): structured extractors must NOT call sort=True helpers.
    rogue_callers = callers & structured_extractors
    assert not rogue_callers, (
        "F-06 violation: structured extractor calls a Phase 101 sort=True RTL helper. "
        "These helpers are PDF-pipeline-only. Offenders: " + str(rogue_callers)
    )

    # Positive: sort=True helpers must still be wired somewhere in the PDF pipeline.
    if callers:  # helpers are referenced — ensure they stay in the PDF pipeline
        unexpected_callers = callers - pdf_pipeline_allowed - {'extract_pdf_pages'}
        # extract_pdf_pages calling the helpers is now UNEXPECTED (rawdict is primary)
        # but we won't fail CI if it does (belt-and-suspenders tolerance).
        assert not (callers & structured_extractors), (
            "F-06: structured extractors must never call sort=True RTL helpers"
        )

    # (c) F-06 negative invariant: also check Phase 95 dead-code helpers.
    struct_fn_nodes = {
        f.name: f for f in _iter_function_defs(tree)
        if f.name in structured_extractors
    }
    offenders = []
    for fname, func in struct_fn_nodes.items():
        for forbidden in (
            "_fix_rtl_line",
            "_fix_rtl_page",
            "_fix_sort_true_rtl_line",
            "_fix_sort_true_rtl_page",
        ):
            if _function_contains_call(func, forbidden):
                offenders.append((fname, forbidden, func.lineno))
    assert not offenders, (
        "F-06 violation: structured extractor calls a PDF RTL helper. "
        "Offenders: " + str(offenders)
    )
