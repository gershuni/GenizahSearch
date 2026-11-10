# -*- coding: utf-8 -*-
"""Phase 97 U-04 — View All incremental rendering (LD-10 lock).

Tests (AST-based — no Qt import required):
  T-E-6a  test_qtimer_singleshot_present — the View All code path in genizah_app.py
          contains at least one QTimer.singleShot(0, ...) call.
  T-E-6b  test_apply_line_numbered_text_called_per_batch — _append_next_view_all_batch
          (or _render_view_all_batch) calls apply_line_numbered_text (LD-10 anti-bypass
          lock: the gutter helper must NOT be bypassed per batch).
"""
import ast
import pathlib

_GENIZAH_APP_SRC = (pathlib.Path(__file__).resolve().parent.parent / "genizah_app.py").read_text(
    encoding="utf-8"
)
_GENIZAH_APP_TREE = ast.parse(_GENIZAH_APP_SRC)


def _find_function(tree: ast.AST, name: str):
    """Return the FunctionDef node with given name (first match at any depth)."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    return None


def _contains_call(func_node: ast.AST, callee_name: str) -> bool:
    """Return True if func_node's body contains a Call to callee_name (by attribute or name)."""
    for node in ast.walk(func_node):
        if isinstance(node, ast.Call):
            func = node.func
            # Check direct name: callee_name(...)
            if isinstance(func, ast.Name) and func.id == callee_name:
                return True
            # Check attribute: obj.callee_name(...)
            if isinstance(func, ast.Attribute) and func.attr == callee_name:
                return True
    return False


def _contains_qtimer_singleshot_zero(tree: ast.AST) -> bool:
    """Return True if the tree contains QTimer.singleShot(0, ...) call."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "singleShot"):
            continue
        # Check that the first arg is 0
        if node.args and isinstance(node.args[0], ast.Constant) and node.args[0].value == 0:
            return True
    return False


# ---------------------------------------------------------------------------
# T-E-6a: QTimer.singleShot(0, ...) exists in the View All / append path
# ---------------------------------------------------------------------------

def test_qtimer_singleshot_present():
    """genizah_app.py must contain at least one QTimer.singleShot(0, ...) call.

    This is the Phase 97 U-04 LD-10 incremental render trigger. The View All
    path renders the first 50 pages immediately, then schedules remaining
    batches via QTimer.singleShot(0, self._append_next_view_all_batch).
    """
    found = _contains_qtimer_singleshot_zero(_GENIZAH_APP_TREE)
    assert found, (
        "QTimer.singleShot(0, ...) not found in genizah_app.py. "
        "Phase 97 U-04 incremental render requires QTimer.singleShot(0, callback) "
        "to schedule batches without freezing the event loop (T-97E-05). "
        "This test will pass after Task 3 implementation."
    )


# ---------------------------------------------------------------------------
# T-E-6b: _append_next_view_all_batch or _render_view_all_batch calls apply_line_numbered_text
# ---------------------------------------------------------------------------

def test_apply_line_numbered_text_called_per_batch():
    """_render_view_all_batch (or _append_next_view_all_batch) must call apply_line_numbered_text.

    LD-10 lock: apply_line_numbered_text owns the line-gutter painter + page-block
    marking. Every incremental batch must go through this helper — bypassing it
    (e.g., calling setText directly) would break line numbering.
    """
    render_node = _find_function(_GENIZAH_APP_TREE, "_render_view_all_batch")
    append_node = _find_function(_GENIZAH_APP_TREE, "_append_next_view_all_batch")

    assert render_node is not None or append_node is not None, (
        "Neither _render_view_all_batch nor _append_next_view_all_batch found in genizah_app.py. "
        "Phase 97 U-04 incremental render helpers are created in Task 3."
    )

    # Check that at least one of these functions calls apply_line_numbered_text
    found_in_render = render_node is not None and _contains_call(
        render_node, "apply_line_numbered_text"
    )
    found_in_append = append_node is not None and _contains_call(
        append_node, "apply_line_numbered_text"
    )

    assert found_in_render or found_in_append, (
        "Neither _render_view_all_batch nor _append_next_view_all_batch calls "
        "apply_line_numbered_text. LD-10 requires each batch to use the gutter "
        "helper — do NOT bypass it with direct setText/setHtml calls."
    )
