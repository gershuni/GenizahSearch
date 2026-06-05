# -*- coding: utf-8 -*-
"""AST guard: TabularQueryBuilderDialog.__init__ must NOT set dialog-level
RightToLeft layout direction.

D-06/R-04: The dialog-level setLayoutDirection(RightToLeft) at genizah_app.py:1555
mirrors the QHBoxLayout arrangement (checkboxes appear on wrong side) and clips
labels. The fix removes that one directive; per-word QLineEdit RTL (line 1779)
and preview label RTL (line 1703) must remain.

This guard runs without QApplication — pure AST analysis only.
"""
import ast
import pathlib

TARGET = pathlib.Path(__file__).parent.parent / "genizah_app.py"


def _find_class_def(tree: ast.Module, class_name: str):
    """Return the ClassDef node for the given class name, or None."""
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return node
    return None


def _find_init_def(class_node: ast.ClassDef):
    """Return the FunctionDef for __init__ inside the class, or None."""
    for node in ast.walk(class_node):
        if isinstance(node, ast.FunctionDef) and node.name == "__init__":
            return node
    return None


def _collect_dialog_level_rtl_calls(init_node: ast.FunctionDef):
    """Return list of (lineno,) for any Call that is self.setLayoutDirection(RightToLeft...)
    at the dialog level (i.e., `self` is the receiver, not a child widget).

    Specifically, we look for:
        Call(func=Attribute(value=Name(id='self'), attr='setLayoutDirection'),
             args=[... something containing 'RightToLeft' ...])
    """
    offenders = []
    for node in ast.walk(init_node):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "setLayoutDirection"):
            continue
        # Check that the receiver is `self` (not e.g. inp.setLayoutDirection)
        if not (isinstance(func.value, ast.Name) and func.value.id == "self"):
            continue
        # Check if any argument contains 'RightToLeft'
        for arg in node.args:
            arg_src = ast.unparse(arg)
            if "RightToLeft" in arg_src:
                offenders.append(node.lineno)
    return offenders


def test_tabular_builder_dialog_no_dialog_level_rtl():
    """D-06: TabularQueryBuilderDialog.__init__ must not call
    self.setLayoutDirection(RightToLeft) at the dialog level.

    Per-widget RTL (individual QLineEdit inputs and preview label) must remain —
    only the dialog-LEVEL directive is forbidden because it mirrors the chrome.
    """
    source = TARGET.read_text(encoding="utf-8")
    tree = ast.parse(source)

    class_node = _find_class_def(tree, "TabularQueryBuilderDialog")
    assert class_node is not None, (
        "TabularQueryBuilderDialog class not found in genizah_app.py — "
        "the guard target may have been renamed"
    )

    init_node = _find_init_def(class_node)
    assert init_node is not None, (
        "TabularQueryBuilderDialog.__init__ not found — "
        "the class may have lost its constructor"
    )

    offenders = _collect_dialog_level_rtl_calls(init_node)
    assert not offenders, (
        "D-06 violation: TabularQueryBuilderDialog.__init__ contains "
        "dialog-level self.setLayoutDirection(RightToLeft) at line(s): "
        + ", ".join(str(ln) for ln in offenders)
        + "\n\nFix: remove the dialog-level directive; individual QLineEdit inputs "
        "and the preview label already have their own RTL directives."
    )


def test_target_file_exists():
    """Guard against genizah_app.py being accidentally removed."""
    assert TARGET.exists(), f"genizah_app.py not found at {TARGET}"
