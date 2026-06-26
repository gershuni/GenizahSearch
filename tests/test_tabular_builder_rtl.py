# -*- coding: utf-8 -*-
"""AST guard: TabularQueryBuilderDialog.__init__ must NOT set dialog-level
RightToLeft layout direction.

D-06/R-04: The dialog-level setLayoutDirection(RightToLeft) mirrors the
QHBoxLayout arrangement (checkboxes appear on wrong side) and clips labels.
The fix removes that one directive; per-word QLineEdit RTL and preview label
RTL must remain.

Phase 126 D1 (GUARD-03, additive): TabularQueryBuilderDialog was MOVED from
genizah_app.py to desktop/settings_dialogs.py (re-exported via a # noqa: F401
shim). This guard now AST-scans BOTH candidate locations and asserts against
whichever module actually defines the class (OR-location). It is intentionally
NOT flipped to new-only — that hard flip is Phase 127's job; keeping the OR
keeps the guard resilient regardless of where the source lives.

This guard runs without QApplication — pure AST analysis only.
"""
import ast
import pathlib

_REPO_ROOT = pathlib.Path(__file__).parent.parent

# OR-location candidate sources for the moved dialog class. Order is not
# significant — the guard uses whichever module defines the class.
CANDIDATE_TARGETS = [
    _REPO_ROOT / "genizah_app.py",
    _REPO_ROOT / "desktop" / "settings_dialogs.py",
]

# Back-compat alias: some tooling/imports referenced the single TARGET path.
TARGET = _REPO_ROOT / "genizah_app.py"


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


def _resolve_class_def(class_name: str):
    """Find ``class_name`` across the OR-location candidate sources.

    Returns ``(target_path, class_node)`` for the first candidate that both
    exists and defines the class. Returns ``(None, None)`` if no candidate
    defines it (which the caller asserts against).
    """
    for target in CANDIDATE_TARGETS:
        if not target.exists():
            continue
        tree = ast.parse(target.read_text(encoding="utf-8"))
        node = _find_class_def(tree, class_name)
        if node is not None:
            return target, node
    return None, None


def test_tabular_builder_dialog_no_dialog_level_rtl():
    """D-06: TabularQueryBuilderDialog.__init__ must not call
    self.setLayoutDirection(RightToLeft) at the dialog level.

    Per-widget RTL (individual QLineEdit inputs and preview label) must remain —
    only the dialog-LEVEL directive is forbidden because it mirrors the chrome.

    Phase 126 D1: accepts the class in EITHER genizah_app.py OR
    desktop/settings_dialogs.py (OR-location, additive retarget).
    """
    target, class_node = _resolve_class_def("TabularQueryBuilderDialog")
    assert class_node is not None, (
        "TabularQueryBuilderDialog class not found in any candidate location "
        + " | ".join(str(p) for p in CANDIDATE_TARGETS)
        + " — the guard target may have been renamed"
    )

    init_node = _find_init_def(class_node)
    assert init_node is not None, (
        "TabularQueryBuilderDialog.__init__ not found in "
        f"{target} — the class may have lost its constructor"
    )

    offenders = _collect_dialog_level_rtl_calls(init_node)
    assert not offenders, (
        "D-06 violation: TabularQueryBuilderDialog.__init__ contains "
        f"dialog-level self.setLayoutDirection(RightToLeft) in {target} at line(s): "
        + ", ".join(str(ln) for ln in offenders)
        + "\n\nFix: remove the dialog-level directive; individual QLineEdit inputs "
        "and the preview label already have their own RTL directives."
    )


def test_target_file_exists():
    """Guard against the host files being accidentally removed.

    Both the genizah_app.py shim host and the desktop/settings_dialogs.py
    extraction target must exist (Phase 126 D1 OR-location).
    """
    missing = [str(t) for t in CANDIDATE_TARGETS if not t.exists()]
    assert not missing, (
        "Phase 126 D1: BOTH the genizah_app.py shim host AND the "
        "desktop/settings_dialogs.py extraction target must exist (move-and-shim). Missing: "
        + " | ".join(missing)
    )
