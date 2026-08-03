# -*- coding: utf-8 -*-
"""Containment guard for the superseded VIS-01 launch-scope shortcut.

`scripts/project_discovery_public.py::_vis01_shortcut` is a STALE rule kept
alive on purpose: `compute_launch_scope_reconciliation` differences it against
the real rule so the reconciliation report can state HOW the two disagree.

It is unsafe as a visibility rule -- its first branch returns True for EVERY
`propagated` row regardless of `source_corpus`, so a propagated row carrying
restricted (M-source / R-source) content would be admitted. The ONE rule that
decides publication is the two-axis conjunction
`shared.discovery_visibility.is_public(assertion_visibility,
identity_visibility)`.

The hazard this guard closes is a READING hazard, not a current defect: a
plausibly-named boolean helper sitting a few hundred lines above the real
projection rules invites a future edit to reach for it. Two pins:

  1. inside the projection script, it may be CALLED from exactly one function;
  2. across shipping code (`shared/`, `web/`, `desktop/`, repo-root modules),
     its name may not appear at all -- so it cannot be imported or copied.

Both are AST/text checks over source; neither needs the sidecar.
"""
import ast
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
PROJECTION_SCRIPT = REPO_ROOT / "scripts" / "project_discovery_public.py"

SHORTCUT_NAME = "_vis01_shortcut"

# The ONLY function permitted to call it. This is a reporting routine: it
# reports the symmetric difference between the stale rule and the real one.
SOLE_PERMITTED_CALLER = "compute_launch_scope_reconciliation"

# Shipping trees. `scripts/` is excluded (that is where the function lives),
# as are `tests/`, `docs/` and `.planning/` (this file names it; so do the
# spec and the decision log, deliberately).
SHIPPING_DIRS = ("shared", "web", "desktop")


def _module_ast():
    src = PROJECTION_SCRIPT.read_text(encoding="utf-8")
    return ast.parse(src, filename=str(PROJECTION_SCRIPT))


def _enclosing_function_names_of_calls(tree, callee_name):
    """Return the set of function names whose body contains a Call to
    `callee_name`. A call at module level is reported as '<module>'."""
    found = set()

    def walk(node, current_func):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                walk(child, child.name)
                continue
            if isinstance(child, ast.Call):
                fn = child.func
                if isinstance(fn, ast.Name) and fn.id == callee_name:
                    found.add(current_func)
                elif isinstance(fn, ast.Attribute) and fn.attr == callee_name:
                    found.add(current_func)
            walk(child, current_func)

    walk(tree, "<module>")
    return found


def test_shortcut_still_exists_so_this_guard_is_not_vacuous():
    """If the function is ever deleted outright, this guard must fail loudly
    rather than pass by finding nothing -- a guard that cannot reach its own
    subject reads as coverage while proving nothing."""
    tree = _module_ast()
    names = {
        n.name for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert SHORTCUT_NAME in names, (
        f"{SHORTCUT_NAME} is gone from {PROJECTION_SCRIPT.name}. That is a fine "
        "outcome -- but delete this guard file in the same commit, so nobody is "
        "left with a green test that checks nothing."
    )
    assert SOLE_PERMITTED_CALLER in names, (
        f"{SOLE_PERMITTED_CALLER} is gone; re-point or retire this guard."
    )


def test_shortcut_is_called_from_the_reconciliation_reporter_only():
    tree = _module_ast()
    callers = _enclosing_function_names_of_calls(tree, SHORTCUT_NAME)

    assert callers, (
        f"{SHORTCUT_NAME} is defined but never called. If it is now dead, remove "
        "it -- a stale, unsafe visibility rule must not sit in the projection "
        "script waiting to be picked up."
    )
    unexpected = callers - {SOLE_PERMITTED_CALLER}
    assert not unexpected, (
        f"{SHORTCUT_NAME} is the SUPERSEDED VIS-01 shortcut and is unsafe as a "
        f"visibility rule (it admits every `propagated` row regardless of "
        f"source_corpus, including restricted corpora). It may only be called "
        f"from {SOLE_PERMITTED_CALLER}, which differences it against the real "
        f"rule for reporting. Found call(s) in: {sorted(unexpected)}. "
        f"Publication decisions go through "
        f"shared.discovery_visibility.is_public(assertion_visibility, "
        f"identity_visibility)."
    )


@pytest.mark.parametrize("tree_name", SHIPPING_DIRS)
def test_shortcut_name_absent_from_shipping_code(tree_name):
    root = REPO_ROOT / tree_name
    if not root.is_dir():
        pytest.skip(f"{tree_name}/ not present")
    offenders = []
    for path in root.rglob("*.py"):
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        if SHORTCUT_NAME in text:
            offenders.append(str(path.relative_to(REPO_ROOT)))
    assert not offenders, (
        f"{SHORTCUT_NAME} must not appear in shipping code -- it is a stale, "
        f"unsafe visibility rule retained only as a reporting input inside "
        f"{PROJECTION_SCRIPT.name}. Found in: {offenders}"
    )
