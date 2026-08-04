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

  1. inside the projection script, it may be CALLED from exactly one function,
     directly OR through any chain of module-level aliases;
  2. across shipping code (`shared/`, `web/`, `desktop/` AND `scripts/`, all but
     the defining file) its name may not appear at all -- so it cannot be
     imported or copied.

Both are AST/text checks over source; neither needs the sidecar.

The first version of this guard proved less than it claimed on both counts: it
matched only direct calls by name, so an alias hop was invisible, and it skipped
`scripts/` entirely -- which is where the shipping projection lives, making that
the largest blind spot of the three trees it did scan (Codex code review
2026-08-03, finding 8). `test_alias_resolution_is_real_not_asserted` is the
positive control for the fix, and it also asserts that the old direct-name-only
matcher MISSES the seeded case, so the control cannot quietly stop
demonstrating the gap it exists to demonstrate.
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

# Trees scanned for the NAME. `scripts/` IS included -- the shipping projection
# lives there, so excluding it left the largest blind spot of all (Codex code
# review 2026-08-03, finding 8). Only the defining file itself is exempt.
# `tests/`, `docs/` and `.planning/` are out of scope: this file names the
# function, and so do the spec and the decision log, deliberately.
SHIPPING_DIRS = ("shared", "web", "desktop", "scripts")


def _module_ast():
    src = PROJECTION_SCRIPT.read_text(encoding="utf-8")
    return ast.parse(src, filename=str(PROJECTION_SCRIPT))


def _module_level_aliases_of(tree, target_name):
    """Names that a module-level assignment binds to `target_name`, transitively.

    `_alias = _vis01_shortcut` followed by `_alias(...)` inside a projection rule
    was invisible to the original direct-name check, so the guard could not prove
    the containment it claimed to prove (Codex finding 8). Resolved to a fixed
    point so a chain of aliases is caught too."""
    aliases = {target_name}
    # A REAL fixed point -- iterate until nothing new is found, with no pass cap.
    #
    # This was `for _ in range(10)`, described in its own comment as "a fixed
    # point". It was not: a chain longer than ten hops, or one written in reverse
    # source order (which resolves one hop per pass), silently stopped early and
    # reported containment it had not proven. The forward-ordered three-hop
    # fixture below could not detect that, because it resolves in a single pass
    # (Codex code review 2A, finding 8).
    #
    # Unbounded is safe here: `aliases` only ever grows, and it is bounded above
    # by the number of module-level assignment targets in one file, so the loop
    # terminates.
    while True:
        grew = False
        for node in ast.iter_child_nodes(tree):
            if not isinstance(node, ast.Assign):
                continue
            if not isinstance(node.value, ast.Name) or node.value.id not in aliases:
                continue
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id not in aliases:
                    aliases.add(target.id)
                    grew = True
        if not grew:
            return aliases


def _enclosing_function_names_of_calls(tree, callee_names):
    """Return the set of function names whose body contains a Call to any name in
    `callee_names`. A call at module level is reported as '<module>'."""
    found = set()

    def walk(node, current_func):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                walk(child, child.name)
                continue
            if isinstance(child, ast.Call):
                fn = child.func
                if isinstance(fn, ast.Name) and fn.id in callee_names:
                    found.add(current_func)
                elif isinstance(fn, ast.Attribute) and fn.attr in callee_names:
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
    names = _module_level_aliases_of(tree, SHORTCUT_NAME)
    callers = _enclosing_function_names_of_calls(tree, names)

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
        if path.resolve() == PROJECTION_SCRIPT.resolve():
            continue  # the defining file, necessarily
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


def test_alias_resolution_is_real_not_asserted(tmp_path):
    """Positive control for the alias resolution itself.

    Without it, `_alias = _vis01_shortcut` followed by `_alias(...)` inside a
    projection rule reads as containment while proving nothing -- which is what
    the first version of this guard did (Codex code review 2026-08-03, finding
    8). Seeded in a throwaway module under tmp_path, never in the tracked tree.
    """
    src = (
        "def _vis01_shortcut(a, b):\n"
        "    return True\n"
        "\n"
        "_alias = _vis01_shortcut\n"
        "_alias2 = _alias\n"
        "\n"
        "def compute_launch_scope_reconciliation(conn):\n"
        "    return _vis01_shortcut(1, 2)\n"
        "\n"
        "def _project_works(ctx):\n"
        "    return _alias2(1, 2)\n"
    )
    tree = ast.parse(src)

    aliases = _module_level_aliases_of(tree, SHORTCUT_NAME)
    assert aliases == {SHORTCUT_NAME, "_alias", "_alias2"}, (
        f"alias chain not resolved to a fixed point: {aliases}"
    )

    callers = _enclosing_function_names_of_calls(tree, aliases)
    assert "_project_works" in callers, (
        "a projection rule calling the stale predicate through a two-step alias "
        "was invisible -- the guard would report containment it has not proven"
    )

    # and the direct-name-only version misses it, which is the point
    direct_only = _enclosing_function_names_of_calls(tree, {SHORTCUT_NAME})
    assert "_project_works" not in direct_only, (
        "this control no longer demonstrates the gap it exists to demonstrate"
    )


def test_alias_resolution_survives_reverse_order_and_a_long_chain():
    """The bound the previous control could not reach (Codex 2A, finding 8).

    `test_alias_resolution_is_real_not_asserted` writes its chain in FORWARD
    source order, so the whole thing resolves in a single pass -- it passes
    identically whether the resolver iterates once, ten times, or to a fixed
    point. It therefore proved alias-following but said nothing about the
    `range(10)` cap sitting in the same function.

    Two shapes it could not express:

      * REVERSE order -- each pass resolves exactly one hop, so N hops need N
        passes. This is the realistic bad case: nothing requires a module to
        define its aliases in dependency order.
      * A chain LONGER than the old cap.

    Both together, so a resolver that silently stops early is caught by name.
    """
    hops = 25  # comfortably past the old range(10)
    # Reverse order: `_a25 = _a24` appears FIRST, `_a1 = _vis01_shortcut` LAST.
    lines = ["def _vis01_shortcut(a, b):", "    return True", ""]
    for i in range(hops, 1, -1):
        lines.append(f"_a{i} = _a{i - 1}")
    lines.append(f"_a1 = {SHORTCUT_NAME}")
    lines.append("")
    lines.append("def _project_rows(ctx):")
    lines.append(f"    return _a{hops}(1, 2)")
    tree = ast.parse("\n".join(lines))

    aliases = _module_level_aliases_of(tree, SHORTCUT_NAME)
    missing = {f"_a{i}" for i in range(1, hops + 1)} - aliases
    assert not missing, (
        f"alias resolution stopped early -- {len(missing)} of {hops} hops "
        f"unresolved: {sorted(missing)[:5]}... A capped loop reports containment "
        f"it has not proven."
    )

    callers = _enclosing_function_names_of_calls(tree, aliases)
    assert "_project_rows" in callers, (
        "a projection rule reaching the stale predicate through a 25-hop "
        "reverse-ordered chain was invisible"
    )

    # And the control is only meaningful if this shape really does need many
    # passes -- otherwise it would pass against a capped resolver too.
    one_pass = {SHORTCUT_NAME}
    for node in ast.iter_child_nodes(tree):
        if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Name)
                and node.value.id in one_pass):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    one_pass.add(target.id)
    assert len(one_pass) < hops, (
        "this fixture resolves in a single pass, so it does not exercise the "
        "iteration bound and proves nothing the forward-ordered control did not"
    )
