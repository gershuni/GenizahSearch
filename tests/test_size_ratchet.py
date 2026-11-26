# -*- coding: utf-8 -*-
"""The largest units of code may shrink but not grow.

Extraction has cut `genizah_app.py` by a third before, and with nothing to stop it the file
grew back past its old size: new desktop features land as methods on `GenizahGUI`, which holds
the state, and new web features as closures inside a page factory. `genizah_core.py` shows the
other way. Given a fixed role and tests that guard it, it was cut to about 750 lines and stayed
there.

So each unit below has a ceiling, its size in lines when this test was written or last lowered:

- It may not grow. New logic goes in a module of its own with its own state, the way
  `web/pages/browse_desk_document.py` and `shared/reading_desk_model.py` were added, and only
  the call that wires it in goes here.
- If that wiring alone needs the lines, raise the ceiling in this file, in the same PR, and
  say why in the PR description. A raise is then a visible decision in review, not drift.
- When a unit shrinks by more than SLACK lines, the test fails until the ceiling is lowered to
  the new size, so the space cannot quietly fill again.

Sizes are lines as Python sees them: the whole file, or one top-level class or function from
its first line (decorators excluded) to its last.
"""
from __future__ import annotations

import ast
import os

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# (file, top-level class or function, or None for the whole file) -> ceiling in lines
CEILINGS = {
    ("genizah_app.py", None): 39660,
    ("genizah_app.py", "GenizahGUI"): 37549,
    ("web/pages/search.py", "create_search_page"): 7248,
    ("web/pages/parallels.py", "create_parallels_page"): 6505,
    ("web/pages/browse.py", "create_browse_page"): 5663,
    ("web/pages/joins_lab.py", "create_joins_lab_page"): 4377,
    ("web/pages/puzzle.py", "create_puzzle_page"): 2681,
}

# How far below its ceiling a unit may fall before the ceiling must be lowered.
SLACK = 100


def _size(path, unit):
    with open(os.path.join(REPO, path), encoding="utf-8-sig") as fh:
        source = fh.read()
    if unit is None:
        return len(source.splitlines())
    for node in ast.parse(source).body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == unit:
            return node.end_lineno - node.lineno + 1
    raise AssertionError(
        f"{path} has no top-level {unit!r}. If it was renamed or split up, update CEILINGS in "
        f"{os.path.basename(__file__)} to name what is there now."
    )


@pytest.mark.parametrize("path,unit", sorted(CEILINGS, key=str), ids=lambda v: str(v))
def test_the_unit_does_not_grow_and_its_ceiling_follows_it_down(path, unit):
    ceiling = CEILINGS[(path, unit)]
    label = path if unit is None else f"{path}::{unit}"
    size = _size(path, unit)
    assert size <= ceiling, (
        f"{label} is {size} lines, {size - ceiling} over its ceiling of {ceiling}. Put the new "
        f"logic in a module of its own, with its own state, and leave only the call that wires "
        f"it in here. If that call itself needs the lines, raise the ceiling in "
        f"{os.path.basename(__file__)} in the same PR and say why in the PR description."
    )
    assert size >= ceiling - SLACK, (
        f"{label} shrank to {size} lines, {ceiling - size} under its ceiling of {ceiling}. Lower "
        f"the ceiling in {os.path.basename(__file__)} to {size}, so the space cannot fill again."
    )
