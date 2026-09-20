# -*- coding: utf-8 -*-
"""Every root alias stub IS the module it names -- and nothing else.

Stage 2 of the repo-structure plan moves modules out of the repository root and leaves a stub at
the old path so ``import old`` and ``from old import X`` keep working in the source tree. The stub
rebinds ``sys.modules[old]`` to the real module, so the two names are ONE object: module-level
state (caches, loggers) is shared and monkeypatching either name patches both. This file pins
that identity per stub, pins the stub shape (docstring, ``import sys``, one from-import, the
rebinding -- no code that could drift), and keeps the table below equal to the rows of
``tests/test_canonical_module_locations.py::CANONICAL`` whose path left the root.

The frozen app never imports a stub once its consumers are rewritten, so the frozen half of the
proof is ``GenizahSearchPro.exe --self-test-imports`` (tests/test_local_pyinstaller_smoke.py).
"""
from __future__ import annotations

import ast
import importlib
import importlib.util
import pathlib
import sys

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# old root module name -> the real dotted module it aliases
ALIASES: dict[str, str] = {
    "column_filter_dialog": "desktop.column_filter_dialog",  # Stage 2 trial, 2026-09-19
}


def _canonical_table() -> dict[str, str]:
    """CANONICAL from tests/test_canonical_module_locations.py, loaded by path (tests/ is not a package)."""
    path = REPO_ROOT / "tests" / "test_canonical_module_locations.py"
    spec = importlib.util.spec_from_file_location("_canonical_module_locations", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return dict(module.CANONICAL)


@pytest.mark.parametrize("old,new", sorted(ALIASES.items()))
def test_alias_is_the_real_module(old, new):
    old_mod = importlib.import_module(old)
    new_mod = importlib.import_module(new)
    assert old_mod is new_mod, f"import {old} gave {old_mod!r}, not the module {new}"
    assert sys.modules[old] is new_mod, f"sys.modules[{old!r}] was not rebound to {new}"
    assert old_mod.__name__ == new
    real_path = (REPO_ROOT / (new.replace(".", "/") + ".py")).resolve()
    assert pathlib.Path(new_mod.__file__).resolve() == real_path


@pytest.mark.parametrize("old,new", sorted(ALIASES.items()))
def test_stub_is_only_an_alias(old, new):
    src = (REPO_ROOT / f"{old}.py").read_text(encoding="utf-8")
    kinds = [type(node).__name__ for node in ast.parse(src).body]
    assert kinds == ["Expr", "Import", "ImportFrom", "Assign"], (
        f"{old}.py must be exactly: docstring, import sys, the from-import, the sys.modules rebinding; "
        f"got {kinds}"
    )
    pkg, _, name = new.rpartition(".")
    assert f"from {pkg} import {name} as _real" in src, f"{old}.py does not alias {new}"
    assert "sys.modules[__name__] = _real" in src, f"{old}.py does not rebind sys.modules[__name__]"


def test_alias_table_matches_canonical():
    """A canonical module that left the root needs a stub row here, and every stub row must name a
    module the canonical table knows at that path."""
    moved = {
        basename[:-3]: path[:-3].replace("/", ".")
        for basename, path in _canonical_table().items()
        if "/" in path
    }
    assert moved == ALIASES, (
        f"CANONICAL says these modules left the root: {moved}; ALIASES says {ALIASES}. "
        "Update both in the same commit."
    )
