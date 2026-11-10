# -*- coding: utf-8 -*-
"""Records that live in the private repository, for the tests that check against them.

Planning records, decision logs and operations guides are kept in a separate
private repository, checked out BESIDE this one (never inside it), with the same
relative paths they had here. A test that compares code with one of those
records runs where that checkout is available and skips everywhere else (for
example in public CI), saying which record it needs.

Where the private checkout is found is decided in ONE place,
``scripts/private_repo.py`` (shared with the scripts):
  1. GENIZAH_PRIVATE_DIR, if set;
  2. otherwise the folder ``GenizahSearch-private`` next to the MAIN checkout
     (also when the test runs inside a worktree under .claude/worktrees/).

Call ``private_path`` (or ``private_module``) inside a test, a fixture or a helper
they call, never at import time.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Optional

import pytest

from scripts.private_repo import planning_dir, private_dir

_PLANNING = ".planning/"


def private_root() -> Optional[Path]:
    """The private checkout, or None when this machine does not have one."""
    root = private_dir()
    return root if root.is_dir() else None


def _resolve(rel: str) -> Path:
    if rel.startswith(_PLANNING):
        return planning_dir() / rel[len(_PLANNING):]
    return private_dir() / rel


def private_path(*candidates: str) -> Path:
    """The first of ``candidates`` (paths relative to the private checkout) that
    exists; skips the calling test when none does."""
    for rel in candidates:
        path = _resolve(rel)
        if path.exists():
            return path
    pytest.skip("needs a record from the private repository (set GENIZAH_PRIVATE_DIR): "
                + " or ".join(candidates))


def private_module(rel: str) -> ModuleType:
    """Import a Python file kept in the private repository (``rel`` is its path
    relative to the private checkout, e.g. ``tests/some_fixture.py``); skips the
    calling test when it is not there. The module is imported once per process,
    under a name of its own, so it never shadows a public module."""
    path = private_path(rel)
    name = "_private_" + rel.replace("/", "_").replace(".", "_")
    module = sys.modules.get(name)
    if module is not None and getattr(module, "__file__", None) == str(path):
        return module
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return module

