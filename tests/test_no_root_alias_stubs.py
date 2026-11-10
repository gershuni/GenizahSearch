# -*- coding: utf-8 -*-
"""The modules that left the repository root are named by their real dotted paths -- and the root
has no compatibility stub left to fall back on.

Round 1 Stage 2 moved eleven modules into ``desktop/`` and ``shared/`` and left a four-line alias
stub at each old path so ``import gui_threads`` kept working while the consumers were rewritten.
Round 2 deleted those stubs, and moved a twelfth module -- ``genizah_translations`` -- with no stub
at all, because its consumers were rewritten in the move commit itself. This file is the successor
to ``tests/test_root_alias_stubs.py``: where that one proved each stub WAS the real module, this one
proves no root file by any of these names exists and that nothing reaches for one.

Why a static sweep and not "the import would just fail": three of the rewritten import sites sit
inside a handler that swallows the failure, so a regression there is silent.
``desktop/join_workbench.py:600`` imports ``desktop.gui_threads`` under ``except ImportError``, and
a failure sets ``_QT_AVAILABLE = False``, which deletes the whole Join Workbench UI;
``desktop/corrections_client.py:1614`` imports ``desktop.supabase_corrections_client`` the same way
and downgrades the app to the REST client; ``web/export_service.py::_localize_search_mode`` imports
``shared.genizah_translations`` under ``except Exception`` and returns the ENGLISH label in a Hebrew
export. None of the three raises at run time, and all three pass ``--self-test-imports``, which only
proves the module itself is importable. Only a check over every tracked file sees them.
"""
from __future__ import annotations

import ast
import importlib
import importlib.util
import pathlib
import subprocess
import sys

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# old root module name -> the real dotted module that replaced it. Equal, by the last test here,
# to the rows of tests/test_canonical_module_locations.py::CANONICAL whose path left the root.
MOVED: dict[str, str] = {
    "column_filter_dialog": "desktop.column_filter_dialog",
    "corrections_client": "desktop.corrections_client",
    "corrections_ui": "desktop.corrections_ui",
    "filter_text_dialog": "desktop.filter_text_dialog",
    "gui_threads": "desktop.gui_threads",
    "list_filter_dialog": "desktop.list_filter_dialog",
    "supabase_corrections_client": "desktop.supabase_corrections_client",
    # Round 2, 2026-09-21: moved with no stub, so the sweep below is its only static guard.
    "genizah_translations": "shared.genizah_translations",
    "lists_sync": "shared.lists_sync",
    "pgp_tag_translations": "shared.pgp_tag_translations",
    "sefaria_utils": "shared.sefaria_utils",
    "unified_variants": "shared.unified_variants",
}


def _canonical_table() -> dict[str, str]:
    """CANONICAL from tests/test_canonical_module_locations.py, loaded by path (tests/ is not a package)."""
    path = REPO_ROOT / "tests" / "test_canonical_module_locations.py"
    spec = importlib.util.spec_from_file_location("_canonical_module_locations", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return dict(module.CANONICAL)


def _tracked_files() -> set[str]:
    out = subprocess.run(
        ["git", "ls-files", "-z"], cwd=REPO_ROOT, capture_output=True, check=True,
    ).stdout.decode("utf-8", "surrogateescape")
    return {p for p in out.split("\0") if p}


@pytest.mark.parametrize("old", sorted(MOVED))
def test_no_stub_remains_at_the_old_root_path(old):
    path = REPO_ROOT / f"{old}.py"
    assert not path.exists(), (
        f"{old}.py is back at the repository root. The module lives at "
        f"{MOVED[old].replace('.', '/')}.py; a root file by that name would shadow nothing but "
        "would resurrect the two-names-for-one-module ambiguity Round 2 removed."
    )
    assert f"{old}.py" not in _tracked_files()


@pytest.mark.parametrize("old,new", sorted(MOVED.items()))
def test_the_real_module_imports_and_sits_where_the_table_says(old, new):
    module = importlib.import_module(new)
    expected = (REPO_ROOT / (new.replace(".", "/") + ".py")).resolve()
    assert pathlib.Path(module.__file__).resolve() == expected


@pytest.mark.parametrize("old", sorted(MOVED))
def test_the_old_bare_name_no_longer_resolves(old):
    """The point of the deletion: the old name is not a second route to the module.

    ``tests/conftest.py`` puts the repository root on ``sys.path`` and nothing else, so this fails
    the moment a stub (or any other file) reappears at the old path.
    """
    sys.modules.pop(old, None)
    assert importlib.util.find_spec(old) is None, (
        f"`import {old}` still resolves; use `{MOVED[old]}`"
    )


def _tracked_python_files() -> list:
    out = subprocess.run(
        ["git", "ls-files", "-z", "*.py"], cwd=REPO_ROOT, capture_output=True, check=True,
    ).stdout.decode("utf-8", "surrogateescape")
    return [p for p in out.split("\0") if p.endswith(".py")]


def _imports_of(source: str, name: str) -> list:
    """Line numbers where ``source`` imports the bare module ``name`` (any nesting)."""
    hits = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            hits += [node.lineno for a in node.names if a.name == name]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module == name:
            hits.append(node.lineno)
    return hits


def test_no_tracked_file_imports_a_moved_module_by_its_old_name():
    offenders = []
    for rel in _tracked_python_files():
        source = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="replace")
        for old in MOVED:
            if old not in source:
                continue
            for lineno in _imports_of(source, old):
                offenders.append(f"{rel}:{lineno} imports `{old}` (use `{MOVED[old]}`)")
    assert not offenders, (
        "these files import a module by a root name that no longer exists:\n  "
        + "\n  ".join(sorted(offenders))
    )


def test_moved_table_matches_canonical():
    """A canonical module whose path left the root needs a row here, and every row here must name a
    module the canonical table knows at that path."""
    moved = {
        basename[:-3]: path[:-3].replace("/", ".")
        for basename, path in _canonical_table().items()
        if "/" in path
    }
    assert moved == MOVED, (
        f"CANONICAL says these modules left the root: {moved}; MOVED says {MOVED}. "
        "Update both in the same commit."
    )
