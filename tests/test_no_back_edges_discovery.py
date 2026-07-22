# -*- coding: utf-8 -*-
"""NEW essential back-edge guard (Phase 134, 134-06, F6): the Discovery
service layer must never import web/nicegui/fastapi at module level.

Unlike ``tests/test_no_back_edges_core.py`` -- which only bans module-level
``genizah_core`` imports for a REGISTRY of extracted ``shared/`` modules --
this is a genuinely NEW test: it bans an entirely different target-module
set (``web``, ``web.*``, ``nicegui``, ``nicegui.*``, ``fastapi``,
``fastapi.*``) over ``shared/discovery_service.py`` and
``shared/discovery_errors.py`` specifically. It reuses the core guard's
scope-aware AST traversal SHAPE only (descend import-time compound
statements -- If/Try/With/For/While/Match/ClassDef -- but stop at
FunctionDef/AsyncFunctionDef bodies, since those are evaluated lazily at
call time, not at import time).

Only module-level (import-time) imports are scanned, INCLUDING a top-level
``try:``-guarded back-edge import (a module-level ``try: import web`` is an
import-time statement regardless of the surrounding try/except). Lazy
imports inside function/method bodies are intentional and are NOT flagged.
"""
import ast
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# The Discovery service-layer modules this guard protects (134-06 files_modified).
TARGET_MODULES = [
    "shared/discovery_service.py",
    "shared/discovery_errors.py",
]

# Banned import targets: a bare module name OR any dotted submodule of it.
_BANNED_PREFIXES = ("web", "nicegui", "fastapi")

# Compound statement types whose bodies run at import time (we DO descend
# into these to catch guarded back-edges like try: import web / if cond:
# from web.x import y) -- mirrors tests/test_no_back_edges_core.py exactly.
_IMPORT_TIME_COMPOUND = (
    ast.If,
    ast.For,
    ast.AsyncFor,
    ast.While,
    ast.With,
    ast.AsyncWith,
    ast.Try,
    ast.ClassDef,
)
if hasattr(ast, "TryStar"):  # Python 3.11+ ExceptGroup / try*
    _IMPORT_TIME_COMPOUND = _IMPORT_TIME_COMPOUND + (ast.TryStar,)
if hasattr(ast, "Match"):  # Python 3.10+
    _IMPORT_TIME_COMPOUND = _IMPORT_TIME_COMPOUND + (ast.Match,)

# Function bodies are evaluated lazily -- stop here.
_LAZY_SCOPE = (ast.FunctionDef, ast.AsyncFunctionDef)


def _is_banned(module_name):
    if not module_name:
        return False
    return any(module_name == prefix or module_name.startswith(prefix + ".") for prefix in _BANNED_PREFIXES)


def _collect_stmt_lists(node):
    """Yield every statement list that executes at import time under *node*.

    Recurses into import-time compound statements but NOT into function
    bodies -- identical shape to test_no_back_edges_core.py's own helper.
    """
    if isinstance(node, _LAZY_SCOPE):
        return  # lazy scope -- do not descend

    if isinstance(node, ast.Module):
        yield node.body
    elif isinstance(node, ast.If):
        yield node.body
        yield node.orelse
    elif isinstance(node, (ast.For, ast.AsyncFor, ast.While)):
        yield node.body
        yield node.orelse
    elif isinstance(node, (ast.With, ast.AsyncWith)):
        yield node.body
    elif isinstance(node, ast.Try):
        yield node.body
        for handler in node.handlers:
            yield handler.body
        yield node.orelse
        yield node.finalbody
    elif hasattr(ast, "TryStar") and isinstance(node, ast.TryStar):
        yield node.body
        for handler in node.handlers:
            yield handler.body
        yield node.orelse
        yield node.finalbody
    elif hasattr(ast, "Match") and isinstance(node, ast.Match):
        for case in node.cases:
            yield case.body
    elif isinstance(node, ast.ClassDef):
        yield node.body


def _find_banned_imports(source: str) -> list[int]:
    """Return line numbers of module-level (import-time) imports of any
    banned module (web/nicegui/fastapi, or a dotted submodule of one).

    Uses the same scope-aware traversal as test_no_back_edges_core.py:
    descends import-time compound statements but never FunctionDef/
    AsyncFunctionDef bodies -- so a lazy function-body import is never
    flagged, but a module-level try:/if: guarded import IS.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    violations: list[int] = []

    def _visit_stmts(stmts):
        for stmt in stmts:
            if isinstance(stmt, ast.ImportFrom):
                if _is_banned(stmt.module):
                    violations.append(stmt.lineno)
            elif isinstance(stmt, ast.Import):
                for alias in stmt.names:
                    if _is_banned(alias.name):
                        violations.append(stmt.lineno)
            if isinstance(stmt, _IMPORT_TIME_COMPOUND) and not isinstance(stmt, _LAZY_SCOPE):
                for child_stmts in _collect_stmt_lists(stmt):
                    _visit_stmts(child_stmts)

    _visit_stmts(tree.body)
    return violations


@pytest.mark.parametrize("rel_path", TARGET_MODULES)
def test_no_module_level_web_nicegui_fastapi_import(rel_path):
    """DATA-06/T-134-layer: shared/discovery_service.py and
    shared/discovery_errors.py must never import web/nicegui/fastapi at
    module level (Import + ImportFrom + guarded top-level)."""
    path = REPO_ROOT / rel_path
    assert path.exists(), f"{rel_path} does not exist"
    source = path.read_text(encoding="utf-8")
    violations = _find_banned_imports(source)
    assert not violations, (
        f"{rel_path} imports a banned module (web/nicegui/fastapi) at module level "
        f"on lines {violations}. shared/ modules must stay web-free (back-edge guard)."
    )


def test_guard_catches_top_level_guarded_import():
    """A module-level try:-guarded 'import web' is an import-time back-edge
    and MUST be flagged, even nested inside a try block."""
    source = (
        "import os\n"
        "try:\n"
        "    import web\n"
        "except ImportError:\n"
        "    web = None\n"
    )
    violations = _find_banned_imports(source)
    assert violations, "Guard did not catch a module-level try:-guarded 'import web'."


def test_guard_catches_top_level_guarded_import_from():
    """A module-level try:-guarded 'from web.discovery_assets import x' is
    also an import-time back-edge and MUST be flagged."""
    source = (
        "import os\n"
        "try:\n"
        "    from web.discovery_assets import discovery_available\n"
        "except ImportError:\n"
        "    discovery_available = None\n"
    )
    violations = _find_banned_imports(source)
    assert violations, "Guard did not catch a module-level try:-guarded 'from web.x import y'."


def test_guard_ignores_lazy_function_body_import():
    """A lazy import inside a function body is evaluated at CALL time, not
    import time -- it must NOT be flagged."""
    source = (
        "import os\n"
        "\n"
        "def f():\n"
        "    from web.discovery_assets import discovery_available\n"
        "    return discovery_available\n"
    )
    violations = _find_banned_imports(source)
    assert violations == [], (
        f"Guard incorrectly flagged a function-body lazy import: lines {violations}. "
        "FunctionDef bodies must be excluded from the import-time traversal."
    )


def test_guard_flags_nicegui_and_fastapi_too():
    """The guard bans nicegui and fastapi at module level, not just web."""
    for mod in ("nicegui", "fastapi"):
        source = f"import {mod}\n"
        violations = _find_banned_imports(source)
        assert violations, f"Guard did not flag a bare 'import {mod}' at module level."


def test_guard_does_not_false_positive_on_unrelated_module_named_similarly():
    """A module name that merely STARTS WITH one of the banned words as a
    substring but is not actually that module or a submodule of it (e.g.
    'webbrowser') must NOT be flagged -- the guard checks an exact name or a
    dotted-submodule prefix, never a bare substring."""
    source = "import webbrowser\n"
    violations = _find_banned_imports(source)
    assert violations == [], (
        f"Guard incorrectly flagged 'webbrowser' as a 'web' back-edge: {violations}"
    )
