"""GUARD-04: no desktop/ module may import genizah_app at MODULE level.

The DESKTOP_MODULES registry covers all 19 desktop/*.py files (18 existing
+ desktop/update_ui.py pre-registered for Phase 127 extraction).

Only module-level (import-time) imports are scanned. Lazy imports inside
function/method bodies are intentional and are NOT flagged (e.g.,
desktop/join_workbench.py:4135 uses a lazy function-body import that
must NOT be flagged — Pitfall 3).

The helper uses a scope-aware traversal: it descends top-level compound
statements (If/Try/With/For/While/Match/ClassDef bodies) because those
execute at import time, but stops at FunctionDef/AsyncFunctionDef bodies
because those are evaluated lazily at call time.

This is intentionally more thorough than a flat ast.iter_child_nodes scan
(which would miss module-level try: from genizah_app import X patterns).
"""
import ast
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# Registry: all desktop/*.py modules (18 existing + 1 pre-registered for Phase 127).
# desktop/update_ui.py is pre-registered here so the guard auto-enforces the
# moment Plan 02 creates the file — no test edit needed.
DESKTOP_MODULES = [
    "desktop/__init__.py",
    "desktop/consent_dialog.py",
    "desktop/dialogs_filter.py",
    "desktop/dialogs_scholarly.py",
    "desktop/file_actions.py",
    "desktop/image_loader.py",
    "desktop/join_workbench.py",
    "desktop/my_library_tab.py",
    "desktop/pdf_image_controller.py",
    "desktop/pdf_page_renderer.py",
    "desktop/puzzle.py",
    "desktop/result_dialog.py",
    "desktop/settings_dialogs.py",
    "desktop/telemetry.py",
    "desktop/title_helpers.py",
    "desktop/ui_widgets.py",
    "desktop/update_ui.py",   # Phase 127 (pre-registered; skip-until-exists guard)
    "desktop/viewers.py",
    "desktop/vs_cache.py",
]

# Compound statement types whose bodies run at import time
# (we DO descend into these to catch guarded back-edges like
# try: from genizah_app import X / if cond: import genizah_app)
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
# Python 3.11+ TryStar (ExceptGroup / try*)
if hasattr(ast, "TryStar"):
    _IMPORT_TIME_COMPOUND = _IMPORT_TIME_COMPOUND + (ast.TryStar,)
# Python 3.10+ Match
if hasattr(ast, "Match"):
    _IMPORT_TIME_COMPOUND = _IMPORT_TIME_COMPOUND + (ast.Match,)

# Function bodies are evaluated lazily — stop here
_LAZY_SCOPE = (ast.FunctionDef, ast.AsyncFunctionDef)


def _collect_stmt_lists(node):
    """Yield every statement list that executes at import time under *node*.

    Recurses into import-time compound statements but NOT into function bodies.
    """
    if isinstance(node, _LAZY_SCOPE):
        return  # lazy scope — do not descend

    # For a Module or compound statement, yield each child statement list
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


def _has_module_level_genizah_app_import(source: str) -> list[int]:
    """Return line numbers of module-level (import-time) genizah_app imports.

    Uses a scope-aware traversal that descends into import-time compound
    statements (If/Try/With/For/While/Match/ClassDef) but NOT into
    FunctionDef/AsyncFunctionDef bodies — lazy imports there are intentional.

    Never uses bare ast.walk (which would descend into function bodies
    and produce false positives for legitimate lazy back-edges).
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    violations = []

    def _visit_stmts(stmts):
        for stmt in stmts:
            # Check if this statement itself is an import
            if isinstance(stmt, ast.ImportFrom):
                if stmt.module == "genizah_app" or (
                    stmt.module is not None and stmt.module.startswith("genizah_app.")
                ):
                    violations.append(stmt.lineno)
            elif isinstance(stmt, ast.Import):
                for alias in stmt.names:
                    if alias.name == "genizah_app" or alias.name.startswith(
                        "genizah_app."
                    ):
                        violations.append(stmt.lineno)
            # Recurse into import-time compound statement bodies
            if isinstance(stmt, _IMPORT_TIME_COMPOUND) and not isinstance(
                stmt, _LAZY_SCOPE
            ):
                for child_stmts in _collect_stmt_lists(stmt):
                    _visit_stmts(child_stmts)

    _visit_stmts(tree.body)
    return violations


@pytest.mark.parametrize("rel_path", DESKTOP_MODULES)
def test_no_module_level_genizah_app_import(rel_path):
    """GUARD-04 strict: desktop/ module must not import genizah_app at module level.

    Modules that are pre-registered in DESKTOP_MODULES but not yet created
    (e.g., desktop/update_ui.py registered in Phase 127 Wave 0 before it is
    created in Wave 1) are skipped with a descriptive message.  Once the file
    exists, the test automatically becomes enforcing with no code change needed.
    """
    path = REPO_ROOT / rel_path
    if not path.exists():
        pytest.skip(
            f"{rel_path} not yet created (pre-registered in Phase 127 Wave 0); "
            "this test will become enforcing automatically once the file exists."
        )
    source = path.read_text(encoding="utf-8")
    violations = _has_module_level_genizah_app_import(source)
    assert not violations, (
        f"{rel_path} imports genizah_app at module level on lines {violations}. "
        "GUARD-04 violation: desktop/ modules must be import-cycle-free. "
        "Use lazy imports inside method bodies if genizah_app symbols are needed, "
        "or retarget to the shared/ module that owns the symbol."
    )


def test_guard_catches_top_level_guarded_import():
    """Codex HIGH #2: guard catches a top-level try:-guarded back-edge import.

    A module-level try: from genizah_app import X is an import-time
    back-edge and MUST be flagged, even though it is nested inside a try block.
    """
    source = (
        "import os\n"
        "try:\n"
        "    from genizah_app import Config\n"
        "except ImportError:\n"
        "    Config = None\n"
    )
    violations = _has_module_level_genizah_app_import(source)
    assert violations, (
        "Guard did not catch a module-level try:-guarded 'from genizah_app import Config'. "
        "The scope-aware traversal must descend into ast.Try bodies."
    )


def test_guard_ignores_lazy_function_body_import():
    """Codex HIGH #2: guard does NOT flag a lazy import inside a function body.

    Function-body imports are evaluated lazily at call time, not at import
    time. These are intentional patterns (e.g., desktop/join_workbench.py:4135)
    and must not be flagged.
    """
    source = (
        "import os\n"
        "\n"
        "def f():\n"
        "    from genizah_app import _build_search_results_xlsx_bytes\n"
        "    return _build_search_results_xlsx_bytes\n"
    )
    violations = _has_module_level_genizah_app_import(source)
    assert violations == [], (
        f"Guard incorrectly flagged a function-body lazy import: lines {violations}. "
        "FunctionDef bodies must be excluded from the import-time traversal."
    )
