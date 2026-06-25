"""GUARD-01: no extracted shared/ module may import genizah_core at module level.

The EXTRACTED_MODULES registry grows each phase of the v8.3.0 decomposition:
  Phase 122: shared/config.py
  Phase 123: (add entries here as modules are extracted)
  ...

Only module-level (import-time) imports are scanned. Lazy imports inside
function/method bodies are intentional and are NOT flagged.

The helper uses a scope-aware traversal: it descends top-level compound
statements (If/Try/With/For/While/Match/ClassDef bodies) because those
execute at import time, but stops at FunctionDef/AsyncFunctionDef bodies
because those are evaluated lazily at call time.

This is intentionally more thorough than a flat ast.iter_child_nodes scan
(which would miss module-level try: from genizah_core import X patterns).
"""
import ast
import os
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# Registry: add one entry per phase as modules are extracted (v8.3.0 decomposition).
# Phase 122: Config only.
# Phase 123: browse_map_utils, text_normalize, variants, responsa, codicological, joins_manager, lists_manager
EXTRACTED_MODULES = [
    "shared/config.py",
    "shared/browse_map_utils.py",
]

# Compound statement types whose bodies run at import time
# (we DO descend into these to catch guarded back-edges like
# try: from genizah_core import X / if cond: import genizah_core)
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


def _has_module_level_genizah_core_import(source: str) -> list[int]:
    """Return line numbers of module-level (import-time) genizah_core imports.

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
                if stmt.module == "genizah_core" or (
                    stmt.module is not None and stmt.module.startswith("genizah_core.")
                ):
                    violations.append(stmt.lineno)
            elif isinstance(stmt, ast.Import):
                for alias in stmt.names:
                    if alias.name == "genizah_core" or alias.name.startswith(
                        "genizah_core."
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


@pytest.mark.parametrize("rel_path", EXTRACTED_MODULES)
def test_no_module_level_genizah_core_import(rel_path):
    """GUARD-01 strict: extracted shared/ module must not import genizah_core at module level."""
    path = REPO_ROOT / rel_path
    assert path.exists(), f"Extracted module {rel_path} not found — was it created?"
    source = path.read_text(encoding="utf-8")
    violations = _has_module_level_genizah_core_import(source)
    assert not violations, (
        f"{rel_path} imports genizah_core at module level on lines {violations}. "
        "GUARD-01 violation: extracted shared/ modules must be import-cycle-free. "
        "Use lazy imports inside method bodies if genizah_core symbols are needed, "
        "or retarget to the shared/ module that owns the symbol."
    )


def test_config_identity():
    """CONFIG-01: genizah_core.Config is the same class object as shared.config.Config."""
    import shared.config
    import genizah_core

    assert shared.config.Config is genizah_core.Config, (
        "genizah_core.Config is not the same object as shared.config.Config. "
        "The re-export shim in genizah_core.py must be: "
        "from shared.config import Config  # noqa: F401"
    )


def test_config_paths_resolve_to_repo_root():
    """Codex BLOCKER #1: Config.BASE_DIR / FILE_V8 / LIBRARIES_CSV resolve to repo root.

    In shared/config.py the non-frozen else-branch MUST use
    dirname(dirname(abspath(__file__))) (climb shared/ -> repo root).
    A verbatim copy would have dirname(abspath(__file__)) pointing at
    shared/ instead, silently breaking FILE_V8/LIBRARIES_CSV paths.
    """
    import shared.config as c

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(c.__file__)))
    assert c.Config.BASE_DIR == repo_root, (
        f"Config.BASE_DIR={c.Config.BASE_DIR!r} does not equal repo_root={repo_root!r}. "
        "Check the non-frozen else-branch in shared/config.py — it must use "
        "os.path.dirname(os.path.dirname(os.path.abspath(__file__)))"
    )
    assert c.Config.FILE_V8 == os.path.join(repo_root, "Transcriptions.txt"), (
        f"Config.FILE_V8={c.Config.FILE_V8!r} does not resolve to repo_root/Transcriptions.txt"
    )
    assert c.Config.LIBRARIES_CSV == os.path.join(repo_root, "libraries.csv"), (
        f"Config.LIBRARIES_CSV={c.Config.LIBRARIES_CSV!r} does not resolve to repo_root/libraries.csv"
    )


def test_guard_catches_top_level_guarded_import():
    """Codex HIGH #2: guard catches a top-level try:-guarded back-edge import.

    A module-level try: from genizah_core import X is an import-time
    back-edge and MUST be flagged, even though it is nested inside a try block.
    """
    source = (
        "import os\n"
        "try:\n"
        "    from genizah_core import Config\n"
        "except ImportError:\n"
        "    Config = None\n"
    )
    violations = _has_module_level_genizah_core_import(source)
    assert violations, (
        "Guard did not catch a module-level try:-guarded 'from genizah_core import Config'. "
        "The scope-aware traversal must descend into ast.Try bodies."
    )


def test_guard_ignores_lazy_function_body_import():
    """Codex HIGH #2: guard does NOT flag a lazy import inside a function body.

    Function-body imports are evaluated lazily at call time, not at import
    time. These are intentional patterns (e.g., shared/local_indexer.py)
    and must not be flagged.
    """
    source = (
        "import os\n"
        "\n"
        "def f():\n"
        "    from genizah_core import Config\n"
        "    return Config\n"
    )
    violations = _has_module_level_genizah_core_import(source)
    assert violations == [], (
        f"Guard incorrectly flagged a function-body lazy import: lines {violations}. "
        "FunctionDef bodies must be excluded from the import-time traversal."
    )


# ---------------------------------------------------------------------------
# Phase 123: browse_map_utils (CORE-06)
# ---------------------------------------------------------------------------

def test_browse_map_utils_identity():
    """CORE-06: genizah_core.normalize_shelfmark is the same object as shared.browse_map_utils.normalize_shelfmark."""
    import shared.browse_map_utils
    import genizah_core

    assert shared.browse_map_utils.normalize_shelfmark is genizah_core.normalize_shelfmark, (
        "genizah_core.normalize_shelfmark is not the same object as "
        "shared.browse_map_utils.normalize_shelfmark. "
        "The re-export shim must be: from shared.browse_map_utils import normalize_shelfmark  # noqa: F401"
    )
    assert shared.browse_map_utils.natural_sort_key is genizah_core.natural_sort_key, (
        "genizah_core.natural_sort_key is not the same object as shared.browse_map_utils.natural_sort_key."
    )
    assert shared.browse_map_utils.get_library_display is genizah_core.get_library_display, (
        "genizah_core.get_library_display is not the same object as shared.browse_map_utils.get_library_display."
    )
    assert shared.browse_map_utils.LIBRARY_CODES is genizah_core.LIBRARY_CODES, (
        "genizah_core.LIBRARY_CODES is not the same object as shared.browse_map_utils.LIBRARY_CODES."
    )
    assert shared.browse_map_utils.dedupe_browse_map is genizah_core.dedupe_browse_map, (
        "genizah_core.dedupe_browse_map is not the same object as shared.browse_map_utils.dedupe_browse_map."
    )


def test_browse_map_utils_standalone_import():
    """CORE-06 smoke: shared.browse_map_utils can be imported and has the key symbols."""
    import shared.browse_map_utils
    assert hasattr(shared.browse_map_utils, 'normalize_shelfmark')
    assert hasattr(shared.browse_map_utils, 'natural_sort_key')
    assert hasattr(shared.browse_map_utils, 'LIBRARY_CODES')
    assert hasattr(shared.browse_map_utils, 'get_library_display')
    assert hasattr(shared.browse_map_utils, 'dedupe_browse_map')
