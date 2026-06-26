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
# Phase 124: metadata_manager, indexer
EXTRACTED_MODULES = [
    "shared/config.py",
    "shared/browse_map_utils.py",
    "shared/text_normalize.py",
    "shared/variants.py",
    "shared/responsa.py",
    "shared/codicological.py",
    "shared/joins_manager.py",
    "shared/lists_manager.py",
    "shared/metadata_manager.py",
    "shared/indexer.py",             # Phase 124
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


# ---------------------------------------------------------------------------
# Phase 123: text_normalize (CORE-07)
# ---------------------------------------------------------------------------

def test_text_normalize_identity():
    """CORE-07: genizah_core.strip_nikud and strip_search_diacritics are the same objects."""
    import shared.text_normalize
    import genizah_core

    assert shared.text_normalize.strip_nikud is genizah_core.strip_nikud, (
        "genizah_core.strip_nikud is not the same object as shared.text_normalize.strip_nikud. "
        "The re-export shim must be: from shared.text_normalize import strip_nikud  # noqa: F401"
    )
    assert shared.text_normalize.strip_search_diacritics is genizah_core.strip_search_diacritics, (
        "genizah_core.strip_search_diacritics is not the same object as "
        "shared.text_normalize.strip_search_diacritics."
    )
    assert shared.text_normalize.NIKUD_PATTERN is genizah_core.NIKUD_PATTERN, (
        "genizah_core.NIKUD_PATTERN is not the same object as shared.text_normalize.NIKUD_PATTERN."
    )
    assert shared.text_normalize.COMBINING_DIACRITICALS_PATTERN is genizah_core.COMBINING_DIACRITICALS_PATTERN, (
        "genizah_core.COMBINING_DIACRITICALS_PATTERN is not the same object as "
        "shared.text_normalize.COMBINING_DIACRITICALS_PATTERN."
    )


def test_text_normalize_standalone_import():
    """CORE-07 smoke: shared.text_normalize can be imported and has the key symbols."""
    import shared.text_normalize
    assert hasattr(shared.text_normalize, 'strip_nikud')
    assert hasattr(shared.text_normalize, 'strip_search_diacritics')
    assert hasattr(shared.text_normalize, 'NIKUD_PATTERN')
    assert hasattr(shared.text_normalize, 'COMBINING_DIACRITICALS_PATTERN')


# ---------------------------------------------------------------------------
# Phase 123: variants (CORE-08)
# ---------------------------------------------------------------------------

def test_variants_identity():
    """CORE-08: genizah_core.VariantManager is the same class object as shared.variants.VariantManager."""
    import shared.variants
    import genizah_core

    assert shared.variants.VariantManager is genizah_core.VariantManager, (
        "genizah_core.VariantManager is not the same object as shared.variants.VariantManager. "
        "The re-export shim must be: from shared.variants import VariantManager  # noqa: F401"
    )


def test_variants_standalone_import():
    """CORE-08 smoke: shared.variants can be imported and VariantManager instantiates."""
    import shared.variants
    assert hasattr(shared.variants, 'VariantManager')
    # Smoke: instantiate with no settings, call get_variants
    vm = shared.variants.VariantManager(settings=None)
    variants = vm.get_variants("test", "variants")
    assert isinstance(variants, list)
    assert "test" in variants


# ---------------------------------------------------------------------------
# Phase 123: responsa (CORE-01)
# ---------------------------------------------------------------------------

def test_responsa_identity():
    """CORE-01: key responsa symbols are the same objects via genizah_core shim."""
    import shared.responsa
    import genizah_core

    assert shared.responsa.ResponsaComponent is genizah_core.ResponsaComponent, (
        "genizah_core.ResponsaComponent is not the same object as "
        "shared.responsa.ResponsaComponent."
    )
    assert shared.responsa.parse_responsa_query is genizah_core.parse_responsa_query, (
        "genizah_core.parse_responsa_query is not the same object as "
        "shared.responsa.parse_responsa_query."
    )
    assert shared.responsa._apply_explosion_guard is genizah_core._apply_explosion_guard, (
        "genizah_core._apply_explosion_guard is not the same object as "
        "shared.responsa._apply_explosion_guard."
    )
    assert shared.responsa._count_expanded_terms is genizah_core._count_expanded_terms, (
        "genizah_core._count_expanded_terms is not the same object as "
        "shared.responsa._count_expanded_terms."
    )
    assert shared.responsa.GRAMMATICAL_PREFIXES is genizah_core.GRAMMATICAL_PREFIXES, (
        "genizah_core.GRAMMATICAL_PREFIXES is not the same object as "
        "shared.responsa.GRAMMATICAL_PREFIXES."
    )


def test_responsa_standalone_import():
    """CORE-01 smoke: shared.responsa can be imported and parse_responsa_query works."""
    import shared.responsa
    assert hasattr(shared.responsa, 'ResponsaComponent')
    assert hasattr(shared.responsa, 'parse_responsa_query')
    assert hasattr(shared.responsa, '_apply_explosion_guard')
    assert hasattr(shared.responsa, 'GRAMMATICAL_PREFIXES')
    assert hasattr(shared.responsa, 'GRAMMATICAL_SUFFIXES')
    assert hasattr(shared.responsa, '_SOFIT_TO_NORMAL')
    assert hasattr(shared.responsa, 'LineGroup')
    assert hasattr(shared.responsa, '_parse_line_break_query')
    # Smoke: parse a simple query
    result = shared.responsa.parse_responsa_query("word1 word2")
    assert isinstance(result, list)
    assert len(result) == 2


# ---------------------------------------------------------------------------
# Phase 123: codicological (CORE-03)
# ---------------------------------------------------------------------------

def test_codicological_identity():
    """CORE-03: genizah_core.CodicologicalManager is the same class as shared.codicological.CodicologicalManager."""
    import shared.codicological
    import genizah_core

    assert shared.codicological.CodicologicalManager is genizah_core.CodicologicalManager, (
        "genizah_core.CodicologicalManager is not the same object as "
        "shared.codicological.CodicologicalManager."
    )


def test_codicological_standalone_import():
    """CORE-03 smoke: shared.codicological can be imported and CodicologicalManager instantiates."""
    import shared.codicological
    assert hasattr(shared.codicological, 'CodicologicalManager')
    # Smoke: instantiate with no arguments
    mgr = shared.codicological.CodicologicalManager()
    assert mgr is not None


# ---------------------------------------------------------------------------
# Phase 123: joins_manager (CORE-04)
# ---------------------------------------------------------------------------

def test_joins_manager_identity():
    """CORE-04: genizah_core.JoinsManager is the same class as shared.joins_manager.JoinsManager."""
    import shared.joins_manager
    import genizah_core

    assert shared.joins_manager.JoinsManager is genizah_core.JoinsManager, (
        "genizah_core.JoinsManager is not the same object as "
        "shared.joins_manager.JoinsManager."
    )


def test_joins_manager_standalone_import():
    """CORE-04 smoke: shared.joins_manager can be imported and JoinsManager has JOINS_FILE."""
    import shared.joins_manager
    assert hasattr(shared.joins_manager, 'JoinsManager')
    # Smoke: JOINS_FILE class attribute is set (Pitfall 3 guard — Config must be importable)
    assert hasattr(shared.joins_manager.JoinsManager, 'JOINS_FILE')
    assert 'joins_cache.pkl' in shared.joins_manager.JoinsManager.JOINS_FILE


# ---------------------------------------------------------------------------
# Phase 123: lists_manager (CORE-05)
# ---------------------------------------------------------------------------

def test_lists_manager_identity():
    """CORE-05: genizah_core.ListsManager is the same class as shared.lists_manager.ListsManager."""
    import shared.lists_manager
    import genizah_core

    assert shared.lists_manager.ListsManager is genizah_core.ListsManager, (
        "genizah_core.ListsManager is not the same object as "
        "shared.lists_manager.ListsManager."
    )


def test_lists_manager_standalone_import():
    """CORE-05 smoke: shared.lists_manager can be imported and ListsManager instantiates."""
    import shared.lists_manager
    assert hasattr(shared.lists_manager, 'ListsManager')
    # Smoke: _tr helper is present (replacing tr() calls in methods)
    assert hasattr(shared.lists_manager, '_tr')
    # Smoke: instantiate with no arguments
    mgr = shared.lists_manager.ListsManager()
    assert mgr is not None


# ---------------------------------------------------------------------------
# Phase 124: metadata_manager (CORE-09)
# ---------------------------------------------------------------------------

def test_metadata_manager_identity():
    """CORE-09: genizah_core.MetadataManager is the same class as shared.metadata_manager.MetadataManager."""
    import shared.metadata_manager
    import genizah_core

    assert shared.metadata_manager.MetadataManager is genizah_core.MetadataManager, (
        "genizah_core.MetadataManager is not the same object as "
        "shared.metadata_manager.MetadataManager. "
        "The re-export shim must be: from shared.metadata_manager import MetadataManager  # noqa: F401"
    )
    assert shared.metadata_manager._BoundedLRUCache is genizah_core._BoundedLRUCache, (
        "genizah_core._BoundedLRUCache is not the same object as "
        "shared.metadata_manager._BoundedLRUCache."
    )
    assert shared.metadata_manager.MARC_FUTURE_TIMEOUT is genizah_core.MARC_FUTURE_TIMEOUT, (
        "genizah_core.MARC_FUTURE_TIMEOUT is not the same object as "
        "shared.metadata_manager.MARC_FUTURE_TIMEOUT."
    )
    assert shared.metadata_manager._NLI_CACHE_MAX_ENTRIES is genizah_core._NLI_CACHE_MAX_ENTRIES, (
        "genizah_core._NLI_CACHE_MAX_ENTRIES is not the same object as "
        "shared.metadata_manager._NLI_CACHE_MAX_ENTRIES."
    )


def test_metadata_manager_standalone_import():
    """CORE-09 smoke: shared.metadata_manager can be imported and MetadataManager has expected API."""
    import shared.metadata_manager
    assert hasattr(shared.metadata_manager, 'MetadataManager')
    assert hasattr(shared.metadata_manager, '_BoundedLRUCache')
    assert hasattr(shared.metadata_manager, '_NLI_CACHE_MAX_ENTRIES')
    assert hasattr(shared.metadata_manager, 'MARC_FUTURE_TIMEOUT')
    assert hasattr(shared.metadata_manager, 'NLI_IIIF_FUTURE_TIMEOUT')
    assert hasattr(shared.metadata_manager, 'EXTERNAL_IIIF_HTTP_TIMEOUT')
    # Smoke: _BoundedLRUCache can be instantiated and basic dict-like ops work
    cache = shared.metadata_manager._BoundedLRUCache(maxsize=5)
    cache['key1'] = 'val1'
    assert 'key1' in cache
    assert cache['key1'] == 'val1'
    assert len(cache) == 1


# ---------------------------------------------------------------------------
# Phase 124: indexer (CORE-10)
# ---------------------------------------------------------------------------

def test_indexer_identity():
    """CORE-10: genizah_core.Indexer is the same class as shared.indexer.Indexer."""
    import shared.indexer
    import genizah_core

    assert shared.indexer.Indexer is genizah_core.Indexer, (
        "genizah_core.Indexer is not the same object as shared.indexer.Indexer. "
        "The re-export shim must be: from shared.indexer import Indexer  # noqa: F401"
    )


def test_indexer_standalone_import():
    """CORE-10 smoke: shared.indexer imports and Indexer instantiates with a mock meta_mgr."""
    import shared.indexer
    assert hasattr(shared.indexer, 'Indexer')

    class _FakeMM:
        pass

    idx = shared.indexer.Indexer(_FakeMM())
    assert idx.meta_mgr is not None
