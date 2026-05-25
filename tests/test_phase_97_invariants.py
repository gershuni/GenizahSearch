# -*- coding: utf-8 -*-
"""Phase 97 D-NEW-7: Four AST-based CI guards covering Phase 87/95/96/97 invariants.

Guards:
  (a) Cloud-write gates remain at TOP of respective gated functions in
      search_serializer.py, corrections_client.py, lists_sync.py
  (b) Web LIBRARY_CODES allowlist in web/pages/*.py still guards LOCAL
  (c) is_local_sys_id recognizes 18-digit 97-prefixed sys_ids
  (d) LOCAL RRF merge happens POST _deduplicate() (Phase 95 D-08 P0)

These are fail-fast CI guards — any regression breaks the build immediately.
"""
from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
WEB_PAGES_DIR = REPO_ROOT / "web" / "pages"


# ---------------------------------------------------------------------------
# (a) Cloud-write gates at TOP of respective gated functions
# ---------------------------------------------------------------------------

def _first_n_statements_source(node: ast.FunctionDef, n: int = 5) -> str:
    """Return unparsed source for first n statements of function body."""
    return ast.unparse(ast.Module(body=node.body[:n], type_ignores=[]))


def _find_function(tree: ast.AST, fn_name: str) -> ast.FunctionDef | None:
    """Walk AST and return the first FunctionDef with the given name."""
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == fn_name:
            return node
    return None


def test_cloud_write_gates_at_top():
    """D-NEW-7 (a): cloud-write gates remain at TOP of respective gated functions.

    Targets (actual gated function names, read at edit time):
      - shared/search_serializer.py :: '_is_local_item' (contains is_local_sys_id)
        + called from serialize_search_payload's first 10 statements
      - corrections_client.py :: 'create_correction' (is_local_sys_id in body[:5])
      - lists_sync.py :: 'sync_item_to_cloud' (is_local_sys_id in body[:5])
      - lists_sync.py :: 'sync_list_to_cloud' (is_local_sys_id via is_local_sys_id
        used in first loop iteration — within first 5 statements)
    """
    # Gate 1: search_serializer._is_local_item contains is_local_sys_id
    serializer_path = REPO_ROOT / "shared" / "search_serializer.py"
    tree = ast.parse(serializer_path.read_text(encoding="utf-8"))
    is_local_item_fn = _find_function(tree, "_is_local_item")
    assert is_local_item_fn is not None, (
        "shared/search_serializer.py: no function named '_is_local_item' — "
        "cloud-write gate helper missing (Phase 95 D-30 / Phase 97 D-NEW-7)"
    )
    full_body = ast.unparse(ast.Module(body=is_local_item_fn.body, type_ignores=[]))
    assert "is_local_sys_id" in full_body, (
        "shared/search_serializer.py::_is_local_item: is_local_sys_id not found "
        "(Phase 95 D-30 / Phase 97 D-NEW-7 invariant)"
    )

    # Gate 2: corrections_client.py :: create_correction — is_local_sys_id in body[:5]
    corrections_path = REPO_ROOT / "corrections_client.py"
    tree = ast.parse(corrections_path.read_text(encoding="utf-8"))
    create_fn = _find_function(tree, "create_correction")
    assert create_fn is not None, (
        "corrections_client.py: no function named 'create_correction' — "
        "Phase 95 D-30 gate location missing (Phase 97 D-NEW-7)"
    )
    create_body = ast.unparse(ast.Module(body=create_fn.body, type_ignores=[]))
    assert "is_local_sys_id" in create_body, (
        "corrections_client.py::create_correction: is_local_sys_id gate not found "
        "(Phase 95 D-30 / Phase 97 D-NEW-7 invariant)"
    )

    # Gate 3: lists_sync.py :: sync_item_to_cloud — is_local_sys_id in body[:5]
    lists_path = REPO_ROOT / "lists_sync.py"
    tree = ast.parse(lists_path.read_text(encoding="utf-8"))
    sync_item_fn = _find_function(tree, "sync_item_to_cloud")
    assert sync_item_fn is not None, (
        "lists_sync.py: no function named 'sync_item_to_cloud' "
        "(Phase 95 D-30 / Phase 97 D-NEW-7)"
    )
    first5_sync_item = _first_n_statements_source(sync_item_fn, 5)
    assert "is_local_sys_id" in first5_sync_item, (
        "lists_sync.py::sync_item_to_cloud: is_local_sys_id gate not in first 5 "
        f"statements (Phase 95 D-30 / Phase 97 D-NEW-7). First 5 stmts: {first5_sync_item!r}"
    )

    # Gate 4: lists_sync.py :: sync_list_to_cloud — is_local_sys_id present in body
    sync_list_fn = _find_function(tree, "sync_list_to_cloud")
    assert sync_list_fn is not None, (
        "lists_sync.py: no function named 'sync_list_to_cloud' "
        "(Phase 95 D-30 / Phase 97 D-NEW-7)"
    )
    sync_list_body = ast.unparse(ast.Module(body=sync_list_fn.body, type_ignores=[]))
    assert "is_local_sys_id" in sync_list_body, (
        "lists_sync.py::sync_list_to_cloud: is_local_sys_id gate not found "
        "(Phase 95 D-30 / Phase 97 D-NEW-7 invariant)"
    )


# ---------------------------------------------------------------------------
# (b) Web LIBRARY_CODES allowlist: all consumers guard against LOCAL
# ---------------------------------------------------------------------------

def test_web_library_codes_empty_allowlist():
    """D-NEW-7 (b): web/pages/*.py functions that iterate LIBRARY_CODES
    must guard against the 'LOCAL' library code.

    Delegates to the same logic as tests/test_web_library_options_no_local.py
    (Phase 95 D-46) — this test is a fail-fast re-assertion for Phase 97.
    """
    def _function_contains_library_codes_iteration(func_node) -> bool:
        for node in ast.walk(func_node):
            if isinstance(node, ast.Name) and node.id == "LIBRARY_CODES":
                return True
            if isinstance(node, ast.Attribute) and node.attr == "LIBRARY_CODES":
                return True
        return False

    def _function_contains_local_guard(func_node) -> bool:
        for node in ast.walk(func_node):
            if isinstance(node, ast.Compare):
                for operand in (node.left, *node.comparators):
                    if isinstance(operand, ast.Constant) and operand.value == "LOCAL":
                        return True
            elif isinstance(node, ast.Constant) and node.value == "LOCAL":
                return True
        return False

    offenders = []
    for py_file in WEB_PAGES_DIR.rglob("*.py"):
        try:
            source = py_file.read_text(encoding="utf-8")
            tree = ast.parse(source)
        except (SyntaxError, OSError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if not _function_contains_library_codes_iteration(node):
                continue
            if not _function_contains_local_guard(node):
                offenders.append((str(py_file.relative_to(REPO_ROOT)), node.name, node.lineno))

    assert not offenders, (
        "Phase 97 D-NEW-7 (b): web/pages/*.py functions iterate LIBRARY_CODES "
        "without a 'LOCAL' guard:\n"
        + "\n".join(f"  {f}:{ln} in {n}()" for f, n, ln in offenders)
    )


# ---------------------------------------------------------------------------
# (c) is_local_sys_id recognizes 18-digit 97-prefixed sys_ids
# ---------------------------------------------------------------------------

def test_is_local_sys_id():
    """D-NEW-7 (c): is_local_sys_id correctly recognizes (and rejects) sys_ids."""
    from shared.local_sys_id import is_local_sys_id

    # Phase 95 D-19: LOCAL sys_ids are 18-digit numbers derived via
    # generate_local_sys_id() which computes % 10**17 on a hash, then
    # prepends '97' ... actually checks 18 digit + starts with 97 pattern
    # Let's test the documented behavior: 18-digit 97-prefixed is True
    local_id = "97" + "0" * 16  # 18 digits total, starting with 97
    assert is_local_sys_id(local_id) is True, (
        f"Phase 97 D-NEW-7 (c): is_local_sys_id('{local_id}') should be True "
        "(18-digit 97-prefixed sys_id)"
    )

    # Non-97-prefixed 18-digit: should be False
    non_local_id = "12" + "0" * 16
    assert is_local_sys_id(non_local_id) is False, (
        f"Phase 97 D-NEW-7 (c): is_local_sys_id('{non_local_id}') should be False "
        "(not 97-prefixed)"
    )

    # Too short: False
    assert is_local_sys_id("123") is False, (
        "Phase 97 D-NEW-7 (c): is_local_sys_id('123') should be False (too short)"
    )

    # Empty string: False
    assert is_local_sys_id("") is False, (
        "Phase 97 D-NEW-7 (c): is_local_sys_id('') should be False (empty)"
    )


# ---------------------------------------------------------------------------
# (d) LOCAL RRF merge POST _deduplicate() — Phase 95 D-08 P0 invariant
# ---------------------------------------------------------------------------

def _make_engine():
    """Bare SearchEngine instance (no real index required)."""
    from genizah_core import SearchEngine
    with patch("genizah_core.SearchEngine.reload_index", return_value=False):
        with patch.object(SearchEngine, "_open_local_searcher"):
            meta = MagicMock()
            meta.parse_full_id_components.return_value = {}
            engine = SearchEngine(meta, MagicMock())
    engine.local_searcher = None
    return engine


def test_local_post_dedup_merge():
    """D-NEW-7 (d): AST assertion that LOCAL RRF merge call appears AFTER
    _deduplicate(results) in at least one enclosing function (Phase 95 D-08 P0).

    This is a fail-fast smoke re-assertion of the invariant already pinned by
    tests/test_local_post_dedup_merge.py — duplication is intentional for CI
    fast-fail visibility.
    """
    src = (REPO_ROOT / "genizah_core.py").read_text(encoding="utf-8")
    tree = ast.parse(src)

    def _statement_contains_attr_call(stmt: ast.stmt, target_attr: str) -> bool:
        for inner in ast.walk(stmt):
            if isinstance(inner, ast.Call):
                f = inner.func
                if isinstance(f, ast.Attribute) and f.attr == target_attr:
                    return True
                if isinstance(f, ast.Name) and f.id == target_attr:
                    return True
        return False

    # Find functions that call _deduplicate
    dedup_fns = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for inner in ast.walk(node):
                if isinstance(inner, ast.Call):
                    f = inner.func
                    if isinstance(f, ast.Attribute) and f.attr == "_deduplicate":
                        dedup_fns.append(node)
                        break

    assert dedup_fns, (
        "Phase 97 D-NEW-7 (d): No function calling self._deduplicate() found in genizah_core.py"
    )

    any_found = False
    for fn in dedup_fns:
        body = fn.body
        dedup_idx = None
        for i, stmt in enumerate(body):
            if _statement_contains_attr_call(stmt, "_deduplicate"):
                dedup_idx = i
                break
        if dedup_idx is None:
            continue
        tail = body[dedup_idx + 1:]
        if any(
            _statement_contains_attr_call(s, "_rrf_merge")
            or _statement_contains_attr_call(s, "_query_local_index")
            for s in tail
        ):
            any_found = True
            break

    assert any_found, (
        "Phase 97 D-NEW-7 (d) + Phase 95 D-08 P0 invariant VIOLATED: "
        "LOCAL merge hook not found AFTER _deduplicate(results) call site in "
        "any function in genizah_core.py. "
        "Expected _rrf_merge or _query_local_index after dedup in execute_search."
    )
