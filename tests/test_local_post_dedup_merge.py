# -*- coding: utf-8 -*-
"""Phase 95 D-08 (Codex P0): LOCAL hits must merge AFTER _deduplicate().

Pin that:
  1. A LOCAL hit passed to _deduplicate() is dropped (dedup only passes V0.8/V0.7).
  2. A LOCAL hit merged AFTER _deduplicate() survives in the final result list.
  3. AST assertion: the _rrf_merge/_query_local_index call appears in a statement
     AFTER the _deduplicate(results) call site in the same function body (W6).
"""
from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helper — bare SearchEngine instance (no real index required)
# ---------------------------------------------------------------------------

def _make_engine():
    from genizah_core import SearchEngine
    with patch("genizah_core.SearchEngine.reload_index", return_value=False):
        with patch.object(SearchEngine, "_open_local_searcher"):
            meta = MagicMock()
            meta.parse_full_id_components.return_value = {}
            engine = SearchEngine(meta, MagicMock())
    engine.local_searcher = None  # ensure clean state
    return engine


def _v08_hit(uid: str = "v8_uid") -> dict:
    return {
        "uid": uid,
        "full_text": "genizah text",
        "snippet": "genizah text",
        "display": {"source": "V0.8"},
    }


def _local_hit(uid: str = "local_uid") -> dict:
    return {
        "uid": uid,
        "full_text": "local text",
        "snippet": "local text",
        "display": {"source": "LOCAL"},
    }


# ---------------------------------------------------------------------------
# Test 1: _deduplicate drops LOCAL hits (proves why D-08 matters)
# ---------------------------------------------------------------------------

def test_local_hit_before_dedup_dropped():
    """D-08 Codex P0: a LOCAL hit injected BEFORE _deduplicate() is dropped
    because _deduplicate() only passes V0.8/V0.7 sources through.
    This is the REGRESSION TEST proving why merging after dedup is required.
    """
    engine = _make_engine()
    local = _local_hit()
    v08 = _v08_hit()

    # _deduplicate processes both; LOCAL source is not whitelisted
    result = engine._deduplicate([local, v08])

    uids = [r["uid"] for r in result]
    assert "local_uid" not in uids, (
        "_deduplicate must drop LOCAL hits (only V0.8/V0.7 survive)"
    )
    assert "v8_uid" in uids, "_deduplicate must keep V0.8 hits"


# ---------------------------------------------------------------------------
# Test 2: LOCAL hit AFTER _deduplicate survives
# ---------------------------------------------------------------------------

def test_local_hit_after_dedup_survives():
    """D-08 Codex P0: a LOCAL hit merged AFTER _deduplicate() via _rrf_merge
    survives in the final result list.
    """
    engine = _make_engine()
    genizah_hits = [_v08_hit("g1"), _v08_hit("g2")]
    local_hits = [_local_hit("l1")]

    # Simulate the post-dedup merge
    deduped = engine._deduplicate(genizah_hits)  # V0.8 survive
    merged = engine._rrf_merge(deduped, local_hits, k=60)

    uids = [r["uid"] for r in merged]
    assert "l1" in uids, "LOCAL hit merged AFTER _deduplicate must survive"
    assert "g1" in uids, "Genizah hits must also survive"
    assert "g2" in uids, "All Genizah hits must survive"


# ---------------------------------------------------------------------------
# Test 3 (W6): AST assertion — _rrf_merge/_query_local_index appear AFTER
# _deduplicate(results) in the same function body.
# ---------------------------------------------------------------------------

def _find_functions_containing_call(tree: ast.AST, target_attr: str):
    """Walk AST; return ALL FunctionDefs whose body contains a Call to .{target_attr}."""
    results = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for inner in ast.walk(node):
                if isinstance(inner, ast.Call):
                    f = inner.func
                    if isinstance(f, ast.Attribute) and f.attr == target_attr:
                        results.append(node)
                        break  # found in this function, no need to keep searching it
    return results


def _statement_contains_attr_call(stmt: ast.stmt, target_attr: str) -> bool:
    for inner in ast.walk(stmt):
        if isinstance(inner, ast.Call):
            f = inner.func
            if isinstance(f, ast.Attribute) and f.attr == target_attr:
                return True
            if isinstance(f, ast.Name) and f.id == target_attr:
                return True
    return False


def test_local_merge_inserts_after_dedup_call_site():
    """W6: AST-asserted that LOCAL merge appears AFTER the _deduplicate(results)
    call site in AT LEAST ONE enclosing function (D-08 Codex P0).
    There are multiple functions that call _deduplicate; we assert that at least
    one of them has a subsequent _rrf_merge or _query_local_index call (i.e.,
    execute_search has the hook, even if _execute_line_break_search does not).
    Runs from a pytest file (not an illegal python -c one-liner per W6 fix).
    """
    src = Path("genizah_core.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    fns = _find_functions_containing_call(tree, "_deduplicate")
    assert fns, (
        "Expected at least one function that calls self._deduplicate(...) — none found"
    )
    # Check that at least one function has the merge hook AFTER the dedup call.
    any_found = False
    for fn in fns:
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
        "LOCAL merge hook not found AFTER _deduplicate(results) call site in any "
        "function — expected _rrf_merge or _query_local_index after dedup in "
        "execute_search (D-08 Codex P0)"
    )
