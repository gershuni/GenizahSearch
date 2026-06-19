# -*- coding: utf-8 -*-
"""SC#3 off-loop guard: assert execute_search is never called on the NiceGUI event loop
in web/pages/joins_lab.py, and that the enclosing synchronous function is passed to
run.io_bound (MEDIUM-4 from 117-REVIEWS.md).

MEDIUM-4 — the guard catches BOTH engine-call shapes that joins_lab.py can use:
  (a) state.searcher.execute_search(...)  — attribute chain ending in .searcher.execute_search
  (b) <name>.execute_search(...)          — routed through the WebSearchExecutor instance
      (e.g. executor.execute_search(...))

Strategy (mirrors tests/test_no_raw_storage_access.py):
  1. Parse web/pages/joins_lab.py with ast.
  2. Find all Call nodes whose function is an Attribute named "execute_search".
  3. For each such call, walk up the parent chain to find the nearest enclosing
     FunctionDef or AsyncFunctionDef.
  4. FAIL if the enclosing function is an async def (direct call on the event loop).
  5. FAIL if the enclosing sync def's name never appears as a positional arg in a
     run.io_bound(...) call in the module (proves enclosure, not just definition).

Scope: ONLY web/pages/joins_lab.py is scanned.  web/joins_executor.py is intentionally
EXCLUDED — the adapter is synchronous and IS meant to call state.searcher.execute_search
directly, since it is itself invoked from inside run.io_bound.

While web/pages/joins_lab.py does not yet exist (Wave 1), the live-file test skips with a
clear message; it becomes load-bearing once Plan 04 lands (Wave 2).

Synthetic-violation sub-tests prove the detector fires for:
  (i)  executor.execute_search(...) directly inside an async def handler.
  (ii) execute_search inside a sync def that is never passed to run.io_bound.
  (iii) the PASS case: execute_search inside a sync def that IS passed to run.io_bound.
"""

import ast
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
JOINS_LAB_PATH = REPO_ROOT / "web" / "pages" / "joins_lab.py"

# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------


def _build_parent_map(tree: ast.AST) -> dict:
    """Return a dict mapping each child node to its parent node."""
    parent_map: dict[int, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parent_map[id(child)] = parent
    return parent_map


def _enclosing_function(node: ast.AST, parent_map: dict):
    """Walk up the parent map from *node* and return the nearest enclosing
    FunctionDef or AsyncFunctionDef, or None if not found."""
    cur_id = id(node)
    while cur_id in parent_map:
        parent = parent_map[cur_id]
        if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return parent
        cur_id = id(parent)
    return None


def _collect_io_bound_args(tree: ast.AST) -> set:
    """Return the set of names passed as the first positional arg to
    ``run.io_bound(...)`` calls anywhere in the module.

    Matches patterns:
        run.io_bound(fn_name)
        run.io_bound(fn_name, ...)
        await run.io_bound(fn_name)

    The call may be inside an Await node; we walk all Call nodes.
    """
    io_bound_args: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        # Match `run.io_bound` attribute call
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "io_bound"
            and isinstance(func.value, ast.Name)
            and func.value.id == "run"
        ):
            if node.args and isinstance(node.args[0], ast.Name):
                io_bound_args.add(node.args[0].id)
    return io_bound_args


def _find_blocking_call_violations(
    source: str,
    blocking_attrs: list,
    filename: str = "<string>",
) -> list:
    """Generic parameterized version of the blocking-call detector.

    Identical logic to _find_execute_search_violations, but matches any method
    name in *blocking_attrs* instead of the hardcoded 'execute_search'.

    Used by Phase 119 to guard VS lookup (get_suggestions) and enrichment batch
    (get_measurement_summaries_batch) call sites.

    Each returned dict has:
        {
            "line": int,
            "call_shape": str,
            "enclosing_fn": str | None,
            "reason": str,
        }

    Violations:
      V1: blocking method called directly inside an async def.
      V2: blocking method inside a sync def whose name is never passed to run.io_bound.
    """
    tree = ast.parse(source, filename=filename)
    parent_map = _build_parent_map(tree)
    io_bound_args = _collect_io_bound_args(tree)

    violations = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        # Match Attribute calls whose attr is one of the blocking method names
        if not (isinstance(func, ast.Attribute) and func.attr in blocking_attrs):
            continue

        call_shape = ast.get_source_segment(source, func) or f"?.{func.attr}"

        enclosing = _enclosing_function(node, parent_map)

        if enclosing is None:
            violations.append({
                "line": node.lineno,
                "call_shape": call_shape,
                "enclosing_fn": None,
                "reason": (
                    f"{func.attr} called at module level (not inside any function)"
                ),
            })
            continue

        # V1: inside an async def → runs on the event loop
        if isinstance(enclosing, ast.AsyncFunctionDef):
            violations.append({
                "line": node.lineno,
                "call_shape": call_shape,
                "enclosing_fn": enclosing.name,
                "reason": (
                    f"{func.attr} is called directly inside async def "
                    f"'{enclosing.name}' (line {enclosing.lineno}) — "
                    f"this runs on the NiceGUI event loop. "
                    f"Wrap the call in a synchronous function and dispatch it via "
                    f"run.io_bound(...)."
                ),
            })
            continue

        # V2: inside a sync def, but that def is never passed to run.io_bound
        fn_name = enclosing.name
        if fn_name not in io_bound_args:
            violations.append({
                "line": node.lineno,
                "call_shape": call_shape,
                "enclosing_fn": fn_name,
                "reason": (
                    f"{func.attr} is inside sync def '{fn_name}' (line "
                    f"{enclosing.lineno}), but '{fn_name}' is never passed as the "
                    f"first positional arg to run.io_bound(...) in this module. "
                    f"An uncalled sync closure does not protect the event loop."
                ),
            })

    return violations


def _find_execute_search_violations(source: str, filename: str = "<string>") -> list:
    """Parse *source* and return a list of violation dicts.

    Each dict has:
        {
            "line": int,
            "call_shape": str,    # e.g. 'executor.execute_search' or 'state.searcher.execute_search'
            "enclosing_fn": str | None,
            "reason": str,
        }

    Violations are:
      V1: execute_search called directly inside an async def.
      V2: execute_search inside a sync def whose name is never passed to run.io_bound.
    """
    tree = ast.parse(source, filename=filename)
    parent_map = _build_parent_map(tree)
    io_bound_args = _collect_io_bound_args(tree)

    violations = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        # We need Attribute calls whose attr is "execute_search"
        if not (isinstance(func, ast.Attribute) and func.attr == "execute_search"):
            continue

        # Determine a human-readable call shape for error messages
        call_shape = ast.get_source_segment(source, func) or "?.execute_search"

        enclosing = _enclosing_function(node, parent_map)

        if enclosing is None:
            # Module-level execute_search — always a violation
            violations.append({
                "line": node.lineno,
                "call_shape": call_shape,
                "enclosing_fn": None,
                "reason": "execute_search called at module level (not inside any function)",
            })
            continue

        # V1: inside an async def → runs on the event loop
        if isinstance(enclosing, ast.AsyncFunctionDef):
            violations.append({
                "line": node.lineno,
                "call_shape": call_shape,
                "enclosing_fn": enclosing.name,
                "reason": (
                    f"execute_search is called directly inside async def "
                    f"'{enclosing.name}' (line {enclosing.lineno}) — "
                    f"this runs on the NiceGUI event loop. "
                    f"Wrap the call in a synchronous function and dispatch it via "
                    f"run.io_bound(...)."
                ),
            })
            continue

        # V2: inside a sync def, but that def is never passed to run.io_bound
        fn_name = enclosing.name
        if fn_name not in io_bound_args:
            violations.append({
                "line": node.lineno,
                "call_shape": call_shape,
                "enclosing_fn": fn_name,
                "reason": (
                    f"execute_search is inside sync def '{fn_name}' (line "
                    f"{enclosing.lineno}), but '{fn_name}' is never passed as the "
                    f"first positional arg to run.io_bound(...) in this module. "
                    f"An uncalled sync closure does not protect the event loop."
                ),
            })
            # No continue — there can only be one enclosing fn per call node

    return violations


# ---------------------------------------------------------------------------
# Live-file test (skips while joins_lab.py absent; load-bearing from Wave 2)
# ---------------------------------------------------------------------------


def test_joins_lab_execute_search_not_on_event_loop():
    """SC#3 / MEDIUM-4: assert joins_lab.py never calls execute_search on the event loop.

    Skips with a clear message while web/pages/joins_lab.py does not yet exist (Wave 1).
    Becomes load-bearing once Plan 04 creates the file (Wave 2).

    Scope: ONLY web/pages/joins_lab.py — NOT web/joins_executor.py (the adapter is
    synchronous and is MEANT to call state.searcher.execute_search directly, because it
    runs inside io_bound).
    """
    if not JOINS_LAB_PATH.exists():
        pytest.skip(
            "web/pages/joins_lab.py not yet created — "
            "this test becomes load-bearing once Plan 04 lands (Wave 2)."
        )

    source = JOINS_LAB_PATH.read_text(encoding="utf-8")
    violations = _find_execute_search_violations(source, filename=str(JOINS_LAB_PATH))

    if violations:
        lines = []
        for v in violations:
            lines.append(
                f"  Line {v['line']} — {v['call_shape']!r}: {v['reason']}"
            )
        raise AssertionError(
            f"Found {len(violations)} execute_search off-loop violation(s) in "
            f"web/pages/joins_lab.py:\n" + "\n".join(lines)
        )


# ---------------------------------------------------------------------------
# Synthetic-violation sub-tests (prove the detector FIRES)
# ---------------------------------------------------------------------------


class TestSyntheticViolationDetected:
    """Negative-control tests: verify the detector fires for known bad patterns."""

    def test_v1_direct_async_call_detected(self):
        """Detector FIRES when executor.execute_search is called inside an async def."""
        source = textwrap.dedent("""\
            async def handle_search(executor):
                results = executor.execute_search("query", mode="exact", gap=0)
                return results
        """)
        violations = _find_execute_search_violations(source)
        assert violations, (
            "Detector should fire for executor.execute_search inside an async def, "
            "but no violations were found."
        )
        assert violations[0]["enclosing_fn"] == "handle_search"
        assert "async def" in violations[0]["reason"]

    def test_v1_awaited_async_call_detected(self):
        """Detector FIRES when execute_search result is awaited inside an async def."""
        source = textwrap.dedent("""\
            async def handle_search(state):
                results = await state.searcher.execute_search("query", mode="exact", gap=0)
                return results
        """)
        violations = _find_execute_search_violations(source)
        assert violations, (
            "Detector should fire for awaited state.searcher.execute_search in async def."
        )
        assert "async def" in violations[0]["reason"]

    def test_v2_sync_def_not_passed_to_io_bound_detected(self):
        """Detector FIRES when execute_search is in a sync def never passed to run.io_bound."""
        source = textwrap.dedent("""\
            async def handle_search(executor):
                def run_core():
                    return executor.execute_search("query", mode="exact", gap=0)
                # Forgot to dispatch: should call run.io_bound(run_core)
                return run_core()
        """)
        violations = _find_execute_search_violations(source)
        assert violations, (
            "Detector should fire when sync def containing execute_search is never "
            "passed to run.io_bound."
        )
        assert "run.io_bound" in violations[0]["reason"]

    def test_v_pass_sync_def_passed_to_io_bound_no_violation(self):
        """Detector PASSES (no violations) when the sync def IS passed to run.io_bound."""
        source = textwrap.dedent("""\
            async def handle_search(executor):
                def run_core():
                    return executor.execute_search("query", mode="exact", gap=0)
                results = await run.io_bound(run_core)
                return results
        """)
        violations = _find_execute_search_violations(source)
        assert not violations, (
            f"Detector should NOT fire when sync def is passed to run.io_bound, "
            f"but got: {violations}"
        )

    def test_v1_state_searcher_execute_search_in_async_detected(self):
        """Detector FIRES for state.searcher.execute_search(...) inside an async def (shape a)."""
        source = textwrap.dedent("""\
            async def do_search():
                results = state.searcher.execute_search("text", mode="exact", gap=0)
                return results
        """)
        violations = _find_execute_search_violations(source)
        assert violations, (
            "Detector must also catch state.searcher.execute_search in async def."
        )
        assert "async def" in violations[0]["reason"]

    def test_scope_adapter_file_excluded(self):
        """web/joins_executor.py is NOT the live-test scan target — only joins_lab.py is.

        The adapter (web/joins_executor.py) is synchronous and is MEANT to call
        state.searcher.execute_search directly; it runs inside run.io_bound as dispatched
        by joins_lab.py.  Scanning the adapter with this detector would produce false
        positives (V2: sync def not passed to io_bound in THAT file).

        This test confirms:
          1. The live test function uses JOINS_LAB_PATH (not the adapter path) as its
             scan target.
          2. Running the detector on the adapter source would indeed find V2 "violations"
             — proving the live test MUST be scoped to joins_lab.py only.
        """
        adapter_path = REPO_ROOT / "web" / "joins_executor.py"
        assert adapter_path.exists(), "web/joins_executor.py should exist after Task 1"

        # Confirm the live-file test scans JOINS_LAB_PATH specifically.
        # Inspect the source of the live test function and verify it opens JOINS_LAB_PATH.
        import inspect as _inspect
        live_test_source = _inspect.getsource(test_joins_lab_execute_search_not_on_event_loop)
        assert "JOINS_LAB_PATH" in live_test_source, (
            "The live-file test must use JOINS_LAB_PATH as its scan target"
        )
        # Additionally confirm the live test does NOT open any path containing "joins_executor"
        # in its body (beyond read-only references in comments/docstrings that `getsource`
        # would include).  We check for actual open/read_text patterns.
        assert "joins_executor.py" not in live_test_source.replace("NOT web/joins_executor.py", ""), (
            "The live-file test must NOT open/read web/joins_executor.py"
        )

        # Prove that scanning the adapter with the detector WOULD yield V2 violations —
        # confirming the live test's scope restriction to joins_lab.py is necessary.
        adapter_source = adapter_path.read_text(encoding="utf-8")
        adapter_violations = _find_execute_search_violations(adapter_source)
        assert adapter_violations, (
            "Running the detector on web/joins_executor.py should find V2 violations "
            "(sync methods call execute_search but are not passed to run.io_bound in "
            "that file — they are dispatched externally by joins_lab.py). "
            "This confirms the live test MUST be scoped to joins_lab.py only."
        )


# ---------------------------------------------------------------------------
# Phase 119: VS lookup + enrichment batch off-loop guard (live-file + synthetic)
# ---------------------------------------------------------------------------


def test_vs_lookup_not_on_event_loop():
    """Phase 119 SC#3 extension: assert joins_lab.py never calls get_suggestions on the event loop.

    Skips while web/pages/joins_lab.py does not yet exist, or while it has not yet
    added any get_suggestions call site (Wave 2 adds the calls).  Becomes load-bearing
    once Wave 2 lands.
    """
    if not JOINS_LAB_PATH.exists():
        pytest.skip(
            "web/pages/joins_lab.py not yet created — "
            "this test becomes load-bearing once Wave 2 lands."
        )

    source = JOINS_LAB_PATH.read_text(encoding="utf-8")
    if "get_suggestions" not in source:
        pytest.skip(
            "web/pages/joins_lab.py does not yet contain 'get_suggestions' — "
            "this test becomes load-bearing once Wave 2 adds the VS lookup call site."
        )

    violations = _find_blocking_call_violations(source, ["get_suggestions"],
                                                filename=str(JOINS_LAB_PATH))
    if violations:
        lines = []
        for v in violations:
            lines.append(f"  Line {v['line']} — {v['call_shape']!r}: {v['reason']}")
        raise AssertionError(
            f"Found {len(violations)} get_suggestions off-loop violation(s) in "
            f"web/pages/joins_lab.py:\n" + "\n".join(lines)
        )


def test_enrichment_batch_not_on_event_loop():
    """Phase 119 SC#3 extension: assert joins_lab.py never calls get_measurement_summaries_batch
    on the event loop.

    Skips while web/pages/joins_lab.py does not yet exist, or while it has not yet
    added any get_measurement_summaries_batch call site (Wave 2 adds the calls).
    Becomes load-bearing once Wave 2 lands.
    """
    if not JOINS_LAB_PATH.exists():
        pytest.skip(
            "web/pages/joins_lab.py not yet created — "
            "this test becomes load-bearing once Wave 2 lands."
        )

    source = JOINS_LAB_PATH.read_text(encoding="utf-8")
    if "get_measurement_summaries_batch" not in source:
        pytest.skip(
            "web/pages/joins_lab.py does not yet contain 'get_measurement_summaries_batch' — "
            "this test becomes load-bearing once Wave 2 adds the enrichment call site."
        )

    violations = _find_blocking_call_violations(
        source, ["get_measurement_summaries_batch"], filename=str(JOINS_LAB_PATH)
    )
    if violations:
        lines = []
        for v in violations:
            lines.append(f"  Line {v['line']} — {v['call_shape']!r}: {v['reason']}")
        raise AssertionError(
            f"Found {len(violations)} get_measurement_summaries_batch off-loop violation(s) in "
            f"web/pages/joins_lab.py:\n" + "\n".join(lines)
        )


class TestSyntheticViolationsPhase119:
    """Negative-control tests: verify _find_blocking_call_violations fires for VS + enrichment."""

    def test_vs_get_suggestions_in_async_detected(self):
        """Detector FIRES when vs_svc.get_suggestions is called inside an async def."""
        source = textwrap.dedent("""\
            async def fetch_vs(anchor_sid, vs_svc):
                raw = vs_svc.get_suggestions(anchor_sid, 200)
                return raw
        """)
        violations = _find_blocking_call_violations(source, ["get_suggestions"])
        assert violations, (
            "Detector should fire for vs_svc.get_suggestions inside an async def, "
            "but no violations were found."
        )
        assert violations[0]["enclosing_fn"] == "fetch_vs"
        assert "async def" in violations[0]["reason"]

    def test_vs_get_suggestions_in_io_bound_no_violation(self):
        """Detector PASSES (no violations) when get_suggestions is inside a sync def
        passed to run.io_bound — the correct off-loop pattern."""
        source = textwrap.dedent("""\
            async def fetch_vs(anchor_sid, vs_svc):
                def run_vs_core():
                    return vs_svc.get_suggestions(anchor_sid, 200)
                raw = await run.io_bound(run_vs_core)
                return raw
        """)
        violations = _find_blocking_call_violations(source, ["get_suggestions"])
        assert not violations, (
            f"Detector should NOT fire when get_suggestions is inside a sync def "
            f"passed to run.io_bound, but got: {violations}"
        )

    def test_enrichment_batch_in_async_detected(self):
        """Detector FIRES when get_measurement_summaries_batch is called inside an async def."""
        source = textwrap.dedent("""\
            async def enrich(sys_ids, fjms):
                data = fjms.get_measurement_summaries_batch(sys_ids)
                return data
        """)
        violations = _find_blocking_call_violations(
            source, ["get_measurement_summaries_batch"]
        )
        assert violations, (
            "Detector should fire for get_measurement_summaries_batch inside an async def, "
            "but no violations were found."
        )
        assert violations[0]["enclosing_fn"] == "enrich"
        assert "async def" in violations[0]["reason"]

    def test_enrichment_batch_in_io_bound_no_violation(self):
        """Detector PASSES (no violations) when get_measurement_summaries_batch is inside
        a sync def passed to run.io_bound — the correct off-loop pattern."""
        source = textwrap.dedent("""\
            async def enrich(sys_ids, fjms):
                def run_enrich_core():
                    return fjms.get_measurement_summaries_batch(sys_ids)
                data = await run.io_bound(run_enrich_core)
                return data
        """)
        violations = _find_blocking_call_violations(
            source, ["get_measurement_summaries_batch"]
        )
        assert not violations, (
            f"Detector should NOT fire when get_measurement_summaries_batch is in a sync def "
            f"passed to run.io_bound, but got: {violations}"
        )
