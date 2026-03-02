---
status: ready_for_handoff
created: 2026-02-19T00:00:00Z
updated: 2026-02-19T00:00:00Z
owner: codex
scope: web-domain-filter-lag-and-other-toggle
---

# Domain Filter Lag and "Other" Behavior: Root Cause Report

## Objective

Provide a clear technical diagnosis of:
1. Domain filter dialog lag (7-19s reported)
2. "Other" filter behavior inconsistencies
3. Why prior fix attempts did not converge
4. A concrete remediation plan for the next agent

## Code Paths Investigated

- `web/pages/search.py:1719` (`_open_domain_filter_dialog`)
- `web/pages/search.py:2107` (hierarchy pre-cache in `execute_search`)
- `web/pages/parallels.py:1433` (`_open_parallels_domain_filter_dialog`)
- `shared/fjms_service.py:549` (`get_domain_hierarchy`)
- `shared/fjms_service.py:289` (`qualify_domain_name`)
- `shared/fjms_service.py:230` (`AMBIGUOUS_CHILD_DOMAINS`)
- `web/main.py:1915` (search route import path)
- `web/main.py:2297` (`NICEGUI_RELOAD` behavior / startup mode)

## Key Findings

### Finding A: Main hot spot is `get_domain_hierarchy()`

`shared/fjms_service.py:get_domain_hierarchy()` is the expensive operation.

Measured repeatedly in current environment:
- ~4.9s to ~5.3s per call (service-level call timing)

The query:
- groups by `(Domain, ParentDomain)`
- uses `COUNT(DISTINCT AlmaId)`
- forces temp B-tree work for grouping and ordering

Result:
- Any synchronous fallback call during dialog open creates a visible UI freeze.

### Finding B: Dialog data build is fast when hierarchy is already available

Benchmarks of dialog-side Python processing (domain counting + hierarchy match + HTML string build) are small:
- ~0.07s even at large simulated result sets

Result:
- Rendering strategy changes (checkbox vs HTML) help, but do not fix the main freeze if hierarchy DB aggregation still occurs on open.

### Finding C: Runtime/process mismatch was a major source of confusion

Evidence from debug artifacts and current tree state indicates prior sessions had process ambiguity (stale server/reload confusion), causing "no change" observations despite code edits.

Result:
- Multiple fix cycles were attempted without stable proof that the active process was serving edited code.

### Finding D: "Other" qualification logic is present and structurally correct

Current code uses qualified names for ambiguous child domains:
- `AMBIGUOUS_CHILD_DOMAINS = {'Other'}`
- `qualify_domain_name("Other", parent) -> "Other (Parent)"`

Search and parallels both consume qualified names in dedup/filter matching.

Result:
- If active runtime is current code, parent-specific "Other" should be separable.
- If user still sees global collapse behavior, likely stale runtime path or old process.

## Why Previous Attempts Struggled

1. Runtime verification was not enforced first (active process uncertainty).
2. UI rewrites were done while runtime ambiguity existed.
3. Handoff/docs drifted from actual working tree state.
4. Dominant service-level query cost was not eliminated, so lag persisted in fallback paths.

## Recommended Fix Plan (Priority Order)

### Phase 1: Runtime hygiene and proof of active code

1. Ensure a single web server process on port `8081`.
2. During debugging, run with `NICEGUI_RELOAD=false`.
3. Add one startup marker and one dialog-entry marker; verify both in active logs.
4. Hard refresh browser after restart.

Acceptance:
- Clicking domain filter consistently hits the expected function in logs.

### Phase 2: Remove service bottleneck

1. Add in-memory cached hierarchy in `FjmsService` (thread-safe lock).
2. Compute hierarchy once, reuse for subsequent calls.
3. Optional optimization: replace `COUNT(DISTINCT AlmaId)` with `COUNT(*)` if data invariant holds.

Data check in this environment:
- No duplicate `(AlmaId, Domain, ParentDomain)` tuples were found.

Acceptance:
- First hierarchy build is bounded and subsequent calls are near-instant.

### Phase 3: Make open path non-blocking on cold cache

1. Make dialog open handler async.
2. If hierarchy missing, fetch via `await run.io_bound(...)` with loading indicator.
3. Do not perform direct synchronous DB aggregation in on-click path.

Acceptance:
- No multi-second freeze on first open.

### Phase 4: Lock behavior with tests

Add tests for:
1. `qualify_domain_name` and `unqualify_domain_name`
2. Parent-specific "Other" exclusion independence
3. Search/parallels domain exclusion correctness with qualified names

Current state:
- `tests/test_fjms_service.py` passes, but UI-level regression coverage for this bug class is missing.

## Tactical Cleanup Before Commit

1. Remove temporary debug prints in:
   - `web/pages/search.py`
   - `web/main.py`
2. Keep duplicate-child merge fix in `shared/fjms_service.py` (safe guard).
3. Confirm sidecar source path is `fist_data/fjms_enrichment.db` (avoid confusion with root zero-byte `fjms_enrichment.db`).

## Final Conclusion

The real issue is service-layer hierarchy aggregation cost plus runtime/process ambiguity during debugging.

UI control strategy alone is not sufficient if `get_domain_hierarchy()` is allowed to execute synchronously in dialog-open code paths.

If the next agent stabilizes runtime first and then adds hierarchy caching + non-blocking cold-load behavior, both lag and "Other" behavior should converge predictably.

