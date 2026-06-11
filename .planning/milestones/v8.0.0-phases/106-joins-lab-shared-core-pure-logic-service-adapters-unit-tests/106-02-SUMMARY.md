---
phase: 106-joins-lab-shared-core-pure-logic-service-adapters-unit-tests
plan: "02"
subsystem: shared-core
tags: [joins-lab, cross-side, dedup, merge, tdd, pure-logic, search-executor]
dependency_graph:
  requires:
    - shared/joins_lab.py BuilderRow/SideQuery/Candidate/MergeResult + SearchExecutor Protocol (Plan 01)
    - tests/test_joins_lab.py FakeSearchExecutor + _make_result helper (Plan 01)
  provides:
    - shared/joins_lab.py resolve_other_side_pages() pure frozenset neighbor-page logic
    - shared/joins_lab.py cross_side_membership() pure AND/OR set logic
    - shared/joins_lab.py apply_cross_side() I/O-bound cross-side orchestrator via SearchExecutor
    - shared/joins_lab.py dedup_candidates() one-per-(sys_id,page) compaction with anchor-self handling
    - shared/joins_lab.py merge_candidates() stable both→text→VS-only ordering with provenance
    - tests/test_joins_lab.py TestResolveOtherSide / TestCrossSide / TestDedup / TestMerge
  affects:
    - Plan 03 (appends snippet/self-match helpers to same files)
    - Phase 107 desktop JWB (wires apply_cross_side + dedup/merge via the desktop SearchExecutor impl)
tech_stack:
  added:
    - "import dataclasses (module name) — first use of dataclasses.replace() in shared/joins_lab.py"
  patterns:
    - pure functions take already-fetched data (D-06)
    - dataclasses.replace() for frozen-Candidate provenance annotation (T-106-06)
    - try/except graceful degradation on executor calls (T-106-05)
    - FakeSearchExecutor test double with canned results + call recording
key_files:
  created: []
  modified:
    - shared/joins_lab.py (resolve_other_side_pages, cross_side_membership, apply_cross_side,
        dedup_candidates, merge_candidates; import dataclasses added; 357→632 lines)
    - tests/test_joins_lab.py (TestResolveOtherSide, TestCrossSide, TestDedup, TestMerge;
        28→50 tests)
decisions:
  - "import dataclasses added immediately after import re (mirrors shared/refinement.py line 19 analog)"
  - "cross_side_membership() receives totals as a dict parameter (not a lambda) for clean pure-function contract and testability"
  - "apply_cross_side() returns MergeResult not a plain list — consistent with Plan 01 MergeResult contract and carries diagnostic note"
  - "dedup_candidates() uses Candidate.key=(sys_id,page) not the sketch uid-based key — cleaner canonical key per D-02, covers VS-only (page=None) correctly per Pitfall 3"
metrics:
  duration: "~4 minutes"
  completed: "2026-06-03"
  tasks_completed: 2
  tasks_total: 2
  files_created: 0
  files_modified: 2
  tests_added: 22
  lines_added: 275
---

# Phase 106 Plan 02: Cross-Side Membership + Dedup/Merge Summary

resolve_other_side_pages / cross_side_membership / apply_cross_side (SC#2) + dedup_candidates / merge_candidates (SC#3+SC#4) appended to shared/joins_lab.py with frozen-Candidate provenance via dataclasses.replace(); 50 tests green.

## What Was Built

### Task 1 — Other-Side Page Resolution + Cross-Side AND/OR Membership (SC#2)

Three functions appended to `shared/joins_lab.py`:

- **`resolve_other_side_pages(page, total_pages)`** — PURE. Returns a `frozenset` of neighbor page numbers: first page → `{p+1}`, last page → `{p-1}`, middle → `{p-1, p+1}`, single-page doc → `frozenset()`. `total_pages=None` disables the upper clamp while the lower clamp (`< 1`) still applies.

- **`cross_side_membership(base_keys, b_set, combine, totals)`** — PURE set logic transplanted from sketch `_CrossSideWorker.run` (L420-428 AND, L400-419 OR). AND keeps base keys that have a neighbor in `b_set`; OR starts from base and adds in-bounds neighbor pages for each `b_set` entry not already present.

- **`apply_cross_side(executor, base, b_query, b_responsa_options, combine, anchor_pattern=None)`** — the one I/O-bound orchestrator. Runs query B through the injected `SearchExecutor` with `corpus_scope='genizah'`, builds `b_set`, applies AND/OR. AND: filters base candidates. OR: synthesizes neighbor `Candidate` objects via `get_browse_page` + `get_meta_for_id` + `get_library_for_id`. All executor calls wrapped in `try/except` → graceful degradation (T-106-05). Returns `MergeResult(candidates=tuple, note=str)`.

Tests: `TestResolveOtherSide` (5 tests), `TestCrossSide` (6 tests including 3 FakeSearchExecutor-based).

### Task 2 — Candidate Dedup/Compaction + Text/VS Merge Ordering (SC#3 + SC#4)

Step 0: `import dataclasses` added immediately after `import re` — the first use of `dataclasses.replace()` in this module (Plan 01 deliberately omitted it to stay F401-clean).

Two functions appended:

- **`dedup_candidates(raw, anchor_sid, include_self=False)`** — PURE. Single-pass O(n) dedup using `Candidate.key = (sys_id, page)` as the dedup key (VS-only → `(sys_id, None)`, so one VS-only entry per sys_id survives). Every surviving `Candidate` gets `via_text=True` via `dataclasses.replace()`. Anchor-self candidates are excluded by default; when `include_self=True` they are kept with `is_anchor_self=True`. Returns `(deduped_list, anchor_matched: bool)`.

- **`merge_candidates(text_cands, vs_cands)`** — PURE. Annotates text candidates that also appear in the VS set using `dataclasses.replace(via_vs=True, vs_rank=...)` (frozen-safe, T-106-06). Appends VS-only candidates. Stable sort by `(tier, vs_rank)` where tier 0 = both `via_text AND via_vs`, tier 1 = text-only, tier 2 = VS-only. Short-circuits for empty lists.

Tests: `TestDedup` (6 tests), `TestMerge` (5 tests).

## Commits

| Hash | Type | Description |
|------|------|-------------|
| `583015b7` | test | RED: failing tests for cross-side membership + resolve_other_side_pages (SC#2) |
| `f2cfa474` | feat | GREEN: resolve_other_side_pages + cross_side_membership + apply_cross_side (SC#2) |
| `b7cc8069` | test | RED: failing tests for dedup_candidates + merge_candidates (SC#3, SC#4) |
| `4b28aa9f` | feat | GREEN: import dataclasses + dedup_candidates + merge_candidates (SC#3, SC#4) |

## Deviations from Plan

### Auto-fixed Issues

None — plan executed exactly as written.

The only minor design refinement: `cross_side_membership()` receives `totals` as a plain `dict` parameter rather than a closure/lambda (plan said "helper that calls get_browse_page"). The I/O concern lives in `apply_cross_side()`; the pure set-logic function gets a pre-built dict, which makes it cleanly testable without a mock and matches the D-06 "pure functions take already-fetched data" constraint.

## TDD Gate Compliance

| Gate | Commit | Status |
|------|--------|--------|
| RED (Task 1) | `583015b7` | PASS — ImportError on missing functions |
| GREEN (Task 1) | `f2cfa474` | PASS — 11 new tests pass |
| RED (Task 2) | `b7cc8069` | PASS — ImportError on missing functions |
| GREEN (Task 2) | `4b28aa9f` | PASS — 11 more tests pass; 50 total |

## Known Stubs

None — all implementations are complete. The functions return real Candidate objects, real MergeResult tuples, and real provenance annotations.

## Threat Flags

No new network endpoints, auth paths, file access patterns, or schema changes.

- T-106-04 (DoS — unbounded memory): accepted per plan. dedup is O(n) single-pass and strictly reduces memory; OR synthesis bounded by `len(b_set) * 2`.
- T-106-05 (executor call failures): mitigated — all `execute_search` / `get_browse_page` / `get_meta_for_id` / `get_library_for_id` calls wrapped in `try/except`, degrading to fewer/no candidates.
- T-106-06 (frozen Candidate mutation): mitigated — `dataclasses.replace()` used exclusively; confirmed by `grep 'dataclasses.replace' shared/joins_lab.py` and `TestMerge::test_overlap_annotated`.

## Self-Check: PASSED

- `shared/joins_lab.py` exists: FOUND
- `tests/test_joins_lab.py` exists: FOUND
- `grep -q '^import dataclasses$' shared/joins_lab.py`: PASS
- `grep -q 'dataclasses.replace' shared/joins_lab.py`: PASS
- `grep -q 'def resolve_other_side_pages' shared/joins_lab.py`: PASS
- `grep -q 'def cross_side_membership' shared/joins_lab.py`: PASS
- `grep -q 'def apply_cross_side' shared/joins_lab.py`: PASS
- `grep -q 'def dedup_candidates' shared/joins_lab.py`: PASS
- `grep -q 'def merge_candidates' shared/joins_lab.py`: PASS
- Commit `583015b7` exists: FOUND (RED SC#2)
- Commit `f2cfa474` exists: FOUND (GREEN SC#2)
- Commit `b7cc8069` exists: FOUND (RED SC#3/SC#4)
- Commit `4b28aa9f` exists: FOUND (GREEN SC#3/SC#4)
- `pytest tests/test_joins_lab.py -x -q` → 50 passed (Plan 01: 28 + Plan 02: 22)
- `python -m ruff check shared/joins_lab.py tests/test_joins_lab.py` → All checks passed
- Line count `shared/joins_lab.py`: 632 (> min_lines: 120)
- `class TestCrossSide` in `tests/test_joins_lab.py`: FOUND
