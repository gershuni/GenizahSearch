---
phase: 86-cudl-coverage-audit-and-synthetic-reattempt
plan: 01
subsystem: data-bridge
tags:
  - bridge-module
  - fist-cudl
  - phase-86
  - cudl-coverage
  - shelfmark-normalization
  - sqlite

# Dependency graph
requires:
  - phase: 84-cudl-shelfmark-normalization
    provides: cudl_normalize (NORM-03) from shared/shelfmark_bridge.py
  - phase: 85-synthetic-fjms-inventory-rows
    provides: synthetic_sys_id helpers + TestNoIntCoercion lint
provides:
  - shared/fist_cudl_bridge.py — bidirectional FIST<->CUDL shelfmark normalizer
  - fist_to_cudl_keys() — 4 D-02a candidate-key generators
  - build_fist_alias_index() — one-shot alias index over dbo_Inventory
  - explain_fist_by_cudl() — status-aware lookup (not_found/single/multi_inventory_ambiguous)
  - lookup_fist_by_cudl() — convenience wrapper returning single record or None
  - InventoryRecord frozen dataclass with inventory_id, fist_shelfmark, has_alma, title_heb, genizah_title
  - tests/test_fist_cudl_bridge.py — 20 deterministic unit tests
affects:
  - phase-86-02 (generation rewrite consumes explain_fist_by_cudl)
  - phase-86-03 (residue patterns may extend bridge)
  - phase-86-04 (audit re-runs the generation pipeline)
  - Phase 85 carry-forward (SYNTH-01..06 activates once Plan 02 emits new synthetic rows)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Sibling-not-extension preserves NORM-04 frozen contract on shared/shelfmark_bridge.py"
    - "3-table production-correct join path (dbo_InventorySignature -> dbo_Signature -> dbo_UnitCatalogRec)"
    - "Deterministic CTE form (WITH min_ucr_per_inv) pins title fields to MIN(UnitCatalogRecId) row"
    - "Status-aware lookup API (explain_X + convenience wrapper) for disambiguation downstream"
    - "Module-scoped autouse pytest fixture for module-state reset (no conftest mutation)"

key-files:
  created:
    - shared/fist_cudl_bridge.py (292 lines) — bidirectional FIST<->CUDL bridge module
    - tests/test_fist_cudl_bridge.py (279 lines) — 20 deterministic unit tests
  modified: []

key-decisions:
  - "Sibling module over extension: new shared/fist_cudl_bridge.py imports cudl_normalize from shelfmark_bridge.py but does NOT mutate it (NORM-04 byte-stability preserved)"
  - "3-table SQL join through dbo_Signature mirrors scripts/export_fist_enrichment.py exactly (Pass 2 HIGH-2 — schema-shortcut would have produced empty titles against real data)"
  - "Deterministic CTE form (WITH min_ucr_per_inv) replaces non-deterministic GROUP BY shape (Pass 3 MED-86-01) — SQLite permits non-aggregated columns alongside MIN() but the pairing is undefined"
  - "Mosseri concat-form emission: BOTH 'mosseriiii27.1' (dotted) AND 'mosseriiii271' (concat) per Pitfall 1 — CUDL stores both forms"
  - "(N) series-suffix strip family-gated to T-S F / T-S Ar prefixes only (Codex MEDIUM) — prevents spurious aliases on Add./Or. families"
  - "explain_fist_by_cudl status API ('not_found' | 'single' | 'multi_inventory_ambiguous') disambiguates Plan 02 residue ambiguity_kind values (Codex HIGH #6)"
  - "InventoryRecord carries title_heb + genizah_title from dbo_UnitCatalogRec so synthetic-row title fallback has data (Gemini HIGH #8)"

patterns-established:
  - "Generation-time-only bridge: alias index is one-shot module state, NOT a runtime hot path"
  - "In-memory FIST.db schema seeds for tests — NEVER touch real fist_data/FIST.db (Phase 84 Round 3 Codex MEDIUM discipline)"
  - "Test schema mirrors production join shape: dbo_Signature included so tests exercise the same SQL path as scripts/export_fist_enrichment.py"

requirements-completed:
  - AUDIT-01

# Metrics
duration: ~15min
completed: 2026-05-11
---

# Phase 86 Plan 01: FIST<->CUDL Bridge Module Summary

**Bidirectional FIST<->CUDL normalizer with 4 D-02a patterns, status-aware lookup API, and 3-table production-correct alias index over dbo_Inventory — sibling to Phase 84's shelfmark_bridge.py preserving NORM-04 byte-stability**

## Performance

- **Duration:** ~15 min (worktree agent, parallel wave 1)
- **Started:** 2026-05-11
- **Completed:** 2026-05-11
- **Tasks:** 2 (both completed)
- **Files modified:** 0 (2 new files created)
- **Tests added:** 20

## Accomplishments

- `shared/fist_cudl_bridge.py` (292 lines) exposes the public API: `fist_to_cudl_keys`, `build_fist_alias_index`, `lookup_fist_by_cudl`, `explain_fist_by_cudl`, `InventoryRecord`.
- 4 D-02a normalizers implemented: Mosseri Roman expansion with BOTH dotted and concat forms (HIGH #1), prefix-strip after last colon, (N) series-suffix strip family-gated to T-S F / T-S Ar (Codex MEDIUM), Or. multi-segment dot-fix.
- Alias index walks `dbo_Inventory LEFT JOIN dbo_InventoryAlma LEFT JOIN dbo_InventorySignature -> dbo_Signature -> dbo_UnitCatalogRec` (3-table production-correct path, matches `scripts/export_fist_enrichment.py`).
- Deterministic CTE form (`WITH min_ucr_per_inv`) pins title metadata to the MIN(UnitCatalogRecId) row per inventory — replaces non-deterministic `MIN()` + non-aggregated `cat.Title` shape that SQLite permits but doesn't guarantee.
- Status-aware lookup (`explain_fist_by_cudl`) returns `(status, entries)` with `status in {'not_found', 'single', 'multi_inventory_ambiguous'}` for downstream ambiguity classification.
- `tests/test_fist_cudl_bridge.py` (279 lines, 20 tests) exercises all 4 normalizers, (N) family-gating negative fixture, all 3 status values, title metadata propagation through 3-table join schema, T-S NS 329.96 (originating user case) closure, and `has_alma` propagation.
- Full pytest suite green: 1807 passed / 21 skipped / 0 failed.
- NORM-04 byte-stability preserved: `git diff --quiet shared/shelfmark_bridge.py` returns 0.
- TestNoIntCoercion (D-01b lint) still passes — no `int(sys_id)` patterns in new module.

## Task Commits

Each task was committed atomically:

1. **Task 1: Create shared/fist_cudl_bridge.py with 4 D-02a normalizers + 3-table join + lookup + explain** — `91eb38a6` (feat)
2. **Task 2: Wave 0 test scaffolding — tests/test_fist_cudl_bridge.py with 20 unit tests** — `5a079a68` (test)

_Note: This was structured as two `tdd="true"` tasks (Task 1 = module, Task 2 = tests). Task 1 ran inline `<verify>` after writing the module; Task 2 wrote the full unit-test suite that exercises Task 1's behavior. Test-first ordering happens at the plan level (Wave 0 produces this bridge + tests before Plan 02 consumes it)._

## Files Created/Modified

- `shared/fist_cudl_bridge.py` (created, 292 lines) — Bidirectional FIST<->CUDL bridge. Public API: `fist_to_cudl_keys`, `build_fist_alias_index`, `lookup_fist_by_cudl`, `explain_fist_by_cudl`, `InventoryRecord`. Imports `cudl_normalize` from `shared.shelfmark_bridge`. Module-level `_FIST_ALIAS_INDEX` populated by `build_fist_alias_index` (one-shot generation-time cost).
- `tests/test_fist_cudl_bridge.py` (created, 279 lines) — 20 deterministic unit tests across 3 test classes: `TestFistToCudlKeys` (12), `TestExplainFistByCudl` (3), `TestLookupFistByCudl` (5). `_seed_fist` helper builds in-memory `:memory:` SQLite with the full 5-table schema (Inventory, InventoryAlma, InventorySignature, Signature, UnitCatalogRec). Module-scoped `autouse` fixture resets `_FIST_ALIAS_INDEX` per test.

## Decisions Made

- **Sibling-not-extension architecture:** Created `shared/fist_cudl_bridge.py` as a new module rather than extending `shared/shelfmark_bridge.py`. This preserves Phase 84 NORM-04 byte-stability and keeps reverse-direction (FIST -> CUDL) logic separate from forward (libraries.csv -> CUDL) logic. The new module imports `cudl_normalize` from the old module but does not mutate it.
- **3-table SQL join through `dbo_Signature`:** The title-metadata SELECT joins `dbo_InventorySignature -> dbo_Signature -> dbo_UnitCatalogRec` rather than taking the 2-table shortcut `dbo_UnitCatalogRec.SignatureId = dbo_InventorySignature.SetSignatureId`. Reference: `scripts/export_fist_enrichment.py` lines 150-176 (and 5 other join sites) all use the 3-table form. Pass 2 HIGH-2 review caught that the shortcut would silently produce empty titles against real `FIST.db` data while tests passed against a mirroring shortcut schema.
- **Deterministic CTE form:** Used `WITH min_ucr_per_inv AS (SELECT MIN(UnitCatalogRecId) ...) ... LEFT JOIN dbo_UnitCatalogRec ucr_pick ON ucr_pick.UnitCatalogRecId = m.min_ucr_id` rather than the simpler `SELECT cat.Title, MIN(cat.UnitCatalogRecId) ... GROUP BY isig.InventoryId`. SQLite permits non-aggregated columns alongside aggregates but the non-aggregated pairing is undefined behavior (could pick any row in the group, not the MIN). CTE form pins title to the deterministic min-rowid row.
- **Mosseri concat-form emission:** Both `mosseriiii27.1` (dotted) and `mosseriiii271` (concat) emitted for `Moss. III,27.1`. Per RESEARCH.md Pitfall 1: CUDL stores BOTH forms across its corpus. Codex+Gemini HIGH concern #1.
- **(N) family-gating:** `(N)` series-suffix strip is gated to `_SERIES_N_FAMILY_PREFIXES = ("t-s f", "t-s ar")`. Without gating, `Add. 12 (1)` would spuriously gain a dropped-(N) alias `add12`, corrupting Add./Or. lookups (Codex MEDIUM).
- **Status-aware lookup API:** `explain_fist_by_cudl` returns `(status, entries)` so Plan 02's residue classification can distinguish `no_fist_match` (key absent) from `multi_inventory_ambiguous` (D-04a exclude) — both result in `None` from `lookup_fist_by_cudl` but have different audit semantics (Codex HIGH #6).
- **Title metadata in `InventoryRecord`:** Added `title_heb` and `genizah_title` fields so Plan 02's synthetic-row generation can populate title fallback values from `dbo_UnitCatalogRec` (Gemini HIGH #8). Without this, synthetic rows would have empty titles, blocking browse-display fallback.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Plan must_have spec typo: `mosseriv27.1` for `Moss. IV,27.1`**
- **Found during:** Task 2 (running `test_prefix_strip_after_last_colon`)
- **Issue:** Plan's `must_haves.truths` and `<behavior>` specified `'mosseriv27.1' AND 'mosseriv271'` as expected keys for `Moss. IV,27.1`. The actual Mosseri concat pattern is `mosseri` + `iv` (Roman IV in lowercase) = `mosseriiv` (the trailing 'i' of "mosseri" plus the leading 'i' of "iv"). The plan got the III case correct (`mosseriiii27.1` for `Moss. III,27.1` = `mosseri` + `iii`), so this was a single-character typo dropping one `i`. Verified via `construct_mosseri_cudl_label('Moss. IV,27.1')` -> `MS-MOSSERI-IV-00027-00001` -> `_index_key_for_label` -> `mosseriiv271`.
- **Fix:** Updated `test_prefix_strip_after_last_colon` to assert `'mosseriiv27.1' in keys` and `'mosseriiv271' in keys`. Added a comment documenting the deviation and explaining how to verify the empirical CUDL convention.
- **Files modified:** `tests/test_fist_cudl_bridge.py`
- **Verification:** All 20 tests now pass; full pytest suite green (1807 passed / 21 skipped).
- **Committed in:** `5a079a68` (Task 2 commit)

**2. [Rule 1 - Bug] Plan's inline `<verify>` automated assertion contained a buggy expression**
- **Found during:** Task 1 (running the verify command after writing the module)
- **Issue:** The plan's inline verify command included `all('add12' not in key or '(' not in key for key in add_keys)` which fails when `add_keys = {'add12(1)'}` (the only key produced by `fist_to_cudl_keys('Add. 12 (1)')`). The assertion was over-strict and contradicted the plan's own `<behavior>` line: "Add. 12 (1) does NOT add a spurious dropped-(N) alias". The actual behavior matches the spec — the module produces only `{'add12(1)'}`, no dropped-(N) alias `'add12'`.
- **Fix:** Relied on the explicit `<behavior>` and `<acceptance_criteria>` (which use grep-based file checks, not the buggy expression). The Task 2 test `test_series_n_strip_family_gating_add_not_stripped` exercises the correct intent: `stripped = cudl_normalize("Add. 12")` (= `'add12'`) MUST NOT be in keys. That test passes.
- **Files modified:** None (verify command was buggy; module behavior was already correct)
- **Verification:** `pytest tests/test_fist_cudl_bridge.py::TestFistToCudlKeys::test_series_n_strip_family_gating_add_not_stripped -q` passes.
- **Committed in:** No separate commit (no code change needed)

---

**Total deviations:** 2 auto-fixed (both Rule 1 - plan-spec bugs)
**Impact on plan:** Both deviations were inconsequential typos in plan documentation; the actual module/test behavior matches the empirical CUDL convention and the plan's `<behavior>` section. No scope creep; no architectural change.

## Issues Encountered

- **Worktree base mismatch at startup:** Initial worktree base was `ac6c4771` instead of expected `fd6ef898`. Hard-reset to `fd6ef898` per `<worktree_branch_check>` protocol (safe because fresh worktree has no user changes).
- **No other issues:** Implementation followed the plan's `<action>` blocks verbatim. Tests passed on first run after fixing the plan-spec Mosseri-IV typo.

## TDD Gate Compliance

This plan has `type: execute` (not `type: tdd`) so plan-level RED/GREEN/REFACTOR gates do not apply. Both tasks are marked `tdd="true"` as a hint that they form a test+impl pair, but the execution order (Task 1 = impl with inline verify, Task 2 = full test suite) is plan-determined and structured as Wave 0 test scaffolding produced alongside the bridge module.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- **Plan 02 (generation rewrite) unblocked:** Can `from shared.fist_cudl_bridge import lookup_fist_by_cudl, explain_fist_by_cudl, InventoryRecord, build_fist_alias_index` without errors. Status API supports residue ambiguity_kind classification.
- **Plan 03 (residue patterns):** Can extend `_SERIES_N_FAMILY_PREFIXES` or add new D-02a patterns by adding to `fist_to_cudl_keys`. Module structure mirrors Phase 84 bridge for maintenance familiarity.
- **Plan 04 (audit):** Can re-run generation pipeline; the title-metadata join path is production-correct so first run should match real FIST.db data.
- **No blockers:** All Task 1 + Task 2 acceptance criteria met; full pytest suite green; NORM-04 byte-stability preserved.

## Self-Check: PASSED

Files created:
- FOUND: shared/fist_cudl_bridge.py (292 lines)
- FOUND: tests/test_fist_cudl_bridge.py (279 lines)

Commits exist:
- FOUND: 91eb38a6 — feat(86-01): add shared/fist_cudl_bridge.py
- FOUND: 5a079a68 — test(86-01): add tests/test_fist_cudl_bridge.py

Acceptance criteria (Task 1):
- File >= 180 lines: 292 (PASS)
- fist_to_cudl_keys: 1 def (PASS)
- build_fist_alias_index: 1 def (PASS)
- lookup_fist_by_cudl: 1 def (PASS)
- explain_fist_by_cudl: 1 def (PASS)
- InventoryRecord class: 1 (PASS)
- title_heb >= 4: 6 (PASS)
- genizah_title >= 4: 6 (PASS)
- rest_norm.replace >= 1: 1 (PASS)
- _SERIES_N_FAMILY_PREFIXES >= 2: 2 (PASS)
- multi_inventory_ambiguous >= 1: 4 (PASS)
- cudl_normalize import: 1 (PASS)
- int(sys_id) == 0: 0 (PASS)
- dbo_UnitCatalogRec >= 1: 8 (PASS)
- JOIN dbo_Signature >= 1: 1 (PASS)
- WITH min_ucr_per_inv >= 1: 1 (PASS)
- min_ucr_id >= 2: 2 (PASS)
- ucr_pick.UnitCatalogRecId = m.min_ucr_id >= 1: 1 (PASS)
- TestNoIntCoercion lint: PASS
- shelfmark_bridge.py byte-stable: PASS

Acceptance criteria (Task 2):
- File >= 150 lines: 279 (PASS)
- pytest tests/test_fist_cudl_bridge.py: 20 passed (PASS)
- TestFistToCudlKeys: 1 class (PASS)
- TestExplainFistByCudl: 1 class (PASS)
- TestLookupFistByCudl: 1 class (PASS)
- test_mosseri_roman_concat_form: 1 (PASS)
- test_series_n_strip_family_gating_add_not_stripped: 1 (PASS)
- test_status_multi_inventory_ambiguous: 1 (PASS)
- test_inventory_record_carries_title_metadata: 1 (PASS)
- test_one_inventory_resolves_unambiguously: 1 (PASS)
- test_multi_signature_within_one_inventory_picks_lowest (old name, must be 0): 0 (PASS)
- CREATE TABLE dbo_Signature: 1 (PASS)
- sig_links >= 2: 4 (PASS)
- 65549106 >= 2: 5 (PASS)
- Full pytest suite: 1807 passed / 21 skipped (PASS)

---
*Phase: 86-cudl-coverage-audit-and-synthetic-reattempt*
*Plan: 01*
*Completed: 2026-05-11*
