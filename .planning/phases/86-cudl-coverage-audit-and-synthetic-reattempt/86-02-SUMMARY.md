---
phase: 86-cudl-coverage-audit-and-synthetic-reattempt
plan: 02
subsystem: generation-rewrite
tags:
  - cudl-walked
  - generation-rewrite
  - phase-86
  - synthetic-reattempt
  - bridge-consumer
  - idempotency

# Dependency graph
requires:
  - phase: 86-01
    provides: shared/fist_cudl_bridge.py (explain_fist_by_cudl, lookup_fist_by_cudl, InventoryRecord, build_fist_alias_index)
  - phase: 84-cudl-shelfmark-normalization
    provides: shared/shelfmark_bridge.py (lookup_cudl, build_alias_index, cudl_normalize)
  - phase: 85-synthetic-fjms-inventory-rows
    provides: shared/synthetic_sys_id.py + scripts/generate_synthetic_rows.py outer contract (markers, manifest, residue, CSV-injection fail-loud, CRLF)
provides:
  - scripts/generate_synthetic_rows.py — CUDL-walked _build_qualifying_inventories (was FIST-walked + multi_signature STRICT)
  - scripts/generate_synthetic_rows.py — _build_real_only_csv_bank helper (Pass 2 HIGH-1 idempotency primitive)
  - scripts/generate_synthetic_rows.py — _guess_pattern helper (7 residue categories for Plan 03 adjudication)
  - scripts/generate_synthetic_rows.py — _load_parent_shelfmark_set helper (D-06 filter)
  - scripts/generate_synthetic_rows.py — _build_csv_bank_from_rows helper (libraries.csv -> MetadataManager csv_bank shape for build_alias_index input)
  - scripts/generate_synthetic_rows.py — widened _classify_library_code (Codex MEDIUM Mosseri-prefix detection)
  - scripts/generate_synthetic_rows.py — _write_residue header extended with pattern_guess column (signature stays path-first per Pass 2 MEDIUM-3)
  - scripts/generate_synthetic_rows.py — --dry-run writes residue to reports/synthetic_ambiguity_residue_dryrun.csv (Codex HIGH #5)
  - scripts/generate_synthetic_rows.py — main() invokes build_alias_index(_build_real_only_csv_bank(csv_bank)) before _build_qualifying_inventories
  - tests/test_synthetic_generation_phase86.py — 17 integration tests (3 classes: TestCudlWalkedGeneration, TestClassifyLibraryCode, TestBuildRealOnlyCsvBank)
affects:
  - phase-86-03 (residue patterns consume reports/synthetic_ambiguity_residue_dryrun.csv with pattern_guess column)
  - phase-86-04 (audit re-runs `python scripts/generate_synthetic_rows.py --apply` after Plan 03's adjudication)
  - Phase 85 SYNTH-02..06 carry-forward activates once Plan 04's --apply emits new synthetic rows

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Single-function rewrite preserving outer contract verbatim (MARKER_BEGIN/MARKER_END, _strip_existing_synthetic_block, _build_synthetic_rows D-01a collision, _write_manifest 5-key shape, CSV-injection fail-loud, CRLF detection)"
    - "Status-aware bridge consumer: explain_fist_by_cudl returns ('not_found' | 'single' | 'multi_inventory_ambiguous', entries) so residue ambiguity_kind classifies correctly"
    - "Idempotency primitive: synthetic-stripped csv_bank view passed to Phase 84 alias-index builder so prior-run synthetic block cannot mask qualifying classmarks"
    - "Family-gated _classify_library_code Mosseri detection (canonical + prefix + post-last-':' substring)"
    - "Dry-run residue path explicit-suffix (_dryrun.csv) — separates Plan 03 input from canonical --apply artifact"
    - "Test scaffolding mirrors production 3-table FIST join schema (Pass 2 HIGH-2)"

key-files:
  created:
    - tests/test_synthetic_generation_phase86.py (492 lines) — Phase 86 integration tests (17 tests across 3 classes)
  modified:
    - scripts/generate_synthetic_rows.py — _build_qualifying_inventories REPLACED (CUDL-walked); 4 new helpers (_build_real_only_csv_bank, _guess_pattern, _load_parent_shelfmark_set, _build_csv_bank_from_rows); _classify_library_code widened; _write_residue header extended with pattern_guess column; main() inserts build_alias_index(real_only) wiring and dry-run residue path. Outer contract byte-stable. 771 -> 875 lines.
    - tests/test_generate_synthetic_rows.py — _make_nli_seed schema fix (1-col -> 3-col); _seed_fist_nli adds CUDL manifest for inv 3; 3 obsolete Phase 85 predicate tests REMOVED (Rule 1 deviation); 4 remaining Phase 85 tests updated to seed CUDL manifests so the CUDL-walked path actually visits them.

key-decisions:
  - "Single-function predicate replacement: _build_qualifying_inventories REWRITTEN; every other byte of the Phase 85 outer contract preserved verbatim (markers, manifest, residue header structure modulo new column, CSV-injection fail-loud, CRLF detection). NORM-04 byte-stability preserved on shared/shelfmark_bridge.py + shared/synthetic_sys_id.py + shared/fist_cudl_bridge.py + scripts/export_fist_enrichment.py."
  - "Pass 2 HIGH-1 idempotency fix via _build_real_only_csv_bank: option A (synthetic-stripped csv_bank view passed to build_alias_index) chosen over option B (skip_synthetic_sys_ids param to build_alias_index) — zero Phase 84 mutation, preserves NORM-04 byte-stability on shared/shelfmark_bridge.py."
  - "_write_residue stays path-first (Pass 2 MEDIUM-3): production callers and Phase 85 tests both expect path-first; only the header gets a 9th column (pattern_guess) appended."
  - "--dry-run residue path uses ROOT/reports/synthetic_ambiguity_residue_dryrun.csv literal when residue_path == default (Codex HIGH #5) so plan acceptance grep matches; non-default paths use stem + '_dryrun.csv' suffix derivation."
  - "Phase 85 test contract reconciliation (Rule 1 deviation): 3 obsolete tests REMOVED — `test_d02_expanded_predicate_includes_bibliography`, `test_d02_expanded_predicate_includes_free_desc`, `test_ambiguity_residue_multi_signature_logged`. These tested the dropped Phase 85 predicate (D-02 EXPANDED bib-only emission and D-05a multi_signature exclusion) which Phase 86 D-01a image-bearing-only + D-04 multi_signature relax explicitly REPLACE. The plan's acceptance criterion 'tests/test_generate_synthetic_rows.py existing tests still pass' is interpreted as 'outer-contract tests + non-obsolete predicate tests still pass'."

patterns-established:
  - "Generation-time bridge consumer: import explain_fist_by_cudl + lookup_fist_by_cudl + InventoryRecord from shared.fist_cudl_bridge; build_fist_alias_index(fist_conn) once at the top of _build_qualifying_inventories; query the index via status-aware API for each CUDL classmark."
  - "Synthetic-stripped csv_bank pattern: when consuming a Phase 84 alias index for a generation-time-only purpose, filter synthetics out of the input before invoking build_alias_index. Avoids re-apply masking and preserves Phase 84 byte-stability."
  - "Test fixture schema discipline: in-memory FIST seeds MUST include dbo_Signature (3-table join shape); in-memory NLI seeds MUST use the 3-column cambridge_manifests schema (label, manifest_url, normalized_shelfmark). The 1-col / 2-table shortcut shapes silently produce empty title metadata against real DBs."

requirements-completed:
  - AUDIT-01

# Metrics
duration: ~30min
completed: 2026-05-11
---

# Phase 86 Plan 02: CUDL-walked generation rewrite Summary

**Single-function predicate replacement in `scripts/generate_synthetic_rows.py` — `_build_qualifying_inventories` walks `nli_crossref.db.cambridge_manifests` instead of FIST.db inventories, resolves each classmark through Plan 01's `explain_fist_by_cudl` status-aware API, propagates `title_heb`/`genizah_title` from the FIST UnitCatalogRec join, and emits image-bearing-only synthetic rows. Pass 2 HIGH-1 idempotency closed via synthetic-stripped csv_bank view passed to Phase 84's `build_alias_index`. Plan 85 outer contract preserved verbatim.**

## Performance

- **Duration:** ~30 min (worktree agent, parallel wave 2)
- **Started:** 2026-05-11
- **Completed:** 2026-05-11
- **Tasks:** 2 (both completed)
- **Files modified:** 2 (scripts/generate_synthetic_rows.py, tests/test_generate_synthetic_rows.py)
- **Files created:** 1 (tests/test_synthetic_generation_phase86.py, 492 lines)
- **Tests added:** 17 new Phase 86 tests (TestCudlWalkedGeneration: 11, TestClassifyLibraryCode: 4, TestBuildRealOnlyCsvBank: 2)
- **Tests removed/reconciled:** 3 obsolete Phase 85 predicate tests removed (Rule 1 deviation); 4 remaining Phase 85 tests updated to seed CUDL manifests under the new walker semantics
- **Full pytest suite:** 1821 passed / 21 skipped / 0 failed (182s)

## Accomplishments

- **`_build_qualifying_inventories` rewritten** as CUDL-walked, image-bearing-only resolver. Walks `cambridge_manifests` with `ORDER BY normalized_shelfmark`; for each classmark: (1) skip if `lookup_cudl(classmark) is not None` (Phase 84 real-row covered); (2) resolve via `explain_fist_by_cudl` → status-aware classification; (3) on `single`, skip if `rec.has_alma=True` (alias-only audit-only coverage), log residue for D-06 parent-shadow or CSV-injection leader, otherwise emit a qualifying row with `title_heb`/`genizah_title` propagated from the `InventoryRecord`.
- **Pass 2 HIGH-1 idempotency invariant closed**: new `_build_real_only_csv_bank(csv_bank)` helper produces a synthetic-stripped view; outer `main()` calls `build_alias_index(_build_real_only_csv_bank(csv_bank))` before `_build_qualifying_inventories`. Without this, a prior `--apply` run's synthetic block would resolve via `lookup_cudl` and silently mask the qualifying classmark on the next `--apply` (block-wipe risk). `csv_bank` is never mutated — the helper returns a new dict.
- **Codex HIGH #6 multi_inventory vs no_fist_match distinction**: `explain_fist_by_cudl` returns `('not_found' | 'single' | 'multi_inventory_ambiguous', entries)` and the consumer emits a distinct `ambiguity_kind` for each. Plan 03 can adjudicate the per-category breakdown.
- **Codex HIGH #5 --dry-run residue path explicit**: when `residue_path == RESIDUE_PATH` (canonical default), `--dry-run` writes to `reports/synthetic_ambiguity_residue_dryrun.csv`; non-default paths derive `<stem>_dryrun.csv`. The canonical `reports/synthetic_ambiguity_residue.csv` is only written on `--apply`.
- **Codex HIGH #7 + Pass 2 HIGH-3 audit-only-coverage framing**: classmarks resolving to a FIST inventory with `has_alma=True` are skipped silently (no synthetic, no residue). Plan 04's coverage report counts them as the distinct `phase86_existing_alma_candidate` tier with explicit framing.
- **Gemini HIGH #8 title metadata propagation**: `qualifying[rec.inventory_id]['title_heb']` and `['genizah_title']` flow from the `InventoryRecord` which (via Plan 01's 3-table production-correct join) carries `dbo_UnitCatalogRec.Title` and `GenizahTitleText`. Synthetic rows now have title fallback data; browse-display fallback works.
- **Codex MEDIUM `_classify_library_code` widening**: detects Mosseri in three forms — canonical `'Moss. <Roman>,...'`, `'Mosseri:'` prefix, and any shelfmark whose post-last-`:` substring begins with `'Moss.'`. Default `'CUL'`.
- **`_guess_pattern` residue categorization**: 7 categories (`tsf_flattened_series`, `tsar_flattened_series`, `tsns_minute_or_letter`, `tsns_other`, `or_single_segment`, `mosseri_exotic_letter`, `tsmisc_multi_segment`, `other`) tag every residue row so Plan 03 can adjudicate.
- **`_load_parent_shelfmark_set`** loads `reports/synthetic_parent_shelfmarks.csv` (175-row Phase 85 audit) gracefully — empty set when file is absent.
- **`_write_residue` extended**: header gains `pattern_guess` as the 9th column; existing 8 columns and sort behavior preserved. Path-first signature `_write_residue(path: Path, residue: list[dict]) -> None` preserved per Pass 2 MEDIUM-3.
- **Test suite** (`tests/test_synthetic_generation_phase86.py`, 492 lines): 17 deterministic tests including the originating user case `T-S NS 329.96` (InventoryId=65549106), the 3-table production join title-metadata propagation, multi_inventory vs no_fist_match distinction, alias-only-Alma silent skip, D-06 parent-shadow filter, D-01a image-bearing-only invariant, 6 pattern_guess categories, dry-run residue path, and the Pass 3 shared MEDIUM idempotency test that invokes the REAL `shared.shelfmark_bridge.build_alias_index` against raw + synthetic-stripped csv_bank variants asserting contrasting `lookup_cudl` behaviour.
- **NORM-04 + frozen-contract preservation**: `git diff` shows zero changes to `shared/shelfmark_bridge.py`, `shared/synthetic_sys_id.py`, `shared/fist_cudl_bridge.py`, and `scripts/export_fist_enrichment.py`.
- **Plan 01 → Plan 02 integration verified**: `python -c "from shared.fist_cudl_bridge import explain_fist_by_cudl, lookup_fist_by_cudl, InventoryRecord, build_fist_alias_index; print('plan-01 wired in')"` succeeds at worktree startup.

## Task Commits

Each task was committed atomically (worktree branch, `--no-verify`):

1. **Task 1: Rewrite `_build_qualifying_inventories` with CUDL-walked path + synthetic-stripped alias index + status API + title metadata + Mosseri-prefix _classify_library_code + dry-run residue path + _guess_pattern + _load_parent_shelfmark_set + extend _write_residue header with pattern_guess** — `2e6a155c` (feat). Modified `scripts/generate_synthetic_rows.py` + reconciled `tests/test_generate_synthetic_rows.py` schema + obsolete-test removal.
2. **Task 2: Wave 0 test scaffolding — `tests/test_synthetic_generation_phase86.py` with T-S NS 329.96 closure + D-01a invariant + D-06 filter + multi_inventory DISTINCT-from-no_fist_match + title-metadata propagation + Mosseri-prefix _classify_library_code + idempotency on synthetic-block-present rerun** — `afc74aa1` (test).

## Files Created/Modified

- `scripts/generate_synthetic_rows.py` (modified, 771 → 875 lines). Imports `build_alias_index`, `lookup_cudl`, `cudl_normalize` from `shared.shelfmark_bridge`; `InventoryRecord`, `build_fist_alias_index`, `explain_fist_by_cudl`, `lookup_fist_by_cudl` from `shared.fist_cudl_bridge`; `is_synthetic_sys_id`, `encode_inventory_sys_id` from `shared.synthetic_sys_id`. New helpers `_build_real_only_csv_bank`, `_guess_pattern`, `_load_parent_shelfmark_set`, `_build_csv_bank_from_rows`. `_build_qualifying_inventories` REPLACED entirely with CUDL-walked implementation. `_classify_library_code` widened to detect Mosseri in three prefix forms. `_write_residue` header extended with `pattern_guess` as the 9th column (signature stays path-first). `main()` constructs `csv_bank_full`/`real_only` and invokes `build_alias_index(real_only)` before `_build_qualifying_inventories`; `--dry-run` branch writes residue to `reports/synthetic_ambiguity_residue_dryrun.csv` via `_write_residue` (path-first).
- `tests/test_synthetic_generation_phase86.py` (created, 492 lines). 17 tests across 3 classes covering T-S NS 329.96 closure, title metadata propagation through the 3-table production-correct join, multi_inventory vs no_fist_match distinction, alias-only-Alma silent skip, D-06 parent-shadow filter, D-01a image-bearing-only invariant, 6 pattern_guess categories, Mosseri-prefix `_classify_library_code`, `_build_real_only_csv_bank` immutability + stripping, and the Pass 3 shared MEDIUM idempotency test that exercises the REAL `shared.shelfmark_bridge.build_alias_index` against raw + synthetic-stripped csv_bank variants. Module-scoped `autouse` fixture resets both `_FIST_ALIAS_INDEX` and `_CUDL_ALIAS_INDEX` per test so the idempotency test's mutations don't bleed into siblings. Monkeypatch target is `scripts.generate_synthetic_rows.lookup_cudl` (Codex MEDIUM — the imported binding, not the source module).
- `tests/test_generate_synthetic_rows.py` (modified). `_make_nli_seed` schema fix (1-col → 3-col `cambridge_manifests`) — required because the new SQL `SELECT label, manifest_url, normalized_shelfmark FROM cambridge_manifests` would raise `no such column: label` against the prior 1-col schema (ALL 15 initial test failures had this root cause). `_seed_fist_nli` extended to seed `"tsns330.10"` so inv 3 actually visits the CUDL walker (Phase 85 only seeded inv 2's classmark because the FIST-walked predicate could qualify inv 3 via FJMS metadata alone). `test_d02_expanded_predicate_includes_bibliography`, `test_d02_expanded_predicate_includes_free_desc`, and `test_ambiguity_residue_multi_signature_logged` REMOVED — they tested the dropped Phase 85 D-02 EXPANDED bib-only predicate and the dropped D-05a multi_signature exclusion, both of which Phase 86 explicitly replaces. `test_ambiguity_residue_multi_inventory_logged`, `test_csv_injection_fail_loud`, `test_csv_injection_excludes_row`, and `test_collision_check_fails_loud` updated to seed CUDL manifests so the CUDL-walked path actually visits the inventories under test.

## Decisions Made

- **Plan's `>=820` `min_lines` budget honored**: script grew from 771 to 875 lines (104-line net add) for the new helpers + rewritten predicate. The byte-stability of Phase 85 sections outside the predicate is `git diff`-verifiable (only `_write_residue` header gets `pattern_guess` appended; everything else in the outer contract is untouched).
- **Synthetic-stripped csv_bank pattern (Pass 2 HIGH-1, option A)** chosen over the alternative skip-synthetic-sys_ids parameter to `build_alias_index`. Option A produces a small wrapper helper in the consumer and ZERO changes to `shared/shelfmark_bridge.py`. Preserves NORM-04 byte-stability and ensures runtime consumers of `csv_bank` (browse, FJMS lookups) continue to see synthetic rows unchanged — only the generation-time alias-index input is filtered.
- **`--dry-run` residue path uses literal `synthetic_ambiguity_residue_dryrun.csv` when `residue_path == RESIDUE_PATH`** (the canonical default). This satisfies the plan's grep acceptance criterion `grep -c "synthetic_ambiguity_residue_dryrun" >= 1` and produces the exact filename Plan 03 expects to consume. Non-default `--residue-path` overrides derive the dry-run path via `with_name(stem + "_dryrun.csv")` so test fixtures using `tmp_path` still get a sensible suffix.
- **`_write_residue` sort key string-coerced**: Phase 85 legacy residue dicts carried int `inventory_id`/`signature_id`; Phase 86 residue dicts carry blank strings (`""` for no_fist_match) AND int strings (for `multi_inventory`). The sort key now coerces every field to string so Python 3.10's stricter ordering (no `int < str` comparison) doesn't break the deterministic sort.
- **Phase 85 test contract reconciliation** (Rule 1 deviation): the plan's `must_haves.truths` line "tests/test_generate_synthetic_rows.py existing tests still pass (Phase 85 contract intact)" is fundamentally incompatible with Phase 86 semantics because Phase 85 had two predicate-correctness tests (`test_d02_expanded_predicate_includes_bibliography`, `test_d02_expanded_predicate_includes_free_desc`) asserting bib-only / free-desc-only emission without a CUDL manifest, which the plan explicitly forbids ("DO NOT introduce bib-only / metadata-only inclusion branches (D-01a image-bearing-only)"), AND one D-05a multi_signature exclusion test that the plan explicitly relaxes ("T-S NS 329.96 ... closes here via D-04 multi_signature relax"). The 3 obsolete tests were removed in the Task 1 commit; the remaining 19 Phase 85 tests still cover the outer contract (idempotency, marker-block, manifest, residue header, CRLF, collision check) and all pass under the new semantics.
- **Test fixture schema discipline**: `_make_nli_seed` and `_make_fist_seed` in both test files use the production-correct 3-column `cambridge_manifests` schema and the 5-table FIST schema with `dbo_Signature`. The bridge's SQL uses the 3-table production join (`InventorySignature -> Signature -> UnitCatalogRec`) so the 2-table shortcut schema would silently produce empty title metadata against real `FIST.db`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Plan acceptance criterion "tests/test_generate_synthetic_rows.py existing tests still pass" contradicts Phase 86 semantic changes**

- **Found during:** Task 1 (pytest first run after rewriting `_build_qualifying_inventories`)
- **Issue:** Plan claimed all 22 Phase 85 tests should still pass, but 3 tests specifically assert the DROPPED Phase 85 predicate behaviour:
  - `test_d02_expanded_predicate_includes_bibliography` — asserts inv 3 (no CUDL manifest, bibliography signal only) qualifies. Phase 86 D-01a explicitly forbids this ("Phase 85's FJMS-only inclusion is DROPPED").
  - `test_d02_expanded_predicate_includes_free_desc` — asserts inv 4 (no CUDL manifest, free-description signal only) qualifies. Same D-01a contradiction.
  - `test_ambiguity_residue_multi_signature_logged` — asserts a single InventoryId with 2 SignatureIds produces `ambiguity_kind='multi_signature'` residue. Phase 86 D-04 explicitly relaxes this ("T-S NS 329.96 (InventoryId=65549106, 13 SignatureIds) ... closes here via D-04 multi_signature relax").
- **Fix:** Removed the 3 obsolete tests. The plan's behavioural truth list explicitly drops these predicates; the tests cannot logically coexist with the new semantics.
- **Additional adjustments:** 4 remaining Phase 85 tests (`test_ambiguity_residue_multi_inventory_logged`, `test_csv_injection_fail_loud`, `test_csv_injection_excludes_row`, `test_collision_check_fails_loud`) used empty `_make_nli_seed([])` because Phase 85's FIST-walk emitted residue/qualifying independently of CUDL manifests. Under Phase 86's CUDL-walk, an empty cambridge_manifests means the walker visits no inventories. Updated each test to seed the appropriate CUDL classmark so the walker reaches the assertion-relevant code path. `_seed_fist_nli` similarly updated to include `"tsns330.10"` so inv 3 qualifies under the new semantics.
- **Files modified:** `tests/test_generate_synthetic_rows.py` (committed with Task 1 — both edits were directly caused by the new function semantics).
- **Verification:** `pytest tests/test_generate_synthetic_rows.py -q` exits 0 (19 passed = 22 - 3 obsolete). Full pytest suite still green (1821 passed).
- **Committed in:** `2e6a155c` (Task 1 commit)

**2. [Rule 1 - Bug] `_make_nli_seed` 1-column `cambridge_manifests` schema incompatible with new SQL**

- **Found during:** Task 1 (pytest first run)
- **Issue:** Phase 85's `_make_nli_seed` created `CREATE TABLE cambridge_manifests (normalized_shelfmark TEXT)` — a single-column schema. Phase 86's CUDL-walked SQL is `SELECT label, manifest_url, normalized_shelfmark FROM cambridge_manifests` — three columns. Result: ALL 15 initial test failures shared the same `OperationalError: no such column: label` root cause.
- **Fix:** Updated `_make_nli_seed` to use the production-correct 3-column schema with deterministic synthesized `label = manifest_url = normalized_shelfmark` for test simplicity. This also matches the new `tests/test_synthetic_generation_phase86.py::_make_nli_seed` helper.
- **Files modified:** `tests/test_generate_synthetic_rows.py`
- **Verification:** 6 of the 15 initial failures resolved immediately after the schema fix; the remaining 9 needed the seed-CUDL-manifest fixes documented in deviation #1.
- **Committed in:** `2e6a155c` (Task 1 commit)

**3. [Rule 1 - Bug] Plan's `--dry-run` residue path derivation didn't satisfy its own grep acceptance criterion**

- **Found during:** Task 1 (running the plan's automated verify command)
- **Issue:** The plan's `<action>` block suggested `dryrun_path = ROOT / "reports" / "synthetic_ambiguity_residue_dryrun.csv"` (literal). But the script supports `--residue-path` CLI override, and the literal form ignores that override. A naive `.with_name(stem + "_dryrun.csv")` derivation handles overrides but doesn't produce the literal string `synthetic_ambiguity_residue_dryrun` when `residue_path` is non-canonical — which the plan's grep criterion (`grep -c "synthetic_ambiguity_residue_dryrun" >= 1`) requires.
- **Fix:** Conditional logic — if `residue_path == RESIDUE_PATH` (canonical default), use the literal `ROOT / "reports" / "synthetic_ambiguity_residue_dryrun.csv"`; otherwise derive `<stem>_dryrun.csv`. Both branches satisfy the grep criterion via the literal-string-bearing branch.
- **Files modified:** `scripts/generate_synthetic_rows.py`
- **Verification:** `grep -c "synthetic_ambiguity_residue_dryrun" scripts/generate_synthetic_rows.py` returns 2 (satisfies `>= 1`).
- **Committed in:** `2e6a155c` (Task 1 commit)

**4. [Rule 1 - Bug] `_write_residue` sort key TypeError on mixed-type residue rows**

- **Found during:** Task 1 (running `tests/test_generate_synthetic_rows.py::TestResidueWriter` — though indirectly via TestQualifyingInventories which writes residue)
- **Issue:** Phase 85 sort key was `lambda x: (x["cudl_label"], x["ambiguity_kind"], x.get("signature_id", 0))`. Phase 86 residue dicts can have `signature_id=""` (no_fist_match / multi_inventory rows) AND `signature_id=200` (integer from legacy code paths if any), causing Python 3.10+ `TypeError: '<' not supported between instances of 'int' and 'str'`.
- **Fix:** Coerce every sort-key field to `str()` so all comparisons are string-based. Sort stability is preserved because the field set is deterministic and the string-coercion is bijective for the values produced by the code paths.
- **Files modified:** `scripts/generate_synthetic_rows.py`
- **Committed in:** `2e6a155c` (Task 1 commit)

---

**Total deviations:** 4 auto-fixed (all Rule 1 — three are plan-spec bugs, one is a tactical fixture compatibility issue). No Rule 4 architectural changes needed.
**Impact on plan:** No scope creep. All 4 deviations were either plan-internal contradictions (deviation #1, #3) or schema/typing oversights (deviation #2, #4) that needed reconciliation to satisfy the plan's own acceptance criteria.

## Issues Encountered

- **Worktree base mismatch at startup:** Initial worktree base was `ac6c4771` instead of expected `2bddfdec`. Hard-reset to `2bddfdec` per `<worktree_branch_check>` protocol (safe because fresh worktree had no user changes). Plan 01 artifact (`shared/fist_cudl_bridge.py`) confirmed importable after reset.
- **Mixed-type sort key failure** (deviation #4 above): surfaced during Phase 85 test runs after the predicate rewrite. Fixed inline.
- **No other issues:** Implementation followed the plan's `<action>` blocks structurally. All Task 1 + Task 2 acceptance criteria met.

## TDD Gate Compliance

Plan has `type: execute` (not `type: tdd`), so plan-level RED/GREEN/REFACTOR gates do not apply. Both tasks are marked `tdd="true"` as a hint that they form an impl + test pair, but the execution order is plan-determined: Task 1 ships the new function inline with the test-side reconciliation (Phase 85 tests still pass under new semantics), Task 2 ships the dedicated Phase 86 integration tests. Both commits are non-empty and demonstrate working code + green tests.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- **Plan 03 (residue patterns) unblocked:** can run `python scripts/generate_synthetic_rows.py --dry-run` (when run against real `FIST.db` + `nli_crossref.db`) to produce `reports/synthetic_ambiguity_residue_dryrun.csv` with the new `pattern_guess` column. Plan 03 will consume this artifact to produce `86-RESIDUE-PATTERNS.md`.
- **Plan 04 (audit) unblocked:** can run `python scripts/generate_synthetic_rows.py --apply` after Plan 03's adjudication. The output libraries.csv will contain the new synthetic block; running again will be idempotent (Pass 2 HIGH-1 closed via the synthetic-stripped csv_bank pattern).
- **Phase 85 SYNTH-02..06 carry-forward:** activates once Plan 04 emits new synthetic rows. The synthetic sys_id contract (`99 + zfill(10) + 000000`) is unchanged; downstream consumers (browse hide-NLI gates, FJMS InventoryId fallback, lists/exclusions/parallels round-trip) already exist from Phase 85's reverted-but-preserved infrastructure.
- **No blockers:** all Task 1 + Task 2 acceptance criteria met; full pytest suite green (1821 passed); NORM-04 byte-stability preserved on `shared/shelfmark_bridge.py`, `shared/synthetic_sys_id.py`, `shared/fist_cudl_bridge.py`, and `scripts/export_fist_enrichment.py`.

## Self-Check: PASSED

Files created:
- FOUND: tests/test_synthetic_generation_phase86.py (492 lines)

Files modified:
- FOUND: scripts/generate_synthetic_rows.py (875 lines, was 771)
- FOUND: tests/test_generate_synthetic_rows.py

Commits exist:
- FOUND: 2e6a155c — feat(86-02): rewrite _build_qualifying_inventories to CUDL-walked predicate
- FOUND: afc74aa1 — test(86-02): add tests/test_synthetic_generation_phase86.py (17 tests)

Acceptance criteria (Task 1):
- def _build_qualifying_inventories == 1: 1 (PASS)
- def _guess_pattern == 1: 1 (PASS)
- def _load_parent_shelfmark_set == 1: 1 (PASS)
- def _classify_library_code == 1: 1 (PASS)
- def _build_real_only_csv_bank == 1: 1 (PASS)
- is_synthetic_sys_id >= 2: 4 (PASS)
- from shared.fist_cudl_bridge import == 1: 1 (PASS)
- explain_fist_by_cudl >= 2: 4 (PASS)
- multi_inventory >= 2: 4 (PASS)
- rec.title_heb >= 1: 4 (PASS)
- rec.genizah_title >= 1: 4 (PASS)
- synthetic_ambiguity_residue_dryrun >= 1: 2 (PASS)
- mosseri: >= 1: 1 (PASS)
- lookup_cudl( >= 1: 2 (PASS)
- build_alias_index >= 2: 5 (PASS)
- _build_real_only_csv_bank >= 3: 3 (PASS)
- def _write_residue(path == 1: 1 (PASS — path-first signature preserved)
- MARKER_BEGIN = == 1: 1 (PASS)
- MARKER_END = == 1: 1 (PASS)
- _CSV_INJECTION_LEADERS >= 1: 2 (PASS)
- pattern_guess >= 3: 9 (PASS)
- int(sys_id == 0: 0 (PASS — D-01b lint)
- python -m py_compile: exit 0 (PASS)
- git diff shared/shelfmark_bridge.py: empty (PASS — NORM-04 preserved)
- git diff shared/synthetic_sys_id.py: empty (PASS — D-11 frozen)
- git diff shared/fist_cudl_bridge.py: empty (PASS — Plan 01 untouched)
- git diff scripts/export_fist_enrichment.py: empty (PASS — D-11 frozen)
- script line count >= 820: 875 (PASS)

Acceptance criteria (Task 2):
- test file lines >= 220: 492 (PASS)
- test_tsns_329_96_synthetic_emitted == 1: 1 (PASS)
- test_synthetic_row_has_title_metadata == 1: 1 (PASS)
- test_multi_inventory_ambiguity_kind_distinct == 1: 1 (PASS)
- test_no_fist_match_ambiguity_kind == 1: 1 (PASS)
- test_all_emitted_have_cudl_manifest == 1: 1 (PASS)
- test_parent_shadow_filter_applied == 1: 1 (PASS)
- test_pattern_guess_categories == 1: 1 (PASS)
- test_idempotent_when_synthetic_block_present_in_csv_bank == 1: 1 (PASS)
- empty_phase84_index NOT in idempotency-test signature: PASS (Pass 3 shared MEDIUM)
- from shared.shelfmark_bridge import build_alias_index, lookup_cudl >= 1: 1 (PASS)
- build_alias_index(csv_bank) >= 1: 1 (PASS — raw variant)
- build_alias_index(_build_real_only_csv_bank >= 1: 3 (PASS — stripped variant + Part 3 replay)
- stripped_hit is None >= 1: 1 (PASS)
- raw_hit is not None >= 1: 1 (PASS)
- class TestBuildRealOnlyCsvBank == 1: 1 (PASS)
- class TestClassifyLibraryCode == 1: 1 (PASS)
- test_mosseri_prefix_form == 1: 1 (PASS)
- 65549106 >= 1: 9 (PASS)
- scripts.generate_synthetic_rows.lookup_cudl >= 1: 1 (PASS — Codex MEDIUM monkeypatch target)
- CREATE TABLE dbo_Signature >= 1: 1 (PASS — Pass 2 HIGH-2 3-table schema)
- encode_inventory_sys_id >= 1: 4 (PASS)
- pytest tests/test_synthetic_generation_phase86.py: 17 passed (PASS)
- pytest tests/test_generate_synthetic_rows.py: 19 passed (PASS — Phase 85 outer contract intact, 3 obsolete tests removed per Rule 1 deviation)
- pytest tests/ (full suite): 1821 passed / 21 skipped / 0 failed (PASS)

---
*Phase: 86-cudl-coverage-audit-and-synthetic-reattempt*
*Plan: 02*
*Completed: 2026-05-11*
