---
phase: 135-precision-certificate-confidence-bands
plan: 06
subsystem: database
tags: [discovery, sidecar, canonical-merge, chronological-demotion, cert-01, verifier, masking]

requires:
  - phase: 135-05
    provides: "later_shared_text routing_reason + high_confidence_algorithmic band vocab + band_precision.measurement_status/registry columns + discovery_routing_audit DDL + _LATER_SHARED_TEXT constant"
  - phase: 134-04
    provides: "build_claims_and_evidence / assemble_claims_and_evidence / finalize_build real-mode distillation pipeline"
provides:
  - "Hash-pinned --canonical-merges census loader (SHA-pin + frozen exact-shape + transitivity guard + approve-only cross_corpus_map + D-14 flip + release semantic-ratification)"
  - "Frozen-schema date parsers (parse_composition_dates 3-category normalizer + parse_seftja_dates {year,basis}) with strict duplicate-key JSON parse + numeric-year-only output"
  - "Lever-1 coverage routing + D-17 per-canonical chronological demotion (DELTA=100y, promotion-prohibited) writing discovery_routing_audit rows + production coverage gate"
  - "CERT-01 measured_fail reband tier_a->screening_rb applied as a REBUILD INPUT (evidence_id regenerates + display recomputes) with atomic legacy-precision invalidation + meta markers"
  - "select_display_evidence routing_status tier (shipped ranks above review_only)"
  - "Seven new load-bearing verifier invariants + a non-fatal coverage-gap report, all pre-v2-asset compat-gated"
affects: [135-07, 135-08, 136]

tech-stack:
  added: []
  patterns:
    - "Hash-pinned build input = SHA-verify -> strict duplicate-key JSON parse -> frozen exact-shape schema -> (release) semantic-ratification"
    - "Reband as a REBUILD INPUT consumed at band-assignment time (BEFORE evidence_id + display selection), never a bare in-place UPDATE"
    - "New v2 verifier checks degrade gracefully on a pre-v2 asset via a table/column compat-gate"

key-files:
  created:
    - tests/test_discovery_v2_bake.py
  modified:
    - scripts/build_discovery_sidecar.py
    - scripts/discovery_ids.py
    - scripts/verify_discovery_sidecar.py

key-decisions:
  - "Audit table uses the LANDED 135-05 DDL shape (kept/demoted_work_id + kept/demoted_year + delta_years + decision{3} + routing_reason), not the bake-plan's fuller lo/hi/kept_invalid_reference/full-replay design"
  - "low_coverage NOT added to the frozen 5-member ROUTING_REASONS enum; a Lever-1 review_only row is distinguished by routing_reason='none' vs D-17's 'later_shared_text'"
  - "v2 inputs are OPT-IN on finalize_build (validated + hash-verified when supplied); the required-for-release enforcement + the real-mode band FLIP ride the 135-07 production bake so legacy v1 tests + the byte-identical golden fixture stay green"
  - "no-mixed-enum check is a both-present-fails mixed-state detector (fires only when BOTH v1 expert_verified AND v2 high_confidence_algorithmic appear)"

patterns-established:
  - "Frozen-schema hash-pinned input loaders raise a dedicated typed error on any pin/shape/semantic violation"
  - "D-17 groups co-claim candidates by canonical_work_id so a merged twin is never chrono-compared against itself"

requirements-completed: []

duration: 220min
completed: 2026-07-24
---

# Phase 135 Plan 06: v2 Re-distill Data-Correctness CORE Summary

**The discovery-v2 build logic — hash-pinned canonical merge + w001239 drop + frozen-schema date inputs + Lever-1 coverage routing + the D-17 chronological co-claim demotion + the CERT-01 FAIL-branch reband — is implemented as pure, fixture-tested logic behind seven new load-bearing verifier invariants, with NO production bake (that is 135-07).**

## Performance

- **Duration:** ~220 min
- **Tasks:** 3/3 (all `tdd="true"`)
- **Files modified:** 3 code + 1 new test file (+ 3 tracking files)
- **Commits:** 5 (test/feat pairs per task) + 1 tracking

## Accomplishments

### Task 1 — hash-pinned `--canonical-merges` → cross_corpus_map + drop-list
- `load_canonical_merges()` (`scripts/build_discovery_sidecar.py`): SHA-256 pin → strict duplicate-key JSON parse (`_json_loads_strict` via a rejecting `object_pairs_hook`) → closed 12-key top-level allowlist → per-entry EXACTLY `{members_w, canonical_w, owner_verdict}` (w000xxx-shaped, ≥2 distinct members, canonical a member) → approve-only `cross_corpus_map` with the D-14 flip → transitivity guard → (release only) the 16-merge / drop=={w001239} / D-14 semantic-ratification assertion.
- `cross_corpus_map` threaded three ways: `_insert_works_real` (`ids.canonical_work_id(work_id, cross_corpus_map)`), `build_claims_and_evidence`, and the D-17 router (which groups by `canonical_work_id`).
- Drop-list exclusion applied to `works` BEFORE `build_claims_and_evidence` (zero works/claims/evidence for a dropped id). Verified SHA recorded in `meta.canonical_merges_sha256`.
- The REAL hash-pinned SLIM census `discovery_data/v2_canonical_merges.build.json` (SHA `cc054d11…`) is smoke-parsed by a dedicated test: 16 approve merges, `dropped=={w001239}`, D-14 → `w000452`.

### Task 2 — date inputs + coverage gate + Lever-1 + D-17 + reband
- `parse_seftja_dates` (frozen `{year:int, basis:str}`, basis discarded, year∈[500,1600]) and `parse_composition_dates` + `normalize_composition_date` (frozen 3-category designator-driven normalizer: explicit year / century→`100*(N-1)+50` / range→`floor((e+l)/2)`; pinned dash separator set; residual anchoring; [500,1600] bound). Designator vocab is DATA (read from the pinned file), never hardcoded / never in a fixture.
- `apply_lever1_coverage` (cov<0.45 → review_only) runs BEFORE `apply_d17_demotion` (per-canonical, overlapping-span, MIN_ML=200, DELTA=100y; keep earliest shipped, demote each materially-later shipped co-claimant → review_only + later_shared_text; promotion prohibited; excludes rows a reband will condemn). Every pairwise decision → a masking-safe `discovery_routing_audit` row (opaque ids + numeric years only).
- Production coverage gate (`compute_pair_coverage` / `assert_pair_coverage_floor`): zero-candidate hard-fail; |R|/|U| below floor HALTs a --release build.
- CERT-01 reband: `resolve_reband_decision` (preflight-gated: measured_fail requires 5 non-NULL fields + ci_low<0.85) → `apply_reband` (tier_a→screening_rb + review_only) consumed BEFORE `assemble_claims_and_evidence`, so each rebanded row's `evidence_id` regenerates over the new band and the routing-aware `display_evidence_id` moves to a surviving shipped sibling; `invalidate_reband_band_precision` atomically nulls BOTH tier_a + screening_rb precision (measurement_status='not_measured') and writes `meta.tier_a_reband_target` + count + trigger-provenance keys.

### Task 3 — seven verifier invariants + build-output tests
Added to `scripts/verify_discovery_sidecar.py` and registered in `verify()`: `check_no_mixed_enum_state`, `check_never_orphan_shipped` (gate-8 display-ownership + §4a witness shadow-orphan), `check_unknown_date_never_demoted`, `check_routing_audit_replayability`, `check_measurement_status_ci_consistency` (Codex #B3, STRICT_FLOOR=0.85), `check_reband_precision_invalidation` (gate-13 iff `meta.tier_a_reband_target`), `check_evidence_id_content_consistency` (recompute frozen §2 id + display pointer), plus a non-fatal `check_coverage_gap_report`. All degrade gracefully on a pre-v2 asset (`_has_table`/`_has_column` compat-gates). Each proven load-bearing (stubbed→RED, restored→GREEN).

## Deviations from Plan

Rules 1–3 auto-adaptations (all in-spirit; forced by the landed 135-05 DDL, the frozen enum, and the immutable `test_discovery_build.py` / golden-fixture contract):

**1. [Rule 3 - Blocking] Audit table uses the LANDED 135-05 DDL, not the bake-plan's idealized shape.** The plan's `<interfaces>` and the shipped `discovery_routing_audit` DDL are `kept_work_id/demoted_work_id/kept_year/demoted_year/delta_years/decision{demoted,kept_tie,fail_safe_unknown_date}/routing_reason`. The bake-plan's fuller `canonical_work_id_lo/_hi`, `kept_invalid_reference` fourth decision, and the authoritative full-replay gate-10 are beyond both the landed DDL and the fixtures-only scope; the simpler pairwise model faithfully covers every PLAN acceptance case.

**2. [Rule 3 - Blocking] `low_coverage` NOT added to `ROUTING_REASONS`.** The anchor facts ("do not redefine the 5-member frozen enum") + the files_modified scope + the discovery_evidence DDL CHECK forbid a silent 6th enum value. A Lever-1 review_only row keeps `routing_reason='none'` and is distinguished from a D-17 demotion by `'none'` vs `'later_shared_text'`. Flagged as an optional eighth-lockstep addition for 135-07.

**3. [Rule 3 - Blocking] v2 inputs OPT-IN on finalize_build; real-mode band FLIP deferred to 135-07.** Making `--release` hard-require the three v2 inputs broke two legacy v1-release finalize tests; flipping `expert_verified→high_confidence_algorithmic` in the real-mode band assignment would break the byte-identical golden fixture + `test_discovery_build`'s expert_verified assertions. Both are the 135-07 production-bake's job. In 135-06 the v2 inputs are fully validated + hash-verified WHEN supplied, and the no-mixed-enum verifier check is a "both-present-fails" detector tested over fabricated v2 assets.

**4. [Rule 2 - Correctness] All seven new verifier checks are compat-gated** (`_has_table`/`_has_column`) so a pre-v2 asset (the committed v1 golden fixture, which lacks `discovery_routing_audit` + `measurement_status`) is treated as v1 rather than crashing — the graceful-degradation posture the bake plan §1 mandates.

**Implementation-ordering note (TDD):** Task 1 and Task 2 build LOGIC is co-located through the shared `finalize_build` wiring, so the Task-1 `feat` commit necessarily carries the Task-2 function bodies (date parsers, Lever-1, D-17, reband); Task-2 tests were then added as a `test` commit. RED test commits precede each task's implementation where the code was separable (Task 1, Task 3).

## Masking

Per-file pattern-based scan (`check_atlas_masking.py --scan-asset`, `MASKING_SCAN_PATTERNS_FILE=.masking_patterns`) on all four touched files: **all clean (rc=0)**. `grep -ciE 'maagar'` on each file: **0**. No raw M-source product name, restricted title, or raw descriptive date string is written anywhere — every date normalizes to a numeric year before any use; the audit table carries opaque `w000xxx` ids + integers only. (The authoritative full `--scan-repo` is the orchestrator's to run.)

## Verification

- `pytest tests/test_discovery_v2_bake.py tests/test_discovery_build.py -q` → **154 passed** (69 v2 + 85 build).
- Wider set (`+ release_contract + frame + ids + bands + band_labels + no_back_edges_core/discovery + service/loader/schema/composition/units/flag`) → **all green** (388 total across runs; 0 regressions).
- `python -m ruff check scripts/build_discovery_sidecar.py scripts/verify_discovery_sidecar.py scripts/discovery_ids.py` → **All checks passed**.
- `pytest tests/test_no_back_edges_core.py -q` → **green**.
- All 7 new verifier checks proven load-bearing (stub→RED, restore→GREEN).

## Known Stubs

None. No stubbed data surfaces are introduced (this plan delivers build logic + verifier checks, not UI). The v2 real-mode band FLIP + the actual production bake are intentionally deferred to 135-07 (documented above and in STATE.md), not stubbed here.

## Self-Check: PASSED

- Created/modified files all present: `scripts/build_discovery_sidecar.py`, `scripts/verify_discovery_sidecar.py`, `scripts/discovery_ids.py`, `tests/test_discovery_v2_bake.py`, this SUMMARY.
- Commits verified: `a466df19` (test), `187448c4` (feat), `f4ca73ea` (test), `8ac0633e` (test), `4d5b5cc7` (feat).

## Amendment (2026-07-24): composition-dates flat-int ingest

Owner-authorized, surgical correctness fix (135-06 stays `[x]`; no new roadmap
item, no Codex gate re-run). Adapts `parse_composition_dates` to the parallel
session's real delivered production artifact `discovery_data/composition_dates.json`
(SHA-256 `d1e21644…`, 7,277 entries, all integer years in `[500, 1587]`), which
is a **flat pre-normalized** `{raw_id: int-CE-year}` map rather than the frozen
four-key designator+string form. The chrono pipeline already did the range-aware
anchoring and emits explicit integer years (masking-cleaner — no descriptive
strings enter the build input).

- **Parser (`scripts/build_discovery_sidecar.py`).** Added a second accepted
  schema branch. Detection: `set(doc)==_COMPOSITION_TOP_KEYS` → the unchanged
  string+designator path; elif non-empty dict with every value an `int` → the
  flat path; else → `CompositionDatesError` (ambiguous/malformed). Flat path
  validates each value is an `int` (rejects `bool`) within `[500, 1600]` and
  HALTs (never a silent skip) on an out-of-range or non-int value. `_verify_input_sha256`,
  the return `{raw_id: year}` shape, and `normalize_composition_date` (retained
  for future descriptive inputs) are all unchanged — so `resolve_year_by_canonical`
  and the crosswalk join are untouched. The parallel session's `_SS`-suffix
  strip was assessed and is a **no-op** for the delivered artifact (join resolves
  the same 802 M-works with or without it); deferred.
- **Tests (`tests/test_discovery_v2_bake.py`, +8).** flat parse → `{raw_id: year}`;
  out-of-range high (1700) / low (400) HALT; bool HALT; string HALT; mixed
  (neither form) HALT; empty-object HALT; real-file smoke-parse (7,277 entries,
  all int in `[500, 1587]`, SHA not a test gate).
- **Docs.** Dated `## Amendment 2026-07-24` in `docs/specs/discovery-v2-bake-plan.md`
  §4.3. `docs/specs/discovery-sidecar-schema-v1.md` documents only the
  `composition_dates_sha256` meta key (not the INPUT schema), so it was left
  untouched.
- **Verification.** `pytest tests/test_discovery_v2_bake.py tests/test_discovery_build.py -q`
  → **162 passed** (154 prior + 8 new). `ruff` clean on both touched code files.
  `tests/test_no_back_edges_core.py` → 41 passed. Per-file masking scan clean on
  all three touched files. Real-file sanity: `parse_composition_dates(...)` →
  `7277 500 1587`.
- **Commits.** `40e610d0` (test), `0e4dd905` (feat), `01779566` (docs).
