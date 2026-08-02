---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 08
subsystem: discovery-privacy
tags: [vis-01, visibility, masking, closed-graph, projection, sqlite, discovery-sidecar]

# Dependency graph
requires:
  - phase: 136-01
    provides: "The Amendment 2026-08-02 schema contract defining assertion_visibility/identity_visibility, discovery_identification, manuscript_display, meta.audience, and the D-10a index set"
provides:
  - "shared/discovery_visibility.py: the two D-22 axes (assertion_visibility, identity_visibility) as pure, fail-closed functions, plus is_public (the ONE public-eligibility conjunction) and reconcile_launch_scope (build-time launch-scope-vs-conjunction reconciliation, resolves nothing)"
  - "scripts/project_discovery_public.py: the closed-graph public projection -- an unrecognized table is a build error; discovery_identification and display_work_id are recomputed bottom-up from surviving public evidence, never filtered from the private build's own rows; counts/aggregates recomputed, never copied; the projection's own final gate runs both --scan-asset and --scan-sqlite"
  - "tests/test_vis01_projection.py: the six leak controls (structural absence, cell-level leak, schema-level leak, fail-closed, orphan FK, copied total, unprojected table), each watched actually failing"
affects: [136-11, 136-12, 136-13, 136-20]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Test-injectable masking patterns (`run_masking_gate(db, patterns=...)` / `project(..., masking_patterns=...)`) mirroring `scripts/build_discovery_sidecar.py::finalize_build(masking_patterns=...)` -- production default reads MASKING_SCAN_PATTERNS_FILE + runs --scan-repo; tests inject a small disposable pattern (fast, portable) or, for the two leak controls specifically, a REAL pattern sourced dynamically from the local file at test time (never hardcoded)"
    - "Schema DDL replay (read the private db's own sqlite_master.sql verbatim and re-execute it against the output) instead of a second hand-typed CREATE TABLE copy -- the output schema can never drift from whatever the private build actually shipped"
    - "Bottom-up aggregate recomputation: discovery_identification groups are built FROM the surviving evidence set (grouped by sys_id x canonical_work_id), never by filtering the private build's own discovery_identification rows -- structurally prevents a private claim's contribution from surviving inside a shared aggregate"

key-files:
  created:
    - shared/discovery_visibility.py
    - scripts/project_discovery_public.py
    - tests/test_discovery_visibility.py
    - tests/test_vis01_projection.py
  modified: []

key-decisions:
  - "assertion_visibility(evidence_row) reads a build-time-only `assertion_source_corpus` key (the raw per-evidence-occurrence origin, discarded after build) -- distinct from identity_visibility(work_row), which reads the EXISTING works.source_corpus column. Both are pure, fail-closed (anything other than the two open masked codes -- sefaria/ja -- yields private, including unknown codes, None, and raw-id-shaped strings)."
  - "reconcile_launch_scope is intentionally BUILD-TIME ONLY (it needs the raw corpus origin, which no longer exists once the private asset is built). The projection script's own compute_launch_scope_reconciliation is a SEPARATE, parallel reporting routine operating on the ALREADY-BUILT private asset's STORED assertion_visibility/identity_visibility columns -- both report the identical symmetric-difference shape and both call the SAME shared.discovery_visibility.is_public conjunction; neither restates it."
  - "discovery_identification's main_pool/main_pool_reason bucketing uses a SIMPLE, explicitly-documented stand-in heuristic (human_confirmed > multi-folio > full-coverage > insufficient_length), NOT the real main-pool-rule engine, because shared/discovery_main_pool.py (owned by plans 136-07/136-11/136-12) did not exist in this worktree at execution time. Flagged in-code and here for 136-11/136-12 to reconcile by importing the real module once it lands, rather than leaving this heuristic as an undocumented drift risk."
  - "band_precision is projected by verbatim copy, not recomputation -- its numerator/denominator/precision figures are EXTERNAL, pre-registered certification measurements (a held-out sample draw over a whole collection/band), never a per-asset row aggregate of the current build, so the 'recompute, don't copy' discipline (which governs release-contract ROW COUNTS) does not apply to it. Stated explicitly as its own projection rule so a future table is never assumed to share this same pass-through behavior without its own citation."
  - "discovery_routing_audit rows are dropped (not merely left alone) when either kept_work_id or a non-null demoted_work_id resolves to a work that did not survive the projection -- protects both the closed-graph invariant (the audit table has no formal FK but its work_id values should still resolve) and the masking discipline (a private work's existence must not be inferable from an audit-log row that outlived it)."

patterns-established:
  - "A second, independent eligibility conjunction is a code smell this plan explicitly guards against: is_public is called exactly twice in the whole tree (once inside shared/discovery_visibility.py's own reconcile_launch_scope, once inside scripts/project_discovery_public.py's ProjectionContext) -- grep confirms no inline `assertion_vis == 'public' and identity_vis == 'public'` restatement anywhere."

requirements-completed: [VIS-01, NOVEL-02]

# Metrics
duration: 55min
completed: 2026-08-02
---

# Phase 136 Plan 08: VIS-01 Two-Axis Visibility Derivation & Closed-Graph Public Projection Summary

**Public/private eligibility is now the conjunction of two independently-derived, fail-closed axes (never a `works.source_corpus`-only proxy), and the public projection is a closed graph that recomputes every aggregate from the surviving public evidence set rather than filtering the private build's own stored totals -- proven by six leak controls that were each watched actually failing.**

## Performance

- **Duration:** 55 min
- **Started:** 2026-08-02 (session continuation, wave 2)
- **Completed:** 2026-08-02
- **Tasks:** 3 completed
- **Files modified:** 4 (all new)

## Accomplishments

- `shared/discovery_visibility.py`: `assertion_visibility(evidence_row)` and `identity_visibility(work_row)` derive D-22's two axes as pure, fail-closed functions returning only the closed `{public, private}` enum -- never a raw origin id. `is_public(assertion_vis, identity_vis)` is the ONE eligibility conjunction in the tree. `reconcile_launch_scope(rows)` reports (never resolves) the disagreement between VIS-01's own corpus/family shortcut and the two-axis conjunction, broken down by corpus x family -- a real number for plan 136-13's gate battery. 70 tests, both mislabelling directions (open work/restricted assertion, restricted work/open assertion), a full adversarial-input table (unknown code, `None`, empty string, non-string, raw-id-shaped string), and an explicit no-raw-id-in-return-value assertion.
- `scripts/project_discovery_public.py`: emits a NEW SQLite artifact containing only rows the conjunction allows, then closes the graph. Replays the private db's own `sqlite_master.sql` DDL verbatim (no second, drift-prone schema copy). An explicit `PROJECTION_RULES` dict covers every table (`works`, `discovery_claim`, `discovery_evidence`, `witness_units`/`witness_unit_members`, `manuscript_display`, `discovery_routing_audit`, `band_precision`, `discovery_identification`, `meta`) -- an unrecognized table raises `ProjectionError` rather than being silently copied whole. `discovery_identification` rows are re-derived bottom-up from surviving evidence grouped by `(sys_id, canonical_work_id)`, never filtered from the private build's own rows; `display_work_id` is re-selected among the canonical group's PUBLIC members only (schema SS(B1)'s ordered rule, restricted), correctly skipping a private canonical anchor in favor of a public sibling. Release-contract row counts are recomputed over the projected tables, never copied. `check_fk_closure`/`check_meta_counts` are independent, callable-alone functions so a hand-corrupted artifact can be diagnosed directly (used by the orphan and copied-total controls). The projection's own final gate runs BOTH `--scan-asset` and `--scan-sqlite` (production default: the literal required CLI invocation under `--strict --scan-repo`; test mode: an injectable `masking_patterns` list, mirroring `build_discovery_sidecar.py::finalize_build`'s own convention) and removes the artifact on any failure. Refuses an output path inside `web/static/`.
- `tests/test_vis01_projection.py`: a baseline test over the four measured shapes (open/open survives; the other three combinations are excluded) plus the six required leak controls, each asserting a FAILURE and naming the check that fired: (1) structural absence -- queries the public db directly and asserts the private row's primary key does not exist; (2) cell-level leak -- a marker sourced dynamically from the local `MASKING_SCAN_PATTERNS_FILE` (never hardcoded) seeded into a surviving work's title fails the gate, the artifact is removed, and the failure message records the issue count (regex-extracted and asserted `>= 1`); (2b) schema-level leak -- the same marker seeded into a column NAME is caught by `scan_sqlite` via the `::schema` surface tag, demonstrating coverage `--scan-asset` alone is not proven to provide; (3) fail-closed -- an unset `MASKING_SCAN_PATTERNS_FILE` causes a nonzero exit; (4) orphan -- deleting a work referenced by a surviving claim fails `check_fk_closure`; (5) copied total -- leaving a stored total un-recomputed fails `check_meta_counts`; (6) unprojected table -- an extra table with no projection rule is a build error. Two additional tests directly prove the anti-leak recompute discipline: a private sibling claim on the same manuscript/canonical-work group cannot change the public identification's `page_count`/`best_band_rank`/`max_coverage_ppm`/`main_pool_reason`, and a private canonical anchor is skipped in favor of a public group member for `display_work_id` (never left dangling). 16 tests, all genuinely executed (verified verbose -- none skipped on this machine, since `.masking_patterns` is present locally).

## Task Commits

Each task was committed atomically (Task 1 followed the TDD RED -> GREEN cycle per its `tdd="true"` flag):

1. **Task 1 (RED): failing tests for the two-axis derivation** - `4328b9e3` (test)
2. **Task 1 (GREEN): the two-axis derivation implementation** - `bd42d389` (feat)
3. **Task 2: the closed-graph public projection script** - `b94db6fd` (feat)
4. **Task 3: the six leak controls + baseline/recompute tests** - `382a7238` (test)

_Plan metadata commit (this SUMMARY + STATE/ROADMAP/REQUIREMENTS) follows separately per the executor protocol -- STATE.md/ROADMAP.md are NOT touched by this executor per the orchestrator's instruction._

## Files Created/Modified

- `shared/discovery_visibility.py` (273 lines) - the two D-22 axes, `is_public`, `reconcile_launch_scope`
- `scripts/project_discovery_public.py` (812 lines) - the closed-graph public projection CLI + library
- `tests/test_discovery_visibility.py` (309 lines) - 70 tests for the visibility module
- `tests/test_vis01_projection.py` (674 lines) - 16 tests for the projection script, including all six leak controls

## Decisions Made

See `key-decisions` in the frontmatter above. The most consequential: `reconcile_launch_scope` stays strictly build-time (raw-corpus-input) per D-22's own framing, while the projection script's `compute_launch_scope_reconciliation` is a deliberately SEPARATE, post-build routine reading the asset's already-stored (masked) axes -- both report the identical shape and both delegate to the SAME `is_public` conjunction, so there is still only ONE eligibility rule in the tree even though there are two reporting call sites.

## Deviations from Plan

### Auto-fixed / documented adjustments (Rule 2 -- filling a gap the plan left implicit)

**1. [Rule 2] `discovery_identification`'s main_pool bucketing needed a concrete implementation the plan's own read_first pointers did not yet provide in code.**
- **Found during:** Task 2
- **Issue:** The plan's Task 2 action text requires `discovery_identification` rows to be "re-derived over the claims that survive the projection" including `main_pool`/`main_pool_reason`, but the actual main-pool bucketing predicate (owner rulings D-13c/D-13d/D-13e, documented in `.claude/skills/sketch-findings-genizahsearch/references/main-pool-rule.md`) is owned by plans 136-07/136-11/136-12, none of which had landed a shared, importable module (`shared/discovery_main_pool.py`) in this worktree.
- **Fix:** Implemented a simple, explicit, clearly-labeled stand-in heuristic (human_confirmed > multi-folio > full-coverage > insufficient_length) sufficient to make the CLOSED-GRAPH / RECOMPUTE-NOT-COPY structural properties this plan actually owns (VIS-01) genuinely testable, with an in-code comment block and this SUMMARY entry flagging it for 136-11/136-12 to reconcile by importing the real module once it exists, rather than reimplementing bucket logic a second time.
- **Files modified:** `scripts/project_discovery_public.py` (`_recompute_identification_row`)
- **Verification:** `test_discovery_identification_recomputed_from_public_claims_only` proves the STRUCTURAL property this plan is responsible for (a private sibling claim cannot leak into the public aggregate) independent of the exact bucketing predicate's fidelity to the eventual real rule.
- **Commit:** `b94db6fd`

No other deviations. The plan's stated tasks, acceptance criteria, and verification commands were otherwise followed as written.

## Ruling F/H interaction (phase context)

This plan's visibility axes (public/private masking, D-22) are orthogonal to owner rulings F and H (the novelty-shade DEFAULT-VISIBILITY rules for `diverges_work`/`diverges_part` hidden-by-default vs `container_predicts` shown-normally): VIS-01 governs whether a row is STRUCTURALLY PRESENT in the public artifact at all, while F/H govern whether a PRESENT row is rendered by default or behind a toggle on the panel/findings-page UI (owned by plans 136-15/136-16/136-17/136-18, not this plan). This plan's `discovery_identification` recomputation carries a `novelty_status` field through (pass-through of the winning surviving evidence row's own value, whatever shade vocabulary -- seven/eight/nine/ten-value -- the real build populates, since this script never hardcodes or filters on that vocabulary), but implements NO default-visibility/hiding logic of its own -- there is nothing in this plan's code that could contradict, generalize, or narrow rulings F or H's hiding/showing rules. Confirmed no conflict.

## F-05 disposition (per this plan's own `<success_criteria>`)

This plan implements only ONE of the THREE independent controls F-05 requires: private rows are structurally absent from the projected artifact (proven by Control 1 and the baseline test). It does NOT make a private-audience artifact unloadable by a public route (plan 136-20's job) and does NOT place the private database on the public host or refrain from doing so (plan 136-13's job, in the deploy/gate-battery sense). F-05 is honored only when all three controls hold together -- a correct projection deployed alongside the private database would satisfy this plan's own tests while still violating the requirement in production. This is stated explicitly here so 136-13/136-20 do not treat this plan's green tests as sufficient on their own.

## Verification Results

- `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_visibility.py tests/test_vis01_projection.py -q` -> 86 passed.
- `python scripts/project_discovery_public.py --help` -> documents both positional paths (`private_db`, `public_db_out`).
- `python scripts/check_atlas_masking.py --scan-asset shared/discovery_visibility.py` -> clean.
- `python scripts/check_atlas_masking.py --scan-asset scripts/project_discovery_public.py` -> clean.
- `python scripts/check_atlas_masking.py --scan-asset tests/test_vis01_projection.py` -> clean.
- `python scripts/check_atlas_masking.py --scan-repo` (with `MASKING_SCAN_PATTERNS_FILE` set) -> exits 0, clean. **This criterion IS MET on this run** (the pattern file was present locally; the "not met" fallback path in this plan's `<verification>` section was not needed).
- `python -m ruff check shared/discovery_visibility.py scripts/project_discovery_public.py tests/test_discovery_visibility.py tests/test_vis01_projection.py` -> all checks passed.
- `grep -n "is_public" scripts/project_discovery_public.py` -> exactly two call sites, both delegating to `shared.discovery_visibility.is_public`; no inline conjunction restatement.

## Known Stubs

None that block this plan's own goal. The one documented simplification (`_recompute_identification_row`'s main_pool bucketing heuristic, see Deviations above) is NOT a stub in the "empty/placeholder UI" sense -- it produces real, internally-consistent values satisfying every schema CHECK constraint and every acceptance criterion this plan owns; it is flagged only because its BUSINESS-RULE FIDELITY to the eventual `shared/discovery_main_pool.py` (owned by 136-07/136-11/136-12) has not yet been cross-checked against that module, which does not exist yet in this tree.

## Threat Flags

None. This plan's own `<threat_model>` register (T-136-08-01 through T-136-08-06, T-136-08-SC) already enumerates every trust boundary this plan's files touch (the private-asset-to-public-artifact packaging boundary, the public-artifact-to-any-public-surface boundary, and the restricted-provenance-to-derived-enum boundary); no new files or surfaces outside that register were introduced.

## Self-Check: PASSED

- `shared/discovery_visibility.py` -- FOUND
- `scripts/project_discovery_public.py` -- FOUND
- `tests/test_discovery_visibility.py` -- FOUND
- `tests/test_vis01_projection.py` -- FOUND
- Commit `4328b9e3` -- FOUND in `git log`
- Commit `bd42d389` -- FOUND in `git log`
- Commit `b94db6fd` -- FOUND in `git log`
- Commit `382a7238` -- FOUND in `git log`

## Next Phase Readiness

- Plans 136-11/136-12 (build wiring) should import `shared/discovery_visibility.py`'s `assertion_visibility`/`identity_visibility`/`is_public` directly when computing the STORED axes on the real asset, rather than re-deriving the conjunction; and should reconcile `scripts/project_discovery_public.py`'s documented main-pool-bucketing stand-in against `shared/discovery_main_pool.py` once that module lands.
- Plan 136-13's gate battery has a working `reconcile_launch_scope` (build-time) and `compute_launch_scope_reconciliation` (post-build) to put a real symmetric-difference number in front of the owner.
- Plan 136-20 (runtime loader `meta.audience` gate) and plan 136-13 (deploy/gate-battery placement) still owe the OTHER two of F-05's three independent controls -- see the F-05 disposition note above.
- No blockers.

---
*Phase: 136-read-surfaces-connections-panel-work-witnesses*
*Completed: 2026-08-02*
