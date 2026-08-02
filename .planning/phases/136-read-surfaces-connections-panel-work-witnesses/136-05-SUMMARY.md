---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 05
subsystem: infra
tags: [discovery, rebuild-preservation, masking, sqlite, cert01, precision-gate]

# Dependency graph
requires:
  - phase: 136-01
    provides: "docs/specs/discovery-sidecar-schema-v1.md's Amendment 2026-08-02 (Phase 136) -- the exact new-field contract this gate's allowlist implements against"
  - phase: 135-09
    provides: "cert01_prereg.json (population_hash/cluster_map_hash/db_content_hash/stratum_counts already pinned for the live v2 asset) + scripts/cert01_frame.py's frozen recipes"
provides:
  - "scripts/verify_rebuild_preservation.py -- the old/new allowlisted full-table diff harness over the six core discovery tables, streamed in PK order, plus a dedicated band_precision D-02a check and a CERT-01 card-binding check"
  - "136-REBUILD-PRESERVATION-EXPECTED.json -- pinned, REAL, from the currently-live production v2 asset (not a placeholder), before any rebuild exists"
  - "tests/test_rebuild_preservation.py -- baseline PASS + seven positive controls proving the gate can fail"
affects: [136-06, 136-11, 136-12, 136-13]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Streamed old/new allowlisted table diff via lazy sqlite3 cursor iteration (never .fetchall()) -- O(1) memory per table regardless of row count"
    - "Allowlist-provenance self-check: read the frozen contract doc at runtime and assert every allowlisted column is actually cited there"
    - "db_content_hash cross-check as the structural closer for the F-04 candidate-sourced-expectation failure mode (no special-case logic needed -- it falls out of the existing provenance check)"

key-files:
  created:
    - scripts/verify_rebuild_preservation.py
    - .planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-REBUILD-PRESERVATION-EXPECTED.json
    - tests/test_rebuild_preservation.py
  modified: []

key-decisions:
  - "band_precision is checked via a SEPARATE, dedicated small-table full-scan (check_band_precision_authorized_change), not folded into the six-table streamed mechanism -- it carries exactly ONE authorized ROW-level exception (tier_a measurement_status/ci_low, D-02a) rather than a column-level allowlist, and the plan's own acceptance criteria names exactly six tables for the streamed diff while separately requiring a band_precision positive control (Task 3, control 5)"
  - "Generated the REAL 136-REBUILD-PRESERVATION-EXPECTED.json from the actual currently-live production asset (discovery-v1-33499c5b...db, 388MB) and the actual research corpus (fullcorpus_v2.db, 3GB), both reachable read-only from this worktree via absolute path outside the isolated tree -- never fabricated placeholder hashes. Independently cross-verified: db_content_hash/frame_content_hash/population_hash/cluster_map_hash/stratum_counts all match the values already pinned in cert01_prereg.json and docs/specs/discovery-frames-v2.md for the SAME asset"
  - "population_hash/cluster_map_hash/stratum_counts are computed via cert01_frame.compute_estimand_rows (imported, never reimplemented) -- reusing the SAME frozen recipe the CERT-01 pre-registration already used, rather than inventing a second population definition for this gate"
  - "The CERT-01 card-binding check resolves each graded card's binding via cert01_frame's frozen _RANKED_ESTIMAND_SQL_TEMPLATE (imported, never re-derived), reading the STORED discovery_claim.display_evidence_id rather than recomputing display selection -- so a repoint is caught even though the ranking algorithm itself is untouched"

patterns-established:
  - "Streamed old/new allowlisted table diff (compute_table_hash for a cheap PASS/FAIL short-circuit, _first_diff's dual-cursor merge only when hashes disagree, to localize the first differing row/column without ever materializing a full table)"

requirements-completed: []

# Metrics
duration: 40min
completed: 2026-08-02
---

# Phase 136 Plan 05: Rebuild-Preservation Gate Summary

**A streamed old/new allowlisted diff harness over the six core discovery tables (plus a dedicated `band_precision` D-02a check and a CERT-01 card-binding check), with its expectation REALLY pinned from the currently-live 297K-evidence-row production asset -- verified against `cert01_prereg.json`'s independently-computed values for the same asset, and proven able to fail seven distinct ways.**

## Performance

- **Duration:** 40 min
- **Started:** 2026-08-02T09:15:00Z (approx.)
- **Completed:** 2026-08-02T09:54:38Z
- **Tasks:** 3 completed
- **Files modified:** 3 (all new)

## Accomplishments

- `scripts/verify_rebuild_preservation.py`: the CLI shape (`<old_db> <new_db> --expected <pinned.json>`) mirrors `scripts/verify_discovery_sidecar.py`; imports `population_hash`/`cluster_map_hash`/`hash_file` from `scripts/cert01_frame.py` and `compute_frame_content_hash` from `scripts/build_discovery_sidecar.py` (no reimplementation, confirmed by grep in the automated Task-1 verify). Streams every row of `works`, `discovery_claim`, `discovery_evidence`, `witness_units`, `witness_unit_members`, `discovery_routing_audit` in primary-key order, projecting out a per-table allowlist cited by `docs/specs/discovery-sidecar-schema-v1.md`'s `## Amendment 2026-08-02 (Phase 136)` section letter, and folds the projection into a running SHA-256 (never `.fetchall()`'d -- O(1) memory per table). On a hash mismatch, a dual-cursor lockstep re-scan locates the FIRST differing row/column, reporting only the table, primary key, and column NAME (never a cell value). `check_allowlist_provenance()` reads the schema doc at runtime and asserts every allowlisted column is actually cited there.
- A dedicated `band_precision` check (`check_band_precision_authorized_change`): every row byte-identical old vs new EXCEPT the one authorized `tier_a` row's `measurement_status`/`ci_low` pair (D-02a); `precision` must stay NULL under all circumstances.
- A CERT-01 card-binding check (`resolve_card_bindings`/`check_card_binding`): for every graded card, resolves `claim_id`/`display_evidence_id`/`span_start`/`span_end`/`snapshot_hash` via `cert01_frame`'s frozen ranked-display SQL template (imported, never re-derived) against both assets and asserts byte-identical bindings.
- `136-REBUILD-PRESERVATION-EXPECTED.json`: generated via `--generate` against the ACTUAL currently-deployed production sidecar (`discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff.db`, 388MB, 268,361 claims / 297,415 evidence rows / 6,270 routing-audit rows) and the actual research corpus (`fullcorpus_v2.db`, 3GB) -- both reachable read-only via absolute path from this worktree. The generated `db_content_hash`, `frame_content_hash`, `population_hash`, `cluster_map_hash`, and `stratum_counts` all match the values already independently pinned in `cert01_prereg.json` and `docs/specs/discovery-frames-v2.md` for the identical asset -- confirming both the correctness of this new computation and that it is genuinely the SAME live asset. Masking scan (`--scan-asset`) exits 0; `git diff --stat` on `scripts/verify_cert01_grading.py` is empty.
- `tests/test_rebuild_preservation.py`: a fabricated fixture pair (via `build_discovery_sidecar.create_schema`, imported) differing ONLY via allowed changes passes the gate cleanly (baseline, 0 violations across all six PASS lines), then seven positive controls each produce a nonzero exit naming the check that fired (violation counts: control 1 = 1, control 2 = 3, control 3 = 2, control 4 = 1, control 5 = 1, control 5b = 2, control 6 card-binding = 4, control 7 = 1). Control 1 independently asserts `compute_frame_content_hash` is unchanged by a `matched_letters`-only mutation while the new gate still fires. Control 7 (expectation sourced from the candidate) fails via the `db_content_hash` provenance cross-check -- a structural, unconditional closer for Codex F-04 that required no special-case code (the expectation generated from the candidate simply carries the candidate's hash, which can never equal the old asset's).

## Task Commits

Each task was committed atomically:

1. **Task 1: The allowlisted old/new full-table diff harness** — `b762d1cd` (feat)
2. **Task 2: Pin the expectation from the currently-live asset, plus the CERT-01 card binding check** — `af3c3f02` (feat)
3. **Task 3: Prove the gate can fail — positive controls over a fixture pair** — `f3eac4e4` (test)

## Files Created/Modified

- `scripts/verify_rebuild_preservation.py` — the diff harness, CLI, band_precision check, card-binding check, allowlist-provenance self-check, and `--generate` mode
- `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-REBUILD-PRESERVATION-EXPECTED.json` — the pinned, REAL expectation artifact
- `tests/test_rebuild_preservation.py` — baseline + seven positive controls + generator/provenance unit tests

## Decisions Made

See `key-decisions` in the frontmatter above. The most consequential: generating the REAL expectation artifact from the actual live production asset (rather than a synthetic placeholder) was possible because the live `discovery_data/` and `same_work_spike/probe/data/fullcorpus_v2.db` are reachable read-only via absolute path from this isolated worktree (they live in the main checkout, gitignored, machine-local) — this was the only way to satisfy the plan's own must-have truth ("the expectation is pinned from the CURRENTLY-LIVE asset, before the rebuild exists") honestly rather than fabricating placeholder hashes. Every value was cross-checked against two independent, already-committed sources (`cert01_prereg.json`, `docs/specs/discovery-frames-v2.md`) computed by an earlier phase, and matched exactly.

## Deviations from Plan

None — plan executed exactly as written. `band_precision` handling was designed as a separate dedicated check rather than folded into the six-table streamed loop; this is a direct, literal reading of the plan's own text (Task 1's acceptance criteria names exactly six tables for the streamed mechanism, while Task 3's control 5 separately requires a `band_precision` positive control) rather than a deviation from it.

## Issues Encountered

None. The one open question during execution — whether the live production discovery.db/research corpus would be reachable from this isolated worktree at all — resolved favorably: both are present on the machine outside the worktree's tracked tree (gitignored, machine-local data), reachable read-only via absolute path, so the artifact could be generated for real rather than deferred.

## User Setup Required

None — no external service configuration required. (Operational note for future deploys: the `136-REBUILD-PRESERVATION-EXPECTED.json` committed here is now the authoritative external frame-hash pin for the Phase-136 rebuild per `docs/specs/discovery-deploy.md`'s Amendment 2026-08-02 note — a future plan should update that note to point at this artifact by name, superseding the interim `discovery-frames-v2.md` §1 pin.)

## Next Phase Readiness

- The rebuild-preservation gate is ready to run the moment a rebuilt candidate `discovery.db` exists (plan 136-06+): `python scripts/verify_rebuild_preservation.py <old_db> <new_db> --expected .planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-REBUILD-PRESERVATION-EXPECTED.json --research-db <fullcorpus_v2.db path> --cert01-cards <graded-cards.json>`.
- No blockers. `scripts/verify_cert01_grading.py`'s check 10 stays untouched (D-02c) — the rebuilt asset will need its own separate compatibility attestation (plan 136-13), not a change to that check.

---
*Phase: 136-read-surfaces-connections-panel-work-witnesses*
*Completed: 2026-08-02*

## Self-Check: PASSED
