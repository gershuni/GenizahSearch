---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 20
subsystem: discovery-runtime-loader
tags: [vis-01, audience, readiness-contract, fail-closed, rollback-safety, discovery-sidecar]

# Dependency graph
requires:
  - phase: 136-01
    provides: "The Amendment 2026-08-02 contract: meta.audience's closed public|private enum, the two new tables, their release-contract count meta keys, and the new columns on existing tables"
  - phase: 136-08
    provides: "scripts/project_discovery_public.py, which writes meta.audience='public' on the projected artifact (and asserts it post-build) -- the value this loader now gates on"
provides:
  - "web/discovery_assets.py: the VIS-01 audience boundary -- a public loader can only ever resolve a PUBLIC artifact; private/missing/empty/unrecognised all fail closed identically"
  - "web/discovery_assets.py: the extended readiness contract -- discovery_identification + manuscript_display are required tables with release-contract count pairs, and _REQUIRED_COLUMNS validates every Amendment-2026-08-02 column via PRAGMA table_info as a subset check"
  - "tests/test_discovery_assets_audience.py: 32 tests -- the audience matrix, the partial-asset/dropped-column matrix, the rollback fixture, and the end-to-end proof that no public read path returns a row from a private artifact"
  - "tests/fixtures/discovery_v2_fixture.py: a reusable post-rebuild sidecar fixture builder (upgrades the committed v1 golden fixture to the Amendment 2026-08-02 shape; defect shapes built UP via omit_tables/omit_columns/extra_columns/meta_overrides/omit_meta_keys)"
affects: [136-11, 136-12, 136-13, 136-19]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Two independent gates on one artifact: the manifest decides WHICH file (unchanged sibling-ignoring resolution, the rollback-safety property), and the audience enum decides what that file is ALLOWED to contain. Neither substitutes for the other."
    - "Fail-closed reason logging that never interpolates the rejected value -- the loader is by definition looking at something it has just decided not to trust, and the reason is fully expressible without echoing any of its content."
    - "Defect fixtures built UP rather than torn DOWN: the fixture builder takes omit_tables/omit_columns/extra_columns knobs so a 'missing column' shape is created by not writing the column, avoiding a dependency on SQLite's version-gated ALTER TABLE ... DROP COLUMN."
    - "A DYNAMIC read-path sweep that refuses to skip: the end-to-end refusal test enumerates every public async reader in web/discovery.py and raises on an unregistered required parameter, so a later plan adding a read path is forced to register it rather than quietly escaping the VIS-01 proof."

key-files:
  created:
    - tests/test_discovery_assets_audience.py
    - tests/fixtures/discovery_v2_fixture.py
  modified:
    - web/discovery_assets.py
    - tests/test_discovery_loader.py
    - tests/test_discovery_flag.py
    - tests/test_discovery_composition.py

key-decisions:
  - "The audience gate is placed in the READINESS path, not in _resolve_versioned_db. Resolution stays exactly as it was (manifest-named file only, siblings ignored) because that is the rollback-safety property and the single selection point; the audience check is a second, independent gate on the CONTENT of whatever resolution selected. git diff on _resolve_versioned_db is empty."
  - "The refusal message never interpolates the rejected audience value. A tampered artifact's meta.audience is attacker-controllable text, and echoing it would put artifact content into the log of the very artifact the loader just refused. A test seeds a distinctive marker into both a content cell and the audience value and asserts neither reaches the log."
  - "_EXPECTED_SCHEMA_VERSION is NOT bumped -- retained at 'discovery-v1', with the decision and its three reasons recorded in a comment beside the constant. Bumping would require lockstep edits to scripts/build_discovery_sidecar.py::SCHEMA_VERSION and the schema document, both owned by same-wave plan 136-11."
  - "_REQUIRED_COLUMNS includes divergence_correctness on discovery_evidence, which the plan's inline enumeration omits. That column was ADDED to the amendment by the 136-03 continuation (owner ruling F) AFTER plan 136-20 was drafted; the plan's governing instruction is 'every column the Amendment 2026-08-02 adds', and omitting it would have left open exactly the partial-builder hole this contract exists to close. Rule 2 (missing critical functionality). Both the loader constant and the test's independent restatement carry a comment recording why."
  - "discovery_routing_audit is named in _REQUIRED_COLUMNS (for demoted_work_id, made contractual by amendment section F) but deliberately NOT added to _REQUIRED_TABLES. Naming it in the column map makes it effectively required anyway -- PRAGMA table_info on an absent table returns no rows, so its required column reads as missing -- and the code comment says so explicitly rather than leaving that as a trap. The builder creates the table unconditionally and the projection replays the private DB's own DDL, so a correct post-rebuild asset always carries it."
  - "The end-to-end refusal test sweeps read paths DYNAMICALLY instead of freezing a list. The related-page COUNT wrapper and the corpus-wide findings reader do not exist yet (later Phase-136 plans), so a hardcoded list would either be incomplete or would fail. The sweep raises on an unregistered required parameter, converting 'a future reader silently escapes the proof' into a loud test failure."

patterns-established:
  - "The rollback case is pinned TWICE, deliberately: once as 'the pre-rebuild asset fails readiness', and once as 'the pre-rebuild asset still fails with a public audience marker stamped on'. The second is the evidence for the retain-discovery-v1 decision -- it takes the audience gate out of the picture and shows the structural checks refusing the asset on their own."

requirements-completed: [VIS-01, NOVEL-02, PANEL-01, PANEL-02]

# Metrics
duration: 70min
completed: 2026-08-03
---

# Phase 136 Plan 20: The Audience Boundary and the Extended Readiness Contract Summary

A publicly reachable route can no longer resolve a private discovery artifact even if the manifest
names one, and a partial, rolled-back or pre-rebuild sidecar now leaves the surfaces hidden rather
than half-working.

## What shipped

**The audience boundary (Task 1).** `web/discovery_assets.py` gained a closed module-level enum
(`_AUDIENCES = {"public", "private"}`, `_PUBLIC_LOADER_AUDIENCE = "public"`) and a readiness-path gate
on `meta.audience`, using the same reject-incompatible idiom the `schema_version` check already used.
Private, missing, empty and unrecognised values all fail closed identically — the default is closed,
never open. `_DiscoveryState` gained an `audience` field, set only on a ready state and reachable
through the existing `discovery_meta("audience")` accessor, so a later diagnostic or admin surface can
report which artifact is live without reopening the database.

`_resolve_versioned_db` is untouched. `git diff 8246efbb..HEAD -- web/discovery_assets.py` contains no
reference to `_resolve_versioned_db`, `asset_basename` or the sibling-ignoring logic. That is the
rollback-safety property and the single selection point; this plan added a second, independent gate on
the CONTENT of whatever the manifest selected, not a change to selection.

**The extended readiness contract (Task 2).** `_REQUIRED_TABLES` gained `discovery_identification` and
`manuscript_display`; `_REQUIRED_META_KEYS` and `_RELEASE_CONTRACT_COUNTS` gained
`expected_rows_discovery_identification` and `expected_rows_manuscript_display`, fed through the
existing count loop with no new mechanism. A new `_REQUIRED_COLUMNS` `{table: frozenset(columns)}`
mapping is validated per table via `PRAGMA table_info` as a SUBSET check.

**The end-to-end proof (Task 3).** With `manifest.json` pointing at a private-audience database and the
flag ON, every public async reader in `web/discovery.py` returns its unavailable envelope. The inverse
control proves the same paths return rows against a valid public artifact, so the refusal test cannot
pass vacuously.

## The required-table, required-COLUMN and count checks were verified to carry the whole weight

This is the evidence the retain-`discovery-v1` decision rests on, and it is stated here because the
decision is only defensible if the checks actually do the work a marker bump would have done.

| Fixture | What it proves | Test |
|---|---|---|
| Pre-rebuild shape (the committed golden v1 fixture, untouched) | A rollback to the currently-live asset leaves `discovery_available()` False, without raising | `test_pre_rebuild_asset_fails_readiness_so_a_rollback_hides_cleanly` |
| Pre-rebuild shape **with a `public` audience marker stamped on** | The audience gate is taken out of the picture and the asset is STILL refused — by the required-table / required-COLUMN / count contract alone | `test_pre_rebuild_asset_fails_on_the_structural_checks_alone` |
| Post-rebuild shape minus `discovery_identification.max_coverage_ppm` | A dropped column on a NEW table fails readiness | `test_missing_required_column_on_a_new_table_fails_readiness` |
| Post-rebuild shape minus `discovery_evidence.coverage_ppm` | A dropped column on a PRE-EXISTING table fails readiness — the case `_REQUIRED_TABLES` structurally cannot catch | `test_missing_required_column_on_an_existing_table_fails_readiness` |
| Post-rebuild shape plus two unknown columns | The column check is a SUBSET check; a future additive build is not gratuitously rejected | `test_extra_unexpected_column_does_not_fail_readiness` |

The second row is the one that matters. Without it, "the table and count checks carry the whole weight"
would have been an assertion rather than a measurement — the pre-rebuild asset also lacks
`meta.audience`, so the plain rollback test alone could have been passing entirely on the audience gate.

## Read paths that do not exist yet — carry into plan 136-19's sweep

The plan's Task 3 names four wrappers. Two of them do not exist in `web/discovery.py` at this plan's
execution time and are therefore NOT covered by a named assertion here:

1. **The related-page COUNT wrapper** — `web/discovery.py` exposes `get_pages_related_to_page` (the
   list) but no count counterpart. Added by a later Phase-136 plan.
2. **The corpus-wide findings reader** — no findings-page reader exists yet.

Rather than freeze a list that would go stale, the refusal test sweeps every public async reader in
`web/discovery.py` DYNAMICALLY and raises on a reader whose required parameter it does not recognise.
So when either path lands, it is automatically swept — and if it takes a new parameter shape, the test
fails loudly and forces registration rather than silently skipping it. **Plan 136-19's sweep should
still add its own explicit assertion for these two paths by name**, per this plan's own instruction.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 — Blocking] The committed golden fixture is a PRE-REBUILD asset, so extending the
contract broke six pre-existing ready-path assertions**

- **Found during:** Task 1 (the audience gate alone was enough to trip it; Task 2 would have tripped it
  again).
- **Issue:** `tests/fixtures/discovery/discovery-v1-fixture.db` predates the Amendment 2026-08-02 — no
  `meta.audience`, no `discovery_identification`/`manuscript_display`, none of the new columns. Six
  assertions across three modules asserted `load_discovery_state() is True` against it:
  `tests/test_discovery_loader.py` (2), `tests/test_discovery_flag.py` (1),
  `tests/test_discovery_composition.py` (3). This is not incidental breakage — it is the plan's own
  stated intent ("the PRE-REBUILD asset ... fails readiness under the new contract") landing on a
  fixture the plan's `files_modified` did not anticipate.
- **Fix:** added `tests/fixtures/discovery_v2_fixture.py`, which copies the golden fixture and upgrades
  it to the Amendment 2026-08-02 shape. The three modules now materialize an upgraded copy in
  `tmp_path` instead of reading the committed (pre-rebuild) directory in place. The committed golden
  fixture itself is UNCHANGED — deliberately, because it is a cross-plan shared artifact, its
  regeneration would need builder DDL owned by same-wave plan 136-11, and seven other test modules read
  it directly.
- **Also fixed, same class:** `tests/test_discovery_loader.py::_build_minimal_db` now runs the upgrade
  as its last step. Without that, its nine defect tests (`test_content_hash_mismatch`,
  `test_row_count_mismatch_fails_closed`, …) would have started passing on a missing `audience` key
  rather than on the defect each one names — green for the wrong reason.
- **Files modified:** `tests/test_discovery_loader.py`, `tests/test_discovery_flag.py`,
  `tests/test_discovery_composition.py`, `tests/fixtures/discovery_v2_fixture.py`.
- **Commits:** `fc2a5145` (the three modules), `8552beca` (the fixture builder).
- **No same-wave overlap:** no other Phase-136 plan names any of these files. Verified by grep across
  all 21 `136-*-PLAN.md`.

**2. [Rule 2 — Missing critical functionality] `divergence_correctness` added to `_REQUIRED_COLUMNS`**

- **Found during:** Task 2, reading the amendment rather than the plan's inline enumeration.
- **Issue:** the plan enumerates nine existing-table columns and Codex round 4 confirmed that list
  complete — but the schema doc's own 136-03 continuation amendment (owner ruling F, dated the same
  day, after 136-20 was drafted) ADDS `divergence_correctness` to `discovery_evidence`. The plan's
  governing instruction is "covering every column the Amendment 2026-08-02 adds".
- **Fix:** included in `_REQUIRED_COLUMNS["discovery_evidence"]` and in the test's independent
  restatement, each with a comment recording why it is there despite being absent from the plan's list.
  Confirmed consistent with plan 136-08's own fixture schema (`tests/test_vis01_projection.py`), which
  already carries the column.
- **Commit:** `808239b1`.

### Deliberately not done

- **The committed golden fixture was NOT regenerated.** See deviation 1.
- **`scripts/build_discovery_sidecar.py` was NOT touched.** Owned by same-wave plan 136-11; this plan's
  column check is precisely what lets the loader close the partial-schema hole without a builder or
  schema-doc edit.
- **`_EXPECTED_SCHEMA_VERSION` was NOT bumped.** Decided in the plan, implemented as decided, reasoning
  recorded beside the constant.
- **`discovery_routing_audit` was NOT added to `_REQUIRED_TABLES`.** See key-decisions.

## Threat model coverage

| Threat ID | Disposition | Where mitigated |
|---|---|---|
| T-136-20-01 | mitigated | Audience gate in the readiness path + `test_private_artifact_returns_no_row_on_any_public_read_path` (dynamic sweep over every public reader) |
| T-136-20-02 | mitigated | Missing/empty/unrecognised all fail closed; `test_missing_audience_key_fails_closed_never_treated_as_public` + 7-case parametrized unrecognised-value test |
| T-136-20-03 | mitigated | `_REQUIRED_TABLES` + `_RELEASE_CONTRACT_COUNTS` + `_REQUIRED_COLUMNS`; missing-table, count-disagreement and both dropped-column tests |
| T-136-20-04 | mitigated | The two rollback fixtures (see the table above) |
| T-136-20-05 | mitigated | The refusal never interpolates the rejected value; `test_refusal_is_logged_with_reason_and_no_row_content` seeds a marker into a content cell AND into the audience value and asserts neither reaches the log |
| T-136-20-06 | mitigated | `test_sibling_ignoring_resolution_is_unchanged` + an empty `git diff` on the resolution logic |
| T-136-20-SC | n/a | No package installs in this plan |

## Verification

| Command | Result |
|---|---|
| `pytest tests/test_discovery_assets_audience.py -q` | **32 passed** |
| `pytest tests/ -k "discovery_assets" -q` | **32 passed, 3 skipped** |
| `pytest tests/ -k "discovery" -q` | **649 passed, 8 skipped** — no pre-existing loader/service/build test regressed |
| `pytest tests/test_vis01_projection.py tests/test_no_raw_storage_access.py tests/test_no_await_sync_function.py -q` | **31 passed** |
| Task 1 source check (`'audience' in web/discovery_assets.py`) | OK |
| Task 2 source check (`discovery_identification` + `manuscript_display` + `_REQUIRED_COLUMNS` + `table_info`) | OK |
| `ruff check` on all six touched files | clean |

## Masking (D-25)

Every value in the new test and fixture modules is fabricated/synthetic. No restricted corpus is named
anywhere; the codename discipline holds (M-source / R-source only). The refusal-log test asserts
POSITIVELY that no cell value and no raw `meta.audience` token reaches the log — the one place where a
refused private artifact could otherwise have leaked its own content into an operator-visible surface.

**Not run in this worktree:** `scripts/check_atlas_masking.py --scan-repo` requires the gitignored
`.masking_patterns` file (`MASKING_SCAN_PATTERNS_FILE`), which by design does not exist in an executor
worktree. This is a worktree limitation, **not** a masking-gate failure. Plan 136-19's sweep runs the
real gate on the main tree.

## Known stubs

None.

## Threat flags

None. This plan adds no network endpoint, no auth path, no file access pattern and no schema change —
it only tightens an existing startup validation and adds a refusal.

## Commits

| Commit | Message |
|---|---|
| `8552beca` | test(136-20): failing audience-boundary tests + post-rebuild fixture builder |
| `fc2a5145` | feat(136-20): the VIS-01 audience boundary — a public loader refuses a private artifact |
| `808239b1` | feat(136-20): extend the readiness contract to the two new tables, their counts and their columns |
| `329cab8c` | test(136-20): prove no public read path reaches a private artifact, end to end |

## Self-Check: PASSED

All four declared artifacts exist on disk (`web/discovery_assets.py`,
`tests/test_discovery_assets_audience.py`, `tests/fixtures/discovery_v2_fixture.py`, this SUMMARY), and
all four commits are present in `git log`.
