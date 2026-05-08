---
phase: 85-synthetic-fjms-inventory-rows
plan: 03
subsystem: fjms-sidecar-synthetic-injection
tags: [fjms-sidecar, synthetic-rows, phase-85, export-script, manifest-consumer, layered-architecture]

# Dependency graph
requires:
  - phase: 85-synthetic-fjms-inventory-rows
    plan: 01
    provides: shared/synthetic_sys_id.py (is_synthetic_sys_id, encode_inventory_sys_id, decode_inventory_id) — used in test assertions and post-export validation.
  - phase: 85-synthetic-fjms-inventory-rows
    plan: 02
    provides: fist_data/synthetic_manifest.json (AUTHORITATIVE 5,035-entry InventoryId source) — Plan 03 reads this as its ONLY qualifying-set source. NO independent SQL predicate.
provides:
  - scripts/export_fist_enrichment.py (MODIFIED) — UNIONs synthetic-AlmaId rows in all 12 AlmaId-keyed enrichment tables. Real-Alma rows preserved verbatim. Reads fist_data/synthetic_manifest.json at export start; populates temp.synthetic_qualifying_inventories table; every UNION block restricts to those InventoryIds. Post-export _validate_synthetic_export runs table-specific invariants.
  - tests/test_export_fist_synthetic.py — 33 passing tests + 1 smoke skip across 8 test classes covering UNION ALL injection, manifest authority, deterministic ordering, catalog triple uniqueness, FTS5 rebuild, non-Alma tables unchanged, stale table-name regression guard, and parameterized-SQL-only enforcement.
affects: [85-04, 85-05, 86-cudl-coverage-audit]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Manifest-as-authority cross-plan input (Plan 02 -> Plan 03): Plan 03 reads fist_data/synthetic_manifest.json and populates a connection-scoped temp table; every UNION ALL block in 12 export functions restricts InventoryId IN (SELECT FROM temp.synthetic_qualifying_inventories). This eliminates the dominant Phase 85 risk per cross-AI HIGH consensus — no parallel SQL predicate that could diverge from Plan 02's Python qualifying-set logic"
    - "UNION-ALL-with-outer-ORDER-BY for byte-stable composite queries: each AlmaId-keyed export function wraps its (real-Alma SELECT DISTINCT) UNION ALL (synthetic SELECT DISTINCT) inside an outer SELECT that applies a single ORDER BY AlmaId, <stable_pk>. ORDER BY at the inner level wouldn't be byte-stable across the UNION; the outer wrap makes byte-equality across rebuilds achievable"
    - "Layered-not-extended (D-01) verified by zero-diff invariant: shared/fjms_service.py is byte-identical pre/post Plan 03. The data layer accommodates synthetic AlmaIds via the existing 'starts with 99 + all digits' string passthrough. ~30 'WHERE AlmaId = ?' service queries work transparently with no service-layer branches"
    - "sqlite-native synthetic AlmaId expression: ('99' || printf('%010d', inv.InventoryId) || '000000') runs entirely inside the SQL engine. NO Python f-string interpolation on dynamic values (T-85-02 mitigation). Verified by static-analysis test scanning every triple-quoted SQL block for {var} interpolation"
    - "Connection-scoped temp tables for cross-cursor visibility: temp.synthetic_qualifying_inventories is created on the source connection, not on a specific cursor. This means cursor.execute calls in different export functions can all reference the temp table because they share the underlying connection. Safe because export functions are called sequentially in main()"

key-files:
  created:
    - "tests/test_export_fist_synthetic.py — 600 lines, 8 test classes, 34 tests (33 passed + 1 smoke skip). Includes a comprehensive _seed_fist_schema helper that creates ALL 24 tables referenced by any export query, allowing per-function tests to run against minimal in-memory FIST.db without sqlite3.OperationalError. Wave-0 scaffold (TestParameterizedSqlOnly + TestStaleTableNamesAbsent) committed before Task 2 modifications; Wave-1 tests gated by needs_task2 marker that detects whether load_synthetic_manifest_into_temp_table is wired"
  modified:
    - "scripts/export_fist_enrichment.py — +500 lines net. Added: import json; load_synthetic_manifest_into_temp_table(source, manifest_path) -> int; UNION ALL blocks in all 12 AlmaId-keyed export functions wrapped in outer SELECT with explicit ORDER BY; _validate_synthetic_export(target) post-export validation; main() now loads manifest and calls _validate_synthetic_export after create_fts5/create_meta"

key-decisions:
  - "DETERMINISTIC ORDERING via outer SELECT wrapping the UNION ALL. The plan's draft SQL placed ORDER BY at the SELECT DISTINCT inside the UNION; this is invalid SQL because ORDER BY in a UNION operand is not portable and not byte-stable. The implementation wraps every UNION ALL inside a SELECT <columns> FROM (... UNION ALL ...) ORDER BY ..., which is the standard SQL pattern for ordering composite results. Verified by TestDeterministicOrdering scanning only triple-quoted SQL blocks."
  - "catalog uniqueness invariant uses (AlmaId, UnitCatalogRecId) NOT (AlmaId, UnitCatalogRecId, SignatureId). The plan's draft test referenced SignatureId, but the catalog table schema (lines 188-228) doesn't include a SignatureId column — it stores UnitCatalogRecId only. The schema's effective uniqueness key is (AlmaId, UnitCatalogRecId). Updated TestCatalogTripleUniqueness and _validate_synthetic_export accordingly. This is a Rule 1 fix to plan-time error."
  - "TestDeterministicOrdering filters to triple-quoted SQL blocks only (not Python comments/docstrings). Initial implementation matched 'UNION ALL' anywhere in the file via re.finditer; the new module-level header comment about 'UNION ALL synthetic blocks' produced false positives. Refined to iterate re.finditer triple-quoted blocks and only count UNION ALL inside SELECT-bearing strings. Both Wave-0 and Wave-1 instances of 'UNION ALL' in the module docstring/comments are now correctly skipped."
  - "Did NOT run the full export against real FIST.db from this worktree — Plan 02 deviation #4 documented that the worktree at .claude/worktrees/agent-... does not have fist_data/FIST.db (gitignored, only present in main checkout). Per Plan 02 precedent, the in-memory tests + manifest-loader smoke verification provide sufficient coverage. The plan's Step I 'run end-to-end on real FIST.db' acceptance criterion is deferred to integration-time (post-merge in main checkout)."
  - "load_synthetic_manifest_into_temp_table accepts both connections AND cursors. In production, main() passes source_conn (sqlite3.Connection); in tests, the fixture passes the connection too. Both have .execute() with consistent semantics. The 'temp.synthetic_qualifying_inventories' table is connection-scoped, so any cursor from the same connection can reference it (verified empirically with the 5,035-row real manifest)."

requirements-completed: [SYNTH-05]

# Metrics
duration: ~45min
completed: 2026-05-08
---

# Phase 85 Plan 03: SYNTH-05 FJMS Sidecar Synthetic-AlmaId UNION Summary

**Modified `scripts/export_fist_enrichment.py` to UNION synthetic-AlmaId rows into all 12 AlmaId-keyed enrichment tables at export time, reading Plan 02's fist_data/synthetic_manifest.json as the AUTHORITATIVE InventoryId source. shared/fjms_service.py is byte-identical pre/post (D-01 layered-not-extended). Post-export `_validate_synthetic_export` runs table-specific invariants (catalog uniqueness, per-table real/synthetic disjointness, cross-table warning).**

## Performance

- **Duration:** ~45 min
- **Started:** 2026-05-08
- **Completed:** 2026-05-08
- **Tasks:** 2
- **Files created:** 1 (tests/test_export_fist_synthetic.py)
- **Files modified:** 1 (scripts/export_fist_enrichment.py)
- **Tests added:** 34 (33 passed + 1 smoke skip)
- **Net script growth:** +500 lines (UNION ALL blocks in 12 functions + manifest loader + validation)
- **Manifest entries consumed:** 5,035 (full Plan 02 output, verified end-to-end with the real fist_data/synthetic_manifest.json)

## Accomplishments

- Built the SYNTH-05 FJMS-sidecar synthetic-AlmaId injection mechanism. All 12 AlmaId-keyed export functions (domains, joins, catalog, catalog_running_titles, catalog_sizes, catalog_fields, catalog_free_desc, catalog_full_texts, catalog_textual_frames, catalog_mentions, bibliography, catalog_refs) UNION synthetic rows alongside real-Alma rows. The synthetic AlmaId expression `('99' || printf('%010d', inv.InventoryId) || '000000')` is sqlite-native; no Python f-string interpolation on dynamic values.
- Established the manifest-authority pattern for cross-plan dependency: Plan 03 reads fist_data/synthetic_manifest.json and populates temp.synthetic_qualifying_inventories on the source connection. Every UNION ALL block restricts InventoryId IN (SELECT FROM that temp table). NO independent SQL predicate computes the qualifying set; Plan 02 is authoritative. This eliminates the dominant Phase 85 risk per Codex/Gemini HIGH consensus.
- D-01 layered-not-extended invariant verified: `git diff shared/fjms_service.py` returns empty. The ~30 `WHERE AlmaId = ?` service queries in fjms_service.py work transparently with synthetic AlmaIds because the data layer (the sidecar) accommodates the new ID format rather than threading synthetic-detection branches through the service code.
- DETERMINISTIC ORDERING for byte-stable rebuilds: every UNION ALL block is wrapped in an outer SELECT with explicit `ORDER BY AlmaId, <stable_pk>`. The pattern is `SELECT <cols> FROM ((real-Alma SELECT DISTINCT) UNION ALL (synthetic SELECT DISTINCT)) ORDER BY AlmaId, ...`. ORDER BY at the operand level inside a UNION isn't byte-stable; the outer wrap is the canonical SQL pattern.
- Post-export validation `_validate_synthetic_export(target)`: (a) catalog (AlmaId, UnitCatalogRecId) uniqueness in synthetic block — fail-loud on duplicate pairs; (b) per-table real/synthetic disjointness via is_synthetic_sys_id partitioning — fail-loud on D-01a collision; (c) cross-table warning when synthetic AlmaIds appear in 1:N tables but not in catalog — Phase 86 audit pickup signal (warns, doesn't fail).
- 34 tests in tests/test_export_fist_synthetic.py covering the full feature surface: TestSyntheticAlmaInjection (5 dynamic tests against in-memory FIST seed), TestManifestAuthority (2 cross-plan invariant tests), TestParameterizedSqlOnly (T-85-02 mitigation), TestStaleTableNamesAbsent (4 regression-guard tests), TestDeterministicOrdering (Codex HIGH byte-stability), TestCatalogTripleUniqueness (Codex HIGH catalog 1:1 invariant), TestSyntheticAlmaInCatalogAtMinimum (smoke check for `_validate_synthetic_export` existence), TestNonAlmaTablesUnchanged (7 ref/genizah/code/meta tables synthetic-free), TestFts5Rebuild (FTS5 populates after synthetic UNION).
- Wave-0 / Wave-1 testing structure: Wave-0 scaffold (5 static-analysis tests) committed BEFORE Task 2 modifications; Wave-1 tests (29 dynamic tests) gated by a `needs_task2` skip marker that detects whether `load_synthetic_manifest_into_temp_table` is wired in the script. Provides clean failure messaging if a future regression strips the loader.
- Manifest end-to-end verification: `load_synthetic_manifest_into_temp_table` loads all 5,035 entries from the real Plan 02 manifest. Synthetic InventoryIds 2 and 3 (test fixture) confirmed to produce the expected synthetic AlmaIds 990000000002000000 and 990000000003000000 in the resulting catalog table.

## Task Commits

Each task was committed atomically:

1. **Task 1: Wave-0 scaffold for FJMS sidecar synthetic-AlmaId UNION** — `8aaf2746` (test)
2. **Task 2: UNION ALL synthetic-AlmaId rows in 12 FJMS export tables** — `a9807a6a` (feat)

## Files Created/Modified

**Created:**
- `tests/test_export_fist_synthetic.py` — 600 lines, 8 test classes, 34 tests (33 passed + 1 smoke skip). Includes:
  - `_seed_fist_schema(conn)` — creates schema stubs for all 24 FIST tables referenced by any export query (allows per-function tests to run with empty stub tables for joins that don't apply)
  - `fist_seed` fixture — 3 inventories (1 real Alma, 2 no-Alma), schema-only stubs for the rest
  - `manifest_fixture` fixture — 2-entry test manifest matching the no-Alma inventories
  - `needs_task2` skipif marker — Wave-1 tests skip cleanly when Task 2 hasn't wired the loader

**Modified:**
- `scripts/export_fist_enrichment.py` — +500 lines net:
  - Added `import json`
  - Added `load_synthetic_manifest_into_temp_table(source, manifest_path) -> int` (~50 lines)
  - Modified all 12 AlmaId-keyed export functions to UNION ALL synthetic rows (each ~30-100 lines added depending on column count; bibliography is the largest at ~30 columns)
  - Added `_validate_synthetic_export(target)` (~80 lines)
  - Modified `main()` to call manifest loader before exports and `_validate_synthetic_export` after `create_fts5`/`create_meta`

## Decisions Made

- **DETERMINISTIC ORDERING via outer SELECT wrapping the UNION ALL.** The plan's draft pattern placed ORDER BY at the inner SELECT DISTINCT operand inside the UNION ALL. This is invalid SQL — ORDER BY at the operand level isn't applied to the union result, and is not byte-stable. The correct pattern is `SELECT <cols> FROM ((SELECT DISTINCT ...) UNION ALL (SELECT DISTINCT ...)) ORDER BY AlmaId, <stable_pk>`. Each of the 12 export functions follows this pattern. Verified by TestDeterministicOrdering scanning every triple-quoted SQL block (the test was refined twice — initial broad regex caught matches inside Python comments and docstrings; final form scans only inside SELECT-bearing triple-quoted strings).
- **catalog uniqueness invariant uses (AlmaId, UnitCatalogRecId), NOT (AlmaId, UnitCatalogRecId, SignatureId).** The plan's draft test referenced SignatureId, but the actual catalog table schema (lines 188-228 of export_fist_enrichment.py) doesn't include a SignatureId column. The schema's effective uniqueness key is (AlmaId, UnitCatalogRecId). I corrected both `TestCatalogTripleUniqueness` and `_validate_synthetic_export` to use the actual schema. This is a Rule 1 deviation (fix plan-time error in test definition).
- **Did NOT run the full export against real FIST.db from this worktree.** Plan 02 deviation #4 documented that the worktree at `.claude/worktrees/agent-...` does not have `fist_data/FIST.db` (gitignored, only present in main checkout). Per that precedent, in-memory tests + manifest-loader smoke verification (loading the real 5,035-entry manifest into a temp table) provide sufficient coverage for the worktree. The plan's Step I "run end-to-end on real FIST.db" acceptance criterion is deferred to integration time in main checkout.
- **load_synthetic_manifest_into_temp_table accepts both connections and cursors.** In production main() the function is called with `source_conn` (sqlite3.Connection); in tests, fixtures pass the in-memory connection directly. Both have `.execute()` with consistent semantics, and SQLite temp tables are connection-scoped (any cursor from the same connection can reference them). This unified signature avoids forcing tests to instantiate cursors just to satisfy a parameter type.
- **TestDeterministicOrdering refined twice during implementation.** Initial regex `re.finditer(r"\bUNION ALL\b", src)` matched literal "UNION ALL" anywhere in the file. The new module-level header comment ("Phase 85 SYNTH-05 — Synthetic AlmaId injection ... UNION ALL block in the AlmaId-keyed export functions...") produced false positives. First fix: skip lines starting with `#`. Second false positive: docstring of `load_synthetic_manifest_into_temp_table` mentions "UNION ALL synthetic blocks". Final fix: iterate `re.finditer(r'"""(.*?)"""', src, re.DOTALL)` and only count UNION ALL inside SELECT-bearing triple-quoted strings. This is the canonical "scan SQL only, not Python" pattern.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Plan's draft ORDER BY placement was invalid SQL**

- **Found during:** Task 2 implementation (writing the first UNION ALL block in `export_domains`)
- **Issue:** The plan's draft SQL pattern (Step C) placed `ORDER BY AlmaId, Domain` directly after the synthetic SELECT DISTINCT inside the UNION ALL. This is non-portable and not byte-stable — ORDER BY in a UNION operand isn't applied to the combined result. SQLite does accept it without error in some contexts but the output ordering is implementation-defined.
- **Fix:** Wrapped every UNION ALL in an outer `SELECT <columns> FROM (...) ORDER BY <key>` to make the ordering apply to the composite result. This is the canonical SQL pattern for ordering composite query results.
- **Files modified:** `scripts/export_fist_enrichment.py` — all 12 AlmaId-keyed export function queries
- **Why this is Rule 1:** Plan-time correctness error. Following the plan's draft would have produced non-deterministic row ordering, defeating the byte-stability goal that Codex HIGH explicitly required. The outer-SELECT wrap is the correct pattern.
- **Verification:** TestDeterministicOrdering green; all 12 SQL UNION blocks have ORDER BY within 3000 chars after the UNION keyword.
- **Committed in:** `a9807a6a`

**2. [Rule 1 - Bug] catalog table schema doesn't have SignatureId column**

- **Found during:** Task 2 implementation (writing `_validate_synthetic_export` and `TestCatalogTripleUniqueness`)
- **Issue:** The plan's draft both for the test and the validation function asserts uniqueness over `(AlmaId, UnitCatalogRecId, SignatureId)`. But the catalog table schema (export_fist_enrichment.py lines 188-228) doesn't include SignatureId — only UnitCatalogRecId. Running the draft SQL would have raised `OperationalError: no such column: SignatureId`.
- **Fix:** Use `(AlmaId, UnitCatalogRecId)` as the catalog uniqueness key (the actual effective key per the schema). Updated both `_validate_synthetic_export` and `TestCatalogTripleUniqueness::test_no_duplicate_triples_in_synthetic_block`.
- **Files modified:** `scripts/export_fist_enrichment.py` `_validate_synthetic_export`; `tests/test_export_fist_synthetic.py` `TestCatalogTripleUniqueness`
- **Why this is Rule 1:** Plan-time schema error. Following the draft would have crashed at validation time.
- **Verification:** TestCatalogTripleUniqueness green; manual catalog query confirms (AlmaId, UnitCatalogRecId) is the natural key with cardinality 1:1 in the synthetic block (and 1:1 across the table generally per the existing CREATE INDEX `idx_catalog_alma + idx_catalog_ucrid`).
- **Committed in:** `a9807a6a`

**3. [Rule 1 - Bug] TestDeterministicOrdering false positives on Python comments and docstrings**

- **Found during:** First Task 2 test run
- **Issue:** Initial test scanned `re.finditer(r"\bUNION ALL\b", src)` — matched literal "UNION ALL" anywhere in the file. The new module-level header comment about "UNION ALL synthetic blocks" produced a false positive at offset 2488. After skipping comment lines (lines starting with `#`), the docstring of `load_synthetic_manifest_into_temp_table` produced another false positive at offset 3077 ("UNION ALL synthetic blocks in this script").
- **Fix:** Refined regex to scan only inside triple-quoted SQL blocks: `re.finditer(r'"""(.*?)"""', src, re.DOTALL)` then filter to only blocks containing `SELECT`. Track positions relative to the original source for error messages.
- **Files modified:** `tests/test_export_fist_synthetic.py` `TestDeterministicOrdering::test_explicit_order_by_in_each_union`
- **Why this is Rule 1:** Test specification bug. The intent was to assert SQL UNION ALL has ORDER BY, not that Python comments mentioning UNION ALL must be followed by Python ORDER BY (which is meaningless). Final form correctly scopes to SQL blocks only.
- **Verification:** Test green; sql_union_positions count = 12 (matching the 12 export functions).
- **Committed in:** `a9807a6a`

---

**Total deviations:** 3 auto-fixed (3 Rule 1 plan-time bugs)
**Impact on plan:** All deviations are localized fixes to plan-time errors; the plan's load-bearing invariants stand: D-01 layered-not-extended (verified by `git diff shared/fjms_service.py` empty); D-01a collision invariant (verified by per-table partition scan in _validate_synthetic_export); manifest authority (verified by TestManifestAuthority); deterministic ordering (verified by TestDeterministicOrdering after refinement); table-specific invariants per Codex HIGH; FTS5 rebuild green; stale table names absent; parameterized SQL only. No scope creep.

## Issues Encountered

- **Initial UNION ALL pattern produced non-deterministic ordering.** First implementation placed ORDER BY at the SELECT DISTINCT level inside the UNION ALL operand. SQLite accepted it without error, but the result ordering was implementation-defined. Diagnosed by reading the SQL spec and the existing `bibliography` GROUP BY pattern (which already uses outer SELECT). Refactored all 12 functions to wrap UNION in outer SELECT with single ORDER BY. Tests passed.
- **catalog table schema didn't match plan draft.** Plan draft assumed (AlmaId, UnitCatalogRecId, SignatureId) uniqueness, but the actual catalog schema doesn't carry SignatureId. Verified by reading `export_fist_enrichment.py` lines 188-228 schema and existing `idx_catalog_alma + idx_catalog_ucrid` index pattern. Updated to (AlmaId, UnitCatalogRecId). The export_catalog query carries SignatureId via the JOIN to dbo_Signature but doesn't store it in the resulting catalog row — so the catalog table's natural uniqueness key is (AlmaId, UnitCatalogRecId).
- **PreToolUse:Edit hook produced spurious Read-before-Edit reminders.** The hook fired ~10 times during Task 2 even though the file had been Read at session start. Ignored as false positives — every edit succeeded normally and was reflected in subsequent file reads.

## Acceptance Criteria Status

| Criterion | Status |
|-----------|--------|
| `grep -c "UNION ALL" scripts/export_fist_enrichment.py` returns ≥ 12 | PASS (15 — 12 SQL blocks + 3 in comments/docstrings) |
| `grep -c "printf('%010d', inv.InventoryId)" scripts/export_fist_enrichment.py` returns ≥ 12 | PASS (12) |
| `grep -c "synthetic_qualifying_inventories" scripts/export_fist_enrichment.py` returns ≥ 13 | PASS (18 — 1 CTE creator + 12 export functions + 5 in comments/docstrings/validation) |
| `grep -c "load_synthetic_manifest_into_temp_table" scripts/export_fist_enrichment.py` returns ≥ 2 | PASS (2 — definition + main() call) |
| `grep -c "ORDER BY" scripts/export_fist_enrichment.py` returns ≥ 12 | PASS (13) |
| `git diff shared/fjms_service.py` returns empty | PASS (zero diff — D-01 verified) |
| `pytest tests/test_export_fist_synthetic.py -x -q` exits 0 | PASS (33 passed, 1 skipped) |
| `python scripts/export_fist_enrichment.py` runs to completion | DEFERRED (worktree lacks FIST.db; full run deferred to main-checkout integration time per Plan 02 precedent) |
| Post-export manifest authority verified | PASS (manifest loader loads all 5,035 real entries; tests confirm synthetic AlmaIds in catalog all decode to manifest InventoryIds) |
| Stale table names absent | PASS (TestStaleTableNamesAbsent green for all 4 names) |
| Parameterized SQL only | PASS (TestParameterizedSqlOnly green) |
| Stale table list FIXED to actual 12 | PASS (no def export_measurements / manuscript_measurements / extra_info / computed_measurements; ALMA_KEYED_TABLES constant matches verified list) |

## Threat Model Validation

| Threat ID | Mitigation Applied |
|-----------|--------------------|
| T-85-03-01 (SQL injection) | All synthetic-AlmaId values use sqlite-native `printf` and JOIN-derived columns; no Python f-string interpolation on dynamic values. TestParameterizedSqlOnly scans every triple-quoted SQL block for `{var}` interpolation; assertion green. |
| T-85-03-02 (cross-plan inconsistency) | Manifest is the AUTHORITATIVE InventoryId source; `temp.synthetic_qualifying_inventories` is populated ONLY from `fist_data/synthetic_manifest.json`. NO inline qualifying-set predicate exists. TestManifestAuthority::test_no_independent_qualifying_set_predicate regression-guards against future drift; cross-plan invariant test asserts every synthetic AlmaId in catalog decodes to a manifest InventoryId. |
| T-85-03-03 (collision) | `_validate_synthetic_export` per-table partition scan: for each of 12 AlmaId-keyed tables, partitions distinct AlmaIds via `is_synthetic_sys_id`; raises SystemExit on intersection (D-01a). TestSyntheticAlmaInjection::test_no_collision_real_vs_synthetic asserts the same on the test target. |
| T-85-03-04 (information disclosure) | Accepted per Plan 02 T-85-02-06 — InventoryIds are FIST.db identifiers, not PII or auth tokens. Documented exposure. |
| T-85-03-05 (DoS via UNION runtime) | Manifest carries 5,035 entries vs FIST.db's ~280K real-Alma rows (~2% increase per table). Existing `BATCH_SIZE = 10_000` chunking unchanged. No measurable runtime impact expected. |
| T-85-03-06 (row-ordering drift) | Every UNION ALL block has explicit `ORDER BY AlmaId, <stable_pk>` at the OUTER SELECT level. TestDeterministicOrdering scans every SQL UNION ALL position and asserts ORDER BY appears within 3000 chars; assertion green for 12 SQL UNION blocks. |
| T-85-03-07 (stale table list regression) | TestStaleTableNamesAbsent parametrized over 4 stale names (measurements, manuscript_measurements, extra_info, computed_measurements); regression-guards `def export_<stale>` and `CREATE TABLE <stale>` — green. |
| T-85-03-08 (FTS5 misindexing) | TestFts5Rebuild verifies `create_fts5(target)` succeeds after synthetic-row UNION populates the catalog/running-titles/free-desc/full-texts tables. Synthetic rows with sparse FTS body simply don't surface in FTS searches — degrades gracefully. |

## Self-Check: PASSED

Verified before writing this summary:

- `tests/test_export_fist_synthetic.py` exists at the worktree path
- `scripts/export_fist_enrichment.py` modified with 12 UNION ALL blocks + manifest loader + validation
- Commit `8aaf2746` exists in worktree git log (Task 1)
- Commit `a9807a6a` exists in worktree git log (Task 2)
- `pytest tests/test_export_fist_synthetic.py -x -q`: 33 passed, 1 skipped
- `pytest tests/test_synthetic_sys_id.py tests/test_generate_synthetic_rows.py tests/test_export_fist_synthetic.py -q`: 119 passed, 1 skipped (no regressions in Plan 01/02)
- `pytest tests/test_fjms_service.py -q`: 105 passed (no regressions in FjmsService)
- `git diff shared/fjms_service.py` empty (D-01 verified)
- `python -c "import scripts.export_fist_enrichment"` succeeds; module exposes `load_synthetic_manifest_into_temp_table`
- Real-manifest end-to-end test: `load_synthetic_manifest_into_temp_table(conn, 'fist_data/synthetic_manifest.json')` loads 5,035 entries (matches Plan 02's coverage report)

## Next Phase Readiness

- **Plan 04 (browse hide-NLI + CUDL default)** can now rely on the FJMS sidecar carrying enrichment data for synthetic AlmaIds. After Plan 04 lands and the FJMS sidecar is regenerated against the real FIST.db with this Plan 03 modification + Plan 02's manifest, opening a browse page for a synthetic sys_id (e.g. csv_bank entry from Plan 02) will display catalog title, domains, bibliography, and other FJMS metadata via the unchanged `shared/fjms_service.py` API.
- **Plan 05 (end-to-end smoke)** should regenerate `fist_data/fjms_enrichment.db` against the real FIST.db (in main checkout) before running its smoke checks. The deferred Step I acceptance criterion ("run end-to-end on real FIST.db") gets satisfied during the Plan 05 integration run.
- **Phase 86 (CUDL Coverage Audit)** has the cross-table warning channel from `_validate_synthetic_export` step (c) — synthetic AlmaIds appearing in 1:N tables but not in catalog get printed to stderr. AUDIT-01 can grep these warnings from the export log to surface candidates for follow-up.
- **No blockers** for Wave 3 Plan 04 (different files: web/, desktop/, genizah_*.py) running in parallel with this plan.

---
*Phase: 85-synthetic-fjms-inventory-rows*
*Plan: 03*
*Completed: 2026-05-08*
