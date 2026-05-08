---
phase: 85-synthetic-fjms-inventory-rows
plan: 02
subsystem: synthetic-rows-generation
tags: [synthetic-rows, libraries-csv, phase-85, regeneration-script, manifest-authority, fjms-only-inventories]

# Dependency graph
requires:
  - phase: 85-synthetic-fjms-inventory-rows
    plan: 01
    provides: shared/synthetic_sys_id.py helpers (encode_inventory_sys_id, is_synthetic_sys_id, decode_inventory_id)
provides:
  - scripts/generate_synthetic_rows.py — idempotent regeneration script that walks FIST.db × cambridge_manifests, encodes synthetic sys_ids via Plan 01 helpers, writes marker-fenced block to libraries.csv, emits AUTHORITATIVE manifest + ambiguity-residue CSV + coverage markdown
  - libraries.csv — modified with 5,035 synthetic rows in marker-fenced block (# BEGIN SYNTHETIC / # END SYNTHETIC)
  - fist_data/synthetic_manifest.json — AUTHORITATIVE qualifying-set audit artifact (5,035 entries) consumed by Plan 03 as its ONLY InventoryId source — eliminates parallel-SQL-predicate divergence
  - reports/synthetic_ambiguity_residue.csv — 10,689 ambiguity-excluded keys (408 multi_inventory + 10,281 multi_signature) for Phase 86 AUDIT-01 consumption
  - reports/synthetic_coverage.md — D-03 tier counts + SYNTH-03 narrowing rationale + Phase 86 audit cross-link
  - tests/test_generate_synthetic_rows.py — 22 tests covering idempotency, collision-detection, ambiguity (multi-inventory + multi-signature), CSV-injection fail-loud, D-02 expanded predicate, deterministic ordering, manifest authority, line-ending preservation, csv_bank marker tolerance, SYNTH-03 narrowing
  - genizah_core.py:_load_csv_bank — single-line guard skips '#'-prefixed marker lines
affects: [85-03, 85-04, 85-05, 86-cudl-coverage-audit]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Manifest as authoritative cross-plan input: Plan 02 emits fist_data/synthetic_manifest.json which Plan 03 reads as its ONLY InventoryId source — replaces parallel SQL predicate divergence (Codex/Gemini HIGH consensus). Pattern: when two waves both need to compute the same set, one wave is the authority and writes the artifact; the dependent wave reads the artifact rather than recomputing"
    - "Pre-aggregate signal sets: when EXISTS subqueries against unindexed tables run > 5min, pre-aggregate each signal table into a Python set (< 1s/table) and join in Python. Used here for D-02 EXPANDED bib/freedesc/fulltext/size signals against ~25K rows × 4 unindexed tables"
    - "Marker-fenced CSV block as idempotent regeneration target: # BEGIN SYNTHETIC / # END SYNTHETIC bracketed block in libraries.csv allows the script to re-write only the synthetic portion without disturbing real-Alma rows, while a single-line loader guard (raw_sys_id.startswith('#')) tolerates the markers in csv_bank"
    - "CSV-injection FAIL-LOUD: rows with leading =/+/-/@ in any string field are EXCLUDED (logged to ambiguity-residue with ambiguity_kind='csv_injection_leader') rather than single-quote-sanitized — single-quote sanitization would mutate runtime-visible Hebrew titles since csv.reader does no Excel-sanitization on read"
    - "Connection-injectable testability: _build_qualifying_inventories accepts sqlite3.Connection arguments rather than hard-coding paths, enabling in-memory test seeds without monkeypatching sqlite3.connect at the module level (Gemini LOW design accepted)"

key-files:
  created:
    - "scripts/generate_synthetic_rows.py — Idempotent regeneration script (~735 lines). Functions: _has_csv_injection_leader, _build_qualifying_inventories, _classify_library_code, _resolve_title, _generate_call_numbers, _read_libraries_csv, _strip_existing_synthetic_block, _build_synthetic_rows, _collect_real_alma_ids, _write_manifest, _write_residue, _write_coverage, main. CLI: --dry-run / --apply (mutually exclusive) plus --fist-db / --nli-db / --csv-path / --manifest-path / --residue-path / --coverage-path overrides for cross-checkout invocation"
    - "fist_data/synthetic_manifest.json — 5,035-entry sorted JSON array, AUTHORITATIVE for Plan 03"
    - "reports/synthetic_ambiguity_residue.csv — 10,689 rows; header: inventory_id, signature_id, ambiguity_kind, classmark, cudl_label, fist_signature_ids, fist_inventory_ids, leading_char"
    - "reports/synthetic_coverage.md — D-03 tier counts + SYNTH-03 narrowing + Phase 86 cross-link"
    - "tests/test_generate_synthetic_rows.py — 22 tests in 5 test classes: TestLoaderMarkerTolerance (3), TestQualifyingInventories (6), TestRegenerateScript (10), TestCoverageReport (1), TestResidueWriter (2), TestSynth03ModeNarrowing (1)"
  modified:
    - "genizah_core.py:_load_csv_bank — single-line guard at line 3377: if raw_sys_id.startswith('#'): continue"
    - "libraries.csv — +5,037 lines (5,035 synthetic data rows + 2 marker lines)"
    - ".gitignore — switched fist_data/ -> fist_data/*, /Reports -> /reports/* with explicit !-exceptions for the three Phase 85 audit artifacts (git cannot re-include files inside excluded directories, but can inside excluded globs)"

key-decisions:
  - "Schema-mapping deviation: plan referenced dbo_BibliographyRef / dbo_FreeDescription / dbo_FullText / dbo_UnitSize / sig.Signature / cat.TitleHeb / cat.GenizahTitle. Actual FIST.db schema has dbo_UnitBibliographyReference / dbo_UnitFreeDescription / dbo_UnitFullText / dbo_CatalogMultiSize / inv.Shelfmark (not on Signature) / cat.Title (already Hebrew) / cat.GenizahTitleText. Mapping locked into the script docstring + qualifying-set function comments"
  - "FIST.db lives at fist_data/FIST.db NOT FIST_DB_BACKUP/FIST.db (the plan's draft path). Path constants updated in script + documented as deviation"
  - "CLI path overrides added beyond the plan's design: --fist-db / --nli-db / --csv-path / --manifest-path / --residue-path / --coverage-path. Necessary because the worktree is at .claude/worktrees/agent-... but FIST.db / nli_crossref.db are gitignored data files only present in the main checkout. The overrides allow the worktree script to operate on main-checkout data files, and they harmonize with future ops use cases (e.g. running against staging databases)"
  - "Performance refactor: replaced per-row EXISTS subqueries with one-time pre-aggregated Python sets. The plan's draft SQL uses EXISTS(...) for D-02 EXPANDED signals against four unindexed FIST tables. On real data (~25K candidate rows × 4 tables), this runs > 5 minutes. Pre-aggregating each signal into a Python set takes < 1 second per table and the per-row check is O(1) hash lookup. Total runtime: real-data --apply completes in ~30s"
  - "Tier 1/Tier 2 = 0 in coverage report is a load-bearing finding: zero CUDL+FJMS overlap and zero CUDL-only synthetic rows because nearly every CUL inventory in FIST.db has multiple SignatureIds (recto/verso/copies), which D-05a STRICT correctly excludes as multi_signature ambiguity. The 100 CUDL-matching shelfmarks in FIST.db × cambridge_manifests all fall into multi_signature exclusion. Tier 3 (FJMS-only no-CUDL) = 5,035 is the entire synthetic population. This is the right behavior per the strict ambiguity policy — silent fan-out across multiple signatures would be the Phase-84-D-06 anti-pattern"
  - "gitignore exemption pattern: fist_data/ -> fist_data/* and /Reports -> /reports/* (with explicit !-exceptions for the three audit artifacts). Necessary because git cannot re-include files inside an excluded *directory*, but can re-include inside an excluded *glob*. The pattern still ignores all other contents of those dirs"

requirements-completed: [SYNTH-02, SYNTH-03]

# Metrics
duration: 75min
completed: 2026-05-08
---

# Phase 85 Plan 02: SYNTH-02 + SYNTH-03 Synthetic Rows Generation Summary

**Idempotent regeneration script (scripts/generate_synthetic_rows.py) walks FIST.db × cambridge_manifests, generates 5,035 synthetic libraries.csv rows for FJMS-only inventories, emits the AUTHORITATIVE manifest consumed by Plan 03, plus ambiguity-residue and coverage artifacts for Phase 86 audit. csv_bank loader extended with one-line marker-tolerance guard.**

## Performance

- **Duration:** ~75 min
- **Started:** 2026-05-08 (worktree branch creation)
- **Completed:** 2026-05-08
- **Tasks:** 2
- **Files created:** 5 (script + manifest + residue + coverage + tests)
- **Files modified:** 3 (genizah_core.py, libraries.csv, .gitignore)
- **Real-data --apply runtime:** ~30s (post-performance-refactor)
- **Synthetic rows emitted:** 5,035 (Tier 3: FJMS-only, no CUDL)
- **Ambiguity-excluded keys:** 10,689 (408 multi_inventory + 10,281 multi_signature)

## Accomplishments

- Built the SYNTH-02 + SYNTH-03 (narrowed Title/Shelfmark) regeneration pipeline. The script is idempotent (verified byte-identical libraries.csv AND byte-identical manifest.json across consecutive --apply runs on real data), fail-loud on D-01a collision, and STRICT on D-05a ambiguity (excludes both multi-inventory AND multi-signature cases per Codex HIGH).
- Established the AUTHORITATIVE manifest pattern for cross-wave dependency: Plan 03 will consume `fist_data/synthetic_manifest.json` as its ONLY InventoryId source rather than running its own qualifying-set SQL predicate. This eliminates the dominant Phase 85 risk per the cross-AI review consensus.
- D-02 EXPANDED predicate honored: an inventory qualifies if it has any of catalog title, GenizahTitleText, bibliography reference, free-description, full-text, or computed-size record. Real-data run produced 5,035 qualifying inventories.
- D-05a STRICT ambiguity exclusion logged 10,689 excluded keys with `ambiguity_kind` ∈ {multi_inventory, multi_signature, csv_injection_leader} for Phase 86 AUDIT-01 consumption. Note: 100 CUDL-matching FIST shelfmarks all fell into multi_signature exclusion (CUL items typically have multiple SignatureIds for recto/verso/copies), which is why Tier 1 = Tier 2 = 0 in the coverage report — this is the correct strict-policy outcome.
- T-85-01 CSV-injection FAIL-LOUD wired throughout: rows with leading `=`/`+`/`-`/`@` in title or shelfmark are EXCLUDED rather than single-quote-sanitized (which would mutate runtime-visible Hebrew titles). Logged to residue with `ambiguity_kind='csv_injection_leader'`.
- Performance refactor unblocked the real-data run: pre-aggregating four D-02 signal tables into Python sets reduced query time from > 5 minutes (per-row EXISTS subqueries against unindexed tables) to < 1 second (set-membership lookup). Total `--apply` runtime: ~30 seconds.
- csv_bank loader integration verified end-to-end: `MetadataManager._load_csv_bank()` loads 260,650 records (255,615 real-Alma + 5,035 synthetic). All 5,035 synthetic entries have populated `shelfmark` field (Pitfall 5 mitigation confirmed). Marker lines correctly skipped via the new one-line guard.
- 22 tests in `tests/test_generate_synthetic_rows.py` cover the full feature surface: idempotency byte-identity (CSV + manifest), collision-fail-loud, D-05a multi-inventory + multi-signature ambiguity, CSV-injection fail-loud, D-02 expanded predicate (bibliography-only and free-desc-only qualifications), deterministic ordering, manifest authority for Plan 03, marker-block round-trip, line-ending preservation, residue header columns, coverage report Phase 86 cross-link, SYNTH-03 narrowing.

## Task Commits

Each task was committed atomically:

1. **Task 1: csv_bank marker-block tolerance + Task 2 test scaffold** — `e5a69a83` (feat)
2. **Task 2: scripts/generate_synthetic_rows.py + libraries.csv synthetic block + manifest + residue + coverage** — `c613fb06` (feat)

## Files Created/Modified

**Created:**
- `scripts/generate_synthetic_rows.py` — 735 lines. Idempotent regeneration script with --dry-run / --apply mutually exclusive args plus --fist-db / --nli-db / --csv-path / --manifest-path / --residue-path / --coverage-path overrides
- `fist_data/synthetic_manifest.json` — 5,035-entry JSON array sorted by inventory_id ascending, byte-stable across runs
- `reports/synthetic_ambiguity_residue.csv` — 10,690 lines (header + 10,689 residue rows). Header: inventory_id, signature_id, ambiguity_kind, classmark, cudl_label, fist_signature_ids, fist_inventory_ids, leading_char
- `reports/synthetic_coverage.md` — Tier counts + SYNTH-03 narrowing rationale + Phase 86 audit cross-link
- `tests/test_generate_synthetic_rows.py` — 22 tests across 6 test classes

**Modified:**
- `genizah_core.py:3370-3378` — single-line guard `if raw_sys_id.startswith('#'): continue` in `_load_csv_bank`
- `libraries.csv` — +5,037 lines (5,035 synthetic data rows + 2 marker lines bracketed by `# BEGIN SYNTHETIC` / `# END SYNTHETIC`)
- `.gitignore` — `fist_data/` → `fist_data/*` and `/Reports` → `/reports/*` patterns with `!`-exceptions for the three Phase 85 audit artifacts

## Decisions Made

- **Schema-mapping deviation from plan's draft.** The plan's interface section listed FIST.db tables/columns that don't match the real schema. I verified the actual schema against `fist_data/FIST.db` from the main checkout and corrected: `dbo_BibliographyRef` → `dbo_UnitBibliographyReference`; `dbo_FreeDescription` → `dbo_UnitFreeDescription`; `dbo_FullText` → `dbo_UnitFullText`; `dbo_UnitSize` → `dbo_CatalogMultiSize`; shelfmark text on `inv.Shelfmark` (NOT `sig.Signature` — that column doesn't exist); title on `cat.Title` (already Hebrew); Genizah title on `cat.GenizahTitleText`. The schema mapping is documented in the script's `_build_qualifying_inventories` docstring.
- **Path constant deviation.** Plan said `FIST_DB_BACKUP/FIST.db`; actual path is `fist_data/FIST.db`. Fixed in module-level `FIST_DB` constant.
- **CLI path overrides added beyond plan design.** The plan's --dry-run / --apply CLI didn't include path overrides, but the worktree at `.claude/worktrees/agent-...` doesn't have `fist_data/`/`nli_data/` (they're gitignored top-level dirs only present in main checkout). Added `--fist-db / --nli-db / --csv-path / --manifest-path / --residue-path / --coverage-path` arguments that default to the module constants. This is a Rule 3 deviation (auto-fix blocking issue) — without overrides the script can't run from the worktree.
- **Performance refactor.** The plan's draft SQL used EXISTS subqueries for D-02 EXPANDED signals. On real data this runs > 5 minutes (unindexed FIST tables × 25K candidate rows × 4 signal tables). Refactored to pre-aggregate each signal table into a Python set in one O(N) scan, then check membership in O(1) per row. Result: < 1 second per signal table; total `--apply` runtime ~30s. Documented inline in the script. This is a Rule 1 deviation (fix logic that doesn't work as intended — performance was effectively a hang).
- **Tier 1 / Tier 2 = 0 is a load-bearing finding, NOT a bug.** Coverage report shows Tier 1 (CUDL+FJMS) = 0 and Tier 2 (CUDL-only no-FJMS) = 0. Investigation: 100 FIST shelfmarks normalize-match CUDL classmarks, but they all have multiple SignatureIds (recto/verso/copies) and fall into D-05a STRICT multi_signature exclusion. This is the correct outcome of the strict ambiguity policy — silent fan-out would be the Phase-84-D-06 anti-pattern. The synthetic mechanism's CUDL coverage payoff is therefore deferred to a future infrastructure phase that handles multi-signature merging deterministically. Tier 3 (FJMS-only) = 5,035 is the entire synthetic population.
- **gitignore exemption pattern.** Default `fist_data/` and `/Reports` patterns exclude entire directories, and git cannot re-include files inside an excluded directory. Switched to glob form (`fist_data/*`, `/reports/*`) so `!`-exceptions can re-include the three audit artifacts that Plan 03 / Phase 86 need as committed inputs. This is a Rule 2 deviation (auto-add missing critical functionality — without this change the manifest can't be committed for Plan 03 to consume).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Performance: per-row EXISTS subqueries effectively hang on real data**

- **Found during:** Task 2 dry-run on real FIST.db (worktree sanity check)
- **Issue:** The plan's draft SQL uses `EXISTS(SELECT 1 FROM dbo_UnitBibliographyReference WHERE SignatureId = sig.SignatureId)` and three similar subqueries against unindexed FIST tables. Against ~25K candidate rows × 4 unindexed tables, this runs for > 5 minutes (timed out a 5-minute budget) and effectively never completes for the user.
- **Fix:** Pre-aggregate each signal table into a Python `set` in one O(N) scan (< 1s per table), then check `sig_id in bib_sigs` per row in O(1). Total `--apply` runtime drops from > 5 minutes (effectively a hang) to ~30 seconds.
- **Files modified:** `scripts/generate_synthetic_rows.py` `_build_qualifying_inventories` body
- **Why this is Rule 1:** Performance regression that effectively prevents the script from completing within any reasonable time budget. The acceptance criterion `python scripts/generate_synthetic_rows.py --dry-run exits 0` is unsatisfiable in the EXISTS form. The pre-aggregation approach is semantically equivalent (still O(N+M) total set operations vs. O(N×M) joins), but actually finishes.
- **Verification:** All 22 tests still pass; real-data --apply completes in ~30s producing byte-identical output.
- **Committed in:** `c613fb06`

**2. [Rule 1 - Bug] Schema mapping: plan's table/column names don't match real FIST.db schema**

- **Found during:** Task 2 implementation (script reading 85-PATTERNS.md + verifying against real FIST.db)
- **Issue:** Plan referenced tables `dbo_BibliographyRef`, `dbo_FreeDescription`, `dbo_FullText`, `dbo_UnitSize` and columns `sig.Signature` / `cat.TitleHeb` / `cat.GenizahTitle`. Verified against `fist_data/FIST.db` schema: actual tables are `dbo_UnitBibliographyReference`, `dbo_UnitFreeDescription`, `dbo_UnitFullText`, `dbo_CatalogMultiSize`. Actual shelfmark text is on `dbo_Inventory.Shelfmark` (not on Signature, which has no shelfmark column). Actual title columns are `cat.Title` (already Hebrew in this corpus) and `cat.GenizahTitleText` (no separate `TitleHeb`).
- **Fix:** Updated SQL queries and Python field names to match actual schema. Documented mapping in `_build_qualifying_inventories` docstring. The plan's note ("actual table names MUST be verified against FIST.db schema by executor") explicitly anticipated this — I followed that instruction.
- **Files modified:** `scripts/generate_synthetic_rows.py` `_build_qualifying_inventories`, `_resolve_title`
- **Why this is Rule 1:** Plan's named tables don't exist; SQL would have failed at runtime. Plan-time error.
- **Verification:** Real-data --dry-run produces 5,035 qualifying inventories with sane breakdown of D-02 signal counts.
- **Committed in:** `c613fb06`

**3. [Rule 3 - Blocking] FIST.db lives at fist_data/FIST.db, not FIST_DB_BACKUP/FIST.db**

- **Found during:** Task 2 first-run smoke check
- **Issue:** Plan's module-level `FIST_DB = ROOT / "FIST_DB_BACKUP" / "FIST.db"` — but the actual path is `fist_data/FIST.db` (verified against main checkout). FIST_DB_BACKUP/ exists but does NOT contain FIST.db (only contains decoded CSV exports and `_FIST.db` macOS metadata stub).
- **Fix:** Updated `FIST_DB = ROOT / "fist_data" / "FIST.db"`. Documented as deviation in script docstring.
- **Files modified:** `scripts/generate_synthetic_rows.py` module-level path constants
- **Why this is Rule 3:** Wrong path makes the script fail to open the database — blocks task completion.
- **Committed in:** `c613fb06`

**4. [Rule 3 - Blocking] Worktree cannot reach main-checkout data files without CLI overrides**

- **Found during:** Task 2 first-run smoke check
- **Issue:** Worktree at `.claude/worktrees/agent-...` doesn't have `fist_data/` or `nli_data/` (they're gitignored). The script's hard-coded paths resolve to non-existent files inside the worktree. Even after fixing the path constant, the script can't run from a worktree against main-checkout data.
- **Fix:** Added `--fist-db / --nli-db / --csv-path / --manifest-path / --residue-path / --coverage-path` CLI argparse arguments. Defaults are the module constants (so behavior is unchanged when invoked from main checkout); overrides allow worktree invocation. Real-data verification used overrides pointing at `C:/Genizahsearch/fist_data/FIST.db` and `C:/Genizahsearch/nli_data/nli_crossref.db`.
- **Files modified:** `scripts/generate_synthetic_rows.py` `main` function
- **Why this is Rule 3:** Without overrides the script can't run from the worktree where it's being developed, blocking the acceptance criterion `python scripts/generate_synthetic_rows.py --dry-run exits 0`.
- **Future ops bonus:** The overrides also enable running against staging/dev databases or older checkouts, which is generally useful.
- **Committed in:** `c613fb06`

**5. [Rule 2 - Critical] gitignore exempts the three audit artifacts**

- **Found during:** Task 2 commit-prep
- **Issue:** Default `.gitignore` rules `fist_data/` and `/Reports` exclude entire directories. The three Phase 85 audit artifacts (`fist_data/synthetic_manifest.json`, `reports/synthetic_ambiguity_residue.csv`, `reports/synthetic_coverage.md`) MUST be committed: the manifest is the AUTHORITATIVE Plan 03 input, the residue is the Phase 86 AUDIT-01 input, and the coverage is the D-03 deliverable. Git cannot re-include files inside an excluded *directory*, but can inside an excluded *glob*.
- **Fix:** Switched `fist_data/` → `fist_data/*` and `/Reports` → `/reports/*` (also added `/Reports/*` for case-sensitivity safety on non-Windows hosts). Added explicit `!`-exceptions for the three artifacts. Verified via `git check-ignore -v` that the artifacts are no longer ignored while other files in the directories still are.
- **Files modified:** `.gitignore`
- **Why this is Rule 2:** Without committed audit artifacts, Plan 03 can't read the AUTHORITATIVE manifest and Phase 86 has no residue to audit. Critical correctness requirement; auto-add per Rule 2.
- **Committed in:** `c613fb06`

---

**Total deviations:** 5 auto-fixed (2 Rule 1 bugs, 2 Rule 3 blockers, 1 Rule 2 critical addition)
**Impact on plan:** All deviations are localized fixes that preserve every load-bearing plan invariant (D-01a, D-02 EXPANDED, D-04a, D-05a STRICT, D-09, D-12, D-15, T-85-01, T-85-02). The plan's manifest-authority pattern, idempotency contract, ambiguity-exclusion semantics, and CSV-injection fail-loud all stand. No scope creep.

## Issues Encountered

- **Real-data run hung on EXISTS subqueries.** First `--dry-run` against real FIST.db ran > 5 minutes without producing output (the 120s and 180s timeouts both hit). Diagnosed by progressively timing each stage of the query: connection (instant), simple count (0.18s), but the full SELECT with EXISTS subqueries against four unindexed tables ran > 5 minutes. Resolved via Rule 1 deviation (pre-aggregation refactor). After refactor: < 1s per signal table, ~30s total.
- **gitignore directory-exclusion limitation.** Force-add via `git add -f` worked but the `!`-exception was being ignored because git can't re-include files inside an excluded *directory*. Resolved via Rule 2 deviation (switch to glob form).

## Acceptance Criteria Status

| Criterion | Status |
|-----------|--------|
| `grep -A1 "raw_sys_id = row\[0\]" genizah_core.py | grep "startswith('#')"` matches | PASS |
| `grep -c "Phase 85 D-04a" genizah_core.py` returns 1 | PASS |
| `tests/test_generate_synthetic_rows.py` exists with `class TestLoaderMarkerTolerance` (3 tests) | PASS |
| `pytest tests/test_generate_synthetic_rows.py::TestLoaderMarkerTolerance -x -q` exits 0 | PASS (3 passed) |
| `scripts/generate_synthetic_rows.py` exists with `def main` and `if __name__ == "__main__":` | PASS |
| `python scripts/generate_synthetic_rows.py --dry-run` exits 0 (with --fist-db / --nli-db overrides) | PASS |
| `python scripts/generate_synthetic_rows.py --apply` exits 0 (with overrides) | PASS |
| `fist_data/synthetic_manifest.json` parses as JSON array sorted by inventory_id ascending | PASS (5,035 items, sorted) |
| Exactly ONE matched `# BEGIN SYNTHETIC` / `# END SYNTHETIC` pair in libraries.csv | PASS (1/1) |
| `reports/synthetic_ambiguity_residue.csv` header includes required columns | PASS (inventory_id, signature_id, ambiguity_kind, classmark all present) |
| `reports/synthetic_coverage.md` has Tier 1/2/3 + SYNTH-03 + Phase 86 sections (≥5) | PASS |
| Collision check fails loud (test_collision_check_fails_loud) | PASS |
| CSV-injection fail-loud excludes the row (test_csv_injection_excludes_row) | PASS |
| Byte-identity idempotency (test_idempotency_byte_identity) — verified on real data too | PASS |
| `pytest tests/test_generate_synthetic_rows.py -x -q` exits 0 (no XFAIL) | PASS (22 passed) |
| No XFAIL placeholders remain | PASS |
| `grep -c "ORDER BY" scripts/generate_synthetic_rows.py` returns ≥ 2 | PASS (8) |
| `grep -c "ambiguity_kind" scripts/generate_synthetic_rows.py` returns ≥ 4 | PASS (17) |
| `grep -c "AUDIT-01\|Phase 86" reports/synthetic_coverage.md` returns ≥ 3 | PASS (5) |
| Synthetic rows in csv_bank have non-empty shelfmark (Pitfall 5) | PASS (5,035 / 5,035) |
| All 22 tests + Plan 01's 64 tests + 63 shelfmark_bridge tests still pass | PASS (149 total) |

## Threat Model Validation

| Threat ID | Mitigation Applied |
|-----------|--------------------|
| T-85-02-01 (SQL injection) | All queries use cursor.execute(structural_sql) with no f-string interpolation on dynamic values. The only f-string with "WHERE" in the file is in a docstring (line 45) explaining the mitigation |
| T-85-02-02 (CSV injection) | `_has_csv_injection_leader` checks shelfmark / title_heb / genizah_title for leading =/+/-/@; matched rows EXCLUDED (not sanitized) and logged with `ambiguity_kind='csv_injection_leader'`. Defense-in-depth re-check at write time aborts SystemExit if a leader reaches `_build_synthetic_rows`. Tested by `test_csv_injection_fail_loud` (qualifying-set level) and `test_csv_injection_excludes_row` (residue-write level) |
| T-85-02-03 (collision attack) | D-01a fail-loud at `_build_synthetic_rows`: `if sys_id in real_alma_ids: raise SystemExit(...)`. Tested by `test_collision_check_fails_loud` (plants colliding sys_id and asserts SystemExit). The `000000` suffix is the discriminator vs NLI's `205171` institution suffix |
| T-85-02-04 (idempotency / data corruption) | Marker-fenced block + DETERMINISTIC ORDERING (8 explicit ORDER BY clauses) + sorted iteration over `qualifying` dict + `json.dumps(sort_keys=True)` produce byte-identical output. Verified on real data: two consecutive --apply runs produce diff-clean libraries.csv AND manifest.json |
| T-85-02-05 (loader DoS) | Loader's existing `if not row or len(row) < 3: continue` swallows empty rows. New `if raw_sys_id.startswith('#'): continue` swallows comment rows. csv_bank loaded 260,650 records from real-data libraries.csv post-regen without exception |
| T-85-02-06 (information disclosure) | InventoryIds are FIST.db identifiers, not PII or auth tokens. By-design exposure |
| T-85-02-07 (silent fan-out) | D-05a STRICT covers BOTH multi-inventory (408 cases) AND multi-signature (10,281 cases) ambiguities. All excluded; both kinds tested by `test_ambiguity_residue_multi_inventory_logged` and `test_ambiguity_residue_multi_signature_logged` |
| T-85-02-08 (cross-plan inconsistency) | Mitigated via authoritative manifest pattern: `fist_data/synthetic_manifest.json` is the AUTHORITATIVE qualifying-set committed in this plan; Plan 03 reads it as its only InventoryId source. Tested by `test_manifest_is_authoritative_for_plan_03` |

## Self-Check: PASSED

Verified before writing this summary:

- `scripts/generate_synthetic_rows.py` exists at the worktree path
- `fist_data/synthetic_manifest.json` exists at the worktree path
- `reports/synthetic_ambiguity_residue.csv` exists at the worktree path
- `reports/synthetic_coverage.md` exists at the worktree path
- `tests/test_generate_synthetic_rows.py` exists at the worktree path
- `genizah_core.py` line 3377 contains the `if raw_sys_id.startswith('#'): continue` guard
- Commit `e5a69a83` exists in worktree git log (Task 1)
- Commit `c613fb06` exists in worktree git log (Task 2)
- 86 synthetic-related tests pass + 63 shelfmark_bridge tests pass + csv_bank loads 260,650 records cleanly
- Real-data --apply byte-identity verified across two consecutive runs

## Recommendation: SYNTH-03 REQUIREMENTS Amendment

The current REQUIREMENTS / ROADMAP wording for SYNTH-03 says synthetic rows must be discoverable in "all standard search modes (text/title/shelfmark/Responsa)". Both Codex and Gemini reviewers (MEDIUM closures) flagged this as broader than the current architecture supports:

- `genizah_core.py:7372-7373` routes ONLY Title and Shelfmark modes through `_execute_metadata_search` (the csv_bank-backed metadata-only path).
- Text and Responsa modes use the Tantivy index. Synthetic rows have no transcription text → they have no Tantivy chunks → they cannot match Text/Responsa queries.

Recommended amendment:
- **Narrow SYNTH-03** to "Title and Shelfmark search modes" (matches current architecture; honest scope), OR
- **Defer Tantivy stub-rows** to a future infrastructure phase (would let synthetic rows match Text/Responsa via metadata-only stub chunks).

This amendment is documented in `reports/synthetic_coverage.md` §"SYNTH-03 Search Mode Coverage" so the cross-link is preserved.

## Next Phase Readiness

- **Plan 03 (FJMS sidecar AlmaId pre-population)** can now read `fist_data/synthetic_manifest.json` as its only InventoryId source. The manifest has 5,035 entries with `inventory_id` and `synthetic_sys_id` keys; Plan 03 walks them and adds UNION ALL rows in the FJMS sidecar exporter pointing each AlmaId-keyed table at the synthetic sys_id.
- **Plan 04 (browse / search / API surface)** can rely on `is_synthetic_sys_id(sys_id)` branch points to apply D-06 (quiet degradation, hide NLI elements) and D-08 (CUDL-as-default-image-source, falling back to metadata-only when no manifest exists). The 5,035 synthetic rows in csv_bank have populated shelfmark + title + library_code fields, so the existing Phase 53 metadata-only browse path handles them with zero new branches.
- **Phase 86 (CUDL Coverage Audit)** has `reports/synthetic_ambiguity_residue.csv` (10,689 rows with `ambiguity_kind` column) to consume in AUDIT-01 re-run. The notable finding — zero CUDL+FJMS overlap because all CUDL-matching FIST shelfmarks fall into multi_signature exclusion — should be surfaced in AUDIT-02's `reports/cudl_coverage.md` deliverable.
- **No blockers** for Wave 3 plans.

---
*Phase: 85-synthetic-fjms-inventory-rows*
*Plan: 02*
*Completed: 2026-05-08*
