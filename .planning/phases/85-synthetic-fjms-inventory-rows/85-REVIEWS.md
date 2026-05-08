---
phase: 85
reviewers: [gemini, codex]
reviewed_at: 2026-05-08T08:01:25Z
plans_reviewed:
  - 85-01-PLAN.md
  - 85-02-PLAN.md
  - 85-03-PLAN.md
  - 85-04-PLAN.md
  - 85-05-PLAN.md
gemini_model: gemini-2.5-pro (after default gemini-3-flash-preview hit 429 RESOURCE_EXHAUSTED)
codex_model: default
---

# Cross-AI Plan Review — Phase 85 (Synthetic FJMS Inventory Rows)

## Gemini Review

### Plan 85-01: Helper Module

**Summary:** Meticulously designs and implements the core `is_synthetic_sys_id` helper module, including encoding and decoding functions, with a comprehensive test suite. Correctly addresses collision scenarios, digit-normalization stability, and input validation.

**Strengths**
- SYNTH-01 fully met; clear API contract per D-01.
- Robust testing covers boundary cases, real Alma collisions (D-01a), D-13 normalization, and input validation.
- Idempotent and deterministic pure functions.
- Reusable fixtures via `synthetic_fixtures.py`.
- String discipline (D-01b) — explicitly forbids `int()` conversion at call sites.
- Pure-function discipline (no I/O at module load).

**Concerns**
- None. D-13 is documented to accept already-normalized input — clear and consistent.

**Suggestions**
- None. Plan is comprehensive.

---

### Plan 85-02: Synthetic Rows + libraries.csv Generation

**Summary:** Crucial regeneration script for synthetic rows. Handles idempotency, collision detection, ambiguity exclusion, audit artifacts. The one-line modification to `genizah_core.py` is minimal and effective.

**Strengths**
- Idempotency (D-04a) via marker block + byte-identical output guarantee.
- Collision Detection (D-01a) — fail-loud on synthetic↔real Alma collision.
- Ambiguity Exclusion (D-05a) — strict exclusion + residue logging for Phase 86.
- Audit Artifacts (D-03) — manifest + coverage report.
- CSV-Injection Mitigation (T-85-02-02) and parameterized SQL (T-85-02-01).
- Surgical loader change (one-line `startswith('#')` guard).

**Concerns**
- **HIGH — Cross-plan consistency of qualifying set:** Plan 02's `_build_qualifying_inventories` is Python-based using `cudl_normalize`; Plan 03 (running parallel in Wave 2) uses simplified SQL string manipulation `LOWER(REPLACE(REPLACE(REPLACE(sig.Signature, ' ', ''), '-', ''), '.', '')))`. Direct divergence in normalization will cause different inventory sets to qualify across the two plans → broken UX where a synthetic search result lacks corresponding browse-page metadata.
- **MEDIUM — SYNTH-03 wording:** Roadmap says "all search modes" but research clarifies text/Responsa won't return synthetic rows by design.
- **LOW — Testability of FIST/NLI connections:** Refactoring `_build_qualifying_inventories` to accept connection objects is cleaner than monkeypatching `sqlite3.connect`.

**Suggestions**
- Centralize qualifying-set logic as a single canonical SQL CTE or Python function consumed by both scripts.
- Refine SYNTH-03 success criteria to "discoverable in metadata-driven search modes (Title/Shelfmark)".
- Refactor connection handling for testability.

---

### Plan 85-03: FJMS Export UNION ALL

**Summary:** Correctly modifies `export_fist_enrichment.py` to UNION synthetic AlmaId rows. Maintains D-01 (`fjms_service.py` byte-identical). Robust in-memory testing.

**Strengths**
- SYNTH-05 fully met; D-01 layered-not-extended preserved.
- Robust SQL pattern (`UNION ALL` + `LEFT JOIN dbo_InventoryAlma WHERE alma.AlmaId IS NULL`).
- SQL-native ID generation via `printf`.
- In-memory SQLite for tests; no mutation of large on-disk DBs.
- Post-export `_validate_synthetic_export` collision check.
- Performance-aware (negligible row count vs existing data).

**Concerns**
- **HIGH — Cross-plan consistency of qualifying set (Critical):** The simplified SQL CUDL matching diverges from Plan 02's `cudl_normalize`. Different inventories will qualify in `fjms_enrichment.db` vs `libraries.csv` → direct violation of the "exact same inventory set" intent.
- **MEDIUM — Idempotency correctness (T-85-03-06):** Plan accepts non-deterministic row ordering due to `SELECT DISTINCT` without `ORDER BY`. While `fjms_service` is order-agnostic, byte-identical output is preferred for build artifacts (CI/diffs).

**Suggestions**
- **CRITICAL — Harmonize qualifying-set logic:**
  1. Extract canonical `cudl_normalize` to SQL — reimplement as SQLite UDF or nested REPLACE/REGEXP, use that single canonical SQL form in both plans.
  2. Add a programmatic comparison test that asserts InventoryIds from Plan 02's Python `_build_qualifying_inventories` exactly equal those in Plan 03's `temp.qualifying_synthetic_inventories`.
- Enforce deterministic `fjms_enrichment.db` output via `ORDER BY` on every `SELECT DISTINCT`.

---

### Plan 85-04: Browse Hide-NLI + CUDL-Default

**Summary:** Meticulously implements D-06 quiet degradation + D-08 CUDL-as-default across web + desktop. Correctly identifies and gates network calls to NLI Alma endpoints (D-14).

**Strengths**
- SYNTH-04 fully met — seamless UX for synthetic rows.
- D-06 quiet degradation (no badge, no banner).
- D-14 network-call guards prevent NLI 404 spam.
- Web + desktop parity enforced.
- Thorough hide-list audit in research.
- Test isolation via `patch` for network calls.
- Grep-based audit test (`TestUiHideListGrep`) as a coarse-grained safety net.

**Concerns**
- **MEDIUM — Completeness of UI hide sites:** "Estimated 22-26 call-site branches" — risk of missing a subtle NLI-dependent element. Grep test is coarse; behavior-oriented test (DOM inspection / screenshot) would increase confidence.
- **MEDIUM — D-08 `total_pages` for synthetic+CUDL:** Plan correctly notes need to populate from `len(cambridge_images)` but action steps focus on `browse.py`/API endpoints rather than `web/services.WebDataService.get_browse_page`. No specific test case for this behavior.

**Suggestions**
- Add explicit action item + code snippet for `web/services.WebDataService.get_browse_page` modification + corresponding test case.
- Enhance smoke tests with assertions about *absence* of NLI fields and *presence* of CUDL images/FJMS data in `BrowsePage`.

---

### Plan 85-05: Public API + Community Round-Trip

**Summary:** Completes integration by exposing `is_synthetic` field, tagging PostHog events, verifying round-trip for community features, deferring corrections-write via UI button hide.

**Strengths**
- SYNTH-06 fully met (modulo deferred corrections-write).
- Public API transparency (D-14) — downstream consumers don't reverse-engineer the ID format.
- PostHog tagging for analytics segmentation.
- Pragmatic corrections deferral (D-10).
- Verified round-tripping for sys_id-keyed features.
- Schema stability — additive change, `SCHEMA_VERSION=1` justified.
- Comprehensive doc updates (`docs/SEARCH_API.md`, `CHANGELOG.md`).

**Concerns**
- **LOW — PostHog `is_synthetic` for /api/parallels:** Tagging by seed is correct but limited; `any()` over results would be more informative (though synthetic naturally won't appear).
- **LOW — Desktop "Add correction" button hiding completeness:** Multiple UI entry points (context menu, shortcut) need verification.

**Suggestions**
- Comprehensive desktop manual smoke for all corrections-write entry points.
- Confirm `docs/SEARCH_API.md` mentions `is_synthetic` applies to `/api/parallels` items too.

---

### Overall Assessment

**Strengths:** Comprehensive research; architectural adherence (layered-not-extended, opaque-string sys_ids); robust data integrity guardrails; user-centric design; high auditability; thorough testing strategy.

**Concerns**
- **CRITICAL/HIGH — Cross-plan consistency of qualifying-set logic** between Plan 02 (Python `cudl_normalize`) and Plan 03 (simplified SQL string manipulation). High-probability path to inconsistent inventory sets.
- **MEDIUM — SYNTH-03 wording ambiguity** ("all modes" vs "metadata-driven only").
- **MEDIUM — Non-deterministic `fjms_enrichment.db` output** (T-85-03-06).
- **MEDIUM — UI hide-list completeness** — "estimated" sites + grep tests.

**Suggestions**
1. **IMMEDIATE: Harmonize qualifying-set logic** via canonical SQL UDF, shared SQL file, or cross-plan integration test.
2. Refine SYNTH-03 wording.
3. Enforce `fjms_enrichment.db` idempotency via `ORDER BY`.
4. Strengthen UI tests with absence/presence assertions.

**Risk Assessment: MEDIUM.** The plans are exceptionally detailed and robust. The cross-plan inconsistency risk is significant but addressable. With harmonization, the risk drops to LOW.

---

## Codex Review

### Overall Summary

The architecture is directionally good: synthetic IDs as a shared contract, data-layer enrichment instead of runtime service branching, and quiet browse degradation are the right moves. But the plans are not yet execution-safe. The biggest risks are Plan 02/03 qualifying-set divergence, Plan 03's non-deterministic SQLite output and stale table list, and Plan 04's incomplete NLI/KTIV audit. As written, Phase 85 could ship rows in `libraries.csv` whose synthetic AlmaIds do not exist in `fjms_enrichment.db`, or browse paths that still call NLI for synthetic IDs.

**Overall phase risk: HIGH** until the cross-plan data contract and hide-list are tightened.

---

### 85-01 Helper Module

**Summary:** Solid helper plan, but it slightly weakens D-01b by blessing integer input and relies on suffix collision checks rather than a stronger provenance check.

**Strengths**
- Good strict discriminator: digit-only, length 18, prefix `99`, suffix `000000` (`85-01-PLAN.md:143-154`, `456`).
- Explicit negative fixture for real Alma `990025143260205171` (`85-01-PLAN.md:263`).
- Boundary tests include min/max InventoryId and bool rejection (`85-01-PLAN.md:124-130`, `385-386`).

**Concerns**
- **MEDIUM:** `is_synthetic_sys_id(990001234560000000) -> True` is explicitly tested (`85-01-PLAN.md:121`, `357`). That makes the helper tolerant of the exact `int()` misuse D-01b tries to prevent.
- **MEDIUM:** The helper contract is only format-based. If Alma ever emits a real 18-digit ID ending in `000000`, the helper misclassifies it until collision checks catch the artifact.
- **LOW:** The "origin case" fixture uses illustrative InventoryId `329960` (`85-01-PLAN.md:256`); downstream plans should not treat it as real data.

**Suggestions**
- Keep `str()` coercion if desired, but remove the positive int test and add a repo grep/lint test for `int(sys_id)` and `int(...sys_id...)`.
- Make collision scans authoritative in Plan 02/03, but keep the helper pure and document that format detection is not proof of row existence.

---

### 85-02 Synthetic Rows

**Summary:** The marker-block regeneration concept is good, but the qualifying predicate is incomplete and idempotency is weaker than claimed.

**Strengths**
- Marker block plus manifest is the right operational/audit split (`85-02-PLAN.md:81-83`, `446`).
- Preserves line endings and tests double-run byte identity (`85-02-PLAN.md:377`, `904`).
- Adds loader tolerance for comment rows (`85-02-PLAN.md:187-215`).

**Concerns**
- **HIGH:** D-02 says "catalog title, scholarly description, measurement record, OR bibliography." The proposed predicate only checks `Title`, `TitleHeb`, `GenizahTitle` (`85-02-PLAN.md:505-542`). This will miss FJMS-only inventories that qualify only by bibliography, measurement, or free description.
- **HIGH:** Ambiguity exclusion is weaker than D-05a. It excludes only multiple inventories, not multiple FIST signatures (`85-02-PLAN.md:528-540`), while D-05a says multiple signatures should be excluded/logged.
- **HIGH:** Byte-identical output is not guaranteed. The SQL has no `ORDER BY`, and `claims[0]` selects an arbitrary row for a key (`85-02-PLAN.md:538`). Sorting by InventoryId later does not fix nondeterministic title/shelfmark selection.
- **MEDIUM:** CSV injection mitigation (`85-02-PLAN.md:930`) protects accidental Excel opening, but it can pollute app-visible titles/shelfmarks because runtime uses `csv.reader`, not Excel.
- **MEDIUM:** Claiming "Title/Shelfmark/Responsa" discoverability is wrong (`85-02-PLAN.md:83`). Actual code routes only `Title` and `Shelfmark` to metadata search (`genizah_core.py:7371-7373`).

**Suggestions**
- Extract a shared qualifying-set module or make Plan 02 emit the authoritative `synthetic_manifest.json`, then have Plan 03 consume it.
- Add `ORDER BY inv.InventoryId, sig.SignatureId, cat.UnitCatalogRecId` and exclude or deterministically resolve multi-signature rows.
- Expand FJMS metadata qualification to bibliography/free description/measurements, or explicitly narrow D-02.
- Prefer fail/log for formula-leading fields in operational `libraries.csv`; sanitize only spreadsheet-facing reports.

---

### 85-03 FJMS Export

**Summary:** The data-layer approach is right, but this plan is the most fragile. It duplicates Plan 02's predicate, accepts row-order drift, and appears stale against the actual exporter table list.

**Strengths**
- Preserves `shared/fjms_service.py` unchanged, which is the right layering (`85-03-PLAN.md:356`, `536`).
- Uses synthetic IDs in `AlmaId`, so existing `WHERE AlmaId = ?` calls can work.
- Has an explicit cross-plan consistency test concept (`85-03-PLAN.md:147`).

**Concerns**
- **HIGH:** Cross-plan consistency is not strong enough. Plan 03 says the predicate "matches Plan 02" (`85-03-PLAN.md:372`), but then uses a coarse SQL superset for CUDL matching (`85-03-PLAN.md:404-410`). This can both miss Plan 02 rows and add extra sidecar rows.
- **HIGH:** Wave 2 parallelism is unsafe. Plan 03 should depend on Plan 02's manifest, or both should depend on a shared generator.
- **HIGH:** Actual `scripts/export_fist_enrichment.py` has AlmaId tables like `catalog_running_titles`, `catalog_fields`, `catalog_textual_frames`, `catalog_mentions`, `catalog_refs` (`scripts/export_fist_enrichment.py:7-19`). The plan's must-have table list mentions `measurements`, `manuscript_measurements`, `extra_info`, `computed_measurements` (`85-03-PLAN.md:29`), which do not match the current exporter.
- **HIGH:** T-85-03-06 accepts no `ORDER BY` despite also requiring deterministic export (`85-03-PLAN.md:357`, `562`). `SELECT DISTINCT` + `UNION ALL` without `ORDER BY` is not byte-stable.
- **MEDIUM:** The post-export duplicate check described as `COUNT(*) = COUNT(DISTINCT AlmaId)` (`85-03-PLAN.md:27`) is invalid for one-to-many tables like bibliography/domains.

**Suggestions**
- Make Plan 03 consume `fist_data/synthetic_manifest.json` as the only InventoryId source.
- Enumerate AlmaId-keyed tables from the live exporter, not from older research notes.
- Add `ORDER BY AlmaId, stable table-specific primary keys` to every export query if byte stability matters; otherwise explicitly verify semantic stability, not byte identity.
- Use table-specific duplicate invariants, not global distinct-AlmaId equality.

---

### 85-04 Browse Hide-NLI

**Summary:** The intended UX is right, but the grep scope is incomplete and the service-layer page-count work is missing from `files_modified`.

**Strengths**
- Correctly prioritizes early NLI network-call guards (`85-04-PLAN.md:31-34`, `603`).
- Tests real Alma short-circuit regression (`85-04-PLAN.md:162`, `607`).
- Includes desktop parity as a requirement (`85-04-PLAN.md:37`).

**Concerns**
- **HIGH:** Hide-list is incomplete. Actual grep finds unplanned KTIV/NLI sites in `web/components/bibliography_dialog.py:51,283`, `desktop/dialogs_scholarly.py:115,1290`, `desktop/result_dialog.py:2809`, `web/static/manuscript_viewer.js:37,134`, `web/pages/search_results.py:646,1193`, and others.
- **HIGH:** D-08 needs `total_pages` from CUDL manifest, but `web/services.py` is not in `files_modified` (`85-04-PLAN.md:13-20`). UI source switching alone will not guarantee browse next/prev works.
- **MEDIUM:** The tests rely heavily on grep occurrence counts (`85-04-PLAN.md:574-579`), which can pass while the wrong branch is guarded.
- **MEDIUM:** `/api/fl_ids` returning 204 may break clients that call `.json()`; 204 is fine for image tags, less clearly fine for JSON endpoints.

**Suggestions**
- Replace the "22-26 sites" estimate with an exhaustive audit of `PNX_MANUSCRIPTS`, `nli_image_by_sysid`, `fl_ids`, `iiif.nli`, `KTIV`, and `fetch_marc`.
- Add `web/services.py` changes/tests for synthetic+CUDL `total_pages`, `cambridge_images`, and metadata-only fallback.
- Add runtime tests for bibliography dialogs and scholarly dialogs, not just browse page source.

---

### 85-05 API / Community

**Summary:** Adding `is_synthetic` is appropriate and `SCHEMA_VERSION=1` is defensible, but the tests are too mock-heavy and the parallels/corrections details are shaky.

**Strengths**
- Top-level `is_synthetic` is the right API shape (`85-05-PLAN.md:31`, `150`).
- Keeping schema version 1 is reasonable for an additive field (`85-05-PLAN.md:180`, `716`).
- Corrections-write deferral is explicitly documented (`85-05-PLAN.md:39`, `675`).

**Concerns**
- **HIGH:** `/api/parallels` does not take a `sys_id` seed; it takes `text` (`web/search_api.py:297-340`). The plan's `captured_state['is_synthetic'] = is_synthetic_sys_id(seed_sys_id)` and test posting `sys_id` are wrong (`85-05-PLAN.md:315-317`, `583-593`).
- **HIGH:** `tests/test_search_serializer.py` already exists and is large; the plan says "Create" it (`85-05-PLAN.md:319-334`). This risks overwriting existing coverage.
- **MEDIUM:** Comments test contains `pass` in the proposed body (`85-05-PLAN.md:527-530`), so the round-trip claim can go green without testing comments.
- **MEDIUM:** Backend corrections remain permissive (`85-05-PLAN.md:718`). That may be acceptable under D-10, but then SYNTH-06 is only partially met, not fully "round-trip corrections."
- **LOW:** OpenAPI uses a generic `additionalProperties: True` success schema (`web/search_api.py:583-590`), so docs may be enough, but examples should be updated or clients won't discover `is_synthetic`.

**Suggestions**
- For parallels, tag `is_synthetic` as `any(result.is_synthetic for results+filtered)` or omit the PostHog property for parallels.
- Append tests to existing `tests/test_search_serializer.py`; do not recreate it.
- Replace mock-only community tests with at least one service-level test per actual function, and no `pass` placeholders.
- If corrections are deferred, make backend reject synthetic corrections with a clear 400/disabled code, or explicitly record this as a requirement deferral.

---

### Phase-Level Assessment

**Strengths**
- The broad shape is good: synthetic rows in `libraries.csv`, synthetic AlmaIds in FJMS sidecar, shared helper, no `fjms_service.py` rewrite.
- The plans correctly avoid fabricating NLI metadata and reuse `CUL`/`Mosseri`.
- The browse UX direction matches the requirement: no badge, no broken NLI UI, Cambridge default when available.

**Highest-Risk Gaps**
- **HIGH:** Plan 02 and Plan 03 can diverge on InventoryId membership. This is the critical failure mode.
- **HIGH:** The phase does not satisfy "all standard search modes" literally. Actual code only metadata-searches Title/Shelfmark; text/Responsa use Tantivy and synthetic rows have no chunks (`genizah_core.py:7371-7375`).
- **HIGH:** Hide-list is not exhaustive across dialogs/components/static JS.
- **HIGH:** Audit requirements AUDIT-01/02/03 are not covered by these five plans; `REQUIREMENTS.md:45-47` still need a closing audit phase or explicit deferral.
- **MEDIUM:** Repeated collision checks are good defense-in-depth only if they all call the same helper and use the same real/synthetic corpus. As written, Plan 02 and Plan 03 checks can drift.

**Required Changes Before Execution**
1. Make Plan 02's manifest the authoritative qualifying set and make Plan 03 depend on it.
2. Fix Plan 03 table enumeration against the actual exporter.
3. Add deterministic ordering or downgrade byte-identity claims to semantic checks.
4. Expand hide-list audit beyond the six files in Plan 04.
5. Clarify SYNTH-03 wording: either add real indexing/metadata overlay for all modes, or state synthetic rows are discoverable via Title/Shelfmark only.
6. Add a Phase 85 audit/verification plan for `scan_cudl_orphans.py` and `reports/cudl_coverage.md`.

**Final Risk Assessment: HIGH.** The implementation approach is sound, but plan quality is not yet sufficient for autonomous execution without material risk of data mismatch and missed NLI call sites.

---

## Consensus Summary

> Two reviewers (Gemini gemini-2.5-pro, Codex). Both converge on the cross-plan qualifying-set divergence as the dominant risk; they diverge on overall risk level (Gemini: MEDIUM, Codex: HIGH).

### Agreed Strengths (both reviewers)
- Synthetic IDs as shared contract via `shared/synthetic_sys_id.py` (D-01) — right architectural lever.
- Pre-populating AlmaId in `fjms_enrichment.db` keeps `shared/fjms_service.py` byte-identical (D-01 layered-not-extended).
- Browse quiet degradation (D-06) and Cambridge-default-for-synthetic (D-08) match the user's "no badge, no apologies" stance.
- Marker block + audit manifest is the right operational/audit split.
- Strict format discriminator for `is_synthetic_sys_id` (length 18, prefix 99, suffix 000000).
- D-14 network-call guards prevent NLI 404 spam.
- Web + desktop parity enforced.
- Strong audit trail (`synthetic_manifest.json`, `synthetic_coverage.md`, `synthetic_ambiguity_residue.csv`).

### Agreed Concerns (priority order — both reviewers raised these)

1. **HIGH (BOTH) — Cross-plan qualifying-set divergence.** Plan 02's Python `_build_qualifying_inventories` (using `cudl_normalize`) and Plan 03's SQL `_create_qualifying_inventories_temp_table` (using simplified `LOWER(REPLACE(REPLACE(REPLACE...)))`) implement the predicate independently. They will diverge — synthetic libraries.csv rows whose AlmaIds have no fjms_enrichment.db backing. This is the dominant risk both reviewers cite.

2. **HIGH (Codex) / MEDIUM (Gemini) — SYNTH-03 wording overpromises.** Roadmap says "all standard search modes" but only Title/Shelfmark route through `_execute_metadata_search`; text/Responsa use Tantivy and synthetic rows have no chunks.

3. **MEDIUM (BOTH) — Idempotency weaker than claimed (Plan 02 + Plan 03).** Both plans use `SELECT DISTINCT` without `ORDER BY`; SQLite ordering is implementation-defined; byte-equality assertions can flake. Plan 02 also uses `claims[0]` arbitrary selection.

4. **MEDIUM (BOTH) — UI hide-list completeness (Plan 04).** Both flag risk of missed NLI/KTIV sites. Codex names specific unplanned files (`web/components/bibliography_dialog.py`, `desktop/dialogs_scholarly.py`, `desktop/result_dialog.py`, `web/static/manuscript_viewer.js`, `web/pages/search_results.py`); Gemini calls the audit "estimated" and notes grep tests are coarse.

5. **MEDIUM (BOTH) — D-08 `total_pages` plumbing missing.** Both note `web/services.py` (or `WebDataService.get_browse_page`) needs synthetic-aware `total_pages` from `len(cambridge_images)`, but the modification isn't in Plan 04's `files_modified` list and lacks a specific test case.

### Codex-only concerns (require resolution)

- **HIGH — Plan 02 D-02 predicate incomplete:** Only checks Title/TitleHeb/GenizahTitle, dropping bibliography/measurement/free-description-only inventories that D-02 explicitly admits.
- **HIGH — Plan 02 D-05a too narrow:** Excludes multi-inventory keys but not multi-signature keys; D-05a wants both.
- **HIGH — Plan 03 stale table list:** Must-haves cite `measurements`, `manuscript_measurements`, `extra_info`, `computed_measurements` not in current exporter; missing `catalog_running_titles`, `catalog_fields`, `catalog_textual_frames`, `catalog_mentions`, `catalog_refs`.
- **HIGH — Plan 05 parallels API contract wrong:** `/api/parallels` accepts `text`, not `sys_id`. The captured_state plumbing and round-trip test post the wrong shape.
- **HIGH — Plan 05 collides with existing test file:** `tests/test_search_serializer.py` exists and is large; Plan 05 says "Create" — risks overwrite.
- **HIGH — AUDIT-01/02/03 not in this phase:** Phase 86 named in ROADMAP but five Phase-85 plans don't reference it; post-Phase-85 verification loop unspecified.
- **MEDIUM — CSV-injection sanitization pollutes runtime data:** Single-quoted titles appear in app UI (csv.reader does no Excel sanitization on read).
- **MEDIUM — `/api/fl_ids` 204** may break JSON-expecting clients.
- **MEDIUM — Plan 04 grep-based tests** assert occurrence counts, not branch correctness.
- **MEDIUM — Plan 05 `pass`-bodied comments test** lets SYNTH-06 round-trip claim go green without actually testing comments.
- **MEDIUM — Helper format-only contract:** `is_synthetic_sys_id(int)` accepting integer input slightly weakens D-01b.

### Gemini-only concerns (lower priority)

- **LOW — Testability of FIST/NLI connections** in Plan 02: refactor to accept connection objects rather than monkeypatch `sqlite3.connect`.
- **LOW — Desktop "Add correction" button** — verify all UI entry points (context menu, shortcut).
- **LOW — `is_synthetic` for /api/parallels** — tag by seed is correct but `any()` over results would be more informative.

### Divergent Views

- **Overall risk level:** Gemini says MEDIUM (citing extensive planning that mitigates most risks); Codex says HIGH (citing concrete plan-text bugs in addition to architecture). The difference is whether the additional Codex-specific HIGH findings (parallels API contract, table list staleness, AUDIT-01/02/03 omission) materially change the risk profile. Treat as HIGH until those concrete bugs are resolved; downgrade to MEDIUM after.

### Required Before Execution (synthesis)

1. **Harmonize qualifying-set logic.** Make Plan 02's `synthetic_manifest.json` authoritative; have Plan 03 read InventoryIds from it. Eliminate parallel predicates entirely. Add a cross-plan integration test that asserts the InventoryId sets are equal.
2. **Fix Plan 03 table enumeration** against the live `scripts/export_fist_enrichment.py` header (lines 7-19). Drop stale names, add the actual ones.
3. **Expand Plan 02 D-02 predicate** to include bibliography / measurement / free-description signals — or explicitly narrow D-02 in CONTEXT.
4. **Fix Plan 02 D-05a ambiguity** to exclude multi-signature keys, not just multi-inventory.
5. **Add deterministic ordering** to all generation/UNION SQL. Use `ORDER BY inv.InventoryId, sig.SignatureId, cat.UnitCatalogRecId` and replace `claims[0]` with a deterministic tie-break.
6. **Run exhaustive grep audit** for `PNX_MANUSCRIPTS|nli_image_by_sysid|fl_ids|iiif.nli|KTIV|fetch_marc` and add every site to Plan 04. Add `web/components/bibliography_dialog.py`, `desktop/dialogs_scholarly.py`, `desktop/result_dialog.py`, `web/static/manuscript_viewer.js`, `web/pages/search_results.py` to `files_modified`.
7. **Add `web/services.py`** to Plan 04 `files_modified` for `total_pages` + `cambridge_images` synthetic-aware computation; add corresponding test.
8. **Fix Plan 05 parallels predicate** to read request body's `text` (not `sys_id`).
9. **Append to `tests/test_search_serializer.py`** rather than recreate it.
10. **Remove `pass` placeholders** in Plan 05 community tests; replace with at least one real service-level test per function.
11. **Either narrow SYNTH-03** to "Title/Shelfmark searchable" or add an indexing surface for text/Responsa modes.
12. **Reference AUDIT-01/02/03 (Phase 86)** explicitly from Phase 85 success criteria, or document deferral.
13. **Replace grep-count tests** in Plan 04 with branch-correctness assertions (e.g. parametrize over file/line, assert is_synthetic_sys_id check is the *governing* condition).
14. **Reconsider CSV-injection prefix-quote** — drop or fail-on-detect rather than mutate runtime-visible text.
15. **Reconsider `/api/fl_ids` 204** — return `{"fl_ids": []}` JSON 200 instead, since clients may call `.json()`.
16. **Either harden corrections-write backend** with a 400 reject on synthetic, or explicitly mark SYNTH-06 corrections as deferred.

---

## Next Step

Feed back into planning:

```
/gsd-plan-phase 85 --reviews
```

Or address findings manually before execution.
