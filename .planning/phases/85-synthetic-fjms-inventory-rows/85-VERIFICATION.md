---
phase: 85-synthetic-fjms-inventory-rows
verified: 2026-05-08T11:34:44Z
status: human_needed
score: 5/5 must-haves verified (with 1 operational gap requiring human action)
overrides_applied: 0
gaps:
  - truth: "FJMS sidecar (fist_data/fjms_enrichment.db) contains synthetic AlmaIds so browse dialogs populate"
    status: failed
    reason: "Plan 03 wired UNION ALL synthetic injection in the export script and tests pass, but the actual fist_data/fjms_enrichment.db on disk has NOT been regenerated since April 21 (pre-Phase-85). Currently 0 synthetic AlmaIds in catalog table. Until the sidecar is rebuilt against real FIST.db with the new manifest, opening a browse page for any synthetic sys_id will not show FJMS catalogue/bibliography/measurements data — defeating SYNTH-04 criterion 4."
    artifacts:
      - path: "fist_data/fjms_enrichment.db"
        issue: "Sidecar dated 2026-04-21 (1.6GB); contains 0 synthetic AlmaIds. Plan 03 SQL not yet applied to runtime data."
    missing:
      - "Run scripts/export_fist_enrichment.py against real fist_data/FIST.db to regenerate fist_data/fjms_enrichment.db with the 5,035 synthetic-AlmaId UNION ALL rows"
      - "Verify post-export catalog table contains 5,035 synthetic AlmaIds matching manifest"
      - "Deploy regenerated sidecar to web server + bundle in next desktop installer"
human_verification:
  - test: "Open browse for synthetic sys_id (e.g. /browse?sys_id=990000002099000000) on web app"
    expected: "Page renders with FJMS catalogue/bibliography/measurements panels populated; NLI elements (KTIV button, NLI source toggle, Alma metadata) hidden; if CUDL manifest available, Cambridge IIIF image loads; if not, metadata-only Phase 53 fallback with no broken-image placeholder; no console errors; no 404 noise in NLI logs"
    why_human: "Visual rendering of FJMS dialogs and image source quality cannot be verified by grep/static analysis; requires runtime against regenerated sidecar"
  - test: "Search 'T-S NS 329.96' (or equivalent FJMS-only shelfmark from manifest) in Shelfmark mode"
    expected: "Returns synthetic row with FJMS-derived title and matching call_numbers; clicking through to browse opens the synthetic-row page successfully"
    why_human: "End-to-end search → browse flow with live Tantivy index requires running web app and inspecting result cards"
  - test: "Add synthetic sys_id to a saved list, then reload the list"
    expected: "Round-trip preserves synthetic sys_id without crash; list item displays shelfmark; remove operation succeeds"
    why_human: "Real Supabase round-trip with auth; tests use mocks for the insert call but cannot exercise persistence + display"
  - test: "Click Edit/correction button on synthetic browse page"
    expected: "Web: button hidden; cannot bypass via UI. Desktop: btn_b_edit hidden; Ctrl+Shift+S programmatic shortcut shows 'Corrections not available' QMessageBox without crashing"
    why_human: "Desktop UI behavior + keyboard shortcut handling needs interactive verification"
  - test: "Run scripts/scan_cudl_orphans.py after Phase 85 to gauge orphan-classmark reduction (Phase 86 input)"
    expected: "Synthetic rows reduce orphan count; Phase 86 input artifacts ready"
    why_human: "Phase 86 verification scope; surfaces here only as confirmation that phase 85 lays the right groundwork"
  - test: "Open desktop app v7.11 (post-build): browse synthetic sys_id, attempt all features (lists/exclusions/corrections/external links/parallels)"
    expected: "Web+desktop parity preserved; no QMessageBox surprises beyond the corrections-disabled message; no Qt warnings; no crashes"
    why_human: "Desktop installer needs build + interactive smoke test; CLAUDE.md mandates 'both apps must be maintained'"
---

# Phase 85: Synthetic FJMS Inventory Rows Verification Report

**Phase Goal:** Independent libraries.csv rows for the ~93 T-S FJMS-only inventories (and any residue from Mosseri/Or post-Phase-84) using Option-2 18-digit synthetic sys_id format (`99` + InventoryId-padded-10 + `000000`).

**Verified:** 2026-05-08T11:34:44Z
**Status:** human_needed
**Re-verification:** No — initial verification

## Goal Achievement

The static / structural goal is achieved: helper module + generation script + 5,035 marker-fenced libraries.csv rows + manifest + audit residue + browse hide-NLI gates + public API is_synthetic field + corrections-write rejection are all wired, tested, and committed. 211 phase-85 tests pass.

The runtime goal is partially blocked by an operational deferral: `fist_data/fjms_enrichment.db` has not been regenerated against real FIST.db using Plan 03's UNION-ALL injection, so synthetic browse pages will not render FJMS catalogue/bibliography/measurement data until the sidecar is rebuilt. This was foreseen by Plan 03 ("Plan 05 should regenerate fjms_enrichment.db before its smoke checks") but Plan 05 SUMMARY's claim "Synthetic rows present in fjms_enrichment.db (Plan 03)" is overstated — the schema and SQL are present in the script, but the runtime data file has not been refreshed.

### Observable Truths (from ROADMAP success criteria)

| #   | Truth | Status | Evidence |
| --- | ----- | ------ | -------- |
| 1 | `is_synthetic_sys_id()` helper plus encode/decode utilities exist in shared code, with a test suite covering boundary cases | VERIFIED | `shared/synthetic_sys_id.py` (139 lines) exports the 3 functions; 64 tests in `tests/test_synthetic_sys_id.py` pass (TestIsSyntheticSysId / TestEncodeInventorySysId / TestDecodeInventoryId / TestRoundTrip / TestRealAlmaCollisionNegative / TestNoIntCoercion). Smoke: `is_synthetic_sys_id('990001234560000000')→True; is_synthetic_sys_id('990025143260205171')→False; encode_inventory_sys_id(123456)→'990000123456000000'; decode_inventory_id('990001234560000000')→1234560`. |
| 2 | Tantivy index includes synthetic rows so `T-S NS 329.96` etc. return search results in all modes | VERIFIED (NARROWED) | csv_bank loads 260,650 records (255,615 real + 5,035 synthetic, all with populated shelfmark/title). _execute_metadata_search at genizah_core.py:7398 routes Title+Shelfmark modes through csv_bank. **NARROWING DEVIATION:** ROADMAP wording says "all standard search modes (text/title/shelfmark/Responsa)" but implementation supports Title+Shelfmark only — text/Responsa use Tantivy chunks which synthetic rows lack. Documented in `reports/synthetic_coverage.md` §"SYNTH-03 Search Mode Coverage" with REQUIREMENTS amendment recommendation. Plan 02 SUMMARY explicitly flags this for follow-up. **Decision needed by user:** accept narrowing or schedule Tantivy-stub-rows infrastructure work. |
| 3 | Browse renders synthetic-row pages with FJMS catalogue/bibliography/measurements/CUDL manifest images, gracefully handling absent NLI fields | PARTIAL | Static wiring complete (12 source files modified; 35 browse-synthetic tests pass; 85-04-AUDIT.md enumerates every NLI/KTIV/PNX call site). NLI hide-gates verified at all enumerated sites. CUDL-default branch added at web/pages/browse.py auto-default block. **OPERATIONAL GAP:** fist_data/fjms_enrichment.db is dated 2026-04-21 with 0 synthetic AlmaIds — until regenerated, FJMS dialogs return empty for synthetic sys_ids in live app. |
| 4 | FJMS enrichment dialogs populate via InventoryId fallback when sys_id is synthetic | VERIFIED (architecturally) | D-01 layered-not-extended pattern applied: shared/fjms_service.py is byte-identical pre/post Plan 03 (verified `git diff shared/fjms_service.py` returns empty since master-main). Synthetic AlmaIds are pre-populated INTO the AlmaId column at export time via UNION ALL with sqlite-native `('99' \|\| printf('%010d', inv.InventoryId) \|\| '000000')` expression. The ~30 `WHERE AlmaId = ?` queries in fjms_service.py work transparently because the data layer accommodates the new ID format rather than threading detection branches through service code. **Same operational gap as truth 3:** sidecar must be regenerated for runtime payoff. |
| 5 | Lists, exclusions, parallels, comments, corrections round-trip synthetic sys_ids without crashes or silent data loss; web and desktop parity preserved | VERIFIED | 14 round-trip tests pass (TestListsRoundTrip, TestCommentsRoundTrip, TestExclusionsRoundTrip, TestParallelsTextInputTolerance, TestParallelsResultsNaturallyExcludeSynthetic, TestCorrectionsReadSafe). Corrections-write deferred at corrections_client.py:619 + supabase_corrections_client.py:803 with `synthetic_corrections_disabled` error — NOT a crash, intentional restriction. UI parity: web Edit toggle hidden (browse.py:3898), desktop btn_b_edit hidden (genizah_app.py:7383-7385) + defense-in-depth method-top guard at _browse_save_correction (3400-3406). |

**Score:** 5/5 truths verified (criteria 3+4 have operational gap; criterion 2 has documented narrowing deviation)

### Required Artifacts

| Artifact | Expected | Status | Details |
| -------- | -------- | ------ | ------- |
| `shared/synthetic_sys_id.py` | Three pure helpers + module constants | VERIFIED | 139 lines, exports is_synthetic_sys_id / encode_inventory_sys_id / decode_inventory_id; constants `_SYNTHETIC_PREFIX="99"`, `_SYNTHETIC_SUFFIX="000000"`, `_INVENTORY_PAD=10`, `_TOTAL_LENGTH=18`. No `import logging`, no I/O at import. |
| `tests/test_synthetic_sys_id.py` | Helper unit tests + D-01b lint | VERIFIED | 64 tests pass; 6 test classes including TestNoIntCoercion repo-grep lint. |
| `tests/fixtures/synthetic_fixtures.py` | Reusable golden cases | VERIFIED | SYNTHETIC_GOLDEN_CASES, REAL_ALMA_NEGATIVE_CASES, D13_NORMALIZATION_NEGATIVES exported. |
| `scripts/generate_synthetic_rows.py` | Idempotent regeneration script | VERIFIED | 735 lines; --dry-run / --apply mutually exclusive; CLI path overrides; D-01a collision check; D-05a strict ambiguity exclusion (multi_inventory + multi_signature); CSV-injection fail-loud. |
| `libraries.csv` (synthetic block) | 5,035 marker-fenced synthetic rows | VERIFIED | exactly 1 `# BEGIN SYNTHETIC` / 1 `# END SYNTHETIC` pair; 5,035 data rows between. All sys_ids pass is_synthetic_sys_id (verified 0/5,035 fails). All have populated shelfmark + title + library_code ∈ {CUL, Mosseri}. |
| `fist_data/synthetic_manifest.json` | Authoritative qualifying-set | VERIFIED | 5,035 entries, sorted ascending by inventory_id, with {inventory_id, synthetic_sys_id, source, canonical_shelfmark, library_code} keys. |
| `reports/synthetic_ambiguity_residue.csv` | D-05a residue | VERIFIED | 10,690 lines (header + 10,689 residue rows); columns include ambiguity_kind ∈ {multi_inventory, multi_signature, csv_injection_leader}. |
| `reports/synthetic_coverage.md` | D-03 + Phase 86 cross-link | VERIFIED | Tier 1=0, Tier 2=0, Tier 3=5,035; SYNTH-03 narrowing documented; Phase 86 cross-link (AUDIT-01/02/03) prominent. |
| `scripts/export_fist_enrichment.py` (modified) | UNION ALL synthetic in 12 AlmaId-keyed tables | VERIFIED (script-level) | 12 UNION ALL blocks across export_domains/joins/catalog/catalog_running_titles/catalog_sizes/catalog_fields/catalog_free_desc/catalog_full_texts/catalog_textual_frames/catalog_mentions/bibliography/catalog_refs; load_synthetic_manifest_into_temp_table loader present; _validate_synthetic_export post-export check. 33 tests pass. |
| `fist_data/fjms_enrichment.db` (regenerated) | Contains 5,035 synthetic AlmaIds | **MISSING** | Sidecar on disk dated 2026-04-21, 0 synthetic AlmaIds in catalog table. Operational regeneration deferred — see gaps section. |
| `genizah_core.py:_load_csv_bank` | Marker tolerance guard | VERIFIED | Line 3377 `if raw_sys_id.startswith('#'): continue` with Phase 85 D-04a comment. csv_bank loads 5,035 synthetic rows cleanly. |
| `web/api.py` synthetic gates | /api/fl_ids JSON empty list + /api/nli_image_by_sysid 204 | VERIFIED | Lines 476 + 608 implement the differentiated content-type-aware short-circuits. |
| `web/pages/browse.py` synthetic gates | KTIV/NLI hide + CUDL default + window.GENIZAH_IS_SYNTHETIC | VERIFIED | 12 occurrences of is_synthetic_sys_id including auto-default Cambridge branch + Edit toggle gate. |
| `web/pages/browse_enrichment.py` marc_bib short-circuit | VERIFIED | 2 occurrences. |
| `web/pages/search_results.py` image proxy gates | VERIFIED | 3 occurrences. |
| `web/components/bibliography_dialog.py` KTIV hide | VERIFIED | 3 occurrences. |
| `web/services.py` defensive marker | VERIFIED | 1 import marker for Phase 86 AUDIT-03. |
| `web/static/manuscript_viewer.js` JS gates | VERIFIED | 3 GENIZAH_IS_SYNTHETIC occurrences. |
| `desktop/viewers.py` KTIV hide | VERIFIED | 3 occurrences. |
| `desktop/dialogs_scholarly.py` KTIV hide | VERIFIED | 3 occurrences. |
| `desktop/result_dialog.py` KTIV hide | VERIFIED | 3 occurrences. |
| `genizah_app.py` corrections + KTIV gates | VERIFIED | 5 occurrences including btn_b_edit setVisible(False) + _browse_save_correction method-top guard + clipboard + browse_open_catalog. |
| `genizah_core.py` D-14 network guards | VERIFIED | 10 occurrences across fetch_iiif_manifest / fetch_marc_data / _fetch_single_worker / _fetch_fl_ids / format_with_link KTIV. |
| `shared/search_serializer.py` is_synthetic field | VERIFIED | 4 occurrences including top-level is_synthetic on _serialize_item (line 310) and serialize_browse_payload (line 646). SCHEMA_VERSION stays at 1 per Phase 83 stability commitment. |
| `web/api_hardening.py` PostHog is_synthetic | VERIFIED | 6 occurrences plumbing captured_state + capture_api_event signature + props dict. |
| `web/search_api.py` endpoint is_synthetic | VERIFIED | search/browse populate; parallels intentionally omits with documented rationale. |
| `corrections_client.py` synthetic rejection | VERIFIED | gate at create_correction line 619 returns `synthetic_corrections_disabled` error. |
| `supabase_corrections_client.py` synthetic rejection | VERIFIED | gate at create_correction line 803 returns same error before line-811 client.table('corrections').insert call. |
| `tests/test_browse_synthetic.py` | Browse hide-NLI tests | VERIFIED | 35 tests pass. |
| `tests/test_synthetic_round_trip.py` | Round-trip tests | VERIFIED | 14 passed, 2 skipped (parallels — search engine not in test env). |
| `tests/test_export_fist_synthetic.py` | Export synthetic tests | VERIFIED | 33 passed, 1 skipped. |
| `tests/test_generate_synthetic_rows.py` | Generation tests | VERIFIED | 22 tests pass. |
| `tests/test_search_serializer.py` (extended) | Phase 85 is_synthetic field tests | VERIFIED | 43 total tests pass; 11 new TestPhase85IsSynthetic + TestPhase85ExistingFilePreserved tests; pre-existing 818-line baseline preserved (now 1045 lines). |
| `.planning/phases/85-synthetic-fjms-inventory-rows/85-04-AUDIT.md` | NLI/KTIV/PNX site enumeration for Phase 86 | VERIFIED | 204 lines, authoritative AUDIT-03 input. |
| `docs/SEARCH_API.md` is_synthetic doc | VERIFIED | is_synthetic row in /api/search Response item fields table + /api/browse Top-level fields table + new v7.11 changelog subsection. |
| `CHANGELOG.md` v7.11 section | VERIFIED | [Unreleased] v7.11 section with Added/Changed/Deferred subsections + Phase 86 cross-link. |

### Key Link Verification

| From | To  | Via | Status | Details |
| ---- | --- | --- | ------ | ------- |
| `tests/test_synthetic_sys_id.py` | `shared/synthetic_sys_id.py` | `from shared.synthetic_sys_id import` | WIRED | imports verified |
| `scripts/generate_synthetic_rows.py` | `shared/synthetic_sys_id.py` | imports encode + is_synthetic | WIRED | 5,035 synthetic rows generated via encode |
| `scripts/generate_synthetic_rows.py` | `fist_data/synthetic_manifest.json` | writes authoritative manifest | WIRED | 5,035 entries on disk |
| `scripts/generate_synthetic_rows.py` | `libraries.csv` | rewrites marker-block | WIRED | 5,037 lines including markers |
| `genizah_core.py:_load_csv_bank` | `libraries.csv` synthetic block | startswith('#') skip | WIRED | csv_bank loads 260,650 records cleanly |
| `scripts/export_fist_enrichment.py` | `fist_data/synthetic_manifest.json` | load_synthetic_manifest_into_temp_table | WIRED (script) | manifest loads 5,035 entries; UNION ALL in 12 functions |
| `scripts/export_fist_enrichment.py` | `fist_data/fjms_enrichment.db` | regenerated sidecar | **NOT_RUN** | Script wired correctly; runtime data file unchanged. **Operational gap — see gaps section.** |
| `shared/fjms_service.py` | synthetic AlmaIds in fjms_enrichment.db | `WHERE AlmaId = ?` queries | WIRED (data layer) | D-01 layered: service queries unchanged; data accommodates new format |
| `web/api.py` /api/fl_ids handler | synthetic short-circuit | is_synthetic_sys_id | WIRED | 200 + {"fl_ids": []} |
| `web/api.py` /api/nli_image_by_sysid handler | synthetic short-circuit | is_synthetic_sys_id | WIRED | 204 No Content |
| `web/pages/browse.py` Edit button | synthetic hide | is_synthetic_sys_id | WIRED | line 3898 area |
| `genizah_app.py` btn_b_edit | synthetic hide | is_synthetic_sys_id | WIRED | lines 7383-7385 |
| `corrections_client.py:create_correction` | synthetic rejection | is_synthetic_sys_id | WIRED | line 619; returns synthetic_corrections_disabled |
| `supabase_corrections_client.py:create_correction` | synthetic rejection | is_synthetic_sys_id | WIRED | line 803 |
| `shared/search_serializer.py` envelope | is_synthetic field | _serialize_item + serialize_browse_payload | WIRED | top-level field; SCHEMA_VERSION=1 |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
| -------- | ------------- | ------ | ------------------ | ------ |
| `libraries.csv` synthetic block | row data | scripts/generate_synthetic_rows.py | Yes (5,035 rows from FIST.db) | FLOWING |
| `csv_bank` | synthetic entries | _load_csv_bank reads libraries.csv | Yes (5,035 entries with shelfmark+title populated) | FLOWING |
| `fist_data/synthetic_manifest.json` | authoritative qualifying-set | scripts/generate_synthetic_rows.py main() | Yes (5,035 entries) | FLOWING |
| `fist_data/fjms_enrichment.db` synthetic AlmaIds | UNION ALL synthetic rows | scripts/export_fist_enrichment.py | **No (0 synthetic AlmaIds in catalog)** | **DISCONNECTED — sidecar not regenerated** |
| `_serialize_item` is_synthetic | bool flag | is_synthetic_sys_id(final_sys_id) | Yes (computed per-call) | FLOWING |
| `captured_state['is_synthetic']` | PostHog property | derived from envelope items / browse sys_id | Yes (always populated for /api/search and /api/browse, intentionally None for /api/parallels) | FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| -------- | ------- | ------ | ------ |
| Helper functions importable + correct | `python -c "from shared.synthetic_sys_id import ..."` | OK | PASS |
| All 5,035 synthetic rows pass is_synthetic_sys_id | walk libraries.csv synthetic block | total=5035 fails=0 | PASS |
| csv_bank loads synthetic rows with populated fields | MetadataManager._load_csv_bank() | total=260,650, synthetic=5,035, sample shelfmark="RNL: Firkovitch II A 195/2", title populated | PASS |
| Manifest is sorted ascending by inventory_id | `sorted_correctly` check | True (5,035 entries) | PASS |
| FJMS sidecar contains synthetic AlmaIds | `SELECT COUNT(DISTINCT AlmaId) FROM catalog WHERE AlmaId LIKE '99%' AND length(AlmaId)=18 AND AlmaId LIKE '%000000'` | 0 | **FAIL — sidecar not regenerated** |
| Phase 85 unit tests pass | `pytest tests/test_synthetic_sys_id.py` | 64 passed in 0.84s | PASS |
| Generation tests pass | `pytest tests/test_generate_synthetic_rows.py` | 22 passed | PASS |
| Export tests pass | `pytest tests/test_export_fist_synthetic.py` | 33 passed, 1 skipped | PASS |
| Browse-synthetic tests pass | `pytest tests/test_browse_synthetic.py` | 35 passed | PASS |
| Round-trip tests pass | `pytest tests/test_synthetic_round_trip.py` | 14 passed, 2 skipped | PASS |
| Search serializer tests pass | `pytest tests/test_search_serializer.py` | 43 passed | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
| ----------- | ----------- | ----------- | ------ | -------- |
| SYNTH-01 | 85-01 | Helper module + tests | SATISFIED | shared/synthetic_sys_id.py + 64 tests pass |
| SYNTH-02 | 85-02 | User can search FJMS-only shelfmark | SATISFIED (architecturally — needs runtime smoke) | csv_bank loads 5,035 synthetic rows with populated shelfmark+title; Title+Shelfmark search modes route through metadata path |
| SYNTH-03 | 85-02 | Tantivy/all-modes coverage | NARROWED | Implemented for Title+Shelfmark only; text/Responsa narrowing documented in `reports/synthetic_coverage.md` with REQUIREMENTS amendment recommendation. Both Codex and Gemini reviewers (MEDIUM) flagged this as honest scope. **Decision needed by user:** accept narrowing or schedule Tantivy-stub-rows. |
| SYNTH-04 | 85-04 | Browse renders synthetic-row pages | SATISFIED (static) — BLOCKED (runtime) | Plan 04 hide-NLI gates verified across 12 source files; 35 tests pass; CUDL-default branch in browse.py. **Runtime payoff blocked by FJMS sidecar regeneration deferral.** |
| SYNTH-05 | 85-03 | FJMS service AlmaId fallback | SATISFIED (architecturally) | D-01 layered: shared/fjms_service.py byte-identical pre/post; export script UNION-ALL pattern in 12 functions; 33 tests pass. **Runtime payoff blocked by FJMS sidecar regeneration deferral.** |
| SYNTH-06 | 85-05 | Lists/exclusions/parallels/comments/corrections round-trip | SATISFIED | 14 round-trip tests pass; corrections-write deferred at 2 client classes + UI hide on web+desktop; public API is_synthetic field; PostHog telemetry asymmetric per endpoint semantics |

**All 6 requirement IDs from PLAN frontmatter accounted for. ROADMAP maps SYNTH-01..06 to Phase 85; no orphans.**

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
| ---- | ---- | ------- | -------- | ------ |
| `fist_data/fjms_enrichment.db` | N/A | Stale data file (dated 2026-04-21, 0 synthetic rows) | Blocker (operational) | Browse for synthetic sys_ids will not show FJMS data until regenerated |

(No code-level anti-patterns found. Static analysis on all 12 modified source files showed no TODO/FIXME/PLACEHOLDER stub markers in synthetic-row code paths. CSV-injection fail-loud + D-01a collision fail-loud + D-05a strict ambiguity exclusion are all wired in the generation script. No `int(sys_id)` coercion outside the helper module per TestNoIntCoercion repo-grep lint.)

### Human Verification Required

See `human_verification` frontmatter section above. Summary:

1. **Sidecar regeneration + browse smoke** — Run `scripts/export_fist_enrichment.py` against real `fist_data/FIST.db` to refresh `fist_data/fjms_enrichment.db` with 5,035 synthetic AlmaIds, then open `/browse?sys_id=990000002099000000` and verify FJMS catalogue/bibliography/measurements panels populate, NLI elements hidden, no errors.
2. **Search → browse end-to-end** — Search 'T-S NS 329.96' (or any FJMS-only manifest shelfmark) in Shelfmark mode; verify result row → browse page navigates correctly.
3. **Lists round-trip** — Add synthetic to a saved list, reload; verify shelfmark displays + remove succeeds.
4. **Edit/correction button** — Web + desktop: confirm button hidden; desktop Ctrl+Shift+S shortcut shows QMessageBox without crashing.
5. **CUDL coverage audit** — Run `scripts/scan_cudl_orphans.py` post-Phase-85 (Phase 86 input).
6. **Desktop installer build + smoke** — Per CLAUDE.md "Both apps must be maintained"; v7.11 desktop should follow web release.

### Gaps Summary

The phase achieves its ARCHITECTURAL goal completely: helper module + generation pipeline + libraries.csv synthetic block + manifest + audit residue + browse hide-NLI gates + public API is_synthetic + corrections-write rejection are all wired, tested, and committed. 211 phase-85 tests pass.

The phase has **one runtime gap** that requires a manual operational step before the synthetic mechanism delivers user-visible value: `fist_data/fjms_enrichment.db` must be regenerated using the modified `scripts/export_fist_enrichment.py` against real FIST.db (in main checkout). Until that regeneration happens, browsing a synthetic sys_id will silently render empty FJMS panels — Plan 04's NLI hide-gates work, but the FJMS data they're meant to "frame" doesn't load.

The phase has **one documented narrowing** that requires user decision: SYNTH-03 was implemented for Title+Shelfmark search modes only, narrower than the ROADMAP's "all standard search modes" wording. Both reviewers flagged this as honest scope; `reports/synthetic_coverage.md` recommends a REQUIREMENTS amendment. User should either accept the narrowing (likely path) or schedule Tantivy-stub-rows infrastructure work.

Plan 05 SUMMARY's Phase 86 Readiness Confirmation overstates the state: claim "Synthetic rows present in fjms_enrichment.db" is true at the script level but false at the data-file level. Phase 86 cannot proceed to AUDIT-01 (`scripts/scan_cudl_orphans.py` re-run) until the sidecar reflects the new data — and even then, AUDIT-01 reads `libraries.csv` directly (not the sidecar), so the orphan-classmark count comparison should still work. But any AUDIT-02 collection-breakdown that surveys live FJMS resolution will need the regenerated sidecar.

**Recommended next steps for the user:**

1. Run `scripts/export_fist_enrichment.py` on the main checkout (worktree lacks FIST.db) to regenerate the sidecar (~30s expected, plus full export ~minutes).
2. Verify the post-export sidecar contains 5,035 synthetic AlmaIds in catalog table matching manifest.
3. Smoke-browse `/browse?sys_id=990000002099000000` to confirm FJMS dialogs populate.
4. Decide on SYNTH-03 narrowing: amend REQUIREMENTS to "Title+Shelfmark only" OR schedule Tantivy-stub-rows phase.
5. Proceed to Phase 86 (CUDL Coverage Audit).
6. v7.11 release: web deploy + desktop installer rebuild per CLAUDE.md.

---

*Verified: 2026-05-08T11:34:44Z*
*Verifier: Claude (gsd-verifier)*
