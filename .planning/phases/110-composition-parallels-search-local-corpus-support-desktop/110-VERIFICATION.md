---
phase: 110-composition-parallels-search-local-corpus-support-desktop
verified: 2026-06-09T00:00:00Z
status: passed
score: 6/6 must-haves verified
overrides_applied: 0
re_verification:
  previous_status: none
  note: "Initial verification (no prior VERIFICATION.md)"
---

# Phase 110: Composition / Parallels Search — LOCAL Corpus Support Verification Report

**Phase Goal:** Wire the LOCAL ("My Library") corpus into composition / parallels search — a pre-search Genizah/Local/ALL selector on the composition tab (corpus orthogonal to mode; Lab Mode NOT hardwired to LOCAL), composition executes against the selected corpus (Local = LOCAL only, ALL = Genizah+LOCAL merged, Genizah unchanged), and `export_comp_report` is LOCAL-aware (EXP-F3). Desktop-only. Genizah default path is a strict non-regression.
**Verified:** 2026-06-09
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

Verified against the **AS-BUILT** design (CONTEXT "⚠ DESIGN CORRECTION 2026-06-08" block is authoritative): standard (Lab-off) composition with scope=Local/ALL queries the **regular My-Library index** (`SearchEngine.local_searcher`/`local_index`), NOT the LAB side-index; Lab Mode keeps the LAB/fingerprint index. The default path has no weights-hash/staleness concept (empty LOCAL = "no results", like Genizah). The original D-08 staleness banner / `_lab_weights_hash_override` / `lbl_comp_local_stale` were intentionally removed; staleness is Lab-Mode-only.

### Observable Truths

| #   | Truth | Status | Evidence |
| --- | ----- | ------ | -------- |
| 1   | Pre-search Genizah/Local/ALL selector on the composition tab; Local=LOCAL-only, ALL=merged, Genizah=unchanged (COMP-LOC-01/02) | ✓ VERIFIED | `comp_corpus_scope_combo` built bilingual at `genizah_app.py:6725-6742`; `search_composition_logic(corpus_scope=...)` gates the Genizah loop on `!= 'local'` (`genizah_core.py:9046`) and the regular-LOCAL hook on `!= 'genizah'` (`:9183`); `'all'` merges into the same `doc_hits` accumulator (NOT RRF, per RF-2 decision) |
| 2   | Corpus orthogonal to mode; Lab Mode NOT hardwired to LOCAL (COMP-LOC-01) | ✓ VERIFIED | `run_composition` reads `_comp_scope` once (`genizah_app.py:22164-22167`) and passes `corpus_scope=_comp_scope` to BOTH `LabCompositionThread` (`:22199`) and `CompositionThread` (`:22225`); `lab_composition_search` gates its LAB loops on the same scope (`genizah_core.py:1501,1505,1656`); fail-closed normalizer on both paths (`:1428-1429`, `:8979-8980`) |
| 3   | Standard Local/ALL queries the REGULAR My-Library index (design correction); empty LOCAL = "no results", no default-path staleness banner | ✓ VERIFIED | Standard hook parses `self.local_index` fields `content/content_head/content_tail` with the v7.16 metacharacter-strip fallback (`genizah_core.py:9182-9221`); the removed staleness machinery (`lbl_comp_local_stale`, `_refresh_lab_weights_hash_override`, `_lab_weights_hash_override`, `_refresh_comp_stale_label_for_scope`) is **absent** from both `genizah_app.py` and `desktop/my_library_tab.py` (grep: no matches) |
| 4   | Lab path still uses the LAB side-index AND reports per-run staleness (Lab-Mode-only; SC#4 satisfied at the Lab layer) | ✓ VERIFIED | `lab_composition_search` sets `local_lab_stale` only when a LAB index is present-but-not-fresh (`stale != no-index`, M2) at `genizah_core.py:1647-1654`; LAB loop gated on `corpus_scope != 'genizah'` + freshness (`:1656`); every return path carries `corpus_scope` + `local_lab_stale` (`:1434`, `:1929`, `:8994`, `:9445`) |
| 5   | A Local/ALL composition export emits LOCAL hits with local-meaningful columns across all 4 formats (EXP-F3); Genizah-only export structurally unchanged | ✓ VERIFIED (human + tests) | `export_comp_report` (`genizah_app.py:20501`) uses module-level `_partition_comp_export_rows` / `_build_local_comp_row` / `filter_genizah_ids_for_metadata` (`shared/export_dossier.py:1281-1324`); `_has_local`/`_local_only_comp_export` over ALL comp items (`:20603-20607`); LOCAL-only drops empty Genizah sheets + uses Documents terminology + suppresses MiDRASH credits (`:20622`, FIX 2/3); xlsx/csv/txt/docx Main+Appendix coverage; `tests/test_comp_export_local.py` (4 real passes) |
| 6   | D-12: LOCAL `97…` ids NEVER reach NLI/FJMS network paths (export prefetch AND display grouping) | ✓ VERIFIED | Export prefetch: `genizah_ids = filter_genizah_ids_for_metadata(unique_ids, is_local_sys_id)` before `_fetch_metadata_with_dialog` (`genizah_app.py:20557-20561`); display grouping BLOCKER fix: `group_composition_results` filters LOCAL ids before `batch_fetch_shelfmarks` (`genizah_core.py:9578-9582`); plus `_collect_comp_domain_data` (`:16720`), enrichment workers (`:17874`), `start_metadata_loading` defensive strip (`:18657`), `_collect_id` (`:22822`); pinned by `test_local_comp_grouping_no_nli_fetch` + `test_local_only_export_no_metadata_fetch` |

**Score:** 6/6 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
| -------- | -------- | ------ | ------- |
| `genizah_core.py` | corpus_scope on both composition engines + fail-closed + gating + per-run stale verdict + group filter | ✓ VERIFIED | `search_composition_logic(corpus_scope)` `:8957`; `lab_composition_search(corpus_scope)` `:1406`; `group_composition_results` LOCAL filter `:9578` |
| `gui_threads.py` | corpus_scope plumbed through both composition threads | ✓ VERIFIED | `CompositionThread`/`LabCompositionThread` forward `corpus_scope=self.corpus_scope` (`:112,:188,:215,:246,:282`) |
| `genizah_app.py` | selector + handler + 3-path persistence + run_composition wiring + LOCAL display helpers + LOCAL-aware export | ✓ VERIFIED | selector `:6725`; handler `:16883`; persistence full `:25833` / prefs `:25530` / history `:25336`; display helpers `_comp_item_is_local` `:21787` / `_comp_local_display_fields` `:21813` / `_prime_comp_local_filepath_cache` `:21845`; export `:20501` |
| `shared/export_dossier.py` | module-level LOCAL comp export helpers | ✓ VERIFIED | `_build_local_comp_row` `:1281`, `_partition_comp_export_rows` `:1303`, `filter_genizah_ids_for_metadata` `:1324` |
| `genizah_translations.py` | EN+HE keys for new export terminology (D-11) | ✓ VERIFIED | `Documents Found`/`Main Documents`/`Excluded Documents`/`EXCLUDED DOCUMENTS`/`Matched Text` `:265-270`; (unused-but-harmless `LOCAL index is outdated…` key `:4060` from Plan 01) |
| `tests/test_comp_corpus_scope.py` | engine routing + D-12 + non-regression + fail-closed | ✓ VERIFIED | collected + passing (12 tests incl. `test_genizah_default_nonregression`, `test_local_comp_grouping_no_nli_fetch`) |
| `tests/test_comp_export_local.py` | EXP-F3 export shape + LOCAL-only no-metadata-fetch | ✓ VERIFIED | 8 tests, all REAL passes (xfail markers removed per FIX 7), incl. credit/terminology bug tests |

### Key Link Verification

| From | To | Via | Status | Details |
| ---- | --- | --- | ------ | ------- |
| `comp_corpus_scope_combo.currentData()` | both composition threads | `run_composition` `corpus_scope=` | ✓ WIRED | `genizah_app.py:22164→22199,22225` |
| `CompositionThread.run` | `search_composition_logic` | `corpus_scope=self.corpus_scope` | ✓ WIRED | `gui_threads.py:112` |
| `LabCompositionThread.run` | `lab_composition_search` | `corpus_scope=self.corpus_scope` | ✓ WIRED | `gui_threads.py:215,282` |
| `export_comp_report` | `_partition_comp_export_rows` / `_build_local_comp_row` / `build_local_document_row` | module-level partition + Phase 103 row builder | ✓ WIRED | `genizah_app.py:20553-20557`, `:2846-2848` |
| `export_comp_report` (prefetch) | `is_local_sys_id` | `filter_genizah_ids_for_metadata` before `_fetch_metadata_with_dialog` | ✓ WIRED | `genizah_app.py:20557-20561` |
| `group_composition_results` | `is_local_sys_id` | filter before `batch_fetch_shelfmarks` | ✓ WIRED | `genizah_core.py:9578-9582` |
| `export_comp_report` (filepath) | `my_library_tab._indexer.get_filepaths` | batched LOCAL filepath prime | ✓ WIRED | `genizah_app.py:20571-20576` |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
| -------- | ------------- | ------ | ------------------ | ------ |
| Standard LOCAL comp hook | `doc_hits[uid]` | `self.local_searcher.search()` over the regular My-Library Tantivy index | ✓ (real Tantivy query on `content`/`full_header`/`source`/`shelfmark`) | ✓ FLOWING |
| LOCAL comp display | `shelf, library_display` | `_lookup_local_filepath(sys_id)` → primed `_local_filepath_cache` (LOCAL indexer `get_filepaths`) | ✓ (real filepath → basename/parentfolder) | ✓ FLOWING |
| LOCAL export rows | 5-col LOCAL row | `_local_row_for_page` from primed cache + page-level `p_num`/`chunk_locator` + `source_ctx` | ✓ (no hardcoded empties) | ✓ FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| -------- | ------- | ------ | ------ |
| Requirement tests pass | `pytest tests/test_comp_corpus_scope.py tests/test_comp_export_local.py -q` | `20 passed` | ✓ PASS |
| Engine + export regression intact | `pytest tests/test_lab_composition_chunk_hits.py tests/test_corpus_scope_routing.py tests/test_export_dossier*.py tests/test_local_export_non_regression.py tests/test_export_xlsx_cross_parity.py -q` | `128 passed` | ✓ PASS |
| Modules import cleanly | `python -c "import genizah_app, genizah_core, gui_threads, shared.export_dossier"` | `IMPORTS_OK` (exit 0) | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
| ----------- | ----------- | ----------- | ------ | -------- |
| COMP-LOC-01 | 110-01/02/03 | Pre-search Genizah/Local/ALL selector on comp tab; orthogonal to mode; Lab NOT hardwired to LOCAL | ✓ SATISFIED | Truths 1, 2; selector + dual-thread scope plumbing + Lab decoupling |
| COMP-LOC-02 | 110-01/02/03 | Composition executes against selected corpus (Local-only / ALL-merged / Genizah-unchanged); stale LAB surfaces signal (Lab-Mode-only per correction) | ✓ SATISFIED | Truths 1, 3, 4; regular-index hook + merged accumulator + Lab per-run staleness verdict |
| EXP-F3 | 110-01/04 | LOCAL-aware `export_comp_report` (filename/folder/filepath/page/matched-text) all 4 formats; Genizah-only unchanged | ✓ SATISFIED | Truths 5, 6; module-level helpers + 4-format emission + D-12 prefetch filter + structural Genizah parity |

No orphaned requirements: REQUIREMENTS.md maps exactly COMP-LOC-01/02 + EXP-F3 to Phase 110, and all three appear in PLAN frontmatter and are accounted for.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
| ---- | ---- | ------- | -------- | ------ |
| `genizah_translations.py` | 4060 | Unused `LOCAL index is outdated…` translation key (Plan 01 staleness banner, removed by design correction) | ℹ️ Info | Harmless dead translation key; the staleness UI it served was intentionally removed. Not a stub. |

No blocker or warning anti-patterns. The CODEX code review's 1 BLOCKER + 2 HIGH + 3 MED + 1 LOW findings were all addressed (verified in code): BLOCKER `group_composition_results` LOCAL filter (`genizah_core.py:9578`), HIGH credit suppression (`:20622`), HIGH LOCAL-only sheet/terminology (FIX 3), MED excluded-row partition, MED display-time prime (`:22361`), MED `_has_local` item-predicate (`:20604`), LOW xfail markers removed (8 real passes).

### Human Verification Required

None outstanding. The Plan 110-03 human-verify checkpoint (selector + routing + Lab decoupling + persistence + history + parallels-from-browse + LOCAL hit display rendering shelfmark=filename / Library=parent-folder) was **APPROVED live by the user (2026-06-09)** per 110-03-SUMMARY.md. UI behaviors are confirmed; marked verified-by-human (Truth 5).

### Gaps Summary

No gaps. All six observable truths verified against the as-built (design-corrected) behavior. The phase goal is genuinely achieved:
- The composition tab has a working pre-search Genizah/Local/ALL selector, orthogonal to Lab Mode (Lab no longer hardwired to LOCAL).
- Standard Local/ALL composition queries the regular My-Library index (the UAT root-cause correction); ALL merges Genizah+LOCAL into one accumulator; Genizah default is a strict non-regression (fail-closed normalizer + 128-test regression green).
- `export_comp_report` is LOCAL-aware across all four formats, reusing the Phase 103 helpers, with LOCAL-only credit/terminology/sheet corrections and Genizah-only structural parity.
- The D-12 privacy invariant holds on BOTH the export prefetch and the pre-display grouping path — private LOCAL `97…` ids never reach NLI/FJMS network calls.

SC#4's default-path staleness banner was intentionally removed per the authoritative CONTEXT design correction (regular index has no staleness concept; empty LOCAL = "no results"); the Lab-Mode staleness verdict is preserved and carried per-run, satisfying the spirit of SC#4 at the layer where a LAB index actually exists.

---

_Verified: 2026-06-09_
_Verifier: Claude (gsd-verifier)_
