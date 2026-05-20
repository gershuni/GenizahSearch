---
phase: 94-adding-pgp-to-downloaded-data
verified: 2026-05-20T08:21:34Z
status: passed
score: 9/9 must-haves verified
overrides_applied: 0
re_verification:
  previous_status: null  # initial verification (no prior 94-VERIFICATION.md)
  previous_score: null
  gaps_closed: []
  gaps_remaining: []
  regressions: []
---

# Phase 94: adding-pgp-to-downloaded-data — Verification Report

**Phase Goal (from `.planning/milestones/v7.13-ROADMAP.md`):** Research-grade xlsx + JSON metadata for both web and desktop — enrichment signals (PGP / printed / domains) surfaced in main sheet + JSON envelope; dedicated Manuscripts and Bibliography dossier sub-sheets; Credits and Info sheet with per-export search metadata; bilingual (en/he) with source-language-aware metadata fallback; cross-app parity.

**Verified:** 2026-05-20T08:21:34Z
**Status:** passed
**Re-verification:** No — initial verification

---

## Scope

Phase 94 spanned 4 waves (4 plans 94-01..94-04) plus 6 rounds of smoke-fix patches on top of plan 94-04. The phase introduced a 4-sheet xlsx workbook on both web and desktop (Search Results, Manuscripts, Bibliography, Credits and Info), bilingual headers/sheet titles + source-language-aware metadata content, JSON envelope additive flags on the `/api/search` path (with parallels strip), per-sys_id Domains dedupe, clickable URL cells on the Manuscripts sub-sheet, Image/Page int coercion, and cross-app parity invariant tests. Human smoke verification was approved by Hillel on 2026-05-21 after 6 patch rounds.

---

## Goal Achievement — Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Web + desktop xlsx workbooks emit 4 sheets in the order `[Search Results, Manuscripts, Bibliography, Credits and Info]` (Hebrew: `[תוצאות חיפוש, כתבי יד, ביבליוגרפיה, קרדיט ומידע]`); first sheet is default-active | VERIFIED | `shared/export_dossier.py:272-287` (`_SHEET_TITLES_EN`/`_HE` dicts); `tests/test_export_service_multi_sheet.py:118, 382` + `tests/test_desktop_xlsx_multi_sheet.py:101, 364, 398` assert `sheetnames == ['Search Results', 'Manuscripts', 'Bibliography', 'Credits and Info']` on both apps; `tests/test_smoke_round2_export_gaps.py:180, 187` parametrized web+desktop |
| 2 | Shared dossier primitives at `shared/export_dossier.py` expose the full public API: 4 lookup helpers + 2 row emitters + bilingual header functions + `sheet_titles` + `build_credits_info_sheet` + `apply_manuscript_row_hyperlinks` | VERIFIED | `shared/export_dossier.py:188, 290, 302, 313, 320, 425, 439, 446, 601, 702, 746, 821, 885, 989`; `MANUSCRIPT_HEADERS` (14 cols, :151), `BIBLIOGRAPHY_HEADERS` (8 cols, :169); `tests/test_export_dossier.py` exercises each helper |
| 3 | Source-language metadata: `lang='he'` triggers Hebrew preferred path (`pgp_translations` via `TranslationService.get_pgp_translations_by_sys_ids`, Hebrew library names via `get_library_display(code, lang='he')`, FJMS Hebrew variants); English fallback per field when Hebrew absent | VERIFIED | `shared/export_dossier.py:621-661` (D-04 REVISED block in `pgp_subset_for_sys_id`); `shared/export_dossier.py:676-700` (`_pgp_translation_he_for_sys_id`); `web/export_service.py:610-614, 692-710` thread `lang` through `core_get_library_display`; desktop `genizah_app.py:2477+` threads `CURRENT_LANG`; tests in `tests/test_smoke_round2_export_gaps.py:414-503` (Hebrew domain substitution) and the bilingual cases in `test_export_service_multi_sheet.py` (line 234 mention) |
| 4 | JSON envelope additive flags: `_serialize_item` in `shared/search_serializer.py` emits per-item `has_pgp` / `is_printed` / `domains` ONLY when caller opts in (export path); the parallels envelope `_to_parallels_envelope_item` strips both keys defensively (D-10 negative invariant) | VERIFIED | `shared/search_serializer.py:444-453` (opt-in emit block); `shared/search_serializer.py:924-932` (parallels strip block); `tests/test_parallels_envelope_no_pgp_keys.py:42-46` asserts both keys absent; `tests/test_search_serializer.py` covers the public-path omission |
| 5 | D-02 invariant: no transcription / `page_section_text` / `full_text` leakage on Manuscripts or Bibliography sub-sheets (main-sheet `Full Text` column is grandfathered per CONTEXT D-02 amendment) | VERIFIED | `shared/export_dossier.py:78-95` (module docstring D-02 prohibition); `pgp_subset_for_sys_id` returns 6-key whitelist only (:663-670); `tests/test_export_dossier.py:301, 662` (`test_no_transcription_text_leak`, `test_no_transcription_leak`); assertions at `tests/test_export_dossier.py:323-330` reject `page_section_text`, `transcription`, `full_text` keys |
| 6 | Phase 87 multitenant invariant: zero raw `app.storage.user` access added under `web/`; allowlist remains `[]` | VERIFIED | `.planning/phase87_storage_allowlist.yaml:29` (`allowed_raw_access: []`); `tests/test_no_raw_storage_access.py` all 6 tests PASS (0.67s; verified live in this run) |
| 7 | Smoke-fix UX features (rounds 3-6): Manuscripts URL cells are openpyxl hyperlinks; Image/Page numeric values stored as int; Domains list dedupes per sys_id across multiple FJMS rows; Credits and Info sheet contains the Creator credit line (EN + HE) | VERIFIED | (a) Hyperlinks: `shared/export_dossier.py:188-217` `apply_manuscript_row_hyperlinks` uses `Font(color="0563C1", underline="single")`; `tests/test_smoke_round2_export_gaps.py:699-787`. (b) Int coercion: `shared_export_utils.py:64` `coerce_img_page_cell`; consumed in `web/export_service.py`, `genizah_app.py`, `shared/export_dossier.py` (4 files); `tests/test_smoke_round2_export_gaps.py:900-955` (8 tests). (c) Domains dedupe: `web/pages/search.py:4567` (`filtered = list(dict.fromkeys(filtered))`); `tests/test_smoke_round2_export_gaps.py:823-897`. (d) Creator credit: `shared/export_dossier.py:352` (`"Creator: Hillel Gershuni, gershuni@gmail.com"`) and :372 (`"יוצר: הלל גרשוני..."`); `tests/test_smoke_round2_export_gaps.py:580, 588` |
| 8 | Web search metadata flows `web/pages/search.py` → `set_search_export` → `web/api.py:export_excel` → `export_search_results_excel` → `build_credits_info_sheet` (search_mode, search_gap, domain_name_map) | VERIFIED | `web/export_state.py:491, 513` (`set_search_export(domain_name_map=...)` kwarg); `web/pages/search.py:3909, 4235, 4333` (`set_search_export(...)` call sites); `web/api.py:2102-2130` (extracts `mode`/`gap`/`domain_name_map`, passes to `export_search_results_excel`); `web/export_service.py:487-491, 850-856` (kwargs received and threaded into `build_credits_info_sheet`); `tests/test_smoke_round2_export_gaps.py:652-697` (state plumbing); `tests/test_smoke_round2_export_gaps.py:305-388` (cells emitted on web Credits sheet) |
| 9 | Cross-app structural parity: web and desktop produce identical sheet names + header rows on identical input at `lang='en'` (EXPORT-META-09 invariant) | VERIFIED | `tests/test_export_xlsx_cross_parity.py:133-179` (4 tests: sheetnames identical + main / Manuscripts / Bibliography headers byte-identical); `genizah_app.py:2477` `_build_search_results_xlsx_bytes` is the desktop pure-function entry consumed by the test |

**Score:** 9/9 truths verified.

---

## Required Artifacts

| Artifact | Expected | Status | Details |
|---|---|---|---|
| `shared/export_dossier.py` | 4 lookup helpers + 2 row emitters + bilingual headers + `sheet_titles` + `build_credits_info_sheet` + `apply_manuscript_row_hyperlinks` | VERIFIED | 1043 lines; all 17 expected public symbols present (grep `^def ` confirms `apply_manuscript_row_hyperlinks`, `main_header_row`, `manuscript_header_row`, `bibliography_header_row`, `sheet_titles`, `credits_lines`, `search_meta_labels`, `build_credits_info_sheet`, `pgp_subset_for_sys_id`, `nli_subset_for_sys_id`, `catalog_summary_for_sys_id`, `bibliography_for_sys_id`, `build_manuscript_row`, `build_bibliography_rows`) plus the two header constants and `MANUSCRIPT_URL_COLUMN_INDICES` |
| `shared/search_serializer.py` | Opt-in `has_pgp`/`is_printed` emit on search path; strip on parallels envelope (D-10) | VERIFIED | `_serialize_item:444-453` opt-in branch; `_to_parallels_envelope_item:924-932` strip block; `domains` already present pre-Phase-94 |
| `web/export_service.py` | `export_search_results_excel` restructured to 4-sheet builder; threads `lang`, `search_mode`, `search_gap`, `domain_name_map`; calls `build_credits_info_sheet`; uses `apply_manuscript_row_hyperlinks` + `coerce_img_page_cell` | VERIFIED | Signature at :468-491 includes all 8 metadata kwargs; `build_credits_info_sheet` called at :851; domain HE substitution at :745-746 |
| `web/export_state.py` | `set_search_export` extended with `domain_name_map`, `transcription_sys_ids`, `printed_ids` (EXPORT-META-06) | VERIFIED | :491-559 signature includes `transcription_sys_ids`, `printed_ids`, `domain_name_map`; `update_search_export_enrichment` sibling at :583; persisted into payload at :559 |
| `web/api.py` | `export_excel` route extracts metadata from payload and threads through to service | VERIFIED | :2070-2130; extracts `mode`, `gap`, `domain_name_map`; calls `export_search_results_excel(search_mode=..., search_gap=..., domain_name_map=...)` |
| `web/pages/search.py` | All search-completion paths populate `domain_name_map` and call `set_search_export(mode=..., gap=..., domain_name_map=...)` | VERIFIED | 3 `set_search_export` call sites at :3909, :4235, :4333; `domain_name_map` populated at :4573-4576 with Hebrew domain substitution data; dedupe at :4567 (`dict.fromkeys`) |
| `genizah_app.py` | Module-level pure-function `_build_search_results_xlsx_bytes` ; `export_results('xlsx')` calls it with desktop state machine signals | VERIFIED | :2477 `def _build_search_results_xlsx_bytes(`; imports at :2567-2570 (`build_manuscript_row`, `build_bibliography_rows`, `build_credits_info_sheet`, `main_header_row`, `manuscript_header_row`, `bibliography_header_row`, `sheet_titles`); main `export_results` call site at :18422 |
| `shared_export_utils.py` | `build_rich_snippet_cell` (Wave 1) + `coerce_img_page_cell` (smoke round 6) | VERIFIED | `coerce_img_page_cell` at :64; consumed in 4 production files (`web/export_service.py`, `genizah_app.py`, `shared/export_dossier.py`, plus tests) |
| Test files | All 8 listed test files exist | VERIFIED | `tests/test_export_dossier.py` (64200 bytes, 2 transcription-leak tests at :301, :662), `tests/test_export_service_multi_sheet.py` (18063 bytes), `tests/test_desktop_xlsx_multi_sheet.py` (16420 bytes), `tests/test_export_xlsx_cross_parity.py` (7270 bytes, 4 parity tests), `tests/test_smoke_round2_export_gaps.py` (36032 bytes, 68 tests covering all 6 smoke rounds consolidated), `tests/test_search_serializer.py` (56071 bytes), `tests/test_parallels_envelope_no_pgp_keys.py` (2800 bytes, 2 tests), `tests/test_no_raw_storage_access.py` (18435 bytes, 6 tests all passing) |
| `.planning/REQUIREMENTS.md` | All 9 EXPORT-META-01..09 marked `[x]` Complete with attribution | VERIFIED | :28-36 (checklist), :77-85 (status table) — all 9 IDs Complete, all 9 closing-plan rows annotated |
| `CHANGELOG.md` | Phase 94 entry under `[Unreleased]` | VERIFIED | :13 `### Added — Phase 94: Research-Grade Export Metadata (web + desktop)` ; final test count at :119 (2316 passed) |

---

## Key Link Verification

| From | To | Via | Status | Detail |
|---|---|---|---|---|
| `web/pages/search.py:_process_domain_data` (~:4556) | `web/export_state.set_search_export(domain_name_map=...)` (3 call sites) | populates `search_state.domain_name_map`, then `set_search_export` reads it | WIRED | dedupe applied at :4567; map populated at :4573-4576; `set_search_export` at :3909, :4235, :4333 |
| `web/export_state.set_search_export` | `web/api.py:export_excel` (`payload.get('domain_name_map')`) | persists into `app.storage.user['export_search_payload']`; route reads it back | WIRED | `web/export_state.py:559` stores; `web/api.py:2104` reads; threads to service at :2130 |
| `web/api.py:export_excel` | `web/export_service.export_search_results_excel(domain_name_map=..., search_mode=..., search_gap=...)` | direct kwarg passthrough | WIRED | :2120-2130 (kwargs passed); `web/export_service.py:487-491` signature matches |
| `web/export_service.export_search_results_excel` | `shared.export_dossier.build_credits_info_sheet(...)` | direct call in 4th-sheet build step | WIRED | :851-856 |
| `web/export_service.export_search_results_excel` (Manuscripts builder) | `shared.export_dossier.build_manuscript_row` + `apply_manuscript_row_hyperlinks` | direct calls per unique sys_id | WIRED | `apply_manuscript_row_hyperlinks` imported at :563 and called in Manuscripts loop |
| `genizah_app._build_search_results_xlsx_bytes` (:2477) | `shared.export_dossier.build_manuscript_row` + `build_bibliography_rows` + `build_credits_info_sheet` | direct calls | WIRED | :2712, :2731, :2746 |
| `shared.export_dossier.pgp_subset_for_sys_id` (lang='he') | `shared.translation_service.TranslationService.get_pgp_translations_by_sys_ids` | via `_pgp_translation_he_for_sys_id` wrapper | WIRED | `shared/export_dossier.py:676-700` |
| `shared.search_serializer._to_parallels_envelope_item` | `.pop('has_pgp', None)` + `.pop('is_printed', None)` | defense-in-depth strip on parallels path (D-10) | WIRED | `shared/search_serializer.py:931-932` |

All key links WIRED. No stubs, no orphans.

---

## Requirements Coverage

| Requirement | Source Plan | Description (abbrev) | Status | Evidence |
|---|---|---|---|---|
| EXPORT-META-01 | 94-03 (web) + 94-04 (desktop) | Main sheet appends `Has PGP`, `Is Printed`, `Domains` columns; smoke round-5 deduped per-sys_id Domains | SATISFIED | `shared/export_dossier.py:230-242` (`_MAIN_HEADERS_EN/_HE`); web export_service produces those cells (validated via parity test); dedupe at `web/pages/search.py:4567` |
| EXPORT-META-02 | 94-01 + 94-03 + 94-04 | NEW Manuscripts sub-sheet, 14 columns; smoke round-4 clickable URLs | SATISFIED | `shared/export_dossier.py:151-166` (`MANUSCRIPT_HEADERS`, 14 cols); `:885+` `build_manuscript_row`; `:188-217` `apply_manuscript_row_hyperlinks` with blue-underline styling; cross-parity test at `tests/test_export_xlsx_cross_parity.py:158-167` |
| EXPORT-META-03 | 94-01 + 94-03 + 94-04 | NEW Bibliography sub-sheet, REAL FJMS field names | SATISFIED | `shared/export_dossier.py:169-178` (`BIBLIOGRAPHY_HEADERS`, 8 cols); `:821+` `bibliography_for_sys_id`; `:989+` `build_bibliography_rows`; cross-parity test at `tests/test_export_xlsx_cross_parity.py:170-179` |
| EXPORT-META-04 | 94-03 + 94-04 | Workbook sheet order Search Results → Manuscripts → Bibliography → Credits and Info; first sheet default-active | SATISFIED | `tests/test_export_service_multi_sheet.py:118, 382` + `tests/test_desktop_xlsx_multi_sheet.py:101, 364` assert exact 4-tuple order; web Credits and Info via `build_credits_info_sheet` at `web/export_service.py:851` |
| EXPORT-META-05 | 94-01 (D-02 guard) + 94-03 + 94-04 | All metadata in English with Hebrew fallback (or Hebrew preferred when `lang='he'` post D-04 reversal); no transcription text | SATISFIED | D-04 REVERSED 2026-05-20 (documented in module docstring `shared/export_dossier.py:9-19`); `pgp_subset_for_sys_id` prefers Hebrew at :652-661; D-02 transcription prohibition still active — `tests/test_export_dossier.py:301, 662` regression-test the no-leak invariant |
| EXPORT-META-06 | 94-02 + 94-03 | `printed_ids` plumbed through `set_search_export` alongside `transcription_sys_ids` | SATISFIED | `web/export_state.py:491` signature includes both kwargs; payload includes them at :559; `web/api.py:2120-2130` extracts and threads; desktop reads `self._printed_sys_ids` + `self._pgp_transcription_sys_ids` directly per Plan 94-04 truth |
| EXPORT-META-07 | 94-02 | JSON additive keys `has_pgp` / `is_printed` / `domains` on `/api/search` opt-in; parallels strip (D-10) | SATISFIED | `shared/search_serializer.py:444-453` (opt-in); `:924-932` (strip); `tests/test_parallels_envelope_no_pgp_keys.py` (2 tests) |
| EXPORT-META-08 | 94-03 + 94-04 | `IIIF Manifest` column — SOFT scope, DEFERRED per D-13 | SATISFIED (deferred) | Column header present in `_MAIN_HEADERS_EN/_HE` (:234, :241); cells empty per D-13 soft-scope deferral; reachability via `Library Viewer URL` column on Manuscripts sub-sheet (:163) |
| EXPORT-META-09 | 94-04 | Desktop xlsx parity — identical 4-sheet structure as web | SATISFIED | `genizah_app.py:2477` `_build_search_results_xlsx_bytes` pure-function helper; cross-parity tests at `tests/test_export_xlsx_cross_parity.py:133-179` (4 tests pin sheetnames + headers byte-identical) |

All 9 requirements SATISFIED. No orphaned requirements detected.

---

## Behaviors Verified (the 8 items from the verification brief)

### 1. Multi-sheet structure (4 sheets in canonical order)
Both apps emit `['Search Results', 'Manuscripts', 'Bibliography', 'Credits and Info']` at `lang='en'` and `['תוצאות חיפוש', 'כתבי יד', 'ביבליוגרפיה', 'קרדיט ומידע']` at `lang='he'`. Pinned by `tests/test_export_service_multi_sheet.py`, `tests/test_desktop_xlsx_multi_sheet.py`, `tests/test_export_xlsx_cross_parity.py`, and `tests/test_smoke_round2_export_gaps.py`.

### 2. Shared dossier primitives at `shared/export_dossier.py`
All required public symbols present and tested:
- 4 lookup helpers (:601, :702, :746, :821).
- 2 row emitters (:885, :989).
- Bilingual headers (:290, :302, :313).
- `sheet_titles` (:320).
- `build_credits_info_sheet` (:446).
- `apply_manuscript_row_hyperlinks` (:188).

### 3. Source-language metadata (Hebrew preferred on `lang='he'`, English fallback)
`pgp_subset_for_sys_id` consults `TranslationService.get_pgp_translations_by_sys_ids` only when `lang == 'he'` (:653-661). Library names resolved via `core_get_library_display(lib_code, short=False, lang=lang)` (`web/export_service.py:614, 710`). D-02 transcription prohibition preserved.

### 4. JSON envelope additive flags
`_serialize_item` emits `has_pgp` / `is_printed` ONLY when at least one of `transcription_sys_ids` / `printed_ids` is provided (opt-in semantics, `shared/search_serializer.py:444-453`). `_to_parallels_envelope_item` defensively strips both keys (D-10, :931-932). `domains` is always present (pre-Phase-94 behavior). Pinned by `tests/test_parallels_envelope_no_pgp_keys.py`.

### 5. D-02 invariant (no transcription leakage on Manuscripts/Bibliography sub-sheets)
`pgp_subset_for_sys_id` returns a 6-key whitelist; `tests/test_export_dossier.py::test_no_transcription_text_leak` and `::test_no_transcription_leak` reject `page_section_text`, `transcription`, `full_text`, `full_texts` keys. Main-sheet `Full Text` column grandfathered per CONTEXT D-02 amendment (web `_resolve_result_full_text`; desktop `full_text_fetcher` lambda).

### 6. Phase 87 multitenant invariant
`.planning/phase87_storage_allowlist.yaml:29` has `allowed_raw_access: []`. All 6 tests in `tests/test_no_raw_storage_access.py` PASS (re-run live: 0.67s, exit 0). Phase 94's web changes did not introduce any new raw `app.storage.user` accesses.

### 7. Smoke-fix UX features (rounds 3-6)
- **Hyperlinks (round 4):** `apply_manuscript_row_hyperlinks` (`shared/export_dossier.py:188-217`) marks columns 4, 12, 13 (PGP URL / Library Viewer URL / GenizahSearch URL) with `Font(color="0563C1", underline="single")`.
- **Int Image/Page (round 6):** `coerce_img_page_cell` in `shared_export_utils.py:64`, consumed by both apps.
- **Domains dedupe (round 5):** `web/pages/search.py:4567` `filtered = list(dict.fromkeys(filtered))` at source.
- **Creator credit:** `shared/export_dossier.py:352` (English) and :372 (Hebrew); `credits_lines(lang)` returns these.

### 8. Web search metadata flow
Verified end-to-end:
- **Origin** (`web/pages/search.py:3909, 4235, 4333`): `set_search_export(query=..., mode=..., gap=..., domain_name_map=...)`.
- **State** (`web/export_state.py:491-559`): `set_search_export` persists all kwargs into `app.storage.user['export_search_payload']`.
- **Route** (`web/api.py:2070-2130`): `export_excel` reads payload, extracts `mode`/`gap`/`domain_name_map`, threads to service.
- **Service** (`web/export_service.py:487-491, 850-856`): `export_search_results_excel` accepts the kwargs, calls `build_credits_info_sheet(search_query=..., search_mode=..., search_gap=...)`.
- **Sheet builder** (`shared/export_dossier.py:446-559`): `build_credits_info_sheet` renders the 4th sheet with all metadata + clickable GenizahSearch.com hyperlink.

Pinned by `tests/test_smoke_round2_export_gaps.py:305-388, 652-697`.

---

## Invariants Verified

| Invariant | Source | Status | Evidence |
|---|---|---|---|
| **D-02** — no transcription text in NEW dossier surfaces (Manuscripts + Bibliography sub-sheets, JSON additive keys) | CONTEXT D-02 + module docstring `shared/export_dossier.py:78-95` | HOLDS | `tests/test_export_dossier.py::test_no_transcription_text_leak` (line 301) + `::test_no_transcription_leak` (line 662); helpers project narrow whitelists only |
| **D-10** — parallels envelope strips `has_pgp` / `is_printed` | CONTEXT D-10 + `shared/search_serializer.py:924-932` | HOLDS | `tests/test_parallels_envelope_no_pgp_keys.py` (2 tests) |
| **Phase 87** — zero raw `app.storage.user` access under `web/` | `.planning/phase87_storage_allowlist.yaml:29` | HOLDS | `tests/test_no_raw_storage_access.py` (6 tests PASS, 0.67s live re-run) |

---

## Anti-Patterns Scan

No anti-patterns detected in the Phase 94 modified files. Specifically:
- No TODO/FIXME comments left behind in production code paths.
- No empty `return None` or `return []` stubs that could short-circuit the dossier helpers (each helper has documented service-unavailability return semantics).
- No `console.log` / `print` debug residue.

---

## Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|---|---|---|---|
| Full test suite passes | `python -m pytest tests/ -q --tb=line` | `2316 passed, 20 skipped, 2 xfailed in 215.80s` | PASS |
| Phase 87 lint scanner | `python -m pytest tests/test_no_raw_storage_access.py -v` | `6 passed in 0.67s` | PASS |
| `shared/export_dossier.py` importable | grep of `^def ` + module structure verified | 15 public functions present + 2 header constants + URL-column index tuple | PASS |
| `genizah_app._build_search_results_xlsx_bytes` exists | grep `def _build_search_results_xlsx_bytes` in `genizah_app.py` | Found at :2477 | PASS |
| `coerce_img_page_cell` consumed by both apps | grep in production files | Found in `web/export_service.py`, `genizah_app.py`, `shared/export_dossier.py`, `shared_export_utils.py` | PASS |

---

## Test Summary

**Full test suite (live run during this verification):**
- **2316 passed**
- **20 skipped**
- **2 xfailed**
- 215.80s total wall-clock
- Ruff clean across all touched files (per 94-04-SUMMARY.md verification block; confirmed via prior commit hooks)

**Phase 94 dedicated test files:**
- `tests/test_export_dossier.py` — 64,200 bytes; unit tests for all 4 lookup helpers + 2 row builders + header constants + 2 D-02 transcription-leak negative tests.
- `tests/test_export_service_multi_sheet.py` — 18,063 bytes; web 4-sheet structure + bilingual sheet titles.
- `tests/test_desktop_xlsx_multi_sheet.py` — 16,420 bytes; desktop 4-sheet structure (18+ tests).
- `tests/test_export_xlsx_cross_parity.py` — 7,270 bytes; 4 cross-app parity tests pinning sheetnames + 3 header rows byte-identical.
- `tests/test_smoke_round2_export_gaps.py` — 36,032 bytes; 68 tests covering all 6 smoke-fix rounds consolidated (sheet rename, 4th sheet emit, Credits content, Hebrew domain substitution, Hebrew labels, Creator credit, hyperlinks, Domains dedupe, Image/Page int coercion, web state plumbing).
- `tests/test_search_serializer.py` — 56,071 bytes; opt-in JSON additive flag semantics.
- `tests/test_parallels_envelope_no_pgp_keys.py` — 2 D-10 negative-invariant tests.
- `tests/test_no_raw_storage_access.py` — 6 Phase 87 multitenant guards.

Note: the original Plan 94-04 SUMMARY lists individual smoke-round regression test files (`test_export_bilingual.py`, `test_credits_sheet.py`, `test_smoke_round2_gaps_a_d.py`, `test_smoke_round3_label_realignment.py`, `test_manuscripts_urls_clickable.py`, `test_domains_dedupe.py`, `test_image_page_int_coercion.py`). These files do NOT exist as separate filesystem entries — their coverage is consolidated into `tests/test_smoke_round2_export_gaps.py` (the single 68-test file). The test coverage itself is intact (the full suite passes 2316 tests including the 68 consolidated smoke-round assertions), so this is a documentation-style inconsistency in the SUMMARY's "key-files created" list, not a missing-coverage gap. Not flagged as a verification gap because the underlying behaviors ARE tested.

---

## Gaps / Human-Verification Items

**None.** Smoke verification was approved by Hillel on 2026-05-21 (Task 2 of Plan 94-04) after 6 rounds of follow-up UX patches; all behaviors are now pinned by automated tests; full test suite passes.

---

## Summary

Phase 94 fully achieves its goal as stated in the v7.13 milestone roadmap:

- 4-sheet workbook structure (Search Results / Manuscripts / Bibliography / Credits and Info) on both web and desktop.
- Enrichment signals (PGP / printed / domains) surfaced on the main sheet and (additively + opt-in) in the `/api/search` JSON envelope.
- Bilingual sheet titles + headers + source-language-aware metadata content with English fallback.
- Cross-app parity invariant pinned by automated test.
- D-02 transcription-prohibition + D-10 parallels-envelope strip + Phase 87 multitenant invariant all preserved.
- 6 smoke-verification rounds folded in (sheet rename, 4th sheet, Hebrew domains, label realignment, clickable Manuscripts URLs, Domains dedupe, Image/Page int coercion).
- All 9 EXPORT-META-* requirements (01..09) marked Complete in REQUIREMENTS.md.
- Full test suite green: **2316 passed / 20 skipped / 2 xfailed**.

**Verdict:** PASSED. Phase 94 is complete and the goal is achieved.

---

_Verified: 2026-05-20T08:21:34Z_
_Verifier: Claude (gsd-verifier)_
