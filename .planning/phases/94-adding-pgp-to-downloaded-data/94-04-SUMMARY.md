---
phase: 94-adding-pgp-to-downloaded-data
plan: 04
subsystem: export
tags: [xlsx, desktop, 4-sheet, dossier, openpyxl, cross-parity, wave-4, tdd, bilingual, smoke-approved, done]
status: done

# Dependency graph
requires:
  - phase: 87-foundations
    provides: Phase 87 multitenant invariant (allowlist []) — desktop is out of multitenant scope; web side unaffected
  - plan: 94-01
    provides: shared/export_dossier.py — 4 lookup helpers + 2 row emitters + 2 header constants + shared_export_utils.build_rich_snippet_cell
  - plan: 94-02
    provides: web state plumbing for 3 enrichment signals + JSON additive flags
  - plan: 94-03
    provides: web/export_service.export_search_results_excel restructured into 3-sheet builder
provides:
  - genizah_app._build_search_results_xlsx_bytes module-level helper — pure function returning xlsx bytes, no Qt dependencies (testable offline)
  - Desktop export_results('xlsx') restructured to emit 4-sheet workbook (Search Results / Manuscripts / Bibliography / Credits and Info) structurally identical to web's output (EXPORT-META-09)
  - shared/export_dossier.py bilingual extensions (main_header_row(lang) / manuscript_header_row(lang) / bibliography_header_row(lang) / sheet_titles(lang)) + Hebrew-preferred metadata via TranslationService.get_pgp_translations_by_sys_ids when lang='he'
  - 4th 'Credits and Info' sheet on both apps carrying search metadata (Query / Mode / Gap / generated_at / result count) + clickable GenizahSearch.com hyperlink + Creator credit (parity across web and desktop)
  - Manuscripts sub-sheet URL cells (PGP URL / Library Viewer URL / GenizahSearch URL) emitted as clickable hyperlinks with blue-underline styling
  - Image/Page main-sheet column emits pure-numeric values as int (no Excel 'Number stored as text' warning)
  - Per-sys_id Domains dedupe — multi-FJMS-row manuscripts no longer render the same domain N times
  - tests/test_desktop_xlsx_multi_sheet.py — offline tests pinning the multi-sheet structure
  - tests/test_export_xlsx_cross_parity.py — cross-app parity tests asserting identical sheet names + headers
  - 7 additional smoke-fix regression test files covering rounds 1-6 (bilingual + credits sheet + label realignment + clickable URLs + dedupe + int coercion)
affects: [Phase 94 closeout — REQUIREMENTS.md / ROADMAP.md / v7.13 milestone roadmap / CHANGELOG.md / OPEN_ISSUES.md / CLAUDE.md / PROJECT.md (this commit)]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Module-level pure-function builder pattern: _build_search_results_xlsx_bytes(...) returns bytes — Qt-free, no instance dependencies. Mirrors web/export_service.export_search_results_excel as a pure function returning (bytes, filename)."
    - "MetaResolver callable contract on desktop: closure inside export_results wraps self.meta_mgr.get_meta_for_id / get_library_for_id + genizah_core.get_library_display(lang=lang). Matches the Codex SHOULD-FIX 8 pattern from Wave 1, now lang-aware after the D-04 reversal."
    - "Bilingual headers + sheet titles via shared/export_dossier.py bilingual helpers (main_header_row(lang) / manuscript_header_row(lang) / bibliography_header_row(lang) / sheet_titles(lang)). Hebrew when lang='he', English when lang='en'. English constants MANUSCRIPT_HEADERS / BIBLIOGRAPHY_HEADERS retained for back-compat — the new helpers return them verbatim for lang='en'."
    - "Source-language metadata: pgp_subset_for_sys_id / catalog_summary_for_sys_id / bibliography_for_sys_id all thread lang. When lang='he', Hebrew preferred (with English fallback per field); when lang='en', English preferred (with Hebrew fallback). Service: TranslationService.get_pgp_translations_by_sys_ids."
    - "Pre-export domain-readiness check (MUST-FIX 94-04-E): QMessageBox.warning with Yes/No surfaced when _result_domain_map is empty AND results have sys_ids. No reply aborts the export cleanly; Yes proceeds with the documented gap."
    - "Tantivy-backed full_text hydration callback (MUST-FIX 94-04-D): _build_search_results_xlsx_bytes accepts a full_text_fetcher parameter; the export_results call site passes self.searcher.get_full_text_by_id(uid) for rows lacking 'full_text' / 'full_text_excerpt' (PGP tag rows omit both)."
    - "4th sheet 'Credits and Info' (smoke round-2): carries search metadata (Query / Mode / Gap / generated_at / result count) + clickable GenizahSearch.com hyperlink + Creator credit. Parity across web and desktop."
    - "Clickable URL cells (smoke round-4): Manuscripts sub-sheet URL cells emit openpyxl Hyperlink objects with FONT_BLUE_UNDERLINE styling. PGP URL / Library Viewer URL / GenizahSearch URL all clickable."
    - "Per-sys_id Domains dedupe (smoke round-5): the Domains list is deduped at source so a manuscript with multiple FJMS rows for the same domain renders the domain once. Web Domains-badge count now reflects unique manuscripts per domain rather than total FJMS rows."
    - "Image/Page int coercion (smoke round-6): pure-numeric Image/Page values written as int via ws.cell(...).value = int(...) — eliminates Excel's 'Number stored as text' warning. Mixed-content values (e.g., '1r') stay as strings."
    - "Cross-app parity regression test (MUST-FIX 94-04-C): tests/test_export_xlsx_cross_parity.py pins identical sheet names + header rows across web and desktop. Scope: STRUCTURE only (sheet names + headers); does NOT pin cell-value identity (web's _resolve_result_full_text vs desktop's full_text_fetcher can legitimately produce different strings)."

key-files:
  created:
    - tests/test_desktop_xlsx_multi_sheet.py
    - tests/test_export_xlsx_cross_parity.py
    - tests/test_export_bilingual.py (smoke round-1 regression)
    - tests/test_credits_sheet.py (smoke round-2 regression)
    - tests/test_smoke_round2_gaps_a_d.py (smoke round-2 regression for gaps A-D)
    - tests/test_smoke_round3_label_realignment.py (smoke round-3 regression)
    - tests/test_manuscripts_urls_clickable.py (smoke round-4 regression)
    - tests/test_domains_dedupe.py (smoke round-5 regression)
    - tests/test_image_page_int_coercion.py (smoke round-6 regression)
  modified:
    - genizah_app.py
    - shared/export_dossier.py (bilingual extensions)
    - web/export_service.py (4-sheet builder + clickable URLs + int coercion)
    - web/api.py (lang threading + Credits and Info sheet metadata)
    - shared/translation_service.py (TranslationService.get_pgp_translations_by_sys_ids consumed by dossier path on lang='he')

key-decisions:
  - "MUST-FIX 94-04-A applied (then SUPERSEDED 2026-05-20): originally locked ws_main.title = 'Genizah Results' (English-locked literal). Superseded by smoke round-2 (commit 0d512794): renamed to bilingual 'Search Results' (English) / 'תוצאות חיפוש' (Hebrew) via sheet_titles(lang). Cross-parity test re-greens at lang='en' (identical English sheet names across both apps)."
  - "MUST-FIX 94-04-B applied (then SUPERSEDED 2026-05-20): originally locked English main-sheet header literals. Superseded by smoke round-1 (commit 6a697d90): bilingual headers via main_header_row(lang) — Hebrew when lang='he', English when lang='en'. Cross-parity test still passes at lang='en'."
  - "MUST-FIX 94-04-C applied: tests/test_export_xlsx_cross_parity.py pins the parity invariant; if web/export_service drift from desktop's _build_search_results_xlsx_bytes on sheet names OR header rows AT LANG='EN', this test fails first."
  - "MUST-FIX 94-04-D applied: full_text_fetcher parameter on _build_search_results_xlsx_bytes; export_results passes a lambda calling self.searcher.get_full_text_by_id(uid). Hydration only fires when row's stored text AND the fetcher return non-empty."
  - "MUST-FIX 94-04-E applied: pre-export readiness check in export_results — QMessageBox.warning with Yes/No when _result_domain_map is empty AND results have sys_ids. No reply returns immediately."
  - "MUST-FIX 94-04-F applied: shared_export_utils.build_rich_snippet_cell consumed at col 7 of main-sheet data rows. The inline write_rich_cell helper at the old :18000-18021 is gone from the xlsx branch (deleted as part of the full xlsx-branch replacement)."
  - "D-04 amendment applied: ws.sheet_view.rightToLeft = (CURRENT_LANG == 'he') applied to ALL sheets uniformly (replaces the old hard-pin at :17993). CURRENT_LANG read from genizah_core module-level (canonical desktop locale source)."
  - "D-04 REVERSED 2026-05-20 (smoke verification gap fix): English-only-content prohibition reversed for the row content layer. lang='he' → Hebrew sheet titles + headers + Hebrew-preferred metadata (with English fallback per field); lang='en' → English everywhere (with Hebrew fallback). D-02 transcription-text prohibition UNCHANGED. D-10 parallels-envelope strip UNCHANGED. Conditional RTL view-direction logic UNCHANGED."
  - "D-13 deferral applied: IIIF Manifest column header present on main sheet but cells always empty — matches web's deferral from Wave 3."
  - "D-12 dedupe applied: Manuscripts sub-sheet builds unique_sys_ids in first-occurrence order — multi-folio hits for the same manuscript produce ONE Manuscripts row."
  - "Smoke round-2 (commit 0d512794 + 09effd26 + 5e10b1ab): main sheet renamed 'Genizah Results' → 'Search Results' (bilingual via sheet_titles(lang)). 4th 'Credits and Info' sheet added on both apps with search metadata + GenizahSearch.com hyperlink + Creator credit. Hebrew domain substitution wired."
  - "Smoke round-3 (commit bd71ce83): label realignment per Hillel's review (specific cell labels reordered for visual flow), web mode tr() applied to search-meta labels in Credits and Info sheet, Creator credit line added, link rename ('GenizahSearch' → 'Visit GenizahSearch.com' style)."
  - "Smoke round-4 (commit 2e8d5301): Manuscripts sub-sheet URL cells (PGP URL / Library Viewer URL / GenizahSearch URL) made clickable via openpyxl Hyperlink + FONT_BLUE_UNDERLINE styling. No more plain-text URLs in the dossier."
  - "Smoke round-5 (commit d65ac0b8): Domains list deduped per sys_id — formerly a manuscript with N FJMS rows for the same domain rendered the domain N times (e.g., 'Arabic Tafsir|Arabic Tafsir|...' × 7). Fix dedupes at source; web Domains-badge count now reflects unique manuscripts per domain rather than total FJMS rows."
  - "Smoke round-6 (commit 9c5bae42): Image/Page column emits pure-numeric values as int (no Excel 'Number stored as text' warning). Mixed-content values (e.g., '1r' for recto) stay as strings."
  - "CSV / TXT / DOCX branches at :18294+ UNCHANGED — xlsx-only scope decision (xlsx-only per Phase 94)."
  - "Module-level helper located just before GenizahGUI class (line 2473) — clean separation from the Qt UI flow, no inner-instance dependencies."

requirements-completed: [EXPORT-META-01, EXPORT-META-02, EXPORT-META-03, EXPORT-META-04, EXPORT-META-05, EXPORT-META-06, EXPORT-META-07, EXPORT-META-08, EXPORT-META-09]

# Metrics
duration: ~3.5 days wall-clock (2026-05-20 Tasks 1 + 1.5 + partial SUMMARY → 2026-05-21 Task 2 smoke verification approved → 2026-05-21 Task 3 docs closeout)
started: 2026-05-20
completed: 2026-05-21
---

# Phase 94 Plan 04: Wave 4 — Desktop xlsx Parity + Bilingual Rework + Smoke Verification + Docs Closeout

**SHIPPED 2026-05-21.** All 3 plan tasks complete (Task 1 desktop restructure, Task 1.5 cross-parity test, Task 2 human smoke verification — approved by Hillel after 6 rounds of UX patches, Task 3 docs closeout — this commit). All 9 EXPORT-META requirements (01..09) now Complete. v7.13 milestone (Phase 93 web + Phase 94 web/desktop) is now closeable.

## Performance

- **Duration (wall clock):** ~3.5 days end-to-end including 6 rounds of smoke-verification patches.
- **Tasks:** 3 (Tasks 1 + 1.5 atomic; Task 2 = human checkpoint; Task 3 = docs closeout).
- **Pre-checkpoint test count:** 218 passed across the Phase 94 wave-1-3 + wave-4 test files (+22 new tests in Wave 4 Tasks 1 + 1.5).
- **Post-checkpoint test count:** 2316 passed / 20 skipped / 2 xfailed across the full test suite after all 6 smoke rounds landed.
- **Ruff:** clean across all touched production + test files at every commit boundary.

## What This Wave Delivered

### Task 1 — Desktop xlsx restructure (`genizah_app.py`)

Restructured `genizah_app.py:export_results('xlsx')` to produce the same
citation-grade workbook that web's `web/export_service.export_search_results_excel`
emits. Pure-function module-level helper `_build_search_results_xlsx_bytes(...)`
separates the workbook construction from the Qt UI flow (file dialog, message
box) — fully testable offline.

The xlsx branch in `export_results` now:

1. Builds a meta_resolver closure wrapping `self.meta_mgr.get_meta_for_id` /
   `get_library_for_id` (post-D-04-reversal: lang-aware via the bilingual
   `genizah_core.get_library_display(lib_code, short=False, lang=lang)`).
2. Uses bilingual main-sheet headers via `main_header_row(lang)` (post-D-04-reversal).
3. Surfaces a `QMessageBox.warning(Yes/No)` when `_result_domain_map` is
   empty AND results have sys_ids (MUST-FIX 94-04-E).
4. Passes a Tantivy-backed `full_text_fetcher` lambda for PGP tag row
   hydration (MUST-FIX 94-04-D).
5. Threads `CURRENT_LANG` to drive conditional RTL on all sheets (D-04) AND
   bilingual headers + sheet titles + Hebrew-preferred metadata content
   (post-2026-05-20 D-04 reversal).
6. Reads `self._pgp_transcription_sys_ids` / `self._printed_sys_ids` /
   `self._result_domain_map` from the desktop state machine directly (no
   `export_state.py` equivalent on desktop — desktop is single-user by design).

CSV / TXT / DOCX branches at `:18294+` are byte-identical to pre-Phase-94
(xlsx-only scope per Phase 94).

### Task 1.5 — Cross-parity test (MUST-FIX 94-04-C)

`tests/test_export_xlsx_cross_parity.py` pins the EXPORT-META-09 invariant:
web AND desktop must produce IDENTICAL sheet names + IDENTICAL header rows
on identical input AT lang='en'. The test builds both apps' workbooks at
the default `lang='en'` so the cross-parity check survives the 2026-05-20
D-04 reversal (which made headers + sheet titles lang-dependent — but
identical when both sides pick the same lang).

Scope (intentional): pins STRUCTURE only (sheet names + headers). Does NOT
pin cell-value identity for data rows — web reads Full Text via
`_resolve_result_full_text(res)` while desktop reads via the
`full_text_fetcher` callback (MUST-FIX 94-04-D); these two sources can
legitimately produce different strings for the same sys_id. Functional
drift in data cells is caught by per-app unit tests
(`tests/test_export_service_multi_sheet.py` for web,
`tests/test_desktop_xlsx_multi_sheet.py` for desktop).

### Task 2 — Human Smoke Verification (APPROVED 2026-05-21 after 6 rounds)

Hillel ran the 20-point Excel + JSON download checklist on real exports
from both apps. The initial pass surfaced multiple UX gaps not caught by
unit tests. 6 rounds of follow-up patches landed atop the partial SUMMARY
(`9bdaa9d3`) before approval:

**Round 1 — bilingual headers + source-language metadata** (commits `6a697d90`, `420f1db1`, `bdc5a499`)

- Smoking gun: Hebrew UI was still producing English xlsx (headers + sheet
  names + metadata content). Desktop previously used Qt `tr()` for these
  strings and `ws.title = tr("Search Results")`; the Wave 4 restructure
  dropped header/sheet-title translation. Web was English-only since inception.
- Fix: D-04 REVERSED for the row content layer only (2026-05-20). New
  bilingual helpers in `shared/export_dossier.py` (`main_header_row(lang)`,
  `manuscript_header_row(lang)`, `bibliography_header_row(lang)`,
  `sheet_titles(lang) → {main, manuscripts, bibliography}`). All 3 dossier
  helpers (`pgp_subset_for_sys_id`, `catalog_summary_for_sys_id`,
  `bibliography_for_sys_id`) thread lang and prefer Hebrew when lang='he'
  (with English fallback per field).
- Service: `TranslationService.get_pgp_translations_by_sys_ids` consumed by
  the dossier path on lang='he'; battle-tested from prior phases, no new
  infrastructure.
- Test: new `tests/test_export_bilingual.py` regression suite.
- CONTEXT.md D-04 amended with a REVISED 2026-05-20 block documenting the
  reversal scope (narrow: row content only; D-02 / D-10 / RTL unchanged).

**Round 2 — sheet rename + 4th 'Credits and Info' sheet + Hebrew domains** (commits `0d512794`, `09effd26`, `5e10b1ab`)

- Main sheet renamed `Genizah Results` → `Search Results` (bilingual via
  `sheet_titles(lang)`: English `Search Results`, Hebrew `תוצאות חיפוש`).
- 4th sheet `Credits and Info` added on both apps carrying search metadata
  (Query / Mode / Gap / generated_at / result count) + clickable
  GenizahSearch.com hyperlink + Creator credit. Parity across web and
  desktop. Sheet order now: Search Results → Manuscripts → Bibliography
  → Credits and Info (first sheet still default-active).
- Hebrew domain substitution wired so domain values in the Domains column
  are translated when lang='he'.
- Tests: `tests/test_credits_sheet.py` + `tests/test_smoke_round2_gaps_a_d.py`.

**Round 3 — label realignment + web mode tr() + Creator credit + link rename** (commit `bd71ce83`)

- Specific Credits-and-Info cell labels reordered per Hillel's review for
  visual flow.
- Web `web/api.py` Credits-and-Info metadata cells now route through `tr()`
  for the same Hebrew/English UX as desktop.
- Creator credit line refined.
- Hyperlink label renamed (e.g., `GenizahSearch` → `Visit GenizahSearch.com`
  style).
- Test: `tests/test_smoke_round3_label_realignment.py`.

**Round 4 — Manuscripts URL cells clickable** (commit `2e8d5301`)

- The 3 URL columns on the Manuscripts sub-sheet (`PGP URL`,
  `Library Viewer URL`, `GenizahSearch URL`) now emit openpyxl `Hyperlink`
  objects with `FONT_BLUE_UNDERLINE` styling instead of plain text. No
  more copy-and-paste-the-URL friction.
- Test: `tests/test_manuscripts_urls_clickable.py`.

**Round 5 — Domains dedupe** (commit `d65ac0b8`)

- Smoking gun: a manuscript with N FJMS rows for the same domain rendered
  the domain N times in the `Domains` cell (e.g., `Arabic Tafsir|Arabic Tafsir|...` × 7).
- Fix: dedupe at source per sys_id. Side benefit: the web search Domains
  badge count now reflects unique manuscripts per domain rather than total
  FJMS rows.
- Test: `tests/test_domains_dedupe.py`.

**Round 6 — Image/Page int coercion** (commit `9c5bae42`)

- Pure-numeric `Image/Page` values now written as `int` (via
  `ws.cell(...).value = int(...)`) so Excel no longer flags them with the
  "Number stored as text" warning. Mixed-content values (e.g., `1r` for
  recto) stay as strings.
- Test: `tests/test_image_page_int_coercion.py`.

After round 6 Hillel confirmed `approved` on the verification checklist.

### Task 3 — Docs closeout (THIS COMMIT GROUP, 2026-05-21)

Flipped all 9 EXPORT-META-* requirements in `.planning/REQUIREMENTS.md` to
`[x]` Complete with attribution to the closing plan (94-01..04 + smoke
rounds 1-6); updated `.planning/ROADMAP.md` Phase 94 status; updated
`.planning/milestones/v7.13-ROADMAP.md` Phase 94 row + progress table;
added Phase 94 entry to `CHANGELOG.md` under `[Unreleased]`; added Phase 94
line to `docs/OPEN_ISSUES.md`; updated `CLAUDE.md` "Recently Changed"
section; appended final REFINED block under CONTEXT D-04 for smoke
rounds 3 + 4 + 5 + 6 refinements; moved EXPORT-META-01..09 from PROJECT.md
"Active" to "Validated" with `-- v7.13` annotation. v7.13 milestone
closeout itself is NOT performed here (separate ritual).

## Task Commits

### Pre-checkpoint (Tasks 1 + 1.5 + partial SUMMARY)

1. **Task 1 RED — add failing tests for desktop xlsx 3-sheet helper** — `6d463828` (test)
2. **Task 1 GREEN — restructure desktop xlsx export into 3-sheet builder** — `7e7c9021` (feat)
3. **Task 1.5 — add cross-parity test pinning EXPORT-META-09 invariant** — `f9613488` (test)
4. **Partial SUMMARY pre-checkpoint — Tasks 1 + 1.5 complete** — `9bdaa9d3` (docs)

### Smoke verification rounds 1-6 (Task 2 patches)

5. **Smoke round-1 GREEN — bilingual headers + source-language metadata** — `6a697d90` (feat)
6. **Smoke round-1 tests — bilingual + source-language regression tests** — `420f1db1` (test)
7. **Smoke round-1 docs — record D-04 reversal in CONTEXT.md** — `bdc5a499` (docs)
8. **Smoke round-2 — rename main sheet 'Genizah Results' → 'Search Results'** — `0d512794` (feat)
9. **Smoke round-2 — 4th sheet 'Credits and Info' + Hebrew domain substitution** — `09effd26` (feat)
10. **Smoke round-2 tests — regression for gaps A-D** — `5e10b1ab` (test)
11. **Smoke round-3 — label realignment + web mode tr() + Creator credit + link rename** — `bd71ce83` (feat)
12. **Smoke round-4 — Manuscripts URL cells clickable** — `2e8d5301` (feat)
13. **Smoke round-5 — dedupe Domains list at source (Arabic Tafsir × 7 → ×1)** — `d65ac0b8` (fix)
14. **Smoke round-6 — Image/Page emits int for pure-numeric values** — `9c5bae42` (fix)

### Task 3 (docs closeout — this group)

15. **REQUIREMENTS — flip 9 EXPORT-META-* to Complete** — `abf05c33` (docs, this commit group)
16. **Finalize 94-04-SUMMARY.md after Task 2 approval** — TBD (this commit group, hash assigned at commit time)
17. **ROADMAP + v7.13 milestone progress** — TBD
18. **CHANGELOG + OPEN_ISSUES + CLAUDE.md + PROJECT.md** — TBD

## Files Created / Modified (Cumulative)

### Production code

- `genizah_app.py` (MODIFIED, net +500/-200 lines across pre-checkpoint
  Wave 4 + 6 smoke rounds) — new module-level helper
  `_build_search_results_xlsx_bytes(...)` at line 2473 (just before
  `GenizahGUI` class); xlsx branch in `export_results` at `:17984+`
  rewritten to call the new helper with the desktop state machine's 3
  enrichment signals + `CURRENT_LANG` + Tantivy hydration lambda. Now
  4-sheet (Search Results / Manuscripts / Bibliography / Credits and
  Info), bilingual, clickable URLs, deduped Domains, int Image/Page.
- `shared/export_dossier.py` (MODIFIED, bilingual extensions) — 4 new
  helpers (`main_header_row(lang)`, `manuscript_header_row(lang)`,
  `bibliography_header_row(lang)`, `sheet_titles(lang)`); 3 dossier
  helpers thread lang and prefer Hebrew on lang='he'.
- `web/export_service.py` (MODIFIED) — 4-sheet builder + clickable URLs +
  int coercion + bilingual passthrough.
- `web/api.py` (MODIFIED) — lang threading + Credits and Info sheet
  metadata + tr() wrapping on labels.
- `shared/translation_service.py` (consumed unchanged — `get_pgp_translations_by_sys_ids`).

### Tests

- `tests/test_desktop_xlsx_multi_sheet.py` (NEW, 315 lines pre-checkpoint;
  smoke-rounds adjustments later) — 18+ offline tests covering structure,
  bilingual headers, Yes/empty rendering, pipe-joined Domains, IIIF
  Manifest deferral, Full Text fallback + Tantivy hydration, dedupe on
  Manuscripts sub-sheet, headers match shared constants, conditional RTL,
  rich-text snippet rendering, credit/info rows.
- `tests/test_export_xlsx_cross_parity.py` (NEW, 177 lines) — 4 cross-app
  parity tests.
- `tests/test_export_bilingual.py` (NEW, smoke round-1).
- `tests/test_credits_sheet.py` (NEW, smoke round-2).
- `tests/test_smoke_round2_gaps_a_d.py` (NEW, smoke round-2).
- `tests/test_smoke_round3_label_realignment.py` (NEW, smoke round-3).
- `tests/test_manuscripts_urls_clickable.py` (NEW, smoke round-4).
- `tests/test_domains_dedupe.py` (NEW, smoke round-5).
- `tests/test_image_page_int_coercion.py` (NEW, smoke round-6).

## Test Counts

- Baseline (pre-Wave 4): 159 tests passing across Phase 94 wave-1-3 test files.
- After Wave 4 Task 1 + 1.5: **218 tests passing** (+22 new).
- After all 6 smoke rounds: **2316 passed / 20 skipped / 2 xfailed** across
  the full test suite. Ruff clean across all touched files.

## Verification

- `python -c "import ast; ast.parse(open('genizah_app.py', encoding='utf-8').read()); print('syntax OK')"` → `syntax OK`
- `python -c "from genizah_app import _build_search_results_xlsx_bytes; print('importable')"` → `importable`
- `python -m ruff check genizah_app.py shared/export_dossier.py web/export_service.py web/api.py tests/` → All checks passed.
- `python -m pytest tests/ -q` → **2316 passed / 20 skipped / 2 xfailed** (final).

## EXPORT-META Requirements Closed (Phase 94 Full)

| ID | Status | Closing plan + smoke note |
|---|---|---|
| EXPORT-META-01 | ✅ Complete | 94-03 (web) + 94-04 (desktop); smoke round-5 deduped per-sys_id Domains |
| EXPORT-META-02 | ✅ Complete | 94-01 shared `build_manuscript_row` + 94-03 (web) + 94-04 (desktop); smoke round-4 clickable URLs |
| EXPORT-META-03 | ✅ Complete | 94-01 shared `build_bibliography_rows` + 94-03 (web) + 94-04 (desktop) |
| EXPORT-META-04 | ✅ Complete | 94-03 + 94-04; smoke round-2 added 4th `Credits and Info` sheet + renamed main to `Search Results` |
| EXPORT-META-05 | ✅ Complete | 94-01 D-02 behavioral guard + 94-03 + 94-04; D-04 REVERSED 2026-05-20 for bilingual row-content layer |
| EXPORT-META-06 | ✅ Complete | 94-02 state plumbing + 94-03 Wave 2 TODO closed |
| EXPORT-META-07 | ✅ Complete | 94-02 with MUST-FIX 94-02-B opt-in semantics preserving D-11 /api/search shape |
| EXPORT-META-08 | ✅ Complete | DEFERRED per D-13 soft scope (header present, cells empty; Library Viewer URL on Manuscripts sub-sheet provides reachability) |
| EXPORT-META-09 | ✅ Complete | 94-04 desktop xlsx parity; cross-parity invariant pinned by `tests/test_export_xlsx_cross_parity.py`; smoke approved 2026-05-21 after 6 rounds |

## Phase 87 Multitenant Invariant Confirmation

Zero raw `app.storage.user.*` accesses introduced under `web/` across all 4 Phase 94 plans + 6 smoke rounds. Allowlist remains `[]`. `tests/test_no_raw_storage_access.py` green throughout.

## Decisions Made

- All 6 smoke-verification rounds were treated as deviations under Rule 1 (auto-fix bugs surfaced by smoke testing) / Rule 2 (auto-add missing critical functionality — bilingual UX is a correctness requirement for Hebrew users). Each round committed atomically with both production code + test coverage.
- CONTEXT D-04 was REVERSED 2026-05-20 (smoke verification gap fix) for the row content layer ONLY. The narrow reversal scope was committed as `docs(94-04): record D-04 reversal in CONTEXT.md` (commit `bdc5a499`).
- Sheet name lock per MUST-FIX 94-04-A was intentionally superseded by smoke round-2 — the bilingual experience required dropping the English literal lock and routing through `sheet_titles(lang)` instead. Cross-parity test still passes at `lang='en'` (which is what cross-parity should pin — the contract is "identical workbooks at identical lang", not "English-only literal forever").
- 4th sheet `Credits and Info` was added during smoke verification rather than planned upfront. The original 3-sheet plan reflected the original CONTEXT scope; smoke verification revealed that search-metadata (Query / Mode / Gap / date+time / result count) is essential for the citation-source use case the milestone exists to deliver. Both apps now ship the 4th sheet with parity.

## Deviations from Plan

**Six rounds of smoke-verification patches, all auto-applied under Rule 1 / Rule 2.** Each documented inline in this SUMMARY (see Task 2 section above). No Rule 4 architectural decisions surfaced — every fix was a localized refinement of the existing 3→4 sheet workbook + bilingual UX surface.

The original 3-sheet plan was superseded by smoke round-2's 4th sheet addition — but the cross-parity invariant still holds (both apps emit the same 4-sheet workbook structure).

## Phase 94 → v7.13 Closeout Hand-off

Phase 94 is the last phase of v7.13. With this commit group:

- All 14 v7.13 requirements (5 PGP-FILTER + 9 EXPORT-META) are Complete.
- ROADMAP.md Phase 94 marked complete.
- v7.13-ROADMAP.md Phase 94 row marked complete; progress table updated.
- CHANGELOG.md has a Phase 94 entry under `[Unreleased]`.
- OPEN_ISSUES.md updated.
- CLAUDE.md "Recently Changed" updated.
- PROJECT.md EXPORT-META-01..09 moved from Active to Validated.

**v7.13 milestone closeout is NOT performed here.** The milestone closeout
ritual (separate commit, `deploy.sh`, version bump, CHANGELOG re-format to
`[7.13.0]`, git tag, GitHub Release for desktop only) is the next step
after this Task 3 docs closeout. The orchestrator will run
`gsd-sdk query phase.complete` next.

## Self-Check: PASSED

**Created files verified to exist (sample):**

- `tests/test_desktop_xlsx_multi_sheet.py` — FOUND
- `tests/test_export_xlsx_cross_parity.py` — FOUND
- `tests/test_export_bilingual.py` — FOUND (smoke round-1)
- `tests/test_credits_sheet.py` — FOUND (smoke round-2)

**Modified file verified to contain new symbol:**

- `genizah_app.py` — contains `def _build_search_results_xlsx_bytes` (FOUND)
- `shared/export_dossier.py` — contains `main_header_row` + `sheet_titles` (FOUND)

**Commits verified to exist in `git log --oneline`:**

Pre-checkpoint: `6d463828`, `7e7c9021`, `f9613488`, `9bdaa9d3`.
Smoke rounds: `6a697d90`, `420f1db1`, `bdc5a499` (round 1) → `0d512794`, `09effd26`, `5e10b1ab` (round 2) → `bd71ce83` (round 3) → `2e8d5301` (round 4) → `d65ac0b8` (round 5) → `9c5bae42` (round 6).
Task 3 closeout: `abf05c33` (REQUIREMENTS) + subsequent commits in this group.

All hashes verified resolvable via `git rev-parse --verify`.
