---
phase: 94-adding-pgp-to-downloaded-data
plan: 04
subsystem: export
tags: [xlsx, desktop, 3-sheet, dossier, openpyxl, cross-parity, wave-4, tdd, checkpoint-pending]
status: in-progress-awaiting-checkpoint

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
  - Desktop export_results('xlsx') restructured to emit 3-sheet workbook structurally identical to web's output (EXPORT-META-09)
  - tests/test_desktop_xlsx_multi_sheet.py — 18 offline tests pinning 3-sheet structure
  - tests/test_export_xlsx_cross_parity.py — 4 cross-app parity tests asserting identical sheet names + headers
affects: [Phase 94 closeout (Task 3 docs after checkpoint approval)]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Module-level pure-function builder pattern: _build_search_results_xlsx_bytes(...) returns bytes — Qt-free, no instance dependencies. Mirrors web/export_service.export_search_results_excel as a pure function returning (bytes, filename)."
    - "MetaResolver callable contract on desktop: closure inside export_results wraps self.meta_mgr.get_meta_for_id / get_library_for_id + genizah_core.get_library_display(lang='en') hard-pin. Matches the Codex SHOULD-FIX 8 pattern from Wave 1."
    - "English-locked main-sheet headers per MUST-FIX 94-04-B: tr() is EXPLICITLY OVERRIDDEN for these 12 specific strings ('System ID', 'Library', ...) — single-strings-list scope, NOT a broader desktop convention change. Other UI strings (buttons, menus, dialogs) stay tr()-translated."
    - "Pre-export domain-readiness check (MUST-FIX 94-04-E): QMessageBox.warning with Yes/No surfaced when _result_domain_map is empty AND results have sys_ids. No reply aborts the export cleanly; Yes proceeds with the documented gap."
    - "Tantivy-backed full_text hydration callback (MUST-FIX 94-04-D): _build_search_results_xlsx_bytes accepts a full_text_fetcher parameter; the export_results call site passes self.searcher.get_full_text_by_id(uid) for rows lacking 'full_text' / 'full_text_excerpt' (PGP tag rows at :17065-17076 omit both)."
    - "Cross-app parity regression test (MUST-FIX 94-04-C): tests/test_export_xlsx_cross_parity.py pins identical sheet names + header rows across web and desktop. Scope: STRUCTURE only (sheet names + headers); does NOT pin cell-value identity (web's _resolve_result_full_text vs desktop's full_text_fetcher can legitimately produce different strings)."

key-files:
  created:
    - tests/test_desktop_xlsx_multi_sheet.py
    - tests/test_export_xlsx_cross_parity.py
  modified:
    - genizah_app.py

key-decisions:
  - "MUST-FIX 94-04-A applied: ws_main.title = 'Genizah Results' (English-locked literal). Cross-parity test asserts identical sheet names across web and desktop on identical input."
  - "MUST-FIX 94-04-B applied: main-sheet header literals are passed in English ('System ID', 'Library', 'Shelfmark', 'Title', 'Image/Page', 'Source', 'Snippet', 'Full Text', 'Has PGP', 'Is Printed', 'Domains', 'IIIF Manifest') — desktop tr() convention is EXPLICITLY OVERRIDDEN for this single strings list per EXPORT-META-09 parity."
  - "MUST-FIX 94-04-C applied: tests/test_export_xlsx_cross_parity.py pins the parity invariant; if web/export_service drift from desktop's _build_search_results_xlsx_bytes on sheet names OR header rows, this test fails first."
  - "MUST-FIX 94-04-D applied: full_text_fetcher parameter on _build_search_results_xlsx_bytes; export_results passes a lambda calling self.searcher.get_full_text_by_id(uid). Hydration only fires when row's stored text AND the fetcher return non-empty."
  - "MUST-FIX 94-04-E applied: pre-export readiness check in export_results — QMessageBox.warning with Yes/No when _result_domain_map is empty AND results have sys_ids. No reply returns immediately."
  - "MUST-FIX 94-04-F applied: shared_export_utils.build_rich_snippet_cell consumed at col 7 of main-sheet data rows. The inline write_rich_cell helper at the old :18000-18021 is gone from the xlsx branch (deleted as part of the full xlsx-branch replacement)."
  - "D-04 amendment applied: ws.sheet_view.rightToLeft = (CURRENT_LANG == 'he') applied to ALL 3 sheets uniformly (replaces the old hard-pin at :17993). CURRENT_LANG read from genizah_core module-level (canonical desktop locale source)."
  - "D-13 deferral applied: IIIF Manifest column header present on main sheet but cells always empty — matches web's deferral from Wave 3."
  - "D-12 dedupe applied: Manuscripts sub-sheet builds unique_sys_ids in first-occurrence order — multi-folio hits for the same manuscript produce ONE Manuscripts row."
  - "CSV / TXT / DOCX branches at :18294+ UNCHANGED — xlsx-only scope decision (xlsx-only per Phase 94)."
  - "Module-level helper located just before GenizahGUI class (line 2473) — clean separation from the Qt UI flow, no inner-instance dependencies."

requirements-completed-pending-verification: [EXPORT-META-01, EXPORT-META-02, EXPORT-META-03, EXPORT-META-04, EXPORT-META-05, EXPORT-META-08, EXPORT-META-09]

# Metrics
duration: in-progress
started: 2026-05-20
completed: pending-human-checkpoint
---

# Phase 94 Plan 04: Wave 4 — Desktop xlsx Parity + Human Smoke Checkpoint Summary (PENDING)

> **STATUS: Tasks 1 + 1.5 COMPLETE. Task 2 (Human Smoke Verification Checkpoint) PENDING.
> Task 3 (docs closeout) will execute after the user replies `approved` to the
> 20-point verification checklist in Task 2.**

## What This Wave Delivered (Pre-Checkpoint)

### Task 1 — Desktop xlsx restructure (`genizah_app.py`)

Restructured `genizah_app.py:export_results('xlsx')` to produce the same
3-sheet citation-grade workbook that web's `web/export_service.export_search_results_excel`
emits (per Wave 3). Pure-function module-level helper
`_build_search_results_xlsx_bytes(...)` separates the workbook construction
from the Qt UI flow (file dialog, message box) — fully testable offline.

The xlsx branch in `export_results` now:

1. Builds a meta_resolver closure wrapping `self.meta_mgr.get_meta_for_id` /
   `get_library_for_id` and hard-pinning library_name to English via
   `genizah_core.get_library_display(lib_code, short=False, lang='en')`
   (D-04 / Shared Pattern F from Wave 1).
2. Uses English-locked main-sheet header literals (MUST-FIX 94-04-B).
3. Surfaces a `QMessageBox.warning(Yes/No)` when `_result_domain_map` is
   empty AND results have sys_ids (MUST-FIX 94-04-E).
4. Passes a Tantivy-backed `full_text_fetcher` lambda for PGP tag row
   hydration (MUST-FIX 94-04-D).
5. Threads `CURRENT_LANG` to drive conditional RTL on all 3 sheets (D-04).
6. Reads `self._pgp_transcription_sys_ids` / `self._printed_sys_ids` /
   `self._result_domain_map` from the desktop state machine directly (no
   `export_state.py` equivalent on desktop — desktop is single-user by design).

CSV / TXT / DOCX branches at `:18294+` are byte-identical to pre-Phase-94
(xlsx-only scope per Phase 94).

### Task 1.5 — Cross-parity test (MUST-FIX 94-04-C)

`tests/test_export_xlsx_cross_parity.py` pins the EXPORT-META-09 invariant:
web AND desktop must produce IDENTICAL sheet names + IDENTICAL header rows
on identical input. 4 tests:

- `test_sheet_names_identical` — `['Genizah Results', 'Manuscripts', 'Bibliography']` order + same active sheet across both apps.
- `test_main_sheet_headers_byte_identical` — 12-col header row byte-identical.
- `test_manuscripts_sub_sheet_headers_identical` — 14-col header row identical.
- `test_bibliography_sub_sheet_headers_identical` — 8-col header row identical.

Scope (intentional): pins STRUCTURE only (sheet names + headers). Does NOT
pin cell-value identity for data rows — web reads Full Text via
`_resolve_result_full_text(res)` while desktop reads via the
`full_text_fetcher` callback (MUST-FIX 94-04-D); these two sources can
legitimately produce different strings for the same sys_id. Functional
drift in data cells is caught by per-app unit tests
(`tests/test_export_service_multi_sheet.py` for web,
`tests/test_desktop_xlsx_multi_sheet.py` for desktop).

## Task Commits (So Far)

1. **Task 1 RED — add failing tests for desktop xlsx 3-sheet helper** — `6d463828` (test)
2. **Task 1 GREEN — restructure desktop xlsx export into 3-sheet builder** — `7e7c9021` (feat)
3. **Task 1.5 — add cross-parity test pinning EXPORT-META-09 invariant** — `f9613488` (test)

## Files Created / Modified (So Far)

- `tests/test_desktop_xlsx_multi_sheet.py` (NEW, 315 lines) — 18 offline tests
  covering 3-sheet structure, 12-col main-sheet headers (English-locked),
  Yes/empty rendering for Has PGP + Is Printed, pipe-joined Domains, IIIF
  Manifest empty (D-13), Full Text fallback chain + Tantivy hydration, dedupe
  on Manuscripts sub-sheet, headers match shared constants, conditional RTL,
  rich-text snippet rendering, credit/info rows preserved above headers.
- `tests/test_export_xlsx_cross_parity.py` (NEW, 177 lines) — 4 cross-app parity tests.
- `genizah_app.py` (MODIFIED, +306/-81 lines) — new module-level helper
  `_build_search_results_xlsx_bytes(...)` at line 2472 (just before
  `GenizahGUI` class); xlsx branch in `export_results` at `:17984+` rewritten
  to call the new helper with the desktop state machine's 3 enrichment signals
  + `CURRENT_LANG` + Tantivy hydration lambda.

## Test Counts

- Baseline (pre-Wave 4): 159 tests passing across Phase 94 wave-1-3 test files.
- After Wave 4 Task 1 + 1.5: **218 tests passing** (+22 tests):
  - +18 in `tests/test_desktop_xlsx_multi_sheet.py`
  - +4 in `tests/test_export_xlsx_cross_parity.py`
- Full sweep: `python -m pytest tests/test_export_dossier.py tests/test_shared_rich_snippet.py tests/test_export_state_enrichment.py tests/test_search_serializer.py tests/test_parallels_envelope_no_pgp_keys.py tests/test_export_service_multi_sheet.py tests/test_desktop_xlsx_multi_sheet.py tests/test_export_xlsx_cross_parity.py tests/test_no_raw_storage_access.py tests/test_export_state_selection.py tests/test_export_state_cap.py -q` → 218 passed.

## Verification Run (Pre-Checkpoint)

- `python -c "import ast; ast.parse(open('genizah_app.py', encoding='utf-8').read()); print('syntax OK')"` → `syntax OK`
- `python -c "from genizah_app import _build_search_results_xlsx_bytes; print('importable')"` → `importable`
- `python -m ruff check genizah_app.py tests/test_desktop_xlsx_multi_sheet.py tests/test_export_xlsx_cross_parity.py` → All checks passed
- Acceptance grep counts (Task 1):
  - `grep -c "^def _build_search_results_xlsx_bytes" genizah_app.py` → 1
  - `grep -c "from shared.export_dossier import" genizah_app.py` → 1
  - `grep -c "from shared_export_utils import build_rich_snippet_cell" genizah_app.py` → 1
  - `grep -c "_build_search_results_xlsx_bytes(" genizah_app.py` → 2 (definition + call site)
  - `grep "ws.sheet_view.rightToLeft = True\b" genizah_app.py` → NO MATCH (old hard-pin gone)
  - `grep "core_get_library_display\|get_library_display.*lang='en'" genizah_app.py` → 2 matches (import alias + call)

## Pending — Task 2 (Human Smoke Verification Checkpoint)

The plan's `<task type="checkpoint:human-verify" gate="blocking">` requires the
user to run both apps and verify the 20-point checklist (a)-(t) on real
Excel + JSON downloads. Until the user replies `approved` (or describes a
failure), Task 3 (docs closeout) MUST NOT proceed.

## Pending — Task 3 (Docs Closeout)

After `approved`:

- Flip all 9 EXPORT-META-* requirements in `.planning/REQUIREMENTS.md` to `[x]` Complete.
- Update `.planning/ROADMAP.md` v7.13 Phase 94 row.
- Flip `.planning/milestones/v7.13-ROADMAP.md` Phase 94 row to 4/4 Complete.
- Add Phase 94 entry to `CHANGELOG.md` under v7.13 in-progress section.
- Refresh `docs/OPEN_ISSUES.md` Last Updated timestamp.
- Finalize this SUMMARY with the smoke verdict + 9/9 requirements traceability + Phase 94 hand-off.

## Self-Check: PASSED (Tasks 1 + 1.5)

**Created files verified to exist:**

- `tests/test_desktop_xlsx_multi_sheet.py` — FOUND
- `tests/test_export_xlsx_cross_parity.py` — FOUND

**Modified file verified to contain new symbol:**

- `genizah_app.py` — contains `def _build_search_results_xlsx_bytes` (FOUND)

**Commits verified to exist in `git log --oneline`:**

- `6d463828` (test(94-04): add failing tests for desktop xlsx 3-sheet helper) — FOUND
- `7e7c9021` (feat(94-04): restructure desktop xlsx export into 3-sheet builder) — FOUND
- `f9613488` (test(94-04): add cross-parity test pinning EXPORT-META-09 invariant) — FOUND
