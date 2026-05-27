---
phase: 95
plan: "09"
subsystem: docs-export-web-guard-packaging
tags: [help, about, export, local, packaging, pymupdf, ast-guard, bookkeeping]
dependency_graph:
  requires: [95-02, 95-03, 95-04, 95-07, 95-08]
  provides: [phase-95-closeout, skip_local-kwarg, web-library-guard, packaging-smoke]
  affects: [shared/export_dossier.py, web/export_service.py, genizah_app.py, web/pages/help.py, web/pages/about.py, Help.html, GenizahSearchPro.spec]
tech_stack:
  added: []
  patterns:
    - skip_local kwarg on row builders (defense-in-depth pattern)
    - static AST guard scanning web/pages/ for LIBRARY_CODES consumers
    - headless CLI self-test flag before QApplication construction
    - two-tier packaging smoke (venv Tier-1 + EXE subprocess Tier-2)
key_files:
  created:
    - tests/test_export_dossier_local_handling.py
    - tests/test_web_library_options_no_local.py
    - tests/test_local_pyinstaller_smoke.py (replaced Wave-0 stub)
  modified:
    - web/pages/help.py
    - web/pages/about.py
    - Help.html
    - genizah_app.py
    - genizah_translations.py
    - shared/export_dossier.py
    - web/export_service.py
    - GenizahSearchPro.spec
    - docs/OPEN_ISSUES.md
    - CHANGELOG.md
    - CLAUDE.md
    - .planning/phases/95-my-library/95-VALIDATION.md
decisions:
  - "D-30/D-46 audit confirmed: no web/pages/*.py currently iterates LIBRARY_CODES; AST guard pins the invariant for future additions (NO-OP today)"
  - "D-32 Seewald attribution final wording: EN = 'My Library feature inspired by Yehuda Seewald's GenizahLocal prototype.' HE = 'תכונת הספרייה שלי בהשראת אב-טיפוס GenizahLocal של יהודה זיוואלד.' (per CONTEXT.md)"
  - "D-45 skip_local placed on build_manuscript_row + build_bibliography_rows using is_local_sys_id detection; web caller passes skip_local=True, desktop skip_local=False"
  - "HIGH-5 self-test flag uses sys._MEIPASS detection for frozen EXE path; fixture path resolves correctly in both venv and bundled contexts"
  - "wave_0_complete flipped to true: all 26 Wave-0 stubs confirmed GREEN (0 'Wave 0 placeholder' strings in pytest output across 164 Phase 95 tests)"
metrics:
  duration_minutes: 25
  completed_date: "2026-05-21"
  tasks_completed: 5
  files_changed: 12
---

# Phase 95 Plan 09: Docs, Export Web Guard, and Packaging — Summary

Phase 95 closeout: Help/About documentation, export LOCAL filtering, web guard,
PyInstaller packaging smoke, and project bookkeeping. All 5 tasks GREEN.

## What Was Built

### Task 1 — Help + Seewald Attribution (web + desktop, EN + HE)

`web/pages/help.py` gains a new "My Library — Local Documents" section in both
English and Hebrew TOC + content, covering: what gets indexed, where data lives,
privacy guarantee + three cloud-write gates, three-state filter usage, hostname-
rename caveat, and D-33 cleartext-on-disk disclosure. The Hebrew section mirrors
the English content.

`web/pages/about.py` credits sections (EN + HE) each get a D-32 Seewald
attribution line:
- EN: "My Library feature inspired by Yehuda Seewald's GenizahLocal prototype."
- HE: "תכונת הספרייה שלי בהשראת אב-טיפוס GenizahLocal של יהודה זיוואלד."

`genizah_app.py` desktop About tab gains the same EN attribution under a new
"My Library" subsection. `genizah_translations.py` ABOUT_HTML Hebrew translation
gains the HE attribution.

`Help.html` (desktop static help) gains full My Library sections in both the
English and Hebrew div blocks, including D-33 cleartext disclosure and D-32
Seewald attribution.

Commit: `9d3caba4`

### Task 2 — Export-path skip_local kwarg (D-45)

`shared/export_dossier.py`:
- `build_manuscript_row` gains `skip_local: bool = False` — returns `None` for
  LOCAL sys_ids when `True` (detected via `is_local_sys_id`).
- `build_bibliography_rows` gains `skip_local: bool = False` — returns `[]` for
  LOCAL sys_ids when `True`.

`web/export_service.py`: both callers now pass `skip_local=True` (defense-in-depth
for web — web Tantivy never indexes LOCAL, but the shared helper is hardened).

`genizah_app.py`: both callers now pass `skip_local=False` explicitly (desktop
includes LOCAL rows in the user's local xlsx file; PGP/NLI/viewer URL cells are
empty for LOCAL since no upstream metadata exists).

`tests/test_export_dossier_local_handling.py`: 9 tests covering include/exclude
paths for both row builders. All GREEN (was Wave-0 stub raising NotImplementedError).

Regression: `tests/test_export_xlsx_cross_parity.py` 4/4 still pass.

Commit: `79ee57bd`

### Task 3 — Web library-options static AST guard (D-46)

`tests/test_web_library_options_no_local.py`: 3 tests.
- `test_no_web_page_iterates_library_codes_without_local_guard`: scans every
  `.py` under `web/pages/` via AST; any function iterating `LIBRARY_CODES`
  without a `'LOCAL'` string comparison guard fails CI.
- `test_exempt_functions_set_is_defined`: structural guard.
- `test_web_pages_dir_exists`: sanity.

**D-30/D-46 audit result:** No `web/pages/*.py` file currently iterates
`LIBRARY_CODES` — the modification to existing web files is a NO-OP today. The
guard pins the invariant for future consumers.

All 3 tests GREEN (was Wave-0 stub with `pytest.skip`).

Commit: `6acb3ecc`

### Task 4 — PyInstaller packaging smoke (D-43 + HIGH-5)

`genizah_app.py` `if __name__ == "__main__":` block: `--self-test-pymupdf` flag
checked BEFORE `QApplication(sys.argv)` construction. The handler imports fitz,
opens `tests/fixtures/local_indexer/hebrew_sample.pdf`, runs `get_text("blocks")`,
prints `PYMUPDF_OK` (exit 0) or `PYMUPDF_FAIL: ...` (exit 1). Supports both dev
(uses `__file__` parent) and frozen EXE (`sys._MEIPASS`) paths.

`GenizahSearchPro.spec`: `datas` list extended with
`('tests\\fixtures\\local_indexer\\hebrew_sample.pdf', 'tests/fixtures/local_indexer')`
so the fixture is bundled into the packaged EXE for the Tier-2 subprocess test.

`tests/test_local_pyinstaller_smoke.py`: 4 tests all gated `@pytest.mark.packaging`.
- **Tier 1** (venv, always runs): `test_fitz_importable` + `test_packaged_exe_extracts_hebrew_pdf` + `test_spec_file_collects_pymupdf` — 3 PASS.
- **Tier 2** (EXE required): `test_packaged_exe_self_test_pymupdf_subprocess` — 1 SKIP (EXE not built in dev; will PASS in release CI after `pyinstaller GenizahSearchPro.spec`).

Was Wave-0 stub with single `pytest.skip`. Now 3 PASS + 1 SKIP.

Commit: `1289af45`

### Task 5 — Project bookkeeping

- `docs/OPEN_ISSUES.md`: Last Updated timestamp updated with Phase 95 closeout note.
- `CHANGELOG.md`: new `## [Unreleased]` section with `### v7.14 — My Library (Phase 95)` entry. NO version bump (release-time action per CLAUDE.md).
- `CLAUDE.md` "Recently Changed": new entry at the top: `v7.14 Phase 95 — My Library CLOSED (2026-05-21)`.
- `.planning/phases/95-my-library/95-VALIDATION.md`: `wave_0_complete: false` flipped to `wave_0_complete: true` (LOW-1 review fix — all 26 Wave-0 stubs confirmed GREEN; 0 "Wave 0 placeholder" strings in pytest output).

Commit: `ca7f6659`

## Deviations from Plan

None — plan executed exactly as written.

**D-30/D-46 audit note (expected):** The plan flagged that web/pages modification might be a NO-OP. Confirmed: zero `LIBRARY_CODES` iteration sites in `web/pages/*.py` today. The AST guard is purely forward-looking.

**genizah_translations.py added (not in plan file list):** The plan listed `genizah_app.py` for desktop About attribution but the Hebrew desktop About dialog is rendered from `ABOUT_HTML` in `genizah_translations.py`. The file was added to the Task 1 commit as Rule 2 (missing functionality — without it, the Hebrew About tab would lack the Seewald attribution). Not a deviation — a correctness requirement.

## Known Stubs

None. All Wave-0 stubs for this plan are now GREEN.

## Threat Flags

None — no new network endpoints, auth paths, or schema changes introduced by this plan.

## Self-Check
