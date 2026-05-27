---
phase: 95
plan: "01"
subsystem: my-library
tags: [wave-0, stubs, packaging, pymupdf, nyquist, fixtures]
dependency_graph:
  requires: []
  provides:
    - 26 Wave-0 skipped-placeholder stub test files collectable by pytest
    - pymupdf>=1.24,<2.0 pinned in requirements.txt
    - GenizahSearchPro.spec updated with collect_all('pymupdf')
    - Hebrew PDF fixture + expected.txt for D-44
    - tests/conftest.py: temp_local_index_dir, mock_supabase_client, local_indexer_fixtures_dir
    - tests/fixtures/local_sys_id_fixtures.py: 5 fixture constants
  affects:
    - tests/ (26 new stub files + conftest.py + fixtures)
    - requirements.txt (pymupdf line added)
    - GenizahSearchPro.spec (pymupdf collect_all + hiddenimports)
    - pyproject.toml (packaging marker registered)
    - .planning/phases/95-my-library/95-VALIDATION.md (Wave-0 checklist checked off)
tech_stack:
  added:
    - pymupdf>=1.24,<2.0 (fitz 1.27.2.3 installed)
  patterns:
    - Wave-0 skipped-placeholder stubs (pytest.skip / NotImplementedError with tracking ref)
    - pytest markers: packaging (D-43 CI gate, added to pyproject.toml)
    - try/except ImportError + pytest.skip(allow_module_level=True) for module-level skip
key_files:
  created:
    - tests/test_local_sys_id_namespace.py
    - tests/test_local_sys_id_parser_compat.py
    - tests/test_local_indexer.py
    - tests/test_local_indexer_incremental.py
    - tests/test_local_indexer_scale.py
    - tests/test_local_indexer_mutex.py
    - tests/test_side_index_merge.py
    - tests/test_local_post_dedup_merge.py
    - tests/test_local_lab_invalidation.py
    - tests/test_local_two_phase_commit.py
    - tests/test_local_delete_by_uid.py
    - tests/test_local_index_open_fallback.py
    - tests/test_local_unavailable_folder.py
    - tests/test_canonical_filepath.py
    - tests/test_folder_overlap_detection.py
    - tests/test_local_filter_cascade.py
    - tests/test_local_filter_persistence.py
    - tests/test_local_namespace_no_api_leak.py
    - tests/test_local_namespace_no_lists_leak.py
    - tests/test_local_namespace_no_corrections_leak.py
    - tests/test_web_library_options_no_local.py
    - tests/test_export_dossier_local_handling.py
    - tests/test_local_schema_evolution.py
    - tests/test_local_pyinstaller_smoke.py
    - tests/test_local_ceiling_enforcement.py
    - tests/test_my_library_tab.py
    - tests/fixtures/local_sys_id_fixtures.py
    - tests/fixtures/local_indexer/hebrew_sample.pdf
    - tests/fixtures/local_indexer/hebrew_sample.expected.txt
    - GenizahSearchPro.spec (new in worktree; gitignored upstream, force-added)
  modified:
    - requirements.txt (pymupdf line added after python-docx==1.2.0)
    - tests/conftest.py (3 fixtures appended)
    - pyproject.toml (packaging marker registered)
    - .planning/phases/95-my-library/95-VALIDATION.md (Wave-0 checklist checked off)
decisions:
  - "GenizahSearchPro.spec is gitignored (*.spec in .gitignore); force-added to worktree with git add -f"
  - "Ruff auto-removed unused imports from stub files (pytest import not needed when tests only raise NotImplementedError); noqa: F401 added to intentional try/except imports"
  - "packaging pytest marker registered in pyproject.toml to suppress PytestUnknownMarkWarning from test_local_pyinstaller_smoke.py"
  - "VALIDATION.md nyquist_compliant: true was already set by planner; Task 7 only updated Wave-0 checklist to [x]"
metrics:
  duration: "590s (~10 minutes)"
  completed: "2026-05-21"
  tasks_completed: 7
  tasks_total: 7
  files_created: 30
  files_modified: 4
---

# Phase 95 Plan 01: Wave-0 Stubs and Packaging Summary

Wave-0 foundations established: 26 skipped-placeholder stub test files enforce the Nyquist verification contract, pymupdf pinned for Hebrew RTL PDF extraction, PyInstaller spec updated to collect PyMuPDF C-extension binaries, Hebrew PDF fixture committed, and shared conftest fixtures registered for Wave 1-3 plans.

## Tasks Completed

| Task | Name | Commit | Key Files |
|------|------|--------|-----------|
| 1 | Verify Hebrew PDF fixtures (pre-resolved) | fd2ede05 | tests/fixtures/local_indexer/hebrew_sample.pdf + .expected.txt |
| 2 | Pin pymupdf in requirements.txt | fd2ede05 | requirements.txt |
| 3 | Update GenizahSearchPro.spec (D-43) | fd876bc6 | GenizahSearchPro.spec |
| 4 | Create shared conftest fixtures | a56d46c5 | tests/conftest.py |
| 5 | Create LOCAL sys_id fixtures module | d4686650 | tests/fixtures/local_sys_id_fixtures.py |
| 6 | Create 26 Wave-0 stub test files | c34f4696 | tests/test_local_*.py + 5 others + pyproject.toml |
| 7 | Mark VALIDATION.md Wave-0 checklist complete | d18db6ca | .planning/phases/95-my-library/95-VALIDATION.md |

## 26 Stub Test Files

All 26 stubs are collectable by `pytest --collect-only` (37 test functions total). Each raises `NotImplementedError` or `pytest.skip()` with a tracking reference to the implementing plan. None pass — RED until Wave 1-3 ships.

| # | File | Requirement | Implementing Plan |
|---|------|-------------|-------------------|
| 1 | test_local_sys_id_namespace.py | REQ-2 + D-19 | Plan 02 Wave 1 |
| 2 | test_local_sys_id_parser_compat.py | D-13 | Plan 02 Wave 1 |
| 3 | test_local_indexer.py | REQ-1 + REQ-4 + D-44 | Plan 03 Wave 1 |
| 4 | test_local_indexer_incremental.py | REQ-5 + D-36 | Plan 03 Wave 1 |
| 5 | test_local_indexer_scale.py | REQ-10 (slow) | Plan 07 Wave 3 |
| 6 | test_local_indexer_mutex.py | D-25 | Plan 07 Wave 3 |
| 7 | test_side_index_merge.py | REQ-3 + D-08 | Plan 05 Wave 2 |
| 8 | test_local_post_dedup_merge.py | D-08 Codex P0 | Plan 05 Wave 2 |
| 9 | test_local_lab_invalidation.py | D-09 + D-38 | Plan 06 Wave 3 |
| 10 | test_local_two_phase_commit.py | D-21 | Plan 03 Wave 1 |
| 11 | test_local_delete_by_uid.py | D-20 + D-36 | Plan 03 Wave 1 |
| 12 | test_local_index_open_fallback.py | D-37 | Plan 05 Wave 2 |
| 13 | test_local_unavailable_folder.py | D-40 | Plan 07 Wave 3 |
| 14 | test_canonical_filepath.py | D-42 | Plan 02 Wave 1 |
| 15 | test_folder_overlap_detection.py | D-17 | Plan 03 Wave 1 |
| 16 | test_local_filter_cascade.py | REQ-6 + D-10 | Plan 08 Wave 4 |
| 17 | test_local_filter_persistence.py | D-39 | Plan 08 Wave 4 |
| 18 | test_local_namespace_no_api_leak.py | REQ-9 | Plan 04 Wave 1 |
| 19 | test_local_namespace_no_lists_leak.py | REQ-9 + D-30 Codex P0 | Plan 04 Wave 1 |
| 20 | test_local_namespace_no_corrections_leak.py | REQ-9 | Plan 04 Wave 1 |
| 21 | test_web_library_options_no_local.py | D-46 | Plan 09 Wave 4 |
| 22 | test_export_dossier_local_handling.py | D-45 | Plan 09 Wave 4 |
| 23 | test_local_schema_evolution.py | D-35 | Plan 03 Wave 1 |
| 24 | test_local_pyinstaller_smoke.py | D-43 (@pytest.mark.packaging) | Plan 09 Wave 4 |
| 25 | test_local_ceiling_enforcement.py | REQ-10 + D-26 + D-41 | Plan 07 Wave 3 |
| 26 | test_my_library_tab.py | REQ-7 + REQ-8 | Plans 07+08 |

## requirements.txt Change

Added one line after `python-docx==1.2.0`:
```
pymupdf>=1.24,<2.0  # Phase 95 D-43 — PDF extraction (Hebrew RTL)
```
`python-docx==1.2.0` exact pin unchanged. Installed version: `fitz 1.27.2.3`.

## GenizahSearchPro.spec Change

Two modifications to the pre-existing (gitignored) spec:
1. `hiddenimports` extended: `['tantivy', 'numpy', 'PIL', 'fitz', 'pymupdf']`
2. `collect_all('pymupdf')` block added after `collect_all('tantivy')` — prevents `ModuleNotFoundError: fitz._fitz` at runtime (RESEARCH.md Pitfall #5).

The file is gitignored (`*.spec` in `.gitignore`); force-added to worktree with `git add -f`.

## Fixture Decisions (D-44)

Hebrew PDF fixture was pre-committed on the parent branch (commit 851d6dde) by the user:
- `tests/fixtures/local_indexer/hebrew_sample.pdf` — 6.3 MB, Word-authored Hebrew PDF
- `tests/fixtures/local_indexer/hebrew_sample.expected.txt` — 282 KB expected reading-order reference

These were copied from the main checkout to the worktree.

## VALIDATION.md State

- `nyquist_compliant: true` — was already set by planner pre-execution; confirmed unchanged
- `wave_0_complete: false` — correctly deferred to Plan 09 closeout (LOW-1 review fix)
- Wave-0 Requirements: all 26+2 items checked off

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing functionality] Added `packaging` pytest marker to pyproject.toml**
- **Found during:** Task 6
- **Issue:** `@pytest.mark.packaging` in test_local_pyinstaller_smoke.py triggered `PytestUnknownMarkWarning` and would fail `--strict-markers` CI
- **Fix:** Registered `packaging` marker in `pyproject.toml` `[tool.pytest.ini_options]` alongside existing `slow` and `e2e` markers
- **Files modified:** pyproject.toml
- **Commit:** c34f4696

**2. [Rule 1 - Bug] Removed unused imports from stub files after ruff --fix**
- **Found during:** Task 6 ruff check
- **Issue:** `import pytest` and fixture imports were flagged F401 in stubs where tests only raise NotImplementedError (no pytest.mark.* or pytest.skip calls)
- **Fix:** `ruff --fix` auto-removed 25 unused imports; added `# noqa: F401` to 2 intentional try/except imports (`is_local_sys_id`, `LocalIndexer`) that exist to trigger ImportError for module-level skip
- **Files modified:** 21 stub files
- **Commit:** c34f4696

**3. [Rule 3 - Blocking] GenizahSearchPro.spec not in git history**
- **Found during:** Task 3
- **Issue:** `*.spec` is in `.gitignore`; the spec file only existed in the main checkout, not tracked in git
- **Fix:** Read spec from main checkout at `/c/Genizahsearch/GenizahSearchPro.spec`, created updated version in worktree, force-added with `git add -f`
- **Files modified:** GenizahSearchPro.spec
- **Commit:** fd876bc6

## Known Stubs

All 26 test files are intentional stubs — they are the Wave-0 placeholder structure per the Nyquist contract. No stubs prevent this plan's goal (establishing verification infrastructure). Each stub cites its implementing plan.

## Threat Flags

No new network endpoints, auth paths, file access patterns, or schema changes introduced. The Hebrew PDF fixture (`hebrew_sample.pdf`) was reviewed: it is a Word-authored document with no personal data (T-95-01 mitigated).

## Self-Check: PASSED

| Check | Result |
|-------|--------|
| 26 stub files exist | PASSED (26 files, 37 test functions) |
| hebrew_sample.pdf exists | PASSED |
| hebrew_sample.expected.txt exists | PASSED |
| local_sys_id_fixtures.py exists | PASSED |
| pymupdf pin in requirements.txt | PASSED (`pymupdf>=1.24,<2.0`) |
| GenizahSearchPro.spec has collect_all('pymupdf') | PASSED |
| 3 conftest fixtures discoverable | PASSED |
| VALIDATION.md nyquist_compliant: true | PASSED |
| VALIDATION.md wave_0_complete: false | PASSED |
| Commits fd2ede05, fd876bc6, a56d46c5, d4686650, c34f4696, d18db6ca | ALL FOUND |
