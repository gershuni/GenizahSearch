---
phase: 96
plan: "01"
subsystem: my-library-desktop
tags: [phase-96, my-library, wave-0, fixtures, tdd-skeletons, codex-high-absorption]
dependency_graph:
  requires: []
  provides:
    - tests/fixtures/local_indexer/single_word_per_line.pdf
    - tests/test_local_pdf_extraction_fallback.py
    - tests/test_local_hit_highlighting.py
    - tests/test_local_optout_persistence.py
    - tests/test_local_optout_filter.py
    - tests/test_local_nav_page_chunk.py
    - tests/test_result_dialog_local_button_removed.py
    - tests/test_local_filter_cascade.py (extended)
    - tests/test_local_browse_panel.py (updated)
    - .planning/phases/96-completing-my-library-feature-add-features-and-fix-bugs/96-08-WIRING-NOTES.md
  affects:
    - Wave 1+ plans (96-02 through 96-08) reference fixtures and skeletons
tech_stack:
  added: []
  patterns:
    - xfail(strict=True) flip mechanism for NEW-1 incompatible tests
    - pytest.skip with explicit plan-ID citations per Phase 95 LOW-1 lesson
    - algebra-level regression guard for cross-folder opt-out set operations
    - AST-walker pattern extended to D-F1 cascade joinpoints
key_files:
  created:
    - tests/fixtures/local_indexer/single_word_per_line.pdf
    - scripts/generate_single_word_fixture.py
    - tests/test_local_pdf_extraction_fallback.py
    - tests/test_local_hit_highlighting.py
    - tests/test_local_optout_persistence.py
    - tests/test_local_optout_filter.py
    - tests/test_local_nav_page_chunk.py
    - tests/test_result_dialog_local_button_removed.py
    - .planning/phases/96-completing-my-library-feature-add-features-and-fix-bugs/96-08-WIRING-NOTES.md
  modified:
    - tests/test_local_filter_cascade.py
    - tests/test_local_browse_panel.py
decisions:
  - "Fixture validated against production heuristic (single_word_ratio >= 0.70) not PyMuPDF block-count internals — self-validating regardless of PyMuPDF version (checker BLOCKER 1 closure)"
  - "3 NEW-1-incompatible tests in test_local_browse_panel.py inverted with xfail(strict=True) + negated assertions — auto-flip to PASS when 96-07 removes btn_rd_open_browse"
  - "test_regex_non_match_filtered_out REPLACES old test_local_snippet_fallback_when_regex_no_match per D-04.1 reversal (Codex HIGH #2 closure)"
  - "test_folder_a_optout_survives_folder_b_toggle uses algebra-level test (not production code import) — passes in Wave 0 without production implementation"
  - "96-08-WIRING-NOTES.md is an authoritative Wave-0 reconnaissance artifact — plans 96-06 and 96-08 read it as source of truth, no hasattr discovery at Wave 3/4 runtime"
metrics:
  duration: "~20 minutes"
  completed: "2026-05-24"
  tasks_completed: 4
  tasks_total: 4
  files_created: 11
  files_modified: 2
---

# Phase 96 Plan 01: Wave-0 Foundations Summary

Wave-0 fixtures, test scaffolding, and reconnaissance notes for Phase 96 (My Library: completing features and fixing bugs). No production code — all production work deferred to Wave 1+.

## What Was Built

### Task 1: D-F4 Pathological PDF Regression Fixture

Created `tests/fixtures/local_indexer/single_word_per_line.pdf` — a synthetic PDF with one word per line generated via PyMuPDF `TextWriter` (independent content streams per word). Fixture was validated against the **production detection heuristic** (`single_word_ratio >= 0.70` over `>= 5` non-empty lines), NOT PyMuPDF block-count internals.

Validation result: `ratio=0.96` (27 lines, 26 single-word) — pathological under the heuristic.

Generator script `scripts/generate_single_word_fixture.py` is idempotent and reproducible. If PyMuPDF changes block-detection behavior in a future version, re-running the script against the updated validation command will immediately confirm whether the fixture is still valid.

**REVISION 2026-05-24 — checker BLOCKER 1 closure:** switched from `len(blocks) >= 20` to heuristic-based validation.

### Task 2: 6 New Skeleton Test Files

All 6 files are pytest-collectable with no SyntaxError or ImportError. Each carries `Implementation plan: 96-XX` in its module docstring.

| File | Plan | Wave-0 state |
|------|------|--------------|
| `test_local_pdf_extraction_fallback.py` | 96-02 | 3 SKIPPED |
| `test_local_hit_highlighting.py` | 96-03 | 2 PASSED + 4 SKIPPED |
| `test_local_optout_persistence.py` | 96-04/96-06 | 4 PASSED + 1 SKIPPED |
| `test_local_optout_filter.py` | 96-05 | 6 PASSED (stub logic) |
| `test_local_nav_page_chunk.py` | 96-03/96-08 | 4 SKIPPED |
| `test_result_dialog_local_button_removed.py` | 96-07 | 1 XFAIL + 1 PASSED |

**Cross-AI Review fold-in (REVISION 2026-05-24):**

- **Codex HIGH #1 closure:** `test_folder_a_optout_survives_folder_b_toggle` — algebra-level regression guard for cross-folder opt-out persistence. Tests the SET-DIFFERENCE/UNION algebra directly; PASSES immediately in Wave 0 without any production code dependency.
- **Codex HIGH #2 / D-04.1 LOAD-BEARING closure:** `test_regex_non_match_filtered_out` REPLACES `test_local_snippet_fallback_when_regex_no_match`. Under D-04.1, LOCAL hits where the regex does NOT match are FILTERED OUT (not displayed with fallback content[:200]). Old fallback-display semantics are explicitly reversed.
- **Codex MEDIUM #9 closure:** `test_canonical_filepath_windows_variants` — asserts `_canonical_filepath` normalizes mixed case + slash variants on Windows. SKIPPED on non-Windows CI runners; PASSED on this Windows 11 machine.
- **checker BLOCKER 4 closure:** `test_d_f5_integration_regex_arrives_at_build_local_result_dict` — integration spy test that catches the silent no-op failure mode (regex=None → fallback content[:200]).
- **W10 cascade-interaction additions:** 3 new tests in `test_local_optout_filter.py` covering `_local_filter_inactive_chip_visible` state transitions when opt-out composes with three-state LOCAL filter.

### Task 3: Extend Cascade AST Guard + Update Browse Panel Tests

**`tests/test_local_filter_cascade.py`** extended with:
- `test_optout_filter_applied_within_both_cascades` — SKIPPED until 96-05 lands `_apply_local_optout_filter` calls in both cascade joinpoints
- `test_apply_local_optout_filter_function_exists` — SKIPPED until 96-05 creates the method

Existing Phase 95 cascade tests (4) still PASS.

**`tests/test_local_browse_panel.py`** updated: the 3 NEW-1-incompatible tests at the ResultDialog section were inverted and decorated with `@pytest.mark.xfail(strict=True)`. Their assertions are negated (`not in`, `is None`, `not in`). The moment plan 96-07 removes `btn_rd_open_browse`, these tests flip from XFAIL to XPASSED, which `strict=True` converts to a failure — signaling to the executor to flip `xfail` off and turn them into permanent passing regression guards.

`grep -c "btn_rd_open_browse" tests/test_local_browse_panel.py` returns 3 (symbol name preserved in negated assertions). `grep -c "xfail(strict=True)" tests/test_local_browse_panel.py` returns 3.

**REVISION 2026-05-24 — checker BLOCKER 3 closure:** `test_yiyun_browse_button_still_present` uses a defensive `pytest.skip` fallback: if `btn_view_transcription` is renamed by a future phase, the test skips rather than self-blocking subsequent waves.

### Task 4: 96-08-WIRING-NOTES.md Reconnaissance Artifact

`96-08-WIRING-NOTES.md` (13,884 bytes) is an authoritative planning artifact pinning all load-bearing attribute names that plans 96-08 and 96-06 consume. Plans must read this file rather than performing `hasattr` discovery at runtime.

Key sections:
- **ResultDialog:** `self.text_ms`, `self.btn_compact_pg_prev`, `self.btn_compact_pg_next`, `self.spin_page`, `self.lbl_total`, `self.current_p_num`, `self.btn_view_transcription`
- **Browse panel:** `self.browse_text`, `apply_line_numbered_text`, `self.btn_prev_ms`, `self.btn_next_ms`; per-page nav buttons DO NOT EXIST yet — plan 96-08 must CREATE them
- **Language detection:** `CURRENT_LANG` (module-level global, NOT `self.lang`)
- **Session JSON nesting:** clarifies `local_filter` keys are NESTED in surface dicts; `local_file_optouts` goes TOP-LEVEL (cross-surface) — W6 closure
- **`### Plan 96-06 wiring` section** (REVISION 2026-05-24 — Codex MEDIUM #10 closure): pins `self._folder_list`, `currentItemChanged`, `Qt.ItemDataRole.UserRole`, `_on_indexer_finished`, `self._indexer`, `self._indexer_mutex`, `list_all_filepaths` (to be added — preferred over `_conn` direct access), `_canonical_filepath`

## Deviations from Plan

None — plan executed exactly as written.

## Test Results Summary

```
Full regression bundle:
  36 passed, 2 skipped, 3 xfailed (test_local_*.py + web_library + no_raw_storage_access)

New skeleton files:
  13 passed, 12 skipped, 1 xfailed
```

No tests transitioned from PASSED to FAILED.

## Self-Check: PASSED

All 11 files created/modified confirmed on disk. All 4 commits confirmed in git log.

| Commit | Message |
|--------|---------|
| 159e4cff | feat(96-01): add D-F4 pathological PDF regression fixture + generator |
| a17d4ca0 | feat(96-01): add 6 skeleton test files for Phase 96 Wave 1+ features |
| 58ea6612 | feat(96-01): extend cascade AST guard + invert browse panel NEW-1 tests |
| 60850cf6 | docs(96-01): produce 96-08-WIRING-NOTES.md — pinned attribute names for 96-08 + 96-06 |
