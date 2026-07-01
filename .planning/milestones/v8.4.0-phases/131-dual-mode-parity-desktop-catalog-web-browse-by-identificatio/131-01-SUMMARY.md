---
phase: 131-dual-mode-parity-desktop-catalog-web-browse-by-identificatio
plan: "01"
subsystem: testing

tags: [library-filter, dual-mode, pytest, desktop, catalog, parallels, wave-0, tdd]

requires:
  - phase: 130-dual-mode-filter-core-web-search
    provides: "(mode + set) shape, pure-mirror + AST-scan test pattern, pluralized button keys"

provides:
  - "Wave-0 test scaffolds for all 3 Phase 131 surfaces: desktop dialog (DMF-07/DMF-13), catalog (DMF-08/DMF-12/DMF-10), parallels (DMF-09/DMF-10)"
  - "Revised desktop tests carrying mode='show_only' + library_codes_with_manuscripts() universe (Codex R1 BLOCKER #1)"
  - "Stale handoff test fixed + Hide-mode handoff coverage added (Codex N3)"

affects: [131-02, 131-03, 131-04, 131-05]

tech-stack:
  added: []
  patterns:
    - "Pure-mirror helpers (no production import) + AST source-contract scans — the Phase 130 Wave-0 pattern applied to 3 new surfaces"
    - "RED assertions for source-contract scans (not pytest.skip) — wave-0 state, RED until surface plans land"

key-files:
  created:
    - tests/test_catalog_dual_mode_library_filter.py
    - tests/test_parallels_library_filter.py
  modified:
    - tests/test_libfilter_desktop.py

key-decisions:
  - "Existing inclusion-only desktop tests revised IN PLACE with mode='show_only' (not append-only), Codex R1 BLOCKER #1"
  - "Universe expectation updated to set(library_codes_with_manuscripts())-{'LOCAL'} rather than LIBRARY_CODES-LOCAL (DMF-13)"
  - "Stale handoff test _catalog_build_browse_filters_includes_library sets _catalog_library_mode='show_only' (Codex N3)"
  - "Hide-mode handoff test asserts no library filter passed + no resolve_library_sys_ids call + notice fires (Codex HIGH #4)"
  - "Source-contract scans use real failing assertions, not pytest.skip (intended RED until Plan 03/04/05)"

patterns-established:
  - "Wave-0 test scaffold: pure-mirror helpers (self-contained, always green) + AST source-contract scans (RED until production lands)"
  - "Per-surface test file isolation: test_catalog_dual_mode_library_filter.py / test_parallels_library_filter.py / test_libfilter_desktop.py"

requirements-completed: [DMF-07, DMF-08, DMF-09, DMF-10, DMF-12, DMF-13]

duration: ~35min
completed: 2026-06-30
---

# Phase 131 Plan 01: Dual-Mode Wave-0 Test Scaffolds Summary

**Wave-0 pure-mirror + AST-scan test scaffolds for 3 parity surfaces (desktop catalog dialog, web catalog Browse, web /parallels) with stale handoff test fixed and Hide-mode coverage added**

## Performance

- **Duration:** ~35 min (resume of prior interrupted executor)
- **Started:** 2026-06-30 (prior executor tasks 1+2 committed; this executor completed task 3)
- **Completed:** 2026-06-30
- **Tasks:** 3
- **Files modified:** 3

## Accomplishments

- Created `tests/test_catalog_dual_mode_library_filter.py` — 15 tests covering DMF-08/DMF-12/DMF-10 with pure-mirror behavior tests (always green) and AST source-contract scans against `web/pages/catalog_browse.py` (RED until Plan 04)
- Created `tests/test_parallels_library_filter.py` — 16 tests covering DMF-09/DMF-10 with pure-mirror behavior tests (always green) and AST source-contract scans against `web/pages/parallels.py` (RED until Plan 05)
- Revised `tests/test_libfilter_desktop.py` IN PLACE: 4 inclusion-only dialog tests now carry `mode='show_only'`; universe updated to `library_codes_with_manuscripts()-{'LOCAL'}`; stale handoff test fixed (sets `_catalog_library_mode='show_only'`); 6 new DMF-07/DMF-13 tests + 2 new Hide-mode handoff tests added (Codex N3)

## Task Commits

1. **Task 1: Create tests/test_catalog_dual_mode_library_filter.py** - `5a42b896` (test)
2. **Task 2: Create tests/test_parallels_library_filter.py** - `da6d9ae2` (test)
3. **Task 3: Revise tests/test_libfilter_desktop.py + cleanup** - `d016087f` (test)

## Files Created/Modified

- `tests/test_catalog_dual_mode_library_filter.py` — New: 15 tests for catalog surface; pure-mirror helpers (migration/apply/shortlist) + AST scans (restore branches, dict-persist, LOCAL guard, fjms.get_browse_library_facets( instance call, pluralized button keys)
- `tests/test_parallels_library_filter.py` — New: 16 tests for parallels surface; pure-mirror helpers + AST scans (ParallelsState fields, restore, LOCAL helper, no-search-import, export-before-storage ordering, pluralized button keys)
- `tests/test_libfilter_desktop.py` — Revised + extended: 4 inclusion-only tests carry `mode='show_only'`; 6 new dual-mode tests; 1 revised + 2 new handoff tests; removed unused imports from catalog/parallels files

## Decisions Made

- Revised existing tests IN PLACE rather than append-only, because the plan explicitly required it (BLOCKER: Plan 03 will break the old tests unless they carry `mode='show_only'`)
- Added `_catalog_library_mode='show_only'` to the stale handoff MagicMock so Plan 03's Show-only gate doesn't fail the test for the wrong reason (Codex N3 directive)
- Cleaned up unused `import re` and `sanitize_library_codes` from the two already-committed files to keep ruff clean

## Deviations from Plan

None — plan executed exactly as written. The minor cleanup (removing two unused imports from tasks 1+2's committed files) was bundled into the task 3 commit since the files were already open for modification in the working tree.

## Issues Encountered

The prior executor was interrupted before completing task 3. The working tree had the complete task 3 changes already written but uncommitted. This executor verified all files parse cleanly, ran the targeted pure-mirror tests to confirm they pass, ran ruff to confirm clean linting, then committed.

## Known Stubs

None — this plan creates test scaffolds only. The RED AST source-contract scans are intentional Wave-0 state (they scan for production code not yet written; they will go green as Plans 02-05 land).

## Next Phase Readiness

- All 3 Wave-0 test files are in place; every surface plan (Plans 02-05) has a concrete `<automated>` verify command to satisfy the Nyquist rule
- Plan 02 (web catalog `get_browse_library_facets` service method) can proceed immediately
- Plan 03 (desktop dialog dual-mode + Show-only-gated handoff) will make the RED desktop tests go green
- Plans 04/05 (web catalog/parallels production code) will make the RED AST scans go green

---
*Phase: 131-dual-mode-parity-desktop-catalog-web-browse-by-identificatio*
*Completed: 2026-06-30*
