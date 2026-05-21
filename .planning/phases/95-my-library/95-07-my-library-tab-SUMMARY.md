---
phase: 95-my-library
plan: 07
subsystem: ui
tags: [pyqt6, qthread, qmutex, local-indexer, desktop, tantivy]

# Dependency graph
requires:
  - phase: 95-03
    provides: LocalIndexer Qt-free indexer with scan_all, prescan_count, prescan_count_all, add_folder, remove_folder, startup_recovery
  - phase: 95-05
    provides: SearchEngine.reload_local_indexes() — live-session reload method wired here as HIGH-1 call sites
  - phase: 95-06
    provides: rebuild_local_lab_index — LAB side-index rebuild whose completion triggers HIGH-1 call site 3
provides:
  - MyLibraryTab(QWidget) — 7th desktop tab with multi-folder management, progress bar, per-file status table
  - LocalIndexerWorker(QThread) — Qt thread wrapper with cooperative cancellation (D-24) and intra-file cancel
  - QMutex serialization of all indexer mutations (D-25), FIFO queue max depth 1
  - W8: two ceiling-check entry points (_check_ceiling_single_folder for Add, _check_ceiling_refresh_aggregate for Refresh)
  - HIGH-1: four reload_local_indexes() call sites (worker finished, remove folder, rebuild LAB, startup recovery)
  - D-40: unavailable folder orange highlight + tooltip, previously-indexed files remain searchable
  - B2: always-visible Refresh completion feedback (status bar message with re-indexed/skipped counts)
affects: [95-08, 95-09, genizah_app]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - QThread worker with cooperative cancel flag threaded into scan_all cancel_check lambda (D-24)
    - QMutex.tryLock() with FIFO queue max depth 1 for mutation serialization (D-25)
    - Deferred property pattern for search_engine — reads parent.searcher at call time, None-safe
    - Status bar toast via parent.statusBar().showMessage() with fallback to logger
    - Always-fire completion feedback in _on_worker_finished (B2 fix)

key-files:
  created:
    - desktop/my_library_tab.py
  modified:
    - genizah_app.py
    - tests/test_my_library_tab.py

key-decisions:
  - "W8 RESOLVED: two distinct ceiling entry points — single-folder for Add Folder, aggregate for Refresh; aggregate excludes unavailable folders per D-40"
  - "HIGH-1 four reload call sites: _on_worker_finished, _on_remove_folder_clicked, _on_rebuild_lab_completed, _on_startup_recovery_completed"
  - "D-40 deferred property pattern: search_engine reads parent.searcher at call time (not stored at __init__ time) to handle lazy init"
  - "SQLite threading fix (deviation): per-thread connections via threading.local() to eliminate cross-thread ProgrammingError when worker thread accesses indexer"
  - "B2 feedback: always call _show_status_message in _on_worker_finished regardless of whether any files were indexed; progress bar briefly hits 100% before hiding"
  - "UI for where to search (LOCAL filter button on search/composition/parallels tabs) is deferred-by-design to Plan 95-08 (Wave 4)"

patterns-established:
  - "QThread cooperative cancel: worker sets _cancel_requested flag; indexer scan_all accepts cancel_check=lambda: self._cancel_requested and checks between files and between PDF pages/DOCX chunks"
  - "QMutex FIFO queue: tryLock(); if held, store pending action as lambda in _queued_action (collapsing duplicates); process queued action at end of _on_worker_finished"
  - "Reload wiring pattern: call search_engine.reload_local_indexes() after every side-index commit, before any user notification"

requirements-completed: [REQ-7, REQ-8, REQ-10]

# Metrics
duration: multi-session (Tasks 1+2 first session, threading fix second session, B2 patch third session)
completed: 2026-05-21
---

# Phase 95 Plan 07: My Library Tab Summary

**MyLibraryTab (QWidget) desktop UI with QThread worker, QMutex mutation gating, W8 aggregate ceiling checks, HIGH-1 live-reload wiring at four call sites, D-40 unavailable folder UI, and B2 Refresh completion feedback**

## Performance

- **Duration:** Multi-session (Tasks 1+2 + threading deviation fix + B2 feedback patch)
- **Started:** 2026-05-21
- **Completed:** 2026-05-21
- **Tasks:** 3 (Task 1 tab+worker, Task 2 registration, Task 3 manual smoke) + 2 deviation fixes
- **Files modified:** 3 (desktop/my_library_tab.py created, genizah_app.py modified, tests/test_my_library_tab.py created)

## Accomplishments

- `MyLibraryTab(QWidget)` shipped as the 7th tab in the desktop QTabWidget (Pitfall #4 respected)
- `LocalIndexerWorker(QThread)` wraps the Qt-free `LocalIndexer.scan_all()` with per-file and intra-file cooperative cancellation (D-24 Codex revision)
- QMutex gates all indexer mutations with FIFO queue max depth 1 (D-25 Codex revision)
- W8 RESOLVED: two ceiling-check entry points — `_check_ceiling_single_folder` for Add Folder (single-folder pre-scan), `_check_ceiling_refresh_aggregate` for Refresh (aggregate across all registered available folders per D-16)
- HIGH-1 RESOLVED: four `reload_local_indexes()` call sites wired so newly indexed / deleted / rebuilt files are live-searchable in the same session without restart
- D-40: unavailable folders shown with orange color (#f39c12) + tooltip; previously-indexed files remain searchable; excluded from Refresh aggregate
- B2 RESOLVED: `_on_worker_finished` always calls `_show_status_message` with a "Refresh complete — N re-indexed, M up to date" summary, eliminating the zero-work silent case the user reported

## Task Commits

1. **Task 1+2: MyLibraryTab + LocalIndexerWorker + genizah_app.py registration** - `50266370` (feat)
2. **Deviation: SQLite threading fix — per-thread connections** - `d10159bd` (fix)
   - Merged via worktree: `b2dbeaf9` (chore: merge)
3. **B2 feedback patch: show status message on every Refresh completion** - `18725da9` (fix)

## Files Created/Modified

- `desktop/my_library_tab.py` — MyLibraryTab(QWidget) + LocalIndexerWorker(QThread), 762 lines
- `genizah_app.py` — 3 lines added: import + instantiation + addTab for 7th tab
- `tests/test_my_library_tab.py` — 11 tests covering widget structure, HIGH-1 reload wiring at all 4 call sites, B2 feedback contract

## Smoke Test Results (Task 3)

All 6 smoke-test sections passed:

| Check | Result |
|-------|--------|
| A) Tab presence — 7th tab "My Library" visible | PASS |
| B) Add Folder + first scan — folder picker, progress bar, per-file table | PASS (after threading fix) |
| B2) Refresh completion feedback — status message visible even zero-work | PASS (after B2 patch) |
| C) Remove Folder — files removed from index immediately | PASS |
| D) Search picks up LOCAL hits after Refresh | PASS |
| E) Unavailable folder orange highlight + previous files searchable | PASS |
| F) Ceiling dialog on > threshold (aggregate for Refresh) | PASS |

## Decisions Made

- **W8 ceiling scope locked:** Add Folder uses single-folder `prescan_count(path)`; Refresh uses aggregate `prescan_count_all()` across all registered + available folders. Unavailable folders contribute 0 to aggregate. This is per-trigger (D-26/D-41), not per-folder.
- **HIGH-1 deferred property:** `search_engine` reads `parent.searcher` at call time rather than storing at `__init__` time, since `GenizahGUI` assigns `self.searcher` asynchronously in `on_startup_finished()`. None-safe with `getattr` fallback.
- **B2 lightest touch:** Always call `_show_status_message` unconditionally in `_on_worker_finished`. Progress bar briefly hits 100% before hiding. No `QMessageBox` modal (non-blocking). Two dedicated tests pin the feedback contract.
- **"UI for where to search" deferred by design:** The user noted that there is still no UI showing where searches look (LOCAL vs corpus). This is explicitly addressed by Plan 95-08 (Wave 4) — the three-state LOCAL filter button on Search / Composition / Parallels tabs. This is out of scope for Plan 95-07.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] SQLite cross-thread ProgrammingError when worker accesses LocalIndexer**
- **Found during:** Task 1 (first smoke test — Add Folder + scan, check B)**
- **Issue:** `LocalIndexer` opened one SQLite connection at `__init__` time on the main thread, then `LocalIndexerWorker.run()` called `scan_all()` which committed on the worker thread. SQLite3 raised `ProgrammingError: SQLite objects created in a thread can only be used in that same thread`
- **Fix:** Changed `LocalIndexer._sqlite_conn` to `threading.local()` storage; each thread opens its own connection lazily via `_get_conn()`. Connections are closed on thread exit.
- **Files modified:** `shared/local_indexer.py`
- **Commit:** `d10159bd`

**2. [Rule 2 - Missing Critical] No visible feedback on Refresh completion in zero-work case**
- **Found during:** Task 3 manual smoke (check B2 — Refresh on already-indexed folder)**
- **Issue:** `_on_worker_finished` only called `_show_status_message` when `toast=True` (startup auto-rescan). Manual Refresh (`toast=False`) gave no feedback at all when no files needed re-indexing — status table was empty, progress bar disappeared silently. User reported: "does not provide indication that it did something (not showing again the list)"
- **Fix:** `_on_worker_finished` now always calls `_show_status_message` with "Refresh complete — N re-indexed, M up to date" (or "Refresh cancelled" if cancelled). Progress bar briefly hits 100% before hiding. Two regression tests added.
- **Files modified:** `desktop/my_library_tab.py`, `tests/test_my_library_tab.py`
- **Commit:** `18725da9`

---

**Total deviations:** 2 auto-fixed (1 bug, 1 missing critical)
**Impact on plan:** Both fixes were necessary for correctness and user experience. No scope creep.

## Issues Encountered

- Threading issue surfaced only during live smoke test (not caught by automated tests because test suite mocks the indexer and does not call `scan_all()` from a real QThread). The fix is correct: per-thread SQLite connections via `threading.local()`.
- B2 gap was identified via user smoke test feedback, not automated tests. The fix is minimal and the regression tests added in `18725da9` will catch any future regression.

## Known Stubs

None. The tab is fully wired to the real `LocalIndexer` and shows real per-file results.

## Threat Flags

None beyond what was in the plan's `<threat_model>`.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- Plan 95-08 (Wave 4): Result badge + LOCAL filter button on Search / Composition / Parallels tabs. `MyLibraryTab` is complete and `reload_local_indexes()` is wired. The "UI for where to search" observation from the user smoke test is the primary driver for 95-08.
- Plan 95-09: Docs, export web guard, PyInstaller packaging. `desktop/my_library_tab.py` needs to be included in the spec file — `GenizahSearchPro.spec` already exists (created in Phase 95-01).

## Self-Check

- [x] `desktop/my_library_tab.py` exists
- [x] `tests/test_my_library_tab.py` exists with 11 tests
- [x] Commit `50266370` exists (Tasks 1+2)
- [x] Commit `d10159bd` exists (threading fix)
- [x] Commit `18725da9` exists (B2 feedback patch)
- [x] All 38 tests in test_my_library_tab + test_local_indexer* pass

## Self-Check: PASSED

---
*Phase: 95-my-library*
*Completed: 2026-05-21*
