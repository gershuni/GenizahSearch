---
phase: 97-more-local-features
plan: "04"
subsystem: desktop
tags: [sqlite, tantivy, pyqt6, disk-indicator, folder-counters, prescan-worker, ceiling-lift, desktop]

# Dependency graph
requires:
  - phase: 97-more-local-features/97-01
    provides: SQLite migration ladder (user_version=2), folders.indexed_count/error_count/
              pending_count/oversized_count/last_aggregate_at columns (Wave A D-NEW-1),
              _commit_batch durability bracket (synchronous=FULL + BEGIN IMMEDIATE + ROLLBACK)
  - phase: 97-more-local-features/97-02
    provides: _upsert_local_files_status() LD-9 dual-write, local_files.extraction_status
              populated for oversized/zip_bomb_suspected files, _CommitTriggers
  - phase: 97-more-local-features/97-03
    provides: _SUPPORTED_EXTENSIONS extended to .html/.xlsx/.csv (folder counters must
              aggregate ALL formats — this wave relies on accurate extraction_status values
              for all 6 supported extensions)

provides:
  - _MAX_FILES_CEILING=50_000 / _MAX_BYTES_CEILING=50 GB (was Phase 95 5K/2GB hard stop)
  - PrescanWorker QThread: runs prescan_count() off the UI thread with QProgressDialog + Cancel
  - prescan_count(cancel_check=) kwarg for cooperative cancellation from PrescanWorker
  - estimate_index_size(): sum of all file sizes under index dir (Tantivy segments + SQLite sidecar)
  - _refresh_folder_counters_for(): LD-9 folder counter UPDATE inside _commit_batch transaction
  - MyLibraryTab._update_disk_indicator(): live disk label with 2x merge-headroom warning
  - _disk_label QLabel + 30s QTimer in MyLibraryTab._build_ui

affects: [97-05, 97-06, local_indexer, my_library_tab]

# Tech tracking
tech-stack:
  added: []  # no new deps; shutil already stdlib; QProgressDialog already in PyQt6
  patterns:
    - "PrescanWorker QThread pattern: QProgressDialog(max=0) indeterminate + Cancel + signal-driven continuation"
    - "C-06 disk headroom rule: (free - 2*index_size) < 1 GB threshold fires warning label"
    - "LD-9 folder counter query: local_files.folder_id JOIN (NOT filepath LIKE prefix) inside BEGIN IMMEDIATE transaction"
    - "Cancel-check kwarg threading: prescan_count(cancel_check=lambda: self._cancel) enables mid-walk abort"

key-files:
  created:
    - tests/test_disk_headroom.py
  modified:
    - desktop/my_library_tab.py
    - shared/local_indexer.py
    - tests/test_local_indexer_migrations.py

key-decisions:
  - "PrescanWorker uses QProgressDialog.exec() event loop block (NOT QThread.wait) so Cancel button actually works while the walk runs"
  - "estimate_index_size() walks self._index_dir only (not lab_index_dir) — the merge-headroom concern is for the main LOCAL index where Tantivy segment merges happen"
  - "_refresh_folder_counters_for called INSIDE the BEGIN IMMEDIATE transaction of _commit_batch for atomicity — counter update and status=committed UPDATE are one atomic unit"
  - "indexed_count query uses extraction_status IN ('ok', 'committed') — 'committed' never appears in local_files (only in processed_files) but is listed per LD-9 spec; harmless and future-proof"
  - "Rule-1 fix: removed local 'from shared.local_sys_id import _canonical_filepath' inside _scan_all_impl — Python hoists local variable bindings to function scope top, causing UnboundLocalError at the earlier oversized/zip-bomb usage of the same name"

patterns-established:
  - "Worker-thread pre-scan: QThread emits finished_signal(int, int) -> UI slot continues ceiling check; error_signal(str) -> abort; QProgressDialog.canceled -> worker.cancel()"
  - "Disk indicator: _update_disk_indicator() called from showEvent + _on_worker_finished + 30s QTimer — three trigger sites for freshness"

requirements-completed: [C-01, C-03, C-04, C-06]

# Metrics
duration: ~60min
completed: 2026-05-25
---

# Phase 97 Plan 04: Wave D — Capacity Limits + Folder Walk Worker + Disk Indicator Summary

**50K/50GB soft-warning ceiling, prescan QThread with QProgressDialog Cancel, LD-9 folder counter aggregation via local_files.folder_id inside _commit_batch transaction, and live disk indicator with 2x Tantivy merge-headroom warning.**

## Performance

- **Duration:** ~60 min
- **Started:** 2026-05-25T12:40:41Z
- **Completed:** 2026-05-25T13:40:00Z (approximate)
- **Tasks:** 2 (TDD: RED stubs -> GREEN implementation)
- **Files modified:** 4

## Accomplishments

### C-01: Ceiling Lift (5K/2GB -> 50K/50GB)

- `_MAX_FILES_CEILING = 50_000` / `_MAX_BYTES_CEILING = 50 * 1024 ** 3` in `desktop/my_library_tab.py`
- The existing `_show_ceiling_confirm_dialog` (Yes / Cancel) is unchanged — it was already a soft prompt, not a hard stop
- Codex P0 sequencing respected: constant only lifted after Wave A (R-04 durability bracket, D-NEW-1 migration) and Wave B (C-05 oversized/zip-bomb limits) are in place

### C-03: PrescanWorker QThread

- New `PrescanWorker(QThread)` class with `finished_signal(int, int)` + `error_signal(str)`
- `prescan_count()` extended with `cancel_check: Optional[Callable[[], bool]] = None` kwarg; returns `(-1, -1)` on cancel
- `_check_ceiling_single_folder()` now spawns a `PrescanWorker` + shows `QProgressDialog(max=0)` (indeterminate spinner); Cancel button wires to `worker.cancel()`
- UI thread blocks via `progress_dlg.exec()` event loop (not a raw thread join), keeping the Cancel button responsive

### C-04 + LD-9: Persisted Folder Counters

- `_refresh_folder_counters_for(filepaths)` added to `LocalIndexer`
- Aggregates `local_files.extraction_status` via `folder_id` JOIN — NOT via `filepath LIKE folder_path || '%'` (Codex MEDIUM #3 fix: LIKE-prefix would match C:\\foo AND C:\\foobar)
- Counter categories:
  - `indexed_count`: extraction_status IN ('ok', 'committed')
  - `error_count`: extraction_status IN ('error', 'encoding_error', 'changed_during_index', 'no_text_layer')
  - `pending_count`: extraction_status = 'pending'
  - `oversized_count`: extraction_status IN ('oversized', 'zip_bomb_suspected')
- Called from `_commit_batch` **inside** the `BEGIN IMMEDIATE` / `COMMIT` transaction (same block as the `status='committed'` UPDATE) for atomicity
- Wave F extraction_status codes ('unreachable', 'timeout') listed now — COUNT() over the defined set means new codes default to uncounted until the query is extended (acceptable for Phase 97 v1)

### C-06: Disk Indicator with Merge Headroom

- `estimate_index_size()` added to `LocalIndexer`: walks `self._index_dir` recursively, sums file sizes
- `_update_disk_indicator()` added to `MyLibraryTab`:
  - Reads `shutil.disk_usage(Config.LOCAL_INDEX_DIR)` + `estimate_index_size()`
  - Headroom = free - 2 × index_size (2x reserves Tantivy segment-merge scratch)
  - Warning label fires when headroom < 1 GB: "⚠ low merge headroom"
- `_disk_label QLabel` added to `_build_ui()` (Section 4 after unified tree)
- 30s `QTimer` in `_build_ui()` refreshes indicator while tab is visible
- Also wired to `showEvent()` (tab becomes visible) and `_on_worker_finished()` (after indexing batch)

## Task Commits

1. **Task 1: Wave 0 RED test stubs** - `7074587a` (test)
2. **Task 2: Wave D GREEN implementation** - `a1a0a757` (feat)

## Files Created/Modified

- `tests/test_disk_headroom.py` — 2 tests: test_warning_below_threshold, test_no_warning_above_threshold (NEW)
- `tests/test_local_indexer_migrations.py` — 2 new tests appended: test_folder_counters_updated_in_scan_all, test_folder_counter_aggregation_uses_folder_id_not_like_prefix
- `desktop/my_library_tab.py` — ceiling constants bumped; PrescanWorker class; _check_ceiling_single_folder uses worker; _disk_label + _disk_timer in _build_ui; _update_disk_indicator + showEvent; shutil + QProgressDialog imports
- `shared/local_indexer.py` — prescan_count cancel_check kwarg; estimate_index_size(); _refresh_folder_counters_for(); _commit_batch extended to call _refresh_folder_counters_for inside transaction; Rule-1 fix for UnboundLocalError

## Wave F Dependencies

The `_refresh_folder_counters_for` counter SQL lists these extraction_status codes explicitly:
- `'ok'`, `'committed'` → indexed_count
- `'error'`, `'encoding_error'`, `'changed_during_index'`, `'no_text_layer'` → error_count
- `'pending'` → pending_count
- `'oversized'`, `'zip_bomb_suspected'` → oversized_count

Wave F will introduce `'unreachable'`, `'timeout'` (D-NEW-2 network drive semantics). These are listed in the counter SQL now to prevent counting when they appear — Wave F will reclassify them into the appropriate category (likely error_count) or add a separate column.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `UnboundLocalError` for `_canonical_filepath` in `_scan_all_impl`**
- **Found during:** Task 2 (test_folder_counters_updated_in_scan_all first run)
- **Issue:** A local `from shared.local_sys_id import _canonical_filepath` import at line ~1498 inside `_scan_all_impl` caused Python to hoist `_canonical_filepath` as a local variable binding for the entire function. Any earlier reference to `_canonical_filepath` (e.g., at the oversized-file branch ~line 1419) raised `UnboundLocalError` before the import was executed.
- **Fix:** Removed the local import — `_canonical_filepath` is already imported at module level (line 71 of local_indexer.py). The local import was added in Phase 96 fix-7 as a defensive pattern but was incorrect.
- **Files modified:** `shared/local_indexer.py`
- **Committed in:** `a1a0a757` (Task 2)

**2. [Rule 1 - Bug] Test queries used `str(folder)` but indexer stores canonical (lowercased) path**
- **Found during:** Task 2 (test_folder_counters_updated_in_scan_all assertion)
- **Issue:** On Windows, `_canonical_filepath()` lowercases paths. `str(tmp_path / "docs")` returns mixed-case (e.g., `C:\Users\gersh\...`), but the indexer stored `c:\users\gersh\...`. The SQLite query with `WHERE path = ?` had an exact string mismatch.
- **Fix:** Updated both new tests to use `_canonical_filepath(str(folder))` for the query key.
- **Files modified:** `tests/test_local_indexer_migrations.py`
- **Committed in:** `a1a0a757` (Task 2)

---

**Total deviations:** 2 auto-fixed (both Rule 1 bugs)
**Impact:** Both fixes essential for test correctness. No scope creep — fixes were within the task's files.

## Known Stubs

None — all Wave D functionality fully implemented. The `_refresh_folder_counters_for` SQL lists Wave F status codes (`'unreachable'`, `'timeout'`) in comments/docstring but they are NOT included in the COUNT() sets (correct — Wave F will add them when the codes are populated).

## Threat Flags

No new network endpoints, auth paths, or cross-trust-boundary surfaces introduced. All changes are:
- Local filesystem walks (prescan_count, estimate_index_size)
- SQLite aggregate queries (local_files.folder_id JOIN, folder counter UPDATE)
- Qt widget updates (disk label, progress dialog)

T-97D-01 through T-97D-06 mitigations implemented:
- T-97D-01 (UI DoS from 50K os.walk): mitigated — PrescanWorker + QProgressDialog Cancel
- T-97D-02 (folder counter drift on crash): mitigated — _refresh_folder_counters_for inside BEGIN IMMEDIATE with _commit_batch
- T-97D-03 (disk exhaustion): mitigated — _update_disk_indicator with 2x merge-headroom reservation + 1 GB safety margin
- T-97D-06 (LIKE-prefix cross-contamination): mitigated — folder_id JOIN + test_folder_counter_aggregation_uses_folder_id_not_like_prefix pins behavior

## Self-Check: PASSED

- `tests/test_disk_headroom.py` FOUND (2 tests)
- `tests/test_local_indexer_migrations.py` FOUND (8 tests: 6 Wave A + 2 Wave D)
- `desktop/my_library_tab.py` FOUND (contains PrescanWorker, _MAX_FILES_CEILING=50_000, _update_disk_indicator, _disk_label, low merge headroom)
- `shared/local_indexer.py` FOUND (contains estimate_index_size, _refresh_folder_counters_for, prescan_count cancel_check)
- Task 1 commit `7074587a` FOUND
- Task 2 commit `a1a0a757` FOUND
- 10 target tests (test_disk_headroom.py + test_local_indexer_migrations.py): 10/10 PASSED
- 33 Phase 97 tests (Waves A+B+C+D): 33 PASSED
- ruff check on modified files: CLEAN
- check_plan_artifacts.py: EXIT 0

---
*Phase: 97-more-local-features*
*Completed: 2026-05-25*
