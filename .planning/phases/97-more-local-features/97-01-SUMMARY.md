---
phase: 97-more-local-features
plan: "01"
subsystem: database
tags: [sqlite, tantivy, zstandard, migration, atomic-rebuild, scan-runs, recovery-gate, desktop]

# Dependency graph
requires:
  - phase: 96-my-library-polish
    provides: LocalIndexer, MyLibraryTab, two-phase commit (D-21), Windows os-error-5 retry

provides:
  - Phase 97 final canonical Tantivy schema (scan_run_id + chunk_locator added to 11 Phase 95 fields)
  - SQLite migration ladder user_version 0->1->2 with idempotent ALTER TABLE + D-NEW-4 prune
  - zstd compressed cached_text in local_pages (single canonical write site _write_page_doc)
  - Atomic Tantivy index rebuild with 7-handle closure + Windows rename retry
  - _commit_batch durability bracket: PRAGMA synchronous=FULL + BEGIN IMMEDIATE + ROLLBACK
  - scan_runs lifecycle table replacing _pending_cleanup sentinel anti-pattern
  - is_searchable gate in _query_local_index via weakref from SearchEngine to MyLibraryTab
  - scripts/check_plan_artifacts.py (LD-12 forbidden-token static auditor)

affects: [97-02, 97-03, 97-04, 97-05, 97-06, local_indexer, my_library_tab, genizah_core]

# Tech tracking
tech-stack:
  added: [zstandard>=0.22 (zstd compression for cached_text)]
  patterns:
    - SQLite user_version migration ladder with BEGIN IMMEDIATE per step + idempotent _alter_safe
    - Atomic index rebuild via temp-dir swap with Windows-access-denied retry (250ms/1s/2s)
    - weakref from SearchEngine to MyLibraryTab for is_searchable gate (default-True when ref dead)
    - scan_runs lifecycle table (INSERT on begin, UPDATE on end/cancel) replaces sentinel file
    - _ConnWrapper Python shim for testing C-level sqlite3.Connection.execute interception

key-files:
  created:
    - shared/local_indexer_migrations.py
    - tests/test_local_indexer_migrations.py
    - tests/test_cached_text.py
    - tests/test_atomic_rebuild.py
    - tests/test_two_phase_durability.py
    - tests/test_recovery_gate.py
    - tests/fixtures/local_indexer/__init__.py
    - scripts/check_plan_artifacts.py
  modified:
    - shared/local_indexer.py
    - genizah_core.py
    - desktop/my_library_tab.py
    - genizah_app.py
    - requirements.txt
    - requirements-lock.txt
    - GenizahSearchPro.spec

key-decisions:
  - "scan_all refactored into wrapper + _scan_all_impl so lifecycle hooks wrap impl without changing callers"
  - "_ConnWrapper Python shim (not monkeypatch) for testing sqlite3.Connection.execute interception — C-level attribute is read-only"
  - "is_searchable gate wired in genizah_app.on_startup_finished (not __init__) because SearchEngine is not yet available during MyLibraryTab.__init__"
  - "fresh DB stamped at user_version=2 in init_sqlite (empty processed_files check) so migration ladder only runs on pre-existing Phase 95 DBs"
  - "LAB search gates added at both Parallels (line ~1592) and Composition (line ~8808) call sites"

patterns-established:
  - "Migration ladder: each step runs in BEGIN IMMEDIATE/COMMIT/ROLLBACK; _alter_safe swallows duplicate-column errors for idempotency"
  - "Atomic rebuild 7-step: build in .rebuild-<id>/ -> validate -> close 7 handles -> rename live->.old-<ts> -> rename rebuild->live -> reload -> record .old in pending_dir_cleanup"
  - "is_searchable weakref gate: default-True when ref dead ensures CLI/test contexts are never blocked"

requirements-completed: [LOCAL-SCALE-01, LOCAL-SCALE-02, LOCAL-SCALE-03, LOCAL-SCALE-04, LOCAL-SCALE-08]

# Metrics
duration: ~95min
completed: 2026-05-25
---

# Phase 97 Plan 01: Recovery Foundation Summary

**SQLite migration ladder (v0->v2), zstd cached_text, 7-handle atomic Tantivy rebuild, ROLLBACK durability bracket, scan_runs lifecycle, and is_searchable recovery gate — making My Library usable at 13K-file / 43 GB scale.**

## Performance

- **Duration:** ~95 min
- **Started:** 2026-05-25T08:00:00Z (approximate)
- **Completed:** 2026-05-25T09:30:00Z
- **Tasks:** 4 (all TDD: RED stubs -> GREEN implementation)
- **Files modified:** 15

## Accomplishments

- Final Phase 97 canonical Tantivy schema locked: 11 Phase 95 fields + `scan_run_id` (raw tokenizer, for Wave E delete-by-run) + `chunk_locator` (stored, for Wave F display)
- SQLite migration ladder `user_version` 0->1->2 with integrity_check gate (T-97A-01), idempotent `_alter_safe`, fresh-DB stamp bypass, and D-NEW-4 prune of unsupported-extension rows
- `_write_page_doc` single canonical write site now stores zstd-compressed `cached_text` + `chunk_locator` + `scan_run_id` in both Tantivy and `local_pages` atomically
- Atomic Tantivy rebuild (`rebuild_main_index_atomic`) with 7-handle closure (4 SearchEngine + 2 LocalIndexer + writer), Windows rename retry envelope, and `.old-<ts>` GC via `pending_dir_cleanup`
- `_commit_batch` durability bracket: `PRAGMA synchronous=FULL` + `BEGIN IMMEDIATE` + `ROLLBACK` on failure + `NORMAL` restore in `finally`
- `scan_runs` lifecycle table: `_begin_scan_run` / `_end_scan_run` / `start_recovery_probe` replacing `_pending_cleanup` sentinel anti-pattern
- `MyLibraryTab`: `is_searchable=False` default, `_show_recovery_modal` (3-button EN+HE Resume/Restart/Skip), `closeEvent` clean-shutdown sweep
- `SearchEngine._query_local_index`: `is_searchable` gate at first executable line via weakref (default-True when ref dead for CLI/test contexts)
- `scripts/check_plan_artifacts.py` (LD-12): forbidden-token auditor for Phase 97 plan .md files

## Task Commits

Each task was committed atomically:

1. **Task 1: Wave 0 RED test stubs + scripts/check_plan_artifacts.py** - `e4d752e3` (test)
2. **Task 2: LD-1 schema + LD-2 migration ladder + LD-3 cached_text write path** - `3af6dea1` (feat)
3. **Task 3: LD-4 atomic rebuild wired into SearchEngine startup + LD-5 handle closure** - `cb65ff67` (feat)
4. **Task 4: LD-8 ROLLBACK bracket + LD-6 scan_runs lifecycle + R-01 recovery gate** - `08d991a8` (feat)

## Final Phase 97 Tantivy Schema (LD-1)

```
Field             stored  tokenizer
unique_id         yes     raw
content           yes     whitespace
content_head      no      whitespace
content_tail      no      whitespace
line_starts       no      whitespace
line_ends         no      whitespace
source            yes     default
full_header       yes     default
shelfmark         yes     default
scope             yes     default
boundaries        yes     default
scan_run_id       yes     raw          [Phase 97 NEW — U-02: delete_documents by run]
chunk_locator     yes     default      [Phase 97 NEW — D-NEW-5: display string]
```

## Final Phase 97 SQLite Schema (user_version=2)

Key tables (additions only — Phase 95 baseline unchanged):
- `folders`: +indexed_count, error_count, pending_count, oversized_count, last_aggregate_at
- `processed_files`: +scan_run_id, mtime_ns
- `local_pages`: +cached_text (BLOB), cached_text_codec, cached_text_uncompressed_len, extraction_format_version, chunk_locator
- `scan_runs` (NEW): scan_run_id PK, started_at, ended_at, status CHECK IN ('running','completed','canceled','discarded')
- `pending_dir_cleanup` (NEW): path PK, kind, created_at — GC for .old-<ts> rebuild dirs

## Migration Ladder Map

| Step | Action |
|------|--------|
| 0->1 | No-op baseline stamp (Phase 95 tables already exist via init_sqlite) |
| 1->2 | All Phase 97 ALTER TABLE ADD COLUMNs + CREATE TABLE scan_runs + pending_dir_cleanup + D-NEW-4 prune |
| fresh DB | Stamped directly at user_version=2 in init_sqlite (empty processed_files guard) |

## Atomic Rebuild 7-Step Protocol

```
1. Build fresh_index in <dir>.rebuild-<scan_run_id>/
2. Validate: fresh_index.searcher() (releases handle before rename)
3. close_searcher_cb() — nullifies:
     SearchEngine.local_searcher
     SearchEngine.local_index
     SearchEngine.local_lab_searcher
     SearchEngine._local_lab_index
4. _close_internal_writer_index() — nullifies LocalIndexer._writer + _index
5. os.rename(live -> <dir>.old-<ts>)  [with 250ms/1s/2s retry]
6. os.rename(rebuild -> live)          [with retry; rollback on failure]
7. reload_searcher_cb() + _reopen_internal_writer_index()
   + INSERT pending_dir_cleanup(kind='rebuild_old') + _write_schema_marker
```

## Schema Marker File

`.schema_version` written alongside index dir containing sha256[:16] of `build_local_schema` source. On `Index.open`, mismatch triggers atomic rebuild (T-97A-08).

## _commit_batch Durability Bracket

Located at `shared/local_indexer.py` (lines ~1694-1732 post-edit):
```
PRAGMA synchronous = FULL
try:
    BEGIN IMMEDIATE
    UPDATE processed_files SET status='committed' WHERE filepath IN (...)
    COMMIT
except:
    ROLLBACK
    raise
finally:
    PRAGMA synchronous = NORMAL
```

## scan_runs Lifecycle Flow

```
_begin_scan_run() -> INSERT status='running', return run_id
    scan_all body (crash leaves row at 'running' — recovery probe fires)
_end_scan_run(run_id, 'completed'|'canceled'|'discarded')
    -> UPDATE ended_at=now(), status=?

start_recovery_probe() -> SELECT scan_run_id WHERE status='running'
    -> MyLibraryTab shows 3-button modal if non-empty

closeEvent clean-shutdown sweep:
    UPDATE scan_runs SET status='completed', ended_at=now() WHERE status='running'
```

## is_searchable Gate Location

`genizah_core.py::SearchEngine._query_local_index` — first executable lines after docstring:
```python
tab = self._my_library_tab_ref() if self._my_library_tab_ref is not None else None
if tab is not None and not getattr(tab, "is_searchable", True):
    return []
```
Default-True when weakref is dead (engine running standalone / CLI / tests).

Also applied to LOCAL LAB search paths in `search_composition_logic` (~line 1592) and `search_composition_logic_scl` (~line 8808).

## LD-12 Audit Script

```bash
python scripts/check_plan_artifacts.py .planning/phases/97-more-local-features/
```
Forbidden tokens: `requirements-desktop.txt`, `pytest.ini`, `browse_text_edit`, `_build_pages_html`, `_pending_cleanup`.
Exit 0 on clean; exit 1 with `<path>:<line>: forbidden token '<token>'` on finding.
Negation exemptions: lines containing NOT/MUST NOT/REPLACES/was replaced/instead of/do NOT; files matching `*-REVIEWS.md` or `*-CODEX-*`.

## Files Created/Modified

- `shared/local_indexer_migrations.py` — Migration ladder (0->1->2), integrity_check gate, idempotent _alter_safe
- `shared/local_indexer.py` — Schema extension, compress/decompress, rebuild_main_index_atomic, _commit_batch bracket, scan_runs lifecycle, schema marker
- `genizah_core.py` — attach_my_library_tab, close_local_searcher, _open_local_searcher R-02, _query_local_index gate
- `desktop/my_library_tab.py` — is_searchable, _show_recovery_modal, closeEvent scan_runs sweep
- `genizah_app.py` — on_startup_finished: attach_my_library_tab wiring
- `requirements.txt` — zstandard>=0.22,<1.0
- `requirements-lock.txt` — regenerated with zstandard==0.25.0
- `GenizahSearchPro.spec` — collect_all('zstandard')
- `scripts/check_plan_artifacts.py` — LD-12 forbidden-token auditor (NEW)
- `tests/test_local_indexer_migrations.py` — 6 tests (NEW)
- `tests/test_cached_text.py` — 3 tests (NEW)
- `tests/test_atomic_rebuild.py` — 5 tests (NEW)
- `tests/test_two_phase_durability.py` — 2 tests (NEW)
- `tests/test_recovery_gate.py` — 4 tests (NEW)
- `tests/fixtures/local_indexer/__init__.py` — package marker (NEW)

## Decisions Made

1. **scan_all split into wrapper + _scan_all_impl**: Wrapping scan_all body with lifecycle hooks required restructuring. Split keeps all existing callers unaffected (same public API) while the wrapper handles begin/end_scan_run.

2. **_ConnWrapper Python shim for tests**: `sqlite3.Connection.execute` is a C-level attribute and is read-only — `monkeypatch.setattr` raises `AttributeError`. Python-level wrapper class with `__getattr__` delegation allows intercepting specific SQL statements in tests.

3. **is_searchable gate wired in genizah_app.on_startup_finished**: SearchEngine is not available during `MyLibraryTab.__init__` (set asynchronously in `on_startup_finished`). The `attach_my_library_tab` call is placed after `self.searcher = searcher` in `on_startup_finished` with a guard for `hasattr(self, 'my_library_tab')`.

4. **LAB search gates at two call sites**: The LAB lab search is inline in two composition search methods (`search_composition_logic` and `search_composition_logic_scl`) rather than a dedicated `_query_local_lab_index` method. Gates added at both call sites using the same `_my_library_tab_ref` weakref pattern.

5. **fresh DB user_version stamp in init_sqlite**: The "empty `processed_files`" check stamps user_version=2 on fresh DBs so the migration ladder is bypassed entirely. Pre-existing Phase 95 DBs at user_version=0 (which have data) fall through and are migrated by `migrations.run()` in `LocalIndexer.__init__`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `test_integrity_check_failure_surfaces_reset_my_library` — `sqlite3.DatabaseError` not caught**
- **Found during:** Task 2 (migration tests)
- **Issue:** `PRAGMA integrity_check` raises `sqlite3.DatabaseError` on severely corrupt files rather than returning a non-"ok" string
- **Fix:** Wrapped the PRAGMA call in `try/except sqlite3.DatabaseError` in `migrations.run()`, converting to `RuntimeError("...Reset My Library...")`
- **Files modified:** `shared/local_indexer_migrations.py`
- **Committed in:** `3af6dea1` (Task 2)

**2. [Rule 1 - Bug] Fresh DB missing Phase 97 columns in `init_sqlite`**
- **Found during:** Task 2 (`test_write_page_doc_populates_cached_text`)
- **Issue:** Fresh DBs stamped at `user_version=2` (empty table guard) but `init_sqlite` CREATE TABLE statements only had Phase 95 baseline columns — `cached_text` etc. never existed on fresh DBs
- **Fix:** Updated `init_sqlite` to include all Phase 97 columns in the CREATE TABLE statements directly, plus `scan_runs` and `pending_dir_cleanup` tables
- **Files modified:** `shared/local_indexer.py`
- **Committed in:** `3af6dea1` (Task 2)

**3. [Rule 1 - Bug] `LocalIndexer.__init__` triggered atomic rebuild on fresh dirs**
- **Found during:** Task 2/3 integration
- **Issue:** `tantivy.Index.open()` fails with "FileDoesNotExist meta.json" on a fresh directory, causing the exception handler to trigger atomic rebuild (which then tried to rebuild from empty DB)
- **Fix:** Check for `meta.json` existence first — if absent (fresh dir), use `tantivy.Index(schema, path=index_dir)` to create; only trigger rebuild path for existing corrupt/mismatched indexes
- **Files modified:** `shared/local_indexer.py`
- **Committed in:** `3af6dea1` (Task 2)

**4. [Rule 1 - Bug] `monkeypatch.setattr` on `sqlite3.Connection.execute` fails (C-level read-only)**
- **Found during:** Task 4 (`test_update_failure_rolls_back`)
- **Issue:** `sqlite3.Connection.execute` is a C-level method, its attribute is read-only — `monkeypatch.setattr` raises `AttributeError: 'sqlite3.Connection' object attribute 'execute' is read-only`
- **Fix:** Replaced monkeypatch approach with a Python-level `_ConnWrapper` class injected via `indexer._thread_local._conn = wrapper`
- **Files modified:** `tests/test_two_phase_durability.py`
- **Committed in:** `08d991a8` (Task 4)

---

**Total deviations:** 4 auto-fixed (all Rule 1 bugs)
**Impact on plan:** All fixes essential for correctness. No scope creep — all fixes were within the task's own files or test code.

## Issues Encountered

- The `_conn` property reads from `threading.local()` (`self._thread_local._conn`), not a simple instance attribute — direct assignment to `indexer._conn` raised `AttributeError: property '_conn' has no setter`. The test needed to inject via `indexer._thread_local._conn` instead.

## Known Stubs

None — all Wave A functionality is fully implemented. Waves E/F placeholders are documented stubs: `rebuild_lab_index_atomic` contains `...` (Wave E will implement), and `_show_recovery_modal` marks Resume as "Wave E will implement actual resume logic" — both are Wave A design stubs, not data stubs affecting search correctness.

## Threat Flags

No new network endpoints, auth paths, or cross-trust-boundary surfaces introduced. All changes are local filesystem + SQLite only. T-97A-01 through T-97A-08 mitigations all implemented as specified in the plan threat register.

## Self-Check: PASSED

- All 20 Phase 97 Wave A tests pass (`test_local_indexer_migrations.py` + `test_cached_text.py` + `test_atomic_rebuild.py` + `test_two_phase_durability.py` + `test_recovery_gate.py`): 20/20 in 1.87s
- Orchestrator finalized SUMMARY.md commit after worktree connection drop (agent `af49f729a56c5e586`); 4 task commits were already landed before drop
- Broader test suite re-run scheduled at phase end after all waves complete

---
*Phase: 97-more-local-features*
*Completed: 2026-05-25*
