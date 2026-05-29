---
phase: 102-pdf-extraction-reorder-adopt-meiri-glyph-level-parser-d-f13-
plan: 04
subsystem: desktop/my-library + shared/local-indexer
tags: [migration, sqlite, schema, status-display, desktop-ui, tdd]
dependency_graph:
  requires: []
  provides:
    - corrupt_encoding status visible in My Library tree (red label, 3 surface points)
    - SQLite migration 2→3 (no-op DDL stamp, D-10 compliant)
    - corrupt_encoding protected in _KEPT_STATUSES from D-NEW-4 prune
    - fresh-DB stamp corrected to user_version=3 (MED-7)
    - reset-cycle test assertion bumped to user_version=3 (M1)
  affects:
    - shared/local_indexer_migrations.py
    - shared/local_indexer.py
    - desktop/my_library_tab.py
tech_stack:
  added: []
  patterns:
    - TDD (RED/GREEN) per task
    - Source inspection for AST-level surface verification
    - No-op migration stamp (D-10 compliant — no mass reindex)
key_files:
  created:
    - tests/test_local_indexer_migration_2_to_3.py
    - tests/test_my_library_corrupt_status_label.py
  modified:
    - shared/local_indexer_migrations.py
    - shared/local_indexer.py
    - tests/test_phase_97_2_reset_my_library_full_cycle.py
    - desktop/my_library_tab.py
decisions:
  - "_migrate_2_to_3 is a pure no-op stamp with no DDL or row mutations — D-10 compliance (no Phase 101 D-04 regression)"
  - "corrupt_encoding added to _KEPT_STATUSES so corrupt rows survive the 1→2 D-NEW-4 prune that deletes unsupported-extension rows"
  - "MED-8 test seeds corrupt_encoding BEFORE migration runs to prove _KEPT_STATUSES protection during the real prune, not post-prune"
  - "update_file_status is on _UnifiedFileTreeWidget, not MyLibraryTab — test helper updated accordingly"
metrics:
  duration: "~12 minutes"
  completed: "2026-05-29"
  tasks_completed: 2
  tasks_total: 2
  files_modified: 6
---

# Phase 102 Plan 04: corrupt_encoding Status + Migration 2→3 Summary

**One-liner:** SQLite schema migration 2→3 (no-op D-10 stamp) + corrupt_encoding in _KEPT_STATUSES + fresh-DB stamp MED-7 fix + red "Corrupt encoding" tree label at all three desktop My Library surface points.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 (RED) | Migration 2→3 tests | 5d4008c9 | tests/test_local_indexer_migration_2_to_3.py |
| 1 (GREEN) | Migration 2→3 + _KEPT_STATUSES + init_sqlite + M1 | a5b0216f | shared/local_indexer_migrations.py, shared/local_indexer.py, tests/test_phase_97_2_reset_my_library_full_cycle.py |
| 2 (RED) | corrupt_encoding label/color tests | b12aeb49 | tests/test_my_library_corrupt_status_label.py |
| 2 (GREEN) | corrupt_encoding at :333/:486/:519 | c839fc94 | desktop/my_library_tab.py |

## Implementation Details

### Task 1: Migration 2→3

**shared/local_indexer_migrations.py:**
- `_LATEST_VERSION` bumped 2 → 3
- `"corrupt_encoding"` added to `_KEPT_STATUSES` tuple
- `_migrate_2_to_3()` added — intentionally no DDL, no row mutations (D-10: no auto-flip)
- `2: _migrate_2_to_3` registered in `_MIGRATIONS` dict
- Module docstring updated to document 2→3 step

**shared/local_indexer.py (MED-7):**
- `PRAGMA user_version = 2` literal in `init_sqlite` bumped to `PRAGMA user_version = 3`
- Adjacent comment updated to note Phase 102 bump

**tests/test_phase_97_2_reset_my_library_full_cycle.py (M1):**
- Stale `assert v == 2` at line 71 updated to `assert v == 3`
- Docstring comment updated accordingly

### Task 2: corrupt_encoding tree label + color

**desktop/my_library_tab.py — 3 surface points:**
- `:333 _build_leaf_item_status`: new branch `if prior_st == 'corrupt_encoding': return pages_str, tr("Corrupt encoding"), '#e74c3c'`
- `:486 update_file_status`: new elif `elif status == "corrupt_encoding": display_status = tr("Corrupt encoding")`
- `:519 update_file_status`: red-paint guard extended from `('error', 'encoding_error')` to `('error', 'encoding_error', 'corrupt_encoding')`

## Verification

```
python -m pytest tests/test_local_indexer_migration_2_to_3.py tests/test_my_library_corrupt_status_label.py tests/test_phase_97_2_reset_my_library_full_cycle.py tests/test_local_indexer_migrations.py -q
# Result: 24 passed, 1 warning
```

```
python -m ruff check shared/local_indexer_migrations.py shared/local_indexer.py desktop/my_library_tab.py
# Result: All checks passed!
```

## Deviations from Plan

### Auto-fixed Issues

None — plan executed as written.

**Minor deviation (test helper):** `_UnifiedFileTreeWidget.__init__` takes `(parent: QWidget, app: object)` — not `(app,)`. Test helper `_make_tree_widget()` was written initially with wrong signature, corrected before commit. This is a test scaffolding fix, not a code deviation.

## Known Stubs

None — no stubs introduced. The `corrupt_encoding` status display is fully wired at all three surface points. The extractor that *produces* `corrupt_encoding` rows lands in Plan 03 (Wave 3); this plan provides the display infrastructure first (per plan objective).

## Threat Flags

None — no new network endpoints, auth paths, or schema changes at trust boundaries. The migration adds only a no-op PRAGMA stamp. The status string flows through a bounded enum to a static label/color (T-102-14 accepted).

## Self-Check

- [x] `tests/test_local_indexer_migration_2_to_3.py` exists
- [x] `tests/test_my_library_corrupt_status_label.py` exists
- [x] Commits `5d4008c9`, `a5b0216f`, `b12aeb49`, `c839fc94` exist in git log
- [x] `_LATEST_VERSION = 3` in shared/local_indexer_migrations.py
- [x] `"corrupt_encoding"` in `_KEPT_STATUSES`
- [x] `def _migrate_2_to_3` present
- [x] `2: _migrate_2_to_3` registered
- [x] `PRAGMA user_version = 3` in shared/local_indexer.py init_sqlite
- [x] `assert v == 3` in test_phase_97_2_reset_my_library_full_cycle.py
- [x] `corrupt_encoding` appears >= 3 times in desktop/my_library_tab.py

## Self-Check: PASSED
