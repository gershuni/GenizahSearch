---
phase: 96
plan: "04"
subsystem: desktop-my-library
tags: [phase-96, my-library, persistence, session-json, d-f1]
dependency_graph:
  requires: [96-01]
  provides: [_local_file_optouts-attribute, session-json-persistence, prune-helper]
  affects: [genizah_app.py, desktop/my_library_tab.py]
tech_stack:
  added: []
  patterns: [session-json-persistence, pure-helper-function]
key_files:
  modified:
    - genizah_app.py
    - desktop/my_library_tab.py
decisions:
  - "D-08 REVISED 2026-05-24: persist via session JSON (not QSettings) — matches Phase 95 local_filter_* pattern"
  - "Top-level placement of local_file_optouts key (cross-surface) vs nested per-surface Phase 95 keys"
  - "_prune_optouts_to_disk is a pure module-level function (no Qt, no I/O) for testability"
metrics:
  duration: "~15 minutes"
  completed: "2026-05-24"
  tasks_completed: 2
  tasks_total: 2
  files_modified: 2
---

# Phase 96 Plan 04: D-F1 Persistence Layer Summary

**One-liner:** Session-JSON persistence for per-file LOCAL opt-out set (`_local_file_optouts`) with rescan-preserve helper, unblocking filter cascade (96-05) and tree UI (96-06).

## What Was Built

### Task 1: `genizah_app.py` — Attribute + Session Save/Restore

- `self._local_file_optouts: set[str] = set()` initialized in `__init__` at line 2879, adjacent to Phase 95 `_local_filter_state_*` attributes
- `_save_session` extended with top-level key `'local_file_optouts': sorted(getattr(self, '_local_file_optouts', set()))` at line 23597, alongside `word_excluded_sys_ids` / `active_tab` (NOT nested in `regular_search`)
- `_restore_session` extended with `self._local_file_optouts = set(state.get('local_file_optouts', []))` at line 23656, reading from `state` (top level), NOT from `reg` (regular_search)

**W6 closure:** The structural distinction between Phase 96's cross-surface key (top-level) and Phase 95's per-surface keys (nested in `regular_search` / `composition_search`) is explicit and source-pinned. Both patterns coexist correctly.

### Task 2: `desktop/my_library_tab.py` — `_prune_optouts_to_disk` Helper

Pure module-level function added at line 76 (before `LocalIndexerWorker` class):

```python
def _prune_optouts_to_disk(optouts: set, on_disk: set) -> set:
    """Phase 96 D-F1 D-09: filter the opt-out set to entries still on disk."""
    if not optouts: return set()
    if not on_disk: return set()
    return optouts & on_disk
```

Semantics: returns the intersection of opt-outs with files currently on disk. Files removed or renamed since the last scan drop their opt-out state (D-09).

## Persistence Design (D-08 REVISED 2026-05-24)

The original D-08 said "QSettings". REVISED after the researcher surfaced that ALL other LOCAL filter state (`local_filter`, `local_filter_composition`, `local_filter_parallels`, `domain_exclusions`, `excluded_sys_ids`, etc.) persists in session JSON. Using QSettings would create two stores for the same feature family. Session JSON provides identical restart-survival semantics with one store.

The new key `local_file_optouts` is at **TOP LEVEL** of the session dict (same level as `pre_search_filters`, `word_excluded_sys_ids`, `active_tab`) because opt-out is cross-surface — one set applies to Search, Composition, and Parallels equally. This differs from Phase 95's `local_filter` which is nested in `regular_search` (per-surface).

## Tests

All 5 tests in `tests/test_local_optout_persistence.py` pass:

| Test | Was | Now |
|------|-----|-----|
| `test_session_json_roundtrip_preserves_optouts` | PASS | PASS |
| `test_optout_list_default_empty_for_old_sessions` | PASS | PASS |
| `test_rescan_preserves_survivors_drops_removed` | **SKIPPED** | **PASS** |
| `test_folder_a_optout_survives_folder_b_toggle` | PASS | PASS |
| `test_canonical_filepath_windows_variants` | PASS | PASS |

Phase 95 invariants: 24 tests in `test_local_filter_persistence.py` + `test_my_library_tab.py` all PASS. Full LOCAL regression bundle: 172 passed, 15 skipped, 5 xfailed.

## Downstream Consumers

- **96-05** (Wave 2): reads `self._local_file_optouts` in the filter cascade (`_apply_results_table_filters` + `_apply_comp_tree_filters`)
- **96-06** (Wave 3): mutates `self._local_file_optouts` on tree-widget checkbox toggle using set-difference/union algebra (NOT clear+rebuild — Codex HIGH #1 guard), and calls `_prune_optouts_to_disk` on scan completion

## W8 Note

`LocalIndexer.list_all_filepaths()` does not exist as of 2026-05-24. Plan 96-06 adds it (or queries `_conn` directly) to obtain the `on_disk` set for `_prune_optouts_to_disk`.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | c2f0e9a0 | feat(96-04): initialize _local_file_optouts + wire session save/restore |
| 2 | 229c031b | feat(96-04): add _prune_optouts_to_disk helper for rescan preservation |

## Deviations from Plan

None — plan executed exactly as written. The D-08 REVISED decision was already baked into the plan (pre-revision was QSettings; plan specifies session JSON as the final decision).

## Self-Check: PASSED

- FOUND: genizah_app.py
- FOUND: desktop/my_library_tab.py
- FOUND: commit c2f0e9a0
- FOUND: commit 229c031b
- `grep -c "_local_file_optouts" genizah_app.py` = 3 (exactly: init, save, restore)
- `'local_file_optouts':` save key = 1 match at top-level position
- `state.get('local_file_optouts'` restore = 1 match reading from `state` not `reg`
- `_prune_optouts_to_disk` at module level (line 76, before class)
- All 5 D-F1 persistence tests PASS
- Ruff clean on both modified files
