---
phase: 95
plan: "04"
subsystem: cloud-write-gates
tags: [security, REQ-9, lists-sync, corrections, search-serializer, local-namespace]
dependency_graph:
  requires: [95-02]
  provides: [cloud-write-gates, local-namespace-enforcement]
  affects: [lists_sync, corrections_client, search_serializer]
tech_stack:
  added: []
  patterns: [gate-before-client, parallel-or-gate, defense-in-depth-filter]
key_files:
  created:
    - tests/test_local_namespace_no_lists_leak.py
    - tests/test_local_namespace_no_corrections_leak.py
    - tests/test_local_namespace_no_api_leak.py
  modified:
    - lists_sync.py
    - corrections_client.py
    - shared/search_serializer.py
decisions:
  - "Gates placed as FIRST STATEMENT of each function body, before is_sync_available() and _get_client() per Codex P0 finding"
  - "HIGH-2 fix: sys_id derivation in sync_item_to_cloud runs outside any if item_data branch (flattened ternary) so LOCAL item_id with missing item_data is also gated"
  - "sync_list_to_cloud iterates data['items'] flat dict checking item_data['lists'] membership per B2 pinned field names from sync_to_cloud:619-635"
  - "corrections_client uses parallel separate gate (not OR-merge) to keep distinct error codes: local_corrections_disabled vs synthetic_corrections_disabled"
  - "_is_local_item() helper in search_serializer checks both library_code=='LOCAL' and is_local_sys_id(sys_id) for belt-and-suspenders coverage"
metrics:
  duration: "~15 minutes"
  completed: "2026-05-21"
  tasks_completed: 3
  tasks_total: 3
  files_modified: 6
---

# Phase 95 Plan 04: Cloud-Write Gates Summary

**One-liner:** Three cloud-write boundaries gating LOCAL sys_ids (97-prefix) via `is_local_sys_id` — lists sync top-of-function (Codex P0 + HIGH-2), corrections parallel gate, search serializer pre-filter.

## What Was Built

Three cloud-write boundary gates preventing LOCAL sys_ids (Phase 95 local documents with 97-prefix 18-digit sys_ids) from ever reaching Supabase or public API surfaces.

### Gate 1: `lists_sync.py` — `sync_item_to_cloud` (lines 754–766)

Inserted as the FIRST STATEMENT of the function body, before `is_sync_available()` (old line 738) and before `_get_client()` (old line 742):

```python
item_data = self.lists_manager.data.get('items', {}).get(item_id)
sys_id = item_data.get('sys_id', item_id) if item_data else item_id
if is_local_sys_id(sys_id):
    logger.info("[local-only item, not synced] item_id=%s sys_id=%s", item_id, sys_id)
    return False
```

**HIGH-2 fix:** The `sys_id` derivation runs as a flattened ternary OUTSIDE any `if item_data:` branch. When `item_data` is None and `item_id` itself is a LOCAL sys_id (e.g., a race condition where the item was removed from memory), the gate still fires before any cloud touch.

### Gate 2: `lists_sync.py` — `sync_list_to_cloud` (lines 701–713)

Inserted as FIRST STATEMENTS before `is_sync_available()`, iterating all items in the list to abort if any are LOCAL:

```python
items_map = self.lists_manager.data.get('items', {})
for iid, item_data in items_map.items():
    if list_id not in (item_data.get('lists') or []):
        continue
    if is_local_sys_id(item_data.get('sys_id', iid)):
        logger.info("[list contains LOCAL items, not synced] list_id=%s", list_id)
        return False
```

**B2 resolution confirmed:** Field names pinned from `sync_to_cloud:619-635` canonical pattern:
- Items collection: `self.lists_manager.data.get('items', {})` (flat dict keyed by item_id)
- List membership field: `item_data.get('lists', [])` (list of list_ids)
- Sys_id field: `item_data.get('sys_id', iid)` (string, fallback to item_id)

### Gate 3: `corrections_client.py` — `create_correction` (lines ~625–631)

Parallel gate added IMMEDIATELY AFTER the existing SYNTH-06 synthetic gate:

```python
# Phase 95 REQ-9 — LOCAL sys_ids never reach the corrections cloud surface.
if is_local_sys_id(document_id):
    return (
        None,
        "local_corrections_disabled: corrections cannot be added to LOCAL sys_ids",
    )
```

NOT merged into a single OR — distinct error codes required by REQ-9 acceptance criteria.

### Gate 4: `shared/search_serializer.py` — `serialize_search_payload` (defense-in-depth)

Helper `_is_local_item()` added, filter applied before `_serialize_item` listcomp:

```python
results = [r for r in results if not _is_local_item(r)]
```

`_is_local_item()` checks both `library_code == 'LOCAL'` and `is_local_sys_id(sys_id)` for full coverage.

## Test Files

| File | Tests | Coverage |
|------|-------|----------|
| `tests/test_local_namespace_no_lists_leak.py` | 6 | sync_item (item_data present + missing), sync_list (local abort + no-local regression), synthetic regression, HIGH-2 load-bearing |
| `tests/test_local_namespace_no_corrections_leak.py` | 3 | LOCAL gate, synthetic gate unchanged, real Alma passes both gates |
| `tests/test_local_namespace_no_api_leak.py` | 2 | LOCAL dropped from output, no LOCAL unchanged |

All 11 tests pass. Load-bearing assertions:
- `_get_client.call_count == 0` for LOCAL sys_ids in `sync_item_to_cloud` (both item_data-present and item_data-absent paths)
- `_get_client.call_count == 0` for LOCAL items in `sync_list_to_cloud`
- `mock_request.call_count == 0` for LOCAL sys_ids in `create_correction`
- `len(envelope['results']) == 2` when 1 of 3 input results is LOCAL

## Commits

| Hash | Description |
|------|-------------|
| `51f385d2` | feat(95-04): LOCAL gates at TOP of sync_item_to_cloud + sync_list_to_cloud (D-30 Codex P0 + HIGH-2, REQ-9) |
| `430fcb6f` | feat(95-04): extend corrections_client with parallel LOCAL gate (REQ-9) |
| `aa55c64d` | feat(95-04): add LOCAL filter to shared/search_serializer.py (REQ-9 defense-in-depth) |

## Deviations from Plan

None — plan executed exactly as written. All Codex P0 and HIGH-2 requirements satisfied.

## Known Stubs

None — all three gates are fully wired and operational.

## Threat Flags

No new network endpoints, auth paths, file access patterns, or schema changes introduced. All changes are guard clauses on existing cloud-write paths.

## Self-Check: PASSED

All files present and all commits verified:

- `lists_sync.py` — FOUND
- `corrections_client.py` — FOUND
- `shared/search_serializer.py` — FOUND
- `tests/test_local_namespace_no_lists_leak.py` — FOUND
- `tests/test_local_namespace_no_corrections_leak.py` — FOUND
- `tests/test_local_namespace_no_api_leak.py` — FOUND
- Commit `51f385d2` — FOUND
- Commit `430fcb6f` — FOUND
- Commit `aa55c64d` — FOUND
