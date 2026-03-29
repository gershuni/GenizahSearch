---
plan: 56-01
status: complete
started: 2026-03-29
completed: 2026-03-29
---

## Summary

Created the shared exclusion service module (`shared/exclusion_service.py`) with:

- **ExclusionSource** dataclass: multi-source tracking (label, source_type, source_id, sys_ids, unresolved, resolved_entries)
- **ResolvedEntry** dataclass: per-row resolution detail (original, normalized, sys_id, status)
- **parse_shelfmark_file**: plain text parsing (strips blanks, ignores # comments)
- **parse_csv_shelfmarks**: CSV with column auto-detect (7 keywords), BOM handling
- **resolve_shelfmarks**: normalize + shelf_map lookup, returns 3-tuple with ResolvedEntry tracking
- **build_shelf_map**: indexes primary shelfmark + call_numbers_raw variants (first-write-wins)
- **compute_excluded_ids**: union of all sources' sys_ids
- **serialize_sources / deserialize_sources**: session persistence roundtrip (resolved_entries excluded as transient)

## Key Files

### Created
- `shared/exclusion_service.py` — 165 lines, 9 public functions
- `tests/test_exclusion.py` — 15 tests covering all functions

## Verification

- `pytest tests/test_exclusion.py -x -q` — 15/15 passed
- Import verification clean

## Deviations

None. Plan executed as specified.
