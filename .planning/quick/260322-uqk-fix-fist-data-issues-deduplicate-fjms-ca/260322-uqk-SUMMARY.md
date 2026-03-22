---
quick_id: 260322-uqk
status: complete
commit: 77e562e2
---

# Summary: Deduplicate FJMS Catalog Free Descriptions

## What was done

Added deduplication to `get_catalog_detail()` in `shared/fjms_service.py` for the free descriptions section. The FIST source data contains 14,504 duplicate rows in `catalog_free_desc` (same AlmaId + FreeDesc text, different SignatureIds) across 12,507 manuscripts. The fix deduplicates by (source_name, text) tuple, keeping the first occurrence.

## Changes

- **shared/fjms_service.py**: Added `seen_descs` set to deduplicate free descriptions by (SourceName, FreeDesc) in both the v4.1.0+ and fallback code paths.

## Verification

- Ms. Add. 3207 (sys_id 990001398720205171): was showing 4 free descriptions (2 identical Sussmann entries), now correctly shows 3 unique entries.
- 464 tests pass (1 pre-existing puzzle model test failure, unrelated).

## Out of Scope

- **Missing bibliography volumes**: 98% of FIST bibliography entries (531K/542K) have NULL Volume field. This is a FIST source data gap, not a code bug. Needs investigation against the real FIST.db (in backup folder) to determine if volume data exists but wasn't exported.
