# Quick Task 007: Summary

## What changed

**File:** `genizah_core.py` — `MetadataManager.resolve_system_by_shelfmark()` (line 3804)

Added 7 lines at the top of the method that strip known library code prefixes (from `LIBRARY_CODES` dict) before normalizing the shelfmark query. This allows the Go button to work correctly when the input field contains a library-prefixed shelfmark like "CUL T-S NS 23.23".

## How it works

Before the existing normalization step, the new code:
1. Iterates through all known `LIBRARY_CODES` keys
2. Checks if the query starts with a code followed by a space (case-insensitive)
3. Strips the prefix if found

## Test results

- Manual test: 8/8 cases pass (CUL, Oxford, JTS, BL, RNL, Manchester, no-prefix, code-only)
- Existing tests: 156 passed (13 pre-existing failures unrelated to this change)
