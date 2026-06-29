---
phase: 129-library-filter-search-browse-by-identification-seed-026
fixed_at: 2026-06-28T00:00:00Z
review_path: .planning/phases/129-library-filter-search-browse-by-identification-seed-026/129-REVIEW.md
iteration: 1
findings_in_scope: 5
fixed: 5
skipped: 0
status: all_fixed
---

# Phase 129: Code Review Fix Report

**Fixed at:** 2026-06-28
**Source review:** `.planning/phases/129-library-filter-search-browse-by-identification-seed-026/129-REVIEW.md`
**Iteration:** 1

**Summary:**
- Findings in scope: 5 (1 Critical + 4 Warnings; 3 Info findings excluded per scope)
- Fixed: 5
- Skipped: 0

## Fixed Issues

### CR-01: Six new UI strings leak English under Hebrew (no translation entries)

**Files modified:** `genizah_translations.py`
**Commit:** `0b5bb13d`
**Applied fix:** Added 6 Hebrew translation entries to `genizah_translations.TRANSLATIONS` immediately before the PGP filter block (~line 2900). Entries added:
- `"Filter by library"` → `"סינון לפי ספרייה"`
- `"Filter results by library"` → `"סינון התוצאות לפי ספרייה"`
- `"Select libraries..."` → `"בחר ספריות..."`
- `"Library filter"` → `"מסנן ספרייה"`
- `"All Libraries"` → `"כל הספריות"`
- `"Libraries"` → `"ספריות"`

Both the web `tr()` (`web/translations.py`) and the desktop `tr()` (`genizah_core.py`) read from the same `TRANSLATIONS` dict, so one set of entries fixes both apps.

### WR-01: Desktop passes live mutable `library_filter` list into worker thread

**Files modified:** `genizah_app.py`
**Commit:** `6fd016a2`
**Applied fix:** Changed `library_filter=self._catalog_library_filter` to `library_filter=list(self._catalog_library_filter)` at line 10190 in `_catalog_start_async_refresh`. The worker now receives an independent snapshot that the UI thread cannot mutate while the worker iterates it.

### WR-02: Desktop catalog offers a non-functional LOCAL ("My Library") option

**Files modified:** `genizah_app.py`
**Commit:** `2e9e0e61`
**Applied fix:** Changed the desktop library menu build loop from `for _lib_code in list(LIBRARY_CODES.keys())` to `for _lib_code in (c for c in LIBRARY_CODES.keys() if c != 'LOCAL')` at line 9854. This mirrors the explicit `if code != 'LOCAL'` exclusion that the web catalog already applies at `web/pages/catalog_browse.py:1356`.

### WR-03: TEMP-table token uses collision-prone `hash()`

**Files modified:** `shared/fjms_service.py`
**Commit:** `3dbaea71`
**Applied fix:** Replaced `_lib_token = hash(tuple(sorted(library_codes)))` with `_lib_token = tuple(sorted(library_codes))`. Updated the `_ensure_filter_temp` type annotation from `token: int` to `token` (untyped, accepts any equality-comparable value — the PGP/edition callers still pass `len(sys_ids)` which is an `int`, the library caller now passes a `tuple`). Updated the docstring to document the collision-free approach. The token registry compares via `==` throughout, so `tuple == tuple` works correctly with no other changes.

### WR-04: Word-search library-only path missing "Library filter" count indicator

**Files modified:** `web/pages/search.py`
**Commit:** `aeaa095c`
**Applied fix:** Replaced the single-format string in the word-search `else` branch results count (lines 4005-4015) with a `count_parts` list pattern consistent with the other filter paths. The excluded count is always in `count_parts`; when `search_state.library_filter` is truthy, `tr('Library filter')` is appended, producing e.g. `"42 of 1000 Results (0 excluded, מסנן ספרייה)"` under Hebrew. Matches the pattern used at lines 3561-3570 and 4065-4077.

## Skipped Issues

None — all 5 in-scope findings were successfully fixed.

---

_Fixed: 2026-06-28_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
