---
phase: 129-library-filter-search-browse-by-identification-seed-026
reviewed: 2026-06-28T00:00:00Z
depth: standard
files_reviewed: 9
files_reviewed_list:
  - genizah_app.py
  - shared/fjms_service.py
  - tests/conftest.py
  - tests/test_libfilter_catalog.py
  - tests/test_libfilter_desktop.py
  - tests/test_libfilter_web_search.py
  - web/pages/catalog_browse.py
  - web/pages/search.py
  - web/pages/search_state.py
findings:
  critical: 1
  warning: 4
  info: 3
  total: 8
status: issues_found
---

# Phase 129: Code Review Report

**Reviewed:** 2026-06-28
**Depth:** standard
**Files Reviewed:** 9
**Status:** issues_found

## Summary

Reviewed the SEED-026 library-filter implementation (phase 129) at standard depth, focusing on the diff since base `066ba752`. The push-down filter architecture in `shared/fjms_service.py` is sound: the library condition is composed into the shared `where` clause that drives BOTH `COUNT(DISTINCT c.AlmaId)` and the paginated `LIMIT/OFFSET` results query (lines 2283-2310), so `total` correctly reflects the full filtered set before pagination. The content-derived TEMP-table token (`hash(tuple(sorted(library_codes)))`) correctly rebuilds on same-size-but-different selections, and the selected-but-resolved-empty fail-open path is implemented and tested. Off-event-loop / off-UI-thread resolution is correct on both web catalog (`run.io_bound` → `_fetch_results_blocking`) and desktop (`_CatalogRefreshWorker.run()`). Phase 87 safe_storage chokepoint compliance is correct on both web surfaces (`persist_value`/`_safe_get` in search, `safe_user_get`/`safe_user_set` in catalog).

The dominant defect is **i18n**: six of the seven new UI strings have NO Hebrew translation entry, and both `tr()` implementations fall back to the raw English literal when a key is missing — so under the default Hebrew web UI (and Hebrew desktop UI) the filter button, tooltip, dropdown label, count indicator, and "All Libraries"/"Libraries" labels all leak English. This directly violates the stated focus-area requirement ("EN/HE i18n with no English leak under Hebrew") and there is no global i18n guard test that would catch it. Secondary issues: a desktop thread-safety race on the shared mutable `library_filter` list, and a web/desktop parity gap where the desktop catalog offers a non-functional `LOCAL` ("My Library") option that the web catalog deliberately excludes.

## Critical Issues

### CR-01: Six new UI strings leak English under Hebrew (no translation entries)

**File:** `genizah_translations.py` (missing keys); used in `web/pages/search.py:1593,1614,1621,3573,4073`, `web/pages/catalog_browse.py:759,1347,1362`, `genizah_app.py:9845,10460,10464`

**Issue:** Both `tr()` implementations fall back to the raw English literal on a missing key under Hebrew:
- `web/translations.py:44` → `return TRANSLATIONS.get(text, text)`
- `genizah_core.py:586` → `return TRANSLATIONS.get(text, text)`

The web app defaults to Hebrew (`web/translations.py:12 _current_lang = 'he'`). The diff introduces seven `tr()` literals, of which six are absent from `genizah_translations.TRANSLATIONS` (verified by grep against the file):

| String | In TRANSLATIONS? | Surfaces |
|--------|------------------|----------|
| `Library` | YES | chip prefix (OK) |
| `Filter by library` | **MISSING** | web search button, web catalog card label |
| `Filter results by library` | **MISSING** | web search button tooltip |
| `Select libraries...` | **MISSING** | web catalog `ui.select` label |
| `Library filter` | **MISSING** | web search results-count indicator |
| `All Libraries` | **MISSING** | desktop button default text |
| `Libraries` | **MISSING** | desktop button text when active (`Libraries (N)`) |

Result: under the default Hebrew UI, the search "Filter by library" button, its tooltip, the catalog multi-select label, the results-count "(Library filter)" suffix, and the desktop "All Libraries"/"Libraries (N)" button all render in English. This is precisely the "English leak under Hebrew" the phase focus area calls out, and no global i18n guard test exists (the i18n tests in `tests/` are scoped to joins-lab/join-workbench only), so the existing suite will not catch it.

Note: the per-library *names* themselves are correctly Hebrew — `get_library_display(code, short=False, lang='he')` resolves via `LIBRARY_CODES_HE` (verified: 89/89 codes have HE entries). Only the static UI chrome strings leak.

**Fix:** Add Hebrew entries to `genizah_translations.TRANSLATIONS` for all six missing keys, e.g.:
```python
"Filter by library": "סינון לפי ספרייה",
"Filter results by library": "סינון התוצאות לפי ספרייה",
"Select libraries...": "בחר ספריות...",
"Library filter": "מסנן ספרייה",
"All Libraries": "כל הספריות",
"Libraries": "ספריות",
```
(Translations should be reviewed by a Hebrew speaker.) Optionally add a phase-scoped i18n guard test asserting these keys exist, mirroring `tests/test_joins_lab_i18n.py`.

## Warnings

### WR-01: Desktop passes the live mutable `library_filter` list into the worker thread — concurrent-mutation race

**File:** `genizah_app.py:10190` (and `genizah_app.py:520`, `10447-10450`)

**Issue:** `_catalog_start_async_refresh` passes the live instance list by reference:
```python
library_filter=self._catalog_library_filter,   # line 10190
```
The worker stores the same object (`self._library_filter = library_filter or []`, line 520) and, in `run()` (off the UI thread), iterates it twice: inside `resolve_library_sys_ids` (`{c for c in library_codes if c in _VALID_CODES}`) and again in `get_browse_results` (`hash(tuple(sorted(library_codes)))`). Meanwhile `_catalog_toggle_library` mutates that SAME list **in place** on the UI thread (`self._catalog_library_filter.append(code)`, line 10448). `_catalog_start_async_refresh` does not cancel/await the previous worker before starting a new one. A rapid second toggle while a prior worker is mid-iteration can raise `RuntimeError: set/list changed size during iteration` (or silently produce inconsistent results). The PGP/editions filters are immune because they pass immutable strings; the library filter is the first catalog filter to hand a mutable shared list to the worker, so this is a newly-introduced risk.

**Fix:** Pass a snapshot copy so the worker owns an immutable view:
```python
library_filter=list(self._catalog_library_filter),
```
(Defensively, `_CatalogRefreshWorker.__init__` could also do `self._library_filter = list(library_filter) if library_filter else []`.)

### WR-02: Desktop catalog offers a non-functional `LOCAL` ("My Library") option — web/desktop parity gap

**File:** `genizah_app.py:9854` (menu build loop)

**Issue:** The desktop builds the library menu over the full code set with no LOCAL exclusion:
```python
for _lib_code in list(LIBRARY_CODES.keys()):   # includes 'LOCAL'
    action = lib_menu.addAction(get_library_display(_lib_code, short=False))
```
The web catalog deliberately excludes it (`web/pages/catalog_browse.py:1356` — `if code != 'LOCAL'  # My Library is a local-only concept, not a Genizah library`). Selecting "My Library" in the desktop catalog sets `library_filter=['LOCAL']`; `resolve_library_sys_ids` finds no csv_bank rows with `library_code == 'LOCAL'` (csv_bank is the Genizah corpus from libraries.csv, not the local index), returns an empty set, and `get_browse_results` then **fails open** and returns ALL results. The user sees a checked "My Library" filter that silently does nothing — confusing UX and a parity divergence from the web surface this phase is supposed to mirror.

**Fix:** Mirror the web exclusion in the desktop menu loop:
```python
for _lib_code in (c for c in LIBRARY_CODES.keys() if c != 'LOCAL'):
```

### WR-03: TEMP-table token relies on `hash()`, which can collide (stale-data risk)

**File:** `shared/fjms_service.py:2261` (`_lib_token = hash(tuple(sorted(library_codes)))`) consumed by `_ensure_filter_temp` line 2025 (`if reg.get(name) == token: return True`)

**Issue:** The content token is a Python `hash()` of the selection tuple. `_ensure_filter_temp` skips the rebuild whenever the new token equals the cached token. Because `hash()` is not injective, two *different* selections on the same worker thread can (rarely) collide to the same `int`, causing the second selection to silently reuse the first selection's TEMP rows and return wrong results. The probability is astronomically low for small string tuples, so this is a latent correctness risk rather than a practical one — but the comment claims the content token "guarantees a rebuild when the selection changes," which is not strictly true. (The PGP/edition `len`-based tokens are safe because those sets are static per value.)

**Fix:** Use a collision-free token, e.g. store the canonical selection itself: `token = tuple(sorted(library_codes))` and compare tuples (the registry already stores arbitrary values and only does `==`). This removes the hash-collision class entirely at no cost.

### WR-04: Web search results-count indicator is inconsistent across filter paths when library is active

**File:** `web/pages/search.py:4006-4011` (word-search "else" branch) vs `web/pages/search.py:4072-4073` and `3573`

**Issue:** Most filter-render paths append a `tr('Library filter')` token to the results-count when `search_state.library_filter` is set (e.g. lines 3573, 4072-4073, 4108). But the word-search "library-only" else-branch (lines 4006-4012) applies `_apply_library_filter` and then renders the count as `"{showing} of {total} Results ({n_excl} excluded)"` with no "Library filter" indicator. The user filtering a word-search result set by library sees the count shrink with no explanation of which filter caused it. This is a UX/consistency defect, not a data-correctness one (the filter itself is applied correctly to the full set before render).

**Fix:** Add the same indicator in the word-search else-branch, e.g. build a `count_parts` list there too and append `tr('Library filter')` when `search_state.library_filter` is truthy, mirroring the other paths.

## Info

### IN-01: `_compute_library_facets` re-imports `Counter` on every call

**File:** `web/pages/search.py:3540`

**Issue:** `from collections import Counter` is inside the function body, so it runs on every facet recompute (every dropdown rebuild). Harmless functionally but unnecessary; hoist to module-level imports for clarity.

**Fix:** Move `from collections import Counter` to the top-of-file imports.

### IN-02: `clear_library_code` mutates the persisted list in place rather than reassigning

**File:** `web/pages/catalog_browse.py:977-985`

**Issue:** `clear_library_code` does `lst = current_library_filter['value']; lst.remove(code)` (in-place), whereas `_on_library_filter_change` reassigns a fresh validated list. The in-place path is functionally fine here (single-threaded async, value re-synced into the widget afterwards), but the inconsistency between the two mutation styles is a small maintainability smell and could become a problem if the same list object is ever aliased elsewhere. Prefer the reassignment style used by `_on_library_filter_change`.

**Fix:** `current_library_filter['value'] = [c for c in current_library_filter['value'] if c != code]`.

### IN-03: Desktop library menu labels are fixed at construction time (no live language switch)

**File:** `genizah_app.py:9854-9856`, `10460-10464`

**Issue:** Per-library menu-item labels are computed once at widget construction via `get_library_display(_lib_code, short=False)`. If the desktop language is changed at runtime, the menu labels (and the once-set button text) will not refresh. This matches the desktop's general "language change effectively needs a restart" posture and the static nature of a 110-entry menu, so it is consistent with existing behavior — noted only for completeness. (The button label itself does re-`tr()` on update via `_catalog_update_library_filter_btn`, but depends on CR-01's missing keys.)

**Fix:** No action required unless live language switching is a desktop goal; if so, rebuild the menu on language change alongside other catalog labels.

---

_Reviewed: 2026-06-28_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
