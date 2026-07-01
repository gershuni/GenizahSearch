---
phase: 131-dual-mode-parity-desktop-catalog-web-browse-by-identificatio
plan: "07"
subsystem: desktop-catalog-library-filter
tags: [desktop, library-filter, search, sort, parity, PyQt6, translations]
dependency_graph:
  requires: [131-06]
  provides: [DMF-07-desktop-search-sort-parity]
  affects: [desktop/dialogs_filter.py, genizah_translations.py, tests/test_libfilter_desktop.py]
tech_stack:
  added: []
  patterns: [QLineEdit-textChanged-setHidden, QButtonGroup-buttonToggled-repopulate, _populate_rows-checked_set-helper]
key_files:
  created: []
  modified:
    - desktop/dialogs_filter.py
    - genizah_translations.py
    - tests/test_libfilter_desktop.py
decisions:
  - "Sort toggle implemented as two QRadioButton in a QButtonGroup (mirrors the web's two flat buttons) rather than QComboBox — direct visual mapping to the web UX."
  - "_populate_rows extracted as a helper taking a checked_set param so both initial population (__init__) and re-sort (_repopulate) share one code path."
  - "_apply_search_filter matches on item.text() (the display label including count suffix) — a library-name substring still matches because the name is a prefix of the label; consistent with web matching on data-label."
  - "Sort toggle signal connected AFTER initial _populate_rows call to avoid a spurious _on_sort_changed during __init__."
metrics:
  duration: "~20 minutes"
  completed: "2026-06-30"
  tasks: 3
  files: 3
---

# Phase 131 Plan 07: LibraryFilterDialog Search Box + Sort Toggle Summary

Desktop catalog LibraryFilterDialog gains a type-to-find search box (case-insensitive label substring, setHidden, check-state preserved) and an A-Z/By-count sort toggle (facets-desc with A-Z fallback), at parity with web /catalog catLibFilterSearch + catLibFilterSort.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Add type-to-find search box + A-Z/By-count sort toggle + re-sort/filter logic | 57344637 | desktop/dialogs_filter.py |
| 2 | Add Hebrew translations for new sort-control strings | 57344637 | genizah_translations.py |
| 3 | Extend tests/test_libfilter_desktop.py — 6 new tests | 0104900b | tests/test_libfilter_desktop.py |

## What Was Built

### Task 1 — LibraryFilterDialog enhancements (`desktop/dialogs_filter.py`)

New methods added to `LibraryFilterDialog`:

- **`_populate_rows(self, codes, checked_set)`** — extracted from the old `__init__` loop; builds `Name (count)` rows for the given code order with check state from `checked_set`. Used by both `__init__` (initial population) and `_repopulate` (re-sort preserving check state).
- **`_apply_search_filter(self, *args)`** — hides every row whose `item.text()` does not contain the query string (case-insensitive substring). Empty query shows all rows. Does NOT change check state.
- **`_on_sort_changed(self, *args)`** — listens to `_sort_group.buttonToggled`; ignores deactivation signals; calls `_repopulate`.
- **`_repopulate(self)`** — re-sorts `self._all_codes` (By-count desc via `self._facets`, falls back to A-Z when facets empty), clears and re-populates the list widget with preserved check state, re-applies the active search filter, and updates the OK button.

New controls added in `__init__`:

- `self.search_input = QLineEdit()` — placeholder `tr("Search libraries...")`, clearable, above the list widget.
- Sort controls: `QLabel(tr("Sort:"))` + `QRadioButton(tr("A–Z"))` (default, checked) + `QRadioButton(tr("By count"))` in `self._sort_group`.

All existing methods (`get_mode`, `_on_mode_changed`, `_on_item_changed`, `_update_ok_button`, `_select_all`, `_select_none`, `_on_accept`, `get_checked_codes`) unchanged — they all iterate `range(self.list_widget.count())` over ALL items including hidden ones, which is exactly the web parity requirement.

### Task 2 — Hebrew translations (`genizah_translations.py`)

Three new entries added near the existing `Sort by:` key (line ~911):

- `"By count": "לפי כמות"` — shared key with web `tr('By count')`
- `"A–Z": "א–ת"` — EN-DASH U+2013 key matches web `tr('A–Z')` exactly; Hebrew uses aleph–taw
- `"Sort:": "מיון:"` — bare form used by the sort label (the existing `"Sort by:"` is a different string)

`"Search libraries..."` already existed at line 2924 — untouched.

### Task 3 — Tests (`tests/test_libfilter_desktop.py`)

Six new `@pytest.mark.gui` tests appended (33 total, all pass headless):

1. `test_search_hides_nonmatching_rows_and_preserves_checks` — search hides non-matching rows; CUL stays Checked; clearing the box shows all rows again.
2. `test_get_checked_codes_returns_hidden_checked_codes` — hidden-but-checked codes (CUL, BL) appear in `get_checked_codes()` (web parity — hidden rows still count).
3. `test_by_count_sort_orders_by_facets_descending` — facets `{CUL:5000, BL:900, JTS:100}` → row order CUL before BL before JTS.
4. `test_az_sort_orders_by_display_name` — row order equals `sorted(codes, key=get_library_display)` ascending (default).
5. `test_by_count_falls_back_to_az_when_facets_empty` — By-count with empty facets produces identical order to A-Z.
6. `test_select_all_ignores_active_search_filter` — Select All checks ALL rows including hidden ones (web parity).

## Verification

```
GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_libfilter_desktop.py -q
33 passed, 1 warning in 3.47s

python -m ruff check desktop/dialogs_filter.py genizah_translations.py tests/test_libfilter_desktop.py
All checks passed!
```

## Deviations from Plan

None — plan executed exactly as written.

## Known Stubs

None. All new functionality is fully wired.

## Threat Flags

None. This is a desktop-only GUI change with no network endpoints, auth paths, or schema changes.

## Known Human-Needed Item

Live PyQt6 render-smoke (open Catalog tab → library filter button → type in search box and watch rows filter; click By count / A-Z and watch rows reorder; confirm checked-then-hidden library still applies on Apply) is `human_needed` — Qt cannot be rendered headless in CI per MEMORY.md. Record in 131-HUMAN-UAT.md UAT test #7.

## Self-Check: PASSED

- `desktop/dialogs_filter.py` modified (FOUND)
- `genizah_translations.py` modified (FOUND)
- `tests/test_libfilter_desktop.py` modified (FOUND)
- Commit `57344637` exists (FOUND)
- Commit `0104900b` exists (FOUND)
- 33 tests pass headless (VERIFIED)
- Ruff clean (VERIFIED)
