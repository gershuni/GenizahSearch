---
phase: "131"
plan: "09"
subsystem: library-filter-dialog
tags: [gap-closure, catalog, desktop, web, library-filter]
dependency_graph:
  requires: [131-08]
  provides: [131-09]
  affects: [desktop/dialogs_filter.py, web/pages/catalog_browse.py]
tech_stack:
  added: []
  patterns: [always-on with_code, language-agnostic code display]
key_files:
  modified:
    - desktop/dialogs_filter.py
    - web/pages/catalog_browse.py
    - tests/test_libfilter_desktop.py
    - tests/test_libfilter_catalog.py
decisions:
  - "Drop `import genizah_core as _gc` in LibraryFilterDialog.__init__ since _gc.CURRENT_LANG was its only use"
  - "with_code=True replaces both language-gated call sites; shared helper default stays OFF"
metrics:
  duration: "~10m"
  completed: "2026-06-30"
  tasks_completed: 1
  files_changed: 4
---

# Phase 131 Plan 09: Library Code in English UI Catalog Dialog Summary

Gap closure extending 131-08's with_code feature from Hebrew-only to both languages (always-on) in the catalog library filter dialog on desktop and web.

## What Was Built

131-08 added `with_code=(_lang == 'he')` at two web call sites and `self._with_code = (_gc.CURRENT_LANG == 'he')` on desktop. This plan flips both gates to always-on so English-UI users also see "Cambridge University Library (CUL)" in the library filter dialog rows and can type 'CUL' in the type-to-find search box.

## Changes

### desktop/dialogs_filter.py
- Removed `import genizah_core as _gc` (was only used for the language gate)
- Changed `self._with_code = (_gc.CURRENT_LANG == 'he')` to `self._with_code = True`
- Sort keys in `__init__` and `_repopulate` unchanged (bare `get_library_display` without `with_code`, so A-Z order is keyed on the bare name)

### web/pages/catalog_browse.py
- Shortlist builder: `with_code=(_lang == 'he')` → `with_code=True`
- Expand section builder: `with_code=(_lang == 'he')` → `with_code=True`
- Expand A-Z sort key unchanged: `get_library_display(c, short=False, lang=_lang)` (no `with_code`)

### tests/test_libfilter_desktop.py
- Renamed `test_en_label_unchanged` → `test_en_label_contains_code`; flipped assertion from "label equals bare EN name" to "label contains '(CUL)'"
- Added `test_en_search_matches_code`: EN-UI typing 'CUL' keeps Cambridge row visible (mirrors HE counterpart `test_he_search_matches_code`)
- Hebrew-lang tests (a, b, d, e) unchanged
- `test_get_library_display_default_off_en` / `test_get_library_display_default_off_he` unchanged (shared helper default remains OFF)

### tests/test_libfilter_catalog.py
- `test_web_catalog_shortlist_label_builder_passes_with_code`: asserts `with_code=True` present and `with_code=(_lang == 'he')` absent; expand sort key assertion unchanged

## Scope Boundary Respected

Only the two catalog filter-dialog call sites were changed. The shared `get_library_display` helper remains `with_code=False` by default. All other library name display sites (search results column, browse page, /search & /parallels dialogs) are unaffected.

## Deviations from Plan

None — plan executed exactly as specified.

## Test Results

63/63 tests pass. Ruff clean on all 4 touched files.

## Self-Check: PASSED

- `desktop/dialogs_filter.py` modified: confirmed
- `web/pages/catalog_browse.py` modified: confirmed
- `tests/test_libfilter_desktop.py` modified: confirmed
- `tests/test_libfilter_catalog.py` modified: confirmed
- Commit `bf53c31d` (feat): confirmed
- Commit `42570b0c` (test): confirmed
