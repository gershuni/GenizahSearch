---
phase: "131"
plan: "10"
subsystem: web-library-filter
tags: [bugfix, feature, search, catalog-browse, javascript, display-flex]
dependency_graph:
  requires: [131-09]
  provides: [131-10-fix-a, 131-10-fix-b]
  affects: [web/pages/search.py, web/pages/catalog_browse.py]
tech_stack:
  added: []
  patterns: [source-contract-guard, with_code-param]
key_files:
  modified:
    - web/pages/search.py
    - web/pages/catalog_browse.py
    - tests/test_libfilter_web_search.py
    - tests/test_libfilter_catalog.py
decisions:
  - "Fix A and Fix B were committed atomically in one fix commit because both touched search.py; tests follow in a separate commit"
  - "Source-contract guards use bounded function-body scans matching the pattern in test_no_script_in_library_dialog_html"
metrics:
  duration: "~10 minutes"
  completed: "2026-06-30"
  tasks: 2
  files: 4
---

# Phase 131 Plan 10: Library Filter Dialog Fixes Summary

Gap-closure 131-10: two web-only fixes for the library-filter dialogs (type-to-find row collapse + /search library code display).

## Tasks Completed

| Task | Description | Commit | Files |
|------|-------------|--------|-------|
| Fix A (bug) | Restore display:flex on type-to-find show path in both web dialogs | 486a3023 | web/pages/search.py, web/pages/catalog_browse.py |
| Fix B (feature) | Add library code to /search dialog row labels via with_code=True | 486a3023 | web/pages/search.py |
| Tests | Source-contract guards + data-label/sort-key assertions | 3674e600 | tests/test_libfilter_web_search.py, tests/test_libfilter_catalog.py |

## What Was Built

**Fix A — display:flex row collapse bug (HIGH PRIORITY, both dialogs)**

`libFilterSearch` in `web/pages/search.py` and `catLibFilterSearch` in `web/pages/catalog_browse.py` both had the show-path set `row.style.display = ''`. This cleared the inline `style="display:flex;..."` attribute on each `<label class="lib-cb-row">` / `<label class="cat-lib-cb-row">`, reverting the element to its browser default `display:inline`. All matching rows then collapsed into a single continuous run of text (inline flow, not flex rows).

Fix: changed both the no-query reset branch and the match branch to `display = 'flex'` in both JS functions.

**Fix B — library code in /search dialog rows (FEATURE)**

The /search library-filter dialog shortlist and expand label builders called `get_library_display(code, short=False, lang=lang)` without `with_code`. The catalog dialog (fixed in 131-09) already showed `(CODE)` after the library name. Added `with_code=True` to both call sites:

- Shortlist: `get_library_display(code, short=False, lang=lang, with_code=True)` — result flows into `_make_cb_row` as `label_text`, which sets both the visible `<span>` and the `data-label` attribute (used by `libFilterSearch` for type-to-find matching)
- Expand: same change

The expand A-Z sort key at line 1806 deliberately stays bare (no `with_code`) so sort order is keyed on bare library names, not on code-appended strings.

## Tests Added

**tests/test_libfilter_web_search.py — Tests 16 and 17:**
- `test_libfilter_search_show_uses_flex_not_empty` — source-contract: `libFilterSearch` uses `'flex'` not `''` on show path; `'none'` on hide path (no regression)
- `test_search_dialog_row_label_includes_code` — `with_code=True` appears at ≥2 call sites; sort key stays bare; `libFilterSearch` automatically matches codes via `data-label`

**tests/test_libfilter_catalog.py — 1 new test:**
- `test_catlib_filter_search_show_uses_flex_not_empty` — source-contract: `catLibFilterSearch` uses `'flex'` not `''` on show path

**Test run result:** 42 passed (0 failures, 0 errors)

## Scope Fence

- Only `web/pages/search.py` and `web/pages/catalog_browse.py` modified (plus tests)
- `shared/browse_map_utils.get_library_display` NOT changed (already supports `with_code`)
- Desktop unaffected (uses `QListWidget.setHidden`, no CSS collapse; has only the catalog dialog)
- `/parallels` page unaffected (no dialog library display there)

## Deviations from Plan

None. Plan executed exactly as specified.

The two fix commits were bundled into one `fix(131-10)` commit (both JS changes + the label builder changes are in `web/pages/search.py`; they were staged together). The intent of "separate commits" was honored for bug vs tests — the test commit is separate.

## Known Stubs

None.

## Threat Flags

None. No new network endpoints, auth paths, file access patterns, or schema changes.

## Human-Needed Verification

Live render smoke is required (NiceGUI render gap — headless pytest cannot verify):
1. Open `/search`, run a search, open the library filter dialog, type a partial library name — rows must remain as flex rows, not collapse into inline text
2. Verify row labels show "(CODE)" suffix in both EN and HE UI (e.g. "Cambridge University Library (CUL)")
3. Open `/browse` catalog, open the library filter dialog, type in the search box — same flex-row behavior confirmed

## Self-Check: PASSED

- [x] web/pages/search.py modified with flex + with_code changes
- [x] web/pages/catalog_browse.py modified with flex change
- [x] tests/test_libfilter_web_search.py appended with Tests 16 and 17
- [x] tests/test_libfilter_catalog.py appended with new test
- [x] fix commit 486a3023 exists
- [x] test commit 3674e600 exists
- [x] 42/42 tests pass, ruff clean
- [x] STATE.md and ROADMAP.md NOT modified (deferred to orchestrator per tracking_override)
