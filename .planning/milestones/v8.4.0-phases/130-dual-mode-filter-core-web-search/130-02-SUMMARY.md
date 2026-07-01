---
phase: 130-dual-mode-filter-core-web-search
plan: "02"
subsystem: web-search-filter
tags: [library-filter, dual-mode, safe_storage, dialog, i18n, DMF]
dependency_graph:
  requires: [130-01]
  provides:
    - dual-mode restore+migration (legacy list -> Show-only; dict shape read-back)
    - mode-branch _apply_library_filter (Show-only=in, Hide=not-in, empty=pass-through)
    - dict persist shape for search_library_filter via both writers
    - redesigned dialog (mode toggle + LOCAL-excluded shortlist + expand-all + text search)
    - 3-state button label (Neutral / Showing N/total / Hiding N)
    - bilingual translation keys for all new DMF strings
  affects:
    - web/pages/search.py
    - web/components/filter_panel.py
    - genizah_translations.py
tech_stack:
  added: []
  patterns:
    - dual-mode filter restore/sanitize (list vs dict branch, D-06 legacy migration)
    - mode-branch filter function (mirroring _apply_pgp_filter)
    - show-all normalization (Show-only+empty -> neutral hide/[])
    - dict persist shape {'mode', 'codes'} via persist_value chokepoint
    - mode-aware JS Apply-enable (data-libmode attribute, libFilterSetMode)
    - single ui.html checkbox block (never per-item ui.checkbox — avoids 7-19s open)
    - page-level JS only (no <script> inside ui.html, BUG-B)
key_files:
  created: []
  modified:
    - web/pages/search.py
    - web/components/filter_panel.py
    - genizah_translations.py
decisions:
  - "show-all has ONE persisted representation: neutral {'mode':'hide','codes':[]} — Show-only+empty normalizes to this at Apply time (never persists 'show_only'+'[]')"
  - "Restore detects legacy plain list (v8.3.0) and migrates non-empty to Show-only, empty to neutral Hide/[] (D-06)"
  - "Empty Hide-set stays applyable (Apply enabled); empty Show-only blocks Apply — mode-aware libFilterUpdateApply reads data-libmode attr"
  - "libFilterSetMode(cid, mode) sets data-libmode + resets all checkboxes (D-04 transient in-dialog reset)"
  - "Restore-sync guard `if search_state.library_filter:` is mode-agnostic (checks non-empty set) — correct for both modes; normalized show-all (hide/[]) stays neutral with no button update"
  - "3-state button: Neutral=outline-primary, Show-only-active=filled-negative, Hide-active=filled-deep-orange (distinct colors per mode)"
  - "4 new bilingual translation keys: Hiding/Show only selected/Hide selected/Search libraries..."
metrics:
  duration_minutes: 8
  completed_date: "2026-06-30"
  tasks_completed: 3
  tasks_total: 3
  files_changed: 3
---

# Phase 130 Plan 02: Dual-Mode Library Filter Runtime (Web Search) Summary

**One-liner:** Implemented full dual-mode (Show-only/Hide) library filter on web `/search` — mode-aware restore with v8.3.0 legacy migration, mode-branch filter, redesigned dialog with mode toggle + LOCAL-excluded count-shortlist + expandable all-canonical section + text search, 3-state button label, dict persist shape from both writers, and 4 new bilingual translation keys.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Mode-aware restore + legacy migration + mode-branch filter + dict persist (both writers) | f2926180 | web/pages/search.py, web/components/filter_panel.py |
| 2 | Redesign _open_library_filter_dialog — mode toggle + LOCAL-excluded count-shortlist + expand-all + text search | 60730f2e | web/pages/search.py |
| 3 | 3-state button label + new bilingual keys + restore-sync + page-level JS | 925efd12 | web/pages/search.py, genizah_translations.py |

## What Was Built

### Task 1 — Mode-aware restore + legacy migration + dual-mode filter + dict persist

**web/pages/search.py restore/sanitize block (~188-210):**
- Replaced the single-line `_lib0` restore with a 3-branch mode-aware restore:
  - `isinstance(_lib_raw, list)`: D-06 legacy migration — non-empty list -> `mode='show_only'`, codes=sanitized list; empty list -> `mode='hide'`, codes=[]
  - `isinstance(_lib_raw, dict)`: new dict shape — reads `mode` (validated in `{'show_only','hide'}`) and `codes` (sanitized against `LIBRARY_CODES` and `!= 'LOCAL'`)
  - else: fresh default `mode='hide'`, codes=[] (D-05)
- Both branches use `[c for c in ... if c in LIBRARY_CODES and c != 'LOCAL']` (DMF-10)

**web/pages/search.py `_apply_library_filter` (rewritten):**
- Added mode branch: reads `search_state.library_mode` via `getattr(..., 'hide')`
- Show-only: non-empty codes keeps `in` set; empty codes returns input unchanged (D-08)
- Hide: non-empty codes keeps `not in` set; empty codes returns input unchanged (D-05)
- Fully-populated Hide set returns [] → clean 0-results render downstream (DMF-06)

**web/pages/search.py `apply_library_filter` dialog handler:**
- HIGH-3: sanitizes JS-returned codes with `[c for c in checked if c in LIBRARY_CODES and c != 'LOCAL']` BEFORE any mode/selection mapping
- Show-all normalization: Show-only+empty codes -> `search_state.library_mode='hide'`, `library_filter=[]` (neutral representation)
- Persists `{'mode': search_state.library_mode, 'codes': search_state.library_filter}` via `persist_value` (dict shape, D-09)

**web/pages/search.py New-Search reset:**
- Added `search_state.library_mode = 'hide'` alongside `library_filter = []` (D-04/DMF)

**web/components/filter_panel.py `consume_incoming_filters` (browse->search handoff):**
- HIGH-1: imports `LIBRARY_CODES` from `shared.browse_map_utils` (local import matching file style)
- Sanitizes codes: `[str(c) for c in incoming['library_filter'] if c and str(c) in _LIBRARY_CODES and str(c) != 'LOCAL']`
- Sets `state.library_mode = 'show_only'` (browse intent is always "show only these")
- Persists `{'mode': 'show_only', 'codes': _lib_codes}` — NOT the old bare list (T-130-02-06)

### Task 2 — Redesigned dialog

**_open_library_filter_dialog (full redesign in place):**

Mode toggle (`ui.toggle`) at top of dialog:
- Options: `{'show_only': tr('Show only selected'), 'hide': tr('Hide selected')}`
- Initialized from `search_state.library_mode` via `current_mode[0]` closure
- On-change calls `libFilterSetMode(cid, mode)` JS which sets `data-libmode` attr, resets all checkboxes (D-04), re-syncs Apply-enable

Text-search input (`ui.input`):
- Placeholder: `tr('Search libraries...')`
- On-change wires to `libFilterSearch(cid, query)` JS that shows/hides `.lib-cb-row` elements by `data-label` match

Count shortlist:
- `[c for c in facets if c in LIBRARY_CODES and c != 'LOCAL']` sorted by `-facets[c]` (count desc) — HIGH-2/DMF-10
- Labels: `"Display Name (count)"`
- Initial checked state reflects current mode and filter

Expandable section:
- `[c for c in LIBRARY_CODES if c != 'LOCAL' and c not in shortlist_set]` sorted A-Z by display name
- Wrapped in `<details>/<summary>` HTML with `tr('All libraries')` summary
- Same `c != 'LOCAL'` guard keeps LOCAL excluded by construction (DMF-10)

Single `ui.html()` block with all checkboxes (`.lib-cb-row` + `.lib-cb`):
- `data-label` attr per row for JS text-search
- `data-code` attr for `libFilterGetChecked`
- `data-libmode` on the container for mode-aware `libFilterUpdateApply`
- BUG-B: NO `<script>` inside `ui.html()` — all JS at page level via `add_head_html`

Updated JS block (`libFilterUpdateApply`):
- Reads `data-libmode` attribute from container
- Disables Apply only when `mode === 'show_only' && n === 0` — hide mode with zero checked leaves Apply ENABLED (D-08/MEDIUM)

New JS helpers added at page level:
- `libFilterSetMode(cid, mode)`: sets `data-libmode`, resets checkboxes, calls `libFilterUpdateApply`
- `libFilterSearch(cid, query)`: shows/hides `.lib-cb-row` by `data-label.toLowerCase().indexOf(q)` match

Apply handler reads mode from `current_mode[0]` closure, routes to Show-only (with all-checked->[] normalization and show-all neutral path) or Hide (empty set allowed) branches.

### Task 3 — 3-state button + bilingual keys

**_update_library_btn (3-state):**
- Reads `mode = getattr(search_state, 'library_mode', 'hide')` and `codes = set(search_state.library_filter)`
- Show-only active: `codes` non-empty AND not all in-result libraries shown → `f"{tr('Showing')} {shown}/{total}"`, filled negative
- Hide active: `mode=='hide'` AND `codes` non-empty → `f"{tr('Hiding')} {len(codes)}"`, filled deep-orange
- Neutral (everything else, incl. empty code set in any mode): `tr('Filter by library')`, outline primary
- Each branch calls `library_filter_btn.props(remove='color')` / `props(remove='color outline')` before setting new color (mirrors `_update_printed_filter_btn` pattern to prevent stuck color)

**genizah_translations.py — 4 new keys:**
- `"Hiding"`: `"מסתיר"`
- `"Show only selected"`: `"הצג רק נבחרות"`
- `"Hide selected"`: `"הסתר נבחרות"`
- `"Search libraries..."`: `"חיפוש ספריות..."`
- Pre-existing `"Showing"`, `"Filter by library"`, `"Filter by Library"` entries left untouched

**Restore-sync:** existing `if search_state.library_filter:` guard is mode-agnostic — fires correctly for non-empty Hide set; normalized show-all (hide/[]) stays neutral (correct).

## Verification Results

- `python -c "import ast; ast.parse(open('web/pages/search.py',...).read()); ..."` — parses OK (no syntax error)
- `pytest tests/test_no_raw_storage_access.py tests/test_web_library_options_no_local.py tests/test_phase_97_invariants.py` — 13/13 passed
- `python -m ruff check web/pages/search.py web/components/filter_panel.py genizah_translations.py` — all checks passed
- Mode-aware JS assertions (data-libmode, libFilterSetMode, Show-only Apply-disable) — verified via Python AST inspection
- No `<script>` inside `ui.html()` calls — confirmed via AST scan of all `ui.html` string arguments

## Deviations from Plan

**[Rule 2 - Auto-add] `_make_cb_row` helper extracted from inline HTML building**
- Found during: Task 2
- Issue: Checkbox row HTML was duplicated for shortlist and expand sections
- Fix: Extracted `_make_cb_row(code, label_text, checked)` pure helper inside dialog function to DRY up the HTML construction without changing behavior
- Files modified: web/pages/search.py
- Commit: 60730f2e

None other — plan executed as written for all 3 tasks. Both trust-boundary mitigations (HIGH-1 browse->search handoff sanitize + mode stamp; HIGH-3 Apply handler sanitize) were implemented as specified. Show-all normalization (MEDIUM) implemented at both the dialog Apply and the new dialog Apply handler.

## Known Stubs

None. All filter behavior is wired end-to-end: restore path reads from storage, mode-branch filter applies to results, Apply handler writes back to storage, button label reflects mode. The expand section lists all canonical libraries (minus LOCAL) for completeness even when none are in results.

## Threat Surface Scan

No new network endpoints, auth paths, or schema changes. The two trust boundaries identified in the plan's threat register have been mitigated:
- T-130-02-01/T-130-02-02: restore path and Apply handler sanitize codes against LIBRARY_CODES + 'LOCAL'
- T-130-02-04: all reads via `_safe_get`, all writes via `persist_value` (no raw `app.storage.user`)
- T-130-02-05: no `<script>` inside `ui.html()` (BUG-B)
- T-130-02-06: `consume_incoming_filters` now stamps `mode='show_only'` and persists dict shape

## Self-Check: PASSED

- `web/pages/search.py` — modified (exists, verified)
- `web/components/filter_panel.py` — modified (exists, verified)
- `genizah_translations.py` — modified (exists, verified)
- Commit `f2926180` — exists in git log (Task 1)
- Commit `60730f2e` — exists in git log (Task 2)
- Commit `925efd12` — exists in git log (Task 3)
- All 13 guard tests pass
- ruff clean on all 3 touched files
