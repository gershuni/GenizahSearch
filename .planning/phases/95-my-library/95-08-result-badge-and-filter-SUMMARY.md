---
phase: 95-my-library
plan: 08
subsystem: ui
tags: [desktop, pyqt6, local-search, filter, badge, browse]

# Dependency graph
requires:
  - phase: 95-07
    provides: MyLibraryTab + LocalIndexer with get_filepath; LOCAL hits flowing into search results

provides:
  - COL_SRC LOCAL badge (blue #3498db) in main search table (D-11)
  - comp_col_src column (index 8) added to comp_tree for Composition/Parallels (D-12)
  - Three-state LOCAL filter button on Search/Composition/Parallels surfaces (REQ-6/D-10/D-39)
  - LOCAL hit double-click opens Browse panel in text-only mode + Open file button (D-27/D-28)
  - Pre-search corpus-scope dropdown (Genizah/Local/ALL) with Genizah as UI default
  - ResultDialog for LOCAL double-click (not Browse redirect); Library=parent folder, Shelfmark=filename

affects: [95-09, future composition-search surfaces]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - Three-state filter button pattern (mirrors Phase 93 PGP filter): all -> only_local -> no_local -> all
    - Static AST cascade scanner (mirrors tests/test_pgp_filter_cascade.py) enforcing _apply_local_filter in both desktop cascade joinpoints
    - Session-JSON persistence for corpus_scope (not QSettings) — avoids Qt registry pollution

key-files:
  created:
    - tests/test_local_filter_cascade.py
    - tests/test_local_filter_persistence.py
    - tests/test_corpus_scope_routing.py
  modified:
    - genizah_app.py

key-decisions:
  - "Engine API default corpus_scope='all' preserved for backward compat; only UI default changed to 'genizah'"
  - "LOCAL double-click opens ResultDialog (not Browse panel redirect) per smoke-test user direction"
  - "Library column = parent folder path for LOCAL hits; Shelfmark column = filename"
  - "Pre-search corpus dropdown lives only on Search tab; Composition/Parallels use the post-search 3-state filter only (symmetry extension deferred)"
  - "Corpus scope persisted in session JSON (not QSettings) to keep per-session state separate from app-wide prefs"

patterns-established:
  - "Pattern: _apply_local_filter hooked into both _apply_results_table_filters AND _apply_comp_tree_filters (W4 resolved)"
  - "Pattern: D-10 P1 NO-OP — when filter state is only_local/no_local but zero LOCAL hits exist, show all results + inactive chip"

requirements-completed: [REQ-6, REQ-7]

# Metrics
duration: multi-session
completed: 2026-05-21
---

# Phase 95 Plan 08: Result Badge and Filter Summary

**LOCAL hits get blue COL_SRC badge, three-state filter buttons on all result surfaces, Browse text-only mode with Open File, and a pre-search corpus-scope dropdown defaulting to Genizah**

## Performance

- **Duration:** multi-session (Tasks 1-3 + smoke + patches)
- **Started:** 2026-05-21
- **Completed:** 2026-05-21
- **Tasks:** 4 (3 auto + 1 human checkpoint) + 3 post-smoke patches
- **Files modified:** genizah_app.py + 3 test files

## Accomplishments

- COL_SRC LOCAL badge in blue `#3498db` in main search table; visibility rule extended to OR-in LOCAL presence (D-11)
- New `comp_col_src = 8` column added to comp_tree; Parallels inherits automatically via shared tree (D-12 audit resolution)
- Three-state LOCAL filter button (`Filter Local` / `Only Local` / `No Local`) on Search, Composition Search, and Parallels surfaces; per-surface QSettings persistence; D-10 P1 NO-OP chip when no LOCAL hits present
- LOCAL double-click opens ResultDialog (not Browse redirect) with Open File button; Library = parent folder, Shelfmark = filename
- Pre-search corpus-scope dropdown (Genizah / Local / ALL) with Genizah as default; persisted in session JSON
- Static AST cascade guard `tests/test_local_filter_cascade.py` enforces `_apply_local_filter` in both `_apply_results_table_filters` AND `_apply_comp_tree_filters`

## Task Commits

Each task was committed atomically:

1. **Task 1: COL_SRC LOCAL badge + comp_col_src column (D-11 + D-12)** - `b5d0c0a2` (feat)
2. **Task 2: Three-state LOCAL filter button + cascade hooks (REQ-6/D-10/D-39)** - `a513ed66` (feat)
3. **Task 3: LOCAL hit click → Browse text-only + Open file button (D-27 + D-28)** - `6acd6a0e` (feat)
4. **Task 4: Manual smoke — ALL sections pass** — human checkpoint approved
5. **Post-smoke patch (items 1/2/3/4)** - `d8106609` (fix)
   - Item 1: LOCAL double-click → ResultDialog (not Browse); Open File button in dialog
   - Item 2: Pre-search corpus dropdown (ALL / Genizah / Local) added to Search tab
   - Item 3: Library column = parent/folder path for LOCAL hits
   - Item 4: Shelfmark column = filename for LOCAL hits
6. **QSettings → session JSON hotfix** - `f61d4c9e` (fix)
   - corpus_scope was being saved/restored via QSettings (app-wide); switched to session JSON for per-session scope
7. **UX reorder: Genizah/Local/ALL, Genizah default** - `4ef9eada` (fix)
   - Reordered dropdown from ALL/Genizah/Local to Genizah/Local/ALL
   - Changed UI default `_search_corpus_scope` from `'all'` to `'genizah'`
   - Engine API default (`execute_search corpus_scope` param) unchanged as `'all'` for backward compat

## Files Created/Modified

- `genizah_app.py` — COL_SRC badge, comp_col_src column, three-state filter buttons, _apply_local_filter, _set_browse_image_pane_visible helper, corpus-scope dropdown, ResultDialog for LOCAL, Open File button
- `tests/test_local_filter_cascade.py` — static AST scanner asserting cascade discipline; filter cycle test; NO-OP test
- `tests/test_local_filter_persistence.py` — QSettings 3-key persistence test
- `tests/test_corpus_scope_routing.py` — corpus_scope routing regression guard (genizah/local/all routing)

## Decisions Made

- **Engine API default unchanged:** `SearchEngine.execute_search(corpus_scope='all')` stays `'all'` — changing it would break all callers that rely on the current behavior. Only the UI default (`_search_corpus_scope`) was changed to `'genizah'`.
- **LOCAL double-click → ResultDialog:** User smoke-test directed that LOCAL hits should open a ResultDialog (not redirect to the Browse panel), consistent with how the file content is best surfaced in a dialog with an Open File button.
- **Composition/Parallels dropdown deferred:** Pre-search corpus dropdown is only on the Search tab. Composition Search and Parallels use only the post-search 3-state filter. Extending the dropdown to those surfaces is a potential follow-up if symmetry is desired.
- **Session JSON for corpus_scope:** QSettings was incorrect because it persists across sessions app-wide; session JSON is the right chokepoint for per-session search state (matches the Phase 88 session-separation architecture).

## Deviations from Plan

### Post-checkpoint Patches (user smoke feedback, not plan deviations)

**1. [Rule 1 - Bug] LOCAL double-click opened Browse panel instead of ResultDialog**
- **Found during:** Manual smoke Task 4 (Section E)
- **Issue:** Task 3 wired LOCAL clicks to `_open_local_browse` which redirected to Browse panel; user directed ResultDialog is the correct target for LOCAL hits
- **Fix:** Smoke-patch `d8106609` — wired LOCAL double-click to ResultDialog; added Open File button inside the dialog
- **Files modified:** genizah_app.py
- **Committed in:** `d8106609`

**2. [Rule 2 - Missing Critical] Pre-search corpus dropdown not in original plan**
- **Found during:** Manual smoke Task 4 — user requested a pre-search corpus selector
- **Fix:** Added QComboBox to Search tab row1 with ALL/Genizah/Local options (later reordered)
- **Files modified:** genizah_app.py
- **Committed in:** `d8106609`

**3. [Rule 1 - Bug] Library/Shelfmark columns showed wrong data for LOCAL hits**
- **Found during:** Manual smoke Task 4 (Section A)
- **Issue:** Library column was showing library_code; Shelfmark was showing sys_id; for LOCAL hits these should be parent folder and filename
- **Fix:** Smoke-patch `d8106609` — LOCAL rows write parent folder to Library column, filename to Shelfmark column
- **Files modified:** genizah_app.py
- **Committed in:** `d8106609`

**4. [Rule 1 - Bug] corpus_scope persistence used QSettings instead of session JSON**
- **Found during:** Post-smoke testing
- **Issue:** QSettings persists app-wide across sessions; corpus_scope is per-session search state
- **Fix:** Hotfix `f61d4c9e` — switched to session JSON (`_save_session` / `_restore_session`)
- **Files modified:** genizah_app.py
- **Committed in:** `f61d4c9e`

---

**Total deviations:** 4 post-smoke patches (all auto-fixed or user-directed)
**Impact on plan:** All patches necessary for correct UX behavior. No scope creep beyond user smoke direction.

## D-12 Audit Resolution

The planner's pre-execution audit confirmed:
- `self.comp_tree` (Composition Search / Parallels shared tree) had **8 columns** at plan time (no Src column)
- Plan 08 added `comp_col_src = 8` as a 9th column (after Printed at index 7)
- Parallels routes results via `send_result_to_composition` into the same `comp_tree` — inherits the new Src column automatically with no separate column work

## W4 Cascade Joinpoint Resolution

Desktop has **no** `_apply_pgp_filter` (that is web-only). The two cascade joinpoints on desktop are:
- `_apply_results_table_filters` — master cascade for the main search results table
- `_apply_comp_tree_filters` — master cascade for comp_tree (covers Composition + Parallels)

Both functions were modified to call `_apply_local_filter`. The static AST guard in `tests/test_local_filter_cascade.py` enforces this permanently.

## I15 "No Image" Browse Pattern Resolution

The existing "no image" pattern is `self.browse_viewer.setVisible(False)` driven by `btn_b_toggle_img.isChecked()`. Plan 08 added `_set_browse_image_pane_visible(visible: bool)` as a thin wrapper keeping the toolbar toggle button in sync with the pane. After the post-smoke patch (item 1), this helper is called from `_open_local_browse` but LOCAL hits primarily open a ResultDialog; the Browse text-only path remains available for future use.

## Smoke Test Verdict

**ALL PASS** — user smoke-tested sections A (badge), A2 (comp_col_src), B (filter cycling), C (filter persistence), D (D-10 P1 NO-OP chip), E (Browse + Open file), and the new pre-search corpus dropdown.

Final UX reorder (Genizah/Local/ALL, Genizah default) confirmed by user after smoke.

## Deferred by Design

- **Pre-search corpus dropdown on Composition Search / Parallels surfaces:** Only the Search tab has the corpus-scope dropdown. Composition Search and Parallels have the post-search 3-state LOCAL filter only. Extending the pre-search dropdown to those surfaces is a potential follow-up if symmetry is desired — not in scope for Phase 95.
- **LOCAL manuscripts in Browse tab primary listing:** Genizah Browse (the Browse tab's main listing) remains Genizah-only by design (D-29). LOCAL manuscripts are accessible only via Search results.

## Self-Check

- `genizah_app.py` modified: confirmed (multiple edits committed)
- `tests/test_corpus_scope_routing.py` updated: confirmed (`4ef9eada`)
- All 21 relevant tests pass: confirmed (`pytest` output: `21 passed in 1.79s`)
- Commits `b5d0c0a2`, `a513ed66`, `6acd6a0e`, `d8106609`, `f61d4c9e`, `4ef9eada` all present in git log

## Self-Check: PASSED

All claimed commits exist. Test suite passes. No STATE.md or ROADMAP.md edits made (per objective).

---
*Phase: 95-my-library*
*Completed: 2026-05-21*
