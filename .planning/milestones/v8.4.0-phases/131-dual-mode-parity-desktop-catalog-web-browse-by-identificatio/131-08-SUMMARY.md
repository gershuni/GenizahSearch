---
phase: 131-dual-mode-parity-desktop-catalog-web-browse-by-identificatio
plan: 08
subsystem: ui
tags: [library-filter, hebrew, i18n, catalog, browse, desktop, web, nicegui, pyqt6]

# Dependency graph
requires:
  - phase: 131-dual-mode-parity-desktop-catalog-web-browse-by-identificatio
    provides: "Plans 01-07: LibraryFilterDialog, catalog browse dialog, facets, search/sort"

provides:
  - "get_library_display with_code=True param appends ' (CODE)' after the resolved name"
  - "Desktop LibraryFilterDialog Hebrew-UI rows show 'Hebrew name (CUL)' — type-to-find matches the code"
  - "Web /catalog dialog Hebrew-UI shortlist + expand rows carry the code in both visible label and data-label"
  - "A-Z / by-count sort order unchanged — sort keys stay on bare name without with_code"
  - "English UI unchanged — code not appended when lang != 'he'"

affects:
  - 131-HUMAN-UAT (UAT test #9 now codeable)
  - any future phase using get_library_display (default OFF, zero impact)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Opt-in default-OFF param on shared helper: with_code=False keeps all existing callers byte-identical"
    - "Live genizah_core.CURRENT_LANG read (not stale module-level snapshot) for dialog-open-time lang gate"
    - "TDD Red/Green per task: tests written before implementation, both files pass headless"

key-files:
  created: []
  modified:
    - shared/browse_map_utils.py
    - desktop/dialogs_filter.py
    - web/pages/catalog_browse.py
    - tests/test_libfilter_desktop.py
    - tests/test_libfilter_catalog.py

key-decisions:
  - "with_code param default OFF so ALL existing callers are byte-identical — no audit of call sites needed"
  - "Desktop: import genizah_core as _gc; self._with_code = (_gc.CURRENT_LANG == 'he') — reads live value at dialog-open time, not stale line-14 snapshot"
  - "Sort keys stay on bare name (no with_code) in both apps — appending code at END of label keeps A-Z stable by prefix"
  - "Web: pass code-bearing label into existing _make_cat_cb_row so data-label gets the code automatically; no JS change"
  - "A-Z order test checks he-dialog-order vs expected bare-name sort (not he vs en, which differ by language)"

patterns-established:
  - "Catalog dialog row label = bare_name [+ ' (CODE)' in he] [+ ' (count)'] — code between name and count"

requirements-completed: [DMF-07]

# Metrics
duration: 22min
completed: 2026-06-30
---

# Phase 131 Plan 08: Library Code in Hebrew UI Filter Rows Summary

**Opt-in `with_code` param on `get_library_display` appends ' (CUL)' in Hebrew UI catalog dialogs (desktop + web) so type-to-find matches the English library code; A-Z sort unchanged via bare-name sort keys.**

## Performance

- **Duration:** ~22 min
- **Started:** 2026-06-30T~08:00Z
- **Completed:** 2026-06-30
- **Tasks:** 4 (Tasks 1-2 TDD Red/Green; Task 3 direct; Task 4 integrated into Tasks 1-2 commits)
- **Files modified:** 5

## Accomplishments

- `get_library_display` gains a 4th trailing param `with_code: bool = False`; every existing caller is byte-identical (no call site audit needed)
- Desktop `LibraryFilterDialog._populate_rows` reads the live `genizah_core.CURRENT_LANG` at dialog-open time (not the stale module-level snapshot) and appends the code in Hebrew UI
- Web `/catalog` dialog shortlist + expand row builders pass `with_code=(_lang == 'he')` so `data-label` contains the lowercased code and `catLibFilterSearch` matches 'cul'/'jts'
- Both apps' sort sites (init + `_repopulate` on desktop; expand sort lambda on web) stay on the bare name — A-Z and by-count order unchanged
- 62 tests pass headless (38 desktop + 24 catalog); ruff clean

## Task Commits

1. **Task 1: Add opt-in with_code param to shared get_library_display** - `a9d642bd` (feat + test TDD)
2. **Task 2: Desktop LibraryFilterDialog — append code in Hebrew UI** - `cfd15930` (feat + test TDD)
3. **Task 3: Web /catalog dialog — append code to label + data-label** - `421d1c70` (feat)

## Files Created/Modified

- `shared/browse_map_utils.py` — added `with_code: bool = False` trailing param to `get_library_display`; appends ` ({code})` when `with_code=True` and `short=False`
- `desktop/dialogs_filter.py` — `LibraryFilterDialog.__init__` reads live `_gc.CURRENT_LANG`, sets `self._with_code`; `_populate_rows` passes `with_code=self._with_code`; sort keys unchanged
- `web/pages/catalog_browse.py` — shortlist + expand label builders pass `with_code=(_lang == 'he')`; expand sort key left without `with_code`
- `tests/test_libfilter_desktop.py` — 5 new tests: he label contains code, he+facets, en unchanged, he search matches code, A-Z stability
- `tests/test_libfilter_catalog.py` — 7 new tests: shared with_code unit (he/en/default-off/short-wins/empty), web AST scan asserting call-site + sort-key correctness

## Decisions Made

- `with_code` defaults to `False` so all existing callers (search-results Library column, browse, /search dialog, export) are unaffected — zero call site audit required (T-131-08-01 mitigated)
- Live `genizah_core.CURRENT_LANG` read via `import genizah_core as _gc` in `__init__`, not the stale `CURRENT_LANG` imported at module load time (line 14) which is a snapshot from Python startup
- A-Z order test compares dialog code sequence against `sorted(all_codes, key=lambda c: get_library_display(c, short=False, lang='he'))` — this is the correct invariant (not he-vs-en, which differ by language choice)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] A-Z stability test fixed to compare within same language**
- **Found during:** Task 2 test writing
- **Issue:** Initial test (e) compared he-order vs en-order, but those differ because sort keys use Hebrew vs English names — the invariant is that the code-append does NOT change the within-language order
- **Fix:** Test now compares dialog code sequence (built with `with_code=True` for labels, bare-name sort keys) against `sorted(all_codes, key=bare_he_name)` — proves the sort key is unaffected by the code-append
- **Files modified:** tests/test_libfilter_desktop.py
- **Committed in:** cfd15930 (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (test logic correction)
**Impact on plan:** Test strengthened — now directly verifies the invariant stated in the plan ("A-Z sort order is UNCHANGED by appending the code"). No scope creep.

## Issues Encountered

None — all three source changes were straightforward. TDD cycle confirmed correct RED before GREEN at each step.

## Known Stubs

None — the feature is fully wired. Hebrew UI shows the code, type-to-find matches it. The only remaining gap is the live render-smoke (human_needed):

**Live render-smoke required (human_needed):** Open the catalog library dialog in Hebrew UI on both web `/catalog` and desktop; confirm:
- Each library row shows e.g. "ספריית האוניברסיטה של קיימברידג' (CUL)"
- Typing 'CUL' in the type-to-find box shows only the Cambridge row
- A-Z sort order is unchanged

## Threat Flags

None — library codes come from the fixed `LIBRARY_CODES` dict (not user input); `_html.escape()` already applied in `_make_cat_cb_row`. No new trust boundaries introduced.

## Self-Check

Files exist:
- `shared/browse_map_utils.py` — FOUND (modified)
- `desktop/dialogs_filter.py` — FOUND (modified)
- `web/pages/catalog_browse.py` — FOUND (modified)
- `tests/test_libfilter_desktop.py` — FOUND (modified)
- `tests/test_libfilter_catalog.py` — FOUND (modified)

Commits exist:
- `a9d642bd` — Task 1 (browse_map_utils.py + test_libfilter_catalog.py)
- `cfd15930` — Task 2 (dialogs_filter.py + test_libfilter_desktop.py)
- `421d1c70` — Task 3 (catalog_browse.py)

Test run: 62 passed, 0 failed (GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen)

## Self-Check: PASSED

All 5 modified files verified present. All 3 task commits verified in git log. 62 tests pass headless.

## Next Phase Readiness

- UAT test #9 (131-HUMAN-UAT) is ready for live render-smoke: open catalog library dialog in Hebrew UI and confirm code visible + searchable
- No blockers for human UAT

---
*Phase: 131-dual-mode-parity-desktop-catalog-web-browse-by-identificatio*
*Completed: 2026-06-30*
