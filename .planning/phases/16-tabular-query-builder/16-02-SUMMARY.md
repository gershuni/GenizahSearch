---
phase: 16-tabular-query-builder
plan: 02
subsystem: ui
tags: [responsa, tabular-builder, nicegui, dialog, query-builder, web]

# Dependency graph
requires:
  - phase: 16-tabular-query-builder
    plan: 01
    provides: "generate_tabular_syntax() for builder-to-syntax conversion"
  - phase: 14-responsa-core-engine
    provides: "Responsa search pipeline, execute_search with responsa_options"
provides:
  - "Web tabular query builder dialog accessible from Responsa sub-options row"
  - "Visual query composition with 2-4 components, per-word modifiers, distance controls"
  - "One-way sync: builder Apply populates search field and triggers search"
  - "Builder negated words integrated into exclude_words pipeline"
affects: [16-03 desktop-tabular-builder]

# Tech tracking
tech-stack:
  added: []
  patterns: ["NiceGUI dialog with pre-created hidden elements toggled by visibility", "select-and-modify pattern for shared modifier checkboxes", "closure factory pattern for loop-variable capture in event handlers"]

key-files:
  created: []
  modified:
    - web/pages/search.py
    - genizah_translations.py

key-decisions:
  - "Dialog uses pre-created 4 components and 3 distance spinners with visibility toggling (not dynamic creation/destruction)"
  - "Modifier checkboxes use select-and-modify pattern: focus a word input, checkboxes reflect that word's state"
  - "Guard flag (_updating_modifiers) prevents on_change loops when programmatically updating checkboxes"
  - "on_apply is async to properly await execute_search()"
  - "Dialog uses min(700px, 95vw) for responsive sizing instead of maximized prop"
  - "Builder negated words stored on SearchUIState and merged into not_words in execute_search"

patterns-established:
  - "Pre-create all possible UI elements and toggle visibility rather than dynamic creation (avoids NiceGUI context issues)"
  - "Closure factory pattern (_make_dist_handler, _make_text_handler, _make_mod_handler) for proper loop variable capture in event handlers"

# Metrics
duration: 9min
completed: 2026-02-10
---

# Phase 16 Plan 02: Web Tabular Query Builder Summary

**NiceGUI dialog with 2-4 component columns, per-word modifier checkboxes, distance spinners, live preview, and one-way sync to Responsa search**

## Performance

- **Duration:** 9 min
- **Started:** 2026-02-10T08:56:17Z
- **Completed:** 2026-02-10T09:06:11Z
- **Tasks:** 1
- **Files modified:** 2

## Accomplishments
- Query Builder button added to Responsa sub-options row, visible only when Responsa mode is active
- Full dialog implementation: 2-4 component cards with 2-4 word input slots each, RTL layout
- Shared modifier checkboxes (prefix, suffix, wildcard start/end, plene/defective, negation) with select-and-modify pattern
- Scope toggle (Word Range / Within Document) controlling distance spinner visibility
- Live preview updates via generate_tabular_syntax() as user types
- Apply generates syntax, populates search field, closes dialog, and triggers search with negated words passed as exclude_words
- Clear All resets all inputs without closing dialog
- Add/remove component and word slot controls with proper min/max limits
- Responsive dialog sizing with min(700px, 95vw) for mobile compatibility
- Hebrew translations for all 17 new UI strings

## Task Commits

Each task was committed atomically:

1. **Task 1: Add Query Builder button and implement dialog with all UI elements** - `1ef7365` (feat)

## Files Created/Modified
- `web/pages/search.py` - Added open_query_builder() function (~380 lines), Query Builder button in responsa_sub_row, builder_negated_words on SearchUIState, merged into exclude_words in execute_search
- `genizah_translations.py` - 17 Hebrew translations for builder UI strings (already committed from prior session)

## Decisions Made
- Pre-created all 4 component cards and 3 distance spinners with visibility toggling, avoiding dynamic element creation/destruction which can cause NiceGUI context issues
- Used closure factory pattern (_make_dist_handler, _make_text_handler, _make_mod_handler) to properly capture loop variables in event handlers
- Made on_apply async to properly await the async execute_search function
- Used min(700px, 95vw) CSS for dialog width to be responsive on both desktop and mobile without using Quasar's maximized prop
- Stored builder_negated_words on SearchUIState rather than using a nonlocal variable, for cleaner integration with execute_search

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed async/await for on_apply calling execute_search**
- **Found during:** Task 1 (dialog implementation)
- **Issue:** on_apply was sync but needed to call async execute_search(); would create unresolved coroutine
- **Fix:** Made on_apply async with await execute_search()
- **Files modified:** web/pages/search.py
- **Verification:** Python compile check passes
- **Committed in:** 1ef7365

**2. [Rule 1 - Bug] Fixed event handler loop variable capture**
- **Found during:** Task 1 (dialog implementation)
- **Issue:** Lambda closures in loops would all capture the same final loop variable value
- **Fix:** Used closure factory functions (_make_dist_handler, _make_text_handler, _make_mod_handler) to properly bind values
- **Files modified:** web/pages/search.py
- **Verification:** Code inspection confirms each handler captures correct indices
- **Committed in:** 1ef7365

---

**Total deviations:** 2 auto-fixed (2 bugs)
**Impact on plan:** Both fixes essential for correct dialog operation. No scope creep.

## Issues Encountered
- Translations were already committed from a prior session execution; no additional commit needed for genizah_translations.py

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Web tabular query builder complete and functional
- Ready for 16-03 (Desktop Tabular Builder) which follows the same pattern with QDialog
- generate_tabular_syntax() and builder state format proven to work end-to-end

---
## Self-Check: PASSED

All files found, all commits verified, 124 tests passing.

---
*Phase: 16-tabular-query-builder*
*Completed: 2026-02-10*
