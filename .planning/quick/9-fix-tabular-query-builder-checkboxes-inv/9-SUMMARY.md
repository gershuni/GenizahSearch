---
phase: quick
plan: 9
subsystem: ui
tags: [css, dark-mode, quasar, checkbox, theme]

# Dependency graph
requires: []
provides:
  - Dark-mode CSS rules for Quasar q-checkbox components
  - Theme-aware component card borders in tabular query builder
affects: [web-theming, search-page]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "CSS custom property theming for Quasar checkbox components"

key-files:
  created: []
  modified:
    - web/main.py
    - web/pages/search.py

key-decisions:
  - "No parchment-specific checkbox rules needed -- default Quasar styling works on light backgrounds"

patterns-established:
  - "Dark theme checkbox pattern: [data-theme=dark] .q-checkbox__inner for unchecked, __inner--truthy for checked, .q-checkbox__label for text"

# Metrics
duration: 1min
completed: 2026-02-11
---

# Quick Task 9: Fix Tabular Query Builder Checkboxes Invisible in Dark Mode

**Dark-mode CSS rules for Quasar q-checkbox visibility (unchecked/checked/label) plus theme-aware component card borders replacing hardcoded #e0e0e0**

## Performance

- **Duration:** 1 min
- **Started:** 2026-02-11T08:35:02Z
- **Completed:** 2026-02-11T08:35:57Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- All checkbox components on the search page are now visible in dark mode (modifier checkboxes, search option checkboxes, Responsa outer checkboxes, select-all)
- Unchecked checkboxes show light gray borders (--text-secondary), checked show green (--primary-400), labels show white (--text-primary)
- Component card borders in tabular query builder now adapt to all three themes via var(--border-light)

## Task Commits

Each task was committed atomically:

1. **Task 1: Add dark-mode CSS rules for q-checkbox** - `ba4725b` (fix)
2. **Task 2: Replace hardcoded border color with CSS variable** - `80a8f64` (fix)

## Files Created/Modified
- `web/main.py` - Added 3 CSS rules for dark theme checkbox visibility (unchecked, checked, label states)
- `web/pages/search.py` - Changed component card border from hardcoded #e0e0e0 to var(--border-light)

## Decisions Made
- No parchment-specific checkbox rules added -- parchment uses dark text on light backgrounds so default Quasar styling is already readable

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Steps
- Visual verification recommended in all three themes (light, dark, parchment) to confirm no regression

## Self-Check: PASSED

All files exist, all commits verified, all CSS rules confirmed in source.

---
*Quick Task: 9-fix-tabular-query-builder-checkboxes-inv*
*Completed: 2026-02-11*
