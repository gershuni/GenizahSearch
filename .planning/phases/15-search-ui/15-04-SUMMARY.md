---
phase: 15-search-ui
plan: 04
subsystem: ui
tags: [pyqt6, desktop, responsa, combo-mode, search-ui]

# Dependency graph
requires:
  - phase: 15-02
    provides: "Desktop Responsa checkbox row with master toggle and sub-options"
  - phase: 14
    provides: "Responsa core engine with parse, expand, build query/regex"
provides:
  - "Desktop Responsa as combo box mode option (dropdown, not separate checkbox row)"
  - "Sub-options row with Variants, JA, Flex Spacing, Bidirectional checkboxes"
  - "Syntax legend for Responsa shortcuts visible when mode active"
affects: [16-tabular-builder, 17-integration-testing]

# Tech tracking
tech-stack:
  added: []
  patterns: ["Combo mode-driven sub-options visibility pattern"]

key-files:
  created: []
  modified: ["genizah_app.py"]

key-decisions:
  - "Responsa inserted at combo index 2, shifting all subsequent modes by +1"
  - "Responsa base mode is 'exact' -- pipeline takes over via responsa_options dict"
  - "variant_mode always 'exact' (no pre-responsa mode tracking needed)"
  - "Old amber label, master toggle, and _on_responsa_mode_toggled removed entirely"

patterns-established:
  - "MODE_RESPONSA/MODE_PGP_TAGS constants for index references instead of hardcoded numbers"

# Metrics
duration: 4min
completed: 2026-02-10
---

# Phase 15 Plan 04: Desktop Responsa Dropdown Mode Summary

**Replaced desktop Responsa checkbox row with "Responsa (R)" as a combo mode option, showing sub-options and syntax legend only when selected**

## Performance

- **Duration:** 4 min
- **Started:** 2026-02-10T04:05:57Z
- **Completed:** 2026-02-10T04:09:53Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- Added "Responsa (R)" to mode_combo at index 2 (after Variants, before Fuzzy)
- Sub-options row (Variants, JA, Flex Spacing, Bidirectional) appears only when Responsa mode selected
- Syntax legend showing #word, word#, #word#, %word, *word, (a/b) shortcuts visible in sub-row
- Completely removed old checkbox row, amber label indicator, master toggle, and toggle method
- Updated all shifted mode indices throughout genizah_app.py (AI dialog, mode mapping, PGP tags)
- 99 existing Responsa tests all pass

## Task Commits

Each task was committed atomically:

1. **Task 1: Add Responsa to mode_combo, replace checkbox row with sub-options row** - `47aac3f` (feat)

## Files Created/Modified
- `genizah_app.py` - Replaced Responsa checkbox row with combo mode option; 45 insertions, 83 deletions

## Decisions Made
- Responsa inserted at combo index 2, all subsequent modes shift +1 (Fuzzy=3, Regex=4, Title=5, Shelfmark=6, PGP Tags=7)
- When Responsa mode is selected, base mode is 'exact' since the Responsa pipeline takes over via responsa_options
- variant_mode in responsa_options is always 'exact' (simplified from the old approach that tracked pre-Responsa mode)
- Old amber label indicator, _pre_responsa_mode_idx tracking, and _updating_responsa_mode guard fully removed

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Desktop Responsa dropdown mode complete, matching the web UX approach
- Ready for Phase 16 (Tabular Builder) or Phase 17 (Integration Testing)
- Both web (15-01, 15-03) and desktop (15-02, 15-04) now use dropdown mode for Responsa

## Self-Check: PASSED

- FOUND: genizah_app.py
- FOUND: commit 47aac3f
- FOUND: 15-04-SUMMARY.md

---
*Phase: 15-search-ui*
*Completed: 2026-02-10*
