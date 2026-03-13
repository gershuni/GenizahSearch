---
phase: 46-dicta-translation
plan: 06
subsystem: ui
tags: [translation, nicegui, fjms, catalog-dialog, toggle-badge]

requires:
  - phase: 46-dicta-translation
    provides: "TranslationService.get_fjms_translations_by_signature_ids method, RunningTitle translations in fjms_translations table"
provides:
  - "Web catalog dialog per-record RunningTitle translation with interactive toggle"
affects: [catalog-dialog, translation-display]

tech-stack:
  added: []
  patterns: [per-record-signature-id-lookup, inline-nicegui-toggle-badge]

key-files:
  created: []
  modified: [web/components/catalog_dialog.py]

key-decisions:
  - "Inline ui.row/ui.column/ui.badge layout instead of _field_row for RunningTitle to support interactive widgets"
  - "Per-record translation lookup via get_fjms_translations_by_signature_ids matching desktop pattern"

patterns-established:
  - "Per-record FJMS translation: collect UnitCatalogRecIds, batch fetch via get_fjms_translations_by_signature_ids, per-entry toggle"

requirements-completed: [TRANS-02]

duration: 2min
completed: 2026-03-13
---

# Phase 46 Plan 06: RunningTitle Per-Record Translation Summary

**Web catalog dialog RunningTitle now uses per-record translation lookup (get_fjms_translations_by_signature_ids) with clickable Translated/Original toggle badges, matching desktop behavior**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-13T06:33:50Z
- **Completed:** 2026-03-13T06:35:02Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- Replaced broken single-entry RunningTitle lookup (fjms_trans.get) with per-record batch fetch via get_fjms_translations_by_signature_ids
- Built inline NiceGUI layout (ui.row/ui.column/ui.badge) replacing _field_row to support interactive toggle widgets
- Each team column now shows the correct translated RunningTitle for its specific catalog records
- Direction-aware display: en2he translations shown in Hebrew UI, he2en in English UI
- Clickable Translated/Original badge toggles per running title entry

## Task Commits

Each task was committed atomically:

1. **Task 1: Replace RunningTitle translation lookup with per-record method and inline layout** - `cb5b40a6` (feat)

**Plan metadata:** pending (docs: complete plan)

## Files Created/Modified
- `web/components/catalog_dialog.py` - Replaced RunningTitle block (lines 274-302) with per-record translation lookup and inline toggle badge layout

## Decisions Made
- Used inline ui.row/ui.column/ui.badge layout instead of _field_row because _field_row only renders plain ui.label strings and cannot host interactive badge widgets
- Matched desktop pattern exactly: collect all UnitCatalogRecIds with running_titles data, batch fetch, per-record lookup

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- UAT test 7 (RunningTitle translation in web catalog dialog) should now pass
- All RunningTitle translations display per-record, matching desktop behavior

---
*Phase: 46-dicta-translation*
*Completed: 2026-03-13*
