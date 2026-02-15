---
phase: quick-13
plan: 01
subsystem: desktop-ui
tags: [ui, compact-mode, result-dialog, ux-improvement]
dependency_graph:
  requires: []
  provides:
    - compact-mode-toggle
    - inline-domain-display
  affects:
    - result-dialog-header
tech_stack:
  added: []
  patterns:
    - compact-header-toggle
    - sync-state-between-modes
key_files:
  created: []
  modified:
    - genizah_app.py
    - genizah_translations.py
decisions: []
metrics:
  duration: "5 minutes"
  completed: "2026-02-14T21:54:28Z"
---

# Quick Task 13: ResultDialog Compact Mode - Remove Preview Summary

**One-liner:** Desktop ResultDialog header reduced: removed 120x120 thumbnail, inlined domain info, added compact mode toggle

## Overview

Overhauled the ResultDialog header in the desktop app to reduce visual clutter and maximize manuscript viewing area by:
1. Removing the redundant 120x120 preview thumbnail from the header
2. Inlining domain info onto the sys_id/fl_id info row (eliminating a separate row)
3. Adding a "Compact Mode" toggle that collapses the full header into a single essential-controls bar

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Remove thumbnail and inline domain info | b4d461b | genizah_app.py, genizah_translations.py |
| 2 | Add compact mode toggle with collapsible header | dcdd523 | genizah_app.py |

## Implementation Details

### Task 1: Remove Thumbnail and Inline Domain Info

**Changes:**
- Removed 120x120 preview thumbnail (`lbl_thumb`) from ResultDialog header layout
- Kept `lbl_thumb` as a hidden dummy widget for backward compatibility (existing methods like `fetch_image`, `on_img_loaded`, `on_img_failed`, `_on_thumb_resolved` still reference it)
- Removed separate `domain_info_row` layout
- Inlined `lbl_rd_domains` into `info_row` (same row as sys_id/fl_id)
- Updated `_update_rd_domain_label()` to prepend " | Domain: " text when visible
- Added Hebrew translations: "Compact" → "תצוגה מצומצמת", "Show More" → "הצג עוד"

**Result:** Header now has one fewer row, domain info appears inline like: "Sys: 123456 | FL: 1r | Domain: Legal, Commercial"

### Task 2: Add Compact Mode Toggle

**Changes:**
- Added "Compact" toggle button to top navigation bar (between result count and "Next Result")
- Created `compact_bar` widget with essential controls:
  - Prev Result button
  - Shelfmark label (bold, selectable)
  - Image navigation (< page spinner >)
  - Add to List button
  - Extended Info button (when available)
  - Joins dropdown button
  - "Show More ..." button
  - Next Result button
- Moved `txt_extended_info` outside `header_widget` so it remains accessible in compact mode
- Implemented `_toggle_compact_mode(compact)` method to switch between full and compact views
- Added state sync logic throughout ResultDialog:
  - Sync compact bar page label when navigating pages
  - Sync compact shelfmark when loading results (two locations)
  - Sync extended info button visibility and state in `toggle_extended_info()`
  - Sync extended info button visibility in four places where `btn_ext_info.setVisible()` is called
  - Sync "Add to List" button label when updated
- Shared `rd_joins_menu` between full `btn_joins` and compact `btn_compact_joins`

**Result:** Users can click "Compact" to collapse the full header into a single row with essential controls. Click "Show More" to restore full view.

## Deviations from Plan

None - plan executed exactly as written.

## Testing

**Manual testing performed:**
- ✅ Desktop app launches without errors
- ✅ ResultDialog opens with no thumbnail visible
- ✅ Domain info appears inline with sys_id/fl_id (when domains exist)
- ✅ "Compact" button visible in top navigation bar
- ✅ Clicking "Compact" collapses header, shows compact bar
- ✅ All compact bar controls functional (navigation, add to list, joins dropdown)
- ✅ "Show More" button restores full header
- ✅ Extended info toggle works in both modes
- ✅ State syncs correctly when navigating results/pages

## Files Modified

### genizah_app.py
- **ResultDialog.init_ui()**: Removed thumbnail from layout, inlined domain label, added compact toggle button, created compact_bar widget, moved txt_extended_info outside header
- **ResultDialog._toggle_compact_mode()**: New method to switch between compact and full header modes
- **ResultDialog._update_rd_domain_label()**: Prepend " | Domain: " text for inline display
- **ResultDialog.toggle_extended_info()**: Sync compact extended info button state
- **ResultDialog page loading**: Sync compact bar page label, shelfmark
- **Extended info visibility**: Sync compact button visibility in 4 locations
- **Add to list**: Sync compact button label

### genizah_translations.py
- Added "Compact" → "תצוגה מצומצמת"
- Added "Show More" → "הצג עוד"

## Self-Check: PASSED

✅ **Files exist:**
- C:\GenizahSearch\genizah_app.py
- C:\GenizahSearch\genizah_translations.py
- C:\GenizahSearch\.planning\quick\13-resultdialog-compact-mode-remove-preview\13-SUMMARY.md

✅ **Commits exist:**
- b4d461b: feat(quick-13): remove ResultDialog thumbnail and inline domain info
- dcdd523: feat(quick-13): add compact mode toggle with collapsible header

✅ **Functionality verified:**
- Thumbnail removed from ResultDialog header
- Domain info inlined (no separate row)
- Compact mode toggle functional
- All navigation works in both modes
- State syncs correctly between modes

## Impact

**User Experience:**
- Reduced header visual clutter (removed 120x120 thumbnail)
- One fewer header row (domain info inline)
- Optional compact mode for maximum manuscript viewing area
- Essential controls still accessible in compact mode

**Code Quality:**
- Backward compatible (lbl_thumb kept as hidden dummy)
- State sync logic maintains consistency between modes
- Shared menu widget (rd_joins_menu) prevents duplication

**Maintenance:**
- txt_extended_info moved to main_layout (independent of header toggle)
- hasattr() guards prevent errors during initialization
- blockSignals() prevents infinite toggle loops

## Next Steps

None - quick task complete. All success criteria met:
1. ✅ Preview thumbnail removed from ResultDialog
2. ✅ Domain info inlined into info row
3. ✅ Compact mode toggle collapses full header
4. ✅ All existing functionality preserved in full mode
5. ✅ Essential controls available in compact mode
