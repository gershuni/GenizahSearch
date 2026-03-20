---
phase: quick
plan: 260320-dvg
subsystem: desktop-browse
tags: [ui, browse, icons, navigation, external-link]
dependency_graph:
  requires: []
  provides: [browse-icons, browse-reorg, cross-shelfmark-nav, browse-external-link, rd-image-toggle-persistence]
  affects: [genizah_app.py]
tech_stack:
  patterns: [emoji-icon-buttons, compact-toggle, cross-shelfmark-nav]
key_files:
  modified:
    - genizah_app.py
decisions:
  - Browse ext_info_row reorganized to group action buttons (Puzzle/Parallels/List) then info/bib/catalog then external links
  - Translations toggle made compact icon-only (32px) with grey/green state colors
  - Cross-shelfmark page nav always enabled (browse_navigate already supports allow_cross=True)
  - ResultDialog image toggle persisted by saving isChecked() before load and restoring after
metrics:
  duration: 3min
  completed: 2026-03-20
---

# Quick Task 260320-dvg: Add Icons and Reorganize Browse by Shelfmark Summary

**One-liner:** Emoji icons on browse tab buttons matching ResultDialog, reorganized ext_info_row, external library link, cross-shelfmark nav fix, RD image toggle persistence

## Completed Tasks

| # | Task | Commit | Files |
|---|------|--------|-------|
| 1 | Add icons to browse buttons, reorganize ext_info_row, compact translations toggle | 2af21808 | genizah_app.py |
| 2 | Add external library link button, fix cross-shelfmark nav, preserve RD image toggle | 36073014 | genizah_app.py |

## Changes Made

### Task 1: Icons, Reorganization, Compact Toggle
- Added emoji icons to browse buttons: Parallels (magnifying glass), View on Ktiv (globe), Add to View (eye), View Corrections (notepad), Info (info icon)
- Moved Puzzle, Parallels, Add to List buttons from row1 to ext_info_row for cleaner top bar
- New ext_info_row order: Puzzle | Parallels | List | Info | Bib FJMS | Bib NLI | Catalog | Ktiv | External Link | stretch | Translations
- Translations toggle changed from wide text button to compact 32px icon button with grey (#94a3b8) unchecked / green (#059669) checked states
- Updated _browse_toggle_extended_info to use short "Info"/"Hide Info" labels

### Task 2: External Link, Cross-Nav, Image Toggle
- Added btn_b_external_link button with _browse_open_external_link handler (CUDL iiif->view URL transform)
- External link populated dynamically in browse metadata load with provider detection (Oxford/Cambridge/Manchester/Princeton)
- Removed page boundary disable at lines 27996-27997 -- prev/next always enabled for cross-shelfmark wrapping
- Preserved ResultDialog image toggle state: save isChecked() before metadata load, restore after if user had hidden images

## Deviations from Plan

None - plan executed exactly as written.

## Verification

- Syntax validation: `python -c "import ast; ast.parse(...)"` -- PASSED for both tasks

## Self-Check: PASSED
