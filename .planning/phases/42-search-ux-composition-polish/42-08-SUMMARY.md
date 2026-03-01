---
phase: 42-search-ux-composition-polish
plan: 08
subsystem: desktop-translations
tags: [translations, hebrew, ux, cancel-notification, gap-closure]
dependency_graph:
  requires: []
  provides: [bare-searching-key, excluded-reason-translations, cancel-partial-notification]
  affects: [genizah_translations.py, genizah_app.py]
tech_stack:
  added: []
  patterns: [statusbar-notification-on-cancel]
key_files:
  created: []
  modified:
    - genizah_translations.py
    - genizah_app.py
decisions:
  - "Added bare 'Searching' key separate from 'Searching...' to support desktop status line format"
  - "Used 5000ms timeout for partial results statusbar message to match existing notification pattern"
metrics:
  duration: 4min
  completed: 2026-03-01T19:30:32Z
---

# Phase 42 Plan 08: UAT R3 Gap Closure -- Missing Hebrew Translations + Desktop Partial Results Notification

**One-liner:** 3 missing Hebrew translation keys (bare Searching, excluded reason sub-headers) + statusbar partial results notification on desktop search cancel

## What Was Done

### Task 1: Add Missing Hebrew Translation Keys (4fa5aebc)

Added 3 translation keys to the TRANSLATIONS dict in genizah_translations.py:

1. **Bare "Searching" key** -- `"Searching": "מחפש"` -- The desktop search status line uses `tr('Searching')` (without ellipsis) but only `"Searching..."` existed, causing English leak in Hebrew mode.

2. **"Found in source text" key** -- `"Found in source text": "נמצא בטקסט המקור"` -- Excluded section reason sub-headers were displaying in English.

3. **"High frequency" key** -- `"High frequency": "תדירות גבוהה"` -- Same issue as above for frequency-based exclusion reasons.

**Files modified:** genizah_translations.py (+4 lines)

### Task 2: Show Partial Results Notification After Desktop Regular Search Cancel (72f44bd2)

Added `self.statusBar().showMessage(tr('Partial results'), 5000)` to `stop_search()` method after `reset_ui()`. When the user cancels a regular search, the status bar now shows "Partial results" (or "תוצאות חלקיות" in Hebrew mode) for 5 seconds.

The key "Partial results" already existed in translations from Phase 42-02.

**Files modified:** genizah_app.py (+2 lines)

## Deviations from Plan

None -- plan executed exactly as written.

## Verification Results

1. Translation keys: All 3 keys present and returning correct Hebrew values
2. Partial results notification: `showMessage(tr('Partial results'), 5000)` present in stop_search method

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | 4fa5aebc | Add missing Hebrew translation keys |
| 2 | 72f44bd2 | Show partial results notification after cancel |

## Self-Check: PASSED

- FOUND: 42-08-SUMMARY.md
- FOUND: commit 4fa5aebc (Task 1)
- FOUND: commit 72f44bd2 (Task 2)
