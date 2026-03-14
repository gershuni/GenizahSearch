---
phase: quick-20
plan: 01
subsystem: web-ui
tags: [ui, header, language-toggle]
key-files:
  modified: [web/main.py]
decisions: []
metrics:
  duration: "41s"
  completed: "2026-03-14"
  tasks_completed: 1
  tasks_total: 1
---

# Quick Task 20: Move Language Toggle to Header Bar

Compact language toggle button (EN/עב) added to header right section between auth buttons and help button, removed from sidebar footer.

## Task Summary

| # | Task | Commit | Files |
|---|------|--------|-------|
| 1 | Move language toggle from sidebar footer to header right section | 55ee8d6d | web/main.py |

## Changes Made

- Added `toggle_lang()` function and compact flat round button inside `render_header_right()`, positioned between auth buttons and help button
- Button shows "EN" when in Hebrew mode (click to switch to English) and "עב" when in English mode (click to switch to Hebrew)
- Removed the entire language toggle block from sidebar footer (lines 412-428), keeping the Translation Toggle intact
- Style matches adjacent help button: `flat round text-color=white`

## Deviations from Plan

None - plan executed exactly as written.

## Verification

- Import check passed: `from web.main import create_layout` succeeds
- Language toggle is in header right section between auth and help buttons
- Sidebar footer only contains translation toggle (language toggle removed)

## Self-Check: PASSED
