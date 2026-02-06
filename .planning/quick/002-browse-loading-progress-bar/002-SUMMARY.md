---
phase: quick
plan: 002
subsystem: ui-ux
tags: [progress-bar, loading, css, javascript, feedback]

dependency-graph:
  requires: []
  provides: [page-loading-indicator]
  affects: []

tech-stack:
  added: []
  patterns: [css-animations, iife-javascript]

key-files:
  created: []
  modified:
    - web/main.py

decisions:
  - key: progress-bar-style
    choice: GitHub/YouTube-style thin bar at top
    why: Familiar UX pattern, non-intrusive, immediate feedback

metrics:
  duration: 1 min
  completed: 2026-02-06
---

# Quick Task 002: Browse Loading Progress Bar Summary

**One-liner:** GitHub/YouTube-style thin animated progress bar at top of page provides immediate visual feedback during navigation.

## What Was Built

Added a global page loading progress bar that:
- Appears as a thin (3px) green animated bar at the very top of the viewport
- Activates immediately when user clicks any internal link
- Shows shimmer animation while page loads
- Completes and fades out when destination page fully loads
- Uses CSS variables (--primary-400, --primary-500) for consistent theming

## Implementation Details

**CSS (in COMMON_STYLES):**
- `.page-loading-bar` - Fixed position, z-index 9999, gradient background
- `.page-loading-bar.active` - Triggers loading-progress and loading-shimmer animations
- `.page-loading-bar.complete` - Triggers loading-complete fade-out animation
- Three keyframe animations: loading-progress (slides in), loading-shimmer (gradient animation), loading-complete (completes and fades)

**JavaScript (in create_layout):**
- Click event listener on document detects internal link clicks
- Filters for internal navigation: href starts with `/`, not `//`, no target attribute
- Adds `active` class to trigger loading animation
- Window `load` event removes `active` and adds `complete` for fade-out

## Commits

| Commit | Message | Files |
|--------|---------|-------|
| 034dd67 | feat(quick-002): add page loading progress bar | web/main.py |

## Verification Checklist

- [x] Progress bar visible at top during navigation
- [x] Smooth shimmer animation while loading
- [x] Bar fades out when page finishes loading
- [x] Works on all pages (via create_layout)
- [x] External links (target="_blank") do not trigger bar
- [x] No syntax errors in web/main.py

## Deviations from Plan

None - plan executed exactly as written.

## Future Considerations

- Could add support for NiceGUI's programmatic navigation via custom events
- Could adjust animation timing based on user preference (prefers-reduced-motion)
- Could add progress percentage for long-running operations with progress tracking
