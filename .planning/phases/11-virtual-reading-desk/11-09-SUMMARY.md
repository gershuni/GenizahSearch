---
phase: 11
plan: 09
subsystem: web-ui
tags: [state-management, language-persistence, reading-desk, navigation]

dependency-graph:
  requires: [11-02, 11-06]
  provides: [language-persistence, smart-reading-desk-restore]
  affects: [11-10]

tech-stack:
  added: []
  patterns:
    - "app.storage.user for cross-reload language persistence"
    - "Set comparison for language-switch vs navigation detection"

file-tracking:
  key-files:
    created: []
    modified:
      - web/main.py
      - web/pages/browse.py

decisions:
  - id: DEC-11-09-01
    decision: "Persist language to app.storage.user and restore in create_layout()"
    rationale: "Single restore point in create_layout() covers all pages before any tr()/is_rtl() calls"
  - id: DEC-11-09-02
    decision: "Compare initial_sys_id against persisted desk entry sys_ids to distinguish reload from navigation"
    rationale: "Language-switch reload preserves URL params including sys_id; cross-page nav has a different sys_id"

metrics:
  duration: "3 min"
  completed: "2026-02-08"
---

# Phase 11 Plan 09: Language Switch and Navigation State Fix Summary

**One-liner:** Smart reading desk state management that persists language preference and distinguishes language-switch reloads from cross-page navigation using set comparison of sys_ids.

## What Was Done

### Task 1: Persist language to storage and restore on page load
- Modified `toggle_lang()` in `web/main.py` to save `ui_language` to `app.storage.user` before calling `ui.navigate.reload()`
- Added language restoration at the start of `create_layout()` in `web/main.py`, before any `is_rtl()` or `tr()` calls
- This ensures language preference survives `ui.navigate.reload()` across all pages

### Task 2: Distinguish language-switch from cross-page navigation
- Replaced the unconditional `_restore_reading_desk_state()` call in the `elif initial_sys_id:` branch
- New logic: fetch persisted reading desk entries, build a set of their sys_ids, compare against `initial_sys_id`
  - If `initial_sys_id` is in the set: this is a language-switch reload, restore the full reading desk
  - If `initial_sys_id` is NOT in the set: this is cross-page navigation, clear stale state, load the requested manuscript
- Added debug logging to `_restore_reading_desk_state()` for diagnostics
- The `else` branch (no sys_id in URL) is unchanged -- legitimate restore scenario for language switch from standalone desk

## Decisions Made

| ID | Decision | Rationale |
|----|----------|-----------|
| DEC-11-09-01 | Persist language to app.storage.user, restore in create_layout() | Single restore point covers all pages before any translated content renders |
| DEC-11-09-02 | Set comparison of sys_ids for reload vs navigation detection | Language-switch reload preserves URL params; cross-page nav brings a different sys_id |

## Task Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Persist language to storage and restore on page load | e6c3b82 | web/main.py |
| 2 | Distinguish language-switch from cross-page navigation | 8c1a775 | web/pages/browse.py |

## Deviations from Plan

None -- plan executed exactly as written.

## Verification Results

1. `web/pages/browse.py` parses without errors
2. `web/main.py` parses without errors
3. `ui_language` persisted in toggle_lang (line 1657) and restored in create_layout (line 1389)
4. `persisted_sids` set comparison in initialization block (lines 3668-3669)
5. Cross-page navigation clears stale state via `pop('reading_desk_state')` (line 3678)
6. `_restore_reading_desk_state()` still called in else block for no-sys_id restore (line 3687)

## Next Phase Readiness

- Plan 11-10 (the remaining UAT gap closure plan) can proceed
- No blockers or concerns introduced

## Self-Check: PASSED
