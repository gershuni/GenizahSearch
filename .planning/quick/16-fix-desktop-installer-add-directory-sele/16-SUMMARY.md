---
phase: quick-16
plan: 01
subsystem: installer
tags: [installer, inno-setup, upgrade-fix]
key-files:
  modified:
    - CompileScriptGenizah.iss
decisions:
  - DisableDirPage=no forces directory selection on all installs including upgrades
metrics:
  duration: 2min
  completed: "2026-03-10"
---

# Quick Task 16: Fix Desktop Installer - Add Directory Selection

**One-liner:** Force directory selection page on upgrades and update installer output filename to v6.2.0

## What Changed

Two changes to `CompileScriptGenizah.iss`:

1. **Added `DisableDirPage=no`** (line 33) -- Inno Setup 6.x defaults to `auto`, which hides the directory page on upgrades when a previous install path exists in the registry. If that path is stale (e.g., on a different drive from v5.9.3), the installer fails with "drive not accessible". Setting `no` forces the page to always appear.

2. **Updated `OutputBaseFilename`** from `GenizahSearchPro_V6.1.1_Setup` to `GenizahSearchPro_V6.2.0_Setup` (line 37) to match the current `MyAppVersion` defined on line 6.

## Commits

| Task | Description | Commit | Files |
|------|-------------|--------|-------|
| 1 | Add DisableDirPage and fix OutputBaseFilename | ebb7e2f0 | CompileScriptGenizah.iss |

## Deviations from Plan

None - plan executed exactly as written.

## Self-Check: PASSED

- [x] CompileScriptGenizah.iss contains `DisableDirPage=no`
- [x] CompileScriptGenizah.iss OutputBaseFilename references V6.2.0
- [x] Commit ebb7e2f0 exists
