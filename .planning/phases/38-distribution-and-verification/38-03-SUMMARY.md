---
phase: 38-distribution-and-verification
plan: 03
subsystem: desktop, infra
tags: [pyqt6, sidecar, github-releases, auto-update, sqlite, singleton]

# Dependency graph
requires:
  - phase: 38-01
    provides: "PyInstaller build config with pgp.db bundled, PgpService.get_version()"
provides:
  - "SidecarUpdateThread and SidecarDownloadThread in gui_threads.py"
  - "reset_pgp_service(), reset_fjms_service(), reset_nli_crossref_service() singleton reset functions"
  - "LOCALAPPDATA sidecar path resolution in all three service __init__ methods"
  - "About screen Data Sources section with sidecar version display"
  - "Non-blocking startup sidecar version check via GitHub Releases manifest"
affects: [desktop-app, sidecar-services, about-screen]

# Tech tracking
tech-stack:
  added: []
  patterns: ["LOCALAPPDATA user-data path for sidecar updates (separate from read-only bundled location)", "GitHub Releases manifest for sidecar version checking", "Sequential download queue with singleton reset after completion"]

key-files:
  created: []
  modified: ["gui_threads.py", "genizah_app.py", "shared/document_service.py", "shared/fjms_service.py", "shared/nli_crossref_service.py"]

key-decisions:
  - "Sidecar updates download to %LOCALAPPDATA%/GenizahSearchPro/data/ (not bundled location)"
  - "Service __init__ checks LOCALAPPDATA first, falls back to project root -- minimal 3-line change"
  - "Sequential download queue (not parallel) for simplicity and predictable progress"
  - "HTML entities for check/dash in About screen to avoid encoding issues"

patterns-established:
  - "reset_*_service() pattern: close connection -> clear singleton -> next access re-creates"
  - "LOCALAPPDATA path resolution: user-updated sidecar takes precedence over bundled"

requirements-completed: [DIST-01, PERF-01]

# Metrics
duration: 7min
completed: 2026-02-18
---

# Phase 38 Plan 03: Sidecar Update Mechanism Summary

**Desktop app auto-checks GitHub Releases for sidecar updates on startup, prompts user, downloads to LOCALAPPDATA, resets service singletons, and About screen shows installed data versions**

## Performance

- **Duration:** 7 min
- **Started:** 2026-02-18T05:15:02Z
- **Completed:** 2026-02-18T05:21:33Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- All three sidecar service modules (PGP, FJMS, NLI) have reset_service() functions for safe sidecar file replacement
- LOCALAPPDATA path resolution added to all service __init__ methods (user-updated sidecar takes priority)
- SidecarUpdateThread reads GitHub Releases manifest, compares SemVer versions, emits update list
- SidecarDownloadThread downloads with progress, atomic file replacement via temp file + move
- Desktop app startup triggers non-blocking sidecar check alongside existing app update check
- About screen shows Data Sources table with version and status for all three sidecars

## Task Commits

Each task was committed atomically:

1. **Task 1: Add reset_service() and LOCALAPPDATA path resolution** - `b3a496a7` (feat)
2. **Task 2: SidecarUpdateThread, download integration, About screen** - `b7ae8c8c` (feat)

## Files Created/Modified
- `shared/document_service.py` - Added reset_pgp_service() and LOCALAPPDATA path check in PgpService.__init__
- `shared/fjms_service.py` - Added reset_fjms_service() and LOCALAPPDATA path check in FjmsService.__init__
- `shared/nli_crossref_service.py` - Added reset_nli_crossref_service() and LOCALAPPDATA path check in NliCrossrefService.__init__
- `gui_threads.py` - Added SidecarUpdateThread (GitHub manifest check) and SidecarDownloadThread (download with progress)
- `genizah_app.py` - Import new threads, startup sidecar check, update/download handlers, About screen Data Sources section

## Decisions Made
- Downloads go to LOCALAPPDATA always (safe for both dev and PyInstaller modes where bundled location may be read-only)
- Service __init__ checks LOCALAPPDATA first with minimal 3-line addition (no refactoring of existing path resolution)
- Sequential download queue rather than parallel downloads (simpler, predictable)
- Used HTML entities (&#10003; and &#8212;) instead of Unicode in About screen for encoding safety
- try/except around each service version check in About screen for graceful degradation

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- Two pre-existing test failures found (test_msviewer_ktiv_button_exists, test_suffixes_counted_in_explosion_guard) -- confirmed pre-existing by running against clean commit; not related to this plan's changes

## User Setup Required

None - no external service configuration required. The sidecar update mechanism works automatically via GitHub Releases API.

## Next Phase Readiness
- Phase 38 complete (all 3 plans done)
- Desktop app has full sidecar bundling (38-01), offline verification (38-02), and auto-update mechanism (38-03)
- Ready for milestone completion or next milestone planning

## Self-Check: PASSED

All files verified present. Both task commits (b3a496a7, b7ae8c8c) confirmed in git log.

---
*Phase: 38-distribution-and-verification*
*Completed: 2026-02-18*
