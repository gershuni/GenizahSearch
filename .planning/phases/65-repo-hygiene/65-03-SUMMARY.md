---
phase: 65-repo-hygiene
plan: 03
subsystem: infra
tags: [gitignore, repo-hygiene, debris-cleanup]

requires:
  - phase: 65-repo-hygiene
    provides: "Context decisions D-08 through D-12 for debris classification and gitignore strategy"
provides:
  - "Extended .gitignore covering 15+ debris categories with root-anchored patterns"
  - "Commented exemption block documenting intentional root assets"
  - "Prevention of future debris accumulation (translation checkpoints, backups, scratch files)"
affects: [repo-hygiene, developer-workflow]

tech-stack:
  added: []
  patterns: [root-anchored-gitignore-patterns]

key-files:
  created: []
  modified: [.gitignore]

key-decisions:
  - "Added /_*.json pattern for underscore-prefixed JSON scratch files (e.g., _multi_ie_transcription_fls.json)"
  - "Gitignored large untracked data files (FIST.db, fjms_enrichment.db, nli_crossref.db, pgp.db, libraries_translations.db) that are local-only sidecars"
  - "Classified bodleian_master_index.csv and CrossReference_Final.csv as legacy unreferenced files (gitignored)"
  - "Classified nli_crossreference.csv as legacy (superseded by nli_crossref.db sidecar, only referenced in import scripts)"
  - "Classified sample_*.txt, pgp_sample_*.txt, test_*.txt as unreferenced scratch (gitignored)"
  - "Added /AGENTS.md pattern for AI workspace files"

patterns-established:
  - "Root-anchored gitignore: use /*.ext not *.ext to avoid hiding subdirectory files"
  - "Commented exemption block: document intentional root assets in .gitignore itself"

requirements-completed: [HYGN-03, HYGN-04]

duration: 2min
completed: 2026-04-15
---

# Phase 65 Plan 03: Root Debris Gitignore Summary

**Extended .gitignore with 80 new lines covering 15+ debris categories via root-anchored patterns, reducing untracked root files from 67 to 1**

## Performance

- **Duration:** 2 min
- **Started:** 2026-04-15T01:12:31Z
- **Completed:** 2026-04-15T01:14:17Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- Extended .gitignore from 50 to 130 lines with root-anchored patterns for all D-10 debris categories
- Added SQLite WAL/journal artifact patterns (*.db-shm, *.db-wal, *.db-journal)
- Added patterns for large local-only data files (FIST.db, fjms_enrichment.db, etc.)
- Audited 7 borderline files for code references; classified and gitignored unreferenced ones
- Verified ALL 14 intentional root assets remain NOT ignored via git check-ignore
- Added commented exemption block documenting tracked root assets in .gitignore itself
- Before/after root inventory: 67 untracked root files reduced to 1 (intentional primary_ie_map.json)

## Task Commits

Each task was committed atomically:

1. **Task 1: Root inventory, borderline file audit, and extend .gitignore** - `c5e0fa95` (chore)

## Files Created/Modified
- `.gitignore` - Extended with 80 new lines: debris patterns, data file patterns, exemption documentation

## Decisions Made
- Added `/_*.json` pattern beyond the plan's D-10 list (covers `_multi_ie_transcription_fls.json` and similar scratch JSON)
- Gitignored large untracked DB/data files (FIST.db, fjms_enrichment.db, nli_crossref.db, pgp.db, libraries_translations.db, FIST_Computed_Measurements.xlsx) as local-only sidecars not tracked in git
- Classified `nli_crossreference.csv` as legacy (only referenced in import scripts, superseded by nli_crossref.db)
- Added `/AGENTS.md` pattern for AI workspace metadata files
- Added `/test_*.txt` pattern for ad-hoc test scratch files (test_3750.txt, test_hebrew.txt -- unreferenced)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Added /_*.json pattern for scratch JSON files**
- **Found during:** Task 1 (root inventory)
- **Issue:** Plan's D-10 list covered `_*.txt`, `_*.log`, `_*.js`, `_*.md` but not `_*.json` -- inventory showed `_multi_ie_transcription_fls.json` as underscore-prefixed scratch
- **Fix:** Added `/_*.json` to the underscore-prefixed scratch block
- **Files modified:** .gitignore
- **Committed in:** c5e0fa95

**2. [Rule 2 - Missing Critical] Added large data/DB file patterns**
- **Found during:** Task 1 (borderline file audit)
- **Issue:** Plan mentioned sqlite WAL patterns but not the root-level DB files themselves (FIST.db, fjms_enrichment.db, etc.) which are large local-only sidecars
- **Fix:** Added dedicated block for 6 large data/DB files
- **Files modified:** .gitignore
- **Committed in:** c5e0fa95

---

**Total deviations:** 2 auto-fixed (2 missing critical)
**Impact on plan:** Both additions are natural extensions of the debris pattern strategy. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 65 repo-hygiene is now complete (all 3 plans done)
- All HYGN requirements addressed across the 3 plans
- pytest baseline should remain green (gitignore changes don't affect tests)

---
*Phase: 65-repo-hygiene*
*Completed: 2026-04-15*
