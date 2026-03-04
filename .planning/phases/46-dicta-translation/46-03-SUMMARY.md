---
phase: 46-dicta-translation
plan: 03
subsystem: translation
tags: [dicta, fjms, catalog, gap-fill, free-description, bibliography, batch-script, sqlite]

# Dependency graph
requires:
  - phase: 46-01
    provides: "Dicta API client (shared/dicta_client.py), few-shot templates, fjms_translations schema"
provides:
  - "FJMS catalog gap-fill script (scripts/translate_fjms_catalog.py) for 6 field categories (~5,546 items)"
  - "FJMS free description translation script (scripts/translate_fjms_free_desc.py) for ~255K items"
  - "Bibliography translation scaffold (deferred, ~338K RunningTitle entries)"
affects: [46-04, 46-05]

# Tech tracking
tech-stack:
  added: []
  patterns: [category-based-gap-fill, long-batch-robustness, sigint-checkpoint, sqlite-reconnect]

key-files:
  created:
    - scripts/translate_fjms_catalog.py
    - scripts/translate_fjms_free_desc.py
  modified: []

key-decisions:
  - "6 gap-fill categories covering all FJMS bilingual field pairs (Title/TitleHeb, AuthorText, GenizahTitle OrgTitle/EngTitle, GenizahPerson HebDesc/EngDesc)"
  - "Bibliography uses RunningTitle column (not BibDesc which does not exist) for translation candidates"
  - "Bibliography mode requires --force flag for full run as safety guard for ~40-hour operation"
  - "SIGINT handler for graceful shutdown during 22-hour free description runs"
  - "SQLite connection refresh every 10,000 items to prevent stale connections in long batches"

patterns-established:
  - "Category-based gap-fill: separate SQL queries per field type, each with IS NULL OR = '' guard"
  - "Long-batch robustness: SIGINT handler, exponential backoff (1s-30s), reconnect interval, periodic logging with ETA"
  - "Dual-direction few-shot loading: HE->EN and EN->HE templates loaded together for mixed categories"

requirements-completed: [TRANS-02, TRANS-03, TRANS-04]

# Metrics
duration: 5min
completed: 2026-03-04
---

# Phase 46 Plan 03: FJMS Batch Translation Scripts Summary

**Batch scripts for FJMS catalog gap-fill (~5,546 items across 6 categories) and free description translation (~255K items, ~18h) with robust checkpointing, plus deferred bibliography scaffold**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-04T06:51:15Z
- **Completed:** 2026-03-04T06:56:16Z
- **Tasks:** 2
- **Files created:** 2

## Accomplishments
- FJMS catalog gap-fill script handling 6 bilingual field categories: Title HE->EN (1,156), Title EN->HE (1,720), Authors HE->EN (178), Genizah Titles (626), Persons HE->EN (1,163), Persons EN->HE (703)
- Free description translation script with robust 22-hour run support: SIGINT handler, exponential backoff, SQLite reconnect every 10K items, progress logging with rate/ETA
- Bibliography translation scaffolded with deferral warning and --force safety guard; translates RunningTitle column (~338K candidates)
- All gap-fill queries use strict NULL/empty guards -- never overwrites existing human translations

## Task Commits

Each task was committed atomically:

1. **Task 1: FJMS catalog gap-fill script** - `4538dc81` (feat: 6 categories, checkpoint, dry-run, per-category execution)
2. **Task 2: FJMS free description + bibliography scaffold** - `cb8ae451` (feat: 255K candidates, SIGINT, backoff, reconnect, bibliography scaffold)

## Files Created/Modified
- `scripts/translate_fjms_catalog.py` - Batch gap-fill for FJMS catalog fields across 6 categories (~5,546 total candidates)
- `scripts/translate_fjms_free_desc.py` - Batch translation for FJMS free descriptions (~255K) with deferred bibliography scaffold (~338K)

## Decisions Made
1. **Bibliography column adaptation:** Plan referenced BibDesc which does not exist in the bibliography table. Adapted to use RunningTitle column (361K non-empty, 338K >= 20 chars) which is the primary descriptive text in bibliography entries.
2. **Author language detection:** AuthorText gap detection filters to Hebrew-containing strings only (178 of 204 distinct values), since English AuthorText values don't need HE->EN translation.
3. **SIGINT handler with signal module:** Used signal.signal(signal.SIGINT, handler) instead of try/except KeyboardInterrupt in the ThreadPoolExecutor loop, ensuring cleaner shutdown with checkpoint save during the 22-hour free description batch.
4. **Dual checkpoint files:** Free description and bibliography modes use separate checkpoint files (translate_fjms_freedesc_checkpoint.json vs translate_fjms_bib_checkpoint.json) so runs don't interfere.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Bibliography BibDesc column does not exist**
- **Found during:** Task 2 (bibliography scaffold)
- **Issue:** Plan specified querying `BibDesc` from bibliography table, but that column does not exist. Table has: RunningTitle, ArticleName, ArticleAuthorEng, ArticleAuthorHeb, etc.
- **Fix:** Used RunningTitle column (361K non-empty) as the primary translatable text, with field_name='BibRunningTitle' in fjms_translations
- **Files modified:** scripts/translate_fjms_free_desc.py
- **Verification:** `--mode bibliography --dry-run` reports 338K candidates correctly
- **Committed in:** cb8ae451 (Task 2 commit)

**2. [Rule 1 - Bug] genizah_titles column names differ from plan**
- **Found during:** Task 1 (catalog gap-fill)
- **Issue:** Plan referenced GenizahTitleOrgTitle/GenizahTitleEngTitle but the actual genizah_titles table has OrgTitle/EngTitle columns
- **Fix:** Used actual column names (OrgTitle, EngTitle) in gap detection queries while keeping field_name='GenizahTitleEngTitle' for fjms_translations storage
- **Files modified:** scripts/translate_fjms_catalog.py
- **Verification:** dry-run reports 626 genizah_titles candidates correctly
- **Committed in:** 4538dc81 (Task 1 commit)

---

**Total deviations:** 2 auto-fixed (2 bugs -- schema mismatches between plan and reality)
**Impact on plan:** Essential fixes for correct database queries. No scope creep.

## Issues Encountered
- Free description candidate count (254,835) lower than plan estimate (303K) because catalog_free_desc has 303K total rows but only 255K have length >= 20 chars
- Bibliography candidate count (338K RunningTitle >= 20 chars) vs plan's 542K because 542K is total bibliography rows, many with short/NULL RunningTitle

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Both FJMS batch scripts ready to run for production translation batches
- Catalog gap-fill (~5,546 items) estimated at ~25 minutes with 5 workers
- Free description batch (~255K items) estimated at ~18 hours with 5 workers
- Bibliography scaffold ready but deferred (~338K items, ~24 hours)
- Next: 46-04 (web search integration with translation toggle)

## Self-Check: PASSED

All 2 created files verified present. All 2 task commits verified in git log.

---
*Phase: 46-dicta-translation*
*Completed: 2026-03-04*
