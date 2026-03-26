---
phase: quick
plan: 260326-u9e
subsystem: docs
tags: [documentation, cleanup, archive]
dependency_graph:
  requires: []
  provides: [clean-docs-structure]
  affects: [docs/DOCUMENTATION_INDEX.md, docs/plans/PLANS_INDEX.md]
tech_stack:
  added: []
  patterns: [git-mv-for-history-preservation]
key_files:
  created: [docs/archive/plans/]
  modified: [docs/DOCUMENTATION_INDEX.md, docs/plans/PLANS_INDEX.md]
decisions:
  - Archived LISTS_UNIFICATION_PLAN and JOINS_FEED_PLAN as pre-Supabase stale plans
  - DOCUMENTATION_MAINTENANCE.md archived since CLAUDE.md covers same content
  - FJMS_TRANSCRIPTION_SCRAPING.md skipped (exists only as untracked file in repo root, not in docs/)
metrics:
  duration: 3min
  completed: 2026-03-26
---

# Quick Task 260326-u9e: Reorganize docs/ Folder Summary

Archived 22 completed/stale documents and 7 responsa-search design docs, reducing docs/ root from 14 to 7 active references and docs/plans/ from 19 to 4 files.

## What Changed

### Task 1: Move completed/stale docs to archive (0c2c2b21)

Moved via `git mv` to preserve history:

**From docs/ root to docs/archive/:**
- CODE_REVIEW_46_translation_wiring.md (one-time code review)
- FJMS_EXPORT_AND_TRANSLATION_BUGS.md (resolved bug report)
- POSTHOG_ANALYTICS_REPORT_2026-03-13.md (point-in-time snapshot)
- OCR_CORRECTIONS_SAMPLES.txt (sample data)
- JOIN_FINDER_REPORT.md (completed research report)

**From docs/plans/ to docs/archive/plans/:**
- 15 completed plan files (FIST, Translation, Lists, Responsa, BG removal, etc.)
- responsa-search/ directory (7 design docs)

**Remaining in docs/plans/:** MOBILE_RESPONSIVE_PLAN.md, JOIN_FINDER_IMPLEMENTATION_PLAN.md, USER_TEXT_SEARCH_PLAN.md, PLANS_INDEX.md

### Task 2: Update index files (3927e257)

- Rewrote DOCUMENTATION_INDEX.md with accurate directory tree and file listings
- Rewrote PLANS_INDEX.md with only 3 future plans + archived plans reference
- Moved DOCUMENTATION_MAINTENANCE.md to archive (redundant with CLAUDE.md)

## Deviations from Plan

### Skipped Item

**FJMS_TRANSCRIPTION_SCRAPING.md** - Plan listed this for archival from docs/ but it only exists as an untracked file in the repository root (not in docs/), so there was nothing to move. Not a deviation per se -- the file was never in docs/.

## Known Stubs

None.

## Self-Check: PASSED

- docs/ root: 7 .md files (confirmed)
- docs/plans/: 4 .md files (confirmed)
- docs/archive/plans/: 15 .md files + responsa-search/ (confirmed)
- Commit 0c2c2b21: exists
- Commit 3927e257: exists
