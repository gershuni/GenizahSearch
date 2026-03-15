---
gsd_state_version: 1.0
milestone: v7.0.0
milestone_name: Transcription Search
status: not_started
stopped_at: null
last_updated: "2026-03-15T05:07:00.000Z"
last_activity: 2026-03-15 - Completed quick task 21: Convert ResultDialog buttons to icon+short text format
progress:
  total_phases: 4
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-14)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v7.0.0 Transcription Search — not yet started

## Current Position

Milestone: v7.0.0 Transcription Search
Phase: None started
Status: Between milestones — v6.5.0 shipped, v7.0.0 not yet planned
Last activity: 2026-03-14 - Completed quick task 20: Move language change button to top bar in web app

## Performance Metrics

**Velocity:**
- Total plans completed: ~161 (across 10 milestones)
- Average duration: ~12 min (historical)

**Recent Trend:**
- v6.5.0: 26 plans, 5 phases, 15 days
- v6.0.0: 21 plans, 6 phases, 6 days
- Trend: Stable

## Accumulated Context

### Decisions

See PROJECT.md Key Decisions table for full history.

### Pending Todos

- Migrate desktop corrections fetch to shared corrections_service
- CUT-01: Remove read-only PGP tables from Supabase (legacy desktop users depend on them)
- Date range filter using CopyToDate (21K rows) — show "from-to" date display
- Creation type filter via code_values (CreationTypeCode, 69K rows) — Original/Copy/Commentary/Tafsir
- Display scholarly Comment (100K rows) and Colophon (789 rows) in expanded detail rows
- Script/vocalization/cantillation filters for paleography researchers
- Copyist name browse axis (CopyName, 1.6K rows)
- OrgCreation/OrgAuthor cross-refs for commentary identification display

### Blockers/Concerns

- FIST.db access required for v7.0.0 Phase 47 (FJMS transcription import) -- confirm file available
- v7.0.0 Phase 48 extends Tantivy schema -- need backward-compatible index upgrade strategy

### Quick Tasks Completed

| # | Description | Date | Commit | Directory |
|---|-------------|------|--------|-----------|
| 15 | Move catalog/bib buttons to page nav pane in Browse; fix FJMS button in advanced mode | 2026-02-22 | da8cd4ab | [15-move-catalog-bib-buttons-to-page-nav-pan](./quick/15-move-catalog-bib-buttons-to-page-nav-pan/) |
| 16 | Fix installer: show directory selection on upgrades, update filename to v6.2.0 | 2026-03-10 | ebb7e2f0 | [16-fix-desktop-installer-add-directory-sele](./quick/16-fix-desktop-installer-add-directory-sele/) |
| 17 | Create bump_version.py script, fix version_info.txt (6.1.1->6.2.0), document in CLAUDE.md | 2026-03-10 | 45e6d801 | [17-create-bump-version-py-script-and-fix-ve](./quick/17-create-bump-version-py-script-and-fix-ve/) |
| 18 | Fix composition search ResultDialog parent_slot error and missing next/prev for filtered results | 2026-03-13 | a0a8c9d2 | [18-fix-composition-search-resultdialog-pare](./quick/18-fix-composition-search-resultdialog-pare/) |
| 19 | Fix desktop session restore for browse tabs, composition summary, and active tab | 2026-03-14 | f64690d8 | [19-fix-desktop-session-restore-parallel-sea](./quick/19-fix-desktop-session-restore-parallel-sea/) |
| 20 | Move language toggle from sidebar to header bar | 2026-03-14 | 55ee8d6d | [20-move-language-change-button-to-top-bar-i](./quick/20-move-language-change-button-to-top-bar-i/) |
| 21 | Convert ResultDialog buttons to icon+short text format | 2026-03-15 | dc1b9c34 | [21-desktop-resultdialog-convert-buttons-to-](./quick/21-desktop-resultdialog-convert-buttons-to-/) |

## Session Continuity

Last session: 2026-03-15
Stopped at: Completed quick task 21: Convert ResultDialog buttons to icon+short text format
Resume file: None
