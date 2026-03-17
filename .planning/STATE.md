---
gsd_state_version: 1.0
milestone: v5.6
milestone_name: milestone
status: not_started
stopped_at: Phase 52 context gathered
last_updated: "2026-03-17T08:27:17.757Z"
last_activity: 2026-03-16 -- Completed 49-01 web puzzle page with Fabric.js canvas, API endpoints, nav integration
progress:
  total_phases: 6
  completed_phases: 4
  total_plans: 12
  completed_plans: 12
---

---
gsd_state_version: 1.0
milestone: v5.6
milestone_name: milestone
status: not_started
stopped_at: Completed 50-03-PLAN.md
last_updated: "2026-03-16T18:23:22.382Z"
last_activity: 2026-03-17 - Completed quick task 260317-aru: Fix background removal for CUL blue conservation mat
progress:
  total_phases: 6
  completed_phases: 4
  total_plans: 12
  completed_plans: 12
---

---
gsd_state_version: 1.0
milestone: v5.6
milestone_name: milestone
status: not_started
stopped_at: Phase 49 context gathered
last_updated: "2026-03-16T10:08:03.839Z"
last_activity: 2026-03-16 -- Completed Phase 48 (Desktop Canvas) with full interactive verification
progress:
  total_phases: 6
  completed_phases: 2
  total_plans: 7
  completed_plans: 7
---

---
gsd_state_version: 1.0
milestone: v5.6
milestone_name: milestone
status: not_started
stopped_at: Completed 48-03-PLAN.md (Phase 48 complete)
last_updated: "2026-03-16T09:06:41.953Z"
last_activity: 2026-03-16 -- Completed 48-03 integration buttons + visual checkpoint with extensive UX polish
progress:
  total_phases: 6
  completed_phases: 2
  total_plans: 7
  completed_plans: 7
---

---
gsd_state_version: 1.0
milestone: v5.6
milestone_name: milestone
status: not_started
stopped_at: Phase 48 context gathered
last_updated: "2026-03-16T05:31:08.140Z"
last_activity: 2026-03-16 -- Completed 47-03 puzzle image service with IIIF fetch, bg removal, disk cache
progress:
  total_phases: 6
  completed_phases: 1
  total_plans: 4
  completed_plans: 4
  percent: 100
---

---
gsd_state_version: 1.0
milestone: v5.6
milestone_name: milestone
status: not_started
stopped_at: Completed 47-03-PLAN.md
last_updated: "2026-03-16T03:15:20.367Z"
last_activity: 2026-03-16 -- Completed 47-02 background removal engine
progress:
  [██████████] 100%
  completed_phases: 0
  total_plans: 4
  completed_plans: 3
  percent: 75
---

---
gsd_state_version: 1.0
milestone: v5.6
milestone_name: milestone
status: not_started
stopped_at: Completed 47-03-PLAN.md
last_updated: "2026-03-16T03:11:24.924Z"
last_activity: 2026-03-16 -- Completed 47-02 background removal engine
progress:
  [████████░░] 75%
  completed_phases: 0
  total_plans: 4
  completed_plans: 2
---

---
gsd_state_version: 1.0
milestone: v5.6
milestone_name: milestone
status: not_started
stopped_at: Phase 47 context gathered
last_updated: "2026-03-15T21:29:00.725Z"
last_activity: 2026-03-15 -- Roadmap created for v7.0.0 Fragment Puzzle (6 phases, 23 requirements)
progress:
  total_phases: 6
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
---

---
gsd_state_version: 1.0
milestone: v7.0.0
milestone_name: Fragment Puzzle
status: not_started
stopped_at: null
last_updated: "2026-03-15T14:00:00.000Z"
last_activity: 2026-03-15 - Roadmap created for v7.0.0 Fragment Puzzle (6 phases, 23 requirements)
progress:
  total_phases: 6
  completed_phases: 0
  total_plans: 4
  completed_plans: 3
  percent: 75
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-15)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 49 - Web Canvas

## Current Position

Milestone: v7.0.0 Fragment Puzzle
Phase: 49 (3 of 6 in milestone) -- IN PROGRESS
Plan: 1 of 2 complete
Status: Completed 49-01-PLAN.md (puzzle page + API endpoints)
Last activity: 2026-03-17 - Completed quick task 260317-gsb: Fix desktop corrections showing anonymous for all users

Progress: [████░░░░░░] 33% (2 of 6 phases: 47, 48 complete)

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

Recent decisions affecting current work:
- [v7.0.0]: Fabric.js (web) + QGraphicsScene (desktop), shared PuzzleDocument model only -- no shared canvas abstraction
- [v7.0.0]: Pillow + NumPy for background removal (no OpenCV, no ML models)
- [v7.0.0]: Desktop-first build order -- QGraphicsScene validates data model before Fabric.js/NiceGUI
- [v7.0.0]: joins.db SQLite sidecar for local persistence, optional Supabase for community publish
- [v7.0.0]: 800px images for canvas interaction, full-res only for server-side composite export
- [Phase 47-02]: Pillow HSV 0-255 scale; low-saturation S<30 triggers value-only distance; MIN_FOREGROUND_RATIO=0.05 (5%)
- [Phase 47]: Cambridge IIIF fetched directly (not NLI-hosted), separate code path
- [Phase 48-01]: View dispatches wheel to items via adjust_scale_from_wheel() to avoid QWheelEvent/QGraphicsSceneWheelEvent type mismatch
- [Phase 48-01]: Corner-handle rotation on selected items (HANDLE_SIZE=14px), pan on middle-click or left-click on empty canvas
- [Phase 48-02]: Fragment items keyed by (sys_id, folio_label) tuple for stable tracking across folio navigation; functools.partial for binding item_key to signal callbacks
- [Phase 48-03]: Extensive UX improvements during interactive testing -- crop mode, flip-all, 6 backgrounds, keyboard shortcuts, per-edge crop, CUL auto-threshold, hue-weighted bg removal
- [Phase 49-01]: puzzle_folios endpoint uses IIIF manifest FL IDs (fetch_fl_ids_from_nli), NOT nli_crossref fgp_image_number_id -- FGP numbers are Friedberg photo numbers, not NLI FL IDs
- [Phase 49-01]: window.puzzleCanvas JS global object pattern (matches window.manuscriptViewer from browse.py); Fabric.js v6.4.3 via CDN
- [Phase 50]: Crop offsets in 800px canvas-pixel space, scaled to export resolution at compose time
- [Phase 50]: Thumbnail preserved on metadata-only saves by reading existing DB value
- [Phase 50]: PIL.rotate angle negated to match clockwise-positive convention (Fabric.js + PyQt)
- [Phase 50]: Event-driven auto-save via scene.changed with dual-timer debounce; loading guard prevents partial-state overwrites
- [Phase 50]: getCropState reads per-object Fabric.js properties, not transient _cropOffsets; crop restore in on_puzzle_add_result callback

### Pending Todos

- Migrate desktop corrections fetch to shared corrections_service
- CUT-01: Remove read-only PGP tables from Supabase (legacy desktop users depend on them)
- Date range filter using CopyToDate (21K rows) -- show "from-to" date display
- Creation type filter via code_values (CreationTypeCode, 69K rows) -- Original/Copy/Commentary/Tafsir
- Display scholarly Comment (100K rows) and Colophon (789 rows) in expanded detail rows
- Script/vocalization/cantillation filters for paleography researchers
- Copyist name browse axis (CopyName, 1.6K rows)
- OrgCreation/OrgAuthor cross-refs for commentary identification display

### Blockers/Concerns

- IIIF physicalScale metadata availability unverified -- DPI calibration may reduce to per-library lookup table + manual override
- Background removal edge quality on real Genizah manuscripts verified good during Phase 48 interactive testing; hue-weighted removal added for colored backgrounds
- NLI S1/S2 recto/verso pairing convention needs verification across libraries (Phase 51)

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
| Phase 47 P04 | 4min | 1 tasks | 1 files |
| Phase 50 P01 | 3min | 2 tasks | 3 files |
| Phase 50 P02 | 3min | 1 tasks | 1 files |
| Phase 50 P03 | 5min | 2 tasks | 2 files |
| 260317-aru | Fix background removal for CUL blue conservation mat images with border frames | 2026-03-17 | pending | [260317-aru-fix-background-removal-for-cul-blue-cons](./quick/260317-aru-fix-background-removal-for-cul-blue-cons/) |
| 260317-gsb | Fix desktop corrections showing anonymous for all users | 2026-03-17 | 78ce4c41 | [260317-gsb-fix-desktop-corrections-showing-anonymou](./quick/260317-gsb-fix-desktop-corrections-showing-anonymou/) |

## Session Continuity

Last session: 2026-03-17T08:27:17.753Z
Stopped at: Phase 52 context gathered
Resume file: .planning/phases/52-community-integration/52-CONTEXT.md
