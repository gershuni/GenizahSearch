---
phase: 07-joins-ui
plan: 02
subsystem: joins-ui
tags: [joins, UI, metadata-sidebar, translations, multi-fragment-viewer]
requires:
  - 07-01 (unified joins data layer)
provides:
  - Inline Related Fragments panel in metadata sidebar
  - View All Fragments mode in main viewer
  - Hebrew translations for all joins UI strings
affects:
  - Milestone completion (final plan of Phase 7)
tech-stack:
  added: []
  patterns:
    - Stacked vertical [Image | Text] layout for multi-fragment viewing
    - on_view_all callback from joins dialog to browse page state
    - fragment_details populated from both PGP and user joins
key-files:
  created: []
  modified:
    - web/pages/browse.py
    - web/components/joins_panel.py
    - genizah_translations.py
key-decisions:
  - id: JOIN-06
    description: "Inline Related Fragments panel in metadata sidebar (below PGP Metadata, above Export)"
  - id: JOIN-07
    description: "View All Fragments mode: stacked [Image | Text] per page per fragment in main viewer"
  - id: JOIN-08
    description: "fragment_details populated from both user joins (sys_id) and PGP joins for unified View All"
  - id: JOIN-09
    description: "Full PGP transcription shown at bottom of joined view when pgpid available"
duration: ~20 min
completed: 2026-02-07
---

# Phase 7 Plan 2: Inline Related Fragments Panel + View All Fragments

**One-liner:** Added inline joins panel in sidebar, View All Fragments mode in main viewer with stacked [Image | Text] per fragment, and Hebrew translations for all joins UI strings.

## Performance

- Duration: ~20 minutes (including user feedback and iteration)
- 2 tasks + 2 orchestrator fixes
- 4 commits

## Accomplishments

1. **Inline Related Fragments panel** in metadata sidebar showing clickable fragment list with PGP badges, relationship types, and navigation arrows. Section only appears when joins exist.

2. **View All Fragments mode** accessible from joins dialog (chain icon). Switches main viewer to stacked vertical layout showing each fragment's recto and verso as [Image | Text] pairs. Full PGP transcription displayed at bottom.

3. **User joins support for View All** — `fragment_details` now populated from both PGP document_fragments and user fragment_joins (sys_id extraction), enabling View All to work for all join types.

4. **Hebrew translations** for 27 joins UI strings (Related Fragments, Physical join, Same composition, View All Fragments, etc.).

5. **View whole document dialog** with all fragment images and full transcription (kept as secondary option alongside the main viewer mode).

## Task Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Add inline Related Fragments panel + translations | 59a2210 | web/pages/browse.py, genizah_translations.py |
| fix | Add missing sanitize=False to ui.html calls | 6edb537 | web/pages/browse.py |
| 1+ | Add View All Fragments mode to joins dialog and main viewer | b44e87b | web/pages/browse.py, web/components/joins_panel.py, genizah_translations.py |
| fix | Populate fragment_details from user joins for View All | b207f46 | web/components/joins_panel.py |

## Files Modified

- `web/pages/browse.py` — BrowseState (view_joined, joined_fragments_info, joined_pgpid), enter/exit_joined_view functions, inline Related Fragments section in metadata sidebar, stacked joined fragments viewer, on_view_all callback passed to joins button
- `web/components/joins_panel.py` — on_view_all callback parameter on create_joins_button/create_joins_dialog, "View All Fragments" button in dialog, fragment_details populated from user joins with deduplication
- `genizah_translations.py` — 27 Hebrew translations for joins UI strings

## Decisions Made

1. **JOIN-06: Inline panel location** — Related Fragments section placed below PGP Metadata, above Export in the metadata sidebar. Hidden when no joins exist.

2. **JOIN-07: View All Fragments layout** — Stacked vertical scroll with each fragment showing recto and verso as [Image | Text] rows. Full PGP transcription at bottom. Accessible from joins dialog button.

3. **JOIN-08: Unified fragment_details** — Both user joins (fragment_a_sys_id/fragment_b_sys_id) and PGP joins (document_fragments table) populate fragment_details, with deduplication by upper-case shelfmark.

4. **JOIN-09: Full PGP transcription placement** — Shown at bottom of joined view when pgpid is available, with PGP badge and Hebrew font styling.

## Deviations from Plan

- Added "View All Fragments" mode in main viewer (user-requested enhancement beyond original plan)
- Added user joins support for fragment_details (discovered gap during testing)
- Fixed missing sanitize=False on ui.html calls (NiceGUI version requirement)

## Issues Encountered

1. **ui.html sanitize parameter** — NiceGUI version requires explicit `sanitize=False` for raw HTML. Two calls in the document viewer dialog were missing this parameter.

2. **User joins missing from fragment_details** — Original implementation only populated fragment_details from PGP joins. Fixed by extracting sys_ids from user join records.

## Self-Check: PASSED
