---
gsd_state_version: 1.0
milestone: v7.3
milestone_name: Search Refinement & Scholarly Joins
status: Ready to execute
stopped_at: Completed 55-01-PLAN.md
last_updated: "2026-03-28T19:10:41.350Z"
progress:
  total_phases: 11
  completed_phases: 7
  total_plans: 24
  completed_plans: 22
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-15)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 55 — search-within-results

## Current Position

Phase: 55 (search-within-results) — EXECUTING
Plan: 2 of 3

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
- [Phase 52]: Client-parameter pattern: publish service functions accept Supabase client as first arg for web+desktop reuse
- [Phase 52]: Publish button green color prop for state; check_publish_state on document load; fork creates local copy before /puzzle?doc= nav
- [Phase 52-03]: PuzzlePublishThread QThread worker prevents UI freeze during desktop publish
- [Phase 52-03]: get_feed() merges puzzle_join FeedItems into standard feed pipeline for filter support
- [Phase 52-03]: community_container widget pattern with clear-on-refresh prevents duplication in JoinsDialog
- [Phase 53-01]: 38,673 FIST gap records merged into libraries.csv (216,942->255,615), 7 new library codes, Yevr/Halper shelfmark aliases
- [Phase 53-01]: FIST AlmaId is integer, CSV system_number is string -- CAST(AlmaId AS TEXT) required for matching
- [Phase 53-01]: LibraryId 230 (Vernadsky) mapped to 'Harkavy' code (shelfmarks are Harkavi-prefixed)
- [Phase Phase 53-02]: Metadata search extracted into _execute_metadata_search helper, moved above Tantivy guard; uses meta_mgr.get_meta_for_id(sid) for display dict (not self.csv_bank which doesn't exist on SearchEngine)
- [Phase Phase 53-02]: metadata_only flag on all metadata search results: True for records without Tantivy text (FIST-only), False for records with text; browse page hides page nav for metadata_only=True results
- [Phase 54]: Single import_measurements.py script as sole owner of all measurement tables; flag exclusion at aggregation time in summary
- [Phase 54]: Teal color scheme for measurements dialog, distinct from catalog and bib
- [Phase 54]: Web async dialog fetch via run.io_bound; desktop lazy-fetch on first click
- [Phase 54]: COALESCE(catalog, computed) for width/height filtering maximizes coverage
- [Phase 54]: _normalize_range backend guard swaps reversed min/max bounds
- [Phase 54]: Material labeled 'Material (measured)' to distinguish from printed filter; post-search state separate from pre-search; shared _apply_measurement_post_filters helper in web; _measurement_fetch_complete race guard in desktop
- [Phase 55]: Explicit None vs empty-set contract in compute_effective_restrict for search restrict merging

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
| Phase 52 P01 | 3min | 1 tasks | 3 files |
| Phase 52 P01 | 3min | 2 tasks | 3 files |
| 260317-tt9 | Fix puzzle IIIF server-side processing so puzzle works on server | 2026-03-17 | 1b90fc2f | [260317-tt9-fix-puzzle-iiif-server-side-processing-s](./quick/260317-tt9-fix-puzzle-iiif-server-side-processing-s/) |
| 260317-vgh | Remove dead CF Worker proxy, stabilize localhost helper as primary fallback | 2026-03-17 | 4e4d9b94 | [260317-vgh-remove-dead-cf-worker-proxy-stabilize-lo](./quick/260317-vgh-remove-dead-cf-worker-proxy-stabilize-lo/) |
| 260318-jf1 | Fix Manchester image fetch: recto shown for both sides -> distinct multi-page canvases | 2026-03-18 | 500ff460 | [260318-jf1-fix-manchester-image-fetch-recto-shown-f](./quick/260318-jf1-fix-manchester-image-fetch-recto-shown-f/) |
| 260318-jyz | Fix library attribution credit lines per source library | 2026-03-18 | b4486a4e | [260318-jyz-fix-library-attribution-credit-lines-per](./quick/260318-jyz-fix-library-attribution-credit-lines-per/) |
| 260318-kk1 | Fix puzzle image loading for non-NLI libraries (Manchester, Oxford, JTS, Cambridge) | 2026-03-18 | 7bf99391 | [260318-kk1-fix-puzzle-image-loading-for-non-nli-lib](./quick/260318-kk1-fix-puzzle-image-loading-for-non-nli-lib/) |
| 260318-tkj | Add CUDL as image source for Mosseri collection (3,141/3,194 records) | 2026-03-18 | 398e44f7 | [260318-tkj-add-cudl-cambridge-digital-library-as-an](./quick/260318-tkj-add-cudl-cambridge-digital-library-as-an/) |
| 260319-mc4 | Search UX overhaul: hero search bar, inline accordion, citation collapse, thumbnail images | 2026-03-19 | 6d5f5817 | [260319-mc4-search-ux-hero-search-bar-on-home-page-a](./quick/260319-mc4-search-ux-hero-search-bar-on-home-page-a/) |
| Phase 53 P02 | 45 | 2 tasks | 3 files |
| 260320-dvg | Add icons and reorganize Browse by Shelfmark tab buttons to match ResultDialog patterns | 2026-03-20 | c7b0bde7 | [260320-dvg-add-icons-and-reorganize-browse-by-shelf](./quick/260320-dvg-add-icons-and-reorganize-browse-by-shelf/) |
| 260319-dt1 | Add image adjustment controls (brightness, contrast, gamma, invert) to all viewers | 2026-03-19 | a4764fd7 | [260319-dt1-add-image-controls-brightness-contrast-g](./quick/260319-dt1-add-image-controls-brightness-contrast-g/) |
| 260321-qjh | Fix puzzle session restore for external fragments + wire Firefox addon link | 2026-03-21 | 99199a74 | [260321-qjh-fix-bug-adding-to-puzzle-in-web-does-not](./quick/260321-qjh-fix-bug-adding-to-puzzle-in-web-does-not/) |
| 260321-tiv | Extract shared filter panel component and manuscript viewer JS | 2026-03-21 | 6b54a1f4 | [260321-tiv-extract-shared-filter-panel-and-image-vi](./quick/260321-tiv-extract-shared-filter-panel-and-image-vi/) |
| 260322-kr9 | Close ResultDialog when adding to puzzle from desktop app | 2026-03-22 | cab3753e | [260322-kr9-close-resultdialog-when-adding-to-puzzle](./quick/260322-kr9-close-resultdialog-when-adding-to-puzzle/) |
| 260322-uqk | Dedup FJMS catalog + enhance bibliography (8 new FIST fields, volume fix, Hebrew titles) | 2026-03-22 | da414fd6 | [260322-uqk-fix-fist-data-issues-deduplicate-fjms-ca](./quick/260322-uqk-fix-fist-data-issues-deduplicate-fjms-ca/) |
| 260323-gmy | Add Princeton DPUL as main source for JTS images | 2026-03-23 | 36ebe881 | [260323-gmy-add-princeton-dpul-as-main-source-for-jt](./quick/260323-gmy-add-princeton-dpul-as-main-source-for-jt/) |
| 260325-eol | Fix browse tab Recently Viewed list: sort by view time + make resizable | 2026-03-25 | a346b2f3 | [260325-eol-fix-browse-tab-recently-viewed-list-sort](./quick/260325-eol-fix-browse-tab-recently-viewed-list-sort/) |
| 260325-hhn | Fix missing FJMS bibliography author + untranslated Penn/Halper catalog entries | 2026-03-25 | pending | [260325-hhn-fix-missing-fjms-bibliography-author-unt](./quick/260325-hhn-fix-missing-fjms-bibliography-author-unt/) |
| 260325-kkp | Fix Responsa wildcard (*) ignored with line-break pipe (\|) and line position options | 2026-03-25 | 9ff25977 | [260325-kkp-fix-responsa-wildcard-ignored-when-combi](./quick/260325-kkp-fix-responsa-wildcard-ignored-when-combi/) |
| Phase 54 P01 | 5min | 2 tasks | 6 files |
| Phase 54 P02 | 6min | 2 tasks | 4 files |
| 260326-u9e | Reorganize docs/ folder: archive 28 completed/stale docs, update indexes | 2026-03-26 | 3927e257 | [260326-u9e-reorganize-docs-folder-archive-outdated-](./quick/260326-u9e-reorganize-docs-folder-archive-outdated-/) |
| Phase 54 P03 | 12min | 2 tasks | 3 files |
| Phase 54 P04 | 25min | 2 tasks | 4 files |
| Phase 55 P01 | 2min | 1 tasks | 2 files |

## Session Continuity

Last session: 2026-03-28T19:10:41.344Z
Stopped at: Completed 55-01-PLAN.md
Resume file: None
