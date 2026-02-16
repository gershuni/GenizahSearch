# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-15)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v5.9.0 Multi-Source Image & Metadata Integration

## Current Position

Phase: 34 of 34 (Library IIIF Integration)
Plan: 5 of 5 in current phase
Status: Phase 34 complete -- all 5 plans executed
Last activity: 2026-02-16 -- Completed 34-05 (Desktop ManuscriptViewer Manchester/JTS)

Progress: [████████████████████] 100%

## Performance Metrics

**Velocity:**
- Total plans completed: 95 (across all milestones)
- Average duration: ~8 min
- Total execution time: ~9.6 hours

**By Milestone:**

| Milestone | Phases | Plans | Total Time |
|-----------|--------|-------|------------|
| v1 | 1-7 | 18 | 173 min |
| v5.6.0 | 8-12 | 25 | ~134 min |
| v5.7.0 | 14-17 | 14 | ~140 min |
| v5.7.2 | 18-21 | 11 | ~1 day |
| v5.7.3 | 22-24 | 3 | 6 min |
| v5.8.0 | 25-28 | 12 | 57 min |
| v5.9.0 | 29-34 | 13 | 30 min |
| Phase 31 P01 | 5min | 2 tasks | 5 files |
| Phase 31 P02 | 7min | 2 tasks | 4 files |
| Phase 31 P03 | 3min | 2 tasks | 3 files |
| Phase 32 P01 | 3min | 2 tasks | 5 files |
| Phase 32 P02 | 2min | 2 tasks | 2 files |
| Phase 32 P03 | 1min | 2 tasks | 2 files |
| Phase 34 P01 | 7min | 2 tasks | 1 file |
| Phase 34 P02 | 19min | 2 tasks | 1 file |
| Phase 34 P03 | 3min | 2 tasks | 3 files |
| Phase 34 P04 | 4min | 2 tasks | 3 files |
| Phase 34 P05 | 2min | 2 tasks | 1 file |

## Accumulated Context

### Decisions

See PROJECT.md Key Decisions table for full history.

- 29-01: Separate sidecar file (nli_crossref.db) rather than adding to fjms_enrichment.db -- different provenance and update cycles
- 29-01: All 25 NLI CSV columns stored as TEXT -- no filtering per user decision
- 29-01: CUDL label normalization: strip MS- prefix, split by dash, strip leading zeros, rejoin with dots between numerics
- 29-02: Followed FJMS service pattern exactly for NliCrossrefService -- same _find_project_root(), URI read-only mode, thread_safe param, singleton
- 29-02: get_image_sources combines NLI FGP and Cambridge checks in single call for efficient UI badge rendering
- 30-01: FGPImageNumberId values used directly as FL IDs (no transformation needed)
- 30-01: Local sidecar resolution added as first-try path, all existing network fallback logic preserved unchanged
- 30-02: Single crossref_svc initialization in enrich_metadata serves both Cambridge supplement and NLI FL ID paths
- 30-02: Cambridge supplement sets external_url on current_meta when found from sidecar, feeding into existing CUDL fetch logic
- [Phase 31]: Folio label falls back: crossref folio_label -> extract_folio_number -> Page N
- [Phase 31]: Source indicator chips are styled flat buttons with colored borders, not NiceGUI chip component
- 31-02: KTIV button uses QPushButton with green chip styling matching web NLI indicator
- 31-02: image_source_info and folio_images added to enrich_metadata for both web and desktop
- 31-02: btn_external visibility fixed (was always hidden, now shows for Cambridge/Oxford)
- 31-03: Cambridge proxy fetches images_ext from nli_cache -- no new canvas discovery needed
- 31-03: Source chips toggle only when both NLI and Cambridge available; single-source keeps external-link behavior
- 32-01: Search-based fallback URLs for CUL, JTS, Manchester, BL (no reliable direct-link patterns)
- 32-01: Guard against duplicate links when Oxford/Cambridge already shown via existing external_url path
- 32-01: Material value passed through tr() for Hebrew translation of Paper/Parchment/Vellum
- 32-02: Physical metadata and library URL added to enrich_metadata crossref block for unified desktop/web enrichment
- 32-02: phys_html prepended before KTI table in desktop extended info for consistent top-of-panel display
- 32-03: Manchester servlet/view/search path (servlet/s/ non-functional), BL searcharchives.bl.uk with leaf stripping (manuscripts site down), JTS cairo_geniza slug (geniza returned 404)
- 32-03: BL spaces URL-encoded not underscored -- verified underscores return zero results on searcharchives
- 34-01: LUNA id and identity fields are identical; JRL filename from urlSize0 lowercased matches crossref ImageSourceName
- 34-01: Sidecar extension pattern: new table in existing nli_crossref.db, version bumped to 1.1.0
- 34-01: 27,940 LUNA items imported, 83.9% manuscript match rate (11,321/13,496)
- 34-02: Per-shelfmark DPUL search, not per-base -- each leaf has unique ARK ID in DPUL
- 34-02: Exact quoted search (q="shelfmark") required for precise DPUL matching
- 34-02: 90.6% match rate on 500 JTS shelfmarks; 453 with Figgy manifest URLs
- 34-03: Manchester detail URL via luna.manchester.ac.uk/luna/servlet/detail/{luna_id}, JTS via dpul_url from sidecar
- 34-03: external_provider key ('manchester'/'jts') set in enrich_metadata for UI labeling
- 34-03: JTS shelfmark lookup tries full then base (strip .N suffix); Manchester uses ImageSourceName JOIN
- 34-04: Reuse cambridge_images (images_ext) for all external providers, external_provider key differentiates proxy endpoint
- 34-04: Manchester pink (#e91e63), JTS orange (#ff9800) chip colors; toggle when NLI + external, link-only when single source
- 34-05: library_viewer_url preferred over raw manifest URL for Manchester/JTS external button in desktop
- 34-05: external_provider explicit key checked first in _detect_external_provider, URL patterns as fallback

### Blockers/Concerns

- Phase 13 (Transcription Search) still deferred -- needs server-side index architecture
- FIST catalogs at unit (codex) level, not individual leaf level -- upstream FIST data design
- IMG-05 (library IIIF fallback) resolved: Manchester LUNA and JTS Figgy IIIF integrated in Phase 34; BL remains search-only (no IIIF endpoint)

### Future Improvements

- FTS5 catalog search UI (schema ready in sidecar, deferred to future milestone)
- FJMS structured metadata search -- leverage TextualFrame tags with FTS5
- Transcription search (Phase 13, needs server-side index architecture)

## Session Continuity

Last session: 2026-02-16
Stopped at: Completed 34-05-PLAN.md (Desktop ManuscriptViewer Manchester/JTS)
Resume file: None
Notes: 6 milestones shipped. v5.9.0 roadmap: 6 phases (29-34), 15 requirements. Phase 34 complete -- all 5 plans executed. Manchester LUNA and JTS/Princeton IIIF fully integrated in service layer, web app, and desktop app.
