---
phase: 34-library-iiif-integration
plan: 02
subsystem: data-import
tags: [dpul, princeton, jts, iiif, figgy, ark, catalog-api, sqlite]

# Dependency graph
requires:
  - phase: 29-01
    provides: "nli_crossref.db sidecar database with nli_images table containing JTS shelfmarks"
provides:
  - "scripts/import_jts_dpul.py -- JTS/Princeton DPUL bulk import script with checkpointing"
  - "jts_dpul table in nli_crossref.db with ARK suffixes, Figgy manifest URLs, DPUL catalog URLs"
affects: [34-03, 34-04, 34-05]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "DPUL catalog API two-step lookup: search with exact quoted shelfmark, then fetch item detail for manifest"
    - "Per-leaf-level shelfmark DPUL search (each leaf has unique ARK, not shared per base shelfmark)"
    - "JSON checkpoint file for resumable long-running API imports"

key-files:
  created:
    - "scripts/import_jts_dpul.py"
  modified:
    - "nli_data/nli_crossref.db (jts_dpul table + meta updates)"

key-decisions:
  - "Per-shelfmark search instead of per-base: DPUL assigns unique ARKs per leaf (ENA 2573.1 and ENA 2573.2 have different ARK IDs)"
  - "Exact quoted search (q=\"shelfmark\") for precise matching -- unquoted returns too many false positives"
  - "90.6% match rate on 500 shelfmarks; ~44K total would need ~14 hours at 1 worker"

patterns-established:
  - "DPUL catalog API: GET /cairo_geniza/catalog.json?search_field=all_fields&q=\"{shelfmark}\" for search"
  - "DPUL item detail: GET /cairo_geniza/catalog/{ark_suffix}.json for manifest URL extraction"
  - "JSON field paths: response.document.content_metadata_iiif_manifest_field_ssi for Figgy manifest"

# Metrics
duration: 19min
completed: 2026-02-16
---

# Phase 34 Plan 02: JTS/Princeton DPUL Import Summary

**DPUL catalog API import script with two-step ARK/manifest lookup, JSON checkpointing, and 90.6% match rate on 500 validated shelfmarks**

## Performance

- **Duration:** 19 min
- **Started:** 2026-02-16T03:40:50Z
- **Completed:** 2026-02-16T03:59:49Z
- **Tasks:** 2
- **Files created:** 1

## Accomplishments
- Created import script that searches DPUL catalog API per JTS shelfmark with exact quoted matching
- Two-step lookup extracts ARK suffix from catalog search, then fetches Figgy IIIF manifest URL from item detail
- Validated end-to-end with 500 shelfmarks: 453 found (90.6%), all with manifest URLs and thumbnails
- Checkpoint system enables resumable imports for the full ~44K shelfmark corpus
- CLI with --dry-run, --limit, --resume, --workers options for flexible operation

## Task Commits

Each task was committed atomically:

1. **Task 1: Create JTS/Princeton DPUL import script** - `f1200b94` (feat)
2. **Task 2: Run and validate pipeline** - No code changes (data validation only)

## Files Created/Modified
- `scripts/import_jts_dpul.py` - JTS/Princeton DPUL catalog import with checkpointing, parallel workers, and CLI interface (336 lines)

## Decisions Made
- **Per-shelfmark search over per-base grouping:** The plan suggested grouping by base shelfmark (strip .N suffix) to reduce searches from ~44K to ~9K. Investigation revealed DPUL assigns unique ARK IDs per leaf -- "ENA 2573.1" and "ENA 2573.2" have completely different ARK suffixes. Base-level search ("ENA 2573") returns 0 results because DPUL only stores leaf-level items. Per-shelfmark search is the correct approach.
- **Exact quoted search required:** Unquoted searches like "ENA NS I 1" return 108+ results with poor relevance. Wrapping in quotes ("ENA NS I.1") returns exactly 1 correct result.
- **Table schema kept as planned:** shelfmark PRIMARY KEY with ark_suffix, manifest_url, dpul_url, thumbnail_url -- all fields populated for every matched item.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Per-shelfmark search instead of per-base grouping**
- **Found during:** Task 1 (API investigation)
- **Issue:** Plan assumed base shelfmarks share ARK IDs. DPUL actually stores each leaf as a separate item with unique ARK. Base shelfmark search ("ENA 2573") returns 0 results.
- **Fix:** Search per crossref shelfmark (full leaf-level) instead of per base. Schema stores per-shelfmark results.
- **Files modified:** scripts/import_jts_dpul.py
- **Verification:** 90.6% match rate on 500 shelfmarks confirms correct approach
- **Committed in:** f1200b94 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 bug in plan assumptions)
**Impact on plan:** Increases API calls from ~9K base to ~44K leaf, but yields correct per-leaf ARK mappings. No alternative would work.

## Issues Encountered
- First 20 alphabetical shelfmarks (ENA 1025.x series) are not in DPUL, giving 0% match on initial dry-run test. Random sampling and broader testing confirmed the actual match rate is ~65-90% depending on the shelfmark series.
- Full import of ~44K shelfmarks estimated at ~14 hours with 1 worker (2 API calls per shelfmark at ~1.1s each). Script designed with checkpoint resumption for this long-running operation.

## User Setup Required
None - no external service configuration required. The script can be run standalone:
```bash
python scripts/import_jts_dpul.py --workers 3  # Full import (~5 hours with 3 workers)
python scripts/import_jts_dpul.py --resume      # Resume interrupted import
```

## Next Phase Readiness
- jts_dpul table available with 453 validated rows (partial import demonstrating pipeline)
- Full import can be completed independently by running the script with --resume
- Ready for Plan 03/04/05 to use ARK suffixes and manifest URLs for IIIF image integration

## Self-Check: PASSED

- [x] scripts/import_jts_dpul.py exists
- [x] Commit f1200b94 found in git log
- [x] jts_dpul table has 453 rows in nli_crossref.db
- [x] DPUL catalog URL resolves (HTTP 200)
- [x] Figgy IIIF manifest returns valid sc:Manifest JSON

---
*Phase: 34-library-iiif-integration*
*Completed: 2026-02-16*
