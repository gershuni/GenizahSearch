---
created: 2026-03-18T19:45:41.247Z
title: "Fill missing genizah manuscripts from FIST.db"
area: data
files:
  - genizah_core.py
  - shared/fjms_service.py
  - libraries.csv
---

## Problem

libraries.csv has **216,942** manuscripts. FIST.db has **260,330** active inventory records. After matching by AlmaId (= system_number), **38,679 FIST manuscripts have no match in libraries.csv** — they're invisible in GenizahSearch.

These missing manuscripts have rich metadata (domains, catalog descriptions, bibliography, joins) but no transcription text. Users can't browse, search, or view images for them.

### Gap by Library

| Library | Missing from libraries.csv |
|---------|---------------------------|
| JTS | 13,520 |
| CUL | 12,641 |
| Mosseri | 4,862 |
| BL | 2,982 |
| Manchester | 1,741 |
| AIU | 809 |
| Oxford | 602 |
| St. Petersburg | 455 |
| Budapest, MTA | 294 |
| Lewis-Gibson | 158 |
| + others | ~615 |

Additionally, **6,829 FIST records have no AlmaId at all** (mostly 6,145 "Undefined Shelfmarks" — likely unusable).

## Available Metadata for Gap Records

- **nli_crossref.db**: 253,103 distinct AlmaIds with images (avg 3.2 images/ms). Many gap records likely have NLI images already in the crossref.
- **fjms_enrichment.db**: 226,456 catalog records, 90% have domain classifications. 730K catalog entries, 390K domain assignments, 542K bibliography refs, 303K free-text descriptions, 48K join groups.
- **fjms_translations**: 704K Dicta translations already available.

## Proposed Solution

### Phase 1: Data Export & Gap Analysis
1. Export FIST-only AlmaIds (the 38,679) with their shelfmarks, library codes, collection names
2. Cross-reference against nli_crossref.db to determine image availability
3. Cross-reference against fjms_enrichment.db to determine metadata richness
4. Produce a gap report: how many have images, how many have domains, how many have catalog descriptions

### Phase 2: libraries.csv Extension
1. Generate libraries.csv rows for gap manuscripts using FIST inventory data (Shelfmark, CollectionId → library_code, AlmaId → system_number)
2. Map FIST library/collection codes to existing library_code conventions (CUL, JTS, etc.)
3. Extract Hebrew titles from FJMS catalog where available
4. Append to libraries.csv (or create a supplementary file)

### Phase 3: Search Index & App Integration
1. Add gap records to Tantivy index (metadata-only — shelfmark, title, domain, catalog description as searchable text)
2. Ensure browse page domain/author/work facets include the new records
3. Image viewer should work automatically via existing nli_crossref.db lookups
4. FJMS enrichment (catalog dialog, bibliography, joins) already keyed by AlmaId — should work

### Key Considerations
- **No transcription text**: These records appear in browse/metadata search but NOT in text search (Responsa, word search)
- **Tantivy field**: May need a `has_transcription` flag to let users filter
- **Deduplication**: Match by AlmaId (primary), fallback shelfmark normalization for the 6,829 without AlmaId
- **2,516 libraries.csv-only records** (no FIST match) — investigate separately, may be NLI-only catalog entries
- **Incremental**: Can ship Phase 1-2 first (just making them browsable), Phase 3 later (searchable)

## Estimated Scale
- **38,679 new manuscript records** to add
- Most already have FJMS enrichment and NLI images — integration is largely "connecting existing data"
- Main engineering work is libraries.csv generation + Tantivy re-index + browse page capacity testing
