# Phase 53: Fill Missing Genizah Manuscripts from FIST - Context

**Gathered:** 2026-03-18
**Status:** Ready for planning
**Source:** Pre-research from todo + database analysis

<domain>
## Phase Boundary

Add 38,673 Genizah manuscripts from FIST.db that exist in the Friedberg system but are missing from libraries.csv. These manuscripts have NLI images (100%) and FJMS enrichment (62.7%) but no transcription text. After this phase, they will be browsable, viewable with images, and searchable by shelfmark/title metadata.

**In scope:** CSV generation, library code mapping, metadata search guard fix, shelfmark normalization improvements
**Out of scope:** Tantivy index changes (not needed), transcription text for these records, NLI MARC crawl (separate todo)

</domain>

<decisions>
## Implementation Decisions

### Data Source & Gap
- FIST.db `dbo_Inventory` (260,330 active records) vs libraries.csv (216,942) = 38,673 gap records
- Match key: `dbo_InventoryAlma.AlmaId` = libraries.csv `system_number`
- Skip 6,145 "Undefined Shelfmarks" (no AlmaId) and 683 other no-AlmaId records

### libraries.csv Row Format
- Column 0 (system_number): AlmaId from FIST (18-digit, ends `05171`)
- Column 1 (oxford_part_id): Empty for all new rows (Oxford-only field)
- Column 2 (call_numbers): Single shelfmark from `dbo_Inventory.Shelfmark` (FIST only provides one variant)
- Column 3 (library_code): Mapped from FIST LibraryId via CODE_Library/CODE_Collection
- Columns 4-6: Always empty (vestigial)
- Column 7 (titles_non_placeholder): From FJMS catalog where available (very few: 47 Hebrew, 149 English direct titles; 8,632 have GenizahTitleOrgTitle via GenizahTitleId)

### Library Code Mapping (53 direct + 12 new)
Direct mappings (top 12):
- CUL (233) → `CUL`, JTS (79) → `JTS`, St. Peterburg (229) → `RNL`
- Manchester (242) → `Manchester`, Oxford (235) → `Oxford`, BL (238) → `BL`
- Mosseri (177) → `Mosseri`, AIU (169) → `AIU`
- Lewis-Gibson (248) → `Westminster`, Budapest MTA (147) → `HAS`
- Penn CAJS (90) → `Katz`, Frankfurt (130) → `Senckenberg`

New codes needed (79 records total):
- `Halpern` (49 records), `Solomon` (15), `Reinach` (6), `Harvard` (3)
- `Vatican` (1), `Copenhagen` (1), `Mehlman` (1), `CentralArch` (1)
- `Princeton` (1), `JCMainz` (1), `Chapira` (1), `Corwin` (1)

### Tantivy Index — No Changes
- Index built from Transcriptions.txt only, not libraries.csv
- Metadata search (title/shelfmark) queries csv_bank directly
- Catalog browse queries csv_bank + FJMS enrichment directly
- Image viewer keyed by AlmaId via nli_crossref.db

### Metadata Search Fix
- `execute_search()` at ~line 6409: `if not text: continue` skips records with no transcription
- Must adjust this guard so metadata-only records appear in title/shelfmark search
- Records should show with images and metadata but indicate "no transcription available"

### Shelfmark Normalization (Bonus)
- Add `Yevr.` → `EVR` alias in normalize_shelfmark() — fixes 15,594 existing RNL matches
- Add `Halper` → `Genizah` alias — fixes 534 existing CAJS matches
- Optional: load all pipe-separated CSV variants in csv_bank (recovers 12,617 additional matches)

### Claude's Discretion
- Script implementation details (Python, SQL queries, batch processing)
- How to handle GenizahTitleOrgTitle → title column population
- Whether to generate shelfmark variants for new rows or leave as single variant
- Error handling for edge cases (duplicate AlmaIds, malformed shelfmarks)
- Whether the metadata search fix shows a placeholder or just omits the text preview

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Core Code
- `genizah_core.py:125-163` — normalize_shelfmark() function
- `genizah_core.py:1587` — LIBRARY_CODES dict
- `genizah_core.py:2859` — _load_csv_bank() (how libraries.csv is loaded)
- `genizah_core.py:6385-6428` — execute_search() metadata search branch (the `if not text: continue` guard)

### Data Files
- `libraries.csv` — Master metadata file (target for new rows)
- `FIST.db` — Source database (dbo_Inventory, dbo_InventoryAlma, CODE_Library, CODE_Collection)
- `nli_data/nli_crossref.db` — NLI image crossreference (253K AlmaIds)
- `fist_data/fjms_enrichment.db` — FJMS enrichment data (226K catalog AlmaIds)

### Pre-Research
- `.planning/todos/pending/2026-03-18-fill-missing-genizah-manuscripts-from-fist.md` — Full pre-research with gap analysis, image coverage, enrichment stats, library mapping, shelfmark compatibility

</canonical_refs>

<specifics>
## Specific Ideas

### CSV Generation Script
- Export from FIST.db: `SELECT i.InventoryId, i.Shelfmark, ia.AlmaId, i.CollectionId, c.LibraryId FROM dbo_Inventory i JOIN dbo_InventoryAlma ia ON ia.InventoryId = i.InventoryId JOIN CODE_Collection c ON i.CollectionId = c.CollectionId WHERE i.RecordStatus = 0 AND ia.AlmaId NOT IN (SELECT system_number FROM existing_csv)`
- Mapping table: hardcode the 53+12 LibraryId → library_code mappings
- Title extraction: join FJMS catalog's GenizahTitleId to genizah_titles for OrgTitle

### Gap Records by Library (for validation)
| Library | Count |
|---------|-------|
| JTS | 13,520 |
| CUL | 12,641 |
| Mosseri | 4,862 |
| BL | 2,982 |
| Manchester | 1,741 |
| AIU | 809 |
| Oxford | 602 |
| RNL | 455 |
| HAS | 294 |
| Westminster | 158 |

### Image Coverage
- 100% of 38,673 gap records have NLI images
- Average 1.9 images per manuscript
- 72,263 total images

### FJMS Enrichment Coverage
- 24,250 (62.7%) have at least one enrichment type
- 20,190 have domain classifications (52.2%)
- 14,423 are bare (images only, 37.3%)

</specifics>

<deferred>
## Deferred Ideas

- Loading all pipe-separated CSV variants in csv_bank (12,617 match improvement) — separate optimization
- NLI MARC crawl for offline Ktiv metadata — separate todo
- Tantivy indexing of catalog descriptions as searchable text — future enhancement
- Shelfmark variant generation for new rows — nice-to-have, not needed for MVP

</deferred>

---

*Phase: 53-fill-missing-genizah-manuscripts-from-fist*
*Context gathered: 2026-03-18 from pre-research*
