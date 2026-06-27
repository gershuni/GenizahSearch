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

libraries.csv has **216,942** manuscripts. FIST.db has **260,330** active inventory records. After matching by AlmaId (= system_number), **38,673 FIST manuscripts have no match in libraries.csv** — they're invisible in GenizahSearch.

These missing manuscripts have rich metadata and images but no transcription text. Users can't browse, search, or view them.

### Gap by Library

| Library | Missing | Top collections |
|---------|---------|-----------------|
| JTS | 13,520 | ENA, ENA NS, Schechter |
| CUL | 12,641 | T-S AS, T-S NS |
| Mosseri | 4,862 | Mosseri I-VIII, 2nd Series |
| BL | 2,982 | Or. series |
| Manchester | 1,741 | Gaster, Rylands |
| AIU | 809 | |
| Oxford | 602 | |
| St. Petersburg | 455 | |
| Budapest, MTA | 294 | |
| Lewis-Gibson | 158 | |
| + 42 others | ~609 | |

Additionally, **6,829 FIST records have no AlmaId** (6,145 "Undefined Shelfmarks" — skip).

---

## Pre-Research Results (2026-03-18)

### 1. Image Coverage: 100%

All 38,673 gap manuscripts have NLI images. Zero image gap.

| Metric | Value |
|--------|-------|
| Total images for gap set | 72,263 |
| Average per manuscript | 1.9 |
| 1 image | 16,444 (42.5%) |
| 2 images | 21,237 (54.9%) |
| 3+ images | 992 (2.6%) |

### 2. FJMS Enrichment Coverage

| Enrichment type | Gap records covered | % of 38,673 |
|-----------------|--------------------:|--------------|
| Catalog records | 22,392 | 57.9% |
| Domain classifications | 20,190 | 52.2% |
| Free-text descriptions | 15,677 | 40.5% |
| Running titles | 14,347 | 37.1% |
| Bibliography refs | 13,794 | 35.7% |
| Full scholarly texts | 10,840 | 28.0% |
| Join groups | 2,497 | 6.5% |

- **24,250 (62.7%)** have at least one type of enrichment
- **14,423 (37.3%)** are bare (images only, no enrichment)
- Of 22,392 with catalog: 10,148 have substantive content (title/author/date), 12,244 have only source attribution
- 8,632 have identified works (GenizahTitleId), 2,896 have identified authors
- Only 44 have TitleHeb directly — titles come via GenizahTitleOrgTitle instead

### 3. Library Code Mapping: 53/65 Direct Matches

**53 FIST libraries** map directly to existing library_code values. Key mappings:

| FIST | CSV code | Notes |
|------|----------|-------|
| CUL (233) | `CUL` | Direct |
| JTS (79) | `JTS` | Includes ENA, Lutzki, Schechter |
| St. Peterburg (229) | `RNL` | Direct |
| Manchester (242) | `Manchester` | Includes Gaster sub-collections |
| Oxford (235) | `Oxford` | Direct |
| BL (238) | `BL` | Includes Shapira |
| Mosseri (177) | `Mosseri` | Direct |
| AIU (169) | `AIU` | Direct |
| Lewis-Gibson (248) | `Westminster` | Name difference |
| Budapest, MTA (147) | `HAS` | Name difference |
| Penn CAJS (90) | `Katz` | Institution renamed |
| Frankfurt (130) | `Senckenberg` | Name difference |

**12 FIST libraries need new codes** (79 total records):
- Halpern (49), Solomon H. (15), Reinach (6), Harvard (3), Vatican (1), Copenhagen (1), Mehlman (1), Central Archives (1), Princeton (1), JC Mainz (1), Chapira (1), Temple Israel (1)

**14 CSV codes have no direct FIST library** (50 records): sub-collections (Lutzki, Adler) or catalog references (Allony, AllonyLoew).

### 4. Shelfmark Compatibility: 90.6% Match Rate

For the 215,191 overlapping records:
- **84.8%** match on shortest CSV variant (current csv_bank behavior)
- **90.6%** match when checking all pipe-separated CSV variants
- **9.4%** (20,154) have no shelfmark match — AlmaId-only linking

**Top incompatibilities (ranked by record count):**

| Issue | Records | Fix |
|-------|---------|-----|
| RNL `Yevr.` vs CSV `EVR` prefix | 15,594 | One alias in normalize_shelfmark() |
| JTS internal `MS R/L fol.` vs NLI catalog numbers | 1,776 | AlmaId-only match (unfixable by normalization) |
| CAJS `Halper` vs CSV `Genizah` naming | 534 | Alias in normalize_shelfmark() |
| NLI bare numbers vs `Ms. Heb.` prefix | 432 | Alias |
| Hungarian Academy bare numbers vs `Ms. Kaufmann GEN` | 266 | Alias |
| BL sub-item granularity (per-folio vs whole MS) | 177 | AlmaId match |
| Mosseri 2nd series labeling | 82 | Manual mapping |
| CUL F-series zero-padding | 39 | Strip leading zeros |

**Existing `normalize_shelfmark()` handles**: case folding, slash→dot, whitespace/punctuation removal, Ms. prefix stripping.
**Does NOT handle**: Yevr→EVR, Halper→Genizah, zero-padding, collection name differences.

**csv_bank limitation**: `_load_csv_bank()` stores only the shortest pipe-separated variant, missing 12,617 records that would match on longer variants.

### 5. libraries.csv Column Structure

Only 4 of 8 columns matter. Columns 4-6 are always empty vestigial columns.

| Index | Column | Required? | Source for new rows |
|-------|--------|-----------|-------------------|
| 0 | system_number | YES | FIST `dbo_InventoryAlma.AlmaId` (18-digit, ends `05171`) |
| 1 | oxford_part_id | NO | Empty (Oxford-only, 5.4% of existing rows) |
| 2 | call_numbers | YES | FIST `dbo_Inventory.Shelfmark` (single variant) |
| 3 | library_code | YES | Mapped from FIST `LibraryId` via CODE_Library/CODE_Collection |
| 4-6 | *(empty)* | NO | Always empty |
| 7 | titles_non_placeholder | Optional | FJMS catalog titles where available (very few: 47 Hebrew, 149 English) |

Example new row: `990053030350205171,,ENA NS 77.379,JTS,,,,`

### 6. Tantivy Index: No Changes Needed

The Tantivy index is built exclusively from `Transcriptions.txt` — it only indexes records with actual transcription text. Metadata-only records are **not** in the index and don't need to be.

**What already works without Tantivy changes:**
- **Catalog browse** (domain/author/work facets) — reads csv_bank + FJMS enrichment, not Tantivy
- **Image viewer** — keyed by AlmaId via nli_crossref.db, independent of Tantivy
- **FJMS enrichment** (catalog dialog, bibliography, joins) — keyed by AlmaId, independent

**One code change needed:** `execute_search()` at line ~6409 has `if not text: continue` for metadata search results, which skips records with no transcription text. This guard needs adjustment so metadata-only records appear in title/shelfmark search results (without transcription preview).

**No index rebuild required** for the 38K additions — only `csv_bank` (loaded from libraries.csv at startup) needs the new rows.

---

## Proposed Solution (Final)

### Phase 1: CSV Generation Script
1. Script reads FIST.db `dbo_Inventory` + `dbo_InventoryAlma` for the 38,673 gap AlmaIds
2. Maps CollectionId → library_code via FIST CODE_Library/CODE_Collection (53 direct mappings + 12 new codes)
3. Extracts shelfmarks from `dbo_Inventory.Shelfmark`
4. Optionally populates title column from FJMS catalog (GenizahTitleOrgTitle for 8,632 records)
5. Appends to libraries.csv (format: `AlmaId,,Shelfmark,library_code,,,,Title`)
6. Skips 6,145 "Undefined Shelfmarks" (no AlmaId)

### Phase 2: Code Adjustments
1. Add 12 new library codes to `LIBRARY_CODES` dict in genizah_core.py
2. Fix `execute_search()` metadata guard — allow metadata-only results in title/shelfmark search
3. Optional: add `Yevr.` → `EVR` alias to normalize_shelfmark() (bonus: fixes 15,594 existing matches)
4. Optional: add `Halper` → `Genizah` alias (534 records)

### Phase 3: Validation & Testing
1. Verify new records appear in catalog browse with correct domains/facets
2. Verify images load for sample gap records across all major libraries
3. Verify FJMS catalog dialog works for gap records
4. Test shelfmark search returns gap records
5. Test text search (Responsa) correctly excludes metadata-only records
6. Performance test: app startup time with 255K csv_bank entries

### Key Properties
- **No Tantivy index rebuild** — gap records are browse/metadata-only
- **No transcription text** — records don't appear in text search (correct behavior)
- **100% image coverage** — every gap record has NLI images
- **62.7% enrichment** — most have catalog/domain/bibliography already
- **Incremental**: Phase 1 alone (CSV generation) makes records browsable
- **Side benefit**: Shelfmark normalization fixes improve 15K+ existing record matches

## Estimated Effort
- CSV generation script: ~2 hours (SQL export + formatting)
- Code adjustments: ~1 hour (LIBRARY_CODES + metadata guard + optional normalization)
- Validation: ~1 hour
- **Total: ~1 session**
