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

---

## Proposed Solution (Updated)

### Phase 1: libraries.csv Generation (the main deliverable)
1. Export 38,673 FIST-only records with: AlmaId → system_number, Shelfmark → call_numbers, CollectionId → library_code (using mapping above)
2. Extract Hebrew titles from FJMS catalog (GenizahTitleOrgTitle for 8,632 records, TitleHeb for 44)
3. Generate pipe-separated shelfmark variants where FIST format differs from convention
4. Append to libraries.csv
5. Skip the 6,145 "Undefined Shelfmarks"

### Phase 2: Shelfmark Normalization Fixes
1. Add `Yevr.` → `EVR` alias to normalize_shelfmark() (fixes 15,594 existing records too)
2. Add `Halper` → `Genizah` alias (534 records)
3. Consider loading all CSV pipe variants in csv_bank (recovers 12,617 matches)
4. Strip leading zeros in shelfmark comparison

### Phase 3: Tantivy Index & Browse Integration
1. Rebuild index with 255K+ records (metadata-only for the 38K additions)
2. Add `has_transcription` flag or similar to distinguish text-searchable vs metadata-only
3. Browse facets (domain/author/work) should auto-expand — test capacity
4. Image viewer and FJMS enrichment already keyed by AlmaId — should work with no code changes

### Key Considerations
- **No transcription text**: Gap records appear in browse/metadata search only, not text search
- **Incremental shipping**: Phase 1 alone makes 38K manuscripts browsable with images + enrichment
- **Performance**: Tantivy index grows ~18%, browse facet counts increase — test load times
- **12 new library codes** needed for small collections (79 records total)
- **14,423 bare records** (images only) still valuable for browse-by-shelfmark and image viewing

## Estimated Scale
- **38,673 new manuscript records** to add to libraries.csv
- **100% image coverage** — all have NLI images ready
- **62.7% enrichment coverage** — most have catalog/domain/bibliography data
- **Phase 2 normalization** also fixes 15,594+ existing shelfmark matches as a side benefit
- Main engineering: CSV generation script + normalize_shelfmark updates + index rebuild
