# Phase 33: Metadata Enrichment - Context

**Gathered:** 2026-02-16
**Status:** Ready for planning

<domain>
## Phase Boundary

Surface all available scholarly metadata from FIST source database, FJMS sidecar, and NLI crossref. This replaces the original "Fragment Relationships" scope (PartOf: only 30 rows, See: 0 rows -- insufficient data for a phase).

Three categories of work:

**A. New FIST export (needs export script + sidecar import + UI):**
- Bibliography references (733K rows) -- which scholars discussed/mentioned each manuscript, with page numbers, transcription/translation flags, article-level author attribution
- Catalog cross-references (78K rows) -- which published catalogs list each manuscript, with entry numbers
- Reference tables: CODE_Catalog (80 scholarly catalogs), CODE_Title (4,309 publications), CODE_Author (2,969 scholars)

**B. Already-imported data needing UI (in fjms_enrichment.db or nli_crossref.db):**
- FJMS catalog SourceName (466K scholarly source classifications)
- FJMS catalog Title, Author, CopyPlace (sparse but valuable: 1.3-1.8K each)
- NLI CatalogEntry / Neubauer-Cowley numbers (2,919 Oxford entries)
- NLI CollectionName, OBBox/Volume/Folio (physical storage references)
- NLI IsNotGenizah flag (29,081 items in our corpus flagged as non-Genizah)

**C. Cleanup:**
- Delete stale 0-byte nli_data/fjms_enrichment.db

Both web and desktop apps must be updated.

</domain>

<decisions>
## Implementation Decisions

### FIST Bibliography (KEY NEW DATA)
- 733K rows in `dbo_UnitBibliographyReference`, linked via SignatureId chain from AlmaId
- Multi-source: each manuscript can have multiple bibliography entries from different scholars/publications
- User stressed: "FJMS data is multiple sourced -- user can see what team A identified, team B, Scholar C, source D etc."
- Data per entry: publication (TitleId -> CODE_Title), page reference, mention type (Discussion/Mentioned/Index), transcription flag (Full/Partial/None), translation flag, article authors
- MentionType breakdown: 648K "Mentioned", 78K "Discussion" (deeper analysis), 7K "Index"
- Transcription flags: 39K "Full transcription", 81K "Partial transcription"
- Translation flags: 22K with translation info
- Article-level authors via `dbo_BibMultiArticleAuthor` (190K rows) -> CODE_Author (2,969 scholars)
- Display as bibliography section showing: "Author, Title (Year) pp. X-Y [Discussion] [Has transcription]"
- Group by publication for readability when many entries exist

### FIST Catalog Cross-References (KEY NEW DATA)
- 78K rows in `dbo_CatalogMultiCatalogRef`, linking manuscripts to published scholarly catalogs
- Maps to CODE_Catalog (80 catalogs): Baker/Polliack (16K refs), Davis/Outhwaite III (15K), Shivtiel/Niessen (14K), Brody (10K)
- Each entry has CatalogCode -> catalog name, CatalogEntry -> entry number within catalog
- This is the structured "where to find this manuscript in published catalogs" data
- Overlaps with Neubauer-Cowley from NLI crossref (CODE_Catalog has Neubauer-Cowley as one of 80 catalogs)
- Display as catalog references: "Baker/Polliack #1234", "Davis I #567"

### FIST Export Chain
- Join path: AlmaId -> dbo_InventoryAlma -> dbo_Inventory -> dbo_InventorySignature (SetSignatureId) -> dbo_Signature (SignatureId) -> dbo_UnitBibliographyReference
- Same chain to dbo_UnitCatalogRec -> dbo_CatalogMultiCatalogRef for catalog cross-refs
- Export script extends existing `scripts/export_fist_enrichment.py` pattern
- New tables in existing `fist_data/fjms_enrichment.db` sidecar (same DB, new tables)

### FIST Reference Tables
- CODE_Catalog (80 rows): CatalogType, Author, CatAcronym, Title, Domain, Collection -- full publication metadata for each scholarly catalog
- CODE_Title (4,309 rows): FullTitleEng/Heb, RunningTitle, Acronym, City, Year, Publisher, ISBN -- for all referenced publications
- CODE_Author (2,969 rows): EngDesc, HebDesc (bilingual names for all referenced scholars)
- Export as reference/lookup tables in sidecar for resolving IDs to display names

### Neubauer-Cowley catalog references
- 2,919 entries from NLI crossref, all Oxford. Format: "Neubauer - Cowley 2603.1"
- These connect to Oxford's codicological parts system (oxford_part_id in libraries.csv)
- Store as alternative catalog identifiers alongside the shelfmark
- Display in shelfmark/title area AND in metadata section
- Future goal (out of scope): browseable by catalog number ("Oxford Neubauer 2032")
- Future goal (out of scope): extend to other libraries' popular catalog numbering systems
- Note: CODE_Catalog also has Neubauer-Cowley -- these two sources should cross-reference

### IsNotGenizah flag
- 37,934 manuscripts flagged, 29,081 ARE in our corpus (~13% of libraries.csv)
- Mostly from mixed collections: St. Petersburg (15K), BL (8.7K), Mosseri (7.5K), AIU (4K)
- Show as a visual badge/warning on browse page when viewing a non-Genizah item
- No search filter for now (just awareness badge)

### FJMS SourceName
- 466K rows but dominated by generic labels ("Catalogs" 215K, "Institution" 167K)
- Scholarly ones (~50K) provide which scholarly corpus cataloged the item (e.g., "Documentary Material (Goitein)", "Judeo-Arabic Halakhic Literature")
- Different classification axis from Domain -- provenance of who studied it
- User wants to see examples in context before deciding on display approach
- Plan should show SourceName when it differs from the generic labels AND adds info beyond the domain classification
- Defer final presentation decision to implementation (show samples to user during UAT)

### Physical storage references (OBBox/Volume/Folio)
- OBBox: 381K rows (116 distinct box numbers)
- OBVolume: 726K rows (3,217 distinct)
- OBFolio: 771K rows (8,740 distinct)
- Display as secondary metadata (original catalog placement references)

### Layout
- Claude's discretion on where metadata appears (expand existing panel vs. new section)
- Optimize based on how much data each manuscript has
- Bibliography section should be collapsible given potentially many entries per manuscript

### Stale database cleanup
- Delete nli_data/fjms_enrichment.db (0 bytes, stale copy wiped during Phase 34)
- Canonical FJMS data at fist_data/fjms_enrichment.db is healthy and used by FjmsService

</decisions>

<specifics>
## Specific Ideas

- Neubauer-Cowley numbers should eventually be browseable ("Oxford Neubauer 2032") alongside other popular catalog systems -- but that's a future milestone, not this phase
- User mentioned codicological parts connection: CatalogEntry maps to the same Oxford manuscripts we already have oxford_part_id for -- cross-reference this
- SourceName examples show it as scholarly provenance: "Documentary Material (Goitein)" means Goitein's team cataloged this in his documentary corpus
- Bibliography display could show a mini citation: "Goitein, Mediterranean Society vol. 3 (1978) p. 245 [Discussion, Full transcription]"
- When many bibliography entries exist for a manuscript, consider grouping by publication or limiting initial display with "Show all N references" expandable
- Catalog cross-references from FIST overlap with and extend the NLI Neubauer-Cowley data -- FIST has 80 catalogs vs NLI's single Neubauer-Cowley

</specifics>

## Data Inventory

### New FIST Export (Category A)

| Source Table | Rows | Key Fields | Notes |
|-------------|------|------------|-------|
| dbo_UnitBibliographyReference | 733K | SignatureId, TitleId, CatalogId, MentionPage, MentionTypeCode, IsHasTranscription/Translation/ImageCode, ArticleName, Volume | Core bibliography |
| dbo_CatalogMultiCatalogRef | 78K | UnitCatalogRecId, CatalogCode, CatalogEntry, IsSource | Catalog cross-refs |
| CODE_Catalog | 80 | CatalogId, CatAcronym, Author, Title, Domain, Collection, CatalogType | Reference lookup |
| CODE_Title | 4,309 | TitleId, FullTitleEng/Heb, RunningTitleEng/Heb, AcronymEng, City, Year, Publisher | Publication lookup |
| CODE_Author | 2,969 | AuthorId, EngDesc, HebDesc | Scholar lookup |
| dbo_BibMultiArticleAuthor | 190K | UnitBibliographyReferenceId, ArticleAuthorId, AuthorOrder | Article authors |

### Already Imported (Category B)

| Data Source | Field | Fill Rate | Rows Affected | Notes |
|-------------|-------|-----------|---------------|-------|
| NLI crossref | CatalogEntry (Neubauer-Cowley) | 28K images / 2,919 entries | Oxford only | All "Neubauer - Cowley" prefix |
| NLI crossref | CollectionName | 815K (124 distinct) | All libraries | Sub-collection within library |
| NLI crossref | OBBox | 381K (116 distinct) | Subset | Original box reference |
| NLI crossref | OBVolume | 726K (3,217 distinct) | Most | Volume reference |
| NLI crossref | OBFolio | 771K (8,740 distinct) | Most | Folio reference |
| NLI crossref | IsNotGenizah | 305K images / 29,081 in corpus | Mixed-collection libraries | Flag for non-Genizah |
| FJMS catalog | SourceName/Heb | 466K (93%) | Via AlmaId join | Scholarly corpus classification |
| FJMS catalog | Title/TitleHeb | 1.8K (0.4%) | Sparse | FJMS-assigned titles |
| FJMS catalog | AuthorText | 1.6K (0.3%) | Sparse | Author attribution |
| FJMS catalog | CopyPlace | 1.4K (0.3%) | Sparse | Place of copying |

### FIST Join Chain (for export script)

```
dbo_InventoryAlma (AlmaId -> InventoryId)           254K rows
    |
dbo_Inventory (InventoryId PK)
    |
dbo_InventorySignature (InventoryId, SetSignatureId)  1.67M rows
    |
dbo_Signature (SetSignatureId -> SignatureId)         1.56M rows
    |
    +---> dbo_UnitBibliographyReference (SignatureId)   733K rows
    |         +---> CODE_Title (TitleId)                 4.3K rows
    |         +---> CODE_Catalog (CatalogId)             80 rows
    |         +---> dbo_BibMultiArticleAuthor             190K rows
    |                   +---> CODE_Author                 3K rows
    |
    +---> dbo_UnitCatalogRec (SignatureId)               411K rows
              +---> dbo_CatalogMultiCatalogRef            78K rows
                        +---> CODE_Catalog                80 rows
```

### MentionType Code Values

| Code | Meaning | Count |
|------|---------|-------|
| 1035 | Discussion | 78K |
| 2035 | Mentioned | 648K |
| 3035 | Index | 7K |

### Transcription/Translation Code Values

| Code | Meaning |
|------|---------|
| 1036 | Full |
| 2036 | None |
| 3036 | Partial |
| 4036 | Exists |

<deferred>
## Deferred Ideas

- Browseable catalog numbers (search/browse by "Neubauer 2603") -- future milestone
- Extend catalog references to other libraries beyond Oxford -- needs research on available catalog systems
- IsNotGenizah as search filter -- could add later if badge proves useful
- BifolioWith relationships (306K rows, mostly "0") -- numeric FGP IDs linking bifolio pairs, needs further investigation
- PartOf (30 rows) and See (0 rows) -- too sparse to be useful
- FTS5 search over bibliography titles/authors -- future milestone
- CODE_GenizahPerson (2,286 rows) and CODE_GenizahTitle (775 rows) -- people/titles mentioned in texts, needs further investigation of how they link to manuscripts
- dbo_ImgRelation (231K rows) -- image relations, unexplored

</deferred>

---

*Phase: 33-metadata-enrichment*
*Context gathered: 2026-02-16*
