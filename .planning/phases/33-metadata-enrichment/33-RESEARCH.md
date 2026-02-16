# Phase 33: Metadata Enrichment - Research

**Researched:** 2026-02-16
**Domain:** SQLite sidecar data export, service layer extension, dual-app UI rendering
**Confidence:** HIGH

## Summary

Phase 33 adds comprehensive scholarly metadata to both apps' browse pages. The work divides into three streams: (A) exporting 733K bibliography references and 78K catalog cross-references from the FIST source database into the existing `fjms_enrichment.db` sidecar; (B) surfacing already-imported NLI crossref fields (Neubauer-Cowley, CollectionName, storage references, IsNotGenizah) and FJMS SourceName through new service methods and UI; (C) deleting a stale 0-byte database file.

The codebase has a well-established pattern for all three: `scripts/export_fist_enrichment.py` handles the FIST-to-sidecar pipeline, `shared/fjms_service.py` and `shared/nli_crossref_service.py` provide the service layer, and both apps render metadata in their browse panels (NiceGUI components for web, HTML builders for desktop). The FIST join chain is verified end-to-end and produces clean data. All infrastructure is in place; this phase extends existing patterns with new tables, methods, and UI sections.

**Primary recommendation:** Follow the established export/service/UI three-layer pattern. Export bibliography and catalog data as new tables in the existing sidecar. Add service methods to both FjmsService and NliCrossrefService. Render in both apps following existing section patterns (NiceGUI components for web, HTML builders for desktop).

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**FIST Bibliography (KEY NEW DATA)**
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

**FIST Catalog Cross-References (KEY NEW DATA)**
- 78K rows in `dbo_CatalogMultiCatalogRef`, linking manuscripts to published scholarly catalogs
- Maps to CODE_Catalog (80 catalogs): Baker/Polliack (16K refs), Davis/Outhwaite III (15K), Shivtiel/Niessen (14K), Brody (10K)
- Each entry has CatalogCode -> catalog name, CatalogEntry -> entry number within catalog
- This is the structured "where to find this manuscript in published catalogs" data
- Overlaps with Neubauer-Cowley from NLI crossref (CODE_Catalog has Neubauer-Cowley as one of 80 catalogs)
- Display as catalog references: "Baker/Polliack #1234", "Davis I #567"

**FIST Export Chain**
- Join path: AlmaId -> dbo_InventoryAlma -> dbo_Inventory -> dbo_InventorySignature (SetSignatureId) -> dbo_Signature (SignatureId) -> dbo_UnitBibliographyReference
- Same chain to dbo_UnitCatalogRec -> dbo_CatalogMultiCatalogRef for catalog cross-refs
- Export script extends existing `scripts/export_fist_enrichment.py` pattern
- New tables in existing `fist_data/fjms_enrichment.db` sidecar (same DB, new tables)

**FIST Reference Tables**
- CODE_Catalog (80 rows): CatalogType, Author, CatAcronym, Title, Domain, Collection -- full publication metadata for each scholarly catalog
- CODE_Title (4,309 rows): FullTitleEng/Heb, RunningTitle, Acronym, City, Year, Publisher, ISBN -- for all referenced publications
- CODE_Author (2,969 rows): EngDesc, HebDesc (bilingual names for all referenced scholars)
- Export as reference/lookup tables in sidecar for resolving IDs to display names

**Neubauer-Cowley catalog references**
- 2,919 entries from NLI crossref, all Oxford. Format: "Neubauer - Cowley 2603.1"
- These connect to Oxford's codicological parts system (oxford_part_id in libraries.csv)
- Store as alternative catalog identifiers alongside the shelfmark
- Display in shelfmark/title area AND in metadata section
- Note: CODE_Catalog also has Neubauer-Cowley -- these two sources should cross-reference

**IsNotGenizah flag**
- 37,934 manuscripts flagged, 29,081 ARE in our corpus (~13% of libraries.csv)
- Mostly from mixed collections: St. Petersburg (15K), BL (8.7K), Mosseri (7.5K), AIU (4K)
- Show as a visual badge/warning on browse page when viewing a non-Genizah item
- No search filter for now (just awareness badge)

**FJMS SourceName**
- 466K rows but dominated by generic labels ("Catalogs" 215K, "Institution" 167K)
- Scholarly ones (~50K) provide which scholarly corpus cataloged the item
- Plan should show SourceName when it differs from the generic labels AND adds info beyond the domain classification
- Defer final presentation decision to implementation (show samples to user during UAT)

**Physical storage references (OBBox/Volume/Folio)**
- OBBox: 381K rows (116 distinct box numbers)
- OBVolume: 726K rows (3,217 distinct)
- OBFolio: 771K rows (8,740 distinct)
- Display as secondary metadata (original catalog placement references)

**Layout**
- Claude's discretion on where metadata appears (expand existing panel vs. new section)
- Optimize based on how much data each manuscript has
- Bibliography section should be collapsible given potentially many entries per manuscript

**Stale database cleanup**
- Delete nli_data/fjms_enrichment.db (0 bytes, stale copy wiped during Phase 34)
- Canonical FJMS data at fist_data/fjms_enrichment.db is healthy and used by FjmsService

### Claude's Discretion

- Layout of where metadata appears (expand existing panel vs. new section)
- SourceName display strategy (filtering generic labels, presentation format)
- Bibliography grouping UX (by publication, initial display count, expansion pattern)
- How to handle 0-reference manuscripts vs. 50+ reference manuscripts gracefully
- Whether to denormalize author names into bibliography rows at export time vs. join at query time

### Deferred Ideas (OUT OF SCOPE)

- Browseable catalog numbers (search/browse by "Neubauer 2603") -- future milestone
- Extend catalog references to other libraries beyond Oxford -- needs research on available catalog systems
- IsNotGenizah as search filter -- could add later if badge proves useful
- BifolioWith relationships (306K rows, mostly "0") -- numeric FGP IDs linking bifolio pairs, needs further investigation
- PartOf (30 rows) and See (0 rows) -- too sparse to be useful
- FTS5 search over bibliography titles/authors -- future milestone
- CODE_GenizahPerson (2,286 rows) and CODE_GenizahTitle (775 rows) -- people/titles mentioned in texts, needs further investigation of how they link to manuscripts
- dbo_ImgRelation (231K rows) -- image relations, unexplored
</user_constraints>

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| sqlite3 | Python stdlib | Read/write sidecar databases | All sidecar data is SQLite; established pattern |
| tqdm | existing dep | Progress bars during export | Used by existing export script |
| NiceGUI | existing dep | Web app UI components | All web browse page rendering |
| PyQt6 | existing dep | Desktop app HTML panels | All desktop browse panel rendering |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| csv | Python stdlib | Read libraries.csv for AlmaId validation | Export script coverage check |
| pathlib | Python stdlib | Cross-platform path handling | All file path operations |
| pytest | existing dep | Test service methods | Unit tests for new methods |

### No New Dependencies

This phase uses only existing libraries. No new pip installs needed.

## Architecture Patterns

### Recommended Structure

```
scripts/export_fist_enrichment.py    # EXTEND: add bibliography, catalog_refs, reference tables
shared/fjms_service.py               # EXTEND: add get_bibliography(), get_catalog_refs()
shared/nli_crossref_service.py       # EXTEND: add get_catalog_entry(), get_storage_refs(), get_is_not_genizah()
web/pages/browse.py                  # EXTEND: add bibliography section, catalog refs, badges
genizah_app.py                       # EXTEND: add _build_bibliography_html(), _build_catalog_refs_html()
tests/test_fjms_service.py           # EXTEND: add bibliography/catalog_ref tests
tests/test_nli_crossref_service.py   # EXTEND: add catalog_entry/storage/IsNotGenizah tests
```

### Pattern 1: Export Script Extension (verified from `scripts/export_fist_enrichment.py`)

**What:** Add new export functions following the established batch-insert pattern.
**When to use:** Adding new tables to the sidecar.

```python
# Source: scripts/export_fist_enrichment.py lines 37-94
def export_bibliography(source, target):
    """Export bibliography references from FIST to sidecar."""
    print("Exporting bibliography...")
    target.execute("DROP TABLE IF EXISTS bibliography")
    target.execute("""
        CREATE TABLE bibliography (
            AlmaId TEXT NOT NULL,
            TitleId INTEGER,
            RunningTitle TEXT,
            Year TEXT,
            MentionPage TEXT,
            FromPage TEXT,
            ToPage TEXT,
            Volume TEXT,
            MentionType TEXT,
            TranscriptionType TEXT,
            TranslationType TEXT,
            ArticleName TEXT,
            ArticleAuthor TEXT,
            CatalogAcronym TEXT
        )
    """)
    # ... batch insert pattern with tqdm ...
    target.execute("CREATE INDEX idx_bib_alma ON bibliography(AlmaId)")
```

**Key design decision: Denormalize at export time.** The join chain from AlmaId to bibliography entries with resolved author/title names is 6+ tables deep. Resolving at export time (joining CODE_Title, CODE_Author, CODE_FullCode) and storing display-ready strings avoids complex multi-table joins at query time. This follows the same pattern as the existing domains/catalog tables which pre-resolve all JOINs.

### Pattern 2: Service Method Extension (verified from `shared/fjms_service.py`)

**What:** Add new query methods following the established error-handling pattern.
**When to use:** Adding new data access methods to service classes.

```python
# Source: shared/fjms_service.py lines 110-140
def get_bibliography(self, sys_id: str) -> list[dict]:
    """Get bibliography references for a manuscript."""
    if self._conn is None:
        return []
    try:
        cursor = self._conn.execute(
            "SELECT * FROM bibliography WHERE AlmaId = ? "
            "ORDER BY CASE MentionType WHEN 'Discussion' THEN 0 "
            "WHEN 'Mentioned' THEN 1 ELSE 2 END",
            (sys_id,),
        )
        return [dict(row) for row in cursor]
    except Exception as e:
        logger.error(f"FjmsService.get_bibliography error for {sys_id}: {e}")
        return []
```

### Pattern 3: Web Browse Page Section (verified from `web/pages/browse.py` lines 2022-2130)

**What:** Add UI sections using NiceGUI components following the FJMS Catalog pattern.
**When to use:** Adding new metadata sections to the browse page.

```python
# Source: web/pages/browse.py lines 2022-2032 (FJMS Catalog section header pattern)
ui.separator().classes('my-3')
with ui.row().classes('items-center gap-2 mb-2'):
    h3(tr('Bibliography References'), classes='text-xs font-bold',
       style='color: var(--text-secondary);')
    ui.badge('FJMS', color='purple').props('outline dense').classes('text-xs')
    if len(bib_entries) > 5:
        ui.badge(str(len(bib_entries)), color='grey').props('dense').classes('text-xs')
```

### Pattern 4: Desktop HTML Builder (verified from `genizah_app.py` lines 8909-9014)

**What:** Build HTML strings for QTextBrowser with border-left styling and theme colors.
**When to use:** Adding new metadata sections to the desktop extended info panel.

```python
# Source: genizah_app.py lines 8965-8968
html = (
    f"<div style='color:{text_color}; padding: 10px; margin-bottom: 10px; "
    "border-left: 3px solid #e67e22; text-align: left;' dir='ltr'>"
    f"<p style='margin-top:0;'><b>{tr('Bibliography')}</b></p>"
)
```

### Pattern 5: NLI Crossref Service Extension (verified from `shared/nli_crossref_service.py`)

**What:** Add query methods for existing nli_images columns not yet surfaced.
**When to use:** Exposing CatalogEntry, CollectionName, OBBox/Volume/Folio, IsNotGenizah.

```python
# Source: shared/nli_crossref_service.py lines 313-344
def get_catalog_entry(self, sys_id: str) -> Optional[dict]:
    """Get Neubauer-Cowley catalog entry for this manuscript."""
    if self._conn is None:
        return None
    try:
        cursor = self._conn.execute(
            "SELECT DISTINCT CatalogAbbrev, CatalogEntry FROM nli_images "
            "WHERE NLI_AlmaId = ? AND CatalogEntry != ''",
            (sys_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return {
            "catalog_abbrev": row["CatalogAbbrev"],
            "catalog_entry": row["CatalogEntry"],
        }
    except Exception as e:
        logger.error(f"... error for {sys_id}: {e}")
        return None
```

### Anti-Patterns to Avoid

- **Multi-table JOINs at query time:** The FIST join chain is 6 tables deep. Never reconstruct this chain in the service layer. Denormalize at export time.
- **Loading all 733K bibliography rows into memory:** Use AlmaId index for per-manuscript queries. The service should NOT have bulk-load methods for bibliography.
- **Separate sidecar for bibliography:** Use the existing `fjms_enrichment.db`. Don't create a new database file -- the user decision explicitly says "same DB, new tables."
- **Rendering 50+ bibliography entries without collapse:** The user specified collapsible bibliography. Always limit initial display and provide expansion.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| MentionType code resolution | Custom code lookup at query time | Pre-resolve at export via CODE_FullCode JOIN | 3 codes only, resolved once |
| Transcription/Translation flags | Custom enum mapping | Pre-resolve at export via CODE_FullCode JOIN | 4 codes each, resolved once |
| Author name lookup | ArticleAuthorId -> CODE_Author JOIN at runtime | Denormalize into bibliography table at export | 190K rows, avoid runtime JOINs |
| Publication title lookup | TitleId -> CODE_Title JOIN at runtime | Denormalize RunningTitle + Year at export | 4.3K titles, resolved once |
| Catalog acronym lookup | CatalogCode -> CODE_Catalog JOIN at runtime | Denormalize CatAcronym at export | 80 catalogs, resolved once |

**Key insight:** The FIST data model is heavily normalized (academic relational design). For a read-only sidecar, denormalization at export time is strictly better -- simpler queries, faster reads, no JOIN overhead, and the export script only runs once.

## Common Pitfalls

### Pitfall 1: MentionTypeCode is Negative in Some Rows
**What goes wrong:** The sample data shows `MentionTypeCode = -1035` (negative) in some rows. This is likely a FIST convention for different states.
**Why it happens:** FIST stores negated codes for certain states (possibly "unverified" or "auto-assigned" mentions).
**How to avoid:** Use `ABS(MentionTypeCode)` in the export query, or handle both positive and negative values when joining to CODE_FullCode. The CODE_FullCode table stores only positive `ComputedCode` values (1035, 2035, 3035).
**Warning signs:** Missing MentionType values in exported data where MentionTypeCode was non-NULL.

### Pitfall 2: Hebrew Encoding in FIST Source
**What goes wrong:** Sample FIST data shows mojibake for Hebrew text (e.g., `"�' �����"` for article authors).
**Why it happens:** The FIST.db was exported from SQL Server; Hebrew text in some columns may have encoding issues.
**How to avoid:** The `ArticleAuthorDisplay` column in `dbo_BibMultiArticleAuthor` contains the display-ready author name (which may have encoding issues). Instead, prefer the `CODE_Author.EngDesc` / `HebDesc` columns which are in proper Unicode. For ArticleName, test encoding before using.
**Warning signs:** Garbled Hebrew characters in exported bibliography data.

### Pitfall 3: Duplicate Bibliography Entries per AlmaId
**What goes wrong:** The join chain can produce duplicates because a single AlmaId may map through multiple InventorySignature paths to the same UnitBibliographyReference.
**Why it happens:** The FIST schema has 1.67M InventorySignature rows and 1.56M Signature rows, creating many-to-many paths.
**How to avoid:** Use `SELECT DISTINCT` on the full join chain, keying on `(AlmaId, UnitBibliographyReferenceId)` to deduplicate. The existing export functions all use `SELECT DISTINCT` for this reason.
**Warning signs:** Manuscript showing the same bibliography entry multiple times.

### Pitfall 4: NLI CatalogEntry Has Empty AlmaIds for Some Oxford Items
**What goes wrong:** Some NLI crossref rows with `CatalogEntry != ''` have empty `NLI_AlmaId`, making them unreachable by sys_id lookup.
**Why it happens:** Some Oxford items in the NLI crossref have shelfmarks but no AlmaId mapping (e.g., `MS canonicimisc. 334/1` has CatalogEntry `2778.1` but empty AlmaId).
**How to avoid:** For Neubauer-Cowley display, query by both `NLI_AlmaId` and `Shelfmark` as fallback. Accept that some Neubauer-Cowley numbers may not be reachable via sys_id alone.
**Warning signs:** Oxford manuscripts showing no Neubauer-Cowley number despite being in the crossref.

### Pitfall 5: Sidecar Version Mismatch
**What goes wrong:** Old sidecar files missing new tables cause `sqlite3.OperationalError: no such table: bibliography`.
**Why it happens:** Users may not rebuild the sidecar after updating code.
**How to avoid:** Wrap new table queries in try/except at the service level (same pattern as manchester_luna/jts_dpul tables in NliCrossrefService). Check table existence before querying, or gracefully degrade.
**Warning signs:** Stack traces mentioning missing tables in production.

### Pitfall 6: IsNotGenizah is Image-Level, Not Manuscript-Level
**What goes wrong:** The `IsNotGenizah` flag is on individual image rows in `nli_images`. A single AlmaId may have some images flagged and others not.
**Why it happens:** NLI crossref data is at the image level. For most manuscripts, all images have the same flag value.
**How to avoid:** Use `SELECT DISTINCT IsNotGenizah FROM nli_images WHERE NLI_AlmaId = ?` and treat as "True" if ANY row is "True". Or, use the count from CONTEXT.md (29,081 distinct manuscripts) -- verified: `SELECT COUNT(DISTINCT NLI_AlmaId) FROM nli_images WHERE IsNotGenizah = 'True'` returns 37,934 total, of which 29,081 are in our libraries.csv.
**Warning signs:** Inconsistent IsNotGenizah display depending on which image row is checked.

## Code Examples

### Export Bibliography with Denormalized Fields (verified join chain)

```python
# Verified: this exact JOIN chain produces clean results
# Source: FIST.db inspection, 2026-02-16
cursor = source.execute("""
    SELECT DISTINCT
        TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
        bib.UnitBibliographyReferenceId,
        t.RunningTitleEng,
        t.TitleYearEng,
        bib.MentionPage,
        bib.FromPage,
        bib.ToPage,
        bib.Volume,
        fc.EngDesc as MentionType,
        ft.EngDesc as TranscriptionType,
        fl.EngDesc as TranslationType,
        bib.ArticleName,
        a.EngDesc as ArticleAuthorEng,
        a.HebDesc as ArticleAuthorHeb,
        t.AcronymEng as TitleAcronym,
        cat.CatAcronym as CatalogAcronym
    FROM dbo_InventoryAlma alma
    JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
    JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
    JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
    JOIN dbo_UnitBibliographyReference bib ON sig.SignatureId = bib.SignatureId
    LEFT JOIN CODE_Title t ON bib.TitleId = t.TitleId
    LEFT JOIN CODE_FullCode fc ON ABS(bib.MentionTypeCode) = fc.ComputedCode
    LEFT JOIN CODE_FullCode ft ON bib.IsHasTranscriptionCode = ft.ComputedCode
    LEFT JOIN CODE_FullCode fl ON bib.IsHasTranslationCode = fl.ComputedCode
    LEFT JOIN dbo_BibMultiArticleAuthor baa
        ON bib.UnitBibliographyReferenceId = baa.UnitBibliographyReferenceId
        AND baa.AuthorOrder = 1
    LEFT JOIN CODE_Author a ON baa.ArticleAuthorId = a.AuthorId
    LEFT JOIN CODE_Catalog cat ON bib.CatalogId = cat.CatalogId
""")
```

**Note:** `ABS(bib.MentionTypeCode)` handles the negative code values observed in the data.

### Export Catalog Cross-References

```python
# Verified join chain for catalog cross-references
cursor = source.execute("""
    SELECT DISTINCT
        TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
        cat.CatAcronym,
        cat.Author as CatalogAuthor,
        cat.Title as CatalogTitle,
        ccr.CatalogEntry,
        ccr.IsSource
    FROM dbo_InventoryAlma alma
    JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
    JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
    JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
    JOIN dbo_UnitCatalogRec ucr ON sig.SignatureId = ucr.SignatureId
    JOIN dbo_CatalogMultiCatalogRef ccr ON ucr.UnitCatalogRecId = ccr.UnitCatalogRecId
    JOIN CODE_Catalog cat ON ccr.CatalogCode = cat.CatalogId
""")
```

### Reference Table Export (CODE_Catalog, CODE_Title, CODE_Author)

```python
# Small reference tables: export entire table
target.execute("DROP TABLE IF EXISTS ref_catalogs")
target.execute("""
    CREATE TABLE ref_catalogs (
        CatalogId INTEGER PRIMARY KEY,
        CatalogType TEXT, Author TEXT, CatAcronym TEXT,
        Title TEXT, Domain TEXT, Collection TEXT
    )
""")
cursor = source.execute(
    "SELECT CatalogId, CatalogType, Author, AuthorAcronym, CatAcronym, "
    "Title, Domain, Collection FROM CODE_Catalog"
)
target.executemany(
    "INSERT INTO ref_catalogs VALUES (?, ?, ?, ?, ?, ?, ?)",
    [(r[0], r[1], r[2], r[4], r[5], r[6], r[7]) for r in cursor]
)
```

### NLI Crossref: IsNotGenizah Service Method

```python
def get_is_not_genizah(self, sys_id: str) -> bool:
    """Check if this manuscript is flagged as non-Genizah."""
    if self._conn is None:
        return False
    try:
        cursor = self._conn.execute(
            "SELECT DISTINCT IsNotGenizah FROM nli_images "
            "WHERE NLI_AlmaId = ? AND IsNotGenizah = 'True' LIMIT 1",
            (sys_id,),
        )
        return cursor.fetchone() is not None
    except Exception as e:
        logger.error(f"...error for {sys_id}: {e}")
        return False
```

### Web Badge for IsNotGenizah

```python
# In browse.py, near shelfmark display:
if is_not_genizah:
    ui.badge(tr('Not Genizah'), color='orange').props(
        'outline dense'
    ).classes('text-xs').tooltip(
        tr('This item may not be from the Cairo Genizah')
    )
```

### Desktop Badge for IsNotGenizah

```python
# In genizah_app.py, near shelfmark label:
if is_not_genizah:
    label_text += (
        f" <span style='background:#fff3e0; color:#e65100; "
        f"padding:1px 4px; border-radius:3px; font-size:10px;'>"
        f"{tr('Not Genizah')}</span>"
    )
```

## Data Coverage Analysis

### Bibliography Coverage

| Metric | Value |
|--------|-------|
| Total bibliography rows | 733,209 |
| Distinct AlmaIds with bibliography | 133,019 |
| Our corpus (libraries.csv) | 216,906 AlmaIds |
| Estimated overlap | ~61% of our corpus has bibliography |

### Catalog Cross-Reference Coverage

| Metric | Value |
|--------|-------|
| Total catalog cross-ref rows | 78,376 |
| Distinct AlmaIds with catalog refs | 56,705 |
| Estimated overlap | ~26% of our corpus has catalog refs |
| Number of catalogs | 80 (CODE_Catalog) |

### NLI Crossref Metadata Coverage (already imported, needs UI)

| Field | Distinct AlmaIds | Notes |
|-------|-----------------|-------|
| IsNotGenizah='True' | 37,934 (29,081 in corpus) | ~13% of corpus |
| CatalogEntry (Neubauer-Cowley) | 10,369 | Oxford only, via NLI_AlmaId |
| CollectionName | ~all 815K images | 124 distinct values |
| OBBox | subset | 116 distinct values |
| OBVolume | most | 3,217 distinct values |
| OBFolio | most | 8,740 distinct values |

### Sidecar Size Estimates

Current `fjms_enrichment.db` is 150 MB (domains: 391K, joins: 49K, catalog: 501K, FTS5).

New tables estimated additions:
- bibliography: ~733K rows x ~200 bytes avg = ~140 MB
- catalog_refs: ~78K rows x ~80 bytes avg = ~6 MB
- ref_catalogs: 80 rows = negligible
- ref_titles: 4,309 rows = negligible
- ref_authors: 2,969 rows = negligible

**Estimated new sidecar size: ~300 MB** (after VACUUM). This is acceptable for a local sidecar distributed with the app.

## Layout Recommendation (Claude's Discretion)

### Web Browse Page: Section Order

The browse page metadata panel (`web/pages/browse.py` lines 1870-2210) currently shows:
1. Basic metadata (title, pages, FL ID, material, folio count)
2. Oxford metadata (part title, contents, provenance)
3. External links
4. PGP document info
5. FJMS Catalog (title, author, date, place, textual frames)
6. FJMS Domain Classifications
7. Related Fragments

**Recommended additions (in order):**

After "FJMS Domain Classifications" (item 6), add:
- **Neubauer-Cowley** (if Oxford): display inline near shelfmark area at top, also in a dedicated row
- **Bibliography References** (new FJMS section): collapsible, show first 5 entries, "Show all N" expansion
- **Catalog Cross-References** (new FJMS section): show all entries (typically 1-5 per manuscript)
- **Scholarly Source** (FJMS SourceName): show when non-generic, inline after domains
- **Collection & Storage** (NLI): CollectionName, OBBox/Volume/Folio as secondary metadata
- **IsNotGenizah badge**: near shelfmark, not in a separate section

### Desktop Extended Info Panel: Section Order

Currently (`genizah_app.py` lines 8749-8752):
1. FJMS Domain Classifications (purple border-left)
2. FJMS Catalog Metadata (purple border-left)
3. KTI/Oxford/Cambridge enrichment
4. PGP metadata (green border-left)

**Recommended additions:**
- **Bibliography References** (orange border-left, after FJMS Catalog)
- **Catalog Cross-References** (teal border-left, after Bibliography)
- **IsNotGenizah badge**: in browse info label (near shelfmark)
- **Neubauer-Cowley**: in browse info label for Oxford manuscripts
- **Collection & Storage**: after physical metadata in enrichment section

### Bibliography Display Format

```
Bibliography References [FJMS] [47]
─────────────────────────────────────
  Goitein, Mediterranean Society vol. 3 (1978) pp. 245-248
    [Discussion] [Full transcription]
  Gil, Palestine during the First Muslim Period (1992) p. 302
    [Mentioned]
  Friedman, Jewish Marriage in Palestine vol. 2 (1980) pp. 15-20
    [Discussion] [Full transcription] [Has translation]
  ...
  ▼ Show all 47 references
```

When grouped by publication:
```
  Mediterranean Society vol. 3 (Goitein, 1978):
    pp. 245-248 [Discussion, Full transcription]
    pp. 301-303 [Mentioned]
```

### Catalog Cross-Reference Display Format

```
Catalog References [FJMS]
  Baker/Polliack #1234
  Davis/Outhwaite III #567
  Brody #2891
  Neubauer-Cowley 2603.1  ← from NLI crossref (overlay with FIST data)
```

## Open Questions

1. **SourceName filtering threshold**
   - What we know: 466K rows, but ~382K are generic ("Catalogs", "Institution")
   - What's unclear: Exact list of generic labels to filter out
   - Recommendation: Build a GENERIC_SOURCE_NAMES set (`{'Catalogs', 'Institution', 'Collection'}`) and skip display for these. Show remaining ~50K scholarly source names. Let user see samples during UAT.

2. **Bibliography grouping: by publication or flat list?**
   - What we know: User wants grouping "for readability when many entries exist"
   - What's unclear: At what count threshold to switch from flat to grouped
   - Recommendation: Show flat list for <= 10 entries. Group by RunningTitle for > 10 entries. Always show "Discussion" entries first (they're richer).

3. **Neubauer-Cowley: overlap between NLI crossref and FIST CODE_Catalog**
   - What we know: NLI has 10,369 distinct AlmaIds with CatalogEntry; FIST CODE_Catalog includes Neubauer-Cowley as one of 80 catalogs
   - What's unclear: Whether the same manuscripts appear in both sources
   - Recommendation: Display NLI Neubauer-Cowley data first (it's already imported). FIST catalog_refs will include Neubauer-Cowley entries too -- merge/deduplicate at display time by checking if CatAcronym matches "Neubauer" or similar.

4. **Export script: idempotent rebuild or additive?**
   - What we know: Current script deletes existing target entirely and rebuilds
   - What's unclear: Whether user wants to keep existing tables when adding new ones
   - Recommendation: Keep the full-rebuild approach (idempotent). Bump version to 2.0.0 since schema changes. Add new export functions, call them in main() alongside existing ones.

## Sources

### Primary (HIGH confidence)
- `scripts/export_fist_enrichment.py` -- verified export pattern, batch sizes, index creation
- `shared/fjms_service.py` -- verified service pattern, error handling, singleton, batch queries
- `shared/nli_crossref_service.py` -- verified crossref service pattern, all 25 columns available
- `FIST_DB_BACKUP/FIST.db` -- verified join chain end-to-end, all column names and types
- `nli_data/nli_crossref.db` -- verified schema, IsNotGenizah distribution, CatalogEntry values
- `fist_data/fjms_enrichment.db` -- verified current schema and file size
- `web/pages/browse.py` -- verified current UI section rendering pattern
- `genizah_app.py` -- verified desktop HTML builder pattern
- `web/services.py` -- verified BrowsePage dataclass and get_browse_page enrichment flow
- `genizah_core.py` -- verified enrich_metadata flow and crossref integration

### Verified Data Points
- Bibliography join chain produces clean data (10 sample rows verified)
- `ABS(MentionTypeCode)` needed for negative code values
- CatalogEntry in NLI crossref is exclusively "Neubauer - Cowley" prefix
- Some Oxford CatalogEntry rows have empty NLI_AlmaId
- Stale `nli_data/fjms_enrichment.db` already deleted (confirmed via ls)
- 133,019 distinct AlmaIds have bibliography data
- 56,705 distinct AlmaIds have catalog cross-references

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all existing libraries, no new dependencies
- Architecture: HIGH -- extends well-established patterns (export/service/UI)
- Pitfalls: HIGH -- verified against actual data, edge cases identified from real queries
- Data coverage: HIGH -- verified all counts against live databases

**Research date:** 2026-02-16
**Valid until:** 2026-03-16 (stable -- no external API dependencies, all local data)
