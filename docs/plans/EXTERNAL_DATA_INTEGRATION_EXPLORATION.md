# External Data Integration Exploration

> **Status:** Exploration Phase
> **Date:** February 2026
> **Purpose:** Document available external data sources for GenizahSearch enrichment

---

## Executive Summary

This document explores external data sources that can significantly enrich GenizahSearch's metadata and functionality.

### Important Context: Existing Joins System

GenizahSearch already has a **Joins system** implemented (see `docs/specs/JOINS_TECHNICAL_SPEC.md`). This system supports:
- **Physical joins** - Fragments from the same original document
- **Same page** - Different photographs of the same page
- **Same composition** - Different manuscripts of the same literary work
- **Same scribe** - Manuscripts by the same hand

**Why this matters for PGP integration:**
- PGP's database is organized **per document**, not per fragment
- A single PGPID often represents **2+ joined manuscripts** (e.g., "T-S 13J35.3 + AIU VII.A.23")
- PGP transcriptions and descriptions apply to the **joined document**, not individual fragments
- The Joins system was designed to accommodate this - metadata can be associated with join groups

**Integration Approach:**
1. Parse PGP shelfmarks for multi-fragment indicators (`+`, ` + `, etc.)
2. Create join groups for multi-fragment PGPIDs
3. Associate PGP metadata (descriptions, transcriptions, dates) with join groups
4. Link individual GenizahSearch fragments to their PGP-defined join groups

**Reference:** See `docs/specs/JOINS_SIMPLIFIED_SPEC.md` section "Princeton Import Integration" for the planned import function using pairwise joins with `source="princeton"`.

### Data Sources Analyzed

1. **Princeton Geniza Project (PGP) Metadata** - Scholarly descriptions, transcriptions, document classification, dating, and people/places data for ~41,000 documents
2. **NLI CrossReference_Final.csv** - 815,000 image-level records with library metadata, physical relationships, and join keys

Additional data files are expected:
- Joins/relationships file (fragment physical joins)
- Dimensions and number of lines data

---

## Table of Contents

1. [Princeton Geniza Project Metadata](#1-princeton-geniza-project-metadata)
2. [NLI CrossReference Data](#2-nli-crossreference-data)
3. [Cambridge IIIF Collection Manifest](#3-cambridge-iiif-collection-manifest)
4. [Manchester Genizah Collection (LUNA)](#4-manchester-genizah-collection-luna)
5. [Penn/CAJS Cairo Genizah Collection (OPenn)](#5-penncajs-cairo-genizah-collection-openn)
6. [JTS Genizah Collection (via Princeton/FGP)](#6-jts-genizah-collection-via-princetonfgp)
7. [British Library Hebrew Manuscripts](#7-british-library-hebrew-manuscripts)
8. [Join Strategy](#8-join-strategy)
9. [Current GenizahSearch Data Model](#9-current-genizahsearch-data-model)
10. [Enrichment Opportunities](#10-enrichment-opportunities)
11. [Incoming Data Files](#11-incoming-data-files)
12. [Technical Considerations](#12-technical-considerations)
13. [Next Steps](#13-next-steps)
14. [Questions for Future Implementation](#14-questions-for-future-implementation)

---

## 1. Princeton Geniza Project Metadata

### Repository Information

- **URL:** https://github.com/princetongenizalab/pgp-metadata
- **DOI:** 10.5281/zenodo.15839056
- **Update Frequency:** Daily automated exports from PGP database
- **License:** Open data (check repository for specific terms)
- **Source Code:** https://github.com/Princeton-CDH/geniza

### Data Files Overview

| File | Records | Size | Description |
|------|---------|------|-------------|
| `documents.csv` | ~41,000 | 23 MB | Core document metadata |
| `fragments.csv` | ~36,000 | 8.7 MB | Fragment/shelfmark data with IIIF URLs |
| `footnotes.csv` | ~420,000 | 29 MB | Scholarly annotations, transcriptions, translations |
| `people.csv` | ~1,800 | 300 KB | Named individuals mentioned in documents |
| `places.csv` | ~500 | 67 KB | Geographic locations with coordinates |
| `sources.csv` | ~700 | 235 KB | Bibliography/scholarship records |

### documents.csv Schema

| Column | Description | Example |
|--------|-------------|---------|
| `pgpid` | Princeton Geniza Project ID | `1618` |
| `url` | PGP document URL | `https://geniza.princeton.edu/documents/1618/` |
| `iiif_urls` | IIIF manifest URLs (pipe-separated) | `https://cudl.lib.cam.ac.uk/iiif/MS-TS-00012-00147` |
| `fragment_urls` | Direct fragment viewer URLs | `https://cudl.lib.cam.ac.uk/view/MS-TS-00012-00147/1` |
| `shelfmark` | Primary shelfmark | `T-S 12.147` |
| `multifragment` | Multi-fragment indicator | (boolean) |
| `side` | Recto/verso | `recto`, `verso` |
| `region` | Geographic region | |
| `type` | Document type | `Letter`, `Legal document`, `List` |
| `tags` | Subject tags (comma-separated) | `communal, excommunication` |
| `description` | English scholarly description | Full paragraph descriptions |
| `scholarship_records` | Bibliography with HTML links | |
| `shelfmarks_historic` | Historical shelfmark variants | `TS Box 13 J 35, fol. 3` |
| `languages_primary` | Primary language(s) | `Judaeo-Arabic` |
| `languages_secondary` | Secondary language(s) | `Hebrew` |
| `language_note` | Language notes | |
| `doc_date_original` | Original date notation | `1570` |
| `doc_date_calendar` | Calendar system | `Seleucid` |
| `doc_date_standard` | Standardized date | `1258-08-31/1259-09-19` |
| `inferred_date_display` | Display format for inferred date | `1160–1166` |
| `inferred_date_standard` | Standardized inferred date | `1160/1166` |
| `inferred_date_rationale` | Reason for inference | `Person mentioned` |
| `inferred_date_notes` | Dating notes | |
| `initial_entry` | First cataloged date | `1987-12-17 05:00:00+00:00` |
| `last_modified` | Last update timestamp | `2022-09-14 17:08:19+00:00` |
| `input_by` | Contributors | `Amir Ashur ; Catherine Beaumont` |
| `library` | Library/libraries | `CUL ; AIU` |
| `collection` | Collection(s) | `CUL, T-S ; AIU` |
| `has_transcription` | Transcription available | `Y` / `N` |
| `has_translation` | Translation available | `Y` / `N` |

### Multi-Fragment Document Structure

**Critical for Integration:** PGP documents often combine multiple fragments:

| PGPID | Shelfmark | Interpretation |
|-------|-----------|----------------|
| 444 | `T-S 13J35.3 + AIU VII.A.23` | 2 fragments physically joined |
| 448 | `Moss. IV,14.2 + AIU VII.E.119` | 2 fragments from different collections |

**Parsing Requirement:**
```python
# Split multi-fragment shelfmarks
shelfmark = "T-S 13J35.3 + AIU VII.A.23"
fragments = [s.strip() for s in shelfmark.split(' + ')]
# Result: ['T-S 13J35.3', 'AIU VII.A.23']
```

**Integration with Joins System:**
1. For each multi-fragment PGPID, create a join group
2. Add each fragment as a group member
3. Set `relationship_type = 'physical_join'` (most common)
4. Associate PGP metadata with the join group
5. Link to GenizahSearch fragments via shelfmark normalization

### Document Type Distribution

| Type | Count | Notes |
|------|-------|-------|
| Letter | ~15,000+ | Personal and business correspondence |
| Legal document | ~8,000+ | Contracts, court records, deeds |
| List | ~3,000+ | Inventories, accounts |
| Literary | ~2,000+ | Poetry, religious texts |
| State document | ~1,000+ | Official documents |
| Unknown/Unclassified | ~5,000+ | Pending classification |

### Subject Tags (Top 20)

Common tags include: `communal`, `marriage`, `trade`, `Qaraite`, `medicine`, `calendar`, `ketubba`, `betrothal`, `11th c`, `12th c`, `Nahray B. Nissim`, `excommunication`, `physician`, `kosher`, `Shehita`, `Levirate marriage`, `Damascus`, `Fustat`, `Alexandria`

### fragments.csv Schema

| Column | Description |
|--------|-------------|
| `shelfmark` | Primary shelfmark identifier |
| `pgpids` | Associated PGP document ID(s) |
| `shelfmarks_historic` | Historical variants |
| `collection` | Collection identifier |
| `library` | Full library name |
| `library_abbrev` | Library abbreviation (CUL, JTS, etc.) |
| `collection_name` | Full collection name |
| `collection_abbrev` | Collection abbreviation |
| `url` | Fragment viewer URL |
| `iiif_url` | IIIF manifest URL |
| `is_multifragment` | Part of multi-fragment document |
| `created` | Record creation timestamp |
| `last_modified` | Last update timestamp |
| `provenance_display` | Provenance description |
| `provenance` | Structured provenance |
| `material_support` | Material (paper, parchment) |

### footnotes.csv Schema

| Column | Description |
|--------|-------------|
| `document` | Document reference with PGPID |
| `document_id` | Numeric PGPID |
| `source` | Citation |
| `source_slug` | URL-safe source identifier |
| `location` | Location in source (page, card number) |
| `doc_relation` | Relationship type |
| `emendations` | Editorial corrections |
| `notes` | Additional notes |
| `url` | Link to source (e.g., index cards) |
| `content` | **Actual transcription/translation text** |

### Footnote Types (doc_relation)

| Type | Count | Description |
|------|-------|-------------|
| Digital Edition | 7,966 | Digital transcriptions |
| Discussion | 6,804 | Scholarly commentary |
| Edition | 6,448 | Published transcriptions |
| Digital Translation | 1,791 | Digital translations |
| Edition ; Translation | 538 | Combined |
| Translation | 264 | Translations only |

**Note:** ~9,700 footnotes contain actual transcription/translation content in the `content` field.

### people.csv Schema

| Column | Description |
|--------|-------------|
| `name` | Primary name |
| `name_variants` | Alternative names |
| `gender` | Male/Female |
| `social_roles` | Occupations, positions |
| `auto_date_range` | Computed date range |
| `manual_date_range` | Manually assigned dates |
| `description` | Biographical notes |
| `tags` | Classification tags |
| `related_people_count` | Number of related individuals |
| `family_traces_roots_to` | Family lineage |
| `home_base` | Primary location |
| `traveled_to` | Travel destinations |
| `related_documents_count` | Associated documents |
| `url` | PGP person page URL |

### places.csv Schema

| Column | Description |
|--------|-------------|
| `name` | Primary place name |
| `name_variants` | Alternative names (including Arabic) |
| `is_region` | Region vs. specific location |
| `coordinates` | Geographic coordinates |
| `geographic_area` | Broader geographic area |
| `notes` | Historical/geographic notes |
| `related_documents_count` | Associated documents |
| `related_people_count` | Associated people |
| `related_events_count` | Associated events |
| `url` | PGP place page URL |

---

## 2. NLI Data Sources

### Background

The National Library of Israel (NLI) is a key data provider for GenizahSearch:

1. **FGP Integration:** The Friedberg Genizah Project (FGP) data was integrated into NLI
2. **Rosetta Platform:** NLI uses Ex Libris Rosetta for digital preservation
3. **KTIV Portal:** Hebrew manuscripts portal (includes non-Genizah materials)

### KTIV (Hebrew Manuscripts Portal)

**URL:** https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts

**IIIF/MARC Access Patterns:**
- MARC tags: `https://iiif.nli.org.il/IIIFv21/marc/bib/{system_number}`
- Example: `https://iiif.nli.org.il/IIIFv21/marc/bib/997012976364305171`

**Note:** KTIV contains all Hebrew manuscripts, not just Genizah materials. Filter by collection/library as needed.

### CrossReference_Final.csv

- **File:** `CrossReference_Final.csv`
- **Source:** National Library of Israel
- **Records:** 814,955 rows (image-level granularity)
- **Scope:** Comprehensive coverage of major Genizah collections

### Schema

| Column | Index | Description |
|--------|-------|-------------|
| `LibraryNameEng` | 0 | Full library name in English |
| `LibraryAbbrev` | 1 | Library abbreviation |
| `LibraryCity` | 2 | City location |
| `LibraryNameHeb` | 3 | Library name in Hebrew |
| `CollectionName` | 4 | Collection name |
| `Shelfmark` | 5 | Manuscript shelfmark |
| `InventoryId` | 6 | NLI inventory identifier |
| `OBBox` | 7 | Box reference |
| `OBVolume` | 8 | Volume reference |
| `OBFolio` | 9 | Folio reference |
| `NLI_AlmaId` | 10 | **NLI Alma system ID - KEY JOIN FIELD** |
| `CatalogAbbrev` | 11 | Catalog abbreviation |
| `CatalogEntry` | 12 | Catalog entry reference |
| `FGPImageNumberId` | 13 | FGP image number ID |
| `FGPNumber` | 14 | FGP number (e.g., C21365) |
| `ImageName` | 15 | Standardized image filename |
| `ImageSourceName` | 16 | Original image source filename |
| `PartOf` | 17 | Parent fragment/collection |
| `See` | 18 | Cross-reference to related items |
| `BifolioWith` | 19 | Bifolio pair identifier |
| `NumFolio` | 20 | Number of folios |
| `NumBifolio` | 21 | Number of bifolios |
| `Material` | 22 | Paper/Parchment |
| `Size` | 23 | Size category |
| `IsNotGenizah` | 24 | Exclusion flag |

### Library Coverage

| Library | Abbreviation | Image Records |
|---------|--------------|---------------|
| Cambridge University Library | CUL | 314,857 |
| National Library of Russia (St. Petersburg) | St. Petersburg | 271,476 |
| Jewish Theological Seminary | JTS | 83,893 |
| Bodleian Libraries (Oxford) | Oxford | 38,838 |
| University of Manchester | Manchester | 29,931 |
| British Library | BL | 17,223 |
| Alliance Israélite Universelle | AIU | 14,821 |
| Mosseri Collection | Mosseri | 14,090 |
| Undefined Shelfmarks | - | 8,735 |
| Lewis-Gibson Collection | Lewis-Gibson | 3,895 |
| Budapest | Budapest | 3,532 |
| CAJS | CAJS | 2,683 |
| Strasbourg | Strasbourg | 2,247 |
| Hebrew Union College | HUC | 1,723 |
| National Library of Israel | NLI | 1,452 |
| And others... | - | - |

### Relationship Fields Analysis

| Field | Populated Records | Percentage | Notes |
|-------|-------------------|------------|-------|
| `PartOf` | 424,872 | 52% | Parent/child relationships |
| `BifolioWith` | 265,315 | 33% | Bifolio pair identifiers |
| `See` | 19,659 | 2% | Cross-references |

### Data Quality Notes

- **Size field:** 90% empty (735,911 of 814,955 records)
- **Size data issues:** Mixed content (some entries are material types like "Paper", "Vellum" instead of dimensions)
- **PartOf field:** Contains image filenames - may need parsing
- **BifolioWith field:** Contains numeric IDs (likely InventoryIds or FGP IDs)

---

## 3. Cambridge IIIF Collection Manifest

### File Information

- **File:** `cambridge_genizah.json`
- **Source:** https://cudl.lib.cam.ac.uk/collections/genizah/1
- **Format:** IIIF Collection (JSON)
- **Size:** ~707K lines, ~141K manifests

### Structure

```json
{
  "@type": "sc:Collection",
  "manifests": [
    {
      "@type": "sc:Manifest",
      "label": "MS-TS-00012-00147",
      "@id": "http://cudl.lib.cam.ac.uk/iiif/MS-TS-00012-00147"
    },
    ...
  ]
}
```

### Collection Statistics

| Collection Prefix | Manifest Count | Description |
|-------------------|----------------|-------------|
| MS-TS | 135,946 | Taylor-Schechter Collection (main Genizah) |
| MS-MOSSERI | 3,883 | Mosseri Collection (Jacques Mosseri bequest) |
| MS-OR | 1,426 | Oriental Manuscripts |
| MS-ADD | 113 | Additional Manuscripts |
| **Total** | **141,368** | |

### Label to Shelfmark Mapping

The manifest labels use a standardized format that maps to traditional shelfmarks:

| Manifest Label | Shelfmark | Pattern |
|----------------|-----------|---------|
| `MS-TS-00012-00147` | T-S 12.147 | Remove MS-, convert TS→T-S, strip leading zeros, use `.` separator |
| `MS-TS-00013-J-00035-00003` | T-S 13J35.3 | J-series: insert letter after box number |
| `MS-MOSSERI-I-00001` | Mosseri I.1 | Roman numeral series |
| `MS-MOSSERI-IV-00014-00002` | Moss. IV,14.2 | Multi-part items |
| `MS-ADD-03158` | Add. 3158 | Additional manuscripts |
| `MS-OR-01080-J-00093` | Or. 1080 J 93 | Oriental collection |

### Sample Manifests

```
MS-TS-00006-F-00001      → T-S 6F.1
MS-TS-00006-H-00002-00001 → T-S 6H2.1
MS-TS-NS-00321-00008     → T-S NS 321.8
MS-TS-AR-00054-00093     → T-S Ar.54.93
MS-TS-MISC-00035-00178   → T-S Misc.35.178
```

### Value for GenizahSearch

1. **Direct IIIF URLs:** 141K pre-built manifest URLs, no API lookups needed
2. **Complete Coverage:** Entire Cambridge Genizah collection digitized
3. **Instant Validation:** Verify if a shelfmark has Cambridge images
4. **Offline Capability:** No network dependency for URL resolution

### Current Integration Status

GenizahSearch already has CUDL/IIIF integration:

| File | Integration |
|------|-------------|
| `genizah_core.py` | IIIF manifest fetching, view→iiif URL conversion |
| `web/services.py` | External URL handling, CUDL detection |
| `web/pages/browse.py` | Cambridge CUDL link display |
| `genizah_app.py` | Desktop CUDL viewer integration |

**Current approach:** Fetch IIIF data from MARC records or construct URLs dynamically.

**Potential enhancement:** Use `cambridge_genizah.json` as a local lookup table for instant URL resolution without network calls.

### Shelfmark Normalization Function (Proposed)

```python
def manifest_label_to_shelfmark(label: str) -> str:
    """Convert CUDL manifest label to standard shelfmark.

    MS-TS-00012-00147 → T-S 12.147
    MS-MOSSERI-I-00001 → Mosseri I.1
    """
    # Implementation needed
    pass

def shelfmark_to_manifest_label(shelfmark: str) -> str:
    """Convert standard shelfmark to CUDL manifest label.

    T-S 12.147 → MS-TS-00012-00147
    """
    # Implementation needed
    pass
```

---

## 4. Manchester Genizah Collection (LUNA)

### Platform Information

- **URL:** https://luna.manchester.ac.uk/luna/servlet/ManchesterDev~95~2
- **Platform:** Luna DAMS (Digital Asset Management System)
- **Collection Size:** ~27,943 digitized items (~15,000 fragments)
- **Acquisition:** Purchased from Moses Gaster's estate in 1954

### Collection Overview

The University of Manchester Library holds nearly 15,000 fragments from the Genizah of the Ben Ezra Synagogue in Old Cairo. The collection contains important early fragments, including various autographs of Maimonides.

**Date Range:** 10th to 19th century CE
**Earliest Dated Item:** Rylands Genizah 2 (954 CE) - Biblical fragment with Jeremiah

**Languages:**
- Hebrew
- Judeo-Arabic
- Arabic
- Judeo-Spanish

### IIIF Implementation

**Platform:** LUNA with IIIF support

**Manifest URL Pattern:**
```
https://luna.manchester.ac.uk/luna/servlet/iiif/m/{identifier}/manifest
```

**Identifier Format:** `ManchesterDev~95~2~{item_number}~{sequence_number}`

**Example Manifests:**
| Item | Identifier | Description |
|------|------------|-------------|
| Maimonides autograph | `ManchesterDev~95~2~1018~119689` | Mishneh Torah fragment |
| Scorpion amulet | `ManchesterDev~95~2~20994~100545` | Magic amulet |
| Kitab Jami` al-Alfaz | `ManchesterDev~95~2~1202~119524` | Linguistic text |

### Limitations

1. **No Collection-Level Manifest:** Unlike Cambridge, no bulk IIIF collection manifest available
2. **Item List Not Exported:** Would require scraping or library request to obtain full item list
3. **Platform Migration:** Some items moving to Manchester Digital Collections (new platform)

### Alternative Access

**Manchester Digital Collections** (newer platform):
- URL: https://www.digitalcollections.manchester.ac.uk/
- Hebrew Manuscripts available at `/collections/hebrew`
- Genizah not fully migrated yet
- Manifest pattern: `https://www.digitalcollections.manchester.ac.uk/iiif/MS-GENIZAH-{series}-{number}`

### Current GenizahSearch Status

| Data Source | Records | Status |
|-------------|---------|--------|
| CrossReference_Final.csv | 29,931 Manchester images | Available |
| IIIF Integration | None | Potential enhancement |
| Shelfmark Mapping | Via CrossReference | Available |

### Potential Enhancement

To enable Manchester IIIF integration:
1. Request item list from library, or
2. Scrape LUNA interface for identifiers, or
3. Map CrossReference shelfmarks to LUNA identifiers

---

## 5. Penn/CAJS Cairo Genizah Collection (OPenn)

### Platform Information

- **URL:** https://openn.library.upenn.edu/html/genizah_contents.html
- **Platform:** OPenn (Open Penn) - University of Pennsylvania Libraries
- **Collection Size:** 420+ digitized items (from ~600 total fragments)
- **Holding:** Library at the Katz Center (CAJS - Center for Advanced Judaic Studies)

### Collection Overview

The Penn Libraries hold more than 600 medieval manuscript fragments from the Cairo Genizah. The bulk of the collection is known as the Halper Collection, named after librarian Benzion Halper who catalogued them in 1924.

**Date Range:** 9th century CE onward (primarily 10th-14th centuries)
**Languages:** Hebrew, Judeo-Arabic, Samaritan Aramaic, Yiddish

**Content Types:**
- Biblical texts with Masorah
- Liturgical texts
- Letters and correspondence
- Poetry and grammar
- Marriage contracts
- Commercial documents

### Identifier Format

| Type | Format | Example |
|------|--------|---------|
| Halper number | H + sequential | H002, H420 |
| Call number | RAR MS format | RAR MS 82.258.115 |
| Genizah Fragments | GF series | GF60 |

### Data Access Methods

**Per-Item Access:**
| Format | Description |
|--------|-------------|
| Browse (HTML) | Web viewing interface |
| TEI XML | Machine-readable manuscript descriptions |
| Data directory | Complete dataset with images |

**Bulk Access:**
- Anonymous FTP
- Anonymous RSYNC
- wget command-line tool

**Image Formats:**
| Type | Resolution | Format |
|------|------------|--------|
| Master | 400+ ppi | TIFF |
| Web | 1800px max | JPEG |
| Thumbnail | 190px max | JPEG |

### IIIF Status

**Current:** No IIIF manifests available

Images are referenced directly in TEI XML using `<surface>` and `<graphic>` elements:
```xml
<surface n="1r">
  <graphic url="master/h002_0000.tif"/>
  <graphic url="web/h002_0000_web.jpg"/>
  <graphic url="thumb/h002_0000_thumb.jpg"/>
</surface>
```

### Licensing

| Content | License |
|---------|---------|
| Images | Public Domain |
| Metadata | CC0 (Creative Commons Zero) |

### Example Item

**Halper 2:** Genesis 10:14-12:15 with Masorah magna and parva
- Date: 10th-11th century
- Script: Oriental square with Tiberian vocalization
- Material: Parchment, 3 folios
- Size: 20.3 × 20.9 cm
- URL: https://openn.library.upenn.edu/Data/0002/h002/

### Current GenizahSearch Status

| Data Source | Records | Status |
|-------------|---------|--------|
| CrossReference_Final.csv | 2,683 CAJS images | Available |
| libraries.csv | CAJS entries | Available |
| IIIF Integration | None | TEI-based alternative possible |

### Value for Integration

1. **Public Domain Images:** No licensing restrictions
2. **TEI Metadata:** Structured scholarly descriptions
3. **Halper Catalog Numbers:** Well-known reference system
4. **FTP/Bulk Access:** Can download entire collection
5. **Scribes of the Cairo Geniza:** Related crowdsourcing project with transcriptions

### Related Project

**Scribes of the Cairo Geniza** is a multilingual crowdsourcing project for transcribing Genizah fragments, involving Penn, Princeton Geniza Lab, and other institutions. May provide additional transcription data.

---

## 6. JTS Genizah Collection (via Princeton/FGP)

### Institution Information

- **Institution:** Jewish Theological Seminary of America (JTS)
- **Location:** New York, NY
- **Collection Size:** ~43,000 fragments
- **Rank:** Second largest Genizah collection worldwide (after Cambridge)
- **Acquisition:** Elkan Nathan Adler Collection, acquired 1923

### Collection Overview

JTS owns the second largest Cairo Geniza collection in the world, consisting of 43,000 manuscript fragments. Most date between 1000 and 1250 CE, with a sizable minority of Ottoman-period material.

**Shelfmark Format:** ENA (Elkan Nathan Adler) + number
- Example: ENA 1025.1, ENA 1025.2, etc.

### Digital Access Platforms

| Platform | URL | Content | Access Type |
|----------|-----|---------|-------------|
| Princeton DPUL | dpul.princeton.edu/cairo_geniza | ~36,283 items | Public, IIIF |
| Friedberg (FGP) | fgp.genizah.org | 739,868 images | Registration required |
| Princeton Figgy | figgy.princeton.edu | Backend repository | IIIF manifests |

### IIIF Implementation

**Platform:** Princeton Figgy (backend repository)

**Manifest URL Pattern:**
```
https://figgy.princeton.edu/concern/scanned_resources/{UUID}/manifest
```

**Example Manifest:**
```
https://figgy.princeton.edu/concern/scanned_resources/e5313f5e-f2fc-4bdd-a894-7cffac271dfd/manifest
```

**Limitations:**
- No collection-level IIIF manifest available
- Items accessed via catalog browse or search
- UUID-based identifiers (not shelfmark-based)

### Friedberg Genizah Project (FGP)

The JTS collection is also accessible through the Friedberg Genizah Project:

**URL:** https://fgp.genizah.org/

**Total FGP Content:** 739,868 digital images from 60+ libraries

**Access Requirements:**
- Registration required
- Non-commercial use only
- Attribution required for publications

**Note:** FGP is the primary aggregator for Genizah images across institutions.

### Current GenizahSearch Status

| Data Source | Records | Status |
|-------------|---------|--------|
| CrossReference_Final.csv | 83,893 JTS images | Available |
| libraries.csv | JTS library_code entries | Available |
| IIIF Integration | None | Via Princeton Figgy possible |

### DPUL Collection Statistics

| Subcollection | Items |
|---------------|-------|
| Cairo Geniza (total) | 36,283 |
| ENA shelfmarks | 25,453 |
| MS shelfmarks | 1,063 |
| ENA New Series | 148 |
| Coins | 12 |

### Value for Integration

1. **Second Largest Collection:** 43K fragments, essential for comprehensive coverage
2. **ENA Shelfmarks:** Well-documented numbering system
3. **IIIF Access:** Available via Princeton infrastructure
4. **CrossReference Coverage:** 84K images already mapped
5. **PGP Integration:** Many JTS items catalogued in Princeton Geniza Project

### Limitations

1. **No Bulk Manifest:** Cannot download collection-level IIIF manifest
2. **UUID Identifiers:** Figgy uses UUIDs, not shelfmarks
3. **FGP Restrictions:** Registration and non-commercial use only
4. **Platform Fragmentation:** Images across multiple systems

---

## 7. British Library Hebrew Manuscripts

### Institution Information

- **Institution:** British Library
- **Location:** London, UK
- **URL:** https://www.bl.uk/collection/digitised-manuscripts-archives
- **Platform:** BL Digitised Manuscripts with IIIF API

### Collection Overview

The British Library's Hebrew manuscript collection includes:
- **1,300 Hebrew manuscripts** digitized (Polonsky Foundation project)
- **~435,000 digitized images** total
- **Or. (Oriental) collection** containing Genizah fragments
- **GASTER collection** (Moses Gaster materials)

**Date Range:** 10th century CE to early 20th century
**Geographic Scope:** Europe, North Africa, Middle East, Asia

### Shelfmark Format

| Collection | Format | Example |
|------------|--------|---------|
| Oriental | Or. + number | Or. 5565E |
| Gaster | GASTER + number | GASTER 1201.1 |

### ⚠️ Current Status: CYBER ATTACK RECOVERY

**October 2023:** British Library suffered a major ransomware attack affecting all digital systems.

**Recovery Progress (as of late 2024):**
- ~3,000 digitised manuscripts restored
- Phased restoration based on request priority
- Some IIIF endpoints may be unavailable
- Full recovery ongoing

### IIIF Implementation

**Manifest URL Pattern:**
```
https://api.bl.uk/metadata/iiif/ark:/81055/{identifier}/manifest.json
```

**Access Method:**
1. Find item in BL collection guide
2. Open in Universal Viewer
3. Click 'share' link
4. Copy IIIF manifest URL

**Limitations:**
- No collection-level manifest available
- API access may be intermittent during recovery
- Some items not yet restored

### Data Exports

Available via BL Institutional Repository (bl.iro.bl.uk):

| Dataset | Content |
|---------|---------|
| 22 Hebrew MS datasets | TEI XML + 300ppi JPEGs |
| Catalogue records | Structured metadata |
| Torah scroll textiles | Conservation images |

**License:** Public domain / CC-BY with ethical use guidelines

### Current GenizahSearch Status

| Data Source | Records | Status |
|-------------|---------|--------|
| CrossReference_Final.csv | 17,223 BL images | Available |
| Or. collection | 17,068 items | Primary Genizah content |
| Shapira collection | 155 items | Non-Genizah materials |
| IIIF Integration | None | Pending BL recovery |

### Value for Integration

1. **Large Hebrew Collection:** 1,300 MSS, 435K images
2. **Or. Collection:** Significant Genizah holdings
3. **Gaster Materials:** Moses Gaster's collection
4. **TEI Data Exports:** Machine-readable metadata
5. **Public Domain:** Free reuse for most items

### Limitations

1. **Cyber Attack:** Recovery still in progress
2. **No Bulk Manifest:** Individual item access only
3. **API Reliability:** May be intermittent
4. **Complex Identifiers:** ARK-based system

---

## 8. Join Strategy

### Primary Join Keys

```
GenizahSearch (libraries.csv)
    system_number (e.g., "990051334280205171")
         │
         ├──► NLI CrossReference (via NLI_AlmaId)
         │         └── Shelfmark, FGP IDs, Material, Foliation, Relationships
         │
         └──► PGP Data (via normalized shelfmark)
                   └── pgpid, description, type, tags, dates, IIIF URLs
```

### Shelfmark Normalization

Different sources use different shelfmark formats:

| Source | Format | Example |
|--------|--------|---------|
| libraries.csv | Full variants, pipe-separated | `Cambridge University Library Ms. T-S 12.147 \| Ms. T-S 12.147 \| T-S 12.147` |
| PGP | Short form | `T-S 12.147` |
| CrossReference | Short form | `T-S 12.147` |

**Normalization strategy:** Extract shortest variant from libraries.csv (already implemented in `genizah_core.py`)

### Validated Cross-References

| sys_id | Shelfmark | NLI_AlmaId | pgpid | Verified |
|--------|-----------|------------|-------|----------|
| 990051334280205171 | T-S 12.147 | 990051334280205171 | 1618 | ✓ |
| 990051250670205171 | T-S 13J35.3 | 990051250670205171 | 444 | ✓ |

### Join Implementation Options

1. **Pre-computed lookup tables** - Build join tables at startup/index time
2. **Runtime joins** - Query external data on-demand
3. **Hybrid approach** - Index core fields, lazy-load extended metadata

---

## 9. Current GenizahSearch Data Model

### libraries.csv (Primary Metadata Source)

| Column | Index | Description |
|--------|-------|-------------|
| `system_number` | 0 | NLI Alma ID (unique identifier) |
| `oxford_part_id` | 1 | Oxford Neubauer catalog part ID |
| `call_numbers` | 2 | Pipe-separated shelfmark variants |
| `library_code` | 3 | Library abbreviation |
| (empty) | 4-6 | Reserved/legacy |
| `titles_non_placeholder` | 7 | Hebrew title |

**Records:** 216,907

### Tantivy Search Index Schema

| Field | Type | Stored | Description |
|-------|------|--------|-------------|
| `unique_id` | String | Yes | Document identifier |
| `text_normalized` | String | Yes | Normalized searchable text |
| `fingerprint` | String | No | Static fingerprint (rare-letter search) |
| `fingerprint_dyn` | String | No | Dynamic fingerprint |
| `full_header` | String | Yes | `[SysId] Shelfmark @ p.N` |
| `shelfmark` | String | Yes | Shelfmark for filtering |
| `source` | String | Yes | Source version (V0.8/V0.7) |
| `content` | String | Yes | Original text |

### MetadataManager (csv_bank)

Maps `sys_id` to:
- `shelfmark` (shortest call_number variant)
- `title` (titles_non_placeholder)
- `oxford_part_id`
- `library_code`

### SearchResult Dataclass

```python
@dataclass
class SearchResult:
    uid: str                    # Unique ID from search index
    sys_id: str                 # System ID (manuscript)
    display: Dict[str, str]     # {'id', 'shelfmark', 'title', 'library_code'}
    snippet: str                # Text preview
    raw_header: str             # Full header
    source: str                 # 'V0.8' or 'V0.7'
    full_text: str = ''
    library_code: str = ''
```

### BrowsePage Dataclass

```python
@dataclass
class BrowsePage:
    uid: str
    p_num: int
    text: str
    full_header: str
    total_pages: int
    sys_id: str
    fl_id: Optional[str]        # NLI image ID
    shelfmark: str = ''
    title: str = ''
    library_code: str = ''
    library_name: str = ''
    oxford_part_id: Optional[str]
    oxford_part_metadata: Dict[str, str] = {}
```

---

## 10. Enrichment Opportunities

### From PGP documents.csv

| Feature | Field(s) | Impact |
|---------|----------|--------|
| Document Type Classification | `type` | Enable filtering by Letter, Legal, List, etc. |
| Subject Tags | `tags` | Enable topic-based search/filtering |
| English Descriptions | `description` | Display scholarly summaries |
| Dating Information | `doc_date_*`, `inferred_date_*` | Enable date-range filtering |
| Language Metadata | `languages_primary/secondary` | Language-based filtering |
| IIIF URLs | `iiif_urls` | Direct image viewer integration |
| Multi-fragment Links | `multifragment`, shelfmark parsing | Show related fragments |
| Transcription Flags | `has_transcription`, `has_translation` | Indicate available content |

### From PGP footnotes.csv

| Feature | Field(s) | Impact |
|---------|----------|--------|
| Transcription Text | `content` (Digital Edition) | Searchable transcriptions |
| Translations | `content` (Digital Translation) | English translations |
| Scholarly Notes | `notes`, `emendations` | Additional context |
| Bibliography Links | `source`, `url` | Reference materials |

### From PGP people.csv

| Feature | Field(s) | Impact |
|---------|----------|--------|
| Person Search | `name`, `name_variants` | Find documents by person |
| Social Network | `related_people_count` | Relationship exploration |
| Prosopography | `social_roles`, `date_range` | Historical research |

### From PGP places.csv

| Feature | Field(s) | Impact |
|---------|----------|--------|
| Map-based Browse | `coordinates` | Geographic visualization |
| Place Search | `name`, `name_variants` | Find documents by location |

### From NLI CrossReference

| Feature | Field(s) | Impact |
|---------|----------|--------|
| FGP Image IDs | `FGPImageNumberId`, `FGPNumber` | Direct image access |
| Physical Joins | `BifolioWith`, `PartOf`, `See` | Fragment relationships |
| Material Type | `Material` | Paper vs. parchment filtering |
| Foliation | `NumFolio`, `NumBifolio` | Codicological data |
| Comprehensive Coverage | All fields | 815K image records |

### From Cambridge IIIF Manifest

| Feature | Field(s) | Impact |
|---------|----------|--------|
| Direct IIIF URLs | `@id` | Instant manifest URLs, no API calls |
| Image Availability | Presence in manifest | Know if Cambridge has images |
| Shelfmark Validation | `label` | Verify Cambridge shelfmarks |
| Offline Resolution | Local JSON | No network dependency |
| Complete Coverage | 141K manifests | Entire Cambridge Genizah digitized |

### From Penn/CAJS OPenn

| Feature | Field(s) | Impact |
|---------|----------|--------|
| TEI Descriptions | `<msDesc>` | Structured scholarly metadata |
| Public Domain Images | TIFF/JPEG | No licensing restrictions |
| Halper Numbers | Item IDs | Standard reference system |
| Bulk Download | FTP/rsync | Can mirror entire collection |
| High-Res Images | 400+ ppi TIFFs | Research-quality images |

### From JTS/Princeton

| Feature | Field(s) | Impact |
|---------|----------|--------|
| IIIF Manifests | Figgy UUID | Standard image viewer integration |
| ENA Shelfmarks | Catalog IDs | Well-known reference system |
| Second Largest Collection | 43K fragments | Essential coverage |
| PGP Cross-references | pgpid links | Scholarly metadata linkage |
| CrossReference Coverage | 84K images | Already mapped in NLI data |

### From British Library

| Feature | Field(s) | Impact |
|---------|----------|--------|
| TEI XML Metadata | Dataset exports | Machine-readable descriptions |
| Hebrew MS Collection | 1,300 MSS | Significant non-Cambridge coverage |
| Or./Gaster Collections | Shelfmarks | Genizah fragment holdings |
| 300ppi Images | JPEG/TIFF | High-resolution viewing |
| CrossReference Coverage | 17K images | Already mapped in NLI data |

**Note:** BL IIIF API recovering from October 2023 cyber attack.

---

## 11. Incoming Data Files

### 1. Joins/Relationships File (Expected from NLI)

**Purpose:** Structured fragment relationship data

**Expected content:**
- Physical joins (fragments that are physically connected)
- Same composition (fragments from the same original document)
- Scholarly joins (identified by researchers)

**Note:** CrossReference already has relationship data:
- PartOf: 424,872 records
- BifolioWith: 265,315 records
- See: 19,659 records

The new file may provide cleaner, more structured data.

### 2. Dimensions and Number of Lines (Expected from NLI)

**Purpose:** Physical measurements and paleographic data

**Expected content:**
- Height and width measurements
- Number of text lines per page
- Writing area dimensions

**Note:** Current CrossReference `Size` field is:
- 90% empty (735,911 of 814,955 records)
- Has data quality issues (mixed with material types)

New dimension data will be valuable for:
- Codicological research
- Fragment matching
- Search filtering

### 3. Additional NLI Data (Format TBD)

**Purpose:** Additional metadata in NLI's format

**Note:** More data coming from NLI - structure to be documented when files are received.

---

## 12. Technical Considerations

### Storage Options

| Option | Pros | Cons |
|--------|------|------|
| Expand libraries.csv | Simple, single file | Large file, slow startup |
| Separate CSV files | Modular, easy updates | Multiple file management |
| SQLite database | Fast queries, joins | Additional dependency |
| Supabase | Cloud sync, shared | Network dependency |

### Index Strategy

| Approach | Description | Use Case |
|----------|-------------|----------|
| Full indexing | Add all PGP fields to Tantivy | Maximum search capability |
| Core indexing | Index type, tags, dates only | Balanced performance |
| Metadata-only | Keep Tantivy as-is, enrich at display | Minimal changes |

### Update Mechanism

PGP updates daily. Options:
1. **Manual refresh** - Download when needed
2. **Scheduled sync** - Nightly/weekly updates
3. **On-demand** - Fetch fresh data per-session

### Performance Considerations

- libraries.csv: 217K records, ~48MB, loaded at startup
- CrossReference: 815K records - too large for memory
- PGP documents: 41K records - manageable
- Tantivy index: ~290K documents, batched search (5K/batch)

Recommendation: Index core PGP fields, lazy-load CrossReference data.

---

## 13. Next Steps

### Current Status

**Phase:** Exploration Complete

**Documented Sources (7 institutions + NLI):**
1. PGP Metadata (GitHub) - 41K documents, daily updates
2. NLI CrossReference (local) - 815K image records
3. Cambridge IIIF (local) - 141K manifests
4. Manchester LUNA (API) - ~28K items
5. Penn/CAJS OPenn (FTP) - 420+ items
6. JTS/Princeton (API) - 36K+ items
7. British Library (API, recovering) - 17K+ items

### Awaiting from NLI

1. Joins/relationships file
2. Dimensions and number of lines data
3. Additional data in NLI format (TBD)

### When Data Arrives

1. Analyze new file structures
2. Design unified data model
3. Plan integration approach
4. Implement and test

### Open Questions

1. **Priority:** Which enrichment features are most valuable?
2. **UI:** What new filters/displays are needed?
3. **Updates:** How often should PGP data be refreshed?
4. **Storage:** Preferred storage mechanism?

---

## 14. Questions for Future Implementation

> **Note:** This section captures key questions and decision points for future sessions working on this integration. Fresh context may reveal additional considerations.

### A. PGP Import Strategy

**Q1: Import Priority Order**
Which PGP data should be imported first?
- [ ] Joins from multi-fragment shelfmarks (e.g., `T-S 13J35.3 + AIU VII.A.23`)
- [ ] Document type classifications (`Letter`, `Legal document`, `List`, etc.)
- [ ] English descriptions (~41K scholarly summaries)
- [ ] Transcriptions from footnotes.csv (~9,700 records with Hebrew/Aramaic text)
- [ ] Subject tags (100+ tags: `communal`, `marriage`, `trade`, `Qaraite`, etc.)
- [ ] Dating information (original dates, standard dates, inferred dates)

**Q2: Multi-Fragment Parsing**
The current logic parses PGP shelfmarks using `shelfmark.split(' + ')`. Are there other separators in PGP data?
- Known: ` + ` (space-plus-space)
- Check: Does PGP use `+` (no spaces), `/`, `&`, or other separators?
- Reference: See `docs/specs/JOINS_SIMPLIFIED_SPEC.md` section "Princeton Import Integration"

**Q3: PGP Document-to-Join Mapping**
PGP documents often represent **joined fragments** (single PGPID = multiple shelfmarks). How to handle:
- Single-fragment PGPIDs (majority): Direct metadata enrichment
- Multi-fragment PGPIDs: Create join group + associate metadata with group
- What if join group already exists in GenizahSearch? Merge? Update? Skip?

### B. Storage Architecture

**Q4: Where to Store Enrichment Data?**

| Option | Pros | Cons | Decision Needed |
|--------|------|------|-----------------|
| Expand libraries.csv | Simple, single file | 217K→260K records, slower startup | |
| Separate lookup CSV | Modular, easy updates | Multiple file I/O | |
| SQLite database | Fast queries, proper joins | New dependency, packaging | |
| Supabase cloud | Cross-device sync, shared | Network dependency, latency | |

**Q5: Index vs. Runtime Enrichment**
- **Full indexing:** Add PGP fields (type, tags, dates, description) to Tantivy index
  - Pro: Fast full-text search across all fields
  - Con: Larger index, reindexing needed for updates
- **Runtime enrichment:** Keep Tantivy as-is, fetch PGP data on display
  - Pro: No index changes, easy updates
  - Con: Per-result lookup overhead

### C. Shelfmark Normalization

**Q6: Existing Normalization Function**
Does `genizah_core.py` have a shelfmark normalization function that handles all PGP variants?
- Current: Extracts shortest variant from pipe-separated `call_numbers`
- Needed: Normalize PGP shelfmarks to match (spaces, punctuation, prefixes)
- Example variants: `T-S 8J6.1` vs `T-S 8J 6.1` vs `TS 8J6.1`

**Q7: Cross-Reference Key Strategy**
Primary join key is `system_number` → `NLI_AlmaId`. But for PGP:
- PGP has no sys_id field - must join via normalized shelfmark
- What's the best lookup structure? Dict? SQLite index?
- How to handle shelfmark variants that don't match exactly?

### D. NLI Data Integration

**Q8: Timing - Start PGP or Wait for NLI?**
Should implementation start with PGP data now, or wait for NLI files?
- **Start now:** PGP has mature, well-documented data; import can begin
- **Wait:** NLI joins file may have better structure than CrossReference fields
- **Parallel:** Design to accommodate both, implement PGP first

**Q9: NLI CrossReference Relationship Fields**
Current CrossReference has partial relationship data:
- `PartOf`: 424,872 records (52%)
- `BifolioWith`: 265,315 records (33%)
- `See`: 19,659 records (2%)

Questions:
- What's the relationship between these and PGP joins?
- Will the new NLI joins file supersede these fields?
- Should we wait for the cleaner data?

### E. User Interface

**Q10: New Filters and Displays**
What UI elements are needed for enriched data?

| Feature | Priority | Implementation Notes |
|---------|----------|---------------------|
| Document type filter | High | Dropdown or sidebar chips |
| Subject tag filter | High | Multi-select, hierarchical? |
| Date range filter | Medium | Slider or input fields |
| Language filter | Medium | Based on PGP `languages_primary` |
| "Has transcription" filter | Medium | Boolean badge |
| PGP description display | High | Collapsible panel on browse page |
| Map-based browse | Low | Requires places.csv integration |

**Q11: Join Group Metadata Display**
When a user views a fragment that's part of a PGP-defined join:
- Show join group info in browse page?
- Link to other fragments in the join?
- Display PGP description for the entire join, not just fragment?

### F. Cambridge IIIF Lookup

**Q12: Local Manifest Lookup**
`cambridge_genizah.json` has 141,368 IIIF manifests. Currently GenizahSearch fetches IIIF URLs dynamically.

Should we implement local lookup?
- Pro: Instant resolution, no API calls, offline support
- Pro: Can validate Cambridge shelfmarks
- Con: Additional memory (~141K entries)
- Con: Need to keep file updated (Cambridge may add items)

Implementation options:
- Load into Dict at startup
- Use SQLite for memory efficiency
- Lazy-load on first Cambridge request

### G. Technical Architecture

**Q13: Data Model Expansion**
What new fields/classes are needed?

```python
# Proposed additions to SearchResult or new EnrichedResult class
pgpid: Optional[int] = None
document_type: Optional[str] = None  # "Letter", "Legal", etc.
tags: List[str] = []
date_standard: Optional[str] = None
date_original: Optional[str] = None
description_en: Optional[str] = None
has_transcription: bool = False
join_group_id: Optional[str] = None
iiif_url: Optional[str] = None
```

**Q14: Update Mechanism**
PGP updates daily. How to handle updates?
- Manual: Download new files periodically
- Scheduled: Nightly sync script
- On-demand: Check for updates at app startup
- Version tracking: Compare file hashes or dates

### H. Edge Cases and Quality

**Q15: Data Quality Issues**
Known issues to address:
- CrossReference `Size` field: 90% empty, has data quality problems
- Shelfmark variants that don't match between sources
- PGP multi-fragment documents with typos in shelfmarks
- Missing PGPIDs for some GenizahSearch records

**Q16: Orphan Handling**
What to do with:
- GenizahSearch records with no PGP match (~176K records without PGPID)
- PGP records with no GenizahSearch match (unlikely but possible)
- Partial join matches (some fragments found, others missing)

---

## Key Reference Documents

For future sessions continuing this work:

1. **This document:** `docs/plans/EXTERNAL_DATA_INTEGRATION_EXPLORATION.md`
2. **Joins system spec:** `docs/specs/JOINS_TECHNICAL_SPEC.md`
3. **Simplified joins spec:** `docs/specs/JOINS_SIMPLIFIED_SPEC.md` (see "Princeton Import Integration")
4. **Plans index:** `docs/plans/PLANS_INDEX.md`
5. **Local data files:**
   - `libraries.csv` - 217K records, primary metadata
   - `CrossReference_Final.csv` - 815K NLI image records
   - `cambridge_genizah.json` - 141K IIIF manifests

---

## Appendix A: Sample Data

### PGP Document Example (PGPID 1618)

```
pgpid: 1618
shelfmark: T-S 12.147
type: Letter
tags: calendar controversy, Qaraite, communal, 11th c, calendar
languages_primary: Judaeo-Arabic
doc_date_standard: 1052
description: Karaite document, compiled by a delegation of 12 Gaza and
Jerusalem Karaites, regarding the state of fields in the spring in order
to determine whether to intercalate the year, March 1052.
has_transcription: Y
has_translation: N
iiif_urls: https://cudl.lib.cam.ac.uk/iiif/MS-TS-00012-00147
```

### CrossReference Example (T-S 12.147)

```
LibraryNameEng: Cambridge University Library
LibraryAbbrev: CUL
Shelfmark: T-S 12.147
InventoryId: 22576104
NLI_AlmaId: 990051334280205171
FGPNumber: C22335
ImageName: T_S_12_147__L1F0B0S1
NumFolio: 1
```

### libraries.csv Example

```
system_number: 990051334280205171
call_numbers: Cambridge University Library Ms. T-S 12.147 | Ms. T-S 12.147 | T-S 12.147
library_code: CUL
titles_non_placeholder: מעשה בית דין...
```

---

## Appendix B: File Locations

| File | Location | Notes |
|------|----------|-------|
| libraries.csv | `C:\GenizahSearch\libraries.csv` | Primary metadata (217K records) |
| CrossReference_Final.csv | `C:\GenizahSearch\CrossReference_Final.csv` | NLI data (815K records) |
| cambridge_genizah.json | `C:\GenizahSearch\cambridge_genizah.json` | Cambridge IIIF manifest (141K manifests) |
| Manchester IIIF | LUNA API (no local file) | ~28K items, individual manifest access |
| Penn/CAJS OPenn | FTP/rsync (no local file) | 420+ items, TEI XML + images |
| JTS/Princeton DPUL | Figgy API (no local file) | 36K+ items, IIIF manifests |
| British Library | IIIF API (recovering) | 1.3K MSS, 435K images |
| PGP documents.csv | GitHub (see URL above) | Download as needed |
| PGP fragments.csv | GitHub | Download as needed |
| PGP footnotes.csv | GitHub | Download as needed |
| PGP people.csv | GitHub | Download as needed |
| PGP places.csv | GitHub | Download as needed |

## Appendix C: IIIF & Data Endpoints

| Institution | Endpoint Pattern | Collection Manifest |
|-------------|------------------|---------------------|
| Cambridge (CUDL) | `https://cudl.lib.cam.ac.uk/iiif/{label}` | `cambridge_genizah.json` (local) |
| Manchester (LUNA) | `https://luna.manchester.ac.uk/luna/servlet/iiif/m/{id}/manifest` | Not available |
| Manchester (MDC) | `https://www.digitalcollections.manchester.ac.uk/iiif/{id}` | Genizah not migrated |
| Penn/CAJS (OPenn) | `https://openn.library.upenn.edu/Data/0002/{id}/` | TEI XML (no IIIF) |
| JTS/Princeton (Figgy) | `https://figgy.princeton.edu/concern/scanned_resources/{uuid}/manifest` | Not available |
| Friedberg (FGP) | `https://fgp.genizah.org/` | Registration required |
| British Library | `https://api.bl.uk/metadata/iiif/ark:/81055/{id}/manifest.json` | ⚠️ Recovering |
| NLI KTIV | `https://iiif.nli.org.il/IIIFv21/marc/bib/{sys_id}` | MARC + IIIF |

---

*Document created: February 2026*
*Last updated: February 5, 2026*
*Section 14 (Questions for Future Implementation) added for new conversation continuity*
