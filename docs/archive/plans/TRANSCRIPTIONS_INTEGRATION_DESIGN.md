# Transcription Integration Design

> **Status:** Deferred (Phase 13 — index build too slow for desktop)
> **Date:** February 2026 (updated 2026-03-13)
> **Priority:** HIGH - Core feature enhancement (deferred)

---

## Executive Summary

Adding searchable transcriptions will transform GenizahSearch from "search by title/metadata" to "search in actual manuscript text." This document analyzes available transcription sources and designs the integration approach.

### Key Finding

**Two complementary sources available:**

| Source | Type | Linkable Records | Content Type |
|--------|------|------------------|--------------|
| **PGP** (Princeton) | Actual transcriptions | 5,337 | Hebrew/Judeo-Arabic text with scholarly apparatus |
| **FIST** (NLI) | Catalog descriptions | 85,313 | Scholarly descriptions, some quoted text |

**Recommendation:** Prioritize PGP transcriptions first - they provide actual searchable text content.

---

## Source Analysis

### 1. PGP Footnotes (Princeton Geniza Project)

**Location:** `pgp_data/footnotes.csv`
**Source:** https://github.com/princetongenizalab/pgp-metadata

#### Statistics

| Metric | Count |
|--------|-------|
| Total footnotes | 24,388 |
| Edition/Digital Edition records | 17,106 |
| Records with actual content | 9,740 |
| Records with substantial content (>200 chars) | 9,205 |
| Unique documents with transcriptions | 7,342 |
| **Linkable to GenizahSearch** | **5,337** |

#### Content Types (doc_relation field)

| Type | Count | Description |
|------|-------|-------------|
| Digital Edition | 7,968 | Modern digital transcriptions |
| Discussion | 6,805 | Scholarly discussion (not transcription) |
| Edition | 6,448 | Published editions |
| Digital Translation | 1,792 | English translations |
| Edition + Translation | 538 | Combined |

#### Sample Content

```
Document ID: 11665
Source: Menahem Ben-Sasson
Content:
בש רח
לכל אחינו הזקנים הישרים הכשרי[ם
אשר בסורא וקריה אסמאעיל וול ניל [
הערים ישמרם צור ויעזרם וית[ן
...
```

**Key Characteristics:**
- Actual manuscript text in Hebrew/Judeo-Arabic
- Scholarly transcription conventions ([ ] for lacunae, etc.)
- Attribution to scholars
- Links to PGPID which links to shelfmark

#### Linkage Path

```
PGP footnotes.csv
    document_id →
PGP documents.csv
    pgpid → shelfmark →
GenizahSearch libraries.csv
    call_numbers (normalized) → system_number
```

---

### 2. FIST Full Texts (NLI)

**Location:** `FIST_DB_BACKUP/FIST.db` → `dbo_UnitFullText`

#### Statistics

| Metric | Count |
|--------|-------|
| Total records | 65,332 |
| Unique AlmaIds with text | 85,313 |
| Records >500 chars | 3,640 |
| Records >1000 chars | 642 |
| Records >5000 chars | 9 |
| Average text length | 240 chars |

#### Content Analysis

**Critical Finding:** FIST "FullText" is NOT actual manuscript transcriptions.

It contains:
1. **Catalog descriptions** (most common)
2. **Scholarly analysis** of content
3. **Quoted excerpts** embedded in descriptions
4. **Bibliographic references**

#### Sample Content

```
AlmaId: 990001935970205171
Shelfmark: Halper 275

A collection of liturgic poems. Fol. 1 is headed בשם י"י נעשה ובצלו נחסה שבחות.
1 (fol. 1a). A long hymn having twenty-three stanzas...
The hymn begins אבוא לפניך בתחנון, and ends ב' י"י לעולם אמן ואמן.
```

**Value:** Rich scholarly metadata, but NOT primary searchable transcription.

#### Linkage Path

```
FIST dbo_UnitFullText
    SignatureId →
dbo_Signature
    SetSignatureId →
dbo_InventorySignature
    InventoryId →
dbo_InventoryAlma
    AlmaId →
GenizahSearch libraries.csv
    system_number
```

---

## Comparison

| Aspect | PGP | FIST |
|--------|-----|------|
| **Content Type** | Actual transcriptions | Catalog descriptions |
| **Language** | Hebrew/Judeo-Arabic | English + quoted Hebrew |
| **Searchability** | High - direct text search | Medium - keyword search |
| **Coverage** | 5,337 linkable | 85,313 linkable |
| **Scholarly apparatus** | Yes ([ ], emendations) | No |
| **Attribution** | Per-transcription | General |
| **Update frequency** | Daily (GitHub) | Static |

### Overlap Analysis

Need to determine:
1. How many shelfmarks have BOTH PGP and FIST content?
2. Are they complementary or redundant?

---

## Integration Design

### Phase 1: PGP Transcriptions (Priority)

**Goal:** Enable Hebrew/Judeo-Arabic text search in 5,337 manuscripts

#### Option A: Add to Tantivy Index

**Pros:**
- Unified search experience
- Fast full-text search
- Supports Hebrew morphology (with analyzer)

**Cons:**
- Requires index rebuild
- Mixed content types (titles vs transcriptions)
- Storage increase

**Implementation:**
```python
# Add 'transcription' field to Tantivy schema
schema = {
    ...existing fields...,
    'transcription': TEXT,  # New field
    'transcription_source': KEYWORD,  # 'pgp', 'fist', etc.
}
```

#### Option B: Separate Search System

**Pros:**
- Clean separation
- Can use specialized Hebrew analyzer
- Independent updates

**Cons:**
- Two search interfaces
- User confusion

#### Option C: Hybrid (Recommended)

1. Add `transcription` field to Tantivy
2. Index transcriptions alongside existing content
3. Add UI toggle: "Search in transcriptions"
4. Display transcription source/attribution

### Phase 2: FIST Descriptions (Secondary)

**Goal:** Enable keyword search in catalog descriptions

**Approach:**
- Index FIST FullText as `description` field
- Lower search weight than transcriptions
- Display in browse view for enrichment

---

## Data Pipeline

### PGP Import Pipeline

```
1. Load PGP documents.csv
   - Map pgpid → shelfmark

2. Load PGP footnotes.csv
   - Filter: doc_relation contains 'Edition' or 'Digital'
   - Filter: content length > 50 chars

3. Normalize shelfmarks
   - Remove prefixes ("Cambridge University Library", "Ms.")
   - Normalize spaces/punctuation

4. Match to GenizahSearch
   - Lookup normalized shelfmark → system_number

5. Prepare for indexing
   - Combine multiple footnotes per document
   - Track source/scholar attribution

6. Update Tantivy index
   - Add transcription field to existing documents
   - Or create new index with transcription schema
```

### Export Script: pgp_transcriptions_export.py

```python
import csv
import re

def normalize_shelfmark(shelf):
    """Normalize shelfmark for matching"""
    shelf = re.sub(r'^Cambridge University Library\s*', '', shelf, flags=re.I)
    shelf = re.sub(r'^Ms\.\s*', '', shelf, flags=re.I)
    shelf = shelf.strip().lower()
    shelf = re.sub(r'\s+', ' ', shelf)
    return shelf

def export_pgp_transcriptions():
    # Load GenizahSearch shelfmark → sys_id mapping
    gs_lookup = {}
    with open('libraries.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            sys_id = row[0]
            for variant in row[2].split('|'):
                normalized = normalize_shelfmark(variant)
                if normalized:
                    gs_lookup[normalized] = sys_id

    # Load PGP pgpid → shelfmark mapping
    pgp_docs = {}
    with open('pgp_data/documents.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            pgpid = row.get('\ufeffpgpid') or row.get('pgpid')
            pgp_docs[pgpid] = row.get('shelfmark', '')

    # Export transcriptions with sys_id linkage
    with open('pgp_transcriptions_linked.csv', 'w', encoding='utf-8-sig', newline='') as out:
        writer = csv.writer(out)
        writer.writerow(['sys_id', 'pgpid', 'shelfmark', 'source', 'doc_relation', 'content'])

        with open('pgp_data/footnotes.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rel = row.get('doc_relation', '')
                content = row.get('content', '')
                doc_id = row.get('document_id')

                if ('Edition' in rel or 'Digital' in rel) and content and len(content) > 50:
                    shelfmark = pgp_docs.get(doc_id, '')

                    # Try to match to GenizahSearch
                    sys_id = None
                    for part in shelfmark.split('+'):
                        normalized = normalize_shelfmark(part.strip())
                        if normalized in gs_lookup:
                            sys_id = gs_lookup[normalized]
                            break

                    if sys_id:
                        writer.writerow([
                            sys_id,
                            doc_id,
                            shelfmark,
                            row.get('source', ''),
                            rel,
                            content
                        ])

if __name__ == '__main__':
    export_pgp_transcriptions()
```

---

## UI Changes

### Search Page

1. **Transcription Search Toggle**
   ```
   [x] Search in titles
   [x] Search in transcriptions  ← NEW
   ```

2. **Results Display**
   - Highlight matched text in transcription
   - Show transcription source (PGP, scholar name)
   - Link to full transcription view

### Browse/Viewer Page

1. **Transcription Panel**
   - Display available transcription
   - Show source/attribution
   - Link to PGP for full context

2. **Transcription Badge**
   - Visual indicator when transcription available
   - Icon: 📜 or similar

---

## Technical Considerations

### Hebrew Text Search

**Challenge:** Hebrew morphology, nikud, final letters

**Options:**
1. **Simple normalization** - Remove nikud, normalize finals
2. **Hebrew analyzer** - Use ICU analyzer or Hebrew-specific
3. **Fuzzy matching** - Levenshtein distance for variants

**Recommendation:** Start with simple normalization, add analyzer later.

### Multi-Source Attribution

Track source for each transcription:
```python
@dataclass
class Transcription:
    sys_id: str
    content: str
    source: str  # 'pgp', 'fist', 'user'
    scholar: Optional[str]
    pgpid: Optional[str]
    url: Optional[str]
```

### Incremental Updates

PGP updates daily on GitHub. Design for:
1. Detect changed documents
2. Re-index only changed transcriptions
3. Track version/timestamp

---

## Implementation Phases

### Phase 1: Data Export (Week 1)
- [ ] Create `pgp_transcriptions_export.py`
- [ ] Generate `pgp_transcriptions_linked.csv`
- [ ] Validate linkage accuracy
- [ ] Document unmatched records

### Phase 2: Index Update (Week 2)
- [ ] Add `transcription` field to Tantivy schema
- [ ] Create indexing script for transcriptions
- [ ] Test search performance
- [ ] Handle Hebrew normalization

### Phase 3: UI Integration (Week 3)
- [ ] Add transcription search toggle
- [ ] Display transcriptions in browse view
- [ ] Add transcription availability indicator
- [ ] Attribution display

### Phase 4: FIST Descriptions (Week 4+)
- [ ] Export FIST descriptions with AlmaId
- [ ] Add as secondary content field
- [ ] Lower search weight

---

## Questions to Resolve

1. **Index Strategy**
   - Rebuild entire index with new schema?
   - Or supplementary index for transcriptions?

2. **Search Weight**
   - Equal weight to title and transcription?
   - Boost exact matches?

3. **Display Priority**
   - Show transcription by default or collapsed?
   - How much to show in search results?

4. **Multi-fragment Documents**
   - PGP often has "T-S X + T-S Y" shelfmarks
   - Apply transcription to all fragments or primary only?

5. **Update Mechanism**
   - Manual refresh from GitHub?
   - Automated sync?

---

## Success Metrics

| Metric | Target |
|--------|--------|
| Linkable transcriptions | 5,000+ |
| Search response time | <500ms |
| User satisfaction | Survey after launch |
| Search usage | Track transcription searches vs title searches |

---

## Related Documents

- `FIST_INTEGRATION_DESIGN.md` - FIST database integration (domains, joins)
- `EXTERNAL_DATA_INTEGRATION_EXPLORATION.md` - Data source overview
- `docs/specs/JOINS_TECHNICAL_SPEC.md` - Joins system (relevant for multi-fragment)

---

## Export Results (February 5, 2026) - FINAL

### Final Export Statistics

| Metric | Value |
|--------|-------|
| Total transcription records | 9,703 |
| **Linked to GenizahSearch** | **9,364 (96.5%)** |
| Unmatched records | 339 |
| Unique linked documents | 7,090 |
| Unique unmatched documents | 253 |

### Normalization Improvements Applied

The following shelfmark normalizations were implemented to improve matching:

1. **Library prefixes removed:**
   - Cambridge University Library, CUL
   - The University of Manchester Library
   - Freer Gallery of Art, Smithsonian Institution
   - The Jewish Theological Seminary of America
   - Bodl., AIU, RNL, NLI, HUC, BL, PER, UPenn, JTSA

2. **Format normalizations:**
   - T-S series: "T-S 13 J 35" → "T-S 13J35"
   - Or.1080/1081: "Or. 1080 J 70" → "Or.1080 J70"
   - BL Or.: "BL Or. 2570" → "OR 2570"
   - Bodleian heb.: "heb. a 2/22" → "heb. a.2.22"
   - JRL → Manchester: "JRL A 316" → "A 316", "JRL P 213" → "P 213"
   - JRL AF: "JRL AF 255" → "AF  255" (FIST double space)
   - JRL Gaster: "JRL Gaster heb. ms X" → "Gaster heb. ms X"
   - JTS Schechter: "JTS Schechter 4" → "Schechter.4"
   - Lewis-Gibson: "L-G Misc. 58" → "L-G Misc .58"
   - RNL Yevr.: "RNL Yevr.-Arab I 19" → "Yevr.-Arab. I 19"
   - IOM: "IOM D 55.13" → "D 55/13"
   - Em-dash normalization: "–" → "-"
   - Mosseri: "Moss. V, 39" → "Moss. V,39"
   - Letter suffix fallback: "1860/10a" matches "1860/10"

3. **FIST supplement:** 130,947 shelfmarks from 18 libraries:
   - RNL, BL, AIU, CAJS, NLI, HUC, JTS, Bodleian, Manchester
   - Mosseri, Lewis-Gibson, Vienna, Freer, Heidelberg
   - Sassoon, Heidelberg-Papyrology, Penn-Museum, IOM
   - CUL items missing from libraries.csv (18,730 records)

### Remaining Unmatched Analysis (339 records)

| Category | Count | Notes |
|----------|-------|-------|
| BL | 82 | British Library Or. 4684 series not in FIST |
| MIAC | 60 | Museum of Islamic Art, Cairo - external collection |
| Copenhagen | 56 | Royal Library - only 1 record with AlmaId |
| Unknown | 33 | Excavation images, no shelfmark |
| Other | 24 | Karaite archives, Coptic Museum, rare collections |
| JRL | 21 | Genizah Arabic series not in FIST |
| St. Catherine | 18 | Monastery - not in FIST |
| Empty | 7 | No shelfmark in PGP |
| Mosseri | 6 | Specific folios (VI series with period) |
| Bodleian | 6 | Arabic MS series |
| Strasbourg | 6 | Not in FIST |
| Uppsala | 6 | Not in FIST |
| RNL/Yevr | 5 | Antonin, special series |
| T-S | 5 | Special cases (T-S 1*, multi-fragment) |
| Firkovitch | 3 | II Firk. Arab. format |
| Michaelides | 1 | No AlmaId |

### Output Files

```
pgp_data/
├── transcriptions_linked.csv      # 9,364 records with sys_id
├── transcriptions_unmatched.csv   # 339 records (external collections)
├── fist_shelfmarks_supplement.csv # 130,947 FIST shelfmarks
└── export_report.txt              # Full statistics
```

### Conclusion

**96.5% match rate achieved.** The remaining 3.5% (339 records) are almost entirely:
- **External collections not in GenizahSearch:** MIAC (Cairo), Copenhagen, St. Catherine Monastery, Strasbourg, Uppsala
- **Missing from FIST:** BL Or. 4684 series, JRL Genizah Arabic
- **Unidentified:** Excavation images, documents without shelfmarks

These represent the practical maximum achievable without importing entirely new collections into the system.

### Progress Summary

| Stage | Linked | Unmatched | Rate |
|-------|--------|-----------|------|
| Initial | 8,779 | 924 | 90.5% |
| +FIST libraries | 9,117 | 586 | 94.0% |
| +More libraries | 9,285 | 418 | 95.7% |
| +Normalizations | 9,341 | 362 | 96.3% |
| **Final** | **9,364** | **339** | **96.5%** |

### Next Steps

1. **Immediate:** Add transcriptions to Tantivy index
2. **UI:** Add transcription search toggle and display
3. **Future:** Consider importing MIAC, Copenhagen collections

---

*Document created: February 5, 2026*
*Export completed: February 5, 2026*
*Final update: February 5, 2026 - 96.5% match rate achieved*
