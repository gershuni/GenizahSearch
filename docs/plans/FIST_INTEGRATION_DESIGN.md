# FIST Database Integration Design

> **Status:** Design Phase
> **Date:** February 2026
> **Source:** FIST.db (Friedberg Image and Study Tool database from NLI)

---

## Executive Summary

The FIST database (Friedberg Image and Study Tool) provides rich scholarly metadata for ~253,000 manuscripts that can be directly linked to GenizahSearch via the `system_number` (AlmaId) field. This document designs the integration approach.

### Key Statistics

| Metric | Value |
|--------|-------|
| Total AlmaIds in FIST | 253,316 |
| With catalog records | 226,098 (89%) |
| With domain classifications | 203,252 (80%) |
| With scientific joins | 20,159 (8%) |
| Join groups (2+ fragments) | 14,640 |
| Total join group memberships | 35,254 |

### Known Data Gaps

| Gap Type | Count | Impact |
|----------|-------|--------|
| GenizahSearch records without FIST linkage | 2,321 | Cannot enrich with FIST data |
| FIST records with problematic Alma links | 43,933 | Internal NLI issue, doesn't affect GenizahSearch |

**Note:** Gap files from NLI are in `FIST_DB_BACKUP/gap_files/` with full documentation.

---

## Data Sources Summary

### 1. FIST Content Available for Import

| Data Type | Records | Value for GenizahSearch |
|-----------|---------|-------------------------|
| **Domain Classifications** | 386,711 classifications for 203K AlmaIds | Enable subject filtering (Bible, Piyyut, Letters, etc.) |
| **Scientific Joins** | 14,926 join groups | Fragment relationships with scholar attribution |
| **Catalog Records** | 411,022 records | Hebrew titles, authors, dates, descriptions |
| **Full Texts** | 65,332 texts | Transcriptions and content |
| **Bibliography** | 733,209 references | Scholarly sources |

### 2. Domain Classification Hierarchy

FIST uses a hierarchical domain system (259 domains). Top-level categories:

| Domain | AlmaId Count | Description |
|--------|--------------|-------------|
| Piyyut | 49,246 | Liturgical poetry |
| Bible Texts | 39,451 | Biblical manuscripts |
| Liturgy | 22,312 | Prayer texts |
| Letters | 13,188 | Documentary |
| Talmud | 12,954 | Talmudic texts |
| Halakhic | 11,982 | Legal texts |
| Vocabulary | 11,619 | Lexicographic |
| Prayer | 8,974 | Prayer texts |
| Midrash | 3,632 | Interpretive texts |

### 3. Join Types

| Code | Type | Count |
|------|------|-------|
| 1347 | Codex join | - |
| 2347 | Physical Join | Most common |
| 8347 | Scribe join | - |
| 9347 | Unspecified join | - |
| 10347 | Partial Physical Join | - |

---

## Join Chain: GenizahSearch to FIST Content

```
GenizahSearch                    FIST Database
=============                    ==============

libraries.csv.system_number  <-> dbo_InventoryAlma.AlmaId
                                      |
                                 dbo_Inventory.InventoryId
                                      |
                                 dbo_InventorySignature
                                      |
                                 dbo_Signature.SignatureId
                                      |
                        +-------------+-------------+
                        |                           |
                 dbo_UnitCatalogRec            dbo_UnitJoin
                 (catalog records)           (scientific joins)
                        |                           |
           +------------+-----------+        Scholar attribution
           |            |           |        Join type codes
     Domains       FullText    Bibliography
   (386K records)  (65K texts)  (733K refs)
```

---

## Integration Design

### Phase 1: Domain Classifications (High Priority)

**Goal:** Enable subject-based filtering in GenizahSearch

**Implementation:**

1. **Export lookup table from FIST:**
```sql
-- domains_by_alma.csv
SELECT DISTINCT
    alma.AlmaId,
    d.EngDesc as Domain,
    d.HebDesc as DomainHeb,
    pd.EngDesc as ParentDomain
FROM dbo_InventoryAlma alma
JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
JOIN dbo_Signature sig ON isig.SignatureId = sig.SignatureId
JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
JOIN dbo_CatalogMultiDomain cmd ON cat.UnitCatalogRecId = cmd.UnitCatalogRecId
JOIN CODE_Domain d ON cmd.DomainId = d.DomainId
LEFT JOIN CODE_Domain pd ON d.BelongToDomainId = pd.DomainId
```

2. **Load at GenizahSearch startup:**
```python
# In genizah_core.py or new fist_enrichment.py
class FistEnrichment:
    def __init__(self, domains_file: str):
        self.domains_by_sysid = {}  # sys_id -> [domain1, domain2, ...]
        self._load_domains(domains_file)

    def get_domains(self, sys_id: str) -> List[str]:
        return self.domains_by_sysid.get(sys_id, [])
```

3. **Add domain filter to search UI:**
   - Dropdown/chip selector with top domains
   - Filter search results by domain

**Files to create:**
- `fist_data/domains_by_alma.csv` (~200K rows)
- `fist_enrichment.py` (loader class)

### Phase 2: Scientific Joins (High Priority)

**Goal:** Import scholarly fragment joins with attribution

**Implementation:**

1. **Export joins from FIST:**
```sql
-- joins_by_alma.csv
SELECT
    alma.AlmaId,
    uj.UnitJoinId as JoinGroupId,
    uj.ScholarName,
    uj.Comment,
    fc.EngDesc as JoinType
FROM dbo_InventoryAlma alma
JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
JOIN dbo_Signature sig ON isig.SignatureId = sig.SignatureId
JOIN dbo_UnitJoin uj ON sig.SignatureId = uj.SignatureId
LEFT JOIN CODE_FullCode fc ON uj.JoinTypeCode = fc.ComputedCode
```

2. **Map to GenizahSearch Joins system:**
```python
# Integration with existing joins system
def import_fist_joins(joins_csv: str, supabase_client):
    """Import FIST joins as source='fist'"""
    for group_id, group_members in groupby(joins_csv, 'JoinGroupId'):
        members = list(group_members)
        if len(members) < 2:
            continue

        # Create pairwise joins (as per JOINS_SIMPLIFIED_SPEC.md)
        base = members[0]
        for other in members[1:]:
            create_join(
                sys_id_a=base['AlmaId'],
                sys_id_b=other['AlmaId'],
                relationship_type='physical_join',
                source='fist',
                scholar=base.get('ScholarName'),
                notes=base.get('Comment')
            )
```

3. **Display in UI:**
   - Show join group info on browse page
   - Link to other fragments in group
   - Display scholar attribution

**Files to create:**
- `fist_data/joins_by_alma.csv` (~35K rows)
- Update `web/pages/browse.py` for join display

### Phase 3: Catalog Enrichment (Medium Priority)

**Goal:** Enrich GenizahSearch with Hebrew titles, dates, descriptions

**Implementation:**

1. **Export catalog data:**
```sql
-- catalog_by_alma.csv
SELECT DISTINCT
    alma.AlmaId,
    cat.Title as CatalogTitle,
    cat.TitleHeb,
    cat.AuthorText,
    cat.CopyDate,
    cat.CopyPlace,
    cat.IdentificationTextEng as Description
FROM dbo_InventoryAlma alma
JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
JOIN dbo_Signature sig ON isig.SignatureId = sig.SignatureId
JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
```

2. **Merge with existing libraries.csv or create supplement:**
   - Option A: Add columns to libraries.csv
   - Option B: Create separate `fist_catalog.csv` loaded as supplement

3. **Display enriched metadata:**
   - Show FIST title if available
   - Display author and date info
   - Add English description when present

### Phase 4: Full Texts (Lower Priority)

**Goal:** Add searchable transcriptions from FIST

**Note:** FIST has 65,332 full texts. Consider:
- Adding to Tantivy index vs. runtime lookup
- Storage requirements (~50-100MB estimated)
- Deduplication with existing GenizahSearch transcriptions

---

## File Structure

```
C:\GenizahSearch\
├── fist_data/                      # New directory
│   ├── domains_by_alma.csv         # Domain classifications
│   ├── joins_by_alma.csv           # Scientific joins
│   ├── catalog_by_alma.csv         # Catalog metadata
│   └── README.md                   # Data documentation
├── fist_enrichment.py              # Loader and enrichment class
└── FIST_DB_BACKUP/
    ├── FIST.db                     # Source database (keep for reference)
    └── export_scripts/             # SQL export scripts
```

---

## Export Scripts

### export_domains.py

```python
import sqlite3
import csv

conn = sqlite3.connect('FIST_DB_BACKUP/FIST.db')
cursor = conn.cursor()

query = """
SELECT DISTINCT
    alma.AlmaId,
    d.EngDesc as Domain,
    d.HebDesc as DomainHeb,
    pd.EngDesc as ParentDomain
FROM dbo_InventoryAlma alma
JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
JOIN dbo_Signature sig ON isig.SignatureId = sig.SignatureId
JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
JOIN dbo_CatalogMultiDomain cmd ON cat.UnitCatalogRecId = cmd.UnitCatalogRecId
JOIN CODE_Domain d ON cmd.DomainId = d.DomainId
LEFT JOIN CODE_Domain pd ON d.BelongToDomainId = pd.DomainId
ORDER BY alma.AlmaId
"""

with open('fist_data/domains_by_alma.csv', 'w', newline='', encoding='utf-8-sig') as f:
    writer = csv.writer(f)
    writer.writerow(['AlmaId', 'Domain', 'DomainHeb', 'ParentDomain'])
    for row in cursor.execute(query):
        writer.writerow(row)

conn.close()
```

### export_joins.py

```python
import sqlite3
import csv

conn = sqlite3.connect('FIST_DB_BACKUP/FIST.db')
cursor = conn.cursor()

query = """
SELECT
    alma.AlmaId,
    uj.UnitJoinId as JoinGroupId,
    uj.ScholarName,
    uj.Comment,
    fc.EngDesc as JoinType
FROM dbo_InventoryAlma alma
JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
JOIN dbo_Signature sig ON isig.SignatureId = sig.SignatureId
JOIN dbo_UnitJoin uj ON sig.SignatureId = uj.SignatureId
LEFT JOIN CODE_FullCode fc ON uj.JoinTypeCode = fc.ComputedCode
ORDER BY uj.UnitJoinId, alma.AlmaId
"""

with open('fist_data/joins_by_alma.csv', 'w', newline='', encoding='utf-8-sig') as f:
    writer = csv.writer(f)
    writer.writerow(['AlmaId', 'JoinGroupId', 'ScholarName', 'Comment', 'JoinType'])
    for row in cursor.execute(query):
        writer.writerow(row)

conn.close()
```

---

## Integration with Existing Systems

### With libraries.csv

The join key is direct:
- `libraries.csv.system_number` = `FIST.dbo_InventoryAlma.AlmaId`

Both are 18-digit NLI Alma IDs (e.g., `990051334280205171`).

### With GenizahSearch Joins System

FIST joins can be imported using the existing joins infrastructure:
- Source: `'fist'` (new source identifier)
- Relationship types map to FIST JoinTypeCodes
- Scholar attribution preserved

### With PGP Data

FIST and PGP complement each other:
- **FIST:** Domain classifications, scholarly joins, Hebrew metadata
- **PGP:** Document types, English descriptions, transcriptions, tags

Both use AlmaId/shelfmark for linking.

---

## UI Changes Required

### Search Page

1. **Domain Filter:**
   - Dropdown with top-level domains
   - Badge count showing documents per domain
   - Multi-select for combined filtering

### Browse Page

1. **Domain Display:**
   - Show assigned domains below shelfmark
   - Clickable links to filter by domain

2. **Join Group Display:**
   - If fragment is part of FIST join group:
     - Show "Part of join group" indicator
     - List other fragments in group
     - Show scholar attribution
     - Display join type

3. **Catalog Enrichment:**
   - Show FIST catalog title if different from existing
   - Display author, date, place info
   - Show description in collapsible panel

---

## Performance Considerations

### Memory Usage

| Data File | Estimated Size | Load Strategy |
|-----------|----------------|---------------|
| domains_by_alma.csv | ~15 MB | Load at startup into dict |
| joins_by_alma.csv | ~3 MB | Load at startup |
| catalog_by_alma.csv | ~30 MB | Lazy load or SQLite |

### Lookup Performance

```python
# O(1) lookup by sys_id
enrichment = FistEnrichment()
domains = enrichment.get_domains(sys_id)  # Fast dict lookup
```

---

## Next Steps

### Immediate Actions

1. [ ] Create `fist_data/` directory
2. [ ] Run export scripts to generate CSV files
3. [ ] Implement `FistEnrichment` class
4. [ ] Test domain lookup performance

### Short Term

5. [ ] Add domain filter to search UI
6. [ ] Display domains on browse page
7. [ ] Import FIST joins to Supabase

### Medium Term

8. [ ] Add catalog enrichment
9. [ ] Consider full text indexing
10. [ ] Update documentation

---

## Questions Resolved

From `EXTERNAL_DATA_INTEGRATION_EXPLORATION.md` Section 14:

| Question | Resolution |
|----------|------------|
| Q8: Start PGP or wait for NLI? | FIST data now available - can implement in parallel |
| Q9: NLI CrossReference relationship fields | FIST `dbo_UnitJoin` provides cleaner join data with scholar attribution |
| Storage architecture | CSV files with Python dict lookup for domains/joins |

---

*Document created: February 5, 2026*
