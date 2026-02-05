# Architecture Patterns: Document-Level Entity Integration

**Domain:** Cairo Genizah manuscript research platform
**Researched:** February 5, 2026
**Focus:** Integrating PGP document-level entities with page-level GenizahSearch architecture

---

## Executive Summary

GenizahSearch's current architecture is **page-level** (each sys_id = one fragment/page), while PGP organizes **document-level** (each PGPID spans multiple fragments). This architectural mismatch requires a bridge layer to present documents as first-class entities while preserving backward compatibility with the existing sys_id model.

**Recommended approach:** Implement a Document virtual entity layer that groups fragments without restructuring the core data model. PGP becomes an authoritative source alongside FIST for multi-fragment relationships, integrated through the existing `fragment_joins` infrastructure with enhancements for document-level metadata.

---

## Current Architecture

### Component Boundaries

| Component | Responsibility | Key Files |
|-----------|---------------|-----------|
| **MetadataManager** | sys_id lookup, shelfmark normalization, CSV bank | `genizah_core.py` |
| **SearchEngine** | Tantivy indexing, fingerprint search | `genizah_core.py` |
| **LabEngine** | Advanced parallel search | `genizah_core.py` |
| **Supabase Client** | User data, corrections, fragment_joins | `web/supabase_client.py` |
| **Browse/Viewer** | Page display, image+transcription panels | `web/pages/browse.py` |
| **Version Selector** | V0.7/V0.8/User corrections toggle | `web/components/version_selector.py` |
| **Joins Panel** | Pairwise fragment relationships | `web/components/joins_panel.py` |

### Current Data Flow

```
libraries.csv (217K records)
    |
    v
MetadataManager.csv_bank[sys_id] -> {shelfmark, title, library_code}
    |
    v
Tantivy Index (unique_id=sys_id, content, fingerprint, shelfmark)
    |
    v
Search Results -> Browse Page -> sys_id lookup
    |                              |
    v                              v
Image URLs (IIIF)              Transcription (V0.8/corrections)
```

### Current Joins Model

```sql
-- Supabase fragment_joins table (pairwise, shelfmark-based)
fragment_joins (
    id,
    fragment_a_sys_id, fragment_a_shelfmark,
    fragment_b_sys_id, fragment_b_shelfmark,
    join_type,  -- 'physical_join', 'same_composition', 'uncertain'
    source,     -- 'user', 'fist', 'pgp' (planned)
    user_id, notes, evidence
)
```

**Key limitation:** No document-level entity. Joins are pairwise, computed via BFS for connected components.

---

## Recommended Architecture

### Option A: Document Entity Table (RECOMMENDED)

Introduce a `documents` table that groups sys_ids without modifying core data.

```
                    +-----------------+
                    |    documents    |
                    | (PGPID, title,  |
                    |  doc_type, etc) |
                    +-----------------+
                            |
                            | 1:N
                            v
                 +----------------------+
                 |  document_fragments  |
                 | (doc_id, sys_id,     |
                 |  shelfmark, seq)     |
                 +----------------------+
                            |
                            | N:1
                            v
                    +-----------------+
                    |   sys_id / CSV  |
                    |   (existing)    |
                    +-----------------+
```

#### Schema Design

```sql
-- New table: PGP documents as first-class entities
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    pgpid INTEGER UNIQUE,           -- PGP document ID
    title TEXT,                     -- Document title
    doc_type VARCHAR(100),          -- Letter, Legal, etc.
    languages TEXT[],               -- Primary languages
    date_original VARCHAR(100),     -- Original date string
    date_standard VARCHAR(50),      -- Standardized date range
    description TEXT,               -- PGP description
    pgp_url TEXT,                   -- Link to PGP
    source VARCHAR(50) DEFAULT 'pgp',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP
);

-- Junction table: document-fragment relationships
CREATE TABLE document_fragments (
    id SERIAL PRIMARY KEY,
    document_id INTEGER REFERENCES documents(id) ON DELETE CASCADE,
    sys_id VARCHAR(50) NOT NULL,           -- GenizahSearch sys_id
    shelfmark VARCHAR(200) NOT NULL,       -- Shelfmark from PGP
    matched_shelfmark VARCHAR(200),        -- How we matched it
    sequence_order INTEGER DEFAULT 0,      -- Order in document (recto before verso)
    side VARCHAR(20),                      -- 'recto', 'verso', NULL
    is_primary BOOLEAN DEFAULT FALSE,      -- Primary fragment for display
    UNIQUE(document_id, sys_id)
);

-- Indexes for efficient lookup
CREATE INDEX idx_doc_frags_sys_id ON document_fragments(sys_id);
CREATE INDEX idx_doc_frags_doc_id ON document_fragments(document_id);
CREATE INDEX idx_documents_pgpid ON documents(pgpid);
```

#### Advantages

1. **Backward compatible**: sys_id remains primary key, no migration required
2. **Clean separation**: Document metadata distinct from fragment metadata
3. **Flexible**: Single-fragment documents work naturally
4. **Queryable**: Can lookup "which documents contain this fragment?"
5. **Extendable**: Can add NLI documents, user-created documents later

### Option B: Enhanced Fragment Joins Only (Simpler but Limited)

Use existing `fragment_joins` with additional metadata.

```sql
-- Add columns to fragment_joins
ALTER TABLE fragment_joins ADD COLUMN pgpid INTEGER;
ALTER TABLE fragment_joins ADD COLUMN doc_title TEXT;
ALTER TABLE fragment_joins ADD COLUMN doc_type VARCHAR(100);
ALTER TABLE fragment_joins ADD COLUMN source_scholar TEXT;
```

**Advantages:** No new tables, uses existing infrastructure
**Disadvantages:** No central document entity, metadata duplicated per join pair

### Recommendation: Option A

The Document Entity approach (Option A) is recommended because:
1. PGP has 7,090 unique documents with rich metadata
2. Document-level operations (view all fragments of a letter) are natural
3. Future NLI integration will also have document-level structure
4. Version selector needs a document-level concept for PGP transcriptions

---

## Component Architecture

### New Components

```
+------------------+
|  DocumentService |  <-- New: Document CRUD, fragment linkage
+------------------+
        |
        v
+------------------+
| PGPTranscription |  <-- New: Transcription source alongside V0.8
|     Service      |
+------------------+
        |
        v
+------------------+
| MultiFragViewer  |  <-- Enhanced: Multiple fragments in one view
+------------------+
```

### Modified Components

| Component | Modification |
|-----------|-------------|
| **Version Selector** | Add PGP as source type: 'V0.8', 'V0.7', 'user', **'pgp'** |
| **Browse Page** | Add "Part of Document" indicator, link to document view |
| **Joins Panel** | Show document-level joins (via PGPID), not just pairwise |
| **Search** | Add "Search in PGP transcriptions" toggle |
| **Tantivy Index** | Add `transcription` field, `transcription_source` field |

### Enhanced Version Selector

```python
# Current sources
SOURCES = ['V0.8', 'V0.7', 'user']

# Enhanced sources
SOURCES = [
    'V0.8',           # HTR transcription
    'V0.7',           # Older HTR
    'user',           # User corrections
    'pgp',            # PGP scholarly transcription (NEW)
]

def fetch_page_versions(sys_id: str, page_num: int = 1) -> dict:
    """Fetch all versions including PGP transcriptions."""
    versions = []

    # Existing: User corrections from Supabase
    corrections = get_corrections(sys_id=sys_id, status='approved')
    for c in corrections:
        versions.append({
            'source': 'user',
            'content': c['corrected_text'],
            'author': c['profiles']['full_name'],
            ...
        })

    # NEW: PGP transcription
    pgp_data = get_pgp_transcription(sys_id)
    if pgp_data:
        versions.append({
            'source': 'pgp',
            'content': pgp_data['content'],
            'author': pgp_data['source_scholar'],
            'pgpid': pgp_data['pgpid'],
            'doc_type': pgp_data['doc_type'],
            ...
        })

    return {'all_versions': versions, ...}
```

---

## Data Flow (Target Architecture)

### Search Flow

```
User Query
    |
    v
Tantivy Search (content + transcription fields)
    |
    +--> Standard results (sys_id, score, source)
    |
    +--> If source='pgp': lookup document_fragments
              |
              v
         Show document context (other fragments)
```

### Browse Flow

```
User navigates to sys_id
    |
    v
MetadataManager.get_meta_for_id(sys_id)
    |
    v
Document lookup: SELECT * FROM document_fragments WHERE sys_id = ?
    |
    +--> No document: Show single fragment (existing behavior)
    |
    +--> Has document:
              |
              v
         Show "Part of Document" badge
         Link to document view
         Version selector shows PGP option
```

### Document View Flow (NEW)

```
User clicks "View Full Document" or navigates to /document/{pgpid}
    |
    v
DocumentService.get_document(pgpid)
    |
    v
Fetch all fragments: SELECT * FROM document_fragments WHERE document_id = ?
    |
    v
MultiFragViewer: Show all fragments in sequence
    +--> Image panels (side by side or stacked)
    +--> Single transcription (PGP joined text)
    +--> Metadata panel (doc_type, date, description)
```

---

## Tantivy Index Strategy

### Option A: Unified Index (RECOMMENDED)

Add transcription to existing schema.

```python
# Current schema fields
schema = {
    'unique_id': TEXT,        # sys_id
    'text_normalized': TEXT,  # V0.8 transcription (normalized)
    'content': TEXT,          # V0.8 transcription (original)
    'shelfmark': TEXT,
    'source': TEXT,           # 'V0.8', 'V0.7'
    'full_header': TEXT,
    'fingerprint': TEXT,
}

# Enhanced schema
schema = {
    ...existing...,
    'transcription': TEXT,           # PGP transcription (NEW)
    'transcription_source': TEXT,    # 'pgp', 'fist' (NEW)
    'pgpid': TEXT,                   # For document linking (NEW)
    'doc_type': TEXT,                # Letter, Legal, etc. (NEW)
}
```

**Indexing strategy:**
1. Index V0.8/V0.7 as today (content field)
2. For sys_ids with PGP data, also populate transcription field
3. Search can target: content only, transcription only, or both

### Option B: Separate Index

Create `PGP_Transcription_Index/` alongside `Genizah_Index/`.

**Advantages:** Clean separation, independent updates
**Disadvantages:** Two search systems, harder to rank together

### Recommendation: Unified Index (Option A)

Keeps search unified, allows ranking across sources, simpler UX.

---

## Migration Path

### Phase 1: Data Foundation (Week 1)

1. Create `documents` and `document_fragments` tables in Supabase
2. Import PGP documents (7,090 records from `documents.csv`)
3. Import fragment linkages (9,364 linked transcriptions)
4. Verify linkage via `transcriptions_linked.csv`

```python
# Import script outline
def import_pgp_documents():
    # 1. Load PGP documents.csv
    for row in pgp_documents:
        doc = insert_document(
            pgpid=row['pgpid'],
            title=row['description'][:500],
            doc_type=row['type'],
            languages=row['languages_primary'].split(', '),
            pgp_url=row['url']
        )

        # 2. Link fragments via transcriptions_linked.csv
        linked = transcriptions[transcriptions['pgpid'] == row['pgpid']]
        for frag in linked:
            insert_document_fragment(
                document_id=doc.id,
                sys_id=frag['sys_id'],
                shelfmark=frag['shelfmark']
            )
```

### Phase 2: Tantivy Enhancement (Week 2)

1. Extend Tantivy schema with transcription fields
2. Rebuild index with PGP data
3. Implement search toggle in UI

```python
def rebuild_index_with_pgp():
    # Existing: Index V0.8/V0.7
    process_file(Config.FILE_V8, 'V0.8')
    process_file(Config.FILE_V7, 'V0.7')

    # NEW: Add PGP transcriptions
    for row in pgp_transcriptions_linked:
        doc = tantivy.Document(
            unique_id=row['sys_id'],
            transcription=row['content'],
            transcription_source='pgp',
            pgpid=str(row['pgpid']),
            doc_type=row['doc_type'],
            # Inherit other fields from existing document
        )
        writer.add_document(doc)
```

### Phase 3: Version Selector Integration (Week 3)

1. Add PGP to version selector dropdown
2. Show PGP transcription when selected
3. Display attribution (scholar, PGPID link)

### Phase 4: Document View (Week 4)

1. Create `/document/{pgpid}` route
2. Implement MultiFragViewer component
3. Add "Part of Document" badges to browse page
4. Integrate with Joins Panel

---

## Patterns to Follow

### Pattern 1: Virtual Entity Layer

**What:** Document as computed entity from fragments
**When:** Multi-fragment groupings with shared metadata
**Example:**
```python
class DocumentService:
    def get_document_for_fragment(self, sys_id: str) -> Optional[Document]:
        """Get document containing this fragment, if any."""
        frag = supabase.table('document_fragments').select('document_id').eq('sys_id', sys_id).single()
        if frag:
            return supabase.table('documents').select('*').eq('id', frag['document_id']).single()
        return None
```

### Pattern 2: Source Attribution

**What:** Track where data comes from (V0.8, PGP, FIST, user)
**When:** Multiple authoritative sources for same content
**Example:**
```python
class TranscriptionVersion:
    content: str
    source: str              # 'V0.8', 'pgp', 'user'
    source_detail: str       # Scholar name, user name
    source_url: Optional[str]  # Link to original
    confidence: str          # 'authoritative', 'community', 'automated'
```

### Pattern 3: Graceful Degradation

**What:** Single fragments work same as before
**When:** Not all sys_ids have document associations
**Example:**
```python
def render_browse_page(sys_id: str):
    doc = document_service.get_document_for_fragment(sys_id)

    if doc:
        # Enhanced view: show document context
        show_document_badge(doc)
        show_related_fragments(doc)
        show_pgp_transcription(doc)
    else:
        # Standard view: single fragment (existing behavior)
        show_standard_fragment_view(sys_id)
```

---

## Anti-Patterns to Avoid

### Anti-Pattern 1: sys_id Restructuring

**What:** Changing sys_id to include document ID
**Why bad:** Breaks all existing references, URLs, user lists
**Instead:** Keep sys_id stable, add document linkage as separate layer

### Anti-Pattern 2: Duplicating Transcriptions

**What:** Copying PGP transcription into each sys_id's content field
**Why bad:** Duplicates data, unclear source, update complexity
**Instead:** Keep transcriptions in dedicated field with source attribution

### Anti-Pattern 3: Replacing Joins with Documents

**What:** Removing fragment_joins in favor of documents-only model
**Why bad:** User joins and FIST joins don't have document metadata
**Instead:** Keep both systems, document_fragments is additional layer

### Anti-Pattern 4: Immediate Full Integration

**What:** Trying to implement document view before data foundation
**Why bad:** Dependencies not met, blocked development
**Instead:** Phase 1 (data) -> Phase 2 (search) -> Phase 3 (UI) -> Phase 4 (document view)

---

## Scalability Considerations

| Concern | At Current Scale | At 10x Documents | Notes |
|---------|------------------|------------------|-------|
| Document lookup | O(1) Supabase query | O(1) with indexes | Index on sys_id, pgpid |
| Fragment aggregation | Simple query | Consider caching | Cache document->fragments map |
| Tantivy rebuild | ~5 min | ~50 min | Index sharding may help |
| Version selector | 3-4 sources | Still fast | Parallel fetches |

---

## Build Order Implications

Based on dependencies:

```
Phase 1: Database Schema
    |-- documents table
    |-- document_fragments table
    |-- Import PGP data
    |
    v
Phase 2: Data Services
    |-- DocumentService class
    |-- get_document_for_fragment()
    |-- get_fragments_for_document()
    |
    v
Phase 3: Tantivy Enhancement
    |-- Add transcription field
    |-- Rebuild index
    |-- Search toggle
    |
    v
Phase 4: Version Selector
    |-- Add PGP source
    |-- Attribution display
    |
    v
Phase 5: UI Integration
    |-- "Part of Document" badge
    |-- Joins Panel enhancement
    |
    v
Phase 6: Document View (optional)
    |-- /document/{pgpid} route
    |-- MultiFragViewer component
```

**Critical dependencies:**
- Phase 3 requires Phase 1 (data must exist to index)
- Phase 4 requires Phase 2 (need service to fetch versions)
- Phase 5 requires Phase 4 (badge links to versions)

---

## Backward Compatibility

### URL Structure

| Current | Preserved | New |
|---------|-----------|-----|
| `/browse?sys_id=X` | Yes | Works unchanged |
| `/browse?shelfmark=Y` | Yes | Works unchanged |
| `/browse?doc_id=Z` | N/A | NEW: Document view |
| `/document/{pgpid}` | N/A | NEW: PGP document |

### API Compatibility

| Endpoint | Change |
|----------|--------|
| `GET /api/v1/document/{sys_id}` | Enhanced: add `pgp_document` field if linked |
| `GET /api/v1/joins/connected/{shelf}` | Enhanced: add `document_context` if linked |
| `GET /api/v1/documents/{pgpid}` | NEW: Get PGP document with fragments |

### List Items

Existing list items (stored by sys_id) continue to work. New option to add document references.

---

## Confidence Assessment

| Area | Confidence | Reason |
|------|------------|--------|
| Document entity model | HIGH | Standard relational pattern, PGP data well-structured |
| Tantivy extension | HIGH | Verified Tantivy supports multi-field search |
| Version selector enhancement | HIGH | Clean extension of existing pattern |
| Multi-fragment viewer | MEDIUM | UI complexity, need UX research |
| Performance at scale | MEDIUM | May need caching layer for document aggregation |

---

## Sources

- `web/supabase_client.py` - Current Supabase schema
- `web/components/joins_panel.py` - Current joins UI
- `web/components/version_selector.py` - Current version selection
- `docs/specs/JOINS_SIMPLIFIED_SPEC.md` - Pairwise joins model
- `docs/plans/TRANSCRIPTIONS_INTEGRATION_DESIGN.md` - PGP data analysis
- `docs/plans/FIST_INTEGRATION_DESIGN.md` - FIST joins structure
- `pgp_data/MATCHING_SUMMARY.md` - PGP matching results (96.5%)
- `pgp_data/documents.csv` - PGP document schema
- `pgp_data/fragments.csv` - PGP fragment schema

---

*Document created: February 5, 2026*
