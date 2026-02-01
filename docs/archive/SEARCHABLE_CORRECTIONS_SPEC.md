# Technical Specification: Searchable Corrections & External Transcriptions
## A System for Indexing User Corrections and External Transcription Sources

**Date:** January 2026
**Version:** 1.0
**Branch:** `claude/searchable-corrections-sync`
**Risk Level:** HIGH - Core search functionality
**Dependencies:** Joins system, Corrections system, Tantivy index

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Problem Statement](#problem-statement)
3. [Goals and Non-Goals](#goals-and-non-goals)
4. [Architecture Overview](#architecture-overview)
5. [Data Models](#data-models)
6. [Sync Mechanism](#sync-mechanism)
7. [Search Integration](#search-integration)
8. [External Import System (Princeton, etc.)](#external-import-system)
9. [Shelfmark Normalization](#shelfmark-normalization)
10. [Implementation Phases](#implementation-phases)
11. [Risk Analysis and Mitigation](#risk-analysis-and-mitigation)
12. [Testing Strategy](#testing-strategy)
13. [Rollback Plan](#rollback-plan)
14. [Open Questions](#open-questions)

---

## Executive Summary

This specification describes a system to make user corrections and external transcriptions (e.g., Princeton Genizah Project) searchable in both the desktop application and web platform.

**Key Features:**
- Desktop syncs approved corrections from web backend to local index
- Search queries both original transcriptions AND corrections
- Bulk import of curated transcriptions from external sources
- Support for joined documents (multiple fragments = one transcription)
- Clear priority system: External curated > User corrections > MiDRASH base

**Why This Matters:**
Currently, when users search, they only find matches in the original MiDRASH transcriptions. User corrections and high-quality external transcriptions are invisible to search, limiting their value.

---

## Problem Statement

### Current State

```
DESKTOP SEARCH
    │
    └── Queries local Tantivy index
        └── Contains only: MiDRASH V0.7 / V0.8 transcriptions
        └── Does NOT contain: User corrections, Princeton, etc.

WEB SEARCH
    └── No full-text search implemented yet
```

### Desired State

```
DESKTOP SEARCH
    │
    ├── Local Tantivy index (MiDRASH base)
    ├── Local corrections index (synced from web)
    └── Local external transcriptions index (synced from web)

WEB SEARCH
    │
    ├── Server-side corrections index
    └── Server-side external transcriptions index

BOTH return unified, deduplicated results with source attribution
```

---

## Goals and Non-Goals

### Goals

1. **G1:** User corrections (approved) are searchable in desktop and web
2. **G2:** External transcriptions (Princeton, etc.) are searchable
3. **G3:** Joined document transcriptions are properly indexed and searchable
4. **G4:** Search results show source attribution and allow version comparison
5. **G5:** Desktop sync is fast (seconds) and non-blocking
6. **G6:** System handles deletions/edits gracefully
7. **G7:** Shelfmark variations don't break matching

### Non-Goals (Out of Scope)

- Real-time sync (eventual consistency is acceptable)
- Automatic transcription generation (AI/OCR)
- Conflict resolution for user edits to external transcriptions
- Full offline editing capability

---

## Architecture Overview

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           WEB BACKEND (FastAPI)                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐ │
│  │ Corrections │  │  External   │  │   Joined    │  │  Sync Service   │ │
│  │   Service   │  │   Import    │  │Transcription│  │                 │ │
│  │             │  │   Service   │  │   Service   │  │ - /sync/...     │ │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └────────┬────────┘ │
│         │                │                │                   │          │
│         ▼                ▼                ▼                   │          │
│  ┌─────────────────────────────────────────────────────────┐ │          │
│  │                    PostgreSQL Database                   │ │          │
│  │  - corrections (status=APPROVED)                         │ │          │
│  │  - external_transcriptions (NEW)                         │ │          │
│  │  - joined_transcriptions (NEW)                           │ │          │
│  │  - join_groups (existing)                                │ │          │
│  └─────────────────────────────────────────────────────────┘ │          │
│         │                │                │                   │          │
│         ▼                ▼                ▼                   │          │
│  ┌─────────────────────────────────────────────────────────┐ │          │
│  │              Server Tantivy Indexes                      │ │          │
│  │  - corrections_index                                     │ │          │
│  │  - external_transcriptions_index                         │ │          │
│  └─────────────────────────────────────────────────────────┘ │          │
│                                                               │          │
└───────────────────────────────────────────────────────────────┼──────────┘
                                                                │
                              HTTPS API                         │
                                                                │
┌───────────────────────────────────────────────────────────────┼──────────┐
│                        DESKTOP APP (PyQt6)                    │          │
├───────────────────────────────────────────────────────────────┼──────────┤
│                                                               │          │
│  ┌─────────────────┐    ┌──────────────────────────────────┐ │          │
│  │  Sync Manager   │◄───┤  GET /sync/corrections           │◄┘          │
│  │                 │    │  GET /sync/external-transcriptions│            │
│  │  - On startup   │    │  GET /sync/joined-transcriptions  │            │
│  │  - Manual button│    └──────────────────────────────────┘            │
│  └────────┬────────┘                                                     │
│           │                                                              │
│           ▼                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │                    Local Tantivy Indexes                             ││
│  │  ┌─────────────┐  ┌─────────────────┐  ┌───────────────────────┐    ││
│  │  │ Main Index  │  │ Corrections     │  │ External/Joined       │    ││
│  │  │ (MiDRASH)   │  │ Index (synced)  │  │ Transcriptions Index  │    ││
│  │  │             │  │                 │  │ (synced)              │    ││
│  │  └─────────────┘  └─────────────────┘  └───────────────────────┘    ││
│  └─────────────────────────────────────────────────────────────────────┘│
│           │                   │                       │                  │
│           └───────────────────┴───────────────────────┘                  │
│                               │                                          │
│                               ▼                                          │
│                    ┌─────────────────────┐                              │
│                    │   Unified Search    │                              │
│                    │   Query Engine      │                              │
│                    │   (deduplication)   │                              │
│                    └─────────────────────┘                              │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
1. CORRECTION APPROVAL FLOW
   User submits correction → Admin approves → status=APPROVED
   → Server indexes in corrections_index
   → Desktop syncs on next startup/manual sync
   → Desktop indexes in local corrections_index

2. EXTERNAL IMPORT FLOW
   Admin imports Princeton data → Parse HTML → Extract shelfmarks
   → Normalize shelfmarks → Match to our documents
   → Create/link JoinGroup → Create JoinedTranscription
   → Server indexes → Desktop syncs

3. SEARCH FLOW
   User searches "אברהם"
   → Query main index (MiDRASH)
   → Query corrections index
   → Query external/joined index
   → Merge results → Deduplicate by document
   → Rank by priority → Return unified results
```

---

## Data Models

### New Tables

#### `external_transcriptions`

Stores transcriptions from external sources (non-joined, single fragment).

```sql
CREATE TABLE external_transcriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Document identification
    document_id VARCHAR(100) NOT NULL,        -- sys_id (primary)
    shelfmark VARCHAR(200) NOT NULL,          -- display shelfmark
    shelfmark_normalized VARCHAR(200) NOT NULL, -- for matching

    -- Content
    transcription_text TEXT NOT NULL,

    -- Source information
    source_name VARCHAR(100) NOT NULL,        -- "princeton", "cambridge", etc.
    source_url TEXT,                          -- original URL
    source_reference TEXT,                    -- academic citation

    -- Metadata
    priority INTEGER DEFAULT 50,              -- 1-100, higher = preferred
    quality_score FLOAT,
    language VARCHAR(50),                     -- "hebrew", "judeo-arabic", etc.

    -- Tracking
    imported_at TIMESTAMP DEFAULT NOW(),
    imported_by INTEGER REFERENCES users(id),
    updated_at TIMESTAMP,
    indexed_at TIMESTAMP,                     -- when added to search index

    -- Status
    is_active BOOLEAN DEFAULT TRUE,

    UNIQUE(document_id, source_name)
);

CREATE INDEX idx_ext_trans_doc ON external_transcriptions(document_id);
CREATE INDEX idx_ext_trans_shelfmark ON external_transcriptions(shelfmark_normalized);
CREATE INDEX idx_ext_trans_source ON external_transcriptions(source_name);
```

#### `joined_transcriptions`

Stores transcriptions that span multiple fragments (linked to JoinGroup).

```sql
CREATE TABLE joined_transcriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Link to existing joins system
    join_group_id VARCHAR(36) REFERENCES join_groups(id) ON DELETE CASCADE,

    -- Content
    transcription_text TEXT NOT NULL,

    -- Line-to-fragment mapping (JSON)
    -- Format: {"1-10": "AIU VII.A.23", "11-43": "T-S 13J35.3"}
    line_mappings JSONB,

    -- Source information
    source_name VARCHAR(100) NOT NULL,
    source_url TEXT,
    source_reference TEXT,

    -- Metadata
    priority INTEGER DEFAULT 80,              -- joins typically higher quality
    quality_score FLOAT,

    -- Tracking
    imported_at TIMESTAMP DEFAULT NOW(),
    imported_by INTEGER REFERENCES users(id),
    updated_at TIMESTAMP,
    indexed_at TIMESTAMP,

    -- Status
    is_active BOOLEAN DEFAULT TRUE,

    UNIQUE(join_group_id, source_name)
);

CREATE INDEX idx_joined_trans_group ON joined_transcriptions(join_group_id);
CREATE INDEX idx_joined_trans_source ON joined_transcriptions(source_name);
```

#### `sync_state` (Desktop local SQLite)

Tracks sync status on desktop.

```sql
CREATE TABLE sync_state (
    sync_type VARCHAR(50) PRIMARY KEY,        -- "corrections", "external", "joined"
    last_sync_token VARCHAR(100),             -- server-provided token
    last_sync_at TIMESTAMP,
    items_synced INTEGER DEFAULT 0,
    last_full_refresh_at TIMESTAMP
);
```

### Modified Tables

#### `corrections` (add indexed_at)

```sql
ALTER TABLE corrections ADD COLUMN indexed_at TIMESTAMP;
```

#### `join_group_members` (add normalized shelfmark)

```sql
ALTER TABLE join_group_members ADD COLUMN shelfmark_normalized VARCHAR(200);
```

### Tantivy Index Schemas

#### Corrections Index Schema

```python
CORRECTIONS_SCHEMA = {
    "id": {"type": "u64", "stored": True, "indexed": True},
    "document_id": {"type": "text", "stored": True, "indexed": True, "tokenizer": "raw"},
    "shelfmark": {"type": "text", "stored": True, "indexed": True, "tokenizer": "raw"},
    "original_text": {"type": "text", "stored": True, "indexed": False},
    "corrected_text": {"type": "text", "stored": True, "indexed": True, "tokenizer": "hebrew"},
    "page_number": {"type": "u64", "stored": True, "indexed": True},
    "correction_type": {"type": "text", "stored": True, "indexed": True, "tokenizer": "raw"},
    "author_id": {"type": "u64", "stored": True, "indexed": True},
    "author_name": {"type": "text", "stored": True, "indexed": False},
    "created_at": {"type": "date", "stored": True, "indexed": True},
    "priority": {"type": "u64", "stored": True, "indexed": True},
}
```

#### External Transcriptions Index Schema

```python
EXTERNAL_SCHEMA = {
    "id": {"type": "text", "stored": True, "indexed": True, "tokenizer": "raw"},
    "document_id": {"type": "text", "stored": True, "indexed": True, "tokenizer": "raw"},
    "shelfmark": {"type": "text", "stored": True, "indexed": True, "tokenizer": "raw"},
    "transcription_text": {"type": "text", "stored": True, "indexed": True, "tokenizer": "hebrew"},
    "source_name": {"type": "text", "stored": True, "indexed": True, "tokenizer": "raw"},
    "source_reference": {"type": "text", "stored": True, "indexed": False},
    "priority": {"type": "u64", "stored": True, "indexed": True},
    "is_joined": {"type": "bool", "stored": True, "indexed": True},
    "join_shelfmarks": {"type": "text", "stored": True, "indexed": False},  -- JSON array
    "imported_at": {"type": "date", "stored": True, "indexed": True},
}
```

---

## Sync Mechanism

### API Endpoints

#### `GET /sync/corrections`

Returns approved corrections since last sync.

```
Request:
GET /sync/corrections?since_token={token}&limit=1000

Response:
{
    "corrections": [
        {
            "id": 123,
            "document_id": "99123456",
            "shelfmark": "T-S 8J6.1",
            "corrected_text": "...",
            "page_number": 1,
            "correction_type": "transcription",
            "author_id": 5,
            "author_name": "John Doe",
            "created_at": "2026-01-15T10:00:00Z",
            "updated_at": "2026-01-15T10:00:00Z"
        },
        ...
    ],
    "deleted_ids": [456, 789],           -- corrections deleted/rejected since last sync
    "next_token": "abc123",              -- use for next request
    "has_more": false,
    "server_time": "2026-01-19T12:00:00Z"
}
```

#### `GET /sync/external-transcriptions`

Returns external transcriptions since last sync.

```
Request:
GET /sync/external-transcriptions?since_token={token}&limit=500

Response:
{
    "transcriptions": [
        {
            "id": "uuid-123",
            "document_id": "99123456",
            "shelfmark": "T-S 13J35.3",
            "transcription_text": "...",
            "source_name": "princeton",
            "source_reference": "Gil 1983, vol. 2",
            "priority": 80,
            "is_joined": false,
            "join_shelfmarks": null,
            "imported_at": "2026-01-10T10:00:00Z"
        },
        ...
    ],
    "deleted_ids": ["uuid-456"],
    "next_token": "def456",
    "has_more": false
}
```

#### `GET /sync/joined-transcriptions`

Returns joined transcriptions since last sync.

```
Request:
GET /sync/joined-transcriptions?since_token={token}&limit=500

Response:
{
    "transcriptions": [
        {
            "id": "uuid-789",
            "join_group_id": "group-123",
            "shelfmarks": ["AIU VII.A.23", "T-S 13J35.3"],
            "document_ids": ["99111111", "99222222"],
            "transcription_text": "...",
            "line_mappings": {"1-10": "AIU VII.A.23", "11-43": "T-S 13J35.3"},
            "source_name": "princeton",
            "source_reference": "Gil 1983, vol. 2",
            "priority": 90,
            "imported_at": "2026-01-10T10:00:00Z"
        },
        ...
    ],
    "deleted_ids": ["uuid-111"],
    "next_token": "ghi789",
    "has_more": false
}
```

### Desktop Sync Manager

```python
class SyncManager:
    """Manages synchronization of corrections and transcriptions from web backend."""

    def __init__(self, api_client, local_db, corrections_indexer, external_indexer):
        self.api_client = api_client
        self.local_db = local_db
        self.corrections_indexer = corrections_indexer
        self.external_indexer = external_indexer

    async def sync_all(self, progress_callback=None) -> SyncResult:
        """
        Sync all data types. Called on startup (background) and manual trigger.
        Returns SyncResult with counts and any errors.
        """
        result = SyncResult()

        # Run syncs in parallel
        corrections_task = self.sync_corrections(progress_callback)
        external_task = self.sync_external_transcriptions(progress_callback)
        joined_task = self.sync_joined_transcriptions(progress_callback)

        await asyncio.gather(corrections_task, external_task, joined_task)

        return result

    async def sync_corrections(self, progress_callback=None) -> int:
        """Incremental sync of approved corrections."""
        token = self.local_db.get_sync_token("corrections")
        total_synced = 0

        while True:
            response = await self.api_client.get(
                "/sync/corrections",
                params={"since_token": token, "limit": 1000}
            )

            # Handle deletions first
            for deleted_id in response["deleted_ids"]:
                self.corrections_indexer.delete_correction(deleted_id)

            # Add/update corrections
            for correction in response["corrections"]:
                self.corrections_indexer.index_correction(correction)
                total_synced += 1

            token = response["next_token"]

            if not response["has_more"]:
                break

        # Save sync state
        self.local_db.set_sync_token("corrections", token)
        return total_synced

    def should_full_refresh(self, sync_type: str) -> bool:
        """Check if full refresh is needed (weekly)."""
        last_full = self.local_db.get_last_full_refresh(sync_type)
        if not last_full:
            return True
        return (datetime.now() - last_full).days >= 7
```

### Sync Behavior

| Event | Action |
|-------|--------|
| App startup | Background sync (non-blocking) |
| Manual "Sync" button | Foreground sync with progress |
| Weekly | Full refresh (re-download all, not just delta) |
| Sync failure | Retry 3x, then skip (use stale data) |
| Index corruption detected | Full rebuild |

---

## Search Integration

### Unified Search Query

```python
class UnifiedSearchEngine:
    """Queries multiple indexes and returns deduplicated results."""

    def __init__(self, main_index, corrections_index, external_index):
        self.main_index = main_index           # MiDRASH V0.7/V0.8
        self.corrections_index = corrections_index
        self.external_index = external_index

    def search(self, query: str, options: SearchOptions) -> UnifiedSearchResult:
        """
        Search across all indexes and return unified results.
        """
        # Query all indexes in parallel
        with ThreadPoolExecutor(max_workers=3) as executor:
            main_future = executor.submit(self.main_index.search, query, options)
            corr_future = executor.submit(self.corrections_index.search, query, options)
            ext_future = executor.submit(self.external_index.search, query, options)

            main_results = main_future.result()
            corr_results = corr_future.result()
            ext_results = ext_future.result()

        # Merge and deduplicate
        merged = self._merge_results(main_results, corr_results, ext_results)
        deduplicated = self._deduplicate_by_document(merged)
        ranked = self._rank_by_priority(deduplicated)

        return UnifiedSearchResult(
            results=ranked,
            total_main=len(main_results),
            total_corrections=len(corr_results),
            total_external=len(ext_results)
        )

    def _deduplicate_by_document(self, results: List[SearchHit]) -> List[DocumentResult]:
        """
        Group results by document_id.
        Each DocumentResult contains all versions that matched.
        """
        by_document = defaultdict(list)

        for hit in results:
            doc_id = hit.document_id
            by_document[doc_id].append(hit)

        document_results = []
        for doc_id, hits in by_document.items():
            # Sort hits by priority (highest first)
            hits.sort(key=lambda h: h.priority, reverse=True)

            document_results.append(DocumentResult(
                document_id=doc_id,
                shelfmark=hits[0].shelfmark,
                best_match=hits[0],           # highest priority version
                all_versions=hits,            # all matching versions
                version_count=len(hits)
            ))

        return document_results
```

### Search Result Display

```
┌─────────────────────────────────────────────────────────────────┐
│ T-S 13J35.3                                           [3 versions]│
│                                                                  │
│ Match: "...וסאל ען דלך פכאן מן קולה מעוד באללה..."              │
│                                                                  │
│ Showing: Princeton (Gil 1983) ▼                                 │
│ ┌──────────────────────────────────────────────────────────────┐│
│ │ ● Princeton (Gil 1983)        - Priority: 90                 ││
│ │ ○ User correction (John Doe)  - Priority: 70                 ││
│ │ ○ MiDRASH V0.8                - Priority: 50                 ││
│ └──────────────────────────────────────────────────────────────┘│
│                                                                  │
│ [View Document]  [Compare Versions]                              │
└──────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ [JOIN] AIU VII.A.23 + T-S 13J35.3                    [1 version] │
│                                                                  │
│ Match in joined transcription (line 12)                         │
│ "...ודעי אברהם וסאל ען דלך..."                                  │
│                                                                  │
│ Source: Princeton (Gil 1983)                                     │
│ Fragments: AIU VII.A.23, T-S 13J35.3                            │
│                                                                  │
│ [View Join]  [View AIU VII.A.23]  [View T-S 13J35.3]            │
└──────────────────────────────────────────────────────────────────┘
```

### Priority System

| Source | Priority | Rationale |
|--------|----------|-----------|
| External curated (Princeton, etc.) | 80-100 | Academic, peer-reviewed |
| User corrections (approved) | 60-79 | Community-verified |
| MiDRASH V0.8 | 50 | Latest automatic transcription |
| MiDRASH V0.7 | 40 | Older automatic transcription |

---

## External Import System

### Princeton Import Pipeline

```python
class PrincetonImporter:
    """Imports transcriptions from Princeton Genizah Project."""

    def import_document(self, html_content: str) -> ImportResult:
        """
        Parse Princeton HTML and import transcription.

        Steps:
        1. Parse HTML to extract title, content, metadata
        2. Extract shelfmarks from title (split on "+")
        3. Normalize shelfmarks
        4. Match to our database
        5. Create/find JoinGroup if multiple fragments
        6. Create JoinedTranscription or ExternalTranscription
        7. Index for search
        """
        # Step 1: Parse
        parsed = self._parse_html(html_content)

        # Step 2-3: Extract and normalize shelfmarks
        raw_shelfmarks = self._extract_shelfmarks(parsed.title)
        normalized = [normalize_shelfmark(s) for s in raw_shelfmarks]

        # Step 4: Match to our documents
        matches = []
        for norm_shelf in normalized:
            match = self._find_document(norm_shelf)
            if match:
                matches.append(match)
            else:
                # Create stub entry for unknown document
                match = self._create_stub_document(norm_shelf)
                matches.append(match)

        # Step 5-6: Create appropriate record
        if len(matches) > 1:
            # This is a join
            return self._import_as_join(parsed, matches)
        else:
            # Single fragment
            return self._import_as_external(parsed, matches[0])

    def _parse_html(self, html: str) -> ParsedDocument:
        """Extract structured data from Princeton HTML."""
        soup = BeautifulSoup(html, 'html.parser')

        title = soup.find('h1').get_text()

        # Extract source reference
        source_ref = soup.find('p').get_text() if soup.find('p') else None

        # Extract transcription lines with canvas mappings
        sections = []
        for div in soup.find_all('div', attrs={'data-canvas': True}):
            canvas_url = div['data-canvas']
            lines = [li.get_text() for li in div.find_all('li')]
            sections.append({
                'canvas_url': canvas_url,
                'shelfmark': self._canvas_to_shelfmark(canvas_url),
                'lines': lines,
                'start_line': None,  # computed later
                'end_line': None
            })

        # Compute line ranges
        line_num = 1
        for section in sections:
            section['start_line'] = line_num
            section['end_line'] = line_num + len(section['lines']) - 1
            line_num = section['end_line'] + 1

        return ParsedDocument(
            title=title,
            source_reference=source_ref,
            sections=sections,
            full_text='\n'.join(
                line for section in sections for line in section['lines']
            )
        )

    def _canvas_to_shelfmark(self, canvas_url: str) -> str:
        """
        Convert IIIF canvas URL to shelfmark.
        Example: https://cudl.lib.cam.ac.uk/iiif/MS-TS-00013-J-00035-00003/canvas/1
                 -> T-S 13J35.3
        """
        # Parse the URL path
        match = re.search(r'/MS-([^/]+)/canvas/', canvas_url)
        if match:
            raw = match.group(1)
            # Convert MS-TS-00013-J-00035-00003 to T-S 13J35.3
            # This needs library-specific logic
            return self._decode_cambridge_shelfmark(raw)
        return None
```

### Import Flow Diagram

```
┌─────────────────┐
│ Princeton HTML  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Parse HTML     │
│  - Title        │
│  - Sections     │
│  - Canvas URLs  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌──────────────────┐
│ Extract         │     │ Shelfmark        │
│ Shelfmarks      │────▶│ Normalization    │
│ from title      │     │ Function         │
└────────┬────────┘     └──────────────────┘
         │
         ▼
┌─────────────────┐     ┌──────────────────┐
│ Match to our    │────▶│ Fuzzy matching   │
│ database        │     │ if exact fails   │
└────────┬────────┘     └──────────────────┘
         │
         ├─── Single fragment ───▶ Create ExternalTranscription
         │
         └─── Multiple fragments ─▶ Create/find JoinGroup
                                   └─▶ Create JoinedTranscription
                                       └─▶ Populate line_mappings
```

---

## Shelfmark Normalization

### Normalization Function

```python
import re
from typing import Optional

def normalize_shelfmark(shelfmark: str) -> str:
    """
    Normalize shelfmark for reliable matching across sources.

    Handles variations like:
    - "T-S 8J6.1" vs "TS 8J 6.1" vs "T-S 8J 6.1"
    - "AIU VII.A.23" vs "AIU VII A 23"
    - Different dash types (-, –, —)

    Returns uppercase normalized form.
    """
    if not shelfmark:
        return ""

    s = shelfmark.strip()

    # Uppercase for consistency
    s = s.upper()

    # Standardize dash types
    s = s.replace('–', '-').replace('—', '-')

    # T-S (Taylor-Schechter) normalization
    s = re.sub(r'^TS[\s\-]*', 'T-S ', s)
    s = re.sub(r'^T[\s]*-[\s]*S[\s]*', 'T-S ', s)

    # Remove extra spaces
    s = re.sub(r'\s+', ' ', s)

    # Normalize number spacing in common patterns
    # "8J 6.1" -> "8J6.1"
    s = re.sub(r'(\d+[A-Z]+)\s+(\d)', r'\1\2', s)

    # Normalize Roman numerals spacing
    # "VII. A. 23" -> "VII.A.23"
    s = re.sub(r'([IVX]+)\.\s*([A-Z])\.\s*(\d+)', r'\1.\2.\3', s)

    return s.strip()


def shelfmark_similarity(shelf1: str, shelf2: str) -> float:
    """
    Calculate similarity between two shelfmarks.
    Returns 0.0 to 1.0.
    """
    n1 = normalize_shelfmark(shelf1)
    n2 = normalize_shelfmark(shelf2)

    if n1 == n2:
        return 1.0

    # Use Levenshtein ratio for fuzzy matching
    from Levenshtein import ratio
    return ratio(n1, n2)


def find_document_by_shelfmark(db, shelfmark: str, threshold: float = 0.85):
    """
    Find document by shelfmark with fuzzy matching fallback.

    1. Try exact match on normalized shelfmark
    2. If no match, try fuzzy search with threshold
    3. Return best match or None
    """
    normalized = normalize_shelfmark(shelfmark)

    # Exact match first
    exact = db.query(Document).filter(
        Document.shelfmark_normalized == normalized
    ).first()

    if exact:
        return exact

    # Fuzzy fallback - get candidates and compare
    # Use prefix match to narrow down candidates
    prefix = normalized[:5] if len(normalized) >= 5 else normalized
    candidates = db.query(Document).filter(
        Document.shelfmark_normalized.like(f"{prefix}%")
    ).limit(100).all()

    best_match = None
    best_score = 0

    for candidate in candidates:
        score = shelfmark_similarity(normalized, candidate.shelfmark_normalized)
        if score > best_score and score >= threshold:
            best_score = score
            best_match = candidate

    return best_match
```

### Common Shelfmark Patterns

| Collection | Pattern | Example | Normalized |
|------------|---------|---------|------------|
| Cambridge T-S | T-S {box}{class}.{num} | T-S 13J35.3 | T-S 13J35.3 |
| Cambridge T-S | T-S {collection} {num} | T-S Misc.28.103 | T-S MISC.28.103 |
| AIU (Paris) | AIU {roman}.{letter}.{num} | AIU VII.A.23 | AIU VII.A.23 |
| Bodleian | MS. Heb. {letter}. {num} | MS. Heb. d. 65 | MS. HEB. D.65 |
| JTS | JTS ENA {num}.{num} | JTS ENA 2727.23 | JTS ENA 2727.23 |

---

## Implementation Phases

### Phase 0: Preparation (1-2 days)
**Goal:** Set up infrastructure, no user-facing changes

- [ ] Create branch `claude/searchable-corrections-sync`
- [ ] Write database migrations for new tables
- [ ] Add `shelfmark_normalized` column to existing tables
- [ ] Implement shelfmark normalization function
- [ ] Write unit tests for normalization

**Deliverable:** Migrations ready, normalization tested

---

### Phase 1: Sync Infrastructure (3-4 days)
**Goal:** Desktop can sync corrections from web backend

- [ ] Implement `/sync/corrections` API endpoint
- [ ] Create `SyncManager` class for desktop
- [ ] Create local corrections Tantivy index
- [ ] Implement incremental sync with deletions
- [ ] Add sync state tracking (SQLite)
- [ ] Background sync on startup
- [ ] Manual "Sync" button in UI

**Deliverable:** Desktop syncs corrections, but search doesn't use them yet

**Testing:**
- Sync 1000 corrections, verify all indexed
- Delete correction on server, verify removed locally
- Interrupt sync, verify recovery

---

### Phase 2: Search Integration (3-4 days)
**Goal:** Search queries corrections index

- [ ] Create `UnifiedSearchEngine` class
- [ ] Modify desktop search to query both indexes
- [ ] Implement result deduplication
- [ ] Implement priority ranking
- [ ] Update search results UI to show source
- [ ] Add "Compare versions" feature

**Deliverable:** Search finds matches in both MiDRASH and corrections

**Testing:**
- Search term that only exists in correction
- Search term that exists in both (verify dedup)
- Verify priority ordering

---

### Phase 3: External Transcriptions (3-4 days)
**Goal:** Support non-joined external transcriptions

- [ ] Create `external_transcriptions` table
- [ ] Implement `/sync/external-transcriptions` API
- [ ] Create external transcriptions indexer
- [ ] Add to unified search
- [ ] Admin UI for manual import

**Deliverable:** External single-fragment transcriptions searchable

---

### Phase 4: Joined Transcriptions (4-5 days)
**Goal:** Support Princeton-style joined transcriptions

- [ ] Create `joined_transcriptions` table
- [ ] Link to existing `join_groups`
- [ ] Implement `/sync/joined-transcriptions` API
- [ ] Parse line-to-fragment mappings
- [ ] Update search results UI for joins
- [ ] Navigation from join result to individual fragments

**Deliverable:** Joined transcriptions searchable with proper attribution

---

### Phase 5: Princeton Import (3-4 days)
**Goal:** Automated import from Princeton

- [ ] Implement Princeton HTML parser
- [ ] Canvas URL to shelfmark converter
- [ ] Fuzzy shelfmark matching
- [ ] Batch import tool
- [ ] Import progress tracking
- [ ] Error handling for unmatched shelfmarks

**Deliverable:** Can import Princeton transcriptions in bulk

---

### Phase 6: Web Search (2-3 days)
**Goal:** Same search capabilities on web

- [ ] Implement web search API endpoint
- [ ] Unified search on server side
- [ ] Web UI for search results with sources

**Deliverable:** Web has same search capabilities as desktop

---

### Phase 7: Polish & Hardening (2-3 days)
**Goal:** Production ready

- [ ] Performance optimization
- [ ] Error handling and recovery
- [ ] Index corruption detection and rebuild
- [ ] Logging and monitoring
- [ ] Documentation

**Deliverable:** Feature ready for release

---

## Risk Analysis and Mitigation

### High Risk

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Index corruption | Search broken | Medium | Checksums, auto-rebuild, backups |
| Sync breaks search | Core feature broken | Medium | Feature flag to disable sync results |
| Performance regression | Slow search | Medium | Benchmark before/after, query optimization |
| Shelfmark matching failures | Missing results | High | Fuzzy matching, manual review queue |

### Medium Risk

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Duplicate results confuse users | UX issue | Medium | Clear dedup logic, UI explanation |
| Sync takes too long | Startup delay | Low | Background sync, progress indicator |
| External source changes format | Import breaks | Medium | Modular parsers, validation |

### Mitigation Strategies

**1. Feature Flag**
```python
FEATURE_FLAGS = {
    "search_corrections": True,      # Can disable if issues
    "search_external": True,
    "background_sync": True,
}
```

**2. Index Backup**
- Before any sync, backup current index
- If sync fails, restore from backup
- Keep last 3 index backups

**3. Gradual Rollout**
- Phase 1-2: Internal testing only
- Phase 3-4: Beta users
- Phase 5+: General availability

---

## Testing Strategy

### Unit Tests

```python
# test_shelfmark_normalization.py
def test_ts_variations():
    assert normalize_shelfmark("T-S 8J6.1") == "T-S 8J6.1"
    assert normalize_shelfmark("TS 8J6.1") == "T-S 8J6.1"
    assert normalize_shelfmark("T-S 8J 6.1") == "T-S 8J6.1"
    assert normalize_shelfmark("t-s 8j6.1") == "T-S 8J6.1"

def test_aiu_variations():
    assert normalize_shelfmark("AIU VII.A.23") == "AIU VII.A.23"
    assert normalize_shelfmark("AIU VII. A. 23") == "AIU VII.A.23"

# test_sync_manager.py
def test_sync_handles_deletions():
    # Setup: index correction locally
    # Server marks correction as deleted
    # Sync
    # Assert: correction removed from local index

def test_sync_resumes_after_failure():
    # Setup: start sync
    # Interrupt after 50%
    # Resume sync
    # Assert: all items synced, no duplicates
```

### Integration Tests

```python
# test_unified_search.py
def test_search_finds_correction():
    # Create correction with unique text
    # Approve correction
    # Sync to desktop
    # Search for unique text
    # Assert: found in results with source="correction"

def test_search_deduplicates():
    # Same document has MiDRASH + correction + external
    # Search for term in all three
    # Assert: one result with 3 versions

def test_priority_ordering():
    # Document has Princeton (90) + correction (70) + MiDRASH (50)
    # Search
    # Assert: Princeton shown as "best match"
```

### Performance Tests

```python
def test_search_performance():
    # Index 10,000 corrections
    # Index 5,000 external transcriptions
    # Search common term
    # Assert: results in < 500ms

def test_sync_performance():
    # Sync 10,000 corrections
    # Assert: completes in < 30 seconds
```

---

## Rollback Plan

### If Issues in Production

**Level 1: Disable new search sources**
```python
# Quick toggle in settings
SEARCH_CORRECTIONS = False
SEARCH_EXTERNAL = False
# Search reverts to MiDRASH only
```

**Level 2: Disable sync**
```python
# Desktop skips sync on startup
ENABLE_SYNC = False
# Users keep existing local data
```

**Level 3: Full rollback**
```bash
# Revert to previous version
git checkout v4.1.1
# Users prompted to rebuild index
```

### Data Preservation

- Local correction index is separate from main index
- Can delete correction index without affecting MiDRASH search
- External transcriptions in separate table, can truncate

---

## Open Questions

1. **Q: Should corrections be editable after approval?**
   - If yes: need to sync updates, not just new corrections
   - Current assumption: yes, track `updated_at`

2. **Q: How to handle corrections that span multiple pages?**
   - Need to decide: one index entry per correction, or split by page?

3. **Q: Should we show "no results in corrections" explicitly?**
   - UX decision: show absence of correction results, or just omit?

4. **Q: Rate limiting for sync API?**
   - Desktop might sync frequently; need to protect server

5. **Q: What if Princeton transcription contradicts approved correction?**
   - Priority says Princeton wins, but user might disagree
   - Consider: user override per document?

6. **Q: Offline correction submission queue?**
   - Out of scope for v1, but may need later

---

## Appendix: File Structure

```
backend/
├── api/routes/
│   ├── sync.py                    # NEW: Sync endpoints
│   └── import.py                  # NEW: Import endpoints
├── services/
│   ├── sync_service.py            # NEW: Sync logic
│   ├── external_import_service.py # NEW: Princeton importer
│   └── shelfmark_service.py       # NEW: Normalization
├── models/
│   ├── external_transcription.py  # NEW
│   └── joined_transcription.py    # NEW
└── migrations/
    ├── add_external_transcriptions.py
    ├── add_joined_transcriptions.py
    └── add_shelfmark_normalized.py

desktop/
├── sync/
│   ├── sync_manager.py            # NEW: Desktop sync
│   ├── corrections_indexer.py     # NEW: Local indexer
│   └── external_indexer.py        # NEW: Local indexer
├── search/
│   └── unified_search.py          # NEW: Multi-index search
└── data/
    ├── corrections_index/         # NEW: Local Tantivy
    └── external_index/            # NEW: Local Tantivy
```

---

*This specification should be reviewed and approved before implementation begins.*

*Last updated: January 2026*
*Author: Claude Code*
*Branch: claude/searchable-corrections-sync*
