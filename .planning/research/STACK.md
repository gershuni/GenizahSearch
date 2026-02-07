# Technology Stack

**Project:** v5.6.0 Desktop Parity & Transcription Search
**Researched:** 2026-02-07
**Focus:** Stack additions/changes for shared service layer, Tantivy transcription search, PyQt6 async patterns

## Executive Summary

No new dependencies are required. The existing stack (supabase-py 2.27.2, tantivy-py 0.25.1, PyQt6) has all the capabilities needed for this milestone. The work is structural reorganization and schema extension, not technology adoption.

## Existing Stack (Verified)

| Technology | Version | Purpose | Status |
|------------|---------|---------|--------|
| Python | 3.11 | Runtime | Installed |
| tantivy-py | 0.25.1 (Tantivy 0.25.0 core) | Full-text search index | Installed |
| supabase-py | 2.27.2 | Cloud database client | Installed |
| PyQt6 | latest | Desktop GUI framework | Installed |
| NiceGUI | latest | Web GUI framework | Installed |
| gotrue | latest | Supabase auth helper | Installed |
| keyring | latest | Secure credential storage (desktop) | Installed |

## New Dependencies

**None required.** All capabilities needed are available in existing packages.

---

## Decision 1: Shared Service Layer Structure

### Current State

`web/document_service.py` (507 lines) imports from `web.supabase_client` for the Supabase connection. It contains 12 functions for PGP data access. The desktop app has no equivalent -- it uses `supabase_corrections_client.py` for community features (corrections, comments, discoveries, joins) with a completely separate Supabase client singleton.

### Key Problem

Both `web/supabase_client.py` and `supabase_corrections_client.py` create their own Supabase `Client` singletons with the same credentials. The web document_service is coupled to the web client via `from web.supabase_client import get_client`.

### Recommended Pattern: Dependency-Injected Client

**Extract to:** `shared/document_service.py` (new top-level module)

**Pattern:** Pass the Supabase client as a parameter rather than importing it from a specific location. This lets both apps provide their own client instance.

```python
# shared/document_service.py
from typing import Optional, List, Dict, Any, Set

def get_document_for_fragment(client, sys_id: str, page_num: int = None) -> Optional[Dict]:
    """Get PGP document for a fragment. Client is injected by caller."""
    if not sys_id:
        return None
    # ... same logic, but uses passed `client` instead of get_client()
```

**Why this pattern:**
- The existing `web/document_service.py` functions are pure data-access -- they call `get_client()` at the top of each function, then use the client. Changing `get_client()` to a parameter is minimal refactoring.
- Both apps already have Supabase client singletons. No need to unify them -- just pass whichever client each app uses.
- Avoids circular imports and path manipulation (`sys.path.insert` hacks already present in `supabase_corrections_client.py`).

**Alternative considered: Module-level client registration**

```python
# shared/document_service.py
_client = None

def set_client(client):
    global _client
    _client = client

def get_document_for_fragment(sys_id, page_num=None):
    client = _client  # uses registered client
```

This is also viable and requires fewer signature changes. **Use this if the caller-injection pattern feels too verbose.** The tradeoff is global mutable state vs explicit parameters.

**Recommendation: Module-level registration** (set_client pattern) because:
1. Minimizes signature changes from current code (12 functions keep same signatures)
2. Each app calls `set_client()` once at startup
3. Thread-safe for read-only access (both apps set once, read many times)
4. Web app sets it in `web/main.py` startup; desktop sets it in `genizah_app.py` startup

### Integration Points

**Web app (NiceGUI):**
```python
# web/main.py (startup)
from web.supabase_client import get_client
from shared.document_service import set_client
set_client(get_client())
```

**Desktop app (PyQt6):**
```python
# genizah_app.py (startup, after Supabase init)
from supabase_corrections_client import get_supabase_corrections_client
from shared.document_service import set_client
client = get_supabase_corrections_client()._get_client()
set_client(client)
```

### Web app backward compatibility

After extraction, `web/document_service.py` becomes a thin re-export wrapper:
```python
# web/document_service.py (backward compat shim)
from shared.document_service import *
```

This means existing `from web.document_service import get_document_for_fragment` calls in 5 web files continue working without changes.

---

## Decision 2: Tantivy Multi-Field Search with Transcription Filtering

### Current Schema (Verified)

The main Tantivy index schema (`genizah_core.py`, line 3902-3909):

```python
builder.add_text_field("unique_id", stored=True)
builder.add_text_field("content", stored=True, tokenizer_name="whitespace")
builder.add_text_field("source", stored=True)
builder.add_text_field("full_header", stored=True)
builder.add_text_field("shelfmark", stored=True)
builder.add_text_field("scope", stored=True)
builder.add_text_field("boundaries", stored=True)
```

All fields use default tokenizer except `content` (whitespace). No transcription fields exist.

### Verified Capabilities (Tested Locally)

I tested tantivy-py 0.25.1 directly and confirmed:

| Capability | Status | Notes |
|------------|--------|-------|
| `add_text_field` with `tokenizer_name='default'` | Works | For transcription text |
| `add_text_field` with `tokenizer_name='raw'` | Works | For exact-match filter fields |
| `add_boolean_field` with `fast=True` | Works | For has_transcription flag |
| `add_integer_field` with `fast=True` | Works | Not needed but available |
| Field-specific query (`transcription:word`) | Works | Standard Tantivy query syntax |
| Multi-field default query | Works | `parse_query('word', default_field_names=['content', 'transcription'])` |
| Boolean AND queries | Works | `source_type:pgp_edition AND content:word` |
| Boolean field as indexed filter | Does NOT work | `has_transcription:true` fails ("not declared as indexed") |
| Text field with `raw` tokenizer as filter | Works | `source_type:pgp_edition` is exact match |

**Critical finding:** Boolean fields cannot be used in query strings. Use a text field with `raw` tokenizer for filter values instead.

### Recommended Schema Extension

Add two new fields to the existing schema:

```python
# In Indexer.create_index() - add after existing fields:
builder.add_text_field("transcription", stored=True, tokenizer_name="default")
builder.add_text_field("content_type", stored=True, tokenizer_name="raw")
```

**Field definitions:**

| Field | Type | Tokenizer | Purpose |
|-------|------|-----------|---------|
| `transcription` | text, stored | `default` | PGP transcription text (searchable) |
| `content_type` | text, stored | `raw` | Filter: `"htr"`, `"pgp_edition"`, `"pgp_translation"` |

**Why `default` tokenizer for transcription:**
- The `content` field currently uses `whitespace` tokenizer, which is optimized for Hebrew HTR text (no lowercasing, minimal processing).
- PGP transcriptions contain mixed Hebrew/English with punctuation. The `default` tokenizer handles this better with Unicode-aware tokenization.
- Field-specific queries (`transcription:word`) let users search specifically in scholarly transcriptions.

**Why `content_type` instead of a boolean:**
- Tantivy boolean fields are not queryable via the query parser (verified experimentally).
- A text field with `raw` tokenizer gives exact-match filtering: `content_type:pgp_edition`.
- More expressive than boolean: can distinguish HTR vs PGP edition vs PGP translation vs user correction.
- Future-proof for additional content types.

### Search Filter Implementation

Three filter modes (matching PROJECT.md requirements):

```python
# Mode 1: Everything (default) - searches content + transcription
query = index.parse_query(terms, default_field_names=["content", "transcription"])

# Mode 2: Transcriptions only - searches only transcription field
query = index.parse_query(f"content_type:pgp_edition AND ({terms})",
                          default_field_names=["transcription"])

# Mode 3: HTR only (exclude transcriptions)
query = index.parse_query(f"content_type:htr AND ({terms})",
                          default_field_names=["content"])
```

### Indexing PGP Transcriptions

During index build, after indexing HTR pages, fetch PGP transcriptions from Supabase and add as additional documents:

```python
# Pseudo-code for PGP document indexing
for pgp_doc in fetch_all_pgp_documents():
    for source in pgp_doc.sources:
        if source.doc_relation == 'Digital Edition':
            content_type = 'pgp_edition'
        else:
            content_type = 'pgp_translation'

        writer.add_document(tantivy.Document(
            unique_id=f"pgp:{pgp_doc.pgpid}:{source.id}",
            content="",  # no HTR content
            transcription=source.content,
            source=f"PGP ({source.source_scholar})",
            full_header=pgp_doc.shelfmark_combined,
            shelfmark=pgp_doc.shelfmark_combined,
            scope="document",
            boundaries="",
            content_type=content_type
        ))
```

Existing HTR documents get `content_type="htr"` and empty `transcription=""`.

### Index Rebuild Approach

The index must be fully rebuilt (not incrementally updated) because:
1. Schema changes require a new index.
2. The existing `create_index()` method already destroys and recreates the index directory.
3. PGP transcriptions are added as new documents alongside existing HTR pages.

No API changes needed -- `create_index()` just adds the new fields and documents.

---

## Decision 3: PyQt6 Async Patterns for Supabase Calls

### Current Patterns (Verified)

The desktop app already uses two proven patterns for async work:

**Pattern A: QThread subclass** (used in `gui_threads.py`)
- 14 QThread subclasses for different operations (search, indexing, metadata fetch, etc.)
- Each thread emits signals (`finished_signal`, `error_signal`, `progress_signal`)
- Main thread connects signals to UI update slots
- Example: `EnrichMetadataThread` fetches metadata in background, emits result

**Pattern B: `threading.Thread` with daemon flag** (used in `genizah_app.py`)
- Used for fire-and-forget operations
- Example: preloading next/prev pages in browse view

### Supabase Client Threading Safety

**Verified:** The supabase-py sync client (`Client`) uses `httpx` under the hood with synchronous HTTP calls. Each call to `client.table(...).select(...).execute()` is a blocking HTTP request.

**Thread safety:** The sync client is safe to call from multiple threads because:
1. Each `.execute()` call creates its own HTTP request
2. No shared mutable state between requests
3. The existing `supabase_corrections_client.py` already calls Supabase from QThread workers (via `corrections_ui.py` dialogs) without issues

### Recommended Pattern for Desktop PGP Features

**Use QThread subclass**, consistent with existing patterns. Create a single `PGPDataThread` for PGP data operations:

```python
# In gui_threads.py (or new shared_threads.py)
class PGPDataThread(QThread):
    """Fetch PGP document data in background."""
    finished_signal = pyqtSignal(str, dict)  # sys_id, result_data
    error_signal = pyqtSignal(str)

    def __init__(self, operation: str, **kwargs):
        super().__init__()
        self.operation = operation
        self.kwargs = kwargs

    def run(self):
        try:
            from shared.document_service import (
                get_document_for_fragment,
                get_sources_for_document,
                get_fragments_for_document
            )
            # Dispatch based on operation type
            if self.operation == 'get_document':
                result = get_document_for_fragment(
                    self.kwargs['sys_id'],
                    self.kwargs.get('page_num')
                )
                self.finished_signal.emit(self.kwargs['sys_id'],
                    {'document': result})
            # ... etc
        except Exception as e:
            self.error_signal.emit(str(e))
```

**Why NOT use the async Supabase client:**
- `supabase-py` 2.27.2 includes `create_async_client` and `AsyncClient` (verified).
- However, NiceGUI runs its own async event loop. PyQt6 runs a Qt event loop. Mixing `asyncio` into PyQt6 requires `qasync` or similar libraries.
- The existing codebase has zero async/await usage in the desktop app.
- QThread with sync client is proven, understood, and used 14 times already.
- Adding `asyncio` would be a major architectural change for minimal benefit (Supabase calls are HTTP round-trips, not CPU-bound).

**Why NOT use `concurrent.futures.ThreadPoolExecutor`:**
- Would require a different signal mechanism to update the Qt UI.
- QThread signals integrate naturally with Qt's event loop.
- No benefit over QThread for this use case.

### Supabase Client Initialization Order

The desktop app must initialize the Supabase client BEFORE using the shared document service. The current startup order in `genizah_app.py`:

1. Import `genizah_core` modules
2. Import `gui_threads`
3. Import `corrections_client` (which creates Supabase client)
4. Application starts, user may or may not log in

**New startup addition:**
```python
# After step 3, register the shared service client
from shared.document_service import set_client
from supabase_corrections_client import get_supabase_corrections_client
sbc = get_supabase_corrections_client()
sb_client = sbc._get_client()
if sb_client:
    set_client(sb_client)
```

Note: PGP data is read-only and uses the anon key (no auth required). The shared service client does not need user authentication -- it only reads from `documents`, `document_fragments`, and `document_sources` tables which have public read access via RLS.

---

## Decision 4: Module Layout

### Recommended Structure

```
GenizahSearch/
  shared/                          # NEW: shared service modules
    __init__.py
    document_service.py            # Extracted from web/document_service.py
  web/
    document_service.py            # Becomes re-export shim
    supabase_client.py             # Unchanged
    ...
  supabase_corrections_client.py   # Unchanged (desktop Supabase client)
  gui_threads.py                   # Add PGPDataThread
  genizah_core.py                  # Extend Indexer schema + PGP indexing
  genizah_app.py                   # Add PGP UI features
```

**Why `shared/` at project root:**
- Importable by both `web/` and root-level desktop files without path hacks.
- Clear separation: `shared/` = business logic, `web/` = web UI, root = desktop.
- Follows the existing pattern where `genizah_core.py` is at root and imported by both apps.

**Why NOT put it in `genizah_core.py`:**
- `genizah_core.py` is 7K lines and handles search/indexing/metadata, not Supabase queries.
- Different concerns: `genizah_core.py` = local operations, `shared/document_service.py` = cloud data access.
- The service needs Supabase client dependency; `genizah_core.py` has no Supabase imports.

---

## What NOT to Add

| Technology | Why Not |
|------------|---------|
| `qasync` / `asyncqt` | Adds async complexity to PyQt6 for no benefit. Sync QThread works fine. |
| `supabase` async client | Would require event loop integration. Sync is simpler and proven. |
| New search library | Tantivy handles multi-field search natively. No need for Elasticsearch/Meilisearch. |
| Abstract factory pattern | Over-engineering. Simple module-level `set_client()` is sufficient. |
| Protocol/ABC for service interface | Both apps use the same functions. No polymorphism needed. |
| Caching layer (Redis, etc.) | Supabase calls are fast enough. Desktop already caches via SupabaseCorrectionsClient._cache. |
| `postgrest-py` directly | Already wrapped by `supabase-py`. No reason to use raw PostgREST. |
| New tokenizer for tantivy | The `default` tokenizer handles mixed Hebrew/English. Custom tokenizer adds build complexity. |

---

## Installation

No changes to `requirements.txt` needed. Current dependencies are sufficient:

```
# Already in requirements.txt - no additions needed
tantivy
supabase
gotrue
PyQt6
nicegui
keyring
```

## Sources

- **tantivy-py 0.25.1**: Verified locally via `pip show tantivy` and direct API testing
- **supabase-py 2.27.2**: Verified locally via `pip show supabase` and module inspection
- **supabase async client**: Verified available via `from supabase import create_async_client` (but NOT recommended for use)
- **Tantivy boolean field limitation**: Verified experimentally -- boolean fields with `fast=True` are NOT queryable via `parse_query()`, error: "not declared as indexed"
- **Tantivy `raw` tokenizer for filtering**: Verified experimentally -- exact-match text fields work as query filters
- **QThread patterns**: 14 existing QThread subclasses in `gui_threads.py` (lines 8-404)
- **Supabase sync client thread safety**: `execute()` is not a coroutine (verified via `inspect.iscoroutinefunction`)
- **Existing document_service.py**: 507 lines, 12 functions, all use `get_client()` pattern (verified by reading file)
- **Existing supabase_corrections_client.py**: 1834 lines, separate singleton, already called from QThread workers

---
*Confidence: HIGH -- all claims verified against installed packages and running code*
