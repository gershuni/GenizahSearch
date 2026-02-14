# Phase 13: Transcription Search - Research

**Researched:** 2026-02-08
**Domain:** Tantivy full-text search indexing, Supabase data fetching, NiceGUI/PyQt6 search UI
**Confidence:** HIGH

## Summary

Phase 13 makes PGP transcriptions and user corrections searchable through the existing Tantivy full-text search infrastructure. The codebase has **two separate Tantivy indexes**: (1) the **main index** (`Indexer.create_index`, stored at `{INDEX_DIR}/tantivy_db`) used by `SearchEngine` for the desktop search tab and web standard search, and (2) the **lab index** (`LabEngine.rebuild_lab_index`, stored at `{INDEX_DIR}/lab_index`) used for advanced fingerprint/composition searches. Phase 13 needs to extend the **main index only** -- the lab index is a specialized fingerprint engine that operates on different principles.

The main index currently stores HTR transcriptions (V0.8 and V0.7) from local text files (`Transcriptions.txt`, `AllGenizah_OLD.txt`). Each document has fields: `unique_id`, `content`, `source` (either "V0.8" or "V0.7"), `full_header`, `shelfmark`, `scope`, and `boundaries`. To add PGP transcription search, we need to: (1) add a `content_type` text field to the schema to distinguish HTR content from PGP/correction content, (2) fetch PGP transcriptions from Supabase `documents` table and user corrections from `corrections` table during index rebuild, (3) add those as new documents to the index with `content_type` values like "pgp" and "correction", and (4) modify `SearchEngine.execute_search` to support filtering by `content_type` using tantivy-py's `Query.boolean_query` with `Occur.Must/MustNot` constraints.

The key technical insight from prior research (MEMORY.md): **Tantivy boolean fields are NOT queryable via `parse_query`** -- you cannot use `parse_query("is_pgp:true")`. Instead, content type must be stored as a text field and filtered using `field:value` syntax within `parse_query`, or more reliably using programmatic `Query.boolean_query` with `Query.term_query` constraints. The tantivy-py 0.25.1 API (installed in this project) provides `Query.boolean_query`, `Query.term_query`, and all needed query composition methods.

**Primary recommendation:** Add a `content_type` text field (values: "htr", "pgp", "correction") to the main Tantivy index schema. During index rebuild, fetch PGP transcriptions via `shared/document_service.py` and approved corrections via Supabase `corrections` table. Use `Query.boolean_query` to compose content-type filters with the user's search query. Implement temp-then-swap pattern for safe index rebuilds.

## Standard Stack

### Core (already in project)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tantivy (tantivy-py) | 0.25.1 | Full-text search index | Already used for both main and lab indexes |
| supabase-py | 2.x | Database client for PGP data | Already used via `shared/supabase_provider.py` |
| PyQt6 | 6.x | Desktop UI framework | Already used for desktop search tab |
| NiceGUI | 2.x | Web UI framework | Already used for web search page |

### Supporting (already in project)
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `shared/document_service.py` | N/A | PGP data access layer | Fetching transcriptions during index build |
| `shared/supabase_provider.py` | N/A | Supabase client singleton | Getting client for corrections queries |
| `gui_threads.py` | N/A | QThread workers for desktop | Background index rebuild on desktop |
| `genizah_core.py` | N/A | `Indexer`, `SearchEngine`, `Config` | All index/search modifications |

### No new dependencies needed
All required libraries are already in the project.

## Architecture Patterns

### Two-Index Architecture (DO NOT CONFUSE)

```
genizah_core.py
    |
    +-- Indexer (line 3891)
    |   |-- create_index() -> {INDEX_DIR}/tantivy_db
    |   |-- Schema: unique_id, content, source, full_header, shelfmark, scope, boundaries
    |   |-- Sources: Transcriptions.txt (V0.8), AllGenizah_OLD.txt (V0.7)
    |   \-- Used by: SearchEngine
    |
    +-- SearchEngine (line 4139)
    |   |-- execute_search() -> searches {INDEX_DIR}/tantivy_db
    |   |-- parse_query() on "content" field
    |   |-- Used by: Desktop SearchThread, Web execute_search
    |   \-- ** THIS IS WHAT PHASE 13 MODIFIES **
    |
    +-- LabEngine (line 489)
        |-- rebuild_lab_index() -> {INDEX_DIR}/lab_index
        |-- Schema: unique_id, text_normalized, fingerprint, fingerprint_dyn, content, etc.
        |-- Sources: Same text files, but with fingerprint encoding
        \-- Used by: Lab/composition search (DO NOT MODIFY)
```

### Pattern 1: Content Type Field for Filtering

**What:** Add a `content_type` text field to distinguish content sources. Use text values ("htr", "pgp", "correction") because tantivy-py `parse_query` cannot filter boolean fields.

**When to use:** Every document added to the index gets a `content_type` value.

**Why text, not boolean:** From MEMORY.md: "Tantivy boolean fields NOT queryable via parse_query -- use raw tokenizer text fields." A text field with known values can be filtered via `parse_query("content_type:pgp")` or programmatic `Query.term_query`.

**Example:**
```python
# Schema extension
builder.add_text_field("content_type", stored=True)  # "htr", "pgp", "correction"

# When adding HTR documents (existing):
writer.add_document(tantivy.Document(
    unique_id=str(cid), content="\n".join(ctext), source=str(label),
    full_header=str(chead), shelfmark=str(shelfmark),
    scope="page", boundaries="",
    content_type="htr"  # NEW FIELD
))

# When adding PGP transcription documents (new):
writer.add_document(tantivy.Document(
    unique_id=f"pgp:{pgpid}",
    content=transcription_text,
    source="PGP",
    full_header=f"PGP:{pgpid} {shelfmark}",
    shelfmark=str(shelfmark),
    scope="page",
    boundaries="",
    content_type="pgp"
))

# When adding user correction documents (new):
writer.add_document(tantivy.Document(
    unique_id=f"corr:{correction_id}",
    content=corrected_text,
    source="correction",
    full_header=f"CORR:{sys_id}",
    shelfmark=str(shelfmark),
    scope="page",
    boundaries="",
    content_type="correction"
))
```

### Pattern 2: Query Composition with content_type Filter

**What:** Use tantivy-py's `Query.boolean_query` to combine the user's text query with content_type filter constraints.

**When to use:** When user selects "Transcriptions only" or "Exclude transcriptions" filter.

**Example:**
```python
from tantivy import Query, Occur

# User's original text query (already built by SearchEngine)
text_query = index.parse_query(t_query_str, ["content"])

# Filter: "Transcriptions only" (PGP + corrections, exclude HTR)
transcription_filter = Query.boolean_query([
    (Occur.Must, text_query),
    (Occur.MustNot, Query.term_query(schema, "content_type", "htr"))
])

# Filter: "Exclude transcriptions" (HTR only, no PGP/corrections)
htr_only_filter = Query.boolean_query([
    (Occur.Must, text_query),
    (Occur.Must, Query.term_query(schema, "content_type", "htr"))
])

# Filter: "All content" (default) -- no additional filter, use text_query as-is
```

### Pattern 3: Safe Temp-Then-Swap Index Rebuild

**What:** Build the new index in a temporary directory, verify it, then atomically swap with the existing one. Existing index remains usable until swap completes.

**When to use:** Every time `Indexer.create_index` is called. Critical because current implementation uses destructive `shutil.rmtree`.

**Example:**
```python
import tempfile
import shutil
import os

db_path = os.path.join(Config.INDEX_DIR, "tantivy_db")
temp_path = os.path.join(Config.INDEX_DIR, "tantivy_db_building")
backup_path = os.path.join(Config.INDEX_DIR, "tantivy_db_old")

# 1. Build in temp directory
if os.path.exists(temp_path):
    shutil.rmtree(temp_path)
os.makedirs(temp_path)

# ... build index in temp_path ...
writer.commit()

# 2. Verify new index
try:
    test_index = tantivy.Index.open(temp_path)
    test_searcher = test_index.searcher()
    # Basic sanity: check doc count > 0
    test_query = test_index.parse_query("*", ["content"])
    hits = test_searcher.search(test_query, 1).hits
    assert len(hits) > 0, "New index appears empty"
except Exception as e:
    # Rollback: remove failed temp, keep existing
    shutil.rmtree(temp_path, ignore_errors=True)
    raise RuntimeError(f"Index verification failed: {e}")

# 3. Atomic swap
if os.path.exists(backup_path):
    shutil.rmtree(backup_path, ignore_errors=True)
if os.path.exists(db_path):
    os.rename(db_path, backup_path)  # Move current to backup
os.rename(temp_path, db_path)        # Move new to current

# 4. Cleanup backup
try:
    shutil.rmtree(backup_path, ignore_errors=True)
except Exception:
    pass  # Non-critical
```

### Pattern 4: PGP Transcription Fetching During Index Build

**What:** During index rebuild, fetch all PGP transcriptions from Supabase and add them to the index.

**When to use:** Inside `Indexer.create_index()`, after indexing HTR documents.

**Key consideration:** There are 35,839 PGP documents but only ~7,664 have edition transcriptions and ~1,696 have translations. The transcription text is in the `documents.transcription` column. The `document_fragments` table links PGP documents to sys_ids, enabling us to construct proper `unique_id` and `shelfmark` values.

**Data flow:**
```
Supabase documents table (35,839 records)
    |-- transcription column (text, nullable)
    |-- pgpid (PK)
    |-- shelfmark_combined
    \-- via document_fragments -> sys_id mapping

Supabase corrections table
    |-- corrected_text column
    |-- sys_id (text)
    |-- shelfmark (text)
    |-- status = 'approved' (filter)
    \-- page_number (int)
```

**Batch fetching pattern:**
```python
from shared.supabase_provider import get_client

client = get_client()

# Fetch PGP transcriptions in batches
PAGE_SIZE = 1000
offset = 0
while True:
    response = client.table('documents').select(
        'pgpid, transcription, shelfmark_combined'
    ).not_.is_('transcription', 'null').range(
        offset, offset + PAGE_SIZE - 1
    ).execute()

    if not response.data:
        break

    for doc in response.data:
        # Add to index...

    offset += PAGE_SIZE
    if len(response.data) < PAGE_SIZE:
        break

# Fetch approved user corrections
corrections_response = client.table('corrections').select(
    'id, sys_id, shelfmark, corrected_text, page_number'
).eq('status', 'approved').execute()
```

### Pattern 5: Linking PGP Results Back to Manuscripts

**What:** When a PGP transcription search result is found, it needs to link back to the GenizahSearch manuscript (sys_id) for display in the results table.

**When to use:** In search result processing, when the `content_type` is "pgp".

**Key consideration:** A PGP document may link to multiple fragments (sys_ids). For search results, we want to show the manuscript-level result. The `document_fragments` table provides this mapping.

**Approach:** During indexing, store the sys_id (from `document_fragments`) in the `unique_id` field for PGP documents, similar to how HTR documents use the sys_id. For multi-fragment PGP documents, either create one index document per fragment or one per PGP document with the primary sys_id.

**Recommendation:** Create one index document per PGP document (not per fragment). Store `unique_id=f"pgp:{pgpid}"` and include the first linked sys_id in the `full_header` field for display metadata lookup. The deduplication logic in `_deduplicate` already handles V0.8/V0.7 source priority; extend it to handle PGP and correction sources alongside HTR.

### Anti-Patterns to Avoid

- **Modifying the LabEngine index:** Phase 13 only touches the main `Indexer`/`SearchEngine` pair. The lab index uses fingerprint encoding, not raw text search, and has a completely different schema and purpose.

- **Using boolean fields for content_type:** Tantivy-py's `parse_query` cannot filter boolean fields. Use a text field instead.

- **Destructive index rebuild without backup:** The current `create_index` uses `shutil.rmtree(db_path)` before building. This makes the index unavailable during rebuild and risks data loss on failure. Always use temp-then-swap.

- **Fetching all 35,839 documents when only ~9,360 have transcriptions:** Filter for non-null transcription column to avoid unnecessary network traffic.

- **Blocking UI during Supabase fetch:** Index rebuild with Supabase fetching runs in background thread (desktop: QThread via `SearchThread` pattern, web: `run.io_bound`). Never call Supabase from the main thread.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Content-type filtering | Custom post-query filtering | `Query.boolean_query` with `Query.term_query` | Tantivy does the filtering at search time, much faster than post-filtering |
| Transcription fetching | Direct SQL or custom HTTP | `shared/supabase_provider.get_client()` | Existing pattern, handles auth, connection pooling |
| Index verification | Manual file checks | Open index + test search | Only Tantivy itself can verify index integrity |
| Result deduplication | New dedup system | Extend existing `_deduplicate` method | Already handles V0.8/V0.7 priority, just add PGP/correction awareness |
| Nikud stripping | Custom regex | Existing `strip_nikud()` function | Already exists in genizah_core.py, handles full Unicode range |

**Key insight:** The Tantivy query composition API (`Query.boolean_query`, `Query.term_query`) is the right way to implement search filters. Post-filtering in Python would require loading all documents, defeating the purpose of an index.

## Common Pitfalls

### Pitfall 1: Schema Mismatch After Adding Fields
**What goes wrong:** Adding `content_type` to the schema but not providing it for every document causes indexing errors or search failures.
**Why it happens:** Every document in a Tantivy index must conform to the schema. Missing fields cause silent failures or errors.
**How to avoid:** Add `content_type="htr"` to ALL existing HTR document insertions (both in the V0.8/V0.7 per-page documents AND in continuous/system/part aggregated documents).
**Warning signs:** Index rebuild completes but search returns no results; `parse_query` throws field-not-found errors.

### Pitfall 2: Deduplication Breaking with New Source Types
**What goes wrong:** The existing `_deduplicate` method (line 4626) only handles V0.8 and V0.7 sources. PGP and correction results get dropped or create duplicates.
**Why it happens:** `_deduplicate` uses `r['display']['source'] == "V0.8"` and only preserves V0.7 results if not in V0.8. PGP results have `source="PGP"` and corrections have `source="correction"`.
**How to avoid:** Extend `_deduplicate` to handle all source types. PGP results should be shown alongside HTR results for the same manuscript, not deduplicated against them. They represent different content, not different versions of the same content.
**Warning signs:** Searching for text that appears in both HTR and PGP transcriptions shows only one result per manuscript instead of both.

### Pitfall 3: Tokenizer Mismatch Between HTR and PGP Content
**What goes wrong:** HTR content uses `tokenizer_name="whitespace"` for the `content` field, which tokenizes on whitespace only. PGP transcriptions may contain punctuation, line numbers, or section markers that affect tokenization differently.
**Why it happens:** The existing main index uses whitespace tokenizer for content. PGP transcriptions have different formatting (section headers like "Recto", line numbers, scholarly annotations).
**How to avoid:** Pre-process PGP transcription text before indexing: strip section headers (Recto/Verso markers), remove line numbers, strip nikud, and normalize whitespace. This ensures consistent tokenization.
**Warning signs:** Searching for a word that appears in PGP transcriptions returns no results, even though the text is indexed.

### Pitfall 4: Index Rebuild Requires Supabase Connection
**What goes wrong:** Desktop users who have no internet connection cannot rebuild their index because PGP fetching fails.
**Why it happens:** The index builder now depends on Supabase, which requires network access.
**How to avoid:** Make PGP/correction fetching graceful: if Supabase is unreachable, log a warning and continue with HTR-only indexing. The index should still be usable without PGP content.
**Warning signs:** Index rebuild hangs or fails for offline users.

### Pitfall 5: Continuous Documents (scope="system"/"part") Not Getting content_type
**What goes wrong:** The `create_index` method builds continuous documents per system ID and per codicological part. These aggregated documents call `_add_continuous_document`, which doesn't include `content_type`.
**Why it happens:** Only the per-page document insertion was updated, but aggregated documents are built from collected page data.
**How to avoid:** Add `content_type` to `_add_continuous_document` and `_add_chunked_continuous_documents`. HTR continuous docs get `content_type="htr"`. PGP documents are page-scope only (no continuous aggregation needed).
**Warning signs:** Composition search (which uses continuous documents) breaks or shows no results after index rebuild.

### Pitfall 6: Large Supabase Fetch Exceeding Memory
**What goes wrong:** Fetching all 9,364 transcriptions at once may exceed memory or hit Supabase row limits.
**Why it happens:** Supabase has default row limits (typically 1000) and transcription text can be large.
**How to avoid:** Use paginated fetching with `.range()` in batches of 500-1000. Track progress via callback for UI feedback.
**Warning signs:** Index rebuild hangs at "Fetching PGP transcriptions" step or returns truncated results.

### Pitfall 7: Windows File Locking During Swap
**What goes wrong:** On Windows, renaming/deleting directories that have open file handles fails with PermissionError.
**Why it happens:** The `SearchEngine` may still have the index open when the swap occurs.
**How to avoid:** Close the existing index (`self.index = None`, `self.searcher = None`) and force garbage collection before the swap. Wait briefly (0.5s) for file handles to release. The lab engine's `_close_index` pattern demonstrates this.
**Warning signs:** Index rebuild succeeds but swap fails with "Access is denied" on Windows.

## Code Examples

### Example 1: Extended Schema for Main Index
```python
# Source: genizah_core.py Indexer.create_index (line 3911+)
builder = tantivy.SchemaBuilder()
builder.add_text_field("unique_id", stored=True)
builder.add_text_field("content", stored=True, tokenizer_name="whitespace")
builder.add_text_field("source", stored=True)
builder.add_text_field("full_header", stored=True)
builder.add_text_field("shelfmark", stored=True)
builder.add_text_field("scope", stored=True)
builder.add_text_field("boundaries", stored=True)
builder.add_text_field("content_type", stored=True)  # NEW: "htr", "pgp", "correction"
schema = builder.build()
```

### Example 2: Filtering Search Query with content_type
```python
# Source: tantivy-py 0.25.1 API (verified via source code inspection)
from tantivy import Query, Occur

def execute_search_with_filter(self, query_str, mode, gap, content_filter="all",
                                progress_callback=None, exclude_words=None):
    """Extended execute_search with content_type filtering."""

    # Build the original text query (existing logic)
    terms = query_str.split() if mode != 'Regex' else [query_str]
    t_query_str = self.build_tantivy_query(terms, mode)

    # Parse text query against content field
    text_query = self.index.parse_query(t_query_str, ["content"])

    # Apply content_type filter
    schema = self.index.schema
    if content_filter == "transcriptions_only":
        # Show only PGP + corrections (exclude HTR)
        query = Query.boolean_query([
            (Occur.Must, text_query),
            (Occur.MustNot, Query.term_query(schema, "content_type", "htr"))
        ])
    elif content_filter == "exclude_transcriptions":
        # Show only HTR (exclude PGP and corrections)
        query = Query.boolean_query([
            (Occur.Must, text_query),
            (Occur.Must, Query.term_query(schema, "content_type", "htr"))
        ])
    else:
        # "all" - no filter
        query = text_query

    # Execute search with the filtered query
    res_obj = self.searcher.search(query, Config.SEARCH_LIMIT)
    # ... rest of result processing
```

### Example 3: Fetching PGP Transcriptions for Indexing
```python
# Source: shared/document_service.py patterns + supabase-py API
from shared.supabase_provider import get_client

def fetch_pgp_transcriptions(progress_callback=None):
    """Fetch all PGP documents with non-null transcriptions."""
    client = get_client()
    all_docs = []
    PAGE_SIZE = 500
    offset = 0

    while True:
        try:
            response = client.table('documents').select(
                'pgpid, transcription, shelfmark_combined'
            ).not_.is_('transcription', 'null').neq(
                'transcription', ''
            ).range(offset, offset + PAGE_SIZE - 1).execute()
        except Exception as e:
            LOGGER.warning("Failed to fetch PGP transcriptions at offset %d: %s", offset, e)
            break

        if not response.data:
            break

        all_docs.extend(response.data)
        if progress_callback:
            progress_callback(f"Fetched {len(all_docs)} PGP transcriptions...")

        offset += PAGE_SIZE
        if len(response.data) < PAGE_SIZE:
            break

    return all_docs

def fetch_approved_corrections():
    """Fetch all approved user corrections."""
    client = get_client()
    try:
        response = client.table('corrections').select(
            'id, sys_id, shelfmark, corrected_text, page_number'
        ).eq('status', 'approved').execute()
        return response.data or []
    except Exception as e:
        LOGGER.warning("Failed to fetch approved corrections: %s", e)
        return []
```

### Example 4: PGP-to-sys_id Mapping for Search Results
```python
# During indexing: build a pgpid -> sys_id mapping from document_fragments
def fetch_pgp_fragment_mapping():
    """Build mapping from PGP document IDs to GenizahSearch sys_ids."""
    client = get_client()
    mapping = {}  # pgpid -> [sys_id, ...]

    PAGE_SIZE = 1000
    offset = 0
    while True:
        response = client.table('document_fragments').select(
            'document_id, sys_id, shelfmark'
        ).range(offset, offset + PAGE_SIZE - 1).execute()

        if not response.data:
            break

        for frag in response.data:
            pgpid = frag.get('document_id')
            if pgpid not in mapping:
                mapping[pgpid] = []
            mapping[pgpid].append({
                'sys_id': frag.get('sys_id'),
                'shelfmark': frag.get('shelfmark')
            })

        offset += PAGE_SIZE
        if len(response.data) < PAGE_SIZE:
            break

    return mapping
```

### Example 5: Web Search Filter UI
```python
# In web/pages/search.py, add to the search options panel
# Source: existing mode_select pattern in search.py

# Content filter dropdown (new)
with ui.column().classes('gap-1'):
    h3(tr('Content'), classes='text-sm font-medium',
       style='color: var(--text-secondary);')
    content_filter = ui.select({
        'all': tr('All Content'),
        'transcriptions_only': tr('Transcriptions Only'),
        'exclude_transcriptions': tr('Exclude Transcriptions'),
    }, value='all').classes('w-48').props('outlined dense')
```

### Example 6: Desktop Search Filter UI
```python
# In genizah_app.py create_search_tab(), add to row2
# Source: existing mode_combo pattern in genizah_app.py (line 6120)

self.content_filter_combo = QComboBox()
self.content_filter_combo.addItems([
    tr("All Content"),
    tr("Transcriptions Only"),
    tr("Exclude Transcriptions")
])
self.content_filter_combo.setToolTip(
    tr("Filter by content type:\n"
       "All = HTR + PGP transcriptions\n"
       "Transcriptions Only = PGP + user corrections\n"
       "Exclude = HTR content only")
)
row2.addWidget(QLabel(tr("Content:")))
row2.addWidget(self.content_filter_combo)
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Destructive `shutil.rmtree` index rebuild | Temp-then-swap pattern | Phase 13 | Index remains usable during rebuild; automatic rollback on failure |
| HTR-only search index | HTR + PGP + corrections unified index | Phase 13 | Searching finds text across all transcription sources |
| No content-type distinction | `content_type` field with filtering | Phase 13 | Users can scope search to specific content types |

**Deprecated/outdated:**
- `source` field values "V0.8" and "V0.7" remain for HTR content. New values "PGP" and "correction" are added for new content types. The `content_type` field provides a cleaner categorization.

## Open Questions

1. **PGP Transcription Text Preprocessing**
   - What we know: PGP transcriptions contain Recto/Verso section markers, potentially line numbers, and scholarly annotations. The `parse_transcription_sections` function in document_service.py already handles this for display.
   - What's unclear: Should we strip these markers before indexing, or keep them? Stripping means cleaner search tokens; keeping means users can search for section-specific content.
   - Recommendation: Strip Recto/Verso markers and line numbers before indexing. The section structure is metadata, not searchable content. Use `strip_nikud` to normalize Hebrew text consistently with HTR content.

2. **Corrections: Full Text or Corrected Segments Only?**
   - What we know: User corrections store `corrected_text` which is the user's version of a specific page's transcription. The `original_text` field stores the original for diff display.
   - What's unclear: Should we index only `corrected_text`, or should we index a merged version of the original + correction?
   - Recommendation: Index only `corrected_text` from approved corrections. This represents the human-verified version. The original text is already indexed from HTR sources.

3. **Multi-Fragment PGP Documents: One Document or Many?**
   - What we know: 74 PGP documents link to multiple fragments (max 4). Each fragment has its own sys_id. The index currently creates per-page documents using the unique_id from the HTR file header.
   - What's unclear: Should multi-fragment PGP documents be indexed as one document or split per fragment?
   - Recommendation: Index as one document per PGP document (using `pgpid` as identifier). The full transcription text is the unit of scholarship. For search results, use the first linked fragment's sys_id for display metadata.

4. **Desktop Index Rebuild Trigger**
   - What we know: Desktop users rebuild the index via the "Build Index" button. The web app may also need index rebuild capability.
   - What's unclear: Should index rebuild automatically fetch PGP data on every rebuild, or should it be optional?
   - Recommendation: Make PGP fetching automatic during rebuild if Supabase is available, with graceful fallback to HTR-only if offline. Add a checkbox or option to skip PGP indexing for users with poor connectivity.

5. **Query.term_query Schema Parameter**
   - What we know: tantivy-py 0.25.1 `Query.term_query` takes `(schema, field_name, field_value, index_option)`. We need to pass the index's schema object.
   - What's unclear: Whether the schema can be obtained from `self.index.schema` at query time.
   - Recommendation: Store `self.schema = self.index.schema` during `reload_index()` for use in query composition. Verify this works in implementation.

## Sources

### Primary (HIGH confidence)
- **genizah_core.py** (lines 3891-4067): `Indexer.create_index()` -- current main index schema and build logic
- **genizah_core.py** (lines 4139-4631): `SearchEngine` -- current search, query building, deduplication
- **genizah_core.py** (lines 489-675): `LabEngine` -- reference for temp-swap pattern (avoid modifying)
- **shared/document_service.py** (lines 117-147, 279-310, 426-453): PGP data access functions
- **supabase_corrections_client.py** (lines 760-848): Corrections table schema and queries
- **tantivy-py 0.25.1** (installed): Verified via `pip show tantivy` -- version 0.25.1
- **tantivy-py source code** (query.rs): All Query methods verified: boolean_query, term_query, etc.

### Secondary (MEDIUM confidence)
- **tantivy-py readthedocs** (tutorials): `parse_query` syntax with field names, `Query.boolean_query` composition
- **tantivy-py GitHub** (schemabuilder.rs): All field types verified: add_text_field parameters (stored, indexed, fast, tokenizer_name)
- **MEMORY.md**: Prior decisions on boolean field limitation, temp-swap pattern, QThread patterns

### Tertiary (LOW confidence)
- None. All findings verified from codebase and official sources.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all libraries already in project, versions verified
- Architecture: HIGH -- two-index architecture fully understood from codebase, tantivy-py Query API verified from source code
- Pitfalls: HIGH -- based on direct code analysis of existing patterns and known constraints from MEMORY.md
- Open Questions: MEDIUM -- some implementation details need validation during coding

**Research date:** 2026-02-08
**Valid until:** 2026-03-08 (30 days -- stable domain, no expected changes)
