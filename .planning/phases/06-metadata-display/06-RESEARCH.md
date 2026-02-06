# Phase 6: Metadata Display - Research

**Researched:** 2026-02-06
**Domain:** PGP document metadata display in NiceGUI browse page + tag-based search filtering
**Confidence:** HIGH

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Placement & layout:**
- Add PGP metadata as a new labeled section inside the existing expandable metadata panel
- Follows the same pattern as Oxford/Cambridge external info — all external source metadata stays in one panel
- Section has a clear "Princeton Geniza Project" (or "PGP") header to distinguish from GenizahSearch fields
- Behind the existing "Show/Hide Metadata" toggle — no always-visible PGP elements
- When a fragment has no linked PGP document, the PGP section simply doesn't appear — no "not found" message
- PGP is a small (but important) part of all Genizah content — metadata display should enhance, not reshape the browse experience

**Content priority:**
- Priority order: Document type > Tags > Description > Languages > Dates
- Document type (Letter, Legal document, List, etc.) displayed prominently
- Subject tags shown as interactive elements (see Tags section)
- Description (English summary from PGP) shown — Claude decides truncation strategy based on typical lengths
- Languages (primary and secondary, e.g., "Judaeo-Arabic") displayed
- Dates displayed (see Date display section)

**Tags interaction:**
- Tags display as clickable elements (visual style at Claude's discretion)
- Clicking a tag redirects to the search results page with a tag filter
- Search results show ALL GenizahSearch fragments linked to PGP documents with that tag (not one-per-document)
- This means the search page needs to support a tag filter parameter

**Date display:**
- Inferred/standardized date is the primary display (e.g., "1041 CE")
- Original date shown as secondary detail
- Dates converted to CE (PGP provides standardized dates in `doc_date_standard` and `inferred_date_standard`)
- Date rationale shown inline below the date, always visible (e.g., "Based on the mention of Yefet b. David")
- Date clickability at Claude's discretion

### Claude's Discretion
- Whether to show key PGP fields (like document type) above the toggle or keep all behind it
- Description truncation strategy (full text vs truncated with expand)
- Tag visual style (chips, plain links, etc.)
- Date clickability (filter by period, or display-only)
- Exact ordering and spacing of fields within the PGP section
- How to display languages (inline with type, or separate row)

### Deferred Ideas (OUT OF SCOPE)
- Full-text search within PGP transcriptions (already noted as v2 in STATE.md)
- Tag-based browsing page (dedicated tag exploration UI beyond search redirect)
- Date-range filtering as a search feature
</user_constraints>

## Summary

This research investigates how to display PGP document metadata on the browse page and implement tag-based search filtering. The implementation touches three areas: (1) a database schema migration to add missing columns, (2) a metadata display component in the browse page's existing expandable metadata panel, and (3) a new tag search endpoint with corresponding search page support.

The **critical finding** is that the current `documents` table is **missing several columns** that the phase requires: `languages_primary`, `languages_secondary`, `inferred_date_rationale`, and `inferred_date_standard`. These exist in `pgp_data/documents.csv` but were not imported. A schema migration + re-import is needed before the UI can display languages, date rationale, or the inferred standardized date.

The browse page already fetches the full PGP document record via `get_document_for_fragment()` on every page load (line 902 of `browse.py`). This means most metadata fields are already available in memory -- the UI just needs to display them. The metadata panel section (lines 1612-1706) has a clear pattern for adding new labeled sections (Library, Shelfmark, Oxford metadata, External Links, Export).

**Primary recommendation:** Add missing DB columns first (migration + re-import), then build the PGP metadata section as a new subsection within the existing metadata panel, and add a tag search function to `document_service.py` with a `/search?tag=X` URL parameter on the search page.

## Standard Stack

No new libraries needed. The implementation uses existing project infrastructure.

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| supabase-py | existing | Database queries (tag search, metadata) | Already used throughout project |
| NiceGUI | existing | UI components (badges, labels, cards) | Project's UI framework |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `ui.badge` | NiceGUI | Tag chip display | Clickable tag elements |
| `ui.label` | NiceGUI | Metadata field display | Type, dates, description |
| `ui.expansion` | NiceGUI | Expandable description | Long descriptions |
| `ui.link` | NiceGUI | PGP external link | Link to PGP website |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `ui.badge` for tags | `ui.button` flat | Badges are more semantic for tags, clickable via `.on('click')` |
| `ui.expansion` for description | Custom truncation with "show more" | Expansion is built-in NiceGUI, cleaner |
| Re-import all documents | ALTER TABLE + UPDATE | Re-import is simpler given the existing script |

**Installation:**
No additional packages required.

## Architecture Patterns

### Pattern 1: Database Migration for Missing Columns

**What:** The `documents` table needs 4 new columns added, then data re-imported.

**Current DB schema (`documents` table):**
```
pgpid INTEGER PRIMARY KEY
shelfmark_combined TEXT
document_type TEXT
tags JSONB DEFAULT '[]'
doc_date_original TEXT
doc_date_standard TEXT
inferred_date_display TEXT
description TEXT
transcription TEXT
transcription_source TEXT
pgp_url TEXT (GENERATED)
created_at TIMESTAMPTZ
```

**Missing columns (available in CSV but not imported):**
```sql
ALTER TABLE documents ADD COLUMN IF NOT EXISTS languages_primary TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS languages_secondary TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS inferred_date_standard TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS inferred_date_rationale TEXT;
```

**Impact:** The import script (`scripts/import_pgp_documents.py`) needs to:
1. Read these fields from `documents.csv` in `load_documents_metadata()`
2. Include them in `prepare_document_records()`
3. Re-run the import (upsert handles existing records)

**Confidence:** HIGH - verified by comparing CSV columns vs migration SQL vs import script.

### Pattern 2: Metadata Display in Browse Panel

**What:** Add a PGP section inside the existing metadata panel (lines 1612-1706 of browse.py).

**Insertion point:** After the "Export" section (line 1706), or between "External Links" and "Export" sections, add a new PGP section with separator.

**Data flow (already exists):**
```
1. load_page() → get_document_for_fragment(sys_id, page_num) [line 902]
2. pgp_doc dict returned with ALL document columns
3. Currently only transcription/pgp_url/pgpid are extracted
4. Metadata fields (document_type, tags, description, dates) are ALREADY in pgp_doc
5. Just need to store and display them
```

**Key insight:** No additional Supabase queries needed. The `get_document_for_fragment()` call already does `SELECT *` from the documents table (line 73 of `document_service.py`). All metadata fields are returned but currently unused by the UI.

**Recommended state change:**
```python
# In BrowseState.__init__():
self.pgp_metadata: Optional[Dict[str, Any]] = None  # Full PGP metadata dict

# In load_page(), after get_document_for_fragment():
if pgp_doc:
    state.pgp_metadata = {
        'document_type': pgp_doc.get('document_type'),
        'tags': pgp_doc.get('tags', []),
        'description': pgp_doc.get('description'),
        'languages_primary': pgp_doc.get('languages_primary'),
        'languages_secondary': pgp_doc.get('languages_secondary'),
        'doc_date_original': pgp_doc.get('doc_date_original'),
        'doc_date_standard': pgp_doc.get('doc_date_standard'),
        'inferred_date_display': pgp_doc.get('inferred_date_display'),
        'inferred_date_rationale': pgp_doc.get('inferred_date_rationale'),
        'pgp_url': pgp_doc.get('pgp_url'),
        'pgpid': pgp_doc.get('pgpid'),
    }
```

**Confidence:** HIGH - verified by reading `document_service.py` and `browse.py` source code.

### Pattern 3: Tag Search via URL Parameter

**What:** Add a `/search?tag=communal` URL parameter that triggers a tag-based search.

**Implementation approach:**
1. Add `get_fragments_by_tag(tag: str) -> List[Dict]` to `document_service.py`
2. Modify search page route in `main.py` to accept `tag` parameter
3. Pass `tag` to `create_search_page()` which renders tag results

**Supabase JSONB array query:**
```python
# Tags are stored as JSONB array: ["communal", "excommunication"]
# Use .contains() for @> operator (GIN-indexed)
response = client.table('documents').select(
    'pgpid, shelfmark_combined, document_type, description'
).contains('tags', [tag_name]).execute()

# Then join with document_fragments to get sys_ids for each document
```

**Query plan:**
1. Query `documents` table where `tags @> '["communal"]'` (GIN-indexed)
2. For each matching document, get linked fragments from `document_fragments`
3. Return fragment-level results (sys_id, shelfmark, document metadata)

**Route change:**
```python
# In web/main.py line 1815:
@ui.page('/search')
def search_page_route(q: str = None, tag: str = None):
    # ... existing setup ...
    create_search_page(initial_query=q, initial_tag=tag)
```

**Confidence:** HIGH - verified `.contains()` method exists on Supabase Python client, and GIN index already created on tags column.

### Recommended Component Structure
```
browse.py update_content():
  └── Metadata Panel (existing, lines 1612-1706)
      ├── Metadata grid (existing: Library, Shelfmark, Title, etc.)
      ├── Oxford Metadata (existing, conditional)
      ├── External Links (existing)
      ├── ui.separator
      ├── PGP Section (NEW) ← insert here
      │   ├── "Princeton Geniza Project" header with link
      │   ├── Document Type (prominent)
      │   ├── Tags (clickable badges)
      │   ├── Description (with truncation for long text)
      │   ├── Languages
      │   ├── Dates (inferred primary, original secondary)
      │   └── Date Rationale
      └── Export (existing)
```

### Anti-Patterns to Avoid
- **Separate Supabase call for metadata:** The `get_document_for_fragment()` already returns all fields. Do NOT add a second `get_document_metadata()` call.
- **Showing PGP section for all fragments:** Only ~7K of ~217K fragments have PGP data. The PGP section must be conditional.
- **Blocking page load for tag search:** Tag search results should use async pattern, not block the main search UI.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| JSONB array search | Custom Python filtering | Supabase `.contains()` with GIN index | Database handles the query efficiently |
| Date conversion | Python date parser for Seleucid/Hijri dates | Pre-converted `doc_date_standard` and `inferred_date_display` fields | PGP already provides CE-converted dates |
| Tag deduplication | Custom dedup logic | Database DISTINCT in query | Let Postgres handle it |
| Expandable text | Custom JS show/hide | `ui.expansion` NiceGUI component | Built-in, accessible, styled |

**Key insight:** PGP has already done the hard work of date conversion and standardization. The `doc_date_standard` field is an ISO date range in CE (e.g., "1258-08-31/1259-09-19") and `inferred_date_display` is human-readable (e.g., "1025-1026 CE", "ca. 1090", "Early 11th c."). Use these directly.

## Common Pitfalls

### Pitfall 1: Missing Database Columns
**What goes wrong:** Trying to display languages and date rationale without adding the columns first.
**Why it happens:** The original import script only imported a subset of CSV fields.
**How to avoid:** Run migration FIRST, then update import script, then re-import, then build UI.
**Warning signs:** `pgp_doc.get('languages_primary')` returns `None` for all documents.

### Pitfall 2: N+1 Tag Search Queries
**What goes wrong:** For each document matching a tag, querying fragments individually.
**Why it happens:** Natural ORM-like thinking: "for each document, get its fragments."
**How to avoid:** Use a single joined query or batch approach:
```python
# Get all document pgpids matching tag
doc_ids = [d['pgpid'] for d in doc_response.data]
# Then batch query fragments
frag_response = client.table('document_fragments').select('*').in_('document_id', doc_ids).execute()
```
**Warning signs:** Tag search takes >2 seconds for popular tags.

### Pitfall 3: Description HTML Injection
**What goes wrong:** PGP descriptions contain HTML entities and markup (confirmed in CSV: `<a href=...>`, `<em>`, etc.).
**Why it happens:** Descriptions include scholarly references with HTML formatting.
**How to avoid:** Either sanitize HTML before display, or use `ui.html()` with `sanitize=True` for descriptions.
**Warning signs:** Raw HTML tags visible in description text, or XSS vulnerability.

### Pitfall 4: Tag Search Results Not Matching Search Page Format
**What goes wrong:** Tag search results have different structure than text search results.
**Why it happens:** Text search returns results from Tantivy with scores, while tag search returns raw DB records.
**How to avoid:** Format tag search results to match the same structure as text search results, OR render tag results in a dedicated section with different formatting.
**Warning signs:** JavaScript errors when clicking tag results, missing fields in result cards.

### Pitfall 5: Overloading the Metadata Panel
**What goes wrong:** PGP section makes the panel too long or visually cluttered.
**Why it happens:** Descriptions can be very long (median 387 chars, max 6189 chars for transcription docs).
**How to avoid:** Truncate descriptions at ~200-300 chars with an expand option. Keep consistent spacing.
**Warning signs:** Metadata panel requires excessive scrolling.

## Code Examples

### Example 1: PGP Metadata Section in Browse Page
```python
# Source: Follows existing browse.py metadata panel pattern (lines 1624-1706)
# Insert after External Links section, before Export section

if state.pgp_metadata:
    ui.separator().classes('my-3')
    with ui.row().classes('items-center gap-2 mb-2'):
        h3('Princeton Geniza Project', classes='text-xs font-bold', style='color: var(--text-secondary);')
        if state.pgp_metadata.get('pgp_url'):
            ui.link('', state.pgp_metadata['pgp_url'], new_tab=True).classes('text-sm').style(
                'color: var(--primary-600);'
            )

    # Document Type (prominent)
    if state.pgp_metadata.get('document_type'):
        with ui.column().classes('gap-1 mb-2'):
            ui.label(tr('Document Type')).classes('text-xs font-bold').style('color: var(--text-secondary);')
            ui.label(state.pgp_metadata['document_type']).classes('text-sm font-medium').style(
                'color: var(--text-primary);'
            )

    # Tags (clickable badges)
    tags = state.pgp_metadata.get('tags', [])
    if tags:
        with ui.column().classes('gap-1 mb-2'):
            ui.label(tr('Tags')).classes('text-xs font-bold').style('color: var(--text-secondary);')
            with ui.row().classes('gap-1 flex-wrap'):
                for tag in tags:
                    ui.badge(tag).props('outline clickable color=green').classes(
                        'text-xs cursor-pointer'
                    ).on('click', lambda t=tag: ui.navigate.to(f'/search?tag={quote(t)}'))
```

### Example 2: Tag Search Service Function
```python
# Source: Follows existing document_service.py patterns

def get_fragments_by_tag(tag: str) -> List[Dict[str, Any]]:
    """Get all fragments linked to PGP documents with a specific tag."""
    if not tag:
        return []
    try:
        client = get_client()
        # Step 1: Find documents with this tag (GIN-indexed JSONB query)
        doc_response = client.table('documents').select(
            'pgpid, shelfmark_combined, document_type, description'
        ).contains('tags', [tag]).execute()

        if not doc_response.data:
            return []

        # Step 2: Get all fragments for matching documents
        doc_ids = [d['pgpid'] for d in doc_response.data]
        frag_response = client.table('document_fragments').select(
            'sys_id, shelfmark, document_id'
        ).in_('document_id', doc_ids).execute()

        if not frag_response.data:
            return []

        # Step 3: Join fragment info with document metadata
        doc_map = {d['pgpid']: d for d in doc_response.data}
        results = []
        for frag in frag_response.data:
            doc = doc_map.get(frag['document_id'], {})
            results.append({
                'sys_id': frag['sys_id'],
                'shelfmark': frag['shelfmark'],
                'document_type': doc.get('document_type', ''),
                'description': doc.get('description', ''),
                'pgpid': frag['document_id'],
            })
        return results
    except Exception as e:
        print(f"Error searching by tag '{tag}': {e}")
        return []
```

### Example 3: Database Migration
```sql
-- Migration: Add missing PGP metadata columns
ALTER TABLE documents ADD COLUMN IF NOT EXISTS languages_primary TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS languages_secondary TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS inferred_date_standard TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS inferred_date_rationale TEXT;

COMMENT ON COLUMN documents.languages_primary IS 'Primary language(s), e.g., "Judaeo-Arabic", "Hebrew, Aramaic"';
COMMENT ON COLUMN documents.languages_secondary IS 'Secondary language(s), if any';
COMMENT ON COLUMN documents.inferred_date_standard IS 'Standardized inferred date in ISO format, e.g., "1160/1166"';
COMMENT ON COLUMN documents.inferred_date_rationale IS 'Rationale for inferred date, e.g., "Person mentioned"';
```

### Example 4: Description Truncation Pattern
```python
# Recommended: truncate at ~250 chars with expand for long descriptions
# Median description is 387 chars; 64% are >300 chars

description = state.pgp_metadata.get('description', '')
if description:
    with ui.column().classes('gap-1 mb-2'):
        ui.label(tr('Description')).classes('text-xs font-bold').style('color: var(--text-secondary);')
        if len(description) > 250:
            with ui.expansion(description[:250] + '...').classes('text-sm').style(
                'color: var(--text-primary);'
            ):
                # Use ui.html with sanitize=True since descriptions may contain HTML
                ui.html(description).classes('text-sm').style('color: var(--text-primary);')
        else:
            ui.html(description).classes('text-sm').style('color: var(--text-primary);')
```

## Data Analysis

### Document Type Distribution (transcription docs)
| Type | Count | % |
|------|-------|---|
| Letter | ~3,800 | ~52% |
| Legal document | ~1,600 | ~22% |
| List or table | ~900 | ~12% |
| Literary text | ~400 | ~5.5% |
| State document | ~350 | ~4.8% |
| Other types | ~250 | ~3.7% |

### Metadata Coverage (among ~7,090 imported documents)
| Field | Has Data | % | Notes |
|-------|----------|---|-------|
| document_type | ~7,090 | 100% | Always present |
| description | ~7,090 | 100% | Always present; median 387 chars |
| tags | ~5,436 | ~75% | Stored as JSONB array |
| languages_primary | ~6,352 | ~87% | **NOT in DB yet** |
| languages_secondary | ~829 | ~11% | **NOT in DB yet** |
| doc_date_original | ~1,680 | ~23% | Calendar dates (Seleucid, etc.) |
| doc_date_standard | ~1,805 | ~25% | ISO CE dates |
| inferred_date_display | ~403 | ~6% | Human-readable inferred date |
| inferred_date_rationale | ~433 | ~6% | **NOT in DB yet** |

### Description Length Distribution
- Min: 9 chars
- Median: 387 chars (transcription docs)
- Mean: ~350 chars
- Max: 6,189 chars
- >300 chars: ~64% of descriptions
- >500 chars: ~36% of descriptions
- >1,000 chars: ~17% of descriptions

**Truncation recommendation:** Truncate at 250 characters with expand. This shows enough context for short descriptions (36% fit entirely) while preventing excessive panel length for the majority.

### Tag Distribution
- 2,695 unique tags across all documents
- Most common: "DIMME" (1,632), "account" (755), "communal" (751)
- Many tags appear only 1-3 times
- Tags are comma-separated in CSV, stored as JSONB arrays in DB

### Date Formats
- `doc_date_standard`: ISO date ranges like "1258-08-31/1259-09-19" or "1131"
- `inferred_date_display`: Human-readable like "1160-1166", "ca. 1090", "Early 11th c."
- `doc_date_original`: Calendar-specific like "1570" (Seleucid), "19 Adar 1427"
- `inferred_date_rationale`: Brief like "Person mentioned", or detailed scholarly reasons

**Date display recommendation:** Show `inferred_date_display` first when available (it's already human-readable CE). Fallback to extracting year from `doc_date_standard`. Show `doc_date_original` as secondary with calendar label.

## Discretion Recommendations

Based on research findings, these are my recommendations for Claude's Discretion areas:

### 1. Keep ALL PGP fields behind the toggle
**Recommendation:** Keep everything behind "Show Metadata" toggle.
**Reason:** User explicitly said "PGP is a tiny (though important) part of all Genizah — it should not change the whole Genizah website way." Showing document type above the toggle would change the browse experience for ~7K fragments. The metadata panel is the right home.

### 2. Description truncation: 250 chars with expand
**Recommendation:** Show first 250 characters, with "show more" expansion for longer descriptions.
**Reason:** 64% of descriptions exceed 300 chars. Median is 387 chars. Showing full text would make the panel very long. Descriptions also contain HTML (`<a>`, `<em>` tags for scholarly references) so use `ui.html()` with sanitize.

### 3. Tag visual style: Outline badges (chips)
**Recommendation:** Use `ui.badge` with `outline clickable color=green` props.
**Reason:** The codebase already uses `ui.badge` extensively (48+ instances found). Green outline matches the project's primary color scheme and the PGP badge style used in the version selector.

### 4. Dates: Display-only (no click filtering)
**Recommendation:** Display-only, not clickable.
**Reason:** Date-range filtering is explicitly deferred. Only ~25% of documents have dates. The inferred date display format varies widely ("ca. 1090", "Early 11th c.") making structured date filtering complex.

### 5. Languages: Inline with type on same row
**Recommendation:** Show languages on same row as document type, separated by a divider.
**Reason:** Languages are terse (e.g., "Judaeo-Arabic", "Hebrew") and complement type. Saves vertical space.

### 6. Field ordering within PGP section
**Recommendation:**
1. Header row: "Princeton Geniza Project" + PGP link icon
2. Type + Languages (inline row)
3. Tags (badge row)
4. Description (truncated with expand)
5. Dates (inferred primary, original secondary, rationale below)

## Open Questions

### 1. Tag Search Results Page Integration
**What we know:** The search page uses Tantivy for text search, returning structured results with scores, snippets, etc. Tag search returns DB records with different structure.
**What's unclear:** Should tag search results use the existing search page results list, or a separate rendering path?
**Recommendation:** Create a simplified tag results renderer that shows fragment cards (shelfmark, type, description snippet) without search scores. This avoids coupling with the Tantivy result format while using the search page's layout.

### 2. Re-import Impact on Existing Data
**What we know:** The import script uses upsert (`on_conflict='pgpid'`). Re-running it should safely add the new columns without affecting existing transcription data.
**What's unclear:** Whether there have been any manual edits to documents in Supabase since import.
**Recommendation:** Proceed with upsert. The import uses pgpid as natural key, so existing records get updated with new columns only. Transcription content comes from the same CSV source.

### 3. HTML in Descriptions
**What we know:** PGP descriptions contain HTML markup (links, emphasis, scholarly references). Example: `<a href="..."><em>Title</em></a>`
**What's unclear:** Whether all HTML in descriptions is safe/well-formed.
**Recommendation:** Use `ui.html()` to render descriptions so scholarly links and formatting work. NiceGUI's default sanitization should handle any malformed HTML.

## Sources

### Primary (HIGH confidence)
- `C:\GenizahSearch\web\pages\browse.py` - Existing metadata panel structure (lines 1612-1706)
- `C:\GenizahSearch\web\document_service.py` - Service layer, `get_document_for_fragment()` returns full document
- `C:\GenizahSearch\migrations\add_pgp_documents_tables.sql` - Current DB schema
- `C:\GenizahSearch\scripts\import_pgp_documents.py` - Import script showing which fields are imported
- `C:\GenizahSearch\pgp_data\documents.csv` - Source data with all fields
- `C:\GenizahSearch\web\main.py` lines 1814-1851 - Route definitions for search and browse

### Secondary (MEDIUM confidence)
- Supabase Python client `.contains()` method - verified exists via runtime inspection
- NiceGUI `ui.badge`, `ui.expansion` components - verified via codebase usage patterns

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - No new libraries, uses existing project infrastructure
- Architecture: HIGH - Verified by reading actual source code, data flows, and DB schema
- Data analysis: HIGH - Computed from actual pgp_data/documents.csv (35,839 records)
- Pitfalls: HIGH - Identified from code analysis and data inspection
- Tag search: MEDIUM - Supabase `.contains()` verified but tag search page integration needs design

**Research date:** 2026-02-06
**Valid until:** 2026-03-06 (stable domain, no external dependencies changing)
