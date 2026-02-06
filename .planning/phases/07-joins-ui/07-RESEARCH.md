# Phase 7: Joins UI - Research

**Researched:** 2026-02-06
**Domain:** NiceGUI UI component, Supabase data unification (PGP document_fragments + user fragment_joins)
**Confidence:** HIGH

## Summary

This research investigates how to display fragment relationships ("joins") on the browse page, unifying two data sources: (1) PGP multi-fragment document links stored in `document_fragments` (7,764 links across 492 multi-fragment documents, imported in Phase 2), and (2) user-created pairwise joins stored in `fragment_joins` (user-contributed via the existing joins panel). The browse page already has a joins button that opens a dialog showing user-created joins only; this phase extends it to also surface PGP joins.

The existing `joins_panel.py` component already provides a full-featured dialog with fragment listing, navigation, relationship type display, join creation, and admin deletion. It currently only queries the `fragment_joins` table. The core work is to **modify `fetch_connected_fragments()` to also query `document_fragments`** for PGP document-level joins, then merge both sources into a unified display. The `get_fragments_for_document()` service function from Phase 3 already returns all fragments for a PGP document, ordered by sequence. A new section "Related Fragments" could also be added as an inline panel in the metadata sidebar for increased visibility, rather than requiring users to click the small joins button.

The approach is primarily a data-layer merge (adding PGP joins to the existing fetch function) plus a minor UI enhancement (making joins more visible in the metadata panel). No new libraries or tables are needed. The existing component patterns, caching, and navigation callbacks can be reused as-is.

**Primary recommendation:** Extend `fetch_connected_fragments()` in `joins_panel.py` to query `document_fragments` via `get_fragments_for_document()` in addition to `fragment_joins`, merging results with source attribution ("PGP" vs "user"). Add an inline "Related Fragments" section in the metadata panel for fragments with multi-fragment PGP documents.

## Standard Stack

No new libraries needed. This phase uses existing project infrastructure:

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| NiceGUI | (current) | UI framework | Already used for all web pages |
| supabase-py | (current) | Database access | Already used for all Supabase queries |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `web.document_service` | (project) | PGP document-fragment queries | `get_fragments_for_document()`, `get_document_for_fragment()` |
| `web.supabase_client` | (project) | User joins CRUD | `get_fragment_joins()`, `create_fragment_join()` |
| `web.components.joins_panel` | (project) | Existing joins UI component | Extend, don't rewrite |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Extending joins_panel.py | New "related fragments" component | Would duplicate navigation, caching, join creation; extending is simpler |
| Inline metadata panel display | Keep dialog-only | Less discoverable; users must click small button to see joins |

**No installation needed.**

## Architecture Patterns

### Data Flow: Two Sources, Unified Display

```
Browse Page loads fragment (sys_id)
    |
    v
fetch_connected_fragments(shelfmark, document_id)
    |
    +---> Query 1: fragment_joins table (existing user joins)
    |         Returns: pairwise joins with shelfmarks, join_type, source='user'
    |
    +---> Query 2: document_fragments via get_document_for_fragment()
    |         If PGP document found:
    |           get_fragments_for_document(pgpid)
    |           Returns: all fragments in same PGP document, source='PGP'
    |
    v
Merge results (deduplicate by shelfmark, preserve source attribution)
    |
    v
Display in joins panel dialog + metadata panel inline section
```

### Key Data Structures

**fragment_joins table** (user-created pairwise joins):
```
id | user_id | fragment_a_sys_id | fragment_a_shelfmark | fragment_b_sys_id | fragment_b_shelfmark | join_type | confidence | notes | evidence | created_at
```
- Pairwise: each row connects exactly 2 fragments
- User-created; requires authentication to create
- `join_type`: 'physical_join', 'same_composition', 'uncertain'
- Uses BFS through connected components to find full cluster

**document_fragments table** (PGP-imported multi-fragment documents):
```
id | document_id (pgpid) | sys_id | shelfmark | page_info | sequence_order | created_at
```
- Group-based: all fragments of a PGP document share the same `document_id`
- System-imported; read-only for users
- `page_info`: 'recto', 'verso', or NULL
- `sequence_order`: ordering within the document
- 7,764 total links, 492 multi-fragment documents (rest are single-fragment)

### Pattern 1: Extending fetch_connected_fragments()
**What:** Add a second data source query to the existing function
**When to use:** Every time joins are loaded for any fragment

```python
# Source: web/components/joins_panel.py (to be modified)
def fetch_connected_fragments(shelfmark: str = None, document_id: str = None, force_refresh: bool = False) -> Dict:
    # ... existing cache logic ...

    # Source 1: User-created joins (existing)
    user_joins = get_fragment_joins(fragment_sys_id=document_id)

    # Source 2: PGP document joins (NEW)
    pgp_fragments = []
    pgp_doc = None
    if document_id:
        from web.document_service import get_document_for_fragment, get_fragments_for_document
        pgp_doc = get_document_for_fragment(document_id)
        if pgp_doc and pgp_doc.get('pgpid'):
            pgp_fragments = get_fragments_for_document(pgp_doc['pgpid'])

    # Merge: user joins + PGP fragments
    # ... build unified result ...
```

### Pattern 2: Inline Metadata Panel Section
**What:** Show "Related Fragments" section below PGP metadata when joins exist
**When to use:** When the current fragment has PGP document joins OR user joins

```python
# Source: web/pages/browse.py metadata panel area (after PGP metadata section)
# === Related Fragments Section ===
if related_fragments:  # pre-fetched during load_page
    ui.separator().classes('my-3')
    with ui.row().classes('items-center gap-2 mb-2'):
        h3(tr('Related Fragments'), classes='text-xs font-bold', style='color: var(--text-secondary);')
        ui.badge(str(len(related_fragments))).props('color=green')

    for frag in related_fragments:
        # Clickable fragment card with shelfmark, source badge, navigation
        with ui.row().classes('items-center gap-2 cursor-pointer hover:bg-gray-50 p-1 rounded'):
            ui.icon('description').classes('text-gray-500')
            ui.label(frag['shelfmark']).classes('text-sm font-medium')
            if frag['source'] == 'PGP':
                ui.badge('PGP').props('color=blue outline dense').classes('text-xs')
```

### Pattern 3: Source Attribution in Dialog
**What:** Show where each join came from (PGP vs user-created)
**When to use:** In the joins dialog fragment list

The existing dialog already shows a source badge for non-user sources (line 362-363 of joins_panel.py):
```python
if source and source != 'user':
    ui.badge(source).props('color=blue outline dense').classes('text-xs')
```
PGP joins should use `source='PGP'` so they automatically get the blue badge.

### Anti-Patterns to Avoid
- **Creating a separate "PGP joins" panel:** Unify both sources in one display. Users don't care where the join came from; they care about related fragments.
- **Querying document_fragments on every page load without caching:** Use the existing cache in `fetch_connected_fragments()`. The 30-second TTL is appropriate.
- **Allowing users to delete PGP joins:** These are system-imported data. The admin delete button should only appear for user-created joins (source='user'). The existing code already checks `direct_join_id` which would be None for PGP joins.
- **Treating single-fragment PGP documents as joins:** Only show "Related Fragments" when `get_fragments_for_document()` returns more than 1 fragment with different sys_ids.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Fragment navigation | Custom URL building | Existing `navigate_to_shelfmark()` callback + `search_shelfmark()` | Already handles all navigation edge cases |
| Join caching | New cache system | Existing `_joins_cache` with TTL in joins_panel.py | Already thread-safe with lock |
| Shelfmark display | Custom lookup | `state.meta_mgr.get_meta_for_id()` | Already handles all library formats |
| Source attribution display | Custom badge logic | Existing source badge pattern in joins dialog | Already styled and positioned |

**Key insight:** The existing joins_panel.py component handles 90% of the UI requirements. The work is data-layer integration, not UI building.

## Common Pitfalls

### Pitfall 1: Double-Counting Fragments
**What goes wrong:** A fragment appears twice - once from a user join and once from PGP document_fragments
**Why it happens:** The same physical join may exist in both `fragment_joins` (user-created) and `document_fragments` (PGP-imported). For example, a user may have manually joined T-S 8J5.11 and T-S 8J5.12, and these same fragments also appear together in a PGP document.
**How to avoid:** Deduplicate by normalized shelfmark when merging sources. Use a set for tracking unique fragments, keeping the first source encountered (prefer PGP as canonical).
**Warning signs:** Fragment count shows more fragments than actually exist in the cluster.

### Pitfall 2: Single-Fragment PGP Documents Showing as "Joined"
**What goes wrong:** Every PGP fragment shows a "Related Fragments" panel, even single-fragment manuscripts
**Why it happens:** Every PGP document has at least one entry in document_fragments (7,764 total links for 7,090 documents). Single-fragment documents have exactly 1 row.
**How to avoid:** Only show PGP joins when `get_fragments_for_document()` returns fragments with **more than 1 unique sys_id**. Count distinct sys_ids, not rows.
**Warning signs:** Nearly every fragment with PGP data shows "1 Related Fragment" (itself).

### Pitfall 3: Performance Regression on Browse Page Load
**What goes wrong:** Adding a second Supabase query per page load slows down the browse page
**Why it happens:** The fetch is already called via `ui.timer(0.1, ...)` asynchronously, but adding document_fragments query doubles the API calls.
**How to avoid:** Leverage the existing cache (30-second TTL). Also, the pgp_doc is already fetched during `load_page()` for metadata/transcription - store the pgpid in state and pass it to the joins function to avoid a redundant `get_document_for_fragment()` call.
**Warning signs:** Browse page takes noticeably longer to load; spinner visible for extended time on joins button.

### Pitfall 4: Same sys_id Different page_info Not Handled
**What goes wrong:** A fragment appears in multiple PGP documents (e.g., one for recto, one for verso) and shows as having joins from each
**Why it happens:** Some fragments have multiple entries in document_fragments with different document_ids (different PGP documents for recto vs verso)
**How to avoid:** When querying PGP joins, get ALL document_ids for the current sys_id first, then get fragments for each. Deduplicate across documents.
**Warning signs:** Fragment shows joins to itself or shows the same related fragment multiple times.

### Pitfall 5: Breaking Existing User Joins
**What goes wrong:** Modifying fetch_connected_fragments() changes the return format, breaking the existing dialog display
**Why it happens:** The dialog code expects specific keys in the result dict
**How to avoid:** Keep the existing return format exactly: `{fragments, joins, total_fragments, total_joins}`. Add PGP fragments to the same `fragments` list and PGP joins to the same `joins` list with appropriate source attribution.
**Warning signs:** Existing user joins no longer display correctly after the change.

## Code Examples

### Current: How fetch_connected_fragments() Works (joins_panel.py lines 27-95)
```python
# Source: web/components/joins_panel.py
def fetch_connected_fragments(shelfmark, document_id, force_refresh=False):
    # 1. Check cache
    # 2. Query fragment_joins table for user joins
    joins = get_fragment_joins(fragment_sys_id=document_id)
    # 3. Build fragments set and formatted joins
    # 4. Return {fragments, joins, total_fragments, total_joins}
```

### Current: How PGP Document is Loaded in Browse (browse.py lines 905-940)
```python
# Source: web/pages/browse.py
pgp_doc = get_document_for_fragment(page.sys_id, page.p_num)
if pgp_doc:
    state.pgp_metadata = {
        'pgpid': pgp_doc.get('pgpid'),
        # ... other metadata fields
    }
```
The pgpid is already available in `state.pgp_metadata['pgpid']` during page load. This can be passed to the joins function to avoid a redundant query.

### Current: How get_fragments_for_document() Works (document_service.py lines 88-114)
```python
# Source: web/document_service.py
def get_fragments_for_document(pgpid: int) -> List[Dict]:
    response = client.table('document_fragments').select('*').eq(
        'document_id', pgpid
    ).order('sequence_order', desc=False).execute()
    return response.data or []
    # Returns: [{id, document_id, sys_id, shelfmark, sequence_order, page_info}, ...]
```

### Proposed: Extended fetch_connected_fragments()
```python
# Proposed modification to web/components/joins_panel.py
def fetch_connected_fragments(shelfmark=None, document_id=None, pgpid=None, force_refresh=False):
    # ... existing cache logic (add pgpid to cache key) ...

    # Source 1: User-created joins (unchanged)
    if document_id:
        user_joins = get_fragment_joins(fragment_sys_id=document_id)
    elif shelfmark:
        user_joins = get_fragment_joins()
        user_joins = [j for j in user_joins if
                      j.get('fragment_a_shelfmark', '').upper() == shelfmark.upper() or
                      j.get('fragment_b_shelfmark', '').upper() == shelfmark.upper()]
    else:
        user_joins = []

    # Source 2: PGP document joins (NEW)
    pgp_join_fragments = []
    if pgpid or document_id:
        from web.document_service import get_document_for_fragment, get_fragments_for_document

        resolved_pgpid = pgpid
        if not resolved_pgpid and document_id:
            pgp_doc = get_document_for_fragment(document_id)
            resolved_pgpid = pgp_doc.get('pgpid') if pgp_doc else None

        if resolved_pgpid:
            all_frags = get_fragments_for_document(resolved_pgpid)
            # Only include if multi-fragment (more than 1 unique sys_id)
            unique_sys_ids = set(f['sys_id'] for f in all_frags)
            if len(unique_sys_ids) > 1:
                pgp_join_fragments = all_frags

    # Merge into unified result
    fragments_set = set()
    formatted_joins = []

    # Add user joins (existing logic)
    for j in user_joins:
        frag_a = j.get('fragment_a_shelfmark', '')
        frag_b = j.get('fragment_b_shelfmark', '')
        if frag_a: fragments_set.add(frag_a)
        if frag_b: fragments_set.add(frag_b)
        formatted_joins.append({
            'id': j.get('id'),
            'fragment_a': frag_a,
            'fragment_b': frag_b,
            'relationship_type': j.get('join_type'),
            'source': 'user',
            'notes': j.get('notes', '')
        })

    # Add PGP fragments (NEW)
    if pgp_join_fragments:
        current_shelfmark_upper = (shelfmark or '').upper()
        for frag in pgp_join_fragments:
            frag_shelf = frag.get('shelfmark', '')
            if frag_shelf:
                fragments_set.add(frag_shelf)
            # Create virtual join entries for display
            if frag_shelf.upper() != current_shelfmark_upper:
                formatted_joins.append({
                    'id': None,  # No join ID (not user-created)
                    'fragment_a': shelfmark or '',
                    'fragment_b': frag_shelf,
                    'relationship_type': 'same_composition',
                    'source': 'PGP',
                    'notes': f'PGP Document #{resolved_pgpid}'
                })

    return {
        "fragments": list(fragments_set),
        "joins": formatted_joins,
        "total_fragments": len(fragments_set),
        "total_joins": len(formatted_joins)
    }
```

### Proposed: Inline Metadata Panel Section
```python
# Proposed addition to web/pages/browse.py, after PGP Metadata section (~line 1799)

# === Related Fragments Section ===
# Use pgpid from state.pgp_metadata to check for multi-fragment document
pgpid_for_joins = state.pgp_metadata.get('pgpid') if state.pgp_metadata else None
joins_data = fetch_connected_fragments(
    shelfmark=page.shelfmark,
    document_id=page.sys_id,
    pgpid=pgpid_for_joins
)

if joins_data.get('total_fragments', 1) > 1:
    ui.separator().classes('my-3')
    with ui.row().classes('items-center gap-2 mb-2'):
        h3(tr('Related Fragments'), classes='text-xs font-bold',
           style='color: var(--text-secondary);')
        ui.badge(str(joins_data['total_fragments'])).props('color=green')

    for frag_shelfmark in joins_data['fragments']:
        if frag_shelfmark.upper() == (page.shelfmark or '').upper():
            continue  # Skip current fragment

        # Find source for this fragment
        source = 'user'
        for join in joins_data['joins']:
            if frag_shelfmark in (join.get('fragment_a'), join.get('fragment_b')):
                source = join.get('source', 'user')
                break

        def make_nav(target=frag_shelfmark):
            return lambda: navigate_to_shelfmark(target)

        with ui.row().classes(
            'items-center gap-2 cursor-pointer hover:bg-gray-50 p-1 rounded w-full'
        ).on('click', make_nav()):
            ui.icon('description').classes('text-gray-500')
            ui.label(frag_shelfmark).classes('text-sm font-medium')
            if source == 'PGP':
                ui.badge('PGP').props('color=blue outline dense').classes('text-xs')
            ui.icon('arrow_forward' if not is_rtl() else 'arrow_back').classes(
                'text-gray-400 ml-auto'
            )
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| User-only pairwise joins | PGP multi-fragment documents + user joins | Phase 2 (Feb 2026) | 492 multi-fragment PGP documents available |
| Joins button only (dialog) | Joins button + inline metadata panel | Phase 7 (this phase) | Better discoverability |
| fragment_joins table only | fragment_joins + document_fragments | Phase 7 (this phase) | Unified view of all relationships |

**Current state:**
- `fragment_joins`: User-created pairwise joins (likely small count, community feature)
- `document_fragments`: 7,764 links from PGP import (492 multi-fragment documents)
- Both exist in Supabase but are completely separate with no cross-referencing

## Open Questions

1. **How many user-created joins currently exist in fragment_joins?**
   - What we know: The table exists and the UI works. The desktop app syncs joins.
   - What's unclear: Actual row count. If zero or very small, the merge logic is less critical for correctness but still needed for completeness.
   - Recommendation: Check count during implementation; don't skip the merge even if count is low.

2. **Should the inline panel also show in non-metadata sidebar view?**
   - What we know: The metadata panel is a collapsible sidebar. When collapsed, joins are only visible via the button.
   - What's unclear: Whether non-power-users would discover joins at all if they don't open the metadata panel.
   - Recommendation: Keep the joins button in the toolbar (existing) AND add the inline panel in metadata sidebar. Two entry points.

3. **Should PGP document description be shown with PGP joins?**
   - What we know: PGP multi-fragment documents have descriptions like "Letter fragment, physical join with T-S 8J5.12"
   - What's unclear: Whether the description adds value next to the fragment list
   - Recommendation: Show PGP document type in the Related Fragments header (e.g., "Related Fragments - Letter") but not the full description (it's already shown in PGP Metadata section above).

## Sources

### Primary (HIGH confidence)
- `web/components/joins_panel.py` - Full existing joins component code, 734 lines
- `web/document_service.py` - Document-fragment query service, 508 lines
- `web/pages/browse.py` - Browse page implementation, ~2600 lines
- `web/supabase_client.py` - fragment_joins CRUD functions
- `scripts/import_pgp_documents.py` - PGP import with multi-fragment parsing
- `docs/guides/SUPABASE_GUIDE.md` - Database schema documentation
- `.planning/phases/02-pgp-data-import/02-02-SUMMARY.md` - Import stats (7,764 links, 492 multi-fragment)
- `.planning/REQUIREMENTS.md` - JOIN-01 through JOIN-05 requirements
- `tests/test_document_service.py` - Service layer test patterns

### Secondary (MEDIUM confidence)
- `genizah_core.py` JoinsManager class (desktop app) - Confirms BFS connected components pattern for join clusters
- `corrections_ui.py` JoinsDialog class (desktop app) - Confirms desktop UI patterns for joins
- `supabase_corrections_client.py` - Desktop Supabase joins client

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - All components already exist in codebase, no new libraries
- Architecture: HIGH - Clear data flow from existing code; two well-understood data sources
- Pitfalls: HIGH - Identified from reading actual code and data model; edge cases are concrete
- Code examples: HIGH - Based on actual codebase patterns, not hypothetical

**Research date:** 2026-02-06
**Valid until:** 2026-03-06 (stable - internal project, no external dependencies changing)
