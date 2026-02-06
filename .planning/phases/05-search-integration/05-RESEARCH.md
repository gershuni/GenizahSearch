# Phase 5: Search Integration - Research

**Researched:** 2026-02-06
**Domain:** Search result enhancement with transcription availability indicators
**Confidence:** HIGH

## Summary

This research investigates how to efficiently indicate PGP transcription availability in search results. The core challenge is avoiding N+1 queries when displaying 200+ search results while keeping the UI responsive.

The standard approach is a **batch lookup pattern**: collect all sys_ids from search results, query `document_fragments` table once with `.in_()` filter, and return a set of sys_ids that have transcriptions. The UI then checks set membership (O(1)) per result.

The existing codebase already uses this pattern for profile lookups in `get_corrections()` and `get_feed_items()` (lines 636-649 and 1162-1182 in `supabase_client.py`), making implementation straightforward.

**Primary recommendation:** Add a batch lookup function `get_sys_ids_with_transcriptions(sys_ids: List[str]) -> Set[str]` to `document_service.py`, call it once after search completes, and pass the set to `render_results()` for icon display.

## Standard Stack

The implementation uses existing project infrastructure with no new dependencies.

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| supabase-py | existing | Database queries | Already used throughout project |
| NiceGUI | existing | UI components (badges, icons) | Project's UI framework |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| ui.icon | NiceGUI | Material icons | Transcription indicator |
| ui.badge | NiceGUI | Count/status badges | Optional alternative to icon |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Batch lookup | Per-result query | N+1 problem - 200x slower |
| Server-side batch | Client-side cache | Client cache adds complexity, not needed |
| Precomputed column | Dynamic lookup | Schema change, stale data risk |

**Installation:**
No additional packages required.

## Architecture Patterns

### Recommended Approach: Batch Lookup Pattern

```
1. Search executes, returns results[]
2. Extract sys_ids from results
3. Single query: SELECT DISTINCT sys_id FROM document_fragments WHERE sys_id IN (...)
4. Build Set[str] of sys_ids with transcriptions
5. Pass set to render_results()
6. Per-card: if sys_id in set, show indicator
```

### Pattern 1: Batch Lookup Function
**What:** Single function that returns all sys_ids with transcriptions from a list
**When to use:** After search completes, before rendering
**Example:**
```python
# Source: Pattern from existing supabase_client.py lines 636-649
def get_sys_ids_with_transcriptions(sys_ids: List[str]) -> Set[str]:
    """
    Batch check which sys_ids have PGP transcriptions.

    Args:
        sys_ids: List of system IDs to check

    Returns:
        Set of sys_ids that have linked PGP documents with transcriptions
    """
    if not sys_ids:
        return set()

    try:
        client = get_client()
        # Query document_fragments for matching sys_ids
        response = client.table('document_fragments').select(
            'sys_id'
        ).in_('sys_id', sys_ids).execute()

        return {row['sys_id'] for row in (response.data or [])}
    except Exception as e:
        print(f"Error batch checking transcriptions: {e}")
        return set()
```

### Pattern 2: UI Indicator in Result Card
**What:** Conditional icon/badge based on set membership
**When to use:** In `create_result_card()` function
**Example:**
```python
# Source: Adapted from existing library_code badge pattern in search.py lines 1181-1186
# Inside create_result_card(), after shelfmark label:
sys_id = display.get('id')
if sys_id and sys_id in transcription_sys_ids:
    ui.icon('description').classes('text-sm').style(
        'color: var(--success-600);'
    ).tooltip(tr('Has PGP Transcription'))
```

### Pattern 3: Integration Point in Search Flow
**What:** Call batch lookup after search, before render
**When to use:** In `execute_search()` after results are collected
**Example:**
```python
# After search completes, before render_results():
sys_ids = [r.get('display', {}).get('id') for r in results if r.get('display', {}).get('id')]
transcription_sys_ids = await run.io_bound(
    get_sys_ids_with_transcriptions, sys_ids
)
# Pass to render
render_results(results, transcription_sys_ids)
```

### Anti-Patterns to Avoid
- **Per-result database query:** Never call `get_document_for_fragment()` inside `create_result_card()` - this creates N+1 queries
- **Blocking batch call:** Always use `run.io_bound()` to keep UI responsive
- **Storing result in state:** The transcription set is search-result-specific, not page state

## Don't Hand-Roll

Problems that look simple but have existing solutions:

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Batch query | Custom SQL | supabase-py `.in_()` | Already tested, handles escaping |
| Icon styling | Custom CSS | NiceGUI icon classes | Consistent with existing UI |
| Async execution | Manual threading | `run.io_bound()` | NiceGUI's standard pattern |

**Key insight:** The codebase already has all required patterns - this is assembly, not invention.

## Common Pitfalls

### Pitfall 1: N+1 Query Problem
**What goes wrong:** Querying database per search result causes 200+ queries
**Why it happens:** Tempting to reuse `get_document_for_fragment()` which works for single items
**How to avoid:** Always use batch lookup for lists
**Warning signs:** Search becomes slow after adding transcription indicator

### Pitfall 2: Blocking UI Thread
**What goes wrong:** UI freezes during batch lookup
**Why it happens:** Supabase queries are synchronous by default
**How to avoid:** Wrap in `run.io_bound()` for async execution
**Warning signs:** Search results take longer to appear after feature added

### Pitfall 3: Stale Set After Page Navigation
**What goes wrong:** Transcription indicators don't update when user changes search
**Why it happens:** Caching the set at wrong scope
**How to avoid:** Compute fresh set per search execution, not globally
**Warning signs:** Wrong indicators after new search

### Pitfall 4: Empty List Query
**What goes wrong:** `.in_('sys_id', [])` may fail or return unexpected results
**Why it happens:** Edge case not handled
**How to avoid:** Guard clause: `if not sys_ids: return set()`
**Warning signs:** Errors when search returns zero results

## Code Examples

Verified patterns from project codebase:

### Existing Batch Lookup Pattern (from supabase_client.py)
```python
# Source: supabase_client.py lines 636-649
# Fetch profile data for authors
if corrections:
    user_ids = set(c.get('author_id') for c in corrections if c.get('author_id'))
    if user_ids:
        profiles_response = client.table('profiles').select(
            'id, full_name, username'
        ).in_('id', list(user_ids)).execute()
        profiles_map = {p['id']: p for p in (profiles_response.data or [])}
```

### Existing Icon with Tooltip Pattern (from search.py)
```python
# Source: search.py lines 1181-1186
if library_code:
    from genizah_core import get_library_display, LIBRARY_CODES
    full_name = get_library_display(library_code, short=False)
    ui.label(library_code).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
        'background: var(--primary-100); color: var(--primary-700);'
    ).tooltip(full_name)
```

### Existing Badge Pattern (alternative)
```python
# Source: NiceGUI documentation
ui.badge('PGP').props('outline color=green').classes('text-xs')
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Per-item DB query | Batch `.in_()` query | Always best practice | 50-200x faster |
| Sync DB calls | `run.io_bound()` | NiceGUI standard | Non-blocking UI |

**Deprecated/outdated:**
- N/A - This is a new feature

## Open Questions

Things that couldn't be fully resolved:

1. **Icon vs Badge vs Text?**
   - What we know: NiceGUI supports all three, existing code uses icons with tooltips
   - What's unclear: User preference for visual indicator style
   - Recommendation: Start with icon (matches existing patterns), adjust based on UAT feedback

2. **Show count of sources?**
   - What we know: Some documents have multiple transcription sources (24.2%)
   - What's unclear: Whether users want to see "3 transcriptions" vs just "has transcription"
   - Recommendation: Start simple (boolean indicator), enhance later if requested

## Sources

### Primary (HIGH confidence)
- supabase_client.py (lines 636-649, 1162-1182) - Existing batch lookup patterns
- search.py (lines 1146-1218) - Existing result card rendering
- document_service.py - Existing document lookup functions

### Secondary (MEDIUM confidence)
- [Supabase Python API Reference](https://supabase.com/docs/reference/python/using-filters) - Filter documentation
- [NiceGUI Badge Documentation](https://nicegui.io/documentation/badge) - UI component reference

### Tertiary (LOW confidence)
- N/A - All patterns verified in existing codebase

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - Using only existing project dependencies
- Architecture: HIGH - Pattern already used in codebase (supabase_client.py)
- Pitfalls: HIGH - Common patterns well documented
- UI: MEDIUM - Icon choice is a design decision, may need UAT adjustment

**Research date:** 2026-02-06
**Valid until:** 60 days (stable patterns, no external dependencies)
