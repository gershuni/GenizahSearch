# Phase 4: Transcription Display - Research

**Researched:** 2026-02-05
**Domain:** NiceGUI UI integration, version selector extension, PGP transcription display
**Confidence:** HIGH

## Summary

This phase integrates PGP transcriptions into the existing browse page. The work is primarily UI wiring since the service layer (Phase 3) already provides all needed data access functions. The key challenge is extending the existing version selector pattern to include PGP as a new transcription source while maintaining backward compatibility with HTR versions (V0.7, V0.8) and user corrections.

The browse page (`web/pages/browse.py`) already has a well-established pattern for displaying transcription text with version switching. The `create_version_selector` component in `web/components/version_selector.py` provides the UI pattern we need to extend. PGP transcriptions should appear as the primary (top) option in the version menu when available, with proper attribution.

**Primary recommendation:** Extend the existing `version_selector.py` component to check for PGP transcriptions via `document_service.py`, adding PGP as a new version source type that appears above HTR versions in the version menu.

## Standard Stack

The established libraries/tools for this domain:

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| NiceGUI | 1.x | Web UI framework | Already in use, provides all UI components |
| Supabase Python Client | existing | Database access | Service layer already configured |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| web/document_service.py | N/A | PGP data access | All PGP lookups (already built in Phase 3) |
| web/components/version_selector.py | N/A | Version switching UI | Extend for PGP support |
| web/translations.py | N/A | i18n strings | New UI text for PGP attribution |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Extending version_selector | Separate PGP panel | More intrusive change, less consistent UX |
| Menu-based selection | Tabs | Existing UX uses menu pattern, stick with it |

**Installation:**
No new packages required - all dependencies already present.

## Architecture Patterns

### Recommended Project Structure
```
web/
├── components/
│   └── version_selector.py   # MODIFY: Add PGP version support
├── pages/
│   └── browse.py             # MODIFY: Wire up PGP transcription loading
├── document_service.py       # EXISTS: No changes needed
└── translations.py           # MODIFY: Add PGP-related strings
```

### Pattern 1: Service Layer Data Access
**What:** Use document_service.py functions for all PGP data
**When to use:** Any PGP-related lookup in browse.py or version_selector.py
**Example:**
```python
# Source: web/document_service.py (existing)
from web.document_service import get_document_for_fragment

# In browse.py or version_selector.py:
def check_pgp_transcription(sys_id: str) -> Optional[dict]:
    """Check if fragment has PGP transcription available."""
    doc = get_document_for_fragment(sys_id)
    if doc and doc.get('transcription'):
        return {
            'source': 'pgp',
            'content': doc['transcription'],
            'attribution': doc.get('transcription_source', 'PGP'),
            'pgp_url': doc.get('pgp_url'),
            'pgpid': doc.get('pgpid')
        }
    return None
```

### Pattern 2: Version Selector Extension
**What:** Add PGP as a new version source in the existing menu structure
**When to use:** When displaying version options for a fragment
**Example:**
```python
# Extend create_version_selector in version_selector.py
# Add PGP transcription as first option when available

def create_version_selector(
    document_id: str,
    page_number: int,
    original_text: str,
    on_version_change: Optional[Callable[[str, dict], None]] = None,
    size: str = "sm",
    pgp_transcription: Optional[dict] = None  # NEW PARAMETER
):
    """Create a version selector dropdown with PGP support."""
    # ... existing setup code ...

    with menu:
        # PGP transcription (if available) - APPEARS FIRST
        if pgp_transcription:
            def select_pgp():
                version_label.text = f"PGP - {pgp_transcription.get('attribution', 'PGP')}"
                menu.close()
                if on_version_change:
                    on_version_change(pgp_transcription['content'], {
                        'source': 'pgp',
                        'attribution': pgp_transcription.get('attribution'),
                        'pgp_url': pgp_transcription.get('pgp_url'),
                        'is_pgp': True
                    })
            with ui.menu_item(on_click=select_pgp).classes('text-sm'):
                with ui.row().classes('items-center gap-2'):
                    ui.icon('verified', size='xs').classes('text-green-600')
                    ui.label(f"PGP - {pgp_transcription.get('attribution', '')}").classes('font-medium')
            ui.separator()

        # V0.8 Original (existing code)
        # ...
```

### Pattern 3: Attribution Link Display
**What:** Show source attribution with clickable link to PGP
**When to use:** When PGP transcription is selected/displayed
**Example:**
```python
# In browse.py transcription panel header
def render_pgp_attribution(pgp_info: dict):
    """Render PGP attribution with link."""
    with ui.row().classes('items-center gap-2 text-sm'):
        ui.icon('verified', size='xs').classes('text-green-600')
        ui.label(f"Transcription by {pgp_info.get('attribution', 'PGP')}")
        if pgp_info.get('pgp_url'):
            ui.link('View on PGP', pgp_info['pgp_url'], new_tab=True).classes(
                'text-blue-600 hover:underline'
            )
```

### Anti-Patterns to Avoid
- **Duplicating service calls:** Always use document_service.py, never query Supabase directly from UI
- **Blocking on load:** Check PGP availability asynchronously, don't block page render
- **Losing version state:** Preserve selected version across page navigation within same manuscript

## Don't Hand-Roll

Problems that look simple but have existing solutions:

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| PGP URL generation | String concatenation | Database GENERATED column | URL is computed in documents.pgp_url |
| Version menu UI | Custom dropdown | Existing version_selector.py | Consistent UX, tested pattern |
| Text rendering | Custom formatter | Existing transcription-text CSS class | RTL Hebrew handling already solved |
| Error handling | Try/catch everywhere | Service layer returns None | document_service already handles errors |

**Key insight:** The existing codebase has solved most of the hard UI problems. This phase is primarily about wiring new data into existing patterns.

## Common Pitfalls

### Pitfall 1: PGP Check on Every Render
**What goes wrong:** Calling get_document_for_fragment on every UI update causes performance issues
**Why it happens:** NiceGUI re-renders frequently; if PGP check is in render path, it queries DB excessively
**How to avoid:** Check once when loading the page, cache result in state
**Warning signs:** Slow page loads, excessive Supabase API calls in logs

### Pitfall 2: Lost Version Selection on Page Change
**What goes wrong:** User selects PGP version, navigates to next page, returns to original - loses selection
**Why it happens:** Version selection stored in local variable, not persisted
**How to avoid:** Store selected version source in BrowseState for the current manuscript
**Warning signs:** Users report "keeps resetting to V0.8"

### Pitfall 3: Missing Attribution Display
**What goes wrong:** PGP transcription shows but source attribution not visible
**Why it happens:** Only updated the version selector, not the transcription panel header
**How to avoid:** Update both: (1) version label and (2) panel header/attribution area
**Warning signs:** User can't tell if they're looking at HTR or PGP transcription

### Pitfall 4: RTL Text Rendering Issues
**What goes wrong:** PGP transcription Hebrew text displays incorrectly
**Why it happens:** PGP transcriptions may have different formatting than HTR text
**How to avoid:** Use existing transcription-text CSS class which already handles RTL
**Warning signs:** Text alignment issues, mixed LTR/RTL problems

### Pitfall 5: PGP Link Opens Same Tab
**What goes wrong:** Clicking "View on PGP" navigates away from GenizahSearch
**Why it happens:** Forgot `new_tab=True` on ui.link
**How to avoid:** Always use `new_tab=True` for external links
**Warning signs:** Users lose their place in GenizahSearch

## Code Examples

Verified patterns from the existing codebase:

### Loading PGP Data for a Fragment
```python
# Source: web/document_service.py (existing Phase 3 code)
from web.document_service import get_document_for_fragment

# In browse.py, add to load_page function
def load_page(direction: int = 0):
    # ... existing page loading code ...

    # After page is loaded, check for PGP transcription
    if state.current_page and state.current_page.sys_id:
        pgp_doc = get_document_for_fragment(state.current_page.sys_id)
        if pgp_doc and pgp_doc.get('transcription'):
            state.pgp_transcription = {
                'content': pgp_doc['transcription'],
                'attribution': pgp_doc.get('transcription_source', 'PGP'),
                'pgp_url': pgp_doc.get('pgp_url'),
                'pgpid': pgp_doc.get('pgpid')
            }
        else:
            state.pgp_transcription = None
```

### Extending BrowseState for PGP
```python
# Source: web/pages/browse.py (extend existing class)
class BrowseState:
    def __init__(self):
        # ... existing fields ...

        # PGP transcription state (new)
        self.pgp_transcription: Optional[dict] = None
        self.current_version_source: str = 'original'  # 'original', 'pgp', 'user'
```

### Version Selector with PGP Priority
```python
# Source: web/components/version_selector.py (extend existing function)

# Add at top of menu building in load_versions():
def load_versions():
    menu.clear()
    with menu:
        # PGP transcription first (if available via parameter)
        if pgp_transcription:
            def select_pgp():
                version_label.text = f"PGP"
                menu.close()
                if on_version_change:
                    on_version_change(pgp_transcription['content'], {
                        'source': 'pgp',
                        'attribution': pgp_transcription.get('attribution'),
                        'pgp_url': pgp_transcription.get('pgp_url')
                    })

            with ui.menu_item(on_click=select_pgp).classes('text-sm'):
                with ui.column().classes('gap-0'):
                    with ui.row().classes('items-center gap-1'):
                        ui.icon('verified', size='xs').classes('text-green-600')
                        ui.label('PGP Transcription').classes('font-medium')
                    ui.label(pgp_transcription.get('attribution', '')).classes(
                        'text-xs'
                    ).style('color: var(--text-muted);')

            ui.separator()

        # Existing V0.8 Original option...
```

### Translation Strings to Add
```python
# Source: web/translations.py (add new strings)
TRANSLATIONS = {
    'en': {
        # ... existing ...
        'PGP Transcription': 'PGP Transcription',
        'Transcription by': 'Transcription by',
        'View on PGP': 'View on PGP',
        'Princeton Geniza Project': 'Princeton Geniza Project',
    },
    'he': {
        # ... existing ...
        'PGP Transcription': 'תעתוק PGP',
        'Transcription by': 'תעתוק מאת',
        'View on PGP': 'צפה ב-PGP',
        'Princeton Geniza Project': 'פרויקט הגניזה של פרינסטון',
    }
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Single transcription source | Multiple version sources | Existing | Version selector already supports this |
| Direct Supabase queries in UI | Service layer abstraction | Phase 3 | Use document_service.py for all PGP queries |

**Deprecated/outdated:**
- No deprecated approaches - this is a new feature building on existing patterns

## Open Questions

Things that couldn't be fully resolved:

1. **Default version when PGP available**
   - What we know: Requirements say "PGP as primary version when available"
   - What's unclear: Should PGP auto-select on page load, or just appear first in menu?
   - Recommendation: Auto-select PGP as default when available (user can switch to V0.8)

2. **Multi-fragment document transcription display**
   - What we know: PGP transcription is at document level, not fragment level
   - What's unclear: Should we show full document transcription or try to split by fragment?
   - Recommendation: Show full transcription with note "Transcription covers full document"

3. **PGP transcription formatting**
   - What we know: PGP transcriptions may have their own markup/formatting
   - What's unclear: What formatting conventions PGP uses, how to render
   - Recommendation: Display as plain text initially, preserve line breaks

## Sources

### Primary (HIGH confidence)
- `web/pages/browse.py` - Existing browse page implementation, version selector usage
- `web/components/version_selector.py` - Existing version selector pattern
- `web/document_service.py` - Service layer API (Phase 3 output)
- `migrations/add_pgp_documents_tables.sql` - Database schema

### Secondary (MEDIUM confidence)
- [NiceGUI Documentation](https://nicegui.io/documentation) - UI component patterns
- [NiceGUI Best Practices](https://www.oreateai.com/blog/comprehensive-analysis-and-best-practices-guide-for-nicegui-page-layout/33d6025a4cc288327f2ed04df616323f) - Layout and component organization

### Tertiary (LOW confidence)
- None - all findings verified with primary codebase sources

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - Using existing codebase patterns only
- Architecture: HIGH - Extending established version_selector pattern
- Pitfalls: MEDIUM - Based on UI development experience, not all tested

**Research date:** 2026-02-05
**Valid until:** 60 days (stable codebase, well-understood patterns)
