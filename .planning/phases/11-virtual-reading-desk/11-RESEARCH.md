# Phase 11: Virtual Reading Desk - Research

**Researched:** 2026-02-08
**Domain:** Multi-manuscript viewer / reading desk UI (NiceGUI web + PyQt6 desktop)
**Confidence:** HIGH

## Summary

The Virtual Reading Desk allows users to view multiple manuscripts together in a single view. It must work in both web (NiceGUI) and desktop (PyQt6) apps. The three entry points are: (1) opening a joined/multi-fragment PGP document, (2) adding manuscripts by shelfmark or sys_id, and (3) populating from personal lists.

The codebase already contains a partial implementation of joined fragment viewing in the web browse page (`enter_joined_view` in browse.py, lines 1020-1033). This shows all fragments from a PGP document in a scroll area with side-by-side image+text panels. However, this is embedded within the browse page, not a standalone feature. The desktop app has no equivalent. The task is to extract and generalize this pattern into a standalone reading desk feature accessible from both apps, with the ability to add/remove manuscripts dynamically.

**Primary recommendation:** Create a new web page (`/reading-desk`) and a new desktop dialog/tab that both consume a shared `ReadingDeskModel` data class. The shared model should handle fragment resolution, image URL generation, and transcription retrieval. The UI layers should be thin wrappers over this shared data logic.

## Standard Stack

### Core (No New Dependencies)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `NiceGUI` | current | Web UI | Already in use for all web pages |
| `PyQt6` | current | Desktop UI | Already in use for desktop app |
| `supabase-py` | sync | Data access | Already in use for all Supabase operations |
| `shared/document_service.py` | current | PGP data functions | Extracted in Phase 8, shared between apps |
| `shared/supabase_provider.py` | current | Client singleton | Shared Supabase client provider |

### Supporting (Already in Codebase)
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `web/services.py` | current | Browse/search services | For `get_full_manuscript`, `get_browse_page`, image URLs |
| `web/components/version_selector.py` | current | Transcription version switching | For per-fragment PGP version selection |
| `web/components/joins_panel.py` | current | Fragment joins discovery | For finding connected fragments |
| `genizah_core.ListsManager` | current | Desktop lists access | For getting list items in desktop |
| `web/user_lists.py` | current | Web lists access | For getting list items in web |
| `gui_threads.PGPSourceWorker` | current | Desktop async PGP fetch | For background Supabase calls in desktop |

### No New Dependencies
This phase requires zero new libraries. Everything needed is already in the codebase.

## Architecture Patterns

### Recommended Project Structure (Changes Only)
```
shared/
  reading_desk_model.py     # NEW: Shared data model for reading desk entries
web/
  pages/
    reading_desk.py         # NEW: Web reading desk page
web/main.py                 # MODIFY: Add /reading-desk route
genizah_app.py              # MODIFY: Add reading desk dialog or tab
gui_threads.py              # MODIFY: Add ReadingDeskWorker (batch fragment loader)
```

### Pattern 1: Shared ReadingDeskModel (Data Layer)
**What:** A shared data model that represents the reading desk state -- a list of manuscripts with their metadata, images, and transcriptions. This is NOT a service class with Supabase calls; it is a plain data container that both UI layers populate.
**When to use:** Both web and desktop create and populate this model before rendering.
**Key design:**
```python
# shared/reading_desk_model.py
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

@dataclass
class ReadingDeskEntry:
    """A single manuscript on the reading desk."""
    sys_id: str
    shelfmark: str
    title: str = ''
    library_code: str = ''
    pgpid: Optional[int] = None
    # Per-page data
    pages: List[Dict[str, Any]] = field(default_factory=list)
    # Each page: {p_num, text, image_url, is_oxford, full_header}
    # PGP sources (if available)
    sources: List[Dict[str, Any]] = field(default_factory=list)
    pgp_doc: Optional[Dict[str, Any]] = None
    # Fragment ordering info
    sequence_order: int = 0

@dataclass
class ReadingDeskState:
    """Full reading desk state."""
    entries: List[ReadingDeskEntry] = field(default_factory=list)
    source_description: str = ''  # "From PGP Document #1234" or "Custom selection"
    pgpid: Optional[int] = None   # If populated from a joined document
```

### Pattern 2: Web Page with Scrollable Panels
**What:** A new `/reading-desk` page that renders all entries in a vertical scroll with side-by-side image+text panels per page. Heavily based on the existing `enter_joined_view` pattern in browse.py (lines 2054-2187).
**When to use:** Web app.
**Key design:**
- Route: `/reading-desk?pgpid=1234` or `/reading-desk?sys_ids=A,B,C` or `/reading-desk?list_id=5`
- Uses URL query parameters to determine initial population
- Sidebar/toolbar to add more manuscripts by shelfmark search
- Each fragment shows recto/verso with images and text side by side
- Per-fragment version selector for PGP editions/translations
- Image controls (zoom, rotate) per image panel

### Pattern 3: Desktop Dialog with Splitter Layout
**What:** A new QDialog or dockable panel that shows multiple manuscripts in a scrollable area with image+text panels. Follows the existing ResultDialog pattern (QDialog with QSplitter).
**When to use:** Desktop app.
**Key design:**
- Launched from: joins dropdown "View All in Reading Desk", lists context menu, browse toolbar
- QScrollArea with QVBoxLayout containing per-fragment widgets
- Each fragment: QSplitter(Image QLabel | QTextBrowser)
- PGP version combo per fragment (reuse `_populate_pgp_combo` pattern)
- PGPSourceWorker per fragment for async loading (use batch pattern)
- QThread worker to load all fragment data in background

### Pattern 4: Entry Points (How Users Open the Reading Desk)

**Entry 1: From Joined Document (VIEW-01)**
- Web: Click "View All Fragments" in joins panel -> navigates to `/reading-desk?pgpid=1234`
- Desktop: Click "View All" in joins dropdown -> opens ReadingDeskDialog
- Data flow: `get_fragments_for_document(pgpid)` -> list of sys_ids -> resolve each

**Entry 2: Manual Add by Shelfmark/sys_id (VIEW-02)**
- Web: Type in search bar on reading desk page -> add to desk
- Desktop: Type in input field on reading desk dialog -> add to desk
- Data flow: `search_by_shelfmark()` or direct sys_id -> add to entries list
- Uses existing `service.search_by_shelfmark()` (web) or `meta_mgr.search_variants()` (desktop)

**Entry 3: From Personal List (VIEW-03)**
- Web: From lists page, "Open in Reading Desk" button -> navigates to `/reading-desk?list_id=5`
- Desktop: From lists panel, context menu "Open in Reading Desk" -> opens dialog
- Data flow: `get_list_items(list_id)` -> extract sys_ids -> populate desk
- Web uses `state.lists_mgr.get_items_in_list_sync(list_id)` or `get_list_items(list_id)`
- Desktop uses `self.lists_mgr.get_items_in_list(list_id)` (ListsManager)

### Anti-Patterns to Avoid
- **Don't embed reading desk in browse.py:** The browse page already handles single-manuscript viewing with complex state. Adding multi-manuscript management would create an unmaintainable monolith. Create a separate page/dialog.
- **Don't fetch all fragment data synchronously on page load:** For a desk with 4+ manuscripts, loading all images and transcriptions blocks the UI. Use lazy loading: show placeholders, load data progressively.
- **Don't create new Supabase queries:** All needed queries exist in `shared/document_service.py`. Reuse `get_fragments_for_document`, `get_all_sources_for_fragment`, `get_sources_for_document`.
- **Don't duplicate image fallback logic:** The web browse page has complex image error handling with NLI manifest fallback, Oxford proxy, etc. Reuse the existing `handleImageError` JS function and `/api/nli_image_by_sysid/` + `/api/oxford_image/` endpoints.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Image loading with fallback | Custom IIIF client | Existing `/api/nli_image_by_sysid/`, `/api/oxford_image/` endpoints + `handleImageError` JS | Complex multi-step fallback (NLI IIIF, manifest, proxy, Oxford) already debugged |
| PGP transcription fetching | New Supabase queries | `shared/document_service.get_all_sources_for_fragment()` | Already handles multi-doc fragments, page filtering, sorting |
| Fragment resolution for joins | New join query logic | `shared/document_service.get_fragments_for_document()` | Already returns fragments ordered by sequence |
| Version selector UI (web) | New version dropdown | `web/components/version_selector.create_version_selector()` | Already handles PGP editions, translations, user corrections, V0.8 |
| Version selector UI (desktop) | New combo box builder | `genizah_app._populate_pgp_combo()` | Already handles grouped PGP editions/translations in QComboBox |
| Lists item retrieval | New list queries | `web/user_lists.get_items_in_list_sync()` (web) / `ListsManager.get_items_in_list()` (desktop) | Already handles auth-aware and local lists |
| Desktop async Supabase | Direct sync calls | `PGPSourceWorker` QThread pattern | Prevents UI freeze, established pattern |
| Image viewer with zoom (web) | New viewer component | Reuse `manuscriptViewer` JS + CSS from browse.py | Complex drag/zoom/rotate already debugged |
| Shelfmark search | New search function | `service.search_by_shelfmark()` (web) / `meta_mgr.search_variants()` (desktop) | Already handles variant matching |

**Key insight:** The web browse page's joined view (lines 2054-2187 of browse.py) is essentially a prototype of the reading desk. The reading desk is an extraction and generalization of that code into a standalone page with add/remove capabilities.

## Common Pitfalls

### Pitfall 1: Image URL Generation Differs Per Library
**What goes wrong:** Images from NLI, Cambridge, Oxford, and other libraries all use different IIIF endpoints and URL patterns. Oxford requires a server proxy due to CORS.
**Why it happens:** No unified image URL API -- each library has its own endpoint.
**How to avoid:** Always use the existing image endpoint pattern:
- Check if Oxford (shelfmark starts with "MS heb" or "MS. Heb")
- Oxford: `/api/oxford_image/{sys_id}?page={page_idx}`
- All others: `/api/nli_image_by_sysid/{sys_id}?page={page_idx}`
- Desktop: Use `ImageLoaderThread` or `browse_viewer.load_images()` pattern
**Warning signs:** Images showing 404 or blank for specific libraries.

### Pitfall 2: Desktop Must Use QThread for All Supabase Calls
**What goes wrong:** Calling `get_fragments_for_document()` or `get_sources_for_document()` directly on the main thread freezes the UI.
**Why it happens:** Supabase HTTP calls take 200-500ms each. Loading 4 fragments with sources = 8+ calls = potential 4-second freeze.
**How to avoid:** Create a `ReadingDeskWorker(QThread)` that batch-loads all fragment data. Signal results back to main thread. Pattern: see `PGPSourceWorker` in gui_threads.py.
**Warning signs:** UI becomes unresponsive when opening reading desk.

### Pitfall 3: Multiple PGP Documents Per Fragment
**What goes wrong:** Some fragments have multiple PGP documents (e.g., one for recto, one for verso). The `get_all_sources_for_fragment(sys_id)` function handles this by returning sources from all linked documents.
**Why it happens:** PGP data model allows multiple documents per physical fragment.
**How to avoid:** Use `get_all_sources_for_fragment(sys_id)` rather than `get_sources_for_document(pgpid)` when loading sources per fragment. Each source includes `page_info` field ('recto'/'verso').
**Warning signs:** Missing transcriptions for some pages when fragment has split recto/verso documents.

### Pitfall 4: Lists Have Different APIs in Web vs Desktop
**What goes wrong:** Web lists use Supabase tables (`user_lists` + `list_items`), desktop lists use local pickle file via `ListsManager`. The same list_id means different things.
**Why it happens:** Desktop still uses local storage with cloud sync, web uses Supabase directly.
**How to avoid:**
- Web: Use `state.lists_mgr.get_items_in_list_sync(list_id)` which handles both auth and local
- Desktop: Use `self.lists_mgr.get_items_in_list(list_id)` from `ListsManager`
- Both return dicts with `sys_id` and optional `shelfmark` keys
**Warning signs:** Empty list items or wrong list_id type (int vs string).

### Pitfall 5: Per-Fragment Image Controls (Pending User Feedback)
**What goes wrong:** The existing joined view in browse.py shows static images without zoom/rotate controls per image. User feedback from Phase 8 verification requested individual image controls for each fragment.
**Why it happens:** The joined view was a quick implementation; only the single-page view has full `manuscriptViewer` zoom/rotate support.
**How to avoid:** Each image panel in the reading desk needs its own zoom/rotate state. On web, create multiple `manuscriptViewer` instances with unique DOM selectors. On desktop, create separate image widgets with independent zoom state.
**Warning signs:** Zoom affecting all images simultaneously rather than just the targeted one.

### Pitfall 6: RTL Text Directionality Per Source
**What goes wrong:** Hebrew editions should be RTL, English translations should be LTR. Mixing them in the same view without proper directionality makes text unreadable.
**Why it happens:** Decision DEC-10-01-02 requires per-source directionality.
**How to avoid:** Check `source.get('language')` -- if 'English', use LTR; otherwise RTL. The version selector callback includes language info.
**Warning signs:** English translations displayed RTL (right-aligned, wrong reading order).

## Code Examples

### Example 1: Populating Reading Desk from Joined Document (Shared Logic)
```python
# This pattern is reusable in both web and desktop
from shared.document_service import get_fragments_for_document, get_all_sources_for_fragment

def populate_from_pgpid(pgpid: int) -> list:
    """Get all fragments for a PGP document to populate reading desk."""
    fragments = get_fragments_for_document(pgpid)
    entries = []
    for frag in fragments:
        entries.append({
            'sys_id': frag.get('sys_id'),
            'shelfmark': frag.get('shelfmark', ''),
            'sequence_order': frag.get('sequence_order', 0),
            'pgpid': pgpid,
        })
    return entries
```

### Example 2: Image URL Pattern (Web)
```python
# From browse.py lines 2095-2118 -- reuse this pattern
def get_image_url(sys_id: str, page_idx: int, shelfmark: str) -> str:
    """Get the correct image endpoint URL for a fragment page."""
    sm_lower = shelfmark.lower()
    is_oxford = sm_lower.startswith('ms heb') or sm_lower.startswith('ms. heb')
    if is_oxford:
        return f'/api/oxford_image/{sys_id}?page={page_idx}'
    else:
        return f'/api/nli_image_by_sysid/{sys_id}?page={page_idx}'
```

### Example 3: Desktop PGP Worker Pattern (from gui_threads.py)
```python
# Established pattern for async Supabase calls in desktop
class ReadingDeskWorker(QThread):
    """Batch load PGP sources for multiple fragments."""
    finished_signal = pyqtSignal(list)  # list of (sys_id, sources, pgp_doc)
    error_signal = pyqtSignal(str)

    def __init__(self, entries: list):
        super().__init__()
        self.entries = entries  # [{sys_id, ...}, ...]

    def run(self):
        try:
            from shared.document_service import (
                get_all_sources_for_fragment,
                get_document_for_fragment,
            )
            results = []
            for entry in self.entries:
                sys_id = entry.get('sys_id')
                sources = get_all_sources_for_fragment(sys_id)
                pgp_doc = get_document_for_fragment(sys_id)
                results.append((sys_id, sources, pgp_doc or {}))
            self.finished_signal.emit(results)
        except Exception as e:
            self.error_signal.emit(str(e))
```

### Example 4: NiceGUI Route Pattern (from web/main.py)
```python
# Pattern for adding a new page route
@ui.page('/reading-desk')
def reading_desk_page_route(
    pgpid: int = None,
    sys_ids: str = None,
    list_id: str = None
):
    set_current_page('/reading-desk')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.reading_desk import create_reading_desk_page
        create_reading_desk_page(
            initial_pgpid=pgpid,
            initial_sys_ids=sys_ids.split(',') if sys_ids else None,
            initial_list_id=list_id
        )
```

### Example 5: Web Lists Item Retrieval
```python
# Getting items from a user's list (web)
from web.state import state as app_state
items = app_state.lists_mgr.get_items_in_list_sync(list_id)
# Each item: {'sys_id': '...', 'shelfmark': '...', 'title': '...', ...}
sys_ids = [item.get('sys_id') for item in items if item.get('sys_id')]
```

### Example 6: Desktop Lists Item Retrieval
```python
# Getting items from a user's list (desktop)
items = self.lists_mgr.get_items_in_list(list_id)
# Each item: {'sys_id': '...', 'shelfmark': '...', 'shelfmark_override': '...', ...}
sys_ids = [item.get('sys_id') or item.get('item_id') for item in items]
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| No multi-fragment view | Joined view in browse.py | Phase 8 (Feb 2026) | Basic prototype exists but embedded in browse page |
| Direct Supabase calls in desktop UI | QThread workers | Phase 10 (Feb 2026) | Must use PGPSourceWorker pattern for all Supabase calls |
| Single PGP transcription per fragment | Multi-source with editions + translations | Phase 9 (Feb 2026) | Version selector handles multiple editions/translations |
| FastAPI backend | Supabase direct | Jan 2026 | All data access goes through Supabase |

**What already exists that the reading desk builds on:**
- `enter_joined_view` in browse.py (web) -- prototype of joined fragment display
- `_rd_view_joins` / `_browse_view_joins` in genizah_app.py (desktop) -- opens JoinsDialog
- `fetch_connected_fragments` in joins_panel.py -- finds all connected fragments
- `get_fragments_for_document` in document_service.py -- ordered fragment list for PGP doc
- `get_all_sources_for_fragment` -- multi-doc sources per fragment
- `create_version_selector` -- web version dropdown with PGP/V0.8/corrections
- `_populate_pgp_combo` -- desktop version dropdown

## Open Questions

1. **Reading desk as separate page vs. browse mode?**
   - What we know: The existing joined view is embedded in browse.py as a mode toggle. A separate page would be cleaner but requires navigation flow.
   - What's unclear: User preference for inline vs. separate page.
   - Recommendation: Create a separate `/reading-desk` page (web) and `ReadingDeskDialog` (desktop). Keep the existing browse inline joined view as-is for quick access, but link to the full reading desk for expanded functionality. This gives users both quick inline view and full-featured desk.

2. **Should the reading desk support reordering fragments?**
   - What we know: PGP joined documents have `sequence_order`. User-added manuscripts have no inherent order.
   - What's unclear: Whether users need drag-and-drop reordering.
   - Recommendation: Display PGP fragments in sequence_order. User-added manuscripts appear in the order added. Do NOT implement drag-and-drop reordering in the first version -- keep it simple.

3. **Should the reading desk state persist across sessions?**
   - What we know: PGP joins and list items persist. Manual "add by shelfmark" entries do not.
   - What's unclear: Whether users need to save a custom reading desk configuration.
   - Recommendation: For v1, do NOT persist reading desk state. Users can create a list and open from list. The desk is transient -- populated on open, lost on close. This avoids new database tables.

4. **Desktop: New tab vs. dialog?**
   - What we know: Desktop already has ~10 tabs. A new tab adds permanent UI real estate. A dialog is modal/modeless.
   - What's unclear: Whether users want reading desk always available or on-demand.
   - Recommendation: Use a modeless QDialog (like ResultDialog) that can be opened alongside the main app. Not a tab -- it's a transient workspace. Can be launched from browse, results, or lists.

## Sources

### Primary (HIGH confidence)
- `shared/document_service.py` -- all PGP data access functions (read in full)
- `web/pages/browse.py` -- existing joined view implementation (read relevant sections)
- `web/components/joins_panel.py` -- fragment discovery and navigation (read in full)
- `web/components/version_selector.py` -- transcription version switching (read in full)
- `genizah_app.py` -- desktop browse tab, ResultDialog, PGP integration (read relevant sections)
- `gui_threads.py` -- PGPSourceWorker pattern (read relevant section)
- `web/services.py` -- BrowsePage, DocumentPage, image URL helpers (read relevant sections)
- `web/supabase_client.py` -- lists CRUD operations (read relevant sections)
- `web/user_lists.py` -- auth-aware lists manager (read relevant sections)
- `genizah_core.py` -- ListsManager class (read relevant sections)
- `web/main.py` -- page routing patterns (read relevant sections)
- `shared/supabase_provider.py` -- Supabase client singleton (read in full)
- `web/document_service.py` -- backward-compat shim (read in full)

### Secondary (MEDIUM confidence)
- Phase 10 RESEARCH.md -- established PGPSourceWorker and desktop PGP patterns
- Phase 8 prior decisions -- shared service layer, desktop QThread requirements

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- zero new dependencies, all from existing codebase
- Architecture: HIGH -- patterns directly observed in existing code
- Pitfalls: HIGH -- all derived from reading the actual implementation
- Data model: HIGH -- verified from document_service.py and Supabase tables
- Lists integration: HIGH -- both web and desktop list APIs verified in code

**Research date:** 2026-02-08
**Valid until:** 2026-03-08 (stable -- internal codebase, no external API changes expected)
