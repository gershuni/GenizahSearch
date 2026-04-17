# Phase 73: Browse Page Split - Pattern Map

**Mapped:** 2026-04-16
**Files analyzed:** 3 (2 new, 1 modified)
**Analogs found:** 3 / 3

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `web/pages/browse_state.py` (new) | state | data-holder | `web/pages/search_state.py` | exact |
| `web/pages/browse_enrichment.py` (new) | service | request-response | `web/pages/search_results.py` | role-match |
| `web/pages/browse.py` (modified) | controller | request-response | `web/pages/search.py` (import wiring) | exact |

## Pattern Assignments

### `web/pages/browse_state.py` (state, data-holder)

**Analog:** `web/pages/search_state.py`

**Module docstring pattern** (lines 1-11):
```python
# -*- coding: utf-8 -*-
"""
Search State Classes and Helpers

Extracted from web/pages/search.py (Phase 72, Plan 01).
Contains SearchUIState, AdvancedViewState, SearchPageRefs dataclass,
search history management functions, and domain_display_name helper.

This module has ZERO UI (nicegui.ui) dependencies -- it only holds state
and pure logic that operates on app.storage or SearchUIState fields.
"""
```
Key constraint: **ZERO UI (nicegui.ui) dependencies** -- browse_state.py must follow this same rule. BrowseState is pure data.

**Imports pattern** (lines 12-20):
```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, List, Set
from datetime import datetime

from nicegui import app
from web.translations import tr, get_language
from web.services import BrowsePage
```
Note: `nicegui.app` (for `app.storage`) is acceptable, but `nicegui.ui` is NOT imported.

**State class pattern** (lines 27-123 of search_state.py -- SearchUIState):
```python
class SearchUIState:
    def __init__(self):
        self.progress = 0.0
        self.status = ""
        self.is_running = False
        # ... ~95 fields with type annotations and defaults
```
BrowseState (browse.py lines 479-535) already matches this pattern -- 30 fields, all initialized in `__init__`, no methods beyond data.

**Source to extract** -- BrowseState class from `web/pages/browse.py` lines 479-535:
```python
class BrowseState:
    """Holds the state for the browse page."""

    def __init__(self):
        self.shelfmark_query: str = ''
        self.current_page: Optional[BrowsePage] = None
        self.sys_id: Optional[str] = None
        # ... (57 lines total)
```

**Module-level cache** -- also extract `_crossref_cache` from browse.py lines 537-539:
```python
# Module-level crossref cache: keyed by sys_id, persists across page navigations
# within the session. Crossref data is read-only public metadata, safe to share.
_crossref_cache: Dict[str, dict] = {}
```

---

### `web/pages/browse_enrichment.py` (service, request-response)

**Analog:** `web/pages/search_results.py`

**Module docstring pattern** (lines 1-12 of search_results.py):
```python
# -*- coding: utf-8 -*-
"""
Search Results Rendering Functions

Extracted from web/pages/search.py (Phase 72, Plan 02).
Contains the four main rendering functions (toggle_expansion, render_results,
create_result_card, open_advanced_dialog) plus two standalone helpers
(copy_result_text, show_add_to_list_dialog).

Each function that was a closure in create_search_page() now takes explicit
search_state and refs parameters instead of capturing them via closure.
"""
```
Key principle: **explicit state + refs parameters instead of closure capture**.

**Imports pattern** (lines 13-38 of search_results.py):
```python
from __future__ import annotations

from nicegui import ui, run, app
from web.state import state
from web.translations import tr, is_rtl, get_language
# ... feature flags, components, services ...
from web.pages.search_state import (
    AdvancedViewState, domain_display_name,
)
import logging
import asyncio
```
Note: `nicegui.ui` IS imported here since enrichment functions render UI elements (unlike state module).

**Explicit parameter pattern** (lines 86-111 of search_results.py):
```python
def toggle_expansion(search_state, refs, index):
    """Toggle inline accordion expansion for a result card."""
    if search_state.expanded_index == index:
        ...
        
def render_results(search_state, refs, results, page=None, ...):
    refs.results_container.clear()
    ...
```
Every function takes `search_state` (the state object) and `refs` (the PageRefs dataclass) as first two parameters.

**Dataclass refs pattern** (lines 174-196 of search_state.py -- SearchPageRefs):
```python
@dataclass
class SearchPageRefs:
    """UI element references and callbacks needed by extracted search_results functions.

    Populated in create_search_page() after all UI elements and callbacks are defined.
    Plan 02 will wire this up; Plan 01 only defines the dataclass.
    """
    results_container: Any           # ui.scroll_area
    query_input: Any                 # ui.input
    page_client: Any                 # ui.context.client

    page_size: int = 50              # PAGE_SIZE constant

    # Callback functions (set after definition in create_search_page)
    update_search_within_btn: Any = None
    update_refinement_strip: Any = None
    # ...
```
BrowsePageRefs follows this pattern but with browse-specific fields. Per CONTEXT.md D-02/D-06, it captures: `content_container`, `slider_refs`, `enrichment_refs`, `_load_generation`, `_page_client`, and callbacks like `enter_joined_view`, `navigate_to_shelfmark`, `update_content`.

**BrowsePageRefs fields** -- derived from closure analysis of enrichment functions (browse.py lines 903-1380):

| Closure variable | Used in | BrowsePageRefs field |
|-----------------|---------|---------------------|
| `enrichment_refs` | `_update_enrichment_sections()` lines 1243-1298 | `enrichment_refs: dict` |
| `_load_generation` | generation guard line 1103 | `load_generation: dict` |
| `_page_client` | (not directly in enrichment, but needed for async context) | `page_client: Any` |
| `content_container` | (used by update_content called from enrichment) | `content_container: Any` |
| `slider_refs` | (used by update_content) | `slider_refs: dict` |
| `enter_joined_view` | joins button line 1291 | `enter_joined_view: Any` (callback) |
| `update_content` | called at line 1237 | `update_content: Any` (callback) |

**Source to extract** -- enrichment functions from `web/pages/browse.py`:
- `_load_enrichment()` -- lines 903-1238 (335 lines, the core async function with fetch_pgp/fetch_fjms/fetch_crossref/fetch_browse_enrichment inner functions and result processing)
- `_update_enrichment_sections()` -- lines 1240-1298 (59 lines, patches UI containers after Phase B)
- `_populate_bib_catalog_buttons()` -- lines 1299-1379 (81 lines, bibliography/catalog/measurements/VS chips)

---

### `web/pages/browse.py` (modified -- import wiring)

**Analog:** `web/pages/search.py` import block

**Import wiring pattern** (search.py lines 24-37):
```python
from web.pages.search_state import (
    SearchUIState, SearchPageRefs,
    get_search_history, add_to_search_history,
    delete_search_history_entry, clear_search_history,
    domain_display_name,
)
from web.pages.search_results import (
    toggle_expansion as _toggle_expansion,
    render_results as _render_results,
    create_result_card as _create_result_card,
    open_advanced_dialog as _open_advanced_dialog,
    copy_result_text,
    show_add_to_list_dialog as show_add_to_list_dialog_local,
)
```
Note: search.py uses `_` prefix aliases for extracted functions, then wraps them in thin closures that inject state/refs. Browse may use direct calls if fewer call sites (per CONTEXT.md "Claude's Discretion" note).

**Thin wrapper pattern** (from search.py, used to inject closure state into extracted functions):
```python
# In create_search_page():
refs = SearchPageRefs(
    results_container=results_container,
    query_input=query_input,
    page_client=ui.context.client,
)

def toggle_expansion(index):
    _toggle_expansion(search_state, refs, index)
```
For browse.py, the equivalent would be constructing `BrowsePageRefs` in `create_browse_page()` after all UI elements are created, then either thin wrappers or direct calls passing `state` + `refs`.

**Call site pattern** -- browse.py line 901 currently calls enrichment:
```python
await _load_enrichment(state.current_page, my_gen)
```
After extraction, this becomes:
```python
from web.pages.browse_enrichment import load_enrichment
# ...
await load_enrichment(state, refs, state.current_page, my_gen)
```

---

## Shared Patterns

### Zero-UI State Module Convention
**Source:** `web/pages/search_state.py` line 11
**Apply to:** `web/pages/browse_state.py`
```python
# This module has ZERO UI (nicegui.ui) dependencies -- it only holds state
# and pure logic that operates on app.storage or SearchUIState fields.
```

### Explicit Parameters Over Closures
**Source:** `web/pages/search_results.py` line 12
**Apply to:** All functions in `web/pages/browse_enrichment.py`
```python
# Each function that was a closure in create_search_page() now takes explicit
# search_state and refs parameters instead of capturing them via closure.
```

### Generation Guard for Stale Rejection
**Source:** `web/pages/browse.py` line 1103
**Apply to:** `browse_enrichment.py` -- `load_enrichment()` must check generation before and after awaits
```python
# Stale check
if generation != _load_generation['value']:
    return
```
The `_load_generation` dict travels via `BrowsePageRefs.load_generation`.

### Error Handling in Enrichment Fetchers
**Source:** `web/pages/browse.py` lines 916-919 (repeated pattern in all fetch_* functions)
**Apply to:** All fetch functions in `browse_enrichment.py`
```python
try:
    return await run.io_bound(_sync_fn)
except Exception as e:
    logger.error(f"Failed to fetch XYZ data: {e}")
    return None  # or {} for dict returns
```

### asyncio.gather for Parallel Enrichment
**Source:** `web/pages/browse.py` lines 1092-1100
**Apply to:** `browse_enrichment.py` -- `load_enrichment()` orchestrates parallel fetches
```python
try:
    (all_sources, pgp_doc), fjms_data, crossref_data, browse_enrich = await asyncio.gather(
        fetch_pgp(), fetch_fjms(), fetch_crossref(), fetch_browse_enrichment()
    )
except Exception as e:
    logger.error(f"Enrichment fetch failed: {e}")
    state.enrichment_loaded = True
    state.enrichment_loading = False
    return
```

---

## No Analog Found

None -- all three files have close analogs from the Phase 72 search split.

## Metadata

**Analog search scope:** `web/pages/search_state.py`, `web/pages/search_results.py`, `web/pages/search.py`, `web/pages/browse.py`
**Files scanned:** 4 analog files + 1 source file
**Pattern extraction date:** 2026-04-16
