# Phase 72: Search Page Split - Research

**Researched:** 2026-04-16
**Domain:** Python/NiceGUI code decomposition (structural refactor, no behavior change)
**Confidence:** HIGH

## Summary

Phase 72 decomposes `web/pages/search.py` (6,732 lines) into three modules: `search_state.py` (state classes + helpers), `search_results.py` (rendering functions), and the trimmed `search.py` (entry point + search execution). The key technical challenge is mapping closure variable dependencies so extracted functions receive everything they need via explicit parameters rather than closure capture.

The research traced every closure variable accessed by the four functions being extracted (`toggle_expansion`, `render_results`, `create_result_card`, `open_advanced_dialog`). The critical finding is that these functions access **7 distinct closure variables** beyond `search_state`: `results_container`, `query_input`, `_page_client`, `PAGE_SIZE`, plus 3 callback functions (`_update_search_within_btn`, `_update_refinement_strip`, `_undo_zero_result_refine`). Several more are accessed indirectly through calls to other closures (`update_selection_ui`, `show_add_to_list_dialog_local`, `copy_result_text`, `_domain_display_name`, `_apply_word_search_exclusions_and_render`).

**Primary recommendation:** Define `SearchPageRefs` as a frozen dataclass holding UI element references and callback function references. Pass `search_state`, `refs`, and `_page_client` as the three parameters to all extracted functions.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Extract functions as module-level functions (not class methods), taking `search_state` as parameter
- **D-02:** Use `SearchPageRefs` dataclass for UI element references and callbacks (not 20 loose parameters)
- **D-03:** `SearchPageRefs` defined in `search_state.py`, populated in `create_search_page()` after UI creation
- **D-04:** `execute_search()` stays in `search.py` (too many live page refs)
- **D-05:** Split: search_state.py (state classes + helpers), search_results.py (rendering), search.py (entry + search)
- **D-06:** `open_advanced_dialog()` goes in `search_results.py` (not web/components/)
- **D-07:** Extracted functions can use `ui.*` because they execute within page async context
- **D-08:** Functions needing `app.storage` receive via SearchUIState or direct import
- **D-09:** pytest baseline must remain green
- **D-10:** Import smoke test must pass
- **D-11:** Web smoke test (search, expand, advanced dialog, navigate)
- **D-12:** CI green (Ubuntu + Windows)

### Claude's Discretion
- Exact contents of SearchPageRefs dataclass
- Which helper functions from lines 297-1779 move to search_state.py
- Commit granularity
- Whether `_has_active_filters()`, `_domain_display_name()` stay or move

### Deferred Ideas (OUT OF SCOPE)
- execute_search() extraction
- open_advanced_dialog() to web/components/
- browse.py split (Phase 73)
- parallels.py split
</user_constraints>

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Search UI state management | Frontend Server (NiceGUI) | -- | All state lives in server-side Python objects |
| Result rendering | Frontend Server (NiceGUI) | Browser (CSS/JS) | NiceGUI generates HTML server-side, browser renders |
| Advanced dialog | Frontend Server (NiceGUI) | Browser (JS viewers) | Dialog is server-driven with client-side image viewer JS |
| Module decomposition | Frontend Server | -- | Pure code reorganization, no tier change |

## Closure Variable Dependency Map (CRITICAL FINDING)

### Variables accessed by `toggle_expansion()` (lines 4576-4601)
| Variable | Type | Source | Notes |
|----------|------|--------|-------|
| `search_state` | SearchUIState | closure | `.expanded_index`, `.expansion_refs`, `._lazy_loaders` |
| `_page_client` | nicegui Client | closure (line 160) | Used for lazy loader context manager |

### Variables accessed by `render_results()` (lines 4603-4830)
| Variable | Type | Source | Notes |
|----------|------|--------|-------|
| `search_state` | SearchUIState | closure | `.displayed_results`, `.current_page`, `.is_running`, `.domain_excluded_results`, `.word_search_excluded_results`, `.manuscript_excluded_results`, `.refinement_chain`, `._all_terms_filter`, `._zero_result_refine`, `.expanded_index`, `.expansion_refs` |
| `results_container` | ui.scroll_area | closure (line 1774) | `.clear()`, context manager, `.run_method()` |
| `PAGE_SIZE` | int (50) | closure (line 61) | Pagination constant |
| `state` | web.state.state | module import | `.last_results` for export sync |
| `_update_search_within_btn` | function | closure (line 1901) | Called after rendering |
| `_update_refinement_strip` | function | closure (line 1827) | Called after rendering |
| `_undo_zero_result_refine` | function | closure (line 1915) | Called on zero-result recovery button |
| `_apply_word_search_exclusions_and_render` | function | closure (line 3700) | Called from word exclusion restore buttons |
| `open_advanced_dialog` | function | closure (line 5234) | Called from excluded result click handlers |
| `toggle_expansion` | function | closure (line 4576) | Called from card click + re-expand after enrichment |
| `create_result_card` | function | closure (line 4831) | Called for each result in page slice |

### Variables accessed by `create_result_card()` (lines 4831-5233)
| Variable | Type | Source | Notes |
|----------|------|--------|-------|
| `search_state` | SearchUIState | closure | `.selected_indices`, `.transcription_sys_ids`, `.result_domains`, `.printed_ids`, `.vs_availability`, `.title_translations`, `.translation_data`, `.catalog_source_counts`, `.refinement_chain`, `.expansion_refs`, `._lazy_loaders` |
| `state` | web.state.state | module import | `.meta_mgr.csv_bank`, `.meta_mgr.parse_full_id_components`, `.lists_mgr` |
| `query_input` | ui.input | closure (line 538) | `.value` for snippet enrichment with chain terms |
| `_page_client` | nicegui Client | closure (line 160) | Used in lazy loader wrapping |
| `toggle_expansion` | function | closure | Click handler for card content column |
| `open_advanced_dialog` | function | closure | Click handler for Quick View button and image click |
| `show_add_to_list_dialog_local` | function | closure (line 6544) | Click handler for star button |
| `update_selection_ui` | function | closure (line 2226) | Called after checkbox toggle |
| `_domain_display_name` | function | closure (line 350) | Reads `search_state.domain_name_map` |
| `toggle_expansion` (via lazy loaders) | function | closure | Lazy text loading hooks |

### Variables accessed by `open_advanced_dialog()` (lines 5234-6529)
| Variable | Type | Source | Notes |
|----------|------|--------|-------|
| `search_state` | SearchUIState | closure | `.displayed_results`, `.title_translations`, `.translation_data`, `.printed_ids` |
| `AdvancedViewState` | class | closure (line 363) | Instantiated per dialog open |
| `state` | web.state.state | module import | `.meta_mgr`, `.lists_mgr` |
| `get_service` | function | module import | Browse page data |
| `show_add_to_list_dialog_local` | function | closure (line 6544) | Add to list button |
| `copy_result_text` | function | closure (line 6530) | Copy button |
| `WEB_PUZZLE_ENABLED` | bool | module import | Puzzle button visibility |

## SearchPageRefs Dataclass Design

Based on the closure analysis above, `SearchPageRefs` needs these fields:

```python
@dataclass
class SearchPageRefs:
    """UI element references and callbacks needed by extracted search_results functions."""
    # UI element references
    results_container: Any           # ui.scroll_area (line 1774)
    query_input: Any                 # ui.input (line 538)
    
    # NiceGUI client context
    page_client: Any                 # ui.context.client (line 160)
    
    # Constants
    page_size: int = 50              # PAGE_SIZE
    
    # Callback functions (set after definition in create_search_page)
    update_search_within_btn: Any = None    # _update_search_within_btn
    update_refinement_strip: Any = None     # _update_refinement_strip
    undo_zero_result_refine: Any = None     # _undo_zero_result_refine
    apply_word_search_exclusions_and_render: Any = None  # _apply_word_search_exclusions_and_render
    update_selection_ui: Any = None         # update_selection_ui
    show_add_to_list_dialog: Any = None     # show_add_to_list_dialog_local
    copy_result_text: Any = None            # copy_result_text (could also be extracted as standalone)
    domain_display_name: Any = None         # _domain_display_name
```

**Note on circular references:** `render_results` calls `create_result_card` and `toggle_expansion`, and `create_result_card` calls `open_advanced_dialog` and `toggle_expansion`. Since all four are in the same `search_results.py` module, these are just module-internal calls -- no circular import issue. [VERIFIED: codebase grep]

**Note on `copy_result_text`:** This function (lines 6530-6542) only uses `ui.run_javascript` and `ui.notify` with `tr()` -- it has zero closure dependencies. It could be defined as a standalone function in `search_results.py` instead of being passed via refs. Same for `show_add_to_list_dialog_local` (lines 6544-6561) which only uses `ui.notify`, `tr`, `state.lists_mgr`, and a component import. [VERIFIED: codebase grep]

## Helper Function Classification (Lines 297-1779)

### Candidates for search_state.py (only depend on SearchUIState / app.storage)

| Function | Lines | Reason | Dependencies |
|----------|-------|--------|-------------|
| `_has_active_filters()` | 297-299 | Thin wrapper around `filter_panel.has_active_filters(search_state)` | search_state only |
| `_domain_display_name()` | 350-360 | Reads `search_state.domain_name_map` + `tr()` | search_state, tr, get_language |
| `_get_search_history()` | 302-304 | Pure `app.storage.user` read | app.storage only |
| `_add_to_search_history()` | 306-337 | Pure `app.storage.user` write | app.storage only |
| `_delete_search_history_entry()` | 339-344 | Pure `app.storage.user` write | app.storage only |
| `_clear_search_history()` | 346-348 | Pure `app.storage.user` write | app.storage only |

**Recommendation:** Move `_domain_display_name` and the 4 search history functions to `search_state.py` as module-level functions. `_has_active_filters` is a trivial one-liner that delegates to `filter_panel.has_active_filters` -- leave it inline or remove (callers can use `has_active_filters(search_state)` directly). [ASSUMED]

### Should stay in search.py (depend on UI elements or complex closures)

| Function | Lines | Reason |
|----------|-------|--------|
| `_update_chip_bar()` | 1221-1351 | Touches `filter_chip_container`, `domain_select`, `author_select`, `work_select` |
| `_remove_filter()` | 1353-1468 | Touches filter UI elements |
| `_update_refinement_strip()` | 1827-1866 | Touches `refinement_strip`, `refine_breadcrumbs` |
| `_update_search_within_btn()` | 1901-1913 | Touches `search_within_btn` |
| `update_selection_ui()` | 2226-2243 | Touches `selection_counter`, `bulk_actions_row`, `select_all_checkbox` |
| All filter panel UI setup | 846-1210 | Deeply coupled to UI element creation |

## AdvancedViewState Location and Contents

**Location:** Lines 363-397 inside `create_search_page()` [VERIFIED: codebase grep]

**Contents:**
```python
class AdvancedViewState:
    current_result_idx: int = 0
    results: List[dict] = []
    current_sys_id: Optional[str] = None
    current_p_num: int = 1
    current_fl_id: Optional[str] = None
    total_pages: int = 1
    current_page: Optional[BrowsePage] = None
    show_image_panel: bool = True
    zoom_level: float = 1.0
    rotation: int = 0
    is_fullscreen: bool = False
    edit_mode: bool = False
    edit_text: str = ""
    edit_notes: str = ""
    original_edit_text: str = ""
    draft_saved: bool = False
    draft_id: Optional[str] = None
    fjms_data: Optional[dict] = None
    crossref_data: Optional[dict] = None
    volume_ie: Optional[str] = None
    highlight_terms: List[str] = []
    # UI refs (set during dialog construction)
    result_label = None
    score_badge = None
    prev_btn = None
    next_btn = None
    content_container = None
    image_container = None
    header_container = None      # Referenced in render_content
    info_bar_container = None    # Referenced in render_content
    brightness_sl = None         # Image adjustment sliders
```

**Move to:** `search_state.py` as a module-level class. No closure dependencies -- only uses type imports (`Optional`, `List`, `BrowsePage`). [VERIFIED: codebase grep]

## NiceGUI Context Requirements

**NiceGUI version:** 3.8.0 [VERIFIED: pip]

**Key finding:** Functions defined outside `create_search_page()` CAN use `ui.*` calls as long as they execute within the correct client context. NiceGUI uses a context-local stack -- what matters is the runtime call stack, not the lexical definition scope. [ASSUMED -- based on NiceGUI architecture knowledge]

**Evidence from existing codebase:**
- `web/components/filter_panel.py` defines module-level functions that use `app.storage.user` -- these work because they're called from within a page context.
- `web/components/catalog_dialog.py`, `web/components/bibliography_dialog.py` all define functions outside page functions that create `ui.dialog()` elements.
[VERIFIED: codebase grep]

**Caveat:** The `_page_client` object (captured via `ui.context.client` at page creation time, line 160) is needed for deferred async operations that may run after the original request context has ended. This must be passed via `SearchPageRefs` to `toggle_expansion` which wraps lazy loaders with `with _page_client:`. [VERIFIED: codebase grep, line 4599]

## SearchUIState Location and Move Considerations

**Location:** Lines 63-155 inside `create_search_page()` [VERIFIED: codebase grep]

**Move to:** `search_state.py` as a module-level class. No closure dependencies. Only uses `Set` from typing. [VERIFIED: codebase grep]

**Fields that need `expanded_index` and `expansion_refs`:** These are set at line 1509-1510 (not in `__init__`). They must be added to `SearchUIState.__init__` when extracting, or set in `create_search_page()` after instantiation. Current code sets them via attribute assignment: `search_state.expanded_index = None`, `search_state.expansion_refs = {}`. [VERIFIED: codebase grep]

Similarly `_lazy_loaders` is accessed via `getattr(search_state, '_lazy_loaders', {})` -- it's dynamically attached. Add it to `__init__` for cleanliness.

## Existing Tests

**No tests directly import from `web.pages.search`.** The only import is in `web/main.py` which imports `create_search_page`. Tests use the search page indirectly through web app testing or test shared services. [VERIFIED: codebase grep]

This means the refactor has zero risk of breaking test imports -- only runtime behavior matters.

## Existing Extraction Patterns

### web/search_bootstrap.py pattern
- Module-level pure functions, no class
- Takes explicit parameters (no state object)
- Returns dict
- Imported by search.py

### web/components/filter_panel.py pattern
- Module-level functions taking `state` parameter
- Uses `app.storage.user` directly (via `from nicegui import app`)
- Handler factory function (`create_filter_handlers`) returns dict of closures
- UI element refs passed as `filter_refs` dict parameter

**Recommendation:** Follow the `filter_panel.py` pattern for `search_results.py` -- module-level functions taking `search_state` + `refs` parameters. [ASSUMED]

## Common Pitfalls

### Pitfall 1: Missing closure variable in extracted function
**What goes wrong:** Extracted function references a variable that was available via closure but isn't in the new parameter list. Results in `NameError` at runtime, often only triggered by specific user interactions (e.g., clicking an excluded result).
**Why it happens:** The 4 functions together reference ~15 distinct closure variables across 2,000+ lines.
**How to avoid:** The dependency map above is exhaustive. Test every interaction path: search, paginate, expand card, open advanced dialog, navigate in dialog, excluded results section, word search exclusion restore.
**Warning signs:** Any function call without a clear import or parameter path.

### Pitfall 2: Late-bound closure callbacks
**What goes wrong:** Callbacks like `_update_search_within_btn` are defined AFTER the extracted functions are imported. If `SearchPageRefs` is constructed before these callbacks exist, the refs will be None.
**Why it happens:** In `create_search_page()`, UI construction and function definitions are interleaved.
**How to avoid:** Construct `SearchPageRefs` after ALL referenced callbacks are defined. Or use a two-phase approach: create refs with None callbacks, then populate them.

### Pitfall 3: `state.last_results` assignment in render_results
**What goes wrong:** `render_results` sets `state.last_results = results` (line 4611) for export sync. This references the global `web.state.state`, not `search_state`.
**Why it happens:** Easy to confuse `state` (global web state) with `search_state` (per-page instance).
**How to avoid:** In `search_results.py`, import `from web.state import state` explicitly. Document the distinction clearly.

### Pitfall 4: Dynamic attributes on SearchUIState
**What goes wrong:** Several attributes are set dynamically without being in `__init__`: `expanded_index`, `expansion_refs`, `_lazy_loaders`, `_filter_recompute_gen`.
**Why it happens:** Organic code growth -- features were added incrementally.
**How to avoid:** When moving SearchUIState to `search_state.py`, add ALL dynamically-set attributes to `__init__` with proper defaults.

### Pitfall 5: open_advanced_dialog's internal nested functions
**What goes wrong:** `open_advanced_dialog` defines ~10 nested functions internally (`navigate_result`, `load_result`, `load_page`, `toggle_edit_mode`, `cancel_edit`, `save_draft`, `submit_correction`, `render_content`, `copy_result_text`, `toggle_fullscreen`). These all capture `adv_state`, `dialog`, `service` from the outer function's scope -- this is fine since they move together with `open_advanced_dialog`.
**Why it happens:** The function is self-contained except for its read of `search_state.displayed_results`, `search_state.title_translations`, `search_state.translation_data`, `search_state.printed_ids`.
**How to avoid:** These four search_state reads are covered by passing `search_state` as a parameter.

## Code Examples

### Pattern: Extracted render function signature
```python
# web/pages/search_results.py
from web.pages.search_state import SearchUIState, SearchPageRefs

def render_results(
    search_state: SearchUIState,
    refs: SearchPageRefs,
    results: list,
    page: int = None,
    scroll_to_top: bool = False,
    reset_expansion: bool = True,
):
    """Render search results into the results container."""
    refs.results_container.clear()
    # ... existing logic, replacing closure refs with refs.xxx
```
[ASSUMED -- recommended pattern based on analysis]

### Pattern: SearchPageRefs population in create_search_page
```python
# In create_search_page(), after all UI elements and callbacks are defined:
refs = SearchPageRefs(
    results_container=results_container,
    query_input=query_input,
    page_client=_page_client,
    page_size=PAGE_SIZE,
    update_search_within_btn=_update_search_within_btn,
    update_refinement_strip=_update_refinement_strip,
    undo_zero_result_refine=_undo_zero_result_refine,
    apply_word_search_exclusions_and_render=_apply_word_search_exclusions_and_render,
    update_selection_ui=update_selection_ui,
    show_add_to_list_dialog=show_add_to_list_dialog_local,
    copy_result_text=copy_result_text,
    domain_display_name=_domain_display_name,
)
```
[ASSUMED -- recommended pattern]

### Pattern: Wrapping extracted functions for closure compatibility
```python
# In create_search_page(), to avoid changing all 20+ call sites:
def render_results(results, page=None, scroll_to_top=False, reset_expansion=True):
    from web.pages.search_results import render_results as _render
    _render(search_state, refs, results, page, scroll_to_top, reset_expansion)
```
[ASSUMED -- alternative pattern if thin wrappers are preferred over updating all call sites]

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Monolithic 6.7K-line page function | Split into 3 modules | Phase 72 | Maintainability, code navigation |
| All state in closure | Explicit state + refs dataclasses | Phase 72 | Testability, composability |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | NiceGUI module-level functions work in page context if called from within that context | NiceGUI Context | HIGH -- if wrong, entire extraction approach breaks. Mitigated by existing filter_panel.py pattern |
| A2 | Move search history helpers to search_state.py | Helper Classification | LOW -- they could also stay in search.py with minimal impact |
| A3 | `copy_result_text` and `show_add_to_list_dialog_local` can be standalone functions in search_results.py | SearchPageRefs Design | LOW -- if wrong, just add to refs dataclass |
| A4 | Thin wrapper pattern is viable alternative to updating all call sites | Code Examples | LOW -- both approaches work |

## Open Questions

1. **Wrapper vs. direct call pattern**
   - What we know: The extracted functions are called from ~15 places within search.py (render_results from execute_search, _apply_domain_exclusions, session restore; create_result_card from render_results; etc.)
   - What's unclear: Whether to update all call sites to pass `(search_state, refs, ...)` or use thin local wrappers
   - Recommendation: Use thin local wrappers in search.py to minimize diff size and risk. The wrappers add ~4 lines each (16 total) but avoid touching 15+ call sites.

2. **`_domain_display_name` location**
   - What we know: Only reads `search_state.domain_name_map` + calls `tr()`. Used by both `create_result_card` (extracted) and domain exclusion logic (stays in search.py).
   - What's unclear: Whether to put in search_state.py or search_results.py
   - Recommendation: Put in search_state.py as a module-level function taking `search_state` parameter. Both modules can import it.

## Environment Availability

Step 2.6: SKIPPED (no external dependencies -- code-only reorganization)

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest |
| Config file | pytest.ini (root) |
| Quick run command | `pytest tests/ -x -q` |
| Full suite command | `pytest tests/` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| WEBM-01 | search.py split into 3 modules | smoke/import | `python -c "from web.pages.search_state import SearchUIState, AdvancedViewState, SearchPageRefs; from web.pages.search_results import render_results, create_result_card"` | Wave 0 |
| WEBM-01 | No test regression | unit | `pytest tests/ -x -q` | Existing |
| WEBM-01 | Web functional test | manual | Launch app, search, expand, advanced dialog | Manual |

### Wave 0 Gaps
- [ ] Import smoke test (can be a simple python -c command, or a test file)
- No new test files strictly needed -- this is a zero-behavior-change refactor

## Sources

### Primary (HIGH confidence)
- `web/pages/search.py` -- full 6,732-line file read and analyzed for closure dependencies
- `web/components/filter_panel.py` -- existing extraction pattern (509 lines)
- `web/search_bootstrap.py` -- existing extraction pattern (68 lines)
- NiceGUI 3.8.0 installed locally -- version confirmed via pip

### Secondary (MEDIUM confidence)
- NiceGUI context model based on codebase evidence (filter_panel, catalog_dialog, bibliography_dialog all work as module-level functions)

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Closure dependency map: HIGH -- every variable traced in source code
- SearchPageRefs design: HIGH -- derived directly from dependency map
- Helper classification: MEDIUM -- some judgment calls on what moves
- NiceGUI context behavior: MEDIUM -- verified by pattern but not docs

**Research date:** 2026-04-16
**Valid until:** 2026-05-16 (stable -- code structure, not external APIs)
