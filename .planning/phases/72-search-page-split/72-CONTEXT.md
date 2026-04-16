# Phase 72: Search Page Split - Context

**Gathered:** 2026-04-16
**Status:** Ready for planning

<domain>
## Phase Boundary

Decompose `web/pages/search.py` (6,732 lines) into focused modules for state and results rendering. First web decomposition phase in v7.9. search.py remains the entry point and retains `create_search_page()` + `execute_search()`.

In scope:
- **`web/pages/search_state.py`** (new): `SearchUIState`, `AdvancedViewState`, session restore/persist helpers, `SearchPageRefs` dataclass for UI references
- **`web/pages/search_results.py`** (new): `toggle_expansion()`, `render_results()`, `create_result_card()`, `open_advanced_dialog()`
- **`web/pages/search.py`** (modified): remains entry point, imports from split modules, retains `create_search_page()` and `execute_search()`

Out of scope:
- `execute_search()` extraction (2,155 lines — too deeply intertwined with UI elements; see D-04)
- `SearchPageController` class refactor (architectural redesign, not structural decomposition)
- Moving `open_advanced_dialog()` to `web/components/` (not reusable yet — see D-06)
- Other web page splits (browse.py, parallels.py — later phases)
- Any behavior change, styling tweak, or feature addition

</domain>

<decisions>
## Implementation Decisions

### Extraction Strategy (Gray Area 1)
- **D-01:** Extract functions that take `search_state` as parameter — module-level functions in the new modules, not a class refactor. This matches the "minimal change" decomposition philosophy from the desktop phases.
- **D-02:** Don't pass 20 loose parameters. Use `search_state` (SearchUIState instance) plus a compact **`SearchPageRefs`** dataclass for UI element references and callbacks. This dataclass holds things like `results_container`, `progress_bar`, `search_btn`, `mode_select`, etc. that the extracted functions need to manipulate.
- **D-03:** `SearchPageRefs` is a simple `@dataclass` defined in `search_state.py` alongside SearchUIState. It's populated in `create_search_page()` after the UI elements are created, then passed to the extracted functions.

### Module Boundaries (Gray Area 2)
- **D-04:** `execute_search()` stays in `search.py`. It touches too many live page refs: `progress_bar`, `results_count`, `search_btn`, `stop_btn`, `adv_filters_panel`, `mode_select`, `gap_input`, `text_position_select`, `lab_mode`, `refine_badge`, `refine_cancel_btn`, and more. Extracting it now would replace nested-closure coupling with giant function signatures — churn without simplification.
- **D-05:** The split is:
  - `web/pages/search_state.py`: SearchUIState class, AdvancedViewState class, SearchPageRefs dataclass, session restore/persist helper functions
  - `web/pages/search_results.py`: toggle_expansion(), render_results(), create_result_card(), open_advanced_dialog()
  - `web/pages/search.py`: create_search_page() entry point, execute_search(), UI construction, filter logic, refinement chain, all remaining closures

### open_advanced_dialog Placement (Gray Area 3)
- **D-06:** `open_advanced_dialog()` goes into `search_results.py` with the other rendering code, NOT into `web/components/`. It is search-specific: reads `search_state.displayed_results`, `search_state.title_translations`, search snippets/highlights, and result-navigation assumptions. Moving it to components would advertise reuse the code doesn't have. If a later phase identifies genuine reuse, it can be extracted to `web/components/advanced_dialog.py` then.

### NiceGUI Context Considerations
- **D-07:** Extracted functions in `search_results.py` can use `ui.*` and NiceGUI context because they run within the page's async context (called from closures inside `create_search_page()`). NiceGUI doesn't require functions to be defined inside the page function — only that they execute within the correct client context.
- **D-08:** Functions that need `app.storage.user` or `app.storage.tab` receive these via SearchUIState (which is populated during session restore) or via `from nicegui import app` import.

### Verification
- **D-09:** pytest baseline must remain green (no regression from current counts).
- **D-10:** Import smoke: `python -c "from web.pages.search_state import SearchUIState, AdvancedViewState, SearchPageRefs; from web.pages.search_results import render_results, create_result_card"` — all succeed.
- **D-11:** Web smoke test: launch web app, perform a search, verify results render correctly, click a result to expand (toggle_expansion), open advanced dialog, navigate between results in advanced dialog, close. No visual regression.
- **D-12:** CI green (Ubuntu + Windows matrix).

### Claude's Discretion
- Exact contents of SearchPageRefs dataclass — derived from what the extracted functions actually need.
- Which helper functions from lines 297-1779 move to search_state.py vs. stay in search.py.
- Commit granularity within plans.
- Whether `_has_active_filters()`, `_domain_display_name()`, etc. stay in search.py or move to search_state.py (if they only operate on SearchUIState, they're candidates for search_state.py).

### Folded Todos
None — matched todos are orthogonal.

</decisions>

<canonical_refs>
## Canonical References

### Roadmap & Requirements
- `.planning/ROADMAP.md` — Phase 72 entry; v7.9 milestone boundaries
- `.planning/REQUIREMENTS.md` — WEBM-01
- `.planning/PROJECT.md` — v7.9 Active milestone

### Source — Subject of the Phase
- `web/pages/search.py` (6,732 lines) — the entire file is in scope for splitting
  - Lines 63-155: SearchUIState class
  - Lines 297-1779: History/filter helpers
  - Lines 1780-1946: Refinement chain
  - Lines 3937-4575: execute_search() (stays in search.py per D-04)
  - Lines 4576-4830: render_results()
  - Lines 4831-5233: create_result_card()
  - Lines 5234-6529: open_advanced_dialog()
  - Lines 6530-6732: Misc helpers

### Existing Patterns
- `web/components/filter_panel.py` — existing example of extracted search UI component
- `web/search_bootstrap.py` — existing helper module for search page
- `shared/refinement.py` — shared refinement chain logic (already extracted)
- `shared/exclusion_service.py` — shared exclusion logic (already extracted)

### CI & Verification
- `.github/workflows/ci.yml` — Ubuntu + Windows matrix
- `tests/` — current baseline must remain green

</canonical_refs>

<code_context>
## Existing Code Insights

### SearchUIState Already Exists
`SearchUIState` (lines 63-155) is a class defined INSIDE `create_search_page()`. It holds all search state: query params, filters, refinement chain, exclusions, VS state, measurement filters, translation data. This is the primary candidate for extraction to search_state.py.

### AdvancedViewState Also Exists
There's likely a second state class for the advanced dialog view. Grep for `AdvancedViewState` to find it.

### Closure Variable Sharing
All 60 nested functions share these key variables through closure:
- `search_state` (SearchUIState instance)
- UI element references (created in the function body)
- `_page_client` (NiceGUI client context)
- `_csv_bank` (metadata lookup)

The `SearchPageRefs` dataclass (D-02/D-03) captures the UI element references so extracted functions can access them without being closures.

### NiceGUI Async Pattern
Many functions are `async def` and use `run.io_bound()` for blocking operations. The extracted functions must preserve this pattern.

</code_context>

<deferred>
## Deferred Ideas

### For a Future Phase
- **execute_search() extraction** — if a `SearchPageController` class is introduced, `execute_search()` becomes a method, which solves the UI reference problem naturally
- **open_advanced_dialog() → web/components/** — when genuine reuse emerges (e.g., browse page wants same dialog)
- **browse.py split** (5,076 lines) — Phase 73
- **parallels.py split** (3,451 lines) — future

</deferred>

---

*Phase: 72-search-page-split*
*Context gathered: 2026-04-16*
