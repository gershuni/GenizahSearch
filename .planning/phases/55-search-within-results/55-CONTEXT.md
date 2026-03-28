# Phase 55: Search Within Results - Context

**Gathered:** 2026-03-28
**Status:** Ready for planning

<domain>
## Phase Boundary

Researchers can progressively refine their search by running a second query restricted to the manuscripts from their current result set. This includes a trigger button, breadcrumb chain display, clear/undo behavior, and correct interaction with existing pre-search filters. Both web (NiceGUI) and desktop (PyQt6) apps.

</domain>

<decisions>
## Implementation Decisions

### Trigger & Entry Point
- **D-01:** "Search within these N results" button appears in the results header bar (next to result count) on both web and desktop
- **D-02:** Clicking the button activates "refine mode" and focuses the main search bar — no secondary search input. A visual indicator (chip/badge) shows the bar is in refine mode
- **D-03:** Desktop uses the same pattern — button in results header, activates refine mode on main search bar

### Breadcrumb Chain Display
- **D-04:** Refinement chain displayed as chip/tag chain with › separator: [חידושים] › [רמבם] › [הלכה]
- **D-05:** No nesting depth limit — users can refine as many times as they want, chips scroll horizontally if they overflow
- **D-06:** Result count shown only for the final (current) step, not on every chip
- **D-07:** Chip styling reuses existing filter chip pattern from Phase 45

### Search Mode Interaction
- **D-08:** Cross-mode refinement is allowed — user can refine from any search mode into any other (e.g., word search → Responsa refinement). The restrict set is just sys_ids regardless of mode
- **D-09:** Refinement and pre-search filters (domain, dimensions, material from Phase 45/54) are additive — they intersect. Refinement narrows further, never replaces existing filters (per SRCH-03)
- **D-10:** When the refinement chain mixes modes, each chip shows the search mode label: [חידושים (Word)] › [רמב"* (Responsa)]. Mode labels only appear when the chain actually uses different modes

### Clear & Undo Behavior
- **D-11:** "Clear all" button removes entire refinement chain and returns to unrestricted search
- **D-12:** Each chip has × to remove that step — removing a chip also removes all subsequent chips (chain must stay sequential). Removing a middle chip pops it AND everything after it
- **D-13:** When popping back, the earlier query is re-executed (not cached). Tantivy searches are fast enough that re-running is preferred over caching complexity
- **D-14:** Refinement chain persists in session state (web: SearchUIState session persistence from Phase 43; desktop: session JSON). Consistent with how filters and exclusions already persist

### Claude's Discretion
- Exact chip styling, colors, and layout details
- Whether breadcrumb bar appears above or below the results count
- RTL layout adjustments for the chip chain
- How "refine mode" is visually indicated in the search bar (chip, badge, background color change, etc.)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Search Engine
- `genizah_core.py` §6605 — `execute_search()` already accepts `restrict_sys_ids: set` parameter, wired through all search paths (Tantivy, metadata, Responsa, line-break)
- `genizah_core.py` §6330-6430 — `_execute_tantivy_search()` uses restrict_sys_ids for Tantivy clause injection (≤500 ids) and post-filter (>500 ids)

### Web Search State
- `web/pages/search.py` §57-130 — `SearchUIState` class with `restrict_sys_ids`, session persistence fields, filter state
- `web/pages/search.py` — main search page (~3,200 lines)

### Desktop Search
- `genizah_app.py` §6094 — `create_search_tab` entry point
- `gui_threads.py` §25 — `SearchThread` with responsa_options

### Prior Phase Context
- `.planning/phases/45-filtered-search-context/45-CONTEXT.md` — filter chip patterns, session persistence, "search within" from browse
- `.planning/phases/54-dimensions-display-filtering/54-CONTEXT.md` — measurement filter patterns, pre/post-search filter layers
- `.planning/phases/43-session-persistence-search-history/43-CONTEXT.md` — session state persistence infrastructure

### Requirements
- `.planning/REQUIREMENTS.md` §SRCH-01,02,03 — search within results requirements

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `SearchUIState.restrict_sys_ids` — already exists for pre-search filter restriction, can be extended for refinement chain
- `execute_search(..., restrict_sys_ids=)` — core engine fully supports restrict sets in all search modes
- Filter chip UI patterns from Phase 45 — removable chips with × buttons
- Session persistence infrastructure from Phase 43 — web (SearchUIState serialization) and desktop (session JSON)

### Established Patterns
- Pre-search filters compute `restrict_sys_ids` from FJMS catalog queries, then pass to `execute_search`
- Post-search filters (domain exclusion, printed filter, dimension filter) operate on the result set after search
- Refinement is conceptually a pre-search restriction (computed from prior result sys_ids)

### Integration Points
- Results header bar (web): where result count and pagination controls live — add "Search within" button here
- Results header bar (desktop): similar location
- Search bar (web): `ui.input` with mode selector — needs refine mode state
- Search bar (desktop): `QLineEdit` with mode combo — needs refine mode state
- Session persistence: add refinement chain to saved/restored state

</code_context>

<specifics>
## Specific Ideas

No specific requirements — open to standard approaches

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 55-search-within-results*
*Context gathered: 2026-03-28*
