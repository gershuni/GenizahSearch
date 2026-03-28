# Phase 55: Search Within Results - Context

**Gathered:** 2026-03-28 (discussion), 2026-03-28 (Codex review incorporated)
**Status:** Ready for planning

<domain>
## Phase Boundary

Researchers can progressively refine their search by running a second query restricted to the manuscripts from their current result set. This includes a trigger button, breadcrumb chain display, clear/undo behavior, and correct interaction with existing pre-search filters. Both web (NiceGUI) and desktop (PyQt6) apps.

</domain>

<decisions>
## Implementation Decisions

### Trigger & Entry Point
- **D-01:** "Search within these N results" button appears in the results header bar (next to result count) on both web and desktop. Button is hidden/disabled when there are no results
- **D-02:** Clicking the button activates "refine mode" — scrolls to main search bar, focuses it, and shows a visible badge like "Refining within 1,772 results". No secondary search input. Scroll-to is important because the button may be far below the search bar
- **D-02a:** "Cancel" button/link exits refine mode without running a search — returns to the current result set unchanged. This is the escape hatch for accidental clicks (distinct from "Clear all" which dismantles the chain)
- **D-03:** Desktop uses the same pattern — button in results header, activates refine mode on main search bar. Dedicated results-scope strip above the results table (not squeezed into existing dense row 1 at genizah_app.py:14124)

### Breadcrumb Chain Display
- **D-04:** Refinement chain displayed as chip/tag chain with › separator: [חידושים] › [רמבם] › [הלכה]. Breadcrumb lives on its own dedicated strip — NOT inside the existing results header (web header at search.py:1350 is already crowded with count + select-all + domain + printed controls)
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
- **D-14:** Refinement chain persists in session state (web: SearchUIState session persistence from Phase 43; desktop: session JSON). Persist metadata only — store each chip's search params (query, mode, gap, exclude_words, text_position, responsa_options), NOT the result sys_id lists. Sys_ids are recomputed on restore by replaying the chain
- **D-14a:** Zero-result refinement is recoverable — show "0 results within current scope" with a one-click "Back to previous step" button. Don't make the user feel they lost the previous set

### Search History Interaction
- **D-15:** Refined searches are NOT added to normal search history. A query like "Rambam" is reusable; "Rambam within that last 1,772-result set" usually isn't. Only the original (first) query in a chain enters history

### Filter Scope Changes During Active Chain
- **D-16:** If the user edits Focus Search filters while a refinement chain is active, show an explicit "Scope changed — results will update" indicator. Don't silently change the scope underneath the chain

### Claude's Discretion
- Exact chip styling, colors, and layout details
- RTL layout adjustments for the chip chain
- How "refine mode" badge is styled (chip, background color change, etc.)
- Exact wording of the "scope changed" indicator (D-16)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Search Engine
- `genizah_core.py` §6605 — `execute_search()` already accepts `restrict_sys_ids: set` parameter, wired through all search paths (Tantivy, metadata, Responsa, line-break)
- `genizah_core.py` §6330-6430 — `_execute_tantivy_search()` uses restrict_sys_ids for Tantivy clause injection (≤500 ids) and post-filter (>500 ids)

### Web Search State
- `web/pages/search.py` §57-130 — `SearchUIState` class with `restrict_sys_ids` (to be split into filter_restrict + refinement_chain), session persistence fields, filter state
- `web/pages/search.py` §1350 — results header bar (count + select-all + domain + printed controls) — button goes here, breadcrumb on separate strip
- `web/pages/search.py` — main search page (~3,200 lines)

### Desktop Search
- `genizah_app.py` §6094 — `create_search_tab` entry point
- `genizah_app.py` §14124 — results header area (already dense, breadcrumb needs own strip)
- `genizah_app.py` §23947 — where restrict_sys_ids is passed to SearchThread (needs split)
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
- `execute_search(..., restrict_sys_ids=)` — core engine fully supports restrict sets in all search modes
- Filter chip UI patterns from Phase 45 — removable chips with × buttons
- Session persistence infrastructure from Phase 43 — web (SearchUIState serialization) and desktop (session JSON)

### Architecture: Separate Restrict Concepts (IMPORTANT)
Do NOT overload the existing `restrict_sys_ids` field. On web it already means pre-search filter scope (search.py:95), and desktop passes only the pre-search restrict set into the thread (genizah_app.py:23947). Introduce separate concepts:
- `filter_restrict_sys_ids` — existing pre-search filter scope (rename from `restrict_sys_ids`)
- `refinement_chain: list[RefinementStep]` — ordered list of refinement steps, each storing full search params
- `effective_restrict_sys_ids` — computed intersection of filter_restrict + refinement chain result, passed to `execute_search`

Each `RefinementStep` must store the full executable search state: query, mode, gap, exclude_words, text_position, responsa_options. Without this, pop-back and session restore will silently replay the wrong search.

### Established Patterns
- Pre-search filters compute restrict_sys_ids from FJMS catalog queries, then pass to `execute_search`
- Post-search filters (domain exclusion, printed filter, dimension filter) operate on the result set after search
- Refinement is a pre-search restriction (computed from prior result sys_ids). Domain/printed/measurement filters continue to apply afterward — this separation matches existing architecture

### Integration Points
- Results header bar (web, ~search.py:1350): add "Search within" button. Breadcrumb goes on its own strip below this, not inside it
- Results header bar (desktop, ~genizah_app.py:14124): add button. Breadcrumb on dedicated strip above results table
- Search bar (web): `ui.input` with mode selector — needs refine mode state + badge + cancel
- Search bar (desktop): `QLineEdit` with mode combo — needs refine mode state + badge + cancel
- Session persistence: add refinement_chain metadata to saved/restored state (params only, not sys_id lists)

</code_context>

<specifics>
## Specific Ideas

### Minimum Test Matrix (from Codex review)
- Exact → Exact refine
- Exact → Responsa refine (cross-mode)
- Refine with active Focus Search filters (additive intersection)
- Remove middle chip and verify later chips are dropped
- Enter refine mode, then cancel without searching
- Zero-result refine (recoverable state)
- Session restore with active chain (replay from metadata)
- RTL overflow / chip scrolling on both web and desktop

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 55-search-within-results*
*Context gathered: 2026-03-28*
