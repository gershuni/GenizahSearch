# Phase 56: Exclude Known Manuscripts - Context

**Gathered:** 2026-03-29
**Status:** Ready for planning

<domain>
## Phase Boundary

Researchers can hide manuscripts they have already reviewed from search results, using either a saved Supabase cloud list or an imported shelfmark file (text/CSV). Exclusion is post-search (not pre-filter), displayed with counts and per-source clear buttons. Both web and desktop apps.

</domain>

<decisions>
## Implementation Decisions

### Exclusion Source Picker
- **D-01:** Two entry points: (1) button in filter panel alongside domain/material/measurement filters, (2) post-search action button near results count (like "Search within N manuscripts" from Phase 55). Both apps.
- **D-02:** Picker opens a dialog with two modes: select a saved Supabase list, or import a shelfmark file.

### File Import
- **D-03:** Accepted formats: plain text (one shelfmark per line) and CSV (auto-detect shelfmark column).
- **D-04:** Resolution report: table showing resolved items with sys_ids (following the desktop composition search table pattern), unresolved shelfmarks clearly marked. Claude's discretion on exact layout.

### Exclusion Display in Results
- **D-05:** Collapsible excluded section at bottom of results (reusing the domain exclusion pattern from `_apply_domain_exclusions()` in search.py). Shows excluded manuscripts with which source excluded them.
- **D-06:** Per-source clear buttons so user can remove one exclusion source without clearing all.
- **D-07:** Count displayed in results header with source breakdown (e.g., "3 excluded from 'My reviewed list'").

### Chain Interaction
- **D-08:** Exclusions are post-search filters, independent of the refinement chain (Phase 55 breadcrumb chips). Exclusions do NOT feed into `restrict_sys_ids` -- they filter displayed results after search completes.
- **D-09:** Clearing the refinement chain does not clear exclusions, and vice versa.

### Session Persistence
- **D-10:** Exclusion state persists within the session (web: SearchUIState + app.storage.user; desktop: session state JSON) so switching between searches does not lose the active exclude set. Per roadmap success criterion #5.

### Claude's Discretion
- Exact dialog layout (tabs vs sections for list picker / file import)
- Resolution report table column layout
- Button placement details within filter panel
- Desktop dialog implementation (QDialog subclass design)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Search & Exclusion Patterns
- `web/pages/search.py` lines 58-140 — SearchUIState with existing exclusion fields (domain_exclusions, word_search_excluded_ids, filter_material_exclude)
- `web/pages/search.py` `_apply_domain_exclusions()` ~line 3197 — Pattern for post-search exclusion with collapsible display
- `web/pages/search.py` `_apply_word_search_exclusions_and_render()` ~line 3152 — Per-result exclusion pattern with restore buttons

### List System
- `web/user_lists.py` — ListsManager with Supabase-backed list CRUD, `get_items_in_list_sync()` returns items with sys_id + shelfmark
- `web/supabase_client.py` line 595 `get_list_items()` — Supabase list_items table (has sys_id, shelfmark fields)

### Shelfmark Resolution
- `genizah_core.py` line 125 `normalize_shelfmark()` — Variant normalization (CUL T-S, Yevr/EVR, full library names)

### Filter Panel
- `web/components/filter_panel.py` — Existing filter panel with domain/material/measurement filters
- `shared/fjms_service.py` line 860 `get_filter_sys_ids()` — Pre-search filter sys_id computation (NOT used for this phase -- exclusions are post-search)

### Desktop
- `genizah_app.py` — Desktop app with session state persistence, composition search table pattern for resolution report reference

### Phase 55 (Refinement Chain)
- `web/pages/search.py` lines 133+ — refinement_restrict_sys_ids, refinement_chain fields
- Exclusions are independent of refinement chain per D-08/D-09

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `_apply_domain_exclusions()` in search.py: Full pattern for post-search filtering with collapsible excluded results section, count display, and re-render without re-searching
- `ListsManager` in web/user_lists.py: Complete list CRUD with Supabase, already returns items with sys_id
- `normalize_shelfmark()` in genizah_core.py: Handles CUL T-S, Yevr/EVR, full library name stripping, ENA-MS normalization
- `filter_panel.py`: Existing filter panel component with established button patterns
- SearchUIState: Established pattern for adding new exclusion fields with session persistence via app.storage.user

### Established Patterns
- Post-search exclusion: Filter displayed results, track excluded items with reasons, show in collapsible section
- Session persistence: `persist_value()` helper writes to app.storage.user, restored on session init
- Desktop session state: JSON-based session state with restore on app launch

### Integration Points
- SearchUIState: Add new fields for manuscript exclusion set and source tracking
- Filter panel: Add "Exclude manuscripts" button
- Results area: Add post-search "Exclude" action button (near "Search within N" button)
- Results rendering: Add manuscript exclusion filtering alongside domain/word search exclusions
- Desktop: Mirror in genizah_app.py search tab with QDialog for picker

</code_context>

<specifics>
## Specific Ideas

- Resolution report should follow the desktop composition search table pattern (table with sys_id column for found items)
- Post-search filtering chosen because exclusion sets are typically small relative to result sets -- no performance benefit from pre-filtering via restrict_sys_ids

</specifics>

<deferred>
## Deferred Ideas

None -- discussion stayed within phase scope

</deferred>

---

*Phase: 56-exclude-known-manuscripts*
*Context gathered: 2026-03-29*
