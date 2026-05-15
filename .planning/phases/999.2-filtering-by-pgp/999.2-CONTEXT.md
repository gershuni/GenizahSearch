# Phase 999.2: Filtering by PGP - Context

**Gathered:** 2026-05-15
**Status:** Ready for planning (backlog — promote with /gsd-review-backlog when ready)

<domain>
## Phase Boundary

Add a post-search result-list toggle on the web search results page that
lets the user show only results with PGP transcriptions, hide them, or see
all. Mirrors the existing 3-state "Filter Printed" pattern.

In scope:
- Web `/search` results toolbar — new 3-state toggle button next to the
  existing printed-filter button.
- Visible active-filter chip / badge in the results header area when the
  filter is set to anything other than "All".
- Persist user's choice across sessions.

Out of scope (see Deferred Ideas):
- Pre-search PGP filter (returning only PGP-tagged manuscripts from the
  search engine itself).
- Same toggle on the parallels page.
- Same toggle on the desktop app.

</domain>

<decisions>
## Implementation Decisions

### Filter mode and semantics
- **D-01:** **Post-search filter.** Operates on the in-memory result list returned by an already-completed search. No re-query, no search-engine changes. Mirrors the existing `printed_filter` mechanism at `web/pages/search.py:1402-1434`.
- **D-02:** **3-state** cycle: `all` → `only_pgp` → `hide_pgp` → `all`. Same cycle shape as `printed_filter`'s `['all', 'hide_printed', 'only_printed']` ordering, but with the active states named for PGP presence.
- **D-03:** Decision criterion: a result is "with PGP" iff its `sys_id` is in `search_state.transcription_sys_ids` (the existing set already populated for the green "PGP" badge at `web/pages/search_results.py:397-400`). No new data fetching required.

### Button placement and labels
- **D-04:** Place the new button in the results toolbar **immediately after the existing `printed_filter_btn`** at `web/pages/search.py:1430-1434`. Same row, same `outline dense no-caps` styling.
- **D-05:** Button labels per state (each wrapped in `tr()`):
  - `all` → `'All'`
  - `only_pgp` → `'Has PGP'`
  - `hide_pgp` → `'No PGP'`
- **D-06:** Use color states matching the printed_filter convention: green/positive when `only_pgp` is active (matches the existing green PGP badge color at `search_results.py:399`), red/negative when `hide_pgp` is active. Exact color tokens are Claude's discretion (must use existing `success-*` / `red` props rather than introducing new colors).

### Visibility gating
- **D-07:** Button is **hidden until the current result set contains at least one PGP-tagged hit**. Concretely: `_set_btn_visible(pgp_filter_btn, bool(search_state.transcription_sys_ids))` (or the equivalent intersection-with-current-results check used by `printed_filter`). Same idiom as `_set_btn_visible(printed_filter_btn, False)` at `search.py:1434`.

### Active-filter badge
- **D-08:** When the filter is active (state is `only_pgp` or `hide_pgp`), show a visible chip / badge in the results header area indicating the active filter — *not* just on the button itself. Position it near the existing `exclusion_chips_row` at `web/pages/search.py:1448-1449` so all active-filter indicators co-locate.
- **D-09:** Chip label mirrors the active state: `'Only PGP'` / `'Hiding PGP'` (translated). Dismiss / clear-filter affordance on the chip is Claude's discretion (single-click chip → revert to `all` is a natural pattern; not mandatory).

### Persistence
- **D-10:** Persist via `persist_value('search_pgp_filter', ...)`, exactly matching the printed_filter pattern at `web/pages/search.py:1406`. State is read at search-page bootstrap with the same `_safe_get('search_pgp_filter', 'all')` shape as `:148`. Goes through the Phase 87 `web/safe_storage.py` chokepoint (no raw `app.storage.user` access).

### Interaction with other filters
- **D-11:** PGP filter **stacks** with other active filters (printed, domain exclusions, manuscript exclusions, refinement chain). Apply it in the same render pipeline as `printed_filter` — see the cascade at `web/pages/search.py:1409-1414` (`exclusion_sources` → `domain_exclusions` → fallback to applying remaining filters to `search_state.results`). The new PGP filter slots in after `printed_filter` in this cascade.

### Scope boundaries
- **D-12:** Web `/search` only. Parallels page and desktop app are explicitly **out of scope** (user excluded them when picking "Where should this filter apply?"). Their absence captured in Deferred Ideas.

### Claude's Discretion
- Whether to **also** surface this filter from inside the `filter_panel.py` dialog for discoverability (user softly signaled "It may be in the filter panel"). Decide during planning based on whether it slots in cleanly without adding new plumbing. Default: toolbar only; revisit only if `filter_panel.py` has a natural post-search-toggle section.
- Exact icon (`verified`, `auto_stories`, or none — Material green PGP styling).
- Chip dismiss UX (one-click clear vs explicit X button).
- Whether the chip and button should be visually linked (e.g. clicking the chip also flashes the button briefly).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Pattern to copy (printed_filter)
- `web/pages/search.py:148` — `search_state.printed_filter = _safe_get('search_printed_filter', 'all')` — bootstrap pattern.
- `web/pages/search.py:1402-1414` — `_toggle_printed_filter` cycle + cascade trigger.
- `web/pages/search.py:1416-1428` — `_update_printed_filter_btn` per-state label & color updates.
- `web/pages/search.py:1430-1434` — Button construction + `_set_btn_visible(..., False)` initial hide.
- The new PGP filter mirrors this end-to-end. Read this block first before touching anything else.

### Data source (already populated)
- `web/pages/search_state.py:43` — `self.transcription_sys_ids: Set[str] = set()` — the set of sys_ids with PGP transcriptions, already populated by the existing search enrichment pipeline.
- `web/pages/search_results.py:397-400` — Existing PGP badge render — confirms `transcription_sys_ids` is the canonical "this result has PGP" signal.

### Apply / render pipeline
- `web/pages/search.py:1409-1414` — Filter cascade (`exclusion_sources` → `domain_exclusions` → `printed_filter`). The new PGP filter joins this cascade.
- `_apply_printed_filter_and_render` — reference for how a post-search filter is composed into the final render.

### Active-filter chip placement
- `web/pages/search.py:1448-1449` — `exclusion_chips_row` — the row where active-filter indicators live. The new PGP filter chip co-locates here.

### Persistence chokepoint (Phase 87 requirement)
- `web/safe_storage.py` — All per-user persistence MUST go through this module. `persist_value` already routes through it.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `search_state.transcription_sys_ids` — already populated for every search; the data we need is in memory.
- `persist_value` — existing per-user persistence helper used by `printed_filter`.
- `_safe_get` — existing safe-storage read helper.
- `_set_btn_visible` — existing visibility helper used by `printed_filter` and others.
- The toolbar row at `search.py:1400-1434` has clearly-defined slots for additional filter buttons — append, don't restructure.

### Established Patterns
- Post-search filters live in the **results toolbar**, not in `filter_panel.py`. The filter panel is for pre-search filters (domains, authors, works, dates, material).
- Filter buttons are 3-state cycles with stateful label updates and persisted preferences.
- Filter cascades apply in a fixed order: manuscript exclusions → domain exclusions → printed → (new: PGP).
- Active filters surface as chips near `exclusion_chips_row` for at-a-glance visibility.

### Integration Points
- One render addition (the new button) at `search.py:1430-1434`-adjacent.
- One cascade addition (apply PGP filter) inside `_apply_printed_filter_and_render` or a sibling function — exact factoring is Claude's discretion during planning.
- One bootstrap read at `search.py:148`-adjacent.
- One active-filter chip in the row at `search.py:1448`.

</code_context>

<specifics>
## Specific Ideas

- User explicitly asked for **a clear badge in the main results when a filter is active** — not just a button state change. This is the chip/badge requirement in D-08. Don't skip it.
- User picked the shorter `'All' / 'Has PGP' / 'No PGP'` label set over the more verbose printed_filter-style wording. Honor the brevity.
- User left the door open for the filter to also appear in the filter panel ("It may be in the filter panel"). That's a Claude's-discretion fall-back, not a requirement.

</specifics>

<deferred>
## Deferred Ideas

- **Pre-search PGP filter** — wire `has_pgp_transcription` into the filter-panel dialog and pass it through to the search engine so the result list is filtered server-side. Useful if PGP-only searches become a frequent workflow.
- **PGP filter on the parallels page** — same toggle on `/parallels`. The parallels page uses the same `filter_panel.py` helpers, so much of the work would carry over.
- **PGP filter on the desktop app** — parity entry. Desktop search has its own toolbar in `genizah_app.py` near `create_search_tab` (line 5163).
- **Filter by PGP source / author / version** — more fine-grained: only Goitein, only V0.8, only translations, etc. Out of scope for this phase but a natural successor.

</deferred>

---

*Phase: 999.2-filtering-by-pgp*
*Context gathered: 2026-05-15*
