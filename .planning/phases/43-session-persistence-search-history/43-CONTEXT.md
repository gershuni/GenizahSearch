# Phase 43: Session Persistence & Search History - Context

**Gathered:** 2026-03-01
**Status:** Ready for planning

<domain>
## Phase Boundary

Users never lose search state (exclusions, filters, results) when the app restarts, and can recall past searches. Covers both desktop (PyQt6) and web (NiceGUI) apps. Desktop currently has zero persistence — this is the primary pain point (user lost 5K exclusions). Web already has partial persistence via `app.storage.user`.

</domain>

<decisions>
## Implementation Decisions

### Desktop State Persistence
- Persist **everything** by default: full results list, all exclusions (manuscript + domain), search params (query, mode, gap), filter states, current page position
- Covers **both** regular search and composition/parallels search (composition IS parallels — same mode)
- Save on **every significant action** (search completes, exclusion added/removed, filter changed) — must survive crashes
- Store **full result data locally** with no size cap — disk space is cheap
- Persistence is **on by default**, can be disabled in settings

### Search History UX
- **Dropdown near the search input** — compact, always visible
- Each entry shows **query + result count** (minimal and scannable)
- **Separate sections** in dropdown for regular search vs composition/parallels
- **20 entries per search type** by default, configurable in settings
- Each history entry stores **full state** (results + exclusions + filters) — enables complete session restore
- Click a history entry: **restore saved state** by default, with a **"re-run" option** to get fresh results
- **Individual delete** per entry plus a **"Clear All"** option
- Duplicate searches (same params) **update the existing entry** rather than creating a new one
- Composition search history appears **in the composition/parallels tab**, not mixed with regular search
- Regular search history dropdown appears in the regular search tab

### Restore Behavior
- **Auto-restore with banner** on app startup — app opens looking like it did when closed, brief "Session restored" notification fades after a few seconds
- If a search was **interrupted mid-execution** (e.g., long composition search), **offer to resume** from where it stopped
- No explicit "resume session?" dialog — restoration is automatic and non-disruptive

### Cross-app Parity
- History is **local only** — no Supabase sync between web and desktop
- UX **adapted per platform** — same concept/behavior but native widgets (Qt for desktop, NiceGUI for web)
- New settings toggles (enable/disable persistence, history limit) added to **existing settings area** — no new section

### Claude's Discretion
- Desktop storage mechanism (SQLite vs JSON vs other)
- Web app persistence expansion (whether to go beyond current partial persistence)
- Web restore depth on page reload
- Data change detection (whether to warn if underlying data changed since last session)
- Overwrite behavior when restoring a history entry over current unsaved state

</decisions>

<specifics>
## Specific Ideas

- The user who reported losing 5K exclusions was doing composition search — this is the primary use case driving the feature
- "Session restored" banner should feel non-intrusive, like a toast notification
- History dropdown should be scannable at a glance — no clutter

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `app.storage.user` (NiceGUI): Already persists search query, mode, preset, gap, domain exclusions, and full search results in the web app — pattern can be extended
- `SearchUIState` class (`web/pages/search.py:48`): Holds all per-session search state (results, exclusions, filters, domains, printed_ids) — defines what needs to be serialized
- `web/pages/settings.py`: Existing settings page with theme, results_per_page, default_search_mode, default_gap, lab_mode toggles — new toggles go here

### Established Patterns
- Web uses `app.storage.user.get()` / `app.storage.user['key'] = value` for persistence (NiceGUI browser storage with `storage_secret='genizah-secret-v5'`)
- Desktop `closeEvent` (`genizah_app.py:22307`) handles thread cleanup on close — persistence save hooks would go near here
- Search results are stored as list of dicts with `display`, `sys_id`, domain info — serializable
- Domain exclusions stored as sets (need list conversion for JSON)

### Integration Points
- Desktop `closeEvent` — add state serialization before `super().closeEvent()`
- Desktop app startup (after `__init__`) — add state restoration
- Web `search_page()` function (`web/pages/search.py:75`) — already restores some state from storage at page load
- `web/pages/parallels.py:170` — parallels/composition also restores domain exclusions from storage
- `web/pages/settings.py` — add persistence/history settings toggles
- Desktop settings dialog — add matching toggles

</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 43-session-persistence-search-history*
*Context gathered: 2026-03-01*
