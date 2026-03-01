# Phase 42: Search UX & Composition Polish - Context

**Gathered:** 2026-03-01
**Status:** Ready for planning

<domain>
## Phase Boundary

Improve the composition/parallels search experience with progress feedback, partial results on cancel, and visual polish. Add CreationType badge to all result views. UX improvements on existing search infrastructure — no new search modes or capabilities.

</domain>

<decisions>
## Implementation Decisions

### Progress & Duration Display
- Show elapsed timer + chunk counter + ETA during composition/parallels search
- Timer appears on ALL search modes, not just composition — users always know how long their search took
- Placement: inline below search button, above results — compact and unobtrusive
- After search completes, show final summary line: "Search completed in 4m 12s — 150 chunks, 23 results found". Stays visible until next search
- ETA calculated via linear extrapolation from current chunk processing pace
- Desktop: same info displayed in the existing QProgressBar area with format text

### Cancel & Partial Results
- Show partial results immediately with warning banner when user cancels
- No resume option — user must re-run for full results (simpler implementation)
- Partial results get full functionality: sorting, domain filtering, exclusions — same controls as complete results
- Desktop: add cancel button plus Escape keyboard shortcut (desktop users expect keyboard shortcuts)
- Banner shows count searched: "Partial results — 45 of 150 chunks searched" (simple, not overwhelming)

### CreationType Badge
- Small colored chip inline with other metadata (like domain chips)
- Color-coded: distinct colors per type (Original, Copy, Print, etc.)
- Print gets special emphasis: distinct warning/attention color (orange or red-ish) so it stands out at a glance. Other types use neutral colors
- Badge appears everywhere results appear: search results, browse page, catalog browse, parallels results — consistent visibility across both apps
- Manuscripts with no CreationType data: show nothing (no badge). Only FJMS-classified manuscripts get a badge. Clean, no noise
- CreationTypeCode column in fjms_enrichment.db catalog table, decoded via code_values table

### Min-Chunks Filter
- Available for all chunk modes (regular and composition), not just boundary mode
- Different defaults: 1 for regular search, higher (e.g., 3) for composition search
- UI control: Claude's discretion — pick best control based on existing search panel patterns (slider or number input)

### Excluded Results Separator
- Collapsible section at bottom of results — collapsed by default, user clicks to expand
- Keeps focus on main results while excluded are accessible
- Show reason for each exclusion (which exclusion rule matched, e.g., "Excluded: T-S 12.123" or "Excluded: domain Bible")
- Applies to both web and desktop expanded views

### Claude's Discretion
- Exact color choices for CreationType chips (as long as Print stands out)
- Min-chunks control type (slider vs number input) based on UI patterns
- Progress bar visual style on desktop (text format in QProgressBar)
- Exact spacing and typography of progress/summary line
- How to handle ETA jitter (smoothing, minimum display time, etc.)

</decisions>

<specifics>
## Specific Ideas

- User feedback letter (2026-02-27) items: א (duration), ב (ETA), ג (partial results), ו (separator), ז (chunk count), ח (min-chunks), טו (CreationType badge)
- The power user runs very long composition searches (minutes) and switches to other apps — progress info is essential for knowing when to come back
- User specifically asked to identify printed materials vs manuscripts — the Print badge with attention color directly addresses this
- User lost 5K exclusions once — exclusion reason display helps users understand and trust their filters

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `parallels.py` progress callback framework: 50ms ui.timer update loop (line 1119-1152), `progress_cb(current, total)` pattern
- `parallels.py` cancel mechanism: `p_state.is_cancelled` flag, `InterruptedError` propagation, partial result handling (line 1154-1368)
- `genizah_core.py` result separation: `lab_composition_search()` returns `{'main': [], 'filtered': [], 'known': []}` (line 1435-1453)
- `gui_threads.py` thread classes: `CompositionThread` (line 88), `LabCompositionThread` (line 132) — have progress_signal, need cancel flag
- `genizah_app.py` QProgressBar: `self.comp_progress` (line 8968) with `setFormat()` for text display
- `shared/fjms_service.py` catalog access: `get_catalog_records()` method, catalog table has CreationTypeCode column (v5.0.0)
- `code_values` table: 3,440 decoded field values including CreationType codes

### Established Patterns
- Domain chips in search results: colored Quasar chips with Hebrew/English text — same pattern for CreationType
- Post-search domain enrichment: `fjms.get_domains_for_sys_ids(sys_ids)` batch lookup — same pattern for CreationType batch lookup
- Desktop signal pattern: `progress_signal = pyqtSignal(int, int)` for thread→UI progress updates
- Web io_bound pattern: `run.io_bound(run_search)` with progress callback for non-blocking search

### Integration Points
- Progress display: hooks into existing `update_ui()` timer in parallels.py, existing `on_comp_progress()` in genizah_app.py
- CreationType badge: hooks into search result rendering (web: `render_results()`, desktop: result tree item creation)
- CreationType data: batch lookup via new `get_creation_types_for_sys_ids()` method in fjms_service.py
- Min-chunks: new parameter in `lab_composition_search()` and `search_composition_logic()`
- Desktop cancel: new `cancel_flag` attribute on thread classes, checked in progress callback

</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 42-search-ux-composition-polish*
*Context gathered: 2026-03-01*
