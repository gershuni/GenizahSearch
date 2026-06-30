# Phase 128: Search Results Space-Scroll (SEED-025) - Context

**Gathered:** 2026-06-27
**Status:** Ready for planning

<domain>
## Phase Boundary

Add a **Space-to-page-scroll** affordance to the search-results area on **both apps** (web NiceGUI + desktop PyQt6). Pressing **Space** page-scrolls the results when no result control holds an actionable focus; **Shift+Space** scrolls up. When a result's checkbox / expand-collapse / open-detail control has focus (or a detail dialog is open), Space keeps doing that action and is NOT stolen. Self-contained UX polish; no change to search logic, result rendering, or any other surface.

**In scope:** the search-results scroll behavior on `/search` (web) and the desktop results table. **Out of scope:** any other scrollable surface (browse, reading desk, catalog, Joins Lab, puzzle), new keyboard shortcuts beyond Space/Shift+Space, and the library filter (that's Phase 129).
</domain>

<decisions>
## Implementation Decisions

### Suppression set — when Space does NOT scroll (D-01)
- **D-01:** Space scrolls the results UNLESS one of these holds keyboard focus / is active: a result's **checkbox**, an **expand/collapse** toggle, an **open-detail** control (link/button that opens the result), or an **open detail dialog/accordion**. Everything else (including a focused-but-inert container, or no focus) falls through to scroll. This matches the user's phrasing "scroll if nothing was selected to be checked/opened/closed." The exact membership of this set MUST be enumerated and unit-tested (SEED-025 open-question #2).

### Scroll step & direction (D-02)
- **D-02:** **Space = one viewport page down; Shift+Space = one viewport page up** (standard browser/reader convention). Not a fixed row count. Native PageDown/PageUp should keep working (don't break them).

### Web scroll target (D-03)
- **D-03:** On web, Space scrolls **only the results pane** — the existing `.results-scroll-area` container (`web/pages/search.py:1763`) — not the document body. The search bar, filter sidebar, and header stay fixed. Integrate with the existing global `ui.keyboard(on_key=...)` handler (search.py:1959, already `ignore=['input','textarea']`); add a Space branch with a focus guard + `preventDefault` + container `scrollBy(±viewport)`. Do NOT `preventDefault` when a control legitimately wants Space (a11y intact — SEED-025 open-question #4).

### Desktop Space semantics (D-04)
- **D-04:** In the desktop results `QTableWidget` (`genizah_app.py:4828`, checkbox column `COL_CHECKBOX` at :4851), Space toggles the checkbox **only when that checkbox cell has focus**; otherwise Space routes to the table's **page-down** (Shift+Space page-up). Preserves today's checkbox-toggle behavior for the focused-cell case.

### Claude's Discretion
- Exact mechanism for detecting "actionable focus" on each platform (web: `document.activeElement` class/role test inside the keydown `js_handler`; desktop: `focusWidget()` / current cell + checkbox-column test) is the planner's/implementer's call, as long as D-01 membership holds and a11y is preserved.
- Smooth vs instant scroll animation — implementer's choice (lean native/instant for predictability).
</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Feature spec
- `.planning/seeds/SEED-025-space-scroll-search-results.md` — the originating seed: intent, why-not-already-working (web focus-steal + own scroll container; desktop checkbox toggle), open questions (now resolved in D-01..D-04), and code pointers.

### Milestone tracking
- `.planning/ROADMAP.md` § "Phase 128: Search Results Space-Scroll (SEED-025)" — goal + success criteria.
- `.planning/REQUIREMENTS.md` § SCROLL — requirements **SCROLL-01** (web) and **SCROLL-02** (desktop).

No external ADRs/specs beyond the seed — requirements fully captured above.
</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **Web results scroll container:** `web/pages/search.py:1763` — `ui.scroll_area().classes('w-full flex-grow results-scroll-area')`. The scroll target for D-03; addressable via its `.results-scroll-area` class in the keydown `js_handler`.
- **Web keyboard handler:** `web/pages/search.py:1959` — existing global `ui.keyboard(on_key=lambda e: handle_keyboard_shortcut(e), ignore=['input','textarea'])` + `handle_keyboard_shortcut` (1961). The Space branch slots in here; the `ignore=['input','textarea']` precedent shows the focus-guard pattern.
- **Desktop results table:** `genizah_app.py:4828` — `self.results_table = QTableWidget(...)`, checkbox column `COL_CHECKBOX` (width set :4851). The desktop scroll target + the checkbox-focus case for D-04.

### Established Patterns
- NiceGUI keydown with a `js_handler` for `preventDefault` + container `scrollBy(...)` is the documented SEED-025 approach and matches the existing keyboard-shortcut wiring.
- Desktop: subclass/override `keyPressEvent` on the results table (or an event filter) to intercept Space and route to `verticalScrollBar()` page-step when no checkbox cell is focused.
- **Post-v8.3.0 decomposition:** desktop results-table code currently lives in `genizah_app.py` (the method-based search-results panel was DEFERRED to SEED-028, NOT extracted) — implement in place there; do not assume a `desktop/search_results_panel.py`.

### Integration Points
- Web: the `handle_keyboard_shortcut` callback + the `.results-scroll-area` element.
- Desktop: the `results_table` QTableWidget event handling; respect the existing checkbox column semantics.
</code_context>

<specifics>
## Specific Ideas

- User's literal request (2026-06-26): "I want to be able to scroll search results with Space (if nothing was selected to be checked/opened/closed)."
- Behavior should feel like the platform-native page-scroll (Space/Shift+Space), not a bespoke nudge.
</specifics>

<deferred>
## Deferred Ideas

- Space-scroll on other scrollable surfaces (browse, reading desk, catalog, Joins Lab) — out of this phase's scope; could be a future polish pass if requested.
- Library filter (SEED-026) — Phase 129, the other half of the v8.3.0 feature work.

None of the open backlog todos matched this phase's scope.
</deferred>

---

*Phase: 128-Search Results Space-Scroll (SEED-025)*
*Context gathered: 2026-06-27*
