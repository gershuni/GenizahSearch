# Phase 55: Search Within Results - Research

**Researched:** 2026-03-28
**Domain:** Search refinement / progressive narrowing (NiceGUI web + PyQt6 desktop)
**Confidence:** HIGH

## Summary

Phase 55 adds progressive search refinement -- "search within results" -- to both the web and desktop apps. The core search engine (`genizah_core.py:execute_search`) already accepts a `restrict_sys_ids: set` parameter, wired through all search paths (Tantivy word search, metadata search, Responsa, line-break). This means the engine layer needs zero changes. The work is entirely in UI and state management: adding the refinement chain data structure, computing the effective restrict set (intersection of filter restrict + refinement chain), building the breadcrumb chip UI, and wiring session persistence.

The main complexity lies in correctly separating the existing `restrict_sys_ids` (which already means "pre-search filter scope" on both web and desktop) from the new refinement concept, and in handling edge cases around cross-mode refinement, zero-result recovery, and chain replay on session restore.

**Primary recommendation:** Introduce a `RefinementStep` dataclass (shared/refinement.py) used by both apps. Web stores refinement_chain as list of step metadata in SearchUIState; desktop stores it as a list attribute. The effective restrict set is computed as intersection of filter_restrict_sys_ids and the result sys_ids from replaying the chain. All UI changes are additive -- existing filter chips and header controls remain untouched.

<user_constraints>

## User Constraints (from CONTEXT.md)

### Locked Decisions
- D-01: "Search within these N results" button in results header bar (next to result count), hidden when no results
- D-02: Clicking button activates "refine mode" -- scrolls to main search bar, focuses it, shows badge "Refining within N results". No secondary search input
- D-02a: "Cancel" button exits refine mode without running search -- returns to current results unchanged
- D-03: Desktop uses same pattern -- button in results header, activates refine mode on main search bar. Dedicated results-scope strip above results table (not in existing dense row 1)
- D-04: Chip chain with > separator on dedicated strip, NOT inside existing results header
- D-05: No nesting depth limit, chips scroll horizontally on overflow
- D-06: Result count shown only for final (current) step
- D-07: Chip styling reuses existing filter chip pattern from Phase 45
- D-08: Cross-mode refinement allowed (e.g., word search -> Responsa). Restrict set is just sys_ids
- D-09: Refinement and pre-search filters are additive (intersect). Refinement never replaces existing filters
- D-10: Mixed-mode chain shows mode labels on chips only when modes differ
- D-11: "Clear all" button removes entire chain, returns to unrestricted search
- D-12: Each chip has x to remove that step + all subsequent steps
- D-13: Pop-back re-executes earlier query (no caching)
- D-14: Refinement chain persists in session state (params only, not sys_id lists). Sys_ids recomputed on restore by replaying chain
- D-14a: Zero-result refinement is recoverable -- "Back to previous step" button
- D-15: Refined searches NOT added to search history. Only original (first) query enters history
- D-16: If user edits Focus Search filters during active chain, show "Scope changed -- results will update" indicator

### Claude's Discretion
- Exact chip styling, colors, and layout details
- RTL layout adjustments for the chip chain
- How "refine mode" badge is styled
- Exact wording of the "scope changed" indicator (D-16)

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope

</user_constraints>

<phase_requirements>

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| SRCH-01 | User can run a second search restricted to current result set's sys_ids | Core engine already supports restrict_sys_ids; new RefinementStep dataclass + chain computation + UI trigger button |
| SRCH-02 | User can see a refinement breadcrumb showing the search chain | Chip chain UI on dedicated strip, reusing Phase 45 chip pattern, with mode labels for mixed-mode chains |
| SRCH-03 | User can clear refinement to return to full search | "Clear all" button + per-chip x removal + "Back to previous step" on zero results |

</phase_requirements>

## Project Constraints (from CLAUDE.md)

- Python 3.10+, NiceGUI for web UI, PyQt6 for desktop UI
- Both apps must be maintained with shared service layer
- Hebrew RTL -- text and chip chains flow right-to-left
- Search logic lives in genizah_core.py
- Session persistence infrastructure already exists (web: app.storage.user via persist_value/load_filter_state; desktop: shared/session_persistence.py JSON)
- No FastAPI backend -- all local
- Test with `pytest tests/`

## Standard Stack

No new libraries needed. This phase is entirely within existing dependencies.

### Core (already installed)
| Library | Purpose | How Used |
|---------|---------|----------|
| NiceGUI | Web UI | ui.chip (removable), ui.row, ui.button, ui.run_javascript (scroll-to) |
| PyQt6 | Desktop UI | QHBoxLayout, QPushButton, QLabel, QFrame for breadcrumb strip |
| dataclasses | Shared model | RefinementStep dataclass in shared/ |

### No New Dependencies
No packages to install. All UI components use existing NiceGUI chip/button/row and PyQt6 widget primitives.

## Architecture Patterns

### Recommended Structure

```
shared/
  refinement.py          # RefinementStep dataclass + chain helpers (NEW)
web/pages/
  search.py              # Add refinement_chain to SearchUIState, breadcrumb strip, refine mode
genizah_app.py           # Add refinement chain state, breadcrumb strip widget, refine mode
```

### Pattern 1: RefinementStep Dataclass (shared)

**What:** A dataclass storing the full executable state of one refinement step.
**When to use:** Every step in the chain -- both for display and for replay on session restore.

```python
# shared/refinement.py
from dataclasses import dataclass, field, asdict
from typing import Optional

@dataclass
class RefinementStep:
    """One step in a search refinement chain."""
    query: str
    mode: str                                    # 'exact', 'variants', 'responsa', 'Title', 'Shelfmark', etc.
    gap: int = 0
    exclude_words: list = field(default_factory=list)
    text_position: Optional[str] = None          # None, 'start', 'end', 'line_start', 'line_end'
    responsa_options: Optional[dict] = None       # Full responsa dict if mode == 'responsa'
    result_count: int = 0                         # Stored after execution for display

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> 'RefinementStep':
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    @property
    def display_label(self) -> str:
        """Short label for breadcrumb chip."""
        return self.query
```

### Pattern 2: Effective Restrict Set Computation

**What:** The intersection of filter-based restrict_sys_ids and refinement chain restrict_sys_ids.
**When to use:** At search execution time, after computing both sets.

```python
def compute_effective_restrict(filter_restrict: set | None, chain_restrict: set | None) -> set | None:
    """Intersect filter and refinement restrict sets."""
    if filter_restrict is None and chain_restrict is None:
        return None
    if filter_restrict is None:
        return chain_restrict
    if chain_restrict is None:
        return filter_restrict
    return filter_restrict & chain_restrict
```

### Pattern 3: Chain Replay (Session Restore)

**What:** On session restore, replay each step's search params sequentially to rebuild the restrict set.
**When to use:** Web page load with persisted refinement_chain; desktop _restore_session.

```python
async def replay_chain(searcher, steps: list[RefinementStep], filter_restrict: set | None) -> set | None:
    """Replay refinement chain to rebuild restrict sets. Returns final restrict set."""
    current_restrict = filter_restrict
    for step in steps:
        effective = compute_effective_restrict(filter_restrict, current_restrict)
        results = searcher.execute_search(
            step.query, step.mode, step.gap,
            exclude_words=step.exclude_words or None,
            responsa_options=step.responsa_options,
            restrict_sys_ids=effective,
            text_position=step.text_position,
        )
        result_sys_ids = {r.get('display', {}).get('id') for r in results if r.get('display', {}).get('id')}
        step.result_count = len(result_sys_ids)
        current_restrict = result_sys_ids if result_sys_ids else set()
    return current_restrict
```

### Pattern 4: Separate Restrict Concepts (CRITICAL)

**What:** The existing `restrict_sys_ids` field in SearchUIState and `pre_search_restrict_sys_ids` on desktop must NOT be overloaded.

**Web (SearchUIState):**
- `restrict_sys_ids` (existing) -- rename to nothing, keep as-is for filter scope
- Add: `refinement_chain: list[RefinementStep]` -- ordered steps
- Add: `refinement_restrict_sys_ids: set | None` -- sys_ids from last chain step
- At search time: `effective = compute_effective_restrict(restrict_sys_ids, refinement_restrict_sys_ids)`

**Desktop (GenizahApp):**
- `pre_search_restrict_sys_ids` (existing) -- keep as-is for filter scope
- Add: `refinement_chain: list[RefinementStep]`
- Add: `refinement_restrict_sys_ids: set | None`
- At search time (genizah_app.py:23947): pass `compute_effective_restrict(pre_search_restrict_sys_ids, refinement_restrict_sys_ids)`

### Pattern 5: Refine Mode State Machine

**What:** A simple boolean state controlling the search bar behavior.
**States:**
1. **Normal mode** -- search bar runs unrestricted (or filter-restricted) search
2. **Refine mode** -- search bar shows badge, runs search with refinement restrict set, has Cancel button

**Transitions:**
- Click "Search within" button -> enter refine mode (scroll to search bar, focus, show badge)
- Execute search in refine mode -> append RefinementStep, update restrict set, return to normal mode with chain active
- Click "Cancel" in refine mode -> return to normal mode, no changes
- Click "Clear all" on breadcrumb -> clear chain, clear refinement_restrict_sys_ids, return to normal mode

### Anti-Patterns to Avoid
- **Overloading restrict_sys_ids:** Do NOT merge refinement sys_ids into the filter restrict set. They are conceptually different -- filter restrict comes from FJMS catalog queries, refinement restrict comes from prior search results. Merging makes it impossible to clear one without the other.
- **Caching result sys_ids for chain steps:** Per D-13, always re-execute. Tantivy searches are fast enough (sub-second for most queries).
- **Adding refined searches to history:** Per D-15, only the original query enters history.
- **Secondary search input:** Per D-02, there is NO secondary search bar. The main search bar enters "refine mode."

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Chip rendering | Custom HTML/CSS chips | NiceGUI `ui.chip(removable=True)` (web), `QPushButton` with x icon (desktop) | Phase 45 already established the pattern; consistency matters |
| Session persistence | New persistence mechanism | `persist_value()` / `app.storage.user` (web), `save_session_state()` JSON (desktop) | Phase 43 infrastructure already works |
| Scroll to element | Manual offset calculation | `ui.run_javascript('element.scrollIntoView({behavior: "smooth"})` (web), `QWidget.ensureVisible` (desktop) | Browser/Qt handle smooth scrolling |
| Horizontal scroll overflow | Custom scroll container | CSS `overflow-x: auto; white-space: nowrap;` (web), `QScrollArea` with horizontal policy (desktop) | Native scroll handles RTL correctly |

## Common Pitfalls

### Pitfall 1: Stale Restrict Set After Filter Change
**What goes wrong:** User has a 3-step refinement chain, then changes a Focus Search filter. The chain was computed against the OLD filter restrict set, but now the filter set is different.
**Why it happens:** Filter restrict and refinement restrict are computed at different times.
**How to avoid:** When filters change during an active chain, either (a) re-execute the entire chain from step 1 with the new filter restrict, or (b) show D-16 "Scope changed" indicator and invalidate the chain. Per D-16, the user chose the indicator approach. The next search should replay the chain with the new filter set.
**Warning signs:** Result count on breadcrumb doesn't match actual results.

### Pitfall 2: sys_id Extraction From Results
**What goes wrong:** Extracting sys_ids from results uses wrong key path.
**Why it happens:** Results are dicts with nested `display.id` key.
**How to avoid:** Always use `r.get('display', {}).get('id')` -- this is the established pattern at search.py:3366.

### Pitfall 3: RTL Chip Chain Direction
**What goes wrong:** Breadcrumb reads left-to-right in RTL mode, making the chain confusing.
**Why it happens:** CSS direction inheritance from RTL page layout.
**How to avoid:** The chip chain should read logically: oldest step first (right in RTL), newest last (left in RTL). The > separator between chips needs to be flipped to < in RTL mode, or use a direction-neutral separator. Consider `direction: ltr` on the chip container with individual chip text in RTL.
**Warning signs:** Chain order looks reversed for Hebrew users.

### Pitfall 4: Zero-Result Refine Losing State
**What goes wrong:** User refines into 0 results, chain state is updated, and they can't get back.
**Why it happens:** The step was appended before checking result count.
**How to avoid:** Per D-14a, show "0 results within current scope" with "Back to previous step" button. Either (a) don't append the step until results > 0, showing a provisional state, or (b) append the step but provide easy undo. Option (a) is cleaner -- show the zero-result message with the back button, and only commit the step to the chain if the user proceeds.
**Warning signs:** User clicks refine, gets 0 results, and the breadcrumb shows a dead-end chip.

### Pitfall 5: Session Restore Chain Replay Perf
**What goes wrong:** Restoring a 5-step chain replays 5 sequential searches, making page load slow.
**Why it happens:** Each step depends on the previous step's results.
**How to avoid:** Tantivy searches are fast (sub-second), so 5 sequential searches should take < 5s. Show a progress indicator during replay. Consider running replay in run.io_bound (web) or QThread (desktop) to avoid blocking UI.
**Warning signs:** Page appears frozen during restore.

### Pitfall 6: Desktop QThread Refinement Race
**What goes wrong:** User clicks "Search within" before the previous SearchThread completes.
**Why it happens:** The refine button is visible while search is running.
**How to avoid:** Hide/disable the "Search within" button while `is_searching` is True. Only show it after results are rendered.

## Code Examples

### Web: Breadcrumb Strip (NiceGUI)

```python
# Dedicated strip below results header, above results
refinement_strip = ui.row().classes('w-full px-4 py-1 gap-1 items-center').style(
    'background: var(--bg-secondary); border-bottom: 1px solid var(--border-light); '
    'overflow-x: auto; white-space: nowrap; min-height: 0;'
)
refinement_strip.set_visibility(False)

def _update_refinement_strip():
    refinement_strip.clear()
    chain = search_state.refinement_chain
    if not chain:
        refinement_strip.set_visibility(False)
        return
    refinement_strip.set_visibility(True)

    needs_mode_labels = len(set(s.mode for s in chain)) > 1

    with refinement_strip:
        for i, step in enumerate(chain):
            if i > 0:
                ui.label('\u203a').classes('text-lg mx-1').style('color: var(--text-tertiary);')

            label = step.query
            if needs_mode_labels:
                label = f"{step.query} ({step.mode})"

            chip = ui.chip(
                label, removable=True,
                color='indigo-2',
            )
            chip.on('remove', lambda _i=i: _remove_refinement_step(_i))

        # Result count for final step
        ui.label(f'{chain[-1].result_count:,}').classes('text-sm font-bold ml-2').style(
            'color: var(--primary-600);'
        )

        # Clear all button
        ui.button(tr('Clear all'), icon='clear_all',
                  on_click=_clear_refinement_chain
        ).classes('text-xs ml-2').props('flat dense no-caps')
```

### Web: Refine Mode Badge

```python
refine_badge = ui.chip(
    '', icon='filter_list', color='amber-3',
).classes('text-sm')
refine_badge.set_visibility(False)

refine_cancel = ui.button(tr('Cancel'), icon='close',
    on_click=_exit_refine_mode
).classes('text-xs').props('flat dense no-caps')
refine_cancel.set_visibility(False)

def _enter_refine_mode():
    n = len(search_state.displayed_results)
    refine_badge.text = f"{tr('Refining within')} {n:,} {tr('results')}"
    refine_badge.set_visibility(True)
    refine_cancel.set_visibility(True)
    search_state._refine_mode = True
    # Scroll to search bar and focus
    ui.run_javascript(f'''
        document.getElementById("c{query_input.id}").scrollIntoView({{behavior: "smooth", block: "center"}});
        setTimeout(() => document.getElementById("c{query_input.id}").querySelector("input").focus(), 500);
    ''')
```

### Desktop: Breadcrumb Strip (PyQt6)

```python
# Dedicated QFrame strip above results table
self.refinement_strip = QFrame()
strip_layout = QHBoxLayout(self.refinement_strip)
strip_layout.setContentsMargins(8, 2, 8, 2)
self.refinement_strip.setStyleSheet(
    "QFrame { background: #f0f4f8; border-bottom: 1px solid #ddd; }"
)
self.refinement_strip.setVisible(False)
# Insert above results table in the layout
```

### Extracting sys_ids From Results (established pattern)

```python
# From search.py:3366 -- this is the canonical way
result_sys_ids = {
    r.get('display', {}).get('id')
    for r in search_state.results
    if r.get('display', {}).get('id')
}
```

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest |
| Config file | tests/ directory, no pytest.ini (convention-based) |
| Quick run command | `pytest tests/test_refinement.py -x` |
| Full suite command | `pytest tests/` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| SRCH-01 | Search restricted to current result sys_ids | unit | `pytest tests/test_refinement.py::test_refinement_step_restrict -x` | Wave 0 |
| SRCH-01 | Cross-mode refinement (word -> responsa) | unit | `pytest tests/test_refinement.py::test_cross_mode_refinement -x` | Wave 0 |
| SRCH-01 | Refinement intersects with filter restrict | unit | `pytest tests/test_refinement.py::test_filter_refinement_intersection -x` | Wave 0 |
| SRCH-02 | Breadcrumb display with mode labels | unit | `pytest tests/test_refinement.py::test_breadcrumb_labels -x` | Wave 0 |
| SRCH-02 | Chip removal pops subsequent steps | unit | `pytest tests/test_refinement.py::test_chip_removal_pops_chain -x` | Wave 0 |
| SRCH-03 | Clear all removes chain | unit | `pytest tests/test_refinement.py::test_clear_all -x` | Wave 0 |
| SRCH-03 | Zero-result recovery | unit | `pytest tests/test_refinement.py::test_zero_result_recovery -x` | Wave 0 |
| SRCH-01 | Session persistence round-trip | unit | `pytest tests/test_refinement.py::test_session_roundtrip -x` | Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/test_refinement.py -x`
- **Per wave merge:** `pytest tests/`
- **Phase gate:** Full suite green before /gsd:verify-work

### Wave 0 Gaps
- [ ] `tests/test_refinement.py` -- covers all SRCH requirements (dataclass serialization, chain computation, effective restrict intersection, display labels, chain truncation)
- Framework install: already present (`pytest` installed)

## Open Questions

1. **Filter change during active chain -- replay or invalidate?**
   - What we know: D-16 says show "Scope changed" indicator. But should the chain auto-replay with new filters, or wait for user action?
   - Recommendation: Show indicator + auto-replay on next search. Don't auto-replay immediately (it could be slow for long chains). Mark the chain as "stale" and replay it when the user triggers any search action.

2. **Composition search (composition tab) refinement?**
   - What we know: CONTEXT.md only mentions word search + Responsa + metadata modes. Composition search has a different result structure.
   - Recommendation: Exclude composition search from refinement for this phase. The feature targets the main search tab only.

## Sources

### Primary (HIGH confidence)
- `genizah_core.py:6605` -- execute_search restrict_sys_ids parameter, verified all search paths support it
- `web/pages/search.py:57-130` -- SearchUIState class structure
- `web/pages/search.py:1093-1220` -- Filter chip bar rendering pattern (Phase 45)
- `web/pages/search.py:3127-3300` -- execute_search flow with restrict_sys_ids computation
- `web/components/filter_panel.py:220` -- persist_value implementation
- `genizah_app.py:23947` -- Desktop SearchThread restrict_sys_ids wiring
- `genizah_app.py:30013-30086` -- Desktop session save format
- `genizah_app.py:30097-30172` -- Desktop session restore flow
- `gui_threads.py:50-80` -- SearchThread class

### Secondary (MEDIUM confidence)
- `55-CONTEXT.md` -- All decisions D-01 through D-16, canonical code references

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new libraries, all existing patterns
- Architecture: HIGH -- core engine already supports restrict_sys_ids, UI patterns established
- Pitfalls: HIGH -- based on direct code reading of existing state management

**Research date:** 2026-03-28
**Valid until:** 2026-04-28 (stable -- no external dependencies changing)
