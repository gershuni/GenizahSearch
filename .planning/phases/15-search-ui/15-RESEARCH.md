# Phase 15: Search UI (Both Apps) - Research

**Researched:** 2026-02-09
**Domain:** NiceGUI web UI + PyQt6 desktop UI -- checkbox wiring, visibility toggling, URL state, SearchThread extension
**Confidence:** HIGH

## Summary

Phase 15 is purely UI wiring: adding Responsa checkboxes to both apps and connecting them to the Phase 14 core engine's `execute_search(responsa_options=...)` parameter. No new search logic is needed. The core engine already accepts a `responsa_options` dict with keys `responsa_mode`, `variants`, `ja`, `flex_spacing`, `bidirectional`, and `variant_mode`. It also attaches an `responsa_warning` string to the first result when the explosion guard triggers.

The web app (NiceGUI 3.5, `web/pages/search.py`, 2579 lines) already has patterns for dynamic visibility toggling via `set_visibility()`, mode switching via `on_mode_change()`, and URL state via FastAPI query parameters (`/search?q=...&tag=...`). The desktop app (`genizah_app.py`, 17722 lines) has `create_search_tab()` at line 6094 with a mode combo, variant controls, and a `SearchThread` class in `gui_threads.py` (47 lines) that needs an optional `responsa_options` parameter.

**Primary recommendation:** Split into 2 plans: (1) Web UI -- checkboxes, mode interaction, URL state, explosion warning display; (2) Desktop UI -- checkboxes, mode interaction, SearchThread extension, warning display.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- New row below the search bar, dedicated to Responsa controls
- Flat row of checkboxes with labels -- no visual container, border, or card
- Short labels with tooltips explaining each checkbox on hover
- Responsa row only visible in Exact and Variants search modes -- hidden for Shelfmark, Title, PGP Tags, Fuzzy, Regex
- Responsa Mode checkbox is the only one visible by default (when in Exact/Variants modes)
- Checking Responsa Mode reveals sub-checkboxes (Variants, JA, Flex Spacing) and hides the mode dropdown
- Unchecking Responsa Mode hides sub-checkboxes and restores the mode dropdown
- When Responsa Mode checked ON: mode dropdown hides, sub-checkboxes appear
- When Responsa Mode checked OFF: mode dropdown restores to the previous mode (remembered), sub-checkboxes hide
- In PGP Tags mode: all Responsa checkboxes are hidden (not disabled)
- All checkboxes start unchecked on fresh page load
- No auto-enable: checking Responsa Mode does not auto-check any sub-options
- Sub-checkboxes only appear when Responsa Mode is ON
- Variants checkbox hidden when Responsa OFF -- the dropdown's "Variants" mode serves that role instead
- Desktop: checkboxes reset to defaults on app startup (no persistence)
- Explosion guard warning: inline message under the search bar, auto-dismisses after 5 seconds
- Results header shows expanded term count: e.g., "42 results (searching 150 expanded terms)"
- Same highlighting color for exact matches and expanded-term matches -- no differentiation

### Claude's Discretion
- Checkbox order within the row
- Bidirectional Gap checkbox placement (Advanced Options or main row)
- Mode dropdown hide/show animation/transition style
- Visual indicator for active Responsa mode (when dropdown is hidden)
- Desktop layout adaptation based on existing search tab patterns
- Mobile collapse implementation (icon type, expand behavior)

### Deferred Ideas (OUT OF SCOPE)
- None -- discussion stayed within phase scope
</user_constraints>

## Standard Stack

### Core (Already in Use)
| Library | Version | Purpose | Notes |
|---------|---------|---------|-------|
| NiceGUI | 3.5.0 | Web UI framework | Already installed, Quasar-based |
| PyQt6 | (current) | Desktop UI framework | Already installed |
| genizah_core.py | N/A | Shared search engine | Phase 14 completed Responsa pipeline |
| gui_threads.py | N/A | Desktop async threading | SearchThread needs extension |

### No New Dependencies
This phase adds zero new libraries. Everything is pure UI wiring using existing frameworks.

## Architecture Patterns

### Web App: Current Search UI Structure
```
web/pages/search.py
  create_search_page(initial_query, initial_tag)
    SearchUIState class          # Line 32 -- state management
    expanded_panel               # Line 282 -- contains all search controls
      query_input                # Line 315 -- RTL text input
      mode_select                # Line 383 -- ui.select with mode_options dict
      max_changes_col            # Line 407 -- visible for variant modes
      gap_input                  # Line 427 -- gap control
      search_btn / stop_btn      # Line 437 -- search action
      Advanced Options           # Line 449 -- ui.expansion with Lab Mode, NOT filter
    on_mode_change()             # Line 501 -- handles mode switching
    execute_search()             # Line 1060 -- async, calls state.searcher.execute_search()
    run_core_search()            # Line 1124 -- sync wrapper for execute_search
    render_results()             # Line 1190 -- renders result cards
    results_count label          # Line 547 -- "N Results" text
```

### Desktop App: Current Search Tab Structure
```
genizah_app.py
  create_search_tab()           # Line 6094
    Row 1: query_input, btn_search, btn_ai
    Row 2: mode_combo (QComboBox), variant_controls_container, search_params_container
           tag_search_combo (hidden, shown in PGP Tags mode)
    search_params_container: gap_input, exclude_input, btn_search_settings, btn_lab_mode_toggle, chk_lab_deep
    results_table (QTableWidget) # Line 6328
    status_label                 # Line 6396 -- "Showing X of Y results"
  _on_search_mode_changed(idx)  # Line 12331 -- handles variant/PGP visibility
  toggle_search()               # Line 12635 -- starts or stops search
  start_search()                # Line 12644 -- creates SearchThread, starts it
  on_search_finished(results)   # Line 12872 -- processes results, loads batches
```

### Pattern 1: Master Toggle (Responsa Mode)
**What:** Responsa Mode checkbox controls visibility of sub-checkboxes AND mode dropdown
**When to use:** This is the primary interaction pattern for this phase

**Web implementation approach:**
```python
# After mode_select definition, add Responsa controls row
responsa_row = ui.row().classes('w-full items-center gap-4 px-2')
responsa_row.set_visibility(False)  # Hidden by default until mode check

# Master toggle
responsa_mode_cb = ui.checkbox('Responsa Mode')
responsa_mode_cb.tooltip('Enable Responsa Project-style grammatical expansion')

# Sub-checkboxes (hidden until master is ON)
responsa_variants_cb = ui.checkbox('Variants')
responsa_ja_cb = ui.checkbox('Judeo-Arabic')
responsa_flex_cb = ui.checkbox('Flex Spacing')

# Initially hide sub-checkboxes
responsa_variants_cb.set_visibility(False)
responsa_ja_cb.set_visibility(False)
responsa_flex_cb.set_visibility(False)

# Remember pre-Responsa mode for restoration
pre_responsa_mode = {'value': mode_select.value}

def on_responsa_toggle():
    is_on = responsa_mode_cb.value
    if is_on:
        pre_responsa_mode['value'] = mode_select.value
        mode_select.set_visibility(False)  # Hide dropdown
    else:
        mode_select.set_visibility(True)
        mode_select.value = pre_responsa_mode['value']
    # Show/hide sub-checkboxes
    responsa_variants_cb.set_visibility(is_on)
    responsa_ja_cb.set_visibility(is_on)
    responsa_flex_cb.set_visibility(is_on)
```

**Desktop implementation approach:**
```python
# New row between row1 (query) and row2 (mode/params)
self.responsa_row = QWidget()
responsa_layout = QHBoxLayout(self.responsa_row)
responsa_layout.setContentsMargins(0, 0, 0, 0)

self.chk_responsa_mode = QCheckBox(tr("Responsa Mode"))
self.chk_responsa_mode.setToolTip(tr("Enable grammatical expansion"))
self.chk_responsa_mode.toggled.connect(self._on_responsa_mode_toggled)

self.chk_responsa_variants = QCheckBox(tr("Variants"))
self.chk_responsa_ja = QCheckBox(tr("Judeo-Arabic"))
self.chk_responsa_flex = QCheckBox(tr("Flex Spacing"))
# Sub-checkboxes hidden until Responsa ON
self.chk_responsa_variants.setVisible(False)
# ... etc

# Remember mode before Responsa
self._pre_responsa_mode_idx = 0
```

### Pattern 2: Mode Interaction (Visibility Rules)
**What:** Responsa row visible only in Exact/Variants modes; hidden entirely in other modes

**Web -- extend on_mode_change():**
```python
def on_mode_change():
    mode = mode_select.value
    is_variants = mode in ('variants', 'variants_extended', 'variants_maximum')
    is_tags = mode == 'pgp_tags'
    is_exact = mode == 'exact'
    is_responsa_eligible = is_exact or is_variants

    # Show/hide Responsa row based on mode
    responsa_row.set_visibility(is_responsa_eligible)

    # If switching away from eligible mode while Responsa is ON, turn it off
    if not is_responsa_eligible and responsa_mode_cb.value:
        responsa_mode_cb.value = False
        on_responsa_toggle()

    # ... existing mode change logic ...
```

**Desktop -- extend _on_search_mode_changed():**
```python
def _on_search_mode_changed(self, index):
    is_variants = (index == 1)
    is_exact = (index == 0)
    is_pgp_tags = (index == self.MODE_PGP_TAGS)
    is_responsa_eligible = is_exact or is_variants

    # Responsa row visibility
    self.responsa_row.setVisible(is_responsa_eligible and not is_pgp_tags)

    # If leaving eligible mode while Responsa ON, turn off
    if not is_responsa_eligible and self.chk_responsa_mode.isChecked():
        self.chk_responsa_mode.setChecked(False)

    # ... existing logic ...
```

### Pattern 3: URL State Persistence (Web Only)
**What:** Responsa checkbox states persisted in URL query parameters
**Current pattern:** `/search?q=...&tag=...` via FastAPI route params

**Route signature change needed:**
```python
# web/main.py line 1828
@ui.page('/search')
def search_page_route(
    q: str = None, tag: str = None,
    responsa: int = None, variants: int = None,
    ja: int = None, flex_spaces: int = None,
    bidirectional: int = None
):
```

**Pass to create_search_page:**
```python
create_search_page(
    initial_query=q, initial_tag=tag,
    initial_responsa=responsa, initial_variants=variants,
    initial_ja=ja, initial_flex_spaces=flex_spaces,
    initial_bidirectional=bidirectional
)
```

**URL update on search (navigate approach):**
```python
# Build URL with checkbox state when executing search
params = f'?q={quote(query)}'
if responsa_mode_cb.value:
    params += '&responsa=1'
    if responsa_variants_cb.value: params += '&variants=1'
    if responsa_ja_cb.value: params += '&ja=1'
    if responsa_flex_cb.value: params += '&flex_spaces=1'
    if bidirectional_cb.value: params += '&bidirectional=1'
ui.navigate.to(f'/search{params}')
```

**Alternative (better UX -- no page reload):** Use `ui.run_javascript()` to update the browser URL without navigation:
```python
ui.run_javascript(f"history.replaceState(null, '', '/search{params}')")
```

### Pattern 4: SearchThread Extension (Desktop Only)
**What:** Add optional `responsa_options` parameter to SearchThread without breaking existing callers

```python
# gui_threads.py -- SearchThread
class SearchThread(QThread):
    results_signal = pyqtSignal(list)
    progress_signal = pyqtSignal(int, int)
    error_signal = pyqtSignal(str)

    def __init__(self, searcher, query, mode, gap, exclude_words=None, responsa_options=None):
        super().__init__()
        self.searcher = searcher
        self.query = query
        self.mode = mode
        self.gap = gap
        self.exclude_words = exclude_words
        self.responsa_options = responsa_options

    def run(self):
        try:
            def cb(curr, total): self.progress_signal.emit(curr, total)
            results = self.searcher.execute_search(
                self.query,
                self.mode,
                self.gap,
                progress_callback=cb,
                exclude_words=self.exclude_words,
                responsa_options=self.responsa_options
            )
            self.results_signal.emit(results)
        except Exception as e:
            self.error_signal.emit(str(e))
```

### Pattern 5: Explosion Guard Warning Display
**What:** Show warning when explosion guard downgrades options

**Core engine behavior (already implemented):**
- `execute_search()` attaches `responsa_warning` string to `results[0]` if guard triggered
- Warning text describes what was downgraded (e.g., "Variants downgraded to basic (30 pairs)")

**Web display:**
```python
# After search completes, check first result for warning
if results and results[0].get('responsa_warning'):
    warning = results[0]['responsa_warning']
    # Inline notification that auto-dismisses
    ui.notify(warning, type='warning', timeout=5000)
```

**Desktop display:**
```python
# In on_search_finished():
if results and results[0].get('responsa_warning'):
    warning = results[0]['responsa_warning']
    self.status_label.setText(warning)
    # Auto-dismiss after 5 seconds
    QTimer.singleShot(5000, lambda: self.status_label.setText(
        tr("Showing {} of {} results").format(self.results_loaded, len(self.last_results))
    ))
```

### Pattern 6: Expanded Term Count in Results Header
**What:** Show count of expanded terms in results header

**Challenge:** The expanded term count is computed inside `execute_search()` in genizah_core.py but not currently returned to the caller. Options:
1. **Attach to results metadata** (like `responsa_warning`): Add `responsa_expanded_count` to first result
2. **Return as tuple** -- breaks API, not recommended
3. **Log and parse** -- fragile

**Recommendation:** Option 1 -- attach count to first result dict. Requires a small addition to `execute_search()` in genizah_core.py:
```python
# After building component_dicts, calculate total expanded terms
total_expanded = sum(len(cd['tantivy_terms']) for cd in component_dicts)
# Attach to first result
if deduped:
    deduped[0]['responsa_expanded_count'] = total_expanded
```

**Web display:**
```python
# In results_count label update:
expanded_count = results[0].get('responsa_expanded_count', 0) if results else 0
if expanded_count > 0:
    results_count.text = f"{len(results)} {tr('Results')} ({tr('searching')} {expanded_count} {tr('expanded terms')})"
else:
    results_count.text = f"{len(results)} {tr('Results')}"
```

### Anti-Patterns to Avoid
- **DO NOT create separate Responsa search function in UI code** -- all search logic must stay in genizah_core.py (XAPP-02)
- **DO NOT disable checkboxes instead of hiding** -- user decision says "hidden not disabled"
- **DO NOT auto-check sub-options when Responsa Mode is toggled ON** -- user decision: no auto-enable
- **DO NOT persist Responsa state in app.storage.user** unless it's also in the URL -- URL is the source of truth
- **DO NOT add border/card/container around Responsa checkboxes** -- flat row with labels only

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Checkbox visibility | Custom JS show/hide | NiceGUI `set_visibility()` / PyQt6 `setVisible()` | Framework-native, tested |
| URL state | Manual URL parsing | FastAPI query params (already used) | Consistent with existing `/search?q=&tag=` |
| Tooltip text | Custom popup dialogs | NiceGUI `.tooltip()` / PyQt6 `setToolTip()` | Native, consistent |
| Auto-dismiss warning | Manual timer + hide | NiceGUI `ui.notify(timeout=5000)` / PyQt6 `QTimer.singleShot` | Built-in |
| Async search (desktop) | Manual thread management | Extend existing `SearchThread` | Proven pattern, backward-compatible |

## Common Pitfalls

### Pitfall 1: Mode Dropdown State Loss
**What goes wrong:** When Responsa Mode is toggled OFF, the mode dropdown forgets which mode it was in before
**Why it happens:** Hiding the dropdown doesn't preserve its value if the UI framework resets it
**How to avoid:** Store `pre_responsa_mode` in a closure variable (web) or instance variable (desktop) BEFORE hiding. Restore on uncheck.
**Warning signs:** After toggling Responsa ON then OFF, mode reverts to "Exact" instead of what user had

### Pitfall 2: Circular Event Triggers
**What goes wrong:** `on_mode_change` and `on_responsa_toggle` trigger each other in a loop
**Why it happens:** Changing mode_select.value fires `on_mode_change`, which may modify Responsa state, which fires `on_responsa_toggle`
**How to avoid:** Use a guard flag (`_updating_mode = True`) to prevent recursive callbacks. Or use `blockSignals` in PyQt6.
**Warning signs:** UI freezes, infinite loops, or unexpected state changes

### Pitfall 3: URL State Not Reflecting Checkbox State
**What goes wrong:** User checks Responsa + JA, shares URL, recipient sees different state
**Why it happens:** URL only updated on search execution, not on checkbox change
**How to avoid:** Update URL on search execution (not on every checkbox toggle -- that would cause unnecessary navigations). Restore from URL params on page load.
**Warning signs:** URL shows `?q=...` but no Responsa params despite checkboxes being checked

### Pitfall 4: SearchThread Backward Compatibility
**What goes wrong:** Existing callers of `SearchThread(searcher, query, mode, gap)` break
**Why it happens:** Adding `responsa_options` as a required parameter
**How to avoid:** Make `responsa_options=None` a keyword-only default. Existing calls don't need to change.
**Warning signs:** `TypeError: __init__() takes X positional arguments but Y were given`

### Pitfall 5: Desktop Checkbox Persistence
**What goes wrong:** Checkboxes remember their state between sessions via QSettings
**Why it happens:** PyQt6 state persistence by default (QSettings), or developer adds persistence
**How to avoid:** Explicitly do NOT save checkbox state to QSettings. User decision: "defaults on startup."
**Warning signs:** Reopening desktop app shows Responsa Mode still checked from previous session

### Pitfall 6: Responsa Row Visible in Wrong Modes
**What goes wrong:** Responsa checkboxes appear in Fuzzy, Regex, Shelfmark, or Title modes
**Why it happens:** Incomplete mode checking in visibility logic
**How to avoid:** Whitelist eligible modes: `is_responsa_eligible = mode in ('exact', 'variants', 'variants_extended', 'variants_maximum')` (web) or `index in (0, 1)` (desktop)
**Warning signs:** Checkboxes visible when user selects Fuzzy search

### Pitfall 7: Explosion Warning Not Visible
**What goes wrong:** Warning is attached to `results[0]` but UI never reads it
**Why it happens:** Results processing code doesn't check for `responsa_warning` key
**How to avoid:** Check `results[0].get('responsa_warning')` immediately after search completes in both apps
**Warning signs:** User gets fewer results than expected but sees no explanation

## Code Examples

### Web: Mode Select Options (existing, for reference)
```python
# Source: web/pages/search.py line 371
mode_options = {
    'exact': tr('Exact') + ' (=)',
    'variants': tr('Variants Basic') + ' (?)',
    'variants_extended': tr('Variants Extended') + ' (??)',
    'variants_maximum': tr('Variants Maximum') + ' (???)',
    'fuzzy': tr('Fuzzy') + ' (~)',
    'Regex': tr('Regex') + ' (/)',
    'Shelfmark': tr('Shelfmark') + ' (#)',
    'Title': tr('Title') + ' ($)',
    'pgp_tags': tr('PGP Tags'),
}
```

### Web: Existing Visibility Toggle Pattern
```python
# Source: web/pages/search.py line 517
query_column.set_visibility(not is_tags)
tag_column.set_visibility(is_tags)
```

### Desktop: Mode Combo Items (existing)
```python
# Source: genizah_app.py line 6130
self.mode_combo.addItems([
    tr("Exact (=)"), tr("Variants (?)"), tr("Fuzzy (~)"),
    tr("Regex (/)"), tr("Title ($)"), tr("Shelfmark (#)"), tr("PGP Tags")
])
# Index: 0=Exact, 1=Variants, 2=Fuzzy, 3=Regex, 4=Title, 5=Shelfmark, 6=PGP Tags
self.MODE_PGP_TAGS = 6
```

### Desktop: Existing Mode Change Handler
```python
# Source: genizah_app.py line 12331
def _on_search_mode_changed(self, index):
    is_variants = (index == 1)
    is_pgp_tags = (index == self.MODE_PGP_TAGS)
    self.variant_controls_container.setVisible(is_variants and not is_pgp_tags)
    self.search_row1_container.setVisible(not is_pgp_tags)
    self.tag_search_combo.setVisible(is_pgp_tags)
    self.search_params_container.setVisible(not is_pgp_tags)
```

### Core: responsa_options Dict Structure (Phase 14)
```python
# Source: genizah_core.py line 5261-5274
responsa_options = {
    'responsa_mode': True,      # Master toggle
    'variants': True/False,     # Spelling variants
    'ja': True/False,           # Judeo-Arabic expansion
    'flex_spacing': True/False, # Flexible spacing
    'bidirectional': True/False,# Bidirectional gap
    'variant_mode': 'exact',    # Variant level (from mode dropdown pre-Responsa)
}
```

### Core: SearchThread Current Signature
```python
# Source: gui_threads.py line 31
def __init__(self, searcher, query, mode, gap, exclude_words=None):
```

### Web: Route Signature (current)
```python
# Source: web/main.py line 1828
@ui.page('/search')
def search_page_route(q: str = None, tag: str = None):
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `SearchThread(searcher, q, mode, gap)` | Add `responsa_options=None` kwarg | Phase 15 | Backward-compatible |
| `/search?q=&tag=` | `/search?q=&tag=&responsa=1&variants=1&ja=1&flex_spaces=1&bidirectional=1` | Phase 15 | URL state for Responsa |
| Mode dropdown always visible | Hidden when Responsa Mode ON | Phase 15 | Master toggle pattern |

## Key Design Decisions (Recommendations for Claude's Discretion Areas)

### Checkbox Order
**Recommendation:** Responsa Mode | Variants | Judeo-Arabic | Flex Spacing
**Rationale:** Follows the order from most commonly used to most specialized. Variants is the most basic enhancement, JA is specific to Arabic texts, Flex Spacing is niche.

### Bidirectional Gap Placement
**Recommendation:** Place in Advanced Options section (alongside Lab Mode and Exclude Words)
**Rationale:** Bidirectional gap is an advanced feature rarely needed. Keeping it in Advanced Options prevents cluttering the main row. Users who need it know to look in Advanced Options.

### Mode Dropdown Animation
**Recommendation:** No animation -- instant show/hide via `set_visibility()` (web) and `setVisible()` (desktop)
**Rationale:** Keeps implementation simple and consistent with existing patterns in the codebase (e.g., variant_controls_container visibility toggling). Animation adds complexity for minimal UX benefit.

### Visual Indicator for Active Responsa Mode
**Recommendation:** Add a small colored badge/label "Responsa" where the mode dropdown was, so users know the mode is active
**Rationale:** When the dropdown is hidden, users might be confused about what mode they're in. A visible badge clarifies.

**Web:**
```python
responsa_active_badge = ui.badge('Responsa', color='amber').props('outline').classes('text-sm')
responsa_active_badge.set_visibility(False)  # Shown when Responsa ON, hidden when OFF
```

**Desktop:**
```python
self.lbl_responsa_active = QLabel(tr("Responsa Mode"))
self.lbl_responsa_active.setStyleSheet("color: #f39c12; font-weight: bold;")
self.lbl_responsa_active.setVisible(False)  # Shown when mode combo is hidden
```

### Desktop Layout Adaptation
**Recommendation:** Add Responsa row as a new row between existing row2 (mode/params) and the results table
**Rationale:** Follows the existing multi-row layout pattern. Row 1 = query input, Row 2 = mode + params, Row 3 (new) = Responsa controls.

### Mobile Collapse
**Recommendation:** Use a single icon button (e.g., `tune` or `settings`) that expands/collapses the Responsa checkbox row. Apply Tailwind responsive classes.
**Rationale:** Consistent with the existing Advanced Options expansion pattern.

**Web:**
```python
# Wrap Responsa controls in a responsive container
# On desktop: show as flat row
# On mobile: collapse behind icon
with ui.row().classes('w-full items-center gap-3 hidden sm:flex'):
    # Full checkbox row (hidden on small screens)
    ...
with ui.row().classes('sm:hidden'):
    # Mobile: icon button that opens dialog/expansion
    ...
```

## Expanded Term Count: Core Change Needed

The core engine (`execute_search`) currently does NOT return expanded term count. The planner should include a small core task:

**In genizah_core.py `execute_search()`, after building `component_dicts` (around line 5350):**
```python
total_expanded = sum(len(cd['tantivy_terms']) for cd in component_dicts)
```

**Then after dedup (around line 5487):**
```python
if responsa_options and responsa_options.get('responsa_mode') and deduped:
    deduped[0]['responsa_expanded_count'] = total_expanded
```

This is a 3-line addition to the core, not a UI change, but it's needed for WEB-01 / DESK-01 requirement of showing expanded term count.

## Open Questions

1. **Web URL update approach**
   - What we know: The current app navigates to `/search?q=...&tag=...` for tag search, but regular searches store state in `app.storage.user` and don't navigate
   - What's unclear: Should Responsa params be in the URL (enabling sharing) or just in storage? The requirement says URL persistence (`?responsa=1&variants=1...`)
   - Recommendation: Use `history.replaceState` to update URL without page reload after each search. On page load, read URL params to restore state. This matches the requirement without forcing navigation.

2. **Mode restoration edge case**
   - What we know: User selects "Variants Extended", checks Responsa ON (dropdown hides), then unchecks (dropdown returns to "Variants Extended")
   - What's unclear: If user had slider mode enabled (single "Variants" option), does restoration work the same?
   - Recommendation: Store the raw mode_select value before hiding. Restore it on uncheck. Works for both slider and preset modes.

## Sources

### Primary (HIGH confidence)
- `web/pages/search.py` -- full source read, lines 1-2579
- `genizah_app.py` -- lines 6094-6430 (create_search_tab), 12331-12430 (_on_search_mode_changed), 12635-12735 (start_search), 12872-12920 (on_search_finished)
- `gui_threads.py` -- lines 1-100 (SearchThread class)
- `genizah_core.py` -- lines 4183-4250 (ResponsaComponent, parse_responsa_query), 5179-5220 (parse_query_syntax), 5222-5488 (execute_search)
- `web/main.py` -- line 1828 (search route signature)
- NiceGUI 3.5.0 installed (verified via import)

### Secondary (MEDIUM confidence)
- `.planning/phases/14-responsa-core-engine/14-02-PLAN.md` -- line 130 confirms responsa_warning attachment pattern

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new libraries, pure UI wiring
- Architecture: HIGH -- all patterns verified from actual source code
- Pitfalls: HIGH -- based on actual code analysis and known interaction patterns
- Core changes: HIGH -- verified execute_search API, responsa_options dict structure

**Research date:** 2026-02-09
**Valid until:** 2026-03-09 (stable, pure UI work)
