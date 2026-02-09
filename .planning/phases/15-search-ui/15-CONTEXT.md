# Phase 15: Search UI (Both Apps) - Context

**Gathered:** 2026-02-09
**Status:** Ready for planning

<domain>
## Phase Boundary

Add Responsa search checkboxes to both web and desktop apps. Wire checkboxes to the Phase 14 core engine. Handle mode switching, URL state, and PGP Tags mode interaction. No new search capabilities — this phase is purely UI wiring.

</domain>

<decisions>
## Implementation Decisions

### Checkbox Layout & Grouping
- New row below the search bar, dedicated to Responsa controls
- Flat row of checkboxes with labels — no visual container, border, or card
- Short labels with tooltips explaining each checkbox on hover
- Responsa row only visible in Exact and Variants search modes — hidden for Shelfmark, Title, PGP Tags, Fuzzy, Regex
- On mobile/narrow screens, collapse the checkbox row into an icon or expandable section instead of wrapping

### Checkbox Visibility & Hierarchy
- **Responsa Mode** checkbox is the only one visible by default (when in Exact/Variants modes)
- Checking Responsa Mode **reveals** sub-checkboxes (Variants, JA, Flex Spacing) and **hides** the mode dropdown
- Unchecking Responsa Mode **hides** sub-checkboxes and **restores** the mode dropdown
- This is a master toggle pattern: one checkbox controls visibility of the sub-options

### Mode Switching Behavior
- When Responsa Mode checked ON: mode dropdown hides, sub-checkboxes appear
- When Responsa Mode checked OFF: mode dropdown restores to the **previous** mode (remembered), sub-checkboxes hide
- The edge case of toggling Responsa from incompatible modes (Shelfmark/Title/PGP Tags) cannot happen — Responsa row is hidden in those modes
- In PGP Tags mode: all Responsa checkboxes are hidden (not disabled) — per prior decision

### Checkbox Defaults & Dependencies
- All checkboxes start unchecked on fresh page load
- No auto-enable: checking Responsa Mode does not auto-check any sub-options
- Sub-checkboxes (Variants, JA, Flex Spacing) only appear when Responsa Mode is ON
- Variants checkbox hidden when Responsa OFF — the dropdown's "Variants" mode serves that role instead
- Desktop: checkboxes reset to defaults on app startup (no persistence) — per prior decision

### Feedback & Status
- Explosion guard warning: inline message under the search bar, auto-dismisses after 5 seconds
- Results header shows expanded term count: e.g., "42 results (searching 150 expanded terms)"
- Same highlighting color for exact matches and expanded-term matches — no differentiation

### Claude's Discretion
- Checkbox order within the row
- Bidirectional Gap checkbox placement (Advanced Options or main row)
- Mode dropdown hide/show animation/transition style
- Visual indicator for active Responsa mode (when dropdown is hidden)
- Desktop layout adaptation based on existing search tab patterns
- Mobile collapse implementation (icon type, expand behavior)

</decisions>

<specifics>
## Specific Ideas

- Master toggle pattern: Responsa checkbox reveals/hides the sub-options row, similar to an "Advanced" toggle
- Mode dropdown remembers its pre-Responsa state and restores on unchecking
- Tooltips should explain what each expansion does in plain language (not technical terms)
- Expanded term count in results header gives users insight into search scope without overwhelming them

</specifics>

<deferred>
## Deferred Ideas

- None — discussion stayed within phase scope

</deferred>

---

*Phase: 15-search-ui*
*Context gathered: 2026-02-09*
