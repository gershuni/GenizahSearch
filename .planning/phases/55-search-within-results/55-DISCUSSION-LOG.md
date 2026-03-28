# Phase 55: Search Within Results - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-03-28
**Phase:** 55-search-within-results
**Areas discussed:** Trigger & entry point, Breadcrumb chain display, Search mode interaction, Clear & undo behavior

---

## Trigger & Entry Point

### Q1: Where should the trigger appear on web?

| Option | Description | Selected |
|--------|-------------|----------|
| Results header button | Button in results summary bar next to result count | ✓ |
| Reuse main search bar | Toggle/chip on search bar for refine mode | |
| Inline secondary search bar | Dedicated refinement input below results header | |

**User's choice:** Results header button
**Notes:** Most discoverable, consistent with existing header layout

### Q2: Where does user type refinement query?

| Option | Description | Selected |
|--------|-------------|----------|
| Focus main search bar | Activates restrict mode, focuses existing search bar | ✓ |
| Secondary input below header | Dedicated refinement input slides in | |

**User's choice:** Focus main search bar
**Notes:** Keeps UI clean, visual indicator shows refine mode is active

### Q3: Desktop pattern?

| Option | Description | Selected |
|--------|-------------|----------|
| Same pattern | Button in results header, refine mode on main search bar | ✓ |
| Toolbar button | Separate toolbar button for refine mode | |

**User's choice:** Same pattern as web

---

## Breadcrumb Chain Display

### Q1: How should refinement chain be displayed?

| Option | Description | Selected |
|--------|-------------|----------|
| Chip chain with arrows | Chips with › separator, reuses Phase 45 chip pattern | ✓ |
| Text breadcrumb | Plain text trail with > separators | |

**User's choice:** Chip chain with arrows

### Q2: Nesting depth limit?

| Option | Description | Selected |
|--------|-------------|----------|
| Unlimited | No artificial limit, chips scroll horizontally | ✓ |
| Cap at 5 levels | | |
| Cap at 3 levels | | |

**User's choice:** Unlimited

### Q3: Result count display on chips?

| Option | Description | Selected |
|--------|-------------|----------|
| Only final count | Count shown only for current step | ✓ |
| Count on every chip | Each chip shows its step count | |
| Count on hover/tooltip | Chips show query only, count on hover | |

**User's choice:** Only final count

---

## Search Mode Interaction

### Q1: Cross-mode refinement?

| Option | Description | Selected |
|--------|-------------|----------|
| Same mode only | Switching modes clears refinement chain | |
| Cross-mode allowed | Refine in any mode, restrict set is just sys_ids | ✓ |
| You decide | | |

**User's choice:** Cross-mode allowed

### Q2: Interaction with pre-search filters?

| Option | Description | Selected |
|--------|-------------|----------|
| Additive intersection | Refinement AND filters both apply, narrow together | ✓ |
| Refinement replaces filters | Starting refinement clears pre-search filters | |

**User's choice:** Additive intersection

### Q3: Show mode label on chips when cross-mode?

| Option | Description | Selected |
|--------|-------------|----------|
| Show mode on chip | Each chip shows mode when chain mixes modes | ✓ |
| No mode label | Chips show query text only | |
| You decide | | |

**User's choice:** Show mode on chip (only when chain mixes modes)

---

## Clear & Undo Behavior

### Q1: How should clearing work?

| Option | Description | Selected |
|--------|-------------|----------|
| Clear all + pop last | Clear all button + per-chip × (removing middle pops it and all after) | ✓ |
| Clear all only | Single clear all button, no per-chip removal | |

**User's choice:** Clear all + pop last

### Q2: Restore from cache or re-run?

| Option | Description | Selected |
|--------|-------------|----------|
| Re-run the search | Always re-execute earlier query, fresh results | ✓ |
| Cache and restore | Store result set at each step, restore instantly | |

**User's choice:** Re-run the search

### Q3: Persist in session state?

| Option | Description | Selected |
|--------|-------------|----------|
| Yes, persist | Save chain in session state, survives refresh/restart | ✓ |
| No, ephemeral only | Chain lost on refresh/restart | |

**User's choice:** Yes, persist

---

## Claude's Discretion

- Exact chip styling, colors, and layout details
- Breadcrumb bar position relative to results count
- RTL layout adjustments
- Visual indicator style for refine mode in search bar

## Deferred Ideas

None — discussion stayed within phase scope
