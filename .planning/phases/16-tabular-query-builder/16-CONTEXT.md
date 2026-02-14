# Phase 16: Tabular Query Builder — CONTEXT

> Decisions gathered 2026-02-10. Downstream agents: use these as constraints, not suggestions.

## What This Phase Builds

A visual tabular query builder (inspired by the Bar-Ilan Responsa Project's "חיפוש טבלאי") that lets users compose Responsa queries by filling in component columns with words and operators, instead of typing raw syntax. The builder generates syntax text that gets inserted into the search field.

**Platforms:** Web (NiceGUI) and Desktop (PyQt6) — both must be implemented.

---

## 1. Component Layout

### Word Slots
- **Start with 2 visible** word input slots per component
- **Expandable to 4** via a small "+" button
- Empty slots are ignored when generating syntax

### Components
- **Start with 2 components** when builder opens
- **Maximum 4 components** (can add via "+ component" button)
- **Minimum 2** — cannot remove below 2 components
- Remove button only visible on 3rd and 4th components

### Distance Between Components
- **Per-pair distance spinners** between each adjacent pair of components
- Spinner visually placed between component columns
- **Distance embedded in generated syntax** using bracket notation: `#word1 [3] word2*`
  - Parser must be extended to handle `[N]` notation between terms
  - MVP: simple `[N]` for word distance
  - Future: more complex notation like `[3:-3]` (Responsa Project advanced syntax)

### Scope
- Two scope modes:
  - **Word range** (default): uses per-pair distance spinners
  - **Within document** (בתוך מסמך): essentially AND logic, no distance constraint
- UI: radio/toggle that hides distance spinners when "within document" is selected
- Claude decides exact UI placement

### Builder Always Opens Empty
- No reverse-parsing of existing search text
- Builder is a **composition tool only** — always starts fresh

---

## 2. Per-Word Modifiers

### UX Pattern: Select-and-Modify
- **One shared set of modifier checkboxes** in the builder
- Checkboxes context-switch based on which **word input is currently focused**
- Changing checkboxes saves to that specific word
- Clicking a different word input updates checkboxes to show that word's settings
- **Standard focus styling** is sufficient — no extra highlight indicator needed

### Modifier Checkboxes (per word)
| Modifier | Symbol | Description |
|----------|--------|-------------|
| Grammatical prefixes | `#word` | Expand with ו/ה/ב/כ/ל/מ/ש etc. |
| Grammatical suffixes | `word#` | Expand with suffix forms |
| Wildcard prefix | `*word` | Ends with this word |
| Wildcard suffix | `word*` | Starts with this word |
| Plene/defective | `%word` | ו/י spelling variants |
| Negation | `✕` | Exclude this word (NOT) |

### NOT Included (Out of Scope)
- **Root search** (שורש) — requires morphological engine we don't have
- **Per-word variants** — variants stay as a global checkbox
- **Per-word JA** — JA stays as a global checkbox

### Global Options
- Global checkboxes (Variants, JA, Flex Spacing, Bidirectional) placement:
  - Claude decides whether to duplicate inside builder or keep in main search area only
  - Whichever produces cleaner UX

---

## 3. Builder Trigger & Flow

### Visibility
- Builder button/panel **only visible when Responsa mode is ON**
- No auto-enable — Responsa must already be checked

### Web Platform
- Claude decides: expansion panel OR dialog — whichever fits best
- **If expansion panel**: MUST auto-collapse when search runs so results are visible
- **If dialog**: closes on Apply

### Desktop Platform
- **QDialog** (confirmed from planning docs)
- Opens via "Query Builder" (בונה שאילתות) button

### Apply Action
- **Apply = populate text field + auto-trigger search**
- One-click workflow: user fills builder, clicks Apply, search runs immediately
- Responsa mode stays enabled

### Clear All
- **"Clear All" (ניקוי הכל) button** resets entire builder
- Clears all word inputs, unchecks all modifiers, resets distances to 0
- Does NOT close the builder

### One-Way Sync
- **Builder → text field only** (confirmed from planning docs)
- No reverse sync (text field → builder)

### Saved Queries
- **Not implemented now**
- Structure data model so saved/recalled tabular queries could be added later

---

## 4. Visual Styling

### Layout Direction
- **Full RTL** — Component 1 on the right, Component 4 on the left
- Distance spinners between components flow right-to-left
- Text inputs RTL

### Component Visual Style
- Claude decides: cards with borders vs. light separation
- Should look clean and modern — doesn't need to exactly match Responsa Project's dated UI

### Modifier Labels
- Claude decides labeling approach (full Hebrew, abbreviations with tooltips, or operator symbols)
- Priority: clarity and space efficiency

### Live Preview
- **Nice-to-have**: live-updating "translated query" text at bottom of builder
- **Skip if it creates complexity** — user accepted it may be dropped
- If included: read-only text area showing generated syntax, updates on every change

---

## 5. Syntax Extension: Gap Notation

### New Syntax: `[N]` Between Terms
The builder needs to generate per-pair distances. This requires extending the Responsa parser:

```
#(קוצץ/עוקר) [3] (עץ/אילן)* [2] #גן
```

- `[N]` between component groups specifies word distance for that pair
- Overrides the global Gap field for that specific pair
- Parser in `parse_responsa_query()` must handle `[N]` tokens
- If no `[N]` present, fall back to global Gap value

### "Within Document" Scope
- When scope is "within document", no `[N]` tokens generated
- Components connected by AND logic (both must appear anywhere in document)
- Generated syntax TBD by Claude (possibly just space-separated without `[N]`)

---

## 6. Platform-Specific Notes

### Web (NiceGUI)
- Builder type: Claude decides (expansion panel or dialog)
- Auto-collapse on search if expansion panel
- NiceGUI components: `ui.input`, `ui.checkbox`, `ui.number`, `ui.button`
- Must work on mobile (responsive)

### Desktop (PyQt6)
- Builder type: QDialog
- Components: QLineEdit, QCheckBox, QSpinBox, QPushButton
- RTL via `setLayoutDirection(Qt.LayoutDirection.RightToLeft)`
- Modal dialog: blocks main window until Apply/Cancel

---

## 7. Deferred Ideas (NOT This Phase)

- Root search (שורש) — needs morphological engine
- Complex gap notation `[3:-3]` — start simple with `[N]`
- Saved/recalled tabular queries — structure data model for later
- Sentence/paragraph scope — corpus lacks reliable structure
- Bidirectional sync (text field → builder) — too complex for MVP
- Desktop persistence of builder state — defaults on startup
