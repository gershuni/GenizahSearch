---
status: complete
phase: 16-tabular-query-builder
source: 16-01-SUMMARY.md, 16-02-SUMMARY.md, 16-03-SUMMARY.md
started: 2026-02-10T12:00:00Z
updated: 2026-02-10T12:00:00Z
---

## Current Test

number: done
name: All tests complete
status: complete

## Tests

### 1. Web Query Builder Button Visibility
expected: In the web app, select "Responsa (R)" from the Mode dropdown. A "Query Builder" button should appear in the Responsa sub-options row. Switching to any other mode should hide the button.
result: PASS
note: User wants the button label changed from "Query Builder" to "חיפוש טבלאי" (Tabular Search)

### 2. Web Query Builder Dialog Opens
expected: Click the "Query Builder" button. A dialog opens with component columns (starting with 2), word input fields, modifier checkboxes, distance spinners, scope toggle, preview area, and Apply/Cancel/Clear All buttons. Dialog should be responsive (not too wide on smaller screens).
result: PASS
note: UX enhancement needed — each word input should show a small indicator of which modifiers are active on it (so user doesn't have to click into each word to see). Also, modifier checkboxes should show meaningful Hebrew labels (קידומות, סיומות, מלא/חסר, etc.) not just symbols.

### 3. Web Builder Add/Remove Components
expected: The dialog starts with 2 component columns. Clicking "+" adds a 3rd, then 4th column. Clicking "x" on a component removes it. Minimum 2 components enforced (can't remove below 2). Maximum 4 components enforced (can't add beyond 4).
result: PASS

### 4. Web Builder Add/Remove Word Slots
expected: Each component starts with 2 word input slots. Clicking the add-word button adds more (up to 4). Clicking remove on a word slot removes it (minimum 2 enforced). Words within a component act as OR alternatives.
result: PASS
note: BUG — generated query is missing the last character of each word (truncated by one char)

### 5. Web Builder Modifier Checkboxes (Select-and-Modify)
expected: Click into a word input field. The shared modifier checkboxes (prefix #, suffix #, wildcard *, plene %, negation) should reflect that word's current modifiers. Toggling a checkbox updates that specific word's modifiers. Clicking a different word input switches the checkboxes to show that word's state.
result: PASS
note: UX enhancement — the focused word input should keep its highlighted border (glow) even when clicking on modifier checkboxes, so the user always knows which word they're modifying.

### 6. Web Builder Live Preview
expected: Type Hebrew words into word input slots. The preview area updates in real-time showing the generated Responsa syntax. Adding modifiers (e.g., checking "prefix" shows # before the word in preview). Multiple words in one component show as (word1/word2) in preview.
result: PASS

### 7. Web Builder Distance Spinners
expected: Distance spinners between components control the [N] gap notation. Changing a spinner value updates the preview to show [N] between the corresponding component terms. Distance 0 shows no bracket notation.
result: PASS

### 8. Web Builder Scope Toggle
expected: Toggling scope between "Word Range" and "Within Document" affects how distances are interpreted. In "Within Document" mode, distance spinners should be hidden.
result: PASS

### 9. Web Builder Apply Generates Syntax and Triggers Search
expected: Fill in at least 2 components with Hebrew words, click "Apply". The dialog closes, the generated Responsa syntax appears in the search text field, and a search is automatically triggered. Results should appear.
result: PASS

### 10. Web Builder Negated Words
expected: Check the "negation" modifier on a word, then click Apply. The negated word should appear in the exclude/not-words field (not in the main search text), and search results should exclude documents containing that word.
result: FAIL
note: Negated words don't appear in the exclude field. User also suggests that negation should use minus syntax (-word) directly in the generated query string rather than routing to a separate exclude field — more intuitive UX.

### 11. Web Builder Clear All
expected: Fill in several components with words and modifiers. Click "Clear All". All inputs reset to empty, all checkboxes uncheck, component count returns to default (2), but the dialog stays open.
result: PASS

### 12. Desktop Query Builder Button Visibility
expected: In the desktop app, select "Responsa (R)" from the Mode dropdown. A "Query Builder" button should appear in the Responsa sub-options row. Switching to another mode hides it.
result: PASS

### 13. Desktop Query Builder Dialog Opens
expected: Click the "Query Builder" button. A QDialog opens with 2-4 component columns, word inputs, modifier checkboxes, distance spinners, scope toggle, preview, and Apply/Cancel/Clear All buttons. RTL layout.
result: PASS
note: BUG — dialog styling doesn't adapt to dark mode. Needs dark theme support.

### 14. Desktop Builder Apply Generates Syntax and Triggers Search
expected: Fill in at least 2 components with Hebrew words in the desktop builder, click "Apply". The dialog closes, the Responsa syntax appears in the search field, negated words go to the exclude field, and search is auto-triggered.
result: PASS

### 15. Desktop Builder Select-and-Modify Pattern
expected: In the desktop builder, click into different word input fields. The shared modifier checkboxes switch context to show each word's modifiers. Toggling checkboxes updates that specific word only.
result: PASS

## Summary

total: 15
passed: 14
issues: 3
pending: 0
skipped: 0

## Gaps

### GAP-1: Last character of each word truncated in generated query
- **Source:** Test 4
- **Severity:** Bug (functional)
- **Description:** When the tabular builder generates the Responsa syntax, each word is missing its last character (e.g., "שלו" becomes "של").
- **Root Cause:** In `web/pages/search.py` ~line 1438, `_make_text_handler()` defines `handler()` with no parameters. NiceGUI's `update:model-value` event passes the new value as a parameter, but the handler ignores it and reads `input_el.value` which is stale (off by one keystroke due to Vue.js reactivity timing).
- **Fix:** Change handler to accept `value` parameter and use it directly:
  ```python
  def _make_text_handler(input_el, c_idx, w_idx):
      def handler(value):  # Accept value parameter
          on_word_text_change(c_idx, w_idx, value)  # Use value directly
      return handler
  ```
- **Files:** `web/pages/search.py`
- **Status:** Diagnosed, ready to fix

### GAP-2: Negated words not appearing in exclude field + design change
- **Source:** Test 10
- **Severity:** Bug + Design change
- **Description:** (A) Negated words don't appear in the exclude field after Apply. (B) User wants negation to use minus syntax (-word) directly in the generated query string rather than routing to a separate exclude field.
- **Root Cause:** Web `on_apply()` stores negated words in `search_state.builder_negated_words` (used internally by execute_search) but does NOT update the `not_filter` UI field — unlike desktop which correctly appends to `exclude_input`. The chain works functionally, but user gets no visual feedback.
- **Fix (quick — match desktop behavior):** In `on_apply()`, after getting `neg` from `generate_tabular_syntax()`, also set `not_filter.set_value()` with the negated words so they're visible in the exclude field.
- **Fix (design change — user preference):** Keep negated words in the generated syntax as `-word` inline. This requires: (1) modify `generate_tabular_syntax()` to embed `-word` instead of extracting, (2) add `-word` parsing to `parse_responsa_query()`, (3) handle negation in `build_regex_pattern()` and `execute_search()`. More involved but better UX.
- **Files:** `web/pages/search.py`, `genizah_core.py` (if design change)
- **Status:** Diagnosed, quick fix ready; design change needs planning

### GAP-3: Desktop dialog doesn't support dark mode
- **Source:** Test 13
- **Severity:** Bug (visual)
- **Description:** The TabularQueryBuilderDialog QDialog doesn't adapt its styling to dark mode.
- **Root Cause:** 6 hardcoded light-theme stylesheets in `genizah_app.py` ~lines 4473-4590:
  - Preview label: `background: #f8f9fa` (always light gray)
  - Component frames: `background: #fafafa` (always light)
  - Word inputs: `background: white` (white on dark = unreadable)
  - Button text colors: hardcoded blue (#2980b9) and red (#c0392b)
  - Distance labels: hardcoded gray (#7f8c8d)
- **Fix:** Add dark mode detection in `__init__` using `palette.color(QPalette.ColorRole.Window).lightness() < 128` (same pattern as rest of app, e.g., line 3358), then apply conditional stylesheets with dark-appropriate colors for all 6 locations.
- **Files:** `genizah_app.py`
- **Status:** Diagnosed, ready to fix

## UX Enhancements (non-blocking, from test notes)

### UX-1: Rename button to "חיפוש טבלאי"
- **Source:** Test 1
- **Files:** `web/pages/search.py`, `genizah_app.py`, `genizah_translations.py`

### UX-2: Per-word modifier indicators
- **Source:** Test 2
- **Description:** Small badge/indicator next to each word input showing active modifiers (so user doesn't need to click into each word to see its state)
- **Files:** `web/pages/search.py`, `genizah_app.py`

### UX-3: Hebrew modifier labels
- **Source:** Test 2
- **Description:** Modifier checkboxes should show meaningful Hebrew labels (קידומות, סיומות, מלא/חסר, etc.)
- **Files:** `web/pages/search.py`, `genizah_app.py`, `genizah_translations.py`

### UX-4: Focus glow persistence on modifier click
- **Source:** Test 5
- **Description:** Word input's highlighted border should persist when clicking modifier checkboxes
- **Files:** `web/pages/search.py`
