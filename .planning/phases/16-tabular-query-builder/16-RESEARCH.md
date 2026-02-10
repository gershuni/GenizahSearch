# Phase 16: Tabular Query Builder - Research

**Researched:** 2026-02-10
**Domain:** UI composition tool (NiceGUI web + PyQt6 desktop) + Responsa parser extension
**Confidence:** HIGH

## Summary

Phase 16 adds a visual tabular query builder that generates Responsa syntax text and inserts it into the search field. The builder must be implemented on both platforms: NiceGUI (web) and PyQt6 (desktop). The core engine also needs a parser extension to handle `[N]` gap notation between component groups.

This phase is primarily a UI task with a focused parser extension. The existing Responsa infrastructure (Phase 14-15) provides all the expansion, Tantivy query building, and regex pattern building. The tabular builder simply generates the syntax string that feeds into the existing `parse_responsa_query()` pipeline. The key technical challenges are: (1) the select-and-modify UX pattern where a shared set of checkboxes context-switches based on focused word input, (2) the `[N]` gap notation parser extension and its integration into `build_regex_pattern()` for per-pair distances, and (3) making the web builder mobile-responsive with RTL layout.

**Primary recommendation:** Use NiceGUI `ui.dialog()` for the web builder (not expansion panel) to avoid consuming vertical space above results. Implement the `[N]` gap notation in `_tokenize_responsa_query()` and thread per-pair gaps through `build_regex_pattern()` as a list of gap values instead of a single `max_gap` integer.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Component Layout:**
- Start with 2 visible word input slots per component, expandable to 4 via "+" button
- Start with 2 components when builder opens, maximum 4 components
- Minimum 2 components (cannot remove below 2)
- Remove button only visible on 3rd and 4th components
- Per-pair distance spinners between each adjacent pair of components
- Distance embedded in generated syntax using bracket notation: `#word1 [3] word2*`
- Two scope modes: Word range (default) and Within document (no distance)
- Builder always opens empty (no reverse-parsing)

**Per-Word Modifiers:**
- One shared set of modifier checkboxes in the builder
- Checkboxes context-switch based on which word input is currently focused
- Standard focus styling is sufficient
- Per-word modifiers: Grammatical prefixes (#word), Grammatical suffixes (word#), Wildcard prefix (*word), Wildcard suffix (word*), Plene/defective (%word), Negation (exclude)
- NOT included: Root search, per-word variants, per-word JA
- Global options (Variants, JA, Flex Spacing, Bidirectional) placement: Claude decides

**Builder Trigger & Flow:**
- Builder button/panel only visible when Responsa mode is ON
- Desktop: QDialog, modal, blocks main window
- Apply = populate text field + auto-trigger search
- Clear All resets entire builder, does NOT close builder
- One-way sync: builder -> text field only
- Saved queries: not implemented now, but structure data model for later

**Visual Styling:**
- Full RTL layout (Component 1 on right, Component 4 on left)
- Live preview: nice-to-have, skip if it creates complexity

**Syntax Extension:**
- New `[N]` notation between terms for per-pair distances
- Parser in `parse_responsa_query()` must handle `[N]` tokens
- "Within document" scope: no `[N]` tokens, AND logic

**Platforms:**
- Web: NiceGUI, must work on mobile
- Desktop: PyQt6, QDialog with QLineEdit, QCheckBox, QSpinBox, QPushButton

### Claude's Discretion

- Web platform: expansion panel OR dialog (whichever fits best)
- Component visual style: cards with borders vs. light separation
- Modifier labels: full Hebrew, abbreviations with tooltips, or operator symbols
- Live preview: include or skip based on complexity
- Global options placement: duplicate inside builder or keep in main search area only
- "Within document" generated syntax format
- UI placement for scope toggle (radio/toggle)

### Deferred Ideas (OUT OF SCOPE)

- Root search (needs morphological engine)
- Complex gap notation `[3:-3]` (start simple with `[N]`)
- Saved/recalled tabular queries (structure data model for later)
- Sentence/paragraph scope (corpus lacks reliable structure)
- Bidirectional sync (text field -> builder) (too complex for MVP)
- Desktop persistence of builder state (defaults on startup)
</user_constraints>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| NiceGUI | Current (project already uses) | Web UI components for builder | Already in project, provides `ui.dialog`, `ui.input`, `ui.checkbox`, `ui.number`, `ui.button` |
| PyQt6 | Current (project already uses) | Desktop UI for QDialog builder | Already in project, provides QDialog, QLineEdit, QCheckBox, QSpinBox, QGridLayout |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `re` (stdlib) | N/A | Parser extension for `[N]` gap notation | Tokenizer regex in `_tokenize_responsa_query()` |
| `dataclasses` (stdlib) | N/A | Data model for tabular query state | Internal state representation |

### No New Dependencies Required
This phase uses only existing project dependencies. No new packages need to be installed.

## Architecture Patterns

### Recommended Structure

The tabular builder spans four areas of the codebase:

```
genizah_core.py
  ├── ResponsaComponent (existing dataclass — no changes needed)
  ├── parse_responsa_query() — extend to handle [N] tokens
  ├── _tokenize_responsa_query() — extend to emit [N] as separate tokens
  ├── build_regex_pattern() — extend to accept per-pair gap list
  └── build_tantivy_query() — no changes (AND between components is gap-agnostic)

web/pages/search.py
  ├── Query Builder button (in responsa_sub_row area)
  └── Query Builder dialog function (new, ~200-300 lines)

genizah_app.py
  ├── Query Builder button (in responsa_sub_row area)
  └── TabularQueryBuilderDialog (new QDialog class, ~200-300 lines)

genizah_translations.py
  └── New translation keys for builder UI strings
```

### Pattern 1: Syntax Generation (Builder -> Text)

**What:** Convert tabular builder state into Responsa syntax string
**When to use:** When user clicks Apply in the builder

The builder generates syntax text following this pattern:
```python
def generate_syntax(components, distances, scope):
    """Generate Responsa syntax from tabular builder state.

    Args:
        components: List of dicts, each with:
            - words: List of (word_text, modifiers_dict) tuples
            - modifiers are per-word: {prefix: bool, suffix: bool,
              wildcard_prefix: bool, wildcard_suffix: bool,
              plene: bool, negation: bool}
        distances: List of ints (len = len(components) - 1)
        scope: 'word_range' or 'within_document'

    Returns:
        Syntax string like: '#(word1/word2) [3] word3*'
    """
    parts = []
    for i, comp in enumerate(components):
        words_with_mods = []
        for word, mods in comp['words']:
            if not word.strip():
                continue
            decorated = word
            if mods.get('plene'):
                decorated = '%' + decorated
            if mods.get('prefix'):
                decorated = '#' + decorated
            if mods.get('suffix_gram'):
                decorated = decorated + '#'
            if mods.get('wildcard_prefix'):
                decorated = '*' + decorated
            if mods.get('wildcard_suffix'):
                decorated = decorated + '*'
            # Negation handled separately (NOT in syntax,
            # excluded via exclude_words mechanism)
            words_with_mods.append(decorated)

        if len(words_with_mods) > 1:
            part = f"({'/'.join(words_with_mods)})"
        elif words_with_mods:
            part = words_with_mods[0]
        else:
            continue

        parts.append(part)

        # Add distance notation between components
        if scope == 'word_range' and i < len(distances):
            dist = distances[i]
            if dist > 0:
                parts.append(f'[{dist}]')

    return ' '.join(parts)
```

### Pattern 2: Select-and-Modify UX (Shared Checkboxes)

**What:** One shared set of modifier checkboxes that context-switches based on focused word input
**When to use:** For the per-word modifier checkboxes in both web and desktop

```python
# State tracking for select-and-modify pattern
# Each word input has its own modifier state stored in a dict
word_modifiers = {}  # key: (component_idx, word_idx), value: {prefix: bool, ...}
active_word = None   # Currently focused word key

def on_word_focus(comp_idx, word_idx):
    """Update shared checkboxes to reflect the focused word's state."""
    global active_word
    active_word = (comp_idx, word_idx)
    mods = word_modifiers.get(active_word, default_modifiers())
    # Update each checkbox to show this word's state
    prefix_cb.value = mods.get('prefix', False)
    suffix_cb.value = mods.get('suffix_gram', False)
    # ... etc

def on_modifier_change(modifier_name, value):
    """Save modifier change to the currently focused word."""
    if active_word is None:
        return
    if active_word not in word_modifiers:
        word_modifiers[active_word] = default_modifiers()
    word_modifiers[active_word][modifier_name] = value
```

### Pattern 3: Parser Extension for [N] Gap Notation

**What:** Extend the Responsa tokenizer to handle `[N]` tokens between component groups
**When to use:** In `_tokenize_responsa_query()` and `build_regex_pattern()`

The current tokenizer splits on whitespace respecting parentheses. The `[N]` token is just another whitespace-delimited token that matches the pattern `\[\d+\]`. The parser needs to:

1. Recognize `[N]` tokens in the token stream
2. Extract them as gap annotations (not as ResponsaComponent objects)
3. Thread per-pair gaps into `build_regex_pattern()` which currently takes a single `max_gap` int

```python
# In parse_responsa_query() — return type changes:
# Before: List[ResponsaComponent]
# After:  Tuple[List[ResponsaComponent], List[Optional[int]]]
#   where gaps[i] = gap between component[i] and component[i+1]
#   gaps has length len(components) - 1

# Or alternatively: keep parse_responsa_query() returning components only,
# and add a separate function to extract gaps from tokens.

# In build_regex_pattern() — accept gaps list:
def build_regex_pattern(self, terms, mode, max_gap,
                        responsa_components=None, responsa_options=None,
                        per_pair_gaps=None):
    # If per_pair_gaps provided, use per_pair_gaps[i] between parts[i] and parts[i+1]
    # Otherwise fall back to max_gap for all pairs
```

### Pattern 4: NiceGUI Dialog (Web Builder)

**What:** Use `ui.dialog()` for the web tabular builder
**When to use:** When Responsa mode is active and user clicks the builder button

The project already uses `ui.dialog()` extensively (40+ instances found in the codebase). The dialog pattern is well-established:

```python
# Existing pattern from web/pages/search.py:930
with ui.dialog() as dialog, ui.card().classes('p-6 min-w-96'):
    h3(tr('Title'), classes='text-xl font-bold mb-2')
    # ... dialog content ...
    with ui.row().classes('w-full justify-end gap-2 mt-4'):
        ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
        ui.button(tr('Apply'), on_click=on_apply).classes('btn-primary')
dialog.open()
```

**Recommendation: Dialog over expansion panel** because:
1. The builder is complex (multiple components, checkboxes, spinners) and would consume significant vertical space above results
2. An expansion panel that auto-collapses on search would lose the builder state visually, making iteration harder
3. The dialog pattern matches the desktop QDialog approach, reducing cognitive load for dual-platform users
4. Dialog closes on Apply (which also triggers search), giving immediate results visibility
5. Mobile: a dialog can use `.props('maximized')` on small screens for full-screen experience

### Pattern 5: PyQt6 QDialog (Desktop Builder)

**What:** Modal QDialog for the desktop tabular builder
**When to use:** Confirmed by user decision

The project has 10+ existing QDialog subclasses. The established pattern:

```python
class TabularQueryBuilderDialog(QDialog):
    def __init__(self, parent):
        super().__init__(parent)
        self.setWindowTitle(tr("Query Builder"))
        self.resize(700, 500)
        if CURRENT_LANG == 'he':
            self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        layout = QVBoxLayout(self)
        # ... build UI ...

        # Bottom buttons
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def get_syntax(self) -> str:
        """Return the generated syntax string."""
        return self._generate_syntax()
```

### Anti-Patterns to Avoid

- **Storing builder state in URL:** The CONTEXT.md explicitly states "URL: text + checkbox states only, no tabular state." The builder is ephemeral.
- **Bidirectional sync:** Do NOT try to parse text field content back into builder state. One-way only (builder -> text field).
- **Persisting desktop builder state:** Builder always opens empty. No saving/restoring between sessions.
- **Adding per-word Variants or JA:** These remain global checkboxes only. Do not add them to the per-word modifier set.
- **Modifying ResponsaComponent dataclass:** The existing dataclass already has all needed fields (words, grammatical_prefixes, grammatical_suffixes, plene_defective, wildcard, wildcard_pattern). No changes needed to the dataclass itself.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| RTL layout | Custom CSS/JS for RTL | NiceGUI `.style('direction: rtl;')` + PyQt6 `setLayoutDirection(Qt.LayoutDirection.RightToLeft)` | Both frameworks have native RTL support already used in the project |
| Dialog management | Custom modal overlay | `ui.dialog()` (NiceGUI) / `QDialog` (PyQt6) | Established patterns with 40+ examples in codebase |
| Focus tracking (web) | Custom JS focus management | NiceGUI `.on('focus', handler)` events | Native browser focus events work reliably |
| Negation | New syntax operator | Existing `exclude_words` parameter in `execute_search()` | The exclude_words mechanism already filters results containing specific words |

**Key insight:** The builder is a pure composition tool. It generates a syntax string that feeds into the existing pipeline. Do not add new search paths or bypass the existing `parse_responsa_query() -> expand -> build_tantivy/regex -> execute_search` flow.

## Common Pitfalls

### Pitfall 1: Per-Pair Gaps Breaking the Regex Builder

**What goes wrong:** The current `build_regex_pattern()` uses a single `max_gap` to build one separator pattern, then joins all component regex parts with that same separator. With per-pair gaps, each pair needs a different separator.
**Why it happens:** The separator is currently a single string applied uniformly via `sep.join(parts)`.
**How to avoid:** Replace `sep.join(parts)` with a loop that builds the regex string by concatenating `parts[i] + sep_for_gap_i + parts[i+1]`. When bidirectional is enabled, reverse both the parts AND the gap list.
**Warning signs:** All searches returning results as if gap were uniform.

### Pitfall 2: Focus Event Ordering on Web

**What goes wrong:** When the user clicks from one word input to another, the blur event of the old input fires before (or after) the focus event of the new input. If modifier state is saved on blur and loaded on focus, race conditions can occur.
**Why it happens:** Browser event ordering between blur and focus across different elements is well-defined (blur fires first), but NiceGUI's event propagation can add latency.
**How to avoid:** Save modifier state on every checkbox change (not on blur), and load state on focus only. The active_word tracker is set on focus and read on checkbox change.
**Warning signs:** Modifier checkboxes showing wrong state when clicking between word inputs.

### Pitfall 3: Negation Syntax Ambiguity

**What goes wrong:** The CONTEXT.md lists Negation as a per-word modifier but there is no existing negation syntax operator in the Responsa parser.
**Why it happens:** The current exclude_words mechanism is a post-filter on results, not a per-component feature.
**How to avoid:** For MVP, implement negation by collecting all negated words from the builder and injecting them into the `exclude_words` parameter of `execute_search()`. Do NOT add a new negation syntax operator. The builder extracts negated words and passes them separately alongside the generated syntax.
**Warning signs:** Trying to add `-word` or `!word` syntax to the parser.

### Pitfall 4: Empty Word Slots Generating Invalid Syntax

**What goes wrong:** Empty word inputs generate `()` or bare operators like `#` in the syntax.
**Why it happens:** Not filtering empty word values before syntax generation.
**How to avoid:** Always filter out empty/whitespace-only word slots before generating syntax. If all word slots in a component are empty, skip that component entirely.
**Warning signs:** Parser errors or empty search results after using builder with partially-filled components.

### Pitfall 5: "Within Document" Scope Not Generating Correct Syntax

**What goes wrong:** "Within document" mode should produce AND logic (no distance constraint), but the generated syntax might still include `[N]` tokens or use the global Gap value.
**Why it happens:** Incomplete scope mode handling in the syntax generator.
**How to avoid:** When scope is "within_document", generate syntax WITHOUT any `[N]` tokens. The existing Responsa pipeline already uses AND between components in the Tantivy query. For regex, when no gaps are specified, use the global Gap value from the Gap spinner (which the user can set to a large number for "anywhere in document" matching). Alternatively, generate the syntax as normal but set per-pair gaps to a very large value or use a special marker.
**Warning signs:** "Within document" searches still respecting distance constraints.

### Pitfall 6: Web Dialog Losing State on Re-open

**What goes wrong:** The builder always opens empty (per CONTEXT.md), but if the dialog is created once and re-opened, it retains previous state.
**Why it happens:** NiceGUI dialogs are persistent DOM elements.
**How to avoid:** Either create a new dialog each time (destroying the old one), or explicitly clear all inputs and reset all checkboxes when the dialog opens.
**Warning signs:** Builder showing previous query when reopened.

### Pitfall 7: Mobile Layout Breaking

**What goes wrong:** The tabular builder with 2-4 components side by side doesn't fit on mobile screens.
**Why it happens:** Fixed-width columns exceeding viewport width.
**How to avoid:** On mobile, stack components vertically instead of horizontally. Use NiceGUI's responsive classes (`flex-wrap`) and consider `.props('maximized')` for the dialog on mobile. Distance spinners can be placed above/below instead of between components when stacked.
**Warning signs:** Horizontal scrolling or clipped content on phone screens.

## Code Examples

### Example 1: Web Query Builder Dialog

```python
# Pattern for the web builder dialog (NiceGUI)
def open_query_builder():
    """Open the tabular query builder dialog."""
    # State for the builder
    builder_state = {
        'components': [
            {'words': [{'text': '', 'mods': {}} for _ in range(2)] + [None, None]},  # 2 visible, 2 hidden
            {'words': [{'text': '', 'mods': {}} for _ in range(2)] + [None, None]},
        ],
        'distances': [0],  # Between comp 0 and comp 1
        'scope': 'word_range',
        'active_word': None,  # (comp_idx, word_idx) tuple
        'num_components': 2,
    }

    with ui.dialog() as dialog, ui.card().classes('p-6').style('min-width: 600px; direction: rtl;'):
        h3(tr('Query Builder'), classes='text-xl font-bold mb-4')

        # Scope toggle
        scope_toggle = ui.toggle(
            {
                'word_range': tr('Word Range'),
                'within_document': tr('Within Document'),
            },
            value='word_range'
        ).classes('mb-4')

        # Components row (RTL: right to left)
        components_row = ui.row().classes('w-full gap-4 flex-wrap justify-end')

        # ... component cards with word inputs ...

        # Shared modifier checkboxes
        with ui.row().classes('w-full gap-4 mt-4 items-center'):
            ui.label(tr('Modifiers:')).classes('text-sm font-medium')
            prefix_cb = ui.checkbox(tr('Prefixes') + ' #')
            suffix_cb = ui.checkbox(tr('Suffixes') + ' #')
            wild_prefix_cb = ui.checkbox(tr('Wildcard') + ' *_')
            wild_suffix_cb = ui.checkbox(tr('Wildcard') + ' _*')
            plene_cb = ui.checkbox(tr('Plene/Def') + ' %')
            negation_cb = ui.checkbox(tr('Exclude') + ' -')

        # Bottom buttons
        with ui.row().classes('w-full justify-end gap-2 mt-4'):
            ui.button(tr('Clear All'), on_click=clear_all).props('flat')
            ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
            ui.button(tr('Apply'), on_click=on_apply).classes('btn-primary')

    dialog.open()
```

### Example 2: Desktop Query Builder Dialog

```python
# Pattern for the desktop builder dialog (PyQt6)
class TabularQueryBuilderDialog(QDialog):
    def __init__(self, parent):
        super().__init__(parent)
        self.setWindowTitle(tr("Query Builder"))
        self.setMinimumSize(700, 500)
        if CURRENT_LANG == 'he':
            self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        main_layout = QVBoxLayout(self)

        # Scope radio buttons
        scope_layout = QHBoxLayout()
        self.radio_word_range = QRadioButton(tr("Word Range"))
        self.radio_word_range.setChecked(True)
        self.radio_within_doc = QRadioButton(tr("Within Document"))
        scope_layout.addWidget(self.radio_word_range)
        scope_layout.addWidget(self.radio_within_doc)
        main_layout.addLayout(scope_layout)

        # Components area (using QGridLayout for flexible column arrangement)
        self.components_grid = QGridLayout()
        main_layout.addLayout(self.components_grid)

        # Shared modifier checkboxes
        mods_layout = QHBoxLayout()
        self.chk_prefix = QCheckBox(tr("Prefixes") + " #")
        self.chk_suffix = QCheckBox(tr("Suffixes") + " #")
        # ... etc
        main_layout.addLayout(mods_layout)

        # Buttons
        btn_layout = QHBoxLayout()
        btn_clear = QPushButton(tr("Clear All"))
        btn_clear.clicked.connect(self.clear_all)
        btn_layout.addWidget(btn_clear)
        btn_layout.addStretch()
        btn_cancel = QPushButton(tr("Cancel"))
        btn_cancel.clicked.connect(self.reject)
        btn_apply = QPushButton(tr("Apply"))
        btn_apply.clicked.connect(self.accept)
        btn_layout.addWidget(btn_cancel)
        btn_layout.addWidget(btn_apply)
        main_layout.addLayout(btn_layout)
```

### Example 3: Parser Extension for [N] Gaps

```python
# Extending _tokenize_responsa_query to handle [N] tokens
# [N] tokens are already naturally separated by whitespace,
# so the existing tokenizer will emit them as tokens like "[3]"

import re
GAP_TOKEN_RE = re.compile(r'^\[(\d+)\]$')

def parse_responsa_query_with_gaps(query_str):
    """Parse Responsa query, extracting both components and per-pair gaps.

    Returns:
        Tuple of (components: List[ResponsaComponent], gaps: List[Optional[int]])
        gaps[i] is the gap between component[i] and component[i+1]
        None means "use global gap value"
    """
    tokens = _tokenize_responsa_query(query_str.strip())

    components = []
    gaps = []
    last_was_component = False

    for token in tokens:
        if not token:
            continue

        gap_match = GAP_TOKEN_RE.match(token)
        if gap_match:
            # This is a gap token [N]
            gap_value = int(gap_match.group(1))
            if last_was_component and len(gaps) == len(components) - 1:
                gaps.append(gap_value)
            last_was_component = False
        else:
            # This is a regular component token
            if last_was_component and len(gaps) < len(components):
                # No gap token between two components -> use global gap
                gaps.append(None)
            components.append(_parse_single_token(token))
            last_was_component = True

    # Pad gaps list if needed
    while len(gaps) < len(components) - 1:
        gaps.append(None)

    return components, gaps
```

### Example 4: Per-Pair Gap in Regex Builder

```python
# In build_regex_pattern, Responsa branch:
# Replace:   pattern_str = sep.join(parts)
# With:

def _build_regex_with_per_pair_gaps(parts, per_pair_gaps, max_gap, flex_spacing, bidirectional):
    """Build regex pattern with per-pair gap separators."""
    def make_sep(gap_value):
        if gap_value == 0:
            if flex_spacing:
                return r'[^\w\u0590-\u05FF\']*'
            else:
                return r'[^\w\u0590-\u05FF\']+'
        else:
            return rf'(?:[^\w\u0590-\u05FF\']+{Config.WORD_TOKEN_PATTERN}){{0,{gap_value}}}[^\w\u0590-\u05FF\']+'

    # Build forward pattern
    forward_parts = [parts[0]]
    for i in range(1, len(parts)):
        gap = per_pair_gaps[i-1] if per_pair_gaps and i-1 < len(per_pair_gaps) and per_pair_gaps[i-1] is not None else max_gap
        forward_parts.append(make_sep(gap))
        forward_parts.append(parts[i])
    forward = ''.join(forward_parts)

    if bidirectional and len(parts) >= 2:
        # Reverse: both parts and gaps
        rev_parts = [parts[-1]]
        for i in range(len(parts) - 2, -1, -1):
            gap = per_pair_gaps[i] if per_pair_gaps and i < len(per_pair_gaps) and per_pair_gaps[i] is not None else max_gap
            rev_parts.append(make_sep(gap))
            rev_parts.append(parts[i])
        backward = ''.join(rev_parts)
        return f"({forward})|({backward})"

    return forward
```

## Discretion Recommendations

### Web Platform: Dialog (not expansion panel)

**Recommendation:** Use `ui.dialog()`.

Rationale:
1. The builder is complex UI (2-4 component columns, word inputs, checkboxes, spinners, scope toggle, action buttons). As an expansion panel, this would consume 300-400px of vertical space above results.
2. The existing Advanced Options expansion panel already occupies that area; stacking another panel below creates awkward nesting.
3. Dialog closes on Apply and auto-triggers search, giving immediate results visibility.
4. On mobile, `.props('maximized')` makes the dialog full-screen for easy use.
5. Matches desktop QDialog pattern (cross-platform UX consistency).
6. The user explicitly said "If dialog: closes on Apply" -- this is the simpler flow.

### Component Visual Style: Light cards with subtle borders

**Recommendation:** Use `ui.card().classes('p-3').style('border: 1px solid var(--border-light); border-radius: 8px;')` on web, `QGroupBox` or a styled `QFrame` on desktop. Light, modern separation that doesn't overpower the content.

### Modifier Labels: Operator symbols with tooltips

**Recommendation:** Use compact operator symbols as labels (`# _`, `_ #`, `* _`, `_ *`, `%`, `x`) with Hebrew tooltips explaining each. This saves space (critical with 6 checkboxes in a shared row) while remaining clear. Example:
- `# _` tooltip: "קידומות דקדוקיות (ו/ה/ב/כ/ל/מ/ש)"
- `_ #` tooltip: "סיומות דקדוקיות"
- `* _` tooltip: "תחילית כוכבית — מילים שמסתיימות ב..."
- `_ *` tooltip: "סיומת כוכבית — מילים שמתחילות ב..."
- `%` tooltip: "כתיב מלא/חסר (ו/י)"
- `x` tooltip: "שלילה — הוצא מילה זו מהתוצאות"

### Live Preview: Include it

**Recommendation:** Include the live preview. It is straightforward to implement: call the same `generate_syntax()` function on every input/checkbox change and display the result in a read-only `ui.input` or `ui.label`. The complexity is minimal (one event handler, one display element). The preview helps users understand the syntax being generated and builds confidence in the builder.

### Global Options Placement: Keep in main search area only

**Recommendation:** Do NOT duplicate Variants/JA/Flex Spacing/Bidirectional checkboxes inside the builder dialog. These are already visible in the `responsa_sub_row` and remain active after the dialog closes. Duplicating them creates sync complexity and confusion about which set controls the search. The builder generates the syntax text only; global options are applied by the existing pipeline during execution.

### "Within Document" Syntax: Space-separated without [N]

**Recommendation:** When scope is "within_document", generate syntax as plain space-separated components without any `[N]` tokens. The existing pipeline treats space-separated components with AND logic in Tantivy. For the regex gap, when no `[N]` tokens are present, use the global Gap value from the Gap spinner. The user can set Gap to a large value (e.g., 10) to approximate "within document" semantics. This avoids adding a new scope concept to the parser.

### Scope Toggle Placement: Above components row

**Recommendation:** Place the scope toggle (radio buttons or NiceGUI `ui.toggle`) at the top of the builder dialog, above the component columns. This establishes context before the user starts filling in components. When "within document" is selected, hide the distance spinners between components.

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Raw syntax typing | Tabular builder + syntax generation | This phase | Users can compose queries visually |
| Single global gap | Per-pair `[N]` gap notation | This phase | Different distances between different component pairs |
| Mode dropdown for Responsa | Mode dropdown (Phase 15 decision) | Phase 15 | Builder button appears in responsa_sub_row |

## Open Questions

1. **Negation implementation detail**
   - What we know: Negation is listed as a per-word modifier. The existing `exclude_words` mechanism filters results post-search.
   - What's unclear: Should negated words be extracted from the builder and injected into `exclude_words`, or should we add negation syntax to the parser?
   - Recommendation: Use `exclude_words` injection. Extract negated words from builder state before generating syntax, pass them separately. This avoids parser changes and reuses existing infrastructure.

2. **"Within document" regex gap handling**
   - What we know: No `[N]` tokens means fall back to global Gap. But "within document" semantically means "anywhere in the same document."
   - What's unclear: The global Gap spinner max is 10 words. "Within document" could mean thousands of words apart.
   - Recommendation: When scope is "within_document", do NOT use regex proximity at all. Instead, modify the Responsa pipeline to use Tantivy AND logic only (no regex proximity filtering). This could be done by passing a `scope='document'` flag in `responsa_options` that causes `build_regex_pattern` to match each component independently and check that ALL match somewhere in the document. Alternative: set max_gap to a very large number and accept that regex might miss very distant matches.

3. **Data model for future saved queries**
   - What we know: CONTEXT.md says "structure data model so saved/recalled tabular queries could be added later."
   - What's unclear: What storage format? JSON? Supabase table?
   - Recommendation: Define a simple dict/JSON schema for the builder state: `{components: [{words: [{text, mods}], visible_count}], distances: [int], scope: str}`. Don't implement storage, but ensure the internal state uses this structure so it could be serialized later.

## Integration Points

### Where to Add the Builder Button (Web)

Location: `web/pages/search.py`, line ~484-509 (inside `responsa_sub_row`).

Add a "Query Builder" button to the right side of the `responsa_sub_row` (the row containing Variants, JA, Flex Spacing, Bidirectional checkboxes):

```python
# After the syntax legend in responsa_sub_row (around line 508):
ui.button(tr('Query Builder'), icon='grid_view',
          on_click=open_query_builder).classes('ml-auto').props('outline dense')
```

### Where to Add the Builder Button (Desktop)

Location: `genizah_app.py`, line ~6325-6330 (inside `responsa_sub_layout`).

Add a "Query Builder" QPushButton to the `responsa_sub_layout`:

```python
# After syntax_legend in responsa_sub_layout:
self.btn_query_builder = QPushButton(tr("Query Builder"))
self.btn_query_builder.setToolTip(tr("Open the tabular query builder"))
self.btn_query_builder.clicked.connect(self.open_query_builder)
responsa_sub_layout.addWidget(self.btn_query_builder)
```

### Where to Inject Generated Syntax (Web)

After the dialog's Apply action:
```python
def on_apply():
    syntax = generate_syntax(builder_state)
    query_input.set_value(syntax)  # One-way sync: builder -> text field
    dialog.close()
    execute_search()  # Auto-trigger search
```

### Where to Inject Generated Syntax (Desktop)

After the QDialog's accept:
```python
def open_query_builder(self):
    dlg = TabularQueryBuilderDialog(self)
    if dlg.exec() == QDialog.DialogCode.Accepted:
        syntax = dlg.get_syntax()
        self.search_input.setText(syntax)
        self.start_search()  # Auto-trigger search
```

### Parser Extension Location

In `genizah_core.py`, the changes are:

1. **`_tokenize_responsa_query()` (line ~4245):** No changes needed. The tokenizer already splits `[3]` as a separate token since `[` and `]` are not parentheses and `[3]` has no internal spaces.

2. **`parse_responsa_query()` (line ~4202):** Add gap token recognition. Either modify return type to include gaps, or add a parallel function `extract_per_pair_gaps()`.

3. **`execute_search()` (line ~5294, Responsa pipeline):** Call the gap extraction function, pass gaps to `build_regex_pattern()`.

4. **`build_regex_pattern()` (line ~4948, Responsa branch):** Accept and use per-pair gap list instead of uniform `max_gap`.

## Sources

### Primary (HIGH confidence)
- Codebase direct inspection: `genizah_core.py`, `web/pages/search.py`, `genizah_app.py`, `gui_threads.py`, `web/translations.py`, `genizah_translations.py`
- Existing planning docs: `docs/plans/responsa-search/06_ui_integration_sketch.md` (Option IIb sketch)
- Phase 14-15 summaries in `.planning/phases/`
- CONTEXT.md for Phase 16 (user decisions)

### Secondary (MEDIUM confidence)
- NiceGUI `ui.dialog()` API — verified via 40+ usage instances in the codebase
- PyQt6 QDialog patterns — verified via 10+ existing QDialog subclasses in `genizah_app.py`

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - uses only existing project dependencies
- Architecture: HIGH - patterns verified against 40+ existing instances in codebase
- Pitfalls: HIGH - derived from direct code analysis of existing Responsa pipeline
- Parser extension: HIGH - `_tokenize_responsa_query()` code inspected, `[N]` handling straightforward
- Select-and-modify UX: MEDIUM - focus event handling patterns are standard but NiceGUI-specific behavior needs validation during implementation

**Research date:** 2026-02-10
**Valid until:** 2026-03-10 (stable domain, no external dependencies)
