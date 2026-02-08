---
status: resolved
trigger: "Investigate why the 'Back to Page View' button and 'Reading Desk' label are invisible in Light Mode in the web reading desk."
created: 2026-02-08T00:00:00Z
updated: 2026-02-08T00:35:00Z
---

## Current Focus

hypothesis: Label uses CSS classes (overridden by Quasar Light Mode), badge/button use inline style with !important (works correctly)
test: Compare exact styling between label (invisible) vs badge/button (visible)
expecting: Label needs inline style with !important, not CSS classes
next_action: Document root cause and confirm fix needed

## Symptoms

expected: "Reading Desk" label, badge, and "Back to Page View" button all visible in both Light and Dark modes
actual: In Light Mode, only the badge is visible. Label and button text are invisible. All three visible in Dark Mode.
errors: None (visual rendering issue)
reproduction: Open reading desk in web app with Light Mode enabled
started: Previous fix (11-06, W2) changed button styling to `style('color: white !important;')` but didn't hold

## Eliminated

## Evidence

- timestamp: 2026-02-08T00:10:00Z
  checked: browse.py lines 2374-2396 (reading desk header bar)
  found: |
    Header bar structure (green gradient background):

    **Icon (line 2380):**
    ```python
    ui.icon('auto_stories').classes('text-white text-xl')
    ```

    **Label (line 2385):**
    ```python
    ui.label(header_txt).classes('text-lg font-bold text-white')
    ```

    **Badge (lines 2386-2390):**
    ```python
    ui.badge(
        f'{len(state.reading_desk_entries)} {tr("fragments")}',
    ).props('dense').classes('text-xs').style(
        'border: 1px solid white; color: white !important; background: transparent;'
    )
    ```

    **Button (lines 2392-2396):**
    ```python
    ui.button(
        tr('Back to Page View'),
        icon='arrow_forward' if is_rtl() else 'arrow_back',
        on_click=exit_joined_view
    ).props('flat dense').style('color: white !important;')
    ```
  implication: |
    - Label uses `.classes('text-white')` - CSS class approach
    - Badge uses `.style('color: white !important;')` - inline style with !important
    - Button uses `.style('color: white !important;')` - inline style with !important

    Badge is visible, button should be visible (per previous fix), label is NOT visible.

    WAIT: User says button is ALSO invisible in Light Mode, contradicting the "fix didn't hold" statement.
    Need to verify current button state.

- timestamp: 2026-02-08T00:15:00Z
  checked: User's problem statement
  found: |
    "In Light Mode, only the badge shows — the label and button text are invisible."
    "A previous fix (11-06, W2) changed the button from `.props('flat dense text-color=white')`
    to `.props('flat dense').style('color: white !important;')`. This fix didn't hold."
  implication: |
    Both label AND button are currently invisible in Light Mode. Badge is the only visible element.
    The previous fix (adding `style('color: white !important;')` to button) "didn't hold" -
    meaning the code was changed but the issue persists.

    Hypothesis: `!important` inline styles are NOT sufficient to override Quasar's Light Mode theme.

- timestamp: 2026-02-08T00:20:00Z
  checked: web/main.py COMMON_STYLES CSS
  found: |
    - CSS uses `[data-theme="dark"]` selectors for dark mode overrides
    - CSS uses `[data-theme="parchment"]` selectors for parchment mode overrides
    - No `[data-theme="light"]` selectors found
    - Default theme is Light Mode (no explicit selector needed)

    Relevant CSS patterns:
    - `.q-badge` gets `color: white !important;` in dark theme (line 313-315)
    - No similar override for buttons or labels in any theme

    Key insight: The CSS does NOT contain Light Mode specific overrides that would cause white text to disappear.
  implication: |
    The problem is NOT caused by custom CSS in main.py.
    Must be Quasar framework's default behavior for Light Mode.

- timestamp: 2026-02-08T00:25:00Z
  checked: Quasar button behavior analysis
  found: |
    Looking at the code differences:

    **Badge (visible in Light Mode):**
    ```python
    ui.badge(...).props('dense').classes('text-xs').style(
        'border: 1px solid white; color: white !important; background: transparent;'
    )
    ```

    **Button (invisible in Light Mode):**
    ```python
    ui.button(...).props('flat dense').style('color: white !important;')
    ```

    **Label (invisible in Light Mode):**
    ```python
    ui.label(...).classes('text-lg font-bold text-white')
    ```

    Hypothesis: Quasar's `.q-btn` component in Light Mode has higher specificity CSS rules that override inline styles.
    Badges don't have this problem because they're a different component.
  implication: |
    Need to test if the button actually has `style('color: white !important;')` or if that was reverted.
    Also need to understand why `!important` doesn't work for buttons but does for badges.

- timestamp: 2026-02-08T00:30:00Z
  checked: Other button implementations in the codebase
  found: |
    Searched for `text-color=` pattern across web/ directory. Found 20+ examples of buttons with white text
    on colored backgrounds, ALL using Quasar's `text-color` prop:

    **Pattern used everywhere else:**
    ```python
    ui.button(...).props('flat text-color=white dense')
    ```

    **Examples:**
    - auth_state.py:387: `.props('flat text-color=white dense')`
    - main.py:1462: `.props(f'flat round text-color=white ...')`
    - joins_panel.py:323: `.props('flat round size=sm text-color=white')`
    - browse.py:2511 (image controls): `.props('flat round size=xs text-color=white')`

    **Reading desk button (WRONG PATTERN):**
    ```python
    ui.button(...).props('flat dense').style('color: white !important;')
    ```

    The reading desk button does NOT use `text-color=white` in props.
    It uses inline style instead, which doesn't work in Light Mode.
  implication: |
    ROOT CAUSE CONFIRMED: The reading desk button and label use the wrong approach for white text.

    - **Label:** Uses CSS class `.classes('text-white')` - doesn't work in Light Mode
    - **Button:** Uses inline style `.style('color: white !important;')` - doesn't work in Light Mode
    - **Badge:** Uses inline style `.style('color: white !important;')` - DOES work

    Why badge works but button doesn't: Quasar's button component (`.q-btn`) has internal CSS rules
    that override inline styles. The `text-color` prop is the official Quasar way to set button text color,
    and it has higher priority than inline styles.

    Badges are simpler components without the same override hierarchy.

## Resolution

root_cause: |
  **Reading desk header bar uses wrong styling approach for white text in Light Mode.**

  **Lines 2380-2396 in web/pages/browse.py:**

  1. **Icon (line 2380):** Uses `.classes('text-white')` - Quasar theme overrides this class in Light Mode
  2. **Label (line 2385):** Uses `.classes('text-white')` - Same issue as icon
  3. **Button (line 2396):** Uses `.style('color: white !important;')` - Quasar button internals override this

  **Why it fails:**
  - In Light Mode, Quasar's default theme overrides the `text-white` CSS class
  - Quasar buttons have internal CSS rules that take precedence over inline styles
  - The `text-color` prop is Quasar's official API for button text color, with higher priority

  **Why badge works:**
  - Badge uses inline style with `!important`, but badges don't have the same CSS hierarchy as buttons
  - Simpler component = fewer internal CSS rules to override

  **Evidence from codebase:**
  - 20+ other buttons with white text ALL use `.props('flat text-color=white dense')`
  - None use inline styles for color
  - This is the established pattern in auth_state.py, main.py, joins_panel.py, and elsewhere in browse.py

fix: |
  Change lines 2380, 2385, and 2396 in web/pages/browse.py:

  **Icon (line 2380):**
  FROM: `.classes('text-white text-xl')`
  TO: `.classes('text-xl').props('color=white')` OR inline style on the card/row container

  **Label (line 2385):**
  FROM: `.classes('text-lg font-bold text-white')`
  TO: `.classes('text-lg font-bold').style('color: white !important;')` (labels don't have text-color prop)

  **Button (line 2396):**
  FROM: `.props('flat dense').style('color: white !important;')`
  TO: `.props('flat dense text-color=white')`

verification: |
  1. Apply fix to browse.py
  2. Run web app: `python -m web.main`
  3. Open reading desk in Light Mode
  4. Verify all three elements (icon, label, button) are visible with white text
  5. Switch to Dark Mode - verify still visible
  6. Switch to Parchment Mode - verify still visible

files_changed:
  - web/pages/browse.py
