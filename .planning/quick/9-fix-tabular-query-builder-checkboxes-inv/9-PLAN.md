---
phase: quick
plan: 9
type: execute
wave: 1
depends_on: []
files_modified:
  - web/main.py
  - web/pages/search.py
autonomous: true
must_haves:
  truths:
    - "All checkboxes in the tabular query builder are visible and readable in dark mode"
    - "Component card borders adapt to dark/parchment themes instead of being hardcoded gray"
  artifacts:
    - path: "web/main.py"
      provides: "Dark mode CSS rules for q-checkbox components"
      contains: "q-checkbox"
    - path: "web/pages/search.py"
      provides: "Theme-aware component card border"
      contains: "var(--border-light)"
  key_links:
    - from: "web/main.py"
      to: "web/pages/search.py"
      via: "CSS custom properties consumed by Quasar checkbox components"
      pattern: "data-theme.*dark.*q-checkbox"
---

<objective>
Fix checkboxes in the tabular query builder (and all other checkboxes on the search page) being invisible in dark mode due to missing dark-theme CSS rules for Quasar's q-checkbox component, and fix the hardcoded #e0e0e0 component card border that also fails in dark mode.

Purpose: Checkboxes for modifiers (prefix, suffix, wildcard, plene, negation) and search options (variants, JA, flex, bidir) are completely invisible in dark mode -- users cannot see or interact with them.
Output: Visible, theme-aware checkboxes and card borders across all three themes.
</objective>

<execution_context>
@C:/Users/gersh/.claude/get-shit-done/workflows/execute-plan.md
@C:/Users/gersh/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@web/main.py (lines 204-350 - dark theme CSS overrides section)
@web/pages/search.py (line 1507-1508 - hardcoded border; lines 1570-1575 - modifier checkboxes; lines 1598-1601 - search option checkboxes; lines 506-515 - outer Responsa checkboxes; line 653 - select all checkbox)
</context>

<tasks>

<task type="auto">
  <name>Task 1: Add dark-mode and parchment CSS rules for q-checkbox in web/main.py</name>
  <files>web/main.py</files>
  <action>
In web/main.py, in the COMMON_STYLES string, add a new CSS block for checkbox dark-mode fixes AFTER the existing "Dark Theme Select/Dropdown Fixes" block (after line ~349, before the "Parchment theme input fixes" block at line ~351).

Add these rules:

```css
/* Dark Theme Checkbox Fixes */
[data-theme="dark"] .q-checkbox__inner {
    color: var(--text-secondary) !important;
}

[data-theme="dark"] .q-checkbox__inner--truthy {
    color: var(--primary-400) !important;
}

[data-theme="dark"] .q-checkbox .q-checkbox__label {
    color: var(--text-primary) !important;
}
```

This targets three states:
1. `.q-checkbox__inner` -- the unchecked checkbox border/icon (currently black, invisible on dark bg). Set to `--text-secondary` (#cbd5e1) for visible light gray.
2. `.q-checkbox__inner--truthy` -- the checked state. Set to `--primary-400` (#34d399) for the green accent color matching the theme.
3. `.q-checkbox .q-checkbox__label` -- the label text next to the checkbox. Set to `--text-primary` (#f1f5f9) for readability.

Do NOT add parchment-specific checkbox rules -- parchment theme uses dark text on light backgrounds so the default Quasar checkbox styling is fine there.
  </action>
  <verify>
Open the web app (`python -m web.main`), switch to dark theme, navigate to the search page, switch to Responsa mode and open the tabular query builder. Verify:
1. Modifier checkboxes (Prefixes, Suffixes, Wildcard, Plene, Negation) are visible with light borders
2. Search option checkboxes (Variants, JA, Flex Spacing, Bidirectional) are visible
3. Checking a checkbox turns it green (primary-400)
4. Outer Responsa sub-option checkboxes are also visible in dark mode
5. Select-all checkbox in results header is visible
6. All checkboxes still look correct in light theme and parchment theme (no regression)
  </verify>
  <done>All q-checkbox components on the search page are visible and readable in dark mode with appropriate themed colors for unchecked, checked, and label states.</done>
</task>

<task type="auto">
  <name>Task 2: Replace hardcoded #e0e0e0 border with CSS variable in search.py</name>
  <files>web/pages/search.py</files>
  <action>
In web/pages/search.py at line 1508, change the hardcoded border color from `#e0e0e0` to use the CSS custom property `var(--border-light)`.

Change:
```python
'border: 1px solid #e0e0e0; border-radius: 8px; min-width: 150px; flex: 1;'
```

To:
```python
'border: 1px solid var(--border-light); border-radius: 8px; min-width: 150px; flex: 1;'
```

This ensures the component card border uses:
- Light theme: `--border-light` = `#e2e8f0` (similar to current #e0e0e0, no visual change)
- Dark theme: `--border-light` = `#334155` (visible but subtle on dark background)
- Parchment: `--border-light` = `#fde68a` (warm gold tone)
  </action>
  <verify>
With the web app running, check the tabular query builder component cards in all three themes:
1. Light theme: cards have subtle gray border (visually same as before)
2. Dark theme: cards have visible dark slate border that does not disappear
3. Parchment theme: cards have warm-toned border matching the theme
  </verify>
  <done>Component card borders in the tabular query builder are theme-aware across all three themes, with no hardcoded color values.</done>
</task>

</tasks>

<verification>
1. Dark mode: All checkboxes in the tabular query builder are visible (unchecked = light gray border, checked = green, labels = white)
2. Dark mode: Component card borders are visible (slate gray, not invisible)
3. Light mode: No visual regression -- checkboxes and card borders look the same as before
4. Parchment mode: No visual regression -- everything remains readable
5. Outer Responsa checkboxes (Variants, JA, Flex, Bidirectional) also visible in dark mode
6. Results header select-all checkbox visible in dark mode
</verification>

<success_criteria>
- All checkbox elements on the search page are visible and interactive in dark mode
- Component card borders adapt to theme changes
- Zero visual regression in light and parchment themes
</success_criteria>

<output>
After completion, create `.planning/quick/9-fix-tabular-query-builder-checkboxes-inv/9-SUMMARY.md`
</output>
