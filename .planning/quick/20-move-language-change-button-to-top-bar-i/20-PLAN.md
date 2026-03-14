---
phase: quick-20
plan: 01
type: execute
wave: 1
depends_on: []
files_modified: [web/main.py]
autonomous: true
must_haves:
  truths:
    - "Language toggle button is visible in the header bar"
    - "Language toggle is NOT in the sidebar footer"
    - "Clicking the button switches between Hebrew and English and reloads"
  artifacts:
    - path: "web/main.py"
      provides: "Language toggle in header right section"
  key_links:
    - from: "render_header_right() language button"
      to: "toggle_lang() -> storage + reload"
      via: "on_click handler"
---

<objective>
Move the language change button from the sidebar footer to the header top bar.

Purpose: Make language switching more discoverable and accessible — always visible in the header rather than buried in the sidebar.
Output: Updated web/main.py with language toggle in header, removed from sidebar.
</objective>

<context>
@web/main.py
</context>

<tasks>

<task type="auto">
  <name>Task 1: Move language toggle from sidebar footer to header right section</name>
  <files>web/main.py</files>
  <action>
In `render_header_right()` (around line 231), add a compact language toggle button BETWEEN the auth buttons block (lines 288-291) and the help button (lines 293-294).

Add the toggle_lang function inside render_header_right:
```python
def toggle_lang():
    current = get_language()
    new_lang = 'en' if current == 'he' else 'he'
    try:
        app.storage.user['ui_language'] = new_lang
    except Exception:
        pass
    set_language(new_lang)
    ui.navigate.reload()
```

Then add a flat round button matching the help button style:
```python
# Language Toggle
lang_label = "EN" if get_language() == 'he' else "עב"
ui.button(lang_label, on_click=toggle_lang).props('flat round text-color=white').tooltip(tr('Switch language')).classes('lang-btn-header')
```

This uses a compact text label ("EN" when in Hebrew mode to switch to English, "עב" when in English mode to switch to Hebrew) — consistent with the flat round style of the help button next to it.

Then REMOVE the entire language toggle block from the sidebar footer (lines 412-428): the `toggle_lang` function definition, `lang_btn_text` variable, and the `ui.row()` block with the icon and label. Keep the Translation Toggle that follows (lines 430+) intact.
  </action>
  <verify>
    <automated>cd C:/GenizahSearch && python -c "from web.main import create_layout; print('Import OK')"</automated>
  </verify>
  <done>Language toggle appears as a flat round button in the header right section (between auth and help). Sidebar footer no longer contains the language toggle. Clicking the button switches language and reloads.</done>
</task>

</tasks>

<verification>
- Run the web app (`python -m web.main`) and confirm the language button appears in the header bar
- Confirm the sidebar no longer has the language toggle
- Click the button and verify language switches correctly
</verification>

<success_criteria>
- Language toggle visible in header top bar as a compact flat round button
- Sidebar footer has no language toggle (translation toggle remains)
- Button click switches language and reloads the page
</success_criteria>

<output>
After completion, create `.planning/quick/20-move-language-change-button-to-top-bar-i/20-SUMMARY.md`
</output>
