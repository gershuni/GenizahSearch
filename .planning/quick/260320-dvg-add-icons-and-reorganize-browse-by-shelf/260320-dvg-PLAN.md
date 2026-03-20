---
phase: quick
plan: 260320-dvg
type: execute
wave: 1
depends_on: []
files_modified:
  - genizah_app.py
autonomous: true
requirements: [BROWSE-ICONS, BROWSE-REORG, BROWSE-CROSS-NAV, BROWSE-EXT-LINK, RD-IMG-TOGGLE]
must_haves:
  truths:
    - "Browse tab buttons have emoji icons matching ResultDialog style"
    - "ext_info_row is reorganized: Puzzle, Parallels, List, ExtInfo, Bib FJMS, Bib NLI, Catalog, Ktiv, External Link, Translations toggle"
    - "Page prev/next buttons remain enabled at shelfmark boundaries for cross-shelfmark navigation"
    - "External library link button appears when external_url is available in browse tab"
    - "ResultDialog image toggle state persists across result navigation"
    - "View Corrections button has notepad emoji"
    - "Translations toggle is compact icon button with colored/uncolored state"
  artifacts:
    - path: "genizah_app.py"
      provides: "All browse tab icon/layout changes, cross-nav fix, external link, RD image toggle persistence"
  key_links:
    - from: "genizah_app.py:ext_info_row"
      to: "browse tab action buttons"
      via: "QHBoxLayout widget ordering"
      pattern: "ext_info_row\\.addWidget"
    - from: "genizah_app.py:btn_b_external_link"
      to: "meta_mgr.nli_cache external_url"
      via: "metadata lookup on manuscript load"
      pattern: "external_url|external_provider"
---

<objective>
Add emoji icons and reorganize the Browse by Shelfmark tab's button rows to match ResultDialog patterns. Also fix cross-shelfmark page navigation, add external library link button, preserve ResultDialog image toggle state, and make translations toggle compact.

Purpose: Consistent UX between ResultDialog and Browse tab; remove artificial navigation limits at page boundaries; add missing external library links.
Output: Updated genizah_app.py with all 6 changes from CONTEXT.md decisions.
</objective>

<execution_context>
@C:/Users/gersh/.claude/get-shit-done/workflows/execute-plan.md
@C:/Users/gersh/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@genizah_app.py

Key reference points in genizah_app.py:
- ResultDialog action_row: lines ~5610-5683 (icon patterns to match)
- ResultDialog btn_external_link: line 5579, populated at line 7745-7761
- ResultDialog load_result_by_index: line 7124 (image toggle state not preserved)
- ResultDialog btn_toggle_image: line 5635 (reset to checked=True at line 7725)
- Browse tab row1: lines ~14081-14122 (top bar with nav inputs + action buttons)
- Browse ext_info_row: lines ~14132-14167 (Extended Info + Bib + Catalog)
- Browse btn_b_catalog (View on Ktiv): line 14069, currently in row1
- Browse community_bar: lines ~14267-14329 (corrections, comments, joins)
- Browse page nav disable: lines 27964-27965 (boundary disable to remove)
- Browse metadata load: lines ~14760-14789 (where to add external link logic)
- browse_open_catalog: line 28010 (opens Ktiv URL)
</context>

<tasks>

<task type="auto">
  <name>Task 1: Add icons to browse buttons, reorganize ext_info_row, compact translations toggle</name>
  <files>genizah_app.py</files>
  <action>
**A) Add emoji icons to browse tab buttons (matching ResultDialog patterns):**

1. `btn_browse_add_to_list` (line ~14107): Already uses `_format_add_to_list_label` which has folder icon -- verify it matches ResultDialog pattern, no change needed if already has icon.
2. `btn_find_parallels` (line ~14065): Change from `tr("Find parallels")` to `f"🔍 {tr('Parallels')}"` (matches ResultDialog `btn_search_parallels`).
3. `btn_b_add_to_view` (line ~14091): Change from `tr("Add to View")` to `f"👁️ {tr('Add to View')}"` (eye icon per decision).
4. `btn_b_catalog` aka View on Ktiv (line ~14069): Change from `tr("View on Ktiv")` to `f"🌐 {tr('View on Ktiv')}"` (globe icon).
5. `btn_b_view_corrections` (line ~14305): Change from `tr("View Corrections")` to `f"📝 {tr('View Corrections')}"` (notepad emoji per decision).
6. `btn_b_ext_info` (line ~14134): Change from `tr("Show Extended Info")` to `f"ℹ️ {tr('Info')}"` when unchecked and `f"ℹ️ {tr('Hide Info')}"` when checked (match ResultDialog's `btn_ext_info` pattern). Also update the toggled handler `_browse_toggle_extended_info` to swap text.

**B) Reorganize ext_info_row -- move action buttons from row1 to ext_info_row:**

Current row1 has: [prev_ms] [SysID input] [Shelf input] [next_ms] [FL input] [Go] [Add to View] [Puzzle] [Parallels] [Add to List] [spacing] [View on Ktiv] [stretch] [Help ?]

New row1 (simplified): [prev_ms] [SysID input] [Shelf input] [next_ms] [FL input] [Go] [Add to View] [stretch] [Help ?]

Remove from row1: btn_b_add_to_puzzle, btn_find_parallels, btn_browse_add_to_list, the addSpacing(20), btn_b_catalog.

New ext_info_row layout:
- Left group (actions moved from row1): btn_b_add_to_puzzle, btn_find_parallels, btn_browse_add_to_list
- Then: btn_b_ext_info (moved from left of current ext_info_row)
- Then: btn_b_bibliography_fjms, btn_b_bibliography_nli, btn_b_catalog_records
- Then: btn_b_catalog (View on Ktiv), btn_b_external_link (NEW, see Task 2)
- Right end: btn_b_translations (compact icon)
- addStretch() before translations toggle

Remove the old addStretch() that was between btn_b_translations and bibliography buttons.

**C) Make translations toggle compact icon button:**

Change `btn_b_translations` from wide text button to compact icon:
- Text: just "🌐" (globe emoji, no text label)
- Add tooltip: `tr("Toggle translations")`
- Keep checkable behavior
- Style: when checked (ON) use green background `#059669`, when unchecked (OFF) use grey `#94a3b8`
- Set fixedWidth to ~32px to keep it compact
- Remove the text-changing logic in `_browse_toggle_translations` that sets "Translations ON"/"Translations OFF" -- just keep the icon and change background color via stylesheet or checked state styling

The stylesheet already handles checked vs unchecked colors, but update to:
```python
"QPushButton { background-color: #94a3b8; color: white; border-radius: 4px; padding: 2px 6px; }"
"QPushButton:checked { background-color: #059669; }"
```
  </action>
  <verify>
    <automated>python -c "import ast; ast.parse(open('genizah_app.py', encoding='utf-8').read()); print('SYNTAX OK')"</automated>
  </verify>
  <done>All browse buttons have emoji icons matching ResultDialog, ext_info_row is reorganized with action buttons moved from row1, translations toggle is a compact colored icon with tooltip</done>
</task>

<task type="auto">
  <name>Task 2: Add external library link button, fix cross-shelfmark nav, preserve RD image toggle</name>
  <files>genizah_app.py</files>
  <action>
**A) Add btn_b_external_link button to browse tab:**

1. Create button near where btn_b_catalog (View on Ktiv) is defined (~line 14069):
```python
self.btn_b_external_link = QPushButton(tr("External Website"))
self.btn_b_external_link.setToolTip(tr("Open in external library website"))
self.btn_b_external_link.setVisible(False)
self.btn_b_external_link.clicked.connect(self._browse_open_external_link)
```

2. Add it to ext_info_row right after btn_b_catalog (View on Ktiv) per Task 1 layout.

3. Create the click handler `_browse_open_external_link`:
```python
def _browse_open_external_link(self):
    if hasattr(self, '_browse_external_url') and self._browse_external_url:
        url = self._browse_external_url
        if "cudl.lib.cam.ac.uk\\iiif\\" in url or "cudl.lib.cam.ac.uk/iiif/" in url:
            url = url.replace("/iiif/", "/view/")
        QDesktopServices.openUrl(QUrl(url))
```

4. In the browse metadata load section (~line 14785, after `external_meta = meta.get('external_meta', {})`), add external link population -- copy the exact pattern from ResultDialog lines 7745-7761:
```python
self._browse_external_url = meta.get('external_url') or meta.get('marc', {}).get('external_iiif_link')
if self._browse_external_url:
    provider = meta.get('external_provider', '')
    if provider == 'oxford':
        btn_label = tr("Oxford")
    elif provider == 'cambridge' or "cudl.lib.cam.ac.uk" in (self._browse_external_url or "").lower():
        btn_label = tr("Cambridge")
    elif provider == 'manchester':
        btn_label = "Manchester LUNA"
    elif provider == 'jts':
        btn_label = "Princeton Digital Library"
    else:
        btn_label = tr("External Website")
    self.btn_b_external_link.setText(btn_label)
    self.btn_b_external_link.setVisible(True)
else:
    self.btn_b_external_link.setVisible(False)
```

Also initialize `self._browse_external_url = None` near `self.btn_b_external_link` definition.

**B) Fix cross-shelfmark page navigation:**

At lines 27964-27965, remove the boundary disable logic:
```python
# BEFORE:
self.btn_b_prev.setEnabled(pd['current_idx'] > 1)
self.btn_b_next.setEnabled(pd['current_idx'] < pd['total_pages'])

# AFTER:
# Keep prev/next always enabled for cross-shelfmark navigation
# browse_navigate already uses allow_cross=True
self.btn_b_prev.setEnabled(True)
self.btn_b_next.setEnabled(True)
```

Note: Keep the initial setEnabled(False) at line 14223 so buttons are disabled before any manuscript is loaded. The change only applies to the update after a page loads (line 27964-27965).

**C) Preserve ResultDialog image toggle state across result navigation:**

In `load_result_by_index` (line ~7124), the image toggle is always reset. Fix:

1. Before the metadata loading at line ~7717 (`has_images = bool(...)`), save the current toggle state:
```python
_prev_img_visible = self.btn_toggle_image.isChecked()
```

2. After the block that sets toggle state (lines 7720-7743), restore user preference:
```python
if has_images and not _prev_img_visible:
    # User had hidden images -- respect that choice
    self.btn_toggle_image.setChecked(False)
    self.external_pane.setVisible(False)
```

This way: if the new result has images AND user previously hid the image pane, keep it hidden. If no images, it stays hidden naturally. If user had it shown, the default `setChecked(True)` at line 7725 keeps it shown.
  </action>
  <verify>
    <automated>python -c "import ast; ast.parse(open('genizah_app.py', encoding='utf-8').read()); print('SYNTAX OK')"</automated>
  </verify>
  <done>External library link button visible for Cambridge/Oxford/Manchester/Princeton manuscripts in browse tab; page nav buttons stay enabled at boundaries for cross-shelfmark wrapping; ResultDialog remembers image hide/show state when navigating between results</done>
</task>

</tasks>

<verification>
1. `python -c "import ast; ast.parse(open('genizah_app.py', encoding='utf-8').read())"` -- syntax valid
2. `python genizah_app.py` launches without errors (manual)
3. Browse tab: buttons have emoji icons, ext_info_row shows reorganized layout
4. Browse tab: at last page of a shelfmark, Next button stays enabled and wraps to next shelfmark
5. Browse a Cambridge manuscript: external link button appears with "Cambridge" label
6. ResultDialog: hide image, click Next, image stays hidden
7. Translations toggle is compact colored icon
</verification>

<success_criteria>
- All 6 decision items from CONTEXT.md implemented
- Browse tab button row matches ResultDialog visual patterns (emoji icons)
- ext_info_row reorganized per the specified layout order
- Cross-shelfmark navigation works without boundary blocking
- External library link button appears for applicable libraries
- ResultDialog image toggle state preserved during navigation
- Translations toggle is compact icon with color state
</success_criteria>

<output>
After completion, create `.planning/quick/260320-dvg-add-icons-and-reorganize-browse-by-shelf/260320-dvg-SUMMARY.md`
</output>
