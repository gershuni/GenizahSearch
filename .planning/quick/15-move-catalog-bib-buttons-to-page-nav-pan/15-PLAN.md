---
phase: quick-15
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - web/pages/browse.py
autonomous: true
requirements: [QUICK-15]
must_haves:
  truths:
    - "Bibliography FJMS, Bibliography Ktiv, and Catalog Records buttons appear in the page navigation pane (the folio header bar) instead of buried in the metadata panel"
    - "Catalog Records button works correctly (opens dialog) regardless of whether the metadata panel is open or closed"
    - "Buttons only appear when relevant data is available (same conditional logic as before)"
  artifacts:
    - path: "web/pages/browse.py"
      provides: "Relocated bibliography and catalog buttons in page nav pane"
  key_links:
    - from: "page nav pane buttons"
      to: "show_catalog_dialog / create_fjms_bibliography_dialog / create_nli_bibliography_dialog"
      via: "on_click callbacks with correct fjms_service reference"
      pattern: "show_catalog_dialog|create_fjms_bibliography_dialog|create_nli_bibliography_dialog"
---

<objective>
Move the Bibliography (FJMS + Ktiv) and Catalog Records buttons from the expandable metadata panel to the page navigation pane (folio header bar), and fix the bug where the FJMS Catalog Records button does nothing when metadata is expanded.

Purpose: These buttons are important research tools but are currently hidden behind "Show Metadata". Making them always visible in the page navigation pane improves discoverability and workflow. The bug fix ensures the catalog dialog always opens correctly.

Output: Modified browse.py with buttons relocated to the page nav pane.
</objective>

<execution_context>
@C:/Users/gersh/.claude/get-shit-done/workflows/execute-plan.md
@C:/Users/gersh/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@web/pages/browse.py
@web/components/catalog_dialog.py
@web/components/bibliography_dialog.py
</context>

<tasks>

<task type="auto">
  <name>Task 1: Move bibliography and catalog buttons from metadata panel to page navigation pane</name>
  <files>web/pages/browse.py</files>
  <action>
This task relocates 3 buttons (Bibliography FJMS, Bibliography Ktiv, Catalog Records) from the metadata panel (inside `if show_metadata['value']:` block, lines ~2334-2381) to the page navigation pane (the folio header card at line ~3552).

**Step 1: Remove buttons from metadata panel.**

Delete the entire section from line ~2334 (`# === Bibliography References (separate FJMS / NLI dialogs) ===`) through line ~2381 (the `catalog_btn.disable()` line and its containing `if` block). This includes:
- The imports of `create_fjms_bibliography_dialog`, `create_nli_bibliography_dialog`, `show_catalog_dialog`
- The `fjms_bib`, `marc_bib` data gathering
- The `catalog_source_count` computation
- The separator and button row with all 3 buttons
- Keep the `ui.separator` before this block only if other content follows that needs it

**Step 2: Add buttons to the page navigation pane.**

In the page navigation pane (the `ui.card()` at line ~3552 that contains folio info, source chips, prev/next nav), add the bibliography and catalog buttons to the right-side group (after the "Add to Reading Desk" button at line ~3814, but before the `# === SIDE-BY-SIDE LAYOUT` comment).

Insert a new row BELOW the existing navigation card (after the card's closing), creating a small secondary toolbar:

```python
# === Bibliography & Catalog Buttons ===
# These use enrichment data loaded in Phase B (_load_enrichment)
# Use enrichment_refs pattern for deferred population after enrichment loads
bib_catalog_el = ui.element('div').classes('w-full')
enrichment_refs['bib_catalog_container'] = bib_catalog_el
if state.enrichment_loaded:
    _populate_bib_catalog_buttons(bib_catalog_el, state, page)
```

Then define a helper function `_populate_bib_catalog_buttons(container, state, page)` near the top of the `update_content` function (or as a nested function) that:

1. Reads `fjms_data = state.fjms_data or {}`
2. Gets `fjms_bib = fjms_data.get('bibliography', [])`
3. Gets `marc_bib` from `app_state.meta_mgr.nli_cache` (same pattern as current code)
4. Gets `catalog_source_count = len(fjms_data.get('source_names', []))`
5. If any data exists, creates a `ui.row().classes('items-center gap-2 flex-wrap px-3 py-1')` with:
   - FJMS Bibliography button (if `fjms_bib`)
   - Ktiv Bibliography button (if `marc_bib`)
   - Catalog Records button (always shown, disabled if count==0)
6. Gets `fjms = get_fjms_service(thread_safe=True)` for the catalog dialog callback
7. Uses `props('outline dense').classes('text-sm')` on all buttons (same styling as current)

**CRITICAL: Fix the FJMS button bug.** The current code at line ~2229 gets `fjms = get_fjms_service(thread_safe=True)` and uses it in the lambda at line ~2378. This works fine. However, to ensure the button works regardless of metadata panel state, the new code must import and instantiate `fjms_service` at button creation time, NOT depend on any variable from the metadata panel scope. Use:
```python
on_click=lambda s=page.sys_id, sm=page.shelfmark or '': show_catalog_dialog(s, sm)
```
This lets `show_catalog_dialog` auto-create its own fjms_service (it already has `if fjms_service is None: ... get_fjms_service()` fallback at line 30-32 of catalog_dialog.py). This is simpler and avoids scope issues.

**Step 3: Wire enrichment callback.**

In the `_load_enrichment` function (around line ~1094 where `state.fjms_data = fjms_data` is set), after the existing enrichment ref population code, add population of the bib/catalog container:

```python
bib_catalog_container = enrichment_refs.get('bib_catalog_container')
if bib_catalog_container:
    _populate_bib_catalog_buttons(bib_catalog_container, state, page)
```

This follows the exact same pattern used for `pgp_link_container`, `version_container`, and `joins_container`.

**Important:** Keep the FJMS Catalog Metadata section (lines ~2225-2312, the inline catalog records, domains, etc.) in the metadata panel -- only the 3 dialog-opening BUTTONS are being moved. The metadata panel still shows the detailed FJMS catalog info, domain classifications, catalog cross-references, and source names inline.
  </action>
  <verify>
Run `python -c "import ast; ast.parse(open('web/pages/browse.py').read()); print('Syntax OK')"` to verify no syntax errors.

Run `python -m web.main` and:
1. Navigate to any manuscript in Browse
2. Verify Bibliography FJMS / Ktiv / Catalog Records buttons appear in the page navigation area (not requiring "Show Metadata")
3. Click "Catalog Records" button -- should open the catalog dialog
4. Click "Show Metadata" -- verify metadata panel still shows FJMS catalog info, domains, etc. inline but NO duplicate bibliography/catalog buttons
5. Navigate to a different manuscript -- verify buttons update correctly
  </verify>
  <done>
Bibliography FJMS, Bibliography Ktiv, and Catalog Records buttons are visible in the page navigation pane without needing to expand metadata. Catalog Records button opens the dialog correctly. No duplicate buttons in the metadata panel. Buttons show correct counts and are conditionally displayed based on data availability.
  </done>
</task>

</tasks>

<verification>
- Browse page loads without errors
- Buttons appear in navigation pane with correct counts
- Catalog Records dialog opens when button is clicked
- Metadata panel still shows FJMS catalog detail, domains, cross-refs (but not the 3 buttons)
- Navigation between manuscripts updates buttons correctly
- Buttons handle enrichment loading delay (appear after Phase B fetch completes)
</verification>

<success_criteria>
- Bibliography FJMS, Bibliography Ktiv, and Catalog Records buttons are always visible in the page navigation pane
- Catalog Records button reliably opens the catalog dialog (bug fixed)
- No buttons duplicated between nav pane and metadata panel
- Enrichment-based deferred loading works (buttons populate after background fetch)
</success_criteria>

<output>
After completion, create `.planning/quick/15-move-catalog-bib-buttons-to-page-nav-pan/15-SUMMARY.md`
</output>
