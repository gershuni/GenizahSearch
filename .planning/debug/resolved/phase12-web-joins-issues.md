---
status: diagnosed
trigger: "Test 9: Web Toggle Filters not shown; Test 10: PGP joins not in dropdown menu"
created: 2026-02-08
updated: 2026-02-08
---

# Issue 1 (Test 9): Web Toggle Filters Not Shown

## Root Cause

The `toggle_filters()` function in `web/pages/search.py` (line 728-734) reads back
`filters_panel.style` as if it were a string, but in NiceGUI `.style` returns a `Style`
object (dict-like), not a CSS string. The toggle logic therefore never detects the
`display: none` state and fails to reveal the panel.

**Broken code** (line 730-734):

```python
def toggle_filters():
    current_display = filters_panel.style or ''          # Style object, always truthy
    if 'display: none' in current_display:               # string-in-object check fails
        filters_panel.style('background: ...; ')         # never reached
    else:
        filters_panel.style('background: ...; display: none;')  # always hides
```

The panel starts with `display: none` (line 539). When the user clicks the filter icon
button, `toggle_filters()` runs, but the condition on line 731 never evaluates to `True`
because `current_display` is a `Style` object, not a string containing `"display: none"`.
So the panel is re-hidden every time the button is clicked.

The other toggle function in the same file (`toggle_search_panel`, line 588) correctly
uses a **state variable** (`search_state.is_panel_collapsed`) instead of reading back
`.style` -- confirming this is a pattern mismatch.

**Fix direction:** Use a boolean state variable (e.g., `filters_visible = {'value': False}`)
to track filter panel visibility, matching the pattern used by `toggle_search_panel`.

**Files involved:**
- `web/pages/search.py` lines 728-734 (toggle_filters function)
- `web/pages/search.py` line 538-539 (initial panel style with display: none)

---

# Issue 2 (Test 10): PGP Joins Not in Dropdown Menu

## Root Cause

The QToolButton dropdown menus (`joins_menu` for Browse view, `rd_joins_menu` for Reading
Desk) only query the local `JoinsManager` for **user-created joins**. They never call
`_get_pgp_joins()` to fetch PGP multi-fragment data from Supabase.

In contrast, the `JoinsDialog` (the full dialog opened by clicking the button itself)
**does** call `_get_pgp_joins()` and merges PGP joins into the display in three places:
- `load_joins()` at lines 3611-3613 (PGP-only fallback)
- `load_joins()` at lines 3633-3636 (no user joins, check PGP)
- `_display_cached_joins()` at line 4008 (`_merge_pgp_joins_into_display`)

**Browse view dropdown** (`_update_joins_dropdown`, line 5764-5878):
- Queries only `self.joins_mgr.get_connected_fragments_by_id()` and
  `self.joins_mgr.get_connected_fragments()` (user joins only)
- If no user joins found, displays "No joined fragments" and returns (line 5793-5798)
- **Never calls `_get_pgp_joins()` or queries Supabase `document_fragments` table**

**Reading Desk dropdown** (`_rd_update_joins_menu`, line 2723-2851):
- Same pattern: queries only `parent.joins_mgr` for user joins
- If no user joins found, displays "No joined fragments" and returns (line 2765-2769)
- **Never calls `_get_pgp_joins()` or queries Supabase `document_fragments` table**

So when a document has PGP-sourced multi-fragment joins (from `document_fragments` table)
but no user-created joins, the dropdown menu shows "No joined fragments" while the
full dialog correctly shows the PGP joins.

**Fix direction:** After the user-joins lookup in both `_update_joins_dropdown()` and
`_rd_update_joins_menu()`, add a PGP joins fallback. This requires calling
`get_document_for_fragment()` and `get_fragments_for_document()` from
`shared/document_service.py`, similar to how `JoinsDialog._get_pgp_joins()` works.
The PGP join entries should be styled distinctly (e.g., with a "[PGP]" prefix) and
should appear after any user joins. Note: Supabase calls should NOT be made on the
main thread -- use a worker or cache.

**Files involved:**
- `genizah_app.py` lines 5764-5878 (`_update_joins_dropdown` - Browse view)
- `genizah_app.py` lines 2723-2851 (`_rd_update_joins_menu` - Reading Desk)
- `corrections_ui.py` lines 3490-3568 (`JoinsDialog._get_pgp_joins` - reference impl)
- `corrections_ui.py` lines 3570-3638 (`JoinsDialog.load_joins` - where PGP fallback works)
