---
status: diagnosed
trigger: "Investigate 3 related desktop issues: PGP tag click, browse stuck, ResultDialog from tag search"
created: 2026-02-08T00:00:00Z
updated: 2026-02-08T00:00:00Z
---

## Issue 1: PGP Tag Click Navigation Does Nothing (Test 3)

### Root Cause

**Duplicate method override.** There are TWO definitions of `_search_by_pgp_tag` in `GenizahGUI`:

1. **Line 7472** (original, Browse tab version):
   - Switches to Search tab via `self.tabs.setCurrentWidget(self.search_tab)`
   - Sets query input text and triggers `toggle_search()` (text search)

2. **Line 12859** (newer, tag search version):
   - Sets `tag_search_combo` text and calls `_execute_tag_search()`
   - Does **NOT** switch to Search tab

Python's method resolution means the **second definition (12859) silently overrides the first (7472)**. When the user clicks a green PGP tag link in the Browse tab's extended info panel, the call chain is:

```
_on_browse_ext_link_clicked (line 7463)
  -> self._search_by_pgp_tag(tag)   -- resolves to line 12859 (override)
     -> tag_search_combo.setCurrentText(tag)
     -> _execute_tag_search()        -- fires search, results go to Search tab
     -- BUT tab is never switched! User stays on Browse tab.
```

The tag search actually **executes correctly in the background** and results populate the Search tab's results table, but the user never sees them because **the tab switch was lost** when the second definition replaced the first.

The same issue affects `ResultDialog._on_rd_ext_link_clicked` (line 3846), which calls `parent._search_by_pgp_tag(tag)` -- same override, same missing tab switch.

### Fix Direction

Merge the two methods into one at line 12859 that:
1. Switches to Search tab first: `self.tabs.setCurrentWidget(self.search_tab)`
2. Then sets combo and executes tag search

Remove the dead first definition at line 7472.

### Files Involved

- `genizah_app.py:7472` - First (dead) definition with tab switch
- `genizah_app.py:12859` - Second (active) definition missing tab switch
- `genizah_app.py:7463` - Browse tab link click handler (caller)
- `genizah_app.py:3846` - ResultDialog link click handler (caller)

---

## Issue 2: Tag Search Result Gets Browse Tab Stuck (Test 12)

### Root Cause

**Missing state reset in `open_result_in_browse`.** When navigating to Browse from a tag search result (via the browse button in the results table), `open_result_in_browse` (line 13407) has two gaps:

1. **Does not clear `browse_shelf_input`** -- the old shelfmark from a previous browse session remains in the input field
2. **Does not call `_set_last_browse_field("sys")`** when there is no FL ID (which is always the case for tag search results, since they have `raw_header: ''`)

The consequence:

```
open_result_in_browse (line 13407):
  browse_sys_input.setText(sid)       # Correct sys_id set
  browse_shelf_input -> NOT touched   # Old shelfmark stays!
  _set_last_browse_field -> NOT called when derived_fl_id is None (line 13443-13444)
  browse_load() called

browse_load (line 16414):
  priority based on last_browse_field (stale value!)
  If last_browse_field == "shelf":
    -> Resolves OLD shelfmark from browse_shelf_input
    -> Loads WRONG manuscript (not the tag search result)
  After wrong load:
    browse_render_page sets browse_shelf_input to wrong manuscript's shelfmark
    browse_sys_input still has tag result's sys_id
    -> User sees wrong manuscript, types new shelfmark
    -> If new shelfmark doesn't resolve, falls back to sys_id (tag result)
    -> Neither the displayed manuscript nor the user's input -- appears "stuck"
```

Even when the initial load works (if `last_browse_field` happens to be "sys"), the subsequent navigation is fragile: `browse_shelf_input` contains the loaded manuscript's shelfmark (from `browse_render_page` line 16844), but `browse_sys_input` also contains the sys_id. If the user edits the shelfmark and the resolve fails, the sys_id fallback loads the same manuscript.

### Fix Direction

In `open_result_in_browse`, when `derived_fl_id` is None (the else branch at line 13443):
1. Clear `browse_shelf_input` to prevent stale shelfmark interference
2. Call `_set_last_browse_field("sys")` to ensure priority is sys_id

### Files Involved

- `genizah_app.py:13407-13447` - `open_result_in_browse` missing state reset
- `genizah_app.py:16414-16524` - `browse_load` priority logic affected by stale state

---

## Issue 3: ResultDialog Not Working From Tag Search (Test 13)

### Root Cause

**Missing required fields in tag search result format.** When the user double-clicks a tag search result, `show_full_text` (line 13207) calls `ResultDialog` which calls `load_result_by_index` (line 3552). This method expects normal Tantivy search result fields that tag search results do not have.

Tag search results (from `_on_tag_search_results`, line 12825-12836) have:
```python
{
    'display': {'id': sid, 'shelfmark': shelf, 'title': title, 'library': lib, 'img': '', 'source': ''},
    'snippet': snippet,
    'raw_header': '',    # Empty!
}
```

`load_result_by_index` (line 3552-3564) does:

```python
# Line 3554-3555: KeyError on missing 'uid'
if not data.get('full_text'):
    data['full_text'] = self.searcher.get_full_text_by_id(data['uid'])  # KeyError: 'uid'
    #                                                       ^^^^^^^^^
    # Tag results have no 'uid' key

# Line 3564: parse_full_id_components returns all None
ids = self.meta_mgr.parse_full_id_components(data['raw_header'])
#     raw_header is '' -> sys_id, p_num, fl_id all None

# Line 3565-3567: current_sys_id = None, p = raises exception (caught)
self.current_sys_id = ids['sys_id']  # None
```

The crash happens at **line 3555**: `data['uid']` raises `KeyError` because tag search results have no `uid` key. Even if this were guarded, `raw_header` is empty so `sys_id` would be None, making the dialog non-functional.

### Fix Direction

In `load_result_by_index`, add fallback for tag search results:
1. Guard `data['uid']` with `.get('uid')` to avoid KeyError
2. Fall back to `display.id` for sys_id when `parse_full_id_components` returns None
3. When `full_text` can't be retrieved by uid, use `searcher.get_full_manuscript(sid)` or load from browse data

Alternatively, `_on_tag_search_results` could populate `raw_header` and `uid` fields in the formatted results so `load_result_by_index` works without changes.

### Files Involved

- `genizah_app.py:3552-3564` - `load_result_by_index` crashes on missing `uid` key
- `genizah_app.py:12816-12836` - `_on_tag_search_results` missing fields in formatted output

---

## Evidence

- timestamp: 2026-02-08
  checked: Two definitions of _search_by_pgp_tag in GenizahGUI class
  found: Line 7472 (with tab switch) and line 12859 (without tab switch). Python uses last definition.
  implication: Tag click from Browse/ResultDialog executes search but never switches tab.

- timestamp: 2026-02-08
  checked: open_result_in_browse state management for tag search results
  found: browse_shelf_input not cleared, _set_last_browse_field not called when fl_id is None
  implication: browse_load may use stale priority and stale shelfmark from previous session.

- timestamp: 2026-02-08
  checked: Tag search result format vs load_result_by_index expectations
  found: Tag results lack 'uid' key (KeyError at line 3555), have empty 'raw_header' (sys_id=None at line 3564)
  implication: ResultDialog crashes or is non-functional for tag search results.

- timestamp: 2026-02-08
  checked: PGPTagSearchWorker and get_fragments_by_tag
  found: Returns sys_id, shelfmark, document_type, description, pgpid. Formatted with raw_header=''.
  implication: No Tantivy index fields available in tag search results.
