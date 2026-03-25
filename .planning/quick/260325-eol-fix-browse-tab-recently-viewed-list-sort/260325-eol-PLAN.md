---
phase: quick
plan: 260325-eol
type: execute
wave: 1
depends_on: []
files_modified:
  - web/user_lists.py
  - genizah_app.py
autonomous: true
requirements: [recently-viewed-sort, recently-viewed-auth-fix]

must_haves:
  truths:
    - "Recently Viewed items display in most-recently-viewed-first order on desktop browse tab"
    - "Recently Viewed items display correctly for authenticated web users (not empty)"
    - "Recently Viewed items display in most-recently-viewed-first order on web lists page"
    - "Desktop browse tab lists panel splitter remains resizable between tree and items sections"
  artifacts:
    - path: "web/user_lists.py"
      provides: "Fixed get_items_in_list_sync for 'recent' with authenticated users"
      contains: "get_recent_items"
    - path: "genizah_app.py"
      provides: "View-time sort for recently viewed in desktop browse"
      contains: "sort_by"
  key_links:
    - from: "web/user_lists.py:get_items_in_list_sync"
      to: "web/supabase_client.py:get_recent_items"
      via: "special-case for list_id == 'recent'"
      pattern: "get_recent_items"
    - from: "genizah_app.py:browse_on_list_selected"
      to: "genizah_core.py:get_items_sorted"
      via: "sort_by parameter"
      pattern: "sort_by"
---

<objective>
Fix two bugs in the Recently Viewed list across both apps:

1. **Desktop browse tab**: When clicking "Recently Viewed" in the browse lists panel, items are sorted by shelfmark (`get_items_sorted(list_id, sort_by='shelfmark')`) instead of by view time. Most recently viewed should appear first.

2. **Web (authenticated users)**: `get_items_in_list_sync('recent')` tries `int('recent')`, catches ValueError, and returns `[]`. The Supabase `get_recent_items()` function is never called. This means logged-in users see an empty Recently Viewed list on the lists page, home page, comment dialog, and joins panel.

The "resizable" aspect is already working -- both web (`ui.splitter`) and desktop (`QSplitter`) support dragging.

Purpose: Users who browse manuscripts expect Recently Viewed to reflect their actual browsing order.
Output: Fixed sort order on desktop, fixed empty list for authenticated web users.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@web/user_lists.py (WebListsManager — get_items_in_list_sync at line 534)
@web/supabase_client.py (get_recent_items at line 654, add_recent_item at line 665)
@genizah_core.py (ListsManager — get_items_sorted at line 9374, get_items_in_list at line 9155)
@genizah_app.py (browse_on_list_selected at line 14937)

<interfaces>
<!-- From web/supabase_client.py -->
```python
def get_recent_items(user_id: str, limit: int = 50) -> List[Dict]:
    """Get recent items for a user. Orders by viewed_at desc."""
    # Returns: [{'id': int, 'user_id': str, 'sys_id': str, 'shelfmark': str, 'title': str, 'fl_id': str, 'viewed_at': str}, ...]
```

<!-- From web/user_lists.py -->
```python
class WebListsManager:
    def get_items_in_list_sync(self, list_id: str) -> List[Dict]:
        # BUG: tries int('recent') -> ValueError -> returns []
        # Should special-case 'recent' to call get_recent_items(self.user_id)

    # Also fix async version:
    async def get_items_in_list(self, list_id: str) -> List[Dict]:
        # Same bug pattern
```

<!-- From genizah_core.py -->
```python
class ListsManager:
    def get_items_sorted(self, list_id, sort_by='shelfmark', reverse=False):
        # For 'recent' list, sort_by='shelfmark' destroys the view-time ordering
        # get_items_in_list('recent') already returns items in correct order
```

<!-- From genizah_app.py -->
```python
def browse_on_list_selected(self, item, column):
    # Line 14946: calls get_items_sorted(list_id, sort_by='shelfmark')
    # For 'recent' list, should preserve insertion order (no sort, or sort_by=None)
```
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Fix web authenticated user Recently Viewed (empty list bug)</name>
  <files>web/user_lists.py</files>
  <action>
In `WebListsManager.get_items_in_list_sync()` (line 534), add a special case BEFORE the `int(list_id)` conversion: if `list_id == 'recent'` and `self.is_authenticated`, call `get_recent_items(self.user_id)` and return the result. The Supabase `get_recent_items()` already returns items ordered by `viewed_at desc` (most recent first).

Transform the Supabase response format to match the expected item dict format used by callers:
- Map `sys_id`, `shelfmark`, `title`, `fl_id` directly from the Supabase row
- Set `item_id` to the row's `sys_id` (consistent with local ListsManager behavior)
- Callers expect: `{'item_id': str, 'sys_id': str, 'shelfmark': str, 'title': str, 'fl_id': str, ...}`

Apply the SAME fix to the async `get_items_in_list()` method (line 522) — same pattern, same special case for `list_id == 'recent'`.

Import `get_recent_items` is already imported at the top of the file (line 30).
  </action>
  <verify>
    <automated>python -c "from web.user_lists import WebListsManager; print('import ok')"</automated>
  </verify>
  <done>Authenticated web users see their Recently Viewed items (not empty) on lists page, home page, comment dialog, and joins panel. Items are ordered by most recently viewed first.</done>
</task>

<task type="auto">
  <name>Task 2: Fix desktop browse tab Recently Viewed sort order</name>
  <files>genizah_app.py</files>
  <action>
In `browse_on_list_selected()` (line 14946), check if the selected list is the 'recent' list. The list_id for the recent list is the string `'recent'`.

When `list_id == 'recent'`:
- Call `self.lists_mgr.get_items_in_list('recent')` directly instead of `get_items_sorted()`. This preserves the insertion order (most recently viewed first) since `recent_items` list is maintained with most-recent at index 0.
- Still enrich with metadata (shelfmark, title) from `self.meta_mgr` for display — copy the enrichment pattern from `get_items_sorted` (lines 9379-9384).

When `list_id != 'recent'`:
- Keep existing behavior: `self.lists_mgr.get_items_sorted(list_id, sort_by='shelfmark')`.

This is a targeted change at line 14946 only — replace:
```python
items = self.lists_mgr.get_items_sorted(list_id, sort_by='shelfmark')
```
with:
```python
if list_id == 'recent':
    items = self.lists_mgr.get_items_in_list('recent')
    # Enrich with metadata
    if self.meta_mgr:
        for item in items:
            sid = item.get('sys_id', '')
            shelfmark, title = self.meta_mgr.get_meta_for_id(sid)
            item['shelfmark'] = item.get('shelfmark_override') or shelfmark or 'Unknown'
            item['title'] = title or ''
else:
    items = self.lists_mgr.get_items_sorted(list_id, sort_by='shelfmark')
```
  </action>
  <verify>
    <automated>python -c "from genizah_core import ListsManager; m = ListsManager.__new__(ListsManager); m.data = {'recent_items': ['b','a','c'], 'items': {'a': {'sys_id': 'a', 'lists': []}, 'b': {'sys_id': 'b', 'lists': []}, 'c': {'sys_id': 'c', 'lists': []}}}; result = m.get_items_in_list('recent'); assert [r['sys_id'] for r in result] == ['b','a','c'], f'Order wrong: {result}'; print('PASS: recent items preserve insertion order')"</automated>
  </verify>
  <done>Desktop browse tab shows Recently Viewed items in most-recently-viewed-first order, while all other lists continue to sort by shelfmark.</done>
</task>

</tasks>

<verification>
1. Web (authenticated): Navigate to Lists page -> click Recently Viewed -> items should appear (not empty) in most-recently-viewed-first order
2. Web (anonymous): Navigate to Lists page -> click Recently Viewed -> items appear in view-time order (unchanged, was working)
3. Desktop: Open browse tab -> show lists panel -> click Recently Viewed -> items appear in most-recently-viewed-first order
4. Desktop: Click any other list -> items still sort by shelfmark (no regression)
</verification>

<success_criteria>
- Authenticated web users see populated Recently Viewed list ordered by viewed_at desc
- Desktop browse tab Recently Viewed shows items in view-time order (not shelfmark order)
- No regression in other list sort behavior
</success_criteria>

<output>
After completion, create `.planning/quick/260325-eol-fix-browse-tab-recently-viewed-list-sort/260325-eol-SUMMARY.md`
</output>
