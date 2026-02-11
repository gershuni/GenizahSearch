---
phase: quick-12
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - web/pages/corrections.py
  - supabase_corrections_client.py
autonomous: true
must_haves:
  truths:
    - "Leaderboard shows actual reputation points (not 0) for each contributor"
    - "Leaderboard shows actual correction counts (not 0) for each contributor"
    - "User info bar shows actual reputation score (not 0)"
    - "Desktop browse tab shows contributor username instead of 'User'"
  artifacts:
    - path: "web/pages/corrections.py"
      provides: "Fixed field names and correction count query"
    - path: "supabase_corrections_client.py"
      provides: "Profile data fetched and merged into corrections"
  key_links:
    - from: "web/pages/corrections.py"
      to: "profiles table"
      via: "reputation field (not reputation_score)"
      pattern: "user\\.get\\('reputation'"
    - from: "supabase_corrections_client.py"
      to: "profiles table"
      via: "batch profile lookup for correction authors"
      pattern: "profiles_map"
---

<objective>
Fix two bugs in the corrections/leaderboard system:
1. Web leaderboard shows 0 points and 0 corrections for all users (wrong field names + missing count query)
2. Desktop browse tab shows "User" instead of actual contributor names (missing profile fetch in desktop client)

Purpose: Users cannot see who contributed corrections or their reputation, undermining the collaborative corrections feature.
Output: Working leaderboard with real data; desktop showing real contributor names.
</objective>

<execution_context>
@C:/Users/gersh/.claude/get-shit-done/workflows/execute-plan.md
@C:/Users/gersh/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@web/pages/corrections.py
@web/supabase_client.py
@supabase_corrections_client.py
@genizah_app.py
</context>

<tasks>

<task type="auto">
  <name>Task 1: Fix web leaderboard field names and add correction count query</name>
  <files>web/pages/corrections.py</files>
  <action>
Three fixes in `web/pages/corrections.py`:

1. **Line 79** (user info bar): Change `reputation_score` to `reputation`.
   - Before: `user.get('reputation_score', 0)`
   - After: `user.get('reputation', 0)`

2. **Line 678** (leaderboard badge): Change `reputation_score` to `reputation`.
   - Before: `user.get('reputation_score', 0)`
   - After: `user.get('reputation', 0)`

3. **Line 677** (leaderboard corrections count): The `corrections_count` field does not exist in the profiles table. Add a batch correction count query in `create_leaderboard_view()`.

   After fetching the 20 profiles (line 648-649), add a batch query to get correction counts:
   ```python
   # Batch-fetch correction counts for leaderboard users
   if users:
       user_ids = [u['id'] for u in users if u.get('id')]
       if user_ids:
           try:
               from web.supabase_client import get_client as get_sc
               counts_client = get_sc()
               for uid in user_ids:
                   count_resp = counts_client.table('corrections').select('id', count='exact').eq('author_id', uid).eq('status', 'approved').execute()
                   # Find this user in the list and set the count
                   for u in users:
                       if u.get('id') == uid:
                           u['_corrections_count'] = count_resp.count if count_resp.count is not None else 0
                           break
           except Exception:
               pass  # Counts will show 0 on error
   ```

   IMPORTANT: A simpler single-batch approach is not available because Supabase Python client does not support GROUP BY in `.select()`. The profiles list is at most 20, so 20 individual count queries is acceptable for a leaderboard page.

   Alternatively, use the existing `get_user_corrections_count(user_id)` function from `web/supabase_client.py` (line 339) which already does exactly this per-user query. This is cleaner:
   ```python
   from web.supabase_client import get_user_corrections_count
   if users:
       for u in users:
           if u.get('id'):
               u['_corrections_count'] = get_user_corrections_count(u['id'])
   ```

   Then change line 677 from `user.get('corrections_count', 0)` to `user.get('_corrections_count', 0)`.

   Use the underscore-prefixed key `_corrections_count` to avoid confusion with any future profile column.
  </action>
  <verify>
Run the web app (`python -m web.main`), navigate to the corrections page, click the Leaderboard tab. Verify:
- Each user shows their actual reputation points (not 0)
- Each user shows their actual correction count (not 0)
- The user info bar at the top also shows the correct reputation score
  </verify>
  <done>
Leaderboard displays real reputation values and correction counts for all listed contributors. User info bar shows correct reputation.
  </done>
</task>

<task type="auto">
  <name>Task 2: Fix desktop contributor names by adding profile batch lookup</name>
  <files>supabase_corrections_client.py</files>
  <action>
In `supabase_corrections_client.py`, modify `get_corrections_for_document()` (line 820) to fetch and merge profile data into corrections before parsing, following the exact pattern used in the web version (`web/supabase_client.py` lines 685-700).

After `response = query.order('created_at', desc=True).execute()` (line 831) and before the `return` on line 832, insert:

```python
corrections_data = response.data or []

# Batch-fetch profile data for correction authors (same pattern as web client)
if corrections_data:
    user_ids = set(c.get('author_id') for c in corrections_data if c.get('author_id'))
    if user_ids:
        try:
            profiles_response = client.table('profiles').select(
                'id, full_name, username'
            ).in_('id', list(user_ids)).execute()
            profiles_map = {p['id']: p for p in (profiles_response.data or [])}
            for c in corrections_data:
                aid = c.get('author_id')
                if aid and aid in profiles_map:
                    c['profiles'] = profiles_map[aid]
                else:
                    c['profiles'] = {}
        except Exception as e:
            logger.warning(f"Failed to fetch profiles for corrections: {e}")
            for c in corrections_data:
                c['profiles'] = {}

return [self._parse_correction(c) for c in corrections_data]
```

Replace the existing `return [self._parse_correction(c) for c in response.data or []]` on line 832 with this block.

The `_parse_correction` method (line 933) already reads `profiles.get('username')` correctly -- it just needs the profiles data to actually be present in the dict, which this fix provides.

This means `corr.author_username` will now contain the real username, so `genizah_app.py` lines 3096 and 6251 (`corr.author_username or 'User'`) will show actual names instead of always falling back to "User". No changes needed in genizah_app.py.
  </action>
  <verify>
Run the desktop app (`python genizah_app.py`), browse to a document that has corrections. In the version selector area, verify that contributor names show actual usernames instead of "User".

Also verify no errors in the console log related to profile fetching.
  </verify>
  <done>
Desktop browse tab displays real contributor usernames for corrections instead of the generic "User" fallback.
  </done>
</task>

</tasks>

<verification>
1. Web leaderboard: reputation points and correction counts are non-zero for active contributors
2. Web user info bar: reputation displays correctly
3. Desktop browse tab: corrections show actual contributor usernames
4. No regression: corrections page still loads, leaderboard still renders, desktop browse still works
</verification>

<success_criteria>
- All four `reputation_score` -> `reputation` and `corrections_count` -> `_corrections_count` field name fixes applied in web/pages/corrections.py
- Batch profile lookup added to supabase_corrections_client.py get_corrections_for_document()
- Leaderboard shows real data for both reputation and correction counts
- Desktop shows real contributor names
</success_criteria>

<output>
After completion, create `.planning/quick/12-fix-leaderboard-0-points-and-desktop-con/12-SUMMARY.md`
</output>
