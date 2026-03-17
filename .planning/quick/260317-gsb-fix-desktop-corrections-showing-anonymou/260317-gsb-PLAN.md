---
phase: quick
plan: 260317-gsb
type: execute
wave: 1
depends_on: []
files_modified: [supabase_corrections_client.py]
autonomous: true
must_haves:
  truths:
    - "Desktop 'My Corrections' tab shows real usernames instead of Anonymous"
    - "Desktop 'All Corrections' tab shows real usernames instead of Anonymous"
  artifacts:
    - path: "supabase_corrections_client.py"
      provides: "Profile batch-fetch in get_my_corrections and get_all_corrections"
      contains: "profiles_map"
  key_links:
    - from: "supabase_corrections_client.py:get_my_corrections"
      to: "profiles table"
      via: "batch select with in_() on user_ids"
      pattern: "client\\.table\\('profiles'\\)"
    - from: "supabase_corrections_client.py:get_all_corrections"
      to: "profiles table"
      via: "batch select with in_() on user_ids"
      pattern: "client\\.table\\('profiles'\\)"
---

<objective>
Fix desktop corrections showing "Anonymous" for all users by adding profile batch-fetch to get_my_corrections and get_all_corrections.

Purpose: Both methods fetch corrections but skip the profile lookup that get_corrections_for_document already does (lines 847-865). Without profiles, author_username is always None, so UI shows "Anonymous".
Output: Both methods enriched with profile data, usernames display correctly.
</objective>

<execution_context>
@C:/Users/gersh/.claude/get-shit-done/workflows/execute-plan.md
@C:/Users/gersh/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@supabase_corrections_client.py
</context>

<interfaces>
<!-- Existing working pattern from get_corrections_for_document (lines 847-865): -->
```python
if corrections_data:
    user_ids = list(set(c.get('author_id') for c in corrections_data if c.get('author_id')))
    if user_ids:
        try:
            profiles_response = client.table('profiles').select(
                'id, full_name, username'
            ).in_('id', user_ids).execute()
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
```
</interfaces>

<tasks>

<task type="auto">
  <name>Task 1: Add profile batch-fetch to get_my_corrections and get_all_corrections</name>
  <files>supabase_corrections_client.py</files>
  <action>
Two edits in supabase_corrections_client.py:

1. **get_my_corrections** (around line 891): After `response = query.order(...).range(...).execute()` and before `corrections = [self._parse_correction(c) ...]`, insert the profile batch-fetch block. Use `response.data or []` as corrections_data:
   ```python
   corrections_data = response.data or []

   # Batch-fetch profile data for correction authors
   if corrections_data:
       user_ids = list(set(c.get('author_id') for c in corrections_data if c.get('author_id')))
       if user_ids:
           try:
               profiles_response = client.table('profiles').select(
                   'id, full_name, username'
               ).in_('id', user_ids).execute()
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

   corrections = [self._parse_correction(c) for c in corrections_data]
   ```

2. **get_all_corrections** (around line 929): Same pattern. After `response = query.order(...).range(...).execute()` and before `corrections = [self._parse_correction(c) ...]`, insert the identical profile batch-fetch block.

Both methods already have `client` in scope. The _parse_correction method already handles the `profiles` key to populate author_username.
  </action>
  <verify>
    <automated>python -c "import supabase_corrections_client; print('Import OK')"</automated>
  </verify>
  <done>Both get_my_corrections and get_all_corrections include profile batch-fetch identical to the working pattern in get_corrections_for_document. The _parse_correction method receives profile data and populates author_username correctly.</done>
</task>

</tasks>

<verification>
- `python -c "import supabase_corrections_client"` succeeds without errors
- Code inspection: both methods contain `client.table('profiles').select(` block
- Both methods use `corrections_data` variable before passing to `_parse_correction`
</verification>

<success_criteria>
- get_my_corrections fetches profiles and passes them to _parse_correction
- get_all_corrections fetches profiles and passes them to _parse_correction
- No regressions in get_corrections_for_document (untouched)
</success_criteria>

<output>
After completion, create `.planning/quick/260317-gsb-fix-desktop-corrections-showing-anonymou/260317-gsb-SUMMARY.md`
</output>
