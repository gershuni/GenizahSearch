---
phase: quick-11
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - web/pages/profile.py
  - web/supabase_client.py
  - web/pages/admin.py
autonomous: true

must_haves:
  truths:
    - "Profile page displays correct reputation score from database"
    - "Profile page shows accurate count of approved corrections"
    - "Approving a correction increments author's reputation in database"
  artifacts:
    - path: "web/pages/profile.py"
      provides: "Profile display with correct field names"
      min_lines: 200
    - path: "web/supabase_client.py"
      provides: "Function to count approved corrections per user"
      exports: ["get_user_corrections_count"]
    - path: "web/pages/admin.py"
      provides: "Reputation increment on correction approval"
      min_lines: 110
  key_links:
    - from: "web/pages/profile.py"
      to: "profiles.reputation"
      via: "profile.get('reputation', 0)"
      pattern: "reputation(?!_score)"
    - from: "web/pages/profile.py"
      to: "get_user_corrections_count"
      via: "function call with user_id"
      pattern: "get_user_corrections_count\\("
    - from: "web/pages/admin.py"
      to: "profiles.reputation"
      via: "increment on approval"
      pattern: "reputation.*\\+.*1"
---

<objective>
Fix profile page always displaying 0 reputation and 0 corrections for users with actual approved corrections.

Purpose: Users need to see accurate reputation scores and correction counts on their profile pages
Output: Profile page showing live data from database with reputation increments on approval
</objective>

<execution_context>
@C:/Users/gersh/.claude/get-shit-done/workflows/execute-plan.md
@C:/Users/gersh/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.planning/STATE.md
@docs/guides/SUPABASE_GUIDE.md

## Bug Analysis

Three distinct bugs causing profile page to show 0/0:

**Bug 1: Wrong field name (profile.py:181)**
- Code reads `profile.get('reputation_score', 0)`
- Actual database column: `reputation` (per SUPABASE_GUIDE.md line 89)

**Bug 2: Non-existent field (profile.py:185)**
- Code reads `profile.get('corrections_count', 0)`
- This field doesn't exist in profiles table
- Need to query corrections table to count approved corrections

**Bug 3: Missing reputation update (admin.py:80-100)**
- `update_correction_status()` only updates correction row status
- Never increments author's reputation score when approving
</context>

<tasks>

<task type="auto">
  <name>Fix profile field names and add corrections count query</name>
  <files>
web/pages/profile.py
web/supabase_client.py
  </files>
  <action>
**1. Add corrections count function to supabase_client.py (after get_profile around line 337):**

```python
def get_user_corrections_count(user_id: str) -> int:
    """Get count of approved corrections for a user."""
    try:
        client = get_client()
        response = client.table('corrections').select('id', count='exact').eq('user_id', user_id).eq('status', 'approved').execute()
        return response.count if response.count is not None else 0
    except Exception:
        return 0
```

**2. Fix profile.py field name and add corrections count (lines 181-185):**

Change line 181 from:
```python
ui.label(str(profile.get('reputation_score', 0))).classes('font-medium')
```

To:
```python
ui.label(str(profile.get('reputation', 0))).classes('font-medium')
```

Change line 185 from:
```python
ui.label(str(profile.get('corrections_count', 0))).classes('font-medium')
```

To:
```python
from web.supabase_client import get_user_corrections_count
# ... (at line 185):
corrections_count = get_user_corrections_count(user_id)
ui.label(str(corrections_count)).classes('font-medium')
```

Note: The import should be added at the top of profile.py around line 5-10 where other supabase_client imports are.
  </action>
  <verify>
1. Check supabase_client.py has `get_user_corrections_count` function
2. Check profile.py line 181 reads `profile.get('reputation', 0)` not `reputation_score`
3. Check profile.py calls `get_user_corrections_count(user_id)` to get corrections count
4. Run: `python -m pytest tests/test_supabase_client.py -v` (if tests exist)
  </verify>
  <done>
- `get_user_corrections_count` function exists in supabase_client.py
- profile.py uses correct `reputation` field name
- profile.py queries live corrections count instead of non-existent profile field
  </done>
</task>

<task type="auto">
  <name>Increment reputation on correction approval</name>
  <files>
web/pages/admin.py
  </files>
  <action>
**Modify `update_correction_status()` in admin.py (lines 80-100):**

After line 97 (before `return {'success': True}`), add reputation increment when status is 'approved':

```python
        response = client.table('corrections').update(data).eq('id', correction_id).execute()

        # Increment author reputation when approving correction
        if status == 'approved' and response.data:
            try:
                # Get correction to find author
                correction = response.data[0]
                author_id = correction.get('user_id')

                if author_id:
                    # Get current reputation
                    profile_response = client.table('profiles').select('reputation').eq('id', author_id).single().execute()
                    current_reputation = profile_response.data.get('reputation', 0) if profile_response.data else 0

                    # Increment reputation by 1
                    client.table('profiles').update({'reputation': current_reputation + 1}).eq('id', author_id).execute()
            except Exception as e:
                print(f"Warning: Failed to update reputation for correction {correction_id}: {e}")
                # Don't fail the approval if reputation update fails

        return {'success': True} if response.data else {'error': 'Update failed'}
```

This ensures:
- Reputation increments only on approval (not rejection)
- Failure to update reputation doesn't block the correction approval
- Current reputation is fetched to avoid race conditions
  </action>
  <verify>
1. Check admin.py `update_correction_status` includes reputation increment logic after line 97
2. Check it only increments when `status == 'approved'`
3. Check it safely handles exceptions (try/except with warning)
4. Run web app: `python -m web.main`
5. Test manually: Approve a correction via admin panel, verify author's reputation increases by 1
  </verify>
  <done>
- `update_correction_status` increments author reputation on approval
- Reputation update is wrapped in try/except to avoid breaking approval flow
- Manual test confirms: approving correction → reputation+1 in database
  </done>
</task>

<task type="auto">
  <name>Test and verify fixes</name>
  <files>
web/pages/profile.py
web/supabase_client.py
web/pages/admin.py
  </files>
  <action>
**Manual verification steps:**

1. Start web app: `python -m web.main`
2. Navigate to a user profile page (must be logged in)
3. Check reputation displays current value (not 0)
4. Check corrections count displays actual approved corrections (not 0)
5. Go to admin panel, approve a pending correction
6. Refresh the author's profile page
7. Verify reputation incremented by 1
8. Verify corrections count increased

**Code verification:**
- Grep for `reputation_score` in profile.py → should find NONE (changed to `reputation`)
- Grep for `corrections_count` in profile.py → should find NONE (replaced with function call)
- Check admin.py contains reputation increment logic in `update_correction_status`
  </action>
  <verify>
```bash
# Check no references to wrong field names
grep -n "reputation_score" web/pages/profile.py  # Should return nothing
grep -n "corrections_count" web/pages/profile.py  # Should return nothing

# Check correct field name used
grep -n "profile.get('reputation'" web/pages/profile.py  # Should find line

# Check corrections count function exists and is called
grep -n "get_user_corrections_count" web/supabase_client.py  # Should find function definition
grep -n "get_user_corrections_count" web/pages/profile.py  # Should find import and call

# Check reputation increment on approval
grep -n "status == 'approved'" web/pages/admin.py  # Should find conditional
grep -n "reputation.*+ 1" web/pages/admin.py  # Should find increment

# Run existing tests (shouldn't break anything)
python -m pytest tests/ -v
```
  </verify>
  <done>
- All grep checks pass (correct field names, function exists)
- Profile page displays live reputation and corrections count
- Approving corrections increments author reputation
- All existing tests still pass
  </done>
</task>

</tasks>

<verification>
**Functional verification:**
1. Profile page shows non-zero reputation for users with reputation > 0
2. Profile page shows accurate approved corrections count
3. Approving a correction in admin panel increments author's reputation by 1

**Code verification:**
- No references to `reputation_score` or `corrections_count` in profile.py
- `get_user_corrections_count` function exists in supabase_client.py
- `update_correction_status` in admin.py increments reputation on approval
</verification>

<success_criteria>
- User profile page displays correct reputation value from `profiles.reputation` column
- User profile page displays accurate count of approved corrections from live query
- Admin approval of correction increments author's reputation in database
- No regression: existing tests pass, no errors in web app logs
</success_criteria>

<output>
After completion, create `.planning/quick/11-fix-profile-page-showing-0-reputation-an/11-SUMMARY.md`
</output>
