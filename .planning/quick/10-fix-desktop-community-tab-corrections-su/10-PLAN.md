---
phase: quick-10
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - genizah_app.py
  - supabase_corrections_client.py
autonomous: true

must_haves:
  truths:
    - "Desktop Community tab loads all corrections without AttributeError"
    - "Desktop Community tab loads only user-created joins (not system imports)"
  artifacts:
    - path: "genizah_app.py"
      provides: "Correct method call to get_all_corrections"
      contains: "get_all_corrections"
    - path: "supabase_corrections_client.py"
      provides: "source filter in search_joins"
      contains: "eq.*source"
  key_links:
    - from: "genizah_app.py:12105"
      to: "supabase_corrections_client.py:get_all_corrections"
      via: "method call"
      pattern: "get_all_corrections\\(page_size=20\\)"
    - from: "genizah_app.py:12525"
      to: "supabase_corrections_client.py:search_joins"
      via: "method call with source filter"
      pattern: "search_joins\\(source='user'"
---

<objective>
Fix two bugs in the desktop Community tab that prevent corrections and joins from loading correctly.

Purpose: The Community tab crashes on load because `search_corrections` doesn't exist on `SupabaseCorrectionsClient`, and the joins listing shows all joins (including system imports) instead of only user-created ones.
Output: Working Community tab with correct method call and proper source filtering.
</objective>

<execution_context>
@C:/Users/gersh/.claude/get-shit-done/workflows/execute-plan.md
@C:/Users/gersh/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@genizah_app.py (lines 12095-12116 for corrections bug, lines 12518-12543 for joins caller)
@supabase_corrections_client.py (lines 866-875 for get_all_corrections, lines 1378-1410 for search_joins)
</context>

<tasks>

<task type="auto">
  <name>Task 1: Fix search_corrections call and add source filter to search_joins</name>
  <files>genizah_app.py, supabase_corrections_client.py</files>
  <action>
**Bug 1 - genizah_app.py line 12105:**
Replace:
```python
corrections, total = self.corrections_client.search_corrections(page_size=20)
```
With:
```python
corrections, total = self.corrections_client.get_all_corrections(page_size=20)
```

The `get_all_corrections` method has signature `(status=None, document_id=None, search_text=None, page=1, page_size=20) -> Tuple[List[Correction], int]` which returns the same `(corrections, total)` tuple the caller expects. No other changes needed at the call site.

**Bug 2 - supabase_corrections_client.py, `search_joins` method (line 1378-1410):**
Add source filtering between the `relationship_type` check (line 1400) and the `response = q.order(...)` call (line 1402). Insert:
```python
if source:
    q = q.eq('source', source)
```

This ensures the caller at genizah_app.py:12525 (`search_joins(source='user', limit=20)`) correctly filters to user-created joins only, excluding system/PGP-imported joins.
  </action>
  <verify>
1. `python -c "from supabase_corrections_client import SupabaseCorrectionsClient; print(hasattr(SupabaseCorrectionsClient, 'get_all_corrections'))"` returns True
2. Grep genizah_app.py for `search_corrections` -- should return zero matches (the old call is gone)
3. Grep supabase_corrections_client.py search_joins method for `eq.*source` -- should find the new filter line
4. `python -c "import ast; ast.parse(open('genizah_app.py').read()); print('OK')"` -- syntax check passes
5. `python -c "import ast; ast.parse(open('supabase_corrections_client.py').read()); print('OK')"` -- syntax check passes
  </verify>
  <done>
- genizah_app.py calls `get_all_corrections(page_size=20)` instead of the nonexistent `search_corrections`
- `search_joins` filters by `source` column when the parameter is provided
- Both files parse without syntax errors
  </done>
</task>

</tasks>

<verification>
- No references to `search_corrections` remain in genizah_app.py
- The `search_joins` method applies `source` filter when parameter is non-None
- Both modified files have valid Python syntax
- Desktop app can be imported without errors: `python -c "import genizah_app"`
</verification>

<success_criteria>
Desktop Community tab loads without AttributeError on corrections, and the "All Joins" list shows only user-created joins (source='user') rather than all joins including system imports.
</success_criteria>

<output>
After completion, create `.planning/quick/10-fix-desktop-community-tab-corrections-su/10-SUMMARY.md`
</output>
