---
phase: quick-14
plan: 1
type: execute
wave: 1
depends_on: []
files_modified:
  - shared/fjms_service.py
  - web/pages/search.py
  - web/pages/parallels.py
  - genizah_app.py
autonomous: true
requirements: [FIX-DOMAIN-MULTI-PARENT]

must_haves:
  truths:
    - "Excluding 'Other' under 'Liturgy' does NOT exclude 'Other' under 'Kabbalah'"
    - "Domain filter dialog shows 'Other' under each parent it belongs to, with independent checkboxes"
    - "Manuscripts classified as 'Other' under different parents are treated as distinct domain assignments"
  artifacts:
    - path: "shared/fjms_service.py"
      provides: "qualify_domain_name helper and get_ambiguous_domains utility"
    - path: "web/pages/search.py"
      provides: "Qualified domain names in dedup and filter logic"
    - path: "web/pages/parallels.py"
      provides: "Qualified domain names in parallels dedup and filter logic"
    - path: "genizah_app.py"
      provides: "Qualified domain names in desktop dedup and filter logic"
  key_links:
    - from: "shared/fjms_service.py"
      to: "web/pages/search.py"
      via: "qualify_domain_name used during domain dedup"
    - from: "shared/fjms_service.py"
      to: "genizah_app.py"
      via: "qualify_domain_name used during domain dedup"
---

<objective>
Fix domain filtering for domains that appear as children of multiple parent categories (currently only "Other", which appears under 15 parents).

Problem: The domain "Other" is a child of 15 different parent categories (Liturgy, Kabbalah, Rabbinic Literature, etc.). The current code uses bare domain names as identifiers, so all "Other" entries collapse into a single checkbox. Unchecking "Other" under one parent incorrectly excludes manuscripts classified as "Other" under ALL parents.

Root cause: The per-manuscript domain dedup logic (search.py:2036, parallels.py:1297, genizah_app.py:15878) stores bare domain names like `["Other"]`. The filter dialog and exclusion logic both key on these bare names, making them indistinguishable across parents.

Fix: Qualify ambiguous child domains with their parent name: `"Other (Liturgy and Brakhot)"` instead of `"Other"`. This makes each parent's "Other" a distinct entry in the domain list, filter dialog, and exclusion set. Only domains appearing under multiple parents need qualification (currently just "Other").

Output: Working domain filtering where "Other" under different parents can be independently included/excluded.
</objective>

<execution_context>
@C:/Users/gersh/.claude/get-shit-done/workflows/execute-plan.md
@C:/Users/gersh/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@shared/fjms_service.py
@web/pages/search.py (lines 2020-2070 for dedup, 1682-1855 for filter dialog, 1856-1893 for exclusion apply)
@web/pages/parallels.py (lines 1280-1370 for dedup and filter)
@genizah_app.py (lines 4780-4995 for DomainFilterDialog, 15263-15355 for desktop filter, 15860-15892 for desktop dedup)
</context>

<tasks>

<task type="auto">
  <name>Task 1: Add domain qualification helper to fjms_service.py</name>
  <files>shared/fjms_service.py</files>
  <action>
Add a module-level helper function `qualify_domain_name(domain, parent_domain)` and a cached set `_AMBIGUOUS_DOMAINS` to `shared/fjms_service.py`.

1. Add a module-level frozen set near the top (after `GENERIC_SOURCE_NAMES`):
```python
# Domains that appear as children of multiple parent categories.
# These need qualification with parent name to be distinguishable in filters.
# Data: SELECT Domain FROM domains WHERE ParentDomain IS NOT NULL AND ParentDomain != Domain
#        GROUP BY Domain HAVING COUNT(DISTINCT ParentDomain) > 1
AMBIGUOUS_CHILD_DOMAINS = frozenset({'Other'})
```

2. Add a helper function (after the `is_team_source` / `get_team_display_name` functions, before `_find_project_root`):
```python
def qualify_domain_name(domain: str, parent_domain: str = None) -> str:
    """Qualify ambiguous child domain names with their parent for uniqueness.

    Domains like "Other" appear under multiple parent categories. Without
    qualification, filtering by "Other" in one parent would affect all parents.
    This function returns "Other (Liturgy and Brakhot)" for ambiguous domains
    and the bare domain name for unambiguous ones.

    Args:
        domain: The domain name (e.g., "Other", "Piyyut").
        parent_domain: The parent domain name (e.g., "Liturgy and Brakhot").

    Returns:
        Qualified name if ambiguous, otherwise the bare domain name.
    """
    if domain in AMBIGUOUS_CHILD_DOMAINS and parent_domain and parent_domain != domain:
        return f"{domain} ({parent_domain})"
    return domain


def unqualify_domain_name(qualified: str) -> tuple[str, str]:
    """Extract bare domain and parent from a qualified domain name.

    Returns (domain, parent_domain) tuple. For unqualified names,
    parent_domain is empty string.

    Args:
        qualified: e.g., "Other (Liturgy and Brakhot)" or "Piyyut".

    Returns:
        Tuple of (domain, parent_domain).
    """
    if ' (' in qualified and qualified.endswith(')'):
        idx = qualified.index(' (')
        domain = qualified[:idx]
        parent = qualified[idx + 2:-1]
        if domain in AMBIGUOUS_CHILD_DOMAINS:
            return (domain, parent)
    return (qualified, '')
```

3. Update `get_domain_hierarchy()` to NOT merge ambiguous child domains across parents. Currently the dedup block at lines 566-591 merges child domains that also appear as standalone roots. For "Other", this should still work correctly since "Other" doesn't appear as a standalone root. No change needed to get_domain_hierarchy itself.

4. No changes needed to `get_manuscripts_by_domain()` -- it searches by raw Domain column which is correct.
  </action>
  <verify>Run `python -c "from shared.fjms_service import qualify_domain_name, unqualify_domain_name, AMBIGUOUS_CHILD_DOMAINS; print(qualify_domain_name('Other', 'Liturgy')); print(qualify_domain_name('Piyyut', 'Something')); print(unqualify_domain_name('Other (Liturgy)')); print(unqualify_domain_name('Piyyut'))"` -- should print `Other (Liturgy)`, `Piyyut`, `('Other', 'Liturgy')`, `('Piyyut', '')`.</verify>
  <done>Helper functions exist and correctly qualify/unqualify ambiguous domain names. AMBIGUOUS_CHILD_DOMAINS frozenset exported.</done>
</task>

<task type="auto">
  <name>Task 2: Update domain dedup and filter logic in all three UIs</name>
  <files>web/pages/search.py, web/pages/parallels.py, genizah_app.py</files>
  <action>
Update the domain dedup logic and filter dialog in all three locations to use qualified domain names for ambiguous child domains.

**A. Web search (web/pages/search.py)**

1. Domain dedup (around line 2034-2038): Change from bare domain names to qualified names.
   Replace:
   ```python
   child_names = {d['domain'] for d in doms}
   filtered = [d['domain'] for d in doms if not (d.get('parent_domain') and d['parent_domain'] in child_names and d['parent_domain'] != d['domain'])]
   ```
   With:
   ```python
   from shared.fjms_service import qualify_domain_name
   child_names = {d['domain'] for d in doms}
   filtered = [qualify_domain_name(d['domain'], d.get('parent_domain')) for d in doms if not (d.get('parent_domain') and d['parent_domain'] in child_names and d['parent_domain'] != d['domain'])]
   ```
   Also update the domain_name_map building (lines 2039-2043) to include qualified names:
   ```python
   for d in doms:
       qname = qualify_domain_name(d['domain'], d.get('parent_domain'))
       if d.get('domain_heb') and qname not in search_state.domain_name_map:
           search_state.domain_name_map[qname] = d['domain_heb']
       if d.get('domain_heb') and d['domain'] not in search_state.domain_name_map:
           search_state.domain_name_map[d['domain']] = d['domain_heb']
       if d.get('parent_domain_heb') and d.get('parent_domain') and d['parent_domain'] not in search_state.domain_name_map:
           search_state.domain_name_map[d['parent_domain']] = d['parent_domain_heb']
   ```

2. Filter dialog domain counting (around line 1696-1699): The `domain_counts` loop iterates over `search_state.all_result_domains[sys_id]` which now contains qualified names like `"Other (Liturgy)"`. The hierarchy matching (lines 1703-1736) needs to match qualified child names against hierarchy children. Update the hierarchy building loop:
   - When checking if a child domain is in domain_counts, also check qualified variants:
   ```python
   for child in info.get('children', []):
       # Check both bare and qualified names
       qname = qualify_domain_name(child['domain'], parent_name)
       if qname in domain_counts:
           children_in_results.append({
               'domain': qname,  # Use qualified name as key
               'domain_heb': child.get('domain_heb', child['domain']),
               'count': domain_counts[qname],
           })
       elif child['domain'] in domain_counts and child['domain'] not in AMBIGUOUS_CHILD_DOMAINS:
           children_in_results.append({
               'domain': child['domain'],
               'domain_heb': child.get('domain_heb', child['domain']),
               'count': domain_counts[child['domain']],
           })
   ```
   - Similarly for parent_in_results check.
   - Import AMBIGUOUS_CHILD_DOMAINS at top of the function.

3. The `_apply_domain_exclusions()` function (lines 1856-1892) and inline exclusion logic (lines 2134-2157) already use `all(d in search_state.domain_exclusions for d in result_domains)` which will work correctly with qualified names since result_domains now contains qualified names and the exclusion set will also use qualified names from the dialog checkboxes.

4. The `calc_visible()` function inside the dialog (lines 1761-1773) uses the same pattern and will work correctly.

**B. Web parallels (web/pages/parallels.py)**

Same pattern as search.py. Update the dedup block (around line 1295-1304):
```python
from shared.fjms_service import qualify_domain_name
child_names = {d['domain'] for d in doms}
filtered_doms = [qualify_domain_name(d['domain'], d.get('parent_domain')) for d in doms if not (d.get('parent_domain') and d['parent_domain'] in child_names and d['parent_domain'] != d['domain'])]
```
Also update domain_name_map building to include qualified names (same pattern as search.py).

The parallels domain filter dialog (`_open_parallels_domain_filter_dialog`, around line 1382) uses the same hierarchy-based approach. Apply the same qualified name matching as in search.py.

**C. Desktop app (genizah_app.py)**

1. Domain dedup (around line 15876-15887): Same pattern:
   ```python
   from shared.fjms_service import qualify_domain_name
   child_names = {d['domain'] for d in doms}
   filtered = [qualify_domain_name(d['domain'], d.get('parent_domain')) for d in doms if not (d.get('parent_domain') and d['parent_domain'] in child_names and d['parent_domain'] != d['domain'])]
   ```
   Also update domain_name_map and domain_counts to use qualified names.

2. DomainFilterDialog._populate_tree (around line 4838-4893): The dialog receives `result_domains` as a `{domain_name: count}` dict. With qualified names, this dict will have entries like `"Other (Liturgy and Brakhot)": 63`. The tree population iterates the hierarchy and needs to match qualified names:
   - When checking `child['domain'] in self.result_domains`, also check the qualified variant:
   ```python
   from shared.fjms_service import qualify_domain_name, AMBIGUOUS_CHILD_DOMAINS
   # Inside the children loop:
   qname = qualify_domain_name(child['domain'], parent_name)
   if qname in self.result_domains:
       child_domain_key = qname
   elif child['domain'] in self.result_domains and child['domain'] not in AMBIGUOUS_CHILD_DOMAINS:
       child_domain_key = child['domain']
   else:
       continue
   ```
   - Use `child_domain_key` for count lookup and as the UserRole data.

3. DomainFilterDialog.get_excluded_domains (lines 4979-4995): Already reads UserRole data, which will now contain qualified names. No change needed.

4. Desktop _apply_domain_exclusions (lines 15309-15348): Already uses `all(d in self._domain_exclusions for d in result_domains)`. With qualified names in both, this works correctly. No change needed.

5. Composition domain filter (`_collect_comp_domain_data`, around line 15359): Apply the same qualified name pattern to the composition search domain dedup (around line 15408).

**D. Display name helper**

The `_domain_display_name` functions in both web and desktop need to handle qualified names. For display, a qualified name like `"Other (Liturgy and Brakhot)"` should show as `"Other (Liturgy and Brakhot)"` in English and with Hebrew parent if available. The current `_domain_display_name` in the web (around search.py) and `_domain_display_name` in desktop (line 15299) look up the name in `domain_name_map`. Since we also add qualified names to the map, this should work. But for Hebrew display of qualified names, store: `domain_name_map["Other (Liturgy and Brakhot)"] = "אחר (ליטורגיה וברכות)"` etc. Since the Hebrew parent name is available from `parent_domain_heb`, construct it during dedup.

In the dedup section, when building domain_name_map for qualified names:
```python
qname = qualify_domain_name(d['domain'], d.get('parent_domain'))
if qname != d['domain'] and d.get('domain_heb') and d.get('parent_domain_heb'):
    search_state.domain_name_map[qname] = f"{d['domain_heb']} ({d['parent_domain_heb']})"
```

Apply the same pattern in desktop and parallels.
  </action>
  <verify>
1. Run `pytest tests/test_fjms_service.py -v` -- existing tests pass (service layer unchanged except new helpers).
2. Run `python -c "from web.pages.search import *"` -- no import errors.
3. Run `python -c "from shared.fjms_service import qualify_domain_name, unqualify_domain_name, AMBIGUOUS_CHILD_DOMAINS; assert 'Other' in AMBIGUOUS_CHILD_DOMAINS"`.
4. Manual verification: Run the web app (`python -m web.main`), search for a common term, open domain filter dialog, verify "Other" appears as separate entries under different parents (e.g., "Other (Liturgy and Brakhot)", "Other (Kabbalah)"), and that unchecking one does not affect manuscripts under other parents.
  </verify>
  <done>Domain filtering correctly distinguishes "Other" (and any future ambiguous domains) across different parent categories. Unchecking "Other" under one parent only excludes manuscripts classified under that specific parent's "Other", not all "Other" entries.</done>
</task>

</tasks>

<verification>
1. `pytest tests/test_fjms_service.py -v` -- all existing tests pass
2. `python -c "from shared.fjms_service import qualify_domain_name, AMBIGUOUS_CHILD_DOMAINS"` -- imports work
3. Web app domain filter dialog shows "Other" as distinct entries per parent
4. Desktop app domain filter dialog shows "Other" as distinct entries per parent
5. Excluding one parent's "Other" does not affect results under other parents
</verification>

<success_criteria>
- qualify_domain_name and unqualify_domain_name helpers exist in shared/fjms_service.py
- Domain dedup in all 4 locations (web search, web parallels, desktop search, desktop composition) uses qualified names
- Filter dialogs in all UIs show ambiguous child domains as separate per-parent entries
- Exclusion filtering uses qualified names so per-parent filtering works independently
- All existing fjms_service tests pass
</success_criteria>

<output>
After completion, create `.planning/quick/14-fix-domain-filtering-for-misc-categories/14-SUMMARY.md`
</output>
