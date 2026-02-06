---
phase: quick-001
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - web/components/project_tree.py
autonomous: true

must_haves:
  truths:
    - "Recently Viewed list appears at the top of the lists sidebar"
    - "Other system lists remain at the bottom"
    - "Projects and standalone lists appear in their normal positions"
  artifacts:
    - path: "web/components/project_tree.py"
      provides: "Reordered list rendering"
      contains: "Recently Viewed"
  key_links:
    - from: "web/components/project_tree.py"
      to: "system_lists filter"
      via: "list name check"
      pattern: "Recently Viewed"
---

<objective>
Move "Recently Viewed" list to the top of the lists sidebar in the web app.

Purpose: Users want quick access to recently viewed items without scrolling to the bottom of their lists.
Output: Modified project_tree.py with "Recently Viewed" rendered first.
</objective>

<execution_context>
@C:\Users\gersh\.claude/get-shit-done/workflows/execute-plan.md
@C:\Users\gersh\.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@web/components/project_tree.py
</context>

<tasks>

<task type="auto">
  <name>Task 1: Move Recently Viewed to top of sidebar</name>
  <files>web/components/project_tree.py</files>
  <action>
In `create_project_tree()` function (around lines 110-165), modify the rendering order:

1. Before the projects section (line 114), add a new section to render "Recently Viewed" first:
   - Extract "Recently Viewed" from system_lists (filter by name == 'Recently Viewed')
   - Render it with a header "Recent" (similar to "System" header style)
   - Use `_render_list_item()` with `show_color=True`

2. In the system lists section (lines 150-165), exclude "Recently Viewed" from being rendered again:
   - Filter: `other_system_lists = [l for l in system_lists if l.get('name') != 'Recently Viewed']`
   - Only render the "System" section if `other_system_lists` is not empty

Current order: Projects -> Standalone Lists -> System Lists (Recently Viewed at bottom)
New order: Recently Viewed -> Projects -> Standalone Lists -> Other System Lists
  </action>
  <verify>
Run the web app and visit /lists page:
```bash
python -m web.main
```
Visual check: "Recently Viewed" should appear at the very top of the sidebar, before any projects or other lists.
  </verify>
  <done>"Recently Viewed" list appears at the top of the lists sidebar, above projects and standalone lists. Other system lists (if any) remain at the bottom.</done>
</task>

</tasks>

<verification>
- [ ] Web app starts without errors
- [ ] /lists page loads successfully
- [ ] "Recently Viewed" appears at top of sidebar
- [ ] Projects appear below "Recently Viewed"
- [ ] Standalone lists appear in their normal position
- [ ] No duplicate "Recently Viewed" entries
</verification>

<success_criteria>
User sees "Recently Viewed" at the top of their lists sidebar, making it easily accessible without scrolling.
</success_criteria>

<output>
After completion, create `.planning/quick/001-recently-viewed-list-on-top/001-SUMMARY.md`
</output>
