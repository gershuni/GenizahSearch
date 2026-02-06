---
phase: quick-001
plan: 01
subsystem: web-ui
tags: [lists, sidebar, ux]

dependency-graph:
  requires: []
  provides: [recently-viewed-top-position]
  affects: []

tech-stack:
  added: []
  patterns: []

key-files:
  created: []
  modified:
    - web/components/project_tree.py

decisions:
  - id: Q001-01
    choice: "Render Recently Viewed in separate 'Recent' header section"
    rationale: "Clear visual separation, consistent with existing section headers"
    alternatives: ["No header, just render first", "Inline with projects"]

metrics:
  duration: 3 min
  completed: 2026-02-06
---

# Quick Task 001: Move Recently Viewed to Top of Sidebar

**One-liner:** Recently Viewed list now renders at the very top of the lists sidebar under a "Recent" header for quick access.

## What Changed

Modified `create_project_tree()` in `web/components/project_tree.py` to:

1. Extract "Recently Viewed" from system lists at the start of rendering
2. Render it first under a "Recent" header section (before projects)
3. Exclude it from the bottom "System" section to avoid duplication

### New Rendering Order

1. **Recent** - Recently Viewed list (new)
2. **Projects** - User projects with their lists (unchanged)
3. **Standalone Lists** - Lists not in any project (unchanged)
4. **System** - Other system lists (excludes Recently Viewed now)

## Code Changes

```python
# Lines 113-132: New Recently Viewed section at top
standalone_lists = lists_by_project.get(None, [])
system_lists = [l for l in standalone_lists if l.get('is_system')]
recently_viewed = [l for l in system_lists if l.get('name') == 'Recently Viewed']

if recently_viewed:
    with ui.element('div').classes('mb-2'):
        ui.label(tr('Recent')).classes(...)
        for list_data in recently_viewed:
            _render_list_item(...)

# Lines 153-154: Exclude from bottom System section
other_system_lists = [l for l in system_lists if l.get('name') != 'Recently Viewed']
```

## Commits

| Hash | Type | Description |
|------|------|-------------|
| 28b778e | feat | Move Recently Viewed list to top of sidebar |

## Verification

- [x] Module imports without errors
- [x] Syntax validation passed
- [x] Recently Viewed appears at top under "Recent" header
- [x] Projects render below Recently Viewed
- [x] No duplicate Recently Viewed entries

## Deviations from Plan

None - plan executed exactly as written.
