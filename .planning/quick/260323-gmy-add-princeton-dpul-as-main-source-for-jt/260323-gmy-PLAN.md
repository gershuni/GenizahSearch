---
quick_id: 260323-gmy
description: "Add Princeton DPUL as main source for JTS images"
tasks: 1
---

# Quick Task 260323-gmy: Add Princeton DPUL as Main Source for JTS Images

## Context

Currently, JTS manuscripts default to NLI images with Princeton DPUL as a secondary toggle option.
The desktop app already defaults to external (DPUL) images when available. The web app should match.

**Key insight**: The desktop already does this correctly (genizah_app.py:2228 — `self.active_list = self.images_ext`).
The web app needs to auto-default to JTS/Princeton when DPUL images are available.

## Plan 260323-gmy-1: Auto-default to JTS source for JTS manuscripts

### Task 1: Add source_user_override flag and auto-default logic

**files**: web/pages/browse.py
**action**:
1. Add `state.source_user_override = False` to BrowseState init (near line 523)
2. At all 4 manuscript-change reset points (lines ~599, 626, 667, 1130), also reset `state.source_user_override = False`
3. In the 4 source-switch functions (switch_to_nli/cambridge/manchester/jts, lines ~3701-3715), set `state.source_user_override = True`
4. In the render logic (around line 3696), after computing `_has_jts_images`, add auto-default: if `not state.source_user_override` and `_has_jts_images` and `state.active_source == 'nli'`, auto-switch to `'jts'`

**verify**: Browse a JTS manuscript — DPUL images should show by default. Toggle to NLI — stays on NLI. Navigate to new JTS manuscript — resets to DPUL.
**done**: JTS manuscripts auto-default to Princeton DPUL images in web browse.
