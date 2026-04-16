---
title: "Reading Desk UX fixes: pre-populate add-to-view field, compact green bar"
created: 2026-04-16
area: desktop
priority: low
source: Phase 69 smoke test (user feedback)
---

# Reading Desk UX Fixes

Two UX issues reported during Phase 69 image viewer extraction smoke test:

## 1. "Add to view" field not pre-populated

When clicking "Add to view" in the reading desk, the shelfmark/sys_id field should pre-populate with the current manuscript's shelfmark or sys_id. Currently appears empty.

**Location:** `genizah_app.py`, reading desk area (~line 12796+, `_browse_rd_*` methods)

## 2. Green status bar too tall

The green status/info bar in the reading desk view takes too much vertical space. Reduce height/padding to be more compact.

**Location:** `genizah_app.py`, reading desk layout setup (green QFrame/QLabel styling)

## Notes

- Both are cosmetic/UX polish, not functional bugs
- Also logged in `docs/OPEN_ISSUES.md`
