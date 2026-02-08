---
phase: 11-virtual-reading-desk
status: gaps_found
score: 5/8
verified_by: human + orchestrator
date: 2026-02-08
---

# Phase 11 Verification: Virtual Reading Desk

## Phase Goal
Users can view multiple manuscripts together in a reading desk, populated from joins, personal lists, or manual entry, in both web and desktop apps.

## Must-Haves Assessment

| # | Must-Have | Status | Evidence |
|---|-----------|--------|----------|
| 1 | User can open a joined document and see all fragments with images and transcriptions | PARTIAL | Web: works but visual bugs (button visibility, badge). Desktop: scroll sync broken |
| 2 | User can add any manuscript by typing a shelfmark or sys_id | PASS | Both apps support shelfmark add (desktop UX needs polish) |
| 3 | User can populate reading desk from a personal list | PARTIAL | Web: dialog shows list names but not contents. Desktop: works via list panel |
| 4 | Reading desk works in both web and desktop with equivalent functionality | PARTIAL | Both apps have the feature but with multiple bugs affecting usability |

## Gaps Found

### Gap 1: Web Visual/UX Bugs (W1-W3, W5)
**Affects must-haves:** 1, 3
- "Back to Page View" button invisible in Light Mode (color contrast)
- Fragment count badge invisible in Dark Mode (color contrast)
- Add from List dialog doesn't show manuscripts inside lists
- Reading desk text pane missing word wrap

### Gap 2: Web State Persistence Bug (W4)
**Affects must-haves:** 4
- Language switch loses reading desk state (goes back to no manuscript)
- State persistence implementation not working as intended

### Gap 3: Desktop Scroll Sync Bug (D1)
**Affects must-haves:** 1, 4
- Scrolling text pane only moves images pane, not text itself
- Proportional sync mapping may be incorrect or one-directional

### Gap 4: Desktop UX Polish (D2, D4)
**Affects must-haves:** 2, 4
- "Add" button in toolbar confusing — user clicked "Add to List" instead
- "Add to View" button should be positioned right after Go button for discoverability

### Out of Scope (Noted)
- D3: PGP joins not visible in desktop — belongs to Phase 12 (Desktop PGP Discovery, plan 12-03)

## Recommendation

Create gap closure plans to fix the 8 in-scope issues (W1-W5, D1-D2, D4). The PGP joins item (D3) remains in Phase 12 scope.

Suggested plan grouping:
- **11-06**: Web reading desk fixes (W1-W5) — visual bugs, state persistence, word wrap, list dialog
- **11-07**: Desktop reading desk fixes (D1, D2, D4) — scroll sync, UX polish, button positioning
