---
status: complete
phase: 52-community-integration
source: 52-01-SUMMARY.md, 52-02-SUMMARY.md, 52-03-SUMMARY.md
started: 2026-03-17T14:00:00Z
updated: 2026-03-17T15:30:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Web Publish Toggle
expected: In the web puzzle page, create or load a puzzle with 2+ fragments. Log in. Click the Publish button. A confirm dialog appears. Click Publish. Button turns green, share dialog with copyable link appears. Click green button again — unpublishes.
result: pass

### 2. Web Deep Link Loading
expected: Copy the shareable link from the publish dialog. Open new tab and navigate to that URL. The puzzle page loads and opens the saved document with all fragments in correct positions.
result: pass

### 3. Web Discoveries Feed — Puzzle Join
expected: After publishing, navigate to Discoveries Center. Published puzzle join appears in feed with thumbnail, title, shelfmarks, display name. Published Puzzles stat card appears at top.
result: pass

### 4. Web Discoveries Detail + Fork
expected: Click View Details on published puzzle join. Detail dialog shows full-res download, notes, fragments, Open in Puzzle button. Click Open in Puzzle — forks and navigates to /puzzle?doc={new_id}.
result: pass

### 5. Web Joins Panel — Community Section
expected: Browse to a manuscript in a published join. Open Joins panel. Community Puzzle Joins section shows published joins for current fragment.
result: pass

### 6. Desktop Publish Toggle
expected: Open Fragment Puzzle, load/create puzzle. Log in. Click Publish (globe icon). Confirm dialog, progress dialog (non-frozen UI), success, button turns green. Click again to unpublish.
result: pass

### 7. Desktop Discoveries Feed — Puzzle Joins
expected: Open Discoveries Center. Published puzzle joins appear with cyan badge. Type filter to Puzzle Joins — only puzzles. To Discoveries — no puzzles. Back to All — reappear.
result: pass

### 8. Desktop JoinsDialog — Community Section
expected: Browse to manuscript in published join. Click Joins. Community Puzzle Joins section appears. Close and re-open — no duplication.
result: pass

### 9. Desktop JoinsFeedDialog — All Puzzles / My Puzzles Tabs
expected: Four tabs in both JoinsFeedDialog and main community panel: All Joins, My Joins, All Puzzles, My Puzzles. All Puzzles shows table. My Puzzles shows own puzzles.
result: pass

### 10. Desktop Fork from Puzzle Tab
expected: Double-click a published puzzle in All Puzzles tab. Fragment Puzzle window opens with forked document loaded.
result: pass

## Summary

total: 10
passed: 10
issues: 0
pending: 0
skipped: 0

## Gaps

[none]
