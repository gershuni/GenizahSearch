---
status: partial
phase: 107-desktop-join-workbench-anchor-entry-points-actions-join-model
source: [107-VERIFICATION.md]
started: 2026-06-04T10:00:00Z
updated: 2026-06-04T10:00:00Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. ResultDialog → Find joins opens the Workbench
expected: Open a result from the desktop ResultDialog, click "Find joins". JoinWorkbenchWindow opens as a modeless window anchored on the folio you were viewing (showing the anchor's shelfmark, image, and numbered transcription), and the ResultDialog closes.
result: [pending]

### 2. Browse → Find joins opens the Workbench, Browse stays open
expected: Open the Browse tab, load a manuscript, click "Find joins". JoinWorkbenchWindow opens with anchor from current_browse_sid/current_browse_p; the Browse tab is still visible.
result: [pending]

### 3. Single-instance re-anchoring
expected: Click "Find joins" twice on two different fragments. Only ONE Workbench window exists; the second call re-anchors/raises it rather than opening a second window (D-01/D-02).
result: [pending]

### 4. Four-source Known Joins panel
expected: With a fragment that has known joins (user + PGP or FJMS), the Known Joins panel appears with correct per-member rows and source badges (PGP=blue, FJMS=purple, user=green, community=green); the panel is hidden when the fragment has no known joins (setVisible(count>0)).
result: [pending]

### 5. Add as Join → persist → panel refreshes
expected: Click "Add as Join" in the Workbench action row. JoinsDialog opens with Fragment A pre-filled with the anchor and Fragment B empty. Create a join; after closing, _reload_known_joins fires and the new join appears in the Known Joins panel (SC#4).
result: [pending]

### 6. Dark mode + Hebrew bilingual rendering
expected: With a dark-mode desktop theme active, the anchor image area has a dark loading background (#374151), the ANCHOR tag is teal (#14b8a6), and Hebrew strings display correctly under lang=he. No hardcoded English text visible.
result: [pending]

### 7. Zoom + folio nav without side effects
expected: Zoom in/out on the anchor image rescales without re-fetching from network; folio prev/next pages the SAME fragment without reloading the Known Joins panel (D-07).
result: [pending]

## Summary

total: 7
passed: 0
issues: 0
pending: 7
skipped: 0
blocked: 0

## Gaps
