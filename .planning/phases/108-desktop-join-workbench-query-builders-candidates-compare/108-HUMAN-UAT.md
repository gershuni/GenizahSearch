---
status: partial
phase: 108-desktop-join-workbench-query-builders-candidates-compare
source: [108-VERIFICATION.md]
started: 2026-06-05T12:59:01Z
updated: 2026-06-05T12:59:01Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. Grid/table candidate render
expected: Open the Workbench, anchor a fragment, run a multi-box OR builder query. Grid shows 20 candidates per page (4 columns); each card has a thumbnail image loading, a dimension/material line from FJMS batch enrichment, a 72px snippet browser with highlighted terms, and Y/?/N triage buttons that change the card border color. Toggle grid↔table and confirm counts match.
result: [pending]

### 2. CompareDialog matched-page + cross-side label
expected: With a candidate matching via the other-side builder (AND or OR), click Compare. CompareDialog opens modeless 1320x870; left pane shows anchor image+text; right pane shows candidate image for the cross-side neighbor page; meta line contains "other side matched" text; anchor pane stays static when stepping prev/next.
result: [pending]

### 3. Four actions + Add-as-Join Fragment B pre-fill
expected: Trigger each of the four actions (Browse / Puzzle / Add to List / Add as Join) from a grid card and from inside CompareDialog. All four delegate to the workbench host without any _vs_* private calls; Add as Join opens JoinsDialog with Fragment A = anchor and Fragment B = candidate shelfmark pre-filled.
result: [pending]

### 4. Self-match readout + include-anchor toggle
expected: Build a query the anchor itself satisfies; the self-match readout shows "⚓ anchor matches this query ✓" inline in the status bar. Toggle "Include anchor itself" (default OFF) and confirm the anchor appears in / disappears from the candidate list on the next search run.
result: [pending]

### 5. Global ja/flex/bidir toggles reach the engine (RR-14)
expected: Toggle Judeo-Arabic, Flex Spacing, or Bidirectional in the builder and re-run search; results differ from the plain search because _merge_globals() merges them back into the composed ro after compose() hardcodes them False.
result: [pending]

## Summary

total: 5
passed: 0
issues: 0
pending: 5
skipped: 0
blocked: 0

## Gaps
