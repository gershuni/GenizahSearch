---
status: partial
phase: 119-candidates-compare-visual-similarity
source: [119-VERIFICATION.md]
started: 2026-06-19T00:00:00Z
updated: 2026-06-19T00:00:00Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. Candidate grid visual layout
expected: Open `/joins-lab`, load an anchor (sys_id), run a search with 1+ result lines. Grid renders up to 24 cards per page; each card shows a ≈160×160 image-first thumbnail, library chip, shelfmark, title, 👁 badge (if via_vs), Yes/Maybe/No triage buttons, a "View in Browse" link, and a "Compare fragment" button; Prev/Next pagination appears when more than 24 candidates.
result: [pending]

### 2. Grid↔table triage consistency + bulk-triage bar
expected: Toggle Grid↔Table. Table shows 8 columns (Shelfmark, Score, Snippet, Material, Dimensions, Page, Triage, select); is sortable; multi-select works; selecting rows reveals a "Mark N selected as: Yes/Maybe/No" bulk bar; verdicts set in grid show the same in table (and vice-versa).
result: [pending]

### 3. Filter dialog enrichment gate + apply behavior
expected: Open Filters. Material multi-select starts disabled with a "Loading…" note; after enrichment completes it populates with available materials; applying a material filter re-renders with fewer candidates and resets to page 1; size-mismatch exclusion removes mismatched candidates.
result: [pending]

### 4. Compare modal per-pane independence + card restyle after verdict
expected: Click "Compare fragment". Full-screen modal opens with anchor image left, candidate image right (two independent AnchorViewers); the candidate pane navigates folios without moving the anchor pane; recording "Yes" gives the grid card a green border and auto-advances to the next candidate.
result: [pending]

### 5. VS toggle: intersection mode + empty-builder union mode
expected: Toggle the 👁 Visual Similarity switch ON with a query active — displayed candidates narrow to the text∩VS intersection (fewer, each 👁-badged) and the count notice updates. Then clear the builder with VS ON — the pure VS union renders look-alikes with NO "Enter at least one search line" toast (F1 empty-builder branch).
result: [pending]

### 6. Re-anchor invalidation + VS refetch
expected: Load a fresh anchor after triaging some candidates — all triage verdicts clear (no verdict borders on any card); if VS was ON, look-alikes refetch for the new anchor and the loading notice appears briefly.
result: [pending]

## Summary

total: 6
passed: 0
issues: 0
pending: 6
skipped: 0
blocked: 0

## Gaps
