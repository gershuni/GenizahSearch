---
status: partial
phase: 117-vertical-spine
source: [117-VERIFICATION.md]
started: 2026-06-17T19:35:00Z
updated: 2026-06-17T19:35:00Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. Anchor image renders with zoom/pan + folio navigation (ANC-01)
expected: Open `/joins-lab?sys_id=<known id>` in a browser without logging in. The fragment image is visible with working zoom/pan controls and functional previous/next folio buttons; images load via the per-provider proxy (never a direct IIIF URL).
result: [pending]

### 2. RTL numbered transcription alongside the image (ANC-03)
expected: On the same loaded anchor, the transcription is displayed as right-aligned (RTL) numbered lines next to the image.
result: [pending]

### 3. End-to-end search → deduped candidate grid (BLD-05, CND-01, CND-02)
expected: On a loaded anchor, type 2–3 Hebrew manuscript lines (one per line) into the Search-lines textarea and click Run Search. A deduped one-per-image candidate grid renders below the builder within a few seconds, each card showing thumbnail + shelfmark + library chip + title. Rapidly clicking Run Search twice shows only the latest result (latest-wins).
result: [pending]

### 4. Two anonymous sessions — no cross-session anchor bleed (FND-06 / SC#5)
expected: Open `/joins-lab` in two separate private/incognito windows and load a different anchor in each. Each window independently keeps its own anchor; session A's anchor never appears in session B (and vice versa). Bonus: on a narrow (<640px) viewport the layout stacks to a single column (D-03).
result: [pending]

## Summary

total: 4
passed: 0
issues: 0
pending: 4
skipped: 0
blocked: 0

## Gaps
