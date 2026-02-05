---
status: complete
phase: 04-transcription-display
source: [04-01-SUMMARY.md, 04-02-SUMMARY.md]
started: 2026-02-05T21:15:00Z
updated: 2026-02-05T21:35:00Z
---

## Current Test

[testing complete]

## Tests

### 1. PGP Option in Version Selector
expected: Version history menu shows "PGP Transcription" as first option with green verified icon
result: issue
reported: "PGP transcription displays but shows FULL text on both recto and verso images. The text contains 'Verso' marker indicating where to split. Image 1 should show text up to 'Verso', image 2 should show text after 'Verso' (excluding the word 'Verso' itself)."
severity: major

### 2. PGP Auto-Selection on Page Load
expected: When loading a fragment with PGP transcription, PGP is auto-selected (version label shows "PGP" in green, not "V0.8")
result: pass

### 3. Attribution Display
expected: PGP menu item shows scholar attribution (e.g., "Transcription by Marina Rustow") in smaller text below the label
result: pass

### 4. View on PGP Link
expected: External link icon (open_in_new) appears next to attribution. Clicking it opens PGP document page in new browser tab.
result: pass

### 5. Version Switching
expected: Can switch between PGP and V0.8 versions by clicking menu items. Notification shows source (e.g., "PGP Transcription - [scholar]")
result: pass

### 6. Fragments Without PGP
expected: On a fragment WITHOUT PGP transcription, version selector shows only "V0.8 (Original)" - no PGP option appears
result: pass

## Summary

total: 6
passed: 5
issues: 1
pending: 0
skipped: 0

## Gaps

- truth: "PGP transcription displays correctly per page (recto/verso split)"
  status: failed
  reason: "User reported: PGP transcription shows FULL text on both images. Text contains 'Verso' marker indicating split point. Image 1 should show text up to 'Verso', image 2 should show text after 'Verso' (excluding the word itself)."
  severity: major
  test: 1
  root_cause: ""
  artifacts: []
  missing: []
  debug_session: ""

- truth: "Multi-section transcriptions display sections matched to corresponding images"
  status: failed
  reason: "User reported: Multi-fragment documents (e.g., T-S 8J22.21) show ALL transcription sections (Recto, Recto - right margin, Verso, Verso - address) in one block instead of matching each section to its corresponding image. PGP shows each section next to its image - we should too."
  severity: major
  test: 1
  note: "Related to Test 1 - same underlying recto/verso splitting issue"
  root_cause: ""
  artifacts: []
  missing: []
  debug_session: ""
