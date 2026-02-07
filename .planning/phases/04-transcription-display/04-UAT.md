---
status: complete
phase: 04-transcription-display
source: [04-01-SUMMARY.md, 04-02-SUMMARY.md, 04-03-SUMMARY.md]
started: 2026-02-05T21:45:00Z
updated: 2026-02-05T22:15:00Z
---

## Current Test

Testing recto/verso splitting fix (04-03) with edge cases

## Tests

### 1. Simple Recto/Verso Split
expected: Page 1 shows only Recto content, Page 2 shows only Verso content
test_fragment: T-S 8J22.24 (or similar with simple Recto/Verso markers)
result: pass

### 2. Multi-Section Document (Margins)
expected: "Recto" and "Recto - right margin" both show on page 1; "Verso" sections show on page 2
test_fragment: T-S 8J22.21 (has Recto, Recto - right margin, Verso, Verso - address)
result: issue
reported: "Joined document (T-S 8J22.21 + T-S NS J193) shows recto/verso sections from BOTH fragments without indicating which fragment each section belongs to. User sees content from both manuscripts mixed together."
severity: minor
note: "Recto/verso splitting works correctly, but joined documents need fragment labels to distinguish which text belongs to which manuscript. Deferred to Phase 7 (Joins UI) since it requires document_fragments sequence info."

### 3. Document Without Section Markers
expected: Full transcription shows on both pages (graceful fallback)
test_fragment: Find a PGP document without Recto/Verso markers
result: pass

### 3a. Recto/Verso Headers Should Be Preserved
expected: Section headers (Recto, Verso, etc.) should remain visible in the displayed text
result: issue
reported: "Since joined documents don't yet show fragment labels, the Recto/Verso section headers should remain visible to help orient users. Currently they are stripped during parsing."
severity: minor
fix: "Modify parse_transcription_sections to include the header text in output, or display headers as section labels"

### 3b. Translations Mixed with Transcriptions
expected: Only Hebrew/Aramaic transcriptions shown as "PGP Transcription"
test_fragment: T-S 8J22.1 (has "Trans. Gil" - English translation)
result: issue
reported: "PGP data includes English translations (marked as 'Trans. [scholar]') alongside original transcriptions. Translations are valuable but should be offered separately in version selector, not mixed with transcriptions."
severity: major
fix: "Filter out entries where attribution contains 'Trans.' or similar translation markers. Consider adding translations as separate version option in future."

### 4. PGP Option in Version Selector (regression check)
expected: Version history menu shows "PGP Transcription" as first option with green verified icon
result: pass

### 5. Attribution and PGP Link (regression check)
expected: Attribution displays, "View on PGP" link works
result: pass

### 6. Version Switching (regression check)
expected: Can switch between PGP and V0.8 versions
result: pass

## Summary

total: 8
passed: 5
issues: 3
pending: 0
skipped: 0

## Gaps

### Gap 1: Joined Documents Need Fragment Labels (Minor - Deferred to Phase 7)
- **Truth:** "Multi-fragment transcriptions display which fragment each section belongs to"
- **Status:** deferred
- **Reason:** Joined documents show recto/verso sections from all fragments without indicating which manuscript each belongs to
- **Resolution:** Phase 7 (Joins UI) will add fragment relationship display

### Gap 2: Recto/Verso Headers Stripped (Minor - Quick Fix)
- **Truth:** "Section headers remain visible to help orient users"
- **Status:** open
- **Reason:** parse_transcription_sections strips markers during parsing
- **Fix:** Modify parsing to preserve or display section headers

### Gap 3: Translations Mixed with Transcriptions (Major - Phase 4.1)
- **Truth:** "PGP Transcription shows only original text transcriptions, not English translations"
- **Status:** open
- **Reason:** 1,696 Digital Translations imported alongside 7,664 Digital Editions. doc_relation field distinguishes them.
- **Fix:** Filter by doc_relation='Digital Edition' in document service, or offer translations as separate version option
- **Data:** ~18% of imported content is translations (marked as "Digital Translation" in doc_relation column)
