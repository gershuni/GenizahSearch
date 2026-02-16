---
status: resolved
phase: 33-metadata-enrichment
source: 33-03-SUMMARY.md, 33-04-SUMMARY.md
started: 2026-02-16T08:00:00Z
updated: 2026-02-16T20:00:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Bibliography section on CUL manuscript (web)
expected: Browse to T-S 12.863. Bibliography References section shows ~120 entries with FJMS badge, author/title/year formatting, mention type badges, and collapsible expansion for entries beyond the first 5.
result: pass
notes: "UX feedback: Data needs better organization — group page ranges under same publication, add filtering by mention/partial/discussion/translation, consider a dialog for this information"

### 2. Catalog cross-references on CUL manuscript (web)
expected: Same T-S 12.863 browse page shows "Catalog References" section with FJMS badge. Entry displays as "Davis I #3516".
result: pass

### 3. Neubauer-Cowley catalog entry on Oxford manuscript (web)
expected: Browse to Oxford MS heb. a.1/1 (sys_id 990053385780205171). Below the shelfmark, tertiary text shows "2613.1" (the Neubauer-Cowley catalog number).
result: pass
notes: "UX feedback: Should be labeled 'Neubauer-Cowley' in metadata. In title where [part 1] is shown, display as [2613] [part 1] with tooltip 'Neubauer-Cowley Catalog number'"

### 4. IsNotGenizah badge (web)
expected: Browse to Allony Ms. 113 (sys_id 990000465700205171) or Austrian H 1 (sys_id 990001696070205171). An orange "Not Genizah" badge appears near the shelfmark.
result: pass

### 5. Collection & Storage section (web)
expected: Browse to Austrian H 1 (sys_id 990001696070205171). A "Collection & Storage" section shows "Vienna — Box H, Vol. 1". On T-S 12.863, shows "T-S 8 - 32, Vol. 12, Fol. 863".
result: pass

### 6. Scholarly Source Names (web)
expected: Browse to T-S 13 J 35.3 (sys_id 990051250670205171). A "Scholarly Sources" section displays labels like "Documentary Material (Goitein)" and "Books".
result: pass

### 7. Desktop bibliography section
expected: Open desktop app, browse to T-S 12.863. Extended info panel shows an orange-bordered "Bibliography References" section with author/title/page formatting and mention type badges. Limited to 20 entries with overflow count.
result: pass
notes: "UX feedback: Bibliography needs own dialog with sort/filter, accessed via button not inline. Critical: handle NLI vs FGP bibliography deduplication"

### 8. Desktop catalog cross-references
expected: Same T-S 12.863 in desktop. Extended info shows teal-bordered "Catalog References" section with "Davis I #3516".
result: pass

### 9. Desktop IsNotGenizah badge + Neubauer-Cowley
expected: Desktop browse to Oxford MS heb. a.1/1 — info label shows "2613.1" Neubauer-Cowley number. Browse to Allony Ms. 113 — orange "Not Genizah" badge appears near shelfmark.
result: issue
reported: "Oxford - don't see. Don't see any badge."
severity: major
retest-1: issue
retest-1-reported: "990053385780205171 - No label, also no bib buttons and no image. Allony Ms. 113 - No badge."
retest-1-severity: major

### 10. Desktop secondary metadata (sources + storage)
expected: Desktop browse to Austrian H 1. Extended info shows grey-bordered section with "Scholarly Sources: Documentary Material (Goitein)" and "Collection & Storage: Vienna — Box H, Vol. 1".
result: pass

### 11. Graceful degradation — no metadata
expected: Browse to any manuscript WITHOUT FIST/NLI enrichment (e.g., Mosseri P 723 II). No bibliography, catalog refs, IsNotGenizah badge, or collection/storage sections appear. No errors.
result: pass

## Summary

total: 11
passed: 10
issues: 1
pending: 0
skipped: 0

## Gaps

- truth: "Desktop browse page shows IsNotGenizah badge near shelfmark for flagged manuscripts and Neubauer-Cowley catalog number for Oxford manuscripts"
  status: resolved
  reason: "Retest: Oxford 990053385780205171 - no label, no bib buttons, no image. Allony Ms. 113 - no badge. Previous fix (33-05) did not resolve."
  severity: major
  test: 9
  root_cause: "browse_render_page() at line ~19604 overwrites the enriched info label set by on_browse_enriched_loaded(). browse_render_page() builds info_text from scratch WITHOUT catalog_entry or IsNotGenizah badge. The enrichment callback correctly sets the label at line 9137, then calls browse_load_page() at line 9240, which triggers browse_render_page() to overwrite it."
  artifacts:
    - path: "genizah_app.py"
      issue: "Line ~19604: browse_render_page() sets info label without catalog_entry or is_not_genizah badge"
    - path: "genizah_app.py"
      issue: "Line ~9240: on_browse_enriched_loaded() calls browse_load_page() which triggers the destructive overwrite"
  missing:
    - "Add catalog_entry lookup from nli_cache to browse_render_page() info_text construction"
    - "Add is_not_genizah badge lookup from nli_cache to browse_render_page() info_text construction"
    - "Use setHtml() instead of setText() in browse_render_page() for info label (to render HTML badge)"
  debug_session: ".planning/debug/desktop-isnotgenizah-badge.md, .planning/debug/oxford-part-desktop-missing-data.md"
  data_notes:
    - "No bibliography for Oxford MS heb. a.1/1: FJMS has no entries (data absence, not code bug)"
    - "No image for Oxford MS heb. a.1/1: oxford_full_db.json has empty images array for Part 1 (data absence)"

## UX Enhancement Notes (from passing tests)

These are not gaps but improvement suggestions for a future iteration:

1. **Bibliography dialog**: Move bibliography from inline section to dedicated dialog with sort/filter by mention type, group page ranges under same publication (tests 1, 7)
2. **NLI vs FGP deduplication**: Handle overlap between NLI bibliography and FIST/FGP bibliography data (test 7)
3. **Neubauer-Cowley labeling**: Label as "Neubauer-Cowley" in metadata; show as [2613] [part 1] in title with tooltip (test 3)
