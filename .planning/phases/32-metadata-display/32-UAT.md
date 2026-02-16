---
status: diagnosed
phase: 32-metadata-display
source: [32-01-SUMMARY.md, 32-02-SUMMARY.md]
started: 2026-02-16T01:30:00Z
updated: 2026-02-16T02:00:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Web browse — material type display
expected: Navigate to a CUL T-S manuscript on the web browse page. The metadata panel shows a "Material" field with a value like "Paper", "Parchment", or "Vellum".
result: pass

### 2. Web browse — folio count display
expected: On the same manuscript, the metadata panel shows a "Folios" field with a count (e.g., "2 Folios" or "1 Folios + 1 Bifolios"). Only appears when data is non-zero.
result: pass

### 3. Web browse — library link in metadata panel
expected: In the external links section of the metadata panel, a library-specific link appears (e.g., "Cambridge University Library" linking to CUDL). This link should NOT duplicate the existing Cambridge CUDL link if one already appears from the Oxford/Cambridge path.
result: issue
reported: "Manchester Ms. B 3243 sys id 990002093540205171 links to https://luna.manchester.ac.uk/luna/servlet/s/B%203243 which gets: Unexpected Error - java.lang.NullPointerException. Will have to check EVERY library to see that the links are ok"
severity: major

### 4. Web browse — library link in compact header
expected: In the compact header bar (alongside KTIV and PGP buttons), a library link button with an external-link icon appears for manuscripts with library URL data. Clicking it opens the library's digital collection in a new tab. Should NOT appear for Oxford/Cambridge manuscripts that already have their own links.
result: pass

### 5. Web browse — graceful degradation
expected: Navigate to a manuscript without NLI crossref data (e.g., a CUL T-S manuscript). No empty "Material" or "Folios" fields appear. The metadata panel looks normal with no errors or blank sections.
result: pass

### 6. Desktop browse — material type and folio count
expected: Open the desktop app, browse to a manuscript with NLI crossref data (e.g., BL Or. 10112A.1 or AIU I.A.146). The extended info panel (below the image) shows "Physical Description" with material type (Paper/Parchment/Vellum) and folio count.
result: pass

### 7. Desktop browse — library link
expected: In the desktop browse extended info panel, a clickable library link appears (e.g., "British Library" for BL manuscripts). Clicking it opens the URL in the default browser.
result: pass

### 8. Desktop browse — graceful degradation
expected: Browse to a manuscript without NLI crossref material data in the desktop app (e.g., a CUL T-S shelfmark). No empty "Physical Description" section appears. The extended info panel looks normal — no errors, no blank sections.
result: pass

### 9. KTIV link still works
expected: On the web browse page, the KTIV link button in the header still opens the NLI KTIV viewer. On the desktop app, the KTIV button in ManuscriptViewerWidget still opens the KTIV viewer. No regressions.
result: pass

## Summary

total: 9
passed: 8
issues: 1
pending: 0
skipped: 0

## Gaps

- truth: "Library digital collection links work correctly for all supported libraries (CUL, JTS, Manchester, BL)"
  status: failed
  reason: "User reported: Manchester LUNA search URL pattern broken. Investigation found 3 of 4 library URL patterns are broken."
  severity: major
  test: 3
  root_cause: "Wrong URL patterns in get_library_viewer_url(). Manchester uses servlet/s/ but should use servlet/view/search?q=. JTS/DPUL search by shelfmark returns no results (uses ark identifiers). BL shelfmark needs spaces→underscores transform and leaf number stripping."
  artifacts:
    - path: "shared/nli_crossref_service.py"
      issue: "Manchester URL uses servlet/s/ instead of servlet/view/search?q=. JTS search_field=all_fields doesn't find by shelfmark. BL ref= gets raw shelfmark with spaces instead of underscore-formatted ref."
  missing:
    - "Manchester: change to luna/servlet/view/search?q={shelfmark}&search=Go&QuickSearchA=QuickSearchA"
    - "JTS: DPUL uses ark identifiers, search by shelfmark broken — may need to drop JTS support or find alternative URL"
    - "BL: transform shelfmark (spaces→underscores, strip leaf/page suffixes like .1) e.g., 'OR 10112A.1' → 'Or_10112A'"
  debug_session: ""
