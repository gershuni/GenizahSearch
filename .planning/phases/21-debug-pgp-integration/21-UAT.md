---
status: diagnosed
phase: 21-debug-pgp-integration
source: 21-01-SUMMARY.md, 21-02-SUMMARY.md, 21-03-SUMMARY.md
started: 2026-02-11T13:00:00Z
updated: 2026-02-11T13:10:00Z
---

## Current Test

[testing complete]

## Tests

### 1. PGP document shows structured transcription sections (web)
expected: Open the web app, navigate to a PGP document with transcriptions (e.g., PGPID 444, 445, or 446). The transcription panel should display section text corresponding to the current manuscript page (recto/verso), not the full unstructured text dump.
result: pass

### 2. Section changes when switching manuscript pages
expected: On a PGP document with recto+verso transcription sections, switch between recto and verso pages. The displayed transcription section should change to match the selected page — recto text for recto page, verso text for verso page.
result: pass

### 3. Document without structured sections uses regex fallback
expected: Navigate to a PGP document that does NOT have pgp-text HTML imported (a PGPID outside the 6,894 imported set, or one with only plain-text sources). The transcription should still display with section parsing via the regex fallback — not broken or empty.
result: pass

### 4. Multiple sources show sections independently
expected: Find a PGP document with multiple transcription sources (edition + translation). Each source should display its own section text for the current page independently.
result: pass

### 5. Desktop app shows structured sections
expected: Open the desktop app and navigate to a PGP document with transcriptions. Structured sections should display correctly, matching the web app behavior — section text corresponds to the current manuscript page.
result: issue
reported: "In the desktop app, the Hebrew Translation shows the English one (checked where there were Eng and Heb)"
severity: major

## Summary

total: 5
passed: 4
issues: 1
pending: 0
skipped: 0

## Gaps

- truth: "Desktop app displays correct translation content per language — Hebrew translation shows Hebrew text, English translation shows English text"
  status: failed
  reason: "User reported: In the desktop app, the Hebrew Translation shows the English one (checked where there were Eng and Heb)"
  severity: major
  test: 5
  root_cause: "Desktop app displays translations in database sequence_order without language-based grouping. Web app explicitly groups Hebrew first, English second. When DB has English (seq 1) and Hebrew (seq 2), desktop shows English first under Hebrew label."
  artifacts:
    - path: "genizah_app.py:6070-6115"
      issue: "_populate_pgp_combo iterates translations in DB order, no language grouping"
    - path: "web/components/version_selector.py:256-264"
      issue: "Correct implementation — groups Hebrew before English"
  missing:
    - "Add language-based grouping to desktop _populate_pgp_combo matching web app behavior"
  debug_session: ".planning/debug/resolved/desktop-hebrew-translation-english-content.md"
