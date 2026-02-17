---
status: resolved
phase: 36-pgp-service-layer
source: 36-01-SUMMARY.md, 36-02-SUMMARY.md
started: 2026-02-17T06:00:00Z
updated: 2026-02-17T06:45:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Web App - PGP Document Metadata
expected: Browse to a PGP document in the web app. Metadata (type, tags, date, description) should display correctly, identical to before the migration.
result: issue
reported: "990053173470205171 I see transcription but not PGP link to tags. Also 990053655710205171 for example - there I see no PGP info, though in search results there was PGP tag"
severity: major

### 2. Web App - Transcriptions and Editions
expected: On a PGP document that has transcription sources, the version selector should list all available editions and translations. Selecting one should display the transcription text with correct section parsing.
result: pass

### 3. Web App - Footnotes
expected: On a PGP document with footnotes, footnotes should appear in the footnotes section with correct content and attribution.
result: pass

### 4. Web App - Fragment Navigation
expected: On a PGP document with multiple fragments, the fragment links should appear and clicking one should navigate to the corresponding library record.
result: pass

### 5. PGP Tag Search
expected: Search using a PGP tag filter (e.g., "legal" or "letter" tag). Results should return PGP documents matching that tag, same as before (now using SQLite json_each instead of Supabase GIN).
result: pass

### 6. Search Results - Transcription Indicators
expected: Run a general search that returns PGP documents. Results that have transcription sources should show the transcription indicator badge, confirming the batch sys_id lookup still works.
result: pass

### 7. Test Suite Green
expected: Running `pytest tests/test_document_service.py tests/test_shared_service.py` should pass all 33 PgpService tests and updated import smoke tests with zero failures.
result: pass

## Summary

total: 7
passed: 6
issues: 1
pending: 0
skipped: 0

## Gaps

- truth: "PGP metadata (tags, type, description) should display on browse page for PGP-linked documents"
  status: resolved
  reason: "User reported: 990053173470205171 I see transcription but not PGP link to tags. Also 990053655710205171 for example - there I see no PGP info, though in search results there was PGP tag"
  severity: major
  test: 1
  root_cause: "FL ID initialization path in browse.py (lines 4139-4160) never sets state.pgp_metadata. The load_page() path (lines 922-937) correctly sets it, but when navigating via URL with fl_id (from search results), the FL ID branch executes instead and skips pgp_metadata assignment."
  artifacts:
    - path: "web/pages/browse.py"
      issue: "FL ID init path (lines 4139-4160) missing state.pgp_metadata assignment"
  missing:
    - "Add state.pgp_metadata = {...} block into FL ID init path after line 4140 (if pgp_doc:), mirroring lines 924-937"
  debug_session: ".planning/debug/pgp-metadata-browse-gap.md"
