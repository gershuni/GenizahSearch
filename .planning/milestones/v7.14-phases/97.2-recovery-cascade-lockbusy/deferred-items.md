# Phase 97.2 — Deferred Items

Items discovered during execution that are out of scope per Rule SCOPE BOUNDARY (pre-existing, not directly caused by current task changes).

## Pre-existing ruff F401 in tests/test_phase_97_2_schema_marker_absence.py

- Lines 14-15: `_compute_schema_marker`, `_read_schema_marker` imported but unused
- Origin commit: `a93322b2` (RED test for R97.2-F)
- The imports appear unused because the test only asserts via `os.path.isfile` and `indexer._writer is not None`; the imported names provide documentation of what the rebuild path internally relies on
- Not blocking: ruff check passes for production code paths (shared/local_indexer.py, genizah_core.py) per the Task 11 acceptance criterion scope
- Recommended cleanup: remove the unused imports OR add a `# noqa: F401` comment OR reference them in a sanity-check assertion within the test

Deferred to a future cleanup pass (e.g., Phase 97.2-02 or general test maintenance).

## Pre-existing failures in tests/test_local_indexer.py

- `test_supported_file_types_docx_pdf_txt` — asserts HTML status is 'unsupported', actual is 'ok'
- `test_unsupported_extension_status` — asserts HTML status is 'unsupported', actual is 'ok'

Both failures verified pre-existing via `git stash` + re-run before any Phase 97.2-01 commits.
Origin: Phase 97-03 (Wave C, commit `bc9e1fae`) added HTML support to the indexer (`extract_html_pages`),
which makes HTML files now legitimately index as 'ok' rather than 'unsupported'. The two test
assertions were not updated to match.

Not in scope for Phase 97.2-01 per SCOPE BOUNDARY rule. Recommended cleanup: update the tests
to use a truly unsupported extension (e.g., `.bin` or `.xyz`) or to assert HTML now indexes
as 'ok'.
