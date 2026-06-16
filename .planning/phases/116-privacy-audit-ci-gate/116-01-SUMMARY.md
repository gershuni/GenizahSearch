---
phase: 116-privacy-audit-ci-gate
plan: "01"
subsystem: telemetry-privacy
tags: [telemetry, privacy, testing, posthog, scrubber]
dependency_graph:
  requires: [desktop/telemetry.py, shared/posthog_server.py, tests/test_telemetry_review_fixes.py]
  provides: [tests/test_telemetry_priv04.py, _safe_context-hardening]
  affects: [desktop/telemetry.py]
tech_stack:
  added: []
  patterns: [scrubber-unit-test, autouse-fixture-verbatim-copy, json-dumps-needle-assertion]
key_files:
  created: [tests/test_telemetry_priv04.py]
  modified: [desktop/telemetry.py]
decisions:
  - "_safe_context hardened with _CONTEXT_FILENAME_RE = re.compile(r'\\.[A-Za-z][A-Za-z0-9]{0,7}\\Z') to reject filename-extension-shaped contexts (e.g. 'manuscript_notes.docx') while preserving legitimate dotted codes like 'search_tab.run_query' whose final segment contains '_'"
  - "Test file uses verbatim autouse fixture from test_telemetry_review_fixes.py (PATTERNS.md mandate)"
  - "Hebrew context test asserts 'unregistered' (not '[REDACTED]') — REVIEWS HIGH #2 confirmed: context routes through _safe_context, not _scrub_value"
  - "PRIV-03 AST guard (test_telemetry_no_direct_posthog.py) verified green and NOT modified (D-09)"
metrics:
  duration: "3 minutes"
  completed: "2026-06-16"
  tasks_completed: 3
  files_modified: 2
---

# Phase 116 Plan 01: PRIV-04 Scrubber Tests + _safe_context Hardening Summary

One-liner: PRIV-04 forbidden-field/value scrubber-unit tests (8 tests + 1 pre-consent) proving no path/filename/query/hostname/username reaches `enqueue_event`, plus `_safe_context` hardened to reject filename-extension-shaped contexts.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 0 | Harden `_safe_context` (PRIV-04 allowed-key leak) | 90a4269c | desktop/telemetry.py |
| 1 | PRIV-04 forbidden-field/value scrubber-unit tests | 44b31a76 | tests/test_telemetry_priv04.py |
| 2 | Pre-consent zero-emit test + PRIV-03 guard verified | 44b31a76 | tests/test_telemetry_priv04.py |

## What Was Built

### Task 0: `_safe_context` hardening (desktop/telemetry.py)

Added `_CONTEXT_FILENAME_RE = re.compile(r'\.[A-Za-z][A-Za-z0-9]{0,7}\Z')` and a secondary rejection inside `_safe_context`:

- Before: `'manuscript_notes.docx'` passed `_CONTEXT_RE` verbatim → PRIV-04 leak
- After: any `_CONTEXT_RE`-passing value whose final dotted segment looks like a file extension collapses to `'unregistered'`
- No regression: `'search_tab.run_query'` (final segment `run_query` contains `_`) still passes

### Tasks 1 + 2: `tests/test_telemetry_priv04.py` (9 test functions)

**Forbidden-field/key tests (D-01):**

| Test | Forbidden Input | Key-level assertion | Value-level assertion | Surviving allowlisted prop |
|------|----------------|---------------------|-----------------------|---------------------------|
| `test_priv04_my_library_path_not_in_payload` | `path=r'C:\Users\gersh\...'` | `'path' not in props` | needle absent from `json.dumps` | `search_mode`, `corpus_scope` |
| `test_priv04_filename_key_dropped` | `filename='manuscript_notes.docx'` | `'filename' not in props` | needle absent | `search_mode` |
| `test_priv04_hebrew_query_context_unregistered` | `context='תשובות הרמבּם'` | `props['context'] == 'unregistered'` | Hebrew needle absent | `search_mode` |
| `test_priv04_hebrew_value_redacted_on_scrub_path` | `$set={'h': 'תשובות'}` | `nested['h'] == '[REDACTED]'` | Hebrew needle absent | — |
| `test_priv04_filename_shaped_context_not_leaked` | `context='manuscript_notes.docx'` | `props['context'] == 'unregistered'` | filename absent | `search_mode` |
| `test_priv04_track_error_path_context_and_message_not_leaked` | `context=r'C:\...\q.docx', exc=ValueError(path)` | `props['context'] == 'unregistered'` | both path needles absent | `exc_type=='ValueError'` |
| `test_priv04_crash_forbidden_fields_dropped` | `frame_locals={...}, traceback_raw='...path...'` | both keys absent | raw values absent | `exc_type` |
| `test_priv04_hostname_username_dropped` | `hostname='hillelpc', username='gersh'` | both keys absent | both raw values absent | `app_version` |

**Pre-consent zero-emit (D-02):**

| Test | Entry points exercised | Assertion |
|------|----------------------|-----------|
| `test_priv04_pre_consent_zero_emit_all_entry_points` | `track()`, `track_performance()`, `track_error()` | `ph._event_queue.empty()` after all three |

## Verification Results

```
python -m pytest tests/test_telemetry_priv04.py -q           -> 9 passed
python -m pytest tests/test_telemetry_no_direct_posthog.py -q -> 7 passed (PRIV-03 green, unchanged)
python -m pytest tests/test_telemetry_scrubbing.py tests/test_telemetry_review_fixes.py tests/test_telemetry_consent_gate.py tests/test_telemetry_no_direct_posthog.py tests/test_telemetry_priv04.py -q -> 51 passed
python -m ruff check desktop/telemetry.py tests/test_telemetry_priv04.py -> All checks passed
```

## Deviations from Plan

None — plan executed exactly as written.

The plan correctly predicted that `_CONTEXT_FILENAME_RE` with the `\.[A-Za-z][A-Za-z0-9]{0,7}\Z` pattern would preserve `search_tab.run_query` (final segment `run_query` has `_`) while rejecting `manuscript_notes.docx`, `report.pdf`, and `notes.txt`.

## Known Stubs

None. All tests exercise the actual scrubber pipeline with real payloads captured from `ph._event_queue`.

## Threat Flags

None. The new test file is test-only infrastructure with no new network endpoints, auth paths, or schema changes. The `_safe_context` hardening is a pure tightening of an existing guard with no new trust boundary crossings.

## Self-Check: PASSED

- `tests/test_telemetry_priv04.py` exists: FOUND
- `desktop/telemetry.py` has `_CONTEXT_FILENAME_RE`: FOUND
- Commit `90a4269c` exists: FOUND (Task 0)
- Commit `44b31a76` exists: FOUND (Tasks 1+2)
- All 9 tests in `test_telemetry_priv04.py` pass: VERIFIED
- PRIV-03 guard (`test_telemetry_no_direct_posthog.py`) unchanged and green: VERIFIED
- Full 51-test telemetry suite green: VERIFIED
