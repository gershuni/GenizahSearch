---
status: partial
phase: 88-state-separation-by-deletion
source: [88-VERIFICATION.md]
started: 2026-05-13T00:00:00Z
updated: 2026-05-13T00:00:00Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. SC#2 — Two concurrent browser sessions, xlsx export does not leak between users
expected: |
  Open https://genizahsearch.com (or local dev server) in two different browser
  profiles or two different machines so each has a distinct NiceGUI session
  cookie. In session A: run a search that produces a non-trivial result set
  (e.g., query='שלום' with mode='text'). In session B: do not run any search
  (leave results empty) OR run a different search whose result set is
  disjoint from A's. Then in session B, trigger an xlsx export via the
  export button. The downloaded xlsx file MUST contain session B's result
  set (or an empty/error response indicating B has no results) — it must
  NEVER contain session A's results.
result: [pending]

### 2. SC#2b — Parallels export cross-user isolation under real concurrency
expected: |
  In session A: run a parallels (composition) search with some unique
  source_text (e.g., 'alpha-leak-bait'). In session B: do not run any
  parallels search OR run a different one. Trigger parallels excel/word/json
  export from session B. The exported file MUST NOT contain session A's
  source_text or results. Verify in particular that 'alpha-leak-bait' does
  not appear anywhere in B's exported payload — this proves the legacy
  `app.storage.user['parallels_source_text']` reader-side fallback is
  genuinely dead in production.
result: [pending]

## Summary

total: 2
passed: 0
issues: 0
pending: 2
skipped: 0
blocked: 0

## Gaps
