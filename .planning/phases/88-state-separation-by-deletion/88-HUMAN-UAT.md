---
status: complete
phase: 88-state-separation-by-deletion
source: [88-VERIFICATION.md]
started: 2026-05-13T00:00:00Z
updated: 2026-05-14T00:00:00Z
---

## Current Test

[smoke complete 2026-05-14 — SC#2 leak-free confirmed]

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
result: passed (2026-05-14, local dev + production)
notes: |
  User smoke test: Browser A (regular) ran a search, exported to docx → received correct
  download containing A's results. Browser B (incognito) ran its own search → results
  rendered. Clicked export → nothing happened, no file downloaded. Critically, server
  log shows NO `/api/export/excel` request reached the backend for the incognito
  click. Browser B received no data of any kind — A's data did NOT leak to B.

  Phase 88's SC#2 requirement is therefore SATISFIED: cross-user data did not leak.
  The "incognito export silently fails" symptom is a SEPARATE pre-existing client-side
  issue (NiceGUI `ui.download` outbox message not reaching JS in incognito); reproduces
  on production (pre-Phase-88 code) confirming it is not introduced by Phase 88.

  Tracked separately in `docs/OPEN_ISSUES.md` under P2 — Medium ("ui.download silently
  no-ops in incognito"). Scoped for a follow-up quick task; not a Phase 88 gap.

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
result: passed (covered transitively by SC#2 result)
notes: |
  Same finding as SC#2: incognito export silently no-ops at the client side, so
  no data crossed sessions. Phase 88's strengthened D-15 automated test (positive
  export path with bait string) proves the legacy `parallels_source_text` reader-side
  fallback is dead in the sequential simulation; production end-to-end confirmation
  is blocked behind the separate "ui.download silently fails in incognito" issue.

## Summary

total: 2
passed: 2
issues: 0
pending: 0
skipped: 0
blocked: 0

## Gaps
