---
plan: 75-02
phase: 75-non-regression-verification
status: blocked
outcome: partial-blocker-path
date: 2026-04-17
requirements: [NREG-01]
---

# Plan 75-02 Summary — Walkthrough (HALTED on surface 1)

## Self-Check: PARTIAL — BLOCKER PATH (per D-15, D-17, D-18)

Plan executed Task 1 for surface 1 only, then halted per D-18 because a blocker regression was recorded. Surfaces 2–4 not executed. Task 2 (pytest baseline) not executed per D-18 guard ("pytest runs LAST, only after all manual surfaces pass").

## What happened

Ran inline (not via subagent) per user preference — driving the user through the `75-UAT.md` checklist in the live conversation. Surface 1 (Web Search Responsiveness) walkthrough:

- Items (a) cold-load `/`, (b) Hebrew query `"שלום"` → results, (c) accordion expand, (d) Browse navigation from result, (f) Paginate forward twice → all passed.
- Item (d) has a new regression in the **Back** direction: hitting browser Back from `/browse` to `/` triggers a fresh search from scratch instead of restoring the saved result state. User confirmed against live website (`genizahsearch.com`): live restores state on Back; current working tree does not.
- Item (e) Export: with one row checkbox ticked, Export still emits the full list instead of only the checked row. User classified this as **pre-existing** (confirmed on live website), so it is NOT a v7.9 decomposition regression.

User classified the Back-navigation regression as **Blocker — fix now** (D-15 blocker path).

## Artifacts modified

- `.planning/phases/75-non-regression-verification/75-UAT.md`:
  - Frontmatter `status: in-progress` → `status: failed`
  - `## Current Test` → `[HALTED on surface 1 — BLOCKER regression found; gap plan required before walkthrough resumes]`
  - Test 1 `result: pending` → `result: failed (BLOCKER — back-navigation regression ...)` with detailed note on which items passed and which are pre-existing
  - `## Summary` updated: `total: 5, passed: 0, issues: 1, pending: 4, skipped: 0, blocked: 0`
  - `## Gaps` populated with a structured bullet citing suspected Phase 74 `restore_search_snapshot` / `persist_search_snapshot` origin
  - New `## Notes on surface 1 items that passed` subsection itemizing (a)–(f) passes vs regressions vs pre-existing
- `docs/OPEN_ISSUES.md`:
  - Appended pre-existing Export-ignores-checkbox bug as a new P2 row (NOT under any v7.9 decomposition heading — pre-existing is explicitly out of scope for Phase 75)
  - Updated "Last Updated" timestamp to 2026-04-17
  - Bumped P2 Medium Bugs Open count 13 → 14, Total 72 → 73, grand total Open 24 → 25, grand Total 126 → 127

## Artifacts NOT produced (per D-18 guard)

- `75-pytest-baseline.txt` — NOT generated. Task 2 pytest capture was gated on all 4 surfaces passing (D-18). Will run after gap plan fixes the blocker and surface 1 re-signs.
- `docs/OPEN_ISSUES.md` "v7.9 decomposition — cosmetic perf observations" section — not created. The single regression found was Blocker, not Minor, so it does not belong in that section.

## D-02 fallback

Not invoked. User compared against live website directly (no local baseline worktree needed).

## Next step

Per D-15 blocker path:

```
/gsd-plan-phase 75 --gaps
```

The gap plan should target the Phase 74 snapshot-restore logic in `web/pages/search.py` / `web/pages/search_state.py` / `web/search_bootstrap.py`. After the gap plan commits a fix, re-run the walkthrough from surface 1 (the other surfaces 2–4 are not yet verified), then run pytest baseline as Task 2.

## Why this is the right outcome

D-15 explicitly calls out exactly this scenario: "surface is demonstrably slower in a way that impacts real workflow" → user explicitly labels it "slow enough to fix now" → halt phase, run `/gsd-plan-phase 75 --gaps`. Phase 75 succeeded at its actual purpose (**catching a real decomposition regression**). The refactor did introduce a regression; Phase 75 caught it before merge.

## Notes on Phase 74 traceability

Phase 74's `74-CONTEXT.md` D-20 added E2E coverage for URL-bar updates on browse navigation (which the walkthrough confirmed is still working in the forward direction — URL bar does update on Prev/Next). But the `restore_search_snapshot` / `persist_search_snapshot` helpers added in Phase 74 for search-side state appear to have either a scope mismatch (wrong key / wrong route guard) or are not being invoked on Back navigation into `/`. The gap plan's researcher/planner should examine `web/search_bootstrap.py:bootstrap_search_page()` (or equivalent) and `web/pages/search_state.py:restore_search_snapshot()` first.

## Commits produced by this plan

None yet — this summary is written before commit. The orchestrator will commit `75-UAT.md`, `docs/OPEN_ISSUES.md`, and this `75-02-SUMMARY.md` atomically.
