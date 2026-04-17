---
plan: 75-02
phase: 75-non-regression-verification
status: complete
outcome: passed
date: 2026-04-17
requirements: [NREG-01]
---

# Plan 75-02 Summary — Walkthrough COMPLETE (all 5 tests green)

## Self-check: COMPLETE

Plan 75-02 initially halted on surface 1 (back-navigation blocker, recorded in the earlier version of this summary). Plan 75-03 closed the gap (commits 4a04aab3 + 8f9c5ef3 + f40b8eab + 1d722198), surface 1 re-signed green, and this plan resumed at surface 2. All 4 manual surfaces then signed off, pytest ran LAST per D-18, all tests passed.

## Per-surface sign-offs

| Surface | Status | Notes |
|---|---|---|
| 1 — Web Search Responsiveness | passed 2026-04-17 | Initial run failed on item (d) back-nav; 75-03 fixed; user re-signed green. Item (e) pre-existing Export bug logged to OPEN_ISSUES.md P2. |
| 2 — Web Browse Responsiveness | passed 2026-04-17 | Items (a)/(b) re-run against substitute sys_id 990001273900205171 (HUC MS. 2065) because original Oxford arch. O.d.8/1 had no in-app data; Fixed Test Manuscripts table updated. Item (f) JTS DPUL image-source-switch pre-existing bug logged to OPEN_ISSUES.md P2. |
| 3 — Desktop Search Responsiveness | passed 2026-04-17 | Functional baseline + D-09 composition timer/ETA green. Two pre-existing perf issues logged to OPEN_ISSUES.md P2 (desktop search ~30s time-to-first-results; composition UI freeze ~15s). User confirmed both NOT v7.9 regressions. |
| 4 — Desktop Browse Responsiveness | passed 2026-04-17 | Functional baseline + D-06 responsiveness overlay both green. No issues. |
| 5 — pytest baseline | passed 2026-04-17 | 1089 passed, 8 skipped, 0 failed, 0 errors in 37.9s. +18 vs 75-03 expectation (1071) is env-explained: local full suite vs Phase 74 CI's `--ignore=tests/e2e` (1085) + 4 new 75-03 tests = 1089. User accepted. |

## D-02 worktree fallback

Not invoked at any point. Surface 1 blocker was confirmed against the live website directly; no A/B comparison needed at subsequent surfaces.

## Pytest baseline counts

- **Actual:** `1089 passed, 8 skipped` (0 failed, 0 errors, 37.91s)
- **Expected per 75-03:** `1071 passed, 8 skipped`
- **Expected per 75-02 original:** `1067 passed, 8 skipped`
- **Reconciliation:** Phase 74 CI ran with `--ignore=tests/e2e` (→ 1085 passed). Local full-suite includes e2e tests. 1085 + 4 new tests from 75-03 (back-nav restore, fresh-query-different-saved, empty-snapshot edge, back-nav-restores-saved-mode-Title) = 1089. The 1067/1071 baseline numbers encoded in earlier plan artifacts were themselves stale CI-basis estimates; the actual local full-suite result is 1089 with zero failures and no new skips, which is healthy and non-regressive.

## Pre-existing issues logged to OPEN_ISSUES.md (P2)

All out of Phase 75 scope — phase only covers regressions introduced by Phases 67–74. Logged for future triage:

1. **Web search Export ignores row checkboxes** — surface 1 item (e). `web/pages/search.py` Export handler emits full list regardless of checkbox selection.
2. **JTS browse page missing Princeton DPUL source switch** — surface 2 item (f). `web/pages/browse.py` — no source-switch control visible; credit reads NLI for a JTS manuscript that should default to DPUL per v7.2.3 design intent.
3. **Desktop search time-to-first-results ~30s** — surface 3 note. `genizah_app.py`, `gui_threads.py::SearchThread`.
4. **Desktop composition UI freezes ~15s** — surface 3 note. `genizah_app.py` composition path, `gui_threads.py::SearchThread` composition branch.

OPEN_ISSUES.md P2 count moved from 14 to 17; Total from 127 to 130 across the walkthrough.

## Artifacts produced

- `.planning/phases/75-non-regression-verification/75-UAT.md` — frontmatter `status: passed`; all 5 tests `result: passed`; Summary `passed: 5, pending: 0`; Current Test `[none — all items passed]`. Fixed Test Manuscripts table records the HUC 2065 substitution for surface 2 items (a)/(b).
- `.planning/phases/75-non-regression-verification/75-pytest-baseline.txt` — full pytest output, final line `1089 passed, 8 skipped in 37.91s`. 60 lines total.
- `docs/OPEN_ISSUES.md` — 4 new P2 rows added (2026-04-17 surfacing source noted in each); summary counts updated.

## Artifacts NOT produced (per phase separation)

- `75-VERIFICATION.md` — verifier agent's job, invoked separately by the orchestrator via `/gsd-verify-phase 75`. This plan stops at `75-UAT.md` reaching `status: passed`.

## Minor perceptual notes (D-15 minor path)

No "v7.9 decomposition — cosmetic perf observations" section was created in OPEN_ISSUES.md. The two desktop perf issues flagged on surface 3 are PRE-EXISTING (user explicitly confirmed NOT v7.9 regressions), so they belong in the regular P2 section, not a v7.9-decomp subsection. The v7.9 decomposition did NOT introduce any new perceptible slowdowns in the 4 surfaces.

## Commits produced across 75-02's execution arc

- (earlier halt) commits written before 75-02's initial 2026-04-17 halt for the blocker-path UAT state.
- 4a04aab3 — docs(75-03): root-cause file
- 8f9c5ef3 — fix(75-03): bootstrap is_back_navigation branch + 9 tests + storage-write hole close
- f40b8eab — fix(75-03): elif cascade reorder (Gemini Option B) + UAT flip
- 1d722198 — docs(75-03): 75-03 summary
- 64afdf6b — docs(75-02): surface 2 passed + log pre-existing JTS DPUL bug + HUC 2065 substitute
- 3b9f4ece — docs(75-02): surface 3 passed + log 2 pre-existing desktop perf issues
- (this commit) — docs(75-02): finalize — surface 4 passed + pytest 1089 passed + UAT status: passed

## Next step

Orchestrator invokes `/gsd-verify-phase 75` to produce `75-VERIFICATION.md`. With UAT `status: passed`, all 5 tests green, and `75-pytest-baseline.txt` captured, the phase gate is satisfied per D-19.
