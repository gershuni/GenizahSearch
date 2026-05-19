---
quick_id: 260519-9pk
title: "Re-open P1 web memory leak -- investigate secondary leak after export-cap fix"
created: 2026-05-19
scope: docs-only (no code changes)
result: success
---

# Quick task 260519-9pk -- closeout summary

## What this task did

Three docs/state updates, zero code changes:

1. **Re-opened the P1 row in `docs/OPEN_ISSUES.md`**
   - Status cell flipped from `Fixed (2026-05-18)` -> `Partially Fixed (2026-05-18 export cap); Secondary leak Re-opened (2026-05-19)`
   - Notes cell got a "RE-OPENED 2026-05-19" preamble documenting the 411 MB/hr post-soak growth rate, the verdict-band threshold breach (>100 MB/hr = secondary leak), and the evidence that the cap fix is still working for its own surface
   - Suspect-surfaces list extended with item (6): the Phase 92.2 WeakKeyDictionary task-memo on `get_user_client()`
   - Quick Summary counts updated: P1 Open 0->1, Total Open 33->34
   - Last Updated stamp bumped to 2026-05-19 with prepended preamble
   - Change log row appended

2. **Migrated `.planning/todos/pending/2026-05-18-verify-memstat-after-export-cap-fix.md` to `.planning/todos/done/`**
   - Verdict block appended at the end recording the 411 MB/hr outcome and the warning-band classification
   - Three follow-up actions documented (P1 re-open, follow-up scoped, objgraph baseline deferred to the follow-up phase)
   - File migrated as a git rename

3. **Created `.planning/todos/pending/2026-05-19-leak-attribution-phase.md`**
   - Scopes the next attribution work: add `/_internal/objgraph` + `/_internal/tracemalloc` companion endpoints next to `/_internal/memstat`, attribute the next surface from objgraph data, ship a bounded-cache / weak-ref / cancellation fix
   - Done-criteria: 24h soak measures < 30 MB/hr growth rate
   - Explicitly NOT scoped into v7.13 (roadmap locked at Phase 93 + 94); default schedule is "next phase after v7.13 ships", with an urgent-insertion override trigger if growth worsens

## Key numbers

| Metric | Value | Source |
|---|---|---|
| Post-deploy baseline RSS (2026-05-18 15:41:13 UTC) | 1.78 GB | predecessor todo |
| 11h soak reading (2026-05-19) | 6.3G, peak 6.8G | systemctl status |
| Growth rate | ~411 MB/hr | computed |
| Verdict threshold breached | >100 MB/hr -- secondary leak | predecessor todo |
| Cap-fix surface (still working) | 498 MB -> 512 KB live payload | predecessor todo |

## What this task did NOT do

- No code changes to `web/main.py` or anywhere else
- No new endpoints (`/_internal/objgraph` etc.) -- that's scoped into the new follow-up todo
- No leak attribution -- requires objgraph data first
- No fix attempt -- the fix is a future phase

## What's next

The user can now (a) review `docs/OPEN_ISSUES.md` for the re-opened P1 entry,
(b) review the new pending todo at
`.planning/todos/pending/2026-05-19-leak-attribution-phase.md`, and
(c) decide when to schedule the attribution phase -- default is "after v7.13
ships" unless growth worsens.

## Commits

- `0cbd2bda` docs(quick-260519-9pk): re-open P1 web memory leak in OPEN_ISSUES.md
- `3bfba741` docs(quick-260519-9pk): close verify-memstat todo with warning-band verdict (git rename)
- `374e677d` docs(quick-260519-9pk): append verdict block to verify-memstat todo
- (this task) docs(quick-260519-9pk): scope leak-attribution follow-up + closeout

## Self-Check

To be appended below after task execution complete.

---

## Self-Check: PASSED

All files exist on disk:
- `docs/OPEN_ISSUES.md` (modified: P1 row re-opened, counts updated, change log appended)
- `.planning/todos/done/2026-05-18-verify-memstat-after-export-cap-fix.md` (migrated with verdict block)
- `.planning/todos/pending/2026-05-19-leak-attribution-phase.md` (new follow-up scope)
- `.planning/quick/260519-9pk-re-open-p1-web-memory-leak-investigate-s/260519-9pk-SUMMARY.md` (this file)

Old pending todo confirmed removed:
- `.planning/todos/pending/2026-05-18-verify-memstat-after-export-cap-fix.md` (no longer exists)

All commits found in git log:
- `0cbd2bda` re-open P1 web memory leak in OPEN_ISSUES.md
- `3bfba741` close verify-memstat todo with warning-band verdict (git rename)
- `374e677d` append verdict block to verify-memstat todo

Final Task 3 closeout commit follows.
