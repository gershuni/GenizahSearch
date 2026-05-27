---
phase: 100
reviewers: [codex]
reviewed_at: 2026-05-27
review_round: 2
plans_reviewed: [100-01-PLAN.md, 100-02-PLAN.md, 100-03-PLAN.md]
---

# Cross-AI Plan Review — Phase 100 (Round 2)

> Round 2. The round-1 review (single shared controller is the HIGH flaw) drove a replan to
> per-surface scoped controller state + `cancel()` + de-dup. This round verifies those fixes
> and hunts for NEW issues introduced by the per-scope architecture. Round-1 REVIEWS content is
> preserved in git history (commit prior to `58da463a`).

## Codex Review

**Summary**

The per-scope controller revision is architecturally sound for HIGH-1: Browse and ResultDialog no longer share one pending token/state slot, and the ResultDialog single-trigger reasoning matches the actual LOCAL call chain. There is one NEW high-severity timer subtlety: an old in-flight request's watchdog can fire after a newer same-scope request has replaced `_awaiting_token`, causing a false TIMEOUT for the new request.

**Round-1 Fix Verification**

| Item | Status | Note |
|---|---|---|
| HIGH-1 shared controller state | RESOLVED | Per-scope `_awaiting_token`, `_pending`, debounce/watchdog timers fix cross-surface stranding. |
| HIGH-2 cancellation leaving PDF | PARTIAL | Controller cancel is good; ResultDialog should also cover `finished/reject/done`, and Browse non-LOCAL (Genizah) transitions do not explicitly cancel `"browse"`. |
| HIGH-3 duplicate initial ResultDialog render | RESOLVED | Actual chain is `load_result_by_index -> load_page -> load_local_page`; one trigger in `load_local_page` is correct. |
| MEDIUM-4 retained callbacks | RESOLVED | Terminal success/failure/timeout/cancel clear `_pending`; caveat only when cancel is actually reached. |
| MEDIUM-5 Browse page guard | RESOLVED | Lambdas guard both `current_browse_sid` and `current_browse_p`. |
| MEDIUM-6 `fp.lower()` None crash | RESOLVED | Controller `is_pdf()` is None-safe; Browse keeps `or ""` plus `bool(filepath)`. |
| MEDIUM-7 unbounded FIFO backlog | PARTIAL | Debounce fixes sub-150ms bursts; sustained >150ms navigation can still enqueue stale renders, accepted as residual. |
| LOW-8 heavy `_lang()` import | RESOLVED | Lazy import from `genizah_core`, not `genizah_app`. |
| LOW-9 line drift | RESOLVED | Plans use semantic anchors and disjoint edit areas. |

**NEW Concerns**

- **HIGH — Old watchdog can invalidate a newer same-scope request (latest-wins violation).**
  `request()` overwrites `_awaiting_token[scope]` but does NOT stop/re-guard the existing watchdog. Timeline: token 1 enqueued, its watchdog due at 8000ms; at ~7990ms token 2 is requested and enters debounce; token 1's watchdog fires and `_on_watchdog(scope)` reads the CURRENT `_awaiting_token[scope]` — now token 2 — then clears token 2 and shows TIMEOUT. This strands the latest page on a false timeout. The watchdog must be guarded by the token it was armed for.

- **MEDIUM — ResultDialog close coverage may miss reject/accept/done/Esc paths.**
  Plan 02 cancels in `closeEvent`, but a QDialog can finish through `reject()`, `accept()`, `done()`, or Esc without a `closeEvent` depending on Qt behavior. The requirement says "closing/rejecting". Connect the `finished` signal (or override `done()`) so scope cleanup is guaranteed on every termination path.

- **LOW/MEDIUM — Per-dialog timer dict entries accumulate.**
  `cancel()` stops timers but leaves `_debounce_timers[id(dialog)]` and `_watchdog_timers[id(dialog)]` populated for the whole app session. Not a stale-callback leak (the timer lambdas capture only the integer scope), but an unbounded small QObject leak over many opened PDF dialogs. Also note `id(self)` can be reused after a dialog is GC'd, so a stale timer entry could be inherited by a new object reusing that id — a `discard_scope` that removes the dict entries closes both.

- **LOW — Browse non-LOCAL navigation relies on guards, not cancellation.**
  Plan 03 cancels only for non-PDF LOCAL files. Moving from a LOCAL PDF to a Genizah manuscript is caught by the sid/page guards (no stale display), but the controller still retains the `"browse"` callbacks until the in-flight render succeeds or times out.

- **No concern:** `_scope_for_token` linear scan is correct with a single global monotonic counter — duplicate tokens cannot occur in normal UI-thread use. Clearing scope state BEFORE calling `on_image` is the right re-entrancy order.

**Suggestions**

1. Add a per-scope watchdog-token guard: store `_watchdog_token[scope]`; in `request()` stop/clear the scope's watchdog before replacing state; set the guard token in `_fire_pending()`; have `_on_watchdog()` no-op unless the guarded token still equals `_awaiting_token[scope]`.
2. Add a test: "new request arrives just before old watchdog fires" (the HIGH race).
3. Add `discard_scope(scope)` (or `cancel(scope, discard_timers=True)`) for transient dialog scopes: stop, `deleteLater()`, and remove the `_debounce_timers`/`_watchdog_timers` entries. Call it from ResultDialog teardown.
4. Connect `ResultDialog.finished` to idempotent scope cleanup, or override `done()` and clean up before `super().done(result)`.
5. Consider cancelling `"browse"` when entering non-LOCAL (Genizah) Browse paths, not only non-PDF LOCAL.

**Risk Assessment: MEDIUM**

The main architecture is now correct, but the old-watchdog/new-token race is a real latest-wins violation. It is narrow timing-wise but can produce a false timeout and a dropped render for the current page. Once the guarded watchdog token and dialog `finished` cleanup are added, this drops to LOW.

---

## Consensus Summary

Single reviewer (Codex), round 2. The round-1 architectural flaw (shared single-token controller) is **RESOLVED** by the per-scope rewrite. Four new/residual items remain, in priority order:

### Top Concerns (priority order)
1. **HIGH — Watchdog/new-token race.** `request()` replaces `_awaiting_token[scope]` without re-guarding the prior watchdog; the old watchdog fires and times out the NEW token. **Fix:** per-scope `_watchdog_token` guard — the watchdog only acts if its armed token is still the awaited one; stop the scope's watchdog in `request()` before overwriting state. Add the "new request just before old watchdog" test.
2. **MEDIUM — Dialog cleanup not guaranteed on all close paths.** Use `ResultDialog.finished` / override `done()` rather than only `closeEvent`, so reject/accept/Esc all tear down the scope.
3. **LOW/MEDIUM — Per-dialog timer entries accumulate** in `_debounce_timers`/`_watchdog_timers` keyed by `id(dialog)` for the app session (small QObject leak; `id()` reuse risk). **Fix:** a `discard_scope(scope)` that removes the dict entries + `deleteLater()` the timers, called from dialog teardown.
4. **LOW — Browse retains `"browse"` callbacks** when moving from a LOCAL PDF to a Genizah manuscript (guards prevent stale display, but no cancel). Consider cancelling `"browse"` on non-LOCAL Browse transitions too.

### Agreed Strengths
- Per-scope state correctly isolates Browse from ResultDialog (HIGH-1 fixed).
- De-dup reasoning matches the real `load_result_by_index → load_page → load_local_page` chain (HIGH-3 fixed).
- `_scope_for_token` + single global counter is correct; success-before-callback ordering is the right re-entrancy choice.

### Divergent Views
None (single reviewer).

### Recommended next step
`/gsd-plan-phase 100 --reviews` to fold in the watchdog-token guard (HIGH), guaranteed dialog-`finished` cleanup (MEDIUM), and `discard_scope` timer-entry removal (LOW/MEDIUM) — plus the "new request before old watchdog" test.
