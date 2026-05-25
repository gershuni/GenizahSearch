---
phase: 98
reviewers: [codex]
reviewed_at: 2026-05-25T14:00:00Z
plans_reviewed: [98-01-PLAN.md, 98-02-PLAN.md, 98-03-PLAN.md, 98-04-PLAN.md, 98-05-PLAN.md, 98-06-PLAN.md]
verdict: NOT_READY — 5 issues must be addressed before execution
---

# Cross-AI Plan Review — Phase 98 NLI Resilience

## Codex Review

**Verdict:** Not good as-is. 5 real plan↔code drift issues found that the internal `gsd-plan-checker` did not catch.

### Issue 1 (BLOCKER) — Plans 98-05 / 98-06 assume a nonexistent `GenizahCore` class

**Symptom:** Current code has `class MetadataManager` at `genizah_core.py:3540`; the NLI methods and legacy class breaker live there. There is no `GenizahCore` symbol anywhere in the codebase.

**Affected lines:**
- `98-05-PLAN.md:494, 520, 612, 706` — references `GenizahCore`
- `98-06-PLAN.md:339-352` — references `GenizahCore`

**Fix:** Replace every `GenizahCore` with `MetadataManager`. Tests/acceptance checks will fail to import otherwise.

**Severity:** BLOCKER. The plans cannot execute correctly until this is fixed.

---

### Issue 2 (BLOCKER) — D-26 concurrency test does not prove the Nyquist invariant

**Symptom:** `98-VALIDATION.md:69` requires 20 workers, wall time `<10s`, and `_nli_session.get` calls `≤10`. But Plan 98-02's proposed test:
- Uses a fake call that sleeps only `0.3s` (`98-02-PLAN.md:830-834`)
- Permits `threshold + N_WORKERS` calls (`:865-870`, effectively up to 23)
- Only asserts `< N_WORKERS` at `:876`

**Why it's weak:** With 20 workers spawning near-simultaneously, all of them can pass the `is_open()` check BEFORE any of the first 3 failures finish recording. The test could pass even if the breaker doesn't actually short-circuit anything.

**Fix:** The test needs to either:
- Use a slower fake call (e.g., 2-3 seconds per failure to give the breaker time to trip during the workload)
- Use a synchronization primitive (`threading.Event` set after threshold-th failure is recorded) so workers spawn after the breaker is already open
- AND assert `get_call_count <= threshold` (3), not `< N_WORKERS` (20). The whole point is to prove that ≤3 calls hit the network even with 20 workers ready to fire.

**Severity:** BLOCKER. This is the Nyquist-critical invariant; if this test doesn't actually prove threadpool resilience, the phase doesn't meet its goal.

---

### Issue 3 (HIGH) — Several "guard both / guard loop" requirements are only guarded once

**Symptom:** After the breaker opens mid-call, a single inbound request can still burn extra NLI timeouts because the breaker is checked only at the entry point, not at fallback boundaries.

**Affected sites:**
- `web/api.py:713` MARC fallback — Plan 98-03 records IIIF failure, then proceeds to MARC with no breaker recheck (`98-03-PLAN.md:315-356`). D-15 mandates the MARC fetch is guarded.
- `/api/nli_image/{fl_id}` Rosetta fallback — Plan checks before IIIF only (`98-03-PLAN.md:408-414`), then Rosetta fallback runs without rechecking (`:449-478`). D-16 says guard both.
- `_fetch_nli_image_bytes` FL ID iteration — Plan checks once before FL resolution (`98-03-PLAN.md:481-520`); current fallback loop is `web/api.py:849-854`; no per-iteration or `_try_fl` breaker check. With cached FL IDs, one request can still try every FL ID.
- `_fetch_single_worker` retry — Plan 98-05 guards before the retry loop only (`98-05-PLAN.md:382-430`), so a first failure that opens the breaker can still be followed by a second retry call.

**Fix:** Add a breaker recheck inside each fallback/loop boundary. Cheap (it's a lock-protected read on a module-level int) but materially changes worst-case blocking.

**Severity:** HIGH. Doesn't break correctness but materially weakens the threadpool-resilience guarantee.

---

### Issue 4 (MEDIUM) — Static timeout validation will false-fail on legitimate non-NLI code

**Symptom:** Plan 98-03 says `timeout=30` should not appear in `web/api.py` (`98-03-PLAN.md:875-877`), but current non-NLI image endpoints legitimately use `timeout=30` at `web/api.py:1037, 1105, 1162, 1280`. Plan 98-06 allows only one `timeout=15` and one `timeout=30` across four files (`98-06-PLAN.md:268-281`), but `genizah_core.py:4282` and `:4425` have `Future.result(timeout=15)` — these are NOT NLI `requests` calls, they're concurrent-future waits.

**Fix:** Use AST/context filtering, not raw grep. Either:
- Use `ast.NodeVisitor` to find `requests.get/post(...)` calls whose URL string matches `iiif.nli.org.il` or `rosetta.nli.org.il`, then check their timeout kwarg
- OR grep for `_nli_session.get(... timeout=` specifically (since the project uses a dedicated NLI session)

**Severity:** MEDIUM. Will cause false-positive test failures during execution, blocking the verify step.

---

### Issue 5 (MEDIUM) — PostHog telemetry creates two disconnected queues

**Symptom:** Plan 98-01 explicitly says `web/api_hardening.py` is NOT modified (`98-01-PLAN.md:50, 576`). That preserves existing `capture_api_event` callers and tests that monkeypatch `web.api_hardening._event_queue`. But it creates a separate `shared.posthog_server` queue for the breaker, with separate drop counters.

**Operational consequence:** `98-VALIDATION.md:100` instructs operators to watch `web/api_hardening.get_dropped_event_count()` — this counter will NOT observe drops from the NLI breaker telemetry. An operator seeing 0 dropped events could be missing a flood of breaker-event drops.

**Fix:** Either:
- (A) Update `98-VALIDATION.md` to also surface `shared.posthog_server.get_dropped_event_count()` (or rename `web/api_hardening.get_dropped_event_count` to delegate to the shared module)
- (B) Refactor `web/api_hardening.py` to import from `shared/posthog_server.py` (Plan 98-01 should do this to avoid two queues)

**Severity:** MEDIUM. Doesn't break the breaker; degrades observability.

---

## Checks that PASSED

- Symbol existence: requested symbols exist with the expected signatures, except the `GenizahCore` issue (Issue 1).
- Line-number drift is minor: `web/api.py:647` is the semaphore comment, actual acquire is `:648`; `:680` and `:713` are correct. Breaker block starts at `genizah_core.py:3938` not `:3940`, but Plan 98-05 names all four legacy attributes and all three methods correctly.
- `/api/nli_image/{fl_id}` and `/api/proxy_image` ARE covered by Plan 98-03 (though incompletely per Issue 3).
- No additional production server-side `requests.get` calls to NLI hosts beyond D-14..D-23. Browser-side JS fetches and maintenance scripts don't consume the Starlette threadpool.
- D-04 monotonic-time is satisfied in the planned breaker module. Remaining `time.time()` usage is for TTL caches or quoted-for-deletion code.

---

## Consensus Summary

Single-reviewer review (Codex only — Claude is the executing AI, skipped for independence).

### Required fixes before execution
1. **Issue 1 (BLOCKER):** `GenizahCore` → `MetadataManager` across 98-05 and 98-06
2. **Issue 2 (BLOCKER):** Strengthen D-26 concurrency test — slower fakes, sync primitive for spawn ordering, assert `get_call_count <= threshold`
3. **Issue 3 (HIGH):** Add breaker rechecks at MARC fallback, Rosetta fallback, FL-ID iteration loop, and retry loop
4. **Issue 4 (MEDIUM):** AST/context filtering for timeout validation tests
5. **Issue 5 (MEDIUM):** Unify PostHog drop counter or update observability docs

### Recommendation
Re-plan with `/gsd-plan-phase 98 --reviews` to incorporate these fixes before executing.
