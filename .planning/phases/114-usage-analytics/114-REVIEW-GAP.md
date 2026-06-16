---
phase: 114-usage-analytics
reviewed: 2026-06-16T00:00:00Z
depth: standard
files_reviewed: 2
files_reviewed_list:
  - genizah_app.py
  - tests/test_telemetry_phase114.py
findings:
  critical: 1
  warning: 2
  info: 2
  total: 5
status: issues_found
---

# Phase 114: Gap-Closure Code Review Report (CR-114-01..06)

**Reviewed:** 2026-06-16
**Depth:** standard (adversarial, scoped to `a665b4ef..HEAD`)
**Files Reviewed:** 2 (`genizah_app.py`, `tests/test_telemetry_phase114.py`)
**Status:** issues_found

## Summary

Reviewed the six gap-closure fixes (CR-114-01..06) closing Codex findings in the desktop
telemetry. Five of the six fixes are correct and complete:

- **CR-114-02** (`_reset_search` cancelled emit) — CORRECT. Mirrors `stop_search`, emitted-guard
  gives exactly-once, cancelled action omits `result_count_bucket` (D-08 holds).
- **CR-114-03** (`_reset_composition` cancelled emit) — CORRECT. Emitted-guard prevents
  double-emit with `on_comp_scan_finished`; cancelled omits bucket; no-run case returns early.
- **CR-114-04** (`closeEvent` SESSION_END gate) — CORRECT. Gate is
  `_telemetry_ready() AND truthy _session_id AND not _session_end_emitted`; no orphan emit,
  real session emits once, flag set only inside the gated branch.
- **CR-114-05** (`open_join_workbench(emit_telemetry=…)`) — CORRECT. Default `True` preserves
  user-gesture emits; restore passes `False`; the distinct anchor-load method
  `open_joins_workbench` (with `s`) is untouched and not reachable during restore, so no other
  caller is accidentally suppressed.
- **CR-114-06** (comp-resume uses `_set_active_tab`) — CORRECT. `_set_active_tab` sets
  `_programmatic_tab_change=True` around a synchronous `setCurrentWidget`, suppressing the
  `tab_activated` emit; `_restoring_session` is already `False` at that site, so the
  programmatic flag is the load-bearing suppressor (comment is accurate).

**CR-114-01 (PGP-tag stale-slot guard) is NOT correctly fixed.** The per-run token guard is
unreachable dead code: it compares the *live* run object's token against the *live* active
token, both of which are always set in lockstep in `_execute_tag_search`. A stale queued slot
does not carry its own token — it reads current state — so the guard can never fire for the
race it claims to close. The only actual protection is `finished.disconnect()`, whose ability
to drop already-posted queued invocations is Qt-implementation-dependent and not guaranteed.
The accompanying regression test fabricates an impossible runtime state, so it passes
vacuously without exercising the real defect.

Cross-cutting checks PASS: no user content / PII enters any event prop (the PGP `tag` string is
never passed to the emit helper; only `_telemetry_result_bucket(result_count)` and hardcoded
`mode`/`corpus`); identity untouched (still `_uuid`); no forbidden translated accessor
(`currentText()`/`tabText()`) introduced into any telemetry-call argument by these fixes.

All 89 tests in `tests/test_telemetry_phase114.py` pass (offscreen Qt).

## Critical Issues

### CR-01: CR-114-01 token guard is unreachable dead code — the stale-slot race is NOT closed

**File:** `genizah_app.py:19071` (guard) + `genizah_app.py:19099-19116` (`_execute_tag_search`)

**Issue:**
The fix adds a per-run token guard to `_emit_pgp_tag_search_telemetry`:

```python
run = getattr(self, '_current_pgp_tag_search_run', None)
if run is None or run.get('emitted'):
    return
if run.get('token') != getattr(self, '_pgp_tag_active_token', None):  # 19071
    return
```

The intent (per CR-114-01) is that a stale queued `_on_tag_search_results` slot from a
superseded PGP worker "carries the OLD run's token" and so returns early on mismatch. **But the
stale slot does not carry any token of its own.** When the queued slot finally runs, it reads
`self._current_pgp_tag_search_run` — which `_execute_tag_search` has *already replaced* with the
new run B object — and `self._pgp_tag_active_token` — also already bumped to B. `_execute_tag_search`
is the only writer of both, and it always sets them together (19108 + 19115):

```python
self._pgp_tag_run_seq = getattr(self, '_pgp_tag_run_seq', 0) + 1
self._pgp_tag_active_token = self._pgp_tag_run_seq          # 19108
self._current_pgp_tag_search_run = {
    'mode': 'pgp_tags', 'corpus': 'genizah', 'emitted': False,
    'token': self._pgp_tag_active_token,                    # 19115  (== active token, always)
}
```

So at the moment the stale slot executes, `run.get('token') == self._pgp_tag_active_token` is
**always true** — the guard never fires. There is no code path anywhere that desynchronizes
`_current_pgp_tag_search_run['token']` from `_pgp_tag_active_token` (verified: those are the only
two write sites). Reproduction of the actual runtime sequence:

```
Run A:  active_token=1, current_run={token:1, emitted:False}
        worker A emits finished(tagA, resultsA) -> QUEUED to UI thread
User triggers run B; _execute_tag_search runs:
        wait(A); disconnect(A); active_token=2; current_run={token:2, emitted:False}
Queued STALE A-slot now delivered -> _emit_pgp_tag_search_telemetry('completed', len(resultsA)):
        run = current_run (token=2);  run['token']==active_token(2) -> guard PASSES
        -> emits with run A's count, marks run B emitted=True  (WRONG)
Real B-slot delivered -> sees emitted=True -> suppressed  (run B never reported, or misreported)
```

The *only* thing that can actually prevent the stale A-slot from running is the new
`finished.disconnect(self._on_tag_search_results)` (19103). However, in Qt a queued
(cross-thread) slot invocation is posted to the receiver's event queue at emit time; worker A's
`finished.emit(...)` is the last statement of `run()`, so by the time `_execute_tag_search`'s
`wait()` returns the `QMetaCallEvent` is already posted. Disconnecting a signal does **not**
reliably retract already-posted queued invocations (this is version/implementation-dependent in
Qt/PyQt). Relying on that undocumented behavior, with a non-functional guard as the claimed
backstop, leaves the race materially open.

Impact is analytics-only (a misattributed / suppressed `desktop_search_executed` for PGP-tag
runs) — no crash, no PII leak, no data loss. It is classified BLOCKER because the fix's central
mechanism (token mismatch -> early return) cannot execute at runtime, so the defect CR-114-01
was tasked to close is not actually closed, and the regression test (CR-02 below) hides this.

**Fix:** Make the slot carry its own token so the guard compares the slot's origin run against
the live active token. Capture the token at connect time and pass it through, e.g.:

```python
# in _execute_tag_search, after minting the token:
worker = PGPTagSearchWorker(tag)
_tok = self._pgp_tag_active_token
worker.finished.connect(lambda tag, results, tok=_tok: self._on_tag_search_results(tag, results, tok))
# _on_tag_search_results(self, tag, results, token) -> pass token into the emit helper:
#   self._emit_pgp_tag_search_telemetry('completed', len(formatted), token=token)
# and in the helper compare the PASSED-IN token (not run['token']) to _pgp_tag_active_token:
def _emit_pgp_tag_search_telemetry(self, action, result_count=None, *, token=None):
    ...
    if token is not None and token != getattr(self, '_pgp_tag_active_token', None):
        return  # this slot belongs to a superseded run
```

Alternatively, snapshot the run object per-worker (bind the specific dict to the slot) instead
of reading the shared `self._current_pgp_tag_search_run`, so the stale slot mutates only its own
(already-superseded) dict and can never touch run B.

## Warnings

### WR-01: CR-114-01 regression test is vacuous — asserts an impossible runtime state

**File:** `tests/test_telemetry_phase114.py:252-292`
(`test_pgp_tag_stale_slot_does_not_mark_new_run_emitted`)

**Issue:**
The test sets up `_pgp_tag_active_token = 2` while manually installing
`_current_pgp_tag_search_run = {... 'token': 1}` — a state the production code can never
produce (the two are always written together in `_execute_tag_search`; see CR-01). It then
calls the emit helper and asserts no event fires. The test passes, but it exercises a synthetic
mismatch that the real code path cannot reach, so it gives false confidence that the stale-slot
race is closed when it is not. The companion source-inspection test
(`test_pgp_tag_execute_drains_previous_worker_before_run_object`, lines 314-336) only checks that
`finished.disconnect` precedes the run-object assignment — it does not verify the queued-slot
behavior at all.

**Fix:** After fixing CR-01 so the slot carries its own token, rewrite this test to drive the
real sequence: install run A's token, simulate worker A's slot firing with token=1 *after*
`_pgp_tag_active_token` has been bumped to 2 and `_current_pgp_tag_search_run` replaced with run
B — then assert run B is NOT marked emitted and the stale event does not fire. The test must
feed the *slot's* origin token, not a hand-desynced `run['token']`.

### WR-02: `_reset_composition` cancel path does not disconnect/guard a cooperatively-finishing comp thread by token

**File:** `genizah_app.py:22701-22711`

**Issue:**
`_reset_composition` cancels the comp thread (`cancel_flag=True; wait(3000); terminate()`) and
then emits `_emit_comp_search_telemetry('cancelled')`. This is exactly-once for the *current*
run via the `emitted` flag, which is correct for the normal case. But unlike the PGP-tag path,
there is no per-run token here either — exactly-once across rapid back-to-back comp runs relies
solely on `wait()`/`terminate()` having fully drained `on_comp_scan_finished` before the next
`run_composition` installs a fresh `_current_comp_search_run`. If a cooperatively-finishing comp
scan posts a queued `on_comp_scan_finished` that is delivered *after* a subsequent
`run_composition` replaces the run object (analogous to the CR-114-01 race), the cancelled emit
could attribute to or suppress the new comp run. This is lower-likelihood than the PGP-tag case
(comp cancel is an explicit user gesture and `wait(3000)`+`terminate()` is more aggressive), and
no regression test was found exercising it. Flagging as WARNING so it is consciously assessed
rather than silently inheriting the same class of defect as CR-114-01.

**Fix:** If the team adopts the per-run-token pattern to fix CR-01, apply the same snapshot/token
discipline to the composition run object, or document why `wait(3000)+terminate()` makes the
queued-delivery window non-existent for comp (e.g., the comp thread's finished signal cannot be
in-flight past `terminate()`).

## Info

### IN-01: CR-114-01 disconnect relies on Qt queued-call drop semantics that are undocumented

**File:** `genizah_app.py:19101-19105`

**Issue:** The `try/except (RuntimeError, TypeError)` around `finished.disconnect(...)` is safe
(no exception escapes to the UI thread; `_pgp_tag_search_worker` is initialized to `None` at
3398 so the first-call `is not None` guard avoids `AttributeError`). The drain/disconnect itself
introduces no leak or double-free — the old QThread has finished (`wait()` returned) and is
replaced; GC reclaims it. The only concern is the dependence on disconnect dropping
already-posted queued slot calls (see CR-01); this is a documentation/robustness note, not a
correctness defect in the disconnect mechanics themselves.

**Fix:** Add a brief inline note that the token guard (once functional per CR-01) — not the
disconnect — is the authoritative stale-slot defense, so future readers don't assume disconnect
alone suffices.

### IN-02: CR-114-04 leaves `_session_end_emitted` unset when telemetry is not ready

**File:** `genizah_app.py:26901-26906`

**Issue:** When the gate fails (telemetry not ready or empty `_session_id`),
`_session_end_emitted` is never assigned. This is harmless because `closeEvent` runs once at app
exit (no later re-entry to double-emit), so the exactly-once invariant is preserved by
construction. Noting only for completeness — no action required.

**Fix:** None needed; behavior is correct.

---

_Reviewed: 2026-06-16_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
