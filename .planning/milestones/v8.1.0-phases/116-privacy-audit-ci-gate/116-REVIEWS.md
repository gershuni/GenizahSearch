---
phase: 116
reviewers: [codex]
reviewed_at: 2026-06-16T10:27:08Z
plans_reviewed: [116-01-PLAN.md, 116-02-PLAN.md, 116-03-PLAN.md]
---

# Cross-AI Plan Review — Phase 116

## Codex Review

**Overall Summary**

The phase structure is mostly right: lightweight tests, one small CLI probe, and operational docs. The main problem is Plan 116-02: its `SSL_OK` logic is not valid against the current transport because `get_dropped_event_count()` only counts queue saturation, not missing keys, SSL failures, network failures, or swallowed `requests.post()` exceptions. As written, the self-test can falsely pass SC#3. Plan 116-01 is directionally good but should assert forbidden values are absent across the whole payload, not only banned keys.

**116-01 Review**

*Strengths*
- Correctly keeps PRIV-04 at scrubber/chokepoint unit depth, matching the locked "nothing heavy" decision.
- Extends existing pytest patterns and covers all three public consent-gated entry points.
- Correctly treats PRIV-03 as already shipped and only verifies it.

*Concerns*
- **HIGH:** `test_priv04_hebrew_query_value_redacted` expects `context='Hebrew...'` to become `[REDACTED]`, but current `_emit()` special-cases `context` through `_safe_context()`, so it should become `unregistered`.
- **HIGH:** The plan tests `filename` only on a banned key. It does not catch filename/query leakage through allowed keys such as `context`; current `_safe_context` permits dotted identifier-shaped strings, so a filename-shaped context is worth testing.
- **MEDIUM:** Key-absence assertions are weaker than the requirement. PRIV-04 is about no paths, filenames, query text, usernames, or hostnames in the payload, so tests should search serialized payload values too.
- **LOW:** Queue capture through `ph._event_queue.get()` follows existing patterns but is potentially racy with the drain thread.

*Suggestions*
- Add a helper that serializes the captured payload with `json.dumps(..., ensure_ascii=False)` and asserts raw forbidden needles are absent.
- Change the Hebrew `context` expectation to `unregistered`, or use a nested allowed container like `$set` if the test specifically wants `[REDACTED]`.
- Add regression cases for `context='manuscript_notes.docx'`, path-like context, and `track_error()` with a path/query-shaped context and exception message.
- Consider disabling `_start_drain_thread_once` in this test fixture if queue capture flakes.

*Risk Assessment:* **MEDIUM**. The test plan is close, but as written it can miss allowed-key value leaks and one proposed assertion conflicts with current code behavior.

**116-02 Review**

*Strengths*
- Correct entry-point placement: pre-`QApplication`, modeled on `--self-test-pymupdf`.
- Correctly avoids `set_consent(True)` and uses an in-memory consent toggle restored in `finally`.
- Includes the right HUMAN-UAT: clean Windows VM with no Python, live PostHog check, offline launch.

*Concerns*
- **HIGH:** `get_dropped_event_count()` is the wrong success signal. In `shared/posthog_server.py`, it only increments on `queue.Full`; no key and `requests.post()` failures are silently ignored. This can print `SSL_OK` when no event was delivered.
- **HIGH:** `sleep(1.5)` does not prove the daemon completed one POST, especially with `requests.post(timeout=2.0)`. The process can exit before a deterministic result exists.
- **MEDIUM:** `--telemetry-selftest-offline` still calls `run_selftest()` after wiring transport, so it may start a background POST attempt even though the arm claims no network wait.
- **MEDIUM:** No-key handling is not actually implemented despite being called out in research.

*Suggestions*
- Replace the drop-counter check with a synchronous, return-valued self-test send path. Minimal shape: a helper that resolves the configured key/host, returns `NO_KEY` if no `phc_` key is wired, performs one `requests.post(..., timeout=2.0)`, checks HTTP success, and returns `SSL_OK` or `SSL_FAIL`.
- Keep `run_selftest()` for pipeline sanity if desired, but do not use the async queue/drop counter as delivery proof.
- Update `files_modified` if the fix needs `shared/posthog_server.py` or `desktop/telemetry.py`.
- For offline mode, either avoid wiring a key or clearly document that `OFFLINE_OK` is only a no-GUI/no-config-write smoke path; the real network-disabled behavior remains HUMAN-UAT.

*Risk Assessment:* **HIGH as written**. This plan can falsely satisfy SC#3, which is the phase's main non-automated validation.

**116-03 Review**

*Strengths*
- Covers the required runbook topics: shared project, `platform=desktop`, key posture, rotation, both drop counters, self-test, opt-out behavior.
- Correctly amends the stale isolated-project requirement and cites the 2026-06-14 decision.
- Documents the milestone-exit regression gate without adding CI churn.

*Concerns*
- **MEDIUM:** It depends on Plan 116-02 wording; if the self-test remains drop-counter based, the runbook will document misleading diagnostics.
- **MEDIUM:** `depends_on: []` is unsafe for status flips. This plan can mark INFRA-06 or PRIV-04 complete before the tests/self-test code actually land.
- **LOW:** The proposed grep `grep -qiv "isolated"` does not prove absence of the word; it succeeds if any line does not contain it.
- **LOW:** The docs check command should use the repo's Windows-safe pattern from AGENTS: `PYTHONIOENCODING=utf-8 python scripts/check_docs.py`.

*Suggestions*
- Make 116-03 depend on 116-01 and the code portion of 116-02, or split "draft docs" from "mark requirements complete."
- Fix runbook language so drop counters are described only as queue saturation counters.
- Replace the grep absence check with `! grep -qi "isolated" docs/guides/TELEMETRY_RUNBOOK.md`.
- Leave phase-level completion and HUMAN-UAT closure to the final milestone verification pass.

*Risk Assessment:* **LOW-MEDIUM**. The doc scope is right, but it must not encode the flawed self-test status semantics or prematurely mark requirements complete.

---

## Orchestrator Code Verification

> Claude verified Codex's HIGH findings against the live code before recording them. All
> three HIGH findings are **confirmed against current source** (not stale-code assumptions),
> which makes them high-confidence input for `--reviews` replanning.

| Finding | Plan | Verdict | Evidence (file:line) |
|---------|------|---------|----------------------|
| `get_dropped_event_count()` only counts `queue.Full` — wrong SSL success signal | 116-02 | **CONFIRMED** | `shared/posthog_server.py:88-89` docstring ("dropped due to queue saturation"); `_dropped_events += 1` only inside `except queue.Full:` at `:228-230`; `requests.post(..., timeout=2.0)` at `:258` wrapped in bare `except Exception:` at `:259` (swallows SSL/network); module docstring `:14-15` notes no-key drops silently at drain time |
| `run_selftest()` returns no value | 116-02 | **CONFIRMED** | `desktop/telemetry.py:836` — `def run_selftest() -> None:` |
| Hebrew `context` becomes `unregistered`, not `[REDACTED]` | 116-01 | **CONFIRMED** | `_safe_context` returns `'unregistered'` for non-identifier strings (`desktop/telemetry.py:349-355`), applied to `context` after scrub at `:675-676` |
| Filename-shaped `context` (`manuscript_notes.docx`) may survive `_safe_context` | 116-01 | **PLAUSIBLE — must test** | `_safe_context` returns the value verbatim when "identifier-shaped" (`:349-354`); a dotted filename may match → worth an explicit regression test |
| `depends_on: []` lets 116-03 flip INFRA-06/PRIV-04 complete before code lands | 116-03 | **VALID** | all 3 plans are `wave: 1, depends_on: []`; status-flip in 116-03 has no ordering guard vs 116-01/116-02 code |

**Net:** Codex caught a real false-confidence bug — keying SC#3's `SSL_OK` off the drop counter would pass even when delivery failed. This alone justifies replanning 116-02 around a synchronous, return-valued send path (new helper in `shared/posthog_server.py` or `desktop/telemetry.py` that does one `requests.post` and returns `SSL_OK`/`SSL_FAIL`/`NO_KEY`). 116-01's test expectations should be aligned to actual scrubber behavior (`unregistered`) and extended to forbidden-*value* assertions over the serialized payload + allowed-key (`context`) leak paths.

---

## Consensus Summary

Single external reviewer (Codex), corroborated by orchestrator code verification.

### Agreed Strengths
- Correct lightweight scope: scrubber-unit test depth, one small CLI probe, ops docs — matches the locked "nothing heavy" decision.
- PRIV-03 correctly treated as already-shipped (verify-only, not re-implemented).
- Self-test entry-point placement (pre-`QApplication`, in-memory consent toggle, no `set_consent`) is right.
- Runbook topic coverage (shared project, both drop counters, key posture/rotation, opt-out) and the INFRA-06 amendment are complete.

### Agreed Concerns (priority order)
1. **[HIGH] 116-02 SC#3 false pass** — the `SSL_OK` decision must NOT key off `get_dropped_event_count()` (queue-saturation only) or a fixed `sleep`. Needs a synchronous return-valued send path that distinguishes `SSL_OK` / `SSL_FAIL` / `NO_KEY`. *Confirmed against code.*
2. **[HIGH] 116-01 test↔code drift** — Hebrew `context` redaction expectation should be `unregistered`; add forbidden-*value* assertions over the serialized payload and an allowed-key (`context`) filename-leak regression. *Confirmed against code.*
3. **[MEDIUM] 116-03 ordering** — split "draft docs" from "mark requirements complete," or add `depends_on` so status flips don't precede the code/tests landing; runbook must describe drop counters strictly as queue-saturation counters.
4. **[LOW] 116-03 grep logic** — `grep -qiv "isolated"` doesn't prove absence; use `! grep -qi "isolated" <file>`. Use the repo's Windows-safe `PYTHONIOENCODING=utf-8 python scripts/check_docs.py`.

### Divergent Views
None — single reviewer.
