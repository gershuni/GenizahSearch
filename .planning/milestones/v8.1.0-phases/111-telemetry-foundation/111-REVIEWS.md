---
phase: 111
reviewers: [codex]
reviewed_at: {
  timestamp: 2026-06-14T05:16:36.711Z
}
plans_reviewed: [111-01-PLAN.md, 111-02-PLAN.md, 111-03-PLAN.md]
model: gpt-5.5 (codex exec, reasoning effort xhigh)
---

# Cross-AI Plan Review — Phase 111 (Telemetry Foundation)

> Codex pre-flight (plan↔code drift focus). Internal gsd-plan-checker had already
> PASSED (0 blockers); Codex verified the plans' file/line/signature/test claims
> against the live repository.

## Codex Review

## Summary
The plans are mostly grounded in the current repo: the cited `shared/posthog_server.py`, `genizah_core.py`, `version.py`, `web/auth_state.py`, `tests/test_no_raw_storage_access.py`, and `desktop/` package assumptions are largely accurate. The biggest plan-code drift is not in the line numbers; it is architectural: `desktop/telemetry.py` is planned to define/use `GENIZAH_TELEMETRY_KEY`, but the actual shared transport only posts with `POSTHOG_API_KEY`, so the desktop key/host override will not affect emission unless the plan adds missing plumbing.

## Strengths
- `shared/posthog_server.py` claims are mostly correct: `_event_queue = queue.Queue(maxsize=10000)` at [shared/posthog_server.py](C:/Genizahsearch/shared/posthog_server.py:47), `enqueue_event(event, properties, distinct_id='system')` at [line 65](C:/Genizahsearch/shared/posthog_server.py:65), `_drain_posthog_queue` at [line 100](C:/Genizahsearch/shared/posthog_server.py:100), `POSTHOG_API_KEY` guard at [line 106](C:/Genizahsearch/shared/posthog_server.py:106), EU `/capture` POST with `timeout=2.0` at [line 122](C:/Genizahsearch/shared/posthog_server.py:122), `_reset_for_tests` at [line 140](C:/Genizahsearch/shared/posthog_server.py:140), and `__all__` at [line 158](C:/Genizahsearch/shared/posthog_server.py:158).
- `genizah_core.load_app_config()` / `save_app_config(new_data)` exist and match the merge/no-raise contract at [genizah_core.py](C:/Genizahsearch/genizah_core.py:2871) and [line 2882](C:/Genizahsearch/genizah_core.py:2882).
- `version.py` is safe for `from version import APP_VERSION`: it has no imports and defines `APP_VERSION = "8.0.0"` at [version.py](C:/Genizahsearch/version.py:1).
- Web identity contract is accurately cited: web uses `user.get('id')` as the PostHog distinct ID and sends email/name as person properties at [web/auth_state.py](C:/Genizahsearch/web/auth_state.py:159), and calls `posthog.reset()` on logout at [line 216](C:/Genizahsearch/web/auth_state.py:216).
- `desktop/` is a real package and has `__init__.py`, so `desktop/telemetry.py` and `import desktop.telemetry` are valid targets: [desktop/__init__.py](C:/Genizahsearch/desktop/__init__.py:1).
- The AST-guard model in `tests/test_no_raw_storage_access.py` is real: repo root/web dir setup at [line 32](C:/Genizahsearch/tests/test_no_raw_storage_access.py:32), alias detection at [line 44](C:/Genizahsearch/tests/test_no_raw_storage_access.py:44), visitor at [line 93](C:/Genizahsearch/tests/test_no_raw_storage_access.py:93), synthetic violation at [line 235](C:/Genizahsearch/tests/test_no_raw_storage_access.py:235), and production lint loop at [line 356](C:/Genizahsearch/tests/test_no_raw_storage_access.py:356).

## Concerns
- **HIGH**: Desktop telemetry key plumbing is missing. `shared/posthog_server.py` only reads `POSTHOG_API_KEY` inside the drain loop at [line 106](C:/Genizahsearch/shared/posthog_server.py:106) and `_flush_before_exit` is planned to do the same. Plan 02 says `desktop/telemetry.py` will define `_TELEMETRY_KEY = os.environ.get('GENIZAH_TELEMETRY_KEY', ...)`, but `_emit()` only calls `enqueue_event(...)`; it never passes that key to the transport. Result: `GENIZAH_TELEMETRY_KEY` and the placeholder/shared desktop key are inert, self-test will not actually target the desktop key, and desktop emission depends on `POSTHOG_API_KEY` being set in the desktop process. This contradicts SC#1 and D-03. Web key location is at [web/main.py](C:/Genizahsearch/web/main.py:794).
- **HIGH**: New `shared.posthog_server` state is not reset by the existing `_reset_for_tests()` plan. Existing tests expect the default distinct ID to remain `'system'`, e.g. [tests/test_posthog_server.py](C:/Genizahsearch/tests/test_posthog_server.py:53). Plan 02 telemetry tests call `set_consent(True)`, which calls `set_default_distinct_id(...)`, but the Plan 02 fixtures only reset `desktop.telemetry` state and swap `_event_queue`; they do not clear the shared default distinct ID. This can make `tests/test_posthog_server.py` order-dependent and fail after telemetry tests.
- **MEDIUM**: The “5 monkeypatches target `_event_queue`” claim is inaccurate. The shared `_event_queue` monkeypatches are in `tests/test_posthog_server.py` at [line 44](C:/Genizahsearch/tests/test_posthog_server.py:44), [line 105](C:/Genizahsearch/tests/test_posthog_server.py:105), and [line 120](C:/Genizahsearch/tests/test_posthog_server.py:120). `tests/test_nli_circuit_breaker.py` monkeypatches `ph.enqueue_event`, not `_event_queue`, at [line 156](C:/Genizahsearch/tests/test_nli_circuit_breaker.py:156). `tests/test_api_hardening.py` and `tests/test_search_api_v2.py` patch the separate `web.api_hardening._event_queue`, not the shared one.
- **MEDIUM**: `_flush_before_exit(timeout=0.5)` is not actually bounded to 0.5s if it calls `requests.post(..., timeout=2.0)` per event. A single blocked POST can exceed the advertised total timeout. The existing daemon’s 2s timeout is at [shared/posthog_server.py](C:/Genizahsearch/shared/posthog_server.py:122), but using it synchronously in a crash/exit path weakens the safety claim.
- **MEDIUM**: The `$identify` design is mostly aligned with PostHog ingestion behavior, but Plan 02 should prevent generic `track('$identify')`. Plan 02 includes `DesktopEvent.IDENTIFY = '$identify'` and says `track()` accepts any enum value; that means a future caller can emit `$identify` through the generic path without `$anon_distinct_id` or the explicit user distinct ID. Keep `$identify` in the enum for registry purposes, but reject it in `track()` and only allow `identify()` to emit it. PostHog’s ingestion docs describe `$identify` + `$anon_distinct_id` merging behavior, while the capture API docs also warn raw `$identify` differs from JS `identify()`: [PostHog ingestion pipeline](https://posthog.com/docs/how-posthog-works/ingestion-pipeline), [PostHog capture API](https://posthog.com/docs/api/capture).
- **MEDIUM**: The scrubber key rule will drop `context`. Plan 02 allowlists `context`, and `track_error(context, exc)` is supposed to emit it, but `_BANNED_KEYS` includes `text` and uses substring matching. `"context"` contains `"text"`, so `_scrub_props()` will remove it. See Plan 02 scrubber action around [.planning/.../111-02-PLAN.md](C:/Genizahsearch/.planning/phases/111-telemetry-foundation/111-02-PLAN.md:197).
- **LOW**: `Config.CONFIG_FILE` is correctly `config.pkl`, but the plan overstates its path. It is usually LOCALAPPDATA, but portable and legacy paths can override `INDEX_DIR` at [genizah_core.py](C:/Genizahsearch/genizah_core.py:2343), then `CONFIG_FILE` is defined at [line 2377](C:/Genizahsearch/genizah_core.py:2377).
- **LOW**: Plan 03’s production AST skip `if path.name == 'telemetry.py': continue` is too broad. It would exempt a future `desktop/widgets/telemetry.py`. Compare the resolved path to `DESKTOP_DIR / 'telemetry.py'` instead.

## Suggestions
- Add explicit transport key support before Plan 02 lands: either a neutral `shared.posthog_server.set_api_key_provider(...)` / `set_capture_config(...)`, or have `desktop.telemetry` set a desktop-specific key into the transport without mutating global `POSTHOG_API_KEY`. Keep web behavior unchanged.
- Extend `shared.posthog_server._reset_for_tests()` to clear `_default_distinct_id` and `_scrub_hook`, or make every telemetry fixture call `ph.set_default_distinct_id(None)` and `ph.register_scrub_hook(None)` before/after. The first option is more robust.
- Make `_flush_before_exit` enforce a real deadline: compute remaining time before each POST and use `timeout=min(remaining, 0.2 or 0.5)`, or stop before posting if remaining time is exhausted.
- In `track()`, explicitly reject `DesktopEvent.IDENTIFY` / `'$identify'`; only `identify()` should emit the protocol event.
- Change banned-key matching from broad substring matching to exact keys plus explicit suffix/prefix patterns, or remove `text` as a substring token. Add a test that `context` survives when it contains a safe developer constant.
- Tighten Plan 03’s skip to `if path.resolve() == (DESKTOP_DIR / 'telemetry.py').resolve(): continue`, and add a synthetic test for `from shared import posthog_server as ph`.

## Risk Assessment
**MEDIUM** overall. The repository-shape assumptions are mostly correct, so the plans are executable, but the key/host plumbing gap is a real delivery blocker for “desktop events target the shared web project via `GENIZAH_TELEMETRY_KEY`.” The test-isolation gap and `$identify` generic-track loophole are also likely to cause either test flakiness or future privacy/identity mistakes unless fixed before implementation.

---

## Consensus Summary (single reviewer: Codex)

### Agreed Strengths
- Plan↔code references are accurate: `shared/posthog_server.py` symbols + line numbers, `genizah_core.load_app_config/save_app_config` (no-raise merge contract), `version.py` (`APP_VERSION="8.0.0"`, zero imports), the web identity contract (`web/auth_state.py`), and the `tests/test_no_raw_storage_access.py` AST-guard model all verified against the repo.
- `desktop/` is a real package with `__init__.py` — `import desktop.telemetry` and the `DESKTOP_DIR` AST scan are valid.

### Agreed Concerns (action items for --reviews replan)
- **HIGH — transport key plumbing missing.** `desktop/telemetry.py` defines `GENIZAH_TELEMETRY_KEY`/placeholder key but never passes it to the transport; `shared/posthog_server.py` only reads `POSTHOG_API_KEY` from env in its drain loop / `_flush_before_exit`. As planned, desktop emission silently depends on `POSTHOG_API_KEY` being set in the desktop process — the desktop key + the D-06 self-test would be inert. Contradicts SC#1 + D-03. Needs a neutral transport-key/config setter on `shared/posthog_server.py` (no global `POSTHOG_API_KEY` mutation, web behavior unchanged).
- **HIGH — shared module test-state leak.** New `_default_distinct_id` / `_scrub_hook` state is not cleared by `shared.posthog_server._reset_for_tests()`. Plan 02 telemetry tests call `set_consent(True)` → `set_default_distinct_id(...)`; if only `desktop.telemetry` state + `_event_queue` are reset, `tests/test_posthog_server.py` (which expects default `distinct_id='system'`) becomes order-dependent and can fail after telemetry tests. Fix: extend `_reset_for_tests()` to clear both new globals (more robust than per-fixture resets).
- **MEDIUM — scrubber drops the allowlisted `context` key.** `_BANNED_KEYS` includes `text` with substring matching; `"context"` contains `"text"`, so `_scrub_props()` strips the very `context` field `track_error(context, exc)` is meant to emit. Switch to exact-key / suffix matching (or drop the `text` substring token); add a regression test that `context` survives.
- **MEDIUM — `$identify` generic-track loophole.** `track()` accepts any `DesktopEvent` value, and `DesktopEvent.IDENTIFY='$identify'` is a member — a future caller could emit `$identify` via the generic path without `$anon_distinct_id`/proper distinct_id. Reject `$identify` inside `track()`; only `identify()` may emit it.
- **MEDIUM — `_flush_before_exit(timeout=0.5)` not truly bounded.** Per-event `requests.post(timeout=2.0)` means one blocked POST exceeds the advertised total at crash/exit time. Enforce a real deadline (compute remaining time per POST; stop when exhausted).
- **MEDIUM — interface note inaccurate.** The "5 monkeypatches target `_event_queue`" claim is wrong: `test_posthog_server.py` patches `_event_queue` (:44/:105/:120); `test_nli_circuit_breaker.py` patches `ph.enqueue_event` (not `_event_queue`); `test_api_hardening.py`/`test_search_api_v2.py` patch the *separate* `web.api_hardening._event_queue`. Backward-compat still holds, but the plan's claim should be corrected so the executor verifies the right tests.
- **LOW — Plan 03 chokepoint skip too broad.** `if path.name == 'telemetry.py': continue` would also exempt a future `desktop/widgets/telemetry.py`. Compare the resolved path to `DESKTOP_DIR / 'telemetry.py'`; add a synthetic `from shared import posthog_server as ph` test.
- **LOW — `Config.CONFIG_FILE` path overstated.** Usually `LOCALAPPDATA/.../config.pkl`, but portable/legacy paths can override `INDEX_DIR` (`genizah_core.py:2343`, `CONFIG_FILE` at :2377).

### Divergent Views
None — single reviewer.

### Overall Risk: MEDIUM
Plans are executable (repo-shape assumptions hold), but the HIGH key-plumbing gap is a real delivery blocker for "desktop events reach the shared project," and the test-isolation + `$identify` loophole + `context`-scrub bug should be fixed before execution.
