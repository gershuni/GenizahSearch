---
phase: 113
reviewers: [codex]
reviewed_at: 2026-06-15T08:11:03Z
plans_reviewed: [113-01-PLAN.md, 113-02-PLAN.md, 113-03-PLAN.md]
---

# Cross-AI Plan Review — Phase 113

> Single independent reviewer (Codex, default model). Running inside Claude Code, so the
> claude CLI was skipped for independence. Codex was instructed to read the live repo and
> hunt plan↔code drift the internal structural checker cannot see. It did — see below.

## Codex Review

**Summary**

The plans are well-structured and mostly match the live code locations, but I would not execute them unchanged. The main blockers are in the crash-path invariants: the proposed “direct” sender still acquires `_capture_config_lock`, the proposed lazy import inside the crash hook can take Python’s import lock, persisted-consent launches do not populate the new `_crash_distinct_id` snapshot, and Plan 03 omits the required crash-hook `_flush_before_exit(0.5)` call. Those are correctness issues against D-05/D-06/CRASH-06, not just test polish.

**Strengths**

- The cited core line ranges are largely accurate: `_setup_crash_handler()` is at [genizah_app.py](C:/Genizahsearch/genizah_app.py:149), `_ALLOWED_PROPS` at [desktop/telemetry.py](C:/Genizahsearch/desktop/telemetry.py:247), `_emit()` lock use at [desktop/telemetry.py](C:/Genizahsearch/desktop/telemetry.py:505), `enqueue_event()` at [shared/posthog_server.py](C:/Genizahsearch/shared/posthog_server.py:160), and `_flush_before_exit()` at [shared/posthog_server.py](C:/Genizahsearch/shared/posthog_server.py:272).
- The wave order is sensible: transport helper, then lock-free payload/emission primitives, then hook/faulthandler wiring.
- D-07 privacy posture is directionally right: no `traceback.format_exception`, no `str(exc)`, no message string, no raw faulthandler text.
- Wiring `install_exception_hooks()` immediately after `_setup_crash_handler()` is the right startup point; `_setup_crash_handler()` is called at [genizah_app.py](C:/Genizahsearch/genizah_app.py:170), before `QApplication(sys.argv)` at [genizah_app.py](C:/Genizahsearch/genizah_app.py:26860).

**Concerns**

- **HIGH: D-05 lock-free invariant is violated by the proposed direct sender.** `send_crash_event_direct()` is specified to call `_resolve_api_key()` and `_resolve_capture_url()`, but both take `_capture_config_lock` at [shared/posthog_server.py](C:/Genizahsearch/shared/posthog_server.py:145) and [shared/posthog_server.py](C:/Genizahsearch/shared/posthog_server.py:152). That means `_emit_crash_direct()` is not lock-free end-to-end.

- **HIGH: Lazy import inside the crash hook can acquire the import lock.** Plan 02/03 use `from shared.posthog_server import send_crash_event_direct` inside `_emit_crash_direct()` / `_emit_native_crash()`. `desktop/telemetry.py` already imports `shared.posthog_server` at module import time at [desktop/telemetry.py](C:/Genizahsearch/desktop/telemetry.py:43), so the direct sender should be imported there too. Importing inside a failing-thread hook is not lock-free.

- **HIGH: `_crash_distinct_id` is not populated on persisted-consent startup.** `_load_consent_state()` loads `_current_distinct_id` and calls `set_default_distinct_id(distinct_id)` when consent is already true at [desktop/telemetry.py](C:/Genizahsearch/desktop/telemetry.py:342), but Plan 02 only mirrors `_crash_distinct_id` in `set_consent()` and `_set_current_distinct_id()`. Since direct send bypasses transport default-id substitution, existing opted-in users’ crash events can go out as `'system'`.

- **HIGH: Plan 03 does not call `_flush_before_exit(0.5)` inside the exception hook.** The roadmap/SC#5 and D-06 say the crash path should direct-send the crash and still call `_flush_before_exit(0.5)` for queued events. Plan 03 registers only the clean-exit atexit flush. If you add the flush, note that `_flush_before_exit()` itself uses queue internals and `_resolve_api_key()` at [shared/posthog_server.py](C:/Genizahsearch/shared/posthog_server.py:272), so the plan must explicitly reconcile this with D-05.

- **MEDIUM: The proposed global autouse fixture in `tests/conftest.py` is too broad.** Existing `tests/conftest.py` already has project-wide autouse fixtures, e.g. [tests/conftest.py](C:/Genizahsearch/tests/conftest.py:221). Adding an autouse telemetry fixture that monkeypatches `genizah_core.load_app_config/save_app_config` for every test can break unrelated config tests and hide integration behavior.

- **MEDIUM: Plan 03 uses `qtbot`, but this repo appears pytest-qt-free.** Existing Qt tests explicitly use the “pytest-qt-FREE” pattern at [tests/test_join_workbench_construct.py](C:/Genizahsearch/tests/test_join_workbench_construct.py:15), and `pyproject.toml` does not declare pytest-qt. `test_qtimer_slot_raise_reaches_excepthook(qtbot, ...)` will likely fail collection.

- **MEDIUM: `threading.excepthook` chaining skips any already-installed hook.** Plan 03 captures `threading.__excepthook__`, not the current `threading.excepthook`. That preserves the default, but it does not wrap a prior non-default hook if one is installed before telemetry. D-08 says chain prior hooks exactly once.

- **MEDIUM: `_reset_for_tests()` plan can leave process hooks polluted.** Existing `_reset_for_tests()` resets only telemetry state at [desktop/telemetry.py](C:/Genizahsearch/desktop/telemetry.py:738). If Plan 02 adds `_hooks_installed = False` without restoring `sys.excepthook` and `threading.excepthook`, later tests can double-wrap hooks and register duplicate atexit handlers.

- **MEDIUM: `_IN_APP_ROOTS` by basename is fragile.** Enumerating every basename under `desktop/` and `shared/` includes generic files like `__init__.py`, so external frames can be misclassified as in-app. The transmitted value is still only a basename, so this is more fingerprint quality than PII, but it weakens D-07.

**Suggestions**

- Add lock-free capture snapshots in `shared/posthog_server.py`, updated by `set_capture_api_key()` / `set_capture_host()`, and have `send_crash_event_direct()` read only those plain globals. Do not call `_resolve_api_key()` / `_resolve_capture_url()` from the crash path.
- Import `send_crash_event_direct` at `desktop/telemetry.py` module import time, alongside `enqueue_event`, instead of lazy-importing it inside hook functions.
- Update `_load_consent_state()` to set `_crash_distinct_id = distinct_id` whenever persisted consent is true and a distinct id exists.
- Explicitly decide how to satisfy SC#5 without violating D-05. Either document that direct-send supersedes hook-time FIFO flush, or implement a separate bounded flush path with pre-snapshotted key/url and clear lock assumptions.
- Keep telemetry reset fixtures local to the new crash test modules, or make a non-autouse fixture that crash tests opt into.
- Replace `qtbot` with the repo’s existing `QApplication.instance() or QApplication(sys.argv)` plus `QEventLoop`/`QTimer` pattern.
- For in-app frame detection, classify using resolved source roots internally, then transmit only `os.path.basename(...)`. Exclude generic basenames like `__init__.py`.
- Make `_reset_for_tests()` restore any installed hooks and avoid repeated atexit registration in tests.

**Risk Assessment**

Overall risk: **HIGH** until the lock-free/direct-send and distinct-id startup gaps are fixed. The privacy payload design is mostly sound, but the current plans can deadlock in the crash path via `_capture_config_lock`/import lock, lose identity alignment for already-consented users, and miss an explicit CRASH-06 success criterion. Once those are corrected, the remaining risks are mostly test isolation and Qt verification details.

---

## Consensus Summary

Only one external reviewer (Codex) ran, so "consensus" = Codex's findings, triaged here.
Codex independently confirmed the cited core line numbers are accurate (no location drift),
then found four HIGH correctness issues against the locked decisions by reading the actual
`shared/posthog_server.py` / `desktop/telemetry.py` — issues that are invisible to a
plan-internal structural checker.

### Agreed Strengths
- Cited line ranges are accurate (`_setup_crash_handler` :149, `_ALLOWED_PROPS` :247, `_emit` :505, `enqueue_event` :160, `_flush_before_exit` :272).
- Wave order (transport → lock-free primitives → hook/faulthandler wiring) is sound.
- D-07 privacy posture is directionally right (no `format_exception`, no `str(exc)`, no message, no raw faulthandler text).
- `install_exception_hooks()` startup point (after `_setup_crash_handler()` at :170, before `QApplication(sys.argv)` at :26860) is correct.

### Agreed Concerns (priority order)

**HIGH — must fix before execute (correctness against D-05/D-06/CRASH-06):**
1. **D-05 lock-free VIOLATED by the direct sender.** `send_crash_event_direct()` calls `_resolve_api_key()`/`_resolve_capture_url()`, both of which take `_capture_config_lock` (`posthog_server.py:145,:152`). The "lock-free" crash path therefore acquires a lock. Fix: lock-free capture snapshots (api_key + url) as plain globals, written on set, read without a lock in the crash path.
2. **Lazy import inside the hook can take the import lock.** `from shared.posthog_server import send_crash_event_direct` inside `_emit_crash_direct`/`_emit_native_crash` is not lock-free; `telemetry.py` already imports `shared.posthog_server` at module load (`:43`). Fix: import `send_crash_event_direct` at module top, alongside `enqueue_event`.
3. **`_crash_distinct_id` not populated on persisted-consent startup.** `_load_consent_state()` calls `set_default_distinct_id` when consent is already true (`telemetry.py:342`), but the plans only mirror `_crash_distinct_id` in `set_consent()`/`_set_current_distinct_id()`. Already-opted-in users' crash events would go out as `'system'`. Fix: set `_crash_distinct_id` in `_load_consent_state()` too.
4. **Plan 03 omits the hook-time `_flush_before_exit(0.5)` (SC#5/CRASH-06).** Only the clean-exit atexit flush is wired. And if added, `_flush_before_exit` itself calls `_resolve_api_key()` (lock) — so the plan must explicitly reconcile SC#5 with D-05 (either document direct-send supersedes hook-time FIFO flush, or build a bounded flush with pre-snapshotted key/url).

**MEDIUM — should fix:**
5. Proposed global autouse fixture in `tests/conftest.py` is too broad (existing project-wide autouse at `conftest.py:221`); keep crash-test fixtures local / opt-in.
6. Plan 03 uses `qtbot`, but the repo is pytest-qt-FREE (`test_join_workbench_construct.py:15`; `pyproject.toml` has no pytest-qt). Use the repo's `QApplication.instance() or QApplication(sys.argv)` + `QEventLoop`/`QTimer` pattern.
7. `threading.excepthook` chaining captures `threading.__excepthook__`, not the current `threading.excepthook` — skips an already-installed non-default prior hook (D-08 says chain prior exactly once).
8. `_reset_for_tests()` must restore `sys.excepthook`/`threading.excepthook` and avoid repeated `atexit` registration, or tests double-wrap hooks.
9. `_IN_APP_ROOTS` by basename is fragile (includes generic `__init__.py`); classify by resolved source root internally, transmit only the basename, exclude generic names.

### Divergent Views
None — single reviewer.

### Overall Risk (Codex): HIGH
Until the lock-free/direct-send (#1, #2, #4) and distinct-id-on-startup (#3) gaps are fixed. Privacy payload design is mostly sound; remaining items are test isolation + Qt verification details.
