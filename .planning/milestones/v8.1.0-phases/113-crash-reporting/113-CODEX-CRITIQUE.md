# Codex Critique — Phase 113 Crash Reporting (pre-CONTEXT)

**Date:** 2026-06-15
**Model:** codex exec (gpt-5.x)
**Brief:** `113-CODEX-BRIEF.md`
**Grounded in:** `genizah_app.py:148`, `desktop/telemetry.py` (scrubber/allowlist/track_error), `shared/posthog_server.py` (enqueue/flush), `genizah_core.py:2342` (Config.INDEX_DIR).

Verbatim verdict per decision + missed pitfalls. (Resolutions folded into 113-CONTEXT.md.)

---

**D1 — CONCERN / MEDIUM.** sys.excepthook-only for slot exceptions is plausible but not proven for frozen
PyQt6/Windows without a smoke test. Bigger issue: `threading.excepthook` covers `threading.Thread`, not
necessarily `QThread.run()` failures; many workers already handle errors via `error_signal` (e.g.
`StartupThread`, `genizah_app.py:3428`) and won't hit either hook. Add tests for `QTimer.singleShot` slot,
signal-connected slot, and a real `QThread.run()` exception in BOTH dev and PyInstaller builds. If adding
`QApplication.notify`, install via a QApplication subclass BEFORE `QApplication(sys.argv)`, non-swallowing.

**D2 — CONCERN / MEDIUM.** "Parse and send the first line" is too trusting — fatal lines vary by
platform/locale and may include extension-provided text. Do NOT transmit raw first-line text. Map only known
prefixes to a small fixed enum (`segmentation_fault`, `access_violation`, `abort`, `stack_overflow`,
`unknown_native`); cap and discard everything else.

**D3 — CONCERN / HIGH.** Read+classify the previous dump BEFORE enabling faulthandler (opening with `w` erases
evidence). Keep the file handle in a module global for process lifetime; truncate/rotate only AFTER the
one-shot prior-crash decision. Path claim is wrong: `config.pkl` is under `Config.INDEX_DIR`, which can be
portable / legacy / `%LOCALAPPDATA%\GenizahSearchPro\Index` — use `Config.INDEX_DIR`, not a hardcoded AppData
path (`genizah_core.py:2342`). Consent timing is unresolved: first-run consent is deferred behind
citation/modal handling (`genizah_app.py:3524`, `:15856`) — prior-crash emission needs an explicit "pending
native crash, emit after consent becomes true" path or it will emit too early or never.

**D4 — AGREE / LOW.** Correct scope. Caveat: caught "fatal" UI paths (`QMessageBox.critical` startup failures,
`genizah_app.py:3432`, `:3535`) stay invisible until Phase 114 — acceptable only if stakeholders know 113 is
not "all errors."

**D5 — BLOCKER / HIGH.** Lock-free `_enabled` read is valid (atomic global bool read under the GIL;
`threading.excepthook` runs in the failing thread) — but it removes only ONE lock. The emission path still
takes `_state_lock` in `_emit()` (`telemetry.py:505`) and `_default_distinct_id_lock` / `_scrub_hook_lock` in
`enqueue_event()` (`posthog_server.py:184,198`) plus queue internals. A crash while `set_consent()` holds
`_state_lock`/`_enabled_lock` (`telemetry.py:426`) can still deadlock. Phase 113 needs a DEDICATED crash
enqueue path: lock-free cached consent + distinct_id, no `_emit()`, no public `enqueue_event()` locks, plus a
recursion guard.

**D6 — CONCERN / HIGH.** atexit desktop-side is right and runs on normal `sys.exit(app.exec())`. But it does
not solve crash delivery: the daemon drain thread races the synchronous flush (daemon started before the put,
`posthog_server.py:188`; can dequeue the crash event at `:225` before flush sees it), and `_flush_before_exit`
drains FIFO (`:290`) so a saturated queue spends the 0.5s budget on OLDER events while the crash event is late
or dropped. CRASH-06 ("prioritized over a full queue") is not provided by the current transport — add a
priority/direct crash flush path or pass the crash payload directly to the bounded synchronous sender.

**D7 — AGREE / MEDIUM caveat.** Top-frame-only is right; no full traceback string. Allowlist must change from
the current `exc_module`/`exc_lineno`/`traceback_scrubbed`/`thread_name` (`telemetry.py:259`) to the crash
keys, and a dedicated `_make_crash_props` beats reusing `track_error()`. Use the innermost IN-APP frame, not
blindly the innermost (else fingerprints collapse onto library internals). `error_module` = sanitized basename
from an allowlisted app module root, else `external`; never allow arbitrary user/plugin/temp filenames.

**Missed pitfalls:**
- HIGH — `install_exception_hooks()` is a stub (`telemetry.py:704`) AND not called from `genizah_app.py`. Wire
  it after the existing module-level `_setup_crash_handler()` runs, before risky startup.
- HIGH — queue-full drops NEW events via `put_nowait()` (`posthog_server.py:207`); the crash event needs a
  reserved slot / drop-oldest / direct send / separate priority queue.
- HIGH — avoid `traceback.format_exception` for the payload (it formats messages + full paths in memory). Walk
  frames / extract directly; never touch `str(exc)`.
- MEDIUM — `crash_log.txt` writes adjacent to `__file__` (`genizah_app.py:156`); can fail silently in Program
  Files. Tests must not assume it is writable in frozen installs.
- MEDIUM — idempotent hook registration + dedupe (slot + notify + excepthook can double-report the same
  exception unless keyed by traceback/object id).
- MEDIUM — exclude `KeyboardInterrupt` and probably `SystemExit` in both hooks; chain prior hooks exactly once.
- MEDIUM — PyInstaller must bundle TLS/cert for `requests` or the crash flush silently drops even when consented.
- LOW — native dump is local-only but contains full Python paths/frames; never auto-upload it without a
  separate explicit user action.
