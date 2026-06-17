# Codex Critique Brief — Phase 113: Crash Reporting (GenizahSearch desktop telemetry)

You are a skeptical senior reviewer. Critique the **implementation decisions** below for a privacy-first,
opt-in crash-reporting phase in a **PyQt6 frozen-binary (PyInstaller) Windows desktop app**. We have NOT
written code yet — this is the pre-planning decision brief. Find correctness bugs, privacy leaks, missed
pitfalls, and frozen-binary gotchas. Be specific. Rank findings HIGH/MEDIUM/LOW. If a decision is sound,
say so briefly; spend your effort on what could break or leak.

## Project / privacy posture
- v8.1.0 "Desktop Telemetry": opt-in PostHog telemetry for the desktop app. Default OFF until consent.
- Reuses the existing shared web PostHog project; transport is `shared/posthog_server.py` (fire-and-forget
  `queue.Queue(maxsize=10000)` + a `daemon=True` drain thread). **No `posthog` SDK** (PII risk).
- Hard rule: NEVER transmit search/query content, My Library file paths/filenames, frame locals, or
  exception message strings. Identity, when logged in, is the bare Supabase user.id; else anonymous uuid4.

## Already built (Phases 111/112) — do NOT redesign, just rely on:
- `desktop/telemetry.py` chokepoint: the ONLY `desktop/` path to `enqueue_event` (enforced by an AST guard).
- `_scrub_props()` (strips banned keys, redacts path-like strings, drops frame locals) + a static property
  **allowlist** (rejects any prop not on the list) + a fixed `DesktopEvent` enum (no dynamic event names).
  Enum already includes `CRASH='desktop_crash'` and `PRIOR_CRASH='desktop_prior_crash'`.
- `shared/posthog_server._flush_before_exit(timeout=0.5)`: bounded synchronous drain+POST that bypasses the
  dying daemon thread, with a true wall-clock deadline. Already implemented + tested.
- `_drain_and_discard()`: purge queue on opt-out.
- `is_enabled()`: returns cached `_enabled` bool but **acquires a non-reentrant `threading.Lock` (`_enabled_lock`)**;
  try/except-guarded, no disk read.
- A thin `track_error(context, exc)` that emits `DesktopEvent.CRASH` with `{context, exc_type}` ONLY
  (no message string). It is currently producer-less.
- Existing `_setup_crash_handler()` (`genizah_app.py:148-170`): installs `sys.excepthook` at module import,
  writes full traceback to `crash_log.txt` (adjacent to `__file__`, `except OSError: pass`), prints to stderr,
  then chains to `sys.__excepthook__`. `threading.excepthook` is NOT currently installed.

## Phase 113 locked requirements (CRASH-01..07) and success criteria
- CRASH-01: capture unhandled main-thread exceptions via `sys.excepthook`, **chaining to (never replacing)**
  the existing handler so `crash_log.txt` keeps working; telemetry in `try/finally` so a telemetry failure
  can't suppress the existing crash-log handler.
- CRASH-02: capture worker/QThread exceptions via `threading.excepthook`; Qt-slot exceptions "via a
  `QApplication.notify` override." (SEE DECISION 1 — we are amending this.)
- CRASH-03: native crashes (Tantivy/PyMuPDF) captured to a **local file via faulthandler — NOT transmitted**.
- CRASH-04: crash events contain only exception type name, a scrubbed/sanitized stack location, app version,
  OS — never frame locals, message strings, file paths, filenames, or query text.
- CRASH-05: hooks non-blocking (enqueue only, no network I/O), re-entrancy-safe, consent gate uses a
  **cached** value (no disk read / settings init inside the hook) so the gate cannot throw during crash handling.
- CRASH-06: final crash event delivered via **bounded synchronous flush** before exit, prioritized over a full queue.
- CRASH-07: a native crash that can't emit at crash time is detected on the **next launch** and emitted once
  (after consent).
- SC#4 (ROADMAP): the hook body executes only `traceback.format_exception` + scrub + `put_nowait` —
  **"no network I/O, no disk I/O, no lock acquisition"** — wrapped in `try/finally`.
- KeyboardInterrupt excluded from both hooks.

## DECISIONS MADE IN DISCUSSION (critique these)

**D1 — Qt-slot capture (CRASH-02 conflict).** PyQt6 routes exceptions escaping a slot to `sys.excepthook`
(then aborts) since PyQt 5.5. So a wrapped `sys.excepthook` already covers slot exceptions. Decision:
default to **`sys.excepthook`-only** (+ `threading.excepthook`); amend CRASH-02 to "satisfied by sys.excepthook";
add a test that a slot exception reaches the hook. Planner may add a **non-swallowing, deduped**
`QApplication.notify` override ONLY IF a spike proves slot exceptions escape `sys.excepthook` in our frozen
PyQt6 build. Is sys.excepthook-only actually reliable for slot exceptions in PyQt6 6.x frozen on Windows?

**D2 — Native crash event content.** On next-launch detection, transmit `desktop_prior_crash` with base props
(app_version, OS) + a **path-free fatal-error label parsed only from faulthandler's FIRST line** (e.g.
"Segmentation fault", "Aborted"). NO frames, NO paths, NO module/line. Is the first line reliably path-free
across platforms/locales? Any way PII sneaks into that label?

**D3 — Native dump file + lifecycle.** `faulthandler.enable(file=handle, all_threads=True)` at startup, dump
file in the **config.pkl dir** (`%LOCALAPPDATA%\GenizahSearchPro\`, reliably writable, unlike the install dir
under Program Files). Next launch: if dump non-empty AND consent enabled → emit `desktop_prior_crash` once →
**truncate**. If not consented, leave it (pending or overwritten by next crash). Concerns: persistent open
file handle for whole process lifetime in a frozen exe; distinguishing "previous-run crash" from "this run's
faulthandler just opened the file"; the not-yet-consented edge case; first-run-dialog consent timing
(Phase 112 shows the consent dialog AFTER the window paints via QTimer.singleShot(0), and it must not stack on
the interrupted-indexing recovery modal) — so when exactly is consent "known" for the prior-crash emit?

**D4 — Handled-error wiring DEFERRED.** 113 = unhandled crash hooks + native only (the 7 SCs). `track_error()`
stays producer-less until Phase 114/follow-up. Agree this is correct scoping, or is there a CRASH-xx that
implicitly needs handled-error wiring?

**D5 — Lock-free consent read in the hook.** SC#4 says "no lock acquisition," but `is_enabled()` acquires
`_enabled_lock`. If the crashing thread already holds that lock (crash inside `set_consent`), the hook would
deadlock on the non-reentrant Lock. Decision: inside the hook read the module global `_enabled` **directly,
lock-free** (atomic bool read under the GIL) via a `_is_enabled_nolock()` helper. Is the GIL-atomicity
assumption valid here (CPython, plain `global` bool read)? Any tearing/visibility risk? Does
`threading.excepthook` run on the worker thread (so the same lock-holder concern applies cross-thread)?

**D6 — atexit registration.** Register the `atexit` handler inside `install_exception_hooks()` (desktop-side,
called once at startup) — NOT in `shared/posthog_server.py` (ungated; the long-lived web process must not
exit-flush). Clean-exit flush timeout ~1–2s (crash path stays 0.5s per SC#5). Handler no-ops when queue
empty/opted-out. Does `atexit` reliably run in a PyInstaller frozen Windows exe on normal QApplication quit?
Interaction with the daemon drain thread?

**D7 — Python crash payload = top-frame-only (resolves a research inconsistency).** ARCHITECTURE.md proposed a
scrubbed FULL traceback string (`traceback_scrubbed`); FEATURES.md proposed top-frame-only
(`error_type`/`error_module` basename/`error_line`/`error_fingerprint`/`is_background_thread`, NO full
traceback). We lock **top-frame-only, no traceback string** (matches SC#3 "a scrubbed stack location",
minimizes leak surface). The Phase 111 **property allowlist must gain** `error_module`, `error_line`,
`error_fingerprint`, `is_background_thread`, and the native `fatal_error` label key. The crash hook therefore
needs a dedicated payload builder (`_capture_crash` / `_make_crash_props`) rather than reusing the thin
`track_error`. Agree? Any risk in `error_module`/`error_fingerprint` (basename of innermost frame) — could a
basename ever be PII (e.g. a user plugin path)? Is innermost-frame the right frame to fingerprint, or should
it be the innermost in-app frame?

## What to return
For each decision D1–D7: AGREE / CONCERN / BLOCKER + one-paragraph reasoning. Then a short list of
**missed pitfalls** (anything not covered above that will bite us in a frozen PyQt6/Windows crash path or that
risks a privacy leak). Concrete > generic.
