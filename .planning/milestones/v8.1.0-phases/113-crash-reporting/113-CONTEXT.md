# Phase 113: Crash Reporting - Context

**Gathered:** 2026-06-15
**Status:** Ready for planning
**Second opinion:** Codex critique run pre-CONTEXT (`113-CODEX-CRITIQUE.md`) — 1 BLOCKER + 4 HIGH folded below.

<domain>
## Phase Boundary

Wire the **crash producers** on top of the Phase 111 engine + Phase 112 consent surface: install chained
exception hooks (`sys.excepthook` + `threading.excepthook`) that capture uncaught Python exceptions on any
thread, a `faulthandler`-based local capture for native C-extension crashes (Tantivy/PyMuPDF), a **bounded,
priority synchronous flush** so the crash event survives process exit, and **next-launch detection** that
re-emits a native-crash signal once after consent. The hook **wraps, never replaces** the existing
`_setup_crash_handler()` so `crash_log.txt` keeps working. This is the first phase that actually emits events.

Requirements (from ROADMAP/REQUIREMENTS): **CRASH-01, CRASH-02, CRASH-03, CRASH-04, CRASH-05, CRASH-06, CRASH-07.**

**Locked by Success Criteria (not open for discussion):**
- Hook **chains** to the existing handler (`crash_log.txt` + stderr + `sys.__excepthook__` still run); telemetry
  step in `try/finally` so a telemetry failure can never suppress the existing crash log (SC#1, CRASH-01).
- `threading.excepthook` installed for worker threads; `KeyboardInterrupt` excluded (SC#2). **SystemExit also
  excluded** (Codex pitfall).
- Crash payload = exception type + scrubbed stack **location** + app version + OS only — **no frame locals, no
  exception message string, no paths, no query text** (SC#3, CRASH-04), enforced by the Phase 111 scrubber +
  allowlist.
- Hook body non-blocking — no network I/O, no disk I/O, no lock acquisition — fully `try/finally`-wrapped (SC#4).
- Bounded synchronous flush delivers the crash event before exit + `atexit` for clean exits (SC#5, CRASH-06).
- Consent gate uses a **cached** value, no disk read inside the hook (CRASH-05).

**Already built in Phase 111 (reuse, do NOT re-implement):** `_scrub_props`/`_scrub_value` (paths + Hebrew +
length cap + banned keys), the `_ALLOWED_PROPS` allowlist, the fixed `DesktopEvent` enum
(`CRASH='desktop_crash'`, `PRIOR_CRASH='desktop_prior_crash'`), `shared/posthog_server._flush_before_exit(0.5)`
(bounded, deadline-respecting), `_drain_and_discard`, the cached `_enabled` consent state. **`track_error()`
exists but is thin (`{context, exc_type}` only) — the crash hook does NOT reuse it (see D-07/D-05).**

**Out of phase (later):** handled/non-fatal `track_error()` site wiring (D-04 — deferred), usage events (114),
perf events (115), the CI privacy gate + frozen-binary SSL self-test + runbook (116).
</domain>

<decisions>
## Implementation Decisions

### D-01 — Qt-slot capture (resolves CRASH-02 conflict) — "You decide" + Codex MEDIUM
- **Default: `sys.excepthook`-only** (+ `threading.excepthook`). PyQt6 routes exceptions escaping a slot to
  `sys.excepthook` (then aborts) since PyQt 5.5, so a wrapped `sys.excepthook` already covers slot exceptions.
  **Amend CRASH-02** so its `QApplication.notify` clause reads "satisfied by `sys.excepthook`."
- **Do NOT add a `QApplication.notify` override by default.** Its only distinct power is *swallowing* a slot
  exception (changes crash-abort semantics, risks masking real crashes). The planner may add one **only if** a
  spike proves slot exceptions escape `sys.excepthook` in our **frozen PyQt6/Windows** build — and then it MUST
  be: installed via a `QApplication` **subclass before `QApplication(sys.argv)`** (not monkey-patched later),
  **non-swallowing** (re-raise / let abort proceed), and **deduped** against the excepthook path.
- **Worker-thread nuance (Codex):** `threading.excepthook` covers `threading.Thread`; `QThread.run()` coverage
  is not guaranteed, and most workers (`SearchThread`, `LocalIndexerWorker`, `FolderWalkWorker`, `StartupThread`)
  already catch + emit `error_signal`, so the hooks are a **backstop** for the un-caught minority — not the
  primary path. **Required test matrix** (dev AND frozen): a `QTimer.singleShot` slot raise, a signal-connected
  slot raise, and a real `QThread.run()` raise — assert the hook fires (or document the gap).

### D-02 — Native-crash event content — fixed enum, never raw text (Codex MEDIUM)
- On next-launch detection, transmit `desktop_prior_crash` with base props (`app_version`, `os_*`) + a
  **path-free fatal-error label drawn from a FIXED ENUM** (`segmentation_fault`, `access_violation`, `abort`,
  `stack_overflow`, `unknown_native`). The label is produced by **mapping known faulthandler first-line
  prefixes** to the enum — **never by transmitting the raw parsed text** (fatal lines vary by platform/locale
  and may carry C-extension-supplied text). Anything unrecognized → `unknown_native`.
- NO frames, NO paths, NO module/line in the native event. The raw dump stays **local only, never transmitted**
  (CRASH-03). Add the label key (e.g. `native_signal` / `fatal_error`) to the `_ALLOWED_PROPS` allowlist.

### D-03 — Native dump file + lifecycle (Codex HIGH — path + ordering corrections)
- Use **`Config.INDEX_DIR`** for the dump file — **NOT** a hardcoded `%LOCALAPPDATA%\GenizahSearchPro\` path.
  `Config.INDEX_DIR` already resolves the portable / legacy / `…\GenizahSearchPro\Index` cases and is the same
  reliably-writable dir as `config.pkl` (`genizah_core.py:2342-2381`). (Distinct from the existing
  `crash_log.txt`, which writes adjacent to `__file__` and can fail under Program Files.)
- **Read + classify the previous dump BEFORE calling `faulthandler.enable()`** — opening the file for write
  first would erase last run's evidence. This also cleanly distinguishes "previous run crashed" from "this run
  just opened the handle."
- `faulthandler.enable(file=handle, all_threads=True)`; keep the handle in a **module global for the whole
  process lifetime**. Truncate/rotate the dump **only after** the one-shot prior-crash decision is made.
- **Pending-emit path (consent timing):** Phase 112's first-run consent is **deferred** behind the
  citation/recovery/sync modals (`genizah_app.py:3524`, `:15856`), so consent may not be known when startup
  runs. If a prior native crash is detected but consent is not yet `True`, **hold it as pending** and emit once
  iff/when consent becomes `True` (CRASH-07 "after consent"). If the user never consents, never emit; the dump
  is overwritten by the next crash. Emit **exactly once** per detected crash.

### D-04 — Handled/non-fatal `track_error()` wiring — DEFERRED (Codex AGREE)
- Phase 113 = **unhandled crash hooks + native crash only** (exactly the 7 CRASH success criteria). The
  `track_error()` API stays **producer-less** until Phase 114 / a small follow-up. Nothing is lost — it's
  pure wiring later.
- **Known caveat (stakeholder-aware):** caught "fatal" UI paths (e.g. `QMessageBox.critical` startup failures
  at `genizah_app.py:3432`, `:3535`) remain invisible until that wiring lands. 113 is not "all errors."

### D-05 — Crash emission path must be LOCK-FREE end-to-end (Codex BLOCKER — HIGH)
- Reading `_enabled` lock-free inside the hook (atomic global bool read under the GIL; `threading.excepthook`
  runs on the failing thread) is **necessary but NOT sufficient.** The normal path still acquires locks the
  hook must avoid: `_emit()` takes `_state_lock` (`telemetry.py:505`); `enqueue_event()` takes
  `_default_distinct_id_lock` + `_scrub_hook_lock` (`posthog_server.py:184,198`); plus queue internals. A crash
  while `set_consent()`/identity code holds `_state_lock`/`_enabled_lock` (`telemetry.py:426`) would deadlock —
  violating SC#4's "no lock acquisition."
- **Decision:** build a **dedicated crash emission path** that:
  1. reads consent via a lock-free `_is_enabled_nolock()` (direct `_enabled` global read);
  2. reads `distinct_id` from a **lock-free snapshot global** (a plain module global mirror of the current
     distinct_id, written whenever consent/identity changes, read without a lock in the hook);
  3. builds the scrubbed payload and delivers it **without** going through `_emit()` or the public
     `enqueue_event()` lock-takers;
  4. is guarded by a **re-entrancy / recursion flag** (crash inside the crash handler must not loop).

### D-06 — Crash delivery must be PRIORITY/direct, not plain enqueue+FIFO-flush (Codex HIGH)
- Plain `enqueue_event` + `_flush_before_exit` **races the daemon drain thread** (which may dequeue the crash
  event and start a slow POST) and `_flush_before_exit` drains **FIFO** — a saturated/stale queue can spend the
  whole 0.5s budget on OLDER events, leaving the crash event late or dropped. CRASH-06's "prioritized over a
  full queue" is **not** provided by the current transport.
- **Decision:** the crash event gets **priority/direct delivery** — either a reserved priority slot or (simpler)
  the crash path hands the payload **directly to the bounded synchronous sender** (a direct POST of the crash
  event, deadline-bounded), bypassing the FIFO queue, then still calls `_flush_before_exit(0.5)` for anything
  else. Requires a **neutral, backward-compatible** addition to `shared/posthog_server.py` (consistent with
  Phase 111 D-04 — no behavior change for web/breaker callers, no break to the 5 `_event_queue` monkeypatches).
  Also note queue-full currently drops the **NEW** event (`put_nowait`, `posthog_server.py:207`) — the crash
  event must not be the one dropped.

### D-07 — Payload = top-frame-only, frame-walked, allowlist reconciled (Codex AGREE + MEDIUM caveat)
- **Top-frame-only, NO full traceback string** (resolves the ARCHITECTURE.md `traceback_scrubbed` vs FEATURES.md
  divergence in favor of SC#3 "a scrubbed stack location"). A **dedicated `_make_crash_props` / `_capture_crash`
  builder** — NOT the thin `track_error()`.
- **Frame-walk, do not format:** extract the location by walking the traceback frames
  (`co_filename` basename + `tb_lineno`) — **do NOT call `traceback.format_exception`** (it materializes the
  message + full paths in memory) and **never touch `str(exc)`**. This is a strictly-safer reading of SC#4's
  "`traceback.format_exception` + scrub" wording; same intent, smaller leak surface.
- **Innermost IN-APP frame**, not blindly the innermost frame (else fingerprints collapse onto library
  internals). `error_module` = sanitized basename from an **allowlisted app module root**, else `external`;
  never let arbitrary user/plugin/temp filenames into the payload.
- **Allowlist reconciliation** (`_ALLOWED_PROPS`, `telemetry.py:247-266`): keep `exc_type`/`exc_module`/
  `exc_lineno`; **ADD** `error_fingerprint` + a **boolean** `is_background_thread` (prefer the bool over the
  free-ish `thread_name`) + the native `fatal_error` label key; **REMOVE `traceback_scrubbed`** from the
  allowlist (defense-in-depth — no full traceback string may ever leave the chokepoint). Planner finalizes
  exact key names; the *removal* of `traceback_scrubbed` and the *no-full-traceback* rule are locked.

### D-08 — Hook installation wiring + exit flush (Codex HIGH pitfalls)
- `install_exception_hooks()` is currently a **stub** (`telemetry.py:704`) **and is never called** from
  `genizah_app.py`. Wire it **after** the existing module-level `_setup_crash_handler()` has run (so `_prior_hook
  = sys.excepthook` captures the crash-log writer), and **before** risky startup work.
- **Idempotent registration + dedup:** guard against double-install and double-report (slot/`notify`/excepthook
  paths) by keying on the traceback / exception object id; **chain prior hooks exactly once.**
- **`atexit`** registered **inside `install_exception_hooks()`** (desktop-side — NOT in the ungated, long-lived
  `shared/posthog_server.py` that the web process shares). Clean-exit flush timeout **~1–2s** (crash path stays
  **0.5s** per SC#5); no-op when the queue is empty / opted-out.

### Claude's Discretion (within the locked decisions above)
- Exact new `_ALLOWED_PROPS` key spellings (subject to D-07's removals/additions); the in-app module-root
  allowlist contents; the precise faulthandler-prefix→enum mapping table; the recursion-guard mechanism shape;
  the exact priority/direct-send API name added to `posthog_server`; the clean-exit `atexit` timeout (1–2s);
  whether the `notify` spike is even run (default: skip unless a slot-escape is observed).
</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & roadmap (locked)
- `.planning/REQUIREMENTS.md` — Phase 113 covers CRASH-01..07. MUST read. **CRASH-02 is amended by D-01**
  (notify clause satisfied by `sys.excepthook`).
- `.planning/ROADMAP.md` §"Phase 113: Crash Reporting" — goal + 5 success criteria.

### Phase second-opinion (this phase)
- `.planning/phases/113-crash-reporting/113-CODEX-CRITIQUE.md` — the Codex critique that produced D-05 (BLOCKER)
  and the D-01/D-02/D-03/D-06/D-07/D-08 refinements. MUST read.
- `.planning/phases/113-crash-reporting/113-CODEX-BRIEF.md` — the brief sent to Codex (decision inputs).

### Prior-phase context (the engine + consent this phase drives)
- `.planning/phases/111-telemetry-foundation/111-CONTEXT.md` — chokepoint, scrubber, allowlist, identity, the
  consent-gate-placement rule (D-04 there: `posthog_server` stays UNGATED; additions must be NEUTRAL).
- `.planning/phases/111-telemetry-foundation/111-PATTERNS.md` — pattern map for telemetry files.
- `.planning/phases/112-consent-ux/112-CONTEXT.md` — first-run consent is **deferred** behind
  recovery/sync/citation modals (relevant to D-03's pending-emit-after-consent timing).

### Research (this milestone) — read with the noted corrections
- `.planning/research/ARCHITECTURE.md` §4 (Global Exception Handling) — chaining pattern + non-blocking
  invariant. **SUPERSEDED on two points:** its `QApplication.notify`-skip is now D-01 (default skip, spike-gated);
  its `traceback_scrubbed` full-traceback prop is **rejected** by D-07 (top-frame-only, no traceback string).
- `.planning/research/FEATURES.md` §4 (Crash and Error Reporting) — the top-frame-only payload model adopted by
  D-07 (`error_type`/`error_module` basename/`error_line`/`error_fingerprint`/`is_background_thread`); the
  `_scrub_path`/`_make_crash_props` sketch. **Correction:** use frame-walking, not `format_exception` (D-07).
- `.planning/research/PITFALLS.md` — Pitfalls 1 (PII via tracebacks), 2 (excepthook chaining), 3 (blocking I/O
  in hook + daemon-thread loss). Directly governs CRASH-01/04/05/06.
- `.planning/research/STACK.md` — no-SDK / reuse-queue / zero-new-deps posture (faulthandler is stdlib).

### Code to read / extend
- `genizah_app.py:148-170` — existing `_setup_crash_handler` (chain after this; `crash_log.txt` writer);
  `genizah_app.py:3432`/`:3535`/`:3524`/`:15856` — startup modal/consent sequencing + `QMessageBox.critical`
  fatal paths (D-03 timing, D-04 caveat). Wire `install_exception_hooks()` here (D-08).
- `desktop/telemetry.py` — `install_exception_hooks()` stub `:704`; `_ALLOWED_PROPS` `:247-266` (D-07
  reconciliation); `_emit` `:505` (the `_state_lock` the crash path must bypass — D-05); `track_error` `:603`
  (NOT reused by the hook); `is_enabled` `:377` (add `_is_enabled_nolock` sibling — D-05); `set_consent` `:418`
  (the lock-holder the deadlock concern is about).
- `shared/posthog_server.py` — `enqueue_event` `:160-220` (the `_default_distinct_id_lock`/`_scrub_hook_lock`
  the crash path must bypass + the `put_nowait` drop-new behavior `:207`); `_flush_before_exit` `:272` (FIFO
  budget concern — D-06); daemon-thread start `:188` + drain `:225`. NEUTRAL priority/direct-send addition here.
- `genizah_core.py:2342-2381` — `Config.INDEX_DIR` resolution (D-03 dump-file location); `load_app_config`/
  `save_app_config` `:2871-2891` (consent read for the pending-emit gate).
- `tests/test_telemetry_no_direct_posthog.py` — the PRIV-03 AST guard; new crash code stays inside
  `desktop/telemetry.py` (the only sanctioned `enqueue_event` caller).
</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `_scrub_props`/`_scrub_value` + `_ALLOWED_PROPS` + `DesktopEvent.CRASH`/`PRIOR_CRASH` — reuse as-is (D-07
  adds keys, removes `traceback_scrubbed`).
- `shared/posthog_server._flush_before_exit(0.5)` — the bounded synchronous flush for the crash + atexit paths
  (D-06 adds a priority/direct crash send alongside it).
- `genizah_app.py:148-170 _setup_crash_handler` — the hook to chain after (never replace); `crash_log.txt`
  fallback must keep working.
- `Config.INDEX_DIR` — reliably-writable dir for the faulthandler dump (D-03).

### Established Patterns
- **Chokepoint + AST guard** — all crash code lives in `desktop/telemetry.py`; no other `desktop/` file may
  import `shared.posthog_server` (PRIV-03).
- **Shared module stays UNGATED + PyQt-free** — `posthog_server` additions must be NEUTRAL/backward-compatible
  (Phase 111 D-04); the 5 `_event_queue` monkeypatch tests must keep passing.
- **Never block the UI / never leak content** — hook body is format-free frame-walk + scrub + non-blocking
  hand-off; offline/missing-key degrades silently.

### Integration Points
- NEW: `install_exception_hooks()` body (`sys.excepthook` + `threading.excepthook` chaining, KeyboardInterrupt
  + SystemExit excluded, idempotent + deduped) → called from `genizah_app.py` after `_setup_crash_handler()`.
- NEW: faulthandler enable + module-global handle + next-launch classify/emit/truncate (D-02/D-03).
- NEW: dedicated lock-free crash emission path + distinct_id snapshot global + recursion guard (D-05).
- NEW: priority/direct crash-send helper in `posthog_server` + `atexit` registration in
  `install_exception_hooks()` (D-06/D-08).
- CHANGED: `_ALLOWED_PROPS` (D-07 reconciliation).
- No PyInstaller spec change for code; **but** flag for Phase 116: TLS/cert (certifi) bundling so the crash
  flush isn't silently dropped in the frozen binary.
</code_context>

<specifics>
## Specific Ideas

- Native fatal-error enum: `segmentation_fault` / `access_violation` / `abort` / `stack_overflow` /
  `unknown_native` — mapped from faulthandler first-line prefixes; raw text never transmitted.
- Crash payload: `exc_type`, `exc_module` (in-app basename else `external`), `exc_lineno`, `error_fingerprint`,
  `is_background_thread` (bool), base props. No message, no full traceback, no paths.
- Test the hook fires for: a `QTimer.singleShot` slot raise, a signal-connected slot raise, a real
  `QThread.run()` raise — in dev AND frozen; plus a test that `crash_log.txt` still gets written after install.
- Dedicated lock-free crash path: `_is_enabled_nolock()` + distinct_id snapshot global + recursion guard +
  direct/priority send.
</specifics>

<deferred>
## Deferred Ideas

- **Handled/non-fatal `track_error()` site wiring** (LocalIndexer / SearchThread / NLI / export) — D-04;
  Phase 114 or a small follow-up. API already exists.
- **`QApplication.notify` override** — only if a spike proves slot exceptions escape `sys.excepthook` in the
  frozen build (D-01). Otherwise never built.
- **PyInstaller TLS/cert bundling + frozen-binary offline SSL self-test + operational runbook** — Phase 116
  (already its scope); flagged here because a missing cert bundle silently drops the crash flush.
- **`crash_log.txt` relocation to `Config.INDEX_DIR`** — out of scope; the existing adjacent-to-`__file__`
  behavior is preserved by chaining (it already tolerates `OSError`). Tests must not assume it's writable in
  frozen installs.

### Reviewed Todos (not folded)
The 7 pending project todos were already reviewed against the v8.1.0 telemetry milestone in Phase 111 and found
**unrelated to telemetry** (corrections migration, Reading Desk UX, server-side search, unified metadata
search, citations, FIST manuscript fill, NLI MARC crawl). None apply to Phase 113.
</deferred>

---

*Phase: 113-crash-reporting*
*Context gathered: 2026-06-15*
