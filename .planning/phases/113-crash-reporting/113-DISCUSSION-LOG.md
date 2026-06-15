# Phase 113: Crash Reporting - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-15
**Phase:** 113-crash-reporting
**Areas discussed:** Qt-slot capture (CRASH-02 conflict), Native-crash reporting (CRASH-03/07), Handled-error wiring scope, Hook safety + atexit details
**Second opinion:** Codex critique requested before CONTEXT (`113-CODEX-CRITIQUE.md`)

---

## Qt-slot capture (CRASH-02 conflict)

| Option | Description | Selected |
|--------|-------------|----------|
| Rely on sys.excepthook (Recommended) | No notify override; sys.excepthook + threading.excepthook cover slot/worker exceptions in PyQt6; amend CRASH-02; add a slot-reaches-hook test | |
| Add notify override too | Install a non-swallowing, deduped QApplication.notify override IN ADDITION as defense-in-depth | |
| You decide during planning | Default to sys.excepthook-only; add a non-swallowing deduped notify override only if a spike shows slot exceptions escape in the frozen build | ✓ |

**User's choice:** You decide during planning.
**Notes:** Captured as D-01. Default = sys.excepthook-only + threading.excepthook; CRASH-02 amended to "satisfied by sys.excepthook." Notify override is spike-gated and, if added, must be a QApplication subclass installed before QApplication(sys.argv), non-swallowing, deduped. Codex added the QThread.run()-coverage nuance + required test matrix.

---

## Native-crash reporting (CRASH-03/07)

**Q1 — next-launch event content:**

| Option | Description | Selected |
|--------|-------------|----------|
| Boolean + signal label (Recommended) | base props + a path-free fatal-error label parsed from faulthandler's first line; no frames/paths/module/line | ✓ |
| Boolean only | pure count signal, base props only, nothing parsed | |
| Parse top frame (module+line) | parse + scrub the innermost native frame like the Python path | |

**User's choice:** Boolean + signal label.
**Notes:** Captured as D-02 — **refined by Codex** from "parse first line" to a FIXED ENUM mapping (segmentation_fault / access_violation / abort / stack_overflow / unknown_native); raw first-line text is never transmitted.

**Q2 — dump file location + lifecycle:**

| Option | Description | Selected |
|--------|-------------|----------|
| Config dir + read-then-truncate (Recommended) | dump in config.pkl dir; faulthandler.enable at startup; next launch emit-once-when-consented then truncate | ✓ |
| Next to crash_log.txt | beside the existing crash log (os.path.dirname(__file__)) — risk of read-only/temp frozen path | |
| You decide during planning | default to config-dir read-then-truncate; planner finalizes filename + edge cases | |

**User's choice:** Config dir + read-then-truncate.
**Notes:** Captured as D-03 — **corrected by Codex:** use `Config.INDEX_DIR` (not a hardcoded AppData path); read+classify the previous dump BEFORE `faulthandler.enable()`; keep handle in a module global; truncate only after the one-shot decision; explicit pending-emit-after-consent path because Phase 112's first-run consent is deferred behind modals.

---

## Handled-error wiring scope

| Option | Description | Selected |
|--------|-------------|----------|
| Defer handled-error wiring (Recommended) | 113 = unhandled crash hooks + native only; track_error() API stays producer-less until 114/follow-up | ✓ |
| Wire a few high-value sites now | also instrument LocalIndexer/SearchThread/NLI/export with track_error() | |
| You decide during planning | default to deferring; planner may add 1-2 trivial sites | |

**User's choice:** Defer handled-error wiring.
**Notes:** Captured as D-04. Codex AGREE; flagged the caveat that QMessageBox.critical startup-failure paths stay invisible until 114.

---

## Hook safety + atexit details

**Q1 — consent check inside the hook:**

| Option | Description | Selected |
|--------|-------------|----------|
| Lock-free global read (Recommended) | read module global _enabled directly via _is_enabled_nolock(); satisfies SC#4 "no lock acquisition" + CRASH-05 | ✓ |
| Keep is_enabled() | use is_enabled() as-is (acquires _enabled_lock; narrow deadlock window) | |
| You decide during planning | default to lock-free read | |

**User's choice:** Lock-free global read.
**Notes:** Captured as D-05 — **escalated by Codex to BLOCKER:** lock-free `_enabled` read is necessary but NOT sufficient; the crash path must also bypass `_emit()`'s `_state_lock` and `enqueue_event()`'s `_default_distinct_id_lock`/`_scrub_hook_lock`, use a lock-free distinct_id snapshot, and add a recursion guard.

**Q2 — atexit registration + clean-exit timeout:**

| Option | Description | Selected |
|--------|-------------|----------|
| Desktop-side, in install_exception_hooks() (Recommended) | register atexit there (not in shared posthog_server); clean-exit timeout ~1-2s; no-op when empty/opted-out | ✓ |
| At telemetry.py import time | register at module import for broader coverage | |
| You decide during planning | default to install_exception_hooks() with ~1-2s | |

**User's choice:** Desktop-side, in install_exception_hooks().
**Notes:** Captured as D-08 — Codex added the D-06 delivery-priority concern (daemon races FIFO flush; crash event needs priority/direct send) + the wiring pitfall (install_exception_hooks() is an uncalled stub) + idempotent/deduped registration + SystemExit exclusion.

---

## Claude's Discretion

- D-01: whether the notify-override spike is run at all (default: skip unless a slot-escape is observed); exact subclass shape if built.
- D-02: the precise faulthandler-prefix→enum mapping table.
- D-05: the recursion-guard mechanism + distinct_id snapshot-global shape.
- D-06: the exact priority/direct-send API name added to posthog_server.
- D-07: exact new `_ALLOWED_PROPS` key spellings + the in-app module-root allowlist (the `traceback_scrubbed` removal + no-full-traceback rule are locked).
- D-08: clean-exit atexit timeout (1–2s).

## Deferred Ideas

- Handled/non-fatal `track_error()` site wiring → Phase 114 / follow-up (D-04).
- `QApplication.notify` override → only if a spike proves slot-escape in the frozen build (D-01).
- PyInstaller TLS/cert bundling + frozen offline SSL self-test + runbook → Phase 116 (flagged because a missing cert bundle silently drops the crash flush).
- `crash_log.txt` relocation to `Config.INDEX_DIR` → out of scope; chaining preserves the existing behavior.
