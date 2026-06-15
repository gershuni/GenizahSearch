---
phase: 113
reviewers: [codex]
reviewed_at: 2026-06-15T08:55:49Z
review_pass: 2
plans_reviewed: [113-01-PLAN.md, 113-02-PLAN.md, 113-03-PLAN.md]
---

# Cross-AI Plan Review — Phase 113 (PASS 2)

> Pass 1 (4 HIGH + 5 MEDIUM) is preserved in git history at commit `620f8815` and was folded
> into the plans via `/gsd:plan-phase 113 --reviews` (commit `dd2f1e51`). This is the PASS-2
> verification: confirm each prior finding is resolved + fresh look. Running inside Claude Code,
> so the claude CLI was skipped for independence.
>
> **Result:** all 4 HIGH RESOLVED; 7/9 MEDIUM resolved; **MED-9 PARTIAL + 3 new MEDIUM remain.**
> Codex verdict: **safe to execute? NO — patch the plans first (small, correctness-relevant).**
> Orchestrator independently confirmed: `_BASE_PROPS()` emits no OS field; `.venv` AND `venv`
> both exist under the repo root (MED-9 severity real); CONTEXT :140 does lock traceback-id dedup.

## Codex Review (Pass 2)

Scope note: the repo is still pre-Phase-113 implementation. I’m judging whether the revised plans now give correct, test-pinned instructions against the current live code.

**Verdict Table**

| Finding | Verdict | Proof |
|---|---:|---|
| HIGH-1 | RESOLVED | Plan 01 now requires snapshot globals and forbids `_resolve_*`, `_capture_config_lock`, `_event_queue` in `send_crash_event_direct` ([113-01-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-01-PLAN.md:216), [113-01-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-01-PLAN.md:230)); live code proves why: `_capture_config_lock` at [shared/posthog_server.py](C:/Genizahsearch/shared/posthog_server.py:76), `_resolve_api_key()` / `_resolve_capture_url()` lock at [shared/posthog_server.py](C:/Genizahsearch/shared/posthog_server.py:145), [shared/posthog_server.py](C:/Genizahsearch/shared/posthog_server.py:152). |
| HIGH-2 | RESOLVED | Plan 02 explicitly adds `send_crash_event_direct` to the module-top import and bans in-hook imports ([113-02-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-02-PLAN.md:154)); live import block is already at module top and is the right seam ([desktop/telemetry.py](C:/Genizahsearch/desktop/telemetry.py:43)). |
| HIGH-3 | RESOLVED | Plan 02 adds `_crash_distinct_id = distinct_id` in `_load_consent_state()` ([113-02-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-02-PLAN.md:170)); live startup path currently only calls `set_default_distinct_id(distinct_id)` at [desktop/telemetry.py](C:/Genizahsearch/desktop/telemetry.py:365). |
| HIGH-4 | RESOLVED | Plan 03 explicitly chooses direct-send over hook-time `_flush_before_exit` and documents the SC#5 reconciliation ([113-03-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-03-PLAN.md:235), [113-03-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-03-PLAN.md:333)); live `_flush_before_exit()` takes config locks via `_resolve_*` at [shared/posthog_server.py](C:/Genizahsearch/shared/posthog_server.py:288). |
| MED-5 | RESOLVED | Plan 01 uses a non-autouse `crash_telemetry_state` fixture and only local crash-module autouse wrappers ([113-01-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-01-PLAN.md:154)); live project already has the broad autouse fixture at [tests/conftest.py](C:/Genizahsearch/tests/conftest.py:221). |
| MED-6 | RESOLVED | Plan 03 says no `qtbot` and copies the repo’s `QApplication.instance() or QApplication(sys.argv)` pattern ([113-03-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-03-PLAN.md:243)); live pattern is [tests/test_join_workbench_construct.py](C:/Genizahsearch/tests/test_join_workbench_construct.py:15). |
| MED-7 | RESOLVED | Plan 03 captures `_prior_threading_hook = threading.excepthook`, explicitly not `threading.__excepthook__` ([113-03-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-03-PLAN.md:227)); live hook installer is still a stub at [desktop/telemetry.py](C:/Genizahsearch/desktop/telemetry.py:704), so this is the right insertion point. |
| MED-8 | RESOLVED | Plan 02 restores sys/thread hooks in `_reset_for_tests`, and Plan 03 stores prior hooks + guards atexit registration ([113-02-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-02-PLAN.md:176), [113-03-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-03-PLAN.md:219)); live reset currently only clears telemetry state ([desktop/telemetry.py](C:/Genizahsearch/desktop/telemetry.py:738)). |
| MED-9 | PARTIAL | Plan 02 fixes basename-only classification with resolved roots and generic-basename exclusion ([113-02-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-02-PLAN.md:227)), but it includes the whole repo root as an app root ([113-02-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-02-PLAN.md:229)). Live repo root contains `venv/Lib/site-packages` and `.venv/Lib/site-packages`, so third-party frames under the repo can be misclassified as in-app and transmit arbitrary basenames. |

**New Issues**

- **MEDIUM: Crash/native payloads omit required OS fields.** CRASH-04 requires app version and OS ([REQUIREMENTS.md](C:/Genizahsearch/.planning/REQUIREMENTS.md:59)); live `_BASE_PROPS()` only returns `platform` and `app_version` ([desktop/telemetry.py](C:/Genizahsearch/desktop/telemetry.py:307)), and Plans 02/03 reuse `_BASE_PROPS()` for crash/native payloads ([113-02-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-02-PLAN.md:166), [113-03-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-03-PLAN.md:164)). Add lock-free `os_family` / `os_version` base props or a crash-specific static base-props helper.

- **MEDIUM: Locked dedup requirement is still not implemented.** Context requires double-report dedup keyed by traceback / exception object id ([113-CONTEXT.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-CONTEXT.md:140)); Plan 03 only covers install idempotency and prior-hook chaining ([113-03-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-03-PLAN.md:219)). Add an explicit no-lock dedup mechanism and test for duplicate hook invocations on the same traceback.

- **MEDIUM: Native pending dump lifecycle is internally contradictory.** Plan 03 says when consent is false, set `_pending_native_crash` and “do NOT truncate” ([113-03-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-03-PLAN.md:172)), but the next step opens the same dump with `'w'`, which truncates it ([113-03-PLAN.md](C:/Genizahsearch/.planning/phases/113-crash-reporting/113-03-PLAN.md:173)). Either document that memory-only pending is intentional, or preserve/rotate the prior dump until consent or the next native crash.

**Overall Risk**

**MEDIUM.** The four prior HIGH findings are now addressed in the revised plans, but MED-9 is only partially fixed and the revised plans still drift from locked crash requirements around OS props, dedup, and pending native-dump handling.

**Safe to execute? No.** I would patch the plans first; the fixes are small but correctness-relevant.

---

## Remaining Items To Fold In (next `--reviews` pass)

All MEDIUM; no HIGH outstanding. Each fix is small and verified-real against the live repo.

1. **MED-9 (PARTIAL) — repo root as app source root leaks third-party basenames.** The in-app
   classification includes the whole repo root, but `.venv/` and `venv/` live under it, so
   `venv/Lib/site-packages/<pkg>/<file>.py` frames classify as IN-APP and transmit arbitrary
   third-party basenames. Fix: app roots = `desktop/` + `shared/` (+ the top-level app modules
   like `genizah_app.py`/`genizah_core.py`) resolved roots ONLY — explicitly EXCLUDE any path
   containing `site-packages`, `.venv`, `venv`. Add a test: a frame under `venv/Lib/site-packages`
   classifies as `external`.

2. **NEW MEDIUM — crash/native payload omits OS (CRASH-04 / SC#3 / D-02 `os_*`).** `_BASE_PROPS()`
   returns only `{'platform':'desktop','app_version':...}` — `platform` is the web/desktop
   discriminator, NOT an OS. Crash + native events reuse `_BASE_PROPS()`, so they ship no OS field.
   Fix: add lock-free `os_family`/`os_version` (e.g. `platform.system()`/`platform.release()` read
   ONCE at import into module constants, NOT inside the hook) to the crash base props (or a
   crash-specific static base-props helper); add the keys to `_ALLOWED_PROPS`; assert presence in
   the payload test. Must stay lock-free (D-05).

3. **NEW MEDIUM — locked traceback-id dedup not implemented (CONTEXT D-08, :140-141).** Plan 03
   covers install idempotency + chain-once, but NOT the locked "double-REPORT dedup keyed by
   traceback / exception object id." Fix: add a no-lock dedup (e.g. remember the last reported
   `id(exc_traceback)` / `id(exc)` in a module global; skip re-emit if it matches) so the
   slot/excepthook double-delivery path emits once. Add a test firing the wrapper twice on the
   same traceback ⇒ exactly one emit.

4. **NEW MEDIUM — native pending-dump lifecycle contradiction (Plan 03).** When consent is false,
   the plan says set `_pending_native_crash` and "do NOT truncate," but the next step opens the
   same dump with `'w'` (which truncates). Fix: either document that pending is memory-only and
   the dump truncation is intentional (the enum label is already captured in memory), or preserve/
   rotate the prior dump until consent or the next native crash. Make the plan internally consistent.

### Verdict recap
- HIGH-1..HIGH-4: **RESOLVED** ✓
- MED-5, MED-6, MED-7, MED-8: **RESOLVED** ✓
- MED-9: **PARTIAL** (item 1 above)
- New: OS props (item 2), traceback dedup (item 3), pending-dump truncation (item 4)
- Overall risk: **MEDIUM** · Safe to execute: **NO until items 1-4 are patched**
