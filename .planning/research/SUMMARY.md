# Project Research Summary

**Project:** v8.1.0 Desktop Telemetry -- Dicta Genizah Search Pro
**Domain:** Opt-in, privacy-first telemetry + crash reporting for a PyQt6 frozen-binary desktop app
**Researched:** 2026-06-13
**Confidence:** HIGH

---

## Executive Summary

Desktop telemetry for a privacy-sensitive scholarly tool is a well-understood pattern with one
dominant correct approach: fire-and-forget event queue, opt-in consent default OFF, scrubbed
crash payloads, and a single public chokepoint that structurally prevents PII from reaching the
network. The infrastructure already exists -- `shared/posthog_server.py` was factored in Phase 98
specifically to be web-independent and reusable. The v8.1.0 milestone requires zero new pip
dependencies, zero spec-file changes, and no new PostHog SDK -- only three small additions to the
existing queue module plus a new `desktop/telemetry.py` chokepoint module and wiring into
`genizah_app.py`.

The recommended approach is to build `desktop/telemetry.py` as the sole gated path to
`shared/posthog_server.enqueue_event`, enforce this structurally via an AST CI guard (mirroring the
Phase 87 `web/safe_storage.py` pattern), and persist consent state in `config.pkl` via the
existing `load_app_config`/`save_app_config` interface. The event taxonomy is modest: session
start/end, tab usage, search mode + corpus + result bucket, crash/error with scrubbed tracebacks,
and a session-level performance summary. Hard privacy rules: no query text, no My Library paths or
filenames, no exception message strings (type name only), no frame-local variables, no account
linkage. These rules are enforced at three layers -- API design, `_scrub_props()`, and a static
allowlist/forbidden-property CI test.

The primary risks are PII leakage via crash tracebacks, hook clobbering, and daemon-thread event
loss at process exit. All three are addressed by Day 1 requirements in Phase 111: the scrubbing
layer must exist before the first crash event is sent, the exception hook must wrap (not replace)
the existing `_setup_crash_handler()`, and a flush helper must be called inside the exception hook
and via `atexit` to ensure crash events survive the frozen-binary exit sequence.

---

## Resolved Cross-Document Discrepancy: Consent State Storage

**ARCHITECTURE** researcher found (by direct grep of `genizah_app.py`, verified at lines
2344-2378): zero `QSettings` instantiation in the main app code. Consent state should go in
`config.pkl` via `load_app_config` / `save_app_config`.

**PITFALLS** researcher referenced `QSettings("Dicta", "GenizahSearchPro")` (found at
`desktop/my_library_tab.py` line 1047) and cautioned that any consent key MUST use that same
organization/app string to avoid landing in a different registry hive.

**Resolved decision: use `config.pkl` via `load_app_config` / `save_app_config`.**

Rationale: `config.pkl` is the existing persistent preference store for language choice, variant
settings, lab config path, and all other durable preferences. `QSettings` is used only in
`desktop/my_library_tab.py` for that specific component. The PITFALLS concern about key mismatch
applies to the QSettings-based approach, not config.pkl. Using `config.pkl` avoids the mismatch
problem entirely. `session.json` is explicitly the wrong choice -- it is cleared by crash recovery
and Reset My Library flows.

**Flag for discussion:** If any future phase adds consent-state access from a component that cannot
import `genizah_core`, the QSettings path would need reconsideration. For v8.1.0 all access paths
run through `desktop/telemetry.py` which imports `genizah_core` normally -- no issue.

---

## Key Findings

### Recommended Stack

The headline decision: **reuse `shared/posthog_server.py`; do NOT add the `posthog` SDK.**

The SDK's `capture_exception_code_variables=True` feature sends frame-local variable values to
PostHog before the `before_send` hook fires -- making it impossible to prevent query text and My
Library paths from leaking via crash reports without patching the SDK itself. The SDK also adds
one new transitive dependency (`backoff`) for no meaningful benefit at the ~1,800 events/day
expected volume. The raw queue is already production-proven (Phase 98 NLI breaker telemetry),
already points at the correct EU endpoint, and requires only three backward-compatible additions.

**Core technologies:**

- `shared/posthog_server.py` (existing, extend): fire-and-forget queue to EU PostHog -- already
  production-proven, web-independent, zero new deps; needs `_telemetry_enabled` gate +
  `_scrub_hook` + `set_default_distinct_id()`
- `desktop/telemetry.py` (new module): single public chokepoint -- consent gate, scrubbing,
  install-ID management, exception hooks, event helpers; AST guard prevents any other file under
  `desktop/` from calling `enqueue_event` directly
- `uuid.uuid4()` (stdlib): anonymous per-install identifier -- pure random, no MAC address, no
  hardware linkage; minted only on opt-in, stored in `config.pkl` under `telemetry_install_id`
- `sys.excepthook` + `threading.excepthook` + `faulthandler` (stdlib): layered crash coverage --
  main thread, background Python threads, and native C-extension crashes; hooks WRAP the existing
  `_setup_crash_handler()`, never replace it
- `genizah_core.load_app_config` / `save_app_config` (existing): consent state persistence --
  `telemetry_enabled` (bool), `telemetry_first_run_shown` (bool), `telemetry_install_id` (str
  uuid4 hex) stored in `config.pkl`; survives crashes, updates, and session recovery

**Net new pip dependencies: zero. Spec file changes: none.**

The `phc_...` PostHog project API key is a write-only publishable key, safe to embed as a string
constant in `desktop/telemetry.py` (the same practice used for the web app's `<script>` tag). A
separate PostHog project for desktop telemetry is recommended to contain abuse blast radius.

### Expected Features

**Must have (table stakes -- P1, ship in v8.1.0):**

- Opt-in consent dialog, bilingual EN+HE, default OFF, modal first-run, two equal-weight buttons
- UUID generation only on opt-in; deleted (not just ignored) on opt-out
- Settings/About toggle with immediate effect (checked at call site, not just startup)
- `$process_person_profile: False` on every event (keeps events in PostHog anonymous tier, 4x
  cheaper ingestion, no person profile ever created)
- `desktop_session_start`, `desktop_tab_activated`, `desktop_search_executed` (mode + corpus +
  result_count_bucket, no content)
- `desktop_session_performance_summary` at app close -- aggregated per-session, not per-search
  (reduces volume ~50x for heavy users; ~60-90 events/day vs ~1,500/day)
- `desktop_crash` via `sys.excepthook` + `threading.excepthook` with scrubbed props (type name +
  basename module + line number; no exception message string, no frame locals)
- Static AST CI guard -- forbidden property names (`query`, `text`, `path`, `filename`,
  `shelfmark`, etc.) blocked structurally
- Privacy disclosure text, bilingual, inline in consent dialog

**Should have (differentiators -- P2, add in v8.1.x after validation):**

- `desktop_responsa_options` -- Responsa sub-option bitmask (expansion/fuzzy/Judeo-Arabic/spacing)
- `desktop_joins_lab_action` -- Joins Lab adoption signal for Component B prioritization
- `desktop_error` at high-value handled-error sites (LocalIndexerWorker, NLI fetch, export)
- `desktop_export` -- format breakdown (xlsx/csv/txt/docx)

**Defer to v2+:**

- Re-ask consent if data categories expand materially
- PostHog dashboard insights / aggregate session analysis (operational, no code change)
- `desktop_puzzle_action` -- Puzzle is a secondary feature

**Hard anti-features (never implement):**

- Any query or search text in any event payload
- My Library file paths, filenames, or document counts
- Manuscript shelfmarks or sys_ids of opened results
- Hardware fingerprinting (MAC, CPU ID, screen resolution)
- Exception message string `str(exc_value)` -- commonly contains file paths and query content
- Frame-local variable capture in any traceback payload
- Supabase user ID or email as `distinct_id`
- Always-on / opt-out default

### Architecture Approach

The architecture follows the Phase 87 `web/safe_storage.py` chokepoint pattern applied to
telemetry: a single module is the only sanctioned path to the network, enforced by a CI AST test.
`desktop/telemetry.py` exposes eight public callables; every call passes through `_scrub_props()`
before reaching `shared/posthog_server.enqueue_event`. `SearchThread`, `CompositionThread`, and
`LabSearchThread` emit a new `perf_signal(float, int)` Qt signal; the UI-thread handler calls
`track_performance()` -- keeping telemetry always on the UI thread, consistent with all other
result-handling in this codebase. Consent state lives in `config.pkl`; the first-run dialog fires
from `GenizahGUI.__init__` post-show; the Settings/About toggle calls `set_consent()`.

**Major components:**

1. `desktop/telemetry.py` (new) -- consent gate, scrubbing, install-ID, exception hooks, all
   public event helpers; the ONLY file permitted to call `enqueue_event`
2. `shared/posthog_server.py` (extend, 3 additions) -- `_telemetry_enabled` flag +
   `set_telemetry_enabled()`, `_scrub_hook` + `register_scrub_hook()`,
   `set_default_distinct_id()`; backward-compatible; existing callers unaffected
3. `genizah_app.py` (wire) -- `install_exception_hooks()` after `_setup_crash_handler()`,
   `show_first_run_prompt()` after `self.show()`, `track()` at action sites
4. `gui_threads.py` (wire) -- `perf_signal = pyqtSignal(float, int)` + timing wrapper in
   `SearchThread.run()`, `CompositionThread.run()`, `LabSearchThread.run()`
5. `SettingsDialog._build_general_tab()` (wire) -- telemetry checkbox row, wired to `set_consent()`

**Structural invariant:** The path to the network is always:
```
track() / track_error() / _capture_crash()
    -> _scrub_props()  [always, internal]
    -> enqueue_event()
```

### Critical Pitfalls

All 13 pitfalls from PITFALLS.md target Phase 111. The five that must become explicit test
requirements:

1. **PII via traceback frame locals and exception message** -- Never include `str(exc_value)`.
   Emit only `type(exc).__name__` + scrubbed module basename + line number. Apply `_scrub_props()`
   BEFORE enqueuing (in the hook, not the drain thread). Strip frame `vars` entirely. Test: emit
   a fake crash with a frame local containing a file path; verify PostHog payload contains no path.

2. **Exception hook clobbering** -- Wrap, never replace. Capture `_prior_hook = sys.excepthook`
   AFTER `_setup_crash_handler()` runs (line ~170 in `genizah_app.py`). Chain unconditionally at
   end of wrapper. Guard entire hook body in `try/except Exception: pass`. Test: verify
   `crash_log.txt` is still written after the telemetry hook is installed.

3. **Pre-consent event firing** -- `is_enabled()` returns `False` when `telemetry_enabled` key is
   absent from `config.pkl`. UUID not minted until `set_consent(True)`. Test: verify zero events
   enqueued on a fresh `config.pkl` before the consent dialog is shown.

4. **Daemon-thread event loss at process exit** -- `posthog_server.py` drain thread is a daemon
   killed when the main thread exits. Add `_flush_before_exit(timeout=0.5)` called (a) from
   `atexit` for clean exits and (b) from inside the exception hook after enqueueing the crash
   event (`atexit` does not run on unhandled exceptions in CPython). Day 1 requirement. Test:
   mock drain, enqueue 3 events, call `_flush_before_exit()`, verify all 3 POSTed.

5. **Property allowlist violation** -- Static AST test modeled on `test_no_raw_storage_access.py`
   scans all `track()` call sites under `desktop/` and asserts no forbidden property names appear.
   Forbidden: `query`, `text`, `content`, `path`, `filename`, `shelfmark`, `sys_id`, `fl_id`,
   `email`, `user_id`, `username`, `supabase_id`, `jwt`, `token`, `clean_query`, `query_text`.
   Run in CI on Ubuntu + Windows.

Additional pitfalls to address:

- **`threading.excepthook` missing** (codebase grep: zero current hits) -- install it; covers
  `SearchThread`, `LocalIndexerWorker`, `FolderWalkWorker` where most desktop crashes originate
- **Opt-out queue race** -- on opt-out, drain and discard in-memory queue without sending
- **PyInstaller SSL certs** -- verify `certifi` data files in `GenizahSearchPro.spec`; test on
  clean Windows VM
- **Offline degradation** -- `requests.post(timeout=(1.0, 2.0))` tuple; 5-failure backoff;
  app starts normally with no network

---

## Implications for Roadmap

The researchers proposed two decompositions. Both are presented; the roadmapper should choose.

### Option A: 6-Phase Decomposition (recommended by ARCHITECTURE researcher)

Strict dependency ordering; each phase green before the next. Maximum testability isolation.

**Phase 111 -- Foundation: `desktop/telemetry.py` + consent storage + scrubbing + flush**
- Rationale: Everything else depends on `is_enabled()`, `track()`, and `_scrub_props()` existing
- Delivers: consent gate, UUID lifecycle, `_scrub_props()`, `_flush_before_exit()`,
  `set_consent()`, BASE_PROPS helper, 3 additions to `shared/posthog_server.py`
- Avoids: Pitfalls 1 (PII via traceback), 3 (sync network in hook), 4 (pre-consent firing),
  5 (opt-out race), 6 (non-anonymous UUID), 8 (property allowlist), 9 (daemon thread exit),
  10 (PyInstaller SSL)
- Tests: `test_telemetry_consent_gate.py`, `test_telemetry_scrubbing.py`,
  `test_telemetry_no_direct_posthog.py` (AST guard)
- Research flag: STANDARD PATTERNS -- no research-phase needed

**Phase 112 -- Consent UX: first-run dialog + Settings toggle**
- Rationale: Consent must be plumbable before any events can fire
- Delivers: bilingual QDialog (default OFF, two buttons), `show_first_run_prompt()` wired into
  `GenizahGUI.__init__`, checkbox row in `SettingsDialog._build_general_tab()`
- Avoids: Pitfall 5 (opt-in defaults on), Pitfall 7 (re-prompts every launch), Pitfall 13
  (bilingual disclosure)
- Tests: dialog fires exactly once; Settings toggle calls `set_consent()`; config.pkl persists
- Research flag: STANDARD PATTERNS

**Phase 113 -- Exception hooks**
- Rationale: Hook installation is startup-time; must chain existing `_setup_crash_handler()`
- Delivers: `install_exception_hooks()` (wraps `sys.excepthook` + installs
  `threading.excepthook`), `faulthandler.enable()`, crash event scrubbing,
  `_flush_before_exit()` called in hook
- Avoids: Pitfall 2 (hook clobbering), Pitfall 3 (sync network in hook)
- Tests: prior hook still runs; `crash_log.txt` written; enqueue non-blocking;
  `KeyboardInterrupt` not captured; thread hook covers worker threads
- Research flag: STANDARD PATTERNS

**Phase 114 -- Usage events**
- Rationale: Core feature-usage data; needs consent gate and scrubbing from Phases 111-112
- Delivers: `desktop_session_start`, `desktop_tab_activated`, `desktop_search_executed`,
  `desktop_joins_lab_action`, `desktop_my_library_action`; `track()` calls at action sites
- Avoids: Pitfall 8 (volume blowup -- per-search events carry no duration, only mode/corpus/
  result bucket; duration goes only to the session accumulator)
- Tests: events gated on consent; scrubbing rules exercised; forbidden property names blocked
- Research flag: STANDARD PATTERNS

**Phase 115 -- Performance events**
- Rationale: Independent of crash hooks; needs `track_performance()` from Phase 111
- Delivers: `perf_signal = pyqtSignal(float, int)` on SearchThread/CompositionThread/
  LabSearchThread; in-memory threading.Lock-protected session accumulator;
  `desktop_session_performance_summary` at app close; `latency_bucket()` /
  `result_count_bucket()` reused from `web/api_hardening.py`
- Avoids: Pitfall 8 (session-summary approach: ~60-90 events/day vs ~1,500/day per-search)
- Research flag: STANDARD PATTERNS

**Phase 116 -- Privacy audit + CI gate**
- Rationale: Validation phase; requires all prior phases to be wired
- Delivers: full test suite exercise of every `track()` callsite with `is_enabled()` forced True;
  AST guard confirmed green on Ubuntu + Windows CI; bilingual disclosure finalized;
  PyInstaller SSL cert verification on clean VM
- Research flag: STANDARD PATTERNS -- validation, not new development

### Option B: 2-Phase Decomposition

Faster delivery; less isolation.

**Phase 111 -- Full infrastructure** (merges Options A 111 + 113 + 116):
- `desktop/telemetry.py` scaffold + consent + scrubbing + flush + exception hooks + AST guard
- All privacy-safety requirements before any events fire

**Phase 112 -- Consent UX + usage events + performance** (merges Options A 112 + 114 + 115):
- First-run dialog + Settings toggle + all event emission + performance summary

**Recommendation:** Option A. The 6-phase structure matches the codebase's established GSD rhythm
and gives natural UAT checkpoints. Phases 111-113 are safety-critical; shipping them independently
reduces the risk of privacy regressions slipping in alongside UX work.

### Phase Ordering Rationale

- **Phase 111 must be first** because `is_enabled()` and `_scrub_props()` are called by every
  other phase. Building exception hooks before the consent gate exists would mean crash events
  could fire before consent is recorded.
- **`_flush_before_exit()` belongs in Phase 111**, not as a follow-up. Crash events are lost
  without it, and flush is queue infrastructure, not UI.
- **Consent UX (Phase 112) before exception hooks (Phase 113)** ensures the first event that
  could fire (a crash on first run) is already gated.
- **Usage events (Phase 114) before performance** because mode/corpus data is simpler to add
  and validate without the timing infrastructure.
- **Privacy audit (Phase 116) last** because it validates the entire stack.

### Research Flags

All 6 phases use standard, well-documented patterns. All integration points have been directly
verified against real source files (exception hook lines 148-170, `config.pkl` lines 2344-2378,
`SearchThread` lines 96-121, `SettingsDialog` line 2210). No `/gsd-research-phase` needed for
any of these phases.

---

## Open Questions (carry into requirements discussion)

1. **UUID retention on opt-out:** Delete UUID file on opt-out (current recommendation for full
   anonymity signal), or retain so a user who opts back in keeps the same install identity?
   FEATURES and ARCHITECTURE both recommend deletion. Confirm with user.

2. **Prompt on upgrade vs. fresh install only:** Show first-run dialog for existing users
   upgrading from v8.0.0? Current design: show once per config.pkl (i.e., existing users see it
   on first v8.1.0 launch). Confirm with user before locking requirements.

3. **`faulthandler` log path:** Recommend `%LOCALAPPDATA%\GenizahSearchPro\faulthandler.log`
   resolved at runtime. Confirm path and whether to surface it in any "send logs" flow.

4. **Performance accumulator vs. per-search sampling:** ARCHITECTURE researcher defaults to
   1-in-3 per-search `track_performance()` emit; FEATURES researcher uses session-summary
   aggregation. These are complementary -- session-summary is the right v8.1.0 answer;
   `track_performance()` feeds the accumulator (no per-search PostHog duration event).
   Clarify in requirements.

5. **Web `search_executed` query-text property:** The web app currently sends
   `query: clean_query[:100]` in its `search_executed` event -- a pre-existing privacy gap.
   Not v8.1.0 scope but flag as a follow-up cleanup item.

6. **Separate PostHog project for desktop:** PITFALLS researcher recommends a separate `phc_...`
   key to contain abuse blast radius and prevent schema pollution. Confirm whether to use existing
   project (id 134161) or create a new desktop-only project -- this determines which API key gets
   embedded in `desktop/telemetry.py`.

---

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | All claims verified against real source files: `posthog_server.py`, `GenizahSearchPro.spec`, `requirements-lock.txt`, PyPI API. No inferences -- direct reads. |
| Features | HIGH | Event taxonomy and property allowlists cross-verified against existing web instrumentation in `web/pages/search.py` and `web/api_hardening.py`. |
| Architecture | HIGH | All integration points verified by direct file reads: `_setup_crash_handler()` lines 148-170, `load_app_config` lines 2344-2378, `SearchThread.run()` lines 96-121, `SettingsDialog._build_general_tab()` line 2210. `threading.excepthook` grep: zero hits. `QSettings` in `genizah_app.py` grep: zero hits. |
| Pitfalls | HIGH | 13 pitfalls identified, all cross-referenced to specific lines in the real codebase. QSettings discrepancy between ARCHITECTURE and PITFALLS researchers is fully resolved above. |

**Overall confidence: HIGH**

### Gaps to Address

- **PyInstaller SSL cert bundle:** Needs manual verification (run frozen `.exe` on a clean
  Windows VM, confirm PostHog events received). Cannot be verified without a build. Make this
  an explicit success criterion for Phase 116.
- **`faulthandler` on Windows:** Behavior differs slightly from POSIX. Add to Phase 116 checklist
  for explicit testing in the frozen binary.
- **PostHog project selection:** If a separate desktop project is created, the `phc_...` key
  embedded in `desktop/telemetry.py` differs from `POSTHOG_API_KEY` in the web env. Document
  this clearly before implementation starts.

---

## Sources

### Primary (HIGH confidence -- direct codebase reads)

- `genizah_app.py` lines 148-170 -- `_setup_crash_handler()` and `sys.excepthook` chain
- `genizah_app.py` lines 2344-2378 -- `Config.CONFIG_FILE` = `%LOCALAPPDATA%\GenizahSearchPro\config.pkl`
- `genizah_app.py` line 2210 -- `SettingsDialog._build_general_tab()`
- `shared/posthog_server.py` -- `Queue(maxsize=10000)` line 47; daemon thread line 132-133; EU endpoint line 44
- `gui_threads.py` lines 96-121 -- `SearchThread.run()`
- `desktop/my_library_tab.py` line 1047 -- `QSettings("Dicta", "GenizahSearchPro")`
- `web/pages/search.py:4439` -- web `search_executed` sends `query: clean_query[:100]`
- `web/api_hardening.py` -- `latency_bucket()`, `result_count_bucket()`
- `GenizahSearchPro.spec` -- `('shared', 'shared')` in `datas`; no new spec changes needed
- `requirements-lock.txt` -- `backoff` absent; confirms zero new deps
- Codebase grep -- `threading.excepthook`: zero hits; `QSettings` in `genizah_app.py`: zero hits

### Primary (HIGH confidence -- official documentation)

- PostHog Python SDK PyPI `https://pypi.org/pypi/posthog/json` -- version 7.18.3; `backoff>=1.10.0` transitive dep confirmed
- PostHog error tracking docs -- `capture_exception_code_variables` sends local vars before `before_send` fires
- PostHog project API key safety -- write-only publishable key, safe to embed in distributed binary
- PostHog anonymous events -- `$process_person_profile: False` semantics and anonymous-tier cost
- Python docs: `sys.excepthook`, `threading.excepthook`, `faulthandler`, `uuid` -- all stdlib, Python 3.10+
- GDPR Article 7 -- opt-in consent requirements; pre-checked checkboxes not valid

### Secondary (MEDIUM confidence)

- PyQt6 exception hook patterns (fman.io) -- slot exceptions suppressed by C++ layer; `sys.excepthook` alone insufficient
- PyInstaller `certifi` bundle documentation -- must be explicitly included for `requests` SSL in frozen binaries
- VSCode telemetry docs -- opt-out anti-pattern reference
- Zotero privacy policy -- scholarly tool minimal-telemetry pattern

---
*Research completed: 2026-06-13*
*Ready for roadmap: yes*