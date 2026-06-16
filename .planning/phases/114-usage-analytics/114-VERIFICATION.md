---
phase: 114-usage-analytics
verified: 2026-06-15T22:00:00Z
status: human_needed
score: 6/6
overrides_applied: 0
human_verification:
  - test: "Opt the desktop app in to telemetry, log in with a Supabase account, then open PostHog and confirm a desktop_session_start event appears with distinct_id equal to the user's Supabase UUID (not an int hash), no hostname/username/path props present."
    expected: "Event appears in PostHog with correct distinct_id and only app_version, os_family, os_version, python_version, pyqt_version, ui_language, session_id props."
    why_human: "End-to-end event transmission to the PostHog EU project requires a live desktop session with valid credentials and real network access — not verifiable by grep or unit tests."
  - test: "Reword consent_dialog.py lines 306-308 (EN) and 339-340 (HE) to say 'Supabase account identifier (UUID)' instead of 'user.id' — or accept this as a known disclosure-accuracy gap (WR-04)."
    expected: "Consent text accurately describes the identifier sent (user._uuid, a UUID string) rather than user.id (an int hash)."
    why_human: "WR-04 is a compliance/disclosure surface — the decision to fix or accept requires human judgment. The code behavior is privacy-correct; only the disclosure text is inaccurate."
---

# Phase 114: Usage Analytics Verification Report

**Phase Goal:** The desktop app emits allowlisted usage events (session start/end, tab/surface activations, search mode and corpus enums) that enable DAU/MAU, version adoption, and feature-use measurement in PostHog — with no query content, no My Library data, and no environment identifiers beyond OS family/version.
**Verified:** 2026-06-15T22:00:00Z
**Status:** human_needed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 0 | On login desktop calls identify(distinct_id = user._uuid), aliases prior anon uuid; logout resets to anon id; only uuid sent, never email/name | VERIFIED | `genizah_app.py:3598` calls `telemetry.identify(user._uuid)` — grep confirms zero `identify(user.id)` matches. `_do_logout` at :4109 calls `telemetry.reset_identity()`. Login/register success blocks call `_sync_telemetry_identity()`. `telemetry.identify()` internally uses `$anon_distinct_id = install_id`. |
| 1 | Session-start fires once per process, after identity resolves, carrying only allowlisted env props — never hostname/username/exe/cwd | VERIFIED | Coordinator at `genizah_app.py:3604` calls `_sync_telemetry_identity()` unconditionally before the `_telemetry_session_started` one-shot guard. Session_start props: `session_id`, `ui_language`, `python_version`, `pyqt_version` (plus `_BASE_PROPS()` = `platform`, `app_version`, `os_family`, `os_version`). `_BASE_PROPS()` at `telemetry.py:354-367` contains no hostname/username/exe/cwd. 75 tests pass including `test_coordinator_session_start_props_allowlisted`. |
| 2 | Feature usage events capture tabs and key surfaces as counts; no free-text/content properties | VERIFIED | `_on_tab_changed` uses `_TAB_NAME_MAP` (hardcoded 7-entry dict, never `tabText()`). `_emit_feature_opened` at `genizah_app.py:3672` carries only hardcoded constants: `joins_lab`, `fragment_puzzle`, `fjms_catalog`, `result_detail`, `visual_similarity`, `export`, `export_xlsx/csv/txt/docx`. Both puzzle open paths, both FJMS open paths, and the live VS path (source='visual' branch in `open_joins_workbench`) are wired. AST guard `test_no_dynamic_telemetry_strings.py` passes — no forbidden accessor in any telemetry call argument. |
| 3 | Search executions captured with search_mode and corpus_scope as fixed enums; query text/filter/exclusion structurally absent | VERIFIED | `_emit_search_telemetry`, `_emit_pgp_tag_search_telemetry`, `_emit_comp_search_telemetry` each build props from hardcoded `_SEARCH_MODE_ENUM`/`_COMP_SEARCH_MODE_ENUM` maps and `currentData()` (fixed codes). PGP-tags path (which bypasses `start_search`) has its own helper at :19032 with hardcoded `'pgp_tags'` — the `tag` argument is never stored in props. `corpus_scope_combo.currentText()` appears at lines 20890 and 21317 for non-telemetry display use; confirmed absent from any `track()` call argument. All 3 emitters tested for zero content leakage. |
| 4 | Every event carries base props (platform=desktop, $process_person_profile=false, app_version) through single shared _emit() helper; no callsite bypasses | VERIFIED | `_BASE_PROPS()` at `telemetry.py:354-367` injects `platform='desktop'`, `app_version`, `os_family`, `os_version` into every event through `_emit()` at :642 and :787. `$process_person_profile` is injected by `_emit()` conditionally per identity state. All Phase 114 producers route through `telemetry.track()` → `_emit()`. No raw `enqueue_event` calls in phase 114 producers (existing `test_telemetry_no_direct_posthog.py` guard passes). |
| 5 | Exactly one telemetry session_id per process; all timestamps UTC; performance durations use monotonic clock; crash-restart begins fresh session without duplicate session-start | VERIFIED | `self._session_id = uuid.uuid4().hex` at coordinator :3634 is one per process; `_telemetry_session_started` one-shot guard prevents duplicate. `datetime.now(timezone.utc)` used at :3636-3637 and in `_maybe_emit_active_ping`. Crash path: no closeEvent runs on crash, so no session_end fires and next launch runs a fresh coordinator minting a new session_id. Test `test_coordinator_idempotent_session_start` passes. |

**Score:** 6/6 truths verified

### Deferred Items

None identified.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `desktop/telemetry.py` | ACTIVE_PING enum member | VERIFIED | `ACTIVE_PING = 'desktop_active_ping'` at :159; `_VALID_EVENT_VALUES` auto-rebuilds at import |
| `genizah_app.py` | `_run_startup_telemetry_coordinator` + `_sync_telemetry_identity` + `_telemetry_ready` + session_end + login/logout wiring + all usage producers | VERIFIED | All methods exist and are substantive: coordinator :3604, identity :3579, ready :3660, closeEvent shutdown :26847, login :4087, logout :4109, tab :4024, search emitters :17604/:19032/:22771, feature :3672, heartbeat :3705 |
| `tests/test_telemetry_phase114.py` | 70 tests across all producers | VERIFIED | 70 tests exist, all pass in 1.34s |
| `tests/test_no_dynamic_telemetry_strings.py` | AST guard (D-17) | VERIFIED | 5 tests including production scan; all pass |
| `desktop/result_dialog.py` | `result_detail` in `__init__` + `fjms_catalog` in `_show_rd_catalog` | VERIFIED | `:53-61` and `:2893-2901` confirmed |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `on_startup_finished` | `_run_startup_telemetry_coordinator` | `QTimer.singleShot(700, ...)` | VERIFIED | `genizah_app.py:3570` |
| `_run_startup_telemetry_coordinator` | `_sync_telemetry_identity` | unconditional before session_start guard | VERIFIED | `genizah_app.py:3623` (before `_telemetry_session_started` check at :3626) |
| `_sync_telemetry_identity` | `telemetry.identify` | `user._uuid` | VERIFIED | `genizah_app.py:3598`; zero `identify(user.id)` matches |
| `closeEvent` | `telemetry.track(SESSION_END)` | `_session_end_emitted` guard | VERIFIED | `genizah_app.py:26847-26856` |
| `_on_tab_changed` | `telemetry.track(TAB_ACTIVATED)` | `_programmatic_tab_change` + `_restoring_session` + `_telemetry_ready()` | VERIFIED | `genizah_app.py:4029-4051`; `_set_active_tab` helper wraps all programmatic jumps |
| `start_search` | `_current_search_run` | per-run state, `_emit_search_telemetry` | VERIFIED | `:17502-17506`; emit in `stop_search` :17652 and `on_search_finished` :18088/:18221 |
| `_execute_tag_search` / `_on_tag_search_results` | `_emit_pgp_tag_search_telemetry` | per-run object; tag text never in props | VERIFIED | `:19078`; emitted in all 3 outcome branches :19091/:19111/:19167 |
| `run_composition` | `_emit_comp_search_telemetry` | per-run object; `on_comp_scan_finished` | VERIFIED | `:22905-22909`; emitted at `on_comp_scan_finished` |
| `open_joins_workbench (source in visual/combined)` | `telemetry.track(FEATURE_OPENED, dialog_name='visual_similarity')` | source guard in LIVE path | VERIFIED | `genizah_app.py:15960-15961`; dead `_browse_view_visual_similarity` not instrumented |
| `ResultDialog.__init__` | `telemetry.track(FEATURE_OPENED, dialog_name='result_detail')` | direct (no `_telemetry_ready` gate — see WR-01) | WIRED (WARNING) | `result_dialog.py:53-61`; bypasses `_telemetry_ready()` gate — see Warnings |

### Data-Flow Trace (Level 4)

All events route through `telemetry.track()` → `_emit()` → `_BASE_PROPS()` + scrubber + `enqueue_event()`. The producer chain is statically verifiable; no dynamic data sources involved in event construction (all values are hardcoded constants, currentData() codes, or uuid4 hex strings).

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| All 75 telemetry tests pass | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py tests/test_no_dynamic_telemetry_strings.py -q` | 75 passed, 1 warning in 1.34s | PASS |
| No `identify(user.id)` in codebase | `grep -nE "identify\(\s*user\.id" genizah_app.py` | zero matches | PASS |
| Session_start props no forbidden keys | `test_coordinator_session_start_props_allowlisted` | passes | PASS |
| Ruff clean | `python -m ruff check desktop/telemetry.py genizah_app.py desktop/result_dialog.py tests/test_*.py` | no output (clean) | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| USAGE-01 | Plan 01 | Session-start event records ONLY allowlisted env props | SATISFIED | `_run_startup_telemetry_coordinator` emits only session_id/ui_language/python_version/pyqt_version + `_BASE_PROPS()`; no hostname/username/path; test `test_coordinator_session_start_props_allowlisted` |
| USAGE-02 | Plans 02/03 | Feature usage captured as counts — tabs/surfaces, no free-text | SATISFIED | `_on_tab_changed` hardcoded map; `_emit_feature_opened` hardcoded constants; AST guard prevents forbidden accessors in telemetry args |
| USAGE-03 | Plan 02 | Search mode + corpus as enums, never query text | SATISFIED | All 3 search emitters (regular/PGP-tags/comp) use hardcoded enum maps and `currentData()`; query text never in props |
| USAGE-04 | Plan 03 | Active-user/session signal enabling DAU/MAU | SATISFIED | `_maybe_emit_active_ping` fires at most once per UTC day, active-only, not on launch day; 5-min QTimer + `applicationStateChanged` |
| USAGE-05 | Plan 01 (telemetry.py infra) | Base props (platform=desktop, app_version) through shared helper | SATISFIED | `_BASE_PROPS()` injected by `_emit()` into every event; verified by existing `test_telemetry_identity.py`/`test_telemetry_consent_gate.py` |
| USAGE-06 | Plan 01 | Session/clock correctness — one session_id/process, UTC timestamps, crash-restart fresh session | SATISFIED | `uuid4().hex` one per process; `_telemetry_session_started` one-shot; `datetime.now(timezone.utc)` throughout |
| IDENT-01 | Plan 01 | Logged-in user identified with Supabase UUID (same as web) | SATISFIED | `telemetry.identify(user._uuid)` — `_uuid` is the raw Supabase UUID string, same field web uses; zero `.id` (int hash) usage |
| IDENT-02 | Plan 01 | Anon events aliased to account on login; reset to anon on logout | SATISFIED | `telemetry.identify()` uses `$anon_distinct_id = install_id` (alias); `telemetry.reset_identity()` called on logout |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `desktop/result_dialog.py` | 53-61, 2893-2901 | `telemetry.track()` called WITHOUT `_telemetry_ready()` gate | WARNING | WR-01: A ResultDialog opened in the ~700ms startup window before the coordinator fires will emit `desktop_feature_opened` with `session_id=''` and before `desktop_session_start` has fired — an orphaned event PostHog cannot attach to a session. Privacy-safe (consent-gated), but breaks session ordering. |
| `genizah_app.py` | 22905-22909 | `_current_comp_search_run` assigned before the `lab_engine` / `searcher` validation guards at :22912-22916 | WARNING | WR-02: A composition that fails the pre-flight guard silently records no telemetry (the dangling run object is never emitted — harmless for counts but invisible in analytics). Not a privacy defect. |
| `desktop/consent_dialog.py` | 307, 339 | Disclosure text says `user.id` but code sends `user._uuid` | WARNING | WR-04: Consent text inaccuracy on a compliance-sensitive surface. Code behavior is correct (`_uuid` is the privacy-correct pseudonymous UUID); only the EN/HE disclosure text names the wrong field. |
| `genizah_app.py` | 17486-17489 | `_SEARCH_MODE_ENUM` maps index `7: 'pgp_tags'` — unreachable (PGP-tags dispatches via `_execute_tag_search` before `start_search` is called) | INFO | Dead entry; cosmetic only. Not harmful. |
| `desktop/telemetry.py` | 302 | `duration_bucket_ms` allowlisted but no Phase 114 producer emits it | INFO | Forward-provisioned for Phase 115; harmless dead surface area. |

### Human Verification Required

### 1. Live PostHog Event Delivery

**Test:** Opt the desktop app in to telemetry, log in with a Supabase account, let the startup coordinator fire (~700ms after launch). Open PostHog and verify:
- A `desktop_session_start` event appears with `distinct_id` equal to the user's Supabase UUID
- Props contain only: `app_version`, `os_family`, `os_version`, `python_version`, `pyqt_version`, `ui_language`, `session_id` — NOT hostname, username, executable path, or working directory
- The same `distinct_id` matches the web session for the same user

**Expected:** `desktop_session_start` visible in PostHog, attached to the merged person profile that includes web activity for the same user.
**Why human:** End-to-end PostHog delivery requires a live desktop session with real credentials and network connectivity — cannot be verified by grep or unit tests.

### 2. Consent Disclosure Accuracy (WR-04)

**Test:** Review `desktop/consent_dialog.py` lines 306-308 (EN) and 339-340 (HE). The text currently says "bare Supabase `user.id`". The code sends `user._uuid` (the raw UUID string). Decide: fix the disclosure text, or accept the existing wording as-is with a documented rationale.

**Expected:** Consent text accurately describes the identifier (UUID string, same as the web app's PostHog identity), OR a decision is documented that the existing text is acceptable.
**Why human:** This is a compliance/disclosure judgment call — whether `user.id` in the consent text is close enough to `user._uuid` in the implementation. The behavior is privacy-correct; only the user-facing text is potentially misleading.

### Gaps Summary

No BLOCKER gaps found. All 6 must-have truths are VERIFIED against the actual codebase. Tests pass (75/75). Ruff clean.

Two WARNING gaps that do not block the phase goal:

**WR-01 (result_dialog telemetry bypasses `_telemetry_ready()` gate):** The two `telemetry.track()` calls in `desktop/result_dialog.py` — `result_detail` in `__init__` and `fjms_catalog` in `_show_rd_catalog` — emit directly without checking `_telemetry_ready()`. A ResultDialog opened in the ~700ms startup window will produce an orphaned event with `session_id=''`. This is a correctness gap (events unattachable to a session) but NOT a privacy or goal-blocking defect. The code-review fix is to route both calls through `app._emit_feature_opened(...)` which owns the `_telemetry_ready()` gate.

**WR-04 (consent dialog text says `user.id`, code sends `user._uuid`):** The user-facing disclosure text in both EN and HE mentions `user.id`. The code correctly sends `user._uuid` (the raw Supabase UUID). Since the REQUIREMENTS.md IDENT-01 wording says `distinct_id = Supabase user.id` (using the PostgreSQL field name which IS the UUID), the behavior satisfies the requirement; only the disclosure text has an accuracy gap. Requires human decision on whether to update the text.

---

_Verified: 2026-06-15T22:00:00Z_
_Verifier: Claude (gsd-verifier)_
