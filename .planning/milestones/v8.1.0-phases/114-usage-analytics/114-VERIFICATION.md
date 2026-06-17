---
phase: 114-usage-analytics
verified: 2026-06-16T10:00:00Z
status: human_needed
score: 6/6
overrides_applied: 0
re_verification:
  previous_status: human_needed
  previous_score: 6/6
  gaps_closed:
    - "CR-114-01: PGP-tag per-run token BOUND INTO the .finished connect lambda; _on_tag_search_results threads it to _emit_pgp_tag_search_telemetry, which early-returns when the slot-bound token != live _pgp_tag_active_token (REACHABLE guard — Codex gap-review BLOCKER on the initial dead-code run['token'] compare resolved in commit 7e7236b7)"
    - "CR-114-02: _reset_search sets _search_was_cancelled=True + emits _emit_search_telemetry('cancelled') in the isRunning branch, mirroring stop_search"
    - "CR-114-03: _reset_composition emits _emit_comp_search_telemetry('cancelled') guarded by the emitted flag"
    - "CR-114-04: closeEvent SESSION_END gated on _telemetry_ready() AND truthy _session_id"
    - "CR-114-05: open_join_workbench(emit_telemetry=...) param; _restore_join_lab passes False"
    - "CR-114-06: interrupted-composition resume uses _set_active_tab(self.composition_tab)"
    - "WR-04: consent_dialog.py EN+HE disclosure reworded to 'Supabase account identifier (a UUID)' (commit f7bf67e4)"
  gaps_remaining: []
  regressions: []
human_verification:
  - test: "Opt the desktop app in to telemetry, log in with a Supabase account, then open PostHog and confirm a desktop_session_start event appears with distinct_id equal to the user's Supabase UUID (not an int hash), no hostname/username/path props present."
    expected: "Event appears in PostHog with correct distinct_id and only app_version, os_family, os_version, python_version, pyqt_version, ui_language, session_id props."
    why_human: "End-to-end event transmission to the PostHog EU project requires a live desktop session with valid credentials and real network access — not verifiable by grep or unit tests."
---

# Phase 114: Usage Analytics Verification Report

**Phase Goal:** The desktop app emits allowlisted usage events (session start/end, tab/surface activations, search mode and corpus enums) that enable DAU/MAU, version adoption, and feature-use measurement in PostHog — with no query content, no My Library data, and no environment identifiers beyond OS family/version.
**Verified:** 2026-06-16T10:00:00Z
**Status:** human_needed
**Re-verification:** Yes — after gap closure (plan 114-04, CR-114-01..06 + WR-04)

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 0 | On login desktop calls identify(distinct_id = user._uuid), aliases prior anon uuid; logout resets to anon id; only uuid sent, never email/name | VERIFIED | `genizah_app.py:3598` calls `telemetry.identify(user._uuid)` — `grep -n "identify(user.id)"` returns zero matches. `_do_logout` calls `telemetry.reset_identity()`. No regression from 114-04. |
| 1 | Session-start fires once per process, after identity resolves, carrying only allowlisted env props — never hostname/username/exe/cwd | VERIFIED | Coordinator at `:3604` calls `_sync_telemetry_identity()` before the `_telemetry_session_started` one-shot guard. Session_start props: `session_id`, `ui_language`, `python_version`, `pyqt_version` + `_BASE_PROPS()`. No regression from 114-04. 94 tests pass. |
| 2 | Feature usage events capture tabs and key surfaces as counts; no free-text/content properties | VERIFIED | `_on_tab_changed` uses `_TAB_NAME_MAP` (hardcoded 7-entry dict). `_emit_feature_opened` at `:3672` carries only hardcoded constants. CR-114-05 (+CR-114-06) fixed ghost events from restore/comp-resume. AST guard `test_no_dynamic_telemetry_strings.py` passes. |
| 3 | Search executions captured with search_mode and corpus_scope as fixed enums; query text/filter/exclusion structurally absent | VERIFIED | `_emit_search_telemetry`, `_emit_pgp_tag_search_telemetry`, `_emit_comp_search_telemetry` build props from hardcoded maps. CR-114-01 adds per-run token guard. CR-114-02/03 add cancelled-emit on reset paths. No content leakage path exists. |
| 4 | Every event carries base props (platform=desktop, $process_person_profile=false, app_version) through single shared _emit() helper; no callsite bypasses | VERIFIED | `_BASE_PROPS()` at `telemetry.py:354-367` injects `platform='desktop'`, `app_version`, `os_family`, `os_version` into every event through `_emit()`. No bypass introduced by 114-04. |
| 5 | Exactly one telemetry session_id per process; all timestamps UTC; performance durations use monotonic clock; crash-restart begins fresh session without duplicate session-start | VERIFIED | `self._session_id = uuid.uuid4().hex` at coordinator `:3634` is one per process. CR-114-04 ensures session_end is only emitted when `_telemetry_ready()` AND `_session_id` is truthy — eliminating the orphan `session_id=''` path. |

**Score:** 6/6 truths verified

### Gap Closure — Plan 114-04 (CR-114-01..06)

All six Codex cross-AI code-review findings are closed. Confirmed by direct code inspection and 95 passing tests. **NOTE:** a second adversarial gap-review (`114-REVIEW-GAP.md`) found the *initial* CR-114-01 fix was dead code (BLOCKER); it was corrected — see the CR-114-01 row.

| Finding | Verified Present | Evidence |
|---------|-----------------|----------|
| CR-114-01: PGP-tag run token + worker drain | VERIFIED (corrected) | Token bound into the `.finished` connect lambda (`t=_tel_tok`); `_on_tag_search_results(..., token)` threads it to `_emit_pgp_tag_search_telemetry(..., token=...)`, which early-returns when the slot-bound `token != _pgp_tag_active_token`. Drain + `finished.disconnect()` (all slots) precede the new run install. **Gap-review BLOCKER (`114-REVIEW-GAP.md`): the initial impl compared the LIVE run's own `run['token']` to the active token — always equal, dead code; a stale slot could still mark the new run emitted. Fixed in commit `7e7236b7`** with the slot-bound token + a real stale-slot regression test (was vacuous) + an end-to-end wiring source-check. |
| CR-114-02: _reset_search cancelled emit | VERIFIED | `_search_was_cancelled = True` at `:17691` (inside `isRunning()` branch). `_emit_search_telemetry('cancelled')` at `:17701`. Mirrors `stop_search` at `:17649`/`:17659`. |
| CR-114-03: _reset_composition cancelled emit | VERIFIED | `_emit_comp_search_telemetry('cancelled')` at `:22711` inside the `isRunning()` branch of `_reset_composition`. `on_comp_scan_finished` call at `:23173` still present. |
| CR-114-04: session_end gate | VERIFIED | `closeEvent` at `:26901-26904` now guards with `self._telemetry_ready() and getattr(self, '_session_id', '') and not getattr(self, '_session_end_emitted', False)`. `_session_end_emitted` set only after gate passes. |
| CR-114-05: restore-suppressed joins_lab feature_opened | VERIFIED | `open_join_workbench` signature at `:15915` now `(self, *, emit_telemetry: bool = True)`. Emit gated at `:15931`. `_restore_join_lab` calls `self.open_join_workbench(emit_telemetry=False)` at `:26821`. |
| CR-114-06: comp-resume programmatic tab switch | VERIFIED | `_set_active_tab(self.composition_tab)` at `:26884` in interrupted-comp resume. `grep "self.tabs.setCurrentWidget(self.composition_tab)"` returns 0 matches anywhere in the file. |

**WR-04 (consent disclosure):** `desktop/consent_dialog.py` EN (lines 307-308) and HE (lines 339-340) now read "Supabase account identifier (a UUID)" / "מזהה החשבון שלכם ב-Supabase (מסוג UUID)". Confirmed in code. HUMAN-UAT item 2 marked `passed` at commit `f7bf67e4`.

### Deferred Items

None identified.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `desktop/telemetry.py` | ACTIVE_PING enum member | VERIFIED | `ACTIVE_PING = 'desktop_active_ping'` at :159; no regression |
| `genizah_app.py` | All 114-01/02/03 producers + all 6 CR-114-04 fixes | VERIFIED | All CR-114-01..06 confirmed present; all original producers intact |
| `tests/test_telemetry_phase114.py` | 89 tests (76 original + 13 new regression tests) | VERIFIED | 89 tests collected; 94 total with AST guard suite; all pass in 1.37s |
| `tests/test_no_dynamic_telemetry_strings.py` | AST guard (D-17) | VERIFIED | 5 tests pass; no forbidden accessor introduced by 114-04 |
| `desktop/consent_dialog.py` | Accurate UUID disclosure (WR-04) | VERIFIED | Lines 307-308 (EN) and 339-340 (HE) corrected |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `on_startup_finished` | `_run_startup_telemetry_coordinator` | `QTimer.singleShot(700, ...)` | VERIFIED | `genizah_app.py:3570` — unchanged |
| `_run_startup_telemetry_coordinator` | `_sync_telemetry_identity` | unconditional before session_start guard | VERIFIED | `genizah_app.py:3623` — unchanged |
| `_sync_telemetry_identity` | `telemetry.identify` | `user._uuid` | VERIFIED | `genizah_app.py:3598`; zero `identify(user.id)` matches |
| `closeEvent` | `telemetry.track(SESSION_END)` | `_telemetry_ready() AND _session_id AND _session_end_emitted guard` | VERIFIED (CR-114-04) | `genizah_app.py:26901-26910`; now gated, no orphan session_id='' |
| `_execute_tag_search` | `_current_pgp_tag_search_run` (token-guarded) | per-run token; previous worker drained before run install | VERIFIED (CR-114-01) | Token at :19107-19115; drain/disconnect at :19099-19105 |
| `_reset_search` | `_emit_search_telemetry('cancelled')` | `_search_was_cancelled=True` + emit in `isRunning()` branch | VERIFIED (CR-114-02) | :17691 + :17701 |
| `_reset_composition` | `_emit_comp_search_telemetry('cancelled')` | emit in `isRunning()` branch, guarded by `emitted` flag | VERIFIED (CR-114-03) | :22711 |
| `_restore_join_lab` → `open_join_workbench` | `_emit_feature_opened` (suppressed) | `emit_telemetry=False` param | VERIFIED (CR-114-05) | :26821 calls `open_join_workbench(emit_telemetry=False)` |
| interrupted-comp resume | `_set_active_tab(self.composition_tab)` | programmatic tab switch helper | VERIFIED (CR-114-06) | :26884; bare `setCurrentWidget` gone (0 matches) |

### Data-Flow Trace (Level 4)

All events route through `telemetry.track()` → `_emit()` → `_BASE_PROPS()` + scrubber + `enqueue_event()`. Producer chain is statically verifiable. No dynamic data sources in event construction. No regression from 114-04 (all new emit sites use hardcoded constants 'cancelled', 'pgp_tags', 'genizah').

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| All 94 telemetry tests pass | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py tests/test_no_dynamic_telemetry_strings.py -q` | 94 passed, 1 warning in 1.37s | PASS |
| No `identify(user.id)` in codebase | grep pattern in genizah_app.py | zero matches | PASS |
| PGP-tag token guard present | `grep "_pgp_tag_active_token" genizah_app.py` | matches at :19069-19115 | PASS |
| _reset_search cancelled emit | `grep "_emit_search_telemetry\('cancelled'\)"` | matches at :17659 (stop_search) + :17701 (_reset_search) | PASS |
| _reset_composition cancelled emit | `grep "_emit_comp_search_telemetry\('cancelled'\)"` | matches at :22711 (_reset_composition) + :23173 (on_comp_scan_finished) | PASS |
| session_end gate | closeEvent block at :26901-26904 contains `_telemetry_ready()` AND `_session_id` check | confirmed | PASS |
| restore-suppress joins_lab | `grep "open_join_workbench(emit_telemetry=False)"` | match at :26821 | PASS |
| comp-resume programmatic tab | `grep "_set_active_tab(self.composition_tab)"` | match at :26884; bare setCurrentWidget → 0 matches | PASS |
| D-17 AST guard still green | `test_no_dynamic_telemetry_strings.py` | 5 passed | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| USAGE-01 | Plan 01 | Session-start event records ONLY allowlisted env props | SATISFIED | No regression; CR-114-04 adds session_end gating as the complement invariant |
| USAGE-02 | Plans 02/03 | Feature usage captured as counts — tabs/surfaces, no free-text | SATISFIED | CR-114-05 eliminates ghost joins_lab; CR-114-06 eliminates ghost tab_activated on comp-resume |
| USAGE-03 | Plan 02 | Search mode + corpus as enums, never query text | SATISFIED | CR-114-01 adds token guard for PGP-tag count accuracy; CR-114-02/03 add cancelled-emit on reset paths |
| USAGE-04 | Plan 03 | Active-user/session signal enabling DAU/MAU | SATISFIED | No regression |
| USAGE-05 | Plan 01 | Base props through shared helper | SATISFIED | No regression |
| USAGE-06 | Plan 01 | Session/clock correctness | SATISFIED | CR-114-04 closes the orphan session_end gap |
| IDENT-01 | Plan 01 | Logged-in user identified with Supabase UUID | SATISFIED | `user._uuid` unchanged; zero `identify(user.id)` |
| IDENT-02 | Plan 01 | Anon events aliased to account on login; reset to anon on logout | SATISFIED | No regression |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `genizah_app.py` | 22905-22909 | `_current_comp_search_run` assigned before validation guards at :22912-22916 | WARNING | WR-02 (pre-existing): a composition that fails the pre-flight guard silently records no telemetry. Not a privacy defect; harmless for counts. Unchanged by 114-04. |
| `genizah_app.py` | 17486-17489 | `_SEARCH_MODE_ENUM` maps index `7: 'pgp_tags'` — unreachable | INFO | Dead entry (pre-existing); cosmetic only. Unchanged by 114-04. |
| `desktop/telemetry.py` | 302 | `duration_bucket_ms` allowlisted but no Phase 114 producer emits it | INFO | Forward-provisioned for Phase 115 (pre-existing). Harmless. |

**Previously open WR-01 (ResultDialog bypasses `_telemetry_ready()` gate):** CLOSED prior to 114-04 via commit `5c6970c8` — all six `ResultDialog(...)` construction sites pass `self` so `self._app._emit_feature_opened(...)` resolves to the gated host helper. Confirmed by Codex review.

**Previously open WR-04 (consent disclosure):** CLOSED by commit `f7bf67e4` — EN/HE now say "Supabase account identifier (a UUID)". Confirmed in code.

### Human Verification Required

### 1. Live PostHog Event Delivery

**Test:** Opt the desktop app in to telemetry, log in with a Supabase account, let the startup coordinator fire (~700ms after launch). Open PostHog and verify:
- A `desktop_session_start` event appears with `distinct_id` equal to the user's Supabase UUID
- Props contain only: `app_version`, `os_family`, `os_version`, `python_version`, `pyqt_version`, `ui_language`, `session_id` — NOT hostname, username, executable path, or working directory
- The same `distinct_id` matches the web session for the same user

**Expected:** `desktop_session_start` visible in PostHog, attached to the merged person profile that includes web activity for the same user.
**Why human:** End-to-end PostHog delivery requires a live desktop session with real credentials and network connectivity — cannot be verified by grep or unit tests.

### Gaps Summary

No BLOCKER or WARNING gaps remain.

All six Codex findings (CR-114-01..06) are closed and verified in code. WR-01 (ResultDialog gate) was closed before 114-04. WR-04 (consent disclosure) was closed by commit f7bf67e4.

The sole remaining open item is the live PostHog event delivery HUMAN-UAT test (requires a real desktop session with network). Status is `human_needed` solely because of this one pending end-to-end network verification — all automated checks pass (94/94 tests).

---

_Verified: 2026-06-16T10:00:00Z_
_Verifier: Claude (gsd-verifier)_
_Re-verification: initial 2026-06-15T22:00:00Z (6/6, human_needed); re-verified 2026-06-16 after plan 114-04 gap closure_
