---
phase: 112-consent-ux
verified: 2026-06-15T08:30:00Z
status: passed
score: 5/5
overrides_applied: 0
---

# Phase 112: Consent UX Verification Report

**Phase Goal:** The user can give or withdraw consent through a bilingual first-run dialog (shown exactly once, on first launch after updating to v8.1.0) and a Settings/About toggle; opting out immediately drains and discards any already-queued events; a bilingual privacy disclosure is reachable from both surfaces.
**Verified:** 2026-06-15T08:30:00Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | First-launch bilingual EN/HE modal, two equal-weight buttons, no default, Enter routes to decline | VERIFIED | `ConsentDialog`: both buttons call `setDefault(False)` + `setAutoDefault(False)`; `keyPressEvent` intercepts `Key_Return`/`Key_Enter` → `_on_decline()`. Test `test_consent_dialog_no_default_button` + `test_consent_dialog_enter_is_decline` pass (34/34 green). |
| 2 | Shown exactly once: `FIRST_RUN_SHOWN_KEY` written unconditionally on every exit path | VERIFIED | Single `done()` finalizer (lines 201-209 of `consent_dialog.py`) writes `genizah_core.save_app_config({FIRST_RUN_SHOWN_KEY: True})` unconditionally before `set_consent`. Gate in `show_first_run_prompt()` (telemetry.py:725) returns early if flag is truthy. Gate doubled in `_maybe_show_first_run_prompt()` (genizah_app.py:15847). Tests `test_done_finalizer_writes_flag_on_{accept,decline,escape,close}` and `test_first_run_gate_skips_if_shown` pass. |
| 3 | Stored consent record has timestamp + app version + consent-UI version | VERIFIED | `set_consent(True)` in `desktop/telemetry.py` lines 435-437 writes `CONSENT_TIMESTAMP_KEY` (ISO-8601), `CONSENT_APP_VERSION_KEY` (_APP_VERSION), `CONSENT_UI_VERSION_KEY='1'`. ConsentDialog calls `set_consent(True)` on accept via `done()` finalizer. |
| 4 | Settings/About toggle reads/writes same key as dialog; opting out drains queued events | VERIFIED | `chk_telemetry` initial state from `is_enabled()` (blocked signals); handler calls `set_consent(new_val)` only on confirm-Yes (no raw `save_app_config({'telemetry_enabled'...})`). `_open_settings_dialog` refreshes checkbox before every `exec()` (REVIEWS HIGH-3). `set_consent(False)` calls `_drain_and_discard()` (CONSENT-08). Tests `test_settings_toggle_applies_on_confirm`, `test_settings_toggle_reverts_on_cancel_confirm`, `test_settings_checkbox_refreshes_on_open`, `test_optout_drains_queue`, `test_settings_cancel_does_not_desync_telemetry` all pass. |
| 5 | Bilingual privacy disclosure reachable from BOTH first-run "Learn more" AND Settings "Privacy details" (and About) | VERIFIED | `ConsentDialog._on_learn_more()` opens `PrivacyDialog(self).exec()`. `_build_general_tab` has a flat "Privacy details" button wired to `PrivacyDialog(self).exec()`. About tab has a language-agnostic bilingual block + `btn_privacy_about` wired to `PrivacyDialog(self).exec()`. `PrivacyDialog._build_html()` contains `dir='ltr'` + `dir='rtl'` blocks, 'PostHog', 'My Library', 'privacy-preserving'. Test `test_privacy_dialog_constructs_bilingual` passes. |

**Score:** 5/5 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `desktop/consent_dialog.py` | ConsentDialog (single done() finalizer, bilingual, no-default buttons) + PrivacyDialog (bilingual disclosure) | VERIFIED | 351 lines; contains `class ConsentDialog(QDialog)` and `class PrivacyDialog(QDialog)`; parse-ok |
| `tests/test_telemetry_consent_ux.py` | Headless gate-logic + done()-finalizer + opt-out-drain + Qt-offscreen tests | VERIFIED | 756 lines; defines all 12+ required test functions; 34/34 pass |
| `desktop/telemetry.py` | `show_first_run_prompt(parent=None)` gates on `FIRST_RUN_SHOWN_KEY`, lazy-imports + execs `ConsentDialog`, never raises | VERIFIED | Lines 712-731; try/except wraps whole body; lazy `from desktop.consent_dialog import ConsentDialog` inside function |
| `genizah_app.py` | `_maybe_show_first_run_prompt` + `activeModalWidget` reschedule guard + `on_startup_finished` else-branch + chk_telemetry + snapshot exemption + About bilingual block | VERIFIED | All wiring confirmed; 4 occurrences of `_maybe_show_first_run_prompt`; `activeModalWidget` appears in method; `_TELEMETRY_SNAPSHOT_EXCLUDE` strips 7 keys |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `ConsentDialog.done()` | `desktop.telemetry.set_consent` | `telemetry.set_consent(True if self._accepted_telemetry else False)` in single finalizer | WIRED | `telemetry.set_consent` called in `done()` — opt-in only on explicit Enable click |
| `ConsentDialog.done()` | `genizah_core.save_app_config (FIRST_RUN_SHOWN_KEY)` | `genizah_core.save_app_config({FIRST_RUN_SHOWN_KEY: True})` unconditionally | WIRED | Module-attribute access (not from-import) — test monkeypatch intercepts correctly |
| `genizah_app.py::on_startup_finished` | `GenizahGUI._maybe_show_first_run_prompt` | `QTimer.singleShot(500, self._maybe_show_first_run_prompt)` in else-branch (citation already seen) | WIRED | genizah_app.py:3505 |
| `genizah_app.py::_show_citation_reminder` | `GenizahGUI._maybe_show_first_run_prompt` | Direct call after `save_app_config({'citation_reminder_seen': True})` | WIRED | genizah_app.py:15829 |
| `genizah_app.py::_maybe_show_first_run_prompt` | `QApplication.activeModalWidget` reschedule guard | `if QApplication.activeModalWidget() is not None: QTimer.singleShot(300, self._maybe_show_first_run_prompt)` | WIRED | genizah_app.py:15849-15851 |
| `desktop/telemetry.py::show_first_run_prompt` | `desktop.consent_dialog.ConsentDialog` | Lazy import + `.exec()` inside try/except | WIRED | telemetry.py:727-729 |
| `genizah_app.py::SettingsDialog._on_telemetry_changed` | `desktop.telemetry.set_consent` | `_tel_set_consent(new_val)` on Yes confirm | WIRED | genizah_app.py:2372 |
| `genizah_app.py::GenizahGUI._open_settings_dialog` | `SettingsDialog.chk_telemetry` refresh from `is_enabled()` | `chk.blockSignals(True); chk.setChecked(_tel_is_enabled()); chk.blockSignals(False)` | WIRED | genizah_app.py:15730-15734 |
| `genizah_app.py::SettingsDialog.__init__` | telemetry key exemption | `_TELEMETRY_SNAPSHOT_EXCLUDE` frozenset of 7 keys strips them from `_config_snapshot` | WIRED | genizah_app.py:2190-2197 |
| `genizah_app.py::SettingsDialog (General tab + About tab)` | `desktop.consent_dialog.PrivacyDialog` | `Privacy details` flat button + About `btn_privacy_about` both wired to `PrivacyDialog(self).exec()` | WIRED | genizah_app.py:2385, 2658 |

### Data-Flow Trace (Level 4)

Not applicable — this phase produces consent UI dialogs and configuration state writes, not data-rendering components. All key data flows (consent flag write, queue drain) verified through test execution.

### Behavioral Spot-Checks (Test Execution)

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Full telemetry consent UX test suite | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry_consent_ux.py tests/test_telemetry_no_direct_posthog.py tests/test_telemetry_consent_gate.py -q` | 34 passed, 1 warning in 1.04s | PASS |
| PRIV-03 AST guard | `pytest tests/test_telemetry_no_direct_posthog.py -q` | 6 passed in 0.38s | PASS |
| All three modified files parse cleanly | `python -c "import ast; ast.parse(...)"` for consent_dialog.py, telemetry.py, genizah_app.py | parse-ok (all 3) | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| CONSENT-02 | 112-01, 112-02 | First-run bilingual dialog on first launch, explicit equal-weight yes/no | SATISFIED | `ConsentDialog` exists with two equal-weight buttons; `show_first_run_prompt()` wired into `on_startup_finished` and `_show_citation_reminder`; `test_first_run_constructs_and_execs_once` passes |
| CONSENT-03 | 112-01, 112-02 | Shown at most once; prompt-shown flag persists; timestamp/version audit trail | SATISFIED | Single `done()` finalizer writes `FIRST_RUN_SHOWN_KEY=True` unconditionally; `show_first_run_prompt()` gates on the flag; `set_consent(True)` writes `CONSENT_TIMESTAMP_KEY`, `CONSENT_APP_VERSION_KEY`, `CONSENT_UI_VERSION_KEY='1'`; all 4 done()-finalizer tests pass |
| CONSENT-04 | 112-03 | User can toggle telemetry from Settings/About at any time via same source of truth | SATISFIED | `chk_telemetry` in General tab reads `is_enabled()`, writes via `set_consent()`; confirm-on-change; reverts on cancel; refreshed on every open; `test_settings_toggle_*` tests pass |
| CONSENT-08 | 112-01 (verify-only) | Opt-out drains/discards queued events | SATISFIED | `set_consent(False)` calls `_drain_and_discard()` (Phase 111 implementation); `test_optout_drains_queue` proves queue empty after opt-out |
| PRIV-05 | 112-01, 112-03 | About/Help bilingual privacy disclosure with what IS/ISN'T collected, opt-in, how to turn off | SATISFIED | `PrivacyDialog` with EN+HE stacked disclosure covering all D-10 points; About tab has language-agnostic bilingual block below browser (REVIEWS HIGH-4 — renders for both EN and HE users); `PrivacyDialog` reachable from ConsentDialog "Learn more", Settings "Privacy details", and About "Privacy details" buttons |
| PRIV-03 | 112-01 (guard check) | AST guard for posthog_server.enqueue_event chokepoint | SATISFIED | `consent_dialog.py` has zero non-comment posthog_server references; `tests/test_telemetry_no_direct_posthog.py` passes 6/6 |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| — | — | None found | — | No TBD/FIXME/XXX/TODO/HACK/PLACEHOLDER markers in any phase-modified file |

**Wording compliance (D-11 / REVIEWS MED):**
- `consent_dialog.py`: no bare `anonymous` word; uses `privacy-preserving` (3×) and `pseudonymous` (1×)
- `genizah_app.py`: `anonymous usage data` occurrences = 0; `privacy-preserving` = 2; no raw `save_app_config({'telemetry_enabled'...})` = 0

**Hebrew copy note (best-effort, non-blocking per plan spec):** The Hebrew in `ConsentDialog` uses "מזהה פסאודו-אנונימי" (pseudo-anonymous identifier) where the English uses "pseudonymous". The plan explicitly flags Hebrew copy as best-effort / translation-workflow review. The `PrivacyDialog` Hebrew uses "פסאודו-אנונימי" throughout. This is a translation quality issue, not a structural or logic defect — classified as info-only.

### Human Verification Required

1. **First-run dialog appears once on fresh launch**

   **Test:** On a fresh desktop installation (or after deleting `telemetry_first_run_shown` from config.pkl), launch the app. Verify the ConsentDialog appears after the citation reminder closes. Close the app; relaunch. Verify the dialog does NOT appear again.
   **Expected:** Dialog appears exactly once; second launch suppresses it.
   **Why human:** Requires live app startup sequence with real Qt event loop — not testable headlessly without running the full app.

2. **activeModalWidget reschedule guard works in practice**

   **Test:** On first launch with no prior FIRST_RUN_SHOWN_KEY, quickly open Settings before the QTimer fires (500ms window). Verify the consent dialog reschedules itself behind Settings and appears after Settings closes, without stacking.
   **Expected:** No modal stacking; consent dialog defers gracefully.
   **Why human:** Timing-dependent; requires real event loop.

3. **Settings checkbox reflects correct post-first-run-dialog state**

   **Test:** On first launch, opt IN via the first-run dialog. Then open Settings → General → Preferences. Verify the "Help improve the app" checkbox is CHECKED (not stale from startup).
   **Expected:** Checkbox shows the post-consent state — CHECKED after opt-in.
   **Why human:** REVIEWS HIGH-3 stale-checkbox scenario; requires live first-run → Settings flow.

4. **Hebrew UI: both languages visible correctly in dialogs**

   **Test:** Switch app language to Hebrew; open Settings; verify the "Privacy details" button label and the disclosure block below the About browser show Hebrew text. Open the PrivacyDialog from there; verify Hebrew text is visible and right-to-left.
   **Expected:** Both EN and HE blocks render correctly in all three surfaces (ConsentDialog, PrivacyDialog, About tab).
   **Why human:** Hebrew rendering and RTL layout correctness requires visual inspection; cannot be validated by grep.

---

### Gaps Summary

No gaps found. All 5/5 success criteria are verified in the codebase. The 34-test telemetry subset passes, all source-level assertions hold, and all key links are wired. The 4 human verification items above are behavioral/visual checks appropriate for manual UAT — they do not block the goal.

---

_Verified: 2026-06-15T08:30:00Z_
_Verifier: Claude (gsd-verifier)_
