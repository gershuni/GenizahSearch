---
phase: 112-consent-ux
plan: "01"
subsystem: desktop-telemetry-ui
tags: [telemetry, consent, privacy, dialog, qt, bilingual]
dependency_graph:
  requires: [desktop/telemetry.py (Phase 111), genizah_core.save_app_config]
  provides: [desktop/consent_dialog.ConsentDialog, desktop/consent_dialog.PrivacyDialog, tests/test_telemetry_consent_ux.py]
  affects: [desktop/telemetry.show_first_run_prompt (Plan 02 fills stub)]
tech_stack:
  added: []
  patterns: [single-done()-finalizer, no-default-buttons, module-attr-monkeypatch, bilingual-EN-HE-stacked, Qt-offscreen-test-gating]
key_files:
  created:
    - desktop/consent_dialog.py
    - tests/test_telemetry_consent_ux.py
  modified: []
decisions:
  - "Single done() finalizer as sole exit path — no set_consent/flag-write in closeEvent/reject/accept (REVIEWS HIGH-1)"
  - "keyPressEvent intercepts Return/Enter → _on_decline so Enter can never silently opt in even with btn_enable focused (REVIEWS HIGH-2)"
  - "genizah_core.save_app_config called via module-attr access (not from-import) so test fixture monkeypatch intercepts dialog writes"
  - "Qt-offscreen close test uses reject() directly (close() on non-visible QDialog is Qt no-op; reject()→done() is the production close path)"
  - "PrivacyDialog uses neutral/factual disclosure voice; ConsentDialog uses personal first-person appeal (D-11)"
metrics:
  duration: "~25 minutes"
  completed: "2026-06-15"
  tasks: 2
  files: 2
---

# Phase 112 Plan 01: ConsentDialog + PrivacyDialog Foundation Summary

**One-liner:** Bilingual consent dialog with single `done()` finalizer (Enter-cannot-opt-in, all exit paths write shown-flag) and full bilingual privacy disclosure covering D-10 points using privacy-preserving/pseudonymous wording.

## What Was Built

### `desktop/consent_dialog.py` (350 lines, new)

Two dialog classes:

**`ConsentDialog(QDialog)`** — first-run bilingual consent prompt:
- Personal first-person appeal from the developer per D-11 with locked sign-off "— Hillel Gershuni, Dicta"
- Both EN and HE text always visible regardless of `CURRENT_LANG` (D-01)
- `self.btn_enable` and `self.btn_decline` — both have `setDefault(False)` + `setAutoDefault(False)` (T-112-EnterOptIn SC#1)
- `keyPressEvent` routes `Key_Return`/`Key_Enter` → `_on_decline()` (belt-and-braces on top of no-default buttons — REVIEWS HIGH-2)
- `_accepted_telemetry` flag — set `True` only on explicit Enable click
- **Single `done(result)` finalizer** (REVIEWS HIGH-1): writes `FIRST_RUN_SHOWN_KEY=True` unconditionally + calls `telemetry.set_consent(True if _accepted_telemetry else False)` — fires on accept/decline/Escape/X
- "Learn more" button opens `PrivacyDialog(self).exec()` (D-09)
- `genizah_core.save_app_config(...)` called via module-attribute access (not from-import) so test fixture monkeypatch intercepts correctly

**`PrivacyDialog(QDialog)`** — full bilingual disclosure (D-10):
- Palette-aware colours (SettingsDialog pattern)
- `QTextBrowser` with `setOpenExternalLinks(True)` and `_build_html()` rendering EN (`dir='ltr'`) + HE (`dir='rtl'`) stacked
- Covers: what IS collected (privacy-preserving usage counts, version/OS, perf buckets, crash signals); what is NOT (no search content, no My Library paths/filenames, no email/name beyond bare Supabase `user.id`); who processes (PostHog EU + Dicta with link); pseudonymous install id; opt-out via Settings
- Uses "privacy-preserving" and "pseudonymous" — NOT "anonymous" as the headline descriptor (REVIEWS MED + D-11)

### `tests/test_telemetry_consent_ux.py` (405 lines, new)

Two-section test suite:

**HEADLESS section** (no QApplication required):
- `test_first_run_gate_skips_if_shown` — FIRST_RUN_SHOWN_KEY=True skips ConsentDialog construction
- `test_first_run_constructs_and_execs_once` — authored Wave 0; goes GREEN after Plan 02 fills stub
- `test_done_finalizer_writes_flag_on_accept/decline/escape/close` — verifies shown-flag + set_consent on all exit paths
- `test_optout_drains_queue` — CONSENT-08 verification via engine's set_consent(False)→_drain_and_discard

**Qt-OFFSCREEN section** (gated via module-level QApplication guard):
- `test_consent_dialog_no_default_button` — both buttons isDefault/autoDefault = False
- `test_consent_dialog_enter_is_decline` — btn_enable focused + Return → set_consent(True) NOT called
- `test_consent_dialog_escape_opts_out` — Escape → FIRST_RUN_SHOWN_KEY True + set_consent(False)
- `test_consent_dialog_close_opts_out` — reject() (X-close path) → FIRST_RUN_SHOWN_KEY True + set_consent(False)
- `test_privacy_dialog_constructs_bilingual` — PostHog, My Library, privacy-preserving, dir=rtl, dir=ltr, no bare 'anonymous'

## Test Results

| Command | Result |
|---------|--------|
| `pytest tests/test_telemetry_consent_ux.py -k "done_finalizer or gate_skips or optout_drains" -x -q` | 6 passed |
| `pytest tests/test_telemetry_no_direct_posthog.py -x -q` | 6 passed |
| `pytest tests/test_telemetry_consent_ux.py -k "no_default or enter_is_decline or escape_opts_out or close_opts_out or privacy_dialog_constructs or done_finalizer" -x -q` | 9 passed |
| Full file | 11 passed, 1 expected-fail (`constructs_and_execs_once` — green after Plan 02) |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `close()` on hidden QDialog does not trigger `done()` in Qt**
- **Found during:** Task 2 verify
- **Issue:** Qt's `close()` only sends a `closeEvent` (→`reject()`→`done()`) to _visible_ widgets. A non-shown dialog's `close()` is a no-op. Both `test_done_finalizer_writes_flag_on_close` (headless) and `test_consent_dialog_close_opts_out` (Qt-offscreen) failed because they called `close()` on a non-visible dialog.
- **Fix:** Updated both tests to call `reject()` directly (which is the production code path that X-close triggers via `closeEvent`). The production `done()` finalizer correctly fires on `reject()`; the tests now document the close-event chain accurately. Comment added explaining this Qt behaviour.
- **Files modified:** `tests/test_telemetry_consent_ux.py`

## Known Stubs

- `desktop/telemetry.show_first_run_prompt()` remains a no-op until Plan 02. `test_first_run_constructs_and_execs_once` is marked as expected-fail until then. This is intentional per the plan (Wave 0 — scaffold first, fill second).

## Threat Flags

No new network endpoints, auth paths, file access patterns, or schema changes introduced. Both new files are pure UI/test code. PRIV-03 AST guard verified green (no `shared.posthog_server` import in `desktop/consent_dialog.py`).

## Notes

- **Hebrew copy is best-effort** and flagged for translation-workflow review (RESEARCH Open Question 2). The English copy is authoritative. Hebrew is present in both `ConsentDialog` and `PrivacyDialog` inline; no external translation file was modified.
- Consent and Settings copy uses **"privacy-preserving"** and **"pseudonymous"** throughout — NOT bare "anonymous" — per updated D-11 and REVIEWS MED wording constraint.
- The `constructs_and_execs_once` test is the only non-green test in the file; it is intentionally authored Wave 0 and will go green when Plan 02 implements `show_first_run_prompt()`.

## Self-Check: PASSED

Files verified:
- `desktop/consent_dialog.py` — FOUND (350 lines)
- `tests/test_telemetry_consent_ux.py` — FOUND (405 lines)

Commits verified:
- `3e23f4b3` — test(112-01): add Wave 0 consent UX test suite
- `77390514` — feat(112-01): add ConsentDialog + PrivacyDialog (bilingual, single done() finalizer)
