---
phase: 112
slug: consent-ux
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-14
revised: 2026-06-15
---

# Phase 112 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `112-RESEARCH.md` § Validation Architecture.
> Revised 2026-06-15 (REVIEWS pass): corrected the config-file drift (LOW — there is NO `pytest.ini`)
> and updated the test map to the REVIEWS-strengthened test names.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x (existing) |
| **Config file** | `pyproject.toml` `[tool.pytest.ini_options]` (NOT `pytest.ini` — there is no `pytest.ini`) |
| **Registered markers** | `slow`, `e2e`, `packaging`, `scale` (in `pyproject.toml`). `qt` is NOT registered — do NOT use `@pytest.mark.qt`; gate Qt tests via the `tests/conftest.py` offscreen pattern (`QApplication.instance() or QApplication([])`, skip-if-unavailable). |
| **Quick run command** | `pytest tests/test_telemetry_consent_ux.py -x -q` |
| **Full suite command** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry*.py -x` |
| **Estimated runtime** | ~30 seconds (telemetry subset) |

> **Windows note** (`feedback_full_suite_testing_windows.md`): full `pytest tests/` aborts on a
> non-deterministic PyQt6 headless segfault. Run Qt-dependent tests with
> `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen`. Headless tests (config.pkl gate logic,
> snapshot exemption, done()-finalizer shown-flag write) need NO Qt — keep them separable in the same file.
>
> **Shell note** (LOW — REVIEWS): the `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest …` prefix is
> Bash-style. The repo's interactive shell is PowerShell — set env vars there with
> `$env:QT_QPA_PLATFORM='offscreen'; $env:GITHUB_ACTIONS='true'` before `pytest`, or rely on
> `tests/conftest.py` (which already `setdefault`s `QT_QPA_PLATFORM=offscreen` on Linux-without-DISPLAY).
> The `pytest` commands themselves are shell-agnostic.

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_telemetry_consent_ux.py -x -q`
- **After every plan wave:** Run `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry*.py -x`
- **Before `/gsd:verify-work`:** Full telemetry suite must be green
- **Max feedback latency:** ~30 seconds

---

## Per-Task Verification Map

> Task IDs are assigned by the planner. The rows below capture the behaviors each requirement
> must prove (from RESEARCH.md § Validation Architecture → Phase Requirements → Test Map). The
> planner/executor fills `Task ID`, `Plan`, `Wave`, and `Status`.
> Updated 2026-06-15 (REVIEWS): single `done()` finalizer (HIGH-1), focused-Enter (HIGH-2),
> stale-checkbox refresh (HIGH-3), positive construct-once (MED), reschedule guard (MED).

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| TBD | 112-02 | 1→2 | CONSENT-02 | — | First-run dialog gate skips when `FIRST_RUN_SHOWN_KEY` already True | unit (headless) | `pytest tests/test_telemetry_consent_ux.py::test_first_run_gate_skips_if_shown -x` | ❌ W0 | ⬜ pending |
| TBD | 112-02 | 1→2 | CONSENT-02 | — | First-run prompt constructs + execs `ConsentDialog` exactly once when flag absent (positive) | unit (headless) | `pytest tests/test_telemetry_consent_ux.py::test_first_run_constructs_and_execs_once -x` | ❌ W0 | ⬜ pending |
| TBD | 112-01 | 1 | CONSENT-02 | T-112-EnterOptIn | Dialog buttons equal-weight, neither is default | GUI (offscreen) | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry_consent_ux.py::test_consent_dialog_no_default_button -x` | ❌ W0 | ⬜ pending |
| TBD | 112-01 | 1 | CONSENT-02 | T-112-EnterOptIn | Enable button focused THEN Return → does NOT opt in (focused-button consume case) | GUI (offscreen) | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry_consent_ux.py::test_consent_dialog_enter_is_decline -x` | ❌ W0 | ⬜ pending |
| TBD | 112-01 | 1 | CONSENT-03 | T-112-CloseNoFlag | Single `done()` finalizer writes `FIRST_RUN_SHOWN_KEY` + correct `set_consent` on accept / decline / Escape / X | unit (headless) | `pytest tests/test_telemetry_consent_ux.py -k done_finalizer -x` | ❌ W0 | ⬜ pending |
| TBD | 112-01 | 1 | CONSENT-03 | T-112-CloseNoFlag | Escape and X opt out (set_consent(False)) + flag written, via real Qt routing | GUI (offscreen) | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry_consent_ux.py -k "escape_opts_out or close_opts_out" -x` | ❌ W0 | ⬜ pending |
| TBD | 112-03 | 3 | CONSENT-04 | — | Settings toggle initial state == `is_enabled()` | GUI (offscreen) | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry_consent_ux.py::test_settings_toggle_initial_state -x` | ❌ W0 | ⬜ pending |
| TBD | 112-03 | 3 | CONSENT-04 | — | Toggle → confirm → `set_consent()` called; cancel-confirm → not called + checkbox reverts | GUI (offscreen) | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry_consent_ux.py -k toggle -x` | ❌ W0 | ⬜ pending |
| TBD | 112-03 | 3 | CONSENT-04 | T-112-StaleCheckbox | `chk_telemetry` refreshes from `is_enabled()` on every Settings open (no stale state after first-run opt-in) | GUI (offscreen) | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry_consent_ux.py::test_settings_checkbox_refreshes_on_open -x` | ❌ W0 | ⬜ pending |
| TBD | 112-03 | 3 | CONSENT-04 | T-112-CancelDesync | Settings Cancel does not overwrite/desync telemetry keys in config.pkl | unit (headless) | `pytest tests/test_telemetry_consent_ux.py::test_settings_cancel_does_not_desync_telemetry -x` | ❌ W0 | ⬜ pending |
| TBD | 112-01/03 | 1/3 | PRIV-05 | — | PrivacyDialog (and About block) bilingual, privacy-preserving wording (not "anonymous") | GUI (offscreen) | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry_consent_ux.py::test_privacy_dialog_constructs_bilingual -x` | ❌ W0 | ⬜ pending |
| TBD | * | * | PRIV-05 | T-112-PRIV03 | No new `desktop/` file imports `shared.posthog_server` (AST guard still green) | static AST | `pytest tests/test_telemetry_no_direct_posthog.py -x` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_telemetry_consent_ux.py` — new file; covers CONSENT-02 / CONSENT-03 / CONSENT-04 + D-07b headless and GUI-offscreen cases, including the REVIEWS-strengthened tests (done()-finalizer on all exit paths, focused-Enter, positive construct-once, stale-checkbox refresh). Establish with at least the headless gate-logic + done()-finalizer tests before implementation begins.
- [ ] Reuse existing headless telemetry fixture pattern from `tests/test_telemetry_consent_gate.py` (monkeypatches `genizah_core.save_app_config` on the module attribute — the dialog must reach `save_app_config` via `genizah_core.save_app_config` module-attr access so the monkeypatch intercepts it; REVIEWS MED).
- [ ] Gate Qt tests via the `tests/conftest.py` offscreen pattern (`QApplication.instance() or QApplication([])`, skip-if-unavailable). Do NOT add a `qt` marker (it is not registered in `pyproject.toml`).
- [ ] `tests/test_telemetry_no_direct_posthog.py` (PRIV-03 AST guard) — already exists; must continue to pass.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Bilingual EN/HE copy reads correctly and RTL renders Hebrew block right-to-left | CONSENT-02, PRIV-05 | Visual/linguistic correctness of stacked bilingual text is not assertable headlessly | Launch desktop app fresh (clear `FIRST_RUN_SHOWN_KEY`), confirm consent dialog shows both EN + HE blocks and the "— Hillel Gershuni, Dicta" sign-off; open "Learn more" → PrivacyDialog shows both languages; HE block renders RTL; About tab shows the bilingual telemetry block for BOTH languages |
| First-run dialog does not stack on the interrupted-indexing recovery modal / citation reminder / any other modal (D-04) | CONSENT-02 | Startup-modal sequencing depends on live timing of recovery + citation modals; the `activeModalWidget()` reschedule guard needs a live event loop | Trigger an interrupted-index recovery state, launch app, confirm recovery modal resolves first, then consent dialog appears after citation reminder closes; separately, open Settings quickly on first launch and confirm consent reschedules behind it rather than stacking |
| Settings checkbox not stale after first-run opt-in (HIGH-3) | CONSENT-04 | Cross-dialog state timing (SettingsDialog built before first-run) is awkward to assert fully headless | On a fresh profile, opt IN via the first-run dialog, then open Settings → the telemetry checkbox shows ENABLED (the offscreen `test_settings_checkbox_refreshes_on_open` covers the refresh logic; this manual pass confirms the real first-run → Settings sequence) |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] No `pytest.ini` reference remains (config is `pyproject.toml`); no `@pytest.mark.qt`
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
