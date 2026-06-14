---
phase: 112
slug: consent-ux
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-14
---

# Phase 112 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `112-RESEARCH.md` § Validation Architecture.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x (existing) |
| **Config file** | `pytest.ini` (existing) |
| **Quick run command** | `pytest tests/test_telemetry_consent_ux.py -x -q` |
| **Full suite command** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry*.py -x` |
| **Estimated runtime** | ~30 seconds (telemetry subset) |

> **Windows note** (`feedback_full_suite_testing_windows.md`): full `pytest tests/` aborts on a
> non-deterministic PyQt6 headless segfault. Run Qt-dependent tests with
> `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen`. Headless tests (config.pkl gate logic,
> snapshot exemption, shown-flag write) need NO Qt — keep them separable in the same file.

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

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| TBD | TBD | 1 | CONSENT-02 | — | First-run dialog gate skips when `FIRST_RUN_SHOWN_KEY` already True | unit (headless) | `pytest tests/test_telemetry_consent_ux.py::test_first_run_gate_skips_if_shown -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | 1 | CONSENT-02 | T-112-EnterOptIn | Dialog buttons equal-weight, neither is default; Enter does not opt in | GUI (offscreen) | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry_consent_ux.py::test_consent_dialog_no_default_button -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | 1 | CONSENT-03 | T-112-CloseNoFlag | `FIRST_RUN_SHOWN_KEY` written on accept / decline / close-Escape | unit (headless) | `pytest tests/test_telemetry_consent_ux.py -k shown_flag_written -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | 2 | CONSENT-04 | — | Settings toggle initial state == `is_enabled()` | GUI (offscreen) | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry_consent_ux.py::test_settings_toggle_initial_state -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | 2 | CONSENT-04 | — | Toggle → confirm → `set_consent()` called; cancel-confirm → not called + checkbox reverts | GUI (offscreen) | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry_consent_ux.py -k toggle -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | 2 | CONSENT-04 | T-112-CancelDesync | Settings Cancel does not overwrite/desync telemetry keys in config.pkl | unit (headless) | `pytest tests/test_telemetry_consent_ux.py::test_settings_cancel_does_not_desync_telemetry -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | * | PRIV-05 | T-112-PRIV03 | No new `desktop/` file imports `shared.posthog_server` (AST guard still green) | static AST | `pytest tests/test_telemetry_no_direct_posthog.py -x` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_telemetry_consent_ux.py` — new file; covers CONSENT-02 / CONSENT-03 / CONSENT-04 + D-07b headless and GUI-offscreen cases. Establish with at least the headless gate-logic tests before implementation begins.
- [ ] Reuse existing headless telemetry fixture pattern from `tests/test_telemetry_consent_gate.py`.
- [ ] `tests/test_telemetry_no_direct_posthog.py` (PRIV-03 AST guard) — already exists; must continue to pass.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Bilingual EN/HE copy reads correctly and RTL renders Hebrew block right-to-left | CONSENT-02, PRIV-05 | Visual/linguistic correctness of stacked bilingual text is not assertable headlessly | Launch desktop app fresh (clear `FIRST_RUN_SHOWN_KEY`), confirm consent dialog shows both EN + HE blocks; open "Learn more" → PrivacyDialog shows both languages; HE block renders RTL |
| First-run dialog does not stack on the interrupted-indexing recovery modal / citation reminder (D-04) | CONSENT-02 | Startup-modal sequencing depends on live timing of recovery + citation modals | Trigger an interrupted-index recovery state, launch app, confirm recovery modal resolves first, then consent dialog appears after citation reminder closes |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
