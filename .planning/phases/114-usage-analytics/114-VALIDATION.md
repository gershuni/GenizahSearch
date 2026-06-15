---
phase: 114
slug: usage-analytics
status: ready
nyquist_compliant: true
wave_0_complete: false
created: 2026-06-15
updated: 2026-06-15
---

# Phase 114 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | pytest.ini / pyproject.toml |
| **Quick run command** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py tests/test_no_dynamic_telemetry_strings.py -q` |
| **Full suite command** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry*.py tests/test_no_dynamic_telemetry_strings.py -q` |
| **Estimated runtime** | ~30 seconds (targeted telemetry suite; full `tests/` is NOT run — it exhausts RAM loading Tantivy per worker and pops GUI dialogs) |

---

## Sampling Rate

- **After every task commit:** Run the quick run command (filtered with `-k` to the task's tests)
- **After every plan wave:** Run the full telemetry suite (`tests/test_telemetry*.py tests/test_no_dynamic_telemetry_strings.py`)
- **Before `/gsd:verify-work`:** Full telemetry suite must be green
- **Max feedback latency:** ~30 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 114-01-01 | 01 | 1 | USAGE-04, USAGE-06 | T-114-03 | ACTIVE_PING is a valid trackable event; consent-gated (no emit before consent) | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q -k active_ping` | ❌ W0 (this task creates the file) | ⬜ pending |
| 114-01-02 | 01 | 1 | IDENT-01, USAGE-01, USAGE-06 | T-114-01 / T-114-02 / T-114-04 | identify uses `_uuid` not `.id`; session_start once, allowlisted env props only; stale identity → reset | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q -k "coordinator or session_start or identify_uuid"` | ✅ (114-01-01) | ⬜ pending |
| 114-01-03 | 01 | 1 | IDENT-02, USAGE-05, USAGE-06 | T-114-01 / T-114-03 | login/register alias via `_uuid`; logout resets; session_end exactly once; `$process_person_profile=False` for anon via `_emit()` | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q -k "login or logout or register or session_end or person_profile"` | ✅ (114-01-01) | ⬜ pending |
| 114-02-01 | 02 | 2 | USAGE-02 | T-114-05 | tab_name from hardcoded `_TAB_NAME_MAP`; programmatic restore suppressed | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q -k "tab_activated or tab_name"` | ✅ (114-01-01) | ⬜ pending |
| 114-02-02 | 02 | 2 | USAGE-03 | T-114-06 / T-114-07 / T-114-08 | search_mode from static index map; corpus from `currentData()`; bucketed counts; cancelled has no bucket; exactly-once | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q -k "search_emit or bucket or search_mode or emitted_once"` | ✅ (114-01-01) | ⬜ pending |
| 114-02-03 | 02 | 2 | USAGE-03 | T-114-07 / T-114-08 | single-emit across completed/cancelled paths; shutdown cancel not counted | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q -k "completed or cancelled or shutdown or single_emit"` | ✅ (114-01-01) | ⬜ pending |
| 114-03-01 | 03 | 3 | USAGE-02 | T-114-09 | feature_name/dialog_name hardcoded constants, never windowTitle()/dialog titles | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q -k "feature_opened or joins_lab or puzzle or fjms"` | ✅ (114-01-01) | ⬜ pending |
| 114-03-02 | 03 | 3 | USAGE-04 | T-114-10 / T-114-11 | active_ping once per UTC day, active-only, not on session_start day; focus/resume-aware | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q -k "active_ping or heartbeat or daily"` | ✅ (114-01-01) | ⬜ pending |
| 114-03-03 | 03 | 3 | USAGE-02, USAGE-03 (D-17) | T-114-05 / T-114-06 / T-114-09 / T-114-12 | producer-layer AST guard: no telemetry value from currentText()/tabText()/windowTitle()/text()/selectedFiles()/toPlainText() | AST guard | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_no_dynamic_telemetry_strings.py -q` | ❌ W0 (this task creates the file) | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

**Sampling continuity check:** No 3 consecutive tasks lack an automated `<verify><automated>`. Every one of the 9 tasks has an automated test command. ✅

---

## Wave 0 Requirements

- [ ] `tests/test_telemetry_phase114.py` — created by **114-01-01** (Task 1 of Plan 01). Copies the canonical autouse `_reset_telemetry_state` fixture from `tests/test_telemetry_identity.py` (monkeypatch `load_app_config`/`save_app_config` on both `genizah_core` and `desktop.telemetry`; `ph._reset_for_tests()`; fresh `queue.Queue(maxsize=10000)`; `tel._reset_for_tests()` + `tel._load_consent_state()`; inspect `ph._event_queue`). All subsequent Plan 01/02/03 unit tests extend this file and reuse the fixture.
- [ ] `tests/test_no_dynamic_telemetry_strings.py` — created by **114-03-03** (Task 3 of Plan 03), the D-17 AST guard. Modeled on `tests/test_telemetry_no_direct_posthog.py` (no imports of `desktop/` modules — static AST scan only).

No new framework install needed — pytest + the existing telemetry test fixtures cover all phase requirements. The two new test files ARE the Wave-0 scaffolds (created as the first task of the plan that needs them, before the producer tasks in the same plan run).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Real cross-surface (web↔desktop) person merge | IDENT-01 / IDENT-02 | The actual merge happens inside PostHog's ingestion pipeline (server-side); unit tests verify the `$identify` payload shape (distinct_id == `_uuid`, `$anon_distinct_id` == install_id) but cannot observe the server merging the anon person into the Supabase person. | With consent ON and a real Supabase login on BOTH web and desktop using the same account, then in PostHog project 134161 confirm web and desktop events appear under ONE person whose distinct_id is the Supabase UUID. Verify a logged-out desktop session's anon events later merge into that person after login (PostHog person timeline). |
| Daily heartbeat actually fires across a real UTC-day boundary | USAGE-04 | Unit tests inject the date and app-state, but the real once-per-day/resume-aware behavior over a real midnight + sleep/resume cannot be fully reproduced in a fast unit test. | Leave the app open across a UTC midnight (or change the clock), confirm exactly one `desktop_active_ping` appears in PostHog for the new UTC day and none on the session_start day; sleep/resume the machine and confirm no duplicate ping. |
| Session_end best-effort delivery on real process exit | USAGE-06 / D-15 | `closeEvent` emission is unit-tested for the exactly-once guard, but real fire-and-forget delivery before process teardown depends on the queue flush timing in a live process. | Cleanly close the desktop app with consent ON; confirm a single `desktop_session_end` arrives in PostHog and none arrives after a crash/kill (Phase 113 crash event covers the crash case). |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies (every task maps to an automated command; the two files are W0-created by the first task that needs them)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify (all 9 tasks have automated tests)
- [x] Wave 0 covers all MISSING references (`tests/test_telemetry_phase114.py` + `tests/test_no_dynamic_telemetry_strings.py`)
- [x] No watch-mode flags (all commands are single-shot `-q` runs)
- [x] Feedback latency < 30s (targeted telemetry suite only; full `tests/` deliberately avoided per Windows RAM/GUI constraint)
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-06-15
