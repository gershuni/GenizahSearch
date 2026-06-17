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
| 114-01-02 | 01 | 1 | IDENT-01, USAGE-01, USAGE-06 | T-114-01 / T-114-02 / T-114-03 / T-114-04 | identify uses `_uuid` not `.id`; session_start once, allowlisted env props only; stale identity → reset; identity-sync SPLIT from session_start so opt-out→opt-in re-identifies (REVIEWS HIGH-4); `_telemetry_ready()` gate exists (REVIEWS MEDIUM-9) | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q -k "coordinator or session_start or identify_uuid or reopt_in or telemetry_ready"` | ✅ (114-01-01) | ⬜ pending |
| 114-01-03 | 01 | 1 | IDENT-02, USAGE-05, USAGE-06 | T-114-01 / T-114-03 | login/register alias via `_uuid` (shared `_sync_telemetry_identity`); logout resets; session_end exactly once; `_app_shutting_down` set at top of closeEvent for the Plan 02 emit guards | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q -k "login or logout or register or session_end"` | ✅ (114-01-01) | ⬜ pending |
| 114-02-01 | 02 | 2 | USAGE-02 | T-114-05 | tab_name from hardcoded `_TAB_NAME_MAP`; programmatic restore AND code-driven setCurrentWidget/Index jumps suppressed (`_programmatic_tab_change` via `_set_active_tab` — REVIEWS MEDIUM-5); gated on `_telemetry_ready()` (MEDIUM-9) | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q -k "tab_activated or tab_name or programmatic_tab"` | ✅ (114-01-01) | ⬜ pending |
| 114-02-02 | 02 | 2 | USAGE-03 | T-114-06 / T-114-07 / T-114-08 / T-114-14 | search_mode from static index map; corpus from `currentData()`; bucketed counts; cancelled has no bucket; exactly-once; `_app_shutting_down` FIRST-line guard inside `_emit_search_telemetry` (REVIEWS HIGH-2); `_telemetry_ready()` gate (MEDIUM-9) | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q -k "search_emit or bucket or search_mode or emitted_once or search_shutdown"` | ✅ (114-01-01) | ⬜ pending |
| 114-02-03 | 02 | 2 | USAGE-03 | T-114-07 / T-114-08 | single-emit across completed/cancelled paths; shutdown cancel not counted via BOTH the closeEvent queued-completion path (on_search_finished, REVIEWS HIGH-2) AND stop_search; zero-result completed → bucket '0' | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q -k "completed or cancelled or shutdown or single_emit or zero_result"` | ✅ (114-01-01) | ⬜ pending |
| 114-02-04 | 02 | 2 | USAGE-03 | T-114-06 / T-114-07 / T-114-08 | PGP-Tags search (dispatched via `_execute_tag_search`, bypassing `start_search` — REVIEWS HIGH-1) emits `desktop_search_executed` exactly once with `search_mode='pgp_tags'`, `corpus_scope='genizah'`, bucket on completion, on all three `_on_tag_search_results` outcomes; the `tag` text is NEVER a telemetry value (D-04); `_app_shutting_down`/`_telemetry_ready` guards | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q -k "pgp_tag or pgp_tags or tag_search"` | ✅ (114-01-01) | ⬜ pending |
| 114-02-05 | 02 | 2 | USAGE-03 | T-114-06 / T-114-07 / T-114-08 / T-114-14 | composition (+parallels, which seeds comp) search_mode from static `_COMP_SEARCH_MODE_ENUM` (comp_*/lab_comp_*); corpus from `currentData()`; bucketed on completion; cancelled/partial has no bucket; exactly-once via `_current_comp_search_run` emitted guard; a comp completion arriving during the closeEvent cooperative-interrupt window (`_app_shutting_down` True) emits NOTHING (first-guard inside `_emit_comp_search_telemetry`); `_telemetry_ready()` gate (MEDIUM-9) | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q -k "comp_search or composition or comp_mode or comp_emit or comp_shutdown"` | ✅ (114-01-01) | ⬜ pending |
| 114-03-01 | 03 | 3 | USAGE-02 | T-114-09 / T-114-13 / T-114-15 | feature_name/dialog_name/action hardcoded constants for ALL D-03 surfaces (joins_lab, fragment_puzzle from BOTH `_open_puzzle_window` AND `add_to_puzzle` — REVIEWS MEDIUM-7, fjms_catalog from BOTH the Browse path AND `ResultDialog._show_rd_catalog` — REVIEWS MEDIUM-6, result_detail, visual_similarity, export + 4 export actions), never windowTitle()/dialog titles/selectedFiles(); the visual_similarity event fires from the LIVE `source in ('visual','combined')` branch of `open_joins_workbench` (NOT the dead handlers), distinguishable from an ordinary joins_lab open (no double-count); export `dialog_name='export'` only after the no-data early-return and `action='export_*'` only after a path is chosen (REVIEWS MEDIUM-8); `_telemetry_ready()` gate (MEDIUM-9) | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q -k "feature_opened or joins_lab or puzzle or fjms or result_detail or visual_similarity or export_action or export_dialog"` | ✅ (114-01-01) | ⬜ pending |
| 114-03-02 | 03 | 3 | USAGE-04 | T-114-10 / T-114-11 | active_ping once per UTC day, active-only, not on session_start day; focus/resume-aware; `_telemetry_ready()` first guard (MEDIUM-9) | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q -k "active_ping or heartbeat or daily"` | ✅ (114-01-01) | ⬜ pending |
| 114-03-03 | 03 | 3 | USAGE-02, USAGE-03 (D-17), IDENT-01 (D-10) | T-114-05 / T-114-06 / T-114-09 / T-114-12 | producer-layer AST guard over genizah_app.py + gui_threads.py + desktop/result_dialog.py: re-scoped to inspect ONLY telemetry-call argument expressions (REVIEWS HIGH-3 — no false positive on `on_search_finished`/`export_results`/`export_comp_report` non-telemetry accessor use), forbids currentText()/tabText()/windowTitle()/text()/selectedFiles()/toPlainText() inside telemetry args, AND an identify()-callsite check that flags non-`_uuid` identity sources (REVIEWS LOW-10) | AST guard | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_no_dynamic_telemetry_strings.py -q` | ❌ W0 (this task creates the file) | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

**Sampling continuity check:** No 3 consecutive tasks lack an automated `<verify><automated>`. Every one of the 11 tasks (Plan 01: 3, Plan 02: 5, Plan 03: 3) has an automated test command. ✅

---

## Wave 0 Requirements

- [ ] `tests/test_telemetry_phase114.py` — created by **114-01-01** (Task 1 of Plan 01). Copies the canonical autouse `_reset_telemetry_state` fixture from `tests/test_telemetry_identity.py` (monkeypatch `load_app_config`/`save_app_config` on both `genizah_core` and `desktop.telemetry`; `ph._reset_for_tests()`; fresh `queue.Queue(maxsize=10000)`; `tel._reset_for_tests()` + `tel._load_consent_state()`; inspect `ph._event_queue`). All subsequent Plan 01/02/03 unit tests extend this file and reuse the fixture — including the Plan 01 re-opt-in test (HIGH-4) + `_telemetry_ready` test (MEDIUM-9), the Plan 02 PGP-tags tests (114-02-04, HIGH-1), the regular-search shutdown test (114-02-03, HIGH-2), the composition-shutdown-window test (114-02-05), the WARNING-4 zero-result-completed test, and the Plan 03 live-VS / both-puzzle-paths / both-FJMS-paths / export-placement tests.
- [ ] `tests/test_no_dynamic_telemetry_strings.py` — created by **114-03-03** (Task 3 of Plan 03), the D-17 AST guard. Modeled on `tests/test_telemetry_no_direct_posthog.py` (the canonical AST-guard template; no imports of `desktop/` modules — static AST scan only). TARGET_FILES includes `genizah_app.py`, `gui_threads.py`, and `desktop/result_dialog.py`. RE-SCOPED (REVIEWS HIGH-3) to inspect ONLY telemetry-call argument expressions (not whole functions) + an identify()-callsite identity-source check (REVIEWS LOW-10). (The visual_similarity emit stays in `genizah_app.py`, so no additional TARGET_FILES entry is needed for the VS rewiring.)

No new framework install needed — pytest + the existing telemetry test fixtures cover all phase requirements. The two new test files ARE the Wave-0 scaffolds (created as the first task of the plan that needs them, before the producer tasks in the same plan run).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Real cross-surface (web↔desktop) person merge | IDENT-01 / IDENT-02 | The actual merge happens inside PostHog's ingestion pipeline (server-side); unit tests verify the `$identify` payload shape (distinct_id == `_uuid`, `$anon_distinct_id` == install_id) but cannot observe the server merging the anon person into the Supabase person. | With consent ON and a real Supabase login on BOTH web and desktop using the same account, then in PostHog project 134161 confirm web and desktop events appear under ONE person whose distinct_id is the Supabase UUID. Verify a logged-out desktop session's anon events later merge into that person after login (PostHog person timeline). |
| Mid-session opt-out → opt-in stays identified | IDENT-01 / D-13 (REVIEWS HIGH-4) | Unit tests verify `_sync_telemetry_identity` re-fires `identify(_uuid)` on re-opt-in, but the real Settings-toggle flow over a live login is verified end-to-end manually. | With consent ON and logged in, perform an action; toggle telemetry OFF in Settings, then ON again; perform another action; in PostHog confirm the post-re-opt-in events still attribute to the Supabase UUID (NOT a fresh anon person). |
| PGP-Tags search event over the real flow | USAGE-03 / D-05 (REVIEWS HIGH-1) | Unit tests verify the `_on_tag_search_results` emit shape, but the real PGP-Tags dispatch (`toggle_search` → `_execute_tag_search`) over a live PGP corpus is verified end-to-end manually. | With consent ON, run a PGP-Tags search from the search tab and confirm ONE `desktop_search_executed` with `search_mode='pgp_tags'`, `corpus_scope='genizah'`, and NO event property containing the tag text. |
| Composition + parallels search event over the real flow | USAGE-03 / D-05 | Unit tests verify `_emit_comp_search_telemetry` payload shape, but the real composition run (and the parallels → seed-comp → run path) over a live index is verified end-to-end manually. | With consent ON, run a Composition search (standard and Lab) and confirm one `desktop_search_executed` with `search_mode='comp_*'`/`'lab_comp_*'`. Then use Browse → 🔍 Parallels to seed the composition tab, press Analyze, and confirm the resulting event carries `search_mode='comp_*'` (parallels has no separate event by design). Cancel a long composition and confirm `action='cancelled'` with no bucket. Close the app while a long composition is mid-run and confirm NO stray `desktop_search_executed action='cancelled'` arrives after `desktop_session_end`. |
| Visual Similarity feature_opened over the real live entry | USAGE-02 / D-03 | The Browse-tab VS button was removed in Phase 109; the only live VS activation is the in-app reroute through `open_joins_workbench(source='visual')`. Unit tests verify the emit fires on that branch, but the real live entry point exercised by a user is verified manually. | With consent ON, trigger Visual Similarity from its live in-app entry (the Join Workbench VS source toggle / the rerouted activation) and confirm ONE `desktop_feature_opened` with `dialog_name='visual_similarity'`. Open the Join Workbench normally (Find Joins) and confirm `feature_name='joins_lab'` (NOT a visual_similarity event) — the two surfaces stay distinct. |
| Fragment Puzzle + FJMS catalog over BOTH live open paths | USAGE-02 / D-03 (REVIEWS MEDIUM-6/7) | Unit tests verify each emit site fires, but exercising both real entry points is verified manually. | With consent ON: open the puzzle via the corner button (`_open_puzzle_window`) AND via Browse/ResultDialog "Add to puzzle" (`add_to_puzzle`) — confirm each produces ONE `desktop_feature_opened feature_name='fragment_puzzle'` (no double-count for a single gesture). Open the FJMS catalog from the Browse tab AND from the ResultDialog reading desk (`_show_rd_catalog`) — confirm each produces `dialog_name='fjms_catalog'`. |
| Export placement (no-data / cancelled save) | USAGE-02 / D-03 (REVIEWS MEDIUM-8) | Unit tests verify the placement logic, but the real QFileDialog cancel + no-data paths are verified manually. | With consent ON: attempt a composition export with no data → confirm NO `desktop_feature_opened` fires. Open an export save dialog then CANCEL it → confirm `dialog_name='export'` fired but NO `action='export_*'`. Complete an export → confirm `action='export_xlsx'` (etc.) fires. |
| Daily heartbeat actually fires across a real UTC-day boundary | USAGE-04 | Unit tests inject the date and app-state, but the real once-per-day/resume-aware behavior over a real midnight + sleep/resume cannot be fully reproduced in a fast unit test. | Leave the app open across a UTC midnight (or change the clock), confirm exactly one `desktop_active_ping` appears in PostHog for the new UTC day and none on the session_start day; sleep/resume the machine and confirm no duplicate ping. |
| Session_end best-effort delivery on real process exit | USAGE-06 / D-15 | `closeEvent` emission is unit-tested for the exactly-once guard, but real fire-and-forget delivery before process teardown depends on the queue flush timing in a live process. | Cleanly close the desktop app with consent ON; confirm a single `desktop_session_end` arrives in PostHog and none arrives after a crash/kill (Phase 113 crash event covers the crash case). |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies (every one of the 11 tasks maps to an automated command; the two files are W0-created by the first task that needs them)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify (all 11 tasks have automated tests)
- [x] Wave 0 covers all MISSING references (`tests/test_telemetry_phase114.py` + `tests/test_no_dynamic_telemetry_strings.py`)
- [x] No watch-mode flags (all commands are single-shot `-q` runs)
- [x] Feedback latency < 30s (targeted telemetry suite only; full `tests/` deliberately avoided per Windows RAM/GUI constraint)
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-06-15 (revised 2026-06-15 — added 114-02-04 composition/parallels row; expanded 114-03-01 to all D-03 surfaces; AST guard now scans desktop/result_dialog.py). Revision round 2 (2026-06-15): 114-02-04 adds the composition shutdown-window guard secure behavior + `comp_shutdown` test key; 114-03-01 rewires the visual_similarity emit to the LIVE `open_joins_workbench` source-guarded path and adds the joins_lab-vs-VS distinctness behavior. **Revision round 3 — REVIEWS pass (2026-06-15):** Plan 02 split into 5 tasks — NEW 114-02-04 = PGP-Tags search path (REVIEWS HIGH-1), composition renumbered 114-02-04→114-02-05; 114-02-02/03 add the regular-search `_app_shutting_down` first-line guard (REVIEWS HIGH-2); 114-02-01 adds the `_programmatic_tab_change` guard (REVIEWS MEDIUM-5); 114-01-02 adds identity-sync-split/re-opt-in (HIGH-4) + `_telemetry_ready` gate (MEDIUM-9); 114-03-01 adds both puzzle paths (MEDIUM-7), both FJMS paths (MEDIUM-6), and export placement (MEDIUM-8); 114-03-03 re-scopes the AST guard to telemetry-argument expressions (HIGH-3) + identity-callsite check (LOW-10). Producer gate `_telemetry_ready()` threaded through all usage producers (MEDIUM-9). 11 tasks total; Nyquist-compliant.
