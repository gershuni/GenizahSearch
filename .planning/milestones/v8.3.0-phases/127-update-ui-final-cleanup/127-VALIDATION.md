---
phase: 127
slug: update-ui-final-cleanup
status: planned
nyquist_compliant: true
wave_0_complete: false  # Wave 0 = 3 NEW test files (update_ui_coordination, no_back_edges_desktop, genizah_core_facade)
created: 2026-06-26
---

# Phase 127 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution. Phase 127 is the FINAL,
> ZERO-BEHAVIOR-CHANGE phase of v8.3.0. It (a) MOVE-and-shims the 4 update-UI classes
> (`UpdateNotificationBar`/`WhatsNewBar`/`WhatsNewDialog`/`UpdateProgressDialog`) into
> `desktop/update_ui.py`, (b) retires the Phase-126 D1 re-export shim markers + retargets the one
> external caller, (c) installs `tests/test_no_back_edges_desktop.py` (GUARD-04) + creates
> `tests/test_genizah_core_facade.py` (permanent-facade identity), and (d) full-suite sign-off.
> The sidecar reset/download COORDINATION methods are NOT moved (research crux verdict: moderately
> entangled across 4 ownership domains) — DESK-08 behavioral tests are written AGAINST those methods
> IN PLACE on GenizahGUI. Validation is dominated by **MOVE-and-shim identity**
> (`genizah_app.X is desktop.update_ui.X`), **behavior-parity** (existing suites stay green), three
> **new Wave-0 test files**, and the **load-bearing gui slice**.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.0.2 (Python 3.11.9, PyQt6 6.10.2) |
| **Config file** | `tests/conftest.py` (marker-based gui split via `_GUI_TEST_FILES`) |
| **Quick run command** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_update_ui_coordination.py tests/test_no_back_edges_desktop.py tests/test_genizah_core_facade.py tests/test_telemetry_consent_ux.py tests/test_privacy_disclosure_strings.py tests/test_tabular_builder_rtl.py -q -p no:cacheprovider` |
| **Full suite command** | bulk: `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -m "not gui and not render_smoke" -q -rfE -p no:cacheprovider --tb=no` · gui: same with `-m "gui or render_smoke"` |
| **Estimated runtime** | bulk ~5 min · gui slice ~1–2 min |

**NEVER `-n auto`** (OOMs loading Tantivy per worker / Qt segfault). gui + render_smoke run as a
SEPARATE marker slice — load-bearing for this phase (these ARE GUI classes).

---

## Sampling Rate

- **After every task commit:** run the quick command (the 6 phase-touched test files).
- **After every plan wave:** run BOTH the bulk slice AND the gui slice.
- **Before `/gsd:verify-work`:** full bulk + gui green.
- **Max feedback latency:** ~5 min (bulk).

**Stable pre-existing baseline (NOT regressions — do not chase):** the 6 env-only
`tests/test_search_api_v2.py::test_search_mode_real_index_returns_at_least_one_result[*]`
(no real index in test env). Bulk = 6 failed / ~4853 passed is GREEN. Any OTHER failure is real.
**Trust the base-vs-HEAD NAME-level diff, not the count** (the Phase-124/125 lesson).

---

## Per-Task Verification Map

> Populated per task. MOVE-and-shim: the 4 update-UI classes are DELETED from `genizah_app.py` and
> re-exported via a `# noqa: F401` shim, so `genizah_app.X is desktop.update_ui.X` identity must HOLD.
> The D1 shim flip + external-caller retarget happen here (the deferred-from-126 GUARD-03/04 flip).

| Task ID | Req | Wave | Test Type | Automated Command | Status |
|---------|-----|------|-----------|-------------------|--------|
| update_ui extraction (4 classes MOVE+shim) | DESK-08 | 1 | import-smoke (identity ×4) + ruff | `python -c "import genizah_app,desktop.update_ui as u; assert all(getattr(genizah_app,n) is getattr(u,n) for n in ['UpdateNotificationBar','WhatsNewBar','WhatsNewDialog','UpdateProgressDialog'])"` ; `ruff check desktop/update_ui.py genizah_app.py` | ⬜ pending |
| NEW `test_update_ui_coordination.py` (coordination methods IN PLACE) | DESK-08 | 0 | gui (NEW direct) | `pytest tests/test_update_ui_coordination.py -x` ; conftest `_GUI_TEST_FILES` registration if QApplication needed | ⬜ pending |
| D1 shim retirement: drop `# noqa: F401` markers; retarget external caller | GUARD-03/04 | 2 | runtime + identity + ruff | `pytest tests/test_telemetry_consent_ux.py -x` (retargeted to `desktop.settings_dialogs.SettingsDialog`) ; `ruff check genizah_app.py` (no F401) ; D1 identity still 9/9 | ⬜ pending |
| `test_privacy_disclosure_strings.py` OR-location flip | GUARD-03 | 2 | source-scan | `pytest tests/test_privacy_disclosure_strings.py -x` | ⬜ pending |
| `test_tabular_builder_rtl.py` unchanged (already OR-location) | GUARD-03 | 2 | source-scan | `pytest tests/test_tabular_builder_rtl.py -x` | ⬜ pending |
| NEW `test_no_back_edges_desktop.py` (AST guard, 19 desktop modules) | GUARD-04 | 0 | unit (AST) | `pytest tests/test_no_back_edges_desktop.py -x` | ⬜ pending |
| NEW `test_genizah_core_facade.py` (20-name permanent facade identity) | GUARD-04 | 0 | unit (identity) | `pytest tests/test_genizah_core_facade.py -x` | ⬜ pending |
| `test_no_back_edges_core.py` still green (GUARD-01, unchanged) | GUARD-04 | — | unit (AST) | `pytest tests/test_no_back_edges_core.py -x` | ⬜ pending |
| Full-suite sign-off (bulk + gui) | GUARD-02 | final | integration | bulk slice == 6-env baseline ; gui slice green | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements (NEW test files — written before/with extraction)

- [ ] `tests/test_update_ui_coordination.py` — DESK-08 behavioral tests for the sidecar reset/download
  coordination methods, written AGAINST the methods IN PLACE on `GenizahGUI` (NOT moved; research crux
  verdict). Construct via `GenizahGUI.__new__` + attribute stubs (the `test_telemetry_consent_ux.py`
  pattern). Cover: `_reset_sidecar_connections` (calls the 3 services + catalog filter),
  `_download_next_sidecar` (pops queue + fires download thread), `_on_sidecar_download_finished`
  (advances queue). Register in `conftest.py` `_GUI_TEST_FILES` ONLY if QApplication errors appear at
  collection (start without).
- [ ] `tests/test_no_back_edges_desktop.py` — GUARD-04 AST guard mirroring `test_no_back_edges_core.py`;
  asserts no `desktop/*.py` module imports `genizah_app` at MODULE level (lazy/in-function allowed). Must
  scan all 19 desktop modules (18 existing + new `update_ui.py`). `join_workbench.py`'s genizah_app import
  is LAZY (in-function) → must NOT be flagged.
- [ ] `tests/test_genizah_core_facade.py` — GUARD-04/SC#3 permanent-facade identity: `genizah_core.X is
  shared.Y.X` for all ~20 moved core names (Config + the 122–125 names). Source the assertion list from
  the identity block currently embedded in `test_no_back_edges_core.py`.
- [ ] No framework install needed (pytest + PyQt6 already present).

---

## GUARD-03 Source-Scan Test Retarget Map (by FILENAME — no bare counts)

| Action | Test file | Note |
|--------|-----------|------|
| Retarget caller (genizah_app → desktop.settings_dialogs) | `test_telemetry_consent_ux.py` | 4× `genizah_app.SettingsDialog` → `desktop.settings_dialogs.SettingsDialog` |
| OR-location flip (Phase-126-deferred) | `test_privacy_disclosure_strings.py` | flip to assert against the desktop location |
| No change (already OR-location) | `test_tabular_builder_rtl.py` | accepts genizah_app.py OR desktop/settings_dialogs.py |
| No retarget needed | (update_ui class names) | zero existing tests reference the 4 update_ui class names |

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Desktop app launches; the update-notification bar, What's-New bar/dialog, update-progress dialog, and the sidecar update/reset/download flow all behave normally | DESK-08 | Full interactive launch (real network update check + sidecar download threads + window paint) is untestable headless | At phase end, run `python genizah_app.py` against the real legacy index; trigger Check-for-Updates / observe the What's-New bar; confirm no crash + normal behavior |

---

## Validation Sign-Off

- [x] Every task verifies via an automated command (identity / source-scan / AST / slice)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers the 3 new test files (update_ui_coordination, no_back_edges_desktop, genizah_core_facade)
- [x] No watch-mode flags
- [x] Feedback latency < 300s (bulk)
- [x] `nyquist_compliant: true` set in frontmatter
- [x] MOVE-and-shim identity (`genizah_app.X is desktop.update_ui.X`) is an acceptance criterion in the extraction task
- [x] GUARD-03 retarget map enumerated by FILENAME (no bare counts)
- [x] Coordination methods stay on GenizahGUI (research crux verdict) — DESK-08 tests target them in place

**Approval:** orchestrator — per-task map + Wave-0 + retarget map populated from 127-RESEARCH.md
Validation Architecture; nyquist_compliant set. Planner refines per-task IDs during planning.
