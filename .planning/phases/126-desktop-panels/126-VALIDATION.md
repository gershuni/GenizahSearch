---
phase: 126
slug: desktop-panels
status: planned
nyquist_compliant: true
wave_0_complete: false  # Wave 0 (new test_search_results_panel.py) is task 126-03-02
created: 2026-06-26
---

# Phase 126 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution. Phase 126 is a
> ZERO-BEHAVIOR-CHANGE refactor (MOVE panel clusters out of `genizah_app.py` into `desktop/`,
> replace the originals with `# noqa: F401` re-export shims — the genizah_core 122–125 recipe).
> Validation is therefore dominated by **behavior-parity** (existing suites stay green via the
> re-export shims), **MOVE-and-shim identity** (`genizah_app.X is desktop.Y.X`), one **new direct
> panel test** (D3), and the **load-bearing gui slice**.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.0.2 (Python 3.11.9, PyQt6 6.10.2) |
| **Config file** | `tests/conftest.py` (marker-based gui split via `_GUI_TEST_FILES`) |
| **Quick run command** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest <retargeted panel test files for the wave> -q -p no:cacheprovider` |
| **Full suite command** | bulk: `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -m "not gui and not render_smoke" -q -rfE -p no:cacheprovider --tb=no` · gui: same with `-m "gui or render_smoke"` |
| **Estimated runtime** | bulk ~5 min · gui slice ~1–2 min |

**NEVER `-n auto`** (OOMs loading Tantivy per worker / Qt segfault). gui + render_smoke run as a
SEPARATE marker slice — load-bearing for this phase (these ARE the GUI panels).

---

## Sampling Rate

- **After every task commit:** run the wave's retargeted panel test file(s) (quick command).
- **After every plan wave (D1…D5):** run BOTH the bulk slice AND the gui slice.
- **Before `/gsd:verify-work`:** full bulk + gui green.
- **Max feedback latency:** ~5 min (bulk).

**Stable pre-existing baseline (NOT regressions — do not chase):** the 6 env-only
`tests/test_search_api_v2.py::test_search_mode_real_index_returns_at_least_one_result[*]`
(no real index in test env). Bulk = 6 failed / ~4853 passed is GREEN. Any OTHER failure is real.
**Trust the base-vs-HEAD NAME-level diff, not the count** (the Phase-124/125 lesson).

---

## Per-Task Verification Map

> Populated by the planner per task. Each panel-extraction task verifies via its GUARD-03-retargeted
> source-scan test(s) + the bulk/gui slice. MOVE-and-shim: the cluster code is DELETED from
> `genizah_app.py` and re-exported via shim, so `genizah_app.X is desktop.Y.X` identity must HOLD;
> GUARD-03 source-scan tests are additively retargeted (OR-location — accept the old genizah_app.py
> path OR the new desktop module). The external-caller retarget + shim deletion happen in Phase 127.

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | Status |
|---------|------|------|-------------|-----------|-------------------|--------|
| 126-01-01 | D1 | 1 | DESK-02 | import-smoke (identity) + ruff | `python -c import desktop.ui_widgets + g.X is w.X` ; `ruff check desktop/ui_widgets.py genizah_app.py` | ⬜ pending |
| 126-01-02 | D1 | 1 | DESK-01 | gui (runtime) + identity + ruff | `pytest test_telemetry_consent_ux.py` ; `g.SettingsDialog is s.SettingsDialog` | ⬜ pending |
| 126-01-03 | D1 | 1 | GUARD-03/02 | source-scan (1 additive) + slices | `pytest test_tabular_builder_rtl.py` (OR-location) ; bulk + gui slice | ⬜ pending |
| 126-02-01 | D2 | 2 | DESK-03 | import-smoke (identity) + ruff | `python -c import desktop.catalog_browse + g.X is c.X (panel + worker)` ; `ruff check` | ⬜ pending |
| 126-02-02 | D2 | 2 | GUARD-03/02 | runtime + web-side false-positive + slices | `pytest test_catalog_availability_filter.py test_seed023_catalog_filters.py` ; bulk + gui | ⬜ pending |
| 126-03-01 | D3 | 3 | DESK-04 | import-smoke (identity) + ruff | `python -c import desktop.search_results_panel + g.X is p.X` ; `ruff check` | ⬜ pending |
| 126-03-02 | D3 | 3 | DESK-04 | gui (NEW direct test) | `pytest test_search_results_panel.py` (mock SearchThread) ; conftest registration check | ⬜ pending |
| 126-03-03 | D3 | 3 | GUARD-03/02/04 | source-scan (1 additive) + slices + name diff | `pytest test_local_filter_cascade.py` (retargeted; HIGH-2) ; bulk + gui slice ; `dir(genizah_app)` name diff | ⬜ pending |
| 126-04-01 | D4 | 4 | DESK-06 | import-smoke (identity) + ruff | `python -c import desktop.reading_desk_panel + g.X is r.X` ; `ruff check` | ⬜ pending |
| 126-04-02 | D4 | 4 | DESK-05 | import-smoke + signal-attr (4 sites) + ruff | `python -c import desktop.browse_panel + g.X is b.X + browse_thumb_resolved/_on_browse_thumb_resolved attrs` ; `ruff check` | ⬜ pending |
| 126-04-03 | D4 | 4 | GUARD-03/02 | source-scan (12 additive) + slices | `pytest` 12 browse source-scan tests (HIGH-3) ; bulk + gui | ⬜ pending |
| 126-05-01 | D5 | 5 | DESK-07 | import-smoke (identity) + ruff | `python -c import desktop.lists_tab _ListsSyncCoordinator + g.X is l.X` ; `ruff check` | ⬜ pending |
| 126-05-02 | D5 | 5 | DESK-07 | import-smoke + scope-guard + ruff | `python -c import ListsPanel + show_add_to_list_menu & Community populators NOT moved` ; `ruff check` | ⬜ pending |
| 126-05-03 | D5 | 5 | GUARD-03/02/04 | web-side false-positives + slices + phase-final name diff | `pytest` 4 web lists tests + runtime ; bulk + gui ; phase-final name diff | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [x] (planned) `tests/test_search_results_panel.py` — NEW direct test of `SearchResultsPanel` imported from
  `desktop/search_results_panel.py` (mock `SearchThread` from `gui_threads.py`); first panel with a
  direct-module test (D3 success criterion #3). Register it in `conftest.py` `_GUI_TEST_FILES`.
- [ ] No framework install needed (pytest + PyQt6 already present).

*Otherwise existing infrastructure covers all phase requirements (behavior-parity refactor).*

---

## GUARD-03 Source-Scan Test Retarget Map (by FILENAME — HIGH-3: no bare counts)

| Plan | Retargeted (additive, OR-location) | Excluded (web-side false positive / not this cluster) |
|------|-------------------------------------|-------------------------------------------------------|
| D1 (126-01) | `test_tabular_builder_rtl.py` | — |
| D2 (126-02) | (none — runtime cache kept in place) | `test_seed023_catalog_filters.py` (web.pages.catalog_browse) |
| D3 (126-03) | `test_local_filter_cascade.py` (HIGH-2; scans `_apply_results_table_filters`/`_apply_local_filter`/`_apply_local_optout_filter`) | — |
| D4 (126-04) | `test_browse_synthetic.py`, `test_local_browse_panel.py`, `test_wr01_open_local_browse_page_ast.py`, `test_desktop_folio_navigation.py`, `test_view_all_cap.py`, `test_view_all_incremental.py`, `test_desktop_pending_corrections.py`, `test_fgp_chooser_integration.py`, `test_local_nav_codex_fix7.py`, `test_local_nav_codex_fix8.py`, `test_my_library_tab.py`, `test_synthetic_round_trip.py` (12 total; + `test_join_workbench_vs.py` genizah_app.py-read OR-location for `btn_b_find_joins`) | `test_browse_state.py` (web.pages.browse_state); `test_local_filter_cascade.py` (→ D3) |
| D5 (126-05) | (none — all web-side) | `test_add_to_list_dialog_ui_context.py`, `test_user_lists_cache_isolation.py`, `test_user_lists_data_threading.py`, `test_user_lists_refresh_data_returns.py` (all web.*) |

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Desktop app launches; the affected tabs (Settings, Catalog, Search results, Browse/Reading desk, Lists) are fully functional | DESK-01..07 | Full interactive launch (real Tantivy index + sidecars + window paint) is untestable headless; headless construction tests approximate but don't paint | After each wave (or at phase end), run `python genizah_app.py` against the real legacy index; open each affected tab; confirm no crash + normal behavior |

---

## Validation Sign-Off

- [x] All tasks verify via a retargeted source-scan test + bulk/gui slice (or Wave 0 dep for D3)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers the new `test_search_results_panel.py` (task 126-03-02)
- [x] No watch-mode flags
- [x] Feedback latency < 300s (bulk)
- [x] `nyquist_compliant: true` set in frontmatter (planner/checker)
- [x] MOVE-and-shim identity (`genizah_app.X is desktop.Y.X`) is an acceptance criterion in every extraction task (BLOCKER resolution)
- [x] GUARD-03 retarget map enumerated by FILENAME (HIGH-3; D3=1, D4=12, no bare counts)

**Approval:** planner — per-task map populated + retarget map enumerated by filename; nyquist_compliant set; revised per 126-PREFLIGHT-CODEX.md
