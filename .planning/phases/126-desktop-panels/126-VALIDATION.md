---
phase: 126
slug: desktop-panels
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-26
---

# Phase 126 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution. Phase 126 is a
> ZERO-BEHAVIOR-CHANGE refactor (copy panel clusters out of `genizah_app.py` into `desktop/`,
> leave re-export shims). Validation is therefore dominated by **behavior-parity** (existing suites
> stay green) plus one **new direct panel test** (D3) and the **load-bearing gui slice**.

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
> source-scan test(s) + the bulk/gui slice. Copy-not-move means every existing GUARD-03 test stays
> green automatically in Phase 126 (retarget is ADDITIVE — scan both old + new location; the
> delete+flip happens in Phase 127).

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | Status |
|---------|------|------|-------------|-----------|-------------------|--------|
| {126-Dx-yy} | Dx | n | DESK-0x / GUARD-02/03/04 | unit + import-smoke | `pytest <panel test> + bulk/gui slice` | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_search_results_panel.py` — NEW direct test of `SearchResultsPanel` imported from
  `desktop/search_results_panel.py` (mock `SearchThread` from `gui_threads.py`); first panel with a
  direct-module test (D3 success criterion #3). Register it in `conftest.py` `_GUI_TEST_FILES`.
- [ ] No framework install needed (pytest + PyQt6 already present).

*Otherwise existing infrastructure covers all phase requirements (behavior-parity refactor).*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Desktop app launches; the affected tabs (Settings, Catalog, Search results, Browse/Reading desk, Lists) are fully functional | DESK-01..07 | Full interactive launch (real Tantivy index + sidecars + window paint) is untestable headless; headless construction tests approximate but don't paint | After each wave (or at phase end), run `python genizah_app.py` against the real legacy index; open each affected tab; confirm no crash + normal behavior |

---

## Validation Sign-Off

- [ ] All tasks verify via a retargeted source-scan test + bulk/gui slice (or Wave 0 dep for D3)
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers the new `test_search_results_panel.py`
- [ ] No watch-mode flags
- [ ] Feedback latency < 300s (bulk)
- [ ] `nyquist_compliant: true` set in frontmatter (planner/checker)

**Approval:** pending
