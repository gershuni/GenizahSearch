---
phase: 108
slug: desktop-join-workbench-query-builders-candidates-compare
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-05
---

# Phase 108 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from 108-RESEARCH.md `## Validation Architecture`. Per-task IDs filled at plan time.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing) |
| **Config file** | none — default pytest discovery |
| **Quick run command** | `pytest tests/test_joins_lab.py tests/test_join_workbench*.py tests/test_fjms_service.py -x` |
| **Full suite command** | `pytest tests/ -x` |
| **Estimated runtime** | quick ~15s · full ~several min |

Pure logic (`shared/joins_lab.py`) is headless-unit-testable with NO QApplication. The new
surface is the desktop `SearchExecutor` adapter + Qt UI composition: term-joining, `is_empty()`,
triage keying, batch enrichment, and AST/grep guards are headless; visual RTL layout, grid/table
render, CompareDialog panes, and snippet highlighting in-widget are Qt/UAT.

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_joins_lab.py tests/test_fjms_service.py -x`
- **After every plan wave:** Run `pytest tests/ -x`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** ~15 seconds (quick run)

---

## Per-Requirement Verification Map

| Requirement | Behavior | Test Type | Automated Command | File Exists | Status |
|-------------|----------|-----------|-------------------|-------------|--------|
| JWB-06 / JWB-10 | `compose()` from multi-box rows produces correct line-break query | unit (headless) | `pytest tests/test_joins_lab.py -k compose -x` | ✅ (Phase 106) | ⬜ pending |
| JWB-06 | Builder widget `is_empty()` True when all boxes blank; OR-boxes join into `term` with `\|` | unit (headless) | `pytest tests/test_join_workbench_builder.py -x` | ❌ W0 | ⬜ pending |
| JWB-11 | Cross-side AND keeps only candidates with matched neighbor (`(sys_id, page±1)`) | unit (headless) | `pytest tests/test_joins_lab.py -k cross_side -x` | ✅ (Phase 106) | ⬜ pending |
| JWB-11 | Cross-side OR adds synthesized neighbor results | unit (headless) | `pytest tests/test_joins_lab.py -k cross_side_or -x` | ✅ (Phase 106) | ⬜ pending |
| JWB-07 | Candidates render deduped one-per-image (grid + table); filter bar narrows | unit + UAT | `pytest tests/test_joins_lab.py -k dedup -x` + manual | ✅ partial | ⬜ pending |
| JWB-12 (verify) | `detect_self_match()` finds anchor in raw results; include-anchor toggle | unit (headless) | `pytest tests/test_joins_lab.py -k self_match -x` | ✅ (Phase 106) | ⬜ pending |
| JWB-12 (text/combined) | `merge_candidates(text, [])` returns text as-is; ✎text provenance badge | unit (headless) | `pytest tests/test_joins_lab.py -k merge -x` | ✅ (Phase 106) | ⬜ pending |
| JWB-08 | CompareDialog opens candidate side-by-side to the matched page; four actions reachable | UAT | manual (Qt) | — | ⬜ pending |
| D-13 (R-03) | `get_measurements_batch(sys_ids)` returns correct data via one IN-query | unit (headless) | `pytest tests/test_fjms_service.py -k measurements_batch -x` | ❌ W0 | ⬜ pending |
| D-10 (R-05) | Triage keyed by `sys_id`; same fragment at different pages shares triage state | unit (headless) | `pytest tests/test_join_workbench_triage.py -x` | ❌ W0 | ⬜ pending |
| D-06 (R-04) | `TabularQueryBuilderDialog.__init__` has no dialog-level `setLayoutDirection(RightToLeft)` | unit (AST/grep) | `pytest tests/test_tabular_builder_rtl.py -x` | ❌ W0 | ⬜ pending |
| D-19 | All new builder/candidate/CompareDialog strings are `tr()`-wrapped | unit (AST guard) | `pytest tests/test_join_workbench_i18n.py -x` | ✅ (Phase 107, full-module scan) | ⬜ pending |
| D-20 | No `_vs_*` private calls on the workbench path | static AST guard | `pytest tests/test_join_workbench_no_private.py -x` | ✅ (Phase 107, full-module scan) | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_join_workbench_builder.py` — `JoinQueryBuilder.is_empty()`, `build_side_query()`/`compose()` from widget state (headless pure logic), **multiple OR-boxes per row auto-joined into `BuilderRow.term` with `\|`** (D-04/D-05 multi-box design, user-confirmed 2026-06-05)
- [ ] `tests/test_fjms_service.py::TestGetMeasurementsBatch` — batch IN-query returns correct data; missing sys_ids absent; batch size (500) respected
- [ ] `tests/test_join_workbench_triage.py` — triage keyed by `sys_id`; same fragment at different pages shares triage state; cleared on re-anchor
- [ ] `tests/test_tabular_builder_rtl.py` — AST assertion that `TabularQueryBuilderDialog.__init__` does NOT call `self.setLayoutDirection(RightToLeft)`

*Already exist (Phase 107, full-module AST scan — cover Phase 108 additions automatically, no new file needed):*
- `tests/test_join_workbench_i18n.py` — all string literals in `desktop/join_workbench.py` wrapped in `tr()` (D-19)
- `tests/test_join_workbench_no_private.py` — no `_vs_*` method calls in `desktop/join_workbench.py` (D-20)

*Existing Phase 106 tests in `tests/test_joins_lab.py` already cover compose, dedup, merge, cross-side, self-match, snippet helpers — no gaps there.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Builder content area renders RTL (first word on right), chrome LTR in EN; everything RTL in HE | D-06, JWB-06 | Visual layout direction not assertable headless | Open Workbench, anchor a fragment, expand builder under `lang=en` then `lang=he`; confirm word-box order + chrome mirroring |
| Grid cards show thumbnail + material + score + highlighted snippet + Y/?/N; table toggle matches | JWB-07 | In-widget render + image load | Run a builder query with >20 hits; toggle grid⇄table; verify dedup one-per-image + highlight |
| CompareDialog opens to the matched page (incl. cross-side p±1) with "other side matched" label | JWB-08, R-06 | Qt modal + folio nav | Match a candidate via the OTHER-side builder; open Compare; confirm it opens to neighbor page with the side label |
| Self-match readout ✓/✗ + "include anchor itself" toggle (default OFF) | JWB-12 | Readout placement + toggle wiring | Build a query the anchor satisfies; confirm ✓ and that toggling include-anchor adds/removes the anchor from candidates |
| Four actions (Browse / Puzzle / Add-to-List / Add-as-Join) work from candidate + inside Compare; Add-as-Join pre-fills A=anchor, B=candidate | JWB-08 | Cross-dialog action wiring | Trigger each action from a grid card and from inside CompareDialog |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
