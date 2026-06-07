---
phase: 109
slug: visual-similarity-merge-soft-retire
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-07
---

# Phase 109 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Seeded from 109-RESEARCH.md § "Validation Architecture". The planner fills the
> Per-Task Verification Map (task IDs assigned during planning); the plan-checker verifies it.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x (existing) |
| **Config file** | `pytest.ini` (existing) |
| **Quick run command** | `pytest tests/test_join_workbench_vs.py tests/test_join_workbench_i18n.py tests/test_joins_lab.py -x` |
| **Full suite command** | `pytest tests/ -x` |
| **Estimated runtime** | quick ~5–10s · full ~several min |

---

## Sampling Rate

- **After every task commit:** Run the quick run command
- **After every plan wave:** Run the full suite command
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** ~10 seconds (quick run)

---

## Per-Task Verification Map

> Task IDs are assigned by the planner. Each task MUST map to one of the requirement
> rows below (or declare a Wave 0 dependency / Manual-Only verification). The plan-checker
> enforces Dimension 8 (no 3 consecutive tasks without an automated verify).

| Req ID | Behavior | Test Type | Automated Command | File Exists | Status |
|--------|----------|-----------|-------------------|-------------|--------|
| JWB-12a | VS adapter: `_vs_to_norm_dict` + `normalize_candidate` → `Candidate(sys_id=alma_id, page=None, via_vs=True, vs_rank=rank, vs_score=svm_score)` | unit | `pytest tests/test_join_workbench_vs.py::test_vs_adapter_maps_fields -x` | ❌ Wave 0 | ⬜ pending |
| JWB-12b | Parity invariant (D-14a): Workbench Visual source returns the same `sys_id` set as `get_vs_service().get_suggestions(anchor)` for sample anchors | integration | `pytest tests/test_join_workbench_vs.py::test_vs_parity_invariant -x` | ❌ Wave 0 | ⬜ pending |
| JWB-12c | `merge_candidates` with VS cands → ★both first, then ✎text, then ⊙VS-by-rank | unit | `pytest tests/test_joins_lab.py -k merge_candidates -x` | ✅ existing | ⬜ pending |
| JWB-12d | i18n guard: all new `tr()` keys present in TRANSLATIONS (D-17) | static | `pytest tests/test_join_workbench_i18n.py::test_all_tr_keys_in_translations -x` | ✅ existing | ⬜ pending |
| JWB-12e | No `_vs_*` private calls from the workbench path (D-18) | static | `pytest tests/test_join_workbench_no_private.py -x` | ✅ existing (may need update) | ⬜ pending |
| JWB-12f | None-page guard (RR-12): VS-only `Candidate.page=None` does not crash `CandidateCard` / `_render_table` / `CompareDialog` | unit | `pytest tests/test_join_workbench.py -k none_page -x` | ✅ may exist | ⬜ pending |
| JWB-12g | `has_suggestions()` is False → Visual (and Combined) source greyed out (D-08) | unit | `pytest tests/test_join_workbench_vs.py::test_visual_source_greyed_when_no_vs -x` | ❌ Wave 0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_join_workbench_vs.py` — new file covering JWB-12a (adapter field mapping), JWB-12b (parity invariant), JWB-12g (grey-out on no-VS anchor)
- [ ] Parity test (JWB-12b) can model `tests/test_visual_similarity.py`'s `tmp_vs_db` fixture (in-memory SQLite with known VS rows) to assert the Workbench Visual source `sys_id` set equals `get_suggestions(anchor)`
- [ ] Confirm/extend `tests/test_join_workbench_no_private.py` (JWB-12e) to cover the new reroute + VS wiring path

*Existing infrastructure (pytest + the three guard tests above) covers JWB-12c/d/e/f.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Parity UAT sign-off (D-14b): old standalone VS dialog vs Workbench Visual source on a handful of real anchors — same look-alikes reachable + all four actions (Browse / Puzzle / Add-to-List / Add-as-Join) work on VS candidates (D-16) | JWB-12 (SC#2) | Requires human visual comparison + live `visual_similarity.db` + NLI image fetches; gates the deprecation marker flip | Open 3–5 anchors with VS data in both the old dialog and the Workbench Visual source; confirm the suggestion set matches and each candidate's four actions succeed. Record in `109-HUMAN-UAT.md`. |
| Perf on ~80-candidate VS load (SC#3 / D-09): page-lazy + batched enrichment, no per-candidate-serial stall | JWB-12 (SC#3) | Wall-clock/responsiveness is observed, not asserted | Load an anchor with ≥80 VS look-alikes; confirm the first 20-card page renders promptly and paging stays responsive (enrichment is page-lazy, not all-200-upfront). |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (`tests/test_join_workbench_vs.py`)
- [ ] No watch-mode flags
- [ ] Feedback latency < 10s (quick run)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
