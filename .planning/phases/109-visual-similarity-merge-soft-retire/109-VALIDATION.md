---
phase: 109
slug: visual-similarity-merge-soft-retire
status: approved
nyquist_compliant: true
wave_0_complete: false  # flips true when the executor completes Wave 0 (Plan 01 Task 1 creates tests/test_join_workbench_vs.py)
created: 2026-06-07
revised: 2026-06-07  # --reviews pass: parity test moved to Plan 02 (_load_visual_candidates), +3 review-driven tests
---

# Phase 109 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Seeded from 109-RESEARCH.md § "Validation Architecture". The planner fills the
> Per-Task Verification Map (task IDs assigned during planning); the plan-checker verifies it.
> **Revised 2026-06-07 (--reviews):** the D-14a parity test was strengthened — it now drives the
> Workbench `_load_visual_candidates` helper (Plan 02) instead of a shim-identity check (Plan 01),
> per review concern #3. Three review-driven tests added (shelfmark fallback, page=None actions,
> network-page-lazy assertion). The map below is reconciled with the revised plans.

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
>
> **Revision note:** JWB-12b (D-14a parity) moved from Plan 01 to **Plan 02** because the true
> invariant requires the `_load_visual_candidates` helper, which needs the live pane wiring
> (review #3). Plan 01 Task 1 still carries automated verifies (JWB-12a + the new shelfmark
> fallback + JWB-12g), so Dimension 8 holds with no 3-task automated-verify gap.

| Req ID | Behavior | Test Type | Automated Command | Plan/Task | File Exists | Status |
|--------|----------|-----------|-------------------|-----------|-------------|--------|
| JWB-12a | VS adapter: `_normalize_vs_row` + `normalize_candidate` → `Candidate(sys_id=alma_id, page=None, via_vs=True, vs_rank=rank, vs_score=svm_score)` | unit | `pytest tests/test_join_workbench_vs.py::test_vs_adapter_maps_fields -x` | 01/T1-T2 | ❌ Wave 0 | ⬜ pending |
| JWB-12b | **Parity invariant (D-14a):** the Workbench **Visual source** (`_load_visual_candidates`) returns the same `sys_id` set as `get_vs_service().get_suggestions(anchor)` | integration | `pytest tests/test_join_workbench_vs.py::test_load_visual_candidates_parity -x` | **02/T1** | ❌ Wave 0 (added Plan 02) | ⬜ pending |
| JWB-12c | `merge_candidates` with VS cands → ★both first, then ✎text, then ⊙VS-by-rank | unit | `pytest tests/test_joins_lab.py -k merge_candidates -x` | 02/T2 | ✅ existing | ⬜ pending |
| JWB-12d | i18n guard: all new `tr()` keys present in TRANSLATIONS; no `✎ text` key (review #6, D-17) | static | `pytest tests/test_join_workbench_i18n.py::test_all_tr_keys_in_translations -x` | 01/T3 | ✅ existing | ⬜ pending |
| JWB-12e | No `_vs_*` private calls from the workbench path (D-18) | static | `pytest tests/test_join_workbench_no_private.py -x` | 01/T2, 02/T1, 03/T1 | ✅ existing | ⬜ pending |
| JWB-12f | None-page guard (RR-12): VS-only `Candidate.page=None` does not crash `CandidateCard` / `_render_table`; the four action dispatchers don't crash (review #7) | unit | `pytest tests/test_join_workbench_vs.py::test_page_none_actions_do_not_crash -x` | 02/T2 | ❌ Wave 0 (added Plan 02) | ⬜ pending |
| JWB-12g | `has_suggestions()` is False → Visual (and Combined) source greyed out (D-08) | unit | `pytest tests/test_join_workbench_vs.py::test_visual_source_greyed_when_no_vs -x` | 01/T1-T2 | ❌ Wave 0 | ⬜ pending |
| JWB-12h | **Review #5:** the VS shim never produces a blank shelfmark (fallback to `str(alma_id)`) | unit | `pytest tests/test_join_workbench_vs.py::test_vs_adapter_shelfmark_fallback -x` | 01/T1-T2 | ❌ Wave 0 (added) | ⬜ pending |
| JWB-12i | **Review #4 / D-09 AMENDMENT:** the network/thumbnail path (ThumbResolver) receives only the visible page (≤`_PER_PAGE`), never the full ≤200 set | unit | `pytest tests/test_join_workbench_vs.py::test_thumbnail_path_is_page_scoped -x` | 02/T2 | ❌ Wave 0 (added) | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_join_workbench_vs.py` — new file. **Plan 01 Task 1** seeds JWB-12a (adapter field mapping), JWB-12h (shelfmark fallback, review #5), JWB-12g (grey-out on no-VS anchor). **Plan 02 Task 1** adds JWB-12b (`test_load_visual_candidates_parity` — the true D-14a invariant, review #3). **Plan 02 Task 2** adds JWB-12f (`test_page_none_actions_do_not_crash`, review #7) and JWB-12i (`test_thumbnail_path_is_page_scoped`, review #4).
- [ ] The parity test (JWB-12b) drives `JoinCandidatePane._load_visual_candidates(anchor_sid, service=)` with a `tmp_vs_db`-backed `VisualSimilarityService` and asserts `{c.sys_id} == {r["alma_id"] for r in get_suggestions}` (review #3 — not a shim-identity check).
- [ ] Confirm/keep `tests/test_join_workbench_no_private.py` (JWB-12e) green across the reroute + VS wiring path (all new helpers are off the `_vs_` prefix: `_normalize_vs_row`, `_load_vs`, `_load_visual_candidates`, `_on_source_changed`, `apply_source`, `set_source`).

*Existing infrastructure (pytest + the three guard tests above) covers JWB-12c/d/e.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Parity UAT sign-off (D-14b): Workbench Visual source vs `get_suggestions` on a handful of real anchors — same look-alikes reachable, no stale text in the Visual view (review #1), non-blank shelfmarks (review #5), reused-window re-anchor reloads VS (review #2), all four actions (Browse / Puzzle / Add-to-List / Add-as-Join) work on VS candidates (D-16) | JWB-12 (SC#2) | Requires human visual comparison + live `visual_similarity.db` + NLI image fetches; gates the deprecation marker flip | Open 3–5 anchors with VS data; confirm the suggestion set matches, no text leaks into Visual, shelfmarks render, re-anchoring reloads VS, and each candidate's four actions succeed. Record in `109-HUMAN-UAT.md`. |
| Perf on ~80-candidate VS load (SC#3 / D-09 AMENDMENT): page-lazy network/thumbnail fetch (≤20/page), cheap local enrichment batched-full, no per-candidate-serial network stall | JWB-12 (SC#3) | Wall-clock/responsiveness is observed, not asserted (the structural ≤20/page gate IS asserted by JWB-12i) | Load an anchor with ≥80 VS look-alikes; confirm the first 20-card page renders promptly and paging stays responsive (thumbnails fetch per-page, not all-200-upfront). |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies (all auto tasks across plans 01–03 carry a `<verify><automated>` command; Task 3 of Plan 03 is a human-verify checkpoint whose automated prerequisite is `test_load_visual_candidates_parity`)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify (Plan 01 T1/T2/T3 all automated; Plan 02 T1/T2 both automated; Plan 03 T1/T2 automated, T3 human-verify gated on an automated test)
- [x] Wave 0 covers all MISSING references (`tests/test_join_workbench_vs.py` created in Plan 01 Task 1; parity + review-driven tests added in Plan 02)
- [x] No watch-mode flags
- [x] Feedback latency < 10s (quick run)
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-06-07 (plan-time; gsd-plan-checker Dimension 8 pass) · revised 2026-06-07 (--reviews: parity test strengthened + 3 review-driven tests reconciled)
