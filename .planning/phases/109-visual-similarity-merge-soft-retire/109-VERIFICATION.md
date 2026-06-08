---
phase: 109-visual-similarity-merge-soft-retire
verified: 2026-06-08T00:00:00Z
status: passed
score: 3/3 must-haves verified
overrides_applied: 3
overrides:
  - must_have: "Badge scheme ★both / ⊙VS / ✎text as specified in REQUIREMENTS.md JWB-12"
    reason: "Replaced by single 👁 eye badge for all visual look-alikes (G-06, gap round 3). Approved by Hillel in 109-HUMAN-UAT.md round-4 (2026-06-08). Same provenance-badge intent satisfied: eye badge identifies VS candidates; ⚓self / ⇄other-side badges handle the other provenance signals."
    accepted_by: "Hillel"
    accepted_at: "2026-06-08T00:00:00Z"
  - must_have: "3-radio source selector Text / Visual / Combined as originally designed"
    reason: "Replaced by a single 'Visual Similarity' toggle (G-04, gap round 2). Toggle ON + empty = pure VS; toggle ON + term = intersection (Search + visual); toggle OFF = text with eye badges. Functionally equivalent to the three sources and approved in UAT."
    accepted_by: "Hillel"
    accepted_at: "2026-06-08T00:00:00Z"
  - must_have: "JoinsDialog pick-mode hook preserved (keep the JoinsDialog pick-mode hook)"
    reason: "D-12 pick-mode was intentionally REVERSED in G-08 (gap round 3). JoinsDialog now opens the Workbench plain (no pick-back). Approved in 109-HUMAN-UAT.md round-4 Scenario A8."
    accepted_by: "Hillel"
    accepted_at: "2026-06-08T00:00:00Z"
---

# Phase 109: Visual Similarity Merge & Soft-Retire Verification Report

**Phase Goal:** The candidate surface gains the visual-similarity look-alike source and a combined view (provenance badges + both-first ordering) via the shared VS service; the standalone Visual Similarity dialog's entry points are rerouted into the Workbench and the old dialog is marked removable after a parity verification pass (the JoinsDialog pick-mode hook is preserved/retired per gap rounds).

**Verified:** 2026-06-08
**Status:** PASSED
**Re-verification:** No — initial verification

---

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|---------|
| 1 | VS look-alikes and "Search+visual" (intersection) populate the candidate surface from `shared.visual_similarity_service`, with a provenance badge (👁 eye) and both-first ordering | VERIFIED (override applied) | `desktop/join_workbench.py` `_on_vs_toggle` / `_start_search` / `merge_candidates` in `shared/joins_lab.py` (tier 0=both, tier 1=text-only, tier 2=VS-only). Eye badge via `_candidate_shelf_badge`. Badge scheme change ★both→👁 is an approved deviation. |
| 2 | Standalone VS dialog entry points (Browse + ResultDialog VS buttons, JoinsDialog pick) are rerouted into the Workbench; `_show_vs_dialog` is marked REMOVABLE after approved parity UAT | VERIFIED (override applied) | Browse + ResultDialog VS buttons removed (G-07). `btn_b_visual_sim` / `btn_rd_visual_sim` are absent from all source files (confirmed by grep). `_show_vs_dialog` marked "DEPRECATED — REMOVABLE (Phase 109, D-11 / D-14b)" at `genizah_app.py:4769`. `109-HUMAN-UAT.md` frontmatter: `parity_sign_off: APPROVED`. JoinsDialog button rerouted to plain open + close (G-08, `corrections_ui.py:4778-4779`). Pick-mode retired per approved override. |
| 3 | Per-candidate enrichment is batched (not per-candidate-serial), verified on ~80-candidate VS load | VERIFIED (human-verified) | `_EnrichWorker.run()` calls `fjms_svc.get_measurement_summaries_batch(sys_ids)` once for all candidates (`desktop/join_workbench.py:1644-1647`). UAT Scenario M (≥80 look-alikes, prompt first page) re-verified and approved in round-4. SC#3 is a perf criterion accepted as human-verified. |

**Score:** 3/3 truths verified (3 overrides applied for intentional UX evolution, all user-approved 2026-06-08)

---

### Deferred Items

None. All three success criteria are met within this phase.

---

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `desktop/join_workbench.py` | VS toggle, eye badge, merge, _EnrichWorker batched | VERIFIED | `btn_vs_toggle` (line 2238), `_candidate_shelf_badge` (line 413), `merge_candidates` call (line 2761), `get_measurement_summaries_batch` (line 1647), crash-safe `_retire_enrich_worker` + `_retired_workers` retention (lines 2778-2825) |
| `shared/joins_lab.py` | `merge_candidates` with both-first ordering | VERIFIED | Lines 511-559: tier 0 (both), tier 1 (text-only), tier 2 (VS-only); `via_text`/`via_vs` annotation via `dataclasses.replace` |
| `shared/visual_similarity_service.py` | `get_vs_service()` + `get_suggestions()` | VERIFIED | `VisualSimilarityService.get_suggestions` (line 97), `get_vs_service` (line 312) |
| `corrections_ui.py` | JoinsDialog 🔗 link button, plain open, dialog close | VERIFIED | `btn_vs_pick = QPushButton("🔗")` (line 3443), tooltip `tr("find joins in joins lab")` (line 3445), `open_joins_workbench(res)` plain open (line 4778), `self.close()` (line 4779) |
| `genizah_app.py` | `_show_vs_dialog` marked REMOVABLE; no live VS buttons | VERIFIED | `_show_vs_dialog` at line 4769: "DEPRECATED — REMOVABLE (Phase 109, D-11 / D-14b): the parity UAT (109-HUMAN-UAT.md, round 4) signed off on 2026-06-08." No `btn_b_visual_sim` / `btn_rd_visual_sim` definitions anywhere in file. |
| `desktop/result_dialog.py` | No VS button; only Find Joins | VERIFIED | `btn_rd_find_joins` at line 288, `_open_join_workbench` at line 723/744. No VS-button definitions. |
| `genizah_translations.py` | Gap-round-3 i18n keys: "visual similarity", hint, combined empty, link tooltip | VERIFIED | "visual similarity" → "דמיון חזותי" (line 4030), "Turn off Visual Similarity to see more results" (line 4033), "No look-alikes match this search — turn off Visual Similarity to see all results" (line 4037), "find joins in joins lab" → "מצא צירופים במעבדת הצירופים" (line 4041) |

---

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `JoinCandidatePane._on_vs_toggle` | `shared.visual_similarity_service.get_vs_service()` | `_load_visual_candidates` called in `_set_anchor` and `_on_vs_toggle` | WIRED | `join_workbench.py:2640-2641` imports and calls `get_vs_service()` |
| VS candidates | `merge_candidates` | `_start_search` calls `merge_candidates(text, vs)` | WIRED | `join_workbench.py:2761-2773` |
| `merge_candidates` result | `_EnrichWorker` | `_start_enrich()` called with `self.results` after merge | WIRED | `join_workbench.py:2776` |
| `_EnrichWorker.run` | `get_measurement_summaries_batch` | batch call with all `sys_ids` | WIRED | `join_workbench.py:1644-1647` |
| Browse "Find Joins" button | `open_joins_workbench` | `_browse_open_join_workbench` → `open_joins_workbench(res)` | WIRED | `genizah_app.py:9840-9858` |
| ResultDialog "Find Joins" button | `open_joins_workbench` | `_open_join_workbench` → `app.open_joins_workbench(res)` | WIRED | `desktop/result_dialog.py:723-745` |
| JoinsDialog 🔗 button | Workbench plain open + dialog close | `_show_vs_picker` → `open_joins_workbench(res)` + `self.close()` | WIRED | `corrections_ui.py:4778-4779` |
| `_show_vs_dialog` | no live caller | All entry points removed or dead | ORPHANED (intentional — soft-retire) | No live caller; marked REMOVABLE. `_on_vs_fetch_complete` and `_enrich_vs_suggestions` also orphaned. |

---

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|--------------|--------|--------------------|--------|
| `JoinCandidatePane` (VS candidates) | `self._vs_cands` | `get_vs_service().get_suggestions(sys_id)` → SQLite VS DB | Yes — real DB query via `VisualSimilarityService.get_suggestions` | FLOWING |
| `_EnrichWorker` | `meas` dict | `fjms_svc.get_measurement_summaries_batch(sys_ids)` → FJMS DB | Yes — batch SQL query, not static | FLOWING |
| `JoinCandidatePane` (text candidates) | `self.results` | existing `SearchExecutor`/`JoinSearchWorker` pipeline | Yes — real Tantivy/regex search | FLOWING |

---

### Behavioral Spot-Checks

SC#3 (batched enrichment on ~80 VS candidates) was verified in UAT Scenario M (approved by Hillel 2026-06-08). Automated behavioral spot-checks are not applicable for this desktop-Qt phase (no HTTP API endpoint to probe without running the app). Skipping automated spot-checks; human UAT serves this purpose.

---

### Requirements Coverage

| Requirement | Source Plans | Description | Status | Evidence |
|-------------|-------------|-------------|--------|---------|
| JWB-12 | Plans 01-13 (all) | Unified candidate sources: VS + text + combined; provenance badges; both-first ordering; soft-retire standalone VS dialog; JoinsDialog hook resolved | SATISFIED | All three SC sub-items verified above. Three approved overrides document intentional UX evolution from original JWB-12 wording. |

JWB-12 traceability in `REQUIREMENTS.md` (line 206): "JWB-12 (unified sources + VS merge) | 108 (text/combined surface) + 109 (VS source + soft-retire) | Active". Phase 109 closes the VS source + soft-retire half.

---

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `desktop/join_workbench.py` | 3976-4008 | `_pane_text_workers` list populated with `_PageTextWorker` threads | ℹ Info (was warning WR-01) | **Fixed in commit `58ca107d`** (most recent commit, day of verification). `_reap_pane_text_worker` + `CompareDialog.closeEvent` now drain workers before GC. Regression test `test_compare_page_text_worker_teardown_is_crash_safe` passes. |
| `genizah_app.py` | 4768 | `_show_vs_dialog` retained dead code | ℹ Info | Intentional one-cycle soft-retire (D-11). Marked REMOVABLE. Scheduled for physical deletion in next cleanup phase. Not a defect. |
| `genizah_app.py` | 4708, 4733 | `_browse_view_visual_similarity`, `_enrich_vs_suggestions` retained dead code | ℹ Info | Same one-cycle soft-retire rationale as above. No live callers. |
| `corrections_ui.py` | 4781 | `_on_vs_pick` retained dead callback | ℹ Info | Same one-cycle soft-retire rationale. D-11. |

No BLOCKERS. All warning-level items are either fixed (WR-01) or intentional soft-retire scaffolding.

Code review INFO items not yet addressed (all low-priority, explicitly noted in `109-REVIEW.md`):
- IN-01: Pane image loads have no generation/staleness guard (pre-existing, not a regression).
- IN-02: `_save_session` drops `join_lab` key on live-window serialization exception (pre-existing asymmetry, low priority).
- IN-03: `_pane_text_workers` list bounded by WR-01 fix; resolved as side effect.

---

### Human Verification Required

None. All success criteria have been verified programmatically or via the approved round-4 human UAT (2026-06-08, `109-HUMAN-UAT.md`: `parity_sign_off: APPROVED`).

SC#3 (batched enrichment performance on ~80 candidates) is a performance criterion that was verified in UAT Scenario M and accepted as complete.

---

### Gaps Summary

No gaps. All three success criteria are met. The three overrides document intentional, user-approved UX evolution:
1. Single 👁 eye badge (replaces ★both / ⊙VS / ✎text scheme) — satisfies the provenance-badge intent.
2. Toggle-based source model (replaces 3-radio selector) — satisfies "Visual similarities + Search+visual sources."
3. JoinsDialog pick-mode retired, not preserved — satisfies "JoinsDialog behavior resolved" per final UAT.

The automated test gate runs 62 tests green. WR-01 (the code review's only warning, a `_PageTextWorker` crash-class twin) was fixed on 2026-06-08 in commit `58ca107d` before this verification was written.

---

_Verified: 2026-06-08_
_Verifier: Claude (gsd-verifier)_
