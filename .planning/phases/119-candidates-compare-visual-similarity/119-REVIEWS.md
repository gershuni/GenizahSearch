# Phase 119 — Cross-AI Plan Review (Codex pre-flight)

**Reviewer:** Codex CLI (codex-cli 0.139.0), read-only sandbox, against live source
**Date:** 2026-06-19
**Verdict:** BLOCK (1 blocker, 1 high, 2 medium, 1 low)
**Brief:** `_tmp/codex-119-plan-review-brief.md` · **Raw critique:** `_tmp/codex-119-plan-critique.md`

> Focus was plan↔code DRIFT (assumptions in the plans that the real code contradicts), complementing
> the internal plan-checker's plan-internal validation.

## Confirmations (no change needed)
- `shared/joins_lab.badge_and_tooltip` is **genuinely absent** — the Wave-0 (119-01) blocker premise is correct.
- `Candidate`, `dedup_candidates(..., include_self=False)`, `merge_candidates` (tiering), `detect_self_match`
  all exist with the expected fields/behavior.
- VS + FJMS reads are LOCAL SQLite; the off-loop guard extension is feasible.
- Safe-storage multitenant invariant and the `[]` allowlist check out.

## Findings

### F1 [BLOCKER] — VS ON + empty builder cannot render (early return)
- **Plan claim:** 119-04 (`:15`, `:50`, `:123`, `:195-196`) — VS ON + empty builder → pure VS union `merge_candidates([], vs)`.
- **Code reality:** `execute_joins_search()` returns BEFORE any VS/search rendering when the anchor builder
  is empty — `web/pages/joins_lab.py:991-997` (responsa empty guard also `:1012-1015`). The VS-only path
  never runs.
- **Fix:** Add an explicit VS-only branch in `execute_joins_search` that, when the builder is empty AND the
  👁 toggle is ON, BYPASSES the text `execute_search` early-return, fetches VS off-loop (`run_vs_core`), and
  renders `merge_candidates([], vs)`. Add/adjust a task in 119-04 to cover this branch (and its empty-data state).

### F2 [HIGH] — Compare must be keyed per-image, not by sys_id alone
- **Plan claim:** Compare launch/lookup keyed by `sys_id` — 119-02 (`:145`), 119-04 (`:164`), 119-03 (`:102`).
- **Code reality:** Candidate identity is per IMAGE: `Candidate.key == (sys_id, page)` (`shared/joins_lab.py:124-137`);
  `tests/test_joins_lab.py:425-433` proves the same `sys_id` on pages 5 and 6 BOTH survive dedup. Keying Compare
  by `sys_id` alone collides → wrong folio opened.
- **Fix:** Pass the `Candidate` (or `uid` / `(sys_id, page)`) to the Compare open/lookup path across 119-02/03/04.
  Keep TRIAGE keyed by `sys_id` (that part is correct).

### F3 [MEDIUM] — D-05 wording "alma_id == sys_id" is imprecise
- **Plan/CONTEXT claim:** `119-CONTEXT.md:67-70` — returned `alma_id == sys_id`.
- **Code reality:** `shared/visual_similarity_service.py:111-124` — input `sys_id` is `alma_id_a`; each suggestion's
  `alma_id` is the PARTNER (`alma_id_b`). The 119-04 adapter mapping (`Candidate(sys_id=row['alma_id'], ...)`) is
  CORRECT; only the wording is loose.
- **Fix:** Reword CONTEXT/plan note to "the returned `alma_id` is the suggested PARTNER sys_id (`alma_id_b`)".
  No adapter code change.

### F4 [MEDIUM] — "proxy-only" thumbnail claim contradicts the Oxford direct-Bodleian path
- **Plan claim:** thumbnails are "proxy-only" / no direct host URL — 119-02 (`:82`, `:142`, `:204`, `:212`),
  119-04 (`:224`, `:233`).
- **Code reality:** `build_thumbnail_url` can return a DIRECT Bodleian URL for Oxford
  (`web/components/candidate_grid.py:95-101`, `web/services.py:193-212`); `tests/test_candidate_grid.py:123-134`
  accepts that. A strict "proxy-only" acceptance criterion would FAIL against existing behavior.
- **Fix (decision needed):** EITHER (a) document the existing Oxford direct-Bodleian exception in the plans'
  acceptance criteria, OR (b) change `build_thumbnail_url` to force `/api/oxford_image` for Oxford (routes Oxford
  through the proxy+breaker too — behavior change + test update).

### F5 [LOW] — Plan-02 "no gap-3" grep will fail on pre-existing gap-3
- **Plan claim:** 119-02 (`:222`) verification greps that `candidate_grid.py` contains no `gap-3`.
- **Code reality:** the file ALREADY has `gap-3` at `web/components/candidate_grid.py:177`, `:273`, `:305`.
- **Fix:** Either include "replace existing `gap-3`/spacing classes" in the task action, or scope the grep assertion
  to newly added code only.
