---
phase: 109
reviewers: [codex]
reviewed_at: 2026-06-07
plans_reviewed: [109-01-PLAN.md, 109-02-PLAN.md, 109-03-PLAN.md]
self_skipped: claude (running inside Claude Code)
reviewer_caveat: "codex's sandbox blocked file reads — findings are from the supplied plan/context/research excerpts + plan logic, NOT fresh repo reads. The plan↔code line-number drift checks were NOT performed; a --reviews replan should still verify the cited anchors against live code."
---

# Cross-AI Plan Review — Phase 109

> Reviewer: **codex** (codex-cli 0.136.0, default model). Single external reviewer (`claude` skipped — this session IS Claude Code). Codex spent ~77K tokens. **Caveat:** codex reported its local sandbox failed before it could run file commands, so it reviewed from the supplied excerpts only — it did not independently re-read the repo. Its line references mirror the plans' own excerpts; treat the plan↔code drift dimension as still-unverified.

## Codex Review

### Summary

The plans are directionally solid and understand the main seam: raw VS rows must be adapted before `normalize_candidate`, then merged through `merge_candidates`. But there are several high-risk execution issues — not algorithmic, but source-state, lifecycle, and verification gaps that could make the UI show stale/incorrect candidates while the tests still pass.

### Strengths

- The VS adapter contract is correct: `alma_id -> display.id`, `rank -> vs_rank`, `svm_score` passthrough, `_via_vs=True`, `display.img=None`.
- Plan ordering is sensible: adapter/tests first, Workbench wiring second, reroute/deprecation last.
- The plans correctly avoid `_vs_*` helper names in `desktop/join_workbench.py`, preserving `tests/test_join_workbench_no_private.py`.
- Rerouting callers instead of rewriting `_show_vs_dialog` is the right way to preserve JoinsDialog pick-mode.
- Desktop-only scope respected; no web files included.
- Shared service usage correct; no new custom SQLite path proposed.

### Concerns

- **[HIGH] Visual source can include stale text candidates.** Plan 02 changes `_maybe_assemble` (`desktop/join_workbench.py:2398-2404`) to always merge `self._text_cands` and `self._vs_cands`. Since `_on_source_changed("visual")` does not clear `_text_cands`, selecting Visual after a text search can show text+VS, not Visual-only. The empty-builder Combined path (`self._text_cands = self._text_cands or []`) also risks merging stale text.

- **[HIGH] D-09 page-lazy enrichment is not actually satisfied.** The plan preserves `_EnrichWorker` over the full `self.results` list and argues only thumbnails need to be page-lazy. D-09 explicitly says browse text / measurement / thumbnail / snippet / membership must enrich only the visible 20-card page. The plan still enriches all 80–200 candidates upfront for non-thumbnail fields.

- **[HIGH] The automated parity test does not test the Workbench Visual source.** Plan 01's parity test adapts `svc.get_suggestions()` rows directly with `_normalize_vs_row`; it does not exercise `JoinCandidatePane._load_vs`, source selection, grey-out, or `_maybe_assemble`. That is not the D-14 invariant "Workbench Visual source returns the same sys_id set."

- **[HIGH] `set_source("visual")` has lifecycle hazards.** Plan 03 calls `set_anchor(res)` then `set_source("visual")`. The ordering is right for `_anchor_sid`, but Plan 02's grey-out refresh happens later in `_on_anchor_loaded` (`desktop/join_workbench.py:4082`). If the reused Workbench had Visual disabled from a previous no-VS anchor, `set_source` will refuse to select it and no later retry occurs. Also, if Visual is already checked, `rb.setChecked(True)` may not emit a signal, so a new anchor may not reload VS.

- **[MEDIUM] Required ✎ text provenance badge is omitted.** JWB-12 calls for ★both / ⊙VS / ✎text badges. Plan 02 explicitly says text-only gets no badge — may fail the requirement.

- **[MEDIUM] `self._sources` naming is inconsistent.** Plan 02 sets Visual to `{"visual"}` but Combined to `{"text", "vs"}`. If existing code reads `_sources`, this inconsistency can cause subtle drift. Use one canonical source enum.

- **[MEDIUM] VS candidates may render blank identifiers.** `_normalize_vs_row` sets `shelfmark=""` and the plan says `_EnrichWorker` fills it later — but the `_EnrichWorker` description emphasizes measurements/snippets, not shelfmark metadata. If it does not fill shelfmarks, `CandidateCard` (`desktop/join_workbench.py:1667`) renders blank shelf text for VS-only rows.

- **[MEDIUM] Four candidate actions on `page=None` are not tested.** JWB-12 / D-16 require Browse / Puzzle / Add-to-List / Add-as-Join on VS candidates. The plan checks card/table rendering safety but not the action handlers, which may assume a page outside `CandidateCard._card_page`.

- **[MEDIUM] Deprecation marker is added before human parity UAT.** Plan 03 Task 2 adds the marker before Task 3 (UAT). If D-14 is strict, phrase the comment "pending parity" or move it after sign-off.

- **[LOW] Broad `except Exception` around `set_source` can hide broken reroutes.** In `open_joins_workbench`, catching all exceptions could make the VS cutover silently fail. Prefer `RuntimeError` / `AttributeError` + logging.

### Suggestions

- Make `_maybe_assemble` source-aware: track `self._active_source` and choose inputs explicitly — `text=[]` for Visual, `vs=[]` for Text, clear stale text when Combined has an empty builder.
- Add a reusable `_load_visual_candidates(anchor_sid, service=None)` helper and test it; add a lightweight pane test proving `_load_vs` populates `_vs_cands` from the service (the true D-14a invariant).
- Rework `set_source` with a pending-source mechanism: apply after `_on_anchor_set`, and call `_on_source_changed(source)` directly when the radio is already checked.
- Implement page-scoped enrichment: start `_EnrichWorker` only for the current 20 visible candidates, merge into `_enrich`, re-trigger on page change. Add an instrumentation test proving the worker receives only 20 candidates for an 80/200-candidate set.
- Add `tr("  ✎ text")` + matching key, unless existing UI shows a text badge elsewhere.
- Set the VS shim shelfmark to `alma_id` as a fallback, or add explicit batch metadata enrichment before cards render.
- Add targeted tests/checklist for VS-only `page=None` actions, not just rendering.
- Move the deprecation marker after parity UAT, or phrase it "pending parity sign-off."

### Risk Assessment

**Overall risk: HIGH.** The adapter itself is low-risk and the reroute concept is sound. The risk comes from UI state reuse, stale candidate lists, an insufficient parity gate, and a performance requirement the current plan does not actually meet. Fixing those before execution would reduce the phase to medium/low risk.

---

## Consensus Summary

Single external reviewer (codex). No cross-reviewer consensus to compute, but the orchestrator (Claude) assessed each finding against the plan internals:

### Agreed Strengths
- VS→Candidate adapter contract is correct (`alma_id→display.id`, `rank→vs_rank`, `svm_score` passthrough, `_via_vs=True`, `img=None→page=None`).
- Plan/wave ordering is sound (shim+tests → wiring → reroute/deprecate).
- `_vs_*`-prefix avoidance preserves the no-private guard; rerouting callers (not rewriting `_show_vs_dialog`) correctly preserves pick-mode (D-12).
- Desktop-only scope (D-13) respected; shared-service data path (D-19) correct.

### Must-Address Concerns (orchestrator-validated against the plans — these are real)
1. **[HIGH] Stale text in the Visual source.** `_on_source_changed("visual")` never clears `_text_cands`, and `_maybe_assemble` unconditionally merges both halves → Visual-after-text-search shows text+VS, violating D-01. **Fix:** make `_maybe_assemble` source-aware (pass `text=[]` when source is Visual; clear/ignore stale text when Combined's builder is empty).
2. **[HIGH] `set_source` lifecycle on the reused modeless window.** (a) Grey-out (`_on_anchor_set`) runs async in `_on_anchor_loaded` AFTER `set_source` runs synchronously post-`set_anchor` → a previously-disabled Visual blocks selection with no retry; (b) `setChecked(True)` on an already-checked radio emits no signal → no VS reload for the new anchor. **Fix:** pending-source mechanism applied after `_on_anchor_set`, and direct `_on_source_changed(source)` call when the radio is already checked.
3. **[HIGH→MEDIUM] Automated parity test is weaker than D-14a.** Plan 01's test is a shim identity check, not "the Workbench Visual source returns the same sys_id set." **Fix:** add a pane/helper-level test that drives `_load_vs` (extract a testable `_load_visual_candidates(anchor_sid, service=None)`).
4. **[HIGH→MEDIUM] D-09 page-lazy vs the plan.** Plan keeps non-thumbnail enrichment batched-but-full over ≤200; D-09 says page-lazy for ALL fields. **Resolve:** either make non-thumbnail enrichment page-scoped (codex's page-window suggestion) OR explicitly relax D-09 with a measured justification and update CONTEXT (don't silently diverge from a locked decision).
5. **[MEDIUM] Blank VS shelfmarks.** Shim sets `shelfmark=""` and trusts `_EnrichWorker`, which may not populate shelfmark/title → blank cards. **Fix:** shim fallback `shelfmark=alma_id`, or a batch metadata enrichment step (the old `_enrich_vs_suggestions` did shelfmark/library/domain — confirm what fills it now).
6. **[MEDIUM] ✎text badge.** JWB-12/CONTEXT list ✎text as a badge; the plan omits it. **Resolve:** add `tr("  ✎ text")` or document why text-only intentionally carries no badge (Phase-108 parity).
7. **[MEDIUM] Four actions on `page=None` only manually verified.** D-16 requires all four actions work on VS candidates; only the human UAT covers it. **Fix:** add a targeted automated test for the action handlers on a `page=None` candidate.

### Lower-Priority / Hygiene
- `_sources` vocabulary inconsistency (`"vs"` vs `"visual"`) — pick one canonical token.
- Deprecation marker wording ("pending parity sign-off") — the plan already sequences/gates it; tighten the comment text.
- Narrow the `except Exception` around `set_source` to `RuntimeError`/`AttributeError` + log.

### Divergent Views / Caveats
- **Repo-access caveat:** codex could not run file commands, so it did NOT verify the plans' cited line numbers/signatures against live code. The plan↔code drift dimension remains UNVERIFIED by an external reviewer. The gsd-phase-researcher (HIGH confidence) and gsd-pattern-mapper DID verify these anchors against live code during planning, so drift risk is lower than codex's "HIGH" implies — but a `--reviews` replan should still re-confirm the load-bearing anchors (`_maybe_assemble:2402`, `open_joins_workbench:15464`, `_on_anchor_loaded:4082`, the `_EnrichWorker` shelfmark behavior).
- Codex's HIGH overall risk is driven mainly by concerns #1–#4, which are genuine plan-logic gaps rather than code drift. Addressing #1, #2, #3, #5 (and resolving #4 + #6 by decision) is the high-value set before execution.
