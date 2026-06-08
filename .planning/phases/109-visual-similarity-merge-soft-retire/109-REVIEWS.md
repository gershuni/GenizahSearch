---
phase: 109
round: gap-closure-round-3
reviewers: [codex]
reviewed_at: 2026-06-08T00:23:22Z
plans_reviewed: [109-08, 109-09, 109-10, 109-11, 109-12, 109-13]
codex_model: default (codex-cli 0.136.0)
self_skipped: claude (running inside Claude Code)
note: >
  Round-3 pre-execution review of the NEW gap-closure plans only (G-06..G-13).
  Codex's first pass could not read files (Windows sandbox spawn failure); re-run
  with --dangerously-bypass-approvals-and-sandbox so the plan↔code drift check
  actually executed against the live tree (144K tokens, files read).
  The prior round-2 review is preserved at 109-REVIEWS-round2.md.
---

# Cross-AI Plan Review — Phase 109 (gap-closure round 3)

Single external reviewer this round: **Codex** (`codex exec`, file access). Claude self-skipped
(this orchestrator runs inside Claude Code — independence rule). Codex's highest-value contribution
is **plan↔code drift** — it opened the live source and verified every anchor. The internal
plan-checker validated plan-internal consistency and PASSED; Codex caught a drift the checker could
not (it took the plan's "fully unreferenced" claim at face value; Codex grepped the actual code).

Baseline confirmed by Codex against the live tree: the 36-test gate currently passes (`36 passed`).

---

## Codex Review

**Summary**

Not ready to execute as-is. Plans 109-08 through 109-11 mostly match the live source, but 109-12 and
109-13 rest on a false code assumption: `_show_vs_dialog` is still referenced by
`_on_vs_fetch_complete()` in `genizah_app.py`, so the planned "fully unreferenced" marker and grep
acceptance will fail.

**Plan↔Code Drift**

| Plan | Drift Review |
|---|---|
| 109-08 | Verified — anchors match. `Visual Similarity` at `genizah_translations.py:3194`; Phase-109 blocks at `:3996` and `:4011`. `tests/test_join_workbench_i18n.py` has the described full-file JWB scan and Phase-107 host-key guard. |
| 109-09 | Verified — anchors match. Badge block at `desktop/join_workbench.py:1726`, toggle at `:2204`, current `vs_rank` append at `:1736-1737`. |
| 109-10 | Verified — anchors match. `btn_b_visual_sim` construction/wiring at `genizah_app.py:7240` and `:8251`; `btn_rd_visual_sim` at `desktop/result_dialog.py:459` and `:3078`. Find-Joins buttons separate and present at `genizah_app.py:6936` and `desktop/result_dialog.py:288`. |
| 109-11 | Verified — anchors match. `mark()` at `desktop/join_workbench.py:4731`; triage row and folio row separate at `:1782` and `:1800`; grid insertion `:2364`; empty-intersection status `:2860-2864`. |
| 109-12 | **Drift.** `_show_vs_dialog` is NOT "fully unreferenced": `_on_vs_fetch_complete()` still calls it at `genizah_app.py:4753`. The acceptance criterion that `grep -n "_show_vs_dialog" genizah_app.py` shows only the `def` plus internals will not hold after comment-only edits. Other anchors match: JoinsDialog button at `corrections_ui.py:3443`, `_show_vs_picker` at `:4756-4778`, pick machinery at `desktop/join_workbench.py:279`, `:1888`, `:4067`, `:4389`. |
| 109-13 | **Drift inherited from 109-12.** The plan says approval makes the `_show_vs_dialog` marker live because it is fully unreferenced, but `genizah_app.py:4753` still references it unless 109-12 is amended. |

**Concerns**

- **HIGH / 109-12, 109-13:** false deprecation premise. `_show_vs_dialog` still has a direct source
  reference from `_on_vs_fetch_complete()` at `genizah_app.py:4753`. The marker should not say
  "fully unreferenced" until this is handled or explicitly scoped as dead fallback code.
- **MEDIUM / 109-09:** the proposed `elif c.via_vs` branch means `is_anchor_self` or
  `via_other_side` candidates that also have `via_vs=True` will not get the eye. That may violate the
  locked "ANY via_vs candidate" wording (G-06.4), depending on whether those provenance states can
  overlap.
- **LOW / 109-10:** `_parent` exists elsewhere in both host files (`desktop/result_dialog.py:2086`,
  `genizah_app.py:17174`); the plan's local deletion is fine, but the executor should avoid broad
  replacement.
- **LOW / 109-09, 109-11:** stale comments still mention `★both` / VS badge semantics at
  `desktop/join_workbench.py:2700` and `:2720`. If new tests grep too broadly for `★` or `⊙`, they
  may fail despite correct UI code.
- No same-wave file collisions found. No D-13 web touch in these plans.

**Suggestions**

- Amend 109-12 Task 2 to explicitly deal with `_on_vs_fetch_complete()`: either mark that callback
  removable and remove/neutralize the `_show_vs_dialog` call, or change the marker/acceptance from
  "fully unreferenced" to "no live UI entry point; retained dead fetch callback still references it."
- Gate 109-13's marker flip on the revised `_show_vs_dialog` grep after 109-12, not on the current
  assumption.
- In 109-09, make the eye suffix additive for `via_vs` after self/other-side labeling if "ANY
  via_vs" is meant literally.
- Update stale badge comments when removing `★both` / `⊙VS#rank`.

**Risk Assessment**

MEDIUM — most anchors are accurate and the current gate is green, but 109-12/13 have a concrete false
source-reference assumption that would invalidate the deprecation marker.

---

## Orchestrator Verification (independent grep against live tree, 2026-06-08)

I re-checked Codex's two non-LOW findings against the source before recording them:

- **HIGH — CONFIRMED REAL.** `grep -n "_show_vs_dialog" genizah_app.py` returns TWO hits: the call
  at `:4753` (inside `_on_vs_fetch_complete`, def at `:4749`) and the def at `:4755`.
  `_on_vs_fetch_complete` is the async completion callback of `_browse_view_visual_similarity`
  (`:4708`) — the very handler G-07 marks *removable-but-retained*. So after G-07 (button deleted,
  handler retained) + G-08 (pick-mode rerouted), `_show_vs_dialog` still has a live reference inside
  the retained dead-fetch machinery. The fix is consistent with D-11: treat `_on_vs_fetch_complete`
  as part of the same marked-removable `_browse_view_visual_similarity` cluster, and reword the
  marker + grep acceptance to "no live UI entry point; the only references are inside the
  one-cycle-retained `_browse_view_visual_similarity` / `_on_vs_fetch_complete` dead-fetch machinery"
  rather than "fully unreferenced." 109-13's flip-on-approval must gate on that revised grep.

- **MEDIUM — REAL AMBIGUITY, planner decision needed.** The badge block (`desktop/join_workbench.py:
  1727-1740`) is a strict `if/elif` chain: `is_anchor_self` → `via_other_side` → `via_text and via_vs`
  (★both) → `via_vs and not via_text` (⊙VS). G-06.1 says the eye *replaces* the ★both AND ⊙VS
  branches (the last two), and G-06.4 says "⚓self / ⇄other-side badges unchanged." Two valid readings:
  (a) keep the `if/elif` — eye replaces only the ★both/⊙VS branches, self/other-side stay exclusive
  (matches "badges unchanged"); or (b) make the eye *additive* so a `via_other_side & via_vs`
  candidate shows both ⇄ and 👁 (matches "ANY via_vs" literally). Overlap is plausible for
  `via_other_side & via_vs` (a known join partner that is also visually similar), less so for
  `is_anchor_self`. 109-09 must state which reading it implements and test it.

---

## Synthesis — Action Items for `/gsd-plan-phase 109 --reviews`

| # | Sev | Plan(s) | Action |
|---|-----|---------|--------|
| 1 | HIGH | 109-12, 109-13 | Drop "fully unreferenced." Fold `_on_vs_fetch_complete` into the marked-removable `_browse_view_visual_similarity` cluster (D-11). Reword the deprecation marker to "no live UI entry point; only references are inside the retained dead-fetch machinery." Rewrite 109-12's grep acceptance to expect the `def` + the `_on_vs_fetch_complete` call (not def-only). Gate 109-13's marker flip on that revised grep. |
| 2 | MEDIUM | 109-09 | Decide eye semantics explicitly: keep `if/elif` (eye replaces ★both/⊙VS only; self/other-side exclusive — recommended, matches "badges unchanged") OR make the eye additive for all `via_vs`. State the choice in the task action and add a test for the `via_other_side & via_vs` case. |
| 3 | LOW | 109-10 | Instruct the executor to delete the two VS buttons' local `_parent`/wiring lines precisely (anchor-scoped), not via broad find/replace — `_parent` recurs elsewhere in both files. |
| 4 | LOW | 109-09, 109-11 | When removing `★both`/`⊙VS#rank`, update the stale badge comments at `join_workbench.py:2700`/`:2720`; ensure new badge tests grep the render path (not the whole file) so leftover `★`/`⊙` in comments/assemble-logic don't false-fail. |

**Not flagged (Codex confirmed clean):** anchors for 109-08/09/10/11; no same-wave file collisions;
no D-13 web-file touch; the 36-test baseline is green.

These are refinements to plan wording and acceptance criteria, not a redesign — the locked
G-06..G-13 decisions stand. Fold via `/gsd-plan-phase 109 --reviews`.
