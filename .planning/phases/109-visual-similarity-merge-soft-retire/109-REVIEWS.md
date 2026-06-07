---
phase: 109
round: gap-closure (G-01..G-05)
reviewers: [codex]
reviewed_at: 2026-06-07
plans_reviewed: [109-04-PLAN.md, 109-05-PLAN.md, 109-06-PLAN.md, 109-07-PLAN.md]
note: >
  Pre-execution cross-AI drift review of the GAP-CLOSURE plans (04-07), run after the internal
  gsd-plan-checker passed. The internal checker validated plan-internal consistency; Codex read the
  LIVE code and found 4 HIGH execution blockers + 2 MEDIUM + 1 LOW. The original pre-execution review
  of Plans 01-03 is preserved at 109-REVIEWS-preexec.md.
verdict: REVISION REQUIRED (4 HIGH blockers) — fed back to planner same session
---

# Cross-AI Plan Review — Phase 109 gap-closure round

## Codex Review (codex-cli 0.136.0, read-only repo access)

**Summary** — The plans are mostly grounded in the live code and Plan 04 is suitably surgical, but
Plans 05 and 06 have several live-code execution blockers. The biggest drift: G-04 explicitly requires
VS data loaded even when the toggle is OFF so text rows can still get VS badges; Plan 05 only preserves
`_vs_cands` if it was already loaded. There are also reused-window/re-anchor state bugs and a pick-mode
ordering bug that can leave the Workbench without a "Select as partner" button.

**Strengths**
- Plan 04's G-01 edit set is correct and surgical: VS offenders at `genizah_translations.py:3832-3837`
  + `:4005-4006`; legitimate "external" strings remain at `:43, 107, 112, 315, 1732, 2867, 3414`.
- The intersection filter is logically correct: `merge_candidates()` annotates text candidates with
  `via_vs=True` by `sys_id` at `shared/joins_lab.py:531-546`, so `[c for c in merged if c.via_text and
  c.via_vs]` is the right ★both-only set.
- D-11 physical retention respected in Plan 06: updates `_show_vs_dialog` comments but does not delete
  the method or `on_pick` branch (`genizah_app.py:4755-4765`, `:5083-5093`).
- The empty-results enrichment branch exists: `_start_enrich()` reaches `self._enrich = {};
  self.apply_filters()` when `self.results` is empty at `desktop/join_workbench.py:2651-2658`.

**Concerns**
- **HIGH / Plan 05 / G-04 — OFF-mode badges missing or stale.** Context requires "VS must be loaded
  for the anchor whenever available" even with toggle OFF (`109-CONTEXT.md:332-334`). Plan 05 only says
  "Do NOT null `self._vs_cands`" on uncheck (`109-05-PLAN.md:201-204`); `_maybe_assemble()` uses
  `vs = self._vs_cands or []` (`:293-308`). Fresh text/OFF searches won't badge ★both unless VS was
  loaded by a prior toggle. Worse: after re-anchor, stale `_vs_cands` annotates against the PREVIOUS anchor.
- **HIGH / Plan 05 / G-04 + deferred Scenario L — re-anchor reload not handled.** Live `set_anchor()`
  starts anchor load but does not clear/reload candidate state (`desktop/join_workbench.py:4270-4311`);
  `_on_anchor_set()` only enables/disables radios + applies pending source (`:2547-2568`). Plan 05 keeps
  the same shape (`109-05-PLAN.md:217-222`). Re-anchoring from a candidate card (`:1864-1866`) while VS
  is on won't reload VS for the new anchor unless a pending source happened to be set.
- **HIGH / Plan 05 / G-04 — `set_source("visual")` swallowed on reused window.** `set_source()` clears
  pending after `apply_source()` returns (`desktop/join_workbench.py:4256-4261`); `apply_source()`
  returns normally when visual is disabled (`:2580-2586`). Plan 05 repeats that with `_pending_vs`
  (`109-05-PLAN.md:224-226`) and uses widget enabled-state as "anchor HAS VS" (`:207-212`) — can lose
  the visual request before `_on_anchor_set()` learns the new anchor has VS.
- **HIGH / Plan 06 / G-05 — pick-mode callback set too late.** Plan 06 adds the card pick button only
  when `self.pane.wb._pick_callback is not None` (`109-06-PLAN.md:158-163`), but sets/clears the callback
  AFTER `set_anchor()`/`set_source()` (`:213-223`). `set_source("visual")` synchronously loads VS and
  renders cards via `_maybe_assemble()`/`_render_grid_page()` (`desktop/join_workbench.py:2535-2544,
  2599-2619, 2782-2788`) BEFORE the callback exists → first-page pick-mode cards may have no "Select as
  partner" button; normal opens after pick mode may render stale pick buttons before `clear_pick_callback()`.
- **MEDIUM / Plan 05 / G-03 — "no matches" empty-state key preseeded but not used.** Plan 04 adds
  `"No look-alikes match this search"` (`109-04-PLAN.md:150-153`), Plan 05 says it'll call it (`:230`),
  but `_maybe_assemble()` only calls `_start_enrich()` (`:309-311`) and says not to change the render
  pipeline (`:317-320`). Live `apply_filters()` only sets `"{n}/{n} shown"` (`desktop/join_workbench.py:
  2751-2754`). Avoids a spinner but does not produce the promised empty-state message.
- **MEDIUM / Plan 06+07 / D-11–D-14b — deprecation marker wording inconsistent.** Context: marker stays
  "pending parity sign-off" until clean re-UAT (`109-CONTEXT.md:289-293`). Plan 06 says mark the method
  "removable in the next cleanup phase" (`109-06-PLAN.md:250-255`); Plan 07 says it stays pending until
  all UAT passes (`109-07-PLAN.md:15-23`). The code comment must not imply final removability pre-sign-off.
- **LOW / Plan 07 / regression gate.** `tests/test_join_workbench_construct.py` is in Plan 07's action
  text (`109-07-PLAN.md:80-86`) but omitted from its `<verify>`/acceptance criteria (`:129-135`). The
  toggle rewrites Qt construction (`desktop/join_workbench.py:2126-2151`) — construct must be in the
  formal acceptance gate, not just narrative.

**Suggestions**
- Add `_ensure_vs_loaded_for_anchor(silent=True)`; call on anchor-set for VS-bearing anchors and before
  text/OFF assembly so badges are computed from current-anchor VS data.
- On `set_anchor()`, clear/invalidate pane candidate state (`_text_cands`, `_vs_cands`, `results`,
  `filtered`, cards) and reload/assemble per the current toggle after `_on_anchor_set()`.
- Don't use `btn_vs_toggle.isEnabled()` as source of truth for the new anchor — query
  `get_vs_service().has_suggestions(self.wb._anchor_sid)` or defer source application until
  `_on_anchor_set()` completes; clear `_pending_vs` only after the requested state was actually applied.
- In `open_joins_workbench(..., pick_callback=)`, set/clear the callback BEFORE `set_anchor()`/
  `set_source()`, or make `set_pick_callback()` force a re-render of visible cards. Add a call-order test.
- Actually use `tr("No look-alikes match this search")` in the empty-intersection path,
  before/inside `apply_filters()` when `_vs_on` and the term produce zero ★both candidates.
- Keep `_show_vs_dialog` wording "pending parity sign-off; normal and pick callers rerouted" until Plan 07.

**Risk Assessment** — Overall **HIGH**. Intersection logic is sound and Plan 04 is low-risk, but
Plan 05's state model does not satisfy the authoritative OFF-badge/current-anchor VS-load requirement,
and Plan 06's callback ordering can break the partner-picker.

**RELEASE / EXECUTION BLOCKERS**
1. Plan 05 must load current-anchor VS data even when the toggle is OFF.
2. Plan 05 must fix re-anchor + pending visual-source application on reused windows.
3. Plan 06 must set/clear `pick_callback` before any render, or re-render after setting it.
4. Plan 05 must wire the empty-intersection message if "no matches" is an acceptance requirement.

---

## Consensus Summary

Single external reviewer (Codex) this round. The internal `gsd-plan-checker` PASSED on plan-internal
consistency; Codex's value was reading the LIVE code and finding behavioral/state drift the checker
could not see. Net: **4 HIGH blockers** concentrated in Plan 05 (toggle state model) and Plan 06
(pick-callback ordering), plus 2 MEDIUM (empty-state message wiring; deprecation-marker wording) and
1 LOW (construct test in the acceptance gate). Plan 04 (G-01 Hebrew fix) is clean.

### Agreed Concerns (priority)
1. OFF-mode VS-badge load + re-anchor staleness (current-anchor VS must always be loaded; clear on re-anchor).
2. Pick-callback ordering in `open_joins_workbench` (set before render or re-render).
3. `set_source('visual')` reliability on reused/disabled-toggle windows (don't trust widget enabled-state).
4. Empty-intersection message actually rendered; deprecation wording stays "pending sign-off"; construct test gated.
