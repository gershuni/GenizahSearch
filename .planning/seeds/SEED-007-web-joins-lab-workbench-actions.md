---
id: SEED-007
status: shipped
planted: 2026-06-19
planted_during: v8.2.0 / Phase 119 (Web Joins Lab)
trigger_when: Phase 120 (Actions & Persistence) discuss — pull these items in there. Phase 120 ALREADY scopes "Add-as-Join (login-gated)" + persistence; items #1,#2,#3,#5,#6 below are additive to that same phase, NOT a separate one.
scope: medium (fold into existing Phase 120)
---

> **ROUTING (revised 2026-06-19):** These are NOT a new standalone phase. The v8.2.0 roadmap already has
> **Phase 120 — Actions & Persistence** (Add-as-Join + bulk puzzle handoff + add-to-list/export + state
> persistence). Item #4 below ("Add as join") is already Phase-120 scope; items #1/#2/#3/#5/#6 are additive
> workbench actions for the SAME phase. Surface this seed at `/gsd:discuss-phase 120`. (The R2-1/R2-2 Hebrew/RTL
> items from the same UAT are i18n — they belong to the Phase-119 gap closure and/or **Phase 121 — i18n Polish**.)

# SEED-007: Web Joins Lab — workbench actions (anchor management, joins, Compare info/browse, stop-with-partial)

> Captured as a seed (NOT implemented). These are NEW capabilities the user flagged during the
> 2026-06-19 live Compare UAT and explicitly deferred to "the next phase." They are distinct from
> the Phase-119 round-2 polish gaps (R2-1..R2-10 in `119-HUMAN-UAT.md`), which are bug/UX fixes on the
> existing surface and go through `/gsd:plan-phase 119 --gaps`. THIS seed is new features → its own phase.

## Why This Matters

Phase 117-119 built the web Joins Lab read/search/triage/Compare surface. The desktop Joins Lab
(Phases 106-110, v8.0.0) has a richer workbench. The web app now needs the workbench *actions* that
turn triage into actual research output (saved joins, anchor pivots) and bring Compare to parity with
the desktop / Browse experience.

## Scope — six user-requested features (2026-06-19)

1. **Stop search with partial results** — let the user cancel a running candidate/VS search and keep
   whatever candidates have already arrived (mirrors the desktop composition "partial results on cancel"
   pattern; see the 2026-02-27 power-user letter item ג).
2. **"Make an anchor"** — promote a candidate (or an arbitrary fragment) to the anchor slot, so the user
   can pivot the workbench around a new fragment without re-navigating from scratch.
3. **Show the anchor's joins** — surface existing saved joins (`joins.db`) that involve the current anchor,
   so the user sees prior work / avoids duplicate joins.
4. **"Add as join"** — persist a confirmed anchor↔candidate pairing as a saved join document
   (`shared/puzzle_service.py` / `joins.db` `join_documents` + `join_document_fragments`), web-side.
5. **Browse-in-Compare** — a control in the Compare window to open the candidate (and/or anchor) in the
   full Browse reader, carrying the correct sys_id + page.
6. **Info buttons (catalog + bibliography) in Compare** — surface FJMS catalog + bib metadata for each
   pane inside Compare (parity with the Browse/ResultDialog info affordances).

## When to Surface

**Trigger:** the next Web Joins Lab phase after the Phase-119 round-2 Compare-polish gap closure ships.
Pull the six items above into REQUIREMENTS/ROADMAP for that phase. Cross-reference desktop Joins Lab
(`project_join_lab_desktop_redesign`, `project_v8_joins_lab_milestone`) for behavior parity and the
`shared/puzzle_service.py` / `joins.db` write path for "Add as join".

## Notes / pointers
- Web Compare modal: `web/components/compare_modal.py`; candidate surface: `web/components/candidate_grid.py`;
  page: `web/pages/joins_lab.py`; anchor viewer: `web/components/anchor_viewer.py`.
- Saved joins write path already exists for desktop/web puzzle: `shared/puzzle_service.py` (joins.db sidecar).
- Off-loop discipline: VS / enrichment / metadata fetches MUST stay off the NiceGUI event loop
  (`web/joins_executor.py`; statically enforced by `tests/test_joins_lab_off_loop.py`).
- "Browse-in-Compare" + info buttons should reuse the existing `/browse` route + FJMS/PGP info dialogs
  rather than re-implement metadata fetching.
