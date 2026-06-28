---
phase: 129-library-filter-search-browse-by-identification-seed-026
gate: codex-plan-preflight (gap-closure plans 129-05/06/07)
reviewer: codex (gpt-5.x via codex exec)
reviewed: 2026-06-28
verdict: APPROVE WITH CHANGES → resolved (commit 795ccda1)
findings: { blocker: 0, high: 1, medium: 1, low: 1 }
status: resolved
---

# Phase 129 Gap-Closure — Codex PLAN Pre-Flight (cross-AI gate)

Independent Codex pre-flight of the 3 gap-closure plans (129-05/06/07) AFTER they passed
the internal gsd-plan-checker, BEFORE any code is written — satisfying the phase's
Codex-review-before-code gate (locked decision #1) for the gap-closure cycle.

**Verdict: APPROVE WITH CHANGES.** Codex independently CONFIRMED the two riskiest claims:
- GAP-F handoff ordering is TRUE — `search.py:187-189` loads `search_library_filter` before
  `consume_incoming_filters` at `:199`, and the consume return prevents the later
  `load_filter_state` branch from clobbering the setattr.
- GAP-D chip relocation is safe — dropping `or bool(search_state.library_filter)` from `has_any`
  does not hide the domain/text/material/measurement chips (they still drive
  `_has_active_filters()` / `_pos_active`).

## Findings (all resolved in commit 795ccda1)

### HIGH — all-unchecked dialog state collides with the "empty = show all" sentinel (129-05/06/07)
The checkbox dialog uses the locked inclusion model (checked = show; checked set → `library_codes`;
all-checked ⇒ `[]`/None = show all). But an all-UNCHECKED apply also yields `[]` → "show all",
the opposite of "hide all". **Fix:** the all-unchecked applied state is made UNREACHABLE — no
"Select None" that applies empty; Apply/OK disabled (+ defensive Python short-circuit) at
zero-checked; "Select all" = clear filter. Data layer unchanged (`[]` stays "show all"); the
degenerate state is simply prevented in the UI. Applied to web search, web catalog, and the
desktop `LibraryFilterDialog` (which therefore diverges from `DomainFilterDialog`'s exclusion-
semantics Select-None). Encoded as truths + guard tests + acceptance_criteria + STRIDE rows.

### MEDIUM — desktop recompute path drops the library restriction (129-07)
Adding `pre_search_filters['library']` alone is insufficient: `_remove_filter()` recomputes via
`FilterCountWorker` (gui_threads.py ~1240), which ignored `library` — so removing a *different*
chip after a catalog→search handoff silently dropped the library restriction. **Fix (approach a):**
`FilterCountWorker` gains an optional `meta_mgr` and resolves+intersects `filters['library']` via
`resolve_library_sys_ids` in `run()`; ALL FOUR recompute/restore sites
(`_remove_filter` + 3 session/history restore sites) pass `meta_mgr=self.meta_mgr` (broader than
the one site Codex named); the unrelated `dialogs_filter.py` caller defaults to None (no-op).
gui_threads.py added to 129-07 `files_modified`. Encoded as a recompute-preserves-library test +
acceptance_criteria.

### LOW — 129-05 plan text wrongly referenced a "printed chip" in `_update_chip_bar` (129-05)
Printed is a button, not a chip. **Fix:** corrected all plan/test assertions to the chip types
that actually exist in `chip_bar_container` (text-position/domain/author/work/date/material/text/
measurement/manuscript-count/library); removed any "printed chip" assertion.

## Post-fix validation
- `frontmatter.validate --schema plan`: 3/3 valid. `verify.plan-structure`: 3/3 valid.
- Plan-checker (pre-Codex) had already returned VERIFICATION PASSED; these edits are additive.

No BLOCKER. All HIGH/MEDIUM/LOW resolved → gate satisfied; plans cleared for execution.
