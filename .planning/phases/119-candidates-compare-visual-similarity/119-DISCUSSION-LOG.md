# Phase 119: Candidates, Compare & Visual Similarity - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-19
**Phase:** 119-candidates-compare-visual-similarity
**Areas discussed:** Compare presentation & flow, Visual Similarity ON behavior, Candidate bounding, Filters & default view, Self-match, Multi-select scope, Compare verdict flow

---

## Compare presentation

| Option | Description | Selected |
|--------|-------------|----------|
| Full-screen modal overlay | `ui.dialog` filling viewport, anchor \| candidate panes, extracted /browse viewer per pane | ✓ |
| Inline split panel in-page | Compare opens within the work column; list stays partly visible, smaller images | |
| Reuse desktop's exact model | Defer to whatever the UAT-approved desktop does | |

**User's choice:** Full-screen modal overlay.
**Notes:** Scout confirmed this also matches desktop (modeless 1320×870 window) — web-idiomatic AND parity.

## Compare navigation

| Option | Description | Selected |
|--------|-------------|----------|
| Flip-through (prev/next + verdict) | Compare stays open; ‹ › step candidates in sort/filter order; verdict synced live | ✓ |
| One candidate at a time | Open → verdict → close → pick next | |

**User's choice:** Flip-through (prev/next + verdict).

## Visual Similarity ON behavior

| Option | Description | Selected |
|--------|-------------|----------|
| Desktop's conditional model | ON+query → intersection; ON+empty → union; OFF → text-only with badge | ✓ |
| Always union (merge-in) | ON always adds VS-only look-alikes (more candidates) | |
| User picks union vs intersect | Expose a broaden/narrow sub-control | |

**User's choice:** Desktop's conditional model.
**Notes:** Resolves the requirement's ambiguous "merged / intersection" wording. VS lookup is a local `visual_similarity.db` read (no breaker).

## Candidate bounding (CND-07)

| Option | Description | Selected |
|--------|-------------|----------|
| Paginate through everything | ~24/page, render current page only, no silent cut | ✓ |
| Keep 200 hard cap + notice | Reuse Phase-117 `_MAX_RENDERED_CANDIDATES=200` + banner | |
| Cap + paginate within it | Paginate up to a 500 ceiling matching the fuzzy cap | |

**User's choice:** Paginate through everything.

## Default view

| Option | Description | Selected |
|--------|-------------|----------|
| Grid (image-first) | Thumbnails-first default, toggle to table | ✓ |
| Table (data-first) | Sortable/multi-select table default | |

**User's choice:** Grid — **"The images should be large enough."** (Phase-117 grid uses tiny 48×48 thumbs; Joins-Lab grid needs sizable, visually-triageable cards.)

## Filter UI

| Option | Description | Selected |
|--------|-------------|----------|
| Inline compact filter bar | Always-visible chip/toolbar row, size-mismatch as one-click toggle | |
| Filters in a popover/dialog | "Filters" button opens a popover/dialog (desktop parity) | ✓ |

**User's choice:** Filters in a popover/dialog.

## Self-match (CND-05)

| Option | Description | Selected |
|--------|-------------|----------|
| Banner + 'include anchor' toggle | Notice + toggle to add anchor into candidate list (Phase-108 parity) | |
| Banner only (inform) | Dismissible notice, anchor stays excluded | |
| *(free text)* Nothing, silently ignore the anchor | No banner; anchor silently excluded (= desktop runtime parity) | ✓ |

**User's choice:** "Nothing, silently ignore the anchor."
**Notes:** Diverges from CND-05 / ROADMAP SC#2 (which call for a self-match banner). Logged as a deliberate divergence — verifier must not fail the phase for a missing banner. User declined even a subtle notice when offered.

## Multi-select scope in 119

| Option | Description | Selected |
|--------|-------------|----------|
| Bulk triage now | Selected rows markable Y/?/N in one action; selection ready for Phase 120 actions | ✓ |
| Selection state only | Multi-select inert until Phase 120 attaches actions | |

**User's choice:** Bulk triage now.

## Compare verdict flow

| Option | Description | Selected |
|--------|-------------|----------|
| Auto-advance to next | Recording a verdict jumps to next candidate; ‹ › still allow manual stepping | ✓ |
| Stay on current | Verdict recorded, stay put; click › to advance | |

**User's choice:** Auto-advance to next.

---

## Claude's Discretion

- Table default sort column + the VS-rank-when-👁-on sort switch.
- Exact thumbnail/card dimensions ("large enough") + grid columns-per-row breakpoints + page-size (~24) + pagination control styling.
- Empty / disabled / no-VS-data / empty-intersection state wording for the 👁 toggle.
- Whether the parity text-filter field is included in the filter dialog.
- Self-match internal handling beyond silent exclusion (no UI either way).
- Compare launch affordances beyond card/row/double-click; verdict-button layout.

## Deferred Ideas

- Bulk Add-to-Puzzle / Add-to-List / Add-as-join / Export from selected candidates → Phase 120 (ACT-01/02/03).
- Cross-refresh / cross-navigation persistence of triage/filter/view + re-run-on-restore → Phase 120 (PST-01..03), incl. the `resolves_phase: 120` "results survive navigation" todo.
- Self-match banner / "include anchor" toggle → declined; possible later polish.
- Complete i18n / RTL / Hebrew-leak audit → Phase 121.
