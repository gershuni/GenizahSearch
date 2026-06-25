# Phase 117: Vertical Spine - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-17
**Phase:** 117-vertical-spine
**Areas discussed:** Page layout & responsive shape, Cold-start entry & empty state, Spine builder scope, Anchor image viewer fidelity, Spine persistence scope

---

## Gray-area selection (multiSelect)

User chose to discuss **all four** offered gray areas: Page layout & responsive shape,
Cold-start entry & empty state, How minimal is the spine builder, Anchor image viewer fidelity.
(The rest of the phase was treated as locked by REQUIREMENTS.md + the Codex pre-lock critique.)

---

## Page layout & responsive shape

| Option | Description | Selected |
|--------|-------------|----------|
| Anchor pinned to a side, work column scrolls | Anchor (image + RTL transcription) sticky/in-view; builder + grid in the scrolling main column. Direction-aware, collapses to stacked on narrow. | ✓ |
| Two balanced columns | Anchor + work column side-by-side ~50/50, each scrolls (closest to desktop QSplitter). | |
| Single scroll column | Anchor collapsible on top, builder, grid below; one page scrolls. | |

**User's choice:** Anchor pinned to a side, work column scrolls (chose the diagrammed preview).
**Notes:** Honors the Joins Lab's "keep ONE anchor in view" principle. Claude decided the
follow-ons unilaterally (no objection): direction-aware side (reading-start: left EN / right HE),
narrow screens stack with anchor collapsible on top.

---

## Cold-start entry & empty state

| Option | Description | Selected |
|--------|-------------|----------|
| Shelfmark OR sys_id in one smart box | Single input; shelfmark resolved via existing normalization/search. | (partial) |
| Shelfmark box with typeahead suggestions | Live lookup as you type. | |
| sys_id only | Simplest; poor cold-start UX. | |
| **Other (free text)** | "Shelfmark or sys_id AND choose from list button" | ✓ |

**User's choice:** Smart box (shelfmark OR sys_id) **AND** a "choose from a list" button —
mirroring the desktop's shelfmark-entry + 📋 pick-from-list.
**Notes:** A follow-up clarified the list-source collision with the no-login-wall rule (FND-06).

### Follow-up — "choose from list" source & anon behavior

| Option | Description | Selected |
|--------|-------------|----------|
| Saved lists; prompt login on click | Always visible; pulls from `/lists`; anon click → login prompt. | ✓ |
| Saved lists; button hidden until logged in | Same source; hidden for anon. | |
| Defer the list button to Phase 120 | Ship only the smart box this phase. | |

**User's choice:** Saved lists; prompt login on click (button always visible).
**Notes:** Empty state decided by Claude — centered "pin an anchor" panel + box + button +
one-line Lab description.

---

## How minimal is the spine builder

| Option | Description | Selected |
|--------|-------------|----------|
| Multi-line textarea → one BuilderRow per line | Each line → `BuilderRow(term=line)` → `SideQuery` → `compose()` → adapter → `dedup_candidates`. Exercises the real multi-row model. | ✓ |
| Row widgets now, minus modifiers/toggles | Build per-line row UI this phase; defer only ⚙ + toggles. | |
| Single one-line search box | One input → one BuilderRow; never exercises multi-row compose. | |

**User's choice:** Multi-line textarea → one BuilderRow per line.
**Notes:** Grounded in `shared/joins_lab.py` `BuilderRow`/`SideQuery`/`compose`. Claude set the
spine default to exact mode (variants off) — toggles are Phase 118 (BLD-04).

---

## Anchor image viewer fidelity

| Option | Description | Selected |
|--------|-------------|----------|
| Reuse /browse's viewer (extract to shared) | Lift `manuscriptViewer` JS + proxy resolution + folio nav into reusable form; 119 Compare reuses it. | ✓ |
| Lighter spine viewer, upgrade later | Simpler image + basic nav now; redo zoom/pan later. | |

**User's choice:** Reuse /browse's viewer (extract to shared).
**Notes:** `/browse` already has exactly the zoom/pan/rotate/folio-nav + per-provider proxy that
ANC-01/ANC-02 require. Extraction refactor pays off twice (anchor + Phase 119 Compare).

---

## Spine persistence scope (additional gray area Claude surfaced)

| Option | Description | Selected |
|--------|-------------|----------|
| Persist last anchor; bare /joins-lab restores it | Versioned schema + write anchor sys_id via `safe_user_*`; URL param wins when present. | ✓ |
| Define schema only, persist nothing yet | Establish chokepoint + version field; anchor always from URL; 120 fills persistence. | |

**User's choice:** Persist last anchor; bare /joins-lab restores it.
**Notes:** Gives SC#5's no-bleed test a real write to exercise; full builder/triage/filter
persistence + re-run-on-restore stays locked to Phase 120.

---

## Claude's Discretion

- Direction-aware anchor side, narrow-screen stacking, exact breakpoints/widths.
- Spine search default = exact mode (variants off, no modifiers).
- Empty-state panel composition; optional "open in /browse" link on grid cards.
- `safe_storage` key name + dict shape (must carry `schema_version`).
- The exact deep-link param subset surfaced in 117 (minimum `sys_id`; document whatever is built).

## Deferred Ideas

- Typeahead/autocomplete on the cold-start box → 118+ polish (if desired).
- Builder modes / global toggles → Phase 118.
- Candidate triage / per-card actions / table / Compare / VS → Phases 119–120.
- Full builder/triage/filter persistence + re-run-on-restore → Phase 120.
- 7 low-score `todo.match-phase` hits reviewed and NOT folded (all spurious keyword matches).
