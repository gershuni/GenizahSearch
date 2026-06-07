# Phase 109: Visual-Similarity Merge & Soft-Retire - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-07
**Phase:** 109-visual-similarity-merge-soft-retire
**Areas discussed:** Source semantics, VS volume / noise, Soft-retire depth, Parity & transition

---

## Source semantics

| Question | Options | Selected |
|----------|---------|----------|
| Visual source on select | Auto-load immediately / Require Find click | **Auto-load immediately** ✓ |
| Combined with empty builder query | Degrade to Visual-only / Require a query / Warn | **Degrade to Visual-only** ✓ |
| Other-side (p±1) builder effect on VS | No — VS ignores both builders / Other-side narrows VS too | **No — VS ignores both builders** ✓ |
| ★both keying | Yes — manuscript-level / Tighten later | **Yes — manuscript-level ★both** ✓ |

**Notes:** Grounded against `merge_candidates` (`shared/joins_lab.py:531` — `vs_by_sid` keyed by
`sys_id`), confirming ★both is manuscript-level by construction.

## VS volume / noise

| Question | Options | Selected |
|----------|---------|----------|
| VS count | Cap top-N (~50) / **Show all up to 200, paginated** / Top-N + control | **Show all up to 200, paginated** ✓ |
| Quality floor | **No floor** / Soft default floor revealable | **No floor — rely on ordering** ✓ |
| Combined balance | Cap VS independently / **Single merged list, paginate** | **Single merged list, paginate** ✓ |
| Empty VS (~50% of mss) | Clear empty state keep text / **Grey out Visual when no VS** | **Grey out Visual when no VS** ✓ |

**Notes:** Because the merged set can reach 200, per-candidate enrichment must be **page-lazy**
(visible 20-card page) on top of batched — that is how SC#3's ~80-candidate budget holds (CONTEXT D-09).

## Soft-retire depth

| Question | Options | Selected |
|----------|---------|----------|
| Normal-mode entry points | **Reroute both → Workbench (Visual)** / Reroute+offer rest / Only ResultDialog | **Reroute both → Workbench (Visual)** ✓ |
| Old normal-mode dialog code | **Mark removable, delete later** / Delete now | **Mark removable, delete later** ✓ |
| JoinsDialog pick-mode | **Keep as-is, untouched** / Route into Workbench | **Keep as-is, untouched** ✓ |
| Web VS dialog | **Desktop-only — leave web untouched** / Also touch web | **Desktop-only — leave web untouched** ✓ |

## Parity & transition

| Question | Options | Selected |
|----------|---------|----------|
| Parity gate | **Both: auto test + manual UAT** / Manual only / Auto only | **Both: auto test + manual UAT** ✓ |
| Cutover | **Reroute now, retain dead code 1 cycle** / Temporary fallback toggle | **Reroute now, retain dead code 1 cycle** ✓ |
| Parity bar | **Same look-alikes + four actions work** / Full visual parity | **Same look-alikes + four actions work** ✓ |

## Claude's Discretion

- Parity-pass record location (lean: 109-HUMAN-UAT scenario + automated test); deprecation marker style.
- ⊙VS card rank/score display; grey-out vs hide for disabled Visual; reroute source=Visual mechanism;
  reuse vs replace of `_enrich_vs_suggestions`.

## Deferred Ideas

Page-weighted ★both; physical deletion of old dialog code (later cleanup); pick-mode→Workbench;
web VS soft-retire; temporary fallback toggle; min-score floor / top-N cap; full visual parity.

---

# Gap-Closure Round 3 (2026-06-07) — UAT REJECTED, 8 change requests (G-06..G-13)

**Areas discussed:** G-08 pick-mode intent, G-06 badge vocabulary, G-13 "see more" message, G-12+G-11 visual/layout
**Locked as specified without discussion:** G-07 (remove duplicate VS buttons), G-09 (remove vs_rank), G-10 (triage second-click undo), G-11 mechanics

## G-08 — JoinsDialog VS button intent (REVERSES G-05)

| Option | Description | Selected |
|--------|-------------|----------|
| Leave dialog → explore in Lab | Button anchors Workbench on Fragment A and closes JoinsDialog; no pick-back; create join via "Add as Join". Retires G-05 pick-callback. | ✓ |
| Keep pick-back, close on pick | Keep "Select as partner" → fill Fragment B; close both after pick. Preserves G-05. | |

**Follow-up — toggle state on open:** Toggle ON / **Toggle OFF (plain)** ✓ — consistent with Find-Joins after G-07.
**Follow-up — pick-callback machinery:** **Keep, marked removable (D-11 one-cycle)** ✓ / Remove now.

## G-06 — Badge vocabulary

| Option | Description | Selected |
|--------|-------------|----------|
| One eye for all VS | Eye 👁 replaces BOTH ★both and ⊙VS; same eye on the toggle button; tooltip "visual similarity"; no rank. | ✓ |
| Eye for intersection, keep ⊙VS | Eye = ★both only; pure look-alikes keep ⊙VS (no rank). Two symbols. | |

## G-13 — "Turn off Visual Similarity to see more results" message

| Question | Options | Selected |
|----------|---------|----------|
| Placement | **Distinct hint line near results grid** / Appended to status line | **Distinct hint line** ✓ |
| Empty-intersection | **Combine: "No look-alikes match this search — turn off VS to see all results"** / Keep empty message only | **Combine both** ✓ |

**Trigger (not asked — locked from Hillel's wording):** shown whenever toggle ON and results are shown (pure-VS + intersection).

## G-12 — Visibly-ON toggle

| Option | Description | Selected |
|--------|-------------|----------|
| Accent fill when checked | Colored fill (blue bg, white text) on :checked. | |
| Sunken/border only | Stronger border + sunken look, no color fill. | ✓ |
| Add a checkmark to label | "👁 Visual Similarity ✓". | |

**Caveat captured:** bare native sunken state was the original too-subtle complaint — planner must use an explicit :checked rule with a clearly heavier/darker border so ON is unmistakable.

## G-11 — Folio nav + triage on one row

| Option | Description | Selected |
|--------|-------------|----------|
| Triage left, folio right | [Y][?][N] … [▶ p.N ◀] | |
| Folio left, triage right | [▶ p.N ◀] … [Y][?][N] | ✓ |
| All adjacent, left-aligned | [Y][?][N] [▶ p.N ◀] … | |

## Claude's Discretion (round 3)

- Exact eye glyph rendering (emoji 👁 vs themed icon); precise :checked border weight/shade for G-12;
  exact wording/styling of the G-13 hint line; how `self.close()` vs reject/accept is invoked in `_show_vs_picker`.

## Deferred / Out of scope (round 3)

- Physical deletion of `_show_vs_dialog`, the pick-callback machinery, and the G-07 reroute handlers
  (all marked-removable, one-cycle soft-retire per D-11 — a later cleanup phase).
