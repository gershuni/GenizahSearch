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
