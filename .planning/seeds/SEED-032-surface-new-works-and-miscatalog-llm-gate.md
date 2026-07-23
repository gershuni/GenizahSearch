---
id: SEED-032
status: dormant
planted: 2026-07-23
planted_during: v9.0.0 Discovery milestone (raised during Phase 135 planning)
trigger_when: when planning Phase 136 (Discovery claim-rendering surfaces — connections panel + /work/{id}); also relevant to Phase 138 (leads)
scope: medium
---

# SEED-032: Surface new/uncataloged discoveries above known works; treat miscatalog as a distinct, LLM-gated category

## Why This Matters

The highest-value output of Discovery is not re-confirming what catalogs already know — it is surfacing what they DON'T: identifications for uncataloged/undercataloged fragments (gaps), and, more valuable still, disagreements with existing catalog entries (errors). A default view that highlights NEW identifications above already-known works lets a scholar go straight to the frontier. The data already distinguishes the two: every claim carries an `is_new` flag (`discovery-sidecar-schema-v1.md`, C-8; ~43,046 rows are `is_new=0` / already cataloged, the rest flagged new).

But Phase 134 deliberately made `is_new` **"a flag, NOT a surface."** Turning it into a ranked/filtered surface (new-above-known, "show only new" default, "see all" toggle) is a NEW Phase-136 design decision built on top of the flag — this seed is that decision waiting to be made.

## When to Surface

**Trigger:** when planning Phase 136 (the Discovery claim-rendering surfaces — connections panel + `/work/{id}`). Also relevant to Phase 138 (leads).

## The Idea, In Full

### (a) New / uncataloged / undercataloged — surface above known
Driven off the existing `is_new` flag: default/highlight the not-in-catalog identifications above already-known works; let the user filter to "only new" or toggle "see all." Low-risk — reuses the "show more possible matches" pattern from the Phase-135 band-display work; the flag already exists in the sidecar.

### (b) Miscatalog — a distinct, richer, harder category
Beyond uncataloged (gaps) there is **miscataloged**: the catalog says work X but we identify work Y — a *disagreement*, not a gap. Potentially the most valuable class of all (finding catalog ERRORS), but NOT captured by the binary `is_new` flag; it would need its own field/treatment.

**Hard problem (owner-flagged):** it is often hard to tell a *genuine* miscatalog from a *benign/generic* mismatch —
- **Cross-language:** the catalog describes a fragment in (Judeo-)Arabic while our identification is labeled in Hebrew → a naive title/string compare flags a false "disagreement."
- **Generic descriptions:** catalog says "commentary" / "prayers" / "fragment" — neither confirms nor contradicts our identification.

So a naive comparison will over-flag miscatalogs and cry wolf.

**Candidate mitigation — NEEDS INVESTIGATION (owner: "an idea we need to check"):** an LLM adjudication gate that judges whether the catalog entry and our identification genuinely disagree (real miscatalog) vs. are compatible / generic / cross-language-equivalent. Even WITH an LLM gate, confidence is bounded — but *more* confident than string matching. The LLM-gate approach is itself unproven here and must be validated (reliability, false-positive rate, cost, prompt design, how to keep it honest) via a spike **before** any miscatalog surface ships.

**Hard discipline — catalogue-blind (`feedback_catalogue_never_evidence`):** the catalog is a recall yardstick, NEVER acceptance evidence. A miscatalog surface must **present the disagreement for a scholar to judge** — it must never assert "the catalog is wrong" as a system verdict. The LLM gate is a display/triage filter, not an adjudicator of truth.

## Scope Estimate

**Medium** — two separable parts:
1. **The surfacing feature (phase-sized):** new-above-known ranking + "show only new" default + "see all" toggle, off the `is_new` flag. Straightforward once the flag becomes a surface.
2. **The miscatalog investigation (spike-sized, must precede any miscatalog surface):** the LLM-gate feasibility spike (accuracy on cross-language / generic cases, false-positive rate, cost), plus a new field/model for catalog-disagreement. Ships only if the spike clears — and always under the catalogue-blind constraint.

## Breadcrumbs

- `docs/specs/discovery-sidecar-schema-v1.md` — the `is_new` flag (C-8); the two shipped evidence sources (`track1_direct` + `propagated`).
- `.planning/seeds/SEED-029-fragment-textual-similarity-same-work-detection.md` — catalog-identity propagation (the reserved future MS-to-MS + external-catalog/title/FGP evidence_source); adjacent territory.
- memory `feedback_catalogue_never_evidence` — the governing discipline.
- `.planning/ROADMAP.md` — Phase 136 (claim surfaces) + Phase 138 (leads).
- `shared/discovery_service.py` / `web/discovery_assets.py` — where surfaces read the sidecar bands/flags.

## Notes

Raised by the owner (Hillel) on 2026-07-23 during Phase 135 planning, as a natural use of the `is_new` flag + the band-display "show more" pattern. Part (a) is low-risk and phase-ready; part (b) is gated on the LLM-feasibility spike and the catalogue-blind constraint. Captured via the seed workflow (numbered SEED-032 = max+1, since the sequence has a gap at 019 and the default count-based id would collide).
