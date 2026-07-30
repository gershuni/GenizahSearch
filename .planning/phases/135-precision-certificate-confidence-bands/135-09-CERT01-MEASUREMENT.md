# CERT-01 — Measured Result (tier_a precision)

**Graded:** 2026-07-28 by the owner (grader `Hillel`), all 280 cards of the frozen deck.
**Gate:** `scripts/verify_cert01_grading.py` → **12/12, exit 0.**
**Outcome:** **PASS** against the D-07 Strict floor (lower confidence bound ≥ 0.85).

Provenance: measured against the deployed v2 sidecar
`discovery-v1-33499c5b…` / frame `53725098…`, over the immutable pre-registration
`cert01_prereg.json` (`report_id` recomputes) and the deck bound by
`cert01_deck_manifest.json`. Ledger: `same_work_spike/probe/review/cert01_deck_verdicts.json`
(gitignored; masking scan clean). Grading was fully catalogue-blind — **zero reveals across
all 280 cards.**

---

## 1. Headline — the pre-registered estimand

Weighted to the shipped `tier_a` population (D-08), clustered by the frozen physMS
`unit_key` map, via the REUSED `e1_deck.comp_bootstrap` (B=10,000, seed 7):

| | Point | 95% CI | vs 0.85 floor |
|---|---|---|---|
| **Weighted (THE GATE)** | **0.9382** | **[0.9084, 0.9644]** | **PASS** |
| Unweighted | 0.8995 | [0.8558, 0.9401] | PASS (narrowly — 0.8558) |

219 determinate candidate cards over 208 physMS clusters. 1 INS.

**Honest caveat:** the pass is weight-dependent. Unweighted, the lower bound clears the
floor by 0.006. It passes because the weak strata carry small weight and the strong
stratum is large — a legitimate estimand, but not a wide margin on the raw sample.

The pre-outcome OC table predicted this correctly in advance (60–73% pass probability at
true p=0.90, near-certain at 0.95); realized weighted p ≈ 0.94 sat in the likely-pass
region. The measurement behaved as designed.

## 2. Verdicts by card role

| Role | n | A | B | C | INS | A / determinate |
|---|---|---|---|---|---|---|
| candidate (the estimand) | 220 | 197 | 8 | 14 | 1 | **0.900** |
| gold (repeat consistency) | 20 | 19 | 1 | 0 | 0 | 0.950 |
| diagnostic — demoted | 20 | 10 | 1 | 9 | 0 | 0.500 |
| diagnostic — retained | 20 | 12 | 4 | 4 | 0 | 0.600 |

**Gold consistency 19/20 (0.95)** against the prior adjudication — grading is stable.

**Diagnostic (D-17 classifier validation, never adjudication evidence):** demoted cards
graded 50% A vs retained 60% A. A real signal in the right direction, but **weak
separation** — the chronological demotion rule is only mildly enriched for false
positives. See §4.

## 3. Per-stratum spread — the finding that matters for display

| Stratum | Weight | n | A-rate |
|---|---|---|---|
| ja:high | 9.3% | 27 | **1.000** |
| ja:medium | 1.5% | 17 | **1.000** |
| sefaria:high | 66.7% | 102 | 0.980 |
| sefaria:medium | 12.5% | 31 | 0.839 |
| msource:high | 8.4% | 25 | 0.760 |
| **msource:medium** | **1.6%** | **17** | **0.471** |

A reader looking at an `msource:medium` claim faces ~47% precision, not 94%. **Publishing
the weighted headline without the per-stratum spread would mislead anyone in the weak
tail** — the spread belongs on the BAND-05 methods page.

## 4. Error concentration — the liturgical-containment class

22 of 220 candidate cards were non-A, and they are strikingly concentrated:

| Work (opaque id) | Non-A cards | source_corpus |
|---|---|---|
| `w000176` | **10** | msource |
| `w000112` | 3 | sefaria |
| `w000179` | 2 | msource |
| `w000177` | 2 | msource |
| 5 others | 1 each | mixed |

**One work causes 45% of all measured error; the top three cause 68%.** 15 of the 22
errors sit in the two msource strata.

**Mechanism (owner-identified during grading):** a later halakhic code *embeds* the full
liturgy, so a Genizah page carrying a common prayer matches the code rather than the
prayer-book. This is **containment, not coincidence** — hence systematic rather than
scattered, and the same class as the already-ratified `w000177 → w001159` directional
embed in the owner census.

**Why D-17 does not catch it:** D-15 removed the `work_relations` table and handed
containment to the D-17 chronological demotion. But the code POSTDATES the liturgy it
quotes, so demoting the later work is backwards for this case. The diagnostic sample's
weak 50%/60% separation is consistent with that. A containment-aware rule (or a
liturgy-specific guard) is the real fix — logged as a v2.1 candidate, not a v2 defect.

## 5. Public-scope subgroup — NOT pre-registered, label it as such

The owner's 2026-07-27 publication strategy holds M-source, JA and R-source as private
*inputs*: first public scope = Sefaria-direct ∪ propagated. Since the public asset is a
different population, the frozen all-strata estimand does not directly certify it.

| Scope | Shipped rows | n | Precision | 95% CI | vs 0.85 |
|---|---|---|---|---|---|
| Pre-registered (all strata) | 134,123 | 219 | 0.9382 | [0.9084, 0.9644] | PASS |
| **Public v1 — Sefaria only** | 106,233 | 133 | **0.9580** | **[0.9240, 0.9847]** | PASS |
| Sefaria + JA | 120,736 | 177 | 0.9631 | [0.9336, 0.9866] | PASS |
| msource only (private) | 13,387 | 42 | 0.7140 | [0.5551, 0.8511] | **FAIL** |

**Status of these rows: post-hoc SUBGROUP analysis, descriptive only.** They are NOT the
pre-registered measurement and must never be presented with pre-registration status.

Two things do strengthen the Sefaria-only figure: the visibility boundary was fixed by an
independent owner decision on **2026-07-27**, BEFORE the deck was drawn on **2026-07-28**
(so the subgroup was pre-specified, not selected to flatter the outcome), and n=133 is a
reasonable sample. **The clean move before publishing it is to pre-register the public
estimand ahead of the public bake and measure it there.**

Also note the failing population is precisely the one being withheld, and `msource`'s
0.714 rests on n=42 with a CI spanning 0.555–0.851 — directional, not precise. Do not
publish it as a figure.

## 6. What has NOT been done

- **`band_precision` is unchanged.** `tier_a` still carries no measured precision
  (row id=5). Writing 0.938 in requires a re-bake through `--precision-spec` plus a
  production asset deploy — a separate, owner-gated decision.
- No public surface renders any of these numbers. The discovery flag remains OFF.
- The word "certified" is not used anywhere; posture stays "expert-measured · independent
  audit pending" (D-06), with the independent audit still the deferred FUT-07 gate.
- CERT-01 in `REQUIREMENTS.md`: the measurement now EXISTS and passes, but REL-01 also
  requires the CERT-02 outcome copy applied and the number published, so the requirement
  is satisfied on its measurement clause only — see the Phase-139 checklist.

---

*Phase 135, plan 09 — CERT-01 measured, PASS, 12/12 validator. Supersedes
`135-09-OWNER-ATTESTATION.md`.*
