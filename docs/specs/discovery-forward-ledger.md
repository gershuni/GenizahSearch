# Discovery Forward Ledger — probe-validated ideas awaiting a home

**Purpose:** the SEED-029 probe (`same_work_spike/probe/` — gitignored,
local-git-only) validated more machinery than Phase 134/135 ships. This
tracked ledger keeps those ideas findable so they survive outside the spike
tree. Review at each phase boundary (136–139) and at gen-2/v2.1 planning.

**Status legend:** UNCLAIMED = no phase/plan owns it · PARKED = deliberately
shelved with recorded rationale · PARTIAL = partially absorbed.

## A. Unclaimed (validated in the probe, no current owner)

| # | Idea | One-liner | Probe source | Suggested slot | Status |
|---|---|---|---|---|---|
| 1 | Page-chain evidence | 9,279 multi-page chains (117 ≥3 pages) — near-certain same-work assertions; promote to an evidence_source or corroboration booster + propagate IDs along chains | SYNTHESIS A7; `chain_pages` | v2.1 / Phase 136+ | UNCLAIMED |
| 2 | Residue mining | Rank unidentified units by witness count = "most-copied Genizah texts no reference corpus contains" (70% RNL, Karaite liturgy/piyyut); headline scholarly product | SYNTHESIS B2; `residue_most_copied.py` | Phase 138 (verify leads-queue scope includes it) | UNCLAIMED |
| 3 | Fragmentary-tail catalog auto-validation | Score catalog-description agreement for motif +1/+2 gains; AGREE ≈ free external validation (~75%), DISAGREE = review queue | SYNTHESIS B3; `frag_tail_catalog_check.py` | Phase 138 | UNCLAIMED |
| 4 | Conformal+FDR thresholds | Replace hand-tuned density gates with FDR-bounded operating points from target-decoy nulls; validated on pilot DB, full calibration never scheduled | SPIKE-BRIEFS A5; `probe_conformal_fdr.py` | gen-2 engine | UNCLAIMED |
| 5 | Motif/units product layer | 43,278 brakhah-granular motifs + 81,365 passage units exist in the map but have zero product expression; Leiden/Louvain motif-v2 spike unclaimed | SYNTHESIS A6; `motif_v2_communities.py` | post-139 product | UNCLAIMED |
| 6 | Interleaved-reference builder + bitext harvest | Synthetic Bible×Onkelos/Tafsir interleaved refs as a Track-1 reference type; interleaved pages double as a verse-aligned Heb↔JA bitext (gold for any future semantic layer) | SYNTHESIS A3/S1; `probe_interleaved.py` | gen-2 reference build | PARTIAL (adjudication done; D-17 multi-register invariant protects the class; builder unbuilt) |
| 7 | Reversed-direction finds | Pages the edition quotes (MS is the SOURCE) — a product class that falls out of gen-2's direction-aware router | HANDOFF-MAPV2 §4 | gen-2 → later surface | UNCLAIMED |
| 8 | Confusion-matrix engine assets | Measured HTR confusion matrix as (a) edit-cost prior in the matcher, (b) visual-confusability hard negatives, (c) stemma innovation weighting — never wired in | METHOD §3/§6.2; SYNTHESIS S3 | gen-2 heavy run (cheapest quality upgrade to ride along) | UNCLAIMED |
| 9 | Per-work quotation profiles | Citation grades cluster at coverage 0.43–0.50 for some works; refines the flat 0.45/0.15 coverage thresholds that Lever-1 hardcodes | HANDOFF | v2.1 | UNCLAIMED |
| 10 | MAPV2-8/-9 engine debts | Revert 152 severe HTR-substitution pages; re-key the cite-formula exemption (currently re-admits the geonic-digest family); JA/HTR-tolerant citation markers | HANDOFF-MAPV2 §6–8 | MUST ride any gen-2 heavy re-run | UNCLAIMED |

## B. Parked with recorded rationale (keep findable, no action owed)

- **Track-3 semantic layer (JABERT/RamBERT)** — parked by the 2026-07-09 goal
  pivot (same-work witnesses are copies; lexical is the right tool). Phase-0
  dataset + training script exist in the spike; revivable if lexical recall
  plateaus.
- **Stemma probe (D1)** — hard-capped exploratory; gate = rite-correlation
  signal on the BH witness web.
- **Matres-light normalization view; JA-stratum evaluation; FRAG-3
  glossary/lemma matcher; E1 Bible-stratum certification (rejected: Bible IDs
  aren't discoveries).**
- **SEED-032** (surface new/uncataloged discoveries above known works) —
  seeded in commit `a3573566` but referenced by no 135/136 planning doc;
  needs an explicit disposition at Phase 136 or 138 planning.

## Masking

Tracked file: restricted corpora referenced only as "M-source" / "R-source".
