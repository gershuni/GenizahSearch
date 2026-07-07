# SEED-029 Separability Probe — RESULTS (2026-07-06)

> Internal lab log. For the standalone technical method report (written for external
> collaborators — full pipeline, parameters, calibration, evaluation), see **`METHOD.md`**.

One-day feasibility probe for shared-passage detection over MiDRASH HTR
(SEED-029 revised architecture: normalize → DF-banded char-5-gram seeds →
diagonal two-hit → alignment verify). Run entirely on the dev box
(12C/24T, 63 GB). All code under `same_work_spike/probe/scripts/`,
all outputs under `data/` + `results/`.

## VERDICT: GO — with one engineering frontier and one calibration task

The candidate stage lost **zero** ground-truth pairs (100% recall on all three
families, inside a 17,228-page corpus with 10K random background pages). The
verifier finds real parallels with correct span boundaries (manually confirmed
in Hebrew/Judeo-Arabic). The two open items are **candidate-volume engineering**
(31.7M candidate pairs at pilot scale — needs diagonal-keyed counting before
full corpus) and **acceptance-threshold calibration** (density 0.30 clips
liturgical/high-noise true pairs; the right boundary is a sloped
length×density rule).

## The pilot

| Component | Value |
|---|---|
| Corpus | 17,228 pages: 1,393 BH witnesses + 1,088 joins + 4,963 title-groups + 740 FGP-overlap + 10,000 random background (reservoir, seed 43) |
| Source | `Transcriptions.txt` (948,549 page records; streamed in 63 s) |
| Normalization | NFC, strip nikud/marks (incl. U+0307), final-letter fold, **space-stripped letter stream** + offset map (`normalize.py`) |
| Engine | `engine.py` — inverted char-k-gram index, DF-band, diagonal two-hit, span + Levenshtein verify (rapidfuzz); per-pair anchor cap 64 (memory guard) |
| Candidate mode | k=5, df_drop=100 pages, min_anchors=2, band=±20, min_span=25, max_density=0.30 |
| Ground-truth mode | k=4, no DF drop (posting cap 800), min_anchors=2 |
| Runtime | full pilot end-to-end ≈ 10 min single-threaded Python (candidates 334 s, verify 270 s) |

## Headline numbers

| Measurement | Result |
|---|---|
| **Candidate recall vs Tier-1** (joins / titles / BH) | **1.00 / 1.00 / 1.00** |
| Verified recall at density 0.30 | 0.94 / 0.90 / **0.50 (BH — threshold-clipped)** |
| Empirical letter-CER (HTR vs 209 FGP human transcriptions) | **micro 20.1%, median 16.6%** (p25 8.9%, p90 42%) — upper bound |
| Top confusions | י↔ו (620), ד↔ר (597), ב↔כ (485), ח↔ה, ה↔ת/ק — the classic set, now with weights (`results/confusion_matrix.json`) |
| Join groups sharing ANY wording | **1 of 114 (1%)** — physical joins are textually invisible; corpus fact |
| Title groups textually connected | 9 of 14 (64%); heterogeneous (ספר מצוות 29/33 MSS, 1,506 pairs; שו"ת ראב"ם ומגילת תענית 0) |
| BH witnesses connected, density sweep | 0.30→101, 0.35→163, 0.40→220, **0.45→294 of 428 (69%)** |
| BH identification (top partner is a known BH witness) | 65% of 232 connected pages (at strict 0.30) |
| Candidate volume (the frontier) | 31.7M pairs = 21% of all pilot pairs; verify prunes to 4,417 |
| Unplanned discovery | 2 background MSS carrying the same halakhic text on אונאה (density 0.15, 1,658 letters) — found among 10K random pages |

## Findings

**F1 — Seeds survive the noise; the architecture is right.** At measured CER
16–20%, char-5-gram seeds + two-hit still captured every Tier-1 pair. The
LSH design killed in the seed revision would have been blind here (predicted
Jaccard 0.17–0.27 → 2–15% capture); the probe retroactively validates the
pivot to seed-and-extend.

**F2 — The verifier, not the seeds, is the only loss point — and it's a
threshold, not a design flaw.** Density ≤0.30 sits at the two-sided noise
floor (two CER-15% copies of identical text differ by ~26–30%). True-pair
densities cluster 0.13–0.30 for literary works, 0.24–0.45 for liturgy
(nusach variance on top of noise). Fix: sloped boundary — accept long spans
at higher density (e.g. len≥200 & density≤0.45 / len≥60 & density≤0.30),
ROC-tune on the scatter (`results/separability.png`). The exact-duplicate
line at density 0.000 and the short+dense gray cloud are the FP frontier.

**F3 — DF-banding HELPS liturgical recall (assumption inverted).** Under a
per-pair anchor budget, common grams dilute anchors across many diagonals;
DF-banding spends the budget on distinctive grams that cluster on the true
diagonal. df=50/30 found 415/699 BH pairs vs 135 at df=None. The "whitelist
for formulaic classes" worry is largely dissolved — DF-banding is the
mechanism that makes liturgy WORK.

**F4 — Physical joins ≠ textual parallels: 1%.** Empirically settled. Joins
are useless as recall positives (SEED-029 eval design confirmed); the one
"connected" join group was two catalog records of the same Talmud leaf.

**F5 — Catalog-level same-work ≠ passage overlap.** 64% of title groups
connect; the zeros (שו"ת ראב"ם, מגילת תענית, נואדר אלפלאספה) are
different-parts-of-work or tiny/noisy pages. Tier-1 filtering (verifier over
known pairs) is the correct recall denominator, as designed.

**F6 — Stage-0 dedup is mandatory and easy.** The corpus contains exact
duplicates under different sys_ids — `997…` NLI catalog variants sharing the
SAME IE/P/FL image ids (22 of top-25 cross pairs; density exactly 0.0).
Dedup key: FL image id (trivial), plus near-dup pass for re-photographed
leaves (the join-group case).

**F7 — NEW false-parallel class: microfilm target sheets / catalog cards.**
Pages that are HTR gibberish of scale bars + the FGP card template
(סימן/תוכן/מחבר/שנה/הערות) match each other across unrelated manuscripts.
Stage-0 filter: template-keyword + low-Hebrew-entropy heuristic.

**F8 — Candidate volume is the scale frontier.** Two-hit without diagonal
consistency at candidate stage → 21% of all pairs become candidates. At 948K
pages this explodes. Mitigations (in order): (1) key the accumulator on
(pair, diagonal-bucket) so the two-hit test IS diagonal-consistent — cheap,
kills most background pairs; (2) min_anchors=3; (3) per-page candidate caps
with logging; (4) numpy/Rust posting-list representation (Python dicts hit
15 GB at 17K pages — fine on this box, won't scale 55×).

**F9 — CER heterogeneity demands per-genre thresholds.** p25=8.9% vs
p90=42% — one global acceptance threshold cannot serve both square-script
Bible hands and cursive documentary hands. The confusion matrix
(`results/confusion_matrix.json`) enables confusion-weighted alignment costs
(ד/ר, י/ו, ב/כ substitutions should cost less than random substitutions).

**F10 — BH experiment: the witness index is the better oracle.** Candidate
mode at df=30 found 738 witness pairs vs the GT run's 117 — the "oracle"
suffered the same threshold clipping. For liturgy, recall should be measured
against the human witness index (any cross-witness hit is presumptively true
after dedup/target-sheet filtering), not against a same-pipeline GT run.

## BH experiment detail (מפתח כתבי היד, ספר ברכת המזון)

- Index: 484 sigla parsed from the docx (542 table rows + 54 tzerufim lines);
  **471 resolved (97.3%) → 597 sys_ids → 1,393 HTR pages (556 sys_ids, 93%)**.
  Unresolved: 8 RNL Evr. III B (absent from libraries.csv), Budapest Gen,
  Sassoon/Letchworth (not in corpus). Resolver handles ranges, parentheticals,
  Or.1080→CUL, NLI =4 markers, codex→leaves prefix fallback.
- Witness connectivity vs density: 24% → 38% → 51% → **69%** (0.30→0.45).
  Remaining 31% at 0.45: tiny fragments, extreme noise, or BH sections with
  no counterpart preserved (the index includes witnesses of *any* part of BH).
- Identification framing: of BH pages with any partner at 0.30, 65% point to
  another known BH witness; the rest share OTHER liturgy (Hallel/psalms) with
  non-BH siddur pages — correct behavior, wrong label, reinforcing the
  canonical-channel design (Track 1).

## What the full pipeline should change (vs SEED-029 revised)

1. Acceptance: sloped length×density boundary; per-genre calibration;
   confusion-weighted costs. (F2, F9)
2. Candidate stage: diagonal-keyed two-hit. (F8)
3. Stage-0 adds: FL-id dedup; 997-variant collapse; target-sheet filter. (F6, F7)
4. DF-banding stays — and is a recall FEATURE on formulaic text. (F3)
5. Liturgy eval: witness-index-as-oracle pattern (BH generalizes to any
   indexed composition). (F10)
6. Track 1 (canon) unchanged — probe reinforced it (Hallel cross-matches).

## Artifacts

| Path | What |
|---|---|
| `scripts/normalize.py` | union-view normalizer + offset back-projection |
| `scripts/engine.py` | seed-and-extend engine (both modes) |
| `scripts/resolve_bh_witnesses.py` → `data/bh_witnesses.json` | BH index resolution (reusable pattern for any witness index docx) |
| `scripts/{extract_pilot,define_buckets}.py` → `data/probe.db` | pilot corpus (17,228 pages) |
| `scripts/confusion_matrix.py` → `results/confusion_matrix.json` | empirical CER + confusions |
| `scripts/ground_truth.py` → `results/tier1.json` | Tier-1 verified pairs |
| `scripts/separability.py` → `results/separability.{json,png}`, `verified_pairs.json`, `discoveries.txt` | the probe itself |
| `scripts/bh_experiment.py` → `results/bh_{report.txt,experiment.json}` | BH sweeps |
| `results/tier1_samples.txt` | manually-QA'd Hebrew span samples |

## Round 2 (same day): diagonal-keyed candidates + boundary calibration ✅

**Next-steps #1 and #2 were executed in the same session.** Artifacts:
`scripts/separability2.py`, `scripts/roc_boundary.py`,
`results/separability2_report{,_cap1}.{txt,json}`,
`results/verified_pairs_d50{,_cap1}.json`, `results/roc_boundary.{md,png,json}`.

**R1 — Diagonal-keyed two-hit (`engine.build_diag_pairs` + `verify_span`):**
accumulates per (pair, diagonal-bucket) a fixed-size count+extent record (no
position lists); candidate = ≥ min_anchors within a bucket±1 cluster;
`per_gram_pair_cap=1` makes the two hits DISTINCT grams. Results (pilot,
17,228 pages): **candidate recall stays 1.00 on all three families**;
candidates 31.7M → **11.4M** (2.8×); verify 11 µs/pair (125 s); accumulation
memory ↓ (~3.3 vs 15 GB at peak-comparable stage). NOT the >10× volume drop
hoped for — random Hebrew page pairs do share 2 distinct same-diagonal
5-grams under DF=100 at pilot density. HOWEVER: the absolute DF cap becomes
RELATIVELY stricter as the corpus grows (at 948K pages most grams exceed 100
pages → auto-dropped), so pilot volume does NOT extrapolate linearly — the
100K rehearsal (step R5 below) is the honest instrument. The
counting-representation (numpy sort-merge over (pair,bucket) keys) remains
required for full scale (122M accumulator entries in Python dicts ≈ 12 GB
at pilot size).

**R2 — Sloped acceptance boundary (fitted per length band, keep-95% of
Tier-1 per family; `results/roc_boundary.md`):** all profiles converge on
one shape — **density ≤ 0.30 for spans < 100 letters; ≈ 0.39–0.42 for
spans ≥ 100**. Short evidence must be clean; long evidence tolerates
liturgical noise. Recommended production profile = `liturgy_q95`
([25–100): 0.30, [100–200): 0.386, [200+): 0.418):
- Tier-1 recall: joins **1.00**, titles **0.984**, BH **0.974**
- **BH witnesses connected: 241/428 (56%)** vs 82 (19%) at flat 0.30
- Verified-recall ladder (diag candidates, k=5): titles 86%@0.30 → 98%@0.40;
  BH 46%@0.30 → 96%@0.40 → 98%@0.45
- Cost: ~4.9K accepted `cross` pairs in the (deliberately enriched) pilot —
  a mixture of canonical shares, undedup'd duplicates, target sheets, and
  genuine discoveries; **the 100–300-letter × 0.35–0.45 region is where true
  and cross overlap** — concentrate the graded precision sampling there.

## Round 3 (2026-07-07): first human grades + the flank-contrast classifier

**Early grading signal (Hillel, n=19, overlap_cross stratum — the FP
frontier):** canonical 13 (68%) / shared_formula 3 / near_verbatim 1
(possible genuine discovery) / unrelated+junk 2 (11%). I.e. **~89% of the
apparent false-positive mass is correctly-detected shared text of the wrong
KIND** — routing, not precision, is the issue: Track-1 canonical masking
alone would absorb ~⅔ of the frontier. (Small n; continue grading.)

**Flank-contrast classifier (Hillel's heuristic, mechanized):** after
accepting a span, align the ~150-letter flanks on both sides.
- flanks align → match CONTINUES → same-work evidence;
- flanks dissimilar (density ≈ random floor ~0.6) → the span is an ISLAND →
  quotation/formula; island ∧ canon-index hit → canonical quote; island ∧
  NOT canon → **citation of a non-canonical work** (indirect textual witness
  — the project's most valuable category). Cost: 2 extra Levenshtein calls
  per accepted pair. → REQUIRED in the full pipeline's verifier.

**Line-break-agreement duplicate detector (Hillel's second heuristic,
mechanized):** line breaks are PHYSICAL-page properties — genuine parallel
witnesses never agree on them; a re-photographed page (book-spread shot vs
single-page shot, different FL ids) must. Detector: ≥60% of HTR lines
(≥10 letters, ≥4 lines — short-page accident guard) matching in order at
≤0.30 normalized distance ⇒ flag. Validation on the review sample: fires on
**25/36 join anomalies** (confirming they are duplicate photography, not
textual joins) and **6/40 top "discoveries"** (incl. the two Hillel caught
by eye); zero fires in overlap/BH strata. → stage-0 dedup tier (c), after
(a) same-FL-id and (b) same-shelfmark-via-libraries.csv. Bonus: same-page-
HTRed-twice pairs = a free HTR-vs-HTR variance measurement.

**Analysis caveat:** the review tool recomputes spans (uncapped) — its
exported densities differ from engine densities; join grades to
`verified_pairs_d50_cap1.json` by pair id for calibration analysis.

### Round 3 FINAL: 164 human grades — precision is effectively solved

Full analysis: `results/grades_analysis.md`; raw grades:
`review/grades_hillel_2026-07-07.json` (label semantics recorded there —
canonical = quotation in a DIFFERENT work, Bible↔Bible = same_text;
same_text judged at the UNIT level: siddur-BH ↔ Haggadah-BH = same_text
⇒ **same-work clustering must cluster textual UNITS, not manuscripts**).

| Measurement | Result |
|---|---|
| Actually spurious | **1/164 (0.6%)** |
| Real shared text after stage-0 removes dup+junk | **110/111 (99.1%)** |
| Real-rate per engine-density band 0.30→0.45 | **100 / 100 / 100 / 97%** |
| Discovery stratum | **34/40 genuine same-composition discoveries** (6 dup-photos) |
| join_anomaly stratum | **36/36 duplicate photography** — ZERO textual joins; the "1% of joins share text" is fully closed |
| bh_boundary stratum (the loosened threshold) | 29/31 same_text — raised boundary vindicated |
| short_span stratum | 6/8 junk (title sheets) — short spans need stage-0 BEFORE they're useful |
| Line-agreement detector vs human | precision **100%** (31/31), recall 74% — add same-shelfmark tier + threshold tuning for the rest |

Consequences: (1) the acceptance boundary can safely sit at ~0.42–0.45 for
spans ≥100 letters — even the 0.40–0.45 band is 97% real; (2) the engine's
error mass is ROUTING (canonical/formula/duplicate), all mechanically
classifiable (Track 1 + flank-contrast + stage-0); (3) with 1,335
discovery-class pairs in the enriched pilot and ~85% raw precision, the
full-corpus run should yield thousands of genuine new same-composition
links. Caveat: pilot is enriched with related material — corpus-wide
precision still needs the pooling eval at scale, but the boundary
calibration itself is now human-grounded.

## Next steps (handoff-ready, ordered — R1/R2 DONE above)

3. ✅ **Stage-0 module** (2026-07-07): FL-id dedup, shelfmark dedup,
   line-agreement dedup, target-sheet filter — `scripts/stage0.py`; run over
   the FULL corpus: 231,679 short + 40,452 dup-FL + 9,007 target sheets
   dropped → effective corpus ≈667K pages. (Language ID still open.)
4. ✅ **Precision sampling** (Round 3 above): 164 human grades; boundary
   human-grounded.
5. ✅ **Scale rehearsal** (2026-07-07): 102,568 pages end-to-end in ~14 min
   via the numpy sort-merge engine (`scripts/engine_np.py`); recall ROSE at
   scale (tier-1 titles 0.993, BH 64%); volume law measured (654M raw hits;
   DF cap self-tightens — full corpus ≈4–5B hits → disk-partitioned merge);
   first text-reuse map built (337K accepted pairs → 244K MS pairs; giant
   canonical component 15,969 MSS — Bible+liturgy+piyyut+exegesis bridged
   ⇒ **Track-1 canon masking is the gate**).
   Full writeup: **`REHEARSAL-RESULTS.md`**; map `results/rehearsal_100k_map.md`;
   atlas `review/rehearsal_100k_atlas.html`.
6. ✅ **Track 1** (2026-07-07, same day): Maagarim (5,274 works incl. full
   Tanakh/Mishnah/Bavli/Yerushalmi) + Friedberg JA (89 works) reference;
   asymmetric matcher `scripts/track1_match.py`. **26.4% of the random
   100K sample identified** (Bible-domain recall 66.3%, Documents floor
   3.7%); 4,096 JA identifications; mesirah channel (editions of Genizah
   fragments) live. Masked Track-2 rerun: accepted pairs 337K→72.7K
   (−78%), giant component 15,969→7,561; the residue = Karaite liturgy +
   piyyut NOT covered by Maagarim/JA — itself a discovery product
   (high-witness unidentified units). See REHEARSAL-RESULTS.md §Track 1.
7. Then: the full-corpus run per SEED-029 (disk-partitioned engine +
   page-chain extension of edge-class spans + Track-1 full-corpus pass).
