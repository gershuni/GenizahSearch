# SEED-029 — Fragment-Identification Plan (goal pivot 2026-07-09)

Hillel's decision (2026-07-09) on the Track-3 question: the goal is **same-work
witness finding** — "more same-work, more granularity, more precision and
recall, trying also to identify small/fragmentary fragments." NOT translation
(#1) and NOT literary-parallel (#2).

**Consequence:** semantic embedding fine-tuning (Track-3 / JABERT cosine +
margin-MSE) is PARKED. Same-work witnesses are *copies* — they share actual
letters, so lexical matching is the right tool and paraphrase-embeddings add
nothing to this goal (orchestrator's own 2026-07-09 analysis; the near-circular
Phase-0 setup — training on lexically-found pairs — confirmed it). The WSL/GPU
environment, the phase0 dataset, and the off-the-shelf baseline
(`phase0_eval_baseline.json`, dev AUC 0.977) stay committed and revivable IF
the goal ever becomes #1 (translation) — that is the RamBERT cross-lingual
experiment, a separate identification product, never the census.

Everything else built this session already serves THIS goal; it needs to be
(a) grounded in fragment-length measurement, then (b) pointed at the fragment
regime at full scale.

## The levers, mapped to the goal

| Goal facet | Existing machinery | State |
|---|---|---|
| RECALL — find witnesses the DF cap starves | A2 `work_query.py` (work-as-query, per-query DF immunity) | built, Codex-APPROVE; full run gated behind mask job |
| RECALL — attach fragments to known passages | `motif_query.py` (motif-as-query, DF immunity, two-sided) | built; one sweep done (+25,327 memberships; frag tail 1,219) |
| RECALL — more identifiable works | REF-1 reference expansion (Targum done, Sefaria staged, B2 gap list, Dicta-internal TBD) | Stage 1 done; Stage 2 = Map-v2 rebuild |
| PRECISION — short-match acceptance | A5 conformal + FDR **length-conditional** thresholds + two-tier (census / candidate) | probe.db validated; full calibration = wave 2 |
| GRANULARITY — passage/brakhah level | A6 motif-v2 communities + `passage_units` | A6 adopt; units rebuilt |
| DISCOVERY product | new? queue (1,168), frag-tail (B3 75% catalog agree), residue (B2) | delivered off current snapshot |

The gap: **none of these has been measured as a function of fragment LENGTH.**
Recall is 1.00 on GT families in aggregate, but we do not know where it falls
off for tiny fragments, nor how precision degrades on short accepted matches.
That curve must drive the tuning.

---

## FRAG-1 — length-stratified recall/precision via SYNTHETIC TRUNCATION (grounding spike)

**Codex plan-review (2026-07-09) → REVISE folded in.** The original design
(stratify tier-1 GT by length) is CIRCULAR and cannot find the small-fragment
knee: tier-1 positives were themselves produced by a lexical engine
(`ground_truth.py` k=4 permissive), so a short page only enters GT if it was
already lexically recoverable — the curve flatters short-fragment recall, and
tier-1 has ~no positives below 100 letters anyway (joins are all 800+, BH
starts 100-200). A5 decoy-FDR bins by ALIGNED-SPAN length (not the fragment's
own length) and bounds only chance-alignment error, so it is calibration
evidence, not precision. Human grades are too thin at the short end (0 pairs
<60 letters, 5 in 60-100) to conclude from.

**Fix (Codex's, adopted): synthetic truncation.** Ground truth by CONSTRUCTION,
free of the lexical-recoverability selection bias.

READ-ONLY / bounded compute: index built from `ref_corpus.pkl` only; queries
are a few thousand short crops, NOT a 667K-page scan. Scripts dir,
`python -X utf8 -u`; report `../results/frag1_truncation.md`; no git commit.

1. **Unbiased recall(length) by truncation.** Sample ~500 pages that are
   independently, confidently Track-1-labeled as a known work W (live rows,
   `shadowed_by IS NULL`, high `matched_letters` = full-page testimony;
   stratify the sample across works/domains, incl. JA). For each, cut its
   normalized stream to crops of target lengths {40, 60, 80, 100, 150, 200, 300}
   letters (from a random in-page offset; several crops per length to average
   noise). Run each crop as a DF-immune QUERY against the reference index (the
   `track1_match` / `motif_query` mechanism — the reference IS work W's text).
   Recovery = W appears among the crop's accepted identifications at the
   length-appropriate boundary. **recall(length) = recovered / total per crop
   bin** — true by construction, since the crop provably belongs to W. Locate
   the knee where fragments stop being identifiable.
2. **Unbiased precision(length) from the SAME experiment — mis-attribution.**
   A crop of W identified as some OTHER work W'≠W is a FALSE identification.
   **mis-attribution(length) = crops identified as a wrong work / crops
   identified as anything**, per length bin — an unbiased precision proxy
   needing no human grades (the true label is W by construction). Sweep the
   acceptance density boundary to trace the recall/precision tradeoff at each
   length — this directly parameterizes A5's length-conditional two-tier
   thresholds. Report retained effective 5-gram count per crop (post-mask/DF),
   not just raw length (Codex MEDIUM-4).
3. **Failure-mode stage attribution.** For a real sample (~300) of currently-
   UNIDENTIFIED short pages (fullcorpus.db, live `track1_matches` shadowed-
   filtered + `accepted_pairs_canonmask` — NOT unmasked; Codex MEDIUM-7),
   classify WHY each is unidentified, one stage each: `no_grams` /
   `<2_anchors` / `no_diagonal_cluster` / `density_fail` / `ambiguous` /
   `no_reference_covers_it`. This tells us which lever fixes which regime:
   `no_reference` → REF-1 expansion; `<2_anchors`/`density_fail` on short but
   real text → DF-immune query + length-conditional threshold; genuinely
   `ambiguous`/too-short → candidate tier or unrecoverable.
4. **Fragment-population census** (fullcorpus.db, counts only). Short pages
   (<100, <200 letters): how many already Track-1-identified (live), how many
   in an `accepted_pairs_canonmask` pair, how many neither = the target
   population; cross-tabulate by domain. Sizes the prize.
5. **Design output:** the fragment-ID pipeline grounded in 1-4 — the length
   bins where DF-immune querying suffices, where the A5 two-tier candidate
   (review, not census) treatment is forced by the mis-attribution rate, the
   floor below which fragments are unrecoverable without external signal, and
   the specific per-length thresholds. Plus a targeted short-bin human-grading
   request for Hillel (the real-data precision check the synthetic curve can't
   fully replace). Feeds FRAG-2 (full-scale run, wave 3).

**Acceptance:** the truncation recall(length) + mis-attribution(length) curves
(unbiased, construction-labeled), the failure-mode histogram, the population
census, and a pipeline design whose thresholds are justified by measured knees.
Honest negatives welcome (e.g. "below 60 letters mis-attribution exceeds X% at
any recall-useful threshold → those need human review or external signal").

**Caveat to state (Codex MEDIUM-8):** truncation models a CLEAN contiguous
fragment; it does not model HTR damage or heavy textual variance, which need
lower-k / confusion-weighted matching — flag that as a separate axis FRAG-1
does not measure (a FRAG-2 extension: crop + inject HTR-confusion noise).

---

## FRAG-2 (wave 3, after FRAG-1 + mask job + Map-v2) — full-scale fragment run

Systematized DF-immune fragment identification at full corpus scale: work-query
(A2) + motif-query as the recall engine, A5 length-conditional FDR thresholds as
the acceptance gate (two-tier: census vs candidate-for-review), over the
expanded reference set (Targum + Sefaria + whatever Dicta-internal adds). Output
= the extended witness census + a ranked fragment-identification review queue
(candidate tier), graded by Hillel. Scoped in detail once FRAG-1's numbers land.

## Open input from Hillel
- Dicta-internal rabbinic corpora as reference sources (biggest untapped recall
  lever for fragment ID — a fragment is only identifiable against a reference
  that exists).
