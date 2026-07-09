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

## Reference universe is CLOSED at Maagarim + Sefaria + JA (Hillel 2026-07-09)

Dicta's other corpora are usually irrelevant here — Genizah fragments are
early-medieval, so Dicta's later/modern-register material (responsa, printed
acharonim, modern Hebrew) does not overlap the fragment content. So reference
expansion is essentially complete: Maagarim (have) + JA (have) + Sefaria
(REF-1, adding Targum + canonical liturgy/works). No further reference-source
hunt.

**Consequence — the fragment population bifurcates, and only one half is an
identification problem:**
- **Reference-covered fragment** (its work is in Maagarim/Sefaria/JA) →
  IDENTIFIABLE; the FRAG-1/FRAG-2 DF-immune-query + length-threshold machinery
  is the lever. This is where recall/precision engineering pays off.
- **No-reference fragment** (work in no corpus — e.g. much of the B2 residue:
  JA Karaite exegesis/philology) → cannot be labeled against a reference, by
  definition. Only Track-2 / motif / unit clustering can group it with other
  witnesses of the same *unknown* work and surface it as a DISCOVERY product.
  Not a fixable recall gap.

FRAG-1's `no_reference_covers_it` failure bucket measures the split directly —
it tells us how much of the unidentified-short-fragment mass is "recoverable
with better matching" vs "discovery-only."

## FRAG-1 RESULTS (2026-07-09, commit re frag1_truncation.md) — reorders the levers

- **recall(length) knee ≈ 150 letters** at the current boundary (40:0% / 80:9% /
  100:28% / 150:87% / 200:95% / 300:98%). The 40-letter 0% is a STRUCTURAL floor
  (~36 grams; HTR variants wipe shared anchors) — not tunable; those need
  paleography/join/catalog signal or human review.
- **Boundary sweep = the recovery lever.** At 100 letters, loosening 1.0×→1.2×
  lifts recall 28%→84% at ~9% wrong-work mis-attribution; at 150, 1.0×→1.1×
  gives 87%→93%. Length-conditional loosening (A5) recovers most of the
  sub-150 loss into a CANDIDATE tier.
- **Failure census reorders priorities:** 297/300 orphan short pages fail at
  `density_fail`, **0 `no_reference`, 0 anchor-starved.** So for the SHORT-
  FRAGMENT population the bottleneck is the ACCEPTANCE BOUNDARY (A5), NOT
  reference coverage. (This does NOT contradict the bifurcation above: the
  B2 no-reference residue is a DIFFERENT, high-witness population; the short
  orphans here DO hit references but get rejected.)
- **Prize size:** 16,420 orphan pages <100 letters, 71,176 <200 (neither
  Track-1 ID nor Track-2 pair); Piyyut/Bible/Liturgy dominant.
- **Honest caveat:** truncation used CLEAN crops → these are OPTIMISTIC upper
  bounds; real HTR-damaged/variant fragments are harder (FRAG-2 must crop +
  inject confusion-matrix noise). Mis-attribution measured on known-work clean
  crops; real-orphan precision needs the A5 boundary applied + human grading.

**Consequence for sequencing:** A5 length-conditional / two-tier thresholds is
now the FIRST fragment lever (was going to be reference expansion). Its full
calibration (deferred to wave 2 in the A5 brief) is now on the critical path.

## FRAG-2 (wave 3, after FRAG-1 + mask job + Map-v2) — full-scale fragment run

Systematized DF-immune fragment identification at full corpus scale: work-query
(A2) + motif-query as the recall engine, A5 length-conditional FDR thresholds as
the acceptance gate (two-tier: census vs candidate-for-review), over the
expanded reference set (Targum + Sefaria + whatever Dicta-internal adds). Output
= the extended witness census + a ranked fragment-identification review queue
(candidate tier), graded by Hillel. Scoped in detail once FRAG-1's numbers land.

## Open input from Hillel
- (RESOLVED 2026-07-09) Reference universe closed at Maagarim + Sefaria + JA;
  Dicta's other corpora usually irrelevant (wrong period/register). No further
  reference-source hunt — see "Reference universe is CLOSED" above.
