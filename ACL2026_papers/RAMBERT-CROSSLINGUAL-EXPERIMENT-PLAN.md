# Experiment Plan — Does a jointly-pretrained Hebrew+Judeo-Arabic encoder (RamBERT) beat two specialist encoders on cross-lingual intertextuality?

Status: DRAFT plan, 2026-07-08. SEED-029 / Track-3. Companion to
`TRACK3-DECISION-BRIEF.md` and `TRACK3-ENRICHMENT-BIBLIOGRAPHY.md`.

**One-line thesis.** Cross-lingual Hebrew↔Judeo-Arabic reuse detection is the
task class where joint pretraining is *theoretically obligated* to pay off — so
it is simultaneously (a) the discriminating benchmark RamBERT currently lacks and
(b) the cross-lingual validation Track-3 needs. One experiment answers both.

---

## 1. The question and why it has no answer yet

We hold two internal MiDRASH-ERC encoders: **JABERT** (Judeo-Arabic only,
published) and **RamBERT** (combined Hebrew+JA, unpublished). The open problem:
*no evaluation has shown RamBERT is actually better than using two separate
specialist BERTs.* This plan explains why — and fixes it.

A combined model competes against specialists under the **curse of
multilinguality**: splitting capacity across two languages typically makes a
combined model equal-or-worse than a specialist *on that specialist's own
language*. Therefore on every **monolingual** benchmark (MLM perplexity, POS,
NER, monolingual STS) RamBERT is structurally disadvantaged — which is exactly
why generic evaluation keeps returning "not clearly better."

The **only** place joint pretraining can strictly dominate is a task requiring a
**single shared representation across both languages**, where two specialists
produce two disjoint spaces and must *bolt on* an alignment step, while the
combined model gets that alignment for free. Alignment is hardest to bolt on when
labeled cross-lingual pairs are scarce — our exact regime. Cross-lingual
intertextuality is that task. If RamBERT cannot win here, joint pretraining
bought nothing; if it wins anywhere, it is here. Either outcome is decisive.

Symmetry for us: cross-lingual Heb↔JA reuse is **invisible to our char-n-gram
lexical engine** (different script, no shared n-grams beyond embedded Hebrew
quotes), so this is genuinely new capability, not a re-run of Tracks 1–2.

---

## 2. Hypotheses (pre-registered)

- **H1 (main, difference-in-differences).** The advantage of the combined
  encoder (RamBERT, arm A) over the best two-specialist stack (arm B) is
  significantly **larger on cross-lingual pairs than on monolingual pairs**.
  Formally: `Δ_xling = (A − B)|cross-lingual` and `Δ_mono = (A − B)|monolingual`;
  H1 predicts `Δ_xling − Δ_mono > 0` with a bootstrap CI excluding 0.
- **H0 (null).** `Δ_xling − Δ_mono ≤ 0` — joint pretraining confers no
  cross-lingual-specific benefit over a fairly-aligned specialist stack.
- **H2 (monolingual tax, expected).** On monolingual strata, specialists
  match-or-beat RamBERT (`Δ_mono ≤ 0`). We *predict our own model loses here* —
  stating it up front is what makes H1 credible rather than cherry-picked.
- **H3 (low-resource crossover).** RamBERT's cross-lingual advantage grows as the
  alignment supervision available to arm B shrinks; there exists a supervision
  budget below which A strictly dominates B on cross-lingual retrieval.
- **H4 (difficulty gradient).** The A-over-B cross-lingual gap widens from
  translation → quotation → paraphrase/allusion (the paraphrase stratum is where
  a shared space matters most, mirroring MiqraBERT's narrative-vs-poetic split).

**Pre-registered decision rule.** RamBERT is declared "worth it for cross-lingual
reuse" iff H1 holds (Δ_xling − Δ_mono > 0, bootstrap 95% CI excludes 0) on the
held-out quotation+paraphrase strata, across ≥10 seeds. Translation-only wins do
NOT count (see §4). We commit to this rule before looking at test scores.

---

## 3. Experimental arms

All arms receive the **identical** Track-3 fine-tuning recipe (§5), the **same**
training-pair budget, the **same** hyperparameter-search budget, and the **same**
evaluation (§6). The encoder configuration is the only manipulated variable.

- **A — RamBERT (combined).** SBERT/contrastive fine-tune once; embeds both
  languages in its native shared space. *(Our model under test.)*
- **B — Two specialists, fairly aligned (the honest competitor).** JABERT (JA) +
  best Hebrew base from the §5 bake-off (NeoDictaBERT-embed / BEREL / MsBERT).
  Primary variant **B-joint**: fine-tune *both* encoders end-to-end with a shared
  contrastive projection head on the cross-lingual training pairs — the strongest
  honest way to pull two spaces together. Secondary **B-procrustes**: independent
  fine-tunes + supervised orthogonal Procrustes / learned linear map on the
  translation pairs (a cheaper, weaker alignment; a within-arm floor).
- **C — Two specialists, unaligned.** JABERT + Hebrew base, concatenate/late-fuse
  with no cross-lingual alignment. The floor — shows what "just use two BERTs
  naively" actually costs.
- **D — Strong multilingual reference.** BGE-M3 / LaBSE, same fine-tune. External
  yardstick: another shared space of different provenance. If D ≈ A, the win is
  "shared space," not "our shared space"; if A > D, our in-domain joint
  pretraining beats generic multilinguality.

---

## 4. Data / gold-set construction

The task: **given a JA passage that reuses a Hebrew/Aramaic source, retrieve the
source from a candidate pool** — and the reverse direction (Heb→JA). Positives
are graded by reuse type into a **difficulty gradient**:

| Stratum | Source | Abundance | Role |
|---|---|---|---|
| **Translation** | **Saadia Gaon's Tafsīr** (JA Torah translation, verse-aligned to Masoretic Hebrew; abundant in the Genizah) | HIGH | **Train / align** (easy positives) |
| **Quotation** | JA works quoting Bible / Mishnah / Talmud verbatim or near-verbatim | MED | **Test** (some catchable by the lexical engine → free anchors + labels) |
| **Paraphrase / allusion** | JA exegesis, responsa, philosophy (e.g. Maimonides) reworking Hebrew/Aramaic sources | LOW | **Test — the decisive stratum** |

**Design principle — abundant-easy trains, scarce-hard discriminates.** The
plentiful translation pairs (Tafsīr) supply Track-3 positives *and* arm B's
alignment supervision; the scarcer, harder quotation+paraphrase pairs are held
out as the *discriminating test set*. This resolves the central tension: a shared
space is easy to demonstrate on translation (both A and a well-aligned B may
succeed → non-discriminating), so translation results are reported for sanity
only and are explicitly excluded from the H1 decision rule.

- **Monolingual control strata (for the difference-in-differences):** JA↔JA and
  Heb↔Heb reuse pairs drawn from the existing 1.34M lexical-engine output. These
  give `Δ_mono`.
- **Candidate pool / negatives:** for each query, the full (or large sampled) set
  of Hebrew source passages; **hard negatives** = topically/lexically near the
  true source; mined and **denoised** per §5.
- **Anchors bootstrap:** embedded Hebrew quotations inside JA text are already
  found by the lexical engine → free, high-precision cross-lingual labels to seed
  the quotation stratum and validate the harness.
- **Splits:** strict passage-level dedup between train/align and test (no
  Tafsīr verse or its neighbors leaks across); test positives are human-verified.
- **PREREQUISITE INPUTS (gate the build):**
  1. Digital, verse-aligned **Saadia Tafsīr** (or equivalent aligned JA↔Heb
     bitext) reachable in our data? *(proposed backbone)*
  2. A **Hebrew-only checkpoint from RamBERT's own pipeline** for the pristine
     ablation (§7)? If absent, we run the practical comparison only.

---

## 5. Track-3 fine-tuning protocol (shared across all arms)

Per the decision brief; held constant so only the encoder differs.

- **Objective — graded, not binary.** Primary: **Margin-MSE** regressing onto the
  density-score margin of (anchor, positive, mined-negative) triplets
  (lowest-friction upgrade of MiqraBERT's regression head). Secondary ablation:
  **RankCSE** listwise distillation from the density score.
- **Negatives — mined, denoised, ambiguous-band.** Mine near-misses + shadowed
  lexical hits; sample the difficulty band *just below* the reuse threshold
  (SimANS), drop likely false negatives via the density-score filter (RocketQA);
  treat high-overlap non-reuse as SNCSE soft negatives with a margin.
- **Scale + noise.** Filtered slice of the 1.34M as weak positives (E5 /
  Augmented-SBERT), consistency-filtered; optional self-teaching branch
  (clean→OCR-perturbed distillation) for manuscript-noise robustness.
- **Matched budget (fairness).** Identical #training-pairs, #steps, and
  hyperparameter-search trials per arm; report the budget explicitly.
- **Bake-off feed:** the Hebrew base for arms B/C is chosen by a label-free
  pre-screen (NN-overlap + MMTEB-shaped retrieval on our verified pairs) among
  NeoDictaBERT-embed / BEREL 3.0 / MsBERT before the main runs.

---

## 6. Metrics & analysis

- **Primary metric = distribution separation:** Wasserstein distance + overlap
  coefficient + AUC between positive and negative similarity distributions
  (MiqraBERT / arXiv:2506.24117), **not** raw top-k. Heed the E5 trap
  (highest-similarity ≠ best-separator).
- **Retrieval:** recall@{1,5,10} + MRR on held-out positives, both directions
  (JA→Heb, Heb→JA); report the directional asymmetry.
- **Stratified everywhere:** by reuse type (translation / quotation / paraphrase)
  and by direction. Aggregate-only numbers are prohibited (they hide the effect).
- **The decisive read (H1):** compute `Δ_xling − Δ_mono` with a **bootstrap CI
  over query items** and a paired test across ≥10 seeds. Plot the A/B/C/D margins
  per stratum — the confirming picture is a large A−B bar on cross-lingual,
  flat-or-negative on monolingual.
- **Calibration:** report raw + isotonic-calibrated cosine; set operating points
  as quantiles (survive re-embedding); gold-free thresholds via conformal+FDR.
- **Ceiling honesty:** where paraphrase separation is low, report the
  silhouette/AUC ceiling so a floor isn't misread as a bad model.

---

## 7. Confounds & controls

1. **Curse-of-multilinguality confound** → controlled by the difference-in-
   differences: we claim only the *cross-lingual-minus-monolingual* gap, and we
   pre-declare that RamBERT is expected to lose monolingually (H2).
2. **Alignment-budget fairness** → arm B gets a *fair and swept* amount of
   alignment supervision (H3 low-resource curve). "B needs more cross-lingual
   pairs than exist" is a legitimate, reportable win condition, not a rigging.
3. **Not a pristine ablation** → RamBERT and JABERT differ in vocab + data, so B
   tests "RamBERT-as-shipped vs the best available two-model stack." The clean
   "joint-vs-separate, all else equal" ablation needs a Hebrew-only checkpoint
   from RamBERT's pipeline (§4 prerequisite 2). We run the pristine version *if*
   that checkpoint exists; otherwise we frame honestly as the
   practically-decisive product comparison.
4. **"Saw more data" confound** → matched fine-tune budget (§5); the combined
   model is *permitted* to keep the cross-lingual co-occurrence it saw in
   pretraining, because that free alignment is precisely the hypothesized
   mechanism and is available with zero labeling — the honest real-world claim is
   "given realistic supervision, does joint pretraining help."
5. **Leakage** → passage-level dedup across train/align/test; Tafsīr neighbors
   quarantined; test positives human-verified.

---

## 8. Threats to validity (honest)

- The most decisive stratum (paraphrase/allusion) is the scarcest in gold — CIs
  there may be wide; mitigate with careful human verification and by reporting
  effect sizes with uncertainty, not point claims.
- Prior art in an analogous historical-religious corpus (Manjavacas) got only
  *moderate* gains from semantics over lexical baselines — set expectations; a
  small-but-consistent, correctly-localized gap is a real result.
- Translation-heavy gold could inflate all arms; excluded from the decision rule
  by design, but reviewers will probe it — pre-empt in the write-up.
- Cross-script similarity is where "signals die crossing scripts" bites hardest;
  a null on paraphrase may reflect an intrinsic ceiling, which we state rather
  than tune away.

---

## 9. Phasing (de-risking spine, each with a go/no-go)

- **Phase 0 — pipeline smoke (monolingual JA).** JABERT + the §5 recipe on JA↔JA
  reuse; prove the fine-tune + WD/overlap harness works and beats off-the-shelf.
  *(This is the brief's "one cheap green-light experiment.")* **Go/no-go:**
  separation beats the untuned JABERT baseline.
- **Phase 1 — gold build + translation sanity.** Construct the Tafsīr-aligned
  set + quotation/paraphrase test set + monolingual controls; run all arms on the
  *translation* stratum. **Go/no-go:** shared-space arms achieve high translation
  recall (harness validated); proceed only if the cross-lingual harness is sound.
- **Phase 2 — the decisive test.** Full A/B/C/D on held-out quotation+paraphrase;
  the H1 difference-in-differences. **This is the paper.** **Go/no-go:** H1
  decision rule (§2).
- **Phase 3 — ablations + write-up.** Alignment-budget sweep (H3); difficulty
  gradient (H4); pristine ablation if the Hebrew-only checkpoint exists; Margin-
  MSE vs RankCSE; calibration; directional asymmetry.

---

## 10. Cost & resources

- Compute is modest: all encoders are BERT-base (~184M params); SBERT-style
  fine-tunes run in hours on a single GPU per arm × seeds. The bottleneck is
  **gold-set construction + human verification of the hard strata**, not compute.
- Reuses proven code paths (MiqraBERT recipe, our existing engine for candidate
  generation and hard-negative mining, our density scores as graded labels).

---

## 11. Publication & collaboration framing

- **Contribution:** first evaluation isolating when a jointly-pretrained
  Hebrew+JA encoder beats specialist encoders, via cross-lingual intertextuality
  — a task where the benefit is theoretically motivated and empirically localized
  by a difference-in-differences design. Doubles as the RamBERT paper's missing
  evaluation section and as Track-3's cross-lingual validation.
- **Venue fit:** LT4HALA / ML4AL (JABERT already published at LT4HALA 2026).
- **Positioning / citations:** SPhilBERTa (shared-space via synthetic pairs) and
  LaBSE (cross-lingual dual-encoder) as arm-B lineage; MiqraBERT + arXiv:2506.24117
  as the monolingual-Hebrew reuse precedent and the WD/overlap metric source;
  T'OMIM as an external benchmark.
- **In-house bridge:** MiDRASH-ERC (Avi Shmidman, NLP PI, JABERT co-author; H.
  Gershuni co-author) — this hands the unpublished RamBERT the discriminating
  benchmark it needs, inside the funded intertextuality work package.

---

## 12. Immediate next actions

1. Confirm §4 prerequisites: Saadia Tafsīr (or equivalent) alignment availability;
   existence of a Hebrew-only same-pipeline checkpoint.
2. Run **Phase 0** (monolingual JA smoke) — unblocked today; needs only JABERT +
   a JA reuse slice from the existing engine output.
3. In parallel, scope the Tafsīr gold build (verse alignment → pairs) and the
   quotation/paraphrase human-verification protocol.
