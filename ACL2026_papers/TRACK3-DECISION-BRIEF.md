# Track-3 Base-Model & Training Decision Brief (SEED-029)

Compiled 2026-07-08. Integrates: a close read of **MiqraBERT** (arXiv:2606.19638,
the published proof-of-concept of our Track-3) + its predecessor benchmark
(arXiv:2506.24117); a four-lane literature sweep (`TRACK3-ENRICHMENT-BIBLIOGRAPHY.md`);
and our **internal** MiDRASH-ERC encoders. "Track-3" = the semantic-embedding
layer that catches paraphrase-level reuse the lexical engine (Tracks 1–2) misses.

---

## 0. The reframe — we already own the hardest piece

MiqraBERT is the **existence proof**: SBERT-fine-tuning a masked-LM Hebrew
encoder on ~1,150 pairs works for narrative reuse (recall@10 87%) and **collapses
on poetic/paraphrase reuse (<9%)**. It got there from AlephBERT — a *Modern*
Hebrew MLM, a register mismatch for us and useless for Judeo-Arabic.

We hold purpose-built encoders in exactly our register:

| Model | Iter | Vocab | Notes |
|---|---|---|---|
| **JABERT** (`JA_FinalModel`) | 86,010 | 128K | Published: HF `MiDRASH-ERC/JABERT`, CC-BY-4.0, ERC MiDRASH #101071829, LT4HALA 2026. Co-authored (incl. H. Gershuni, A. Shmidman). Already consumed by `mrustow/geniza-lacunae` (Rustow / Princeton Genizah Lab). |
| **RamBert_round_2** | 86,010 | 128K | Internal. Distinct weights **and** vocab from JABERT despite the shared iter number. |
| **GereshlessRamBert** | 65,980 | 128K | Internal; geresh (׳) stripped — a script-normalization ablation. |

All three: **BERT-base** (12L / 768h / 12 heads / 3072 ff, 512 positions,
~184M params, `BertForMaskedLM`). Tokenizer carries custom **`[GAP]` / `[ONEGAP]`**
tokens — **lacuna-native**, built to reason about damaged manuscript text.

**Consequence:** the public literature has *no* Judeo-Arabic-in-Hebrew-script
encoder — the Lane-C agent, blind to these, proposed a clumsy
transliterate-JA→Arabic-script→CAMeLBERT workaround. We don't need it. This
collapses the single hardest piece of the base-model problem, in our register,
manuscript-native, from our own ecosystem.

**Open questions for the team (only Hillel can answer):**
1. **RamBERT scope** — is RamBERT a *combined Hebrew+JA* model (a native shared
   space, which would resolve cross-lingual Heb↔JA reuse in-house) or JA-only
   like JABERT? Its different-vocab-vs-JABERT hints at a different data mix.
2. Which checkpoint is the intended Track-3 base — JABERT (published, citable)
   or a RamBERT round?
3. Is there a held-out MLM eval (perplexity / masked-accuracy) per register we
   can reuse to rank them before fine-tuning?

---

## 1. Base model — decision

- **Judeo-Arabic slice → JABERT / RamBERT (ours).** No transliteration detour.
  Supersedes Lane-C's Arabic-script workaround. Bonus: `[GAP]`/`[ONEGAP]`
  tokens fit our lacunose corpus natively.
- **Hebrew slice → bake off**, picking by the register that dominates *our*
  pairs: **NeoDictaBERT-bilingual-embed** (only Hebrew model with a retrieval
  head + long context) vs **BEREL 3.0** (Rabbinic) vs **MsBERT** (manuscript
  Hebrew), with **DictaBERT** and MiqraBERT's **AlephBERT** as references.
- **Cross-lingual Heb↔JA:** *if* RamBERT is Hebrew+JA, it is a native shared
  space — best case, resolves the problem SPhilBERTa/MITRA solve with synthetic
  pairs. Else, build a shared space by fine-tuning JABERT + a Hebrew model on
  mined/synthetic cross-script pairs (SPhilBERTa recipe).
- **All are masked-LM** → Track-3 = SBERT/contrastive fine-tune (the MiqraBERT
  move), but from a vastly better base + our data advantage.
- **Do not discard the lexical backbone** (survey's explicit Hebrew-Aramaic
  caution): embeddings are a **re-ranker / recall-expander** over Tracks 1–2
  candidates (Burns/Tesserae), not a replacement — also the robust choice under
  OCR/handwriting noise and lacunae.

## 2. Training objective — decision (beat MiqraBERT's two weak choices)

MiqraBERT's weaknesses were *forced by data scarcity*, which we don't share.

- **Binary → graded.** Swap cosine-regression-on-{0,1} for a graded objective
  that consumes our continuous density score. Two routes:
  - lowest-friction: **Margin-MSE** onto the density-score *margin* of an
    (anchor, reuse, near-miss) triplet — same loss family MiqraBERT trusts;
  - higher-ceiling: **RankCSE** listwise ranking distilled from the density score.
- **Random → mined, denoised, ambiguous-band hard negatives** (SimCSE / ANCE /
  RocketQA / SimANS / NV-Retriever / SCAD all converge here). Mine near-misses
  and *shadowed* lexical hits; but **denoise** (false negatives are rampant in
  reuse-saturated liturgy) — take the band *just below* threshold, not top-1
  hardest; treat high-overlap non-reuse as **SNCSE soft negatives** with a
  margin loss.
- **Scale up** to a filtered slice of the 1.34M as weak positives (E5 /
  Augmented-SBERT), with an E5-style consistency filter first; pair-selection
  strategy matters more than raw volume.
- **Noise robustness as an objective:** self-teaching (distill clean→OCR-
  perturbed) — our lacuna tokens already help.
- **Guard the ceiling:** our teacher is *lexical*, so silver labels + mined
  negatives are precision-high / recall-low for pure paraphrase — the exact
  thing Track-3 exists to catch. Keep a small **human-graded paraphrase set OUT
  of training**, for eval + final supervised fine-tune only.

## 3. The paraphrase / liturgical-poetry failure — our #1 research risk

MiqraBERT's <9% is not a fluke; it is **mechanistic and corroborated**
(NN-overlap: embedders default to lexical surface; Manjavacas: allusion = sparse
shared words → only *moderate* semantic gains historically; silhouette-ceiling:
part of the floor is intrinsic to the genre). Liturgy/piyyut is our poetic
stratum — the most at-risk **and** among the most scholarly valuable.

- Attack on three fronts: **contrastive fine-tune + linguistically-informed
  augmentation** (D'Angelo's Greek *formulae* ≈ piyyut formulae; SCAD zero-shot
  allusion); **hard-positive mining** of low-overlap true parallels;
  genre-adaptive/stratified training.
- **Set expectations honestly** (Manjavacas): expect moderate gains, lean on
  strong candidate-generation/windowing, report poetic recall transparently as a
  research target — never hide it in an unstratified aggregate.

## 4. Evaluation — honest, gold-free-first, at 1.34M scale

- **Primary metric = distribution separation** (Wasserstein + overlap
  coefficient + AUC) across seeds, per MiqraBERT / 2506.24117 — **not** raw
  top-k. Heed the **E5 trap**: the highest-raw-similarity model is often the
  *worst separator*.
- **Stratify every number by genre** (narrative / prose / liturgical-poetry /
  paraphrase-heavy) and state the intrinsic ceiling on the poetic stratum.
- **Calibrate cosine** (isotonic on a few hundred labels; **quantile thresholds
  survive re-embedding**); for gold-free operating points use **conformal
  p-values + FDR** (ACL wave-1 shortlist item 10) — provably-bounded
  false-positive rate over all pairs; do **not** confidence-prune rare works.
- **Label-free screens** (NN-lexical-overlap of returned neighbors; topological
  geometry) for cheap model selection *before* spending human-label budget.
- **Sparse expert gold:** recall@10 on held-out known parallels + precision by
  manual sampling (the DH-field norm).

## 5. Collaboration / citation posture

- **External:** MiqraBERT + the **T'OMIM** dataset (Zenodo) + arXiv:2506.24117
  = a ready benchmark and the closest prior art to cite/benchmark against
  (Smiley, Notre Dame). `Loci Similes` (Latin) is a second eval-protocol model.
- **Internal:** JABERT / MiDRASH-ERC + **Avi Shmidman** (BEREL/DictaBERT, and a
  JABERT co-author) = the in-house pipeline. The Rustow `geniza-lacunae` usage
  shows JABERT already deployed on Genizah lacunae — a natural bridge.

## 6. The one cheap experiment that tests the whole thesis

Take **JABERT** (or RamBERT), apply the **MiqraBERT recipe** — but with **our
graded labels + mined/denoised hard negatives** — on a Judeo-Arabic reuse slice.
Evaluate by Wasserstein + overlap, stratified by genre. It reuses code MiqraBERT
already proved, starts from a base we already own, and exercises every lever in
§2 at small scale. If separation beats the JABERT-off-the-shelf baseline (and
especially if it holds on a paraphrase-heavy stratum), Track-3 is green-lit.
