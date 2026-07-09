# Wave-2 analogy cards — Machine_Learning_for_NLP (176) + *SEM (36)

Scope: `Tracks/Machine_Learning_for_NLP/` + `Workshops/SEM/` (posters ignored).
This is the methods motherlode, but ~85% of ML4NLP is RL-for-reasoning training
internals (GRPO/DPO/PPO variants, entropy control, reward shaping, credit
assignment, LoRA/quantization/decoding-speed) with no transfer to corpus-scale
text-reuse. The transferable mass lives in a thin seam: data-selection /
disentanglement / calibration / clustering / cross-lingual alignment. *SEM
punched far above its size (lexical-semantic-change and cross-lingual sense
work is structurally our census + bridge problems).

Counts: titles skimmed 212 · abstracts checked 31 · A 10 · B 11.

---

## A — adopt (surprise × usefulness; not findable by domain keyword)

### TIGER: Text-Informed Generalized Enzyme-Reaction Retrieval
- **id / track / file**: ACL 2026 Long pp.35530 · Machine_Learning_for_NLP · `Tracks/Machine_Learning_for_NLP/Zhang_Song_TIGER_Text_Informed_Generalized_Enzyme_Reaction_Retriev.pdf`
- **surface domain**: computational biology — bidirectional enzyme↔reaction retrieval.
- **mechanism**: to align two hard-to-align spaces, generate a TEXT description of one side as a pivot/bridge, fuse it with the raw features through a learned Dynamic Gating Network, and project both sides into one shared latent via a Structure-Shared Projector; explicitly engineered to survive ASYMMETRY between the two retrieval directions.
- **analogy**: their `enzyme↔reaction retrieval with a generated-text pivot + direction asymmetry` ≅ our `problem L + our asymmetric reference matcher` (noisy manuscript ↔ clean reference edition, and Judeo-Arabic ↔ Hebrew, are both asymmetric alignments).
- **why it transfers**: our reference matcher is already asymmetric (dirty ms vs clean edition); a generated normalized-text pivot (e.g., a de-noised/transliterated bridge string) + a shared projector is a concrete way to align JA and Hebrew witnesses of one work without parallel supervision.
- **why it might NOT**: enzyme sequences have a mature protein→text generator to lean on; we have no equivalent "manuscript→canonical-text" generator that isn't circular with the matcher itself.
- **transfer confidence**: medium
- **bib leads**: TIGER Dynamic Gating Network; Structure-Shared Feature Projector; protein-to-text distillation baselines cited therein.

### From Latents to Labels: Zero-Shot NER using Sparse Autoencoder Features
- **id / track / file**: *SEM 2026 pp.164 · SEM · `Workshops/SEM/Vuth_Schwab_From_Latents_to_Labels_Zero_Shot_Named_Entity_Recogniti.pdf`
- **surface domain**: zero-shot NER via interpretable features.
- **mechanism**: use MONOSEMANTIC sparse-autoencoder features (instead of polysemantic dense embeddings) and map feature activations to labels with no training; separately, they treat the predictions as SILVER data and show — via controlled corruption — that FALSE NEGATIVES, not boundary errors or false positives, are the dominant silver-label quality bottleneck.
- **analogy**: their `SAE monosemantic feature = an isolable concept axis` ≅ our `problem I` (strip/isolate scribe-hand, script, or language-identity from page vectors); their `silver-data error-type ablation` ≅ our `problem G` (Track-3 trained on surface-matcher silver labels).
- **why it transfers**: two concrete wins — (1) an SAE over Hebrew/JA embeddings gives monosemantic axes we can zero out to remove scribe/script factors before matching; (2) the false-negative-dominance finding is a directly testable directive: bias our silver-label generation toward RECALL (missed reuse hurts Track-3 more than spurious pairs or ragged span edges).
- **why it might NOT**: SAEs need a large clean activation corpus to train stable monosemantic features; at 16–20% CER our activations may be too noisy to yield clean features, and the silver-data ablation was on English/biomedical NER, not span-level reuse.
- **transfer confidence**: medium-high
- **bib leads**: SAE-NER precision-estimation mapping; controlled-perturbation silver-data protocol; monosemanticity / dictionary-learning SAE refs.

### GenDis: Generative-Discriminative Dual-View Co-Training for Generalized Category Discovery
- **id / track / file**: ACL 2026 Long pp.2330 · Machine_Learning_for_NLP · `Tracks/Machine_Learning_for_NLP/Chen_Xiong_GenDis_Generative_Discriminative_Dual_View_Co_Training.pdf`
- **surface domain**: Generalized Category Discovery (GCD) — label known + discover novel classes from partially-labeled data.
- **mechanism**: co-train two views (a discriminative classifier + a generative/semantic latent), let discriminative pseudo-labels shape a separable generative latent, enforce cross-view consistency with CCA-based alignment, and refine with a curriculum-guided, DISPERSION-AWARE pseudo-labeling loop.
- **analogy**: their `discover novel categories among partially-labeled data` ≅ our `witness census + the "new?" queue (1,168 candidate new works)`; their `two cooperating views` ≅ our `surface matcher + Track-3 semantic embedding`.
- **why it transfers**: reframes our new-work discovery as GCD (a task with an established literature): we HAVE labels for known compositions and need to spin novel clusters out of the unmatched residue. CCA-aligned dual-view co-training is a ready template for making our surface and semantic tracks bootstrap each other, and dispersion-aware pseudo-labeling is a principled confidence gate for admitting a new cluster.
- **why it might NOT**: GCD assumes a fixed, closed universe of "novel" categories to be discovered; our tail is genuinely open and heavy with singletons, where dispersion-based cluster admission may over-merge or refuse to form clusters.
- **transfer confidence**: medium
- **bib leads**: GCD literature (Vaze et al. Generalized Category Discovery); CCA cross-view alignment; dispersion-aware pseudo-labeling.

### The LSCD Benchmark: a Testbed for Diachronic Word Meaning Tasks
- **id / track / file**: *SEM 2026 pp.148 · SEM · `Workshops/SEM/Schlechtweg_Arefyev_The_LSCD_Benchmark_a_Testbed_for_Diachronic_Word_Meanin.pdf`
- **surface domain**: standardized benchmark for lexical-semantic-change detection.
- **mechanism**: the canonical LSCD PIPELINE — (1) derive Word-in-Context labels for pairs of usages, (2) assemble those pairwise labels into a GRAPH, (3) run Word Sense Induction = graph clustering to get sense clusters, (4) compare clusters ACROSS TIME to score change.
- **analogy**: their `pairwise-usage labels → graph → WSI clustering` ≅ our `problem E` (pairwise reuse matches → similarity graph → motif-v2 / witness communities); their `compare clusters over time` ≅ a lightweight `problem F` (lineage/drift).
- **why it transfers**: this is the single cleanest structural isomorphism in the whole scan — their four-stage pipeline IS our census pipeline, and it hands us a modular, reusable eval harness plus a pointer into the entire WSI / correlation-clustering-on-noisy-pairwise-graphs subfield we should be mining for our community-detection step.
- **why it might NOT**: LSCD graphs are per-lemma and tiny (dozens of usages); the clustering methods it standardizes may not survive our million-node reuse graph without re-engineering.
- **transfer confidence**: high (as framing + bib leads) / medium (as a drop-in algorithm)
- **bib leads**: Word Sense Induction methods; correlation clustering on WiC graphs (Schlechtweg SemEval-2020 Task 1 lineage); WiC labeling models cited in the repo.

### Breaking the Generator Barrier: Disentangled Representation for Generalizable AI-Text Detection
- **id / track / file**: ACL 2026 Long pp.2586 · Machine_Learning_for_NLP · `Tracks/Machine_Learning_for_NLP/Pu_Bi_Breaking_the_Generator_Barrier_Disentangled_Representat.pdf`
- **surface domain**: detecting AI-generated text that generalizes to unseen generators.
- **mechanism**: progressively disentangle task semantics from GENERATOR-specific artifacts — compact latent encoding for semantic minimality → perturbation-based regularization to shed residual entanglement → discriminative adaptation to the task; performance keeps rising as training-generator DIVERSITY grows.
- **analogy**: their `generator-specific artifact` ≅ our `scribe hand / script / hand-idiosyncrasy factor` (problem I); their `unseen generators` ≅ `unseen scribes/scriptoria`.
- **why it transfers**: our matcher should key on the TEXT, not on scribe-hand fingerprints; their recipe (minimal latent + perturbation regularization + diversity-scaling of the "nuisance" source) is a concrete way to train page embeddings invariant to who copied the page, so reuse generalizes across scribes.
- **why it might NOT**: they have explicit generator labels to disentangle against; our scribe-hand labels are sparse and themselves uncertain, so the perturbation/regularization target is weaker.
- **transfer confidence**: medium
- **bib leads**: DRGD disentanglement stages; perturbation-based regularization for invariance; MAGE benchmark.

### Mitigating Language Bias in Multilingual Sentence Embeddings
- **id / track / file**: *SEM 2026 pp.385 · SEM · `Workshops/SEM/Nonomura_Kajiwara_Mitigating_Language_Bias_in_Multilingual_Sentence_Embed.pdf`
- **surface domain**: cross-lingual similarity via debiased multilingual embeddings.
- **mechanism**: explicitly split each sentence embedding into a LANGUAGE-DEPENDENT and a LANGUAGE-AGNOSTIC component and keep only the agnostic part for cross-lingual similarity; they dissect WHICH training constraints do what — intra-component constraints tighten within-space uniformity, inter-component constraints do the cross-lingual alignment (and the two matter differently for encoder- vs decoder-based backbones).
- **analogy**: their `remove language-identity factor so parallel sentences match` ≅ our `problem I + L` — strip the Judeo-Arabic-vs-Hebrew language signal from page vectors so a work's JA and Hebrew witnesses land together.
- **why it transfers**: directly addresses our hardest alignment boundary (JA↔Hebrew); the intra/inter-constraint decomposition is an actionable recipe and tells us which loss term buys cross-lingual alignment vs mere within-language cleanup.
- **why it might NOT**: their languages have abundant parallel sentence pairs to train the disentangler; our JA↔Hebrew parallel supervision is thin and noisy.
- **transfer confidence**: medium
- **bib leads**: Tiyajamorn et al. language-agnostic/dependent decomposition; the intra- vs inter-component constraint ablation here.

### RIFT: Repurposing Negative Samples via Reward-Informed Fine-Tuning
- **id / track / file**: Findings ACL 2026 pp.14399 · Machine_Learning_for_NLP · `Tracks/Machine_Learning_for_NLP/Liu_Yuan_RIFT_Repurposing_Negative_Samples_via_Reward_Informed_F.pdf`
- **surface domain**: LLM alignment / math-reasoning fine-tuning from self-generated data.
- **mechanism**: instead of HARD-thresholding samples into keep/discard (rejection sampling), reweight the training loss by each sample's SCALAR reward so positive AND negative trajectories both contribute proportionally; a stabilized loss formulation prevents the unbounded-loss collapse that naive reward×loss multiplication causes.
- **analogy**: their `graded scalar reward reweighting the loss over mixed-quality samples` ≅ our `problem G` (Track-3 trained on GRADED silver similarity, not binary pos/neg from a hard threshold).
- **why it transfers**: our surface matcher emits a continuous similarity/density score, not a clean binary label; RIFT is a drop-in principle — feed all candidate pairs into a reward-weighted contrastive loss rather than thresholding into positives/negatives, keeping the signal in near-misses. The numerical-stabilization trick is the load-bearing detail.
- **why it might NOT**: RIFT's reward is a verifiable correctness signal; our "reward" is a self-referential similarity score, so reward-weighting could amplify the surface matcher's own biases.
- **transfer confidence**: medium
- **bib leads**: RIFT stabilized loss derivation; Rejection-Sampling FT (RFT) baselines it beats.

### Generalizing Trust: Weak-to-Strong Trustworthiness in Language Models
- **id / track / file**: ACL 2026 Long pp.46625 · Machine_Learning_for_NLP · `Tracks/Machine_Learning_for_NLP/Sun_Lakkaraju_Generalizing_Trust_Weak_to_Strong_Trustworthiness_in_La.pdf`
- **surface domain**: whether trustworthiness properties survive weak-to-strong generalization.
- **mechanism**: train a STRONG model only on a WEAK model's (imperfect) labels and it can surpass the weak labeler; they add trustworthiness-regularization during both the weak model's training and the transfer, and map which properties generalize (robustness/OOD/fairness do; privacy doesn't).
- **analogy**: their `strong model bootstrapped from weak, noisy labels` ≅ our `problem G` — Track-3 (strong semantic model) trained on the surface matcher's silver labels (the weak labeler), with the explicit hope it OUTPERFORMS the surface matcher.
- **why it transfers**: this is the theoretical license for the whole Track-3 plan and warns us it isn't automatic — regularizing BOTH the labeler and the transfer step is what makes the strong model exceed the weak one, and some properties simply won't carry over.
- **why it might NOT**: their study is about trustworthiness properties on classification, not retrieval recall; "surpasses the weak labeler" for text-reuse recall is assumed, not demonstrated here.
- **transfer confidence**: medium
- **bib leads**: weak-to-strong generalization (Burns et al.); the two trustworthiness-regularization fine-tuning strategies here.

### Data-Efficient RLVR via Off-Policy Influence Guidance (CROPI)
- **id / track / file**: ACL 2026 Long pp.46167 · Machine_Learning_for_NLP · `Tracks/Machine_Learning_for_NLP/Zhu_Wang_Data_Efficient_RLVR_via_Off_Policy_Influence_Guidance.pdf`
- **surface domain**: data selection for reinforcement fine-tuning of reasoning LLMs.
- **mechanism**: use INFLUENCE FUNCTIONS to estimate each training point's contribution to the objective; make it scalable with (a) OFF-POLICY estimation from pre-collected offline trajectories (no fresh rollouts) and (b) SPARSE RANDOM PROJECTION of the huge gradients to keep storage/compute tractable; iterate as a curriculum picking the currently-most-influential data.
- **analogy**: their `pick the most-influential training points at scale` ≅ our `problem J + G` — which of the millions of candidate reuse pairs are worth putting into Track-3 training / human review.
- **why it transfers**: at 1.34M pairs we cannot train on or audit everything; influence-based selection with sparse-random-projected gradients is a concrete, theoretically-grounded (not heuristic) way to rank pairs by training value, and the off-policy trick avoids re-running our expensive matcher.
- **why it might NOT**: influence functions are notoriously unstable for large non-convex models and assume a differentiable end-model, which our surface-matcher stage is not.
- **transfer confidence**: medium
- **bib leads**: influence functions (Koh & Liang); sparse random projection for gradient sketching; TracIn-style offline influence.

### When High Accuracy Hides Poor Calibration: Balanced Brier Score
- **id / track / file**: ACL 2026 Long pp.45888 · Machine_Learning_for_NLP · `Tracks/Machine_Learning_for_NLP/Fonseca_Goncalves_When_High_Accuracy_Hides_Poor_Calibration_Rethinking_Co.pdf`
- **surface domain**: confidence calibration of fine-tuned text classifiers.
- **mechanism**: shows ECE and Brier become BIASED in high-accuracy regimes — the mass of correct predictions swamps the metric and hides severe miscalibration ON THE ERRORS; proposes Balanced Brier Score that equalizes the contribution of correct vs incorrect predictions per confidence bin.
- **analogy**: their `high-accuracy regime masks error-miscalibration` ≅ our `problem H` — when we validate trust thresholds on a set where true matches dominate, naive calibration will look great while our rare false-positive matches stay badly miscalibrated.
- **why it transfers**: we set density/confidence gates and will want to check they're calibrated; BBS is a small, direct fix so our calibration numbers reflect the minority error class we actually care about (spurious "same-work" calls), not the easy majority.
- **why it might NOT**: it's an evaluation-metric fix, not a threshold-setting method, and still needs some correctness labels — it sharpens H diagnostics rather than solving no-gold calibration outright.
- **transfer confidence**: medium
- **bib leads**: Balanced Brier Score; ECE bias critiques; Logistic-Regression-as-calibrated-baseline framing.

---

## B — worth knowing (weaker/less-certain transfer or a useful framing)

### RACC: Regret-Aware Confidence Calibration for Masked Diffusion Decoding
- **id / track / file**: Findings ACL 2026 pp.22656 · Machine_Learning_for_NLP · `Tracks/Machine_Learning_for_NLP/Zeng_Wang_RACC_Regret_Aware_Confidence_Calibration_for_Consistent.pdf`
- **mechanism → analogy**: a training-free MOMENTUM ANCHOR tracks a confidence trajectory and fires a "regret" signal when a value drops abruptly below its historical trend — i.e. a cheap online change-point detector on a score stream ≅ our **problem D** span-boundary detection (where banded-alignment quality collapses = the end of a reused span).
- **why B not A**: their signal governs diffusion token demotion, not span segmentation; the transfer is a reframing we'd have to build and validate, but it's concrete and zero-overhead.

### Beyond Single Representations: Multi-Model Embedding Fusion for Stable Text Classification
- **id / track / file**: ACL 2026 Long pp.38568 · Machine_Learning_for_NLP · `Tracks/Machine_Learning_for_NLP/Gwak_Jung_Beyond_Single_Representations_Multi_Model_Embedding_Fus.pdf`
- **mechanism → analogy**: fusing embeddings from MULTIPLE models yields more robust reps with the biggest gains on LOW-RESOURCE data; layer effectiveness is dataset- not architecture-driven ≅ our **problem B / L / Track-3** page embeddings for low-resource Hebrew/JA.
- **why B not A**: embedding fusion is well-trodden; the value is the specific low-resource robustness finding, not a novel mechanism.

### Towards Sense-level Bilingual Dictionary Induction (SENSEBDI)
- **id / track / file**: *SEM 2026 pp.275 · SEM · `Workshops/SEM/Korber_Zhao_Towards_Sense_level_Bilingual_Dictionary_Induction.pdf`
- **mechanism → analogy**: induce cross-lingual SENSE equivalents (polysemy-aware, time-stamped) via nearest-neighbor over cross-lingual embeddings of glosses AND usages; finding: USAGES beat glosses ≅ our **problem L** JA↔Hebrew term/sense bridging.
- **why B not A**: bilingual dictionary induction is an expected place to look; the actionable nugget is "align on usage contexts, not dictionary glosses," and handle polysemy.

### Model-Agnostic Meta-Learning for Class Imbalance Adaptation (HAMR)
- **id / track / file**: Findings ACL 2026 pp.10442 · Machine_Learning_for_NLP · `Tracks/Machine_Learning_for_NLP/Rao_Huang_Model_Agnostic_Meta_Learning_for_Class_Imbalance_Adapta.pdf`
- **mechanism → analogy**: bi-level instance-weighting that prioritizes hard samples + a NEIGHBORHOOD-AWARE resampler that amplifies focus on a hard example AND its semantically-similar neighbors ≅ our **problem J / G** — upweight rare/hard witness pairs and their neighbors in Track-3 training.
- **why B not A**: class-imbalance meta-learning is findable under "hard-example mining"; the neighborhood-resampling twist is the reusable bit.

### Self-Consistency from Only Two Samples: CoT–PoT Ensembling
- **id / track / file**: Findings ACL 2026 pp.32804 · Machine_Learning_for_NLP · `Tracks/Machine_Learning_for_NLP/Saparkhan_Raza_Self_Consistency_from_Only_Two_Samples_CoT_PoT_Ensembli.pdf`
- **mechanism → analogy**: agreement between two DECORRELATED reasoning modes beats many samples of one mode (9.3× fewer samples) ≅ our **problem H / K** — trust matches where two decorrelated engines (char-n-gram seed-extend AND semantic Track-3) agree, rather than re-sampling one.
- **why B not A**: it's a self-consistency efficiency result; the "complementary-method agreement as a cheap high-precision gate" framing maps neatly onto our two-track design.

### SELECting over Tokens: Curating Pre-training Data at Scale via Token Classification
- **id / track / file**: ACL 2026 Long pp.48060 · Machine_Learning_for_NLP · `Tracks/Machine_Learning_for_NLP/Tong_Zheng_SELECting_over_Tokens_Curating_Pre_training_Data_at_Sca.pdf`
- **mechanism → analogy**: reframe data cleaning as PER-TOKEN informative/noisy classification (fine-grained, no generation) instead of per-sample heuristics ≅ our **problem J / A** — a per-character reliability model to mask HTR-garbage positions before/within alignment.
- **why B not A**: needs token-level noise labels; the framing (token-classification beats sample-heuristics and generation for noise removal) is the transferable idea.

### KV-Embedding: Training-free Text Embedding via Internal KV Re-routing
- **id / track / file**: ACL 2026 Long pp.11773 · Machine_Learning_for_NLP · `Tracks/Machine_Learning_for_NLP/Tang_Yang_KV_Embedding_Training_free_Text_Embedding_via_Internal.pdf`
- **mechanism → analogy**: turn a FROZEN decoder LLM into a strong embedder by re-routing final-token KV states as a prefix (all tokens see full context in one pass); automated LAYER selection by intrinsic dimensionality ≅ our **problem B / Track-3** cheap embedding baseline without contrastive fine-tuning.
- **why B not A**: on-topic embedding-methods work; genuinely useful as a no-train bootstrap for a Hebrew/JA LLM we can't easily fine-tune.

### FRANQ: Faithfulness-Aware Uncertainty Quantification for RAG
- **id / track / file**: Findings ACL 2026 pp.6814 · Machine_Learning_for_NLP · `Tracks/Machine_Learning_for_NLP/Fadeeva_Panov_Faithfulness_Aware_Uncertainty_Quantification_for_Fact.pdf`
- **mechanism → analogy**: don't conflate two notions of correctness — first classify a claim (faithful-to-context or not), then apply a DIFFERENT uncertainty estimator conditioned on that class ≅ our **problem H** — route a match to a different trust estimator by match type (reference-edition hit vs manuscript-manuscript hit).
- **why B not A**: the conditional-UQ design pattern is useful but abstract; the transfer is a framing, not a runnable component.

### EvoEdit: Evolving Null-space Alignment for Knowledge Editing
- **id / track / file**: Findings ACL 2026 pp.1520 · Machine_Learning_for_NLP · `Tracks/Machine_Learning_for_NLP/Lyu_Lu_EvoEdit_Evolving_Null_space_Alignment_for_Robust_and_Ef.pdf`
- **mechanism → analogy**: project edits into the NULL SPACE of knowledge you must preserve, with output-invariance guarantees ≅ our **problem I** — remove a factor (scribe hand) by projecting embeddings onto the null-space of a "preserve-the-content" subspace.
- **why B not A**: it's a continual-editing method; the null-space-projection-for-invariance mechanism is real but the concept-removal use is speculative.

### Entropy-aware Masking for Masked Language Modeling
- **id / track / file**: *SEM 2026 pp.395 · SEM · `Workshops/SEM/Srinivasagan_Georges_Entropy_aware_Masking_for_Masked_Language_Modeling.pdf`
- **mechanism → analogy**: mask the highest-entropy (most informative/uncertain) tokens rather than random, with a SELF-masking variant needing no external reference model ≅ our **problem G** — efficient MLM pretraining of a Hebrew/JA encoder under low resource.
- **why B not A**: informative-token-masking is a modest curriculum tweak; the no-external-reference self-masking is the low-resource-friendly part.

---

## C / skipped — coverage honesty

**Whole classes deliberately skipped (title-triaged, not carded):** the dominant
~120+ ML4NLP papers are RL-for-reasoning training internals with no transfer to
corpus-scale text-reuse — GRPO/DPO/PPO variants, entropy/exploration control,
reward shaping and reward-hacking, credit assignment, KL-divergence estimation,
policy-optimization stability, LoRA/rank-allocation, quantization/4-bit,
Muon/optimizer tricks, long-context (16M) and decoding-speed (diffusion/non-AR),
agentic tool-use and multi-turn reasoning, test-time scaling, and model
merging/editing. I abstract-checked a representative ~25 of these; none surfaced
a domain-neutral mechanism for A/B/C/D/E/F beyond what is carded above.

**Abstract-checked but judged C (resemblance-only or no runnable transfer):**
- `Bansal_..._Curriculum_Driven_DPO` — generic curriculum + preferred/non-preferred DPO; nothing beyond standard.
- `Zheng_Liu_Training_Free_Test_Time_Contrastive_Learning` — "contrastive" is over reasoning-trajectory RULES, not embeddings.
- `Jang_Kim_A_Few_Bad_Apples` — "few critical tokens dominate collapse" is an RL-internal echo of our low-DF-signal intuition, no mechanism.
- `Guan_Meng_Knowing_When_to_Quit` — abstention via RL reward shaping; H-adjacent but a training method, not a threshold tool.
- `Zangari_Medya_Colorful_Talks_with_Graphs` — graph→text prompting (WL classes → color tokens); prompting, not clustering.
- `Chen_Xiong` is A (GenDis); noted to avoid confusion.
- `Zhang_Huang_LGSA` — embeds a KNOWN label hierarchy via an orthogonal frame; we must DISCOVER our hierarchy, so it inverts our problem (would help only if we align witnesses to a fixed composition taxonomy).
- `Gong_Wang_Punctuation_Steered_ReFT` — punctuation tokens as representation hubs; too niche.
- `Zhang_Bollegala_Synthetic_Data_Generation` — synthetic diversity for commonsense generation; not our need.
- `Liu_Yu_SemToken` (*SEM) — semantic-density variable-granularity tokenization + merge equivalent spans; a faint D/C resemblance for adaptive seeding, but efficiency-motivated, low transfer.
- `Olsen_Pado_Finding_Sense_in_Nonsense` (*SEM) — anomalous-vs-nonsensical distinction; poetic tie to "OCR-garbage vs genuine rare reading," no scalable mechanism.
- `Tat_Speelman_ReFRAME` (*SEM) — frame-semantics LSC; part of the LSCD isomorphic cluster (see LSCD card A) but the frame-semantics method itself doesn't transfer.

**Noted caveat (not a transfer, a warning):** `Momen_Zarrieß_Frequency_Confound`
(*SEM) shows LM surprisal is confounded by raw lexical frequency — a direct
caution for us: our claim that low-DF n-grams are "distinctive" is partly a
frequency artifact; control for frequency before attributing significance to
rare-gram matches.
