# WAVE 2 — Safety & Alignment track (analogy hunt)

Scope: `Tracks/Safety_and_Alignment/` non-poster papers (324). Posters ignored.
Method: strict title-triage; abstract-checked ~30 mechanism-promising titles; deep
enough to card the ones below. The track is dominated by jailbreak-attack/defense,
RLHF/reward-modeling, and unlearning-of-facts papers with no transfer — but a rich
seam of **representation-surgery** (SAE / orthogonal-subspace / causal-head) and
**no-gold calibration** methods hides here, plus one language-invariance geometry
result that is a direct hit for our JA↔Hebrew problem.

---

## A — adopt (transferable + we would NOT have found it by domain keyword)

### LASA: Language-Agnostic Semantic Alignment at the Semantic Bottleneck for LLM Safety
- **id / track / file**: ACL2026 pp.41238–41259 · Safety & Alignment · `Yang_Huang_LASA_Language_Agnostic_Semantic_Alignment_at_the_Semant.pdf`
- **surface domain**: making LLM safety refusals work in low-resource languages, not just English.
- **mechanism**: they empirically locate a *semantic bottleneck* — an intermediate layer where representation geometry is governed by **shared semantic content, not language identity** — and operate there so behaviour is language-invariant.
- **analogy**: their *"layer where language identity drops out and only content remains"* ≅ our **problem L (cross-representation alignment, Judeo-Arabic↔Hebrew↔Aramaic)** and **problem I (strip language/script factor)**.
- **why it transfers**: for Track-3 we want a representation in which the *same work* copied by a JA scribe and a Hebrew scribe lands in the same place. LASA gives a concrete, testable recipe: probe layers for the one whose geometry is driven by content over script, and do matching/embedding there — rather than fighting script identity at the surface.
- **why it might NOT**: their "bottleneck" is a property of a *generative multilingual LLM*; our HTR-transcription encoder may not have such a clean layer, and Hebrew/JA/Aramaic are far closer scripts than their high/low-resource split.
- **transfer confidence**: high
- **priority**: A
- **bib leads**: the multilingual-safety-gap literature they build on (Qwen/LLaMA cross-lingual ASR studies); the linear-representation-hypothesis probing they assume.

### Principled Detection of Hallucinations via Multiple Testing
- **id / track / file**: ACL2026 pp.34132–34145 · Safety & Alignment · `Li_Veeravalli_Principled_Detection_of_Hallucinations_in_Large_Languag.pdf`
- **surface domain**: deciding when an LLM answer is a hallucination, without a per-answer gold label.
- **mechanism**: cast the decision as a **hypothesis test**, aggregate many heterogeneous empirical scores via **conformal p-values + multiple-testing (FDR-style) control**, giving calibrated accept/reject with a *provably controlled false-alarm rate* and no ground truth.
- **analogy**: their *"combine many unreliable scores into one calibrated decision with a controlled false-positive rate"* ≅ our **problem H (calibration / threshold choice with NO gold)** — our density gates + confidence labels.
- **why it transfers**: SEED-029 currently trusts matches via hand-tuned density gates. This replaces the guesswork: feed n-gram density, banded-Levenshtein ratio, reference-edition score and (future) embedding similarity in as separate scores, get conformal p-values, and set one FDR level — a defensible false-positive budget across 1.34M pairs with no manual thresholds.
- **why it might NOT**: conformal p-values need an exchangeable calibration set of *known* non-reuse pairs; our pair distribution is heavily corpus-dependent and "true negative" is fuzzy for related liturgy.
- **transfer confidence**: medium-high
- **priority**: A
- **bib leads**: conformal prediction (Vovk; Angelopoulos–Bates); Benjamini–Hochberg FDR; OOD-detection-as-testing framing they cite.

### CRISP: Persistent Concept Unlearning via Sparse Autoencoders
- **id / track / file**: ACL2026 pp.1806–1825 · Safety & Alignment · `Ashuach_Belinkov_CRISP_Persistent_Concept_Unlearning_via_Sparse_Autoenco.pdf`
- **surface domain**: permanently removing dangerous knowledge (WMDP) from an LLM while keeping general ability.
- **mechanism**: train **sparse autoencoders** on activations to get monosemantic features, **automatically identify the salient features for the target concept across layers, and suppress their activations** — baked into parameters, not just inference-time steering.
- **analogy**: their *"find the SAE features that encode concept X and null them"* ≅ our **problem I (concept removal / disentanglement — strip scribe-hand / script / genre from vectors)**. This is the profile's canonical "SAE that erases a concept."
- **why it transfers**: our planned Track-3 embeddings will entangle *what the text says* with *who wrote it / which script*. An SAE over those embeddings could isolate a monosemantic "Judeo-Arabic-script" or "scribe-hand" feature and null it, so witnesses of one work cluster by content regardless of hand — the exact disentanglement we lack.
- **why it might NOT**: SAEs need lots of activation data and a model to attach to; our retrieval embeddings aren't a generative residual stream, and monosemanticity may not hold for paleographic factors.
- **transfer confidence**: medium
- **priority**: A
- **bib leads**: WMDP benchmark; Anthropic/OpenAI SAE dictionary-learning line; SAILS (below) and Safe-SAIL (below) are same-family and worth reading together.

### ReGLU: Representation-Guided Low-rank (Parameter-Efficient) LLM Unlearning
- **id / track / file**: ACL2026 (Findings) pp.14602–14616 · Safety & Alignment · `Xiao_Chen_Representation_Guided_Parameter_Efficient_LLM_Unlearnin.pdf`
- **surface domain**: forgetting a "forget set" of facts via LoRA without wrecking a "retain set".
- **mechanism**: a **representation-guided LoRA**: initialise in the subspace best for forgetting, then a regulariser **constrains the update to lie in the orthogonal complement of the retain set's representation subspace**, so removal doesn't interfere with what must be kept. Directly attacks the *superposition/polysemanticity* problem.
- **analogy**: their *"edit only in the orthogonal complement of the subspace you must preserve"* ≅ our **problem I** — remove a nuisance factor (script/scribe) while provably not disturbing the content subspace used for retrieval.
- **why it transfers**: gives a clean, runnable geometry for factor removal on *our* embeddings: estimate the content ("retain") subspace from same-work witness pairs, then project/learn the script-removal in its orthogonal complement — denoise without degrading recall.
- **why it might NOT**: assumes forget/retain subspaces are approximately separable; our content and script signals may be more entangled than fact-level forget/retain sets.
- **transfer confidence**: medium
- **priority**: A
- **bib leads**: superposition/polysemanticity (Elhage et al.); LoRA; task-vector / model-editing arithmetic.

### CausalDetox: Causal Head Selection and Intervention for Detoxification
- **id / track / file**: ACL2026 (Findings) pp.11893–11914 · Safety & Alignment · `Wang_Sundaram_CausalDetox_Causal_Head_Selection_and_Intervention_for.pdf`
- **surface domain**: removing toxic generation by editing the responsible attention heads.
- **mechanism**: use **Probability of Necessity and Sufficiency (PNS)** to isolate the *minimal set* of attention heads necessary-and-sufficient for a factor, then steer/fine-tune only those — evaluated on **aligned counterfactual pairs** (same sentence, factor toggled: PARATOX/ParaDetox).
- **analogy**: their *"PNS-minimal causal component set for factor X + aligned minimal-pairs to measure it"* ≅ our **problem I (locate & excise the factor)**, and the minimal-pair construction ≅ our natural data: *same work, different scribe/script*.
- **why it transfers**: two adoptable pieces — (1) a causal (not just correlational) way to find the few dimensions/heads carrying "scribe hand," and (2) the counterfactual-pair evaluation, which we can build directly from multi-witness copies of one work in the Genizah corpus to measure whether a factor was truly removed.
- **why it might NOT**: PNS estimation wants clean counterfactual toggling; scribe/script "toggling" in real manuscripts co-varies with date, region, and content, so the pairs aren't as controlled as PARATOX.
- **transfer confidence**: medium
- **priority**: A
- **bib leads**: Pearl's necessity/sufficiency (PNS); ToxiGen / ImplicitHate / ParaDetox; inference-time intervention (ITI) steering.

---

## B — worth knowing (weaker or less-certain transfer, or a useful framing)

### EZ-MIA: Powerful Training-Free Membership Inference Against Fine-Tuned LMs
- **id / track / file**: ACL2026 pp.14077–14093 · `Ilic_Cvejoski_Powerful_Training_Free_Membership_Inference_Against_Fin.pdf`
- **surface domain**: auditing whether a specific text was in a model's fine-tuning data.
- **mechanism**: the **Error Zone score** — memorization shows most strongly *at error positions* (tokens the model gets wrong yet assigns elevated probability), measured as directional probability imbalance vs a clean **reference model**; two forward passes, no training.
- **analogy**: membership ≅ **our problem B** ("was this passage copied from a reference edition?"). Their "signal concentrates at disagreement points relative to a clean reference" mirrors our **asymmetric matcher against 8,300 clean reference editions**: reuse evidence concentrates where the noisy witness diverges from the clean text.
- **why it transfers**: a framing/diagnostic, not a runnable algorithm for us — it reinforces designing the reference matcher around *divergence positions* rather than global similarity.
- **why it might NOT**: the mechanism is built on LM token-probabilities; our pipeline is string-alignment with no probabilistic reference model.
- **transfer confidence**: speculative
- **priority**: B

### Hallucinations as Orthogonal Noise (Dynamic Contextual Orthogonalization)
- **id / track / file**: ACL2026 (Findings) pp.36575–36586 · `Zhao_Li_Hallucinations_as_Orthogonal_Noise_Inference_Time_Manif.pdf`
- **surface domain**: reducing hallucination by editing the residual stream at inference time.
- **mechanism**: hallucination = **component orthogonal to the context/semantic subspace**; project it out at inference (no training) — the inference-time twin of ReGLU's orthogonal-complement idea (problem I).
- **why it transfers**: a *training-free* projection to strip an off-manifold nuisance from embeddings — attractive because it needs no retraining of Track-3.
- **why it might NOT**: relies on the linear-representation hypothesis holding for our factor; identifying the "content manifold" for fragmentary noisy text is non-trivial.
- **transfer confidence**: medium · **priority**: B

### SAILS / Safe-SAIL — SAE-based safety subspaces (companion reads to CRISP)
- **id / track / file**: `Wang_He_Interpretable_Safety_Alignment_via_SAE_Constructed_Low.pdf` (ACL2026 pp.4703–4721) and `Weng_Wang_Safe_SAIL_Towards_a_Fine_grained_Safety_Landscape_of_La.pdf` (Findings pp.18916–18935)
- **mechanism**: SAILS builds an *interpretable subspace from SAE decoder directions* and inits LoRA there (with a monosemanticity recovery-error bound); Safe-SAIL is a framework for *choosing which SAE yields the best features for a target concept domain* and cheaply explaining them.
- **analogy**: both = **problem I** infrastructure — how to derive and validate a monosemantic subspace for a factor we want to remove/steer (script, scribe).
- **why it might NOT**: same SAE-availability caveats as CRISP. **transfer confidence**: medium · **priority**: B

### Targeted Neuron Tuning (Neuronal Insights into LLM Attacks)
- **id / track / file**: ACL2026 (Findings) pp.34414–34435 · `Shi_Xiong_Neuronal_Insights_into_LLM_Attacks_Targeted_Neuron_Tuni.pdf`
- **mechanism**: a **similarity-based gradient attribution that works in open-ended generation with no fixed ground-truth output**, to localise the neurons carrying a behaviour; finds "universal" vs "interference" neurons.
- **analogy**: **problem I + H** — factor localisation *without gold labels*, which is our regime.
- **why it might NOT**: neuron-level, generative-model-specific. **transfer confidence**: speculative · **priority**: B

### MAST: Multi-View Alignment + Optimal-Transport Contrastive Clustering of Short Text
- **id / track / file**: ACL2026 (Findings) pp.13483–13500 · `Zheng_Lu_MAST_A_Multi_View_Alignment_Strategy_for_Optimal_Transp.pdf`
- **mechanism**: contrastive clustering of *short texts* with an **optimal-transport pseudo-label assignment** (global, to stop pseudo-label noise accumulating) + high-confidence guided refinement + structure-aware negative reweighting.
- **analogy**: **problem E (motif clustering / witness census)** + **G (contrastive with silver labels)** + **L (OT)**. Short-text clustering ≅ clustering fragmentary witnesses; OT global assignment ≅ resolving noisy motif-cluster membership.
- **why it might NOT**: designed for clean short docs, not 16–20% CER fragments; OT cost matrix at 10^6 scale is heavy. **transfer confidence**: medium · **priority**: B

### G-IdiomAlign: Gloss-Pivoted Cross-Lingual Idiom Alignment
- **id / track / file**: ACL2026 pp.38720–38739 · `Ye_Wong_G_IdiomAlign_A_Gloss_Pivoted_Benchmark_for_Cross_Lingua.pdf`
- **mechanism**: align non-compositional idioms across languages via an **English gloss pivot** because literal surface mapping fails; contrast no-gloss vs with-gloss to isolate the pivot's effect.
- **analogy**: **problem L (pivot/bridge alignment)** — align a JA phrase to its Hebrew counterpart through a normalized-meaning pivot when surface forms don't line up.
- **why it might NOT**: it's a benchmark, not a scalable method; we lack a Wiktionary-grade gloss resource for medieval JA. **transfer confidence**: medium · **priority**: B

### Beyond Neural Incompatibility: Cross-Scale Knowledge Transfer via Latent Semantic Alignment
- **id / track / file**: ACL2026 (Findings) pp.21893–21905 · `Gu_Zhang_Beyond_Neural_Incompatibility_Cross_Scale_Knowledge_Tra.pdf`
- **mechanism**: transfer knowledge between models of different architecture/scale by **aligning their latent activation spaces** (Procrustes-like) instead of moving parameters.
- **analogy**: **problem L** — align two embedding spaces (two HTR-model versions, or a JA-tuned vs Hebrew-tuned encoder) so matches are comparable across them.
- **why it might NOT**: about parametric knowledge transfer, not retrieval; alignment quality across our noisy encoders unproven. **transfer confidence**: speculative · **priority**: B

### SWAN: Semantic Watermarking with Abstract Meaning Representation
- **id / track / file**: ACL2026 pp.36304–36315 · `Ye_Mehrabi_SWAN_Semantic_Watermarking_with_Abstract_Meaning_Repres.pdf`
- **mechanism**: embed a signature in the **AMR semantic structure**, so *any meaning-preserving paraphrase preserves the signature*; detect via AMR parse + one-proportion z-test.
- **analogy**: **problem B (robust variant)** — represent a text at a *variation-invariant structural level* so heavy scribal variation doesn't destroy the match; the z-test detection is a clean no-gold statistic.
- **why it might NOT**: no AMR parser exists for medieval Hebrew/JA; watermarking injects a known signal, we detect an unknown one. **transfer confidence**: speculative (framing) · **priority**: B

### TrajGuard: Streaming Hidden-state Trajectory Detection
- **id / track / file**: ACL2026 (Findings) pp.13371–13388 · `Liu_Ding_TrajGuard_Streaming_Hidden_state_Trajectory_Detection_f.pdf`
- **mechanism**: a **cheap sliding-window monitor** accumulates risk over a decoding trajectory and **escalates to an expensive check only when windowed risk persistently exceeds a threshold**.
- **analogy**: **problem K (cheap-candidate → expensive-verify)** — our two-stage matcher: let a cheap per-window score accumulate, trigger banded-Levenshtein verification only where it persistently crosses threshold.
- **why it might NOT**: their signal is LLM hidden states; the pattern is generic but the specifics don't port. **transfer confidence**: speculative (framing) · **priority**: B

### Debate to Align: Reliable Entity Alignment via Two-Stage Multi-Agent Debate
- **id / track / file**: ACL2026 (Findings) pp.5989–6010 · `Wang_Bao_Debate_to_Align_Reliable_Entity_Alignment_through_Two_S.pdf`
- **mechanism**: cheap **embedding similarity flags an "alignment-uncertain" set**, then a **multi-agent LLM debate** resolves only those hard cases.
- **analogy**: **L (link witnesses of one work across manuscripts = cross-KG entity alignment) + K (escalate only uncertain pairs) + H (committee/debate adjudication)**.
- **why it might NOT**: KG-entity setting; debate over medieval-text witness identity would need domain-grounded agents. **transfer confidence**: medium · **priority**: B

### RCTEA: Richness-guided Co-training for Temporal Entity Alignment
- **id / track / file**: ACL2026 (Findings) pp.39295–39310 · `Li_RCTEA_Richness_guided_Co_training_for_Temporal_Entity_A.pdf`
- **mechanism**: jointly model **structural + temporal** features (treated as "orthogonal yet complementary") to align entities across *temporal* knowledge graphs.
- **analogy**: **problem F (lineage / stemma — WE DON'T DO THIS YET)** — combining shared-reading *structure* with *dating* to reconstruct who-copied-whom is exactly the structural+temporal fusion a stemma needs.
- **why it might NOT**: GNN-on-KG method, far from text; a genuine stemma also needs directionality (ancestry), which entity alignment doesn't give. **transfer confidence**: speculative · **priority**: B

### Judge-robustness cluster (caveats for our H / committee design)
- `Zahraei_HakkaniTur_Prior_Beliefs_Prejudice_LLM_as_Judge...` (ACL2026 pp.42049–42082): LLM judges conflate their *trained priors* with quality — a warning for any LLM-adjudicator we use to rank witnesses (it may over-favour canonical/famous texts); their controlled-stance-variation design is a way to *measure* judge bias.
- `Yang_Tsvetkov_Among_Us_Measuring_and_Mitigating_Malicious_Contributio...` (ACL2026 pp.15969–15988): detecting/​down-weighting a bad member in a multi-model committee — robustness of our committee-of-signals.
- `Cui_Wang_Towards_Provably_Secure...Reliable_Consensus...` (Findings pp.23040–23055): **consensus/abstention with provable risk control** — accept a match only when independent signals agree, abstain otherwise (complements the Principled-Hallucination-Detection A-card).
- **analogy**: all → **problem H** · **transfer confidence**: medium (framing) · **priority**: B

### WaveDetect: Machine-Generated-Text Detection via Wavelet Transform
- **id / track / file**: ACL2026 (Findings) pp.8712–8727 · `Liu_Xu_WaveDetect_Robust_Framework_for_Machine_Generated_Text.pdf`
- **mechanism**: treat a token-probability stream as a **signal**, apply a **continuous wavelet transform** to get perturbation-/domain-robust "spectral fingerprints."
- **analogy**: **A (noisy alignment) / B** — a frequency-domain representation robust to *adversarial perturbation* is a tantalising analogy for representing a transcription robustly under 16–20% CER, but we have characters, not probability streams.
- **why it might NOT**: no probability signal in our pipeline; purely a representational analogy. **transfer confidence**: speculative · **priority**: B

---

## C / skipped — coverage notes (honest)

Whole classes deliberately skipped as **no real transfer** (surface-domain only):

- **Jailbreak attacks/defenses** (by far the largest class, ~90+ papers): multi-turn/role-play/reasoning-hijack/code-template/visual jailbreaks, guardrails, over-refusal trade-offs, safety-drift after fine-tuning, red-teaming frameworks. Mechanisms are prompt/decoding-space attack search or refusal-tuning — no isomorphism to reuse/alignment. (Checked `Song_Zhang` visual-degradation and `Han_Ruan` sparse-logit-editing as possible A-vision/perturbation analogies — both turned out to be about model *attention/logits*, not character/visual confusion; skipped.)
- **Watermark embedding** (~15 papers: DualGuard, ReasMark, TDMA→CDMA, QuantileMark, EntroBench, Topic-based, RShield, AgentMark, "The Mark Fades" paraphrase-attack, etc.): injecting a *known* signal into generated text is the inverse of detecting *unknown* reuse. Kept only SWAN (variation-invariant framing) and noted TTP-Detect/black-box detection as non-transferring.
- **Reward modeling / RLHF / DPO / preference alignment** (~40 papers: generative reward models, consistency/self-reflection RMs, pairwise↔pointwise, curriculum RLAIF, MoE routing, GRPO variants): pure alignment training — out of scope per instructions.
- **Fact-forgetting unlearning** (DUSK, CURaTE, CiPO, LUNE, AGTAO, Rotation-Control, Anatomy-of-Unlearning, Decoding-Unlearning, multimodal unlearning): kept only the *representation-geometry* ones (CRISP, ReGLU); the rest are forget-set/retain-set benchmarking with no factor-disentanglement mechanism for us.
- **Safety/value benchmarks & evals** (XGUARD, FinSafetyBench, MolSafeEval, PII-VisBench, MHSafeEval, COMPASS, value-alignment/value-structure measurement, persona/companion safety): dataset papers, no transferable algorithm.
- **PII / privacy / de-identification** (SecureGate, Privacy-Collapse, Towards-Privacy-Preserving-Text): phenomenon/eval, no mechanism.
- **Multi-agent / GUI-agent / tool-safety / backdoor-in-RLVR** and **model-merging safety** (SafeMERGE, OASIS, push-pull, topology/persistent-homology alignment, multi-task MI alignment): representation-alignment *titles* but the mechanisms are training-stability/task-balancing, not cross-space or factor-removal transfer. Persistent-homology (Pan_Peng) and MI multi-task (Hu_Zhang) checked and skipped.

**No homoglyph / Unicode-confusable / transliteration-jailbreak paper found in this
track** (the profile flagged these as A/L hopes) — the closest visual paper
(`Song_Zhang`) is about attention overload, not character confusion. Coverage of that
sub-hunt: negative result.
