# WAVE 2 — LLM Efficiency (+ Language Models title-triage) — analogy cards

Scope: `Tracks/LLM_Efficiency/` (272 papers) scanned in full by title; strong
mechanism candidates abstract-checked. `Tracks/Language_Models/` (480) skimmed
by title, abstract-checking ONLY the assigned buckets (tokenization, data
selection/dedup, long-context, embeddings, scale-engineering) plus the profile's
explicitly-called-out mechanisms (membership-inference/contamination → B,
segmentation → D, matching → C). Domain "make LLMs cheap"; hunting the scale
algorithms underneath, not the on-domain match.

---

## A — adopt (transferable mechanism we would NOT have found by domain keyword)

### Confidence-Weighted Token Set Cover for Early Hypothesis Pruning in Self-Consistency
- **id / track / file**: ACL2026 Findings pp.41148–41155 · LLM_Efficiency · `LLM_Efficiency/Sultan_Astudillo_Confidence_Weighted_Token_Set_Cover_for_Early_Hypothesi.pdf`
- **surface domain**: pruning redundant chain-of-thought hypotheses in self-consistency (majority voting) to save tokens.
- **mechanism**: a fast **weighted set-cover** algorithm that keeps a minimal subset of items whose *lexical coverage* explains all the rest, weighted by a per-item confidence score.
- **analogy**: their `weighted-set-cover over hypotheses by lexical coverage`  ≅  our `competitive span "shadowing" (problem C) — keep the best span that covers a text region when several claim it`.
- **why it transfers**: our shadowing is currently greedy "keep the highest-scoring span, mark the rest shadowed." Reframing it as *confidence-weighted set cover over covered character positions* gives a principled objective (minimum weighted set of witness-spans that covers the shared region) and a fast greedy algorithm with an approximation guarantee — a drop-in upgrade to `track1_shadow.py` that could reduce the residual double-counting the census still shows.
- **why it might NOT**: set cover optimizes *coverage minimality*, whereas shadowing also cares about *which* work owns a region — we'd need to fold ownership/priority into the weights, not just coverage.
- **transfer confidence**: high
- **bib leads**: greedy weighted set-cover (Chvátal 1979) as the algorithmic core; Aggarwal et al. 2023 (adaptive self-consistency).

### LycheeCluster: Efficient Long-Context Inference with Structure-Aware Chunking and Hierarchical KV Indexing
- **id / track / file**: ACL2026 Findings pp.7607–7623 · LLM_Efficiency · `LLM_Efficiency/Li_Zhang_LycheeCluster_Efficient_Long_Context_Inference_with_Str.pdf`
- **surface domain**: KV-cache management — retrieve the relevant cached key-value blocks for long-context decoding without scanning the whole cache.
- **mechanism**: **boundary-aware chunking** (segment at semantic seams, not fixed size) + a **recursive hierarchical index rooted in the triangle inequality** that turns candidate retrieval from a linear scan into theoretically-bounded **logarithmic-time pruning** (a metric-tree / cover-tree over vectors).
- **analogy**: their `triangle-inequality metric tree for O(log n) candidate pruning`  ≅  our `all-pairs near-duplicate candidate generation without O(n²) (problem B)`; and their `boundary-aware chunking`  ≅  our `embedding-unit / motif breakpoints (problem D)`.
- **why it transfers**: for the planned Track-3 semantic layer, a triangle-inequality metric tree (VP-/ball-/cover-tree) over embeddings gives exact logarithmic-time pruning of the candidate set — an alternative to (or pre-filter before) LSH/PQ for the 62K-MS / 948K-page corpus. The boundary-aware chunking is directly the "where do embedding units begin/end" question we currently answer with fixed windows.
- **why it might NOT**: metric-tree pruning degrades toward linear in high-dimensional embedding spaces (curse of dimensionality) unless intrinsic dim is low; may need PQ/LSH anyway at 10^6 scale.
- **transfer confidence**: medium
- **bib leads**: Quest, ClusterKV (their baselines); cover-tree / VP-tree metric-space ANN literature.

### Distilling Large Embeddings via Hyperspherical Householder Quantization (HHQ)
- **id / track / file**: ACL2026 Main (Long) pp.10562–10576 · LLM_Efficiency · `LLM_Efficiency/Wang_Cheng_Distilling_Large_Embeddings_via_Hyperspherical_Househol.pdf`
- **surface domain**: compressing dense retrieval embeddings into short discrete document identifiers for generative retrieval.
- **mechanism**: **angular (cosine-preserving) quantization** — iterative Householder transforms on the unit hypersphere compress an embedding into a *short* discrete code while explicitly preserving cosine similarity at each step; Euclidean quantization is shown to be geometrically misaligned with contrastively-trained embeddings.
- **analogy**: their `hyperspherical code for contrastive embeddings`  ≅  our `binary / product-quantized embedding codes for corpus-scale candidate generation (problem B) + Track-3 (problem G)`. Wave 1 flagged Isolation-Kernel binary embeddings — this is a *better* code for the same job.
- **why it transfers**: Track-3 embeddings will be contrastively trained (angular geometry), so the standard Euclidean PQ/binary codes wave 1 considered are provably misaligned; HHQ's cosine-preserving codes give higher recall per bit for the ANN candidate stage, at ~5 tokens/doc.
- **why it might NOT**: designed for a *generative* retriever that decodes identifiers token-by-token; using just the codes for classic ANN drops that machinery and the gains may shrink.
- **transfer confidence**: medium-high
- **bib leads**: Matryoshka Representation Learning (MRL); DSI / generative retrieval (Tay et al. 2022).

### HeteroSpec + Speculative Verification — adaptive verification-effort allocation
- **id / track / file**: ACL2026 Main (Long) pp.12930–12947 · LLM_Efficiency · `LLM_Efficiency/Liu_He_HeteroSpec_Leveraging_Contextual_Heterogeneity_for_Effi.pdf` (paired: ACL2026 Findings pp.36290–36307 · `LLM_Efficiency/Kim_Seo_Speculative_Verification_Exploiting_Information_Gain_fo.pdf`)
- **surface domain**: speculative decoding — a small draft model proposes tokens; a large model verifies them in parallel.
- **mechanism**: **allocate expensive verification in proportion to a cheap uncertainty signal.** HeteroSpec uses a lightweight *entropy* quantifier to stratify draft candidates and spends verification depth only where it pays; Speculative Verification adds a tiny companion model and uses its *information gain* to decide how much verification to run.
- **analogy**: their `cheap predictor of whether the expensive verifier will accept → budget the verifier accordingly`  ≅  our `two-stage matcher (problem K) — cheap seed/DF signal decides whether to run the expensive banded-Levenshtein verification` and the calibration of that gate (problem H).
- **why it transfers**: our DF-banded candidate gate is essentially a fixed threshold. Making it *adaptive* — stratify candidate pairs by a cheap entropy/seed-density signal and spend banded-Levenshtein effort proportional to expected payoff — directly targets the two-pass disk-spill engine's compute budget and the "new? queue" triage (1,168 items).
- **why it might NOT**: their signals come from a language model's token distribution; we'd have to hand-craft the cheap predictor (seed density, DF profile) and calibrate it, which is exactly the hard part.
- **transfer confidence**: medium-high
- **bib leads**: EAGLE-3 (their SOTA baseline); Leviathan et al. 2023 (speculative decoding).

### ACSE: An Ancient Character Semantic-Aware Embedding for LLMs
- **id / track / file**: ACL2026 Findings pp.9000–9012 · Language_Models · `Language_Models/Zhou_Xu_ACSE_An_Ancient_Character_Semantic_Aware_Embedding_for.pdf`
- **surface domain**: embedding pre-Qin Chinese excavated scripts (Oracle Bone, Bronze, Bamboo) that are low-digitization, scarce-corpus, glyph-rich.
- **mechanism**: fuse **glyph + lexical** features of an ancient character and **map it into the modern/canonical semantic space** via a lightweight two-stage parameter-efficient training scheme.
- **analogy**: their `ancient-script → modern-semantic-space bridge with glyph fusion`  ≅  our `cross-representation alignment (problem L: Judeo-Arabic ↔ Hebrew, script ↔ script)` and `stripping/injecting the glyph/visual factor (problem I)`.
- **why it transfers**: a near-exact structural match to our unbuilt cross-script bridge — map noisy HTR'd Genizah script (16–20% CER, glyph-level errors) into a clean reference semantic space, using glyph features to help where the character stream is corrupt. The two-stage lightweight training is feasible on our small clean-reference set (8,300 editions).
- **why it might NOT**: Chinese logographs carry semantics *in the glyph*; Hebrew/Judeo-Arabic glyphs are phonemic, so the "glyph carries meaning" premise is weaker for us (though glyph-shape confusions ARE our HTR error model).
- **transfer confidence**: medium
- **bib leads**: adapter / two-stage PEFT for embeddings; glyph-aware CJK encoders (Glyce-style).

### LEAF: Knowledge Distillation of Text Embedding Models with Teacher-Aligned Representations
- **id / track / file**: ACL2026 Main (Long) pp.43362–43383 · LLM_Efficiency · `LLM_Efficiency/Vujanic_Ruckstie_LEAF_Knowledge_Distillation_of_Text_Embedding_Models_wi.pdf`
- **surface domain**: distilling a small text-embedding model that stays *compatible* with its large teacher's vector space (for RAG retrieval).
- **mechanism**: **asymmetric bi-encoder in one shared space** — documents encoded by the large teacher, queries by a small distilled "leaf" model living in the *same* space; no hard negatives, no relevance judgments, small batches; inherits Matryoshka + quantization robustness for free.
- **analogy**: their `asymmetric expensive-doc-side / cheap-query-side encoders sharing a space`  ≅  our `asymmetric matcher against 8,300 clean reference editions (problem G + reference-edge layer)`.
- **why it transfers**: our reference edge is intrinsically asymmetric — clean printed references vs noisy HTR manuscripts. LEAF is a concrete recipe to encode the expensive/clean reference side with a strong teacher and the noisy manuscript side with a cheap student that lands in the same space, trainable without the hard-negative mining we can't easily source.
- **why it might NOT**: LEAF assumes both sides are the *same modality/language* as the teacher; our two sides differ in noise level and sometimes language (JA vs Hebrew), which pure distillation may not bridge without the ACSE-style alignment above.
- **transfer confidence**: medium
- **bib leads**: Matryoshka Representation Learning; BEIR / MTEB (their eval); RepLLaMA.

---

## B — worth knowing (plausible but weaker / less-certain transfer, or a useful framing)

### RACER + LogitSpec — retrieval-based speculative decoding (draft = retrieved span)
- **id / track / file**: ACL2026 Findings pp.19962–19988 / pp.33070–33092 · LLM_Efficiency · `LLM_Efficiency/Zhang_Zhao_RACER_...pdf`, `LLM_Efficiency/Liu_Sun_LogitSpec_...pdf`
- **surface domain**: training-free speculative decoding where draft tokens are *retrieved* from context rather than produced by a draft model.
- **mechanism**: retrieve an **exact-match reference span** as the draft, and when no exact match exists, **extrapolate** with a cheap logit signal (RACER unifies both; LogitSpec widens the retrieval net by speculating the next-next token).
- **analogy**: their `exact-anchor retrieval + flexible extrapolation`  ≅  our `char-n-gram seed (exact) + banded-Levenshtein extend (problem A/K)` — the exact-match-breaks-under-noise problem is literally ours at 16–20% CER.
- **why it transfers / might NOT**: validates the seed+extend split and the "expand the retrieval range when the exact seed fails" trick; but their extrapolation uses an LM's logits, which we don't have on the corpus side — we'd substitute approximate/fuzzy n-gram seeds.
- **transfer confidence**: medium · **priority**: B

### GlimpRouter: Collaborative Inference by Glimpsing One Token
- **id / track / file**: ACL2026 Findings pp.17850–17864 · LLM_Efficiency · `LLM_Efficiency/Zeng_Gu_GlimpRouter_...pdf`
- **surface domain**: route each reasoning step to a small or large model.
- **mechanism**: a cheap "glimpse" — the **entropy of just the first token** — predicts step difficulty and gates escalation to the expensive model.
- **analogy**: `cheap one-glimpse signal → escalate to expensive path only if uncertain`  ≅  our `cheap-candidate-then-expensive-check gate (K/H)`.
- **why it transfers / might NOT**: a very cheap gating heuristic (first-window seed density as our "glimpse"); but "first-token entropy" is LM-specific, so only the *pattern* transfers, not the signal.
- **transfer confidence**: medium · **priority**: B

### SCVQ: Sparse-Compensated Vector Quantization
- **id / track / file**: ACL2026 Main (Long) pp.8934–8950 · LLM_Efficiency · `LLM_Efficiency/Zhou_He_SCVQ_...pdf`
- **surface domain**: 2-bit weight quantization of LLMs via vector-quantized codebooks.
- **mechanism**: **salience-weighted K-means codebook** + a structured sparse matrix that unifies **outliers, salient entries, and residuals** to preserve fidelity under aggressive compression.
- **analogy**: their `PQ/residual-VQ codebook machinery with explicit outlier/residual handling`  ≅  our `product-quantized embedding codes for candidate generation (B)`.
- **why it transfers / might NOT**: reusable VQ engineering for compressing Track-3 embeddings (and the "keep rare/salient entries un-quantized" idea echoes preserving distinctive rare n-grams under DF-banding); but it's applied to *weights*, so the salience notion doesn't map one-to-one.
- **transfer confidence**: speculative · **priority**: B

### Modeling and Solving Stable Matching under Probabilistic Preferences
- **id / track / file**: ACL2026 Findings pp.37021–37033 · Language_Models · `Language_Models/Kong_Shen_Modeling_and_Solving_Stable_Matching_under_Probabilisti.pdf`
- **surface domain**: two-sided matching markets (dating/jobs) with stochastic human preferences.
- **mechanism**: **Expected Blocking Pairs** — a continuous relaxation of the classic blocking-pair notion — + a **hybrid Gale-Shapley** with probabilistic acceptance.
- **analogy**: their `stable matching under uncertain/soft preferences`  ≅  our `competitive span→work assignment (C) when match scores are uncertain`.
- **why it transfers / might NOT**: our shadowing/assignment could be cast as many-to-many matching of candidate spans to reference works with *soft* scores, and EBP gives a graded stability objective; but our problem is arguably closer to weighted interval scheduling / set cover (see A-card) than to two-sided markets.
- **transfer confidence**: speculative · **priority**: B

### BoundRL: Token-level Structured Text Segmentation via Reinforced Boundary Generation
- **id / track / file**: ACL2026 Findings pp.34706–34726 · Language_Models · `Language_Models/Li_Rangwala_BoundRL_...pdf`
- **surface domain**: segmenting structured text (code/placeholders) into semantic units.
- **mechanism**: emit **only boundary (start) tokens** and reconstruct segments by locating them in the source (−90% output); train boundaries with a reconstruction-fidelity + coherence reward; **perturb boundaries to create curriculum "stepping stones."**
- **analogy**: their `boundary-only representation + locate-in-source + boundary-perturbation search`  ≅  our `motif breakpoint detection (problem D)`.
- **why it transfers / might NOT**: a clean way to represent and optimize a segmentation by its cut-points against a reconstruction objective — matches how we'd emit motif boundaries; but it's an RL-with-verifiable-rewards setup needing a generator, heavier than our current statistical breakpoints.
- **transfer confidence**: medium · **priority**: B

### Repeated Sequences Reveal Gaps between LLMs and Natural Language
- **id / track / file**: ACL2026 Main (Long) pp.8367–8382 · Language_Models · `Language_Models/TanakaIshii_Repeated_Sequences_Reveal_Gaps_between_Large_Language_M.pdf`
- **surface domain**: diagnosing LLM text quality via long-range statistical structure.
- **mechanism**: model the **distribution of repeated subsequences across block-lengths** and its **higher-order Rényi-entropy growth** as a signature of how text reuses established structure.
- **analogy**: their `null model for how often / how long substrings repeat by chance`  ≅  our `calibration/threshold with no gold (problem H) — is a shared span longer than chance? which DF cap is principled?`.
- **why it transfers / might NOT**: gives a statistical background model for repeat-length significance, which is exactly what our density gates and per-domain DF policy approximate heuristically; but adapting entropy-scaling to a 16–20% CER corpus (where "exact repeat" is fuzzy) is non-trivial and unproven.
- **transfer confidence**: speculative · **priority**: B

### Robust Membership Inference under Adversarial Generative Corruption (MoMIA)
- **id / track / file**: ACL2026 Main (Long) pp.39531–39547 · Language_Models · `Language_Models/Huang_Qi_Robust_Membership_Inference_...pdf`
- **surface domain**: auditing whether text was in an LLM's training data, robust to AI-generated decoys.
- **mechanism**: a **mixture-of-experts committee of complementary detectors** (multiple MIA signals + AI-text detectors) that stays robust when *high-confidence-but-not-genuine* text mimics true members.
- **analogy**: their `committee of detectors to avoid being fooled by high-confidence look-alikes`  ≅  our `candidate scoring robust to formulaic/high-frequency shared text (B/H)` — the danger that common liturgical/formulaic phrasing masquerades as genuine reuse.
- **why it transfers / might NOT**: the *caution* (don't trust one confidence signal; formulaic content = false positive) plus the *remedy* (combine n-gram overlap + rarity + alignment quality in a small committee) is directly usable; but their detectors are LM-likelihood based, not corpus-matching.
- **transfer confidence**: medium · **priority**: B

### LeBoT: LLM-enabled Bag-of-Texts Representations for Short-Text Clustering
- **id / track / file**: ACL2026 Main (Long) pp.6432–6447 · Language_Models · `Language_Models/Lin_Verberne_LLMs_Enable_Bag_of_Texts_Representations_for_Short_Text.pdf`
- **surface domain**: label-free short-text clustering when you don't trust an embedder's distances.
- **mechanism**: convert **pairwise similarity judgments** into a "bag-of-texts" representation where items start **equidistant** (no assumed prior geometry), then cluster; scales cheaply.
- **analogy**: their `cluster directly from pairwise judgments, not from a suspect embedding metric`  ≅  our `motif-v2 / witness census clustering from a pairwise match-score graph (problem E)`.
- **why it transfers / might NOT**: we already have pairwise match scores between witnesses (not clean embeddings), which is exactly LeBoT's input assumption; but it's built for *short* texts and unknown cluster count — behavior at 62K-node graph scale is untested.
- **transfer confidence**: medium · **priority**: B

### InstructDiff: Domain-Adaptive Data Selection via Contrastive Entropy
- **id / track / file**: ACL2026 Main (Long) pp.10630–10648 · LLM_Efficiency · `LLM_Efficiency/Su_Chen_InstructDiff_...pdf`
- **surface domain**: selecting the most useful 10% of fine-tuning data.
- **mechanism**: score each sample by **contrastive entropy between a base and a lightly-tuned calibrated model**; a label-free difficulty/quality signal (direction is domain-adaptive).
- **analogy**: their `contrastive signal between two models as a label-free selection score`  ≅  our `contrastive learning from silver labels + hard-example mining for Track-3 (problem G)`.
- **why it transfers / might NOT**: a recipe to rank candidate training pairs by a two-model contrastive gap rather than by (unavailable) gold labels; but requires two aligned models and is designed for instruction data, not reuse pairs.
- **transfer confidence**: speculative · **priority**: B

---

## C / skipped — coverage honesty

**LLM_Efficiency (272): scanned ALL titles; abstract-checked ~19.** Whole classes
deliberately skipped as non-transferable (they operate on model internals, not on
symbol-stream matching / candidate generation):
- **Weight quantization** (~30: Fisher-guided, ternary/1.25-bit, NVFP4, sub-1-bit, W1AX, ACBQ, RoPE-spectral, etc.) — model compression, not our problem (VQ *machinery* captured via SCVQ).
- **LoRA / adapters / low-rank** (~30: SOS-LoRA, CoMoL, MoA, TLoRA, GraphLoRA, PRIME merging, …) — parameter-efficient fine-tuning.
- **Pruning** (~25: structured/layer/token/path/head, EOP, GRASPrune, LaCo, …) — model sparsification.
- **Reasoning-length / CoT-budget / early-exit / overthinking** (~55: O1-Pruner, ThinkBrake, SelfBudgeter, step-pruners, budget-guidance, …) — reasoning-token economy.
- **Reasoning distillation, RL/GRPO efficiency, PRM training** (~40) — training-time recipes.
- **KV-cache compression/eviction** (~20: ContrastKV, FastKV, OjaKV, HqeKV, MixKVQ, ReFreeKV, …) — attention memory; only LycheeCluster's metric-tree survived as transferable; a weak "eviction = importance-based selection" resemblance was rejected as resemblance-only.
- **MoE routing/load-balancing** (LayerMoE, RouteMoA, Alloc-MoE, LightMoE, Faster-MoE) — briefly considered for per-domain DF policy / committee routing but the mechanisms are LM-training-internal; DSMoE ("domain-specific experts exist, steer them, train-free") and Data-Mixing-Agent (RL-learned transferable domain re-weighting) are the closest to our per-domain policy but transfer is speculative — logged here, not carded.
- Bag of speculative-decoding papers beyond the 4 carded (UniSpec, Jakiro, SSSD, MARS, HCSpec, SpecExtend, Multi-Drafter, SPIDE, DeepPrune, DiffuSpec, LongSpec, Williams speculative-vocabulary, …) — same K mechanism; carded the most transferable representatives (adaptive-verification + retrieval-draft) rather than every instance.

**Language_Models (480): skimmed ALL titles; abstract-checked ~13** (the ones in
the assigned buckets with a plausible mechanism + the profile's called-out
B/C/D specials). Deliberately skipped without abstract-check (per instructions):
the large majority — pure **reasoning/CoT/RL, hallucination detection, safety/
backdoor/jailbreak/watermark-attack, prompting/persona, knowledge-editing,
uncertainty/calibration-of-answers, benchmarks, agents, recommendation, and
domain applications** (molecular/geo/legal/psych). In-bucket titles noted but not
carded (thin or duplicative transfer): data-mixture (LLMSurgeon, DynamixSFT,
Rethinking-Data-Mixing, DOSE, Instruction-Data-Selection-via-Answer-Divergence),
long-context (UltraLong 128K→4M = training recipe only; Sparse-Frontier = useful
sparse-attention *taxonomy/reference* but no algorithm for us; Rectified Sparse
Attention; Gated Working Memory), embeddings (GASE, Frozen-LLM-decoders,
Hierarchical-Token-Prepending), scale-eng architecture (Lizard linearization,
Polymorphic Universal Transformer, StateX, LEGO-2D). Detection/segmentation
specials judged **C or resemblance-only**: Gap-K% and CheckMIABench (membership
inference — right *framing* for B but the algorithms are model-likelihood-based,
not corpus-matching; CheckMIABench's "blind methods beat published ones" is a
worth-remembering methodological caution for evaluating our own detector without
a chance-level control); "Think in Sentences" (delimiter insertion — framing
only); Topic-Models survey (reference for problem E clustering); Reddy_Tanner
scripts/formats-on-numeracy (actionable only as a numeral-normalization note for
our digit-bidi/script handling); PCED context-of-experts and Tandem large+small
collaboration (committee framing, but generation-side, weak transfer).
