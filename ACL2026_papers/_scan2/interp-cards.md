# WAVE 2 — Interpretability & Analysis (168) + Explainability Special Theme (21)
Analogy hunt: mechanism over domain. Cards = A first, then B, then C/skipped.

Titles skimmed: 189 · Abstracts checked: 27 · A: 6 · B: 8

---

## A — adopt (transferable mechanism × surprise; not findable by domain keyword)

### SCOUT: Selective Coupling via Optimal Unbalanced Transport for Interpretable Text Classification
- **id / track / file**: ACL2026 main (Long) p6413–6431 · Interpretability · `Interpretability_and_Analysis/Jia_Wu_SCOUT_Selective_Coupling_via_Optimal_Unbalanced_Transpo.pdf`
- **surface domain**: interpretable prototype-based text classification.
- **mechanism**: represent each document as a discrete distribution over span embeddings; align spans to class prototypes with **differentiable Unbalanced Optimal Transport (UOT)** so that only discriminative fragments get matched and irrelevant background stays **unmatched via geometric mass suppression**.
- **analogy**: their `UOT span→prototype coupling that leaves noise unmatched` ≅ our **L (cross-representation span alignment, JA↔Hebrew / HTR↔clean-edition)** *and* **C (competitive assignment / span shadowing)** — UOT's "don't-transport-this-mass" is a soft, differentiable version of our shadowing where not every span must claim a partner.
- **why it transfers**: gives us a principled, differentiable objective for many-to-many noisy span alignment where large stretches legitimately match nothing (fragmentary witnesses, formulaic filler) — cleaner than forcing a full alignment, and the "unmatched mass" cost is exactly the knob shadowing tries to hand-tune.
- **why it might NOT**: UOT is O(n·m) per pair with an entropic solver; at 10^6-pair corpus scale we'd only afford it in the verification stage on already-banded candidates, not as the generator.
- **transfer confidence**: high
- **bib leads**: Sinkhorn / entropic UOT (Cuturi; Chizat et al. partial OT); prototype-based interpretable classification (ProtoPNet lineage).

### CLaS-Bench: A Cross-Lingual Alignment and Steering Benchmark
- **id / track / file**: Findings ACL2026 p21591–21628 · Interpretability · `Interpretability_and_Analysis/Gurgurov_Ostermann_CLaS_Bench_A_Cross_Lingual_Alignment_and_Steering_Bench.pdf`
- **surface domain**: benchmarking "language-forcing" — steering an LLM's internal reps toward a target language across 32 languages.
- **mechanism**: treats **language identity as a manipulable direction in activation space** and evaluates a whole menu of extraction methods for it: residual-stream **DiffMean**, probe-derived directions, **language-specific neurons**, **PCA/LDA vectors**, and **SAEs**.
- **analogy**: their `language-identity steering direction` ≅ our **I (strip language-identity / script / genre from Track-3 embeddings)** — this is the direct, deeper cousin of Wave-1's LangSAE, and it hands us a ranked comparison of *which* extractor best isolates the language factor (so we can then ablate/project it out).
- **why it transfers**: our corpus mixes Hebrew, Judeo-Arabic and Aramaic; before contrastive reuse-matching we want to remove the language axis so a Hebrew↔JA paraphrase isn't penalized as "different." CLaS gives an off-the-shelf recipe set (DiffMean + language neurons are cheap and training-light) plus an eval protocol.
- **why it might NOT**: they *force* a language (add the direction) at inference on generative LLMs; we want to *remove* it from a frozen encoder's pooled embedding — the inverse op, and removal selectivity is harder than injection.
- **transfer confidence**: high
- **bib leads**: DiffMean / activation steering (Turner et al., Rimsky et al.); language-specific neurons (Tang et al., Kojima et al.); LangSAE (Wave-1 find).

### Vocab Diet: Reshaping the Vocabulary of LLMs via Vector Arithmetic
- **id / track / file**: Findings ACL2026 p32334–32352 · Interpretability · `Interpretability_and_Analysis/Reif_Schwartz_Vocab_Diet_Reshaping_the_Vocabulary_of_LLMs_via_Vector.pdf`
- **surface domain**: shrinking LLM vocabularies by expressing morphological variants as offsets.
- **mechanism**: word-form variation (walk→walked, capitalization, inflection) is captured as a **single additive "transformation vector"** applied to a base-form embedding, in both input and output spaces; surface forms are *composed* from base + transform rather than stored separately (frozen backbone, tiny adapters).
- **analogy**: their `orthographic/morphological form = linear offset on a base embedding` ≅ our **I (strip the surface-form factor)** and our variant-robustness problem — plene/defective spelling, JA orthographic conventions, and matres-lectionis are exactly "surface-form variation" we want to *collapse* so two spellings of one word land at the same point.
- **why it transfers**: if a spelling axis is (approximately) a fixed linear offset, we can normalize variants by projecting it out before matching, or add it as a controlled augmentation for contrastive Track-3 negatives — lightweight, backbone-frozen, and they validate across 5 languages incl. Hebrew (HUJI authors).
- **why it might NOT**: Hebrew/JA orthographic variation is far less regular than English inflection (context-dependent, script-mixing, HTR-error-entangled) — a single global offset may not linearize it.
- **transfer confidence**: medium
- **bib leads**: linear representation / word-analogy arithmetic (Mikolov); output-embedding tying; morphology-as-direction probes.

### SemCSE-Multi: Multifaceted and Decodable Embeddings for Aspect-Specific Interpretable Domain Mapping
- **id / track / file**: ACL2026 main (Long) p40565–40586 · Interpretability · `Interpretability_and_Analysis/Brinner_Zarrie_SemCSE_Multi_Multifaceted_and_Decodable_Embeddings_for.pdf`
- **surface domain**: unsupervised multifaceted embeddings of scientific abstracts (invasion biology, medicine).
- **mechanism**: an **unsupervised** pipeline generates aspect-specific summarizing sentences, trains embeddings that place same-aspect summaries nearby so **distinct aspects occupy separable subspaces**, distills them into one model that emits multiple aspect-embeddings per doc in one pass, plus a **decoder from embedding back to natural language**.
- **analogy**: their `separable aspect subspaces + silver summaries as the contrastive signal` ≅ our **G (contrastive training from auto-generated/graded labels)** + **I (put content-reuse on a different axis than style/genre)** — Track-3 wants an embedding where "textual-reuse similarity" is one decodable facet independent of topic/style facets.
- **why it transfers**: it's a recipe for building the multi-facet Track-3 space *without* gold labels (auto-generated aspect summaries are our silver labels), and the decode-back gives us human-auditable justification for why two fragments were judged similar.
- **why it might NOT**: their aspects are semantic content facets of clean English abstracts; disentangling "reuse vs. genre" on 16–20%-CER Hebrew/JA HTR is a harder, noisier regime and the summary-generation step needs a capable Hebrew/JA LLM.
- **transfer confidence**: medium
- **bib leads**: SemCSE / SimCSE contrastive sentence embeddings; embedding inversion/decoding (vec2text, Morris et al.).

### From Isolation to Entanglement: When Do Interpretability Methods Identify and Disentangle Known Concepts?
- **id / track / file**: ACL2026 main (Long) p17188–17210 · Interpretability · `Interpretability_and_Analysis/Mueller_Reizinger_From_Isolation_to_Entanglement_When_Do_Interpretability.pdf`
- **surface domain**: auditing whether SAEs and probes actually disentangle concepts.
- **mechanism**: a **multi-concept evaluation** (sentiment, domain, voice, tense) that measures whether each concept is *independently manipulable* — finding that steering one feature usually perturbs several concepts, so correlational selectivity metrics and "separate subspaces" do NOT prove clean disentanglement.
- **analogy**: their `multi-concept steering-selectivity test` ≅ the evaluation harness for our **I (strip scribe-hand / language / script / genre)** — it is the exact question "if I ablate the scribe-hand direction, do I also damage the content signal?" turned into a protocol.
- **why it transfers**: it de-risks our Problem-I plan before we build it: adopt their multi-concept probe/steer eval to certify that removing one factor leaves reuse-similarity intact, and it warns us not to trust a single-concept selectivity number.
- **why it might NOT**: it's a diagnostic, not a removal method — it tells us how to *test* disentanglement, not how to *achieve* it; and their concepts are cleanly labeled, ours (scribe hand) are latent/graded.
- **transfer confidence**: medium-high
- **bib leads**: SAE feature evaluation (Bricken/Cunningham); causal disentanglement metrics; concept probing (Belinkov).

### Textual Steering Vectors Can Improve Visual Understanding in Multimodal LLMs
- **id / track / file**: ACL2026 main (Long) p40056–40087 · Explainability · `Explainability_Special_Theme/Gan_Neiswanger_Textual_Steering_Vectors_Can_Improve_Visual_Understandi.pdf`
- **surface domain**: steering multimodal LLMs using vectors derived from their text-only backbone.
- **mechanism**: a concept/steering direction extracted in **one representation space (text-only LLM) transfers and works in a related-but-different space (the multimodal model's visual reasoning)** — validated with SAE, Mean-Shift, and Linear Probing directions; cross-modal reuse of interpretability tools.
- **analogy**: their `direction learned in space A applied in aligned space B` ≅ our **L (cross-representation alignment)** + **I** — a "reuse" or concept direction learned in the *clean reference-edition* embedding space could be ported to the *noisy HTR* space, or a direction learned in Hebrew ported to Judeo-Arabic, without re-deriving it from scratch in the noisy regime.
- **why it transfers**: we have a clean side (8,300 reference editions) and a noisy side (HTR); if steering/normalization directions transfer clean→noisy, we can compute them where labels are trustworthy and apply them where they aren't.
- **why it might NOT**: their two spaces share a frozen backbone (same tokenizer/residual stream); our clean-vs-HTR or Hebrew-vs-JA spaces may not be linearly compatible enough for a raw direction to survive the transfer.
- **transfer confidence**: medium
- **bib leads**: Mean-Shift steering; cross-modal representation alignment; linear probing directions (Alain & Bengio).

---

## B — worth knowing (weaker/less-certain transfer, or useful framing)

### Constructing Interpretable Features from Compositional Neuron Groups (SNMF)
- **id / track / file**: ACL2026 main (Long) p42326–42348 · Interpretability · `Interpretability_and_Analysis/Shafran_Geva_Constructing_Interpretable_Features_from_Compositional.pdf`
- **surface domain**: finding interpretable feature directions without SAEs.
- **mechanism**: decompose MLP activations with **semi-nonnegative matrix factorization (SNMF)** into sparse linear combos of co-activated neurons, each mapped back to its activating inputs → directly interpretable, causally better than SAEs.
- **analogy**: their `SNMF concept-direction extraction` ≅ our **I** — a cheaper, label-free alternative to SAEs for isolating a scribe-hand/language direction to project out.
- **why it transfers / might NOT**: SNMF is lightweight and interpretable-by-construction, a plausible drop-in for our disentanglement layer; but it's demonstrated on English LLM internals, not on pooled document-embedding spaces like Track-3's. Confidence: medium.
- **bib leads**: semi-NMF (Ding et al.); dictionary learning vs SAE.

### Similarity-Distance-Magnitude (SDM) Activations
- **id / track / file**: Findings ACL2026 p22037–22057 · Explainability · `Explainability_Special_Theme/Schmaltz_Similarity_Distance_Magnitude_Activations.pdf`
- **surface domain**: a calibrated, interpretable replacement for softmax for selective classification.
- **mechanism**: augments the decision score with **SIMILARITY (depth-matches into training)** and **DISTANCE-to-training-distribution** awareness; an SDM estimator partitions class-wise empirical CDFs to **control prediction-conditional accuracy under abstention**, robust to covariate shift / OOD, with interpretability-by-exemplar via dense matching.
- **analogy**: their `selective-prediction rule from distance-to-known + similarity, no task labels` ≅ our **H (which matches to trust with no gold)** + **J (abstain on uncertain)** — a principled version of our density gates / confidence labels.
- **why it transfers / might NOT**: gives a calibrated "trust this match / abstain" estimator grounded in nearest-exemplar distance — exactly our reference-edition matching setting; but it's a classifier final-layer method, so we'd adapt the CDF-partition idea to our pair-score distribution rather than adopt the layer. Confidence: medium.
- **bib leads**: conformal prediction; selective classification (Geifman & El-Yaniv); deep kNN calibration.

### Embracing Anisotropy: Turning Massive Activations into Interpretable Control Knobs
- **id / track / file**: ACL2026 main (Long) p29930–29956 · Interpretability · `Interpretability_and_Analysis/Roh_Kim_Embracing_Anisotropy_Turning_Massive_Activations_into_I.pdf`
- **surface domain**: massive-activation dimensions as domain detectors + steering knobs.
- **mechanism**: a **training-free magnitude criterion** identifies "Domain-Critical Dimensions" that act as detectors for symbolic/quantitative/domain-specific patterns; steering *only* those dimensions beats whole-vector steering.
- **analogy**: their `few outlier dims carry domain identity` ≅ our **I** — if a handful of embedding dims encode script/genre, neutralize just those instead of a full projection.
- **why it transfers / might NOT**: training-free and surgical, appealing for our disentanglement; but "massive activations" are an LLM residual-stream artifact and may not appear in a purpose-trained contrastive Track-3 encoder. Confidence: medium. Also relevant to embedding-anisotropy design for contrastive training.
- **bib leads**: massive activations (Sun et al.); anisotropy of embeddings (Ethayarajh; Rudman IsoScore).

### Context Attribution with Multi-Armed Bandit Optimization
- **id / track / file**: Findings ACL2026 p11651–11662 · Interpretability · `Interpretability_and_Analysis/Pan_Chawla_Context_Attribution_with_Multi_Armed_Bandit_Optimizatio.pdf`
- **surface domain**: which retrieved-context segments support a RAG answer.
- **mechanism**: casts segment attribution as a **combinatorial multi-armed bandit** solved with **Linear Thompson Sampling** — adaptively samples the most-informative candidate subsets to cut expensive model queries ~30% vs uniform (SHAP-style) sampling.
- **analogy**: their `adaptive candidate sampling to minimize expensive checks` ≅ our **K (cheap-candidate → expensive-verify)** + **J** — when Track-3 semantic verification or an LLM-judge step is the costly stage, prioritize the most-promising banded candidates by posterior rather than checking uniformly.
- **why it transfers / might NOT**: a clean way to budget expensive verification over a candidate pool; but our current banded-Levenshtein verify is already cheap — payoff only in the semantic/LLM-judge tier. Confidence: medium.
- **bib leads**: Linear Thompson Sampling (Agrawal & Goyal); combinatorial bandits; SHAP as baseline.

### De-Anonymization at Scale via Tournament-Style Attribution
- **id / track / file**: ACL2026 main (Long) p32270–32283 · Interpretability · `Interpretability_and_Analysis/Zhang_De_Anonymization_at_Scale_via_Tournament_Style_Attribut.pdf`
- **surface domain**: linking anonymous documents to authors among tens of thousands of candidates.
- **mechanism**: **dense-retrieval prefilter** to shrink the pool → **tournament/sequential elimination** (LLM repeatedly picks the most-same-author survivor) → **majority-vote aggregation** over independent runs for a robust top-k.
- **analogy**: their `prefilter → tournament → vote` for same-author-among-10^4 ≅ our **J/B/K** — finding which reference edition / which witness a fragment belongs to among thousands, and "same author" ≅ "same work/scribe."
- **why it transfers / might NOT**: the prefilter+elimination+vote scaffold maps onto candidate witness ranking; but the elimination core is LLM-prompting per comparison — too costly at our 10^6 scale except as a final re-ranker on a tiny survivor set. Confidence: medium.
- **bib leads**: authorship attribution / verification; dense-retrieval reranking; self-consistency voting.

### Establishing a Scale for Kullback–Leibler Divergence in Language Models
- **id / track / file**: Findings ACL2026 p23223–23248 · Interpretability · `Interpretability_and_Analysis/Kishino_Shimodaira_Establishing_a_Scale_for_Kullback_Leibler_Divergence_in.pdf`
- **surface domain**: making KL divergence between LMs interpretable by fixing a consistent scale across settings.
- **mechanism**: builds a **reference scale for a divergence metric** from many controlled settings (size, seed, quantization, layer) so a raw KL value maps to "how big is this really," with no task labels.
- **analogy**: their `calibrate a raw divergence into an interpretable scale via reference distributions` ≅ our **H** — turning a raw pair-similarity/edit-distance score into a calibrated match-confidence without gold, using reference (e.g. random-pair / same-work) score distributions.
- **why it transfers / might NOT**: the methodology of grounding a threshold in reference-setting distributions is directly reusable for our density gates; but KL-between-distributions ≠ our edit/embedding distance, so only the calibration *idea* transfers. Confidence: medium.
- **bib leads**: log-likelihood-vector model comparison; empirical-null calibration (Efron).

### Reasoning-Based Refinement of Unsupervised Text Clusters with LLMs
- **id / track / file**: Findings ACL2026 p9899–9917 · Interpretability · `Interpretability_and_Analysis/Islam_Reasoning_Based_Refinement_of_Unsupervised_Text_Cluster.pdf`
- **surface domain**: post-hoc validation/cleanup of clustering output using LLMs as judges.
- **mechanism**: three decoupled stages over *any* clustering result — **coherence verification**, **redundancy adjudication** (merge/reject overlapping clusters), and **label grounding** — separating representation from structural validation.
- **analogy**: their `redundancy adjudication + coherence check` ≅ our **E (motif-v2 witness census clustering)** + **C (merge overlapping = shadowing-adjacent)** — an LLM pass to merge near-duplicate motif clusters and drop incoherent ones after graph clustering.
- **why it transfers / might NOT**: a plug-in cleanup layer for our motif census that's agnostic to the clustering algorithm; but LLM-as-judge over 16–20%-CER Hebrew/JA is unreliable and would need careful Hebrew-capable prompting. Confidence: medium.
- **bib leads**: LLM-as-judge; cluster validity without labels; overlapping/correlation clustering.

### Locate, Steer, and Improve: A Practical Survey of Actionable Mechanistic Interpretability
- **id / track / file**: Findings ACL2026 p10317–10362 · Explainability · `Explainability_Special_Theme/Zhang_Wong_Locate_Steer_and_Improve_A_Practical_Survey_of_Actionab.pdf`
- **surface domain**: survey organizing mechanistic-interpretability into a Locate→Steer→Improve action pipeline.
- **mechanism**: a taxonomy of concrete *intervention* methods (locate features/circuits, steer them, measure improvement) rather than observation-only MI.
- **analogy**: not a mechanism itself — a **map of the concept-removal/steering toolbox** for our **I**; the single best index to mine for candidate methods (which locate + which steer) before we pick one.
- **why it transfers / might NOT**: high bib value for planning Problem I; but it's a survey, no runnable artifact. Confidence: high (as a reference), n/a as a method.
- **bib leads**: use its Locate + Steer sections as a method menu for scribe-hand/language removal.

---

## C / skipped (no real transfer — coverage note)

Deliberately skipped whole classes as on-domain or non-isomorphic:
- **Uncertainty/calibration-of-LLM-confidence papers** (~15+: Chen×2, Mao, Srey, Zhang_Collier, Zhang_Low, Zhang_Vlachos LoVeC, Zhou_Cheng, Yeom EpiCaR, SanzGuerrero, Ren, Sun_Augenstein, Yaldiz, Li_Huai): all address model self-confidence, not no-gold *match* thresholding — only Schmaltz (SDM) and Kishino (KL scale) carry a transferable calibration mechanism; the rest are C for us.
- **Hallucination / faithfulness / fact-check mechanism papers** (Chang_Zhuang AHEAD, Lee_Yoo, Li_Yu, Luo ART, Alon Faithful-Serum, Kong REFLEX, Sun_Augenstein, Rudman VLM-hallucination): interpretability of generation errors — no reuse-detection transfer.
- **Reasoning / chain-of-thought / grokking mechanism papers** (Zhao Grok, He_Chen Grokking, Hu, Chang_Niu latent reasoning, Zaman CoT, dAliberti Illusion, Shi_Xiong RL-generalize, Zhang entropy-CoT, Sun_Rajmohan trajectories, Singh reasoning-tokens): mechanistic-reasoning, not our problems.
- **Neuron/head-localization + model-editing/pruning** (Huang_Lin role neurons, Zhang_Wu preference heads, Aljaafari role circuits, Zheng_Tian DPN-LE, Zheng_Li emotion neurons, He_Xiong TENP prune, Ji OCP prune, Pei FFN→MoE, Gu editing, Liu_Cai SpecEdit, Tang_Wu knowledge-edit): CLARE (entanglement graph, `Baser_Gurusamy…`) is the closest to our **I/E** but stays speculative (fact-editing ripple, not embedding disentanglement) → B-resemblance-only, not carded.
- **Attention-sink / positional-bias / info-flow** (RanMilo sinks, Rulli diffusion sinks, Dai attention-floating, Kaplan Follow-the-Flow, Ok_Lee prompt-order): flagged in profile for long-doc pooling; RanMilo + Kaplan give a *caveat* (position/first-token dominance can swamp mean-pooled long-doc embeddings; concentrate pooling on informative tokens) but no runnable tool → resemblance-only, noted not carded.
- **Copying/retrieval/induction-head circuits** (Lin Retrieval-Heads-are-Dynamic, Hochman factual-retrieval): conceptually the "copy" mechanism ≅ text-reuse, but purely observational on LLM internals — no transfer to our matcher. Resemblance-only.
- **Memorization / membership-inference** (Nishida non-verbatim, Udsa cross-client, Cohen REMIND, Zhu reversal): membership-inference is profile-kin to **B**, but these measure *model* memorization, not corpus text-reuse; REMIND's embedding-proximity variant-generation is a faint methodological echo only.
- **SAE/steering variants not carded** (Fang_Feng, Yao_Du AdaptiveK, Dang_Ngo Selective-Steering [norm-preserving — useful safety detail for our ablation, B-adjacent], Li_Kasneci steering-pitfalls, Sastre Concept-Tokens, Bigoulaeva instruction-vectors, Saiyed SAE-robustness, Dong NeuReasoner): same family as the carded CLaS-Bench/Mueller/SNMF but weaker/narrower; Dang_Ngo's norm-preserving rotation is worth remembering when we ablate a direction without collapsing the embedding.
- **Cross-modal / vision-language / speech / molecule / quantum / graph-domain** (Deguchi CLIP, Maliha diffusion, Yang viewpoint, Li_Hsu eyesight, Stafford speech-phonology, Pu MCLE-Mol, Li_Yin quantum, Baghershahi GNN, Gan already carded): domain-specific; only Gan's cross-modal *direction transfer* carried a Problem-L mechanism.
- **Lineage / phylogeny / stemma (Problem F — our biggest gap): NO hits.** Bayazit "Crosscoding Through Time" tracks training-time feature emergence, not manuscript ancestry; nothing in these 189 reconstructs a copy-tree. Honest null result — F remains unaddressed by this track.
