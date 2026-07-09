# WAVE 2 — Cross-Domain Analogy Cards (widest serendipity net)

Scope: Financial/TimeSeries, Computational Social Science, Discourse/Pragmatics,
Sentiment/Style/Argumentation, NLG, Human-AI Interaction, NLP Applications, Demos
(+ strict skim of Workshops: GEM, NLP-CSS, MELLM, SurgeLLM). Posters ignored.

Problem letters: A noisy alignment · B all-pairs near-dup · C competitive assignment ·
D change-point/segmentation · E graph clustering · F lineage/tree · G contrastive-from-graded ·
H calibration-without-gold · I concept removal/disentangle · J rare-event · K draft-then-verify ·
L cross-representation alignment.

Counts: ~562 titles skimmed · 43 abstracts checked · 7 A · 18 B.

---

## A — adopt (surprise × usefulness; a mechanism we'd not have found by domain keyword)

### TRUST: Robust Social Bot Detection via Uncertainty-Guided Pseudo-Labeling and Graph Structure Purification
- **id / track / file**: ACL-Findings 2026 · Computational_Social_Science · `Tracks/Computational_Social_Science/Xu_Cheng_TRUST_Towards_Robust_Social_Bot_Detection_via_Uncertain.pdf`
- **surface domain**: detecting bot accounts in a social-network graph where bots forge edges to real users.
- **mechanism**: (1) evidential deep learning → calibrated per-node uncertainty; (2) pseudo-label only LOW-uncertainty nodes via a dynamic threshold; (3) **graph structure purification** — selectively DELETE "heterophilous" edges (links between nodes that are probably different classes) that violate the homophily assumption.
- **analogy**: their `purge forged bot→human edges + propagate labels from confident nodes` ≅ our `F/E — build a clean witness/transmission graph by deleting spurious candidate-match edges between unrelated fragments, then bootstrap attribution labels from high-confidence witnesses`.
- **why it transfers**: our candidate-match graph is exactly a noisy similarity graph riddled with heterophilous edges (coincidental phrase overlaps linking unrelated works). Edge-purification-by-label-disagreement + uncertainty-gated label propagation is a concrete recipe for the stemma/witness-graph layer we do NOT yet have, and the evidential-uncertainty part gives calibration without gold (H).
- **why it might NOT**: their edge/node labels come from a supervised GNN with real class labels; we'd have to substitute our reuse-score as the linking signal and define "same-work" homophily, which is fuzzier than human/bot.
- **transfer confidence**: medium
- **priority**: A
- **bib leads**: evidential deep learning for uncertainty (Sensoy et al.); graph structure learning / edge pruning under heterophily.

### Wait! There's a Way Out: A Decision Mechanism for Forecasting Conversational Derailment
- **id / track / file**: ACL 2026 Long · Computational_Social_Science · `Tracks/Computational_Social_Science/Kim_DanescuNiculescuMizil_Wait_There_s_a_Way_Out_A_Decision_Mechanism_for_Forecas.pdf`
- **surface domain**: online moderation — deciding, utterance by utterance, whether to raise an alert that a conversation will turn into a personal attack.
- **mechanism**: **decouple the decision to ACT from the likelihood estimate.** Instead of triggering whenever P(bad) is high, run forward-looking simulations to check whether the situation still admits plausible "recovery" paths, and DEFER the decision when it does — slashing false positives at equal accuracy.
- **analogy**: their `defer the trigger while recovery is still plausible` ≅ our `C/K — defer committing a span match/assignment while a plausibly-better-covering span could still emerge as the alignment extends`.
- **why it transfers**: our threshold gates (H) and competitive span shadowing (C) currently commit on a static score. A deferral layer — "don't finalize this match/shadow decision yet; simulate whether extending the seed resolves the ambiguity" — is a principled false-positive reducer for the two-stage matcher and for shadowing overlap resolution.
- **why it might NOT**: "forward simulation of recovery" is cheap for conversations (roll the LM forward); for us the analog is re-running banded extension, which may not be cheap enough to do speculatively on every candidate.
- **transfer confidence**: medium
- **priority**: A
- **bib leads**: Chang & Danescu-Niculescu-Mizil (conversational forecasting / CRAFT); cost-aware decision theory for online triggers.

### ODASim: Ordered, Distinctive and Absolute Semantic Similarity for Code Explanation Evaluation
- **id / track / file**: ACL-Findings 2026 · NLP_Applications · `Tracks/NLP_Applications/Kumar_Tamilselvam_ODASim_Ordered_Distinctive_and_Absolute_Semantic_Simila.pdf`
- **surface domain**: judging whether an LLM's natural-language explanation of a code snippet is correct, where embedding cosine is poorly calibrated.
- **mechanism**: a **graded fine-tuning framework for embedding models** that learns a similarity that is Ordered (monotone in quality), Distinctive, and Absolute (calibrated), trained on **graded labels synthesized by strategic PERTURBATIONS of gold** — cuts Expected Calibration Error ~85%.
- **analogy**: their `graded labels from perturbing gold + calibrated ordered similarity` ≅ our `G+H — Track-3 embeddings trained on ordinal reuse tiers (verbatim → variant → paraphrase → motif) whose cosine must be MONOTONE in reuse strength and ABSOLUTELY calibrated so a fixed threshold means the same thing corpus-wide`.
- **why it transfers**: we already manufacture graded reuse strength (banded-Levenshtein score, shared-phrase count → Tier A/B/C). ODASim is a ready blueprint for turning those into an embedding-training signal and, crucially, for making the resulting similarity calibrated so density gates don't need per-query retuning.
- **why it might NOT**: their perturbations are semantic edits to English explanations; our "perturbations" (orthographic variants, HTR noise, JA↔Hebrew) are noisier and may not produce cleanly ordered grades.
- **transfer confidence**: medium-high
- **priority**: A
- **bib leads**: Expected Calibration Error for similarity; graded/ordinal contrastive (e.g. CoSENT, ranking losses); CodeXGLUE.

### Uncovering Sentiment Analysis Circuit in Large Language Models (SAE + circuit intervention)
- **id / track / file**: ACL 2026 Long · Sentiment_Style_Argumentation · `Tracks/Sentiment_Style_Argumentation/Li_Uncovering_Sentiment_Analysis_Circuit_in_Large_Language.pdf`
- **surface domain**: why LLM sentiment predictions are fragile to prompt phrasing; mechanistic interpretability of the "sentiment" computation.
- **mechanism**: use **Sparse Autoencoders** to extract interpretable features from activations, isolate the causal "sentiment circuit," then do a **training-free inference-time intervention that AMPLIFIES those features** to fix under-activation.
- **analogy**: their `identify a concept's SAE features and steer them` ≅ our `I — identify the SAE features that encode a NUISANCE factor (scribe hand, language identity JA-vs-Hebrew, script/genre) and SUPPRESS them so reuse embeddings compare works, not scribes`.
- **why it transfers**: the profile's stated I-target is literally "SAE concept erasure." This is a concrete, training-free recipe to disentangle language/scribe/script from Track-3 embeddings by negating (rather than amplifying) the offending features — no adversarial retraining needed.
- **why it might NOT**: SAEs must be trained on OUR embedding/model, and it's unproven that "scribe hand" or "JA-ness" is linearly and sparsely encoded the way sentiment is; could be entangled.
- **transfer confidence**: speculative
- **priority**: A
- **bib leads**: sparse autoencoders for feature dictionaries (Anthropic/Cunningham); activation patching; concept erasure (LEACE, RLACE).

### GovScape: A Public Multimodal Search System for 70 Million Pages of Government PDFs
- **id / track / file**: ACL 2026 Demo · Demos · `Tracks/Demos/Huang_Lee_GovScape_A_Public_Multimodal_Search_System_for_70_Milli.pdf`
- **surface domain**: a public search portal over 10M archived US-government PDFs (70M pages) for cultural-heritage/web-archive access.
- **mechanism**: a hybrid corpus-exploration stack — (1) metadata facets, (2) exact text, (3) semantic (vector) text search, (4) **visual search** over page images ("redacted documents", "pie charts") — with an embedding pipeline that cost ~$1,500 for 10M PDFs (~47k pages/$).
- **analogy**: their `exact + semantic + facet + VISUAL search over 70M heritage pages at trivial cost` ≅ our `product — GenizahSearch's 948K-page corpus, plus the missing VISUAL-search affordance (find pages by layout/marginalia/hand/decoration, not just transcription text)`.
- **why it transfers**: it's almost our exact product at 70× the scale, and open-source. The demonstrated cost curve de-risks adding vector + visual page-image search alongside our Tantivy text index; "find pages that look like X" is a genuinely new user affordance for manuscript research.
- **why it might NOT**: this is a product/UX blueprint, not a SEED-029 matching algorithm; visual search over degraded manuscript images is harder than over clean government PDFs.
- **transfer confidence**: high (as a product blueprint)
- **priority**: A
- **bib leads**: open-source codebase (govscape.net); page-image embedding for visual search (CLIP / ColPali-style page retrieval).

### MEIC-DT: Memory-Efficient Incremental Clustering for Long-Text Coreference with Dual-Threshold Constraints
- **id / track / file**: ACL-Findings 2026 · Discourse_Pragmatics · `Tracks/Discourse_Pragmatics/Luo_Sun_MEIC_DT_Memory_Efficient_Incremental_Clustering_for_Lon.pdf`
- **surface domain**: clustering coreferent mentions across very long documents under a hard memory budget.
- **mechanism**: dual-threshold **incremental (streaming) clustering** with a Statistics-Aware Eviction Strategy (cache management keyed on train/inference statistical profiles) and an Internal Regularization Policy that **condenses each cluster to its most representative members** — competitive accuracy inside a fixed memory ceiling.
- **analogy**: their `stream mentions into clusters, evict, keep representatives, never hold all pairs` ≅ our `E + disk-spill — cluster 62K+ witnesses/motifs into works incrementally without an O(n²) in-RAM pair matrix`.
- **why it transfers**: our two-pass disk-spill engine already fights the memory wall; MEIC-DT is a principled streaming-clustering discipline (eviction + representative condensation + dual thresholds) that maps onto online motif/witness clustering, replacing ad-hoc spill heuristics with a bounded-memory clustering algorithm.
- **why it might NOT**: their linking score is a trained coreference model; we'd swap in our reuse similarity, and the eviction statistics are tuned to mention streams, not manuscript-span streams.
- **transfer confidence**: medium
- **priority**: A
- **bib leads**: incremental/online coreference (Xia et al.; Toshniwal et al. streaming coref); bounded-memory clustering.

### Incorporating Temporal Coherence to Cross-Document Event Coreference (CohTP)
- **id / track / file**: ACL 2026 Long · Discourse_Pragmatics · `Tracks/Discourse_Pragmatics/Chen_Zhu_Incorporating_Temporal_Coherence_to_Cross_Document_Even.pdf`
- **surface domain**: clustering mentions of the same real-world EVENT scattered across many documents (cross-document event coreference).
- **mechanism**: build an event graph via NLI, refine it with an Edge-Aware GNN to resolve conflicts, **PARTITION into coherent (temporal) segments FIRST, then resolve coreference only WITHIN a segment** — a blocking + graph-conflict-resolution pipeline, with temporal constraints as the partitioner.
- **analogy**: their `segment-then-resolve blocking + graph conflict resolution across documents` ≅ our `E/F — link witnesses of ONE work across manuscripts: block by cheap signal, then resolve within-block, using paleographic/dating constraints as the partitioner instead of event time`.
- **why it transfers**: this is the canonical "cross-doc coref → witness linking" isomorphism the profile calls out, delivered as an actual pipeline. The temporal-partition idea directly suggests using manuscript dating/provenance as a hard constraint to prune impossible witness links (a lineage-consistency prior, F).
- **why it might NOT**: least-surprising card here (the analogy is expected), and the GNN + NLI components need training data (mention/event pairs) we don't have for manuscripts.
- **transfer confidence**: medium
- **priority**: A
- **bib leads**: cross-document event coreference (ECB+, Cattan et al.); temporal event graphs; blocking for entity resolution.

---

## B — worth knowing (plausible but weaker/less-certain transfer, or a useful framing)

### Are LLM Benchmarks Already Contaminated? A Systematic Review of Contamination Detection Methods
- **id / track / file**: GEM Workshop 2026 · Workshops/GEM · `Workshops/GEM/Nourbakhsh_Slavin_Are_LLM_Benchmarks_Already_Contaminated_A_Systematic_Re.pdf`
- **surface domain**: survey of methods to detect whether benchmark test data leaked into LLM training sets.
- **mechanism**: a **four-tier contamination taxonomy (Exact / Syntactic / Semantic / Task-Level)** and a comparative map of **five detection families** (string-matching, likelihood-based, membership-inference, LLM-prompted, benchmark-auditing) with access assumptions and failure modes.
- **analogy**: their `contamination = test text reused in training` ≅ our `B — text-reuse detection`; the T1–T4 tiers map onto our reuse tiers (verbatim → orthographic variant → paraphrase → thematic motif).
- **why it transfers**: it's a ready-made literature map of our entire SEED-029 problem under a different name — a bib-lead goldmine and a validated 4-tier framing that mirrors ours; a place to shop for a candidate-generation algorithm that beats DF-banding.
- **why it might NOT**: it's a survey, not a runnable mechanism; contamination methods assume LLM logits/access we don't use.
- **transfer confidence**: medium (framing + leads)
- **priority**: B
- **bib leads**: chase the "string-matching" and "semantic" detection families for corpus-scale near-dup algorithms; Ravaut et al. 2025 survey.

### Text Embedding as Treatment: A Meta-Causal Approach for Robust Sentiment Classification
- **id / track / file**: ACL-Findings 2026 · Sentiment_Style_Argumentation · `Tracks/Sentiment_Style_Argumentation/Cheng_Zhang_Text_Embedding_as_Treatment_A_Meta_Causal_Approach_for.pdf`
- **surface domain**: making sentiment classifiers robust by keeping causally-predictive words and dropping spurious correlates.
- **mechanism**: estimate each word's **causal treatment effect** on the label (via context-clustering + per-cluster effect estimation, generalizing to novel/low-freq words) and down-weight low-effect (spurious) tokens.
- **analogy**: their `causal down-weighting of non-causal words` ≅ our `I / distinctiveness weighting — separate the tokens that carry the shared-TEXT signal from scribal idiolect and frequency artifacts that produce spurious matches`.
- **why it transfers**: a principled successor to our DF-capping: instead of "cap by document frequency," weight n-grams by their estimated causal contribution to a true-reuse verdict — could reduce false positives from common formulaic phrasing.
- **why it might NOT**: "treatment effect" needs a label to regress against; our reuse verdicts are themselves noisy/self-generated, risking circularity.
- **transfer confidence**: speculative
- **priority**: B
- **bib leads**: causal feature attribution / invariant risk minimization; TextCause (Pryzant et al.).

### Uncertainty-Calibrated Elastic Alignment for Multimodal Sentiment (EASE)
- **id / track / file**: ACL-Findings 2026 · Sentiment_Style_Argumentation · `Tracks/Sentiment_Style_Argumentation/He_Ji_Uncertainty_Calibrated_Elastic_Alignment_for_Multimodal.pdf`
- **surface domain**: sentiment analysis when some modalities are missing; avoid over-fitting the imputed/ambiguous regions.
- **mechanism**: estimate per-region uncertainty, then **elastically relax alignment constraints where uncertainty is high** and tighten them where confident.
- **analogy**: their `uncertainty-adaptive alignment tolerance` ≅ our `A — make the banded-Levenshtein band width a function of local per-character HTR confidence: widen the band in high-error regions, tighten in clean ones`.
- **why it transfers**: we already have per-character HTR confidence; a fixed band is wasteful (too tight in noisy runs, too loose in clean ones). "Elastic band driven by local uncertainty" is a clean, implementable upgrade to seed-and-extend.
- **why it might NOT**: their elasticity lives in learned embedding space with a trained uncertainty head; ours would be a hand-built rule over edit-distance, so it's inspiration, not a drop-in.
- **transfer confidence**: medium
- **priority**: B
- **bib leads**: evidential/uncertainty-aware alignment; adaptive-band DTW.

### Speculative Refinement: Hybrid Autoregressive-Diffusion Decoding (SpecRef)
- **id / track / file**: GEM Workshop 2026 · Workshops/GEM · `Workshops/GEM/Gupta_Kumar_Speculative_Refinement_A_Hybrid_Autoregressive_Diffusio.pdf`
- **surface domain**: warm-starting a diffusion LM from an autoregressive draft for faster code/text generation.
- **mechanism**: **entropy-guided selective masking** — only re-decide the high-entropy (uncertain) positions of a draft; freeze confident ones. Also documents a "refinement tension": multi-stage correction DEGRADES already-correct tokens.
- **analogy**: their `refine only uncertain positions` ≅ our `K — lacuna/HTR-error restoration self-refine: re-predict only low-confidence characters, freeze confident spans`; the refinement-tension warning ≅ our self-refine risk of corrupting good reconstructions.
- **why it transfers**: gives a concrete gating rule (entropy threshold) for WHERE to self-refine and an empirical caution that over-refinement hurts — directly applicable to our restoration loop.
- **why it might NOT**: it's a short eval-focused paper; the mechanism is demonstrated for code, and "entropy" for our restorer would need a calibrated confidence we may lack.
- **transfer confidence**: medium
- **priority**: B
- **bib leads**: speculative decoding (Leviathan et al.); masked-diffusion LMs.

### EvolvR: Self-Evolving Pairwise Reasoning for Story Evaluation
- **id / track / file**: ACL 2026 Long · NLG · `Tracks/Natural_Language_Generation/Wang_Xiao_EvolvR_Self_Evolving_Pairwise_Reasoning_for_Story_Evalu.pdf`
- **surface domain**: building a reliable LLM judge for open-ended story quality (where absolute scoring is unstable).
- **mechanism**: ground judgment in **PAIRWISE comparison** (a judge hesitates on an absolute score but is precise choosing between two), then self-synthesize + self-filter CoT training data.
- **analogy**: their `relative pairwise judgment beats absolute scoring` ≅ our `H/C — rank candidate matches/witnesses pairwise ("which is the better parallel/span") instead of assigning absolute confidences to threshold`.
- **why it transfers**: our density gates are absolute-threshold. A pairwise-ranking layer for confidence (and for competitive shadowing's "which span wins") is more robust without gold and matches the shadowing decision natively.
- **why it might NOT**: pairwise scales O(n²) in candidates; needs careful tournament design at corpus scale.
- **transfer confidence**: medium
- **priority**: B
- **bib leads**: LLM-as-judge pairwise reliability; Bradley-Terry ranking from comparisons.

### Text-to-Distribution Prediction with Quantile Tokens and Neighbor Context
- **id / track / file**: ACL 2026 Long · NLP_Applications · `Tracks/NLP_Applications/Zhu_Malmasi_Text_to_Distribution_Prediction_with_Quantile_Tokens_an.pdf`
- **surface domain**: predicting a full conditional distribution (not a point) from text.
- **mechanism**: retrieve semantically-similar **neighbor instances and use their EMPIRICAL outcome distribution to ground/calibrate** the current prediction (plus dedicated quantile tokens).
- **analogy**: their `neighbor-grounded empirical calibration` ≅ our `H — calibrate a match's confidence from the empirical verify-outcomes of its nearest-neighbor candidate pairs (kNN-conformal style)`.
- **why it transfers**: a no-gold way to attach a calibrated interval to each match: "pairs that looked like this one were confirmed X% of the time." Complements conformal prediction with local empirical grounding.
- **why it might NOT**: quantile-token architecture is LLM-regression-specific; only the neighbor-empirical idea ports, and it needs a labeled neighbor pool.
- **transfer confidence**: medium
- **priority**: B
- **bib leads**: conformal prediction; kNN-conformal / retrieval-augmented calibration.

### Capturing Epistemic Uncertainty in LLM-Based Soft Labeling
- **id / track / file**: GEM Workshop 2026 · Workshops/GEM · `Workshops/GEM/Jiang_Liang_Capturing_Epistemic_Uncertainty_in_LLM_Based_Soft_Label.pdf`
- **surface domain**: generating soft (distributional) labels for subjective tasks from LLMs.
- **mechanism**: treat annotator disagreement as **epistemic uncertainty, not noise**; produce soft labels via **model-ensemble variation** (most informative) rather than majority-vote.
- **analogy**: their `ensemble-variation soft labels` ≅ our `G/H — bootstrap graded/silver reuse labels for Track-3 by ensembling detectors; a pair only SOME methods flag is genuinely ambiguous reuse, not an error to discard`.
- **why it transfers**: tells us to keep and model disagreement among our detectors (seed-and-extend vs embedding vs reference-matcher) as a calibrated uncertainty signal for training, instead of collapsing to a hard verdict.
- **why it might NOT**: our "annotators" are algorithms with correlated failure modes, so their disagreement may under-represent true uncertainty.
- **transfer confidence**: medium
- **priority**: B
- **bib leads**: learning with disagreement (Plank; Uma et al.); soft-label distillation.

### HieroSA: Stroke-Level Structural Analysis of Hieroglyphic Scripts without Language-Specific Priors
- **id / track / file**: ACL-Findings 2026 · Computational_Social_Science · `Tracks/Computational_Social_Science/Luo_Liu_Enabling_Stroke_Level_Structural_Analysis_of_Hieroglyph.pdf`
- **surface domain**: deriving stroke-level structure of logographic characters (Chinese, Egyptian) from bitmaps.
- **mechanism**: convert a character IMAGE into explicit, interpretable **line-segment features in a normalized coordinate space**, no handcrafted per-script data → cross-lingual generalization.
- **analogy**: their `glyph bitmap → normalized stroke geometry` ≅ our `I — scribe-hand / paleographic features: decompose Hebrew letter-forms into stroke geometry from page images to cluster HANDS (separate scribe from content)`.
- **why it transfers**: our pipeline is text-first; hand identification needs image-derived geometry, and this gives a script-agnostic bitmap→structure extractor that could feed a scribe-hand clustering layer we don't have.
- **why it might NOT**: Hebrew is alphabetic (fewer strokes than logographs), and we'd need an image pipeline + segmentation we currently lack.
- **transfer confidence**: speculative
- **priority**: B
- **bib leads**: graphonomics / online-handwriting stroke features; writer identification.

### Suggest-Verify-Revise: Document-Level Event Causality with Narrative Consistency (SVRECI)
- **id / track / file**: ACL 2026 Long · NLG · `Tracks/Natural_Language_Generation/Su_Tan_Suggest_Verify_Revise_A_Three_Stage_Document_Level_Even.pdf`
- **surface domain**: identifying causal links among events in a document.
- **mechanism**: **suggest** cheap candidate links (LLM heuristics + hypergraph) → **verify** with a global consistency constraint → **revise** by iteratively pruning noisy edges from a dynamically-evolving graph (contrastive at edge level).
- **analogy**: their `suggest → verify → iteratively prune noisy edges on an evolving relation graph` ≅ our `F/K — build a witness/transmission graph: propose candidate copy-links cheaply, verify with alignment, iteratively delete noisy edges toward a clean stemma`.
- **why it transfers**: a concrete three-stage skeleton for the lineage graph we lack, with the key move (iterative global-consistency edge pruning) that our current pairwise matching never does.
- **why it might NOT**: the verify step (Topological Hawkes over event time) is domain-specific; we'd replace it with a paleographic/textual consistency check that isn't off-the-shelf.
- **transfer confidence**: medium
- **priority**: B
- **bib leads**: iterative graph denoising; global-consistency constraints for relation extraction.

### Graph-Based Alternatives to LLMs for Human Simulation (GEMS)
- **id / track / file**: ACL 2026 Long · Computational_Social_Science · `Tracks/Computational_Social_Science/Suh_Chang_Graph_Based_Alternatives_to_LLMs_for_Human_Simulation.pdf`
- **surface domain**: predicting people's close-ended choices (survey/test answers).
- **mechanism**: formulate as **link prediction on a heterogeneous graph** of individuals×choices; a small GNN matches LLMs with ~1000× fewer parameters.
- **analogy**: their `link prediction on a bipartite individuals×choices graph` ≅ our `E/F — predict missing fragment→work links on a bipartite fragment×composition graph (attribution / join suggestion)`.
- **why it transfers**: reframes attribution/joins as link prediction and shows a cheap GNN can beat an LLM — a cost lesson for us and a concrete model class for the join-suggestion problem.
- **why it might NOT**: needs a partially-observed graph with reliable positive links to train on; our known attributions are sparse and biased.
- **transfer confidence**: medium
- **priority**: B
- **bib leads**: heterogeneous-graph link prediction; GNNs vs LLMs for tabular/relational tasks.

### Copyright Detective: A Forensic System to Evidence LLMs' Copyright Leakage
- **id / track / file**: ACL 2026 Demo · Demos · `Tracks/Demos/Zhang_Copyright_Detective_A_Forensic_System_to_Evidence_LLMs.pdf`
- **surface domain**: interactive system auditing whether an LLM reproduces copyrighted text (verbatim + paraphrase).
- **mechanism**: treats reuse as an **evidence-DISCOVERY process, not binary classification** — combines content-recall testing, paraphrase-level similarity, and iterative probing into a forensic dossier.
- **analogy**: their `reuse-as-evidence-dossier UI (verbatim vs paraphrase, graded)` ≅ our `product/H — GenizahSearch's tiered A/B/C browse-grounded evidence reporting; a forensic UI that shows WHY a parallel is claimed, at graded strength`.
- **why it transfers**: validates and refines our "honest partial-evidence reporting" stance and offers concrete UI patterns (verbatim vs paraphrase panels, iterative evidence accumulation) for presenting reuse findings.
- **why it might NOT**: product/UX inspiration, not a matching algorithm; its probes assume an interactive black-box LLM, not a static corpus.
- **transfer confidence**: medium (product)
- **priority**: B
- **bib leads**: memorization/verbatim extraction (Carlini et al.); paraphrase-robust reuse similarity.

### Losses that Cook: Topological Optimal Transport for Structured Recipe Generation
- **id / track / file**: ACL-Findings 2026 · NLG · `Tracks/Natural_Language_Generation/Ottoborgo_Garza_Losses_that_Cook_Topological_Optimal_Transport_for_Stru.pdf`
- **surface domain**: generating recipes where a few key tokens (ingredients/quantities) matter far more than connective words.
- **mechanism**: represent an item set as a **point cloud in embedding space and minimize optimal-transport divergence** to the gold set (order-free); explicitly weights high-impact vs low-impact tokens.
- **analogy**: their `OT divergence between two unordered embedding point-clouds` ≅ our `L/B — score similarity of two witnesses by their content-word point-clouds, robust to word order and insertions; + content-vs-function-word weighting`.
- **why it transfers**: gives an order-free set-similarity (OT) for comparing bags of content words across witnesses — a complement to sequence alignment when word order is scrambled — and formalizes the high/low-impact token asymmetry we handle via DF-capping.
- **why it might NOT**: it's a training LOSS for generation; we'd repurpose only the OT set-distance as a scorer, and OT is O(n³)-ish per pair (needs blocking).
- **transfer confidence**: medium
- **priority**: B
- **bib leads**: Sinkhorn/entropic OT (Cuturi); word-mover's distance (Kusner et al.).

### DMRetriever: A Family of Models for Improved Text Retrieval in Disaster Management
- **id / track / file**: ACL 2026 Long · NLP_Applications · `Tracks/NLP_Applications/Yin_Caverlee_DMRetriever_A_Family_of_Models_for_Improved_Text_Retrie.pdf`
- **surface domain**: a domain-specialized dense retriever for disaster-response queries.
- **mechanism**: three-stage training — bidirectional-attention adaptation → unsupervised contrastive pre-training → **difficulty-aware PROGRESSIVE (curriculum) instruction fine-tuning**, with a data-refinement pipeline; tiny models beat 13× larger ones.
- **analogy**: their `curriculum contrastive domain-specialized retriever` ≅ our `G — Track-3: train a Genizah-reuse embedding via curriculum from easy (verbatim) to hard (paraphrase/JA↔Hebrew) pairs, cheaply`.
- **why it transfers**: a concrete, parameter-efficient recipe for building our own reuse-retrieval embedding instead of a generic sentence encoder — the curriculum ordering matches our natural difficulty tiers.
- **why it might NOT**: standard-ish recipe; the win depends on a good data-refinement pipeline that we'd have to build for noisy Hebrew/JA.
- **transfer confidence**: medium
- **priority**: B
- **bib leads**: contrastive dense retrieval (E5, GTE, BGE); curriculum learning for IR.

### POTATO 2.0: Annotation Platform with AI-in-the-Loop
- **id / track / file**: ACL 2026 Demo · Demos · `Tracks/Demos/Jurgens_Iyer_Potato_2_0_A_Comprehensive_Annotation_Platform_with_AI.pdf`
- **surface domain**: open-source annotation platform (39 task types, multimodal) with a one-human + LLM workflow.
- **mechanism**: **uncertainty-driven instance selection** + iterative prompt refinement + progressive autonomy → efficient dataset creation without a large annotation team.
- **analogy**: their `uncertainty-driven active annotation` ≅ our `J/product — build silver/gold reuse-pair and scribe-hand labels with one annotator + LLM, prioritizing high-uncertainty (informative) instances`.
- **why it transfers**: directly usable tooling to produce Track-3 training labels and evaluation sets cheaply, with active-learning instance selection baked in.
- **why it might NOT**: it's infrastructure, not a matching mechanism; RTL/Hebrew + manuscript-image annotation may need customization.
- **transfer confidence**: medium (product/tooling)
- **priority**: B
- **bib leads**: active learning by uncertainty; human-in-the-loop annotation.

### Draft, Verify, Restore: Self-Refining Historical Inscription Restoration (UniHIR)
- **id / track / file**: ACL 2026 Long · Computational_Social_Science · `Tracks/Computational_Social_Science/Zhang_Jin_Draft_Verify_Restore_Self_Refining_Historical_Inscripti.pdf`
- **surface domain**: restoring illegible/damaged stone-inscription text with a unified multimodal LLM (heritage — domain-adjacent to us).
- **mechanism**: a single model does Draft-Guided Localization + **Hierarchical Self-Refinement** (localize damage → predict content → iteratively self-correct) with **step-aware supervision on intermediate drafts**, replacing task-separated pipelines that accumulate irreversible errors.
- **analogy**: their `unified draft→verify→restore with step supervision` ≅ our `K — lacuna restoration / HTR-error correction as one self-refining loop rather than separate detect→align→fill stages`.
- **why it transfers**: mirrors our restoration goal on very similar data (degraded historical script); the "step-aware intermediate-draft supervision" and "avoid irreversible pipeline error accumulation" lessons apply to our multi-stage matcher+restorer.
- **why it might NOT**: domain-adjacent (low surprise; likely also in Wave-1), and it's image-to-text MLLM restoration — heavier than our text-side pipeline.
- **transfer confidence**: medium
- **priority**: B
- **bib leads**: iterative refinement / self-correction; Ithaca (Assael et al., text restoration).

### Markovian Linguistic-Temporal Bridge (MGSAA)
- **id / track / file**: ACL 2026 Long · Financial_and_TimeSeries · `Tracks/Financial_and_TimeSeries/Sun_Yang_Markovian_Linguistic_Temporal_Bridge_Unlocking_the_Pote.pdf`
- **surface domain**: adapting LLMs to time-series forecasting by aligning the linguistic and temporal representation spaces.
- **mechanism**: distill one domain's latent structure into a **Markov state-transition graph, transfer it as a structural prior to the other domain, then align via state-constrained cross-attention** — "global structural isomorphism" instead of pointwise matching.
- **analogy**: their `align two spaces by a transferred structural prior + constrained attention` ≅ our `L — align Judeo-Arabic↔Hebrew (or image↔text) by mapping a structural prior across and constraining the cross-space matching`.
- **why it transfers**: a template for structure-level (not token-level) cross-representation alignment, which is the right altitude for JA↔Hebrew where token-level correspondences are weak.
- **why it might NOT**: heavily tuned to LLM-for-forecasting; the "Markov state graph of language" abstraction may not carry a useful structural prior for script/language bridging.
- **transfer confidence**: speculative
- **priority**: B
- **bib leads**: optimal transport / Procrustes for embedding-space alignment; structure-constrained cross-attention.

### HINDSIGHT: Structured Agent Memory (fact/belief separation + temporal entity graph + hybrid retrieval)
- **id / track / file**: ACL 2026 Demo · Demos · `Tracks/Demos/Latimer_Ramakrishnan_Hindsight_Structured_Agent_Memory_that_Retains_Recalls.pdf`
- **surface domain**: long-term memory system for AI agents (open-source, pgvector-backed).
- **mechanism**: separates **objective FACTS from subjective BELIEFS with confidence scores**, keeps **temporal entity graphs**, and retrieves via a **parallel hybrid pipeline (vector + keyword + graph traversal + temporal filter)**.
- **analogy**: their `fact-vs-belief-with-confidence + temporal graph + hybrid retrieval` ≅ our `product/data-model — store established attributions (facts) apart from candidate/hypothesized reuse (beliefs w/ confidence) over a DATED witness graph, queried by hybrid text+graph+temporal search`.
- **why it transfers**: a clean data-model + retrieval architecture for our attribution store and dated witness graph, and an off-the-shelf pgvector hybrid-retrieval stack.
- **why it might NOT**: product/architecture inspiration only; no reuse-detection content.
- **transfer confidence**: medium (product)
- **priority**: B
- **bib leads**: hybrid dense+sparse+graph retrieval; temporal knowledge graphs.

---

## C / skipped — coverage honesty (what whole classes were checked-and-dropped or not opened)

**Checked but no real transfer (C):** most LLM-reasoning-over-time-series benchmarks
(Tan *Inferring Events*, Time-RA anomaly, TimeSAF fusion, CaTS-Bench captioning,
Temporal-Tokenization, ZARA) — they test whether an LLM can *reason about* numbers, not
liftable alignment/segmentation algorithms; `pAtChWoRK` (title is a false friend — data
restructuring, NOT physical fragment reassembly); Maveli *compression/invertibility*
(LLM code-reasoning, not compression-distance similarity); XMark & Yan *steganography*
(embed/decode hidden bits — not detecting reuse of pre-existing text); Salazar
*disentangling compositionality* (capability-transfer analysis, not concept removal);
Wanner *Sinclair local news* (standard log-odds CSS); TreeDiff AST-diffusion (structure-
aware masking, but generation not collation); DySECT / CaTS synthetic-caption pipelines
(generic bootstrapping).

**Whole classes deliberately skipped (skimmed titles only, judged low mechanism-transfer):**
- **Financial (bulk):** finance-reasoning & agentic-trading benchmarks (FinMaster, FinSight,
  RealFin, AlphaQuanter, FinChain, FinChart, Finch, Nirvana, FinKario, FinCARDS…) — domain
  benchmarks, no transferable algorithm.
- **Comp. Social Science (bulk):** cultural-alignment / LLM-values / persona / survey-
  simulation / moral-judgment papers (~40) — sociocultural *evaluation*, not algorithms
  (sampled GEMS, TRUST, CohTP-adjacent, diffusion, TAIGR as representatives).
- **Sentiment/Style (bulk):** multimodal MSA + stance-detection architecture papers —
  model-specific (sampled EASE, sentiment-circuit, embedding-as-treatment, authorship-
  attribution as reps).
- **NLG (bulk):** story-generation / long-form consistency / character-logic / poetry-eval —
  generation quality (sampled OT-recipe, EvolvR, disentangle, protein-STORY, gold-standard).
- **Human-AI Interaction (all 31):** trust/delegation/persona/preference/theory-of-mind —
  HCI studies, no liftable mechanism (nearest was Kim-derailment, carded from CSS).
- **NLP Applications (bulk):** recommendation (ReRec, UCGRec), legal agents (GLARE, Ready-
  Jurist, Law-in-Silico), medical/ICD coding, chart/table reasoning, unlearning,
  watermarking, C→Rust — sampled contamination/retrieval/calibration/graph/steganography reps.
- **Demos (bulk):** agent-framework/orchestration toolkits (OxyGent, MASFactory, AgentFactory,
  LiTS, MixtureKit, rosaOS, ScaleBox) — infra, no transfer (carded the corpus-search /
  annotation / memory / interpretability tools).
- **Workshops:** GEM is dominated by reference-free eval / LLM-as-judge / reproducibility
  (ReproHum series) — a large H-flavored cluster; carded the few with a liftable move
  (contamination survey, soft-labeling, SpecRef) and treat the rest as "trust-without-gold"
  background. **MELLM** (multilingual / tokenizer-parity / cross-lingual) is L-adjacent
  (JA↔Hebrew) but mostly benchmarks — cross-lingual style/AA transfer already covered by
  LaCava; no separate card. **SurgeLLM** (text-to-SQL / table / structured) — structural-
  faithfulness could tangentially touch collation, but no crisp mechanism (carded TreeDiff→C).
  **NLP-CSS** — carded/checked diffusion + content-shift reps.

**Not carded but noted as thin-B framing:** LaCava *Multilingual Authorship Attribution*
(I — useful caveat that stylometric AA does NOT transfer across scripts/language families,
directly relevant to Hebrew↔JA scribe-hand modeling); Protein-STORY (fusing many textual
descriptions of ONE entity into a single canonical embedding ≅ one canonical embedding of a
work from many witnesses); Fast-MIA (systems pattern: compute shared intermediate results
once, cache, share across detection methods — mirrors our two-pass shared-seed caching);
Zhou *emoji diffusion* & Mei *Illusions of the Gold Standard* (framing only: transmission/
diffusion of a textual unit; and that human "gold" is under-documented → reinforces our
no-gold stance). Neo *Spectra* / AnnoHID: minor tooling (VLM interpretability lib; low-
resource annotation) — usable if/when we build those pipelines.
