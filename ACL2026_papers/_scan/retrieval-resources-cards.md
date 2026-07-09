# ACL 2026 scan — Retrieval / RAG / Resources & Evaluation tracks

Scope: Tracks/Information_Extraction_Retrieval (89), Tracks/Retrieval_Augmented_LMs (103),
Tracks/Resources_and_Evaluation (316). Posters/ ignored. Strict triage per RELEVANCE-PROFILE.md.
Counts: scanned ~508 titles · abstract-checked 42 · A 9 · B 10 · C (rest).

---

## A — act on these

### The Overlooked Role of Graded Relevance Thresholds in Multilingual Dense Retrieval
- **id**: ACL 2026 Findings (pp. 7738–7750) · Information_Extraction_Retrieval · **file**: Tracks/Information_Extraction_Retrieval/Wullach_Cohen_The_Overlooked_Role_of_Graded_Relevance_Thresholds_in_M.pdf
- **authors/lab**: Tomer Wullach, Ori Shapira, Amir DN Cohen — **OriginAI (Israeli NLP company — a contact worth flagging)**.
- **tl;dr**: Contrastive retrieval training needs *binary* relevance labels, but relevance is graded. Using an LLM-annotated multilingual set, they show the graded→binary threshold is the hidden knob: the optimal cutoff varies systematically by language/resource level, and a good threshold improves effectiveness, *cuts the amount of fine-tuning data needed*, and mitigates annotation noise.
- **relevance**: [embed, xlingual, llm-annot] — This is our Track-3 training setup almost exactly: we mint silver labels from the lexical engine + LLM-as-annotator and must binarize graded reuse strength into positive/negative pairs. Says threshold calibration is a first-class pipeline decision, especially across our uneven Hebrew/JA/Aramaic resource levels.
- **stealable**: Treat the silver-label threshold as a tuned hyperparameter per language, not a fixed 0.5; graded LLM relevance scores + calibrated cutoff reduce required training pairs and denoise labels.
- **priority**: A
- **bib leads**: Arabzadeh & Clarke 2025 "Benchmarking LLM-based relevance judgment methods"; Chapelle et al. 2009 "Expected reciprocal rank for graded relevance"; Huang et al. 2025 "Boosting Data Utilization for Multilingual Dense Retrieval"; MMTEB (Enevoldsen et al. 2025).

### LANGSAE EDITING: Improving Multilingual IR via Post-hoc Language Identity Removal
- **id**: ACL 2026 Long (pp. 36374–36389) · Information_Extraction_Retrieval · **file**: Tracks/Information_Extraction_Retrieval/Kim_Lim_LangSAE_Editing_Improving_Multilingual_Information_Retr.pdf
- **authors/lab**: Dongjun Kim, Jeongho Yoon, Chanjun Park, Heuiseok Lim — Korea Univ. / Soongsil.
- **tl;dr**: Multilingual embeddings encode language identity alongside semantics, which inflates same-language similarity and crowds out relevant cross-language evidence. They train a post-hoc sparse autoencoder on pooled embeddings, identify language-associated latent units via cross-language activation stats, suppress them at inference, and reconstruct in the original dimensionality — **no retraining/re-encoding, drop-in for an existing vector DB**.
- **relevance**: [embed, xlingual] — Directly targets our JA↔Hebrew problem: a Hebrew paraphrase of a Judeo-Arabic passage should rank high, but language-identity signal keeps same-script neighbors on top. Post-hoc SAE editing lets us de-bias whatever multilingual encoder we adopt for Track-3 without rebuilding the index.
- **stealable**: Sparse-autoencoder language-identity ablation as a post-processing layer over cached embeddings; keeps vector-DB compatibility.
- **priority**: A
- **bib leads**: Huang et al. 2024 "Language concept erasure for language-invariant dense retrieval"; Hu et al. 2023 "Language agnostic multilingual IR with contrastive learning"; Feng et al. 2022 LaBSE; Artetxe & Schwenk 2019 (massively multilingual sentence embeddings). (Note: Ravfogel-style concept erasure lineage — Israeli.)

### Code-Switching Information Retrieval: Benchmarks, Analysis, and the Limits of Current Retrievers
- **id**: ACL 2026 Findings (pp. 13055–13071) · Information_Extraction_Retrieval · **file**: Tracks/Information_Extraction_Retrieval/Zeng_Yokoya_Code_Switching_Information_Retrieval_Benchmarks_Analysi.pdf
- **authors/lab**: Qingcheng Zeng (Northwestern), Yuheng Lu (Waseda) et al.; RIKEN AIP / Univ. Tokyo.
- **tl;dr**: Builds CSR-L (human-annotated mixed-language queries) + CS-MTEB; shows code-switching is a persistent bottleneck that degrades even strong multilingual dense/cross-encoder/late-interaction retrievers, and traces the failure to a measurable divergence between monolingual and code-switched *query* embeddings.
- **relevance**: [xlingual, product] — Judeo-Arabic is itself a code-switched register (Arabic lexicon in Hebrew script with embedded Hebrew/Aramaic). This quantifies exactly the embedding-divergence failure we'd hit and gives a benchmark-construction template for a Genizah code-switched retrieval eval.
- **stealable**: The CSR-L human-annotation protocol + the monolingual-vs-mixed embedding-divergence diagnostic as a health check on our JA encoder.
- **priority**: A
- **bib leads**: Do et al. 2024 "ContrastiveMix: overcoming code-mixing dilemma in cross-lingual transfer for IR"; Gupta et al. 2014 "Query expansion for mixed-script IR"; Conneau et al. 2018 "Word translation without parallel data"; Banerjee et al. 2016 MSIR overview.

### LLMs Meet Isolation Kernel: Lightweight, Learning-free Binary Embeddings for Fast Retrieval
- **id**: ACL 2026 Findings (pp. 13601–13623) · Information_Extraction_Retrieval · **file**: Tracks/Information_Extraction_Retrieval/Zhang_Nguyen_LLMs_Meet_Isolation_Kernel_Lightweight_Learning_free_Bi.pdf
- **authors/lab**: Zhibo Zhang, Yang Xu, Kai Ming Ting, Cam-Tu Nguyen — Nanjing University.
- **tl;dr**: IKE transforms a high-dim LLM embedding into a *binary* embedding via Isolation Kernel — learning-free, low memory, bitwise scoring. Reports up to 16.7× faster retrieval and 16× lower memory vs the original embedding, beating Matryoshka/CSR compression on the accuracy/efficiency trade-off.
- **relevance**: [scale] — Our all-pairs frontier is 62,414 MSS / 1.34M candidate pairs; a Track-3 semantic layer at that scale needs cheap ANN. Learning-free binarization (no training data needed) is ideal for our unlabeled corpus and slots under a Faiss/HNSW index for the candidate-generation stage before banded verification.
- **stealable**: Isolation-Kernel binary encoding as the compression step for corpus-scale embedding search; learning-free so no Genizah labels required.
- **priority**: A
- **bib leads**: Charikar 2002 "Similarity estimation techniques from rounding algorithms" (SimHash); Jégou et al. 2011 "Product quantization for NN search"; Guo et al. 2020 "Anisotropic vector quantization" (ScaNN); BehnamGhader et al. 2024 "LLM2Vec"; Douze et al. 2025 "The Faiss library".

### MTEB-NL and E5-NL: Embedding Benchmark and Models for Dutch
- **id**: ACL 2026 Findings (pp. 24684–24709) · Resources_and_Evaluation · **file**: Tracks/Resources_and_Evaluation/Banar_Daelemans_MTEB_NL_and_E5_NL_Embedding_Benchmark_and_Models_for_Du.pdf
- **authors/lab**: Nikolay Banar, Ehsan Lotfi, … Walter Daelemans — Univ. Antwerp (DH/cultural-heritage group; Banar also works on iconography retrieval).
- **tl;dr**: End-to-end recipe for an under-resourced language: assemble a task-diverse embedding benchmark (existing + newly created sets), compile a training set from retrieval data **complemented with LLM-generated synthetic data to cover tasks beyond retrieval**, and release compact E5-NL models with strong multi-task performance.
- **relevance**: [embed, product] — This is the blueprint for a "MTEB-HE/JA" plus our own compact Genizah embedding model. The synthetic-data augmentation to expand task coverage maps to our silver-label generation, and the compact-model angle matches our need for cheap corpus-scale encoding.
- **stealable**: The whole pipeline: benchmark + LLM-synthetic training augmentation + compact E5 fine-tune, as a template for a Hebrew/Judeo-Arabic embedding suite.
- **priority**: A
- **bib leads**: Bhatia et al. 2025 "Swan and ArabicMTEB: dialect-aware, Arabic-centric, cross-lingual, cross-cultural embedding models and benchmarks" (**directly Semitic — chase this**); Baysan & Güngör 2025 "TR-MTEB (Turkish)"; Bonifacio et al. 2022 "InPars" (unsupervised synthetic query generation); Ciancone et al. 2024 (French MTEB).

### FLARE: Task-Agnostic Embedding Model Evaluation via Normalizing Flows
- **id**: ACL 2026 Findings (pp. 39271–39294) · Resources_and_Evaluation · **file**: Tracks/Resources_and_Evaluation/Jiang_Tam_FLARE_Task_Agnostic_Embedding_Model_Evaluation_via_Norm.pdf
- **authors/lab**: Jingzhou Jiang, Yixuan Tang, Yi Yang, Kar Yan Tam — HKUST.
- **tl;dr**: Picks the best embedding model for a target corpus **without task labels** by estimating information sufficiency from normalizing-flow log-likelihoods (avoids distance/kernel density estimates that collapse in high dimensions). Finite-sample bounds depend on the data's intrinsic dimension; Spearman ρ up to 0.90 with supervised benchmarks, stable at d≥3,584.
- **relevance**: [embed, product] — We have essentially no gold retrieval labels for the Genizah corpus, so choosing among candidate encoders (multilingual-E5 vs BGE-M3 vs a Hebrew model) is currently guesswork. FLARE gives a principled label-free selection criterion on our own unlabeled pages.
- **stealable**: Run FLARE over a sample of our transcribed pages to rank candidate embedders before committing to Track-3, no annotation required.
- **priority**: A
- **bib leads**: Darrin et al. 2024 "When is an embedding model more promising than another?" (NeurIPS); Durkan et al. 2019 "Neural spline flows"; Ansuini et al. 2019 "Intrinsic dimension of data representations"; Chen et al. 2024b "AIR-Bench (automated heterogeneous IR benchmark)".

### Large Language Models Are Effective Human Annotation Assistants, But Not Good Independent Annotators
- **id**: ACL 2026 Findings (pp. 71–89) · Information_Extraction_Retrieval · **file**: Tracks/Information_Extraction_Retrieval/Gu_BoydGraber_Large_Language_Models_Are_Effective_Human_Annotation_As.pdf
- **authors/lab**: Feng Gu, Zongxia Li, … Jordan Boyd-Graber — Univ. Maryland (+ START terrorism-studies consortium).
- **tl;dr**: Evaluates a *whole* event-annotation workflow (coreference + argument extraction) in AI-only, AI-assisted, and human-only modes. AI recall is 7× a TF-IDF baseline but far from replacing experts; however, experts adopt AI-extracted arguments 60% of the time, cutting extraction time 25%. Verdict: LLMs assist, don't replace.
- **relevance**: [llm-annot] — Cautionary design input for our LLM-as-annotator: for philological silver labels, use the LLM as an assistant with human-in-the-loop review of full workflows, not as an autonomous labeler; measure adoption-rate and time-saved, not just accuracy.
- **stealable**: The 3-mode (AI-only / AI-assist / human-only) evaluation harness + "expert adoption rate" and "time saved" metrics for scoring our annotation loop.
- **priority**: A
- **bib leads**: Cattan/Eirew/Dagan 2020–21 cross-document coreference (Bar-Ilan, Dagan lab — Israeli); Barhom et al. 2019 (joint cross-doc entity/event coref); Bulian et al. 2022 "Beyond token-level answer equivalence for QA evaluation"; Bugert et al. 2021 (generalizing cross-doc coref across corpora).

### Refining and Reusing Annotation Guidelines for LLM Annotation
- **id**: ACL 2026 Long (pp. 37951–37964) · Resources_and_Evaluation · **file**: Tracks/Resources_and_Evaluation/Kim_Aizawa_Refining_and_Reusing_Annotation_Guidelines_for_LLM_Anno.pdf
- **authors/lab**: Kon Woo Kim, Jin-Dong Kim, Akiko Aizawa — NII / SOKENDAI (biomedical NLP).
- **tl;dr**: Proposes systematic reuse+refinement of annotation *guidelines* as the alignment mechanism for LLM annotators: an iterative moderation loop that simulates the early phase of an annotation project. Confirms three hypotheses on biomedical NER (NCBI Disease, BC5CDR, BioRED) across GPT/Gemini/DeepSeek — guideline integration helps, reasoning models help, and refinement works under minimal supervision.
- **relevance**: [llm-annot] — Our LLM annotator struggles with the specialized conventions of medieval Hebrew/JA philology exactly like it struggles with gold-standard biomedical conventions. This gives a concrete recipe to iteratively evolve a guideline prompt that encodes our reuse/paraphrase criteria.
- **stealable**: The iterative guideline-moderation framework (LLM proposes guideline edits → re-annotate → converge) to bootstrap a stable Track-3 annotation prompt.
- **priority**: A
- **bib leads**: Bibal et al. 2025 "Automating annotation guideline improvements using LLMs"; Fonseca & Cohen 2024 "Can LLMs follow concept annotation guidelines? (scientific & financial)"; Kim et al. 2025 "Repurposing annotation guidelines to instruct LLM annotators"; Huang et al. 2025 "GuideNER: guidelines are better than examples for in-context NER".

### Mediocrity is the Key for LLM-as-a-Judge Anchor Selection
- **id**: ACL 2026 Long (pp. 15491–15513) · Resources_and_Evaluation · **file**: Tracks/Resources_and_Evaluation/DonYehiya_Abend_Mediocrity_is_the_key_for_LLM_as_a_Judge_Anchor_Selecti.pdf
- **authors/lab**: Shachar Don-Yehiya, Asaf Yehudai, Leshem Choshen, Omri Abend — **Hebrew University of Jerusalem + IBM Research / MIT (Israeli — strong contact)**.
- **tl;dr**: To dodge the quadratic cost of pairwise LLM-judge comparisons, benchmarks compare all models to one *anchor*. Testing 22 anchors on Arena-Hard-v2.0, they find anchor choice is critical and the common extreme choices (best/worst model) are poor anchors because they beat/lose to everyone; a mediocre anchor is most informative of relative ranking.
- **relevance**: [llm-annot, scale] — When we use an LLM judge to rank candidate parallels or adjudicate silver labels, and want to avoid all-pairs judgments, this tells us how to pick the reference item: a mid-strength anchor, not the strongest or weakest. Also a direct Israeli-lab contact for LLM-as-judge philology work.
- **stealable**: Use a mediocre/median anchor for anchor-based pairwise LLM judging; avoids the O(n²) blowup on our large candidate sets while preserving ranking fidelity.
- **priority**: A
- **bib leads**: Boubdir et al. 2023 "Elo Uncovered: robustness and best practices in LM evaluation"; Chiang & Lee 2023 "Can LLMs be an alternative to human evaluations?"; Chiang et al. 2024 "Chatbot Arena"; Dubois et al. 2024 "Length-controlled AlpacaEval"; Bradley & Terry 1952 (paired comparisons).

---

## B — awareness / design-informing

### SURE or Not? Investigating Semantic Understanding in Dense Retrieval Models
- **id**: ACL 2026 Long (pp. 45873–45887) · Information_Extraction_Retrieval · **file**: Tracks/Information_Extraction_Retrieval/Kong_Sun_SURE_or_Not_Investigating_Semantic_Understanding_in_Den.pdf
- **authors/lab**: Lingdi Kong, Xuanang Chen, Ben He, Le Sun — UCAS / ISCAS.
- **tl;dr**: Introduces SURE, a benchmark probing whether dense retrievers actually capture semantics along three axes — semantic precision, abstraction, and equivalence — over MSMARCO/NQ/FiQA; evaluates 10 models (110M–8B).
- **relevance**: [embed, product] — Gives a ready diagnostic vocabulary (precision / abstraction / equivalence) for validating whether our Track-3 embeddings recognize paraphrase-level equivalence vs surface overlap — the exact capability we need beyond the lexical engine.
- **stealable**: The three-dimension semantic-understanding probe design, ported to Hebrew/JA paraphrase pairs.
- **priority**: B
- **bib leads**: Hagen et al. 2024 "Revisiting query variation robustness of transformer models"; Guo et al. 2022 "Semantic models for first-stage retrieval: a review"; Hofstätter et al. 2021 "Balanced topic-aware sampling".

### Situated Embedding Models for Context-Aware Dense Retrieval
- **id**: ACL 2026 Short (pp. 37–49) · Information_Extraction_Retrieval · **file**: Tracks/Information_Extraction_Retrieval/Wu_Yu_Situated_Embedding_Models_for_Context_Aware_Dense_Retri.pdf
- **authors/lab**: Junjie Wu (HKUST), … Mo Yu (Tencent) et al.
- **tl;dr**: Chunk embeddings lose cross-chunk context; rather than encoding longer windows (which strains capacity and returns coarse evidence), they embed a *short* chunk *conditioned on* a broader context window, keeping localized retrieval while injecting document context.
- **relevance**: [embed, product] — Genizah fragments are short, context-poor chunks whose meaning depends on the surrounding page/composition. Situated (context-conditioned) chunk embeddings could sharpen fragment-level retrieval without ballooning the unit of return.
- **stealable**: Condition short-fragment embeddings on a broader page/composition context vector; cf. "late chunking" and contextual retrieval.
- **priority**: B
- **bib leads**: Günther et al. 2024 "Late chunking: contextual chunk embeddings"; Anthropic 2024 "Contextual Retrieval"; Moreira et al. 2024 "NV-Retriever: effective hard-negative mining".

### Reliable Evaluation Protocol for Low-Precision Retrieval
- **id**: ACL 2026 Short (pp. 396–409) · Information_Extraction_Retrieval · **file**: Tracks/Information_Extraction_Retrieval/Yang_Lim_Reliable_Evaluation_Protocol_for_Low_Precision_Retrieva.pdf
- **authors/lab**: Kisu Yang et al. (VAIV / Korea Univ.); Isabelle Augenstein (Copenhagen).
- **tl;dr**: Low-precision (quantized) scoring creates spurious ties that make retrieval metrics unstable. Proposes High-Precision Scoring (upcast only the final scoring step) + a Tie-aware Retrieval Metric reporting expected score/range/bias, over 12 datasets × 3 scoring functions.
- **relevance**: [scale, product] — If we quantize/binarize embeddings for corpus-scale search (see the Isolation-Kernel A-card), tie-induced ranking noise is exactly what we'll face; HPS is a cheap fix and TRM is an honest metric for our retrieval evals.
- **stealable**: Upcast only the final scoring step to break ties + tie-aware reporting; drop-in for a quantized Genizah index.
- **priority**: B
- **bib leads**: McSherry & Najork 2008 "Computing IR performance measures in the presence of tied scores"; Kurtic et al. 2024 "Give Me BF16 or Give Me Death"; Micikevicius et al. 2017 "Mixed precision training".

### When Does Mixing Help? Query Embedding Interpolation in Multilingual Dense Retrieval
- **id**: ACL 2026 Long (pp. 31544–31562) · Information_Extraction_Retrieval · **file**: Tracks/Information_Extraction_Retrieval/Zhu_Kan_When_Does_Mixing_Help_Analyzing_Query_Embedding_Interpo.pdf
- **authors/lab**: Tongyao Zhu, Chao-Ming Huang, Min-Yen Kan — NUS.
- **tl;dr**: Constructs mixed-language queries as interpolations of monolingual embeddings on mMARCO; an optimal mixing ratio beats the best monolingual endpoint in 88/105 cases, with an English-dominance asymmetry and a negative correlation between mixing gains and typological distance.
- **relevance**: [xlingual, embed] — Suggests representing a bilingual JA/Hebrew query as an embedding interpolation and tuning the mix ratio; the typological-distance finding warns that Hebrew↔Arabic mixing behaves differently than closely related pairs.
- **stealable**: Embedding-level interpolation of parallel-language query representations with a tuned ratio, as a cheap cross-lingual bridge.
- **priority**: B
- **bib leads**: Do et al. 2024 "ContrastiveMix"; Chakma & Das 2016 "CMIR (Hindi-English code-mixed IR)"; Van der Goot et al. 2025 "DistaLS: language distance measures".

### PL-MTEB: Polish Massive Text Embedding Benchmark
- **id**: ACL 2026 Findings (pp. 35601–35619) · Resources_and_Evaluation · **file**: Tracks/Resources_and_Evaluation/Poswiata_Perekiewicz_PL_MTEB_Polish_Massive_Text_Embedding_Benchmark.pdf
- **authors/lab**: Rafał Poświata, Sławomir Dadas, Michał Perełkiewicz — National Information Processing Institute, Poland.
- **tl;dr**: A 30-task Polish MTEB across classification, clustering, pair-classification, retrieval, and STS; adds 12 new tasks and evaluates 30 embedding models with per-task-type and per-size analysis.
- **relevance**: [embed, product] — A second concrete template (alongside MTEB-NL) for standing up a Hebrew/JA embedding benchmark, notably including a dedicated STS category — the paraphrase-similarity axis Track-3 cares about.
- **stealable**: Task taxonomy + the practice of adding new native-language STS/clustering tasks to the MTEB harness.
- **priority**: B
- **bib leads**: Bhatia et al. 2025 "Swan & ArabicMTEB" (Semitic); Dadas 2022 "Training effective neural sentence encoders from automatically mined paraphrases" (silver-paraphrase mining); Dadas et al. 2024 "PIRB (Polish dense/hybrid retrieval benchmark)".

### BNLP: A Text Annotation Platform for Quality Control of LLM-Generated Annotations
- **id**: ACL 2026 Findings (pp. 23675–23684) · Resources_and_Evaluation · **file**: Tracks/Resources_and_Evaluation/Zhuang_Zhang_BNLP_A_Text_Annotation_Platform_for_Quality_Control_of.pdf
- **authors/lab**: Xinhao Zhuang, Qiongyu Tian, … Guoqing Zhang — SINH-CAS / Fudan.
- **tl;dr**: An annotation platform that embeds LLM labeling into a quality-aware collaborative workflow: LLM outputs are treated as revisable intermediate states, with multi-role collaboration, iterative review cycles, and consistency analysis for continuous quality monitoring.
- **relevance**: [llm-annot, product] — Tooling pattern for our human-AI silver-label loop: LLM proposes, humans revise, consistency metrics gate acceptance — the operational counterpart to the guideline-refinement A-card.
- **stealable**: "LLM output as revisable intermediate state" + consistency-analysis gating, as the workflow spec for our annotation UI.
- **priority**: B
- **bib leads**: He et al. 2024 "AnnoLLM"; Kim et al. 2024 "MEGAnno+ (human-LLM collaborative annotation)"; Klie et al. 2018 "INCEpTION"; Pei et al. 2022 "POTATO".

### Toward Robust Evaluation for Multilingual Grammatical Error Correction: Can LLMs Replace Human References?
- **id**: ACL 2026 Long (pp. 47440–47463) · Resources_and_Evaluation · **file**: Tracks/Resources_and_Evaluation/Rozovskaya_Roth_Toward_Robust_Evaluation_for_Multilingual_Grammatical_E.pdf
- **authors/lab**: Alla Rozovskaya (CUNY), Dan Roth (UPenn / Oracle AI).
- **tl;dr**: Standard fixed-reference GEC evaluation underestimates systems because valid corrections are many. They generate *closest-gold* references by prompting an LLM with each system's output; these automatic closest-golds correlate well with human closest-golds, whereas standard reference-based scores show weak/no correlation.
- **relevance**: [noise, llm-annot] — Our HTR post-correction / orthographic-normalization outputs have the same many-valid-answers problem; this is a method for evaluating them without an exhaustive gold set, using an LLM to synthesize output-conditioned references.
- **stealable**: LLM-generated closest-gold references for evaluating noise-correction/normalization on Genizah text.
- **priority**: B
- **bib leads**: Asano et al. 2017 "Reference-based metrics can be replaced with reference-less metrics for GEC"; Alhafni et al. 2023 "Arabic grammatical error detection & correction" (Semitic); Bryant et al. 2023 "GEC: a survey of the state of the art"; Benkirane et al. 2024 "MT hallucination detection for low/high-resource languages".

### Breaking Language Preference in Multilingual RAG via Language-Controllable Retrieval and Language-Agnostic Reasoning
- **id**: ACL 2026 Findings (pp. 7579–7589) · Retrieval_Augmented_LMs · **file**: Tracks/Retrieval_Augmented_LMs/Huo_Qin_Breaking_Language_Preference_in_Multilingual_RAG_via_La.pdf
- **authors/lab**: Wenshuai Huo, Xiaocheng Feng, … Bing Qin — Harbin Institute of Technology / Pengcheng Lab.
- **tl;dr**: Multilingual RAG suffers "language preference": semantically equivalent queries in different languages retrieve different results, and models over-weight surface language form over semantic relevance. They disentangle the pipeline into language-controllable retrieval + language-agnostic reasoning.
- **relevance**: [xlingual, product] — Same surface-form bias we face when matching a Judeo-Arabic passage to its Hebrew parallel; the language-agnostic-reasoning framing reinforces the LangSAE A-card's language-identity-removal idea from the generation side.
- **stealable**: Explicit separation of "which language to retrieve from" vs "reason on meaning, ignore script/form", as an architecture principle for JA↔Hebrew.
- **priority**: B
- **bib leads**: Ki et al. 2025 "Linguistic nepotism: trading-off quality for language preference in multilingual RAG"; Chirkova et al. 2024 "RAG in multilingual settings"; Cruz Blandón et al. 2025 "MEMERAG (multilingual RAG meta-eval)"; Asai et al. 2021 "XOR QA".

### ViDoRe V3: A Comprehensive Evaluation of RAG in Complex Real-World Scenarios
- **id**: ACL 2026 Long (pp. 16570–16600) · Retrieval_Augmented_LMs · **file**: Tracks/Retrieval_Augmented_LMs/Loison_Viaud_ViDoRe_V3_A_Comprehensive_Evaluation_of_Retrieval_Augme.pdf
- **authors/lab**: António Loison, Quentin Macé, … Manuel Faysse (ColPali author), Gautier Viaud — Illuin Technology / NVIDIA / CentraleSupélec.
- **tl;dr**: A multimodal RAG benchmark over *visually rich* document pages (26K pages, 3,099 human-verified queries, 6 languages, 12K hours of annotation), evaluating retrieval + generation jointly with source grounding.
- **relevance**: [product, noise] — The ColPali/ViDoRe lineage retrieves over page *images* directly, which is a frontier route for us that could bypass HTR noise entirely (search manuscript images, not error-laden transcriptions). Worth watching as an alternative Track to lexical/embedding search.
- **stealable**: Visual-document-retrieval framing (embed page images, skip OCR) and the joint retrieval+grounding eval protocol.
- **priority**: B
- **bib leads**: Faysse et al. 2025 "ColPali: efficient document retrieval with VLMs"; Conti et al. 2025 "Context is gold: contextual document embeddings"; Günther et al. 2025 "jina-embeddings-v4 (multimodal multilingual retrieval)"; Cho et al. 2024 "M3DocRAG".

### Revisiting Metric Reliability for Fine-grained Evaluation of MT and Summarization in Indian Languages (ITEM)
- **id**: ACL 2026 Long (pp. 25543–25561) · Resources_and_Evaluation · **file**: Tracks/Resources_and_Evaluation/Yari_Koto_Revisiting_Metric_Reliability_for_Fine_grained_Evaluati.pdf
- **authors/lab**: Amir Hossein Yari (Sharif), … Fajri Koto (MBZUAI).
- **tl;dr**: ITEM benchmarks 29 automatic metrics against human judgments across six Indian languages with fine-grained annotations, testing agreement, outlier sensitivity, language-specific reliability, inter-metric correlation, and resilience to controlled perturbations — most metrics were validated only on English.
- **relevance**: [embed, noise] — Before we trust any embedding/similarity metric to score Genizah paraphrase or JA→Hebrew translation quality, we need exactly this kind of low-resource, perturbation-resilience audit; ITEM is a methodology template.
- **stealable**: The metric-audit protocol (perturbation resilience + outlier sensitivity + per-language reliability) applied to our similarity metrics on noisy Hebrew/JA.
- **priority**: B
- **bib leads**: Agrawal et al. 2024 "Can automatic metrics assess high-quality translations?"; Alves et al. 2022 "Robust MT evaluation with sentence-level multilingual augmentation"; Clark et al. 2023 "SEAHORSE"; Bhattacharjee et al. 2023 "CrossSum".

---

## C / skipped (same keywords, different problem)

### Information_Extraction_Retrieval
- Chen_Liu ReasonEmbed — reasoning-intensive doc retrieval embeddings; modern web, not similarity/reuse.
- Long_Gu GroupRank — LLM passage reranking paradigm; modern web IR.
- Lv_Chen CapCal — de-biasing listwise reranker position bias; modern.
- Sharifymoghaddam_Lin — reranking tradeoffs in deep search agents; agentic web.
- Wei_Zhao — survey of reasoning-intensive retrieval; relevance-via-inference, not our similarity target.
- Wu_Nie — query-aware dimension selection for dense retrieval; marginal efficiency, modern.
- Huang_Kessaci HQDR — structure-aware quantized retrieval for long-doc QA; layout/QA focus.
- Wang_Lei — multimodal KG of Classical Chinese poetry; ancient-language but KG-metadata method, not reuse/embedding.
- All KG / RE / NER / temporal-KG / multimodal-recommendation papers (Agarwal, Cai×2, Choi, Dai, Ewais, Fan, Fu, Gajo, Guo, Hu, Ji, Jin, Kim×several, Li×many, Liu×many, Ma×3, Ning, Peng, Polonuer, Rao, Rathore, Shen, Sternlicht, Tang, Tian, Toksoz, Wang×3, Wu×2, Xie, Xu×2, Yan, Yang×3, Yuan, Zhang×3, Zhao, Zhou, Zhu) — modern IE/KG/agent, out of profile.

### Retrieval_Augmented_LMs (track is predominantly modern-web/agentic RAG → C by profile)
- Xu_Yang RAG in the Wild — RAG effectiveness study; modern QA.
- Choi_Ko ConvX — RAG context compression; generation efficiency.
- Zhou_Wang Retrieval Bottleneck — RL scaling laws for RAG; modern.
- Lee_Kang CORAL — culturally-aligned multilingual RAG agentic loop; modern.
- Bhatia_Alam Islamic QA — Arabic/English generative QA benchmark; Semitic but hallucination/abstention QA, not reuse/embedding.
- All remaining RAG papers (graph-RAG, agentic-RAG, hallucination-detection, multimodal-RAG, RAG-security/poisoning, memory-agents, reranker-for-RAG, KV-cache) — modern-web RAG, out of profile.

### Resources_and_Evaluation
- Chen_Lu — LLM-embedding semantic-drift in online discussions; social-media, GRU/GCN.
- Gao_Zhao ProHist-Bench — LLM historical-reasoning QA (Chinese Imperial Exam); historical but reasoning-QA, not our methods.
- Guo_Lin LiveCLKTBench — cross-lingual factual knowledge transfer eval; modern.
- Naseem_Yimam POLAR — multilingual online-polarization; social-media.
- Lee_Mohammad DimABSA — dimensional aspect-based sentiment; sentiment.
- Plum_Purschke ltzGLUE — Luxembourgish NLU (classification) benchmark; low-resource but NLU-classification, not retrieval/similarity.
- Huang_Wang NASH — numerically-aware similarity for financial NLP; numbers-focused.
- Suarez_Luger CommonLID — web language identification; high-resource web LID.
- Song_Wu EDIR — composed image retrieval benchmark; multimodal image editing.
- Li_Su PaperRegister — flexible-grained academic paper search; modern.
- Yang_Jin ChangJuan — book-length *modern* Chinese story evaluation; not classical.
- Hou_McAuley BLaIR — LLMs as semantic encoders for recommendation; recsys.
- Grossman_Chen — zero-shot LLM readability assessment; readability.
- Merdjanovska_Rücklé — LLM confidence-estimate sparsity for classification; confidence-calibration (mild llm-annot adjacency, not adopted).
- AlKautsar_Koto ArabCulture-Dialogue — Arabic MSA+dialect cultural-reasoning QA; Semitic but cultural-QA/MT.
- Abootorabi_Asgari BloomBench — English-Arabic multimodal VLM cognition benchmark; Semitic but VLM-reasoning eval.
- The remaining ~270 files are LLM benchmarks for reasoning / agents / coding / medical / legal / theory-of-mind / reward-modeling / LLM-as-judge domains / multimodal / persona / safety — out of profile.
