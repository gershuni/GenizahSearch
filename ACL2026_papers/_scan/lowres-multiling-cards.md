# ACL 2026 scan — Low-resource / Multilinguality / MT / Phon-Morph / Semantics

Scanner scope: 5 track folders (~185 papers, posters ignored). Triaged by title,
abstract-checked ~28, deep-read references for A + key B. Cards below: A first, then B,
then a `## C / skipped` list.

---

## A — act on this

### Information Representation Fairness in Long-Document Embeddings
- **id**: Findings of ACL 2026, pp. 4996–5028 · Multilinguality → Semantics · **file**: Tracks/Semantics/Schuhmacher_Clematide_Information_Representation_Fairness_in_Long_Document_Em.pdf
- **authors/lab**: Elias Schuhmacher, Andrianos Michail, Juri Opitz, Rico Sennrich, Simon Clematide — University of Zurich CL / **impresso** (historical-newspaper DH project). **Strong contact**: Clematide + Opitz + Sennrich work on historical + interpretable multilingual embeddings.
- **tl;dr**: Introduces a permutation-based framework showing SOTA embedding models systematically over-represent early segments and higher-resource-language segments in long, multi-segment documents; later + lower-resource segments become undiscoverable in embedding search. Root-causes positional bias to front-loaded attention in the pooling token and proposes an inference-time attention-calibration fix that redistributes attention and restores discoverability.
- **relevance**: [embed, product, xlingual, noise] — This is our Track-3 embedding-search failure mode made explicit: Genizah pages mix Hebrew/JA/Aramaic and our transcriptions are long/multi-segment, so a naive embedding index will silently bury JA/Aramaic segments and page-tails. Directly informs how we pool + index for corpus-scale semantic retrieval.
- **stealable**: the permutation-based discoverability audit (run it on our multilingual index) + the inference-time attention-calibration to de-bias long-doc embeddings, no retraining. Code released (impresso/fair-sentence-transformers).
- **priority**: A
- **bib leads**: Coelho et al. 2024 "Dwell in the Beginning: How LMs Embed Long Documents for Dense Retrieval"; Fayyaz et al. 2025 "Collapse of dense retrievers: short, early, and literal biases…"; Opitz, Moeller, Michail, Padó, Clematide 2025 "Interpretable text embeddings and text similarity explanation: a survey"; Günther et al. 2025 "Late chunking: contextual chunk embeddings for long-context"; Modarressi et al. 2025 "Nolima: long-context evaluation beyond literal matching".

### Multilingual Language Models Encode Script Over Linguistic Structure
- **id**: Proceedings of ACL 2026 (Long), pp. 33685–33719 · Multilinguality · **file**: Tracks/Multilinguality/Verma_Chakraborty_Multilingual_Language_Models_Encode_Script_Over_Linguis.pdf
- **authors/lab**: Aastha Verma, Anwoy Chatterjee, Mehak Gupta, Tanmoy Chakraborty — IIT Delhi.
- **tl;dr**: Using the LAPE activation metric + Sparse Autoencoders, shows multilingual LMs organize their internal language units around **orthography/script**, not abstract language identity: romanization produces near-disjoint representations (aligning with neither native script nor English), while word-order shuffling barely moves unit identity. Typological structure only emerges in deeper layers.
- **relevance**: [semitic, xlingual, embed] — Judeo-Arabic *is* Arabic written in Hebrew script — this predicts a multilingual encoder will cluster JA with Hebrew (shared script) rather than with Arabic (shared language), and that transliterating JA→Arabic script would create a disjoint, non-aligned representation. A design-critical finding for whether/how we normalize script before embedding for JA↔Hebrew/Arabic matching.
- **stealable**: the finding itself (don't romanize/transliterate blindly for cross-lingual matching); the LAPE + SAE probing recipe to test which of our languages actually share representation space in a candidate base model before we commit.
- **priority**: A
- **bib leads**: Artetxe, Ruder, Yogatama 2020 "On the cross-lingual transferability of monolingual representations"; Brinkmann et al. 2025 "LLMs share representations of latent grammatical concepts across typologically diverse languages"; Deng et al. 2025 "Unveiling language-specific features in LLMs via sparse autoencoders"; Andrylie et al. 2025 "Sparse autoencoders can capture language-specific concepts across diverse languages".

### Lost in Translation, and Found: Detecting and Interpreting Translation Effects
- **id**: Proceedings of ACL 2026 (Long), pp. 17172–17187 · Machine Translation · **file**: Tracks/Machine_Translation/Wein_Pacheco_Lost_in_Translation_and_Found_Detecting_and_Interpretin.pdf
- **authors/lab**: Shira Wein (U. South Florida) et al., w/ Maria Leonor Pacheco (CU Boulder). Wein works on cross-lingual/AMR semantics — potential contact.
- **tl;dr**: Fine-tunes foundation models to classify whether a text was originally written vs translated ("translationese"), hitting 94% Macro F1, and uses interpretability tools to show the signal is a bundle of subtle lexical/grammatical features (many matching translationese theory) that pretrained models already encode without fine-tuning.
- **relevance**: [xlingual, reuse, noise] — Our JA↔Hebrew problem includes deciding which Genizah texts are *translations* vs original compositions (e.g., a Hebrew rendering of a Judeo-Arabic work). A translationese classifier is a cheap first-pass signal to flag translated witnesses and prioritize cross-lingual reuse search, and to avoid training our embeddings on translation-warped text.
- **stealable**: the classifier recipe + the interpretability finding that a compact feature set carries the signal (so a small model suffices); useful as a preprocessing filter over our corpus.
- **priority**: A
- **bib leads**: Koppel & Ordan 2011 "Translationese and its dialects" (**Moshe Koppel, Bar-Ilan — Israeli contact**); Amponsah-Kaakyire et al. 2022 "Explaining translationese: why are neural classifiers better and what do they learn?"; Ilisei et al. 2010 "Identification of translationese: a machine learning approach"; Jalota et al. 2023 "Translating away translationese without parallel data"; Kim et al. 2024 "Threads of subtlety: detecting machine-generated texts through discourse motifs" (motif angle — cf. our motif decomposition).

### CLEAR: Cross-Lingual Enhancement in Retrieval via Reverse-training
- **id**: Proceedings of ACL 2026 (Long), pp. 347–362 · Multilinguality · **file**: Tracks/Multilinguality/Lee_Lim_CLEAR_Cross_Lingual_Enhancement_in_Retrieval_via_Revers.pdf
- **authors/lab**: Seungyoon Lee, Heuiseok Lim et al. — Korea University.
- **tl;dr**: A new contrastive loss for cross-lingual retrieval that uses a high-resource "bridge" passage (English) with a reverse-training scheme to strengthen target-language↔bridge alignment. Gains up to +15% in low-resource cross-lingual retrieval while *avoiding* the usual degradation on well-aligned languages (English).
- **relevance**: [xlingual, embed, product] — Directly the JA↔Hebrew (and Aramaic) cross-lingual retrieval setting with imbalanced resources. Track-3's cross-lingual arm needs exactly this: retrieve a Hebrew parallel to a JA passage without letting the better-resourced side dominate. Hebrew (or a pivot) plays the "bridge" role.
- **stealable**: the reverse-training loss formulation for cross-lingual alignment; the "protect the high-resource side while lifting the low-resource side" result as a training objective for our bi-encoder.
- **priority**: A
- **bib leads**: Chen et al. 2024 "M3-Embedding: multi-linguality/functionality/granularity via self-knowledge distillation" (candidate base model); Moreira de Souza et al. 2024 "NV-Retriever: improving text embedding models with effective hard-negative mining"; Huang, Yu, Allan 2023 "Improving cross-lingual IR on low-resource languages via optimal transport distillation"; Gao, Zhang, Han, Callan 2021 "Scaling deep contrastive learning batch size under memory limited setup".

### DiffCL: Difference-Aware Contrastive Learning with Multi-Level Semantic Modeling
- **id**: Findings of ACL 2026, pp. 19576–19589 · Semantics · **file**: Tracks/Semantics/Chen_Luo_DiffCL_Difference_Aware_Contrastive_Learning_for_Automa.pdf
- **authors/lab**: Lei Chen, BoYu Gao et al. — Jinan University (Guangzhou).
- **tl;dr**: For automatic answer grading, combines heuristic *graded* difference labels (semantic-deviation levels from student vs reference answer, injected as prompts) with dual contrastive objectives: InfoNCE enforcing consistency among identical-score samples + a **hierarchical contrastive constraint guided by score gaps** that separates scoring levels proportionally. Beats cross-entropy baselines, robust on small models.
- **relevance**: [embed, reuse, llm-annot] — This is the training recipe for Track-3's graded silver labels. Our lexical engine yields tiered matches (Tier A/B/C ≈ ordinal similarity); DiffCL shows how to turn ordinal/graded labels into an embedding objective (identical-tier consistency + gap-proportional separation) rather than binary positive/negative — the exact contrastive design question for paraphrase-level intertextuality.
- **stealable**: the graded/hierarchical contrastive loss (separation proportional to score gap) + injecting a coarse difference label as an input prompt; both map cleanly onto our tier labels. Code + data released.
- **priority**: A
- **bib leads**: Mohler & Mihalcea 2011 "Learning to grade short answer questions using semantic similarity + dependency graph alignments" (alignment-based similarity); Mohler & Mihalcea 2009 "Text-to-text semantic similarity for short answer grading". (Most other refs are education-domain — skip.)

---

## B — awareness / cite-worthy

### AEA: Adaptive Expert Allocation Improves Sentence Embeddings from MoE LLMs
- **id**: Proceedings of ACL 2026 (Long), pp. 28837–28851 · Semantics · **file**: Tracks/Semantics/Yang_Gu_AEA_Adaptive_Expert_Allocation_Improves_Sentence_Embedd.pdf
- **authors/lab**: Shufan Yang, Qing Gu et al. — Nanjing University.
- **tl;dr**: Extracts sentence embeddings directly from Mixture-of-Experts LLMs with no fine-tuning by allocating expert budget adaptively per-layer (by expert homogeneity) and per-token (by attention importance). Plug-and-play, no extra latency, consistent STS gains.
- **relevance**: [embed, product] — A zero-training way to get better sentence embeddings out of a MoE LLM we might already run — a cheap Track-3 baseline before we invest in contrastive fine-tuning.
- **stealable**: the layer- + token-wise adaptive expert allocation as an inference-time embedding extractor; plugs onto existing prompt-based embedding methods.
- **priority**: B
- **bib leads**: (STS/MoE-embedding line; nothing distinctive for our low-resource/historical setting — skip.)

### Why Mean Pooling Works: Quantifying Second-Order Collapse in Text Embeddings
- **id**: Proceedings of ACL 2026 (Long), pp. 47180–47201 · Semantics · **file**: Tracks/Semantics/Hara_Yokoi_Why_Mean_Pooling_Works_Quantifying_Second_Order_Collaps.pdf
- **authors/lab**: Tomomasa Hara, Kentaro Inui, Sho Yokoi et al. — Tohoku U / RIKEN / MBZUAI / NINJAL. (Strong embedding-geometry group — contact-worthy.)
- **tl;dr**: Proposes a metric for the information "collapse" mean pooling can cause (distinct token distributions → similar text vectors), then shows empirically the collapse is rare in modern encoders — and rarer in *contrastive fine-tuned* ones than in their backbones — and that robustness to collapse correlates with downstream performance.
- **relevance**: [embed] — Justifies pooling + contrastive-fine-tuning choices for Track-3, and gives a diagnostic to check whether our (noisy, HTR) text is one of the cases where mean pooling loses signal.
- **stealable**: the collapse metric as a QA check on our own encoder/pooling; the empirical case for contrastive fine-tuning over raw backbone pooling.
- **priority**: B
- **bib leads**: (embedding-geometry line; Enevoldsen et al. 2025 MMTEB for eval breadth.)

### Linear-Time and Constant-Memory Text Embeddings Based on Recurrent LMs
- **id**: Proceedings of ACL 2026 (Long), pp. 41459–41481 · Low-resource Methods · **file**: Tracks/Low_resource_Methods/Grantner_Flechl_Linear_Time_and_Constant_Memory_Text_Embeddings_Based_o.pdf
- **authors/lab**: Tobias Grantner, Martin Flechl (Dynatrace Research), Emanuel Sallinger (TU Wien / Oxford).
- **tl;dr**: Turns recurrent LMs (Mamba2, RWKV, xLSTM) into general-purpose text embedders via a "vertically chunked" inference strategy giving constant memory once input exceeds the chunk size — competitive embedding quality vs transformers at a fraction of the memory for long sequences.
- **relevance**: [scale, embed, product] — We need to embed ~948K long, multi-segment pages and do corpus-scale similarity; constant-memory long-doc embedding is directly relevant to the engineering budget of Track-3 at all-pairs scale.
- **stealable**: recurrent embedders + vertically-chunked constant-memory inference for cheap long-document embedding; the E5-Mistral-style training procedure applied to a recurrent backbone.
- **priority**: B
- **bib leads**: Cao et al. 2025 "Single-pass document scanning for question answering"; Gu & Dao 2024 "Mamba: linear-time sequence modeling with selective state spaces"; Enevoldsen et al. 2025 MMTEB.

### Label and Explanation Variation in LLM-Based Annotation (NLI case study)
- **id**: Proceedings of ACL 2026 (Long), pp. 16526–16543 · Semantics · **file**: Tracks/Semantics/Kulmizev_deMarneffe_Label_and_Explanation_Variation_in_LLM_Based_Annotation.pdf
- **authors/lab**: Artur Kulmizev, Marie-Catherine de Marneffe et al. — UCLouvain (CENTAL). (Kulmizev: typology/interpretability background.)
- **tl;dr**: Treats individual LLM generations as "annotators" and studies whether ensembles reproduce genuine human label variation. Ensembles can match human label *distributions* but show idiosyncratic disagreement patterns and far less stylistic diversity in explanations — so LLMs are useful annotation *tools* but not drop-in replacements where authentic human variation matters.
- **relevance**: [llm-annot] — We plan to build silver labels via LLM-as-annotator; this is the caution manual: sample across families/params/temperature to approximate a human distribution, and don't assume single-model judgments are calibrated. Shapes our committee-labeling design.
- **stealable**: the three sampling strategies (generation params / within-family size / cross-model pooling) as our committee-labeling protocol; the diagnostic that explanation diversity, not just label agreement, signals annotation quality.
- **priority**: B
- **bib leads**: Baumann et al. 2025 "LLM Hacking: quantifying the hidden risks of using LLMs for text annotation"; Chen, Peng, Korhonen, Plank 2025 "A rose by any other name: LLM-generated explanations are good proxies for human explanations to collect label distributions"; Giulianelli, Baan, Aziz, Fernández, Plank 2023 "What comes next? Evaluating uncertainty… against human production variability".

### Gained in Translation: Privileged Pairwise Judges Enhance Multilingual Reasoning (SP3F)
- **id**: Proceedings of ACL 2026 (Long), pp. 8687–8705 · Machine Translation · **file**: Tracks/Machine_Translation/Sutawika_Neubig_Gained_in_Translation_Privileged_Pairwise_Judges_Enhanc.pdf
- **authors/lab**: Lintang Sutawika, Graham Neubig et al. — CMU LTI.
- **tl;dr**: Improves multilingual reasoning with no target-language data via SFT on translated QA + self-play RL where a **pairwise judge is given the (English) reference as privileged information**, so it can still rank two imperfect responses. Beats fully post-trained models with <1/8 the data.
- **relevance**: [llm-annot, xlingual] — Our silver-labeling often has a *reference edition* in hand while candidate witnesses are noisy/partial. A privileged pairwise judge (judge sees the canonical text, ranks candidate matches/paraphrases even when none is exact) is a strong pattern for auto-ranking Track-1/Track-3 candidates and mining preference pairs.
- **stealable**: the privileged-reference pairwise judge as our candidate-ranking annotator; self-play preference pairs from its verdicts to train a reranker.
- **priority**: B
- **bib leads**: Swamy et al. 2024 (self-play pairwise feedback — cited as method base); Artetxe et al. 2023 "Revisiting machine translation for cross-lingual classification".

### PEAR: Pairwise Evaluation for Automatic Relative Scoring in MT
- **id**: Proceedings of ACL 2026 (Long), pp. 42189–42207 · Machine Translation · **file**: Tracks/Machine_Translation/Proietti_Post_PEAR_Pairwise_Evaluation_for_Automatic_Relative_Scoring.pdf
- **authors/lab**: Lorenzo Proietti (Sapienza Rome), Roman Grundkiewicz, Matt Post (Microsoft).
- **tl;dr**: A reference-free QE metric that reframes evaluation as **graded pairwise comparison** — given a source + two candidates, predict the direction *and magnitude* of the quality gap — trained on differences of human judgments with a sign-inversion regularizer (swap the pair → flip the sign). Beats larger single-candidate QE metrics with fewer params; doubles as an MBR utility.
- **relevance**: [llm-annot, embed, product] — Same shape as scoring two candidate parallels/paraphrases for a query passage. The graded-pairwise formulation + sign-inversion regularizer is a clean recipe for training a Track-3 reranker from pairwise (human or LLM) judgments.
- **stealable**: graded pairwise supervision (direction + magnitude) with the order-reversal sign-inversion regularizer; use the resulting scorer as an MBR-style reranking utility over candidate witnesses.
- **priority**: B
- **bib leads**: Zouhar et al. 2025 (metric meta-eval as systems converge); Proietti et al. 2025a/b (pairwise MT evaluation line).

### NSL-MT: Linguistically Informed Negative Samples for Low-Resource MT
- **id**: Findings of ACL 2026, pp. 9545–9560 · Machine Translation · **file**: Tracks/Machine_Translation/Keita_Le_NSL_MT_Linguistically_Informed_Negative_Samples_for_Eff.pdf
- **authors/lab**: Mamadou K. Keita, Christopher Homan, Huy Le — Rochester Institute of Technology (African-language NLP).
- **tl;dr**: Augments scarce parallel data with **synthetically generated grammar violations** of the target language and explicitly penalizes the model for assigning them high probability. 3–12% BLEU for strong models, 56–89% for weak ones, and a 5× data-efficiency multiplier (1k examples ≈ 5k normal).
- **relevance**: [embed, xlingual, semitic] — The hard-negative-mining bucket, but linguistically informed: the idea of generating *near-miss* violations as negatives transfers to building hard negatives for our contrastive embedding training (e.g., orthographic/morphological near-variants that are NOT genuine parallels), especially under our low-resource, high-orthographic-variation regime.
- **stealable**: rule-driven "negative space" generation (grammar/orthography violations) + an explicit penalty term; the 5× data-efficiency result as motivation for negatives over more positives.
- **priority**: B
- **bib leads**: Keita, Homan, Diarra 2025 "R2T: rule-encoded loss functions for low-resource sequence tagging"; Mallinson, Sennrich, Lapata 2017 "Paraphrasing revisited with neural machine translation"; Caswell et al. 2025 "SMOL: professionally translated parallel data for 115 under-represented languages".

### Paraphrasing as Zero-shot Translation with Feature-guided Diversity (ParaMNMT)
- **id**: Proceedings of ACL 2026 (Long), pp. 17211–17223 · Machine Translation · **file**: Tracks/Machine_Translation/Yan_Xu_Paraphrasing_as_Zero_shot_Translation_with_Feature_guid.pdf
- **authors/lab**: Ziyue Yan, Hongfei Xu et al. — Zhengzhou University.
- **tl;dr**: Uses a bidirectional multilingual NMT model directly as a paraphraser (ask it to "translate" input→input language) and adds copy / not-copy tags to control lexical overlap, using "not-copy" at inference to force lexical divergence. Produces paraphrases with higher semantic consistency + diversity than parabanks or LLMs; improves low-resource NLP as augmentation.
- **relevance**: [embed, xlingual] — A controllable generator of silver positives (semantically-equal, lexically-divergent) to train Track-3 for paraphrase-level intertextuality, and to produce controlled hard positives where lexical overlap is deliberately low (the paraphrase-not-quote case).
- **stealable**: the copy / not-copy tag trick to dial lexical overlap up/down when generating paraphrase training pairs.
- **priority**: B
- **bib leads**: Mallinson, Sennrich, Lapata 2017 "Paraphrasing revisited with NMT"; Thompson & Post 2020 (paraphrase-based MT metrics).

### DeReA: Improving Idiom Translation with Detect-Retrieve-Arbitrate Reasoning
- **id**: Proceedings of ACL 2026 (Long), pp. 6603–6621 · Machine Translation · **file**: Tracks/Machine_Translation/Jiang_Zhang_DeReA_Improving_Idiom_Translation_with_Detect_Retrieve.pdf
- **authors/lab**: Rongqing Jiang, Xuebo Liu et al. — Harbin Institute of Technology (Shenzhen) + Huawei.
- **tl;dr**: Three-stage framework: a preference-aligned detector flags idiomatic spans by reasoning over literal-vs-contextual conflict; a **fine-tuned embedding model retrieves canonical definitions from an external KB**; a dual-path arbitrator weighs retrieval-augmented vs direct translation. Introduces the low-contamination LoMI benchmark; +5.2 pts on GPT-5-mini.
- **relevance**: [reuse, embed, product] — Structurally our Track-1: detect a distinctive span → retrieve its canonical form from a reference KB → arbitrate. Idioms are fixed non-compositional MWEs, like our liturgical/formulaic phrases and motifs; the detect-then-match-against-canon loop and the fine-tuned retrieval embedding mirror our reference-edge/motif-decomposition design.
- **stealable**: the detect→retrieve-from-canonical-KB→arbitrate pipeline as a template for span-level reuse matching; a fine-tuned retrieval embedder over a canonical-form KB.
- **priority**: B
- **bib leads**: Dankers et al. 2022 (transformers over-compositionality on idioms); Baziotis et al. 2023 (idioms hurt MT).

### From Shijing to English and German: Resources and Evaluation for LLM Translation of Early Chinese Poetry
- **id**: Findings of ACL 2026, pp. 11143–11162 · Machine Translation · **file**: Tracks/Machine_Translation/Jiao_Sun_From_Shijing_to_English_and_German_Resources_and_Evalua.pdf
- **authors/lab**: Ying Jiao (KU Leuven / Leuven.AI), Meng Sun (Shanghai Int'l Studies U).
- **tl;dr**: Builds a line-by-line Chinese-English-German parallel corpus for the *Book of Songs* plus a fine-grained lexical KB for archaic expressions, and a hybrid eval that fuses knowledge-driven, rule-based, and LLM-as-judge signals — achieving markedly higher human correlation than BLEU/METEOR-type metrics for an ancient, formally-constrained text.
- **relevance**: [ancient, xlingual, llm-annot] — Close analog to medieval Hebrew/JA: archaic lexis, formulaic structure, scarce parallels, and the failure of surface metrics. The archaic-expression KB + hybrid (rule + KB + judge) evaluation is a method precedent for how we'd build and evaluate JA↔Hebrew parallel resources.
- **stealable**: line-by-line alignment protocol + an archaic-expression lexical KB feeding a hybrid rule/KB/LLM-judge evaluator (don't trust BLEU/chrF on archaic text).
- **priority**: B
- **bib leads**: Chakrabarty, Saakyan, Muresan 2021 "Don't go far off: an empirical study on neural poetry translation"; Chen et al. 2025 "Benchmarking LLMs for translating classical Chinese poetry (adequacy/fluency/elegance)"; Genzel, Uszkoreit, Och 2010 "'Poetic' statistical MT: rhyme and meter".

### ManCC: A Task-Anchored Benchmark for Manchu–Classical Chinese Cross-Lingual Modeling
- **id**: Findings of ACL 2026, pp. 27271–27292 · Multilinguality · **file**: Tracks/Multilinguality/Wang_Yin_ManCC_A_Task_Anchored_Benchmark_for_Manchu_Classical_Ch.pdf
- **authors/lab**: Meiqi Wang, Xiaoxin Sun et al. — Northeast Normal U. + others.
- **tl;dr**: First benchmark for an extremely-low-resource historical language pair: 16,627 Manchu–Classical-Chinese sentence pairs from a Qing-dynasty text, with a reproducible protocol pairing BLEU/chrF against a 3-dimension human assessment (fidelity/fluency/normativity). Finds broader multilingual pretraining aids transfer and that automatic metrics miss essential historical-translation errors.
- **relevance**: [ancient, xlingual] — Method precedent and cautionary result for building/evaluating our own historical low-resource parallel resources: BLEU/chrF are unsafe for archaic translation, so bake a structured human-eval rubric into our JA↔Hebrew evaluation from the start.
- **stealable**: the parallel-corpus-from-a-single-historical-text construction + the 3-axis human-eval rubric layered over automatic metrics.
- **priority**: B
- **bib leads**: Joshi et al. 2020 "The state and fate of linguistic diversity" (resource-tier framing, widely reused).

### TRIMIX: Efficient Low-Resource Language Adaptation via Multi-Source Dynamic Logit Fusion
- **id**: Proceedings of ACL 2026 (Long), pp. 4540–4557 · Multilinguality · **file**: Tracks/Multilinguality/Zhang_Feng_Efficient_Low_Resource_Language_Adaptation_via_Multi_So.pdf
- **authors/lab**: Chen Zhang, Yansong Feng et al. — Wangxuan Institute, Peking University.
- **tl;dr**: Test-time logit fusion that dynamically balances three sources — LRL competence (small continually-pretrained model), task competence (HRL instruction-tuned model), and scaling (large model) — needing no LRL task annotations, only cheap continual pretraining on raw text. Beats Proxy Tuning; key finding: prioritize the small LRL-specialist's logits over the large model.
- **relevance**: [semitic, product] — A recipe to adapt an LLM to Judeo-Arabic/Aramaic with only raw text (which we have in abundance) and no task labels — for downstream tasks (NER, normalization, JA↔Hebrew) without expensive annotation.
- **stealable**: the annotation-free TRIMIX logit-fusion adaptation, and the counter-intuitive rule to weight the small language-specialist over the big general model.
- **priority**: B
- **bib leads**: Ke et al. 2025 (LLM adaptation to LRLs); Proxy-Tuning line (Liu et al.).

### Kumatigi: Quality-Driven Data Augmentation for Low-Resource MT
- **id**: Findings of ACL 2026, pp. 24433–24446 · Low-resource Methods · **file**: Tracks/Low_resource_Methods/Cisse_Kumatigi_Quality_Driven_Data_Augmentation_for_Low_Resou.pdf
- **authors/lab**: Cheick Tidiani Cissé — Orange Research (French–Bambara).
- **tl;dr**: Ships a 67k quality-scored French–Bambara corpus and augmentation tailored to a morphologically-rich language with heavy **orthographic inconsistency**: dual generation (round-trip for fluency + back-translation preserving authentic vocabulary) plus linguistically-motivated orthographic-variation augmentation. +3–4 BLEU; augmentation alone adds +1–2 beyond clean parallel data.
- **relevance**: [noise, xlingual, semitic] — Hebrew/JA/Aramaic share the core problem: morphological richness + orthographic variability (plene/defective spelling, script-mixing) + noisy HTR. The orthographic-variation augmentation and quality-scored filtering transfer directly to making our matchers/embeddings robust to spelling variants.
- **stealable**: orthographic-variability augmentation as a robustness recipe; quality-scored filtering to weight training pairs; round-trip + back-translation dual-dataset generation.
- **priority**: B
- **bib leads**: Khayrallah et al. 2020 (paraphrase augmentation for low-resource MT); Masakhane/Orife et al. (African-language resource line).

### From Bytes to Subwords: Challenges of Input Representations in NLP
- **id**: Findings of ACL 2026, pp. 10911–10919 · Phonology/Morphology/WordSeg · **file**: Tracks/Phonology_Morphology_WordSeg/VanDerGoot_From_Bytes_to_Subwords_Challenges_of_Input_Representati.pdf
- **authors/lab**: Rob van der Goot — IT University of Copenhagen (MaChAmp / low-resource NLP). Potential contact.
- **tl;dr**: Position paper on input granularity: today's tokenizers are near-duplicates with little diversity; **consistent Unicode normalization removes useful signal** (hurts language classification) so it should be applied only when beneficial; and UTF-8 is neither efficient nor fair across scripts, motivating byte-encoding alternatives.
- **relevance**: [noise, semitic, morphology] — Our pipeline makes exactly these choices for a diacritic-heavy, non-Latin, multi-script (Hebrew/JA/Aramaic), HTR-noisy corpus. "Normalize only when beneficial" and the byte-fairness argument directly inform our orthographic/diacritic normalization and tokenizer choices for both matching and embeddings.
- **stealable**: the empirical caution against blanket Unicode normalization (test per-task) + awareness that tokenizer choice is under-explored for our scripts.
- **priority**: B
- **bib leads**: Gorman & Pinter 2025 "Don't touch my diacritics" (**directly our diacritics problem**); Ahia et al. 2023 "Do all languages cost the same? Tokenization in the era of commercial LMs"; Ansary et al. 2024 "Unicode normalization and grapheme parsing of Indic languages"; Bostrom & Durrett 2020 "BPE is suboptimal for LM pretraining".

### Just Use XML: Revisiting Joint Translation and Label Projection (LabelPigeon)
- **id**: Findings of ACL 2026, pp. 34461–34478 · Low-resource Methods · **file**: Tracks/Low_resource_Methods/K_Hatzel_Just_Use_XML_Revisiting_Joint_Translation_and_Label_Pro.pdf
- **authors/lab**: Thennal D K, Chris Biemann, Hans Ole Hatzel — Language Technology Group, U. Hamburg.
- **tl;dr**: Performs translation and span-label projection jointly via XML tags in a single generation, contradicting prior claims that joint decoding degrades translation. Improves translation quality across 11 languages and yields large cross-lingual transfer gains (up to +40.2 F1 on NER) across 27 languages.
- **relevance**: [xlingual, product] — Two uses: (1) port span annotations (named entities, cited-source spans, our reference-edge markup) across JA↔Hebrew for training catalog/archive NER cross-lingually; (2) a cheap way to bootstrap labeled data in a low-resource language from English span-annotated data.
- **stealable**: XML-tagged joint translation+projection as our label-transfer mechanism (no separate word-aligner); the result that it doesn't hurt translation quality.
- **priority**: B
- **bib leads**: Chen et al. 2023 (label projection via translation); Ebing & Glavaš 2025 (label projection methods).

---

## C / skipped

- Abagyan et al. "One Tokenizer To Rule Them All" (Cohere Labs, Hooker) — universal tokenizer gives +20% adaptation to new/unseen languages; noted for cheaply extending a model to Aramaic/JA, but generic tokenizer-design result. (contact: Cohere Aya team)
- MorLan/Goldman/Tsarfaty "Location Not Found" (LOCQA) — locale bias in multilingual LLMs; off-method, but **flag contact: Reut Tsarfaty (Bar-Ilan ONLP) + Omer Goldman + Guy Mor-Lan — Israeli Hebrew/Arabic-NLP group**.
- Hashiloni/Bar "ID10M-JAM" — adversarial idiom-ID benchmark; MWE relevance thin, but **flag contact: Kfir Bar, Reichman U. (Israeli Hebrew/Arabic NLP)**.
- Rei/Martins "TOWER+" — translation-specialized multilingual LLM recipe (Unbabel/COMET lab); high-resource-focused, reference recipe only if we build JA↔Hebrew MT.
- Tian/Guo "Beyond Literal Mapping" (MENT/RATE) — non-literal MT eval + LLM-judge inconsistency caveat; MT-eval-specific.
- Ponwitayarat "SEA-BED" — SE-Asian embedding benchmark; another "embeddings aren't uniform across langs/tasks" result, no transferable technique.
- Yukhymenko "Recovered in Translation" — automated benchmark-translation pipeline (T-RANK ranking); we detect existing translations, not produce them.
- Quaremba "Citation Needed Detection" — Wikipedia check-worthiness; same word "citation", different problem (not source-reuse citation).
- Liu/Zhang "Would LLMs be Good Historical Linguists" — sound-change/G2P for Chinese dialects via Middle Chinese; phonology, not text.
- Liu/Zhang "Uncertainty-Aware Contrastive Sentence Embedding" — contrastive but for text classification, not similarity/retrieval.
- Bouziane "Candidate-Aware Retrieval and Reranking" — multiple-choice QA reranking, not corpus retrieval.
- Issaka "African Languages Lab" — community/infrastructure paper, no transferable method.
- Li/Niehues "Multimodal ICL for ASR of Low-resource" — speech, not text.
- Miletic "Phonemes to the Rescue" — phoneme-based tokenization; speech/phonology-leaning.
- Choi "b d t p Self-supervised Speech Models Discover Phonology" — speech phonology.
- Land "Which Pieces Does Unigram Tokenization Really Need" — tokenizer-internals, no clear transfer.
- Lai / Jiang(Latent) / Yang(SAME) / Chowdhury / Pan / Ouyang / Zhang(Nakamura) — sign-language & speech/simultaneous translation, out of scope.
- Wu "Bootstrapping Code Translation" / Wu "Parallel SFT cross programming lang" / Zhu "Exons Detect" — code/genomic tokens.
- Most Low-resource-Methods track (Agarwal, Cai, Cheng×2, Das, Ding, Gandhi, Hong, Lee, Liang, Liu×4, Su, Wi, Xiao, Xie, Yang, Yao, Yun, Zhang×3, Zhouxuwen) — LLM efficiency/KV-cache/LoRA/RL/MoE/reasoning-distillation/KGQA; no reuse/embedding/Semitic/historical angle.
- Most Multilinguality track (Aggarwal, Chan, Chang, Chitale, Choo, Civelli, Dan, Fan, Gao, Gautam, Han, Jeong, Kang, Koo, Le, Li×5, Liu×2, Luo, Macko, Noel, Oh, Park, Qi, RoyDipta, Tatariya, Tran, Ukarapol, Vanmassenhove, Veselovsky, Wang(Schmitt), Wastl, Wu, Xia, Xu, Xuan, Zhang(Think), Zhao, Zhu) — multilingual reasoning/toxicity/bias/PII/MGT-detection/RAG/topic-models/instruction-tuning; keyword overlap only.
- Remaining Machine_Translation track (Bu, Chen(Recitation), Cheng, Dragomir, Guan, Han, Ito, Jiang(Breaking), Li×3, Liu(Verbal MWE), Liu×2, Luo, Man, Miao, Nigatu, Ren, Riley, Sayeedi, Schmidt, Selialia, Shen, Shi, Song, Sun, Tan×2, Thellmann, Wang×4, Wein-n/a, Ye×2, Yuan, Zhang×2, Zhou, Zong) — MT quality/RL/reward-hacking/eval-agents/simultaneous/document-level; no direct reuse-embedding transfer.
- Remaining Semantics track (Cho, Das, Ghosh, He, Huang, Jamshidi, Jiang(CodeRL), Jullien, Kang(WSDPO), Kundu, Lee(REZE), Li(ChatHLS), Ma, Mahrous, Shi(MSCode), Shijia, Storek, Wang(Sememe), Wang(LOTUS), Wei, Zhu, Zou) — metaphor/figurative/WSD/clinical-NLI/vision-language/code/security/finance; off-target.
- Phon/Morph remainder (Chen×2, Chizhov, Dwivedi, Huang, Jang, Liao, Shao, Yue) — tokenizer internals / phonological probing / code-LLM security; no Semitic-morphology or reuse angle.
