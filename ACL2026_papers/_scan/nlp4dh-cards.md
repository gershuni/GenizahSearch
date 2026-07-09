# NLP4DH 2026 — relevance cards for the Genizah text-reuse program

Scope: entire NLP4DH workshop (36 papers under `Workshops/NLP4DH/`, posters ignored).
Every paper's page-1 abstract was extracted and read; A/B candidates were deep-read
including their references. Anthology IDs derived from proceedings page order
(`2026.nlp4dh-1.N`). Paper 1.15 (Cohen et al., Classical Tibetan) is our reference
point and was analyzed separately — noted in the C section.

Counts: scanned 36 · abstracts read 36 · A = 4 · B = 8 · C = 24 (incl. 1.15 already covered)

---

# A — act on this

### Quantifying Text Reuse Across Three Kṛṣṇa Yajurveda Recensions Using Multi-Algorithm Computational Collation
- **id**: 2026.nlp4dh-1.5 · NLP4DH · **file**: Workshops/NLP4DH/Miyagawa_Kyogoku_Quantifying_Text_Reuse_Across_Three_K_a_Yajurveda_Recen.pdf
- **authors/lab**: So Miyagawa (U. Tsukuba) et al., + Kyoko Amano (Kyoto), Leipzig — an active Sanskrit/Coptic digital-philology group with a track record in text-reuse (Miyagawa's Coptic Shenoute-Bible reuse work).
- **tl;dr**: They quantify text reuse across three Yajurveda recensions (MS/KS/TS) using ICoMa, a new browser-based multi-algorithm collation tool that runs five independent similarity metrics (incl. Jaccard, Levenshtein, Smith-Waterman) so conclusions aren't method-dependent. All five agree MS–KS is closest; crucially, the two ritual sections show very different reuse profiles (Punarādhāna near-identical 93.5% MS–KS vs. broadly-distributed Agnyupasthāna), read as differing transmission/fixation histories.
- **relevance**: [reuse, ancient, scale] — This is exactly our witness-census + collation problem framed as multi-algorithm robustness. Their "no single metric" principle validates our banded-Levenshtein + char-5-gram + Track-1 asymmetric-matcher stack; their per-section reuse-profile contrast maps to our per-domain DF policy (liturgy vs. non-liturgy behaving differently) and to motif decomposition. **ICoMa explicitly supports the Hebrew script** with script-appropriate tokenization/diacritic handling — a directly inspectable reference implementation.
- **stealable**: The five-algorithm agreement design as a robustness check we could bolt onto our census; the framing of "overlap % per section" as evidence for datable transmission layers; ICoMa's Hebrew-aware tokenization is worth a look.
- **priority**: A
- **bib leads**: Miyagawa 2022, "Shenoute, Besa and the Bible: Digital Text Reuse Analysis" (Coptic monastic reuse — closest analog to Genizah reuse); Miyagawa, Kyogoku, Tsukagoshi & Amano 2024, "Exploring similarity measures and intertextuality in Vedic Sanskrit" (NLP4DH); Nehrdich, Hellwig & Keutzer 2024, "ByT5-Sanskrit: one model for Sanskrit NLP" (byte-level model for a morphologically dense sacred corpus); Smith & Waterman 1981 (local alignment — verify against our seed-and-extend).

### Evaluating Latin and Ancient Greek Sentence Alignment through Parallel Sentence Mining
- **id**: 2026.nlp4dh-1.11 · NLP4DH · **file**: Workshops/NLP4DH/Reichbauer_Fraser_Evaluating_Latin_and_Ancient_Greek_Sentence_Alignment_t.pdf
- **authors/lab**: Sebastian Reichbauer, Shu Okabe, Alexander Fraser (TUM / Munich Center for ML) — Fraser's group is a strong low-resource / cross-lingual MT lab; worth a contact for the JA↔Hebrew alignment problem.
- **tl;dr**: A synthetic benchmark (2,000 translation pairs hidden in a 23k corpus) to evaluate cross-lingual Greek↔Latin parallel-sentence mining. They compare six models, then improve alignment with post-processing, fine-tuning, and knowledge distillation. The winner: whitening transformation + knowledge distillation lifts SPhilBERTa to a 97.6 mining score. They use CSLS instead of cosine to beat the hubness problem, and pin poor cross-lingual alignment on embedding anisotropy.
- **relevance**: [reuse, xlingual, embed, ancient] — This is our Judeo-Arabic↔Hebrew translation-detection problem (Track-3, cross-lingual edge) in a low-resource ancient pair with no parallel data. The whitening + distillation recipe and CSLS-over-cosine mining are directly transferable to building a JA↔Hebrew aligner from scratch. The synthetic "needles in a corpus" benchmark is a cheap way to evaluate our own cross-lingual matcher without gold parallels.
- **stealable**: (1) CSLS metric over cosine for mining (mitigates hubness); (2) whitening transform to de-anisotropize a multilingual encoder *without* parallel data; (3) English-anchored knowledge distillation to create a bilingual embedder; (4) the synthetic-injection mining benchmark design.
- **priority**: A
- **bib leads**: Yousef et al. 2022, "Ugarit: automatic model + gold standard for translation alignment of Ancient Greek" (a translation-alignment tool we should study for JA↔Hebrew); Riemenschneider & Frank 2023b, "Graecia capta… detecting Latin allusions to Ancient Greek" (SPhilBERTa; cross-lingual *allusion* detection = our exact task); Okabe, Hämmerl & Fraser 2025, "Improving parallel sentence mining for low-resource and endangered languages"; Hämmerl et al. 2023, "Exploring anisotropy and outliers in multilingual LMs for cross-lingual STS"; Huang et al. 2021, "WhiteningBERT"; Coffee et al. 2013, "Tesserae project" (the classic intertextuality-detection system).

### Modeling the Dalet Clitic in Historical Hebrew Texts: A New Prefix-Segmented BERT Model and Stylistic Analysis
- **id**: 2026.nlp4dh-1.12 · NLP4DH · **file**: Workshops/NLP4DH/Tal_Shmidman_Modeling_the_Dalet_Clitic_in_Historical_Hebrew_Texts_A.pdf
- **authors/lab**: Rachel Tal, Cheyn Shmuel Shmidman, **Avi Shmidman (Bar-Ilan / DICTA)** — this is our own house (gershuni@account.dicta.org.il); one cited work (MsBERT) lists Hillel Gershuni as co-author. Highest-priority internal contact.
- **tl;dr**: They release a new historical-Hebrew BERT in which *all prefixes are segmented into independent tokens*, letting the model reason at the proclitic level. Using an unsupervised MLM-probe method they disambiguate the Aramaic dalet clitic (subordinator vs. possessive) at F1 > 0.89 with no supervised training, then run it over 300M+ words of historical Hebrew to surface geographic/diachronic/genre stylistic clusters. Model + annotated dataset released for unrestricted use.
- **relevance**: [semitic, embed, product] — A ready-to-use, released historical-Hebrew encoder trained on our exact register (Responsa 21%, Talmud commentary, halakha, medieval + modern rabbinic — the Genizah's downstream literature). Prefix-segmentation directly attacks the Hebrew-morphology matching pain in our search/tokenizer (`hebword`, `content_search` diacritic-fold), and the MLM-probe trick is a zero-annotation way to disambiguate clitics/orthographic ambiguities. Their 300M-word corpus with region/period/genre metadata is a candidate silver-source and stylistic-clustering blueprint for our census.
- **stealable**: The prefix-segmented BERT itself (evaluate as a Track-3 / re-ranker backbone for Hebrew); the unsupervised "probe the MLM head" method for grammatical/orthographic disambiguation without labels; the IDW geographic-interpolation + stylistic-clustering pipeline for presenting corpus-scale findings.
- **priority**: A
- **bib leads**: Shmidman et al. 2024a, "MsBERT: reconstruction of lacunae in Hebrew manuscripts" (ML4AL — *directly* our fragmentary-text restoration problem; user is a co-author); Shmidman et al. 2022, "BEREL: BERT embeddings for rabbinic-encoded language"; Shmidman, Shmidman & Koppel 2023, "DictaBERT"; Shmidman, Shmidman, Koppel & Tsarfaty 2024b, "MRL parsing without tears: the case of Hebrew"; Schick & Schütze 2021 (cloze/PET — the MLM-probe precedent).

### Matching Meaning at Scale: Evaluating Semantic Search for 18th-Century Intellectual History through the Case of Locke
- **id**: 2026.nlp4dh-1.20 · NLP4DH · **file**: Workshops/NLP4DH/Wu_Tolonen_Matching_Meaning_at_Scale_Evaluating_Semantic_Search_fo.pdf
- **authors/lab**: Yu Wu, Ananth Mahadevan, Filip Ginter (TurkuNLP), Mikko Tolonen (Helsinki COMHIS) — a leading historical text-reuse-at-scale group (ECCO/EEBO, impresso adjacency).
- **tl;dr**: A proof-of-concept evaluation of off-the-shelf semantic search vs. lexical text-reuse detection for tracing Locke's reception. They build a hierarchical "reception" taxonomy (verbatim reuse / paraphrase / implicit meaning-match / mere topical) and an iterative expert-annotation workflow. Semantic search surfaces an order of magnitude more implicit reception than string matching — but a "lexical gatekeeping" effect persists: dense retrieval is still constrained by surface vocabulary overlap even though it's robust to OCR noise and syntactic divergence.
- **relevance**: [reuse, embed, product, scale] — This is the precise argument for our Track-3 (semantic layer beyond the lexical engine) with an honest ceiling stated: embeddings help most for paraphrase but don't escape lexical bias. Their reception taxonomy is a ready-made label schema for the silver/gold intertextuality pairs we need for Track-3, and their diagnostic (measure vocab-overlap vs. OCR-noise vs. syntax to expose gatekeeping) is a test we should run on our own retriever. "Robust to OCR noise" is directly encouraging for our 16-20% CER setting.
- **stealable**: The 4-tier reception taxonomy (verbatim/paraphrase/implicit/topical) as our annotation rubric; the "lexical gatekeeping" diagnostic (decompose retrieval success by vocab overlap / OCR noise / syntactic divergence); the iterative annotate-on-non-lexical-hits evaluation workflow.
- **priority**: A
- **bib leads**: Mahadevan, Mathioudakis, Mäkelä & Tolonen 2025, "Text reuse in large historical corpora: optimization of a data-science system" (their scaling engine — compare to our DF-banded generation); Düring et al. 2023, "Impresso Text Reuse at Scale" (interface + method for noisy historical newspapers); Franklin et al. 2024, "News Deja Vu: Connecting Past and Present with Semantic Search"; Kanerva et al. 2025, "Semantic search as extractive paraphrase span detection"; Vesanto et al. 2017 (OCR-resilient text reuse); Douze et al. 2025, "The FAISS Library" (ANN backbone for scale).

---

# B — awareness / design value

### From OCR to Analysis: Tracking Correction Provenance in Digital Humanities Pipelines
- **id**: 2026.nlp4dh-1.1 · NLP4DH · **file**: Workshops/NLP4DH/Guo_Wei_From_OCR_to_Analysis_Tracking_Correction_Provenance_in.pdf
- **authors/lab**: Haoze Guo, Ziqi Wei (U. Wisconsin–Madison).
- **tl;dr**: A provenance-aware framework for OCR-corrected corpora that records every correction as a *base-anchored span edit* (edit type, source, confidence, review status), so downstream results stay auditable and replayable. Pilot: downstream NER changes substantially across raw / fully-corrected / provenance-filtered text, and provenance signals flag unstable outputs for prioritized human review.
- **relevance**: [noise, product] — We run a 16-20% CER corpus with a community corrections layer (Supabase) feeding retrieval. Their base-anchored span-edit schema is a clean model for tracking HTR corrections without destroying the original, and their finding that correction pathways move entity/retrieval results argues for keeping raw+corrected variants queryable rather than overwriting.
- **stealable**: The base-anchored span-edit provenance schema (edit type/source/confidence/review status) + "policy-driven variant construction" (query raw vs. corrected on demand) for our corrections pipeline.
- **priority**: B
- **bib leads**: Lyu et al. 2021, "Neural OCR post-hoc correction of historical corpora" (TACL); Nguyen et al. 2021, "Survey of post-OCR processing approaches"; Boroş et al. 2020, "Alleviating digitization errors in NER for historical documents"; Ehrmann et al. 2024, "NER and classification in historical documents: a survey" (HIPE).

### MADRAG: Multi-Agent Debate with Retrieval-Augmented Generation for Training-Free Analytic Essay Scoring
- **id**: 2026.nlp4dh-1.30 · NLP4DH · **file**: Workshops/NLP4DH/Keramati_Warschauer_MADRAG_Multi_Agent_Debate_with_Retrieval_Augmented_Gene.pdf
- **authors/lab**: Ali Keramati, Mark Warschauer et al. (UC Irvine).
- **tl;dr**: A training-free LLM-scoring framework that replaces the single LLM-judge with an Advocate/Skeptic/Judge debate, where the Judge is augmented with rubric-aligned exemplar retrieval for calibration. Beats prompt-based baselines, approaches supervised systems; ablations show retrieval drives calibration and debate improves reasoning, while mitigating central-tendency (middle-score) bias.
- **relevance**: [llm-annot, product] — Our Track-3 plan leans on LLM-as-annotator for silver intertextuality labels (like the Tibetan reference paper's judge committee + best-worst scaling). MADRAG is a concrete recipe to make that annotator less biased and more calibrated: debate + retrieval of scored exemplars. The central-tendency-bias mitigation matters when we grade paraphrase-similarity on a scale.
- **stealable**: Advocate/Skeptic/Judge decomposition + rubric-aligned exemplar retrieval to calibrate our LLM annotator; the observation that retrieval (not debate) drives calibration — so invest in an exemplar bank of scored pairs.
- **priority**: B
- **bib leads**: Chan et al. 2023, "ChatEval: better LLM evaluators through multi-agent debate"; Du et al. 2024, "Improving factuality and reasoning through multiagent debate"; Li et al. 2025, "Evaluating scoring bias in LLM-as-a-judge"; Shibata & Miyamura 2025, "LCES" (pairwise/comparative scoring — relates to best-worst scaling); Hicke et al. 2025, "Says who? Effective zero-shot annotation of focalization."

### Twenty's Plenty: Semantic Scaffolding and Span Architecture for 19-Label NER in Medieval Latin Charters
- **id**: 2026.nlp4dh-1.22 · NLP4DH · **file**: Workshops/NLP4DH/Kovacs_Vogeler_Twenty_s_Plenty_Semantic_Scaffolding_and_Span_Architect.pdf
- **authors/lab**: Tamás Kovács, Giuseppe Consolo, Georg Vogeler (U. Graz, Digital Humanities).
- **tl;dr**: High-quality 19-label NER over medieval Latin charters from only ~298 training sentences. "Semantic scaffolding" = feeding rich *English* label descriptions as GLiNER prompts to activate latent multilingual knowledge (80.8% F1, no oversampling). A custom span bi-encoder (XLM-RoBERTa-large + attention-pooled spans + frozen BGE-M3 label vectors + asymmetric focal-Dice loss + InfoNCE + hard-negative mining) reaches 83.4% F1. Notably, domain pretraining gives *no* advantage once task fine-tuning is applied.
- **relevance**: [ancient, product] — Directly applicable to NER over our catalog/metadata and manuscript text (persons, places, shelfmarks, titles, dates) where annotation is scarce and expert-expensive. The "domain pretraining doesn't help post-fine-tune" finding is a useful budget signal; the English-label-scaffold trick is a cheap cross-lingual boost for Hebrew/JA labels.
- **stealable**: Semantic scaffolding (verbose English label descriptions as prompts) for low-annotation NER; asymmetric focal-Dice + InfoNCE + hard-negative-mining span head to handle rare classes; the "few hundred sentences is enough" data-budget target.
- **priority**: B
- **bib leads**: Zaratiana et al. 2024, "GLiNER" (label-as-prompt NER); Chen et al. 2024, "BGE-M3" (multilingual multi-granularity embeddings — candidate label/text encoder); Stepanov et al., "The Million-Label NER: GLiNER bi-encoder at scale"; Chastang et al. 2021, "A NER model for medieval Latin charters."

### Data Contamination in Neural Hieroglyphic Translation: A Reproducibility Study
- **id**: 2026.nlp4dh-1.6 · NLP4DH · **file**: Workshops/NLP4DH/Toutou_Basta_Data_Contamination_in_Neural_Hieroglyphic_Translation_A.pdf
- **authors/lab**: Ammar Toutou, Abdelrahman Harb (AIU, Egypt), Christine Basta (HiTZ / Alexandria).
- **tl;dr**: Reproducing a 61.5-BLEU hieroglyphic→German result yields only 37.0. The gap is data contamination: 32% of test targets appear verbatim in training because *formulaic* ancient corpora distribute identical passages across a random split. Contaminated samples score 29-47 BLEU higher than clean ones across architectures. Document-level dedup is insufficient — *target-level* dedup is required. They release a decontaminated test set + 8-gram-overlap contamination-detection scripts.
- **relevance**: [ancient, scale, noise] — A direct warning for our Track-3 benchmark construction. The Genizah is saturated with formulaic material (liturgy/piyyut, standard halakhic formulae, the Temple-Scroll-class repetition we already fight) — a naive split of silver pairs mined by our lexical engine will leak near-identical passages train↔test and inflate every metric. Their audit protocol is exactly the eval hygiene we should adopt.
- **stealable**: Character 8-gram-overlap thresholding to detect train/test leakage; enforce *target-level* (not just document-level) dedup when splitting; report a "clean vs. contaminated" metric range rather than a single inflated number.
- **priority**: B
- **bib leads**: Gutherz et al. 2023, "Translating Akkadian to English with NMT" (PNAS Nexus — ancient Semitic MT, closest to our JA); Magar & Schwartz 2022, "Data contamination: from memorization to exploitation"; Kocyigit et al. 2025, "Overestimation in LLM evaluation… data contamination's impact on MT"; Sommerschield et al. 2023, "Machine learning for ancient languages: a survey"; Abbas et al. 2026, "Obscuring data contamination through translation: evidence from Arabic corpora."

### Measuring Embedding Sensitivity to Authorial Style in French: Comparing Literary Texts with Language Model Rewritings
- **id**: 2026.nlp4dh-1.8 · NLP4DH · **file**: Workshops/NLP4DH/Icard_Ganascia_Measuring_Embedding_Sensitivity_to_Authorial_Style_in_F.pdf
- **authors/lab**: Benjamin Icard, Jean-Gabriel Ganascia et al. (LIP6, Sorbonne).
- **tl;dr**: Using a controlled French literary dataset (originals + fixed-topic LLM rewrites), they quantify how much *authorial style* (vs. semantic content) is encoded in embeddings via changes in embedding dispersion. Embeddings reliably capture stylistic features, and that signal persists after LLM rewriting, with model-specific patterns.
- **relevance**: [embed] — Cautionary for Track-3: if our intertextuality embeddings absorb scribe/genre/register style, style will confound *semantic* paraphrase similarity (two liturgically-styled but unrelated passages could look "similar"). Their dispersion metric is a way to audit whether our encoder is matching meaning or manner, and points to content-controlled training to separate the two.
- **stealable**: Embedding-dispersion-under-controlled-variation as a probe for style-vs-semantics leakage in our retriever; the content-controlled-training idea (Wegmann) to decontaminate semantic similarity from style.
- **priority**: B
- **bib leads**: Wegmann et al. 2022 (content-controlled training improves style–topic separation); Terreau et al. 2021 (does the embedding space encode style or semantics?); Patel et al. 2023, "LISA" (interpretable style-dimension embeddings); Kim, Zhang & Jurgens 2025, "Leveraging multilingual training for authorship representation."

### Perspectives — Interactive Document Clustering for Qualitative Data Analysis
- **id**: 2026.nlp4dh-1.36 · NLP4DH · **file**: Workshops/NLP4DH/Fischer_Biemann_Perspectives_Interactive_Document_Clustering_for_Qualit.pdf
- **authors/lab**: Tim Fischer, Chris Biemann (LT Group, U. Hamburg).
- **tl;dr**: A human-in-the-loop document-clustering tool where scholars define an analytical "lens" via rewriting prompts + instruction-based embeddings, then interactively refine clusters on a 2D map, with optional embedding fine-tuning to align with user intent.
- **relevance**: [product, embed] — A UX/architecture template for adding semantic exploration/clustering to the GenizahSearch web app on top of noisy embeddings, plus the instruction-embedding + fine-tune-from-user-feedback loop we'd want for Track-3 retrieval that adapts to a scholar's research question.
- **stealable**: Instruction/aspect-conditioned embeddings + "define the lens before clustering"; the HITL refine-and-fine-tune loop; interactive 2D map (à la Nomic Atlas) as a corpus-exploration surface.
- **priority**: B
- **bib leads**: Su et al. 2023, "INSTRUCTOR: One Embedder, Any Task"; Viswanathan et al. 2024, "LLMs enable few-shot clustering"; Wang et al. 2024, "Multilingual E5 Text Embeddings" (candidate multilingual backbone); Muennighoff et al. 2023, "MTEB" (embedding-model selection).

### Beyond Prompt-Sensitive Emotion Words: Stable Embeddings for Tang Poetry Analysis
- **id**: 2026.nlp4dh-1.7 · NLP4DH · **file**: Workshops/NLP4DH/Zhang_Li_Beyond_Prompt_Sensitive_Emotion_Words_Stable_Embeddings.pdf
- **authors/lab**: Linyue Zhang, Feiyue Li.
- **tl;dr**: LLM one-word emotion labels for Classical Chinese poetry are highly prompt-sensitive (only 50.3% A/B agreement across prompt phrasings), undermining reproducibility. Their fix: treat continuous hidden-state embeddings as the primary signal — cluster embeddings, then consolidate labels — which is stable (normalized entropy 0.989, all 20 clusters active).
- **relevance**: [embed, llm-annot] — Reinforces a Track-3 design choice: prefer embedding-based silver signals over prompt-generated discrete labels, because prompt sensitivity will corrupt any LLM-annotator we build for intertextuality grading. Their "represent-then-consolidate" order is a reproducibility discipline for our annotation pipeline.
- **stealable**: The represent-first (cluster embeddings) → consolidate-labels-last workflow; report A/B prompt-agreement as a reproducibility metric before trusting LLM labels.
- **priority**: B
- **bib leads**: Buechel & Hahn 2017, "EmoBank" (dimensional vs. categorical annotation — relevant to graded-similarity vs. binary reuse labels); Bamman, Underwood & Smith 2014 (latent literary structure at scale).

### Evaluating Open-Source LLMs for Text Summarization and NER in Apartheid Witness Reports
- **id**: 2026.nlp4dh-1.35 · NLP4DH · **file**: Workshops/NLP4DH/Kister_Schirmer_Evaluating_Open_Source_LLMs_for_Text_Summarization_and.pdf
- **authors/lab**: Pauline Kister (TU Munich), Miriam Schirmer (Northwestern).
- **tl;dr**: Zero-shot open-source LLMs applied to South African TRC testimonies for abstractive summarization + fine-grained NER (roles, violation types). Summarization is strong (BERTScore up to 0.77, beating non-LLM baselines) but NER stays weak (F1 ≤ 0.61); a two-stage summarize-then-NER-on-summaries pipeline measurably improves results. Reads as a fluency-over-factual-precision tradeoff.
- **relevance**: [product, noise] — Awareness for making our unstructured archival material (catalog free-text, scholarly descriptions, testimonies) searchable via LLM summarization + NER. The two-stage pipeline and the honest "NER is the weak link" finding temper expectations for entity extraction over messy historical prose.
- **stealable**: The two-stage summarize→NER-on-summary pipeline as a cheap accuracy lift; open-source-LLM zero-shot baselines + BERTScore/F1 as an evaluation template for archive enrichment.
- **priority**: B
- **bib leads**: Ehrmann et al. 2024, "NER in historical documents: a survey" (HIPE); Boroş et al. 2020, "Alleviating digitization errors in NER for historical documents."

---

# C / skipped (peripheral — same keywords, different problem)

- **2026.nlp4dh-1.15 — Cohen et al., "Scaling Sentence Similarity for Classical Tibetan"** — ALREADY COVERED; this is our reference-point paper (Reichman U., Israel), analyzed in depth separately. Highly relevant (silver labels + LLM-judge committee + best-worst scaling); not re-carded here per scope.
- **1.24 — Barré, "In Search of Lost Adventure Novels"** — supervised genre retrieval + kNN-graph corpus refinement; French novels, no reuse/Semitic angle.
- **1.26 — Chatterjee et al., "Lost in Translation: Grammatical Gender Latin→Occitan"** — diachronic gender morphology; custom tokenizer note is the only mild overlap.
- **1.34 — Dinu et al., "Authorship Attribution… The Selfish Giant"** — stylometry/authorship of Wilde tales; off-problem.
- **1.18 — Fundal & Bizzoni, "Directional Alignment… Human–LLM Co-Writing"** — creative co-writing dynamics; off-problem.
- **1.29 — Gao & Brody, "Topological Invariance in Semantic Embeddings"** — preliminary persistent-homology of embeddings; cross-lingual translation-equivalence idea is adjacent but no usable method yet.
- **1.21 — Gordon, "Tracing Thematic Change in Early Science Fiction"** — diachronic LDA topic modeling; small-corpus method, off-problem.
- **1.17 — Griebel & Underwood, "Fluency and Faithfulness in… Literary Translation"** — translation-quality (translationese classifier / COMET); evaluation not detection.
- **1.23 — Grimes & Washington, "Artistic Interventions… Machinic Glossolalia"** — art project on LLM glossolalia; off-problem.
- **1.33 — Guhr, "Between Whispers and Screams"** — loudness-SD proxy for explicit content in romance novels; off-problem.
- **1.14 — Henriksson et al., "Register Mixing Is the Norm on the Web"** — LLM web-register segmentation; off-problem.
- **1.2 — Jeong & Choi, "Frequency Accelerates Semantic Change (Korean)"** — diachronic word-embedding law-of-conformity; off-problem.
- **1.3 — Jung et al., "Narrative Landscape: Mapping LLM Dispositions"** — LLM persona/disposition profiling; off-problem.
- **1.32 — Khan & Zhang, "StoicLLM"** — preference-optimization persona alignment on 300 Stoic examples; micro-dataset trick only mildly interesting.
- **1.31 — Lionnet-Rollin & Cafiero, "Never Care For What They Say"** — creepypasta cross-platform stylistics; off-problem.
- **1.19 — Puttick & El-Wazzi, "Bias Mitigation in Hiring NLP"** — fairness/debiasing on Norwegian bios; off-problem.
- **1.10 — Raihan & Zampieri, "Temporal Text Classification with LLMs"** — text dating; manuscript-dating is a real interest but this is clean English/Portuguese, weak transfer to noisy Hebrew.
- **1.27 — Schöffel & Garces Arias, "Traditional Taggers to LLMs: POS for Medieval Romance"** — POS tagging; recurring cross-lingual-transfer lesson but not our core task.
- **1.25 — Swearingen et al., "Educational Theory in Low-SES Contexts"** — social-science narrative analysis; off-problem.
- **1.16 — Tabuzo et al., "PHMartialLawNER (Tagalog)"** — historical-archive NER corpus; semi-automatic annotation pipeline is the only mild overlap.
- **1.28 — Tiwari, "Statistical Structure in Indus Sign Sequences"** — undeciphered-script entropy/BiLSTM; decipherment, not reuse.
- **1.13 — Wang & Lyu, "Beyond Genre Categories (film tropes)"** — trope community-detection on films; off-problem.
- **1.9 — Wimalasuriya et al., "Prompting the Past"** — Gen-AI image reconstruction for heritage; off-problem.
- **1.4 — Yeshpanov, "100,000+ Movie Reviews from Kazakhstan"** — Russian/Kazakh code-switched *sentiment* corpus; code-switching angle too thin for our JA needs.
