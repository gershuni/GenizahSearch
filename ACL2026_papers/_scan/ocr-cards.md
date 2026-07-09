# OCR / noise cluster — ACL 2026 relevance cards

Scope: `Topic_OCR/` (24 papers, posters/ ignored). Lens: bucket 3 (noise) — our
corpus is HTR at 16-20% letter CER, so the questions are: does noise break
retrieval/text-reuse, how do others post-correct / restore / evaluate under noise,
and what transfers to Semitic historical manuscripts. Abstracts extracted for all
24; A/B candidates deep-read incl. references.

---

## A — act on this

### When Good OCR Is Not Enough: Benchmarking OCR Robustness for Retrieval-Augmented Generation
- **id**: 2026.acl-industry.60 · Industry Track · **file**: `Topic_OCR/Sun_Zhang_When_Good_OCR_Is_Not_Enough_Benchmarking_OCR_Robustness.pdf`
- **authors/lab**: Lin Sun et al., Beijing Qiyuan Technology / Qihoo 360. (InduOCRBench, open at github.com/Qihoo360/InduOCRBench.)
- **tl;dr**: Builds InduOCRBench (570 docs / 3,402 pages, stratified from 10K industrial docs across 11 hard categories incl. "historical documents with non-standard reading orders"). Runs SOTA OCR through a controlled OCR-first RAG pipeline and shows the headline result: **high OCR accuracy (low CER/WER) does NOT translate into strong downstream retrieval** — structural/semantic errors cause disproportionate retrieval failures even at near-perfect character scores, and this holds stably across RAG architectures.
- **relevance**: [noise, product, scale] — This is the evaluation-philosophy paper for our whole noise story. We report a 16-20% CER and implicitly assume lower CER = better matching; this shows character-level metrics are the wrong yardstick and we should measure our Track-1 asymmetric matcher / seed-and-extend by **downstream witness-recall**, not edit-distance to a clean reference. Directly justifies an OCR-to-retrieval eval protocol for SEED-029 and warns that structural noise (reading order, fragmentation) — endemic to Genizah folios — is the real killer, not per-glyph CER.
- **stealable**: The OCR-to-retrieval evaluation protocol (stage-wise attribution: retrieval-side vs generation-side failures); the stratified-sampling recipe to build a high-signal eval set from a long-tailed corpus (10K → 570 balanced); the finding that lets us reframe our metrics from CER to recall@retrieval.
- **priority**: A
- **bib leads**:
  - Es et al. 2025, "RAGAS: Automated evaluation of retrieval augmented generation" — RAG eval baseline (assumes clean text; our gap).
  - Saad-Falcon et al. 2024, "ARES" (automated RAG eval).
  - Chen et al. 2024, "BGE-M3: multi-lingual multi-granularity text embeddings via self-knowledge distillation" — candidate multilingual retriever for Track-3.
  - Anand et al. 2023, "TC-OCR" + Kasem et al. 2022 survey — structural (table) OCR error → semantics literature.
  - Jin et al. 2025, "FlashRAG: modular toolkit for efficient RAG research" — reusable eval harness.

### Digitizing Nepal's Written Heritage: A Comprehensive HTR Pipeline for Old Nepali Manuscripts
- **id**: 2026.acl-long.671 · Main (Long) · **file**: `Topic_OCR/Sarawgi_Zotter_Digitizing_Nepal_s_Written_Heritage_A_Comprehensive_HTR.pdf`
- **authors/lab**: Anjali Sarawgi, Esteban Garces Arias, Christof Zotter — **LMU Munich (Statistics/MCML) + Heidelberg Academy of Sciences (Documenta Nepalica)**. A DH lab doing exactly our kind of low-resource historical HTR; **flag as potential contact** (Garces Arias has a whole line of historical-HTR + decoding papers — Old Occitan EMNLP 2023, adaptive contrastive search). Code at github.com/anjalisarawgi/nepOCR.
- **tl;dr**: First end-to-end HTR pipeline for Old Nepali (low-resource historical). Line-level; systematically compares vision-encoder+LM-decoder architectures (TrOCR, Swin+BERT/GPT-2), byte-level vs subword tokenization, data augmentation that preserves script integrity, and 6 decoding strategies. Best CER 4.9%; includes token-level confusion analysis of error patterns.
- **relevance**: [noise, ancient, semitic] — Closest methodological analog to our situation. The **token-level confusion analysis** is the actionable piece: we could build the equivalent Hebrew/Judeo-Arabic HTR confusion matrix (which letters our HTR swaps — ב/כ, ד/ר, ה/ח, final forms) and feed it as **substitution-cost weights into our banded Levenshtein verification**, making seed-and-extend noise-aware instead of using uniform edit costs. Their augmentation-that-preserves-script and domain-adaptation-via-pretraining also inform any future silver-data generation for Track-3.
- **stealable**: A per-character confusion matrix → weighted-edit-distance for our verifier; the tokenization-granularity ablation (byte vs subword) as precedent if we ever tokenize noisy Hebrew; script-preserving augmentation for synthetic-noise training data.
- **priority**: A
- **bib leads**:
  - Garces Arias et al. 2023 (EMNLP), "Automatic transcription of handwritten Old Occitan" — sibling low-resource historical HTR pipeline.
  - Souibgui et al. 2022, "…manuscripts with rare alphabets" — few-shot HTR for rare scripts.
  - Chammas et al. 2018, "Handwriting recognition of historical documents with few labeled data".
  - de Sousa Neto et al. 2024, "Data augmentation for offline HTR: a systematic review".
  - AlKendi et al. 2024, "Advancements and challenges in HTR: a comprehensive survey" (Journal of Imaging).

---

## B — awareness / cite-worthy

### HisDoc-OCR: Restoring Visual Grounding in MLLMs for Chinese Historical Document OCR
- **id**: 2026.findings-acl.301 · Findings · **file**: `Topic_OCR/Cao_Jin_HisDoc_OCR_Restoring_Visual_Grounding_in_MLLMs_for_Chin.pdf`
- **authors/lab**: Jiahuan Cao, Yongxin Shi, … Lianwen Jin (South China Univ. of Technology — a prolific historical-document / ancient-script OCR + LLM group; see their TongGu, oracle-bone work).
- **tl;dr**: Diagnoses that MLLM OCR on degraded historical Chinese hallucinates because it over-trusts linguistic priors over visual evidence (visual-textual misalignment). Fixes it with three training-time tricks: Layout Injection (2-D layout as delimiters in the text target), First-Occurrence Boost (up-weight vision-dependent first-seen characters), and Self-Distilled Attention Focusing.
- **relevance**: [noise, ancient, semitic] — The root-cause framing (a model leaning on language priors and fabricating plausible-but-wrong text over degraded ink) is exactly the failure mode we must guard against if we ever let an LLM/VLM read or "clean" Genizah images, and it warns that LLM-as-annotator over noisy transcripts (our Track-3 silver labels) can hallucinate agreement. First-Occurrence Boost = a principled way to force visual grounding on rare/first-seen glyphs, relevant to rare Judeo-Arabic forms.
- **stealable**: The "first-occurrence reweighting" idea (emphasize the first, hardest-to-guess occurrence of a token) as a training signal; using layout-aware delimiters in the transcription target so downstream reading-order stays intact (echoes the Sun/Zhang reading-order finding).
- **priority**: B
- **bib leads**:
  - Cao et al. 2024 (EMNLP), "TongGu: mastering classical Chinese with knowledge-grounded LLMs".
  - Cao et al. 2023, "Translating ancient Chinese to modern Chinese at scale" (LLM) — historical intra-language "translation" akin to our JA↔Hebrew reframing.
  - Liu et al. 2024, "SikuGPT: generative pre-trained model for ancient texts" (DH/Computing & Cultural Heritage).
  - He et al. 2025, "Seeing is believing? Mitigating OCR hallucinations in MLLMs" (arXiv 2506.20168).
  - Bai et al. 2024, "Hallucination of MLLMs: a survey".

### Benchmarking Vision-Language Models on Chinese Ancient Documents: From OCR to Knowledge Reasoning (AncientDoc)
- **id**: 2026.findings-acl.1438 · Findings · **file**: `Topic_OCR/Yu_Li_Benchmarking_Vision_Language_Models_on_Chinese_Ancient.pdf`
- **authors/lab**: Haiyang Yu et al., Fudan University + ByteDance. (bytedance.github.io/AncientDoc)
- **tl;dr**: AncientDoc — first benchmark for Chinese ancient documents, 14 doc types, 100+ books, ~3,000 pages, five tasks: page-level OCR, vernacular translation, reasoning-QA, knowledge-QA, and **linguistic-variant QA**. Evaluates mainstream VLMs with a human-aligned LLM scorer.
- **relevance**: [ancient, noise, product] — A template for how to build a graded benchmark over a historical corpus that goes beyond transcription into downstream tasks — a model for a future GenizahBench. "Linguistic-variant QA" maps to our orthographic/historical-variant handling; "vernacular translation" parallels JA↔Hebrew. The human-aligned LLM scorer is a concrete llm-as-judge design we could reuse for evaluating retrieval/paraphrase output.
- **stealable**: The five-task ladder (OCR → translation → reasoning → knowledge → variant) as our own eval scaffold; the human-aligned LLM scoring protocol for open-ended answers.
- **priority**: B
- **bib leads**:
  - Akoushideh et al. 2025, "Persian/Arabic scene text recognition with CRNN" — Semitic-script OCR.
  - Borchmann et al. 2021, "DUE: end-to-end document understanding benchmark" — benchmark-construction precedent.
  - Ding et al. 2024, "MVQA: multimodal information retrieval in PDF-based VQA".

### Beyond Atomic Characters: Glyph-Aware Sub-character Alignment for Low-Resource Multilingual OCR (BASA)
- **id**: 2026.acl-long.1392 · Main (Long) · **file**: `Topic_OCR/Zhu_Shi_Beyond_Atomic_Characters_Glyph_Aware_Sub_character_Alig.pdf`
- **authors/lab**: Mengxiao Zhu et al., North China Univ. of Technology + Minzu Univ. (MOE Ethnic Language lab) + BIT. (github.com/NcutLLM/BASA)
- **tl;dr**: Low-resource OCR framework whose Glyph-Aware Fine-grained Adapter aligns sub-character structural primitives (strokes/radicals) with visual features via learnable glyph prototypes, explicitly resolving confusions among visually similar characters. Adds a Glyph-Aware Reverse Synthesis pipeline for zero-cost, large-scale synthetic training corpora with automatic component labels; BASA-Bench spans 11 languages.
- **relevance**: [noise, semitic] — The core problem — errors among visually similar characters under weak linguistic priors — is precisely our Hebrew HTR confusion story (letters differing by a stroke/dot). The reverse-synthesis pipeline (render glyph components into synthetic noisy text at zero labeling cost) is a template for generating synthetic-HTR-noised Hebrew/JA to train a noise-robust Track-3 embedding or to stress-test seed-and-extend.
- **stealable**: Zero-cost synthetic-noise corpus generation via component/glyph recomposition; the sub-character (radical/stroke) similarity model → informs a Hebrew glyph-similarity prior for edit weights.
- **priority**: B
- **bib leads**:
  - Agarwal & Anastasopoulos 2024, "A concise survey of OCR for low-resource languages" (AmericasNLP) — directly on-point survey.
  - Feng et al. 2025, "Dolphin: document image parsing via heterogeneous anchor prompting".
  - Anuradha et al. 2021, "…image resolution and algorithmic complexity for Sinhala OCR" — resolution vs error study.

### Zero-shot Jianzi Recognition as Structured Visual Information Extraction
- **id**: 2026.acl-long.1356 · Main (Long) · **file**: `Topic_OCR/Li_Cheng_Zero_shot_Jianzi_Recognition_as_Structured_Visual_Infor.pdf`
- **authors/lab**: Zehan Li, Fu Zhang et al., Northeastern University (Shenyang).
- **tl;dr**: Recognizes Guqin Jianzi tablature (an open, unbounded compositional glyph system) as vision-to-sequence prediction of canonical component sequences under a zero-shot split. Builds Synthetic-JZ, synthesizes manuscript-like images via **component-wise style recomposition + manuscript-domain noise modeling**, fine-tunes a VLM, and at inference uses a **lightweight legality-guided correction module** that re-ranks decoding candidates to suppress structural hallucinations without touching the backbone. 63% seq-accuracy on real data, +35 over Gemini-3-Pro.
- **relevance**: [ancient, noise, product] — Two transferable tricks. (1) **Manuscript-domain noise modeling** to synthesize realistic degraded training data — the recipe we'd want for generating HTR-noised Hebrew to train/evaluate noise-robust matching. (2) **Legality-guided post-hoc re-ranking** — a constraint-checker that rescoring candidates against a legality/lexicon model — maps onto a post-correction re-ranker over our seed-and-extend candidates using a Hebrew/JA lexicon or the clean reference editions (Track-1) as the "legality" oracle.
- **stealable**: The inference-time legality-guided candidate re-ranker (no retraining) as a cheap post-correction layer keyed on our 8,300 clean reference editions; manuscript-domain noise modeling for synthetic degradation.
- **priority**: B
- **bib leads**:
  - Guan et al. 2024 (ACL), "Deciphering oracle bone language with diffusion models" (Lianwen Jin group) — ancient-script decipherment via generative models.
  - Ao et al. 2025, "Bayesian classifier calibration based on synthesized samples for zero-shot Chinese character recognition".
  - Bao et al. 2025 (EMNLP), "CalligraphicOCR for Chinese calligraphy recognition".

### The olmOCR Project: Building Fully Open OCR using VLMs
- **id**: 2026.acl-demo.62 · Demo · **file**: `Topic_OCR/Poznanski_Soldaini_The_olmOCR_Project_Building_Fully_Open_OCR_using_VLMs.pdf`
- **authors/lab**: Jake Poznanski, Kyle Lo, Luca Soldaini — **Allen Institute for AI (AI2)**. Fully open models + data + code.
- **tl;dr**: Open 7B VLM OCR: SFT on 260K diverse PDF pages, then RL with **visual unit tests** (binary structural-fidelity checks on tables/equations) used both as interpretable eval AND as direct RL reward. Ships olmOCR-Bench (1.4K challenging PDFs); SOTA among open systems and matching proprietary APIs at a fraction of cost; deployed to 100M+ PDFs to curate Olmo 3 pretraining data.
- **relevance**: [noise, product, scale] — Not Hebrew-tuned, so not a drop-in transcriber, but the **visual-unit-test-as-RL-reward** methodology is a clean, stealable idea: define binary structural checks (reading order preserved? line count matches? no repeated hallucinated runs?) and use them as both eval and optimization target — directly relevant given the Sun/Zhang finding that structural fidelity, not CER, drives downstream success. Also the reference open OCR stack if we ever want a VLM baseline.
- **stealable**: Visual/structural unit tests as an interpretable, low-cost eval harness (and RL signal) for transcription quality; the open olmOCR-Bench construction pattern (challenging-PDF curation).
- **priority**: B
- **bib leads**:
  - Blecher et al. 2023, "Nougat: neural optical understanding for academic documents".
  - He et al. 2025, "Seeing is believing? Mitigating OCR hallucinations in MLLMs" (2506.20168).
  - DeepSeek-AI 2025, "DeepSeek-OCR: contexts optical compression".
  - Kim et al. 2021, "OCR-free document understanding transformer (Donut)".

### Scaling Beyond Context: A Survey of Multimodal Retrieval-Augmented Generation for Document Understanding
- **id**: 2026.acl-long.204 · Main (Long) · **file**: `Topic_OCR/Gao_Gong_Scaling_Beyond_Context_A_Survey_of_Multimodal_Retrieval.pdf`
- **authors/lab**: Sensen Gao et al., MBZUAI + Alibaba + Tsinghua. (github.com/SensenGao/Multimodal-RAG-Survey-For-Document)
- **tl;dr**: Systematic survey of Multimodal RAG for document understanding; taxonomy by domain / retrieval-modality / granularity; reviews graph structures, agentic frameworks, datasets, benchmarks, deployment; flags open challenges in efficiency, fine-grained representation, and robustness. Notes OCR-based pipelines lose structural detail while MLLMs struggle with context.
- **relevance**: [product, scale, noise] — Orientation + bib-mining resource for the "RAG over a noisy Genizah corpus" product direction and for Track-3 retrieval design (granularity choices, robustness). Cite-worthy as the landscape survey; the retrieval-granularity taxonomy informs whether we embed at line / passage / folio level.
- **stealable**: The granularity taxonomy (page vs region vs element retrieval) as a design lens for our embedding units; robustness/efficiency open-problem list to position our contribution.
- **priority**: B
- **bib leads**:
  - Abootorabi et al. 2025, "Ask in any modality: a comprehensive survey on multimodal RAG".
  - Bach 2025, "Hierarchical patch compression for ColPali: efficient multi-vector document retrieval" — efficient visual retrieval at scale.
  - An et al. 2024, "GoldenRetriever: high-fidelity agentic RAG for industrial knowledge base".

### A Survey on MLLM-based Visually Rich Document Understanding
- **id**: 2026.findings-acl.652 · Findings · **file**: `Topic_OCR/Ding_Peng_A_Survey_on_MLLM_based_Visually_Rich_Document_Understan.pdf`
- **authors/lab**: Yihao Ding, Siwen Luo et al., Univ. of Western Australia + Melbourne + Weill Cornell.
- **tl;dr**: Survey of MLLM-based VRDU (OCR-based and OCR-free): techniques for fusing text/visual/layout features, training paradigms (pretraining, instruction tuning), and challenges — data scarcity, multi-page, multilingual, RAG, agentic frameworks.
- **relevance**: [product, noise] — Secondary orientation survey; useful mainly for its data-scarcity and multilingual sections and as a bib source. Lower unique value than the Gao MM-RAG survey but complementary on the OCR-free vs OCR-based tradeoff that Genizah faces.
- **stealable**: The OCR-free-vs-OCR-based decision framing for deciding whether to keep Dicta HTR text as the pipeline entry point or move to a native-visual retriever.
- **priority**: B
- **bib leads**:
  - Barboule et al. 2025, "Survey on QA over visually rich documents".
  - Chen et al. 2025, "MosaicDoc: a large-scale bilingual benchmark for VRDU".
  - Ding et al. 2025a, "SynDoc: hybrid discriminative-generative framework for synthetic document domain" — synthetic-doc generation.

### BanglaSTEM: A Parallel Corpus and Term-Weighted Evaluation for Technical Bangla-English Translation
- **id**: 2026.acl-srw.34 · Student Research Workshop · **file**: `Topic_OCR/Hasan_Adnan_BanglaSTEM_A_Parallel_Corpus_and_Term_Weighted_Evaluati.pdf`
- **authors/lab**: Kazi Reyazul Hasan et al., BUET (Bangladesh).
- **tl;dr**: Builds a 5,000-pair Bangla-English technical corpus by OCR-extracting matching passages from official bilingual textbooks, using LLMs to align sentences and mark technical terms, few-shot-generating 12K new pairs, then human-selecting the best 5K. Introduces a **term-weighted BLEU** that up-weights technical terms, showing it correlates with downstream accuracy better than standard BLEU (which scores wrong translations high).
- **relevance**: [xlingual, llm-annot, noise] — Two ideas for us. (1) The **LLM-align-then-human-curate** silver-labeling pipeline is a direct template for generating JA↔Hebrew translation-pair silver labels for Track-3. (2) **Term-weighted evaluation** parallels our instinct to weight distinctive/rare phrases higher when scoring text-reuse — a metric that discounts common-word matches and rewards distinctive-term matches (cf. our DF-cap / distinctive-phrase tiering).
- **stealable**: Term-weighted (DF-aware) match/eval metric; the LLM-align + few-shot-expand + human-select recipe for cheap parallel silver data.
- **priority**: B
- **bib leads**:
  - Goyal et al. 2022, "FLORES-101 evaluation benchmark for low-resource MT".
  - Hasan et al. 2020, "Aligner ensembling, batch filtering, new datasets for Bengali-English MT".
  - Chu & Wang 2018, "A survey of domain adaptation for NMT".

---

## C / skipped

- **APEX (Chen/Qian, findings-acl.243)** — multi-objective RLHF for text-to-image; OCR is just one reward. Not our problem.
- **MT^3 (Feng/Liu, acl-long.460)** — RL for text-image machine *translation* (translating text inside images); our JA↔Hebrew task is text-level, not image-embedded.
- **Ryze (Huang/Mai, acl-demo.73)** — biomedical VLM data-synthesis demo; OCR-cleansing is incidental, domain-specific.
- **Talk to Your Slides (Jung/Choo, findings-acl.166)** — slide-editing agent avoiding OCR; unrelated.
- **Making MLLMs Blind (Li/Hu, findings-acl.1006)** — adversarial smuggling attacks on content moderation; security, not philology.
- **OCR-Memory (Li/Ngai, acl-long.474)** — optical encoding of agent trajectories for long-horizon memory; not a text corpus.
- **TEN (Mehrotra/Tiwari, acl-industry.138)** — neurosymbolic table extraction from OCR-flattened text; table-structure domain.
- **MultiFinBen (Peng/Xie, acl-long.770)** — multilingual/multimodal finance benchmark; "financial OCR" only.
- **From Short Video to Clickable Search (Tian/Wang, acl-industry.38)** — RLVR listwise query suggestion; OCR is an input feature.
- **Fico (Tu/Song, findings-acl.1758)** — VLM robustness under visual text *compression*; rendering paradigm, tangential (though its OCR-vs-VQA cross-task-transfer finding echoes Sun/Zhang).
- **FUMA (Wang/Feng, acl-long.1679)** — DocVQA evidence localization via bijective page-semantic mapping; VQA, not reuse.
- **MMTutorBench (Yang/Jiang, acl-long.1068)** — AI math-tutoring benchmark; "OCR degrades tutoring" + generic LLM-as-judge note only.
- **Doc-V^* (Zheng/Bai, acl-long.2129)** — OCR-free agentic multi-page DocVQA navigation; different task.
- **Doc-AGround (Zhou/Zhang, findings-acl.16)** — OCR-free attention-based visual text grounding for DocVQA; grounding, not reuse.
