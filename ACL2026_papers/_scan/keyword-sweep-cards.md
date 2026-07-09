# Keyword-sweep scan cards — ACL 2026 (Genizah relevance)

Scope: the 122 title-keyword hits in `keyword_candidates.md`, EXCLUDING paths under
`Session_*`, `Topic_OCR`, `Workshops/NLP4DH` (covered by other agents). 72 papers in
scope; all abstract-checked. A/B cards below; C list at end.

Note: three high-expectation titles from the brief fall in skipped paths and were left
to their owning agents — "Digitizing Nepal's Written Heritage: HTR Pipeline" (Topic_OCR),
"Evaluating Latin/Ancient Greek Sentence Alignment" (NLP4DH), "From OCR to Analysis:
Correction Provenance" (NLP4DH). Also skipped (out of scope, high-value for others):
"Scaling Sentence Similarity for Classical Tibetan" (NLP4DH — the program's reference
point), "Modeling the Dalet Clitic in Historical Hebrew" (NLP4DH), "Quantifying Text
Reuse Across Yajurveda Recensions" (NLP4DH), "Validator-Guided Hard Negative Mining for
Low-Resource Ancient Languages" (Session_D).

---

## A-priority

### Human–AI Annotation Error Auditing for Hebrew Diacritization with Frontier LLMs
- **id**: 2026.law-main.4 · Workshop LAW XX · **file**: `Workshops/LAW/Gershuni_Shmidman_Human_AI_Annotation_Error_Auditing_for_Hebrew_Diacritiz.pdf`
- **authors/lab**: Hillel Gershuni & Avi Shmidman — Bar-Ilan University & DICTA (this is our own group / Dicta; the project owner is first author).
- **tl;dr**: An LLM-as-auditor workflow for finding sparse annotation errors in a large Hebrew nikud (diacritization) dataset, tested on the EACL-2023 Hebrew Homograph Challenge Set. On 12 homograph sets with 271 human-verified errors, Gemini 3 Pro hits 83.6% recall / 99.1% precision — beating two human experts (62.4% and 42.8%; union 73.4%) and cutting review effort >95%. Analyzes batch-size vs recall trade-offs and releases a human-verified gold + globally corrected challenge set.
- **relevance**: [llm-annot, semitic, product] — This is the exact method precedent for the SEED-029 "Track-3" plan of silver-labeling with our lexical engine + LLM-as-annotator: it quantifies where frontier LLMs beat humans at sparse-target error search, which is what QC of our silver text-reuse labels looks like. Directly reusable as the auditing loop for any Hebrew/JA gold we build.
- **stealable**: The whole recall/precision-at-scale protocol for LLM error auditing (batch-size↔recall curve; morph/dictionary header prompt design — note the "union morph header hurt error-correction by 10.7pp" finding); confidence-set-per-homograph framing; and their released gold + difficulty-banded corrected challenge set as an eval anchor.
- **priority**: A
- **bib leads**: Shmidman et al. 2023 "Do pretrained CLMs distinguish Hebrew homograph analyses?" (EACL — the source challenge set); Shmidman et al. 2020 "Nakdan: Professional Hebrew diacritizer"; Northcutt et al. 2021 "Confident Learning: estimating uncertainty in dataset labels"; Swayamdipta et al. 2020 "Dataset Cartography"; Gershuni & Pinter 2022 "Restoring Hebrew diacritics without a dictionary".

### ACSE: An Ancient Character Semantic-Aware Embedding for Large Language Models
- **id**: 2026.findings-acl.437 · Findings · **file**: `Tracks/Language_Models/Zhou_Xu_ACSE_An_Ancient_Character_Semantic_Aware_Embedding_for.pdf`
- **authors/lab**: Zhihan Zhou, Xiaolei Diao (UCL), Hao Xu et al. — Jilin Univ. "Key Lab of Ancient Chinese Script, Culture Relics and AI" + QMUL/UCL. A dedicated ancient-script-NLP lab; possible contact for low-resource historical embedding work.
- **tl;dr**: Builds embeddings for extremely low-resource ancient Chinese scripts (Oracle Bone, Bronze, Chu bamboo) by fusing glyph + lexicality (from a knowledge graph + the ShuoWen dictionary) via contrastive learning, and maps ancient characters into a modern-Chinese semantic space through a token-indexing strategy over an LLM (Qwen chosen for BBPE compatibility). Adds ~+4–14 pts on ablations vs a Llama3-8B baseline.
- **relevance**: [embed, ancient, noise, semitic-adjacent] — Method precedent for Track-3 embeddings over a corpus with "low digitization, scarce training corpora, complex semantics" — exactly the Genizah HTR situation. Their glyph+lexicality fusion is analogous to fusing our noisy HTR forms with normalized/lexical signals; the contrastive recipe + mapping-into-a-modern-semantic-space is directly transferable to a Hebrew/JA historical embedding.
- **stealable**: The contrastive training recipe that injects glyph + dictionary (lexicality) priors into an LLM's token embeddings on a tiny corpus; the backbone-selection lesson (BBPE tokenizer compatibility matters for injecting rare-script tokens); MDS/cosine visualization for sanity-checking the learned space.
- **priority**: A
- **bib leads**: Assael et al. 2022 "Restoring and attributing ancient texts using deep neural networks" (Nature — Ithaca); Cao et al. 2023 "Translating ancient Chinese to modern Chinese at scale (LLM-based)"; Chang et al. 2021 "Time-aware ancient Chinese text translation and inference"; Chi et al. 2022 "ZiNet: linking Chinese characters spanning three thousand years"; SikuGPT 2023 (generative PLM for ancient-text digital humanities).

### Draft, Verify, Restore: Self-Refining Historical Inscription Restoration with a Unified MLLM
- **id**: 2026.acl-long.1254 · Comp. Social Science track · **file**: `Tracks/Computational_Social_Science/Zhang_Jin_Draft_Verify_Restore_Self_Refining_Historical_Inscripti.pdf`
- **authors/lab**: Yuyi Zhang, Lianwen Jin et al. — South China University of Technology (Jin's group is a major historical-document / OCR lab).
- **tl;dr**: UniHIR, the first unified MLLM for end-to-end restoration of damaged historical inscriptions. Two designs: Draft-Guided Localization (find the illegible regions) and Hierarchical Self-Refinement (iteratively predict + self-correct content, 6 iterations), yielding page-level-consistent restoration. Ships HIR-Bench + a memory-efficient step-aware instruction-tuning factory; adds an OCR-based restoration metric and a human-in-the-loop review stage.
- **relevance**: [noise, ancient, product] — Directly on our "restoration of damaged/fragmentary text" need: Genizah fragments have lacunae, and we hold manuscript images. The Draft→Verify→Refine loop is a transferable pattern for lacuna reconstruction, and the "restore then verify with an independent signal + expert in the loop" design mirrors how we'd want to gate machine-proposed reconstructions.
- **stealable**: The iterative self-refinement loop (draft → localize damage → predict → verify → correct) as a controllable restoration strategy; the OCR-as-metric evaluation trick (invert rubbing colors to match OCR training distribution; report per-char accuracy); the explicit human-in-the-loop gate on intermediate drafts.
- **priority**: A
- **bib leads**: Assael et al. 2025 "Contextualizing ancient texts with generative neural networks" (Nature — Aeneas) + Assael et al. 2022 (Ithaca, Nature) — the canonical restoration/attribution precedents; Bamman & Burns 2020 "Latin BERT: a contextual LM for classical philology"; Cao et al. 2024 "TongGu: mastering classical Chinese understanding with knowledge-grounded LLMs"; Diao et al. 2025 "Oracle bone inscription image restoration via glyph extraction".

---

## B-priority

### Paraphrasing as Zero-shot Translation with Feature-guided Diversity Enhancement
- **id**: 2026.acl-long.783 · Machine Translation track · **file**: `Tracks/Machine_Translation/Yan_Xu_Paraphrasing_as_Zero_shot_Translation_with_Feature_guid.pdf`
- **authors/lab**: Ziyue Yan, Hongfei Xu et al. — Zhengzhou University.
- **tl;dr**: Trains a bidirectional multilingual NMT on a bilingual parallel corpus and uses zero-shot same-language translation (X→X) as a paraphrase generator, arguing this avoids the overfitting/information-loss of the usual MT-parabank approach; adds feature-guided diversity enhancement.
- **relevance**: [xlingual, embed] — Track-3 needs paraphrase-level positive pairs to train sentence embeddings; this is a cheap silver-paraphrase generator. The MNMT framing is also directly on our JA↔Hebrew translation-detection axis (build a bidirectional model, exploit zero-shot directions).
- **stealable**: The "zero-shot X→X translation = diverse paraphrase" trick for generating training pairs without a dedicated parabank; the diversity-enhancement objective to avoid collapsed near-copies (important for hard positives in contrastive STS training).
- **priority**: B
- **bib leads**: B. Zhang et al. 2020 "Improving massively multilingual NMT and zero-shot translation"; B. Zhang et al. 2023 "Prompting LLM for machine translation: a case study"; L. Zhang et al. 2024 "Respond in my language: mitigating language inconsistency".

### BanglaSTEM: A Parallel Corpus and Term-Weighted Evaluation for Technical Bangla–English Translation
- **id**: 2026.acl-srw.34 · Student Research Workshop · **file**: `Topic_NewDataset/Hasan_Adnan_BanglaSTEM_A_Parallel_Corpus_and_Term_Weighted_Evaluati.pdf`
- **authors/lab**: Kazi Reyazul Hasan, M. A. Adnan et al. — BUET (Bangladesh).
- **tl;dr**: Builds a 5K technical Bangla–English parallel corpus by an OCR→LLM-align→few-shot-generate→human-select pipeline: OCR matching passages from bilingual textbooks, LLM aligns sentences and marks technical terms, those become few-shot prompts to generate 12K new pairs, humans pick the best 5K preserving terminology. Proposes a term-weighted BLEU.
- **relevance**: [xlingual, reuse, noise] — A concrete, low-cost recipe for parallel-sentence mining/alignment from noisy OCR sources — the same shape as building a JA↔Hebrew aligned corpus, and adjacent to the Latin/Greek parallel-sentence-mining work another agent covers. The term-weighted metric idea maps to weighting distinctive/rare tokens in our matcher.
- **stealable**: The OCR→LLM-sentence-alignment→few-shot-augmentation→human-curation pipeline (contamination/copyright-safe); term-weighted BLEU (up-weight domain-distinctive tokens) as an eval that rewards preserving rare, discriminative terms — analogous to weighting low-DF n-grams in our seed-and-extend.
- **priority**: B
- **bib leads**: Hasan et al. 2020 "Not low-resource anymore: aligner ensembling, batch filtering… Bengali-English MT"; Goyal et al. 2022 "FLORES-101 low-resource MT benchmark"; Gala et al. 2023 "IndicTrans2".

### HSS-Synth: Humanities and Social Sciences Data Synthesis for LLMs
- **id**: 2026.findings-acl.1880 · Findings · **file**: `Tracks/Language_Models/Peng_Zhao_HSS_Synth_Humanities_and_Social_Sciences_Data_Synthesis.pdf`
- **authors/lab**: Ru Peng, Junbo Zhao (Zhejiang Univ.) + Qwen Team / Ant Group.
- **tl;dr**: First data-synthesis pipeline aimed at the humanities/social sciences: seed docs mined from web with multi-step filtering + judge; "requirements + persona" backtranslation of seeds into diverse-but-faithful instructions with a Q&A alignment check; teacher-forced answering anchored to the seed to curb hallucination on open-ended HSS content.
- **relevance**: [llm-annot, embed, product] — A silver-data generation template for an open-ended humanities corpus, i.e., the kind of synthetic supervision Track-3 needs. The Q&A-alignment faithfulness check and teacher-forced-answering (anchor to seed) are guardrails we'd want when LLM-generating training pairs over Genizah passages.
- **stealable**: The faithfulness-gated synthesis loop (backtranslate instruction ← seed, then verify via Q&A alignment); teacher-forced answering that feeds the seed during generation to anchor semantics and reduce hallucination; judge-based multi-step seed filtering.
- **priority**: B
- **bib leads**: Broder 1997 "On the resemblance and containment of documents" (MinHash — relevant to our scale/dedup bucket); Ben Allal et al. 2024 "Cosmopedia"; Chen et al. 2024 "DoG-instruct: text-grounded instruction wrapping"; Cao et al. 2025 "Condor: knowledge-driven data synthesis and refinement".

### ChunQiuTR: Time-Keyed Temporal Retrieval in Classical Chinese Annals
- **id**: 2026.findings-acl.612 · Findings · **file**: `Tracks/Safety_and_Alignment/Wang_ChunQiuTR_Time_Keyed_Temporal_Retrieval_in_Classical_Ch.pdf`
- **authors/lab**: Yihao Wang, Keze Wang et al. — Sun Yat-Sen University.
- **tl;dr**: A time-keyed retrieval benchmark over the Spring-and-Autumn Annals + its exegetical tradition, with a CTD dual-encoder (Fourier absolute calendrical context + relative offset biasing) that beats semantic dual-encoders under time-keyed eval. Curated by LLM-proposes-candidate + human-verifies, with published audit/acceptance statistics.
- **relevance**: [reuse, ancient, product] — Two things: (1) the later-commentary→canonical-record alignment step is essentially citation/allusion detection where later sources refer to a canonical passage via "compressed paraphrases, lexical reformulations, short subtitles" — the same problem as our reference-edge matching and motif-as-query against canonical editions. (2) The LLM-candidate + human-verify curation with explicit audit tables is a model for building our own aligned/benchmark data honestly.
- **stealable**: The commentary-to-canonical alignment framing (compressed paraphrase → source record) and its "chrono-near confounder" hard-negative construction; the LLM-propose-only + human-verify curation protocol with acceptance-rate audit tables; M3-embedding as a multilingual retrieval baseline.
- **priority**: B
- **bib leads**: Chen et al. 2024 "M3-Embedding: multi-linguality/functionality/granularity text embeddings via self-knowledge distillation"; Ting Chen et al. 2020 "SimCLR — a simple framework for contrastive learning"; Cao et al. 2024 "TongGu: knowledge-grounded classical Chinese LLM"; W. Chen et al. 2021 "A dataset for answering time-sensitive questions".

### Scripts Through Time: A Survey of the Evolving Role of Transliteration in NLP
- **id**: 2026.findings-acl.1176 · Findings · **file**: `Tracks/Discourse_Pragmatics/Jayakumar_Dabre_Scripts_Through_Time_A_Survey_of_the_Evolving_Role_of_T.pdf`
- **authors/lab**: Thanmay Jayakumar, Raj Dabre et al. — AI4Bharat / IIT Madras.
- **tl;dr**: Survey of transliteration for cross-lingual transfer: taxonomy of motivations, ways to inject transliterations as input, trade-offs, and settings where it helps (code-mixed text, language-family relatedness, inference efficiency), with concrete recommendations.
- **relevance**: [xlingual, semitic] — Judeo-Arabic is Arabic written in Hebrew script; bridging JA↔Arabic (and JA↔Hebrew) is fundamentally a transliteration/script-normalization problem. This is the survey to ground any romanization/script-folding step feeding our cross-lingual matcher or embedding alignment, and it explicitly covers code-mixed handling (our Hebrew/Aramaic/JA interleaving).
- **stealable**: The decision framework for when/how to transliterate to raise lexical overlap between scripts (directly applicable to a JA→Arabic-script or common-script normalization layer before matching/embedding); pointers to Romanization-for-transfer methods.
- **priority**: B
- **bib leads**: Amrhein & Sennrich 2020 "On Romanization for model transfer between scripts in NMT"; Aqlan et al. 2019 "Arabic–Chinese NMT: Romanized Arabic as subword unit"; Chari et al. 2025 "Lost in transliteration" (retrieval); Al Ghanim et al. 2024 "Jailbreaking LLMs with Arabic transliteration and Arabizi" (script-robustness caveat).

### ManCC: A Task-Anchored Benchmark for Manchu–Classical Chinese Cross-Lingual Modeling
- **id**: 2026.findings-acl.1359 · Multilinguality track · **file**: `Tracks/Multilinguality/Wang_Yin_ManCC_A_Task_Anchored_Benchmark_for_Manchu_Classical_Ch.pdf`
- **authors/lab**: Meiqi Wang, Minghao Yin et al. — Northeast Normal Univ. + collaborators.
- **tl;dr**: First Manchu–Classical-Chinese translation benchmark: a 16,627-pair parallel corpus derived from a Qing-dynasty historical text, plus a reproducible eval protocol combining BLEU/chrF with a three-dimensional human assessment (fidelity, fluency, linguistic normativity), evaluated across non-pretrained / multilingual-pretrained / LLM families.
- **relevance**: [xlingual, ancient] — A clean template for constructing a JA↔Hebrew (or Aramaic) parallel eval set for an extremely low-resource historical language pair, including a human-eval rubric tailored to historical fidelity rather than generic fluency.
- **stealable**: The task-anchored parallel-corpus-from-one-historical-source construction; the 3-axis human eval rubric (fidelity/fluency/normativity) as a better fit than BLEU alone for historical translation quality; eBLEU (embedding-based) as a lightweight automatic metric.
- **priority**: B
- **bib leads**: A. Chen et al. 2025 "Benchmarking LLMs for translating classical Chinese poetry (adequacy/fluency/elegance)"; Chung & Choi 2025 "Finetuning VLMs as OCR for low-resource languages: Manchu"; ElNokrashy & Kocmi 2023 "eBLEU: MT eval using simple word embeddings".

### From Curated Data to Scalable Models: Continual Pre-training of Dense and MoE LLMs for Tibetan
- **id**: 2026.acl-long.1866 · Long / NewDataset · **file**: `Topic_NewDataset/Yang_Xiong_From_Curated_Data_to_Scalable_Models_Continual_Pre_trai.pdf`
- **authors/lab**: Lei Yang, Deyi Xiong et al. — TJUNLP Lab, Tianjin University.
- **tl;dr**: End-to-end Tibetan LM pipeline: a 72 GB curated Tibetan corpus (largest to date), balanced multilingual continual pretraining of Qwen2.5-7B (Tibetan+Chinese+English) + multilingual instruction tuning, then scaled to a 50B-A10B MoE; builds Tibetan benchmarks where none existed.
- **relevance**: [ancient, product] — Track-3's stated reference domain is Classical Tibetan (Cohen et al.); this shows the corpus-curation + balanced-continual-pretraining route for a low-resource historical language, i.e., how we'd adapt a base model to Hebrew/JA/Aramaic Genizah text without catastrophic forgetting.
- **stealable**: The balanced-multilingual continual-pretraining recipe (mix target + high-resource languages to avoid forgetting) and the data-curation-first mindset; MoE scaling as a capacity lever if we ever train a Genizah-adapted model.
- **priority**: B
- **bib leads**: J. Chen et al. 2024 "Towards effective and efficient continual pre-training of LLMs"; Dou et al. 2024 "Sailor: open LMs for Southeast Asia"; Fedus et al. 2022 "Switch Transformers" (MoE); Cahyawijaya et al. 2024 "LLMs are few-shot in-context low-resource language learners".

### Alexandria: A Multi-Domain Dialectal Arabic Machine Translation Dataset
- **id**: 2026.acl-long.1503 · Long / NewDataset · **file**: `Topic_NewDataset/ELMekki_AbdulMageed_Alexandria_A_Multi_Domain_Dialectal_Arabic_Machine_Tran.pdf`
- **authors/lab**: Abdellah El Mekki, Muhammad Abdul-Mageed et al. — UBC + a very large multi-institution Arabic-NLP consortium (Birzeit, MBZUAI-adjacent, King Khalid, AUB, …); a strong network of Arabic/Semitic-NLP contacts.
- **tl;dr**: A large, multi-domain, multi-country dialectal-Arabic MT dataset built to make LLMs culturally/linguistically inclusive across Arabic varieties.
- **relevance**: [semitic, xlingual] — Judeo-Arabic is medieval/dialectal Arabic in Hebrew script; broad dialectal-Arabic MT resources and the models trained on them are candidate components for understanding/normalizing JA and for the JA↔Hebrew axis. Awareness/resource value plus a contact network.
- **stealable**: The dataset itself (dialectal-Arabic MT pairs) as auxiliary data or eval for any JA-handling component; the multi-country dialect-coverage design if we ever characterize JA dialectal variation.
- **priority**: B
- **bib leads**: Abdul-Mageed et al. 2020 "Toward micro-dialect identification in diglossic and code-switched environments"; Alhafni et al. 2022 "Arabic parallel gender corpus 2.0"; Alnumay et al. 2025 "Command R7B Arabic"; Agirre et al. 2012 "SemEval-2012 STS" (semantic textual similarity anchor).

### Under the Surface: Probing Tamil Paraphrase Intelligence
- **id**: 2026.starsem-conference.15 · *SEM 2026 · **file**: `Workshops/SEM/RR_S_Under_the_Surface_Probing_Tamil_Paraphrase_Intelligence.pdf`
- **authors/lab**: Viswadarshan R Raamiya et al. — Thiagarajar College of Engineering, India.
- **tl;dr**: Bootstraps a Tamil paraphrase-detection benchmark by translating English paraphrase corpora (QQP/PAWS/MRPC) through a multi-signal verification pipeline — semantic-similarity, round-trip consistency, classifier agreement, and human check — then evaluates multilingual encoders + a Tamil-specific model, including embedding-based classification.
- **relevance**: [embed, xlingual] — A low-resource recipe for standing up a paraphrase/STS eval set (Track-3's core evaluation) when you have no native paraphrase data: translate-and-verify from English. The round-trip + classifier-agreement + human filter is a reusable quality gate for Hebrew/JA silver paraphrase pairs.
- **stealable**: The multi-signal verification pipeline (round-trip consistency + classifier agreement + semantic similarity + human spot-check) for cleaning translated silver paraphrase data; the encoder-probing-via-lightweight-classifier evaluation of embedding quality.
- **priority**: B
- **bib leads**: Dolan & Brockett 2005 "Automatically constructing a corpus of sentential paraphrases" (MRPC); Conneau et al. 2020 "XLM-R (unsupervised cross-lingual representation learning at scale)"; Doddapaneni et al. 2023 "Leaving no Indic language behind" (low-resource corpora/benchmarks/models).

---

## C / skipped (in-scope, verdict C — one line each)

- Alexandria's sibling AraVQA (2026.acl-long.91) — Arabic factoid VQA from Wikipedia; PDF unreadable; off-topic.
- A Logical Analysis of Autosegmental Root-and-Pattern Morphology in Arabic — formal MSO/FO-logic phonology theory, not applicable.
- An NLP Framework … Opioid Industry Documents Archive — corporate-litigation topic modeling, unrelated.
- Authorship Attribution in Multilingual Machine-Generated Texts — MGT generator attribution, different problem.
- Automatic Slide Updating with Dynamic Templates — presentation editing, unrelated.
- Beyond Annotator Disagreement: Guideline-Induced Errors in Arabic Hate Speech — annotation-guideline critique, hate speech domain.
- Beyond Instruction Optimization: Multi-Agent Class Description Refinement — contact-center classification prompts.
- Beyond Query Memorization: LLM Routing — model-routing efficiency, unrelated.
- BoYaEval: Ancient Chinese Musical Scores — MLLM music-notation benchmark, no text-reuse transfer.
- BV-Blend: Uncertainty-Weighted Historical Baselines for RL — RLVR training stability ("historical" = reward baselines).
- Can LLMs Act as Historians? (Chinese Imperial Examination) — historical-reasoning QA benchmark, awareness-only.
- Can LLMs Learn to Map the World from Local Descriptions? — spatial cognition, unrelated.
- Can LLMs See Without Pixels? (SiT-Bench) — spatial intelligence from text, unrelated.
- Candidate-Aware Retrieval and Reranking for MCQA (Arabic) — MCQA RAG reranking, narrow.
- CaTS-Bench: Can LMs Describe Time Series? — time-series captioning, unrelated.
- CodeDet-NITS / Codexa / contestant001 / FMI_SU / LATE-IIMAS / MedHastra / NUST CodeIntel / Osint / Pixel Phantoms / Farhan Rayhan / Stylometry / Team Vivek / WWTC@UniA (SemEval-2026 Task 13) — machine-generated *code* detection/authorship; stylometry over source code, not text reuse.
- Completing/Validating the Re-Aligned Switchboard Dialog Act Corpus — speech-text realignment, unrelated.
- Cultural Benchmarking of LLMs in MSA/Dialectal Arabic Dialogue — cultural QA benchmark; Semitic but off-task.
- Designing Annotation Guidelines for Arabic Automated Essay Scoring — AES annotation methodology.
- Documenting Corporate Harm: Semantic Action Trajectories (Opioid) — SRL over litigation docs, unrelated.
- DualGuard / The Mark Fades — LLM watermark defense/attack, unrelated.
- Enhancing MLLMs for Ancient Chinese Character Evolution (Glyph-Driven FT) — glyph-evolution benchmark; overlaps ACSE, weaker fit.
- Evaluating the Reliability of LLMs in Faithfully Updating Text (FRUIT) — text-update faithfulness, unrelated.
- Feedback to Reasoning: LLM Molecular Optimization — chemistry, unrelated.
- FTibSuite: Tibetan Vision–Language — Tibetan VLM resource; Tibetan angle better covered by the CPT card.
- G-HiRel / Risk-Controlled Cascading KG Updates — knowledge-graph editing, unrelated.
- HistoryBankQA: Multilingual Temporal QA on Historical Events — Wikipedia-timeline QA, awareness-only.
- Large-Scale Multimodal KG about Classical Chinese Poetry — poetry KG construction, narrow.
- Learning Stress in Arabic Low-Resource Settings — phonological stress prediction, unrelated.
- Leveraging Label Semantics for LLM Fine-grained Entity Typing — FET method, generic.
- MCGA: Classical Chinese Literary Genre Audio Corpus — speech/audio tasks, unrelated.
- Measuring Visual Salience in Human and AI Descriptions — psycholinguistics of image captions, unrelated.
- MINED: Multimodal Time-Sensitive Knowledge — temporal-knowledge probing, unrelated.
- NAMAA / REGLAT (SemEval-2026 Task 9, Arabic polarization) — polarization detection, off-task.
- New Compendium of a Myriad of Plants (Ancient Chinese Plants) — botanical-encyclopedia dataset, narrow.
- ODL-TempLLM — ontology/description-logic temporal reasoning, unrelated.
- PBEBench: Programming-by-Examples inspired by Historical Linguistics — sound-law inductive reasoning eval, peripheral.
- Qayyem: Real-time Arabic Essay Scoring platform — AES web demo.
- SAHM: Arabic Financial & Shari'ah Reasoning — finance benchmark, off-task.
- Synergizing Stylometrics with Semantics — MGT text detection/attribution, different problem.
- TalkTag: Morphosyntactic Error Annotation for Transcribed Speech — clinical-speech annotation tool.
- When More Words Say Less (Image Description Specificity) — VLM caption evaluation, unrelated.
- Who Wrote This Line? (LLM-Generated Classical Chinese Poetry) — AI-poetry detection, different problem.
- Worldwide LiveVQA — real-time multilingual visual QA, unrelated.
- Would LLMs be Good Historical Linguists / Chinese Dialect Learners? — sound-law induction / G2P, peripheral.
