# ACL 2026 scan — sessions cards (researcher's 91 curated picks)

Scope: all PDFs directly under Session_A/…/Session_G/ (posters/ ignored).
Scanned: 91 · abstract-checked: 91 · A: 5 · B: 17 · C: 69.
Judged against `_scan/RELEVANCE-PROFILE.md`.

---

## A — act on these

### Beyond Word Boundaries: A Hebrew Coreference Benchmark and an Evaluation Protocol for Morphologically Complex Text
- **id**: 2026.acl-long.488 · ACL main (long) · **file**: Session_F/hebrew_coreference__2026.acl-long.488.pdf
- **authors/lab**: Refael Shaked Greenfeld & Reut Tsarfaty, **Bar-Ilan University (ONLILP lab)** — top-priority Israeli contact; Tsarfaty's group is the reference point for Hebrew MRL NLP.
- **tl;dr**: Introduces KibutzR, the first Modern-Hebrew coreference dataset, annotating mentions at word / sub-word / multi-word levels, plus an evaluation protocol that handles the word≠mention-boundary problem endemic to morphologically rich languages (pronominal clitics, fused tokens). Finds LLMs do far worse on Hebrew than English, that raw unsegmented text degrades performance, and — notably — that small encoders beat large decoders on Hebrew.
- **relevance**: [semitic, product] — Directly our language family and the exact boundary problem our HTR text has (clitics, no clean word segmentation). The segmentation-aware evaluation protocol is a template for scoring any Hebrew/JA span task (motif spans, witness alignment), and the "small encoder > decoder on Hebrew" result guides our Track-3 model choice.
- **stealable**: The sub-word/multi-word mention annotation scheme + morpheme-boundary-aware scoring metric; the empirical finding that encoder models (AlephBERT/DictaBERT class) beat decoders on Hebrew — cheaper for our corpus-scale embedding layer.
- **priority**: A
- **bib leads**: DictaBERT — a SOTA BERT suite for Modern Hebrew (Shmidman 2023); AlephBERT: sub-word to sentence-level Hebrew LM (Seker et al. 2022); DictaLM 2.0 — adapting LLMs to Hebrew with enhanced vocabulary (Shmidman et al. 2024); HeQ — Hebrew reading-comprehension benchmark (Cohen, Tsarfaty et al. 2025); LingMess — linguistically informed multi-expert coref scorers (Otmazgin, Cattan, Goldberg 2023).

### NeoAraBERT: A Modern Foundation Model for Arabic Embeddings with Diacritics-Aware Tokenization and POS-Targeted Masking
- **id**: 2026.findings-acl.1293 · Findings · **file**: Session_E/neoarabert__2026.findings-acl.1293.pdf
- **authors/lab**: Chakra, Hamoud, Al Mraikhat, Zaraket et al., **Arab Center for Research and Policy Studies (Doha) + American University of Beirut** — strong Arabic-NLP contact (Zaraket, Jarrar-adjacent).
- **tl;dr**: A NeoBERT-architecture Arabic **text-embedding** foundation model pretrained across MSA, **classical**, and dialectal Arabic, with ablations on text normalization, light stemming, and diacritics-aware tokenization plus POS-targeted masking. Ranks 1st on 18 of 23 Arabic tasks incl. a new synonym ("Muradif") embedding-quality probe requiring no fine-tuning.
- **relevance**: [semitic, embed, product] — The closest thing to a drop-in encoder for our Judeo-Arabic layer and a direct model precedent for Track-3 embeddings over classical Semitic text. Diacritics-aware tokenization + light stemming + POS-masking are exactly the design levers we need for JA/Hebrew orthographic variance and the diacritic-fold problem in our search stack.
- **stealable**: The diacritics-aware tokenization + POS-targeted masking recipe; the "Muradif" synonym-based intrinsic embedding evaluation (a cheap way to validate a JA/Hebrew embedding without labeled STS); the classical-Arabic pretraining mix.
- **priority**: A
- **bib leads**: AraBERT (Antoun, Baly, Hajj 2020) & AraELECTRA (Antoun 2021); ARBERT & MARBERT — deep bidirectional transformers for Arabic (Abdul-Mageed 2021); Swan and ArabicMTEB — dialect-aware, Arabic-centric embedding models & benchmarks (Bhatia et al. 2025); Sadeed — Arabic diacritization via small LM (Aldallal et al. 2025); Curras+Baladi Levantine corpus (Al-Haff, Jarrar, Zaraket 2022).

### Leveraging External Knowledge for Historical Document Restoration via Retrieval-Augmented Large Language Models
- **id**: 2026.findings-acl.2148 · Findings · **file**: Session_B/historical_document_restoration__2026.findings-acl.2148.pdf
- **authors/lab**: Gabeen Kim & Kyeongpil Kang, Kangwon National University (Korea) — DH/historical-restoration group.
- **tl;dr**: Existing MLM-based restoration of damaged historical documents fills local context well but fails on named entities needing external historical knowledge. Their framework "ARI" combines pretrained-LLM implicit knowledge with **retrieved external context (RAG)** to restore context-dependent proper nouns; on Korean Joseon-dynasty records it beats MLM/BERT baselines on both general characters and named entities, validated by expert assessment.
- **relevance**: [noise, ancient, product] — This is the exact frontier for Genizah: restoring damaged/illegible fragments where local context is insufficient and external witnesses/reference editions supply the missing entity. Maps onto a RAG restoration layer grounded in our own Track-1 reference corpus + witness census — the retrieved "external knowledge" would be our parallel witnesses.
- **stealable**: The RAG-for-restoration architecture (retrieve external witnesses → condition LLM infill), and the finding that named-entity restoration specifically needs retrieval, not just MLM — directly reusable with our reference editions as the retrieval store.
- **priority**: A
- **bib leads**: Restoring and attributing ancient texts using deep neural networks / Ithaca (Assael, Sommerschield et al., Nature 2022) + the 2019 Greek-epigraphy precursor; Restoring/mining the Joseon-dynasty records via neural LM + MT (Kang et al. 2021); Zero-shot methods for historical text restoration (Liu, Mueller, Wilkens 2025); Spelling normalization of historical documents via MT (Domingo & Casacuberta 2018); RAG for knowledge-intensive NLP (Lewis et al. 2020).

### Validator-Guided Hard Negative Mining for Masked Language Modeling in Low-Resource Ancient Languages
- **id**: 2026.acl-srw.69 · ACL SRW · **file**: Session_D/validator_guided_hard_negative_mining__2026.acl-srw.69.pdf
- **authors/lab**: Andrei Voinea, Babeș-Bolyai University — student work, but methodologically clean.
- **tl;dr**: For Sumerian (a language isolate absent from mBERT), builds a hierarchical rule-based **validator** (subword/word/POS patterns from 4,545 annotated sequences) and uses it to filter candidates and mine **hard negatives** for MLM fine-tuning. Vanilla mBERT 18.0% hit@10 → validator alone 72.8% → hard-negative fine-tuning 78.3% → combined 86.7% (+68.7 pts). Hard-negative mining transfers across time periods; rigid rules don't.
- **relevance**: [ancient, embed, noise] — Two directly transferable pieces: (1) hard-negative mining is a core Track-3 contrastive-embedding technique, and here it's shown to work for fragmentary ancient text with tiny data and to generalize temporally — exactly our regime; (2) the validator = our lexical engine (seed-and-extend + banded-Levenshtein) as a silver-label / hard-negative generator for training the semantic layer.
- **stealable**: The concrete hard-negative definition — candidates the validator accepts but the base MLM's top-5 rejects (`N_hard = V(i) \ B5(i) \ {w_i}`), include a position only if ≥15 hard negatives — a cheap recipe to mine hard negatives for our JA/Hebrew embedding from our own lexical matcher.
- **priority**: A
- **bib leads**: Filling the Gaps in Ancient Akkadian Texts: a MLM approach (Lazar, Saret, Yehudai, Horowitz, Wasserman, **Stanovsky** 2021 — Israeli, direct analog); Restoration of fragmentary Babylonian texts using RNNs (Fetaya, Lifshitz, Aaron, **Gordin** 2020, PNAS); NV-Retriever — effective hard-negative mining for embeddings (Moreira et al. 2024); RocketQA optimized dense-retrieval training (Qu et al. 2021); EvaCun 2025 shared task — lemmatization & token prediction in Akkadian/Sumerian with LLMs (Gordin et al. 2025).

### Syntax as a Rosetta Stone: Universal Dependencies for In-Context Coptic Translation
- **id**: 2026.findings-acl.1803 · Findings · **file**: Session_G/syntax_as_a_rosetta_stone__2026.findings-acl.1803.pdf
- **authors/lab**: Purushothama, Thronson, Guo, **Amir Zeldes — Georgetown Corpling Lab** — premier DH lab for Coptic/low-resource historical NLP; strong contact.
- **tl;dr**: Low-resource MT of Coptic→English via in-context learning, augmenting prompts with Universal Dependencies parses (raw parses, plain-English verbalizations, and targeted instructions for hard constructions) on top of bilingual-dictionary glosses. Syntax alone underperforms glosses, but **dictionary glosses + syntax together** yield significant gains and a new SOTA for Coptic. Uses real manuscript excerpts (e.g., Apophthegmata Patrum).
- **relevance**: [ancient, xlingual] — A working recipe for translating a low-resource, manuscript-sourced ancient language with almost no parallel data — our Judeo-Arabic↔Hebrew problem. The dictionary-gloss + structural-augmentation ICL pattern is directly portable, and the lab is a natural collaborator/citation anchor for Genizah philology-NLP.
- **stealable**: The gloss+syntax ICL prompt construction (retrieve dictionary items per token, add parse verbalizations, add targeted construction instructions) as a translation-detection/alignment aid for JA↔Hebrew; the finding that glosses beat syntax but combine best.
- **bib leads**: Dictionary-based phrase-level prompting of LLMs for MT (Ghazvininejad, Gonen, Zettlemoyer 2023); Translating a low-resource language using GPT-3 and a human-readable dictionary (Elsner & Needle 2023); RAG-enhanced NMT of Ancient Egyptian / THOTH AI (Miyagawa 2025); NMT for Coptic-French for low-resource ancient languages (Chaoui & Khoury 2025); A Linked Coptic Dictionary Online (Feder, Kupreyev, Manning, Schroeder, Zeldes 2018).
- **priority**: A

---

## B — awareness / cite-worthy

### Localizing Factual Inconsistencies in Attributable Text Generation (QASemConsistency)
- **id**: 2026.tacl-1.6 · TACL · **file**: Session_F/localizing_factual_inconsistencies__2026.tacl-1.6.pdf
- **authors/lab**: Arie Cattan, Paul Roit, Roee Aharoni, Idan Szpektor, **Ido Dagan (Bar-Ilan)** + Google Research Israel, UNC — major Israeli lab; strong contact.
- **tl;dr**: Decomposes generated text into minimal predicate-argument QA pairs (Neo-Davidsonian) and checks each against a trusted **reference text**, localizing exactly which propositions are unsupported. 3K+ annotated instances; scores correlate with human judgment; automatable with entailment models/LLMs.
- **relevance**: [reuse, product, llm-annot] — "Verify each minimal proposition against a reference text" is structurally the same operation as fine-grained witness/claim alignment against our reference editions — a semantic complement to lexical seed-and-extend for paraphrase-level reuse, and a way to score whether a candidate parallel actually supports a passage.
- **stealable**: The QA-decomposition-then-verify-against-reference protocol as a paraphrase-level intertextuality check; the crowd annotation design that hit high IAA on granular consistency.
- **priority**: B
- **bib leads**: PropSegmEnt — large corpus for proposition-level segmentation & entailment (Chen, Buthpitiya, Roth, Schuster 2023); CLIFF — contrastive learning for faithfulness in summarization (Cao & Wang 2021); Attributed QA — evaluation & modeling for attributed LLMs (Bohnet et al. 2022); Recognizing Textual Entailment: Models and Applications (Dagan, Roth et al. 2013).

### JudgeMeNot: Personalizing LLMs to Emulate Judicial Reasoning in Hebrew
- **id**: 2026.findings-acl.1332 · Findings · **file**: Session_F/judicial_reasoning_in_hebrew__2026.findings-acl.1332.pdf
- **authors/lab**: Razumenko, Sturm, **Nir Grinberg — Ben-Gurion University** — Israeli contact.
- **tl;dr**: A synthetic-organic supervision pipeline turns raw Hebrew judicial decisions into instruction-tuning data for PEFT-personalized per-judge models in a low-resource setting; Causal-LM pretraining + synthetic instruction-tuning beats other personalization baselines on lexical/stylistic/semantic similarity, producing outputs indistinguishable from human judges.
- **relevance**: [semitic, llm-annot] — A concrete Hebrew low-resource pipeline for converting raw domain text into silver instruction data — the same move we need for building Track-3 training signal (LLM-as-annotator over our lexical labels). Uses the Israeli Supreme Court dataset; the synthetic-supervision-from-unlabeled-corpus method is reusable for Genizah genre/style modeling.
- **stealable**: The "synthetic-organic" pipeline (Causal-LM continued pretraining on domain text, then generate instruction pairs from it) for bootstrapping supervised data in Hebrew with no labels.
- **priority**: B
- **bib leads**: REInstruct — building instruction data from unlabeled corpus (Chen et al. 2024); Learning to generate instruction-tuning datasets for zero-shot task adaptation (Nayak et al. 2024); The Israeli Supreme Court Dataset (Muchnik et al. 2023); Computational methods in authorship attribution (Koppel, Schler, Argamon 2009).

### AdabNer: Arabic Digital Archive Books with Nested Entity Recognition
- **id**: 2026.acl-long.1541 · ACL main (long) · **file**: Session_G/adabner__2026.acl-long.1541.pdf
- **authors/lab**: Aya Mourad (Sorbonne) & **Mustafa Jarrar (Birzeit / HBKU)** — leading Arabic-NLP/NER group (Wojood); strong contact.
- **tl;dr**: First large nested-NER dataset for **literary MSA** — 138 books across 10 genres (history, biography, travel lit., 1880s–2020s), ~876K tokens, 21 nested tags, 78,530 mentions (19% nested). AraBERTv2 hits F1 0.86 (stratified)/0.83 (leave-book-out); LLMs lag (Gemini 3 Pro 0.59); multi-domain training closes the out-of-domain gap to <1%.
- **relevance**: [semitic, product] — NER over archival/literary Arabic books is directly the "NER for archives/catalogs" product bucket and a resource for tagging persons/places in our JA material. The nested scheme and leave-book-out protocol are good models for entity extraction over Genizah catalog/transcription text; multi-domain-training-beats-specialization is an actionable finding.
- **stealable**: The nested 21-tag literary annotation scheme + leave-book-out evaluation; the multi-domain joint-training recipe for robust Arabic NER (encoder beats LLM).
- **priority**: B
- **bib leads**: Wojood — nested Arabic NER corpus + BERT (Jarrar, Khalilia, Ghanem 2022); Arabic fine-grained entity recognition (Liqreina, Jarrar et al. 2023); Konooz — multi-domain multi-dialect NER corpus (Hamad, Khalilia, Jarrar 2025); Exploring nested NER with LLMs (Kim, Kim, Kim 2024).

### A Dual-View Analysis of Multiple Languages in Colonial Newspapers
- **id**: 2026.findings-acl.1029 · Findings · **file**: Session_E/colonial_newspapers__2026.findings-acl.1029.pdf
- **authors/lab**: Su, Chen, Mo, da Silva Perez et al., U. Copenhagen + Erasmus — historical/DH multilingual group.
- **tl;dr**: Tackles 18th–19th c. multilingual colonial newspapers under three stated challenges — data scarcity, **OCR noise**, and archaic multilingual prose — via joint multilingual event extraction + temporal semantic-shift analysis (CQA/VQA sets in Dutch, English-French, Spanish), training temporal word embeddings "with a compass." Finds LLMs still weak on low-resource VQA over noisy historical images.
- **relevance**: [noise, ancient, xlingual] — A close analog to our setting: noisy OCR, multilingual, archaic. The temporal-embedding-with-compass method is relevant for diachronic Hebrew/JA meaning-shift, and the paper is a good citation anchor for "NLP under dirty OCR on historical multilingual corpora."
- **stealable**: Temporal word embeddings with a compass for diachronic drift; the joint event-extraction + semantic-shift framing over OCR-noisy multilingual archives.
- **priority**: B
- **bib leads**: Quantifying the impact of dirty OCR on historical text analysis (Hill & Hengchen 2019); LLMs achieve SOTA transcription of handwritten historical documents (Humphries et al. 2024); Evaluating LLMs for historical document OCR — a methodological framework (Levchenko 2025); Training temporal word embeddings with a compass (Di Carlo, Bianchi, Palmonari 2019); Multilingual event extraction from historical newspaper adverts (Borenstein, da Silva Perez, Augenstein 2023).

### Explainable Disentangled Representation Learning for Generalizable Authorship Attribution (EAVAE)
- **id**: 2026.acl-long.2018 · ACL main (long) · **file**: Session_E/generalizable_authorship_attribution__2026.acl-long.2018.pdf
- **authors/lab**: Man, Pham, Ngo, Dernoncourt, Thien Huu Nguyen — U. Oregon + Adobe.
- **tl;dr**: Learns authorship style representations that disentangle **style from content** by design: supervised-contrastive pretraining of a style encoder, then a VAE with separate style/content encoders and a discriminator that both separates authors/content and emits NL explanations. SOTA on PAN21/HRS/Amazon and strong few-shot AI-text detection.
- **relevance**: [embed, product] — Style/content disentanglement + supervised contrastive is a directly relevant recipe for our Track-3 embeddings, where we must separate "same work/text-reuse" signal from topical similarity (a known confound in intertextuality detection). Applicable to scribe/author clustering over Genizah hands and to reuse-vs-topic separation.
- **stealable**: The separation-by-design architecture (contrastive style pretrain → VAE with split encoders + adversarial discriminator) to decorrelate reuse signal from topic in our embedding space.
- **priority**: B
- **bib leads**: Learning universal authorship representations (Rivera-Soto et al. 2021); Supervised contrastive learning (Khosla et al. 2021); Domain-adversarial training of neural networks (Ganin et al. 2016); Counterfactual augmentation for robust authorship representation learning (Man & Nguyen 2024); Whodunit? learning to contrast for authorship attribution (Ai et al. 2022).

### GerAV: German Authorship Verification using Fine-Tuned LLMs on a New Benchmark
- **id**: 2026.findings-acl.1991 · Findings · **file**: Session_A/gerav__2026.findings-acl.1991.pdf
- **authors/lab**: Kiefer, Leiter, Takeshita, Schmidt, **Steffen Eger (UT Nuremberg)** — active metrics/eval group.
- **tl;dr**: A 400K-pair German **authorship-verification** benchmark (Twitter/Reddit; in-domain, cross-domain, profile-based subsets) enabling controlled study of source/domain/length effects. A fine-tuned LLM beats prior baselines by up to +0.09 F1 and beats zero-shot GPT-5 by +0.08; specialization vs generalization trade-off mitigated by mixing training sources.
- **relevance**: [embed, product] — AV = same-author pairwise verification, structurally identical to our same-work/same-scribe pairwise judgments. The benchmark construction (controlled domain/length splits) and the "mix sources to generalize" finding inform how we build training pairs for a Genizah reuse/authorship verifier.
- **stealable**: The pairwise-verification benchmark design with domain/length-controlled subsets; the training-source-mixing trick to trade off specialization vs cross-domain generalization.
- **priority**: B
- **bib leads**: Overview of PAN cross-domain authorship verification (Kestemont et al. 2020); Explainable authorship verification via attention-based similarity learning (Boenninghoff et al. 2019); The Million Authors Corpus — cross-lingual/cross-domain AV (Israeli, Liu, May, Jurgens 2025); Authorship verification for different languages, genres and topics (Halvani, Winter, Pflug 2016); Leveraging multilingual training for authorship representation (Kim, Zhang, Jurgens 2025).

### PAR: Training-Free Positional Perturbation and Attention Recycling for Faithful OCR
- **id**: 2026.acl-long.1065 · ACL main (long) · **file**: Session_C/attention_recycling_for_faithful_ocr__2026.acl-long.1065.pdf
- **authors/lab**: Yao, Liao, Zhang, Zuchao Li, Hai Zhao — Shanghai Jiao Tong + Wuhan U.
- **tl;dr**: Diagnoses "Linguistic Priors Hallucination" in VLM-OCR — models "recite" familiar text instead of "reading" the image, worsening as output lengthens (GlitchText probing set). PAR is a training-free inference-time fix: inject phase noise into rotary positional embeddings + dynamically redistribute attention back to visual regions (Foveal Attention Recycling), cutting CER by ~12% on long contexts.
- **relevance**: [noise, product] — Our HTR runs at 16–20% CER and any VLM-assisted transcription/restoration risks exactly this "recite the expected text" failure — dangerous when a fragment's value is its *deviation* from the canonical text. A training-free CER reducer applicable to any VLM we use for image-grounded reading/restoration.
- **stealable**: The two training-free interventions (RoPE phase-noise perturbation + attention-mass redistribution to visual regions) to force visual grounding and cut hallucinated transcription.
- **priority**: B
- **bib leads**: Mitigating object hallucinations via visual contrastive decoding (Leng et al. 2024); Multi-modal hallucination control by visual information grounding (Favero et al. 2024); See what you are told: visual attention sink in LMMs (Kang et al., ICLR); Visual evidence prompting mitigates hallucinations in VLMs (Li et al. 2025); DASH — detection/assessment of systematic VLM hallucinations (Augustin, Neuhaus, Hein 2025).

### StruNRAG: Evaluation of OCR-Induced Structural Noise on RAG Robustness
- **id**: 2026.findings-acl.955 · Findings · **file**: Session_G/strunrag__2026.findings-acl.955.pdf
- **authors/lab**: Gao, Yin, Zhu, Hou, Ni, Wang — Tongji University.
- **tl;dr**: Benchmark (2,132 bilingual QA pairs) that injects three realistic OCR **structural** noises — line insertion, paragraph interleaving, line interleaving — into RAG inputs. Finding: structural distortion consistently degrades *retrieval*, but *generation* is surprisingly robust to local noise and only breaks under severe global fragmentation.
- **relevance**: [noise, product] — Our transcriptions have exactly this structural noise (interleaved lines, fragmented layout from HTR). Tells us where to spend effort if we build RAG over the corpus: harden retrieval/embedding against structural noise, worry less about the generator for local errors.
- **stealable**: The three structural-noise injection operators as a stress-test suite for our retrieval/embedding layer; the retrieval-vs-generation robustness split as a design prior.
- **priority**: B
- **bib leads**: M3-Embedding — multi-linguality/functionality/granularity via self-knowledge distillation (Chen et al. 2024); Adaptive-RAG — adapt retrieval by question complexity (Jeong et al. 2024); RAGAs — automated RAG evaluation (Es et al. 2024); OmniDocBench — diverse PDF parsing benchmark (Ouyang et al. 2025).

### Neural Induction of Finite-State Transducers
- **id**: 2026.findings-acl.1411 · Findings · **file**: Session_B/neural_induction_of_finite_state_transducers__2026.findings-acl.1411.pdf
- **authors/lab**: Michael Ginn, Alexis Palmer, **Mans Hulden** — U. Colorado / New College of Florida; Hulden is a morphology/FST authority.
- **tl;dr**: Automatically constructs unweighted FSTs by reading out the hidden-state geometry of a trained RNN, then evaluates on **historical normalization, grapheme-to-phoneme, and morphological inflection** — beating classical transducer-learning by up to +87% on held-out sets, while keeping FST efficiency.
- **relevance**: [noise, xlingual, semitic] — Historical spelling/orthographic **normalization** is a direct need for Genizah variant handling, and transliteration (JA↔Hebrew script) is a string-to-string FST task. An efficient, interpretable normalizer to canonicalize orthographic variants before/inside lexical matching.
- **stealable**: The neural→FST induction pipeline for building a fast, inspectable orthographic-normalization/transliteration transducer from example pairs (cheaper than an LLM at corpus scale).
- **priority**: B
- **bib leads**: A large-scale comparison of historical text normalization systems (Bollmann 2019); Automatic induction of FSTs for simple phonological rules (Gildea & Jurafsky 1995); SIGMORPHON 2016 shared task — morphological reinflection (Cotterell et al. 2016); Transliterated mobile keyboard input via weighted FSTs (Hellsten et al. 2017).

### Specializing Large Models for Oracle Bone Script Interpretation via Component-Grounded Multimodal Knowledge Augmentation
- **id**: 2026.acl-long.1626 · ACL main (long) · **file**: Session_F/oracle_bone_script__2026.acl-long.1626.pdf
- **authors/lab**: Zhang, Li, Pang, Xia et al., Jilin University (Key Lab of Ancient Chinese Script + AI).
- **tl;dr**: Reframes Oracle Bone Script decipherment from closed-set image recognition to **component-grounded interpretation**: rare/unique glyphs decompose into recurring pictographic components carrying transferable meaning. An agentic VLM+LLM pipeline does component identification → graph knowledge retrieval → relationship inference. Releases OB-Radix (1,022 char images, 478 components with verified explanations).
- **relevance**: [ancient] — A paleography/decipherment method precedent: decompose rare units into shared sub-components with retrievable semantics — analogous to our motif-decomposition (motifs as reusable sub-units) and to handling rare/damaged glyphs. Multimodal manuscript-understanding pipeline transferable to Genizah paleography.
- **stealable**: The component-grounded decomposition + graph-retrieval reasoning chain — a template for a "motif/component knowledge graph" over recurring sub-units, and for the agentic retrieve-then-infer decipherment loop.
- **priority**: B
- **bib leads**: Deciphering oracle bone language with diffusion models (Guan et al., ACL 2024); OracleFusion — structurally-constrained semantic typography for decipherment (Li et al. 2025); OBI-Bench — can LMMs aid ancient-script study (Chen et al., ICLR 2025); Component-level oracle bone inscription retrieval (Hu et al. 2024).

### Scaling Performance and Low-Resource Annotation with Many-Shot In-Context Learning for NER
- **id**: 2026.findings-acl.1431 · Findings · **file**: Session_E/many_shot_in_context_learning_for_named_entity__2026.findings-acl.1431.pdf
- **authors/lab**: Q. Zhang, Lan, Caragea, Latecki, Dragut — Temple U. + UIC.
- **tl;dr**: Scales ICL to hundreds of demonstrations for NER; with many shots LLMs match/exceed fine-tuned BERT, and ~100 human-labeled examples used as demonstrations let the LLM auto-annotate data that, when used to fine-tune BERT, gives ~+10 F1 on low-resource NER.
- **relevance**: [product, llm-annot] — A practical low-resource-annotation loop: LLM-as-annotator seeded by a small labeled set → silver data → train a cheap encoder. Directly the pattern for bootstrapping NER/tagging over our catalog + JA/Hebrew text where labels are scarce, and a validation of the silver-label pipeline behind Track-3.
- **stealable**: The many-shot-ICL-as-data-annotator recipe (~100 gold demos → generate silver → distill into BERT) for low-resource Genizah tagging tasks.
- **priority**: B
- **bib leads**: Many-shot in-context learning (Agarwal et al. 2024); NuNER — entity-recognition encoder pretraining via LLM-annotated data (Bogdanov et al. 2024); Is GPT-3 a good data annotator? (Ding et al. 2023); ProgGen — generating NER datasets step-by-step with self-reflexive LLMs (Heng et al. 2024).

### SkMTEB: Slovak Massive Text Embedding Benchmark and Model Adaptation
- **id**: 2026.acl-long.2114 · ACL main (long) · **file**: Session_E/skmteb__2026.acl-long.2114.pdf
- **authors/lab**: Šuppa, Ridzik, Hládek et al., Comenius U. / Cisco / KInIT — low-resource embedding group.
- **tl;dr**: First MTEB-style embedding benchmark for a low-resource language (Slovak, 31 datasets/7 task types) + a replicable adaptation recipe: **vocabulary trimming** (top-60K tokens) + fine-tuning Multilingual-E5 into e5-sk-small/large. Despite up to 62% size cut, matches proprietary APIs and stays locally deployable for semantic search/RAG.
- **relevance**: [embed, product, scale] — A direct, cheap blueprint for standing up a locally-deployable Hebrew/JA embedding model for our search stack: trim mE5's vocab to our scripts, fine-tune, evaluate MTEB-style. The efficiency numbers (30–50 min on one H100) make a Genizah-tuned embedding realistic.
- **stealable**: The vocabulary-trimming + mE5 fine-tuning pipeline (with exact hyperparameters and Multiple-Negatives-Ranking loss) and the "build your own MTEB subset" evaluation approach for JA/Hebrew.
- **priority**: B
- **bib leads**: Pre-FT vocabulary trimming (Ushio et al. 2023); MTEB — Massive Text Embedding Benchmark (Muennighoff et al. 2023); Multilingual E5 (mE5) text embeddings; M3-Embedding (Chen et al. 2024).

### The Role of Mixed-Language Documents for Multilingual LLM Pretraining
- **id**: 2026.acl-long.1706 · ACL main (long) · **file**: Session_D/mixed_language_documents_for_multilingual__2026.acl-long.1706.pdf
- **authors/lab**: Shao, Tang, Zhang, Stenetorp, Yang, Lu — UCL + NTU + Waterloo + NVIDIA.
- **tl;dr**: Controlled-pretraining ablation: bilingual data is only 2% of the corpus but removing it drops translation 56% BLEU (cross-lingual QA/reasoning unaffected). Decomposes bilingual data into parallel (14%), code-switching (72%), misc (14%); re-adding **parallel** data restores 91% of translation, code-switching barely helps. Translation depends on systematic token-level alignments from parallel text.
- **relevance**: [xlingual] — Speaks straight to our JA↔Hebrew translation-detection/alignment problem: it's *parallel* (aligned) data, not merely interleaved/code-switched text, that carries the token-level alignment signal. Guides where to invest (mine/curate JA-Hebrew parallels) for any translation-alignment component.
- **stealable**: The empirical prioritization — parallel > code-switching for translation alignment — plus the bilingual-data taxonomy for auditing what alignment signal our corpus actually contains.
- **priority**: B
- **bib leads**: Searching for needles in a haystack: incidental bilingualism in PaLM's translation capability (Briakou, Cherry, Foster 2023); Dict-MLM — improved multilingual pretraining using bilingual dictionaries (Chaudhary et al. 2020); Word translation without parallel data (Conneau et al. 2017); Beyond English-centric multilingual MT (Fan et al. 2020).

### Lost in the Mix: Evaluating LLM Understanding of Code-Switched Text
- **id**: 2026.acl-long.2080 · ACL main (long) · **file**: Session_F/lost_in_the_mix__2026.acl-long.2080.pdf
- **authors/lab**: Amr Mohamed, Zhang, Vazirgiannis, Guokan Shang — MBZUAI + Polytechnique.
- **tl;dr**: Builds linguistically-grounded code-switched variants of Belebele/MMLU/XNLI across five languages. Inserting non-English tokens into English hurts accuracy; embedding English into non-English often helps. ICL mitigation is inconsistent; fine-tuning on CS data gives modest reliable gains.
- **relevance**: [xlingual, semitic] — Judeo-Arabic is inherently code-switched/interleaved (Hebrew script, Arabic language, Hebrew quotations), so LLM behavior on code-switched input directly affects any model we apply. The controlled CS-generation pipeline (respecting linguistic constraints) is reusable for building JA test/train variants.
- **stealable**: The constraint-respecting code-switch generation pipeline for constructing CS evaluation/training sets; the finding that fine-tuning (not ICL) is what reliably recovers CS accuracy.
- **priority**: B
- **bib leads**: Improving pretraining techniques for code-switched NLP (Das, Ranjan, Pathak, Jyothi 2023); Word alignment by fine-tuning embeddings on parallel corpora (Dou & Neubig 2021); LinCE — centralized code-switching evaluation benchmark (Aguilar, Kar, Solorio 2020); Equivalence-constrained LLM code-switched text generation (Kuwanto et al. 2024).

### A Multistage Extraction Pipeline for Long Scanned Financial Documents (Industrial KYC)
- **id**: 2026.acl-industry.99 · ACL Industry · **file**: Session_E/long_scanned_financial_documents__2026.acl-industry.99.pdf
- **authors/lab**: Han, Zhang, Wang, Jin — OCBC, Singapore.
- **tl;dr**: For long, noisy, multilingual **scanned** documents, a pipeline that separates page localization from multimodal reasoning: image preprocessing → multilingual OCR → hybrid page-level retrieval → compact-VLM structured extraction. On 120 docs / ~3000 pages, beats direct PDF→VLM by up to +31.9 pts field accuracy; ablation shows **page-level retrieval is the dominant factor**.
- **relevance**: [noise, product] — Our corpus is exactly long, noisy, multilingual scanned material where task-relevant content is sparse. The "retrieve the right page/region first, then reason" architecture (and its outsized impact) is a strong design prior for extraction/search over Genizah images at scale.
- **stealable**: The decouple-localization-from-reasoning pipeline with a retrieval stage before the VLM; empirical evidence that page/region retrieval dominates end-to-end accuracy on noisy scans.
- **priority**: B
- **bib leads**: ColPali — efficient document retrieval with vision-language models (Faysse et al. 2025); Donut — OCR-free document understanding transformer (Kim et al. 2022); mPLUG-DocOwl 1.5/2 — OCR-free multi-page document understanding (Hu et al. 2024/2025); olmOCR — VLM PDF extraction at scale (Poznanski et al. 2025).

### PARAMANU: Compact and Competitive Monolingual LMs for Low-Resource Morphologically Rich Indian Languages
- **id**: 2026.acl-long.1922 · ACL main (long) · **file**: Session_A/paramanu__2026.acl-long.1922.pdf
- **authors/lab**: Niyogi, Gaussier, Arnab Bhattacharya — Grenoble + IIT Kanpur.
- **tl;dr**: Trains 108M–367M monolingual LMs from scratch for five morphologically rich low-resource Indian languages on a **single GPU under $1,000**, using morphology-aligned low-fertility tokenizers and a RoPE interpolation trick for longer sequences; outperforms multilingual models up to 8B despite tiny size.
- **relevance**: [semitic, scale] — Proof that an affordable, morphology-aware monolingual model can beat giant multilingual ones on the target language — the case for building a dedicated Hebrew/JA base model rather than relying on multilingual giants. Low-fertility morphology-aligned tokenization directly addresses Semitic over-fragmentation.
- **stealable**: The recipe for a cheap-from-scratch monolingual model + morphology-aligned low-fertility tokenizer; the finding that this beats 8B multilingual models on-language — a viable path to a Genizah-native encoder/decoder.
- **priority**: B
- **bib leads**: Byte-pair encoding is suboptimal for LM pretraining (Bostrom & Durrett 2020); When is multilinguality a curse? LM for 250 languages (Chang, Arnett, Tu, Bergen 2024); BanglaByT5 — byte-level modelling for Bangla (Bhattacharyya & Bhattacharya 2025); OSCAR — cleaner document-oriented multilingual crawl (Abadji, Suarez, Romary, Sagot 2022).

### FLEXITOKENS: Flexible Tokenization for Evolving Language Models
- **id**: 2026.findings-acl.848 · Findings · **file**: Session_A/flexitokens__2026.findings-acl.848.pdf
- **authors/lab**: Owodunni, Ahia, Sachin Kumar — Ohio State + U. Washington (Ahia/Tsvetkov-adjacent tokenization line).
- **tl;dr**: Byte-level LMs with a learnable boundary predictor and a simplified training objective (FLEXITOKENS) that avoids the fixed-compression-rate rigidity of prior tokenizer-free methods. Reduces over-fragmentation on out-of-distribution domains, unseen languages/scripts by up to 10 pts on token-classification/generation.
- **relevance**: [semitic, noise, product] — Over-fragmentation on unseen scripts/domains is precisely what hurts Hebrew/JA and noisy HTR text in subword tokenizers. An adaptive byte-level tokenizer that adjusts segmentation per-domain is attractive for a model that must handle clean editions, noisy HTR, and JA orthography together.
- **stealable**: The learnable-boundary byte-level tokenization with a flexible (non-fixed-rate) objective — a way to avoid brittle subword fragmentation across our heterogeneous text conditions.
- **priority**: B
- **bib leads**: MYTE — morphology-driven byte encoding for fairer multilingual LM (Limisiewicz et al. 2024); CANINE — tokenization-free encoder (Clark et al. 2022); Do all languages cost the same? Tokenization in commercial LMs (Ahia et al. 2023); Dynamic chunking for end-to-end hierarchical sequence modeling / H-Net (Hwang & Gu 2025); Efficient transformers with dynamic token pooling (Nawrot et al. 2023).

---

## C / skipped

### Session A
- bertology_view (2026.acl-long.1955) — token/layer probes for efficient LLM classification; serving-infra, not our domain.
- can_you_make_it_sound_like_you (2026.acl-long.2030) — user study on post-editing personal style; HCI/writing, not manuscripts.
- distilling_llm_reasoning_into_dense_encoders (2026.findings-acl.1130) — R2END distillation for recommendation; embeddings but wrong task.
- dual_alignment (2026.acl-long.2143) — LM layers vs human sentence processing; psycholinguistics.
- existence_proof (2026.acl-long.1694) — garden-path effects via surprisal; psycholinguistics.
- filling_in_the_mechanisms (2026.findings-acl.1737) — filler-gap acquisition under BabyLM constraints; interpretability.
- from_style_to_story (2026.findings-acl.968) — imitative novel generation; creative gen.
- hyperparameters_in_language_modeling (2026.acl-long.1939) — HP sensitivity in compling; methodology.
- interpreting_style_representations (2026.findings-acl.2039) — style-eliciting prompts; authorship-adjacent but interpretability-only.
- model_in_distress (2026.findings-acl.2132) — French synthetic sentiment; synthetic-data method, off-domain.
- morpheme_aware_kv (2026.acl-long.1355) — morpheme KV-aggregation for medical/legal domain terms; adapter, not our morphology.
- string_probability_tell_us_about_grammaticality (2026.tacl-1.7) — grammaticality theory.
- word_order_learnability (2026.acl-long.1510) — word-order typology in LMs.

### Session B
- currency_bias_and_syntax_gap (2026.findings-acl.2118) — currency bias in finance embeddings.
- gated_tree_cross_attention (2026.acl-long.1629) — syntax injection into decoder LLMs.
- more_aligned_less_diverse (2026.acl-long.1803) — LLM grammar/lexicon diversity vs humans.
- prompting_across_time (2026.findings-acl.1665) — historical (Early Modern English) but hate-speech classification task.
- shared_syntactic_mechanisms (2026.acl-long.2078) — activation-patching interpretability.
- spence (2026.acl-long.926) — syntactic probe for NL2SQL benchmark contamination.
- tinyattack (2026.findings-acl.1987) — Unicode/homoglyph stylistic adversarial attack; noise-adjacent but attack-focused.
- tokenization_through_lens_of_indian (2026.findings-acl.1632) — morphology-aware Indic tokenization; see flexitokens/paramanu B cards (Unigram-preserves-morphology finding noted).

### Session C
- biomed_enriched (2026.findings-acl.1713) — paragraph-level biomedical pretraining data.
- character_descriptions_in_books (2026.findings-acl.1259) — QA-guided character description gen.
- diff4tst (2026.acl-long.306) — masked-diffusion text style transfer.
- frankentext (2026.acl-long.1457) — stitching verbatim human snippets into narratives + AI-detection evasion; reuse-adjacent but a generation/evasion study.
- gaperon (2026.findings-acl.1955) — FR-EN LM suite; benchmark contamination study.
- model_internal_sleuthing (2026.acl-long.720) — lexical/inflectional feature probing.
- scaffolding_to_assimilation (2026.findings-acl.1913) — format-constrained Chinese poetry generation.

### Session D
- continual_learning_chinese_literature_GEC (2026.acl-long.1546) — Chinese grammatical error correction, continual learning.
- fiction_flows (2026.acl-long.1576) — narrative sequentiality replication.
- multi_domain_acceptability (2026.findings-acl.2096) — context effects in acceptability judgment.
- not_all_animals_are_equal (2026.findings-acl.1911) — metaphor framing via source domains/frames.
- social_story_frames (2026.acl-long.1934) — reader-response formalism for social-media stories.
- subtokentest (2026.acl-long.915) — sub-token understanding benchmark (counting/tables/maps).
- toxicity_recognition_of_span_and_target (2026.findings-acl.1854) — toxicity detection dataset.

### Session E
- bias_dynamics_in_babylms (2026.findings-acl.1668) — compute-efficient bias sandbox via BabyLMs.
- conlangs (2026.findings-acl.1455) — constructed-language generation to probe LLM metalinguistics.
- figsim (2026.findings-acl.1827) — suicide-meme figurative-language dataset.
- logophoric_cues (2026.findings-acl.2135) — Mandarin ziji long-distance reflexive probing.
- morphogen (2026.acl-long.105) — Arabic/French/Hindi gender-morphology *generation* benchmark; Semitic-adjacent but generation task.
- multilingual_idioms (2026.acl-long.564) — idiom comprehension across resource tiers.
- poetry_generation_in_arabic (2026.findings-acl.1931) — Arabic/dialect poetry generation; Semitic but creative-gen, not reuse.
- prague_dependency_treebank (2026.findings-acl.1060) — Czech semantic-pragmatic treebank annotation.
- student_handwritten_solutions (2026.findings-acl.751) — MLLM recognition of STEM handwriting; noise-adjacent (note: low-confidence-to-human routing idea), far off-domain.
- watermark_vs_automatic_detection (2026.acl-industry.9) — synthetic-text detection (watermark vs classifier).

### Session F
- a_model_of_the_language_process (2026.acl-long.590) — temporal BERT (date prediction); diachronic method but contemporary US English.
- classical_chinese_poetry_generation (2026.findings-acl.836) — Tang-poetry generation + LLM-judge biases.
- commonsense_knowledge_with_negation (2026.findings-acl.578) — negation in commonsense KBs.
- concreteness_through_a_figurative_lens (2026.acl-long.705) — concreteness probing in LLMs.
- false_friends_or_cognates (2026.acl-long.1818) — cognate/false-friend disambiguation, Romance; xlingual-adjacent, off-language.
- frame_semantic_knowledge_injection (2026.acl-short.55) — FrameNet-via-LoRA for event inference.
- grammar_teaching_material (2026.findings-acl.1327) — grammar induction for endangered-language teaching.
- inducing_media_narratives (2026.acl-long.1970) — structured clustering of media narratives.
- logical_fallacies_via_socratic (2026.acl-long.2209) — Socratic tutoring on fallacies.
- one_script_instead_of_hundreds (2026.findings-acl.1909) — romanized encoder pretraining; script/tokenization-adjacent, no Semitic focus.
- simpleocr (2026.findings-acl.519) — rendered-question training to force MLLM reading; OCR-adjacent (same "modality laziness" theme as PAR) but general MLLM.
- start_making_sense (arxiv-2511.21974) — attention specialization via lexical ambiguity; interpretability.
- urblimp (2026.findings-acl.29) — Urdu minimal-pairs linguistic-competence benchmark.

### Session G
- feature_inversion_trap (2026.acl-long.1998) — MGT detection under personalization.
- generics_are_not_quantificational (2026.findings-acl.1100) — semantics of generics via LM probabilities.
- itercomp (2026.acl-long.1559) — prompt compression for multi-hop QA.
- multimodal_puns (2026.acl-long.444) — VLM pun understanding.
- neo_classic (2026.acl-long.1266) — classical Chinese poetry aesthetic-reasoning benchmark.
- pragmatic_meaning_from_non_verbal (2026.acl-long.2101) — LLM inference from non-verbal responses.
- prompt_duel_optimizer (2026.findings-acl.490) — label-free prompt optimization (dueling bandits).
- prosody_of_emojis (2026.acl-long.1459) — emoji-prosody link in speech.
- style_over_story (2026.findings-acl.1361) — LLM narrative preferences via structured selection.
- syntactic_influence_in_llm_metaphor (2026.acl-long.1286) — metaphor-processing probing.
- texocr (2026.acl-long.1658) — OCR→compilable LaTeX with RL verifiable rewards; OCR-adjacent but scientific-PDF/LaTeX, far from manuscripts.
