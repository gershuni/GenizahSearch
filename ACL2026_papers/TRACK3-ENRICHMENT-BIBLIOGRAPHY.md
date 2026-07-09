# Track-3 Enrichment Bibliography — literature sweep seeded by MiqraBERT

Compiled 2026-07-08. Four parallel research agents, disjoint lanes, seeded by a
close read of **MiqraBERT** (Smiley, Notre Dame, arXiv:2606.19638, 17 Jun 2026)
— the published proof-of-concept of exactly our planned Track-3 (SBERT
fine-tune of AlephBERT, cosine-similarity regression on ~1,650 Biblical-Hebrew
verse pairs; **narrative recall@10 ≈ 87%, poetic/paraphrase recall@10 < 9%**;
evaluated by Wasserstein distance + overlap coefficient across 10 seeds).

All URLs were checked live by the agents; a few snippet-only entries are marked
LOW/MED-CONFIDENCE. The **integrated recommendation** lives in
`TRACK3-DECISION-BRIEF.md` — this file is the raw evidence base.

---

# Lane A — Neural text-reuse / intertextuality / allusion in ancient & low-resource languages

**Intertextual Parallel Detection in Biblical Hebrew: A Transformer-Based Benchmark** — Smiley (arXiv, 2025). https://arxiv.org/abs/2506.24117
The **direct predecessor to MiqraBERT.** Benchmarks off-the-shelf E5, AlephBERT, MPNet, LaBSE on Samuel/Kings↔Chronicles parallels by cosine + Wasserstein. Key result MiqraBERT builds on: **E5 has highest raw cosine (best at *finding* parallels) but a tiny Wasserstein gap ≈0.081; AlephBERT has lower cosine but a much larger gap ≈0.276 (best at *separating* non-parallels).** The origin of the WD/overlap metric and the "highest similarity ≠ best separator" trap. HIGH. Caveat: clean, narrow narrative set only.

**Graecia capta… Detecting Latin Allusions to Ancient Greek Literature (SPhilBERTa)** — Riemenschneider & Frank (ACL 2023). https://arxiv.org/abs/2308.12008
Trilingual Sentence-RoBERTa (Ancient Greek / Latin / English); trains cross-lingual pairs by **machine-translating English into Ancient Greek** to manufacture parallels. Template for Heb↔JA cross-lingual reuse when gold pairs don't exist. HIGH. Caveat: strongest on near-identical cross-lingual sentences; medieval-JA MT quality is far worse than En→Greek.

**Loci Similes: A Benchmark for Extracting Intertextualities in Latin Literature** — (arXiv, 2026). https://arxiv.org/abs/2601.07533
~172,000 segments, 545 expert-verified parallels spanning "verbatim → subtle allusion → paraphrase disguised by morphological variation"; retrieval+classification with LLM baselines. Mirrors our huge-pool / tiny-gold reality; a model for honest sparse-gold reporting. MED (abstract only).

**Detecting Semantic Reuse in Ancient Greek Literature** — D'Angelo, Taddei & Lenci (CLiC-it 2025). https://aclanthology.org/2025.clicit-1.34/
**Contrastive fine-tuning + linguistically-informed data augmentation**, evaluated by recall@10 retrieving Homeric *formulae*. The single most on-point recipe for beating MiqraBERT's poetic collapse — Greek formulae are structurally close to piyyut/liturgical formulae. HIGH. Caveat: augmentation rules are Greek-morphology-specific; case study, not a full benchmark.

**Profiling of Intertextuality in Latin Literature Using Word Embeddings** — Burns et al. (NAACL 2021). https://aclanthology.org/2021.naacl-main.389/
Extends the **Tesserae** lexical engine with word2vec/fastText similarity as a re-ranker over lexical candidates. The canonical "bolt a semantic layer onto a lexical seed-and-extend pipeline without discarding it" — architecturally closest to how Track-3 rides on our Tracks 1–2. HIGH. Caveat: static embeddings lose word order / long-range meaning (the paraphrase cases).

**Latin BERT: A Contextual Language Model for Classical Philology** — Bamman & Burns (arXiv 2020). https://arxiv.org/abs/2009.10053
Domain-pretrained Latin BERT (642.7M words) + contextual nearest-neighbor search finds passages missed by static embeddings. Proof that base-encoder quality caps everything downstream, and that "embed-everything + ANN-query" is a viable corpus-scale retrieval architecture. MED-HIGH. Caveat: pre-SBERT; needs pooling/fine-tune for sentence similarity.

**SCAD: Self-Supervised Contrastive Learning for Allusion Detection in Chinese Poems** — (Humanities & Social Sciences Communications, 2026). https://www.nature.com/articles/s41599-026-06627-z
Self-supervised contrastive model with an explicit **Negative Sampling Layer** + SikuBERT; **zero-shot detection of unseen allusions** (1,025 allusions / 14,016 poems; ~0.87 vs 0.80 baseline). Clearest worked example of *negative construction as the core design problem* + generalization to sources not in the training set. MED (snippet). Caveat: Classical-Chinese allusion = reuse of a known fixed phrase; looser than our multilingual paraphrase.

**MITRA: Parallel Corpus & Multilingual Model for Pali/Sanskrit/Buddhist-Chinese/Tibetan** — (arXiv, 2026). https://arxiv.org/abs/2601.06400
Gemma-2-based MT + **MITRA-E** cross-lingual embedder; a passage-mining pipeline produced **~1.74M sentence pairs** with no hand annotation. Direct template for turning our 1.34M lexical candidates into embedding-training data. MED. Caveat: Buddhist canon parallels are near-translations (high equivalence); Gemma-2-scale is heavy for our infra.

**Quantitative Intertextuality from the DH Perspective: A Survey** — Siyu Duan (arXiv 2025). https://arxiv.org/abs/2510.27045
Best framing document. Cautions we should heed: eval standards are ambiguous (sample-for-precision, partial-gold-for-recall); LLMs now generate *both* positive and negative training samples; and — pointed — **for Hebrew-Aramaic with heavy spelling variation, char-level overlap remains more appropriate because deep-model training data is scarce** (don't discard the lexical backbone); cross-lingual intertextuality is barely explored (Heb↔JA is near-frontier). MED (survey).

**Modelling Intertextuality with N-gram Embeddings** — Yi Xing (arXiv 2025). https://arxiv.org/abs/2509.06637
N-gram-level embeddings (keeps local order) aggregated to a document-level score + network/centrality view. A middle granularity possibly robust to fragmentary/lacunose lines. LOW-MED (abstract; appears English/general, not ancient-language).

---

# Lane B — The craft of training the embedding model (where our scale / graded labels / hard negatives pay off)

**Sentence-BERT** — Reimers & Gurevych (EMNLP 2019). https://arxiv.org/abs/1908.10084
The bi-encoder/siamese template MiqraBERT instantiates; also the triplet objective as an alternative to cosine regression. The control that frames every change below as "keep the architecture, change the data + loss." HIGH.

**SimCSE** — Gao, Yao & Chen (EMNLP 2021). https://arxiv.org/abs/2104.08821
Contrastive (InfoNCE) with many in-batch negatives; supervised variant uses NLI **contradiction as a hard negative**. Shows one curated hard negative per anchor closes most of the gap to SOTA — validates replacing MiqraBERT's cosine-MSE with a ranking-aware contrastive loss. HIGH. Caveat: dropout-aug + NLI supervision are English-centric; our hard-negative slot must be filled by *our* near-misses.

**ANCE** — Xiong et al. (ICLR 2021). https://arxiv.org/abs/2007.00808
Mines hard negatives from a periodically-refreshed ANN index of the *current* model's embeddings. The canonical argument that random/in-batch negatives give vanishing gradients. HIGH. Caveat: assumes negatives are *true* negatives — over noisy Genizah text, top-ANN neighbors are often genuine reuse (false negatives) → pair with denoising.

**RocketQA** — Qu et al. (NAACL 2021). https://arxiv.org/abs/2010.08191
Cross-batch negatives + **denoised hard negatives** (a cross-encoder drops likely false negatives before use) + cross-encoder data augmentation for positives. **Most directly on-point for us:** our hard negatives are the engine's near-misses — exactly the population contaminated with real reuse; our density score is the denoising filter for free. HIGH. Caveat: denoising needs a teacher more accurate than the miner; a *lexical* density score may miss pure-paraphrase false negatives.

**SimANS** — Zhou et al. (EMNLP 2022). https://arxiv.org/abs/2210.11773
The *hardest* negatives are disproportionately false negatives; sample an **ambiguous band just below the positive's score**, not top-1 hardest. Turns our graded density score into a sampling distribution. HIGH. Caveat: the band is defined by the retriever's own scores; substituting the lexical density score needs re-tuning.

**SNCSE** — Wang et al. (2022). https://arxiv.org/abs/2201.05979
**Soft negatives** (high lexical overlap, opposite meaning) + a Bidirectional Margin Loss to resist "feature suppression." Names the exact failure of any lexical-overlap-trained model — treating shared vocabulary as reuse. MED-HIGH. Caveat: their soft negatives are rule-synthesized; ours would be mined (noisier).

**RankCSE** — Liu et al. (ACL 2023). https://arxiv.org/abs/2305.16726
**Listwise ranking distillation** from a teacher — learns fine-grained *order*, not binary. Operationalizes "learn from a continuous score" — our per-pair density score is exactly the graded signal to distill. HIGH. Caveat: distilled order inherits the lexical teacher's paraphrase blind spot.

**Margin-MSE (cross-architecture distillation)** — Hofstätter et al. (2020). https://arxiv.org/abs/2010.02666
Regress onto a teacher's *continuous margin* score(pos)−score(neg). Bridges MiqraBERT's comfort zone (a regression head) with our advantage (graded scores): same MSE loss, but the target is the **density-score margin** of an (anchor, reuse, near-miss) triplet — the lowest-friction binary→graded upgrade. HIGH. Caveat: needs calibrated/comparable teacher scores.

**Augmented SBERT** — Thakur, Reimers, Daxenberger & Gurevych (NAACL 2021). https://arxiv.org/abs/2010.08240
Cross-encoder labels a large sampled set → **silver** bi-encoder data; up to +37 pts on domain adaptation; **pair-selection strategy dominates results.** Canonical validation of our "mint silver labels at scale" plan; invest in the mining distribution, not just volume. HIGH. Caveat: their labeler is a strong cross-encoder; ours is *lexical* → silver is precision-high / recall-low for the very paraphrase Track-3 targets.

**E5 (weakly-supervised contrastive pre-training)** — Wang et al. (2022). https://arxiv.org/abs/2212.03533
Full modern recipe for turning a large, noisy, weakly-labeled pair set (our 1.34M) into strong embeddings — InfoNCE, big batches, in-batch + mined negatives, and a **consistency filter** cleaning weak pairs before training. HIGH. Caveat: CCPairs is web-scale English; batch sizes + cleaning heuristic must shrink/re-tune for our smaller, multilingual, OCR-noisy corpus.

**CharacterBERT + Self-Teaching for typo-robust dense retrieval** — Zhuang & Zuccon (SIGIR 2022). https://arxiv.org/abs/2204.00716
Character-aware backbone + **self-teaching**: distill clean-text output into typo-perturbed input so noisy & clean forms embed identically. Directly on-point for handwriting/OCR noise; another silver-label source we can already produce. MED-HIGH. Caveat: their noise model is English keyboard typos; needs a Hebrew/JA OCR-error model + an AlephBERT-compatible char-aware variant (may not exist off-the-shelf).

*(Isotropy hedge: BERT-whitening, https://arxiv.org/abs/2103.15316 — post-hoc fix if cosine scores stay compressed.)*

---

# Lane C — Hebrew / Semitic / Judeo-Arabic encoders & benchmarks (base-model bake-off)

> NOTE: This lane predates knowledge of our **internal** JABERT / RamBERT models.
> Its "transliterate-then-Arabic-encode" JA recommendation is **superseded** by
> the internal-models section of `TRACK3-DECISION-BRIEF.md`. The public-model
> survey remains valid for the Hebrew slice and as external baselines.

**AlephBERT / AlephBERTGimmel** — ONLP (Bar-Ilan)+DICTA (ACL 2022 / 2022). https://arxiv.org/abs/2104.04052 · https://arxiv.org/abs/2211.15199
The seed's base (MiqraBERT fine-tuned AlephBERT). ABG bumps vocab 52K→128K. Incumbent-to-beat; Modern-Hebrew register — mismatched for Rabbinic and hard-mismatched for Judeo-Arabic. HIGH.

**DictaBERT** — Shmidman, Shmidman & Koppel, DICTA (2023). https://arxiv.org/abs/2308.16687 · https://huggingface.co/dicta-il/dictabert
Newer Modern-Hebrew SOTA, overtook AlephBERT(Gimmel). Cheap A/B candidate; no Rabbinic/JA advantage. HIGH.

**BEREL / BEREL 3.0** — Shmidman et al., DICTA (2022). https://arxiv.org/abs/2208.01875 · https://huggingface.co/dicta-il/BEREL_3.0
Pretrained on **Rabbinic** Hebrew (Talmud/midrash/halakha) — best register match for the Rabbinic-Hebrew slice of the Genizah. HIGH for Rabbinic. Caveat: encoder only (needs the same SBERT fine-tune); nothing for JA.

**NeoDictaBERT / NeoDictaBERT-bilingual(-embed)** — DICTA (Oct 2025). https://arxiv.org/abs/2510.20386 · https://huggingface.co/dicta-il/neodictabert-bilingual-embed
Most current Hebrew base (NeoBERT arch, 4,096-token context, RoPE) trained on Hebrew+English; the **only Hebrew model with a purpose-built embedding/retrieval variant** and long context. MED-HIGH. Caveat: **no Arabic/JA**; retrieval delta small and Hebrew-only.

**MsBERT** — Shmidman et al., DICTA (ML4AL@ACL 2024). https://aclanthology.org/2024.ml4al-1.2/ · https://huggingface.co/dicta-il/MsBERT
Built on **Hebrew manuscript transcriptions** — closest register+medium match to Genizah manuscript Hebrew; its "quoted passage vs elaboration" task is a reuse primitive. HIGH for manuscript Hebrew. Caveat: MLM-oriented; Hebrew-script only.

**Embible** — Ben-Gurion U. et al. (Findings of EACL 2024). https://aclanthology.org/2024.findings-eacl.56/
Ensemble word+char transformers for Biblical Hebrew **and Aramaic** reconstruction (Targum-relevant); char models damage-robust. MED. Caveat: no clean single checkpoint; ensemble/method paper.

**TavBERT** — Keren, Avinari, Tsarfaty & Levy (2022). https://huggingface.co/tau/tavbert-he
**Character-level** Hebrew (tokenizer-free) — sidesteps vocab mismatch on noisy/OCR/variant orthography and shares *characters* with JA-in-Hebrew-script. MED. Caveat: long sequences (cost); usually trails subword models on semantic tasks. A robustness hedge, not a favorite.

**HeBERT** — Chriqui & Yahav (2020). https://huggingface.co/avichr/heBERT — legacy baseline/floor. HIGH (superseded).

**A Tale of Two Scripts (Judeo-Arabic transliteration + upper-dot)** — Moreno Gonzalez, Alhafni & Habash, NYU-AD CAMeL (2025). https://arxiv.org/abs/2507.04746
Hebrew-script→Arabic-script transliteration for classical JA; the **upper-dot** diacritic lifts accuracy 53.84%→65.10% (dotted → GPT-4o 90.4% w/ post-correction) — same JA upper-dot form our SEED-006 fix normalizes. Confirms JA freely code-switches into Hebrew/Aramaic. HIGH. Caveat: transliteration/tagging/MT, not an embedding model.

**CAMeLBERT-CA** — CAMeL Lab, Inoue et al. (WANLP 2021). https://huggingface.co/CAMeL-Lab/bert-base-arabic-camelbert-ca
Classical-Arabic BERT (864M tokens); "pretraining-domain proximity beats data size." The natural encoder *if* one transliterates JA→Arabic script — but see internal-models reframe. MED-HIGH.

**Swan + ArabicMTEB** — Bhatia et al., UBC/MBZUAI (2024). https://arxiv.org/abs/2411.01192
Arabic embedding models + 94-dataset Arabic eval harness; Arabic-specialized > multilingual on Arabic (Swan-Large 62.45 > mE5-large 61.65). HIGH. Caveat: MSA/dialect, not Classical/medieval — our JA register is out-of-domain even here.

**Multilingual embedders — BGE-M3, multilingual-E5, LaBSE** — https://arxiv.org/abs/2402.03216 · https://arxiv.org/abs/2402.05672 · https://arxiv.org/abs/2007.01852
The only single models natively spanning Hebrew **and** Arabic script; the baselines specialized models report beating; LaBSE = reference cross-lingual/bitext model. HIGH. Caveat: none tuned for medieval Hebrew or JA — a floor, not a ceiling; cross-script similarity is exactly where "signal dies crossing scripts" bites.

**MMTEB / MTEB** — Enevoldsen et al. (2025) https://arxiv.org/abs/2502.13595 · Muennighoff et al. https://arxiv.org/abs/2210.07316
Eval-harness *shape* (retrieval/STS/clustering/bitext) to rank candidate bases label-free on our 1.34M verified pairs. HIGH. Caveat: Hebrew coverage thin, JA absent — our verified-pairs set is the only on-target benchmark; borrow the shape, not the scores.

---

# Lane D — Honest large-scale (gold-free) evaluation + the paraphrase/poetry failure

**Intertextual Parallel Detection in Biblical Hebrew** — Smiley (2025). https://arxiv.org/abs/2506.24117
(Also Lane A.) Origin of the WD/overlap metric; the **E5 trap** — highest raw cosine, worst separation. Report distribution-separation, not just top-k. HIGH.

**QPP-GenRE (Query Performance Prediction via LLM judgments)** — Meng et al. (2024; TOIS 2025). https://arxiv.org/abs/2404.01012
Estimate retrieval quality with **no human labels** via per-item LLM relevance judgments, or read the top-k score distribution as a confidence signal. A path to per-query quality estimates over 1.34M pairs + triage. MED-HIGH. Caveat: validate any LLM judge on a small expert-labeled Heb/JA slice first.

**Calibrated similarity / isotonic calibration of cosine** — (arXiv, 2026). https://arxiv.org/abs/2601.16907
Raw cosine is rank-correct but **miscalibrated in absolute value** (anisotropy piles scores into a narrow band); isotonic regression on a few hundred labels calibrates it, and **quantile thresholds survive re-embedding / model swaps**. Explains why a fixed cosine threshold misbehaves. MED. Caveat: needs *some* labels.

**Topological metric for unsupervised embedding-quality evaluation** — (arXiv, 2025/2026). https://arxiv.org/abs/2512.15285
Persistent-homology / geometry measures of embedding structure, **fully label-free** — a cheap model-selection screen among Track-3 variants before spending human labels. MED. Caveat: geometry correlates weakly with the specific task; a screen, not an acceptance test.

**An Upper Bound on the Silhouette Metric** — (arXiv, 2025). https://arxiv.org/abs/2509.08625
Clusters that overlap/are non-convex cap attainable silhouette. Honesty guardrail: a low poetic separation may be **intrinsic to the genre**, not a bad model — report genre-stratified separation against the data ceiling; don't over-tune to a capped metric. MED. Caveat: silhouette assumes discrete clusters; use on derived clusterings, prefer WD/overlap/AUC on pair scores.

**Situating Sentence Embedders with Nearest Neighbor Overlap** — N. Liu, Schuster & Smith (2019). https://arxiv.org/abs/1909.10724
Embedders' "popular" neighbors have **high lexical overlap** with the query — the mechanistic cause of the poetry failure (paraphrase = low overlap → falls out of top-k). Also a gold-free diagnostic (NN-overlap) of how lexically-biased Track-3's neighbors are. MED-HIGH. Caveat: on English general text.

**On the Feasibility of Automated Detection of Allusive Text Reuse** — Manjavacas, Long & Kestemont (LaTeCH-CLfL 2019). https://aclanthology.org/W19-2514/
Closest prior analog: allusion in a historical religious corpus, "none or very few shared words." Semantics gives only a **moderate** boost over lexical; **query/window construction matters more than the encoder.** Realistic expectation-setter for liturgy. HIGH. Caveat: pre-SBERT (FastText).

**NV-Retriever (positive-aware hard-negative mining)** — Moreira et al., NVIDIA (2024). https://arxiv.org/abs/2407.15831
TopK-MarginPos / TopKPercPos mining filters false negatives while keeping genuinely hard ones. The concrete lever to rank low-overlap paraphrase above lexically-similar-but-unrelated; false-negative filtering is *harder* in a reuse-saturated corpus (liturgy) → corpus-specific thresholds. MED-HIGH.

**E5 (weakly-supervised contrastive pre-training)** — Wang et al. (2022). https://arxiv.org/abs/2212.03533
(Also Lane B.) Backbone recipe for **paraphrase-robust** embeddings; double lesson — strong paraphrase recall but (per 2506.24117) worst separation, so still calibrate. MED. Caveat: limited Heb/JA coverage → multilingual variant / in-domain continued pretraining.

**Detecting Semantic Reuse in Ancient Greek Literature** — D'Angelo et al. (CLiC-it 2025). https://aclanthology.org/2025.clicit-1.34.pdf
(Also Lane A.) Contrastive fine-tune + linguistically-informed augmentation = the plausible lever to lift poetic recall off <9%. MED.

*(Honorable mentions: EMD for cross-lingual embedding eval https://arxiv.org/abs/1910.11005 (LOW); Loci Similes benchmark https://arxiv.org/abs/2601.07533 (LOW).)*
