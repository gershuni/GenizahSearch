# ACL 2026 — Genizah-relevant papers library

Compiled 2026-07-08. Six-agent scan of the local mirror (5,019 papers) against
`_scan/RELEVANCE-PROFILE.md` (9 relevance buckets mapped to SEED-029 components
and GenizahSearch product needs). Coverage: NLP4DH complete (36), Topic_OCR
complete (24), all 91 curated session picks, ~185 low-resource/multilingual/MT/
morphology/semantics track papers, ~508 retrieval/RAG/resources track titles
(strict triage), plus a 122-title domain-keyword sweep across the whole index.
**Verdicts: 28 A (act on) · ~64 B (design-informing) · rest C.**
Full evidence cards live in `_scan/*-cards.md` (schema: tl;dr / relevance /
stealable / bib leads); this file is the navigable synthesis.

Not scanned (beyond keyword hits): the big generic tracks (Language Models 480,
AI/LLM Agents 418, Safety 324, Multimodality 315, Interpretability 168, LLM
Efficiency 272…) and the non-NLP4DH workshops (SCiL, CoNLL, GEM, SemEval, LAW,
*SEM, MELLM…) — a second wave can cover any of these on request.

---

## 1. The one-paragraph verdict

ACL 2026 converges on exactly the program we sketched after the Tibetan paper:
**silver labels from a lexical engine + LLM-annotator committees → contrastive
embeddings for historical low-resource text**. Nothing we found invalidates the
SEED-029 lexical stack — several papers independently validate its design
choices (multi-algorithm collation robustness, DF/term-weighting, witness-recall
over CER). The new value is concentrated in (a) a now-fully-specified **Track-3
recipe** with every design question answered by a specific paper, (b) a
**JA↔Hebrew cross-lingual toolkit** that did not exist in one place before,
and (c) a **restoration product line** (lacunae completion from parallel
witnesses) with three strong method precedents plus MsBERT in-house.

---

## 2. A-tier by theme (act on these)

### 2.1 Text reuse & intertextuality (validates + extends Track-1/2)
| Paper | Where | Take |
|---|---|---|
| Miyagawa & Kyogoku — **Yajurveda multi-algorithm collation (ICoMa)** | nlp4dh-1.5 | 5-metric agreement as robustness check; per-section reuse profiles as transmission evidence; **ICoMa supports Hebrew script** |
| Wu & Tolonen — **Semantic search vs lexical reuse (Locke)** | nlp4dh-1.20 | The Track-3 argument with an honest ceiling ("lexical gatekeeping"); 4-tier reception taxonomy = our label schema; dense retrieval robust to OCR noise |
| Reichbauer & Fraser — **Latin↔Greek parallel-sentence mining** | nlp4dh-1.11 | CSLS-over-cosine + whitening + distillation; synthetic needles-in-corpus benchmark design — the JA↔Hebrew starter kit |

### 2.2 Track-3 embeddings: training recipe (every knob now has a paper)
| Design question | Answer | Paper |
|---|---|---|
| Base model (Hebrew) | prefix-segmented historical-Hebrew BERT (Dicta/Bar-Ilan, our register) + BEREL/DictaBERT | Tal & Shmidman, nlp4dh-1.12 |
| Base model (JA/Arabic) | NeoAraBERT (classical+dialectal, diacritics-aware tok, POS-masking, Muradif synonym probe) | sessions A, findings-1293 |
| Model selection w/o labels | FLARE normalizing-flow information sufficiency (ρ≈0.9 with supervised benchmarks) | retrieval A, findings |
| Loss for graded labels | DiffCL hierarchical contrastive (separation ∝ score gap) — fits our density tiers | lowres A, Semantics |
| Hard negatives | Validator-guided mining (validator = OUR lexical engine); `N_hard = V(i) \ B5(i)`; +68.7 pts on Sumerian | sessions A, acl-srw.69 |
| Silver-label threshold | graded→binary cutoff tuned per language; cuts data + denoises (OriginAI, Israeli) | retrieval A |
| Benchmark construction | MTEB-NL/E5-NL pipeline (+ SkMTEB vocab-trim recipe); **avoid formulaic-corpus leakage** — target-level 8-gram dedup (Toutou, hieroglyphic contamination) | retrieval A + sessions B + nlp4dh B |
| Long-doc / multilingual pooling bias | permutation discoverability audit + inference-time attention calibration (Zurich/impresso) | lowres A |
| Rare-script embedding | ACSE contrastive glyph+dictionary fusion into an LLM space | keyword A, findings-437 |
| Corpus-scale ANN | Isolation-Kernel learning-free binary embeddings (16× smaller/faster) | retrieval A |

### 2.3 JA↔Hebrew cross-lingual toolkit
| Paper | Take |
|---|---|
| Verma et al. — **encoders cluster by SCRIPT, not language** (IIT Delhi) | JA will sit with Hebrew, not Arabic; blind transliteration creates disjoint reps — decide script policy FIRST, probe with LAPE/SAE |
| LangSAE editing (Korea U) | post-hoc SAE removal of language identity from cached embeddings — no retraining, vector-DB compatible |
| CLEAR reverse-training (Korea U) | bridge-language contrastive loss lifting the low-resource side without degrading the high-resource side |
| Code-switching IR benchmark (Zeng et al.) | JA is code-switched by nature; embedding-divergence diagnostic + benchmark template |
| Wein & Pacheco — **translationese detection** (94% F1) | flag translated witnesses before embedding training; lineage → Koppel & Ordan (Bar-Ilan) |
| (B, but load-bearing) Mixed-language pretraining ablation | PARALLEL data, not code-switching, carries token-level alignment — invest in mined JA↔Hebrew parallels |

### 2.4 HTR noise, restoration, and the lacuna product
| Paper | Take |
|---|---|
| Sun & Zhang — **When Good OCR Is Not Enough** (Qihoo) | low CER ≠ retrieval success; structural errors dominate — evaluate by witness-recall (citable precedent for our metric choice) |
| Sarawgi et al. — **Old Nepali HTR pipeline** (LMU/Heidelberg) | token-level confusion analysis → substitution-cost-weighted Levenshtein for our verifier |
| Kim & Kang — **RAG restoration of damaged historical documents** | MLM fails on entities needing external knowledge; retrieve-then-infill with OUR witness census as the store |
| Zhang & Jin — **UniHIR draft→localize→self-refine restoration** (SCUT) | controllable lacuna reconstruction loop + human-in-the-loop gate + OCR-based metric |
| Gershuni & Shmidman — see 2.5 | the QC layer for anything restoration produces |

### 2.5 LLM-as-annotator (the Track-3 label factory)
| Paper | Take |
|---|---|
| **Gershuni & Shmidman — Hebrew diacritization error auditing** (LAW; our own) | LLM auditor 83.6R/99.1P beats union of two human experts; >95% review-effort cut — the QC protocol, transfers as-is |
| Gu & Boyd-Graber — LLMs assist, don't replace annotators | 3-mode eval harness (AI-only/assist/human-only); measure expert-adoption-rate + time-saved |
| Kim & Aizawa — iterative guideline refinement for LLM annotators | evolve the annotation prompt like an early-phase annotation project |
| Don-Yehiya et al. — **mediocre anchor** for LLM-judge ranking (HUJI/IBM, Israeli) | avoid O(n²) pairwise judging: anchor on a mid-strength reference |
| (B) Kulmizev & de Marneffe — LLM ensembles ≠ human variation | sample across families/temps; explanation diversity as a quality signal |
| (B) MADRAG advocate/skeptic/judge + exemplar retrieval | retrieval drives calibration — build an exemplar bank of scored pairs |

### 2.6 Hebrew/Semitic resources & product
| Paper | Take |
|---|---|
| Greenfeld & Tsarfaty — **Hebrew coreference + morpheme-aware eval** (Bar-Ilan) | boundary-aware scoring template for any Hebrew span task; small encoders beat decoders on Hebrew |
| (B) AdabNER — nested NER over literary Arabic archives (Jarrar) | catalog/archive entity tagging; multi-domain training closes OOD gap |
| (B) Neural FST induction (Hulden) | fast inspectable orthographic-normalization/transliteration transducers |
| (B) ViDoRe V3 / ColPali lineage | retrieve over page IMAGES, bypassing HTR — watch as an alternative track |

---

## 3. Second-degree reading list (bibliography votes across cards)

Ranked by how many independent cards chased them + directness to our problems:

1. **Assael, Sommerschield et al. — Ithaca (Nature 2022) + Aeneas (Nature 2025)** — 4+ votes. THE ancient-text restoration/attribution line; required reading before the lacuna product.
2. **BGE-M3 / M3-Embedding (Chen et al. 2024)** — 5+ votes as the default multilingual multi-granularity retriever baseline.
3. **Shmidman et al. — MsBERT (2024, Hebrew manuscript lacunae; Hillel co-author)** — the in-house restoration precedent; combine with #1 and the RAG-restoration card.
4. **Riemenschneider & Frank 2023 — SPhilBERTa, "Graecia capta" (Latin→Greek allusion detection)** — cross-lingual *allusion* detection = our exact task, one step beyond translation alignment.
5. **Mahadevan et al. 2025 — text reuse in large historical corpora (optimization)** + **Düring et al. 2023 — impresso Text Reuse at Scale** — the two peer systems to benchmark our engine against.
6. **Swan & ArabicMTEB (Bhatia et al. 2025)** — 3 votes; dialect-aware Arabic embedding suite — the closest existing "MTEB-Semitic".
7. **NV-Retriever (Moreira et al. 2024)** — 3 votes; the hard-negative mining reference.
8. **Lazar, …, Stanovsky 2021 (Akkadian MLM gap-filling) + Fetaya, …, Gordin 2020 (fragmentary Babylonian, PNAS)** — the Israeli ancient-restoration line.
9. **Tesserae (Coffee et al. 2013)** + **Ugarit (Yousef et al. 2022)** — classic intertextuality system + ancient translation-alignment tool.
10. **Late chunking (Günther et al. 2024/25)** + **"Dwell in the Beginning" (Coelho et al. 2024)** — long-doc embedding bias/context toolkit (2 votes each).
11. **Koppel & Ordan 2011 — translationese** — Bar-Ilan lineage behind the Wein card.
12. **Gorman & Pinter 2025 — "Don't touch my diacritics"** — directly our diacritic normalization question.
13. **Northcutt 2021 Confident Learning + Swayamdipta 2020 Dataset Cartography** — label-noise QC under silver labels.
14. **Sommerschield et al. 2023 — ML for ancient languages: a survey** — the field map.

## 4. People / labs worth contacting

- **In-house & Israel**: Avi Shmidman (Dicta/Bar-Ilan — Dalet-clitic BERT, MsBERT, BEREL: Track-3 base models live here); Reut Tsarfaty ONLP (Bar-Ilan); Ido Dagan lab (QASemConsistency); Don-Yehiya/Yehudai/Choshen/Abend (HUJI/IBM — LLM-judge); OriginAI (Wullach/Shapira/Cohen — graded thresholds); Reichman DSI (Shay Cohen/Kfir Bar/Shai Fine — the Tibetan STS reference group); Nir Grinberg (BGU); Moshe Koppel (translationese lineage).
- **International DH/philology**: Alexander Fraser group (TUM — ancient parallel mining); Amir Zeldes (Georgetown — Coptic); So Miyagawa (Tsukuba — ICoMa collation, Hebrew-script support); TurkuNLP + Helsinki COMHIS (Ginter/Tolonen — reuse at scale); Zurich impresso (Clematide/Opitz/Sennrich — historical multilingual embeddings); Garces Arias (LMU/Heidelberg — historical HTR); Daelemans (Antwerp — MTEB-NL blueprint); Lianwen Jin lab (SCUT) + Jilin ancient-script lab (restoration/ancient embeddings); Mustafa Jarrar (Birzeit — Arabic NER); Abdul-Mageed (UBC — dialectal Arabic).

## 5. What changes Monday morning (action shortlist)

1. **Track-3 kickoff is de-risked**: bake-off {Dalet-prefix BERT, BEREL/DictaBERT, NeoAraBERT, BGE-M3, mE5-trimmed} ranked label-free by FLARE + a Muradif-style Hebrew synonym probe; train with DiffCL graded loss on our density tiers; hard negatives via the validator recipe from our own lexical engine; calibrate the silver threshold per language (Wullach); split with target-level 8-gram dedup (Toutou).
2. **Script policy before any JA work**: run the Verma LAPE/SAE probe on candidate models; plan for LangSAE post-hoc language-identity removal; benchmark with the code-switching IR template; flag translations with a translationese classifier first.
3. **Adopt the auditing loop we already own** (Gershuni & Shmidman LAW protocol) as the QC layer over all silver labels; BWS + mediocre-anchor + guideline-refinement for the committee design.
4. **Lacuna-completion product spike**: MsBERT + RAG-restoration + UniHIR self-refine loop, retrieval store = our witness census; human-in-the-loop gate in the web app.
5. **Cite-ready validations**: witness-recall over CER (Qihoo); multi-algorithm robustness (ICoMa); "lexical gatekeeping" as the Track-3 motivation (Wu & Tolonen).
6. **Verifier upgrade (cheap)**: build the Hebrew HTR confusion matrix into substitution-cost weights for banded Levenshtein (Nepal HTR precedent).
