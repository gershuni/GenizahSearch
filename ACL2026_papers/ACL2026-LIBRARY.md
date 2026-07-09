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

---

# WAVE 2 — cross-domain mechanism transfers (the analogy hunt)

Compiled 2026-07-08. Eight agents scanned the tracks wave 1 skipped
(Interpretability 189, ML-for-NLP+*SEM 212, Safety 324, Multimodality 315,
Efficiency+LM-scale 752, Agents+coref+QA+Summ 523, formal/cognitive
SCiL+CoNLL+CogPsy 140, cross-domain long tail ~562) against
`_scan2/WAVE2-PROFILE.md`. Different question from wave 1: **not "does this
match our domain" but "does this paper's mechanism solve a problem isomorphic
to one of ours, regardless of surface domain."** Twelve domain-neutral target
problems (A noisy alignment · B all-pairs near-dup · C competitive assignment ·
D change-point/segmentation · E graph clustering · F lineage/tree · G
contrastive-from-graded-labels · H calibration-without-gold · I concept
removal/disentanglement · J rare-event · K draft-then-verify · L
cross-representation alignment). Apophenia guard on: every A-card carries a
"why it might NOT transfer" line + a transfer-confidence rating. Cards in
`_scan2/*-cards.md`. Verdicts ~57 A / ~103 B, but **the count is not the
product — the convergences are.**

## The premise, vindicated
The single most useful find comes from a field with no connection to
manuscripts, philology, or history at all (TIGER — bidirectional retrieval
between two very different representations of the same underlying object), and
the fill for our biggest capability gap came from an **LLM-data-provenance
paper** and a **social-bot-detection paper**.
None would ever surface under a "text reuse / Genizah / Hebrew" search. When
the same reformulation arrives independently from tracks that don't cite each
other, that convergence is the signal to trust.

## Convergence 1 — span-shadowing IS an assignment problem (problem C)
Our hand-tuned `track1_shadow.py` (keep-best-span, mark-overlap-shadowed) drew
**five independent principled reformulations from four unrelated tracks**, and
they partition the design space by which assumption fits:
- **Confidence-weighted set cover** (Sultan & Astudillo, LLM-Efficiency) —
  *the highest-confidence transfer of the whole wave.* Minimal weighted set of
  spans covering the region; greedy with a (1−1/e) guarantee; drops into
  `track1_shadow.py`, fold ownership into the weights. **HIGH.**
- **Unbalanced Optimal Transport** (SCOUT, Interpretability) — differentiable,
  and its whole point is that noise legitimately matches *nothing* ("geometric
  mass suppression") = the many-to-many case. **HIGH** (verification-stage only,
  O(n·m)).
- **Bipartite matching** (MatchTIR, Agents) — the clean one-to-one objective,
  with the honest caveat *reuse is many-to-many → needs the OT relaxation above.*
- **Stable matching under probabilistic preferences** (Kong & Shen, LM) — when
  match scores are themselves uncertain (Expected Blocking Pairs).
- **Deferral** (Kim & Danescu-Niculescu-Mizil, CSS) — don't commit the shadow
  while a better-covering span could still emerge as the alignment extends.
Plus a meta-nudge: *learn the selector instead of hand-coding NMS* (Measure
Twice Click Once, Multimodality). **Action:** reimplement shadowing as
weighted set-cover now; keep UOT for the semantic verification tier.

## Convergence 2 — the stemma we don't build (problem F), now over-determined
Wave 1 and the Interpretability + formal hunters all reported F as a *null*.
Then it arrived, in force, from tracks that have nothing to do with philology —
and it decomposes into exactly the two halves of classical (Lachmannian)
stemmatics plus the graph machinery to run it:
- **Shared-innovation principle** (When Agents Look the Same, Agents) —
  separate *mandatory* shared features (forced, uninformative) from
  *non-mandatory* shared features (evidence of a common exemplar). This is
  Lachmann's "shared errors, not shared correct readings, prove descent,"
  independently rediscovered for LLM-distillation detection.
- **Directed derivation graph + propagation** (Tracing the Roots, Agents) —
  build a who-derived-from-whom graph; contamination propagates along ancestry
  = a late gloss back-propagating through a copy chain.
- **Distance-tree half** (Baidildinova & Futrell, SCiL) — LM cross-perplexity →
  cophenetic tree distance; asymmetric, alignment-free, mirrors our asymmetric
  matcher.
- **Graph edge-purification** (TRUST, social-bot detection, CSS) — delete
  heterophilous edges (links between probably-different-class nodes), propagate
  labels only from low-uncertainty nodes = purge coincidental phrase-overlap
  edges, bootstrap attributions from confident witnesses.
- Supporting: iterative global-consistency edge pruning (SVRECI, NLG),
  structural+temporal fusion for direction (RCTEA, Safety), attribution as
  bipartite link prediction on a fragment×work graph (GEMS, CSS), dating as a
  hard partitioner (CohTP, Discourse).
**Action:** this is a full method kit for a new capability — spin up a stemma
spike (weight edges by non-mandatory overlap → purify → cross-perplexity tree
→ date-constrained direction).

## Convergence 3 — concept erasure is a mature, verifiable toolkit (problem I)
Stripping scribe-hand / language-identity / script / genre from Track-3
embeddings drew **six removal methods plus, crucially, the gold-free
verification half**:
- Removal: CRISP (SAE concept-unlearning, Safety), ReGLU (orthogonal-complement
  LoRA, Safety), ReAlign (SAE + training-free whitening, Multimodality),
  SAE-NER monosemantic axes (*SEM), sentiment-circuit SAE steer→suppress
  (Sentiment), CLaS-Bench's ranked *menu* of extractors (Interpretability) —
  building on wave-1 LangSAE.
- **Verification (the missing half)**: LEACE + containment/disentanglement
  diagnostics prove erasure worked with *no labels* (Naowarat & Goldwater,
  CoNLL); "From Isolation to Entanglement" (Interpretability) is the caution —
  erasing one factor usually perturbs others, so test multi-concept.
- **The data**: CausalDetox (Safety) evaluates factor-removal on *aligned
  counterfactual pairs* — which we already own as multi-witness copies of one
  work (same text, different scribe/script).
- Bonus: Vocab Diet (Interpretability, HUJI) — spelling variants may be a single
  additive offset we can subtract to normalize plene/defective forms.
**Action:** the plan is de-risked end-to-end — removal + proof + eval data all
in hand. Pilot LEACE (closed-form) with the containment/disentanglement check.

## Convergence 4 — replace hand-tuned density gates with calibrated, gold-free thresholds (problem H)
- **Conformal p-values + FDR control** (Principled Hallucination Detection,
  Safety) — feed n-gram density, Levenshtein ratio, reference score, embedding
  sim as separate scores; get one provably-controlled false-positive budget
  across 1.34M pairs, no manual thresholds. **HIGH.** (CEBC, Multimodality, is
  the per-domain conformal-quantile sibling.)
- **ODASim** (NLP-Applications) — train the similarity to be *monotone in reuse
  strength and absolutely calibrated* from perturbed-gold grades, so a fixed
  threshold means the same thing corpus-wide (−85% ECE). **MED-HIGH.**
- **GIRB isotonic remap** (Summarization) + **Balanced Brier Score** (ML-for-NLP,
  the caveat that high-accuracy regimes hide error-miscalibration).
- **Load-bearing warning**: Dataset Cartography (CODI-CRAC) — raw confidence is
  confounded by class rarity, so naive confidence-pruning would systematically
  purge our *rare works and rare scripts*. Do not threshold on confidence alone.

## Convergence 5 — the HTR confusion matrix is a triple-purpose asset (problems A + G)
Three unrelated papers point at the same artifact from different angles:
- **Edit costs** (problem A): noisy-channel reading with targeted reanalysis
  (Clark, Levy & Gibson — an *eye-tracking* paper) says replace uniform
  Levenshtein costs with a learned letter-confusion prior, and add a reanalysis
  pass that relocates a greedy misalignment using downstream context. (Nepal-HTR
  from wave 1 gives the same edit-cost recipe.)
- **Hard negatives** (problem G): "Semantic Hardness Is Not Visual Hardness"
  (SAN, Multimodality) — mine Track-3 contrastive negatives from *visual
  confusability*, not semantic similarity — i.e. from the confusion matrix.
So one measured confusion matrix serves the verifier, the embedding trainer,
and the negative sampler. Also here: elastic band width driven by local HTR
confidence (EASE, Sentiment); embedding-space DP alignment as a noise-robust
reframing of seed-and-extend (SEA sign-language, Multimodality).

## A-tier singletons worth their own line (beyond the convergences)
- **TIGER** (ML-for-NLP track; a domain far outside text scholarship) —
  asymmetric two-space retrieval via a generated *text pivot*; template for
  JA↔Hebrew / dirty-ms↔clean-edition (L).
- **GenDis** (ML-for-NLP) — generalized category discovery = our **"new?" queue
  (1,168 items)** as a dual-view (surface + Track-3) co-training task (J/G).
- **LSCD benchmark** (*SEM) — pairwise → graph → sense-induction clustering "IS
  our census pipeline"; opens the WSI / correlation-clustering-on-noisy-graphs
  subfield for problem E.
- **LASA semantic bottleneck** (Safety) — probe for the layer where language
  identity drops out; embed/match there for JA↔Hebrew (L/I).
- **LycheeCluster** (Efficiency) — triangle-inequality metric tree = O(log n)
  candidate pruning (B) + boundary-aware chunking (D), two-for-one.
- **HHQ** (Efficiency) — cosine-preserving quantization; beats wave-1's
  Isolation-Kernel binary codes for contrastive embeddings (B).
- **HeteroSpec / Speculative Verification** (Efficiency) — adaptive
  verification budget: spend banded-Levenshtein effort proportional to a cheap
  seed-density signal (K/H).
- **MEIC-DT** (Discourse) — bounded-memory streaming clustering; a principled
  discipline for our disk-spill witness/motif clustering (E).
- **GovScape** (Demo) — exact-match + semantic + facet + **visual** search over
  70M heritage pages for ~$1.5K/10M PDFs; a near-exact product blueprint at 70×
  scale, and the case for adding image-based "find pages that look like X"
  search (product).

## Honest caveats this wave surfaced (worth heeding)
- **Frequency confound** (Momen & Zarrieß, *SEM): "low-DF = distinctive" is
  partly a raw-frequency artifact — control for frequency before crediting a
  rare-gram match.
- **Committee correlation** (single-agent-diversity, Agents; MoMIA, LM): our
  multi-detector "committee" may give correlated, not independent, signal —
  sampling one strong model N times can be more diverse than N models.
- **Stylometry doesn't cross scripts** (LaCava, cross-domain): authorship/
  scribe-hand features do NOT transfer across language families — a direct
  warning for Hebrew↔JA scribe modeling.

## Wave-2 additions to the Monday-morning shortlist
7. **Reimplement shadowing as weighted set-cover** (Sultan & Astudillo);
   evaluate UOT (SCOUT) for the many-to-many verification tier.
8. **Stemma spike** (new capability): non-mandatory-overlap edge weights (When
   Agents Look the Same) → TRUST edge-purification → cross-perplexity cophenetic
   tree (Baidildinova) → date-constrained direction (CohTP/RCTEA).
9. **Concept-erasure pilot**: LEACE removal + containment/disentanglement
   verification (Naowarat), counterfactual eval on our multi-witness pairs
   (CausalDetox), multi-concept safety check (From Isolation to Entanglement).
10. **Swap density gates for conformal+FDR** (Principled Hallucination
    Detection) with per-domain calibration (CEBC); make Track-3 similarity
    calibrated-and-monotone via ODASim; do NOT confidence-prune rare works
    (Dataset Cartography).
11. **One confusion matrix, three uses**: edit-cost prior + reanalysis pass
    (Clark/Levy/Gibson) + visual-confusability hard negatives (SAN).
12. **new?-queue as generalized category discovery** (GenDis); **scale**:
    metric-tree pruning (LycheeCluster) + HHQ codes; **product**: cost out a
    GovScape-style visual+semantic search over the 948K folios.
