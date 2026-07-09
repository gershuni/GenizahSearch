# WAVE 2 — Analogy cards: AI/LLM-Agents + CODI-CRAC + QA + Summarization

Scan scope: `Tracks/AI_LLM_Agents/` (418), `Workshops/CODI-CRAC/` (18),
`Tracks/Question_Answering/` (62), `Tracks/Summarization/` (25). Posters ignored.
Mechanism-over-domain hunt per `_scan2/WAVE2-PROFILE.md`. Transfer targets A–L.

Counts: titles-skimmed 523 · abstracts-checked 28 · A 13 · B 15.

---

## A — adopt (transferable mechanism we'd NOT have found by domain keyword)

Ordered strongest first.

### Tracing the Roots: A Multi-Agent Framework for Uncovering Data Lineage in Post-Training LLMs
- **id / track / file**: 2026.acl-long (9606–9625) · AI_LLM_Agents · `Li_Wu_Tracing_the_Roots_A_Multi_Agent_Framework_for_Uncoverin.pdf`
- **surface domain**: reconstructing the derivation graph of LLM post-training datasets (which dataset was refined/aggregated from which) and tracing benchmark-contamination propagation along it.
- **mechanism**: infer a directed *evolutionary graph* over artifacts from shared features; characterize structural patterns (vertical refinement vs horizontal aggregation) and detect redundancy from implicit intersections + contamination that propagates along ancestry paths.
- **analogy**: their `dataset lineage graph`  ≅  our **F · stemma / lineage reconstruction (the who-copied-whom tree we do NOT build)**; their `contamination propagates along lineage` ≅ our shared-reading propagation across a copy chain.
- **why it transfers**: gives us a concrete recipe for the capability we flag as high-value-and-missing — turn our 1.34M shared-pair graph into a *directed* derivation graph, distinguishing refinement (one witness elaborates another) from aggregation (a florilegium pulls many), and flag "contamination" (a late gloss that back-propagates into apparent parallels).
- **why it might NOT**: their edges come from documented dataset-provenance metadata + LLM reasoning over descriptions; manuscripts have no provenance labels, so direction must be inferred purely from textual features (harder, and often genuinely undecidable).
- **transfer confidence**: medium
- **bib leads**: their contamination-propagation subsection; classic stemmatics (Lachmann) for the direction-inference problem they sidestep.

### When Agents Look the Same: Quantifying Distillation-Induced Similarity in Tool-Use Behaviors
- **id / track / file**: 2026.acl-long (10482–10502) · AI_LLM_Agents · `Yang_Yu_When_Agents_Look_the_Same_Quantifying_Distillation_Indu.pdf`
- **surface domain**: detecting that many LLM agents are "distilled echoes" of a few teacher models by measuring behavioral similarity in reasoning/tool-use.
- **mechanism**: two similarity metrics — Response Pattern Similarity + **Action Graph Similarity (tool-use as a directed graph)** — explicitly separating *mandatory* shared features (forced by the task, uninformative) from *non-mandatory* shared features (autonomous preferences → evidence of shared ancestry). Within-family pairs score measurably higher.
- **analogy**: their `mandatory vs non-mandatory shared behavior` ≅ **stemmatics' shared-inheritance vs shared-innovation** — the crux of **F · lineage**; their `AGS directed-graph similarity` ≅ our witness-similarity edges refined to only-informative features.
- **why it transfers**: this is the single deepest methodological insight for building our missing stemma: *shared text forced by the archetype tells you nothing; only shared "errors"/innovations indicate a common exemplar.* Their operationalization (isolate non-mandatory overlap, then cluster by it) is directly portable to weighting our shared-span edges before community detection.
- **why it might NOT**: "mandatory" for them is task-necessity, computed against a controlled distillation experiment with known teachers; we have no ground-truth ancestry to calibrate what counts as a mandatory (canonical) vs innovative reading — we'd need a reference-edition baseline to define "mandatory."
- **transfer confidence**: medium
- **bib leads**: their AGS graph-similarity definition; phylogenetics "shared derived character (synapomorphy)" literature.

### Nash-Pruned CredMAS: Dynamic Panel Pruning for VLM-MAS using Nash-based Selection and Doubly-Robust Credits
- **id / track / file**: 2026.findings-acl (39640–39650) · AI_LLM_Agents · `Fan_Zhang_Nash_Pruned_CredMAS_Dynamic_Panel_Pruning_for_VLM_MAS_u.pdf`
- **surface domain**: cutting the cost of a multi-agent VLM committee by activating only the agents that add marginal value each round.
- **mechanism**: (1) offline doubly-robust (AIPW) estimator learns each agent's *causal* marginal contribution from biased interaction logs; (2) online, cast panel selection as **submodular maximization under a budget**, solved greedily with a (1−1/e) guarantee (agents "bid" for slots).
- **analogy**: their `pick the best subset of committee agents under a token budget` ≅ our **H · which LLM-annotators / reference editions to trust** + **C · competitive selection / set-cover** (the profile lists set cover under C).
- **why it transfers**: our annotator committee and our 8,300-edition asymmetric matcher both face "run everything is too expensive." Submodular-greedy subset selection with an AIPW-estimated per-source value function is a principled replacement for ad-hoc DF caps — pick the reference set that maximizes expected coverage per unit compute.
- **why it might NOT**: submodularity of "match coverage" isn't guaranteed for us (overlapping references can be super-/sub-additive in odd ways), and AIPW needs enough logged outcomes to estimate value — cold-start on rare works is exactly where we're weakest.
- **transfer confidence**: medium
- **bib leads**: Nemhauser et al. greedy (1−1/e); AIPW / doubly-robust estimation (Robins).

### ModeX: Evaluator-Free Best-of-N Selection for Open-Ended Generation
- **id / track / file**: 2026.acl-long (14394–14416) · AI_LLM_Agents · `Choi_Li_ModeX_Evaluator_Free_Best_of_N_Selection_for_Open_Ended.pdf`
- **surface domain**: picking the best of N stochastic LLM generations when there is no canonical answer and no reward model.
- **mechanism**: build a **similarity graph over candidate outputs**, recursively apply spectral clustering, and return the centroid of the dominant cluster = the "modal" semantic consensus. Generalizes majority-vote / self-consistency to open-ended text with **no external evaluator**.
- **analogy**: their `evaluator-free modal consensus over candidates` ≅ our **H · calibration/consensus with NO gold** (our LLM-annotator committee) + **E · clustering** of near-identical outputs.
- **why it transfers**: when several LLM annotators return free-text judgements about a candidate match (e.g. "is this the same work?"), we can't string-match-vote; ModeX gives a gold-free aggregator — cluster the free-text verdicts by semantic similarity, take the modal cluster's centroid as the committee decision, and read cluster tightness as confidence.
- **why it might NOT**: spectral clustering of a small N (3–7 annotators) is unstable; "dominant cluster" degrades to a coin-flip on genuine 50/50 disagreements, which are exactly the fragmentary cases we care about.
- **transfer confidence**: medium
- **bib leads**: self-consistency (Wang et al. 2022); Minimum Bayes Risk decoding (compare with ConSUM below — a parametric alternative to ModeX's graph mode).

### MatchTIR: Fine-Grained Supervision for Tool-Integrated Reasoning via Bipartite Matching
- **id / track / file**: 2026.acl-long (11953–11968) · AI_LLM_Agents · `Qu_Yin_MatchTIR_Fine_Grained_Supervision_for_Tool_Integrated_R.pdf`
- **surface domain**: assigning dense per-turn RL rewards to a tool-use trajectory instead of one trajectory-level reward.
- **mechanism**: formulate credit assignment as a **bipartite matching between predicted turns and ground-truth turns**, then derive per-turn rewards from the optimal matching (two assignment strategies).
- **analogy**: their `bipartite match predicted-trace ↔ gold-trace` ≅ our **C · competitive span assignment (shadowing)** — matching detected spans to reference spans / to each other so each region is claimed once by its best owner.
- **why it transfers**: our shadowing is currently a greedy competitive assignment; casting span-to-reference (or span-to-span) assignment as explicit bipartite matching (Hungarian / optimal transport) is a principled, tunable upgrade that could reduce the double-counting the census still shows, and gives a clean objective for "which witness owns this shared passage."
- **why it might NOT**: bipartite matching is one-to-one; genuine textual reuse is many-to-many (one passage legitimately shared by dozens of witnesses), so we'd need a relaxed/soft-matching or set-based variant — the vanilla algorithm doesn't fit our multiplicity.
- **transfer confidence**: medium
- **bib leads**: Hungarian algorithm; optimal-transport soft assignment (Sinkhorn) as the many-to-many relaxation.

### Purging the Gray Zone: Latent-Geometric Denoising for Precise Knowledge Boundary Awareness (GeoDe)
- **id / track / file**: 2026.findings-acl (2562–2576) · Question_Answering · `An_Xu_Purging_the_Gray_Zone_Latent_Geometric_Denoising_for_Pr.pdf`
- **surface domain**: teaching an LLM when to abstain by cleaning up label noise near its knowledge boundary.
- **mechanism**: fit a **truth hyperplane** with linear probes in latent space; use **signed geometric distance to the hyperplane as a gold-free confidence signal**; drop the ambiguous "gray zone" samples near the boundary before fine-tuning (keep only high-margin points).
- **analogy**: their `distance-to-hyperplane confidence + gray-zone filtering` ≅ our **G · denoising silver/graded labels for Track-3** + **H · confidence with no gold** + **I · a probe direction as the decision axis**.
- **why it transfers**: our Track-3 embeddings will be trained on auto-generated (silver) match/no-match pairs riddled with borderline noise. GeoDe says: learn a linear match/no-match hyperplane, and *train only on high-margin pairs*, discarding the near-boundary ambiguous pairs that otherwise teach the model to hallucinate matches — a principled recipe for our label denoising.
- **why it might NOT**: assumes a roughly linearly-separable truth direction in latent space; our JA↔Hebrew, high-CER matches may not be linearly separable, and discarding the gray zone throws away exactly the fragmentary hard cases we most want to recover.
- **transfer confidence**: medium
- **bib leads**: linear probing / concept directions; margin-based sample selection / curriculum.

### Enhancing Factuality through Consensus and Consistency Using Minimum Bayes Risk Decoding (ConSUM)
- **id / track / file**: 2026.findings-acl (41686–41713) · Summarization · `Soetedjo_Watanabe_Enhancing_Factuality_through_Consensus_and_Consistency.pdf`
- **surface domain**: reranking candidate summaries to be more faithful.
- **mechanism**: score each candidate by two axes — **consensus** among the other candidates via **Minimum Bayes Risk (MBR) decoding** (pick the candidate with highest expected similarity to the set) + **consistency** to the source via a factuality metric.
- **analogy**: their `MBR consensus × source-consistency` ≅ our **H · gold-free committee aggregation** + **K · generate-and-rank**; MBR is the parametric sibling of ModeX's graph-mode.
- **why it transfers**: two directly usable ideas. (1) MBR aggregation for our LLM committee: choose the verdict/reconstruction that is most central to the peer set (robust to a single outlier annotator). (2) The *two-axis* score — agreement-with-peers AND agreement-with-source — is exactly our restoration loop's need: a proposed reading must be both peer-plausible and consistent with the surviving reference text.
- **why it might NOT**: MBR needs a good pairwise similarity kernel over candidates; for high-CER Hebrew/JA our surface similarity is noisy, so "expected similarity to the set" could reward the blandest candidate rather than the correct one.
- **transfer confidence**: medium
- **bib leads**: MBR decoding (Kumar & Byrne 2004; Eikema & Aziz 2020).

### GAVEL: Evidence-Contract Debate with Mechanized Scrutiny for Provenance-Grounded Fact-Checking
- **id / track / file**: 2026.findings-acl (35907–35920) · AI_LLM_Agents · `Xu_Sheng_GAVEL_Evidence_Contract_Debate_with_Mechanized_Scrutiny.pdf`
- **surface domain**: fact-checking that must return faithful evidence at fine granularity (exact sentences/cells), not just a verdict.
- **mechanism**: an **Evidence Contract** forces each atomic sub-claim to bind to explicit evidence units; a neutral **Scrutinizer performs deterministic validation of cited identifiers and quoted spans** (non-LLM exact-string check); only then does a Judge decide.
- **analogy**: their `deterministic quoted-span validation behind an LLM debate` ≅ our **K · cheap deterministic verifier after an expensive proposer** + faithfulness/attribution = our **paraphrase-level reuse claim must be backed by a real shared span**.
- **why it transfers**: when an LLM (or our semantic Track-3) *proposes* that two fragments reuse the same passage, GAVEL's pattern says: don't trust the rationale — mechanically re-verify the exact shared span (our banded-Levenshtein) as a hard gate. The "atomic sub-claim → bound evidence unit → deterministic check" is a template for a self-auditing match record.
- **why it might NOT**: their deterministic check is exact-string on clean text; our "quoted spans" are 16–20% CER, so the deterministic verifier must itself be fuzzy (banded), reintroducing a threshold GAVEL gets to avoid.
- **transfer confidence**: medium
- **bib leads**: their Mechanized Chain-of-Scrutiny; cf. "From Trajectories to Graphs: Contract-Checked Editing" (2026.acl-long.2004, same track) for a related contract-checking verifier.

### PrefixNLI: Detecting Factual Inconsistencies as Soon as They Arise
- **id / track / file**: 2026.acl-long (1414–1433) · Summarization · `Harary_Dagan_PrefixNLI_Detecting_Factual_Inconsistencies_as_Soon_as.pdf`
- **surface domain**: NLI/entailment checking that fires on partial decoding prefixes rather than complete sentences (MiniTruePrefixes model), to steer generation early.
- **mechanism**: generalize an expensive whole-sequence consistency judge to operate on **arbitrary prefixes**, enabling early rejection/rerank before the full candidate is produced.
- **analogy**: their `reject a candidate as soon as its prefix goes inconsistent` ≅ our **K · cheap early check that kills expensive work** + **D · onset/change-point** of divergence within a sequence.
- **why it transfers**: our restoration self-refine loop and our banded verifier both benefit from early termination — score partial reconstructions/alignments on their prefix and abandon the ones that diverge from the reference before spending the full DP/LLM pass. Frames "verify incrementally, not at the end" as a first-class, trainable objective.
- **why it might NOT**: we already get prefix early-exit almost for free from banded Levenshtein's diagonal cutoff; PrefixNLI's added value (a *learned* prefix judge) only pays off in the semantic/LLM stage, not the cheap edit-distance stage where we don't need it.
- **transfer confidence**: medium
- **bib leads**: their MiniTruePrefixes training-data construction for prefix-level entailment.

### S2G-RAG: Structured Sufficiency and Gap Judging for Iterative Retrieval-Augmented QA
- **id / track / file**: 2026.acl-long (25846–25862) · Question_Answering · `Li_Zhou_S2G_RAG_Structured_Sufficiency_and_Gap_Judging_for_Iter.pdf`
- **surface domain**: multi-hop RAG that decides what to retrieve next and when it has enough evidence.
- **mechanism**: an explicit controller (S2G-Judge) that each turn (a) predicts whether current evidence is **sufficient** to answer, and if not (b) emits **structured "gap items"** naming the missing information, which are mapped into the next retrieval query; plus a sentence-level evidence memory to suppress distractor accumulation.
- **analogy**: their `sufficiency + structured-gap → next query` ≅ our **Track-3 iterative retrieve-then-reason**: given a partly-matched composition, name the still-unmatched distinctive phrases and issue them as the next witness query.
- **why it transfers**: our witness-finding is iterative (seed phrase → candidates → expand). A gap-judge that explicitly says "phrases X,Y still lack a witness" and turns those into the next query gives a principled stopping/continuation rule and reduces the distractor pile-up that hurts our density gates.
- **why it might NOT**: their sufficiency judge is trained on QA answerability; "have I found enough witnesses of this work?" has no crisp answerability signal — for a fragmentary prize the honest answer is often "never sufficient," so the stopping rule may not port.
- **transfer confidence**: medium → speculative
- **bib leads**: their S2G-Judge controller design; iterative/adaptive-retrieval RAG (FLARE, Self-RAG).

### PortNLP at CRAC 2026: QLoRA Fine-Tuning with Bounded Entity Registry for Multilingual Coreference
- **id / track / file**: 2026.codi-1.25 (193–198) · CODI-CRAC · `Shore_Agrawal_PortNLP_at_CRAC_2026_QLoRA_Fine_Tuning_with_Bounded_Ent.pdf`
- **surface domain**: multilingual coreference resolution over long documents with an LLM under a bounded context.
- **mechanism**: process the stream in 500–700-char chunks with a **bounded rolling entity registry** that tracks up to 30 active entities, scored by a **frequency × recency decay** formula; new mentions link against the registry.
- **analogy**: their `bounded, decaying entity registry for streaming linking` ≅ our **missing capability: linking witnesses of the SAME work/person across a huge stream of fragments** (cross-document entity/work identity at scale).
- **why it transfers**: we cannot hold all works/persons in memory while streaming 948K pages. A bounded registry of "active works/persons" scored by frequency×recency is a concrete, cheap mechanism to maintain running identity of a work across fragments without an all-pairs blowup — a streaming complement to our batch pair-mapping.
- **why it might NOT**: their registry cap (30) and recency decay suit a single narrative's locality; Genizah fragments arrive in no meaningful order and the "same work" can resurface after thousands of unrelated fragments, so recency decay would evict exactly the long-range links we need.
- **transfer confidence**: medium → speculative
- **bib leads**: their registry scoring formula; entity-linking under memory bounds; compare CorPipe (below) for the SOTA span-based alternative.

### Dataset Cartography for Implicit Discourse Relation Recognition: Promises and Pitfalls
- **id / track / file**: 2026.codi-1.8 (53–64) · CODI-CRAC · `Ignatev_Poesio_Dataset_Cartography_for_Implicit_Discourse_Relation_Rec.pdf`
- **surface domain**: applying training-dynamics "cartography" (easy/hard/ambiguous via confidence + variability) to noisy crowdsourced discourse labels.
- **mechanism**: map each silver datapoint by its training-dynamics coordinates; **key negative finding: low confidence is UNreliable for error detection because confidence is confounded by label rarity** — but high-confidence regions cleanly surface cue-driven easy cases worth rebalancing.
- **analogy**: their `training-dynamics triage of silver labels` ≅ our **G · denoising auto-generated ordinal labels for Track-3** + **H · what a confidence score actually measures**.
- **why it transfers**: we will lean on confidence to prune silver match pairs. This paper is a direct, load-bearing *caveat*: raw model confidence conflates true error with class rarity — for us, rare works / rare scripts would be systematically mislabeled "low confidence = error" and purged, biasing Track-3 toward common material. Use cartography to audit, but do NOT threshold on confidence alone; instead flag high-confidence cue-driven (surface-n-gram-driven) easy pairs for down-weighting.
- **why it might NOT**: it's a diagnostic method, not a fix — it tells us confidence is confounded but offers no clean denoiser; the actionable payoff is a warning plus a rebalancing heuristic, not an algorithm we run.
- **transfer confidence**: medium
- **bib leads**: Swayamdipta et al. 2020 (Dataset Cartography); Scholman et al. 2022 (DiscoGeM).

### Calibrating Model-Based Evaluation Metrics for Summarization (GIRB)
- **id / track / file**: 2026.findings-acl (35285–35315) · Summarization · `Liu_Henao_Calibrating_Model_Based_Evaluation_Metrics_for_Summariz.pdf`
- **surface domain**: fixing miscalibrated model-based summary-quality scores without reference summaries or human labels.
- **mechanism**: **group isotonic regression binning (GIRB)** adjusts raw scorer predictions to align monotonically with ground-truth metrics; produces individual + averaged proxy scores **referencelessly**; authors note it applies to discrete tasks (QA) too.
- **analogy**: their `referenceless isotonic calibration of a learned scorer` ≅ our **H · calibrate density/confidence gates to true match-probability with no gold**.
- **why it transfers**: our conf labels and density gates output raw scores whose absolute meaning drifts across scripts/domains. Isotonic regression (monotonic, non-parametric, cheap) is a drop-in to remap those raw scores onto a calibrated match-probability, per-domain grouping matching our per-domain DF policy — turning "score 0.7" into an actual probability we can threshold consistently.
- **why it might NOT**: isotonic calibration needs *some* trusted anchor points to fit the monotone map; fully gold-free calibration relies on proxy targets, and if our proxy (e.g. reference-edition matches) is itself biased toward clean text, the calibration inherits that bias.
- **transfer confidence**: medium
- **bib leads**: isotonic regression calibration (Zadrozny & Elkan); their referenceless proxy-score construction.

---

## B — worth knowing (weaker/less-certain transfer, or a useful framing/caveat)

### Single-Agent Generation Surpasses Multi-Agent Systems in Semantic Diversity
- id/file: 2026.findings-acl · AI_LLM_Agents · `Encheng_Aramaki_Single_Agent_Generation_Surpasses_Multi_Agent_Systems_i.pdf`
- Framing/caveat for our **committee (H/K)**: controlled study shows MAS "diversity gains" are largely a prompt-conditioning artifact, not architecture — parallel agents converge on overlapping ideas; a single model prompted for multi-output is more diverse. Warning: our multi-LLM-annotator committee may give *correlated*, not independent, signal; a single model sampled N times may be a cheaper, more diverse ensemble.

### Belief in Authority: Impact of Authority in Multi-Agent Evaluation Framework
- id/file: 2026.findings-acl · AI_LLM_Agents · `Choi_Kim_Belief_in_Authority_Impact_of_Authority_in_Multi_Agent.pdf`
- Framing/caveat (H): role labels ("expert"/"senior") bias committee aggregation — bias arises from authoritative roles holding position while others flex. If we give committee members personas/roles, we inject bias; keep aggregation role-blind or model the asymmetry explicitly.

### When KV Cache Reuse Fails in Multi-Agent Systems: Cross-Candidate Interaction is Crucial for LLM Judges
- id/file: 2026.acl-long · AI_LLM_Agents · `Liang_Zhou_When_KV_Cache_Reuse_Fails_in_Multi_Agent_Systems_Cross.pdf`
- Operational caveat (H) + a metric: an LLM judge must attend to candidates *jointly*; independent/cached scoring silently destabilizes selection. Gives **Judge Consistency Rate (JCR)** as a measure. If we ever LLM-judge candidate matches, present them together and track JCR.

### Free-MAD: Consensus-Free Multi-Agent Debate
- id/file: 2026.findings-acl · AI_LLM_Agents · `Cui_Zuo_Free_MAD_Consensus_Free_Multi_Agent_Debate.pdf`
- Framing (H): drop majority-vote/consensus; score the *entire debate trajectory* instead, to avoid conformity-driven error propagation. For our committee: aggregate over the full disagreement trace rather than a final vote.

### Mistake Notebook Learning: Batch-Clustered Failures for Training-Free Agent Adaptation
- id/file: 2026.findings-acl · AI_LLM_Agents · `Su_Huang_Mistake_Notebook_Learning_Batch_Clustered_Failures_for.pdf`
- Transfer (E/J): cluster failures into shared error patterns → distilled "mistake notes," updating memory only when *batch* performance improves. For us: cluster our matcher's false positives into recurring error families (e.g. liturgical formulae, scribal filler) to auto-tune gates — a self-improving audit loop.

### SpecAgent: A Speculative Retrieval and Forecasting Agent for Code Completion
- id/file: 2026.acl-long · AI_LLM_Agents · `Ma_Ramanathan_SpecAgent_A_Speculative_Retrieval_and_Forecasting_Agent.pdf`
- Transfer (K, engineering): do the expensive speculative context-construction at **indexing time** to anticipate future queries, so runtime is cheap. For our two-pass disk-spill engine: precompute speculative reference-alignments during indexing to mask query-time latency. Also flags *future-context leakage* inflating benchmarks — relevant to our own eval hygiene.

### Disentangling Reasoning Logic to Resolve Explicit Knowledge Conflicts (KCR)
- id/file: 2026.acl-long · Question_Answering · `Zheng_Zhao_Disentangling_Reasoning_Logic_to_Resolve_Explicit_Knowl.pdf`
- Resemblance (C): adjudicate contradictory contexts by disentangling them into discrete reasoning traces. Loosely maps to adjudicating conflicting witnesses of a passage, but the mechanism is QA-reasoning-specific; framing more than method.

### CAMEC: Complexity-Aware Multi-Expert Collaboration for Reliable Chinese Medical QA
- id/file: 2026.acl-long · Question_Answering · `Wu_Guohua_CAMEC_Complexity_Aware_Multi_Expert_Collaboration_for_R.pdf`
- Transfer (H/K): route each query by predicted complexity, recruiting only a subset of experts. For us: route candidate pairs by difficulty to cheap (edit-distance) vs expensive (semantic/LLM) verification — adaptive compute. Common pattern, but a clean instantiation.

### TrustTable: A Neuro-Symbolic Auditing Framework for Faithful Table QA
- id/file: 2026.acl-long · Question_Answering · `Zhao_Dong_TrustTable_A_Neuro_Symbolic_Auditing_Framework_for_Fait.pdf`
- Transfer (K/H): a **label-free audit loop** that decouples verification into two orthogonal axes — factual grounding (execute generated code against the data) + logical soundness (formal solver). For us: separate "does the span actually exist" (deterministic re-alignment) from "is the semantic match sound" (judge). Overlaps GAVEL's deterministic-check idea; kept as the cleaner two-axis framing.

### Do LLMs Really Know What They Don't Know? Internal States Mainly Reflect Recall, Not Truthfulness
- id/file: 2026.findings-acl · Question_Answering · `Cheang_Deng_Do_LLMs_Really_Know_What_They_Don_t_Know_Internal_State.pdf`
- Caveat (H): LLM internal-state "confidence" tracks knowledge *recall*, not correctness; hallucinations from learned associations are mechanistically like factual recall. Warning against using LLM-internal confidence as a match-quality gate.

### CorPipe at CRAC 2026: Empty Nodes and Cross-Lingual Transfer in Multilingual Coreference
- id/file: 2026.codi-1.27 · CODI-CRAC · `Straka_CorPipe_at_CRAC_2026_Empty_Nodes_and_Cross_Lingual_Tran.pdf`
- Worth knowing (L + the coref capability): the winning multilingual coref system; joint mention + coref-link + **empty-node** prediction in one model, with **cross-lingual zero-shot transfer**. The SOTA baseline to adopt if we build witness/person coreference; cross-lingual transfer angle relevant to JA↔Hebrew.

### Closing the Gap at CRAC 2026: Two-Stage Adaptation for LLM-Based Multilingual Coreference
- id/file: 2026.codi-1.23 · CODI-CRAC · `Bourgois_Poibeau_Closing_the_Gap_at_CRAC_2026_Two_Stage_Adaptation_for_L.pdf`
- Worth knowing (coref engineering): LLM coref via **headword span representation + XML-inspired format with local reindexing + iterative annotation**; two-stage adapter (multilingual base → dataset-specific). A concrete recipe if we prompt an LLM to link mentions across fragments.

### What's in a Bridge? A Multi-Genre Analysis of GUMBridge (Bridging Anaphora)
- id/file: 2026.codi-1.7 · CODI-CRAC · `Levine_Zeldes_What_s_in_a_Bridge_A_Descriptive_Multi_Genre_Analysis_o.pdf`
- Framing (F/E distinction): **bridging** (associative, non-identity reference) vs coreference (identity). Maps to our distinction between *same-work* witnesses (coreference) and *derivative/related* works — commentary↔base text, abridgment↔source (bridging). Their salience/distance/definiteness features could inform linking features.

### SCURank: Ranking Candidate Summaries with Summary Content Units
- id/file: 2026.findings-acl · Summarization · `Wang_Kao_SCURank_Ranking_Multiple_Candidate_Summaries_with_Summa.pdf`
- Resemblance (C/K + D): rank candidates by decomposing into atomic **Summary Content Units** and scoring content-unit coverage instead of surface overlap (ROUGE). Analogous to scoring reuse at the shared-*motif*/atomic-unit level rather than surface n-grams; SCU/Pyramid is well-known, so framing-level.

### Stress Testing Factual Consistency Metrics for Long-Document Summarization
- id/file: 2026.acl-long · Summarization · `Mujahid_Augenstein_Stress_Testing_Factual_Consistency_Metrics_for_Long_Doc.pdf`
- Methodology transfer (B-robustness): a battery of **7 factuality-preserving perturbations** (paraphrase, simplification, synonym, logically-equivalent negation, compression, source insertion) to test metric robustness. Adopt as a *test harness* for SEED-029's paraphrase-level reuse detector: does our matcher survive synonym/paraphrase/compression while rejecting negation?

---

## C / skipped — honest coverage note

**Skimmed all 523 titles; abstract-checked 28.** Deliberate whole-class skips in the
418-paper agents folder (per profile: skip tool-use / web-agent / GUI-agent / mobile-agent
benchmarks unless a verification or assignment mechanism transfers):

- **GUI / mobile / computer-use agent benchmarks & training** (~70+ papers: A3, DAC-Bench,
  D-Artemis, FineState-Bench, FedGUI, HiSA, LearnAct, LPO, MAS-Bench, MobileWorld,
  MPR-GUI, NaturalGAIA, OpenPhone, OS-Symphony, TVWorld, UI-Copilot, WindowsWorld,
  WebSTAR, InfiniteWeb, C-World, GUITester, etc.) — pure environment/benchmark plumbing,
  no transferable verification/assignment mechanism. **Skipped as a class.**
- **Agent memory architectures** (~40 papers: Agentic Memory, GAM, Synapse, MemBuilder,
  Memory-R1, HeLa-Mem, LiCoMemory, MemWeaver, RecMem, EMA, Memp, etc.) — long-horizon
  conversational/procedural memory; no isomorphism to our corpus-scale matching. Skipped.
- **Tool-use / function-calling RL & reward modeling** (~50 papers: ToolRM, ToolPRM, ToolScope,
  LoopTool, EVOTOOL, Progra, GOAT, MCP-Flow, EnvScaler, etc.) — kept only MatchTIR
  (bipartite matching) and the verifier ones (GAVEL-adjacent); rest skipped.
- **Agent safety / attacks / backdoors** (~20: BlindGuard, CORBA, TAMAS, BackdoorAgent,
  MCP-Guard, TopoSHIELD, Evo-Attacker, Web-Fraud, Data-Exfiltration, ACIArena, etc.) —
  security domain, no mechanism transfer. Skipped.
- **Agentic RL policy-optimization variants** (~40: PVPO, STAPO, DPEPO, BAPO, SPARK,
  Fission-GRPO, Graph-GRPO, HRL, etc.) — training recipes, not transferable. Skipped.
- **Social simulation / role-play / persona / negotiation / ToM agents** (~35: Sentipolis,
  GASim, PersonaForge, ThinkPersona, MERIT, Moral-Evolution, Chinese-Court, etc.) — skipped.
- **Domain-application agents** (chemistry/medical/climate/geospatial/finance/robotics:
  ChemAmp, LipoAgent, GraphDx, ClimAgent, Spatial-Agent, Ro-SLM, etc.) — skipped unless a
  committee/verification core surfaced (none did beyond Dialectic-Med / CAMEC, noted).
- **QA folder:** the bulk is table-QA, KGQA, multimodal/visual QA, temporal-KGQA, and
  RAG-retrieval variants — on-domain-adjacent but mechanism-thin for us; carded only the
  sufficiency/gap-judge (S2G-RAG), latent denoising (GeoDe), and committee/verification
  (CAMEC, TrustTable, Do-LLMs-Know). Retrieval-agent RAG papers (CIRAG, SEARCH-R, PROGRAM,
  AutoSearch) noted but not carded — standard iterative-RAG, weaker than S2G-RAG's gap-judge.
- **Summarization folder:** controllable/multimodal/opinion summarization skipped;
  carded the factuality-verification, consensus, calibration, and candidate-ranking papers.

**Honest gaps:** these four folders contain **no strong B (all-pairs near-duplicate /
MinHash-LSH / dedup) or A (noisy sequence-alignment / minimizer) paper** — those core-mechanism
families live in other tracks (IR, efficiency, bio-NLP), outside this scope. The lineage find
(**F**) was the biggest surprise here precisely because it arrived disguised as an
LLM-data-provenance paper, not a stemmatics paper.
