# WAVE 2 — Formal / Cognitive scope cards (SCiL + CoNLL + Linguistic-Theories/CogPsy + Syntax&Parsing)

Scope: `Workshops/SCiL/` (51) + `Workshops/CoNLL/` (47) + `Tracks/Linguistic_Theories_CogPsy/` (37) + `Tracks/Syntax_and_Parsing/` (5). Posters ignored.
Hunt = mechanism-over-domain. A = adopt (surprise × usefulness, non-keyword-findable). B = worth-knowing. C = one line.

---

## A — adopt

### Compiling Search & Change Rules into Subsequential Finite-State Transducers
- **id / track / file**: SCiL 2026 pp.171–179 · SCiL · `Workshops/SCiL/Azadegan_Compiling_Search_Change_Rules_into_Subsequential_Finite.pdf`
- **surface domain**: formal phonology — giving the "Search & Change" procedural rule model a rigorous FST semantics.
- **mechanism**: linguist-authored directional rewrite rules ("scan for a trigger, transform target if licensed") compile to a SINGLE transition structure that is subsequential in one scan direction and reverse-subsequential in the other; linear-time application, proof of correctness, and known learnability from positive input/output pairs.
- **analogy**: their `human-readable directional rewrite rules → one-scan subsequential FST`  ≅  our **A · orthographic-normalization & transliteration engine** (Judeo-Arabic↔Hebrew dot/geresh forms, diacritic folding, final-letter variants).
- **why it transfers**: our normalization is ad-hoc regex today; authoring it as directional S&C rules and compiling to a single left-scan subsequential transducer gives a fast (linear-time), inspectable, auditable normalizer whose behavior is provably characterized — and the same rules are learnable/refinable from observed clean↔noisy pairs.
- **why it might NOT**: our transforms may be simple enough that regex suffices, and adopting the S&C authoring formalism is upfront cost for a layer that already "works."
- **transfer confidence**: medium-high
- **priority**: A
- **bib leads**: Chandlee 2014 (ISL functions, learnable rewrite subclass); Oncina et al. 1993 (OSTIA subsequential learning); Kaplan & Kay 1994 (rules→rational relations); Gorman & Reiss 2025 (Logical Phonology).

### A framework for analyzing concept representations in neural models (containment + disentanglement; LEACE)
- **id / track / file**: CoNLL 2026 pp.574–587 · CoNLL · `Workshops/CoNLL/Naowarat_Goldwater_A_framework_for_analyzing_concept_representations_in_ne.pdf`
- **surface domain**: interpretability — do neural text/speech models keep human concepts in linear subspaces, and can you erase them.
- **mechanism**: a unified framework scoring a concept subspace on two axes — *containment* (the concept lives fully inside the subspace and not outside) and *disentanglement* (isolation from other concepts) — compares 5 estimators and the SOTA concept-erasure method LEACE; empirically shows phone info is contained AND disentangled from speaker identity in HuBERT (i.e. speaker identity is erasable).
- **analogy**: their `erase speaker identity from a speech embedding, keep phones`  ≅  our **I · strip scribe-hand / language-identity (JA vs Hebrew) / script from a text embedding, keep the wording** (Track-3 hygiene).
- **why it transfers**: gives BOTH a method (LEACE closed-form erasure) AND a gold-free way to verify the erasure worked (containment/disentanglement metrics) — exactly what we need to test whether "hand" or "language" has actually been removed before running cross-hand near-duplicate search, without a labelled test set.
- **why it might NOT**: our factors (scribe hand, register) may not be linearly encoded or cleanly separable, and LEACE's generalization to unseen data was flagged as weak here.
- **transfer confidence**: medium-high
- **priority**: A
- **bib leads**: Belrose et al. 2023 (LEACE); Ravfogel et al. (INLP/relaxed erasure); Park et al. 2023 (linear representation hypothesis).

### Harnessing Linguistic Dissimilarity for Language Generalization (adversarial variety-invariant/specific split)
- **id / track / file**: CoNLL 2026 pp.284–300 · CoNLL · `Workshops/CoNLL/Kim_Mortensen_Harnessing_Linguistic_Dissimilarity_for_Language_Genera.pdf`
- **surface domain**: low-resource NLP — generalizing to unseen dialects/varieties instead of collapsing them onto a high-resource language.
- **mechanism**: two-branch model (VAÇAÍ-Bowl) trained so one branch captures variety-SPECIFIC attributes while a parallel branch learns variety-INVARIANT attributes via ADVERSARIAL training; plus TOPPing, a source-variety selection method. Core claim: over-aligning related varieties destroys useful signal — keep an invariant channel AND a variety channel.
- **analogy**: their `adversarially separate variety-invariant content from variety-specific form`  ≅  our **I + L · disentangle wording from JA/Hebrew/script/register so we can match content across scripts** — and their "don't over-align" caution ≅ times we must NOT collapse JA and Hebrew.
- **why it transfers**: a concrete architecture to learn a content-invariant embedding channel (for cross-script/cross-register near-duplicate search) while retaining a variety channel (for provenance/dating). TOPPing = principled choice of which clean reference edition variety to anchor to when several exist.
- **why it might NOT**: adversarial disentanglement is finicky to train and needs a variety label per document, which we only have coarsely (JA vs Hebrew vs Aramaic), not per-hand.
- **transfer confidence**: medium
- **priority**: A
- **bib leads**: Ganin & Lempitsky 2015 (gradient-reversal adversarial domain adaptation); Mortensen et al. (URIEL/lang2vec typological distance).

### Quantifying mutual intelligibility gradients in Turkic languages using language models
- **id / track / file**: SCiL 2026 pp.442–446 · SCiL · `Workshops/SCiL/Baidildinova_Futrell_Quantifying_mutual_intelligibility_gradients_in_Turkic.pdf`
- **surface domain**: how well speakers of one Turkic language understand a related one — measured with LMs.
- **mechanism**: use a language model of variety A to score text of related variety B (cross-perplexity / surprisal) as an ASYMMETRIC, alignment-free distance between closely-related varieties; relate the resulting distances to the genealogical tree via *cophenetic distance* (tree-path distance).
- **analogy**: their `LM cross-perplexity distance between related varieties → tree/cophenetic distance`  ≅  our **L · alignment-free JA↔Hebrew (and hand↔hand, stratum↔stratum) distance**, and the closest in-scope pointer to **F · the stemma we don't yet build**.
- **why it transfers**: gives a runnable recipe — train small char/token LMs per register/script/date-band, build an asymmetric distance matrix from cross-perplexity, then cluster/tree it — turning "who is textually close to whom" into a metric without needing aligned pairs; asymmetry mirrors our asymmetric clean-reference matcher.
- **why it might NOT**: cross-perplexity conflates orthographic noise (16–20% CER) with real textual distance, and the tree step (cophenetic) is borrowed prior art, not validated here for manuscripts.
- **transfer confidence**: medium
- **priority**: A
- **bib leads**: Gooskens et al. 2018 (cophenetic distance & intelligibility); Heeringa/Levenshtein dialectometry; Rama & List (computational phylogenetics of languages).

### Readers make targeted regressions to plausible errors ("noisy-channel garden-path" reanalysis)
- **id / track / file**: CoNLL 2026 pp.435–451 · CoNLL · `Workshops/CoNLL/Clark_Gibson_Readers_make_targeted_regressions_to_plausible_errors_i.pdf`
- **surface domain**: psycholinguistics/eye-tracking — how readers recover when a sentence is best explained as containing an ERROR rather than an alternative parse.
- **mechanism**: a noisy-channel model of comprehension with reanalysis — the reader maintains a posterior over "what was intended + where the corruption is," and when later context arrives makes TARGETED regressions to the most probable error loci (not a uniform re-scan).
- **analogy**: their `infer intended string + posterior over error location from a corrupted observation, revisiting on later evidence`  ≅  our **A + K · seed-and-extend / banded Levenshtein over noisy HTR, then re-align when downstream context reveals a more plausible error site**.
- **why it transfers**: two concrete upgrades to our verifier — (1) replace uniform Levenshtein costs with a *noisy-channel prior* over edits (learned Hebrew-letter OCR confusion probabilities, dot/geresh swaps) so alignments with a-priori-plausible corruptions win; (2) add a reanalysis pass that uses later matched context to relocate an earlier greedy misalignment, instead of committing left-to-right.
- **why it might NOT**: it's a behavioral study with no released algorithm, and the underlying noisy-channel idea (Levy 2008) is classic — we may already approximate it, so the gain is incremental.
- **transfer confidence**: medium
- **priority**: A
- **bib leads**: Levy 2008 (noisy-channel comprehension); Gibson et al. 2013 (rational inference over noise); Ryan/Futrell surprisal-and-reanalysis models.

---

## B — worth knowing

### A Family of Effective Methods for Decompiling Canonical Acceptors (dot-depth one + tier extensions)
- **id / track / file**: SCiL 2026 pp.25–36 · SCiL · `Workshops/SCiL/Lambert_A_Family_of_Effective_Methods_for_Decompiling_Canonical.pdf`
- **surface domain**: formal-language theory — translating a finite automaton BACK into a human-readable logical description (which subregular class/formula describes it).
- **mechanism**: general algebraic procedure to "decompile" a canonical acceptor into propositional-logic descriptions over k-blocks (V*D decomposition), i.e. explain WHAT pattern an automaton encodes.
- **analogy**: their `automaton → readable logical rule`  ≅  making a learned/compiled pattern-matching or normalization engine of ours **inspectable** — recover the human-readable rule a black-box matcher implements.
- **why it transfers**: if we ever learn or compile an FST for normalization (see Azadegan A), decompilation turns it into an auditable rule set a philologist can check/correct.
- **why it might NOT**: we have no such automata to decompile today; this is inspectability infrastructure, not a detection tool.
- **transfer confidence**: speculative
- **priority**: B

### Learning Process Interaction Through Simplex ISL Transducers
- **id / track / file**: SCiL 2026 pp.304–312 · SCiL · `Workshops/SCiL/Wang_Jardine_Learning_Process_Interaction_Through_Simplex_ISL_Transd.pdf`
- **surface domain**: learnability of INTERACTING phonological rules (feeding/opaque orderings).
- **mechanism**: a decomposition algorithm that, given an observed COMPOSED string-to-string map, reconstructs both the individual component transductions AND their relative ordering by exploiting structural properties of simplex ISL2 transducers.
- **analogy**: their `decompose a composed mapping into ordered component processes`  ≅  our **D · motif decomposition** (break an observed text into component recurring pieces) and a micro-**F** (relative ordering of applied transforms = a tiny copy-order signal).
- **why it transfers**: the principle "recover components + their order from only the composite output" is exactly the inverse problem in motif discovery and in inferring which normalization/scribal edit was applied first.
- **why it might NOT**: the machinery is ISL2-phonology-specific; text-reuse composition isn't function composition of local transducers, so the algorithm itself won't port.
- **transfer confidence**: speculative
- **priority**: B

### Comonadic Morphophonology: composing context-dependent rules without state explosion
- **id / track / file**: SCiL 2026 pp.100–112 · SCiL · `Workshops/SCiL/Jang_Comonadic_Morphophonology_A_Compositional_Framework_for.pdf`
- **surface domain**: Finnish morphophonology — composing FSTs for gradation/harmony/assimilation.
- **mechanism**: model each rule as a local-context→output-segment function (cellular-automaton style) and compose them as coKleisli arrows of a Zipper/Writer comonad; avoids the multiplicative state explosion of FST composition, gets bidirectionality (analysis arrows reused for generation) for free — 874 continuation classes → 13 arrows (67:1).
- **analogy**: their `compose many local rewrite rules without blow-up, run them both directions`  ≅  the engineering backbone for our Azadegan-style **A · normalization engine** (normalize ↔ de-normalize as one artifact).
- **why it transfers**: if we build a rule-based normalizer with many interacting rules, the comonadic pattern keeps it compact and gives inverse (de-normalization / candidate expansion) at no extra cost.
- **why it might NOT**: heavy category-theory machinery for a layer our regex already handles; adoption cost is high vs benefit.
- **transfer confidence**: speculative
- **priority**: B

### A Logical Analysis of Autosegmental Root-and-Pattern Morphology in Arabic
- **id / track / file**: SCiL 2026 pp.90–99 · SCiL · `Workshops/SCiL/Payne_A_Logical_Analysis_of_Autosegmental_Approaches_to_Root.pdf`
- **surface domain**: Semitic (!) root-and-pattern morphology, formalized as multi-tier structures with FO/MSO transductions.
- **mechanism**: represent a word as parallel TIERS (consonantal-root tier, prosodic-template tier, affix tier) and define associations as transductions over the relational structure; finite root length lets left-to-right association stay First-Order definable.
- **analogy**: their `consonantal-root tier abstracted from the surface word`  ≅  a **normalization/blocking key for A/B** — index Hebrew/JA by consonantal skeleton so morphological inflection stops masking that two witnesses share a root/lemma.
- **why it transfers**: root-skeleton projection is a proven Semitic-IR normalization; this formalizes exactly which tier to project and how, directly on our language family.
- **why it might NOT**: the paper delivers a definability result, not a matcher or an extractor we can run on noisy text.
- **transfer confidence**: medium
- **priority**: B

### Learning Stress in Arabic Low-Resource Settings (BUFIA grammar induction beats a transformer)
- **id / track / file**: SCiL 2026 pp.262–279 · SCiL · `Workshops/SCiL/Qaddoumi_Heinz_Learning_Stress_in_Arabic_Low_Resource_Settings.pdf`
- **surface domain**: predicting Arabic lexical stress from syllable structure, low-resource.
- **mechanism**: a grammar-induction learner (BUFIA — Bottom-Up Factor Inference) is interpretable AND more sample-efficient than a transformer, especially when data is scarce.
- **analogy**: their `interpretable, sample-efficient grammar induction over sequences`  ≅  a candidate engine for our **D · motif / structure induction** where labels are scarce and we want inspectable rules, not a black box.
- **why it transfers**: validates BUFIA/factor-inference as an interpretable alternative for discovering recurring structural factors in low-resource sequence data — our exact regime.
- **why it might NOT**: stress assignment is a narrow, well-structured task; scaling factor inference to 10^6-scale motif discovery is unproven.
- **transfer confidence**: speculative
- **priority**: B
- **bib leads**: Chandlee, Eyraud & Heinz (BUFIA / bottom-up factor inference).

### On the Proper Treatment of Units in Surprisal Theory
- **id / track / file**: ACL main 2026 pp.32202–32224 · CogPsy · `Tracks/Linguistic_Theories_CogPsy/Kiegeland_Cotterell_On_the_Proper_Treatment_of_Units_in_Surprisal_Theory.pdf`
- **surface domain**: psycholinguistics — reconciling model token alphabets with the word-level "units" experiments care about.
- **mechanism**: a unified framework to aggregate a fine-grained per-token score to ARBITRARY unit inventories, cleanly separating "unit of analysis" from "region of interest"; tokenization is treated as an implementation detail, not a primitive.
- **analogy**: their `aggregate token-level probability to any unit you care about`  ≅  our need to roll a char-n-gram / token-level MATCH score up to word / verse / motif units for scoring and reporting.
- **why it transfers**: gives a principled recipe for converting our sub-word match density into calibrated word/verse-level match confidence, avoiding ad-hoc summation that biases toward long tokens.
- **why it might NOT**: it's about probability aggregation for reading-time regression; our "score" isn't a probability, so the theory only loosely applies.
- **transfer confidence**: speculative
- **priority**: B

### Identifying the Periodicity of Information in Natural Language (AutoPeriod of Surprisal)
- **id / track / file**: ACL main 2026 pp.1161–1175 · CogPsy · `Tracks/Linguistic_Theories_CogPsy/OU_Buschmeier_Identifying_the_Periodicity_of_Information_in_Natural_L.pdf`
- **surface domain**: information density — do documents show periodic structure in their surprisal signal.
- **mechanism**: APS runs a canonical periodicity-detection algorithm (AutoPeriod: FFT + autocorrelation) on the surprisal sequence of a single document and confirms periods via harmonic regression; also floated for LLM-generation detection.
- **analogy**: their `periodicity detection on a per-position score sequence`  ≅  our **D** applied to a match-density / reuse signal along a manuscript — surface periodic/formulaic structure (liturgical refrains, acrostics, formulae) as peaks/periods.
- **why it transfers**: run AutoPeriod over a per-line reuse-density or surprisal track to auto-detect refrain/formula periodicity — a cheap, gold-free structural segmentation cue for liturgy; the generation-detection angle is a contamination-detection cousin.
- **why it might NOT**: periodicity ≠ change-point; strict periods are rare outside strongly formulaic liturgy, so coverage may be narrow.
- **transfer confidence**: speculative
- **priority**: B
- **bib leads**: Vlachos et al. 2005 (AutoPeriod).

### Discovering Lexical Gaps Using Embeddings from Multilingual LLMs
- **id / track / file**: CoNLL 2026 pp.641–660 · CoNLL · `Workshops/CoNLL/Jung_Bergen_Discovering_Lexical_Gaps_Using_Embeddings_from_Multilin.pdf`
- **surface domain**: finding concepts lexicalized in one language but with no single-word equivalent in another.
- **mechanism**: measure each source item's cross-lingual nearest-neighbor similarity in a bilingual embedding space; items with a "gap" show systematically WEAKER cross-space alignment; a logistic classifier on unaligned embeddings separates gap from non-gap (taxonomy-free).
- **analogy**: their `flag items whose cross-space nearest neighbor is weak = "no counterpart exists"`  ≅  our **J + L · find orphan readings** — passages/phrases in a JA witness with no Hebrew counterpart (a genuine textual innovation, unique reading, or interpolation).
- **why it transfers**: gives a gold-free, alignment-free score to surface "this fragment has no match in the reference space," directly serving our fragmentary-prize / rare-event hunt and interpolation detection.
- **why it might NOT**: weak alignment can just mean OCR noise or an out-of-vocabulary form, not a true textual orphan — needs a noise control.
- **transfer confidence**: medium
- **priority**: B

### A Method for Learning Large-Scale Computational Construction Grammars
- **id / track / file**: CoNLL 2026 pp.213–226 · CoNLL · `Workshops/CoNLL/VanEecke_Beuls_A_Method_for_Learning_Large_Scale_Computational_Constru.pdf`
- **surface domain**: constructionist linguistics — mining tens of thousands of form-function "constructions" from annotated corpora.
- **mechanism**: induce a large network of human-interpretable form-meaning patterns (constructions) from a corpus, storing the recurring syntactico-semantic usage patterns as first-class objects.
- **analogy**: their `network of tens of thousands of recurring form patterns mined from a corpus`  ≅  our **D/E · motif inventory** (continuum → tens of thousands of motifs) as an interpretable, queryable structure.
- **why it transfers**: a template for treating motifs as an interpretable inventory of recurring patterns with provenance back to their source occurrences — matching our motif-v2 + witness-census framing.
- **why it might NOT**: their induction requires constituency + semantic-frame annotations we don't have for Genizah; the learning method doesn't port to raw noisy text.
- **transfer confidence**: speculative
- **priority**: B

### Learning Latent Representations with Progressive Hypothesis Space Expansion
- **id / track / file**: SCiL 2026 pp.47–56 · SCiL · `Workshops/SCiL/Paramore_Learning_Latent_Representations_with_Progressive_Hypoth.pdf`
- **surface domain**: phonological learning — when to posit an abstract underlying form.
- **mechanism**: an Occam/MDL-style learner that orders hypotheses by "disparity distance," starts with the most concrete/literal hypothesis, and only expands to more abstract candidates when the current set fails a likelihood threshold — abstraction admitted only when it demonstrably improves likelihood.
- **analogy**: their `cheapest-literal-first, escalate only on failure`  ≅  our **K + H · coarse-to-fine matcher** — try the literal/exact match first, escalate to fuzzy/normalized/transposed candidate generation only when a density/likelihood gate isn't met.
- **why it transfers**: a clean control policy for staging candidate generation cost (and for calibrating WHEN to spend the expensive verification), matching our two-pass draft-then-verify loop.
- **why it might NOT**: coarse-to-fine / progressive widening is generic; the paper offers the principle, not a tuned schedule for our scale.
- **transfer confidence**: speculative
- **priority**: B

### An Information-Theoretic Foundation for the Subregular Hierarchy
- **id / track / file**: ACL main 2026 pp.46417–46429 · CogPsy · `Tracks/Linguistic_Theories_CogPsy/Hung_Do_An_Information_Theoretic_Foundation_for_the_Subregular.pdf`
- **surface domain**: why phonology sits in the subregular classes, explained via information theory.
- **mechanism**: characterizes Strictly Local languages as stationary Markov sources with ZERO conditional mutual information between distant positions given the intervening symbols; MI profiles statistically separate SL-like from tier-based (long-distance) patterns (p<0.001, r=0.84).
- **analogy**: their `conditional-MI profile diagnoses local vs long-distance dependency`  ≅  a gold-free **D/H · diagnostic** for where a symbol stream has purely local structure vs long-range dependency — candidate seam-finder for motif boundaries and a locality check on our n-gram features.
- **why it transfers**: computing conditional MI across positions is a cheap, label-free way to detect whether structure is capturable by local (n-gram) features or needs a tier/long-range model — informing feature and segmentation choices.
- **why it might NOT**: it's an explanatory framing, not an algorithm targeting our tasks; MI estimation on noisy 16–20% CER text is itself fragile.
- **transfer confidence**: speculative (framing)
- **priority**: B

---

## C / skipped — coverage note

**Counts (this scope):** titles skimmed ≈ 140 (SCiL 51 · CoNLL 47 · CogPsy 37 · Syntax&Parsing 5) · abstracts (page-1) checked = 38 · **A = 5** · **B = 11**.

**Whole classes deliberately skipped as C (no runnable transfer):**
- **Pure formal-expressiveness / typology results** — Yolyan & Comer (BMRS ≡ modal µ-calculus), Hampe (output languages of SL rules), Verbil & Hunter (Uyghur harmony exceeds TSL), Hayden (algebraic classification of reduplication — copying-themed but a complexity result, not a detector), Siewert (efficient OT universal generation), Paulson-Rawski (tensor semantics for Minimalist Grammars), Kasenov (categorical Russian vowel-zero), Payne noted as B for the root-tier idea only. These are "which subregular class is X" proofs — elegant, but hand us no algorithm. *Note: the recurring **tier-projection** idea (project only relevant symbols onto a tier, apply local constraints) is the one reusable normalization primitive across several of these.*
- **Phonological/morphological LEARNING of specific processes** — Li & Heinz (tonotactics), Ilie-Prickett (tone sandhi OT), Wang (reduplicative templates), Mita/Hong/Turk-Jarosz, Mailhot (GRU capacity bottleneck — generic capacity-sweep, briefly relevant to Track-3 sizing). Task-specific; no isomorphism to reuse/alignment/lineage.
- **Surprisal / reading-time / human-vs-LLM cognitive-alignment studies** (bulk of CoNLL + CogPsy) — Clark-Schuler, Kajikawa, Nguyen-Arehalli, Nair-Oh, Yamamoto (Mamba), Amouyal, Jacobs, Zhao-Coulson, Trott (false belief), Turk-Neu (cue-based retrieval — memory-interference framing noted but no algorithm), Chai-Warstadt (UID human-vs-LLM), Janarthan (impossible languages), Hobbs-McCoy (collocational bootstrapping), Merlin-Toneva (brain data). Diagnostics about human processing, not transferable mechanisms.
- **LLM-agent / reasoning / ToM / tutoring / metacognition papers** (most of CogPsy main track) — Zhu-Vlachos, Bai-Cheng (SODA), Diao-Gui, Cheng-Wei (MCTS), Fan-Yan, Jeon-Lee, Sheng-Liu, Que-Huang, Li-Zhou, Duan-Liu (Bloom), Wang-Xiao (anthropomorphism), Vijjini (socio-cognitive), etc. Off-mechanism for us.
- **Semantics / grammar-formalism / probing papers** — Neu-Erk, Kalivoda, Li-Rawlins, Li-Futrell (concreteness), Tabor-Lee (metric grammars), Domenichelli (embedding-geometry isotropy — a reusable retrieval-quality diagnostic but generic), Aljaafari (mechanistic interp / causal tracing), Vaishnav (symbolic grounding), Deng (typological alignment). Interpretability/representation studies with no direct transfer.
- **Syntax & Parsing track (5)** — Chen-Guo (structural info emergence), Song-Xia (interactive semantic parsing RL), Shim-Lee (negative-constrained KGQA), Zhang SciFlow-Bench, Zhang-Wan RST-Guarder. Parsing/RST/KGQA engineering; no reuse/lineage isomorphism.

**Honest gaps in THIS scope (classes that were essentially ABSENT here):**
- **B · large-scale near-duplicate / dedup / MinHash-LSH / data-contamination detection** — none. That class lives in the engineering/efficiency tracks, not in SCiL/CoNLL/CogPsy/Syntax. Our strongest transfer target for B is not represented here.
- **F · phylogenetics / stemmatics / explicit tree-inference** — none as a paper; the ONLY in-scope thread is Baidildinova & Futrell's cross-perplexity→cophenetic-distance path (carded A) and the ordering-recovery idea in Wang & Jardine (B). No paper actually reconstructs a lineage tree.
- **L · optimal transport / Procrustes cross-space alignment** — none directly; the disentanglement papers (Kim-Mortensen, Naowarat) are the nearest substitutes.
