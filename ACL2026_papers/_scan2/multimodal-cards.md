# WAVE 2 — Multimodality_and_Grounding track (analogy hunt)

Scope: `Tracks/Multimodality_and_Grounding/` (315 papers, posters/ ignored).
Method: strict title-triage over all 315; abstract-checked 29 mechanism candidates;
deep-read the strong ones. This track is ~85% VLM hallucination-mitigation, VLM
reasoning/benchmarks, token-pruning, GUI/embodied agents, and video understanding —
almost all out of scope per the profile (VQA/captioning/reasoning benchmarks excluded
unless they carry a transferable alignment/retrieval mechanism). The signal is
concentrated in the retrieval, cross-modal-alignment, contrastive-representation, and
calibration sub-areas.

---

## A — adopt (transferable mechanism × surprise)

### Segment, Embed, and Align: A Universal Recipe for Aligning Subtitles to Signing (SEA)
- **id / track / file**: ACL 2026 Long, pp. 30371–30384 · Multimodality_and_Grounding · `Tracks/Multimodality_and_Grounding/Jiang_Zisserman_Segment_Embed_and_Align_A_Universal_Recipe_for_Aligning.pdf`
- **surface domain**: aligning spoken-language subtitles (timestamped text) to continuous sign-language video.
- **mechanism**: segment each stream into units (individual signs), embed every unit into a **shared latent space with text**, then align the two unit sequences with a **lightweight dynamic-programming pass** (runs on CPU in <1 min for hour-long content). Two frozen pretrained models + DP; no end-to-end training tied to a language/dataset.
- **analogy**: their `sign-unit ↔ subtitle-token DP alignment in a shared embedding space`  ≅  our **A · noisy sequence alignment** (seed-and-extend + banded Levenshtein) — a *fresh embedding-space* angle on the same job.
- **why it transfers**: this is exactly the shape of two of our hardest alignments — HTR-image-line ↔ transcription, and Judeo-Arabic ↔ Hebrew (problem L). Instead of char-n-gram seeds over noisy symbols, embed larger units (words/phrases) into a shared space, then DP over embedding similarities. DP over a similarity matrix is robust to substitution noise in a way surface seed-and-extend is not, and the "two frozen embedders + cheap DP" design scales to our corpus without training an aligner per language pair.
- **why it might NOT**: their unit segmentation is a learned sign-segmenter with clean video; our "units" over 16–20% CER text are themselves noisy, so the segmentation step is where our error would concentrate.
- **transfer confidence**: high
- **bib leads**: dynamic time warping (DTW) for sequence alignment; their sign-segmentation + sign-embedding backbones (VGG lineage).

### Semantic Hardness Is Not Visual Hardness: Sign-Aware Hard Negative Mining for Sign Language Retrieval (SAN)
- **id / track / file**: ACL 2026 Long, pp. 28262–28277 · Multimodality_and_Grounding · `Tracks/Multimodality_and_Grounding/Lee_Lim_Semantic_Hardness_Is_Not_Visual_Hardness_Sign_Aware_Har.pdf`
- **surface domain**: fine-grained sign-language retrieval, where visually near-identical signs must be told apart.
- **mechanism**: reframes retrieval failure as a **negative-distribution mismatch** — semantically distinct but *visually confusable* items are almost never sampled as hard negatives when you mine by linguistic/semantic similarity. Fix: mine hard negatives by **visual confusability in the embedding space**, not by text similarity.
- **analogy**: their `mine negatives from the visual-confusion structure, not semantics`  ≅  our **G · contrastive learning (Track-3) driven by the HTR confusion matrix**, and our **L** graphical-similarity model.
- **why it transfers**: our Track-3 embeddings must separate texts that are *graphically* near-identical under the noise channel (ד/ר, ב/כ, ו/י, missing dots in Judeo-Arabic) but are different works — precisely "semantically distinct yet visually confusable." The paper's thesis says: build hard negatives from the noise/confusion structure itself (our confusion matrix), not from semantic neighbors. That is a concrete recipe for our negative-sampling.
- **why it might NOT**: their "visual confusability" lives at sign-gloss granularity in a learned sign space; ours is a per-character error model, so we'd have to lift the confusion matrix from char-level to string/embedding-level confusability.
- **transfer confidence**: medium-high
- **bib leads**: hard-negative mining in metric learning; PHOENIX-2014T (their benchmark).

### Generative Giants, Retrieval Weaklings: Why do MLLMs Fail at Multimodal Retrieval? (ReAlign)
- **id / track / file**: Findings ACL 2026, pp. 15917–15933 · Multimodality_and_Grounding · `Tracks/Multimodality_and_Grounding/Feng_Zhang_Generative_Giants_Retrieval_Weaklings_Why_do_Multimodal.pdf`
- **surface domain**: diagnosing why multimodal-LLM embeddings underperform at zero-shot retrieval.
- **mechanism**: use a **sparse autoencoder (SAE)** to decompose embeddings into interpretable concept directions; discover that the dimensions dominating cosine similarity are **distractors** (nuisance directions from image-text bridging), not the discriminative signal. Fix = **ReAlign**, a *training-free* **whitening transformation** that re-geometrizes the space to suppress those directions.
- **analogy**: their `SAE-identify distractor dims → whiten them out`  ≅  our **I · concept removal / disentanglement** — strip scribe-hand / language-identity / script / genre from a text-reuse embedding, no retraining.
- **why it transfers**: our planned semantic layer will surely be dominated by nuisance factors (language = JA vs Hebrew, script, scribe hand, register) that swamp the "same-work" signal in similarity. This gives a two-step, off-the-shelf procedure: (1) SAE-decompose Track-3 embeddings, (2) locate the directions that dominate false-positive similarity, (3) whiten/project them out at index time. Training-free means we can apply it to a frozen encoder we don't control.
- **why it might NOT**: their distractor directions were found on natural image-text data; identifying *our* nuisance dimensions needs a labeled probe set (JA-vs-Hebrew pairs, same-hand pairs), and whitening can also erode genuine signal if the axes are entangled.
- **transfer confidence**: medium
- **bib leads**: sparse autoencoders for interpretable feature decomposition; whitening / PIP-loss geometry of embeddings; concept-erasure (INLP/LEACE lineage).

### CEBC: Conformal Evidence-Bounded Control for Low-Hallucination Vision–Language Generation
- **id / track / file**: ACL 2026 Long, pp. 46193–46206 · Multimodality_and_Grounding · `Tracks/Multimodality_and_Grounding/Mishra_Foltin_CEBC_Conformal_Evidence_Bounded_Control_for_Low_Halluci.pdf`
- **surface domain**: suppressing hallucinated object mentions in VLM captions/VQA without retraining.
- **mechanism**: a **conformally calibrated acceptance threshold** — set the evidence cutoff from **quantiles of a detector's confidence scores on a small held-out set**, giving an *explicit, controllable* risk bound at test time; then a risk-first selection rule keeps evidence-consistent outputs. Draft-then-minimally-edit loop on top.
- **analogy**: their `conformal quantile threshold with a guaranteed error rate`  ≅  our **H · calibration / threshold choice with no gold** (our density gates / confidence labels), plus a **K** draft-then-verify shell.
- **why it transfers**: we currently pick match-acceptance thresholds heuristically ("density gates"). Conformal calibration converts that into a principled knob: given a small labeled set of true/false spans, set the similarity/Levenshtein-density cutoff so the false-match rate is bounded at, say, 5% — with a distribution-free guarantee, and a threshold that re-derives per-domain (we already found per-domain DF policy matters, cf. the liturgy pass).
- **why it might NOT**: conformal guarantees need an exchangeable calibration set; our labeled match/no-match pairs are scarce and biased toward easy cases, which would make the bound optimistic.
- **transfer confidence**: medium-high
- **bib leads**: conformal prediction / split-conformal risk control; best-of-K sampling.

### Sculpting the Vector Space: Efficient Multi-Vector Visual Document Retrieval via PRUNE-THEN-MERGE
- **id / track / file**: Findings ACL 2026, pp. 24883–24925 · Multimodality_and_Grounding · `Tracks/Multimodality_and_Grounding/Yan_Hu_Sculpting_the_Vector_Space_Towards_Efficient_Multi_Vect.pdf`
- **surface domain**: Visual Document Retrieval (VDR) — retrieve relevant *pages* from corpora of visually-rich documents; the multi-vector (ColPali/ColBERT-style) paradigm.
- **mechanism**: two-stage embedding compression for multi-vector VDR — **adaptive pruning** drops low-information patch tokens, then **hierarchical merging** summarizes the survivors — extending the near-lossless compression range so multi-vector retrieval is affordable. Evaluated on 29 VDR datasets.
- **analogy**: their `multi-vector page-image retrieval`  ≅  our **L + the product frontier**: searching manuscript **images directly**, bypassing HTR noise entirely (ViDoRe/ColPali lineage applied to Genizah folios).
- **why it transfers**: this is the concrete route to "index the folio image, not the HTR transcription." Multi-vector (late-interaction) VDR is the SOTA quality tier; its blocker is storage/compute at scale, and PRUNE-THEN-MERGE is exactly the affordability fix we'd need to run it over ~948K pages. It sidesteps our 16–20% CER at the retrieval stage altogether.
- **why it might NOT**: ColPali-class models are trained on printed/born-digital pages (Latin-script, clean layout); Genizah imagery is degraded, RTL, heavily damaged parchment — zero-shot transfer of the visual encoder is unproven and would likely need domain adaptation.
- **transfer confidence**: medium (mechanism high; domain-gap risk real)
- **bib leads**: ColPali / ColBERT late interaction; ViDoRe benchmark; token pruning + merging for ViT patches.

---

## B — worth knowing (weaker / less-certain transfer, or a useful framing)

### Zero-Shot Multimodal Retrieval with Multi-Scale Contextual Representations (Multi-Score)
- ACL 2026 Long, pp. 20304–20324 · `Saha_Gokhale_Zero_Shot_Multimodal_Retrieval_with_Multi_Scale_Context.pdf`
- **Matryoshka (nested) embeddings** for cheap coarse retrieval → multimodal re-ranking. Training-free, 6× faster. `coarse-Matryoshka-ANN then expensive rerank` ≅ our **K** two-stage matcher and **L** cross-modal. Adoptable for Track-3: train nested embeddings, ANN on a short prefix, rerank on full dim. Caveat: unimodal text-reuse may not need the multimodal rerank stage. Confidence: medium.

### Learning Invariant Modality Representation from a Causal Inference Perspective (CmIR)
- ACL 2026 Long, pp. 45698–45721 · `Mai_Han_Learning_Invariant_Modality_Representation_for_Robust_M.pdf`
- Causal disentanglement splitting each input into **causal-invariant** vs **environment-specific spurious** parts (invariance + mutual-information + reconstruction constraints). ≅ **I** (strip scribe/library/genre as "environment"). A training-time counterpart to ReAlign's test-time whitening. Caveat: needs explicit environment labels we'd have to define; built for affective sentiment. Confidence: medium.

### VisRet: Visualize-then-Retrieve for Knowledge-Intensive Text-to-Image Retrieval
- ACL 2026 Long, pp. 25982–26001 · `Wu_Chang_VisRet_Visualization_Improves_Knowledge_Intensive_Text.pdf`
- Cross-modal embeddings behave as "bags of concepts" and miss structure, so **project the text query INTO the image modality (T2I generation) and retrieve within one modality**, bypassing weak cross-modal alignment. ≅ **L**: instead of aligning image↔text embeddings, render into a common modality. Caveat: generating manuscript-like images from a query is far-fetched; the *framing* transfers more than the method. Confidence: speculative.

### AFMRL: Attribute-Enhanced Fine-Grained Multi-Modal Representation Learning
- Findings ACL 2026, pp. 14366–14379 · `Zhang_Zheng_AFMRL_Attribute_Enhanced_Fine_Grained_Multi_Modal_Repre.pdf`
- Distinguishing near-identical items via attribute-guided contrastive learning that **identifies hard samples AND filters out noisy false negatives**; retrieval gain used as an RL reward. ≅ **G**: our silver-label contrastive training suffers exactly from false negatives (unlabeled true matches sampled as negatives). Their false-negative-filtering idea is the transferable bit. Caveat: "attributes" are MLLM-generated product facets; our analog (motifs/phrases) is undefined. Confidence: medium.

### Measure Twice, Click Once: Co-evolving Proposer and Visual Critic (GUI grounding)
- ACL 2026 Long, pp. 20964–20984 · `Wang_Zhang_Measure_Twice_Click_Once_Co_evolving_Proposer_and_Visua.pdf`
- Replaces a **static overlap-resolution heuristic (geometric clustering / NMS-like self-consistency)** with a **learned critic that selects the best among competing proposals**. ≅ **C** (our competitive span "shadowing", currently a keep-best heuristic). Suggests learning the selector rather than hand-coding it. Caveat: it's an RL co-evolution recipe tied to pixel grounding; heavy machinery for our purely combinatorial overlap resolution. Confidence: speculative.

### Map of Encoders — Mapping Sentence Encoders using Quantum Relative Entropy
- ACL 2026 Long, pp. 14160–14208 · `Zhang_Bollegala_Map_of_Encoders_Mapping_Sentence_Encoders_using_Quantum.pdf`
- Characterize an embedding space by its **Pairwise-Inner-Product matrix**, place encoders relative to each other via **Quantum Relative Entropy**, and **predict downstream retrieval/clustering performance from position — no task labels**. ≅ **H** (choose an embedder without gold) + **L** (compare spaces). Useful for picking a Track-3 backbone. Caveat: a meta-selection tool, not a matcher. Confidence: medium.

### MessToClean: Evidence-Grounded, Structure-Preserving Reconstruction of Degraded Exam Images
- ACL 2026 Long, pp. 40304–40322 · `Tuo_Zhao_MessToClean_Evidence_Grounded_Structure_Preserving_Reco.pdf`
- Degraded **handwritten** images → structured text via a backbone-agnostic pipeline that **grounds extraction in pixel-aligned evidence** and runs **post-hoc consistency auditing** to kill unsupported hallucinations. ≅ our HTR world + **K** (draft-then-verify against pixels). The "verify the transcription against the image evidence" audit is our exact quality-control need. Caveat: a pipeline, not a single portable algorithm. Confidence: medium.

### Multimodal Chemical Structure–Text Coreference (CheST / RULER)
- Findings ACL 2026, pp. 29784–29796 · `Zhong_Zhou_Multimodal_Chemical_Structure_Text_Coreference_in_Intel.pdf`
- Associate a **visual object (chemical structure) with its text referent across a long document**, distinguishing **atom-level differences between adjacent, near-identical structures**. ≅ cross-document coreference = **linking witnesses of one work across manuscripts**, and near-duplicate disambiguation. Caveat: the actual method is rule-guided RL over a domain verifier; only the task framing transfers. Confidence: speculative-medium.

### Bridging the Sensory Gap: Visual Injection for Taxonomy Completion (VITC)
- ACL 2026 Long, pp. 6092–6107 · `Niu_Yuan_Bridging_the_Sensory_Gap_Visual_Injection_for_Taxonomy.pdf`
- **Insert a new node into an existing is-a hierarchy**, disambiguating lexically-similar-but-distinct concepts; uses **cross-modal consensus to filter noise and identify hard negatives**. ≅ **F** (placing a new witness into a stemma — the tree we lack) + **G** (hard negatives). Caveat: taxonomy = semantic is-a hierarchy, not copy-lineage; the tree-insertion analogy is loose. Confidence: speculative.

### SIGMA: Generative Text-to-Image Retrieval via Hierarchical Identifiers
- Findings ACL 2026, pp. 12972–12986 · `Huang_Wang_Generative_Text_to_Image_Retrieval_via_Hierarchical_Ide.pdf`
- Generative retrieval with **multi-granularity hierarchical identifiers** + soft-label semantic internalization enabling **open-set dynamic indexing** (assign IDs to unseen items inductively). ≅ **L**; the inductive open-set indexing is relevant if we keep ingesting new folios. Caveat: generative-ID retrieval is a different, heavier paradigm than our similarity join. Confidence: speculative.

### microCLIP: Unsupervised CLIP Adaptation via Coarse-Fine Token Fusion
- Findings ACL 2026, pp. 33277–33294 · `Silva_Khan_microCLIP_Unsupervised_CLIP_Adaptation_via_Coarse_Fine.pdf`
- **Label-free self-training** for fine-grained image classification: fuse a saliency-guided fine-grained token with the global [CLS] for coarse-fine discrimination; pseudo-labels from a frozen prior. ≅ **G** (silver-label training) + fine-grained visual distinction (our near-identical-hand problem). Caveat: tied to CLIP classification, not retrieval/alignment. Confidence: speculative.

### CoMa: Compressing then Matching — Efficient Pre-training for Multimodal Embedding
- ACL 2026 Long, pp. 3707–3718 · `Li_Zhou_Compressing_then_Matching_An_Efficient_Pre_training_Par.pdf`
- Decouples "comprehensive understanding" from "discriminative contrastive"; a **compression warm-up before contrastive learning** yields strong embeddings from little data. ≅ **G** curriculum for Track-3 (warm-start the encoder before the hard contrastive stage). Caveat: a generic training-recipe tweak; benefit for our unimodal text-reuse unclear. Confidence: speculative.

### Auto-ReID: Iterative Self-Correction for Text-Driven Person Re-Identification
- Findings ACL 2026, pp. 6292–6301 · `Luo_Tang_Iterative_Self_Correction_for_Text_Driven_Person_Re_Ide.pdf`
- Reformulates retrieval as **Reasoner → Hybrid Retriever (anchors dynamic query with stable features to prevent drift) → Corrector (deconstruct + verify candidates)** — a closed-loop retrieve-verify-correct. ≅ **K/L**. Caveat: LLM-agent-heavy; our matcher is deterministic. Confidence: speculative.

### GranuRAG: Multi-Granularity Evidence Retrieval for Verifiable Multimodal RAG
- Findings ACL 2026, pp. 10475–10491 · `Chen_Wong_From_Scenes_to_Elements_Multi_Granularity_Evidence_Retr.pdf`
- Treats **visual elements as first-class retrieval units** (element-level, not whole-image) and explicitly models the **partial-observation challenge** (an image contains only a subset of entities). ≅ **L** + our **fragmentary-witness** reality (a fragment carries only part of a work). Caveat: a RAG benchmark; the granularity framing is the takeaway. Confidence: speculative.

### Glyph / Render-of-Thought (render-text-as-image cluster)
- `Cheng_Huang_Glyph_Scaling_Context_Windows_via_Visual_Text_Compressi.pdf` (ACL Long 37145–37158) + `Wang_Tang_Render_of_Thought_Rendering_Textual_Chain_of_Thought_as.pdf` (ACL Long 45236–45253).
- Both **render text into images and read them with a VLM** (Glyph: 3–4× context compression via a genetic search over rendering configs; RoT: renders reasoning steps, using VLM vision encoders as anchors to align image↔text embeddings). Resemblance to "read manuscripts as images," but purpose is *compression*, not retrieval/alignment. **B: resemblance-only.** The one genuinely reusable nugget: RoT's "use a frozen VLM vision encoder as a plug-and-play anchor to map image embeddings into the text space" is a cheap image↔text bridge for **L**. Confidence: speculative.

---

## C / skipped (coverage honesty)

Whole classes deliberately skipped after title-triage (no transferable mechanism per the profile — these are the bulk of the 315):

- **VLM hallucination mitigation** (~40+ papers: Agrawal, Fazli, Hwang, Huang_Fu survey, Mousi, Qin_Shen, Wang_Zheng, Zhu_He, DiVE, SGPVT, Inject-to-Heal, etc.) — decoding/attention tricks, no reuse.
- **VLM reasoning benchmarks & datasets** (~60+: OMIBench, GOBench, SciVQR, ComicVQA, MatVQA, ErrorRadar, Video-MMMU, BoYaEval, MSEarth, TableVista, PedagogyBench, K-MetBench, EmoS, Uni-MMMU, USB, VeriTaS, etc.) — pure eval.
- **Token pruning / KV-cache / efficient VLM inference** (~30+: HiPrune, TrimTokenator, CrisPrune, VisPCO, Vista-LLM, ReGATE, MM-ShiftKV, HERMES, CAPA, MoEC, etc.) — inference-efficiency, not our problem.
- **GUI / embodied / navigation agents** (~30+: Mobile-R1, NaviMaster, MAGNET, AVA-StarCraft, VLN-*, Embodied-Reasoner, Think-before-Go, On-the-Fly-VLA, SecureWebArena, etc.).
- **Video understanding / temporal QA / long-video** (~25+: MUSEG, APB-V, ViLL-E, MAVIS, WikiVideo, SIV-Bench, v-HUB, VALU, ArrowGEV, StreamMeCo, EgoMemory, etc.) — temporal grounding via attention/RL, not DTW-style alignment we could lift (SEA already dominates that slot).
- **Image/video generation & editing** (Stable-Signer, MENTOR, UniCorn, Self-Correcting-T2V, OSCBench, model-editing-for-video, etc.).
- **RL / reward / preference / SFT-data recipes for VLMs** (GRPO-CARE, DORA, View-R1, S2H-DPO, DaMo, AutoRubric, MARS-RA, VideoCuRL, etc.).
- **Audio-language** (PolyAudio, WoW-Bench, VocalRep, Tseng audio-pretraining, Jang prompt-tuning) — reasoning/pretraining/benchmarks; none is a monotonic/DTW text↔audio *alignment* paper, so no fresh angle beyond SEA.
- **Modality-collapse / robustness / calibration one-offs kept in mind but not carded** (MiMIC, SCOPE, VL-Calibration, VAUQ, He_Swayamdipta quality-scores) — H/I-adjacent but thinner than the A/B picks above.
- **On-domain ancient-script papers** (Song_Xu GEVO ancient-Chinese glyph evolution; Xu_Han FTibSuite Tibetan VLM) — these are wave-1 domain matches (glyph-level visual comparison, low-resource script resources), not wave-2 mechanism transfers; GEVO's "character evolution" flirts with lineage (**F**) but does no phylogenetics.
- **Membership-inference / fingerprinting** (Wang_Qi VideoMIA, Zheng_Wan Ghost-in-the-Shell) — profile flags MIA as B-worthy framing, but the concrete mechanisms are video-temporal / logit-shaping and don't transfer to text-reuse detection; noted, not carded.
- **Steganography** (Lijing_Zhang) — image-pixel hiding; no text-reuse analog.
