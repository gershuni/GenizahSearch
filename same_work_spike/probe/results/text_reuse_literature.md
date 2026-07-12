# Text-Reuse Detection: A Literature Survey for SEED-029

**Purpose.** We built a character-5-gram seed-and-extend matcher that identifies noisy HTR Genizah
pages (16–20% CER) against reference editions, with a probability-graded "candidate tier" below the
strict threshold. This survey maps the scholarly literature onto our four live pain points:

- **(a)** distinguishing **CITATION vs SAME-WORK PARALLEL** — a page *quoting* a Bible verse / Talmud
  passage vs. a page that *is a witness* of a work;
- **(b)** verse / liturgy **chains and formulaic text**;
- **(c)** **calibrated confidence** on very short fragments;
- **(d)** **quotation direction** (who quotes whom).

A short glossary is at the end. Effort-to-adopt ratings assume our existing 5-gram + alignment stack.

---

## 0. Our own prior art (do not reinvent)

**Shmidman, Koppel & Porat, "Identification of Parallel Passages Across a Large Hebrew/Aramaic Corpus"**
(*JDMDH* 2018; arXiv 2016). [[JDMDH]](https://jdmdh.episciences.org/4175)
· [[arXiv 1602.08715]](https://arxiv.org/abs/1602.08715)

- **What it does.** Finds all near-duplicate 4–5-word passages in the Talmud (1.8M words, ~30s) by
  representing each word by its **two rarest letters**, matching windows that differ by ≤1 word, then
  clustering matched pairs. Coverage matches exhaustive search.
- **Idea we already own.** The "two rarest letters" skeleton is a cheap noise- and orthography-tolerant
  fingerprint; it is the philosophical ancestor of our char-5-gram seeds. Worth re-reading before we
  add any new fingerprinting — the org has published on exactly this.
- Dicta's current production detector (per the ACT benchmark below) uses **skip n-grams of size 2–5**
  and scores F1≈0.78 on biblical-quotation detection. That is our internal baseline to beat, and it is
  the number external groups now cite when they beat us.

**Most important new result for us: ACT (Miller, Kuflik & Lavee, U. Haifa, 2025).**
"Automatic Detection of Complex Quotation Patterns in Aggadic Literature."
[[arXiv 2512.23504]](https://arxiv.org/abs/2512.23504) — details in §1. This is the single closest paper
to our problem and it explicitly benchmarks against Dicta, passim and Text-Matcher.

---

## 1. Pain point (a): CITATION vs SAME-WORK PARALLEL — the core problem

### ACT — "Allocate Connections between Texts" (Miller, Kuflik, Lavee 2025) — **most relevant paper**
[[arXiv 2512.23504]](https://arxiv.org/abs/2512.23504)

- **What it does.** Three-stage detector of *biblical* quotations inside *Rabbinic* text — precisely the
  citation case we must separate from same-work witnesses. **F1 = 0.91** (P 0.94 / R 0.89), beating
  Dicta (0.78), passim (0.62), Text-Matcher (0.51) and even manual critical editions (0.64).
- **Pipeline.** (1) Normalize (strip vocalization, *matres lectionis*, special chars) + build a
  **positional inverted index** of the reference (Bible). (2) Sliding-window n-grams query the index;
  survivors are aligned with a **morphology-aware aligner** (Miller 2025) tolerant of paraphrase,
  orthographic variation, **transcription noise**, and word-order changes — directly applicable to our
  16–20% CER. (3) **Quotation enrichment**: score each hit by summed **log word-rarity**
  (`Σ log P(word)`), discard below a grid-searched threshold (they used 21), then infer *citation
  structure*.
- **Ideas we could borrow (high value):**
  1. **Rarity-weighted log-likelihood match score** (`Σ log P(word)`) as our candidate-tier score, so a
     match built from rare words outranks one built from stopwords. *Directly attacks (a), (b), (c).*
     **Effort: low.** (We have corpus frequencies already.)
  2. **"Wave" / "Echo" / "Compound" structural labels.** A *citation* is typically a short verse span
     interspersed with commentary ("wave": verse fragment → elaboration → next fragment) or a repeated
     verse ("echo"). A *same-work witness* is a long, continuous, monotone run with no interleaved
     foreign material. **This structural signature IS our flank-contrast test, formalized:** detect
     interleaving vs. continuation. **Effort: medium** — implement interleave/continuation classification
     on top of our aligned spans.
  3. **Single-word quotations enabled (n=1)** but rescued from false positives *by the rarity score +
     context*, not by length. Lesson for (c): don't gate short fragments by length alone; gate by rarity
     mass. **Effort: low.**
- **Effort to adopt the whole approach: medium.** The morphology-aware aligner is the one heavy piece;
  the scoring and structural labels are cheap and immediately useful.

### passim (David A. Smith et al.) — alignment-based reuse at scale
[[GitHub]](https://github.com/dasmiq/passim) · [[Programming Historian lesson]](https://programminghistorian.org/en/lessons/detecting-text-reuse-with-passim)
· [[Infectious Texts PDF]](https://www.ccs.neu.edu/home/dasmith/infect-bighum-2013.pdf)

- **What it does.** Detects/​aligns reused passages across huge noisy OCR newspaper corpora.
  **n-gram filtering** to pick candidate document pairs, then **Smith–Waterman local alignment**
  (character or word), emitting **clusters** of witnesses that share a text span.
- **Ideas we could borrow:**
  - **Local (Smith–Waterman) alignment with affine gaps** as our extend step. Local alignment naturally
    finds the *maximal contiguous* aligned span and *stops at the flanks* where similarity drops — which
    is exactly the boundary information pain point (a) needs. The score at which the alignment terminates
    on each side is a ready-made **flank-contrast signal**. **Effort: medium.**
  - **Cluster-then-analyze**: group all witnesses of one span, then reason about the cluster (many short
    hits of the same verse across unrelated pages ⇒ citation/formula; one long hit shared by two pages ⇒
    same-work). **Effort: low–medium.**
- **Effort: medium.** passim itself is Spark/Scala; we'd port the *algorithm* (n-gram filter →
  Smith–Waterman → cluster), not the tool.

### KITAB — passim on a religious Arabic corpus (directly analogous genre)
[[kitab-project.org/methods/text-reuse]](https://kitab-project.org/methods/text-reuse)

- **What it does.** Runs passim over classical Arabic (Islamic scholarship, heavy Qur'an/hadith
  quotation) and reports *reuse profiles* rather than binary hits.
- **Idea we could borrow.** Their **"milestone" alignment visualisations and per-pair reuse-density
  profiles** are a proven way to eyeball citation (spiky, localized) vs. shared-recension (broad,
  diagonal). Useful UX/QA pattern for Hillel's review tier. **Effort: low** (analysis/visualisation, not core).

---

## 2. Pain point (b): verse/liturgy chains & formulaic text

### Tesserae — frequency-weighted intertext scoring (Coffee, Koenig et al.)
[[LLC 2012 paper (PDF)]](https://tesserae.caset.buffalo.edu/blog/wp-content/uploads/2012/10/coffee-et-al.llc2012.pdf)
· [[project site]](https://tesserae.caset.buffalo.edu/)

- **What it does.** Ranks Latin/Greek intertextual parallels by matching shared word *pairs* and scoring
  them so that **rare words that sit close together** score highest.
- **The scoring formula (borrow this exactly):**

  > **score = ln [ ( Σ 1/frequency of each matched word, in both texts ) / ( distance_source + distance_target ) ]**

  where *distance* = number of tokens spanned by the two rarest matched words. High score = rare words,
  tightly clustered. This structurally **demotes formulaic/liturgical matches** (built of common words,
  or of common words spread far apart) without a hand-built stop list.
- **Also:** Tesserae uses a **stop-list of the N most frequent lemmas** that are ignored as match anchors.
- **Ideas we could borrow (top pick for (b)):**
  - **Inverse-frequency (idf-style) weighting of the matched words**, divided by span distance, as our
    candidate score. A liturgical chain of high-frequency formulae scores *low*; a distinctive lexical
    match scores *high*. Solves the "verse-chain / formula false positive" problem cleanly. **Effort: low.**
  - **Frequency-ranked stop-anchor list** (drop the top-N most common tokens as *seeds*, keep them for
    alignment/scoring). Prevents seeds from firing on `אשר`, `את`, liturgical refrains, etc.
    **Effort: low.**

### n-gram idf / burstiness for formulaic passages (general IR + passim/BLAST practice)

- **What it does.** Down-weights n-grams that occur in many documents (document frequency), the same way
  TF-IDF down-weights common terms — a standard move in passim's candidate filter and in Vesanto's BLAST
  pipeline.
- **Idea we could borrow.** Cap or down-weight **high-document-frequency n-grams** (SEED-029's own "DF-cap"
  work is exactly this instinct — the literature validates it). Combine DF-cap on *seeds* with
  Tesserae-style idf on *scoring*. **Effort: low** (we already have a DF-cap notion).

### Manjavacas, Long & Kestemont — allusive reuse when words barely overlap
[[arXiv 1905.02973]](https://arxiv.org/abs/1905.02973) · [[ACL W19-2514]](https://aclanthology.org/W19-2514/)

- **What it does.** Treats allusion detection as IR (alluding text = query, source = document). Finding:
  **plain TF-IDF cosine beats fancier query models**; distributional/embedding semantics give only a
  *moderate* boost. A sobering calibration for anyone tempted to jump straight to neural methods.
- **Idea we could borrow.** For the *hard, low-overlap* tail (paraphrase, allusion), a **TF-IDF cosine
  fallback** over a sliding window is a cheap, strong second-pass retriever. **Effort: low–medium.**

---

## 3. Pain point (c): calibrated confidence on short fragments

- **The ACT lesson (see §1):** gate short matches by **rarity mass, not length** — a 3-word match of
  three rare words can be more trustworthy than a 6-word match of stopwords.
- **Calibration mechanics (general ML).** Turn raw scores into probabilities and *check* them:
  fit **Platt scaling / isotonic regression** on a labelled dev set, then verify with a **reliability
  diagram** and **Expected Calibration Error (ECE)** — "matches we call 60% confident are right ~60% of
  the time." Directly gives our candidate tier an *honest* probability.
  [[LLM entity-matching calibration, arXiv 2509.19557]](https://arxiv.org/pdf/2509.19557)
  **Effort: low** (a few hundred labelled pairs + scikit-learn).
- **Biblical-Hebrew embedding benchmark (Smiley 2025).** "Intertextual Parallel Detection in Biblical
  Hebrew: A Transformer-Based Benchmark" [[arXiv 2506.24117]](https://arxiv.org/abs/2506.24117) and the
  follow-up **MiqraBERT** [[arXiv 2606.19638]](https://arxiv.org/abs/2606.19638) fine-tune Sentence-BERT
  (from AlephBERT) with cosine-similarity regression on 1,650 verse pairs; recall@10 ≈ 87% on synoptic
  parallels. **Idea:** a small fine-tuned bi-encoder gives a **semantic similarity score** that
  complements our lexical score — useful both as a calibration feature and for the paraphrase tail.
  Note these are *Modern/Biblical-Hebrew* encoders, not noisy-HTR-Genizah-tuned. **Effort: high**
  (training data + eval); park as a research bet, not a quick win.

---

## 4. Pain point (d): quotation direction (who quotes whom)

### "Mining Asymmetric Intertextuality" (2024) — **the on-topic paper for direction**
[[arXiv 2410.15145]](https://arxiv.org/abs/2410.15145)

- **What it does.** Explicitly models **asymmetric** (source → derivative) links, not just symmetric
  "these two share text." Pipeline: **metadata-enriched hierarchical chunking → hybrid (lexical+vector)
  search with metadata filtering → LLM-based verification** that decides *which* text is the source.
- **Ideas we could borrow:**
  - **Direction is decided at a verification stage, after cheap retrieval** — mirror our architecture:
    seed-and-extend proposes the pair, a light *direction classifier* rules on it. **Effort: medium.**
  - **Metadata as the cheap direction prior.** For us, direction is often *known from metadata*:
    the reference edition (dated printed/critical text) vs. an undated Genizah fragment; a Bible/Talmud
    canon vs. a commentary. A rule using canon status + date + genre resolves most cases with **zero ML**.
    **Effort: low** and probably our first move.
  - Reserve **LLM verification** for the genuinely ambiguous residue. **Effort: medium.**

### Quotation attribution literature (adjacent, for framing)
[[DirectQuote, arXiv 2110.07827]](https://arxiv.org/abs/2110.07827) ·
[[Muzny et al. two-stage sieve]](https://nlp.stanford.edu/pubs/muzny2017twostage.pdf)

- **What it does.** Extracts quotations and attributes them to *speakers* in news/novels (sieve of
  deterministic rules → learned model). Not our exact problem (speaker ≠ source-text), but the
  **"deterministic sieve first, learned model for the residue"** design is a clean template for our
  direction module. **Effort: low** (design pattern only).

---

## 5. Historical text-reuse engines (infrastructure & method reference)

### TRACER / eTRAP (Marco Büchler)
[[eTRAP]](https://www.etrap.eu/research/tracer/) · [[TRACER manual]](https://tracer.gitbook.io/manual/)

- **What it does.** ~700 configurable algorithms for historical reuse, staged as
  **selection → linking → scoring → post-processing**, with word-based features (for paraphrase) and
  n-gram features (for verbatim). Designed for allusion/paraphrase/translation across ancient languages.
- **Idea we could borrow.** TRACER's **explicit "featuring" step** — replace each word by a *feature*
  (lemma, synset, rare-letter skeleton, sound) before matching — is a principled place to plug in
  **noise-tolerant features for HTR** (e.g. confusable-glyph classes). Its **6-stage decomposition** is a
  good mental model for where our flank-contrast/direction stages slot in. **Effort: medium** (adopt the
  staging idea, not the Java tool).

### Vesanto et al. — BLAST for newspaper reuse (Finnish, 1771–1910)
[[ACL W17-0510]](https://aclanthology.org/W17-0510/) ·
[[journal version]](https://www.tandfonline.com/doi/full/10.1080/01615440.2020.1803166)

- **What it does.** Encodes text and runs **NCBI BLAST** (biological local-alignment) to find reuse in
  massive noisy-OCR corpora; scales via BLAST's seed-and-extend heuristics + Solr.
- **Idea we could borrow.** BLAST *is* seed-and-extend with a substitution-scoring matrix — conceptually
  our matcher. The transferable trick is an **HTR-aware substitution/scoring matrix** (analogous to
  BLOSUM): reward alignments through *known confusable characters* rather than treating every CER error
  as a plain mismatch. **Effort: medium**, and a strong fit for 16–20% CER.

### Early-Chinese reuse (Sturgeon; AncientTRD)
[[Sturgeon, DSH 2018]](https://academic.oup.com/dsh/article/33/3/670/4583485) ·
[[AncientTRD, 2025]](https://www.mdpi.com/2076-3417/15/19/10475)

- **What it does.** Unsupervised reuse detection in a corpus with **almost no explicit citation markers**
  (like ours) — n-gram overlap + alignment, later work adds neural/graph modelling of millions of pairs.
- **Idea we could borrow.** Confirms that in citation-marker-poor corpora the **structural/statistical
  signature** (localized overlap density) is the discriminator, not markers — reinforces the
  flank-contrast approach. **Effort: low** (validation/framing).

---

## 6. המאגר שכבר נאסף — ACL 2026 (the in-house collection, cross-referenced)

The project already holds a two-wave scan of the ACL 2026 mirror (5,019 papers):
`ACL2026_papers\ACL2026-LIBRARY.md` (curated library: 28 A-tier + ~64 B-tier verdicts, evidence
cards in `_scan/` and `_scan2/`), plus `TRACK3-DECISION-BRIEF.md` /
`TRACK3-ENRICHMENT-BIBLIOGRAPHY.md` (the semantic-embedding syntheses). This section integrates
that collection with the survey above: what it adds, what is already wired into the pipeline, and
where the two *independent* sweeps converge — which is itself evidence to trust those items.

### 6.1 What the collection adds beyond §§1–5

| Paper (collection) | What it adds to this survey | Pain point |
|---|---|---|
| **Sun & Zhang — "When Good OCR Is Not Enough"** | Low CER ≠ retrieval success; *structural* errors dominate. Citable precedent for our witness-recall-over-CER evaluation choice. | (c), eval |
| **Sarawgi et al. — Old Nepali HTR pipeline** | Token-level confusion analysis → **substitution-cost-weighted Levenshtein**: the same recipe as TOP-5 #3, derived independently from HTR practice. | CER, (a) |
| **Miyagawa & Kyogoku — ICoMa (Yajurveda collation)** | 5-metric agreement as a robustness check; per-section reuse profiles as transmission evidence; ICoMa supports Hebrew script. | (a), QA |
| **Wu & Tolonen — semantic vs lexical reuse (Locke)** | The honest "lexical gatekeeping" ceiling argument + a **4-tier reception taxonomy** usable as our label schema; dense retrieval robust to OCR noise. | (a), labels |
| **Momen & Zarrieß — frequency confound** | "Low-DF = distinctive" is partly a raw-frequency artifact — **control for frequency before crediting a rare-gram match**. Tempers both TOP-5 #1 (Tesserae weighting) and ACT's rarity score. | (b), (c) |
| **Burns et al. 2021 — Tesserae + embeddings** | The canonical "bolt a semantic re-ranker onto a lexical seed-and-extend engine *without discarding it*" — exactly how Track-3 should ride on Tracks 1–2. | (b), tail |
| **D'Angelo et al. — Greek formulae, contrastive** | The most on-point recipe for the liturgical-formula stratum (Greek *formulae* ≈ piyyut formulae) — attacks MiqraBERT's poetic <9% collapse. | (b) |
| **MiqraBERT close-read (`TRACK3-DECISION-BRIEF.md`)** | Much deeper than §3's note: the **E5 trap** (highest raw cosine = *worst* separator), Wasserstein/overlap as the primary metric, and the poetic collapse named as our #1 Track-3 risk. | (c) |
| **Dataset Cartography (wave-2 caveat)** | Raw confidence is confounded by class rarity — naive confidence-pruning would systematically purge **rare works and rare scripts**. A hard constraint on any calibration scheme (TOP-5 #5). | (c) |
| **Silcock, D'Amico-Wong & Dell — Noise-Robust De-Duplication at Scale** (ICLR 2023; already in METHOD.md's bibliography, missed by this survey's first pass) | Neural + hashing dedup benchmarked on noisy-OCR reprints at scale — the modern successor to the passim/BLAST line. | scale |

### 6.2 The five wave-2 convergences — implemented vs still open

The library's wave-2 scan distilled **5 mechanism convergences**, already wired into
`SYNTHESIS-AND-PLAN.md`. Status against the pipeline as of this writing:

| # | Convergence | Pipeline status | Cross-ref to this survey |
|---|---|---|---|
| 1 | **Span-shadowing as an assignment problem** (weighted set cover / optimal transport) | **Implemented** in greedy form (`track1_shadow.py`; Temple-Scroll class solved; 61,922 rows shadowed). Set-cover reformulation is an open *probe* (plan A4: keep greedy if disagreement <2%). | Complements §1 — citation-vs-witness conflicts resolved competitively. |
| 2 | **Stemma kit** (shared-innovation ≈ Lachmann's shared errors; cross-perplexity trees; edge purification; date-constrained direction) | **Open** — capped exploratory spike D1 on the BH 291-witness web. | Overlaps pain point **(d)**: direction-via-structure+dating is the stemma kit's answer, alongside §4's metadata-first rule. |
| 3 | **Concept erasure** (LEACE + label-free verification) for scribe/script/language identity | **Open** — Track-3 pilot item; out of scope for the lexical engine. | Track-3 only. |
| 4 | **Conformal p-values + FDR control** replacing hand-tuned density gates (+ ODASim monotone-calibrated similarity, isotonic remaps) | **Open, planned as A5** — calibrate on the 225 graded pairs, per-genre strata, FDR ≤5%. | **= TOP-5 #5**, arrived at independently. The collection's version is stronger: a provable false-positive bound at 1.34M-pair scale vs plain Platt/isotonic. |
| 5 | **HTR confusion matrix as a triple asset** (edit-cost prior + reanalysis pass; visual-confusability hard negatives; orthographic-innovation weights for stemma) | **Half-done** — the matrix is measured (י↔ו, ד↔ר, ב↔כ) but confusion-weighted alignment costs are explicitly *not yet implemented* (METHOD.md §13, item 2). | **= TOP-5 #3** (BLAST/BLOSUM substitution matrix), arrived at independently from the bioinformatics side. |

Also load-bearing: **flank-contrast is already a graded-in pipeline feature** (island ⇒
citation/formula vs continuation ⇒ same work, per `SYNTHESIS-AND-PLAN.md` §1), so §1's ACT
recommendation is an *upgrade to an existing mechanism*, not a new build.

### 6.3 Convergent vs new — where the two sweeps agree, and what this survey adds

**Convergent (independent agreement — trust these):**
- **ACT / arXiv 2512.23504** — found independently here; already in METHOD.md's bibliography as
  the "Midrash text-reuse benchmark." Same paper, two routes.
- **HTR-aware edit costs** — this survey via BLAST/BLOSUM (§5); the collection via Nepal-HTR and a
  noisy-channel reading paper (Clark/Levy/Gibson). Three independent routes to one artifact.
- **Calibrated thresholds over hand-tuned gates** — this survey via Platt/isotonic + ECE; the
  collection via conformal+FDR (plan A5). Same instinct; the conformal version wins on guarantees.
- **Term/DF weighting against formulaic text** — this survey via Tesserae scoring; the pipeline via
  DF-caps + per-query DF immunity (motif-query). Complementary, not redundant: DF-caps act on
  *seeds* (recall side), Tesserae-style weighting acts on *scoring* (precision side) — and heed the
  Momen & Zarrieß frequency confound on both.
- **Manjavacas' honesty result** (semantics gives only *moderate* gains on allusion) — cited by
  both sweeps; the Track-3 brief builds its expectation-setting on it.
- **Tesserae, passim/KITAB, Vesanto BLAST, MsBERT** — all already in the collection's
  second-degree reading list or METHOD.md's bibliography.

**New in this survey (not in the collection):**
1. **ACT's concrete mechanics** — the wave/echo/compound structural taxonomy, the `Σ log P(word)`
   rarity score with a grid-searched cutoff, single-word quotations rescued by rarity mass, and
   the benchmark in which ACT (F1 0.91) beats Dicta's production detector (0.78). The collection
   knew the paper as a benchmark reference; this survey extracts what to *borrow* from it.
2. **Tesserae's exact scoring formula** — `ln[(Σ 1/freq)/(dist_src + dist_tgt)]`. The collection
   lists Tesserae as reading; the formula is the actionable piece.
3. **The quotation-direction literature** — "Mining Asymmetric Intertextuality" (arXiv 2410.15145:
   metadata-filtered retrieval → LLM verification of direction) and the quote-attribution
   "deterministic sieve → learned residue" pattern (Muzny). The collection's nearest analogue is
   direction-via-stemma (convergence 2); this survey adds the cheap metadata-first rule and the
   verification-stage architecture for pain point (d).
4. **TRACER's staged decomposition and Sturgeon's marker-free early-Chinese precedent** — framing
   for where the flank-contrast/direction modules slot into a reuse pipeline.

---

## Glossary

- **Seed-and-extend:** find short exact/near-exact anchors ("seeds"), then extend outward by alignment.
- **Local alignment (Smith–Waterman):** finds the best-matching *substring* pair and stops where match
  quality falls off — its endpoints are natural passage boundaries.
- **idf / document frequency:** how many documents a term/n-gram appears in; high df ⇒ common/formulaic ⇒
  down-weight.
- **Calibration / ECE:** whether a "0.6 confidence" prediction is actually right 60% of the time;
  ECE measures the gap.
- **Bi-encoder (Sentence-BERT):** neural model mapping a text span to a vector; cosine distance ≈ semantic
  similarity, catches paraphrase with no shared words.

---

## TOP-5 worth adopting (ranked; annotated against the ACL 2026 collection, §6)

1. **Tesserae-style inverse-frequency scoring divided by span distance** *(pain points b, a, c)* —
   `ln[(Σ 1/freq of matched words) / (dist_src + dist_tgt)]`, plus a **top-N frequent-token stop-anchor
   list**. Kills formulaic/liturgical false positives and gives a principled candidate-tier score with
   almost no new infrastructure. **Effort: low.**
   *§6 note:* new to the plan (the collection lists Tesserae only as reading); complements the
   pipeline's seed-side DF-caps on the *scoring* side. Apply the Momen & Zarrieß frequency-confound
   control (don't credit rarity without a raw-frequency check).
   [Tesserae LLC 2012]

2. **ACT's "wave/echo/continuation" structural test as the flank-contrast v2** *(a)* —
   classify a hit as *interleaved citation* vs. *continuous same-work run* using interleave-vs-continuation
   on aligned spans; pair with ACT's **`Σ log P(word)` rarity score** and a grid-searched cutoff.
   The closest published work to our exact problem, and it beats Dicta's F1 (0.91 vs 0.78). **Effort: medium.**
   *§6 note:* flank-contrast is already a graded-in pipeline feature — this is an **upgrade**
   (formal taxonomy + rarity gate), not a new build. ACT was independently flagged in METHOD.md's
   bibliography; the mechanics extracted here are the new part.
   [ACT, arXiv 2512.23504]

3. **Smith–Waterman local alignment (affine gaps) as the extend step, with an HTR-aware substitution
   matrix** *(a, and 16–20% CER)* — maximal contiguous span + built-in flank drop-off (the boundary
   signal we're hand-rolling), and confusable-glyph-aware scoring à la BLAST/BLOSUM. **Effort: medium.**
   *§6 note:* triple-convergent — same artifact demanded by wave-2 convergence 5 (Nepal-HTR edit
   costs, Clark/Levy/Gibson noisy-channel) and METHOD.md's own open item #2 ("confusion-weighted
   alignment costs not yet implemented; matrix measured and ready"). The strongest-supported item
   on this list; the confusion matrix then serves Track-3 hard negatives and stemma weights for free.
   [passim / Vesanto BLAST / ACL wave-2 convergence 5]

4. **Metadata-first direction rule, LLM/classifier only on the residue** *(d)* — resolve who-quotes-whom
   from canon status + date + genre first (near-free), reserve neural verification for ambiguous pairs;
   architecture and "deterministic sieve → learned residue" pattern from asymmetric-intertextuality and
   quote-attribution work. **Effort: low → medium.**
   *§6 note:* the survey's genuinely new contribution — the collection has no direct
   who-quotes-whom line; its nearest neighbor is the stemma kit (convergence 2, spike D1), whose
   date-constrained-direction methods this rule would front-end cheaply.
   [Mining Asymmetric Intertextuality, arXiv 2410.15145; Muzny sieve]

5. **Score calibration — execute plan A5 (conformal p-values + FDR), with Platt/isotonic as the
   on-ramp** *(c)* — turn raw candidate-tier scores into *honest* probabilities so short-fragment
   confidence means what it says. **Effort: low.**
   *§6 note:* independently re-derived here and **already planned as A5** (wave-2 convergence 4);
   the collection's conformal+FDR version supersedes plain Platt/isotonic — it gives a provable
   false-positive budget across all 1.34M pairs. Two constraints from the collection: per-genre
   strata, and **never confidence-prune rare works** (Dataset Cartography). The fine-tuned Hebrew
   bi-encoder feature stays a Track-3 research bet (MiqraBERT / JABERT line, gated by plan C1).
   [ACL wave-2 convergence 4; entity-matching calibration arXiv 2509.19557; MiqraBERT arXiv 2606.19638]

---

### Key sources
- Shmidman, Koppel, Porat — Parallel Passages (Hebrew/Aramaic): https://jdmdh.episciences.org/4175 · https://arxiv.org/abs/1602.08715
- Miller, Kuflik, Lavee — ACT / Aggadic quotation patterns: https://arxiv.org/abs/2512.23504
- passim (Smith et al.): https://github.com/dasmiq/passim · https://programminghistorian.org/en/lessons/detecting-text-reuse-with-passim
- KITAB (Arabic reuse via passim): https://kitab-project.org/methods/text-reuse
- Tesserae (Coffee et al.): https://tesserae.caset.buffalo.edu/blog/wp-content/uploads/2012/10/coffee-et-al.llc2012.pdf
- TRACER / eTRAP (Büchler): https://www.etrap.eu/research/tracer/ · https://tracer.gitbook.io/manual/
- Vesanto et al. — BLAST reuse: https://aclanthology.org/W17-0510/
- Manjavacas et al. — allusive reuse: https://arxiv.org/abs/1905.02973
- Mining Asymmetric Intertextuality: https://arxiv.org/abs/2410.15145
- Sturgeon — early Chinese reuse: https://academic.oup.com/dsh/article/33/3/670/4583485 · AncientTRD: https://www.mdpi.com/2076-3417/15/19/10475
- Smiley — Biblical Hebrew benchmark / MiqraBERT: https://arxiv.org/abs/2506.24117 · https://arxiv.org/abs/2606.19638
- Confidence calibration (entity matching): https://arxiv.org/pdf/2509.19557
- Quotation attribution: https://arxiv.org/abs/2110.07827 · https://nlp.stanford.edu/pubs/muzny2017twostage.pdf
