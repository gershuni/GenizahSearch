# ACL 2026 — WAVE 2: the analogy hunt (mechanism over domain)

Wave 1 asked "does this paper match our domain?" and covered the on-topic
papers (NLP4DH, OCR, Semitic, low-resource, retrieval). **Wave 2 asks a
different question: does this paper's MECHANISM solve a problem that is
ISOMORPHIC to one of ours, even though its surface domain looks unrelated?**

The best ideas often arrive in disguise. Data-contamination detection is
text-reuse detection wearing a different hat. Code-clone / AST-diff is
manuscript collation. Genomics sequence alignment is the literal ancestor of
our seed-and-extend. A sparse-autoencoder that erases a concept from an LLM is
how we would strip scribe-hand or language-identity from an embedding. Your job
is to see through the domain to the mechanism.

## Who we are (context)
Dicta's GenizahSearch: ~948K HTR-transcribed Cairo Genizah manuscript pages
(Hebrew, Judeo-Arabic, Aramaic; 16–20% letter error). Research program
SEED-029 does corpus-scale text-reuse detection: char-n-gram seed-and-extend →
banded Levenshtein verification, DF-banded candidate generation with a
two-pass disk-spill engine, an asymmetric matcher against 8,300 clean
reference editions, competitive span "shadowing", motif decomposition, and a
planned semantic-embedding layer ("Track-3").

## Our problems, stated as DOMAIN-NEUTRAL abstractions (the transfer targets)
Match papers to these, NOT to Genizah keywords. A paper about proteins, stock
prices, or Python code can be an A if it nails one of these:

- **A · Noisy sequence alignment** — align two symbol streams differing by
  substitution/insertion/deletion; recover maximal matching spans under noise.
  (our seed-and-extend + banded Levenshtein over 16–20% CER)
- **B · All-pairs near-duplicate detection at 10^6+ scale** — find all similar
  pairs without O(n²) blowup. (DF-banding, disk-spill; kin: dedup, MinHash/LSH
  successors, similarity joins, **data-contamination detection**, membership
  inference, plagiarism/clone detection)
- **C · Competitive assignment / overlap resolution** — when several
  labels/spans claim one region, keep the best. (our shadowing; kin: NMS,
  bipartite matching, set cover, competitive/constrained decoding)
- **D · Change-point / boundary segmentation** — cut a sequence into coherent
  units at natural seams. (our motif breakpoints; kin: time-series change-point,
  topic/discourse segmentation)
- **E · Graph community detection / clustering** — group nodes from a
  similarity graph. (our motif-v2 + witness census; kin: Louvain successors,
  overlapping/­correlation/spectral clustering)
- **F · Lineage / tree reconstruction** — infer who-copied-whom from shared
  features. **WE DO NOT DO THIS YET — high value.** (stemmatics; kin:
  phylogenetics, ancestry/tree inference, hierarchical mer\­ge trees)
- **G · Contrastive learning from noisy / graded / silver labels** — train
  embeddings when labels are auto-generated and ordinal. (Track-3; kin: hard-
  negative mining, graded/ranking losses, label denoising, curriculum)
- **H · Calibration / threshold choice with NO gold** — decide which matches to
  trust without ground truth. (our density gates / conf labels; kin: conformal
  prediction, self-consistency, uncertainty estimation)
- **I · Concept removal / disentanglement in representations** — strip an
  unwanted factor (scribe hand, language identity, script, genre) from vectors.
  (kin: SAE concept erasure, steering, projection, adversarial removal)
- **J · Rare-event / needle-in-haystack detection** — find sparse targets in a
  huge pool. (our fragmentary-prize + sparse error auditing; kin: active
  learning, anomaly detection, hard-example mining)
- **K · Draft-then-verify / cheap-candidate-then-expensive-check loops** — our
  two-stage matcher and restoration self-refine. (kin: speculative decoding,
  verifier models, self-refinement, generate-and-rank)
- **L · Cross-representation alignment** — align two spaces (Judeo-Arabic↔Hebrew,
  image↔text, script↔script). (kin: optimal transport, procrustes, pivot/bridge,
  cross-modal contrastive)

## Calibration: what a GOOD wave-2 find looks like
- Data-contamination / n-gram-overlap dedup paper → **B** (their algorithm may
  beat our DF-banded generation).
- Code-clone detection, AST/graph diffing → **A/B** (collation of variants).
- Genomics/protein tokenization, minimizers, alignment-free comparison → **A**.
- Speculative decoding, self-consistency, verifier reward models → **K/H**.
- Membership inference / watermark-under-paraphrase → **B** robust variant.
- SAE feature steering / concept unlearning → **I**.
- Time-series change-point, segmentation, motif discovery → **D**.
- Phylogenetics / tree / ancestry inference → **F** (the stemma we lack).
- NMS, bipartite matching, competitive decoding → **C** (shadowing).
- Cross-document coreference / entity linking → linking witnesses of one work
  across manuscripts.
- Optimal-transport / bridge alignment → **L** (JA↔Hebrew).

## Scoring (DIFFERENT from wave 1)
- **A = adopt**: a transferable mechanism we could actually use AND that we would
  NOT have found by domain keyword. Reward `surprise × usefulness`. A boring
  on-domain match is NOT an A here — wave 1 already has those.
- **B = worth knowing**: a mechanism with a plausible but weaker/less-certain
  transfer, or a useful framing.
- **C = no real transfer** (one line).

## Guard against apophenia (IMPORTANT — stay honest)
It is tempting to manufacture connections. Do not. For every A card include a
**"why it might NOT transfer"** line and a **transfer confidence** (high /
medium / speculative). If the link is only a poetic resemblance with no
concrete mechanism we could run, mark it **B: resemblance-only** and move on.
We want real isomorphisms, not horoscopes.

## Card schema (copy exactly)
### <Title>
- **id / track / file**: <anthology-or-arxiv id> · <track/workshop> · <relative path>
- **surface domain**: what the paper is ostensibly about (1 line)
- **mechanism**: the transferable technique, stated domain-neutrally (1–2 lines)
- **analogy**: `their <X in domain D>  ≅  our <problem-letter + component>` — be explicit
- **why it transfers**: 1–2 sentences on the concrete adoption for us
- **why it might NOT**: 1 sentence (the honest caveat)
- **transfer confidence**: high / medium / speculative
- **priority**: A / B
- **bib leads**: 0–4 cited works worth chasing (skip if none distinctive)

## Method notes
- PDFs are LOCAL — never fetch the web.
- This is TITLE-TRIAGE at scale. Skim titles; abstract-check only when a
  MECHANISM might transfer (extract page-1 text with:
  `python -X utf8 -c "import fitz; print(fitz.open(r'<path>')[0].get_text()[:2500])"`).
  Deep-read only strong A candidates. Expect to card only a handful per hundred.
- Shell = Windows PowerShell 5.1 (no `&&`, no heredocs; use python -c one-liners).
- Write cards to your assigned output file (UTF-8), A first then B, then a short
  `## C / skipped` note (counts + what whole classes you deliberately skipped —
  we need to know coverage honestly).
- FINAL agent reply: counts (titles-skimmed / abstracts-checked / A / B) + your
  3 most SURPRISING finds, one line each (lead with the analogy).
