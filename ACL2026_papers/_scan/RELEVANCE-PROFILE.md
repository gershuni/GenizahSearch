# Relevance profile — ACL 2026 scan for the Genizah research program

## Who we are
Dicta's GenizahSearch: search + research platform over ~948K HTR-transcribed
Cairo Genizah manuscript pages (Hebrew, Judeo-Arabic, Aramaic; 16-20% letter
CER). Active research program (SEED-029): corpus-scale text-reuse detection —
char-5-gram seed-and-extend + banded Levenshtein verification, DF-banded
candidate generation, a Track-1 asymmetric matcher against 8,300 clean
reference editions, witness-census / citation-web / motif-decomposition
products. Next frontier ("Track-3"): a semantic layer — sentence embeddings
for paraphrase-level intertextuality, trained with silver labels from our own
lexical engine + LLM-as-annotator (cf. Cohen et al. 2026, "Scaling Sentence
Similarity for Classical Tibetan", NLP4DH — our reference point for what
"highly relevant" means).

## Relevance buckets (tag each card with all that apply)
1. **reuse** — text reuse / intertextuality / parallel passage detection /
   citation detection / plagiarism methods / collation, critical editions,
   witness alignment at scale.
2. **embed** — sentence/passage embeddings & STS for historical, low-resource
   or noisy text; contrastive training recipes; hard-negative mining; silver/
   synthetic annotation; embedding-based retrieval at corpus scale.
3. **noise** — OCR/HTR noise: post-correction, restoration of damaged/
   fragmentary text, noise-robust retrieval/embeddings/NER, RAG under OCR
   noise, spelling/orthographic normalization, historical-variant handling.
4. **semitic** — Hebrew, Judeo-Arabic, Arabic (incl. dialects), Aramaic,
   Syriac; diacritics; root-and-pattern morphology; relevant base models.
5. **xlingual** — translation detection/alignment (our Judeo-Arabic <->
   Hebrew problem), interleaved/code-switched text, transliteration,
   cross-lingual embedding alignment.
6. **llm-annot** — LLM-as-annotator/judge for philological tasks, Best-Worst
   Scaling, committee/ensemble labeling, annotation-error auditing,
   human-AI annotation loops.
7. **ancient** — any ancient/classical/medieval language NLP with a
   transferable method (Tibetan, Latin, Greek, Coptic, Sanskrit, Classical
   Chinese, cuneiform, Oracle Bone...): datasets, models, pipelines,
   decipherment, dating, paleography, multimodal manuscript understanding.
8. **product** — adoptable for the GenizahSearch app: semantic search, RAG
   over noisy corpora, NER for archives/catalogs, search UX, evaluation of
   retrieval quality, efficient serving.
9. **scale** — engineering for all-pairs / massive-corpus similarity:
   ANN indexes, dedup at scale, MinHash/LSH alternatives, efficient
   rerankers.

## Priorities
- **A** = we should act on this (steal a technique, use a dataset/model,
  cite as method precedent). Justify in one sentence what we'd do.
- **B** = solid awareness value; informs design choices; cite-worthy.
- **C** = peripheral / same keywords, different problem. One line only, no card.

## Card schema (uniform — copy exactly)
### <Title>
- **id**: <anthology id or arxiv> · <track/workshop> · **file**: <relative path>
- **authors/lab**: <first author et al., institution if notable; flag
  potential contacts (Israeli groups, DH labs, people working on Semitic/
  historical NLP)>
- **tl;dr**: 2-3 sentences, what they did and what they found.
- **relevance**: [buckets] — 1-2 sentences mapping to OUR pipeline/needs
  (name the specific component: seed-and-extend, Track-1, DF cap, motif
  decomposition, Track-3 embeddings, HTR noise, JA<->Hebrew, web app...).
- **stealable**: the concrete technique/resource/number we could adopt.
- **priority**: A / B
- **bib leads**: 2-5 cited works (title + year) worth chasing for our
  problems, from the paper's references. Skip generic ones (BERT, SBERT).

## Method notes for scanners
- PDFs are LOCAL. Do not fetch from the web.
- Titles are weak evidence. For any maybe-relevant title, extract the
  abstract (page 1) before judging:
  `python -X utf8 -c "import fitz; print(fitz.open(r'<path>')[0].get_text()[:2500])"`
- Deep-read (more pages incl. references) ONLY for A/B candidates.
- Shell is Windows PowerShell 5.1 — no `&&`, no heredocs; prefer one-liner
  python -c calls like the above.
- Write your cards to your assigned output file (UTF-8), A-priority cards
  first, then B. End the file with a `## C / skipped` section listing
  C-verdict titles one line each with a 3-6 word reason.
- In your FINAL agent reply return only: counts (scanned/abstract-checked/
  A/B/C) + the 3 most exciting finds, one line each.
