# Shared-Passage Detection over the MiDRASH HTR Corpus — Technical Method Report

**Project:** GenizahSearch / Dicta — same-work & shared-passage feasibility spike ("SEED-029")
**Date:** 2026-07-06 – 2026-07-07 (one-day probe + calibration + human evaluation)
**Status:** Feasibility probe complete, verdict **GO**. This document describes the full method,
the pipeline as validated, and the design of the production system it implies.
**Companion files:** `PROBE-RESULTS.md` (internal lab log), `review/discoveries_report.html`
(34 confirmed + 60 candidate manuscript pairs), `review/review.html` (grading tool),
`scripts/` (all code), `results/` (all measurements).

---

## 1. Problem statement

Given the MiDRASH HTR transcriptions of the Cairo Genizah — **948,549 page records across
216,911 manuscripts** (`Transcriptions.txt`, 1.47 GB; record header `==> {sys_id}_{IE}_{P######}_{FL} <==`)
— detect **pairs of pages that share a passage of text**, robustly to:

- HTR character noise (measured at 16–20% letter-CER, see §3),
- orthographic variation (plene/defective, final letters, Judeo-Arabic diacritics),
- scribal variation (omissions, additions, substitutions — no two Genizah copies of a
  text are ever verbatim-identical),
- partial overlap (a 50-letter quotation inside otherwise unrelated pages is a *positive*).

From page-pair passage evidence, the downstream goals are: (a) clustering witnesses of the
same textual **unit** (see §8.3 for why "unit", not "manuscript"), (b) identifying
uncatalogued fragments via textual matches to catalogued ones, and (c) surfacing citations
of non-canonical works — indirect textual witnesses, the highest-value discovery class.

---

## 2. Why seed-and-extend (and why not MinHash/LSH or embeddings)

The original design sketch used windowed character-TF-IDF + MinHash/LSH. A three-way review
(mathematical analysis, literature, corpus grounding) replaced it before any code was
written, and the probe retroactively validated the replacement:

1. **The math.** At CER ~10% *per side*, two copies of **identical** text have char-4/5/6-gram
   Jaccard ≈ 0.27 / 0.21 / 0.17. A 32-band × 4-row LSH captures only 2–15% of true pairs in
   that regime; widening bands to catch J≈0.2 pulls in the 0.03–0.05 background Jaccard of
   *unrelated* Hebrew pages → 10⁸–10⁹ noise candidates. Set-similarity has no usable
   operating point here.
2. **The literature.** Silcock et al. (ICLR 2023, arXiv:2210.04261) measured true OCR-duplicate
   Jaccard at 0.23–0.30; 19% of true pairs share *no* 10-gram. Turku's BLAST-based pipeline
   (Vesanto et al., NoDaLiDa 2017, W17-0510) recovered 2–3× more reuse than Passim on noisy
   OCR precisely because seed-and-extend *scores* mismatches instead of requiring set overlap.
3. **The quotation case.** A 50-char quote inside a 300-char window is Jaccard ≤ 0.09 with
   ZERO noise — window set-similarity is structurally blind to the headline use-case.
   Seed-and-extend measures the right objective: *does a contiguous-ish aligned span exist?*

Dense embeddings were deferred on the same evidence: published wins for dense retrieval over
lexical methods are on paraphrase/allusion gold sets, not verbatim-ish reuse under noise,
and retrieval quality degrades sharply from ~5% WER upward. (If lexical recall ever plateaus,
the designated fallback is MsBERT — manuscript-domain, Genizah-evaluated — contrastively
fine-tuned on synthetically HTR-noised pairs, not a general multilingual model.)

The resulting architecture is BLAST-shaped:

```
page text
   │  normalize (union view, §4)
   ▼
space-stripped Hebrew letter stream + offset map
   │  index char-5-grams, DF-banded (§5.1)
   ▼
inverted index: gram → [(page, position)]
   │  diagonal-keyed two-hit accumulation (§5.2)
   ▼
candidate page pairs + diagonal extent
   │  span extension + Levenshtein edit density (§6.1)
   │  sloped length×density acceptance boundary (§6.2)
   ▼
verified shared spans
   │  stage-0 dedup & junk filters (§7)   [in production: BEFORE indexing]
   │  flank-contrast classification (§8.1)
   │  canonical routing, two tracks (§8.2)
   ▼
same-unit links · identifications · citations
```

---

## 3. Measured noise profile (the empirical foundation)

Everything downstream is calibrated against a **measured**, not assumed, noise level.

**Method:** 209 pages for which both MiDRASH HTR and a human transcription (FGP scholarly
transcriptions, 45,034 available with resolved sys_ids) exist were aligned at the
normalized-letter-stream level (same normalization as the pipeline, §4) using Levenshtein
alignment; per-character edit operations were tallied into a confusion matrix
(`results/confusion_matrix.json`).

**Results:**

| Statistic | Value |
|---|---|
| Letter-CER, micro-average | **20.1%** |
| Letter-CER, median page | **16.6%** |
| p25 / p90 | 8.9% / 42% |
| Top substitution confusions | י↔ו (620), ד↔ר (597), ב↔כ (485), then ח↔ה, ה↔ת, ה↔ק |

Notes: this is an **upper bound** on HTR error (some divergence is editorial — the human
transcriptions expand abbreviations, normalize, etc.). The p25↔p90 spread (9%↔42%) means
square-script Bible hands and cursive documentary hands live in different noise regimes —
one global acceptance threshold cannot serve both, motivating the sloped/per-genre boundary
(§6.2) and, in production, confusion-weighted alignment costs (the matrix supplies the
weights; not yet implemented in the probe).

Consequence for two-sided matching: two independent CER-15% copies of *identical* text differ
by ~26–30% of letters — so an edit-density acceptance threshold of 0.30 sits exactly **at**
the noise floor and clips true liturgical pairs (which add nusach variance on top). This
single fact drove the boundary calibration of §6.2.

---

## 4. Normalization — one union view

`scripts/normalize.py::norm_stream`. Design principle: HTR word segmentation is unreliable
(split/merged tokens, whole lines of bracket shards), so **spaces carry no signal** —
matching operates on a space-free letter stream, with an offset map for projecting any
matched span back onto the original text for display.

Steps, in order:

1. Unicode NFC.
2. Every character that is not a Hebrew base letter (א–ת) is dropped: nikud, cantillation,
   all combining marks (including the Judeo-Arabic upper dot U+0307), punctuation, brackets,
   geresh/gershayim/quotes, digits, Latin, and **all whitespace**.
3. Final-letter fold: ךםןףץ → כמנפצ.
4. Output: `(stream, offsets)` where `stream` is the folded letter sequence and
   `offsets[i]` is the index in the (NFC) original of the letter at stream position `i`.
   `project_span(offsets, start, end, orig, pad)` recovers the original-text passage with
   context padding — this is how all human-facing evidence is rendered.

Example: `בְּרוּךֶ אַתָּה [יי] אלהינו…` → stream `ברוכאתהייאלהינו…`.

A "matres-light" second view (folding plene/defective spelling into the same shingle set)
is designed but not yet enabled; it awaits validation on known plene/defective witness pairs.

---

## 5. Candidate generation

`scripts/engine.py`. One implementation, two modes: a near-exhaustive **ground-truth mode**
(k=4, no DF filtering, posting-cap only) used to build evaluation oracles, and the
production-shaped **candidate mode** described here.

### 5.1 DF-banded character-5-gram inverted index

Every overlapping character 5-gram of every page's stream is posted to an inverted index
`gram → [(page, position)]`. Grams are then filtered by **document frequency** (number of
distinct pages containing the gram):

- DF = 1 (singletons): dropped — cannot generate a pair.
- DF > 100 pages (pilot value): dropped — formulaic/ubiquitous material
  (ברוך אתה, אמר רבי, blessing formulae) generates quadratic candidate noise.
- Additionally a hard posting-length cap (3,000) guards against degenerate grams.

**Counter-intuitive empirical finding (probe F3): DF-banding *improves* recall on liturgical
text.** Under a bounded per-pair anchor budget, common grams spread anchors across many
false diagonals; DF-banding spends the budget on distinctive grams that concentrate on the
true diagonal. On the Birkat-Hamazon witness set, DF=50/30 found 415/699 witness pairs vs
135 with no DF filtering. The intuition "protect formulaic genres from DF filtering" is
exactly backwards.

Scaling note: the DF cap is an **absolute** page count, so it becomes *relatively* stricter
as the corpus grows (at 948K pages, most formulaic grams exceed 100 pages and drop out
automatically). Candidate volume therefore does not extrapolate linearly from the pilot —
it must be measured at intermediate scale (§9).

### 5.2 Diagonal-keyed two-hit filtering

A pair of pages becomes a candidate only if it shares **≥ 2 distinct grams on (nearly) the
same alignment diagonal** — the BLAST two-hit heuristic, made diagonal-consistent at
accumulation time (`engine.build_diag_pairs`):

- For each shared gram occurrence at positions `(pos_a, pos_b)`, compute the diagonal
  bucket `(pos_a − pos_b) // 20`.
- Accumulate per key `(page_a, page_b, bucket)` a **fixed-size record**
  `[count, min_a, max_a, min_b, max_b]` — no position lists are stored, so memory per
  candidate is O(1).
- `per_gram_pair_cap = 1`: each gram contributes at most one hit per pair, so the two hits
  are guaranteed to be **two distinct grams** (with a higher cap, one repeated gram could
  satisfy the two-hit rule alone).
- A pair is emitted if some bucket cluster (bucket ± 1, i.e. diagonal tolerance ±20–40
  letters) accumulates ≥ 2 hits; the cluster's min/max extents seed the verification span.
- Same-manuscript pairs (equal sys_id) are excluded at generation time — recto/verso and
  refoliations of one manuscript are not discoveries.

Pilot parameters: `k=5, df_drop=100, min_anchors=2, band=20, per_gram_pair_cap=1`.

**Measured effect (17,228-page pilot):** candidate volume 31.7M pairs (naive two-hit)
→ **11.4M** (diagonal-keyed, distinct-gram), with candidate **recall 1.00 preserved on all
three ground-truth families** (§10.1). Verification costs ~11 µs/pair, so 11.4M candidates
verify in ~125 s single-threaded.

---

## 6. Verification

### 6.1 Span extension + edit density

For each candidate (`engine.verify_span`): take the diagonal cluster's extent, extend by a
margin of 30 letters on each side, extract the two substreams, and compute Levenshtein
distance (rapidfuzz). Define:

- `aligned_len` = max(len(span_a), len(span_b)) — span length in letters;
- `density` = Levenshtein distance / aligned_len — the edit density, 0 = identical.

Reject if the shorter span < 25 letters, or density exceeds the acceptance boundary.

### 6.2 The sloped acceptance boundary (calibrated, then human-validated)

A flat density ≤ 0.30 rule sits at the two-sided noise floor (§3) and clips high-noise true
pairs: Birkat-Hamazon witness connectivity rises 24% → 69% as the threshold moves
0.30 → 0.45. But a flat 0.45 admits short random near-matches. The resolution is a
**sloped length × density boundary**: short evidence must be clean; long evidence may be
noisy (a 300-letter span at density 0.42 is statistically unreachable by chance — random
Hebrew letter streams align at density ≈ 0.60).

Thresholds were fitted per length band as the 95th percentile of true-pair densities per
ground-truth family, constrained monotone non-decreasing (`scripts/roc_boundary.py`,
`results/roc_boundary.md`). The adopted profile (`liturgy_q95`):

| Span length (letters) | Max edit density |
|---|---|
| 25 – 100 | **0.30** |
| 100 – 200 | **0.386** |
| ≥ 200 | **0.418** |

Recall at this boundary (pilot): joins **1.00**, title-groups **0.984**, BH witnesses
**0.974**; BH witness connectivity 241/428 (56%) vs 82/428 (19%) at flat 0.30.

Human validation (§10.3): in the 164-pair graded sample, even the 0.40–0.45 density band is
**97% real shared text** — the boundary can safely sit at ~0.42–0.45 for spans ≥ 100 letters.

Production refinement (designed, not yet implemented): confusion-weighted substitution costs
from the §3 matrix (ד↔ר, י↔ו, ב↔כ cheaper than arbitrary substitutions), which sharpens the
separation between HTR noise and genuine textual difference.

---

## 7. Stage-0: corpus hygiene (mandatory)

The probe identified the false-positive classes empirically; **all of them are mechanical**.
In production these filters run *before* indexing.

**(a) Same-image duplicates.** The corpus contains the same photograph under multiple
sys_ids — NLI catalog variants (`997…`-prefixed sys_ids) sharing identical IE/P/FL image
identifiers. Trivially collapsed on the FL image id.

**(b) Same-shelfmark duplicates.** Different sys_ids resolving to the same physical
shelfmark (via the libraries master table) — catalog-level duplication.

**(c) Re-photographed pages — the line-break-agreement detector.** The subtlest duplicate
class: the *same physical page* photographed twice (e.g. once in a two-page book-spread
shot, once as a single page), HTR'd independently — different FL ids, different sys_ids,
extremely high text similarity. These masquerade as spectacular "discoveries."
The tell (due to Hillel Gershuni): **line breaks are a property of the physical page** —
two genuine textual witnesses never agree on line breaks; two photographs of the same page
must. Detector: split both HTR texts into lines, keep lines of ≥ 10 letters; if the pages
have ≥ 4 such lines and **≥ 60% of lines match in order** at normalized Levenshtein
≤ 0.30, flag as duplicate photography. Validated against human grades: **precision 100%
(31/31), recall 74%**; the recall remainder is covered by tiers (a)/(b) plus threshold
tuning. Bonus: same-page-HTR'd-twice pairs constitute a free HTR-vs-HTR variance
measurement set.

**(d) Microfilm target sheets / catalog cards.** Pages that are HTR renderings of scale
bars and the FGP microfilm card template (recurring words: סימן, תוכן, מחבר, שנה, הערות)
match each other across unrelated manuscripts. Filter: template-keyword + low-entropy
heuristic. In the graded sample, 6 of 8 short-span pairs were this class — short spans are
unusable until this filter is in place.

Empirical justification: in the graded evaluation, **every single one of 36 "join anomaly"
pairs** — physically-joined fragments that appeared to share text — turned out to be
duplicate photography, not textual overlap. (Corollary corpus fact: physically-joined
fragments share running text in ~0% of cases — they are *consecutive*, not *overlapping*;
physical joins are therefore useless as recall positives for this task.)

---

## 8. Classification of accepted spans

### 8.1 The flank-contrast classifier

Accepted spans are evidence of *shared text*; the *kind* of sharing matters. Heuristic (due
to Hillel Gershuni), mechanized: after accepting a span, align the ~150-letter **flanks** on
both sides of the span in both pages (2 extra Levenshtein calls per accepted pair):

- **Flanks also align** → the match *continues* beyond the span → the pages are running
  witnesses of the same text → **same-unit evidence**.
- **Flanks dissimilar** (density near the ~0.60 random floor) → the span is an **island**
  → quotation or shared formula, not a common work.
  - island ∧ span matches the canonical index → **canonical quotation** (routed to Track 1);
  - island ∧ **not** canonical → **citation of a non-canonical work** — an indirect textual
    witness, the project's most valuable discovery class.

### 8.2 Two-track canonical handling

Canonical text (Bible, Mishnah, Talmud, standard liturgy) is both the biggest noise source
for discovery and a first-class identification target. The architecture splits it
structurally rather than by post-hoc filtering:

- **Track 1 — fragment ↔ clean canon (identification).** Match noisy HTR pages against
  *clean* reference corpora (Maagarim/Sefaria). One-sided noise means seed survival
  (1−CER)² instead of (1−CER)⁴ — a fundamentally easier regime. The designated method is
  Shmidman-Koppel-Porat rare-letter skip-grams (arXiv:1602.08715) with two HTR adaptations
  (maxDF posting cap; relaxed cluster validation), or the same seed-and-extend engine with
  the canon as one side. Output: canonical span labels per page + direct identifications
  ("this page is Mishnah Shabbat 3"; "this page quotes Bavli Berakhot").
- **Track 2 — fragment ↔ fragment (discovery).** The engine of §§5–6, with canonical spans
  **masked at the character level before indexing**, so discovery similarity is scored on
  distinctive shared wording only. (In the probe, canon was not yet masked; the graded
  evaluation shows canonical shares are the dominant residual class — i.e., Track-1 masking
  absorbs most of what would otherwise be routing noise.)

### 8.3 Unit-level semantics (annotation policy, binding)

Established during the human evaluation and binding for all future annotation and
clustering: **"same text" is judged at the level of the textual UNIT, not the codicological
container.** A siddur's Birkat Hamazon and a Haggadah's Birkat Hamazon are the *same text*
(shared unit, different containers). Two Bible manuscripts of the same passage are the same
text (both are witnesses of the work — "canonical quotation" means a quotation embedded in
a *different* work). Consequence: same-work clustering must cluster **units** — connected
components over span-level links, segmented by within-page span extent — not manuscripts.

---

## 9. Scale plan (948K pages)

Pilot cost points: pure-Python inverted index reached 15 GB RAM at 17K pages; the
diagonal-keyed accumulator holds ~122M entries ≈ 12 GB in Python dicts at pilot candidate
volume. Both are representation problems, not algorithmic ones — the algorithm is
sort-merge-shaped:

1. Encode postings as numpy arrays; candidate accumulation = radix/sort-merge over packed
   `(page_a, page_b, bucket)` keys, processed in gram-sharded passes (embarrassingly
   parallel; the dev box has 12C/24T, 63 GB).
2. The DF≤100 cap self-tightens with corpus growth (§5.1), so per-page candidate volume at
   948K pages must be **measured, not extrapolated** — the pilot is deliberately enriched
   with related material and overstates density.
3. Verification is embarrassingly parallel and already cheap (11 µs/pair).

Ordered execution plan: stage-0 module → **100K-page rehearsal** with the numpy
representation (measure candidates/page, RAM, wall-clock vs corpus size) → Track 1 canon
labeling → full-corpus run. All compute on the dev box; never on the production web server.

**Rehearsal outcome (2026-07-07, `REHEARSAL-RESULTS.md`):** 102,568 pages end-to-end in
~14 min single-threaded; recall rose at scale (Tier-1 titles 0.993); raw-hit volume grows
~linearly, not quadratically (DF-cap self-tightening confirmed: 146M → 654M hits for 5.7×
pages; full corpus ≈4–5B → disk-partitioned merge). Stage-0 over the full corpus cut it to
≈667K effective pages (24% short/empty, 40,452 duplicate photographs, 9,007 target sheets).
First map: 337K accepted page pairs → 244K manuscript pairs; a giant liturgical component
(15,969 MSS) survives flank-contrast — **empirical confirmation that Track-1 canonical
masking is the prerequisite for a legible works census** (§8.2).

---

## 10. Evaluation

### 10.1 Ground truth: Tier-1 verifier-filtering

Known same-work pairs (catalog joins, shared title-groups, witness indexes) frequently share
**no overlapping passage** (different parts of the work survive) — using them raw as recall
positives is structurally wrong. Protocol: run a near-exhaustive oracle (ground-truth mode:
k=4, no DF filtering) over all known-related pairs once; **Tier-1** = the subset with a
verified shared span. Tier-1 is the recall denominator; the remainder is reported as a
corpus fact, not a miss. Three families were used:

| Family | Source | Outcome |
|---|---|---|
| Physical joins | FJMS join-groups | ~0% share running text (§7) — excluded as positives |
| Title-groups | FJMS catalog `GenizahTitleId` | 64% of sampled groups textually connected; heterogeneous |
| BH witnesses | Human witness index (below) | The liturgical stress-test |

**Candidate-stage recall vs Tier-1: 1.00 / 1.00 / 1.00** on all three families, inside a
pilot of 17,228 pages (1,393 BH witness pages + 1,088 join pages + 4,963 title-group pages
+ 740 FGP-overlap pages + 10,000 random background pages). The seed stage loses nothing;
the acceptance boundary is the only tunable loss point.

### 10.2 The Birkat-Hamazon experiment (witness-index-as-oracle)

An external, human-compiled witness index (מפתח כתבי היד of Sefer Birkat Hamazon) was parsed
from the source docx: 484 sigla, **471 resolved (97.3%)** to 597 sys_ids → 1,393 HTR pages.
This provides an oracle *independent of the pipeline*: any cross-witness link found is
presumptively true (after stage-0), and connectivity of the witness set measures liturgical
recall directly. Results: witness connectivity 24% → 69% across density 0.30 → 0.45
(flat threshold), 56% at the adopted sloped boundary; of BH pages with any partner at the
strict threshold, 65% pointed to another known BH witness, and most of the remainder shared
*other* liturgy (Hallel, psalms) with non-BH siddur pages — correct detections with a
different-unit label, reinforcing both the two-track design and the unit-level semantics.
The pattern generalizes: any indexed composition's witness list can serve as a recall oracle.

### 10.3 Human-graded precision (n = 164)

244 machine-selected pairs were stratified across the false-positive frontier (the
100–300-letter × 0.35–0.45 density region), the discovery stream, join anomalies, boundary
cases, and short spans, and rendered in an RTL side-by-side review tool with highlighted
spans (`review/review.html`). Hillel Gershuni graded 164 on an 8-class scale
(same_text / paraphrase / shared_formula / topical / unrelated / junk / canonical /
duplicate_photo). Grades: `review/grades_hillel_2026-07-07.json`; analysis:
`results/grades_analysis.md`.

| Measurement | Result |
|---|---|
| Actually spurious (topical/unrelated) | **1 / 164 (0.6%)** |
| Precision after stage-0 removes duplicates+junk | **110 / 111 = 99.1%** |
| Real-shared-text rate per density band 0.30→0.45 | 100% / 100% / 100% / 97% |
| Discovery stratum | **34 / 40 genuine same-composition discoveries** (6 = duplicate photography) |
| Join-anomaly stratum | 36 / 36 duplicate photography — zero textual joins |
| Boundary stratum (loosened threshold region) | 29 / 31 same_text — the raised boundary vindicated |
| Short-span stratum | 6 / 8 microfilm title sheets — short spans need stage-0 first |

Headline conclusion: **the engine's residual error mass is routing, not precision** — the
apparent false positives are overwhelmingly correctly-detected shared text of the wrong
*kind* (canonical quotations, formulae, duplicate photography), and every one of those
classes is mechanically classifiable (Track 1 + flank-contrast + stage-0). Caveat: the
pilot is enriched with related material; corpus-wide precision still requires the pooled
sampling evaluation at scale, but the boundary calibration itself is now human-grounded.

### 10.4 Yield

Within the enriched pilot alone: 1,335 discovery-class pairs at ~85% raw precision;
aggregated to manuscript level, **34 human-confirmed + 60 machine-filtered candidate
manuscript pairs** (`review/discoveries_report.html`), including **5 identification
candidates** — uncatalogued CUL fragments textually matching catalogued RNL Bible
manuscripts — and one fully-organic find: two random-background manuscripts carrying the
same halakhic text on אונאה (1,658 aligned letters, density 0.15). A full-corpus run should
yield thousands of genuine new same-unit links.

---

## 11. Parameter summary (adopted values)

| Stage | Parameter | Value |
|---|---|---|
| Normalization | alphabet | Hebrew base letters only, finals folded, space-stripped |
| Indexing | gram length k | 5 (letters) |
| | DF drop | > 100 pages (absolute; self-tightens with scale) |
| | posting cap | 3,000 |
| Candidates | min anchors (two-hit) | 2 distinct grams |
| | diagonal bucket width | 20 letters (cluster = bucket ± 1) |
| | per-gram-pair cap | 1 |
| | same-sys_id pairs | excluded |
| Verification | span margin | 30 letters |
| | min span | 25 letters |
| | acceptance boundary | density ≤ 0.30 (len < 100) / 0.386 (100–200) / 0.418 (≥ 200) |
| Stage-0 dup detector | line agreement | ≥ 60% of lines (≥ 10 letters, ≥ 4 lines) in-order at norm-Lev ≤ 0.30 |
| Flank classifier | flank length | ~150 letters per side |

---

## 12. Code & artifact map

All under `same_work_spike/probe/`:

| Path | What |
|---|---|
| `scripts/normalize.py` | union-view normalizer + offset back-projection |
| `scripts/engine.py` | the engine: `build_anchor_pairs` (naive mode), `build_diag_pairs` + `verify_span` (production shape), `verify_pair`, `run` |
| `scripts/extract_pilot.py`, `define_buckets.py` | pilot corpus construction → `data/probe.db` |
| `scripts/confusion_matrix.py` | HTR↔FGP alignment → `results/confusion_matrix.json` |
| `scripts/ground_truth.py` | Tier-1 oracle → `results/tier1.json` |
| `scripts/separability.py`, `separability2.py` | the probe runs → `results/verified_pairs*.json`, scatter plots |
| `scripts/roc_boundary.py` | boundary fit → `results/roc_boundary.{md,png,json}` |
| `scripts/resolve_bh_witnesses.py` | witness-index docx → sys_ids (reusable for any מפתח כתבי היד) |
| `scripts/bh_experiment.py`, `bh_q3.py` | liturgical recall sweeps |
| `scripts/prep_review_tool.py` | grading tool → `review/review.html` (contains the line-agreement detector) |
| `scripts/analyze_grades.py` | grades → `results/grades_analysis.md` |
| `scripts/build_discoveries_report.py` | manuscript-pair aggregation → `review/discoveries_report.html` + `results/discoveries_report.csv` |

Dependencies: Python 3.10+, `rapidfuzz` (Levenshtein), `python-docx` (witness index parsing),
numpy/matplotlib for analysis. No GPU. Full pilot (17,228 pages) runs end-to-end in ~10 min
single-threaded on a desktop.

---

## 13. Known limitations & open items

1. **Track 1 canon masking is designed, not yet run** — the dominant residual routing class
   in the graded sample is canonical shares; masking + labeling is the next structural gain.
2. **Confusion-weighted alignment costs** not yet implemented (matrix is measured and ready).
3. **Matres-light union view** designed, pending validation on plene/defective pairs.
4. **Corpus-wide precision** requires the pooled evaluation at scale; the pilot is enriched.
5. **Judeo-Arabic**: per-language DF tables and evaluation strata are designed but the pilot
   grading was predominantly Hebrew; JA needs its own stratum.
6. The candidate-volume ↔ corpus-size curve must be measured at the 100K rehearsal before
   committing full-corpus compute (the DF-cap self-tightening makes pilot extrapolation
   pessimistic, but this is an argument, not a measurement).
7. HTR corpus (Zenodo 10.5281/zenodo.17734473, v0.8, CC-BY-4.0) self-describes as
   preliminary — segmentation and reading-order errors are part of the measured noise;
   improved HTR releases would raise every number here.

---

## 14. References

- Shmidman, Koppel, Porat — *Identification of Parallel Passages Across a Large Hebrew/Aramaic Corpus*, arXiv:1602.08715.
- Silcock, D'Amico-Wong, Dell — *Noise-Robust De-Duplication at Scale*, ICLR 2023, arXiv:2210.04261.
- Vesanto et al. — *Applying BLAST to Text Reuse Detection in Finnish Newspapers*, NoDaLiDa 2017, aclanthology W17-0510.
- Passim v2 — github.com/dasmiq/passim (KITAB/OpenITI recipes).
- MsBERT — aclanthology 2024.ml4al-1.2 (dicta-il/MsBERT).
- Midrash text-reuse benchmark — arXiv:2512.23504.
- CHR 2024 Syriac HTR→canon identification — ceur-ws.org/Vol-3834/paper110.pdf.
- MiDRASH HTR corpus — Zenodo 10.5281/zenodo.17734473 (CC-BY-4.0, v0.8).
