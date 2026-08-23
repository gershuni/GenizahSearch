# Passage-Matching Algorithm (v1)

**Status:** specification. Implementable without access to any gitignored research tree.
**Scope:** the character-level passage matcher — normalization, gram indexing, candidate
generation, verification, acceptance boundaries, hygiene, and the interactive query budget.
**Audience:** anyone implementing or auditing passage retrieval in either app.

This document exists because the algorithm had no single home. It was distributed across a
gitignored method report (which specifies fragment-to-fragment matching in full but treats the
fragment-to-reference direction at design level only), the code of three research scripts, and
roughly ten `discovery-*` specs that each cover one downstream concern. Every constant below is
traceable — see the source map at the end.

**Vocabulary note.** Reference corpora are named only by their masked codes (`sefaria`, `ja`,
`msource`). This file is tracked and public-facing; provider identities are not recorded here.
`scripts/check_atlas_masking.py` gates it.

---

## 1. What problem this solves

Given a query text and a corpus of manuscript transcriptions, find the corpus locations that
carry the same passage — robustly to:

- **character noise.** The transcription corpus has a measured letter-level character error rate
  of **20.1% micro-average, 16.6% median page** (p25 8.9%, p90 42%), from aligning 209 pages
  against independent human transcriptions.
- **unreliable word segmentation.** The dominant error class is split and merged tokens, plus
  whole lines of bracket shards. This is why the algorithm is character-based: no amount of
  per-term fuzziness repairs a boundary error.
- **orthographic variation.** Plene/defective spelling, final-letter forms, Judeo-Arabic
  diacritics.
- **scribal variation.** No two manuscript copies of a text are verbatim identical.
- **partial overlap.** A 50-letter quotation inside two otherwise unrelated pages is a
  *positive*, not noise.

Consequence of the noise measurement, and the fact that drives every threshold below: two
independent copies of *identical* text at 15% CER per side differ by **26–30% of letters**. An
edit-density acceptance threshold of 0.30 therefore sits exactly **at** the two-sided noise
floor.

---

## 2. Why seed-and-extend, and not set similarity or embeddings

Recorded because the question recurs and the answer is counter-intuitive.

1. **The arithmetic.** At 10% CER per side, two copies of identical text have character 4/5/6-gram
   Jaccard of about 0.27 / 0.21 / 0.17. A 32-band by 4-row LSH captures 2–15% of true pairs in
   that regime; widening bands to reach J of about 0.2 pulls in the 0.03–0.05 background Jaccard
   of *unrelated* Hebrew pages. There is no usable operating point.
2. **The quotation case is structurally invisible to windowed set similarity.** A 50-character
   quote inside a 300-character window is Jaccard 0.09 or less with zero noise. Seed-and-extend
   measures the right objective: *does a contiguous-ish aligned span exist?*
3. **Published precedent.** Noisy-OCR duplicate detection measures true-duplicate Jaccard at
   0.23–0.30, with 19% of true pairs sharing no 10-gram; a BLAST-based pipeline recovered 2–3x
   more reuse than a set-similarity baseline on noisy OCR precisely because seed-and-extend
   *scores* mismatches instead of requiring set overlap.

Dense embeddings are deferred on the same evidence: published wins for dense retrieval over
lexical methods are on paraphrase and allusion gold sets, not verbatim-ish reuse under noise, and
retrieval quality degrades sharply above about 5% WER. If lexical recall ever plateaus, the
designated fallback is a manuscript-domain encoder contrastively fine-tuned on synthetically
noised pairs — not a general multilingual model.

---

## 3. Normalization — one union view (`normalizer_version: 1`)

Design principle: **spaces carry no signal.** Matching operates on a space-free letter stream,
with an offset map for projecting any matched span back onto the original text for display.

Steps, in order:

1. Unicode **NFC**.
2. Fold final letters: `ך ם ן ף ץ` to `כ מ נ פ צ`.
3. Keep **only** Hebrew base letters `U+05D0`–`U+05EA` (alef through tav). Everything else is a
   separator and is **dropped**: nikud, cantillation, every combining mark including the
   Judeo-Arabic upper dot `U+0307`, punctuation, brackets, geresh, gershayim, quotes,
   apostrophes, digits, Latin, and **all whitespace**.
4. Emit `(stream, offsets)` where `offsets[i]` is the index **in the NFC-normalized original** of
   the letter at stream position `i`.

Note the fold precedes the range test, so a final letter is folded and kept rather than dropped.
`offsets` indexes NFC text, not the raw input — NFC may shift positions, and NFC text is what is
stored and displayed.

**Span projection** maps a stream span from `start` to `end` back to original text with optional
context padding: take `offsets[start]` and `offsets[end-1] + 1`, expand by the pad, clamp to the
text bounds. This is the only sanctioned way to render human-facing evidence.

**The normalizer is versioned and is a hard artifact-identity input.** Any change to the
alphabet, the folding, or the offset semantics invalidates every built index. Version it in the
artifact manifest and refuse to load on mismatch.

Designed but **not enabled**: a "matres-light" second view folding plene and defective spelling
into the same gram set. It awaits validation on known plene/defective witness pairs. If enabled
it must be unioned into the same index, never built as a second index.

---

## 4. Gram coding

Character **5-grams** (`K = 5`) over the normalized stream, encoded as a base-27 positional
integer:

```
code = 0
for j in 0 .. K-1:
    code = code * 27 + (ord(stream[position + j]) - 0x05D0)
```

The alphabet is 27 symbols (alef through tav after final folding), so the code space is
`27^5 = 14,348,907` — comfortably under `2^24 = 16,777,216`. A CSR offset array therefore needs
`27^5 + 1` entries; sizing it to `2^24 + 1` instead wastes 2.4M slots (9.7 MB at uint32) for
simpler indexing. Either is acceptable as long as the manifest records which.

A stream of length `L` yields `max(0, L - K + 1)` grams. Grams **must not cross a record
boundary**; the builder asserts this.

---

## 5. Index orientation — the three arrangements

The same engine admits three arrangements, distinguished only by which side is persisted. This
distinction is the single most important thing in this document, and it was previously implicit
in code.

| Arrangement | Index persisted over | Streamed side | Cost | Use |
|---|---|---|---|---|
| **A. Reference-resident** | reference works | corpus records | O(corpus) per pass | Batch identification: label every record with the works it carries |
| **B. Query-resident** | a batch of query texts | corpus records | O(corpus) per pass | Batch recovery: give one work's grams immunity from a global corpus DF cap |
| **C. Corpus-resident** | **corpus records** | **the query** | O(query x postings admitted) | **Interactive retrieval — what a search surface needs** |

A and B are both O(corpus) per pass, which is fine when hundreds of queries amortize over one
sweep and useless for a single interactive query. **C is the inversion an interactive surface
requires, and it is the one no research script implements.**

Sections 6 through 9 are orientation-neutral. Section 10 covers what changes for C.

---

## 6. Candidate generation — diagonal-keyed two-hit

A location becomes a candidate only if the two sides share **at least 2 distinct grams on
(nearly) the same alignment diagonal**.

- For each shared gram occurrence at positions `(pos_a, pos_b)`, compute the diagonal bucket
  `(pos_a - pos_b) // BAND` with `BAND = 20`.
- Accumulate per key `(a, b, bucket)` a **fixed-size** record
  `[count, min_a, max_a, min_b, max_b]`. No position lists are stored, so memory per candidate is
  O(1).
- A candidate is emitted when a bucket cluster (bucket plus or minus 1, a diagonal tolerance of
  20–40 letters) reaches `count >= MIN_ANCHORS = 2`. The cluster's min and max extents seed the
  verification span.

### 6.1 Distinctness — a decision this spec settles

"Two distinct grams" needs enforcing, and the research code enforces it two different ways:

- The pair-wise engine dedupes postings to the **first position per (gram, record)** before
  accumulating, so one repeated gram cannot satisfy the two-hit rule by itself. Exact, but it
  discards positions and therefore blurs span extents.
- The asymmetric matcher keeps **all** positions and does not track gram identity during
  accumulation, so two hits from one repeated gram *can* create a candidate. This is a deliberate
  relaxation and it costs precision.

**For arrangement C, do neither.** Keep all positions — accurate span extents are needed for
display — and enforce distinctness explicitly: sort hits by `(record, bucket, gram_code)` and
count **distinct gram codes** per cluster with a first-occurrence mask. That is one extra reduce
over data already sorted, it is exact, and it preserves positions. Any implementation that
adopts the relaxation instead must say so in the result envelope.

---

## 7. Verification

For each candidate: extend the cluster extent by `MARGIN = 30` letters on each side, extract the
two substreams, and compute Levenshtein distance (`rapidfuzz`, with a score cutoff for early
exit).

- `aligned_len = max(len(span_a), len(span_b))`
- `density = levenshtein_distance / aligned_len` — 0 is identical, and **unrelated Hebrew letter
  streams align at density of about 0.60**. That random floor is the reference point that makes
  long noisy spans statistically safe.

Reject if the shorter span is below the minimum-span contract (section 8), or if `density`
exceeds the acceptance boundary.

### 7.1 The two acceptance boundaries, and why there are two

Under the two-hit rule, candidate survival scales as `p^2` when only one side is noisy and `p^4`
when both are, where `p` is the probability that a single gram survives on a noisy side — two
required anchors, times the number of noisy sides each must survive. The one-sided regime is
therefore fundamentally easier, and it can afford a **tighter** boundary because its true
matches are cleaner.

| Regime | Span length (letters) | Max density |
|---|---|---|
| **One-sided** — clean query against noisy corpus | 40–99 | **0.28** |
| | 100 and above | **0.35** |
| **Two-sided** — noisy query against noisy corpus | 40–99 | **0.30** |
| | 100–199 | **0.386** |
| | 200 and above | **0.418** |

The two-sided boundary is sloped because a flat 0.30 rule sits at the two-sided noise floor and
clips high-noise true pairs, while a flat 0.45 admits short random near-matches. The resolution
is length-dependent: short evidence must be clean, long evidence may be noisy — a 300-letter
span at density 0.42 is statistically unreachable by chance against a 0.60 random floor.

**Calibration provenance, and an asymmetry between the two rows.** The **two-sided** thresholds are the 95th percentile of true-pair
densities per length band, fitted per ground-truth family and constrained monotone
non-decreasing. Measured recall at that boundary: catalog joins 1.00, title-groups 0.984,
liturgical witness pairs 0.974. Human validation on a 164-pair graded sample found the 0.40–0.45
density band to be **97% real shared text**.

The **one-sided** thresholds are **not** a fit. They are implementation constants in the
asymmetric matcher, justified by the survival argument above but never calibrated against a
labelled set. The table presents both rows with equal authority; they do not have it, and the
one-sided row is the weaker of the two.

### 7.2 Which boundary an interactive query uses

The regime depends on whether the **query** is clean, and that is **not inferable from the query
text** — there is no answer-side error rate to observe, and spelling, word length and rare-gram
density are weak proxies.

So it is **user-declared, not detected.** Default to **one-sided**, because the common case is a
pasted printed edition or critical text, and expose an explicit "my text is a transcription"
control that selects the two-sided boundary. The chosen regime is reported in the result
envelope and in exports, so a reader can tell which rule produced the evidence.

**Not implemented:** confusion-weighted substitution costs. The confusion matrix is measured and
available (top confusions yod/vav 620, dalet/resh 597, bet/kaf 485, then het/he, he/tav, he/qof),
and weighting these below arbitrary substitutions would sharpen the separation between
transcription noise and genuine textual difference. Designed, not built.

---

## 8. Minimum span — one contract

The research code is inconsistent: the method report states **25** letters, the asymmetric
matcher uses **30**, and both query-shaped scripts use **40**. A reader cannot tell which is
authoritative, so this spec settles it.

**`MIN_SPAN` = 40 normalized letters.**

Rationale: the two implementations actually shaped like this use case — a text used as a query
against the whole corpus — both independently chose 40 after operating at scale. An interactive
surface shows its top results directly to a reader, so precision at the short end matters more
than in a batch pass whose output is filtered downstream. Forty letters is roughly 8–10 Hebrew
words, a defensible floor for calling something a parallel. The lower floors belong to batch
contexts that have downstream filtering.

Two consequences to hold onto:

1. **This is the structural floor on query length.** A query shorter than `MIN_SPAN` cannot be
   extended beyond its own length and can therefore never produce an accepted span. It is *not* a
   word count: two distinct 5-grams need only six letters, and one long word can supply several
   anchors. Express every user-facing guard in normalized letters.
2. **`MIN_SPAN` is query policy, not an artifact input.** Changing it does not invalidate a built
   index, provided normalization, gram width and stride are unchanged. Version it in result
   envelopes and evaluation manifests; do **not** wire it to a rebuild trigger.

---

## 9. Stage-0 hygiene — mandatory, not optional

The false-positive classes are empirically known and **all of them are mechanical**. Skipping
this stage produces impressive-looking results that are artifacts of photography and
cataloguing. Two measurements make the point: of the physically-joined fragment pairs that
appeared to share text, **36 of 36** turned out to be duplicate photography; of short-span
pairs, **6 of 8** were microfilm title sheets.

### 9.1 Pre-index record exclusions

Applied while streaming the corpus, before indexing. Each returns a drop reason, and every count
is reported — never silently skipped.

| Reason | Rule |
|---|---|
| `short` | fewer than **80** Hebrew letters |
| `target_sheet` | at least 4 of the microfilm-card template words, or at least 3 of them with fewer than 400 letters |
| `library_stamp` | fewer than 400 letters and matching the library ownership-stamp pattern |

The stamp class was found late: a single apparent "unit" of 2,618 manuscripts sharing nothing but
a photographed ownership stamp. Both stamp and target-sheet records match each other across
entirely unrelated manuscripts, which is what makes them dangerous rather than merely useless.

### 9.2 Duplicate-photography detection (post-verify)

The subtlest duplicate class is the **same physical page photographed twice** — different image
ids, different system numbers, independently transcribed, extremely high text similarity. These
masquerade as spectacular discoveries.

The tell: **line breaks are a property of the physical page.** Two genuine textual witnesses
never agree on line breaks; two photographs of one page must.

Detector: split both texts into lines, normalize each line, keep lines of at least 10 letters,
require at least 4 such lines on both sides, then walk the two line lists in order, matching each
left line against the next **3** right candidates at normalized Levenshtein of 0.30 or less.
Agreement of **0.60** or more means duplicate photography. Measured **precision 100% (31 of 31),
recall 74%**; the remainder is covered by the two cheaper tiers.

Cheaper tiers, applied first: collapse identical image ids (one photograph catalogued under
several system numbers), and flag different system numbers resolving to one physical shelfmark.

### 9.3 Bounding it on an interactive surface

Pairwise line agreement is quadratic in returned records. On a query path it must be bounded:
apply it only within score-adjacent neighbourhoods of the top-N rendered results, and report both
the cap and the number of suppressed records. Unbounded, this stage becomes the latency budget.

---

## 10. Document frequency, and the interactive budget

### 10.1 What the batch arrangements do, and why it does not transfer

The pair-wise arrangement bands grams by document frequency: drop DF = 1 (cannot form a pair),
drop DF above **100 records**, and cap posting length at **3,000**. The reason is quadratic
candidate volume — formulaic material generates noise pairs without bound.

The measured effect is counter-intuitive and worth stating plainly: **DF-banding *improves*
recall on liturgical text.** Under a bounded per-pair anchor budget, common grams spread anchors
across many false diagonals; banding spends the budget on distinctive grams that concentrate on
the true diagonal. On a liturgical witness set, DF = 50/30 found **415 of 699** witness pairs
against **135** with no DF filter. The intuition "protect formulaic genres from DF filtering" is
exactly backwards.

The cap is an **absolute** record count, so it becomes relatively stricter as the corpus grows.
Candidate volume therefore does not extrapolate linearly from a pilot and must be measured at
scale.

**The cap is also the known recall frontier.** In the asymmetric matcher, `REF_DF_CAP = 128` is a
**raw posting count** cap — not a document-frequency cap — and it drops **all** postings of any
code over the cap. That makes matching non-monotonic in reference-corpus size: appending new
material can silently delete seeds for old, unrelated material. It measurably did — one append
deleted 103 live identifications. The query-shaped script diagnosed this and replaced it with a
**distinct-work** DF cap that keeps every posting of a surviving code, so a formula repeated 100
times inside one work has distinct-work count 1 and always survives.

### 10.1a Two different statistics, easily confused

The batch findings above are stated in terms of **document frequency** -- distinct records
containing a gram. The pair-wise engine computes exactly that, because it deduplicates postings
to the first position per `(gram, record)` before banding.

The corpus-resident artifact of section 5 does **not** store that. It keeps every position, on
purpose, because span extents need them, so its per-code count is an **occurrence count**, not a
document frequency. The two differ by the within-record repetition rate: measured on a
60,000-record slice, 27.76 postings per held code against 25.49 records per held code.

Record DF is not stored but is cheap to derive, because postings within a code are ordered by
`(record, position)` -- distinct records equal the number of positions where the record index
changes, which is one vectorized pass. If a policy ever needs it, it should become a stored
artifact of `4 x (27^5 + 1)` bytes rather than a per-query scan. Until then, do not describe the
stored statistic as document frequency.

### 10.2 What an interactive query needs instead

A single query against a corpus has **no quadratic pair explosion** — cost is linear in postings
admitted — so it does not need a global DF cap. But it does not follow that common grams are
harmless: the DF-banding finding above is about the *anchor budget*, and a query has one of those
too. What a query needs is a **bounded, deterministic posting budget**, and that budget must not
be spent rarest-first:

- The rarest grams in a noisy query are disproportionately the **corrupted** ones. Rarest-first
  spends the whole budget on the least reliable end of the distribution.
- Rarest-first is **non-monotonic in query length**: appending text introduces rarer grams that
  displace anchors the shorter query used, so a longer query can return fewer results.
- **Band reservation can create its own non-monotonicity** — an added suffix can fill a band's
  reserved slot and displace an earlier anchor, and an under-used band can waste budget.

So the policy is specified rather than left to implementation:

| Element | Contract |
|---|---|
| Bands | log-DF bands, **edges read from the artifact manifest** — a frozen artifact property, not a runtime guess |
| Deduplication | query grams deduplicated before allocation |
| Budget unit | **postings**, declared explicitly |
| Selection and overflow order | the stable total order `(band, df, gram_code, first_query_position)` |
| Reservation | explicit per-band reservation with **deterministic borrowing** when a band is under-filled |
| Reporting | policy version, excluded grams and excluded postings, in the result envelope |
| Caps | candidate cap and verification cap, each with a stable overflow order and an explicit `truncated` state |
| Monotonicity | a **pass/fail** criterion — a bounded regression tolerance on held-out positives — not merely a test that detects the problem |

The three allocation policies (no cap, band-allocated, rarest-first) are to be compared **under
identical budgets** on measured recall before one is adopted. This spec fixes the *contract*, not
the winner.

### 10.3 Position stride

Indexing every position is the default. Indexing every second or third position reduces the
artifact proportionally, and with `MIN_SPAN = 40` and a two-distinct-gram requirement the recall
cost should be small — but it is unmeasured, so stride is a measured decision, recorded in the
manifest, not an assumption. Note that DF = 1 singletons are **kept** here, unlike the pair-wise
arrangement: a singleton cannot form a pair, but it is a perfectly good query anchor.

---

## 11. Parameter table

| Stage | Parameter | Value | Provenance |
|---|---|---|---|
| Normalization | alphabet | `U+05D0`–`U+05EA`, finals folded, space-stripped | `normalize.py::norm_stream` |
| | normalizer version | 1 | this spec |
| Gram coding | `K` | 5 letters | `engine_np.py::_gram_codes` |
| | encoding | base-27 positional; code space `27^5 = 14,348,907` | `engine_np.py` (`BASE = 27`) |
| Candidates | `BAND` | 20 letters; cluster = bucket plus or minus 1 | `engine_np.py`, `track1_match.py` |
| | `MIN_ANCHORS` | 2 **distinct** gram codes | section 6.1 |
| Verification | `MARGIN` | 30 letters each side | `track1_match.py`, `motif_query.py`, `work_query.py` |
| | `MIN_SPAN` | **40** normalized letters | section 8 — settles a 25/30/40 conflict |
| | random-alignment floor | density about 0.60 | method report |
| | one-sided boundary | 0.28 below 100 letters, 0.35 at 100 and above | `track1_match.py::accept_density` |
| | two-sided boundary | 0.30 below 100, 0.386 for 100–199, 0.418 at 200 and above | `motif_query.py` and `work_query.py::accept_density`; fitted by `roc_boundary.py` |
| Stage-0 | min record letters | 80 | `stage0.py::MIN_STREAM_LETTERS` |
| | target-sheet rule | 4 or more template words, or 3 or more with fewer than 400 letters | `stage0.py::page_filter` |
| | library-stamp rule | fewer than 400 letters and a stamp-pattern match | `stage0.py::page_filter` |
| | line-agreement threshold | 0.60 or more of lines (10 or more letters, 4 or more lines) matching in order at norm-Lev 0.30 or less, lookahead 3 | `stage0.py::line_agreement` |
| DF — batch, pair-wise | DF drop | above 100 records, absolute | method report |
| | posting cap | 3,000 | method report |
| DF — batch, asymmetric | `REF_DF_CAP` | 128 raw postings per code — **known non-monotonic**, section 10.1 | `track1_match.py` |
| DF — interactive | policy | band-allocated posting budget, section 10.2 | this spec |
| Measured noise | letter CER | 20.1% micro, 16.6% median, p25 8.9%, p90 42% | 209-page alignment against human transcriptions |
| Corpus | records / letters | 948,549 records, 602,598,330 normalized letters, longest record 11,809 | measured 2026-08-20 |

---

## 12. What is **not** established

Stated so no reader mistakes any of it for settled.

1. **Corpus-wide precision has never been measured.** Every published precision figure is either
   from a deliberately enriched pilot or is per-band on a stratified sample. The headline pilot
   numbers (110 of 111, or 99.1%, after hygiene filtering; 1 of 164 actually spurious) come from a
   sample enriched with related material and overstate corpus-wide precision.
2. **Corpus-wide recall is unobtainable in principle.** It would require knowing every location
   carrying a given text. Recall claims must name their oracle and that oracle's coverage.
3. **Confusion-weighted alignment costs** are designed and the matrix is measured; not built.
4. **The matres-light normalization view** is designed; unvalidated on plene/defective pairs.
5. **Judeo-Arabic has no evaluation stratum.** The graded sample was predominantly Hebrew.
   Per-language DF tables and strata are designed, not built.
6. **The pilot-to-corpus candidate-volume curve** must be measured, not extrapolated — the
   absolute DF cap self-tightens with scale, which makes pilot extrapolation pessimistic by an
   unquantified amount.
7. **Interactive latency for arrangement C is unmeasured.** No research script implements it.
8. **Stride's recall cost is unmeasured** (section 10.3).
9. **Post-Stage-0 short-span precision is unmeasured.** The finding that 6 of 8 short-span pairs
   were microfilm title sheets was recorded before the hygiene filter existed -- the method
   report says short spans are unusable "until this filter is in place", and it now is. That is
   not the same as showing those six are excluded, that the other two are safe, or that short
   spans inside otherwise long records are safe. The narrow claim is that Stage-0 removes a major
   short-span junk class; residual precision has not been measured.
10. **The method report's "low-entropy heuristic" for target sheets was never implemented** --
    not in the research `stage0.py`, and not in the port. Only the template-keyword and length
    rules exist. The report describes a filter that has never run.
11. **There is no rarity cliff between 25 and 40 letters.** A corpus-derived deterministic
    stratified sample of 4,155 real windows per length, over a 60,000-record slice, puts the
    median record-DF of a window's grams at 152 / 151 / 151 / 151 for lengths 25 / 30 / 35 / 40,
    and the share of windows whose median gram exceeds 200 records at 36.7% / 36.4% / 35.8% /
    34.9%. The hypothesis that 25 lands on a formulaic band which 40 clears -- suggested by a
    table of ten hand-picked formulas -- is **not supported** against the corpus. Any case for 40
    over 25 must rest on chance-match probability under the length x density rule, not on rarity,
    and must be measured rather than argued.
9. **The transcription corpus self-describes as preliminary.** Segmentation and reading-order
   errors are part of the measured noise; an improved release would move every number here.

---

## 13. Source map

The algorithm was reconstructed from the sources below, all of which live in a gitignored
research tree. **This document is the tracked authority**: where they disagree with it, this
document governs; where it is silent, they are the record.

| Source | What it authoritatively covers |
|---|---|
| method report (`METHOD.md`) | the pair-wise arrangement end to end; the noise measurement; boundary calibration; hygiene rationale; evaluation design |
| `normalize.py` | section 3 normalization and span projection |
| `engine_np.py` | section 4 gram coding; section 6 diagonal accumulation; bit-budget history |
| `track1_match.py` | arrangement A; the one-sided boundary; `REF_DF_CAP` and its defect |
| `work_query.py`, `motif_query.py` | arrangement B; the two-sided boundary in a query context; the distinct-work DF cap; `MIN_SPAN = 40` |
| `stage0.py` | section 9 hygiene rules and thresholds |
| `roc_boundary.py` | the two-sided boundary fit |
| [discovery-sidecar-schema-v1.md](discovery-sidecar-schema-v1.md) | the downstream data contract, not the algorithm |
| [discovery-cert01-protocol.md](discovery-cert01-protocol.md) | the precision-measurement protocol |
