# Parallels Method Comparison — interim results

**Status: INTERIM. Tuning split. Not a pre-registered result and not a basis
for changing any default.** Two incumbent configurations are still running;
the holdout split has never been touched.

**Instrument:** FGP self-retrieval. Query = a human FGP transcription; a hit
counts if the method returns any indexed page of that transcription's
manuscript.

**Ground truth is manuscript-grain, not folio-grain, and that is a deliberate
retreat.** The plan specified page-grain positives. The research CER script
reaches page grain by sys_id **plus fuzzy content matching** (rapidfuzz
partial_ratio ≥ 65) — using it here would let a fuzzy content matcher decide
what a passage matcher is supposed to find. A trustworthy identifier-based
page mapping does not exist either: only 18,362 of 45,034 FGP rows carry a
folio label, and `fgp_service` deliberately refuses positional mapping for
"structurally unalignable" manuscripts. Positives therefore come from the pure
identifier join (shelfmark → sys_id, resolved at FGP indexing time). The
measurement answers **"did it find the right manuscript"**, not "the right
folio".

**Population:** 19,090 queries from 41,671 Digital Edition rows. Excluded and
counted: 13,181 per-sys cap (one query per manuscript, so large manuscripts
cannot dominate), 5,750 sys_id not in the index, 3,650 below 120 normalized
letters. The 3,342 `Digital Translation` rows are excluded at the query —
they are translations, not source-language transcriptions, and would have
measured cross-language retrieval while claiming to measure noise robustness.

---

## CORRECTION (2026-08-20): the earlier tables compared different corpora

Everything below the next heading was measured with the two methods searching
**different document sets** — the Tantivy corpus holds 948,549 page records,
the passage index holds 702,466, because Stage-0 excludes 246,083 short pages,
microfilm target sheets and library ownership stamps. The incumbent could
return records the passage engine structurally cannot. That measured document
sets, not methods.

Re-measured with the incumbent restricted to the same 702,466 records
(`-elig`), same 120 tuning queries:

| method | recall@1 | recall@10 | recall@50 | MRR | p50 |
|---|---|---|---|---|---|
| passage `standard-40` | **0.592** | **0.708** | **0.750** [0.666, 0.819] | **0.639** | **391 ms** |
| chunk `c3-exact-f100-elig` | 0.550 | 0.675 | 0.725 [0.639, 0.797] | 0.589 | ~20,500 ms |

**The incumbent got BETTER under equal eligibility, not worse** — recall@1
0.467 → 0.550, MRR 0.533 → 0.589, recall@50 0.708 → 0.725. I had assumed the
unequal comparison flattered it; the opposite was true. Removing junk records
(target sheets, stamps, near-empty pages) from its ranked list pushes correct
answers *up*. The passage engine was already immune, having never indexed them.

**On accuracy the two methods are now indistinguishable at this sample size.**
Recall@50 0.750 against 0.725 with intervals [0.666, 0.819] and [0.639, 0.797]
— almost entirely overlapping. Recall@1 favours passage by 4 points, well
inside noise at n=120. **No accuracy claim is supportable from this data.**

The difference that survives is **speed**: roughly 50× (391 ms against 14.6–20.5
seconds). Caveat on the incumbent's figure: this run overlapped a second heavy
process, so 20.5 s is inflated; its clean earlier reading was 14.6 s. Either
way the ratio is decisive and the direction is not in doubt.

By query length, both on the shared record set:

| normalized letters | passage | chunk `c3-elig` |
|---|---|---|
| < 200 (n=13) | 0.54 | **0.62** |
| < 400 (n=34) | **0.76** | 0.65 |
| < 800 (n=24) | 0.71 | **0.75** |
| < 1600 (n=25) | **0.88** | 0.76 |
| ≥ 1600 (n=24) | 0.75 | **0.83** |

The crossing persists and still favours the incumbent at both extremes, but
per-cell n is 13–34 and the cells alternate — this is noise-shaped, and should
not be read as a routing rule.

### What this changes

The honest current position is: **comparable accuracy, ~50× faster, on this
instrument at n=120.** Speed is a real and sufficient product argument on its
own. An accuracy claim needs the larger pre-registered run, and the grading
deck is needed before anything can be said about precision at all.

---

## SUPERSEDED — original results, unequal document sets

*Kept for the record. The incumbent here searched 246,083 records the passage
engine could not.*

## Results, n = 120, tuning split

| method | recall@1 | recall@10 | recall@50 | MRR | p50 | p95 |
|---|---|---|---|---|---|---|
| passage `standard-40` | 0.592 | 0.708 | **0.750** [0.666, 0.819] | 0.639 | 391 ms | 508 ms |
| passage `flat-25` | 0.592 | 0.708 | **0.750** [0.666, 0.819] | 0.639 | 346 ms | 421 ms |
| chunk `c5-exact-f100` | 0.383 | 0.542 | **0.575** [0.486, 0.660] | 0.440 | 22,262 ms | 195,143 ms |
| chunk `c3-exact-f100` | 0.467 | 0.658 | **0.708** [0.622, 0.782] | 0.533 | 14,565 ms | — |
| chunk `c5-variants-f100` | *running* | | | | | |

**The incumbent's chunk size matters more than the method gap, and sweeping it
changed the conclusion.** At `chunk_size=5` the incumbent scores 0.575 and its
interval does not overlap passage's. At `chunk_size=3` it scores **0.708
[0.622, 0.782]**, and the intervals now overlap heavily — passage's lower bound
(0.666) sits below the incumbent's point estimate. **On recall@50 there is no
longer a demonstrated difference at n=120.**

Reporting `c5` alone would have claimed a decisive win that the evidence does
not support. This is precisely why both methods are swept, and it is worth
recording as the clearest vindication of that decision so far.

Where passage does still separate is in RANKING rather than retrieval:
recall@1 0.592 against 0.467, MRR 0.639 against 0.533. It puts the right
manuscript higher, on the same queries, at 37x the speed (391 ms against
14,565 ms).

### The incumbent's score is its own, not an artifact of a known bug

`build_tantivy_query` emits an uppercase `OR`/`AND`/`NOT` query token bare,
producing an unparseable query, and the composition path drops those chunks
while reporting success (filed P2, `docs/OPEN_ISSUES.md`). That defect fired on
**8 chunks across the entire run** — far too few to account for 0.575 against
0.750. Checked before drawing any conclusion, because a harness that skipped
this check would have credited the loss to the method.

### Where each method wins — by query length

| normalized letters | passage | chunk `c5-exact` | chunk `c3-exact` |
|---|---|---|---|
| < 200 (n=13) | 0.54 | 0.31 | **0.62** |
| < 400 (n=34) | **0.76** | 0.35 | 0.62 |
| < 800 (n=24) | **0.71** | 0.54 | **0.71** |
| < 1600 (n=25) | **0.88** | 0.84 | 0.76 |
| ≥ 1600 (n=24) | 0.75 | 0.79 | **0.83** |

The methods **cross**, but not in the simple way `c5` alone suggested. Against
the better-configured incumbent, passage wins the middle of the range
(400–1600 letters) while `c3` wins at **both extremes**. Any story of the form
"passage wins short queries, token matching wins long ones" is contradicted by
its own strongest configuration.

Per-cell n is 13–34. These cells are too small to support a routing rule, and
the crossing pattern should be treated as a hypothesis for a properly powered
run, not as a finding.

### The span floor did not bind here, and this instrument cannot test it

`flat-25` and `standard-40` are identical to three decimals on every metric.
That is not the answer to "is 25 too noisy": these queries carry a 120-letter
minimum, so accepted spans are long and a 40-letter floor never binds. The
floor question lives in short queries, and this query set structurally cannot
reach them. It needs its own instrument.

---

## What this does not establish

1. **Not a pre-registered result.** Tuning split, by design, so a swept winner
   cannot be reported as a confirmed one. The holdout split is untouched and
   the ledger enforces one scoring per (method, config).
2. **n = 120.** Intervals span roughly ±0.08. Adequate to see a 0.175 gap,
   nowhere near adequate for a 3-point non-inferiority decision.
3. **The incumbent's best configuration is still unknown**, and it has already
   moved once: `c3` beat `c5` by 13 points of recall@50 and erased the gap.
   `c5-variants` has not finished. Until its space is properly explored, no
   winner can be called — the first sweep step already overturned the first
   conclusion.
4. **Manuscript grain, not folio grain** (above).
5. **FGP transcriptions are not the user task.** They are long, clean editions
   of a manuscript's own text. A user pasting a printed edition to find
   witnesses is a different distribution, and the held-out replay set of real
   `/parallels` queries remains owed.
6. **FGP content carries a catalogue preamble** — shelfmark, library code,
   edition — which is query noise both methods receive equally. Fair, but it
   is not clean transcription text.

## An operational finding for the experiment design

At p50 22 s and p95 195 s per query, the incumbent makes large-n comparison
expensive: a 500-query holdout run costs roughly three hours **per
configuration**. The pre-registered run needs either parallel processes, a
query-length cap applied to both methods, or a smaller pre-declared n with the
resulting interval width accepted in advance. This is a design constraint to
settle before the holdout is spent, since it can only be spent once.

---

## Precision, blinded relation grading (100 of 240 cards)

Deck `cc45f5d67e03b203`, pre-registration `13c2dba4b1c5cfd6`: 60 tune-split
queries, **top 3 per method**, pooled and deduplicated to 240 cards, method
label and rank stripped, order a deterministic shuffle by card id. Owner graded
100 on the eight-term relation vocabulary.

**CORRECTED (2026-08-21 re-examination).** The graded 100 are the FIRST 100
cards of the presented deck, an unbroken prefix — the grader worked one pass
and stopped. The earlier claim of a "spread" (41 of the first 100 positions)
was computed against the key file's bookkeeping order, not the shuffled
presentation order, and was wrong. Mitigation, verified: presentation order is
a deterministic shuffle by card id (a hash), uncorrelated with query, method,
and grade, so a prefix of it approximates a random draw of 100-of-240; the
prefix itself contains all three buckets at near-population rates.

**"strict" includes `paraphrase`.** The scorer defines
`REAL_STRICT = {same_text, paraphrase}`; the label below previously read
"strict (`same_text`)" and misdescribed it. With `same_text` alone: passage
40/42 = 0.952, chunk 40/77 = 0.519 — the non-overlap is unchanged.

| | n | strict (`same_text`+`paraphrase`) | useful (any real relation) |
|---|---|---|---|
| passage `standard-40` | 42 | **0.976** [0.877, 0.996] | 1.000 [0.916, 1.000] |
| chunk `c3-exact-f100` | 77 | **0.532** [0.422, 0.640] | 0.974 [0.910, 0.993] |
| both methods agreed | 19 | 1.000 [0.832, 1.000] | 1.000 [0.832, 1.000] |

Method rows include the 19 cards both methods returned, so they sum to
77 + 42 − 19 = 100.

**The strict intervals do not overlap.** On `useful` the methods are tied at
roughly 1.0 — neither returns junk, and only 2 of 100 cards were graded
`unrelated`, both from the incumbent. The entire difference is in what KIND of
match is returned.

### The structural result: the exclusive yields are equal in size and unequal in value

| bucket | n | `same_text` | `shared_formula` | `canonical` | `paraphrase` | `unrelated` |
|---|---|---|---|---|---|---|
| both methods | 19 | 19 | — | — | — | — |
| chunk only | 58 | **21** | 22 | 12 | 1 | 2 |
| passage only | 23 | **21** | 1 | — | 1 | — |

Read the `same_text` column. Each method found **21 same-text pairs the other
missed**, on identical queries at identical depth. The union holds 61 distinct
same-text pairs and each method recovers 40 of them — **65.6% each**.

### Depth probe: the symmetry above is a top-3 artifact, and it breaks in passage's favour

"The other method missed it" in the table means "not in its top 3", because
`per_method_k = 3`. The 2026-08-21 re-examination ran every exclusive
same-text card's query through the OTHER method's **full, uncapped** ranked
list (retrievers rebuilt exactly as the harness builds them):

| exclusive same-text cards | truly ABSENT from the other's full list | present, past top-3 |
|---|---|---|
| passage-only (n=21), probed in chunk | **15 (71%)** — zero score anywhere | 6 (5 at ranks 4–7, 1 at 126) |
| chunk-only (n=21), probed in passage | **8 (38%)** | 13 (mostly ranks 3–13) |

At the looser manuscript grain the asymmetry holds (10/21 vs. fewer absent).
One formulaic query (`fgp:13345`) dominates the chunk-only-present cases.

So the corrected reading:

- **The incumbent's misses are mostly structural** — it assigns zero score to
  71% of passage's exclusive finds; no deeper k recovers them.
- **Passage's misses are mostly demotions** — 62% of the incumbent's exclusive
  finds ARE in passage's list, just past an arbitrary cutoff. A flip to
  passage-only with a deeper displayed list loses ~8 of 61 same-text pairs
  (13%), not 21 of 61 (34%) as the top-3 table alone implies; staying on the
  incumbent forgoes ~15 of 61 (25%) that it cannot retrieve at all.
- **Their exclusive yields differ completely in composition.** Passage's 23
  exclusive cards are 91% same-text. The incumbent's 58 exclusive cards are 36%
  same-text and 59% liturgical formula or scriptural quotation — real matches,
  correctly found, but not what a parallels search is for.

This still argues for a **union or side-by-side presentation** — but the flip
is *less* costly than the top-3 cross-tab suggested, not more. None of this
was visible to any recall instrument: pooled recall is 100% for the union by
construction, so only the *split* of the union is measurable — which is
exactly what the deck measures.

### The depth asymmetry favours passage and must be stated

`per_method_k = 3`, while passage's median result-set size on this query set is
**2** and the incumbent's is far larger. So passage's 0.976 is measured over
approximately its entire output, whereas the incumbent's 0.532 is measured over
its best three of many. The incumbent's precision across its full result list is
therefore **lower** than 0.532, and the reader-burden gap is wider than this
table shows. The asymmetry is in passage's favour and is not corrected here.

### The blinding confound was tested, and it is content-driven

Span length correlates with method, which could have let the grader infer
provenance from the highlight. Checked *within the incumbent alone*, where
method is constant:

| incumbent cards | n | strict |
|---|---|---|
| shared span < 60 letters | 38 | 0.16 |
| shared span ≥ 60 letters | 39 | 0.90 |

Median span by relation, incumbent: `same_text` 130, `canonical` 24,
`shared_formula` 16, `unrelated` 3 letters. Span length tracks **what the match
is**, not who found it. The correlation is a property of formulaic matches
being short, not a bias channel — which defuses the concern rather than
confirming it.

### What this precision result does not establish

1. **Tuning split, exploratory** (`prereg.json: exploratory = true`). The
   holdout is untouched.
2. **n = 100 of 240.** The remaining 140 cards would move both estimates and
   both intervals; the non-overlap is wide but not immune to more data.
   (An earlier phrasing here claimed they "cannot change the non-overlap" --
   that was an overclaim, corrected on external review.)
3. **Depth 3, not depth 50.** Precision deeper in the incumbent's list is
   unmeasured (and, by the asymmetry above, likely worse).
4. **FGP queries.** Same distribution caveat as the recall instrument.
5. **One grader, unreplicated.** No second-grader agreement measurement.
6. **A share of the cards are self-retrieval** (the returned record belongs to
   the query's own FGP source manuscript): 18 of 42 passage cards, 17 of 77
   chunk cards. Splitting them out (external review, 2026-08-21) STRENGTHENS
   the finding rather than weakening it -- on non-source manuscripts alone,
   the externally valid subset, strict precision is **passage 23/24 = 0.958
   vs chunk 27/60 = 0.450**; on source-manuscript cards it is 18/18 = 1.000
   vs 14/17 = 0.824. The holdout deck reports the two populations separately
   by design.

---

## Instrument 2 — witness attestation, an oracle neither method can see

Caveat 5 above ("FGP transcriptions are not the user task") is the gap this
instrument closes. FGP self-retrieval asks a method to recognize the page its
own query was transcribed from. This asks the actual workload: **paste a
passage of a known work, find Genizah fragments that carry it** — with ground
truth from a scholarly witness list built by neither method.

`scripts/build_witness_query_set.py`. The oracle holds 15,072 (work,
manuscript) attestations; 12,713 at `confidence = high`, covering 3,323 works.
Restricting to works with at least 4 attested manuscripts that the passage
index actually holds leaves **614 funnel-passing works → 2,258 queries** of
~900 readable characters each, sampled at deterministic positions spread
across each work's body. Every exclusion is counted and the writer fails on
count divergence (2,456 expected = 2,258 written + 198 below the 400-letter
floor).

**Population corrections (2026-08-21 re-examination).** The 198-slice drop
took ALL four slices from 41 works, so the file holds queries for **573
distinct works**, not 614 — the "573" in the channel analysis below is that
number. Separately, **94 of the 2,258 queries (4.2%) are byte-identical
duplicates** of a same-work sibling: the slicer's short-body fallback can land
several of a work's windows on one start position. Same-work only, zero
cross-work collisions; it mildly inflates the effective n and the builder now
drops duplicates (counted as `duplicate_slice`). The v1 file with duplicates is
what the recorded tune runs used.

Two deliberate design points:

- **The `##` provenance blocks are stripped before the slice is taken.** They
  carry the edition's source-manuscript statement, so leaving them in would
  paste the answer into the query. Verified: 0 of 2,258 query texts contain a
  header block, a Latin run, or a shelfmark-shaped string.
- **Positives are every page record of every attested manuscript**, and the
  metric is *any-positive@k* — did the method surface at least one manuscript
  scholars had already attested. `evaluate()` already ranks by the first
  positive, so this needed no metric change.

### Two deflations, both large, both identical across methods

1. **A witness is a fragment.** It attests that the manuscript carries the
   work, not the passage we pasted. A random passage of a long work is often
   absent from any one fragment.
2. **The oracle is incomplete.** A retrieved page that is not on the list may
   be an unattested witness, not a false positive. Hence recall only — this is
   a yardstick, never acceptance evidence.

Both apply to identical queries and identical positive sets, so the
comparison is fair ON THE OBSERVED ORACLE. That is weaker than "the true
between-method difference is unaffected" (an earlier overclaim, corrected on
external review): witnesses the oracle does not list can be retrieved at
different rates by the two methods, and nothing here measures that. The
instrument compares the methods against the attested slice only.

### Passage-side result, 300 tune queries

| metric | passage `standard-40` |
|---|---|
| any-positive@1 | 0.550 [0.493, 0.605] |
| any-positive@10 | 0.770 [0.719, 0.814] |
| any-positive@50 | **0.787** [0.737, 0.829] |
| any-positive@200 | 0.800 [0.751, 0.841] |
| MRR | 0.636 |
| p50 / p95 | 222 ms / 257 ms |

By witness count: 0.80 at 11+ witnesses (n=97), 0.78 at 4–10 (n=203) — the
deflation is milder than expected and barely varies with fragment count.

The incumbent on the same 300 queries is **running**; at ~20 s per query it
costs about 100 minutes and its row is owed before any comparative claim.

### Is this cross-witness retrieval, or self-retrieval in disguise?

A fair challenge: 440 of the 573 queried works are attested through a single
oracle channel -- the edition's own statement of the manuscript it was
transcribed from (channel names are masked; see the discovery specs). If a
query
retrieves exactly the manuscript its edition was transcribed from, the task is
clean-query/noisy-target recognition — realistic (paste a printed edition, find
the manuscript) but not evidence about finding an *unrelated* witness.

`scripts/measure_witness_coverage.py` discriminates by counting **distinct**
attested manuscripts in the top 50; two or more cannot all be one edition's
single source.

| distinct attested manuscripts in top 50 | queries |
|---|---|
| 0 | 64 |
| 1 | 124 |
| 2 | 65 |
| 3 | 25 |
| 4 | 7 |
| 5+ | 15 |

**112 of 300 queries (37.3%) surface two or more distinct attested
manuscripts — 47.5% of the queries that hit at all.** So the instrument is
unambiguously cross-witness for about half its hits and ambiguous for the rest.
The multi-witness subset is available as a conservative stratum.

### What Instrument 2 does not cover, and where the missing stratum was found

**Judeo-Arabic is absent from this oracle.** All 15,072 rows come from the
Hebrew reference corpus; 0 from the Judeo-Arabic collection. Instrument 2
cannot supply that stratum.

It did not have to. The plan called building a Judeo-Arabic stratum "real work
inside Phase 144"; in fact **37.4% of the FGP query set is already
Judeo-Arabic** (7,142 of 19,090 queries) and had simply never been labelled.
Corrected for classifier error the true share is about 42.6%.

---

## The language stratum, and how it was validated

`scripts/detect_query_language.py`. Judeo-Arabic is written in Hebrew script,
so script detection is useless; the discriminating signal is the Arabic
definite article written as a Hebrew-letter prefix. Geresh-marked letters were
measured too and rejected (F1 0.776), because Hebrew uses the geresh for
abbreviation and numerals.

Two corrections were needed before the classifier could be believed, and both
are worth recording because the first version of each looked fine.

**Precision here is not a property of the classifier.** The first validation
sample was 80% Judeo-Arabic by construction, which reported precision 0.980, a
number that describes the sample's balance rather than the rule. No query set
has that balance. What is asserted instead is **recall and the false-positive
rate**, which are prior-free, with precision derived at a stated prior.

**The rule fired on Hebrew words that merely begin with the same two letters**
(the divine name, demonstratives, several common names). That was the entire
false-positive channel. Excluding the head of that distribution cut the
false-positive rate roughly tenfold *at higher recall*:

| rule | threshold | recall | false-positive rate |
|---|---|---|---|
| bare prefix | 0.02674 | 0.886 | 0.0744 |
| **prefix minus stoplist** | **0.019** | **0.868** | **0.0076** |

Measured on 28,644 Judeo-Arabic and 6,946 Hebrew 600-character windows: the
query length in use, not document length, because the rate is noisier in short
passages and a document-level validation would describe a different input.

Implied precision by true Judeo-Arabic share: 0.991 at 50%, 0.987 at 40%,
0.966 at 20%, 0.927 at 10%, 0.857 at 5%. Robust across the plausible range,
which the first version was not.

### Out-of-sample control

The witness query set is drawn entirely from the Hebrew reference corpus, so a
correct classifier must label it Judeo-Arabic at approximately its
false-positive rate and no more. Measured: **21 of 2,258 = 0.9%**, against a
fitted false-positive rate of **0.76%**. The control lands on the prediction.

### Resulting strata

| query set | n | Hebrew | Judeo-Arabic | unlabelled |
|---|---|---|---|---|
| FGP | 19,090 | 62.3% | **37.4%** (7,142) | 0.3% |
| witness | 2,258 | 99.1% | 0.9% | 0% |

The Judeo-Arabic protected class is therefore available on the FGP instrument
at ample n, and unavailable on the witness instrument. Both methods must be
re-run with `language` in the stratum set before the flip decision; every run
recorded above predates the label.


---

## Re-examination (2026-08-21): what was independently verified, and what changed

Seven independent verifiers re-derived every reported number from raw
artifacts (deck key + verdicts, the oracle, the index, the labelled corpora)
without reading this document's claims. Confirmed exactly: the precision
table, the cross-tab, the confound test, zero leakage in all 2,258 witness
texts (in fact zero non-Hebrew characters at all), the accounting funnel, the
harness's equal-eligibility semantics (filter applied before any cap, ranks
recomputed; verified against 6 live queries), that the harness calls the real
shipping `search_composition_logic`, the ledger's write-once enforcement
(mutation-killed), and the language-classifier counts (all 21,348 labelled
rows re-derived from text, zero mismatches).

Corrections from the same pass are folded in above: the graded-prefix fact,
the `strict` definition, the 573/614 population split, the duplicate slices,
and the depth-probe asymmetry.

### Holdout split repaired before first use

The tune/holdout split hashed the bare query id, so the witness instrument's
four sibling slices per work — identical positive sets — straddled the
boundary for **510 of 573 works**. The holdout had never been scored, so
nothing is contaminated; the split is now group-aware (query id up to the
first `#`), pinned by `tests/test_retrieval_eval_split.py`, including the
property that made the fix safe: ids without `#` (the whole FGP set) keep
byte-identical assignments, so recorded FGP results remain valid. Witness
tune/holdout membership changes; the recorded witness tune numbers refer to
the old membership and remain exploratory either way.

Also measured: per-work clustering in the reported 300-query witness sample is
negligible in practice — the evenly-spaced sampler landed on 298 distinct
works (mean 1.007 queries/work), so the Wilson intervals are effectively
honest for that run (width 0.0924 → 0.0929 under a per-work correction).

### Language-classifier caveats sharpened

- **The seed-stability check validates only the FPR.** `--validate` never
  samples the Judeo-Arabic side, so recall is structurally seed-invariant; the
  stability re-run at a different seed confirms the false-positive rate, not
  recall.
- **The stoplist's cost is concentrated**: about a third of the measured
  recall shortfall traces to אלא and אלי alone, which are also genuine
  Judeo-Arabic function words. The measured operating point (0.868 / 0.0076)
  already absorbs this; do not "improve" the stoplist without re-measuring.
- **Two live label errors found at the known blind spots**: a purely Hebrew
  litany labelled `ja` because אלהיכם is missing from the stoplist and the
  text sits just above the 30-word floor; and a genuine Judeo-Arabic passage
  held at `he` because its Arabic vocabulary happens to carry almost no
  definite articles. Both are the expected error modes at the measured rates,
  not new phenomena.


---

## The tradeoff sweep (2026-08-21): one live knob, two inert ones

Owner request: an internal recall/precision map, and whether parameters can
trade one for the other. `scripts/sweep_passage_tradeoff.py`, tune split,
full n=300 samples on both instruments; precision measured on the 100
already-graded deck pairs (they are labels, in both directions -- the 58
incumbent-only pairs included) plus the count of NEW unlabeled returns.

| density_scale | FGP recall@50 | witness recall@50 | strict on labeled | result size p50 / p90 |
|---|---|---|---|---|
| 1.00 (`standard-40`) | 0.703 | 0.800 | 0.98 | 2 / 47 |
| 1.15 | 0.757 | 0.857 | 1.00 | 2 / 90 |
| **1.30 (`wide-40`)** | **0.793** | **0.893** | **1.00** | **4 / 139** |
| 1.45 | 0.830 | 0.893 | 0.97 | 8 / 235 |

Latency flat (~0.36 s) across the axis -- the caps hold. Findings:

- **`density_scale` is the knob.** 1.30 sits at the knee: +9.0/+9.3
  recall@50 points over the shipping default with no measured precision cost
  on labeled pairs. 1.45 saturates the witness instrument, doubles the
  reader burden again, and shows the first strict dip.
- **`min_span` 25/40/60/80 is inert on these instruments** (broad grid,
  n=60 per cell, every row identical): the queries are long and accepted
  spans are almost never short (short-span share ~1%). Its effect lives in
  short-quote queries, which still have no instrument.
- **`min_anchors` 3 is inert** -- true matches carry tens of anchors.
- **The two-sided regime is a shifted duplicate of the density axis** and
  earns no preset.
- **Tightening below 1.00 only loses:** precision is already at ceiling, so
  a tighter boundary drops real finds (42 -> 31 labeled pairs returned).

The unmeasured remainder: the wide point returns NEW rows the graded deck
never labeled. `deck_delta_v1` (46 cards, manuscript grain, blinded,
`--delta-only`) contains exactly those -- everything `wide-40` adds over
`standard-40` on the 60 tune deck queries and nothing else. Its grading
decides whether wide becomes the default or an option.


---

## The delta deck: what `wide-40` actually adds (46 cards, all graded)

`deck_delta_v1`, cards_hash `188d3d1c73327f44`: every card that
`passage:wide-40` returns and `passage:standard-40` does not, on the 60 tune
deck queries, at manuscript grain, blinded and hash-shuffled. Owner graded
**46 of 46**. Scored by the hardened scorer (deck-id + key-hash verified,
`--min-graded 40` satisfied).

| | n | strict (`same_text`+`paraphrase`) | useful (any real relation) |
|---|---|---|---|
| the delta | 46 | **0.543** [0.372, 0.745] query-clustered | **1.000** [1.000, 1.000] |

Relation mix: **24 `same_text`, 1 `paraphrase`, 14 `canonical`, 7
`shared_formula`, 0 `unrelated`, 0 `junk`.** By source status: 1.000 on
source-manuscript cards, **0.500 [0.355, 0.645]** on non-source ones — the
externally valid population.

So the answer to "discoveries or noise" is neither, in a measured ratio:
**about half of what widening adds is genuinely the same text, the other half
is scriptural quotation or liturgical formula, and none of it is junk.** For
comparison, the incumbent's exclusive yield on the earlier deck was 59%
formula/quotation; widening the passage engine is cleaner than that, and
dirtier than the passage engine's own default output.

### Widening loses nothing — the top-3 "losses" are pure display displacement

At top-3 per query the two configurations look like they trade: 84 shared
cells, 46 wide-only, and 25 that only STANDARD shows. Tracing all 180 of
standard's top-3 finds through wide's **full** ranked list:

| where standard's top-3 finds sit in wide's list | n |
|---|---|
| still in wide's top 3 | 99 |
| wide rank 4–10 | 14 |
| wide rank 11–50 | 8 |
| wide rank 51+ | 1 |
| **absent from wide entirely** | **0** |

Nothing is *absent* — widening only re-ranks. But the follow-on claim I drew
from this was **wrong, and an external review (Codex, 2026-08-21) caught it**:
I wrote that "with a display list of ~10 or deeper, `wide-40` dominates
`standard-40` outright". The trace refutes that on its own numbers — **9 of
standard's top-3 finds land at wide rank 11 or worse** (8 at 11–50, 1 at 51+),
so a 10-deep display does NOT show them. What the trace supports is the
narrower statement: *not absent from wide's full list*. Consequences, corrected:

- Widening re-ranks rather than discards, and most displaced finds (14 of 23)
  land at ranks 4–10 where a deeper list recovers them. Nine do not.
- A 10-deep display is mostly hypothetical anyway: measured on the same 300
  tune queries, **242 of 300 standard queries (81%) and 203 of 300 wide
  queries (68%) return fewer than 10 distinct manuscripts**, with medians of
  1 and 3. "Precision@10" is therefore not the right quantity — most slots do
  not exist. The product-relevant measure is per-query YIELD (how many
  same-text manuscripts a reader is actually shown), which is what the next
  instrument measures.
- Per-result precision does fall, because the additions are 46%
  quotation/formula: blended top-3 strict ≈ **0.82** for wide against ≈ 0.98
  for standard (the standard figure is carried over from the tune deck, a
  different deck — an estimate, not a measurement of this population).
- Result-set size at top-3 grows 109 → 130 cells (+19%); at depth the
  medians are 4 against 2.

### Bearing on the default, and why F1 is not the summary to use

Recall improves at depth (+9.0/+9.3 points recall@50) and nothing drops out of
the full list, so `wide-40` is the stronger retriever. What is NOT yet
measurable is what a reader sees: precision beyond rank 3 is ungraded for
every method.

**F1 is the wrong summary here and will not be reported.** Our recall
denominator is "does the query's own source manuscript surface" (an oracle
over one target per query); our precision denominator is "what relation does
each shown manuscript bear to the query" (graded cards). Those are different
gold universes and their harmonic mean has no product interpretation. On
external review the recommended single number is **expected non-source strict
manuscript yield in the actual display**: distinct `same_text`/`paraphrase`
manuscripts shown per query, counting empty slots as zero — reported beside
sys-grain known-manuscript recall@10 and the formula/canonical burden.

Also flagged for correction: the union and 5+5 recall rows computed on
2026-08-21 are **record-grain** calculations, while the product and the next
deck are manuscript-grain. They must be recomputed at sys grain before being
used for any decision.


---

## The display-policy deck (`deck_display_v1`), built 2026-08-21

Built per the external-review design, over the same 300 FGP tune queries whose
recall numbers are recorded above. Three candidate display views, manuscript
grain, depth 10:

| view | policy | N_display (manuscript cards shown across the panel) | queries showing anything |
|---|---|---|---|
| `S` | `standard-40`, top 10 | 975 | 251 / 300 |
| `W` | `wide-40`, top 10 | 1,456 | 276 / 300 |
| `C5` | `wide-40` + incumbent, 5 each alternating | 2,312 | 298 / 300 |

Stratified by visible rank band x aligned-span band (12 cells), census at 8
per cell plus 24 extra draws spread over the deep bands: **121 view-selections
-> 120 unique cards** to grade, of which 13 are source-manuscript returns and
one card is shown by two views (graded once, counted for both).

Every invariant reconciles: per-query display counts sum exactly to each
view's `N_display`, and each view's stratum `N_h` values sum to the same
number. A blind check of the HTML confirms no view, rank, stratum, method
name, or `is_source` value reaches the grader.

### The all-strict control

Scoring a synthetic deck in which every card is graded `same_text` must return
precision exactly 1.000 and yield exactly `N_display / 300`. It does, on all
three views: 1.000 [1.000, 1.000], and 7.707 / 3.250 / 4.853 — matching
2312/300, 975/300, 1456/300 to the digit. The estimator recovers a known
truth before it is trusted with an unknown one.

### What the numbers will and will not support

Interval widths at 120 cards are roughly +/-0.08 (C5) to +/-0.16 (S) on the
non-source columns, because 120 sampled cards stand in for ~4,700 displayed
ones and a single card in the largest stratum carries about 3% of its view's
estimate. That resolution separates large differences between views and will
NOT separate small ones. If the three views land close together, "no clear
winner" is the finding, not a reason to grade more.

### Three defects the instrument found in itself before use

Each was caught by running the real pipeline and reading its output, not by
unit tests on fixtures:

1. **Impossible intervals.** A precision CI upper bound of 1.486 — the
   query-clustered bootstrap held `N_display` fixed while resampling the
   numerator. Fixed by giving the manifest per-query display counts.
2. **An invalid variance estimator.** The prescribed query-clustered
   bootstrap does not compose with stratification that cuts across queries:
   weights calibrated per stratum do not decompose per query, and 6-8 of
   every ~12 graded queries had a weighted numerator exceeding their own
   display count. Replaced with a stratified bootstrap, which bounds
   precision at 1 by construction.
3. **A point estimate outside its own interval.** The non-source column
   filtered the sampling POOL while the bootstrap still drew `n_h` items,
   re-inflating the numerator to the unfiltered maximum (0.868 with a
   [1.000, 1.000] interval). Replaced with domain estimation — membership in
   the indicator, every unit kept in the pool.


## Display-deck result (all 120 cards graded, 2026-08-22)

Grades: 74 `same_text`, 24 `canonical`, 17 `shared_formula`, 4 `unrelated`,
1 `paraphrase`. Scored with the stratified IPW estimator; intervals are
95% stratified-bootstrap, seed 20260821.

**Headline endpoint — non-source strict manuscript yield per query** (how
many genuinely same-text manuscripts, other than the query's own source, a
reader is actually shown; empty slots count zero):

| view | yield, non-source | precision, non-source | precision, overall |
|---|---|---|---|
| `S` standard, top 10 | 2.051 [1.437, 2.669] | 0.631 [0.442, 0.821] | 0.852 [0.700, 0.972] |
| **`W` wide, top 10** | **3.106 [2.365, 3.778]** | **0.640 [0.487, 0.778]** | 0.748 [0.641, 0.844] |
| `C5` wide + incumbent, 5+5 | 2.805 [1.923, 3.687] | 0.364 [0.249, 0.478] | 0.441 [0.346, 0.540] |

### Widening is free at the display level

`W` and `S` have the SAME non-source precision — 0.640 against 0.631, well
inside each other's intervals — while `W` yields 51% more same-text
manuscripts per query. The earlier top-3 delta deck suggested widening cost
precision (its additions graded 0.543 strict); across a 10-deep display that
cost disappears, because the weaker additions sit below the good ones rather
than replacing them. `W` dominates `S`: same precision, more yield.

### The combined view is dominated on both axes, and this reverses the recall reading

`C5` was the candidate the union recall numbers favoured (union recall@10
0.797 against 0.733 for wide alone). Graded, it is the worst option:

- **precision 0.364 non-source, against 0.640 for `W`** — intervals do not
  overlap, so this is a real difference, not noise;
- **yield 2.805, BELOW `W`'s 3.106** — it does not even buy the extra finds
  it costs precision for.

The mechanism is visible in the rank-band mix. `C5` gives the passage engine
only 5 slots instead of 10, so it discards wide's ranks 6–10 — which grade
59% `same_text` — and fills those slots from the incumbent, whose
contributions grade 33% `canonical` + 38% `shared_formula` + 4% `unrelated`
at ranks 4–10, i.e. 75% non-target.

**All four `unrelated` cards in the whole deck are in `C5`, three of them in
its top 3. Neither passage-only view produced a single one.** Traced to
their source: every one is ABSENT from wide's list entirely and sits at
incumbent rank 2, 3, 2 and 5 — they are incumbent contributions, and the
interleave promotes them into the most visible slots.

Why this contradicts the recall reading: recall@10 asks "did the query's own
manuscript surface", and pooling two methods can only help that. Yield asks
"how many genuinely same-text manuscripts is the reader shown", and pooling
spends half the display budget on a weaker ranker. The two questions have
different answers, and the second is the product question.

### Recommendation

**`wide-40` alone, displayed 10 deep.** Same precision as the tighter
setting, half again the yield, no `unrelated` results at all, and ~40x
faster than any view containing the incumbent. The combined view is not
worth building: it is less precise and yields less than wide alone.

Caveats carried: tune split, exploratory, n=120 cards standing in for ~4,700
displayed ones (interval widths +/-0.08 to +/-0.16); FGP queries only; and
none of this covers short-quotation queries, where `min_span` matters and no
instrument exists.


---

## Owner correction (2026-08-22): list length is not a cost

The owner's constraint, stated directly: *"a researcher has no problem
receiving 50 or even a hundred, two hundred, three hundred fragments if in the
end there is something that fits. It is like search results."*

That invalidates the premise under two earlier conclusions, and both have to
be reopened rather than defended.

### 1. "The combined view is dominated" was conditional on slot scarcity

`C5` lost because it gave the passage engine 5 slots instead of 10 and
discarded its ranks 6-10. With no slot budget the displacement does not
happen, and the right combined design is **append, not interleave**: the
passage engine's full list, then whatever the incumbent found that it did
not, below. Nothing is displaced, so the incumbent's low precision sits in
the tail rather than the head.

| | passage wide alone | wide, then incumbent appended | gain |
|---|---|---|---|
| FGP recall@50 | 0.793 | **0.863** | +7.0 pts (21 queries) |
| FGP recall@200 | 0.820 | **0.870** | +5.0 pts (15 queries) |
| witness recall@50 | 0.893 | **0.920** | +2.7 pts |
| witness recall@200 | 0.897 | **0.937** | +4.0 pts |

The remaining cost is latency, and it is real: the passage engine answers in
~0.3 s, the incumbent in 13-20 s. A page that waits for both forfeits the
speed advantage entirely. Progressive loading (passage results immediately,
incumbent extras appended when they arrive) is the design that keeps both.

### 2. `wide-40` at density_scale 1.3 was chosen at a knee that no longer binds

1.3 was picked because burden doubled at 1.45. Re-measured upward on the same
300 FGP tune queries, with burden no longer a cost:

| density_scale | recall@50 | recall@200 | median manuscripts | p90 | p50 latency |
|---|---|---|---|---|---|
| 1.30 (`wide-40`) | 0.793 | 0.820 | 3 | 123 | 363 ms |
| 1.45 | 0.830 | 0.863 | 6 | 209 | 350 ms |
| **1.60** | **0.843** | **0.893** | 8 | 301 | 509 ms |
| 1.80 | 0.857 | 0.903 | 22 | 458 | 570 ms |
| 2.00 | 0.853 | 0.910 | **102** | 691 | 575 ms |

Two things worth reading carefully:

- **recall@50 peaks at 1.80 and FALLS at 2.00** (0.857 -> 0.853). Even when
  the display is unbounded, ranking still binds: at 2.00 the median query
  returns 102 manuscripts and the true target gets pushed past rank 50 by
  its own noise. "Length is free" does not make ordering free.
- **Returns diminish sharply.** 1.60 -> 1.80 buys +1.0 point of recall@200
  and nearly triples the median list (8 -> 22); 1.80 -> 2.00 buys +0.7 and
  quintuples it (22 -> 102).

Latency stays sub-second throughout, so it is not the binding constraint.

**What is NOT measured:** precision above 1.30. Every graded card in
`deck_display_v1` comes from the 1.30 setting. The owner's tolerance is for
long lists *that end in something useful* — so whether 1.60's additions are
real finds or formulas is exactly the open question, and a ~40-card delta
deck (what 1.60 adds over 1.30) would settle it at low cost.


---

## The FGP validity problem, measured — and the instrument that replaces it

Owner, 2026-08-22: FGP transcriptions are frequently partial, and carry
transcriber summaries and skips. They are not the real search case, which is
a continuous passage of a composition pasted in.

Quantified against each transcription's own HTR page (4,000 rows, normalized
letters):

| transcription / page length | share |
|---|---|
| under 0.5 (materially truncated) | 2.1% |
| 0.5 – 0.8 | 6.5% |
| 0.8 – 1.25 (comparable) | 57.6% |
| **over 1.25 (longer than the page)** | **33.7%** |

The dominant deviation is not truncation but ADDITION — a third of
transcriptions carry more text than the page holds, which is what editorial
notes and apparatus look like. (The ratio alone cannot separate "transcriber
added material" from "HTR missed material"; both inflate it.) About 42%
deviate materially in one direction or the other.

**What this does and does not touch.** Recall was measured on TWO instruments
and they agree (wide > standard > incumbent, every depth, both): the witness
instrument draws continuous ~900-character slices from complete works, so it
is already the ecologically valid one. But **every graded deck to date —
`deck_v5`, `deck_delta_v1`, `deck_display_v1`, `deck_delta_wider_v1` — used
FGP queries.** All precision evidence therefore rests on the unrepresentative
distribution.

### `scripts/build_reference_query_set.py`

Queries drawn as contiguous readable slices from COMPLETE works in the two
reference corpora, no witness-oracle restriction (a precision deck needs no
oracle — it shows query text beside a returned manuscript and asks the
relation, so the draw can span the whole corpus rather than its attested
corner). 896 queries from 1,454 works long enough to contain a passage; the
`##` metadata blocks are stripped before slicing, since they name the source
manuscript. `positives` is emitted EMPTY on purpose, so misuse as a recall
instrument fails loudly instead of scoring zero.

`deck_ref_wider_v1`: 44 cards, 29 queries, every card exclusive to
`wider-40`, drawn from both corpora (27 / 17). Median shared span 62 letters
and 45.5% short, against 56% short in the FGP-based version of the same
question — the reference-source queries produce a somewhat cleaner delta
before any grading, which is itself a small sign that the query distribution
was doing work.

This deck supersedes `deck_delta_wider_v1` (FGP-based, built the same day,
ungraded) for the `wide-40` vs `wider-40` decision.


## `deck_ref_wider_v1` graded (44 of 44): widening to 1.60 buys quotations

Strict (same text or paraphrase) on what `wider-40` adds over `wide-40`:
**0.318 [0.159, 0.500]** query-clustered. Useful 1.000 — zero `unrelated`,
zero `junk`, so nothing it adds is garbage.

| grade | n |
|---|---|
| `canonical` (scriptural / rabbinic quotation) | **28** |
| `same_text` | 12 |
| `paraphrase` | 2 |
| `shared_formula` | 2 |

Per query: 1.52 added manuscripts, of which **0.48 genuine and 0.97
canonical**. Compared with the earlier step (1.00 → 1.30, graded 0.543 strict
with 30% canonical), returns diminish sharply and the canonical share roughly
doubles to 64%.

### The canonical flood is a property of the QUERY, not the threshold

Splitting the same deck by which reference corpus the query came from:

| query corpus | strict | canonical | share genuine |
|---|---|---|---|
| commentary/code-heavy corpus | 5 | 21 | **18.5%** (5 of 27) |
| document/composition corpus | 9 | 7 | **53%** (9 of 17) |

Paste a passage of a commentary and it *contains* the verse it comments on, so
widening finds Genizah fragments of that verse — correctly, and uselessly for
this feature. Paste an original composition and more than half the additions
are real textual parallels. Cell sizes are 17 and 27, so this is a strong
signal at weak resolution, not a calibrated rate.

### Decision

**`wide-40` (1.30) stays the default.** At 1.60 two thirds of what you gain is
scripture you did not ask for, and the gain concentrates in exactly the query
type — commentaries — where it is least wanted.

Two things follow rather than close:

1. **`wider-40` is worth keeping as a user-selectable "widen search"**, since
   nothing it returns is junk and for composition-type queries the additions
   are 53% genuine.
2. **A canonical-overlap demotion would unlock 1.60 and beyond.** The
   discovery pipeline already has the precedent — a row whose best span is
   covered by the union of Bible-match spans on the same page is demoted — and
   the reference corpora contain the canonical works needed to detect it. That
   is a feature, not a parameter, and it is the single change that would make
   the wider settings obviously correct rather than a trade.


---

## Canonical filtering: the existing feature does not solve this, and text overlap is the wrong signal

Owner, 2026-08-23: canonical-citation filtering already exists in the regular
search as an option — the user supplies canonical text or imports it from an
external library. Confirmed present: `filter_text_dialog.py` +
`sefaria_utils.py` fetch and cache canonical texts, and
`SearchEngine.search_composition_logic` routes a chunk to `filtered` when the
chunk's regex matches anywhere inside `filter_text`
(`shared/search_engine.py:3037`). `shared/passage_parallels.py` honours the
same parameter (per-record, on the query-side span). So the wiring exists on
both paths.

**It does not work for this problem.** Tested against the 44 graded cards,
with the full canonical strata of the reference corpora as `filter_text`
(15,163,534 letters from 307 works):

| | result |
|---|---|
| canonical cards filtered out | **0 of 28** |
| genuine finds wrongly filtered | 1 of 12 |

Exact-substring matching of a ~62-letter span against a clean canonical corpus
essentially never fires: the composition's own citation differs from the
canonical text by spelling, abbreviation, or partial quotation. The mechanism
works for the incumbent because its needles are 3-5 word chunks with a
mark-tolerant regex, not 60-letter spans.

**Loosening it makes things worse, not better.** Two weaker variants, both
measured on the same graded cards:

- *Any short sub-window present in the canon*: catches 22 of 28 canonical —
  but also 7 of 12 genuine. Real parallels quote scripture too.
- *Coverage of the span by canonical windows* (the discovery pipeline's guard
  shape): the distributions do not separate at all — median coverage 0.49 for
  `canonical` against 0.53 for `same_text`. At every threshold the genuine
  loss tracks the canonical gain.

### What the signal actually is

Reading the catalogue titles of the returned manuscripts makes it obvious.
The cards graded `canonical` are overwhelmingly manuscripts that ARE canonical
works — Talmud Bavli, Mishnah, Torah, Bible, Halakhot Gedolot, Rif, Mishneh
Torah. The cards graded `same_text` are liturgy, piyyut, Haggadah, Arukh,
derashot — the kind of thing the query itself is.

**"Canonical" is a property of the returned MANUSCRIPT, not of the text
overlap.** The query quoted a canonical work and the engine correctly found a
copy of it.

A title-based rule on data already shipped (`libraries.csv`, resolved for
42/42 of these sys_ids):

| | result |
|---|---|
| canonical demoted | **18 of 26 (69%)** |
| genuine lost | **2 of 14 (14%)** |

The two genuine losses are a Prophets manuscript and a Talmud manuscript that
really did carry the searched text — irreducible for a title rule, which is
why this must DEMOTE rather than delete, and be user-optional. The eight
canonical it misses are untitled or idiosyncratically titled fragments
(`<empty>`, `קובץ`, a Judeo-Arabic legal work).

This is a far better discriminator than any text-overlap test, it needs no
canonical corpus at query time, and it costs a title lookup. It is also NOT
what the existing feature does, so it is new work rather than wiring.

---

## Two live GUI case studies (2026-08-23): the web default moves to 1.8

First real-user sessions on the deployed-locally GUI, both graded row by row
by the owner. Both scored at PRODUCTION caps (verify_cap 3,000).

### Case 1 — Dror Yikra (2 stanzas, 111 normalized letters)

The GUI (standard-40) returned 2 results plus the truncation notice. Verified
offline: removing the verify cap entirely (all 26,164 candidates verified)
still yields exactly 2 — **the notice was a false alarm**; the strongest-first
verify order had already kept everything real. The incumbent (12.4s vs 0.27s)
returned the same two manuscripts plus one low-scored third — and that third
is precisely what passage finds at density_scale 1.6+ (density 0.405). Corpus
truth: 3 recognizable pages, two clean, one garbled. The famous-piyyut prior
("should have many witnesses") measures the Genizah, not this HTR corpus:
tiny liturgical fragments fall under Stage-0's 80-letter floor or arrive
HTR-mangled.

### Case 2 — Yom Shabbaton, three-method comparison with owner grading

The owner ran chunk-4, cross-paragraph, and passage on the same vocalized
query and graded every row (V/X). Nikkud confirmed harmless: the fully
vocalized query matched unvocalized HTR 13-for-13. Manuscript-level, against
the union of his V marks (28 manuscripts):

| method | recall | precision |
|---|---|---|
| chunk-4 | 27/28 | 27/55 (49%) |
| cross-paragraph | 20/28 | 20/20 (100%) |
| passage @ 1.0 (GUI default) | 13/28 | 13/13 (100%) |
| passage @ 1.3 | 18/28 | — |
| passage @ 1.6 | 24/28 | — |
| **passage @ 1.8** | **26/28** | ≥26/35 (74%+) |
| passage @ 2.0 | 26/28 | 26/265 — **the cliff, seen live** |

At 1.8 passage sits one manuscript behind the incumbent's recall at ~74%
precision against its 49%, in 0.6s against minutes — and returned 9
same-series candidates (T-S NS 165.172, T-S NS 27.346, EVR II A 53/934/...)
the incumbent never surfaced, i.e. candidate NOVEL finds pending the owner's
eyes. Still missed at 1.8: MS heb. e.54/44 and Ms. EVR II A 24.

The 2.0 row is the corpus-wide sweep's recall@50 peak-then-fall reproduced on
a single live query: 265 manuscripts returned, in-pool count unchanged.

### Also observed live in the same session

- **The incumbent's duplicate rows are the V0.7+V0.8 double-indexing.**
  Ms. EVR II A 200/1 appeared twice at score 75 with transcription-variant
  text of the same page; the Tantivy index ingests both versions, the
  passage corpus reads V0.8 only (13 distinct manuscripts in 14 rows).
  Pre-existing, incumbent-side; filed separately from this track.
- **The duplicate-photography demotion fired correctly in production**: NLI
  Box K.15/K.16, identical text and score — one physical page in two boxes,
  demoted to Filtered, reachable.

### Decision (owner, 2026-08-23)

The web surface searches at **widest-40 (density_scale 1.8)** — wired in
`web/passage_assets.py::get_passage_searcher`, while `DEFAULT_POLICY` stays
standard-40 so evaluation tooling keeps choosing explicitly. A user-facing
control for the knob is planned (Phase 146A). The truncation-notice wording
question was decided the same day (owner, 2026-08-23): the notice fired
honestly but over-warned -- Case 1 shows a firing with zero results lost --
so it is now neutral-informative and self-describing ("Passage search checked
the N best-evidenced candidates of M", info-level, real numbers from the
QueryReport), and the API's `passage_results_truncated` warning carries the
same `verified`/`candidates` counts.

### The 1.8 run graded (2026-08-23, 48 rows): 7 novel witnesses, precision 94%

The owner graded the widest-40 GUI run row by row: **34 of 36 manuscripts V**
(X: T-S NS 165.172, T-S NS 25.271). Against the two-incumbent pool of 28:
27/28 found -- equal to chunk-4's recall -- **plus 7 verified manuscripts
neither incumbent mode ever surfaced**: Ms. EVR II A 53, 934, 1134, 1501,
2377, 2402, and T-S AS 101.2. These are the method's first owner-verified
NOVEL discoveries. Updated universe (35 verified witnesses): passage 34/35,
chunk-4 27/35, cross-paragraph 20/35. Only MS heb. e.54/44 still escapes.

**The four Filtered rows audited** (the owner asked why the last rows were
filtered): all four are duplicate-photography demotions, all four CORRECT,
and they caught two distinct duplication species in one query. EVR II A
200/1's P9/P10 (agreement 0.96/0.93) are the same pages under TWO IE numbers
-- NLI digitized the manuscript twice, each digitization with its own HTR.
Box K.15/K.16 (agreement 1.00, byte-identical) are the same physical page
under three NLI records; the best-ranked copy stayed in main, both others
demoted. The owner's V marks on all four validate demote-not-delete: genuine
content, redundant rows, still reachable and gradable. This NLI image-level
duplication is a different species from the incumbent's V0.7/V0.8
double-transcription duplicates; the passage corpus is exposed only to the
former, and the hygiene pass suppressed 4 redundant rows of 48.

Offline reproduction of the GUI run is exact: main=44 + filtered=4 = the
export's 48 rows, same scores, same reasons.

### The Birkat Hamazon case (2026-08-23): width was not the limit -- the cap was

The owner pasted ~3,900 characters of Birkat Hamazon liturgy, got 198
manuscripts ("highly relevant"), and asked for an option to widen further.
Measured before building: at his existing setting (widest-40, 1.8) the engine
had already found **497 manuscripts in 0.32s** -- the 200-group display cap
hid 299 of them. At 2.0: 586 found, still <=200 shown. Widening changes what
is FOUND, not what is SHOWN; for many-witness liturgical texts the binding
constraint is the display cap, which makes **paging (Phase 146A) the
highest-value item on that list**, promoted by measurement rather than
preference.

A "Maximal" width step (max-40, density_scale 2.0 -- the validation ceiling)
was added anyway at the owner's request, with the measured caveats in its
preset docstring: 2.0 is past the recall@50 peak, and its value arrives in
full only once paging exists.

### Paging + uncapped exports (2026-08-23): the cap had no job left

Implementing the owner's two 146A pulls (paging; export everything) collapsed
into ONE change once the seams were read: the parallels page ALREADY ships a
working pager (`load_more_main`, 50 groups per "Load more" click, strongest
first -- the chunk path has used it all along), and the export layer already
bounds itself at 5,000 rows independently. The engine's 200-group cap on the
page path was therefore hiding found manuscripts from BOTH surfaces while
protecting neither. The page now requests `render_cap=0` (uncapped); the API
path keeps the searcher default, because its envelope contract is 200 groups
and rendering rows the service would discard is pure waste.

Verified on the owner's own Birkat Hamazon query through the exact page path:
**594 rows / 443 manuscripts + 68 duplicate-photography demotions, zero blank
rows, truncated=False, 0.83s.** "Rendered == kept" holds over the full set --
the uncapped test asserts every returned row carries its highlight text,
because a blank row past the old cap boundary would be finding #1
reintroduced at scale. Worst-case bound: verify_cap (3,000 records) keeps an
uncapped render under ~4s, inside every timeout guarding the path.

One vacuous-pin catch worth recording: the page-side gate originally asserted
`'render_cap=0' in src` -- and stayed GREEN under mutation because an
explanatory COMMENT contains the same substring. The pin now extracts the
actual `get_passage_searcher(...)` call and asserts on that; the repo has
seen the comment-vs-gate failure in both directions now.
