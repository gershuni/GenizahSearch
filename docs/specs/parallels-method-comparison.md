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

The graded subset is spread across the whole deck rather than its head: 41 of
the first 100 deck positions were graded, against 41.7 expected under a uniform
draw. Treated as approximately representative of the 240; the residual risk is
that a deliberately skipped card is not a random omission.

| | n | strict (`same_text`) | useful (any real relation) |
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

That is the finding, and it is not the one the precision headline suggests:

- **The methods are complementary, not ranked.** A default flip to passage-only
  would lose 21 of 61 same-text pairs (34%) in this sample — a recall
  regression on precisely the class the plan names as most costly and hardest
  to detect. Symmetrically, staying on the incumbent forgoes the other 21.
- **Their exclusive yields differ completely in composition.** Passage's 23
  exclusive cards are 91% same-text. The incumbent's 58 exclusive cards are 36%
  same-text and 59% liturgical formula or scriptural quotation — real matches,
  correctly found, but not what a parallels search is for.

This argues for a **union or side-by-side presentation** over a flip, and it
was not visible in any recall instrument: pooled recall is 100% for the union
by construction, so only the *split* of the union is measurable — which is
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
2. **n = 100 of 240.** The remaining 140 cards narrow the intervals; they
   cannot change the non-overlap, which is already wide.
3. **Depth 3, not depth 50.** Precision deeper in the incumbent's list is
   unmeasured (and, by the asymmetry above, likely worse).
4. **FGP queries.** Same distribution caveat as the recall instrument.
5. **One grader, unreplicated.** No second-grader agreement measurement.

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
index actually holds leaves **614 works → 2,258 queries** of ~900 readable
characters each, sampled at deterministic positions spread across each work's
body. Every exclusion is counted and the writer fails on count divergence
(2,456 expected = 2,258 written + 198 below the 400-letter floor).

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
between-method difference is unaffected; only the absolute level is
uninterpretable.

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

### What Instrument 2 still does not cover

**Judeo-Arabic.** The oracle's 15,072 rows are entirely from the Hebrew
reference corpus — 0 rows from the Judeo-Arabic collection. The plan names
language as a protected class for the flip decision, and this instrument cannot
supply that stratum. It remains owed, and no flip can be licensed without it.
