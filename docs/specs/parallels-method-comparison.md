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

## Results, n = 120, tuning split

| method | recall@1 | recall@10 | recall@50 | MRR | p50 | p95 |
|---|---|---|---|---|---|---|
| passage `standard-40` | 0.592 | 0.708 | **0.750** [0.666, 0.819] | 0.639 | 391 ms | 508 ms |
| passage `flat-25` | 0.592 | 0.708 | **0.750** [0.666, 0.819] | 0.639 | 346 ms | 421 ms |
| chunk `c5-exact-f100` | 0.383 | 0.542 | **0.575** [0.486, 0.660] | 0.440 | 22,262 ms | 195,143 ms |
| chunk `c3-exact-f100` | *running* | | | | | |
| chunk `c5-variants-f100` | *running* | | | | | |

The recall@50 intervals do not overlap, so the gap is unlikely to be sampling
noise even at n=120.

### The incumbent's score is its own, not an artifact of a known bug

`build_tantivy_query` emits an uppercase `OR`/`AND`/`NOT` query token bare,
producing an unparseable query, and the composition path drops those chunks
while reporting success (filed P2, `docs/OPEN_ISSUES.md`). That defect fired on
**8 chunks across the entire run** — far too few to account for 0.575 against
0.750. Checked before drawing any conclusion, because a harness that skipped
this check would have credited the loss to the method.

### Where each method wins — by query length

| normalized letters | passage | chunk `c5-exact` |
|---|---|---|
| < 200 (n=13) | **0.54** | 0.31 |
| < 400 (n=34) | **0.76** | 0.35 |
| < 800 (n=24) | **0.71** | 0.54 |
| < 1600 (n=25) | **0.88** | 0.84 |
| ≥ 1600 (n=24) | 0.75 | **0.79** |

The methods **cross**. Token matching strengthens as the query lengthens —
more text means more chances that some five-word window survives intact — and
on the longest queries the incumbent edges ahead. This is the shape the plan
predicted would be the routing signal, appearing in real data. Per-cell n is
13–34, so the crossing point is suggestive, not established.

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
3. **The incumbent's best configuration is unknown.** `c5-exact-f100` is one
   point in its space; `c3` should help precisely where the incumbent is
   weakest (shorter queries mean shorter chunks are easier to match intact).
   Calling a winner before it runs would be the rigged comparison this design
   exists to avoid.
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
