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
