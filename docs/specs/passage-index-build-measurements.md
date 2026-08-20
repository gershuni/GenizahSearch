# Passage Index — Build Measurements (Phase 142, part 1)

**Status:** measured. Supersedes the size and cost estimates in the plan.
**Machine:** 24 cores, 67.8 GB RAM, 146 GB free. Dev box, not the web server.
**Slice:** the first 60,000 records of the corpus — 51,226 indexed after
Stage-0, **86,532,732 letters**, **86,327,828 postings**, 3,109,357 distinct
gram codes.

Projection to the full corpus scales the measured per-letter rates by
602,598,330 / 86,532,732 = **x6.96**. Only size-proportional files are scaled;
`gram_offsets.bin` is fixed at `(27^5 + 1) x 8` bytes regardless of corpus size.

---

## 1. Artifact size — projection confirmed

| File | 60K slice | Full corpus |
|---|---|---|
| `postings.bin` | 411.6 MB | **2.80 GB** |
| `gram_offsets.bin` | 109.5 MB | **109.5 MB** (fixed) |
| `streams.bin` | 82.5 MB | 561 MB |
| `records.bin` + `record_ids.bin` | ~3 MB | ~23 MB |
| **total** | 607.6 MB | **≈ 3.5 GB** |

Projected postings: **601,171,415**, against ~599M predicted from the counted
602,598,330 letters. The projection method holds.

The plan estimated 3.7 GB. Measured projection is 3.5 GB, the difference being
Stage-0 exclusions the estimate did not model.

**`gram_offsets.bin` is 109.5 MB whatever the corpus size.** An early version of
the benchmark scaled it with everything else and reported a 21.8 GB artifact —
a number that would have gone straight into the desktop provisioning decision.

## 2. Construction: `spool` is faster, `scatter` needs no scratch

P=4, artifacts **byte-identical in every configuration**:

| `batch_grams` | scatter wall | scatter peak RSS | spool wall | spool peak RSS | spool scratch |
|---|---|---|---|---|---|
| 1,000,000 | 66.3 s | **630 MB** | **48.8 s** | 1.1 GB | 659 MB |
| 4,000,000 | 88.2 s | 820 MB | 50.6 s | 1.2 GB | 659 MB |
| 16,000,000 | 108.4 s | 1.8 GB | 51.0 s | 1.8 GB | 659 MB |

Full-corpus projection: **spool ≈ 6 min, scatter ≈ 8 min**, with scratch of
about **4.6 GB** for spool and none for scatter.

`spool` is the default: ~1.7x faster and flat in the RAM knob. `scatter` is
kept and supported because it needs no scratch disk, which may be the binding
constraint on a user's machine. The choice cannot change what is built.

**Scatter loses on time for a structural reason.** It re-derives every gram
once per partition, so its cost is `P x derive` and it gets *slower* as P
rises — 66 s at P=4 against 97 s at P=8 in an earlier run. The external review
predicted exactly this when it argued that replacing a known sort with a novel
scatter might make the implementation worse rather than better. On wall clock,
it was right.

## 3. The RAM knob was the wrong unit, and it cost 3 GB

First measurements showed peak RSS of **3.0–3.8 GB for both constructions**,
and — the tell — it did **not** fall as partitions rose, even though scatter's
in-RAM output slice fell from 206 MB to 51 MB. So the slice was never the
driver.

The cause was batching by **record count**. At the corpus mean of 1,689
letters per record, a 20,000-record batch is 33.8M grams: 0.81 GB across three
uint64 arrays, roughly doubled again by `argsort` copies. Record length varies
by two orders of magnitude across this corpus, so record count cannot bound
memory.

Batching by **gram count** fixed it: peak RSS 3.8 GB → **630 MB**, and the
knob now means something — a batch costs about 24 bytes per gram plus copies.
Artifacts are byte-identical across a 250x range of batch sizes (200K to 50M
grams), so this is purely an operational dial.

Projected full-corpus peak RSS, tuned:

| Construction | P=8 | P=16 |
|---|---|---|
| scatter | ~760 MB | ~570 MB |
| spool | ~980 MB | ~680 MB |

Both stay under 1 GB, which was the open question for desktop builds.

## 4. Stage-0 exclusion rates, measured

Over the 60,000-record slice: **8,774 excluded (14.6%)** — `short` 7,111,
`target_sheet` 1,059, `library_stamp` 604. Every excluded id is written to
`excluded_records.tsv`; that file is the eligible-record manifest a method
comparison has to share, so it is an output rather than a side effect.

The rate is below the ~24% the research notes report for the full corpus,
which is expected: the corpus is catalog-ordered and 60,000 records is one
region of it. The full-corpus figure must be measured, not carried over.

## 5. What this does not yet establish

- **Query latency.** Nothing here measures retrieval; the acceptance table for
  warm and cold cache p50/p95/p99 is still owed.
- **Full-corpus build.** Everything above is a x6.96 projection from one
  contiguous slice, and the corpus is catalog-ordered, so a different region
  could have different letters-per-record and gram skew.
- **A desktop-class machine.** These numbers are from 24 cores and 68 GB. A
  4-core laptop will be slower; the RAM figures should carry across, since
  they are set by the tunable batch and slice rather than by the machine.
- **DF cap and stride.** Both are implemented and tested but not yet swept for
  their recall cost, which is the Phase 142 decision they exist for.
