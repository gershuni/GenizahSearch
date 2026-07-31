---
sketch: 003
name: discovery-findings-page
question: "What is the row unit of the corpus-wide findings page, and what can it honestly show before the rebuild?"
winner: null
tags: [discovery, findings-page, phase-136, d-19, novelty, perf-01, nav, gate-5]
---

# Sketch 003: The corpus-wide discovery findings page

## Design Question

D-19 gives the findings surface its own page and its own nav entry, and explicitly leaves **the row
unit OPEN**, asking for a mockup. This is the surface carrying the owner's rationale — *"a big new
amazing feature… maximum ability to see new findings"* — so it is also where the novelty axis has to
carry its weight.

## How to View

```
start .planning\sketches\003-discovery-findings-page\index.html
```

| Control | What it does |
|---|---|
| **A / B / C** | the three candidate row units, with **real** corpus totals |
| **Nav label** | the three surviving label candidates (see the constraint below) |
| **State** | ready · loading · outage · **sidecar absent / flag off** (watch the nav entry vanish) |
| **pretend rebuild has landed** | switches novelty from what the asset can honestly show today to the intended tri-state |
| **עברית / RTL** · **Design notes** · width · theme | as in sketches 001–002 |

## The three candidate row units — measured

| Unit | Rows | Verdict |
|---|---|---|
| one line per **claim** (page × work) | 166,537 | rejected — the same identification repeats once per folio |
| one line per **identification** (manuscript × work) | **65,200** | CONTEXT.md's recommendation, and the only unit where tier, coverage, novelty and the future vote all attach to exactly the thing on the line |
| one line per **manuscript** | 44,375 | **9,806 carry more than one work**, so a novelty verdict on the row is ambiguous — novel *how*? Unit B's rows carry an inline annotation wherever this bites |
| one line per **work** | 1,088 | pleasantly browsable, but the individual find is hidden and giant works dominate by size |

CONTEXT.md recommended the identification unit without a count; **65,200** is that count, measured
here for the first time.

## ⚠ The finding that matters most: the novelty axis cannot honestly ship yet

Novelty is the reason NOVEL-01/02 were pulled into Phase 136 — it is what makes this page worth
using. But in the deployed asset:

| | rows |
|---|---|
| `propagated`, novelty computed = flagged | 14,003 |
| `propagated`, novelty computed = not flagged | 8,240 |
| **`track1_direct`, `is_new = 0` — meaning UNCHECKED** | **144,294** |

A two-state novelty filter over that data would tell a reader that 144,294 findings are *already
recorded in the finding aids*. That is false — they were never checked. **This is not a nice-to-have
data gap; it is the difference between an honest filter and a false claim on the flagship surface.**

So the sketch shows direct rows as **"not checked"** before the rebuild, and the novelty filter group
is visibly disabled and tagged *needs the rebuild*. Press "pretend rebuild has landed" to see the
intended state. This is also the concrete argument for D-23a's tri-state
(`known` / `not_found` / `indeterminate`, fail-closed) being load-bearing rather than bookkeeping.

## Two more things the page is blocked on

- **`coverage_ppm` and `band_rank` do not exist as columns** (verified by `PRAGMA table_info`). The
  coverage filter is therefore inert — rendered, disabled, tagged. The findings page is *blocked on*
  the rebuild, not merely improved by it.
- **PERF-01, independently confirmed.** D-10a measured 3.41–3.55 s for a representative
  novelty/tier/coverage ordering against a 1.5 s cap. Separately, the deduped identification **count**
  for unit A took **16 s** in this extraction. A visible real total is not free — it needs the
  materialized keys and indexes, or a cached/approximate count with honest wording.

## The nav label is constrained from three directions

1. **"Discoveries" is taken** — `web/pages/discoveries.py` is the pre-existing Community page.
2. **"Browse by Identification" already exists** in the nav for `/catalog-browse`, so a bare
   "Identifications" invites confusion between a catalogue browse and a computed-match list.
3. **D-23b prohibits "new"**, so anything like "New Findings" is out.

Three survivors are switchable in the sketch: **Findings / ממצאים** · **Computed Identifications /
זיהויים מחושבים** (consistent with the panel title) · **Text Matches / התאמות טקסט**.

## Modes, not pages

The roadmap records that Phase 138's `/leads` is a **mode** of this page, not a second
implementation, and Phase 137 adds saved judgments. The sketch therefore ships a mode strip with
**All findings** live and **Screening leads** / **My saved** greyed and tagged with their phase. Designing
it now means 137 and 138 add a tab rather than a page, and one filter/sort/paging implementation
serves all three.

## Other decisions visible in the sketch

- **Gated like `/atlas`** — availability predicate ANDed with the flag, not the flag alone. In the
  `absent` state the correct behaviour is that the **nav entry disappears entirely**, not that the page
  renders empty. Asserted in the test suite.
- **Novelty never sorts.** The sort dropdown deliberately offers only tier / pages / matched text.
  D-15a and D-24 prohibit novelty feeding rank or styling, because absence from a finding aid is not
  evidence a match is correct.
- Default sort is tier-first; filters compose as AND; empty = all.
- Rows stack on mobile below 700px.

## Automated checks

`node` smoke over 3 units × 4 states × 2 languages × 3 nav labels × 2 rebuild states —
**153 assertions, all pass**:

- the real totals (65,200 / 44,375 / 1,088) are the ones surfaced
- **before the rebuild, no direct row claims any novelty verdict** — they must all read "not checked"
- `coverage_ppm` / `band_rank` are confirmed absent, so the premise of the disabled filters holds
- the nav entry is gone in the absent state
- novelty is not offered as a sort option
- the tier filter narrows
- no `precision`, no confidence interval, no review badge, no "copy of" / "quotes" / "witness of",
  no stored vocabulary key, no prohibited novelty wording

Positive control: seeding "New discovery — precision 0.9382" into the novelty label produces 162
failures.

### The guard earned its keep on this sketch

My first draft of the page caveat read *"a match is not proof that a folio is a copy of the work."*
The suite failed it — D-21 prohibits "copy of" on display surfaces flatly, and a negated use still
puts the phrase on the page where a grep-based CI guard would catch it. Reworded to *"a text match is
not by itself proof of identity."* Worth recording: the violation was in hand-written prose, not in
data, which is exactly where these rules get broken.

## Open for the owner

1. **Which row unit?** Recommendation: **A, one line per identification** — 65,200 rows, and the only
   unit where every axis attaches to the row.
2. **Which nav label?**
3. Does the mode strip (leads + saved as future tabs) match the intent for 137/138?

## Data provenance

`data.js` is generated by `extract_findings.py` (session scratchpad) from the deployed
`discovery-v1-33499c5b` asset: real totals, real tier facet counts, real novelty state, and real
sample rows for all three units with shelfmarks from `libraries.csv` and titles from `works`. Row
*samples* are bounded (60–86 rows per unit) — the totals are exact, the visible pages are a sample,
and the pager labels itself as such.
