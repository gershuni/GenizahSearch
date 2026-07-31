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

## Domain / author / work facets — the `/catalog-browse` cascade (owner, 2026-07-31)

The page carries the same three-facet cascade `/catalog-browse` uses: a collapsible **domain tree**,
then **author** narrowed by domain, then **work** narrowed by domain + author — mirroring
`fjms.get_browse_authors(domain)` and `fjms.get_browse_works(domain, author)`.

**Domain is the domain of the IDENTIFIED WORK, never the manuscript's catalogue domain.** This is the
owner's ruling and it is the right one. The manuscript route was tempting — FJMS `domains` joins on
`sys_id == AlmaId` at **83% coverage** (37,027 of 44,375 findings-bearing manuscripts) with zero new
work — but it is the wrong axis and actively harmful: **Moss. V,374 is catalogued *Court Documents /
Court Records* while carrying a verifiably correct Rashi-on-Esther finding.** Filtering on manuscript
domain would hide exactly the findings that disagree with the catalogue, which are the most valuable
ones. **338 tier-A findings** sit on manuscripts catalogued documentary/legal.

### What the "complete map" actually costs — only one facet needs it

| Facet | Source | Coverage today | Work needed |
|---|---|---|---|
| **Domain** | assign each work a domain from the FJMS vocabulary (**39 parents / 202 leaves**, bilingual) | 0% — `works.genre` exists but is **entirely empty** | **The one-time curation pass**, ~1,088 works carrying shipped claims |
| **Author** | `works.author`, already in the asset | 520 of 1,088 works (48%); only **96 distinct** strings, of which **81%** match FJMS `genizah_persons` (38 exact + 40 containment) | Small — fill the gaps + a 96-row alias map, same pass |
| **Work** | the discovery works themselves (935 distinct titles) | 100% | **None** |

The work facet needing nothing is the key simplification. Bridging discovery titles to FJMS
`genizah_titles` matches only **5%** (55 of 935, +12 containment, 868 unmatched) and would have been
the expensive part — but that bridge is only needed if this page must speak FJMS's work vocabulary.
It doesn't: the works being identified *are* the discovery works. `/catalog-browse` uses FJMS titles
because that is its corpus; here the corpus is the computed identifications.

### Feasibility sample: 93 works assigned, 96% at high confidence

`work-domains.sample.json` assigns all 93 works appearing in this sketch's sample, drawn only from the
FJMS vocabulary. Result of one pass with **no external lookups: 90 of 93 high confidence (96%)**. The
three low-confidence cases are a literary letter collection (Documentary/Letters vs a literary
parent), an Arabic Josippon (the vocabulary has no history leaf), and a kalam-vs-theology judgement.

Extrapolated, the full pass over ~1,088 works is a bounded agent task with roughly **3–4% needing a
web lookup or an owner ruling** — which matches the owner's expectation. It also surfaces
data-quality fixes for free: the asset attributes *Hovot ha-Levavot* to the wrong Bahya.

**Design requirements the pass must honour:**
- Assign at the **canonical work** level, so duplicates don't get assigned twice.
- **Closed vocabulary** — a leaf outside the FJMS tree is a build error, not a new domain. Asserted in
  the sketch's test suite.
- Persist as a **curated, hash-pinned artifact** (the shape `v2_canonical_merges` and the approved-title
  list already use), not hand-edited into the DB — consistent with DATA-04's fail-closed posture.
- **Record per-row confidence and provenance**, so the owner reviews only the uncertain rows.
- **Unassigned must be a visible bucket**, not a silent disappearance. FJMS itself has "Unspecified
  Domain" (19,709 rows), so there is precedent.


## Confidence scale + prominence (owner, 2026-07-31)

The band labels were not understandable, so the surface now shows a **plain confidence scale, first
and prominent** — and per the owner it describes **what kind of claim this is**, not which internal
band it landed in:

| Level | Meaning | Rule | Rows | Share |
|---|---|---|---|---|
| **Strong** | may be the same work | `direct_witness` in a strong band | 131,164 | 78.8% |
| **Medium** | a long citation-type match | `quotes_this_work`, >= 200 matched letters | 3,501 | 2.1% |
| **Weak** | the rest | everything else | 31,872 | 19.1% |

200 letters sits just above D-13c's 150-letter short-passage cutoff, so the two are consistent; carry
it as a gate-1 tunable exactly like D-13c. The precise frozen band label stays on hover and on the
methods page.

**This resolves an honesty problem the band-based draft had.** A band-derived scale left 20,435 rows
(12.3%) that were never assessed, and calling those "weak" would have asserted an assessment nobody
made — forcing an awkward fourth "not assessed" value. Under the relation-based definition "weak"
describes a weak *relation*, which those rows genuinely have. Three levels, honestly.

**⚠ Needs a dated amendment to `discovery-band-labels-v1.md` §2.** Collapsing seven frozen
`(family, band)` display labels into three user-facing levels is a display-contract change. BAND-03 is
unaffected: screening rows remain the "show more" population.

**Novelty is now a prominent switch**, first in the filter bar, worded **"Not in the catalogues we
checked"** with a **(?)** that discloses the checked source list and dates. Note this is close to but
not identical to D-23b's settled wording ("Not found in the finding aids checked"); the checked set
includes bibliography, titles, PGP and FGP as well as catalogues, so the **(?)** carries the
boundedness that D-23b was protecting. **A NOVEL-01 wording amendment is owed** if this phrasing ships.

**All three row units are user-selectable** via a "Show as" control in the result bar, per the owner —
the row unit is a reader choice, not a design pick. D-19's open question is therefore answered as
"all three", with one row per identification as the default.

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
**160 assertions, all pass**, including the facet cascade and the confidence scale:

- the domain filter narrows, and a leaf narrows further than its parent
- the author list is cross-filtered by domain; the work list by domain + author
- **every domain assignment falls inside the FJMS vocabulary tree** (closed vocab)
- unit C (work rows) is domain-filterable too
- the domain facet header states it is the **identified work's** domain

and the original invariants:

- the real totals (65,200 / 44,375 / 1,088) are the ones surfaced
- **before the rebuild, no direct row claims any novelty verdict** — they must all read "not checked"
- `coverage_ppm` / `band_rank` are confirmed absent, so the premise of the disabled filters holds
- the nav entry is gone in the absent state
- novelty is not offered as a sort option
- the tier filter narrows
- no `precision`, no confidence interval, no review badge, no "copy of" / "quotes" / "witness of",
  no stored vocabulary key, no prohibited novelty wording

Positive control: seeding "New discovery — precision 0.9382" into the novelty label produces 162
failures. A second control — an out-of-vocabulary domain plus a facet header mislabelled as the
*manuscript's* domain — is caught by both new assertions.

That second control earned its keep immediately: the header assertion originally tested the whole
rendered page, and **passed while the header was wrong**, because the design-note prose also contains
the phrase "identified work". It now scopes to the facet header. An assertion that can pass for the
wrong reason is worse than none.

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
4. The three low-confidence domain assignments (letter collection · Arabic Josippon · kalam vs
   theology) — and whether "Unspecified Domain" is the right home for works the vocabulary can't place.

## Data provenance

`data.js` is generated by `extract_findings.py` (session scratchpad) from the deployed
`discovery-v1-33499c5b` asset: real totals, real tier facet counts, real novelty state, and real
sample rows for all three units with shelfmarks from `libraries.csv` and titles from `works`. Row
*samples* are bounded (60–86 rows per unit) — the totals are exact, the visible pages are a sample,
and the pager labels itself as such.
