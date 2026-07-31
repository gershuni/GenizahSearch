---
sketch: 001
name: discovery-panel-architecture
question: "Where does manuscript-level coherence live relative to this page's identifications, and how do the three disclosure buckets read?"
winner: null
tags: [discovery, panel, phase-136, disclosure, rtl, mobile, d-09]
---

# Sketch 001: Discovery panel architecture

## Design Question

Phase 136's browse-page panel has to do two jobs at once: judge *this page's* identifications, and
convey *what this manuscript is*. The owner named the second as the primary job. **D-09 locks the
opposite ordering** — "On this page" first, "Elsewhere in this manuscript" collapsed beneath.

So: where does manuscript-level coherence belong, and does serving the stated primary job require
amending D-09?

Secondary questions the same sketch answers: do the three disclosure levels read as three honestly
different kinds of claim, and does the architecture survive both 1 identification and 427?

## How to View

```
start .planning\sketches\001-discovery-panel-architecture\index.html
```

Opens on **Moss. V,374** — the coherence demonstration case. Controls, top bar:

| Control | What it does |
|---|---|
| **A / B / C** | the three architectures |
| **Manuscript** | all 7 standing-regression manuscripts |
| **עברית / RTL** | full Hebrew + RTL flip, every string |
| **Design notes** | reveals why each rule renders as it does, and flags every illustrative number |
| **375 / 768 / Full** | viewport width — **check 375 first**, ~68% of comparable traffic is mobile |
| theme (bottom right) | light · **parchment** · dark, all three mirrored from production `common.css` |

## Variants

- **A: Page-first** — D-09 exactly as locked. Page rows, then a collapsed "Elsewhere in this
  manuscript". Also the path of least resistance for NiceGUI: a plain vertical stack.
- **B: Manuscript-first** — a "What this manuscript contains" synthesis promoted *above* the page's
  rows, replacing the collapsed group. Serves the stated primary job; **inverts D-09**.
- **C: Page + context rail** — D-09's reading order kept, but manuscript context is permanently
  visible in a side rail instead of collapsed. Honours D-09's intent without its collapse — but the
  rail drops below the rows under 900px, so on a phone C degrades toward A.

## What to Look For

1. **Moss. V,374 is the test.** Page 23's Rashi-on-Esther claim looks arbitrary alone and obviously
   right once you see this is a Rashi-on-Megillot codex in standard order. Which variant makes you
   see that without work? That is the whole argument for or against amending D-09.
2. **Then Ms. EVR II B 25** (426 further identifications across 426 pages, 8 works). Does the
   manuscript picture survive at that volume, or does naming the works become noise? Paging appears
   automatically above 5 works.
3. **T-S Ar. 21.164 and T-S Misc. 12.31.14** — the multi-register cases must survive intact. Several
   works, one page, different passages, all correct. If they look thinned out, the rules are too
   aggressive.
4. **EVR II A 684** — the problem siddur. The identical-span group is out of the identifications and
   in the middle bucket. Does that give you the reading you wanted?
5. **Register check.** You asked for "amazing but caveated". Is the caveat carried honestly without
   the panel feeling apologetic — and does any row's styling imply confidence its tier hasn't earned?
6. **At 375px**, does the three-level disclosure still read as three distinct kinds of claim?

## Findings surfaced while building

- **The manuscript-coherence aid and the BAND-03 screening gate are coupled** — no document
  anticipated this. On Moss. V,374 the 7 pages of רש"י על שיר השירים that complete the Megillot
  reading sit *behind* the screening toggle. Turn "Show more possible matches" on and the codex
  picture completes. So the D-13h reader aid is partly gated by a control designed for a different
  purpose. Visible in all three variants; toggle it to see.
- **D-13h has a data gap.** Four of the seven manuscripts have no captured work titles for their
  "elsewhere" claims, so they render as a bare count — the exact failure mode D-13h forbids. The
  panel cannot name what the query does not return; this is a service-layer requirement, not a
  display one.
- **D-13i is load-bearing on 3 of 7 manuscripts.** Moss. V,374, EVR II A 684 and EVR II B 25 all
  carry catalogue lines that describe the shelfmark rather than the folio. They are labelled, not
  silently juxtaposed.

## Corrections applied to the earlier mockup

The `136-MOCKUP-MULTI.html` data predates three decisions; this sketch renders the corrected
behaviour:

| Decision | Mockup showed | Here |
|---|---|---|
| **D-13f** | a human-review badge on Moss. V,374 | dropped everywhere; every row reads "unreviewed · algorithmic estimate" |
| **D-13g** | P22's human-confirmed row hidden by the routing filter | shown, with a low-coverage note |
| **D-13h** | "426 more on 426 pages" | the works are named |
| **D-13d** | both Rashi granularities filed as generic shared text | collapsed as one identification, narrower title as a `↳` sub-line |

## Data provenance

Every shelfmark, work title, band, coverage figure, matched-letter count, offset range and count is
extracted from `136-MOCKUP-MULTI.html`, i.e. from the deployed `discovery-v1-33499c5b` asset. Two
exceptions, both flagged in the Design notes overlay:

- **T-S Misc. 12.31.14** Rashi row coverage (47%) is *derived* from its sibling row's 961 letters /
  47%, not extracted.
- **EVR II B 25** per-work page counts are *illustrative*. The extracted facts are 426 further
  identifications, 426 pages, 8 works; the split across the eight Nevi'im books is reconstructed for
  layout only.

## Automated checks run on this sketch

`node` render-smoke over 7 manuscripts × 3 variants × 2 languages — **45 assertions, all pass**:
no `precision`, no raw precision figure, no confidence interval, no review badge (EN or HE), no
"copy of" / "quotes" / "witness of", no superseded or prohibited novelty wording; matched-letter
coverage labelled as such on direct rows; no coverage figure on propagated rows.

Verified live by **positive control** — seeding a precision figure, a CI and "copy of" into the row
stamp produces 84 failures. The green result is meaningful, not vacuous. This is the same technique
`136-VALIDATION.md` specifies for Success Criterion 7, prototyped here.
