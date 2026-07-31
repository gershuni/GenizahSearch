---
sketch: 001
name: discovery-panel-architecture
question: "Where does manuscript-level coherence live relative to this page's identifications, and how do the three disclosure buckets read?"
winner: "D"
winner_note: "Even panes (B+C synthesis). Relation filter keeps match-framing wording — D-21 NOT amended (owner, 2026-07-31)."
round: 3
tags: [discovery, panel, phase-136, disclosure, filters, rtl, mobile, d-09, d-21]
---

# Sketch 001: Discovery panel architecture

## Design Question

Phase 136's browse-page panel must do two jobs at once: judge *this page's* identifications, and
convey *what this manuscript is*. The owner named the second as primary. **D-09 locks the opposite
ordering** — "On this page" first, "Elsewhere in this manuscript" collapsed beneath.

So: where does manuscript-level coherence belong, and does serving the stated primary job require
amending D-09?

**Round 2** adds a fourth variant (the requested B+C synthesis with even panes), a per-view
**relation-kind and tier filter**, and widens the corpus from 7 to **13 manuscripts** so the tiers and
relation kinds are actually exercised.

## How to View

```
start .planning\sketches\001-discovery-panel-architecture\index.html
```

Opens on **Moss. V,374** — the coherence demonstration case.

| Control | What it does |
|---|---|
| **A / B / C / D** | the four architectures — **D is the synthesis you asked for** |
| **Manuscript** | 13 manuscripts: the 7 standing-regression anchors + 6 chosen for tier/relation variety |
| **Match type · Tier** | the new filters, inside the panel. AND-composed; empty = all (D-16 semantics) |
| **עברית / RTL** | full Hebrew + RTL, every string, real bilingual band labels |
| **Design notes** | why each rule renders as it does, plus the wording caution below |
| **375 / 768 / Full** | **check 375 first** — ~68% of comparable traffic is mobile |
| theme (bottom right) | light · **parchment** · dark, mirrored from production `common.css` |

## Variants

- **A: Page-first** — D-09 exactly as locked. Path of least resistance for NiceGUI (a plain vertical stack).
- **B: Manuscript-first** — a manuscript synthesis promoted *above* the page's rows. **Inverts D-09.**
- **C: Page + narrow rail** — D-09's order kept, context permanently visible but subordinate. Degrades toward A below 900px.
- **D: Even panes ★** — the B+C synthesis. Two **equal** panes at ≥900px: the page's identifications and the manuscript picture carry the same visual weight. The two shared buckets sit full-width beneath both, since they belong to neither. Keeps D-09's left-to-right reading order but never collapses the manuscript group, so the amendment it needs is **narrower than B's**: strike "collapsed", keep the ordering. On mobile it stacks page-then-manuscript — A's order, nothing hidden.

## What to Look For

1. **Moss. V,374 first.** Page 23's Rashi-on-Esther looks arbitrary alone and obviously right once you
   see the codex is Rashi-on-Megillot in standard order. Which variant makes you see that without
   effort? That is the whole case for or against amending D-09.
2. **`variety-c`** carries **all three relation kinds on one page** — this is where the filter earns
   its keep. Try Direct match only, then add Shared text.
3. **`variety-b` (61 works elsewhere) and `variety-d` (59)** — does naming the works survive that
   volume, or become noise? Paging appears above 6 works.
4. **`judeo-arabic` and `commentary`** — multi-register cases must survive intact.
5. **`siddur`** — the problem prayer book; the identical-span groups are out of the identifications.
6. **At 375px**, do the four disclosure levels still read as distinct kinds of claim?

## ⚠ Wording caution: "citations"

You asked for a relation filter so a page can show "even citations" or "only same work". The stored
axis is real and populated — `claim_type` is `direct_witness` (197,177) / `quotes_this_work` (59,243)
/ `shared_text` (11,941); as shipped *display* claims, 130,330 / 7,386 / 11,941.

But **D-21 explicitly prohibits "quotes" in display**, alongside "copy of" and "witness of", on the
owner's own reasoning: *"we are not sure that tier_a is same work and the next are parallels, just
heuristics."* Labelling the filter "Citations" asserts a directional relation (work A cites work B)
that the heuristic does not establish. For the same reason the filter is **not** labelled "Same work".

So the filter ships here with D-21's own match-framing vocabulary:

| Stored `claim_type` | Filter label (EN / HE) |
|---|---|
| `direct_witness` | Direct match / התאמה ישירה |
| `quotes_this_work` | Partial match / התאמה חלקית |
| `shared_text` | Shared text / טקסט משותף |

Calling the middle one "Citations" is a defensible product choice, but it needs a **dated D-21
amendment** — it is not something to slip in at display time. Your call.

## ⚠ Scope note: filters on the panel are new

D-16 specifies tier + novelty + coverage filters for **`/work/{id}`**, not for the panel. The panel's
locked model is the fixed three-bucket disclosure of D-13e. Adding per-view filters to the panel is
**new scope** — either an amendment to PANEL-01/02, or a deliberate decision to reuse the work page's
filter model on the panel. It is a good idea; it just is not currently in any requirement.

Novelty is deliberately absent from the filter: it is uncomputed in the deployed asset (all direct
rows sit at `is_new = 0`), and NOVEL-01/02 are what the phase's rebuild adds.

## ⚠ CORRECTION (round 3) — the first two rounds overstated what the panel shows

The extractor's `visible_by_default` did not match `discovery-band-labels-v1.md` §4. Two errors:

1. **`not_evaluated` was missing from the toggle-gated set.** §4 gates
   `{screening_rb, screening_canon, not_evaluated}` — the first cut only gated the two screening bands.
2. **The `tier_a` gate was ignored entirely.** §4 makes `tier_a` default-visible **only** when
   `band_precision` carries `measurement_status = "measured_pass"` AND `ci_low >= 0.85` (D-18,
   fail-closed). **The deployed asset has both NULL**, so tier-A — 80.7% of shipped display claims —
   is behind the "show more" toggle today.

Corpus-wide, only **2,660 of 166,537 shipped display claims (1.6%)** are default-visible right now;
the rebuild's D-02a payload takes that to **137,109 (82.3%)**. Across the 13 regression manuscripts,
**9 show ZERO default identifications today**:

| | today | post-rebuild |
|---|---|---|
| clean · commentary · high-count · reviewed | 0 · 0 · 0 · **1** | 1 each |
| judeo-arabic | 0 | 3 |
| variety-a/b/d | 0 | 2 each |
| variety-c/e/f | 1 each | 2 each |
| **siddur · shared-text** | **0** | **0 — even after the rebuild** |

Two consequences worth carrying into the plan:

- **Gate 1 *enables* these surfaces rather than improving them.** Before the rebuild the panel is
  near-empty by design. That is a much stronger sequencing argument than "the surfaces need stored
  fields".
- **The siddur and Ms. Heb. 577.4.99=4 have no default identifications even post-rebuild**, because
  every row they carry is propagated `not_evaluated`, which §4 gates permanently. Their entire content
  lives in the middle bucket and behind the toggle — which is the strongest possible validation of
  D-13e's three-level disclosure. Those manuscripts are *why* the middle bucket exists.

The sketch now carries a **post-rebuild view / TODAY (pre-rebuild)** toggle so both states are
inspectable, and the suite asserts the gate in both directions.

## Findings surfaced while building

- **The manuscript-coherence aid and the BAND-03 screening gate are coupled** — undocumented anywhere.
  Verified in the asset: Moss. V,374 has 8 claims, exactly **1 shipped**; the other 7 are
  `review_only / low_coverage`. So the 5 pages of רש"י על שיר השירים that complete the Megillot reading
  sit *behind* the screening toggle, and the D-13h reader aid is partly gated by a control built for a
  different purpose. Flip "Show more possible matches" and the picture completes.
- **D-13g, quantified.** Corpus-wide, **19 of 121** human-confirmed rows are dropped by the routing
  filter, *all* for `low_coverage`. On Moss. V,374 it is P22 `רש"י על איכה` (414 matched letters),
  while P23's `רש"י על אסתר` (2,809) shows — two rows a human confirmed, treated differently.
- **Coverage is not in the asset.** These percentages are computed here from real HTR page text,
  because `_attach_coverage` computes coverage at build time and then discards it. That is precisely
  why the rebuild must persist `coverage_ppm` — the panel cannot sort or filter on a number that
  isn't stored.
- **The band labels in the earlier mockup were approximations.** The real ones (from
  `shared/discovery_band_labels.py::BAND_LABELS`) are longer and change the row's visual balance —
  e.g. tier A renders as "Algorithmic match — tier A" / "התאמה אלגוריתמית — דרגה א׳", not "tier A".

## Corrections applied to the earlier mockup

| Decision | Mockup showed | Here |
|---|---|---|
| **D-13f** | a human-review badge on Moss. V,374 | dropped everywhere; every row reads "unreviewed · algorithmic estimate" |
| **D-13g** | P22's human-confirmed row hidden | shown, with a low-coverage note |
| **D-13h** | "426 more on 426 pages" | works named, from real data |
| **D-13d** | both Rashi granularities filed as generic shared text | collapsed as one identification, narrower title as a `↳` sub-line |
| band labels | shortened approximations | the real bilingual strings |

## Data provenance

`data.js` is generated by `extract_sk002.py` (kept in the session scratchpad) from the deployed
`discovery-v1-33499c5b…` asset — 13 manuscripts, every shelfmark, work title, band, `claim_type`,
matched-letter count, offset range, witness count, related-page count and "elsewhere" work name read
from the asset or `libraries.csv`. Coverage percentages are computed from real `Transcriptions.txt`
page text (the asset does not store coverage). **Nothing in round 2 is illustrative** — the
reconstructed per-work page counts that round 1 needed are gone, replaced by real ones.

The D-13d granularity split uses research's recommended display-time heuristic (normalized title
containment). The threshold-free version is what ships here; the owner-reviewed alias allowlist that
research recommends as its backstop is a gate-1 task.

## Automated checks

`node` render-smoke over 13 manuscripts × 4 variants × 2 languages — **218 assertions, all pass** (13 manuscripts x 4 variants x 2 languages x 2 visibility states):

- no `precision`, no raw precision figure, no confidence interval
- no review badge (EN or HE), no "copy of" / "quotes" / "witness of"
- **no stored `claim_type` key reaches rendered HTML** (the filter uses short codes for exactly this reason)
- no superseded or prohibited novelty wording
- matched-letter coverage labelled as such on direct rows; **no** coverage figure on propagated rows
- filters narrow, and **compose as AND**; empty set = all (D-16)
- the D-13g row is promoted into default-visible; the 5 gated Song-of-Songs pages are present

Verified live by **positive control**: seeding a precision figure, a CI and a stored-key leak produces
241 failures. The green result is meaningful, not vacuous. This is the technique
`136-VALIDATION.md` specifies for Success Criterion 7, prototyped here.
