# Discovery Panel — Layout & Disclosure

Validated in sketch 001 against 13 real manuscripts from the deployed
`discovery-v1-33499c5b` asset, in three production themes, EN + HE RTL, at 375/768/full width.

## Design Decisions

**Even two-pane layout (variant D) — SELECTED.** At ≥900px the panel is a 1fr/1fr grid: the page's
identifications on one side, the manuscript picture on the other, carrying **equal** visual weight.
Rejected alternatives: page-first with a collapsed manuscript group (the locked D-09 shape — buries
the primary job); manuscript-first (inverts D-09 more than necessary); a narrow 260px context rail
(subordinates the manuscript picture and collapses to the page-first shape on mobile anyway).

On mobile the two panes stack **page first, then manuscript** — the same reading order as D-09, with
nothing hidden.

**Register: "an amazing feature, but caveat is needed."** Uniform row treatment — confidence is NOT
encoded per tier through styling (that edges into what D-24 prohibits). Feature-grade presence
(emerald-bordered panel, a counted entry chip), with the caveat given a permanent designed slot
between header and body rather than being buried as fine print or shouted as a warning.

**Three disclosure levels**, in this order:
1. **Identifications** — default visible.
2. **"Also shares text with / חולק טקסט גם עם"** — collapsed; holds generic identical-span groups and
   the unevaluated related-pages count. Visually marked as *not* identifications (tinted summary,
   inset left border, italic qualifier).
3. **"Show more possible matches"** — screening bands, review-only and short-passage rows.

**Manuscript pane names the works** (D-13h) rather than giving a bare count — "Rashi on Song of Songs
(5 pages)", not "8 more on 7 pages". Chips: solid border + tinted for works on this folio, dashed +
70% opacity for works only reachable behind the screening toggle. Paging appears above 6 works
(needed: one sampled manuscript has 61 works elsewhere).

**One relation filter** in the panel header, AND-composed with the other controls, empty set = all.
Labels use match-framing only — **"Direct match / Partial match / Shared text"**. The owner explicitly
declined "Citations", so D-21 is unamended and the prohibited display words never appear. (Sketch 001
built a relation **+ tier** filter; the tier half is deleted — see *Two buckets* below.)

### Two buckets, and no confidence scale (owner, 2026-08-01 — supersedes 2026-07-31)

The 2026-07-31 ruling put the findings page's three-level confidence scale on the panel too. A check
against the live asset the next day retired the scale entirely on both surfaces. What the panel gets
instead is **two buckets — main pool / more matches** — split by the rule already in the codebase:

```python
from shared.discovery_band_labels import is_default_eligible
```

Full reasoning, measured composition and bucket sizes are in `findings-page.md`. The parts that bear on
the panel:

- **The panel's tier filter is deleted, not converted.** Yesterday's note said it should become a
  confidence filter; that is now wrong, because there is no confidence axis to filter on. Quality is the
  bucket (a default plus a toggle), kind is the relation filter. The panel ends up with **one filter and
  one toggle** — simpler than either previous plan.
- **The row chip shows the relation, not a confidence level** — "Direct match" / "Partial match" /
  "Shared text", with the frozen band label on hover.
- **`confOf()` and `STRONG_BANDS` must not be copied from sketch 003.** They disagree with
  `is_default_eligible()` and sent the best-measured population (0.926) to the bottom level.
- **Routing is untouched.** Bands still decide bucket membership, and §4 still governs default
  visibility. §2's amendment shrinks to "band labels are tooltip-only" — no new display vocabulary.

⚠ **Does the panel's three-level disclosure survive?** Its middle bucket — *"also shares text with"* —
holds `not_evaluated` / shared-text rows, which are **also** in "more matches" by quality. So levels 2
and 3 are both behind-the-default and differ only by relation kind, which the relation chip now
carries. That argues for collapsing to two, matching the owner's model. **D-13e locks the three-bucket
disclosure**, so this needs a decision at gate 1 — flagged, not decided.

Note also that `not_evaluated` is labelled "Shared text" but **5,604 of its claims carry
`claim_type='direct_witness'`**, so a section built on that band name will contain same-work claims.

## CSS Patterns

```css
/* even panes — the selected layout */
.dpanes { display: block; }                      /* mobile: stack */
@media (min-width: 900px) {
  .dpanes { display: grid; grid-template-columns: 1fr 1fr; }
  .dpanes > div + div { border-inline-start: 1px solid var(--border-light); }  /* RTL-safe */
}

/* a bucket that must not read as an identification */
.disc.notid > summary { background: var(--bg-tertiary); }
.disc.notid .dbody {
  border-inline-start: 3px solid var(--border-medium);
  margin-inline-start: 12px; padding-inline-start: 10px;
}

/* work chip states */
.chip.here  { border-color: var(--primary-600); background: var(--bg-active); font-weight: 700; }
.chip.gated { border-style: dashed; opacity: .7; }

/* disclosure arrow must flip for RTL */
.disc > summary::before { content: '▸'; }
[dir="rtl"] .disc > summary::before { content: '◂'; }
[dir="rtl"] .disc[open] > summary::before { transform: rotate(-90deg); }
```

Use `border-inline-start` / `padding-inline-start` / `margin-inline-start` throughout — never `-left`.
The panel renders in both directions and physical properties break RTL.

## HTML Structures

Row anatomy, in order: verb + work link (+ author) → meta line (band label · coverage · unreviewed
stamp · letter offsets) → optional `↳` granularity sub-line → optional low-coverage note → actions
(evidence, other-manuscripts expansion, vote placeholders pushed right with
`margin-inline-start:auto`).

The meta line's first element is the **relation chip**, carrying the frozen band label as its tooltip —
same markup as the findings page:

```html
<span class="rel" title="Algorithmic match — tier A">Direct match</span>
```

Keep it visually neutral. Colour-coding the chip by relation kind reintroduces per-tier styling through
the back door, which is what D-24 prohibits.

Band labels still come from `shared/discovery_band_labels.py::BAND_LABELS` — the real bilingual strings,
not approximations — but they are now **tooltip-only**. Worth knowing why that matters for layout: Tier A
renders as "Algorithmic match — tier A" / "התאמה אלגוריתמית — דרגה א׳", long enough to change a row's
visual balance. Sketch 001 was built with those strings inline, so the panel's real rows will read
tighter than it shows.

## What to Avoid

- **Per-tier confidence styling.** Rejected by the owner; conflicts with D-24.
- **A bare count for "elsewhere in this manuscript."** D-13h requires names. Four of thirteen sampled
  manuscripts had no work titles available for their elsewhere claims — that is a service-layer gap,
  not a display choice.
- **Any precision percentage, confidence interval, or human-review badge.** D-06 and D-13f.
- **Stored vocabulary keys in markup.** `claim_type` values must never reach rendered HTML; the filter
  uses short codes (`dw`/`qw`/`st`) precisely so a grep-based invariant stays clean.
- **A narrow context rail.** It reduces to the page-first layout on mobile, where ~68% of comparable
  traffic is.

## Carried Findings

- **The reader aid is coupled to the screening gate.** On Moss. V,374 the 5 pages of
  רש"י על שיר השירים that make the folio judgeable are `review_only / low_coverage`, so they sit
  behind the BAND-03 toggle. Turning the toggle on completes the codex picture. Undocumented anywhere.
- **D-13g quantified:** 19 of 121 human-confirmed rows are dropped by the routing filter, all for
  `low_coverage`.
- **Coverage is not stored in the asset** — sketch percentages are computed from real HTR text, which
  is exactly why the rebuild must persist `coverage_ppm`.

## Origin

Sketch 001 (round 2), winner variant D. Source in `sources/001-discovery-panel-architecture/`
(`data.js` is the real extracted asset data).

**Amended twice since.** 2026-07-31: the findings page's confidence scale wins and applies to the panel
too. 2026-08-01: that scale is retired entirely in favour of **two buckets** from the existing
`is_default_eligible()` predicate, and the panel's tier filter is deleted rather than converted. Sketch
001's HTML predates both — read it for *layout*, and take the confidence model from `findings-page.md`.
