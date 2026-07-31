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

**Relation + tier filters** in the panel header, AND-composed, empty set = all. Labels use
match-framing only — **"Direct match / Partial match / Shared text"**. The owner explicitly declined
"Citations", so D-21 is unamended and the prohibited display words never appear.

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

Band labels come from `shared/discovery_band_labels.py::BAND_LABELS` — the real bilingual strings, not
approximations. Tier A renders as "Algorithmic match — tier A" / "התאמה אלגוריתמית — דרגה א׳", which is
long enough to change the row's visual balance.

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
