---
name: sketch-findings-genizahsearch
description: Validated design decisions, CSS patterns, and visual direction from GenizahSearch sketch experiments — the Phase 136 discovery panel, its browse-page embedding, and the evidence-highlighting algorithm. Auto-load when building discovery read surfaces or any NiceGUI browse-page UI.
---

<context>
## Project: GenizahSearch

GenizahSearch already has a settled visual language, so these sketches did not invent an aesthetic —
they mirror it. Production is "Deep Academic Green" (`--primary-600: #059669`, header gradient
`#065f46 → #059669`) with three live themes driven by `[data-theme]`: light, **parchment**
(`#fffbf5`, amber-brown — the manuscript reading theme) and dark. WCAG AA is deliberate in the real
CSS, which carries explicit contrast-ratio comments and overrides several Quasar defaults for failing
contrast. `sources/themes/*.css` are exact mirrors of `web/static/common.css`.

Build target is NiceGUI / Quasar. The app uses `q-expansion-item` at only two sites, both driven
awkwardly via `run_javascript`, so nested disclosure is **not** an established pattern here — a plain
vertical stack is the path of least resistance.

Comparable surfaces run **~68% mobile** (measured on `/atlas`), so design phone-first.

Sketch session wrapped: 2026-07-31.
</context>

<design_direction>
## Overall Direction

The register for discovery surfaces, in the owner's words, is **"an amazing feature, but caveat is
needed"** — explicitly neither a quiet scholarly apparatus nor an unqualified product feature, and
explicitly **not** confidence encoded through per-tier styling. So: uniform row treatment,
feature-grade presence, and the caveat given a permanent designed slot rather than buried as fine
print or shouted as a warning.

The primary reader job on a manuscript page is **understanding the whole manuscript**, not judging one
row — manuscript-level coherence is what makes a single claim judgeable. Layout follows from that: the
page's identifications and the manuscript picture get **equal** weight.

Honesty constraints are absolute and greppable: no precision percentage, no confidence interval, no
human-review badge, and none of "copy of" / "quotes" / "witness of" in display. Relation kinds are
labelled with match-framing ("Direct match / Partial match / Shared text"). Coverage is shown only for
the direct family and always labelled as matched-letter coverage.
</design_direction>

<findings_index>
## Design Areas

| Area | Reference | Key Decision |
|------|-----------|--------------|
| Discovery panel layout & disclosure | `references/discovery-panel-layout.md` | Even two-pane layout (1fr/1fr ≥900px, stacks on mobile); three disclosure levels; manuscript pane NAMES the works; relation + tier filters with match-framing labels |
| Browse integration & evidence highlighting | `references/browse-integration-and-highlighting.md` | Entry control in browse toolbar row 2; panel full-width beneath the panes via a fifth `enrichment_refs` placeholder; highlighting needs **normalized→raw offset mapping plus per-line span clipping** |

## Theme

`sources/themes/default.css` (light), `parchment.css`, `dark.css` — exact mirrors of
`web/static/common.css`. Use them rather than inventing tokens.

## Source Files

Interactive sketches are preserved in `sources/`. Both run offline with no build step:

- `sources/001-discovery-panel-architecture/index.html` — 4 layout variants, 13 real manuscripts,
  relation/tier filters, EN+HE RTL, three themes. Winner: **variant D**.
- `sources/002-panel-embedded-in-browse/index.html` — the panel inside a faithful `/browse` frame,
  with 5 service states and 4 highlight modes (including the two broken ones, so the defects are
  reproducible).
- `sources/001-discovery-panel-architecture/data.js` — real data extracted from the deployed
  `discovery-v1-33499c5b` asset. Shared by both sketches.

## Hard-won findings that are NOT in any requirement doc

1. **Stored evidence offsets index the normalized Hebrew-letter stream, not the raw text.** Slicing raw
   text at them highlights the wrong characters (652 characters off on the sampled case).
2. **The highlight must additionally be clipped per line**, because the line-number gutter splits
   highlight HTML on `\n`. Done correctly 72 of 148 grid rows highlight; done naively, 1 — silently.
3. **Search-term highlighting and discovery spans compete for one render parameter**; one renderer must
   emit both.
4. **The version selector invalidates stored offsets** — the highlight must be dropped on source change.
5. **Only the largest span is stored**, so a row's stated matched-letter count can exceed what is
   highlightable.
6. **The manuscript-coherence reader aid is coupled to the BAND-03 screening gate** — part of the codex
   picture sits behind a toggle built for a different purpose.
7. **Outage must not look like a genuine zero** — today's wrappers cannot tell them apart.

## Verification technique worth reusing

Both sketches ship a `node` render-smoke harness asserting the prohibited-wording invariants across
every manuscript × variant × language × state combination (114 and 540 assertions), each proven live by
a **positive control** — seeding a precision figure, a confidence interval and a stored vocabulary key
makes the suite fail. A green check that cannot fail is worthless. This is the same technique
`136-VALIDATION.md` specifies for Success Criterion 7.
</findings_index>

<metadata>
## Processed Sketches

- 001-discovery-panel-architecture (winner: D)
- 002-panel-embedded-in-browse (accepted)
</metadata>
