# Sketch Manifest

## Design Direction

GenizahSearch already has a settled visual language, so these sketches do **not** invent an
aesthetic — they mirror it. The production system is "Deep Academic Green" (`--primary-600: #059669`,
header gradient `#065f46 → #059669`) with three live themes driven by `[data-theme]`: light,
**parchment** (`#fffbf5`, amber-brown — the manuscript reading theme) and dark. WCAG AA is deliberate
in the real CSS, which carries explicit contrast-ratio comments and overrides several Quasar defaults
for failing contrast; sketches inherit that discipline. All three themes live in `themes/` as exact
mirrors of `web/static/common.css`.

The register the owner asked for on the Phase 136 discovery surfaces is **"an amazing feature, but
caveat is needed"** — explicitly neither a quiet scholarly apparatus nor an unqualified product
feature, and explicitly *not* confidence encoded through per-tier styling (which would edge into what
D-24 prohibits). Uniform row treatment, genuine feature-grade presence, and the caveat given a
designed permanent place rather than being buried as fine print or shouted as a warning.

The stated primary job on the panel is **understanding the whole manuscript**, not judging a single
row — which is in tension with the locked D-09 ordering. Surfacing that tension is what sketch 001
exists to do.

Every sketch is grounded in real data from the deployed discovery asset, and mobile-first: the
comparable `/atlas` surface runs ~68% mobile.

## Reference Points

- The app itself — `web/static/common.css`, `web/pages/browse.py`, `web/pages/catalog_browse.py`
- `136-MOCKUP-MULTI.html` — the seven standing-regression manuscripts (real-data probes, **not**
  design mockups; they carry no states, no mobile layout and no RTL treatment, which is why these
  sketches exist)
- NiceGUI / Quasar as the build target — note the app uses `q-expansion-item` at only 2 sites, both
  driven awkwardly via `run_javascript`, so nested disclosure is not an established pattern here

## Sketches

| # | Name | Design Question | Winner | Tags |
|---|------|----------------|--------|------|
| 001 | discovery-panel-architecture | Where does manuscript-level coherence live relative to this page's identifications, and how do the three disclosure buckets read? | **D — even panes** | discovery, panel, phase-136, disclosure, filters, rtl, mobile, d-09, d-21 |

| 002 | panel-embedded-in-browse | How does the panel sit inside the real `/browse` page, and how does offset highlighting actually work on its text pane? | **accepted** | discovery, browse, integration, highlighting, d-12, panel-01, panel-03 |

### Owner decisions (2026-07-31)

- **Variant D — even panes — wins.** Two equal panes at ≥900px; page identifications and the
  manuscript picture carry the same weight. Stacks page-then-manuscript on mobile.
- **The relation filter keeps match-framing wording** ("Direct match / Partial match / Shared text").
  The owner explicitly declined "Citations", so **D-21 is NOT amended** — no escalation needed.
- **D-09 still needs a narrow amendment:** variant D never collapses the manuscript group, so strike
  "collapsed" from D-09 while keeping its left-to-right ordering.
- **Embedding approved:** entry control in browse toolbar row 2 beside Joins; panel body full-width
  beneath the two 60vh panes; wired as a fifth `enrichment_refs` placeholder.
- **Panel filters remain new scope** — D-16 specifies filters for `/work/{id}`, not the panel. Carry
  as a PANEL-01/02 amendment or a deliberate reuse decision at gate 1.

**Sketch 002 found two defects in D-12** (both verified against the asset and the live code, both
cheap to fix, neither currently written down): the stored offsets index the normalized Hebrew-letter
stream rather than the raw text — so D-12's "slice the RAW page text at the stored offsets" ends
Moss. V,374's highlight 652 characters early — and the result must additionally be clipped per line,
because the line-number gutter splits highlight HTML on `\n`. Done correctly 72 of 148 grid rows
highlight; done as written, 1. Plus two collisions (search-term highlighting shares the same render
slot; the version selector invalidates the offsets) and one honesty problem (only the largest span is
stored, so the stated matched-letter count exceeds what can be highlighted).

**Round 2 (owner steer) on sketch 001:** a fourth variant **D — even panes** (the B+C synthesis), a per-view
**relation-kind + tier filter**, and 13 manuscripts instead of 7 so tiers and relation kinds are
actually exercised. All data real from the deployed asset; nothing illustrative.

**Two open decisions this sketch forces:**
1. **D-09** locks "On this page first, then Elsewhere (collapsed)". B and D both drop the collapse; if
   either wins, D-09 needs a dated amendment (narrower for D — strike "collapsed", keep the ordering).
2. **D-21** prohibits "quotes" in display, so the relation filter is labelled with match-framing
   ("Direct match / Partial match / Shared text"), not "Citations". Calling it "Citations" is a
   defensible product choice that needs a dated D-21 amendment.

Also noted: **panel filters are new scope** — D-16 specifies filters for `/work/{id}`, not the panel,
whose locked model is D-13e's fixed three-bucket disclosure.

### Queued (scoped, not yet built)

| # | Name | Design Question |
|---|------|----------------|
| 002 | discovery-row-anatomy | How does one row read as "amazing but caveated"? Placement of band label, matched-letter coverage, offsets, vote placeholders and the `unreviewed · algorithmic estimate` stamp — and how they stack on a phone. |
| 003 | discovery-service-states | Are the four service states unmistakably distinct — especially **outage ≠ genuine zero** (the D-13 envelope, and the one genuinely new test class in the phase)? |
