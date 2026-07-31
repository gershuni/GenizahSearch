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
| 001 | discovery-panel-architecture | Where does manuscript-level coherence live relative to this page's identifications, and how do the three disclosure buckets read? | _pending (round 2)_ | discovery, panel, phase-136, disclosure, filters, rtl, mobile, d-09, d-21 |

**Round 2 (owner steer):** a fourth variant **D — even panes** (the B+C synthesis), a per-view
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
