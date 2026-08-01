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

| 003 | discovery-findings-page | What is the row unit of the corpus-wide findings page, and what can it honestly show before the rebuild? | **accepted** — all three units ship | discovery, findings-page, phase-136, d-19, novelty, perf-01, nav, gate-5 |

**Sketch 003 (the page D-19 explicitly asked for a mockup of) found that the novelty axis cannot
honestly ship on the current asset.** Only propagated rows carry a verdict (14,003 flagged / 8,240
not); all **144,294 direct rows sit at `is_new = 0` meaning UNCHECKED**, so a two-state filter would
assert those findings are already recorded — false. Also: `coverage_ppm` and `band_rank` do not exist
as columns, so the coverage filter is inert, and the deduped identification count took **16 s**,
independently confirming D-10a's PERF-01 problem. Row-unit totals measured for the first time:
**65,200** identifications (the recommended unit) vs 166,537 claims / 44,375 manuscripts / 1,088 works.
The page is therefore *blocked on* gates 1–3, not merely improved by them.

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

### Owner decisions on sketch 003 (2026-07-31, at wrap-up)

- **Nav label: "Computed Identifications" / זיהויים מחושבים.** Consistent with the panel title, and
  "computed" carries the caveat in the label itself. Runners-up were "Findings / ממצאים" and
  "Text Matches / התאמות טקסט".
- **All three row units ship, user-selectable** ("Show as" control), default = one row per
  identification. D-19's open question is answered as "all three" — the row unit is a reader choice.
- **Sketch 003 accepted into the findings skill** as its third design area.

- **The confidence scale wins, and applies to the panel too.** `Strong` / `Medium` / `Weak` becomes the
  visible label on **every** discovery surface; the frozen `BAND_LABELS` strings survive as tooltips.
  This widens the `discovery-band-labels-v1.md` §2 amendment from page-local to system-wide. §4 (default
  visibility) and BAND-03 (screening routing) are untouched — bands still drive gating and bucket
  placement, only the reader-facing label changes.

### Follow-ups from the ruling (2026-08-01)

- **Resolved — the panel's tier filter becomes a confidence filter.** Tier A stays reachable: §4's
  2026-07-24 amendment already keeps `tier_a` behind the "show more" toggle pending CERT-01, so that
  toggle *is* the tier-A control and the granularity cost is minimal.
### The confidence model, settled 2026-08-01

Checking those collisions against the live asset retired the scale entirely. **Two buckets — "main
pool" / "more matches"** — split by `shared/discovery_band_labels.py::is_default_eligible()`, which
already exists and already implements §4 + the D-18 gate. Sketch 003's `confOf()` / `STRONG_BANDS` was a
second, disagreeing implementation of the same idea; it is deleted, along with the three level labels
and `LONG_CITATION`.

- **Measured composition:** main pool **92.4% same-work**; more matches 48.2% same-work / 40.3% shared
  wording / 11.5% quotes. So the tooltip *"best pool for same-work identification"* is earned, but
  *"mostly citations and shared texts"* is **not** — reword to *"lower-confidence and ungraded matches"*.
- **The corroborated bug stops existing.** `is_default_eligible()` already returns True for
  `corroborated`/`weak`; and the narrow fix was never available — the asset's note forbids a
  corroborated-only or weak-only split of the 0.926 measurement.
- **Blocking data fix:** `tier_a` (81% of the corpus) has `measurement_status=NULL, ci_low=NULL`, so the
  main pool is **2,241 of 65,200 identifications instead of 46,644**. CERT-01 passed 2026-07-28 into the
  unshipped **v2** asset; carry it into the v2 bake.
- **Page-unit inflation quantified:** counting per page overstates same-work ~2.3× (88.4% → 77.8%
  deduped), which is why per-identification is the default row unit.

### The main-pool rule, adopted 2026-08-01

`is_default_eligible()` was a placeholder — it splits on *was this band graded*, not *is this the work*.
The adopted rule (full detail in the skill's `references/main-pool-rule.md`), designed by a measured
pass over the live asset and independently reviewed by Codex:

> A fragment is a probable identification when it matches the work **across more than one leaf**, or
> **covers almost a whole page** on its own.

Four non-compensating gates → **main 36,152 (56%) / more 28,357 (44%)**; 0.92 main precision against 211
human grades vs 0.88 for a naive claim-type split (design numbers only — D-06 bars them from any
surface). Multi-folio agreement measured 96% good at two pages; coverage separates same-work (median
0.987) from quotes (0.519).

- **`density` is NOT coverage** — it is Levenshtein edit-distance, and the repo warns that treating it
  as coverage *"demoted ~100% of witnesses"*. Real coverage is computed at bake and thrown away.
- **Containment residue, accepted:** `משנה תורה, ספר אהבה` ranks 7th corpus-wide (above Isaiah) because
  it contains the whole liturgy. Root cause: `claim_type` defaults to *same work* when the true host work
  is absent from the reference corpus, and only 22 liturgy works ship. The rule cuts it 37%; **1–3% of
  the main pool stays misattributed** and no sidecar-computable signal fixes it.
- **Blocking data:** the `tier_a` grade carry-over, plus a `page_id → letter count` table.
- **Work-side offsets are the highest-value rebuild item** (owner, 2026-08-01) — and the matcher already
  computes them, discarding them at ingest. Beyond fixing containment they make citations *locatable*
  ("Mishneh Torah, Laws of Prayer ch. 4" rather than "Mishneh Torah"), which turns the 28,357-row
  "more matches" bucket from an apology into a browsable corpus of addressed quotations, and enables
  side-by-side evidence, join sequencing and leaf ordering.
- **Do not freeze the 0.8 threshold** without a ~300-case scholar review — both reviewers said so.

Still open for the owner: whether the mode strip matches the intent for Phases 137/138; the three
low-confidence domain assignments; whether the panel's three-level disclosure collapses to two (D-13e);
and whether the findings page gains the panel's relation filter (D-16).

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

Renumbered at wrap-up — 002 and 003 are now taken by built sketches.

| # | Name | Design Question |
|---|------|----------------|
| 004 | discovery-row-anatomy | How does one row read as "amazing but caveated"? Placement of band label, matched-letter coverage, offsets, vote placeholders and the `unreviewed · algorithmic estimate` stamp — and how they stack on a phone. |
| 005 | discovery-service-states | Are the four service states unmistakably distinct — especially **outage ≠ genuine zero** (the D-13 envelope, and the one genuinely new test class in the phase)? |

Both are now partly covered: 002 exercises 5 service states and 003 exercises 4, and 003's row anatomy
is settled for the findings page. What remains genuinely unbuilt is the **panel** row anatomy on a phone
and a dedicated outage-vs-zero comparison.
