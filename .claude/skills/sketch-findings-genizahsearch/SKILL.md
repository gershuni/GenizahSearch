---
name: sketch-findings-genizahsearch
description: Validated design decisions, CSS patterns, and visual direction from GenizahSearch sketch experiments — the Phase 136 discovery panel, its browse-page embedding, the evidence-highlighting algorithm, and the corpus-wide findings page. Auto-load when building discovery read surfaces or any NiceGUI browse-page UI.
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

Sketch sessions wrapped: 2026-07-31 (sketches 001–002), 2026-07-31 (sketch 003).
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
the direct family and always labelled as matched-letter coverage. **A negated use still violates the
rule** — "not proof that a folio is a *copy of* the work" is a violation, because a grep-based CI guard
cannot see the negation.

Confidence is displayed as a **three-level scale (Strong / Medium / Weak) derived from the relation
kind, not from the frozen band** — on **every** discovery surface, panel included (owner, 2026-07-31).
The frozen band labels survive as tooltips. Novelty is voiced **"Candidates for new finds"**, asserting
candidacy only; "New discovery" was offered and declined, because it stacks two unearned claims (that
the match is correct, and that it is new) on a row with no human review until Phase 137.
</design_direction>

<findings_index>
## Design Areas

| Area | Reference | Key Decision |
|------|-----------|--------------|
| Discovery panel layout & disclosure | `references/discovery-panel-layout.md` | Even two-pane layout (1fr/1fr ≥900px, stacks on mobile); three disclosure levels; manuscript pane NAMES the works; relation + tier filters with match-framing labels |
| Browse integration & evidence highlighting | `references/browse-integration-and-highlighting.md` | Entry control in browse toolbar row 2; panel full-width beneath the panes via a fifth `enrichment_refs` placeholder; highlighting needs **normalized→raw offset mapping plus per-line span clipping** |
| Corpus-wide findings page | `references/findings-page.md` | Nav label **"Computed Identifications"**; all three row units user-selectable (default = per identification, 65,200); **relation-derived** Strong/Medium/Weak scale; novelty as a prominent switch voiced "Candidates for new finds"; domain/author/work cascade on the **identified work's** domain; modes not pages. **Blocked on the rebuild** — see below |

**RESOLVED (owner, 2026-07-31): the confidence scale wins, on both surfaces.** The frozen band labels
(`shared/discovery_band_labels.py::BAND_LABELS`) become **tooltip-only** everywhere — the visible chip is
`Strong` / `Medium` / `Weak` on the panel and on the findings page alike. Implement `confOf()` once (it is
in `findings-page.md`) and share it.

This makes the `discovery-band-labels-v1.md` **§2** amendment a system-wide display-contract change
rather than a page-local one — write it that way. **§4 (default visibility) and BAND-03 (screening
routing) are untouched**: bands still decide which disclosure bucket a row lands in and what is gated by
default. Only the *label a reader sees* changes.

⚠ It also leaves one follow-on open: the panel's **tier** filter now labels options in a vocabulary its
rows no longer use. Switching it to a confidence filter restores one vocabulary but collapses `tier_a`
and `high_confidence_algorithmic` into `Strong`, so tier A can no longer be isolated. Recommended but
**not decided** — see `discovery-panel-layout.md`.

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
  `discovery-v1-33499c5b` asset. Shared by sketches 001 and 002.
- `sources/003-discovery-findings-page/index.html` — the corpus-wide page: 3 row units × 4 service
  states × 2 languages × 3 nav labels, plus a "pretend rebuild has landed" switch that flips novelty
  from what the asset can honestly show today to the intended tri-state. Its own `data.js` (real
  totals + bounded row samples) and `work-domains.js` / `work-domains.sample.json` (the 93-work domain
  feasibility sample).

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
8. **144,294 direct rows carry `is_new = 0` meaning UNCHECKED, not "known".** A two-state novelty
   filter over that data makes a false claim on the flagship surface. This is why D-23a's fail-closed
   tri-state is load-bearing rather than bookkeeping.
9. **`coverage_ppm` and `band_rank` do not exist as columns** (`PRAGMA table_info`), so the coverage
   filter is inert until the rebuild.
10. **A band-derived confidence scale orphans 20,435 never-assessed rows** (12.3%) and forces a fourth
    "not assessed" level. Deriving it from the relation kind avoids this — three levels, honestly.
11. **`works.genre` is entirely empty**, so the domain facet needs a one-time curation pass (~1,088
    works, ~96% high-confidence in one pass). The **work** facet needs nothing; bridging discovery
    titles to FJMS `genizah_titles` matches only 5% and is not required.
12. **A manuscript's catalogue domain is the wrong filter axis** — Moss. V,374 is catalogued *Court
    Records* while carrying a correct Rashi finding, and 338 tier-A findings sit on manuscripts
    catalogued documentary/legal. Filtering on it hides exactly the findings that disagree with the
    catalogue.
13. **PERF-01 confirmed twice** — the deduped identification *count* alone took 16 s. A visible real
    total is not free.

## Requirement amendments these sketches owe

| Item | Change | Status |
|---|---|---|
| **D-09** | Strike "collapsed" (variant D never collapses the manuscript group); keep the left-to-right ordering | narrow amendment owed |
| **D-12** | Offsets index the normalized letter stream; result must be clipped per line; highlight dropped on version change; search-term precedence rule | rewrite owed |
| **`discovery-band-labels-v1.md` §2** | Seven frozen `(family, band)` display labels → three user-facing confidence levels, **system-wide** (panel + findings page; owner ruled 2026-07-31). Labels become tooltip-only. **§4 and BAND-03 unaffected** | amendment owed |
| Panel **tier** filter | Now labels options in a vocabulary its rows no longer use. Confidence filter recommended; collapses tier_a + high_confidence_algorithmic into Strong | **open**, not decided |
| **NOVEL-01 / D-23b** | D-23b mandates "Not found in the finding aids checked" and prohibits "new"; shipped wording uses "new finds" under a candidacy hedge | amendment owed, with the *candidate ≠ discovery* reasoning on the record |
| **D-21** | — | no change (owner declined "Citations") |
| **PANEL-01/02** | Panel-level relation/tier filters are new scope (D-16 covers `/work/{id}` only) | carry to gate 1 |
| `LONG_CITATION = 200` | The Medium-confidence threshold, consistent with D-13c's 150-letter cutoff | gate-1 tunable |

## Verification technique worth reusing

All three sketches ship a `node` render-smoke harness asserting the prohibited-wording invariants across
every manuscript × variant × language × state combination (114, 540 and 160 assertions), each proven
live by a **positive control** — seeding a precision figure, a confidence interval or a stored vocabulary
key makes the suite fail. A green check that cannot fail is worthless. This is the same technique
`136-VALIDATION.md` specifies for Success Criterion 7.

Sketch 003 adds a second lesson: **scope every assertion to the element it is about.** Its facet-header
assertion tested the whole rendered page and *passed while the header was wrong*, because unrelated
design-note prose happened to contain the phrase it grepped for. An assertion that can pass for the
wrong reason is worse than none.
</findings_index>

<metadata>
## Processed Sketches

- 001-discovery-panel-architecture (winner: D)
- 002-panel-embedded-in-browse (accepted)
- 003-discovery-findings-page (accepted; nav label "Computed Identifications", all three row units ship)
</metadata>
