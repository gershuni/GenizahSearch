# Sketch Wrap-Up Summary

**Date:** 2026-07-31 (two passes: sketches 001–002, then 003)
**Sketches processed:** 3 (all included)
**Design areas:** Discovery panel layout & disclosure · Browse integration & evidence highlighting ·
Corpus-wide findings page
**Skill output:** `./.claude/skills/sketch-findings-genizahsearch/`

## Included Sketches

| # | Name | Winner | Design Area |
|---|------|--------|-------------|
| 001 | discovery-panel-architecture | **D — even panes** | Discovery panel layout & disclosure |
| 002 | panel-embedded-in-browse | **accepted** | Browse integration & evidence highlighting |
| 003 | discovery-findings-page | **accepted** — all three row units ship, nav label "Computed Identifications" | Corpus-wide findings page |

## Excluded Sketches

None.

## Why these sketches happened

The Phase 136 discuss session produced two HTML files that looked like mockups but were real-data
probes — no states, no mobile layout, no RTL treatment. Their job was to apply the agreed rules to real
data and see what fell out, and they did that well (they caught D-13d, D-13g and D-13i). But they were
not a design contract, so the visual and interaction layer was never settled. The owner chose to sketch
the real surfaces before planning rather than generate a formal UI-SPEC.

That call paid for itself twice. Sketching 001/002 surfaced **four defects in D-12** and one
undocumented coupling that no document review had caught. Sketching 003 — the page D-19 explicitly
asked for a mockup of — established that **the findings page is blocked on gates 1–3, not merely
improved by them**, which is a roadmap fact, not a design preference.

## Design Direction

Mirror the existing app rather than invent: "Deep Academic Green" (`--primary-600: #059669`), three
`[data-theme]` themes (light / parchment / dark) copied verbatim from `web/static/common.css`, WCAG AA
deliberate. NiceGUI/Quasar target, phone-first (~68% mobile on comparable surfaces).

Register: **"an amazing feature, but caveat is needed"** — uniform row treatment, feature-grade
presence, caveat in a permanent designed slot, and no confidence encoded through per-tier styling.
Primary reader job is understanding the whole manuscript, so page identifications and manuscript
picture carry equal weight.

## Key Decisions

### The panel (001 / 002)

- **Variant D — even panes.** 1fr/1fr grid at ≥900px; stacks page-then-manuscript on mobile.
- **Three disclosure levels:** identifications (default) · "also shares text with" (collapsed, visibly
  not identifications) · "show more possible matches" (screening / short passages).
- **The manuscript pane names the works** (D-13h), with dashed chips for toggle-gated works.
- **Relation + tier filters** in the panel, AND-composed, empty = all, labelled with match-framing.
- **Embedding:** entry control in browse toolbar row 2 beside Joins; panel body full-width beneath the
  two 60vh panes; wired as a fifth `enrichment_refs` placeholder filled after Phase B.
- **Highlighting:** normalized→raw offset mapping **plus** per-line span clipping; drop the highlight on
  version change; one renderer emits both discovery and search-term marks.

### The findings page (003)

- **Nav label: "Computed Identifications / זיהויים מחושבים."** Constrained from three directions —
  "Discoveries" is taken by the Community page, a bare "Identifications" collides with "Browse by
  Identification", and D-23b bars "new".
- **All three row units ship, user-selectable** via a "Show as" control; default = one row per
  identification (**65,200**, measured here for the first time). D-19's open question is answered as
  "all three" — the row unit is a reader choice, not a design pick.
- **Two buckets — "main pool" / "more matches"** (owner, 2026-08-01), replacing the three-level
  Strong/Medium/Weak scale sketch 003 shipped. The rule: *a fragment is a probable identification when it
  matches the work across more than one leaf, or covers almost a whole page on its own.* Four
  non-compensating gates → 36,152 / 28,357. Full detail in the skill's `references/main-pool-rule.md`;
  designed by a measured pass over the live asset, independently reviewed by Codex.
- **Novelty as a prominent switch** voiced "Candidates for new finds", under an explicit candidacy hedge.
  "New discovery" was offered and declined.
- **Domain / author / work cascade** mirroring `/catalog-browse`, on the **identified work's** domain —
  never the manuscript's catalogue domain.
- **Modes, not pages:** 137's saved judgments and 138's `/leads` become tabs on this page.
- **Gated like `/atlas`** — in the absent state the nav entry disappears; the page does not render empty.
- **Novelty never sorts** (D-15a, D-24).

## Owner decisions taken during the sketches

| Decision | Outcome |
|---|---|
| Layout | Variant D (between B and C, "more even panes") |
| "Citations" as a filter label | **Declined** — keep match-framing wording. **D-21 NOT amended.** |
| Embedding approach | Approved |
| Row unit | **All three, user-selectable**; default per identification |
| Confidence presentation | Plain three-level scale, first and prominent, relation-based |
| Novelty voice | "Candidates for new finds" — significance restored, candidacy preserved |
| Facet domain axis | The **identified work's** domain, with a one-time curation pass |
| Nav label | **Computed Identifications / זיהויים מחושבים** |
| Band labels vs confidence scale | **The confidence scale wins, on both surfaces.** The frozen band labels become tooltip-only everywhere, panel included |

## Requirement / decision changes these sketches imply

| Item | Change needed | Status |
|---|---|---|
| **D-09** | Strike "collapsed" (variant D never collapses the manuscript group); keep the left-to-right ordering | **Narrow amendment owed** |
| **D-12** | Offsets index the normalized letter stream, not raw text; result must be clipped per line; highlight dropped on version change; search-term precedence rule | **Rewrite owed** |
| **`discovery-band-labels-v1.md` §2** | Band labels become **tooltip-only**; the visible split is §4's own default-shown boundary. No new display vocabulary, so a note rather than a contract rewrite. §3, §4, BAND-03 all unaffected | **Small amendment owed** (much smaller than the 2026-07-31 version) |
| Panel **tier** filter | **Deleted** — quality is the bucket, kind is the relation filter | **Resolved 2026-08-01** |
| `band_precision.tier_a` | Carry the CERT-01 result (`measured_pass` + real `ci_low`) into the v2 bake | **Data fix** — blocks the surface, see below |
| **D-16 / PANEL-01** | The findings page needs the relation filter currently specified for the panel only | **Open** — gate 1 |
| **D-13e** | Panel's middle disclosure bucket may collapse into "more matches" | **Open** — gate 1 |
| **NOVEL-01 / D-23b** | D-23b mandates "Not found in the finding aids checked" and prohibits "new"; the shipped wording uses "new finds" under a candidacy hedge | **Amendment owed**, with the *candidate ≠ discovery* reasoning on the record |
| **D-21** | — | **No change** (owner declined "Citations") |
| **PANEL-01/02** | Panel-level relation/tier filters are new scope (D-16 specifies filters for `/work/{id}` only) | Carry to gate 1 |
| Multi-span rows | Stated matched-letter count can exceed the highlightable span — qualify the label or the evidence view | Carry to gate 6 |
| `LONG_CITATION = 200` | Medium-confidence threshold, consistent with D-13c's 150-letter cutoff | Gate-1 tunable |

## Blockers 003 established (roadmap facts, not preferences)

1. **The novelty axis cannot honestly ship pre-rebuild.** 144,294 `track1_direct` rows carry
   `is_new = 0` meaning *unchecked*; a two-state filter would assert they are already recorded. This is
   the concrete argument for D-23a's fail-closed tri-state being load-bearing.
2. **`coverage_ppm` and `band_rank` do not exist as columns** — the coverage filter is inert.
3. **PERF-01 confirmed independently** — the deduped identification *count* alone took 16 s.
4. **`works.genre` is entirely empty** — the domain facet needs a one-time curation pass over ~1,088
   works (measured at ~96% high-confidence in one pass on a 93-work sample; ~3–4% need a lookup or an
   owner ruling).
5. **The `tier_a` grade is missing from `band_precision`** (added 2026-08-01). `tier_a` is 134,449
   claims — 81% of the corpus — with `measurement_status=NULL, ci_low=NULL`, so the D-18 gate fails
   closed and the main pool is **2,241 of 65,200 identifications instead of 46,644**. CERT-01 passed
   2026-07-28 at 0.9382 but into the **v2** asset, which is deployed flag-OFF; the live v1 asset was
   never updated. A data carry-over at the v2 bake, not a measurement — but until it lands the surface
   is not worth shipping.

## Still open for the owner

- Does the mode strip (leads + saved as future tabs) match the intent for Phases 137/138?
- The three low-confidence domain assignments (literary letter collection · Arabic Josippon · kalam vs
  theology) — and whether "Unspecified Domain" is the right home for works the vocabulary can't place.
- **Does the panel's three-level disclosure survive the two-bucket model?** Its middle bucket
  ("also shares text with") is behind-the-default on quality *and* distinguished only by relation, which
  the row's relation chip now carries — so it arguably collapses into "more matches". **D-13e locks
  three buckets.** Gate 1.
- **Does the findings page get the relation filter?** Deleting the confidence chips leaves it with no
  way to filter by kind of match. D-16 specifies that filter for the panel only. Gate 1.

**Resolved 2026-07-31:** the band-label vs confidence-scale disagreement between the two surfaces — the
confidence scale wins on both.
**Resolved 2026-08-01 (supersedes the above):** there is **no confidence scale**. Two buckets, split by
the existing `is_default_eligible()`. The panel's tier filter is **deleted**, not converted. The
corroborated bug stops existing rather than being fixed, because the existing predicate already places
`corroborated`/`weak` in the default pool — and the frozen contract forbade the narrow fix anyway
("NEVER a corroborated-only or weak-only split").

## Verification carried forward

All three sketches ship a `node` render-smoke harness (114, 540 and 160 assertions) enforcing the
prohibited-wording invariants across every manuscript × variant × language × state, each proven live by
a positive control. This is the technique `136-VALIDATION.md` specifies for Success Criterion 7, so it
transfers directly into the phase's render-smoke tests.

Two lessons worth keeping:

- **Negated use of a prohibited phrase still violates the rule.** 003's first page caveat read *"a match
  is not proof that a folio is a copy of the work"* — the suite failed it, correctly, because a
  grep-based CI guard cannot see the negation. The violation was in hand-written prose, not in data,
  which is where these rules actually get broken.
- **Scope every assertion to the element it is about.** 003's facet-header assertion tested the whole
  page and passed while the header was wrong, because unrelated prose contained the phrase it grepped
  for. An assertion that can pass for the wrong reason is worse than none.
