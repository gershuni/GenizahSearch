# Sketch Wrap-Up Summary

**Date:** 2026-07-31
**Sketches processed:** 2 (both included)
**Design areas:** Discovery panel layout & disclosure · Browse integration & evidence highlighting
**Skill output:** `./.claude/skills/sketch-findings-genizahsearch/`

## Included Sketches

| # | Name | Winner | Design Area |
|---|------|--------|-------------|
| 001 | discovery-panel-architecture | **D — even panes** | Discovery panel layout & disclosure |
| 002 | panel-embedded-in-browse | **accepted** | Browse integration & evidence highlighting |

## Excluded Sketches

None.

## Why these sketches happened

The Phase 136 discuss session produced two HTML files that looked like mockups but were real-data
probes — no states, no mobile layout, no RTL treatment. Their job was to apply the agreed rules to real
data and see what fell out, and they did that well (they caught D-13d, D-13g and D-13i). But they were
not a design contract, so the visual and interaction layer was never settled. The owner chose to sketch
the real surfaces before planning rather than generate a formal UI-SPEC.

That call paid for itself: sketching surfaced **four defects in D-12** and one undocumented coupling
that no document review had caught.

## Design Direction

Mirror the existing app rather than invent: "Deep Academic Green" (`--primary-600: #059669`), three
`[data-theme]` themes (light / parchment / dark) copied verbatim from `web/static/common.css`, WCAG AA
deliberate. NiceGUI/Quasar target, phone-first (~68% mobile on comparable surfaces).

Register: **"an amazing feature, but caveat is needed"** — uniform row treatment, feature-grade
presence, caveat in a permanent designed slot, and no confidence encoded through per-tier styling.
Primary reader job is understanding the whole manuscript, so page identifications and manuscript
picture carry equal weight.

## Key Decisions

- **Variant D — even panes.** 1fr/1fr grid at ≥900px; stacks page-then-manuscript on mobile.
- **Three disclosure levels:** identifications (default) · "also shares text with" (collapsed, visibly
  not identifications) · "show more possible matches" (screening / short passages).
- **The manuscript pane names the works** (D-13h), with dashed chips for toggle-gated works.
- **Relation + tier filters** in the panel, AND-composed, empty = all, labelled with match-framing.
- **Embedding:** entry control in browse toolbar row 2 beside Joins; panel body full-width beneath the
  two 60vh panes; wired as a fifth `enrichment_refs` placeholder filled after Phase B.
- **Highlighting:** normalized→raw offset mapping **plus** per-line span clipping; drop the highlight on
  version change; one renderer emits both discovery and search-term marks.

## Owner decisions taken during the sketches

| Decision | Outcome |
|---|---|
| Layout | Variant D (between B and C, "more even panes") |
| "Citations" as a filter label | **Declined** — keep match-framing wording. **D-21 NOT amended.** |
| Embedding approach | Approved |

## Requirement / decision changes these sketches imply

| Item | Change needed | Status |
|---|---|---|
| **D-09** | Strike "collapsed" (variant D never collapses the manuscript group); keep the left-to-right ordering | **Narrow amendment owed** |
| **D-12** | Offsets index the normalized letter stream, not raw text; result must be clipped per line; highlight dropped on version change; search-term precedence rule | **Rewrite owed** |
| **D-21** | — | **No change** (owner declined "Citations") |
| **PANEL-01/02** | Panel-level relation/tier filters are new scope (D-16 specifies filters for `/work/{id}` only) | Carry to gate 1 |
| Multi-span rows | Stated matched-letter count can exceed the highlightable span — qualify the label or the evidence view | Carry to gate 6 |

## Verification carried forward

Both sketches ship a `node` render-smoke harness (114 and 540 assertions) enforcing the
prohibited-wording invariants across every manuscript × variant × language × state, each proven live by
a positive control. This is the technique `136-VALIDATION.md` specifies for Success Criterion 7, so it
transfers directly into the phase's render-smoke tests.
