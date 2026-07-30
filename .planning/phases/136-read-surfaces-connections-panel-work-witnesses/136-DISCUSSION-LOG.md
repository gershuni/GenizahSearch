# Phase 136: Read Surfaces — Connections Panel & Work→Witnesses - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-07-30
**Phase:** 136-read-surfaces-connections-panel-work-witnesses
**Areas discussed:** Claim asset choice, Panel contents & relation wording & v1 scope, tier_a default
visibility & precision numbers, Work page & findability, Public projection × work titles, Novelty scope

**Mid-discussion input from the owner:** `same_work_spike/probe/rsource/HANDOFF-TO-135.md` (the GEN2
v2.1 evidence handoff) was introduced after the first gray-area presentation, which invalidated the
initial framing and forced a re-derivation. The owner also asked for plainer, less jargon-heavy
questions after the second round.

---

## Claim asset choice

| Option | Description | Selected |
|--------|-------------|----------|
| Live v2; v2.1 gets its own phase | Build the surfaces on the deployed, fully-titled, band-bearing asset whose CERT-01 certificate is already graded | ✓ |
| v2.1 re-bake as 136's leadoff | Mirror the 135 pattern — data re-distill first, then build against corrected data | |
| v2.1 restricted to the 1,237 already-titled works | 82,156 claims, no new curation round | |
| Live v2 now; ingest v2.1's surface labels as an additive overlay later | Keep the certified population, gain the quotation split by joining coverage labels | |

**User's choice:** Live v2; v2.1 gets its own phase.
**Notes:** Measured during the discussion and decisive: only 34.7% of v2.1's shipped claims name a
work with an owner-reviewed neutral title; v2.1 carries no MS-to-MS/shared_text family at all (every
claim is `direct_witness`) and `band` is NULL; and a population change strands the CERT-01
measurement that REL-01 requires to describe what ships.

---

## Panel contents, relation wording & v1 scope

| Option | Description | Selected |
|--------|-------------|----------|
| Manuscript-scoped, current page marked | All identifications across the MS, page rows marked/sorted first | |
| Page-scoped only | Only claims on the page in view; no new query | |
| Both: page section first, then rest of manuscript | "On this page" then "Elsewhere in this manuscript" (collapsed) | ✓ |

| Option | Description | Selected |
|--------|-------------|----------|
| Nested: identifications, each expandable to its other MSS | Top level = identification rows; other-manuscripts nests per work | ✓ |
| Three flat collapsible sections | Peer sections, each independently collapsible | |
| Tabs inside the panel | Compact for outlier manuscripts, hides two-thirds behind a click | |

| Option | Description | Selected |
|--------|-------------|----------|
| Accept — one rule everywhere, section behind the toggle | `not_evaluated` never default-shown, section renders only after "show more" | |
| Amend band-labels-v1 so shared_text has its own default | Dated amendment treating related-pages as navigation, not a claim | |
| Show the section header + count by default, rows behind the toggle | Existence discoverable, nothing unevaluated asserted as an identification | ✓ |

| Option | Description | Selected |
|--------|-------------|----------|
| Highlight in the transcription already on screen | Reuse browse's highlight machinery, fail closed on snapshot drift | (leading) |
| Full dedicated evidence pane in v1 | Separate on-demand pane with span excerpt + match stats | |
| Match stats only in v1 | No text rendering at all | |

**User's choice:** Both / Nested / Header+count. PANEL-03 held open: *"Probably 1, but I'll need to see
a mockup (preferably with our real data) to decide."*
**Notes:** `discovery_evidence` has no `sys_id` index, so the manuscript group is served via
`page_id IN (…)` over browse's own page list. Per-manuscript medians (2 claims / 1 work) and the 429
manuscripts over 50 claims set the pagination requirement.

---

## tier_a default visibility & precision numbers

*(Re-asked in plain language at the owner's request after the first, jargon-heavy attempt was declined.)*

| Option | Description | Selected |
|--------|-------------|----------|
| Yes, rebuild first | Precision-only rebuild + redeploy; panel then shows 134,000 instead of 2,660 | ✓ |
| No, build screens now and rebuild at release | Leaves the live file alone; surfaces look empty during build/UAT | |
| Rebuild, but keep the group behind "show more" | Most cautious display; reverses the promote-on-pass decision | |

| Option | Description | Selected |
|--------|-------------|----------|
| Publish 94%, explain the breakdown | The pre-committed figure, understating the public data | |
| Publish 96% as the public figure | Pre-specified public scope, more accurate for what readers see | |
| No percentage in this phase | Tier name + methods link only | ✓ |

| Option | Description | Selected |
|--------|-------------|----------|
| Only on the methods page, linked from every label | Full strata table on the methods page | ✓ (then superseded) |
| Also flag rows from a weak slice | Per-row caveat chip | |
| Leave it out for now | Add at release | |

| Option | Description | Selected |
|--------|-------------|----------|
| Always show the button, with a count | Zero opens to an empty state carrying the recall disclaimer | |
| Hide it when there is nothing | No dead ends; feature invisible on ~83% of manuscripts | ✓ |
| Always show it, no count, load on click | No per-page lookup; a click to learn there is nothing | |

**User's choice:** Rebuild first / no percentage anywhere / hide the button when empty.
**Notes:** The two number answers initially conflicted, so a clarifying question was asked. The owner
resolved it by rejecting both framings: *"No need of full breakdown — the exact percentage may be
misleading and may include sources the user will never see. What's important is the tiers, and the
user can judge for themselves."* This supersedes the 135 carry-forward that the per-stratum spread
must appear on the methods page, and it collides with REL-01/CERT-02's "goes public with its measured
number" clause — flagged for Phase 139 rather than re-opened here.

---

## Work page & findability

| Option | Description | Selected |
|--------|-------------|----------|
| Strongest tier first, then shelfmark | Deterministic, trustworthy entries on top | ✓ (as default of a 3-way toggle) |
| By library, then shelfmark | Catalogue-like | ✓ (as toggle option) |
| By how much of the page matched | Fullest overlap first | ✓ (as toggle option) |

| Option | Description | Selected |
|--------|-------------|----------|
| Plain paging with the real total shown | "13,038 manuscripts — page 1 of 66" | ✓ |
| Per-library summary first, then drill in | Friendlier for giant works, odd for the median work | |
| Default to the strongest tier when huge | Size-dependent behaviour | |

| Option | Description | Selected |
|--------|-------------|----------|
| Tier plus library, reusing the existing library filter | Consistent with search + catalogue browse | (library = optional) |
| Tier and library as fresh chips | A third library-filter behaviour on the site | |
| Tier only | Useless for the 120 oversized works | |

**User's choice:** All three sort orders as a toggle, tier-first by default, **plus a novelty toggle**
("show only novel findings / show everything", the latter with a clear "novel?" mark). Paging with real
totals. Filters: *"Library filter is not the one that's important, though it is not entirely useless.
Tier and 'novel?' is much more important, and also percentage of span."*
**Notes:** On findability the owner declined to choose and asked to discuss: *"It's a big new amazing
feature and it should be very accessible, allowing for maximum ability to see new findings… but we do
have the /catalog page and it's reasonable to put the computed identification also there."* They also
noted that the catalogue page's notion of identification is same-work only, while our data carries a
parallel/citation tier.

---

## Novelty scope (raised by the owner's repeated priority signal)

| Option | Description | Selected |
|--------|-------------|----------|
| Keep it out; build the toggle now, light it up next phase | Toggle designed in but switched off; second rebuild cycle later | |
| Pull it in — one rebuild carries precision, span coverage and novelty | One asset, one deploy; phase roughly doubles | ✓ |
| Pull in only the cheap half | Catalogue/bibliography checks only, flag marked provisional | |

| Option | Description | Selected |
|--------|-------------|----------|
| Add page length; use for sort and filter | Builder already computes it | ✓ |
| Display and sort only, no filter | Less UI, less threshold-as-quality-gate risk | |
| Leave it out of this phase | Third rebuild cycle later | |

| Option | Description | Selected |
|--------|-------------|----------|
| Full check: catalogue, bibliography, PGP, FGP + title judgement | One flag per (sys_id, specific work); ~$27 one-time | ✓ |
| Catalogue difference only, no paid title check | Over- and under-claims per the handoff's measurements | |
| Two separate signals side by side | Strict flag plus a catalogue-only marker | |

| Option | Description | Selected |
|--------|-------------|----------|
| Catalogue page shows same-work only; parallels on panel/work pages | Keeps that page's current meaning | |
| Catalogue page gains parallels as a clearly separate axis | Both present, separately labelled | ✓ (with reworded labels) |
| Do not touch the catalogue page in this phase | Panel links only | |

| Option | Description | Selected |
|--------|-------------|----------|
| Ship the findings browse here | Novelty-first browse; gives SEED-032 a home | ✓ |
| Next to the leads queue two phases on | One browse layout instead of two | |
| Minimal version here, full version later | Small browse surfaces tend to get rebuilt | |

| Option | Description | Selected |
|--------|-------------|----------|
| One phase, data rebuild first, then surfaces | Wave ordering does the de-risking; one deploy | ✓ |
| Split into two numbered phases | Cleaner traceability, extra ceremony, renumbering | |
| One phase, surfaces first against today's data | Unrepresentative-data problem | |

**User's choice:** Pull novelty in; add span coverage for sort and filter; full novelty check; catalogue
page carries both relation kinds but **not** under "copy of"/"quotes" wording; findings browse ships
here; one phase, data first.
**Notes:** The owner's own idea reframed the mechanism — *"it's a novelty gate by its own. What's
computed and not there is new. Not exactly like the LLM-novel gate (we may have identifications in the
cat/bib that is not there) but it's an option."* That is the funnel's catalogue arm; the title check
survives because a catalogue title can name the work in different words. They also insisted on
mockups and a Codex adversarial pass: *"I should again remind we need mockups, and of course consult
Codex since there are many possible pitfalls."*

---

## Relation wording & public/private projection

| Option | Description | Selected |
|--------|-------------|----------|
| Observational — "text of X found here" | States only what was observed | |
| Match framing — "matches X · 68% of page" | Honest about being a computed comparison | ✓ |
| Relationship-named but hedged | "Apparently a copy of" / "apparently quotes" | |

| Option | Description | Selected |
|--------|-------------|----------|
| Decide per row, on where the displayed claim came from | Structural absence from the public file | ✓ |
| Decide per work — drop every restricted work entirely | Simplest to verify; drops 6,564 publishable rows | |
| Keep the row, hide the work's name | Placeholder titles; near-useless to a researcher | |

| Option | Description | Selected |
|--------|-------------|----------|
| Say it is recorded elsewhere, without naming the source | Name it where nameable, otherwise "another reference source" | ✓ |
| Publicly, only flag novel or say nothing | Cannot distinguish "already known" from "not checked" | |
| Name the source whenever we can, drop the row when we cannot | Silently shrinks the public register | |

**User's choice:** Match framing / per-row projection / "recorded elsewhere" without naming.
**Notes:** The owner's rationale for match framing was the heuristic-honesty constraint: *"we are not
sure that tier_a is same work and the next are parallels, just heuristics."* The per-row projection was
supported by a measurement taken during the discussion — the restricted-corpus id prefix maps to 656
restricted-identity works AND 235 open ones, so a corpus-keyed rule mislabels both ways.

---

## Closing

| Option | Description | Selected |
|--------|-------------|----------|
| Nothing else — write it up | Record the decisions; PANEL-03 open pending mockup | ✓ |
| The findings browse — what it actually lists | Row unit and default ordering undesigned | (to the gate) |
| The methods page, now that no numbers appear | Rework of what 135 shipped | (to the gate) |
| Roadmap and requirements bookkeeping | Homing, goal, success criteria, leads relationship | (to the gate) |

**User's choice:** *"write it up. Let's move the wheels and do the corrections and other decisions upon
the mockup+codex gate."*

## Claude's Discretion

- Per-surface BAND-04 disclaimer variants (base sentence fixed in 135 D-12).
- Whether the wave-1 rebuild adds an index on `discovery_evidence.sys_id`.
- Page sizes, timeouts, LRU sizing and query shapes within the PERF-01 caps; overload/absent copy;
  whether `discovery-budgets.md` needs a version bump for the new query shapes.

## Deferred Ideas

- The gen-2 / discovery-v2.1 evidence refresh as its own phase, with the reference-granularity stage
  and the witness-vs-quoter lever.
- Phase 139: the REL-01/CERT-02 "measured number" conflict, the homeless correction/retraction policy,
  VIS-02's positive control.
- Community judgments (137), leads queue (138).
- The work-page library filter if reuse is not cheap.
- Whether restricted-source rows ever move private → public later (projection permanence).
