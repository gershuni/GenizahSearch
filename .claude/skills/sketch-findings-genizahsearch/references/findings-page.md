# Corpus-wide Findings Page

Validated in sketch 003 against real totals and real sample rows from the deployed
`discovery-v1-33499c5b` asset, in three production themes, EN + HE RTL, at 375/768/full width, across
4 service states and 2 rebuild states. This is the page D-19 asked for a mockup of, and the surface
carrying the owner's rationale — *"a big new amazing feature… maximum ability to see new findings."*

## Design Decisions

**Nav label: "Computed Identifications / זיהויים מחושבים" — SELECTED.** Consistent with the panel
title, and "computed" puts the caveat in the label itself. It survived a three-way constraint:
"Discoveries" is already taken by `web/pages/discoveries.py` (the Community page); a bare
"Identifications" collides with the existing "Browse by Identification" nav entry for
`/catalog-browse`; and D-23b bars "new", so "New Findings" is out. The two runners-up were
"Findings / ממצאים" and "Text Matches / התאמות טקסט".

**All three row units ship, user-selectable.** D-19 left the row unit open; the answer is that it is a
reader choice, not a design pick, exposed through a **"Show as"** select in the result bar. Default is
one row per identification.

| Unit | Rows | Note |
|---|---|---|
| per **claim** (page × work) | 166,537 | **not offered** — the same identification repeats once per folio |
| per **identification** (manuscript × work) | **65,200** | **default.** The only unit where tier, coverage, novelty and the future vote all attach to exactly the thing on the line. CONTEXT.md recommended it without a count; this is that count |
| per **manuscript** | 44,375 | **9,806 carry more than one work**, so a novelty verdict on the row is ambiguous — novel *how*? Rows carry an inline annotation wherever this bites |
| per **work** | 1,088 | browsable, but the individual find is hidden and giant works dominate by size. Novelty is not offered on this unit at all |

**A plain three-level confidence scale, first and prominent — defined by relation kind, not by
internal band.** The frozen band labels were not understandable to readers, so the surface describes
*what kind of claim this is*.

> **This scale is system-wide, not page-local.** The owner ruled on 2026-07-31 that it wins and applies
> to the **discovery panel** too, so the frozen band labels become tooltip-only everywhere. `confOf()`
> below is the single implementation for both surfaces — see `discovery-panel-layout.md` for what it
> changes on a panel row, including the one follow-on decision it leaves open (the panel's tier filter).

| Level | Meaning | Rule | Rows | Share |
|---|---|---|---|---|
| **Strong** | may be the same work | `direct_witness` in a strong band | 131,164 | 78.8% |
| **Medium** | a long citation-type match | `quotes_this_work`, ≥ 200 matched letters | 3,501 | 2.1% |
| **Weak** | the rest | everything else | 31,872 | 19.1% |

The relation-based definition is what makes this honest. A **band**-derived scale left 20,435 rows
(12.3%) that were never assessed, and calling those "weak" would assert an assessment nobody made —
forcing an awkward fourth "not assessed" level. Under the relation definition, "weak" describes a weak
*relation*, which those rows genuinely have. Three levels, honestly.

`LONG_CITATION = 200` sits just above D-13c's 150-letter short-passage cutoff, so the two thresholds
are consistent — carry it as a gate-1 tunable exactly like D-13c. The precise frozen band label stays
on the chip's `title` and on the methods page.

**Novelty is a prominent switch, first in the filter bar**, voiced under an explicit candidacy hedge:

| Element | Wording |
|---|---|
| Toggle | **Candidates for new finds** / מועמדים לממצאים חדשים |
| Row badge | **Candidate new find** / מועמד לממצא חדש |
| Sub-line | *Findings you would not reach by searching the catalogues.* |
| **(?)** | no finding aid we checked records this work on this fragment; the checked list is fixed and dated (FJMS + NLI catalogues and bibliography, titles, PGP, FGP, shelfmark attributions); **the identification itself is an unreviewed algorithmic match, so this is a candidate, not a confirmed find** |

The earlier draft ("Not found in the finding aids checked") was accurate but so hedged that readers
did not register this as the significant material. The line deliberately **not** crossed is "new
discovery" / "likely new find", which was offered and declined: that stacks two unearned claims — that
the match is *correct* and that it is *new* — on a row with no human review until Phase 137. A wrong
match that no catalogue records is not a discovery. "Candidates for new finds" asserts only candidacy,
which is what the data supports.

**Novelty never sorts.** The sort dropdown offers only tier / pages / matched text. D-15a and D-24
prohibit novelty feeding rank or styling, because absence from a finding aid is not evidence a match is
correct.

**Domain / author / work facet cascade, mirroring `/catalog-browse`** (`fjms.get_browse_authors(domain)`
→ `get_browse_works(domain, author)`): a collapsible domain tree, then author narrowed by domain, then
work narrowed by domain + author.

**Domain is the domain of the IDENTIFIED WORK, never the manuscript's catalogue domain.** The
manuscript route was tempting — FJMS `domains` joins on `sys_id == AlmaId` at **83% coverage** (37,027
of 44,375 findings-bearing manuscripts) with zero new work — but it is the wrong axis and actively
harmful: **Moss. V,374 is catalogued *Court Documents / Court Records* while carrying a verifiably
correct Rashi-on-Esther finding.** Filtering on manuscript domain would hide exactly the findings that
disagree with the catalogue, which are the most valuable ones. **338 tier-A findings** sit on
manuscripts catalogued documentary/legal.

Only one of the three facets needs new data:

| Facet | Source | Coverage today | Work needed |
|---|---|---|---|
| **Domain** | assign each work a domain from the FJMS vocabulary (**39 parents / 202 leaves**, bilingual) | 0% — `works.genre` exists but is **entirely empty** | **the one-time curation pass**, ~1,088 works carrying shipped claims |
| **Author** | `works.author`, already in the asset | 520 of 1,088 works (48%); only **96 distinct** strings, **81%** matching FJMS `genizah_persons` (38 exact + 40 containment) | small — fill gaps + a 96-row alias map, same pass |
| **Work** | the discovery works themselves (935 distinct titles) | 100% | **none** |

The work facet needing nothing is the key simplification. Bridging discovery titles to FJMS
`genizah_titles` matches only **5%** (55 of 935) and would have been the expensive part — but that
bridge is only needed if the page must speak FJMS's *work* vocabulary. It doesn't: the works being
identified **are** the discovery works.

Feasibility measured on a 93-work sample (`work-domains.sample.json`): **90 of 93 at high confidence
(96%)** in one pass with no external lookups. The three low-confidence cases are a literary letter
collection (Documentary/Letters vs a literary parent), an Arabic Josippon (the vocabulary has no
history leaf), and a kalam-vs-theology judgement. Extrapolated, the full ~1,088-work pass is a bounded
agent task with roughly **3–4% needing a web lookup or an owner ruling**. It surfaces data-quality
fixes for free — the asset attributes *Hovot ha-Levavot* to the wrong Bahya.

Requirements the curation pass must honour:
- assign at the **canonical work** level, so duplicates aren't assigned twice;
- **closed vocabulary** — a leaf outside the FJMS tree is a build error, not a new domain;
- persist as a **curated, hash-pinned artifact** (the shape `v2_canonical_merges` and the
  approved-title list already use), not hand-edited into the DB — consistent with DATA-04's
  fail-closed posture;
- record **per-row confidence and provenance**, so the owner reviews only uncertain rows;
- **Unassigned must be a visible bucket**, not a silent disappearance (FJMS itself has "Unspecified
  Domain", 19,709 rows).

**Modes, not pages.** Phase 138's `/leads` is a *mode* of this page and Phase 137 adds saved judgments,
so the mode strip ships now with **All findings** live and **Screening leads** / **My saved** greyed and
tagged with their phase. 137 and 138 then add a tab rather than a page, and one filter/sort/paging
implementation serves all three.

**Gated like `/atlas`** — availability predicate ANDed with the flag, not the flag alone. In the
`absent` state the **nav entry disappears entirely**; the page does not render empty.

Filters compose as AND; empty set = all; default sort is tier-first; rows stack below 700px.

## ⚠ What the page is blocked on

This is the load-bearing finding: the findings page is **blocked on** gates 1–3, not merely improved by
them.

**1. The novelty axis cannot honestly ship on the current asset.**

| | rows |
|---|---|
| `propagated`, novelty computed = flagged | 14,003 |
| `propagated`, novelty computed = not flagged | 8,240 |
| **`track1_direct`, `is_new = 0` — meaning UNCHECKED** | **144,294** |

A two-state novelty filter over that data would tell a reader that 144,294 findings are *already
recorded in the finding aids*. That is false — they were never checked. So direct rows read **"not
checked"** pre-rebuild and the novelty filter group renders visibly disabled and tagged *needs the
rebuild*. This is the concrete argument for D-23a's tri-state
(`known` / `not_found` / `indeterminate`, fail-closed) being load-bearing rather than bookkeeping.

**2. `coverage_ppm` and `band_rank` do not exist as columns** (verified by `PRAGMA table_info`). The
coverage filter is therefore inert — rendered, disabled, tagged.

**3. PERF-01, independently confirmed.** D-10a measured 3.41–3.55 s for a representative
novelty/tier/coverage ordering against a 1.5 s cap. Separately, the deduped identification **count**
for the default unit took **16 s**. A visible real total is not free — it needs materialized keys and
indexes, or a cached/approximate count with honest wording.

## Amendments owed

| Doc | Change |
|---|---|
| `discovery-band-labels-v1.md` §2 | Collapsing seven frozen `(family, band)` display labels into three user-facing confidence levels is a display-contract change and needs a dated amendment. **Write it system-wide** — the owner ruled 2026-07-31 that the scale applies to the panel too, so the frozen labels become tooltip-only on every surface. **§4 (default visibility) and BAND-03 (screening routing) are unaffected** — bands still decide gating and bucket placement; only the visible label changes |
| **NOVEL-01 / D-23b** | D-23b currently mandates "Not found in the finding aids checked" and prohibits "new" outright; the shipped wording uses "new finds" under a candidacy hedge. The amendment must record the *candidate ≠ discovery* reasoning on the record, not in a commit message |

## CSS Patterns

```css
/* the novelty switch is FIRST in the filter bar regardless of DOM order */
.fg.novgrp { order: -1; }

/* a filter group that is rendered but blocked on the rebuild —
   dashed + dimmed + an amber "needs the rebuild" tag, never silently absent */
.fg.blocked { opacity: .55; }
.fg.blocked .fchip { cursor: not-allowed; border-style: dashed; }
.fg.blocked .fchip:hover { border-color: var(--border-medium); }
.needs {
  font-size: 9px; text-transform: uppercase; letter-spacing: .03em;
  background: var(--accent-amber); color: var(--text-inverse);
  border-radius: var(--radius-full); padding: 1px 6px;
}

/* confidence chips — the ONLY place tier-ish colour is allowed, because it is a
   FILTER control, not row styling (D-24 prohibits per-tier ROW styling) */
.fchip.conf.strong { border-color: var(--primary-600); color: var(--primary-700); }
.fchip.conf.strong.on { background: var(--primary-600); color: #fff; }
.fchip.conf.medium { border-color: var(--accent-amber); color: var(--accent-amber); }
.fchip.conf.medium.on { background: var(--accent-amber); color: var(--text-inverse); }
.fchip.conf.weak { border-color: var(--border-medium); color: var(--text-muted); }
.fchip.conf.weak.on { background: var(--text-muted); color: var(--bg-card); }

/* novelty badge: solid presence for a candidate, italic-muted for the other two
   verdicts. This IS the prominence — see "What to Avoid" below. */
.nov {
  font-size: 10px; font-weight: 700; padding: 1px 7px; border-radius: var(--radius-full);
  border: 1px solid var(--primary-600); color: var(--primary-700); background: var(--bg-active);
}
.nov.unknown {
  border-color: var(--border-medium); color: var(--text-muted);
  background: var(--bg-tertiary); font-weight: 400; font-style: italic;
}

/* the permanent caveat slot — gold rule, RTL-safe */
.phead .caveat {
  background: var(--bg-tertiary);
  border-inline-start: 3px solid var(--accent-gold);
}

/* domain tree: leaf indent and the count must both flip for RTL */
.dnode.leaf { padding-inline-start: 22px; }
.dnode .c   { margin-inline-start: auto; }

/* future modes are visible but inert, tagged with their phase */
.mode.future { opacity: .5; cursor: not-allowed; }

@media (max-width: 700px) {
  .row { flex-direction: column; gap: 6px; }
  .row .side { text-align: start; align-items: flex-start; }
}
```

Every directional property is logical (`-inline-start`, `text-align: start/end`) — the page renders in
both directions.

## HTML Structures

Page shell, top to bottom: **appbar** (nav; the findings entry is `.cur .beta`) → **`.phead`** (h1 +
`.sub` + the permanent `.caveat` slot) → **`.modes`** strip → **`.fbar`** filter bar → **`.rbar`**
result bar (count · "Show as" · sort) → **`.rows`** → **`.pager`** (labelled *(sample)*).

Row anatomy for the default unit: work title link → `.r-sub` with library + shelfmark link + author →
`.r-meta` with **confidence chip (band label on `title`) → novelty chip → pages → matched letters** →
`.side` actions (open manuscript / open work), which move below the row under 700px.

The confidence chip carries the frozen band label as its tooltip:
`<span class="band conf-${confOf(r)}" title="${bandLbl(r.band)}">`. That is how the display-contract
change stays reversible — the precise label is one hover away, never lost.

Classification is a pure function worth lifting verbatim:

```js
const LONG_CITATION = 200;                                   // gate-1 tunable, cf. D-13c's 150
const STRONG_BANDS = new Set(['tier_a', 'high_confidence_algorithmic']);
function confOf(r) {
  if (r.ctype === 'direct_witness' && STRONG_BANDS.has(r.band)) return 'strong';
  if (r.ctype === 'quotes_this_work' && (r.maxLetters || 0) >= LONG_CITATION) return 'medium';
  return 'weak';
}

function novOf(r) {                    // fail-closed: unchecked is NOT "known"
  if (REBUILT) return r.isNew === 1 ? 'not_found'
             : (r.isNew === 0 && r.src === 'propagated' ? 'known' : 'not_found');
  if (r.src === 'propagated') return r.isNew === 1 ? 'not_found' : 'known';
  return 'indeterminate';              // the 144,294 direct rows
}
```

## What to Avoid

- **A two-state novelty filter before the rebuild.** It asserts 144,294 unchecked findings are already
  recorded. This is not a nice-to-have data gap; it is the difference between an honest filter and a
  false claim on the flagship surface.
- **Deriving the confidence scale from bands.** It orphans 20,435 never-assessed rows and forces a
  fourth level. Derive it from the relation kind.
- **"New discovery" / "likely new find" / novelty as a sort key or a row style.** D-23b, D-15a, D-24.
- **Offering novelty on the per-work unit.** A work spanning many manuscripts has no single verdict.
- **Filtering by the manuscript's catalogue domain.** It hides the findings that disagree with the
  catalogue — the valuable ones.
- **Rendering the page empty in the `absent` state.** The nav entry must be gone.
- **A silent "unassigned" domain bucket.** Works the vocabulary can't place must remain visible.
- **Negated use of prohibited wording.** The first draft of the page caveat read *"a match is not proof
  that a folio is a copy of the work"* — the suite failed it, because D-21 prohibits "copy of" on
  display surfaces flatly and a grep-based CI guard cannot see the negation. Reworded to *"a text match
  is not by itself proof of identity."* The violation was in hand-written prose, not in data, which is
  exactly where these rules get broken.

## Known gap between prose and implementation

The sketch README claims candidate rows "carry an accent rule so significance lands before the words
do." **There is no row-level accent rule in the CSS** — no `:has()` selector, no row border. The
prominence that actually ships is (a) `.fg.novgrp { order: -1 }` putting the switch first in the filter
bar and (b) the solid-primary `.nov` badge against italic-muted `.nov.unknown`. If a row-level accent is
wanted, it still has to be built, and it needs a D-24 check first — a row treatment keyed on novelty is
close to the styling D-24 prohibits, even though D-24 names confidence rather than novelty.

## Verification

`node` smoke over 3 units × 4 states × 2 languages × 3 nav labels × 2 rebuild states —
**160 assertions**. Beyond the shared prohibited-wording invariants: the real totals are the ones
surfaced; **before the rebuild no direct row claims any novelty verdict**; `coverage_ppm` / `band_rank`
are confirmed absent so the disabled filters' premise holds; the nav entry is gone in `absent`; novelty
is not a sort option; the domain filter narrows and a leaf narrows further than its parent; the author
list is cross-filtered by domain and the work list by domain + author; **every domain assignment falls
inside the FJMS vocabulary tree**.

Two positive controls. The first seeds "New discovery — precision 0.9382" → 162 failures. The second
seeds an out-of-vocabulary domain plus a facet header mislabelled as the *manuscript's* domain — and it
earned its keep immediately: the header assertion originally tested the whole rendered page and
**passed while the header was wrong**, because the design-note prose also contains the phrase
"identified work". It now scopes to the facet header. *An assertion that can pass for the wrong reason
is worse than none.*

## Origin

Sketch 003. Source in `sources/003-discovery-findings-page/` — `data.js` (real totals, tier facet
counts, novelty state and bounded row samples from `discovery-v1-33499c5b`), `work-domains.js` +
`work-domains.sample.json` (the 93-work domain feasibility sample).
