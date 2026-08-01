# Corpus-wide Findings Page

Validated in sketch 003 against real totals and real sample rows from the deployed
`discovery-v1-33499c5b` asset, in three production themes, EN + HE RTL, at 375/768/full width, across
4 service states and 2 rebuild states. This is the page D-19 asked for a mockup of, and the surface
carrying the owner's rationale — *"a big new amazing feature… maximum ability to see new findings."*

> **Superseded in one place.** Sketch 003 shipped a three-level confidence scale (Strong / Medium /
> Weak). The owner replaced it on 2026-08-01 with **two buckets — "main pool" / "more matches"** — after
> a check against the live asset showed the three-level rule was an invented duplicate of a rule the
> codebase already has. See *Two buckets* below. The sketch HTML still shows the old scale; read it for
> layout, not for the confidence model.

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
| per **identification** (manuscript × work) | **65,200** | **default.** The only unit where tier, coverage, novelty and the future vote all attach to exactly the thing on the line |
| per **manuscript** | 44,375 | **9,806 carry more than one work**, so a novelty verdict on the row is ambiguous — novel *how*? Rows carry an inline annotation wherever this bites |
| per **work** | 1,088 | browsable, but the individual find is hidden and giant works dominate by size. Novelty is not offered on this unit at all |

The default is not arbitrary. Counting per **page** inflates same-work matches ~2.3× relative to
citations, because a fragment that copies a work matches on every folio while a citation matches once:

| kind | avg pages per identification | share per page | share per identification |
|---|---|---|---|
| same work | **2.71** | 88.4% | **77.8%** |
| quotes | 1.2 | 4.4% | 8.8% |
| shared wording | 1.27 | 7.2% | 13.4% |

The effect is real but bounded, because a Genizah "manuscript" here is a **fragment**: median 2 pages,
86.5% at one or two, only 0.9% above fifty (largest 427). Per-identification is the honest unit and now
has a number behind it.

## Two buckets — main pool / more matches

**Replaces the three-level confidence scale.** Two buckets, split by the rule that already exists in
`shared/discovery_band_labels.py::is_default_eligible()`:

| bucket | tooltip | what's in it |
|---|---|---|
| **Main pool** | *best pool for same-work identification* | default-shown claims — **92.4% same-work** |
| **More matches** | *lower-confidence and ungraded matches* | screening bands + never-evaluated — 48.2% same-work, 40.3% shared wording, 11.5% quotes |

**Do not invent a rule.** `is_default_eligible(evidence_source, confidence_band, adjudication_status,
routing_status, measurement_status, ci_low)` is the authority. It already implements §4 of
`discovery-band-labels-v1.md` plus the D-18 fail-closed gate, and it already returns the right answer
for every band. Sketch 003's `confOf()` with its hand-picked
`STRONG_BANDS = {tier_a, high_confidence_algorithmic}` was a second, disagreeing implementation of the
same idea — **delete it**, along with `LONG_CITATION`, the three level labels and the confidence chips.

Why the invented rule was wrong, concretely: `is_default_eligible` returns **True** for `corroborated`
and `weak` (propagated witnesses), because those are already-shipped measured bands. `STRONG_BANDS`
excluded them, so the population with the **highest measured precision in the system** (0.926 [0.875,
0.968]) rendered as "Weak" beside 0.647 screening rows. Using the existing rule, that cannot happen.
The bug is not fixed — it stops existing.

A related trap: that 0.926 is measured over `corroborated ∪ weak` **jointly**, and the asset's own note
forbids splitting it — *"NEVER a corroborated-only (81/86) or weak-only (95/104) split."* The two bands
move together or not at all. There is no narrow fix available.

**The second tooltip must not say "mostly citations and shared texts."** Measured, the overflow's
largest single group is same-work claims (48.2%) — it is not a different *kind* of match, it is the
same kinds at lower grading quality. Pre-rebuild it is 88.3% same-work, which would make that wording
flatly false.

### Bucket sizes — the tier_a grade is a hard dependency

| | main pool | more matches |
|---|---|---|
| **today**, per identification | **2,241** | 62,959 |
| **after the rebuild** | **46,644** | 18,556 |

(out of 65,200 identifications; page-level equivalents are 2,660 / 163,877 → 137,109 / 29,428.)

The gap is one row of data. `tier_a` — **134,449 claims, 81% of the corpus** — carries
`precision=NULL, ci_low=NULL, measurement_status=NULL` in the deployed asset's `band_precision` table,
so `band_measurement_status()` reads `not_measured` and the D-18 gate fails closed. Its note is
deliberate: *"tier_a carries NO measured precision in the frozen contract — NEVER a fabricated number
in a real/release build."*

**The grading exists.** CERT-01 passed 2026-07-28 at a weighted 0.9382 against a 0.85 floor — but into
the **v2** asset, which is deployed flag-OFF. The live v1 asset was never updated. So this is a data
carry-over at the v2 bake (`measurement_status='measured_pass'` + a real `ci_low ≥ 0.85`), not a
measurement, not a design decision. **Until it lands, the surface shows 2,241 of 65,200 and is not
worth shipping.** Add it to the rebuild's list beside novelty, `coverage_ppm` and `band_rank`.

### What the two buckets change on this page

1. **The confidence filter group is deleted** — three chips and their counts go. The bucket is not a
   filter; it is a default plus a "show more matches" toggle, the same shape the panel already uses.
2. **The default result count changes** from 65,200 to the main pool (46,644 post-rebuild). Say so in
   the result bar rather than silently narrowing.
3. **This page now needs the relation filter the panel has** — "Direct match / Partial match / Shared
   text". Removing the confidence chips otherwise leaves *no* way to filter by kind of match, and at
   78 / 9 / 13 that is a filter readers will want. It is also genuinely orthogonal to the bucket
   (kind vs grading quality), which the deleted chips were not.
4. **The row chip becomes a relation chip, not a confidence chip.** Which bucket a row is in is
   positional; what a reader needs on the line is what kind of match it is. Band label stays on hover.
5. **`LONG_CITATION = 200` disappears** as a tunable — it existed only to define the Medium level.

**A plain three-level confidence scale** is therefore no longer part of this design. The reasoning that
produced it still holds and is worth keeping: a band-derived scale would have orphaned 20,435
never-assessed rows and forced an awkward fourth "not assessed" level. Two buckets avoid that by not
grading rows at all — they say *which pool this came from*, which is the only claim the data supports.
Per `discovery-band-labels-v1.md` §3, band precisions are "estimated band-**population** precisions, not
per-item probabilities", and applying one to a row as its confidence is explicitly forbidden. "Best pool
for same-work identification" is a population claim, which is why it is honest.

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

Note that "Screening leads" is now nameable precisely: it is the **more matches** bucket. Phase 138's
mode and the "show more" toggle are the same population viewed two ways — decide at gate 1 whether
both exist.

**Gated like `/atlas`** — availability predicate ANDed with the flag, not the flag alone. In the
`absent` state the **nav entry disappears entirely**; the page does not render empty.

Filters compose as AND; empty set = all; default sort is tier-first; rows stack below 700px.

## ⚠ What the page is blocked on

The findings page is **blocked on** the rebuild, not merely improved by it. Four items, all data:

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

**3. The `tier_a` grade is not in `band_precision`** (see *Bucket sizes* above) — without it the main
pool is 2,241 identifications instead of 46,644.

**4. PERF-01, independently confirmed.** D-10a measured 3.41–3.55 s for a representative
novelty/tier/coverage ordering against a 1.5 s cap. Separately, the deduped identification **count**
took **16 s**. A visible real total is not free — it needs materialized keys and indexes, or a
cached/approximate count with honest wording.

## Two data quirks worth knowing

- **The 11,941 "shared wording" claims have no `matched_letters` value at all** — zero of them. Any row
  layout that promises "N matched letters" has nothing to show on those rows.
- **`not_evaluated` is labelled "Shared text" but 5,604 of its claims carry
  `claim_type='direct_witness'`.** The band name and the relation disagree, so a UI section built on the
  band name will contain same-work claims.

## Amendments owed

| Doc | Change |
|---|---|
| `discovery-band-labels-v1.md` §2 | **Much smaller than it was.** Band labels become tooltip-only; the visible split is the §4 default-shown boundary, which §4 already defines. No new display vocabulary is introduced, so this is a note rather than a contract rewrite. §4 and BAND-03 unaffected |
| **NOVEL-01 / D-23b** | D-23b mandates "Not found in the finding aids checked" and prohibits "new"; the shipped wording uses "new finds" under a candidacy hedge. The amendment must record the *candidate ≠ discovery* reasoning on the record, not in a commit message |
| **D-16 / PANEL-01** | This page needs the relation filter currently specified only for the panel |

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

/* relation chip on a row — neutral by design. Do NOT colour-code it by kind:
   that reintroduces per-tier styling through the back door (D-24). */
.rel {
  font-size: 10px; padding: 1px 7px; border-radius: var(--radius-full);
  border: 1px solid var(--border-light); background: var(--bg-secondary);
  color: var(--text-secondary);
}

/* novelty badge: solid presence for a candidate, italic-muted otherwise */
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

The deleted `.fchip.conf.strong/.medium/.weak` rules are the one part of sketch 003's CSS not to copy.

Every directional property is logical (`-inline-start`, `text-align: start/end`) — the page renders in
both directions.

## HTML Structures

Page shell, top to bottom: **appbar** (nav; the findings entry is `.cur .beta`) → **`.phead`** (h1 +
`.sub` + the permanent `.caveat` slot) → **`.modes`** strip → **`.fbar`** filter bar → **`.rbar`**
result bar (count · "Show as" · sort) → **`.rows`** → **"show more matches"** → **`.pager`** (labelled
*(sample)*).

Row anatomy for the default unit: work title link → `.r-sub` with library + shelfmark link + author →
`.r-meta` with **relation chip (band label on `title`) → novelty chip → pages → matched letters** →
`.side` actions (open manuscript / open work), which move below the row under 700px.

The chip carries the frozen band label as its tooltip, which is how the display-contract change stays
reversible — the precise label is one hover away, never lost:

```html
<span class="rel" title="Algorithmic match — tier A">Direct match</span>
```

Bucket membership comes from the shared predicate, never a local reimplementation:

```python
from shared.discovery_band_labels import is_default_eligible

in_main_pool = is_default_eligible(
    row.evidence_source, row.confidence_band, row.adjudication_status,
    row.routing_status, measurement_status, ci_low=ci_low,
)
```

## What to Avoid

- **Writing a second "is this good enough" rule.** `is_default_eligible()` exists and is the contract.
  Sketch 003's `confOf()` is the cautionary example: a hand-picked band set that disagreed with it and
  labelled the best-measured population "Weak".
- **Splitting `corroborated` from `weak`.** The 0.926 is measured over their union and the asset's own
  note forbids the split.
- **"Mostly citations and shared texts" for the second bucket.** Measured, it is 48.2% same-work.
- **A two-state novelty filter before the rebuild.** It asserts 144,294 unchecked findings are already
  recorded — the difference between an honest filter and a false claim on the flagship surface.
- **Per-row precision language of any kind.** §3 forbids applying a band's population estimate to a row.
  Bucket names are population claims, which is exactly why they are allowed.
- **"New discovery" / "likely new find" / novelty as a sort key or a row style.** D-23b, D-15a, D-24.
- **Offering novelty on the per-work unit.** A work spanning many manuscripts has no single verdict.
- **Filtering by the manuscript's catalogue domain.** It hides the findings that disagree with the
  catalogue — the valuable ones.
- **Rendering the page empty in the `absent` state.** The nav entry must be gone.
- **A silent "unassigned" domain bucket.** Works the vocabulary can't place must remain visible.
- **Negated use of prohibited wording.** The first draft of the page caveat read *"a match is not proof
  that a folio is a *copy of* the work"* — the suite failed it, because D-21 prohibits "copy of" on
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

Add one assertion when the buckets are built: **bucket membership must equal
`is_default_eligible()`** for every row, so a future local reimplementation fails the suite rather than
silently diverging. That is the class of bug `confOf()` was.

Two positive controls. The first seeds "New discovery — precision 0.9382" → 162 failures. The second
seeds an out-of-vocabulary domain plus a facet header mislabelled as the *manuscript's* domain — and it
earned its keep immediately: the header assertion originally tested the whole rendered page and
**passed while the header was wrong**, because the design-note prose also contains the phrase
"identified work". It now scopes to the facet header. *An assertion that can pass for the wrong reason
is worse than none.*

## Origin

Sketch 003, plus a 2026-08-01 verification pass against the deployed asset that replaced its confidence
model. Source in `sources/003-discovery-findings-page/` — `data.js` (real totals, tier facet counts,
novelty state and bounded row samples from `discovery-v1-33499c5b`), `work-domains.js` +
`work-domains.sample.json` (the 93-work domain feasibility sample).
