# The Main-Pool Rule — how "probably the identification" is decided

Owner decision, 2026-08-01. This is the rule behind the **two buckets** on every discovery surface
(the browse-page panel and the corpus-wide findings page). Designed and measured against the deployed
`discovery-v1-33499c5b` asset, cross-reviewed by Codex.

**Goal, in the owner's words:** the main pool should be *"most probably the identification of the
work"*; "show more" is *"more possible results, perhaps citations, shared texts etc."* Heuristics are
explicitly acceptable — *"something like that, not precise"*. This is a **disclosure split for readers,
not a scientific claim.**

## The rule

Unit is the **identification** (manuscript × canonical work) — 64,509 of them. All of an
identification's page-claims travel together. Evaluate in order; **any** gate sends it to *show more*:

| # | Gate | Identifications caught |
|---|---|---|
| 1 | No same-work claim at all (only quotes / shared wording) | 10,302 |
| 2 | Best band is only `screening_canon` or `weak` | 5,620 |
| 3 | Unresolved competition on **every** matched page (an overlapping near-tie span from another canonical work, or a `kept_tie` page) | 3,714 |
| 4 | **Single-page** identification with page coverage **< 0.8** | 8,721 |

`adjudication_status='human_confirmed'` is always Main (99 rows), ahead of every gate.

**Result: Main 36,152 (56%) · Show more 28,357 (44%.)** In claim rows: 132,503 / 34,034.

Sentence for a scholar, and for the UI's methods page:

> A fragment is treated as a probable identification when it matches the work **across more than one
> leaf**, or **covers almost a whole page** on its own. Everything else appears under "more matches".

Two routes into Main, deliberately: **24,046** by multi-folio agreement, **12,007** by single-page full
coverage.

**Do not use a weighted score.** Codex's argument, adopted: a scored sum lets three pages of boilerplate
outvote an unresolved competitor, and the weights cannot be explained honestly. Non-compensating floors
in a fixed order can.

## Why these signals — measured, not argued

Validated by joining the four human grading files (`discovery_data/track1_id_grades (1-4).json`) to
shipped identifications: **211 graded pairs**, grades collapsed to good (correct / co-witness) vs bad
(citation / formula / wrong).

| signal | measurement |
|---|---|
| **Multi-folio agreement** — the strongest free signal | 1 page → 72% good · **2 pages → 96%** · ≥3 → 91%. 47% of same-work identifications have ≥2 pages |
| **Page coverage** | ≥0.8 → 93% good · <0.8 → 73%. Same-work median **0.987**, quotes median **0.519** |
| **The rule as a whole** | 0.92 main precision vs **0.88** for a naive claim-type split |

⚠ **These are design numbers only — they must never reach a surface.** n=211, deliberately
hard-stratified (one file is coverage-stratified by construction), so it is a vibe-check, not a
certified experiment. D-06 forbids precision figures on display regardless.

Codex predicted coverage could no longer subdivide the shipped set, since the bake already routed
everything under 0.45 to `review_only`. **Measured, that prediction is wrong** — inside the surviving
[0.45, 1.0] range the separation is still stark (0.987 vs 0.519). Worth remembering as a case where the
measurement beat the reasoning.

## ⚠ `density` is NOT coverage — a documented trap

`discovery_evidence.density` is the **normalized Levenshtein edit-distance** of the alignment (match
quality; lower is better), not page coverage. The repo carries a scar from exactly this confusion —
`scripts/build_discovery_sidecar.py` (the 135-07 field-name-collision fix) warns:

> *"Feeding `density` in as 'coverage' demoted ~100% of witnesses."*

It also fails empirically as a split signal: `direct_witness` averages 0.211 and `quotes_this_work`
0.230, and it does not separate the 0.889-precision band from the 0.647 one. `tier_a`'s maximum is
exactly **0.3500** (the accept cap); the 0.5455 ceiling seen corpus-wide comes only from the three E1
bands under a looser envelope.

**Do not build anything on `density`.** It is the single most inviting wrong turn in this schema, and
it has already been taken once.

## Page coverage: computed at bake, then thrown away

The real metric is `coverage = min(1.0, matched_letters / page_norm_letters)` — space-free Hebrew base
letters (U+05D0–05EA) after NFC. The bake computes it, routes everything **< 0.45** to `review_only`
(`low_coverage`, 108,235 rows), and **does not persist it**. There is no coverage column.

It was recomputed for this analysis from `same_work_spike/probe/data/fullcorpus.db` (`pages` table):
all 138,967 needed pages found, zero drift, only 24 rows dipping below 0.45 on rounding — so the
recomputation matches the bake exactly.

**To ship gate 4 you need `page_norm_letters`.** Cheapest form is a supplemental table of
`page_id → letter count` (~139K integers). It contains **no text**, so it is masking-safe. Alternatively
compute from the web app's own HTR store, accepting snapshot drift (`meta.htr_snapshot_hash` pins the
bake).

## The failure mode this cannot fix — containment

The owner's Birkat Hamazon concern, found in the wild and quantified:

**`משנה תורה, ספר אהבה` carries 2,070 identifications — 7th corpus-wide, above Isaiah** — because the
work contains Rambam's *Seder Tefilot*, i.e. the entire liturgy. Yom-Kippur Amida pages are claimed as
"Sefer Ahava" at 0.94 coverage. Same family: Tur OC (322), Haggadah↔MT (39% manuscript overlap),
Halakhot Gedolot↔Talmud.

**Root cause is not the matcher.** `claim_type` is assigned by within-page span dominance
(`scripts/discovery_ids.py:336-382`), so it defaults to `direct_witness` whenever the **true host work
is absent from the reference corpus** — and the corpus ships only 22 liturgy works. A liturgical
fragment is therefore attributed to whichever code happens to quote the liturgy. This is a
**corpus-coverage problem wearing a classification problem's clothes.**

**Competition signals cannot catch it, measured:** 74% of Sefer Ahava identifications are uncontested,
precisely because the competitor works aren't in the corpus. Only 6.7% of all same-work rows have any
overlapping competitor at all. Extending competition to hidden `review_only` rows plus composition
dates was **inert** — 300 claims affected, because the liturgy works have no dates mapped.

The rule cuts Sefer Ahava by 37% (2,070 → 1,307) and enriches correctly — its show-more pages are 27%
liturgical-looking vs 19% in main (marker classifier; Genesis control reads 0.3%). But **~19% of what
remains in main still looks liturgical**, because full-page Amida text genuinely *is* contained in the
work. **Residue ≈ 1–3% of the main pool.** Accept it, and state it in the methods page rather than
letting a reader discover it.

The structural fix is in the rebuild list below: store **where in the work** the match lands, so an
appendix hit is distinguishable from a body hit.

## Computable today vs the rebuild

**Today:** gates 1–3 read straight from the shipped sidecar. Gate 4 needs only the page-letter-count
table above.

**Rebuild must add** — beyond the already-planned novelty verdict, `coverage_ppm`, `band_rank` and the
`tier_a` grade:

1. **Work-side match offsets** (`w_start`, `w_end` in the work's normalized stream). **Highest-value
   item on this list — see below.** It is the only structural fix for containment (it flags matches
   landing in Sefer Ahava's Seder-Tefilot appendix, or Haggadah's Hallel = Psalms), but the containment
   fix is the smallest of its uses.
2. **`span_competitors`, computed pipeline-side and PRE-shadowing** — the count of other reference works
   matching ≥50% of the same span. The sidecar only ever sees post-router survivors, so honest
   distinctiveness can only be baked, never derived downstream.
3. **Populate `works.genre` and composition year.** Genre exists in the research metadata and is
   entirely NULL in the asset; the missing liturgy dates are what neutralised date-based demotion.
4. **Fix `discovery_routing_audit`:** `kept_tie` rows have a NULL `demoted_work_id`, so tie pairs cannot
   be reconstructed from the audit alone.

Also worth baking (Codex, and there is a working prototype): an **8-gram IDF over the canon corpus**,
scored only over actually-aligned positions, stored compactly (`idf_mean_ppm`,
`common_gram_share_ppm`, `idf_eligible_grams`, a validity code). A tuned prototype with measured
separation on a gold set already exists in the gitignored `same_work_spike/probe/` tree, so this is a
port rather than research. It would not have saved Sefer Ahava — containment is a different problem —
but it is the general boilerplate detector.

**Materialise the bucket and its reason code at bake time and index them.** Recomputing coverage,
competition or manuscript aggregation inside a query that already takes 3.5 s against a 1.5 s budget is
not credible (PERF-01).

### Why "where inside the work" is the highest-value rebuild item (owner, 2026-08-01)

It arrived on the list as a containment fix. It is worth far more than that, and it is **already
computed**: `same_work_spike/probe/scripts/track1_match.py` slices each work into overlapping
`SEG_LEN = 3800` character windows and tracks every window's offset (`seg_off`), with an explicit
comment that *"gram POSITIONS stay original, so span coordinates are unaffected"*. Each hit is
`(work, p0, p1, dens, seg)`. **The work-side position is discarded at ingest** — exactly like page
coverage. Segment-level location (≈ a chapter or two) is nearly free; exact offsets need only retaining
the alignment positions that already exist.

What persisting it unlocks:

- **A citation becomes a reference you can look up.** "Quotes Mishneh Torah" is a note; "quotes Mishneh
  Torah, Laws of Prayer, ch. 4" is a citation. This is the owner's point and it is the main one — for a
  scholar, an unlocatable citation is close to worthless.
- **It turns the second bucket from an apology into a feature.** 28,357 identifications currently read
  as "things we could not confirm". Located, they become a browsable corpus of *quotations with
  addresses* — arguably a product in its own right, and it reframes "more matches" as useful rather
  than residual.
- **Side-by-side evidence.** Today the panel highlights the manuscript text and the reader must trust
  that it matches something. With the work-side position you can show both texts together, which is what
  actually lets a scholar judge a claim. This is the largest single upgrade available to the evidence
  view in `browse-integration-and-highlighting.md`.
- **Join evidence.** Two fragments landing on adjacent stretches of the same work is evidence they come
  from one codex — and tells you how much is missing between them. Feeds the Joins Lab directly.
- **Leaf ordering.** The leaves of a multi-page fragment sort by where they fall in the work, which is a
  standing cataloguing problem.
- **Corpus-level coverage.** "These 40 fragments together preserve 60% of this work, with these gaps" —
  a view of the Genizah that does not exist today.

⚠ **The known cost:** offsets index a *normalized letter stream*, so turning one into "chapter 4"
requires mapping back to the real text. That is the same normalized→raw problem sketch 002 already found
and solved on the manuscript side (see `browse-integration-and-highlighting.md`) — known problem, known
machinery, but it must be budgeted on the work side too.

**Written into the build plan** as `docs/specs/discovery-v2-bake-plan.md` §"Amendment 2026-08-01
(work-side match offsets)", with prioritisation, the existing Sefaria mapping, the coordinate trap and
five gates.

**Staged (owner, 2026-08-01).** Storing an offset and resolving it to a human reference are separate
jobs, and separating them is what makes the staging work:

- **Stage 1 — `w_start`/`w_end` for ALL corpora**, plus reference resolution for **Sefaria only**. Every
  *internal* use (containment detection, shadowing, join sequencing, leaf ordering, work-coverage stats)
  needs the offset and never a reference string — so **the containment fix lands in full at stage 1,
  corpus-wide.**
- **Stage 2 — JA divisions: deferred, may never happen.** JA has no internal division to map, only one
  to recover or invent, so it is deferred rather than sequenced. In stage 1 it renders like M-source:
  position-only, no reference.
- **M-source — no stage.** Masked; offsets stored for internal use, locus never displayed.

### The display asymmetry — plan the UI around it from the start

Public corpora are prioritised because they are where a locus is both most valuable and actually
available. Measured on the deployed asset:

| corpus | works with claims | claims | citation-type claims | locus available? |
|---|---|---|---|---|
| **Sefaria** | 451 | **124,941 (75%)** | **5,474 (74%)** | **yes, stage 1** — 322 `*.versemap.json` sidecars already exist, verse-level, with character offsets |
| JA | 104 | 19,896 (12%) | 539 | **not in stage 1** — the per-document ingest has no internal division at all. Deferred; position-only for now |
| M-source | 533 | 21,700 (13%) | 1,373 | **never** — masked corpus. Offsets stored for containment/shadowing only |

So at stage 1, 75% of claims carry a real reference — but only 42% of **works** ever will, and JA's 12%
stays position-only indefinitely. Every surface showing a locus needs three graceful tiers: **full
reference** ("Laws of Prayer 4:2") · **position only** ("about 40% through the work") · **nothing**
(omit the element; never a placeholder implying a failed lookup). Design all three in from the start —
two of the three are load-bearing on day one, not hypothetical future states.

### Ramifications on current and future work

**Phase 136 (these surfaces).**
- The evidence view can become **side-by-side** — manuscript text beside the matched passage of the
  work. Today the reader sees a highlight and must trust it. This is the largest available upgrade to
  the evidence design in `browse-integration-and-highlighting.md`, and it is **scope beyond D-12**,
  which covers manuscript-side highlighting only.
- Rows can carry a locus chip. Check it against D-21 first — naming *where* a match falls is a fact
  about position, not a claim about the relationship, so it should pass, but confirm rather than assume.
- The "more matches" bucket gains a reason to exist as a destination rather than an overflow.

**Phase 137 (saved judgments).** Side-by-side text makes adjudication far faster and better-grounded,
which feeds directly into the measurement that gates everything else. A reviewer also gains a verdict
the model currently cannot express: *right work, wrong section*.

**Phase 138 (`/leads`).** A lead you can look up is actionable; one you can't mostly isn't.

**Joins Lab (existing product, not in this milestone).** Two fragments landing on **adjacent stretches
of the same work** is join evidence — and it quantifies the gap between them. This is a join-finding
signal that does not exist today, on an already-shipped surface. Worth a seed of its own.

**Browse / reading view.** Leaf ordering within a multi-page fragment falls out for free (sort by work
offset), which is a standing cataloguing problem.

**Corpus-level.** "These 40 fragments together preserve 60% of this work, with gaps here and here" — a
view of what survives that does not exist today.

**A general lesson worth carrying:** sketch 002 found that manuscript-side offsets index a normalized
stream rather than raw text, and now the work side has the identical trap waiting. **Every offset in
this system needs its coordinate space named at the point of definition.** Treat that as a standing
schema rule, not two coincidences.

## How this relates to `is_default_eligible()`

The morning of 2026-08-01 the two buckets were recorded as `is_default_eligible()` — the §4 / D-18
predicate. **That was a placeholder and this rule replaces it.** `is_default_eligible` splits on *was
this band graded*, not on *is this the work*; the two merely correlate.

The relationship now:
- **§4's screening-band exclusion survives** — it is gate 2.
- **D-18's `tier_a` gate is satisfied, not overridden.** CERT-01 passed 2026-07-28 at 0.9382 against a
  0.85 floor; the rule presupposes that grade is carried into the asset, which is a rebuild item already
  on the list. Until it lands, `tier_a` fails closed and the pool collapses to ~2,241.
- **BAND-03 is unaffected.**

## Wording and internal state

- Bucket names follow the owner: **main pool** / **more matches**.
- The second bucket means **"not enough evidence for the main rule"**, never "probably wrong". Codex's
  point, adopted — it holds probable quotations, shared wording, unresolved ties, missing signals and
  genuinely indeterminate cases alike. If the shipped label risks reading as a verdict, Codex's
  alternative *"other possible connections"* is the better phrasing.
- **Keep reason codes internally** even though only two buckets are visible: `shared_wording`,
  `overlapping_tie`, `low_coverage`, `insufficient_length`, `missing_signal`. They can drive factual
  badges later without ever implying a confidence level. A visible third "uncertain" bucket was
  considered and rejected — a reader reads it as a calibrated class, which the data cannot support.
- **No per-row probability, ever.** Bucket *names* may carry a population-level claim ("best pool for
  same-work identification"); rows may not.

## Before shipping

**Do not freeze the thresholds on these numbers.** Both reviewers said so independently. Review ~300
stratified cases by hand — coverage bands, single vs multi-page, ties, short matches, genres — before
0.8 becomes a constant. For reference, 0.7 yields main = 38,030 at 0.91 precision; the 0.7–0.8 grade bin
measured only 62% good, which is what argues for 0.8.

Adversarial set worth building (Codex's list, trimmed to what applies here): anthology vs constituent
work; nested works (Tur / Mishneh Torah / prayer book); very short pages (incipits, colophons — the
"100% of almost nothing" problem); incomplete HTR; large boilerplate blocks at 500 / 900 / 1,500
letters; multi-register pages (Bible + Targum verse-by-verse, both correct, non-overlapping — the spec
insists these co-exist); `n_spans > 1` as a valid alternating register rather than weak evidence; short
complete works; and library incompleteness — remove known competitors and check that low competition
alone does not promote generic text.

## Known limitations, stated plainly

- **Containment residue** ≈ 1–3% of the main pool (§ above). No sidecar-computable signal fixes it.
- **"Multi-folio" at 2 pages is usually recto + verso of one leaf**, not two independent leaves — yet it
  still measured 96% good.
- **Gate 2 splits `weak` from `corroborated`** although the 0.926 measurement covers their union only.
  Splitting is the conservative direction, but it is a product choice, not a measured one.
- **Synoptic overlaps** (Psalms ↔ Chronicles) can leave both works in main when spans don't near-tie.
- **Composition dates cannot resolve identity.** Codex's point: dates can demote an implausible
  direction of borrowing, but a late manuscript carrying an early passage may still witness the later
  compilation. Containment needs a work-relation graph or must stay unresolved.

## Origin

Owner decision 2026-08-01 following a measured design pass (Fable, 41 tool calls against the live asset)
and an independent review (Codex). Both independently rejected `density`, both landed on coverage +
multi-folio + a decision list, and both recommended two visible buckets with internal reason codes.
Analysis scripts `q1`–`q9` in the session scratchpad; key sources
`scripts/build_discovery_sidecar.py` (coverage / Lever-1, lines 489–567 and 810–864) and
`scripts/discovery_ids.py:336-382` (the `claim_type` span-dominance default).
