# Phase 136 Plan 03 -- Gate-1 Decision Evidence

Measured read-only against the deployed asset `discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff.db` (the LIVE v2 sidecar -- the trimmed rebuild has not run). Re-running `scripts/discovery_gate1_evidence.py` against the same file reproduces every number below exactly (no sampling, no randomness).

**Population note.** The main-pool-rule classification below is computed over "usable" claims: a claim's display evidence has `routing_status='shipped'`, OR any evidence row for the claim is `adjudication_status='human_confirmed'` (the D-13g fix folded in, so a human-confirmed row is never invisible to this measurement merely because routing demoted it). This reproduces **64,522** identifications, main **35,453** / show-more **29,069** -- within ~2% of the design-pass figures already on record in `main-pool-rule.md` (36,152 / 28,357), which is the expected order of agreement given this pass implements gate 3 slightly differently (see the D-13e section below) and a slightly different human-confirmed rule.

Gate breakdown (identification count by classification reason):

| Reason | Bucket | Count |
|---|---|---|
| gate1_no_same_work_claim | show_more | 10,300 |
| gate2_best_band_weak | show_more | 5,754 |
| gate3_unresolved_competition | show_more | 2,530 |
| gate4_low_single_page_coverage | show_more | 8,230 |
| gate4_no_coverage_data | show_more | 2,255 |
| gate4_single_page_full_coverage | main | 9,120 |
| human_confirmed_override | main | 118 |
| multi_folio_agreement | main | 26,215 |

## D-13e -- does the middle "Also shares text with" bucket survive as a THIRD level?

- D-13d generic identical-span-group claims (>=2 DIFFERENT canonical works on one byte-identical span): **3,218**.
  - Of those, **980** belong to an identification that this pass classifies MAIN under the main-pool rule -- i.e. NOT otherwise reachable via "show more matches" (that toggle only ever renders show-more-classified identifications).
  - The remaining **2,238** belong to an identification ALREADY classified show-more -- these rows are already reachable via "show more matches"; giving them a second, separate middle-bucket home would be a duplicate view.
- Related-pages population (D-11a, shared-text page relations -- these never map onto a WORK identification at all, so they can never be reached via the per-work "show more matches" toggle): **37,397** directed (anchor, opposite) page pairs. All of it is, by construction, not otherwise reachable.

**Total middle-bucket population: 40,615.** **Not otherwise reachable: 38,377** (94.5% of the middle bucket). **Overlap with "show more matches": 2,238** (5.5%).

**Methodology note (gate 3):** this pass implements "unresolved competition" as EITHER a `discovery_routing_audit` `kept_tie` page (direct, non-heuristic) OR an overlapping, near-equal-length competing span from another canonical work on the same page (overlap >= 70% of the shorter span AND a length ratio >= 0.7 -- a stated, reproducible threshold). `demoted_work_id` is NULL on every `kept_tie` row in this asset (a known, already-documented flaw -- D-02b), so the audit table alone cannot fully reconstruct every tie; the near-tie span test is this pass's way of recovering most of the gap, and is the main source of any remaining difference from the design-pass figures.

**Question for the owner:** given the numbers above, does the panel keep a distinct THIRD disclosure level ("Also shares text with"), or does it collapse into "more matches"? No recommendation is made here -- D-13e is open by design.

## D-16 / PANEL-01 -- does the findings page also get the relation filter?

Relation distribution (claim_type), corpus-wide (all claims, any routing status):

| Relation | Count |
|---|---|
| direct_witness | 197,177 |
| quotes_this_work | 59,243 |
| shared_text | 11,941 |

Relation distribution restricted to SHIPPED display claims:

| Relation | Count |
|---|---|
| direct_witness | 147,210 |
| quotes_this_work | 7,386 |
| shared_text | 11,941 |

Relation distribution restricted to this pass's MAIN pool (identifications classified `main`):

| Relation | Count |
|---|---|
| direct_witness | 141,958 |
| quotes_this_work | 11,286 |
| shared_text | 3,589 |

**Question for the owner:** does a relation filter on the main pool meaningfully narrow the default view, or does it mostly restate the bucket (since the main pool is already overwhelmingly `direct_witness`)? No recommendation is made here -- D-16 is open by design.

## D-13c -- the short-evidence threshold

Thinnest shipped direct match in the whole asset: **37 matched letters**.

Cumulative row counts below each candidate threshold:

| Threshold (matched letters) | Direct family (of 144,294) | Propagated family (of 40,968) |
|---|---|---|
| < 50 | 10 (0.0%) | 2 (0.0%) |
| < 100 | 1,720 (1.2%) | 3,024 (7.4%) |
| < 150 | 6,558 (4.5%) | 6,497 (15.9%) |
| < 200 | 13,660 (9.5%) | 9,769 (23.8%) |

**Methodology note (propagated family):** the propagated family's length metric is `aligned_len` on shipped `shared_text` evidence rows (propagated `witness`-kind rows -- `corroborated`/`weak` -- carry no length field in this asset at all). This is a slightly different population than an earlier design-pass count (which counted DISPLAY claims rather than evidence rows); the counting unit is stated here so the two are never silently conflated.

Short direct rows (< 150 matched letters) that are nonetheless part of a MAIN identification via multi-folio agreement (the honest counter-argument the owner already accepted -- for a prayer book, a short liturgical passage may be exactly the correct identification): **8,457**.

**Question for the owner:** what is the short-evidence threshold, in matched letters? A defensible default exists: **150** (the figure the owner has already reviewed counts against, per `main-pool-rule.md` / `136-CONTEXT.md` D-13c) -- kept as the recommended default unless the table above changes the owner's mind.

## D-13b -- the lead-attribution tie-break

Identical-span groups (>=2 shipped direct claims on one byte-identical span): **1,553** groups / **3,590** claims.
Of those, **1,542** groups (99.3%) are STILL tied after ordering by band rank alone (**3,567** claims involved) -- band rank alone cannot pick a lead attribution for the overwhelming majority of these groups.

**Question for the owner:** what breaks a tie after band rank when several works claim one passage? A defensible default exists: fall back to the existing TOTAL claim ordering already used elsewhere in the build (`discovery_ids.py`'s `evidence_id` lexicographic tie-break) -- deterministic, already in the codebase, and requires no new concept.

## D-13d -- the granularity separation rule (KNOWN FLAW)

Identical-span groups with >=2 DIFFERENT canonical works: **1,367** groups / **3,218** claims.

**Worked example** (T-S Misc. 12.31.14):

- Page: `990051079570205171_IE158601508_P000002_FL158601518` (sys_id `990051079570205171`)
- Span: offsets 0-962 (962 matched letters)
  - `w000171` (canonical `w000171`): **רש"י על התורה** -- author: שלמה בן יצחק (רש״י)
  - `w001281` (canonical `w001281`): **רש"י על בראשית** -- author: שלמה בן יצחק (רש״י)

Both works share the same author and a common title prefix ("<author> on ...") -- the same underlying commentary recorded at two catalogued granularities (a general work covering the whole Torah, and a specific work covering only Genesis), carrying DIFFERENT `canonical_work_id`s. Under the CURRENT rule (D-13d as originally stated) this whole pair is swept into the generic "also shares text with" bucket and neither title renders as a stand-alone identification for this page, even though the two titles denote a real, nameable commentary.

**Proposed separation rule** (display-time only, not a data fix): treat two works in an identical-span group as the SAME work at different granularity (collapse like a duplicate, per D-13a) when they share a non-null `author` field AND EITHER their normalized titles are identical (an undetected alias) OR share a >= 4-character normalized-title prefix (e.g. a common "<author> on ..." commentary marker). Groups where no such pair exists remain genuinely generic shared text.

**Measured effect:** of the 1,367 different-canonical-work identical-span groups, **276** groups (20.2%, 558 claims) contain a same-author/related-title pair and are candidates for the collapse rule; **1,091** groups (79.8%, 2,660 claims) contain no such pair and remain genuinely generic shared text under the proposed rule.

**Question for the owner:** does this separation rule (same author + identical/prefix-shared normalized title) correctly draw the line? A defensible default exists: adopt it as stated, since it is conservative (author-gated, so it never over-collapses the large generic-collection-title clusters measured separately in the novelty hard-case selection below) and directly resolves the worked example.

