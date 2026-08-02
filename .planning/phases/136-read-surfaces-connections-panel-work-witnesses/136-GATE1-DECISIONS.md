# Phase 136 Plan 03 -- Gate-1 Ratified Decisions

**Status of this record.** The owner ruled on every wave-1 decision (groups A-D below) in one
sitting, recorded verbatim here with its supporting measured number, per `136-GATE1-EVIDENCE.md`
and `136-NOVELTY-HARDCASES.md` (Task 1). These rulings are **LOCKED** -- not re-litigated,
re-derived or "improved" here. **Recorded:** 2026-08-02.

This file currently closes groups A, B, C and D of the plan's Task 2 checkpoint. It does **NOT**
yet close plan Task 4 in full: Task 4 also writes `discovery_data/novelty_hardcase_labels-v1.json`
(the owner-supplied per-case verdicts) and its content hash, and that file cannot exist until
Task 3 -- the owner labelling checkpoint -- has actually run. Task 3 is presented as a separate,
still-open `checkpoint:human-action` (see the executor's completion report). **The label file, its
content hash, and the corresponding entry in this record are PENDING and will be appended once
Task 3 returns.**

**Addendum, 2026-08-02 (same day, later dispatch).** The owner issued ONE more ruling after A-D
above -- **Decision E**, amending the locked D-23a tri-state and NOVEL-01 to a seven-value shade
enum. See **section E** below, appended after section D. This does NOT reopen A-D; those remain
LOCKED exactly as recorded above. Decision E was first attempted through an inter-agent side
channel and correctly refused as a possible injection before being re-delivered through a normal
orchestrator dispatch -- see section E's own Provenance note for the full account.

---

## A. The five gate-1 decisions

### D-13e -- does the middle "Also shares text with" bucket survive as a THIRD disclosure level?

- **Question:** does the panel keep a distinct THIRD disclosure level ("Also shares text with"), or
  does it collapse into "more matches"?
- **Measured number** (`136-GATE1-EVIDENCE.md`): total middle-bucket population **40,615**; of
  those, **38,377** (94.5%) are not otherwise reachable via "show more matches" -- chiefly the
  **37,397** related-page instances, which are not work identifications at all and have no other
  route to any surface; the remaining **2,238** (5.5%) already overlap with "show more matches".
- **Owner's answer (verbatim):** "KEEP IT. The panel retains a third level ('Also shares text
  with'). Rationale on record: 38,377 of the 40,615-item middle bucket (94.5%) are not otherwise
  reachable -- chiefly the 37,397 related-page instances, which are not work identifications and
  have no other route to any surface. The 2,238-item overlap with 'show more matches' is accepted
  as the cost."
- **Date:** 2026-08-02.
- **Code consequence:** the panel implements **three** disclosure levels, not two: (1)
  Identifications (default); (2) "Also shares text with / חולק טקסט גם עם" -- collapsed by default,
  holding the D-13d generic-passage groups plus the D-11a related-pages count, explicitly never
  presented as an identification; (3) the existing "Show more possible matches" toggle. This is a
  structural decision for the panel display model (plans 136-19/136-20), not a single scalar
  constant -- cite this record (`136-GATE1-DECISIONS.md` § D-13e) at the point the three-level
  structure is implemented.

### D-16 / PANEL-01 -- does the findings page also get the relation filter?

- **Question:** does a relation filter on the corpus-wide findings page meaningfully narrow the
  default view, or does it mostly restate the bucket?
- **Measured number** (`136-GATE1-EVIDENCE.md`): within the Main pool, the relation split is
  **141,958** `direct_witness` (94%) / **11,286** `quotes_this_work` / **3,589** `shared_text`.
- **Owner's answer (verbatim):** "NO. Not built on findings. Within the Main pool the split is
  141,958 direct (94%) / 11,286 quotes / 3,589 shared text, so a filter there would restate the
  bucket rather than narrow it. The panel keeps its own relation filter; this decision is
  findings-page-only."
- **Date:** 2026-08-02.
- **Code consequence:** the corpus-wide findings page ("Computed Identifications") ships **without**
  a relation/tier filter control. The panel's own relation filter (PANEL-01/02, `/work/{id}` and the
  per-manuscript panel) is unaffected and proceeds as already specified -- this decision is scoped
  to the findings page only, per D-16's own framing in `136-CONTEXT.md`.

### D-13c -- the short-evidence threshold

- **Question:** what is the short-evidence threshold, in matched letters, below which a
  `track1_direct` row is routed behind the "show more" toggle rather than shown as a default
  identification (mockup M-3)?
- **Measured number** (`136-GATE1-EVIDENCE.md`): **6,558** direct rows (4.5% of 144,294) and
  **6,497** propagated rows (15.9% of 40,968) fall below 150 matched letters; of the short direct
  rows, **8,457** nonetheless remain part of a MAIN identification via multi-folio agreement (the
  honest counter-argument the owner already accepted -- a short liturgical passage may be exactly
  the correct identification for a prayer book).
- **Owner's answer (verbatim):** "KEEP 150 matched letters. Unchanged from the reviewed value. On
  record: 6,558 direct (4.5%) and 6,497 propagated (15.9%) fall below it, of which 8,457 short
  direct rows remain part of a Main identification via multi-folio agreement."
- **Date:** 2026-08-02.
- **Code consequence:** a named integer constant, unit **matched Hebrew base letters**:
  `SHORT_EVIDENCE_THRESHOLD_MATCHED_LETTERS = 150`, to live in `shared/discovery_main_pool.py`
  (cited to this record). Predicate: a `track1_direct` evidence row with `matched_letters < 150`
  routes to "show more" (screening) UNLESS the identification it belongs to already qualifies as
  MAIN via `multi_folio_agreement` (>=2 distinct pages agreeing) -- rows are demoted, never deleted,
  and stay reachable behind the toggle. This mirrors the D-13c mockup M-3 rule ("short-evidence rows
  go behind the 'show more' toggle") and the multi-folio carve-out already measured above.

### D-13b -- the lead-attribution tie-break

- **Question:** what breaks a tie after band rank, when several works claim the identical byte span
  on one page?
- **Measured number** (`136-GATE1-EVIDENCE.md`): **1,553** identical-span groups / **3,590** claims;
  of those, **1,542** groups (99.3%, **3,567** claims) are STILL tied after ordering by band rank
  alone.
- **Owner's answer (verbatim):** "LEXICOGRAPHIC `evidence_id`. Reuse the existing deterministic
  tie-break in `shared/discovery_ids.py`. No new concept. On record: band rank alone leaves 1,542 of
  1,553 identical-span groups (99.3%) still tied."
- **Date:** 2026-08-02.
- **Code consequence:** the lead-attribution ordering for an identical-span group is: (1) band rank
  ascending (strongest first); (2) on a tie, ascending **lexicographic `evidence_id`** -- the exact
  tie-break already implemented at `scripts/discovery_ids.py` (the `evidence_id()` builder plus its
  existing winner-selection total order, whose final tie-break key is
  `str(row.get("evidence_id", ""))`). No new tie-break concept is introduced; the rule is reused
  verbatim and wired into the identical-span-group lead-attribution logic in
  `shared/discovery_grouping.py` (cited to this record). Note for the future module's exact source
  path: as of this recording `discovery_ids.py` lives at `scripts/discovery_ids.py`, not yet under
  `shared/`; whichever module the D-13b logic is implemented in must import (or reproduce, citing
  this file) that exact function -- never a fresh tie-break.

### D-13d -- the granularity separation rule (closes the KNOWN FLAW)

- **Question:** does the author-gated same-title rule correctly separate "the same work at two
  granularities" (collapse it) from "genuinely different works on one passage" (generic shared
  text), for identical-span groups carrying >=2 different canonical works?
- **Measured number** (`136-GATE1-EVIDENCE.md`): **1,367** identical-span groups / **3,218** claims
  carry >=2 different canonical works. Of those, **276** groups (20.2%, **558** claims) contain a
  same-author/related-title pair and collapse under the proposed rule; **1,091** groups (79.8%,
  **2,660** claims) contain no such pair and remain genuinely generic shared text. Worked example
  (T-S Misc. 12.31.14, sys_id `990051079570205171`, span 0-962): `w000171` **רש"י על התורה** and
  `w001281` **רש"י על בראשית**, both authored by שלמה בן יצחק (רש"י) -- the same underlying
  commentary at two catalogued granularities, carrying different `canonical_work_id`s.
- **Owner's answer (verbatim):** "THE AUTHOR-GATED RULE AS PROPOSED. Collapse two works in an
  identical-span group when they share the same non-null author AND either an identical normalized
  title OR a shared >=4-character title prefix. Author-gating is load-bearing: it is what stops the
  rule over-collapsing large generic-title clusters such as 'Responsa of the Geonim'. Measured
  effect to record: 276 of 1,367 groups (20.2%, 558 claims) collapse; 1,091 groups (79.8%, 2,660
  claims) remain genuinely generic."
- **Date:** 2026-08-02.
- **Code consequence:** a **predicate over two works**, `works_related_by_title(work_a, work_b)`
  (ported verbatim from `scripts/discovery_gate1_evidence.py`, cited to this record), reading
  exactly these fields: `works.author` (non-null, exact equality) AND (`works.neutral_title`
  normalized -- NFC, quote/geresh/gershayim marks stripped, whitespace collapsed -- either
  identical between the two works, OR sharing a `>= 4`-character prefix of the normalized titles).
  When true for at least one pair inside an identical-span group with >=2 different canonical
  works, that pair collapses like a D-13a duplicate (the canonical work's own title wins, the other
  is dropped from view) -- a **display-time fix, not a data fix**. When false for every pair in the
  group, the group remains a genuinely generic D-13d "also shares text with" entry (see D-13e
  above). To be implemented in `shared/discovery_grouping.py`, consumed by the panel, the work page
  and the findings page alike, per D-13a's existing scope.
  - **Corpus-wide validation note (measured during this scope's `select_generic_collection_candidates`
    work, see Class 5 below):** the largest same-author/same-title clusters that this rule
    deliberately does NOT collapse (author-gated, cluster size >=3 with >=2 distinct canonical
    work ids) top out at a **43-member** cluster (e.g. a Geonic responsa collection) -- confirming
    empirically that the author-gate is what keeps those large generic collections out of the
    2-member collapse rule, exactly as the owner's rationale states.

---

## B. The novelty funnel run

- **Authorization:** **RUN NOW, PINNED** (option id `run-now-pinned` of the plan's Task 2 options).
- **Owner's answer (verbatim):** "RUN NOW, PINNED. The validated cheap configuration
  (`gemini-3.6-flash`, `reasoning:{effort:"low"}`), ~$27 one-time. Do NOT downgrade the model. Read
  the real `usage.cost` from the provider after the run -- never estimate it."
- **Date:** 2026-08-02.
- **Consequence:** the novelty gate runs with model `gemini-3.6-flash`, `reasoning.effort = "low"`,
  pinned per D-23c's reproducible-contract requirement (prompt hash, model+version, normalized input
  hash, structured abstention to `indeterminate`, explicit cache-key spec -- see `136-CONTEXT.md`
  D-23c/D-23a/D-23d). The measured cost is read from the provider's own `usage.cost` field after the
  run completes and recorded verbatim wherever the run is reported; it is never estimated in
  advance, and the model is never downgraded below this pinned configuration (per
  `reference_discovery_llm_gate_cost` -- the cheap configuration was already measured against this
  task and matches the validated quality; downgrading further has not been measured, and the error
  this axis makes -- telling a reader a finding is unrecorded when it is recorded -- is the
  reputationally expensive one).

## C. The evaluation-set size

- **Owner's answer (verbatim):** "ALL 52, PLUS TWO ADDITIONAL HARDER CLASSES" -- Class 4 (terse or
  missing catalogue identification text, target ~15 cases) and Class 5 (generic collection works,
  target ~15 cases), selected with the same measured, zero-model-call, string/metadata-only
  discipline as the original 52, additive (every existing case kept unchanged).
- **Date:** 2026-08-02.
- **Measured outcome of the extension** (this plan, post-ratification): `scripts/discovery_gate1_evidence.py`
  was extended with `select_terse_catalogue_candidates` (Class 4) and
  `select_generic_collection_candidates` (Class 5) and re-run against the same deployed asset.
  Result: **15** Class 4 candidates (manuscripts whose own `libraries.csv` catalogue-identification
  field is either entirely empty or <=20 characters, best claim per manuscript, emptiest first) and
  **15** Class 5 candidates (round-robin across the 13 author+title-stem clusters of size >=3 with
  >=2 distinct canonical works -- the exact clusters `select_alias_pair_candidates` excludes as
  corpus noise -- largest cluster first, up to 2 manuscripts per cluster). **Total candidate pool:
  82** (52 original + 15 + 15), written to `136-NOVELTY-HARDCASES.md` (verified reproducible: two
  consecutive runs against the same asset produce byte-identical Markdown).
- **Evaluation-set size vs. candidate-pool size:** 82 was the size of the candidate pool put in
  front of the owner at the first Task-3 attempt, not yet the size of the labelled evaluation set.
  **Updated by decision E (this same plan, later dispatch):** a sixth class -- catalogue divergence,
  the shade decision E calls `diverges` and names as having ZERO representation across Classes 1-5
  -- was added per this plan's own Task 3 instruction, using the identical zero-model-call,
  script-reproducible selection discipline (`select_catalogue_divergence_candidates` in
  `scripts/discovery_gate1_evidence.py`, round-robined across distinct divergent-work targets so no
  single frequently-quoted title crowds out the others -- mirroring Class 5's cluster round-robin).
  Measured result: **15** Class 6 candidates, bringing the **total candidate pool to 97** (82 + 15;
  every one of the original 82 kept unchanged in content, verified by two consecutive script runs
  producing byte-identical Markdown). The effective (as opposed to candidate-pool) evaluation-set
  size is still determined by how many of the 97 the owner actually labels (one of the seven
  decision-E shades, or `unsure`) versus explicitly skips at Task 3, which has **not yet run** (see
  the Status note above and the executor's `human-action` checkpoint).

## D. The `needs-ruling` domain rows

- **Owner's answer (verbatim):** "THE OWNER WILL RULE. The 'ship as Unassigned' default is
  explicitly DECLINED."
- **Date:** 2026-08-02.
- **Context:** `works.genre` is entirely NULL today and needs a one-time curation pass over the
  **~1,088** works carrying a shipped claim (`136-CONTEXT.md`, "Materialization" section); roughly
  3-4% of that curation is expected to be cases the controlled vocabulary cannot settle without a
  ruling.
- **Consequence:** plan 136-09 (the domain/genre curation plan) **MUST halt and surface the
  needs-ruling work list to the owner for adjudication** rather than defaulting those rows to an
  `Unassigned` bucket. **No such list exists yet -- plan 136-09 produces it** as part of its own
  execution; this record only fixes the posture (owner rules, no silent default) that 136-09 must
  honor.

---

## E. Novelty becomes a SHADE ENUM, not a tri-state (amends D-23a / NOVEL-01)

- **Question:** does the tri-state novelty flag (`not_in_finding_aids` / `already_recorded` /
  `not_checked`) correctly distinguish the qualitatively different findings a computed
  identification can represent, or does it collapse materially different cases into the same
  bucket?

- **Owner's answer (verbatim ruling, condensed for this record -- the full text is reproduced
  below):** "Novelty becomes a SHADE ENUM, not a tri-state. Amends D-23a and NOVEL-01." The owner
  identified a defect in the locked tri-state. Under NOVEL-01's rule ("does ANY finding aid already
  tie THAT fragment to THAT work"), two very different situations collapse wrongly:

  1. If an aid ties fragment F to work X and we claim work Y, nothing ties F to Y -- so it scores
     `not_in_finding_aids` and surfaces under "Candidates for new finds". But that is not a new
     find; it is a claim that THE CATALOGUE IS WRONG about this page -- a different and far more
     reputationally loaded assertion, currently voiced as novelty.
  2. Conversely a granularity refinement (aid names the parent work, we name the specific book)
     scores `already_recorded` and becomes invisible, though it is genuinely informative. Class 3 is
     20 such cases; the D-13d author-gated rule measured 276 such groups (of 1,367 identical-span
     groups carrying >=2 different canonical works -- see section A's D-13d entry above).

  **Ruling: store the full shade set in the asset; ship a conservative public toggle.**

  | Shade | Condition on claim (fragment F, work W) |
  |---|---|
  | `confirms` | an aid already ties F to W |
  | `refines_granularity` | an aid ties F to a coarser/finer variant of W, per the D-13d author-gated rule |
  | `diverges` | an aid ties F to a different work that is NOT a granularity variant |
  | `fills_gap` | the aids identify F as nothing at all -- the true "previously unknown" |
  | `extends` | aids tie OTHER folios of the same manuscript to W, but not this folio |
  | `alias_merge` | the claim asserts two catalogued works are one (Class 2's situation) |
  | `not_checked` | fail-closed: unrun, failed, or abstained (D-23c structured abstention) |

  **The public surface is UNCHANGED IN SHAPE.** "Candidates for new finds" selects `fills_gap`
  ONLY. `diverges` and `extends` are EXCLUDED from the candidate toggle -- a contradiction is not a
  new find, and a folio-extension is nearly always right and unremarkable. `refines_granularity` is
  stored but never voiced as a new find. No new public filter values, no new bilingual surface
  wording, no change to D-15/D-15a/D-16, no change to D-24's orthogonality to tier. The finer shade
  exists in the asset for Phase 137's judgments and any later filter. `not_checked` remains the
  fail-closed default -- never "novel by default".

  Owner's stated rationale, for the record: novelty is not binary; "previously unknown", "sharply
  diverges from the current identification", and "different granularity" are materially different
  findings and should not be collapsed.

- **Date:** 2026-08-02 (same day as rulings A-D, delivered later in the session).

- **Provenance note (repudiation-resistance, per threat T-136-03-04).** This ruling was FIRST
  presented to the prior executor through an inter-agent side channel -- attached to a tool result
  rather than an orchestrator dispatch -- and was CORRECTLY REFUSED as a possible injection: it
  asked for unilateral amendment of LOCKED decisions (D-23a, NOVEL-01) with real downstream
  schema/verifier/LLM-contract consequences, delivered through a channel that cannot establish
  owner authorship. That refusal cost nothing (no commits needed undoing) and remains the correct
  standing behavior for any future instruction arriving by a route other than an orchestrator
  dispatch. Decision E is recorded here only because it was RE-DELIVERED through a normal
  orchestrator dispatch in the continuation that produced this section, and is treated as an owner
  ruling of the same standing as A-D above -- LOCKED, not re-litigated, re-derived or "improved"
  here.

- **Code consequence -- an enum widening, not a data-model change.** The single existing
  `novelty_status` column (`discovery_evidence`, TEXT, indexed on the status per
  `docs/specs/discovery-sidecar-schema-v1.md`) keeps its name, its nullability, its default
  (`not_checked`), and its role as "the field a read path filters/groups on" -- only its permitted
  value set widens from three values to seven: `confirms` / `refines_granularity` / `diverges` /
  `fills_gap` / `extends` / `alias_merge` / `not_checked`. The public "Candidates for new finds"
  predicate becomes `novelty_status = 'fills_gap'` (previously `novelty_status =
  'not_in_finding_aids'`) -- a one-value substitution, no shape change to the toggle, the badge, the
  sub-line, or the help-affordance text fixed in NOVEL-01's 2026-08-02 (A-6) amendment.
  `novelty_source_label` (populated only on a "recorded" outcome) now populates on `confirms` /
  `refines_granularity` / `alias_merge` / `extends` (every shade where SOME finding aid says
  something about this fragment-work pair) and stays NULL on `diverges` (the aid names a DIFFERENT
  work -- the masked label would misleadingly imply agreement) and `fills_gap` (nothing to name).

- **Downstream contracts this decision amends** (enumerated per this plan's Task 1 instruction;
  **NOT implemented in this plan** -- 136-03 only records the ruling and prepares the ground truth):

  1. **D-23a** (`136-CONTEXT.md`) -- "TRI-STATE, fail-closed" is amended to "SEVEN-VALUE SHADE
     ENUM, fail-closed"; `not_checked` remains the sole fail-closed default value, unchanged in
     meaning.
  2. **NOVEL-01's 2026-07-30 amendment, clause (1)** (`.planning/REQUIREMENTS.md`) -- "The flag is
     TRI-STATE, fail-closed, not boolean: `not_in_finding_aids` / `already_recorded` /
     `not_checked`" is superseded by the shade enum; clauses (2) (display wording) and (3)
     (reviewed novelty identity) are UNCHANGED. Recorded as a new dated `<AMENDED 2026-08-02>`
     sub-bullet on NOVEL-01 by this plan's Task 2.
  3. **The `novelty_status` CHECK constraint and its index** in
     `docs/specs/discovery-sidecar-schema-v1.md` (the `discovery_evidence.novelty_status` column
     definition, the `discovery_identification.novelty_status` CHECK, and the D-10a index "on
     `discovery_evidence(novelty_status)` -- the STATUS column, replacing the legacy `is_new`
     boolean") -- the CHECK's `IN (...)` list widens from three to seven values in both places the
     schema doc currently states it.
  4. **The frozen-enum-vocab readiness check** that `web/discovery_assets.py::discovery_available()`
     fails closed on -- today this runtime spot-check covers `claim_type` (`_CLAIM_TYPES`) and
     `(evidence_source, confidence_band)` (`_CONFIDENCE_BANDS_BY_SOURCE`) but does NOT yet cover
     `novelty_status` (novelty is not wired into this module until 136-12/136-14 build it). When it
     IS wired, the frozenset checked must be the seven-value shade set, not the three-value
     tri-state -- an out-of-vocab novelty value must fail the whole sidecar load closed, exactly as
     an out-of-vocab `claim_type` does today.
  5. **D-23c's pinned LLM contract** (`136-CONTEXT.md`) -- the prompt must now elicit a shade
     (which finding aid, if any, says what about this fragment-work pair), not a boolean/tri-state
     judgement, so the PINNED PROMPT HASH necessarily changes from whatever 136-04 would otherwise
     have pinned. The model, its version, and the reasoning-effort setting (`gemini-3.6-flash`,
     `effort:"low"`) are UNCHANGED per ruling B above -- only the prompt template's output contract
     widens.
  6. **(Beyond this plan's stated minimum, flagged here for completeness.)** The **D-02b
     rebuild-preservation gate** (plan 136-05, already executed per this plan's own
     `completed_state`) allowlists "the authorized novelty changes" as one of the few columns
     permitted to differ between the pre-rebuild and post-rebuild asset. That allowlist entry must
     be read as covering the SEVEN-value shade set, not the three-value tri-state, when 136-05's
     diff harness actually runs against the rebuilt asset in wave 5 (136-13) -- no code change is
     needed in 136-05 itself (the allowlist keys on the COLUMN, `novelty_status`, not its value
     vocabulary), but the diff harness's assumptions should be reviewed for exactly this reason
     before that gate runs.

- **Plans that must implement this ruling** (named by plan id, per this plan's Task 1 instruction;
  none of these plans run inside 136-03):

  - **136-04** -- Novelty: identity key, pinned LLM contract, committed funnel runner, authorized
    run, verdict cache. Must build the seven-value shade classifier (not a boolean/tri-state one)
    and pin a NEW prompt hash for the shade-eliciting prompt.
  - **136-06** -- D-02a tier_a authorization lockstep (builder + verifier + both-branch fixtures).
    Touches the same schema-doc row-set area the novelty CHECK constraint lives beside; must not
    silently narrow the novelty CHECK back to three values while amending the tier_a row-set
    nearby.
  - **136-12** -- Build wiring B: novelty ingestion, visibility axes, curated load, kept_tie fix,
    verifier extensions. This is where `novelty_status` is actually COMPUTED and WRITTEN, and where
    `scripts/verify_discovery_sidecar.py`'s verifier extensions must enforce the seven-value CHECK
    (not the tri-state) and, per item 6 above, where `web/discovery_assets.py`'s runtime spot-check
    should gain a `novelty_status` frozenset.
  - **The release verifier** -- `scripts/verify_discovery_sidecar.py`'s frozen-enum-vocab
    enforcement, extended in 136-12 and actually RUN as part of the gate battery in **136-13** (the
    rebuild, the gate battery, the owner authorization and the one production redeploy) before the
    asset goes live.
  - Downstream UI consumers of the toggle (136-16/136-18 for the findings page, 136-15/136-17 for
    the panel) are UNAFFECTED in shape -- they read `novelty_status = 'fills_gap'` exactly where
    they would previously have read `= 'not_in_finding_aids'`; no plan review is owed there beyond
    that one-value substitution, which this record now makes citable.

- **What this plan (136-03) does NOT do:** it does not edit
  `docs/specs/discovery-sidecar-schema-v1.md`, `web/discovery_assets.py`,
  `scripts/verify_discovery_sidecar.py`, or any build/service module. This section only records the
  ruling and its enumerated consequences so later plans build from a single citable ground truth,
  per this plan's own Task 1 instruction. This plan DOES amend `.planning/REQUIREMENTS.md` (Task 2)
  and extend the novelty hard-case candidate set with a new Class 6 (Task 3) -- both are recorded
  separately below and in `136-NOVELTY-HARDCASES.md`.

---

## Provisional-value / omission audit

None of the five gate-1 answers above (A) were flagged by the owner as provisional -- all five are
recorded as LOCKED, final rulings with no stated revisit condition.

**One related item was NOT addressed by this ruling and is flagged explicitly, rather than letting
the omission pass silently (per this plan's own Task 4 instruction):** `main-pool-rule.md` §"Before
shipping" states, in the reviewers' own words, "Do not freeze the thresholds on these numbers... 
Review ~300 stratified cases by hand -- coverage bands, single vs multi-page, ties, short matches,
genres -- before 0.8 becomes a constant." That **0.8 single-page coverage floor** (gate 4 of the
main-pool classification, distinct from the D-13c 150-matched-letter threshold above) was **NOT**
part of this ruling, and the ~300-case stratified hand review it recommends was **NOT authorized**
here. The 0.8 floor therefore remains an unreviewed, provisional value in the current classification
pass (`scripts/discovery_gate1_evidence.py::classify_identifications`, gate 4) until a future gate
addresses it -- it must not be silently treated as ratified by this record.

---

## Outstanding (pending Task 3)

- `discovery_data/novelty_hardcase_labels-v1.json` -- the owner-supplied ground truth, one entry per
  hard case with its verdict (one of decision E's seven shades, or `unsure`), date and
  `label_provenance`. **Not yet created.**
- The content hash of that file, to be recorded in an update to this section once it exists.
- The effective (as opposed to candidate-pool) evaluation-set size, once the owner has labelled or
  explicitly skipped each of the **97** candidates (82 original + 15 Class 6, added under decision
  E) up to whatever portion they choose to judge.
- **Vocabulary note:** the worksheet (`136-NOVELTY-HARDCASES.md`) has been REGENERATED against
  decision E's seven-shade vocabulary (`confirms` / `refines_granularity` / `diverges` /
  `fills_gap` / `extends` / `alias_merge`, plus `unsure` / `skip`) -- the old 4-value set
  (`already_recorded` / `not_in_finding_aids` / `unsure` / `skip`) no longer appears anywhere in
  that file. No verdict has been pre-filled anywhere; every `PROPOSAL` draft remains explicitly
  marked as a draft, never a label.

This record will be updated (not silently overwritten) once plan Task 3 returns and Task 4 writes
the label file.
