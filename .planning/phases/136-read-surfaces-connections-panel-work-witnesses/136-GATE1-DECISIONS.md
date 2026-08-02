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

**Further addendum, 2026-08-02 (same day, a later orchestrator dispatch still) -- CORRECTION E′.**
The owner spotted an asymmetry in decision E's shade table and issued a correction, delivered
through a normal orchestrator dispatch (the correct channel; see this record's own standing
discipline above for why that provenance matters). This is recorded as a **correction to E**, not
a new ruling -- the shade enum widens from seven values to EIGHT. See the new **"E′ -- correction"**
block appended at the end of section E below, after its Provenance note. Section E's original table,
rationale and Provenance note are left exactly as recorded (repudiation-resistance, T-136-03-04) --
E′ documents what changed and why, rather than silently rewriting the original.

**Further addendum, 2026-08-02 (same day, a later continuation still) -- RULINGS F AND G, PLUS A
LABELLING RESTRUCTURE.** After E′, the owner read the actual 97-case worksheet directly (not merely
the shade definitions) and issued two further rulings, delivered through a normal orchestrator
dispatch to this later continuation (the correct channel; see §F's own Provenance note): **F** splits
`diverges` by SCOPE into `diverges_work` / `diverges_part` (widening the shade enum from EIGHT to
NINE values) and adds a SEPARATE `divergence_correctness` axis, with a NEW default-hidden/
explicit-warned-toggle display rule for both divergence shades -- see **section F** below. **G** rules
that "catalogue vague, we are more specific" is `confirms`, not `refines_granularity`, with a
systemic consequence for the novelty check's own free-text coverage -- see **section G** below. The
SAME continuation also carries out an owner-authorized RESTRUCTURE of what gets labelled: Classes 1-3
(52 cases) are reduced to an ~8-case IDENTITY spot-check (testing the constant-answer assumption, not
building ground truth), and Classes 4-6 are EXPANDED from 45 to ~75 NOVELTY SHADE cases -- see the
"Labelling restructure" note appended to the "Outstanding (pending Task 3)" section below. Sections
A-E/E′ are NOT reopened by any of this; they remain LOCKED exactly as recorded above.

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

### E′ -- CORRECTION (owner, same day, later dispatch): the granularity shade splits by direction

**This is a correction to decision E above, not a new ruling.** Everything in section E not
touched here (the shade table's other six rows, the display posture "store the full shade set /
ship a conservative public toggle", the Provenance note, `not_checked`'s role) stands unchanged.
Only the single `refines_granularity` row is split into two, by direction.

- **The defect the owner found.** Decision E's `refines_granularity` row read "an aid ties F to a
  coarser/finer variant of W, per the D-13d author-gated rule" -- ONE shade covering BOTH
  directions of a granularity relationship. The owner rejected this as an asymmetry error, in the
  owner's own words (verbatim):

  > "if we have רש"י על התורה and catalog has רש"י בראשית כב we are different but for worse, so no
  > novelty at all"

  A catalogue that already names the CHAPTER (רש"י בראשית כב, "Rashi on Genesis 22") knows strictly
  MORE than a claim that only names the whole Torah commentary (רש"י על התורה) -- that direction
  contributes nothing new, and collapsing it into the same shade as the OPPOSITE direction (our
  claim being the finer one, which genuinely adds information) hid a real distinction.

- **The corrected two-row table** (replaces the single `refines_granularity` row in decision E's
  table above; nothing else in that table changes):

  | Shade | Condition | Meaning |
  |---|---|---|
  | `refines_granularity` | OUR claim is **FINER** than the aid's -- e.g. catalogue: רש"י על התורה, our claim: רש"י על בראשית | we ADD precision; informative |
  | `aid_more_specific` | the AID's identification is **FINER** than ours -- e.g. catalogue: רש"י בראשית כב, our claim: רש"י על התורה | we add NOTHING; the catalogue already knows more than we do |

  Both rows still require the D-13d author-gated title relationship to apply (same non-null author
  AND identical/prefix-shared normalized title) -- E′ splits the DIRECTION of an existing
  relationship, it does not change what counts as a granularity relationship in the first place.

- **The shade enum widens from SEVEN to EIGHT values:** `confirms`, `refines_granularity`,
  `aid_more_specific`, `diverges`, `fills_gap`, `extends`, `alias_merge`, `not_checked`.

- **`aid_more_specific` is definitively NOT novel -- the LEAST novel row in the corpus.** It joins
  `confirms`, `refines_granularity`, `diverges` and `extends` as EXCLUDED from the "Candidates for
  new finds" toggle. `fills_gap` remains the ONLY shade that predicate selects -- unchanged by E′.

- **Display ruling (owner): SHOW, BUT NEVER AS A CANDIDATE FIND.** `aid_more_specific` rows display
  normally, with their tier, exactly like every other non-candidate shade -- nothing else about
  rendering changes. The owner was offered, and explicitly **DECLINED**, three alternatives (recorded
  here so none of them is silently revived by a later plan):
  1. Demoting `aid_more_specific` rows below the main pool -- declined.
  2. Showing the aid's more-specific wording alongside ours -- declined.
  3. Hiding `aid_more_specific` rows entirely -- declined.

- **E′ is a NOVELTY-ONLY fix.** No change to the main-pool rule (plan 136-07's scope untouched), no
  new public surface wording, no additional masking surface. The only surface-visible effect is the
  one-shade addition to the (never-public) stored vocabulary and its exclusion from the candidate
  predicate -- identical in shape to how decision E itself touched the public surface.

- **Worked case already in the worksheet.** Class 3 Case 1 (T-S Misc. 12.31.14, sys_id
  `990051079570205171`) claims `רש"י על התורה` / `רש"י על בראשית` while the catalogue's own
  identification text names `בראשית מד` (Genesis chapter 44) -- finer than BOTH claims. `aid_more_specific`
  is the shade required to label that case honestly; `refines_granularity` alone (the pre-E′
  vocabulary) could not.

- **Date:** 2026-08-02 (same day as decisions A-E, delivered later in the session, through a normal
  orchestrator dispatch).

- **Code consequence (supersedes the seven-value figures in decision E's own "Code consequence"
  above with the same structure, widened by one value):** `novelty_status` keeps its name,
  nullability, default (`not_checked`) and role; its permitted value set widens from SEVEN to EIGHT:
  `confirms` / `refines_granularity` / `aid_more_specific` / `diverges` / `fills_gap` / `extends` /
  `alias_merge` / `not_checked`. The public "Candidates for new finds" predicate is UNCHANGED by E′
  (`novelty_status = 'fills_gap'`). `novelty_source_label` populates on `aid_more_specific` exactly as
  it already does on `confirms` / `refines_granularity` / `alias_merge` / `extends` (an aid says
  SOMETHING nameable about this fragment-work pair) and stays NULL on `diverges` / `fills_gap` as
  decision E already specifies.

- **Downstream contracts E′ additionally amends** (same six items decision E enumerated above; each
  gains the `aid_more_specific` value on this account -- not a new list, an extension of E's own):
  1. **D-23a** -- "SEVEN-VALUE SHADE ENUM" (as amended by decision E) is further amended to
     "EIGHT-VALUE SHADE ENUM, direction-split granularity".
  2. **NOVEL-01** -- gains a further dated `⟨AMENDED 2026-08-02 -- E′⟩` sub-bullet (this plan's
     Task 2; see `.planning/REQUIREMENTS.md`), sibling to decision E's own amendment, not a
     replacement of it.
  3. **The `novelty_status` CHECK constraint and its index** (`docs/specs/discovery-sidecar-schema-v1.md`)
     -- the `IN (...)` list widens from seven to eight values in both places the schema doc states it.
  4. **The frozen-enum-vocab readiness check** (`web/discovery_assets.py::discovery_available()`) --
     when `novelty_status` is wired into this runtime spot-check (per decision E's item 4), the
     frozenset checked must be the EIGHT-value shade set.
  5. **D-23c's pinned LLM contract** -- the prompt must now elicit DIRECTION as well as which finding
     aid says what -- "does the aid name a coarser or a finer variant of our claim?" -- not merely
     "granularity variant, yes/no". The PINNED PROMPT HASH changes on this account too, on top of
     decision E's own prompt-hash change (the shade-eliciting rewrite was never pinned before either
     correction landed, so this is one net hash change to pin at 136-04, not two sequential ones).
     Model/version/reasoning-effort (`gemini-3.6-flash`, `effort:"low"`) remain UNCHANGED per ruling B.
  6. **The D-02b rebuild-preservation gate allowlist** (plan 136-05) -- covers the EIGHT-value shade
     set on the same `novelty_status` column-keyed entry decision E already named; no further code
     change needed in 136-05 itself.

- **Plans that must implement this correction** -- the SAME plans decision E already named
  (136-04, 136-06, 136-12, the release verifier, and the unaffected-in-shape UI consumers listed
  under decision E) now build the EIGHT-value shade classifier instead of the seven-value one.
  No plan not already named by decision E is newly implicated by E′.

- **What this correction does NOT do:** exactly what decision E's own "What this plan (136-03) does
  NOT do" already states -- 136-03 does not edit the schema doc, `web/discovery_assets.py`, the
  verifier, or any build/service module. 136-03 DOES (per this continuation's own Task 3 instruction):
  amend NOVEL-01 again (Task 2 here), reissue `136-NOVELTY-HARDCASES.md`'s vocabulary via the script
  (never by hand) to carry `aid_more_specific`, and add a NEW XLSX labelling-workbook deliverable
  emitted by the same script (`scripts/discovery_gate1_evidence.py`) -- see the "XLSX round-trip"
  note appended to the "Outstanding (pending Task 3)" section below.

---

## F. Divergence becomes its own OPT-IN category, split by SCOPE, with a separate CORRECTNESS axis

**Provenance.** Rulings F and G below arrived through a normal orchestrator dispatch in a later
continuation of this same plan (136-03), after the owner read the ACTUAL 97 candidate cases the prior
continuation generated -- not merely the shade's abstract definition. This is the correct channel (see
decision E's own Provenance note for why that distinction is load-bearing); F and G are recorded here
with the same standing as A-E/E′ -- LOCKED, not re-litigated, re-derived or "improved" here.

- **Question:** now that the owner has read the real 15 Class-6 candidate cases, does `diverges`
  still behave as a single shade with a single display posture, or does reading real cases reveal it
  needs to be split and treated differently from every other shade?

- **Owner's answer (verbatim ruling):** "keep it as a category, hidden by default (with a clear
  warning), which the user decides."

- **What prompted this.** The owner reviewed the 15 Class-6 (`catalogue divergence`) cases directly
  and reports that when our claim and the catalogue disagree, **usually the catalogue is right** --
  i.e. divergent rows are largely OUR false positives. Worked examples cited by the owner: case 92
  (we claim ילקוט שמעוני, the catalogue's own identification text says תנחומא) and case 84 (we claim
  משנה תורה, ספר זמנים, the catalogue's own identification text says הגדה של פסח).

- **The standing conflict this threads, stated plainly.** `feedback_catalogue_never_evidence` / this
  project's own CONTEXT discipline holds that the catalogue is a recall yardstick and NEVER acceptance
  evidence, and that adjudication stays catalogue-blind. If the system silently demoted or hid a
  divergent row BECAUSE the catalogue disagrees with it, that would be exactly the forbidden move --
  using the catalogue's disagreement as evidence the claim is wrong (the same rule broken from the
  other side: absence-from-catalogue is not evidence of wrongness; presence-of-a-different-answer
  becoming evidence of wrongness is the identical error in reverse). Yet the owner's own reading of the
  real cases shows divergent rows ARE disproportionately wrong, and publishing them in the default view
  means knowingly shipping rows we have specific, measured reason to believe are incorrect.

- **The owner's resolution: neither the catalogue nor the system adjudicates -- the USER does.**
  Divergent rows are NOT auto-demoted, NOT auto-hidden by policy, and NOT silently trusted as correct
  either. They ship as a visible, NAMED, opt-in category, carrying an explicit warning, behind a
  toggle the user must deliberately open. This is a **structural reuse of the shape BAND-03 already
  established** for screening bands ("show more possible identifications" -- explicit toggle, honest
  probability framing, never silently hidden and never silently shown) -- the same shape now applied
  to a DIFFERENT axis (divergence, not confidence). The catalogue-never-evidence discipline is
  preserved because the SYSTEM never treats the catalogue's disagreement as a verdict; it only
  surfaces the disagreement, transparently and with context, and lets the person reading decide. This
  ruling does NOT generalize: no other axis in this project may use catalogue disagreement to
  auto-adjudicate; this is the owner's explicit, recorded exception to *visibility*, not to
  *adjudication*.

- **The shade splits by SCOPE, not by direction (a different axis than E′'s direction split).** The
  owner identified a third situation, distinct from a flat wrong-work divergence:
  - **`diverges_work`** -- the aid names a genuinely DIFFERENT work. Owner-classified worked cases:
    92, 84, 86, 95, 97, 91, 85 (7 of the 15 candidates). Usually our claim is wrong here.
  - **`diverges_part`** -- the aid names a different or FINER PART of the SAME work. Owner-classified
    worked cases: 90, 94, 96 (3 of the 15 candidates). Owner, verbatim: "more delicate and essentially
    less important."

- **Correctness is a SEPARATE axis from the shade -- this is the load-bearing structural point.** A
  `diverges_work` (or `diverges_part`) verdict only records THAT the aid and the claim disagree and at
  what scope; it does NOT record WHICH side is right. The owner's own review of the 15 cases shows
  both directions occur under the identical shade: sometimes the catalogue is right (our claim is the
  false positive -- the common case per the owner's own reading), and sometimes our claim is right
  (the catalogue is wrong, thinner, or itself mistaken). One shade token cannot carry both meanings
  without losing information the owner explicitly wants recorded. **Divergence rows therefore require
  TWO answers, not one:** (1) the shade (`diverges_work` / `diverges_part`), and (2) a correctness
  call -- `catalogue_correct` / `claim_correct` / `unclear` -- recorded as a SEPARATE field, never
  folded into the shade token itself.

- **Measured finding to record, not fix away (per this plan's own instruction).** Re-reading the 15
  Class-6 candidates against ruling G (below) surfaced that the selector's `diverges` call is itself
  frequently wrong at the SHADE level, not only interesting at the correctness level: it compared
  work-ids against the catalogue's identification TEXT without modelling the structured-id-vs-free-
  text relationship, and -- per the owner's own characterization -- **over-fired on roughly half of
  the 15** (cases 83 and 87 are the two worked, explicit examples; see ruling G). This is recorded
  here as a measured property of the CURRENT heuristic, not corrected by hand in this plan -- Task 3's
  restructure keeps the heuristic exactly as it stands (do not quietly fix it away) so that expanding
  the candidate pool continues to surface the SAME failure mode at a similar rate, which is itself
  useful information for whoever eventually hardens the check.

- **Date:** 2026-08-02 (same day as A-E/E′, a later dispatch still).

- **Code consequence -- the shade enum widens from EIGHT to NINE values, REPLACING `diverges` (not
  adding to it):** `novelty_status` keeps its name, nullability, default (`not_checked`) and role. Its
  permitted value set becomes: `confirms` / `refines_granularity` / `aid_more_specific` /
  `diverges_work` / `diverges_part` / `fills_gap` / `extends` / `alias_merge` / `not_checked` -- NINE
  values, `diverges` retired entirely (every downstream reference to the eight-value E′ enum's
  `diverges` token is replaced by the pair `diverges_work` / `diverges_part`, never left as a stray
  tenth alias). `novelty_source_label` populates on `diverges_work` / `diverges_part` exactly as it
  already did on `diverges` (the aid DOES say something nameable -- a different work or a different
  part -- even though what it says contradicts the claim); this is unchanged from decision E's own
  rule for `diverges`.

  **A NEW, separate stored field for correctness** (NOT part of the `novelty_status` enum -- a sibling
  column, since correctness is an orthogonal axis to shade per the ruling above): `divergence_correctness`,
  nullable, populated ONLY when `novelty_status IN ('diverges_work', 'diverges_part')`, permitted
  values `catalogue_correct` / `claim_correct` / `unclear`, NULL for every other shade (a
  `confirms`/`fills_gap`/etc. row has no divergence to adjudicate correctness on). This is a new column
  the schema doc (`docs/specs/discovery-sidecar-schema-v1.md`) and the release verifier's
  frozen-enum-vocab check must both gain -- not implemented in this plan (136-03), which only records
  the ruling and prepares the ground-truth labelling instrument.

  **The public "Candidates for new finds" predicate is UNCHANGED** (`novelty_status = 'fills_gap'`) --
  `diverges_work` and `diverges_part` are both excluded from it, exactly as `diverges` already was
  under decision E.

  **NEW default-visibility rule (this is the genuinely NEW surface behavior ruling F adds -- decision
  E never said divergent rows should be hidden from the default view, only excluded from the
  candidate-new-finds toggle).** `diverges_work` and `diverges_part` rows are **HIDDEN BY DEFAULT** in
  the panel/work-page/findings-page default view -- not merely "not voiced as a candidate new find"
  (E's posture for `diverges`), but ABSENT from the default render entirely -- and surface ONLY behind
  an explicit, separately-labelled toggle carrying a clear warning (mirroring BAND-03's "show more
  possible identifications" pattern: probability/uncertainty framing, never a silent default, never
  silently suppressed either). This default-visibility rule is layered ON TOP OF decision E's existing
  candidate-toggle exclusion and must be implemented by the same plans that build the panel and
  findings-page display logic: **136-15 / 136-17** (the panel) and **136-16 / 136-18** (the findings
  page) -- named here so none of the four silently reverts to decision E's weaker "excluded from
  candidates but shown normally" posture for what is now a structurally different, opt-in-only
  category.

- **Downstream contracts this decision amends** (same enumeration shape as decisions E/E′, extended):

  1. **D-23a** -- the enum descriptor is further amended from "EIGHT-VALUE SHADE ENUM, direction-split
     granularity" to "NINE-VALUE SHADE ENUM, direction-split granularity AND scope-split divergence,
     plus an orthogonal correctness field on divergence rows."
  2. **NOVEL-01** -- gains a further dated `⟨AMENDED 2026-08-02 -- F⟩` sub-bullet (this plan's Task 2;
     see `.planning/REQUIREMENTS.md`).
  3. **The `novelty_status` CHECK constraint and its index**
     (`docs/specs/discovery-sidecar-schema-v1.md`) -- the `IN (...)` list widens from eight to nine
     values, `diverges` removed and replaced by `diverges_work` / `diverges_part`. The schema doc must
     ALSO gain the new `divergence_correctness` column (nullable, CHECK'd to the three-value
     correctness vocabulary, NULL-required outside the two divergence shades).
  4. **The frozen-enum-vocab readiness check** (`web/discovery_assets.py::discovery_available()`) --
     when wired (per decision E's item 4), the frozenset checked must be the NINE-value shade set; a
     new frozenset (or the same check extended) must also validate `divergence_correctness` against
     its own three-value vocabulary wherever it is non-NULL.
  5. **D-23c's pinned LLM contract** -- the prompt must now elicit, for a divergence verdict
     specifically, BOTH the scope (work vs part) AND a correctness call (catalogue-correct /
     claim-correct / unclear) -- not merely "diverges, yes/no" as under decision E. The PINNED PROMPT
     HASH changes again on this account (the third net change across E/E′/F -- still pinned ONCE at
     136-04, not three times sequentially, since no prompt has actually been pinned yet).
  6. **The D-02b rebuild-preservation gate allowlist** (plan 136-05) -- the `novelty_status`
     column-keyed allowlist entry now covers the NINE-value set; the NEW `divergence_correctness`
     column needs its OWN allowlist entry (it did not exist under E/E′), which 136-05's diff harness
     has never seen -- flagged here so the harness gains it rather than silently treating an
     unrecognized column as a genuine unauthorized diff (or, worse, silently ignoring it).
  7. **(NEW, F-specific) The panel and findings-page display specs.** `136-15`/`136-17` (panel) and
     `136-16`/`136-18` (findings page) must each implement the default-hidden, explicit-warned-toggle
     behavior for `diverges_work`/`diverges_part` rows -- this is NOT covered by any existing plan text
     (decision E only touched the candidate-new-finds toggle, a narrower, already-covered behavior).

- **Plans that must implement this ruling:** **136-04** (shade classifier widens to nine values;
  correctness field added; a further-changed pinned prompt hash), **136-06**/**136-12**
  (schema/build wiring for both the widened enum and the new `divergence_correctness` column), **the
  release verifier** (both frozen-enum-vocab checks), **136-05** (allowlist gains the new column), and
  -- newly, not implied by decision E -- **136-15/136-16/136-17/136-18** (the NEW default-hidden/
  explicit-warned-toggle display behavior, built fresh in these four plans).

- **What this plan (136-03) does NOT do:** exactly as decisions E/E′ already state -- no schema-doc
  edit, no `web/discovery_assets.py` edit, no verifier edit, no build/service module edit. This plan
  amends `.planning/REQUIREMENTS.md` (Task 2), and restructures + regenerates the hard-case worksheet/
  workbook (Task 3) to carry the new shade vocabulary and the new correctness question -- both
  recorded separately below.

---

## G. "Catalogue vague, we are more specific" is `confirms`, NOT `refines_granularity` -- and the SAME failure mode manufactures false novelty at scale

- **Question:** when the catalogue's STRUCTURED identification is generic/vague but the catalogue's
  OWN FREE TEXT already states our specific identification in prose, is that `refines_granularity` (we
  add precision) or `confirms` (an aid already ties this fragment to this work)?

- **Owner's answer (verbatim):** "What you called situation 2 is plain and simple nothing new from our
  side."

- **Worked cases.** Case 83 (Ms. Evr. Antonin B 1104): we claim תשובות האיי גאון against a catalogue
  structured entry keyed to the generic תשובות -- but the catalogue's OWN identification text reads
  שאלות ותשובות מאת האי בן שרירא גאון ("questions and responsa FROM Hai ben Sherira Gaon"), which
  already names the specific author/work our claim names, just under a different spelling of the name
  and a looser structured key. Case 87 (Cambridge Add. 1246): we claim ספר יוסיפון (ערבי) against a
  structured entry keyed to the generic יוסיפון -- but the catalogue's OWN identification text reads
  יוסיפון בערבית ("Yosippon IN ARABIC"), which already states the exact distinguishing fact (the
  Arabic-language edition) our claim adds. In both cases, the catalogue's structured work-id keying is
  coarser than our claim, but its OWN PROSE already states the identification -- we add nothing that
  was not already recorded, somewhere, in the aid.

- **Rule to record.** `refines_granularity` is reserved for cases where we genuinely add information
  the aid does NOT contain IN ANY FORM -- neither a structured field NOR free text. Where the aid's
  own text already states the identification and only the STRUCTURED work-id keying differs, the shade
  is `confirms`, not `refines_granularity` (and not `aid_more_specific` either -- the aid is not MORE
  specific than us here; it is EQUALLY specific, just filed under a coarser key). This is a genuinely
  different situation from E′'s direction-split (which is about which of two claims is textually
  finer) -- G is about WHERE the fine-grained information already lives (structured field vs. free
  text), not about which side's title is finer.

- **SYSTEMATIC CONSEQUENCE for the novelty check itself -- recorded here as a REQUIREMENT, not merely
  a labelling nuance.** The identification frequently lives in the catalogue's FREE TEXT rather than in
  a structured work-id. A novelty check that joins only on structured work-ids will score rows like
  case 83 and case 87 as `fills_gap` ("not in the finding aids checked") when the catalogue plainly
  states the identification in prose -- **manufacturing false novelty at scale, on precisely the rows
  most damaging to publish** (a false "new find" claim is the single most reputationally expensive
  error this axis can make, per the standing cost-asymmetry rationale already on record for the
  model-gate authorization in section B above). NOVEL-01 already enumerates catalogue text among the
  checked sources ("FJMS and NLI catalogue + bibliography, titles, PGP, FGP, and M-source shelfmark
  attributions") -- this ruling makes the FREE-TEXT requirement EXPLICIT so a later implementer cannot
  satisfy NOVEL-01's letter with an id-only join against a structured field while silently never
  reading the prose the field sits beside.

- **The measured over-fire rate is a finding about THIS PROJECT's OWN Class-6 selector, recorded rather
  than quietly fixed.** `select_catalogue_divergence_candidates` (this script) is exactly such an
  id-only-join heuristic: it tests whether the catalogue's free text CONTAINS a different work's title
  as a literal substring, but never checks whether the CLAIMED work's own identity is ALSO recoverable
  from that same free text under a looser reading (a spelling variant, an author-name match, a
  language/edition qualifier) before calling the pair a `diverges` candidate. Per the owner's own
  characterization (recorded in ruling F above), this selector **over-fired on roughly half of the 15**
  Class-6 candidates -- cases 83 and 87 are the two the owner worked through explicitly and confirmed
  as `confirms`, not `diverges`. **This plan does NOT correct the selector's logic** (per the plan's
  own "do not quietly fix it away" instruction) -- Task 3 keeps
  `select_catalogue_divergence_candidates`'s existing structured-id-vs-free-text-substring test
  unchanged, so the SAME failure mode continues to surface at a similar rate as the candidate pool is
  expanded, which is itself the measured signal a future hardening pass should be built against.

- **Date:** 2026-08-02 (same day as A-E/E′/F, a later dispatch still).

- **Code consequence.** No enum value changes here (unlike F) -- this ruling is about the CORRECT
  APPLICATION of the existing `confirms` / `refines_granularity` boundary, not a new stored value. The
  consequence is entirely in HOW the novelty check (and any future re-implementation of
  `select_catalogue_divergence_candidates`-style heuristics) is built: **the check must read the aid's
  free-text identification field as a genuine input to the confirms/refines/diverges decision, not
  merely as a source for literal-substring containment tests against OTHER works' titles.** This is
  flagged explicitly to:
  - **Plan 136-04** (the novelty module + pinned LLM contract) -- the shade-eliciting prompt must
    present the aid's FULL free-text identification (not just a structured work-id join result) to the
    judgment step, and the prompt instructions must state the G rule directly ("if the aid's own prose
    already names this identification under any spelling/phrasing, the answer is `confirms`, even if
    the aid's structured field points elsewhere").
  - **Plan 136-12** (novelty ingestion / build wiring) -- wherever the heuristic funnel does its
    FIRST-PASS string matching before escalating to the model, that first pass must not treat
    "structured field points elsewhere" as sufficient grounds to route a row to
    `diverges_work`/`diverges_part` without also checking the free text for the claimed work's own
    identity.

- **What this plan (136-03) does NOT do:** it does not modify `select_catalogue_divergence_candidates`'s
  selection logic (the over-fire behavior is preserved deliberately, per the instruction above), and it
  does not touch any build/service module. This plan amends `.planning/REQUIREMENTS.md` (Task 2,
  NOVEL-01) and updates the two worked cases' (83, 87) PROPOSAL text in the regenerated worksheet to
  reflect this ruling -- recorded separately below.

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

**SUPERSEDED BY THE LABELLING RESTRUCTURE (this continuation, rulings F/G's sibling instruction) --
retained below as the decision trail, not silently deleted.** Everything under this heading through
the "97 candidates, one Verdict question" framing describes the PRE-restructure worksheet (82
original + Class 6's 15). The worksheet has since been REBUILT to **83 candidates split by question
type**, per the note below. The historical text is kept for provenance; the CURRENT state is
described first.

### Current state (this continuation)

- **83 total candidates, TWO question types, matching how each class was actually constructed:**
  - **Part A -- IDENTITY spot-check, 8 cases** (Class 3 granularity: 3, Class 2 alias: 2, Class 1
    near-miss: 3) -- `same_work` / `different_works` / `unsure` / `skip`. Selected deterministically
    via evenly-spaced sampling (`_evenly_spaced_indices`, `scripts/discovery_gate1_evidence.py`) across
    each class's existing capped candidate pool (unchanged pools: 20 granularity / 12 alias [all of
    them -- the corpus has exactly 12] / 20 near-miss) -- NOT a hand-picked case list, so re-running
    the script against the same asset reproduces the identical 8 cases. This REPLACES full labelling
    of all 52 original Class 1-3 candidates (owner ruling: baking "same work" into the selection
    criterion made full labelling low-signal); the interpretation of the result is fixed in the
    worksheet itself BEFORE any answer comes in: all-`same_work` argues the D-13d collapse rule (276
    of 1,367 groups, 20.2%) is too conservative; even one `different_works` means the full 52-case
    pool needs real labelling after all.
  - **Part B -- NOVELTY SHADE cases, 75 cases** (Class 4 terse catalogue: 20, Class 5 generic
    collection: 25, Class 6 catalogue divergence: 30) -- the nine-value shade vocabulary (below), plus
    a Correctness sub-question on Class 6 rows. EXPANDED from the original 45 (15/15/15), proportioned
    to how genuinely hard/consequential each family is: Class 6 gets the largest expansion (the most
    consequential shade axis, and per ruling G, measurably flawed -- more data helps characterize the
    over-fire rate); Class 5 is second (collection-level ambiguity is genuinely ill-defined, not just
    hard to string-match, and deserves solid coverage); Class 4 is smallest of the three expansions
    (terse/absent catalogue text is real but the most mechanical of the three -- closer to a binary
    "nothing to compare against" signal than a genuinely graded judgment).
- **Vocabulary widens again, per ruling F: NINE shade values** (`confirms` / `refines_granularity` /
  `aid_more_specific` / `diverges_work` / `diverges_part` / `fills_gap` / `extends` / `alias_merge` /
  `not_checked`, `diverges_work`/`diverges_part` REPLACING the single `diverges` token) plus a
  SEPARATE Correctness axis (`catalogue_correct` / `claim_correct` / `unclear`) applicable only to
  Class 6 rows whose shade verdict is `diverges_work`/`diverges_part`. The identity spot-check uses
  its own, unrelated four-token vocabulary (`same_work` / `different_works` / `unsure` / `skip`).
- **Class 6's 12 owner-already-ruled cases carry an UPGRADED PROPOSAL, not a pre-filled verdict.**
  `_CLASS6_OWNER_SCOPE` in `scripts/discovery_gate1_evidence.py` keys the owner's explicit F/G verdicts
  by `sys_id` (stable across a renumbering) for the 7 `diverges_work` + 3 `diverges_part` + 2
  `confirms`-via-free-text cases named in rulings F/G, so the regenerated worksheet's PROPOSAL text
  reflects what the owner has ALREADY said about that specific manuscript, rather than regressing to a
  stale generic "plausibly diverges" draft. The remaining 3 of the original 15 (sys_ids not named in
  either ruling) carry a generic, explicitly-undetermined proposal -- genuinely open, not silently
  resolved. Every one of these remains a `PROPOSAL`, never a pre-filled `Owner verdict`/`Shade verdict`
  cell -- Task 3 still requires the owner's own pass, including CONFIRMING or CORRECTING each upgraded
  proposal.
- **RISK CHECK on the growing shade enum (this plan's own Task 4 instruction -- reported, not
  resolved):** the stored vocabulary is now NINE values
  (`confirms`/`refines_granularity`/`aid_more_specific`/`diverges_work`/`diverges_part`/`fills_gap`/
  `extends`/`alias_merge`/`not_checked`). Assessed candidate pairs for realistic indistinguishability
  by a `gemini-3.6-flash`/effort-low gate:
  - `refines_granularity` vs. `aid_more_specific` (E′'s direction split) and `diverges_work` vs.
    `diverges_part` (F's scope split) are each a SINGLE well-defined binary test (which side is finer;
    same work or different work) layered onto an already-detected relationship -- these are NOT
    expected to be confused with each other MORE than the underlying test already risks, since the
    model is asked one clean question per pair, not required to hold all nine values in mind
    simultaneously for every judgment.
  - The genuinely CLOSE pair, per ruling G's own worked cases, is `confirms` vs. `refines_granularity`/
    `aid_more_specific` when the catalogue's structured field is coarse but its free text is precise --
    this is not a defect in the enum itself, it is exactly the free-text-reading requirement ruling G
    already makes explicit as a PROMPT-DESIGN requirement (present the aid's full free text, not an
    id-only join), so the fix belongs in 136-04's prompt engineering, not in collapsing shade values.
  - `alias_merge` (Class 2's situation -- two work_ids are the SAME work) vs. `refines_granularity`/
    `aid_more_specific` (two RELATED but distinct works at different granularity) is plausibly the pair
    most likely to be confused by a low-effort gate, since both hinge on the same underlying "is this
    really one work under two labels, or two related works" judgment the D-13d title-relation machinery
    was built to approximate -- **no collapse is applied here**; this is reported as a candidate for a
    defensible future collapse (e.g. folding `alias_merge` into `refines_granularity`'s family, or
    demoting `alias_merge` detection to a separate, deterministic string-match step run BEFORE the
    model gate rather than asking the model to distinguish it from a granularity relationship at
    inference time) for the OWNER to decide, not applied unilaterally by this plan.
  - Labelling-CONSISTENCY risk (distinct from the model-confusability risk above): a nine-token
    picklist is measurably more choices than the original tri-state, and the owner is the one hand-
    labelling the 75 shade cases in Task 3 -- the per-class "Plausible shades" hints (narrowing each
    class's realistic answer set to 2-5 tokens) exist specifically to manage this, and are unchanged in
    that role by this restructure (only their content updated for the new tokens).
  - **This assessment is reported for the owner's decision, per this plan's own instruction -- no
    collapse is applied in this plan.**

### Historical (pre-restructure) record, retained for the decision trail

- `discovery_data/novelty_hardcase_labels-v1.json` -- the owner-supplied ground truth, one entry per
  hard case with its verdict, date and `label_provenance`. **Not yet created** (unchanged by this
  continuation -- still pending Task 3).
- The content hash of that file, to be recorded in an update to this section once it exists.
- The effective (as opposed to candidate-pool) evaluation-set size, once the owner has labelled or
  explicitly skipped each of the **83** candidates (superseding the earlier 97-candidate figure --
  see "Current state" above) up to whatever portion they choose to judge.
- **Vocabulary note (updated by correction E′, since further updated by ruling F -- see "Current
  state" above):** the worksheet (`136-NOVELTY-HARDCASES.md`) was, at the E′ stage, regenerated
  against an EIGHT-shade vocabulary carrying `diverges` as a single token; ruling F has since split
  that token into `diverges_work`/`diverges_part` (nine values total) -- the historical EIGHT-value
  figure below is superseded, not re-derived: `confirms` / `refines_granularity` / `aid_more_specific`
  / `diverges` / `fills_gap` / `extends` / `alias_merge`, plus `unsure` / `skip`. The old 4-value
  tri-state-era set (`already_recorded` / `not_in_finding_aids` / `unsure` / `skip`) still does not
  appear anywhere in the current file.

- **XLSX labelling workbook, emitted by the same script -- STRUCTURE UPDATED by this continuation's
  restructure.** The owner reported that Hebrew RTL is hard to work with in Markdown and asked for a
  spreadsheet instead. `scripts/discovery_gate1_evidence.py` now writes a **THREE-sheet**
  `136-NOVELTY-HARDCASES.xlsx` (superseding the earlier two-sheet "Hard Cases"/"Vocabulary"
  structure): **"Identity Spot-Check"** (the 8 Part-A cases, an Identity dropdown), **"Novelty
  Shades"** (the 75 Part-B cases, a Shade dropdown PLUS a Correctness dropdown for Class 6 rows), and
  **"Vocabulary & Instructions"** (both vocabularies, the correctness vocabulary, and the per-class
  hints). Case #s remain GLOBAL and stable across both data sheets (assigned once by
  `assign_case_numbers`, in the fixed `_CLASS_ORDER`), so a case's number identifies it regardless of
  which sheet it lives on. Deterministic at the cell-value/validation-list/sheet-structure level --
  byte-for-byte zip equality is NOT claimed (openpyxl embeds a save timestamp in
  `docProps/core.xml`), so reproducibility is verified at the DECOMPRESSED-content level.

  **Masking note, specific to this artifact (unchanged methodology, re-applied to the new
  structure).** `.xlsx` is a ZIP archive whose inner XML parts are DEFLATE-compressed by default -- a
  raw `check_atlas_masking.py --scan-asset` pass over the `.xlsx` file's OUTER bytes cannot see a
  literal string that is only present, uncompressed, in the workbook's inner XML. The scan must
  additionally run against the workbook's DECOMPRESSED inner text via `zipfile` extraction into a
  scratch file passed as a single explicit `--scan-asset` path. This durable methodology note stands:
  **a bare `--scan-asset foo.xlsx` verifies nothing about its cell text** and must not be relied upon
  alone; `scripts/check_atlas_masking.py` itself was NOT modified (out of scope) to add native
  zip-awareness.

  **Additional finding, this continuation: openpyxl encodes non-ASCII cell text as HTML NUMERIC
  CHARACTER REFERENCES, not raw UTF-8 bytes.** Inspecting this workbook's actual inner
  `xl/worksheets/sheet2.xml` shows every Hebrew string written as decimal `&#1499;&#1502;...`
  sequences (`t="inlineStr"`, no `xl/sharedStrings.xml` at all -- openpyxl wrote inline strings for
  this workbook), never literal Hebrew UTF-8 bytes -- so a decompressed-content scan that only checks
  for LITERAL pattern bytes would still miss a restricted Hebrew pattern hiding in this specific
  encoding. This is NOT a gap in practice: `check_atlas_masking.py` already decodes HTML numeric
  character references as part of its documented "HTML/JS (incl. mixed literal+escaped) forms"
  coverage (`_HTML_NUMREF`, `_deescape_html_js`) from its earlier hardening rounds. Verified with a
  POSITIVE CONTROL this continuation ran directly against this exact encoding shape (per VIS-02's own
  "a positive control confirms the scan would FAIL on a deliberately seeded restricted row"
  discipline): the repo's own gitignored Hebrew mask pattern, hand-encoded as `&#NNN;` decimal
  numeric-character-references exactly as openpyxl writes them, was fed to
  `check_atlas_masking.py --scan-asset` and correctly FAILED (exit 1, `MASK HIT [escape]`) -- proving
  the tool's escape-decoding path is not merely theoretical coverage but actually catches THIS
  concrete artifact's real encoding. The clean (exit 0) result on the actual decompressed workbook
  content is therefore a genuine true-negative, not a blind spot. Recorded here as a durable
  positive-control precedent for any future `.xlsx` masking verification.

- **Task 4's round-trip now reads verdicts back from TWO sheets, not one.** Once the owner returns the
  filled-in workbook, Task 4 (still not implemented by this or any prior continuation, per its own
  instruction) must: (1) read the **Identity verdict** column of "Identity Spot-Check" AND the
  **Shade verdict** + **Correctness** columns of "Novelty Shades", both by **Case #** (never by row
  position alone); (2) treat a truly blank verdict cell as an explicit **skip** for that case (never
  silently filled from its `PROPOSAL` draft); (3) treat a blank Correctness cell as simply "not
  answered / not applicable" -- NOT an error -- on every row whose shade is NOT `diverges_work`/
  `diverges_part`, but flag (not silently accept) a blank Correctness cell on a row that DOES carry one
  of those two shades, since that is a real gap in the label, not a structural non-applicability; (4)
  reject any Identity cell value outside the four-token identity vocabulary and any Shade cell value
  outside the ten-token shade vocabulary (defense in depth behind the DataValidation); (5) write
  `discovery_data/novelty_hardcase_labels-v1.json` with `label_provenance` recording the XLSX origin,
  carrying BOTH the shade/identity verdict and (where applicable) the correctness call per case; the
  Markdown file remains the authoritative human-readable record, the XLSX the labelling INSTRUMENT.

  **Foreseen blocker, flagged rather than solved here (unchanged from the pre-restructure record,
  still applies to the new three-sheet structure):** round-tripping a `.xlsx` the owner has actually
  opened and edited in a real spreadsheet application is a materially less controlled input surface
  than reading back a script's own Markdown -- the application MAY re-save with different sheet
  names, MAY not preserve DataValidation on re-open, and a formula-injection-style value could appear
  in a free-text column. Task 4 should read via `openpyxl` in `data_only=True` mode and fail closed on
  a missing/renamed sheet, a missing Case-# column, an out-of-vocabulary value, or a Case # that does
  not match any emitted case. None of this is implemented here.

This record will be updated (not silently overwritten) once plan Task 3 returns and Task 4 writes
the label file.
