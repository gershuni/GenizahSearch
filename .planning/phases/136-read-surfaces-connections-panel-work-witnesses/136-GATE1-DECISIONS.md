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
  hard case with its verdict (one of the eight decision-E/E′ shades, or `unsure`), date and
  `label_provenance`. **Not yet created.**
- The content hash of that file, to be recorded in an update to this section once it exists.
- The effective (as opposed to candidate-pool) evaluation-set size, once the owner has labelled or
  explicitly skipped each of the **97** candidates (82 original + 15 Class 6, added under decision
  E) up to whatever portion they choose to judge.
- **Vocabulary note (updated by correction E′):** the worksheet (`136-NOVELTY-HARDCASES.md`) has
  been REGENERATED against the corrected EIGHT-shade vocabulary (`confirms` / `refines_granularity`
  / `aid_more_specific` / `diverges` / `fills_gap` / `extends` / `alias_merge`, plus `unsure` /
  `skip`) -- `aid_more_specific` is the one new row E′ adds; every other shade is unchanged from
  decision E. The old 4-value tri-state-era set (`already_recorded` / `not_in_finding_aids` /
  `unsure` / `skip`) still does not appear anywhere in that file. All **97** cases are
  content-unchanged from the decision-E worksheet -- only the vocabulary table and the per-class
  "Plausible shades" hints changed (`aid_more_specific` added to the Class 3 and Class 6 hint lists,
  the two classes where it is genuinely plausible: Class 3's own algorithm already tests the D-13d
  title relationship the shade is defined over, and Class 6 is precisely the boundary where the
  algorithm's own `diverges` classification could, on owner review, turn out to be a missed
  granularity relationship instead). No verdict has been pre-filled anywhere; every `PROPOSAL` draft
  remains explicitly marked as a draft, never a label. Reproducibility re-verified: two consecutive
  script runs against the same asset produced byte-identical Markdown.

- **NEW deliverable: an XLSX labelling workbook, emitted by the same script.** The owner reported
  that Hebrew RTL is hard to work with in Markdown and asked for a spreadsheet instead.
  `scripts/discovery_gate1_evidence.py` now also writes
  `136-NOVELTY-HARDCASES.xlsx` alongside the Markdown (same phase directory; same `cases` data,
  same 97-case content, same class order and case numbering -- the two files are guaranteed to
  agree case-for-case by construction, since both render from one shared, pre-numbered case list).
  RTL sheet view, wrapped text, frozen header + the Case-number/Verdict columns, autofilter, a
  **Verdict** column with an openpyxl `DataValidation` in-cell dropdown restricted to the full
  vocabulary (the seven real shades + `unsure` + `skip` -- nine tokens; blank is allowed by the
  validation itself, since "not yet answered" is a legitimate transient state while the owner works
  through the sheet, but a second sheet explains in words that a blank is NOT a label and `unsure`
  is the real answer for "cannot tell"). A second "Vocabulary & Instructions" sheet carries the same
  shade table, the same per-class plausible-shade hints, and that blank-is-not-a-label note.
  Deterministic: two consecutive runs produce the same logical workbook content (cell values,
  validation list, sheet structure) -- byte-for-byte zip equality is NOT claimed (openpyxl embeds a
  save timestamp in the workbook's internal `docProps/core.xml`), so reproducibility for this
  artifact is verified at the DECOMPRESSED-content level, not the raw-file-bytes level.

  **Masking note, specific to this artifact.** `.xlsx` is a ZIP archive whose inner XML parts are
  DEFLATE-compressed by default -- a raw `check_atlas_masking.py --scan-asset` pass over the `.xlsx`
  file's OUTER bytes was measured (this plan) to be unable to find a literal string that IS present,
  uncompressed, in the workbook's inner XML (verified with a synthetic marker string: present in the
  decompressed `xl/worksheets/sheet1.xml`, ABSENT from the raw file bytes). The scan was therefore
  additionally run against the workbook's DECOMPRESSED inner text (the actual masking-relevant
  surface) via `zipfile` extraction into a scratch file passed as a single explicit `--scan-asset`
  path (a single explicit file is always scanned regardless of suffix, per that script's own
  `scan_asset` logic) -- both the naive raw-file pass and the decompressed-content pass came back
  clean. This is recorded here as a durable methodology note: **a bare `--scan-asset foo.xlsx` on
  this or any future `.xlsx` artifact verifies nothing about its cell text** and must not be relied
  upon alone; `scripts/check_atlas_masking.py` itself was NOT modified by this plan (out of this
  continuation's stated scope) to add native zip-awareness -- that remains a gap for a future plan
  to close if `.xlsx` exports become a recurring surface.

- **Task 4's round-trip now reads verdicts back FROM the XLSX, not the Markdown.** Once the owner
  returns the filled-in workbook, Task 4 (still not implemented by this continuation, per its own
  instruction) must: (1) read the **Verdict** column of the returned `.xlsx` by **Case #**, matching
  each row back to its case by the same stable `case_num` the emitting script assigns (never by row
  position alone, in case the owner reorders/filters/hides rows while working); (2) treat a truly
  blank Verdict cell as an explicit **skip** for that case (never silently filled from its
  `PROPOSAL` draft); (3) reject any cell value outside the nine-token vocabulary (defense in depth
  behind the DataValidation, in case the owner pastes a value or a spreadsheet application silently
  drops the validation on save/re-open); (4) write `discovery_data/novelty_hardcase_labels-v1.json`
  with `label_provenance` recording that the label came from the returned XLSX (not the Markdown),
  and the Markdown file's `136-NOVELTY-HARDCASES.md` remains the authoritative human-readable record
  of the candidate set and its "why it is hard" reasoning -- the XLSX is a labelling INSTRUMENT, the
  Markdown is the record.

  **Foreseen blocker, flagged rather than solved here (per this continuation's own instruction):**
  round-tripping a `.xlsx` the owner has actually opened and edited in a real spreadsheet
  application (Excel, LibreOffice, Google Sheets via download/upload) is a materially less
  controlled input surface than reading back a script's own Markdown -- the application MAY
  re-save with a different sheet name, MAY not preserve the DataValidation on re-open depending on
  the application, and a formula-injection-style value (e.g. a cell starting `=`) could appear in a
  free-text column if the owner ever types into one, though `openpyxl` reads cell VALUES (not
  live-recalculated formulas from another app) so this is a lower-severity concern for reading
  values back than for writing them. Task 4 should read via `openpyxl` in `data_only=True` mode and
  fail closed (never silently coerce) on: a missing/renamed sheet, a missing Case-# column, a
  Verdict value outside the nine-token vocabulary, or a row whose Case # does not match any emitted
  case. None of this is implemented here -- it is recorded so Task 4's own plan text is written
  against a real, already-identified risk list rather than discovering it fresh.

This record will be updated (not silently overwritten) once plan Task 3 returns and Task 4 writes
the label file.
