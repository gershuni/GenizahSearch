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

**Further addendum, 2026-08-02 (same day, after a READ-ONLY research pass) -- RULINGS H AND I.** A
read-only prior-art reconciliation pass (`136-NOVELTY-PRIOR-ART.md`, commit `d9e3ff79`) found that
decisions E/E′/F/G had been recorded but never propagated into the documents that implement them (the
plans, the schema contract, ROADMAP.md), and separately surfaced two open questions the owner had not
yet ruled on: a real, measured "witness" shape (a catalogue naming a broader liturgical container that
predicts a specific unit without naming it) with no shade to hold it, and an unresolved gap between the
pinned novelty gate's validated scope (the five-way vocabulary, the one-title-string input contract) and
what it is now being asked to do (the widened shade enum, ruling G's free-text input contract). The
owner ruled on both, delivered through a normal orchestrator dispatch to the continuation that performs
the reconciliation (the correct channel; see decision E's own Provenance note for why that distinction is
load-bearing): **H** adds a TENTH shade, `container_predicts`, under a name chosen specifically to avoid
colliding with the five OTHER meanings "witness" already carries in this project -- see **section H**
below. **I** conditions the "run now, pinned" authorization in section B above on a RE-MEASUREMENT of the
pinned gate against the widened vocabulary and input contract, using the owner-labelled evaluation set,
BEFORE the production run -- see **section I** below. This continuation ALSO performs the surgical
reconciliation the prior-art pass found overdue: `136-04-PLAN.md`, `136-12-PLAN.md`,
`docs/specs/discovery-sidecar-schema-v1.md`, `.planning/ROADMAP.md` and `.planning/REQUIREMENTS.md` are
brought into agreement with the final ten-value enum, and the hard-case worksheet/workbook are
regenerated to carry the new shade and a new liturgical-container class (Class 7). Sections A-G are NOT
reopened by any of this; they remain LOCKED exactly as recorded above.

**Further addendum, 2026-08-02 (same day, a later continuation still) -- RULING J.** The prior-art pass's
own §7 flagged an open design question decisions A-I never settled: does the LLM arm run over ALL
identifications, or only a heuristic-funnel residual? The owner ruled on it, delivered through a normal
orchestrator dispatch to this continuation (the correct channel; see decision E's own Provenance note) --
**J** adopts the funnel-FIRST architecture (heuristic funnel runs first to cut calls; the LLM sees only
the residual) and records, as the real cost of that choice, that a heuristic FALSE-KNOWN is now
PERMANENT and UNRECOVERABLE -- see **section J** below. This continuation ALSO replaces the hard-case
pool's former Classes 4/5/7 with a THREE-ARM, SOURCE-STRATIFIED sample built against the REAL bib/PGP/
FGP/FJMS-catalogue sidecars (not merely libraries.csv), per the same prior-art pass's finding that the
former pool had zero representation of the source-coverage failure modes Codex measured as most
damaging. Sections A-I are NOT reopened by any of this; they remain LOCKED exactly as recorded above.

**Further addendum, 2026-08-03 (a later, directly-dispatched continuation, following the ruling-I
re-measurement in `136-NOVELTY-RUN.md` §§ 2-3) -- RULINGS K AND L.** After reading the re-measurement's
real, measured results, the owner issued two further rulings, delivered through a normal orchestrator
dispatch (the correct channel; see decision E's own Provenance note) -- **K** keeps the re-derived
~$301 production run UNAUTHORIZED: the 60-case re-measurement scored 47/60 (78.3%) with ZERO
false-novel errors, but the pool contained ZERO true `fills_gap` cases, so the axis that matters most
was never exercised; a purpose-built probe measuring the false-novel rate on the ACTUAL candidate
population (both the model path and the ungated no-source-text bypass path) must run first -- see
**section K** below and `136-NOVELTY-RUN.md`'s new probe section. **L** drops `divergence_correctness`
from the model's job entirely (measured 8/28, at/below chance, against the owner's own 31/32 on the
identical cases) -- it remains a human/owner annotation only; the stored column and every owner value
already collected are unchanged, but the pinned prompt and `resolve_model_output` no longer elicit or
carry it (new prompt hash) -- see **section L** below. Sections A-J are NOT reopened by any of this;
they remain LOCKED exactly as recorded above.

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

## H. Adopt the `witness` concept as a shade, under a NON-COLLIDING name -- `container_predicts`

**Provenance.** This ruling arrived after a READ-ONLY research pass, `136-NOVELTY-PRIOR-ART.md` (commit
`d9e3ff79`), which reconciled the in-conversation decisions E/E′/F/G against substantial prior novelty
engineering already sitting in this repo (chiefly the gitignored `same_work_spike/probe/` research tree)
that was never consulted when those decisions were made. The ruling itself was delivered through a
normal orchestrator dispatch to the continuation performing the reconciliation -- the correct channel,
per decision E's own Provenance note -- and is recorded here with the same standing as A-G: LOCKED, not
re-litigated, re-derived or "improved" here.

- **Question:** the prior-art pass measured a real, non-trivial "witness" shape under the OLDER five-way
  title-gate vocabulary (`same_work_spike/probe/scripts/title_gate_llm.py`) -- 1,327 of 20,410 in-scope
  rows (6.5%) in the population that gate actually scored -- that the CURRENT nine-value shade enum has
  no bucket for: a catalogue entry names a broader rite/cycle/ceremony/container whose STANDARD,
  PREDICTABLE content includes a specific unit, without the catalogue ever naming that unit itself (e.g.
  `יוצר ח פסח` where the aid names `מחזור מנהג אשכנז לשלש רגלים`; `יוצרות לשבתות` where the aid names
  `סדור מנהג אשכנז המזרחי`). Under the current enum these rows fall through to `fills_gap` by
  elimination -- publishing a standard machzor/siddur component as a "Candidate for new finds." Does the
  project adopt this as a shade?

- **Owner's answer (verbatim, condensed for this record):** "Adopt the `witness` concept as a shade,
  under a NON-colliding name." The owner explicitly declined the literal name `witness`: it already
  carries FIVE distinct meanings in this project, catalogued by the prior-art pass's own §8 sweep --
  (1) the shipped `claim_type`/`evidence_kind` enum member `direct_witness` (a span-competition rule);
  (2) the schema's `evidence_source=propagated` "witness family" (§4.2), itself built from a DIFFERENT
  upstream `_bucket=='witness'` router classification in the SEED-029/Q2 pipeline; (3)
  `HANDOFF-TO-135.md`'s informal prose "the page **witnesses** the work" (the GEN2 `coverage_route`
  surface label `same_work`, D-01-deferred, not consumed in 136); (4) the OLDER MAPV2-15o LLM title-gate's
  `witness` VERDICT itself (the concept this ruling adopts -- never shipped); (5) the owner's own 10-way
  grading vocabulary's `co-witness` QC label. A sixth meaning of the same word is not acceptable.
  **Chosen name: `container_predicts`** (the suggested default; `predictable_context` was offered as the
  alternative). Verified to collide with nothing else in the codebase or specs before being pinned
  (`grep -rn "container_predicts" --include='*.py' --include='*.md'` returns only this ruling's own
  occurrences at the time of recording).

- **Date:** 2026-08-02 (same day as A-G, after the prior-art research pass).

- **Adopted DEFINITION -- verbatim from the prior measured work, only the name changes.** *"An aid names
  a broader rite/cycle/ceremony/container whose standard, predictable content includes this specific
  unit, without naming the unit itself."* No re-derivation, no re-scoping -- the five-way gate's own
  `witness` verdict condition (`title_gate_llm.py`'s `SYS_PROMPT`, quoted in `136-NOVELTY-PRIOR-ART.md`
  §3) is carried forward unchanged in substance.

- **Treatment -- excluded from candidates, but NOT hidden by default (this distinction is the load-bearing
  point of this ruling, recorded explicitly so a later implementer does not generalise ruling F's
  posture onto it).** `container_predicts` joins `confirms` / `refines_granularity` / `aid_more_specific`
  / `diverges_work` / `diverges_part` / `extends` as EXCLUDED from the "Candidates for new finds"
  toggle -- `fills_gap` remains the ONLY shade that predicate selects. UNLIKE `diverges_work` /
  `diverges_part`, `container_predicts` rows are shown NORMALLY on every surface -- NOT hidden behind
  ruling F's default-hidden, explicit-warned toggle. **Ruling F's default-hidden posture was specifically
  about rows the owner has MEASURED REASON to believe are OUR false positives** (reading the real Class-6
  cases, the owner found the catalogue is usually right when it disagrees with a claim) -- a genuine
  disagreement between the aid and the claim that the system must not silently adjudicate. **That
  reasoning does not apply here: there is no disagreement.** The aid and the claim are CONSISTENT -- the
  container predicts the unit, it just doesn't name it. Hiding a `container_predicts` row by default would
  misapply F's rationale to a shade where it does not hold, and is explicitly NOT authorized by this
  ruling.

- **Code consequence -- an enum widening, not a new axis.** The shade enum widens from NINE to TEN
  values: `confirms` / `refines_granularity` / `aid_more_specific` / `diverges_work` / `diverges_part` /
  `container_predicts` / `fills_gap` / `extends` / `alias_merge` / `not_checked`. `container_predicts` is
  inserted immediately before `fills_gap` in the enum's canonical ordering -- the shade it would otherwise
  be misfiled into by elimination. `novelty_source_label` populates on `container_predicts` exactly as it
  already does on `confirms` / `refines_granularity` / `aid_more_specific` / `alias_merge` / `extends` /
  `diverges_work` / `diverges_part` (an aid says SOMETHING nameable -- the container's own name -- about
  this fragment-work pair, even though it does not name the specific unit); this is a new instance of the
  SAME rule decision E already established for "every shade where some finding aid says something," not
  an exception.

- **Downstream contracts this decision amends** (same enumeration shape as decisions E/E′/F, extended):

  1. **D-23a** -- the enum descriptor is further amended from "NINE-VALUE SHADE ENUM, direction-split
     granularity AND scope-split divergence, plus an orthogonal correctness field on divergence rows" to
     "TEN-VALUE SHADE ENUM," adding the same-shown-normally `container_predicts` shade.
  2. **NOVEL-01** -- gains a further dated `⟨AMENDED 2026-08-02 -- H⟩` sub-bullet (see
     `.planning/REQUIREMENTS.md`).
  3. **The `novelty_status` CHECK constraint and its index** (`docs/specs/discovery-sidecar-schema-v1.md`)
     -- the `IN (...)` list widens from nine to ten values in both places the schema doc states it
     (currently STALE at the pre-E three-value tri-state in both places -- see the reconciliation this
     same continuation performs, tracked separately from this decision record).
  4. **The frozen-enum-vocab readiness check** (`web/discovery_assets.py::discovery_available()`) -- when
     wired (per decision E's item 4), the frozenset checked must be the TEN-value shade set.
  5. **D-23c's pinned LLM contract** -- the prompt must now also be able to recognise and elicit the
     container-predicts relationship (does the aid name a broader rite/container whose standard content
     predicts this unit, without naming it?) alongside every other shade test. The PINNED PROMPT HASH
     changes again on this account (the fourth net change across E/E′/F/H, still pinned ONCE at 136-04,
     never sequentially).
  6. **The D-02b rebuild-preservation gate allowlist** (plan 136-05) -- covers the TEN-value shade set on
     the same `novelty_status` column-keyed entry decision E already named; no further code change needed
     in 136-05 itself.

- **Plans that must implement this ruling:** the SAME plans decision E already named (136-04, 136-06,
  136-12, the release verifier, and the unaffected-in-shape UI consumers) now build the TEN-value shade
  classifier instead of the nine-value one, with `container_predicts` rendered normally (not behind
  ruling F's toggle) wherever `diverges_work`/`diverges_part` are rendered behind it.

- **What this plan (136-03) does NOT do:** exactly as decisions E/E′/F already state -- 136-03 does not
  edit `web/discovery_assets.py`, `scripts/verify_discovery_sidecar.py`, or any build/service module.
  UNLIKE E/E′/F, this continuation's own dispatch DOES additionally reconcile
  `docs/specs/discovery-sidecar-schema-v1.md`, `136-04-PLAN.md`, `136-12-PLAN.md` and
  `.planning/ROADMAP.md` to the current enum -- an explicit, narrow exception to the "records the ruling,
  does not implement it" convention A-G established, authorized by the objective that dispatched this
  continuation (the prior-art pass's own headline finding was that exactly this propagation had been
  skipped). This plan (its Task 2) amends `.planning/REQUIREMENTS.md`, and (its Task 3) reissues
  `136-NOVELTY-HARDCASES.md`/`.xlsx` via the script to carry `container_predicts` and a new Class 7 --
  see ruling I below and the "XLSX round-trip" note.

---

## I. Re-measure the pinned gate on the WIDENED task before the production run

- **Question:** decision B above authorized "RUN NOW, PINNED" based on the pinned config's prior
  validation (40/40 verdict agreement with a fuller-thinking reference config; that reference config
  itself validated at 99% against 103 human grades). That validation was measured on the FIVE-way
  vocabulary and a ONE-title-string input contract. Since then, the shade enum has widened to TEN values
  (E/E′/F/H) and ruling G mandates reading the aid's free text alongside its structured field. Does the
  prior validation still license running the pinned config on the WIDENED task, or does it need
  re-measuring first?

- **Owner's answer (verbatim, condensed):** decision B's "run now, pinned" stands as an intention, but is
  now CONDITIONED: run the pinned config against the owner-labelled evaluation set on the NEW vocabulary
  and the NEW input contract FIRST, and only THEN authorize the production run. The prior validation
  covers a DIFFERENT, narrower question than the one the production run will actually ask; the pinned
  model has never been measured on the ten-value enum or the free-text-reading contract.

- **Date:** 2026-08-02 (same day as A-H, after the prior-art research pass).

- **What is NOT reopened.** Decision B's model/version/effort pin (`gemini-3.6-flash`,
  `reasoning:{effort:"low"}`) is UNCHANGED -- ruling I is a re-measurement GATE on top of the existing
  pin, not a new model authorization, and does not reopen "do NOT downgrade the model." The `~$27` cost
  figure on record (`reference_discovery_llm_gate_cost`, cited in decision B) is EXPLICITLY a COST
  estimate carried forward by size-extrapolation -- it has never been, and must never be cited as, an
  ACCURACY measurement of this model on this task. Confusing the two is precisely the error this ruling
  exists to prevent: `136-NOVELTY-PRIOR-ART.md` §5c independently reached the same posture and is the
  proximate cause of this ruling.

- **Consequence for the evaluation set (per this ruling and per `136-NOVELTY-PRIOR-ART.md` §5c/§7's own
  recommendation).** The hard-case evaluation set gains a SEVENTH class -- **Class 7, liturgical-container
  predictability** (the `container_predicts` shape ruling H names) -- built with the IDENTICAL
  zero-model-call, script-reproducible selection discipline already used for Classes 4-6 (see
  `select_liturgical_container_candidates` in `scripts/discovery_gate1_evidence.py`), target ~12 cases.
  This is NOT part of the Classes-4-6 45→75 expansion decision C/the labelling restructure already
  authorized -- it is a NEW class, added specifically so the model's FIRST encounter with the
  container-predicts question is a graded evaluation, never production. Measured outcome (this
  continuation): **12** Class 7 candidates, selected on a genuinely NAMED standard-rite container
  collocation (a container noun immediately followed by `מנהג` -- the exact shape of both of the owner's
  own worked H examples) with the claimed work's own title not already named in the catalogue text,
  grouped by claimed work and round-robined (largest group first) so the corpus's dominant instance
  (Psalms, by a wide margin) does not crowd out the other eleven. **Total candidate pool: 95** (83 + 12;
  every one of the 83 kept unchanged in content -- verified by re-running the script and diffing the
  regenerated worksheet against the pre-H version, Classes 1-6 byte-identical).

- **Gating relationship to decision B and to plan 136-04's Task 3.** Plan 136-04's Task 3 ("Run the
  authorized funnel...") must, before authorizing the production run, first run the pinned config against
  the FULL 95-case owner-labelled evaluation set (once the owner labels it) on the CURRENT ten-value
  vocabulary and the free-text input contract, and report agreement against the owner's labels using the
  SAME two-directional-error discipline Task 2's grading harness already implements. Only after that
  re-measurement is on the record does decision B's "run now, pinned" authorization become operative for
  the full production run. This is a NEW acceptance criterion for 136-04, recorded here and reconciled
  into `136-04-PLAN.md` by this same continuation (see the plan's own updated Task 3 acceptance criteria).

- **Code consequence.** No enum value changes here (this ruling is about a MEASUREMENT gate, not a new
  shade or column). The consequence is entirely procedural: 136-04's Task 3 may not authorize the
  production run merely because decision B was recorded; it must first produce and record a
  re-measurement report against the CURRENT vocabulary/contract, using the CURRENT (95-case) evaluation
  set, before proceeding.

- **What this plan (136-03) does NOT do:** it does not run the pinned model against anything (no model
  call is made by this continuation, consistent with every prior continuation of this plan) and it does
  not itself perform the re-measurement -- that is 136-04's Task 3's job, now conditioned by this ruling.
  This plan (its Task 3) adds Class 7 to the candidate pool and regenerates the worksheet/workbook so the
  owner's eventual labelling pass covers it.

---

## J. The LLM arm runs ONLY on the heuristic-funnel residual, not on all identifications -- and the
unrecoverable-false-known consequence this creates

**Provenance.** This ruling closes the open design question `136-NOVELTY-PRIOR-ART.md` § 7 flagged and
explicitly declined to resolve: *"Whether the model arm runs over ALL claims or only a heuristic-funnel
residual is not yet decided in any plan text."* It arrived through a normal orchestrator dispatch to this
continuation (the correct channel; see decision E's own Provenance note for why that distinction is
load-bearing) and is recorded here with the same standing as A-I: LOCKED, not re-litigated, re-derived or
"improved" here.

- **Question:** does the pinned LLM gate (ruling B) run its judgment over EVERY identification NOVEL-01
  checks, or only over the residual the heuristic funnel could not resolve mechanically?

- **Owner's answer (verbatim):** "Makes sense to use LLM only after the heuristics."

- **What this adopts.** `same_work_spike/probe/rsource/GEN2-HANDOFF.md` § 6's own recommendation:
  "heuristic funnel first to cut calls; scope the LLM pass to shipped/same-work headline claims." The
  heuristic funnel (bib/catalogue/FGP/PGP mechanical name-match, per `gen2_novelty_gate.py`'s reference
  design and the checked-source set NOVEL-01 names) runs FIRST, over every identification. Only the rows
  it cannot resolve mechanically -- the RESIDUAL -- are ever presented to the pinned model. Rows the
  funnel resolves (in either direction: a genuine name-match, or a heuristic demotion) never reach the
  model at all.

- **The consequence that must be recorded explicitly, because it is the real cost of this choice and is
  otherwise invisible.** The funnel only ever DEMOTES (discovery -> known/checked-off, never the
  reverse -- this has been the funnel's stated design principle since `discovery_identified_gate.py`'s
  own docstring: "Sources only ever demote, never the reverse"). Under a funnel-first architecture, the
  LLM only ever sees SURVIVORS of that demotion pass. **Therefore a heuristic FALSE-KNOWN -- a row the
  mechanical funnel wrongly marks "already recorded" because a source merely has SOME text, not because
  that text actually names this specific work -- is now PERMANENT and UNRECOVERABLE.** Nothing downstream
  ever re-examines a demoted row; no model verdict is ever computed for it; the row is marked "already
  recorded" and stays that way. Codex measured this exact population in the reference implementation:
  **3,688 `published_full` false-knowns** (bib presence alone, Codex finding 1) and **2,014 PGP
  false-knowns** (PGP description/transcription presence alone, Codex finding 6), of which **942** are
  PGP-sole (no other source also names the claim). Under the pre-J (all-claims-to-model) design, at
  least some of these rows WOULD have reached the model and had a chance to be corrected; under the
  funnel-first design ruling J adopts, they never will.

- **The error runs in the CONSERVATIVE direction -- correct given this phase's publication posture, but a
  real cost, not a free one.** A false-known means a genuine finding is silently LOST (never surfaced as
  a candidate), never that a fake finding is manufactured and published. Per the standing cost-asymmetry
  rationale already on record for the model-gate authorization (decision B: "the error this axis makes --
  telling a reader a finding is unrecorded when it is recorded -- is the reputationally expensive one"),
  losing a real finding is preferable to publishing a false one. But "preferable" is not "free": this is a
  measured, real cost (up to several thousand real findings silently never surfacing), and it MUST be
  measured going forward, not assumed away because the direction of the error happens to be the safer
  one.

- **Date:** 2026-08-02 (same day as A-I, a later dispatch still).

- **What this ruling directly affects, enumerated per this continuation's own instruction:**

  1. **The `~$27` cost basis** (`reference_discovery_llm_gate_cost` memory; decision B). That estimate
     was carried forward by size-extrapolation over the FULL identification set. Under ruling J's
     funnel-first design, the model only ever sees the RESIDUAL -- a strict subset of all identifications
     -- so **the true denominator shrinks to the residual, not the full set.** The `~$27` figure must be
     re-derived (or at minimum re-scoped and re-labelled) against the ACTUAL residual size once the
     heuristic funnel runs for real (plan 136-04); citing it against the full identification count is now
     stale and must not be repeated without this caveat.
  2. **Which Codex-flagged defects ever reach the model.** Codex findings 1 and 6 (bib `published_full`
     over-demotion, PGP over-demotion) are now, structurally, defects the MODEL CAN NEVER CATCH -- a row
     they wrongly demote never reaches the model's judgment at all. Findings 2/3/4/5 (the catalogue field,
     canonical-id collapse, the page-join, the residual's own evidence-assembly quality) all concern rows
     that DO reach the model (the residual), so those remain correctable by 136-04's implementation. This
     is why this continuation's own hard-case redesign (the three-arm sampler, see below) treats Arm 2
     (heuristic-demoted) as a SEPARATE, DEDICATED measurement -- it is the ONLY place a false-known from
     findings 1/6 can ever be caught, precisely because ruling J makes it permanent everywhere else.
  3. **NOVEL-01's honesty wording.** A row demoted by the heuristic funnel receives NO model verdict at
     all -- its stored `novelty_status` reflects the FUNNEL's mechanical judgment, never a model's. Any
     future documentation or UI copy describing how a `novelty_status` value was determined must account
     for this: some rows are funnel-only (never modeled), others are funnel-then-model (the residual).
     `docs/specs/discovery-novelty-v1.md` (136-04's own contract doc) must state this distinction
     explicitly, not imply every shade passed through the same pipeline stage.

- **Code consequence.** No enum value changes here (this ruling is about PIPELINE ORDER, not a new shade
  or column). The consequence is entirely architectural: plan 136-04's funnel runner must implement the
  heuristic pass FIRST and gate the model call on "did the heuristic pass leave this row unresolved" --
  this is now a REQUIRED acceptance criterion for 136-04 (reconciled into `136-04-PLAN.md`'s Task 3 action
  text by this continuation, replacing its prior "run the authorized funnel over the full identification
  set" wording with residual-only, funnel-first wording -- a surgical edit, not a restructuring of that
  plan).

- **Consequence for the hard-case evaluation set (this continuation's own Task 3/4 -- the ground-truth
  labelling instrument).** Per `136-NOVELTY-PRIOR-ART.md` § 7's own recommendation, the labelling pool is
  restructured into a **three-arm, SOURCE-STRATIFIED sample** that measures what the funnel-first
  architecture actually needs measured, rather than continuing to test only the catalogue-text axis
  (former Classes 4/5/7, which read exclusively `libraries.csv` column 7 and had ZERO representation of
  the bib/PGP/FGP failure modes Codex measured as most damaging):

  - **Arm 1 -- RESIDUAL** (rows that WOULD reach the model under ruling J): stratified by which source
    supplied text that failed mechanical name-match -- `bib_sole` / `pgp_sole` / `fgp_sole` /
    `catalogue_sole` / `multi_source`, plus two shape-based strata folding in the former Class 4
    (`terse_catalogue`) and Class 7 (`container_predicts`) as strata of the residual, per this ruling's
    own instruction, rather than as a separate exercise. Measures the model arm's accuracy on the ONLY
    population it will ever actually see.
  - **Arm 2 -- HEURISTIC-DEMOTED** (rows the funnel marks known; the LLM NEVER sees them): oversamples
    `published_full`-sole and PGP-sole demotions -- the two Codex-flagged populations named above. This is
    the ONLY arm that can surface a false-known, because nothing downstream re-examines a demoted row.
    Carries a NEW owner-facing question, `demotion_correct` / `false_known` / `unsure` / `skip`
    (`DEMOTION_VOCABULARY`), since this is a straight correctness check on the demotion itself, not a
    shade or an identity judgment.
  - **Arm 3 -- NO-SOURCE-TEXT** (rows with no checked-source text at all): ship as novelty candidates
    automatically, with NO verdict collected -- checks, informationally, whether that bypass looks safe,
    rather than producing a graded label.

  Measured this continuation (`scripts/discovery_gate1_evidence.py`, real bib/PGP/FGP/FJMS-catalogue
  sidecars joined to the live `discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff.db`
  asset): **101 total candidates** -- 8 identity spot-check (UNCHANGED) + 30 Class 6 catalogue divergence
  (UNCHANGED, per this continuation's own "do not silently discard owner-authorized work" accounting) +
  30 Arm 1 residual (across 6 of 7 strata at a 5-case cap each; `pgp_sole` populated ZERO candidates in
  Arm 1 -- see the note below, not a bug) + 25 Arm 2 heuristic-demoted (`published_full_sole` 10 /
  `pgp_sole` 10 / `other_demotion` 5) + 8 Arm 3 no-source-text.

  **A stratum that cannot be populated from available data is reported as zero, never backfilled from
  elsewhere (per this continuation's own instruction).** Arm 1's `pgp_sole` stratum measured ZERO
  candidates: empirically, a PGP-linked fragment whose document has NEITHER a description NOR a
  transcription (the condition for PGP "present but not named") is rare in this corpus -- PGP's
  "present" and "named" tests are close to synonymous under the current (Codex-flagged over-broad)
  heuristic, so PGP-present rows overwhelmingly land in Arm 2 (heuristic-demoted) rather than Arm 1
  (residual). This is itself a measured finding worth recording: it means Arm 2, not Arm 1, is where PGP's
  behavior is actually exercised and graded.

  Superseded/kept/dropped accounting (per this continuation's Task 5 instruction, restated here for a
  single citable record): Classes 1-3 (identity) and Class 6 (catalogue divergence, including the owner's
  F/G annotations on 12 of the original 15 cases) are KEPT UNCHANGED -- Class 6 specifically because
  rulings F/G already characterize real, specific manuscripts and dropping it would discard that
  owner-authorized work before its own Task-3 confirmation ever ran. Class 4 (terse/missing catalogue
  text) and Class 7 (container_predicts) are FOLDED IN as Arm 1 strata, per this ruling's own instruction.
  Class 5 (generic collection works) is DROPPED outright: no owner ruling exists for any specific Class 5
  case (only generic PROPOSALS), and its collection-level-identity question is orthogonal to source
  coverage -- it does not correspond to any of the three arms.

- **Sizing, and what each arm can and cannot answer (per this continuation's own instruction -- stated
  here and in `136-NOVELTY-HARDCASES.md`'s own intro, not resolved by fiat).** Total candidate pool:
  101 (8 identity + 93 novelty-evaluation, of which 30 is the unchanged Class 6 and 63 is ruling J's own
  new three-arm sample -- under the ~100-novelty-case guidance once Class 6's pre-existing, separately
  engaged work is counted apart from the new redesign).
  - Class 6 (30, unchanged) answers whether the owner confirms the shade/correctness proposals already
    characterized on specific real cases, and how the selector's own measured over-fire rate holds up
    across the pool -- it does NOT test source coverage.
  - Arm 1 (30 across 6 populated strata) answers a per-stratum ACCURACY question ("does the model
    classify a representative case from each source family/shape correctly") -- it does NOT establish a
    corpus-wide RATE for how common each stratum is (the cap is fixed, not proportional).
  - Arm 2 (25, oversampling the two Codex-flagged populations) answers "of the rows the funnel
    demotes WITHOUT ever consulting a model, how many are false-knowns" on a small, oversampled slice --
    it does NOT give a project-wide false-known RATE (Codex's own 3,688/2,014 population counts are
    corpus-wide; this arm samples a tiny, deliberately oversampled fraction of each, never the full
    population).
  - Arm 3 (8, no verdict) answers, qualitatively, whether the "ship with no check at all" bypass looks
    safe -- it is explicitly NOT a labelling exercise and produces no graded number.
  - **What this sizing does not cover:** no arm measures a corpus-wide base rate; a future pass wanting
    base rates must run the real funnel (plan 136-04) over the full corpus and report its own per-stratum
    counts, not re-derive them from this labelling sample.

- **What this plan (136-03) does NOT do:** it does not run the pinned model against anything (no model
  call is made by this continuation, consistent with every prior continuation of this plan). This
  continuation DOES (per its own Task 1-5 instructions): amend `.planning/REQUIREMENTS.md` (NOVEL-01),
  surgically reconcile `136-04-PLAN.md`'s Task 3 action text to residual-only/funnel-first wording, and
  extend `scripts/discovery_gate1_evidence.py` with the three-arm stratified sampler, regenerating both
  `136-NOVELTY-HARDCASES.md` and `.xlsx` (verified reproducible across two consecutive runs, byte-for-byte
  on the Markdown/evidence brief; the XLSX is reproducible at the cell-value/validation-list/sheet-
  structure level, per its own pre-existing "not byte-for-byte, openpyxl embeds a save timestamp"
  methodology note).

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

**Updated by rulings H and I (this continuation) -- retained above unchanged, not rewritten, per this
file's own "record what changed, do not silently overwrite" discipline.** The state described above (83
candidates, nine shade values, Classes 1-6 only) is now SUPERSEDED by:

- **A NEW Class 7 -- liturgical-container predictability, 12 cases** (owner rulings H/I, §§ H/I above),
  bringing the total candidate pool to **95** (83 + 12). Selected by
  `select_liturgical_container_candidates` in `scripts/discovery_gate1_evidence.py`: a manuscript whose
  own catalogue text names a SPECIFIC, NAMED standard-rite container (a container noun immediately
  followed by `מנהג` -- the exact collocation shape of both of the owner's own worked H examples) and
  whose claimed work is NOT already named in that text, grouped by claimed work and round-robined
  (largest group first) so the corpus's dominant real instance (Psalms, by a wide margin -- a standard
  prayer-rite predictably includes specific Psalms as fixed liturgy, so a novelty check reading only the
  container's own name would otherwise misfile the specific Psalm as `fills_gap`) does not crowd out the
  other eleven candidates. Every one of the 83 pre-H candidates is UNCHANGED in content (verified: two
  consecutive script runs against the same asset reproduce byte-identical Markdown for Classes 1-6, Class
  7 appended after them per `_CLASS_ORDER`).
- **The shade vocabulary widens again, per ruling H: TEN shade values** (`confirms` /
  `refines_granularity` / `aid_more_specific` / `diverges_work` / `diverges_part` /
  **`container_predicts`** / `fills_gap` / `extends` / `alias_merge` / `not_checked`) --
  `container_predicts` inserted immediately before `fills_gap`, the shade it would otherwise be misfiled
  into by elimination. Class 7's own "Plausible shades" hint is `container_predicts` / `fills_gap` /
  `confirms` (the same three-token narrowing discipline as every other shade class).
- **Ruling I's re-measurement gate is a NEW acceptance criterion on plan 136-04's Task 3** (reconciled
  into `136-04-PLAN.md` by this continuation), not a change to this worksheet's own construction --
  recorded here only so a reader of this "Outstanding" section knows the evaluation-set-size story is not
  yet finished once Task 3 returns: the 95-case pool must be labelled, AND the pinned gate must be
  re-measured against those labels on the ten-value/free-text-reading contract, BEFORE decision B's
  "run now, pinned" authorization becomes operative for the full production run.
- **The RISK CHECK above is not re-run for `container_predicts`** in this continuation (no new pairwise
  confusability analysis performed) -- `container_predicts` is a single, well-scoped predicate (does a
  named-rite container predict this unit without naming it?) layered onto the existing nine-value
  judgment, structurally similar in kind to E′'s and F's own splits, which the existing RISK CHECK already
  treats as low-confusability relative to the underlying test. Flagged here rather than silently assumed:
  a future session MAY want to extend the RISK CHECK explicitly once Class 7 labels are in hand.

**Updated by ruling J (this continuation) -- retained above unchanged, not rewritten, per this file's own
"record what changed, do not silently overwrite" discipline.** The 95-case, Classes-1-7 state described
above is now SUPERSEDED by a three-arm, SOURCE-STRATIFIED redesign (§ J above), per
`136-NOVELTY-PRIOR-ART.md` § 7's own finding that Classes 4/5/7 had zero representation of the bib/PGP/FGP
failure modes Codex measured as most damaging:

- **101 total candidates** (measured this continuation, real bib/PGP/FGP/FJMS-catalogue sidecars joined
  to the live asset): 8 identity spot-check (Classes 1-3, UNCHANGED) + 30 Class 6 catalogue divergence
  (UNCHANGED) + 30 Arm 1 residual (7 strata, cap 5 each -- 6 strata populated, `pgp_sole` measured zero,
  see § J's note) + 25 Arm 2 heuristic-demoted (`published_full_sole` 10 / `pgp_sole` 10 /
  `other_demotion` 5) + 8 Arm 3 no-source-text (no verdict collected).
- **Classes 4 and 7 are RETIRED as standalone classes** -- folded into Arm 1's `terse_catalogue` and
  `container_predicts` strata respectively. **Class 5 is DROPPED** (no owner ruling exists for any
  specific Class 5 case). Class 6 and Classes 1-3 are UNCHANGED -- see § J's own "kept, folded, dropped"
  accounting for the full rationale.
- **Two NEW owner-facing questions.** Arm 2 introduces `DEMOTION_VOCABULARY`
  (`demotion_correct` / `false_known` / `unsure` / `skip`) -- a straight correctness check on the funnel's
  demotion, distinct from a shade or an identity call. Arm 3 introduces NO question at all -- it is
  explicitly informational, per ruling J's own "ships as candidates with no verdict" design.
- **The XLSX widens from THREE sheets to FIVE**: "Identity Spot-Check" (unchanged), "Novelty Shades"
  (now Class 6 + Arm 1, not Classes 4-7), "Heuristic-Demoted" (Arm 2, NEW), "No-Source-Text" (Arm 3, NEW,
  no verdict column), "Vocabulary & Instructions" (extended with `DEMOTION_VOCABULARY` and the kept/
  folded/dropped accounting).
- **Reproducibility verified**: two consecutive runs of `scripts/discovery_gate1_evidence.py` against the
  same live asset produced byte-identical `136-GATE1-EVIDENCE.md` and `136-NOVELTY-HARDCASES.md`; the
  XLSX is reproducible at the cell-value/validation-list/sheet-structure level (its own pre-existing
  methodology note -- openpyxl embeds a save timestamp, so byte-for-byte is not claimed).
- **A real logic bug was found and fixed during this continuation's own dry run** (not left in): an
  earlier version of the Arm 1 stratum-priority ordering let a terse/absent catalogue field claim
  priority unconditionally, which made `bib_sole` / `pgp_sole` / `fgp_sole` structurally UNREACHABLE (the
  only way a manuscript could be "sole" for one of those sources was for the catalogue to also be
  terse/absent -- exactly the condition the old ordering diverted to `terse_catalogue` first). Fixed by
  only treating the catalogue as a "present" source for the sole/multi-source test when it is
  SUBSTANTIAL (non-terse); `terse_catalogue` now means what the former Class 4 actually tested (the
  catalogue offers nothing AND no other source does either). Recorded here as a durable note against a
  future session re-introducing the same ordering mistake.

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

---

## Task 4 (this continuation, 2026-08-02) -- THE LABEL FILE IS WRITTEN. Task 3 has returned; here is
the measured analysis, per-arm, never as a single headline

**Provenance.** The owner filled the canonical workbook, `136-NOVELTY-HARDCASES.xlsx` (84 of 101
verdict cells), and this ruling was delivered to this continuation through a normal orchestrator
dispatch -- the correct channel (see decision E's own Provenance note for why that distinction is
load-bearing; every prior ruling in this file that arrived by any other route was refused). Before
trusting the dispatch's own summary of the owner's answers, this continuation independently
re-opened the actual `.xlsx` (via `openpyxl`, `data_only=True`, reading every one of the four data
sheets by header name, not position) and cross-checked every cell against what was asserted. **The
verdict DATA matched exactly, cell for cell, across all 101 cases and all four sheets** -- no
discrepancy found in any shade, correctness, demotion or identity value. **One FRAMING discrepancy
was found and is flagged here rather than silently corrected:** the dispatch that requested this
analysis characterized the three skipped identity cases as "Class 3." They are not -- per the
workbook's own "Class" column and per `scripts/discovery_gate1_evidence.py::_CLASS_TITLES`, cases
6-8 are **Class 1 (near-miss titles)**; Class 3 (catalogue entry naming a different granularity of
the same work) is genuinely cases 1-3, which the owner marked `same_work`. The distinction matters
for the D-13d finding below -- see "Class 1 vs. Class 3" section.

### The label file

- **Path:** `discovery_data/novelty_hardcase_labels-v1.json`.
- **Written by:** a new Task-4 read-back mode added to the SAME canonical script this plan already
  uses for every other artifact (`scripts/discovery_gate1_evidence.py`, functions
  `read_owner_labels_from_xlsx` / `write_owner_labels_json`, invoked via
  `python scripts/discovery_gate1_evidence.py --read-labels-from <xlsx> --labels-out <json>`) --
  not a one-off, throwaway script, so the read-back is reproducible and citable the same way the
  workbook's own generation already is. It reuses this module's OWN vocabulary constants
  (`IDENTITY_TOKENS` / `SHADE_TOKENS` / `CORRECTNESS_TOKENS` / `DEMOTION_TOKENS`) as the sole source
  of truth for rejecting an out-of-vocabulary cell value -- never a second, hand-copied list.
- **Content hash:** `sha256:ce0dc2fb176d01de5f04975eac82017feaeb390db71e9ec13f3872a77d28d4b6` --
  computed over `json.dumps(cases, sort_keys=True, ensure_ascii=False)` (the `cases` array only, so
  the hash is stable under any future change to the file's own header/summary fields); recorded in
  the JSON's own `content_hash` field and here, per T-136-03-06, so plan 136-04 can re-verify the
  file it grades against has not been hand-edited since the owner supplied it.
- **Structural guarantees enforced by the read-back (fail CLOSED, not merely reported):** every one
  of the 101 case numbers is present exactly once (1..101, no gaps, no duplicates); every verdict/
  correctness/demotion cell value is one of that sheet's own vocabulary tokens (rejects free text);
  every entry carries `label_provenance` recording owner supply, the source sheet, the date, and
  (where a draft PROPOSAL was shown) whether the owner's verdict confirmed or corrected it; a truly
  blank verdict cell is recorded as `skipped: true` with `value: null` -- **never** filled from the
  row's own PROPOSAL draft (verified: zero cases carry both `skipped: true` and a non-null verdict
  value).
- **Counts:** 101 total cases -- **81 labelled**, **12 skipped** (3 identity + 9 heuristic-demoted
  blanks), **8 no-verdict-by-design** (Arm 3), **0 correctness gaps** (every `diverges_work`/
  `diverges_part` row in both Class 6 and Arm 1 carries a Correctness call -- the owner left none of
  the 32 divergence rows unanswered on that axis).

### A. Identity spot-check (8 cases) -- the constant-answer hypothesis is CONFIRMED on the decided
cases, not disproven

Cases 1-5 (Class 3 granularity ×3, Class 2 alias ×2) all came back `same_work`. Cases 6-8 (Class 1
near-miss) were all **skipped**, with the owner's own note (delivered alongside the workbook, per
the dispatch): *"same work, different location (perhaps not the same page in the ms?)."*
Per the interpretation rule this file itself pre-registered ("Outstanding" section, "How to read the
result"): all-`same_work` on the DECIDED cases argues the D-13d author-gated collapse rule (276 of
1,367 groups, 20.2%) is, if anything, too conservative -- **5 of 5 decided cases confirm this; the
constant-answer hypothesis is CONFIRMED on the population that was actually judged.** The 3 skips
are not evidence against it (a skip is not a `different_works` verdict), but they are also not free
of information -- see the next section, which investigates exactly what the owner's skip note
implies.

### Class 1 vs. Class 3 -- the span/page identity premise, investigated for cases 6-8

**Finding, stated precisely.** Class 3 (granularity, cases 1-3) and Class 1 (near-miss, cases 6-8)
are built by genuinely DIFFERENT selection mechanisms in `scripts/discovery_gate1_evidence.py`, and
only one of them rests on the "byte-identical span claimed by both works" premise:

- **Class 3 (`build_hardcases`'s granularity block) genuinely IS built from real identical-span
  co-occurrence.** Verified directly against the live asset for all three sampled cases: Case 1
  (sys_id `990051079570205171`) has `w000171` and `w001281` BOTH on page
  `..._P000002_FL158601518`, span **0-962**, exactly as rendered. Case 3 (sys_id
  `990001588480205171`) has `w000171` and `w001304` BOTH on page `..._P000007_FL50086673`, span
  **0-246**. Case 2 (sys_id `990000872080205171`) has `w000171` and `w001278` both present with an
  identical span **0-780** on page `..._P000033_FL49277200` (among several pages where the two
  works' claims exist but do not share an identical span -- the rendered "byte-identical span
  0-780" claim in the worksheet is correct for the page it names). **Class 3's own construction is
  sound; this finding does NOT undermine it.**
- **Class 1 (`select_near_miss_candidates` + `build_hardcases`'s near-miss block) is NOT built on
  any co-occurrence premise at all, and its own rendered "why it is hard" text never claims one** --
  it reads "Same author; normalized titles are N% similar (SequenceMatcher) but NOT identical," with
  no mention of a shared span. The manuscript shown for a near-miss pair is picked by
  `best_claim_for_work(claims, wa) or best_claim_for_work(claims, wb)` -- the best claim for
  work A if it has ANY shipped claim, full stop, regardless of whether work B is claimed ANYWHERE in
  that same manuscript. **Directly queried against the live asset for all three sampled cases: the
  "other" work has ZERO claims on the shown manuscript in every one of the three.** Case 6 (sys_id
  `990051317430205171`): only `w000007` has claims there; `w000036` never appears. Case 7 (sys_id
  `990001835240205171`): only `w001167` has claims there; `w001181` never appears. Case 8 (sys_id
  `990051181430205171`): only `w000452` has claims there; `w000467` never appears. **The premise the
  dispatching brief attributed to these three cases -- a byte-identical span claimed by both works
  -- was never true of them; it does not need to be disproven, because it was never asserted by the
  worksheet in the first place.** This is a genuine, if narrow, characterization gap in how the
  request for this analysis was framed (conflating Class 1 with Class 3), not a defect in the
  worksheet or its Markdown/XLSX rendering, both of which describe Class 1's actual construction
  accurately.

**What the owner's skip note is actually evidence of, and the real (dormant) D-13d gap it points
to.** The owner's note -- "same work, different location, perhaps not the same page" -- reads,
against Case 6's actual pair (`המספיק לעובדי השם (כרך ב חלק ב)` / `...(כרך ט חלק ב)`, i.e. the SAME
multi-volume treatise by אברהם בן הרמב"ם, volumes 2 and 9), as an accurate, savvy intuition: these
ARE plausibly "the same work" in the loose sense of belonging to one authored treatise, but they are
DIFFERENT VOLUMES, not interchangeable, and (confirmed above) never co-located in this manuscript --
exactly the kind of case a labeller is right to decline rather than force into `same_work` or
`different_works`. This prompted a targeted, narrow check of whether D-13d's own collapse predicate
(`works_related_by_title`: same non-null author AND identical-or->=4-char-shared-prefix normalized
title) would ALSO fire on this specific pair if it ever did co-occur -- **and it does**:
`works_related_by_title(works["w000007"], works["w000036"])` returns `True` (both share author
`אברהם בן הרמב"ם` and the 4-character prefix `"כרך "`). There is a third catalogued volume in this
same title family, `w000038` ("...(כרך ט חלק א)"), so any 2-of-3 pairing would collapse under the
current predicate. **Empirically, this is currently a DORMANT gap, not an active defect in the
shipped 276-group collapse population:** a direct query of every `load_identical_span_groups` group
in the live asset found **zero** span-groups anywhere in the whole corpus containing two or more of
`{w000007, w000036, w000038}` together -- these three volumes never actually compete for the same
byte span in this asset. A full audit of the 25 unique same-author/related-title pairs that DO drive
the current 276-group collapse population confirms every one is a genuine whole-work/part-of-work
hierarchical relationship (e.g. `רש"י על התורה` / `רש"י על בראשית` -- the whole Torah commentary vs.
one book of it; `משנה תורה, הקדמה ומניין המצוות` / `משנה תורה, ספר X` -- the Code's own
introduction vs. one of its constituent Books) -- **none is a "different volume of a multi-volume
opus" case.** **D-13d is therefore FLAGGED for re-examination, not found broken:** the author-gated
4-character-prefix predicate does not, in principle, distinguish "the same intellectual work at two
levels of containment" (correct to collapse) from "two distinct, non-interchangeable volumes/parts
of one multi-volume opus" (arguably should NOT silently collapse into a single displayed
identification, since that would obscure WHICH volume a fragment actually witnesses) -- this
distinction is not currently exercised by the measured population, but the predicate's own logic
does not prevent it from firing this way in a future corpus growth or a differently-catalogued
multi-volume work. **This plan does NOT change the collapse rule** (per its own instruction) --
this is a flag for whoever next touches `works_related_by_title` in `shared/discovery_grouping.py`,
not an authorized code change.

### B. Novelty-evaluation analysis -- per arm, never as one headline (the arms are capped, not
proportional; the residual is a biased slice by construction; Class 6's distribution is a selection
artifact)

**Arm 1 -- RESIDUAL (30 cases, the EXACT population the pinned LLM gate would score under ruling J's
funnel-first architecture): ZERO `fills_gap` in 30.** Shade distribution: `diverges_work` 19 (63%) /
`container_predicts` 4 (13%) / `confirms` 4 (13%) / `refines_granularity` 2 (7%) / `aid_more_specific`
1 (3%) / `fills_gap` **0** (0%). Of the 19 `diverges_work` rows, correctness is `catalogue_correct`
18, `unclear` 1, `claim_correct` 0. **~60% of residual claims are the claim being WRONG** (19
`diverges_work`, of which 18 carry a recorded `catalogue_correct` call) -- this is not a base rate
(Arm 1's per-stratum cap of 5 is fixed, not proportional, and `pgp_sole` populated zero candidates --
see ruling J's own sizing note), but it is a direct, measured answer to the question this arm was
built to answer: *of the rows that would actually reach the model, is the model's job mostly "confirm
a real gap" or mostly "adjudicate a claim that is probably already wrong"?* **The answer, measured:
mostly the latter.** The novelty gate answers "is this recorded", never "is this right" -- and on
this population, "is this right" is where most of the actual disagreement lives, a question the gate
is not designed to answer at all.

**Ruling H is validated with numbers, not merely adopted on faith.** 4 of the 30 residual cases
(13%) are `container_predicts` -- under the pre-H, nine-value enum these would have fallen through to
`fills_gap` by elimination and shipped as "Candidates for new finds," a wrong "previously unknown"
claim for a standard-rite liturgical unit a catalogue already predicts without naming. Ruling H's
adoption is not merely theoretically motivated; it demonstrably prevents 4 of 30 (one in
roughly every 7-8) residual rows from becoming a false candidate-new-find in this sample.

**Class 6 -- catalogue divergence (30 cases, unchanged, owner rulings E/E'/F/G): the selector
over-fires ~57%.** Shade distribution: `confirms` 15 (50%) / `diverges_work` 13 (43%) /
`refines_granularity` 2 (7%). Of the 13 `diverges_work` rows, correctness is `catalogue_correct` 13
(100%), `unclear` 0, `claim_correct` 0. **The Class-6 divergence selector
(`select_catalogue_divergence_candidates`) over-fires on 15 + 2 = 17 of 30 (57%)** -- cases it
proposed as a plausible divergence that the owner instead judged `confirms` (the catalogue's free
text already states the claim, per ruling G) or `refines_granularity` (a genuine granularity
addition, not a contradiction). This is not a new finding -- ruling G already characterized this
selector's flaw and explicitly declined to fix it so the failure mode would keep surfacing as the
pool grew; the 30-case pool confirms the ~50% figure ruling G estimated from 2 of the original 15
worked cases holds at the larger sample (57% vs. the original ~50% estimate, well within the
uncertainty of a 2-case anecdote). **Where the selector's `diverges_work` call WAS confirmed by the
owner (13 of 30, 43%), the correctness call is unanimous: the catalogue is right, the claim is wrong,
every single time (13/13).** Combined with Arm 1's 18/19 finding above, this is a strong, convergent
signal across BOTH the Class-6 (catalogue-text-only selection) and Arm-1 (source-stratified
selection) populations: **when this project's own heuristics flag a genuine divergence, the
catalogue is very likely to be right.** Per the standing catalogue-never-evidence discipline, this
finding is recorded as a measured property of THESE TWO SAMPLES, not adopted as a rule the system
applies to auto-adjudicate any future case -- ruling F's own resolution (opt-in, warned, user-decided
visibility) already embodies exactly this caution, and this data supports why that caution was
warranted rather than excessive. **Reminder per this plan's own critical invariant:** the
Correctness column's use of the catalogue to judge OUR claims is a deliberate, owner-ruled exception
(ruling F) scoped to labelling THIS evaluation set -- it must not be read as license to let any
production code path treat catalogue disagreement as a verdict.

**No arm here measures, or should be read as measuring, a corpus-wide rate.** Arm 1's per-stratum cap
(5) is fixed, not proportional -- `pgp_sole` populated 0 of a possible 5, `container_predicts` /
`terse_catalogue` / `bib_sole` / `fgp_sole` / `catalogue_sole` / `multi_source` populated 5 each.
Class 6's 30 cases are selected purely by a catalogue-text-containment heuristic already shown above
to over-fire -- its distribution describes what THAT SELECTOR proposes, filtered through the owner's
correction, not what fraction of all claims diverge from their catalogue entry.

### C. Arm 2 -- HEURISTIC-DEMOTED (25 cases): recorded as INCONCLUSIVE -- an instrument failure, not
an owner failure

**Only 7 of 25 rows received a decisive verdict** (`demotion_correct` or `false_known`); 9 came back
`unsure`, and 9 were left genuinely blank (recorded as `skipped`, never filled from the draft
PROPOSAL). By stratum: **`published_full_sole`** (the Codex-finding-1 population, bib-presence-alone
false-knowns) -- 1 `false_known`, 9 blank: **1 of 10 decisive.** **`pgp_sole`** (the Codex-finding-6
population, PGP-presence-alone false-knowns) -- 1 `demotion_correct`, 9 `unsure`: **1 of 10
decisive.** **`other_demotion`** (the non-Codex-flagged stratum) -- 4 `demotion_correct`, 1
`false_known`: **5 of 5 decisive.** **The two strata Codex flagged as the most damaging populations
in the reference implementation (3,688 `published_full` false-knowns; 2,014 PGP false-knowns, 942
PGP-sole) are exactly the two that produced only ONE decisive verdict each out of ten** -- the
oversampling this arm's design intended (per ruling J) to characterize the false-known risk directly
did not, in practice, produce gradeable data for those two populations.

**Diagnosis, verified against the actual workbook text, not assumed.** Judging "was this demotion
correct" requires the ACTUAL bibliography row (its own title/author text) or the actual PGP
description/transcription snippet in front of the labeller. The sheet does not supply either --
its "Why this demotion is being checked" column carries only the STRATUM-level mechanical rationale
(verbatim, Case 69: *"the CURRENT heuristic treats ANY bibliography row with TranscriptionType='Full'
as naming this claim, regardless of whether that row's own title/author actually matches (Codex
finding 1) -- no OTHER checked source agrees"*), never the specific bib/PGP text that would let a
labeller actually check whether that specific row names this specific work. This is precisely why
`published_full_sole` and `pgp_sole` -- where the whole judgment turns on reading unshown source text
-- produced almost no decisive verdicts, while `other_demotion` (whose rationale text apparently gave
enough to judge from) produced 5 of 5. **This is a genuine instrument-design gap in this labelling
pass, not a labelling effort failure** -- the owner engaged with all 25 rows (only 9 are truly blank)
and gave the honest answer the sheet's own information supported (`unsure` for 9 of the 10
unresolvable `pgp_sole` rows).

**The false-known question ruling J's cost analysis depends on remains OPEN and ungraded.** Ruling
J's own text states the funnel-first architecture makes a heuristic false-known PERMANENT and
UNRECOVERABLE, and treats this as an accepted, measured cost precisely BECAUSE the error runs in the
conservative direction. This labelling pass was meant to measure how often that cost is actually paid
on the two populations Codex flagged hardest; it did not produce that measurement. The best available
evidence remains the owner's own prose read of the underlying rationale, offered alongside the
workbook and recorded here EXPLICITLY as ungraded prose, not a measurement:

> "some of them are not the same but most of them are anyway wrong so no harm. Some granularity
> misses. No big deal most of them."

This is recorded as the best available signal, not upgraded to a number: it is consistent with (and
no stronger than) the 1-of-10 decisive rate actually measured on the two hardest strata, and should
not be cited alongside the Arm 1 / Class 6 percentages above as if it carried the same evidentiary
weight.

**Recommendation, not built now:** a redesigned Arm 2 that surfaces the actual demoting source's own
text (the bib row's title/author, or the PGP description/transcription snippet) alongside each case,
so the labeller can judge the demotion the way `other_demotion`'s apparently-sufficient rationale text
let them. This is a recommendation for a future pass, per this plan's own "do not build it now"
instruction -- no new selector, sheet, or script change is made here.

### D. Arm 3 -- NO-SOURCE-TEXT (8 cases): no verdict collected, by design

Unchanged from ruling J's own specification -- these 8 rows ship as candidates automatically
regardless of any observation here; no owner verdict was solicited and none is recorded beyond the
structural fact that all 8 carry `question_type: "no_verdict_by_design"` in the label file.

### Summary table (measured this continuation; every count traceable to the label file's own
`cases` array)

| Population | n | Decisive/labelled | Headline (per-arm, NOT a corpus rate) |
|---|---|---|---|
| Identity spot-check | 8 | 5 same_work / 3 skip | Constant-answer hypothesis CONFIRMED on the 5 decided |
| Class 6 (catalogue divergence) | 30 | 30 | Selector over-fires ~57% (15 confirms + 2 refines) |
| Arm 1 (residual) | 30 | 30 | 0 fills_gap; 19 diverges_work (18 catalogue_correct); container_predicts saved 4 rows from false candidacy |
| Arm 2 (heuristic-demoted) | 25 | 7 decisive / 9 unsure / 9 blank | INCONCLUSIVE -- instrument failure (needs source text, not stratum rationale) |
| Arm 3 (no-source-text) | 8 | n/a | No verdict by design |

**Total: 101 cases, 81 labelled, 12 skipped, 8 no-verdict-by-design, 0 correctness gaps.**

---

## K. The ~$301 production run stays UNAUTHORIZED -- test the `fills_gap` axis first

**Provenance.** This ruling was delivered through a normal orchestrator dispatch to a later,
directly-dispatched continuation (running on the primary checkout, `C:\Genizahsearch`, per that
session's own preconditions check) after the owner read `136-NOVELTY-RUN.md`'s real, measured
re-measurement results (section 2-3 of that record). This is the correct channel (see decision E's
own Provenance note for why that distinction is load-bearing); this ruling is recorded here with the
same standing as A-J -- LOCKED, not re-litigated, re-derived or "improved" here.

- **Question.** The ruling-I re-measurement (`136-NOVELTY-RUN.md` § 2) scored the pinned config at
  47/60 (78.3%) shade agreement against real owner labels, with **ZERO errors in the reputationally
  expensive false-novel direction** (predicting `fills_gap` when the owner says it is not). Does that
  clean result on the false-novel axis license the ~$301 production run (the cost `136-NOVELTY-RUN.md`
  § 3.2 re-derived, an 11x jump from the `~$27` figure decision B authorized), or does the measurement
  itself have a gap that must be closed first?

- **Owner's answer (verbatim, condensed for this record):** "Do NOT authorize the ~$301 production
  run yet. Test the `fills_gap` axis first." The owner identified that the clean 0-error result is
  **not evidence of safety on the axis that matters**: the 60-case pool (30 Class 6 + 30 Arm 1
  residual) contains **ZERO true `fills_gap` cases anywhere in it** (`136-NOVELTY-RUN.md` § 2.5's own
  caveat, and `136-GATE1-DECISIONS.md`'s own Task-4 "Arm 1 -- 0 fills_gap in 30" finding above) -- so
  the question decision B actually authorizes spend on (does the model correctly say "genuinely
  unknown" ONLY when it is actually unknown, and never miss a genuine one) was simply never
  exercised by this sample. A 0/0 result is silence, not a passing grade. Separately, the same
  re-measurement re-derived the real production cost at **~$301** over the real 55,184-candidate
  residual (`136-NOVELTY-RUN.md` § 3.2) -- an **11x** jump from the `~$27` figure decision B
  authorized, itself a new authorization decision the owner had not yet ruled on when this figure was
  re-derived.

- **Date:** 2026-08-03.

- **What is NOT reopened.** Decision B's model/version/effort pin (`gemini-3.6-flash`,
  `reasoning:{effort:"low"}`) is UNCHANGED. Ruling I's re-measurement (60 shade cases, real sidecars,
  real model, real cost) stands as a valid, real measurement of the SHADE axis on the current
  ten-value/free-text contract -- it is not discarded or redone; it is simply insufficient, on its
  own, to license the specific false-novel-rate question ruling K asks. The `~$27` figure remains
  permanently superseded (per ruling J's own instruction and `136-NOVELTY-RUN.md` § 3.2's
  re-derivation) -- **the ~$301 figure is now the operative cost basis, and must never be cited as
  `~$27` again in any future record.**

- **What ruling K requires before the production run may be authorized.** A purpose-built probe that
  measures the false-novel rate on the population that would ACTUALLY ship as "Candidates for new
  finds" -- covering BOTH paths a row can take to become a candidate:
  1. **The model path** -- residual rows the pinned model classifies `fills_gap`.
  2. **The bypass path** -- rows where NO checked source has any text at all (ruling J's Arm 3 design:
     these never reach the model at all; the funnel ships them as `fills_gap` automatically). Arm 3
     sampled 8 of these with NO verdict collected by design (ruling J's own specification) -- ruling K
     explicitly calls this path "arguably the HIGHER risk precisely because nothing examines it,"
     since nothing -- not the heuristic funnel, not the model -- ever checks these rows against
     anything before they ship.
  The probe's own design, sizing, and the owner-labelling instrument it produces are recorded in
  `136-NOVELTY-RUN.md`'s new probe section (§ 4) -- this ruling authorizes and requires the probe;
  it does not itself perform it (this record documents the ruling, per this file's own standing
  discipline of recording ownership decisions separately from their execution).

- **Code consequence.** No enum value, column, or schema change -- this ruling is a SPENDING GATE, not
  a contract change. The consequence is entirely procedural: **the ~$301 production run may not be
  executed on decision B's or ruling I's authorization alone.** A NEW acceptance criterion is added on
  top of ruling I's existing re-measurement gate: before the production run, a probe measuring the
  false-novel rate on the ACTUAL candidate population (both paths) must be built, run, and its results
  (once the owner labels the resulting instrument) brought back to the owner for a final go/no-go call.

- **Downstream contracts this decision amends:**
  1. **Decision B / ruling I's gate** (`136-GATE1-DECISIONS.md` §§ B, I) -- gains a further condition:
     re-measurement on the current vocabulary/contract (ruling I, already satisfied per
     `136-NOVELTY-RUN.md` § 2) is necessary but NOT sufficient; the `fills_gap`-axis probe (ruling K) is
     an ADDITIONAL, separate gate on the SAME production-run authorization.
  2. **`136-NOVELTY-RUN.md`** -- gains a new section documenting the probe's design, its real measured
     cost, and what it will and will not be able to answer once labelled (see that file's own new
     section for the full account).
  3. **`.planning/REQUIREMENTS.md` NOVEL-01** -- gains a further dated `⟨AMENDED 2026-08-03 -- ruling
     K⟩` sub-bullet recording that the ~$301 run stays unauthorized pending the probe.

- **Plans that must implement this ruling:** **136-04** (Task 3's own acceptance criteria gain this as
  a further condition -- the production run instructed there may not proceed on ruling I's
  re-measurement alone); any future plan that would actually execute the ~$301 production run must
  cite this ruling's resolution (a labelled probe result and a subsequent owner go/no-go) as its own
  precondition.

- **What this continuation does NOT do:** it does not run the ~$301 production funnel (explicitly
  unauthorized, per the ruling above) and does not itself render a go/no-go verdict on the probe's
  eventual results (that is the owner's call, once the probe is labelled). This continuation DOES
  build and run the probe itself (real corpus sample, real pinned model calls on the model-path
  sample, real measured cost) and emit the owner-labelling instrument -- see `136-NOVELTY-RUN.md` § 4.

---

## L. `divergence_correctness` is DROPPED from the model's job -- human/owner annotation only

**Provenance.** Delivered through the SAME normal orchestrator dispatch as ruling K above (the
correct channel; see decision E's own Provenance note), to the same directly-dispatched continuation,
after the owner read `136-NOVELTY-RUN.md` § 2.5's real, measured divergence-correctness result.
Recorded here with the same standing as A-K -- LOCKED, not re-litigated, re-derived or "improved" here.

- **Question.** Ruling F added `divergence_correctness` (`catalogue_correct` / `claim_correct` /
  `unclear`) as a SEPARATE axis from the shade, asked of the model on every `diverges_work`/
  `diverges_part` row, because the owner's own review of the real Class-6 cases found BOTH directions
  occur under the identical shade token. The ruling-I re-measurement scored the model's own
  correctness calls at **8/28 = 28.6%** against the owner's real labels -- at or below the ~33% a
  three-way random guess would produce, and the single weakest measured result in that re-measurement
  (`136-NOVELTY-RUN.md` § 2.5). Does this axis remain part of the model's job, given that measured
  result?

- **Owner's answer (verbatim, condensed for this record):** "`divergence_correctness` is DROPPED from
  the model's job." The owner's stated rationale: this axis "asks for scholarly judgment," which the
  owner supplied directly at **31/32** on the identical cases (the owner's own labelling pass over
  Class 6 + Arm 1's divergence rows -- see the Task-4 section above, "13/13" and "18/19"
  `catalogue_correct` findings, a combined 31 of 32 total divergence-row correctness calls the owner
  made across both populations). A model scoring at or below chance on a question the owner answers
  correctly essentially every time is not a defensible thing to ship. Separately, and independently
  dispositive: **ruling F's own default-hidden, explicit-warned-toggle posture for
  `diverges_work`/`diverges_part` rows applies REGARDLESS of which side is right** -- a divergence row
  is hidden by default and shown only behind an explicit warned toggle whether the catalogue turns out
  to be correct or the claim turns out to be correct. **No shipped surface therefore needs this answer
  from the model at all** -- the shade token alone already drives every display decision ruling F
  specifies.

- **Date:** 2026-08-03.

- **What is NOT reopened.** Ruling F's shade split (`diverges_work` / `diverges_part`, replacing the
  single `diverges` token) is UNCHANGED -- this ruling touches ONLY the correctness sub-question, not
  the scope-split shade itself. The default-hidden/explicit-warned-toggle display rule for these two
  shades (ruling F) is UNCHANGED and, per the rationale above, is exactly WHY dropping the correctness
  axis from the model costs nothing on any shipped surface.

- **What is kept, explicitly -- nothing is deleted.** The STORED `divergence_correctness` column, its
  CHECK constraint (`docs/specs/discovery-sidecar-schema-v1.md`), and every owner-supplied value
  already collected (the label file's 32 divergence-row correctness calls, `136-GATE1-DECISIONS.md`'s
  own Task-4 findings quoting them) are ALL UNCHANGED. `divergence_correctness` remains a real,
  meaningful, closed-vocabulary column -- it simply may never again be populated by the model. A
  future human/owner annotation pass (not specified or authorized by this ruling; out of scope here)
  remains the sole path that may populate it going forward, exactly as the owner's own Task-4
  labelling already did for the 32 rows on record.

- **Code consequence.** `shared/discovery_novelty.py`'s pinned prompt
  (`NOVELTY_PROMPT_TEMPLATE`) no longer asks the model for `divergence_correctness` at all -- the
  model's sole output is the ten-value `novelty_status` shade (UNCHANGED in vocabulary; only the
  correctness sub-question is removed from what the model is asked). `resolve_model_output` now
  ALWAYS returns `divergence_correctness: None`, structurally incapable of surfacing a model-supplied
  value for that field (mirroring `masked_provenance_label`'s own "cannot leak because it never echoes
  its input" discipline) -- this holds even against a malformed or legacy raw response that happens to
  still carry the key. **The PROMPT HASH changes again on this account.** The OLD hash recorded for
  the ruling-I re-measurement,
  `PROMPT_SHA256 = 441058ae3bab6e5ee17beb0fc5ea39426d7c250feb6c2bd288f0bc1605c98be5`, is RETIRED and
  must never be cited as current going forward. **The NEW hash, computed from the literal
  post-ruling-L `NOVELTY_PROMPT_TEMPLATE` string in `shared/discovery_novelty.py` at import time:**

  ```
  PROMPT_SHA256 = 4b2874794e82236655e1ca08d8866969350c6302965197dea8f18e06844e5e60
  ```

  `INPUT_NORMALIZATION_SHA256` is UNCHANGED by this ruling (ruling G's free-text normalization contract
  is untouched) -- only `PROMPT_SHA256` moves.

- **Downstream contracts this decision amends:**
  1. **`docs/specs/discovery-novelty-v1.md`** -- section 2 (`divergence_correctness`'s own row) and
     section 5 (the pinned LLM contract) both gain a dated `⟨AMENDED 2026-08-03, owner ruling L⟩`
     sub-note stating the column is now human/owner-only and citing the new prompt hash above.
  2. **`.planning/REQUIREMENTS.md` NOVEL-01** -- gains a further dated `⟨AMENDED 2026-08-03 -- ruling
     L⟩` sub-bullet.
  3. **`.planning/ROADMAP.md`** success criterion 6 -- gains a brief dated note that
     `divergence_correctness` is human-only going forward; the column's existence and CHECK-constraint
     shape are otherwise unaffected.
  4. **`136-12-PLAN.md`'s Task 1** (not yet executed as of this ruling) -- its action text describes
     ingesting `divergence_correctness` from "the verdicts" (the model's own verdict cache); as of this
     ruling, the verdict cache's own `resolve_model_output` output ALWAYS carries `divergence_correctness:
     None`, so Task 1, when actually executed, must NOT expect to ingest a model-supplied value for this
     column -- it stays NULL on every row unless and until a separate human/owner annotation artifact
     (not yet built) supplies it. A dated note has been added to that plan file flagging this for
     whoever executes it next.
  5. **`tests/test_discovery_novelty_contract.py`** -- updated: the test that previously asserted a
     model-supplied `divergence_correctness` value passes through `resolve_model_output` unchanged is
     replaced with one asserting it is ALWAYS dropped (`test_resolve_model_output_divergence_shade_never_carries_correctness_from_model`);
     a new test asserts the pinned prompt template no longer mentions `divergence_correctness` or
     "correctness" at all (`test_prompt_no_longer_elicits_divergence_correctness`).

- **Plans that must implement this ruling:** **136-04** (already executed; this ruling amends its
  shipped artifact directly, per the same continuation that recorded this ruling -- not a re-run of
  Tasks 1-3, a surgical amendment to code those tasks already produced), **136-12** (not yet executed;
  gains the dated note above so its Task 1 does not wire a model-sourced `divergence_correctness`
  value that no longer exists).

- **What this continuation does NOT do:** it does not delete the stored column, the CHECK constraint,
  or any owner-supplied `divergence_correctness` value already in `discovery_data/novelty_hardcase_labels-v1.json`
  -- all of that is explicitly preserved per the owner's own "nothing is deleted" instruction. It does
  not build a human/owner annotation pathway for this column going forward (out of scope; flagged for
  a future plan).

---

## Ruling M — the `fills_gap` novelty gate is ACCEPTED (owner, 2026-08-03)

- **Owner's verdict, verbatim:** *"Almost all are novel. No. 4 is not novel as attested in
  bibliography, but it has specific identification that is not apparent even in bibliography. No. 8 is
  just untrue. Also No. 12, 16 (it's a prayer but not sefer Ahava, common pitfall). Other prayers as
  well (no. 19, 25), are not well identified. All in all I am satisfied from this gate"*

- **What was graded:** the 33-row `fills_gap` probe (`136-NOVELTY-FILLSGAP-PROBE.xlsx`), 13 model-path
  + 20 bypass-path rows, after the instrument was corrected to show each checked source's OWN free
  text (commit `80d54ca5`). The pre-correction instrument showed only our claim and a selection
  rationale and was NOT gradeable -- the same defect that made arm 2 inconclusive.

- **The decisive distinction the owner drew.** Only **case 4** is a NOVELTY error (the bibliography
  does attest it). Cases **8, 12, 16, 19, 25** are *identification* errors: the novelty gate correctly
  reported "not identified in the aids we checked" -- the aids genuinely do not identify these -- but
  the work our pipeline claims is wrong. **These are a different defect, with a different owner, and
  they are NOT evidence against the novelty gate.** Do not conflate the two rates when quoting this
  probe.

- **Ruled: the novelty gate's `fills_gap` judgments are sound enough to ship.** The measured
  false-novel rate on this probe is 1/33 on the novelty question proper.

## Ruling N — an imprecise claim still SHIPS; the baseline is absence, not correctness

- **Owner's ruling, verbatim:** *"It's a wrong claim but it's BETTER than the current absent one since
  it points the reader to the fact it's a prayer"*

- **Context:** cases 12/16/19 claim `משנה תורה, ספר אהבה` for fragments that are liturgy. Sefer Ahava
  contains the order of prayers, so prayer text is drawn to it -- the owner's "common pitfall". The
  bypass-path rows in particular have NO identification in ANY checked aid, so the comparison that
  matters is not *our claim vs. the correct work* but ***our claim vs. nothing at all***.

- **Ruled: these rows ship.** An imprecise but directionally informative claim ("this is liturgy") beats
  silence. This is NOT a defect to suppress, and no plan should add a filter to hide it. Consistent
  with the standing `aid_more_specific` ruling (show, never as a candidate find) and with
  "catalogue is a recall yardstick, never acceptance evidence".

- **Scale (measured 2026-08-03 against the live v2 asset):** `משנה תורה, ספר אהבה` (`w000176`) is the
  **10th most-claimed work in the corpus at 6,437 claim pairs** of 268,361 -- 2.4%, and the largest
  non-Bible claim after Yalkut. So this pattern is a visible fraction of the surface, shipping by
  design under this ruling.

- **Forward link:** this is exactly the population the DEFERRED reference-granularity stage converts
  into precision (a prayer identified as its own composition rather than as its halakhic container).
  Counting it is a forward indicator of what granularity unlocks.

## Recorded — NEW evidence for the deferred witness-vs-quoter lever (compilations absorb their sources)

- **Owner's observation, verbatim:** *"Yalkut may also be many times wrong since it's a compilation"*

- **Why this is new.** The witness-vs-quoter lever is already deferred to discovery-v2.1
  (`136-CONTEXT.md` lines 232/292/800), but the only evidence recorded for it to date runs the OTHER
  way: `low_coverage` routing DEMOTING correct identifications (`docs/OPEN_ISSUES.md`, 100,159 rows).
  The **quoter direction** -- an anthology claimed where the fragment is really the source work it
  quotes -- had no recorded evidence. It does now.

- **Measured exposure (2026-08-03, live v2 asset):**

  | claimed work | claim pairs |
  |---|---|
  | `ילקוט שמעוני על התורה` | 3,040 |
  | `ילקוט שמעוני על נ"ך` | 1,890 |
  | **Yalkut total** | **4,930** |
  | `תנחומא` (a work Yalkut quotes) | 1,480 |

  Yalkut is claimed **3.3x more often than Tanhuma**. Yalkut Shimoni is a 13th-century anthology; the
  Genizah corpus is overwhelmingly earlier. An anthology outranking its own sources by 3:1 is prima
  facie backwards and is consistent with the compilation absorbing fragments of the works it quotes.

- **Not detectable today:** `works.genre` is **NULL for all 1,269 works** -- there is no compilation/
  anthology flag in the asset, so this cannot currently be filtered, measured precisely, or routed.
  Any v2.1 work on this lever needs a work-level compilation classification first.

- **NOT actioned in Phase 136** (out of scope; the gate under test was novelty, and ruling M accepts
  it). Carried to the discovery-v2.1 refresh alongside the reference-granularity stage.

---

## Ruling O — BATCH the pinned gate (owner, 2026-08-03)

- **Owner's authorization, verbatim:** *"Of course we'll want batching"*, then *"We can go to batch 10,
  perhaps we need guardrails against connection losses and price balooning"*.

- **What was actually wrong.** The cost objection was NOT the model's fault and NOT a reason to
  downgrade it. The pinned system prompt is **4,070 chars — 88% of every call's input** — and the probe
  re-sent it for ONE judgment at a time. The originally validated gate
  (`reference_discovery_llm_gate_cost`) ran **batch 40**; our implementation had silently regressed to
  batch 1. That regression *is* the cost problem.

- **Two cost corrections, both mine, both now superseded:**
  1. The **~$301** figure quoted to the owner was wrong. It came from the ruling-I hard-case pool
     ($0.005369/call), which has longer evidence than typical. The probe's own 305 calls ARE residual
     draws, mean $0.003375/call => the honest batch-1 projection was **$186.25** (p90 $276.97).
  2. Batched, the MEASURED projection is **~$34** (batch 10). The `~$27` in the memory was right all
     along; it was measured on a batch-40 gate.

- **Measured batch-size sweep** (real `usage.cost`, same 300 residual cases the single-case arm had
  already classified, so every row has a reference verdict; the accepted batch-1 gate is the reference):

  | batch | real cost | projected over 55,184 | exact 10-shade | 3-way behavioural | **new `fills_gap` (false-novel injected)** | lost `fills_gap` |
  |---|---|---|---|---|---|---|
  | 40 | $0.1517 | $27.91 | 80.7% | 88.3% | **+11** | −2 |
  | 20 | $0.1600 | $29.44 | 76.3% | 87.3% | **+10** | −5 |
  | **10 (CHOSEN)** | $0.1853 | **$34.09** | 72.0% | **89.0%** | **+2** | −4 |
  | 5 | $0.2548 | $46.87 | 79.7% | 92.3% | +2 | −3 |

  **The knee is sharp between 20 and 10** and it is the safety-critical column that moves: false
  `fills_gap` promotions collapse from +10 to +2 and then stop improving. Batch 5 buys 3 points of
  behavioural agreement for another $13 and NO safety gain. Hence 10.

  **Honest reading of the exact-shade column:** it bounces 72–81% with no clean trend, so most of it is
  the model's own run-to-run instability, not a batching effect — consistent with the batch-1 arm
  itself scoring only 78.3% against the owner's own labels in the ruling-I re-measurement. The 3-way
  behavioural decision and the false-novel count are the stable, meaningful signals; do NOT quote the
  exact-shade column as a batching regression.

- **Cheaper models are NOT needed and were NOT adopted.** `gemma4:latest` is installed locally, returns
  valid JSON for this prompt, and is free — but measured **~54s/call**, i.e. weeks of wall-clock for the
  residual. `gemini-flash-lite` carries a measured 30% agreement prior on this task family. Both are
  moot: batching reaches the target cost on the model the owner already validated. **Do not revisit
  model downgrades to cut cost** — the lever is the prompt/judgment ratio.

- **A short-circuit on vacuous titles was investigated and REJECTED as a cost lever** (owner's own
  suggestion). Measured on the real residual: rows whose every checked source is vacuous
  (`''`, `קטעי גניזה`, `ספק גניזה … אין בידנו`, FGP `לא הוגדר`/`not define`, `Placeholder [ASE]`) are only
  **1,056 (1.9%)**, and adding genre-generic rows (`תורה (קטעים)`, `פיוט`, `תפילה וברכות`) reaches
  **2,961 (5.4%)** — together saving **~$14 of $186**. 92.7% of the residual has a source saying
  something substantive that genuinely needs judgment. **The idea is still CORRECT and worth keeping
  for accuracy reasons** (a vacuous aid means `fills_gap`; a genre-generic aid means
  `refines_granularity`, NOT a candidate — treating the latter as novel would inject exactly the false
  Bible-fragment novelty the owner flagged on the bypass path). It is simply not a cost lever.

- **Implemented (this ruling):**
  - `shared/discovery_novelty.py`: `NOVELTY_BATCH_PROMPT_TEMPLATE` derived by string surgery from the
    single-case template (so the judgment instructions stay byte-identical and only the response
    contract differs), guarded by a module-level `assert` that fails loudly if the single template is
    ever edited without re-pinning; `DEFAULT_BATCH_SIZE = 10`; `BatchResponseInvalid`;
    `resolve_batch_model_output`.
    ```
    BATCH_PROMPT_SHA256 = 3adb6f1e363fec13792fc517b642f864ac54f0aecaecd805623f822ea05590bb
    ```
    The single-case `PROMPT_SHA256 = 4b28747…` is **retained UNCHANGED** and remains the validated
    fallback contract.
  - `scripts/discovery_novelty_funnel.py`: `run_model_arm_batched` + `CostCeilingExceeded`.

- **Guardrail 1 — connection loss / malformed replies.** Checkpointing stays per-CANDIDATE and is
  flushed after every batch, so a killed run resumes without re-billing. Alignment is
  **all-or-nothing**: a reply missing a case number, carrying an unexpected one, or repeating one
  raises `BatchResponseInvalid` and NOTHING from it is checkpointed — because accepting the
  well-formed subset could attribute one fragment's verdict to a different fragment, an error that
  would be invisible downstream. Unaligned batches retry (`max_batch_attempts`, default 3) and then
  **degrade to the single-case contract**; with no fallback supplied the batch is left unresolved for
  the next run rather than recorded as a guess. Per-case vocabulary still fails closed to
  `not_checked` independently.

- **Guardrail 2 — price ballooning.** `cost_probe` is consulted **BEFORE** each batch is sent and must
  return REAL cumulative spend read from the provider's own `usage.cost` (never an estimate, per the
  standing rule). At or above `cost_ceiling_usd` the run raises `CostCeilingExceeded` and stops with
  its checkpoint intact. Checking before the call means the ceiling can never be crossed by the batch
  that discovers it.

- **Cache-key correctness.** `prompt_sha256` is a `CACHE_KEY_FIELDS` member, so a batched run MUST
  supply `BATCH_PROMPT_SHA256`; supplying the single-case hash would let a cache hit silently reuse an
  answer produced under a different response contract (only 89% behaviourally interchangeable).
  Pinned by `test_batched_run_must_not_reuse_single_case_cache_entries`.

- **Tests:** 15 new in `tests/test_discovery_novelty_contract.py` (94 → 109), covering prompt
  derivation + hash pinning, the batch-size knee, all-or-nothing alignment (5 misalignment shapes),
  per-case fail-closed, ceiling-stops-before-spending, checkpoint resume without re-billing,
  degrade-to-single-case, nothing-checkpointed-from-unaligned, and cache-key separation.

- **STILL NOT AUTHORIZED: the production run itself.** The owner rejected $301 and has approved batch
  10 as the mechanism; the ~$34 production run needs its own explicit go. Whoever runs it must pass an
  explicit `cost_ceiling_usd` and use `BATCH_PROMPT_SHA256` in the cache key.

---

## Ruling P — two held domain rows settled from FJMS's OWN work-level identification (owner, 2026-08-03)

- **Owner's question, verbatim:** *"Many of the works listed there are already in FGP, so in our database,
  so they have their domain. Am I wrong? If not, we can take the decision from FGP original domain info"*,
  refined to *"The question is how FJMS calls what in ITS IDENTIFICATION these works like Seder Olam"*,
  then **"We need to follow FJMS"**, confirmed against the corrected, narrower reading below.

- **The owner was right that a work-level domain exists, and the 136-09 curation pass missed it.**
  `fjms_enrichment.db::genizah_titles` carries a **`DomainId` populated for 718 of its 775 titles** — a
  domain attached to a TITLE (a work), not to an AlmaId. The curation pass deliberately avoided the
  manuscript-keyed `domains` table (correctly — that is the catalogue axis) but never discovered this
  work-level source. Record this as a real gap in that pass, not as a rules failure.

- **Decoding `DomainId`.** There is NO code→string table (`code_values` does not contain these codes;
  `catalog` has no `DomainId` column). The mapping was recovered empirically and must be recovered the
  SAME way by anyone repeating this: restrict to AlmaIds having **exactly one** `catalog.GenizahTitleId`
  AND **exactly one** `domains` row, then join. That yields **62 DomainIds at 99.8% mean / 100% median
  concentration** — effectively deterministic. **Do NOT decode by modal co-occurrence over all
  manuscripts** (26–54% concentration); that mixes in the domains of other works sharing a codex.

- **RULED — 5 of the 29 held rows, all mapping onto candidate leaves the artifact already offered:**

  | rows | canonical ids | ruled leaf | FJMS support |
  |---|---|---|---|
  | Yosippon (G1) | `w001152`, `w000853`, `w000855` | `Historiography and geographical descriptions / Historiography and geographical descriptions` | DomainId 180000, 100%, n=98, exact title match |
  | Seder Olam (G3) | `w000164`, `w001066` | `Rabbinic Literature / Other` | DomainId 120000, 100%, n=87 |

  Each ruled row must carry an `owner_ruling` citation pointing at THIS section. The remaining **24 rows
  stay held**; `--validate --release` must continue to fail closed until they are ruled.

- **NOT ruled, deliberately — the support is too thin:** `מגילת אביתר` (DomainId 170400 → Polemics
  Rabbinical) rests on **n=6**, and `ספר יצירה` (150500 → Mystical Literature (not Kabbalah)) on **n=1**.
  A 100% concentration over one manuscript is noise wearing a confident face.

- **"Follow FJMS" is SCOPED to this evidence, and is NOT a blanket override.** An initial, WRONGER
  reading of the cross-check was put to the owner and then retracted before anything was applied. Three
  findings bound it:
  1. **FJMS exact-covers only 55 of 1,073 works** (the earlier "195" came from loose containment matching
     and was an artifact).
  2. **FJMS's taxonomy is often COARSER than the rule table's.** One DomainId (n=200) lumps
     `מדרש חסרות ויתרות`, `ברייתא דמזלות`, `מדרש פטירת משה`, `ספר זרובבל` and `ברייתא דישועה` together as
     `Later Midrashim`, where the rule table distinguishes Massorah, Astrology and Philosophy.
  3. **In the largest disagreement the rule table is BETTER:** `הגדה של פסח` (2,180 claims) —
     ours `Liturgy and Brakhot / Passover Haggada` vs FJMS `Liturgy and Brakhot / Liturgical additions`.
  Agreement where comparable is **79.6%** (39/49 exact-matched works), which stands as the rule table's
  first independent validation.

- **RETRACTED before application — the "midrash convention" cluster does not exist.** An earlier analysis
  reported ~14 works and ~8,700 claims turning on an `Aggadic Midrashim` vs `Later Midrashim` boundary
  (Yalkut ×2, Lekach Tov ×5, Tanna de-Bei Eliyahu ×3, Avot de-Rabbi Natan ×2). **None of those works
  exact-matches an FJMS title** — every one was a loose-containment false match. Under exact matching they
  vanish from the disagreement list entirely. No convention ruling was made and none is needed. Anyone
  revisiting this must use exact normalized title matching; containment produced false positives
  including `מחברת` → a DomainId decoding to `Bible: Texts` at n=93,237.

- **Plans that must implement this:** **136-09** (re-emit + re-pin `work_domains-v1.json` with these 5
  rows carrying `domain_leaf` + `owner_ruling`; it remains HALTED for the other 24), and **136-12**
  (must not load `works.genre` until `--validate --release` exits 0).

---

## Ruling Q — the remaining 24 held domain rows, delegated (owner, 2026-08-03)

- **Owner's instruction, verbatim:** *"Go with your judgements, I trust you"* — issued after the full
  24-decision list, with candidates and evidence, was put to them.

- **Provenance note.** These are DELEGATED judgements, not owner-authored ones. Each ruled row cites this
  section. Where a call is thin it is marked ⚠ below and should be the first thing revisited if the facet
  ever looks wrong. Delegation does not make a weak call strong.

- **Governing principle:** where the closed vocabulary carries a leaf for *exactly this work*, use it.
  Falling back to a broader leaf leaves the specific node empty and destroys the information the facet
  exists to expose (so Kifayat al-Abidin → `Sufi Literature`, not the safer `Ethical Literature`).

| # | work | ruled | why |
|---|---|---|---|
| 1,3,14 | המספיק לעובדי השם (×3, 429 claims) | `Philosophy… / Sufi Literature` | the paradigmatic Jewish-Sufi text; the dedicated leaf exists for it |
| 2 | העיונים והדיונים | `Secular Poetry / Other` | Kitab al-Muhadara is Hebrew *poetics*; Grammar is about language structure |
| 4 | תעודות יהודי סיציליה | `Documentary / Documentary` | a deliberately MIXED documentary edition; the coarse parent avoids mislabelling its non-letter documents. Catalogue's 55% `Letters` describes the individual fragments, not the edition |
| 5 | אגרות הרמב״ם (שילת) | `Philosophy… / Ethical Literature` | Yemen / Martyrdom / Resurrection are theological-ethical treatises in letter form; `Documentary/Letters` means archival correspondence and would be a category error |
| 6,9 | ספר יצירה + רס"ג's commentary (224) | `Kabbalah / Other` | the vocabulary has no Sefer Yetzirah leaf; the mystical tradition is its conventional home, and FJMS's own (n=1, unusable) signal also pointed mystical. Kept together so text and commentary do not split across parents ⚠ *Saadia's commentary is genuinely philosophical-cosmological; a defensible alternative is `Philosophy` for #6 alone* |
| 7 | המעשה בפולמוס הכומר | `Polemics / Polemics Jewish-Christian` | catalogue 64% (n=69); its subject IS the disputation |
| 8 | כתאב אלדרר | `Secular Poetry / Other` | catalogue 38% (n=100); al-Harizi is a poet and the anthology carries his verse ⚠ *`Belles Lettres` is arguable for a prose anthology* |
| 10 | עשרים מאמרים | `Kalam / Jewish Kalam` | al-Muqammis' 'Ishrun Maqala is the FOUNDING Jewish kalam text; the dedicated leaf exists for it |
| 11 | משיבת נפש | `Biblical Exegesis / Biblical Exegesis- Karaite` | catalogue is 64% Karaite contexts (n=64), which selects Yeshua b. Judah's commentary over the devotional readings |
| 12 | אגרות שמואל בן עלי | `Documentary / Letters` | unlike #5 these are ACTUAL letters of the Baghdad Gaon preserved as correspondence |
| 13 | איגרת ההשתקה | `Philosophy… / Philosophy` | the artifact's own note leads with "a philosophical treatise addressed as a letter" |
| 15 | חטר בן שלמה, שאלות | `Philosophy… / Philosophy` | **catalogue DELIBERATELY overridden** (51% Responsa, n=37): Hoter b. Solomon is a known Yemenite philosopher and the artifact records the responsa reading as surface form only |
| 16 | מגילת אביתר | `Polemics / Polemics Rabbinical` | follows FJMS's own work-level domain ⚠ *n=6; it is a partisan account, so Historiography is arguable* |
| 17 | ערוגת הבושם | `Philology / Grammar` | the RECORDED author is Archivolti and the catalogue agrees (42%) ⚠ *n=12, and the artifact flags this row as a title/author collision with Abraham b. Azriel's piyyut commentary — a data-quality item, not settled by this ruling* |
| 18 | יהודה ראש הסדר, ספר השנים | `Astronomy / Calendar` | the title states the subject; the lexicographic reading comes only from the author's other work ⚠ |
| 19 | מרפא לעצם | `Medicine / Medical Works` | the title is explicit; magical recipes are a transmission context, not the work's subject |
| 20 | זיכרונות מימי נעוריי | `Historiography and geographical descriptions` | memoir as historical writing. **`Unassigned` deliberately NOT used** — it would hide a row the artifact says may not belong in the corpus at all; assigning it keeps it visible ⚠ *the real question is corpus membership, recorded as data-quality* |
| 21 | תולדות בן סירא | `Stories and Belles Lettres` | the Alphabet of Ben Sira is a satirical folk narrative in midrashic dress |
| 22 | ספר הזיכרון | `Halakhic / Halakhic- Gaonim` | ⚠ **LOWEST-CONFIDENCE CALL IN THIS SET.** The artifact itself says the subject is not determinable from title+author; Saadia's monographs are predominantly halakhic, so this is a prior, not evidence. 3 claims |
| 23 | פרקי ט׳ באב | `Derashot and Later Midrashim / Later Midrashim` | "פרקי" marks a midrashic composition (cf. Pirkei de-Rabbi Eliezer) ⚠ |
| 24 | תולדות רבנו הקדוש | `Stories and Belles Lettres` | hagiography sits closer to narrative than to historiography |

- **Two catalogue overrides are deliberate and should not be "corrected" later**: #15 (philosopher's
  questions, not responsa) and #4 (the edition is mixed even though its fragments are mostly letters).

- **Data-quality items raised, NOT resolved by this ruling:** #17 (title/author collision), #20 (a
  19th-century maskilic memoir carrying claims in a Genizah corpus). Both belong in `docs/OPEN_ISSUES.md`
  for a later pass; a domain assignment does not settle whether the row should exist.

- **With rulings P and Q together all 29 held rows are ruled**, so `--validate --release` can pass once
  the artifact is re-emitted and re-pinned. **136-09 must apply these and re-pin; 136-12 must not load
  `works.genre` until `--validate --release` exits 0.**

---

## Ruling R — curated display title for `משנה תורה, ספר אהבה` (owner, 2026-08-03)

- **Owner's ruling, verbatim:** *"Most MT Ahava is liturgy, so we can label it manually
  משנה תורה ספר אהבה / סידור"* — made after reviewing all 9,887 `fills_gap` rows grouped by
  identification (`novelty-FILLSGAP-by-work.PRIVATE.html`).

- **The problem.** `w000176` (`משנה תורה, ספר אהבה`, source_corpus `msource`) is the **10th
  most-claimed work in the corpus at 6,437 claim pairs**, and **448 of them are on the candidates
  surface**. Sefer Ahava CONTAINS the order of prayers, so liturgy is drawn to it — the text match is
  real, but the bare title tells a reader "this is Maimonides' halakhic book", which for most of these
  pages is wrong.

- **RULED — a DISPLAY-time relabel, corpus-wide for this work:**

  | | |
  |---|---|
  | HE | `משנה תורה, ספר אהבה / סידור` |
  | EN | `Mishneh Torah, Sefer Ahava / Siddur` |

  The label **names both readings and asserts neither** — pinned by a test
  (`test_curated_title_names_both_possibilities_never_asserts_one`).

- **Why display-time and NOT a data change.** The reference text these rows matched against genuinely
  IS Sefer Ahava; what misleads is the impression the title gives a reader. Rewriting
  `works.neutral_title` would (a) falsify what the matcher actually compared, and (b) silently change
  the novelty funnel's own mechanical name-matching if it were ever re-run. The correction therefore
  lives at the surface, where the misleading impression is.

- **Consistent with ruling N**, which already settled that these rows SHIP: an imprecise claim that
  points a reader at "this page is prayer" beats the silence these fragments otherwise have. Ruling R
  makes that pointer explicit instead of leaving the reader to infer it.

- **Implemented:** `shared/discovery_display_strings.py` — `CURATED_WORK_TITLES` +
  `display_work_title(work_id, neutral_title, lang)`. **Every surface that renders a work title MUST
  route through `display_work_title`**; a surface formatting `neutral_title` directly silently opts out
  of the curation and shows the misleading bare title. Applies to **136-15, 136-16, 136-17, 136-18** and
  any later title-rendering surface.

- **This is the FIRST instance of the deferred ~2,670-work title curation** (`136-CONTEXT.md` line 800,
  carried to the v2.1 phase). The mechanism is deliberately a small owner-ruled table, not a
  data-quality workshop: entries stay rare and each one needs a ruling. Registered in the honesty sweep
  (`SWEEP_INPUTS`), so the curated string clears the same gate as every other reader-facing string.

- **Tests:** 4 new in `tests/test_discovery_display_strings.py` (26 → 30) — bilingual override,
  uncurated pass-through, the both-readings invariant, and a bilingual-completeness check so a
  half-filled entry cannot silently fall back to the other language.

---

## Ruling S — the public artifact ships the two-axis conjunction, including JA direct matches (2026-08-03)

**Context.** Gate 9 of the 136-13 rebuild battery reported that the two ways of computing the public
scope disagree on **36,989 of 297,415 evidence rows (12.4%)**. The projection had shipped the two-axis
conjunction. The plan deliberately makes this a REPORT, not a resolution, because a projection
silently widening or narrowing the published set is the failure the number exists to catch.

**Direction, measured (not inferred from the counts):**

| | rows | corpus × family |
|---|---|---|
| the conjunction ships, the VIS-01 shortcut would NOT | 24,094 | `ja` × `track1_direct` |
| the VIS-01 shortcut would ship, the conjunction does NOT | 12,895 | `msource` × `propagated` |

The second half is not a decision: `_vis01_shortcut` returns True for EVERY `propagated` row
regardless of corpus, so it would publish restricted-identity material. The per-row conjunction
correctly withholds it. The shortcut is wrong there and the current behaviour is right.

**Owner ruling: SHIP the JA direct matches.** The per-row visibility flags are the authoritative rule
everywhere else in the system; those works are public and their matches cleared the same routing and
quality gates as the Sefaria ones.

**The claim-integrity question was raised and then ANSWERED, not deferred.** The concern was that
CERT-01 measured precision over a specific population, and the certified figures are recorded per
confidence band, never per corpus — so shipping JA direct rows might extend the certificate over rows
it was never measured on. Checked directly against the graded deck
(`same_work_spike/probe/data/cert01_deck_key.json` + `review/cert01_deck_verdicts.json`, joined to the
rebuilt asset):

- 48 of the 280 graded cards sit on JA-corpus works (6 `track1_direct` only, 42 carrying both families).
- Of the 220 **candidate-role** cards — the population the precision estimand is computed over —
  **44 are JA**, against 133 Sefaria and 43 restricted-corpus.

JA is therefore ~20% of the graded candidates while JA direct matches are ~10% of the shipped public
evidence rows: the corpus is if anything OVER-represented in the measurement relative to what ships.
The certificate covers these rows; it is not being stretched over them.

**Consequence:** the artifact is correct as built. No rebuild, no re-projection, no change to the gate
battery. `_vis01_shortcut` is now known to be a stale, unsafe rule — it survives only as one input to
this reconciliation report and must never be used as a publication gate.

---

## Ruling T — the "more matches" bucket carries ~half the non-Bible discovery value (2026-08-03)

**How this surfaced.** Pre-deploy content review of the public artifact. The corpus is **35.1%
non-Bible** but the default candidates surface is only **18.5%** non-Bible — 3,153 of the 4,152
default-visible candidate finds are Bible. The owner's reaction ("~1,000 non-Bible findings is a bit
disappointing") prompted a diagnosis rather than a reassurance.

**What the numbers showed.** Non-Bible material is not missing; it is in the second bucket.
**2,189 non-Bible `fills_gap` identifications sit outside the main pool** — ~3× the 769 shown.
Hold-out reasons, non-Bible vs Bible: `shared_wording` 983/329, `overlapping_tie` 450/162,
`missing_signal` 458/896, `low_coverage` 263/550, `insufficient_length` 35/125. The two that hit
non-Bible ~3× harder are `shared_wording` and `overlapping_tie`.

**Restricted to STRONG evidence** (`best_band_rank <= 2`) the picture flips: **989 rows**, of which
`overlapping_tie` **462**, `low_coverage` 281, `shared_wording` 205, `insufficient_length` 41. So at
the strong end the dominant cause is *two works competing for the same span and the rule declining to
pick* — a **disambiguation** failure, not a match-quality failure.

**Owner assessment (2026-08-03), on the 975-row review page
`discovery_data/EXCLUDED-STRONG-nonbible.PRIVATE.html`:** *"about half of them are right, and we lose
quite a bit if we give them up."*

**This is a vibe-check, NOT a measurement** — an owner impression over a rendered sample, with no
draw protocol, no blind grading and no held-out frame. It must never be quoted as a precision figure,
and per D-06/D-21 no percentage derived from it may reach any reader-facing surface.

**Ruling: change nothing in the artifact; the rows already ship.** They are present in the public
projection as `fills_gap` with `main_pool = 0`, i.e. in the **"more matches" / "עוד התאמות"** bucket,
whose contract (`shared/discovery_main_pool.py::bucket_label`) already states that it *"means there
was not enough evidence for the main-pool rule — it never means the identification is probably
wrong"* and that a caller *"must never render it as a confidence level or a correctness verdict"*.
The owner's ~half-right reading is consistent with that contract, and is the first evidence for it.

**Do NOT loosen the main-pool rule to promote them.** At roughly half right they sit below every
shipped band, including the weakest (`screening_canon`, 0.647). Promoting them would degrade the
headline surface to raise a count.

**Consequences for the surface plans (136-16 findings page, 136-18 polish):** the "more matches"
bucket is not a long tail to be tucked away — it holds ~2,189 non-Bible candidate finds, roughly half
the non-Bible discovery value in the release. It must be **genuinely reachable** from the findings
page, in match-framing wording, with no number attached. A design that hides it behind an obscure
control would bury the majority of the non-Bible result.

**Consequence for gen-2:** `overlapping_tie` at strong evidence is exactly what the deferred
witness-vs-quoter / compilation lever targets. This replaces the single Yalkut anecdote with **975
labelled cases** and a first-order estimate of the payoff (~half recoverable). See
[[project_novelty_is_granularity_relative]] and the v2.1 track.

---

## Ruling U — launch framing leads with the contribution figure (2026-08-03)

**Decision (owner, 2026-08-03).** The public launch leads with **what the release adds to the
existing finding aids**, not with corpus coverage and not with first-time finds.

```
HEADLINE   9,523 identifications the finding aids did not already have
             4,152  no prior identification            (fills_gap)
             3,873  finer than the aid                 (refines_granularity)
             1,498  aid named only a container         (container_predicts)
CONTEXT    out of 38,431 fragments across 177,402 pages
```

**Why it was a decision.** Three true numbers were on the table and the choice is which one carries
the headline: coverage (38,431 fragments / 177,402 pages), contribution (9,523), or first-time finds
(3,666 manuscripts). Coverage is the largest number the release is entitled to claim; contribution is
the hardest to dismiss with "we knew that already". The owner chose the harder-to-dismiss number over
the larger one. The other two figures still appear on the page as context — this ruling governs
emphasis, not disclosure.

**Basis correction that prompted the ruling.** An earlier draft quoted **13,285** for this figure. It
was wrong — built on two different filters, adding main-pool `fills_gap` to *unfiltered*
`refines_granularity` and `container_predicts`. On one consistent basis (`main_pool = 1`, the default
view) it is **9,523**, over 6,755 manuscripts. Including the "more matches" bucket it is 17,536 over
10,959 manuscripts. Recomputed directly from the deployed public artifact
(`discovery-v1-e9365edc…`), not copied forward.

**Binding constraints on 136-16, 136-17 and 136-18.**

1. **One basis, stated.** All four headline numbers are `main_pool = 1`. Any surface that mixes
   main-pool and all-bucket counts in a single total is a defect. If a page shows an all-bucket
   figure it must say so in words.
2. **These three sub-numbers are now load-bearing** and are read from the artifact, never hardcoded —
   they change on any rebuild. A hardcoded launch number is the same class of bug as the 13,285
   above, and would not survive the next bake.
3. **No precision percentages anywhere** (D-06/D-21). "The finding aids did not already have" is a
   *provenance* claim about the aids, not a *correctness* claim about the match, and must be worded
   so a reader cannot read it as an accuracy rate. This binds error paths and JSON envelopes too.
4. **Match-framing wording throughout** — `container_predicts` renders as "the aid named only a
   container", never as "the aid was wrong".
5. **Ruling T still binds the same pages.** Leading with contribution does not license burying the
   second bucket: it holds 8,013 further adds-something identifications, ~2,189 of them non-Bible
   `fills_gap`. It stays genuinely reachable, unnumbered, in match-framing wording.
