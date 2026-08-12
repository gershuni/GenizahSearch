# Discovery relation matrix v1 — the A0a-2 artifact

*Status: **semantics FROZEN 2026-08-12** (plan phase A0a-2, `_tmp/PLAN-granularity-ship.md`
v10). Population counts are measured on `discovery-v3-PUBLIC` (55,377 identifications,
28,464 main-pool rows) and are **ratified — or amended — in A0b**, once the CD schema batch
locks the pre-withholding population. The reader vocabulary is owner-ruled (2026-08-11).
This document freezes MEANING; it deliberately fixes no threshold the owner has not ruled.*

*What this artifact is for: Contract 1's implementation (C-track) renders relations from
this matrix and nothing else; B's replay harness scores rendered assertions against grades
through §4's crosswalk and nothing else; Contract 3's audit uses §4's vocabulary. A change
to any table here is a dated amendment to this file, not a code comment.*

---

## 1. The relation states and the reader vocabulary

The matrix output — the **rendered relation** — is one of five states. It is a *display*
verdict about one identification (one manuscript × one work): `routing_status`,
`claim_type`, and every other stored column NEVER change.

| rendered state | Hebrew | English | notes |
|---|---|---|---|
| `direct_witness` | מתאים לחיבור | Matches this work | a text claim, not an identity claim |
| `shared_text` | חולק טקסט | Shares text with this work | |
| `quotes_this_work` | כולל ציטוט | Includes a quotation | |
| `work_quotes_page` | — | — | renders ONLY where a validated direction signal supports it; **no such signal exists today**, so this state always falls to `shared_text`. Assigning its strings is an owner item deferred until a signal ships. |
| `uncertain` | דורש בדיקה | Needs review | the fail-closed state |

This is the owner's **softer register** ruling (2026-08-11): it retires `Direct match` /
`התאמה ישירה` — today's constant on 28,462 of 28,464 main-pool rows — and it fixes a
misnomer, because `quotes_this_work` currently renders as "Partial match". The C-track
change lands in `shared/discovery_display_strings.py::_RELATION_CHIP`; every string goes
through `assert_surface_honesty`, and the pair (state → string) is pinned in
`tests/test_discovery_display_strings.py` (the pinned-test annex in the plan owns the
test updates).

## 2. The precedence matrix

Six rules, evaluated in order, **first match renders**. Inputs: the identification's
shipped-evidence existence, the display row's routing reason, the region map, the QUOTER
work-divergence flag (plus the curated quoter list, same step), and the coverage band.

| # | rule (first-firing) | output | main-pool rows¹ |
|---|---|---|---:|
| 1 | no shipped evidence at all → | `uncertain` | 9 |
| 2 | display row's `routing_reason` ∈ {`later_shared_text`, `co_citation`} → | `shared_text` | 173 |
| 3 | region: the row's ENTIRE matched footprint lies in non-discriminative units → | `shared_text` | 115 |
| 4 | QUOTER flag (work divergence ≥ T) **or** work on the curated quoter list → | `quotes_this_work` | 943 |
| 5 | coverage unknown → | `uncertain` | 130 |
| 6 | otherwise the stored relation stands | stored `relation_kind` | 27,094 |

¹ Census of 2026-08-11 (`region_matrix_sweep.py` → `_tmp/region_sweep_2026-08-11.json`),
at the reference parameterization T=50%, region fail-closed, curated list empty. Gross
reaches, for reconciliation: router 182 (= steps 1+2 exactly), QUOTER@50 1,004,
coverage-unknown 147 (17 are consumed by earlier steps), region(closed) net 115 — 88 when
open cards are excluded. Today's baseline for comparison: `direct_witness` 28,462,
`quotes_this_work` 2, `shared_text` 0.

**Rule semantics, frozen:**

- **Step 1** is the honest router predicate at this grain: "no shipped evidence at all"
  (9 rows). The tempting "any evidence is `review_only`" reading flags 4,070 rows and is a
  display-selection artifact — 4,061 of them have shipped evidence for the same claim
  (`_tmp/CONTRACT1-matrix-findings.md` §1; the `display_evidence_id` selection bug is
  tracked separately).
- **Step 3** is a **fail-closed per-unit prior**: demote only when the whole footprint is
  known non-discriminative (units ruled or derived `shared`; `open` cards count as shared
  under the frozen fail-closed posture; a unit **nobody asked about always blocks** — the
  region map is partial and an unasked unit is not a ruling). Region is measured NOT to be
  the load-bearing input: 7 of the 11 known false-direct rows are structurally unreachable
  by any per-unit flag (`_tmp/REGION-SWEEP-findings.md` §3). It stays in the matrix as a
  cheap, correct demotion, not as the fix.
- **Step 4's threshold T is a parameter, not part of the frozen semantics.** The owner has
  ruled that no threshold ships in deploy 1. The one priced candidate worth an owner
  discussion is T=60 **on the expansion pane only**: 20.7% false reduction at 3.2% recall
  loss (A0c resolved read). On the findings page every setting fails (best useful point
  costs 21.7% recall) because that surface's false rows are Bible-quotation rows QUOTER
  cannot reach.
- **Step 4's curated list** is a versioned, owner-ruled list of known quoter works whose
  rows render `quotes_this_work` regardless of divergence. It ships EMPTY until ruled.
  Priced candidate: ילקוט שמעוני על נ"ך — 4/4 false in A0c, 0/57 `correct` in the 1,402
  blind grades, 101 main-pool rows.
- **Step 5** contributes known/unknown, not a band: the public asset has exactly zero rows
  in the 0–10% coverage bands (the router consumed that population upstream), so a band
  threshold would gate nothing.

**Missing-input rule (fail-closed, per input).** "Fail-closed" means: never render a
STRONGER claim than the present inputs support.

| input absent | effect |
|---|---|
| shipped evidence | `uncertain` (step 1 — absence IS the signal) |
| coverage | `uncertain` (step 5) |
| region map entry for any unit in the footprint | step 3 cannot fire (a demotion is also an assertion; it needs positive knowledge) |
| QUOTER divergence value / threshold unset | step 4 cannot fire |
| stored `relation_kind` itself | `uncertain` |

**Asset-relativity and the verifier gate.** The rendered relation is a stored column,
recomputed **per asset** after public pruning and after locus/coverage derivation. The
sidecar verifier recomputes it from its inputs on every shipped asset and asserts equality
row-for-row; the gate is mutation-proven (a deliberately mis-stored row must fail the
build). The region map and the curated list are versioned sidecar tables written by the CD
schema batch — never a JSON on someone's disk. Contract 0's coordinate-basis hash equality
(bake's reference-corpus hash = locus build's) is a precondition of the same batch.

**Deploy-1 parameterization (owner ruling, 2026-08-11):** steps 1, 2, 5 active; step 3
NOT activated (its census is priced: findings 9.1% reduction at 0.9% loss, expansion 9.1%
at 14.3% loss — an A0b decision, and the expansion number says it is not free); step 4
empty (no threshold, empty curated list). Deploy 1 is a fail-closed correction plus the
surface-grain unification of §3, claims no measured improvement, and is gated by B on
no-regression only.

## 3. The surface-grain mapping table

Four grains render relations today, through four different derivations — stored column,
SQL strongest-member, raw `claim_type`, raw alias. After C-track there is ONE shared read.
Rows below name every surface and export path; **a new path that renders a relation lands
a row in this table before it ships.**

| surface | renderer read (today) | grain / member set | relation today | relation after C-track | withheld row² |
|---|---|---|---|---|---|
| findings — identification unit | `shared/discovery_service.py:863` → chip `web/components/findings_rows.py:1690` | one identification (1 ms × 1 work, all its pages) | stored `relation_kind` | stored matrix column | `uncertain`, no locus, row stays |
| findings — work unit (grouped) | `:906` — `NULL AS relation_kind` → no chip | all identifications of one display work | none asserted | unchanged — asserts nothing | row + counts stay |
| findings — manuscript unit (grouped) | `:884` — `NULL AS relation_kind` → no chip | all identifications in one manuscript (multi-work) | none asserted | unchanged — asserts nothing | row + counts stay |
| panel — claim rows | `:2445` aliases raw `claim_type` → `relation_kind`; chips via `shared/discovery_panel_model.py:690,1034,1310` | one page-level claim (member of an identification) | raw `claim_type` | member's own relation, **capped** (§3.2) by its identification's matrix output | `uncertain`, no leaf locus |
| panel — manuscript summary ("elsewhere in this manuscript") | `:1425–1433` SQL strongest-member over claims; chip `web/components/discovery_panel.py:786` | identifications of one canonical work in this manuscript | SQL `MIN(CASE claim_type…)` | strongest **matrix output** over members (drop the second comparator in SQL) | withheld member contributes `uncertain` to the max; row stays |
| panel — expansion pair rows | `:1479` raw `claim_type` + `anchor_claim_type`; **the two raw `relation_chip(claim_type)` calls: `web/components/discovery_panel.py:425,428`** | one carrier × anchor pair | raw claim types, both sides | each side's own relation, capped (§3.2) by its OWN identification's matrix output; `relations_differ` kept | `uncertain`, no citation |
| panel — related pages | `SURFACE_RELATED_PAGE_FIELDS` (no relation field) | unevaluated candidate alignment | none asserted | unchanged — pinned never to grow a relation without a row here | n/a (not an identification) |
| findings — launch shades + facet counts | `SURFACE_LAUNCH_SHADE_FIELDS` / `SURFACE_FACET_FIELDS` | aggregates | none | unchanged | counts NEVER change — withholding a citation never deletes an identification |
| export / API | **none today** — verified: `web/api.py` and `shared/export_dossier.py` carry no discovery field | — | — | any future path (F2's citation sheet) reads the stored matrix column through a projection allowlist and lands a row here first | per its grain |

² Contract 4 semantics: a control-plane-withheld row renders no locus and falls to the
matrix's fail-closed state; the row itself remains on every surface and in every count.

**§3.1 The aggregate rule is grain-dependent and its direction FLIPS** (measured,
`_tmp/CONTRACT1-matrix-findings.md` §7.6):

- **Identification grain — strongest-member is CORRECT.** One identification is one
  manuscript × one work over several pages; a manuscript that witnesses the work on ANY
  page IS a witness. 7,587 identifications (13.7%) carry displayed claims of more than one
  `claim_type`, and 6,449 store `direct_witness` over a mix — a blanket "most conservative
  member" rule would wrongly demote all of them. Strength order (today's SQL order, kept):
  `direct_witness` > `quotes_this_work` > `shared_text` > `uncertain`.
- **Multi-identification groups — no single assertion.** The findings page's work and
  manuscript units aggregate DIFFERENT identifications; they render `NULL` → no relation
  chip today, and that behavior is pinned as the rule: a grouped row renders either no
  relation assertion or (if a future design needs one) the most conservative member —
  never a strongest-member aggregate.
- The manuscript-summary row sits at the identification grain (one canonical work × this
  manuscript), so strongest-member applies — but over MATRIX outputs, not raw claim types.

**§3.2 The member-grain cap rule** *(drafted here — the plan left member-grain semantics
to this table; A0b ratifies)*: a member-grain surface (panel claim row, expansion pair
side) renders the member's OWN stored relation mapped through §1's vocabulary, except it
never out-asserts its identification:

- identification matrix output `uncertain` → the member renders `uncertain`;
- identification demoted to `shared_text` / `quotes_this_work` → a member whose own
  relation is `direct_witness` renders the demoted state instead; members already
  asserting a non-direct relation keep their own.

Rationale: every demote step is evidence about the identification as a whole; a page-level
row asserting MORE than its identification would reopen exactly the gap the matrix closes.

## 4. The scoring crosswalk (grade → metric)

Fixed here, before B is built. B scores rendered assertions per surface against graded
truth through THIS mapping and no other.

**Grade classes** (the 1–9/0 grading scale's class names, as pinned by the A0c and e1l
analyses):

| grade | class |
|---|---|
| `correct`, `cowitness`, `partial` | **true witness** |
| `quote_ab`, `quote_ba`, `quote_shared`, `formula`, `wrong` | **false when rendered `direct_witness`** |
| `unsure` | counts in n, in NEITHER class; fail-closed bounds count it false |
| `too broad` | scores the CITATION (Contract 3), never the relation |

**Per-surface scoring.** Each surface is scored against the assertion IT renders (findings
= the identification row's rendered relation; expansion = the pair side's rendered
relation). *False-direct rate* = weighted share of rendered-`direct_witness` rows graded
false. *Reduction* = false-direct removed ÷ baseline false-direct. *Recall loss* =
true-witness rows demoted from `direct_witness` ÷ baseline true-direct. A0c rows carry
their recorded inverse-probability draw weights; the 1,402 dev grades are unweighted and
always labeled in-sample.

**Membership policy:**

- a control-plane-**withheld** row scores as NOT rendering a false assertion AND as a
  recall loss when its grade is true — withholding is never free;
- a row **absent from the public asset** scores out-of-population;
- **grouped/summary** rows score by their rendered assertion under §3.1 — a grouped row
  that renders no relation scores in neither class;
- golden fixtures for the grouped, withheld, and membership-changed cases live in B's
  test suite.

**Datasets and their roles:** dev = the 1,402 e1l blind grades (rule design, in-sample).
**Release gate = A0c's frozen 220** (frame `a0c-2026-08-11-v2`; the catalogue-assisted
pass-2 resolution is recorded under `-pass2`, never overwriting a blind grade; 3 rows stay
irreducibly unsure). The frozen 400-row set is signal-development data only — it governs a
population the public surfaces do not render, and is never a gate.

**Sparse cells:** the bar applies where a class × surface cell has n ≥ 30; smaller cells
are report-only and fail-closed. Measured: the findings-page false cell is n=13 raw →
**per-surface Contract-1 claims on the findings page are report-only**; the expansion
pane's n=33 meets the floor.

**Baselines B must reproduce before any change ships** (A0c resolved read, weighted;
`_tmp/A0C-findings.md` §7): findings-page false-direct **12.2%** (blind 12.4%, fail-closed
14.0%); expansion-pane **41.3%** (42.0%, 46.3%).

## 5. What A0b ratifies (the open parameters, priced)

1. The census counts of §2, recomputed against the population the CD batch LOCKS.
2. QUOTER threshold per surface — the only live candidate is expansion-only T=60
   (20.7% reduction at 3.2% loss); findings: no setting worth its cost.
3. Region step activation per surface (findings 9.1%@0.9%, expansion 9.1%@14.3%) and the
   fail-closed posture as the open-card set shrinks (the 16-card pass decides 16 cells).
4. The curated quoter list's first membership (ילקוט שמעוני priced above).
5. The member-grain cap rule (§3.2, drafted here).
6. The bar itself: a REACHABLE point on the measured frontier per class × surface — the
   inherited −50%/≤2-point pair is retired by measurement.

Adjacent owner items riding the same sitting (not matrix parameters): the expansion-pane
honesty cue (catalogue-disagree rows are ~two-in-three false — the yardstick stays OUT of
adjudication either way), and A0b's Contract-2/3 floor ratifications.

## 6. Provenance

Measurements: `_tmp/CONTRACT1-matrix-findings.md` (inputs, frontier, §7.6 aggregate
directions) · `_tmp/REGION-SWEEP-findings.md` (region semantics, census, the 7-of-11
unreachability) · `_tmp/A0C-findings.md` (public-population grades, baselines, per-family
tables) · `_tmp/PREFLIGHT-granularity.md` (denominators, floors, too-broad cells). All
gitignored working reports; the tracked narrative record is `docs/OPEN_ISSUES.md`
(entries of 2026-08-11/12). Scripts: `same_work_spike/probe/scripts/region_matrix_sweep.py`,
`a0c_analyse.py`. Owner rulings: 2026-08-11 (vocabulary, no-threshold-yet, A0c approval,
Bible chapter grain).
