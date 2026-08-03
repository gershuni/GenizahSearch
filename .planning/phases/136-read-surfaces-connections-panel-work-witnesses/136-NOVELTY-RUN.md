# Phase 136 Plan 04 Task 3 — Novelty Funnel Run Record

**Status (2026-08-03 session, primary checkout): the ruling-I re-measurement WAS executed, for
real, against the real sidecars and the real pinned model, with a real measured cost. The
authorized PRODUCTION run was NOT executed — not because of an environment gap (the 2026-08-02
worktree gap in section 0 below is a DIFFERENT, now-resolved problem), but because the
re-measurement itself surfaced a material accuracy regression AND a real cost/scale re-derivation
that together make an unconditional, automatic production spend irresponsible. Per this plan's own
Task 3 instruction and the dispatch that authorized this session's work, a material regression is
the owner's decision to accept, not the executor's — so this record stops at that decision point and
reports the measured numbers, rather than proceeding.** Section 0 below is preserved verbatim from
the 2026-08-02 worktree attempt (a genuinely different problem — missing files/credentials — that
this session's environment does not have); sections 2 and 3 are REPLACED with this session's real
results; sections 1, 4, 5 and 6 are updated to reflect what changed.

## 0. Why the FIRST attempt (2026-08-02, parallel worktree) did not happen — an execution-environment gap, not an owner decision

This plan (136-04) was executed by a PARALLEL executor inside an isolated git worktree
(`isolation="worktree"`, per this plan's own dispatch). Before writing this record, the environment
was checked directly and found to lack every precondition Task 3's real run requires:

- **The owner-labelled ground-truth file is absent from this worktree.**
  `discovery_data/novelty_hardcase_labels-v1.json` (written by plan 136-03 Task 4, recorded content
  hash `sha256:ce0dc2fb176d01de5f04975eac82017feaeb390db71e9ec13f3872a77d28d4b6` in
  `136-GATE1-DECISIONS.md`) does not exist anywhere under this worktree's checkout. `discovery_data/`
  is gitignored project-wide (`/discovery_data/` in `.gitignore`), and a git worktree is a SEPARATE
  working directory on disk from the primary checkout — gitignored files are never shared between
  them, only git-tracked content is. The file exists in the primary checkout, not here.
- **Every real checked-source sidecar is likewise absent.** `fist_data/fjms_enrichment.db`,
  `pgp_data/pgp.db`, `fgp_data/fgp_transcriptions.db`, and every `discovery-v1-*.db` asset are all
  gitignored, multi-hundred-megabyte-to-gigabyte binary sidecars that do not exist in this worktree
  either — confirmed directly (`fist_data/` here contains only a small synthetic manifest;
  `pgp_data/` here contains only import-report text files; no `.db` file of any kind exists at the
  repo root of this worktree).
- **No LLM provider credentials are configured in this execution environment.** No
  Gemini/OpenRouter/Google-API-key-shaped environment variable is present. Even had the label file
  and the sidecars been available, the pinned `gemini-3.6-flash` calls this task's re-measurement and
  production run both require could not have been placed from here.
- **The masking-scan pattern file is likewise a gitignored artifact absent from this worktree**
  (`.masking_patterns`) — the masking-scan verification below points
  `MASKING_SCAN_PATTERNS_FILE` at the primary checkout's copy for a read-only scan of this document
  itself; this does not bear on the run-execution gap above, it is noted only for completeness.

None of the above is a Rule 1–3 auto-fixable blocking issue: fabricating sidecar data, guessing at
credentials, or copying multi-gigabyte production databases across a parallel worktree boundary
would each be worse than reporting the gap honestly. This is recorded here, plainly, rather than
worked around.

**This gap is fully resolved in the session that wrote sections 2–3 below.** That session ran on the
PRIMARY CHECKOUT (`C:\Genizahsearch`, branch `master-main`), where the label file, all three real
sidecars, and a working `OPENROUTER_API_KEY` in `.env` were all directly verified present before any
work began.

## 1. The recorded authorization

Per `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-GATE1-DECISIONS.md`
section B, the owner's authorization on record is **`run-now-pinned`**: "RUN NOW, PINNED. The
validated cheap configuration (`gemini-3.6-flash`, `reasoning:{effort:"low"}`), ~$27 one-time. Do NOT
downgrade the model. Read the real `usage.cost` from the provider after the run — never estimate it."

Per section I of the same record, decision B's "run now, pinned" authorization does **NOT** become
operative for the full production run until a re-measurement of the pinned config against the
owner-labelled evaluation set, on the CURRENT ten-value vocabulary and ruling G's free-text input
contract, is on the record. **That re-measurement is now on the record (section 2 below) — real,
not deferred.** Per this plan's own gate (and the dispatch that authorized this session), the
re-measurement's result determines whether decision B's authorization becomes operative for the
production run. Section 3 below explains why it does not, yet.

The `~$27` figure on record is a COST estimate carried forward by size-extrapolation, never an
accuracy result — it is never cited as one here. Section 3 also RE-DERIVES a real, measured cost
projection from a real (free, zero-model-call) full-corpus heuristic pass, which is a much larger
number than `~$27` and is reported as such, per ruling J's own outstanding instruction to re-derive
it "against the ACTUAL residual size."

## 2. Ruling-I re-measurement — EXECUTED, real sidecars, real pinned model, real cost

### 2.1 Preconditions verified before any model call

- `discovery_data/novelty_hardcase_labels-v1.json` loaded via
  `scripts/discovery_novelty_funnel.py::load_owner_labels(path, expected_content_hash=...)` with
  `expected_content_hash = "sha256:ce0dc2fb176d01de5f04975eac82017feaeb390db71e9ec13f3872a77d28d4b6"`
  (the hash recorded in `136-GATE1-DECISIONS.md` Task 4). **The hash matched — verified, not
  assumed.** Had it mismatched, `LabelHashMismatch` would have refused to grade (this refusal path is
  itself unit-tested in `tests/test_discovery_novelty_contract.py`, per Task 2).
- All three real checked-source sidecars (`fist_data/fjms_enrichment.db`, `pgp_data/pgp.db`,
  `fgp_data/fgp_transcriptions.db`), the live discovery-v1 asset
  (`discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff.db`, matching
  `discovery_data/manifest.json`), `libraries.csv`, and a working `OPENROUTER_API_KEY` in `.env` were
  all directly verified present (file existence + a real, successful `openrouter.ai` round trip)
  before any per-case work began.
- The pinned contract constants read at run time from `shared/discovery_novelty.py` (never a second,
  hand-copied literal): `LLM_MODEL="gemini-3.6-flash"`, `LLM_MODEL_VERSION="gemini-3.6-flash"`,
  `LLM_REASONING_EFFORT="low"`, `PROMPT_SHA256=441058ae3bab6e5ee17beb0fc5ea39426d7c250feb6c2bd288f0bc1605c98be5`,
  `INPUT_NORMALIZATION_SHA256=447f8fc7aa9bbb2c25c9db946de94b1ed236f561141e7a81e3478f3d430f2e50`. Every
  real OpenRouter response's own `"model"` field echoed back `google/gemini-3.6-flash` — no silent
  provider-side downgrade or snapshot substitution observed.

### 2.2 Scope of this re-measurement — the SHADE axis only, stated explicitly

The 101-case label file carries FOUR question types. This re-measurement is scoped to the **60
`shade` cases** (Class 6 catalogue-divergence, 30, + Arm 1 residual, 30) — the population the pinned
novelty prompt is actually designed to classify:

- **`identity` (8 cases)** use a wholly different four-token vocabulary
  (`same_work`/`different_works`/`unsure`/`skip`) testing the D-13d granularity-collapse rule, not the
  novelty shade prompt — out of scope for this measurement; no prediction was made for these (the
  grading harness correctly reports them as `missing_predictions`, not as a disagreement).
- **`demotion` (25 cases, Arm 2)** grade the funnel's OWN already-executed heuristic demotion directly
  from the owner's `demotion_correct`/`false_known`/`unsure` verdicts — by ruling J's own design,
  these rows never reach a model at all, so there is no model prediction to compare; the grading
  harness tallies the owner's recorded verdicts unchanged (see 2.6).
- **`no_verdict_by_design` (8 cases, Arm 3)** carry no owner verdict at all by design.

### 2.3 Real candidates built from the real sidecars, per ruling G

For each of the 60 shade cases, a `NoveltyCandidate` was built from the case's `sys_id` and the raw
`ref_work_id` parsed out of its own `claim` field (the CLAIMED work, never the catalogue's
alternative naming in a `catalogue_divergence` row), populated from the REAL sidecars:

- `claimed_title` / `claimed_author` — from the live discovery-v1 asset's `works` table
  (`neutral_title` / `author`), keyed on the raw work id (never `canonical_work_id`, per Codex finding
  3).
- `catalogue_text` — the union of `libraries.csv` column 7 and FJMS `catalog.TitleHeb` /
  `GenizahTitleOrgTitle` / `Title` / `GenizahTitleEngTitle` (never the wrong `catalog_refs` field).
- `bibliography_rows` — REAL row text (`RunningTitle`/`RunningTitleHeb`/`TitleAcronymHeb`/
  `TitleAcronym`/`ArticleName`/`ArticleAuthorHeb`/`ArticleAuthorEng`/`NoteForDisplay`, non-null fields
  joined) plus `TranscriptionType`, read directly from `fist_data/fjms_enrichment.db`'s
  `bibliography` table — never merely a presence flag.
- `pgp_description` / `pgp_transcription` — REAL text from `pgp_data/pgp.db`'s `documents` table,
  joined through `document_fragments` on `sys_id` (concatenated across every PGP document linked to
  this fragment) — never merely a presence flag.
- `fgp_texts` — REAL `title_he`/`author_he`/`title_en` text from
  `fgp_data/fgp_transcriptions.db`'s `fgp_transcriptions` table.
- `m_source_shelfmark_text` — **`None` for every candidate.** Per `docs/specs/discovery-novelty-v1.md`
  §3, M-source shelfmark attributions have **no prior implementation anywhere** to build from in this
  session's scope; treating this source as absent (which the funnel already handles — see Arm 3
  below) is the honest, fail-closed choice, not a fabricated stand-in.
- `page_mapped=True` for all 60 — every case's `sys_id` was independently re-derivable from a real
  shipped `discovery_claim.page_id` in the live asset (the join Codex finding 4 measured clean).

### 2.4 Funnel-first execution (ruling J) — real heuristic pass, then real model calls on the residual

**Heuristic pass (free, mechanical, zero model calls) over all 60 shade candidates:**

- **1 of 60 resolved mechanically to `confirms`** (case 17, via a real bibliography-text name-match).
  **The owner's actual label for this case is `diverges_work`.** This is a REAL, measured instance of
  ruling J's own predicted risk — a mechanical false-known, now PERMANENT under the funnel-first
  design (this row never reached the model at all). It is counted as a disagreement in section 2.5
  below, exactly as the funnel would actually behave in production.
- **59 of 60 were UNRESOLVED (residual)** — these are exactly, and only, the candidates that reached
  the pinned model.

**Model arm (real `gemini-3.6-flash`, `reasoning:{effort:"low"}`, via OpenRouter) over the 59-case
residual**, run through `scripts/discovery_novelty_funnel.py::run_model_arm` with a real, file-backed
JSONL checkpoint (`discovery_data/novelty_remeasurement_checkpoint.jsonl` — gitignored, never staged;
`git status` confirms nothing under `discovery_data/` is tracked by this session):

- Each candidate's evidence bundle (per `assemble_evidence_bundle`, tagged by provenance:
  catalogue/bibliography/pgp/fgp/m_source_shelfmark) was sent as the user message against the pinned
  `NOVELTY_PROMPT_TEMPLATE` system prompt, with `usage:{include:true}` so the REAL `usage.cost` field
  came back on every call (never estimated).
- **Resumability was exercised for real, not merely simulated.** One candidate's response was
  truncated by an initial `max_tokens` budget that proved too small once the model's own reasoning
  tokens were accounted for; after raising the budget, re-running the SAME driver against the SAME
  checkpoint file resumed from exactly where it left off — the other 58 already-checkpointed
  candidates were NOT re-billed, and the API was called again only for the one still-incomplete
  candidate. This is a genuine, observed resumability event in this session (distinct from, and in
  addition to, Task 2's fixture-based demonstration of the same mechanism).
- **Real measured cost: $0.322146 total**, across 72 real API calls (some candidates needed more than
  one call due to the truncation issue above before it was fixed) for 59 residual candidates — **never
  estimated, read directly from each response's own `usage.cost` field.**

### 2.5 Grading result — real agreement against owner labels, both error directions reported separately

Graded via `scripts/discovery_novelty_funnel.py::grade_against_owner_labels` against the full 101-case
label file (restricted internally to `label_provenance.source == "owner_supplied"` entries, per Task
2's hard rule):

| Metric | Value |
|---|---|
| Total cases in label file | 101 |
| Excluded (no owner provenance — the 8 Arm-3 `no_verdict_by_design` rows) | 8 |
| Skipped (3 identity + 9 demotion, owner left blank) | 12 |
| Effective evaluation size (owner-provenance, non-skipped) | 81 |
| — of which **shade** (this re-measurement's scope) | **60** |
| — of which identity (out of scope here — see 2.2) | 5 (0 graded — no predictions attempted) |
| — of which demotion (graded directly from owner verdicts — see 2.6) | 16 |

**Shade-axis result (60 cases, real model + real heuristic predictions vs. owner labels):**

- **Overall agreement: 47/60 = 78.3%.**
- **False-novel direction (predicted `fills_gap` when the owner says it is not): 0.** The
  reputationally expensive direction (decision B) produced zero errors in this sample.
- **False-known direction (owner says `fills_gap`, prediction claims some other recorded shade): 0.**
- **IMPORTANT CAVEAT, stated plainly: this sample contains ZERO true `fills_gap` cases at all** (the
  60-case pool is drawn from Class 6 catalogue-divergence and Arm 1 residual, neither of which
  selects for genuinely-unknown fragments — see `136-GATE1-DECISIONS.md` § J's own Arm-1 finding,
  "ZERO fills_gap in 30"). The 0/0 result on the two-directional novelty-error axis is therefore **not
  evidence of safety on that axis** — it means the axis decision B cares about most was simply never
  exercised by this sample, not that it was exercised and passed.
- **13 "other disagreements"** — real confusion among the EXCLUDED-from-candidacy shades
  (`confirms`/`diverges_work`/`refines_granularity`/`aid_more_specific`/`container_predicts`), where
  the shade choice still matters materially for how a row is DISPLAYED (ruling F's hidden-by-default
  posture applies only to `diverges_work`/`diverges_part`; a `confirms` row shown normally that should
  have been `diverges_work` is shown UNWARNED when the owner's own review found the catalogue is
  usually right in a real divergence; the reverse hides a row that should have displayed normally).

**Per-shade confusion matrix (owner label → predicted label, 60 shade cases):**

| Owner label (n) | Predicted correctly | Confused with |
|---|---|---|
| `confirms` (19) | 14 (73.7%) | `diverges_work` ×3, `aid_more_specific` ×2 |
| `diverges_work` (32) | 28 (87.5%) | `confirms` ×3, `refines_granularity` ×1 |
| `refines_granularity` (4) | 4 (100%) | — |
| `container_predicts` (4) | 1 (25.0%) | `refines_granularity` ×1, `diverges_work` ×2 |
| `aid_more_specific` (1) | 0 (0%, n=1) | `confirms` ×1 |

**No `diverges_part`, `fills_gap`, `alias_merge`, or `extends` owner labels exist anywhere in this
60-case sample** — this measurement says nothing about model accuracy on those four shades; that is
a real, structural limitation of the evaluation-set construction (per `docs/specs/discovery-novelty-v1.md`
§8), not a result to extrapolate past.

**Divergence-correctness sub-question** (asked only where BOTH the owner AND the prediction landed on
a divergence shade, so the two calls are actually comparable): **8/28 = 28.6% agreement** — at or
below the ~33% a 3-way random guess would produce on this vocabulary
(`catalogue_correct`/`claim_correct`/`unclear`). This is the single weakest measured result in this
re-measurement.

**The specific risk this project's own pre-registered RISK CHECK named as most likely
(`alias_merge` vs. `refines_granularity`/`aid_more_specific` confusion, `136-GATE1-DECISIONS.md`
"Outstanding" section) is UNTESTABLE in this sample: zero `alias_merge` labels exist in the 60-case
shade pool** (Class 2's alias cases live only in the separate 8-case `identity` spot-check, a
different question type). **The prediction neither held nor failed — it was never exercised.** The
confusion that DID materialize (`confirms`↔`diverges_work`, and `container_predicts` scattering
across three OTHER shades) is a different pattern than the one the pre-registered risk check
anticipated, and is arguably more concerning precisely because it was not the one anticipated.

### 2.6 Demotion-arm result (16 graded, from the owner's own recorded verdicts — no model involved)

Per ruling J's own design, Arm 2 rows never reach a model — grading here tallies the owner's already-
recorded `demotion_correct`/`false_known`/`unsure` calls directly (this is unchanged from, not a new
measurement beyond, `136-GATE1-DECISIONS.md` Task 4's own analysis): 5 `demotion_correct`, 2
`false_known`, 9 `unsure` — the same instrument-design limitation already documented there (the sheet
did not surface the actual demoting source text to the labeller) applies unchanged; this re-measurement
adds no new information on this arm beyond re-confirming the label file's own recorded counts via the
grading harness.

### 2.7 Real cost — measured, not estimated

**Total real spend for this re-measurement: $0.322146**, read from `usage.cost` on every one of 72
real OpenRouter calls (59 residual candidates, some requiring more than one call due to the
`max_tokens` truncation issue in §2.4). **The `~$27` figure is not cited here as an accuracy or a cost
result for this measurement — it never applied to this scope in the first place.**

## 3. Production run — NOT executed. Halted at the ruling-I gate, per this plan's own instruction

### 3.1 The gate, applied honestly

Comparing section 2's real, measured result against the prior validated level (`136-GATE1-DECISIONS.md`
§ B: 40/40 = 100% agreement between the pinned config and a fuller-thinking reference config, itself
independently validated at 99% against 103 human grades — on the OLD five-way vocabulary and the OLD
one-title-string contract):

- **78.3% overall shade agreement** (measured against REAL owner labels, the more directly comparable
  figure to the prior 99% human-grounded number, not the 100% model-vs-model figure) is a
  **20+ percentage point drop** from that benchmark.
- **28.6% divergence-correctness agreement** is at or below chance for a 3-way vocabulary — the
  weakest single result in this measurement.
- **`container_predicts` (ruling H's own newest shade) scored only 25% (1/4)** — the shade with the
  smallest sample in this pool also has the worst measured accuracy.
- **A real mechanical false-known was observed** (case 17) — not a fixture, a genuine instance of the
  exact PERMANENT, UNRECOVERABLE risk ruling J's own text names, on real data.

**This is judged a MATERIAL REGRESSION, not noise from a harder task.** The task genuinely is harder
(ten values vs. five; free-text reading vs. one title string) and some drop from a prior 99-100%
figure would be expected and tolerable — but a drop to 78.3% overall, with a correctness sub-question
performing at chance and the newest shade performing at 25%, is a real, substantial, measured
degradation that this executor is not authorized to wave through. **Per this plan's own instruction
and the dispatch that authorized this session: "A material regression is the owner's decision to
accept, not yours." This session HALTS here rather than proceeding to the production run.**

### 3.2 Cost and scale re-derivation — real, free, zero-model-call (per ruling J's own outstanding note)

Ruling J's own text states the `~$27` estimate "must be re-derived... against the ACTUAL residual
size once the heuristic funnel runs for real." That re-derivation does NOT require spending on the
model arm — the heuristic pass itself is free and mechanical. It was run for real, over the FULL live
identification set (every distinct `(sys_id, work_id)` pair among `shipped` claims in the live
discovery-v1 asset — **65,200 pairs**, of any `claim_type`):

| Outcome | Count | % of total |
|---|---|---|
| Resolved mechanically, `confirms` (real textual name-match) | 1,689 | 2.6% |
| Resolved mechanically, `fills_gap` (Arm 3 — no checked-source text at all) | 8,327 | 12.8% |
| **RESIDUAL — would require a real model call** | **55,184** | **84.6%** |

**Re-derived cost projection:** this session's real measured rate ($0.322146 ÷ 59 residual candidates
actually billed = **$0.00546/candidate**, including retry overhead) projected linearly across the
real 55,184-candidate residual gives **≈$301** — roughly **11× the stale `~$27` estimate**, because
(a) the ten-value/free-text prompt is materially larger and costs more in reasoning tokens per call
than the five-way/one-title-string prompt the `~$27` figure was based on, and (b) even after the
funnel-first architecture removes 15.4% of the corpus for free, the residual is still the large
majority (84.6%) of all identifications, not a small tail.

**Runtime re-derivation, reported for completeness.** This session's model calls ran sequentially
(no concurrency), consistent with `run_model_arm`'s own one-at-a-time contract. At the observed
per-call latency, a naive serial run across the full 55,184-candidate residual would take on the
order of ONE TO TWO DAYS of continuous, uninterrupted API traffic — not a few minutes. A responsible
production run would need a deliberately concurrent/batched implementation (with its own rate-limit
and cost-monitoring design) that this session did not build, because building it before the accuracy
gate is resolved would risk spending real money and real time on a run whose underlying verdicts are
not yet known to be trustworthy at production scale.

### 3.3 What this means, stated plainly

The production run's non-execution here is **not** the same finding as section 0's environment gap.
Every precondition Task 3 lists (label file, real sidecars, real credentials) was present and used.
The block is now a **measured, informed, owner-facing decision gate**: proceed anyway and accept the
measured regression (perhaps scoping the shipped `divergence_correctness` field down, or suppressing
`container_predicts` pending a harder prompt, or accepting the residual/cost numbers as the real price
of this feature); invest in prompt/heuristic hardening first and re-measure; or some other owner
call this session is not authorized to make unilaterally. **No claim in the shipped asset has changed
as a result of this session** — every identification remains `not_checked` (or whatever it already
was), exactly as before, per NOVEL-01's fail-closed coverage-gap framing.

## 4. The `fills_gap` probe (owner ruling K) — EXECUTED 2026-08-03, real corpus, real model, real cost

**Provenance.** Owner ruling K (`136-GATE1-DECISIONS.md` § K) keeps the ~$301 production run
UNAUTHORIZED until a purpose-built probe measures the false-novel rate on the population that would
ACTUALLY ship as "Candidates for new finds" — the ruling-I re-measurement above (§ 2) scored ZERO
false-novel errors on 60 cases, but the pool contained ZERO true `fills_gap` cases, so that axis was
never exercised. This section reports that probe, built and run in the same directly-dispatched
session as rulings K and L, on the primary checkout, against the real sidecars and the real pinned
model (post-ruling-L: the `divergence_correctness` sub-question is no longer part of the model's
output contract — see § 2.5 above's now-superseded correctness figure and `136-GATE1-DECISIONS.md`
§ L).

### 4.1 The probe's design — both paths to candidacy, real data throughout

Per `136-GATE1-DECISIONS.md` § K, the probe covers BOTH ways a row can become a "Candidate for new
finds":

1. **The model path** — real residual rows (the mechanical heuristic funnel could not resolve them)
   that the REAL pinned gate (`gemini-3.6-flash`, `reasoning.effort=low`, called for real via
   OpenRouter, post-ruling-L prompt) classifies `fills_gap`.
2. **The bypass path** — rows where NO checked source has any text at all. These NEVER reach the
   model at all; the funnel ships them as `fills_gap` automatically, with nothing checked against
   them (ruling J's own Arm 3 design, which sampled only 8 of these with no verdict collected).

New committed script: `scripts/discovery_novelty_probe.py`. It reuses the REAL, committed funnel
(`scripts/discovery_novelty_funnel.py`'s `NoveltyCandidate`/`run_heuristic_pass`/
`run_heuristic_funnel`/`run_model_arm`/`assemble_evidence_bundle`) and the REAL data loaders already
committed for the ruling-J hard-case sampler (`scripts/discovery_gate1_evidence.py`'s
`load_works`/`load_claims`/`load_libraries_csv`/`load_fjms_catalog_text`/`load_bib_rows`/
`load_fgp_rows`/`_combined_catalogue_text`) — never a second, hand-copied loader. A new PGP-text
loader (`load_pgp_texts`, reading REAL `description`/`transcription` text from `pgp_data/pgp.db`,
joined through `document_fragments` on `sys_id`) was added because the existing
`discovery_gate1_evidence.py::load_pgp_signal_index` returns only presence/named booleans for its
own SAMPLING-ONLY approximation — this probe needed the real text to feed the real committed funnel.

**Real, independent validation of the reconstruction.** Before sampling anything, the script builds
a `NoveltyCandidate` for EVERY real shipped `(sys_id, work_id)` pair in the live asset (across ALL
`claim_type`s, not just `direct_witness` — this is the population definition that reproduces the
real 65,200-pair figure § 3.2 above reports) and runs the real committed heuristic pass over the
FULL population (free, mechanical, zero model calls). **Result: 65,200 total candidates → 1,689
resolved `confirms` mechanically, 8,327 no-source-text bypass, 55,184 residual** — an EXACT,
byte-for-byte match to the ruling-I re-measurement session's own full-corpus heuristic pass (§ 3.2
above), independently re-derived from scratch in this session rather than assumed. This is strong
evidence the reconstruction (real sidecars, real committed funnel, real population definition) is
faithful.

### 4.2 Sampling — sized for low single-digit dollars, capped at ~40 for the owner

- **Model-path sample: 300** real residual candidates, drawn by seeded random sample (`seed=20260803`,
  `random.Random(seed).sample(...)` over the deterministic `(sys_id, work_id)`-sorted population — a
  reproducible draw, not a hand-picked one) from the real 55,184-candidate residual. Sized so the real
  model-call cost stays in the low single-digit dollars the task's own guidance allowed (the ruling-I
  re-measurement was $0.32 for 59 calls) while giving a broad, UNSTRATIFIED read across the residual —
  deliberately different from the ruling-J hard-case pool's Arm 1 (which is capped per-stratum at 5 and
  therefore cannot answer a rate question at all).
- **Bypass-path sample: 20** real no-source-text candidates, drawn the same way (`seed=20260804`) from
  the real 8,327-candidate bypass population. Every one of these rows is, by construction, ALREADY a
  shipped candidate — no filtering happens to this path at all, so a plain random sample directly
  exercises the risk ruling K's own text calls out: "arguably the HIGHER risk precisely because nothing
  examines it."
- **Split rationale (owner's time is the scarce resource, cap ~40 total):** the model path's final
  instrument size is NOT the 300-candidate sample itself — it is capped to however many of those 300
  the model actually classified `fills_gap` (bounded separately at 20 if that number were larger; it
  was not, in this run — see § 4.3). The bypass path gets a flat, generous 20-case sample because EVERY
  bypass row is already a candidate by definition (no model filtering reduces it), so 20 is the
  direct, unfiltered read on that path; the model path's 300-candidate INPUT sample is much larger
  than 20 because most residual candidates are NOT classified `fills_gap` (see § 4.3's `diverges_work`-
  heavy finding in the ruling-J hard-case pool), so a much larger input sample was needed to surface
  enough true "would-ship" rows to make the model path's rate meaningful at all.

### 4.3 Real results — measured, not estimated

- **Real total spend: $1.029420** across **305 real OpenRouter calls** (300 from this run + 5 from an
  initial small integration-validation batch under a different seed, both logged to the same real cost
  log and summed together here — real spend on this probe project, in total). Read directly from each
  response's own `usage.cost` field, never estimated. **Average $0.00338/call — cheaper than the
  ruling-I re-measurement's $0.00546/call** (this prompt no longer elicits `divergence_correctness`,
  per ruling L, which plausibly shortens the response). Every response's own `"model"` field echoed
  `google/gemini-3.6-flash` — no silent downgrade observed across any of the 305 calls.
- **Of the 300 model-path candidates, 13 were classified `fills_gap`** (would ship as a "Candidate for
  new finds" in production) — **4.3%** of this UNSTRATIFIED sample. This is a materially different,
  and more informative, number than the ruling-J hard-case pool's Arm 1 (0 of 30, but that pool is
  capped at 5 per source-stratum, not a random draw, and explicitly does not answer a rate question
  per its own sizing note). No subsampling/cap was needed on the model path (13 < the 20-case cap).
- **20 bypass-path candidates** — a plain random sample of the real 8,327-row no-source-text
  population; no model call, no filtering.
- **Total instrument: 33 cases** (13 model-path + 20 bypass-path) — under the ~40-case cap.
- **Every row is a REAL manuscript, a REAL claimed work, and a REAL selection reason** — no fabricated
  case. Examples from the model path (case titles only, per this record's own no-raw-provenance-text
  discipline): a Saadia Gaon Daniel translation (Alliance Israélite Universelle Ms. VII A), a Bible
  Isaiah fragment (NLR Box D.221), a Maimonides Mishneh Torah book (MS heb. d.67/49), a Rashi Torah
  commentary (Adler Ms. 2869.20). Examples from the bypass path: several Bible-book fragments (Genesis,
  Numbers, Jeremiah, Daniel, Isaiah, Joshua), a Targum Onkelos fragment, a Maimonides Mishneh Torah
  book, a weekday Amidah fragment — several of which are plausibly well-known Biblical/liturgical texts
  that a human labeller may judge `actually_recorded` in the ordinary sense even though no CHECKED
  source in this project's own enumerable set happens to have text for this specific manuscript; this
  is exactly the kind of case the probe exists to surface.

### 4.4 The owner-labelling instrument

**Deliverables:** `136-NOVELTY-FILLSGAP-PROBE.md` (this phase directory) + `136-NOVELTY-FILLSGAP-PROBE.xlsx`
(same RTL/data-validation house style as `136-NOVELTY-HARDCASES.xlsx`, one "Candidates" sheet + one
"Vocabulary & Instructions" sheet). 33 rows, each carrying: case #, an EMPTY verdict cell (never
pre-filled — verified: every verdict cell in the saved workbook is `None`), the path (`model` /
`bypass`), the manuscript/shelfmark, `sys_id`, the claimed work, and a plain-language reason stating
WHY the row is in the set (its selection path — never a shade guess or a pre-filled answer).

**The single owner question, identical for every row regardless of path** (per this task's own
plain-English instruction): *"Is this fragment GENUINELY NOT IDENTIFIED in the finding aids we
checked — catalogue, bibliography, PGP, FGP, and (where present) an internal reference-corpus
shelfmark attribution?"* Vocabulary: `genuinely_novel` / `actually_recorded` / `unsure` / `skip`
(XLSX dropdown, free text rejected; a blank cell means "not yet answered", never a label).

**Masking:** both the MD and the XLSX (outer bytes AND the decompressed inner XML, per this project's
own standing methodology for `.xlsx` masking verification — a bare outer-byte scan cannot see
DEFLATE-compressed inner content) pass `scripts/check_atlas_masking.py --scan-asset` clean.
`MASKING_SCAN_PATTERNS_FILE` pointed at the real, gitignored `.masking_patterns` file at the repo
root (unset makes the scan fail closed by design — never a silent false-green).

### 4.5 What this number can and cannot support — stated plainly, per this task's own instruction

- **A 33-case sample gives a COARSE per-path rate, not a tight one.** Do not treat 13/300 = 4.3%
  (model path) or any fraction the owner's eventual labelling produces on the 20 bypass-path rows as a
  precise, publishable corpus-wide false-novel rate. Neither the model-path input sample (300, random
  but unstratified) nor the bypass-path sample (20, a small slice of 8,327) is sized for a tight
  confidence interval — this mirrors the ruling-J hard-case pool's own "coarse, not tight" framing for
  its Arm 1/Arm 2 samples.
- **What it CAN support:** a real, owner-labelled read on whether EITHER path is producing
  disproportionately false "Candidates for new finds" — the specific, concrete question ruling K asks,
  which the ruling-I re-measurement's 60-case pool could not answer at all (zero true `fills_gap`
  cases in that pool). If the owner's labelling finds the model path's 13 rows are mostly
  `genuinely_novel`, that is real, if coarse, evidence the false-novel risk on THAT path is low. If the
  bypass path's 20 rows are disproportionately `actually_recorded` (plausible for common Biblical/
  liturgical texts, per § 4.3's examples), that is a real, concrete finding about the SPECIFIC risk
  ruling K named as the higher-risk, unexamined path — actionable independent of the model-path result.
- **What it CANNOT support:** a base rate for either path across the full corpus (55,184 residual /
  8,327 bypass); a per-source-family breakdown of WHERE false novelty concentrates on the model path
  (this sample is unstratified, unlike the ruling-J Arm 1 design); a statistically rigorous confidence
  interval on either path's rate; or a decision to authorize the ~$301 production run BY ITSELF — per
  ruling K, that decision remains the owner's, informed by (not automatically settled by) this probe's
  eventual labelled result.
- **The ~$301 production run remains UNAUTHORIZED.** It was NOT executed by this session (explicitly
  out of scope per ruling K and this task's own `<do_not>` instruction). This probe's real spend
  ($1.03) is entirely separate from, and far smaller than, that unauthorized figure.

## 5. What WAS built, exercised and verified (Tasks 1–2, unchanged by this session)

Although a full production run was not executed, the funnel's actual MECHANISM was built, and its
riskiest properties were exercised against fixtures at zero cost and with zero model calls (Task 1/2,
unaffected by this session — restated here for continuity, not re-verified):

- **The heuristic (mechanical) funnel pass** (`scripts/discovery_novelty_funnel.py::run_heuristic_pass`)
  was exercised against fixtures covering every named Codex-finding defect this plan was built to
  avoid, and — as section 2.4 above shows — its real behavior on real data reproduces exactly the
  mechanical-false-known risk the fixtures predicted.
- **The checkpointed, resumable model-arm mechanism** (`run_model_arm`) was exercised with a fixture
  simulating a mid-run crash, AND — new in this session — exercised for real (section 2.4).
- **The grading harness's three hard rules** were exercised end to end, including the mutation-tested
  denominator guard (Task 2's own account, unchanged).
- **`load_owner_labels`'s content-hash verification** was exercised against a synthetic file in Task 2,
  AND — new in this session — against the real label file, where it correctly verified a match
  (section 2.1).

All of the above remains committed as automated tests (`tests/test_discovery_novelty_contract.py`, 93
passing, unaffected by this session).

## 6. Path to completion — what happens next

This is no longer "what a future execution must do to even attempt the re-measurement" (section 0's
framing) — that has been done. What remains:

1. **The owner reviews section 3's measured regression and re-derived cost/scale**, and decides one
   of: (a) accept the current accuracy and cost, and authorize the production run as originally
   scoped; (b) accept a NARROWER production run (e.g., ship `novelty_status` but suppress or
   low-confidence-flag `divergence_correctness` and/or `container_predicts` pending hardening); (c)
   authorize a hardening pass on the prompt/heuristic (specifically targeting the `confirms`↔
   `diverges_work` boundary and the `container_predicts` shade) and a fresh re-measurement before any
   production spend; or (d) some other call only the owner can make.
2. **Whichever option is chosen, a concurrent/batched production-run implementation should be built**
   before attempting the full 55,184-candidate residual — the sequential `run_model_arm` driver used
   for this 59-candidate re-measurement is adequate for a small evaluation set but not for a
   corpus-scale run at the re-derived ~1–2 day serial runtime.
3. **136-12's build wiring remains ready to consume real verdicts once they exist** — the contract
   module, the funnel runner and the grading harness are all built, tested, AND now exercised against
   real data at small scale; only the corpus-scale production run (gated on the above) remains
   outstanding.

## 7. Compliance checks for this record

- No prompt text, raw model response, or raw provenance value appears anywhere in this document. Case
  examples are cited by case number, shade token, and English shelfmark/manuscript identifier only —
  never by the raw Hebrew claim/catalogue text this session's driver scripts read (those scripts and
  their intermediate JSON outputs live entirely outside the repository, in the session's own scratch
  directory, and were never staged).
- No verdict-cache file, and no model checkpoint file, is staged in this repository —
  `discovery_data/novelty_remeasurement_checkpoint.jsonl` is gitignored (`git check-ignore` confirms
  it) and `git status` shows nothing under `discovery_data/` tracked by this session.
- This document does not name any restricted corpus — every checked source is referred to generically
  (catalogue / bibliography / PGP / FGP / the internal shelfmark-attribution source), consistent with
  D-25/NOVEL-02 and the project's own codename discipline.
- The `~$27` figure is cited above only as a superseded prior estimate, explicitly re-derived (section
  3.2) rather than repeated as if still current, and is never presented as an accuracy result for any
  vocabulary.
- `python scripts/check_atlas_masking.py --scan-asset` was re-run against this document after every
  edit in this session and exits 0.

**Additions for the § 4 probe (this later continuation, 2026-08-03):**

- Section 4 above cites manuscripts and claimed works by case number, English shelfmark, and an
  ENGLISH paraphrase of the claimed work (e.g. "a Saadia Gaon Daniel translation") only — never a
  literal Hebrew claim/catalogue string, consistent with this document's own standing discipline
  (unlike `136-NOVELTY-FILLSGAP-PROBE.md`/`.xlsx` themselves, which — like `136-NOVELTY-HARDCASES.md`
  before them — render real, already-public `works.neutral_title`/`author` strings, the SAME
  DATA-04-cleared values the live product already displays; never a raw M-source/restricted-corpus
  identifier or reference text).
- `scripts/discovery_novelty_probe.py` (the new committed script) has NO import-time dependency on any
  gitignored research tree, matches `scripts/discovery_gate1_evidence.py`'s own "read-only measurement
  script" shape, and is ruff-clean.
- `discovery_data/novelty_probe_model_checkpoint.jsonl` and `discovery_data/novelty_probe_cost_log.jsonl`
  (this session's real, per-call cost log and model-verdict checkpoint) are gitignored
  (`git check-ignore -v` confirms both against the blanket `/discovery_data/` rule) and were never
  staged.
- Both `136-NOVELTY-FILLSGAP-PROBE.md` and `136-NOVELTY-FILLSGAP-PROBE.xlsx` passed
  `scripts/check_atlas_masking.py --scan-asset` clean — the XLSX scanned BOTH as outer bytes AND via
  `zipfile` extraction of its decompressed inner XML into a scratch directory (this project's own
  standing methodology for `.xlsx` masking verification, since a bare outer-byte scan cannot see
  DEFLATE-compressed inner content). `MASKING_SCAN_PATTERNS_FILE` pointed at the real,
  gitignored `.masking_patterns` file at the repo root throughout — never unset, never a silent
  false-green.
- The real, independent full-corpus heuristic-pass re-derivation (§ 4.1: 65,200 → 1,689/8,327/55,184)
  reproduced the ruling-I re-measurement session's own figures (§ 3.2) EXACTLY, byte-for-byte on every
  count — cited here as a durable cross-session reproducibility record, not merely asserted.
