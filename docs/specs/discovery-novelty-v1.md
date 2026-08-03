# Discovery Novelty Axis — Contract v1 (NOVEL-01/02, Phase 136 plan 136-04)

**Status:** versioned contract for the novelty flag computed per `(sys_id, work)`. This is the
CANONICAL, single-cited restatement of the ten-value shade enum going forward — every other
document (`.planning/ROADMAP.md`, `.planning/REQUIREMENTS.md`, `136-12-PLAN.md`, the panel/findings
plans) cites THIS file or `136-GATE1-DECISIONS.md` directly rather than re-deriving the list a third
time. The one place a second literal restatement is structurally unavoidable is the SQL `CHECK`
constraint in `docs/specs/discovery-sidecar-schema-v1.md` (SQLite `CHECK` constraints cannot import a
shared constant) — `shared/discovery_novelty.py::NOVELTY_STATUSES` is the tie-breaker if the two ever
disagree, and `tests/test_discovery_novelty_contract.py` asserts that equality directly.

Implemented by `shared/discovery_novelty.py`. Owner rulings: `136-GATE1-DECISIONS.md` sections E, E′,
F, G, H, I, J. Prior-art reconciliation: `136-NOVELTY-PRIOR-ART.md`.

---

## 1. Why this exists

Today all 144,294 shipped `track1_direct` evidence rows carry `is_new = 0`, which means UNCHECKED,
not "already recorded" — a coverage gap the frozen v2 asset left open (it only ever computed novelty
for `propagated` rows). A two-state filter over that data would tell a reader that 144,294 findings
are already in the finding aids — a false claim on the flagship surface. This axis computes, per
`(sys_id, work)`, whether ANY available finding aid already ties THAT fragment to THAT work — and,
now, exactly WHAT KIND of relationship it found, because a boolean or a tri-state was measured to
collapse qualitatively different findings into the same bucket (see `shared/discovery_novelty.py`'s
own module docstring for the full rationale and worked cases).

## 2. The ten-value shade enum (canonical statement)

`novelty_status` is a TEN-VALUE closed vocabulary, defaulting to `not_checked` (fail-closed,
unchanged in meaning across every widening step):

| # | Shade | Condition | Candidate toggle | Default visibility |
|---|---|---|---|---|
| 1 | `confirms` | an aid already ties fragment F to work W (including via rule 3 below, and including a known alias of W) | excluded | shown normally |
| 2 | `refines_granularity` | OUR claim is MORE SPECIFIC than what any aid says (same-author/title-prefix relationship, D-13d) | excluded | shown normally |
| 3 | `aid_more_specific` | an AID's identification is MORE SPECIFIC than ours — we add nothing; the aid already knew more (owner correction E′; the LEAST novel shade) | excluded | shown normally |
| 4 | `diverges_work` | an aid names a genuinely DIFFERENT work (not a granularity variant) | excluded | **hidden by default** (ruling F) |
| 5 | `diverges_part` | an aid names a different or finer PART of the SAME work | excluded | **hidden by default** (ruling F) |
| 6 | `container_predicts` | an aid names a broader rite/cycle/ceremony/container whose standard, predictable content includes this unit, without naming it (ruling H) | excluded | shown normally — **not** hidden (ruling H is explicit that F's rationale does not apply here) |
| 7 | `fills_gap` | none of the checked sources identify this fragment as anything at all — the genuine "previously unknown" case | **SOLE selecting value** | shown normally |
| 8 | `extends` | aids tie OTHER folios of the SAME manuscript to W, but not this folio | excluded | shown normally |
| 9 | `alias_merge` | the two work-ids shown ARE the same underlying work, not yet canonically merged (Class 2's situation) | excluded | shown normally |
| 10 | `not_checked` | fail-closed default: unrun, failed, source unavailable, identifier does not normalize, snapshot incomplete, cache stale, or the model abstained | excluded | shown normally (as "not yet checked", never as a candidate) |

The public **"Candidates for new finds"** toggle selects `novelty_status = 'fills_gap'` and NOTHING
else — this has been true, unchanged in shape, since decision E and confirmed unchanged by every
later widening (E′, F, H).

`divergence_correctness` is a SEPARATE, sibling column — `catalogue_correct` / `claim_correct` /
`unclear` — populated IF AND ONLY IF `novelty_status IN ('diverges_work', 'diverges_part')`, `NULL`
for every other shade. It is orthogonal to the shade because the owner's own review of real
divergence cases found BOTH directions (catalogue right / claim right) occur under the identical
shade token — one column cannot carry both meanings (ruling F).

**⟨AMENDED 2026-08-03, owner ruling L — `136-GATE1-DECISIONS.md` § L⟩ `divergence_correctness` is now
a HUMAN/OWNER ANNOTATION ONLY — the model never computes it.** The ruling-I re-measurement
(`136-NOVELTY-RUN.md` § 2.5) scored this axis at 8/28 (28.6%) — at or below chance for a three-way
vocabulary — on cases where the owner's own review of the identical cases scored 31/32. Ruling F's
default-hidden/explicit-warned-toggle posture for `diverges_work`/`diverges_part` rows applies
REGARDLESS of which side is right, so no shipped surface has ever needed the model's correctness
call to decide anything. The STORED column, its CHECK constraint, and every owner-supplied value
already collected are UNCHANGED — nothing is deleted. Only the model's OUTPUT CONTRACT changes: the
pinned prompt (section 5 below) no longer asks for this field at all, and
`shared/discovery_novelty.py::resolve_model_output` now ALWAYS returns `divergence_correctness: None`,
structurally incapable of surfacing a model-supplied value. A future human/owner annotation pass
(not the model arm) is the sole remaining path that may populate this column.

`novelty_source_label` populates on every shade where SOME finding aid says something nameable about
this fragment-work pair: `confirms` / `refines_granularity` / `aid_more_specific` / `alias_merge` /
`extends` / `diverges_work` / `diverges_part` / `container_predicts`. It stays `NULL` on `fills_gap`
(nothing to name) and `not_checked` (nothing was checked).

Implemented as `shared/discovery_novelty.py::NOVELTY_STATUSES` / `DEFAULT_STATUS` /
`CANDIDATE_STATUS` / `HIDDEN_BY_DEFAULT_SHADES` / `DIVERGENCE_SHADES` /
`DIVERGENCE_CORRECTNESS_VALUES` / `SOURCE_LABEL_ELIGIBLE_SHADES` / `novelty_columns_for`.

## 3. The checked-source set — promised vs. implemented (per `136-NOVELTY-PRIOR-ART.md` section 2)

NOVEL-01 promises an enumerable, versioned checked-source set: FJMS and NLI catalogue +
bibliography, titles, PGP, FGP, and M-source shelfmark attributions. Their implementation status,
honestly stated (this table exists specifically so a future reader never has to re-derive that
`catalog_refs` measured zero matches, or that `published_full` over-demotes):

| Source | Status | Detail |
|---|---|---|
| FJMS catalogue (structured + free text) | **Implemented-correct, if the FREE TEXT field is read** | Read `catalog.TitleHeb` / `GenizahTitleOrgTitle` — the catalogue's OWN identification prose. NEVER `catalog_refs` (FJMS catalogue-VOLUME/entry-number field) — Codex measured this at **zero** matched known pairs; it names catalogue volumes and entry numbers, not the entry's own manuscript identification. |
| FJMS bibliography (Friedberg) | **Implemented, but non-decisive alone** | A bibliography row's mere PRESENCE — including `TranscriptionType = published_full` — is NOT evidence it names THIS specific work (Codex finding 1: 3,688 known pairs affected, 3,060 sole-source). Its text must be READ and textually matched, never merely counted as present. |
| Titles (public `libraries.csv` column 7 / "NLI title") | **Implemented** | The public catalogue-identification field every prior novelty artifact (the five-way LLM gate, this project's own hard-case selectors) has actually read. |
| PGP | **Implemented, but non-decisive alone** | A PGP description or transcription's mere PRESENCE is NOT evidence it names this specific work (Codex finding: 2,014 known pairs affected, 942 sole-source). Must be textually matched, never merely counted as present. |
| FGP | **Implemented, functioning as designed, weak in practice** | A purely mechanical name-match under-connects FGP text (Codex measured 1,177 known vs. 9,373 studied/match-failed) — this is not a bug in reading FGP; it reflects that mechanical string matching alone misses a lot of genuinely-present FGP identifications, which is exactly the gap the model residual exists to close. |
| M-source shelfmark attributions | **Not previously implemented anywhere** | Zero prior engineering exists to reconcile against (`136-NOVELTY-PRIOR-ART.md` section 2). Treated as an ordinary checked source in the funnel and the LLM prompt (tagged `m_source_shelfmark`), with its corpus name masked on every code path per section 6 below — never named, even as "the M-source" (the project's own codename rule). |

## 4. Funnel-first architecture (ruling J) — two pipeline stages, never one uniform pass

1. **Mechanical heuristic pass** (`scripts/discovery_novelty_funnel.py`) runs over EVERY
   identification. It reads each checked source's own FREE TEXT (never merely a structured-id join),
   tagged explicitly by provenance (catalogue / bibliography / pgp / fgp / m_source_shelfmark — never
   flattened or stripped of provenance). It can only ever produce TWO outcomes:
   - a genuine textual name-match (the claimed work's title, or a known alias, actually appears in
     some source's free text under a normalized/looser reading) → resolves to `confirms`;
   - no such match anywhere → **UNRESOLVED**, becomes part of the residual.

   It NEVER itself decides `diverges_work` / `diverges_part` / `refines_granularity` /
   `aid_more_specific` / `alias_merge` / `container_predicts` / `extends` — those require judgment
   beyond mechanical string matching and are the model's job alone. Mere SOURCE PRESENCE (a
   bibliography row exists; a PGP description exists) is deliberately never sufficient by itself to
   resolve a row — this is the fix for Codex findings 1 and 6 (`136-NOVELTY-PRIOR-ART.md` section 6):
   the prior reference implementation (`gen2_novelty_gate.py`) treated `published_full` presence and
   bare PGP presence as decisive "known" signals, over-demoting real findings.

   **Special case — no checked-source text at all (Arm 3).** When a candidate has NO text from ANY
   checked source, it ships as a novelty candidate (`fills_gap`) AUTOMATICALLY, with no model call —
   by definition, if literally nothing any checked source says exists for this fragment, no aid can
   possibly name the work, so `fills_gap` is correct without needing a model judgment (ruling J's own
   accounting for "Arm 3 — no-source-text").

   **Special case — unmapped page→sys_id join.** The page→sys_id join was measured clean in the
   reference implementation (198,238 distinct shipped pages, zero missing/null/conflicting mappings —
   Codex finding 4) — but a FUTURE mapping failure must never disappear silently. An unmapped page
   routes explicitly to `not_checked`, with a logged, counted reason — never a silent `.get()`-style
   drop.

2. **The pinned LLM gate** (section 5 below) runs ONLY over the RESIDUAL — rows the mechanical pass
   left unresolved. A row the mechanical pass resolves, in EITHER direction (a genuine name-match, or
   the Arm-3/unmapped-page special cases), NEVER reaches the model.

**The real, measured cost of this architecture (ruling J), stated explicitly, not assumed away:** the
funnel only ever DEMOTES (discovery → known, never the reverse — this has been the funnel's stated
design principle since the original `discovery_identified_gate.py`). Under a funnel-first design, a
MECHANICAL false-known — a row wrongly resolved to `confirms` because a textual match fired when it
should not have — is now PERMANENT and UNRECOVERABLE: nothing downstream ever re-examines a
mechanically-resolved row; no model verdict is ever computed for it. The error runs in the
CONSERVATIVE direction (a real finding is silently lost, never that a fake one is manufactured and
published) — the correct direction to be wrong given this milestone's publication posture — but it is
a real, measured cost, not a free one, and must be reported as such wherever this axis's behavior is
described (see `136-GATE1-DECISIONS.md` section J for the full accounting, including the specific
Codex-measured populations: 3,688 `published_full` false-knowns and 2,014 PGP false-knowns, 942
sole-source, in the REFERENCE implementation this funnel replaces).

**NOVEL-01's honesty wording (ruling J item 3):** a row resolved by the mechanical pass alone carries
NO model verdict — its stored `novelty_status` reflects a MECHANICAL judgment only, never the model's.
Any future documentation or UI copy describing how a `novelty_status` value was determined must
account for this: some rows are funnel-only (never modeled), others are funnel-then-model (the
residual).

## 5. The pinned LLM contract (ruling B, ruling I)

- **Model:** `gemini-3.6-flash` (`shared/discovery_novelty.py::LLM_MODEL`).
- **Model version pin:** `shared/discovery_novelty.py::LLM_MODEL_VERSION` — kept as its own literal
  (not merely an alias of `LLM_MODEL`) so a run record independently confirms the provider's response
  fields against this pin, catching a silent provider-side default-snapshot upgrade.
- **Reasoning effort:** `low` (`LLM_REASONING_EFFORT`) — per ruling B, **do NOT downgrade the model**.
- **Prompt hash:** `PROMPT_SHA256`, computed at import time from the literal
  `NOVELTY_PROMPT_TEMPLATE` string in `shared/discovery_novelty.py` — self-updating; the hash can
  never silently drift from what it claims to hash, because nothing hand-copies a hex digest here.
  The prompt presents the aid's FULL free-text identification (never merely a structured work-id join
  result) to the judgment step, states ruling G's rule directly ("if the aid's own prose already
  names this identification under any spelling/phrasing, the answer is `confirms`, even if the aid's
  structured field points elsewhere"), and is able to recognise and elicit the container-predicts
  relationship (ruling H). **⟨AMENDED 2026-08-03, owner ruling L⟩** the prompt's response contract is
  now the ten-value `novelty_status` shade ALONE — it no longer asks for `divergence_correctness` at
  all (measured at 8/28, at or below chance; ruling F's hidden-by-default posture for
  `diverges_work`/`diverges_part` applies regardless of which side is right, so no shipped surface
  ever needed this call from the model). `PROMPT_SHA256` changed again on this account — the OLD,
  now-retired hash from the ruling-I re-measurement
  (`441058ae3bab6e5ee17beb0fc5ea39426d7c250feb6c2bd288f0bc1605c98be5`) must never be cited as current;
  see `136-GATE1-DECISIONS.md` section L for the new value on record.
- **Input normalization:** `INPUT_NORMALIZATION_SHA256`, computed from the literal
  `INPUT_NORMALIZATION_SPEC` string — NFC normalize, strip nikud
  (`shared.text_normalize.strip_nikud`), strip combining diacritics/quote variants
  (`shared.text_normalize.strip_search_diacritics`), collapse whitespace
  (`shared/discovery_novelty.py::normalize_free_text`).
- **Cache key:** built over `CACHE_KEY_FIELDS`, IN THAT FIXED ORDER — the pinned
  model/version/effort/prompt-hash/input-normalization-hash identifiers FIRST (so a change to any of
  them invalidates every existing cache entry), then `sys_id`, `ref_work_id`, and the NORMALIZED
  claimed title/author and per-source free text, in that order
  (`shared/discovery_novelty.py::build_cache_key`). A cache hit therefore provably means the identical
  question was already asked and answered.
- **Structured abstention:** the model may respond `{"abstain": true, "reason": "..."}` instead of
  guessing; this maps to `not_checked`, never a fabricated shade
  (`shared/discovery_novelty.py::resolve_model_output`). An abstention is a real and useful answer,
  never penalized.

**Ruling I — the pinned config's prior validation does NOT extend to this contract.** The pinned
config (`gemini-3.6-flash`, effort `low`) has a real, MEASURED accuracy result — 40/40 verdict
agreement against a fuller-thinking `gemini-3.5-flash` reference config, itself independently
validated at 99% against 103 human grades (`reference_discovery_llm_gate_cost` memory;
`136-GATE1-DECISIONS.md` section B). **That result was measured on the FIVE-way vocabulary
(`known`/`witness`/`discovery`/`different`/`uncertain`) and the ORIGINAL one-title-string input
contract (comparing a claimed title against ONE catalogue title string) — it does NOT extend to this
document's ten-value shade enum or to this document's free-text input contract (reading every
checked source's own free text, per ruling G), both of which the pinned config has never been
measured against.** The `~$27` figure carried in the same record is a SEPARATE, COST estimate
derived by size-extrapolation — it is NEVER accuracy evidence for this widened task, and must never
be cited as such. Plan 136-04 Task 3 re-measures the pinned config against the owner-labelled
evaluation set on THIS document's current vocabulary/contract BEFORE the production run is
authorized — see `136-NOVELTY-RUN.md` for that re-measurement's own report (or its documented
deferral, if the environment executing Task 3 could not perform it).

## 6. Masking (D-25 / NOVEL-02)

`shared/discovery_novelty.py::masked_provenance_label(source_code, lang)` names the source where it
is nameable ("recorded in the catalogue", "recorded in the bibliography", "recorded in the Princeton
Geniza Project", "recorded in a scholarly transcription", "recorded in the NLI catalogue"); every
other input — including a restricted-corpus code, `None`, or a malformed value — returns the fixed
non-identifying fallback, **"recorded in another reference source"**. This is an ALLOWLIST, not a
denylist: the function never echoes its input back, so a restricted corpus name is structurally
incapable of reaching a return value on any code path, including error paths. This rule applies
identically to copy/clipboard output, JSON payloads, and error paths — no code path is exempt.

The verdict cache produced by a real funnel run is a **build-time artifact and is NEVER shipped
inside the sidecar** — it exists only to make a corpus-scale run resumable and to avoid re-billing
completed model calls; nothing downstream reads it after the build.

## 7. Grading rule — owner labels only, funnel can never grade itself

Agreement is measured against OWNER-SUPPLIED labels only
(`discovery_data/novelty_hardcase_labels-v1.json`, `label_provenance.source == "owner_supplied"`). An
entry lacking owner provenance is EXCLUDED from grading, never treated as truth — the evaluation
cases were selected precisely because a string comparison fails on them, so a pipeline-supplied label
would grade the funnel against its own reasoning and the resulting number would mean nothing. Skipped
cases are excluded and counted separately, so the effective evaluation size is reported rather than
assumed. The two error directions (a false claim of novelty vs. a false claim of "already recorded")
are reported SEPARATELY, never folded into one combined accuracy figure — the first is the
reputationally expensive direction (decision B); the second is ruling J's permanent-false-known risk.
See `scripts/discovery_novelty_funnel.py`'s grading harness for the implementation.

## 8. Known limitations of the ground-truth set (per `136-NOVELTY-PRIOR-ART.md` sections 5c/7/9)

The owner-labelled hard-case set (101 cases as of plan 136-03: 8 identity spot-check + 30 Class-6
catalogue-divergence + 30 Arm-1 residual + 25 Arm-2 heuristic-demoted + 8 Arm-3 no-source-text) is a
genuinely adversarial, zero-model-call-selected sample, but it has two structural limits a future
reader must not lose sight of:

- **It answers a vocabulary-validity question, not a production-accuracy question.** "Does the owner
  agree with the proposed shade labels on genuinely hard cases" is a DIFFERENT question from "how
  accurately will the pinned model classify the full, multi-source checked-source funnel in
  production." Arm 1 (30 cases across 6 populated strata) answers a per-stratum accuracy question but
  does NOT establish a corpus-wide RATE for how common each stratum is. Arm 2 (25 cases, deliberately
  oversampling the two Codex-flagged false-known populations) answers "of the rows a heuristic
  demotes without ever consulting a model, how many are false-knowns" on a small, oversampled slice —
  it does NOT give a project-wide false-known rate. Arm 3 (8 cases) is explicitly not a labelling
  exercise and produces no graded number.
- **No arm measures a corpus-wide base rate.** A future pass wanting base rates must run the real
  funnel over the full corpus and report its own per-stratum counts, never re-derive them from this
  labelling sample.

## 9. Cross-references

- `136-GATE1-DECISIONS.md` sections E, E′, F, G, H, I, J, K, L — the ratified rulings this contract
  implements, with full rationale and worked cases. **K** halts the ~$301 production run pending a
  purpose-built `fills_gap` probe (see `136-NOVELTY-RUN.md`'s probe section); **L** drops
  `divergence_correctness` from the model's output contract (section 2 and section 5 above).
- `136-NOVELTY-PRIOR-ART.md` — the prior-art reconciliation pass (Codex REWORK findings, the
  five-way↔ten-shade mapping, the eval-population analysis).
- `docs/specs/discovery-sidecar-schema-v1.md` — the `novelty_status` / `novelty_source_label` /
  `divergence_correctness` column definitions and the (unavoidably restated) SQL `CHECK` constraint.
- `.planning/REQUIREMENTS.md` NOVEL-01/NOVEL-02 — the dated amendment trail.
- `136-NOVELTY-RUN.md` — the ruling-I re-measurement report and the authorized production run's
  measured cost/agreement (or its documented deferral).
