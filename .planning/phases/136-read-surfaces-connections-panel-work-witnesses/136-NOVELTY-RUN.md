# Phase 136 Plan 04 Task 3 — Novelty Funnel Run Record

**Status: NEITHER the ruling-I re-measurement NOR the production run was executed. Both are
explicitly DEFERRED, for an ENVIRONMENT reason, not an owner decision.** This section states that
distinction up front because Task 3's own acceptance criteria explicitly anticipate and permit this
outcome ("The production-run section is NOT populated (or is explicitly marked deferred, with a
stated reason) unless the ruling-I re-measurement section above is present and complete") — this
record exercises exactly that permitted path, and documents precisely why, so a future session picks
up from an accurate starting point rather than a silently-skipped one.

## 0. Why this run did not happen — an execution-environment gap, not an authorization gap

This plan (136-04) was executed by a PARALLEL executor inside an isolated git worktree
(`isolation="worktree"`, per this plan's own dispatch). Before writing this record, the environment
was checked directly and found to lack every precondition Task 3's real run requires:

- **The owner-labelled ground-truth file is absent from this worktree.**
  `discovery_data/novelty_hardcase_labels-v1.json` (written by plan 136-03 Task 4; recorded content
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

## 1. The recorded authorization (read before any of the above was even checked)

Per `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-GATE1-DECISIONS.md`
section B, the owner's authorization on record is **`run-now-pinned`**: "RUN NOW, PINNED. The
validated cheap configuration (`gemini-3.6-flash`, `reasoning:{effort:"low"}`), ~$27 one-time. Do NOT
downgrade the model. Read the real `usage.cost` from the provider after the run — never estimate it."
This authorization is honoured exactly as recorded: no model call was made under any other
authorization, and none was made at all in this execution (see section 0 above) — this is
consistent with, not a violation of, `run-now-pinned`, because ruling I (below) makes the production
run's operative-ness CONDITIONAL on a re-measurement that itself could not run here either.

Per section I of the same record, decision B's "run now, pinned" authorization does **NOT** become
operative for the full production run until a re-measurement of the pinned config against the
owner-labelled evaluation set, on the CURRENT ten-value vocabulary and ruling G's free-text input
contract, is on the record. **That re-measurement could not be performed in this environment either**
— it requires reading the very label file section 0 establishes is absent here, and making the same
kind of pinned-model call this environment has no credentials for.

The `~$27` figure on record is a COST estimate carried forward by size-extrapolation. **It is NOT,
and is never cited here as, an accuracy result.** No measured cost (no `usage.cost` reading) exists
for this task, because no model call was made.

## 2. Ruling-I re-measurement — DEFERRED

**Not performed.** The gate ruling I imposes — re-measure the pinned config against
`discovery_data/novelty_hardcase_labels-v1.json` (content hash
`sha256:ce0dc2fb176d01de5f04975eac82017feaeb390db71e9ec13f3872a77d28d4b6`, per
`136-GATE1-DECISIONS.md`) on the current ten-value shade enum and the free-text input contract,
grading via `scripts/discovery_novelty_funnel.py::grade_against_owner_labels` — requires exactly the
two things section 0 establishes are unavailable here: the label file itself, and a live pinned-model
connection. Neither exists in this execution environment.

This is recorded as its own, clearly-labelled section, distinct from and preceding the production-run
section below, per Task 3's own acceptance criteria. It is **not** the five-way-vocabulary validation
already on record elsewhere (`reference_discovery_llm_gate_cost`; `136-GATE1-DECISIONS.md` section B)
— that record covers a narrower question (the five-way vocabulary, the one-title-string input
contract) and remains what it always was; this section is the ten-value/free-text re-measurement
ruling I requires, and it simply has not happened yet, for the environment reason stated in section 0.

## 3. Production run — NOT populated, per Task 3's own permitted deferral path

Explicitly deferred. Per Task 3's own acceptance criteria, the production-run section is "NOT
populated ... unless the ruling-I re-measurement section above is present and complete" — section 2
above is not complete (it could not run at all), so this section correctly stays empty of any
production result. No funnel was run over any portion of the real corpus. No `not_checked` count
changed for any real identification as a result of this plan's execution: every identification the
live `discovery-v1-*.db` asset carries remains exactly as it was before this plan ran — `not_checked`
(the fail-closed default) wherever it was already `not_checked`, per NOVEL-01's coverage-gap framing.

## 4. What WAS built, exercised and verified in this environment (Tasks 1–2)

Although the real corpus-scale run could not execute here, the funnel's actual MECHANISM was built,
and its riskiest properties were exercised against fixtures, at zero cost and with zero model calls:

- **The heuristic (mechanical) funnel pass** (`scripts/discovery_novelty_funnel.py::run_heuristic_pass`)
  was exercised against fixtures covering every named Codex-finding defect this plan was built to
  avoid: a `published_full`-only bibliography row and a bare PGP description alone each correctly
  fail to produce a decisive verdict (Codex findings 1/6); two distinct `ref_work` rows sharing a
  conceptual collapsed identity each use their own title, never a shared representative's (Codex
  finding 2/3); an unmapped page routes explicitly to `not_checked` with a logged reason, never a
  silent drop (Codex finding 4); the assembled evidence bundle tags every source's free text by its
  own provenance and always includes bibliography/PGP text even though non-decisive alone (Codex
  finding 5); and a worked case mirroring the real `136-GATE1-DECISIONS.md` section G example (an
  alias spelling present only in the catalogue's free text) correctly resolves to `confirms`, never
  `diverges_work`/`diverges_part`.
- **The checkpointed, resumable model-arm mechanism** (`run_model_arm`) was exercised with a fixture
  that simulates a mid-run crash: the first candidate's verdict is checkpointed to disk, the second
  candidate's call raises, and a subsequent invocation of `run_model_arm` against the SAME checkpoint
  file resumes by calling the model ONLY for the still-incomplete candidate — the completed
  candidate's model call is never repeated ("re-billed"). This demonstrates the resumability property
  a corpus-scale run needs; the property was exercised via this fixture, not via the real
  (unavailable) corpus.
- **The grading harness's three hard rules** were exercised end to end: an entry lacking
  `label_provenance` recording owner supply is excluded from grading and counted separately; a label
  file with ZERO owner-provenance entries makes `grade_against_owner_labels` raise the dedicated
  `NoOwnerProvenanceLabels` exception with the exact literal message `"no owner-provenance labels"` —
  and this specific guard was MUTATION-TESTED by hand during Task 2's implementation: the guard (`if
  len(provenance_cases) == 0: raise NoOwnerProvenanceLabels(...)`) was temporarily commented out in
  `scripts/discovery_novelty_funnel.py`, the two tests asserting this specific exception/message were
  re-run, and BOTH failed with `Failed: DID NOT RAISE <class '...NoOwnerProvenanceLabels'>` (the
  no-guard code path silently returned a result instead) — proving the guard is load-bearing and the
  tests are not vacuously satisfied by any exception. The guard was then restored and the full suite
  re-confirmed green (93 passed). Skipped cases are excluded from grading and counted separately. The
  two novelty error directions (a false claim that something IS novel — the reputationally expensive
  direction per decision B — vs. a false claim that something is already recorded — ruling J's
  conservative lost-finding-risk direction) are reported as two SEPARATE counts, never folded into one
  combined accuracy figure, on both the fixture exercises above and by direct assertion in the test
  suite (`test_grading_reports_two_error_directions_separately_never_combined`).
- **`load_owner_labels`'s content-hash verification** (`LabelHashMismatch`) was exercised against a
  synthetic label file: a mismatched expected hash correctly refuses to load, and the correct hash
  (computed the same way `136-GATE1-DECISIONS.md` records it — sha256 over
  `json.dumps(cases, sort_keys=True, ensure_ascii=False)`) correctly succeeds.

None of the above required the real label file, the real sidecars, or a real model call — it is all
fixture-driven, and all of it is committed as automated tests (`tests/test_discovery_novelty_contract.py`,
93 passing).

## 5. Path to completion — what a future execution must do

Before the production run can proceed, a future execution (ideally NOT a parallel isolated worktree,
or one with the real `discovery_data/`, `fist_data/fjms_enrichment.db`, `pgp_data/pgp.db`,
`fgp_data/fgp_transcriptions.db` sidecars present and real LLM provider credentials configured) must:

1. Load `discovery_data/novelty_hardcase_labels-v1.json`, verify its content hash against
   `sha256:ce0dc2fb176d01de5f04975eac82017feaeb390db71e9ec13f3872a77d28d4b6` via
   `scripts/discovery_novelty_funnel.py::load_owner_labels`.
2. Build `NoveltyCandidate` rows for the 101-case evaluation set from the real sidecars (per source:
   catalogue text from FJMS `catalog.TitleHeb`/`GenizahTitleOrgTitle`, bibliography rows from the
   Friedberg bibliography table, PGP description/transcription, FGP transcription text, and the
   M-source shelfmark attribution with its corpus name masked at the point of read).
3. Run `run_heuristic_funnel` over that set; run the pinned model (`gemini-3.6-flash`,
   `reasoning:{effort:"low"}`) via `run_model_arm` over ONLY the residual, with a real `model_call`
   implementation reading the actual provider's `usage.cost` field after each call and a real
   checkpoint file path.
4. Grade the result via `grade_against_owner_labels` against the SAME 101-case label file, restricted
   to entries with `label_provenance.source == "owner_supplied"`. Record the effective evaluation
   size (after excluding non-owner-provenance and skipped entries), the two error directions
   separately, and the per-shade verdict counts.
5. Only once that re-measurement is recorded here as its own section, with a real measured cost from
   `usage.cost`, does decision B's authorization become operative for the full production run over
   the live corpus — which must then ALSO be checkpointed, its own real measured cost recorded, and
   its own two-directional agreement (against the same label file) reported, before the verdict cache
   is staged (hash-pinned, outside the committed tree, per NOVEL-02 — the cache is a build-time
   artifact and must never ship inside the sidecar).

## 6. Compliance checks for this record

- No prompt text, model response, or raw provenance value appears anywhere in this document (none
  was ever produced — no model call was made).
- No verdict-cache file exists or is staged in this worktree (`git status` shows nothing under
  `discovery_data/` because that directory does not exist here at all).
- This document does not name any restricted corpus — every checked source is referred to generically
  (catalogue / bibliography / PGP / FGP / the internal shelfmark-attribution source), consistent with
  D-25/NOVEL-02 and the project's own codename discipline.
- The `~$27` figure is cited above exactly once, explicitly labelled as a cost estimate, and is never
  presented as an accuracy or agreement result for any vocabulary, past or current.
