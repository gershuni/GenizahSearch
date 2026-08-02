---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 03
subsystem: discovery
tags: [novelty, gate1-decisions, sqlite, openpyxl, xlsx, llm-eval-set, sidecar-schema]

# Dependency graph
requires:
  - phase: 135
    provides: the frozen discovery-v1-33499c5b… asset this plan measures read-only
provides:
  - Five ratified gate-1 decisions (D-13e/D-16/D-13c/D-13b/D-13d) as named constants + predicates, citable by later plans
  - The novelty flag's tri-state amended to a TEN-value shade enum (rulings E/E′/F/G/H), with a separate divergence_correctness axis and a funnel-first architecture (ruling J)
  - An owner-labelled, 101-case, three-arm-plus-identity-plus-Class-6 novelty evaluation set with a content-hashed ground-truth label file
  - A per-arm measured analysis of that evaluation set (Arm 1 residual, Class 6 divergence, Arm 2 heuristic-demoted, Arm 3 no-source-text)
  - A dormant D-13d gap flagged (works_related_by_title over-collapses different volumes of one multi-volume opus) for future re-examination
affects: [136-04, 136-05, 136-06, 136-12, 136-13, 136-15, 136-16, 136-17, 136-18]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Read-only measurement script over a frozen asset, exits nonzero on an unexpected zero-row result (mirrors scripts/bench_discovery.py)"
    - "XLSX labelling workbook as the owner-facing instrument, Markdown as the citable record; both rendered from ONE pre-numbered case list so they can never drift apart"
    - "XLSX round-trip fails CLOSED on a missing/renamed sheet, header mismatch, out-of-vocab cell value, or a Case # outside the expected contiguous range"
    - "Vocabulary tokens defined ONCE as module-level tuples (SHADE_VOCABULARY/IDENTITY_VOCABULARY/CORRECTNESS_VOCABULARY/DEMOTION_VOCABULARY) and reused for the Markdown table, the XLSX DataValidation list, AND the read-back validator — never a second hand-copied list"
    - "Content-hash-over-cases-array as the tamper-detection mechanism for an owner-supplied ground-truth file, recorded in the citable decision doc rather than relying on git tracking (discovery_data/ is gitignored project-wide)"

key-files:
  created:
    - discovery_data/novelty_hardcase_labels-v1.json
    - .planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-GATE1-EVIDENCE.md
    - .planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-NOVELTY-HARDCASES.md
    - .planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-NOVELTY-HARDCASES.xlsx
    - .planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-NOVELTY-PRIOR-ART.md
  modified:
    - scripts/discovery_gate1_evidence.py
    - .planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-GATE1-DECISIONS.md
    - .planning/REQUIREMENTS.md
    - .planning/ROADMAP.md
    - docs/specs/discovery-sidecar-schema-v1.md
    - .planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-04-PLAN.md
    - .planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-12-PLAN.md

key-decisions:
  - "D-13e: KEEP the third 'Also shares text with' disclosure level (94.5% of the 40,615-item middle bucket is not otherwise reachable)"
  - "D-16/PANEL-01: the corpus-wide findings page does NOT get a relation filter (94% of the Main pool is already direct_witness)"
  - "D-13c: the short-evidence threshold stays 150 matched letters"
  - "D-13b: identical-span lead-attribution ties break on lexicographic evidence_id (reused verbatim from scripts/discovery_ids.py)"
  - "D-13d: the author-gated same-title/prefix collapse rule is adopted as proposed (276 of 1,367 groups collapse); a dormant volume-vs-volume over-collapse gap in the SAME predicate is flagged for future re-examination, not fixed"
  - "Novelty flag widens tri-state -> ten-value shade enum across rulings E/E'/F/G/H, with a separate divergence_correctness axis (ruling F) and a funnel-first architecture where the LLM only sees the heuristic funnel's residual (ruling J)"
  - "PANEL-01/PANEL-02/NOVEL-01 are NOT marked complete in REQUIREMENTS.md despite being in this plan's frontmatter requirements list -- this plan only ratifies the decisions and ground truth those requirements will be built against; the actual panel/findings-page/novelty-flag implementation happens in 136-04/136-12/136-15..18"

requirements-completed: []

# Metrics
duration: multi-continuation (same day, 2026-08-02); this closing continuation ~45min
completed: 2026-08-02
---

# Phase 136 Plan 03: Gate-1 Decisions, Novelty Shade Enum & Owner-Labelled Evaluation Set Summary

**Five gate-1 decisions ratified with measured numbers as citable constants/predicates; the novelty flag evolved from a tri-state to a ten-value shade enum across five owner rulings (E/E′/F/G/H) plus a funnel-first architecture (ruling J); and a 101-case, owner-labelled ground-truth evaluation set was built, labelled, and analyzed per-arm via a new XLSX round-trip in `scripts/discovery_gate1_evidence.py`.**

## Performance

- **Duration:** multi-continuation, same calendar day (2026-08-02); this closing continuation (Task 4: label-file round-trip + per-arm analysis + plan close) ~45 min
- **Completed:** 2026-08-02
- **Tasks:** 4 of 4 (Task 1 auto, Task 2 checkpoint:decision, Task 3 checkpoint:human-action, Task 4 auto — this continuation)
- **Files modified:** 7 tracked (`scripts/discovery_gate1_evidence.py`, `136-GATE1-DECISIONS.md`, `136-GATE1-EVIDENCE.md`, `136-NOVELTY-HARDCASES.md`/`.xlsx`, `REQUIREMENTS.md`, `ROADMAP.md`, `docs/specs/discovery-sidecar-schema-v1.md`, `136-04-PLAN.md`, `136-12-PLAN.md`) + 1 gitignored artifact (`discovery_data/novelty_hardcase_labels-v1.json`)

## Accomplishments

- **All five gate-1 decisions ratified** with their measured numbers, verbatim owner answers, dates, and exact code consequences (named constants / predicates) in `136-GATE1-DECISIONS.md` §A — citable by `shared/discovery_main_pool.py` and `shared/discovery_grouping.py` when those modules are built.
- **The novelty axis evolved from a tri-state to a ten-value shade enum** across owner rulings E → E′ → F → G → H, each recorded with its exact schema/prompt/verifier consequences, and reconciled INTO the documents that implement them (`136-04-PLAN.md`, `136-12-PLAN.md`, `docs/specs/discovery-sidecar-schema-v1.md`, `ROADMAP.md`, `REQUIREMENTS.md`) after a prior-art research pass found that propagation had been skipped.
- **Ruling J adopted a funnel-first architecture** for the novelty LLM gate (heuristic funnel runs first; the model only ever sees the residual) and recorded the resulting permanent, unrecoverable heuristic-false-known risk as an accepted, measured cost.
- **A 101-case, owner-labelled ground-truth evaluation set was built and labelled**: 8 identity spot-check + 30 Class-6 catalogue-divergence + 30 Arm-1 residual + 25 Arm-2 heuristic-demoted + 8 Arm-3 no-source-text, selected with zero model calls and full reproducibility.
- **This continuation (Task 4):** read the owner-filled `136-NOVELTY-HARDCASES.xlsx` back via a new fail-closed round-trip mode added to `scripts/discovery_gate1_evidence.py`, wrote `discovery_data/novelty_hardcase_labels-v1.json` with per-entry `label_provenance` and a sha256 content hash, and recorded a per-arm (never single-headline) analysis in `136-GATE1-DECISIONS.md`.
- **A genuine, currently-dormant gap in the D-13d collapse predicate was found and flagged** (not fixed) while investigating the owner's own skip note on the near-miss identity cases.

## Task Commits

Selected commits (34 total across the plan's four tasks and continuations; grouped by wave):

**Task 1 (evidence + hard-case candidates, original 52-case set):**
1. `6ecbad49` feat(136-03): measure the five gate-1 decisions + novelty hard-case candidates

**Task 2 (owner ratifies gate-1 decisions):**
2. `fff0f5d7` docs(136-03): record owner gate-1 rulings in 136-GATE1-DECISIONS.md
3. `163c1803` docs(136-03): record STATE.md position for the Task 3 halt

**Task 3/owner-ruling iterations (E, E′, F, G, H, I, J — each a re-halt at the Task-3 checkpoint after an owner ruling changed the vocabulary or evaluation-set construction):**
4. `28a7a22a` / `5fe8829d` / `efadb975` — decision E (tri-state → 7-shade enum) + Class 6
5. `144dd101` / `5bceaad6` / `6b41cb84` — correction E′ (direction-split granularity) + XLSX workbook
6. `10d5ca39` / `2fbc67f1` / `29fc32f7` — rulings F/G (divergence scope-split + free-text confirms boundary) + worksheet restructure
7. `d9e3ff79` / `d24e53b1` / `65d51645` / `e9f2c137` — prior-art reconciliation pass + rulings H/I (container_predicts + re-measurement gate)
8. `ac77e02e` / `25dde2fa` / `24ba443d` / `e9b81e23` — reconciliation of `136-04-PLAN.md`, `136-12-PLAN.md`, the sidecar schema doc, and `ROADMAP.md` to the current enum
9. `e4a1d5bd` / `6e23837d` / `11fbe9e9` — ruling J (funnel-first architecture) + the three-arm source-stratified sampler rebuild (101 total candidates)

**Task 3 returns / Task 4 (this continuation):**
10. `95b49d8b` docs(136-03): record owner-filled novelty hard-case workbook (Task 3 returns)
11. `8e257f77` feat(136-03): add Task 4 XLSX label round-trip to discovery_gate1_evidence.py
12. `a8e67d0f` docs(136-03): Task 4 complete -- label-file hash, per-arm novelty analysis, D-13d dormant-gap flag

**Plan close (this commit, via the standard final-commit protocol):** pending — SUMMARY.md + STATE.md + ROADMAP.md.

## Files Created/Modified

- `discovery_data/novelty_hardcase_labels-v1.json` — the owner-supplied ground truth (gitignored, matching every sibling `discovery_data/` artifact); 101 cases, `label_provenance` on every entry, sha256 content hash `ce0dc2fb176d01de5f04975eac82017feaeb390db71e9ec13f3872a77d28d4b6` recorded in `136-GATE1-DECISIONS.md`
- `scripts/discovery_gate1_evidence.py` — the single measurement + hard-case-generation + label-round-trip script; new `read_owner_labels_from_xlsx`/`write_owner_labels_json` functions + `--read-labels-from`/`--labels-out` CLI mode (this continuation), on top of the prior continuations' `select_novelty_arms` three-arm sampler and Classes 1-3/6 selectors
- `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-GATE1-DECISIONS.md` — the single citable record: sections A-D (gate-1 decisions), E/E′/F/G/H/I/J (novelty shade-enum evolution), and this continuation's new post-J section (label-file hash + per-arm analysis + the D-13d dormant-gap finding)
- `136-GATE1-EVIDENCE.md` / `136-NOVELTY-HARDCASES.md` / `.xlsx` — the owner-facing briefs and labelling workbook
- `.planning/REQUIREMENTS.md`, `.planning/ROADMAP.md`, `docs/specs/discovery-sidecar-schema-v1.md`, `136-04-PLAN.md`, `136-12-PLAN.md` — reconciled to the final ten-value shade enum + `divergence_correctness` column + funnel-first architecture (prior continuations)

## Decisions Made

See `key-decisions` in the frontmatter for the five gate-1 rulings and the novelty-enum evolution. Additionally, this continuation:

- **Verified the owner's cell-by-cell answers independently** by re-opening the actual `.xlsx` via `openpyxl` rather than trusting the dispatch's own summary — found the DATA matched exactly (zero discrepancies across all 101 cases), but found and reported a FRAMING error in how the dispatch characterized cases 6-8 ("Class 3" — they are Class 1, near-miss titles; Class 3 is genuinely cases 1-3, which the owner marked `same_work`).
- **Investigated, and did not fix, a dormant D-13d gap:** the owner's skip note for the near-miss identity cases prompted checking whether D-13d's `works_related_by_title` predicate would collapse a real example of two different catalogued volumes of one multi-volume opus if they ever shared a span — confirmed it would, but confirmed via a corpus-wide query that this never currently occurs in the live asset. Flagged for a future session; the collapse rule is unchanged.
- **Chose NOT to mark PANEL-01/PANEL-02/NOVEL-01 complete** in `REQUIREMENTS.md`, despite their presence in this plan's frontmatter `requirements` field — this plan ratifies decisions and ground truth those requirements will be built against; the actual panel UI, findings-page filter, and novelty-flag computation are built in 136-04/136-12/136-15 through 136-18. Marking them complete here would overclaim unbuilt behavior.
- **Added the XLSX round-trip to the SAME canonical script** (`scripts/discovery_gate1_evidence.py`) rather than a one-off script, reusing its existing vocabulary constants (`IDENTITY_TOKENS`/`SHADE_TOKENS`/`CORRECTNESS_TOKENS`/`DEMOTION_TOKENS`) as the sole source of truth for out-of-vocabulary rejection, and its `_CLASS_TITLES` reverse lookup for class-code recovery — keeping the whole plan's output reproducible from one module.

## Deviations from Plan

None (Rule 1-3) — Task 4 was executed as specified: read the workbook, write the label file with provenance and a content hash, record the hash and analysis, close the plan. Two items go beyond the plan's literal Task 4 text but are explicitly authorized by this continuation's own dispatch instructions (tasks 3 and 4 of the objective, not Rule-1-3 deviations):

1. **Investigated the Class 1 vs. Class 3 span/page premise for cases 6-8** (dispatch-authorized "Task 4" of the objective) — found the premise the dispatch attributed to those cases was never true of them (Class 1 was never built on a co-occurrence premise), confirmed Class 3's own construction is genuinely sound, and found a real-but-dormant D-13d predicate gap. Flagged, not fixed, per the dispatch's own "Do NOT change the collapse rule" instruction.
2. **Reported wave-2+ impact** (dispatch-authorized "Task 6" of the objective) without acting on it — see "Next Phase Readiness" below.

**Total deviations:** 0 auto-fixed. **Impact on plan:** none — the two investigative items above were explicit instructions in the continuation's own dispatch, not unplanned discoveries requiring a Rule 1-4 judgment call.

## Issues Encountered

- **`discovery_data/` is gitignored project-wide** (`/discovery_data/` in `.gitignore`, verified — no file under that directory has ever been committed). `discovery_data/novelty_hardcase_labels-v1.json` is therefore NOT git-tracked, consistent with every sibling artifact in that directory (the `.db` assets, `manifest.json`, `composition_dates.json`, etc.) — this matches the project's established pattern of keeping large/generated `discovery_data/` assets local-only and citing their content hash from a git-tracked document instead (here, `136-GATE1-DECISIONS.md`). Not a defect; flagged here so a future reader does not mistake the absence of a git commit for a missing deliverable.
- **An Excel lock file** (`~$136-NOVELTY-HARDCASES.xlsx`) was present in the phase directory during this continuation, indicating the workbook may still be open in Excel. The on-disk `.xlsx` read via `openpyxl` was complete and internally consistent (matched the dispatch's own summary of the owner's answers exactly), so this was not a blocker; the lock file itself was left untouched and not committed (it is untracked and was not staged).

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

**Wave 2+ impact of this continuation's findings (reported per the dispatch's own Task 6 instruction; NOT acted on):**

- **136-04 (novelty module + pinned LLM contract) is NOT blocked.** It already carries ruling I's re-measurement gate (must re-measure the pinned config against this 101-case labelled set on the current ten-value/free-text contract before authorizing the production run) and ruling J's funnel-first/residual-only wiring, both reconciled into `136-04-PLAN.md` by an earlier continuation. This continuation's Arm-2 finding (inconclusive, instrument gap) does not change 136-04's acceptance criteria — the plan's existing scope already treats Arm 2's data as a labelled evaluation input, and an inconclusive result on 7 of 25 rows is information for whoever reviews that gate's results, not a reason to halt or re-plan 136-04 itself. The label file exists with its recorded content hash; 136-04 can read and re-verify it as designed.
- **136-13 (the one authorized rebuild) is NOT blocked.** None of this continuation's findings touch the rebuild's own scope (the D-02a authorization row, direct-family coverage_ppm, the ten-value novelty flag for all evidence families, VIS-01 projection fields, D-13d's threshold/rule as already ratified). The D-13d dormant-gap finding is explicitly NOT a change to the ratified collapse rule — `works_related_by_title` ships exactly as decided in §A above; the finding is a flag for a FUTURE re-examination of that predicate, not a pre-rebuild blocker.
- **A genuinely new, small finding for whoever eventually revisits `works_related_by_title` in `shared/discovery_grouping.py`:** the author + ≥4-character-title-prefix predicate does not distinguish "same work, two granularities" (correct to collapse) from "two distinct, non-interchangeable volumes of one multi-volume opus" (arguably should not collapse). Currently dormant (zero occurrences in the live asset among the three affected works `w000007`/`w000036`/`w000038`); worth a one-line note in whichever future plan next touches that predicate.
- **Arm 2's redesign recommendation (surfacing the actual bib/PGP source text alongside each case) is not built and not scheduled** — it is a recommendation for a future labelling pass, should the false-known rate on `published_full_sole`/`pgp_sole` ever need a real measurement.

Plan 136-03 is now CLOSED. Next: `136-04-PLAN.md`.

---
*Phase: 136-read-surfaces-connections-panel-work-witnesses*
*Completed: 2026-08-02*

## Self-Check: PASSED

- FOUND: `discovery_data/novelty_hardcase_labels-v1.json`
- FOUND: `scripts/discovery_gate1_evidence.py`
- FOUND: `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-GATE1-DECISIONS.md`
- FOUND: `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-03-SUMMARY.md`
- FOUND commit: `95b49d8b`, `8e257f77`, `a8e67d0f`
