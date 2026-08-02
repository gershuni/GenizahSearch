---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 07
subsystem: discovery-main-pool-rule
tags: [main-pool, bucket-rule, discovery-grouping, granularity, panel, findings-page, ast-guard]

# Dependency graph
requires:
  - phase: 136-01
    provides: "The Amendment 2026-08-02 schema contract's main_pool_reason closed vocabulary and discovery_identification table shape"
  - phase: 136-02
    provides: "web/pages/help.py's MAIN_POOL_SENTENCE bilingual constant, which this plan's parity test pins against"
  - phase: 136-03
    provides: "The ratified 136-GATE1-DECISIONS.md rulings (D-13b/c/d, the two-Rashi-titles worked case in 136-GATE1-EVIDENCE.md) this plan implements"
provides:
  - "shared/discovery_main_pool.py: main_pool_decision(identification) -> (bool, reason_code) — the four non-compensating gates plus the human_confirmed override, as ONE pure predicate; main_pool_sentence(lang) and bucket_label(in_main_pool, lang) — the sole bilingual wording for the rule and its bucket names"
  - "shared/discovery_grouping.py: collapse_canonical (D-13a), lead_attribution (D-13b, reuses discovery_ids.select_display_evidence's existing total order verbatim), separate_granularity (D-13d, ports works_related_by_title verbatim from scripts/discovery_gate1_evidence.py)"
  - "A standing AST guard (tests/test_discovery_main_pool.py) proving no module under shared/ or web/ re-derives bucket membership from a local band-name set — exercised against a seeded confOf()/STRONG_BANDS-shaped duplicate and observed failing"
affects: [136-11, 136-12, 136-14, 136-15, 136-16, 136-17, 136-18]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Read-never-import cross-layer parity: shared/discovery_main_pool.py's main_pool_sentence() is pinned against web/pages/help.py's MAIN_POOL_SENTENCE via ast.literal_eval over help.py's own source text (shared/ must never import web/), not a plain substring search — the sentence is wrapped across multiple adjacent string literals in help.py's source, so a raw-text substring match cannot locate the merged value."
    - "AST-based 'no second implementation' guard resolving through named constants: the guard scans for an ast.Compare 'in'/'not in' node whose comparator is EITHER an inline literal collection OR a Name resolving (via a first-pass module-level assignment scan) to one, so it also catches the historical confOf()/STRONG_BANDS shape (a named constant), not only an inline literal set."

key-files:
  created: []
  modified:
    - shared/discovery_main_pool.py
    - shared/discovery_grouping.py
    - tests/test_discovery_main_pool.py
    - tests/test_discovery_grouping.py

key-decisions:
  - "The four main-pool gates are non-compensating and evaluated in a fixed order over an Identification record (dataclass-or-Mapping); gate 2 delegates to shared.discovery_band_labels.is_default_eligible rather than re-deriving band quality; human_confirmed is evaluated before every gate, including when routing demoted the row."
  - "D-13d's separate_granularity implements works_related_by_title EXACTLY as ratified (author-gated identical-title-or->=4-char-prefix match), including its dormant, deliberately-unfixed w000007/w000036/w000038 gap — confirmed never triggered corpus-wide, per the decision record's own 'not an authorized code change' framing."
  - "main_pool_sentence/bucket_label are the SOLE wording for the two-bucket rule and its bilingual names anywhere under shared/ — enforced both by a parity test against web/pages/help.py and by a standing AST guard scanning shared/ and web/ for a second, locally-defined bucket-membership predicate."
  - "The scripts/project_discovery_public.py stand-in heuristic for main-pool bucketing (landed by plan 136-08, before this module existed) is explicitly OUT of this plan's scope and is NOT touched here — see 'Reconciling the 136-08 stand-in' below."

patterns-established:
  - "A discovery-surface bucket/label rule ships as: (1) a pure decision function taking a dataclass-or-Mapping fixture, (2) a closed reason-code frozenset asserted equal to the schema's CHECK constraint vocabulary, (3) named constants citing the ratifying decision doc and their PROVISIONAL/LOCKED status, (4) a bilingual wording function pinned against its one UI consumer by AST-literal-eval parity, (5) a standing AST guard against a second reimplementation."

requirements-completed: [PANEL-01, PANEL-02]

# Metrics
duration: 35min (Task 3 + reconciliation + summary, this continuation only; Tasks 1-2 timing recorded in the prior executor's own work, not re-measured here)
completed: 2026-08-03
---

# Phase 136 Plan 07: Main-Pool Bucket Rule & Display-Grouping Predicates Summary

**The two visible discovery buckets ("main pool" / "more matches") are now drawn everywhere by ONE pure, fully-tested predicate with a closed reason-code vocabulary and a single bilingual wording — closing the exact failure class (a hand-picked, drifting band-set rule) that mislabelled the best-measured population in the system "Weak" in an earlier sketch.**

## Continuation notice (read this first)

This plan was executed by **two agents**. A prior executor completed **Task 1** (`shared/discovery_main_pool.py` — commit `ff46f5e1`) and **Task 2** (`shared/discovery_grouping.py` — commit `62be088a`), then **died on an API error before starting Task 3**. Its work was merged to `master-main` (merge commit `f62f3798`, "plan 136-07 tasks 1-2 (agent died on API error before task 3)") and independently verified green (117 tests: `test_discovery_main_pool.py` + `test_discovery_grouping.py` + `test_discovery_visibility.py` + `test_vis01_projection.py`) before this continuation began. **This continuation did NOT redo Tasks 1-2** — it read the merged code and commits, executed **Task 3** only, and reconciled the inherited 136-08 stand-in-heuristic obligation (see below).

## Performance

- **Duration:** 35 min (Task 3 implementation + reconciliation research + this summary)
- **Started:** 2026-08-03 (continuation)
- **Completed:** 2026-08-03
- **Tasks:** 3 of 3 complete (1 and 2 by the prior executor, 3 by this continuation)
- **Files modified:** 2 (both already-existing from Tasks 1-2, extended for Task 3)

## Accomplishments

### Task 1 (prior executor, commit `ff46f5e1`) — `shared/discovery_main_pool.py`, the four gates

`main_pool_decision(identification) -> (bool, reason_code)` implements the four main-pool-rule gates (no same-work claim / screening-band-only / unresolved tie on every page / single-page under the 0.8 coverage floor) plus the D-13c 150-matched-letter short-evidence threshold and the `human_confirmed` override, as one non-compensating, fixed-order predicate over an `Identification` dataclass-or-Mapping. `MAIN_POOL_REASONS` equals the schema's `main_pool_reason` CHECK constraint vocabulary exactly (asserted by test). Gate 2 delegates to `shared.discovery_band_labels.is_default_eligible` rather than re-deriving band quality; the module never reads `density` (the edit-distance field the repo has a documented scar from confusing with coverage) and contains no weighted-sum arithmetic (asserted by an AST scan for any `ast.Mult` node). `SHORT_EVIDENCE_THRESHOLD_MATCHED_LETTERS` (150, LOCKED) and `COVERAGE_FLOOR` (0.8, explicitly PROVISIONAL) are named constants citing `136-GATE1-DECISIONS.md`. 17 tests, one per `<behavior>` bullet plus the vocabulary/no-density/no-weighted-sum/gate-2-delegation/cited-constants acceptance checks.

### Task 2 (prior executor, commit `62be088a`) — `shared/discovery_grouping.py`, display-time collapse and granularity separation

Three pure functions: `collapse_canonical` (D-13a — two claims sharing a `canonical_work_id` collapse to one row, the canonical work's own title winning); `lead_attribution` (D-13b — reuses `scripts.discovery_ids.select_display_evidence`'s existing total order verbatim, band rank then the lexicographic `evidence_id` tie-break, never a fresh tie-break, applied repeatedly over a shrinking candidate set so the ordering is provably the same one the existing selector implements); `separate_granularity` (D-13d — ports `works_related_by_title`/`normalize_title`/`titles_share_prefix` VERBATIM from `scripts/discovery_gate1_evidence.py`, the module the ratified rule was measured against). Closes the ⚠ KNOWN FLAW from the second mockup pass: the two-Rashi-titles worked case (real work ids/titles/span from `136-GATE1-EVIDENCE.md`) is pinned as a named regression fixture asserting BOTH identifications survive. The dormant D-13d gap (`w000007`/`w000036`/`w000038` — three catalogued volumes of one multi-volume opus that WOULD collapse under this exact rule if they ever shared a span) is preserved and documented in the function's own docstring, per the decision record's explicit "not an authorized code change" instruction — confirmed via a corpus-wide check that this never currently occurs. 7 behavior tests plus the density/no-web-import/collapse-changes-counts acceptance checks (14 tests total in the file).

### Task 3 (this continuation) — one wording for the rule

Added to `shared/discovery_main_pool.py`:

- **`main_pool_sentence(lang='en'|'he') -> str`** — the single reader-facing sentence for the two-bucket rule, verbatim from `main-pool-rule.md` § "The rule". Contains no `%` and none of the prohibited relation words (`copy of` / `quotes` / `witness of`).
- **`bucket_label(in_main_pool, lang='en'|'he') -> str`** — the two owner-named bilingual bucket labels ("main pool" / "מאגר עיקרי" and "more matches" / "התאמות נוספות"). Its docstring restates, independent of the module docstring, that the second bucket means "not enough evidence for the rule," never "probably wrong."
- **The parity test** (`test_main_pool_sentence_parity_with_help_page`) reads `web/pages/help.py`'s source text with `ast.parse` + `ast.literal_eval` (never `import web`) to extract its `MAIN_POOL_SENTENCE` dict, and asserts `main_pool_sentence('en')`/`('he')` equal it byte-for-byte, printing both values on divergence. A plain substring search was NOT usable here — `MAIN_POOL_SENTENCE`'s value is split across multiple adjacent string-literal lines in help.py's source (Python implicit concatenation), so the merged string is not a contiguous substring of the raw file text; AST-literal-eval extraction reads the file's own source (satisfying the "read, never import" requirement) without executing the module.
- **The standing "no second bucket-membership predicate" guard** (`test_second_implementation_guard_finds_none_on_the_real_tree` + a positive control `test_second_implementation_guard_catches_a_seeded_duplicate`): an AST scan over every `.py` file under `shared/` and `web/` (exempting `shared/discovery_band_labels.py`, the canonical `is_default_eligible` implementation, and `shared/discovery_main_pool.py` itself, already separately proven band-literal-free) for an `ast.Compare` `in`/`not in` node whose comparator — directly, or through a resolved module-level named constant — contains >= 2 confidence-band-name string literals (`tier_a`, `screening_canon`, `weak`, etc.). This is exactly the shape of sketch 003's `confOf()`/`STRONG_BANDS` bug. The scan resolves NAMED constants (not just inline literal sets) because the real historical bug used a named `STRONG_BANDS` set — an inline-literal-only scanner would have missed it; this was caught during this continuation's own first draft, which initially failed to catch a seeded `band in STRONG_BANDS` pattern until the named-constant resolution pass was added (see "Issues Encountered" below). The guard finds zero violations on the real tree today, and the positive control seeds a scratch module under `tmp_path` (never the tracked tree) containing the exact `confOf()`/`STRONG_BANDS` shape, asserts the guard fails on it, then removes the scratch file — the seed-then-observe-failing-then-revert exercise the plan's acceptance criteria ask for.

**What was scanned, for the record:** every `.py` file under `shared/` and `web/` (via `pathlib.rglob`), excluding `shared/discovery_band_labels.py` and `shared/discovery_main_pool.py`. **Pattern:** an `ast.Compare` node with an `In`/`NotIn` operator whose comparator resolves (directly or via a same-file named-constant assignment) to a literal collection containing 2 or more of `{expert_verified, high_confidence_algorithmic, tier_a, screening_rb, screening_canon, corroborated, weak, not_evaluated}`.

## Reconciling the inherited 136-08 stand-in (explicit disposition)

Plan 136-08 landed **before** this plan (`ff46f5e1`/`62be088a`), so `scripts/project_discovery_public.py::_recompute_identification_row` carries an explicitly-labelled **stand-in heuristic** for main-pool bucketing (`human_confirmed > multi-folio > full-coverage(best_band_rank==1) > insufficient_length`) — not the real four-gate rule, and 136-08-SUMMARY.md flags it for 136-07/136-11/136-12 to reconcile.

**Disposition: (b) — this belongs to plans 136-11 and 136-12, and the stand-in's labelling is left untouched.**

Reasoning, checked concretely rather than assumed:

1. **This plan's own objective scopes it out explicitly.** 136-07-PLAN.md's `<objective>` reads: *"Ship the main-pool rule and the display-grouping predicates as pure, fully tested modules — before any build script or render function touches them."* Wiring the real predicate into a build/projection script is stated as future work, not this plan's job.
2. **The frontmatter `files_modified` list is closed and does not include `scripts/project_discovery_public.py`** — only `shared/discovery_main_pool.py`, `shared/discovery_grouping.py`, and their two test files. Touching that script would be out of this plan's declared scope structurally, independent of the objective's own wording.
3. **136-11-PLAN.md already owns exactly this wiring**, with an explicit `key_links` contract: `from: scripts/build_discovery_sidecar.py` `to: shared/discovery_main_pool.py` `via: "the bake calls the shared rule; no second implementation exists in the builder"` `pattern: "discovery_main_pool"`. That is the PRODUCTION bake's own wiring obligation.
4. **136-08-SUMMARY.md itself already names 136-11/136-12** as the reconciling plans, in its own "Next Phase Readiness" section: *"...should reconcile `scripts/project_discovery_public.py`'s documented main-pool-bucketing stand-in against `shared/discovery_main_pool.py` once that module lands."*
5. **A real, concrete blocker confirms this is non-trivial wiring, not a drop-in swap.** `main_pool_decision`'s `Identification` shape needs per-page unresolved-competitor flags (derivable only from `discovery_routing_audit`'s `kept_tie` rows, keyed by page + work) and a per-band `measurement_status`/`ci_low` (from the `band_precision` table via `shared.discovery_band_labels.band_measurement_status`, not a column on `discovery_evidence` itself) — neither of which `scripts/project_discovery_public.py::ProjectionContext` currently reads or joins. Swapping in the real predicate is a genuine data-plumbing task for the build/projection layer, not a one-line import swap — exactly why 136-08-SUMMARY.md flagged it as future work rather than attempting it inline.

**Verified**: `git diff --stat HEAD~N..HEAD -- scripts/project_discovery_public.py` for this continuation's own commits shows **zero changes** to that file — the stand-in's labelling, comments, and behavior are untouched. `tests/test_discovery_visibility.py` and `tests/test_vis01_projection.py` (136-08's own suites, testing that stand-in's STRUCTURAL properties, not its fidelity to the real rule) still pass — 86 tests, no regression (see Verification Results below).

## Task Commits

1. **Task 1: shared/discovery_main_pool.py — the four gates as one pure rule** (prior executor) — `ff46f5e1` (feat)
2. **Task 2: shared/discovery_grouping.py — canonical collapse, lead attribution, granularity separation** (prior executor) — `62be088a` (feat)
   - *Merge of Tasks 1-2 into master-main (prior executor died before Task 3):* `f62f3798`
3. **Task 3: One wording for the rule — main_pool_sentence and its parity assertion** (this continuation) — `8aec0e56` (feat)

**Plan metadata:** this commit (SUMMARY + STATE/ROADMAP) follows separately, below.

_TDD gate note: see "TDD Gate Compliance" below — Tasks 1 and 2 (`tdd="true"`) each landed as a single combined `feat(...)` commit containing both the test file and the implementation, rather than a separate `test(...)` (RED) commit followed by a `feat(...)` (GREEN) commit. Task 3 (not `tdd="true"`) has no such requirement._

## Files Created/Modified

- `shared/discovery_main_pool.py` (331 lines after Task 3; was 238 before) — Task 1's `main_pool_decision` (prior executor) + Task 3's `main_pool_sentence`/`bucket_label` (this continuation)
- `shared/discovery_grouping.py` (255 lines, unchanged by this continuation) — Task 2's `collapse_canonical`/`lead_attribution`/`separate_granularity` (prior executor)
- `tests/test_discovery_main_pool.py` (519 lines after Task 3; was 277 before) — Task 1's 17 tests (prior executor) + Task 3's 5 tests (this continuation, parity/bucket-label/second-implementation-guard)
- `tests/test_discovery_grouping.py` (302 lines, unchanged by this continuation) — Task 2's 14 tests (prior executor)

## Decisions Made

See `key-decisions` in the frontmatter. Most consequential for future plans: the explicit, evidenced disposition that `scripts/project_discovery_public.py`'s main-pool stand-in is **136-11/136-12's** job, not this plan's — recorded above with the concrete data-plumbing gap (`discovery_routing_audit` join, `band_precision`-sourced `measurement_status`/`ci_low`) that makes it real wiring work rather than an import swap.

## Deviations from Plan

None beyond what is documented above as the 136-08 reconciliation (which is a required disposition, not a code change to this plan's own files). Task 3 was executed exactly as specified, including its acceptance criteria's request to "temporarily add a local band-set predicate to a scratch module" and observe the guard fail — done via a `tmp_path`-scoped positive-control test rather than a hand-edited-then-reverted tracked file, which exercises the identical guard-failure behavior without ever risking a stray tracked-tree artifact.

### Auto-fixed Issues

**1. [Rule 1 - Bug] The second-implementation guard's first draft missed the exact historical bug shape it was built to catch**
- **Found during:** Task 3, while writing the positive-control test
- **Issue:** The initial guard scanned only for an inline literal collection (e.g. `band in {'tier_a', ...}`) as the `in`/`not in` comparator. The real historical bug (sketch 003's `confOf()`) used a **named** constant, `STRONG_BANDS = {...}` defined separately from the `band in STRONG_BANDS` check — the exact shape the seeded positive-control test reproduced. The inline-only guard passed the "finds none on the real tree" test but FAILED the positive control (0 violations on a fixture that should have been caught).
- **Fix:** Added `_resolve_named_literal_string_sets`, a first pass over each file's module-level `Assign` nodes (set/list/tuple literals, or `set(...)`/`frozenset(...)` calls over one) building a `name -> literal-string-set` map; the Compare-node scan now resolves a `Name` comparator through this map before checking for >= 2 band-name literals.
- **Files modified:** `tests/test_discovery_main_pool.py` (test-only; no change to `shared/discovery_main_pool.py`)
- **Verification:** Re-ran both the real-tree test (still 0 violations) and the positive control (now correctly fails, i.e. the assertion that the guard "found a violation" now passes) — `python -m pytest tests/test_discovery_main_pool.py -q -k "sentence or parity or bucket or second_implementation"` → 5 passed.
- **Commit:** `8aec0e56` (part of the Task 3 commit — caught and fixed before committing, never landed broken)

**Total deviations:** 1 auto-fixed (Rule 1, test-only, caught before commit). **Impact on plan:** none on production code; the guard is now demonstrably able to catch its own namesake historical bug, which is the whole point of the positive control.

## TDD Gate Compliance

Tasks 1 and 2 both carry `tdd="true"` in the plan frontmatter. Per the plan-level TDD gate-sequence check: `git log` shows Task 1 and Task 2 each as a **single combined `feat(...)` commit** (`ff46f5e1`, `62be088a`) containing both the new test file and the new implementation file together, rather than a separate `test(...)` commit (RED) followed by a `feat(...)` commit (GREEN). No standalone `test(136-07): ...` commits exist for either task.

This is a **process deviation from the RED→GREEN commit-separation convention**, noted here per this executor's own gate-compliance obligation. It is NOT evidence that TDD was skipped substantively — each landed commit contains a fully-passing, non-trivial test suite (17 tests for Task 1, 14 for Task 2) alongside its implementation, and both suites were independently re-verified green by this continuation before Task 3 began. But since the prior executor died mid-plan and cannot be asked, this continuation cannot independently confirm the tests were literally written and run failing BEFORE the implementation, only that both landed together. Recorded as a gate-compliance note, not re-litigated or re-executed (redoing Tasks 1-2 was explicitly out of scope for this continuation).

## Issues Encountered

The second-implementation guard's positive control initially failed to demonstrate a catch (see "Auto-fixed Issues" above) — resolved during Task 3 itself, before any commit, by adding named-constant resolution to the scan. No other issues.

## User Setup Required

None — no external service configuration required.

## Verification Results

- `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_main_pool.py -q -k "sentence or parity or bucket or second_implementation"` → **5 passed** (the plan's own Task 3 verify command).
- `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_main_pool.py tests/test_discovery_grouping.py -q` → **36 passed** (22 + 14; no regression from Task 3's additions).
- `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_main_pool.py tests/test_discovery_grouping.py tests/test_discovery_visibility.py tests/test_vis01_projection.py -q` → **122 passed** (117-test baseline + 5 new Task 3 tests; confirms the 136-08 baseline this continuation was handed still holds).
- `python -m pytest tests/test_no_back_edges_core.py -q` → **41 passed** — neither module introduces a layering violation.
- `python -c "... assert 'density' not in shared/discovery_main_pool.py ... and 'import web'/'from web' not in it"` → OK (both modules).
- `python -m ruff check shared/discovery_main_pool.py tests/test_discovery_main_pool.py` → all checks passed.
- `MASKING_SCAN_PATTERNS_FILE=C:/Genizahsearch/.masking_patterns python scripts/check_atlas_masking.py --scan-asset shared/discovery_main_pool.py` (and the test file) → clean, both.
- `git diff --diff-filter=D --name-only HEAD~1 HEAD` (Task 3's own commit) → no output — no unexpected deletions.
- `git diff --stat -- scripts/project_discovery_public.py` across this continuation's commits → no output — confirms the 136-08 stand-in was left completely untouched, per the disposition above.

## Known Stubs

None. `main_pool_sentence`/`bucket_label` are fully-implemented, real bilingual constants (not placeholders) already consumed by an existing UI surface (`web/pages/help.py`, landed by 136-02) and pinned to it by test.

## Threat Flags

None. This plan's own threat-model register (T-136-07-01 through -05, T-136-07-SC) already covers every trust boundary its files touch (a second drifting implementation, wording paraphrase drift, the `density` trap, non-compensating-gate elevation-of-privilege, and undocumented threshold changes); Task 3's additions are covered by T-136-07-01 (the second-implementation guard) and T-136-07-02 (the wording parity test) exactly as planned. No new files or surfaces outside that register were introduced.

## Self-Check: PASSED

- `shared/discovery_main_pool.py` — FOUND, contains `main_pool_sentence` and `bucket_label`
- `tests/test_discovery_main_pool.py` — FOUND, 22 tests collected
- Commit `ff46f5e1` — FOUND in `git log`
- Commit `62be088a` — FOUND in `git log`
- Commit `8aec0e56` — FOUND in `git log`
- `git diff --stat -- scripts/project_discovery_public.py` (this continuation) — confirmed empty (file untouched)

## Next Phase Readiness

- **Plan 136-11** (`scripts/build_discovery_sidecar.py`, wave 3): must call `shared.discovery_main_pool.main_pool_decision` directly at bake time to populate `discovery_identification.main_pool`/`main_pool_reason` for the REAL (private) asset — its own `key_links` contract already names this. Will need to derive `Identification`'s `page_has_unresolved_competitor` from `discovery_routing_audit`'s `kept_tie` rows and `best_adjudication_status`/`best_measurement_status`/`best_ci_low` from the winning evidence row plus its `band_precision` entry (via `shared.discovery_band_labels.band_measurement_status`).
- **Plan 136-12**: consumes 136-11's wiring; per its own frontmatter, wires the novelty/visibility axes into the same bake pass.
- **Plans 136-14 through 136-18** (panel, `/work/{id}`, `/catalog-browse`, findings page): should call `main_pool_decision`/`main_pool_sentence`/`bucket_label`/`collapse_canonical`/`lead_attribution`/`separate_granularity` directly rather than re-deriving any of this — the standing AST guard in `tests/test_discovery_main_pool.py` will catch a local reimplementation if one is added by mistake.
- **`scripts/project_discovery_public.py`'s stand-in heuristic remains in place**, explicitly reassigned to 136-11/136-12 above — not fixed by this plan. Its own docstring/comment block (added by 136-08) already names 136-07/136-11/136-12; this SUMMARY additionally narrows that to 136-11/136-12 specifically, now that 136-07 has landed and the real module's actual input requirements (routing-audit join, band_precision-sourced measurement_status) are known concretely.
- No blockers.

---
*Phase: 136-read-surfaces-connections-panel-work-witnesses*
*Completed: 2026-08-03*
