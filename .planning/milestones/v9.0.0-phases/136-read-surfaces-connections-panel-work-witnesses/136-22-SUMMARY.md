---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 22
subsystem: discovery-read-surfaces
tags: [ruling-U, launch-statistics, no-literals-guard, provenance, envelope]
requires:
  - "136-14: make_envelope / _ALL_ALLOWLISTS / _assert_surface_safe"
  - "shared.discovery_novelty.NOVELTY_STATUSES (the closed shade vocabulary)"
  - "web.discovery_assets: _PUBLIC_LOADER_AUDIENCE / _REQUIRED_TABLES (the loader gates)"
provides:
  - "DiscoveryService.get_launch_stats_enveloped (+ _async) — the version-aware, artifact-backed launch reader"
  - "web.discovery.get_launch_stats_enveloped — THE only supported accessor for any launch figure"
  - "SURFACE_LAUNCH_SHADE_FIELDS + surface_safe_launch_shade, registered in _ALL_ALLOWLISTS"
  - "LAUNCH_CONTRIBUTION_SHADES — the frozen three-shade tuple 136-17 classifies items[*].shade against"
  - "_assert_allowlist_safe — the import-time allowlist guard, now callable by a test"
  - "the repo-level no-literals guard, its committed forbidden list and its exemption rules"
affects:
  - "136-18 (renders these numbers into the slot 136-16 reserved; owns the SENTINEL provenance test)"
  - "136-17 (wave 9; classifies items[*].shade against LAUNCH_CONTRIBUTION_SHADES)"
tech-stack:
  added: []
  patterns:
    - "cache keyed on (resolved path, sidecar_version), path resolved BEFORE the lookup"
    - "defensive copy on every cached read, because this read bypasses the outer LRU"
    - "provenance read from the artifact's own meta table, never from an injected provider"
    - "scope proved by exact set equality against an expansion the test computes for itself"
    - "each forbidden-list half proved CONSUMED by a tagged sentinel through its own loader"
key-files:
  created:
    - tests/test_discovery_launch_stats.py
    - tests/fixtures/discovery/launch_figures.json
  modified:
    - shared/discovery_service.py
    - shared/discovery_surface_projection.py
    - web/discovery.py
decisions:
  - "The wrapper passes NO cache_name and passes _findings_timeout() explicitly; four independent detectors fire if that is ever undone."
  - "Provenance (sidecar_version, audience) is read from the artifact's meta table rather than from the injected provider, so a number always carries the provenance of the artifact that produced it."
  - "The floor-module assertion is split: pattern coverage for all eight (rule-level), filesystem membership for the six that exist — two floor modules are created by later plans and cannot be in a glob expansion yet."
  - "The runtime freshness comparison skips when NO artifact resolves at all; it was RUN against the deployed public projection and matched. All three freshness failure controls run unconditionally against a synthetic loader-passing artifact."
metrics:
  duration: "~4h"
  completed: 2026-08-04
  tasks: 2
  tests: 227
requirements: [NOVEL-01, NOVEL-02]
---

# Phase 136 Plan 22: Launch Statistics Reader and the No-Literals Guard — Summary

Ruling U's headline contribution figure and its three shades are now computed from the artifact
being served, on the single basis `main_pool = 1`, decomposing exactly, carrying the sidecar version
and audience that produced them — and no launch figure can enter the codebase as a literal, in any
form the scanner enumerates, without a named test failure pointing at the file, the line and the
accessor to use instead.

## What was built

**Task 1 — the reader** (`shared/discovery_service.py`, `shared/discovery_surface_projection.py`).
`get_launch_stats_enveloped()` returns the four-key envelope: `items` is one row per contribution
shade in the frozen ruling-U order, `total` is *the sum of those rows* (not a separately counted
number, so a total its shades do not reproduce cannot exist), and `meta` carries the basis, the
provenance and the context figures. Both contribution figures come from one grouped statement in two
shapes — with and without the `main_pool` predicate — so the main-pool and all-bucket numbers cannot
drift apart through an edit to one of them.

**Task 2 — the wrapper and the guard** (`web/discovery.py`, `tests/test_discovery_launch_stats.py`,
`tests/fixtures/discovery/launch_figures.json`). The async wrapper passes no `cache_name` and passes
`_findings_timeout()` explicitly. The guard scans a glob-derived source set (59 modules) plus the
translation table for string literals, numeric AST constants, formatted expressions and
constant-folded arithmetic, against a committed 23-value forbidden union.

## Measured figures — and the artifact that produced them

Read through the shipped reader from `discovery-v1-e9365edc…db`
(`meta.audience = public`, `meta.sidecar_version = discovery-v1-real`,
`content_hash = e9365edcab27af7d0739ab1a07b1a187683993bcbff41ff88128c8fe4fbb7181`):

| envelope key | value |
|---|---|
| `total` | 9,523 |
| `items[0]` `fills_gap` | 4,152 identifications / 3,666 manuscripts |
| `items[1]` `refines_granularity` | 3,873 / 2,101 |
| `items[2]` `container_predicts` | 1,498 / 995 |
| `meta.main_pool_manuscript_count` | 6,755 |
| `meta.all_bucket_total` | 17,536 |
| `meta.all_bucket_manuscript_count` | 10,959 |
| `meta.corpus_manuscript_count` | 38,431 |
| `meta.corpus_page_count` | 177,402 |

4,152 + 3,873 + 1,498 = 9,523 — the decomposition holds on the real artifact, and reproduces ruling
U exactly. **These are properties of that one artifact, not project constants.** The private rebuild
answers the same query 10,432 over 7,563 manuscripts while reporting the *identical*
`sidecar_version` string, which is why the cache key carries the path.

**Latency:** 324.0 ms cold, 0.019 ms warm (cache hit), against the findings-page p95 budget of
1500 ms (`docs/specs/discovery-budgets.md` §4.4). Well inside budget.

## Criteria: MET / NOT MET

All acceptance criteria for both tasks are **MET**, with three qualifications recorded below rather
than softened.

**Task 1** — 227 tests pass (`tests/test_discovery_launch_stats.py`), covering all eleven listed
behaviours, the decomposition identity on fixture *and* real artifact, the main-pool basis proved by
a fixture where it excludes rows the all-bucket figure includes, zero-fill for absent shades, the
import-time shade validation, the fourth-shade closure, the path-switch at constant version, the
version flip at constant path, cache isolation, `meta` provenance, three outage tests, allowlist
registration, and the no-precision sweep.

**Task 2** — the wrapper's four-key envelope on `ok` and outage, the unavailable/busy/timeout
mappings, the path switch **through the public async wrapper**, the `cache_name` control, the
timeout spy, exact glob set equality with a glob-drop control, exact key-set completeness, 161
parameterized figure × form controls, three folding shapes, folder bounding, the docstring limit
statement, three placement controls, both consumption sentinels, three freshness controls, four
exemption rejection controls and the admissibility control.

**Qualification 1 — the floor-module assertion is split, and this is a finding, not a softening.**
Two of the eight named floor modules — `web/components/findings_rows.py` and
`web/components/discovery_panel.py` — **do not exist yet**; later plans in this phase create them. A
module that does not exist cannot appear in a filesystem glob expansion, so the criterion's literal
"the named floor is a SUBSET of the derived set" is unsatisfiable for those two today. The test
therefore asserts *both*: (a) all eight match at least one of the four glob patterns — the
rule-level property, independent of the filesystem, which is what "in scope by the rule" actually
means; and (b) the six that exist on disk are in the derived set. (a) is strictly stronger than a
membership check for the absent two, because it holds the moment they land.

**Qualification 2 — the runtime freshness comparison skips when no artifact resolves at all.**
The three *controls* the criterion names (mutated value, nonexistent path, gate-failing artifact)
run unconditionally in every environment, because they build a synthetic artifact that passes the
same audience and required-table checks the public loader applies. Only the comparison of the
committed file against *the artifact actually being served* needs a real one. On this machine the
repository manifest selects a pre-rebuild asset the loader refuses, so that comparison was **RUN
under `DISCOVERY_LAUNCH_GUARD_DB`** pointed at the deployed public projection: it matched, and its
key set equalled the envelope's numeric key set. With the artifact present the whole file is
`236 passed, 0 skipped`. This is recorded as a qualification, not as a met-by-skip.

**Qualification 3 — the `cache_name` mutation control exists in two forms.** As required, I mutated
the shipped wrapper to re-add `cache_name=` and observed the wrapper-level path-switch assertion
fail (below). I *additionally* made it a standing automated test that drives
`_enveloped_off_loop(..., cache_name=…)` directly and asserts it serves the stale artifact, so the
detector survives after the manual mutation is reverted.

## Mutations run, and what failed

Every control was mutated and watched. Production files were restored from `cp` backups I made
myself; `git checkout --` was never used.

| # | Mutation | Result |
|---|---|---|
| i | cache key reduced to the version alone | path-switch test FAILED (served total 6 for an 8-row artifact); version-flip test still passed |
| ii | stale-key ordering (`_band_measurements`'s defect: cached path attribute read for both lookup and store) | path-switch test FAILED on the FIRST post-switch call |
| — | warm-up call removed from the path-switch test, mutation ii still applied | test PASSED — proving the warm-up is load-bearing |
| F | `SURFACE_LAUNCH_SHADE_FIELDS` unregistered from `_ALL_ALLOWLISTS` | registration test FAILED |
| G | shades with no rows omitted instead of zero-filled | zero-count test FAILED |
| H | cached envelope returned by reference on a HIT | cache-isolation test **PASSED** → see "defects I found in my own tests" |
| I | failed query returns `ok` with a zero total | dropped-table outage test FAILED |
| J | total counted separately from the shades | decomposition + basis tests FAILED |
| K | `main_pool` predicate dropped from the headline query | basis, all-bucket and context tests FAILED |
| L | a percentage string added to `meta` | refused at TWO levels — `_assert_surface_safe` raised, and the value sweep FAILED |
| M | `contribution_share` added to the shade allowlist | quality-field test FAILED |
| — | `cache_name=` re-added to the shipped wrapper | FOUR tests FAILED: the public-wrapper path switch, the stale-artifact demonstration, the AST pin, **and the independent timeout spy** (because `_browse_cached_call` hardcodes the browse timeout) |
| A | scanner reduced to string literals only | 93 controls FAILED, including every `str(<figure>)` case |
| B | constant folding removed | 26 controls FAILED, all three folding shapes among them |
| C | translation-table scan removed | translation placement control FAILED |
| D | position classifier always reports "not display-reachable" | exemption rules 1 and 2 controls FAILED |
| E | blanket ban on current figures (the rule round 9 withdrew) | **admissibility** control FAILED — the relaxation is real |
| — | union ignores the ARTIFACT-DERIVED half | caught ONLY by that half's sentinel; empty-half control passed |
| — | union ignores the HISTORICAL half | caught ONLY by that half's sentinel; empty-half control passed |
| — | one glob dropped from the SCANNER's pattern tuple | set-equality control FAILED, naming exactly the dropped glob's expansion |

The last two rows are the round-10 finding-5 prediction confirmed empirically: **emptying a half
proves the half is valid, never that it is consumed.** Both half-ignoring mutations left the
standalone non-emptiness assertions and the empty-half control green.

## Defects I found in my own tests

Three tests could not fail. Each was found by running a mutation, not by reading.

1. **The cache-isolation test (mutation H).** It vandalised the result of the *first* call — which
   MISSED the cache and was therefore already a fresh object — so an implementation returning its
   stored envelope by reference on a cache HIT passed untouched. Fixed to vandalise the cache-HIT
   result as well and check a third call; the mutation now fails it. Committed separately as
   `752c588b` so the defect and its fix are legible.

2. **The path-switch test's warm-up.** With a single pre-switch call, the faithful stale-key
   mutation *passes*: the first call keys on `(None, None)` because `self._last_path` is unresolved,
   so the answer is stored under a key the post-switch call never asks for. Two pre-switch calls are
   required. This is precisely the "an ordinary two-call test would miss it" case the plan named;
   the reason is now written into the test so the call is not removed as redundant.

3. **The glob-drop and docstring controls used the wrong module object.** `tests/__init__.py` does
   not exist, so `import tests.test_discovery_launch_stats` builds a *second* module object and a
   `monkeypatch` lands on the copy rather than on the code under test. All three such sites now use
   `sys.modules[__name__]`. The historical-sentinel test had been passing through the duplicate
   module — a pass for the wrong reason.

A fourth, smaller one: `_tracked_fingerprint()` originally derived its file list from
`scanner_scanned_paths`, i.e. from the thing under test, so it changed shape under the glob-drop
patch and reported a phantom edit. It now uses the test's own expansion.

## Deviations from plan

**1. [Rule 3 — blocking] The floor-module subset assertion, split into a rule-level and a
filesystem-level check.** Cause and reasoning in "Qualification 1" above. Without this the criterion
is unsatisfiable against HEAD.

**2. [Rule 1 — bug] The stale-path mutation had to be sited above `is_available()`.** A two-line
swap adjacent to the cache lookup is a *no-op*, because `is_available()` itself calls `_get_conn()`
and has already refreshed `_last_path` by then. I verified this (the mutation passed), then
reproduced the faithful `_band_measurements` shape — stale key used for both lookup and store,
computed before any resolution — which does fail. Recorded because a future reviewer re-running the
control the obvious way will see it pass and conclude wrongly.

**3. [Rule 2 — missing critical functionality] `_assert_allowlist_safe` factored out of the
import-time loop.** The criterion requires a test that "seeds a forbidden name into a copy of the
allowlist and asserts the import-time guard rejects it". The guard was an inline loop body, so a
test could only have re-implemented it or exercised `is_forbidden_surface_field` underneath it —
either of which passes while the loop skips the allowlist under test. The check is now a function
the loop calls and the test calls, plus an AST assertion that the module-level loop calls *that*
function.

**4. Scanner and guard live in the test module.** `files_modified` permits five files and none of
them is a new production module, so the scanner, its forbidden lists and its exemption rules are in
`tests/test_discovery_launch_stats.py`. The test's independent glob list is a separate literal tuple
in the same file; the glob-drop control confirms the two are independent computations rather than
one list read twice.

## Guard state

- Scanned: **59 modules** from four globs, plus `genizah_translations.py`.
- Forbidden union: **23 figures** — 12 from the committed artifact-derived half (every numeric value
  the envelope exposes, each carrying `sidecar_version`, `audience` and `content_hash`), 11 from the
  committed historical half (each with a written reason naming what it retires).
- `13,285` — the corrected mixed-basis figure — is in the list, named as such, and asserted present.
- Violations on the real tree: **0**. Shipped exemptions: **0** (the expected state, asserted).

## Ruling U compliance

- **Constraint 1 (one basis, stated):** every headline count is `main_pool = 1`; `meta.basis` says
  so; the all-bucket figures live on their own named keys and are never merged into `total`.
- **Constraint 2 (read from the artifact, never hardcoded):** the whole plan.
- **Constraint 3 (no precision anywhere):** nothing the reader emits is a percentage, ratio,
  interval, accuracy rate or review badge; the allowlist names no such field; a value sweep and the
  envelope's own recursive rate/interval detector both cover it, and mutation L confirmed both fire.
- **Constraint 4 (match-framing):** the shade key is the stored vocabulary value and is never
  rendered raw; labels stay in `shared/discovery_display_strings.py`. No label was written here.

## Self-Check: PASSED

- `shared/discovery_service.py` — FOUND, contains `main_pool`, `get_launch_stats_enveloped`
- `shared/discovery_surface_projection.py` — FOUND, contains `SURFACE_LAUNCH_SHADE_FIELDS`
- `web/discovery.py` — FOUND, contains `get_launch_stats_enveloped`
- `tests/test_discovery_launch_stats.py` — FOUND, 2,204 lines (min 160), 227 tests
- `tests/fixtures/discovery/launch_figures.json` — FOUND, contains `sidecar_version`
- Commits `01d8f959`, `2f641ce4`, `157f835d`, `752c588b` — all FOUND in `git log`
- `MASKING_SCAN_PATTERNS_FILE=… python scripts/check_atlas_masking.py --scan-repo` — exit 0, clean
- `python -m ruff check` on all four source files — clean

## Needs ticking by the orchestrator

I did not touch `STATE.md`, `ROADMAP.md` or `REQUIREMENTS.md`, as instructed.

- Phase 136 wave 8 complete — 136-22 is the only plan in it.
- Requirements **NOVEL-01** and **NOVEL-02** satisfied by this plan.
- Plan counter: 136-22 done (19 of 22 by plan number; wave 8 of the wave order).

## For downstream plans

- **136-18** renders these numbers from `web.discovery.get_launch_stats_enveloped()` into the region
  136-16 reserved. It owns the SENTINEL provenance test, which this module's docstring names as the
  mechanism covering the scanner's stated limit — a test asserts that naming, so removing 136-18's
  test as "redundant with the scanner" will fail here.
- **136-17** (wave 9) classifies `items[*].shade` against `LAUNCH_CONTRIBUTION_SHADES`
  (`shared.discovery_service`), a frozen three-value snake_case tuple validated against
  `NOVELTY_STATUSES` at import. `SURFACE_LAUNCH_SHADE_FIELDS` is registered in `_ALL_ALLOWLISTS`.
- **Any plan that rebuilds the artifact** must regenerate the committed figure file, or the freshness
  check fails naming the command:
  `DISCOVERY_LAUNCH_GUARD_DB=<sidecar.db> python -c "from tests.test_discovery_launch_stats import regenerate_committed_figures as r; r()"`
