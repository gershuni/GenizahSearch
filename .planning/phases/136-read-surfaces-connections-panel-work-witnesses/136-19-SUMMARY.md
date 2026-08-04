---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 19
subsystem: discovery-read-surfaces
tags: [NOVEL-02, VIS-01, PANEL-01, PANEL-02, masking, D-25, DATA-05, flag-on-readiness, render-smoke]

requires:
  - "136-02: tests/render_smoke/discovery_honesty_gate.py -- the shared WORDING gate this sweep complements with a PROVENANCE gate"
  - "136-17: tests/render_smoke/test_panel_render_smoke.py -- the panel capture, re-used rather than rebuilt"
  - "136-18: tests/render_smoke/test_findings_render_smoke.py::capture_rendered_output -- the 48-state findings matrix, which had no caller until now"
  - "136-22: the launch-statistics reader and its no-literals guard"
provides:
  - "tests/render_smoke/test_discovery_masking_sweep.py -- the cross-surface masking sweep over four egress classes, with a positive control per class"
  - "masking_readiness() -- the ONE function that decides whether the D-25 item may be recorded met"
  - "PRODUCTION_DATABASE_SCANS + PRODUCTION_SQLITE_NON_VACUITY -- the executor-run database evidence, with dates and measured numbers"
  - ".planning/phases/136-.../136-FLAG-ON-READINESS.md -- the single artifact the release gate reads"
affects: ["the Phase 139 public flip", "the release gate", "docs/OPEN_ISSUES.md"]

tech-stack:
  added: []
  patterns:
    - "coverage derived from what code CALLS (ui.*, transitively) and checked at LINE granularity against Python's own compiler, never from a list of names someone maintains"
    - "a positive control that asserts the SPECIFIC expected failure -- the path class, the identified file, the nonzero exit -- rather than that the suite went red"
    - "membership against a secret routed through a bool-returning helper, so pytest's assertion rewriting cannot echo it, pinned by an AST check"
    - "a derived-not-remembered scope list (manifests on disk, modules importing a module) that is allowed to FAIL when it finds more than the author remembered"

key-files:
  created:
    - tests/render_smoke/test_discovery_masking_sweep.py
    - .planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-FLAG-ON-READINESS.md
  modified:
    - docs/OPEN_ISSUES.md
    - CHANGELOG.md

decisions:
  - "An unavailable pattern file FAILS the sweep and BLOCKS readiness; masking_readiness() is the single place that decision is made and it refuses every partial outcome. Earlier drafts of this plan allowed a recorded skip while still claiming readiness."
  - "The four positive controls use the REAL patterns read from the pattern file at test time, not a fabricated token: a fabricated needle proves the pipeline, real patterns prove the coverage. The panel suite keeps its fabricated-needle mechanism control for the no-secret case."
  - "The rendered class re-uses 136-17's panel capture rather than rebuilding it -- one capture, one place it is maintained -- so the panel's LINE requirement stays in its own suite and only its FUNCTION coverage is re-asserted here. Recorded as a limit rather than papered over."
  - "The JSON class's scope is derived by parsing every web/ module that imports web.discovery, and the enveloped/non-enveloped split is derived from which functions construct an envelope. Two out-of-scope reads found on the first run are RECORDED in the capture file, not dropped."
  - "The database scans are executor-run and explicitly NOT a CI gate: the artifacts are gitignored and 390-470 MB. The run reports covering zero databases rather than passing quietly over an empty set."
  - "The ruling-T browser-check records discrepancy (CI green, 136-16-SUMMARY says NOT MET) was NOT corrected here. A records change should be made by whoever can verify the CI run, not by an executor quoting a dispatch."

metrics:
  duration: "~2h35m"
  completed: 2026-08-05
  tasks: 2
  commits: 2
  tests_added: 33
requirements: [NOVEL-02, VIS-01, PANEL-01, PANEL-02]
---

# Phase 136 Plan 19: The Cross-Surface Masking Sweep and the Flag-On Readiness Attestation

**No restricted-corpus name is reachable from any surface, payload, copy path or error path — proved
by a sweep that has been watched failing on each of those four classes with the class named — and the
two items gating the public flag-on are complete with their evidence. The sweep found three real
coverage gaps and one leak in itself, and the attestation says plainly where the phase's evidence
does not reach.**

## Task commits

| Task | Commit | What landed |
|---|---|---|
| 1 | `f2191c59` | `tests/render_smoke/test_discovery_masking_sweep.py` — 33 tests over four egress classes, four positive controls, the executor-run database record |
| 2 | `4a1b434f` | `136-FLAG-ON-READINESS.md`, the `docs/OPEN_ISSUES.md` entries, the flag-gated `CHANGELOG.md` entry, and the two changelog checks that pin it |

## What this sweep adds beyond the per-surface scans (the acceptance criterion asks for this explicitly)

| Already covered by 136-17 / 136-18 | Added here |
|---|---|
| The PANEL's rendered output, envelopes and six error paths, line-checked, with a fabricated-needle mechanism control (`test_panel_render_smoke.py`) | Re-used, not rebuilt. Its FUNCTION coverage is re-asserted; its LINE requirement stays in its own suite |
| The findings page's markup / envelope / error-path HONESTY gate — wording, 361 gate calls | Nothing; that is a different property (wording vs provenance) |
| `capture_rendered_output` — the 48-state findings matrix | **Its first caller.** 136-18 wrote it and ran the scan BY HAND; nothing called it, so on a clean checkout the findings surface had no masking gate that could run itself |
| — | **Click- and change-driven rendering.** Neither surface's handlers had ever been driven for masking. The findings selects register CHANGE handlers, so even a click sweep would have missed them |
| — | **Line-level coverage of both findings modules**, derived from `ui.*` call closure |
| — | **The JSON class as an ENUMERATED set**, derived from the code |
| — | **The copy/export class.** Neither suite scanned a link target or asserted the absence of a clipboard path |
| — | **A cross-surface error-path pass** including forced REAL log lines |
| — | **The database class**: six sidecars, both modes, with a non-vacuity proof |

## Measured

### The four classes (clean run, `--strict --scan-repo --scan-asset <capture dir>`, exit 0)

| Class | Size | Coverage measured |
|---|---|---|
| rendered | **2,213,790 chars** | all **48** findings state combinations asserted present BY NAME (2 langs × 4 service states × 3 units × 2 buckets); **every executable line** of every UI-emitting function of `findings_rows.py` and `findings.py`, 5 non-painting exemptions; the panel's 12 UI-emitting functions |
| json-payloads | **23,063 chars** | **9** enveloped reads, each dumped raw-UTF-8 AND `\uXXXX`-escaped |
| copy-export | **470 chars** | **1** link target (`/browse?sys_id=…`); 9 clipboard/download/export APIs each recorded ABSENT, derived from source |
| error-paths | **27,566 chars** | 6 panel modes + 9 findings modes + forced real log lines + rendered error states, both languages |

`python scripts/check_atlas_masking.py --scan-repo` exits 0 after the run — the captures are written
under `tmp_path_factory`, outside the working tree, and a fixture-time assertion fails closed if a
`--basetemp` ever put them inside it.

### The four positive controls — what each raised

Each assembles a directory in which ONLY its class is seeded, with the other three taken from the
clean capture. The needle is the longest real pattern, read from `MASKING_SCAN_PATTERNS_FILE` at test
time.

| Control | The needle's real path into the class | Asserted |
|---|---|---|
| `rendered` | a work title seeded on the claim/row source, rendered by the shipped renderers | exit **1**, `class-1-rendered.txt` named, the other three files NOT named, the pattern never echoed |
| `json-payloads` | the same title inside a serialized envelope | exit **1**, `class-2-json-payloads.txt` named, others not |
| `copy-export` | a `sys_id` seeded into `/browse?sys_id=…`, i.e. a real link target a reader can copy | exit **1**, `class-3-copy-export.txt` named, others not |
| `error-paths` | a `page_id` interpolated into `web/discovery.py`'s degraded-path **log line**, plus a rendered malformed row | exit **1**, `class-4-error-paths.txt` named, others not |

A fifth control (`test_CONTROL_the_error_path_marker_really_reaches_a_LOG_LINE`) asserts the error
class specifically on the LOG egress, because an error class demonstrated only on a rendered state
would leave the two egresses that never pass a renderer unproven. `test_the_panel_message_half_of_the_error_class_carries_no_artifact_VALUE`
asserts the complementary property — the panel model refuses a malformed claim by CODE and FIELD
NAME and never interpolates a value — so "the needle could not be routed here" is distinguished from
"nobody tried".

### The databases — six, both modes, all clean, and shown non-vacuous

| Database | What it is | Result | Seconds |
|---|---|---|---|
| `discovery-v1-e9365edc…` | the DEPLOYED public artifact (`manifest.deploy.json`) | clean | 48 |
| `discovery-public-136rebuild.db` | the public projection of the private rebuild | clean | 49 |
| `discovery-v1-136rebuild.db` | the private rebuild | clean | 55 |
| `discovery-v1-33499c5b…` | what the repo's own `manifest.json` resolves, i.e. what a LOCAL run serves | clean | 50 |
| `discovery-v1-89dfa444…` | a superseded bake, still resolvable via `manifest.v1-89dfa.backup.json` | clean | 47 |
| `discovery-v1-8e434513…` | a superseded bake, still resolvable via `manifest.v1.json` | clean | 46 |

All six with `--strict --scan-repo --scan-asset <db> --scan-sqlite <db>`. **The last three were found
by the manifest-derived check, not by recollection** — the executor scanned three, the check named a
fourth, the derivation was widened from three remembered filenames to every `manifest*.json` on disk,
and it named two more.

**The clean runs are shown NON-VACUOUS.** A 48-second clean walk over 14.3M cells (6.8M of them
TEXT/BLOB) is exactly what a walk that never happened also looks like. Values read OUT of the
deployed artifact and fed back as the whole pattern set:

| Needle taken from | exit | hits | seconds |
|---|---|---|---|
| `manuscript_display.sys_id` | 1 | **11** | 32 |
| `discovery_evidence.novelty_status` (a deep column of the 46-column table) | 1 | **26,480** | 32 |
| a TABLE NAME (`witness_unit_members`) | 1 | **1**, reported on `::schema` only | 32 |

The third is the one that matters separately: it proves the schema/identifier pass is real and
distinct from the row walk.

**`--scan-asset` alone is proved INSUFFICIENT by construction**, not by quoting the scanner's
docstring. With `page_size=512`, a value straddling SQLite's local→overflow boundary or an
overflow-page boundary is not contiguous in the file: at offsets 515 / 522 / 529 the byte scan
reports **0** and the cell scan reports **1**. The test carries its own false-positive control —
at offsets 100 / 610 / 1120 the needle is contiguous and BOTH scans see it, so a broken byte scan
cannot manufacture the gap.

## The three real gaps this sweep found in the existing work

1. **The findings surface had no automated masking gate.** `capture_rendered_output` had no caller.
2. **Neither surface's click-painted output had ever been scanned.** The facet tree and its leaves,
   the unit and sort selects, the pager, the bucket control, the candidacy switch, the launch
   outage, the honest empty state, the approximate-total note, the blocked facet state and four row
   shapes — all paint artifact-derived text; none had been in any capture. Closing it needed a
   CHANGE-handler driver as well as a click driver, because the selects register their handler as a
   value-change handler and a click sweep alone still misses them.
3. **Three of six manifest-named databases had never been scanned in this phase.**

## Deviations from plan

### Auto-fixed issues

**1. [Rule 1 — Bug] The sweep leaked the restricted pattern into its own test log.**

- **Found during:** Task 1, running the sweep's own mutation battery (mutation M4, "stop seeding the
  error-path class").
- **Issue:** `assert needle in seeded_text` is rewritten by pytest to print BOTH operands, so the
  first control to go red published the restricted corpus name — and ~2 MB of captured surface —
  into the pytest output. **A masking test that leaks the thing it tests for, on its way to
  reporting a leak, is the worst possible shape for that file.** Nothing about the line looks wrong.
- **Fix:** membership routed through `_contains()`, which returns a bool (`assert survived` prints
  `False`), at all four sites; plus `test_no_assertion_in_this_module_can_ECHO_the_needle`, which
  walks the module's AST and refuses any `assert` whose test expression references a needle-bearing
  name. Structural, not careful.
- **Files modified:** `tests/render_smoke/test_discovery_masking_sweep.py`
- **Commit:** `f2191c59`

**2. [Rule 2 — Missing critical coverage] The pattern-file fallback would have turned a broken CI
secret into a green run.** `_configured_pattern_file()` originally fell back to the local
`.masking_patterns` whenever the env var did not resolve. An env var that is SET but names no
readable file is a misconfiguration — the shape a broken secret takes — and now fails by name
instead of falling back to the one machine that happens to hold a copy.

**3. [Rule 2 — Missing critical coverage] Two changelog checks added.** The plan's acceptance
criterion requires "a check asserts" that no launch figure reaches `CHANGELOG.md`. Added
`test_no_launch_figure_reaches_the_CHANGELOG` (forbidden set DERIVED from the attestation's own
recorded table, so a rebake moves it, with the derivation asserted non-empty so a failed parse
cannot pass vacuously) and `test_the_changelog_says_flag_gated_and_claims_no_launch`.

### Deliberately NOT fixed, reported instead

**The ruling-T browser check reads NOT MET while CI reports it green.** `136-16-SUMMARY.md` records
criteria (e)/(f) as NOT MET; the `findings-browser-check` job passed for the first time on 2026-08-04
(CI run 30931268195). `test_the_findings_deploy_is_blocked_until_the_browser_check_is_recorded_MET`
reads the SUMMARY, not CI, so the deploy gate currently reads BLOCKED on a criterion that is met.
Not corrected here: a records change should be made by whoever can verify the CI run, not by an
executor quoting a dispatch. It fails in the safe direction. Recorded in the attestation § 3 (G5)
and in `docs/OPEN_ISSUES.md`.

**`shared/discovery_display_strings.py` puts stored vocabulary on exception messages** (reported by
the 136-18 executor). Outside this plan's file set; still open in `docs/OPEN_ISSUES.md`. Not a D-25
breach — these are public relation types — but the same class of leak, and
`web/components/discovery_panel.py::_render_expansion_envelope` does not catch it.

## Mutation evidence — 15 mutations, every one watched RED by name, then reverted

Restored from a `cp` backup after each; no `git checkout`, no `git stash`, no `git clean`.

| # | Mutation | The check that went red |
|---|---|---|
| M1 | the rendered class returns "" | `..._captured_and_none_is_a_stub` — "the rendered capture is 0 chars, under its 200000 floor" |
| M2 | value-change driving removed | the LINE test named both selects and all ten lines |
| M2b | the 48-state matrix dropped | `..._every_state_unit_bucket_and_language` listed all 48 missing combinations |
| M3 | one enveloped payload dropped | named `get_work_expansion_enveloped` **and the module that calls it** |
| M4 | the error-path class not seeded | the `[error-paths]` control: "the planted marker did not survive" |
| M5 | the copy/export inventory claims a false absence | named the API and all four surface modules |
| M6 | `MASKING_SCAN_PATTERNS_FILE` unset (env, not code) | 5 errors + the real-scan test; **26 passed, 5 errors — the suite is RED, never a skip** |
| M7 | a `pytest.skip` introduced | the AST check named the construct and its line |
| M8 | a recorded db scan drops `--scan-sqlite` | "…was not scanned with --scan-sqlite" |
| M9 | a PAINTING line added to the exemption list | "exempts a line that PAINTS" |
| M10 | the needle leaks into the CLEAN payload capture | "the UNSEEDED ['json-payloads'] capture(s) already carry the marker — the control is inert" |
| M11 | no page overflow, so no straddle exists | "no offset was found at which the raw byte scan misses…" |
| M12 | a needle-echoing assertion reintroduced | named the line and the offending name |
| M13 | the sqlite identifier scan asked for a name that is not there | "a restricted table NAME was not reported" |
| M14 | a launch figure added to `CHANGELOG.md` | named the figure |
| M15 | the attestation's launch-figure heading renamed | named the missing heading and forbade a fallback list |

> **M11 is worth recording.** The first attempt at it (offsets 0–4) came out GREEN, and the obvious
> conclusion — that the straddle control is inert — was **false**: offsets 3–29 straddle the
> local→overflow boundary, a different boundary from the one at 515. The mutation, not the test, was
> wrong. The straddle test now carries its own contiguous-offset control so this is visible.

## Verification

| check | result |
|---|---|
| `pytest tests/render_smoke/test_discovery_masking_sweep.py -q` | **33 passed** |
| `pytest tests/render_smoke/ -q` | **450 passed, 1 skipped** (3:03) |
| `pytest tests/ -k discovery -m "not render_smoke" -q` | **1,580 passed, 8 skipped** |
| `check_atlas_masking --scan-repo` (pattern file SET) | **clean**, exit 0 |
| `--strict --scan-repo --scan-asset --scan-sqlite` × 6 databases | **all clean** |
| `PYTHONUTF8=1 python scripts/check_docs.py` | exit **0** |
| `ruff check` on the new test module | clean |
| no launch figure in `CHANGELOG.md` | asserted, and the assertion mutation-proved |

## Acceptance criteria — MET / NOT MET

### Task 1 — ALL MET

All four path classes covered and reported; rendered output captured for both surfaces, both
languages, every service state, every row unit and both buckets (all 48 asserted by name); the JSON
class covers every enveloped read this phase ships including the launch statistics, with a derived
enumeration so a later-added envelope is visible as a gap (it found two on its first run); error
paths exercised for 15 failure modes across both surfaces with messages, logs and rendered states all
scanned; four positive controls, one per class including error paths, each asserting the specific
expected failure; the seeded marker generated at test time from the pattern file and `--scan-asset`
over this module confirming no restricted name is hardcoded in it; the capture path outside the
working tree with `--scan-repo` exiting 0 afterwards; an unavailable pattern file FAILS the sweep and
`masking_readiness` records the item NOT MET (three tests); every database scanned with BOTH modes,
and a `--scan-asset`-only run proved insufficient by construction; the absence of a copy/export path
asserted and recorded rather than assumed; and this summary states what 136-17/136-18 already covered
and what this pass adds.

### Task 2 — MET, with two criteria answered honestly rather than affirmatively

MET: one row per owned ROADMAP criterion with its evidence artifact by path; both flag-on gating
items marked complete with evidence, the masking item only because the pattern file was available and
both scan modes ran; rulings R/S/T/U each dispositioned with where they are implemented and which
test pins them; the production launch figures recorded with their sidecar version and audience and
stated as artifact-scoped; no launch figure in `CHANGELOG.md`, asserted by a check; the "still
gating" section names **six** items each with its owner phase and resolves none; the "known and
accepted" section names the containment residue verbatim from the methods page, the review-badge
provenance question and the plain-text work titles; `docs/OPEN_ISSUES.md` carries five new dated
entries; the changelog entry says flag-gated and hidden and claims no launch; `check_docs.py` exits 0.

**Answered honestly rather than affirmatively:**

- **"Any public read path 136-20's end-to-end audience test could not yet exercise is listed."** The
  honest answer is *all of them at the level that matters*: 136-20 proves the audience boundary in
  tests against a private-audience artifact, and the browser check proves one control in CI against a
  synthetic fixture, but **no public read path has ever been exercised against a flag-ON production
  box, because the flag has never been on.** Recorded as attestation § 6.6 and named as the single
  largest gap.
- **Criterion 5 is recorded PARTIALLY MET.** `/catalog-browse` integration and neutral-title search
  both moved to Phase 136.1 and the ROADMAP criterion text was never updated. Recorded as a records
  discrepancy rather than counted as met.

## Bookkeeping NOT done — what needs ticking

Per the dispatch, `STATE.md`, `ROADMAP.md` and `REQUIREMENTS.md` were deliberately not edited.

- **`STATE.md`**: plan 22 of 22 complete; wave 11 closed; `136-19` done. Note that
  `state advance-plan` would write an untrustworthy counter after this phase's out-of-sequence
  parallel waves — the same reason 136-17/18/21/22 left it alone.
- **`ROADMAP.md`**: tick `136-19-PLAN.md`. **Do not run `roadmap update-plan-progress 136`
  blindly** — it ticks the halted `136-09` as a side effect (recorded by two earlier executors).
  Criteria 1, 2, 6, 7 and 8 are met; criterion 5 is PARTIALLY met and its text needs the 2026-08-02
  re-scope folded in (the `/catalog-browse` and neutral-title-search clauses moved to 136.1);
  criteria 3 and 4 belong to 136.1.
- **`REQUIREMENTS.md`**: **NOVEL-02 and VIS-01 are now complete** — NOVEL-02's masking requirement
  ("including copy/clipboard output, JSON payloads, and error paths") is satisfied and evidenced by
  this sweep. **PANEL-01 and PANEL-02 are contributed to by this plan and completed by 136-15/17/18/21
  taken together**; whoever ticks them should tick them for the phase, not for this plan.
- **Not ticked, on purpose:** the ruling-T browser-check record in `136-16-SUMMARY.md` (see
  Deviations).

## Known stubs

None. No hardcoded empty value, placeholder string or unwired component was introduced.

## Self-Check: PASSED

- `tests/render_smoke/test_discovery_masking_sweep.py` — FOUND (1,984 lines)
- `.planning/phases/136-.../136-FLAG-ON-READINESS.md` — FOUND (256 lines)
- `docs/OPEN_ISSUES.md` — FOUND (modified)
- `CHANGELOG.md` — FOUND (modified)
- commit `f2191c59` — FOUND
- commit `4a1b434f` — FOUND
