---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 18
subsystem: discovery-read-surfaces
tags: [NOVEL-01, NOVEL-02, PANEL-02, ruling-U, ruling-T, ruling-R, findings-page, honesty-gate, render-smoke, PERF-01]

requires:
  - "136-16: web/pages/findings.py -- the page shell, the RESERVED headline slot and its marker-class contract"
  - "136-22: web.discovery.get_launch_stats_enveloped + SURFACE_LAUNCH_SHADE_FIELDS + the no-literals guard"
  - "136-17: assert_surface_honesty (SIX detectors) + the CLASSIFICATION partition + assert_envelope_honesty + assert_error_path_honesty"
  - "136-14: _build_findings_query / make_envelope / the SURFACE_* allowlists"
  - "136-10: shared/discovery_display_strings.py + the .gs-discovery CSS block"
  - "136-07: shared/discovery_main_pool.py::bucket_label -- the ONE bucket rule"
provides:
  - "web/components/findings_rows.py -- the launch headline, the three unit row anatomies, the novelty badge and help, the coverage clause, the bucket name"
  - "the filled ruling-U headline on /computed-identifications"
  - "scripts/bench_discovery.py -- the FULL findings combination space, built through the SHIPPED query builder"
  - "docs/specs/discovery-budgets.md §4.4 -- 45 combinations, artifact- and audience-labelled"
  - "tests/render_smoke/test_findings_render_smoke.py -- 155 tests / 359 gate calls over markup, envelopes and error paths"
affects: ["136-19 (the cross-surface sweep)", "the findings production deploy (NOT performed)"]

tech-stack:
  added: []
  patterns:
    - "a SENTINEL fixture as the proof of a data path, covering the shapes a static scan structurally cannot see"
    - "deriving a display clause FROM the shared vocabulary by difference, rather than retyping it"
    - "a benchmark built through the SHIPPED query builder, so probe and service cannot diverge"
    - "asset-fact populations pre-checked into named skips, so the zero-row ABORT keeps meaning 'probe bug'"

key-files:
  created:
    - web/components/findings_rows.py
    - tests/render_smoke/test_findings_render_smoke.py
  modified:
    - web/pages/findings.py
    - scripts/bench_discovery.py
    - docs/specs/discovery-budgets.md
    - tests/test_findings_page.py
    - tests/test_discovery_build.py

decisions:
  - "The coverage clause is DERIVED from shared.discovery_display_strings.row_headline by difference rather than retyped, so the one permitted percentage keeps the exact qualifier the gate recognises and the direct-family gating stays the shared module's decision."
  - "No band tooltip on a findings row: the identification grain exposes best_band_rank and no band label, source or confidence band. Deriving a label from a rank would be a second band vocabulary."
  - "The novelty badge renders for exactly two shades (candidate, not_checked) and for no other. A badge asserts something; its absence asserts nothing, and there is no ratified reader wording for the remaining eight."
  - "The domain facet header now NAMES its axis, so the header assertion is about a claim the markup makes rather than about a word it happens to contain."
  - "The production deploy was NOT performed. Recorded as NOT MET with its reasons."

metrics:
  duration: "~5h"
  completed: 2026-08-04
  tasks: 3
  commits: 4
  tests_added: 155
requirements: [NOVEL-01, NOVEL-02, PANEL-02]
---

# Phase 136 Plan 18: The Completed Findings Page Summary

Ruling U's headline now renders **four numbers on one basis from the artifact
actually being served**, the three shipped row units render with the second
bucket carrying exactly the weight of the first, the whole filter/sort/count
combination space is measured against its versioned caps through the shipped
query builder, and the surface is proved honest across markup, envelopes and
error paths by a suite watched failing on every control in its list — **but the
production deploy was not performed, and the ruling-T launch blocker it depends
on is still recorded NOT MET.**

## Task commits

| Task | Commit | What landed |
|---|---|---|
| 1 | `69060afb` | `web/components/findings_rows.py` + the page wiring + the sibling dispatch-test fix |
| 2 | `5ca6b065` | the full combination space, the measured actuals, four amended sibling tests |
| 3 | `6c4ba715` | the render-smoke suite: 155 tests, 13 positive controls, the FP control, the deploy gate |
| 3 | `963166b9` | a forced log line, so the error-path log scan is not vacuous |

⚠ **Another agent committed to `master-main` between Task 2 and Task 3**
(`2065290c fix(web): mount the nav drawer closed on mobile…`) and pushed, so
`origin/master-main` already carries Tasks 1 and 2. Task 3 is local only. This
is precisely the contended-resource hazard the plan names when it explains the
wave ordering, and it is a second reason the deploy is not mine to perform.

## The measured launch statistics, with their provenance

Read through the shipped reader (`DiscoveryService.get_launch_stats_enveloped`)
from the artifact **production is currently serving** — its SHA-256 matches
`discovery_data/manifest.deploy.json`'s `content_hash` byte for byte:

| | |
|---|---|
| artifact | `discovery-v1-e9365edcab27af7d0739ab1a07b1a187683993bcbff41ff88128c8fe4fbb7181.db` |
| `meta.audience` | `public` |
| `meta.sidecar_version` | `discovery-v1-real` |
| `meta.data_as_of` | `2026-08-03` |
| `meta.basis` | `main_pool` |

| envelope key | value |
|---|---|
| `total` | **9,523** |
| `items[0]` `fills_gap` | 4,152 identifications / 3,666 manuscripts |
| `items[1]` `refines_granularity` | 3,873 / 2,101 |
| `items[2]` `container_predicts` | 1,498 / 995 |
| `meta.main_pool_manuscript_count` | 6,755 |
| `meta.all_bucket_total` | 17,536 |
| `meta.all_bucket_manuscript_count` | 10,959 |
| `meta.corpus_manuscript_count` | 38,431 |
| `meta.corpus_page_count` | 177,402 |

4,152 + 3,873 + 1,498 = 9,523 — the decomposition holds and reproduces ruling U
exactly.

**This is NOT a post-deploy read from the box.** It is a local read of the
artifact whose content hash equals the deployed one. The plan asks for a
post-deploy read; there was no deploy, so that criterion is NOT MET and this is
the closest honest substitute. `GET https://genizahsearch.com/` answers **200**
in 1.16 s and `GET /computed-identifications` answers **404** — the surface is
correctly invisible in production.

## Task 1 — the headline, the rows, the second bucket

`web/components/findings_rows.py` (≈600 lines). What is absent from it is the
interesting part: no bucket rule, no band comparison, no coverage vocabulary of
its own, no letter-count field, no raw stored vocabulary in markup, no
novelty-keyed row treatment, no CSS.

**The coverage clause is DERIVED, not retyped.** `coverage_clause()` calls
`shared.discovery_display_strings.row_headline` twice — once with the
measurement and once with `None` — and takes the suffix difference. Two
properties come free: the string is byte-for-byte the shared vocabulary's, so it
keeps the exact qualifier the honesty gate's percentage exception recognises
(`of page` / `מהדף` inside a 32-character window); and the direct-family gating
(direct only, propagated excluded, null omitted) stays the shared module's
decision rather than a copy of it. `SURFACE_FINDING_FIELDS` carries no
`evidence_source`, so the family gate runs on `relation_kind` alone — the
conservative reading `row_headline` documents, and sound on this grain because
`max_coverage_ppm` is the MAX over the identification's evidence and every
shipped propagated row measures NULL.

**Ruling U's headline** renders the contribution total, the basis in words
(`bucket_name(True, lang)` — the shared bucket vocabulary, never a second name
for the same pool), the three match-framed shades with their manuscript counts,
and the corpus context figures on their own named line saying what they count.
An outage renders a named temporary condition with a retry and **no digit at
all**.

**One Hebrew wording was chosen against the obvious translation and the reason
is load-bearing.** `refines_granularity` reads *"פירוט עדין יותר מכלי העזר"*, not
the natural *"מדויק יותר…"* — `מדויק` is a rate word in the shared gate's Hebrew
lexicon, and beside a shade count it would read, to the gate and to a reader, as
an accuracy claim about the match rather than a statement about granularity.

**Second-bucket rows** go through the same renderer, the same anatomy and the
same classes as main-pool rows. That is a property of the code, not of a
reviewer's care: `render_finding_row` branches on `unit`, never on `main_pool`,
except to read the bucket's NAME from the shared rule.

## Task 2 — the combination space

**45 combinations enumerated, 41 measured, 4 skipped, 2 out of scope**, on the
public artifact, dev-box. Every one PASSES.

| | worst p95 | cap |
|---|---|---|
| ordering combinations (38) | **334.23 ms** (`findings_identification_page_count_more`) | 1,500 ms |
| count combinations (3 + 3 launch) | **104.54 ms** (`findings_work_visible_total`) | 500 ms |

The full table is written into `docs/specs/discovery-budgets.md` §4.4 by
`--write-budgets`, labelled with the artifact, the audience, `data_as_of` and
the host class. **No cap was edited**: §1, §2, §3 and §5 are byte-identical to
`HEAD`, and the §4.2 / §5.1 prod-box actuals survive intact.

**136-14's owed follow-up is CLOSED.** Every combination is built through
`shared/discovery_service.py::_build_findings_query` — the exact builder
`get_findings_enveloped` calls — plus `_build_launch_contribution_sql` /
`_build_launch_manuscript_sql` for the launch rows. The previous probe mirrored
the service in hand-written SQL, and one of its six shapes (a relation filter)
measured a control D-16 ratified the surface **without**.

**The numbers went UP, and that is the point.** The old
`findings_default_ordering` recorded 159 ms; its successor
`findings_identification_band_rank_main` records 224 ms. Nothing regressed — the
shipped builder joins `works` and `manuscript_display` and projects the real row
set, which the hand-written mirror did not. Those are the timings the surface
actually pays.

**Named rather than omitted:**

- `findings_coverage_filter` — the service exposes no coverage predicate at all;
  the page renders that control visibly disabled for exactly that reason.
- `findings_relation_filter` — D-16: the findings page ships without one.
- `findings_work_*_novelty` (3) — novelty is not offered on the per-work unit;
  the service RAISES rather than returning an envelope.
- `findings_work_deep_page_20` — the work unit carries 478 main-pool rows, fewer
  than the page-20 offset.

**Two robustness fixes the enumeration exposed, both real:**

1. `pick_findings_filters` drew its values globally, so a most-frequent genre
   with no main-pool rows turned a legitimate measurement into a zero-row abort.
   It now prefers values present in the main pool, with the global count as the
   fallback.
2. Populations that are **asset facts** (which buckets have rows; whether any
   ruling-U contribution shade is present) are pre-checked and become named
   skips. A zero row count on a filter value *picked from the same asset* stays
   the loud abort F14 requires — that one is a probe bug, not an asset fact, and
   the distinction is what keeps the abort meaningful. Verified: the sibling
   `test_bench_findings_page_never_records_a_timing_on_an_empty_result` still
   passes.

**Prod-box run: NOT DONE.** No deploy happened, and running the probe on the box
requires SSH access this executor did not use. §5.1 already carries prod-box
numbers for the superseded six-shape probe; the combination-space numbers are
dev-box only and the written block says so in words.

**Contrast run recorded** against the private rebuild
(`discovery-v1-136rebuild.db`, `audience=private`, 64,522 identifications, worst
p95 427.81 ms — still inside cap). Both artifacts report the **identical**
`sidecar_version` string, which is exactly why every number now carries its
artifact.

## Task 3 — the suite

**155 tests, 359 element-scoped / envelope / error-path gate calls.**

| coverage | shape |
|---|---|
| rendered page | 3 units × 4 states × 2 languages × 2 buckets = **48 renders**, each scoped per element |
| envelopes | 12 envelopes × 2 languages, recursively, by string value and by float |
| error paths | 9 forced modes × 2 languages, plus a **forced** log line and 3 rendered outage states |

Every scan is `assert_surface_honesty` — the **six**-detector entry point.
`assert_discovery_honesty` keeps a five-detector contract on purpose, and a
source assertion fails if this suite ever calls it.

### The sentinel, and why it is not redundant

The headline fixture carries `24,666 = 8,111 + 9,222 + 7,333` over 3,777 /
81,777 / 92,888 — numbers in **neither** half of 136-22's 23-value forbidden
union and absent from the committed figure file (asserted). With the live
figures in the fixture, a hardcoded headline would agree with the test and pass.

**Both provenance mutation controls were run, watched and reverted, and they
failed DIFFERENTLY as specified.** Operands taken from 136-22's committed figure
file at mutation time (9,523 = 9,000 + 523; neither operand is itself a
forbidden figure), never written into this plan or its suite:

| control | sentinel test | 136-22's scan |
|---|---|---|
| (a) `f"{9000 + 523:,}"` — a computed literal | **FAILED** | **FAILED**, naming `web/components/findings_rows.py:328 9523` |
| (b) `_A = 9000` / `_B = 523`, rendered `str(_A + _B)` | **FAILED** | **PASSED** |

(b) is the one that proves the sentinel is load-bearing: no static scan over
this repository can fold across statements, and a reader who assumed the scanner
covered everything would delete this test as duplication.

### Positive controls — 13, one property each, every one watched

| # | control | raised |
|---|---|---|
| 1 | "New discovery" in a row | the candidacy assertion, naming the phrase |
| 2 | a precision percentage in a row | `unqualified percentage` |
| 3 | an out-of-vocabulary domain | the closed-vocabulary assertion (live FJMS tree) |
| 4 | a header naming the MANUSCRIPT's domain | the header-element assertion, both halves |
| 5 | a row disagreeing with the shared bucket rule | the bucket-membership assertion |
| 6 | a percentage in `meta['reason']` | the ENVELOPE scan; markup scan blind (asserted) |
| 7 | a percentage in an exception message | the ERROR-PATH scan; markup scan blind |
| 8 | `fills_gap` as `band_label` | the NOVELTY vocabulary; companion: it still PASSES in `novelty_status` |
| 9 | `main_full_coverage` as `band_label` | the MAIN-POOL vocabulary; companion: it still PASSES in `main_pool_reason` |
| 10 | `accuracy 0.91` in a rendered row | the ACCURACY detector |
| 11 | a **float** `0.91` under a launch-envelope `meta` key | the numeric rule; companions: neither the string-only nor the markup scan sees it |
| 12 | `accuracy 0.91` in an exception message | the ACCURACY detector via the error path; companions: markup and envelope scans blind |
| 13 | the **live** D-06a sentence in a findings row, envelope and exception | `accuracy/rate claim` in **all three**, both languages |

Control 13 is the one that belongs here and nowhere else: 136-17's own control
seeds that sentence into a PANEL row, and nothing outside this plan proves the
exception fails to reach FINDINGS markup.

**The false-positive control `FP-LIVE-VOCAB` passed both halves:** a real-shaped
findings envelope carrying `direct_witness` in `relation_kind`,
`main_full_coverage` in `main_pool_reason` and `fills_gap` in `novelty_status`
PASSES; the same `direct_witness` seeded into `band_label` FAILS.

### 136-17's classification and corpus checks, RUN here

- Every field of the three allowlists this surface consumes
  (`SURFACE_FINDING_FIELDS`, `SURFACE_FACET_FIELDS`,
  `SURFACE_LAUNCH_SHADE_FIELDS`) is classified in the imported mapping. **No
  field was unclassified** — the partition 136-17 asserts against every
  registered allowlist held, so nothing had to be reported as blocking.
- The derived `CONSUMED_ALLOWLISTS` non-null coverage check PASSES: every field
  of every consumed allowlist is non-null in at least one corpus row.
- Every corpus row's key set EQUALS its registered allowlist (a construction
  check: it catches a hand-written dict that never went through a projection).
- Every `meta` key this surface emits is classified, or is numeric by shape.
- The widened vocabulary is asserted to have **reached** here: `fills_gap` and
  `main_full_coverage` are members of the prohibited set as imported, and
  `novelty_status` / `main_pool_reason` are declared machine carriers.

### Mutations run against the shipped code — 14 + 2, every one failed by name

Applied to a `cp`-restored source and reverted from a scratchpad backup. **`git
checkout --`, `git stash`, `git clean` and `git reset` were not used at any
point.**

| mutation | failed |
|---|---|
| N1 coverage clause always omitted | the POSITIVE coverage test (both languages) |
| N2 bucket name from a local literal | bucket membership — **`[he]` only**, see below |
| N3 a `gated` class on second-bucket rows | the same-anatomy test |
| N4 the unknown badge loses its muted class | the badge-state test |
| N5 the raw title rendered unrouted | ruling R (both languages) |
| N6 the raw shade key rendered | the container-wording test + 12 page-honesty renders |
| N7 the outage headline renders a zero | all 6 outage tests |
| N8 `main_pool_reason` rendered | the second-bucket honesty test + the page matrix |
| N9 the as-of line dropped | the novelty-help test |
| N10 the facet header names the manuscript | the header test |
| N11 a count attached to the bucket control | the no-count test |
| N12 the shelfmark rendered as plain text | the live-link test |
| N13 the multi-work annotation always on | the annotation test |
| N14 the novelty badge ignores `novelty_offered` | the per-work test |

> **N2 is worth recording.** Seeding the English bucket names as literals fails
> only the **Hebrew** parametrisation, because the English literals happen to
> equal `bucket_label`'s English strings exactly. An English-only reviewer would
> see one failure and might read it as noise. The bilingual parametrisation is
> what makes that mutation detectable at all.

Two further controls on the benchmark, both watched and reverted: the ordering
cap constant temporarily set to 1 ms produced `FAIL` per combination **with its
SQLite query plan** and **exit 1** (the budget document was never touched); and
the masking scan with `MASKING_SCAN_PATTERNS_FILE` unset **fails closed**
(`ERROR: no masking patterns loaded — refusing to run a zero-pattern
(false-green) scan`, exit 1) while a seeded committed pattern in a copy of the
capture is **caught** (2 hits, exit 1).

### Masking (DATA-05 / D-25)

| scan | result |
|---|---|
| `--scan-asset` over the captured rendered output — **48 states** (3 units × 4 service states × 2 buckets × 2 languages), 2.18 MB | **no matches — clean** |
| `--scan-repo` | **no matches — clean** |

`MASKING_SCAN_PATTERNS_FILE` was **set explicitly** to
`C:/Genizahsearch/.masking_patterns`; the scan did NOT fall through to its
fail-closed path and **no skip was taken**. The capture was written to
`…/scratchpad/masking/findings_capture.txt`, **outside the working tree**, so a
stray untracked capture cannot trip `--scan-repo` itself.

`--strict` was **not** run: it requires both `--scan-repo` and `--scan-asset
PATH` in one invocation over an asset directory, and this plan's asset is a
transient capture rather than a shipped artifact. Both halves were run
separately and both are clean.

## Verification

| check | result |
|---|---|
| `pytest tests/render_smoke/test_findings_render_smoke.py -q` | **155 passed** |
| `pytest tests/render_smoke/ tests/test_findings_page.py tests/test_discovery_launch_stats.py tests/test_no_await_sync_function.py tests/test_no_raw_storage_access.py -q` | **700 passed, 4 skipped** |
| `pytest tests/ -k discovery -q` | **1,495 passed, 8 skipped** |
| `pytest tests/test_discovery_launch_stats.py` with `DISCOVERY_LAUNCH_GUARD_DB` set to the public artifact | **227 passed, 0 skipped** |
| Task 1 verify command (tests + the grep gate) | **296 passed, 3 skipped; grep gate OK** |
| Task 2 verify command | **OK** |
| `ruff check` on all six owned files | clean |
| `git diff --stat HEAD -- web/static/common.css` | **empty** |
| budget-document caps §1/§2/§3/§5 vs `HEAD` | **byte-identical** |
| `check_atlas_masking --scan-repo` / `--scan-asset` | **clean** (pattern file set) |
| `GET https://genizahsearch.com/` | 200, 1.16 s |
| `GET https://genizahsearch.com/computed-identifications` | **404** (surface correctly invisible) |

## Acceptance criteria — MET / NOT MET

### Task 1 — ALL MET

Headline figures equal the envelope on a sentinel fixture and the shades sum to
the rendered total; the sentinels are asserted absent from both forbidden
halves; both provenance mutation controls observed failing differently and
reverted; the outage state renders a retry and no zero; 136-22's guard RUN;
basis named in words in both languages with a scoped container-wording
assertion; all three units render with their anatomy and the per-claim unit
proved unreachable; coverage asserted POSITIVELY on a direct row with its
qualifier and negatively on null and propagated rows; `grep` clean for
`matched_letters` and the phrase; ruling-R title routing in both languages with
the uncurated pass-through; titles plain, shelfmark linked to
`/browse?sys_id=…`; second-bucket rows present, same anatomy, no count element,
scoped honesty assertion; multi-work annotation and `novelty_offered` both
driven by the row; the novelty help carries the sources, the as-of date and the
candidacy sentence; no novelty-keyed accent rule; no local bucket derivation; no
CSS.

### Task 2 — MET except the prod-box run

MET: the full combination space with its count reported; the second bucket and
the launch query both enumerated; nonzero-result discipline; p50/p95/max with
PASS/FAIL per combination; counts against the separate cap; actuals written by
`--write-budgets` with no cap edited; artifact and audience on every recorded
number; a failing combination exits nonzero and prints its query plan; the
probe now points at `_build_findings_query`.

**NOT MET — "Both local and prod-box numbers are recorded where a prod run was
possible."** A prod run was **not** possible in this session: it needs either a
deploy (forbidden here) or SSH to the box (not used). Recorded here explicitly
rather than left implied.

### Task 3 — MET except the deploy and its dependants

MET: the matrix and its assertion count; every assertion element-scoped with the
facet-header comment recording why; bucket membership asserted against the
shared rule for every rendered row and for every row the shipped query returns;
the second bucket covered on a POPULATED fixture; both headline states; the
recursive envelope scan through the SHARED scanner with the no-second-scanner
source assertion; the false-positive control, both halves; the widened
vocabulary asserted to have reached here; the derived coverage check RUN over
this surface's corpus; the accuracy rate caught in all three egress classes and
in both forms; ≥6 error-path modes with message, log line and rendered state;
one positive control per item in the action's list; the nav entry absent and the
page non-empty in the unavailable state; the masking result recorded for every
state and both languages including whether the pattern file was set; a rollback
path recorded (below).

**NOT MET:**

- **"The findings page is deployed with the flag OFF for the public; the
  deployed commit and the prod-box first-paint numbers are recorded."** — **NOT
  MET.**
- **"The production launch statistics are recorded in the summary WITH their
  sidecar version and audience."** — **PARTIALLY MET.** The figures are recorded
  with full provenance, read through the shipped reader from the artifact whose
  content hash equals `manifest.deploy.json`'s. They are not a *post-deploy*
  read from the box.
- **"A rollback path is recorded as verified available before the deploy."** —
  **MET as far as it can be**: the code rollback is `git reset --hard <prior
  commit> && sudo systemctl restart genizah-web` on the box (`deploy.sh` is
  itself a `git reset --hard origin/<branch>`), and the sidecar rollback is the
  atomic manifest repoint in `docs/specs/discovery-deploy.md` §3. Neither was
  exercised, because no deploy was performed.
- **"The deploy is BLOCKED until plan 136-16's real-browser actionability result
  is recorded as MET."** — the gate is **CLOSED**. The record exists and reads
  **NOT MET** (136-16-SUMMARY.md; the CI `findings-browser-check` job is wired
  but has never had a green run). A test asserts the record exists and the suite
  prints the gate state on every run.

**Why the deploy was not performed.** Three independent reasons, any one of
which is sufficient:

1. **The orchestrator explicitly forbade it** for this wave: `deploy.sh` hard-
   resets production to the pushed branch, and the wave-9 panel deploy is still
   outstanding, so deploys are sequenced by the orchestrator rather than by an
   executor.
2. **The ruling-T launch blocker is still NOT MET.** Shipping the findings page
   with the "more matches" control unproven in a real browser would bury roughly
   half the non-Bible result behind something nobody has confirmed a reader can
   click.
3. **This working tree is shared and another agent committed and pushed to
   `master-main` mid-session.** A push plus a production `git reset --hard`
   would ship a set nobody verified together.

**What is ready to deploy:** commit `963166b9` on `master-main`, with
`DISCOVERY_ENABLED` unset (default OFF), which keeps the route returning the
availability card and the nav entry absent — the state production is in today
(`/computed-identifications` → 404).

## Deviations from plan

**1. [Rule 1 — bug] `tests/test_findings_page.py`'s dispatch-total test counted
only two of the three read kinds.**
Its docstring promised "so plan 136-18 adding a read does not turn this
criterion red", but `reads` was populated by the two `fp` attributes the test
patched, so ANY third read produced one uncounted dispatch and the assertion
went red on a correct implementation. Observed: `4 reads / 5 dispatches`. Fixed
by counting the launch read too — the criterion is unchanged and strictly
stronger (one dispatch per read, now across three read kinds). The alternative
considered and rejected was a page-level availability short-circuit around the
launch read: it would have made the test pass by making the read *not happen* in
the test harness, which is contorting production code to satisfy a test.
**`tests/test_findings_page.py` is outside this plan's `files_modified`.**

**2. [Rule 3 — blocking] Four tests in `tests/test_discovery_build.py` pinned
the superseded six-shape probe contract.**
The plan mandates replacing that contract with the full combination space, so
the two cannot both hold. Amended minimally, preserving every load-bearing
property (per-shape `rows > 0`, the separate count cap, cap-section
byte-identity, the idempotent upsert, the PENDING block) and adding the
artifact/audience assertion. **Outside `files_modified`.**

**3. [Rule 3 — blocking] Ruling U's headline wording, the shade labels and the
row count phrases live in a component-local bilingual table.**
The plan says the shade labels "come from `shared/discovery_display_strings.py`"
— they do not exist there, and that module is outside this plan's
`files_modified`, as is `genizah_translations.py`. Calling `tr()` with an
unregistered key is worse than useless: it falls back to the English key, so a
Hebrew reader would get English on the release's headline. The table follows
136-16's `_FINDINGS_COPY` precedent and every entry is swept through the shared
honesty gate in both languages. **Owed follow-up:** when a plan owns
`shared/discovery_display_strings.py`, these belong there.

**4. [Rule 3] No band tooltip on a findings row.**
The plan's row anatomy asks for "the relation chip (with the band label as its
`title` tooltip)". `SURFACE_FINDING_FIELDS` carries `best_band_rank` and no
`band_label`, `evidence_source` or `confidence_band`, so there is nothing to put
on the chip's `title`. Deriving a label from a rank would be a second band
vocabulary — precisely what the panel renderer refuses to do for the same
reason. Reported rather than papered over with an invented tooltip.

**5. [Rule 2 — missing critical] The domain facet header now NAMES its axis.**
The header rendered `tr("Domain")`, which says nothing about *whose* domain. The
plan's assertion ("the facet header describes the IDENTIFIED WORK's domain,
asserted against the HEADER ELEMENT ONLY") is unsatisfiable against a header
that makes no such claim, and its positive control could not fire. The header
now reads "Domain of the identified work" / "תחום החיבור המזוהה"; the assertion
and its control are both meaningful.

**6. [Rule 2 — missing critical] The novelty help affordance is VISIBLE, and its
as-of date is read from the artifact.**
136-16 put the help text in a `tooltip`, which no scoped assertion and no
masking capture can see. It now renders as an element with its own marker class,
and the as-of date comes from the artifact's own `data_as_of` meta key via
`web.discovery_assets.discovery_meta` — omitted entirely when the artifact
records none, rather than dated by guess. **Stated honestly: `data_as_of` is the
ARTIFACT's data-as-of date, not a per-source snapshot date for each finding aid.
No such per-source date exists anywhere in the shipped data or envelopes**, and
inventing one on the most reputationally loaded control on the page would be a
fabricated date. This is the closest true statement available.

**7. [Rule 2] `launch_shade_label`'s exception message NAMES its authority
instead of enumerating it.**
Written first as `(expected one of ['fills_gap', 'refines_granularity',
'container_predicts'])`, which the error-path scan correctly rejected: three
stored vocabulary values on an egress class that reaches a log without passing
either the markup or the envelope scan. It now names
`shared.discovery_service.LAUNCH_CONTRIBUTION_SHADES`.

**8. [Rule 1 — bug] `pick_findings_filters` drew its values globally**, so a
most-frequent genre carrying no main-pool rows aborted a legitimate measurement.
See Task 2 above.

**9. [Rule 3] The narrowing / cross-filtering assertions run against the
committed GOLDEN fixture sidecar**, materialized through
`scripts/ci_materialize_discovery_fixture.py`. Every value in it is fabricated
and no restricted corpus is named. It is the only artifact available to a
render-smoke suite that carries a real two-level genre tree, real authors and
rows in both buckets; the real artifacts are gitignored. The row-anatomy
assertions use hand-built rows through `surface_safe_finding`, because the
golden fixture has `max_coverage_ppm` NULL and `novelty_status` `not_checked`
throughout.

## Findings to report

**1. `shared/discovery_display_strings.py::relation_chip` emits stored
vocabulary on an exception message.**

```
relation_chip: unknown relation kind 'x' (expected one of ['direct_witness', 'quotes_this_work', 'shared_text'])
```

The error-path scan rejects that, and it is right to: an exception message
reaches a log and, uncaught, a reader, without passing through either the markup
scan or the envelope scan. `shared/` is outside this plan's `files_modified`.

**Severity is bounded by where it is reachable.** This surface CATCHES the
`ValueError` and renders the row without a chip (asserted behaviourally), so it
never reaches an egress from the findings page. The **panel** does not:
`web/components/discovery_panel.py::_render_expansion_envelope` calls
`ds.relation_chip(item.get('claim_type'), lang)` unguarded, so an
out-of-vocabulary claim type there propagates. The same shape appears in
`filter_code`, `filter_label`, `section_header`, `disclosure_toggle` and
`service_state_message`.

**Recommended fix (a later plan):** name the authority
(`scripts.discovery_ids.CLAIM_TYPES`) instead of enumerating its members, as
this plan's own `launch_shade_label` now does.

**2. `_build_findings_query`'s own `ValueError` messages are clean** — the unit,
sort and bucket vocabularies carry no underscore-bearing members — so this is
about the display module specifically, not about raising with a vocabulary in
general.

## Known Stubs

None added. The coverage filter remains rendered-disabled-and-tagged because the
service exposes no coverage predicate (136-16's deviation 3, unchanged and now
also recorded as an out-of-scope benchmark combination). The two future mode-strip
tabs remain inert and phase-tagged. Every element this plan adds is wired to real
data from a real envelope.

## Threat Flags

None. This plan adds no network endpoint, no auth path, no file access and no
schema change. `web/components/findings_rows.py` is a pure renderer; the one new
read is a direct `await` on an existing `web.discovery` wrapper; the one new
non-await accessor (`discovery_meta`) is an in-memory dict read of the loader's
startup-validated meta.

## Needs ticking by the orchestrator

I did not touch `STATE.md`, `ROADMAP.md` or `REQUIREMENTS.md`, as instructed.

- **Phase 136 wave 10 complete** — 136-18 is the only plan in it.
- **Requirements `NOVEL-01`, `NOVEL-02`, `PANEL-02`** are contributed to by this
  plan; `136-19` (wave 11, the cross-surface masking sweep) is the last plan in
  the phase. Do not mark any of them complete until it lands.
- **Plan counter:** 136-18 done (21 of 22 by plan number; wave 10 of 11).
- **Two production actions are OUTSTANDING and sequenced by you:** the wave-9
  panel deploy and this plan's findings deploy. The findings deploy is
  additionally gated on the CI `findings-browser-check` job passing a run
  (136-16 criteria (e)/(f), recorded NOT MET).
- **`origin/master-main` already carries commits `69060afb` and `5ca6b065`**
  (swept along by another agent's push); `6c4ba715` and `963166b9` are local.

## Self-Check: PASSED

Files claimed, verified present on disk:

- `web/components/findings_rows.py` — FOUND
- `web/pages/findings.py` — FOUND (modified)
- `scripts/bench_discovery.py` — FOUND (modified)
- `docs/specs/discovery-budgets.md` — FOUND (modified, §4.4 rewritten)
- `tests/render_smoke/test_findings_render_smoke.py` — FOUND
- `tests/test_findings_page.py` — FOUND (modified)
- `tests/test_discovery_build.py` — FOUND (modified)

Commits claimed, verified in `git log --oneline --all`:

- `69060afb` — FOUND
- `5ca6b065` — FOUND
- `6c4ba715` — FOUND
- `963166b9` — FOUND

---
*Phase: 136-read-surfaces-connections-panel-work-witnesses*
*Completed: 2026-08-04*
