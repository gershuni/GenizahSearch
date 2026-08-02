---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 02
subsystem: web-ui
tags: [discovery, honesty-gate, methods-page, band-labels, nicegui, render-smoke, d-06a]

# Dependency graph
requires:
  - phase: 136-01
    provides: "docs/specs/discovery-band-labels-v1.md Amendment 2026-08-02 (Note 3 — qualitative-only, everywhere) and the REQUIREMENTS.md BAND-05/BAND-03 dated amendment authorizing this rewrite"
provides:
  - "web/pages/help.py's BAND-05 methods section rewritten qualitatively: zero precision percentage, confidence interval, weighted estimate or strata table anywhere, in either language"
  - "The qualitative per-band measurement-status vocabulary (_MEASUREMENT_STATUS_COPY, keyed by shared.discovery_band_labels.band_measurement_status()) as the replacement for the struck weighted-estimate + CI line"
  - "MAIN_POOL_SENTENCE — the single bilingual source of the two-bucket-rule wording, importable from web.pages.help, that plan 136-07 will assert byte-identical against shared.discovery_main_pool.main_pool_sentence()"
  - "Three new bilingual subsections on the methods page: the two-bucket rule, known limitations (containment/two-sides-of-one-leaf/composition-date), and the novelty check (candidate-is-not-a-confirmed-find, D-23b)"
  - "tests/render_smoke/discovery_honesty_gate.py::assert_discovery_honesty(rendered_html, *, scope_selector, lang) — the ONE shared no-numbers gate every later Phase-136 surface suite (panel, work page, findings page, catalog-browse) imports"
affects: [136-04, 136-07, 136-15, 136-17, 136-18, 136-19, 136-20, 136-21, "136.1"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "ONE shared cross-surface test-honesty gate (tests/render_smoke/discovery_honesty_gate.py), imported by every later discovery-surface render-smoke suite rather than reimplemented per-surface"
    - "Qualitative-status-vocabulary pattern: a bilingual copy table keyed by an existing pure-function's enum return value (band_measurement_status()), so a future status change needs a sidecar data change only, never a code edit"
    - "Mandatory-scope test assertions: an honesty/word-gate helper refuses to run over unscoped or wrongly-scoped markup, raising rather than passing vacuously (the findings-page sketch's own facet-header assertion passed for the wrong reason — this pattern prevents that class of bug everywhere it is reused)"

key-files:
  created:
    - tests/render_smoke/discovery_honesty_gate.py
  modified:
    - web/pages/help.py
    - tests/render_smoke/test_help_methods_render_smoke.py

key-decisions:
  - "Second-bucket Hebrew wording for 'confirmed find' avoided the literal substring מאושר (which the D-06 word gate itself prohibits) — used ממצא סופי ('a final finding') instead of a phrase built on the א-ש-ר root, after the first draft tripped the suite's own HE prohibited-word assertion"
  - "The raw-stored-vocabulary-key check in the shared gate is scoped to UNDERSCORE-BEARING tokens only (scripts.discovery_ids enums + shared.discovery_band_labels.MEASUREMENT_STATUSES) — plain English band words that happen to equal a differently-named enum member without an underscore (e.g. 'weak', 'corroborated', 'unreviewed') are structurally exempt, so future honest prose using those ordinary words can never false-positive"
  - "The novelty check's enumerable checked-source list is rendered from a placeholder module-level bilingual constant with a TODO(136-04) comment, naming the category of sources checked (catalogue/titles/bibliography/scholarly attributions) without inventing dates, per the plan's explicit instruction not to fabricate a dated list before plan 136-04 lands"
  - "Split the git history into 2 commits matching Tasks 1+2 (the help.py qualitative rewrite, committed together because they are inseparably coupled — Task 1's own <verify> step runs the SAME render-smoke suite Task 2 also modifies, and an intermediate git state was constructed and independently verified green before committing) and Task 3 (the new shared gate module + its wiring into the suite)"
  - "Skipped requirements mark-complete for PANEL-01/PANEL-02/NOVEL-01 (this plan's shared frontmatter IDs) — this plan lands only the methods-page rewrite + the honesty gate; the bulk of each requirement is implemented across later Phase-136 plans (136-15..136-21 for PANEL-01/02, 136-04/136-12 for NOVEL-01's actual novelty computation). Same precedent as 136-01 and Phase 134's DATA-01/02/03/10 decisions"

patterns-established:
  - "A shared, importable test-honesty gate module lives in tests/render_smoke/ (not shared/ or a script), because it is test infrastructure consumed only by render-smoke suites, never by product code"

requirements-completed: []

# Metrics
duration: 42min
completed: 2026-08-02
---

# Phase 136 Plan 02: BAND-05 Methods Page Qualitative Rewrite + Shared Honesty Gate Summary

**Removed every precision percentage, confidence interval and strata table from the `/help` "Confidence Bands & Methods" section (replaced with a qualitative per-band status vocabulary + three new bilingual subsections explaining the two-bucket rule, known limitations, and the novelty check), then shipped `assert_discovery_honesty()` as the ONE shared no-numbers gate every later Phase-136 surface suite will import — proven able to fail via two positive controls.**

## Performance

- **Duration:** 40 min
- **Started:** 2026-08-02T08:39:00Z (approx., immediately after plan 136-01's final commit)
- **Completed:** 2026-08-02T09:19:00Z
- **Tasks:** 3 completed
- **Files modified:** 3 (1 created, 2 modified)

## Accomplishments

- **Task 1 — Removed every number.** Deleted `format_precision_copy`/`_precision_copy_safe`, `_CONFIDENCE_STRATA_FALLBACK`, `_CONFIDENCE_COLLECTION_LABEL` and the 0.926 collection-scope paragraph from `web/pages/help.py`. `format_precision_copy` is no longer imported anywhere in the file (`grep -c` returns 0), consistent with `docs/specs/discovery-band-labels-v1.md` Amendment 2026-08-02 Note 3 ("`format_precision_copy()` therefore has NO surface caller after this phase"). Kept every non-percentage fact placeholder-safe: population (runtime display-deduplicated count), unit of measurement, the three sample-size numbers, and the four CERT-01 registry fields. `git diff --stat shared/discovery_band_labels.py` is empty — no source-of-truth module touched.
- **Task 2 — Added the qualitative narrative.** New `_MEASUREMENT_STATUS_COPY` table renders each band's `band_measurement_status()` value in words (e.g. "graded to completion and passed its pre-registered floor" for `measured_pass`). Three new bilingual subsections: the two-bucket rule (`MAIN_POOL_SENTENCE`, quoted verbatim from `main-pool-rule.md`, with the second-bucket-means-insufficient-evidence framing spelled out separately), known limitations (containment stated as "a low single-digit share", the two-sides-of-one-leaf caveat, the composition-date caveat), and the novelty check (candidate-is-not-a-confirmed-find, absence-is-not-evidence, D-23b). The dated checked-source list is a placeholder pending plan 136-04 (recorded, not invented).
- **Task 3 — The shared honesty gate.** New `tests/render_smoke/discovery_honesty_gate.py` exports `assert_discovery_honesty(rendered_html, *, scope_selector, lang)`: a stdlib-only (`html.parser`) class-scoped text extractor plus five checks (unqualified percentages, bracketed intervals, human-review badges, prohibited relation words with negation-proof word-boundary matching, and raw stored vocabulary keys). `scope_selector` is mandatory — missing or non-matching raises `DiscoveryHonestyScopeError` rather than passing vacuously. Wired into the methods suite for both languages against the REAL rendered section, plus two positive controls that turned assertions red: a seeded precision-figure-plus-interval (2 violations: unqualified-percentage + bracketed-interval) and a seeded stored vocabulary key `direct_witness` (1 violation: raw-vocab-key).

## Task Commits

Each task was committed atomically:

1. **Task 1 + Task 2: Rewrite the BAND-05 methods section qualitatively (removal + new subsections)** — `cc518800` (feat)
2. **Task 3: The shared discovery honesty gate, with positive controls** — `aefee18a` (test)

_Tasks 1 and 2 landed in a single commit — see "Deviations from Plan" for why._

## Files Created/Modified

- `web/pages/help.py` — the BAND-05 methods section: removed precision/CI/strata rendering; added qualitative status copy + `MAIN_POOL_SENTENCE` + the two-bucket-rule/known-limitations/novelty-check subsections
- `tests/render_smoke/discovery_honesty_gate.py` — NEW: the shared `assert_discovery_honesty()` no-numbers gate (287 lines)
- `tests/render_smoke/test_help_methods_render_smoke.py` — rewritten: updated fixtures/assertions for the qualitative content, exact-anchor-set + field-parity tests, and the honesty-gate wiring + positive controls

## Decisions Made

See `key-decisions` in the frontmatter above. The most consequential: avoiding the literal Hebrew substring מאושר in the novelty-check copy (the D-06 word gate treats it as a plain substring match, so "a confirmed find" phrased with the א-ש-ר root would have tripped the suite's own HE prohibited-word assertion — caught by running the test suite, not by inspection) and scoping the shared gate's raw-vocab-key check to underscore-bearing tokens only (so ordinary English band words like "weak" can never false-positive on a future honest surface).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Combined Task 1 and Task 2 into a single commit**
- **Found during:** Task 1
- **Issue:** Task 1's own `<verify>` step runs `tests/render_smoke/test_help_methods_render_smoke.py`, but the plan's file list assigns test-file changes only to Task 3. The pre-existing test file asserted the OLD precision/CI/strata content (e.g. `assert section.count('92.6%') == 1`), which Task 1 necessarily removes — so Task 1's own verify command cannot pass without also updating the test file, and Task 2 (the new subsections) further extends the same assertions in the same functions. Splitting Task 1 and Task 2 into two commits would require reconstructing an artificial intermediate test-file state with no independent value.
- **Fix:** Committed the help.py qualitative rewrite (Task 1's removals + Task 2's additions) together as one commit, with the test file updated to match the FINAL qualitative content (population/status/subsection assertions, an exact anchor-set check, a field-parity check). Verified the combined state passes independently (8 tests green) before committing, and before adding Task 3's gate-wiring on top.
- **Files modified:** `web/pages/help.py`, `tests/render_smoke/test_help_methods_render_smoke.py`
- **Verification:** `pytest tests/render_smoke/test_help_methods_render_smoke.py -q` green at each of the two commit boundaries (8 tests after commit 1, 15 after commit 2); ruff clean at both boundaries.
- **Committed in:** `cc518800` (Task 1+2 commit)

**2. [Rule 1 - Bug] Hebrew novelty-check copy tripped the suite's own prohibited-word assertion**
- **Found during:** Task 2 (initial pytest run of the rewritten HE test)
- **Issue:** The first draft of the Hebrew novelty-check sentence used "ממצא מאושר" ("a confirmed find") — literally containing the substring מאושר, one of the three HE words the D-06 word gate treats as prohibited everywhere (`assert forbidden not in section`, a plain substring check, no word-boundary carve-out for Hebrew). The test failed with the exact violation it exists to catch.
- **Fix:** Reworded to "ממצא סופי" ("a final finding"), avoiding the א-ש-ר root entirely while preserving the same meaning.
- **Files modified:** `web/pages/help.py`
- **Verification:** `pytest tests/render_smoke/test_help_methods_render_smoke.py::test_help_methods_section_renders_he_rtl -q` green after the fix.
- **Committed in:** `cc518800` (Task 1+2 commit)

---

**Total deviations:** 2 auto-fixed (1 blocking/commit-structure, 1 real bug)
**Impact on plan:** Both fixes were necessary to keep every task's own verify command honestly green; neither expanded scope beyond what Task 1/2's `<action>`/`<acceptance_criteria>` already specified.

## Issues Encountered

None beyond the two items documented above as deviations.

## User Setup Required

None - no external service configuration required. This plan touches one web page module and its render-smoke suite; no environment variables, migrations, or external services are involved.

## Next Phase Readiness

- `tests/render_smoke/discovery_honesty_gate.py::assert_discovery_honesty` is ready for import by every later Phase-136 surface suite (136-15/136-17 panel render honesty, 136-18 findings render honesty, and any `/work/{id}` or `/catalog-browse` suite in Phase 136.1) — no surface needs to reimplement the no-numbers rule.
- `web.pages.help.MAIN_POOL_SENTENCE` is ready for plan 136-07 to assert byte-identical against `shared.discovery_main_pool.main_pool_sentence()` once that module exists.
- The novelty check's checked-source list is a placeholder (`_NOVELTY_CHECKED_SOURCES_PLACEHOLDER`, TODO-tagged) pending plan 136-04's dated enumerable list — a small follow-up edit to `web/pages/help.py` will be needed once that data lands, but it does not block anything in the meantime (the gap is recorded here, not invented).
- No blockers. Full `tests/render_smoke/` suite green (68 passed, 1 pre-existing skip unrelated to this plan); repo-wide masking scan clean with the local `.masking_patterns` file present.

---
*Phase: 136-read-surfaces-connections-panel-work-witnesses*
*Completed: 2026-08-02*

## Self-Check: PASSED

All 3 files confirmed present on disk (`web/pages/help.py`, `tests/render_smoke/discovery_honesty_gate.py`, `tests/render_smoke/test_help_methods_render_smoke.py`); both task commit hashes (`cc518800`, `aefee18a`) confirmed in `git log`.
