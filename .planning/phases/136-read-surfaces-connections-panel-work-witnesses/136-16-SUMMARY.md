---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 16
subsystem: web
tags: [nicegui, discovery, findings-page, routing, feature-flag, rtl, i18n, honesty-gate, off-loop]

# Dependency graph
requires:
  - phase: 136-10
    provides: shared/discovery_display_strings.py (the claim vocabulary incl. display_work_title, bucket_name, rule_sentence, service_state_message, retry_label), the discovery tr() page-chrome block, and the .gs-discovery-scoped CSS block
  - phase: 136-14
    provides: web/discovery.py's async enveloped findings wrappers + the exported closed vocabularies (FINDINGS_UNITS / FINDINGS_SORTS / FINDINGS_BUCKETS / FACET_LEVELS)
  - phase: 136-02
    provides: tests/render_smoke/discovery_honesty_gate.py — the ONE shared honesty gate
  - phase: 134-05
    provides: web/discovery_assets.py::discovery_available() — the fail-closed flag-AND-readiness predicate
provides:
  - the /computed-identifications route and its availability-gated nav entry
  - web/pages/findings.py — the findings-page shell: header, RESERVED launch-headline slot, permanent caveat slot, mode strip, filter bar (incl. the ruling-T "more matches" control), result bar, minimal row, pager, four service states
  - the stable marker-class contract plan 136-18 fills the headline slot through
affects: [136-17, 136-18, 136-22, 137, 138]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "A page-local bilingual copy table for the strings that belong to neither tr() nor the shared claim vocabulary, swept through the SAME shared honesty gate"
    - "A closed-vocabulary constant written once and CHECKED against the exported set at module load, when the composition module does not re-export it"
    - "Executor-dispatch spying as the guard for nested-offload bugs the AST guard cannot see"
    - "Ancestry assertions declared explicitly insufficient, with the real-browser check named as the thing that would catch what they cannot"

key-files:
  created:
    - web/pages/findings.py
    - tests/test_findings_page.py
  modified:
    - web/main.py

key-decisions:
  - "BUCKET_MORE is written once in the page module and checked against the exported FINDINGS_BUCKETS at import, because web/discovery.py does not re-export it and the plan's own verify snippet forbids naming the service module in this file"
  - "Five bilingual strings live in a page-local _FINDINGS_COPY table rather than genizah_translations.py, because that file is outside this plan's files_modified and a missing tr() key renders ENGLISH under a Hebrew UI"
  - "The disabled-filter tag reads 'not available yet' rather than the plan's 'needs the rebuild', because the rebuild has shipped and the coverage filter is inert for a different reason (the service exposes no coverage predicate)"
  - "The page renders a MINIMAL identity row, because ruling T's operability criterion requires the result region to be observably REPLACED and a shell with no rows cannot satisfy it"
  - "The bucket is validated against the exported closed set and then narrowed to the two the page offers, so the all-bucket sentinel can never produce a figure the page cannot name (ruling U constraint 1)"

requirements-completed: []

# Metrics
duration: 210min
completed: 2026-08-04
---

# Phase 136 Plan 16: Corpus-wide Findings Page Shell Summary

**A routable, availability-gated, bilingual `/computed-identifications` shell — reserved digit-free launch-headline slot, permanent designed caveat, inert phase-tagged future modes, a first-class "more matches" control proven to replace the rendered result set through a simulated click that was watched failing, and four distinguishable service states — with 71 tests and one acceptance criterion explicitly NOT MET.**

## Performance

- **Duration:** ~210 min (including one mid-execution transport interruption and resume)
- **Tasks:** 3
- **Files:** 2 created, 1 modified
- **Tests:** 71 passing, 1 skipped (the NOT MET browser check)

## Task Commits

| Task | Name | Commit |
|---|---|---|
| 1 | The route, the availability gate and the nav entry | `164d1d8b` |
| 2 | Header + reserved headline slot + caveat, mode strip, filter bar, the "more matches" control | `d6b0474b` |
| 3 | The result bar, the pager and the four service states | `971ac899` |
| — | Docstring figure removal (Rule 2, below) | `cc40de38` |

## Files Created/Modified

- **`web/pages/findings.py`** (created, 873 lines) — the page shell. Imports only from `shared.discovery_display_strings`, `shared.discovery_novelty`, `web.discovery`, `web.safe_storage` and `web.translations`. Contains no `run.io_bound`, no reference to the composition module's private singleton, no percentage, no digit-bearing user-facing string in the headline scaffolding, and no grade/tier control.
- **`tests/test_findings_page.py`** (created, 1,796 lines, 72 tests) — gating, headline slot, caveat, ruling-T operability, mode strip, filter bar, ruling-R facet labels, string sourcing, off-loop guards + dispatch spies, result bar, pager, four service states, persistence.
- **`web/main.py`** (modified, +76 lines, 0 deletions) — the `@ui.page('/computed-identifications')` route and the `discovery_available()`-gated nav append. No pre-existing route or nav entry changed.

## Self-Check: PASSED

Files claimed, verified present on disk: `web/pages/findings.py` (873 lines) — FOUND; `tests/test_findings_page.py` (1,796 lines, 72 collected tests) — FOUND; `web/main.py` (+76/-0) — FOUND (modified); `136-16-SUMMARY.md` — FOUND.

Commits claimed, verified in `git log --oneline --all`: `164d1d8b` — FOUND; `d6b0474b` — FOUND; `971ac899` — FOUND; `cc40de38` — FOUND.

## Acceptance criteria — MET vs NOT MET

### Task 1 — all MET

| Criterion | Evidence |
|---|---|
| Route is `/computed-identifications`, matching the budget document's findings entry | `test_route_is_registered_at_the_computed_identifications_path` (asserts the module constant, the registration on `app.routes`, and that the budget document still names the page) |
| Unavailable route renders the card and does NOT import the page builder | `test_unavailable_route_renders_card_and_never_imports_the_page_builder` — pops `web.pages.findings` from `sys.modules`, renders, asserts it is still absent |
| Nav entry ABSENT (not disabled) when unavailable, present when available | `test_nav_entry_absent_when_unavailable_and_present_when_available`, **behavioural** — drives the real `create_layout()` in EN and HE |
| Page meta sets `noindex=True` | `test_page_meta_sets_noindex` (captures the real `page_meta` kwargs) |
| No change to the `/discoveries` route or its nav entry | `test_community_discoveries_route_and_nav_entry_are_untouched` + `git diff` on `web/main.py` shows only the two additive hunks |
| `page_client` bound at render time, before any background work | `test_page_client_is_bound_before_the_first_await` (AST: bind line < first `Await` line) |
| `tests/test_no_raw_storage_access.py` passes | green |

### Task 2 — all MET except criterion (e)/(f)

| Criterion | Status | Evidence |
|---|---|---|
| Caveat between header and body, both languages, through the shared honesty gate incl. the negated-wording rule | **MET** | `test_caveat_renders_between_header_and_body_and_passes_the_honesty_gate` — scoped to `gs-findings-caveat`, run through `assert_discovery_honesty` |
| Headline region PRESENT and containing NO DIGIT AT ALL, both languages | **MET** | `test_headline_slot_is_present_and_contains_no_digit` |
| This module writes no digit-bearing user-facing string into the headline scaffolding, in code or in the entries this plan adds | **MET** | `test_module_writes_no_digit_bearing_user_facing_string_into_the_headline` (AST, excluding docstrings and CSS-class literals) |
| (a) "more matches" control present in the FILTER BAR by its accessible name, EN + HE | **MET** | `test_more_matches_control_is_present_in_the_filter_bar` — scoped to the filter-bar element, not the page |
| (b) DOM-ancestry supplement | **MET** (and declared insufficient alone in the test's own docstring) | `test_more_matches_control_sits_in_no_overflow_or_disclosure_ancestor` |
| (c) INTERACTION through the NiceGUI `User` simulation, no preceding disclosure action, clicked THROUGH the simulated user, RENDERED result region REPLACED | **MET** | `test_more_matches_click_replaces_the_rendered_result_set[en/he]` — asserts main-pool rows gone and second-bucket rows present, not that a service call changed. **Watched failing**: with `on_click` removed the test failed with `expected to see at least one element with content=MORE-MATCHES-ROW-SENTINEL` |
| (d) Control's subtree carries no digit and no count element | **MET** | `test_more_matches_control_subtree_carries_no_digit_and_no_count` |
| **(e) REAL-BROWSER actionability at 375px and desktop, both languages** | **NOT MET** | see below |
| **(f) Positive control for (e), watched failing on a collapsed ancestor** | **NOT MET** | see below |
| Mode strip: three modes, two inert and phase-tagged, not clickable | **MET** | `test_mode_strip_renders_three_modes_with_two_inert_and_phase_tagged` |
| Novelty switch first in the filter bar, both directions | **MET** | `test_novelty_switch_renders_first_in_the_filter_bar` — asserts `fg novgrp` on the group AND the `order: -1` rule in the shared CSS |
| No grade/tier filter | **MET** | `test_no_grade_filter_control_exists` (whole-word scan; `\bStrong\b` deliberately does not match the legitimate "Strongest first" sort label) |
| A filter with missing backing data renders disabled WITH the amber tag | **MET** | `test_filter_with_missing_backing_data_renders_disabled_with_the_amber_tag` + `test_facet_group_with_an_outage_renders_disabled_rather_than_absent` |
| Work-facet label equals `display_work_title("w000176", <raw>, lang)` in both languages; the raw title is absent; an uncurated facet passes through | **MET** | `test_work_facet_label_routes_through_display_work_title` (with a fixture-error guard asserting the curated title actually differs from the raw one, so the test cannot pass for the wrong reason) |
| Every string from `tr()`, the shared vocabulary, or the audited table | **MET, with a deviation** — see Deviation 1 | `test_no_inline_user_facing_literal_in_the_module` (AST over render calls) |
| Off-loop: (i) no `run.io_bound`, (ii) no private-singleton access, (iii) no service-module call, (iv) every read an await on a `web.discovery` name | **MET** | `test_module_adds_no_nested_offload_and_no_direct_service_call` |
| Spy: exactly ONE dispatch per enveloped read on an available, cache-cold, successful call | **MET** | `test_exactly_one_executor_dispatch_per_enveloped_read_when_available` |
| Spy: exactly ZERO on the unavailable route, while the page still renders its outage | **MET** | `test_zero_executor_dispatches_when_discovery_is_unavailable` |
| Page-load total = SUM over the reads the page issues, computed from the rule not fixed as a literal | **MET** | `test_page_load_dispatch_total_is_the_sum_over_the_reads_the_page_issues` — asserts `dispatches == len(reads)`, so 136-18 adding a read cannot turn it red |
| No new CSS | **MET** | `test_this_plan_adds_no_css` (`git diff --stat` on `web/static/common.css` is empty) |

**On `tests/test_no_await_sync_function.py`:** it passes, and it could not have caught any of the four off-loop failure modes above. It detects **only** an `await` on a **locally defined synchronous `def`** — it cannot see a `run.io_bound` wrapper around an already-async wrapper, an attribute access on the composition module's private singleton, a direct synchronous service call, or an awaited name imported from the wrong module. That is exactly why this plan carries its own AST guard and dispatch spies.

### Task 3 — all MET

| Criterion | Evidence |
|---|---|
| "Show as" offers exactly the three shipped units and defaults to per-identification | `test_show_as_offers_exactly_the_three_shipped_row_units_and_defaults_to_identification` — option set asserted **equal to** the exported `FINDINGS_UNITS` |
| Novelty absent from sort; offered set equals `FINDINGS_SORTS` | `test_sort_offers_exactly_the_exported_orderings_and_novelty_is_not_one` |
| Result bar states WHICH bucket the count covers, both languages, in BOTH bucket states | `test_result_bar_states_which_bucket_the_count_covers` (4 parametrisations) — and asserts the line never names the other bucket, so two bases can never appear in one statement |
| Approximate count labelled, exact count not, driven by `meta["approximate_total"]` | `test_approximate_total_is_labelled_and_an_exact_one_is_not` |
| Four service states render distinctly; `timeout` and `busy` both offer retry and neither reads as empty | `test_each_outage_state_renders_distinctly_with_a_retry` (6 parametrisations), `test_ok_with_zero_rows_renders_an_honest_empty_state`, `test_the_three_outage_states_are_mutually_distinct` |
| On a fixture whose `total` exceeds `len(items)`, the rendered count is the envelope's `total` | `test_rendered_count_is_the_envelope_total_not_the_page_length` |
| Pager honours the budgeted default and cap; cap enforced SERVER-side | `test_pager_paginates_over_the_full_filtered_set`, `test_page_size_cap_is_enforced_server_side_not_only_in_the_control` (asserts the service clamps 10^6 → 200, that the page does NOT pre-clamp, and that the module restates no ceiling) |
| Per-user state through the chokepoint | `test_selections_persist_through_the_storage_chokepoint`, `test_out_of_vocabulary_persisted_values_fall_back_instead_of_reaching_the_service`, `tests/test_no_raw_storage_access.py` |

## ⚠ NOT MET — criteria (e) and (f), the real-browser actionability check

**Status: NOT MET. Not a skip, not a pass.**

- **What was required:** at 375px and at desktop width, in both languages, with no preceding disclosure action, the browser's own actionability conditions must hold at the "more matches" control's locator (visible, stable, enabled, receiving pointer events at its hit point); then a real click; then the results region's DOM asserted CHANGED. Plus (f): an ancestor deliberately collapsed, the same check confirmed FAILING, and reverted.
- **Why it was not met:** **Playwright is not installed in the execution environment.** `scripts/capture_atlas_html.py` documents it as an ad-hoc dev/ops tool, deliberately absent from every requirements file, and installing it (plus a Chromium download) is a package-manager install — excluded from what an executor may do unattended.
- **Viewport widths attempted:** none. **Languages attempted:** none. **Tool used:** none.
- **What was delivered instead:** the check is written and runnable —
  `tests/test_findings_page.py::run_browser_actionability_check(base_url)` drives both viewports (375×812 and 1440×900) in both languages, asserts the results region's `innerHTML` changes after a real click, and then runs its own positive control by collapsing the control's parent via `eval_on_selector` and asserting the same probe raises a Playwright actionability error. Set `GENIZAH_FINDINGS_BROWSER_CHECK=1` and `GENIZAH_FINDINGS_BROWSER_BASE_URL=<origin>` to run it. **With the env var SET and the tooling ABSENT it FAILS rather than skipping** — it never degrades to a silent green.
- **What is genuinely proven about the control today:** it is in the filter bar under no overflow/`<details>`/advanced ancestor and not below the results (b); a simulated click with no preceding disclosure action replaces the rendered result set in both languages (c); and that interaction test has been **watched failing** — with `on_click` removed it failed with `AssertionError: expected to see at least one element with content=MORE-MATCHES-ROW-SENTINEL`.
- **What remains unproven:** everything only a real browser can see — a collapsed ancestor, a zero-height box, a clip, an overlay, and behaviour at a specific viewport width. Ruling T makes this the reachability of roughly half the non-Bible discovery value in the release, so **this must be run before launch**, either by installing Playwright locally or as part of the live production smoke.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Five bilingual strings had no home, and the file that should hold them is outside this plan's file set**

- **Found during:** Task 2
- **Issue:** The plan requires "every string comes from `shared/discovery_display_strings.py` or `tr()`". Five strings this page must render exist in neither: the permanent **caveat**, the reserved headline region's **accessible label**, the **approximate-total note**, the **second-bucket result-bar line** (`tr()` has only the main-pool one), and the **disabled-filter tag**. Adding `tr()` keys means editing `genizah_translations.py`, which is outside this plan's `files_modified` and is a shared file this plan was explicitly built to avoid touching. Calling `tr()` with an unregistered key is worse than useless here: `web/translations.py::tr` falls back to the English key, so a Hebrew reader would silently get English on the page's most honesty-sensitive element.
- **Fix:** a page-local `_FINDINGS_COPY` table with `copy_text(key, lang)` and `copy_keys()`, bilingual, raising on an unknown key. Every entry is swept through the **same shared honesty gate** in both languages (`test_page_local_copy_passes_the_shared_honesty_gate`) and checked for bilingual completeness (`test_page_local_copy_table_is_bilingually_complete`).
- **Follow-up owed:** if a later plan owns `genizah_translations.py`, these five belong there as `tr()` keys; the split is documented at the top of the module.
- **Committed in:** `d6b0474b`

**2. [Rule 2 - Missing Critical] The plan's `_service`-free verify snippet forbids importing `BUCKET_MORE`**

- **Found during:** Task 2
- **Issue:** The page needs the second bucket's stored value. `web/discovery.py` re-exports `BUCKET_MAIN` and `FINDINGS_BUCKETS` but **not** `BUCKET_MORE`, and the plan's own verify command asserts `'_service' not in <module source>` — which the substring `shared.discovery_service` trips. So the constant could be neither imported from the composition module nor from the service module.
- **Fix:** written once in the page module and **checked against the exported closed set at module load**, raising `RuntimeError` (not `assert`, which `-O` strips) if it ever leaves `FINDINGS_BUCKETS` or collides with `BUCKET_MAIN`; plus `test_bucket_more_matches_the_service_constant` pinning it byte-for-byte against the service's own definition. A rename in the service therefore breaks the import loudly rather than silently sending an out-of-vocabulary bucket that would raise at request time.
- **Committed in:** `d6b0474b`

**3. [Rule 1 - Bug] The plan's "needs the rebuild" tag wording is now false**

- **Found during:** Task 2
- **Issue:** The plan specifies an amber tag reading *"needs the rebuild"*. The rebuild has shipped (`coverage_ppm` and `band_rank` are in `web/discovery_assets.py::_REQUIRED_COLUMNS`). The coverage filter is inert for a *different* reason: `get_findings_enveloped` exposes no coverage predicate at all. Shipping "needs the rebuild" would state something untrue on a surface whose whole design premise is that a disabled control must say honestly why it is disabled.
- **Fix:** the tag reads **"not available yet" / "עדיין לא זמין"**. The `needs` CSS class and the `fg blocked` treatment are unchanged, so the amber visual the criterion asks for is intact; only the words are honest.
- **Committed in:** `d6b0474b`

**4. [Rule 2 - Missing Critical] A minimal result row, which the plan assigns to the row track**

- **Found during:** Task 2
- **Issue:** The plan scopes this file to the *shell* and assigns row anatomy to plans 136-17/136-18. But ruling T's criterion (c) requires the **rendered result region to be observably REPLACED** — "main-pool rows gone, second-bucket rows present" — and Task 3's criterion requires a count proven against a fixture. A shell with no rows cannot satisfy either.
- **Fix:** `_render_row` renders a minimal identity row only — the work title (through `display_work_title`, ruling R) plus library + shelfmark. The relation chip, band tooltip, novelty badge, coverage clause and side actions are deliberately absent and documented as the row track's.
- **Committed in:** `d6b0474b`

**5. [Rule 2 - Missing Critical] A figure in the module docstring**

- **Found during:** final verification
- **Issue:** the D-16 rationale in the module docstring quoted the measured relation split as a percentage. This phase's own recorded failure class is a prohibited string reaching a surface *from hand-written prose*, and plan 136-10 already fixed the identical defect in `shared/discovery_display_strings.py`'s docstring.
- **Fix:** the figure removed; the module now carries **no `%` and no measured figure at all**. The number stays in `136-GATE1-DECISIONS.md`.
- **Committed in:** `cc40de38`

### Deliberate non-deviations

- **No relation filter.** D-16 was ratified 2026-08-02: the findings page ships without one. The plan's Task 2 action says "and the relation filter if the gate-1 decision put it in scope" — it did not.
- **No launch figure anywhere.** Ruling U: this plan reserves the headline region and renders nothing into it. **No launch number appears as a literal anywhere in this plan's code, tests or translations.** The slot is filled by **plan 136-18** from **plan 136-22's** artifact-backed reader; 136-22's figure-specific, artifact-derived guard already globs `web/pages/findings.py`.

## Known Stubs

**One, intentional and named:** the reserved launch-headline region (`gs-findings-headline`) renders as an empty, stable, `aria-label`led container with an empty `gs-findings-headline-value` child. This is the plan's explicit instruction, not an oversight — plan **136-18** fills it from plan **136-22's** reader. A test asserts it is present and digit-free; it will not silently stay empty, because 136-18's own criteria depend on it.

The minimal result row (Deviation 4) is a **scope boundary, not a stub**: it renders real data from the envelope; the row track adds the remaining anatomy.

## Threat Flags

None. This plan adds one publicly reachable route, and it is gated on the same fail-closed `discovery_available()` predicate as `/atlas`, `noindex`ed until the Phase-139 REL-01 gate, and asserted by test not to import its page module while unavailable. No auth path, no file access, no schema change, no network endpoint beyond the gated page route, and no package install (T-136-16-SC).

**Masking (D-25):** no file this plan touches names any restricted corpus in any form — not in code, comments, test fixtures, log messages or exception text. The repo-wide masking sweep was **not run here**: this checkout carries no gitignored `.masking_patterns`, so the scan would fail closed for a missing pattern file, which is not a gate result.

## Verification run

| Check | Result |
|---|---|
| `pytest tests/test_findings_page.py -q` | **71 passed, 1 skipped** |
| `pytest tests/test_no_await_sync_function.py tests/test_no_raw_storage_access.py -q` | **15 passed** |
| `pytest tests/test_atlas_flag_gating.py tests/test_discovery_flag.py tests/test_discovery_display_strings.py tests/test_openapi_scope.py -q` | **64 passed** |
| Plan's Task-1 snippet (`computed-identifications` + `discovery_available()` in `web/main.py`) | **OK** |
| Plan's Task-2 snippet (no `run.io_bound`, no `_service`, `display_work_title` present) | **OK** |
| `git diff --stat web/static/common.css` | **empty** |
| `ruff check` on all three files | **clean** |
| Percent signs in `web/pages/findings.py` | **0** |
| Real-browser actionability (e) + positive control (f) | **NOT MET** — Playwright unavailable |

The full suite was deliberately **not** run: two other agents hold uncommitted work in this shared checkout, including a mid-edit `tests/test_discovery_panel_model.py`, so full-suite failures would carry no signal about this plan.

## State files deliberately NOT updated — for the orchestrator

`STATE.md`, `ROADMAP.md` and `REQUIREMENTS.md` were left untouched, following the precedent 136-10 set and for the same reasons, which apply more strongly here:

1. **This is a parallel wave on a shared checkout.** `state advance-plan` increments a sequential counter; this is wave-7 plan 16 running concurrently with plan 15. Advancing the counter from an out-of-sequence executor writes a number nobody can trust.
2. **`roadmap update-plan-progress 136` recalculates from summaries on disk** and (per 136-10's recorded experience) ticks 136-09 to `[x]` as a side effect, even though that plan HALTED at an owner gate. The orchestrator should tick 136-16 by hand.
3. **`REQUIREMENTS.md` was not marked.** NOVEL-01 and PANEL-02 are requirements this plan *contributes to*, not ones it *completes* — the surfaces they describe are finished by 136-17/136-18.

## Next Plan Readiness

- **136-18** can fill the reserved headline slot: the container class is `gs-findings-headline`, its empty child is `gs-findings-headline-value`, and the region already carries a bilingual `aria-label`. The dispatch-total test is written as `dispatches == len(reads)`, so adding the launch-headline read will not turn it red.
- **136-17** is unaffected: this plan touched no shared file.
- **136-22's** figure-specific guard already covers `web/pages/findings.py`; this module is currently digit-free in every user-facing string and carries no `%` at all.
- **Blocker for launch, not for the next plan:** criteria (e) and (f) are NOT MET and must be run against a real browser before the surface ships.
