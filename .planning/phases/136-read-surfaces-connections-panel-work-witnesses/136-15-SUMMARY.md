---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 15
subsystem: ui
tags: [discovery, panel, display-model, pure-function, envelopes, ruling-r, honesty-gate]

# Dependency graph
requires:
  - phase: 136-07
    provides: shared/discovery_grouping.py (collapse_canonical, lead_attribution, separate_granularity) and shared/discovery_main_pool.py (bucket_label, SHORT_EVIDENCE_THRESHOLD_MATCHED_LETTERS)
  - phase: 136-10
    provides: shared/discovery_display_strings.py — every reader-facing string, including display_work_title (ruling R)
  - phase: 136-14
    provides: shared/discovery_surface_projection.py (the four-key envelope, is_outage, the row allowlists) and the five enveloped reads in web/discovery.py
provides:
  - "shared/discovery_panel_model.py — the panel's whole display model as pure functions over the LIVE service envelopes"
  - "PanelServiceBundle — the integration contract 136-17 and 136-19 build against, with an honest NOT-REQUESTED state for the lazy fifth envelope"
  - "build_panel_rows(bundle) -> PanelModel — collapse, generic-group separation, lead attribution, short-evidence gating, ruling R title routing, status arbitration, manuscript pane, related-pages section, lazy expansion descriptors"
  - "ARBITRATION_TABLE — the total (claim status x page-scope state) table the panel's own suite reads"
  - "A model-level honesty sweep + bucket-membership assertion, with six positive controls"
affects: [136-17, 136-19, 136-21, 136-22]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Display model as a pure function over the SERVICE ENVELOPE SET, never over bare rows"
    - "Optional envelope field whose None means NOT REQUESTED, rejected on every eager field"
    - "Arbitration written as a literal, import-time-total table that the tests read rather than restate"
    - "MACHINE_VOCABULARY_FIELDS — an explicit allowlist of fields whose values are machine vocabulary, so the raw-vocabulary sweep stays able to fail"

key-files:
  created:
    - shared/discovery_panel_model.py
    - tests/test_discovery_panel_model.py
  modified: []

key-decisions:
  - "The lazy related-rows field is Optional with None = NOT REQUESTED; None raises on all four eager fields, so an outage can never be laundered as 'nobody asked'"
  - "Bucket membership is the row's MATERIALIZED main_pool decision and the bucket NAME is bucket_label(); the model contains no second 'is this good enough' rule"
  - "The anchor identity is validated BEFORE any display string is composed, so the all-or-none contract is the error that speaks when a row is short of it"
  - "Optional row fields are ABSENT rather than None (no coverage field at all on a propagated row; no human-review field anywhere)"
  - "The manuscript pane's scope state decides before its envelope status, so an unresolved scope is never rendered as an outage or as a zero"

patterns-established:
  - "One raw recorded-title read per module, feeding display_work_title, pinned by a grep (ruling R)"
  - "Positive controls seed ONE property each and assert the specific detector AND field, never merely that the suite went red"

requirements-completed: [PANEL-01, PANEL-02]

# Metrics
duration: 95min
completed: 2026-08-04
---

# Phase 136 Plan 15: The Discovery Panel Display Model Summary

**Every panel display rule is now one pure function over the five live service envelopes — collapse, generic-group separation, lead attribution, short-evidence gating, ruling R title curation, status arbitration, the manuscript pane and the related-pages section — with 157 tests, a model-level honesty sweep in both languages and six positive controls.**

## Performance

- **Duration:** ~95 min
- **Tasks:** 3 (2 TDD, 1 auto)
- **Files created:** 2 (`shared/discovery_panel_model.py`, 990 lines; `tests/test_discovery_panel_model.py`, 157 tests)
- **Files modified outside the plan's list:** 0

## Accomplishments

- **`PanelServiceBundle`** — the integration contract. Five envelope fields validated on construction against the live four-key shape and the closed status vocabulary. The fifth (related-page ROWS) is optional and `None` means **NOT REQUESTED**; `None` raises on each of the four eager fields.
- **`build_panel_rows(bundle) -> PanelModel`** — a pure function. No query, no UI import, no rule restated: grouping comes from `shared.discovery_grouping`, bucket names from `shared.discovery_main_pool.bucket_label`, every string from `shared.discovery_display_strings`, every title from `display_work_title`.
- **The four-step pipeline, named and ordered in the source** — collapse duplicates → separate identical-span generic groups → lead attribution over what remains → gate short evidence. A test reads the order out of the source and a fixture proves the ordering matters.
- **A total arbitration table** — 16 literal rows over (claim status × page-scope state), guarded at import time, read by the suite rather than restated in it.
- **A model-level honesty sweep** reusing the shared gate's five detectors field-by-field over 1,197 string fields per language across eight service-state variants, plus a bucket-membership comparison over 60 emitted objects per language.

## Task Commits

1. **Task 1: the input bundle, the identification-row model, title routing** — `37f1fd11` (test, RED) → `28425de7` (feat, GREEN)
2. **Task 2: status arbitration, disclosure model, manuscript pane, related pages** — `8d53d35c` (test, RED) → `4200ac9e` (feat, GREEN)
3. **Task 3: model-level honesty invariants with one positive control per property** — `63b6cc56` (test)

## Files Created

- `shared/discovery_panel_model.py` — the panel's whole display model. Public surface: `PanelServiceBundle`, `PanelModel`, `build_panel_rows`, `iter_rows`, `ARBITRATION_TABLE`, `ENVELOPE_KEYS` / `EAGER_ENVELOPE_FIELDS` / `LAZY_ENVELOPE_FIELD` / `LIVE_OK_META_KEYS`, the four row states, the four pane states, the four scope states, `DISCLOSURE_LEVEL_KEYS`, `MACHINE_VOCABULARY_FIELDS`, `MANUSCRIPT_PANE_PAGE_THRESHOLD`, `EXPANSION_PAGE_SIZE`.
- `tests/test_discovery_panel_model.py` — 157 tests.

## What each named fixture protects

| Fixture | Real case | What it pins |
|---|---|---|
| `two_titles_duplicate` | the same work twice under two titles (921 row-pairs corpus-wide) | ONE row, the canonical title, routed through `display_work_title` |
| `verse_chain_generic_group` | the prayer book's page-6 chain on offsets 0–555 | the group leaves the identifications bucket carrying offsets, letter count and a work count of **2** (the duplicate collapsed first) |
| `two_granularity_rashi` | T-S Misc. 12.31.14, identical span 0–962 | the pair collapses like a duplicate and STAYS an identification; with the separate row the page emits **two** identifications and **no** generic group |
| `sixty_six_letter_liturgical` | the siddur's short liturgical matches | gated behind "show more", never deleted, and reachable when the toggle is open |
| `two_human_confirmed_rows` | Moss. V,374 | both rows in the default set, the routing-demoted one with a coverage note, a non-null bucket and reason, and no human-review field |
| `curated_w000176_liturgy` | ruling R | the curated bilingual label, with the raw recorded title absent from every emitted display field |

## Decisions Made

- **`None` on the fifth envelope means NOT REQUESTED and nothing else.** The section emits `not_requested`, and the header count still comes from the eagerly-fetched COUNT envelope, so the default view has a real number without the rows. Fabricating an `ok`/`items=[]`/`total=0` would tell a reader "this page has no related pages" on the strength of a query nobody ran.
- **The scope state decides the manuscript pane before its envelope status does.** 136-17 does not issue the works query when the page scope fails to resolve, so whatever envelope reaches the model in that case says nothing; ordering the outage check first would have rendered our plumbing failure as a service outage instead of as an unresolved scope.
- **The anchor identity is validated first, before any display string is composed.** Without that, a row missing its evidence source surfaced as a band-label lookup error and the all-or-none rule never ran. Found by the plan's own negative test.
- **Optional fields are absent, not null.** A propagated row emits no coverage key at all (a null there is one careless renderer away from reading as zero coverage), and no object emits any human-review field (D-13f: the safest implementation of "no marker" is that the field does not exist).
- **`MACHINE_VOCABULARY_FIELDS` is declared in the model, not in the test.** Six field names (`main_pool_reason`, the four anchor-identity fields, `status`/`reason`) whose values are machine vocabulary by design. Every other emitted field is swept for raw stored vocabulary keys. Mirrors the same mechanism 136-17 specifies for the render-level scan.

## D-13g population — artifact and audience

The human-confirmed fixture is **behavioral, not count-derived**: it constructs a routing-demoted human-confirmed row and asserts it survives into the default set.

The affected population, stated with its artifact and audience: the **private rebuild `discovery-v1-136rebuild.db`** measured **14 of 116** display evidence rows (**19 of 121** across all human-confirmed evidence). **The deployed PUBLIC artifact carries a different population and must be re-measured rather than copied forward.** Neither pair of numbers is asserted anywhere in the suite — a grep of the test file for `121` / `116` / `14 of` / `19 of` returns nothing.

## Positive controls — what each one raised

Six controls, each seeding exactly ONE property and asserting the specific detector AND the specific field. Each mutates a locally-built model inside its own test, so nothing needs reverting and no production code ever carried a seeded violation; the full suite is green.

| # | Property | Seeded into | Raised |
|---|---|---|---|
| 1 (required) | precision figure | `…rows[0].headline` | `DiscoveryHonestyViolation` — `unqualified percentage '94.2%'` `[percentage]`, and no other detector fired |
| 2 (required) | stored vocabulary key in a chip | `…manuscript_pane.works[0].relation_chip` | `DiscoveryHonestyViolation` — `raw stored vocabulary key 'direct_witness'` `[raw-vocabulary]` |
| 3 (required) | human-review marker | `…rows[N].low_coverage_note` | `DiscoveryHonestyViolation` — `human-review badge …` `[review-marker]` |
| 4 (required) | bucket disagreeing with the shared rule | `…rows[0].bucket` | `AssertionError` — `model.rows[0].bucket: … disagrees with the shared rule, which says …` |
| 5 (additional) | bracketed interval | `…rows[0].low_coverage_note` | `DiscoveryHonestyViolation` — `bracketed interval '[0.9084, 0.9644]'` `[interval]` |
| 6 (additional) | negated prohibited relation word | `…manuscript_pane.works[0].work_title` | `DiscoveryHonestyViolation` — `prohibited relation wording 'copy of'` `[prohibited-wording]` |

Controls 5 and 6 are additional, not substitutes: the four the plan requires are #1–#4. A companion test asserts the qualified matched-letter coverage (`68% of page (matched letters)`) PASSES while a bare `68%` in the same field fails.

## Deviations from Plan

### 1. The 66-letter fixture isolates the short-evidence half, and the identical-span half is covered separately

- **Found during:** Task 1.
- **Issue:** The real case is "a siddur whose four liturgical matches share one 66-letter span". Written literally as one identical-span group, D-13d pulls the whole group OUT of the identifications bucket into the generic-passage level — at which point the plan's own acceptance criterion ("the rows are gated rather than deleted, and reachable under `show_more`") could not be satisfied, because generic groups are not gated rows.
- **Resolution:** `sixty_six_letter_liturgical` gives the four matches DISTINCT spans, so what decides them is the ratified 150-matched-letter floor (D-13c) and the acceptance criterion is met exactly. The identical-span half of the same real case is covered by `verse_chain_generic_group`, which is the owner's own byte-identical-offsets case, plus `test_short_evidence_row_kept_by_multi_folio_agreement_stays_in_the_default_level` for D-13c's carve-out. No criterion was narrowed.

### 2. The order-of-operations test feeds the duplicate fixture as well as the two-granularity one

- **Found during:** Task 1.
- **Issue:** The plan asks for the collapse-before-generic ordering to be asserted "by feeding the two-granularity fixture". The two-granularity fixture alone does not discriminate the two orderings: `separate_granularity` returns the same verdict for that pair whichever order runs first.
- **Resolution:** the test feeds BOTH. The discriminating fixture is `two_titles_duplicate`, whose two rows share a canonical work id AND a span — generic-first would see an identical-span group its own predicate cannot decide (`UNDECIDABLE` → conservatively generic) and would file a correct identification away as generic shared text. The test also asserts the four named step markers appear in order in the module source. Superset of what was asked.

### 3. `_anchor_identity` split out of `_expansion_descriptor` (Rule 1 — bug)

- **Found during:** Task 2, by the plan's own negative test.
- **Issue:** With `relation_kind` / `evidence_source` / `confidence_band` absent, a display-strings lookup raised before the all-or-none anchor check ever ran, so the contract that is supposed to speak in that situation never did.
- **Fix:** anchor validation is now the first statement of `_identification_row`, ahead of any string composition. The error names the present and the missing fields.
- **Verification:** `test_a_partial_anchor_identity_raises_naming_present_and_missing_fields`, parameterised over all four fields.

### 4. Two additional positive controls beyond the four required

- The shared gate has five independent detectors; the plan's four required controls exercise three of them plus the bucket rule. Controls 5 and 6 cover the remaining two (bracketed interval, negated prohibited wording) and are labelled "additional" in the source so the required four stay unambiguous.

**Total deviations:** 4 (1 auto-fixed bug under Rule 1; 3 fixture/coverage clarifications, each a superset of the criterion rather than a narrowing).
**Impact on plan:** none on scope. Every acceptance criterion is met.

## Not done, and why

**STATE.md, ROADMAP.md and REQUIREMENTS.md were deliberately NOT updated.** This plan ran concurrently with plan 136-16 on the same working tree, and the dispatch scoped this executor to `shared/discovery_panel_model.py` and `tests/test_discovery_panel_model.py` with an explicit instruction to report rather than edit anything outside that list. Those three files are shared mutable bookkeeping that both executors would race on (`state.advance-plan` in particular). **The orchestrator should run the plan-counter advance, the progress recalculation, the metric record and `requirements mark-complete PANEL-01 PANEL-02` once both parallel plans have landed.**

## Issues Encountered

- The curated-title absence assertion initially failed in Hebrew, because the Hebrew curated label legitimately CONTAINS the raw recorded title as its first half. The assertion now strips every occurrence of the CURATED string and asserts the raw title is gone from the residue — which proves the only route it ever took to a display field was `display_work_title`, and is stricter than a plain "not in" would have been.

## Verification

- `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_panel_model.py -q` → **157 passed**.
- Task 2's scoped run (`-k "disclosure or manuscript_pane or related or entry_control or arbitration or resolved or truncated"`) → 55 passed.
- `python -m pytest tests/test_no_back_edges_core.py tests/test_no_back_edges_discovery.py tests/test_discovery_display_strings.py tests/test_discovery_main_pool.py tests/test_discovery_grouping.py tests/test_discovery_service.py -q` → **208 passed**. `test_second_implementation_guard_finds_none_on_the_real_tree` (the guard that fails if a module under `shared/` defines a second bucket-membership predicate) is green against the new module.
- `python -m ruff check` on both files → clean.
- Module greps: no `nicegui`, no human-review field, no query execution, no literal bucket name, no truthiness test on an envelope item list, exactly ONE raw recorded-title read, and it feeds `display_work_title`.
- `LIVE_OK_META_KEYS` is asserted equal to the meta keys the producing functions actually build, parsed out of `shared/discovery_service.py` and `web/discovery.py` by AST — so a drifted service shape fails here, not in a renderer.

## Threat register — dispositions

| Threat | Mitigation shipped |
|---|---|
| T-136-15-01 one passage inflating a match count | lead attribution nests; generic groups leave the identifications bucket |
| T-136-15-02 the same work twice under two titles | canonical collapse, named fixture, one row and one derived count |
| T-136-15-03 an outage rendering as a manuscript with nothing | envelope-set input, entry-control visibility as a model field, 16-state arbitration cross-product |
| T-136-15-04 vocabulary or band string in a visible field | model-level sweep in both languages with one positive control per property |
| T-136-15-05 a second bucket rule inside the model | bucket-membership comparison against the shared rule + disagreeing-row control; the repo-wide second-implementation guard is green |
| T-136-15-06 a row claiming human review | no such field exists; grep asserts its absence |
| T-136-15-07 a raw title reaching a reader | bilingual `w000176` fixture + single-call-site grep |
| T-136-15-08 unresolved/truncated scope as a fact about the manuscript | both are distinct emitted states with their own tests; an unresolved pane emits no total, no works and no empty marker |
| T-136-15-09 an unrequested section rendered as an empty one | optional fifth field, `None` forbidden on the four eager fields, four-way pairwise-distinct test |
| T-136-15-SC package installs | none in this plan |

No new threat surface: the module opens no socket, reads no file, touches no schema and adds no dependency.

## Next Phase Readiness

- **136-17** can build `PanelServiceBundle` from its four eager envelopes, leave `related_rows=None`, and call `bundle.with_related_rows(envelope)` when the reader opens the toggle. Every display judgement — bucket, gating, ordering, titles, section states, pagination — is already made; the renderer reads fields.
- **136-21**'s expansion wrapper receives `row["expansion"]`, which carries `work_id`, all four anchor-identity fields, a `page_size` and `loaded: False`. Nothing is fetched with the panel.
- **Open for the orchestrator:** the STATE/ROADMAP/REQUIREMENTS updates listed under "Not done, and why".

## Self-Check: PASSED

- `shared/discovery_panel_model.py` — FOUND
- `tests/test_discovery_panel_model.py` — FOUND
- `.planning/phases/136-.../136-15-SUMMARY.md` — FOUND
- Commits `37f1fd11`, `28425de7`, `8d53d35c`, `4200ac9e`, `63b6cc56` — all FOUND in `git log`

## Known Stubs

None. No hardcoded empty value flows to a rendered field: an emitted empty collection is only ever a genuinely-empty SUCCESSFUL state (`PANE_EMPTY`, `ROWS_EMPTY`), and every other zero-ish situation emits `not_requested`, `unresolved_scope` or `outage` instead. A grep of both files for `TODO`, `FIXME`, `placeholder`, `coming soon` and `not available` returns nothing.

---
*Phase: 136-read-surfaces-connections-panel-work-witnesses*
*Completed: 2026-08-04*
