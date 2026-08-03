---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 14
subsystem: discovery-read-service
tags: [envelope, d13g, allowlist-projection, manuscript-scope, findings-query, facet-cascade, perf]
requires:
  - "136-11 (discovery_identification — the materialized grain, coverage_ppm, band_rank)"
  - "136-12 (novelty_status, the two visibility axes, works.genre from the curated artifact)"
  - "136-13 (the rebuilt public artifact live in production, flag OFF)"
  - "135-01 (shared/discovery_band_labels.py — serialize_banded_claim, is_default_eligible)"
  - "136-03 (shared/discovery_main_pool.py — main_pool_decision, the ONE bucket rule)"
provides:
  - "The {status, items, total, meta} envelope over a CLOSED four-value status vocabulary (D-13)"
  - "shared/discovery_surface_projection.py — the allowlist projections every surface row passes through"
  - "The D-13g fix: a human-confirmed row survives a routing demotion, flagged low-coverage"
  - "The panel's display fields in ONE query, joined on display_work_id"
  - "Manuscript scope that NAMES the works, with page counts, gating and a real total (D-13h)"
  - "The related-page count as DISTINCT opposite pages (D-11a), separate from its rows"
  - "web/services.py::get_manuscript_page_ids — the browse-map page-ID accessor (NEW plumbing)"
  - "The corpus-wide findings query: three units, one builder, over the materialized grain"
  - "The domain/author/work cascade on the IDENTIFIED WORK, with a wrong-axis source guard"
affects:
  - "136-15/136-16 (the findings page reads get_findings_enveloped + the facet cascade)"
  - "136-19/136-20 (the panel reads get_claims_for_page_enveloped + the manuscript scope)"
  - "136.1 (PANEL-03, /work/{id}, /catalog-browse — they build on the same envelope)"
tech-stack:
  added: []
  patterns:
    - "allowlist projection at the service boundary (not a renderer assertion)"
    - "COUNT(*) OVER () for a real total inside the SAME query as the page"
    - "closed enums mapped to fixed SQL fragments; every value bound"
    - "one filter builder shared by the row query and its facet counts"
key-files:
  created:
    - "shared/discovery_surface_projection.py"
    - "tests/test_discovery_findings_query.py"
    - ".planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-14-SUMMARY.md"
  modified:
    - "shared/discovery_service.py"
    - "web/discovery.py"
    - "web/services.py"
    - "tests/test_discovery_service.py"
    - "tests/test_discovery_assets_audience.py"
decisions:
  - "eligibility_basis is DERIVED per row, not read from discovery_identification.eligibility_basis — the stored column is an aggregate that mislabels a restored row inside a mixed identification, and it is not yet in the schema contract"
  - "The identification join is a LEFT join: under include_review a review-only/unreviewed claim legitimately has no identification, and an inner join would drop exactly the rows the flag exists to reveal"
  - "The manuscript-scope query applies NO routing filter — a gated work is returned flagged, never omitted, because the folios that make an anchor judgeable are often the demoted ones"
  - "'matched text' sorts on max_coverage_ppm (the grain materializes coverage, not letters) and the envelope names the basis, so the surface never implies a letter count it does not have"
  - "An out-of-vocabulary unit/sort/bucket RAISES rather than returning an envelope — the four statuses stay reserved for things that happened to the service"
metrics:
  duration: "~5h"
  completed: "2026-08-03"
---

# Phase 136 Plan 14: The Read Spine for Both Surfaces Summary

One service layer both surfaces build on: an outage-aware envelope, a human-confirmed-safe page query
delivering every panel field in one statement, manuscript scope that names the works, and a
corpus-wide findings query serving three row units and a facet cascade inside a budget an earlier
shape could not meet.

## What was built

### Task 1 — the envelope, the D-13g fix, and the panel's display fields

**`shared/discovery_surface_projection.py` (new).** Two things, and nothing else may re-implement
either:

1. **The envelope.** `{status, items, total, meta}` over a closed four-value vocabulary
   (`ok` / `unavailable` / `timeout` / `busy`). Before this, every wrapper collapsed a query timeout,
   an overload rejection, an absent sidecar and a genuine zero into `[]` — and the panel's rule is to
   hide itself on a zero, so an outage would have rendered as "this manuscript has no
   identifications". A non-`ok` envelope always carries an empty item list and a total of 0, so a
   caller that reads `total` without checking `status` still cannot render an outage as a count.

2. **The allowlist projections.** `serialize_banded_claim` ALWAYS emits `review_overlay`, and for a
   `human_confirmed` row that value is the literal "Expert-reviewed ✓" badge D-13f dropped. A
   renderer-level assertion cannot stop that string reaching an envelope, a JSON payload or an error
   message — by then it has already left the service. `surface_safe_claim` and its siblings name the
   keys a surface may receive and drop everything else; an allowlist, so a field the serializer grows
   later is excluded by default rather than leaking by default. `FORBIDDEN_SURFACE_FIELDS` plus a
   substring rule validates the allowlists themselves at import time and re-checks every envelope.

**The D-13g fix, in SQL.** `get_claims_for_page` no longer pre-filters human-confirmed rows by
routing status. The predicate now mirrors the eligibility rule the build already materializes into
`discovery_identification`, so the restore cannot be undone one layer down by the join.

**The panel's fields in ONE query.** `COUNT(*) OVER ()` supplies the real total before `LIMIT`, so
the header costs no second query; the identity join is on `display_work_id`.

### Task 2 — manuscript scope, the related-page count, the page-ID accessor

- `get_manuscript_works_enveloped` — one row per distinct canonical work in the page set, with page
  count, strongest band rank, gated flag and title, paginated with a real total.
- `get_related_page_count_enveloped` / `get_related_pages_enveloped` — the header count (DISTINCT
  opposite pages, deduplicated) and the rows behind the toggle, returned separately.
- `web/services.py::get_manuscript_page_ids` + the pure `discovery_page_id_from_header` — NEW
  plumbing. `BrowsePage` carries the current uid, page number and totals only; the browse map carries
  the page list, and a discovery `page_id` IS the corpus page header
  (`{sys_id}_{IE…}_{P00000N}_{FL…}`, verified against both the asset and `Transcriptions.txt`).
  Bounded at 500, volume-aware, and it reports `resolved=False` rather than raising.
- `DiscoveryService.run_off_loop` — the public alias of the off-loop discipline, so the one blocking
  read that is not a sidecar query runs under the same rules instead of growing a second copy of them.

### Task 3 — the findings query and its cascade

- Three row units from ONE parameterised builder. The per-unit difference is DATA (a select list and
  a group-by), not code — a test asserts there is exactly one query builder and exactly one filter
  builder.
- `_build_findings_filter` is shared by the row query and the facet counts, because building the
  predicate twice is how a facet count and the result set beside it drift apart.
- The default result set is the main pool and the envelope's meta says so.
- The cascade reads `works.genre` through `display_work_id` — the IDENTIFIED work's domain. The
  `Unassigned` bucket is selectable with a real count on both axes.
- The per-claim unit and a novelty sort are not reachable; both raise.

## Evidence

### Measured on the real rebuilt asset (`discovery_data/discovery-v1-136rebuild.db`)

The D-13g population, confirmed exactly as the plan states — the two figures are different and must
not be conflated:

| population | total | routing-demoted (restored by the fix) |
|---|---|---|
| ALL `human_confirmed` evidence rows | 121 | **19** |
| **display** evidence — what this page query reads | **116** | **14** |

The restored row on the live asset comes back with `low_coverage_marker=True`,
`eligibility_basis='human_confirmed'`, `main_pool=True`, `main_pool_reason='main_human_confirmed'`,
and its band unchanged.

Timings, through the real service against the rebuilt asset (caps from
`docs/specs/discovery-budgets.md` §1.1 and §5):

| shape | measured | cap |
|---|---|---|
| enveloped page query (50 real pages) | **0.18 ms** each | p95 ≤ 150 ms added browse latency |
| findings, identification unit, 3 sorts | **242–260 ms** p50 (31,913 total) | p95 ≤ 1.5 s |
| findings, manuscript unit, 3 sorts | **247–264 ms** p50 (27,160 total) | p95 ≤ 1.5 s |
| findings, work unit, 3 sorts | **156–166 ms** p50 (903 total) | p95 ≤ 1.5 s |
| domain facet tree (88 nodes) | **85 ms** | — |

`EXPLAIN QUERY PLAN` on the real asset:

- manuscript scope → `SEARCH dc USING INDEX ix_discovery_claim_page_id (page_id=?)`, every join
  index-driven.
- findings default ordering → `SEARCH di USING INDEX ix_discovery_identification_order (main_pool=?)`.

The prior shape for the same findings ordering measured 3.41–3.55 s against the 1.5 s cap, with the
deduped count alone at 16 s.

`scripts/bench_discovery.py` could NOT be re-run locally: it resolves its database through
`web.discovery_assets.discovery_db_path()`, and the local manifest points at the CERT-01-pinned v1
asset, which the fail-closed loader refuses (`load_discovery_state() → False`, `db_path → None`).
That is a pre-existing local condition, not a regression — the bench was run on production at 136-13
and its numbers are already in `discovery-budgets.md` §5.1. The direct measurements above exercise
the new code paths, which the bench does not know about.

### The wrong-axis guard, observed failing

Required by the plan and done literally. `shared/discovery_service.py` was temporarily edited to add:

```python
def _seeded_wrong_axis(sys_ids):
    from shared.fjms_service import get_fjms_service
    return get_fjms_service().get_domains_for_sys_ids(sys_ids)
```

`test_no_findings_query_path_reads_the_manuscripts_catalogue_domain` then failed with:

```
AssertionError: the discovery service imports ['shared.fjms_service'] --
the findings cascade must not reach into the FJMS catalogue at all
```

The edit was reverted. The guard is AST-based rather than a substring scan, and that is not cosmetic:
the service module's own docstring cites `shared/fjms_service.py` as the sidecar-service shape it was
modelled on, so a substring scan fires on prose and would have to be weakened until it caught
nothing. A companion test runs the guard's own logic over a seeded copy and asserts both the import
and the call are caught, and that the clean source is clean — so the guard discriminates rather than
merely passing.

### Masking

`MASKING_SCAN_PATTERNS_FILE=.masking_patterns python scripts/check_atlas_masking.py --scan-repo` →
`no matches -- clean` (exit 0). Nothing in the new code names a corpus; `novelty_source_label` is
passed through because it is already the masked label set.

### Tests

| suite | tests |
|---|---|
| `tests/test_discovery_service.py` | 85 (was 49) |
| `tests/test_discovery_findings_query.py` | 22 (new) |
| both together | **107 passed** |
| `tests/test_discovery_assets_audience.py` | 32 passed |
| `tests/test_no_await_sync_function.py`, `test_no_back_edges_discovery.py`, `test_discovery_build.py`, `test_discovery_band_labels.py`, `test_discovery_composition.py`, `test_discovery_loader.py`, `test_offloaded_readers.py` | all green |

Full CI invocation
(`PYTHONUTF8=1 GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -m "not gui and not render_smoke and not atlas_bake" -q`):

**6,392 passed / 34 skipped / 3 xfailed / 26 xpassed** in 17:00 — against the 6,323-passed baseline,
i.e. +69 with zero regressions. An intermediate full run (`1 failed, 6386 passed`) is what surfaced
the second VIS-01 registration, documented below.

`grep -n "asyncio.wait_for" shared/discovery_service.py` → 2 occurrences, both in prose saying NEVER
to use it. No new occurrence over an executor future.

`python -m ruff check shared/ web/ tests/` → All checks passed.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 1 — Bug] A failing page query reported a genuine zero instead of an outage**

- **Found during:** Task 3 verification, running the new enveloped path against the REAL pre-rebuild
  asset (the CERT-01-pinned v1 database, which has no `discovery_identification` table).
- **Issue:** `_query_claims_for_page` swallowed the query exception into an empty result, so the
  envelope reported `ok` with a total of 0 — the exact false zero D-13 exists to prevent, on the one
  surface whose rule is to hide itself when the total is zero. **Every synthetic fixture in the suite
  carries the new tables, so no unit test could have reached this**; it took an asset that genuinely
  lacks them.
- **Fix:** the query helper raises; the legacy list method turns it back into `[]` (its unchanged
  contract) and the enveloped method maps it to `unavailable` with `reason='query_failed'`. Verified
  against the real pre-rebuild asset: `unavailable {'reason': 'query_failed'} total 0`.
- **Test:** `test_a_failing_query_is_an_outage_not_a_genuine_zero` drops the table from a fixture to
  reproduce that shape exactly.
- **Commit:** `68a07aa0`

**2. [Rule 3 — Blocking] `eligibility_basis` read from a column absent from a contract-shaped asset**

- **Found during:** Task 1, when `tests/test_discovery_composition.py` and
  `tests/test_discovery_assets_audience.py` failed with `no such column: di.eligibility_basis`.
- **Issue:** `tests/fixtures/discovery_v2_fixture.py` hand-mirrors the schema doc's authorized column
  list, which does not include `eligibility_basis` — 136-11 added that column ahead of its own owed
  schema amendment.
- **Fix:** derive `eligibility_basis` PER ROW in SQL instead. Strictly better on two counts. More
  precise: the stored column is an aggregate over an identification's evidence rows, so an
  identification carrying one shipped row and one restored review-only row reports `shipped` for
  both, mislabelling the row the surface must annotate. And portable: a contract-shaped artifact may
  not carry the column at all. Three closed values — `shipped`, `human_confirmed`, `review_opt_in`.

**3. [Rule 3 — Blocking] `tests/test_discovery_assets_audience.py` edited (not in `files_modified`)**

- The VIS-01 refusal sweep is designed to REFUSE an unregistered read path
  (`AssertionError: web.discovery.get_manuscript_works_enveloped() takes an unregistered required
  parameter 'page_ids'`) — it forced registration, exactly as intended. It also had to learn the
  envelope shape: it asserted `result in (None, [], {}, (), False, 0)`, which no envelope satisfies.
  Added `_is_empty_read_result` (an envelope is empty when it carries no item and a zero total) and
  registered `page_ids`. The inverse control now also exercises the four enveloped paths, so their
  refusal cannot pass vacuously.
- **Then CI caught the same guard a second time, on something the targeted run could not see.** The
  sweep was fixed during Task 2; Task 3 then added `get_findings_facets_enveloped()` with a required
  `level` parameter, and only the full-suite run surfaced it
  (`1 failed, 6386 passed` — `AssertionError: … takes an unregistered required parameter 'level'`).
  Registered, and the two findings paths were given an explicitly **weaker** inverse control, written
  into the test rather than left to be inferred: the v2 fixture creates `discovery_identification`
  but seeds no rows, so there is nothing for them to return. What is provable there is that they
  reach the database and report success (versus `unavailable` on a private artifact); their
  row-level non-vacuity is proved in `tests/test_discovery_findings_query.py` against a populated
  fixture. Commit `5ef9b45e`.

### Intentional choices the plan left open

- **The manuscript-scope query applies NO routing filter.** The plan asked for a `gated` flag; the
  panel reference requires gated works to RENDER as dashed chips. Filtering them out and then
  flagging them is not possible, so the query returns everything for the page set and computes
  `gated` from the materialized `main_pool`.
- **`get_claims_for_page`'s ORDER BY changed** from `dc.work_id` to
  `COALESCE(band_rank, 8) ASC, work_id ASC` — strongest first, still a total order. No caller exists
  and every existing test is order-agnostic or unaffected.
- **The identification join is LEFT, not INNER.** Its eligibility rule matches this query's default
  predicate, so it is total for the default population — but under `include_review=True` a
  review-only/unreviewed claim legitimately has no identification row, and an inner join would
  silently drop exactly the rows that flag exists to reveal. (The golden fixture contains such a row,
  so this is exercised, not theoretical.)
- **The findings page gets no relation filter.** `136-GATE1-DECISIONS.md` § D-16/PANEL-01: the owner
  declined it for this surface (within the main pool the split is 94% direct, so it would restate the
  bucket rather than narrow it). The panel's own relation filter is unaffected.

### Requirements deliberately NOT marked complete

The plan's frontmatter carries `requirements: [PANEL-01, PANEL-02, NOVEL-01]`, but
`requirements.mark-complete` was NOT run for them. They are phase-level requirements whose SURFACES
ship in 136-15..136-20; this plan builds the data contract they will read. 136-13 carried the same
IDs and left them `[ ]` for the same reason, so this follows the phase's existing convention rather
than introducing one. Marking PANEL-01 complete while no panel exists would be a false claim in
`REQUIREMENTS.md`.

## Owed follow-ups (out of scope here)

- **`docs/specs/discovery-budgets.md` §5 env list** does not mention `DISCOVERY_FINDINGS_COUNT_MAX`
  (new, default `0` = exact counts; when set it bounds the count and flags `approximate_total`). It
  loosens no cap — it can only make a count less precise, and only with the flag set — but the env
  list should record it. Not edited here: the budgets doc is tunable-only-by-versioning and is not in
  this plan's `files_modified`.
- **`tests/fixtures/discovery_v2_fixture.py` hand-mirrors the schema doc's column list**, so it will
  drift again the next time a column lands ahead of its amendment. The `eligibility_basis` schema
  amendment 136-11 flagged as owed is still owed.
- **`scripts/bench_discovery.py` knows nothing about the findings SERVICE**, only about hand-written
  SQL that mirrors it. Those two can now diverge. A future plan could point the bench at
  `_build_findings_query` so the benchmarked shape is by construction the shipped shape.

## Lesson recorded

`git checkout -- shared/discovery_service.py`, used to undo a scripted temporary edit, reverted the
file to HEAD and destroyed the whole of Task 3's uncommitted implementation. It was recovered only
because a `cp` backup had been taken before the seeding step. When temporarily mutating a file that
carries uncommitted work, restore from a copy — never from git.

## Self-Check: PASSED

Files:

- FOUND: `shared/discovery_surface_projection.py`
- FOUND: `shared/discovery_service.py`
- FOUND: `web/discovery.py`
- FOUND: `web/services.py`
- FOUND: `tests/test_discovery_service.py`
- FOUND: `tests/test_discovery_findings_query.py`
- FOUND: `tests/test_discovery_assets_audience.py`

Commits:

- FOUND: `46702264` feat(136-14): the status envelope, the D-13g human-confirmed fix, and the panel's display fields
- FOUND: `4a99063e` feat(136-14): manuscript scope that names the works, the related-page count, and the page-ID accessor
- FOUND: `7c0a9f28` feat(136-14): the corpus-wide findings query and its facet cascade on the identified work
- FOUND: `68a07aa0` fix(136-14): a failing page query was reporting a genuine zero, not an outage
- FOUND: `5ef9b45e` test(136-14): register the findings facet path with the VIS-01 refusal sweep
