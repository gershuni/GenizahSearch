---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 21
subsystem: discovery-read-surfaces
tags: [PANEL-02, DATA-01, DATA-10, D-13, discovery, expansion, work-witnesses]
requires:
  - "136-14: make_envelope / SURFACE_* allowlists / _ALL_ALLOWLISTS / the raising-helper split"
  - "136-11: manuscript_display (schema Amendment 2026-08-02 (B1))"
  - "shared.discovery_band_labels.serialize_banded_claim (SC#1)"
provides:
  - "DiscoveryService._query_work_expansion(...) -> (rows, total) -- RAISES"
  - "DiscoveryService.get_work_expansion_enveloped{,_async} -- the four-key envelope"
  - "web.discovery.get_work_expansion_enveloped -- the surface-facing wrapper"
  - "SURFACE_EXPANSION_FIELDS + surface_safe_expansion (registered in _ALL_ALLOWLISTS)"
  - "build_work_expansion_rows_sql / build_work_expansion_count_sql / _build_work_expansion_pipeline"
affects:
  - "136-17 (wave 9): four KNOWN_CARRIER_FLOOR members come from SURFACE_EXPANSION_FIELDS"
tech-stack:
  added: []
  patterns:
    - "one factored `ranked -> unit-best -> filtered` SQL fragment serving BOTH the list and the count"
    - "AST shape-prohibition (no transformation of an exact count) paired with a runtime exhaustion identity"
    - "corpus-wide probe built from the SAME pipeline fragment under the SAME parameters as the call"
key-files:
  created:
    - tests/test_discovery_work_expansion.py
  modified:
    - shared/discovery_service.py
    - shared/discovery_surface_projection.py
    - web/discovery.py
decisions:
  - "The enabled-band filter moves to the RESOLVED displayed band when the anchor triple is supplied, and is unchanged when it is not -- stated in the docstring because it is a silent change under the old call shape"
  - "SURFACE_EXPANSION_FIELDS names NEITHER raw band pair: the surface shows the weaker band, and handing it both pairs invites a renderer to re-derive the comparison"
  - "ROW_NUMBER partitions by (work_id, unit_key) -- behaviour-identical on the per-work form, and what lets the corpus-wide probe reuse the same fragment"
  - "The real-artifact probe FAILS BY NAME (never skips) when no artifact resolves -- see Findings for the CI consequence"
metrics:
  duration: "~4h (incl. one network-interruption resume)"
  completed: 2026-08-04
  tasks: 3
  commits: 3
  tests_added: 56
---

# Phase 136 Plan 21: PANEL-02 Work-Witness Expansion Summary

The "Other manuscripts matching ⟨work⟩" expansion now carries both sides' relation kind, displays the
weaker band of the pair, names every carrier by library and shelfmark, reports an exact count-query
total with no capped or approximate alternative reachable on any branch, and turns a failed query into
a named outage instead of a confident zero.

## What was actually wrong

`get_work_witnesses()` returned nine keys — the OTHER carrier's `claim_type` and band, and nothing
else. No anchor relation, no anchor band, no `library_code`, no `shelfmark_display`, no `band_label`,
no total; `manuscript_display` was not joined in `_WORK_WITNESSES_RANKED_CTE_SQL` at all. And every
query failure was swallowed by `except Exception: return []`. "Wiring an existing field through" was
never possible: there was no anchor field to wire, no name to render, and no total to report.

## Task-by-task

| Task | Commit | What landed |
|---|---|---|
| 1 | `ce46d48c` | `manuscript_display` in the shared CTE; the anchor triple (all-three-or-none); `_resolve_displayed_band` via `_band_rank`; `band_label` from `serialize_banded_claim`; the factored pipeline; `_project_work_witnesses` brought to parity |
| 2 | `802b8004` | `_query_work_expansion -> (rows, total)` (raising); the exact count query; `SURFACE_EXPANSION_FIELDS` + `surface_safe_expansion` registered in `_ALL_ALLOWLISTS`; `get_work_expansion_enveloped{,_async}` + the `web/discovery.py` wrapper |
| 3 | `6a03e69b` | Three separate key-set contract tests (internal / public / envelope), the honesty invariants in both languages, and six negative controls |

## Measured evidence (artifact-specific — the next bake will move it)

| Fact | Value |
|---|---|
| Artifact probed | `discovery_data/discovery-public-136rebuild.db` |
| `meta.audience` | `public` |
| Largest EXPANSION `work_id` | `w000112` |
| Its distinct witness units | **5,684** (matches the plan's measured figure exactly) |
| Derived cap below the maximum | 5,683 |
| Derived cap above the maximum | 5,685 (< the 10,402-unit synthetic floor) |
| Count latency, sampled over 300 works | p50 **1.8 ms** / p95 **39.1 ms** / p99 425.7 ms |
| Count latency, the 5,684-unit worst case | **457.8 ms** (best of 5) |
| Browse-path budget (`docs/specs/discovery-budgets.md` §1.1) | added latency p95 ≤ 150 ms; per-query timeout ≤ 2 s |

**The count FITS its budget.** p95 39 ms sits well inside the 150 ms added-latency cap, and the
absolute worst case (594 ms observed) inside the 2 s per-query timeout with ~3.4× headroom. No STOP
condition was reached, so no approximate total was ever a live question.

Query plan on the real artifact (unchanged shape — the added join did not turn the expansion into a
scan):

```
SEARCH dc USING INDEX ix_discovery_claim_work_id (work_id=?)
SEARCH de USING INDEX sqlite_autoindex_discovery_evidence_1 (evidence_id=?)
SEARCH wum USING INDEX sqlite_autoindex_witness_unit_members_1 (sys_id=?) LEFT-JOIN
SEARCH md USING INDEX sqlite_autoindex_manuscript_display_1 (sys_id=?) LEFT-JOIN
```

## Mutations run — what failed, and what did not

### Cap controls (criteria (v), (vi), (vii)). Cap values derived from 5,684, never hardcoded.

| # | Mutation | (iii) synthetic | (iv) real | (vii) anchored | (vi) AST |
|---|---|---|---|---|---|
| M1 | unconditional `min(total, 5683)` | **FAIL** | **FAIL** | **FAIL** | **FAIL** |
| M2 | unconditional `min(total, 5685)` | **FAIL** | pass | — | **FAIL** |
| M3 | `if anchor_sys_id is not None: min(total, 5683)` | pass | pass | **FAIL** | **FAIL** |
| M4 | `if enabled_bands: min(total, 5683)` | pass | pass | **FAIL** | **FAIL** |

M2 is what proves the synthetic floor is load-bearing: a cap sitting just above the real maximum
passes the real-artifact probe and is caught only by the >10,000-unit run. **M3 and M4 are round 10's
finding 3 demonstrated live** — both unanchored runs stay GREEN while only the AST walk and the
anchored/band-filtered exhaustion catch them. Neither control substitutes for the other.

The AST failure message is actionable, e.g.:

```
AssertionError: the exact count is transformed before the envelope
[walk 1: _query_work_expansion] at shared/discovery_service.py:2172
-- min() over 'total': total = min(total, 5683)
```

### Task-3 negative controls — one property each, each observed raising its NAMED failure

| # | Mutation | Test that failed | Message |
|---|---|---|---|
| NC1 | drop `relations_differ` from the projection | `test_public_row_key_set_equals_the_expansion_allowlist` | "the public expansion row no longer carries `'relations_differ'`" |
| NC2 | substitute the anchor's band for the weaker one **in SQL** | `test_stronger_anchor_displays_the_other_carriers_band` | `assert 'expert_verified' == 'screening_rb'` |
| NC2b | the same substitution in the **pure helper** | `test_sql_path_and_pure_helper_agree_on_every_field_both_compute` | "SQL/pure disagreement on `'displayed_evidence_source'`" |
| NC3 | return the page length as the total | `test_total_is_the_full_count_not_the_length_of_the_returned_page` | "the page length was substituted for the total" |
| NC4 | drop `library_code` from the projection | `test_public_row_key_set_equals_the_expansion_allowlist` | "the public expansion row no longer carries `'library_code'`" |
| NC5 | count query swallows its exception into a zero | `test_each_failed_query_is_an_outage_never_ok_with_zero[count]` | "a failed count query reported `ok` — the false-zero class 136-14 fixed" |

**NC2 initially PASSED, and that is a finding rather than a footnote.** The plan names four negative
controls plus one; the "substitute the anchor's band" control was first written against
`_resolve_displayed_band`, and the suite stayed green — because the weaker-band rule has TWO
implementations (the SQL `CASE` on the production path and the Python helper on the DB-free path), and
mutating only the Python one leaves the path every real reader takes untouched. The control was split
into NC2 (SQL) and NC2b (pure helper), so the property is now guarded on both. This is exactly the
"a test that cannot fail" class the phase has produced most often; it was caught only because the
mutation was actually run.

## Acceptance criteria

### Task 1 — ALL MET

- 11+ behaviours, one test each — **MET** (17 tests for Task 1).
- Differing-relations fixture asserting both kinds present and distinct — **MET**.
- Same-relation fixture asserting `relations_differ` false — **MET**.
- Stronger-anchor / weaker-anchor displayed-band fixtures — **MET**.
- `_band_rank` receives BOTH `(evidence_source, confidence_band)` per side, proved by two fixtures
  differing only in the anchor's evidence source (`propagated/corroborated` rank 3 vs
  `track1_direct/screening_canon` rank 5, straddling the carrier's `screening_rb`) — **MET**.
- Renderability asserted POSITIVELY on a seeded fixture: non-null, non-empty `library_code`,
  `shelfmark_display` and `band_label`, with `band_label` equal to `serialize_banded_claim` over the
  RESOLVED pair and explicitly unequal to the raw pair's label — **MET**.
- Companion fixture omitting one `manuscript_display` row: the row is still returned, nulls +
  `display_missing` true — **MET**.
- `EXPLAIN QUERY PLAN` over the exact amended statement, index-driven on `discovery_claim.work_id` and
  asserted not to SCAN — **MET**.
- Two filtering tests (stronger anchor / weaker anchor excluding an enabled carrier) — **MET**.
- No-anchor path filters exactly as before — **MET**.
- Weaker band chosen via `_band_rank`; AST scan finds no second ordering over `confidence_band` —
  **MET**.
- Nine legacy keys intact with null anchor fields; **every pre-existing `get_work_witnesses` test
  passes unmodified** (98 in `test_discovery_service.py` + `test_discovery_composition.py`) — **MET**.
- Full 8-combination anchor matrix, 2 valid / 6 raising — **MET**.
- The error names present AND missing fields — **MET**.
- SQL/pure agreement over every field both compute — **MET** (`band_label` is the sole exclusion, and
  the test asserts the two key sets are otherwise identical rather than merely comparing what happens
  to overlap).
- No `band_precision` join in any expansion query — **MET**.

### Task 2 — ALL MET

- 9+ behaviours — **MET** (24 tests).
- **Three SEPARATE forced-failure tests** (list/page, member, count), each `unavailable` with
  `reason='query_failed'`, none `ok`-with-zero — **MET**, with a deviation on the induction mechanism
  (see Deviations).
- Legacy list API still returns `[]` on all three — **MET**.
- Multi-page: `total` exceeds `len(items)` and equals the sum across pages — **MET**.
- Count honours anchor exclusion and the band filter — **MET**.
- Count built from the SAME factored fragment; asserted structurally (`count_sql.startswith(pipeline)`
  and `count_params == pipeline_params`) plus a scan proving the count tail writes no second
  `ROW_NUMBER` / `PARTITION BY` / `GROUP BY` / `WHERE` / `DISTINCT` / `LIMIT` — **MET**.
- Count and list agree under a WEAKER-anchor filter (the filtering stage decides) — **MET**.
- Count timeout → `timeout`, page length never substituted — **MET**.
- (i) no approximation key in `meta` on `ok` OR on an outage — **MET**.
- (ii) source scan with comments/docstrings excluded — **MET**, scoped to the 14 functions this plan
  added or rewrote (see Deviations).
- (iii) exhaustive pagination on **10,402 units** (> 10,000) — **MET**.
- (iv) real artifact, probed UNANCHORED and UNRESTRICTED, ranking from the SAME pipeline fragment
  under the SAME parameters, with the probe-validity assertion (`returned total == ranked count`) —
  **MET** (`w000112`, 5,684 units, walked to exhaustion).
- (v) two derived cap mutation controls, both observed failing as specified — **MET**.
- (vi) AST assertion + three mutation controls (unconditional, anchor-conditional, band-conditional),
  each observed failing BY NAME — **MET**.
- (vii) exhaustion through an ANCHORED, BAND-FILTERED shape with **10,301** surviving units, with
  assertions that each filter individually bites — **MET**.
- Envelope key set exactly four, `meta` naming anchor mode and filter basis — **MET**.
- `SURFACE_EXPANSION_FIELDS` in `_ALL_ALLOWLISTS` + a companion proving the guard is load-bearing —
  **MET**.
- Count latency recorded against the budget — **MET** (above).
- `tests/test_no_await_sync_function.py` passes — **MET**.

### Task 3 — ALL MET

- Three separate exact-key-set tests, each proved to fail on BOTH a missing and an unexpected key
  (`_rejects_missing_and_extra`) — **MET**.
- Public row EQUALS `SURFACE_EXPANSION_FIELDS`, names the excluded internal-only keys explicitly —
  **MET**.
- Envelope four keys on `ok` AND on an outage — **MET**.
- Module docstring records the pre-plan nine-key shape and the swallowed-exception path — **MET**.
- Five negative controls, one property each, specific named failures — **MET** (six were run; the
  fifth split in two, see above).
- No `review_overlay` / precision / interval, in both languages, on a `human_confirmed` carrier —
  **MET**.
- Projection strips the internal-only keys, named — **MET**.

## Deviations from plan

**1. [Rule 3 — blocking] The three forced failures are induced by injecting a real
`sqlite3.OperationalError` at each statement, not by dropping a table.**
The plan suggested "e.g. dropping the table the query reads". The count, the row query and the member
query read the SAME four tables and run in that order, so dropping a table can only ever exercise
whichever runs FIRST — which is precisely the "one combined 'a query failed' test" the criterion
rejects. Each is therefore failed individually by a connection wrapper that raises a genuine
`sqlite3.OperationalError` when the statement matches that query's signature: a real driver failure
inside the query path against a real-shaped fixture, not a faked return value. A companion test
(`test_a_genuinely_dropped_table_is_an_outage_not_a_zero`) ALSO drops a real table, so the ordinary
missing-table shape is covered too.

**2. [Rule 3 — blocking] The (ii) source scan is scoped to the 14 functions this plan added or
rewrote, not to the whole module.**
A module-wide scan fails on `get_findings_enveloped`'s legitimate, pre-existing `approximate_total`
(plan 136-14's findings page, which has its own documented cap knob). The alternatives were to weaken
the pattern until it caught nothing, or to scope it. The scoped list is explicit and includes the web
wrapper and the projection helper, so a new approximate total anywhere on the expansion path is
caught.

**3. [Rule 2 — correctness] The `ROW_NUMBER` partition became `(work_id, unit_key)`.**
Behaviour-identical on the per-work form (`work_id` is a bind-constant there, asserted by every
pre-existing test still passing), and it is what makes the SAME factored fragment correct for the
corpus-wide probe, where a `unit_key` can legitimately recur across works. Without it the probe's
ranking would have collapsed units across works and silently reported the wrong maximum.

**4. [Rule 2 — correctness] `_build_work_witnesses_ranked_cte_sql(restrict_work_id=False)` exposes a
corpus-wide form.** The service never calls it; only the cardinality probe does. It exists because
criterion (iv) requires the ranking to come from the same fragment "imported, not retyped", and the
per-work form cannot rank every work.

**5. [logging] New log sites log the exception TYPE NAME only.**
Standing constraint 5. `work_id` is still interpolated: it is a minted opaque identifier (verified on
the live artifact — `w000001`-shaped), it is caller-supplied, and every sibling wrapper in both modules
already logs it. No exception object and no artifact ROW value is interpolated anywhere in the new
code.

**6. `meta` carries a third key, `anchor_excluded`.** `anchor_mode` names whether the anchor IDENTITY
was supplied (the axis the filter basis depends on); `anchor_sys_id` (unit exclusion) is an
independent axis and reporting it under the same key would have made a 4-way space read as 2-way.
Criterion (i) is an absence assertion, so this does not over-constrain it.

## Findings for the owner

**F1 — the real-artifact probe FAILS BY NAME on any box without a conforming artifact, including
CI.** This is what the plan requires ("never a skip, never a pass"), and I have not softened it. The
consequence is concrete and you should decide on it:

- `discovery_data/` is gitignored, so CI and every worktree have no artifact. Two tests
  (`test_total_survives_exhaustive_pagination_on_the_real_largest_expansion` and
  `test_synthetic_floor_strictly_exceeds_the_measured_real_maximum`, which share the
  `derived_mutation_caps` helper) will FAIL there rather than skip.
- **The repository manifest currently does NOT resolve either**, and correctly so:
  `discovery_data/manifest.json` still points at the pre-rebuild
  `discovery-v1-33499…db`, which has no `meta.audience` and is missing `discovery_identification` and
  `manuscript_display`. The probe refuses it rather than measuring a stale artifact — the fail-closed
  behaviour working as designed. It will resolve once 136-13 deploys the rebuild manifest.
- Everything in this plan was therefore run with
  `DISCOVERY_EXPANSION_PROBE_DB=C:/Genizahsearch/discovery_data/discovery-public-136rebuild.db`.
  **Without that env var set, the module is 54 passed / 2 failed** (measured, not predicted). The
  failure message names the env var and the manifest condition.

Your options, none of which I took unilaterally: (a) leave it, and set the env var in the one CI job
that has an artifact; (b) point `discovery_data/manifest.json` at the rebuild now, which fixes local
runs but not CI; (c) amend the criterion to permit a skip when NO artifact exists at all while keeping
the hard failure when one exists but does not conform. (c) is the only one that makes an unattended
`pytest tests/` green, and it is a criterion change, so it is yours to make.

**F2 — the module takes ~105 s.** Dominated by the two >10,000-unit exhaustion walks (~53 pages each,
count + rows per page) and the real-artifact probe (~9 s corpus-wide ranking + 29 pages). The
cardinality floor is load-bearing (M2 proves it), so this is the cost of the guarantee rather than
slack to trim.

**F3 — pre-existing dead branch, untouched.** `enabled_bands_list = list(enabled_bands) if
enabled_bands else None` followed by `if enabled_bands_list is not None and len(...) == 0` is
unreachable for lists/tuples (an empty iterable is falsy) and reachable only for a spent generator.
It predates this plan; I preserved it verbatim rather than change a behaviour outside scope.

**F4 — concurrent edits observed, not touched.** Another agent modified `web/discovery_assets.py`,
`tests/test_findings_page.py`, `.github/workflows/ci.yml` and added two `scripts/` files while I was
running. None were staged in any of my three commits; all three commits name my four files explicitly.

## Needs ticking (I did not touch STATE.md / ROADMAP.md / REQUIREMENTS.md)

- `PANEL-02` — the expansion half is now implemented and enveloped. The requirement also covers the
  panel's rendering, which is 136-17's (wave 9) work; tick per your convention for split requirements.
- `DATA-01` — "a projection displays the WEAKER of the two claims' bands" is satisfied on this
  surface.
- Phase 136 plan counter: 136-21 complete, closing wave 7.

## Self-Check: PASSED

- `shared/discovery_service.py` — FOUND
- `shared/discovery_surface_projection.py` — FOUND
- `web/discovery.py` — FOUND
- `tests/test_discovery_work_expansion.py` — FOUND
- `ce46d48c` — FOUND
- `802b8004` — FOUND
- `6a03e69b` — FOUND
