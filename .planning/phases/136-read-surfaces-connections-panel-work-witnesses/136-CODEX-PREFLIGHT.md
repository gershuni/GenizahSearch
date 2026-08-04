# Phase 136 — Codex pre-flight audit (plans)

**Run:** 2026-08-02 · `codex exec --dangerously-bypass-approvals-and-sandbox -C C:\Genizahsearch`
**Target:** the 19 execution plans (`136-01`…`136-19`), after the internal `gsd-plan-checker`
passed them twice (revision 1, commit `b37b0d7a`).
**Grounding:** live repo + the deployed asset
`discovery_data/discovery-v1-33499c5b….db` (Codex opened and queried it).
**Brief:** `_tmp/136-codex-preflight-brief.md`

> **VERDICT: rework** — 10 HIGH · 1 MEDIUM · 1 LOW.
> The internal checker validates plan-internal consistency; it structurally cannot see
> plan↔code drift. Every finding below is cited to a file and line.

## Verbatim findings

1. **ISSUE (HIGH)** — The symbols and listed signatures exist, but `get_work_witnesses()` does not have the return shape assumed by plan 136-14. Its rows contain only the selected other carrier’s `claim_type` and band, with neither the anchor’s relation/band nor a total ([shared/discovery_service.py](C:/Genizahsearch/shared/discovery_service.py:722), [return shape](C:/Genizahsearch/shared/discovery_service.py:831)). Plan 136-14 nevertheless calls this field wiring and requires both sides, the weaker band, and a real total without changing the function ([136-14-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-14-PLAN.md:186)). Fix by passing the already-loaded anchor relation and band explicitly into the wrapper, or joining the anchor claim, and add a count query/envelope contract. A bounded list cannot supply a real total.

2. **ISSUE (LOW)** — The live `PRAGMA table_info` results agree that `coverage_ppm`, `coverage_status`, `band_rank`, visibility axes, tri-state novelty, `discovery_identification`, and `manuscript_display` are new. However, `works.genre` already exists and is NULL on all 1,269 rows; plan 136-01 describes it as a schema addition ([136-01-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-01-PLAN.md:255)). Fix the contract wording to say the existing column becomes populated and constrained; do not generate an `ADD COLUMN genre` migration.

3. **ISSUE (HIGH)** — D-13g is real: SQL filters routing before `is_default_eligible()` can apply its unconditional human-confirmed override ([discovery_service.py](C:/Genizahsearch/shared/discovery_service.py:620), [discovery_band_labels.py](C:/Genizahsearch/shared/discovery_band_labels.py:295)). The live counts need correction: 19/121 applies to all human-confirmed evidence, but this page query reads display evidence, where 14/116 are `review_only`. More importantly, plan 136-11 materializes only shipped identifications ([136-11-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-11-PLAN.md:147)), while plan 136-14 joins that table when restoring review-only human-confirmed rows ([136-14-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-14-PLAN.md:120)). An inner join drops them again; a left join leaves them without a bucket or reason. Materialize rows eligible under `shipped OR human_confirmed`, and update the row-count invariant and regression counts accordingly.

4. **CONFIRM** — Materializing the live 64,509 distinct manuscript/work identifications removes the 268K-row deduplication from request time; the proposed ordering/look-up indexes plus mandatory full-filter benchmarks are an adequate PERF-01 approach.

5. **ISSUE (HIGH)** — The public graph has an unresolved canonical-identity edge. The live `works` table has 15 duplicated `canonical_work_id` groups, including three with different titles and mixed source corpora. Joining the proposed 64,509-row identification grain to `works` on `canonical_work_id` yields 65,587 rows. Yet the materialized schema has no representative/display `work_id` ([136-01-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-01-PLAN.md:245)), while the service plans to join `works` for identity metadata ([136-14-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-14-PLAN.md:124)). This makes title selection and `identity_visibility` ambiguous and can let a private contribution affect a shared aggregate. Add a deterministic `display_work_id` FK—or materialize the masking-safe display dimensions directly—and recompute each identification from surviving public claims during projection.

6. **ISSUE (HIGH)** — The fifth-placeholder and stale-generation mechanism is real ([browse_enrichment.py](C:/Genizahsearch/web/pages/browse_enrichment.py:38), [stale guard](C:/Genizahsearch/web/pages/browse_enrichment.py:318), [renderer seam](C:/Genizahsearch/web/pages/browse_enrichment.py:488)). The missing part is the manuscript page list: plan 136-14 says the new method takes “the browse page’s own page list” ([136-14-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-14-PLAN.md:175)), but `BrowsePage` contains only the current `uid`, page number, and totals—no page IDs ([web/services.py](C:/Genizahsearch/web/services.py:89)). Add an off-loop, volume-aware accessor returning manuscript page IDs, or explicitly carry those IDs on `BrowsePage`; update plan 136-17 to pass them. This is new plumbing, contrary to its current wording.

7. **ISSUE (HIGH)** — The no-review-badge rule is not airtight. Plan 136-14 routes every presentation through `serialize_banded_claim()` ([136-14-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-14-PLAN.md:133)), but that serializer always emits `review_overlay` ([discovery_band_labels.py](C:/Genizahsearch/shared/discovery_band_labels.py:359)), including “Expert-reviewed” for human-confirmed rows ([discovery_band_labels.py](C:/Genizahsearch/shared/discovery_band_labels.py:144)). Renderer-only assertions do not prevent the badge from reaching an envelope or JSON payload. Introduce a surface-safe allowlisted projection that omits `review_overlay`, precision, and CI fields, and assert the forbidden keys/values are absent from envelopes, JSON, errors, and markup.

8. **ISSUE (MEDIUM)** — Plan 136-04 requires only that a zero-owner-label fixture “makes the harness FAIL” ([136-04-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-04-PLAN.md:203)). A broad exception assertion could still pass after the intended guard is removed because division by zero or another incidental error also fails. Require a specific fail-closed exception/exit code and message such as `no owner-provenance labels`; add a mutation test that bypasses the explicit denominator guard and proves the test then fails.

9. **CONFIRM** — The live asset has exactly 144,294 shipped `track1_direct` rows with legacy `is_new=0`, and plan 136-12 explicitly treats that value as unchecked, writes tri-state status independently, and forbids surface derivation from the legacy boolean ([136-12-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-12-PLAN.md:82)).

10. **ISSUE (HIGH)** — The masking commands are syntactically valid, but database coverage is incomplete. The scanner explains that SQLite must be scanned cell-by-cell because raw asset scanning can miss page/overflow content ([check_atlas_masking.py](C:/Genizahsearch/scripts/check_atlas_masking.py:21)); plans 136-08 and 136-13 require only `--scan-asset` for the databases ([136-08-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-08-PLAN.md:152), [136-13-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-13-PLAN.md:93)). In addition, plan 136-19 permits a recorded pattern-file skip while still claiming flag-on readiness ([136-19-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-19-PLAN.md:98)). Require, for each database, `--strict --scan-repo --scan-asset <db> --scan-sqlite <db>`, with cell/schema positive controls. An unavailable pattern file must block readiness, not complete with a skip.

11. **CONFIRM** — The preservation plan streams every pre-existing column of the six mutable core tables, pins expectations before rebuilding, separately controls the authorized `band_precision` change, recomputes frame/population hashes, verifies every graded-card binding, and keeps the CERT-01 preregistration/verifier untouched ([136-05-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-05-PLAN.md:93)).

12. **CONFIRM** — The two serial integration passes are realistic against the builder: its DDL, two evidence insertion sites, and final orchestration are centralized, while the rule modules can be built independently; plans 136-11 and 136-12 serialize their edits rather than attempting concurrent changes to the builder.

13. **ISSUE (HIGH)** — The two surface deploys can safely share one rebuilt asset, but the selected asset is wrong. Plan 136-13 explicitly deploys the private DB and merely stages the public projection ([136-13-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-13-PLAN.md:146)). Runtime has one manifest-selected database and ignores sibling files ([discovery_assets.py](C:/Genizahsearch/web/discovery_assets.py:165)); there is no audience selection. When the flag is enabled, public routes therefore read the private asset. Make production `manifest.json` select the public projection and retain the private DB outside the public loader, or add an explicit public/private loader boundary whose public routes can only resolve the public artifact.

14. **ISSUE (HIGH)** — Plan 136-17 specifies `await run.io_bound(_sync_fn)` while also requiring reads through the enveloped async wrapper ([136-17-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:106)). Existing wrappers already await service async methods ([web/discovery.py](C:/Genizahsearch/web/discovery.py:60)), which offload via `_run_off_loop` ([discovery_service.py](C:/Genizahsearch/shared/discovery_service.py:991)). The literal plan either double-offloads or calls an async function as a sync worker and receives a coroutine object; merely binding `page_client` does not pass it explicitly. Define one boundary: preferably a synchronous enveloped callable invoked once via `run.io_bound(..., client=page_client)`, with user state captured beforehand, or directly await the existing async wrapper if it remains strictly public-data-only. Add a guard against both direct sync SQLite and nested offloading.

15. **ISSUE (HIGH)** — Most F-01–F-15 dispositions are represented, but F-05 and F-14 are walked back operationally. F-05 requires a closed-graph public projection, yet plan 136-13 deploys the private DB; F-14 requires distinct outage-versus-zero envelopes, yet plan 136-17 gives the async envelope an incoherent `run.io_bound` call path. The accepted dispositions are recorded at [136-CONTEXT.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-CONTEXT.md:602). Resolve findings 5, 13, and 14 before the plans can claim those dispositions are honored.

16. **ISSUE (HIGH)** — No plan updates the startup readiness contract for the two new required tables. `_REQUIRED_TABLES` still lists only the v1 tables ([discovery_assets.py](C:/Genizahsearch/web/discovery_assets.py:83)), and readiness only checks that set before setting `ready=True` ([discovery_assets.py](C:/Genizahsearch/web/discovery_assets.py:247)). Because the schema marker remains `discovery-v1`, an old or partially deployed sidecar can pass `discovery_available()`, expose the nav, and then fail when queries touch `discovery_identification` or `manuscript_display`. Add both tables and their expected-row meta keys to startup validation—or bump the schema version—and add rollback/partial-asset tests proving availability stays false.

VERDICT: rework because the public/private deployment boundary, human-confirmed materialization, canonical identity grain, and service/UI contracts are unsafe as written
HIGH: 10   MEDIUM: 1   LOW: 1

---

## Round 2 — convergence check (2026-08-02)

**Target:** revision 2 (commit `eb2bda4f`) — 21 plans / 63 tasks / 9 waves.
**Brief:** `_tmp/136-codex-preflight-r2-brief.md`

> **VERDICT: rework** — **3 HIGH · 2 MEDIUM · 2 LOW** (was 10/1/1).
> 8 of 12 round-1 findings RESOLVED and re-confirmed against the live DB.
> Survivors cluster in three places: the 136-13 deploy checkpoint still authorizes the
> **private** asset (contradicting its own Task 3), the two wave-8 production deploys run
> **concurrently**, and the work-expansion filter/count contract can still drift.
>
> Note finding #13 is invisible to a `files_modified` check by construction — the conflict is
> two concurrent mutations of **production**, not of the repo.

### Verbatim findings

1. **NOT RESOLVED (HIGH)** — The expansion contract remains internally inconsistent. The weaker band requires `_band_rank(evidence_source, confidence_band)`, but only the anchor relation and band are passed ([136-21:105](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:105), [live helper:175](C:/Genizahsearch/shared/discovery_service.py:175)). More importantly, the plan says filtering applies to the final weaker band while also preserving the existing other-carrier filter unchanged ([136-21:99](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:99), [136-21:119](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:119)). That fails when the anchor is weaker. The count also shares only the raw ranked CTE, while unit selection/filtering is presently separate SQL ([discovery_service.py:797](C:/Genizahsearch/shared/discovery_service.py:797)). Fix by passing `anchor_evidence_source`, factoring the complete ranked→unit-best→filtered CTE for both list and count, and testing both stronger-anchor and weaker-anchor filtering.

2. **RESOLVED** — `genre` is correctly identified as an existing column, its 1,269 NULL rows match the live DB, and both action and acceptance prohibit `ADD COLUMN genre` ([136-01:271](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-01-PLAN.md:271)).

3. **RESOLVED** — The live DB confirms 19/121 across all human-confirmed evidence and 14/116 among display evidence. Materialization now uses `shipped OR human_confirmed`, records the admission rule, and requires a non-NULL bucket/reason for the review-only regression ([136-11:150](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-11-PLAN.md:150), [136-14:171](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-14-PLAN.md:171)).

4. **RESOLVED** — The DB reconfirms 15 duplicate canonical groups, three with differing titles and mixed corpora, and the 64,509→65,587 fan-out. The contract requires a NOT NULL, ordered-total `display_work_id`, exact 1:1 build/service joins, and the public projection recomputes both identifications and the representative from surviving public claims ([136-01:258](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-01-PLAN.md:258), [136-08:141](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-08-PLAN.md:141)).

5. **RESOLVED** — The page-ID accessor is explicitly NEW PLUMBING, bounded and off-loop, with an explicit empty result and a panel test distinguishing “unresolvable pages” from a genuine zero ([136-14:209](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-14-PLAN.md:209), [136-17:137](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:137)).

6. **RESOLVED** — `surface_safe_claim` is explicitly an allowlist, including an unexpected-key control and key-and-value assertions over row, envelope, JSON, error path, and the literal badge ([136-14:148](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-14-PLAN.md:148), [136-14:177](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-14-PLAN.md:177)).

7. **RESOLVED** — The novelty harness now requires the dedicated `no owner-provenance labels` failure and a mutation test proving removal of the denominator guard makes the test fail ([136-04:189](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-04-PLAN.md:189), [136-04:207](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-04-PLAN.md:207)).

8. **NOT RESOLVED (LOW)** — The real CLI accepts the required four flags, database scans and schema-level control are correct, and executable acceptance blocks on a missing pattern file. But the threat register still says an unavailable file is an “explicit skip,” directly contradicting the task ([136-19:98](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-19-PLAN.md:98), [136-19:197](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-19-PLAN.md:197)). Replace that stale mitigation with fail/block wording.

9. **NOT RESOLVED (HIGH)** — The loader gate itself is correctly placed in startup readiness and all public wrappers inherit it. However, 136-13’s blocking owner checkpoint still asks authorization to deploy the private asset and merely stage the public projection ([136-13:150](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-13-PLAN.md:150)), contradicting Task 3’s public-manifest/private-off-box requirement ([136-13:212](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-13-PLAN.md:212)). The checkpoint must authorize exactly the public projection.

10. **RESOLVED** — Plans 136-14/17 define one synchronous enveloped implementation, one `run.io_bound(..., client=page_client)` crossing, captured user state, and guards against both coroutine-to-worker and nested-offload mistakes ([136-17:107](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:107)).

11. **NOT RESOLVED (HIGH)** — F-14 is now preserved end-to-end as four states through the single offload boundary. F-05 is not fully converged because the checkpoint still authorizes the exact private-artifact deployment that its three controls are supposed to prevent; see answer 9.

12. **NOT RESOLVED (MEDIUM)** — Required tables, meta count pairs, missing-key checks, and the pre-rebuild rollback fixture are present. But the schema-marker decision was not made: the executor may bump or not bump it ([136-20:152](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-20-PLAN.md:152)). The preferred bump requires editing the builder and schema document, although 136-20 declares only loader/test files ([136-20:7](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-20-PLAN.md:7)); following it also creates an undeclared same-wave overlap with 136-11. Decide “retain v1 and rely on required-table/count checks,” or explicitly rehome and serialize the bump.

13. **NOT RESOLVED (HIGH)** — The declared graph itself has 21 plans, 63 tasks, no same-wave declared-file overlap, and every dependency points to an earlier wave. Plans 136-20 and 136-21 are correctly placed. But both wave-8 plans perform production deployments concurrently: 136-17 deploys the panel ([136-17:293](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:293)), while 136-18 assumes the panel is already deployed and deploys findings ([136-18:202](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:202)). Serialize these external mutations even though their repository file lists do not overlap.

14. **NOT RESOLVED (HIGH)** — The phase is not safe to execute until answers 1, 9, 12, and 13 are corrected. The new loader is fail-closed, so a private artifact selected after that code is installed becomes an outage rather than a disclosure. But 136-13 simultaneously requires production already have compatible code before the asset copy ([136-13:209](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-13-PLAN.md:209)) and requires asset-first ordering ([136-13:222](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-13-PLAN.md:222)). Combined with the private-deploy checkpoint, that leaves the most important disclosure boundary procedurally ambiguous. Once corrected, I found no further disclosure path that would evade the loader, projection, and masking gates.

15. **NOT RESOLVED (MEDIUM)** — Two additional cleanup problems:

   - No builder-owning plan explicitly writes `meta.audience='private'`; 136-08 merely asserts that the private build carries it ([136-08:187](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-08-PLAN.md:187)). Assign that write and its verifier check to 136-11 or 136-12.
   - `136-VALIDATION.md` still contains stale moved-out validation—five surfaces, `/work/{id}`, and a work-page smoke test—and incorrectly assigns the differing-relation service assertion to 136-14 rather than 136-21 ([136-VALIDATION:62](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-VALIDATION.md:62), [136-VALIDATION:69](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-VALIDATION.md:69), [136-VALIDATION:272](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-VALIDATION.md:272)).

VERDICT: rework because the private/public deployment authorization still contradicts itself, the wave-8 production deploys are unsafely concurrent, and the work-expansion filter/count contract can still drift
HIGH: 3   MEDIUM: 2   LOW: 2

---

## Round 3 — final convergence (2026-08-02)

**Target:** revision 3 (commit `3048832b`) — 21 plans / 63 tasks / **10 waves**.
**Brief:** `_tmp/136-codex-preflight-r3-brief.md`

> **VERDICT: rework** — **1 HIGH · 3 MEDIUM · 2 LOW** (10/1/1 → 3/2/2 → 1/3/2).
>
> **The remaining HIGH is wording, not substance.** Codex confirmed the disclosure-critical
> authorization is resolved ("the only go option names the public projection, and I found no
> active wording authorizing the private database onto the public box") and that **code-first is
> safe**. What survives is that 136-13's objective, task name and `read_first` still say
> *asset-first* while its action and acceptance require *code-first*.
>
> **The one residual disclosure-capable item is LOW-tagged (#7):** 136-19's final verification
> still says an unset masking pattern file is "recorded as a skip", contradicting the
> "never a skip and never a pass" rule the same plan now states elsewhere. If followed and later
> trusted as readiness evidence, unscanned restricted text could survive to Phase 139.
>
> Round 3 also caught a defect **revision 3 introduced**: the checkpoint option was renamed to
> `deploy-public-now` but its automated verification still greps `deploy-now`, so a valid
> authorization fails its own check.

### Verbatim findings

1. **NOT RESOLVED (MEDIUM) — work-expansion contract.** The intended paths are now well specified:

   - Both stronger- and weaker-anchor directions are tested.
   - Evidence source is varied independently.
   - Count and list share the complete filtered pipeline, including a weaker-anchor count/list test.

   However, the three anchor parameters independently default to `None`, without an all-or-none invariant. “Anchor arguments are supplied” is therefore ambiguous for partially populated calls, which can rank with an unranked/default side or produce incomplete relation metadata ([136-21-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:111)). Require exactly all three anchor fields or none, reject every partial combination, and test that matrix. Also change the stale `<done>` statement, which still says only that the count reuses the ranked CTE ([136-21-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:211)).

2. **NOT RESOLVED (HIGH) — deploy authorization/order.** The disclosure-critical authorization is resolved: the only go option names the public projection, and I found no active wording authorizing the private database onto the public box.

   Code-first is safe: the new loader encountering the live old asset fails readiness because the new tables are absent, leaving discovery hidden; after the verified public projection and manifest swap, restart loads the new asset while the public flag remains off.

   But the plan still gives contradictory production instructions:

   - Objective: “asset first” ([136-13-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-13-PLAN.md:38)).
   - Task name: “asset-first production redeploy” ([136-13-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-13-PLAN.md:220)).
   - `read_first`: directs the executor to the runbook’s asset-first ordering ([136-13-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-13-PLAN.md:223)).
   - Action and acceptance instead require code-first ([136-13-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-13-PLAN.md:229)).

   Thus the asset-first clause was not fully deleted, and the plan does not state exactly one order throughout.

3. **RESOLVED — concurrent production mutations.** The external-resource scan is correct and sufficiently complete. Production box/code/asset/manifest mutations occur in waves 5, 8, and 9; the LLM/finding-aid calls and spend belong only to 136-04; owner checkpoints are isolated; staged artifacts have distinct writers; and no plan flips the production flag. I found no additional shared cache, API, or deployment target being concurrently mutated.

4. **NOT RESOLVED (MEDIUM) — schema marker/readiness.** Retaining `discovery-v1` safely rejects the exact current rollback asset: the live DB lacks both new tables. Content-hash verification and the atomic manifest workflow also prevent a half-copied database from becoming live.

   It does not cover every partial-schema case claimed by the plan. Phase 136 also adds required columns to existing tables, but startup validation checks only table presence and row counts ([136-20-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-20-PLAN.md:147), [live readiness loop](C:/Genizahsearch/web/discovery_assets.py:247)). An audience-public asset with both tables and correct counts but a missing required column can pass readiness, expose navigation, and fail only when queried. Either validate the required column sets/types at startup or use a new marker emitted only by the complete builder.

5. **RESOLVED — private audience marker.** Plan 136-12 owns the `meta.audience='private'` write, independently verifies the closed enum, and requires missing/out-of-enum fixtures to fail ([136-12-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-12-PLAN.md:132), [acceptance](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-12-PLAN.md:210)).

6. **RESOLVED — validation cleanup.** The differing-relation assertion is assigned to 136-21 with both filter directions; the three moved rows are struck and labeled Phase 136.1; and masking now says both shipped surfaces ([136-VALIDATION.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-VALIDATION.md:62), [moved rows](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-VALIDATION.md:68), [work-expansion assertion](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-VALIDATION.md:277)).

7. **NOT RESOLVED (LOW) — unavailable masking pattern.** The task, acceptance criterion, and `T-136-19-03` are corrected. But the plan’s final verification still says an unset pattern file should be recorded as a skip ([136-19-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-19-PLAN.md:207)), contradicting “never a skip and never a pass.”

8. **Revision-3 graph: structurally clean, with one newly broken check.** I confirmed 21 plans, 63 tasks, 10 waves, no same-wave `files_modified` overlap, and no dependency pointing to the same or a later wave. The new ordering is correct: 136-18 is wave 9 and depends on 136-17; 136-19 is wave 10 and depends on 136-18.

   Revision 3 did break the checkpoint’s automated verification: the valid option is `deploy-public-now`, but the command still searches for `deploy-now` ([option](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-13-PLAN.md:193), [broken verification](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-13-PLAN.md:206)). A valid authorization therefore fails its check. **MEDIUM.**

   Also, Git confirms that 136-17 changed in revision 3, although it was omitted from the prompt’s changed-file list.

9. **Not yet safe to execute as written.** The private-database disclosure path is closed, and no phase-136 plan turns the flag on. Most remaining defects would manifest as a blocked deployment, hidden surface, service error, or incorrect band placement—not as private-data disclosure.

   The one residual disclosure-capable ambiguity is the masking verification’s “record the skip” sentence: if followed and later trusted as readiness evidence, unscanned restricted text in markup, JSON, copy, or error paths could survive to Phase 139. Fixing that sentence restores the intended fail-closed chain.

10. **Additional competent-reviewer cleanup.** Phase-wide deployment counting remains misleading: the roadmap says “Two deploys,” and 136-17 calls itself the first of the phase’s two deploys, despite the separate wave-5 code-plus-asset production redeploy ([ROADMAP.md](C:/Genizahsearch/.planning/ROADMAP.md:196), [136-17-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:299)). Say “three production mutations / two surface deploys.” **LOW.**

VERDICT: rework because the production deploy plan still gives contradictory asset-first and code-first instructions
HIGH: 1   MEDIUM: 3   LOW: 2

---

## Round 4 — SIGN-OFF (2026-08-02)

**Target:** revision 4 (commit `0939f01c`) — 21 plans / 63 tasks / 10 waves.
**Brief:** `_tmp/136-codex-preflight-r4-brief.md`

> **VERDICT: ship with 2 adjustments — HIGH: 0 · MEDIUM: 1 · LOW: 1.**
> Trajectory across four rounds: **10/1/1 → 3/2/2 → 1/3/2 → 0/1/1.**
>
> Codex sign-off: *"I found no remaining disclosure-capable path."* The public projection, the
> audience gate, the flag-off deployments and the final masking gate are all intact; neither
> remaining defect opens a private-data path.
>
> Both adjustments were applied directly (see below) rather than through another planner round.

### The two adjustments — APPLIED 2026-08-02

1. **MEDIUM — `136-20` Task 2's `<automated>` check asserted only the two table names**, so it could
   report green after the table work while never proving `_REQUIRED_COLUMNS` or `PRAGMA table_info`
   existed at all. Codex: *"the exact action/verification drift this round was meant to catch."*
   **Fixed:** the source check now asserts `_REQUIRED_COLUMNS` and `table_info` are present, each with
   its own failure message; acceptance corrected `Six tests` → `Eight tests` and a criterion added
   recording why a table-only check is insufficient.
2. **LOW — `136-21` Task 1 acceptance carried the same stale `Six tests`** after its behaviour list
   grew to eight. **Fixed.**

**Swept for the class, not just the instances:** every task in all 21 plans was checked
programmatically for a stated test count disagreeing with its own `<behavior>` bullet count, in both
`<acceptance_criteria>` and `<done>`. Result: **no mismatches remain anywhere.** (`136-07`'s "Seven"
and `136-08`'s "Six" are correct against their own lists.)

### Verbatim findings


4. **#4 — NOT RESOLVED (MEDIUM).** Substantively, the column set is complete against the phase amendment: all nine affected existing-table columns are named, while every new-table column is covered by the amendment-derived mapping ([136-01:237](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-01-PLAN.md:237), [136-01:248](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-01-PLAN.md:248), [136-20:143](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-20-PLAN.md:143)). `_REQUIRED_COLUMNS`, `PRAGMA table_info`, subset semantics, both dropped-column fixtures, and retaining `discovery-v1` are explicit ([136-20:160](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-20-PLAN.md:160), [136-20:172](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-20-PLAN.md:172), [136-20:201](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-20-PLAN.md:201)).

   However, revision 4 did not extend its own `<automated>` source check: it still asserts only the two table names, so it can pass without `_REQUIRED_COLUMNS` or `PRAGMA table_info` appearing at all ([136-20:196](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-20-PLAN.md:196)). Combined with the six-versus-eight contradiction, this recreates the revision-3 class of “action fixed, verification stale.”

5. **#7 — RESOLVED.** Plan 136-19 now fails the sweep, records masking as not met, and blocks flag-on readiness when the pattern file is missing ([136-19:98](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-19-PLAN.md:98), [136-19:118](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-19-PLAN.md:118), [136-19:207](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-19-PLAN.md:207)). The wave-1 wording is a recorded deferral, not a pass: the criterion remains NOT MET and must be rerun before closure ([136-01:364](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-01-PLAN.md:364)). Plans 136-17/18 still permit a recorded intermediate skip during flag-off deployment ([136-17:291](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:291), [136-18:205](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:205)), but it cannot become readiness evidence because 136-19 requires the actual successful rerun.

6. **#10 — RESOLVED.** The roadmap says three production mutations and two surface deploys ([ROADMAP:196](C:/Genizahsearch/.planning/ROADMAP.md:196)); 136-17 identifies itself as the second production mutation and first surface deploy ([136-17:300](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:300)); 136-18 is the third and last production mutation and second surface deploy ([136-18:208](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:208)).

7. **Revision 4 introduced two adjustments.**

   - **MEDIUM:** 136-20’s automated verification was not updated for `_REQUIRED_COLUMNS`, and its acceptance still says six tests while `<done>` says eight ([136-20:196](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-20-PLAN.md:196), [136-20:199](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-20-PLAN.md:199), [136-20:211](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-20-PLAN.md:211)).
   - **LOW:** 136-21’s acceptance retains the same six-versus-eight stale count ([136-21:95](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:95), [136-21:154](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:154)).

   Otherwise: 21 plans, 63 tasks, 10 waves; no same-wave `files_modified` overlap, no same/later-wave dependency edge, and no newly stale option ID or asserted path.

8. **Sign-off:** not quite safe *as written*, but safe after those two narrow adjustments. Neither defect opens a private-data path. An incompletely implemented readiness-column check would surface as an exposed-then-failing/hidden surface or blocked deployment, while the public projection, audience gate, flag-off deployments, and final masking gate remain intact. I found no remaining disclosure-capable path.

9. **Last look:** the most serious revision-4 item is 136-20’s unchanged automated verifier. It can report green after the table work while never proving the new column mechanism exists—the exact action/verification drift this round was meant to catch. Extend that check to require `_REQUIRED_COLUMNS` and `PRAGMA table_info`, and reconcile its test count; then correct 136-21’s count.

VERDICT: ship with 2 adjustments
HIGH: 0   MEDIUM: 1   LOW: 1

---

# Round 5 — the six UNEXECUTED plans vs. the BUILT code (2026-08-03)

**Run:** 2026-08-03 · `codex exec --dangerously-bypass-approvals-and-sandbox -C C:/Genizahsearch`
**Audited HEAD:** `5ef9b45e` (read-only; worktree untouched).
**Scope:** `136-15`, `136-16`, `136-17`, `136-18`, `136-19`, `136-21` only.
**Brief:** `_tmp/136-codex-preflight-r5-brief.md` (masking-scanned clean before the run).
**Log:** `_tmp/136-codex-preflight-r5.log` (gitignored).

> **VERDICT: rework** — 1 BLOCKER · 8 HIGH · 3 MEDIUM · 1 LOW · 4 CONFIRM.

**Why a round 5 after round 4 signed off at 0 HIGH.** Round 4 was correct for the repo of
2026-08-02. Since then plans 136-01…136-14 executed (144 commits), the asset was rebuilt, the
public projection was built, and 136-13's output was **deployed to production**. The six remaining
plans were written against a repo where the discovery service layer, the rebuilt asset and the
public projection did not exist. Additionally, **owner rulings S, T and U were all recorded
2026-08-03** — after sign-off — so no plan can reference them.

Two findings below were predicted before the run (ruling T's reachability, ruling R's title
routing); the rest were not.

## Verbatim findings

1. **BLOCKER — 136-16 / 136-18 — Ruling U has no implementation owner.**  
   The page header is specified only as title, sub-line, and caveat in [136-16-PLAN.md:138](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-16-PLAN.md:138); the completed-page plan likewise never adds the contribution headline or three sub-numbers in [136-18-PLAN.md:40](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:40). The existing service returns paged findings and per-query metadata only ([discovery_service.py:1431](C:/Genizahsearch/shared/discovery_service.py:1431), [discovery_service.py:1501](C:/Genizahsearch/shared/discovery_service.py:1501)); there is no artifact-backed launch-stat reader. Read-only queries found 9,523 in the deployed public artifact but 10,432 on the private rebuild, proving these numbers already differ between current artifacts.  
   **Change:** assign a plan to add a version-aware artifact query/envelope for the main-pool contribution total and its three shades, render it as the lead framing, and test that neither code nor translations contain numeric literals for those values.

2. **HIGH — 136-16 / 136-18 — “More matches” is mentioned but not acceptance-gated.**  
   Plan 136-16 mentions a toggle at [136-16-PLAN.md:146](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-16-PLAN.md:146), but none of its acceptance criteria at [136-16-PLAN.md:166](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-16-PLAN.md:166) asserts that the control exists, is prominent, works, uses match framing, or is unnumbered. Plan 136-18 delegates membership to the bucket at [136-18-PLAN.md:106](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:106) but has the same omission. The service already supports `BUCKET_MORE` ([discovery_service.py:293](C:/Genizahsearch/shared/discovery_service.py:293), [discovery_service.py:457](C:/Genizahsearch/shared/discovery_service.py:457)). The deployed artifact contains 2,189 non-Bible `fills_gap` rows there versus 769 in the main pool. A broken page omitting the control could satisfy every current criterion.  
   **Change:** require an always-visible, directly operable control; test the main→more interaction against a populated fixture, bilingual match-framing text, no attached count, and mobile visibility.

3. **HIGH — 136-15 — the pure-model input contract is still the pre-envelope “service rows” abstraction.**  
   `build_panel_rows(service_rows, ...)` is specified at [136-15-PLAN.md:88](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-15-PLAN.md:88), yet the model must distinguish four service states at [136-15-PLAN.md:140](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-15-PLAN.md:140). Live code now supplies separate envelopes for claims, page-ID resolution, manuscript works, related count, and related rows ([web/discovery.py:91](C:/Genizahsearch/web/discovery.py:91), [web/discovery.py:122](C:/Genizahsearch/web/discovery.py:122), [web/discovery.py:160](C:/Genizahsearch/web/discovery.py:160), [web/discovery.py:176](C:/Genizahsearch/web/discovery.py:176)). Bare rows cannot distinguish `ok/0` from an outage or carry `meta.resolved`. Synthetic row fixtures could therefore pass while real composition is broken.  
   **Change:** define an explicit `PanelServiceBundle` or equivalent input containing all envelopes, their totals and metadata; specify status arbitration and unresolved/truncated page-scope behavior; add contract tests using exact live envelope shapes.

4. **HIGH — 136-17 — its single-offload callable does not exist, and the accessor envelope is underspecified.**  
   The plan requires one synchronous enveloped callable via `run.io_bound` at [136-17-PLAN.md:108](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:108). Live `web.discovery` instead exposes separate async wrappers, each already dispatching internally ([web/discovery.py:91](C:/Genizahsearch/web/discovery.py:91), [discovery_service.py:2006](C:/Genizahsearch/shared/discovery_service.py:2006)). The page-ID result is itself `{status, items, total, meta}` with `resolved`, `truncated`, and `volume_ie` ([web/discovery.py:122](C:/Genizahsearch/web/discovery.py:122)), while the plan merely says to pass “its result” at [136-17-PLAN.md:138](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:138).  
   **Change:** specify a public synchronous bundle callable that executes the raw page-ID accessor and all panel reads inside one worker, passing `page.volume_ie`, then returns one composite envelope. Explicitly unpack and branch on `status` and `meta.resolved`; do not import the private `_service` or nest the async wrappers.

5. **HIGH — 136-21 / 136-17 — the expansion still cannot render its promised carrier rows.**  
   Plan 136-17 requires library and shelfmark for each other carrier at [136-17-PLAN.md:204](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:204). Plan 136-21 adds anchor relation/band fields only at [136-21-PLAN.md:138](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:138). The live return dictionary has only nine keys and no library, shelfmark, displayed evidence source, or band label ([discovery_service.py:1808](C:/Genizahsearch/shared/discovery_service.py:1808)); `manuscript_display` is not joined in its CTE ([discovery_service.py:610](C:/Genizahsearch/shared/discovery_service.py:610)).  
   **Change:** extend the factored query with `manuscript_display`, return `library_code` and `shelfmark_display`, and return the resolved displayed `(evidence_source, confidence_band)` pair plus a surface label. Add exact non-null assertions on a real-shaped expansion fixture.

6. **HIGH — 136-21 — its public projection file and actual envelope shape are missing from the plan.**  
   The files list excludes `shared/discovery_surface_projection.py` at [136-21-PLAN.md:7](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:7), although Task 3 requires a surface-safe expansion projection at [136-21-PLAN.md:250](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:250). Current allowlists contain no expansion row type ([discovery_surface_projection.py:230](C:/Genizahsearch/shared/discovery_surface_projection.py:230)). The plan also repeatedly states `{status, items, total}` ([136-21-PLAN.md:175](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:175), [136-21-PLAN.md:183](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:183)), but the live closed contract is `{status, items, total, meta}` ([discovery_surface_projection.py:318](C:/Genizahsearch/shared/discovery_surface_projection.py:318)).  
   **Change:** add the projection module to `files_modified`, define a dedicated expansion allowlist, and separately pin the internal and public exact key sets—including `meta`.

7. **HIGH — 136-21 — an item-query failure can still become a false zero.**  
   The plan promises four statuses and tests only count timeout at [136-21-PLAN.md:179](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:179). The live `get_work_witnesses()` catches every query exception and returns `[]` ([discovery_service.py:1822](C:/Genizahsearch/shared/discovery_service.py:1822)). An envelope implemented by wrapping that legacy method can therefore return `ok` with no items after a failed list/member query—the exact false-zero class fixed for claims in 136-14.  
   **Change:** factor a raising internal query helper, retain `[]` only in the legacy list API, and let the envelope map failures. Add separate forced failures for page query, member query, and count query.

8. **HIGH — 136-18 — “matched-letter count” no longer exists on findings rows, and the criterion is vacuous.**  
   The row anatomy requests a matched-letter count at [136-18-PLAN.md:83](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:83). The current identification grain exposes `max_coverage_ppm`, not matched letters ([discovery_service.py:311](C:/Genizahsearch/shared/discovery_service.py:311)); the exact surface allowlist likewise has no `matched_letters` ([discovery_surface_projection.py:194](C:/Genizahsearch/shared/discovery_surface_projection.py:194). The existing test explicitly warns that the surface must not imply a letter count ([test_discovery_findings_query.py:312](C:/Genizahsearch/tests/test_discovery_findings_query.py:312)). The only planned acceptance test asserts omission for a missing value at [136-18-PLAN.md:119](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:119), so omitting the element for every row passes.  
   **Change:** replace “matched-letter count” with qualified matched-letter coverage sourced from `max_coverage_ppm`, and add a positive non-null direct-row assertion plus missing/propagated omission assertions.

9. **HIGH — 136-15 / 136-16 / 136-17 / 136-18 — title acceptance still permits raw `neutral_title`.**  
   The plans ask for canonical/plain-text titles but never require `display_work_title()` ([136-15-PLAN.md:73](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-15-PLAN.md:73), [136-17-PLAN.md:190](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:190), [136-18-PLAN.md:83](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:83)). Work facets are also received as raw labels. The binding helper states every surface must use it ([discovery_display_strings.py:737](C:/Genizahsearch/shared/discovery_display_strings.py:737)), while current service rows deliberately carry raw titles ([discovery_service.py:1200](C:/Genizahsearch/shared/discovery_service.py:1200), [discovery_service.py:1310](C:/Genizahsearch/shared/discovery_service.py:1310), [discovery_service.py:1552](C:/Genizahsearch/shared/discovery_service.py:1552)). “Plain text, not a link” tests would pass with the wrong title.  
   **Change:** require `display_work_title(display_work_id, neutral_title, lang)` for panel rows, manuscript chips, findings rows, and work-facet labels; add the curated work as a bilingual fixture on every title-rendering path.

10. **MEDIUM — 136-16 — its off-loop acceptance can pass with a blocking results read.**  
    The plan says to use async envelopes at [136-16-PLAN.md:157](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-16-PLAN.md:157), then says to follow a separate `run.io_bound` fetch model at [136-16-PLAN.md:199](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-16-PLAN.md:199). The existing `test_no_await_sync_function` only detects `await` on a locally defined synchronous function ([test_no_await_sync_function.py:46](C:/Genizahsearch/tests/test_no_await_sync_function.py:46)); direct synchronous service calls, nested offloads, and wrapping an imported coroutine all evade it.  
    **Change:** state that page code directly awaits `web.discovery`’s async wrappers and adds no `run.io_bound`; add an AST/spy guard forbidding direct sync service calls and verifying one internal executor dispatch per request.

11. **MEDIUM — 136-17 / 136-18 — pre-deploy honesty checks stop at markup.**  
    Both plans deploy after rendered-output tests ([136-17-PLAN.md:278](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:278), [136-18-PLAN.md:182](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:182)); JSON and error-path coverage is deferred to 136-19. Current envelope validation checks forbidden key names, not arbitrary values under innocuous keys ([discovery_surface_projection.py:296](C:/Genizahsearch/shared/discovery_surface_projection.py:296)). Thus a percentage or badge string could leak in an envelope/error message, be discarded by the renderer, and deploy green.  
    **Change:** before each surface deploy, scan its exact envelopes and forced error paths for percentages, intervals, review badges, stored vocabulary, and masking terms. Keep 136-19 as the final cross-surface sweep.

12. **MEDIUM — 136-18 — two positive controls combine independent defects.**  
    One control seeds both “New discovery” and a precision figure; another seeds both an invalid domain and a wrong facet header at [136-18-PLAN.md:199](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:199). The honesty gate has independent detectors ([discovery_honesty_gate.py:276](C:/Genizahsearch/tests/render_smoke/discovery_honesty_gate.py:276)), so either combined control can turn red while the other property is completely untested.  
    **Change:** use one mutation per property and assert the expected failing assertion identifier/message, not merely that “the suite went red.”

13. **LOW — 136-15 — its population literal is private-asset-specific.**  
    The plan fixes 14/116 and 19/121 into acceptance prose at [136-15-PLAN.md:114](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-15-PLAN.md:114), matching the private rebuild and the code comment at [discovery_service.py:224](C:/Genizahsearch/shared/discovery_service.py:224). The deployed public artifact contains 12/84 display human-confirmed rows instead.  
    **Change:** label every population by artifact/audience and record both current public and private values; keep fixtures behavioral rather than count-derived.

14. **CONFIRM — 136-17 — the page-ID accessor now exists and meets the substantive requirements.**  
    The accessor is bounded at 500, derives IDs from page headers, filters by `volume_ie`, and reports resolution/truncation ([services.py:427](C:/Genizahsearch/web/services.py:427)). Its public wrapper dispatches off-loop and returns the four-key envelope ([web/discovery.py:122](C:/Genizahsearch/web/discovery.py:122)). `BrowsePage` carries `volume_ie` at [services.py:121](C:/Genizahsearch/web/services.py:121). Only the integration wording in finding 4 needs correction.

15. **CONFIRM — 136-19 — all three masking requirements remain correctly stated and executable.**  
    The plan requires a blocking missing-pattern outcome, both strict surfaces, and cell-by-cell SQLite scanning at [136-19-PLAN.md:98](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-19-PLAN.md:98) and [136-19-PLAN.md:104](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-19-PLAN.md:104). The live CLI implements exactly those contracts ([check_atlas_masking.py:1327](C:/Genizahsearch/scripts/check_atlas_masking.py:1327), [check_atlas_masking.py:1367](C:/Genizahsearch/scripts/check_atlas_masking.py:1367), [check_atlas_masking.py:1377](C:/Genizahsearch/scripts/check_atlas_masking.py:1377)). I ran strict repo + asset + SQLite scans over all three named sidecars; all returned clean.

16. **CONFIRM — 136-16 / 136-18 — rulings M, N, P, Q, and S are not contradicted.**  
    The findings plans retain candidacy framing and do not add a correctness suppression filter ([136-18-PLAN.md:95](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:95), [136-18-PLAN.md:106](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:106)). The live cascade reads the identified work’s `works.genre` through `display_work_id` ([discovery_service.py:415](C:/Genizahsearch/shared/discovery_service.py:415), [discovery_service.py:1516](C:/Genizahsearch/shared/discovery_service.py:1516)), and the findings filter contains no corpus/VIS-shortcut gate ([discovery_service.py:431](C:/Genizahsearch/shared/discovery_service.py:431)). The two-axis public projection therefore remains authoritative.

17. **CONFIRM — 136-15 / 136-16 — the base service row shapes otherwise match the rebuilt substrate.**  
    Claim rows carry the panel’s IDs, relation, band, coverage, matched letters, offsets, bucket, novelty, and human-confirmation markers through an exact allowlist ([discovery_surface_projection.py:113](C:/Genizahsearch/shared/discovery_surface_projection.py:113)). Findings return the three shipped units with unit-specific nullability and a four-key envelope; live public totals were 27,709 identifications, 23,312 manuscripts, and 478 works. The actionable mismatches are the envelope composition, curated-title routing, launch stats, and nonexistent matched-letter count identified above.

VERDICT: rework — BLOCKER 1, HIGH 8, MEDIUM 3, LOW 1, CONFIRM 4

VERDICT: rework — BLOCKER 1, HIGH 8, MEDIUM 3, LOW 1, CONFIRM 4

---

# Round 6 — the REVISED plans (2026-08-03)

**Run:** 2026-08-03 · `codex exec --dangerously-bypass-approvals-and-sandbox -C C:/Genizahsearch`
**Scope:** the six revised plans + the new `136-22`. Round-5 closure + regression hunt.
**Brief:** `_tmp/136-codex-preflight-r6-brief.md` (masking-scanned clean before the run).
**Log:** `_tmp/136-codex-preflight-r6.log` (gitignored).

> **VERDICT: rework** — 0 BLOCKER · 5 HIGH · 2 MEDIUM · 0 LOW · 8 CONFIRM.
> Trajectory: **1 BLOCKER / 8 HIGH → 0 BLOCKER / 5 HIGH.**

The round-5 BLOCKER is closed: ruling U has an owner in `136-22`, and Codex verified the arithmetic
itself against the public sidecar (4,152 + 3,873 + 1,498 = 9,523 on a single `main_pool = 1` basis).
Wave and dependency integrity was independently re-parsed across all 22 plans: no same-wave file
overlap, no plan ordered before its dependencies.

**Three of the five surviving HIGHs are on work the revision itself introduced** — two on the new
`136-22`, one an inconsistency between `136-15`'s five-envelope bundle and `136-17`'s four fetches.
That is the regression class this phase keeps producing, caught in the round that was briefed to
hunt it.

One finding is a FALSE-POSITIVE risk rather than a gap: the recursive envelope honesty check as
specified would reject valid live envelopes, because the projection intentionally carries machine
vocabulary. A gate that fails on correct output is as costly as one that passes on wrong output.

## Verbatim findings

1. **HIGH — 136-22 — cache invalidation is not acceptance-gated on the live failure mode.** The plan correctly requires a `(path, version)` cache key, but its test changes the version while changing artifacts ([136-22:203](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:203), [136-22:236](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:236)). Read-only SQL shows the public and private rebuilds have different contribution totals but the same `sidecar_version`. Worse, the recommended `_band_measurements` pattern checks the old `_last_path` cache key before `_get_conn()` refreshes it ([discovery_service.py:1160](C:/Genizahsearch/shared/discovery_service.py:1160)). A version-only or stale-path cache can therefore pass. Change the test to switch between different paths with the version held constant, and resolve the current provider path before cache lookup.

2. **HIGH — 136-22 — the no-literals guard does not fail closed without the current artifact and does not scan the claimed source scope.** The manifest currently selects the old sidecar ([manifest.json:2](C:/Genizahsearch/discovery_data/manifest.json:2)); read-only inspection confirms it lacks both new tables and an audience, so the public loader rejects it under its audience gate ([discovery_assets.py:395](C:/Genizahsearch/web/discovery_assets.py:395)). The plan permits a historical fallback when no artifact loads ([136-22:280](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:280)) and only asserts a derived list when one does ([136-22:299](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:299)). A new bake’s literal can therefore pass. It also acceptance-gates only named modules, not the full source/render closure ([136-22:267](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:267), [136-22:297](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:297)). Require an explicit verified sidecar for release tests, fail when unavailable, retain the historical list only as a supplement, and scan all user-facing `web/` and relevant `shared/` source plus translations.

3. **HIGH — 136-15/136-17 — the model requires five envelopes, but browse integration fetches four.** `PanelServiceBundle` requires claims, page IDs, manuscript works, related count, and related rows ([136-15:150](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-15-PLAN.md:150)). Plan 136-17 enumerates only four reads and explicitly says it constructs the bundle from four envelopes ([136-17:103](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:103), [136-17:162](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:162)). Its related-pages acceptance only checks the default hidden state, not that opening the toggle fetches and installs the fifth envelope ([136-17:343](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:343)). A panel whose related rows never load can pass. Add an explicit lazy fifth read with a rendered-toggle interaction test, or give the bundle a distinct, honest `not_requested` carrier rather than fabricating an `ok` zero.

4. **HIGH — 136-17/136-18 — the recursive envelope honesty check rejects valid live envelopes.** The projection intentionally carries machine vocabulary such as `relation_kind`, `evidence_source`, and `confidence_band` for renderer mapping ([discovery_surface_projection.py:130](C:/Genizahsearch/shared/discovery_surface_projection.py:130)). The honesty detector classifies underscore-bearing claim types and evidence sources as raw-vocabulary leaks ([discovery_honesty_gate.py:232](C:/Genizahsearch/tests/render_smoke/discovery_honesty_gate.py:232)). Yet both revised plans require applying all five detectors recursively to every envelope string value ([136-17:386](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:386), [136-18:314](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:314)). All 53,581 rows in the public sidecar have an underscore-bearing relation value. Make the scan schema-aware—raw-vocabulary detection applies to rendered/user-facing labels—or introduce a separate public JSON projection that removes machine enums.

5. **HIGH — 136-21/136-17 — an approximate expansion total is still authorized despite the real-total contract.** Plan 136-21 permits falling back to an explicitly flagged approximate count when the query exceeds budget ([136-21:306](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:306)), while its envelope contract names only anchor mode and filter basis in `meta` ([136-21:299](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:299)), and 136-17 requires the counted real total ([136-17:301](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:301)). An implementation exact on small fixtures but approximate for a high-cardinality work can pass. Remove the approximation escape; otherwise specify and test `approximate_total` end-to-end, including visible renderer wording.

6. **MEDIUM — 136-16 — “more matches” is not unambiguously browser-actionability-gated.** The criterion checks the child’s `display:none`, ancestry names, and that an interaction changes a service call ([136-16:262](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-16-PLAN.md:262)). A direct handler invocation can pass with an inert DOM binding, and a child can be `display:block` under a collapsed generic ancestor using visibility, height, clipping, or a framework expansion container. Require a real browser click on the rendered locator—with no preceding disclosure action—plus actual visibility/actionability and resulting DOM replacement.

7. **MEDIUM — 136-17 — the corrected dispatch criterion is still over-broad across service states.** It says zero dispatch with a rendered panel proves an event-loop sync call ([136-17:242](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:242)). A correct unavailable path performs zero dispatch because wrappers short-circuit before the service ([web/discovery.py:107](C:/Genizahsearch/web/discovery.py:107), [web/discovery.py:137](C:/Genizahsearch/web/discovery.py:137)), while the panel must still render an outage. Scope the `4/3/1` counts to an `ok`, resolved path and separately pin expected counts for unavailable, busy, timeout, and unresolved states.

8. **CONFIRM — 136-22/136-18 — ruling U now has an artifact-backed owner and an unambiguous main-pool basis.** Read-only SQL over the public sidecar produced `4,152 + 3,873 + 1,498 = 9,523`; its all-bucket total is separately `17,536`. The private rebuild produced a distinct `10,432`, confirming why provenance is load-bearing. The plan structurally sums shade rows, filters every shade on `main_pool = 1`, separates the all-bucket key, and requires basis/version/audience metadata ([136-22:165](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:165), [136-22:185](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:185)). 136-18 depends on it and renders its envelope ([136-18:6](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:6)). Change: none beyond findings 1–2.

9. **CONFIRM — 136-15/16/17/18 — title routing is acceptance-gated across every rendering path.** Panel rows and manuscript chips use bilingual curated fixtures ([136-15:209](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-15-PLAN.md:209), [136-15:299](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-15-PLAN.md:299)); work facets cover the live raw `MIN(w.neutral_title)` cascade ([136-16:267](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-16-PLAN.md:267), [discovery_service.py:1553](C:/Genizahsearch/shared/discovery_service.py:1553)); expansion rendering has its own bilingual fixture ([136-17:337](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:337)); findings rows do likewise ([136-18:214](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:214)). Each asserts the raw title is absent. Change: none.

10. **CONFIRM — 136-21 — expansion renderability, false-zero handling, and projection registration are substantive.** The plan requires a LEFT join to `manuscript_display`, non-null carrier assertions, explicit missing-display behavior, resolved displayed band fields, and no `band_precision` join ([136-21:159](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:159), [136-21:228](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:228)). It also requires separate page/member/count forced failures and confines `[]` to the legacy API ([136-21:269](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:269), [136-21:316](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:316)). `SURFACE_EXPANSION_FIELDS` membership in `_ALL_ALLOWLISTS` is explicitly asserted and mutation-tested ([136-21:324](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:324)). Change: none beyond finding 5.

11. **CONFIRM — 136-15/136-21 — the revised envelope and metadata shapes match live code.** The live envelope is always exactly `{status, items, total, meta}` ([discovery_surface_projection.py:318](C:/Genizahsearch/shared/discovery_surface_projection.py:318)). The five panel metadata sets tabulated in 136-15 agree with the live wrappers and synchronous implementations ([136-15:104](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-15-PLAN.md:104), [discovery_service.py:1247](C:/Genizahsearch/shared/discovery_service.py:1247), [web/discovery.py:149](C:/Genizahsearch/web/discovery.py:149)). No surviving normative three-key envelope literal was found. Change: fix only the four-versus-five integration in finding 3.

12. **CONFIRM — 136-17 — the deliberate composite-callable deviation is technically justified, but it crosses the executor multiple times.** The live cache key contains cache name, call arguments, and version ([discovery_service.py:1927](C:/Genizahsearch/shared/discovery_service.py:1927)). Because `page_id` changes while the manuscript page-ID tuple remains stable, a single whole-request cache would discard the useful manuscript-works hit. The plan’s corrected normal-path counts—four cold, three on another folio in the same manuscript, one on the same folio—match the live cache keys. Thus the reasoning holds. The contract nevertheless permits four executor-dispatch crossings on a cold panel load, not one composite dispatch ([136-17:112](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:112)). Change: none except state-scoping finding 7.

13. **CONFIRM — all plans — wave and dependency integrity is correct.** Independent parsing of all 22 plan frontmatters found no same-wave file overlap and no plan whose wave is less than `dependency wave + 1`. Plans 136-21/16/15 are wave 7, 136-17/22 wave 8, 136-18 wave 9, and 136-19 wave 10, matching the recorded roadmap ([ROADMAP.md:196](C:/Genizahsearch/.planning/ROADMAP.md:196), [ROADMAP.md:225](C:/Genizahsearch/.planning/ROADMAP.md:225)). Change: none.

14. **CONFIRM — 136-18 — the false matched-letter criterion and combined controls are genuinely repaired.** The live finding projection exposes `max_coverage_ppm`, not matched letters ([discovery_surface_projection.py:210](C:/Genizahsearch/shared/discovery_surface_projection.py:210)). Acceptance now requires a positive non-null direct-row rendering plus missing and propagated omissions and a grep against the retired field ([136-18:213](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:213)). The seven controls each seed one defect and require the named detector, including separate envelope and error-path controls ([136-18:321](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:321)). Change: none beyond the envelope-scan incompatibility in finding 4.

15. **CONFIRM — 136-19 — masking readiness remains fail-closed and executable.** The plan requires strict repository, byte-level asset, and read-only cell-by-cell SQLite scans, and an unavailable pattern file blocks readiness ([136-19:119](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-19-PLAN.md:119), [136-19:125](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-19-PLAN.md:125)). The live scanner rejects empty pattern sets and performs both asset and SQLite modes ([check_atlas_masking.py:206](C:/Genizahsearch/scripts/check_atlas_masking.py:206), [check_atlas_masking.py:1367](C:/Genizahsearch/scripts/check_atlas_masking.py:1367), [check_atlas_masking.py:1384](C:/Genizahsearch/scripts/check_atlas_masking.py:1384)). Its four positive controls are separated and require specific failures. Change: none.

VERDICT: rework — BLOCKER 0, HIGH 5, MEDIUM 2, LOW 0, CONFIRM 8

---

# Round 7 — the twice-revised plans (2026-08-04)

**Brief:** `_tmp/136-codex-preflight-r7-brief.md` (masking-scanned clean before the run).
**Log:** `_tmp/136-codex-preflight-r7.log` (gitignored).

> **VERDICT: rework** — 0 BLOCKER · 5 HIGH · 2 MEDIUM · 0 LOW · 8 CONFIRM.
> Trajectory: **1 BLOCKER / 8 HIGH → 0 / 5 HIGH → 0 / 5 HIGH.**

**The tally is flat, and that is worth reading correctly.** These are not the round-6 findings
restated. Round 6 closed all seven; rounds 6 and 7 both confirm that. What round 7 does is drill a
finer layer under two of them and expose two consequences of the round-6 fixes:

* Findings 2 and 5 are the same *areas* as round 6, now with concrete counterexamples the earlier
  wording could not have caught: `ui.label(str(9523))` is a numeric AST constant that passes a
  string-literal scan, and `min(exact_total, 1000)` contains no forbidden word and no SQL `LIMIT`,
  so it passes a "high-cardinality" fixture chosen at four pages — while a real work in the public
  artifact carries **4,796** identification rows.
* Findings 3 and 4 are consequences of round 6's own fix. Scoping the raw-vocabulary detector to
  reader-facing fields (correct, and confirmed) left its vocabulary incomplete, and revealed that
  **no detector catches a plain-language accuracy rate at all**: `accuracy 0.91` produces no
  violation through any field, exempt or not, against a standing rule that prohibits exactly that.

All three deliberate deviations were examined and upheld as technically justified rather than
rationalisation (CONFIRM 12/13/14).

**On `_band_measurements`** (CONFIRM 15): the ordering defect is real and WORSE than reported — a
direct call can stay stale indefinitely, not for one call, because a cache hit returns before
`_get_conn()`. But its only live caller runs `is_available()` and the page query first, both of which
refresh the connection, and production's manifest-change-plus-restart flow recreates the service
outright. Real-world impact is effectively none. **No Phase 136 change required**; a later
maintenance fix should resolve the connection before computing the key.

## Verbatim findings

1. **HIGH — 136-22 — the public async cache defeats the new path-aware cache.**  
   Task 1 requires `(current path, version)` invalidation, but Task 2 still directs the wrapper through `_enveloped_off_loop(..., cache_name=...)` ([136-22-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:300)). The live outer LRU key is only `(cache_name, args, version)` and returns before executing the path-aware synchronous reader ([discovery_service.py](C:/Genizahsearch/shared/discovery_service.py:1921)). It also uses `_browse_timeout()` when caching, ignoring the requested findings timeout ([discovery_service.py](C:/Genizahsearch/shared/discovery_service.py:1934)). Thus the sync path-switch test can pass while `web.discovery.get_launch_stats_enveloped()` serves the first artifact after a constant-version path switch.  
   **Change:** either omit the outer `cache_name` and rely on the inner path-aware cache, or make `_browse_cached_call` resolve and include the current path and accept the caller’s timeout. Run the first-post-switch mutation test through the public async wrapper.

2. **HIGH — 136-22 — the no-literals guard remains passable.**  
   The guard is limited to Python string and f-string literals ([136-22-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:322)); a hardcoded integer such as `ui.label(str(9523))` is a numeric AST constant and can pass. Completeness is also not acceptance-gated: the tests require only that both committed halves are non-empty ([136-22-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:373)), although the envelope carries per-shade identification and distinct-manuscript counts plus numeric metadata ([136-22-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:196)). A fixture containing only one current value can satisfy the stated freshness test if the recomputation is equally incomplete.  
   **Change:** scan integer AST constants and formatted expressions, assert an exact required key set covering every numeric `items`, `total`, and `meta` value the launch envelope exposes, and parameterize controls over every figure and literal form.

3. **HIGH — 136-17/136-18 — the “strict” raw-vocabulary scan uses an incomplete vocabulary.**  
   The live prohibited set includes claim, band, adjudication, routing, evidence, and measurement enums only ([discovery_honesty_gate.py](C:/Genizahsearch/tests/render_smoke/discovery_honesty_gate.py:232)). It omits `NOVELTY_STATUSES` ([discovery_novelty.py](C:/Genizahsearch/shared/discovery_novelty.py:166)) and `MAIN_POOL_REASONS` ([discovery_main_pool.py](C:/Genizahsearch/shared/discovery_main_pool.py:81)). Read-only SQL found underscore-bearing `main_pool_reason` values on all 53,581 public rows and underscore-bearing novelty values on 33,862. Consequently, `fills_gap` or `main_full_coverage` seeded into `band_label` passes, although `direct_witness` fails. Plan 136-18 explicitly expects live `novelty_status` and `main_pool_reason` values to scan clean ([136-18-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:329)).  
   **Change:** include every closed stored vocabulary in the prohibited-value set, classify their legitimate carrier fields explicitly as machine fields, and add one reader-facing-field mutation control per vocabulary.

4. **HIGH — 136-17/136-18 — accuracy rates are not detected.**  
   The standing rule prohibits accuracy rates, but these plans enumerate percentage, bracketed-interval, badge, phrase, and raw-vocabulary detectors only ([136-17-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:469)). Against the live detector functions, `accuracy 0.91` produces no violation, while a percent-formatted rate, bracketed interval, and exact badge do fail. Therefore an accuracy rate can pass through any field, exempt or not.  
   **Change:** add an accuracy/rate/ratio detector with markup, envelope, and error-path positive controls. Keep qualified matched-letter coverage as the explicit exception.

5. **HIGH — 136-21 — a quiet large-input cap can still pass.**  
   The high-cardinality fixture is specified only as “more units than several pages” ([136-21-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:344)). An implementation using `min(exact_total, 1000)` contains none of the forbidden words and no SQL `LIMIT`-bounded count; it passes if the executor chooses a four-page fixture. Read-only SQL over the public artifact found a work with 4,796 identification rows, so that example can be wrong in live data while satisfying the criterion.  
   **Change:** assert independently computed exact totals for the real artifact’s highest-cardinality case, pin a fixture cardinality above a named threshold, and add an observed mutation control that inserts a Python-side cap.

6. **MEDIUM — 136-17 — the timeout dispatch row is unreachable as written.**  
   Page-ID resolution precedes the manuscript-works read ([136-17-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:135)), and unresolved scope must suppress that read ([136-17-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:294)). Therefore “timeout on all four reads, cold = 4” ([136-17-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:296)) is impossible when the page-ID accessor times out: only the three independent reads cross. Four crossings apply when page IDs resolve and a downstream read times out.  
   **Change:** split timeout and injected-busy rows by failure location: page-ID failure versus claims/count/work failure.

7. **MEDIUM — 136-16 — its own dispatch assertion is still unscoped.**  
   The plan says exactly one internal executor dispatch “per findings request” ([136-16-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-16-PLAN.md:292)). The live wrapper correctly short-circuits with zero dispatches when discovery is unavailable ([web/discovery.py](C:/Genizahsearch/web/discovery.py:229)), and the route itself has an unavailable early return.  
   **Change:** scope “one” to an available, cold, successful request and explicitly require zero on the unavailable route.

8. **CONFIRM — 136-15/136-17 — the fifth-envelope correction is substantive.**  
   `None`, populated, successful-empty, and outage are required to emit four pairwise-distinct states ([136-15-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-15-PLAN.md:330)); the integration also asserts no pre-toggle read and a populated post-toggle render ([136-17-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:408)). Fabricating an eager `ok/0` cannot satisfy both model and integration criteria. No change.

9. **CONFIRM — 136-16/136-18 — ruling T actionability is now fail-closed.**  
   The simulated-user test requires DOM replacement, the real-browser check covers both widths and languages, and the collapsed-ancestor control must be observed failing ([136-16-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-16-PLAN.md:281)). Unavailable browser tooling is explicitly NOT MET ([136-16-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-16-PLAN.md:285)), and 136-18 blocks deployment on that result ([136-18-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:390)). An inert-but-present control fails. No change.

10. **CONFIRM — all plans — wave and dependency integrity holds.**  
    Independent parsing found 22 plans, 10 waves, no dependency placed in the same or a later wave, and no same-wave `files_modified` collision. The added honesty-gate file in 136-17 does not overlap 136-22’s wave-8 files. No change.

11. **CONFIRM — Round 6’s other confirmations still hold on spot-check.**  
    Read-only SQL reproduced the public `4,152 + 3,873 + 1,498 = 9,523` main-pool decomposition and the private total of 10,432; all three artifacts report `discovery-v1-real`. Title routing, expansion projection/failure handling, four-key envelopes, matched-coverage replacement, and masking fail-closed text remain acceptance-gated. The masking secret is currently unset, so an execution now would correctly be NOT MET rather than skipped ([136-19-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-19-PLAN.md:119)). No change beyond findings above.

12. **CONFIRM — deliberate deviation 1 is technically justified, not rationalization.**  
    The live projections deliberately carry relation, band, routing, and measurement enums used before or during label/chip mapping ([discovery_surface_projection.py](C:/Genizahsearch/shared/discovery_surface_projection.py:113)). Stripping them from the model input would move or duplicate interpretation at the surface boundary. Schema-aware scanning is the right approach; its vocabulary completeness must still be fixed per findings 3–4.

13. **CONFIRM — deliberate deviation 2 holds.**  
    The two measured current totals occur in 136-22’s historical/objective explanation, not in 136-16 or 136-18 acceptance text. Live sidecars confirm those totals are artifact-specific. Keeping them in the plan that owns the historical forbidden list is reasonable.

14. **CONFIRM — deliberate deviation 3 holds in principle, not rationalization.**  
    The repository manifest selects the pre-rebuild artifact, which lacks the two required tables and an audience, so the public loader correctly rejects it ([manifest.json](C:/Genizahsearch/discovery_data/manifest.json:2), [discovery_assets.py](C:/Genizahsearch/web/discovery_assets.py:395)). A committed structural guard plus an explicitly run release-time freshness check is preferable to a CI test that can only be excluded. The structural guard itself still needs finding 2’s fixes.

15. **CONFIRM — `_band_measurements` has the reported ordering defect, but not the claimed production impact.**  
    It builds its key from `_last_path` before calling `_get_conn()` ([discovery_service.py](C:/Genizahsearch/shared/discovery_service.py:1154)). More precisely, a direct call can remain stale indefinitely—not merely for one call—because a cache hit returns before `_get_conn()`. Its only live caller, however, first runs `is_available()` and the page query, both of which refresh the connection/path before `_band_measurements` ([discovery_service.py](C:/Genizahsearch/shared/discovery_service.py:1236)). Production’s manifest-change-plus-restart flow also recreates the service and clears the cache. Real-world production impact is therefore effectively none under the stated deployment procedure. No Phase 136 change is required; a later maintenance fix should resolve the connection before computing this key.

VERDICT: rework — BLOCKER 0 · HIGH 5 · MEDIUM 2 · LOW 0 · CONFIRM 8

---

# Round 8 — the thrice-revised plans (2026-08-04)

**Brief:** `_tmp/136-codex-preflight-r8-brief.md` (masking-scanned clean before the run).
**Log:** `_tmp/136-codex-preflight-r8.log` (gitignored).

> **VERDICT: rework** — 0 BLOCKER · 4 HIGH · 1 MEDIUM · 0 LOW · 5 CONFIRM.
> Trajectory: **1 BLOCKER / 8 HIGH → 0/5 → 0/5 → 0/4.**

Round 7's H1, M6 and M7 are closed and confirmed. Wave/dependency integrity re-parsed independently
across all 22 plans (10 waves, every dependency earlier, no same-wave file collision), and the launch
arithmetic reproduced from the sidecar a third time.

The four survivors are all *incompleteness* of a correct fix, not a wrong fix:
`f"{9_000 + 523:,}"` is a computed constant neither operand of which equals a launch figure; the
field→vocabulary mapping enumerates twelve carriers where the live projection already has more
(92,546 evidence rows carry an underscore-bearing `routing_reason`); and 136-21's real-cardinality
control uses the wrong GRAIN — the live expansion grain is the distinct `unit_key` from the CTE
(max **5,684**), not identification rows per canonical work (max **4,796**), so `min(total, 5000)`
would not fail the real case as specified.

## Owner-decision note on finding 3 — resolved AGAINST the finding

Codex reports that the accuracy detector "misses an already-shipped plain-language accuracy rate" on
the methods page (`web/pages/help.py:178`). The wording is:

> "a small minority of the main pool, a low single-digit share, is misattributed for this reason"

That is the **deliberate D-06a qualitative rewrite** delivered by 136-02, and its own render test
pins the wording. It carries no number, no percentage and no interval. It is therefore **compliant,
and the detector must NOT fire on it** — widening the detector to catch it would fail a page the
owner approved and a test that requires it.

The correct action is not to widen the detector but to make the boundary EXPLICIT: qualitative
error-rate language sanctioned by D-06a is a named exception, recorded alongside qualified
matched-letter coverage, precisely so a future widening cannot break the methods page. Finding 3's
observation is valuable; its prescription is not.

## Verbatim findings

The Round 8 pre-flight does not converge. Four Round 7 areas remain incomplete.

1. HIGH — 136-22’s no-literals guard remains bypassable.

   The scanner handles direct numeric constants and formatting operands, but not computed constants. For example, `ui.label(f"{9_000 + 523:,}")` renders the current headline while neither operand equals a forbidden figure. It would pass the guard and the fixture-based headline equality test in [136-18-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:236), then become stale after a rebuild.

   The exemption boundary is also too narrow: [136-22-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:398) says exemptions may exist only in non-rendering code, but the acceptance test merely excludes `web/pages/` and `web/components/`. It would accept an exemption in `web/discovery.py` or `shared/discovery_display_strings.py`, both capable of carrying a figure to a page.

   The exact-key-set completeness check itself is sound for every numeric value the envelope exposes, and the direct figure × form controls are substantive. They do not close these two paths.

2. HIGH — 136-17’s field→vocabulary restructuring is not structurally complete.

   The plan enumerates twelve machine carriers in [136-17-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:555), but the live projection already contains additional machine-valued fields: `coverage_status`, `eligibility_basis`, and `routing_reason` in [discovery_surface_projection.py](C:/Genizahsearch/shared/discovery_surface_projection.py:135). `routing_reason` has a closed vocabulary in [discovery_ids.py](C:/Genizahsearch/scripts/discovery_ids.py:117).

   Read-only aggregation over the public sidecar found:

   - 92,546 evidence rows with underscore-bearing `routing_reason` values.
   - 29,854 rows with `coverage_status='not_applicable'`.
   - 11 identifications with `eligibility_basis='human_confirmed'`.

   The collection assertions in [136-17-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:665) validate fields already placed in the mapping, but never assert the inverse—that every projected machine carrier is mapped. A mapping that omits these live fields still satisfies the structural criterion. Correct envelopes can consequently false-positive, while omitted vocabularies remain structurally untracked.

   The withdrawal in 136-18 is correct for `novelty_status` and `main_pool_reason`, but it is not fully propagated to the complete live carrier set.

3. HIGH — the accuracy detector misses an already-shipped plain-language accuracy rate.

   The methods page states that “a low single-digit share” is “misattributed” in [help.py](C:/Genizahsearch/web/pages/help.py:178), and its render test explicitly requires that wording in [test_help_methods_render_smoke.py](C:/Genizahsearch/tests/render_smoke/test_help_methods_render_smoke.py:369). That is an error/accuracy rate under the standing rule.

   The proposed lexicon and rate-shaped quantities in [136-17-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:508) contain neither “share” nor “misattributed,” so the widened full render suite remains green over this live rate.

   The exact `accuracy 0.91` cases are covered correctly in markup, envelope strings, and envelope floats. Qualified matched-letter coverage is also preserved.

4. HIGH — 136-21’s real-cardinality mutation expectation uses the wrong grain.

   The live expansion grain is the distinct `unit_key` produced by the CTE in [discovery_service.py](C:/Genizahsearch/shared/discovery_service.py:610), not identification rows per canonical work.

   Read-only queries over the public sidecar found:

   - Maximum `discovery_identification` rows per canonical work: 4,796.
   - Maximum expansion witness units for a work, using the live CTE’s claim-type filter and `COALESCE(unit_id, 'sys:' || sys_id)` grain: 5,684.

   Therefore `min(total, 5000)` must fail on both the synthetic fixture and the true highest-cardinality real expansion. But [136-21-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:375) requires it to fail only synthetically because it assumes 5,000 exceeds every real work. If the probe selects the 4,796 identification case to make that expectation pass, it is not probing the expansion’s highest-cardinality work.

5. MEDIUM — the accuracy detector’s negative/control contract is incomplete.

   The proximity rule treats any decimal fraction in `[0,1]` as rate-shaped, while the following bare-decimal rule claims its two-place minimum avoids `V0.8`. Thus copy such as “precision handling changed in V0.8” can false-positive despite the stated intent. Neither `V0.8` nor `1.25 seconds` has an explicit control in [136-17-PLAN.md](C:/Genizahsearch/.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:667).

   Also, the ten named mutations contain a percentage error-path control but no accuracy-rate error-path control. A wiring defect applying the sixth detector to markup and envelopes but not error messages can satisfy the listed mutations.

Confirmed:

- H1 is closed: no outer `cache_name`, explicit findings timeout, public-wrapper path switch, and the cache-isolation criterion mutates `items`, rows, and `meta`.
- M6’s 3/4/2/3 dispatch counts follow the live pre-dispatch busy gate, post-dispatch timeout, and suppressed manuscript-work read.
- M7 correctly defines one dispatch per available, cold enveloped read—not per page load.
- Independent parsing found 22 plans, 10 waves, every dependency in an earlier wave, and no same-wave file collision.
- No `wave`, `depends_on`, `files_modified`, `requirements`, or `autonomous` header changed since the Round 7 baseline.
- Read-only sidecar queries reproduced the public `4,152 + 3,873 + 1,498 = 9,523`, private `10,432`, and common `discovery-v1-real` version.
- The working tree remains clean; no file or database was written.

VERDICT: rework — BLOCKER 0 · HIGH 4 · MEDIUM 1 · LOW 0 · CONFIRM 5


---

# Round 9 — the four-times-revised plans (2026-08-04)

**Brief:** `_tmp/136-codex-preflight-r9-brief.md`. **Baseline:** § Round 8.
**Scope:** verify round 8's five findings are genuinely closed; hunt regressions.

**Trajectory: 1 BLOCKER / 8 HIGH → 5 → 5 → 4 → 5 HIGH.** The HIGH count did NOT fall this round.
That is not oscillation: findings 1, 2 and 5 are *deeper* than anything earlier rounds reached
(finding 1 is a semantic dependency defect that frontmatter parsing structurally cannot see, and
finding 13 says so explicitly), while findings 3, 4 and 8 are defects **this revision introduced**.

**Self-inflicted rate is the signal worth acting on.** Revision 4 introduced at least three of the
eight findings below, on top of the five its own self-review caught before Codex ran. The revisions
are being made too broadly and too fast. Round 10's instruction is to fix finding 1 structurally and
be surgical everywhere else.

## What round 9 confirmed closed

- **Round 8 finding 1 (constant folding + exemption boundary)** — CONFIRM 9. The folded value is
  compared, unsafe exponentiation excluded, every figure gets independent-operand controls, and the
  stated scanner limit does not swallow 136-18's sentinel data-path test.
- **Round 8 finding 2 (derived carriers)** — CONFIRM 10. The three round-8 carriers match the shape
  rule and **no live `page_id`, work ID, manuscript ID, claim/evidence digest or unit digest does** —
  the ID-collision risk flagged in the brief was checked against live values and is absent.
- **Round 8 finding 3 (D-06a)** — CONFIRM 11. The control genuinely renders the live page and takes
  its text from the render. **The adjudication against Codex was correct; the follow-through was
  not** — see finding 3 below.
- **Round 8 finding 4 (cardinality grain)** — CONFIRM 12. 5,684 reproduced at the expansion grain.
  `136-RESEARCH.md:120`'s third figure (4,637) is a genuinely different table, population and
  surface contract, not a contradiction.
- **Structural graph integrity** — CONFIRM 13. 22 plans, ten waves, every declared dependency
  earlier, no same-wave `files_modified` overlap, no header drift.

## Note on finding 1 — the deviation the author defended in the round-9 brief

Deviation 1 (declaring `coverage_status` / `eligibility_basis` locally rather than exporting to
`shared/`) was defended on the grounds that wave 8 runs 136-17 in parallel with 136-22 over
`shared/`, so an out-of-list edit is an invisible collision. Codex's ruling: **"Deviation 1's
collision concern is real, but the present parallelization is not sound."** The collision reasoning
holds; the wave assignment it was protecting does not. `items[*].shade` — added by 136-22 — is
discovered by 136-17's inverse assertion but cannot be classified by it, so a **correct** launch
envelope fails the gate.

Preferred remedy: **136-17 depends on 136-22** (136-17 → wave 9, 136-18 → wave 10, 136-19 → wave 11).
The alternative — letting 136-18 own the gate-file edit — leaves the suite RED between wave 8 and
wave 10, which is the failure mode these plans already refuse elsewhere.

## Not affected

Wave 7's `136-15` and `136-16` draw no finding in rounds 8 or 9 and were released to execution
before this round reported. Findings 1–8 are all against `136-17`, `136-18`, `136-21`, `136-22`.

## Findings

1. **HIGH — 136-17/136-18/136-22 — a correct launch envelope fails the derived carrier gate.** Plan 136-22 adds `items[*].shade` with three snake-case vocabulary values ([136-22-PLAN.md:247](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:247), [136-22-PLAN.md:304](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:304)). The inverse assertion therefore discovers `shade`, but 136-17 cannot classify it while running parallel with 136-22, and 136-18 is expressly told to import the mapping rather than edit it ([136-17-PLAN.md:735](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:735), [136-18-PLAN.md:387](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:387)). Thus the strict raw-vocabulary scan rejects correct launch rows. This is semantic `depends_on` drift despite valid frontmatter. **Change:** make 136-17 depend on 136-22 and move the downstream waves, or let a later plan add `shade → contribution-vocabulary` to the central mapping and own that gate-file edit. Deviation 1’s collision concern is real, but the present parallelization is not sound.

2. **HIGH — 136-17 — the derived completeness split is not airtight.** The shape derivation excludes `meta` while the “strict” fallback only rejects values already present in the known prohibited union ([136-17-PLAN.md:551](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:551), [136-17-PLAN.md:558](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:558)). A later `meta` value such as an unknown snake-case stored enum passes. The shape rule also cannot discover a later carrier whose vocabulary consists solely of single-word tokens. A registry alone would indeed miss the two locally sourced vocabularies, but rejecting registry-backed discovery altogether is rationalization: shape and authority/registry checks are complementary. **Change:** add a separate bounded `meta` carrier/value mapping and combine shape discovery with available exported registries and pinned local authorities.

3. **HIGH — 136-17 — the D-06a exception is global, not surface-bound.** The plan globally excludes `share`, `minority`, `misattributed`, and `single-digit` from detection and defines the exception as any words-only qualitative frequency statement ([136-17-PLAN.md:633](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:633), [136-17-PLAN.md:664](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:664)). Consequently equivalent wording on a findings row, error path, or unrelated page can claim the exception. The live-page control protects the sanctioned sentence from false-positive changes but does not constrain where the exception applies. **Change:** bind the exception to the exact live help limitations element/context; detect the vocabulary elsewhere.

4. **HIGH — 136-22 — the claimed glob-only placement control is actually in the floor.** `web/pages/browse_enrichment.py` is explicitly part of the named floor ([136-22-PLAN.md:390](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:390)), but criterion (ii) calls it absent from the floor and uses it to prove the glob is load-bearing ([136-22-PLAN.md:527](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:527)). A hand-written floor plus one arbitrary extra module can satisfy every stated scope assertion. **Change:** select the control dynamically from `derived_set - floor`, assert that difference, and seed the figure in that actual module.

5. **HIGH — 136-21 — top N raw candidates do not prove the highest post-filter expansion.** The criterion acknowledges that filtering and anchor rules can reorder candidates, but samples only N ≥ 5 work IDs selected before those rules ([136-21-PLAN.md:398](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-21-PLAN.md:398)). A work outside the raw top five can retain more rows after filtering than every sampled work; a cap between the observed and missed totals can pass. This becomes especially reachable after a future bake where the cap also exceeds the synthetic floor. **Change:** either test the unanchored, unfiltered call—where raw top one is provably the global maximum—or run the real filtered call for every candidate / derive candidates through the identical filtered pipeline.

6. **MEDIUM — 136-17 — the version exclusion is wider than version syntax.** It excludes a fraction when its integer part is preceded by any word character ([136-17-PLAN.md:638](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:638)). Therefore copy such as `accuracy score0.9` is excluded from both rules, despite being a real rate. The three named controls do not cover this glued-token case ([136-17-PLAN.md:844](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:844)). **Change:** restrict exclusion to explicit `V`/`version` syntax and add a non-version alphanumeric-prefix rejection control.

7. **MEDIUM — 136-22 — two assertions can fail on correct data/code.** The plan offers exemptions for coincidental legitimate numeric uses ([136-22-PLAN.md:441](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:441)) but then forbids exempting any current figure ([136-22-PLAN.md:453](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:453)). A future launch count equal to a legitimate page-size or timeout constant blocks correct code with no valid exemption. Separately, requiring each forbidden-list half to contain a value absent from the other fails if a correct rebuild reproduces historical counts ([136-22-PLAN.md:528](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:528)). **Change:** prove both halves are independently loaded and provenance-pinned without requiring value-set uniqueness; permit a current-value exemption only with a derived non-egress proof and end-to-end control.

8. **LOW — 136-17 — the false-positive-control count is stale again.** The action says five controls but contains six bullets, including both the version boundary and live methods-page render ([136-17-PLAN.md:776](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:776), [136-17-PLAN.md:804](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:804)). Acceptance and verification count five by treating the version triplet separately ([136-17-PLAN.md:845](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:845)). **Change:** explicitly move the version triplet outside that list or change all counts to six.

9. **CONFIRM — 136-22/136-18 — constant folding and the sentinel pairing are substantive.** The folded value is compared, unsafe exponentiation is excluded, arithmetic failures are skipped, every figure gets independent-operand controls, and the renderer mutation assembled across names must fail only the sentinel test ([136-22-PLAN.md:411](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:411), [136-22-PLAN.md:524](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-22-PLAN.md:524), [136-18-PLAN.md:257](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-18-PLAN.md:257)). The stated scanner limit does not swallow the renderer data-path test. Change only finding 4’s scope proof and finding 7’s false-positive assertions.

10. **CONFIRM — 136-17 — the three Round-8 carriers match the shape rule, and live IDs do not.** The live fields are present at [discovery_surface_projection.py:138](shared/discovery_surface_projection.py:138), [discovery_surface_projection.py:153](shared/discovery_surface_projection.py:153), and [discovery_surface_projection.py:159](shared/discovery_surface_projection.py:159); their multiword stored values match the regex. Read-only inspection found no live `page_id`, work ID, manuscript ID, claim/evidence digest, or unit digest matching it. The plan correctly requires recording flagged fields before classification ([136-17-PLAN.md:562](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:562)). Change only findings 1–2.

11. **CONFIRM — 136-17 — the D-06a control really renders the live page.** It reuses the existing help-page rendering harness, scopes the limitations section, and takes text from the render rather than a copied string ([136-17-PLAN.md:804](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-17-PLAN.md:804)). The live page and its existing pin remain untouched ([help.py:178](web/pages/help.py:178), [test_help_methods_render_smoke.py:369](tests/render_smoke/test_help_methods_render_smoke.py:369)). Change the exception’s scope per finding 3, not the control’s provenance.

12. **CONFIRM — 136-21 — the cardinality grain is corrected, and the third figure is a different definition.** The live CTE counts distinct `COALESCE(unit_id, 'sys:' || sys_id)` for qualifying claim types ([discovery_service.py:610](shared/discovery_service.py:610), [discovery_service.py:626](shared/discovery_service.py:626)); read-only SQL reproduced 5,684 at that expansion grain. The 4,637 figure in [136-RESEARCH.md:120](.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-RESEARCH.md:120) counts distinct witness units only among the 4,796 `discovery_identification` manuscripts for the canonical work. It is a different table, population, and surface contract. Change only finding 5’s candidate-selection proof.

13. **CONFIRM — all plans — structural graph integrity and Round-7 spot-checks hold.** Independent parsing found 22 plans, ten waves, every declared dependency earlier, no same-wave `files_modified` overlap, and no structural header changes since the Round-8 baseline. The cache-wrapper/path-switch and dispatch-count criteria remain intact. The failure in finding 1 is semantic dependency drift that frontmatter-only parsing cannot see.

No file or database was written by this review. During the audit, HEAD advanced by two unrelated documentation commits and user-owned changes appeared in `web/main.py` and `web/pages/findings.py`; they were not touched.

VERDICT: rework — BLOCKER 0 · HIGH 5 · MEDIUM 2 · LOW 1 · CONFIRM 5
