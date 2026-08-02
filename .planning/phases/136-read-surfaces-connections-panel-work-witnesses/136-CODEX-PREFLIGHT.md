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
