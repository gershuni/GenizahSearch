# Discovery Acceptance Budgets v1 (PERF-01)

**Status:** ACTIVE. Version 1, created 2026-07-22 (Phase 134, plan 134-02). Amended 2026-08-02
(Phase 136, plan 136-01) — added §5, the corpus-wide findings-page ("Computed Identifications") cap
table plus two build-time PENDING slots; no pre-existing cap in §1-§4 is changed by this amendment.

**Tunable ONLY by versioning this artifact.** These numeric caps are a phase
exit criterion (PERF-01) and a hard contract for every downstream Discovery
plan (134-06 `DiscoveryService`, 135-139 read surfaces). A cap in this
document is never silently loosened or tightened in code — any change to a
number below requires a NEW version of this file (`discovery-budgets-v2.md`)
recording what changed and why, exactly like `docs/specs/atlas-asset-schema-v1.md`'s
versioning discipline for the atlas binary contract. Code that reads a budget
value should treat this file (or a values file generated from it) as the
single source of truth — never hardcode a competing number elsewhere.

**Scope:** This artifact covers the v9.0.0 Discovery module only (`DiscoveryService`,
`discovery.db`, the browse-enrichment "Computed identifications" panel, the
`/work/{id}` and `/leads` pages, and the atlas drill-down explorer). It does
not alter existing non-Discovery budgets (e.g. the pre-existing
`SEARCH_API_*` timeouts documented in `CLAUDE.md`).

**Measurement posture:** every cap below is MEASURED BEFORE RELEASE (PERF-01)
against a production-scale `discovery.db` and real traffic patterns. Until
that measurement lands (134-08, later in this phase), this document ships
with INITIAL caps only — see the "Measured Actuals" section at the bottom.

## 1. Initial Numeric Caps (PERF-01, copied verbatim from REQUIREMENTS.md)

### 1.1 Browse-enrichment ("Computed identifications" panel, PANEL-01/02)

| Metric | Cap |
|---|---|
| Added latency (server-side, on top of the existing Browse fetch) | p95 ≤ **150 ms** |
| Per-query timeout (DiscoveryService bounded execution) | ≤ **2 s** |

### 1.2 Work / Leads pages (`/work/{id}`, `/leads`)

| Metric | Cap |
|---|---|
| Rows per page | ≤ **200 rows/page** |
| Response size | ≤ **500 KB** |
| Server response time | p95 ≤ **1.5 s** |
| Per-request timeout | ≤ **5 s** |

### 1.3 Atlas drill-down (server-bounded neighborhood expansion, ATLAS-02)

| Metric | Cap |
|---|---|
| Nodes per response | ≤ **1,500 nodes** |
| Edges per response | ≤ **6,000 edges** |
| Response size | ≤ **2 MB** |
| Server response time | p95 ≤ **3 s** |
| Per-request timeout | ≤ **10 s** |

### 1.4 Memory (whole discovery module, prod box)

| Metric | Cap |
|---|---|
| Additional RSS attributable to Discovery (sidecar + service + caches) | ≤ **250 MB** |

## 2. DATA-06 Discretion Defaults (Claude's Discretion, ratified here for 134-06)

DATA-06 requires "one async `DiscoveryService` chokepoint with per-query
timeouts, bounded concurrency with defined user-facing overload behavior,
indexed bounded queries, LRU caching on the browse-enrichment path, and
server-side pagination everywhere lists can grow." The concrete numbers below
are the planner's discretion call (134-CONTEXT.md "Claude's Discretion"),
chosen to sit safely inside the §1 caps and to follow the existing
`web/search_api.py` / NLI-resilience precedent (`run_in_executor` +
`asyncio.wait`, never `wait_for`, since executor threads are not cancellable).
134-06 implements `DiscoveryService` against these defaults; a future
version of this artifact may retune them from measured production data.

| Setting | Default | Rationale |
|---|---|---|
| Browse-enrichment per-query timeout | **2.0 s** | Equals the §1.1 hard timeout cap exactly (no headroom needed — this is the fast, indexed, single-page lookup path; PERF-01's 150 ms p95 latency budget is the thing actually watched in practice) |
| Work/Leads-page per-query timeout | **5.0 s** | Equals the §1.2 hard timeout cap |
| Atlas drill-down per-query timeout | **10.0 s** | Equals the §1.3 hard timeout cap |
| Bounded concurrency — **HEAVY** budget (corpus-wide reads: work witnesses, findings, facets, launch statistics, the per-work expansion) | **4** | Mirrors the existing `SEARCH_API_HEAVY_CONCURRENCY=2` overload-cap pattern, widened slightly because Discovery queries are lighter (single indexed SQLite lookups against a ≤300 MB read-only sidecar, not full-corpus fuzzy/variants search); a 5th+ concurrent caller gets the overload response below rather than queueing indefinitely |
| Bounded concurrency — **BROWSE** budget (the per-page connections panel, and every other read) | **24** | ⟨AMENDED 2026-08-04, Phase 136⟩ The cap of 4 was documented as covering every query and applied only to the heavy path, so the connections panel dispatched unbounded. Applying 4 to it would put the SECOND simultaneous browse visitor into `busy`: a cold panel load issues THREE reads concurrently and then a fourth. 24 admits eight concurrent cold panel loads; a warm folio turn takes no slot at all (the version-keyed LRU returns before any dispatch). EVERY executor crossing now takes one of the two budgets — there is no unbounded dispatch and no opt-out |
| Execution resource behind each budget | **one `ThreadPoolExecutor` per budget class, `max_workers` = that budget's capacity** | ⟨ADDED 2026-08-04, Phase 136⟩ The two budgets are only separate if they do not share workers. Both previously dispatched into the *default* `run_in_executor` pool, which this repository never configures and whose width is not guaranteed: 24 browse jobs could occupy or queue ahead of every worker, so a heavy read timed out while its own semaphore still had capacity (reproduced with a two-worker default executor). Because `max_workers` equals the capacity, holding a slot now guarantees a worker. Built lazily — a process with the discovery flag OFF pays no threads — and retired when the service is collected |
| Overload behavior | Fail fast with a **503-equivalent "temporarily unavailable"** response (no hang); the existing `heavy_search_busy` + `Retry-After` shape (`web/search_api.py`) is the model | DATA-06 requires "no hang"; matches the already-shipped pattern instead of inventing a new one |
| Browse-enrichment LRU cache size | **5,000 entries** (keyed by `page_id`/`sys_id`) | The browse-enrichment path serves ONE page at a time per request; 5,000 entries covers a large multi-session working set without approaching the §1.4 RSS budget (compare `IIIF_MANIFEST_CACHE_MAX_ENTRIES=1500` for a similarly-scoped existing cache) |
| Work/Leads default page size | **50 rows/page** | Comfortably under the §1.2 200-row ceiling; matches the existing `SEARCH_API_BROWSE_TEXT_CAP`-style pattern of defaulting well inside the hard cap so a client override never exceeds it |
| Work/Leads maximum page size (client-requested) | **200 rows/page** | Equals the §1.2 hard cap exactly — never accept a client `limit` above this |
| Atlas drill-down default expansion | **1 hop, capped at the §1.3 node/edge/byte ceilings** | ATLAS-02: "single-hop server-side expansion"; the client never loads the full edge set |

## 3. Env-Var Naming Convention (for 134-06 to wire; not implemented in this plan)

Following the existing `CLAUDE.md` "Environment Variables" convention
(`SEARCH_API_*`, `NLI_*`), 134-06 should expose these as overridable env
vars, re-read per request (not baked in at import time), so they can be
tuned in production without a restart:

```
DISCOVERY_QUERY_TIMEOUT_BROWSE=2.0        # seconds
DISCOVERY_QUERY_TIMEOUT_WORK=5.0          # seconds
DISCOVERY_QUERY_TIMEOUT_ATLAS=10.0        # seconds
DISCOVERY_MAX_CONCURRENT_QUERIES=4         # the HEAVY budget (corpus-wide reads)
DISCOVERY_MAX_CONCURRENT_BROWSE_QUERIES=24 # the BROWSE budget (added 2026-08-04, Phase 136)
DISCOVERY_BROWSE_LRU_MAX_ENTRIES=5000
DISCOVERY_PAGE_SIZE_DEFAULT=50
DISCOVERY_PAGE_SIZE_MAX=200              # hard ceiling; never overridable above this
```

None of these env vars exist in code yet -- this section only fixes the
NAMES and DEFAULTS 134-06 must implement against, so this artifact and the
DiscoveryService code stay in lockstep from the first line of that plan.

## 4. Measured Actuals — dev-box measured (prod-box PENDING)

Measured 2026-07-23 by `scripts/bench_discovery.py` over the real sidecar
`discovery-public-136rebuild.db` (375.5 MB on disk), through the
`shared.discovery_service.DiscoveryService` async chokepoint, using a
benchmark-only readiness predicate that ANDs the loader's `_state.ready`
WITHOUT the `DISCOVERY_ENABLED` UI flag (F14 — the flag was never set) and
asserting every measured query returns nonzero rows (never an empty no-op).
Browse-path latency was measured with the browse LRU DISABLED
(`DISCOVERY_BROWSE_LRU_MAX_ENTRIES=0`), so every timed call is a real
cache-miss DB query (worst case; the production cache only lowers this).

### 4.1 Query latency + RSS (dev-box actuals vs §1 caps)

| Metric | Cap | Dev-box actual | Note |
|---|---|---|---|
| Browse-enrichment added latency (p95) | ≤ 150 ms | **0.65 ms** | max of the two browse reads below; cache OFF (worst case) |
| &nbsp;&nbsp;• `get_claims_for_page` (p95 / max) | — | 0.65 / 5.91 ms | 50 distinct pages, 51 rows total |
| &nbsp;&nbsp;• `get_pages_related_to_page` (p95 / max) | — | 0.64 / 1.06 ms | 50 distinct pages, 75 rows total |
| `get_work_witnesses` query (p95 / max) | (request cap ≤ 1.5 s) | 523.65 / 753.90 ms | 50 works incl. the heaviest; 2500 unit rows total |
| **Additional RSS (dev-box, sidecar+service+LRU warm)** | ≤ 250 MB | **12.2 MB** | dev-box indicative; prod-box authoritative (Task 3) |

**Executed-query counts (nonzero-result assertion passed for all):**
`get_claims_for_page` = 50 queries / 51 rows;
`get_pages_related_to_page` = 50 queries / 75 rows;
`get_work_witnesses` = 50 queries / 2500 unit rows.

### 4.2 MEASURED ACTUALS (prod-box) — recorded 2026-07-28

Measured on the EC2 web box by `scripts/bench_discovery.py --sample 50
--warm-passes 1` (exit 0) immediately after the 135-08 production deploy of the
v2 sidecar `discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff.db`
(370.0 MB on disk), through the same `DiscoveryService` async chokepoint and the
same F14 benchmark-only readiness predicate — `DISCOVERY_ENABLED` was OFF
throughout and never set. Browse-path latency again measured with the browse LRU
disabled (worst case). This closes the prod-box item deferred from 134-08 Task 3.
Full context: `.planning/phases/135-precision-certificate-confidence-bands/135-08-DEPLOY-LOG.md`.

| Metric | Cap | **Prod-box actual** | Dev-box (§4.1, v1 asset) |
|---|---|---|---|
| Browse-enrichment added latency (p95) | ≤ 150 ms | **0.49 ms** ✓ | 0.57 ms |
| &nbsp;&nbsp;• `get_claims_for_page` (p95 / max) | — | 0.49 / 3.12 ms | 0.57 / 6.99 ms |
| &nbsp;&nbsp;• `get_pages_related_to_page` (p95 / max) | — | 0.47 / 0.90 ms | 0.54 / 0.95 ms |
| `get_work_witnesses` query (p95 / max) | (request cap ≤ 1.5 s) | 200.77 / 357.17 ms | 117.21 / 478.95 ms |
| **Additional RSS (prod-box, sidecar+service+LRU warm)** | ≤ 250 MB | **11.2 MB** ✓ | 11.1 MB |

Executed-query counts (nonzero-result assertion passed for all): 50 queries each;
51 / 76 / 2500 rows respectively; warm-burst 2627 rows.

Both PERF-01 caps that are measurable before the UI surfaces exist are met with
wide margin — added RSS is ~4.5% of the 250 MB cap, and browse-enrichment p95 is
~0.3% of the 150 ms cap. The `get_work_witnesses` p95 is higher than dev-box on a
smaller sample (50 vs 200 works, so the draw hit a heavier mix); it remains
DB-side cost only and well inside the ≤ 1.5 s request budget.

### 4.3 Later-surface caps — PENDING

- **Work/Leads request-time p95 / response size (§1.2)** — PENDING until Phase
  136 ships `/work/{id}` (LEADS-01/02 and `/leads` are Phase 138 per ROADMAP.md +
  the REQUIREMENTS.md traceability table) (the query-latency figures
  above are the DB-side cost only; the full request-time budget is measured
  when the surface exists).
- **Atlas drill-down p95 / node-edge counts / response size (§1.3)** — PENDING
  until Phase 139 ships the bounded explorer (ATLAS-02).
- Any §2 default that measurement shows is mis-set would require a version bump
  per the "tunable only by versioning" rule above.

These later-surface caps and defaults exist now so those plans have a stable
contract to implement against, not because they are measured yet.

### 4.4 Corpus-wide findings page (§5 caps) — the FULL combination space, measured


Measured by `scripts/bench_discovery.py::bench_findings_page()` over 53581 materialized identifications (`discovery_identification`), page size 50, deep page 20. **483 combinations enumerated, 337 measured.** Every combination asserted a NONZERO measured population before its timing was recorded.

**What "full" means, stated rather than claimed.** The filter space is the cartesian product the shipped page can put into the shipped builder: 2 buckets (main, more) × every subset of {novelty, domain, author, work} = **32 filter states**, AND-composed. `web/pages/findings.py::fetch_findings` hands `bucket`, `novelty`, `domain`, `author` and `work_id` to `_build_findings_query` out of the persisted page state, each independently settable, and `_build_findings_filter` composes them with `AND`. Each state is crossed with every ROW UNIT and every SORT MODE for the ordering query, and with every ROW UNIT for the bounded COUNT and for a deep page. A state this asset has no rows for is a named skip carrying the combination it lacked — decided by an `EXISTS` probe against the shipped predicate, never by an argument that some combination "must" be unreachable.

**Artifact, audience and host** — a timing without its artifact is not comparable to the next one, because the public projection and the private rebuild are different databases with different row counts that report the identical `sidecar_version` string; and a laptop measurement is not a server measurement, which is where a slow query does its damage on a single-worker box:

- artifact: `discovery-v1-e9365edcab27af7d0739ab1a07b1a187683993bcbff41ff88128c8fe4fbb7181.db` (375.5 MB)
- audience: `public` · sidecar_version: `discovery-v1-real` · data_as_of: `2026-08-03`
- host: `Windows AMD64` (dev-box)

Every combination is built through the SHIPPED `shared/discovery_service.py::_build_findings_query` — the exact builder `get_findings_enveloped` calls — so the probe and the service can no longer diverge (136-14's owed follow-up, closed). The launch-statistics rows come from `_build_launch_contribution_sql` / `_build_launch_manuscript_sql` for the same reason.

The ordering and the visible-count numbers are recorded SEPARATELY because §5 gives them separate caps. The prior, PRE-materialization measurement was 3.41-3.55 s across four runs (D-10a), against the 1.5 s cap, when the same ordering was computed over display CLAIMS with no materialized band_rank / coverage_ppm and no identification grain, and 16 s for the deduped identification COUNT alone (main-pool-rule.md finding 13, "PERF-01 confirmed twice").

The **Population** column is the quantity the nonzero-result assertion tested: result ROWS for a row query, the COUNTED VALUE for an aggregate. A count query returns one row whatever it counts, so recording its row count would document a count of zero as a passing measurement.

| Combination | Cap | p50 | p95 | max | Population | Result |
|---|---|---|---|---|---|---|
| `findings_identification_band_rank_main` | p95 ≤ 1500 ms | 339.10 ms | **355.40 ms** | 355.40 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_main+novelty` | p95 ≤ 1500 ms | 132.31 ms | **146.03 ms** | 146.03 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_main+domain` | p95 ≤ 1500 ms | 105.66 ms | **115.68 ms** | 115.68 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_main+novelty+domain` | p95 ≤ 1500 ms | 93.38 ms | **102.90 ms** | 102.90 ms | 45 | PASS ✓ |
| `findings_identification_band_rank_main+author` | p95 ≤ 1500 ms | 112.13 ms | **127.24 ms** | 127.24 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_main+novelty+author` | p95 ≤ 1500 ms | 95.99 ms | **98.30 ms** | 98.30 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_main+domain+author` | p95 ≤ 1500 ms | 108.92 ms | **160.70 ms** | 160.70 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_main+novelty+domain+author` | p95 ≤ 1500 ms | 92.32 ms | **96.76 ms** | 96.76 ms | 45 | PASS ✓ |
| `findings_identification_band_rank_main+work` | p95 ≤ 1500 ms | 89.58 ms | **92.42 ms** | 92.42 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_main+novelty+work` | p95 ≤ 1500 ms | 87.02 ms | **114.28 ms** | 114.28 ms | 18 | PASS ✓ |
| `findings_identification_band_rank_main+domain+work` | p95 ≤ 1500 ms | 88.77 ms | **95.59 ms** | 95.59 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_main+novelty+domain+work` | p95 ≤ 1500 ms | 81.69 ms | **90.22 ms** | 90.22 ms | 18 | PASS ✓ |
| `findings_identification_band_rank_main+author+work` | p95 ≤ 1500 ms | 89.89 ms | **96.03 ms** | 96.03 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_main+novelty+author+work` | p95 ≤ 1500 ms | 84.01 ms | **92.84 ms** | 92.84 ms | 18 | PASS ✓ |
| `findings_identification_band_rank_main+domain+author+work` | p95 ≤ 1500 ms | 83.85 ms | **97.89 ms** | 97.89 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_main+novelty+domain+author+work` | p95 ≤ 1500 ms | 81.49 ms | **91.15 ms** | 91.15 ms | 18 | PASS ✓ |
| `findings_identification_band_rank_more` | p95 ≤ 1500 ms | 403.52 ms | **505.59 ms** | 505.59 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_more+novelty` | p95 ≤ 1500 ms | 162.94 ms | **177.63 ms** | 177.63 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_more+domain` | p95 ≤ 1500 ms | 145.11 ms | **158.34 ms** | 158.34 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_more+novelty+domain` | p95 ≤ 1500 ms | 126.12 ms | **131.45 ms** | 131.45 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_more+author` | p95 ≤ 1500 ms | 134.02 ms | **145.17 ms** | 145.17 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_more+novelty+author` | p95 ≤ 1500 ms | 119.47 ms | **137.06 ms** | 137.06 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_more+domain+author` | p95 ≤ 1500 ms | 131.74 ms | **142.13 ms** | 142.13 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_more+novelty+domain+author` | p95 ≤ 1500 ms | 115.01 ms | **117.38 ms** | 117.38 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_more+work` | p95 ≤ 1500 ms | 109.12 ms | **112.15 ms** | 112.15 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_more+novelty+work` | p95 ≤ 1500 ms | 110.90 ms | **120.15 ms** | 120.15 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_more+domain+work` | p95 ≤ 1500 ms | 120.58 ms | **125.82 ms** | 125.82 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_more+novelty+domain+work` | p95 ≤ 1500 ms | 110.66 ms | **113.05 ms** | 113.05 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_more+author+work` | p95 ≤ 1500 ms | 117.71 ms | **127.88 ms** | 127.88 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_more+novelty+author+work` | p95 ≤ 1500 ms | 120.26 ms | **125.70 ms** | 125.70 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_more+domain+author+work` | p95 ≤ 1500 ms | 128.62 ms | **131.13 ms** | 131.13 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_more+novelty+domain+author+work` | p95 ≤ 1500 ms | 108.03 ms | **116.72 ms** | 116.72 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_main` | p95 ≤ 1500 ms | 290.02 ms | **319.80 ms** | 319.80 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_main+novelty` | p95 ≤ 1500 ms | 92.90 ms | **95.33 ms** | 95.33 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_main+domain` | p95 ≤ 1500 ms | 76.50 ms | **81.77 ms** | 81.77 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_main+novelty+domain` | p95 ≤ 1500 ms | 58.84 ms | **72.29 ms** | 72.29 ms | 45 | PASS ✓ |
| `findings_identification_matched_text_main+author` | p95 ≤ 1500 ms | 76.62 ms | **86.43 ms** | 86.43 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_main+novelty+author` | p95 ≤ 1500 ms | 59.93 ms | **65.59 ms** | 65.59 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_main+domain+author` | p95 ≤ 1500 ms | 81.43 ms | **84.84 ms** | 84.84 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_main+novelty+domain+author` | p95 ≤ 1500 ms | 68.05 ms | **71.02 ms** | 71.02 ms | 45 | PASS ✓ |
| `findings_identification_matched_text_main+work` | p95 ≤ 1500 ms | 62.26 ms | **66.42 ms** | 66.42 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_main+novelty+work` | p95 ≤ 1500 ms | 67.71 ms | **73.94 ms** | 73.94 ms | 18 | PASS ✓ |
| `findings_identification_matched_text_main+domain+work` | p95 ≤ 1500 ms | 75.08 ms | **88.88 ms** | 88.88 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_main+novelty+domain+work` | p95 ≤ 1500 ms | 81.40 ms | **85.50 ms** | 85.50 ms | 18 | PASS ✓ |
| `findings_identification_matched_text_main+author+work` | p95 ≤ 1500 ms | 83.55 ms | **83.77 ms** | 83.77 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_main+novelty+author+work` | p95 ≤ 1500 ms | 77.23 ms | **96.18 ms** | 96.18 ms | 18 | PASS ✓ |
| `findings_identification_matched_text_main+domain+author+work` | p95 ≤ 1500 ms | 90.68 ms | **98.80 ms** | 98.80 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_main+novelty+domain+author+work` | p95 ≤ 1500 ms | 93.45 ms | **128.98 ms** | 128.98 ms | 18 | PASS ✓ |
| `findings_identification_matched_text_more` | p95 ≤ 1500 ms | 431.72 ms | **470.76 ms** | 470.76 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_more+novelty` | p95 ≤ 1500 ms | 163.03 ms | **178.16 ms** | 178.16 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_more+domain` | p95 ≤ 1500 ms | 148.26 ms | **173.36 ms** | 173.36 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_more+novelty+domain` | p95 ≤ 1500 ms | 118.88 ms | **134.94 ms** | 134.94 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_more+author` | p95 ≤ 1500 ms | 125.39 ms | **133.27 ms** | 133.27 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_more+novelty+author` | p95 ≤ 1500 ms | 119.32 ms | **134.18 ms** | 134.18 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_more+domain+author` | p95 ≤ 1500 ms | 135.58 ms | **136.73 ms** | 136.73 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_more+novelty+domain+author` | p95 ≤ 1500 ms | 116.83 ms | **132.07 ms** | 132.07 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_more+work` | p95 ≤ 1500 ms | 115.02 ms | **122.44 ms** | 122.44 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_more+novelty+work` | p95 ≤ 1500 ms | 113.21 ms | **114.06 ms** | 114.06 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_more+domain+work` | p95 ≤ 1500 ms | 120.37 ms | **136.99 ms** | 136.99 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_more+novelty+domain+work` | p95 ≤ 1500 ms | 116.17 ms | **125.04 ms** | 125.04 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_more+author+work` | p95 ≤ 1500 ms | 120.59 ms | **122.39 ms** | 122.39 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_more+novelty+author+work` | p95 ≤ 1500 ms | 112.63 ms | **122.87 ms** | 122.87 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_more+domain+author+work` | p95 ≤ 1500 ms | 118.21 ms | **124.83 ms** | 124.83 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_more+novelty+domain+author+work` | p95 ≤ 1500 ms | 116.78 ms | **128.31 ms** | 128.31 ms | 50 | PASS ✓ |
| `findings_identification_page_count_main` | p95 ≤ 1500 ms | 333.09 ms | **358.41 ms** | 358.41 ms | 50 | PASS ✓ |
| `findings_identification_page_count_main+novelty` | p95 ≤ 1500 ms | 128.05 ms | **136.78 ms** | 136.78 ms | 50 | PASS ✓ |
| `findings_identification_page_count_main+domain` | p95 ≤ 1500 ms | 117.95 ms | **125.27 ms** | 125.27 ms | 50 | PASS ✓ |
| `findings_identification_page_count_main+novelty+domain` | p95 ≤ 1500 ms | 88.34 ms | **97.15 ms** | 97.15 ms | 45 | PASS ✓ |
| `findings_identification_page_count_main+author` | p95 ≤ 1500 ms | 111.18 ms | **113.03 ms** | 113.03 ms | 50 | PASS ✓ |
| `findings_identification_page_count_main+novelty+author` | p95 ≤ 1500 ms | 93.49 ms | **97.85 ms** | 97.85 ms | 50 | PASS ✓ |
| `findings_identification_page_count_main+domain+author` | p95 ≤ 1500 ms | 108.63 ms | **111.43 ms** | 111.43 ms | 50 | PASS ✓ |
| `findings_identification_page_count_main+novelty+domain+author` | p95 ≤ 1500 ms | 99.36 ms | **103.58 ms** | 103.58 ms | 45 | PASS ✓ |
| `findings_identification_page_count_main+work` | p95 ≤ 1500 ms | 96.74 ms | **98.69 ms** | 98.69 ms | 50 | PASS ✓ |
| `findings_identification_page_count_main+novelty+work` | p95 ≤ 1500 ms | 83.15 ms | **89.10 ms** | 89.10 ms | 18 | PASS ✓ |
| `findings_identification_page_count_main+domain+work` | p95 ≤ 1500 ms | 89.92 ms | **102.03 ms** | 102.03 ms | 50 | PASS ✓ |
| `findings_identification_page_count_main+novelty+domain+work` | p95 ≤ 1500 ms | 88.75 ms | **93.35 ms** | 93.35 ms | 18 | PASS ✓ |
| `findings_identification_page_count_main+author+work` | p95 ≤ 1500 ms | 81.74 ms | **92.73 ms** | 92.73 ms | 50 | PASS ✓ |
| `findings_identification_page_count_main+novelty+author+work` | p95 ≤ 1500 ms | 91.82 ms | **93.53 ms** | 93.53 ms | 18 | PASS ✓ |
| `findings_identification_page_count_main+domain+author+work` | p95 ≤ 1500 ms | 91.32 ms | **136.80 ms** | 136.80 ms | 50 | PASS ✓ |
| `findings_identification_page_count_main+novelty+domain+author+work` | p95 ≤ 1500 ms | 61.43 ms | **68.57 ms** | 68.57 ms | 18 | PASS ✓ |
| `findings_identification_page_count_more` | p95 ≤ 1500 ms | 325.05 ms | **328.02 ms** | 328.02 ms | 50 | PASS ✓ |
| `findings_identification_page_count_more+novelty` | p95 ≤ 1500 ms | 112.44 ms | **115.69 ms** | 115.69 ms | 50 | PASS ✓ |
| `findings_identification_page_count_more+domain` | p95 ≤ 1500 ms | 90.90 ms | **104.71 ms** | 104.71 ms | 50 | PASS ✓ |
| `findings_identification_page_count_more+novelty+domain` | p95 ≤ 1500 ms | 84.35 ms | **86.57 ms** | 86.57 ms | 50 | PASS ✓ |
| `findings_identification_page_count_more+author` | p95 ≤ 1500 ms | 89.49 ms | **91.34 ms** | 91.34 ms | 50 | PASS ✓ |
| `findings_identification_page_count_more+novelty+author` | p95 ≤ 1500 ms | 82.07 ms | **93.33 ms** | 93.33 ms | 50 | PASS ✓ |
| `findings_identification_page_count_more+domain+author` | p95 ≤ 1500 ms | 103.56 ms | **122.73 ms** | 122.73 ms | 50 | PASS ✓ |
| `findings_identification_page_count_more+novelty+domain+author` | p95 ≤ 1500 ms | 104.36 ms | **120.06 ms** | 120.06 ms | 50 | PASS ✓ |
| `findings_identification_page_count_more+work` | p95 ≤ 1500 ms | 113.00 ms | **121.58 ms** | 121.58 ms | 50 | PASS ✓ |
| `findings_identification_page_count_more+novelty+work` | p95 ≤ 1500 ms | 119.13 ms | **124.42 ms** | 124.42 ms | 50 | PASS ✓ |
| `findings_identification_page_count_more+domain+work` | p95 ≤ 1500 ms | 116.23 ms | **136.25 ms** | 136.25 ms | 50 | PASS ✓ |
| `findings_identification_page_count_more+novelty+domain+work` | p95 ≤ 1500 ms | 110.77 ms | **122.46 ms** | 122.46 ms | 50 | PASS ✓ |
| `findings_identification_page_count_more+author+work` | p95 ≤ 1500 ms | 121.61 ms | **124.71 ms** | 124.71 ms | 50 | PASS ✓ |
| `findings_identification_page_count_more+novelty+author+work` | p95 ≤ 1500 ms | 115.44 ms | **124.55 ms** | 124.55 ms | 50 | PASS ✓ |
| `findings_identification_page_count_more+domain+author+work` | p95 ≤ 1500 ms | 117.23 ms | **127.10 ms** | 127.10 ms | 50 | PASS ✓ |
| `findings_identification_page_count_more+novelty+domain+author+work` | p95 ≤ 1500 ms | 113.98 ms | **121.25 ms** | 121.25 ms | 50 | PASS ✓ |
| `findings_identification_deep_page_20_main` | p95 ≤ 1500 ms | 376.12 ms | **385.12 ms** | 385.12 ms | 50 | PASS ✓ |
| `findings_identification_deep_page_20_main+novelty` | p95 ≤ 1500 ms | 129.99 ms | **141.57 ms** | 141.57 ms | 50 | PASS ✓ |
| `findings_identification_deep_page_20_main+domain` | p95 ≤ 1500 ms | 113.39 ms | **118.38 ms** | 118.38 ms | 50 | PASS ✓ |
| `findings_identification_deep_page_20_main+author` | p95 ≤ 1500 ms | 118.34 ms | **131.68 ms** | 131.68 ms | 50 | PASS ✓ |
| `findings_identification_deep_page_20_main+domain+author` | p95 ≤ 1500 ms | 115.62 ms | **139.29 ms** | 139.29 ms | 50 | PASS ✓ |
| `findings_identification_deep_page_20_more` | p95 ≤ 1500 ms | 421.47 ms | **433.01 ms** | 433.01 ms | 50 | PASS ✓ |
| `findings_identification_deep_page_20_more+novelty` | p95 ≤ 1500 ms | 167.98 ms | **181.68 ms** | 181.68 ms | 50 | PASS ✓ |
| `findings_identification_visible_total_main` | p95 ≤ 500 ms | 140.53 ms | **151.87 ms** | 151.87 ms | 27709 | PASS ✓ |
| `findings_identification_visible_total_main+novelty` | p95 ≤ 500 ms | 99.05 ms | **104.37 ms** | 104.37 ms | 4152 | PASS ✓ |
| `findings_identification_visible_total_main+domain` | p95 ≤ 500 ms | 107.86 ms | **110.36 ms** | 110.36 ms | 1133 | PASS ✓ |
| `findings_identification_visible_total_main+novelty+domain` | p95 ≤ 500 ms | 86.01 ms | **103.65 ms** | 103.65 ms | 45 | PASS ✓ |
| `findings_identification_visible_total_main+author` | p95 ≤ 500 ms | 99.26 ms | **111.98 ms** | 111.98 ms | 1797 | PASS ✓ |
| `findings_identification_visible_total_main+novelty+author` | p95 ≤ 500 ms | 93.66 ms | **98.25 ms** | 98.25 ms | 68 | PASS ✓ |
| `findings_identification_visible_total_main+domain+author` | p95 ≤ 500 ms | 105.24 ms | **117.09 ms** | 117.09 ms | 1133 | PASS ✓ |
| `findings_identification_visible_total_main+novelty+domain+author` | p95 ≤ 500 ms | 91.74 ms | **98.88 ms** | 98.88 ms | 45 | PASS ✓ |
| `findings_identification_visible_total_main+work` | p95 ≤ 500 ms | 87.21 ms | **101.08 ms** | 101.08 ms | 293 | PASS ✓ |
| `findings_identification_visible_total_main+novelty+work` | p95 ≤ 500 ms | 98.87 ms | **107.19 ms** | 107.19 ms | 18 | PASS ✓ |
| `findings_identification_visible_total_main+domain+work` | p95 ≤ 500 ms | 92.86 ms | **131.40 ms** | 131.40 ms | 293 | PASS ✓ |
| `findings_identification_visible_total_main+novelty+domain+work` | p95 ≤ 500 ms | 87.25 ms | **141.19 ms** | 141.19 ms | 18 | PASS ✓ |
| `findings_identification_visible_total_main+author+work` | p95 ≤ 500 ms | 89.33 ms | **100.01 ms** | 100.01 ms | 293 | PASS ✓ |
| `findings_identification_visible_total_main+novelty+author+work` | p95 ≤ 500 ms | 83.13 ms | **86.19 ms** | 86.19 ms | 18 | PASS ✓ |
| `findings_identification_visible_total_main+domain+author+work` | p95 ≤ 500 ms | 81.56 ms | **86.64 ms** | 86.64 ms | 293 | PASS ✓ |
| `findings_identification_visible_total_main+novelty+domain+author+work` | p95 ≤ 500 ms | 83.06 ms | **86.19 ms** | 86.19 ms | 18 | PASS ✓ |
| `findings_identification_visible_total_more` | p95 ≤ 500 ms | 172.75 ms | **179.22 ms** | 179.22 ms | 25872 | PASS ✓ |
| `findings_identification_visible_total_more+novelty` | p95 ≤ 500 ms | 126.10 ms | **131.84 ms** | 131.84 ms | 4251 | PASS ✓ |
| `findings_identification_visible_total_more+domain` | p95 ≤ 500 ms | 118.28 ms | **131.85 ms** | 131.85 ms | 531 | PASS ✓ |
| `findings_identification_visible_total_more+novelty+domain` | p95 ≤ 500 ms | 86.49 ms | **94.50 ms** | 94.50 ms | 138 | PASS ✓ |
| `findings_identification_visible_total_more+author` | p95 ≤ 500 ms | 219.06 ms | **278.40 ms** | 278.40 ms | 390 | PASS ✓ |
| `findings_identification_visible_total_more+novelty+author` | p95 ≤ 500 ms | 174.97 ms | **199.16 ms** | 199.16 ms | 131 | PASS ✓ |
| `findings_identification_visible_total_more+domain+author` | p95 ≤ 500 ms | 135.79 ms | **210.02 ms** | 210.02 ms | 390 | PASS ✓ |
| `findings_identification_visible_total_more+novelty+domain+author` | p95 ≤ 500 ms | 81.29 ms | **87.77 ms** | 87.77 ms | 131 | PASS ✓ |
| `findings_identification_visible_total_more+work` | p95 ≤ 500 ms | 77.55 ms | **91.43 ms** | 91.43 ms | 390 | PASS ✓ |
| `findings_identification_visible_total_more+novelty+work` | p95 ≤ 500 ms | 71.01 ms | **81.29 ms** | 81.29 ms | 131 | PASS ✓ |
| `findings_identification_visible_total_more+domain+work` | p95 ≤ 500 ms | 77.05 ms | **91.03 ms** | 91.03 ms | 390 | PASS ✓ |
| `findings_identification_visible_total_more+novelty+domain+work` | p95 ≤ 500 ms | 75.00 ms | **81.12 ms** | 81.12 ms | 131 | PASS ✓ |
| `findings_identification_visible_total_more+author+work` | p95 ≤ 500 ms | 83.23 ms | **96.16 ms** | 96.16 ms | 390 | PASS ✓ |
| `findings_identification_visible_total_more+novelty+author+work` | p95 ≤ 500 ms | 74.21 ms | **82.48 ms** | 82.48 ms | 131 | PASS ✓ |
| `findings_identification_visible_total_more+domain+author+work` | p95 ≤ 500 ms | 74.05 ms | **80.02 ms** | 80.02 ms | 390 | PASS ✓ |
| `findings_identification_visible_total_more+novelty+domain+author+work` | p95 ≤ 500 ms | 74.74 ms | **94.14 ms** | 94.14 ms | 131 | PASS ✓ |
| `findings_manuscript_band_rank_main` | p95 ≤ 1500 ms | 265.89 ms | **298.78 ms** | 298.78 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_main+novelty` | p95 ≤ 1500 ms | 93.47 ms | **108.90 ms** | 108.90 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_main+domain` | p95 ≤ 1500 ms | 86.61 ms | **90.13 ms** | 90.13 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_main+novelty+domain` | p95 ≤ 1500 ms | 63.47 ms | **72.84 ms** | 72.84 ms | 40 | PASS ✓ |
| `findings_manuscript_band_rank_main+author` | p95 ≤ 1500 ms | 92.37 ms | **101.12 ms** | 101.12 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_main+novelty+author` | p95 ≤ 1500 ms | 62.98 ms | **64.98 ms** | 64.98 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_main+domain+author` | p95 ≤ 1500 ms | 81.40 ms | **87.35 ms** | 87.35 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_main+novelty+domain+author` | p95 ≤ 1500 ms | 65.07 ms | **75.24 ms** | 75.24 ms | 40 | PASS ✓ |
| `findings_manuscript_band_rank_main+work` | p95 ≤ 1500 ms | 57.63 ms | **68.11 ms** | 68.11 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_main+novelty+work` | p95 ≤ 1500 ms | 57.86 ms | **65.44 ms** | 65.44 ms | 18 | PASS ✓ |
| `findings_manuscript_band_rank_main+domain+work` | p95 ≤ 1500 ms | 62.14 ms | **66.91 ms** | 66.91 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_main+novelty+domain+work` | p95 ≤ 1500 ms | 56.54 ms | **64.79 ms** | 64.79 ms | 18 | PASS ✓ |
| `findings_manuscript_band_rank_main+author+work` | p95 ≤ 1500 ms | 57.83 ms | **67.56 ms** | 67.56 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_main+novelty+author+work` | p95 ≤ 1500 ms | 56.46 ms | **64.23 ms** | 64.23 ms | 18 | PASS ✓ |
| `findings_manuscript_band_rank_main+domain+author+work` | p95 ≤ 1500 ms | 63.48 ms | **67.27 ms** | 67.27 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_main+novelty+domain+author+work` | p95 ≤ 1500 ms | 55.84 ms | **61.61 ms** | 61.61 ms | 18 | PASS ✓ |
| `findings_manuscript_band_rank_more` | p95 ≤ 1500 ms | 286.79 ms | **298.73 ms** | 298.73 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_more+novelty` | p95 ≤ 1500 ms | 102.63 ms | **119.96 ms** | 119.96 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_more+domain` | p95 ≤ 1500 ms | 90.70 ms | **104.34 ms** | 104.34 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_more+novelty+domain` | p95 ≤ 1500 ms | 85.86 ms | **89.31 ms** | 89.31 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_more+author` | p95 ≤ 1500 ms | 88.33 ms | **96.73 ms** | 96.73 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_more+novelty+author` | p95 ≤ 1500 ms | 73.27 ms | **77.42 ms** | 77.42 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_more+domain+author` | p95 ≤ 1500 ms | 90.80 ms | **93.50 ms** | 93.50 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_more+novelty+domain+author` | p95 ≤ 1500 ms | 75.03 ms | **83.10 ms** | 83.10 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_more+work` | p95 ≤ 1500 ms | 82.75 ms | **98.84 ms** | 98.84 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_more+novelty+work` | p95 ≤ 1500 ms | 75.27 ms | **81.91 ms** | 81.91 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_more+domain+work` | p95 ≤ 1500 ms | 81.09 ms | **107.09 ms** | 107.09 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_more+novelty+domain+work` | p95 ≤ 1500 ms | 79.67 ms | **89.57 ms** | 89.57 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_more+author+work` | p95 ≤ 1500 ms | 79.97 ms | **86.66 ms** | 86.66 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_more+novelty+author+work` | p95 ≤ 1500 ms | 80.75 ms | **82.89 ms** | 82.89 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_more+domain+author+work` | p95 ≤ 1500 ms | 80.06 ms | **81.70 ms** | 81.70 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_more+novelty+domain+author+work` | p95 ≤ 1500 ms | 83.83 ms | **94.34 ms** | 94.34 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_main` | p95 ≤ 1500 ms | 268.83 ms | **281.51 ms** | 281.51 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_main+novelty` | p95 ≤ 1500 ms | 87.98 ms | **96.71 ms** | 96.71 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_main+domain` | p95 ≤ 1500 ms | 82.39 ms | **88.62 ms** | 88.62 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_main+novelty+domain` | p95 ≤ 1500 ms | 59.86 ms | **62.65 ms** | 62.65 ms | 40 | PASS ✓ |
| `findings_manuscript_matched_text_main+author` | p95 ≤ 1500 ms | 81.83 ms | **98.91 ms** | 98.91 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_main+novelty+author` | p95 ≤ 1500 ms | 59.45 ms | **71.02 ms** | 71.02 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_main+domain+author` | p95 ≤ 1500 ms | 80.37 ms | **91.34 ms** | 91.34 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_main+novelty+domain+author` | p95 ≤ 1500 ms | 64.96 ms | **73.86 ms** | 73.86 ms | 40 | PASS ✓ |
| `findings_manuscript_matched_text_main+work` | p95 ≤ 1500 ms | 70.00 ms | **78.54 ms** | 78.54 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_main+novelty+work` | p95 ≤ 1500 ms | 63.83 ms | **78.28 ms** | 78.28 ms | 18 | PASS ✓ |
| `findings_manuscript_matched_text_main+domain+work` | p95 ≤ 1500 ms | 67.59 ms | **71.86 ms** | 71.86 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_main+novelty+domain+work` | p95 ≤ 1500 ms | 79.53 ms | **84.11 ms** | 84.11 ms | 18 | PASS ✓ |
| `findings_manuscript_matched_text_main+author+work` | p95 ≤ 1500 ms | 84.98 ms | **86.56 ms** | 86.56 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_main+novelty+author+work` | p95 ≤ 1500 ms | 81.13 ms | **97.26 ms** | 97.26 ms | 18 | PASS ✓ |
| `findings_manuscript_matched_text_main+domain+author+work` | p95 ≤ 1500 ms | 74.30 ms | **84.03 ms** | 84.03 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_main+novelty+domain+author+work` | p95 ≤ 1500 ms | 79.09 ms | **105.18 ms** | 105.18 ms | 18 | PASS ✓ |
| `findings_manuscript_matched_text_more` | p95 ≤ 1500 ms | 367.01 ms | **380.65 ms** | 380.65 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_more+novelty` | p95 ≤ 1500 ms | 135.87 ms | **151.81 ms** | 151.81 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_more+domain` | p95 ≤ 1500 ms | 121.25 ms | **141.87 ms** | 141.87 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_more+novelty+domain` | p95 ≤ 1500 ms | 110.97 ms | **121.87 ms** | 121.87 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_more+author` | p95 ≤ 1500 ms | 119.58 ms | **143.86 ms** | 143.86 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_more+novelty+author` | p95 ≤ 1500 ms | 103.81 ms | **114.99 ms** | 114.99 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_more+domain+author` | p95 ≤ 1500 ms | 113.35 ms | **121.50 ms** | 121.50 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_more+novelty+domain+author` | p95 ≤ 1500 ms | 100.16 ms | **111.59 ms** | 111.59 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_more+work` | p95 ≤ 1500 ms | 103.75 ms | **110.62 ms** | 110.62 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_more+novelty+work` | p95 ≤ 1500 ms | 97.42 ms | **103.40 ms** | 103.40 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_more+domain+work` | p95 ≤ 1500 ms | 107.62 ms | **121.39 ms** | 121.39 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_more+novelty+domain+work` | p95 ≤ 1500 ms | 100.67 ms | **104.88 ms** | 104.88 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_more+author+work` | p95 ≤ 1500 ms | 105.97 ms | **119.10 ms** | 119.10 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_more+novelty+author+work` | p95 ≤ 1500 ms | 102.01 ms | **103.17 ms** | 103.17 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_more+domain+author+work` | p95 ≤ 1500 ms | 106.38 ms | **120.56 ms** | 120.56 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_more+novelty+domain+author+work` | p95 ≤ 1500 ms | 104.49 ms | **105.85 ms** | 105.85 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_main` | p95 ≤ 1500 ms | 319.35 ms | **331.04 ms** | 331.04 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_main+novelty` | p95 ≤ 1500 ms | 123.46 ms | **132.93 ms** | 132.93 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_main+domain` | p95 ≤ 1500 ms | 110.34 ms | **117.27 ms** | 117.27 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_main+novelty+domain` | p95 ≤ 1500 ms | 90.61 ms | **95.09 ms** | 95.09 ms | 40 | PASS ✓ |
| `findings_manuscript_page_count_main+author` | p95 ≤ 1500 ms | 113.24 ms | **114.97 ms** | 114.97 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_main+novelty+author` | p95 ≤ 1500 ms | 88.31 ms | **107.38 ms** | 107.38 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_main+domain+author` | p95 ≤ 1500 ms | 108.27 ms | **116.51 ms** | 116.51 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_main+novelty+domain+author` | p95 ≤ 1500 ms | 99.62 ms | **132.95 ms** | 132.95 ms | 40 | PASS ✓ |
| `findings_manuscript_page_count_main+work` | p95 ≤ 1500 ms | 93.77 ms | **101.41 ms** | 101.41 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_main+novelty+work` | p95 ≤ 1500 ms | 86.50 ms | **102.85 ms** | 102.85 ms | 18 | PASS ✓ |
| `findings_manuscript_page_count_main+domain+work` | p95 ≤ 1500 ms | 91.57 ms | **96.21 ms** | 96.21 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_main+novelty+domain+work` | p95 ≤ 1500 ms | 89.00 ms | **100.61 ms** | 100.61 ms | 18 | PASS ✓ |
| `findings_manuscript_page_count_main+author+work` | p95 ≤ 1500 ms | 95.99 ms | **100.64 ms** | 100.64 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_main+novelty+author+work` | p95 ≤ 1500 ms | 88.87 ms | **101.09 ms** | 101.09 ms | 18 | PASS ✓ |
| `findings_manuscript_page_count_main+domain+author+work` | p95 ≤ 1500 ms | 64.96 ms | **75.18 ms** | 75.18 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_main+novelty+domain+author+work` | p95 ≤ 1500 ms | 62.83 ms | **66.30 ms** | 66.30 ms | 18 | PASS ✓ |
| `findings_manuscript_page_count_more` | p95 ≤ 1500 ms | 293.43 ms | **338.05 ms** | 338.05 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_more+novelty` | p95 ≤ 1500 ms | 109.62 ms | **113.99 ms** | 113.99 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_more+domain` | p95 ≤ 1500 ms | 99.81 ms | **100.42 ms** | 100.42 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_more+novelty+domain` | p95 ≤ 1500 ms | 79.65 ms | **83.52 ms** | 83.52 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_more+author` | p95 ≤ 1500 ms | 91.14 ms | **95.05 ms** | 95.05 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_more+novelty+author` | p95 ≤ 1500 ms | 81.80 ms | **92.35 ms** | 92.35 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_more+domain+author` | p95 ≤ 1500 ms | 88.85 ms | **95.88 ms** | 95.88 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_more+novelty+domain+author` | p95 ≤ 1500 ms | 93.69 ms | **108.96 ms** | 108.96 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_more+work` | p95 ≤ 1500 ms | 98.10 ms | **116.15 ms** | 116.15 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_more+novelty+work` | p95 ≤ 1500 ms | 111.29 ms | **114.30 ms** | 114.30 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_more+domain+work` | p95 ≤ 1500 ms | 117.03 ms | **128.14 ms** | 128.14 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_more+novelty+domain+work` | p95 ≤ 1500 ms | 116.99 ms | **125.89 ms** | 125.89 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_more+author+work` | p95 ≤ 1500 ms | 118.54 ms | **128.85 ms** | 128.85 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_more+novelty+author+work` | p95 ≤ 1500 ms | 108.71 ms | **127.19 ms** | 127.19 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_more+domain+author+work` | p95 ≤ 1500 ms | 121.56 ms | **130.02 ms** | 130.02 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_more+novelty+domain+author+work` | p95 ≤ 1500 ms | 108.40 ms | **128.81 ms** | 128.81 ms | 50 | PASS ✓ |
| `findings_manuscript_deep_page_20_main` | p95 ≤ 1500 ms | 337.65 ms | **353.88 ms** | 353.88 ms | 50 | PASS ✓ |
| `findings_manuscript_deep_page_20_main+novelty` | p95 ≤ 1500 ms | 133.71 ms | **168.80 ms** | 168.80 ms | 50 | PASS ✓ |
| `findings_manuscript_deep_page_20_main+domain` | p95 ≤ 1500 ms | 114.02 ms | **117.42 ms** | 117.42 ms | 50 | PASS ✓ |
| `findings_manuscript_deep_page_20_main+author` | p95 ≤ 1500 ms | 117.05 ms | **130.16 ms** | 130.16 ms | 50 | PASS ✓ |
| `findings_manuscript_deep_page_20_main+domain+author` | p95 ≤ 1500 ms | 117.35 ms | **123.49 ms** | 123.49 ms | 50 | PASS ✓ |
| `findings_manuscript_deep_page_20_more` | p95 ≤ 1500 ms | 395.28 ms | **456.39 ms** | 456.39 ms | 50 | PASS ✓ |
| `findings_manuscript_deep_page_20_more+novelty` | p95 ≤ 1500 ms | 161.58 ms | **172.46 ms** | 172.46 ms | 50 | PASS ✓ |
| `findings_manuscript_visible_total_main` | p95 ≤ 500 ms | 160.65 ms | **166.75 ms** | 166.75 ms | 23312 | PASS ✓ |
| `findings_manuscript_visible_total_main+novelty` | p95 ≤ 500 ms | 106.69 ms | **113.16 ms** | 113.16 ms | 3666 | PASS ✓ |
| `findings_manuscript_visible_total_main+domain` | p95 ≤ 500 ms | 110.13 ms | **122.33 ms** | 122.33 ms | 1093 | PASS ✓ |
| `findings_manuscript_visible_total_main+novelty+domain` | p95 ≤ 500 ms | 90.20 ms | **97.59 ms** | 97.59 ms | 40 | PASS ✓ |
| `findings_manuscript_visible_total_main+author` | p95 ≤ 500 ms | 102.01 ms | **116.90 ms** | 116.90 ms | 1714 | PASS ✓ |
| `findings_manuscript_visible_total_main+novelty+author` | p95 ≤ 500 ms | 94.82 ms | **104.08 ms** | 104.08 ms | 61 | PASS ✓ |
| `findings_manuscript_visible_total_main+domain+author` | p95 ≤ 500 ms | 112.92 ms | **121.65 ms** | 121.65 ms | 1093 | PASS ✓ |
| `findings_manuscript_visible_total_main+novelty+domain+author` | p95 ≤ 500 ms | 92.52 ms | **98.74 ms** | 98.74 ms | 40 | PASS ✓ |
| `findings_manuscript_visible_total_main+work` | p95 ≤ 500 ms | 88.24 ms | **97.24 ms** | 97.24 ms | 293 | PASS ✓ |
| `findings_manuscript_visible_total_main+novelty+work` | p95 ≤ 500 ms | 100.97 ms | **146.78 ms** | 146.78 ms | 18 | PASS ✓ |
| `findings_manuscript_visible_total_main+domain+work` | p95 ≤ 500 ms | 92.24 ms | **99.44 ms** | 99.44 ms | 293 | PASS ✓ |
| `findings_manuscript_visible_total_main+novelty+domain+work` | p95 ≤ 500 ms | 88.08 ms | **91.16 ms** | 91.16 ms | 18 | PASS ✓ |
| `findings_manuscript_visible_total_main+author+work` | p95 ≤ 500 ms | 91.34 ms | **96.35 ms** | 96.35 ms | 293 | PASS ✓ |
| `findings_manuscript_visible_total_main+novelty+author+work` | p95 ≤ 500 ms | 96.09 ms | **102.12 ms** | 102.12 ms | 18 | PASS ✓ |
| `findings_manuscript_visible_total_main+domain+author+work` | p95 ≤ 500 ms | 90.56 ms | **90.91 ms** | 90.91 ms | 293 | PASS ✓ |
| `findings_manuscript_visible_total_main+novelty+domain+author+work` | p95 ≤ 500 ms | 82.32 ms | **91.69 ms** | 91.69 ms | 18 | PASS ✓ |
| `findings_manuscript_visible_total_more` | p95 ≤ 500 ms | 180.31 ms | **197.65 ms** | 197.65 ms | 18511 | PASS ✓ |
| `findings_manuscript_visible_total_more+novelty` | p95 ≤ 500 ms | 125.86 ms | **136.35 ms** | 136.35 ms | 2762 | PASS ✓ |
| `findings_manuscript_visible_total_more+domain` | p95 ≤ 500 ms | 137.01 ms | **159.64 ms** | 159.64 ms | 529 | PASS ✓ |
| `findings_manuscript_visible_total_more+novelty+domain` | p95 ≤ 500 ms | 119.37 ms | **122.24 ms** | 122.24 ms | 138 | PASS ✓ |
| `findings_manuscript_visible_total_more+author` | p95 ≤ 500 ms | 121.63 ms | **130.10 ms** | 130.10 ms | 390 | PASS ✓ |
| `findings_manuscript_visible_total_more+novelty+author` | p95 ≤ 500 ms | 82.40 ms | **91.72 ms** | 91.72 ms | 131 | PASS ✓ |
| `findings_manuscript_visible_total_more+domain+author` | p95 ≤ 500 ms | 92.94 ms | **96.37 ms** | 96.37 ms | 390 | PASS ✓ |
| `findings_manuscript_visible_total_more+novelty+domain+author` | p95 ≤ 500 ms | 83.42 ms | **88.97 ms** | 88.97 ms | 131 | PASS ✓ |
| `findings_manuscript_visible_total_more+work` | p95 ≤ 500 ms | 78.63 ms | **87.53 ms** | 87.53 ms | 390 | PASS ✓ |
| `findings_manuscript_visible_total_more+novelty+work` | p95 ≤ 500 ms | 79.93 ms | **81.48 ms** | 81.48 ms | 131 | PASS ✓ |
| `findings_manuscript_visible_total_more+domain+work` | p95 ≤ 500 ms | 74.13 ms | **77.22 ms** | 77.22 ms | 390 | PASS ✓ |
| `findings_manuscript_visible_total_more+novelty+domain+work` | p95 ≤ 500 ms | 79.39 ms | **83.33 ms** | 83.33 ms | 131 | PASS ✓ |
| `findings_manuscript_visible_total_more+author+work` | p95 ≤ 500 ms | 79.36 ms | **89.62 ms** | 89.62 ms | 390 | PASS ✓ |
| `findings_manuscript_visible_total_more+novelty+author+work` | p95 ≤ 500 ms | 78.67 ms | **81.24 ms** | 81.24 ms | 131 | PASS ✓ |
| `findings_manuscript_visible_total_more+domain+author+work` | p95 ≤ 500 ms | 79.49 ms | **87.01 ms** | 87.01 ms | 390 | PASS ✓ |
| `findings_manuscript_visible_total_more+novelty+domain+author+work` | p95 ≤ 500 ms | 88.95 ms | **95.75 ms** | 95.75 ms | 131 | PASS ✓ |
| `findings_work_band_rank_main` | p95 ≤ 1500 ms | 183.65 ms | **219.79 ms** | 219.79 ms | 50 | PASS ✓ |
| `findings_work_band_rank_main+domain` | p95 ≤ 1500 ms | 98.39 ms | **103.06 ms** | 103.06 ms | 14 | PASS ✓ |
| `findings_work_band_rank_main+author` | p95 ≤ 1500 ms | 104.99 ms | **117.83 ms** | 117.83 ms | 25 | PASS ✓ |
| `findings_work_band_rank_main+domain+author` | p95 ≤ 1500 ms | 107.45 ms | **119.57 ms** | 119.57 ms | 14 | PASS ✓ |
| `findings_work_band_rank_main+work` | p95 ≤ 1500 ms | 88.95 ms | **95.85 ms** | 95.85 ms | 1 | PASS ✓ |
| `findings_work_band_rank_main+domain+work` | p95 ≤ 1500 ms | 87.14 ms | **96.39 ms** | 96.39 ms | 1 | PASS ✓ |
| `findings_work_band_rank_main+author+work` | p95 ≤ 1500 ms | 89.47 ms | **98.68 ms** | 98.68 ms | 1 | PASS ✓ |
| `findings_work_band_rank_main+domain+author+work` | p95 ≤ 1500 ms | 87.02 ms | **97.95 ms** | 97.95 ms | 1 | PASS ✓ |
| `findings_work_band_rank_more` | p95 ≤ 1500 ms | 243.59 ms | **259.64 ms** | 259.64 ms | 50 | PASS ✓ |
| `findings_work_band_rank_more+domain` | p95 ≤ 1500 ms | 134.31 ms | **136.54 ms** | 136.54 ms | 2 | PASS ✓ |
| `findings_work_band_rank_more+author` | p95 ≤ 1500 ms | 131.30 ms | **134.68 ms** | 134.68 ms | 1 | PASS ✓ |
| `findings_work_band_rank_more+domain+author` | p95 ≤ 1500 ms | 132.31 ms | **142.03 ms** | 142.03 ms | 1 | PASS ✓ |
| `findings_work_band_rank_more+work` | p95 ≤ 1500 ms | 119.18 ms | **146.32 ms** | 146.32 ms | 1 | PASS ✓ |
| `findings_work_band_rank_more+domain+work` | p95 ≤ 1500 ms | 109.99 ms | **120.57 ms** | 120.57 ms | 1 | PASS ✓ |
| `findings_work_band_rank_more+author+work` | p95 ≤ 1500 ms | 109.44 ms | **112.62 ms** | 112.62 ms | 1 | PASS ✓ |
| `findings_work_band_rank_more+domain+author+work` | p95 ≤ 1500 ms | 113.19 ms | **123.99 ms** | 123.99 ms | 1 | PASS ✓ |
| `findings_work_matched_text_main` | p95 ≤ 1500 ms | 211.99 ms | **229.62 ms** | 229.62 ms | 50 | PASS ✓ |
| `findings_work_matched_text_main+domain` | p95 ≤ 1500 ms | 103.67 ms | **116.45 ms** | 116.45 ms | 14 | PASS ✓ |
| `findings_work_matched_text_main+author` | p95 ≤ 1500 ms | 104.49 ms | **109.92 ms** | 109.92 ms | 25 | PASS ✓ |
| `findings_work_matched_text_main+domain+author` | p95 ≤ 1500 ms | 118.23 ms | **155.53 ms** | 155.53 ms | 14 | PASS ✓ |
| `findings_work_matched_text_main+work` | p95 ≤ 1500 ms | 95.77 ms | **121.95 ms** | 121.95 ms | 1 | PASS ✓ |
| `findings_work_matched_text_main+domain+work` | p95 ≤ 1500 ms | 95.47 ms | **95.91 ms** | 95.91 ms | 1 | PASS ✓ |
| `findings_work_matched_text_main+author+work` | p95 ≤ 1500 ms | 88.46 ms | **90.57 ms** | 90.57 ms | 1 | PASS ✓ |
| `findings_work_matched_text_main+domain+author+work` | p95 ≤ 1500 ms | 87.87 ms | **100.17 ms** | 100.17 ms | 1 | PASS ✓ |
| `findings_work_matched_text_more` | p95 ≤ 1500 ms | 255.03 ms | **262.40 ms** | 262.40 ms | 50 | PASS ✓ |
| `findings_work_matched_text_more+domain` | p95 ≤ 1500 ms | 143.72 ms | **148.19 ms** | 148.19 ms | 2 | PASS ✓ |
| `findings_work_matched_text_more+author` | p95 ≤ 1500 ms | 138.56 ms | **144.20 ms** | 144.20 ms | 1 | PASS ✓ |
| `findings_work_matched_text_more+domain+author` | p95 ≤ 1500 ms | 134.98 ms | **147.41 ms** | 147.41 ms | 1 | PASS ✓ |
| `findings_work_matched_text_more+work` | p95 ≤ 1500 ms | 112.41 ms | **127.49 ms** | 127.49 ms | 1 | PASS ✓ |
| `findings_work_matched_text_more+domain+work` | p95 ≤ 1500 ms | 111.55 ms | **128.22 ms** | 128.22 ms | 1 | PASS ✓ |
| `findings_work_matched_text_more+author+work` | p95 ≤ 1500 ms | 118.41 ms | **119.73 ms** | 119.73 ms | 1 | PASS ✓ |
| `findings_work_matched_text_more+domain+author+work` | p95 ≤ 1500 ms | 121.26 ms | **126.28 ms** | 126.28 ms | 1 | PASS ✓ |
| `findings_work_page_count_main` | p95 ≤ 1500 ms | 207.98 ms | **223.55 ms** | 223.55 ms | 50 | PASS ✓ |
| `findings_work_page_count_main+domain` | p95 ≤ 1500 ms | 103.30 ms | **123.47 ms** | 123.47 ms | 14 | PASS ✓ |
| `findings_work_page_count_main+author` | p95 ≤ 1500 ms | 108.50 ms | **113.63 ms** | 113.63 ms | 25 | PASS ✓ |
| `findings_work_page_count_main+domain+author` | p95 ≤ 1500 ms | 104.59 ms | **117.83 ms** | 117.83 ms | 14 | PASS ✓ |
| `findings_work_page_count_main+work` | p95 ≤ 1500 ms | 63.23 ms | **80.08 ms** | 80.08 ms | 1 | PASS ✓ |
| `findings_work_page_count_main+domain+work` | p95 ≤ 1500 ms | 64.04 ms | **66.46 ms** | 66.46 ms | 1 | PASS ✓ |
| `findings_work_page_count_main+author+work` | p95 ≤ 1500 ms | 58.79 ms | **59.91 ms** | 59.91 ms | 1 | PASS ✓ |
| `findings_work_page_count_main+domain+author+work` | p95 ≤ 1500 ms | 57.38 ms | **62.49 ms** | 62.49 ms | 1 | PASS ✓ |
| `findings_work_page_count_more` | p95 ≤ 1500 ms | 180.99 ms | **200.83 ms** | 200.83 ms | 50 | PASS ✓ |
| `findings_work_page_count_more+domain` | p95 ≤ 1500 ms | 88.72 ms | **89.85 ms** | 89.85 ms | 2 | PASS ✓ |
| `findings_work_page_count_more+author` | p95 ≤ 1500 ms | 85.67 ms | **90.19 ms** | 90.19 ms | 1 | PASS ✓ |
| `findings_work_page_count_more+domain+author` | p95 ≤ 1500 ms | 85.26 ms | **91.31 ms** | 91.31 ms | 1 | PASS ✓ |
| `findings_work_page_count_more+work` | p95 ≤ 1500 ms | 79.04 ms | **83.68 ms** | 83.68 ms | 1 | PASS ✓ |
| `findings_work_page_count_more+domain+work` | p95 ≤ 1500 ms | 88.92 ms | **93.38 ms** | 93.38 ms | 1 | PASS ✓ |
| `findings_work_page_count_more+author+work` | p95 ≤ 1500 ms | 94.13 ms | **103.27 ms** | 103.27 ms | 1 | PASS ✓ |
| `findings_work_page_count_more+domain+author+work` | p95 ≤ 1500 ms | 103.78 ms | **116.41 ms** | 116.41 ms | 1 | PASS ✓ |
| `findings_work_visible_total_main` | p95 ≤ 500 ms | 145.05 ms | **148.65 ms** | 148.65 ms | 478 | PASS ✓ |
| `findings_work_visible_total_main+domain` | p95 ≤ 500 ms | 102.66 ms | **120.96 ms** | 120.96 ms | 14 | PASS ✓ |
| `findings_work_visible_total_main+author` | p95 ≤ 500 ms | 102.34 ms | **119.53 ms** | 119.53 ms | 25 | PASS ✓ |
| `findings_work_visible_total_main+domain+author` | p95 ≤ 500 ms | 104.27 ms | **120.73 ms** | 120.73 ms | 14 | PASS ✓ |
| `findings_work_visible_total_main+work` | p95 ≤ 500 ms | 85.96 ms | **92.85 ms** | 92.85 ms | 1 | PASS ✓ |
| `findings_work_visible_total_main+domain+work` | p95 ≤ 500 ms | 88.02 ms | **94.92 ms** | 94.92 ms | 1 | PASS ✓ |
| `findings_work_visible_total_main+author+work` | p95 ≤ 500 ms | 86.62 ms | **95.32 ms** | 95.32 ms | 1 | PASS ✓ |
| `findings_work_visible_total_main+domain+author+work` | p95 ≤ 500 ms | 85.20 ms | **86.52 ms** | 86.52 ms | 1 | PASS ✓ |
| `findings_work_visible_total_more` | p95 ≤ 500 ms | 180.43 ms | **183.46 ms** | 183.46 ms | 504 | PASS ✓ |
| `findings_work_visible_total_more+domain` | p95 ≤ 500 ms | 127.05 ms | **139.17 ms** | 139.17 ms | 2 | PASS ✓ |
| `findings_work_visible_total_more+author` | p95 ≤ 500 ms | 122.87 ms | **139.92 ms** | 139.92 ms | 1 | PASS ✓ |
| `findings_work_visible_total_more+domain+author` | p95 ≤ 500 ms | 122.23 ms | **139.78 ms** | 139.78 ms | 1 | PASS ✓ |
| `findings_work_visible_total_more+work` | p95 ≤ 500 ms | 105.81 ms | **113.38 ms** | 113.38 ms | 1 | PASS ✓ |
| `findings_work_visible_total_more+domain+work` | p95 ≤ 500 ms | 107.06 ms | **127.79 ms** | 127.79 ms | 1 | PASS ✓ |
| `findings_work_visible_total_more+author+work` | p95 ≤ 500 ms | 106.10 ms | **115.85 ms** | 115.85 ms | 1 | PASS ✓ |
| `findings_work_visible_total_more+domain+author+work` | p95 ≤ 500 ms | 111.20 ms | **118.24 ms** | 118.24 ms | 1 | PASS ✓ |
| `findings_launch_contribution_main_pool` | p95 ≤ 500 ms | 93.17 ms | **107.93 ms** | 107.93 ms | 3 | PASS ✓ |
| `findings_launch_contribution_all_bucket` | p95 ≤ 500 ms | 27.19 ms | **34.77 ms** | 34.77 ms | 3 | PASS ✓ |
| `findings_launch_manuscripts_main_pool` | p95 ≤ 500 ms | 99.84 ms | **108.41 ms** | 108.41 ms | 6755 | PASS ✓ |

Combinations NOT measured, and why:

- `findings_identification_deep_page_20_main+novelty+domain` — the identification unit carries only 45 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_main+novelty+author` — the identification unit carries only 68 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_main+novelty+domain+author` — the identification unit carries only 45 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_main+work` — the identification unit carries only 293 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_main+novelty+work` — the identification unit carries only 18 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_main+domain+work` — the identification unit carries only 293 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_main+novelty+domain+work` — the identification unit carries only 18 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_main+author+work` — the identification unit carries only 293 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_main+novelty+author+work` — the identification unit carries only 18 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_main+domain+author+work` — the identification unit carries only 293 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_main+novelty+domain+author+work` — the identification unit carries only 18 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_more+domain` — the identification unit carries only 531 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_more+novelty+domain` — the identification unit carries only 138 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_more+author` — the identification unit carries only 390 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_more+novelty+author` — the identification unit carries only 131 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_more+domain+author` — the identification unit carries only 390 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_more+novelty+domain+author` — the identification unit carries only 131 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_more+work` — the identification unit carries only 390 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_more+novelty+work` — the identification unit carries only 131 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_more+domain+work` — the identification unit carries only 390 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_more+novelty+domain+work` — the identification unit carries only 131 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_more+author+work` — the identification unit carries only 390 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_more+novelty+author+work` — the identification unit carries only 131 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_more+domain+author+work` — the identification unit carries only 390 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_identification_deep_page_20_more+novelty+domain+author+work` — the identification unit carries only 131 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_main+novelty+domain` — the manuscript unit carries only 40 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_main+novelty+author` — the manuscript unit carries only 61 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_main+novelty+domain+author` — the manuscript unit carries only 40 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_main+work` — the manuscript unit carries only 293 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_main+novelty+work` — the manuscript unit carries only 18 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_main+domain+work` — the manuscript unit carries only 293 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_main+novelty+domain+work` — the manuscript unit carries only 18 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_main+author+work` — the manuscript unit carries only 293 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_main+novelty+author+work` — the manuscript unit carries only 18 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_main+domain+author+work` — the manuscript unit carries only 293 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_main+novelty+domain+author+work` — the manuscript unit carries only 18 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_more+domain` — the manuscript unit carries only 529 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_more+novelty+domain` — the manuscript unit carries only 138 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_more+author` — the manuscript unit carries only 390 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_more+novelty+author` — the manuscript unit carries only 131 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_more+domain+author` — the manuscript unit carries only 390 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_more+novelty+domain+author` — the manuscript unit carries only 131 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_more+work` — the manuscript unit carries only 390 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_more+novelty+work` — the manuscript unit carries only 131 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_more+domain+work` — the manuscript unit carries only 390 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_more+novelty+domain+work` — the manuscript unit carries only 131 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_more+author+work` — the manuscript unit carries only 390 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_more+novelty+author+work` — the manuscript unit carries only 131 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_more+domain+author+work` — the manuscript unit carries only 390 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_manuscript_deep_page_20_more+novelty+domain+author+work` — the manuscript unit carries only 131 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_work_band_rank_main+novelty` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_band_rank_main+novelty+domain` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_band_rank_main+novelty+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_band_rank_main+novelty+domain+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_band_rank_main+novelty+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_band_rank_main+novelty+domain+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_band_rank_main+novelty+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_band_rank_main+novelty+domain+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_band_rank_more+novelty` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_band_rank_more+novelty+domain` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_band_rank_more+novelty+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_band_rank_more+novelty+domain+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_band_rank_more+novelty+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_band_rank_more+novelty+domain+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_band_rank_more+novelty+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_band_rank_more+novelty+domain+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_matched_text_main+novelty` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_matched_text_main+novelty+domain` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_matched_text_main+novelty+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_matched_text_main+novelty+domain+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_matched_text_main+novelty+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_matched_text_main+novelty+domain+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_matched_text_main+novelty+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_matched_text_main+novelty+domain+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_matched_text_more+novelty` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_matched_text_more+novelty+domain` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_matched_text_more+novelty+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_matched_text_more+novelty+domain+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_matched_text_more+novelty+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_matched_text_more+novelty+domain+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_matched_text_more+novelty+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_matched_text_more+novelty+domain+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_page_count_main+novelty` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_page_count_main+novelty+domain` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_page_count_main+novelty+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_page_count_main+novelty+domain+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_page_count_main+novelty+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_page_count_main+novelty+domain+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_page_count_main+novelty+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_page_count_main+novelty+domain+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_page_count_more+novelty` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_page_count_more+novelty+domain` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_page_count_more+novelty+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_page_count_more+novelty+domain+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_page_count_more+novelty+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_page_count_more+novelty+domain+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_page_count_more+novelty+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_page_count_more+novelty+domain+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_deep_page_20_main` — the work unit carries only 478 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_work_deep_page_20_main+novelty` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_deep_page_20_main+domain` — the work unit carries only 14 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_work_deep_page_20_main+novelty+domain` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_deep_page_20_main+author` — the work unit carries only 25 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_work_deep_page_20_main+novelty+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_deep_page_20_main+domain+author` — the work unit carries only 14 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_work_deep_page_20_main+novelty+domain+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_deep_page_20_main+work` — the work unit carries only 1 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_work_deep_page_20_main+novelty+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_deep_page_20_main+domain+work` — the work unit carries only 1 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_work_deep_page_20_main+novelty+domain+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_deep_page_20_main+author+work` — the work unit carries only 1 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_work_deep_page_20_main+novelty+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_deep_page_20_main+domain+author+work` — the work unit carries only 1 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_work_deep_page_20_main+novelty+domain+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_deep_page_20_more` — the work unit carries only 504 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_work_deep_page_20_more+novelty` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_deep_page_20_more+domain` — the work unit carries only 2 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_work_deep_page_20_more+novelty+domain` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_deep_page_20_more+author` — the work unit carries only 1 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_work_deep_page_20_more+novelty+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_deep_page_20_more+domain+author` — the work unit carries only 1 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_work_deep_page_20_more+novelty+domain+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_deep_page_20_more+work` — the work unit carries only 1 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_work_deep_page_20_more+novelty+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_deep_page_20_more+domain+work` — the work unit carries only 1 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_work_deep_page_20_more+novelty+domain+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_deep_page_20_more+author+work` — the work unit carries only 1 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_work_deep_page_20_more+novelty+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_deep_page_20_more+domain+author+work` — the work unit carries only 1 rows under that filter state -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set
- `findings_work_deep_page_20_more+novelty+domain+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_visible_total_main+novelty` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_visible_total_main+novelty+domain` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_visible_total_main+novelty+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_visible_total_main+novelty+domain+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_visible_total_main+novelty+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_visible_total_main+novelty+domain+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_visible_total_main+novelty+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_visible_total_main+novelty+domain+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_visible_total_more+novelty` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_visible_total_more+novelty+domain` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_visible_total_more+novelty+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_visible_total_more+novelty+domain+author` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_visible_total_more+novelty+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_visible_total_more+novelty+domain+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_visible_total_more+novelty+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_visible_total_more+novelty+domain+author+work` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope

Combinations the SURFACE cannot issue, named rather than omitted:

- `findings_coverage_filter` — the findings service exposes NO coverage predicate -- `get_findings_enveloped` takes unit/bucket/novelty/domain/author/work_id/sort/page only, and the page renders the coverage control visibly disabled and tagged for exactly that reason. Measuring a coverage filter here would time a query the surface cannot issue
- `findings_relation_filter` — D-16 was ratified 2026-08-02: the findings page ships WITHOUT a relation filter, and `_build_findings_query` carries no relation predicate. The pre-136-14 probe measured one against hand-written SQL that mirrored a surface which does not exist
## 5. Amendment 2026-08-02 (Phase 136) — Corpus-wide findings page (Computed Identifications)

The Phase-136 corpus-wide findings page ("Computed Identifications" / "זיהויים מחושבים" nav entry)
gets its own versioned budget entry, per D-10a and the `main-pool-rule.md` PERF-01 confirmation. This
section is ADDITIVE — no pre-existing cap in §1-§4 above is changed.

| Metric | Cap |
|---|---|
| Rows per page | ≤ **200** (default **50**, matching the existing Work/Leads §1.2 defaults) |
| Response size | ≤ **500 KB** |
| Server response time | p95 ≤ **1.5 s** |
| Per-request timeout | ≤ **5 s** |
| Visible total count — a SEPARATE cap from the row fetch above | p95 ≤ **0.5 s** |

**Why the visible-count cap is separate.** Two measured hazards motivate splitting it out from the
row-fetch p95: a representative novelty/quality/coverage ordering measured **3.41–3.55 s** across four
runs against the 1.5 s row-fetch cap (D-10a; the count alone in that same measurement took 0.50-0.55 s),
and — separately, and far more severe — a deduped identification COUNT alone measured **16 s**
(`main-pool-rule.md` finding 13, "PERF-01 confirmed twice"). The count cap above is met by reading a
MATERIALIZED identification-grain table (`discovery_identification`, one row per `(sys_id,
canonical_work_id)`), never by counting claim rows at request time. An approximate or cached count is
acceptable only if the surface says so in words (e.g. "approximately N results") — a silent
approximation presented as exact is not acceptable.

**Build-time (offline) section — two PENDING slots.** Both are marked `PENDING — measure at the
rebuild, then version`; no number is invented here, and no committed doc records these runtimes yet:

| Metric | Status |
|---|---|
| Rebuild-preservation full-table diff runtime (D-02b) | PENDING — measure at the rebuild, then version |
| Public-projection masking scan runtime (VIS-02) | PENDING — measure at the rebuild, then version |

**New env vars** (following the existing `DISCOVERY_*` convention in §3 above):

```
DISCOVERY_FINDINGS_PAGE_SIZE_DEFAULT=50   # rows/page default for the findings page; the existing
                                           # DISCOVERY_PAGE_SIZE_MAX=200 ceiling is unchanged and shared
DISCOVERY_QUERY_TIMEOUT_FINDINGS=5.0      # seconds, per-request timeout for the findings-page query
DISCOVERY_FINDINGS_COUNT_MAX=0            # 0 (default) = OFF: report the exact total. Above 0, the
                                           # findings COUNT stops at the cap and the envelope reports
                                           # total=cap with meta.approximate_total=True. Findings-page
                                           # ONLY -- the work-expansion total is exact by contract and
                                           # has no equivalent escape (see below).
```

**`DISCOVERY_FINDINGS_COUNT_MAX` is not a general approximation switch, and must not become one.**
It caps the counting query on the corpus-wide findings page, where a reader is paging a large,
filtered list and a bounded count is an honest trade. Two boundaries hold it there:

- It is **off by default** (`0`), so an exact total is what ships unless someone opts out.
- When it fires, the envelope must carry `meta.approximate_total=True`. A capped total reported as
  exact is a correctness defect, not a performance tuning choice.

The **work-expansion** total on the connections panel is deliberately exact and has **no** cap knob.
An earlier design gave it an approximate-total escape; that was withdrawn during Phase 136 pre-flight
because it was a hole underneath a contract that promises a real count — the honest degradation there
is `timeout`, not a silently truncated number. Do not add a `*_COUNT_MAX` for that path.

No pre-existing numeric cap in §1-§4 above is loosened, tightened, or otherwise modified by this
amendment.

---

*Phase: 134-Discovery Data Spine (plan 134-02)*
*Exit artifact for PERF-01 (initial caps); DATA-06 default values ratified
for 134-06 to consume.*

### 5.1 MEASURED ACTUALS (prod-box) — recorded 2026-08-03, Phase 136 plan 136-13

Measured on the production box immediately after the flag-OFF deploy of the PUBLIC projection
(`discovery-v1-e9365edc…`, 53,581 identifications), via
`python scripts/bench_discovery.py --sample 50 --warm-passes 1`. The benchmark uses the
readiness predicate (`_state.ready`), NOT the UI flag, so this proves the swapped-in sidecar loaded
and returns real rows while `DISCOVERY_ENABLED` stays off.

| shape | p50 ms | p95 ms | cap ms | result |
|---|---|---|---|---|
| `findings_default_ordering` | 113.47 | **124.22** | 1500 | PASS |
| `findings_novelty_filter` | 20.14 | 20.78 | 1500 | PASS |
| `findings_relation_filter` | 38.39 | 40.39 | 1500 | PASS |
| `findings_domain_filter` | 90.15 | 104.15 | 1500 | PASS |
| `findings_visible_total` | 0.02 | 0.46 | 500 | PASS |
| `findings_deep_page_20` | 96.87 | 106.92 | 1500 | PASS |

Other families: `get_claims_for_page` p95 0.60 ms · `get_pages_related_to_page` p95 0.44 ms ·
`get_work_witnesses` p95 363.14 ms (informational, under the 1.5 s work-page request cap) ·
browse-enrichment p95 **0.60 ms** (cap ≤ 150 ms).

**Memory: added RSS 15.3 MB against the ≤ 250 MB cap** (22.8 MB before load → 38.1 MB after burst).
Warm-burst returned 2,626 rows — the nonzero-result sanity check.

The prod box is FASTER than the dev box on every findings shape (124 ms vs 159 ms p95 on the default
ordering). D-10a's motivating shape was 3.41–3.55 s; it now runs with ~12× headroom under its cap in
production.
