# Discovery Acceptance Budgets v1 (PERF-01)

**Status:** ACTIVE. Version 1, created 2026-07-22 (Phase 134, plan 134-02).

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
with INITIAL caps only — see the "MEASURED ACTUALS" section at the bottom.

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
| Bounded concurrency (max simultaneous DiscoveryService queries in flight) | **4** | Mirrors the existing `SEARCH_API_HEAVY_CONCURRENCY=2` overload-cap pattern, widened slightly because Discovery queries are lighter (single indexed SQLite lookups against a ≤300 MB read-only sidecar, not full-corpus fuzzy/variants search); a 5th+ concurrent caller gets the overload response below rather than queueing indefinitely |
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
DISCOVERY_MAX_CONCURRENT_QUERIES=4
DISCOVERY_BROWSE_LRU_MAX_ENTRIES=5000
DISCOVERY_PAGE_SIZE_DEFAULT=50
DISCOVERY_PAGE_SIZE_MAX=200              # hard ceiling; never overridable above this
```

None of these env vars exist in code yet -- this section only fixes the
NAMES and DEFAULTS 134-06 must implement against, so this artifact and the
DiscoveryService code stay in lockstep from the first line of that plan.

## 4. Measured Actuals — dev-box measured (prod-box PENDING)

Measured 2026-07-23 by `scripts/bench_discovery.py` over the real sidecar
`discovery-v1-8e43451300429ed4ace5e29e5513359a29674ac49731d5c969eb1d607e0ca065.db` (368.5 MB on disk), through the
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
| Browse-enrichment added latency (p95) | ≤ 150 ms | **0.57 ms** | max of the two browse reads below; cache OFF (worst case) |
| &nbsp;&nbsp;• `get_claims_for_page` (p95 / max) | — | 0.57 / 6.99 ms | 200 distinct pages, 326 rows total |
| &nbsp;&nbsp;• `get_pages_related_to_page` (p95 / max) | — | 0.54 / 0.95 ms | 200 distinct pages, 425 rows total |
| `get_work_witnesses` query (p95 / max) | (request cap ≤ 1.5 s) | 117.21 / 478.95 ms | 200 works incl. the heaviest; 9930 unit rows total |
| **Additional RSS (dev-box, sidecar+service+LRU warm)** | ≤ 250 MB | **11.1 MB** | dev-box indicative; prod-box authoritative (Task 3) |

**Executed-query counts (nonzero-result assertion passed for all):**
`get_claims_for_page` = 200 queries / 326 rows;
`get_pages_related_to_page` = 200 queries / 425 rows;
`get_work_witnesses` = 200 queries / 9930 unit rows.

### 4.2 Prod-box + later-surface caps — PENDING

- **Additional RSS on the prod box (vs the ≤ 250 MB cap)** — PENDING: the
  authoritative measurement is the 134-08 Task 3 human/live-server step (owner
  runs `bench_discovery.py` / samples the web process RSS around restart on the
  web box); recorded here as **MEASURED ACTUALS (prod-box)** after that run.
- **Work/Leads request-time p95 / response size (§1.2)** — PENDING until Phase
  136 ships the `/work/{id}` + `/leads` surfaces (the query-latency figures
  above are the DB-side cost only; the full request-time budget is measured
  when the surface exists).
- **Atlas drill-down p95 / node-edge counts / response size (§1.3)** — PENDING
  until Phase 139 ships the bounded explorer (ATLAS-02).
- Any §2 default that measurement shows is mis-set would require a version bump
  per the "tunable only by versioning" rule above.

These later-surface caps and defaults exist now so those plans have a stable
contract to implement against, not because they are measured yet.

---

*Phase: 134-Discovery Data Spine (plan 134-02)*
*Exit artifact for PERF-01 (initial caps); DATA-06 default values ratified
for 134-06 to consume.*
