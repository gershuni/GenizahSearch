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


Measured by `scripts/bench_discovery.py::bench_findings_page()` over 53581 materialized identifications (`discovery_identification`), page size 50, deep page 20. **45 combinations enumerated, 41 measured.** Every combination asserted a NONZERO row count before its timing was recorded.

**Artifact, audience and host** — a timing without its artifact is not comparable to the next one, because the public projection and the private rebuild are different databases with different row counts that report the identical `sidecar_version` string; and a laptop measurement is not a server measurement, which is where a slow query does its damage on a single-worker box:

- artifact: `discovery-v1-e9365edcab27af7d0739ab1a07b1a187683993bcbff41ff88128c8fe4fbb7181.db` (375.5 MB)
- audience: `public` · sidecar_version: `discovery-v1-real` · data_as_of: `2026-08-03`
- host: `Windows AMD64` (dev-box)

Every combination is built through the SHIPPED `shared/discovery_service.py::_build_findings_query` — the exact builder `get_findings_enveloped` calls — so the probe and the service can no longer diverge (136-14's owed follow-up, closed). The launch-statistics rows come from `_build_launch_contribution_sql` / `_build_launch_manuscript_sql` for the same reason.

The ordering and the visible-count numbers are recorded SEPARATELY because §5 gives them separate caps. The prior, PRE-materialization measurement was 3.41-3.55 s across four runs (D-10a), against the 1.5 s cap, when the same ordering was computed over display CLAIMS with no materialized band_rank / coverage_ppm and no identification grain, and 16 s for the deduped identification COUNT alone (main-pool-rule.md finding 13, "PERF-01 confirmed twice").

| Combination | Cap | p50 | p95 | max | Rows | Result |
|---|---|---|---|---|---|---|
| `findings_identification_band_rank_main` | p95 ≤ 1500 ms | 272.75 ms | **290.32 ms** | 290.32 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_more` | p95 ≤ 1500 ms | 328.69 ms | **334.06 ms** | 334.06 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_novelty` | p95 ≤ 1500 ms | 94.50 ms | **105.21 ms** | 105.21 ms | 50 | PASS ✓ |
| `findings_identification_band_rank_domain` | p95 ≤ 1500 ms | 216.34 ms | **233.24 ms** | 233.24 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_main` | p95 ≤ 1500 ms | 273.15 ms | **282.62 ms** | 282.62 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_more` | p95 ≤ 1500 ms | 312.05 ms | **340.91 ms** | 340.91 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_novelty` | p95 ≤ 1500 ms | 95.99 ms | **124.05 ms** | 124.05 ms | 50 | PASS ✓ |
| `findings_identification_matched_text_domain` | p95 ≤ 1500 ms | 224.93 ms | **234.69 ms** | 234.69 ms | 50 | PASS ✓ |
| `findings_identification_page_count_main` | p95 ≤ 1500 ms | 241.98 ms | **262.16 ms** | 262.16 ms | 50 | PASS ✓ |
| `findings_identification_page_count_more` | p95 ≤ 1500 ms | 287.30 ms | **294.29 ms** | 294.29 ms | 50 | PASS ✓ |
| `findings_identification_page_count_novelty` | p95 ≤ 1500 ms | 78.28 ms | **83.26 ms** | 83.26 ms | 50 | PASS ✓ |
| `findings_identification_page_count_domain` | p95 ≤ 1500 ms | 190.44 ms | **202.74 ms** | 202.74 ms | 50 | PASS ✓ |
| `findings_identification_deep_page_20` | p95 ≤ 1500 ms | 270.56 ms | **285.69 ms** | 285.69 ms | 50 | PASS ✓ |
| `findings_identification_visible_total` | p95 ≤ 500 ms | 89.13 ms | **103.55 ms** | 103.55 ms | 1 | PASS ✓ |
| `findings_manuscript_band_rank_main` | p95 ≤ 1500 ms | 251.36 ms | **276.76 ms** | 276.76 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_more` | p95 ≤ 1500 ms | 265.11 ms | **275.69 ms** | 275.69 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_novelty` | p95 ≤ 1500 ms | 85.53 ms | **90.99 ms** | 90.99 ms | 50 | PASS ✓ |
| `findings_manuscript_band_rank_domain` | p95 ≤ 1500 ms | 179.49 ms | **187.23 ms** | 187.23 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_main` | p95 ≤ 1500 ms | 273.14 ms | **306.05 ms** | 306.05 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_more` | p95 ≤ 1500 ms | 284.75 ms | **314.43 ms** | 314.43 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_novelty` | p95 ≤ 1500 ms | 90.84 ms | **103.26 ms** | 103.26 ms | 50 | PASS ✓ |
| `findings_manuscript_matched_text_domain` | p95 ≤ 1500 ms | 199.56 ms | **201.81 ms** | 201.81 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_main` | p95 ≤ 1500 ms | 277.86 ms | **283.21 ms** | 283.21 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_more` | p95 ≤ 1500 ms | 296.22 ms | **318.29 ms** | 318.29 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_novelty` | p95 ≤ 1500 ms | 101.97 ms | **107.19 ms** | 107.19 ms | 50 | PASS ✓ |
| `findings_manuscript_page_count_domain` | p95 ≤ 1500 ms | 195.61 ms | **208.00 ms** | 208.00 ms | 50 | PASS ✓ |
| `findings_manuscript_deep_page_20` | p95 ≤ 1500 ms | 263.67 ms | **289.98 ms** | 289.98 ms | 50 | PASS ✓ |
| `findings_manuscript_visible_total` | p95 ≤ 500 ms | 113.94 ms | **129.40 ms** | 129.40 ms | 1 | PASS ✓ |
| `findings_work_band_rank_main` | p95 ≤ 1500 ms | 158.30 ms | **166.80 ms** | 166.80 ms | 50 | PASS ✓ |
| `findings_work_band_rank_more` | p95 ≤ 1500 ms | 188.39 ms | **190.00 ms** | 190.00 ms | 50 | PASS ✓ |
| `findings_work_band_rank_domain` | p95 ≤ 1500 ms | 130.66 ms | **135.19 ms** | 135.19 ms | 39 | PASS ✓ |
| `findings_work_matched_text_main` | p95 ≤ 1500 ms | 139.93 ms | **148.17 ms** | 148.17 ms | 50 | PASS ✓ |
| `findings_work_matched_text_more` | p95 ≤ 1500 ms | 159.52 ms | **178.51 ms** | 178.51 ms | 50 | PASS ✓ |
| `findings_work_matched_text_domain` | p95 ≤ 1500 ms | 112.70 ms | **126.48 ms** | 126.48 ms | 39 | PASS ✓ |
| `findings_work_page_count_main` | p95 ≤ 1500 ms | 142.61 ms | **149.46 ms** | 149.46 ms | 50 | PASS ✓ |
| `findings_work_page_count_more` | p95 ≤ 1500 ms | 172.14 ms | **175.79 ms** | 175.79 ms | 50 | PASS ✓ |
| `findings_work_page_count_domain` | p95 ≤ 1500 ms | 121.96 ms | **130.01 ms** | 130.01 ms | 39 | PASS ✓ |
| `findings_work_visible_total` | p95 ≤ 500 ms | 103.52 ms | **113.85 ms** | 113.85 ms | 1 | PASS ✓ |
| `findings_launch_contribution_main_pool` | p95 ≤ 500 ms | 63.59 ms | **67.28 ms** | 67.28 ms | 3 | PASS ✓ |
| `findings_launch_contribution_all_bucket` | p95 ≤ 500 ms | 18.71 ms | **20.08 ms** | 20.08 ms | 3 | PASS ✓ |
| `findings_launch_manuscripts_main_pool` | p95 ≤ 500 ms | 63.55 ms | **72.75 ms** | 72.75 ms | 1 | PASS ✓ |

Combinations NOT measured, and why:

- `findings_work_band_rank_novelty` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_matched_text_novelty` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_page_count_novelty` — novelty is not offered on the per-work unit -- a work spanning many manuscripts has no single verdict, and the service RAISES rather than returning an envelope
- `findings_work_deep_page_20` — the work unit carries only 478 rows in the main pool -- fewer than the page-20 offset, so deep paging cannot be measured on a nonzero result set

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
