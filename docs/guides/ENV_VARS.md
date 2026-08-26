# Environment Variables Reference

> **This is the single source of truth for every environment variable in
> GenizahSearch.** It was lifted out of `CLAUDE.md` on 2026-08-26: that file is
> read into every session, and this block alone was 41% of it. A partial copy
> also lived in `DEVELOPER_GUIDE.md` and had already drifted from it, so both
> now point here instead.
>
> `CLAUDE.md` keeps only the flags that decide whether a surface is reachable
> at all, because those change what an agent does. Everything else -- tuning
> knobs, timeouts, budgets, resilience settings -- is here.

---

## Environment Variables

```
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_ANON_KEY=eyJ...
POSTHOG_API_KEY=phc_xxxxx (optional - enables PostHog analytics)
WEB_PUZZLE_ENABLED=true (default: true)
ATLAS_PREVIEW_ENABLED=false   # web-only (Phase 133, ATLAS-01). Default OFF — gates the Visual Atlas Preview beta: the /atlas page, its /atlas-data/* routes, and the nav link. The flag is necessary but NOT sufficient: web/atlas_assets.py::atlas_preview_available() ANDs it with the baked-asset readiness (manifest + plain .bin loaded at startup from repo-root atlas_data/, OUTSIDE web/static/), so a flag-ON/asset-missing window still hides cleanly. Set to 1/true to enable in the beta env.
MASKING_SCAN_PATTERNS_FILE=/path/to/.masking_patterns   # dev/CI-only (Phase 133+). Points scripts/check_atlas_masking.py at a gitignored, newline-delimited restricted-string ("M-source") pattern file (never committed in cleartext; same secret-handling posture as .env). Unset/empty => the masking scan fails safe (exit 1, never a silent green). Forerunner of the permanent DATA-05 CI guard (Phase 134).
DISCOVERY_ENABLED=false   # web-only (Phase 134, DATA-07). Default OFF at the code level, but **set to 1/true in production since 2026-08-08** — /computed-identifications and the browse-page connections panel are publicly live there (confirmed by commit 04434714; see CHANGELOG.md [Unreleased]). Local/dev checkouts stay OFF unless set explicitly. The flag is necessary but NOT sufficient: web/discovery_assets.py::discovery_available() ANDs it with the discovery.db sidecar's startup-loaded readiness (versioned, fail-closed: exact manifest asset_basename, content-hash match, PRAGMA integrity_check, schema_version reject-incompatible, release-contract row counts, required meta/tables, frozen enum vocab) — a flag-ON/sidecar-missing-or-corrupt window still hides cleanly. Set to 1/true once Phase 135+ ships a surface to gate.
DISCOVERY_FINDINGS_COUNT_MAX=0   # web-only (Phase 136). Default 0 = OFF: the corpus-wide findings page reports its EXACT total. Above 0, the counting query stops at the cap and the envelope reports total=cap with meta.approximate_total=True — a capped total reported as exact is a correctness defect, not a tuning choice. Findings page ONLY: the connections-panel work-expansion total is exact by contract and deliberately has NO equivalent knob (its approximate-total escape was withdrawn in Phase 136 pre-flight — the honest degradation there is `timeout`, not a truncated number). Read live in shared/discovery_service.py; full rationale in docs/specs/discovery-budgets.md.
DISCOVERY_MAX_CONCURRENT_BROWSE_QUERIES=24   # web-only (Phase 136). The BROWSE-path bounded-concurrency budget, separate from the heavy one (DISCOVERY_MAX_CONCURRENT_QUERIES=4). The two numbers MUST differ (a cold panel load issues 3 reads concurrently plus a fourth, so the heavy cap of 4 would put the second simultaneous visitor into `busy`), and each budget has its OWN ThreadPoolExecutor sized to it -- two semaphores over one pool are two names for one budget. Full rationale, including why the heavy semaphore never applied to the browse path at all: docs/specs/discovery-budgets.md SS2/SS3.
DISCOVERY_MAX_CONCURRENT_EXPORT_QUERIES=2   # web-only (Phase 136.2). The THIRD concurrency budget, for the findings xlsx export, with its OWN ThreadPoolExecutor sized to it (two semaphores over one pool are two names for one budget). Separate from the heavy cap of 4 because an export holds its slot for a whole-corpus walk, not one query: measured 28,635 rows in 52.9 s on the default view. The export is UNCAPPED IN ROWS by owner decision (2026-08-20) and bounded on every other axis instead.
DISCOVERY_EXPORT_TIMEOUT=300.0   # web-only (Phase 136.2). Whole-build timeout for the export walk. Its honest failure is a 504, NEVER a short file: a truncated workbook is indistinguishable from a small result set once downloaded.
DISCOVERY_EXPORT_EXCERPT_CHUNK=500   # web-only (Phase 136.2). Ids per `IN (...)` batch in the export's excerpt read, replacing the per-identification read (1 query/row would be 28,635 serialized SQLite round trips on a single-uvicorn-worker box). Same fix shape as the citation-range P1 (4f6e31f4, 10,478 ms -> 97 ms).
GENIZAH_PUBLIC_BASE_URL=https://genizahsearch.com   # web-only (Phase 136.2). The canonical origin baked into links inside an exported workbook. Deliberately NOT `request.base_url`: that derives from the client-controlled Host header, so a crafted request would put attacker-chosen origins into every link of a file that otherwise looks like ours and carries our provenance sheet — and a downloaded file outlives the request that made it. A non-http(s) value logs a warning and falls back to RELATIVE links (less convenient, never wrong).
GENIZAH_DISCOVERY_DATA_DIR=/path/to/dir   # dev/CI-only (Phase 136). Overrides the directory web/discovery_assets.py reads the discovery sidecar + manifest.json from (default: repo-root discovery_data/). Read ONCE at import, never per request. Exists because discovery_data/ is gitignored, so the CI `findings-browser-check` job has no sidecar and the findings page would clean-hide: the job materializes the SYNTHETIC fixture sidecar into a temp dir (scripts/ci_materialize_discovery_fixture.py) and points this at it. Widens no trust boundary — it selects WHICH directory is read; the loader still applies its full fail-closed contract (exact manifest asset_basename, content hash, PRAGMA integrity_check, schema_version, `public` audience gate, required tables/columns/meta keys, release-contract row counts, frozen enum vocab). Deliberately NOT used to repoint the repo's real discovery_data/manifest.json, which tests/test_cert01_grading_validator.py resolves the real artifact through. Leave unset in production.
DISCOVERY_FINDINGS_PAGE_SIZE_DEFAULT=50   # web-only (Phase 136). Default page size for /computed-identifications, clamped below the page-size max. Read in web/pages/findings.py + shared/discovery_service.py.
IDENTIFICATION_REVIEWS_ENABLED=true   # web-only (2026-08-13, beta). Default ON — gates the community identification-reviews feature (a reader submits a relation verdict on a computed identification; admins moderate before publication). Writes go only through Supabase SECURITY DEFINER RPCs, never a direct table write. Read live in web/identification_reviews.py::reviews_enabled(). Kill-switch: 0/false/no/off. NOT yet a GSD-planned phase — see docs/OPEN_ISSUES.md and the v9.0.0 roadmap note on Phase 137.
MASKING_ATTESTATION_KEY=xxx   # dev/CI-only (discovery-v3 track). HMAC key for the masking scan's keyed pattern-set attestation (scripts/check_atlas_masking.py); the attestation is omitted from output when unset.
V3_REVIEW_M_DIR=/path/to/dir  /  V3_REVIEW_JA_DIR=/path/to/dir   # dev/owner-only (discovery-v3 review tooling). Feed scripts/build_v3_review_db.py + scripts/serve_v3_review.py, the owner's private LOCAL grading server. Never deployed to genizahsearch.com.
DISCOVERY_BROWSE_LRU_MAX_ENTRIES=...   # dev/bench-only. Read by scripts/ benchmarking, not by the web app.
FGP_TRANSCRIPTIONS_ENABLED=true   # shared (both apps): show FGP transcriptions as a distinct, selectable source in the version chooser. Default ON (2026-06-22 go-live) — surfaces wherever the gitignored fgp_data/fgp_transcriptions.db is present; graceful no-op when the DB is absent. Kill-switch: set to 0/false/no/off. Read live in shared/fgp_service.py.
WEB_FGP_ENABLED=...               # optional web-only override of the above (web/feature_flags.py::web_fgp_enabled); defaults to FGP_TRANSCRIPTIONS_ENABLED (ON). Disable on web only with WEB_FGP_ENABLED=0.
PASSAGE_MULTI_WITNESS_ENABLED=false / SEARCH_API_PASSAGE_MAX_WITNESSES=25   # web-only. Multi-witness passage search; ANDed with passage_available(). NEVER concatenate witnesses here -- the posting budget starves (48% vs 74% fused, and worse than ONE witness). Rejected for method='chunk', where union and concatenation measured identical. Contract: docs/SEARCH_API.md.
PASSAGE_PARALLELS_ENABLED=false   # web-only (Phase 145). Gates method='passage' on /api/parallels + the parallels-page method selector. Necessary but NOT sufficient: web/passage_assets.py::passage_available() ANDs it with the passage index's startup-loaded readiness (shared/passage_index.py::open_index is itself fail-closed: manifest, layout/normalizer version, bit budgets, byte order, CSR sanity, declared-vs-actual file sizes). Flag-ON + index-missing/corrupt hides cleanly.
GENIZAH_PASSAGE_DATA_DIR=/path/to/dir   # dev/CI-only. Overrides the dir web/passage_assets.py opens the passage index from (default: repo-root passage_index/current/, gitignored, multi-GB, machine-local). Read ONCE at import.
PUZZLE_UPLOAD_SECRET=xxx (optional - HMAC secret for puzzle upload tokens; auto-generated if unset)
POSTHOG_IP_SALT=xxx (optional - HMAC salt for hashing client IPs; auto-generated if unset, production should set explicitly so hashes survive restarts)

# Search API (Phase 77-83 public HTTP/JSON API over the corpus)
SEARCH_API_MODE=open                  # open | localhost-only | disabled (flippable per request, no restart)
SEARCH_API_RATE_LIMIT=120             # per-IP requests/minute; shared ceiling but each endpoint has its own bucket (raised 30->120 in 2026-06 for API research)
SEARCH_API_POSTHOG_SAMPLE_N=1         # capture every Nth API request to PostHog
SEARCH_API_BROWSE_TIMEOUT=1.0         # per-source enrichment timeout (PGP/FJMS/NLI), seconds
SEARCH_API_BROWSE_CORE_TIMEOUT=2.0    # core BrowsePage fetch timeout, seconds
SEARCH_API_CORE_TIMEOUT=30.0          # interactive baseline (exact/title/shelfmark/responsa); runs in executor off the event loop -> 504 core_timeout
SEARCH_API_VARIANTS_TIMEOUT=60        # /api/search variants-mode core timeout (s); heavy tier
SEARCH_API_FUZZY_TIMEOUT=300          # /api/search fuzzy-mode core timeout (s); heaviest mode
SEARCH_API_PARALLELS_TIMEOUT=300      # /api/parallels composition core timeout (s)
SEARCH_API_HEAVY_CONCURRENCY=2        # max concurrent heavy (variants/fuzzy/parallels method=chunk) requests; over -> 503 heavy_search_busy + Retry-After
SEARCH_API_PASSAGE_TIMEOUT=30         # /api/parallels method='passage' core timeout (s); own ceiling, unrelated to SEARCH_API_PARALLELS_TIMEOUT (Phase 145)
SEARCH_API_PASSAGE_CONCURRENCY=4      # max concurrent method='passage' requests; its OWN semaphore + ThreadPoolExecutor(max_workers=4), never the default executor -> 503 passage_search_busy + Retry-After
SEARCH_API_FUZZY_MAX_LIMIT=500        # fuzzy result-cap ceiling (recall over precision; non-fuzzy stays 100)
SEARCH_API_BROWSE_TEXT_CAP=4000       # default char cap for transcription text; ?text_cap=N override bounded [100, 10000]

# Skill-side (cairo-genizah-research skill consumer)
GENIZAH_API_BASE=https://genizahsearch.com    # overrides --base-url CLI flag (env wins)
GENIZAH_SKILL_REQ_PER_MIN=96                  # skill self-throttle, leaves 24 rpm headroom under server's 120 rpm

# Web memory remediation (2026-07-08 allocator-ratchet attribution; web-only)
GENIZAH_MALLOC_TRIM_SECONDS=300       # periodic glibc malloc_trim(0) loop interval (0 disables; Linux-only, web/malloc_trim.py)
GENIZAH_MALLOC_TRIM_MIN_GROWTH_MB=64  # adaptive: trim only when RssAnon grew this much since the last trim (0 = every tick)
GENIZAH_STORAGE_RETENTION_DAYS=90     # delete .nicegui storage-user files untouched for N days at startup (0 disables;
                                       # a browser absent longer is logged out on return — auth_session lives in the file)
NLI_CACHE_MAX_ENTRIES=20000           # metadata nli_cache LRU bound (code default 75000; prod set 20000 on 2026-07-08)
IIIF_MANIFEST_CACHE_MAX_ENTRIES=1500  # IIIF manifest cache bound (code default 5000; prod set 1500 on 2026-07-08)

# Perf watch (2026-07-30 slowness diagnosis; web-only, web/perf_watch.py). Default ON and
# deliberately quiet — nothing is logged while the app behaves. Added because a 9-second
# response previously left NO server-side trace: nginx uses the default `combined` log format
# (no $request_time/$upstream_response_time) and the only in-app timing was /lists-scoped and
# flag-gated, so origin latency was invisible and had to be inferred from outside.
GENIZAH_PERF_WATCH=1                  # 0/false disables BOTH signals below
GENIZAH_SLOW_REQUEST_MS=1500          # log any http request slower than this (all paths, incl. static)
GENIZAH_LOOP_LAG_MS=300               # log event-loop stalls above this — THE decisive signal: uvicorn
                                       # runs ONE worker, so sync Supabase/NLI I/O on the loop stalls every
                                       # concurrent request incl. static files, while burning no CPU (so it
                                       # is invisible in load average — prod read 0.03 during multi-second TTFBs)
GENIZAH_LOOP_LAG_INTERVAL=1.0         # lag monitor tick, seconds (floor 0.1)
GENIZAH_NOT_SCHEDULED_MS=60000        # (2026-08-19) above this, a tick that burned almost no CPU is
                                       # reported as "monitor NOT SCHEDULED" and kept OUT of max_lag_ms:
                                       # the process stopped running (laptop asleep, container throttled,
                                       # Windows console paused by a QuickEdit selection), it was not
                                       # blocked. A 3,069,031 ms "event loop BLOCKED" reading was a
                                       # sleeping laptop, and it poisoned the all-time maximum that every
                                       # other perf line quotes. Each real stall now also names its KIND
                                       # from the same CPU measurement — GIL-bound Python (a run.io_bound
                                       # worker counts) vs blocking I/O on the loop.
GENIZAH_PERF_SUMMARY_SECONDS=300      # periodic counter summary; 0 disables

# Phase 98 NLI Resilience env knobs (added 2026-05-25)
NLI_CIRCUIT_THRESHOLD=3               # Consecutive failures to trip the shared circuit breaker
NLI_CIRCUIT_WINDOW=60                 # Seconds the breaker stays open before auto-recovery probes
NLI_CONNECT_TIMEOUT=3                 # Connection timeout (seconds) for all NLI/IIIF/Rosetta fetches
NLI_IIIF_READ_TIMEOUT=5               # Read timeout (seconds) for IIIF manifest JSON fetches
NLI_MARC_READ_TIMEOUT=3               # Read timeout (seconds) for MARC bib XML fetches
NLI_IMAGE_READ_TIMEOUT=5              # Read timeout (seconds) for NLI image-bytes fetches

# Existing NLI knob — Phase 98 changed default from 20 -> 1:
NLI_SEMAPHORE_TIMEOUT=1               # Max seconds to wait for a slot in the 8-slot NLI semaphore
                                       # (was 20 pre-Phase-98; waiting >1s burns threadpool workers)

# (Other existing NLI knobs unchanged: NLI_CACHE_TTL, NLI_FAIL_CACHE_TTL, NLI_MAX_CONCURRENT_FETCHES)
```

**Operational note (Phase 98 — two PostHog drop counters):** Phase 98 ships with TWO PostHog
queues — `web/api_hardening.py` keeps its existing queue (for `search_api_request` events) and
the new `shared/posthog_server.py` queue handles breaker telemetry (`nli_breaker_opened` /
`nli_breaker_closed`). At deploy time, monitor BOTH `web.api_hardening.get_dropped_event_count()`
AND `shared.posthog_server.get_dropped_event_count()` — growth in EITHER signals queue saturation.
The two-queue split is intentional (REVIEWS Issue 5 Option A): refactoring `web/api_hardening`'s
queue would break 5 existing test monkeypatches that target `web.api_hardening._event_queue`
directly. A future cleanup plan can unify.
