# 134-08 SUMMARY — PERF-01 benchmark + DATA-08 deploy runbook

**Plan:** 134-08 (wave 5, `autonomous: false`, LAST plan in Phase 134)
**Requirements:** PERF-01, DATA-08
**Status:** Tasks 1–2 COMPLETE + gated; **Task 3 = blocking human/live-server gate (PENDING owner)**
**Date:** 2026-07-23

## What was built

### Task 1 — `scripts/bench_discovery.py` + measured actuals (COMPLETE)

A PERF-01 benchmark probe that measures, over the REAL 368.5 MB sidecar
(`discovery-v1-8e43451300429ed4ace5e29e5513359a29674ac49731d5c969eb1d607e0ca065.db`,
resolved by the EXACT manifest `asset_basename` — no `*` glob, N6) *through* the
`shared.discovery_service.DiscoveryService` async chokepoint:

- **Flag bypass (F14):** injects a benchmark-only availability predicate
  `lambda: web.discovery_assets._state.ready` — loader readiness WITHOUT the
  `DISCOVERY_ENABLED` UI flag (which stays OFF this phase). The flag is never
  set/monkeypatched.
- **Nonzero-result assertion (F14):** every measured query uses a KNOWN live key
  (page_id/work_id drawn from the DB that provably has shipped rows) and asserts
  nonzero rows — the benchmark can never silently measure an empty no-op.
- **Latency isolation:** browse-path latency measured with the browse LRU
  DISABLED (`DISCOVERY_BROWSE_LRU_MAX_ENTRIES=0`, one of the service's OWN knobs)
  so every timed call is a real cache-miss DB query (worst case).
- **Portable RSS reader:** psutil → Linux `/proc` → Windows psapi WorkingSetSize
  → `resource` fallback (guards dev-box Windows vs prod Linux).

**Dev-box measured actuals** (recorded into `docs/specs/discovery-budgets.md` §4,
"MEASURED ACTUALS — dev-box measured (prod-box PENDING)"):

| Metric | Cap | Dev-box actual |
|---|---|---|
| Browse-enrichment added latency (p95) | ≤ 150 ms | **~0.6 ms** (cache OFF, worst case) |
| `get_work_witnesses` query (p95 / max) | (req cap ≤ 1.5 s) | ~116 ms / ~476 ms |
| Additional RSS (dev-box, sidecar+service+LRU warm) | ≤ 250 MB | **~11 MB** |

All comfortably within caps. (RSS is low because the RO SQLite connection pages
in on demand — the 368 MB file never fully resides.) Later-surface caps
(work/leads request-time, atlas drill-down) explicitly marked PENDING (no UI
this phase); prod-box RSS marked PENDING for Task 3.

### Task 2 — `docs/specs/discovery-deploy.md` (COMPLETE)

The DATA-08 asset-first deploy/rollback/rebuild runbook:

- **Deploy:** build → dev-box verify (`--expected-frame-hash`) + strict masking
  gate → scp `.db` asset-first (temp → immutable content-hashed name while the
  live manifest still points at the prior asset) → STAGE `manifest.json.candidate`
  → verify + strict masking gate the STAGED target (filename resolved from the
  CANDIDATE manifest, never the live/old one) → **ATOMIC** `mv` live-manifest swap
  (prior kept as `manifest.prev.json`) → restart → flag-bypassing live smoke
  (`bench_discovery.py`; `DISCOVERY_ENABLED` stays OFF, no UI leaks).
- **Rollback:** single atomic `mv manifest.prev.json manifest.json` + restart —
  safe because the loader ignores any sibling that isn't the manifest's exact
  `asset_basename` (old asset left inert on disk).
- **Rebuild:** the reproducible `build_discovery_sidecar.py` invocation (positional
  `fullcorpus_v2.db` + `--from-approved` + `--crosswalk` + `--research-data-dir`
  + `--libraries-csv` + `--fjms-db` + `--out` + `--release` +
  `--frozen-precision-defaults` | `--precision-spec`), reusing the durable
  crosswalk for id stability; source-DB + crosswalk hashes in `meta` make it
  auditable. Masking note: `--include-masked-metadata` never ungates the M-source
  title.

### Task 3 — OWNER live prod-box RSS + asset-first deploy + rollback drill (PENDING)

Blocking human/live-server checkpoint. The executor stops here. See "Handoff".

## Gates

| Gate | Result |
|---|---|
| Task 1 automated verify (module imports; budgets has MEASURED + PENDING) | **PASS (exit 0)** |
| Task 2 automated verify (deploy doc has atomic/manifest/rollback/rebuild/stag + `--strict`) | **PASS (exit 0)** |
| `ruff check scripts/bench_discovery.py` | **CLEAN** |
| `check_atlas_masking.py --scan-repo --scan-asset docs/specs/discovery-deploy.md --strict` | **exit 0 (clean)** |
| `bench_discovery.py` real run over the 368.5 MB sidecar (flag-bypass, nonzero asserted) | **all queries nonzero; within caps** |

## Files

- `scripts/bench_discovery.py` (new)
- `docs/specs/discovery-budgets.md` (§4 finalized with dev-box actuals; prod-box + later-surface PENDING)
- `docs/specs/discovery-deploy.md` (new)

## Handoff — Task 3 (owner, live server)

Run `docs/specs/discovery-deploy.md` §2 on the web box: scp the sidecar
asset-first, stage the candidate manifest, verify + strict-masking-gate the
staged target on the box, atomically swap the live manifest, restart, run the
flag-bypassing smoke (`python scripts/bench_discovery.py --sample 50
--warm-passes 1`) with `DISCOVERY_ENABLED` OFF. Record the prod-box added RSS
(≤ 250 MB) into `docs/specs/discovery-budgets.md` §4 as MEASURED ACTUALS
(prod-box). Then run the rollback drill (§3) and re-point forward.

**Resume signal:** reply "approved" with the prod-box added-RSS number and
rollback-drill confirmation, or list issues. PERF-01 SC and the DATA-08 live
deploy are satisfied only after that.
