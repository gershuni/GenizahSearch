# Phase 115: Performance Metrics - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-16
**Phase:** 115-performance-metrics
**Areas discussed:** Indexing scope, Result-count buckets, Flush cadence, Summary stats

---

## Indexing scope — include indexing timing?

| Option | Description | Selected |
|--------|-------------|----------|
| Yes — include indexing | Time LocalIndexerWorker (+ LAB rebuild). Own lightweight event: duration + doc-count bucket. Closes PERF-01's "indexing" clause; visibility into onboarding/re-index cost behind past freezes. | ✓ |
| Search-only this cycle | Keep to the 3 search threads + summary; defer indexing telemetry. Smaller surface. | |

**User's choice:** Yes — include indexing.
**Notes:** Indexing is a different shape (one long op vs many short searches), so it is its own event,
NOT folded into the per-search summary.

## Indexing scope — operation-kind granularity?

| Option | Description | Selected |
|--------|-------------|----------|
| Label the operation kind | Enum `initial_scan` / `incremental_add` / `reindex_all` / `lab_rebuild` (hardcoded constant) + duration + doc-count bucket. Separates one-time onboarding cost from routine churn. | ✓ |
| Duration + doc-count only | One flat "indexing" event, no kind label. Simpler; can't distinguish first-run from re-index. | |

**User's choice:** Label the operation kind.
**Notes:** Matches granular-data preference. Needs a new DesktopEvent registry member + allowlist
additions (PRIV-06); value must be a literal producer-side constant.

---

## Result-count buckets — which scheme for the perf summary?

| Option | Description | Selected |
|--------|-------------|----------|
| Finer perf buckets | 0/1-10/11-50/51-200/200+. Better latency-vs-size correlation, matches SC example. Cost: two coexisting bucket schemes. | |
| Reuse coarse buckets | Keep 0/1-9/10-99/100+ everywhere via one shared function. Maximally consistent; coarser resolution. | ✓ |

**User's choice:** Reuse coarse buckets.
**Notes:** One shared `_telemetry_result_bucket()` across all telemetry events; avoids a divergent
second scheme.

---

## Flush cadence — default cadence?

| Option | Description | Selected |
|--------|-------------|----------|
| Periodic ~30min + close | Flush every ~30 min active + at close (reuse active_ping QTimer pattern). Crash-resilient; handful-to-low-tens of events/day. Sampling default OFF (sample_n=1 escape hatch). | ✓ |
| Periodic ~60min + close | Every ~60 min + close. Fewer events; a crash can lose up to an hour of stats. | |
| Close-only | One summary at clean close. Minimal volume but loses ALL perf data on crash/hard-kill (common per Phase 113). | |

**User's choice:** Periodic ~30min + close.
**Notes:** Crash-resilience prioritized over minimal volume. Cadence + sampling env/config-tunable
(PERF-03). Each flush resets the accumulator (self-contained windows, no PostHog double-count) — locked
as Claude's recommendation, accepted.

---

## Summary stats — which extra signals?

| Option | Description | Selected |
|--------|-------------|----------|
| Zero-result count per mode | Search-effectiveness signal at session level. | ✓ |
| Min / max duration per mode | Full latency spread + outliers; useful for D-F12 (~8s wall-clock). | ✓ |
| Result-bucket distribution | Counts per result bucket per mode; correlate size with latency. | ✓ |
| Corpus-scope split | Stats broken down by Genizah/Local/ALL. Adds nesting, stays in one event. | ✓ |

**User's choice:** All four.
**Notes:** Per-mode median/p95 + count locked by SC; all extras added. Everything packs as
low-cardinality numbers nested in the single `desktop_session_performance_summary` event.

---

## Claude's Discretion

- Accumulator location & API; how SC#1 ("handler calls track_performance()") reconciles with SC#3
  ("never one event per search") — accumulate-in-track_performance vs a dedicated accumulator API.
- Exact env var names + the ~30-min periodic interval default; the nested-payload shape + allowlist
  container key(s); whether indexing operation-kind reuses `action` or a new prop; doc-count bucket
  boundaries; whether `GroupingThread` is timed (likely not).

## Deferred Ideas

- Privacy CI audit + frozen-binary self-test + operational runbook — Phase 116.
- Handled/non-fatal error counting — ERR-01 (Future).
- PostHog feature flags / remote config on desktop — FLAG-F1 (Future).
- (Not deferred) PERF-01's "indexing" clause is in-scope this phase.
- 7 keyword-coincidental todo matches reviewed, none folded (none concern perf telemetry).
