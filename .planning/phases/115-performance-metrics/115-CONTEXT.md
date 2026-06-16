# Phase 115: Performance Metrics - Context

**Gathered:** 2026-06-16
**Status:** Ready for planning

<domain>
## Phase Boundary

Wire the **performance-metrics producers** into the PyQt6 desktop app. Search (and indexing)
durations are measured on the worker threads, accumulated into a **per-session in-memory summary**
(median/p95 + counts per search mode), and flushed as a **single** `desktop_session_performance_summary`
event — periodically + at app close, **never one event per search** — plus a separate lightweight
**indexing-duration** event. The telemetry chokepoint already exists (Phases 111/113/114):
`desktop/telemetry.py` exposes `track_performance()`, the `DesktopEvent` registry (incl.
`SESSION_PERF = 'desktop_session_performance_summary'`), `_ALLOWED_PROPS` (incl.
`duration_ms`/`result_count`/`sample_n`/`duration_bucket_ms`), the structural scrubber, and the consent
gate. Phase 115 does NOT build new chokepoint machinery beyond the registry/allowlist additions PRIV-06
requires for the new events — it **adds `perf_signal` to the search threads, an accumulator + flush, and
the indexing producers**, all routed through the existing chokepoint.

**In scope (requirements PERF-01, PERF-02, PERF-03):**
- `perf_signal(float, int)` on `SearchThread`, `LabSearchThread`, `CompositionThread` (+ `LabCompositionThread`
  for completeness); UI-thread handler feeds the accumulator.
- Per-session perf summary (median/p95 + counts + extra stats) → single `desktop_session_performance_summary`
  event, periodic + close flush, env/config-tunable cadence + sampling.
- Result counts reported **only** as bounded buckets (reuse the existing coarse function).
- **Indexing-duration telemetry** (LocalIndexerWorker + LAB rebuild) as its own event with an
  operation-kind enum (PERF-01 "indexing" clause).

**Out of scope:** the privacy CI audit + frozen-binary self-test + operational runbook (Phase 116,
PRIV-03/PRIV-04/INFRA-06). No new SDK, no web changes, no new identity/consent work (rides Phase 111-114).
</domain>

<decisions>
## Implementation Decisions

### Indexing timing (PERF-01 "indexing" clause)
- **D-01:** Include indexing-duration telemetry **this phase**. Time `LocalIndexerWorker` runs AND the
  LAB side-index rebuild. Reported as its OWN lightweight event — **NOT** folded into the per-search
  session summary (different shape: one long background op vs many short searches).
- **D-02:** The indexing event carries an **operation-kind enum** — a hardcoded constant, one of
  `initial_scan` / `incremental_add` / `reindex_all` / `lab_rebuild` — plus `duration_ms` (monotonic)
  and a **doc-count bucket**. NEVER folder paths / filenames / raw doc counts as content. Requires a
  **new `DesktopEvent` registry member** (PRIV-06) + allowlist additions. The operation-kind value may
  reuse the existing allowlisted `action` prop OR a new prop (planner's call), but it MUST be a literal
  producer-side constant (the Phase-114 D-04 value-side discipline applies — never `windowTitle()`/
  status strings/folder names).

### Result-count buckets (PERF-02)
- **D-03:** The perf summary reuses the **existing shared coarse bucket** function
  `_telemetry_result_bucket()` (`0` / `1-9` / `10-99` / `100+`, `genizah_app.py:3278`) — **one bucket
  scheme across ALL telemetry events**. Finer perf-specific buckets (0/1-10/11-50/51-200/200+) were
  considered and **rejected** to avoid a second, divergent scheme. The indexing doc-count bucket stays
  coarse + bounded (may reuse the same function or a parallel small constant set — planner discretion).

### Flush cadence & volume (PERF-03)
- **D-04:** Default flush cadence = **periodic every ~30 min of ACTIVE use + once at app close**. Reuse
  the `active_ping` mechanism (a ~5-min `QTimer` check + `applicationStateChanged` focus/resume
  awareness, `genizah_app.py:3705`) — **NOT** a naive 30-min `QTimer`. Chosen for crash-resilience:
  Phase 113 established that SIGKILL/crash hard-exits are common on this app, so a close-only flush
  would lose most heavy-user data.
- **D-05:** Both cadence and sampling are **tunable via env var / config WITHOUT a code change**
  (PERF-03 hard requirement). Default sampling = **OFF** (`sample_n = 1`, accumulate every search);
  `sample_n` is the escape-hatch knob if stream volume ever climbs. The periodic interval is likewise
  env/config-overridable. Ceiling is "tens/day not hundreds" for a heavy user (~50 searches/day).
- **D-06:** Each flush **RESETS** the in-memory accumulator — every `desktop_session_performance_summary`
  event is a self-contained window, so summing events in PostHog does not double-count. The close flush
  emits the final partial window. (Claude's recommendation, accepted by Hillel — flag if planning finds
  a reason cumulative is better.)

### Session-summary content (PERF-01/02/03)
- **D-07:** The single `desktop_session_performance_summary` event carries, **per search mode**:
  median ms, p95 ms, search count — **plus** (all selected): zero-result count, min ms, max ms,
  result-bucket distribution (counts per coarse bucket), and a **corpus-scope split**
  (Genizah / Local / ALL). All packed as **low-cardinality numbers nested in ONE event**. The
  structured/nested payload sits under allowlisted container key(s) — the scrubber recurses into nested
  dicts (`_scrub_value`, `telemetry.py:245`) and the allowlist gates top-level keys, so the planner must
  add the summary container key(s) to `_ALLOWED_PROPS`. No raw query text, ever.

### Producer wiring (carried-forward locks made concrete)
- **D-08:** `SearchThread`, `LabSearchThread`, `CompositionThread` — and `LabCompositionThread` for
  completeness (SC#1 names only the first three; Lab composition included so it isn't a silent gap —
  planner confirms) — each gain a `perf_signal(float, int)` Qt signal (elapsed **monotonic** ms +
  result count) emitted on completion. The UI-thread handler **feeds the accumulator**, NOT a per-search
  emit. Hang the timing off the **same completion points** the Phase-114 per-run search-state object
  (114 D-09) already uses, so each search is timed exactly once — **completed runs only**; cancelled
  runs carry no duration. `GroupingThread` is post-processing, not a search — likely NOT timed (planner's
  call).
- **D-09:** All emission stays inside `desktop/telemetry.py` (AST guards
  `tests/test_no_raw_storage_access.py` + `tests/test_telemetry_no_direct_posthog.py` enforce it).
  Durations use a **monotonic** clock (114 D-14). The accumulator + flush live behind the
  `_telemetry_ready()` + `is_enabled()` gates. The close flush rides the existing `closeEvent`
  session_end path; consider the Phase-113 `_flush_before_exit` bounded synchronous flush if a periodic
  flush could be mid-send at exit.

### Claude's Discretion
- **Accumulator location & API:** where the per-session summary object lives (a module-level object in
  `desktop/telemetry.py` vs the GUI), and how SC#1 ("handler calls `track_performance()`") reconciles with
  SC#3 ("never one event per search") — resolve by either making `track_performance()` **accumulate**
  instead of emit, or adding a dedicated `accumulate_performance()` and reserving `track_performance`/
  `SESSION_PERF` for the flush. (Note this tension explicitly for the researcher.)
- Exact env var names + the periodic interval default value (~30 min); the precise nested-payload shape +
  which allowlist container key(s) to add; whether indexing operation-kind reuses `action` or a new prop;
  the doc-count bucket boundaries (coarse + bounded). All within the locks above.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & milestone decisions
- `.planning/REQUIREMENTS.md` — **PERF-01 / PERF-02 / PERF-03** are this phase's reqs (Pending rows in
  the traceability table). The "Fixed constraints" + Out-of-Scope tables are LOCKED.
- `.planning/research/POSTHOG-PROJECT-DECISION.md` — shared web PostHog project + web-aligned identity
  (governs the `distinct_id` the summary rides on; no new identity work this phase).

### Prior-phase context (the foundation the producers ride on)
- `.planning/phases/111-telemetry-foundation/111-CONTEXT.md` — chokepoint, consent gate, `_ALLOWED_PROPS`,
  `DesktopEvent` registry, **PRIV-06** (new event names MUST be added to the enum; no dynamic construction).
- `.planning/phases/113-crash-reporting/113-CONTEXT.md` — `_flush_before_exit` bounded synchronous flush +
  `atexit` (the exit-path delivery primitive the periodic-flush-in-flight + close flush can reuse).
- `.planning/phases/114-usage-analytics/114-CONTEXT.md` — **114 D-09** per-run search-state object (time
  exactly once, from completion OR explicit stop), **D-05** search-mode enum map, **D-07** `corpus_scope` +
  `result_count_bucket`, **D-14** monotonic clock + single `session_id`, **D-16** active_ping QTimer pattern
  (the periodic-flush analog), **D-04** value-side hardcoded-constant discipline.

### Live code the producers wire into
- `desktop/telemetry.py` — `track_performance()` (`:710`), `DesktopEvent` enum (`:132`; `SESSION_PERF` at
  `:162`; ADD the indexing event member), `_ALLOWED_PROPS` (`:292`; has `duration_ms`/`result_count`/
  `sample_n`/`duration_bucket_ms`; ADD the summary container key(s) + indexing key(s)), recursive scrubber
  `_scrub_value`/`_scrub_props` (`:245`), `is_enabled`.
- `gui_threads.py` — `SearchThread` (`:80`), `LabSearchThread` (`:123`), `CompositionThread` (`:168`),
  `LabCompositionThread` (`:227`) — add `perf_signal(float, int)` emitted at `run()` completion.
- `genizah_app.py` — `_telemetry_result_bucket()` (`:3278`), `_setup_active_ping`/`_maybe_emit_active_ping`
  (`:3705` — the periodic-check + focus/resume pattern to MIRROR for the flush), `_telemetry_ready()`,
  `closeEvent` session_end (`:26912`), and the Phase-114 search-completion handlers that already build the
  per-run search-state object (`on_search_finished` ~`:17616`, composition paths ~`:19085` / ~`:22858`).
- `shared/local_indexer.py` — `LocalIndexerWorker` (the bulk re-extraction worker) + the LAB rebuild path
  (`build_lab_side_index`) — the indexing-duration producers (D-01/D-02).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **`desktop/telemetry.py` chokepoint** — fully built; `track_performance()`, `SESSION_PERF` enum, and
  `duration_ms`/`result_count`/`sample_n`/`duration_bucket_ms` props already exist in the allowlist. Phase
  115 adds: one indexing `DesktopEvent` member, the summary-container allowlist key(s), the accumulator +
  flush logic.
- **`_telemetry_result_bucket()`** (`genizah_app.py:3278`) — the single shared coarse bucket function;
  REUSE it for the summary's result-bucket distribution (D-03).
- **`active_ping` machinery** (`genizah_app.py:3705`) — `QTimer` (~5-min check) + `applicationStateChanged`
  focus/resume awareness; MIRROR this for the periodic flush (D-04), do not write a naive 30-min timer.
- **Per-run search-state object** (Phase 114, D-09) — already fires exactly once at completion/stop; hang
  perf timing off the same point so a search is timed exactly once (D-08).
- **`_flush_before_exit`** (Phase 113) — bounded synchronous exit flush available for the close path (D-09).

### Established Patterns
- All emission MUST route through `desktop/telemetry.py` (AST guards enforce no raw `enqueue_event`).
  Event names from `DesktopEvent` only (PRIV-06); props from `_ALLOWED_PROPS` only.
- Values placed on events MUST be producer-side literal constants — never `currentText()`/`windowTitle()`/
  status strings/`selectedFiles()`/paths (Phase 114 D-04). Applies to the indexing operation-kind label.
- Durations use a **monotonic** clock; one `session_id` per process (114 D-14); all timestamps UTC.

### Integration Points
- Thread perf: `perf_signal` emitted in each search thread's `run()` completion → UI-thread handler →
  accumulator.
- Accumulator flush: a periodic mechanism mirroring `_maybe_emit_active_ping` + the `closeEvent` path.
- Indexing perf: instrument `LocalIndexerWorker` run boundaries + `build_lab_side_index` (background
  thread — emit from the worker's completion signal, not the worker thread directly, to keep the
  chokepoint UI-thread-safe per existing pattern).

</code_context>

<specifics>
## Specific Ideas

- Hillel wants **granular** perf data — he selected ALL extra summary stats (zero-result count, min/max,
  result-bucket distribution, corpus-scope split) on the principle "aggregate up later" (consistent with
  Phase 114).
- **Crash-resilience prioritized over minimal volume** — he chose periodic+close (not close-only) because
  the app frequently hard-exits (Phase 113). The data being complete for heavy users matters more than the
  smallest possible event count, within the "tens/day" ceiling.
- The open **D-F12** investigation (regular Search ~8s wall-clock) is exactly what min/max + p95 should
  illuminate — a motivating use for the summary.

</specifics>

<deferred>
## Deferred Ideas

- **Privacy CI audit + frozen-binary self-test + operational runbook** — Phase 116 (PRIV-03/PRIV-04/
  INFRA-06). The chokepoint AST guards already exist; Phase 116 extends, not re-implements.
- **Handled/non-fatal error counting** — ERR-01 (Future), explicitly out of v8.1.0.
- **PostHog feature flags / remote config on desktop** — FLAG-F1 (Future).

PERF-01's "indexing" clause is **NOT** deferred — it is in-scope this phase (D-01).

### Reviewed Todos (not folded)
The `todo.match-phase` scan surfaced 7 matches, all **keyword-coincidental** (corrections migration,
Reading Desk UX, server-side search w/ email, unified metadata search, NLI MARC crawl, scholarly
citations, FIST gap fill). None concern performance telemetry — none folded.

</deferred>

---

*Phase: 115-performance-metrics*
*Context gathered: 2026-06-16*
