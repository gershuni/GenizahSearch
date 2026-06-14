# Roadmap: GenizahSearch

## Milestones

- **v1 External Data Integration** -- Phases 1-7 (shipped 2026-02-07)
- **v5.6.0 Desktop Parity & PGP Integration** -- Phases 8-12 (shipped 2026-02-09)
- **v5.7.0 Responsa Search** -- Phases 14-17 (shipped 2026-02-10)
- **v5.7.2 Cleanup, Normalization & Sections** -- Phases 18-21 (shipped 2026-02-11)
- **v5.7.3 Pending Corrections Visibility** -- Phases 22-24 (shipped 2026-02-11)
- **v5.8.0 FJMS Integration** -- Phases 25-28 (shipped 2026-02-15)
- **v5.9.0 Multi-Source Image & Metadata Integration** -- Phases 29-34 (shipped 2026-02-16)
- **v6.0.0 Local Data Architecture** -- Phases 35-40 (shipped 2026-02-22)
- **v6.1.0 Catalog Browse & Navigation** -- Phase 41 (shipped 2026-02-27)
- **v6.5.0 Search UX & Filtered Search** -- Phases 42-46 (shipped 2026-03-14)
- **v7.0.0 Fragment Puzzle** -- Phases 47-52 (shipped 2026-03-17)
- **v7.1.0 FIST Gap Fill** -- Phase 53 (shipped 2026-03-19)
- **v7.6 Search Refinement & Scholarly Joins** -- Phases 54-57 (shipped 2026-03-31)
- **v7.7 Volume-Aware Browse** -- Phases 58-61 (shipped 2026-04-01)
- **v7.8 Structural Foundation** -- Phases 63-66 (shipped 2026-04-15)
- **v7.9 Decomposition** -- Phases 67-76 (complete 2026-04-17)
- **v7.10 Search API** -- Phases 77-83 (shipped 2026-05-05)
- **v7.11 CUDL Coverage & Synthetic Inventories** -- Phases 84-86 (shipped 2026-05-12)
- **v7.12 Multitenant Architecture (Path B)** -- Phases 87-92 + 92.1 + 92.2 + promoted 999.1/999.4 (shipped 2026-05-18)
- **v7.13 Research-Grade Downloads & PGP Filter** -- Phases 93-94 (shipped 2026-05-21)
- **v7.14 My Library — Local Document Search** -- Phases 95-98 (shipped 2026-05-24; closed 2026-05-27)
- **v7.15 My Library Visual** -- Phases 99-101 (shipped 2026-05-28). See `milestones/v7.15-ROADMAP.md`
- **v7.16 Hebrew PDF Text Quality** -- Phase 102 + no-phase quality work (shipped 2026-06-01). See `milestones/v7.16-ROADMAP.md`
- **v8.0.0 Dicta Rebrand & Joins Lab** -- BRAND (no-phase) + Phases 103, 105 (folded from v7.17; Phase 104 → EXP-F3) + Phases 106-110 Joins Lab (shipped 2026-06-09; closed 2026-06-11). Component B (JSA-01/02/03 + JWB-05) + web Joins Lab UI deferred post-v8.0.0. See `milestones/v8.0.0-ROADMAP.md`
- 🚧 **v8.1.0 Desktop Telemetry** -- Phases 111-116 (in progress)

## Phases

<details>
<summary>✅ v8.0.0 Dicta Rebrand & Joins Lab (Phases 103, 105 + 106-110) — SHIPPED 2026-06-09, closed 2026-06-11</summary>

See: .planning/milestones/v8.0.0-ROADMAP.md

7 phases — 103 + 105 (folded from the v7.17 cycle) + 106-110 (Joins Lab Component A). Phase 104 deferred → EXP-F3 (delivered in 110). 31 formal plans (35 completed plan-equivalents incl. 108 redesign/polish + 109 gap rounds). Git range `v7.16.0` → `v8.0.0` (328 commits); 266 files, +55,320 / −785; 2026-06-01 → 2026-06-09.

The flagship **"Dicta Genizah Search Pro"** release: the desktop **rebrand** (display-only; binary identifiers unchanged so installs upgrade in place) + LOCAL ("My Library") **export** support (Phases 103 + 105, closes D-F17) + the new **Joins Lab** — an interactive, human-in-the-loop join-hunting workbench (desktop). Phase 106 shared core (`shared/joins_lab.py`, web-reusable, no PyQt / no direct `fist_data`, `SearchExecutor` adapter); Phases 107-108 the desktop Join Workbench (anchor pane + line-by-line query builders for both sides of the leaf + deduped candidate grid/table + side-by-side Compare + pairwise→group join model + public action APIs); Phase 109 merged Visual Similarity into the candidate surface (single 👁 eye badge + Visual Similarity toggle; standalone VS dialog soft-retired; "Find Joins" is the single entry); Phase 110 added Composition Search over the LOCAL corpus (Genizah/Local/ALL selector orthogonal to Lab mode; standard LOCAL composition uses the REGULAR My-Library index not the LAB side-index; score-interleaved merge, no RRF) + LOCAL-aware `export_comp_report` (EXP-F3). 25 requirements satisfied (BRAND 2 + LEXP 7 + EXPUX 4 + JWB 9 + COMP-LOC 2 + EXP-F3 1). **Deferred by user decision (2026-06-08):** all of Component B (JSA-01/02/03 + JWB-05) and the web Joins Lab UI → post-v8.0.0. Tagged `v8.0.0` @ `71e0912e` (both apps; GitHub Release with installer). Close ritual run retroactively 2026-06-11.

</details>

<details>
<summary>✅ v7.16 Hebrew PDF Text Quality (Phase 102 + no-phase quality work) — SHIPPED 2026-06-01</summary>

See: .planning/milestones/v7.16-ROADMAP.md

1 formal phase (102, 5 plans) + post-phase no-phase quality work. Rewrote the LOCAL ("My Library") Hebrew PDF text-layer extractor onto a `page.get_text("rawdict")` per-glyph foundation (`shared/local_indexer_rtl.py`): RTL-gated reorder (Meiri core, no LTR regression), Unicode-`Mn` nikud/maqaf classification, per-line 1-D Otsu word-gap de-space, `_ltr_damage_guard` RTL-trust fix, corrupt_encoding detection; `extraction_format_version` 2→3. Emphasis letter-spacing no longer shatters Hebrew words (אוצר הגאונים single-letter tokens 73.5%→~3-5%) and tight typesetting no longer fuses phrases (רביצקי 15.8%→0.07%). No-phase work bundled: de-space follow-ups (D-F13b/c/d), LOCAL UAT extraction fixes (HTML/xlsx/CSV + folder opt-out cascade BLOCKER, D-F19..D-F22/D-F25), file-management actions for LOCAL hits (D-F24), and three search/startup freeze fixes (D-F23: 778 MB `search_history.json`, large-folder O(n²) startup, LAB-rebuild churn). Shipped v7.16.0 desktop-only (tag `v7.16.0`, GitHub Release with installer, CI green).

</details>

<details>
<summary>✅ v7.15 My Library Visual (Phases 99-101) — SHIPPED 2026-05-28</summary>

- [x] Phase 99: PDF Page Renderer (2/2 plans) — completed 2026-05-27
- [x] Phase 100: LOCAL PDF Image in ResultDialog + Browse (3/3 plans) — completed 2026-05-27
- [x] Phase 101: LOCAL PDF Text Extraction RTL Fix + Phase 100 Remnant Cleanup (2/2 plans + UAT follow-ons) — completed 2026-05-28

</details>

---

### 🚧 v8.1.0 Desktop Telemetry (Phases 111-116)

**Milestone goal:** Add opt-in, privacy-preserving telemetry to the desktop app ("Dicta Genizah Search Pro") so real-world usage, version adoption, performance, crashes, and cross-surface (web↔desktop) per-user journeys become visible in PostHog — without ever transmitting My Library data or search content. Desktop feeds the **existing shared web PostHog project** and is identity-aligned with the web app (logged-in users → same Supabase `user.id`); **no web code change required**. Default OFF until the user consents. (REVISED 2026-06-14 — see `.planning/research/POSTHOG-PROJECT-DECISION.md`.)

**Foundation-first invariant:** No event can fire before the consent gate, scrubber, and property/event allowlist exist and are tested. Phases 113-115 all depend on Phase 111 being complete and green.

## Summary Checklist

- [ ] **Phase 111: Telemetry Foundation** - `desktop/telemetry.py` chokepoint + consent storage + structural scrubber + property/event allowlist + flush infrastructure (no events fire yet)
- [ ] **Phase 112: Consent UX** - Bilingual first-run dialog + Settings toggle + opt-out queue drain + bilingual privacy disclosure
- [ ] **Phase 113: Crash Reporting** - Exception hooks (chained, non-blocking) + faulthandler + scrubbed crash payloads + bounded synchronous flush + next-launch native-crash detection
- [ ] **Phase 114: Usage Analytics** - Session-start, feature/tab usage, search mode+corpus enums, active-user signal, base props, session/clock correctness
- [ ] **Phase 115: Performance Metrics** - Search/indexing durations, result-count buckets, per-session summary with periodic+close flush, configurable sampling
- [ ] **Phase 116: Privacy Audit + CI Gate** - AST guard in CI, forbidden-field tests, frozen-binary SSL/offline self-test, operational runbook

## Phase Details

### Phase 111: Telemetry Foundation
**Goal**: The `desktop/telemetry.py` chokepoint module exists with its full public API, consent state persists in `config.pkl`, the structural scrubber enforces no-PII at the network boundary, and the property/event allowlist prevents future accidental leaks — but no events fire yet because no producers are wired.
**Depends on**: Nothing (no prior v8.1.0 phases)
**Requirements**: CONSENT-01, CONSENT-05, CONSENT-06, CONSENT-07, INFRA-01, INFRA-02, INFRA-03, INFRA-04, INFRA-05, PRIV-01, PRIV-02, PRIV-06, IDENT-03, IDENT-04
**Success Criteria** (what must be TRUE):
  1. `desktop/telemetry.py` is importable and exposes all eight public callables (`is_enabled`, `track`, `track_performance`, `track_error`, `get_install_id`, `set_consent`, `install_exception_hooks`, `show_first_run_prompt`) plus the identity hooks (`identify`/`reset` or equivalent); every call gate-checks `is_enabled()` and returns immediately when consent is absent or false — a fresh `config.pkl` emits zero events. Events target the shared web project (reused publishable key, env-overridable).
  2. `set_consent(True)` mints a UUID-v4 install ID and persists it in `config.pkl`; `set_consent(False)` stops emission immediately; the install ID is retained on disk (not deleted). `distinct_id` resolves to the **Supabase `user.id` when logged in**, else the per-install uuid; the `$identify`/alias/reset emission mechanism exists and is consent-gated (IDENT-04) though login/logout wiring lands in Phase 114.
  3. `_scrub_props()` strips banned keys, redacts path-like strings, and drops frame locals from any dict before it can reach `enqueue_event` — verified by unit tests with real Windows-path fixtures and Hebrew query strings.
  4. A static property allowlist rejects any property not on the list (including `hostname`, `username`, `executable path`, `cwd`, and all query/content-derived fields); event names are drawn exclusively from a fixed registry enum with no dynamic construction.
  5. `shared/posthog_server.py` gains backward-compatible NEUTRAL additions (`_scrub_hook`, `set_default_distinct_id`, `set_capture_api_key`/`set_capture_host`, `_flush_before_exit`) without breaking its existing web/breaker consumers or the 5 test monkeypatches targeting `_event_queue`. Per D-04 the module stays **UNGATED** — the consent gate (`is_enabled()`) lives only in `desktop/telemetry.py`; no global `_telemetry_enabled` gate is added to the shared module (that would suppress web/NLI-breaker telemetry by desktop consent).
**Plans**: 3 plans
- [ ] 111-01-PLAN.md — `shared/posthog_server.py` neutral additions (set_default_distinct_id, register_scrub_hook, _flush_before_exit, _drain_and_discard) + INFRA-03 tests [Wave 1]
- [ ] 111-02-PLAN.md — `desktop/telemetry.py` chokepoint: consent gate + config.pkl persistence + scrubber + property allowlist + DesktopEvent enum + 8 callables + identity hooks + self-test [Wave 2]
- [ ] 111-03-PLAN.md — PRIV-03 chokepoint AST guard `tests/test_telemetry_no_direct_posthog.py` (landed early from Phase 116) [Wave 3]

### Phase 112: Consent UX
**Goal**: The user can give or withdraw consent through a bilingual first-run dialog (shown exactly once, on first launch after updating to v8.1.0) and a Settings/About toggle; opting out immediately drains and discards any already-queued events; a bilingual privacy disclosure is reachable from both surfaces.
**Depends on**: Phase 111
**Requirements**: CONSENT-02, CONSENT-03, CONSENT-04, CONSENT-08, PRIV-05
**Success Criteria** (what must be TRUE):
  1. On first launch after upgrade (or fresh install), a bilingual EN/HE modal dialog appears with two equal-weight buttons (no pre-selection, no Enter-key shortcut defaulting to Accept) — pressing Enter without reading leaves the user opted out.
  2. The dialog fires exactly once across app restarts: after any choice, `telemetry_first_run_shown=True` is written to `config.pkl` unconditionally and subsequent launches skip the dialog entirely.
  3. The stored consent record includes the consent timestamp, app version, and consent-UI version, providing a lightweight audit trail.
  4. The Settings/About toggle reads and writes the same `telemetry_enabled` key as the first-run dialog; toggling off immediately purges any already-queued un-sent events so nothing buffered before opt-out is transmitted afterward.
  5. A bilingual (EN/HE) privacy disclosure explains what is collected, what is not (no search content, no My Library paths/filenames), who processes the data, and how to opt out — accessible from both the first-run dialog and the Settings toggle location.
**Plans**: TBD
**UI hint**: yes

### Phase 113: Crash Reporting
**Goal**: Uncaught exceptions on any thread are captured, scrubbed, and enqueued non-blockingly before the existing crash-log handler runs; faulthandler captures native C-extension crashes to a local file; a bounded synchronous flush delivers the crash event before process exit; next-launch detection re-emits native crash signals after consent is confirmed.
**Depends on**: Phase 112
**Requirements**: CRASH-01, CRASH-02, CRASH-03, CRASH-04, CRASH-05, CRASH-06, CRASH-07
**Success Criteria** (what must be TRUE):
  1. `install_exception_hooks()` wraps (never replaces) the existing `_setup_crash_handler()` so `crash_log.txt` continues to be written — verified by a test that raises an exception after hook installation and confirms both the PostHog enqueue and the crash-log file write occur.
  2. `threading.excepthook` is installed to capture QThread/worker exceptions (SearchThread, LocalIndexerWorker, FolderWalkWorker) that do not reach `sys.excepthook`; `KeyboardInterrupt` is explicitly excluded from both hooks.
  3. Crash events contain only exception type name, scrubbed module basename + line number, app version, and OS — no frame locals, no exception message string, no file paths, no query text — enforced by the Phase 111 scrubber applied inside the hook before enqueue.
  4. The exception hook body is non-blocking (executes only `traceback.format_exception` + scrub + `put_nowait`; no network I/O, no disk I/O, no lock acquisition) and is entirely wrapped in `try/finally` so the existing crash handler always runs even if the telemetry step throws.
  5. A bounded synchronous `_flush_before_exit(timeout=0.5)` is called inside the exception hook after enqueueing the crash event, and via `atexit` for clean exits, so crash events are not silently lost when the daemon drain thread is killed at process exit.
**Plans**: TBD

### Phase 114: Usage Analytics
**Goal**: The desktop app emits allowlisted usage events (session start/end, tab/surface activations, search mode and corpus enums) that enable DAU/MAU, version adoption, and feature-use measurement in PostHog — with no query content, no My Library data, and no environment identifiers beyond OS family/version.
**Depends on**: Phase 112
**Requirements**: USAGE-01, USAGE-02, USAGE-03, USAGE-04, USAGE-05, USAGE-06, IDENT-01, IDENT-02
**Success Criteria** (what must be TRUE):
  0. On login the desktop calls `identify(distinct_id = Supabase user.id)` (exact match to `web/auth_state.py:160-170`) and aliases the prior anonymous per-install uuid (`$anon_distinct_id`); on logout it resets to the anonymous id. Only the user id is sent — never email/name. A logged-in researcher's web + desktop events merge into one person in the shared project.
  1. A session-start event fires once per process after consent is confirmed, carrying only allowlisted environment props: app version, OS family + version, Python/PyQt version, UI language — never hostname, machine name, username, executable path, or working directory.
  2. Feature usage events capture which tabs and key surfaces (Joins Lab, Fragment Puzzle, major dialogs) are opened as counts; no free-text or content properties appear on any event.
  3. Search executions are captured with `search_mode` (keyword/Responsa/composition/parallels) and `corpus_scope` (Genizah/Local/ALL) as fixed enum values — the query text, filter content, and exclusion list are structurally absent (not present in the event, not scrubbed away).
  4. Every event carries base properties (`platform=desktop`, `$process_person_profile=false`, `app_version`) applied through a single shared `_emit()` helper; no callsite adds these manually or bypasses the helper.
  5. Exactly one telemetry session ID is generated per process; all timestamps are UTC; performance durations use a monotonic clock; a crash-restart begins a fresh session without emitting a duplicate session-start for the crashed process.
**Plans**: TBD

### Phase 115: Performance Metrics
**Goal**: Search and indexing durations are measured on worker threads and accumulated into a per-session summary (aggregated result counts and latency buckets) that is flushed once at session close and periodically — never one event per search — so heavy users (~50 searches/day) do not flood the PostHog stream.
**Depends on**: Phase 112
**Requirements**: PERF-01, PERF-02, PERF-03
**Success Criteria** (what must be TRUE):
  1. `SearchThread`, `CompositionThread`, and `LabSearchThread` each emit a `perf_signal(float, int)` Qt signal on completion carrying elapsed milliseconds and result count; the UI-thread handler calls `track_performance()` without exposing any query text.
  2. Result counts are reported exclusively as bounded buckets (e.g., 0 / 1-10 / 11-50 / 51-200 / 200+), never as raw integers that would create unbounded PostHog histogram cardinality.
  3. Performance data accumulates in a per-session in-memory summary (median/p95 + counts per search mode); the summary flushes as a single `desktop_session_performance_summary` event at app close and on a configurable periodic schedule — the default produces approximately tens of events per day for heavy users, not hundreds; sampling and flush interval are tunable via environment variable or config without a code change.
**Plans**: TBD

### Phase 116: Privacy Audit + CI Gate
**Goal**: The complete telemetry stack is validated end-to-end: the AST guard runs green in CI on both Ubuntu and Windows, automated tests prove that no forbidden field ever reaches `enqueue_event`, frozen-binary SSL and offline degradation are verified on a clean Windows machine, and the operational runbook documents the desktop PostHog project, embedded key posture, and drop-counter monitoring.
**Depends on**: Phases 113, 114, 115
**Requirements**: PRIV-03, PRIV-04, INFRA-06
**Success Criteria** (what must be TRUE):
  1. The AST CI guard (`tests/test_telemetry_no_direct_posthog.py`, modeled on `test_no_raw_storage_access.py`) passes on both Ubuntu and Windows CI: no file under `desktop/` except `desktop/telemetry.py` imports `shared.posthog_server` or calls `enqueue_event` directly.
  2. Automated tests assert that representative crash tracebacks, My Library search scenarios, and composition searches never produce a PostHog payload containing any forbidden field — explicitly: My Library paths, filenames, query/search text, usernames, or hostnames — and that zero events are enqueued before consent is confirmed.
  3. The frozen `.exe` on a clean Windows machine (no Python installed) successfully delivers events to the desktop PostHog project (SSL cert bundle present and functional); with network disabled, the app starts normally and telemetry degrades silently with no dialog, no delay, and no crash.
**Plans**: TBD

## Progress

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 111. Telemetry Foundation | v8.1.0 | 0/3 | Planned | - |
| 112. Consent UX | v8.1.0 | 0/TBD | Not started | - |
| 113. Crash Reporting | v8.1.0 | 0/TBD | Not started | - |
| 114. Usage Analytics | v8.1.0 | 0/TBD | Not started | - |
| 115. Performance Metrics | v8.1.0 | 0/TBD | Not started | - |
| 116. Privacy Audit + CI Gate | v8.1.0 | 0/TBD | Not started | - |
