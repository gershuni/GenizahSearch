---
gsd_state_version: 1.0
milestone: v8.2.0
milestone_name: Web Joins Lab
status: executing
stopped_at: "Phase 121 Plan 03 Task 2 — human-verify checkpoint (HE-mode RTL UAT sign-off)"
last_updated: "2026-06-21T12:53:00Z"
last_activity: 2026-06-21
progress:
  total_phases: 5
  completed_phases: 3
  total_plans: 34
  completed_plans: 32
  percent: 60
---

# Project State

## Project Reference

See: .planning/PROJECT.md

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 121 — i18n-polish

## Current Position

Phase: 121 (i18n-polish) — EXECUTING
Plan: 3 of 3
Status: Ready to execute
Next: Phase 120 — Actions & Persistence
Last activity: 2026-06-21

## Accumulated Context

### Key Decisions (v8.2.0)

- **Vertical spine first (Phase 117):** The `WebSearchExecutor` adapter is the riskiest seam — it wraps `state.searcher.execute_search` directly, off the event loop, NOT `/api/search` (which omits `text_position`/`corpus_scope`). Phase 117 proves this seam end-to-end before building the full feature surface.
- **Build order (5 phases, condensed from an initial 9 per user request 2026-06-17 — spine kept thin):** Spine (117) → Joins, Entry & Full Builders (118) → Candidates, Compare & Visual Similarity (119) → Actions & Persistence (120) → i18n Polish (121).
- **Persistence = server-side per-session, NOT browser localStorage:** `web/safe_storage.py` wraps server-side `app.storage.user` (NiceGUI session cookie). `browser` = small encrypted cookie; `client` dies on refresh; `tab` is volatile per-tab. Decision: use `safe_user_*` helpers (survives refresh for anonymous users, keyed by session cookie). Persist inputs + triage only; re-run search on restore (avoids search-history payload-bloat class of bug).
- **Phase 87 invariant preserved throughout:** Zero raw `app.storage.user` accesses; `tests/test_no_raw_storage_access.py` allowlist stays `[]`; CI-guarded on every phase.
- **No new Supabase schema this milestone:** Known-joins leverage existing pairwise-join path + BFS group; cross-device sync deferred to PST-F1 (future).
- **ANC-05 multitenant known-joins safety:** `fetch_connected_fragments` has a process-global cache — Phase 118 must surface only public/confirmed joins or implement user/status-aware cache isolation to prevent User A's unconfirmed joins leaking to User B.
- **ACT-02 bulk puzzle handoff:** `/puzzle?add=` currently accepts one `sys_id` — Phase 120 ships a bulk staging payload/API so anchor + selected candidates open together.
- **Image resolution:** All images through existing per-provider proxies (NLI, Oxford, Cambridge, Manchester, JTS) + Phase-98 circuit breaker. No direct unguarded IIIF fetches.
- **Web-only milestone:** No GitHub Release (desktop-poll prompt avoidance per project convention). Version bumps but no GitHub Release object.

### Blockers/Concerns

None at roadmap creation. Phase 117 first plan will address the `WebSearchExecutor` adapter design in detail.

### Pending Todos

None yet. Begin with `/gsd:plan-phase 117`.

### Quick Tasks Completed

| # | Description | Date | Commit | Directory |
|---|-------------|------|--------|-----------|
| (none yet) | — | — | — | — |

## Deferred Items

Items carried forward from v8.1.0:

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| CONSENT-F1 | "Reset telemetry id" affordance in Settings | Future | v8.1.0 |
| ERR-01 | Handled/non-fatal error counting at high-value sites | Future | v8.1.0 |
| CRASH-F1 | "Send logs" flow for local faulthandler log | Future | v8.1.0 |
| WEB-F1 | Clean web `search_executed` query-text property (privacy gap) | Future | v8.1.0 |
| FLAG-F1 | PostHog feature flags / remote config on desktop | Future | v8.1.0 |
| PST-F1 | Cloud cross-device sync of Joins Lab candidate lists / triage | Future | v8.2.0 (desktop is local-only; nothing to sync with yet) |
| D-F12 | Regular Search ~8s wall-clock (profile-first) | Future | v8.1.0 |
| D-F18 | Context-menu LOCAL detection via `display` | Future | v8.0.0 |
| JSA-01 | Anchor parallels seeding (Component B) | Future | v8.0.0 |
| JSA-02 | Corpus-driven suggest-then-search completion (Component B) | Future | v8.0.0 |
| JSA-03 | Torn-word completion (Component B) | Future | v8.0.0 |
| JWB-05 | Tear-side assist (Component B) | Future | v8.0.0 |
| JL-UAT1 | Candidate cards: larger image + expandable text snippet | Phase 119 (candidate surface) | v8.2.0 P117 UAT |
| JL-UAT2 | Search progress bar / ETA for long searches | Phase 119/120 | v8.2.0 P117 UAT |
| JL-UAT3 | Long/common-phrase search drops the websocket ("Connection Lost" → reconnect → restored-session). TWO distinct causes: (a) search-compute GIL starvation (known, shared with `/search`; proper fix = deferred async-job pattern); (b) rendering ALL candidates at once (a 782-hit "פזורה" search completed, then the card flood dropped the socket). Cause (b) MITIGATED in P117 (`cad43f8e`): `candidate_grid.cap_candidates` renders at most 200 (app-wide `[:200]` convention) + truncation notice. Cause (a) still future. | (a) Future async-job / (b) MITIGATED P117 | v8.2.0 P117 UAT |
| JL-UAT4 | Stop search + return partial results | Phase 120 (actions) | v8.2.0 P117 UAT |
| JL-UAT5 | Joins Lab pane-width tuning / resizable panes | Phase 119/121 polish | v8.2.0 P117 UAT |
| JL-UAT6 | Candidate pagination / lazy-loading (full solution above the interim 200-cap render limit) — load-more / virtualized grid so all hits are reachable | Phase 119 (candidate surface) | v8.2.0 P117 UAT |

## Session Continuity

Last session: 2026-06-21T12:53:00Z
Stopped at: Phase 121 Plan 03 Task 2 — human-verify checkpoint (HE-mode RTL UAT sign-off)
Resume file: .planning/phases/121-i18n-polish/121-03-PLAN.md
Next step: Hillel runs the HE-mode RTL UAT checklist against the live web app and signs off (SC#2 acceptance gate). Type "approved" to resume after sign-off.

## Performance Metrics

| Phase | Plan | Duration | Notes |
|-------|------|----------|-------|
| (none yet) | — | — | — |
| Phase 117-vertical-spine P01 | 25min | 3 tasks | 3 files |
| Phase 117-vertical-spine P02 | 2min | 2 tasks | 2 files |
| Phase 117-vertical-spine P03 | 30min | 3 tasks | 6 files |
| Phase 117-vertical-spine P04 | 40min | 4 tasks | 3 files |
| Phase 118 P02 | 25min | 2 tasks | 2 files |
| Phase 118 P03 | 3min | 2 tasks | 1 files |
| Phase 118-joins-entry-full-builders P04 | 25min | 3 tasks | 1 files |
| Phase 119-candidates-compare-visual-similarity P02 | 8min | 3 tasks | 1 files |
| Phase 119-candidates-compare-visual-similarity P03 | 18min | 2 tasks | 2 files |
| Phase 119-candidates-compare-visual-similarity P05 | 20min | 2 tasks | 2 files |
| Phase 119-candidates-compare-visual-similarity P06 | 35min | 3 tasks | 4 files |
| Phase 119 P07 | 35m | 3 tasks | 6 files |
| Phase 119 P08 | 120min | 3 tasks | 5 files |
| Phase 119 P11 | 60 | 3 tasks | 5 files |
| Phase 120-actions-persistence P01 | 4m | 2 tasks | 2 files |
| Phase 120 P02 | 45min | 3 tasks | 4 files |
| Phase 120 P04 | 65 | 4 tasks | 6 files |
| Phase 120-actions-persistence P05 | 25 | 2 tasks | 5 files |
| Phase 120-actions-persistence P06 | 120 minutes | 2 tasks | 5 files |
| Phase 120-actions-persistence P07 | 150 | 3 tasks | 7 files |
| Phase 121-i18n-polish P01 | 5min | 2 tasks | 2 files |
| Phase 121-i18n-polish P02 | 20min | 2 tasks | 2 files |
| Phase 121-i18n-polish P03 | 5min | 1 task (paused at human checkpoint) | 1 file |

## Decisions

- **117-01:** WebSearchExecutor has no `__init__`; reads AppState singleton at call time — web AppState is process-global, ready by handler invocation time (mirrors desktop pattern but without per-instance injection)
- **117-01:** `execute_search` uses plain `except Exception: return []` (not InterruptedError re-raise) — SearchEngine catches InterruptedError internally and returns partial results; Plan 04 discards via stale-generation guard
- **117-01:** Off-loop AST guard (SC#3) scoped to `web/pages/joins_lab.py` only — adapter excluded since its sync methods run inside `run.io_bound` dispatched by joins_lab.py; scanning the adapter would produce false V2 violations
- **117-01:** `get_browse_page` stays NARROW — returns SearchEngine text/nav dict only, no image enrichment (HIGH-1); AnchorViewer uses a SEPARATE rich resolver in Plan 06
- [Phase ?]: Versioned safe_storage schema for joins_lab
- [Phase ?]: 117-03: resolve_image_url must be fed from web.services.BrowsePage (service.get_browse_page), not narrow Protocol dict (HIGH-1)
- [Phase ?]: 117-03: resolve_external_images lazy-imports web.state.state.meta_mgr; accepts meta_mgr= param for testability; one source of truth for D-10 external-image enrichment
- **117-06:** AnchorViewer factored with public _resolve_off_loop() + _build_img_html() sync methods for headless testability (no NiceGUI render harness needed in tests)
- **117-06:** Browse_resolver + external_resolver constructor params enable injection in tests without live AppState
- **117-04:** use `get_service()` pattern (not module-level `service`) for shelfmark resolution — matches browse.py:521 convention; `web.services` exports `get_service()` factory, not a `service` instance
- **117-04:** `lines_to_side_query` must pass `line.strip()` to `BuilderRow(term=)` — both the empty-filter and the term must use the stripped value (bug caught by test)
- **117-04:** AnchorViewer instantiated WITHOUT executor= (HIGH-1 honored); self-resolves rich BrowsePage via service.get_browse_page()
- **117-04:** D-06 login gate: logged-in path shows placeholder dialog pointing to /lists (full list picker is Phase 120 scope); anonymous path shows explicit login prompt
- [Phase ?]: BLD-03 modifier hoist desktop parity
- [Phase ?]: BLD-03 widget factory pattern
- [Phase ?]: 118-05: find_joins_url appended as last kwarg to both create_joins_button and create_joins_dialog — backward-compatible; card count load via run.io_bound (T-118-06)
- [Phase ?]: 119-02: snippet sortable per scaffold; on_compare full candidate; TriageState enforces verdicts
- **119-05:** _make_compare_handler hoisted before card block so image click and Compare button share the same candidate-carrying closure (G4)
- **119-05:** _triage_btn_refs per-card render-local dict passed into _make_triage_handler — immediate fill update without grid rebuild (G3, T-119-07)
- **119-05:** Only snippet_html()/htmlify() output passes to ui.html(sanitize=False); cursor:pointer on both image and placeholder branches (G4)
- [Phase ?]: 119-08 F-A1+F-A2 render-smoke harness
- [Phase ?]: 119-08: Task 1 manual — no pytest-asyncio; asyncio.run wrapper pattern over NiceGUI User on httpx.ASGITransport(core.app)
- [Phase ?]: 119-08: execute_search must return raw dicts (not Candidate objects) — dedup_candidates calls .get(); context.slot_stack saved before asyncio.run() and restored in finally for test isolation
- **120-02-D20:** SEED-008 M4 guard: outer try/except RuntimeError opens BEFORE PRE-await mutations in _load_known_joins — existing inner try/except Exception nested inside (M4 requirement: covers client teardown at any point)
- **120-02-D18:** create_login_dialog().open() replaces custom dialog + navigate.to('/settings') — Lab state preserved on anonymous sign-in
- **120-02-D11:** _stop_requested flag checked BEFORE generation check in _make_progress_cb — InterruptedError fires while _should_apply_results still True, so partials applied on user Stop (not discarded)
- [Phase ?]: test summary
- [Phase ?]: confirmed_only=False in Lab known-joins: proposed joins visible immediately post-insert via force_refresh=True cache bypass
- [Phase ?]: safe_user_pop one-shot descriptor with TTL=900s and 4 R2-M2 guards (schema, TTL, logged-in, anchor match); prevents double-fire on subsequent page loads
- [Phase ?]: on_selection_change=None default in create_candidate_table; export NOT selection-scoped per Plan 06 (full filtered set)
- [Phase ?]: Phase 120 Plan 07 Compare+Workbench
- [Phase ?]: Phase 120 Plan 07
- [Phase ?]: Phase 120 Plan 07
- [Phase ?]: Phase 120 Plan 07
- [Phase ?]: Phase 120 Plan 07
- [Phase ?]: 121-01 HE key gap closure

## Operator Next Steps

- Phase 117 is COMPLETE (all 6 plans + integration plan 04 done). Run `/gsd-discuss-phase 118` to start Phase 118 (Joins, Entry & Full Builders).
