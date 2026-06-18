---
gsd_state_version: 1.0
milestone: v8.2.0
milestone_name: Web Joins Lab
status: verifying
stopped_at: Completed 118-05-PLAN.md
last_updated: "2026-06-18T10:46:45.599Z"
last_activity: 2026-06-18
progress:
  total_phases: 5
  completed_phases: 2
  total_plans: 12
  completed_plans: 12
  percent: 40
---

# Project State

## Project Reference

See: .planning/PROJECT.md

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 118 — joins-entry-full-builders

## Current Position

Phase: 118 (joins-entry-full-builders) — EXECUTING
Plan: 5 of 5
Status: Phase complete — ready for verification
Last activity: 2026-06-18

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

Last session: 2026-06-18T10:46:34.883Z
Stopped at: Completed 118-05-PLAN.md
Resume file: None
Next step: `/gsd:plan-phase 117` (Vertical Spine)

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

## Operator Next Steps

- Phase 117 is COMPLETE (all 6 plans + integration plan 04 done). Run `/gsd-discuss-phase 118` to start Phase 118 (Joins, Entry & Full Builders).
