# Phase 74: Page-Scoped State Refactor - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-16
**Phase:** 74-page-scoped-state-refactor
**Areas discussed:** Persistence boundary + write pattern (GA1+GA2), asyncio.ensure_future strategy (GA3), Refactor scope ceiling + browse bootstrap (GA4), Test investment
**Discussion basis:** External Codex review (`74-CODEX-REVIEW.md`) consulted ahead of this session; user invoked `/gsd-discuss-phase 74 see codex review` to ratify or challenge the recommendations.

---

## GA1+GA2 — Persistence Boundary + Write Pattern

| Option | Description | Selected |
|--------|-------------|----------|
| Adopt as-is (Recommended) | Three buckets (runtime_only / restorable_page_snapshot / cross_page_preference) + serializer helpers as the only place that touches storage keys. Keep legacy key format (no migration). Version page-local keys to handle tab collision. Matches existing module-level style at search.py:95, browse.py:1056, filter_panel.py:205. | ✓ |
| Buckets only, defer helpers | Formalize the taxonomy and document it, but leave existing direct app.storage.user writes scattered. Smaller blast radius; less of the WEBM-03 win. | |
| Binary split | Just runtime vs persistent — skip the cross_page_preference bucket. Simpler but loses the distinction that prevents prefs (show_translations) from being treated like snapshots (search_results). | |

**User's choice:** Adopt as-is (Recommended)
**Notes:** Codex's framing of `search_results` and `browse_position` as snapshots, not preferences, is the load-bearing distinction. Tab-collision risk noted — page-local keys versioned + treated as disposable cache. Property-descriptor magic (Codex's W2) explicitly rejected upstream by Codex; not re-litigated here.

---

## GA3 — asyncio.ensure_future Strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Adopt as-is (Recommended) | Convert all Cat-1 wrappers (drop ensure_future on event handlers, return the coroutine and let NiceGUI schedule). Keep Cat-2 (deferred init with client rebinding) explicit. Cat-3 (long-running) as owned task via asyncio.create_task. Touch Cat-4 (background_tasks.create) only if a single helper emerges. | ✓ |
| Cat-1 only | Convert Cat-1 handlers, leave Cat-2/3/4 untouched. Tightest scope, still captures the behavior-restoring win. | |
| Full conversion | Cat-1 + funnel Cat-4 through background_tasks.create() + modernize Cat-3 to create_task. Mild architectural win; expands risk surface. | |

**User's choice:** Adopt as-is (Recommended)
**Notes:** The NiceGUI 3.8.0 finding from Codex is the key insight — `on_click=lambda: asyncio.ensure_future(load_page(...))` returns a `Task` and bypasses NiceGUI's awaitable scheduling path. So Cat-1 cleanup is behavior-restoring, not cosmetic. This is the highest-leverage piece of WEBM-03's async work and likely fixes the URL-bar-not-updating bug class.

---

## GA4 — Refactor Scope Ceiling + Browse Bootstrap

| Option | Description | Selected |
|--------|-------------|----------|
| Adopt as-is (Recommended) | S2 scope + new web/browse_bootstrap.py with resolve_browse_bootstrap() helper. Mirrors the search precedent that already has tests/test_search_bootstrap.py. Codex flags this as the natural place for sys_id-vs-saved-position precedence. | ✓ |
| S2 without browse bootstrap | Same persistence + Cat-1 sweep but leave browse precedence logic inline in browse.py for now. Smaller diff; misses an opportunity while we're already touching browse state. | |
| S1 full sweep | Persistence + ALL ensure_future categories + browse bootstrap. Codex calls this too wide for the riskiest phase in the milestone. | |
| S3 minimal | Page-scoped objects only, leave persistence writes scattered. Codex: 'does not really satisfy WEBM-03 — storage stays an active state authority.' | |

**User's choice:** Adopt as-is (Recommended)
**Notes:** Browse bootstrap extraction added as a fifth in-scope item (Codex identified this as an emergent gray area). Mirrors `web/search_bootstrap.py` + `tests/test_search_bootstrap.py`. Target is the precedence logic embedded around `browse.py:4471`.

---

## Test Investment

| Option | Description | Selected |
|--------|-------------|----------|
| All three (Recommended) | (1) Pure search snapshot round-trip — proves volatile fields excluded, restorable fields survive. (2) Browse bootstrap precedence — explicit sys_id beats saved browse_position; blank /browse restores; reading-desk wins only when intended. (3) E2E assertion: page navigation updates URL bar (the known detached-task bug class). | ✓ |
| Round-trip + precedence only | Skip the E2E URL-bar test. Faster to write; relies on Phase 75 manual smoke for the URL-bar regression. | |
| Smoke only | Rely on existing pytest baseline + Phase 74 web smoke check from success criteria. No new unit tests. Lowest investment, highest residual risk. | |

**User's choice:** All three (Recommended)
**Notes:** The URL-bar E2E test specifically proves Cat-1 cleanup achieved its behavioral goal. Codex's framing: "this phase is mostly about serialization boundaries and precedence rules, which are cheap to test and hard to smoke manually."

---

## Closing Check

| Option | Description | Selected |
|--------|-------------|----------|
| Ready for context (Recommended) | Codex review covers the gray areas comprehensively. Write CONTEXT.md and proceed to plan-phase. | ✓ |
| Explore more gray areas | Surface 2-3 additional implementation gray areas Codex didn't cover (e.g., commit granularity, plan splitting, rollout sequencing). | |

**User's choice:** Ready for context (Recommended)
**Notes:** Plan splitting and commit granularity left to Claude's discretion in CONTEXT.md (D-section "Claude's Discretion").

---

## Claude's Discretion

- Exact field membership in each persistence bucket (per-file audit during research)
- Per-file commit granularity inside each plan
- Whether `restore_search_snapshot` is one function or splits into per-section helpers
- Exact signature of `resolve_browse_bootstrap` (match `resolve_search_bootstrap` shape)
- Whether `filter_panel.py`'s 30 `app.storage.user` calls fold into search snapshot helpers or get their own helper
- Plan splitting strategy (single mega-plan vs. three: persistence / async / browse-bootstrap+tests)

## Deferred Ideas

- Storage format migration to single namespaced key per page (Codex's W3) — defer to a future low-risk phase
- Cat-4 full sweep (funnel all `background_tasks.create()` through one helper) — architectural mild win, not required by WEBM-03
- `SearchPageController` / `BrowsePageController` class refactor — out of scope for WEBM-03 (decomposition + state, not redesign)
