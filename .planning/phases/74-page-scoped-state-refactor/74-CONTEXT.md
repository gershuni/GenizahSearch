# Phase 74: Page-Scoped State Refactor - Context

**Gathered:** 2026-04-16
**Status:** Ready for planning
**Discussion basis:** External Codex review (`74-CODEX-REVIEW.md`) consulted on all four gray areas; user ratified all recommendations.

<domain>
## Phase Boundary

Reduce `app.storage.user` sprawl and detached `asyncio.ensure_future` calls in `web/pages/search.py`, `web/pages/browse.py`, and `web/components/filter_panel.py` by formalizing a persistence boundary and converting async handlers back to NiceGUI 3.8.0 framework defaults where appropriate. Phases 72–73 already extracted `SearchUIState` / `BrowseState` / `*PageRefs` — Phase 74 changes runtime data flow on top of those structural seams.

In scope:
- **Persistence boundary:** classify every page-state field into one of three buckets (runtime_only / restorable_page_snapshot / cross_page_preference). Route ALL writes for snapshot fields through page-specific serializer helpers. Keep legacy storage key format (no migration).
- **New helpers in `web/pages/search_state.py`:** `restore_search_snapshot(...)`, `persist_search_snapshot(...)`, `clear_search_snapshot(...)` as the only place that touches search snapshot keys.
- **New helpers in `web/pages/browse_state.py`:** equivalent `restore_/persist_/clear_browse_snapshot(...)` helpers.
- **New module `web/browse_bootstrap.py`** with `resolve_browse_bootstrap(...)` mirroring `web/search_bootstrap.py` — extracts the precedence logic currently embedded around `browse.py:4471` (explicit `sys_id` vs saved `browse_position` vs reading-desk restore).
- **Cat-1 ensure_future cleanup (full sweep):** convert `on_click=lambda: asyncio.ensure_future(load_page(...))` patterns to `on_click=lambda: load_page(...)`. NiceGUI 3.8 schedules awaitable return values itself; wrapping in `ensure_future` returns a `Task` and bypasses that path. This is behavior-restoring, not cosmetic.
- **Tab-collision hardening:** treat any page-local key surviving in `app.storage.user` as a disposable cache and version it so a second tab's writes don't silently corrupt restore.
- **Tests:** add three round-trip / precedence / E2E tests (see `<decisions>` Tests section).

Out of scope:
- Cat-2 (deferred init with client/container rebinding) — keep explicit with comment justification. No NiceGUI idiom removes the need.
- Cat-3 (long-running owned tasks) — may opportunistically modernize to `asyncio.create_task` but not required.
- Cat-4 (`background_tasks.create()` vs raw `ensure_future`) — only touch if a single helper emerges naturally.
- Storage format migration to a single namespaced key per page (Codex's W3) — better as a future low-risk phase once the serializer boundary exists.
- `SearchPageController` / `BrowsePageController` class refactor — architectural redesign, not what WEBM-03 asks for.
- Any behavior change, styling tweak, or feature addition.

</domain>

<decisions>
## Implementation Decisions

### GA1 — Persistence Boundary Rule (Codex's Rule A, three-bucket form)

- **D-01:** Classify every page-state field into one of three buckets:
  - **`runtime_only`** — never persisted: `progress`, `is_running`, task handles, expanded card, current page objects, transient UI flags
  - **`restorable_page_snapshot`** — page-local persistent state, treated as disposable cache: `query`, filters, refinement chain, exclusions, `search_results`, `browse_position`, `reading_desk_state`
  - **`cross_page_preference`** — true user preferences: `show_translations`, `session_persistence_enabled`, search history/settings
- **D-02:** `search_results` and `browse_position` are NOT preferences — they are serialized snapshots of live page state. Bucket them as `restorable_page_snapshot`.
- **D-03:** `SearchUIState` / `BrowseState` are authoritative during page lifetime. Storage layer is hydrate-on-mount and serialize-on-change only — no live reads from `app.storage.user` after restore.
- **D-04:** Tab collision hardening: any `restorable_page_snapshot` key kept in `app.storage.user` gets a schema version stamp and is treated as a disposable cache (a second tab stomping it must not break the surviving tab).

### GA2 — Write Pattern (Codex's W1, page-specific helpers)

- **D-05:** Single save-helper pattern, implemented as page-specific serializer/deserializer functions — NOT property descriptors / `__setattr__` magic.
- **D-06:** Concrete API in `search_state.py`:
  - `restore_search_snapshot(state: SearchUIState) -> None` — hydrate from `app.storage.user` keys
  - `persist_search_snapshot(state: SearchUIState) -> None` — serialize restorable fields only
  - `clear_search_snapshot() -> None` — wipe search snapshot keys
- **D-07:** Equivalent triple in `browse_state.py`: `restore_browse_snapshot`, `persist_browse_snapshot`, `clear_browse_snapshot`.
- **D-08:** Storage format stays **legacy key-based** for Phase 74 — same key names that exist today. The helpers are the ONLY place allowed to touch those keys. Avoids migration blast radius. Matches existing module-level style at `web/pages/search.py:95`, `web/pages/browse.py:1056`, `web/components/filter_panel.py:205`.
- **D-09:** Property-descriptor magic (W2) explicitly rejected: hidden I/O, hard to batch resets, hard to suppress during restore — leads inevitably to `suspend_persistence()` and dirty-flag machinery, which is the state-management framework that's out of scope.

### GA3 — asyncio.ensure_future Strategy

- **D-10:** **Cat-1 (event handler wrappers)** — full sweep. Convert `on_click=lambda: asyncio.ensure_future(load_page(...))` to `on_click=lambda: load_page(...)`. **Important NiceGUI 3.8.0 detail:** wrapping in `ensure_future` returns a `Task`, which bypasses NiceGUI's awaitable scheduling path. So Cat-1 cleanup is **behavior-restoring**, not cosmetic. This is the highest-leverage piece of WEBM-03's async work.
- **D-11:** **Cat-2 (deferred init with client/container rebinding)** — keep explicit. Add a one-line comment at each surviving site explaining why detached scheduling is required (e.g., "deferred to next event loop tick to allow container to mount"). `background_tasks.create()` does NOT restore slot/client context by magic; the real idiom is "keep a container/client ref and enter it explicitly," which the existing code already does.
- **D-12:** **Cat-3 (long-running owned tasks)** — keep as owned task handle. May modernize to `asyncio.create_task(...)` for cleaner API; not required.
- **D-13:** **Cat-4 (`background_tasks.create()` vs raw `ensure_future`)** — opportunistic. Operationally better (NiceGUI's own path, logs exceptions) but architecturally a mild win. Touch only if a single helper funnels them naturally.

### GA4 — Refactor Scope Ceiling (Codex's S2 Targeted)

- **D-14:** Adopt **S2 targeted scope**:
  - Full sweep on the persistence boundary (D-01 through D-09)
  - Full sweep on Cat-1 handlers (D-10)
  - Keep Cat-2/Cat-3 with explicit justification (D-11, D-12)
  - Only opportunistically clean Cat-4 (D-13)
- **D-15:** S1 (full sweep on everything) explicitly rejected — too wide for the riskiest phase in the milestone.
- **D-16:** S3 (page-scoped objects only, scattered writes) explicitly rejected — would not satisfy WEBM-03; storage would still be an active state authority.
- **D-17:** **Browse bootstrap extraction** — create new module `web/browse_bootstrap.py` with pure `resolve_browse_bootstrap(...)` helper. Mirror the existing `web/search_bootstrap.py` + `tests/test_search_bootstrap.py` pattern. Extract the precedence logic currently embedded near `browse.py:4471` (explicit `sys_id` vs saved `browse_position` vs reading-desk restore).

### Tests (Codex's "minimum useful additions" — all three)

- **D-18:** **Search snapshot round-trip test** in `tests/test_search_state.py` (or extend existing). Construct a `SearchUIState`, populate runtime_only + restorable + preference fields, persist, hydrate into a fresh state, assert: runtime_only fields are pristine defaults, restorable fields match, preferences match.
- **D-19:** **Browse bootstrap precedence test** in new `tests/test_browse_bootstrap.py`. Cases: (a) explicit `sys_id` in URL beats saved `browse_position`, (b) blank `/browse` restores saved `browse_position`, (c) reading-desk restore wins only in the intended scenario. Mirror `tests/test_search_bootstrap.py` structure.
- **D-20:** **URL-bar update E2E assertion** added to existing browse flow test. Asserts that page navigation updates the URL bar — this is the known detached-task failure mode that Cat-1 cleanup should kill (Codex flagged it as the test that proves the async fix works).

### Verification

- **D-21:** `pytest tests/` baseline must remain green (current 1067 passed, 8 skipped per Phase 75 success criteria).
- **D-22:** Web smoke check from ROADMAP success criterion #5: app starts; `/` loads; basic search returns results; `/browse` loads for at least one manuscript; shelfmark navigation between manuscripts works.
- **D-23:** CI green (Ubuntu + Windows matrix per `.github/workflows/ci.yml`).
- **D-24:** Manual cross-tab test: open same `/browse` URL in two tabs, navigate independently, confirm no stomping or restore corruption (validates D-04 versioning).

### Claude's Discretion

- Exact field membership in each bucket — derived from per-file audit during research; codex named the obvious cases.
- Per-file commit granularity inside each plan.
- Whether `restore_search_snapshot` is a single function or splits into `restore_query`, `restore_filters`, `restore_results` etc. — depends on what reads cleanly.
- Exact signature of `resolve_browse_bootstrap` — match `resolve_search_bootstrap` shape.
- Whether `filter_panel.py`'s 30 `app.storage.user` calls are addressed as part of search snapshot helpers (most likely) or get their own helper.
- Plan splitting (single mega-plan vs. three: persistence / async / browse-bootstrap+tests).

### Folded Todos

None — pending todos in STATE.md (corrections fetch migration, CUT-01 PGP cleanup, date-range filter, creation-type filter) are orthogonal to this phase.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Roadmap & Requirements
- `.planning/ROADMAP.md` — Phase 74 entry, success criteria, "most architectural web change" warning
- `.planning/REQUIREMENTS.md` — WEBM-03
- `.planning/PROJECT.md` — v7.9 Active milestone
- `.planning/STATE.md` — WEBM-03 architectural risk note (Phases 72–73 stability prerequisite)

### Phase 74 Inputs
- `.planning/phases/74-page-scoped-state-refactor/74-CODEX-REVIEW.md` — external review that informed every decision in this CONTEXT.md
- `.planning/phases/74-page-scoped-state-refactor/74-DISCUSSION-LOG.md` — Q&A audit trail (companion to this file)

### Prior Phase CONTEXT.md Files (locked decisions)
- `.planning/phases/72-search-page-split/72-CONTEXT.md` — `SearchUIState`, `SearchPageRefs`, `search_state.py` location and shape
- `.planning/phases/73-browse-page-split/73-CONTEXT.md` — `BrowseState`, `BrowsePageRefs`, `browse_state.py` / `browse_enrichment.py` shape

### Source — Subjects of the Phase
- `web/pages/search.py` — 22 `ensure_future` calls, 46 `app.storage` references
- `web/pages/browse.py` — 19 `ensure_future` calls, 13 `app.storage` references
  - `browse.py:1056` — reading-desk save (existing module-level persistence style)
  - `browse.py:4471` — browse bootstrap precedence logic (extraction target → `web/browse_bootstrap.py`)
- `web/pages/search_state.py:95` — session restore reads (extend with new snapshot helpers)
- `web/pages/search_state.py:209` — `session_persistence_enabled` gate
- `web/pages/browse_state.py` — currently no `app.storage` reads (target for new browse snapshot helpers)
- `web/pages/browse_enrichment.py` — 1 `ensure_future` (audit during execution)
- `web/components/filter_panel.py` — 10 `ensure_future` calls, 30 `app.storage.user` references (most likely fold into search snapshot helpers)

### Existing Patterns (mirror these)
- `web/search_bootstrap.py` — pure precedence helper module; `resolve_browse_bootstrap` mirrors this
- `tests/test_search_bootstrap.py` — test structure; `tests/test_browse_bootstrap.py` mirrors this
- `shared/refinement.py`, `shared/exclusion_service.py` — extracted-helper precedent

### NiceGUI 3.8.0 References (from Codex review)
- `requirements.txt:9` — `nicegui==3.8.0` pin
- `requirements-lock.txt:54` — version lock
- `nicegui/events.py` — awaitable scheduling path that Cat-1 wrappers bypass
  - https://raw.githubusercontent.com/zauberzeug/nicegui/main/nicegui/events.py
- `nicegui/elements/button.py` — on_click handler dispatch
  - https://raw.githubusercontent.com/zauberzeug/nicegui/main/nicegui/elements/button.py
- Background tasks discussion — https://github.com/zauberzeug/nicegui/discussions/2729
- Client-context discussion — https://github.com/zauberzeug/nicegui/discussions/2026

### CI & Verification
- `.github/workflows/ci.yml` — Ubuntu + Windows matrix
- `tests/` — baseline 1067 passed, 8 skipped (per Phase 75 success criteria)

</canonical_refs>

<code_context>
## Existing Code Insights

### Persistence Currently Scattered, Not Centralized
`app.storage.user` reads/writes happen at module level across at least three files (search.py:95, browse.py:1056, filter_panel.py:205). No single owner of the storage key namespace. The new `restore_/persist_/clear_*_snapshot` helpers become the sole owners.

### `search_bootstrap.py` Precedent
`web/search_bootstrap.py` already extracts pure precedence logic for the search page bootstrap (URL params vs saved state). It has dedicated unit tests in `tests/test_search_bootstrap.py`. The browse equivalent (`web/browse_bootstrap.py` + `tests/test_browse_bootstrap.py`) is a direct copy of this pattern applied to `browse.py:4471`.

### `SearchUIState` / `BrowseState` Already Page-Scoped
Phases 72–73 already created the page-scoped state objects. Phase 74 doesn't create new state classes — it changes how those objects relate to storage (snapshot serialization rather than scattered key writes) and how their methods are scheduled (NiceGUI awaitable return rather than detached `ensure_future`).

### NiceGUI 3.8 Awaitable Scheduling
`on_click=lambda: load_page(...)` is the framework idiom: NiceGUI sees the awaitable return value and schedules it on the page's client context. `on_click=lambda: asyncio.ensure_future(load_page(...))` returns a `Task` instead, bypassing that path. The Codex review identified this as the root cause of the URL-bar-not-updating bug class — Cat-1 cleanup is the fix.

### Cat-2 Pattern Cannot Be Eliminated
Container/client rebinding for deferred init (e.g., `await asyncio.sleep(0); with container: ...`) is required by NiceGUI's slot/client context model. Cat-2 sites get a comment justification rather than removal.

</code_context>

<specifics>
## Specific Ideas

- **Three-bucket taxonomy** — codex's framing prevents this from drifting into "all state goes into one big serializer." Each field has a clear home before it gets written.
- **Helpers as namespace owners** — only `restore_/persist_/clear_*_snapshot` may touch the storage keys for snapshot fields. Direct `app.storage.user[...]` writes for snapshot fields are forbidden after this phase.
- **Browse bootstrap as a copy of search bootstrap** — not a new pattern, a direct mirror. The test file mirrors `tests/test_search_bootstrap.py` case-by-case.
- **URL-bar E2E test as the proof Cat-1 worked** — single assertion that navigation updates the URL. Tight, specific, regression-catching.

</specifics>

<deferred>
## Deferred Ideas

### For a Future Low-Risk Phase
- **Storage format migration to single namespaced key per page** (Codex's W3) — switch from many legacy keys to one JSON blob per page. Better end state, but only safe once the serializer boundary exists. Defer until a quiet milestone window.
- **Cat-4 full sweep** — funnel all `background_tasks.create()` and remaining `ensure_future` through one helper. Architectural mild win; not required by WEBM-03.
- **`SearchPageController` / `BrowsePageController` class refactor** — methods on a controller solve the UI-reference problem naturally. Out of scope for WEBM-03 (which is decomposition + state, not redesign).

### Reviewed Todos (not folded)
None — no todos matched this phase's scope.

</deferred>

---

*Phase: 74-page-scoped-state-refactor*
*Context gathered: 2026-04-16*
