# Phase 74 — Codex External Review

**Date:** 2026-04-16
**Consulted on:** Four gray areas for Phase 74 (Page-Scoped State Refactor)
**NiceGUI version note:** Repo pinned to `nicegui==3.8.0` (requirements.txt:9, requirements-lock.txt:54) — plan against 3.8.0 behavior, not 2.x.

---

## Summary of Codex's position

The cleanest Phase 74 shape is: make the page object authoritative, push persistence behind explicit snapshot helpers, and do an aggressive Cat-1 async cleanup because those `ensure_future` wrappers are not neutral in NiceGUI.

---

## GA1 — Persistence boundary rule

**Pick: Rule A, but formalize it as three buckets, not one binary split.**

`SearchUIState` / `BrowseState` stay authoritative during page lifetime; storage is only hydrate/serialize. Classify fields as:

- **`runtime_only`** — `progress`, `is_running`, task handles, expanded card, current page objects
- **`restorable_page_snapshot`** — `query`, filters, refinement chain, exclusions, `search_results`, `browse_position`, `reading_desk_state`
- **`cross_page_preference`** — `show_translations`, `session_persistence_enabled`, search history/settings

`search_results` and `browse_position` are **not preferences**; they are serialized snapshots of live page state.

**Sharpest counterargument:** Taxonomy work up front. But that taxonomy is exactly what keeps this from becoming an accidental state framework.

**Underweighted gray area — tab collision:** Anything page-local kept in `app.storage.user` can still be stomped by a second tab. Treat those keys as disposable caches and version them.

---

## GA2 — Write pattern

**Pick: W1 (single save helper), implemented as page-specific serializer/deserializer helpers, not property magic.**

Concrete API:
- `restore_search_snapshot(...)`, `persist_search_snapshot(...)`, `clear_search_snapshot(...)`
- Same for browse

Keep the storage format **legacy key-based** for Phase 74 — avoids migration blast radius. Make the helpers the **only place** allowed to touch those keys. Matches module-level style already in search.py:95, browse.py:1056, filter_panel.py:205.

**W2 is a trap on a 100-field class:** hidden I/O, hard-to-batch resets, hard to suppress during restore. You'd end up inventing `suspend_persistence()` and dirty flags — exactly the framework that's out of scope.

**W3 is a good future end-state, not a good Phase 74 move.** Get the serializer boundary first; switch the backing format in a later low-risk phase if still desired.

---

## GA3 — `asyncio.ensure_future` strategy

**Convert all Cat-1 wrappers now, keep Cat-2 explicit, keep Cat-3 as an owned task handle.**

**Important NiceGUI 3.8.0 detail:** `on_click=lambda: load_page(...)` is fine because NiceGUI schedules awaitable return values itself. But `on_click=lambda: asyncio.ensure_future(load_page(...))` **bypasses that path** because it returns a `Task`. That is why **Cat-1 is not cosmetic cleanup; it is changing behavior back toward the framework default**.

**Cat-2 (deferred init with client rebinding):** No hidden NiceGUI idiom removes the need for client/container rebinding. The real idiom is "keep a container/client ref and enter it explicitly." `background_tasks.create()` does **not** restore slot/client context by magic.

**Cat-3:** Change to `asyncio.create_task(...)` if you want a cleaner modern API.

**Cat-4 (`background_tasks.create()` vs raw `ensure_future`):** Operationally better (framework's own path, logs exceptions), but architecturally a mild win. Does not solve overlap/race semantics. Only touch if you can funnel through one helper.

**Spend risk budget on Cat-1 first.**

---

## GA4 — Refactor scope ceiling

**Pick: S2 (targeted).**

- S1 is too wide for the riskiest phase in the milestone
- S3 does not really satisfy the roadmap — scattered writes would still make storage an active state authority

**Codex's version of S2:**
- Full sweep on the persistence boundary
- Full sweep on Cat-1 handlers
- Keep Cat-2/Cat-3 with explicit justification
- Only opportunistically clean Cat-4

**Strongest counterargument:** Leaves mixed async styles behind. True, but only in places where detached lifetime is actually part of the behavior.

**Added gray area — browse bootstrap:** Search has `resolve_search_bootstrap(...)` and `tests/test_search_bootstrap.py`. Browse's initial restore logic in browse.py:4471 deserves the same treatment. Add a small pure `resolve_browse_bootstrap(...)` helper rather than leaving precedence logic embedded in the page body.

---

## Test investment

**Worth it — add session-restore round-trip unit tests.** This phase is mostly about serialization boundaries and precedence rules, which are cheap to test and hard to smoke manually.

**Minimum useful additions:**
1. Pure search snapshot round-trip test — proves volatile fields are excluded and restorable fields survive
2. Browse bootstrap precedence test — explicit `sys_id` beats saved `browse_position`, blank `/browse` restores it, reading-desk restore only wins in intended cases
3. One E2E assertion added to existing browse flow — page navigation updates the URL bar (the known detached-task failure mode this phase should kill)

---

## References

NiceGUI sources Codex used:
- `nicegui/events.py` — https://raw.githubusercontent.com/zauberzeug/nicegui/main/nicegui/events.py
- `nicegui/elements/button.py` — https://raw.githubusercontent.com/zauberzeug/nicegui/main/nicegui/elements/button.py
- Background page tasks guidance — https://github.com/zauberzeug/nicegui/discussions/2729
- Client-context discussion — https://github.com/zauberzeug/nicegui/discussions/2026

Repo pins:
- `requirements.txt:9` — `nicegui==3.8.0`
- `requirements-lock.txt:54`

Existing module-level persistence style:
- `web/pages/search.py:95` — session restore reads
- `web/pages/browse.py:1056` — reading desk save
- `web/components/filter_panel.py:205` — filter persistence
- `web/pages/browse.py:4471` — browse bootstrap logic (target for extraction)
- `web/search_bootstrap.py` + `tests/test_search_bootstrap.py` — precedent pattern

---

*Saved for next /gsd-discuss-phase 74 session to consume.*
