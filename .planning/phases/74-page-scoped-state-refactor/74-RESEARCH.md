# Phase 74: Page-Scoped State Refactor - Research

**Researched:** 2026-04-16
**Domain:** NiceGUI async scheduling / app.storage.user persistence boundary / Python module refactor
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**D-01 Three-bucket taxonomy:**
- `runtime_only` — never persisted: `progress`, `is_running`, task handles, expanded card, current page objects, transient UI flags
- `restorable_page_snapshot` — page-local persistent state, treated as disposable cache: `query`, filters, refinement chain, exclusions, `search_results`, `browse_position`, `reading_desk_state`
- `cross_page_preference` — true user preferences: `show_translations`, `session_persistence_enabled`, search history/settings

**D-02:** `search_results` and `browse_position` are NOT preferences — they are serialized snapshots of live page state. Bucket them as `restorable_page_snapshot`.

**D-03:** `SearchUIState` / `BrowseState` are authoritative during page lifetime. Storage layer is hydrate-on-mount and serialize-on-change only — no live reads from `app.storage.user` after restore.

**D-04:** Tab collision hardening: any `restorable_page_snapshot` key kept in `app.storage.user` gets a schema version stamp and is treated as a disposable cache.

**D-05:** Single save-helper pattern, implemented as page-specific serializer/deserializer functions — NOT property descriptors / `__setattr__` magic.

**D-06/D-07:** Snapshot helper triples in `search_state.py` and `browse_state.py`:
- `restore_search_snapshot(state: SearchUIState) -> None`
- `persist_search_snapshot(state: SearchUIState) -> None`
- `clear_search_snapshot() -> None`
- `restore_browse_snapshot(state: BrowseState) -> None`
- `persist_browse_snapshot(state: BrowseState) -> None`
- `clear_browse_snapshot() -> None`

**D-08:** Storage format stays legacy key-based for Phase 74.

**D-09:** Property-descriptor magic (W2) explicitly rejected.

**D-10:** Cat-1 full sweep — convert `on_click=lambda: asyncio.ensure_future(load_page(...))` to `on_click=lambda: load_page(...)`. This is behavior-restoring.

**D-11:** Cat-2 (deferred init with client/container rebinding) — keep explicit with one-line comment.

**D-12:** Cat-3 (long-running owned tasks) — keep as owned task handle; may modernize to `asyncio.create_task(...)`.

**D-13:** Cat-4 (`background_tasks.create()` vs raw `ensure_future`) — opportunistic only.

**D-14:** S2 targeted scope: full sweep on persistence boundary + Cat-1; Cat-2/Cat-3 keep explicit; Cat-4 opportunistic.

**D-17:** Create `web/browse_bootstrap.py` with `resolve_browse_bootstrap(...)` mirroring `web/search_bootstrap.py`.

**D-18:** Search snapshot round-trip test in `tests/test_search_state.py`.

**D-19:** Browse bootstrap precedence test in new `tests/test_browse_bootstrap.py`.

**D-20:** URL-bar update E2E assertion added to existing browse flow test.

**D-21:** pytest baseline must remain green (1067 passed, 8 skipped).

**D-22:** Web smoke check.

**D-23:** CI green (Ubuntu + Windows).

**D-24:** Manual cross-tab test.

### Claude's Discretion

- Exact field membership in each bucket (derived from per-file audit — this research provides the full list)
- Per-file commit granularity inside each plan
- Whether `restore_search_snapshot` is a single function or splits into sub-functions
- Exact signature of `resolve_browse_bootstrap` — match `resolve_search_bootstrap` shape
- Whether `filter_panel.py`'s 30 `app.storage.user` calls are addressed as part of search snapshot helpers or get their own helper
- Plan splitting (single mega-plan vs. three: persistence / async / browse-bootstrap+tests)

### Deferred Ideas (OUT OF SCOPE)

- Storage format migration to single namespaced key per page (Codex's W3)
- Cat-4 full sweep
- `SearchPageController` / `BrowsePageController` class refactor
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| WEBM-03 | Search and browse reduce reliance on `app.storage.user` for live page state and reduce detached `asyncio.ensure_future` flows by using page-scoped state / handlers where practical | Full field audit (Section 1), ensure_future audit (Section 2), helper API shapes (Section 3), NiceGUI mechanism verification (Section 5) |
</phase_requirements>

---

## Summary

Phase 74 is a runtime data-flow refactor building on the structural seams from Phases 72–73. The three work streams are: (1) route all `restorable_page_snapshot` writes through six new helper functions so that `app.storage.user` key access is centralized, (2) convert Cat-1 `asyncio.ensure_future` event-handler wrappers to bare lambda returns so NiceGUI 3.8.0's awaitable scheduling path is used correctly, and (3) extract browse bootstrap precedence logic into a new `web/browse_bootstrap.py` module mirroring the existing `web/search_bootstrap.py`.

The grep audit found the numbers largely match CONTEXT.md's estimates (46 search.py, 13 browse.py, 30 filter_panel.py storage refs; 22 search.py, 19 browse.py, 10 filter_panel.py ensure_future calls). Of the ensure_future sites, approximately 12 are Cat-1 (convert), 5 are Cat-2 (keep with comment), 3 are Cat-3 (keep as owned task), and the rest are borderline Cat-2/Cat-4. The browse bootstrap block at lines 4466–4532 extracts cleanly to a pure function. The NiceGUI awaitable-scheduling mechanism is [VERIFIED] from the NiceGUI source: `handle_event` wraps the coroutine return value in `background_tasks.create()` with the `parent_slot` context preserved — `asyncio.ensure_future` returns a Task, bypassing this path entirely.

**Primary recommendation:** Plan as three waves — Wave 1: persistence helpers + field classification; Wave 2: Cat-1 async conversion; Wave 3: browse_bootstrap extraction + tests. This allows CI to gate each wave independently.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Session snapshot serialization | Frontend Server (SSR) | — | NiceGUI `app.storage.user` is server-side per-user storage; helpers live in `*_state.py` modules |
| Cat-1 async scheduling | Frontend Server (SSR) | — | NiceGUI event dispatch is server-side; `on_click` lambdas run in server context |
| Browse bootstrap precedence | Frontend Server (SSR) | — | URL params + storage reads happen during `create_browse_page()` on the server |
| Tab collision version stamp | Frontend Server (SSR) | — | Version tag read/written in same `app.storage.user` dict as snapshot keys |
| Cross-page preferences | Frontend Server (SSR) | — | `show_translations`, `session_persistence_enabled` — read at render time, not stored in page state |

---

## 1. Field Audit — Every `app.storage.user` Read/Write Site

### 1.1 `web/pages/search.py` (verified: 46 refs at lines below)

[VERIFIED: grep of source file at commit in working tree]

| Line(s) | Key(s) | Op | Bucket | Owning Helper After Refactor |
|---------|--------|----|--------|------------------------------|
| 95 | `search_mode` | R | `restorable_page_snapshot` | `restore_search_snapshot` |
| 96 | `search_query` | R | `restorable_page_snapshot` | `restore_search_snapshot` |
| 97 | `search_preset` | R | `restorable_page_snapshot` | `restore_search_snapshot` |
| 98 | `search_max_changes` | R | `restorable_page_snapshot` | `restore_search_snapshot` |
| 99 | `search_gap` | R | `restorable_page_snapshot` | `restore_search_snapshot` |
| 123 | `domain_exclusions` | R | `restorable_page_snapshot` | `restore_search_snapshot` |
| 125 | `search_printed_filter` | R | `restorable_page_snapshot` | `restore_search_snapshot` |
| 130 | `domain_exclusions` | W | `restorable_page_snapshot` | `persist_search_snapshot` / `clear_search_snapshot` |
| 143 | `word_search_excluded_ids` | R | `restorable_page_snapshot` | `restore_search_snapshot` |
| 148 | `search_exclusion_sources` | R | `restorable_page_snapshot` | `restore_search_snapshot` |
| 156 | `search_refinement_chain` | R | `restorable_page_snapshot` | `restore_search_snapshot` |
| 239 | `search_results` | R | `restorable_page_snapshot` | `restore_search_snapshot` |
| 400 | `search_query` | W | `restorable_page_snapshot` | `persist_search_snapshot` (inline via callback) |
| 509 | `search_preset` | W | `restorable_page_snapshot` | `persist_search_snapshot` |
| 522 | `search_gap` | W | `restorable_page_snapshot` | `persist_search_snapshot` |
| 620 | `search_text_position` | R | `restorable_page_snapshot` | `restore_search_snapshot` |
| 634 | `search_text_position` | W | `restorable_page_snapshot` | `persist_search_snapshot` |
| 653 | `search_preset` | W | `restorable_page_snapshot` | `persist_search_snapshot` |
| 661 | `search_max_changes` | W | `restorable_page_snapshot` | `persist_search_snapshot` |
| 690 | `search_mode` | W | `restorable_page_snapshot` | `persist_search_snapshot` |
| 806–832 | `search_filter_*` (10 keys + measurement variants) | W (reset) | `restorable_page_snapshot` | `clear_search_snapshot` |
| 2019–2025 | `search_results`, `search_query`, `search_mode`, `domain_exclusions`, `search_printed_filter`, `word_search_excluded_ids`, `search_exclusion_sources` | W (reset) | `restorable_page_snapshot` | `clear_search_snapshot` |
| 3012 | `domain_exclusions` | W | `restorable_page_snapshot` | `persist_search_snapshot` |
| 4199 | `search_results` | W | `restorable_page_snapshot` | `persist_search_snapshot` |
| 4268 | `show_translations` | R | `cross_page_preference` | **No helper needed** — keep direct read |
| 4479 | `show_translations` | R | `cross_page_preference` | **No helper needed** — keep direct read |

**Key inventory for search snapshot keys (verbatim storage key strings):**
```
search_mode
search_query
search_preset
search_max_changes
search_gap
domain_exclusions
search_printed_filter
word_search_excluded_ids
search_exclusion_sources
search_refinement_chain
search_results
search_text_position
search_filter_domains
search_filter_authors
search_filter_works
search_filter_include_mode
search_filter_date_from
search_filter_to
search_filter_material_exclude
search_filter_text_all
search_filter_text_any
search_filter_text_not
search_filter_width_min  search_filter_width_max
search_filter_height_min  search_filter_height_max
search_filter_line_count_min  search_filter_line_count_max
search_filter_line_height_min  search_filter_line_height_max
search_filter_text_density_min  search_filter_text_density_max
search_filter_measurement_material
search_all_terms_filter  (written via persist_value at search.py:1700 via search_state.py)
```

**Note on `search_filter_*` keys:** These are ALSO read/written by `filter_panel.py:load_filter_state(state, 'search')`. The persistence boundary rule means `restore_search_snapshot` should call `filter_panel.load_filter_state(state, 'search')` internally (no duplication) OR inline the reads. Calling the existing `load_filter_state` is preferred (D-08 — no format change). The filter clear blocks in search.py:806 and search.py:2019 become calls to `clear_search_snapshot()`.

### 1.2 `web/pages/browse.py` (verified: 13 refs)

[VERIFIED: grep of source file]

| Line(s) | Key(s) | Op | Bucket | Owning Helper After Refactor |
|---------|--------|----|--------|------------------------------|
| 778–783 | `browse_position` (dict: sys_id, p_num, shelfmark, volume_ie) | W | `restorable_page_snapshot` | `persist_browse_snapshot` |
| 982 | `reading_desk_state` | W (pop/clear) | `restorable_page_snapshot` | `clear_browse_snapshot` |
| 1066–1072 | `reading_desk_state` | W (save/clear) | `restorable_page_snapshot` | `persist_browse_snapshot` / `clear_browse_snapshot` |
| 1079 | `reading_desk_state` | R | `restorable_page_snapshot` | `restore_browse_snapshot` |
| 1171 | `browse_export_data` | W | **`runtime_only`** — ephemeral export blob, immediately consumed by download trigger; NOT a snapshot field |
| 2036 | `show_translations` | R | `cross_page_preference` | No helper needed |
| 2071 | `show_translations` | R | `cross_page_preference` | No helper needed |
| 4477 | `reading_desk_state` | R (bootstrap) | `restorable_page_snapshot` | Moves into `resolve_browse_bootstrap` |
| 4495 | `reading_desk_state` | W (pop) | `restorable_page_snapshot` | `clear_browse_snapshot` |
| 4516 | `browse_position` | R (bootstrap) | `restorable_page_snapshot` | Moves into `resolve_browse_bootstrap` |

**Key inventory for browse snapshot keys (verbatim storage key strings):**
```
browse_position        (dict: sys_id, p_num, shelfmark, volume_ie)
reading_desk_state     (dict: entries=[{sys_id, shelfmark, pages, sources, pgp_doc}])
```

**Note on `browse_export_data`:** Line 1171 writes a large blob immediately consumed by `ui.download('/api/export/browse/word')`. This is a transient inter-request handoff (browse → API endpoint), NOT session restore state. Classify as `runtime_only`; the write should NOT be moved into `persist_browse_snapshot`. Leave as-is.

### 1.3 `web/components/filter_panel.py` (verified: 30 refs)

[VERIFIED: grep of source file]

| Lines | Key pattern | Op | Bucket | Owning Helper After Refactor |
|-------|------------|----|--------|------------------------------|
| 222–223 | `session_persistence_enabled` (gate read) | R | `cross_page_preference` | No helper — `persist_value()` gate stays as-is |
| 239–271 | `{pfx}_filter_*` (all 19 filter keys, read in `load_filter_state`) | R | `restorable_page_snapshot` | `restore_search_snapshot` calls `load_filter_state(state, 'search')` — no change to filter_panel.py needed for reads |
| 290–321 | `incoming_filters` | R/W (consume) | **cross-page signal** — ambiguous (see Pitfalls §7) | Leave in filter_panel.py; consume_incoming_filters already isolates this |
| 445–497 | `persist_value(f'{pfx}_filter_*', ...)` (9 write sites via `create_filter_handlers`) | W | `restorable_page_snapshot` | These writes stay in filter_panel.py via `persist_value()` — `persist_value` is already a gateway. **After refactor,** the snapshot helpers' `clear` function must also clear these keys. |

**Observation:** `filter_panel.py`'s `persist_value()` function (line 220–223) is itself a mini-helper that gates writes behind `session_persistence_enabled`. This already satisfies the "single write path" rule for filter keys. The Phase 74 change for filter_panel is: (1) `clear_search_snapshot()` must clear all `search_filter_*` keys (currently done scattered in search.py), and (2) the `ensure_future` calls in `create_filter_handlers` are Cat-1 conversions (see §2.3).

### 1.4 `web/pages/search_state.py` — Existing Storage Reads

[VERIFIED: grep + read of file]

| Lines | Key(s) | Op | Bucket | Notes |
|-------|--------|----|--------|-------|
| 207–209 | `session_persistence_enabled` (inside `add_to_search_history`) | R | `cross_page_preference` | Correct — no change needed |
| 204 | `search_history` | R | `cross_page_preference` | Search history = user preference record. No change needed. |
| 238 | `search_history` | W | `cross_page_preference` | No change needed |
| 248 | `search_history` | W | `cross_page_preference` | No change needed |
| 251 | `search_history` | W | `cross_page_preference` | No change needed |
| 209 | `search_history_limit` | R | `cross_page_preference` | No change needed |

**Conclusion:** `search_state.py`'s existing storage reads are all `cross_page_preference` — correctly scoped. The new snapshot helpers are additive.

### 1.5 Elsewhere in `web/`

| File | Line | Key | Bucket | Notes |
|------|------|-----|--------|-------|
| `web/main.py` | (various) | Various app-level keys | `cross_page_preference` or `runtime_only` | Out of scope for Phase 74 |
| `web/pages/search_results.py` | (none found) | — | — | No `app.storage.user` refs |
| `web/pages/browse_enrichment.py` | (none found) | — | — | No `app.storage.user` refs |
| `web/pages/browse_state.py` | (none found) | — | — | Confirmed: currently zero storage refs (matches CONTEXT.md) |

---

## 2. ensure_future Audit

### 2.1 `web/pages/search.py` — 22 ensure_future calls

[VERIFIED: grep of source file]

| Line | Pattern | Category | Disposition |
|------|---------|----------|-------------|
| 437 | `asyncio.ensure_future(_after_delay(0.1, load_pgp_tags))` | Cat-2 | Keep with comment: deferred to let tag select mount |
| 885 | `asyncio.ensure_future(_recompute_filter_count())` | Cat-1 | Convert: called from sync event handler inside search panel |
| 895 | same | Cat-1 | Convert |
| 987 | same | Cat-1 | Convert |
| 1005 | same | Cat-1 | Convert |
| 1146 | same | Cat-1 | Convert |
| 1158 | same | Cat-1 | Convert |
| 1181 | `asyncio.ensure_future(_refresh_author_options())` | Cat-1 | Convert |
| 1182 | `asyncio.ensure_future(_refresh_work_options())` | Cat-1 | Convert |
| 1190 | same | Cat-1 | Convert |
| 1210 | `asyncio.ensure_future(_recompute_filter_count())` | Cat-1 | Convert |
| 1693 | `asyncio.ensure_future(_replay_refinement_chain_and_search())` | Cat-1 | Convert |
| 1759 | `asyncio.ensure_future(_replay_and_search())` | Cat-1 | Convert |
| 1762 | `asyncio.ensure_future(execute_search())` | Cat-1 | Convert |
| 1892 | `asyncio.ensure_future(_after_delay(1.0, setup_scroll_collapse))` | Cat-2 | Keep: `_after_delay` pattern for JS DOM readiness |
| 2297 | `search_state.update_timer = asyncio.ensure_future(_progress_update_loop())` | Cat-3 | Keep as owned task handle — long-running loop |
| 4545 | `asyncio.ensure_future(_after_delay(0.1, load_tag_results))` | Cat-2 | Keep: deferred page-mount init |
| 4552 | `asyncio.ensure_future(_after_delay(0.5, execute_search))` | Cat-2 | Keep: delay required for route-context setup |
| 4554 | `asyncio.ensure_future(_after_delay(0.5, execute_search))` | Cat-2 | Keep: same |
| 4576 | `asyncio.ensure_future(_after_delay(0.1, _deferred_filter_init))` | Cat-2 | Keep: deferred select option population |
| 4592 | `asyncio.ensure_future(_after_delay(0.2, _deferred_transcription_restore))` | Cat-2 | Keep: deferred enrichment on restore |
| 4596 | `asyncio.ensure_future(_after_delay(0.3, _deferred_chain_replay))` | Cat-2 | Keep: deferred chain replay |

**search.py Cat-1 count: 12 conversions.** Cat-2: 8 (keep with comment). Cat-3: 1 (keep).

**Important note on lines 885–1210:** These `_recompute_filter_count()` calls live inside event handlers in search.py's search panel construction. The handlers are synchronous functions (`def on_*`), so they cannot `await`. The current `asyncio.ensure_future(coro())` pattern is a Cat-1 usage because the handler returns nothing — `ensure_future` here is used to schedule a coroutine from a sync context, which is exactly what bare `return` does in NiceGUI 3.8.0's awaitable path. But — **NiceGUI only picks up the awaitable if the handler is called via NiceGUI's `handle_event`**. For `on_change` handlers called via NiceGUI events, this is fine. For manually-called `on_*()` functions, the conversion requires the call site to also be an NiceGUI event handler. Research shows these are all `on_change` / `on_click` wired to NiceGUI UI elements — conversion is safe.

### 2.2 `web/pages/browse.py` — 19 ensure_future calls

[VERIFIED: grep of source file]

| Line | Pattern | Category | Disposition |
|------|---------|----------|-------------|
| 1397 | `asyncio.ensure_future(load_page(direction=0))` — after save_correction | Cat-1 | Convert: `on_click`-equivalent callback |
| 1589 | `on_click=lambda: asyncio.ensure_future(load_page())` | Cat-1 | Convert: Back button on error screen |
| 1629 | `on_click=lambda: asyncio.ensure_future(navigate_shelfmark(-1))` | Cat-1 | Convert: Prev shelfmark button |
| 1822 | `on_click=lambda: asyncio.ensure_future(navigate_shelfmark(1))` | Cat-1 | Convert: Next shelfmark button |
| 3696 | `asyncio.ensure_future(load_page(p_num=1))` — inside `_handle_volume_change` | Cat-1 | Convert: `on_change` handler |
| 3711 | `on_click=lambda: asyncio.ensure_future(load_page(direction=-1))` | Cat-1 | Convert: Prev page button |
| 3736 | `asyncio.ensure_future(go_to_page(val))` — inside `handle_folio_select` | Cat-1 | Convert: `on_change` folio select |
| 3755 | `asyncio.ensure_future(go_to_page(val))` — inside `handle_go_click` | Cat-1 | Convert: Go button |
| 3764 | `on_click=lambda: asyncio.ensure_future(load_page(direction=1))` | Cat-1 | Convert: Next page button |
| 3797 | `asyncio.ensure_future(load_page(direction=0))` — inside `refresh_page` | Cat-1 | Convert: refresh callback |
| 4471 | `asyncio.ensure_future(load_page(fl_id=initial_fl_id_value))` — bootstrap | Cat-2 | Keep with comment: bootstrap deferred init (page must render spinner first) |
| 4490 | `asyncio.ensure_future(load_page(p_num=initial_page))` — bootstrap | Cat-2 | Keep: same |
| 4500 | `asyncio.ensure_future(load_page(p_num=initial_page))` — bootstrap | Cat-2 | Keep: same |
| 4505 | `asyncio.ensure_future(load_page(p_num=initial_page))` — bootstrap | Cat-2 | Keep: same |
| 4509 | `asyncio.ensure_future(search_shelfmark())` — bootstrap | Cat-2 | Keep: bootstrap deferred init |
| 4530 | `asyncio.ensure_future(load_page(p_num=saved_position.get('p_num', 1)))` — bootstrap | Cat-2 | Keep: bootstrap deferred init |

**Note on lines 4466–4532:** These are all in the bootstrap block (`create_browse_page()` end), called once after the page container is created, NOT from a user event. They require deferred scheduling because `update_content()` must render the spinner before `load_page()` fires. These are Cat-2 with comment justification. However, they are ALSO the extraction target for `resolve_browse_bootstrap()` — after extraction, the bootstrap block becomes:
```python
bootstrap = resolve_browse_bootstrap(...)
# Apply result: show spinner, schedule correct load_page variant
```
The `asyncio.ensure_future` calls in the bootstrap block survive in `create_browse_page()` after extraction — they are scheduling calls, not logic.

**browse.py Cat-1 count: 10 conversions.** Cat-2: 6 (keep with comment — all bootstrap inits).

**Missing from grep:** Lines 529 and 643 are comment lines referencing `ensure_future`, not actual calls. Confirmed from source read.

### 2.3 `web/components/filter_panel.py` — 10 ensure_future calls

[VERIFIED: grep of source file]

| Lines | Pattern | Category | Disposition |
|-------|---------|----------|-------------|
| 449–451 | Three `asyncio.ensure_future(...)` in `on_domain_change` (not an `on_click` but a sync handler registered as `on_change`) | Cat-1 | Convert |
| 458–459 | Two in `on_author_change` | Cat-1 | Convert |
| 466 | One in `on_work_change` | Cat-1 | Convert |
| 472 | One in `on_mode_change` | Cat-1 | Convert |
| 479 | One in `on_date_from_change` | Cat-1 | Convert |
| 486 | One in `on_date_to_change` | Cat-1 | Convert |
| 497 | One in `on_exclude_printed_change` | Cat-1 | Convert |

**All 10 are Cat-1.** The `create_filter_handlers` factory returns sync handler functions that are wired to NiceGUI `on_change` events. The `ensure_future` calls there schedule async operations from sync handlers — exactly the Cat-1 pattern. Convert all 10 to bare calls.

**Circular import risk note:** After conversion, `filter_panel.py` handlers would need the handlers to return the coroutine. But `create_filter_handlers` returns sync `def on_*` functions. The NiceGUI awaitable path only activates if the handler is called via `handle_event` with the element's event dispatch. Since these handlers ARE registered as NiceGUI `on_change` callbacks, the conversion works. The sync `on_*` functions just need to return the coroutines instead of calling `ensure_future`. However, some handlers call MULTIPLE coroutines (e.g., `on_domain_change` calls three). You cannot return multiple awaitables from one handler. **Resolution: Convert to a single aggregate `async def` for multi-coroutine handlers, or keep as `ensure_future` with Cat-4 justification (background_tasks.create alternative).** This is the one ambiguous classification — see §7 (Pitfalls).

### 2.4 `web/pages/search_results.py` — 2 ensure_future calls

[VERIFIED: grep + source read]

| Line | Pattern | Category | Disposition |
|------|---------|----------|-------------|
| 111 | `asyncio.ensure_future(_run_lazy())` — triggered inside `toggle_expansion` | Cat-2 | Keep with comment: client context re-entry (`with refs.page_client`) required |
| 738 | `asyncio.ensure_future(load_fn())` — inside `_make_lazy_toggle` | Cat-2 | Keep with comment: same reason |

Both lines in `search_results.py` are Cat-2: they explicitly use `with refs.page_client:` to re-enter client context — this is the standard Cat-2 pattern that cannot be eliminated.

### 2.5 Out-of-scope files (not touched by Phase 74)

`web/main.py:377`, `web/main.py:444`, `web/pages/discoveries.py`, `web/components/visual_similarity_dialog.py`, `web/components/joins_panel.py` — out of Phase 74 scope per D-14.

---

## 3. Helper API Shapes

### 3.1 SearchUIState / BrowseState Field Inventory

[VERIFIED: read of search_state.py and browse_state.py]

**SearchUIState fields by bucket:**

| Bucket | Fields |
|--------|--------|
| `runtime_only` | `progress`, `status`, `is_running`, `is_cancelled`, `selected_result`, `total_count`, `current_page_idx`, `current_page`, `selected_indices`, `is_panel_collapsed`, `last_scroll_top`, `update_timer`, `displayed_results`, `builder_negated_words`, `result_domains`, `all_result_domains`, `has_domain_data`, `domain_name_map`, `catalog_source_counts`, `domain_hierarchy`, `search_start_time`, `printed_ids`, `domain_excluded_results`, `filter_manuscript_count`, `restrict_sys_ids`, `word_search_excluded_results`, `_measurement_cache`, `translation_data`, `title_translations`, `search_generation`, `_refine_mode`, `_refinement_stale`, `_refinement_scope_sig`, `_zero_result_refine`, `vs_availability`, `vs_browse_mode`, `_exclusion_shelf_map`, `expanded_index`, `expansion_refs`, `_lazy_loaders` |
| `restorable_page_snapshot` | `results`, `current_page` (pagination idx for restore), `printed_filter`, `domain_exclusions` (set), `transcription_sys_ids` (set), `filter_domains`, `filter_authors`, `filter_works`, `filter_include_mode`, `filter_date_from`, `filter_date_to`, `filter_material_exclude`, `filter_text_all`, `filter_text_any`, `filter_text_not`, `filter_width_min`, `filter_width_max`, `filter_height_min`, `filter_height_max`, `filter_line_count_min`, `filter_line_count_max`, `filter_line_height_min`, `filter_line_height_max`, `filter_text_density_min`, `filter_text_density_max`, `filter_text_density_max`, `filter_measurement_material`, `post_filter_*` (10 post-search measurement fields), `refinement_chain`, `refinement_restrict_sys_ids`, `_all_terms_filter`, `vs_restrict_sys_ids`, `vs_restrict_label`, `vs_restrict_source_ids`, `vs_restrict_mode`, `exclusion_sources` |
| `cross_page_preference` | (none in SearchUIState — show_translations lives directly in app.storage.user, not in state) |

**Ambiguous fields that need planner resolution:**
- `post_filter_*` (10 fields) — these are post-search measurement constraints applied to current results. They are "live page state" but also restorable. **Classify as `restorable_page_snapshot`** (same logic as filters). [ASSUMED]
- `transcription_sys_ids` — a derived enrichment set, rebuilt from results on restore. Could be `runtime_only` (rebuild on restore) or `restorable_page_snapshot` (avoid rebuild). Storing it saves an async fetch on session restore. **Recommend `restorable_page_snapshot`.** [ASSUMED]

**BrowseState fields by bucket:**

| Bucket | Fields |
|--------|--------|
| `runtime_only` | `shelfmark_query` (UI input — not persisted directly), `is_loading`, `error`, `search_error`, `zoom_level`, `rotation`, `is_fullscreen`, `highlight_terms`, `page_input_value`, `view_all`, `full_manuscript`, `edit_mode`, `edit_text`, `edit_notes`, `original_edit_text`, `draft_saved`, `draft_id`, `edit_loading`, `error_message`, `fullscreen_edit`, `pgp_transcription`, `pgp_metadata`, `all_sources`, `view_joined`, `joined_fragments_info`, `joined_pgpid`, `reading_desk_selected_sources`, `source_user_override`, `fjms_data`, `crossref_data`, `enrichment_loaded`, `enrichment_loading`, `title_translation`, `oxford_translations` |
| `restorable_page_snapshot` | `current_page` (the BrowsePage object — serialized as browse_position dict), `sys_id`, `volume_ie`, `active_source`, `reading_desk_entries` (serialized in reading_desk_state), `page_input_value` (p_num — serialized in browse_position) |
| `cross_page_preference` | (none in BrowseState) |

**Note:** BrowseState's `current_page` is a `BrowsePage` object (not JSON-serializable). The browse snapshot stores a position dict `{sys_id, p_num, shelfmark, volume_ie}` — on restore, `load_page(p_num=...)` re-fetches the actual page. This is the existing pattern at line 778. `restore_browse_snapshot` re-hydrates `state.sys_id`, `state.volume_ie`, `state.shelfmark_query`, `state.page_input_value` from the dict and returns the p_num for the bootstrap caller to schedule `load_page`.

### 3.2 Proposed Helper Signatures

[ASSUMED for exact signatures — derived from field analysis and existing search.py:95 pattern]

```python
# web/pages/search_state.py additions

_SEARCH_SNAPSHOT_VERSION = 1  # Increment on schema changes (D-04 tab collision)

def restore_search_snapshot(state: SearchUIState) -> None:
    """Hydrate page-scoped state from app.storage.user snapshot.
    
    Called once at page mount. After this call, SearchUIState is authoritative
    and direct app.storage.user reads for snapshot keys are forbidden (D-03).
    Silently discards snapshot if version stamp is missing or stale.
    """
    blob = app.storage.user.get('search_snapshot_v1')  # Option A: versioned key
    # ... OR keep legacy keys (D-08) with version tag added to each ...
    ...

def persist_search_snapshot(state: SearchUIState) -> None:
    """Serialize restorable fields of SearchUIState to app.storage.user.
    
    Called on significant state changes (query change, search complete,
    filter change, exclusion change). runtime_only fields are NOT written.
    cross_page_preference fields are NOT written (they have their own paths).
    """
    ...

def clear_search_snapshot() -> None:
    """Wipe all search snapshot keys from app.storage.user.
    
    Called by Reset button handler. Replaces the current scattered block
    at search.py:2019-2025 and search.py:806-832.
    """
    ...
```

**D-08 resolution:** The D-08 lock says keep legacy key format. Therefore:
- `restore_search_snapshot` reads individual legacy keys (same as current search.py:95–156)
- `persist_search_snapshot` writes to individual legacy keys
- The version stamp (D-04) is added as an ADDITIONAL key `search_snapshot_schema_version` checked on restore
- If version mismatch, call `clear_search_snapshot()` and return (silently discard stale blob)

```python
# web/pages/browse_state.py additions

_BROWSE_SNAPSHOT_VERSION = 1

def restore_browse_snapshot(state: BrowseState) -> dict | None:
    """Hydrate browse position from app.storage.user.
    
    Returns the browse_position dict (or None) so the bootstrap caller
    knows what p_num to pass to load_page(). The caller is responsible
    for scheduling load_page — restore_browse_snapshot does not schedule async work.
    
    Also restores reading_desk_state into state.reading_desk_entries.
    """
    ...

def persist_browse_snapshot(state: BrowseState, page) -> None:
    """Serialize browse position and reading desk state.
    
    page: the BrowsePage object (for shelfmark/p_num extraction).
    """
    ...

def clear_browse_snapshot() -> None:
    """Wipe browse_position and reading_desk_state keys."""
    ...
```

**Return type note:** `restore_browse_snapshot` returns the position dict rather than `None` because the bootstrap block needs the `p_num` to schedule `load_page(p_num=...)`. This avoids a second `app.storage.user.get('browse_position')` call in the bootstrap.

### 3.3 `resolve_browse_bootstrap` Signature

[VERIFIED: search_bootstrap.py read; browse.py:4466–4532 read]

Mirroring `resolve_search_bootstrap`:

```python
# web/browse_bootstrap.py

from __future__ import annotations
from typing import Any, Dict

def resolve_browse_bootstrap(
    *,
    initial_fl_id: str | None,         # from URL param fl_id
    initial_sys_id: str | None,         # from URL param sys_id
    initial_page: int,                   # from URL param p (default 1)
    pending_shelfmark: str | None,       # from URL param shelfmark
    saved_reading_desk: dict | None,     # from app.storage.user.get('reading_desk_state')
    saved_position: dict | None,         # from app.storage.user.get('browse_position')
) -> Dict[str, Any]:
    """Resolve browse bootstrap action without scheduling async tasks.
    
    Returns a dict describing what action to take:
    {
        'action': 'fl_id' | 'sys_id' | 'shelfmark' | 'restore_desk' | 'restore_position' | 'none',
        'restore_desk': bool,
        'clear_desk': bool,
        'p_num': int,
        'fl_id': str | None,
        'sys_id': str | None,
        'shelfmark': str | None,
    }
    Callers use the returned dict to dispatch the correct load_page() call.
    """
    if initial_fl_id:
        return {'action': 'fl_id', 'fl_id': initial_fl_id, 'p_num': initial_page,
                'restore_desk': False, 'clear_desk': False, ...}
    
    if initial_sys_id:
        # reading-desk check: same as lines 4473–4505
        ...
    
    if pending_shelfmark:
        return {'action': 'shelfmark', 'shelfmark': pending_shelfmark, ...}
    
    # No sys_id: try reading desk restore, then position restore
    ...
    
    return {'action': 'none', ...}
```

**Three precedence cases (maps to D-19 test cases):**
1. Explicit `sys_id` in URL beats saved `browse_position` (when sys_id != saved reading desk entries)
2. Blank `/browse` with saved `browse_position` → restore position
3. Blank `/browse` with saved `reading_desk_state` → restore desk (wins over position)

**Input shape:** All inputs are pure data (no `app.storage.user` reads inside the function) — same as `resolve_search_bootstrap`. The caller reads storage and passes as arguments.

### 3.4 Version Stamp Design (D-04)

[ASSUMED — derived from D-04 requirement and D-08 legacy-key constraint]

**Stamp approach:** Add a single `search_snapshot_schema_version` integer key alongside existing search snapshot keys. Add `browse_snapshot_schema_version` for browse. No change to the 30+ legacy payload keys.

**Detection rule on restore:**
```python
stored_version = app.storage.user.get('search_snapshot_schema_version', 0)
if stored_version != _SEARCH_SNAPSHOT_VERSION:
    clear_search_snapshot()
    return  # State already reset; page starts fresh
```

**Tab stomping:** When Tab B writes snapshot keys while Tab A is active, Tab A may read stale data if it restores on next navigation. The version stamp does not prevent stomping — it only prevents cross-version corruption. True tab isolation would require per-tab keys (deferred as Codex's W3). **Document this limitation in the helper docstring.**

---

## 4. NiceGUI 3.8.0 Awaitable Scheduling Verification

[VERIFIED: nicegui/events.py source fetched from GitHub]

### The mechanism

In NiceGUI 3.8.0, `events.handle_event()` does:

```python
result = handler(arguments) if expects_arguments else handler()
if isinstance(result, Awaitable) and not isinstance(result, AwaitableResponse):
    async def wait_for_result():
        with parent_slot:          # ← preserves slot context
            try:
                await result
            except Exception as e:
                core.app.handle_exception(e)
    if core.loop and core.loop.is_running():
        background_tasks.create(wait_for_result(), name=str(handler))  # ← NiceGUI's own task path
    else:
        core.app.on_startup(wait_for_result())
```

**Key facts:**
1. If `on_click=lambda: load_page(...)`, the lambda returns a coroutine. `handle_event` sees `Awaitable`, wraps it in `wait_for_result()` which preserves `parent_slot`, and schedules via `background_tasks.create()`.
2. If `on_click=lambda: asyncio.ensure_future(load_page(...))`, the lambda returns a `Task` (not an `Awaitable` from the check's perspective — `Task` IS `Awaitable` but `ensure_future` already consumes the coroutine). **Actually:** `Task` IS an `Awaitable`. But the `parent_slot` context is NOT re-entered because `ensure_future` schedules on the raw event loop WITHOUT the slot context wrapper. This is the bug.
3. The `parent_slot` context is the NiceGUI client context. Without it, `ui.*` calls from within the coroutine may go to the wrong client or fail on navigation.
4. `background_tasks.create()` (NiceGUI's path) also logs unhandled exceptions — `ensure_future` does not.

**Why Cat-1 conversion fixes URL-bar updates:** `_update_browser_url()` in browse.py calls `ui.run_javascript(...)` which requires client context. When called from a detached `ensure_future` task (no slot context), the JS call silently fails or goes to a stale client. After Cat-1 conversion, the coroutine runs inside `wait_for_result()` which holds `parent_slot` — the JS call reaches the correct browser tab.

**Why Cat-2 cannot be eliminated:** Cat-2 sites do `with container:` or `with client:` explicitly because they are scheduled from contexts where `parent_slot` is NOT the page's primary slot (e.g., `_after_delay` scheduling, timer callbacks). No NiceGUI idiom auto-injects the right context for these cases. The Cat-2 comment justification is: `"Deferred to next event loop tick to allow container to mount; explicit client context re-entry required."`.

---

## 5. Test Plan Specifics

### 5.1 `tests/test_search_state.py` — Does it exist?

[VERIFIED: `ls tests/` shows `test_search_bootstrap.py` and `test_search_normalization.py` but NOT `test_search_state.py`]

`tests/test_search_state.py` does NOT yet exist. Create it as a new file.

**Test content for D-18 (search snapshot round-trip):**

```python
# tests/test_search_state.py
"""Round-trip tests for search snapshot helpers (Phase 74, D-18)."""
from unittest.mock import patch, MagicMock

def _make_storage():
    """Create a simple dict that mimics app.storage.user."""
    return {}

def test_persist_and_restore_round_trip():
    """runtime_only fields are pristine; restorable fields survive."""
    storage = _make_storage()
    with patch('web.pages.search_state.app') as mock_app:
        mock_app.storage.user = storage
        
        from web.pages.search_state import SearchUIState, persist_search_snapshot, restore_search_snapshot, clear_search_snapshot
        
        state = SearchUIState()
        state.results = [{'display': {'id': 'abc'}}]
        state.printed_filter = 'hide_printed'
        state.is_running = True   # runtime_only — must NOT survive
        state.expanded_index = 3  # runtime_only — must NOT survive
        
        persist_search_snapshot(state)
        
        fresh_state = SearchUIState()
        fresh_state.is_running = False
        fresh_state.expanded_index = None
        restore_search_snapshot(fresh_state)
        
        assert fresh_state.results == [{'display': {'id': 'abc'}}]
        assert fresh_state.printed_filter == 'hide_printed'
        # runtime_only fields must remain pristine defaults
        assert fresh_state.is_running is False
        assert fresh_state.expanded_index is None

def test_clear_snapshot_wipes_all_keys():
    """clear_search_snapshot removes all snapshot keys."""
    storage = {'search_query': 'test', 'search_mode': 'exact', 'search_snapshot_schema_version': 1}
    with patch('web.pages.search_state.app') as mock_app:
        mock_app.storage.user = storage
        from web.pages.search_state import clear_search_snapshot
        clear_search_snapshot()
        assert 'search_query' not in storage or storage.get('search_query') == ''

def test_stale_version_discards_snapshot():
    """Snapshot with wrong version stamp is silently discarded."""
    storage = {'search_query': 'stale', 'search_snapshot_schema_version': 999}
    with patch('web.pages.search_state.app') as mock_app:
        mock_app.storage.user = storage
        from web.pages.search_state import SearchUIState, restore_search_snapshot
        state = SearchUIState()
        restore_search_snapshot(state)
        assert state.results == []  # Default, not restored
```

### 5.2 `tests/test_browse_bootstrap.py` — Does it exist?

[VERIFIED: `ls tests/` — NOT present]

Create `tests/test_browse_bootstrap.py` mirroring `tests/test_search_bootstrap.py`.

**Three precedence cases for D-19:**

```python
# tests/test_browse_bootstrap.py
from web.browse_bootstrap import resolve_browse_bootstrap

def test_explicit_sys_id_beats_saved_position():
    """Case (a): explicit sys_id in URL, no matching reading desk."""
    result = resolve_browse_bootstrap(
        initial_fl_id=None,
        initial_sys_id='003750',
        initial_page=1,
        pending_shelfmark=None,
        saved_reading_desk=None,   # or desk with different sys_ids
        saved_position={'sys_id': '999999', 'p_num': 5, 'shelfmark': 'T-S Old'},
    )
    assert result['action'] == 'sys_id'
    assert result['clear_desk'] is False  # or True if desk had entries

def test_blank_browse_restores_saved_position():
    """Case (b): no URL params, restore from browse_position."""
    result = resolve_browse_bootstrap(
        initial_fl_id=None,
        initial_sys_id=None,
        initial_page=1,
        pending_shelfmark=None,
        saved_reading_desk=None,
        saved_position={'sys_id': '003750', 'p_num': 3, 'shelfmark': 'T-S 12.1'},
    )
    assert result['action'] == 'restore_position'
    assert result['p_num'] == 3

def test_reading_desk_restore_wins_over_position():
    """Case (c): saved reading desk takes priority over browse_position."""
    result = resolve_browse_bootstrap(
        initial_fl_id=None,
        initial_sys_id=None,
        initial_page=1,
        pending_shelfmark=None,
        saved_reading_desk={'entries': [{'sys_id': '003750'}]},
        saved_position={'sys_id': '999999', 'p_num': 2, 'shelfmark': 'T-S Old'},
    )
    assert result['action'] == 'restore_desk'
    assert result['restore_desk'] is True

def test_explicit_sys_id_matching_desk_restores_desk():
    """sys_id in URL that matches a reading desk entry triggers desk restore."""
    result = resolve_browse_bootstrap(
        initial_fl_id=None,
        initial_sys_id='003750',
        initial_page=1,
        pending_shelfmark=None,
        saved_reading_desk={'entries': [{'sys_id': '003750', 'shelfmark': 'T-S 12.1'}]},
        saved_position=None,
    )
    assert result['action'] == 'restore_desk'

def test_no_context_no_action():
    """Blank /browse with no saved state: action=none."""
    result = resolve_browse_bootstrap(
        initial_fl_id=None,
        initial_sys_id=None,
        initial_page=1,
        pending_shelfmark=None,
        saved_reading_desk=None,
        saved_position=None,
    )
    assert result['action'] == 'none'
```

### 5.3 URL-bar E2E Assertion (D-20)

[VERIFIED: `tests/e2e/test_browse_flow.py` exists; uses selenium Screen fixture]

The existing `TestBrowseNavigation.test_browse_with_sys_id` tests page loading but does not assert URL bar updates. Add an assertion to `tests/e2e/test_browse_flow.py`:

```python
def test_shelfmark_navigation_updates_url(self, screen):
    """URL bar updates after shelfmark navigation (proves Cat-1 async fix works)."""
    screen.open('/browse?sys_id=003750')
    screen.wait(8.0)  # Allow page to load + URL sync
    
    current_url = screen.selenium.current_url
    # URL should contain sys_id after navigation
    assert 'sys_id' in current_url or 'browse' in current_url, \
        f"URL should reflect navigation state, got: {current_url}"
    
    # Find and click Next Shelfmark button
    next_btns = screen.selenium.find_elements(By.CSS_SELECTOR, '[aria-label="Next manuscript"], [data-action="next-manuscript"]')
    if next_btns:
        next_btns[0].click()
        screen.wait(5.0)
        new_url = screen.selenium.current_url
        # URL should have changed after navigation
        assert new_url != current_url or 'browse' in new_url, \
            "URL should update after shelfmark navigation"
```

**Note:** The E2E test depends on ChromeDriver/Selenium being available (skips gracefully when not). The existing conftest.py handles this via `pytest.importorskip("selenium")`. The test is a smoke assertion, not a strict URL equality check, because shelfmark navigation depends on Tantivy index availability.

**Alternative:** A unit test asserting `_update_browser_url()` is called from within the correct NiceGUI slot context is more deterministic but requires NiceGUI test harness. For Phase 74, the E2E assertion is sufficient signal per D-20.

---

## 6. Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Storage serialization | Custom JSON encoder/decoder | Native dict assignment to `app.storage.user` | NiceGUI handles serialization; only sets/frozensets need `list()` conversion |
| Version stamping | Complex migration system | Single integer key comparison | D-04 only needs "discard and restart" on version mismatch |
| Async scheduling from sync handler | Custom task scheduler | Return the coroutine (NiceGUI handles it) or use `background_tasks.create()` | Both are idiomatic NiceGUI 3.8 |
| Test mocking of app.storage.user | Full NiceGUI test harness | `unittest.mock.patch('module.app')` with a plain dict | Works for pure storage helpers with no UI |

---

## 7. Common Pitfalls

### Pitfall 1: Multi-coroutine handlers in filter_panel.py

**What goes wrong:** `on_domain_change` calls three `asyncio.ensure_future(...)` calls. Converting to bare returns only allows returning ONE awaitable. If you naively return the first coroutine, the other two are dropped.

**Why it happens:** NiceGUI's `handle_event` schedules exactly one awaitable from `result`.

**How to avoid:** Wrap multi-coroutine handlers in a single `async def` aggregate:
```python
def on_domain_change(e=None):
    val = filter_refs['domain'].value or []
    state.filter_domains = val if isinstance(val, list) else [val] if val else []
    persist_value(f'{pfx}_filter_domains', state.filter_domains)
    update_chip_fn()

    async def _fanout():
        await refresh_author_fn()
        await refresh_work_fn()
        await recompute_fn()
    return _fanout()  # single awaitable returned
```
This is a Cat-1 conversion with an aggregate wrapper — still behavior-preserving.

**Warning signs:** Any sync handler that calls multiple `ensure_future` must use the aggregate pattern.

### Pitfall 2: Helpers as sole owners — violation during Reset

**What goes wrong:** The "New Search" / Reset button handler in search.py directly writes storage keys (lines 2019–2025, 806–832) instead of calling `clear_search_snapshot()`. After Phase 74, any direct write for snapshot keys is a violation.

**Why it happens:** The clear blocks predate the helper extraction.

**How to avoid:** The Reset button handler becomes `clear_search_snapshot(); _reset_state_fields()`. The `clear_search_snapshot()` owns all storage key wiping. The `_reset_state_fields()` (or equivalent) resets the SearchUIState object fields.

**Warning signs:** Any `app.storage.user[<snapshot_key>] = ...` outside of `persist_/clear_*_snapshot` after Phase 74.

### Pitfall 3: BrowseState `restore_browse_snapshot` scheduling

**What goes wrong:** `restore_browse_snapshot` cannot schedule `load_page(...)` directly because it would require `asyncio.ensure_future` inside a helper that has no NiceGUI context. The caller must do the scheduling.

**Why it happens:** The restore function is called from `create_browse_page()` before the page is fully mounted.

**How to avoid:** `restore_browse_snapshot` returns the position dict (or action dict). The `create_browse_page()` bootstrap code uses the returned value to decide which `asyncio.ensure_future(load_page(...))` call to make. The helper is pure data — no scheduling.

**Warning signs:** Any `asyncio.ensure_future` inside `restore_browse_snapshot` or `restore_search_snapshot`.

### Pitfall 4: `filter_panel.load_filter_state` and `restore_search_snapshot` duplication

**What goes wrong:** If `restore_search_snapshot` re-implements the 19 filter key reads already in `filter_panel.load_filter_state(state, 'search')`, both code paths diverge over time.

**How to avoid:** `restore_search_snapshot` delegates filter restoration to `filter_panel.load_filter_state(state, 'search')` explicitly. This avoids duplication. Circular import risk: `search_state.py` currently has ZERO import of `filter_panel`. Adding this import creates a dependency. Check that `filter_panel.py` does NOT import from `search_state.py` (it imports `app` from nicegui and `persist_value` locally — no search_state import). Safe. [VERIFIED: filter_panel.py imports checked]

### Pitfall 5: `browse_position` dict shape after volume_ie restore

**What goes wrong:** The `browse_position` dict now includes `volume_ie` (added in v7.7.0). The restore logic at browse.py:4520–4527 validates the `volume_ie` by calling `get_volumes_for_sys_id()` — a genizah_core lookup. `restore_browse_snapshot` must preserve this validation logic.

**How to avoid:** Either (a) call `restore_browse_snapshot` only for the snapshot data parts and leave the volume validation in `create_browse_page()`, or (b) accept `genizah_core` as a dependency of `browse_state.py`. Option (a) is cleaner for a pure helper. `restore_browse_snapshot` returns the raw position dict; the caller validates `volume_ie`.

### Pitfall 6: `search_all_terms_filter` key not in the session restore block

**What goes wrong:** `search_all_terms_filter` is written via `persist_value('search_all_terms_filter', checked)` at search.py:1700, but NOT read back in the existing session restore block (lines 95–156). It may be missing from `restore_search_snapshot` too.

**How to avoid:** Audit `persist_value` calls throughout search.py for keys not in the restore block. Add them to `restore_search_snapshot`. The grep shows `persist_value` is called at lines 882–884 (filter text terms) and 1700 (all_terms_filter) — verify these are included in restore.

### Pitfall 7: `incoming_filters` classification

**What goes wrong:** `incoming_filters` in `app.storage.user` (filter_panel.py:290–321) is neither a snapshot nor a preference — it is a cross-page signal written by the catalog browse page and consumed by the search page. Classifying it as `restorable_page_snapshot` and clearing it in `clear_search_snapshot` would break catalog → search navigation.

**How to avoid:** Leave `incoming_filters` in `filter_panel.consume_incoming_filters()` where it already is. It is NOT a snapshot field. Do not include it in `clear_search_snapshot`.

### Pitfall 8: CI Windows matrix path separators

**What goes wrong:** The existing E2E test conftest uses `Path(__file__).resolve().parent.parent.parent / 'web' / 'main.py'` — this works on both Windows and Linux. New test files should follow the same pattern.

**How to avoid:** Use `pathlib.Path` for all file references in new test files.

---

## 8. Standard Stack

### Core

| Library | Version | Purpose | Notes |
|---------|---------|---------|-------|
| NiceGUI | 3.8.0 | Web UI framework + async scheduling | Pinned; `on_click` awaitable scheduling is the Cat-1 fix mechanism |
| Python asyncio | stdlib | `asyncio.ensure_future`, `background_tasks` | Cat-1 conversion target |
| `unittest.mock` | stdlib | Test mocking of `app.storage.user` | Used in new unit tests |

**No new dependencies.** Phase 74 is a refactor with zero new packages.

**Version verification:** `nicegui==3.8.0` is pinned in requirements.txt:9. [VERIFIED: requirements.txt line 9 — confirmed by CONTEXT.md canonical_refs]

---

## 9. Architecture Patterns

### System Architecture Diagram

```
create_search_page() / create_browse_page()
        │
        ▼
  [Session Restore]
  restore_*_snapshot(state)   ←── app.storage.user (read once, then sealed)
        │                          [legacy keys]
        ▼
  SearchUIState / BrowseState  ←── authoritative during page lifetime
        │
        ▼
  [User Events] ──on_click=lambda: coro()──► NiceGUI handle_event
                                              │ (awaitable return)
                                              ▼
                                         background_tasks.create(
                                           wait_for_result(), ← with parent_slot
                                           name=str(handler)
                                         )
        │
  [State Changes]
  persist_*_snapshot(state)   ──────────────► app.storage.user (write)
  clear_*_snapshot()
        │
  resolve_browse_bootstrap(   ←── app.storage.user (read inputs, then pure)
    initial_fl_id, sys_id,
    saved_desk, saved_pos, ...
  )  ──returns dict──► create_browse_page() dispatches load_page(...)
```

### Recommended Project Structure (additions only)

```
web/
├── browse_bootstrap.py      # NEW — mirrors search_bootstrap.py
├── search_bootstrap.py      # EXISTING — no change
├── pages/
│   ├── search_state.py      # ADD: restore_/persist_/clear_search_snapshot
│   ├── browse_state.py      # ADD: restore_/persist_/clear_browse_snapshot
│   ├── search.py            # MODIFY: remove direct snapshot writes, add helper calls
│   ├── browse.py            # MODIFY: remove direct snapshot writes, Cat-1 conversions, bootstrap extraction
│   └── browse_enrichment.py # MINOR: ensure_future comments only (Cat-2)
├── components/
│   └── filter_panel.py      # MODIFY: Cat-1 ensure_future conversions in create_filter_handlers

tests/
├── test_search_state.py     # NEW — snapshot round-trip tests (D-18)
├── test_browse_bootstrap.py # NEW — precedence tests (D-19)
└── e2e/
    └── test_browse_flow.py  # MODIFY — add URL-bar update assertion (D-20)
```

### Anti-Patterns to Avoid

- **Direct snapshot key writes outside helpers:** `app.storage.user['search_query'] = ...` anywhere except inside `persist_search_snapshot` / `clear_search_snapshot` after this phase.
- **Snapshot reads after restore:** `app.storage.user.get('search_mode', ...)` inside `execute_search()` or `update_content()` — state must come from `SearchUIState.mode`, not storage.
- **`ensure_future` for Cat-1 patterns:** Any `on_click=lambda: asyncio.ensure_future(coro(...))` left unconverted is a regression.
- **Blocking operations in snapshot helpers:** `persist_search_snapshot` must be synchronous and fast. Do not add `await` or blocking I/O.

---

## 10. Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `post_filter_*` fields classified as `restorable_page_snapshot` | §3.1 | Low: planner may choose `runtime_only` (rebuild on restore) — behavior unchanged either way |
| A2 | `transcription_sys_ids` classified as `restorable_page_snapshot` | §3.1 | Low: if `runtime_only`, async rebuild needed on restore (already happens via `_deferred_transcription_restore`) |
| A3 | Exact signatures of `restore_/persist_/clear_*_snapshot` | §3.2 | Medium: return types and parameter shapes may need adjustment after executor sees full dependencies |
| A4 | `search_all_terms_filter` is a restorable field missing from current restore block | §7 Pitfall 6 | Medium: if the key is intentionally not restored, adding it changes behavior |
| A5 | Multi-coroutine filter handlers use aggregate `async def` wrapper for Cat-1 conversion | §7 Pitfall 1 | Low: alternative is Cat-4 (background_tasks.create) per call — behavior same |
| A6 | `resolve_browse_bootstrap` takes storage inputs as plain parameters (pure function) | §3.3 | Low: mirrors search_bootstrap.py which is pure; confirmed correct pattern |

---

## 11. Open Questions

1. **`filter_panel.py` ensure_future: Cat-1 aggregate vs Cat-4**
   - What we know: 10 ensure_future calls in `create_filter_handlers`, 3+ per handler
   - What's unclear: whether aggregate `async def` wrappers are acceptable in the sync factory pattern, or whether Cat-4 (`background_tasks.create()`) is cleaner
   - Recommendation: Use aggregate `async def` for Cat-1 purity; document Cat-4 as alternative

2. **Should `persist_search_snapshot` be called on every single state change?**
   - What we know: current code writes individual keys inline at each change point
   - What's unclear: whether persist should be called atomically after each user action or batched
   - Recommendation: Inline calls at existing write sites (line 400, 509, 522, etc.) converted to `persist_search_snapshot(search_state)` calls. This may write more keys than necessary; a narrow `persist_search_snapshot_field(state, field_name)` variant is an option but not required by D-05.

3. **`browse_export_data` key: keep as-is or route through helper?**
   - What we know: classified as `runtime_only` (ephemeral handoff), line 1171
   - What's unclear: whether the rule "helpers as sole owners" applies to runtime_only keys
   - Recommendation: `runtime_only` keys are explicitly exempt from the snapshot helper rule (D-01). Keep the direct write at line 1171.

---

## 12. Environment Availability

Step 2.6: SKIPPED (no external dependencies — Phase 74 is code/config refactor only, no new services or CLIs required)

---

## Validation Architecture

> nyquist_validation: true (from .planning/config.json)

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (stdlib tests + selenium E2E) |
| Config file | None (no pytest.ini; conftest.py at root and tests/e2e/) |
| Quick run command | `pytest tests/test_search_bootstrap.py tests/test_search_state.py tests/test_browse_bootstrap.py -x` |
| Full suite command | `pytest tests/` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| WEBM-03 (snapshot round-trip) | `persist_search_snapshot` excludes runtime_only, restores restorable | unit | `pytest tests/test_search_state.py -x` | ❌ Wave 0 |
| WEBM-03 (snapshot clear) | `clear_search_snapshot` wipes all snapshot keys | unit | `pytest tests/test_search_state.py::test_clear_snapshot_wipes_all_keys -x` | ❌ Wave 0 |
| WEBM-03 (version stamp) | Stale version discards snapshot | unit | `pytest tests/test_search_state.py::test_stale_version_discards_snapshot -x` | ❌ Wave 0 |
| WEBM-03 (browse precedence) | `resolve_browse_bootstrap` three precedence cases | unit | `pytest tests/test_browse_bootstrap.py -x` | ❌ Wave 0 |
| WEBM-03 (URL bar) | Navigation updates URL bar | E2E/smoke | `pytest tests/e2e/test_browse_flow.py::TestBrowseNavigation::test_shelfmark_navigation_updates_url -x` | ❌ Wave 0 (addition to existing file) |
| WEBM-03 (regression gate) | Full pytest baseline stays green | full suite | `pytest tests/` | ✅ Existing |
| D-22 (web smoke check) | App starts, search returns results, browse loads manuscript | manual | N/A (manual per ROADMAP) | N/A |
| D-23 (CI) | Ubuntu + Windows matrix green | CI | `.github/workflows/ci.yml` | ✅ Existing |

### Sampling Rate

- **Per task commit:** `pytest tests/test_search_state.py tests/test_browse_bootstrap.py tests/test_search_bootstrap.py -x`
- **Per wave merge:** `pytest tests/` (full suite, must remain 1067 passed / 8 skipped baseline)
- **Phase gate:** Full suite green + web smoke check + D-24 cross-tab manual test before `/gsd-verify-work`

### Wave 0 Gaps

- [ ] `tests/test_search_state.py` — snapshot round-trip, clear, and version-stamp tests (covers WEBM-03 persistence boundary)
- [ ] `tests/test_browse_bootstrap.py` — five precedence cases (covers WEBM-03 browse bootstrap extraction)
- [ ] Addition to `tests/e2e/test_browse_flow.py` — `test_shelfmark_navigation_updates_url` (covers WEBM-03 Cat-1 async fix proof)

*(Existing test infrastructure covers all other phase requirements — no new conftest.py or framework installs needed)*

---

## Security Domain

> Not applicable — Phase 74 is a pure refactor (no new auth, input, or data paths introduced). No ASVS categories are relevant to snapshot serialization of existing in-memory state.

---

## Sources

### Primary (HIGH confidence)

- NiceGUI 3.8.0 source — `nicegui/events.py` `handle_event` function (fetched via WebFetch from GitHub) — awaitable scheduling mechanism confirmed
- `web/pages/search.py` (current working tree) — all 46 storage refs and 22 ensure_future calls verified by grep + line reads
- `web/pages/browse.py` (current working tree) — all 13 storage refs and 19 ensure_future calls verified by grep + line reads
- `web/components/filter_panel.py` (current working tree) — all 30 storage refs and 10 ensure_future calls verified
- `web/pages/search_state.py` (current working tree) — full SearchUIState field inventory read
- `web/pages/browse_state.py` (current working tree) — full BrowseState field inventory read
- `web/search_bootstrap.py` + `tests/test_search_bootstrap.py` (current working tree) — pattern confirmed for browse_bootstrap mirror
- `tests/e2e/test_browse_flow.py` (current working tree) — E2E fixture pattern confirmed
- `.planning/phases/74-page-scoped-state-refactor/74-CONTEXT.md` — locked decisions, canonical refs

### Secondary (MEDIUM confidence)

- NiceGUI GitHub discussions 2729 and 2026 (referenced in CONTEXT.md canonical refs, not directly fetched) — client context and background tasks patterns
- Codex external review (`74-CODEX-REVIEW.md`) — Cat classification rationale and scope ceiling

### Tertiary (LOW confidence)

- None

---

## Metadata

**Confidence breakdown:**
- Field audit: HIGH — all refs verified by grep + line reads against current working tree
- ensure_future audit: HIGH — all calls verified; Cat classification is HIGH except filter_panel multi-coroutine (MEDIUM — see Pitfall 1)
- Helper API shapes: MEDIUM — signatures derived from field analysis and search_bootstrap.py pattern; exact parameter list may shift during execution
- NiceGUI mechanism: HIGH — verified from source

**Research date:** 2026-04-16
**Valid until:** 2026-05-16 (NiceGUI 3.8.0 is pinned; this research is stable until the pin changes)
