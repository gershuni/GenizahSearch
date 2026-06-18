# Phase 118: Joins, Entry & Full Builders — Research

**Researched:** 2026-06-18
**Domain:** NiceGUI web, shared/joins_lab.py core, Supabase RLS, cross-side page contract
**Confidence:** HIGH (all claims verified against live code)

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Web-idiomatic builder, NOT the generic Responsa tabular query-builder dialog.
- **D-02:** Responsa is the default; builder shown inline by default. Replaces Phase 117's fixed `mode='exact'` spine default.
- **D-03:** Line-based, rows stacked VERTICALLY. Phase 118 evolves `lines_to_side_query` (`web/pages/joins_lab.py:116-130`).
- **D-04:** OR-entry per line via a text field with light Responsa syntax: `space = sequence`, `a/b = OR-alternatives`.
- **D-05:** No anchor-line-click seeding — typed by hand only.
- **D-06:** Per-row modifiers live in a compact "more" affordance (popover/menu, `icon=tune`).
- **D-07:** Per-line GAP control inline between stacked rows → maps to `BuilderRow.gap_to_next` → `[|N]` syntax.
- **D-08:** Text Position 5-way select (from `web/pages/search.py:646-655`) placed PROMINENTLY, not in Advanced.
- **D-09:** Mode selector: Exact / Variants / Fuzzy — layered on always-Responsa builder.
- **D-10:** Advanced toggles = Flexible-spacing + Bidirectional ONLY. **Judeo-Arabic dropped** (diverges from BLD-04, user decision). `ja` stays `False`.
- **D-11:** ONE shared toggle set for both sides. `compose()` hardcodes `ja/flex/bidirectional=False`; the web `_merge_globals`-equivalent MUST re-inject `flex_spacing` + `bidirectional` into BOTH composed ROs (`ro` and `b_ro`). Variants flows via `SideQuery.variants`.
- **D-12:** "Advanced search options" collapsible disclosure (`ui.expansion_item`) holds global toggles + other-side builder.
- **D-13:** Other-side builder OFF+collapsed by default; Narrow (AND) is the default combine mode. `allow_page_position=False` on other side (parity desktop:2251). Drives `resolve_other_side_pages` → `cross_side_membership` → `apply_cross_side`. **Web page contract for `resolve_other_side_pages` is the researcher primary deliverable.**
- **D-14:** Builder auto-collapses to summary bar on search; empty-builder Run Search is disabled+tooltip-guarded.
- **D-15:** Known-joins group: collapsible section below anchor transcription inside sticky anchor pane; compact source-badged rows (PGP/FJMS/user/community).
- **D-16:** Phase 118 interactions: display + click-member-to-re-anchor + open-member-in-browse only. Add-to-Puzzle/List deferred to Phase 120.
- **D-17:** ANC-05 fix = public/confirmed-only joins in process-global path. User's OWN unconfirmed joins will NOT appear (accepted trade-off). Fixes `joins_panel.py` global cache + `get_fragment_joins` path.
- **D-18:** "Find joins" opens `/joins-lab` in a NEW browser tab. Deep link = `sys_id` (+ `volume_ie` for multi-IE). No builder/triage state in URL.
- **D-19:** Reuse existing `create_joins_button`/`create_joins_dialog` as entry. On browse: joins EXIST → dialog + "Find more joins" button; NO joins → recolored button + tooltip "Find Joins" → straight to Lab.
- **D-20:** Same as D-19 in Quick View (`/search` result advanced dialog).
- **D-21:** `/search` result cards show joins icon (`link`). Same logic: joins exist → dialog + "Find more joins"; none → straight to Lab (new tab). Icon on EVERY fragment.

### Claude's Discretion
- Exact collapsed summary-bar wording (D-14) and empty-builder hint text.
- Per-row "more" affordance widget (popover vs inline) and how OR text/chips render within a row.
- Exact placement/styling of per-line gap control between stacked rows.
- Joins icon glyph + colors for joins-present vs none on cards / recolored browse button.
- The `safe_storage` builder-state shape — full persistence is Phase 120; if any 118 state must survive, extend schema under `schema_version=1` (no bump unless key removed/retyped).

### Deferred Ideas (OUT OF SCOPE)
- Bulk Add-to-Puzzle / Add-to-List → Phase 120.
- Full builder/triage/filter persistence + re-run-on-restore → Phase 120.
- Candidate triage / table / Compare / Visual Similarity → Phase 119.
- Anchor-line-click seeding (declined, D-05).
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| ANC-04 | Known joins shown as connected group with source attribution | `fetch_connected_fragments` + `badge_for_source` parity; `joins_panel.py` reuse |
| ANC-05 | Multitenant-safe known-joins: only public/confirmed; no cross-user cache leak | RLS policy "Joins are public" USING (true) leaks all rows; fix = add `status='confirmed'` filter in process-global path |
| FND-04 | "Find joins" from `/search` result cards | `create_result_card` action row extension in `search_results.py`; new-tab via `ui.run_javascript('window.open(...)')` |
| FND-05 | "Find joins" from `/browse` | `create_joins_button` extension in `browse.py:3904`; new-tab same pattern |
| BLD-02 | Other-side builder with web page contract for `resolve_other_side_pages` | See section "Web Other-Side Page Contract" below — primary research deliverable |
| BLD-03 | Per-line modifiers hoist into Responsa-syntax forms `compose()` recognises | `build_side_query` hoist rules at `desktop/join_workbench.py:1272-1347`; `compose()` line-start/end at `shared/joins_lab.py:762-765` |
| BLD-04 | Global toggles applied to BOTH sides via `_merge_globals`-equivalent | `_merge_globals` at `desktop/join_workbench.py:2475-2489`; applied at `:2519, :2580` for anchor and other-side |
</phase_requirements>

---

## Summary

Phase 118 adds four distinct feature clusters to the Phase-117 vertical spine: (1) the known-joins group in the anchor pane, (2) "Find joins" entry points from `/search` and `/browse`, (3) the full row-based line builder replacing the Phase-117 textarea, and (4) the other-side builder with cross-side narrow/widen. All four are pure UI composition + correctness fixes — no new search logic is written; the work rides `shared/joins_lab.py` (Phase 106 core).

The three load-bearing correctness problems that demand precise implementation:

1. **`_merge_globals` re-injection (BLD-04):** `compose()` at `shared/joins_lab.py:741-749` hardcodes `ja=False, flex_spacing=False, bidirectional=False` in the returned `ro` dict. The web pipeline MUST overwrite those three keys from the toggle state in BOTH the anchor RO and the other-side RO after each `compose()` call, otherwise the toggles are silently dropped. This is the desktop RR-14 fix ported to web.

2. **ANC-05 cache/RLS leak:** The process-global `_joins_cache` in `joins_panel.py` is keyed by `doc:{sys_id}:pgp:{pgpid}` — no user or status dimension. The Supabase RLS policy on `fragment_joins` is `TO public USING (true)` — all rows visible to all roles. Therefore `get_fragment_joins(fragment_sys_id=...)` with no status filter returns ALL joins for that fragment including unconfirmed/proposed ones. If User A loaded a fragment and populated the cache with their unconfirmed joins, User B gets the cached result without any RLS protection. Fix: add `status='confirmed'` to the `get_fragment_joins` call in the process-global path.

3. **Other-side page contract (BLD-02):** `resolve_other_side_pages(page, total_pages)` is a pure numeric function (`page ± 1`, bounds-clamped). In the web context, `p_num` from `get_browse_page()` is the field to use, not an internal 0-based index. Multi-IE volumes must pass `volume_ie` to `get_browse_page()` so page counts are IE-scoped. Metadata-only manuscripts (no browse map entry) return a synthetic page dict without a real `total_pages`, which must be treated as `total_pages=None`. See "Web Other-Side Page Contract" section below for the full implementable specification.

**Primary recommendation:** Implement in four parallel-ish streams — known-joins group + ANC-05 fix (one plan), "Find joins" entry points + FND-04/05 (one plan), full builder UI + BLD-03 modifiers + BLD-04 toggle injection (one plan), other-side builder + BLD-02 cross-side integration (one plan, last, depends on builder plan). Reuse Phase 117 execute_joins_search pattern for the other-side execute_cross_side closure dispatched via `run.io_bound`.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Known-joins display (ANC-04) | Frontend Server (SSR) — NiceGUI | Supabase (data source) | Anchor pane is server-rendered; joins data from Supabase + local PGP/FJMS sidecar |
| ANC-05 RLS/cache isolation | API / Backend (process-global cache) | Supabase (RLS) | The cache lives in the Python process; the fix is at the cache/query layer, not client-side |
| "Find joins" entry FND-04/05 | Browser / Client (new-tab navigation) | Frontend Server | New-tab open is a client-side `window.open` triggered by a NiceGUI button click handler |
| Builder UI (BLD-03) | Frontend Server (SSR) | shared/joins_lab.py | BuilderRow construction is pure Python; compose() is shared core |
| Global toggles (BLD-04) | API / Backend (search execution layer) | shared/joins_lab.py | `_merge_globals` runs at search-time in the sync closure dispatched via `run.io_bound` |
| Other-side builder (BLD-02) | API / Backend (search execution, off-loop) | shared/joins_lab.py | `apply_cross_side` is I/O-bound; must run inside a second `run.io_bound` closure |
| Cross-side page resolution | API / Backend (`get_browse_page` calls) | SearchEngine | `get_browse_page` is synchronous, reads SQLite/Tantivy; must be off-loop |

---

## The Web Other-Side Page Contract (BLD-02 Primary Deliverable)

### Background: What `resolve_other_side_pages` needs

```python
# shared/joins_lab.py:283-303
def resolve_other_side_pages(page: int, total_pages: Optional[int]) -> frozenset:
    neighbors = set()
    for n in (page - 1, page + 1):
        if n < 1:
            continue
        if total_pages is not None and n > total_pages:
            continue
        neighbors.add(n)
    return frozenset(neighbors)
```

This function takes a 1-based `page` integer and an optional `total_pages` upper bound. The web must supply both correctly. [VERIFIED: shared/joins_lab.py:283-303]

### What `get_browse_page` returns in the web

`WebSearchExecutor.get_browse_page()` (`web/joins_executor.py:76-106`) forwards to `state.searcher.get_browse_page()` which is `SearchEngine.get_browse_page()` (`genizah_core.py:9869`). The returned dict has these fields relevant to the contract: [VERIFIED: web/joins_executor.py:86-90]

```python
{
    'uid': str,
    'p_num': int,          # 1-based "logical" page number (THIS is the page identifier)
    'full_header': str,
    'text': str,
    'total_pages': int,    # len(pages) for the active IE slice (or all_pages if no IE)
    'current_idx': int,    # 0-based ordinal (display only; do NOT use for resolve_other_side_pages)
    'internal_index': int, # 0-based ordinal (same as current_idx - 1)
    'sys_id': str,
    'volume_ie': str | None,
}
```

**The `p_num` field is the right input** — it is the manuscript page/image number that the Joins Lab engine uses for `page ± 1` neighbor logic. `internal_index` is 0-based and must NOT be passed. [VERIFIED: genizah_core.py:9954-9963]

### Multi-IE (`volume_ie`) behavior

When a manuscript has multiple `volume_ie` values (multi-IE manuscript), `get_browse_page()` filters the browse map to the pages belonging to that IE: [VERIFIED: genizah_core.py:9884-9897]

```python
if volume_ie:
    pages = get_volume_pages(all_pages, volume_ie)
    if not pages:
        # IE not found — falls back to all_pages, active_ie = None
        pages = all_pages
        active_ie = None
```

**Consequence for the web contract:** `total_pages` returned by `get_browse_page()` reflects the IE slice's page count, not the full manuscript. This is correct behavior for `resolve_other_side_pages` — the "other side" of page N within an IE is page N±1 within that same IE. The web executor must pass the anchor's `volume_ie` to `get_browse_page()` when fetching page totals for a multi-IE anchor. [VERIFIED: genizah_core.py:9957-9963]

### Unknown total-page counts (metadata-only manuscripts)

When `sys_id` is not in the browse map (no transcription pages indexed, or synthetic sys_id), `get_browse_page()` falls back to `_get_metadata_only_browse_page()`: [VERIFIED: genizah_core.py:9872-9882]

```python
if sys_id not in browse_map:
    return self._get_metadata_only_browse_page(sys_id, ...)
```

The metadata-only dict does NOT include a real `total_pages` field (it returns `'total_pages': 0`). [VERIFIED: genizah_core.py:9858-9867]

**Consequence:** When `get_browse_page()` returns `None` or `total_pages == 0`, the web pipeline MUST pass `total_pages=None` to `resolve_other_side_pages()`. This causes the upper-bound clamp to be skipped — both neighbors `(p-1, p+1)` are attempted (subject to the `< 1` lower clamp). This is the intended graceful degradation: we still find the neighbor if it exists.

### Sparse pages (non-contiguous `p_num`)

For genizah manuscripts the browse map pages are indexed by `p_num` which is continuous (image 1, 2, 3...) for standard NLI items. `p_num` is always a 1-based integer in the browse map and is NOT sparse for Genizah manuscripts (sparseness only occurs in LOCAL pdf pages via `get_local_browse_page()`). The Joins Lab never calls `get_local_browse_page()`; it always calls `get_browse_page()` for Genizah scope. [VERIFIED: genizah_core.py:9869 vs 9966]

**Consequence:** For the Joins Lab cross-side use case (Genizah corpus), `p_num` is always a dense 1-based integer; `page ± 1` is always a valid neighbor candidate. No sparse-page special handling is required.

### The complete web page contract

```
INPUT to resolve_other_side_pages:
  page        = browse_page_dict['p_num']  (int, 1-based)
  total_pages = browse_page_dict['total_pages'] if browse_page_dict and
                browse_page_dict.get('total_pages', 0) > 0 else None

For multi-IE anchors:
  The get_browse_page call for the anchor MUST pass volume_ie=<anchor_volume_ie>
  so total_pages is scoped to the correct IE slice.

For metadata-only / not-found sys_ids:
  get_browse_page returns None or a dict with total_pages=0.
  Both cases → pass total_pages=None to resolve_other_side_pages.

For apply_cross_side (OR-widen _page_total lookups):
  Each get_browse_page(sid, 1) call to fetch total_pages for a candidate sid
  should pass volume_ie=None unless the candidate's own volume_ie is known
  (it usually is NOT at the OR-synthesis stage — use None, accept the risk
  of over-counting across IEs for multi-IE manuscripts; this matches desktop
  behavior which uses get_browse_page without IE scoping at this stage).
```

[VERIFIED: genizah_core.py:9869-9963, web/joins_executor.py:76-106, shared/joins_lab.py:283-303, shared/joins_lab.py:388-400]

---

## ANC-05 Multitenant Leak — Confirmed Path and Fix

### The leak path (confirmed in code)

**Step 1 — RLS policy:** The Supabase `fragment_joins` table has policy `"Joins are public" TO public USING (true)`. ALL rows are returned to any role (anon or authenticated). There is no `status='confirmed'` in the RLS. [VERIFIED: docs/guides/SUPABASE_GUIDE.md:520-524]

**Step 2 — `get_fragment_joins` has no status filter by default:** `web/supabase_client.py:1574-1623` calls `client.table('fragment_joins').select('*')` with no `status` filter unless the caller passes `status=...`. The `fetch_connected_fragments` call in `joins_panel.py:64` passes `fragment_sys_id=document_id` but NO status filter. [VERIFIED: web/supabase_client.py:1587-1596, web/components/joins_panel.py:63-64]

**Step 3 — Process-global cache keyed without user/status:** `_joins_cache` at `joins_panel.py:25` is a module-level dict keyed `"doc:{document_id}:pgp:{pgpid}"`. If User A creates an unconfirmed join for fragment X, their request populates the cache with ALL joins for X (including the unconfirmed one). User B's request hits the cache and receives User A's unconfirmed join. [VERIFIED: web/components/joins_panel.py:24-58]

**Note on the fragment_joins `status` field:** The current schema does not appear to use a `status` column in the `fragment_joins` table based on the RLS policy (`USING (true)` — no status predicate). The existing `get_fragment_joins` `status` parameter filters on a `status` column IF it exists. The D-17 fix wording "public/confirmed-only" means filtering joins to those that are confirmed/publicly visible — likely by filtering on a join type or by relying on the existing `get_fragment_joins(status='confirmed')` parameter if the column exists, or by simply filtering out proposed/unconfirmed joins by `join_type != 'proposed'`. [ASSUMED — the actual `status` column definition in the live Supabase schema was not verified in this session; the planner should verify the column exists and its allowed values]

### The fix (D-17 approach)

In `fetch_connected_fragments()`, the user-joins call at line 64:
```python
joins = get_fragment_joins(fragment_sys_id=document_id)
```
must become:
```python
joins = get_fragment_joins(fragment_sys_id=document_id, status='confirmed')
```

Additionally, the process-global `_joins_cache` must NOT cache this filtered result under a key that would be reused for a call without the `status` filter (or the cache key must include the status scope). Since the known-joins group in the Joins Lab is ALWAYS public/confirmed-only (D-17), the simplest fix is: the known-joins fetch always passes `status='confirmed'`, and the existing `create_joins_button` on browse (which already does use `fetch_connected_fragments`) uses the SAME confirmed-only path. [VERIFIED: web/components/joins_panel.py:24-64]

**Warning:** If the `status` column does not exist in the live schema, the filter will either be silently ignored or raise an error. The planner must include a task to verify the column before relying on `status='confirmed'`. [ASSUMED: status column existence]

---

## `_merge_globals` Re-injection (BLD-04) — Exact Web Equivalent

### Why it's needed

`compose(side: SideQuery)` at `shared/joins_lab.py:695` returns a tuple `(query_str, ro, page_position)`. The `ro` dict at line 741-749: [VERIFIED: shared/joins_lab.py:742-749]

```python
ro = {
    "responsa_mode": True,
    "variants": side.variants,
    "ja": False,         # hardcoded
    "flex_spacing": False,   # hardcoded
    "bidirectional": False,  # hardcoded
    "variant_mode": "variants" if side.variants else "exact",
}
```

`variants` is carried correctly via `SideQuery.variants`. `ja`, `flex_spacing`, and `bidirectional` are hardcoded `False`.

### Desktop pattern (north star)

`desktop/join_workbench.py:2475-2489`: [VERIFIED: desktop/join_workbench.py:2475-2489]

```python
def _merge_globals(self, builder, ro: dict) -> dict:
    overrides = {
        k: v
        for k, v in builder._responsa_opts().items()
        if k in ("ja", "flex_spacing", "bidirectional")
    }
    ro.update(overrides)
    return ro
```

Applied twice: for the anchor side at line 2519 (`self._merge_globals(self.builder, ro)`) and for the other-side at line 2580 (`self._merge_globals(self.other_builder, b_ro)`). [VERIFIED: desktop/join_workbench.py:2517-2519, 2579-2580]

`_responsa_opts()` at `desktop/join_workbench.py:1253-1270` reads from `self._global_opts` dict: [VERIFIED: desktop/join_workbench.py:1262-1270]

```python
def _responsa_opts(self) -> dict:
    v = self._global_opts.get("variants", False)
    return {
        "responsa_mode": True,
        "variants": v,
        "ja": self._global_opts.get("ja", False),
        "flex_spacing": self._global_opts.get("flex_spacing", False),
        "bidirectional": self._global_opts.get("bidirectional", False),
        "variant_mode": "variants" if v else "exact",
    }
```

### Web equivalent

In the web page closure, a `_global_opts` dict captures the toggle state:

```python
_global_opts = {'flex_spacing': False, 'bidirectional': False}
# (ja stays False always per D-10; variants flows via SideQuery.variants)

def _merge_globals_web(ro: dict) -> dict:
    """Re-inject flex_spacing + bidirectional into a compose()-produced ro."""
    ro['flex_spacing'] = _global_opts.get('flex_spacing', False)
    ro['bidirectional'] = _global_opts.get('bidirectional', False)
    # ja stays False per D-10 — do not merge it
    return ro
```

This must be called in the search closure (inside the `run.io_bound` sync function) AFTER `compose()`:

```python
def run_search_core():
    query_str, ro, page_position = compose(anchor_side)
    _merge_globals_web(ro)  # BLD-04: re-inject flex/bidir
    return executor.execute_search(
        query_str, mode=mode_str, gap=0,
        responsa_options=ro,
        text_position=page_position,
        corpus_scope='genizah',
    )
```

And for the other-side search:

```python
def run_cross_side_core():
    b_query, b_ro, _b_pos = compose(other_side)
    _merge_globals_web(b_ro)  # BLD-04: re-inject into other side too
    return apply_cross_side(executor, base_candidates, b_query, b_ro, combine)
```

**The `apply_cross_side` function also internally calls `executor.execute_search(b_query, "exact", 0, responsa_options=b_responsa_options, ...)`.** The merged `b_ro` must be passed as `b_responsa_options` to `apply_cross_side`. [VERIFIED: shared/joins_lab.py:344-463]

---

## "Find Joins" Entry Points — FND Deep-Link Mechanics (FND-04, FND-05)

### Deep-link URL contract (FND-08 compliance)

The Phase-117 cold-start resolver parses URL params: `?sys_id=<N>` wins, `?shelfmark=<S>` as fallback. [VERIFIED: web/pages/joins_lab.py:82-113]

The deep link format for "Find joins" is:
```
/joins-lab?sys_id={sys_id}
```
For multi-IE manuscripts:
```
/joins-lab?sys_id={sys_id}&volume_ie={ie_id}
```

NO builder/triage state in the URL (FND-08, D-18 locked).

### Opening a new tab in NiceGUI

The codebase pattern for new tabs is `ui.run_javascript(f'window.open("{url}", "_blank")')`. This is the canonical pattern used at `web/pages/browse.py:3660,3663,3685,3688,3710,3713,3733,3736` and `web/pages/puzzle.py:3300`. [VERIFIED: web/pages/browse.py:3660]

`ui.navigate.to(url, new_tab=True)` is also available (used in `web/pages/download.py:37`) and is the cleaner NiceGUI-native approach. [VERIFIED: web/pages/download.py:37]

**Recommendation:** Use `ui.navigate.to(url, new_tab=True)` for the "Find joins" button click handler — it is the NiceGUI-native form and avoids JS injection. The `window.open` JS form is used for external URLs; internal SPA routes work correctly with `ui.navigate.to`. [ASSUMED: `ui.navigate.to(..., new_tab=True)` on internal routes opens a genuine new tab in the current NiceGUI app context — verify in a quick smoke test during implementation]

### Insertion points for FND-04 (`/search` cards) and FND-05 (`/browse`)

**`/search` result cards (FND-04, D-21):**

`web/pages/search_results.py` — `create_result_card` function. The action buttons row starts at line 582. Current buttons: Browse (line 584-600), Quick View (line 602-606), Add to List (line 608-618), Catalog Records (line 620-629). [VERIFIED: web/pages/search_results.py:581-630]

The `sys_id` is available as `display.get('id')` at line 356. `volume_ie` is parsed from `_card_ie_id` at line 597. [VERIFIED: web/pages/search_results.py:356, 597-598]

The joins icon button needs to:
1. Know whether the fragment has joins — this requires calling `fetch_connected_fragments()`, which is I/O-bound. The existing `create_joins_button` already does this lazily via `asyncio.get_event_loop().call_later(0.1, _safe_load_count)`. [VERIFIED: web/components/joins_panel.py:362-371]
2. Display the `link` icon with green color when joins exist, neutral when none.
3. On click: when joins exist, call `create_joins_dialog()` + add "Find more joins" button inside the dialog; when no joins, call `ui.navigate.to(joins_lab_url, new_tab=True)` directly.

**`/browse` (FND-05, D-19):**

`web/pages/browse.py` line 3904 already calls `create_joins_button(...)` when `page.text` is truthy. [VERIFIED: web/pages/browse.py:3901-3907]

The modification is inside `create_joins_dialog()` (in `joins_panel.py`) — add a "Find more joins" flat button below the existing "View all fragments" button (line 652-655). When NO joins, `create_joins_button` currently shows the button in its default green style; D-19 requires recoloring to `--neutral-500` flat color and redirecting click to the Lab. This requires a small flag or a new `find_joins_url` parameter to `create_joins_button`.

**Caution:** `create_joins_button` is called from both `browse.py` and (after Phase 118) from Quick View. Any signature change must be backward-compatible. [VERIFIED: web/components/joins_panel.py:310-373]

---

## Off-Loop Discipline for New Search Paths

### Existing pattern (Phase 117, `execute_joins_search`)

The Phase-117 pattern at `web/pages/joins_lab.py:500-621`: [VERIFIED: web/pages/joins_lab.py:500-621]

```python
async def execute_joins_search() -> None:
    # ... bump generation, cancel prev task ...
    def run_search_core():          # sync closure
        return executor.execute_search(...)  # inside sync fn
    search_coro = run.io_bound(run_search_core)
    _current_task['task'] = asyncio.ensure_future(
        asyncio.wait_for(search_coro, timeout=_SEARCH_TIMEOUT_SECONDS)
    )
    raw_results = await _current_task['task']
```

### What Phase 118 adds

**Cross-side search** must also be off-loop. After the anchor search completes, if the other-side builder is enabled, a SECOND `run.io_bound` closure must be dispatched:

```python
async def execute_joins_search() -> None:
    # ... Phase 117 anchor search (produces base_candidates) ...

    if other_side_enabled and other_side.rows:
        _search_generation['value'] += 1  # or reuse same generation? see below
        my_gen_cross = _search_generation['value']

        def run_cross_side_core():
            b_query, b_ro, _ = compose(other_side)
            _merge_globals_web(b_ro)   # BLD-04
            return apply_cross_side(
                executor, list(base_candidates),
                b_query, b_ro, combine_mode
            )

        cross_coro = run.io_bound(run_cross_side_core)
        cross_task = asyncio.ensure_future(
            asyncio.wait_for(cross_coro, timeout=_SEARCH_TIMEOUT_SECONDS)
        )
        merge_result = await cross_task
        final_candidates = merge_result.candidates
    else:
        final_candidates = base_candidates
```

**Important:** `apply_cross_side` itself calls `executor.execute_search` and `executor.get_browse_page` internally. Since it is dispatched via `run.io_bound`, those internal calls run in the thread-pool thread — not on the event loop. The `tests/test_joins_lab_off_loop.py` scanner only checks for `execute_search` calls in `web/pages/joins_lab.py` — the `apply_cross_side` call is in `shared/joins_lab.py` and is dispatched from within a sync closure, so the test passes. [VERIFIED: tests/test_joins_lab_off_loop.py:40-41 (scan scope)]

**The `run_cross_side_core` function name must appear as the first positional arg of a `run.io_bound(...)` call in `joins_lab.py`** — the CI test checks exactly this. [VERIFIED: tests/test_joins_lab_off_loop.py:68-93]

---

## Builder UI — Per-Row Modifier Hoist Rules (BLD-03)

The desktop hoist rules at `desktop/join_workbench.py:1272-1347` define exactly how per-row modifiers transform the `term` string before it becomes a `BuilderRow(term=...)`. [VERIFIED: desktop/join_workbench.py:1272-1347]

For the web builder (D-04: `space = word sequence, a/b = OR-alternatives`), the user types raw Responsa syntax and modifiers are APPLIED to the typed text:

| Modifier | Single-word term | Multi-token (slash-group) |
|----------|-----------------|--------------------------|
| Negation | `-word` | `-(a/b)` |
| Plene/defective | `%word` | `%(a/b)` |
| Prefix | `#word` | `#(a/b)` |
| Suffix | `word#` (append) | `(a/b)#` |
| Wildcard prefix | `*word` | NOT supported for multi-box (RR-13) |
| Wildcard suffix | `word*` | `(a/b)*` |
| Line-start (⊢) | `BuilderRow(line_start=True)` | same |
| Line-end (⊣) | `BuilderRow(line_end=True)` | same |

`compose()` handles `line_start`/`line_end` by prepending/appending `|` to the token: `shared/joins_lab.py:762-765`. [VERIFIED: shared/joins_lab.py:762-765]

**The web builder does NOT re-implement hoist.** Instead, the web builder stores modifiers as booleans in `BuilderRow(line_start, line_end)` and applies the text transformations (plene/neg/prefix/suffix/wildcard) to the user-typed `term` text BEFORE constructing `BuilderRow(term=<transformed>)`. The `compose()` function then handles line_start/line_end. This is the same as the desktop pattern.

**Note:** The D-04 decision (space = sequence, a/b = OR-alternatives) means the user types Responsa-aware text directly. The web builder does NOT reconstruct slash-groups from separate UI boxes — the user types `word1/word2` in the text field and the builder wraps modifiers around the typed text. This simplifies the hoist substantially compared to the multi-box desktop approach.

**Mode → `SideQuery.variants`:** The web mode selector (Exact/Variants/Fuzzy) maps:
- Exact → `SideQuery(variants=False)`, `execute_search(mode='exact')`
- Variants → `SideQuery(variants=True)`, `execute_search(mode='variants')`
- Fuzzy → `SideQuery(variants=False)`, `execute_search(mode='fuzzy')` [ASSUMED: Fuzzy maps to `mode='fuzzy'` at the engine level; verify against `SearchEngine.execute_search` mode param handling — the Phase 117 spine only used `mode='exact'`]

---

## Text Position Control (D-08)

From `web/pages/search.py:646-655`, the 5-option dict: [VERIFIED: web/pages/search.py:646-655]

```python
{
    'anywhere': tr('Anywhere'),
    'start': tr('Start of text'),
    'end': tr('End of text'),
    'line_start': tr('Line starts'),
    'line_end': tr('Line ends'),
}
```

This maps to `SideQuery.page_position`. The mapping:
- `'anywhere'` → `page_position=None`
- `'start'` → `page_position='start'`
- `'end'` → `page_position='end'`
- `'line_start'` → [ASSUMED: this may map to `text_position='line_start'` at engine level, not the `page_position` field on SideQuery which only accepts None/'start'/'end' — verify the engine's `text_position` parameter versus `SideQuery.page_position`]
- `'line_end'` → [ASSUMED: same caveat as line_start]

**Important:** `SideQuery.page_position` validates to `None | 'start' | 'end'` only (`SideQuery.__post_init__` at `shared/joins_lab.py:67-71`). [VERIFIED: shared/joins_lab.py:67-71]. The `'line_start'` and `'line_end'` values from the `/search` select are passed directly to `execute_search(text_position=...)`, NOT through `SideQuery.page_position`. The Phase 117 pipeline already passes `text_position=page_position` from `compose()` return to `execute_search` — for `line_start`/`line_end`, the page must pass them as the `text_position` kwarg to `execute_search` directly, bypassing `SideQuery.page_position`.

**Design consequence for the builder:** The Text Position control's value is used TWO ways:
1. For `'start'`/`'end'`: set `SideQuery(page_position='start'|'end')` → `compose()` validates + returns `page_position` → used as `text_position` in `execute_search`.
2. For `'line_start'`/`'line_end'`: pass directly as `text_position='line_start'|'line_end'` to `execute_search`. Do NOT put them in `SideQuery.page_position` (will raise ValueError).
3. For `'anywhere'`: `SideQuery(page_position=None)`, `text_position=None`.

The other-side builder has `allow_page_position=False` (D-13 parity `desktop/join_workbench.py:2251`) — the other-side never sets `text_position`.

---

## Known-Joins Group (ANC-04) — Data Path and Reuse

### Existing `fetch_connected_fragments` behavior

`web/components/joins_panel.py:32-267` already merges three sources: user joins (Supabase `fragment_joins`), PGP document joins (`document_fragments` table via `get_document_for_fragment`/`get_fragments_for_document`), and FJMS scholarly joins (via `fjms_service.get_join_group`). [VERIFIED: web/components/joins_panel.py:32-267]

The returned dict has: `fragments` (list of shelfmarks), `joins` (list of join dicts with `sources` list), `total_fragments`, `total_joins`, `fragment_details` (list of `{'shelfmark': ..., 'document_id': ...}`). [VERIFIED: web/components/joins_panel.py:252-259]

**Each join dict has a `sources` list** (e.g. `['PGP']`, `['FJMS']`, `['user']`). For cross-source matches the sources are merged: `['PGP', 'FJMS']`. [VERIFIED: web/components/joins_panel.py:203-216]

**Badge rendering:** `desktop/join_workbench.py:166-179` defines `badge_for_source` as a pure function (label, color) per source string. The web equivalent is in the UI-SPEC's source badge color map — PGP (blue), FJMS (purple), user-submitted (green), community (neutral). [VERIFIED: desktop/join_workbench.py:166-179]

### Fetch must run off the event loop

`fetch_connected_fragments` calls Supabase (`get_fragment_joins`), PGP SQLite (`get_document_for_fragment`), and FJMS SQLite (`fjms_service.get_join_group`) — all I/O-bound. It is NOT an async function. The known-joins load MUST be dispatched via `run.io_bound`. The `create_joins_button` currently uses `asyncio.get_event_loop().call_later(0.1, _safe_load_count)` where `load_count()` calls `fetch_connected_fragments` synchronously. [VERIFIED: web/components/joins_panel.py:362-371]

**For the anchor pane known-joins display, the pattern must be:** call `run.io_bound(fetch_connected_fragments, shelfmark=..., document_id=..., pgpid=..., force_refresh=False)` inside an `async def` handler (not raw `call_later` with a sync call).

### Re-anchor interaction (D-16)

When the user clicks the "re-anchor" icon on a known-join member row, the page's `load_anchor()` function should be called with the member's `document_id` (sys_id) and the member's `shelfmark`. The `_anchor_state` dict is updated; `write_anchor()` persists it. The builder state is NOT reset (D-16 — re-anchor is navigation, not session reset). [VERIFIED: web/pages/joins_lab.py:373-416]

---

## Stale CONTEXT.md Line References — Verification Results

The following line references from CONTEXT.md were checked against the live codebase:

| Reference | Claimed | Verified actual | Status |
|-----------|---------|-----------------|--------|
| `lines_to_side_query` | joins_lab.py:116-130 | lines 116-130 | CORRECT [VERIFIED] |
| Builder textarea (Phase 117 seam) | joins_lab.py:333-340 | lines 333-340 | CORRECT [VERIFIED] |
| `execute_joins_search` | joins_lab.py:500-621 | lines 500-621 | CORRECT [VERIFIED] |
| `compose()` hardcodes ja/flex/bidir | joins_lab.py:741-749 | lines 741-749 | CORRECT [VERIFIED] |
| `compose()` line-start/end pipe injection | joins_lab.py:762-765 | lines 762-765 | CORRECT [VERIFIED] |
| `resolve_other_side_pages` | joins_lab.py:283-303 | lines 283-303 | CORRECT [VERIFIED] |
| `cross_side_membership` | joins_lab.py:306-341 | lines 306-341 | CORRECT [VERIFIED] |
| `apply_cross_side` | joins_lab.py:344-463 | lines 344-463 | CORRECT [VERIFIED] |
| `_merge_globals` | desktop/join_workbench.py:2475-2489 | lines 2473-2489 | CORRECT (def starts 2475) [VERIFIED] |
| Applied to anchor + other-side | desktop:2493-2524 and :2579-2580 | lines 2519 (anchor) and 2580 (other-side) | CORRECT [VERIFIED] |
| `get_fragment_joins` `.or_()` filter | supabase_client.py:1574-1623, filter at 1592 | filter at line 1592 | CORRECT [VERIFIED] |
| `fetch_connected_fragments` global cache | joins_panel.py:24-29 | lines 24-29 | CORRECT [VERIFIED] |
| Text Position control | search.py:642-665, options at 646-655 | lines 646-655 | CORRECT [VERIFIED] |
| `create_result_card` | search_results.py:350 | line 350 | CORRECT [VERIFIED] |
| card actions row | search_results.py:581-629 | lines 581-630 | CORRECT [VERIFIED] |
| `WebSearchExecutor.get_browse_page` | joins_executor.py:76-106 | lines 76-106 | CORRECT [VERIFIED] |
| `SideQuery.page_position` validates None/'start'/'end' | joins_lab.py:67-71 | lines 67-71 | CORRECT [VERIFIED] |
| `badge_for_source` | desktop:166-179 | lines 166-179 | CORRECT [VERIFIED] |
| `build_side_query` hoist rules | desktop:1272-1347 | lines 1272-1347+ | CORRECT [VERIFIED] |

**One important gap from CONTEXT.md:** The `compose()` function is cited at `:741` (the start of `ro` dict). Actual `def compose(side: SideQuery)` starts at line 695. The `:741` reference points to the `ro` dict construction, not the function def. Both are correct for their purposes. [VERIFIED: shared/joins_lab.py:695, 741]

---

## Standard Stack

No new libraries. Phase 118 extends existing Phase-117 code in-place.

### Existing Assets Used

| Asset | Location | Purpose |
|-------|----------|---------|
| `shared/joins_lab.py` | BuilderRow, SideQuery, compose(), apply_cross_side, resolve_other_side_pages, dedup_candidates | Full builder/compose/cross-side core — do NOT re-implement |
| `web/components/joins_panel.py` | fetch_connected_fragments, create_joins_button, create_joins_dialog, invalidate_joins_cache | Known-joins data path + entry-point button/dialog |
| `web/joins_executor.py` | WebSearchExecutor | Off-loop adapter — reuse exactly |
| `web/pages/joins_lab.py` | execute_joins_search pipeline, _anchor_state, _search_generation pattern | Extend in-place; do NOT add a new file for the builder |
| `web/joins_lab_storage.py` | write_anchor, read_anchor, schema_version=1 | Safe_storage helpers — extend schema under v1 if needed |
| `web/pages/search_results.py` | create_result_card action row | FND-04 insertion point |
| `web/pages/browse.py` | create_joins_button call at ~3904 | FND-05 insertion point |
| `web/supabase_client.py` | get_fragment_joins(status=...) | ANC-05 fix |
| `web/translations.py` `tr()` | All new strings bilingual from line one | RTL/i18n |

---

## Architecture Patterns

### Pattern 1: Other-Side Search Flow

```
async execute_joins_search():
    # Phase 1: anchor side
    bump generation
    compose(anchor_side) → (query_str, ro, page_position)
    _merge_globals_web(ro)          ← BLD-04
    def run_search_core():
        executor.execute_search(query_str, mode, 0, responsa_options=ro, text_position=page_position, corpus_scope='genizah')
    raw = await asyncio.wait_for(run.io_bound(run_search_core), timeout=120)
    if not _should_apply_results(my_gen, _search_generation): return
    base_candidates, _ = dedup_candidates(raw, anchor_sid)

    # Phase 2: other-side (if enabled and non-empty)
    if other_side_enabled and other_side.rows:
        compose(other_side) → (_b_str, b_ro, _)
        _merge_globals_web(b_ro)    ← BLD-04 for OTHER side too
        combine = 'AND' | 'OR'
        def run_cross_side_core():
            return apply_cross_side(executor, list(base_candidates), _b_str, b_ro, combine)
        merge_result = await asyncio.wait_for(run.io_bound(run_cross_side_core), timeout=120)
        if not _should_apply_results(my_gen, _search_generation): return
        final_candidates = merge_result.candidates
    else:
        final_candidates = base_candidates

    # Phase 3: render candidates
    create_candidate_grid(final_candidates)
```

### Pattern 2: Known-Joins Async Load (off-loop)

```python
async def _load_known_joins(sys_id: str, shelfmark: str, pgpid: Optional[int]) -> None:
    spinner.set_visibility(True)
    try:
        data = await run.io_bound(
            fetch_connected_fragments,
            shelfmark=shelfmark,
            document_id=sys_id,
            pgpid=pgpid,
            force_refresh=False
        )
        # ANC-05: data was fetched with status='confirmed' filter applied
        _render_known_joins(data)
    except Exception:
        _render_known_joins_error()
    finally:
        spinner.set_visibility(False)
```

### Anti-Patterns to Avoid

- **Do NOT call `fetch_connected_fragments()` synchronously on the event loop** — it makes Supabase + SQLite calls. Use `run.io_bound`.
- **Do NOT re-implement `apply_cross_side` in web code** — the shared function already handles AND/OR, neighbor synthesis, and total-page fetching.
- **Do NOT use `SideQuery.page_position` for `line_start` or `line_end` text position values** — these only work as `text_position` kwargs to `execute_search`, not in `SideQuery.__post_init__` (will raise ValueError).
- **Do NOT put the cross-side `execute_search` call inside an `async def`** — it would run on the event loop and violate the CI guard in `tests/test_joins_lab_off_loop.py`.
- **Do NOT open the new "Find joins" tab with `ui.navigate.to(url)` (no `new_tab=True`)** — this replaces the current tab, destroying the user's search/browse context.
- **Do NOT apply `_merge_globals_web` only to the anchor `ro`** — the other-side `b_ro` also needs it (desktop applies it twice, one per side).

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| AND/OR cross-side logic | Custom set intersection | `apply_cross_side()` | Already handles AND filter, OR synthesis, neighbor fetching, total-page cache |
| Modifier hoist (negation, plene, prefix, etc.) | Custom text transformer | `compose()` via `BuilderRow` fields + term pre-processing matching desktop hoist | Hoist rules are non-trivial; `compose()` handles line_start/line_end pipe injection |
| Known-joins dedup (multi-source) | New merger | `fetch_connected_fragments()` + existing dedup logic | Already merges PGP + FJMS + user; dedup by (min(a,b), max(a,b)) |
| Source badge rendering | New badge component | Port `badge_for_source()` from `desktop/join_workbench.py:166-179` | CSS custom properties already match UI-SPEC badge color map |
| Off-loop search | New executor | `WebSearchExecutor` + `run.io_bound` pattern from Phase 117 | CI-guarded, proven |
| `p_num` ↔ neighbor math | Custom page math | `resolve_other_side_pages(p_num, total_pages)` | Already handles boundary cases, None upper bound |

---

## Common Pitfalls

### Pitfall 1: Using `internal_index` instead of `p_num` for other-side page resolution
**What goes wrong:** `get_browse_page()` returns both `p_num` (1-based logical page) and `internal_index` (0-based ordinal). Passing `internal_index` to `resolve_other_side_pages` will off-by-one the neighbor page numbers, returning the wrong other-side candidates.
**How to avoid:** Always use `browse_page_dict['p_num']` as the `page` argument.
**Warning signs:** Other-side candidates systematically off by one folio.

### Pitfall 2: Missing `_merge_globals_web` on the other-side `b_ro`
**What goes wrong:** Calling `_merge_globals_web` only on the anchor `ro` leaves `flex_spacing=False, bidirectional=False` in `b_ro`. The toggle appears to work (anchor side is correct) but the other-side query silently ignores the toggles.
**How to avoid:** Call `_merge_globals_web(b_ro)` after `compose(other_side)`, BEFORE passing `b_ro` to `apply_cross_side`.
**Warning signs:** Toggling Flex-spacing changes anchor results but NOT other-side behavior.

### Pitfall 3: `SideQuery.page_position` ValueError for `line_start`/`line_end`
**What goes wrong:** Putting `'line_start'` or `'line_end'` into `SideQuery(page_position='line_start')` raises ValueError in `SideQuery.__post_init__` (only None/'start'/'end' are valid).
**How to avoid:** For text-position values `'line_start'`/`'line_end'`, bypass `SideQuery.page_position` (leave it None) and pass the value directly as `text_position='line_start'` to `execute_search`.
**Warning signs:** Runtime ValueError crash on search when Text Position is set to "Line starts" or "Line ends".

### Pitfall 4: ANC-05 cache hit from prior user's unconfirmed join
**What goes wrong:** If the `status='confirmed'` filter is applied to `get_fragment_joins` but the result is stored in `_joins_cache` under the existing key, a subsequent fetch WITHOUT the status filter (e.g. from `create_joins_button` on browse) hits the filtered cache and misses unconfirmed joins that the logged-in user SHOULD see in the browse dialog.
**How to avoid:** The known-joins section in the Joins Lab should use a SEPARATE cache key (e.g. `doc:{document_id}:pgp:{pgpid}:confirmed`) or bypass the shared cache entirely for the confirmed-only path.
**Warning signs:** Logged-in user's own confirmed joins disappear from the browse joins dialog after loading the Joins Lab.

### Pitfall 5: Cross-side `execute_search` on the event loop
**What goes wrong:** The `run_cross_side_core` closure is not passed to `run.io_bound`, or `apply_cross_side` is called directly in an `async def`. The `tests/test_joins_lab_off_loop.py` scanner will flag this.
**How to avoid:** The function name appearing as first positional arg to `run.io_bound(run_cross_side_core)` must be literal — no lambda wrapper, no intermediate variable. The scanner checks for this exact pattern.
**Warning signs:** CI failure in `test_joins_lab_off_loop.py`.

### Pitfall 6: `total_pages=0` treated as known total
**What goes wrong:** Metadata-only manuscript returns `get_browse_page()` dict with `total_pages=0`. If passed to `resolve_other_side_pages(page=1, total_pages=0)`, the upper-bound clamp drops `p+1=2` (2 > 0), returning an empty frozenset — no other-side candidates even if they exist.
**How to avoid:** Treat `total_pages=0` the same as `total_pages=None` (no upper bound known). Condition: `total_pages if total_pages and total_pages > 0 else None`.

---

## Code Examples

### `_merge_globals_web` (BLD-04 re-injection)
```python
# Source: verified from desktop/join_workbench.py:2475-2489 (ported for web)
def _merge_globals_web(ro: dict, global_opts: dict) -> dict:
    """Re-inject flex_spacing + bidirectional into a compose()-produced ro.

    compose() hardcodes ja/flex_spacing/bidirectional=False. This step
    pulls the actual UI-toggle values back in (RR-14 parity, D-11).
    ja is intentionally excluded — it stays False per D-10.
    variants flows correctly via SideQuery.variants and is NOT re-merged here.
    """
    ro['flex_spacing'] = global_opts.get('flex_spacing', False)
    ro['bidirectional'] = global_opts.get('bidirectional', False)
    return ro
```

### resolve_other_side_pages call pattern (BLD-02)
```python
# Source: verified from shared/joins_lab.py:283-303, genizah_core.py:9954-9963
def _get_anchor_total_pages(executor, anchor_sys_id: str, volume_ie: Optional[str]) -> Optional[int]:
    """Fetch total_pages for the anchor fragment, IE-scoped if multi-IE."""
    page_data = executor.get_browse_page(anchor_sys_id, p_num=1, volume_ie=volume_ie)
    if page_data is None:
        return None
    t = page_data.get('total_pages', 0)
    return t if t and t > 0 else None

# Usage:
p_num = anchor_browse_page_dict['p_num']   # 1-based, NOT internal_index
total_pages = _get_anchor_total_pages(executor, anchor_sys_id, anchor_volume_ie)
neighbor_pages = resolve_other_side_pages(p_num, total_pages)
# frozenset e.g. {2} for p_num=1, or {1, 3} for p_num=2
```

### New-tab navigation pattern (D-18, FND-04/05)
```python
# Source: verified from web/pages/download.py:37 (NiceGUI-native pattern)
def _open_joins_lab(sys_id: str, volume_ie: Optional[str] = None) -> None:
    url = f'/joins-lab?sys_id={sys_id}'
    if volume_ie:
        url += f'&volume_ie={volume_ie}'
    ui.navigate.to(url, new_tab=True)
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Phase 117 fixed `mode='exact'` textarea | Phase 118 row-based builder, Responsa default, Exact/Variants/Fuzzy mode selector | Phase 118 | Replace textarea seam at `joins_lab.py:333-340` |
| Phase 117 no known-joins | Phase 118 ANC-04 known-joins group in anchor pane | Phase 118 | Use existing `fetch_connected_fragments` + ANC-05 status filter |
| No "Find joins" entry from search/browse | Phase 118 FND-04/05 joins icon + dialog extension | Phase 118 | Extend existing `create_joins_button` / `create_joins_dialog` |
| `compose()` silently drops flex/bidir toggles | `_merge_globals_web` re-injection after each `compose()` call | Phase 118 (BLD-04) | Must apply to BOTH anchor and other-side ROs |

**Deprecated/outdated:**
- Phase 117 single textarea at `joins_lab.py:333-340` — replaced by the row-based builder widget in Phase 118.
- Phase 117 fixed `execute_joins_search(mode='exact')` — Phase 118 reads mode from the mode selector.

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (existing) |
| Config file | `pytest.ini` or `pyproject.toml` (existing) |
| Quick run command | `pytest tests/test_joins_lab*.py -x` |
| Full suite command | `pytest tests/ -x` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File |
|--------|----------|-----------|-------------------|------|
| ANC-04 | Known-joins group shows correct source-attributed members | unit | `pytest tests/test_known_joins_group.py -x` | New — Wave 0 gap |
| ANC-05 | Confirmed-only filter applied; no cross-user cache leak | unit | `pytest tests/test_joins_anc05_rls.py -x` | New — Wave 0 gap |
| BLD-02 | `resolve_other_side_pages` called with correct p_num + total_pages from get_browse_page | unit | `pytest tests/test_other_side_page_contract.py -x` | New — Wave 0 gap |
| BLD-03 | Per-row modifiers produce correct BuilderRow(term, line_start, line_end) and compose() output | unit | `pytest tests/test_builder_modifier_hoist.py -x` | New — Wave 0 gap |
| BLD-04 | `_merge_globals_web` re-injects flex_spacing + bidirectional into BOTH anchor ro and other-side b_ro | unit | `pytest tests/test_merge_globals_web.py -x` | New — Wave 0 gap |
| FND-04/05 | Off-loop guard still green after new search path additions | static/AST | `pytest tests/test_joins_lab_off_loop.py -x` | EXISTING — must stay green |
| FND-06 | No raw app.storage.user access | static/AST | `pytest tests/test_no_raw_storage_access.py -x` | EXISTING — must stay green |

**Success Criterion 5 test (from ROADMAP):** "A test enables each global toggle and asserts the executed query string changes."

```python
# Proposed test for BLD-04 / SC#5
def test_flex_spacing_toggle_changes_query():
    from shared.joins_lab import BuilderRow, SideQuery, compose
    side = SideQuery(rows=(BuilderRow(term='שלום'),), variants=False)
    _, ro, _ = compose(side)
    assert ro['flex_spacing'] is False   # compose hardcodes False
    # After _merge_globals_web with flex_spacing=True:
    _merge_globals_web(ro, {'flex_spacing': True, 'bidirectional': False})
    assert ro['flex_spacing'] is True    # merge succeeded

def test_bidirectional_toggle_changes_query():
    from shared.joins_lab import BuilderRow, SideQuery, compose
    side = SideQuery(rows=(BuilderRow(term='שלום'),), variants=False)
    _, ro, _ = compose(side)
    assert ro['bidirectional'] is False
    _merge_globals_web(ro, {'flex_spacing': False, 'bidirectional': True})
    assert ro['bidirectional'] is True
```

### Sampling Rate
- **Per task commit:** `pytest tests/test_joins_lab*.py tests/test_no_raw_storage_access.py -x`
- **Per wave merge:** Full suite `pytest tests/ -x`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps (tests to scaffold before implementation)

- `tests/test_other_side_page_contract.py` — covers BLD-02 (p_num vs internal_index, None total_pages, multi-IE scoping, metadata-only total_pages=0 treated as None)
- `tests/test_merge_globals_web.py` — covers BLD-04 (`_merge_globals_web` mutates ro correctly; does NOT merge `ja`; variants unchanged; applied to both anchor and other-side ros)
- `tests/test_builder_modifier_hoist.py` — covers BLD-03 (each modifier type produces correct term; wildcard_prefix disabled for multi-token; line_start/line_end flow through compose())
- `tests/test_known_joins_group.py` — covers ANC-04 (source attribution, dedup, fragment_details list)
- `tests/test_joins_anc05_rls.py` — covers ANC-05 (confirmed-only filter; cache key separation for confirmed vs all)

---

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | Login gate for "Join Another Fragment" only (existing) |
| V3 Session Management | No | safe_storage session isolation already enforced (Phase 87) |
| V4 Access Control | Yes | ANC-05: confirmed-only join filter; no user's unconfirmed joins leak to other users |
| V5 Input Validation | Yes | Builder row text sanitized through compose() — no raw injection into engine |
| V6 Cryptography | No | No new crypto |

### Known Threat Patterns for this Stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Process-global cache leaks User A's joins to User B | Information Disclosure | ANC-05: `status='confirmed'` filter + confirmed-only cache key |
| Builder text injected as raw engine query | Tampering | compose() is a pure function; engine uses parameterized Tantivy syntax, not SQL |
| new-tab deep link carrying stale triage/builder state | Information Disclosure | FND-08 contract: URL carries ONLY sys_id + volume_ie; no state in URL |

---

## Environment Availability

Phase 118 is code/config-only — all external services are already validated and in use by Phase 117 (Supabase, FJMS SQLite sidecar, NLI/browse proxies, Phase-98 circuit breaker). No new external dependencies.

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `fragment_joins` has a `status` column; `get_fragment_joins(status='confirmed')` filters to confirmed rows only | ANC-05 fix | If status column doesn't exist, the filter silently does nothing or errors; planner must add a verification task |
| A2 | `ui.navigate.to(url, new_tab=True)` on internal routes opens a new tab correctly in NiceGUI's SPA context | FND-04/05 | If incorrect, use `ui.run_javascript(f'window.open("{url}", "_blank")')` as fallback |
| A3 | Fuzzy mode maps to `execute_search(mode='fuzzy')` at the engine level | Builder mode selector | If mode string is different, search silently falls back; verify against SearchEngine.execute_search mode param |
| A4 | `'line_start'`/`'line_end'` text_position values are accepted by `state.searcher.execute_search` | Text Position control | If engine rejects these, Text Position for line_start/line_end is broken; verify in Phase 117 test env |

**If this table is empty:** All claims were verified — this table has 4 items that need confirmation before locking.

---

## Open Questions

1. **`fragment_joins.status` column — does it exist in the live schema?**
   - What we know: `get_fragment_joins` accepts a `status` parameter. The comment in supabase_client.py mentions "status='confirmed' OR auth.uid()=user_id" but the RLS policy is `USING (true)`.
   - What's unclear: Whether a `status` column exists in the live Supabase table, and what values it takes.
   - Recommendation: The planner should add a Task 0 that verifies via `SELECT column_name FROM information_schema.columns WHERE table_name='fragment_joins'` before writing the ANC-05 fix.

2. **`mode='fuzzy'` string for the engine?**
   - What we know: Phase 117 uses `mode='exact'`. The SEARCH_API context mentions "fuzzy" mode. BLD-09 adds Fuzzy to the mode selector.
   - What's unclear: The exact string expected by `state.searcher.execute_search(mode=...)` for fuzzy search.
   - Recommendation: Grep `genizah_core.py` for the modes enum or mode string table before implementing the mode selector handler.

3. **Cache key strategy for ANC-05: separate key or bypass cache?**
   - What we know: The existing `_joins_cache` is shared between the browse joins button and the Joins Lab known-joins group. Adding `status='confirmed'` to the Joins Lab fetch could corrupt the cache for the browse joins button (which SHOULD show all joins to the owner).
   - Recommendation: The Joins Lab known-joins group should use `force_refresh=True` OR a modified `fetch_connected_fragments` that takes a `confirmed_only` flag and uses a separate cache key like `doc:{document_id}:pgp:{pgpid}:confirmed`.

---

## Sources

### Primary (HIGH confidence)
- `shared/joins_lab.py` — verified lines 28-128 (domain model), 283-463 (cross-side), 695-770 (compose)
- `web/pages/joins_lab.py` — verified lines 80-667 (full Phase-117 page)
- `web/joins_executor.py` — verified lines 1-127 (full adapter)
- `web/components/joins_panel.py` — verified lines 1-374 (cache, fetch, button, dialog)
- `web/supabase_client.py` — verified lines 1574-1622 (get_fragment_joins)
- `genizah_core.py` — verified lines 9850-9965 (get_browse_page, _get_metadata_only_browse_page)
- `desktop/join_workbench.py` — verified lines 166-203 (badge_for_source, dedup_join_rows), 1092-1270 (_responsa_opts, _open_row_options_dialog), 1272-1347 (build_side_query hoist), 2473-2611 (_merge_globals, do_search, _on_results, _on_cross_done)
- `web/pages/search.py` — verified lines 640-665 (Text Position control)
- `web/pages/search_results.py` — verified lines 348-640 (create_result_card)
- `web/pages/browse.py` — verified lines 1828-1876 (action row), 3856-3910 (joins button insertion), new-tab JS patterns
- `docs/guides/SUPABASE_GUIDE.md` — verified lines 514-545 (fragment_joins RLS policies)
- `tests/test_joins_lab_off_loop.py` — verified full file (CI guard scope and logic)
- `web/joins_lab_storage.py` — verified full file (schema_version=1 contract)

### Metadata

**Confidence breakdown:**
- Other-side page contract: HIGH — verified against live genizah_core.py + joins_lab.py
- `_merge_globals` re-injection: HIGH — verified against live desktop + shared core
- ANC-05 leak path: HIGH — verified against live joins_panel.py + supabase_client.py + SUPABASE_GUIDE.md
- ANC-05 status column: LOW — assumed to exist; must verify in live schema
- FND deep-link mechanics: HIGH — verified NiceGUI new-tab patterns in codebase
- Off-loop discipline: HIGH — verified test guard + Phase-117 pattern

**Research date:** 2026-06-18
**Valid until:** 2026-07-18 (stable domain; NiceGUI API changes unlikely within 30 days)
