# Phase 119: Candidates, Compare & Visual Similarity — Research

**Researched:** 2026-06-19
**Domain:** NiceGUI web UI extension — candidate triage surface, full-screen Compare modal, VS toggle; parity port of desktop Joins Lab Component A
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Compare (CMP-01 / CMP-02 / CMP-03):**
- D-01: Full-screen modal overlay (`ui.dialog` maximized), anchor|candidate panes, each reusing the extracted `/browse` image viewer (Phase 117 D-10), per-pane independent folio navigation + transcription. Desktop parity: `join_workbench.py:3724`, `_fill_anchor:4051`, `_fill_candidate:4086`.
- D-02: Flip-through ‹ Prev / Next › steps through candidates in the current sort/filter order while Compare stays open. Parity `step(delta)` over `wb.filtered` at `:3741`/`:3753`.
- D-03: Y/?/N verdict buttons in Compare; recording a verdict AUTO-ADVANCES to the next candidate. Verdict syncs immediately to the `sys_id`-keyed triage (shared with grid + table). Parity `_mark → wb.mark(sys_id,val) → triage[sys_id]` at `:4202`/`:4981`/`:3344`.

**Visual Similarity (VSM-01 / VSM-02):**
- D-04: Single 👁 toggle — conditional model: ON + builder has query → INTERSECTION (`c.via_text AND c.via_vs`); ON + empty builder → UNION (pure VS browse, `merge_candidates([], vs)`); OFF → text-only but 👁 badge on text hits that are also look-alikes. Desktop parity: `:2788-2802`.
- D-05: Thin web VS adapter — `get_vs_service().get_suggestions(sys_id, limit=200)` → map to `Candidate(via_vs=True, vs_rank=..., vs_score=...)` → `merge_candidates`. Run off-loop via `run.io_bound`. No circuit breaker (LOCAL SQLite). Mirror pattern from `visual_similarity_dialog.py:176`.
- D-06: Toggle tracks loaded anchor sid — look-alikes invalidate/refetch on re-anchor. All four states (OFF / Loading / ON with results / ON empty intersection / no VS data) have explicit non-blank affordances.
- D-07: 👁 badge via `shared/joins_lab.badge_and_tooltip()` precedence: `⚓ is_anchor_self › ⇄ via_other_side › 👁 via_vs`. Consistent across grid, table, Compare. Desktop parity: `:452-457`.

**Candidate surface — bounding (CND-07):**
- D-08: Paginate (~24/page in grid); pagination replaces Phase-117's `_MAX_RENDERED_CANDIDATES = 200` silent cap. Filters apply BEFORE pagination; triage persists across page changes. Enrichment batch covers the FULL filtered set (not just current page).

**Candidate surface — views (CND-02 / CND-03 / CND-04):**
- D-09: Grid default, LARGE thumbnails (160×160px per UI-SPEC). Grid responsive: `grid-cols-1 sm:grid-cols-2 lg:grid-cols-3`.
- D-10: Table view 8-column shape (Checkbox | Shelfmark | Score | Snippet | Material | Dimensions | Page | Triage). Web ADDS sortable columns + multi-select (desktop is sort-disabled). Default sort by score; switch to VS rank when 👁 ON. Both views share the SAME `sys_id`-keyed triage + 👁 badge state.
- D-11: Triage `dict[str, Literal['yes','maybe','no']]` keyed by `sys_id`, in-memory page state this phase. Resets on re-anchor. Phase 120 adds persistence (PST-01) — 119 does NOT write to `safe_storage`.
- D-12: Multi-select ships with BULK TRIAGE in 119 (mark all selected Y/?/N). Selection state structured for Phase 120 bulk ACT-02/03 actions. Those actions are NOT in 119.

**Self-match (CND-05):**
- D-13: Silent exclusion via `dedup_candidates(include_self=False)`. NO banner, NO readout. `detect_self_match` still runs. DOCUMENTED DIVERGENCE from CND-05 "readout/banner" wording and ROADMAP SC#2. Verifier MUST NOT fail the phase for missing a banner — CND-05 is satisfied by correct exclusion, not by UI surface.

**Filters (CND-06):**
- D-14: Filters in a `ui.dialog` popover (not inline), opened by "Filters" button. Dimensions: material / has-dimensions / size-mismatch / triage-state. Text-filter field included (discretion resolved by UI-SPEC). Active filter count badge on button.
- D-15: Size-mismatch formula `ratio = max(w, anchor_w) / min(w, anchor_w) > 1.4`. Anchor's `width_cm` from enrichment. Desktop parity `:1687-1695`.

**Enrichment (CND-08):**
- D-16: Off-loop batched via `shared/fjms_service.get_measurement_summaries_batch(sys_ids)` per `sys_id`. LOCAL `fjms_enrichment.db` — no circuit breaker. THUMBNAIL image fetches use existing per-provider proxy + Phase-98 NLI circuit breaker. Enrichment feeds filters + table columns.

### Claude's Discretion

- Table default sort column and VS-rank-when-👁-on sort switch (D-10) — resolved in UI-SPEC: default=score desc; VS on=VS rank asc.
- Exact thumbnail/card dimensions — resolved in UI-SPEC: 160×160px thumbnails.
- Empty / disabled / no-VS-data / empty-intersection state wording — resolved in UI-SPEC copywriting contract.
- Whether the parity text-filter field is included in the filter dialog (D-14) — resolved in UI-SPEC: included.
- Grid responsive breakpoints — resolved in UI-SPEC: `grid-cols-1 sm:grid-cols-2 lg:grid-cols-3`.
- Pagination control styling — resolved in UI-SPEC: flat buttons + counter.
- Exact Compare launch affordances beyond card/row/double-click (D-02).
- Self-match internal handling beyond silent exclusion (D-13).
- Verdict-button layout in Compare.

### Deferred Ideas (OUT OF SCOPE)

- Bulk Add-to-Puzzle / Add-to-List / Add-as-join / Export from selected → **Phase 120** (ACT-01/02/03).
- Cross-refresh / cross-navigation persistence of triage/filter/view + re-run-on-restore → **Phase 120** (PST-01..03).
- Self-match banner / "include anchor" toggle — declined (D-13); could be revisited as later polish.
- Complete i18n / RTL / Hebrew-leak audit → **Phase 121**.

</user_constraints>

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| CND-03 | User can view candidates in a table surface (sortable columns, multi-select) | D-10 locked; `ui.table` with `row-key`, sortable columns, `selection='multiple'` in NiceGUI; see Architecture Patterns |
| CND-04 | User can triage each candidate Yes/Maybe/No; triage keyed by `sys_id`, reflected consistently across grid, table, Compare, resets on re-anchor | D-11 locked; single in-memory dict; restyle pattern from desktop `_restyle_card:3344` |
| CND-05 | Lab surfaces self-match readout when anchor appears in its own results | D-13 locked divergence: silent exclusion ONLY, no banner; `dedup_candidates(include_self=False)` + `detect_self_match` already in `shared/joins_lab.py:505/601` |
| CND-06 | User can filter candidates (material / dimensions / size-mismatch / triage state) | D-14/D-15 locked; `ui.dialog` popover; `get_measurement_summaries_batch` provides material/dims |
| CND-07 | Candidate surface is bounded — pagination so large set never renders unbounded | D-08 locked; replaces `_MAX_RENDERED_CANDIDATES=200` in `candidate_grid.py:45`; 24/page |
| CND-08 | Candidate metadata enriched off event loop, batched, breaker-guarded for images | D-16 locked; `run.io_bound(fjms_svc.get_measurement_summaries_batch, sys_ids)` pattern |
| CMP-01 | User can open a side-by-side Compare of anchor and chosen candidate | D-01 locked; `ui.dialog(props='maximized')`, reuse AnchorViewer for each pane |
| CMP-02 | Compare supports per-pane zoom and folio navigation | D-01/D-02 locked; AnchorViewer already has `_VIEWER_HEAD` idempotency guard for two instances |
| CMP-03 | User can record a verdict from Compare, synced with `sys_id`-keyed triage | D-03 locked; auto-advances on verdict; triage dict is shared single source of truth |
| VSM-01 | 👁 toggle merges FIST look-alikes; conditional union/intersection; explicit disabled/empty states | D-04/D-05/D-06 locked; VS adapter wraps `get_vs_service().get_suggestions`; `merge_candidates` in `shared/joins_lab.py:547` |
| VSM-02 | Visually-similar candidates carry consistent 👁 badge across grid, table, Compare | D-07 locked; `badge_and_tooltip()` precedence helper — **must be added to `shared/joins_lab.py` (Wave 0 gap)** |

</phase_requirements>

---

## Summary

Phase 119 is a **parity port of the UAT-approved desktop Joins Lab candidate surface** onto the existing web seams established in Phases 117 and 118. It does not implement new search algorithms, new database schemas, or new services — it wires existing shared-core functions (`merge_candidates`, `dedup_candidates`, `detect_self_match`, `get_vs_service`, `get_measurement_summaries_batch`) into a NiceGUI UI that extends `web/components/candidate_grid.py` and `web/pages/joins_lab.py`.

The three features form a coherent working surface: (1) the candidate grid/table grows from read-only into a triage workspace with filters, pagination, and VS badges; (2) a full-screen Compare modal opens the `/browse`-viewer-based side-by-side panel with flip-through + verdict-auto-advance; (3) a 👁 toggle fetches VS look-alikes and merges them via the established conditional intersection/union model.

Four hard invariants bind EVERY path: off-loop discipline (`run.io_bound` + generation counter), image fetches through the per-provider proxy + Phase-98 NLI circuit breaker (thumbnails only; LOCAL SQLite reads are off-loop but breaker-free), zero raw `app.storage.user` (Phase-87 multitenant invariant), and bilingual strings via `tr()` from line one. The existing CI guards (`tests/test_joins_lab_off_loop.py`, `tests/test_no_raw_storage_access.py`) must stay green, and the new VS lookup + enrichment batch must be covered by the same off-loop guard.

**Primary recommendation:** Structure implementation in three waves — Wave 0 adds `badge_and_tooltip()` to `shared/joins_lab.py` and writes test stubs; Wave 1 refactors `candidate_grid.py` into the working triage/pagination/VS-badge surface; Wave 2 adds the Compare modal + flip-through + verdict; Wave 3 wires the VS toggle and enrichment batch.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Candidate triage state | Frontend Server (page-level Python dict) | — | In-memory per-session, no Supabase (Phase 120 adds persistence via `safe_storage`) |
| Grid / table rendering | Frontend Server (NiceGUI/SSR) | Browser (Quasar interactivity) | NiceGUI renders server-side; Quasar table sorting + checkbox is client-side but driven by server updates |
| Pagination logic | Frontend Server (Python slice) | — | Filter then slice; server controls which candidates reach the wire |
| VS lookup | Frontend Server (off-loop) | Local SQLite (visual_similarity.db) | `run.io_bound` dispatch; result merges into page state |
| Enrichment (material/dims) | Frontend Server (off-loop) | Local SQLite (fjms_enrichment.db) | `run.io_bound` dispatch; feeds filter predicates + table cells |
| Thumbnail image URLs | API/Backend (proxy endpoints) | NLI circuit breaker | `/api/nli_image_by_sysid/`, `/api/oxford_image/` etc. — never direct IIIF |
| Compare image viewing | Frontend Server + AnchorViewer | Browser (zoom/pan JS) | AnchorViewer resolves via `service.get_browse_page()` off-loop; JS handles interactive zoom/pan |
| Badge precedence logic | Shared library (shared/joins_lab.py) | — | `badge_and_tooltip()` is pure, no tier dependency |
| Filter evaluation | Frontend Server (Python predicates) | — | Pure comparison against enrichment dict; no network |

---

## Standard Stack

### Core (all existing — nothing new to install)

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| NiceGUI | Existing (project-wide) | Page/component factory, `ui.dialog`, `ui.table`, `ui.switch`, `ui.card`, `run.io_bound` | Locked project stack |
| Quasar | Via NiceGUI | `ui.table` with `selection='multiple'`, sortable columns, `props='maximized'` on dialog | NiceGUI wraps Quasar |
| Tailwind CSS | Via NiceGUI `.classes()` | Responsive grid, spacing tokens | Locked project stack |
| `shared/joins_lab.py` | Project module | `merge_candidates`, `dedup_candidates`, `detect_self_match`, `badge_and_tooltip` | Shared core, do not re-implement |
| `shared/visual_similarity_service.py` | Project module | `get_vs_service().get_suggestions(sys_id, limit=200)` | Existing VS service |
| `shared/fjms_service.py` | Project module | `get_fjms_service().get_measurement_summaries_batch(sys_ids)` | Existing enrichment service |
| `web/components/anchor_viewer.py` | Project module | `AnchorViewer` / `inject_viewer_assets()` — reused for BOTH Compare panes | Phase-117 extracted viewer |
| `web/translations.py` | Project module | `tr()` bilingual strings | Phase-87 invariant |

**No new packages to install.** This phase is UI extension + adapter wiring.

---

## Package Legitimacy Audit

> Not applicable — Phase 119 introduces zero new external packages. All UI is built from existing NiceGUI primitives, Quasar props, and project modules.

**Packages removed due to slopcheck:** none
**Packages flagged as suspicious:** none

---

## Architecture Patterns

### System Architecture Diagram

```
User browser
     │
     ▼
NiceGUI SSR (web/pages/joins_lab.py)
     │
     ├─► Toolbar [Grid/Table toggle | Filters | 👁 VS toggle | Pagination]
     │         │
     │         ├── Filters popover (ui.dialog)
     │         │      └── Material / Has-dims / Size-mismatch / Triage / Text
     │         │
     │         ├── Grid view (web/components/candidate_grid.py — extended)
     │         │      └── Card [160×160 thumb | chip | shelfmark | title | Y/?/N | 👁 | Compare btn]
     │         │
     │         └── Table view (new, lazy-rendered on first switch)
     │                └── Row [☐ | shelfmark 👁 | score | snippet | material | dims | page | Y/?/N]
     │
     ├─► Compare modal (ui.dialog maximized)
     │         ├── AnchorViewer pane (left) ← reused Phase-117 viewer
     │         ├── AnchorViewer pane (right) ← same viewer, different sys_id
     │         └── Verdict bar [‹ Prev | Y Yes | ? Maybe | N No | Next › | counter]
     │
     ├─► Off-loop VS lookup
     │      run.io_bound → get_vs_service().get_suggestions(anchor_sid, 200)
     │                   → map to Candidate(via_vs=True) → merge_candidates(text, vs)
     │
     ├─► Off-loop enrichment batch
     │      run.io_bound → get_fjms_service().get_measurement_summaries_batch(all_sys_ids)
     │                   → populate material/dims dict → filter predicates + table cells
     │
     └─► Image fetches (existing proxy paths, unchanged)
            /api/nli_image_by_sysid/ ──► NLI circuit breaker (Phase 98)
            /api/oxford_image/ ──────────► Bodleian direct
```

**Data flow for triage:**
```
User clicks Y/?/N on card/row/Compare verdict
  → update triage_dict[sys_id]
  → restyle ALL rendered surfaces for that sys_id (grid cards + table rows + Compare verdict bar)
  → NO safe_storage write (Phase 120)
```

### Recommended Project Structure

```
web/
├── pages/
│   └── joins_lab.py         # extend: add VS toggle state, enrichment dispatch,
│                            # candidates_container → CandidateSurface factory call
├── components/
│   ├── candidate_grid.py    # extend: triage / table view / filters / pagination / VS badge
│   └── compare_modal.py     # NEW: CompareModal factory (full-screen dialog + AnchorViewer × 2)
shared/
└── joins_lab.py             # extend: add badge_and_tooltip() pure helper (Wave 0 gap)
```

### Pattern 1: Off-loop Dispatch (existing — must be extended)

**What:** Blocking I/O (SQLite reads, search) runs inside a sync closure passed to `run.io_bound`. The closure is the SOLE site of the blocking call. The literal closure name is what the CI guard `tests/test_joins_lab_off_loop.py` checks against the `io_bound_args` set.

**When to use:** VS lookup, enrichment batch, any new SQLite access.

**Example:**
```python
# Source: web/pages/joins_lab.py (verified — run_search_core pattern)
async def execute_joins_search() -> None:
    def run_search_core():
        return executor.execute_search(query_str, mode=mode_str, ...)

    search_coro = run.io_bound(run_search_core)  # literal name = CI guard
    results = await asyncio.wait_for(search_coro, timeout=_SEARCH_TIMEOUT_SECONDS)
```

**Phase 119 extension — VS lookup:**
```python
# Pattern from visual_similarity_dialog.py:176 (verified in codebase)
async def _fetch_vs_candidates(anchor_sid: str) -> list:
    def run_vs_core():
        vs_svc = get_vs_service(thread_safe=True)
        return vs_svc.get_suggestions(anchor_sid, 200)

    raw = await run.io_bound(run_vs_core)
    return [Candidate(sys_id=r['alma_id'], page=None, uid=f"{r['alma_id']}|vs",
                      via_vs=True, vs_rank=r['rank'], vs_score=r['svm_score'])
            for r in raw]
```

**Phase 119 extension — enrichment batch:**
```python
# Pattern analogous to desktop _EnrichWorker (verified: get_measurement_summaries_batch)
async def _enrich_candidates(sys_ids: list[str]) -> dict:
    def run_enrich_core():
        return get_fjms_service(thread_safe=True).get_measurement_summaries_batch(sys_ids)

    return await run.io_bound(run_enrich_core)
    # Returns {sys_id: {'width_cm': ..., 'height_cm': ..., 'material': ..., ...}}
```

### Pattern 2: Triage State — Single Source of Truth

**What:** One `dict[str, Literal['yes','maybe','no']]` keyed by `sys_id`, held as a page-level closure variable. All grid cards, table rows, and the Compare modal read from and write to the same dict. A restyle function propagates any change to all surfaces immediately.

**When to use:** EVERY triage write (card button, table triage column, Compare verdict bar, bulk triage bar) goes through one function that updates the dict and triggers a restyle.

**Pattern:**
```python
# Page-level closure (not safe_storage — Phase 120 adds persistence)
_triage: dict[str, str] = {}   # 'yes' | 'maybe' | 'no'
_selected: set[str] = set()    # table multi-select

def _set_triage(sys_id: str, verdict: str) -> None:
    _triage[sys_id] = verdict
    _restyle_all(sys_id)        # update card borders, table row color, Compare bar

def _set_triage_bulk(sys_ids: list[str], verdict: str) -> None:
    for sid in sys_ids:
        _triage[sid] = verdict
    for sid in sys_ids:
        _restyle_all(sid)
```

### Pattern 3: Compare Modal with Two AnchorViewer Instances

**What:** `ui.dialog(props='maximized')` containing two columns; each column creates an `AnchorViewer`. The `_VIEWER_HEAD` idempotency guard (`window._msViewerLoaded`) in `anchor_viewer.py` ensures `inject_viewer_assets()` is safe to call twice (the second call is a no-op). Both viewers are independent (separate `sys_id`, separate `p_num` tracking).

**When to use:** Compare modal (CMP-01/CMP-02).

**Key insight from AnchorViewer source (verified):**
- `inject_viewer_assets()` must be called at PAGE-BUILD time (once, in `create_joins_lab_page`), NOT inside the Compare modal factory — the guard handles idempotency.
- Each viewer instance is created with its own `sys_id` and calls `update_content(p_num=N)` to navigate pages.
- The Compare modal does NOT own the anchor pane viewer — it creates a SEPARATE second viewer for the anchor side (to avoid conflicting page state with the sticky anchor pane).

### Pattern 4: badge_and_tooltip() — Wave 0 Gap

**What:** A pure helper that takes a `Candidate` and returns `(icon: str, tooltip: str)` using the fixed precedence: `⚓ is_anchor_self › ⇄ via_other_side › 👁 via_vs`. Desktop parity: `join_workbench.py:452-457`.

**Critical finding:** `badge_and_tooltip()` is referenced in CONTEXT.md D-07 and UI-SPEC as `shared/joins_lab.badge_and_tooltip()`, but it does **NOT** currently exist in `shared/joins_lab.py` (verified by grep). This is a **Wave 0 gap** — the function must be added before any component that renders badges can be implemented.

**Proposed signature:**
```python
# To add to shared/joins_lab.py
def badge_and_tooltip(cand: Candidate) -> tuple[str | None, str]:
    """Return (icon_name_or_None, tooltip_text) for a candidate badge.

    Precedence (desktop parity join_workbench.py:452-457):
      ⚓ is_anchor_self   → ('anchor', 'Anchor fragment')
      ⇄ via_other_side   → ('swap_horiz', 'Found via other side')
      👁 via_vs           → ('visibility', 'Visually similar')
      (none)             → (None, '')
    """
```

### Pattern 5: Pagination Without Candidate Cap

**What:** Remove `_MAX_RENDERED_CANDIDATES = 200` as the rendering bound. Instead, paginate: slice the filtered candidate list to `[page_start:page_start+page_size]`, render only that slice. The filter logic operates on the FULL candidates list before slicing.

**Page state variables:**
```python
_current_page: dict = {'value': 0}   # 0-indexed
_page_size = 24                       # D-08: ~24 per page (UI-SPEC)
```

**When filter changes:** reset `_current_page['value'] = 0` and re-render.
**When triage changes:** update triage dict only — do NOT reset page.
**Enrichment:** triggered once for the FULL filtered set's `sys_id` list, not just the current page.

### Anti-Patterns to Avoid

- **Calling `get_suggestions` or `get_measurement_summaries_batch` directly in an async def:** Both are blocking SQLite calls; they must always be inside a sync closure passed to `run.io_bound`. The CI guard (`test_joins_lab_off_loop.py`) checks for `execute_search` calls — the new VS and enrichment calls are not yet in scope of the guard, but the planner should add tests with the same pattern.
- **Writing triage to `app.storage.user` directly:** Zero raw storage access (Phase-87 invariant). 119's triage is in-memory Python dict only — no storage writes.
- **Reinventing `merge_candidates` or `dedup_candidates`:** Both exist as pure functions in `shared/joins_lab.py`. Do not rewrite VS intersection/union logic in the page.
- **Separate badge logic per surface:** `badge_and_tooltip()` must be the SINGLE source of badge decision. Do NOT add `if cand.via_vs:` logic inline in the card or table row renderers.
- **Using `vs_score=None` as "not visually similar":** Per `Candidate.vs_score` docstring (verified), `None` means "no VS data", NOT "dissimilar". A score of 0.0 would mean dissimilar. Never treat `None` as a falsy VS indicator.
- **Creating two `inject_viewer_assets()` calls at build time:** One call in `create_joins_lab_page` is sufficient; the idempotency guard covers the Compare modal's viewer instances.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| VS candidate merging / tiering | Custom union/intersection logic | `shared/joins_lab.merge_candidates(text, vs)` | Already implements tier0/1/2 ordering with `vs_rank` stability |
| Candidate dedup + self-exclusion | Custom dedup dict | `shared/joins_lab.dedup_candidates(raw, anchor_sid, include_self=False)` | Returns `(list, anchor_matched)` with correct `via_text=True` flag |
| Self-match detection | Scanning results for `sys_id == anchor` | `shared/joins_lab.detect_self_match(raw_results, anchor_sid)` | Pure function, tested |
| Badge rule | Inline `if via_vs / via_other_side` conditionals | `shared/joins_lab.badge_and_tooltip(cand)` (add in Wave 0) | Consistent precedence; single source of truth |
| VS service lookup | Direct SQL on `visual_similarity.db` | `shared/visual_similarity_service.get_vs_service().get_suggestions(sid, 200)` | Thread-safe singleton, per-thread connections, error handling |
| Measurement enrichment | Direct SQL on `fjms_enrichment.db` | `shared/fjms_service.get_fjms_service().get_measurement_summaries_batch(sys_ids)` | Batch SQL, thread-safe, deduplication, graceful error |
| Image URLs | Direct `iiif.nli.org.il` URLs | `build_thumbnail_url(sys_id, page, shelfmark, library_code)` in `candidate_grid.py` | Proxy-only; NLI circuit breaker path; Oxford fork |
| Fragment image viewer | Custom image+zoom component | `AnchorViewer` from `web/components/anchor_viewer.py` | Already has zoom/pan JS, folio nav, transcription, idempotency guard |

---

## Runtime State Inventory

> Not applicable — this is a greenfield feature addition (new UI surfaces). No existing runtime state carries identifiers being renamed or migrated.

---

## Common Pitfalls

### Pitfall 1: Off-loop CI guard scope — VS and enrichment not yet covered

**What goes wrong:** The CI guard `tests/test_joins_lab_off_loop.py` currently only scans for `execute_search` calls in `joins_lab.py`. New VS lookup and enrichment batch calls are NOT checked by the existing guard. A developer could accidentally call them in an async def and the CI would not catch it.

**Why it happens:** The guard was written for Phase 117's search-only scope; VS and enrichment were not in scope yet.

**How to avoid:** Add new AST guard sub-tests (or extend the existing guard) to verify that `vs_svc.get_suggestions` and `fjms_svc.get_measurement_summaries_batch` calls in `joins_lab.py` also reside in sync closures passed to `run.io_bound`.

**Warning signs:** `asyncio.get_event_loop is blocked` errors in logs; UI freezing during VS toggle; enrichment taking 2-3 seconds with the UI unresponsive.

### Pitfall 2: badge_and_tooltip does not exist yet

**What goes wrong:** Any code importing `from shared.joins_lab import badge_and_tooltip` will raise `ImportError` until Wave 0 creates the function.

**Why it happens:** CONTEXT.md refers to this helper as if it exists (desktop parity at `:452-457`), but the web-side `shared/joins_lab.py` only received the functions added through Phase 107 — `badge_and_tooltip` was never ported.

**How to avoid:** Make the Wave 0 task explicit: add `badge_and_tooltip(cand: Candidate) -> tuple[str | None, str]` to `shared/joins_lab.py` with a corresponding test before any component task runs.

**Warning signs:** `ImportError: cannot import name 'badge_and_tooltip' from 'shared.joins_lab'`.

### Pitfall 3: Two AnchorViewer instances — per-pane page isolation

**What goes wrong:** If both Compare panes track folio navigation in the same dict key, navigating the candidate pane also moves the anchor pane.

**Why it happens:** `AnchorViewer` tracks `_current_page` internally via a closure variable, but the comparison pane viewer must have its own independent `_current_page`.

**How to avoid:** Instantiate two SEPARATE `AnchorViewer` instances — one for the anchor, one for the candidate. Do NOT reuse the sticky anchor pane viewer for the Compare anchor side (it would conflict with the sticky pane's page state). The Compare modal creates fresh viewers for both sides.

**Warning signs:** Folio nav on one Compare pane changes the page displayed in the other pane or in the sticky anchor pane.

### Pitfall 4: vs_score=None treated as "no VS data" but `merge_candidates` requires Candidate objects

**What goes wrong:** The VS adapter maps `get_suggestions` results (`{alma_id, svm_score, rank}`) to `Candidate` objects. If `svm_score` is absent or the adapter maps it incorrectly, `vs_score=None` on a VS candidate will look like "no data" instead of a real match — `merge_candidates` uses `v.vs_score` to annotate text candidates (verified in `joins_lab.py:577-581`).

**Why it happens:** `svm_score` → `vs_score` field mapping is easy to swap with `rank` → `vs_rank`. The `Candidate` docstring warns: `None == "no VS data" (NOT 0.0 dissimilar)`.

**How to avoid:** VS adapter: `Candidate(vs_rank=r['rank'], vs_score=r['svm_score'], ...)` (not swapped). Add an explicit unit test verifying the field mapping.

**Warning signs:** VS toggle shows N candidates but `via_vs=True` annotations are missing from text candidates that share the same `sys_id`; tier ordering wrong.

### Pitfall 5: Triage state cleared on filter change

**What goes wrong:** Rebuilding the rendered candidate grid/table on filter change also clears NiceGUI elements that close over `_triage`. If triage state is embedded in element callbacks, it gets lost.

**Why it happens:** NiceGUI replaces child elements in a container when the container is cleared and re-rendered. Callbacks that close over element-local state (not the page-level `_triage` dict) will not persist.

**How to avoid:** `_triage` must be a page-level dict closed over by ALL callbacks, NOT a per-card/per-row local variable. When re-rendering a page of cards, pass the existing `_triage` dict as context and read it to set initial button state.

**Warning signs:** User sets Y on page 1, navigates to page 2, returns to page 1 — Y is gone.

### Pitfall 6: Compare modal opens with wrong candidate index when filtering is active

**What goes wrong:** "Open Compare" is triggered from a grid card at visual position N; the flip-through `step(delta)` advances through `filtered_candidates`, not `all_candidates`. If the Compare is opened from a filtered view but uses an index into the unfiltered list, prev/next steps to the wrong candidates.

**Why it happens:** The grid renders `filtered_candidates[page_start:page_end]`; the card carries the candidate's `sys_id`, not an index. `step(delta)` must index `filtered_candidates`.

**How to avoid:** On "Compare" button click, find the candidate's index in `filtered_candidates` by `sys_id`, store that as `_compare_state['idx']`. The `_compare_state` dict also holds `filtered_candidates` (or a reference to it) so step works.

**Warning signs:** Next/Prev in Compare skips filtered-out candidates.

### Pitfall 7: Enrichment batch timing — filters applied before enrichment completes

**What goes wrong:** The enrichment batch (`get_measurement_summaries_batch`) is async. If the user clicks "Filters → Apply → size-mismatch" before the enrichment response arrives, the filter evaluates against an empty enrichment dict and passes all candidates (no data → no mismatch detected).

**Why it happens:** The enrichment is dispatched off-loop after search completes; there is a window where `_enrichment` dict is empty.

**How to avoid:** The filter dialog's size-mismatch and material options should be disabled (or show "Loading...") until enrichment completes. Once enrichment is done, update a `_enrichment_ready` flag and re-enable the relevant filter controls. Material `ui.select` options should be populated from enrichment results, so they are naturally empty until enrichment fires.

---

## Code Examples

### VS Adapter (D-05 pattern, verified from `visual_similarity_dialog.py:176`)

```python
# Source: visual_similarity_dialog.py:176 (verified — run.io_bound pattern)
async def _fetch_vs_candidates(anchor_sid: str, vs_svc) -> list:
    """Map VS suggestions to Candidate objects for merge_candidates input."""
    import dataclasses
    from shared.joins_lab import Candidate

    def run_vs_core():
        return vs_svc.get_suggestions(anchor_sid, 200)

    raw = await run.io_bound(run_vs_core)
    # raw: list of {'alma_id': str, 'svm_score': float, 'rank': int}
    candidates = []
    for r in raw:
        c = Candidate(
            sys_id=r['alma_id'],
            page=None,                   # VS-only: no specific page
            uid=f"{r['alma_id']}|vs",
            via_vs=True,
            vs_rank=r['rank'],
            vs_score=r['svm_score'],     # NOT swapped with rank (Pitfall 4)
        )
        candidates.append(c)
    return candidates
```

### VS Conditional Merge (D-04, verified from `merge_candidates` in `shared/joins_lab.py:547`)

```python
# Source: shared/joins_lab.py:547 (verified signature)
# D-04 conditional model — mirrors desktop join_workbench.py:2788-2802
if vs_toggle_on:
    if builder_has_query:
        # INTERSECTION: keep only tier0 (both via_text AND via_vs)
        merged = merge_candidates(text_candidates, vs_candidates)
        display_candidates = [c for c in merged if c.via_text and c.via_vs]
    else:
        # UNION / pure VS browse: merge_candidates([], vs_candidates)
        display_candidates = merge_candidates([], vs_candidates)
else:
    # OFF: text-only; via_vs badges still present on shared-sys_id candidates
    display_candidates = merge_candidates(text_candidates, vs_candidates)
    # → tier0 first (text+vs), then tier1 (text-only), tier2 excluded when OFF
    #    because vs_candidates is NOT passed as empty —
    #    the vs_candidates from a prior fetch still annotate via_vs=True on
    #    text hits, giving them 👁 badges even with toggle OFF.
    # For a clean OFF: pass merge_candidates(text_candidates, []) so tier2 is absent
    # BUT D-04 says "text-only but look-alikes among text hits still carry the 👁 badge"
    # → use merge_candidates(text_candidates, vs_candidates) and filter OUT tier2:
    display_candidates = [c for c in merge_candidates(text_candidates, vs_candidates)
                          if c.via_text]  # exclude VS-only (tier2) when toggle is OFF
```

### Size Mismatch Formula (D-15, verified from CONTEXT.md with desktop parity at `:1687-1695`)

```python
# Source: CONTEXT.md D-15 (parity desktop join_workbench.py:1687-1695)
def is_size_mismatch(candidate_width_cm: float | None,
                     anchor_width_cm: float | None,
                     threshold: float = 1.4) -> bool:
    """Return True if the candidate's width is more than threshold× different from anchor."""
    if candidate_width_cm is None or anchor_width_cm is None:
        return False   # no data → not flagged
    if min(candidate_width_cm, anchor_width_cm) == 0:
        return False   # guard division by zero
    ratio = max(candidate_width_cm, anchor_width_cm) / min(candidate_width_cm, anchor_width_cm)
    return ratio > threshold
```

### badge_and_tooltip (Wave 0 gap — to be added to shared/joins_lab.py)

```python
# To add to shared/joins_lab.py (desktop parity join_workbench.py:452-457)
def badge_and_tooltip(cand: "Candidate") -> tuple:
    """Return (icon_name | None, tooltip_text) using fixed precedence.

    Precedence: ⚓ is_anchor_self > ⇄ via_other_side > 👁 via_vs.
    Returns (None, '') when no badge applies.
    Pure function — no I/O.
    """
    if cand.is_anchor_self:
        return ("anchor", "Anchor fragment")
    if cand.via_other_side:
        return ("swap_horiz", "Found via other side")
    if cand.via_vs:
        return ("visibility", "Visually similar")
    return (None, "")
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `_MAX_RENDERED_CANDIDATES = 200` silent hard-cap (candidate_grid.py:45) | Pagination: 24/page, render only current page (D-08) | Phase 119 | No candidates hidden beyond a page; cap replaced by navigation |
| Phase-117 read-only grid (no triage, no table, no VS) | Working triage surface: grid/table + Y/?/N + bulk triage + filters + VS toggle | Phase 119 | Candidate set becomes actionable workspace |
| VS dialog (standalone, separate flow) | VS integrated as 👁 toggle on the candidate surface | Phase 119 | VS and text results in the same ranked list with consistent badges |

**Not deprecated/outdated in Phase 119:**
- `cap_candidates()` in `candidate_grid.py` — kept but the rendering path is replaced by paginated rendering. `cap_candidates` can still be used as a fallback safety net but the `_MAX_RENDERED_CANDIDATES` constant's role as the PRIMARY bound ends.
- `visual_similarity_dialog.py` — the standalone VS dialog remains for the `/browse` and search result cards. Phase 119 adds a VS-service ADAPTER in `joins_lab.py`; it does NOT remove or modify the existing dialog.

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `badge_and_tooltip` does not exist in `shared/joins_lab.py` (grep returned no matches) | Standard Stack / Wave 0 gap | LOW — if it exists under a different name, Wave 0 task finds it and adjusts instead of creating it |
| A2 | AnchorViewer's `_VIEWER_HEAD` idempotency guard (`window._msViewerLoaded`) makes two instances on one page safe | Architecture Patterns | LOW — code comment in `anchor_viewer.py` states this explicitly ("Phase 119 Compare readiness") |
| A3 | NiceGUI `ui.table` supports `selection='multiple'` and sortable columns via `props` | Standard Stack | LOW — NiceGUI wraps Quasar QTable which supports these natively |

**All other claims are VERIFIED from codebase inspection or CITED from CONTEXT.md locked decisions.**

---

## Open Questions

1. **`badge_and_tooltip` icon names for grid/table rendering**
   - What we know: desktop uses Qt icons (not Material Icons); CONTEXT.md says `⚓`, `⇄`, `👁` glyphs at `:3144`.
   - What's unclear: The exact Material Icon names the web should use. UI-SPEC says `icon='visibility'` for VS — that is the 👁 icon. For `⚓` (anchor self) and `⇄` (via_other_side) in the web, sensible choices are `anchor` and `swap_horiz`.
   - Recommendation: Use `anchor` for `is_anchor_self`, `swap_horiz` for `via_other_side`, `visibility` for `via_vs` — all standard Material Icons.

2. **Enrichment timing for filter dialog initial render**
   - What we know: Enrichment batch runs off-loop after search; there is a window where `_enrichment` dict is empty (Pitfall 7).
   - What's unclear: Whether the filter dialog should block on enrichment or be openable before it completes.
   - Recommendation: The filter dialog is non-blocking. Material `ui.select` options are dynamic (populated from enrichment); size-mismatch and material filters are visually disabled with a spinner until `_enrichment_ready` is set. This is the desktop pattern (`_EnrichWorker` fills data asynchronously; filter dialog shows empty/loading state).

3. **Compare modal vs. sticky anchor pane AnchorViewer interaction**
   - What we know: Phase 117 created ONE AnchorViewer for the sticky pane. Phase 119 Compare needs two more (one for each Compare pane). Three total AnchorViewer instances on one page.
   - What's unclear: Whether all three share the same `manuscriptViewer` JS instance or need separate `gammaFilterId` / `imageSelector` scoping.
   - Recommendation: The idempotency guard ensures one `manuscriptViewer` JS object is created. The JS `imageSelector: '.zoomable-image'` is a CLASS selector (multiple elements). Each image gets its own pan/zoom state via the existing DOM-element-keyed maps in `manuscript_viewer.js`. No additional scoping should be needed, but the planner should verify by reading `manuscript_viewer.js` briefly.

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|-------------|-----------|---------|----------|
| `shared/visual_similarity_service.py` | VS toggle (D-05) | ✓ (verified in codebase) | current | If `visual_similarity.db` absent: `is_available()=False`; VS toggle disabled (D-06 covers this) |
| `shared/fjms_service.py` `get_measurement_summaries_batch` | Enrichment (D-16) | ✓ (verified at line 3005) | current | If `fjms_enrichment.db` absent: returns `{}`; material/dims show `—` in table |
| NiceGUI `ui.table` with multi-select | CND-03 | ✓ (project-wide) | existing | — |
| `web/components/anchor_viewer.py` `AnchorViewer` | Compare (CMP-01/02) | ✓ (verified) | Phase 117 | — |
| `shared/joins_lab.badge_and_tooltip` | 👁 badge (VSM-02) | **✗ (not yet in shared/joins_lab.py)** | — | Must be added in Wave 0 |

**Missing dependencies with no fallback:**
- `shared/joins_lab.badge_and_tooltip()` — must be created in Wave 0 before any component task.

**Missing dependencies with fallback:**
- `visual_similarity.db` at runtime: `is_available()=False` → VS toggle disabled with tooltip "No visual similarity data for this fragment" (D-06).

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (project-wide) |
| Config file | none — bare `pytest tests/` |
| Quick run command | `pytest tests/test_candidate_grid.py tests/test_joins_lab.py tests/test_visual_similarity.py tests/test_joins_lab_off_loop.py -x` |
| Full suite command | `pytest tests/ -x --ignore=tests/gui_tests` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CND-03 | Table view renders 8-column shape; sortable; multi-select | unit (headless) | `pytest tests/test_candidate_surface.py -x` | ❌ Wave 0 |
| CND-04 | Triage dict updated correctly; resets on re-anchor; consistent across surfaces | unit (headless) | `pytest tests/test_candidate_triage.py -x` | ❌ Wave 0 |
| CND-05 | `dedup_candidates(include_self=False)` excludes anchor; `detect_self_match` returns True | unit | `pytest tests/test_joins_lab.py -k dedup -x` | ✅ (existing coverage) |
| CND-06 | Filter predicates: material / has-dims / size-mismatch / triage; apply_filters smoke | unit (headless) | `pytest tests/test_candidate_filters.py -x` | ❌ Wave 0 |
| CND-07 | Pagination: 24/page; filter before paginate; triage survives page change | unit (headless) | `pytest tests/test_candidate_pagination.py -x` | ❌ Wave 0 |
| CND-08 | Enrichment batch covers full filtered set; spawns off-loop; degrades gracefully | unit (headless) | `pytest tests/test_candidate_enrichment.py -x` | ❌ Wave 0 |
| CMP-01 | Compare modal opens with anchor + candidate sys_ids | unit (headless smoke) | `pytest tests/test_compare_modal.py -x` | ❌ Wave 0 |
| CMP-02 | Per-pane folio nav is independent | unit (headless) | `pytest tests/test_compare_modal.py -k folio -x` | ❌ Wave 0 |
| CMP-03 | Verdict updates triage dict; auto-advance increments compare index | unit (headless) | `pytest tests/test_compare_modal.py -k verdict -x` | ❌ Wave 0 |
| VSM-01 | VS adapter maps `{alma_id,svm_score,rank}` → Candidate correctly; conditional model (intersection vs union) | unit | `pytest tests/test_vs_adapter.py -x` | ❌ Wave 0 |
| VSM-02 | `badge_and_tooltip` precedence: is_anchor_self > via_other_side > via_vs | unit | `pytest tests/test_joins_lab.py -k badge -x` | ❌ Wave 0 |
| INVARIANT | VS lookup + enrichment batch are off-loop in joins_lab.py | AST static guard | `pytest tests/test_joins_lab_off_loop.py -x` | ✅ (extend to cover new call sites) |
| INVARIANT | Zero raw app.storage.user | AST static guard | `pytest tests/test_no_raw_storage_access.py -x` | ✅ (must stay green) |

### Sampling Rate

- **Per task commit:** `pytest tests/test_candidate_grid.py tests/test_joins_lab.py tests/test_joins_lab_off_loop.py tests/test_no_raw_storage_access.py -x`
- **Per wave merge:** `pytest tests/ -x --ignore=tests/gui_tests`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps

- [ ] `tests/test_candidate_surface.py` — covers CND-03 (table shape, sortable, multi-select)
- [ ] `tests/test_candidate_triage.py` — covers CND-04 (dict ops, reset on re-anchor, bulk triage)
- [ ] `tests/test_candidate_filters.py` — covers CND-06 (filter predicates, size-mismatch formula)
- [ ] `tests/test_candidate_pagination.py` — covers CND-07 (slice math, filter-before-paginate)
- [ ] `tests/test_candidate_enrichment.py` — covers CND-08 (batch covers full set, off-loop guard)
- [ ] `tests/test_compare_modal.py` — covers CMP-01/02/03 (open, folio independence, verdict+advance)
- [ ] `tests/test_vs_adapter.py` — covers VSM-01/02 (field mapping, conditional model, badge precedence)
- [ ] `badge_and_tooltip` added to `shared/joins_lab.py` + covered in `tests/test_joins_lab.py -k badge`
- [ ] Extend `tests/test_joins_lab_off_loop.py` to also guard VS lookup + enrichment batch call sites

---

## Security Domain

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | No auth changes in Phase 119 |
| V3 Session Management | no | 119 triage is in-memory; no session writes (Phase 120) |
| V4 Access Control | partial | `tests/test_no_raw_storage_access.py` enforces zero raw `app.storage.user`; triage dict is page-level Python, not storage |
| V5 Input Validation | yes | `sys_id` values used in URL construction (proxy URLs, Compare open); must pass through `build_thumbnail_url` / `build_browse_url` — these use `json.dumps` / f-strings, not user input direct-inject into HTML. `badge_and_tooltip` pure function has no user input. |
| V6 Cryptography | no | No new cryptographic operations |

### Known Threat Patterns for NiceGUI/Quasar

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| `sys_id` in proxy URL path | Tampering | `build_thumbnail_url` / `build_browse_url` already guard this; same functions used in Phase 119 |
| HTML snippet injection via `htmlify` | Tampering | `shared/joins_lab.htmlify` escapes with `html.escape` before injecting markup; reuse it |
| `stop_propagation` server-side call | Tampering (crashing) | Existing AST guard (`tests/test_no_server_side_stop_propagation.py`); Phase 119 must use `js_handler='(e) => e.stopPropagation()'` for any nested-link sites in Compare modal (see 2026-06-12 hotfix pattern) |

---

## Sources

### Primary (HIGH confidence)
- `shared/joins_lab.py` (VERIFIED) — `dedup_candidates` at line 505, `merge_candidates` at line 547, `detect_self_match` at line 601, `Candidate` dataclass at line 76, `normalize_candidate` at line 257, `htmlify` at line 657, `snippet_plain` at line 708. `badge_and_tooltip` NOT present (confirmed by grep).
- `shared/visual_similarity_service.py` (VERIFIED) — `get_vs_service()` singleton at line 312; `get_suggestions(sys_id, limit=200)` at line 97; returns `list[{'alma_id': str, 'svm_score': float, 'rank': int}]`.
- `shared/fjms_service.py` (VERIFIED) — `get_measurement_summaries_batch(sys_ids: list[str]) -> dict[str, dict]` at line 3005; returns `{AlmaId: {'width_cm', 'height_cm', 'material', ...}}`.
- `web/components/candidate_grid.py` (VERIFIED) — `create_candidate_grid`, `build_thumbnail_url`, `build_browse_url`, `cap_candidates`, `_MAX_RENDERED_CANDIDATES=200` at line 45.
- `web/pages/joins_lab.py` (VERIFIED) — `execute_joins_search` async function, `run_search_core` off-loop pattern, `_search_generation` + `_should_apply_results` + `_make_progress_cb` generation-counter pattern.
- `web/components/anchor_viewer.py` (VERIFIED) — `inject_viewer_assets()`, `_VIEWER_HEAD` idempotency guard comment, Phase 119 Compare readiness note.
- `web/components/visual_similarity_dialog.py` (VERIFIED) — `run.io_bound(vs_service.get_suggestions, sys_id, 200)` at line 176; off-loop pattern for VS.
- `tests/test_joins_lab_off_loop.py` (VERIFIED) — AST guard scope: only `joins_lab.py`; checks `execute_search` calls in sync closure → `run.io_bound`; does NOT yet check VS or enrichment calls.
- `.planning/phases/119-candidates-compare-visual-similarity/119-CONTEXT.md` (CITED) — 16 locked decisions D-01..D-16.
- `.planning/phases/119-candidates-compare-visual-similarity/119-UI-SPEC.md` (CITED) — UI design contract: spacing/typography/color/components, all locked.

### Secondary (MEDIUM confidence)
- `.planning/REQUIREMENTS.md` (CITED) — CND-03..08, CMP-01..03, VSM-01/02 requirement definitions.
- `.planning/STATE.md` (CITED) — Phase 117/118 completed; decisions history.

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — all verified from codebase inspection
- Architecture: HIGH — verified from existing code patterns + locked CONTEXT.md decisions
- Pitfalls: HIGH — verified from existing code + CONTEXT.md canonical refs

**Research date:** 2026-06-19
**Valid until:** 2026-07-19 (30 days; stable NiceGUI/project stack)
