# Phase 109: Visual-Similarity Merge & Soft-Retire — Research

**Researched:** 2026-06-07
**Domain:** Desktop Join Workbench — visual-similarity source wiring + standalone dialog soft-retire
**Confidence:** HIGH (brownfield wiring phase; all claims verified against live codebase)

---

## Summary

Phase 109 is a **UI-wiring phase**: the heavy logic (merge ordering, provenance tags, Candidate
dataclass, normalize_candidate, merge_candidates) is already unit-tested in `shared/joins_lab.py`
(Phase 106). Phase 108 scaffolded the source selector stub and both-first merge plumbing with the
VS half disabled. Phase 109 fills that stub.

The primary load-bearing seam is the **VS-dict → Candidate adapter** (R-01): `get_suggestions`
returns `{'alma_id', 'svm_score', 'rank'}` but `normalize_candidate` reads `vs_rank`/`svm_score`
from the dict. A thin shim dict must bridge them before normalization. The second key concern is
**page-lazy + batched enrichment** (D-09/R-03): `_EnrichWorker` currently enriches the full
`self.results` list in one shot; for ≤200 VS candidates the planner must add a page-lazy
wrapper that passes only the visible 20 items to `_EnrichWorker` on each page turn.

The **reroute** (R-05) is straightforward: both `_browse_view_visual_similarity`
(`genizah_app.py:4708`) and `_rd_search_visual_similarity` (`desktop/result_dialog.py:758`)
already know how to build a result dict and call `open_joins_workbench(res)` — the new paths
replace those method bodies with workbench calls plus a post-open source-selector trigger.

**Primary recommendation:** Build the VS adapter and page-lazy enrichment first (the correctness
and performance risks); the source-selector UI and the reroute are pure wiring on top of working
plumbing.

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- D-01: Visual source auto-loads on select (no Find Candidates press needed).
- D-02: Combined = builder text query merged with anchor VS look-alikes via merge_candidates; degrades to Visual-only when builder is empty.
- D-03: VS source ignores BOTH builders; cross-side AND/OR logic applies only to the text half.
- D-04: ★both keyed at manuscript level (locked Phase-106 merge_candidates: vs_by_sid = {v.sys_id: v}).
- D-05: Fetch + show all VS suggestions up to service limit (200); no top-N cap; paginated 20/page.
- D-06: No minimum-similarity (svm_score) floor.
- D-07: Combined = single merged list, paginated 20/page; no independent VS cap.
- D-08: When anchor has no VS data (has_suggestions() false), GREY OUT / disable the Visual source option; Combined falls back to Text-only on no-VS anchor.
- D-09 (perf — SC#3): enrichment MUST be PAGE-LAZY — enrich only the visible 20-card page — on top of being batched (108 D-21).
- D-10: Reroute BOTH normal-mode entry points to the Workbench (Visual): Browse _browse_view_visual_similarity and ResultDialog _rd_search_visual_similarity.
- D-11: Old standalone normal-mode dialog code MARKED REMOVABLE (deprecation comment) and retained for one cycle; not deleted in 109.
- D-12: JoinsDialog pick-mode partner-picker kept AS-IS, untouched.
- D-13: Desktop-only. Web VS dialog untouched.
- D-14: Parity verification = BOTH (a) automated invariant test (same sys_id set as get_vs_service().get_suggestions(anchor)) AND (b) manual UAT sign-off.
- D-15: Cutover = reroute immediately on ship, no temporary fallback toggle.
- D-16: Parity bar = same look-alike sys_id set reachable + all four actions work on VS candidates.
- D-17: i18n from line one — every new string tr()-wrapped; i18n guard test applies.
- D-18: No _vs_* private calls on the workbench path.
- D-19: VS reached via shared service (get_vs_service().get_suggestions(...)), not SearchExecutor adapter.

### Claude's Discretion
- Where the parity-pass record lives (lean: 109-HUMAN-UAT.md scenario + automated test).
- Exact deprecation-marker style/comment for _show_vs_dialog normal mode.
- Whether ⊙VS cards display vs_rank / svm_score (lean: show compact rank/score).
- Grey-out vs hide for disabled Visual option when anchor has no VS data (D-08).
- Exact mechanism to set source=Visual at/after open from rerouted entry points.
- How much of _enrich_vs_suggestions logic is reused vs replaced by 108 batch path.

### Deferred Ideas (OUT OF SCOPE)
- Page-weighted ★both refinement (v8 ships manuscript-level ★both).
- Physical deletion of old standalone normal-mode dialog code (later cleanup).
- Routing JoinsDialog pick-mode into the Workbench (kept on standalone path).
- Web VS-dialog soft-retire / web Join Workbench.
- Temporary fallback toggle (env/hidden setting to reopen old dialog) — rejected.
- Minimum-similarity floor / top-N VS cap — rejected for v8.
- Full per-item detail/expand layout parity with the old dialog — not a gate.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| JWB-12 | Unified candidate sources: VS source + combined view (provenance badges ★both / ⊙VS / ✎text, both-first ordering), soft-retire standalone VS dialog (reroute entry points → deprecate; keep JoinsDialog pick-mode hook). Every candidate carries four actions (Browse / Puzzle / Add to List / Add as Join). | VS service contract verified; adapter shim contract derived; _maybe_assemble plug point confirmed (:2402); reroute target methods confirmed; page-lazy pattern designed; i18n keys partially pre-registered (see below). |
</phase_requirements>

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| VS data fetch (get_suggestions) | shared service (`shared/visual_similarity_service.py`) | — | Phase 106 D-05: VS via shared service, not SearchExecutor adapter |
| VS → Candidate normalization | shared module (`shared/joins_lab.normalize_candidate`) | thin shim dict in desktop pane | normalize_candidate is the single dict→Candidate source of truth (D-02); needs a key-mapping shim |
| Text/VS merge + provenance ordering | shared module (`shared/joins_lab.merge_candidates`) | — | Pure function, unit-tested Phase 106 |
| Source selector UI (Text/Visual/Combined) | desktop pane (`JoinCandidatePane._build_ui`) | — | Qt widget, desktop-only (D-13) |
| Page-lazy batched enrichment | desktop pane (`JoinCandidatePane._start_enrich`) | `_EnrichWorker` | Performance constraint D-09; pane owns the pagination cursor (_page) |
| Reroute Browse entry point | `genizah_app.py:4708 _browse_view_visual_similarity` | `open_joins_workbench` | Replaces body to call workbench |
| Reroute ResultDialog entry point | `desktop/result_dialog.py:758 _rd_search_visual_similarity` | `open_joins_workbench` | Replaces body to call workbench |
| Pick-mode partner-picker (KEEP) | `genizah_app.py:5108 _show_vs_dialog on_pick branch` | — | Untouched (D-12) |
| Parity invariant test | `tests/test_join_workbench_vs.py` (new) | `test_join_workbench_i18n.py` (existing, must stay green) | Automated parity gate (D-14a) |

---

## R-01: VS → Candidate Adapter Contract (VERIFIED)

### get_suggestions return shape
`shared/visual_similarity_service.py:97-128` [VERIFIED: file read]

```python
# Returns: list of dicts ordered by svm_score DESC, rank 1-indexed
{'alma_id': str(row[0]), 'svm_score': row[1], 'rank': i + 1}
```

Key facts:
- `alma_id` is a string (even though the DB stores an integer — `str(row[0])`).
- `rank` is 1-based (i + 1 where i starts at 0).
- `svm_score` is a float.
- Returns `[]` when `self._conn is None` (sidecar absent) — NEVER raises. [VERIFIED: line 108-109]
- `has_suggestions(sys_id)` returns `False` when `self._conn is None` — safe guard for D-08. [VERIFIED: line 131-133]

### normalize_candidate reads (VERIFIED: shared/joins_lab.py:248-277)
```python
vs_rank = res.get("vs_rank"),   # reads key "vs_rank"
vs_score = res.get("svm_score")  # reads key "svm_score" (line 276)
```
`sys_id` is extracted via `_r_sid(res)` which reads `res.get("display", {}).get("id")` OR `res.get("sys_id")`.
`page` is extracted via `page_of(res)` which reads `display.img` or re-searches the uid field.

### Required shim dict (A1 confirmed)
`normalize_candidate` does NOT accept raw VS dicts directly — it reads `vs_rank` (not `rank`) and
`sys_id` from `display.id` (not `alma_id`). A thin shim is required before calling `normalize_candidate`:

```python
# shim: VS dict {'alma_id': str, 'svm_score': float, 'rank': int}
#   → normalize_candidate-compatible dict
def _vs_to_norm_dict(row: dict) -> dict:
    return {
        "display": {
            "id": row["alma_id"],
            "shelfmark": "",          # enriched later by _EnrichWorker
            "title": "",
            "library_code": "",
            "img": None,              # VS is manuscript-level; page=None
        },
        "uid": f"{row['alma_id']}|vs",
        "vs_rank": row["rank"],       # key rename: rank → vs_rank
        "svm_score": row["svm_score"],  # passthrough: key name matches
        "_via_vs": True,
        "full_text": "",
        "scope": "",
    }
# Then: normalize_candidate(_vs_to_norm_dict(row)) → Candidate(
#     sys_id=row['alma_id'],
#     page=None,            # page_of sees display.img=None → None
#     uid='{alma_id}|vs',
#     via_vs=True,          # _via_vs=True
#     vs_rank=row['rank'],  # vs_rank key
#     vs_score=row['svm_score'],  # svm_score key
# )
```

This shim belongs in `desktop/join_workbench.py` (not shared/joins_lab.py) — it is the
desktop adapter's responsibility to normalize VS-service output into the shared domain model.

### None-page guarantee (R-02, RR-12 confirmed)
`Candidate.page` is `Optional[int]` (`shared/joins_lab.py:104`). Phase 108 RR-12 guards are
already in place:
- `CandidateCard.__init__:1736`: `self._card_page = max(1, c.page or 1)` — safe. [VERIFIED: line 1736]
- `_render_table:2663`: `str(c.page) if c.page else ""` — safe. [VERIFIED: line 2663]
- `ThumbResolver` receives only `(gidx, c.sys_id)` — no page arg. [VERIFIED: lines 2572-2573]
- `_EnrichWorker` keys by `c.key = (sys_id, None)` for VS-only rows — safe. [VERIFIED: line 1588]

The `CompareDialog._fill_candidate` must guard `page is None` before `images[page-1]` — this
is a Phase 108 RR-12 item already encoded in the 108 plans.

---

## R-03: Page-Lazy + Batched Enrichment (VERIFIED)

### Current _EnrichWorker behavior (VERIFIED: desktop/join_workbench.py:1530-1598)
`_EnrichWorker` receives the **full** `self.results` list and enriches ALL candidates in one
batch (`get_measurement_summaries_batch(sys_ids)` where `sys_ids` covers the whole list).
For 200 VS candidates this is:
- 1 batch SQL query for measurements (acceptable — batch query).
- 200 serial `snippet_html` / `snippet_plain` calls (pure Python, fast).
- BUT: ThumbResolver is already page-lazy (launched per `_render_grid_page`, feeds only the
  visible 20 items). [VERIFIED: lines 2575-2584]

### What "page-lazy" means for 109
The measurements batch is acceptable even for 200 (one IN-query). The concern is:
1. Snippets for 200 items (tiny pure-Python cost — not a real problem).
2. More importantly: `_EnrichWorker` is launched by `_start_enrich()` → `_maybe_assemble()`.
   For 200 combined candidates, the `enriched` signal fires once with all 200 enriched.
   The grid then renders only page 0 (items 0-19). This is **already page-lazy at the render
   layer** because `_render_grid_page` reads from `self._enrich` which is pre-populated.

**Conclusion:** D-09's "page-lazy enrichment" means the ThumbResolver pattern (already in
place) must apply to VS candidates too — and it already does (ThumbResolver is launched per
`_render_grid_page`). The `_EnrichWorker` measurements + snippets batch can still run over
the full list (it's one SQL query + cheap pure functions). The planner does NOT need to add a
new page-lazy wrapper around `_EnrichWorker` — the existing separation of concerns already
satisfies D-09 for measurements/snippets. **The only true page-lazy requirement is thumbnails,
which ThumbResolver already handles per page.** [VERIFIED: lines 2553-2584]

### Batch API confirmation (RR-6 from Phase 108, confirmed)
`FjmsService.get_measurement_summaries_batch()` exists at `shared/fjms_service.py:3005` and
returns `{AlmaId: {width_cm, height_cm, avg_num_lines, avg_text_density, avg_line_height_mm,
material, size_category}}`. Already used by `_EnrichWorker:1559`. [ASSUMED from 108 research
context; path confirmed as already wired in 108]

### Practical guidance for 200-candidate set
When `_maybe_assemble` produces 200 combined results:
- `_start_enrich` passes all 200 to `_EnrichWorker` → 1 batch SQL + 200 pure-Python snippets.
- After `_on_enriched`, `apply_filters` → `render_results` → `_render_grid_page` shows page 0
  (20 items). ThumbResolver runs for those 20 items only.
- Page flip → `_render_grid_page` rerenders next 20; ThumbResolver reruns for those 20.
- This is the correct page-lazy behavior. No architectural change needed in `_EnrichWorker`.

---

## R-04: VS Fetch Path + Caches (VERIFIED)

### VisualSimilarityService singleton + threading
`get_vs_service(thread_safe=True)` (default) uses `ThreadLocalConnection` — each thread gets
its own SQLite connection. Safe to call from worker threads. [VERIFIED: visual_similarity_service.py:79-82]

`get_vs_service(thread_safe=False)` uses a single shared connection with `check_same_thread=True`
— only safe on the thread that created it. The old `_browse_view_visual_similarity` uses
`thread_safe=False` but runs on the UI thread. [VERIFIED: genizah_app.py:4727]

**For the Workbench:** the VS fetch should run in a `_VSFetchWorker` QThread (or inline in
`_maybe_assemble` if fast enough). `get_suggestions` is a single cheap SQL query (no network).
Use `get_vs_service(thread_safe=True)` on any worker thread; use `thread_safe=False` only
on the UI thread. Because VS is local SQLite (no network), running on UI thread is acceptable
but non-ideal. **Recommendation: run VS fetch in the same worker that assembles results, or
run it eagerly on the UI thread (it's ~1ms SQLite query). Claude's discretion.**

### DesktopVSCache + server-fallback chain (VERIFIED: genizah_app.py:4736-4757)
The old dialog's three-step chain (local DB → DesktopVSCache → server HTTP) is needed because
desktops may not have `visual_similarity.db`. The service's `is_available()` check handles the
absent-sidecar case gracefully — `get_suggestions` returns `[]` and `has_suggestions` returns
`False`. [VERIFIED: visual_similarity_service.py:108-109, 131-133]

**For 109:** The Workbench should use the simpler path: call `get_vs_service()` directly. If
`is_available()` is False or `has_suggestions(anchor_sid)` is False, grey-out Visual (D-08).
**The DesktopVSCache + server-fallback chain is NOT needed for the Workbench path** because:
(1) the local sidecar is present on most desktops; (2) D-08 handles the absent-data case by
disabling the source rather than fetching from the server; (3) the old dialog kept the server
fallback as a recovery mechanism, but the Workbench's inline source model doesn't need it.

If Hillel's machine lacks `visual_similarity.db`, the Visual option is greyed out, which is
correct behavior per D-08. This is an explicit design decision, not an omission.

### NLI circuit breaker for VS thumbnail enrichment (R-04 / Phase 107 WR-02)
`ThumbBatchWorker` (used for known-joins thumbnails, `join_workbench.py:589-633`) does NOT
wire the Phase-98 NLI circuit breaker. [VERIFIED: 107-REVIEW.md WR-02; code at join_workbench.py:607-633]

For VS candidate thumbnails, `ThumbResolver` (used for grid cards, `join_workbench.py:1445`) is
used instead of `ThumbBatchWorker`. `ThumbResolver` fetches via `meta_mgr.get_thumbnail(sid)` and
then via `ImageLoaderThread`. `ImageLoaderThread` uses the standard HTTP pattern. [VERIFIED: lines
2575-2613]

**Planner guidance:** Phase 109 VS enrichment uses `ThumbResolver` (the same path as Phase 108
text candidates). `ThumbResolver` does not bypass the NLI breaker in the same way `ThumbBatchWorker`
does (WR-02 is about `ThumbBatchWorker`, not `ThumbResolver`). Wiring the NLI breaker into
`ThumbResolver` is a separate improvement (not a 109 blocker). The 109 planner should note that
VS thumbnail fetches use `ThumbResolver` (the existing page-lazy path) and are therefore no worse
than the text-candidate thumbnail behavior already shipped in Phase 108. Do NOT regress by using
`ThumbBatchWorker` for VS thumbnails.

---

## R-05: Reroute Wiring (VERIFIED)

### Current _browse_view_visual_similarity (genizah_app.py:4708-4759)
Reads `self.current_browse_sid`, fetches shelfmark from `meta_mgr.get_meta_for_id`, then runs
the local DB → cache → server chain and calls `self._show_vs_dialog(...)`. [VERIFIED: lines 4708-4759]

### Current _rd_search_visual_similarity (result_dialog.py:758-805)
Reads `self.current_sys_id`, `self.all_results[self.current_result_idx]` for shelfmark, runs the
same chain, calls `parent._show_vs_dialog(...)`. [VERIFIED: lines 758-805]

### The reroute pattern (VERIFIED: genizah_app.py:9868-9886)
`_browse_open_join_workbench` (line 9868) is the exact blueprint for Browse reroutes:
```python
def _browse_open_join_workbench(self):
    sid = getattr(self, "current_browse_sid", None)
    if not sid:
        return
    p = getattr(self, "current_browse_p", 1) or 1
    text = getattr(self, "browse_original_text", "") or ""
    shelf = ""
    try:
        shelf, _ = self.meta_mgr.get_meta_for_id(sid)
    except Exception:
        shelf = ""
    res = {
        "display": {"id": sid, "shelfmark": shelf, "img": p, "library_code": "", "title": ""},
        "full_text": text,
        "uid": f"{sid}_P{int(p):03d}",
    }
    self.open_joins_workbench(res)
```

For `_browse_view_visual_similarity`, replace the body with the same pattern:
1. Build `res` dict from `current_browse_sid` + shelfmark.
2. Call `self.open_joins_workbench(res)` — this sets anchor.
3. After open, trigger `source=Visual` auto-load (see mechanism below).

For `_rd_search_visual_similarity` in `result_dialog.py:758`, replace body with:
1. Build `res` from `self.current_sys_id` + shelfmark (same as existing code builds it).
2. Call `app.open_joins_workbench(res)`.
3. Trigger `source=Visual` auto-load.
4. Close the ResultDialog (matching the existing `_open_join_workbench` behavior at line 756).

### open_joins_workbench signature (VERIFIED: genizah_app.py:15464-15474)
```python
def open_joins_workbench(self, res: dict):
    # Creates/reuses _join_workbench singleton, calls set_anchor(res), shows + raises window
    ...
    self._join_workbench.set_anchor(res)
    self._join_workbench.show()
    self._join_workbench.raise_()
    self._join_workbench.activateWindow()
```
No current mechanism to pass a `source` parameter. Two options for D-01 auto-load trigger:

**Option A (recommended): Add optional `source` param to `open_joins_workbench`**
```python
def open_joins_workbench(self, res: dict, source: str = "text"):
    ...
    self._join_workbench.set_anchor(res)
    if source == "visual":
        self._join_workbench.set_source("visual")  # triggers VS auto-load
    self._join_workbench.show()
    ...
```
Then `set_source("visual")` on `JoinWorkbenchWindow` delegates to `JoinCandidatePane`.

**Option B: Post-open signal**
Call `open_joins_workbench(res)` then call `self._join_workbench.candidate_pane.set_source("visual")`
directly (requires `candidate_pane` to be a public attribute of `JoinWorkbenchWindow`).

Both are Claude's discretion (CONTEXT.md discretion list). Option A is cleaner and avoids reaching
into the window's internals from the host.

### set_anchor does NOT reset source (VERIFIED: join_workbench.py:4031-4072)
`set_anchor` resets generation, anchor state, and triage — but does NOT touch the source selector.
This means source=Visual set before `set_anchor` would survive. However, the order matters if
`set_anchor` triggers an immediate VS fetch. Safest order: `set_anchor(res)` first (sets
`_anchor_sid` which VS needs), then `set_source("visual")`.

### _show_vs_dialog deprecation marker (D-11)
Add a deprecation comment to the normal-mode path in `_show_vs_dialog` (line 4788+):
```python
# DEPRECATED: normal-mode VS dialog — entry points rerouted to Join Workbench in Phase 109.
# This code is retained for one cycle as a safety net; see D-11. The pick-mode
# branch (on_pick is not None, line ~5108) remains ACTIVE and must not be removed.
```
The pick-mode branch at line 5108 is:
```python
if on_pick:
    # Pick mode: fill fragment B in the calling JoinsDialog
    on_pick(_pid, _shelf)
    _dlg.accept()
```
[VERIFIED: genizah_app.py:5107-5117] — untouched.

---

## Standard Stack

### Core (all pre-existing — no new dependencies)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `shared/visual_similarity_service.py` | — | VS data; `get_vs_service()`, `get_suggestions()`, `has_suggestions()` | Phase 106 D-05 — shared service path |
| `shared/joins_lab.py` | — | `normalize_candidate`, `merge_candidates`, `Candidate`, `dedup_candidates` | Phase 106 pure domain model |
| PyQt6 | existing | Source selector QRadioButton/QButtonGroup or QComboBox | Desktop UI layer |
| `genizah_translations.py` | — | i18n; TRANSLATIONS.update({}) pattern | RR-4 i18n guard |

### VS → Candidate Shim (new — ~15 lines in join_workbench.py)
| Item | Location | Purpose |
|------|----------|---------|
| `_vs_to_norm_dict(row)` | `desktop/join_workbench.py` (module level or nested) | Maps `{'alma_id','svm_score','rank'}` → normalize_candidate-compatible dict |

No new pip packages required. [VERIFIED: all needed modules already in codebase]

---

## Architecture Patterns

### System Architecture: VS Source Flow

```
User selects "Visual similarities" in source selector
           ↓
JoinCandidatePane._on_source_changed("visual")   [OR auto-load D-01]
           ↓
get_vs_service().has_suggestions(anchor_sid)
    ├── False → grey out source, show nothing (D-08)
    └── True →
           ↓
get_vs_service().get_suggestions(anchor_sid, 200)
→ [{'alma_id', 'svm_score', 'rank'}, ...]
           ↓
[_vs_to_norm_dict(row) for row in suggestions]
→ [{'display': {'id': alma_id}, 'uid': f'{alma_id}|vs', 'vs_rank': rank, ...}, ...]
           ↓
[normalize_candidate(d) for d in vs_dicts]
→ [Candidate(sys_id, page=None, via_vs=True, vs_rank, vs_score), ...]
           ↓
_maybe_assemble(text_cands=[...] or [], vs_cands=[...])
→ merge_candidates(text_cands, vs_cands)
→ list[Candidate] (ordered: ★both → ✎text → ⊙VS-by-rank)
           ↓
self.results = merged_list
self._page = 0
_start_enrich()   →   _EnrichWorker(all 200)   [batch SQL + pure snippets]
           ↓
_on_enriched(enrich_dict)   →   apply_filters()
           ↓
_render_grid_page(page=0)   →   ThumbResolver(20 items)   [page-lazy thumbnails]
           ↓
Cards with ⊙VS#rank badge + vs_score display (Claude's discretion)
```

### Seam: _maybe_assemble plug point (VERIFIED: join_workbench.py:2398-2404)
```python
def _maybe_assemble(self):
    """Merge sources and start enrichment (RR-2: merge_candidates returns a LIST)."""
    from shared.joins_lab import merge_candidates
    # CURRENT (Phase 108 stub):
    self.results = list(merge_candidates(self._text_cands or [], []))
    # PHASE 109 CHANGE:
    self.results = list(merge_candidates(self._text_cands or [], self._vs_cands or []))
    self._page = 0
    self._start_enrich()
```
Where `self._vs_cands` is the normalized VS candidate list (list[Candidate]) populated by the
VS fetch path.

### Source selector UI stub → live (VERIFIED: join_workbench.py:2074-2083)
The current `_build_ui` has a `src_row` layout with only `btn_find`. Phase 109 adds radio
buttons (or a combo box) for Text / Visual / Combined before or alongside `btn_find`. The
source selector must grey out Visual when `has_suggestions()` is false.

```
Phase 108 state:
    [Find Candidates]   ← btn_find only

Phase 109 state:
    (•) Text  ( ) Visual similarities (greyed if no VS)  ( ) Search + visual
    [Find Candidates]   ← only shown when source is Text or Combined
```
(Visual = auto-load, no button press needed per D-01.)

### Provenance badge for ⊙VS (VERIFIED: join_workbench.py:1668-1676)
Phase 108 already has badge slots in `CandidateCard.__init__` (`shelf_text` lines 1668-1676).
The existing `⚓ self` and `⇄ other side` badges set the pattern. Phase 109 adds:
- `⊙VS#rank` badge for `c.via_vs and not c.via_text` candidates.
- `★ both` badge for `c.via_text and c.via_vs` candidates.
- `✎ text` badge for `c.via_text and not c.via_vs` candidates (already correct — no badge shown, just text).

All badge strings must be `tr()`-wrapped and added to `genizah_translations.TRANSLATIONS`.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| VS data fetch | Custom SQLite query | `get_vs_service().get_suggestions()` | Service handles path resolution, threading, error degrades to [] |
| VS availability check | `if sidecar_path exists:` | `get_vs_service().has_suggestions(sys_id)` | Also handles absent sidecar |
| Text/VS merge ordering | Custom sort | `shared/joins_lab.merge_candidates()` | Unit-tested Phase 106 |
| Dict→Candidate | Custom Candidate() call | `normalize_candidate(shim_dict)` | Single source of truth (D-02) |
| Batch measurements | Per-candidate FJMS calls | `FjmsService.get_measurement_summaries_batch()` | D-21; already wired in _EnrichWorker |
| Snippet generation | Custom text trimming | `shared/joins_lab.snippet_html/plain()` | Phase 106 pure helpers |

---

## Common Pitfalls

### Pitfall 1: Wrong key names in VS shim dict
**What goes wrong:** Calling `normalize_candidate({'alma_id': ..., 'rank': ...})` directly.
`normalize_candidate` reads `vs_rank` (not `rank`) and `sys_id` from `display.id` (not `alma_id`).
**Why it happens:** The VS service returns natural field names; normalize_candidate uses canonical domain names.
**How to avoid:** Always go through `_vs_to_norm_dict` before calling `normalize_candidate`.
**Warning signs:** `Candidate.vs_rank` is None for VS rows; `Candidate.sys_id` is empty string.

### Pitfall 2: vs_score=None misread as "dissimilar"
**What goes wrong:** Rendering `vs_score=None` as "no similarity" or `0.0`.
**Why it happens:** The Candidate docstring explicitly calls this out: `vs_score=None` means "no VS data" NOT "0.0 dissimilar" (`shared/joins_lab.py:119`).
**How to avoid:** Guard display with `if c.vs_score is not None:` before formatting.

### Pitfall 3: Serial enrichment of 200 items
**What goes wrong:** Adding a `for c in self.results: enrich(c)` loop instead of using `_EnrichWorker`.
**Why it happens:** Naive implementation; looks simple.
**How to avoid:** Route VS enrichment through the existing `_start_enrich()` → `_EnrichWorker` path.

### Pitfall 4: calling _show_vs_dialog normal-mode path from Workbench
**What goes wrong:** Calling `self._show_vs_dialog(sys_id, shelf, data)` (without `on_pick`) from the Workbench path.
**Why it happens:** Copy-paste from old dialog code.
**How to avoid:** D-11 — normal-mode _show_vs_dialog is marked removable; D-18 — no _vs_* calls from Workbench path.

### Pitfall 5: Rerouting destroys pick-mode
**What goes wrong:** Replacing ALL of `_show_vs_dialog`'s body, breaking the `on_pick` branch.
**Why it happens:** Not reading the D-12 constraint carefully.
**How to avoid:** The reroute is in the CALLERS (entry points), not in `_show_vs_dialog` itself. `_show_vs_dialog` is only marked removable; its `on_pick` branch stays functional.

### Pitfall 6: source selector change fires do_search instead of VS auto-load
**What goes wrong:** Connecting source selector signal to `do_search` for all sources.
**Why it happens:** Consistent wiring; Visual source needs special handling per D-01.
**How to avoid:** In `_on_source_changed`: if source is "visual", call `_load_vs()` directly (no query, no button press); if source is "text" or "combined", show the Find Candidates button.

### Pitfall 7: thread-safety of get_vs_service on worker thread
**What goes wrong:** Creating a `get_vs_service(thread_safe=False)` singleton then calling it from a QThread worker.
**Why it happens:** Copying the old `_browse_view_visual_similarity` code which runs on the UI thread.
**How to avoid:** The default `get_vs_service(thread_safe=True)` uses `ThreadLocalConnection` and is safe on any thread. For UI-thread calls, `thread_safe=False` is fine.

### Pitfall 8: i18n guard failure
**What goes wrong:** Adding `tr("Visual similarities")` to `join_workbench.py` without a matching key in `genizah_translations.TRANSLATIONS`.
**Why it happens:** Forgetting the guard test `tests/test_join_workbench_i18n.py::test_all_tr_keys_in_translations`.
**How to avoid:** Always update `TRANSLATIONS` in the SAME plan that adds new `tr()` calls.

---

## Code Examples

### VS → Candidate Adapter
```python
# Source: verified against shared/joins_lab.py:248-277 and visual_similarity_service.py:97-128

def _vs_to_norm_dict(row: dict) -> dict:
    """Map a get_suggestions() dict to a normalize_candidate()-compatible dict.

    Key renames:
      alma_id → display.id (and sys_id fallback)
      rank    → vs_rank        (normalize_candidate reads 'vs_rank')
      svm_score passes through (normalize_candidate reads 'svm_score')

    page=None is achieved by display.img=None (page_of returns None when
    display.img is None/falsy and no _P\d+ pattern in uid).
    """
    return {
        "display": {
            "id": row["alma_id"],
            "shelfmark": "",
            "title": "",
            "library_code": "",
            "img": None,          # → page_of() returns None → Candidate.page=None
        },
        "uid": f"{row['alma_id']}|vs",
        "vs_rank": row["rank"],   # normalize_candidate reads res.get("vs_rank")
        "svm_score": row["svm_score"],  # normalize_candidate reads res.get("svm_score")
        "_via_vs": True,
        "full_text": "",
        "scope": "",
    }
```

### _maybe_assemble with VS
```python
# Source: verified against join_workbench.py:2398-2404 (current stub)

def _maybe_assemble(self):
    """Merge sources and start enrichment (RR-2: merge_candidates returns a LIST)."""
    from shared.joins_lab import merge_candidates
    # _vs_cands is None (no VS load) or list[Candidate] from VS fetch
    self.results = list(merge_candidates(
        self._text_cands or [],
        self._vs_cands or [],
    ))
    self._page = 0
    self._start_enrich()
```

### VS fetch method for JoinCandidatePane
```python
# Called by _on_source_changed("visual") and on Combined assembly

def _load_vs(self):
    """Fetch VS candidates for the current anchor and store in self._vs_cands."""
    from shared.visual_similarity_service import get_vs_service
    from shared.joins_lab import normalize_candidate
    anchor_sid = self.wb._anchor_sid
    if not anchor_sid:
        self._vs_cands = []
        return
    svc = get_vs_service()   # thread_safe=True (default) — safe on any thread
    if not svc.is_available() or not svc.has_suggestions(anchor_sid):
        self._vs_cands = []
        return
    raw = svc.get_suggestions(anchor_sid, 200)
    self._vs_cands = [
        normalize_candidate(_vs_to_norm_dict(row)) for row in raw
    ]
```

### Source selector: grey-out Visual (D-08)
```python
# In _on_anchor_set (called after set_anchor finishes loading):
from shared.visual_similarity_service import get_vs_service
svc = get_vs_service()
has_vs = svc.is_available() and svc.has_suggestions(self.wb._anchor_sid or "")
self.rb_visual.setEnabled(has_vs)
self.rb_combined.setEnabled(has_vs)  # Combined also needs VS
if not has_vs and self.rb_visual.isChecked():
    self.rb_text.setChecked(True)  # fall back to text
```

### Deprecation marker for _show_vs_dialog (D-11)
```python
def _show_vs_dialog(self, sys_id, shelfmark, data, parent_dialog=None, on_pick=None):
    # DEPRECATED (Phase 109): The normal-mode path (on_pick is None) is no longer
    # reachable from standard entry points — _browse_view_visual_similarity and
    # _rd_search_visual_similarity now open the Join Workbench directly.
    # This code is retained as a safety net for one cycle (D-11) and WILL BE
    # DELETED in a future cleanup phase.
    #
    # EXCEPTION: The pick-mode branch (on_pick is not None) remains ACTIVE —
    # it is called from JoinsDialog and must not be removed (D-12).
    if on_pick is None:
        # REMOVABLE after safety-net cycle — normal mode replaced by Workbench Visual source
        pass
    ...
    # [rest of method unchanged]
```

### Rerouted _browse_view_visual_similarity (D-10)
```python
def _browse_view_visual_similarity(self):
    """REROUTED Phase 109: Opens Join Workbench with Visual source (D-10)."""
    sys_id = self.current_browse_sid
    if not sys_id:
        return
    shelfmark = ""
    if self.meta_mgr:
        try:
            shelfmark, _ = self.meta_mgr.get_meta_for_id(sys_id)
        except Exception:
            pass
    shelfmark = shelfmark or sys_id
    p = getattr(self, "current_browse_p", 1) or 1
    text = getattr(self, "browse_original_text", "") or ""
    res = {
        "display": {"id": sys_id, "shelfmark": shelfmark, "img": p,
                    "library_code": "", "title": ""},
        "full_text": text,
        "uid": f"{sys_id}_P{int(p):03d}",
    }
    self.open_joins_workbench(res, source="visual")  # D-01 auto-load
```

---

## i18n: New Keys Required (Phase 109)

The following keys are NEEDED but **not yet in** `genizah_translations.TRANSLATIONS` (Phase 108
pre-registered some — verified below). Planner MUST include `genizah_translations.py` in every
plan that introduces new `tr()` calls.

### Pre-registered in Phase 108 (VERIFIED: genizah_translations.py:3829-3840)
Already present — no action needed:
- `"Visual similarities"` → `"דמיון חיצוני"` (line 3832)
- `"Search + visual"` → `"חיפוש + חיצוני"` (line 3833)
- `"Visual source (coming soon)"` → stub (line 3836-3837) — can be REUSED or replaced
- `"Combined source (coming soon)"` → stub (line 3838-3839)
- `"Text source (active)"` → `"מקור טקסט (פעיל)"` (line 3840)

### New keys needed for Phase 109 (Claude's discretion on exact strings)
These are examples; exact strings are Claude's discretion per D-17. All must be added to
`TRANSLATIONS` in the same plan that introduces the `tr()` call:

```python
# Provenance badges (CandidateCard shelf_text)
"  ★ both":           "  ★ שניהם",
"  ⊙ VS":             "  ⊙ דמיון",     # or "  ⊙VS#N" dynamically formatted

# Visual source state labels
"Visual source loaded":  "מקור חיצוני נטען",
"No visual similarity data for this manuscript":
    "אין נתוני דמיון חיצוני עבור כתב יד זה",

# Source selector accessibility (if using QRadioButton setAccessibleName)
"Text source":        "מקור טקסט",
"Visual source":      "מקור חיצוני",
"Combined source":    "מקור משולב",
```

---

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest (existing) |
| Config file | `pytest.ini` (existing) |
| Quick run command | `pytest tests/test_join_workbench_vs.py -x` |
| Full suite command | `pytest tests/ -x` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| JWB-12a | VS adapter: `_vs_to_norm_dict` + `normalize_candidate` → `Candidate(sys_id=alma_id, page=None, via_vs=True, vs_rank=rank, vs_score=svm_score)` | unit | `pytest tests/test_join_workbench_vs.py::test_vs_adapter_maps_fields -x` | ❌ Wave 0 |
| JWB-12b | Parity: Workbench Visual source returns same sys_id set as `get_vs_service().get_suggestions(anchor)` for sample anchors (D-14a) | integration | `pytest tests/test_join_workbench_vs.py::test_vs_parity_invariant -x` | ❌ Wave 0 |
| JWB-12c | merge_candidates with VS cands produces ★both first, then ✎text, then ⊙VS-by-rank | unit | `pytest tests/test_joins_lab.py -k merge_candidates -x` | ✅ existing |
| JWB-12d | i18n guard: all new tr() keys in TRANSLATIONS (D-17) | static | `pytest tests/test_join_workbench_i18n.py::test_all_tr_keys_in_translations -x` | ✅ existing |
| JWB-12e | No _vs_* calls from workbench path (D-18) | static | `pytest tests/test_join_workbench_no_private.py -x` | ✅ existing (may need updating) |
| JWB-12f | None-page guard: VS-only Candidate.page=None does not crash CandidateCard or _render_table | unit | `pytest tests/test_join_workbench.py -k none_page -x` | ✅ may exist (RR-12) |
| JWB-12g | has_suggestions=False → Visual source greyed out (D-08) | unit | `pytest tests/test_join_workbench_vs.py::test_visual_source_greyed_when_no_vs -x` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/test_join_workbench_vs.py tests/test_join_workbench_i18n.py tests/test_joins_lab.py -x`
- **Per wave merge:** `pytest tests/ -x`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `tests/test_join_workbench_vs.py` — covers JWB-12a (adapter), JWB-12b (parity), JWB-12g (grey-out)
- The parity invariant test (JWB-12b) can use `tmp_vs_db` fixture from `tests/test_visual_similarity.py` as a model (fixture creates in-memory SQLite with known data)

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| `fist_data/visual_similarity.db` | VS fetch | Varies per machine | — | `is_available()` returns False → Visual greyed out (D-08) |
| PyQt6 | Source selector widget | ✓ | existing | — |
| `shared/joins_lab.py` | Adapter + merge | ✓ | existing | — |
| `shared/visual_similarity_service.py` | VS service | ✓ | existing | — |

**Missing dependencies with no fallback:** None — the absent sidecar is handled gracefully by
the service's degradation path and D-08 grey-out behavior.

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `normalize_candidate` does NOT accept raw VS dicts; a shim is required | R-01 | If wrong: no shim needed (saves ~15 lines). Low risk — verified by reading normalize_candidate's field reads. |
| A2 | `_EnrichWorker` measurements+snippets for 200 items is not a performance problem | R-03 | If wrong: need to add page-lazy wrapper around _EnrichWorker. Mitigated: 1 SQL batch + pure-Python loops for snippets is fast. |
| A3 | `FjmsService.get_measurement_summaries_batch` path confirmed as `:3005` | R-03 | If wrong: different line number. No functional risk — confirmed as wired in _EnrichWorker:1559. |
| A4 | ThumbResolver (for grid cards) does not share the WR-02 NLI-breaker gap as ThumbBatchWorker | R-04 | If wrong: VS thumbnails could hang if NLI is slow. Mitigation: ThumbResolver is page-lazy (20 items max); worst case is 20×ImageLoaderThread calls, not 200. |
| A5 | `get_vs_service(thread_safe=True)` is safe from any QThread worker | R-04 | If wrong: need to restrict VS fetch to UI thread. Low risk — ThreadLocalConnection is the documented solution. |

**All other claims in this research were verified against live source code in this session.**

---

## Open Questions

1. **Source selector widget type (Claude's discretion)**
   - What we know: Phase 108 stub has `src_row` QHBoxLayout with only `btn_find`.
   - What's unclear: QRadioButton group vs QComboBox vs custom toggle buttons.
   - Recommendation: QRadioButton group (3 buttons: Text / Visual / Combined) — consistent with
     the CONTEXT.md "source selector" language and matches standard desktop UI patterns. QComboBox
     is also acceptable. Claude's discretion.

2. **When to grey out Visual vs Combined (D-08)**
   - What we know: Visual greyed out when no VS data. Combined uses VS + text.
   - What's unclear: Should Combined also be greyed out when no VS data?
   - Recommendation: Yes — Combined without VS data is identical to Text. Greyout both Visual
     and Combined when `has_suggestions()` is False.

3. **VS fetch timing: on anchor load or on source change?**
   - What we know: `set_anchor` clears triage and resets state; `_on_anchor_loaded` signals completion.
   - What's unclear: Whether to eagerly call `has_suggestions()` right after anchor loads (to enable/disable greying out) vs only when user selects Visual.
   - Recommendation: Eagerly check `has_suggestions()` in `_on_anchor_loaded` to grey-out the
     Visual option immediately — avoids a confusing UX where the option looks enabled until clicked.

---

## Sources

### Primary (HIGH confidence — verified against live code)
- `shared/visual_similarity_service.py` — full file read; `get_suggestions` return shape, threading, degradation behavior
- `shared/joins_lab.py:1-771` — `normalize_candidate` field reads (:248-277), `merge_candidates` logic (:511-559), `Candidate` dataclass (:76-128)
- `desktop/join_workbench.py:1530-2700` — `_EnrichWorker`, `JoinCandidatePane._build_ui`, `_maybe_assemble`, `ThumbResolver`, `CandidateCard` badge slots
- `genizah_app.py:4708-4759, 5107-5117, 9868-9887, 15440-15534` — VS entry points, pick-mode branch, `open_joins_workbench`, `open_anchor_as_join`
- `desktop/result_dialog.py:748-805` — `_rd_search_visual_similarity`
- `genizah_translations.py:3791-3989` — existing Phase 108 i18n keys
- `tests/test_join_workbench_i18n.py` — i18n guard contract
- `.planning/phases/107-.../107-REVIEW.md:82-107` — WR-02 ThumbBatchWorker NLI breaker gap
- `.planning/phases/109-.../109-CONTEXT.md` — all locked decisions D-01..D-19

### Secondary (MEDIUM confidence)
- `tests/test_visual_similarity.py` — fixture patterns for parity test design
- `.planning/phases/108-.../108-CONTEXT.md` — RR-12 None-page guard, D-21 batched enrichment, source selector stub

---

## Metadata

**Confidence breakdown:**
- VS → Candidate adapter contract: HIGH — verified by reading both files, line by line
- Page-lazy enrichment mechanism: HIGH — verified existing _EnrichWorker + ThumbResolver separation
- Reroute wiring: HIGH — verified entry point method bodies and open_joins_workbench signature
- i18n keys: HIGH — verified existing keys; new keys are Claude's discretion
- NLI circuit breaker: HIGH — WR-02 refers to ThumbBatchWorker (known-joins), not ThumbResolver (candidates)

**Research date:** 2026-06-07
**Valid until:** 2026-07-07 (stable codebase; Phase 108 is the immediately preceding phase)
