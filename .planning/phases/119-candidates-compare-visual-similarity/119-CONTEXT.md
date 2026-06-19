# Phase 119: Candidates, Compare & Visual Similarity - Context

**Gathered:** 2026-06-19
**Status:** Ready for planning

<domain>
## Phase Boundary

The web Joins Lab gains the surface that makes a **large candidate set workable**, built on the
Phase-117 spine (`/joins-lab`, off-loop `WebSearchExecutor`, sticky anchor pane, read-only
candidate grid) and the Phase-118 builders + known-joins. Phase 119 delivers:

- **Candidate surface (CND-03..08):** the read-only Phase-117 grid becomes a working surface —
  grid **+** sortable/multi-select **table** sharing ONE per-`sys_id` triage (Y/?/N) and 👁 badge
  state; candidate **filters** (material / dimensions / size-mismatch / triage-state); **paginated**
  bounded rendering; and **off-loop, batched** metadata enrichment (breaker-guarded for thumbnails).
- **Compare (CMP-01..03):** a side-by-side anchor⟷candidate panel (image + numbered RTL
  transcription) with per-pane zoom + folio navigation, candidate flip-through, and a verdict that
  syncs back to the shared triage.
- **Visual Similarity (VSM-01/02):** a single 👁 toggle that merges FIST look-alikes via a web
  VS-service adapter + `shared/joins_lab.merge_candidates`, with consistent 👁 badging across grid,
  table, and Compare.

**In scope (Phase 119 requirements):** CND-03, CND-04, CND-05, CND-06, CND-07, CND-08, CMP-01,
CMP-02, CMP-03, VSM-01, VSM-02.

**Explicitly NOT in this phase (locked elsewhere — do not pull forward):**
- Add-as-join (ACT-01), bulk Add-to-Puzzle (ACT-02), add-to-list / export (ACT-03) → **Phase 120**.
  (119 builds multi-select + bulk triage; the *actions* multi-select feeds are Phase 120.)
- Cross-refresh / cross-navigation **persistence** of triage/filter/view + re-run-on-restore →
  **Phase 120** (PST-01..03). 119's triage/filter/view state is in-memory page state.
- Complete i18n coverage pass + RTL audit + Hebrew-leak AST audit → **Phase 121** (but every new
  119 string is bilingual via `tr()` from line one).

</domain>

<decisions>
## Implementation Decisions

### Compare (CMP-01 / CMP-02 / CMP-03) — = desktop parity
- **D-01:** Compare is a **full-screen modal overlay** (`ui.dialog` filling the viewport): anchor
  pane | candidate pane, each pane reusing the **extracted `/browse` image viewer** (Phase 117
  D-10 — the SAME per-pane viewer the anchor uses) → image zoom/pan + **per-pane independent** folio
  navigation, plus the numbered RTL transcription. Mirrors the desktop modeless Compare window
  (`desktop/join_workbench.py:3724` 1320×870; `_fill_anchor:4051` / `_fill_candidate:4086`; per-pane
  `zoom` dict `:3823` + `_pane_page` nav `:4243`).
- **D-02:** **Flip-through navigation INSIDE Compare** — ‹ Prev / Next › step through candidates in
  the current sort/filter order while Compare stays open (parity `step(delta)` over `wb.filtered`,
  `:3741`/`:3753`). Compare opens from a grid card, a table row (double-click parity `:3207`), or a
  shortcut.
- **D-03:** **Y/?/N verdict buttons live in Compare; recording a verdict AUTO-ADVANCES to the next
  candidate** (manual ‹ › still allowed, including back). The verdict syncs **immediately** to the
  `sys_id`-keyed triage shared with grid + table — no refresh (parity `_mark → wb.mark(sys_id,val) →
  triage[sys_id] → restyle`, `:4202`/`:4981`/`:3344`).

### Visual Similarity (VSM-01 / VSM-02) — = desktop conditional model
- **D-04:** Single **👁 toggle**. ON behavior follows the **desktop conditional model**
  (`desktop/join_workbench.py:2788-2802`):
  - **ON + builder has a query** (the dominant Joins-Lab case) → **INTERSECTION**: keep only
    candidates with `c.via_text AND c.via_vs` (high-confidence — both text-matched and visually
    similar).
  - **ON + empty builder** → **UNION** (pure VS browse): `merge_candidates([], vs)` — all FIST
    look-alikes for the anchor.
  - **OFF** → text-only, but look-alikes among the text hits still carry the 👁 badge
    (`merge_candidates(text, vs)` keeping `via_text`).
  This resolves the requirement's ambiguous "VS-merged / intersection" wording.
- **D-05:** Look-alikes are fetched via a **thin web VS-service adapter** mirroring the desktop
  `_vs_adapter_v1` (`:228`): call `shared/visual_similarity_service.get_vs_service().get_suggestions(
  sys_id, limit=200)` (returns `{alma_id, svm_score, rank}`; **`alma_id == sys_id`**), map to
  `Candidate(via_vs=True, vs_rank=..., vs_score=...)`, and feed `shared/joins_lab.merge_candidates`.
  The lookup is a **LOCAL `visual_similarity.db` SQLite read** → run **off the event loop**
  (`run.io_bound`) but it does **NOT** need the NLI circuit breaker (only thumbnail image fetches do).
  The existing web VS path is `web/components/visual_similarity_dialog.py:show_visual_similarity_dialog`
  (`get_suggestions` at `:176`) — reuse the service, not the dialog.
- **D-06:** The toggle **tracks the loaded anchor sid** — look-alikes invalidate/refetch on
  re-anchor (VSM-01). Explicit **disabled / no-VS-data / empty-intersection** states (e.g. ON+query
  but zero candidates are both) render a clear empty/disabled affordance, never a blank surface.
- **D-07:** **👁 badge consistent across grid, table, AND Compare** via the shared
  `shared/joins_lab.badge_and_tooltip()` precedence: **⚓ `is_anchor_self` › ⇄ `via_other_side` ›
  👁 `via_vs`** (`desktop/join_workbench.py:452-457`). Reuse that helper — do not invent a second
  badge rule.

### Candidate surface — bounding (CND-07)
- **D-08:** **Paginate through the entire filtered candidate set** — page controls, **~24/page** in
  grid (parity with desktop's `_PER_PAGE = 20`), rendering **only the current page** so per-page
  WebSocket payload stays small. This **replaces** Phase-117's `_MAX_RENDERED_CANDIDATES = 200`
  silent hard-cap (`web/components/candidate_grid.py:45`) — **pagination is the bound, not
  truncation**. The engine already caps the pool (~100 normal / ≤500 fuzzy), so the set is never
  truly unbounded. Filters apply **before** pagination; triage persists across page changes.

### Candidate surface — views (CND-02 / CND-03 / CND-04)
- **D-09:** **Grid is the default view, with LARGE thumbnails.** The Phase-117 grid's 48×48
  thumbnails are too small — the Joins-Lab grid needs **visually-triageable** fragment images
  (sizable image-first cards). Pairs naturally with fewer-but-bigger cards per page (D-08).
  *(User-emphasized: "the images should be large enough.")*
- **D-10:** **Table view (toggle from grid)** = parity 8-column shape (Checkbox | Shelfmark | Score |
  Snippet | Material | Dimensions | Page | Triage — `:2449-2454`) **but web ADDS sortable columns +
  multi-select** — the desktop table is sort-disabled (`setSortingEnabled(False)` `:2464`), so
  sorting is a deliberate **web addition** per CND-03. Default sort by relevance/score; switch to
  **VS-rank** when 👁 is ON (discretion). **Both views share the SAME `sys_id`-keyed triage + 👁
  badge state** — switching view never resets or hides per-candidate state.
- **D-11:** **Triage Y/?/N keyed by `sys_id`** (parity `wb.triage[sys_id]`, values `yes`/`maybe`/
  `no`), held as **in-memory page state this phase** — survives grid↔table↔Compare switches and
  pagination within a session; **resets on re-anchor**. Cross-refresh **persistence is Phase 120**
  (PST-01) — 119 does **NOT** write triage to `safe_storage`.
- **D-12:** **Multi-select (table) ships with BULK TRIAGE in 119** — mark all selected rows
  Y/?/N in one action (a real 119 capability). Selection state is structured so Phase 120 can wire
  bulk Add-to-Puzzle / Add-to-List onto it (ACT-02/03) — those **actions** are NOT in 119.

### Self-match (CND-05) — DIVERGENCE (parity over the spec'd banner)
- **D-13:** Self-match → **silently exclude the anchor** (`dedup_candidates(include_self=False)`),
  **NO banner / NO readout UI**. `detect_self_match` still runs (the anchor is correctly excluded).
  **Documented divergence from CND-05 / ROADMAP SC#2** (which call for "a self-match readout/banner"):
  user chose **desktop parity** — the desktop also silently excludes the anchor with no banner
  (`include_self = False` hardcoded `:2561`; `_anchor_matched` tracked but never surfaced). **Flag
  for planner + verifier** so the phase is not failed for a missing banner — CND-05 is satisfied by
  correct exclusion, not by a UI surface. (User declined even a subtle notice.)

### Filters (CND-06)
- **D-14:** **Filters live in a popover/dialog** (desktop parity — `_open_filter_dialog:3373`),
  opened by a "Filters" button; the candidate surface stays clean until opened. Dimensions:
  **material / dimensions (has-dimensions) / size-mismatch / triage-state** (text-filter parity field
  optional — discretion). Filters persist across grid↔table.
- **D-15:** **Size-mismatch = parity formula**: `ratio = max(w, anchor_w) / min(w, anchor_w)`,
  flagged when **ratio > 1.4** (`:1687-1695`), compared against the anchor's `width_cm`. Join-critical
  (a physical join must be ~same size). Keep the **1.4** default; an advanced numeric min/max is
  discretion.

### Enrichment (CND-08)
- **D-16:** Candidate metadata is enriched **off the event loop, batched**: `material` / `width_cm`
  / `height_cm` via `shared/fjms_service.get_measurement_summaries_batch()` per `sys_id` (parity with
  the desktop `_EnrichWorker` `:1671`). This is a **LOCAL `fjms_enrichment.db` read** → needs
  `run.io_bound` but **NOT** the breaker. **Thumbnail IMAGE fetches** go through the existing
  per-provider proxy + **Phase-98 NLI circuit breaker** — an NLI outage degrades thumbnails
  gracefully without stalling the surface. Enrichment feeds the filters (material/dimensions/
  size-mismatch) and the table columns.

### Claude's Discretion
- Table default sort column + the VS-rank-when-👁-on sort switch (D-10).
- Exact thumbnail/card dimensions ("large enough" → sizable image-first cards) + grid
  columns-per-row responsive breakpoints + the exact page-size (~24) and pagination control styling
  (D-08/D-09).
- Empty / disabled / no-VS-data / empty-intersection state wording for the 👁 toggle (D-06).
- Whether the parity text-filter field is included in the filter dialog (D-14).
- Self-match internal handling beyond silent exclusion (D-13) — no UI either way.
- Exact Compare launch affordances beyond card/row/double-click (D-02) and the verdict-button layout.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Milestone requirements, roadmap & pre-lock critique (read first)
- `.planning/REQUIREMENTS.md` — the 37 v8.2.0 requirements; Phase 119 owns CND-03..08, CMP-01..03,
  VSM-01/02. Carries the hard cross-phase constraints (safe_storage chokepoint, proxy+breaker,
  off-loop, bilingual, no new Supabase schema, no automated finder).
- `.planning/ROADMAP.md` §"Phase 119: Candidates, Compare & Visual Similarity" — the 6 success
  criteria this phase is verified against; §"Hard constraints across all phases". **Note SC#2 names a
  "self-match banner" that D-13 deliberately drops — see Divergences.**
- `.planning/v8.2.0-REQ-CODEX-CRITIQUE.md` — code-grounded pre-lock critique (off-loop, breaker,
  safe_storage invariants that bind every 119 search/VS/enrichment path).
- `.planning/phases/117-vertical-spine/117-CONTEXT.md` — esp. **D-04** (layout reserved room for the
  119 table view + Compare), **D-10/D-11** (the extracted `/browse` viewer Compare reuses; proxy +
  breaker), **D-16** (the off-loop discipline VS + enrichment must also obey).
- `.planning/phases/118-joins-entry-full-builders/118-CONTEXT.md` — the builder/known-joins layer
  119 sits on; **D-01** (web-idiomatic, desktop = parity north star).

### The shared core this phase rides (do not re-implement)
- `shared/joins_lab.py` — `dedup_candidates(..., include_self=False)` (`:505`), `merge_candidates`
  (union/tiering: tier0 both › tier1 text-only › tier2 vs-only, `:547`), `detect_self_match`
  (`:601`), `Candidate` (`via_text` / `via_vs` / `vs_rank` / `vs_score` / `is_anchor_self` / `.key`),
  `normalize_candidate`, and the `badge_and_tooltip` precedence helper.
- `shared/visual_similarity_service.py` — `get_vs_service().get_suggestions(sys_id, limit=200)` →
  list of `{alma_id, svm_score, rank}`; LOCAL `visual_similarity.db` SQLite (no network). The VS data
  source for D-05.
- `shared/fjms_service.py` — `get_measurement_summaries_batch()` (material / width_cm / height_cm);
  LOCAL `fjms_enrichment.db`. The enrichment + size-mismatch data source for D-15/D-16.

### Parity reference — the desktop Joins Lab being ported (north star)
- `desktop/join_workbench.py` — **Views:** default `view_mode="grid"` (`:2200`), `_GRID_COLS`
  (`:2172`), `_PER_PAGE=20` (`:2173`), `_render_grid_page` (`:3022`), `_render_table` (`:3085`, cols
  `:2449-2454`, `setSortingEnabled(False)` `:2464`, `SelectRows` multi-select `:2461`), `toggle_view`
  (`:3250`), `_update_pagination` (`:3226`). **Triage:** `wb.triage[sys_id]` / `mark` (`:4981`/`:4995`),
  glyphs `:3144`, `_restyle_card` (`:3344`). **Filters:** `_open_filter_dialog` (`:3373`),
  `apply_filters` (`:2935`), enrich material/dims (`:1671`/`:1680`/`:1701`), size-mismatch ratio>1.4
  (`:1687-1695`). **Self-match:** `include_self=False` (`:2561`), `_anchor_matched`. **Compare:**
  dialog (`:3724`), `_fill_anchor`/`_fill_candidate` (`:4051`/`:4086`), per-pane zoom (`:3823`) +
  folio nav (`:4243`), prev/next `step` (`:3741`/`:3753`), `_mark`/`wb.mark` (`:4202`). **VS:**
  conditional union/intersection (`:2788-2802`), `_vs_adapter_v1` (`:228`), `badge_and_tooltip`
  (`:452-457`).

### Web seams to extend (Phase 117/118 + reuse targets)
- `web/components/candidate_grid.py` — the Phase-117 read-only grid to extend with triage / table /
  filters / 👁 badge: `create_candidate_grid(candidates, on_browse_click=...)`, `build_thumbnail_url`
  (`:63`), `build_browse_url` (`:109`), `_MAX_RENDERED_CANDIDATES=200` (`:45`, replaced by pagination
  per D-08). Renders `sys_id`/`page`/`shelfmark`/`library_code`/`title`; thumbnails are 48×48 (D-09
  enlarges them).
- `web/pages/joins_lab.py` — the page hosting the candidate surface + Compare + 👁 toggle;
  `execute_joins_search` compose→off-loop→dedup pipeline to extend with VS merge + enrichment.
- `web/components/visual_similarity_dialog.py` — `show_visual_similarity_dialog` /
  `get_suggestions` call site (`:176`) — reuse the **service**, mirror its `run.io_bound` wrap.
- `web/pages/browse.py` — the image viewer extracted in Phase 117 (Compare panes reuse it).
- `web/safe_storage.py` — `safe_user_*` chokepoint (Phase 87). (119 keeps triage in-memory;
  persistence is Phase 120.)
- `web/translations.py` / `tr()` — bilingual strings from line one.

### Invariant guards (must stay green)
- `tests/test_no_raw_storage_access.py` — Phase 87 CI guard; allowlist MUST stay `[]`.
- `tests/test_joins_lab_off_loop.py` — the search call must stay off the event loop; **the new VS
  lookup AND the enrichment batch must also be off-loop** (`run.io_bound`).
- `docs/guides/MULTITENANT.md` — safe_storage / multitenant reference.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **`shared/joins_lab.py`** — `merge_candidates` (union/tiering), `dedup_candidates`
  (`include_self`), `detect_self_match`, `badge_and_tooltip` precedence, `Candidate` provenance
  flags. **Do not re-implement** — 119 writes UI + adapters around these.
- **`shared/visual_similarity_service.py` `get_suggestions`** — same local service the desktop and
  the existing web VS dialog already use. 119 adds only a thin `Candidate`-mapping adapter (D-05).
- **`shared/fjms_service.get_measurement_summaries_batch`** — the exact batch the desktop
  `_EnrichWorker` calls; web calls it off-loop for material/dimensions/size-mismatch (D-16).
- **`web/components/candidate_grid.py`** — the Phase-117 grid to grow into the working surface
  (triage, table toggle, filters, 👁 badge, large thumbnails, pagination).
- **Extracted `/browse` image viewer** (Phase 117 D-10) — Compare panes reuse it for per-pane
  zoom/pan + folio nav.

### Established Patterns
- **Off-loop discipline** (`run.io_bound` + generation counter + `is_running`, CI-guarded) — applies
  to the existing search AND the new VS lookup AND the enrichment batch.
- **Image fetches → per-provider proxy + Phase-98 NLI breaker** (thumbnails in grid/table/Compare);
  local SQLite reads (VS, FJMS measurements) do NOT use the breaker but still go off-loop.
- **Multitenant invariant (Phase 87):** zero raw `app.storage.user`. 119 triage/filter/view is
  in-memory page state (no `safe_storage` writes until Phase 120).
- **Web-idiomatic UI, desktop behavior = parity north star** (118 D-01).
- **WebSocket payload safety** — bounded render (now via pagination, D-08, not a silent cap).

### Integration Points
- Grid + table + filter dialog + 👁 toggle + Compare all attach to `web/pages/joins_lab.py`'s
  candidate region; reusable bits → `web/components/`.
- Triage state is a single `sys_id`-keyed page-level structure shared by grid, table, and Compare
  (one source of truth).
- VS adapter binds `shared/visual_similarity_service` → `merge_candidates`; tracks the loaded
  anchor sid for invalidation.
- Enrichment binds `shared/fjms_service` batch → filter predicates + table cells, off-loop/batched.

</code_context>

<specifics>
## Specific Ideas

- **"The images should be large enough."** — verbatim user direction on the default grid: sizable,
  visually-triageable fragment thumbnails, not the Phase-117 48×48 (D-09).
- **Visual Similarity ON = the desktop's conditional behavior** — intersection when there's a query,
  union when the builder is empty — chosen explicitly over a single always-union rule or a user
  union/intersect sub-control (D-04).
- **Self-match: silently ignore the anchor, no banner** — verbatim; chosen as desktop parity even
  though it diverges from CND-05's "readout/banner" (D-13).
- **Compare = flip-through + verdict + auto-advance** — the fast "judge → next" triage rhythm the
  user picked (D-02/D-03).
- **Multi-select should be useful immediately (bulk triage), not inert** — D-12.
- **Parity with the UAT-approved desktop Joins Lab** is the north star wherever a web-specific
  decision wasn't required.

</specifics>

<deferred>
## Deferred Ideas

- **Bulk Add-to-Puzzle / Add-to-List / Add-as-join / Export from selected candidates** → Phase 120
  (ACT-01/02/03). 119 builds the multi-select + bulk-triage substrate they ride.
- **Cross-refresh / cross-navigation persistence of triage / filter / view + re-run-on-restore** →
  Phase 120 (PST-01..03). 119's triage/filter/view is in-memory page state.
- **Self-match banner / "include anchor" toggle** — considered and **declined** (D-13); could be
  revisited as later polish if a scholar ever wants to inspect the anchor's own ranking.
- **Complete i18n / RTL / Hebrew-leak audit** → Phase 121 (119 strings are bilingual from line one).

### Reviewed Todos (not folded)
The `todo.match-phase 119` query surfaced 8 pending todos. **None folded:**
- *Joins Lab — search results should survive navigation away and back* (`area: web`, score 0.9) —
  genuinely Joins-Lab, but its own frontmatter says **`resolves_phase: 120`**: it's the
  persist-inputs + **re-run-on-restore** candidate-grid half of PST, explicitly Phase 120. The anchor
  already restores; the candidate-grid restore is 120's job (re-run from persisted inputs, no result
  snapshots). Out of 119 scope.
- The other 7 (*Migrate desktop corrections to shared service*, *Fill missing manuscripts from
  FIST.db*, *Reading Desk UX fixes*, *Server-side search w/ email notification*, *NLI MARC crawl*,
  *Unified metadata text search*, *One-click scholarly citations*) are spurious keyword
  coincidences ("service"/"view"/"image"/"metadata"/"web") — none concern the candidate surface,
  Compare, or Visual Similarity.

</deferred>

---

## Divergences from locked requirements / desktop parity (flagged for the planner)

1. **CND-05 / ROADMAP SC#2 self-match readout DROPPED** → silent anchor exclusion only, no banner
   (D-13). User decision (desktop parity). **The verifier must not fail the phase for a missing
   self-match banner** — CND-05 is met by correct exclusion via `dedup_candidates(include_self=False)`
   + `detect_self_match` running, not by a UI surface.
2. **Table SORTABLE + MULTI-SELECT added** — the desktop table is sort-disabled
   (`setSortingEnabled(False)`); web adds sorting per CND-03 and bulk-triage multi-select per D-12.
3. **Pagination REPLACES the Phase-117 200 hard-cap** (`_MAX_RENDERED_CANDIDATES`) — bounding is by
   per-page render, not silent truncation (D-08). No candidate is hidden beyond a cap.

---

*Phase: 119-candidates-compare-visual-similarity*
*Context gathered: 2026-06-19*
