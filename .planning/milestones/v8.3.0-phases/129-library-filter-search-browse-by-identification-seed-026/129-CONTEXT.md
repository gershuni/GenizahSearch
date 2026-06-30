# Phase 129: Library Filter — Search + Browse-by-Identification (SEED-026) - Context

**Gathered:** 2026-06-28
**Status:** Ready for planning

<domain>
## Phase Boundary

Add a **library filter** keyed on `library_code` (canonical list `LIBRARY_CODES`; Hebrew labels `LIBRARY_CODES_HE`) to three surfaces, at full both-apps parity for the public v8.3.0 release:

1. **Web `/search` results** — a **multi-select** library filter applied over the **FULL** result set **BEFORE** the `[:200]` render cap (empty selection = all). Persisted via the `web/safe_storage.py` chokepoint (Phase 87 invariant, CI allowlist `[]`). Removable chips. i18n EN/HE.
2. **Web Browse-by-Identification** (catalog, `web/pages/catalog_browse.py`) — a `library_codes` arg **pushed DOWN** into `shared/fjms_service.py::get_browse_results`, applied **BEFORE** `COUNT(DISTINCT AlmaId)` + `LIMIT/OFFSET` so `total`/pagination stay correct over the full filtered set. Additive / backward-compatible (None/empty = no-op). Composes with the SEED-023 PGP/Editions filters. Persisted via `safe_storage`.
3. **Desktop catalog Browse-by-Identification** — the same library filter at parity (BUILD NOW — see D-04). Desktop search-results already filters by library/shelfmark and is **untouched**.

**In scope:** the three surfaces above; the new `library_codes` arg to `get_browse_results`.
**Out of scope:** a library filter on any other page; the existing desktop search-results library/shelfmark filtering (already shipped, untouched); changing how `library_code` is assigned; an `/api/search` / `/api/browse` library-filter param (noted as a natural follow-up, not this phase).

**Process gate:** SEED-026 + roadmap success-criterion #4 require a **Codex-review-BEFORE-code gate** ([[feedback_audit_to_cloud_pipeline]]) — the design crux (catalog row → `library_code` mapping + cheapest push-down query shape) must be reviewed before implementation. See `<deferred>`/`<code_context>` for the crux resolution found during scout.
</domain>

<decisions>
## Implementation Decisions

### Library labels (D-01)
- **D-01:** Show **human-readable library names, EN + HE** in the filter list and chips — e.g. "Cambridge UL" / Hebrew via `LIBRARY_CODES_HE`. Use the existing `get_library_display(code, short=...)` helper (already in `shared/browse_map_utils.py`, used across web + desktop). **No English leak under Hebrew UI** (the standing i18n invariant — see [[reference_i18n_audit_method]]). NOT raw codes (CUL/JTS), NOT "name (code)".

### Facet behavior (D-02)
- **D-02:** **Facet on web search, plain list on catalog.**
  - **Web `/search`:** facet-style — show **per-library result counts** and **hide libraries with 0 matches** in the current result set. This is cheap because the full pre-`[:200]` result set is already in hand at filter time (group/count it client-or-server-side without an extra DB query).
  - **Web + desktop catalog Browse-by-Identification:** a **plain list** of libraries, **no per-library counts** — avoids an extra `GROUP BY` over the server-side-paginated browse query. (If a facet count later proves cheap there, it's an additive follow-up, not this phase.)

### Control UI & placement (D-03)
- **D-03:** A **compact "Filter by library" dropdown / menu-button containing a checklist**, placed **beside the existing PGP / Printed filter buttons** (web search) and beside the SEED-023 PGP/Editions filter controls (catalog). Active selections render as **removable chips** consistent with the existing filter chips. NOT an always-visible inline checklist (saves vertical space, matches the current filter row). Empty selection = all (no chips shown).

### Desktop parity scope (D-04)
- **D-04:** **Build the desktop catalog Browse-by-Identification library filter NOW** (full parity, LIBFILTER-03) — the seed hedged "likely deferred" but the user confirmed full parity so desktop earns the v8.3.0 version bump with a visible feature. The wiring already exists: `_get_catalog_filter_sets()` + `_CatalogRefreshWorker` already thread filter sets into `fjms.get_browse_results` (`genizah_app.py:454`/`488`/`537`), so the library filter slots into that same path. Desktop search-results library/shelfmark filtering stays untouched.

### Claude's Discretion
- Exact widget for the dropdown/checklist on each platform (NiceGUI `ui.menu`/`ui.select` with `multiple` vs a custom checklist; Qt `QMenu` with checkable actions vs a multi-select combo) — planner/implementer's call, as long as D-01..D-03 hold (human EN/HE labels, web facet counts + hide-empty, dropdown-with-chips, RTL-correct).
- The cheapest **catalog push-down query shape** (resolve `library_codes` → a sys-id set via `meta_mgr` / a reverse map, or intersect on a temp table keyed by `AlmaId`, per SEED-023's "don't pass a giant `IN (...)`" SHOULD-FIX) — researcher/Codex to pin down. The mapping primitive (`meta_mgr.get_library_for_id(sid)`) is confirmed to exist; the bulk/reverse direction is the open implementation detail.
</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Feature spec (the originating seed — read first)
- `.planning/seeds/SEED-026-library-filter-web-search-and-catalog-browse.md` — intent, the two design cruxes (web post-search filter over the full set; catalog push-down into `get_browse_results` before COUNT/LIMIT), open questions (now resolved in D-01..D-04), and the `[:200]` render-cap caution.

### Template to copy (the exact prior pattern)
- `.planning/seeds/SEED-023-homepage-stats-and-catalog-pgp-edition-filters.md` — the 3-state PGP/Editions filter that was threaded into `get_browse_results` + persisted via `safe_storage`. SEED-026 builds the catalog side on this exact push-down-into-the-query + chip + safe_storage template; the library filter must **compose** with it (intersection).

### Milestone tracking
- `.planning/ROADMAP.md` § "Phase 129: Library Filter — Search + Browse-by-Identification (SEED-026)" — goal + 5 success criteria (incl. the Codex-review-before-code gate).
- `.planning/REQUIREMENTS.md` § "Library Filter (LIBFILTER)" — requirements **LIBFILTER-01** (web search), **LIBFILTER-02** (catalog push-down), **LIBFILTER-03** (desktop parity); + **GUARD-02** (zero behavior change to existing filters/suite).

No external ADRs beyond the two seeds — requirements fully captured above.
</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **sys_id → library_code map (design crux #4 — RESOLVED):** `meta_mgr.get_library_for_id(sid)` returns a row's `library_code` and is ALREADY used for catalog rows on both apps:
  - Web catalog: `web/pages/catalog_browse.py:350` — `library_code = state.meta_mgr.get_library_for_id(sid) or ''`.
  - Desktop: `genizah_app.py:6621`, `:9469`, `:9543` — `self.meta_mgr.get_library_for_id(sid)`.
  So catalog rows already derive their library; the push-down filter needs the **bulk/reverse** direction (library_codes → AlmaId set) — that's the remaining query-shape detail for research.
- **Library display helper:** `get_library_display(library_code, short=...)` (`shared/browse_map_utils.py`, extracted in Phase 123) + `LIBRARY_CODES` / `LIBRARY_CODES_HE` — the canonical EN/HE label source for D-01.
- **SEED-023 catalog filter (the template), web:** `web/pages/catalog_browse.py` — `catalog_pgp_filter`/`catalog_editions_filter` read via `safe_user_get` (lines 106-112), passed to `get_browse_results(pgp_filter=..., editions_filter=...)` (lines 259-264), rendered as removable chips (lines 693-729). Add a `catalog_library_filter` (a list) on the same pattern.
- **SEED-023 catalog filter (the template), desktop:** `genizah_app.py:454` `_get_catalog_filter_sets()` (+ `reset_catalog_filter_sets` :480) and `_CatalogRefreshWorker` (:488, calls `fjms.get_browse_results` at :537). Catalog tab registered at `genizah_app.py:1763` ("Browse by Identification"); CATALOG BROWSE TAB section at `:9561`.
- **`get_browse_results` signature:** `shared/fjms_service.py:2025` — already has `pgp_filter`/`pgp_sys_ids`/`editions_filter`/`edition_sys_ids` plus the total/limit/offset path. Add an additive `library_codes` (and/or precomputed `library_sys_ids`) arg following the same intersection-before-COUNT pattern. `get_filter_sys_ids` (:942) shows the existing "set of sys_ids matching all filters" intersection approach to mirror.
- **Web search filters (the multi-select goes here):** `web/pages/search.py` — `printed_filter`/`pgp_filter` are 3-state cycling buttons persisted via `persist_value`/`_safe_get` (`search_printed_filter` :183, `search_pgp_filter` :184; toggle/btn around :1475-1502); the chip bar is `_update_chip_bar` (:101) + text/measurement chips (:1209-1223). The library multi-select + chips slot into this filter row + chip bar; persist as `search_library_filter` (a list).

### Established Patterns
- **Web search filter timing (LOCKED):** apply the library filter over the FULL result set BEFORE `[:200]` — the full pre-cap list must still be in hand at filter time (it is; PGP/printed already post-filter the results). Do NOT make it a client-only filter of the visible 200.
- **Catalog push-down (LOCKED, SEED-023 B3 lesson):** the filter MUST apply before `COUNT(DISTINCT AlmaId)` and `LIMIT/OFFSET` — page-level post-filtering corrupts `total` + pagination.
- **Persistence:** ALL per-user filter state goes through `safe_storage` (web) — Phase 87 chokepoint, CI guard allowlist must stay `[]`.
- **Decomposition note:** library/browse-map helpers now live in `shared/browse_map_utils.py` (Phase 123); `MetadataManager` in `shared/metadata_manager.py` (Phase 124). Desktop catalog code is still in `genizah_app.py` (catalog panel DEFERRED to SEED-028, NOT extracted) — implement desktop in place there.

### Integration Points
- Web search: the `web/pages/search.py` filter row + chip bar + the post-search result-filtering path (alongside `pgp_filter`/`printed_filter`).
- Web catalog: `web/pages/catalog_browse.py` filter controls + chips + the `get_browse_results(...)` call.
- Shared: `shared/fjms_service.py::get_browse_results` new additive `library_codes` arg.
- Desktop catalog: `_get_catalog_filter_sets` / `_CatalogRefreshWorker` / the catalog browse tab UI in `genizah_app.py`.
</code_context>

<specifics>
## Specific Ideas

- User's literal request (Hillel, 2026-06-26): "filtering by library in Web search, and in Browse by Identification (which can be used also on search). It may be useful also on desktop, though on desktop you can filter search results by library and by shelfmark."
- Desktop is full parity for v8.3.0 (D-04) so the desktop binary earns the public version bump with a visible feature, not just the invisible decomposition refactor.
</specifics>

<deferred>
## Deferred Ideas

- **API library-filter param** (`/api/search`, `/api/browse`) — a natural follow-up to SEED-026 (the endpoints already exist); explicitly out of scope here, note for a later add.
- **Catalog facet counts** — D-02 keeps catalog as a plain list (no `GROUP BY`); per-library counts there could be an additive follow-up if it proves cheap.
- **Library filter on other pages** (reading desk, Joins Lab, puzzle) — out of scope; not requested.

### Reviewed Todos (not folded)
The `todo.match-phase` matches were all keyword-fuzzy and off-topic for the library filter (desktop corrections migration, Reading-Desk UX fixes, server-side search w/ email, NLI MARC crawl, unified metadata search, one-click citations). None touch `library_code` filtering — none folded.
</deferred>

---

*Phase: 129-Library Filter — Search + Browse-by-Identification (SEED-026)*
*Context gathered: 2026-06-28*
