---
id: SEED-026
status: dormant
planted: 2026-06-26
planted_during: User feature request (Hillel, 2026-06-26) during mid-Phase-125 (v8.3.0 god-file decomposition) — parked as a seed rather than implemented inline ([[feedback_seed_midphase_fixes_to_cloud]]); UNRELATED to the decomposition milestone.
trigger_when: A web search/browse UX pass or a standalone /gsd-quick. Reuses the SEED-023 filter pattern (3-state PGP/Editions filters threaded into the FJMS browse query) — build the catalog side on that exact template. Codex-review this seed before coding (project seed-review gate, [[feedback_audit_to_cloud_pipeline]]).
scope: medium (a library multi-select on web search results + a library filter pushed into shared/fjms_service.get_browse_results for catalog browse; desktop parity optional)
---

# SEED-026: Filter by library — Web search + Browse by Identification (and desktop parity)

> User intent (Hillel, 2026-06-26): "filtering by library in Web search, and in Browse by
> Identification (which can be used also on search). It may be useful also on desktop, though on
> desktop you can filter search results by library and by shelfmark."

Add a **library filter** to two web surfaces:
1. **Web search results** (`/search`) — filter the result set by one or more libraries (CUL, JTS,
   RNL, Oxford, Manchester, BL, AIU, Mosseri, Gaster, Halper, NLI, …).
2. **Browse by Identification** (the catalog page, `/catalog` → `catalog_browse.py`) — which doubles
   as a search/browse surface, so a library filter there serves both browse and search use cases.

Desktop ALREADY filters search results by library and by shelfmark (per user) — so desktop is
**parity-only / optional**: the gap there is at most a library filter on its "Browse by
Identification" catalog view (assess at trigger time; not the priority).

## Key field
- Library identity = **`library_code`** (CUL ~128K, JTS ~30K, RNL ~17K, Oxford ~13K, Manchester ~12K,
  BL ~8K, AIU, Mosseri, Gaster, Halper, NLI, …). Canonical list: `genizah_core.LIBRARY_CODES`
  (post-v8.3.0 this may have moved to a `shared/` module — grep, don't trust the path).
- `libraries.csv` column 3 is the library_code; the catalog (FJMS) is keyed by `AlmaId == sys_id`,
  so catalog rows derive their library via the manuscript metadata / a sys_id→library_code map, NOT
  necessarily a column on the FJMS `catalog` table — confirm at trigger time (this is the design crux
  for the catalog side, see below).

## Design crux #1 — Web search (post-search filter, NOT 3-state)
Unlike PGP/printed (binary → 3-state button), library is a **set of ~11+ codes** → use a
**multi-select** (chips / dropdown / checklist), not a cycling 3-state button. Mirror what desktop
already does for search results.
- Each result already carries its library (derivable from `library_code` / sys_id). A post-search
  multi-select filters the displayed results to the chosen libraries (empty selection = all).
- **CAUTION — the [:200] render cap.** Search render is capped at `[:200]` results (WebSocket/memory
  safety, see memory "Project Architecture"). A purely client/post-render library filter only filters
  the visible 200, which is misleading. Decide at trigger time: (a) apply the library filter over the
  FULL result set BEFORE the [:200] cap (preferred — accurate), or (b) make it a PRE-search constraint
  fed into the query. Prefer (a) if the full pre-cap result list is still in hand at filter time.
- Consider surfacing only the library_codes actually present in the current result set (so the
  multi-select isn't a wall of 11 mostly-empty options) + a per-option count, like a facet.
- **Persist** the selection via the `web/safe_storage.py` chokepoint (Phase 87 invariant; CI guard
  allowlist `[]`) — same as `pgp_filter` / `printed_filter`. Removable chips consistent with existing
  filter chips.

## Design crux #2 — Browse by Identification (push DOWN into the FJMS query)
`catalog_browse` is **server-side paginated** (`results` + `total` via
`shared/fjms_service.py::get_browse_results`). A filter MUST apply to the FULL result set before
`COUNT(DISTINCT c.AlmaId)` and `LIMIT/OFFSET` — page-level post-filtering corrupts totals +
pagination (this is exactly the SEED-023 B3 lesson; PGP/Editions filters were threaded into
`get_browse_results` for this reason).
- **Build on the SEED-023 template:** thread a `library_codes` filter arg into
  `shared/fjms_service.py::get_browse_results` (it already has the total/limit/offset path and now the
  SEED-023 PGP/edition intersection plumbing). Apply the library condition BEFORE the COUNT and the
  LIMIT/OFFSET.
- The mechanism depends on where library lives relative to the FJMS catalog: if catalog rows can be
  mapped to `library_code` via `AlmaId==sys_id` + the manuscript metadata, intersect on a
  sys_id-set-per-library (or a temp table keyed by `AlmaId`, per SEED-023's SHOULD-FIX on not passing
  a giant `IN (...)`). VERIFY the cheapest path at trigger time.
- **Persist** via `safe_storage`; removable chips consistent with the existing domain/author/work +
  SEED-023 PGP/Editions chips.

## Open questions (resolve at trigger time)
1. **Multi-select vs single?** Default: multi-select (choose any subset; empty = all). Confirm.
2. **Facet counts?** Show per-library result counts and/or hide libraries with 0 matches in the
   current set? (Nice UX, costs a group-by.)
3. **Search filter timing** — over the full pre-[:200] set (preferred) vs pre-search constraint.
4. **Catalog library source** — does `get_browse_results` already know each row's library, or must we
   join through a sys_id→library_code map / temp table? (Determines query shape.)
5. **Library label/i18n** — show raw codes (CUL/JTS) or human names? If names, EN+HE labels (no
   English leak under Hebrew); `LIBRARY_CODES_HE` already exists for Hebrew library names.
6. **Desktop** — does its "Browse by Identification" catalog view want the same library filter, or is
   the existing search-results library+shelfmark filtering already sufficient? (Likely parity-only.)

## Reuse / invariants
- **Template = SEED-023** (3-state PGP/Editions filters in `get_browse_results`, persisted via
  safe_storage). Reuse its push-down-into-the-query + chip + safe_storage approach.
- Phase 87 `safe_storage` chokepoint for ALL per-user filter state (CI guard allowlist `[]`).
- Don't break existing search filters (PGP/printed) or the SEED-023 catalog PGP/Editions filters.
- `library_code` canonical source = `LIBRARY_CODES`; Hebrew names = `LIBRARY_CODES_HE`.

## Tests required
- Search: selecting one/several libraries narrows results to those `library_code`s; empty = all;
  filter applies over the FULL set (not just the visible [:200]); state persists via safe_storage;
  no English leak under Hebrew if names are shown.
- Catalog: `library_codes` filter changes `total` correctly (full set, not page subset); paginates
  correctly; `all`/empty is a no-op; composes with the SEED-023 PGP/Editions filters; persists.
- `get_browse_results`: new `library_codes` arg is additive + backward-compatible (None/empty = no-op).

## Done when
Web search has a working library (multi-select) filter that narrows the full result set accurately
and persists; Browse by Identification has a library filter pushed into `get_browse_results` that
paginates + counts correctly over the full filtered set and composes with existing filters; tests
green; ruff clean; existing filters untouched; Codex-reviewed before code. (Desktop parity assessed
separately, likely deferred.)

## NOT in scope
A library filter on any other page; pre-existing desktop search-results library/shelfmark filtering
(already shipped); changing how `library_code` is assigned; an API library-filter param (could be a
later add — `/api/search` / `/api/browse` already exist, so note it as a natural follow-up).
