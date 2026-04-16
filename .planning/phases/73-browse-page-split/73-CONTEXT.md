# Phase 73: Browse Page Split - Context

**Gathered:** 2026-04-16
**Status:** Ready for planning

<domain>
## Phase Boundary

Decompose `web/pages/browse.py` (~5,076 lines) into focused modules for state and enrichment. Second web decomposition phase, following the Phase 72 search split pattern. browse.py remains the entry point and retains `create_browse_page()`, `load_page()`, and `update_content()`.

In scope:
- **`web/pages/browse_state.py`** (new): `BrowseState` class, small state/reset helpers
- **`web/pages/browse_enrichment.py`** (new): `_load_enrichment()`, `_update_enrichment_sections()`, `_populate_bib_catalog_buttons()`, enrichment fetch helpers, `BrowsePageRefs` context dataclass
- **`web/pages/browse.py`** (modified): remains entry point, imports from split modules, retains `create_browse_page()`, `load_page()`, `update_content()`

Out of scope:
- `load_page()` extraction — owns Phase A fetch, storage persistence, URL sync, generation guard, immediate render path (browse equivalent of execute_search staying in search.py)
- `update_content()` extraction (2,836 lines) — too intertwined with live UI construction, reading-desk state, image-source switching, edit mode, metadata/header rendering, page navigation
- Any behavior change, styling tweak, or feature addition

</domain>

<decisions>
## Implementation Decisions

### Extraction Strategy
- **D-01:** Same pattern as Phase 72 — extract functions taking explicit state + refs parameters instead of closure capture. Module-level functions, not class refactor.
- **D-02:** `BrowsePageRefs` (or `BrowseEnrichmentContext`) dataclass captures UI refs and callbacks that enrichment functions need: `content_container`, `slider_refs`, `enrichment_refs`, `_load_generation`, `_page_client`, and page callbacks like `enter_joined_view` / navigation handlers used indirectly through placeholder updates. Defined in `browse_enrichment.py` (not browse_state.py, since these are enrichment-specific).

### Module Boundaries
- **D-03:** `browse_state.py`: BrowseState class + small state/reset helpers. BrowseState is already a clean class (63 lines, ~30 fields). Pure data, no UI dependencies.
- **D-04:** `browse_enrichment.py`: `_load_enrichment()` (~300 lines core logic), `_update_enrichment_sections()`, `_populate_bib_catalog_buttons()`, any enrichment fetch helpers, plus the `BrowsePageRefs` dataclass. These functions fetch crossref data, PGP metadata, bibliography, catalog, measurements in parallel deferred loads.
- **D-05:** `browse.py` retains: `create_browse_page()` entry point, `load_page()` (Phase A fetch + generation guards), `update_content()` (2,836-line UI renderer), all navigation/action functions, image controls, edit/corrections. `load_page()` calls the enrichment module for Phase B deferred loading.

### Operational Refs Placement
- **D-06:** Page-local operational refs (`content_container`, `slider_refs`, `enrichment_refs`, `_load_generation`, `_page_client`, `_url_state`, `show_metadata`) do NOT belong in BrowseState — they're ephemeral UI references, not persistent state. They go into `BrowsePageRefs` so the enrichment functions can access them explicitly.

### Verification
- **D-07:** pytest baseline must remain green (no regression).
- **D-08:** Import smoke: `python -c "from web.pages.browse_state import BrowseState; from web.pages.browse_enrichment import BrowsePageRefs"` — succeeds.
- **D-09:** Web smoke test: launch web app, browse a manuscript, verify enrichment loads (bibliography, catalog, measurements panels populate), navigate pages, toggle metadata. No regression.
- **D-10:** CI green (Ubuntu + Windows matrix).
- **D-11:** Wave 1 checkpoint after state extraction (same pattern as Phase 72 Codex revision — verify page load/session restore before enrichment extraction).

### Claude's Discretion
- Exact BrowsePageRefs fields — derived from closure dependency analysis of enrichment functions.
- Which small helpers move to browse_state.py vs. stay in browse.py.
- Commit granularity.
- Whether enrichment functions use thin wrappers in browse.py or direct imports with explicit params at call sites (fewer call sites than search, so direct might be cleaner).

### Folded Todos
None.

</decisions>

<canonical_refs>
## Canonical References

### Roadmap & Requirements
- `.planning/ROADMAP.md` — Phase 73 entry
- `.planning/REQUIREMENTS.md` — WEBM-02
- `.planning/PROJECT.md` — v7.9 Active milestone

### Source
- `web/pages/browse.py` (~5,076 lines) — full file is in scope for splitting
  - Lines 479-541: BrowseState class
  - Lines 747-902: load_page() (stays in browse.py per D-05)
  - Lines 903-1380: _load_enrichment() + enrichment helpers (extraction target)
  - Lines 2083-4918: update_content() (stays in browse.py per D-05)

### Prior Phase Pattern
- `.planning/phases/72-search-page-split/72-CONTEXT.md` — SearchPageRefs pattern, thin wrappers
- `.planning/phases/72-search-page-split/72-RESEARCH.md` — closure dependency mapping approach

### CI & Verification
- `.github/workflows/ci.yml` — Ubuntu + Windows matrix
- `tests/` — current baseline must remain green

</canonical_refs>

<code_context>
## Existing Code Insights

### BrowseState Already Exists
BrowseState (lines 479-541) is a class defined INSIDE `create_browse_page()` with ~30 fields covering: shelfmark query, current page, zoom/rotation, edit state, PGP data, joined view, reading desk entries, enrichment crossref data. Clean extraction candidate.

### Enrichment Flow
`_load_enrichment(page, generation)` is called from `load_page()` as a deferred Phase B operation. It:
- Fetches crossref metadata (NLI images, Cambridge manifests, Manchester)
- Loads PGP document data (transcription, metadata, sources)
- Fetches bibliography and catalog references
- Fetches measurements data
- Populates BrowseState fields with results
- Calls `_update_enrichment_sections()` to patch UI containers

### Closure Variables in Enrichment
Enrichment functions access from closure: `state` (BrowseState), `content_container`, `enrichment_refs` (dict of UI element references), `_load_generation` (generation counter for stale rejection), various UI containers for bib/catalog buttons. These map to BrowsePageRefs fields.

</code_context>

<deferred>
## Deferred Ideas

### For a Future Phase
- **update_content() extraction** — 2,836 lines of UI construction. If browse.py is still too large after Phase 73, this is the next target. Would need a `BrowseRenderContext` similar to SearchPageRefs but much larger.
- **load_page() extraction** — only if browse.py needs further decomposition.

</deferred>

---

*Phase: 73-browse-page-split*
*Context gathered: 2026-04-16*
