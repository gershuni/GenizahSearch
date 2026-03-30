# Phase 57: FIST Visual Similarity Browse & Search - Context

**Gathered:** 2026-03-29
**Status:** Ready for planning

<domain>
## Phase Boundary

Researchers can discover visual similarity suggestions from FJMS's SVM-based image analysis while browsing, and can use those suggestions to restrict text searches. The data source is `Image_BestMarkForJoin` in FIST.db (~35.9M rows, ~15.5M unique scored pairs across ~155K documents). This is distinct from the existing scholarly joins already in the app (`joins` table in fjms_enrichment.db).

**Key distinction:** Scholarly joins (existing) = scholar-confirmed fragment relationships. Visual suggestions (this phase) = algorithmic SVM-scored image similarity pairs from FJMS visual analysis pipeline.

</domain>

<decisions>
## Implementation Decisions

### Data Storage & Import
- **D-01:** Server-only storage. The full visual suggestions dataset lives on the web server only. Desktop does NOT ship a local sidecar.
- **D-02:** On-demand per-manuscript fetch. When a user browses a manuscript, the app fetches that manuscript's visual suggestions from the server and caches them to local disk.
- **D-03:** Optional full DB download. A settings option allows power users to download the entire visual suggestions database for offline use.
- **D-04:** Import all scored pairs (MarkCode=NULL and 10318, deduplicated) from `Image_BestMarkForJoin`. Map DocumentID -> FGPImageNumberId -> InventoryId -> AlmaId via the chain: `Image_ImageDocument` -> `dbo_ImgDigitalImage` -> `dbo_InventoryAlma`.
- **D-05:** Future phases may use this data for line-based join filtering (cross-referencing visual similarity with other join heuristics). Design the storage to support this.

### Browse Enrichment Display
- **D-06:** Dedicated dialog (like Measurements/Bibliography). A "Visual Similarity" button in the browse toolbar opens a sortable dialog showing ranked suggestions with thumbnails, partner metadata (domain, library), and action buttons (Browse, Open in Puzzle).
- **D-07:** Top 20 suggestions shown by default, no score floor. Ranked by SVM score internally.
- **D-08:** No raw SVM score displayed to users. Suggestions shown as a ranked list (#1, #2, #3...) without any score indicator. Users trust the ordering.
- **D-09:** Dialog includes sorting and filtering controls (sort by rank is default; filter by library, domain).

### Search Integration — "Search in Visual Suggestions"
- **D-10:** Cross-cutting action available from multiple contexts: Browse, ResultDialog, Advanced View, List items, Search results. User can select one or more manuscripts and choose "Search in visual suggestions".
- **D-11:** Two combination modes when multiple manuscripts are selected: Union (combine all suggestion partners) or Intersection (only partners suggested for ALL selected manuscripts). User chooses via toggle.
- **D-12:** Two entry modes:
  - **Browse suggestions** — directly show the visual suggestion pool as a browsable result set (no text query needed)
  - **Search within suggestions** — restrict a text search to only the suggestion pool's sys_ids (reuses Phase 55 `restrict_sys_ids` mechanism)

### Labeling & User Expectations
- **D-13:** Label as "Visual Similarity" throughout the UI. Full description: "Visual similarity suggestions from FJMS image analysis". Neutral, accurate — does not imply these are confirmed joins.
- **D-14:** Clear visual distinction from scholarly joins (which use "Scientific Joins" / "Scholarly Joins" labeling in the existing UI).

### Claude's Discretion
- Fetch mechanism for desktop (HTTP API endpoint vs SQLite download — choose based on existing architecture patterns)
- Local disk cache format and eviction policy
- Exact dialog layout and component choices
- Button placement in browse toolbar
- How "Search in visual suggestions" is triggered from each context (button, menu item, right-click)
- Server-side DB format (separate SQLite sidecar vs table in fjms_enrichment.db)
- Import script design

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### FIST.db Source Data
- `fist_data/FIST.db` table `Image_BestMarkForJoin` — 35.9M rows, columns: BestMarkID, DocumentID_A, DocumentID_B, SVMMark, MarkCode
- `fist_data/FIST.db` table `Image_ImageDocument` — 228K rows, maps DocumentId -> FGPImageNumberIdRecto/Verso
- `fist_data/FIST.db` table `dbo_ImgDigitalImage` — 742K rows, maps FGPImageNumberId -> InventoryId
- `fist_data/FIST.db` table `dbo_InventoryAlma` — maps InventoryId -> AlmaId (our sys_id)

### Existing Joins Infrastructure
- `shared/fjms_service.py:2085` — `get_join_group()` method for scholarly joins (existing pattern)
- `web/components/joins_panel.py` — web joins panel (Supabase-backed community joins, NOT FIST visual)
- `genizah_app.py:13909` — `_browse_view_joins()` desktop dialog for scholarly joins

### Related Phase Patterns
- `.planning/phases/54-dimensions-display-filtering/54-CONTEXT.md` — measurements dialog pattern (D-06 follows this)
- `.planning/phases/55-search-within-results/` — `restrict_sys_ids` mechanism (D-12 reuses this)
- `.planning/phases/56-exclude-known-manuscripts/56-CONTEXT.md` — post-search filtering pattern

### Data Stats (verified 2026-03-29)
- ~15.5M unique scored pairs (MarkCode=NULL and 10318 are duplicates; 32318/33318 have score=0)
- ~155K documents with suggestions, each having ~100 ranked partners
- SVM scores range from -0.2 to 20.2 (higher = more visually similar)
- At SVMMark >= 3.0: ~2.87M unique pairs
- Chain verified: DocumentID -> FGP -> InventoryId -> AlmaId works

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `shared/fjms_service.py` FjmsService class — pattern for SQLite sidecar queries with thread-safe connections
- `shared/nli_crossref_service.py` — pattern for on-demand data fetch + local caching
- Phase 55 `restrict_sys_ids` in search — mechanism for restricting search to a set of sys_ids
- Measurements dialog (Phase 54) — UI pattern for dedicated enrichment dialogs in both apps
- `web/pages/browse.py` enrichment panel — where the Visual Similarity button would be placed
- `genizah_app.py` browse tab toolbar — desktop button placement pattern

### Established Patterns
- Enrichment dialogs: button in browse toolbar -> opens QDialog (desktop) / ui.dialog (web)
- Service layer: shared/*.py services with thread-safe SQLite, used by both web and desktop
- On-demand enrichment: browse loads core data first, enrichment fetched async in background
- Search restriction: `restrict_sys_ids` parameter threads through search pipeline

### Integration Points
- Browse enrichment panel (both apps) — new "Visual Similarity" button
- Search filter panel (both apps) — "Search in visual suggestions" action
- ResultDialog / Advanced View — action button/menu for "Search in visual suggestions"
- Lists page — action for list items
- Search results — action for selected results
- Settings page — full DB download toggle

</code_context>

<specifics>
## Specific Ideas

- User emphasized these are "suggestions" from FJMS visual algorithms, most are NOT actual joins
- Desktop app should not bloat — server-only storage with on-demand fetch is critical
- Future join-finding features (e.g., line-based matching) may cross-reference with this visual similarity data
- Power users want maximum data access — optional full download accommodates this
- The "Search in visual suggestions" feature with union/intersection modes is the core power feature of this phase

</specifics>

<deferred>
## Deferred Ideas

- Line-based join search that cross-references with visual similarity data (future phase)
- Thumbnail preview comparison view in the suggestions dialog (could be Phase 57 stretch or separate)
- Visual similarity score calibration or quality tiers (e.g., "high/medium/low confidence")

</deferred>

---

*Phase: 57-fist-joins-browse-search-mode*
*Context gathered: 2026-03-29*
