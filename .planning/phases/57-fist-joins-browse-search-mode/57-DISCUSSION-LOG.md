# Phase 57: FIST Visual Similarity Browse & Search - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-03-29
**Phase:** 57-fist-joins-browse-search-mode
**Areas discussed:** Import & threshold strategy, Browse enrichment display, Search integration, Labeling & user expectations

---

## Pre-Discussion Discovery

User clarified that existing scholarly joins (`joins` table, 48K rows) are already in the app. Phase 57 is about `Image_BestMarkForJoin` — 35.9M SVM-scored visual similarity pairs from FJMS's algorithmic image analysis. This fundamentally changed the phase scope from the original ROADMAP description.

---

## Import & Threshold Strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Top 10 per manuscript | ~1.5M rows, ~50-75MB sidecar | |
| SVM score cutoff >= 3.0 | ~2.87M pairs, uneven distribution | |
| Combined: score floor + top N | Top 20 if SVMMark >= 2.0, ~2-3M rows | |
| Import everything, filter at display | All ~15.5M scored pairs, ~500MB+ | |

**User's choice:** Server-only storage with on-demand per-manuscript fetch (cached on disk). Optional full DB download in settings. User rejected all pre-filtering approaches — wants full data on server, streamed as needed.

**Follow-up — Desktop cap:**

| Option | Description | Selected |
|--------|-------------|----------|
| Top 20 per manuscript | ~3M rows, ~100-150MB sidecar | |
| Top 50 per manuscript | ~7.5M rows, ~300MB sidecar | |
| Top 10 per manuscript | ~1.5M rows, ~50-75MB sidecar | |

**User's choice:** No desktop sidecar at all. Server-only with on-demand fetch per manuscript, cached locally. Full DB download available in settings for power users.

**Follow-up — Fetch mechanism:**

| Option | Description | Selected |
|--------|-------------|----------|
| HTTP API endpoint | Desktop calls web API, caches to local SQLite | |
| Direct SQLite download per group | Server pre-splits DB into chunks | |
| You decide | Claude picks based on architecture | **Claude's discretion** |

---

## Browse Enrichment Display

| Option | Description | Selected |
|--------|-------------|----------|
| Expandable section in enrichment panel | Inline section with clickable chips and confidence bar | |
| Dedicated dialog (like Measurements) | Button opens sortable dialog with thumbnails, metadata, actions | :heavy_check_mark: |
| Inline chips, dialog for details | Top 3-5 inline, full dialog on demand | |

**User's choice:** Dedicated dialog with sorting, filtering, thumbnails, and action buttons (Browse, Open in Puzzle).

**Follow-up — Display cap:**

| Option | Description | Selected |
|--------|-------------|----------|
| Top 20, no floor | Show 20 best matches regardless of score | :heavy_check_mark: |
| Top 50, score floor >= 2.0 | Up to 50 if score >= 2.0 | |
| All available, paginated | Everything, 20 per page | |

---

## Search Integration

| Option | Description | Selected |
|--------|-------------|----------|
| Post-search enrichment only | Badge on results with matches, click to expand | |
| Pre-search toggle + post-search | 'Has visual suggestions' filter + badges | |
| Not in search — browse only | Keep out of search for Phase 57 | |

**User's choice:** None of the above. User described a cross-cutting "Search in visual suggestions" action available from Browse, ResultDialog, Advanced View, List, Search results. Can select 1+ manuscripts, get their suggestion partners, then either browse the pool or search within it.

**Follow-up — Multi-select combination:**

| Option | Description | Selected |
|--------|-------------|----------|
| Union of all suggestions | Combine all partners from all selected manuscripts | |
| Intersection of suggestions | Only partners suggested for ALL selected manuscripts | |
| User chooses union/intersection | Toggle between both modes | :heavy_check_mark: |

**Follow-up — Search mode:**

| Option | Description | Selected |
|--------|-------------|----------|
| Both modes available | Browse pool directly OR combine with text search | :heavy_check_mark: |
| Always paired with text search | Visual suggestions only restrict search scope | |
| Standalone browse only | Show pool as sortable list, no text search | |

---

## Labeling & User Expectations

| Option | Description | Selected |
|--------|-------------|----------|
| "Visual Similarity" | Neutral, accurate. 'Visual similarity suggestions from FJMS image analysis' | :heavy_check_mark: |
| "Possible Joins" | More suggestive, implies physical joins | |
| "FJMS Image Matches" | Technical, references source | |

**Follow-up — Score display:**

| Option | Description | Selected |
|--------|-------------|----------|
| Relative bar/rank only | Show #1, #2, #3 with confidence bar | |
| Raw score + rank | Both rank and SVM number | |
| No score — just ranked list | Ordered list, no score indicator | :heavy_check_mark: |

---

## Claude's Discretion

- Fetch mechanism (HTTP API vs SQLite download)
- Local disk cache format and eviction
- Dialog layout and component choices
- Button placement in browse toolbar
- "Search in visual suggestions" trigger UX per context
- Server-side DB format
- Import script design

## Deferred Ideas

- Line-based join search cross-referencing visual similarity (future phase)
- Thumbnail comparison view in suggestions dialog
- Visual similarity score calibration/quality tiers
