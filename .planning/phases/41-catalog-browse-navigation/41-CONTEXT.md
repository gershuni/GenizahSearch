---
phase: 41
name: Catalog Browse & Navigation
created: 2026-02-26
status: approved
---

# Phase 41: Catalog Browse & Navigation — Context

## Phase Goal
Researchers can explore the manuscript corpus through structured scholarly categories — browsing by domain hierarchy, author, or work title — and combine these axes to narrow results. Both web and desktop apps.

## Decisions

### 1. Hierarchy Navigation

- **Pattern**: Collapsible tree view sidebar (like a file explorer)
- **Counts**: Show manuscript counts next to each tree node, including children (e.g., "Torah (1,245)")
- **Depth**: All levels browsable — full FJMS domain hierarchy, no artificial depth cap
- **Click behavior**: Claude decides — when a node click triggers manuscript display (any level vs. leaf-only), determine based on data distribution and UX

### 2. Browse Layout & Results

- **Result display**: Claude decides — table rows vs. card grid, based on existing app patterns and data density
- **Metadata per result**: Full scholarly context — shelfmark, library, domain, identification (author/work), date attribution, and description snippet
- **Click action**: Both expand inline (quick preview) AND link to full manuscript browse page
- **Pagination**: Claude decides — paginated vs. virtual scroll, based on existing app patterns and performance

### 3. Multi-Axis Filtering

- **Combination model**: Independent filters — three separate controls (domain tree, author search, work search). Results are the intersection. Each usable alone or together.
- **Cross-filtering**: Dynamic — selecting a domain narrows available authors/works to only relevant options. Counts update.
- **Author/work input**: Search-as-you-type with partial matching and Hebrew input support
- **Active filters display**: Removable chip bar above results (e.g., "Domain: Bible x | Author: Maimonides x"). Click X to remove. Clear all button.

### 4. Entry Point & Placement

- **Web app**: Two separate sidebar navigation entries:
  - "Browse by Shelfmark" (renamed from current "Browse") — existing manuscript browse
  - "Browse by Identification" (new) — catalog browse page
- **Desktop app**: Two separate top-level tabs:
  - "Browse by Shelfmark" (renamed from current "Browse Manuscript")
  - "Browse by Identification" (new tab)
- **Tab name**: "Browse by Identification" (user-specified)
- **Default state**: Claude decides — whether to show domain tree expanded or empty prompt on first open
- **Deep linking (web)**: Yes — URL reflects browse state (domain selection, filters, page). Users can share/bookmark specific views.
- **Cross-links**: Bidirectional — domain/author/work labels on the manuscript browse page become clickable links that open catalog browse filtered to that value

### 5. Language & Data Coverage

- **Language in tree/filters**: Follow app language setting. Hebrew interface shows Hebrew names; English interface shows English names.
- **Data coverage**: Include an "Unclassified" bucket for manuscripts without FJMS domain/author/work data. Full corpus browsable.
- **Catalog integration**: Navigating to a manuscript from catalog browse opens the standard browse view (same behavior as direct browse). No special treatment.

## Deferred Ideas

(none captured)

## Requirements Mapped

| Requirement | Decision Coverage |
|-------------|-------------------|
| BROWSE-01 (domain hierarchy) | Sections 1, 5 |
| BROWSE-02 (browse by author) | Section 3 |
| BROWSE-03 (browse by work/title) | Section 3 |
| BROWSE-04 (combine axes) | Section 3 |
| BROWSE-05 (result metadata) | Section 2 |
| BROWSE-06 (both apps) | Section 4 |
