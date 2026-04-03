# Phase 55 UX Revision Summary: Search-Within-Results Clarity & Same-Page Filter

**Date completed:** 2026-03-29
**Status:** Complete
**Scope:** Both web (NiceGUI) and desktop (PyQt6)

## One-liner

Manuscript-count labels, "Only results with all terms" page-level post-filter, and chain-aware snippet highlighting for search-within-results UX clarity

## What Changed

### A. Label Changes (both apps)
- Button: "Search within N manuscripts" (was "Search within N results")
- Badge: "Searching within N manuscripts" (was "Refining within N results")
- Counts now reflect unique manuscript (sys_id) count, not page count

### B. "Only results with all terms" Checkbox (both apps)
- Opt-in page-level post-filter on refinement breadcrumb strip
- Computes uid intersection across all text-search steps in the chain
- Works cross-mode (text/Responsa/Title/Shelfmark)
- Hidden for single-step chains; state persisted in session
- Web: `search.py` checkbox with `_toggle_all_terms_filter()`
- Desktop: `genizah_app.py` QCheckBox with `_apply_all_terms_filter_and_rerender()`

### C. Snippet Highlighting
- `enrich_snippet_with_chain_terms()` in `shared/refinement.py` marks earlier chain terms in snippets
- Helps users see where prior terms appear even in manuscript-level mode

## Files Modified
- `shared/refinement.py` — `compute_all_terms_filter()`, `enrich_snippet_with_chain_terms()`
- `web/pages/search.py` — label text, checkbox widget, post-filter logic
- `genizah_app.py` — same label/checkbox/filter changes for desktop
- `genizah_translations.py` — translation strings for new UI elements

## Shipped
Released as part of v7.4.0 (March 2026).
