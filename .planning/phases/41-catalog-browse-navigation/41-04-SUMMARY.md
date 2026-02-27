---
phase: 41-catalog-browse-navigation
plan: 04
status: completed
duration: ~45min (across multiple sessions)
commits: [pending]
---

## What Was Built

### Cross-links (from 41-04-PLAN.md Task 1)
- Bidirectional cross-links between manuscript browse and catalog browse in both apps
- Domain/author labels on browse pages are clickable links to catalog browse filtered by that value
- Web: `ui.link` to `/catalog-browse?domain=X` or `?author=X`
- Desktop: `_navigate_to_catalog_browse()` switches tab with filter pre-set

### UAT Fixes & Enhancements (post-plan work)
- **FIST v5.0.0 enrichment**: 3 new tables (genizah_persons 2,286, genizah_titles 775, code_values 3,440) + 20 new catalog columns
- Browse authors: 801 (was 204), works: 663, Rambam = 5,369 manuscripts
- **Free text filter** in catalog browse (both apps): FTS5-based search with ALL/ANY/NOT modes
- **FTS5 + domain LIKE hybrid**: domain name search via UNION query (5,305 results for "פילוסופיה", was 0)
- **Chip remove fix**: replaced q-chip `remove` event with button-style `on_click` to avoid NiceGUI slot destruction
- **Export script** (scripts/export_fist_enrichment.py): v5.0.0 with structured author/work FK path
- **Service layer** (shared/fjms_service.py): v5/legacy dual-path for browse authors/works, text filter with FTS5+domain hybrid
- **Web UI** (web/pages/catalog_browse.py): text filter card, mode dropdown, chips, summary, sessionStorage, deep links
- **Desktop UI** (genizah_app.py): text filter section, QComboBox + QLineEdit, chips, summary, scrollable panel
- **Translations** (genizah_translations.py): 15 new Hebrew entries
- **Tests** (tests/test_fjms_service.py): updated to new dict key format (eng_desc/org_title)

## Decisions

| Decision | Rationale |
|----------|-----------|
| Button-style chips instead of q-chip remove | Avoids NiceGUI slot destruction when container is cleared during handler |
| FTS5 + domain LIKE hybrid | FTS5 index covers catalog text fields; domain names live in separate table |
| Structured FK path for v5 authors/works | genizah_persons/titles tables provide Hebrew names and structured IDs |
| Graceful v4 fallback via `_has_persons_titles` | Supports pre-v5 sidecars without code changes |

## Metrics

- Authors: 801 (was 204 with sparse AuthorText)
- Works: 663 (structured via genizah_titles)
- Text filter "פילוסופיה": 5,305 results (FTS5+domain hybrid)
- Tests: 72 passed, 0 failed

## Files Modified

- `shared/fjms_service.py` — text filter, v5 browse authors/works, FTS5+domain hybrid
- `web/pages/catalog_browse.py` — text filter UI, button-style chips, deep links
- `web/main.py` — route params for text filter
- `web/static/common.css` — catalog browse table CSS
- `genizah_app.py` — desktop text filter UI, scrollable panel, v5 author/work display
- `genizah_translations.py` — 15 new Hebrew translations
- `scripts/export_fist_enrichment.py` — v5.0.0 with 3 new tables + 20 catalog columns
- `tests/test_fjms_service.py` — updated dict keys for v5 return format
