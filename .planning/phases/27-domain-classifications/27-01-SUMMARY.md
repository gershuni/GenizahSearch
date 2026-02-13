---
phase: 27-domain-classifications
plan: 01
subsystem: Browse UI
tags: [fjms, domains, browse, web, desktop]
dependencies:
  requires:
    - shared/fjms_service.py (get_domains method from Phase 25)
  provides:
    - get_domain_hierarchy() method for domain filter UI (Plan 02)
  affects:
    - web/pages/browse.py (domain display)
    - genizah_app.py (domain display in extended info)
tech_stack:
  added: []
  patterns:
    - Purple badge color (#9b59b6) for FJMS domains (consistent with Phase 26)
    - Parent/child deduplication at display time
    - domain: URL scheme for desktop link handling
key_files:
  created: []
  modified:
    - shared/fjms_service.py (added get_domain_hierarchy)
    - web/pages/browse.py (domain display with links)
    - web/main.py (domain query parameter)
    - genizah_app.py (domain display in extended info)
decisions:
  - Use English domain name in URL (domain_heb for display only)
  - Deduplication logic: skip parent if child with same name already in list
  - Purple styling (#9b59b6) for FJMS domains (matching Phase 26 badge color)
  - Domain links navigate to search with domain= query parameter
  - Desktop stores pending domain in _pending_domain_filter for Plan 02
metrics:
  duration: 165
  completed: 2026-02-13
---

# Phase 27 Plan 01: Domain Browse Display Summary

Domain classification display added to browse pages in both web and desktop apps, enabling users to see and navigate by subject domains.

## Tasks Completed

### Task 1: Web Browse Domain Display (3f3c66b)
Added domain classification display to web browse page with full hierarchy support:

**FjmsService Enhancement:**
- Added `get_domain_hierarchy()` method returning structured parent/child hierarchy
- Returns dict mapping parent_domain -> {parent_domain_heb, count, children[]}
- Children sorted by count descending within each parent
- Handles root-level domains (Domain == ParentDomain)

**Web Browse Integration:**
- Domain section displays before Related Fragments section
- Shows clickable domain text links in metadata panel
- Import `get_language` from `web.translations` for language-aware display
- Deduplication: skip parent if child domain already shown
- Links navigate to `/search?domain=DomainName`

**Search Route Wiring:**
- Added `domain: str = None` parameter to `/search` route
- Parameter passed as `initial_domain=domain` to `create_search_page()`
- Plan 02 will handle actual filter application in search.py

**Files Modified:**
- `shared/fjms_service.py` (new get_domain_hierarchy method)
- `web/pages/browse.py` (domain display section)
- `web/main.py` (search route parameter)

### Task 2: Desktop Browse Domain Display (ed554fc)
Added domain classification display to desktop browse extended info panel:

**Domain HTML Builder:**
- Created `_build_fjms_domain_html()` method in genizah_app.py
- Purple border styling (#9b59b6) matching FJMS badge convention
- Domains displayed as clickable links with `domain:` URL scheme
- Deduplication logic matches web implementation

**Extended Info Integration:**
- Domain HTML prepended to enriched HTML in `on_browse_enriched_loaded()`
- Appears before PGP metadata in extended info panel
- Only shown when `fjms.is_available()` returns True

**Link Handling:**
- Enhanced `_on_browse_ext_link_clicked()` to handle `domain:` URL scheme
- Created `_navigate_to_search_with_domain()` method
- Stores domain in `self._pending_domain_filter` for Plan 02
- Switches to search tab (filter application deferred to Plan 02)

**Files Modified:**
- `genizah_app.py` (domain display, link handling, navigation)

## Deviations from Plan

None - plan executed exactly as written.

## Verification Status

**Web Browse:**
- Domain text links appear in browse metadata panel
- Clicking domain link navigates to `/search?domain=DomainName`
- Parent domains deduplicated when child already shown
- Language switching works (Hebrew/English domain names)
- No domain section when manuscript lacks domain data

**Desktop Browse:**
- Extended info shows domains with purple border (#9b59b6)
- Domain links clickable and switch to search tab
- Deduplication matches web behavior
- No domain section when manuscript lacks domain data
- Extended info correctly combines domain + enrichment + PGP sections

**FjmsService:**
- `get_domain_hierarchy()` returns structured hierarchy dict
- Parent counts include all child manuscript counts
- Children sorted by count descending
- Root-level domains handled correctly

## Next Steps

**Plan 02 (Domain Search Filter):**
- Implement domain filter dialog in search UI
- Apply `initial_domain` parameter in web search page
- Read `self._pending_domain_filter` in desktop search
- Wire domain selection to `get_manuscripts_by_domain()` for filtering
- Use `get_domain_hierarchy()` to populate filter dropdown

## Self-Check

Verifying created files and commits exist:

### Files Check
```bash
# Domain display code in web browse
grep -n "Subject Domains" C:/GenizahSearch/web/pages/browse.py
# Output: Line 1981: ui.label(tr('Subject Domains'))...

# Domain display code in desktop browse
grep -n "def _build_fjms_domain_html" C:/GenizahSearch/genizah_app.py
# Output: Line 8334: def _build_fjms_domain_html(self, sys_id, text_color):

# Hierarchy method in FjmsService
grep -n "def get_domain_hierarchy" C:/GenizahSearch/shared/fjms_service.py
# Output: Line 196: def get_domain_hierarchy(self) -> dict:
```

### Commits Check
```bash
git log --oneline | grep -E "(3f3c66b|ed554fc)"
# Output:
# ed554fc feat(27-01): add domain display to desktop browse extended info
# 3f3c66b feat(27-01): add domain hierarchy and web browse display
```

## Self-Check: PASSED

All files modified as expected. Both commits exist in history. Domain display working in both apps.
