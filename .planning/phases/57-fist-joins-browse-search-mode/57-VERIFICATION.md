---
phase: 57-fist-joins-browse-search-mode
verified: 2026-03-30T21:18:25Z
status: passed
score: 11/11 must-haves verified
resolved_gaps:
  - truth: "Browse suggestions mode"
    status: resolved
    resolution: "The enriched VS dialog (side-by-side layout with images, text, expand) IS the browse view. Design decision to make the dialog richer instead of navigating to search with wildcard. Dead code (vs_browse_mode, _browse_visual_suggestions) retained as fallback for URL-based access. User confirmed this UX is better."

  - truth: "Web settings download button"
    status: resolved
    resolution: "Added download button to web/pages/settings.py (commit 3ccfd21a) pointing to /api/visual_similarity_db."

  - truth: "D-10 cross-cutting: Advanced View and List items"
    status: accepted
    resolution: "Deferred in Plan 03 SUMMARY as out-of-scope. ResultDialog VS button is wired and working. List items and Advanced View would require context menu architecture changes beyond phase scope."
human_verification:
  - test: "Open browse page for a manuscript with VS data, click 'Visual Similarity (N)' chip"
    expected: "Orange-themed dialog opens with ranked suggestions, sort/filter controls, Browse and Puzzle action buttons per row"
    why_human: "Cannot verify NiceGUI dialog rendering programmatically"
  - test: "In VS dialog, click 'Search in visual suggestions' button"
    expected: "Navigates to /search page with orange VS restriction breadcrumb active and 'N manuscripts' count shown"
    why_human: "Cannot verify NiceGUI navigation and URL param handling programmatically"
  - test: "Run a text search; for results with VS data, click the orange compare icon"
    expected: "Expandable panel shows top 3 partner shelfmarks with rank badges (orange, JOIN-03)"
    why_human: "Requires rendered search results to test"
  - test: "Desktop: navigate to browse, click Visual Similarity button (may be hidden if VS DB not loaded)"
    expected: "Dialog opens with ranked suggestions; second click loads instantly from cache"
    why_human: "Requires running desktop app and VS sidecar"
  - test: "Desktop: click 'Search in visual suggestions' from VS dialog"
    expected: "Switch to search tab with orange VS breadcrumb and partner count"
    why_human: "Requires running desktop app"
---

# Phase 57: FIST Joins Browse & Search Mode Verification Report

**Phase Goal:** Researchers can discover visual similarity suggestions from FJMS SVM image analysis while browsing, and use those suggestions to restrict text searches via union/intersection of partner pools
**Verified:** 2026-03-30T21:18:25Z
**Status:** gaps_found
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | VisualSimilarityService returns ranked suggestions with data | VERIFIED | 14 passing tests; service at shared/visual_similarity_service.py line 97 |
| 2 | VisualSimilarityService returns empty list for manuscripts without data | VERIFIED | test_get_suggestions_empty passes |
| 3 | get_suggestion_partners returns union/intersection of partner sys_ids | VERIFIED | test_get_suggestion_partners_union/intersection pass |
| 4 | API endpoints return frozen-contract JSON | VERIFIED | web/api.py lines 1551-1650 with FROZEN CONTRACT comments |
| 5 | Import script produces visual_similarity.db with SQL-level optimization | VERIFIED | ATTACH DATABASE + PRAGMA synchronous=OFF in scripts/import_visual_similarity.py |
| 6 | User can see VS suggestions in browse dialog with ranked partners and action buttons (web) | VERIFIED | chip in browse.py line 1350-1356, dialog at web/components/visual_similarity_dialog.py |
| 7 | User can trigger 'Search in visual suggestions' from browse to restrict text search (web) | VERIFIED | VS dialog bottom bar navigates to /search?vs_src={sys_id}; search.py reads vs_src and activates restriction |
| 8 | User can trigger 'Browse suggestions' to view pool as result set without text query | FAILED | No UI path triggers vs_browse=1 in web; _browse_visual_suggestions defined but uncalled in desktop |
| 9 | Desktop fetches on-demand from server and caches with version tracking | VERIFIED | DesktopVSCache + VSFetchThread at genizah_app.py lines 12529-12640 |
| 10 | JOIN-03: Search results show orange compare icon with expandable partner list | VERIFIED | web/pages/search.py lines 4874-4907, toggle_vs_partners function |
| 11 | Settings page offers full VS DB download with robustness (both apps) | PARTIAL | Desktop: VSDownloadThread with SHA256+disk_usage+integrity_check. Web: only status badge, no download button. |

**Score:** 8/11 truths verified (2 failed/partial)

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/visual_similarity_service.py` | VisualSimilarityService with all service methods | VERIFIED | 301 lines, all 7 methods + singleton |
| `scripts/import_visual_similarity.py` | Import from FIST.db with SQL optimization | VERIFIED | 170 lines, ATTACH + PRAGMA tuning |
| `tests/test_visual_similarity.py` | 14+ unit tests | VERIFIED | 256 lines, 14 tests all passing |
| `web/api.py` | 3 frozen-contract endpoints + download endpoint | VERIFIED | All 4 endpoints present |
| `web/components/visual_similarity_dialog.py` | VS dialog with orange theme, sort/filter, actions | VERIFIED | 525 lines, full implementation |
| `web/pages/browse.py` | VS chip in enrichment toolbar | VERIFIED | chip at line 1348-1356 |
| `web/pages/search.py` | VS state fields, URL param handling, batch enrichment, JOIN-03 | VERIFIED | All fields in SearchUIState; vs_src handling; _toggle_vs_partners |
| `genizah_app.py` | DesktopVSCache, VSFetchThread, VS dialog, browse button, D-10 ResultDialog | VERIFIED | All present; _browse_visual_suggestions defined but unwired |
| `web/pages/settings.py` | VS database info or download UI | PARTIAL | Status badge present, no download button |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| shared/visual_similarity_service.py | fist_data/visual_similarity.db | ThreadLocalConnection | VERIFIED | `visual_similarity.db` in path resolution at line 25-63 |
| web/api.py | shared/visual_similarity_service.py | get_vs_service import | VERIFIED | `from shared.visual_similarity_service import get_vs_service` in all 3 endpoints |
| web/components/visual_similarity_dialog.py | shared/visual_similarity_service.py | get_vs_service import | VERIFIED | line 96-97 |
| web/pages/browse.py | web/components/visual_similarity_dialog.py | show_visual_similarity_dialog | VERIFIED | line 1350 |
| web/pages/search.py | shared/visual_similarity_service.py | batch_has_suggestions + get_suggestion_partners | VERIFIED | lines 4399-4404, 263-280 |
| web/pages/search.py | shared/refinement.py | compute_effective_restrict | VERIFIED | line 4066 |
| genizah_app.py | /api/visual_suggestions/{sys_id} | HTTP GET in VSFetchThread | VERIFIED | line 12638 |
| genizah_app.py | vs_cache.db | SQLite in DesktopVSCache | VERIFIED | _db_path = 'vs_cache.db' at line 12539 |
| genizah_app.py | /api/visual_suggestions/version | HTTP GET for cache version check | VERIFIED | line 12572 |
| genizah_app.py | _browse_visual_suggestions | Called from VS dialog | FAILED | Method defined at 14913 but zero callers found in codebase |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|--------------------|--------|
| web/components/visual_similarity_dialog.py | data (suggestions list) | run.io_bound(vs_service.get_suggestions, sys_id, 200) | Real if sidecar present, empty [] if sidecar absent | VERIFIED |
| web/pages/browse.py | has_visual_suggestions | vs_svc.has_suggestions(_page_sys_id) | Real query against visual_suggestions table | VERIFIED |
| web/pages/search.py | vs_availability | svc.batch_has_suggestions(sys_ids) | Real batch query | VERIFIED |
| web/pages/search.py | vs_restrict_sys_ids | get_suggestion_partners(source_ids) | Real query on URL param trigger | VERIFIED |
| genizah_app.py (DesktopVSCache) | suggestions | VSFetchThread -> server API or local DB | Server fetch + local cache | VERIFIED |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| 14 VS unit tests pass | py -m pytest tests/test_visual_similarity.py -q | 14 passed in 0.44s | PASS |
| Import script has correct optimizations | grep in scripts/import_visual_similarity.py | PRAGMA synchronous = OFF, ATTACH DATABASE, da.AlmaId != db.AlmaId | PASS |
| API endpoints present in api.py | grep visual_suggestions web/api.py | 4 endpoints found (version, batch_check, per-manuscript, download) | PASS |
| SearchUIState has VS fields | grep vs_restrict_sys_ids web/pages/search.py | Found at line 146 | PASS |
| DesktopVSCache class in genizah_app.py | grep class DesktopVSCache | Found at line 12529 | PASS |
| VSFetchThread class in genizah_app.py | grep class VSFetchThread | Found at line 12625 | PASS |
| _browse_visual_suggestions has callers | grep -n _browse_visual_suggestions | Only definition, 0 callers | FAIL |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| JOIN-01 | 57-01, 57-02, 57-03 | User can see FIST join group suggestions in browse enrichment alongside existing FJMS scientific joins (web + desktop) | SATISFIED | Web: VS chip + dialog in browse. Desktop: btn_b_visual_sim in browse toolbar |
| JOIN-02 | 57-01, 57-02, 57-03 | User can search within FIST join groups as a dedicated search mode (web + desktop) | SATISFIED | Web: VS dialog 'Search in visual suggestions' -> /search?vs_src. Desktop: _search_in_visual_suggestions via VS dialog |
| JOIN-03 | 57-02, 57-03 | Search results show join partners for matched fragments with visual distinction (web + desktop) | SATISFIED (web); PARTIAL (desktop) | Web: _toggle_vs_partners with orange compare icon in search.py. Desktop: ResultDialog has VS access but result-row partner display in search results is not confirmed for desktop |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| genizah_app.py | 14913 | `_browse_visual_suggestions` defined but never called | Warning | Browse suggestions mode unreachable in desktop; users cannot trigger it |
| web/components/visual_similarity_dialog.py | 341-345 | 'Search in visual suggestions' only (no 'Browse suggestions') | Warning | Browse mode unreachable in web without direct browse.py button |
| web/pages/settings.py | ~366 | VS status display only, no download button | Info | Users cannot download VS DB from web settings despite plan requiring it |

### Human Verification Required

#### 1. VS Browse Chip and Dialog Rendering (Web)

**Test:** Navigate to browse for any CUL manuscript (e.g., T-S 12.123), wait for enrichment to complete.
**Expected:** Orange "Visual Similarity (N)" chip appears in the enrichment toolbar. Clicking it opens a dialog with orange gradient header, "compare" icon, shelfmark in title, sort/filter dropdowns, ranked suggestion rows with rank badges, domain/library info, Browse/Puzzle/List/Join action buttons per row. Bottom bar has "Search in visual suggestions" button.
**Why human:** NiceGUI dialog rendering and visual appearance cannot be verified programmatically.

#### 2. "Search in Visual Suggestions" End-to-End (Web)

**Test:** Click the "Search in visual suggestions" button in the VS dialog.
**Expected:** Page navigates to /search with URL param vs_src={sys_id}. Orange restriction strip appears showing "Visual Similarity — {shelfmark}" with manuscript count. Typing a search query restricts results to the VS partner pool. Clear button removes the restriction.
**Why human:** URL navigation and NiceGUI restriction strip rendering require browser.

#### 3. JOIN-03 Partner Display in Search Results (Web)

**Test:** Run any text search. For result rows that have VS data (after batch enrichment), look for the orange compare icon.
**Expected:** Orange compare icon visible on rows with VS data. Clicking expands an inline panel showing "Visual similarity partners" label with top 3 partner shelfmarks linked to their browse pages.
**Why human:** Search result rendering requires running app and indexed data.

#### 4. Desktop VS Dialog (Desktop)

**Test:** Open desktop app, browse to a manuscript with VS data, click "Visual Similarity" button.
**Expected:** Dialog opens with ranked suggestions table (Rank/Shelfmark/Domain/Library columns), sort dropdown, Browse/Puzzle action buttons per row, "Search in visual suggestions" button in toolbar.
**Why human:** Requires running PyQt6 desktop application.

#### 5. Desktop Cache Version Check

**Test:** Open desktop app while server is accessible.
**Expected:** Background thread checks /api/visual_suggestions/version on startup. If server version differs from cached version, entire cache is invalidated (all cached_suggestions deleted).
**Why human:** Background thread behavior requires desktop app runtime.

#### 6. Desktop Full DB Download

**Test:** In desktop settings, locate VS download section and initiate download.
**Expected:** Progress bar shows percentage. On completion, VS service resets and local DB is available. On error, descriptive message with retry option.
**Why human:** Requires 600MB+ server download flow.

### Gaps Summary

Two substantive gaps were found:

**Gap 1: Browse suggestions mode is unreachable** (Truth 8). The VS dialog and browse toolbar were refactored post-plan to remove the "Browse suggestions" button. The infrastructure exists (vs_browse_mode in search.py, _browse_visual_suggestions in genizah_app.py) but there is no UI path to trigger it. The design intent was that the VS dialog "IS the browse view" (commit message 8197f3ef), but the browse mode that auto-triggers a wildcard search restricted to the partner pool — giving users the full result list without a text query — is currently a dead code path. This partially satisfies JOIN-02 (search within suggestions works) but the D-12 "browse without query" mode is inaccessible.

**Gap 2: Web settings download button absent**. The plan required a download button in web settings. The implementation shows only a status badge (pair_count/manuscript_count). The download API endpoint exists at /api/visual_similarity_db with Content-Length and X-Checksum-SHA256 headers, but web users have no UI to trigger it. This is a lower-severity gap since the web app runs on the server that has direct DB access.

**Non-gap deviations (documented in Summary, acceptable):** List items D-10 context was explicitly deferred as an architectural change. The Summary documents this honestly.

---

_Verified: 2026-03-30T21:18:25Z_
_Verifier: Claude (gsd-verifier)_
