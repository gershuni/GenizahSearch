---
phase: 39-bug-fixing-cleanup-performance-improving
verified: 2026-02-20T12:00:00Z
status: gaps_found
score: 25/26 must-haves verified
re_verification:
  previous_status: passed
  previous_score: 27/27
  note: "Previous VERIFICATION.md covered plans 01-07. Plan 39-08 was executed after that document was written in response to RETEST-UAT finding that page navigation remained slow. This re-verification covers all 8 plans and found a NameError bug introduced by plan 39-08."
  gaps_closed:
    - "Bottom pagination scroll-to-top RuntimeError (now uses results_container.run_method after content is built)"
    - "VRD mouse wheel zoom-instead-of-scroll (Ctrl modifier check added)"
    - "E2E tests crash on missing selenium (importorskip guards added)"
    - "Slow page navigation (CSS extracted to cacheable static file, login dialog lazy-built)"
    - "Browse page loads FJMS metadata in parallel (pre-fetch in load_page, state.fjms_data pattern)"
    - "Discoveries page loads stats+feed concurrently (asyncio.gather + run.io_bound)"
  gaps_remaining:
    - "NameError in search.py line 2118: result_sys_ids undefined after plan 39-08 consolidation"
  regressions:
    - "Plan 39-08 Task 1 consolidated result_sys_ids into all_sys_ids but left one reference at line 2118 unchanged; every search execution fails with NameError before results are rendered"
gaps:
  - truth: "Search post-processing (domains, transcriptions, catalog counts) runs in parallel, not sequentially"
    status: failed
    reason: "asyncio.gather is correctly added at line 2077, but line 2118 references result_sys_ids which was removed when consolidating into all_sys_ids. Every search execution crashes at this line with NameError: name 'result_sys_ids' is not defined."
    artifacts:
      - path: "web/pages/search.py"
        issue: "Line 2118: `if sid in set(result_sys_ids)` — result_sys_ids is not defined in execute_search() scope; plan 39-08 renamed this variable to all_sys_ids (line 2064) but did not update line 2118"
    missing:
      - "Change result_sys_ids to all_sys_ids on line 2118 of web/pages/search.py"
---

# Phase 39: Bug Fixing, Cleanup, Performance Improving — Verification Report

**Phase Goal:** Stabilize and polish the app: fix all desktop crashes, add server-side pagination, integrate PostHog analytics, optimize web performance, and add Playwright E2E + performance tests
**Verified:** 2026-02-20
**Status:** GAPS FOUND
**Re-verification:** Yes — after plan 39-08 (page navigation parallelization) was executed post-previous-verification

---

## Scope of This Verification

The previous VERIFICATION.md covered plans 01-07. Plan 39-08 was planned and executed after that document was written, in response to a RETEST-UAT finding that page navigation remained slow. This re-verification covers all 8 plans with full spot-check verification of plan 39-08 and regression checks on plans 01-07.

---

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Desktop app no longer crashes on destroyed Reading Desk scrollbars | VERIFIED | `sip.isdeleted(text_bar) or sip.isdeleted(image_bar)` guards at genizah_app.py lines 10990, 11005 |
| 2 | Desktop app no longer crashes when ZoomableScrollArea destroyed during async image load | VERIFIED | `sip.isdeleted(self._msg_item) or sip.isdeleted(self._pixmap_item)` guards at lines 1423, 1449, 1457 |
| 3 | No unsafe bracket uid access remains in genizah_app.py | VERIFIED | All uid accesses use `.get('uid')` safe pattern; no direct `res['uid']` bracket access |
| 4 | Search results display 50 per page with pagination controls | VERIFIED | `PAGE_SIZE = 50` at line 37; `ui.pagination` at lines 2249 and 2263; `[:1000]` storage cap at line 2136; zero `[:200]` occurrences |
| 5 | User can navigate between result pages using pagination controls | VERIFIED | `ui.pagination(1, total_pages, ...)` at top (line 2249) and bottom (line 2263) with `max-pages=7 boundary-numbers` |
| 6 | Total result count shown accurately regardless of page | VERIFIED | `results_count.text` updated at all filter and search call sites |
| 7 | Result numbering globally correct across pages (page 2 starts at #51) | VERIFIED | `create_result_card(start + i, res)` where `start = page_idx * PAGE_SIZE` (lines 2237, 2254) |
| 8 | Filters work against full result set, then paginate | VERIFIED | `render_results(filtered, page=0)` at apply_filters (lines 1935, 2203, 2205) |
| 9 | Bottom pagination scroll-to-top does not throw RuntimeError | VERIFIED | `results_container.run_method('setScrollPosition', 'vertical', 0)` at line 2268, placed after the `with results_container:` block exits — operates on the live container, not a destroyed parent slot |
| 10 | PostHog JS snippet loads on every page when POSTHOG_API_KEY env var is set | VERIFIED | `ui.add_head_html(POSTHOG_SCRIPT)` in all 14 page handlers in web/main.py (lines 577, 597, 616, 630, 644, 665, 679, 693, 707, 721, 735, 749, 776, 791) |
| 11 | PostHog does NOT load when POSTHOG_API_KEY is not set | VERIFIED | `POSTHOG_SCRIPT = f'...' if _posthog_key else ''` at line 104 — empty string when env var absent |
| 12 | PostHog autocaptures page views, clicks, enables session recordings with masked inputs | VERIFIED | `autocapture: true`, `capture_pageview: true`, `maskAllInputs: true`, `maskTextSelector: 'input, textarea'` in snippet (lines 88-104) |
| 13 | Domain filter dialog opens instantly on second+ access (hierarchy cached) | VERIFIED | Double-checked locking: fast path at line 569; `with self._hierarchy_lock:` at line 576; cache assigned at line 684 |
| 14 | Mouse wheel scrolls VRD view; Ctrl+wheel zooms | VERIFIED | `event.modifiers() & Qt.KeyboardModifier.ControlModifier` at line 1478; plain wheel calls `event.ignore()` at line 1487 |
| 15 | E2E tests skip gracefully when selenium is not installed | VERIFIED | `pytest.importorskip("selenium", ...)` at module level: test_search_flow.py line 17, test_browse_flow.py line 14, test_performance.py line 15 |
| 16 | E2E tests cover search happy path | VERIFIED | 6 tests in test_search_flow.py (179 lines): TestSearchPageLoads + TestSearchExecution |
| 17 | E2E tests cover browse happy path | VERIFIED | 5 tests in test_browse_flow.py (121 lines): TestBrowsePageLoads + TestBrowseNavigation |
| 18 | E2E performance tests cover large result sets and page load times | VERIFIED | 5 tests in test_performance.py (191 lines): TestSearchPerformance + TestPageLoadPerformance |
| 19 | COMMON_STYLES CSS served as browser-cacheable static file | VERIFIED | `COMMON_STYLES = '<link rel="stylesheet" href="/static/common.css">'` at web/main.py line 110; web/static/common.css exists at 1,347 lines of pure CSS |
| 20 | No inline CSS injected per-page | VERIFIED | All `ui.add_head_html(COMMON_STYLES)` calls inject 49-char link tag; no per-page inline `<style>` block |
| 21 | Login dialog built lazily only when Login/Register clicked | VERIFIED | `_ensure_dialog()` with `nonlocal dialog` at web/auth_state.py lines 414-428; `dialog = None` init; `create_login_dialog()` called only inside `_ensure_dialog()` |
| 22 | Browse page loads FJMS metadata in load_page (not serially in update_content) | VERIFIED | `state.fjms_data = {'catalog_records':..., 'domains':..., 'bibliography':..., 'source_names':..., 'catalog_refs':...}` at browse.py lines 966-977; `update_content()` reads `fjms_data = state.fjms_data or {}` at line 2063 — all 5 direct service calls replaced |
| 23 | Discoveries page loads stats and feed concurrently | VERIFIED | `asyncio.gather(run.io_bound(_fetch_stats), run.io_bound(_fetch_feed))` at discoveries.py lines 289-292; pure-UI helpers at lines 307, 325; `ui.timer(0.1, initial_load, once=True)` at line 304 |
| 24 | All three plan 39-08 files have no syntax errors | VERIFIED | `ast.parse()` confirms search.py, browse.py, and discoveries.py all parse cleanly |
| 25 | Search.py, browse.py, and discoveries.py use asyncio import | VERIFIED | `import asyncio` at search.py line 26; `import asyncio` at discoveries.py (confirmed via run.io_bound + asyncio.gather usage) |
| 26 | Search post-processing runs domains, transcriptions, and catalog counts in parallel via asyncio.gather | FAILED | `asyncio.gather` added at line 2077 is correct, but **line 2118 references `result_sys_ids` which is not defined in `execute_search()` scope** — plan 39-08 consolidated this to `all_sys_ids` (line 2064) but left line 2118 unchanged; every search execution raises `NameError: name 'result_sys_ids' is not defined` before results are rendered |

**Score:** 25/26 truths verified

---

## Required Artifacts

### Plans 01-07 (spot-checked, no regressions found)

| Artifact | Status | Evidence |
|----------|--------|---------|
| `genizah_app.py` — sip.isdeleted guards (5 locations) | VERIFIED | Lines 1423, 1449, 1457, 10990, 11005 confirmed present |
| `web/pages/search.py` — pagination (PAGE_SIZE, ui.pagination, [:1000]) | VERIFIED | Lines 37, 2249, 2263, 2136 confirmed |
| `web/main.py` — PostHog snippet (14 pages, conditional) | VERIFIED | Lines 87-104, all 14 add_head_html calls confirmed |
| `shared/fjms_service.py` — thread-safe hierarchy cache | VERIFIED | Lines 355-356, 569-684 confirmed |
| `web/static/common.css` — 1,347 lines of pure CSS | VERIFIED | File exists, 1,347 lines, no HTML tags |
| `web/auth_state.py` — lazy dialog via _ensure_dialog() | VERIFIED | Lines 414-428 confirmed |
| `tests/e2e/` — 3 test files with importorskip (491 total lines) | VERIFIED | All guards present at module level |

### Plan 08: Page Navigation Speed

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `web/pages/search.py` | asyncio.gather for 3 parallel enrichment queries | BROKEN | asyncio.gather at line 2077 is correct; import asyncio at line 26 is correct; **line 2118 uses undefined `result_sys_ids`** instead of `all_sys_ids`; NameError on every search |
| `web/pages/browse.py` | fjms_data batch pre-fetch in load_page | VERIFIED | state.fjms_data dict at lines 966-977; 5 update_content reads at lines 2063-2235 |
| `web/pages/discoveries.py` | Async stats+feed with asyncio.gather | VERIFIED | initial_load() with asyncio.gather at lines 261-304; ui.timer at line 304 |

---

## Key Link Verification

### Plan 08 — Critical Wiring Issue

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `execute_search()` | `asyncio.gather` | parallel run.io_bound calls | WIRED | Line 2077: `raw_domains, transcription_ids, catalog_counts = await asyncio.gather(...)` |
| `execute_search()` | `search_state.result_domains` | all_sys_ids dict slice | BROKEN | Line 2118 uses `result_sys_ids` which does not exist in `execute_search()` scope; was consolidated into `all_sys_ids` by commit f549e75e but line 2118 was not updated |
| `load_page()` | `state.fjms_data` | pre-fetch 5 FJMS calls | WIRED | Lines 966-977: dict with all 5 FJMS calls |
| `update_content()` | `state.fjms_data` | fjms_data.get() reads | WIRED | Line 2063: `fjms_data = state.fjms_data or {}` used for all 5 formerly-serial calls |
| `initial_load()` | `asyncio.gather` | parallel run.io_bound for stats+feed | WIRED | Lines 289-292: stats and feed fetched concurrently |

---

## Bug Detail: NameError in search.py line 2118

**Commit that introduced it:** `f549e75e` (perf(39-08): parallelize search post-processing enrichment queries)

**Root cause:** Plan 39-08 Task 1 consolidated two identical list comprehensions (`result_sys_ids` and `all_sys_ids`) into a single `all_sys_ids` variable. The commit correctly removed all three old sequential await calls that used `result_sys_ids`. However, there was a fourth use of `result_sys_ids` at line 2118 (slicing `result_domains` for badge rendering) that was not updated:

```python
# Line 2118 — BROKEN:
search_state.result_domains = {sid: doms for sid, doms in search_state.all_result_domains.items() if sid in set(result_sys_ids)}

# Should be:
search_state.result_domains = {sid: doms for sid, doms in search_state.all_result_domains.items() if sid in set(all_sys_ids)}
```

**Scope:** `result_sys_ids` is defined at line 1931 inside `_apply_domain_exclusions()` (a different nested function) and at line 2201 inside `execute_search()` (but only within a conditional block that runs AFTER line 2118). Neither is in scope at line 2118.

**Impact:** `NameError: name 'result_sys_ids' is not defined` on every search execution that produces results. Search results will not be rendered.

**Fix:** One-word change on line 2118: `result_sys_ids` → `all_sys_ids`.

---

## Requirements Coverage

All plans declare `requirements: []`. Phase 39 is a maintenance/polish phase with no formal requirement IDs. No entries in `.planning/REQUIREMENTS.md` map to Phase 39. No orphaned requirements.

---

## Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `web/pages/search.py` | 2118 | `result_sys_ids` undefined variable | BLOCKER | NameError on every search execution — search results not rendered |

### Pre-existing non-blocker items (documented in deferred-items.md)

| Item | Severity |
|------|----------|
| `test_msviewer_ktiv_button_exists` test failure | Warning — pre-existing, button styling changed |
| `test_suffixes_counted_in_explosion_guard` test failure | Warning — pre-existing, Hebrew vs English text mismatch |
| `test_prefix_plus_suffix_cascades_down_instead_of_error` test failure | Warning — pre-existing, same text mismatch |

---

## Human Verification Required

### 1. Desktop Crash Stability

**Test:** Run the desktop app, open the Reading Desk view for a manuscript with an image. While an image is loading, close the Reading Desk dialog rapidly. Then rapidly scroll up and down in the VRD. Verify no RuntimeError appears in crash_log.txt.
**Expected:** App remains stable; no new crash entries.
**Why human:** Cannot automate PyQt6 widget destruction race conditions in CI.

### 2. VRD Mouse Wheel Behavior

**Test:** Open the desktop VRD with an image loaded. Scroll the mouse wheel without holding Ctrl. Then hold Ctrl and scroll.
**Expected:** Plain scroll = view scrolls vertically. Ctrl+scroll = image zooms in/out.
**Why human:** Qt wheel event propagation requires a running PyQt6 app to verify.

### 3. PostHog Analytics Activation

**Test:** Set `POSTHOG_API_KEY=phc_test_key`, start the web app, open search page, check browser DevTools Network tab for requests to `us.i.posthog.com`.
**Expected:** PostHog CDN script loads; page view events fire; no JS errors in console.
**Why human:** Cannot verify live CDN requests without a real API key and browser session.

### 4. Browse and Discoveries Page Load Speed

**Test:** Navigate to the browse page for a manuscript with FJMS data. Navigate to the Discoveries/Community page. Both should load faster than before plan 39-08.
**Expected:** Browse metadata panel populates without blocking the UI render. Discoveries stats+feed load concurrently off the UI thread.
**Why human:** Timing verification requires a running server with the fjms_enrichment.db sidecar.

### 5. Search Pagination UX (after line 2118 fix)

**Test:** After fixing line 2118, run a broad search returning 50+ results. Verify results display, pagination controls appear, page 2 starts at #51, and bottom pagination scrolls back to top.
**Expected:** Full pagination flow works with correct numbering and smooth transitions.
**Why human:** Visual/UX verification requires a running browser session.

---

## Gaps Summary

One gap blocks full goal achievement:

**Search NameError (search.py line 2118, plan 39-08):** A one-line bug was introduced when plan 39-08 consolidated `result_sys_ids` into `all_sys_ids`. The `asyncio.gather` implementation at line 2077 is structurally correct. Browse.py (fjms_data pre-fetch) and discoveries.py (async stats+feed) are both complete and correct. The sole issue is that line 2118 still references the old variable name `result_sys_ids` instead of `all_sys_ids`. This causes a `NameError` on every search execution, preventing results from being rendered.

**Fix:** Single-word change on line 2118: `result_sys_ids` → `all_sys_ids`.

All 7 previous plans (01-07) remain verified with no regressions detected. The RETEST-UAT confirmed all 4 previously-found gaps were closed. Plan 39-08 addressed the remaining page speed gap with correct implementations in browse.py and discoveries.py, and nearly correct implementation in search.py (one variable name not updated).

---

_Verified: 2026-02-20_
_Verifier: Claude (gsd-verifier)_
_Mode: Re-verification — plan 39-08 executed after previous VERIFICATION.md was written_
