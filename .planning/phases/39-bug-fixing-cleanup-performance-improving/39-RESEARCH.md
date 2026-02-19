# Phase 39: Bug Fixing, Cleanup, Performance Improving - Research

**Researched:** 2026-02-19
**Domain:** Desktop crash fixes, web performance optimization, analytics, E2E testing
**Confidence:** HIGH

## Summary

This phase addresses five major areas: (1) fixing all desktop crashes from crash_log.txt (2,702 entries across 6 distinct crash types), (2) server-side pagination to break the 200-result WebSocket cap, (3) PostHog analytics integration for real-user monitoring, (4) web performance optimization focusing on the domain filter hierarchy query bottleneck and result rendering, and (5) Playwright/NiceGUI E2E test coverage for critical user flows.

The crash log analysis reveals two dominant Qt object lifecycle issues (QScrollBar 2,347x and QGraphicsSimpleTextItem 341x) that can be fixed with `sip.isdeleted()` guards, plus four rare crashes (KeyError 'uid', AttributeError list.replace, TypeError sequence item, eventFilter) that were caused by early development data shape mismatches and appear to already be addressed in the current code. The web performance bottleneck is dominated by `get_domain_hierarchy()` taking ~5 seconds per call with no caching. PostHog integrates as a simple JS snippet in `<head>` (identical to the existing Google Analytics pattern). NiceGUI's built-in Screen fixture with Selenium provides the E2E testing path.

**Primary recommendation:** Fix the two high-frequency Qt crashes with sip guards, add hierarchy caching to eliminate the domain filter lag, integrate PostHog as a head script alongside GA, implement client-side pagination for search results, and add NiceGUI Screen fixture E2E tests.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Fix ALL crash types -- clean slate approach
- QScrollBar deleted (2,347x) and QGraphicsSimpleTextItem deleted (341x) are the big two -- both Qt object lifecycle issues in desktop app
- Also fix rare crashes: KeyError 'uid' (2x), AttributeError list.replace (2x), TypeError sequence item (1x)
- After fixing: archive current crash_log.txt to crash_log_archive.txt, then clear crash_log.txt for a clean baseline
- Pending todos from STATE.md: Claude assesses which are worth doing based on impact vs effort (JA diacritic dots normalization, desktop corrections migration to shared service, domain click behavior in browse metadata, pre-search domain filtering optimization)
- genizah_app.py (18.5K lines): Claude assesses whether any module extraction would be safe and high-value
- web/pages/search.py (3,200 lines): Claude assesses based on actual code structure
- auth_state.py hardcoded timeouts: Claude assesses if worth the effort
- User reports general web slowness: search results rendering, browse page loading, page navigation
- Integrate PostHog for real-user analytics and performance monitoring
- PostHog scope: core analytics (page views, feature usage, performance timings) + session recordings -- start lightweight, expand later
- Raise the 200-result WebSocket cap with server-side pagination so users can browse beyond 200 results without loading all at once
- Profile and fix web performance hotspots identified through local investigation
- Add Playwright E2E tests for critical user-facing flows (happy paths)
- E2E scope: Search -> View -> Edit -> Submit -> Approve and other key user journeys
- Add performance/stress tests: 1000+ results, 100+ list items
- Coverage goal: ensure all critical paths have at least one happy-path test (no arbitrary number target)
- CI setup: Claude decides based on project setup complexity

### Claude's Discretion
- Specific fix approach for QScrollBar and QGraphicsSimpleTextItem crashes (guard checks, signal disconnection, or both)
- Which pending todos are worth addressing (impact vs effort assessment)
- Whether to extract modules from large files (only if safe and clearly high-value)
- auth_state.py timeout configurability (fix or keep deferring)
- PostHog feature depth beyond core analytics + recordings
- CI integration for Playwright tests (now vs later)
- Web performance optimization techniques (lazy loading, component splitting, caching strategies)

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| PyQt6.sip | (bundled) | Check if Qt C++ objects are deleted before access | Only reliable way to guard against deleted Qt objects in PyQt6 |
| PostHog JS | latest (CDN) | Client-side analytics, session recordings | User-requested; CDN snippet, no npm needed |
| playwright | 1.x | E2E browser testing | Industry standard for Python web E2E; NiceGUI docs suggest Selenium Screen fixture but Playwright is more modern |
| NiceGUI Screen fixture | (bundled) | NiceGUI's built-in integration testing | Handles NiceGUI server lifecycle, Selenium-based |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| pytest-playwright | 0.x | Pytest integration for Playwright | Only if going Playwright route over NiceGUI Screen |
| selenium | (via NiceGUI) | Browser automation | NiceGUI's Screen fixture uses it internally |
| ChromeDriver | match Chrome | Browser driver for Selenium tests | Required by NiceGUI Screen fixture |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| NiceGUI Screen fixture | Playwright Python directly | Playwright is more powerful but requires managing the NiceGUI server lifecycle yourself; Screen fixture handles this automatically |
| PostHog JS CDN | posthog Python SDK | Python SDK is for server-side events; JS snippet gives autocapture, session replay, and page views for free |

**Installation:**
```bash
pip install playwright
playwright install chromium
# OR use NiceGUI's built-in Screen fixture (no extra install, uses selenium)
```

## Architecture Patterns

### Pattern 1: sip.isdeleted() Guard for Qt Object Lifecycle
**What:** Check if a C++ Qt object has been garbage collected before accessing its methods
**When to use:** Any callback/signal handler that accesses a Qt widget which may have been destroyed (tab close, dialog close, page navigation)
**Example:**
```python
from PyQt6 import sip

def sync_text_to_image(value):
    if self._browse_rd_syncing:
        return
    self._browse_rd_syncing = True
    try:
        # Guard against deleted scrollbar objects
        if sip.isdeleted(text_bar) or sip.isdeleted(image_bar):
            return
        text_max = text_bar.maximum()
        image_max = image_bar.maximum()
        if text_max > 0 and image_max > 0:
            ratio = value / text_max
            image_bar.setValue(int(ratio * image_max))
    finally:
        self._browse_rd_syncing = False
```
**Source:** PyQt6.sip module (verified available in this codebase)

### Pattern 2: PostHog JS Snippet (Alongside Existing GA)
**What:** Add PostHog tracking script to page head, same pattern as existing Google Analytics
**When to use:** Every page load
**Example:**
```python
# In web/main.py, alongside ANALYTICS_SCRIPT
POSTHOG_SCRIPT = '''
<script>
    !function(t,e){var o,n,p,r;e.__SV||(window.posthog=e,e._i=[],e.init=function(i,s,a){function g(t,e){var o=e.split(".");2==o.length&&(t=t[o[0]],e=o[1]),t[e]=function(){t.push([e].concat(Array.prototype.slice.call(arguments,0)))}}(p=t.createElement("script")).type="text/javascript",p.crossOrigin="anonymous",p.async=!0,p.src=s.api_host.replace(".i.posthog.com","-assets.i.posthog.com")+"/static/array.js",(r=t.getElementsByTagName("script")[0]).parentNode.insertBefore(p,r);var u=e;for(void 0!==a?u=e[a]=[]:a="posthog",u.people=u.people||[],u.toString=function(t){var e="posthog";return"posthog"!==a&&(e+="."+a),t||(e+=" (stub)"),e},u.people.toString=function(){return u.toString(1)+".people (stub)"},o="init capture register register_once register_for_session unregister unregister_for_session getFeatureFlag getFeatureFlagPayload isFeatureEnabled reloadFeatureFlags updateEarlyAccessFeatureEnrollment getEarlyAccessFeatures on onFeatureFlags onSessionId getSurveys getActiveMatchingSurveys renderSurvey canRenderSurvey getNextSurveyStep identify setPersonProperties group resetGroups setPersonPropertiesForFlags resetPersonPropertiesForFlags setGroupPropertiesForFlags resetGroupPropertiesForFlags reset get_distinct_id getGroups get_session_id get_session_replay_url lib get_property getSessionProperty sessionRecording startSessionRecording stopSessionRecording sessionRecordingStarted captureException loadToolbar get_config __request_queue".split(" "),n=0;n<o.length;n++)g(u,o[n]);e._i.push([i,s,a])},e.__SV=1)}(document,window.posthog||[]);
    posthog.init('YOUR_PROJECT_API_KEY', {
        api_host: 'https://us.i.posthog.com',
        person_profiles: 'identified_only',
    })
</script>
''' if os.environ.get('POSTHOG_API_KEY') else ''
```
**Source:** PostHog official installation docs, verified CDN pattern

### Pattern 3: Client-Side Pagination for Search Results
**What:** Instead of sending 200 results to the client at once, send them in pages of ~50
**When to use:** When search returns more than one page of results
**Key considerations for NiceGUI:**
- All search results already live server-side in `search_state.results`
- `render_results()` currently receives `results[:200]` and creates NiceGUI cards for each
- Pagination means: render only `results[page*size : (page+1)*size]` and add prev/next/jump controls
- Use `ui.pagination` component for page navigation
- The 200 cap is a WebSocket safety limit -- with pagination, we can hold ALL results server-side and only render one page at a time
- Filters and bulk operations need to work against `search_state.results` (full set), not just the displayed page

### Pattern 4: FjmsService Hierarchy Caching
**What:** Cache the domain hierarchy in memory after first computation
**When to use:** `get_domain_hierarchy()` takes ~5 seconds per call; cache eliminates this on subsequent calls
**Example:**
```python
class FjmsService:
    def __init__(self, ...):
        ...
        self._hierarchy_cache = None
        self._hierarchy_lock = threading.Lock()

    def get_domain_hierarchy(self) -> dict:
        if self._hierarchy_cache is not None:
            return self._hierarchy_cache
        with self._hierarchy_lock:
            if self._hierarchy_cache is not None:
                return self._hierarchy_cache
            # ... existing query logic ...
            self._hierarchy_cache = result
            return result
```

### Pattern 5: NiceGUI Screen Fixture E2E Tests
**What:** Use NiceGUI's built-in Screen fixture for E2E tests
**When to use:** Testing user-facing flows that involve NiceGUI pages
**Example:**
```python
from nicegui.testing import Screen

def test_search_happy_path(screen: Screen):
    # NiceGUI starts its own server
    screen.open('/search')
    screen.should_contain('Search')
    # Type a query
    element = screen.selenium.find_element(By.CSS_SELECTOR, 'input[placeholder]')
    element.send_keys('test query')
    element.send_keys(Keys.ENTER)
    # Wait for results
    screen.wait(5.0)
    screen.should_contain('Results')
```
**Source:** NiceGUI docs (nicegui.io/documentation/section_testing, GitHub tests/README.md)

### Anti-Patterns to Avoid
- **Creating 200+ NiceGUI components in a loop:** Each NiceGUI component creates a Vue component and WebSocket binding. The domain filter dialog was rewritten to use raw HTML + JS for this reason. Don't regress.
- **Synchronous DB calls in UI event handlers:** The domain filter lag is caused by `get_domain_hierarchy()` (~5s) running synchronously. Always use `run.io_bound()` or pre-cache.
- **Accessing Qt objects from signal handlers after tab/dialog close:** The QScrollBar/QGraphicsSimpleTextItem crashes happen because scroll sync handlers fire after the widget is destroyed. Always guard with `sip.isdeleted()`.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Qt deleted object detection | try/except RuntimeError | `sip.isdeleted()` from PyQt6 | try/except is a symptom fix; sip.isdeleted is the proper guard |
| Web analytics | Custom event tracking code | PostHog JS snippet autocapture | Auto-captures page views, clicks, recordings without manual instrumentation |
| E2E browser automation | Raw Selenium setup | NiceGUI Screen fixture | Manages server lifecycle, provides assertion helpers |
| Pagination UI | Custom prev/next buttons | NiceGUI `ui.pagination` component | Built-in, themed, handles page math |

**Key insight:** The biggest wins come from caching (hierarchy), guarding (sip), and leveraging existing infrastructure (PostHog CDN, NiceGUI Screen) rather than building custom solutions.

## Common Pitfalls

### Pitfall 1: Stale NiceGUI Process During Development
**What goes wrong:** Code changes don't take effect because an old server process is still running
**Why it happens:** NiceGUI's reload mode can leave stale processes, especially on Windows
**How to avoid:** Always verify with a startup marker print. Run with `NICEGUI_RELOAD=false` during debugging.
**Warning signs:** Changes don't seem to work; domain filter still slow after adding cache

### Pitfall 2: WebSocket Message Size with Pagination
**What goes wrong:** Sending too many UI elements still overwhelms the WebSocket
**Why it happens:** Pagination reduces element count but if page_size is too large (100+), still slow
**How to avoid:** Keep page_size at 50 or less. The existing 200 cap was chosen for WebSocket safety.
**Warning signs:** Page transitions feel laggy; browser memory grows

### Pitfall 3: sip.isdeleted() on Non-SIP Objects
**What goes wrong:** Calling sip.isdeleted() on a Python object that isn't a SIP wrapper crashes
**Why it happens:** Only Qt/SIP-wrapped C++ objects can be checked with sip.isdeleted()
**How to avoid:** Only use on objects known to be Qt widgets (QScrollBar, QGraphicsSimpleTextItem, etc.)
**Warning signs:** TypeError from sip.isdeleted()

### Pitfall 4: NiceGUI Screen Fixture Requires ChromeDriver
**What goes wrong:** Tests fail because ChromeDriver isn't installed or version doesn't match Chrome
**Why it happens:** NiceGUI's Screen fixture uses Selenium which requires ChromeDriver
**How to avoid:** Document ChromeDriver installation in test setup; consider making E2E tests optional (skip if ChromeDriver not available)
**Warning signs:** WebDriverException, SessionNotCreatedException

### Pitfall 5: PostHog Blocking Page Load
**What goes wrong:** PostHog snippet slows down page rendering
**Why it happens:** If loaded synchronously, the script blocks parsing
**How to avoid:** Use async loading (the official snippet already uses async); include after page content
**Warning signs:** Lighthouse scores drop after adding PostHog

### Pitfall 6: Double-Counting in Pagination with Bulk Operations
**What goes wrong:** Select All selects only visible page; Export exports only visible page
**Why it happens:** Bulk operations reference displayed_results instead of all results
**How to avoid:** Clearly distinguish "select visible page" from "select all results"; make export always use search_state.results
**Warning signs:** User selects all, exports, gets only 50 items

## Code Examples

### QScrollBar Crash Fix (sync_text_to_image)
```python
# In genizah_app.py, _browse_rd_setup_sync_scroll()
from PyQt6 import sip

def sync_text_to_image(value):
    if self._browse_rd_syncing:
        return
    self._browse_rd_syncing = True
    try:
        if sip.isdeleted(text_bar) or sip.isdeleted(image_bar):
            return
        text_max = text_bar.maximum()
        image_max = image_bar.maximum()
        if text_max > 0 and image_max > 0:
            ratio = value / text_max
            image_bar.setValue(int(ratio * image_max))
    finally:
        self._browse_rd_syncing = False

def sync_image_to_text(value):
    if self._browse_rd_syncing:
        return
    self._browse_rd_syncing = True
    try:
        if sip.isdeleted(text_bar) or sip.isdeleted(image_bar):
            return
        text_max = text_bar.maximum()
        image_max = image_bar.maximum()
        if text_max > 0 and image_max > 0:
            ratio = value / image_max
            text_bar.setValue(int(ratio * text_max))
    finally:
        self._browse_rd_syncing = False
```

### QGraphicsSimpleTextItem Crash Fix (set_image in ZoomableScrollArea)
```python
# In ZoomableScrollArea.set_image() and set_status_message()
from PyQt6 import sip

def set_image(self, pixmap):
    self._pixmap = pixmap
    self._rotation = 0
    self._auto_fit_enabled = bool(pixmap)

    if not pixmap or pixmap.isNull():
        self._pixmap_item.setVisible(False)
        self.set_status_message(tr("No Image"))
        return

    if sip.isdeleted(self._msg_item):
        return
    self._msg_item.setVisible(False)
    self._pixmap_item.setPixmap(pixmap)
    # ... rest of method
```

### Client-Side Pagination
```python
# In web/pages/search.py
PAGE_SIZE = 50

def render_results(results, page=0):
    results_container.clear()
    total = len(results)
    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    start = page * PAGE_SIZE
    end = min(start + PAGE_SIZE, total)
    page_results = results[start:end]

    search_state.displayed_results = page_results

    with results_container:
        # Page info
        with ui.row().classes('w-full justify-between items-center'):
            ui.label(f"{start+1}-{end} of {total}")
            if total_pages > 1:
                pagination = ui.pagination(1, total_pages, value=page+1,
                    on_change=lambda e: render_results(results, e.value - 1))

        # Render only this page
        with ui.column().classes('w-full gap-2 p-4'):
            for i, res in enumerate(page_results):
                create_result_card(start + i, res)
```

## Discretion Assessments

### Pending Todos Assessment

| Todo | Impact | Effort | Recommendation |
|------|--------|--------|----------------|
| JA diacritic dots normalization | LOW -- niche use case, Judeo-Arabic with diacritics | MEDIUM -- requires Unicode normalization research | **DEFER** -- not worth it this phase |
| Desktop corrections -> shared service | LOW -- desktop corrections work fine, just not using shared service pattern | MEDIUM -- refactoring with risk of regression | **DEFER** -- no user-facing benefit |
| Domain click behavior in browse metadata | MEDIUM -- UX improvement for domain navigation | LOW -- small code change | **INCLUDE** in performance plan if natural fit |
| Pre-search domain filtering optimization | MEDIUM -- faster narrow-domain searches | HIGH -- requires Tantivy filter API investigation | **DEFER** -- current post-filter works, caching hierarchy is higher priority |

### Module Extraction Assessment

**genizah_app.py (20,919 lines):** Too risky. This file has deeply intertwined state (GUI -> self references everywhere). Any extraction would require extensive refactoring of `self.*` references. The class hierarchy (ResultDialog, ZoomableScrollArea, GenizahGUI) is tightly coupled. Not worth the risk of regressions for a cleanup phase.

**web/pages/search.py (3,646 lines):** Moderate candidate. The search page is a single large function `create_search_page()` with many nested functions. Extraction is possible but the nested functions capture closure variables (`search_state`, `results_container`, etc.) making it non-trivial. **Recommendation:** Leave as-is; the nested structure is idiomatic for NiceGUI's builder pattern.

**auth_state.py (464 lines):** No hardcoded timeouts found. The file is clean -- it's a simple class with storage keys and OAuth flow. The "hardcoded values" mentioned in OPEN_ISSUES.md were already addressed (JOINS_CACHE_TTL, NLI_CACHE_TTL, IMAGE_CACHE_TTL env vars). **Recommendation:** No action needed.

### E2E Test Framework Decision

**NiceGUI Screen fixture (Selenium-based):**
- Pros: Built into NiceGUI, handles server lifecycle, simple API
- Cons: Requires ChromeDriver matching Chrome version, Selenium is slower than Playwright
- Setup: `pip install selenium` + ChromeDriver binary

**Playwright Python:**
- Pros: Faster, auto-wait, better debugging, parallel execution
- Cons: Need to manage NiceGUI server startup/shutdown yourself, no built-in NiceGUI integration
- Setup: `pip install playwright && playwright install chromium`

**Recommendation:** Use **NiceGUI Screen fixture** for this phase. It's simpler, handles server lifecycle, and the project doesn't yet have any E2E infrastructure. The Screen fixture's `should_contain()` and Selenium access is sufficient for happy-path tests. Playwright can be adopted later if the test suite grows.

### CI Integration Decision

The project has only one GitHub Actions workflow (docs-check.yml). Adding E2E CI requires:
- ChromeDriver on the runner
- NiceGUI dependencies installed
- Access to local data files (Tantivy index, sidecars)

**Recommendation:** **Defer CI** for E2E tests. The E2E tests should run locally for now. The existing unit tests (633 tests) run without a browser. Adding E2E CI requires solving the data file problem (tests need the search index and sidecar DBs), which is a separate infrastructure task.

## Domain Filter Performance Fix Details

Based on the root cause report at `.planning/debug/domain-filter-lag-root-cause-report-2026-02-19.md`:

1. **Root cause:** `get_domain_hierarchy()` takes ~5s per call (SQLite GROUP BY with COUNT DISTINCT on 390K rows)
2. **No caching exists** -- every dialog open re-queries the database
3. **The fix is straightforward:** Add in-memory cache with thread-safe lock in `FjmsService`
4. **No duplicate (AlmaId, Domain, ParentDomain) tuples** exist, so `COUNT(*)` can replace `COUNT(DISTINCT AlmaId)` for a further speedup
5. **The dialog build itself is fast (~0.07s)** once hierarchy data is available

This should be included in the performance optimization plan as a high-priority fix.

## Crash Log Analysis Detail

| Crash Type | Count | Location | Root Cause | Fix Approach |
|------------|-------|----------|------------|--------------|
| QScrollBar deleted | 2,347 | `sync_text_to_image`, `sync_image_to_text` (line ~10971) | Scroll sync handler fires after Reading Desk tab/dialog closed | `sip.isdeleted()` guard on text_bar and image_bar |
| QGraphicsSimpleTextItem deleted | 341 | `set_image` (line ~1416), `set_status_message` | Image load callback fires after ZoomableScrollArea destroyed | `sip.isdeleted()` guard on `self._msg_item` and `self._pixmap_item` |
| KeyError 'uid' | 2 | `load_result_by_index` (line ~3868) | Tag search results don't have 'uid' key | Already fixed: code now checks `data.get('uid')` with fallback |
| AttributeError list.replace | 2 | `_htmlify` (line ~3839) | PGP `full_text` was a list instead of string | Already fixed: code now joins pages with `'\n'.join()` |
| TypeError sequence item | 1 | `load_result_by_index` (line ~3876) | PGP pages returned dicts instead of strings | Already fixed: code now does `p['text'] for p in pages` |
| ImportError/NameError (GenizahApp) | 3 | `<string>` module | Early development: incorrect class name imports | Not reproducible -- from development probing, not runtime crashes |
| KeyboardInterrupt | 4 | N/A | User Ctrl+C | Not a bug |

**Net fix needed:** Only the QScrollBar and QGraphicsSimpleTextItem crashes need active fixing. The other crashes were already resolved in earlier phases but logged before fixes shipped.

## Open Questions

1. **NiceGUI Screen fixture + app.storage.user:** The project uses `app.storage.user` extensively. Testing with Screen fixture may require setting up storage. NiceGUI's GitHub discussions suggest this can be tricky -- may need to mock or pre-populate storage.
   - What we know: Screen fixture creates a fresh NiceGUI instance per test
   - What's unclear: Whether app.storage.user works in test mode
   - Recommendation: Start with tests that don't require auth; skip auth-dependent tests initially

2. **PostHog session recordings privacy:** The app handles Hebrew manuscript text and user research data. PostHog session recordings capture all visible content.
   - What we know: PostHog has privacy controls (mask inputs, exclude elements)
   - What's unclear: Whether researchers would be comfortable with session recording
   - Recommendation: Enable recordings but mask input fields by default; let admin configure via env var

## Sources

### Primary (HIGH confidence)
- Crash log analysis: `C:/GenizahSearch/crash_log.txt` -- 2,702 entries, categorized by type
- Domain filter root cause: `.planning/debug/domain-filter-lag-root-cause-report-2026-02-19.md`
- PyQt6 sip module: Verified `from PyQt6 import sip; sip.isdeleted` available in project venv
- Codebase analysis: `genizah_app.py` (20,919 lines), `web/pages/search.py` (3,646 lines), `web/main.py`, `shared/fjms_service.py`
- NiceGUI testing docs: https://nicegui.io/documentation/section_testing, https://nicegui.io/documentation/screen
- NiceGUI tests README: https://github.com/zauberzeug/nicegui/blob/main/tests/README.md

### Secondary (MEDIUM confidence)
- PostHog JS installation: https://posthog-com-eight.vercel.app/docs/getting-started/install, https://github.com/PostHog/posthog-js
- PostHog Python SDK: https://pypi.org/project/posthog/ (v7.8.0, Jan 2026)
- NiceGUI pagination: https://github.com/zauberzeug/nicegui/discussions/2351
- Playwright Python: https://playwright.dev/python/

### Tertiary (LOW confidence)
- PostHog session recordings privacy defaults: inferred from general PostHog docs, needs verification on PostHog dashboard

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all libraries verified available or standard CDN
- Architecture: HIGH -- patterns derived from actual codebase analysis and root cause reports
- Pitfalls: HIGH -- based on documented issues (domain filter report, crash log analysis)
- Discretion assessments: MEDIUM -- based on code complexity analysis, subjective effort/risk

**Research date:** 2026-02-19
**Valid until:** 2026-03-19 (stable technologies, no fast-moving dependencies)
