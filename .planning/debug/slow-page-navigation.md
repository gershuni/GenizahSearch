---
status: diagnosed
trigger: "Investigate root cause of slow page-to-page navigation in the GenizahSearch NiceGUI web app"
created: 2026-02-19T00:00:00Z
updated: 2026-02-19T00:00:00Z
---

## Current Focus

hypothesis: CONFIRMED - NiceGUI full-page reload architecture combined with massive per-page UI rebuilding
test: Traced every page route handler and measured per-navigation overhead
expecting: Found multiple compounding factors
next_action: Report findings

## Symptoms

expected: Page-to-page navigation should be near-instant (sub-second)
actual: Navigation between modules (search, browse, lists, etc.) is slow
errors: None reported - performance issue, not crash
reproduction: Navigate between any pages in the web app
started: Unknown - likely gradual as more features added

## Eliminated

- hypothesis: Heavy engine initialization (MetadataManager, SearchEngine) runs per page load
  evidence: Engine init runs once via app.on_startup(initialize_engine) in main.py:2321. State is singleton. Services (FJMS, NLI crossref, PGP) are all lazy singletons. NOT re-initialized per page.
  timestamp: 2026-02-19

## Evidence

- timestamp: 2026-02-19
  checked: NiceGUI navigation model (ui.navigate.to)
  found: Each ui.navigate.to() triggers a FULL HTTP page reload + new WebSocket connection. NiceGUI is NOT an SPA framework - every navigation is a full server-side page rebuild.
  implication: This is architectural - every nav destroys old page DOM + WebSocket, creates new HTTP request, server executes route handler, builds entire UI tree, sends it to client, client establishes new WebSocket.

- timestamp: 2026-02-19
  checked: COMMON_STYLES size (main.py lines 110-1460)
  found: ~1,350 lines of inline CSS injected via ui.add_head_html(COMMON_STYLES) on EVERY page load. Plus META_TAGS, ANALYTICS_SCRIPT, POSTHOG_SCRIPT, and apply_theme_immediately() inline JS.
  implication: Every page navigation sends ~1,500+ lines of inline CSS/JS in the HTML head. This is not cached by the browser since it's inline, not a linked stylesheet.

- timestamp: 2026-02-19
  checked: create_layout() function (main.py lines 1466-1823)
  found: Called on EVERY page load. Builds: header with status indicator, quick search, auth buttons (including login dialog creation), full sidebar with nav items/theme switcher/language toggle, "What's New" banner, citation footer, loading bar JS. ~360 lines of UI construction.
  implication: The entire app shell (header, sidebar, footer, timers, auth dialog) is rebuilt from scratch on every single page navigation.

- timestamp: 2026-02-19
  checked: create_auth_buttons() in auth_state.py
  found: If user is NOT logged in, create_login_dialog() is called which builds a complete login/register dialog with tabs, inputs, Google OAuth button, etc. This dialog is built on EVERY page load even though user may never click Login.
  implication: Unnecessary UI widget creation on every page navigation for anonymous users.

- timestamp: 2026-02-19
  checked: Status heartbeat timer in create_layout()
  found: Two ui.timer instances created per page: ui.timer(2.0, update_status, once=True) and ui.timer(10.0, update_status). The update_status function does a round-trip JavaScript ping (ui.run_javascript('Date.now()', timeout=5.0)) to verify WebSocket connection.
  implication: Minor but adds overhead - timers and JS round-trips start immediately on page construction.

- timestamp: 2026-02-19
  checked: apply_theme_immediately() function (main.py lines 1830-1911)
  found: Generates ~80 lines of inline JS that runs Quasar RTL configuration, DOMContentLoaded listeners, and theme application. Generated fresh on every page load.
  implication: Theme JS is dynamically generated server-side per-request instead of being a static asset.

- timestamp: 2026-02-19
  checked: Page-specific content builders (search.py, browse.py)
  found: search.py create_search_page() is ~3,200 lines defining hundreds of UI elements, closures, and event handlers. browse.py create_browse_page() is similarly large (~3,100 lines). These build massive UI trees on every navigation to that page.
  implication: Page content construction is inherently expensive in NiceGUI since all UI is server-side Python objects that must be serialized to the client.

- timestamp: 2026-02-19
  checked: Import pattern in page route handlers
  found: Pages use lazy imports (e.g., "from web.pages.search import create_search_page" inside the route handler). Python caches modules after first import, so this is NOT a repeated cost after first load.
  implication: Module imports are not contributing to the slowness after first access.

## Resolution

root_cause: |
  NiceGUI's architecture means every page navigation (ui.navigate.to) is a FULL HTTP page reload, not SPA-style client-side routing. This triggers a cascade of expensive per-navigation operations:

  1. FULL UI REBUILD (~360 lines): create_layout() rebuilds the entire app shell (header, sidebar, footer, auth dialog, timers, status indicator) from scratch on every navigation. In a traditional SPA, this shell would persist across navigations.

  2. MASSIVE INLINE CSS/JS (~1,500+ lines): COMMON_STYLES (1,350 lines CSS), META_TAGS, ANALYTICS_SCRIPT, POSTHOG_SCRIPT, and apply_theme_immediately() JS are injected inline via ui.add_head_html() on every page. Being inline means the browser cannot cache them between navigations.

  3. UNNECESSARY WIDGET CREATION: create_login_dialog() builds a complete login/register form with tabs, inputs, OAuth buttons on every page load for anonymous users, even though it may never be opened.

  4. HEAVY PAGE CONTENT: Individual pages like search.py (~3,200 lines) and browse.py (~3,100 lines) construct massive UI trees with hundreds of NiceGUI elements, each requiring server-side Python object creation and WebSocket serialization.

  The fundamental issue is NOT that heavy initialization runs per-page (MetadataManager, SearchEngine, SQLite services are all properly singleton/startup-only). The issue is the NiceGUI full-page-reload paradigm combined with a very large amount of per-page UI construction work.

fix:
verification:
files_changed: []
