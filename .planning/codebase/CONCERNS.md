# Codebase Concerns

**Analysis Date:** 2026-02-05

## Tech Debt

**Large Data Structure in Memory:**
- Issue: `unified_variants.py` contains 25,827 lines (25,802 variant pairs) loaded as Python list at module level
- Files: `C:\GenizahSearch\unified_variants.py` (entire file), `C:\GenizahSearch\genizah_core.py:48-53` (import)
- Impact: Increases startup memory footprint; slow initial parse of large module; no lazy loading. Full list kept in memory even if user only needs top N pairs via `get_top_pairs(n)` function
- Fix approach: Convert to lazy-loaded structure. Store variant pairs in CSV/JSON on disk. Load dynamically based on slider value. Consider caching only top 100-500 most common pairs instead of all 25K+

**Mutable Default Arguments & Global Cache:**
- Issue: Multiple uses of mutable default arguments in function signatures (e.g., `_cache={}` in `fetch_fl_ids_from_nli()`) and global module-level caches
- Files: `C:\GenizahSearch\web\api.py:33` (function cache pattern), `C:\GenizahSearch\web\components\joins_panel.py:19-24` (global `_joins_cache` dict)
- Impact: Caches persist across function calls; potential memory leaks if cache grows unbounded. `_joins_cache` in joins_panel has manual TTL management but no automatic cleanup of expired entries
- Fix approach: Replace mutable defaults with None and initialize inside function. Implement proper LRU cache with automatic eviction (use `functools.lru_cache` with maxsize, or implement cleanup thread)

**Bare Print Statements Mixed with Logging:**
- Issue: Code uses both `print()` and `logger.info()` inconsistently for status messages
- Files: `C:\GenizahSearch\web\supabase_client.py` (lines 336, 338, 352, 465, 524, 570, etc - all use `print()`), `C:\GenizahSearch\web\api.py:31,71,91` (print statements), `C:\GenizahSearch\genizah_core.py:4506-4614` (DEBUG logging)
- Impact: Production logs go to stdout only, not captured by logging framework. Makes it harder to configure production logging. Cannot easily redirect to files or adjust verbosity
- Fix approach: Replace all `print()` calls in production code with proper logger calls. Reserve print for CLI tools only

**Unchecked Exception Return Values:**
- Issue: Multiple functions return dict with 'error' key on exception, but callers often don't check for errors before using response data
- Files: `C:\GenizahSearch\web\supabase_client.py:56-89` (sign_up/sign_in pattern), `C:\GenizahSearch\web\pages\corrections.py:152` (TODO: user_vote handling is incomplete)
- Impact: Silent failures. UI may show None values or crash when accessing missing 'data' keys. Error messages lost if not checked
- Fix approach: Standardize to raising exceptions or use Result type. At minimum, make error checking mandatory by documentation and tests

---

## Known Bugs

**Vote Tracking Incomplete:**
- Symptoms: Comment voting tracked in TODO but not implemented
- Files: `C:\GenizahSearch\web\pages\corrections.py:152` (explicit TODO comment)
- Trigger: Creating a correction and checking vote field
- Workaround: None - feature not yet implemented
- Status: ❌ Open - vote_tracking.user_vote always None

**Cache Growth Without Bounds:**
- Symptoms: NLI image cache (`_cache` dict in `fetch_fl_ids_from_nli()`) grows with every unique system_id requested
- Files: `C:\GenizahSearch\web\api.py:33-96` (both `_cache` and `_cache_time` are unbounded)
- Trigger: Long-running server with many different manuscripts viewed
- Workaround: Server restart clears cache
- Status: ❌ Open - No cache size limit implemented

**Race Condition in Global Cache Access:**
- Symptoms: `_joins_cache` in joins_panel is thread-safe with Lock, but `_cache`/`_cache_time` in api.py has no synchronization
- Files: `C:\GenizahSearch\web\api.py:33-96` (no locking), `C:\GenizahSearch\web\components\joins_panel.py:19-24` (has lock)
- Trigger: Multiple concurrent requests to same system_id in fetch_fl_ids_from_nli
- Workaround: Unlikely to cause crashes, but cache may be written by multiple threads simultaneously
- Status: ❌ Open - api.py caches not thread-safe

---

## Security Considerations

**Hardcoded Supabase Credentials (CRITICAL):**
- Risk: Supabase URL and anonymous key visible in source code and commit history
- Files: `C:\GenizahSearch\web\supabase_client.py:26-27` (hardcoded defaults in code)
- Current mitigation: Environment variables recommended (line 24), but defaults embedded in code. Anonymous key is expected to be public, but URL exposes infrastructure
- Recommendations:
  1. Remove hardcoded defaults completely from source
  2. Require SUPABASE_URL and SUPABASE_ANON_KEY in .env file
  3. Scan git history and force-push to remove credentials
  4. Use git-secrets hook to prevent future commits with credentials

**SSRF Protection Present but Minimal:**
- Risk: Image proxy at `C:\GenizahSearch\web\api.py:19-26` has whitelist of allowed domains, but no validation of full URL path
- Files: `C:\GenizahSearch\web\api.py:98+` (image proxy routes)
- Current mitigation: Domain whitelist includes only NLI and Bodleian
- Recommendations:
  1. Validate that returned Content-Type is image/* before serving
  2. Set size limits on image responses (prevent DoS)
  3. Log all image proxy requests for audit trail

**No Input Sanitization for Searches:**
- Risk: Regex mode search accepts user input as regex directly
- Files: `C:\GenizahSearch\genizah_core.py` (search engine accepts mode='regex'), `C:\GenizahSearch\web\pages\search.py` (allows regex entry)
- Current mitigation: Regex parsing error messages shown, but invalid patterns could cause CPU spike
- Recommendations:
  1. Test regex with timeout using signal.alarm() or timeout wrapper
  2. Reject overly complex patterns (deeply nested groups, lookaheads)
  3. Rate-limit regex queries per user

**OAuth Token Handling:**
- Risk: Access and refresh tokens stored client-side in NiceGUI session
- Files: `C:\GenizahSearch\web\supabase_client.py:205-230` (OAuth token handling)
- Current mitigation: Tokens in browser session storage (not localStorage, per NiceGUI)
- Recommendations:
  1. Verify refresh token rotation policy with Supabase
  2. Implement token expiry checks before each API call
  3. Clear tokens on logout and when detected as expired

---

## Performance Bottlenecks

**Full-Text Search on Large Corpus:**
- Problem: Tantivy search with regex filtering on 217K+ manuscripts. Multiple regex passes when variants or case sensitivity needed
- Files: `C:\GenizahSearch\genizah_core.py:4505-4614` (search method with DEBUG logging shows process), `C:\GenizahSearch\web\pages\search.py` (invokes search)
- Cause: Regex filter applied POST-Tantivy results. For large result sets (1000+ hits), regex filtering can be slow
- Improvement path:
  1. Add result count limit early in search (stop at 5000 hits from Tantivy before regex)
  2. Cache compiled regex patterns
  3. Consider Tantivy-level regex support if available
  4. Profile regex performance on large Hebrew texts

**Variant Pair Processing on Every Search:**
- Problem: When variant mode enabled, `get_top_pairs(slider_value)` loads entire UNIFIED_VARIANT_PAIRS list, then slices it
- Files: `C:\GenizahSearch\unified_variants.py` (full list), `C:\GenizahSearch\genizah_core.py:49` (import), usage in search
- Cause: List comprehension on 25K+ items on each search operation
- Improvement path:
  1. Pre-compute top 100/500 pairs at startup and store separately
  2. Use binary search / interval tree for variant lookups instead of linear iteration
  3. Lazy-load variants only if variant mode enabled

**NLI IIIF Manifest Fetching on Every Viewer Load:**
- Problem: Fetching full IIIF manifest (can be hundreds of canvases) for each document view
- Files: `C:\GenizahSearch\web\api.py:43-96` (IIIF fetching), caches result only for 5 minutes
- Cause: Remote API call, not cached aggressively. Manifest structure large (JSON with image URLs for all pages)
- Improvement path:
  1. Increase cache TTL for stable manifests (1+ hour)
  2. Pre-fetch manifests in background for popular documents
  3. Cache manifest in IndexedDB on client-side to reduce server hits
  4. Consider metadata-only endpoint if NLI provides one

**ThreadPoolExecutor with Max 2 Workers:**
- Problem: NLI image fetching limited to 2 concurrent requests
- Files: `C:\GenizahSearch\genizah_core.py:2814` (`self.nli_executor = ThreadPoolExecutor(max_workers=2)`)
- Cause: Conservative default to avoid overwhelming NLI API, but may underutilize bandwidth
- Improvement path:
  1. Benchmark with higher worker counts (4-8)
  2. Add adaptive worker scaling based on response times
  3. Monitor NLI API rate limits and adjust accordingly

---

## Fragile Areas

**Shelfmark Normalization - 5 Implementations:**
- Files: Previously scattered across multiple files (now unified in `C:\GenizahSearch\genizah_core.py:84-120` as canonical)
- Why fragile: Despite unification, if new code added elsewhere that doesn't use `normalize_shelfmark()`, inconsistency will creep back
- Safe modification:
  1. Always import and use `normalize_shelfmark()` from genizah_core
  2. Add test coverage for all shelfmark variants (see TESTING.md for pattern)
  3. Add pre-commit hook to grep for other normalization patterns
- Test coverage: Moderate - spot checks exist, full matrix of variants not tested

**Search Filter UI State Management:**
- Files: `C:\GenizahSearch\web\pages\search.py:30-75` (SearchUIState class with mutable fields like `results`, `selected_indices`)
- Why fragile: Multiple UI elements reference same state object. Updates can trigger cascading refreshes. Race conditions possible between search completion and UI interaction
- Safe modification:
  1. Never modify `search_state.results` directly - use helper functions
  2. Check `is_running` flag before allowing UI interactions
  3. Cancel timers in `update_timer` before starting new searches
- Test coverage: None - untested integration between state and UI components

**Supabase RLS Policies:**
- Files: Database policies defined in Supabase console, documented in `docs/guides/SUPABASE_GUIDE.md`
- Why fragile: RLS policies are in cloud, not version controlled. If policies change without code changes, permission denials will happen silently
- Safe modification:
  1. Always test permission changes in staging environment first
  2. Document policy changes in SUPABASE_GUIDE.md
  3. Add monitoring for 403 errors from Supabase queries
  4. Implement retry-with-different-auth-state logic
- Test coverage: None - no automated tests for RLS policies

**Global Auth State:**
- Files: `C:\GenizahSearch\web\auth_state.py` (GlobalAuthState singleton), used throughout web app
- Why fragile: Single global instance shared across all users in web session. OAuth callback handling complex (token extraction from URL)
- Safe modification:
  1. Never modify auth_state fields directly - use provided methods
  2. Listen to auth_state_changed events instead of polling
  3. Always check user logged in before accessing user_id
- Test coverage: None - auth flow not tested end-to-end (see OPEN_ISSUES.md item "E2E Integration")

---

## Scaling Limits

**Tantivy Index Size:**
- Current capacity: 217K manuscripts indexed locally
- Limit: Unclear - depends on disk space and RAM. Full-text search still fast at 217K, but no testing at 500K+
- Scaling path:
  1. Test index performance at 500K, 1M manuscripts
  2. Consider index sharding (by library code) if search degrades
  3. Monitor index build time (current time unknown)

**Supabase Database Rows:**
- Current capacity: Corrections, comments, discoveries, joins all stored in Supabase
- Limit: Unknown - depends on Supabase tier. RLS policies apply to every query (may add overhead)
- Scaling path:
  1. Monitor query response times as row counts grow (goal: <500ms for all queries)
  2. Add database indexes for common filters (sys_id, author_id, created_at)
  3. Implement pagination for all queries (already done in code)
  4. Consider materialized views for aggregate queries (voted, featured items)

**Web App Concurrent Users:**
- Current capacity: Unknown - NiceGUI runs on single process, Uvicorn server
- Limit: Single-process bottleneck. Each connected user is a WebSocket connection
- Scaling path:
  1. Profile memory per user (estimate from current deployment)
  2. Set deployment worker count based on expected concurrent users
  3. Monitor NiceGUI WebSocket connection limits
  4. Consider worker pool / load balancing for high-traffic deployment

**Desktop App User Sync:**
- Current capacity: 1000s of lists with 10K+ items each, syncing to Supabase
- Limit: Network bandwidth for sync. Desktop currently syncs on open, on changes
- Scaling path:
  1. Implement incremental sync (only changed items since last sync)
  2. Add sync scheduling to avoid peak times
  3. Test with large list libraries (100K+ items across lists)

---

## Dependencies at Risk

**Tantivy Index Format Stability:**
- Risk: Tantivy is actively developed; index format may not be forward-compatible
- Impact: Index built with Tantivy v0.21 may not open with v0.22+. No index version check in code
- Migration plan:
  1. Add version metadata to `Genizah_Index/` directory
  2. Implement index rebuild on version mismatch
  3. Test Tantivy upgrades in staging before production
  4. Keep old Tantivy version until tested

**PyQt6 Desktop App Distribution:**
- Risk: Packaged with PyInstaller; large binary (100+ MB). No code signing or update verification
- Impact: Antivirus false positives, user trust issues. In-app updater calls external installer without signature verification
- Migration plan:
  1. Consider code signing Windows executable with certificate
  2. Verify downloaded installer hash before execution
  3. Implement rollback on failed update
  4. Test update flow thoroughly (done per OPEN_ISSUES.md)

**Supabase SDK Stability:**
- Risk: Python Supabase client is third-party. Auth changes in Supabase may require SDK updates
- Impact: Breaking changes in client API could require codebase updates
- Migration plan:
  1. Pin Supabase SDK to specific minor version in requirements.txt
  2. Test SDK upgrades in isolated environment first
  3. Monitor Supabase client GitHub for deprecation notices
  4. Plan for OAuth v3.0 migration if announced

**NLI IIIF API Stability:**
- Risk: External API with no SLA. Manifest structure could change
- Impact: FL ID extraction (regex pattern at line 63) could break if manifest format changes
- Migration plan:
  1. Implement fallback MARC API endpoint (already exists)
  2. Monitor NLI API responses for schema changes
  3. Add alerts for high manifest fetch failure rate
  4. Cache aggressive (store manifest URLs, not IDs)

---

## Missing Critical Features

**Vote Tracking for Comments:**
- Problem: Corrections have status field, but comments don't have voting support
- Blocks: Community quality metrics, sorting comments by usefulness
- Fix approach: Add votes table, update RLS policies, implement UI in comment viewer

**Nested Comments/Replies:**
- Problem: Comments stored flat; replies not supported
- Blocks: Discussion threads on manuscripts
- Fix approach: Add parent_id column (note: placeholder code exists at `web\components\notes_display.py:205`), recursive query for thread building

**Search Result Highlighting:**
- Problem: Search returns results but doesn't highlight query terms in displayed text
- Blocks: User can't quickly see why result matched
- Fix approach: Return match positions from Tantivy, highlight in UI using HTML spans

**Access Control for Lists:**
- Problem: User lists are private by default (via RLS), but no sharing/collaboration features
- Blocks: Research groups can't share lists
- Fix approach: Add list_access table with user permissions, implement UI for sharing

---

## Test Coverage Gaps

**Search Integration (Untested):**
- What's not tested: Full search pipeline - query parsing, Tantivy search, regex filtering, result dedup, pagination
- Files: `C:\GenizahSearch\genizah_core.py:4500-4650` (search method), `C:\GenizahSearch\web\pages\search.py` (UI invocation)
- Risk: Bug in search logic silently returns wrong results or crashes on edge cases
- Priority: High - affects core functionality

**OAuth Callback Handling (Untested):**
- What's not tested: Complete OAuth flow - Google/GitHub login, token extraction from URL, session setup
- Files: `C:\GenizahSearch\web\supabase_client.py:169-258` (OAuth functions), `C:\GenizahSearch\web\pages\profile.py` (login page)
- Risk: OAuth flow may fail for edge cases (expired tokens, invalid code, network issues)
- Priority: High - blocks user login

**Correction Submission Workflow (Untested):**
- What's not tested: Full correction flow - submit, database insert, author notification, approval workflow
- Files: `C:\GenizahSearch\web\pages\corrections.py` (corrections page), `C:\GenizahSearch\corrections_ui.py` (desktop UI)
- Risk: Corrections may fail to save or notifications may not be sent
- Priority: High - core community feature

**Browser Compatibility (Untested):**
- What's not tested: UI on Chrome, Firefox, Safari, Edge, mobile browsers
- Files: All web pages in `C:\GenizahSearch\web/pages/`
- Risk: UI broken on some browsers (Hebrew RTL, responsive design may have issues)
- Priority: Medium - per OPEN_ISSUES.md

**Large Result Set Performance (Untested):**
- What's not tested: UI responsiveness with 1000+ search results, large list views
- Files: `C:\GenizahSearch\web\pages\search.py` (result pagination), `C:\GenizahSearch\web\pages\browse.py` (list views)
- Risk: UI freezes or crashes when loading/scrolling large result sets
- Priority: Medium - may affect power users

---

## Additional Observations

**Type Hints Coverage:**
- Status: Inconsistent. Many functions use type hints (encouraged per CLAUDE.md), but older code has none
- Impact: IDE autocomplete limited for untouched code
- Recommendation: Add type hints to public APIs gradually, starting with `genizah_core.py` public methods

**Error Messages for Users:**
- Status: Many errors return generic dicts with 'error' key. Users may see raw error text ("Supabase query failed: ...") instead of friendly messages
- Impact: Confusing error messages for end users
- Recommendation: Implement error translation / user-friendly messaging layer

**Hebrew Text Handling Completeness:**
- Status: Most code handles Hebrew correctly (RTL, nikud stripping), but some edge cases may exist (ligatures, combining marks)
- Impact: Some Hebrew text features may not work as expected
- Recommendation: Add comprehensive Hebrew text normalization test suite

---

*Concerns audit: 2026-02-05*
