# Debug Session: Browse Page PGP Transcription Not Loading from Search Navigation

## Problem Statement

When navigating from search results to the browse page, the PGP transcription doesn't load. However, if you go directly to the browse page (by typing a shelfmark), the transcription loads correctly.

## Technical Context

- **Framework:** NiceGUI (Python web framework)
- **Navigation method:** `ui.navigate.to('/browse?sys_id=...')`
- **Issue:** NiceGUI uses SPA (Single Page Application) routing. When navigating to the same route with different URL parameters, NiceGUI doesn't recreate the page - it reuses the cached page instance.

## Evidence

Debug output shows:
- `[DEBUG] PGP fetch: ...` appears when typing directly in browse ✅
- `[DEBUG] PGP fetch: ...` does NOT appear when navigating from search ❌
- `[DEBUG] Initial load with sys_id=...` also doesn't appear from search ❌

This proves `create_browse_page()` is not being called when navigating from search.

## Test Case

1. Search for "קטעה נכל"
2. Find result "T-S 8J4.22" (sys_id: 990051224900205171)
3. Click "Browse Full Manuscript" button
4. Check if PGP transcription appears in version selector

**Expected:** Transcription should show (it has recto transcription in PGP)
**Actual:** No transcription shown

## Key Files

- `web/pages/search.py` - Contains navigation buttons to browse
- `web/pages/browse.py` - Browse page with PGP fetching logic
- `web/document_service.py` - Functions for fetching PGP data

## Current Debug Code in Place

In `browse.py` around line 878:
```python
print(f"[DEBUG] PGP fetch: sys_id={page.sys_id}, p_num={page.p_num}, all_sources={len(all_sources)}")
```

In `browse.py` around line 2453:
```python
print(f"[DEBUG] Initial load with sys_id={initial_sys_id}, page={initial_page}")
```

## Failed Attempts

1. `ui.run_javascript('window.location.href = ...')` - Didn't execute (may need await)
2. `ui.link` styled as button - Not yet properly tested

## Potential Solutions to Try

1. **Force full page reload** - Use proper JavaScript or `ui.link` elements
2. **Detect URL changes in browse.py** - Add client-side listener for URL parameter changes
3. **Use `@ui.refreshable`** - NiceGUI pattern for refreshing UI components
4. **Use `new_tab=True`** - Opens in new tab (works but poor UX)

## How to Verify Fix Works

After fix, both debug lines should appear when navigating from search:
```
[DEBUG] Initial load with sys_id=990051224900205171, page=1
[DEBUG] PGP fetch: sys_id=990051224900205171, p_num=1, all_sources=3
```

## Related Phase

This bug was discovered during Phase 5 (Search Integration) UAT testing. The search indicator feature is complete, but this navigation bug blocks proper verification.
