# 75-03 Root Cause Validation

CONFIRMED_ROOT_CAUSE — browser-Back from `/browse` to `/search?q=...` fails to
restore the saved snapshot because `resolve_search_bootstrap` treats any
non-empty `initial_query` as a "has route context" signal and disables the
saved-state restore path.

## Applicable hypothesis from 75-02-SUMMARY.md

Hypothesis **(a)**: URL contains query params on Back → `has_route_context=True`
→ `restore_saved_state=False` → snapshot ignored → fresh search fires.

Phase 74 is **NOT** the regression origin. Verified: `web/search_bootstrap.py`
has not been modified by Phase 74. The gating logic was introduced by commit
**`829cd7cf`** ("fix: don't restore stale session filters on URL-driven search
navigation", 2026-03-27), three weeks before Phase 74 landed. The
75-UAT.md Gaps entry's Phase-74 attribution is a misdirection; the gap itself
is real. The fix shape is the same regardless.

## Confirmed code path (file:line references — proof of read)

1. `web/search_bootstrap.py:35` — the gating line that treats URL `q` as route
   context:

   ```python
   has_route_context = bool(from_browse) or any(
   ```

2. `web/search_bootstrap.py:39` — `restore_saved_state = not has_route_context`
   flips to `False` whenever `initial_query` is non-empty.

3. `web/search_bootstrap.py:64` — return dict sets
   `'restore_saved_results': restore_saved_state`, propagating `False` to the
   caller.

4. `web/pages/search.py:106` — the sole call site that feeds
   `resolve_search_bootstrap` (grep confirmed: only one call site in `web/`).

5. `web/pages/search.py:240` — the hydration guard
   `if restore_saved_results and 'search_results' in app.storage.user:` is the
   only place `search_state.results` is populated from the snapshot on page
   entry. When `restore_saved_results=False`, saved results never hydrate.

6. `web/pages/search.py:4176` — `history.replaceState(null, '',
   '/search?q=...')` stamps the URL at end of a successful search. This is the
   URL the browser restores on Back.

7. `web/pages/search.py:4184` — `persist_search_snapshot(search_state)` writes
   `app.storage.user['search_results']` AFTER the URL is stamped. So when Back
   fires, `search_results` is present and matches the URL's `q`.

8. `web/main.py:901-932` — `search_page_route` passes `q`/`tag`/`mode`/variants
   straight through to `create_search_page` as `initial_query` etc.

9. `web/pages/search_state.py:344-351` — `persist_search_snapshot` explicitly
   excludes `search_query` from the snapshot ("Bootstrap-input keys ... are
   NOT cleared here - they are owned by the bootstrap path").

## Storage-write hole audit

Grep: `storage\.user\[['"]search_query['"]\]` inside `web/pages/search.py`
returned exactly two hits:

- `web/pages/search.py:401` — `save_query()` on `query_input.on('blur', ...)`
  — BLUR ONLY, not search-execute.
- `web/pages/search.py:2016` — `'New Search'` reset to `''`.
- `web/pages/search.py:97` — read only (`raw_saved_query =
  app.storage.user.get('search_query', '')`).

Around the search-execute site (lines 4160-4200), **no write** of
`app.storage.user['search_query']` exists. The block goes straight from
`history.replaceState` (4176) to `persist_search_snapshot` (4184) without ever
stamping `search_query`.

**STORAGE_WRITE_HOLE_CONFIRMED** — a user who types a query and presses Enter
without the input first losing focus never updates `search_query`. The
back-nav guard `initial_query == saved_query` then compares the URL's new `q`
against the PREVIOUS query (or `''`). Task 2 Edit 2b is mandatory — it writes
`app.storage.user['search_query'] = clean_query` next to
`persist_search_snapshot` so `saved_query` authoritatively mirrors the URL
the browser will restore on Back.

## Fix shape

Add a new branch in `resolve_search_bootstrap` that detects browser-Back from
`/browse` to `/search?q=<stamped-query>`:

- New keyword-only parameter on `resolve_search_bootstrap`:
  `saved_results_count: int = 0`. Call site passes
  `len(app.storage.user.get('search_results', []) or [])`.
- Guard (all conditions AND-chained):
  1. `from_browse` is None/falsy
  2. `initial_tag` in `(None, '')`
  3. `explicit_mode` is None (i.e. `initial_mode` either None, `''`, or not in
     `VALID_SEARCH_MODES`)
  4. `initial_domain` in `(None, '')`
  5. `initial_query` is not None and non-empty
  6. `initial_query == saved_query`
  7. `saved_results_count > 0`
- When the guard fires (`is_back_navigation=True`):
  - `restore_saved_state = True` (overrides `has_route_context=True`)
  - `restore_saved_results = True`
  - `restore_saved_exclusions = True`
  - `restore_saved_filters = False` — preserves commit 829cd7cf (2026-03-27)
    intent. `history.replaceState` only stamps `q`/`tag`/`mode`/variants; it
    never stamps filters (material/domain/printed sliders). There is no
    authoritative round-trip signal that filter state belongs to the URL, so
    filters stay clean.
  - `resolved_mode = saved_mode or 'exact'` — back-nav restores the saved
    mode (e.g. `Title`), not the `'exact'` default used for fresh URL
    navigations.

Fresh `/search?q=X` requests (shared link, homepage nav, or a different query
than saved) still fall through to the existing `has_route_context` branch —
the 829cd7cf "no stale filter bleed-through" intent is preserved.

Paired fix in `web/pages/search.py` at the search-execute site (~line 4184):
write `app.storage.user['search_query'] = clean_query` immediately after
`persist_search_snapshot` so `saved_query` authoritatively mirrors the URL
the browser will restore on Back. Closes the STORAGE_WRITE_HOLE_CONFIRMED
identified above.
