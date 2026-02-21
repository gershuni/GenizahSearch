---
status: complete
phase: 40-performance-optimization
source: 40-01-SUMMARY.md, 40-02-SUMMARY.md, 40-03-SUMMARY.md, 40-04-SUMMARY.md, 40-05-SUMMARY.md
started: 2026-02-20T14:30:00Z
updated: 2026-02-20T14:30:00Z
---

## Current Test

number: COMPLETE
name: All tests done

## Tests

### 1. Desktop Search Results Display Speed
expected: Run a search in the desktop app. Results should appear in the results list immediately. Domain classification badges and the domain filter button should populate shortly after (~200ms), not blocking the initial display.
result: PASS (after fix — DomainEnrichmentWorker was using main-thread FjmsService singleton with check_same_thread=True; fixed to create thread-local FjmsService(thread_safe=True))

### 2. Desktop Lazy Catalog Detail (Browse)
expected: In the desktop browse tab, navigate to a page that has catalog data. Click the FJMS catalog button. It should fetch data on click (brief status bar message like "Fetching catalog...") rather than having already loaded it during page navigation.
result: PASS (after fix — ResultDialog is QDialog not QMainWindow, removed statusBar() calls that caused AttributeError)

### 3. Desktop Lazy Catalog Detail (Reading Desk)
expected: In the desktop Virtual Reading Desk, click the FJMS catalog button. Same behavior as browse -- fetches on click with status bar feedback, not pre-loaded during page load.
result: PASS

### 4. Web Browse Page Load
expected: Open the web browse page and navigate to a document. The page content should render first, then enrichment data (PGP info, FJMS domains, crossref bibliography/catalog refs) should load in parallel and appear shortly after.
result: PASS (note: first shelfmark load is slow, subsequent navigations are very fast — likely cold-start for SQLite connections/caches)

### 5. Web Browse Back-Navigation Speed
expected: In web browse, view a document page, then navigate to a different document, then navigate back to the first one. On revisit, crossref metadata (bibliography, catalog refs) should appear instantly (cached) with no loading delay.
result: PASS (back-navigation is fast; crossref cache working)

### 6. FL ID Navigation Speed
expected: Navigate to a specific fragment by FL ID (e.g., via a link or the folio navigator). The page should load near-instantly without a noticeable multi-second delay scanning 217K entries.
result: PASS (internal test — O(1) dict lookup: 3.1ms and 0.7ms with 939K-entry index, vs ~1200ms linear scan fallback)

### 7. Responsa Search with Variants
expected: Run a Responsa search with variant expansion enabled (e.g., a Hebrew root search). The search should complete without any noticeable extra delay from duplicate variant computation. Performance should feel similar to or faster than before.
result: PASS (internal test — superset cache slicing confirmed: limit=200 request served from cached limit=8000 result in 0.003ms)

## Summary

total: 7
passed: 7
issues: 6 found and fixed
pending: 0
skipped: 0

## Gaps

[none yet]

## Bugs Found and Fixed During UAT

1. **DomainEnrichmentWorker SQLite thread safety** (gui_threads.py) — Worker reused main-thread FjmsService singleton with check_same_thread=True. Fixed: create thread-local FjmsService(thread_safe=True).
2. **ResultDialog statusBar() crash** (genizah_app.py:4279) — ResultDialog is QDialog, not QMainWindow. Removed statusBar() calls.
3. **Web active_source not reset on navigation** (browse.py) — Stale image source (e.g., 'cambridge') persisted across manuscript navigation. Fixed: reset to 'nli' in navigate_shelfmark, search_shelfmark, select_result.
4. **Web folio select ValueError** (browse.py:3692) — page.p_num exceeded folio options. Fixed: clamped value + added _folio_count_matches guard.
5. **Desktop stale folio combo labels** (genizah_app.py:20287) — Combo only repopulated when total changed, keeping stale labels from previous MS. Fixed: always repopulate.
6. **Web enrichment_refs stale across navigations** (browse.py) — Added enrichment_refs.clear() before load_page on all navigation paths.

## Unresolved / Deferred

1. **Cambridge credit text stretches ResultDialog image pane** — Needs word-wrap/max-width constraint on credit label.
2. **Web images not loading (transient)** — NLI IIIF returning HTTP 520 (Cloudflare outage). Not a code bug.
3. **Text disappearing on rapid forward navigation** — May be manuscripts without indexed text or NiceGUI rendering race. Needs retest after fixes.
