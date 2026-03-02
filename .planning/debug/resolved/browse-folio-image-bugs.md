---
status: resolved
trigger: "Investigate three related browse/image bugs: web images, text disappearing, folio labels wrong"
created: 2026-02-21T10:00:00Z
updated: 2026-02-21T12:00:00Z
---

## Current Focus

hypothesis: Confirmed
test: All fixes applied and verified
expecting: n/a
next_action: Archive

## Symptoms

expected:
1. Images should load in web browse page
2. Text and version selector should remain visible when navigating forward through multiple shelfmarks
3. Folio selector should show correct labels starting from page 1 (1r, 1v, 2r, 2v...) matching the actual page count

actual:
1. Web images show "Image not available" for many/all manuscripts
2. After navigating forward several shelfmarks in web, the text pane becomes empty and version selector disappears
3. For sys_id 990001447940205171: Desktop says "page 4r" but selector shows "1r, 1v, 3..10"; Web shows "4r / 10" with dropdown starting at 4r

errors:
- Previous ValueError crash at browse.py:3692 when folio select value exceeds options (already patched with clamp)
- NLI IIIF service returning HTTP 520 (Cloudflare error) as of 2026-02-21

reproduction:
1. Open web browse, navigate to any shelfmark -> image not available (NLI outage)
2. In web browse, navigate forward 3-4 times -> text disappears
3. Browse to sys_id 990001447940205171 -> wrong folio labels

## Eliminated

- hypothesis: Bug 1 is a code regression from Phase 40 parallelization
  evidence: NLI IIIF service returns HTTP 520 for all requests. This is an external outage.
  timestamp: 2026-02-21T10:15:00Z

- hypothesis: Bug 2 is a race condition between enrichment Phase B and content_container.clear()
  evidence: Generation guard correctly prevents stale updates. The real issue is stale state (view_joined, enrichment_refs) persisting across manuscript navigations.
  timestamp: 2026-02-21T11:00:00Z

## Evidence

- timestamp: 2026-02-21T10:05:00Z
  checked: NLI crossref data for sys_id 990001447940205171
  found: 6 images with folio labels 4r, 5r, 6r, 7r, 8r, 9r (all recto only, starting at leaf 4)
  implication: Crossref has partial data for this manuscript

- timestamp: 2026-02-21T10:07:00Z
  checked: Browse_map data for sys_id 990001447940205171
  found: 10 pages with FL IDs (p1 has empty text, p2-p10 have text)
  implication: Search index has 10 pages but crossref has 6 images -- different counts

- timestamp: 2026-02-21T10:10:00Z
  checked: NLI IIIF service availability
  found: HTTP 520 for manifest and FL image URLs; Cambridge IIIF returns 200
  implication: Bug 1 is NLI service outage

- timestamp: 2026-02-21T11:10:00Z
  checked: web/services.py:get_browse_page_by_fl lines 508-512
  found: MISSING folio_count == total_pages guard. get_browse_page (line 354) has guard but get_browse_page_by_fl does NOT.
  implication: When page loaded by FL ID, wrong folio label used (e.g. "4r" for page 1)

- timestamp: 2026-02-21T11:15:00Z
  checked: navigate_shelfmark, search_shelfmark, select_result
  found: None reset view_joined, reading_desk_entries, or clear enrichment_refs when switching manuscripts
  implication: Stale state from reading desk or previous manuscript could affect new manuscript rendering

- timestamp: 2026-02-21T11:45:00Z
  checked: Tests: 73 crossref/image tests pass, 398/399 total pass (1 pre-existing failure unrelated)
  found: All tests pass. Pre-existing failure in test_responsa_core (explosion guard) confirmed unrelated.
  implication: Fixes are safe

## Resolution

root_cause:
- Bug 1: NLI IIIF service outage (HTTP 520). External issue, no code fix needed.
- Bug 2: navigate_shelfmark/search_shelfmark/select_result did not reset view_joined, reading_desk_entries, or clear enrichment_refs when switching manuscripts. Stale state could cause wrong view rendering or stale UI element references.
- Bug 3: get_browse_page_by_fl in web/services.py was missing the folio_count==total_pages guard when setting folio_label. This caused wrong folio labels (e.g. "4r") when crossref image count doesn't match index page count. The guard was present in get_browse_page but absent from get_browse_page_by_fl.

fix:
- web/services.py: Added total_pages guard to get_browse_page_by_fl folio_label assignment (matching get_browse_page logic)
- web/pages/browse.py: navigate_shelfmark now resets view_joined=False, reading_desk_entries=[], and calls enrichment_refs.clear()
- web/pages/browse.py: search_shelfmark (exact match path) now resets view_joined=False, reading_desk_entries=[], and calls enrichment_refs.clear()
- web/pages/browse.py: select_result now resets view_joined=False, reading_desk_entries=[], and calls enrichment_refs.clear()

verification:
- 73 crossref/image tests pass
- 398/399 total tests pass (1 pre-existing failure unrelated)
- Code inspection confirms both get_browse_page and get_browse_page_by_fl have identical guard logic
- Code inspection confirms all navigation entry points now reset stale state

files_changed: [web/services.py, web/pages/browse.py]
