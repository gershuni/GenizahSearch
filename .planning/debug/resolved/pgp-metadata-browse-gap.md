---
status: resolved
trigger: "PGP metadata not displaying for specific documents on browse page"
created: 2026-02-17T00:00:00Z
updated: 2026-02-17T06:45:00Z
---

## Current Focus

hypothesis: CONFIRMED - FL ID initialization path in browse.py missing state.pgp_metadata assignment
test: Code comparison between FL ID init path (line 4123-4160) and load_page path (line 894-960)
expecting: FL ID path would be missing pgp_metadata assignment
next_action: Report root cause

## Symptoms

expected: PGP metadata (tags, PGP links) should appear on browse page for sys_ids 990053173470205171 and 990053655710205171
actual: PGP metadata does NOT appear on browse page, but DOES appear in search results for the same documents
errors: None reported (silent failure)
reproduction: Navigate from search Advanced View to browse page (which includes fl_id in URL)
started: After Phase 36 rewrite of document_service.py to use local SQLite (pgp.db)

## Eliminated

- hypothesis: Data missing from pgp.db for affected sys_ids
  evidence: Direct SQLite queries confirm document_fragments has rows for both sys_ids (990053173470205171 -> pgpid 657, 990053655710205171 -> pgpid 19170), and documents table has full records with tags and pgp_url for both.
  timestamp: 2026-02-17T00:01:00Z

- hypothesis: get_document_for_fragment function fails for these sys_ids
  evidence: Called get_document_for_fragment('990053173470205171') directly - returns valid pgp_doc with pgpid=657, tags=['marital reconciliation', 'Marriage', 'Jerusalem'], pgp_url. Same for 990053655710205171 (pgpid=19170). Function works correctly.
  timestamp: 2026-02-17T00:02:00Z

- hypothesis: Browse page uses different code path from search for PGP lookup
  evidence: Both use get_document_for_fragment from shared.document_service (web shim is pass-through). However, the INITIALIZATION code path differs - see root cause.
  timestamp: 2026-02-17T00:03:00Z

- hypothesis: sys_ids not in libraries.csv or browse_map.pkl
  evidence: All three sys_ids (affected + working) exist in both libraries.csv and browse_map.pkl with 2 pages each.
  timestamp: 2026-02-17T00:04:00Z

- hypothesis: NLI image availability differs causing different rendering
  evidence: All three sys_ids have identical NLI image sources (nli_fgp=True, 2 images each).
  timestamp: 2026-02-17T00:05:00Z

## Evidence

- timestamp: 2026-02-17T00:01:00Z
  checked: pgp.db document_fragments and documents tables
  found: Both affected sys_ids have valid fragment links and documents with tags/urls
  implication: Data layer is correct; problem is in code path

- timestamp: 2026-02-17T00:02:00Z
  checked: get_document_for_fragment() for all three sys_ids
  found: All return valid pgp_doc dicts with tags, pgp_url, pgpid
  implication: Service layer works correctly

- timestamp: 2026-02-17T00:03:00Z
  checked: web/document_service.py shim
  found: Pure re-export from shared.document_service, no transformation
  implication: No discrepancy between web and shared imports

- timestamp: 2026-02-17T00:04:00Z
  checked: Search-to-browse navigation URLs in search.py
  found: Multiple places (lines 2733-2735, 2808-2810, 3144-3146, 3317-3320) build browse URLs with both sys_id AND fl_id when fl_id is available
  implication: Navigation from search Advanced View triggers FL ID initialization path

- timestamp: 2026-02-17T00:05:00Z
  checked: Browse page initialization branching (lines 4110-4195)
  found: Line 4111 "if initial_fl_id_value:" takes precedence over line 4168 "elif initial_sys_id:". When fl_id is present in URL, the FL ID path runs instead of load_page.
  implication: FL ID path is the active code path for search-to-browse navigation

- timestamp: 2026-02-17T00:06:00Z
  checked: FL ID init path PGP code (lines 4123-4160) vs load_page PGP code (lines 894-960)
  found: CRITICAL DIFFERENCE - FL ID path sets state.pgp_transcription (line 4146) and state.all_sources (line 4137), but NEVER sets state.pgp_metadata. The load_page path sets ALL THREE (state.pgp_metadata at line 924, state.pgp_transcription at line 945, state.all_sources at line 918).
  implication: This is the root cause. PGP metadata (tags, links, document type, dates, description) is never populated when browse page loads via FL ID.

- timestamp: 2026-02-17T00:07:00Z
  checked: UI rendering of PGP metadata in browse.py
  found: PGP link button in header (line 1774) checks "if state.pgp_metadata and state.pgp_metadata.get('pgp_url')". PGP metadata section (line 1977) checks "if state.pgp_metadata". Both will be falsy when pgp_metadata is never set.
  implication: All PGP metadata display elements are gated on state.pgp_metadata which is None on FL ID path

## Resolution

root_cause: The FL ID initialization path in web/pages/browse.py (lines 4123-4160) is missing the `state.pgp_metadata` assignment that exists in the `load_page()` function (lines 922-937). When a user navigates from search results to the browse page via a URL that includes `fl_id` (e.g., `/browse?sys_id=990053173470205171&fl_id=155932945`), the FL ID branch at line 4111 executes instead of the `load_page()` call at line 4168. This FL ID branch correctly fetches the pgp_doc via `get_document_for_fragment()` and sets `state.pgp_transcription` and `state.all_sources`, but it never sets `state.pgp_metadata`. As a result, the PGP link button in the header bar, the PGP metadata section (tags, document type, dates, description), and the PGP link in the external links section all fail to render because they are conditional on `state.pgp_metadata` being truthy.

This is NOT specific to these two sys_ids -- it affects ALL documents when the browse page is loaded via a URL containing fl_id. The "working" document (990001399620205171 / CUL Add.3430) likely appeared to work because it was tested via direct shelfmark search in the browse page (which calls load_page) or via the "Open in Viewer" button in search result cards (line 2309, which does NOT include fl_id in the URL). Navigating pages after initial load (next/prev) also calls load_page() which correctly sets pgp_metadata, so the bug self-heals on page navigation.

fix: Add the missing `state.pgp_metadata` assignment block to the FL ID initialization path, between lines 4140 and 4141, matching the same structure used in load_page at lines 924-937. Specifically, after `if pgp_doc:` on line 4140, add:
```python
state.pgp_metadata = {
    'document_type': pgp_doc.get('document_type'),
    'tags': pgp_doc.get('tags', []),
    'description': pgp_doc.get('description'),
    'languages_primary': pgp_doc.get('languages_primary'),
    'languages_secondary': pgp_doc.get('languages_secondary'),
    'doc_date_original': pgp_doc.get('doc_date_original'),
    'doc_date_standard': pgp_doc.get('doc_date_standard'),
    'inferred_date_display': pgp_doc.get('inferred_date_display'),
    'inferred_date_standard': pgp_doc.get('inferred_date_standard'),
    'inferred_date_rationale': pgp_doc.get('inferred_date_rationale'),
    'pgp_url': pgp_doc.get('pgp_url'),
    'pgpid': pgp_doc.get('pgpid'),
}
```
Also add `state.pgp_metadata = None` in the else branch (line 4155) and exception handler (line 4157).

verification:
files_changed: []
