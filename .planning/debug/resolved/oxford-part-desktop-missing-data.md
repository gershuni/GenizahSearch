---
status: resolved
trigger: "Desktop Oxford Part - No Neubauer-Cowley label, no bibliography, no image for sys_id 990053385780205171"
created: 2026-02-16T00:00:00Z
updated: 2026-02-16T20:00:00Z
---

## Current Focus

hypothesis: CONFIRMED - on_browse_enriched_loaded sets catalog entry, then calls browse_load_page which overwrites info label without catalog entry
test: Complete - code trace through all three code paths confirms the overwrite
expecting: N/A
next_action: Return diagnosis report

## Symptoms

expected: Oxford Part manuscript shows Neubauer-Cowley catalog label, bibliography buttons, and image
actual: No Neubauer-Cowley label, no bibliography, no image
errors: None reported - just missing UI elements
reproduction: Browse to sys_id 990053385780205171 (Oxford MS heb. a.1/1) in desktop app
started: Likely since Oxford Part code path was created

## Eliminated

- hypothesis: EnrichMetadataThread not started for Oxford Parts (previous 33-05 diagnosis root cause)
  evidence: Code at lines 19436-19451 of genizah_app.py shows enrichment thread IS started. The 33-05 fix was applied.
  timestamp: 2026-02-16T00:10:00Z

- hypothesis: setText() vs setHtml() on QLabel (previous 33-05 diagnosis secondary cause)
  evidence: QLabel uses Qt.AutoText by default, which auto-detects HTML. setText() on QLabel renders HTML correctly. No setTextFormat override exists.
  timestamp: 2026-02-16T00:12:00Z

- hypothesis: Crossref database missing data for sys_id 990053385780205171
  evidence: Direct DB query returns CatalogEntry='2613.1' for this sys_id. get_catalog_entry() works correctly.
  timestamp: 2026-02-16T00:15:00Z

- hypothesis: Bibliography data missing is a code bug
  evidence: FJMS returns empty bibliography for this sys_id. This is a DATA absence, not code bug.
  timestamp: 2026-02-16T00:18:00Z

## Evidence

- timestamp: 2026-02-16T00:10:00Z
  checked: genizah_app.py lines 19436-19451
  found: EnrichMetadataThread IS started in _browse_load_part (added by 33-05 fix)
  implication: The previous root cause (missing enrichment thread) was already fixed

- timestamp: 2026-02-16T00:12:00Z
  checked: genizah_app.py line 8534 (QLabel creation) and setText calls
  found: browse_info_lbl is a plain QLabel with no setTextFormat override. Qt default is AutoText which renders HTML.
  implication: setText() vs setHtml() was never the real issue for QLabel

- timestamp: 2026-02-16T00:15:00Z
  checked: nli_crossref.db via direct SQL query
  found: CatalogEntry='2613.1' exists for NLI_AlmaId=990053385780205171
  implication: Data is available, so enrichment should populate catalog_entry in meta dict

- timestamp: 2026-02-16T00:18:00Z
  checked: FJMS service for sys_id 990053385780205171
  found: get_bibliography returns empty list, get_catalog_refs returns empty list
  implication: No FJMS bibliography data exists for this manuscript - absence is data, not code

- timestamp: 2026-02-16T00:20:00Z
  checked: oxford_full_db.json line 88
  found: Part "MS. Heb. a. 1/1" has "images": [] (empty array). Folio range [0,2]. Next part 1/2 has images starting from folio 3.
  implication: No Oxford images exist for this Part. Image absence is a DATA issue.

- timestamp: 2026-02-16T00:25:00Z
  checked: on_browse_enriched_loaded lines 9125-9137 and browse_render_page lines 19567-19604
  found: CRITICAL - on_browse_enriched_loaded at line 9137 sets info label with catalog_entry, then at line 9240 calls browse_load_page(). browse_load_page() calls browse_render_page() which at line 19604 OVERWRITES the info label with fresh text that does NOT include catalog_entry or IsNotGenizah badge.
  implication: Even though enrichment correctly fetches and appends catalog_entry, browse_render_page immediately destroys it

- timestamp: 2026-02-16T00:28:00Z
  checked: _browse_nav_rendered flag usage
  found: Flag only set to True in browse_navigate() (line 19489). Never set during _browse_load_part flow. So browse_load_page() at line 9240 always fires.
  implication: The second browse_load_page call is not suppressed for initial Part loading

## Resolution

root_cause: **browse_render_page() overwrites enriched info label, destroying catalog entry and badge**

The 33-05 fix correctly added the EnrichMetadataThread to `_browse_load_part`. The enrichment thread runs, fetches the Neubauer-Cowley catalog entry ("2613.1"), and `on_browse_enriched_loaded` correctly builds `label_text` with the catalog entry appended (line 9127). However:

1. After setting the info label at line 9137, `on_browse_enriched_loaded` calls `browse_load_page()` at line 9240
2. `browse_load_page()` -> `browse_render_page()` builds its OWN `info_text` from scratch (lines 19567-19604)
3. `browse_render_page()` does NOT include `catalog_entry` or `is_not_genizah` badge
4. Line 19604 OVERWRITES the enriched label with the non-enriched version

This affects ALL manuscripts (not just Oxford Parts) - but it's only noticeable for Oxford manuscripts because they're the only ones with catalog entries.

For images and bibliography: these are DATA absences, not code bugs. Oxford Part MS. Heb. a. 1/1 has `"images": []` in oxford_full_db.json and no FJMS bibliography records.

fix:
verification:
files_changed: []
