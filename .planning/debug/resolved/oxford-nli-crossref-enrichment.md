---
status: resolved
trigger: "Oxford manuscripts fail to get NLI crossref enrichment data (Neubauer-Cowley, IsNotGenizah, bibliography, images) in the desktop app"
created: 2026-02-16T00:00:00Z
updated: 2026-02-16T22:00:00Z
---

## Current Focus

hypothesis: CONFIRMED - _browse_load_part skips EnrichMetadataThread when basic CSV meta is cached
test: traced full code path
expecting: N/A - root cause confirmed
next_action: report findings

## Symptoms

expected: Oxford manuscripts should get NLI enrichment (Neubauer-Cowley, IsNotGenizah, bibliography, images) like non-Oxford manuscripts do
actual: Oxford manuscripts get NO enrichment at all; non-Oxford manuscripts (e.g., Allony Ms. 113) work correctly
errors: No error messages -- silent failure (no data returned)
reproduction: Browse to Oxford MS heb. a.1/1 (sys_id 990053385780205171) -- no enrichment. Compare with Allony Ms. 113 (sys_id 990000465700205171) which works.
started: Since Phase 33 gap closure added enrichment to _browse_load_part

## Eliminated

- hypothesis: NLI crossref uses shelfmark instead of sys_id for lookup
  evidence: All NliCrossrefService methods use NLI_AlmaId (sys_id). Oxford sys_ids DO exist in nli_crossref.db (990053385780205171 has 2 rows with CatalogEntry=2613.1)
  timestamp: 2026-02-16T00:00:30Z

- hypothesis: Oxford shelfmark format mismatch in nli_crossref.db
  evidence: DB has "MS heb. a.1/1" for sys_id 990053385780205171 - format matches. Lookup is by NLI_AlmaId not shelfmark anyway.
  timestamp: 2026-02-16T00:00:30Z

## Evidence

- timestamp: 2026-02-16T00:00:10Z
  checked: nli_crossref.db for Oxford sys_ids
  found: Oxford has 38,838 rows in nli_images. sys_id 990053385780205171 has 2 rows with CatalogEntry=2613.1. sys_id 990053635020205171 has 2 rows.
  implication: NLI crossref data EXISTS for Oxford - the problem is not missing data

- timestamp: 2026-02-16T00:00:20Z
  checked: NliCrossrefService methods (shared/nli_crossref_service.py)
  found: All methods (get_is_not_genizah, get_catalog_entry, get_images, etc.) use sys_id as NLI_AlmaId. No shelfmark-based lookup for these methods.
  implication: Lookup mechanism is correct - if called, it would return data

- timestamp: 2026-02-16T00:00:30Z
  checked: genizah_app.py browse_load (line 19147) - normal non-Oxford path
  found: Always starts EnrichMetadataThread unconditionally (line 19287). Thread calls enrich_metadata which does ALL crossref lookups.
  implication: Non-Oxford manuscripts always get enriched - explains why they work

- timestamp: 2026-02-16T00:00:40Z
  checked: genizah_app.py _browse_load_part (line 19305) - Oxford Part path
  found: Lines 19440-19451 check if cached_meta exists. If YES (always true for Oxford since they're in CSV), calls on_browse_enriched_loaded with un-enriched data and NEVER starts EnrichMetadataThread. Only the else branch starts enrichment.
  implication: THIS IS THE BUG. enrich_metadata is never called for Oxford manuscripts.

- timestamp: 2026-02-16T00:00:50Z
  checked: libraries.csv for Oxford sys_ids
  found: Both test sys_ids exist in CSV with Oxford library_code. fetch_nli_data finds them in csv_bank, populates basic meta (shelfmark, title only). This cached meta triggers the short-circuit.
  implication: Any Oxford manuscript in CSV (all of them) will hit this bug.

- timestamp: 2026-02-16T00:00:55Z
  checked: browse_navigate (line 19453) for same pattern
  found: Lines 19491-19493 have the SAME bug pattern: if cached_meta exists, calls on_browse_enriched_loaded directly without enrichment.
  implication: Navigating to ANY new manuscript also skips enrichment if basic meta is cached.

## Resolution

root_cause: In genizah_app.py _browse_load_part (line 19436-19451), when basic metadata is already cached from CSV (which is always true for Oxford manuscripts), the code calls on_browse_enriched_loaded directly with un-enriched data instead of starting EnrichMetadataThread. The enrich_metadata method (which calls all NLI crossref service methods for catalog_entry, is_not_genizah, bibliography, images, etc.) is NEVER invoked. The same pattern exists in browse_navigate (line 19491-19502).
fix:
verification:
files_changed: []
