## [Unreleased] -- Cambridge links: the right item, by structure (web + desktop, branch `ccr-981a58d9-4pz261`)

### Fixes
- **Cambridge (CUDL) links were missing for renamed and bracketed shelfmarks, and some opened another
  fragment.** Reported 2026-10-29 by Adiel Breuer. A record was matched to CUDL through a lossy normalized key that
  drops every other dot of a number run and all brackets: T-S Ar.18(1).6 and Or.1080 3.45 never matched their own
  items (the green box offered a CUDL search instead), while T-S NS 318.41/.42 and T-S Ar.7.31/.32 opened 318.4.1/.4.2
  and 7.3.1/.3.2 -- other fragments, images included. The desktop tried only its cached name, which for an entry first
  fetched from NLI is NLI's old one ("Ms. T-S Loan Collection 180" for T-S Misc.36.180), and a fallback read any
  "Ms. <Roman> N" as Mosseri, so eight AIU and JTS records opened Mosseri items.
- Now one decision per record (`shared/cudl_identity.py`, through `MetadataManager.resolve_cambridge`): a CUDL item
  is the record's only when its label has the same letters and numbers in the same order as one of the record's
  catalogue names (`Ja` = `J-A`, zero padding and separators aside; `T-S F 6.1` is not `T-S 6F1`); only CUL and
  Mosseri records; the main name decides, the other names when it names nothing; a name with characters it cannot
  read, or a number range, names nothing; the NLI catalogue's own link (MARC 856) is used when the names name
  nothing, never against them. Candidates come from the existing `cambridge_manifests` key, so the sidecar is
  unchanged. Against CUDL's list and `libraries.csv`: 1,916 records gain a direct link, 4 are corrected, 11 lose a
  wrong one (`reports/cudl_resolution_changes_2026-10-29.csv`). A Mosseri second-series record made of several
  first-series items keeps linking the first of them (63 records), flagged for a later chooser -- an interim
  choice a reviewer judged reasonable; how to present these records is still the owner's decision.
- Every surface follows that decision: the availability flag (`get_image_sources` no longer probes the table), the
  green-box library link (now the item, not a search), Browse's Cambridge chip (no URL built from the shelfmark), the
  desktop's Cambridge button, viewer and result dialog (no MARC fallback), Joins Lab's details link. The desktop
  header shows the catalogue's name.
- Cached state cannot bring an old link back: `enrich_metadata` drops what an earlier run derived from Cambridge
  (link, images, alignment, credit) before deciding again; each decision carries `RESOLVER_REVISION`, and the image
  shortcuts (`resolve_external_images`, the image routes, Visual Similarity, the desktop reading desk) re-enrich an
  entry without a current one; `/api/cambridge_image` URLs carry `cv=`, its server cache is at version 3, and the
  Puzzle's disk cache is keyed by the CUDL item. A catalogue still loading is never cached as "no Cambridge item".
- Review fixes before release: `enrich_metadata` builds the entry on a copy and publishes it whole (in place, a
  concurrent request saw the new decision beside the old item's images, and the Puzzle cached those pixels under
  the new item); the desktop result dialog re-enriches an entry cached before the decision (it reused any entry with
  MARC data); every `/api/puzzle_ext_image` URL carries `cv=` (the browser kept the old bytes for up to an hour).
  Then: the decision is checked on the cache entry the images and links come from (`cudl_identity.fresh_external_entry`,
  `entry_decision_stale`) -- a second read could return a newer entry whose current decision vouched for the first
  read's old images, which the memory cache and the Puzzle's disk copy then kept under the new item; the memory and
  disk caches keep only what a final decision produced, the disk name carries the revision, and `get_thumbnail` writes
  onto the latest entry instead of putting back the one it read.

### Tests and tools
- `tests/test_cudl_identity.py` (fixture table built with the importer's own key; real CUDL labels from both sides of
  every collision). `tests/fixtures/cudl_baseline_resolved.csv`: the 8 rows that pinned the four wrong items corrected,
  listed in `tests/fixtures/cudl_baseline_ledger.csv`. `scripts/audit_cudl_saved_work.py` lists saved and published
  joins that reference a corrected or removed record.
