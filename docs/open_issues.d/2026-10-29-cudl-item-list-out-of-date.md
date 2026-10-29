## P3 data: Our copy of CUDL's item list lacks items CUDL publishes, so their records get no Cambridge link
Where: `cambridge_genizah.json` -> `nli_data/nli_crossref.db` (`cambridge_manifests`, built by `scripts/import_nli_crossref.py`)
Opened: 2026-10-29

Since 2026-10-29 a record links to a CUDL item only when the item's label has the record's letters and numbers
in the same order (`shared/cudl_identity.py`). Before, the lossy normalized key linked "Moss. X,2.1"
(990053810430205171) to Mosseri X.21, a different letter; that link is gone, and the record now has no direct
CUDL link, because `cambridge_genizah.json` (141,368 items) does not hold MS-MOSSERI-X-00002-00001 -- which CUDL
does publish (`https://cudl.lib.cam.ac.uk/iiif/MS-MOSSERI-X-00002-00001` answered 200, "Genizah fragment (Mosseri
X.2.1)", 2026-10-29). Other records may be in the same position; how many is unknown until the list is refreshed.
"Moss. III,25.1" and "Moss. IX,12.2" were linked to III.251 and IX.122 the same way (IX.122 is record
990053816210205171's medical book); CUDL answered 500 for III.25.1 and IX.12.2, so those two may have no CUDL item.
Fix sketch: re-harvest CUDL's Genizah collection manifest into `cambridge_genizah.json`, rebuild the table on a
copy of the sidecar, review the delta (`reports/cudl_resolution_changes_2026-10-29.csv` is the shape), deploy the
sidecar first, and bump `shared/cudl_identity.py::RESOLVER_REVISION` in the same change so cached decisions and
browser image URLs refresh.
