## [Unreleased] -- /browse keeps its catalogue record and images through an NLI outage (web + desktop, branch `claude/nli-metadata-disk-cache`)

The National Library of Israel's catalogue and IIIF servers fail in streaks: PostHog
counted 4,338 openings of the shared NLI circuit breaker in the 30 days to 2026-11-03
(about 750 of them from local test runs, since stopped). For the whole of each
breaker window, and on every 5xx or timeout outside one, `fetch_marc_data` and
`fetch_iiif_manifest` answered empty, so /browse -- the most-used page -- lost the
manuscript's catalogue record, its image labels and, for a manuscript with no other
image source, its images, and the empty answer replaced the good one in the saved
NLI cache.

Both answers are now kept on disk once parsed, the way thumbnails and image ids
already are: `shared/nli_metadata_store.py`, one SQLite file
(`<INDEX_DIR>/nli_metadata.db`, `NLI_METADATA_STORE_FILE`) beside the FL-id store,
read by both apps.

- A row younger than `NLI_METADATA_STORE_TTL` (30 days) answers without a network
  call. An older one is refreshed; when NLI cannot answer -- breaker open, negative
  cache, 5xx, 429, timeout, connection error, or an unparseable body -- the old row
  is served instead, marked `stale: True` with its `fetched_at`, and the breaker and
  the per-manuscript negative cache record the failure as before.
- Only a parse with content is written: an error page served with status 200, an
  empty manifest or a record with no fields never replaces the last good copy.
- Synthetic sys_ids reach neither the store nor the network (Phase 85 D-14); the
  store refuses them too.
- The file is capped at `NLI_METADATA_STORE_MAX_ROWS` (200,000, about 2 KB a row),
  pruned oldest-fetched first at each write and at web startup; counters, row count
  and size are in `/_internal/memstat` under `nli_metadata_store`. Safe to delete.
- The web's /browse shows a quiet note under the image, "Catalogue details from
  <date>", when what it is showing came from the stored copies; `enrich_metadata`
  records the oldest such date as `nli_stale_from` and clears it once NLI answers.

`tests/test_nli_metadata_store.py` pins each of these against `MetadataManager`
with a fake session; the ten fetch-path tests fail on the previous code.
