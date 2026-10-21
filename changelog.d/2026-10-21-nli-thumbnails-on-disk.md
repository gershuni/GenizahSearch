## [Unreleased] -- NLI thumbnails kept on the server, image ids kept 30 days (web, branch `claude/happy-maxwell-2hdkns`)

Owner report: in the Visual Similarity thumbnail grid some cards showed the
crossed-out image icon, and different ones after each size switch. The images
exist. Each card whose manuscript the server had not resolved needed an NLI
manifest download, only 8 run at once with a 1-second wait, and the rest
answered 404; a failure is not cached, so each redraw asked again for exactly
the failed cards. Reproduced: 12 of 20 cards lost when a manifest takes 1.5 s.

### Fixed
- **The NLI image-id cache keeps 30 days, as it was meant to.** Every save
  rewrote `nli_fl_ids_cache.json` from the memory cache, which keeps an entry 5
  minutes, so the "30-day" file held the last 5 minutes. It is now a SQLite file,
  `<INDEX_DIR>/nli_fl_ids.db`, written one entry at a time and read when memory
  misses: a manuscript anyone opened in the last 30 days needs no manifest and
  takes none of the 8 slots. The old JSON file is read into it once at startup
  and renamed `.imported`.

### Added
- **Thumbnails on disk** (`web/nli_image_store.py`): the NLI image proxy keeps
  each page image it gets from NLI's IIIF server at the grid width (300 px) in
  `<INDEX_DIR>/nli_thumbs/` and serves it from there afterwards, also during an
  NLI outage (it is read before the circuit breaker). Only the requested page's
  own IIIF JPEG is kept, never a Rosetta stand-in or another page's image; the
  folder stops at 10 GB, and at 2 GB of free disk. Settings:
  `NLI_THUMB_CACHE_DIR`, `NLI_THUMB_CACHE_WIDTHS`, `NLI_THUMB_CACHE_MAX_MB`,
  `NLI_THUMB_CACHE_MIN_FREE_MB`, `NLI_FL_ID_STORE_FILE` (docs/guides/ENV_VARS.md). Counters in
  `/_internal/memstat` under `nli_image_store`.

### Not yet
- The first view of manuscripts nobody opened in 30 days can still lose cards
  in a large grid. Fetching thumbnails ahead of time, and whether the desktop
  asks the server for thumbnails: docs/plans/NLI_THUMBNAIL_PREFETCH_PLAN.md.
