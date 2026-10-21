# NLI thumbnails: fill the server's thumbnail cache ahead of time

**Status:** plan, 2026-10-21. Nothing in it is built. Four owner decisions gate it (section 2).
**Parents:** [SEED-001](../../.planning/seeds/SEED-001-server-iiif-image-cache.md) (server image
cache, dormant since 2026-03-31) and [`JOINS_LAB_VS_IMPROVEMENTS_PLAN.md`](JOINS_LAB_VS_IMPROVEMENTS_PLAN.md)
PR 6 step (b), the disk store filled as images are viewed, which was built on 2026-10-21 (section 1).

## 1. Why, and what the 2026-10-21 change already does

**The bug (owner report, 2026-10-21).** In the Visual Similarity thumbnail grid some cards came up
with the crossed-out image icon, and different ones after each size switch. The images exist:
ENA 1487.87 and T-S Ar.43.124, both blank in the report, return images from production the same
day. The cause, reproduced locally with NLI simulated as healthy but slow:

- Each card asks `/api/nli_image_by_sysid/<sys_id>?page=0&width=300`. For a manuscript the
  server has not resolved, the proxy first downloads its NLI IIIF manifest.
- Only 8 manifest downloads run at once, and a request waits at most 1 second for one of the 8
  slots (`NLI_MAX_CONCURRENT_FETCHES`, `NLI_SEMAPHORE_TIMEOUT`, `web/api.py`). An uncached
  thumbnail took 2.3-3.5 s from production. With 20 cards at once, most of the 12 that wait give
  up, and the route answers 404.
- A failure is `no-store` and a success is kept a week, so each redraw (the size buttons redraw
  the grid; all three sizes use the same 300 px URL) asks again for exactly the failed cards, and
  with fewer of them competing, more get through.

| Manifest delay | 1st draw | 2nd draw | 3rd draw |
|---|---|---|---|
| 0.4 s | 0/20 failed | - | - |
| 0.8 s | 4/20 | 0/4 | - |
| 1.5 s | 12/20 | 4/12 | 0/4 |

**Built on 2026-10-21** (`web/nli_image_store.py`):

1. **The FL-id store keeps 30 days.** The old `nli_fl_ids_cache.json` was meant to keep a
   manuscript's image ids for 30 days, but every save rewrote it from the memory cache, which
   keeps an entry 5 minutes, so it never held more than the last 5 minutes. It is now a SQLite
   file (`<INDEX_DIR>/nli_fl_ids.db`) written one entry at a time and read when memory misses.
   A manuscript anyone opened in the last 30 days needs no manifest download, and therefore no
   slot.
2. **Thumbnails are kept on disk.** The grid width (300 px, `NLI_THUMB_CACHE_WIDTHS`) is written
   to `<INDEX_DIR>/nli_thumbs/` the first time the proxy gets it from NLI's IIIF server, and read
   from there before the NLI circuit breaker is consulted, so it also shows during an NLI outage.
   The folder stops growing at `NLI_THUMB_CACHE_MAX_MB` (10 GB) or 2 GB of free disk.

**What that does not fix:** the first view of a manuscript nobody has opened in 30 days still
needs a manifest, and a grid of 20 such cards still loses some of them. Most Visual Similarity
look-alikes are manuscripts nobody has opened. Fetching the thumbnails ahead of time fixes the
first view too.

## 2. Owner decisions before anything is fetched

1. **NLI's terms of use** for downloading their thumbnails in bulk. The Joins Lab plan already
   waits on this reading for step (b); bulk prefetching needs it even more.
2. **Disk:** `df -h /` on the server and the budget for `nli_thumbs/` (estimate below).
3. **Where and how fast:** from the server (the same IP the live site uses for NLI, so a rate
   limit there hurts the site) or from another machine with the folder copied up; and the
   request rate (section 4).
4. **The desktop** (section 7): whether the desktop app asks our server for thumbnails.

## 3. What to fetch

- One image per manuscript: the first image, 300 px wide, first IE. That is what every grid card
  asks for (`web/components/candidate_grid.py::build_thumbnail_url`; Visual Similarity, Joins Lab
  Small/Medium and the search results all use page 0 at width 300). Kept at
  `nli_image_store.thumb_path(sys_id, 0, 300, 1)`, so the live proxy serves it with no change.
- Order:
  1. every look-alike in `fist_data/visual_similarity.db` (distinct `alma_id`; not counted yet --
     the database is not in the cloud container this plan was written in);
  2. the rest of the corpus from `libraries.csv` (255,723 records), CUL first (about 128K).
- Skipped: Oxford (the grid goes to the Bodleian directly), synthetic sys_ids (no NLI image),
  and anything already on disk.
- **Size:** six 300 px thumbnails measured on production were 19-34 KB, about 26 KB on average:
  about 6-7 GB for the whole corpus, a fraction of that for the look-alikes alone. (SEED-001's
  800 px reading images would be about 86 GB; not in this plan.)

## 4. How

A script, `scripts/prefetch_nli_thumbnails.py`, using the store module's own functions so the
layout and the checks live in one place:

- For each sys_id: skip it if `thumb_path(...)` already exists; take its FL ids from the FL-id
  store, else download the manifest (and record the ids with `fl_id_store_put`, which also helps
  the Browse page); fetch `https://iiif.nli.org.il/IIIFv21/FL<fl>/full/300,/0/default.jpg`; keep it
  only if it is a JPEG (`thumb_put` checks); no Rosetta stand-ins.
- **Polite by default:** one request at a time, a fixed rate (start at 1 request a second: about
  2 requests a manuscript, so about 2 seconds a manuscript and roughly 6 days for the corpus at
  that rate; the look-alikes first), back off on 429/5xx and stop after a run of failures. The web
  app's breaker is per process and does not see the script, so the script keeps its own count.
  Run in the quiet hour if it runs on the server.
- **Resumable:** what is on disk is the progress; failures go to a CSV (`sys_id`, reason) for a
  later retry. Options: `--source vs|corpus`, `--limit`, `--rate`, `--dry-run` (counts and
  estimated size only).
- Off the server: run it anywhere with the same `NLI_THUMB_CACHE_DIR` layout and copy the folder
  up; the paths do not depend on the machine.

## 5. Staleness

NLI rarely changes a manuscript's first image, and nothing expires a kept thumbnail. To redo one,
delete its file; to redo all, delete the folder. A later option: recheck files older than N months.

## 6. How we will know it works

- `/_internal/memstat`, section `nli_image_store`: `thumb_hits` rising, `thumb_misses` falling,
  `thumb_skipped_low_disk` at 0.
- The owner's case: Visual Similarity for a manuscript whose look-alikes nobody has opened shows
  every card on the first draw.
- Tests for the script: existing files skipped, back-off and stop, JPEG-only, the failure CSV.

## 7. Desktop (owner decision pending)

How it works today: `meta_mgr.get_thumbnail()` (`shared/metadata_manager.py`) downloads the NLI
manifest unless this machine has seen the manuscript; in the Joins Lab grid this runs one card at a
time (`ThumbResolver`, `desktop/join_workbench.py`). `ImageLoaderThread`
(`desktop/image_loader.py`) then fetches the 400 px image from NLI and keeps it in
`<INDEX_DIR>/images_cache/` (no size limit; tracker).

Recommended:

- **Thumbnails:** the machine's own copy first, then our server
  (`/api/nli_image_by_sysid/<sys_id>?page=0&width=300`), then NLI directly as today when the
  server fails. One request per card instead of a manifest plus an image. Measured from outside: a
  thumbnail Cloudflare holds took 0.27-0.63 s including the TLS handshake, about 0.1 s on an open
  connection; an uncached one 2.3-3.5 s (the server makes the same two NLI calls). The gain is
  real for anything cached and about zero for a first view, which this plan's prefetch removes.
- **Full-size viewer images (2000 px):** stay direct from NLI; the server should not relay large
  images for desktop users.
- **Fix in the same change:** the local cache names a file by FL id only (`FL<id>_v2.jpg`,
  `image_loader.py`), so a 400 px thumbnail and the 2000 px viewer image of the same page share a
  file, and the viewer can show the small one enlarged (found by reading the code; not reproduced).
- **Privacy:** the server would see which manuscripts desktop users look at, as it already does
  for web users.
- **Order:** the server change must be live before a desktop release uses it.

## 8. Not in this plan

- SEED-001's 800 px reading images.
- The Cambridge, Manchester and JTS proxies: the grid reaches them only while NLI's breaker is
  open.
- A browser-side retry of a failed thumbnail (the Joins Lab tracker row proposes one). It would
  cover the days before the prefetch runs and is independent of it.
