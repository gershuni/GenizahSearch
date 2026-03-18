# Web Puzzle Technical Specification

> Status: active — browser extension implemented, Chrome Web Store submission pending
> Last updated: 2026-03-18 (extension solution implemented)
> Audience: AI agents and developers working on the web puzzle system

---

## 1. Executive Summary

The **Fragment Puzzle** is a mixed Python + browser feature for visually reconstructing joins between Genizah fragments. It already works well in the desktop app and mostly works in local web development, but it is **not yet production-ready for general web users**.

The central blocker is not Fabric.js or persistence. It is **image acquisition and processing on the web**:

- The puzzle depends on background-removed fragment images.
- Background removal currently runs in Python (`NumPy`/`Pillow`) in `shared/background_removal.py`.
- Production server-side fetching from NLI IIIF fails because NLI blocks requests from datacenter/CDN IP ranges.
- Browser-only display works, but browser-only processing does not, because cross-origin images can be displayed without permitting pixel access.
- A localhost helper path was proven to work for users who run a local helper service, but it does **not** solve the product problem for ordinary web users.

Because of that, the web puzzle has now been placed behind an emergency feature flag:

- `WEB_PUZZLE_ENABLED=false` by default
- route and entry points hidden
- existing puzzle code kept in the repository for future reactivation

This document explains the current code structure, the architectural constraints, and the realistic solution paths.

---

## 2. Current Operational Status

### 2.1 Production status as of 2026-03-18

- The puzzle implementation still exists.
- The puzzle is **hidden in production by feature flag**.
- The route `/puzzle` is guarded in `web/main.py`.
- Entry points from browse/search/lists/community/help are hidden when `WEB_PUZZLE_ENABLED` is false.
- The previous Cloudflare Worker proxy path was removed after testing showed that NLI also blocks Cloudflare.

### 2.2 What still works technically

- Desktop puzzle image loading and processing works.
- Local web development can still use:
  - `/api/puzzle_image` when running from a machine that NLI accepts
  - localhost helper fallback when the helper is running
- The browser UI/canvas/save/export/publish stack is largely implemented and was actively debugged in March 2026.

### 2.3 What is not solved

- A production-safe, zero-install image-processing path for ordinary web users
- A trustworthy replacement for server-side IIIF acquisition
- A secure long-term replacement for `/api/puzzle_process`

---

## 3. Goals and Non-Goals

### Goals

1. Let web users add fragments, manipulate them on a canvas, and navigate folios.
2. Preserve parity with desktop puzzle behavior where practical.
3. Support processed images with transparent backgrounds.
4. Support save/load/export/publish in the web app.
5. Avoid fragile datacenter-IP-dependent image fetching.

### Non-Goals

1. Rewriting the whole puzzle in a separate frontend framework
2. Replacing the desktop puzzle implementation
3. Solving Cambridge blue-mat background removal perfectly before restoring the web feature
4. Keeping the production puzzle public before image acquisition is stable

---

## 4. Code Structure

This section maps the relevant files and their roles.

### 4.1 Route and feature gating

#### `web/feature_flags.py`

Defines:

- `WEB_PUZZLE_ENABLED`

This is currently the production kill switch for the entire web puzzle UI.

#### `web/main.py`

Responsibilities:

- Registers the `/puzzle` route
- Hides/shows the puzzle navigation item
- Hides/shows the puzzle "What's New" banner
- Returns a temporary unavailable card instead of the puzzle page when `WEB_PUZZLE_ENABLED` is false

This is the first file to inspect when re-enabling the feature.

---

### 4.2 Main web puzzle page

#### `web/pages/puzzle.py`

This is the core web puzzle implementation.

It contains:

- NiceGUI page construction
- embedded Fabric.js canvas setup
- a large inline JavaScript object: `window.puzzleCanvas`
- Python-side event handlers and persistence hooks

Key browser responsibilities inside `window.puzzleCanvas`:

- initialize/destroy Fabric canvas
- add/remove fragment images
- keep a fragment registry
- selection handling
- viewport pan/zoom
- fragment transforms
- folio navigation
- reload processed/original variants
- communicate back to Python through DOM `CustomEvent`s

Important image-loading entry points:

- `addFragment(...)`
- `_reloadFragment(...)`
- `navigateFolio(...)`

These three paths must stay behaviorally aligned. Historically, one of the main regression classes was fixing one path but not the other two.

Important recent web-puzzle fixes made in March 2026 included:

- using a real `<canvas>` element instead of `ui.html(...)`
- JS→Python fragment metadata events
- selection sync
- delete persistence
- processed/original reload wiring
- center-preserving folio swaps
- export coordinate parity with desktop

---

### 4.3 Web API endpoints

#### `web/api.py`

Relevant endpoints:

- `GET /api/puzzle_image`
- `POST /api/puzzle_process`
- `GET /api/puzzle_folios/{sys_id}`
- document CRUD and export/publish endpoints

##### `GET /api/puzzle_image`

Purpose:

- canonical server-side image endpoint for puzzle fragments

Behavior:

- calls `shared.puzzle_image_service.resolve_fragment_image(...)`
- returns processed PNG or original JPEG from cache/fetch pipeline
- returns `404` when production server cannot fetch the image

This endpoint is fine as a first attempt, but it is not sufficient on production because the production host cannot reliably fetch from NLI.

##### `POST /api/puzzle_process`

Purpose:

- fallback endpoint for client-uploaded raw image bytes

Current concerns:

- still considered risky
- currently accepts unauthenticated uploaded image bytes
- can cache caller-supplied bytes under shared keys

This endpoint should **not** be considered a secure final architecture unless its trust model is redesigned.

##### `GET /api/puzzle_folios/{sys_id}`

Purpose:

- returns ordered folio/FL-ID list for folio navigation

This endpoint is not the central production blocker; image acquisition is.

---

### 4.4 Shared image pipeline

#### `shared/puzzle_image_service.py`

This is the core image orchestration layer used by both web and desktop.

Responsibilities:

- determine cache paths
- fetch raw IIIF images
- run background removal
- cache original or processed results
- expose a shared `resolve_fragment_image(...)` API

Current fetch behavior:

- direct requests to `https://iiif.nli.org.il/IIIFv21/...`
- browser-like headers are added
- if the request fails, returns `None`

Important implication:

- this code works from local machines that NLI accepts
- this code fails from production server environments that NLI blocks

This file should remain a core reference implementation even if acquisition strategy changes.

---

### 4.5 Background removal engine

#### `shared/background_removal.py`

This is the actual Python image-processing implementation.

High-level behavior:

- samples image corners to infer background
- optionally handles Cambridge/CUL blue-mat cases
- applies HSV-like thresholding
- emits transparent PNG-style output when processed

Current known weakness:

- some Cambridge conservation-mat images still defeat the background detector

This is a separate problem from the production web acquisition problem.

---

### 4.6 Puzzle persistence

#### `shared/puzzle_model.py`

Defines shared model objects such as:

- `PuzzleDocument`
- `PuzzleFragment`

These objects unify web and desktop document/export logic.

#### `shared/puzzle_service.py`

Provides local persistence in `joins.db`.

Responsibilities:

- save/load/delete puzzle documents
- keep fragment index table for reverse lookups
- serve both web and desktop usage

This layer is in comparatively good shape.

---

### 4.7 Export pipeline

#### `shared/puzzle_export.py`

Responsibilities:

- compose full-resolution exports from positioned fragments
- add metadata banner
- generate thumbnails

Important design note:

- export now tries to stay faithful to the on-canvas appearance
- processed fragments reuse the same 800px processed images shown on canvas rather than silently reprocessing at a different resolution

This file matters because image strategy changes must not break export fidelity.

---

### 4.8 Community publishing

#### `shared/puzzle_publish_service.py`

Responsibilities:

- render publishable composite image
- generate thumbnail
- upload to Supabase storage
- upsert publish metadata
- rebuild fragment index in Supabase

Dependencies:

- relies on `compose_puzzle_export(...)`
- relies on puzzle fragments being renderable

This subsystem is not the primary blocker. It only becomes relevant after image acquisition is stable enough for the web puzzle to return.

---

### 4.9 Local helper experiment / prototype

#### `scripts/puzzle_local_helper.py`

This script is the proof of concept for the "process on the user's machine" direction.

It exposes:

- `GET /health`
- `GET /puzzle/resolve?sys_id=...`
- `GET /puzzle/image?fl_id=...`
- `GET /puzzle/image_by_sysid?sys_id=...&page=...`

It reuses:

- `shared.puzzle_image_service.resolve_fragment_image(...)`

Important conclusion from testing:

- this path works technically
- it also worked when called from the live site
- but it is not a general product solution because the website cannot launch local processes on users' machines

---

### 4.10 Desktop reference implementation

Desktop files are useful as the "known good" reference:

#### `gui_threads.py`

Contains:

- `PuzzleImageLoaderThread`
- `PuzzleMetaLoaderThread`

`PuzzleImageLoaderThread` directly calls `resolve_fragment_image(...)`.

#### `genizah_app.py`

Contains:

- puzzle UI window logic
- `add_fragment(...)`
- `PuzzleExportThread`
- publish/export integrations

The desktop implementation proves that:

- the shared image pipeline is valid
- the persistence/export logic is valid
- the hard production blocker is web acquisition/deployment, not core puzzle logic

---

## 5. Main Runtime Flows

### 5.1 Add fragment

Typical web flow when the puzzle is enabled:

1. User comes from browse/search/lists or enters a shelfmark/sys_id.
2. Python side resolves fragment metadata and constructs fragment payload.
3. Browser calls `window.puzzleCanvas.addFragment(...)`.
4. JS tries image loading in a fallback chain.
5. Loaded image becomes a Fabric object.
6. Python receives metadata events and updates server-side state.

### 5.2 Current browser image fallback chain (updated 2026-03-18)

All image paths now use a single unified `_loadImageWithFallbacks()` function. The fallback chain is:

1. `GET /api/puzzle_image` — checks server disk cache first; returns upload token on miss
2. **Browser extension** — if installed, fetches from NLI via user's IP, sends raw bytes to `POST /api/puzzle_process` with HMAC token for server-side bg removal + caching
3. localhost helper (`http://127.0.0.1:43111/...`) — for power users running desktop helper
4. direct NLI display path — degraded display-only fallback (no bg removal)

Interpretation:

- step 1 serves cached images instantly (populated by extension or desktop users)
- step 2 is the primary acquisition path for production — uses user's residential/institutional IP which NLI accepts
- step 3 is legacy fallback for users running the helper locally
- step 4 is degraded display-only for users without extension or helper

### 5.3 Folio navigation

Folio navigation must update:

- `fl_id`
- displayed image
- folio label
- persisted fragment metadata

Historically this was broken because initial add and reload were patched but `navigateFolio(...)` was not.

### 5.4 Save / load / delete

Web save/load uses:

- `shared/puzzle_service.py`
- document CRUD endpoints in `web/api.py`

State also uses NiceGUI/browser storage for session continuity.

### 5.5 Export

Export uses:

- persisted puzzle fragment geometry
- `shared/puzzle_export.py`
- already-processed images where available

### 5.6 Publish

Publish uses:

- saved/local document
- composite image generation
- Supabase storage and metadata tables

---

## 6. Core Technical Challenges

### 6.1 NLI blocks production-style server fetches

This is the central production blocker.

Observed facts:

- desktop/local machine requests succeed
- AWS server-side requests fail
- Cloudflare Worker proxy requests also fail

Likely conclusion:

- NLI is blocking datacenter/CDN IP ranges, not just a specific missing header

### 6.2 Browser security model

The browser can often **display** a cross-origin image but still cannot **read pixels** from it.

Consequences:

- direct `<img>` display is not enough for background removal
- browser-side JS/WASM processing is blocked unless the image becomes same-origin or explicitly CORS-readable

### 6.3 Web cannot launch local software

The localhost-helper prototype worked technically, but the website itself cannot:

- install software
- start a Python process
- launch arbitrary local executables

That makes localhost helper a partial technical solution, not a complete web product solution.

### 6.4 Security debt in `/api/puzzle_process`

This endpoint still has a trust problem:

- caller supplies bytes
- server caches them under shared keys

Even with size/type validation, provenance is still weak.

### 6.5 Canvas/UI complexity

The puzzle is not a thin API wrapper. It is a stateful browser interaction system with:

- Fabric.js object lifecycle
- Python↔JS synchronization
- selection state
- persistence
- async image replacement
- folio mutation

Agents should avoid "small" changes to only one path without checking:

- initial add
- reload/toggle
- folio next/prev
- restore from saved document
- export

### 6.6 Background-removal quality on some CUL images

The image acquisition problem and the segmentation-quality problem are distinct.

Even if acquisition is solved, some blue conservation-mat images still need algorithmic improvement.

---

## 7. Solution Space

This section lists the realistic options from most tactical to most strategic.

### Option A: Packaged localhost helper

Description:

- keep the current localhost-helper architecture
- package it as an installable helper for users
- puzzle page uses it automatically if installed and running

Pros:

- already technically proven
- reuses existing Python pipeline
- avoids server-IP block

Cons:

- requires user installation
- still not purely web-native
- operational support burden

Assessment:

- best short-term technical path if the goal is "restore web puzzle soon"

### Option B: Desktop integration / custom protocol

Description:

- users with the desktop app installed trigger a desktop-controlled local helper
- website talks to localhost after the desktop app starts the helper

Pros:

- leverages existing installed app
- good for power users already using desktop

Cons:

- does not help web-only users
- requires OS-level protocol or app bridge work

Assessment:

- useful complement, not a universal solution

### Option C: Own image storage / image mirror / derivative cache

Description:

- serve puzzle images from infrastructure we control
- server-side processing becomes stable because acquisition is same-origin or internal

Variants:

1. full mirror of upstream images
2. incremental derivative cache
3. only cache puzzle-sized derivatives (for example 800px)

Pros:

- best long-term web architecture
- no helper required
- no browser security hackery once same-origin copies exist

Cons:

- legal/rights considerations
- storage footprint
- initial ingestion path still must come from somewhere

Assessment:

- strongest long-term path if legally and operationally feasible

### Option D: Remote fetcher on an allowed network

Description:

- run a fetcher/worker from a network NLI accepts
- sync results back to our storage/cache

Pros:

- avoids datacenter-IP restriction on production host
- can pre-fill cache progressively

Cons:

- infrastructure complexity
- still depends on a special trusted network

Assessment:

- possible intermediate architecture if storage mirror is desired but direct ingestion from production is impossible

### Option E: Publish/save-time precomputation

Description:

- whenever a puzzle is saved/published from a working environment, generate and upload processed derivatives

Pros:

- reduces repeat work
- helps shared/public puzzle artifacts

Cons:

- does not solve first-time ad hoc fragment acquisition for new web sessions

Assessment:

- good optimization, not sufficient alone

### Option F: Browser-side processing (JS/WASM)

Description:

- port background removal to the browser

Pros:

- eliminates Python server dependency for processing

Cons:

- still needs same-origin/CORS-readable bytes
- significant implementation effort

Assessment:

- not the first problem to solve
- only becomes attractive after image bytes are safely available in browser memory

### Option G: Re-enable current localhost-helper path publicly

Description:

- turn the puzzle back on and rely on helper + degraded fallback

Pros:

- minimal code work

Cons:

- confusing product for ordinary users
- many users would see degraded behavior
- support burden

Assessment:

- not recommended as the default public state

---

## 8. Recommended Strategy (updated 2026-03-18)

### Implemented solution: Browser extension + server derivative cache

As of 2026-03-18, a **browser extension** approach was implemented and deployed:

1. **GenizahSearch Image Helper** Chrome extension fetches NLI images via user's own IP
2. Server processes images (bg removal) and caches to disk (~150GB available)
3. Cached images serve all future users instantly without the extension
4. HMAC upload tokens prevent cache poisoning
5. `WEB_PUZZLE_ENABLED=true` set on production for staged rollout

Key files: `extension/`, `web/puzzle_tokens.py`, `web/api.py`, `web/pages/puzzle.py`

### Long-term recommendation

The server derivative cache grows organically from extension + desktop users. Over time, most fragments will be cached and accessible without the extension. Future options to accelerate cache growth:

- Batch-seed cache from allowed network (home/university machine)
- Desktop app cache contribution (upload processed images to server)
- Owned image storage for puzzle-sized derivatives

### Why not rely on more proxy experiments

Because the main proxy experiments already failed:

- AWS server fetch failed
- Cloudflare Worker failed

This strongly suggests the problem is not a missing header tweak.

---

## 9. Implementation Guidance for Future Agents

### 9.1 If you are re-enabling the web puzzle

Check all of these files first:

- `web/feature_flags.py`
- `web/main.py`
- `web/pages/puzzle.py`
- `web/api.py`
- `shared/puzzle_image_service.py`
- `scripts/puzzle_local_helper.py`

### 9.2 Do not patch only one image path

Every image-loading change must be tested through:

1. initial add
2. processed/original toggle
3. folio next/prev
4. saved-document restore

### 9.3 Treat desktop as the reference implementation

When unsure about intended behavior, inspect:

- `gui_threads.py`
- `genizah_app.py`

Desktop is currently the best source of truth for how the puzzle should behave.

### 9.4 Be careful with `/api/puzzle_process`

Do not expand its usage before fixing the trust model.

### 9.5 Keep feature-flag rollout discipline

The route guard in `web/main.py` exists for a reason. If you re-enable the feature:

- do it intentionally
- keep a rollback path
- avoid removing the feature flag until the acquisition path is truly general-user-safe

---

## 10. Minimum Acceptance Criteria Before Public Re-Enable

The puzzle should not be publicly restored unless all of the following are true:

1. A general-user image acquisition path exists without requiring manual developer intervention.
2. Background removal is available in the normal user path, not just raw-image fallback.
3. Initial add, reload, folio navigation, save/load, export, and publish all work.
4. Failure mode is understandable and supportable.
5. The route can be re-disabled quickly with `WEB_PUZZLE_ENABLED=false`.

---

## 11. Practical Next Steps

### If the team wants the fastest viable return

Build and ship a small packaged localhost helper.

### If the team wants the strongest website architecture

Design an owned image-storage / derivative-cache plan and solve legal + ingestion questions.

### If the team wants to keep the feature hidden for now

Leave `WEB_PUZZLE_ENABLED=false` and use this document as the starting point for the next iteration.

---

## 12. Key Takeaway

The web puzzle is **not blocked by canvas manipulation, persistence, export, or publishing**. Those areas are largely implemented.

It is blocked by one architectural truth:

> The web version needs a production-safe way to obtain processable image bytes for background removal, and the current server cannot get them reliably from NLI.

Everything else flows from that.
