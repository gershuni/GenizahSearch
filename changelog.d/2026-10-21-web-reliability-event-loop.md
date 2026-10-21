## [Unreleased] -- A freer event loop: saves off the loop, lighter /browse page turns, one GC freeze, spread-out reloads after a restart (web, branch `ccr-cb9f4b76-qh2uvw`)

The website serves every visitor from one event loop, and the 2026-10-20
reliability review measured four things that held it: Supabase writes made on
the loop, /browse rebuilding its whole content area two or three times per page
turn, full garbage collections walking the startup data, and the reload burst
open tabs set off after a restart. Each is changed here; none of it touches
search.

### Fixed
- **Saving no longer stops the site for everyone else.** List writes (create,
  rename, move, delete, restore, empty trash, add, remove, notes, tags,
  projects), comments, and /browse corrections and drafts ran their Supabase
  round trip (~150 ms, Oregon to Frankfurt) on the event loop. They now run in
  a worker with the visitor's own client, built on the loop. "Add all results to
  a list" is one insert instead of one per row. A second click while a save is
  in flight is ignored, so a double click no longer writes twice.
- **A /browse page turn rebuilds the page twice, not three or four times.** The
  spinner no longer clears the page: the previous page stays under it, dimmed
  and inert, and the spinner floats over it without moving it. Phase B fills its
  placeholders once (the version selector and its default-source choice ran
  twice), and the whole manuscript's letters are counted once. Measured on the
  test page: opening /browse cleared the content area 4 times, now 2; a Next
  click 3 times, now 2. The line-number button no longer disappears when Phase
  B lands.
- **Element builds about a third faster on NiceGUI 3.18.** NiceGUI asked
  `inspect.signature` for the same three functions on every element it built;
  the answer is now remembered (1,204 elements: 102-110 ms to 64-67 ms median on
  the dev box).
- **Full garbage collections stop walking the startup data.** Once the startup
  loaders finish, the site runs `gc.collect(); gc.freeze()` once, so the 0.6-0.84
  M startup objects leave the collector (a full collection took 465-534 ms on
  the loop in a real /browse run). `GENIZAH_GC_FREEZE=0` turns it off;
  `/_internal/objgraph` reports the freeze count and can unfreeze for a leak
  hunt (docs/guides/ENV_VARS.md).

### Added
- **Reconnect backoff.** After a restart every open tab used to reload within
  the same 5 seconds. A small script in every page's head now spreads socket.io's
  reconnects (up to 30 s apart) and delays the reload after a refused handshake
  or a connect timeout by a jittered 1.5 s, doubling per consecutive reload up to
  60 s; a background tab waits until it is viewed. Settings:
  `RECONNECT_BACKOFF_ENABLED`, `RECONNECT_BACKOFF_BASE_MS`,
  `RECONNECT_BACKOFF_CAP_MS`, `RECONNECT_DELAY_MAX_MS`. Handshake outcomes are
  counted in `/_internal/memstat` under `handshakes` (a warning when more than
  30 unknown clients arrive in a minute), and the browser sends a PostHog
  `reload_scheduled` event.

### Not yet
- Phase B of a /browse page still rebuilds the whole content area once, instead
  of filling only its own containers.
- The /corrections page's save, approve and reject, the lists' `*_sync` helpers
  and the comment dialog's shelfmark picker still call Supabase on the loop.
- None of the above has been measured on the server yet. After the deploy: the
  `[init] gc.freeze:` line, perf_watch's loop-lag lines, and, during the next
  restart, `handshakes` in `/_internal/memstat` and `/browse` latency.
