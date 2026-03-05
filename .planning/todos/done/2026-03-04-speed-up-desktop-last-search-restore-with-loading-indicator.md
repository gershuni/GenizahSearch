---
created: 2026-03-04T18:45:13.789Z
title: Speed up desktop last-search restore with loading indicator
area: desktop
files:
  - genizah_app.py
  - gui_threads.py
---

## Problem

Loading the last search when opening the desktop app takes ~10 seconds. This is a poor first impression — the app feels frozen on startup. Two issues:

1. **Performance**: The restore is too slow (~10s). Needs profiling to identify bottleneck (re-running search? loading results? enrichment? rendering?).
2. **UX feedback**: No visual indicator that loading is in progress — user sees a blank/stale UI with no explanation.

## Solution

a) **Profile and optimize** the restore path — consider caching serialized results instead of re-running the search, lazy-load enrichment data, or defer heavy rendering.
b) **Add loading visualization** — spinner, progress bar, or skeleton UI with "Restoring last search..." message while results load.

TBD: Need to trace the exact restore code path in genizah_app.py to identify what's slow.
