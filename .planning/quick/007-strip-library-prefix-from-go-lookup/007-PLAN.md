# Quick Task 007: Strip library prefix from Go button lookup

## Problem

In the desktop browse tab, after navigating to a manuscript (e.g., "T-S NS 23.23"), the input field gets updated to include the library code prefix (e.g., "CUL T-S NS 23.23"). When the user then edits the shelfmark number and presses Go, `resolve_system_by_shelfmark()` fails because it doesn't recognize the library code prefix.

## Fix

Add library code prefix stripping at the top of `MetadataManager.resolve_system_by_shelfmark()` in `genizah_core.py`. Before normalizing the query, check if it starts with any known `LIBRARY_CODES` key followed by a space, and strip it.

## Tasks

1. **Edit `resolve_system_by_shelfmark()`** — Add prefix stripping logic before normalization (genizah_core.py ~line 3804)

## Scope

- Single function edit in genizah_core.py
- Desktop-only bug (web doesn't prepend library code to input field)
- Both apps share the same core function, so fix is universal
