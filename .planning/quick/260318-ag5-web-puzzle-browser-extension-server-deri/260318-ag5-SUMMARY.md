# Quick Task 260318-ag5: Web Puzzle Browser Extension + Server Derivative Cache

## Status: COMPLETE

## What Was Built

### Browser Extension (`extension/`)
- Chrome MV3 manifest with NLI IIIF host permissions
- Background service worker fetches images as ArrayBuffer (binary, not base64)
- Content script bridges page↔background via postMessage, injects detection meta tag
- Firefox compatible via `browser`/`chrome` API detection

### HMAC Upload Token System (`web/puzzle_tokens.py`)
- Server issues signed tokens on cache miss (5-min expiry, fl_id-bound)
- Upload endpoints require valid token — prevents cache poisoning
- Constant-time signature comparison

### Hardened API Endpoints (`web/api.py`)
- `GET /api/puzzle_image` — cache-first, returns upload token on 404
- `POST /api/puzzle_process` — token-verified, rate-limited (60/min/IP), size+type validated
- `POST /api/puzzle_upload_derivative` — for desktop/external processed image uploads

### Cache Versioning (`shared/puzzle_image_service.py`)
- `PROCESSING_VERSION = 'v3'` in cache keys for automatic invalidation
- Backward compat: falls back to legacy (unversioned) cache paths
- `save_derivative_to_cache()` helper for external contributions

### Unified Image Loader (`web/pages/puzzle.py`)
- Single `_loadImageWithFallbacks()` function for ALL image paths
- Fallback chain: server cache → extension → localhost helper → direct NLI (degraded)
- All 4 paths use it: addFragment, navigateFolio, _reloadFragment, saved doc restore

### Extension UX
- Install banner (bilingual, dismissible) when extension not detected
- Green "Extension active" indicator when detected
- Banner created via DOM API (createElement) to avoid Python/JS escaping issues

## Commits
| Hash | Description |
|------|-------------|
| 3bd6da32 | Browser extension, HMAC tokens, hardened API, cache versioning |
| c99ef215 | Unified image loader, extension integration, install banner |
| acc20df5 | Bugfixes: manifest patterns, cache compat, banner escaping |

## Bugs Fixed During Testing
1. Chrome manifest: `*://localhost:*` invalid → `http://localhost/*`
2. `state.language` AttributeError → `get_language()`
3. `_hasExtension()` caching stale results → check DOM every call
4. `ui.html()` shadow DOM isolation → `ui.element('div')` + JS createElement
5. `\'` in Python `'''` string → syntax error → use createElement API instead
6. Cache key `_v3` suffix invalidated all existing cache → fallback to legacy paths

## Feature Flag
`WEB_PUZZLE_ENABLED` remains `false` by default. Staged rollout as planned.
