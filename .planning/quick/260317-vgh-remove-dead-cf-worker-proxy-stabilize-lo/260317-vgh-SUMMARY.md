# Quick Task 260317-vgh: Remove Dead CF Worker Proxy, Stabilize Localhost Helper

## What Changed

### Problem
NLI blocks all datacenter/CDN IPs — both AWS EC2 and Cloudflare Worker return HTTP 500. The Cloudflare Worker proxy added in 260317-tt9 is dead code.

### Solution
- **Reverted** proxy code from `shared/puzzle_image_service.py` (direct NLI fetch only, no proxy fallback)
- **Removed** `/iiif-proxy/` from all 3 JS fallback chains in `web/pages/puzzle.py`
- **Stabilized** localhost helper as the primary fallback between server and direct NLI
- **Updated** `docs/plans/PUZZLE_IIIF_SERVER_SIDE_PROCESSING.md` to mark CF Worker as tested and failed

### New Fallback Chain (all 3 paths)
```
/api/puzzle_image (server fetch)
  → localhost helper (http://127.0.0.1:43111, opt-in)
    → direct NLI <img> (no bg removal, display-only)
```

### Files Modified
| File | Change |
|------|--------|
| `shared/puzzle_image_service.py` | Removed `NLI_IIIF_PROXY`, `PUZZLE_IIIF_MODE`, simplified `_fetch_iiif_image` to direct-only |
| `web/api.py` | Updated docstrings for puzzle_image and puzzle_process endpoints |
| `web/pages/puzzle.py` | Removed `/iiif-proxy/` from addFragment, navigateFolio, _reloadFragment fallback chains |
| `docs/plans/PUZZLE_IIIF_SERVER_SIDE_PROCESSING.md` | Marked CF Worker as tested/failed, documented localhost helper approach |

### How to Test
1. **Local dev** (no change needed): puzzle works as before via direct NLI fetch
2. **Production with localhost helper**:
   - Start helper: `python scripts/puzzle_local_helper.py`
   - Open: `https://genizahsearch.com/puzzle?local_helper=1`
   - Add a fragment — should load with background removal via localhost
3. **Production without helper**: fragments load via direct NLI `<img>` (no bg removal, degraded but functional)

## Commit
`4e4d9b94`
