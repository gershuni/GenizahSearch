# Domain Hierarchy Performance Optimization

## Status: RESOLVED (2026-03-01)

## What Works Now
- **Tree load is instant** — warmup QThread + disk cache + all indexes moved out of __init__
- **"Loading..." shows** while tree data loads
- **Domain click is async** — shows "Loading..." immediately, UI stays responsive
- **All catalog actions are async** — domain, author, work select, pagination, text filter, date filter, chip removal
- **sip import fixed** — `from PyQt6 import sip`
- **thread_safe=True** on desktop FjmsService singleton

## Fixes Applied (2026-03-01)

### Fix 1: Async UI (no more freezes)
1. **Module-level `_CatalogRefreshWorker(QThread)`** at module scope with `done = pyqtSignal(object)`. Fixes signal delivery — locally-defined QThread classes can't register signals in PyQt6.
2. **`_catalog_start_async_refresh(refresh_authors, refresh_works)`** uses the module-level class with flags.
3. **All 13 callers wired up** — sync `_catalog_refresh()` etc. replaced with async.

### Fix 2: SQLite threading
- Changed `FjmsService` default to `thread_safe=True` (both `__init__` and `get_fjms_service`). Safe: read-only connection + threading locks on caches.

### Fix 3: Query optimization (35s -> 0.8s)
- **Replaced `INNER JOIN domains + OR`** with `IN (SELECT ... UNION SELECT ...)` subquery — enables proper index utilization.
- **Pre-dedup catalog in CTE** (`WITH dc AS (SELECT DISTINCT ...)`) then `COUNT(*)` instead of expensive `COUNT(DISTINCT AlmaId)` on 685K-row table with 3x duplicates.
- Benchmarks for "Halakhic Literature" domain (20,951 manuscripts):
  - Authors: 30s -> 0.27s (111x faster)
  - Works: 4.4s -> 0.29s (15x faster)
  - Results count+page: 0.24s -> 0.25s
  - **Total: 35s -> 0.83s**

### Fix 4: Cache version
- Added `_BROWSE_CACHE_VERSION = 2` to invalidate stale disk caches (pre-3-level-nesting).

## Architecture Summary
- `shared/fjms_service.py`: `pre_warm_caches()` creates ALL indexes + disk cache. `__init__` only opens DB.
- `genizah_app.py`: warmup QThread in `init_ui()` with `thread_safe=True`. Tree load via `warmup.finished` signal → `_catalog_load_tree_from_cache`. All catalog actions async via `_CatalogRefreshWorker`.
- `web/main.py`: `pre_warm_caches()` in `initialize_engine()`.

## Files Changed
- `shared/fjms_service.py` — disk cache, pre_warm_caches (all indexes), NOT EXISTS query, composite indexes, `_unclassified_cache`, canonical domain ordering
- `genizah_app.py` — module-level `_CatalogRefreshWorker`, async tree load, warmup QThread, sip fix, all catalog browse actions async
- `web/main.py` — pre_warm_caches in initialize_engine
- `web/pages/catalog_browse.py` — sub-sub-domain expansion panels
- `web/pages/search.py` — sub-sub-domain hierarchy in domain filter
