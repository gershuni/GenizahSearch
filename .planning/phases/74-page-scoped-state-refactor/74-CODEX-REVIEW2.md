---
status: issues_found
reviewer: codex-cli 0.117.0
depth: standard
phase: 74-page-scoped-state-refactor
reviewed_at: 2026-04-17
findings:
  warning: 3
  info: 1
  total: 4
---

# Phase 74 — Codex External Code Review

## WARNING — Legacy snapshots destroyed on first post-upgrade load

`restore_browse_snapshot()` treats a missing version stamp as stale and immediately calls `clear_browse_snapshot()` (`web/pages/browse_state.py:122`); that path is live from `web/pages/browse.py:4452`. `restore_search_snapshot()` has the same logic at `web/pages/search_state.py:255`. Pre-74 browsers have no stamp, so existing `browse_position` / `reading_desk_state` data is wiped once instead of being migrated.

Suggested fix: on missing stamp, adopt the legacy payload once (re-stamp), then treat future version mismatches as stale.

## WARNING — `clear_browse_snapshot()` too broad for new call sites

It deletes `browse_position` as well as `reading_desk_state` (`web/pages/browse_state.py:187`), and Phase 74 now uses it when exiting joined view (`web/pages/browse.py:975`) and when clearing a stale desk during explicit `?sys_id=` navigation (`web/pages/browse.py:4469`). Pre-refactor, both paths only cleared the desk. This now erases the user's last single-page position unexpectedly.

Suggested fix: add `keep_position=True` parameter (or split into `clear_reading_desk_only`) and use it in these two sites.

## WARNING — `clear_search_snapshot()` too broad for `_clear_all_adv_filters()`

The "Clear All" handler now calls the full snapshot reset (`web/pages/search.py:784`, `web/pages/search.py:820`), and that helper clears persisted results, exclusions, printed filter, refinement chain, and `search_all_terms_filter` (`web/pages/search_state.py:344`). `_clear_all_adv_filters()` only resets pre-search filter state in memory, so storage diverges from the live page and a reload restores a different state than the one still on screen.

The broad clear is appropriate for New Search (`web/pages/search.py:1981`), not here.

## INFO — Test coverage misses the risky branches

- `tests/test_search_state.py` only checks a stamped mismatch (`999`) at `tests/test_search_state.py:71`; missing branch: missing-stamp path.
- No direct coverage for `browse_state` helper behavior.
- The new E2E test skips if the stable shelfmark selector disappears instead of failing (`tests/e2e/test_browse_flow.py:131,140`).

## Sweep verification (PASS)

No remaining `on_click=lambda: asyncio.ensure_future(...)` in `web/pages/search.py`, `web/pages/browse.py`, `web/pages/search_results.py`, or `web/components/filter_panel.py`. `web/browse_bootstrap.py` is pure.

## Comparison to internal REVIEW.md

- Agree with WR-02 (`_clear_all_adv_filters` state divergence) and WR-03 (missing-stamp wipe).
- Disagree (partially) with WR-01: `restore_search_snapshot` ownership mismatch is not a live user-facing regression yet because `search.py` still restores those keys directly. The bigger live miss is the `_clear_all_adv_filters()` misuse.
- New finding not in internal review: `clear_browse_snapshot()` over-broad scope at the two specific call sites in `browse.py:975` and `browse.py:4469`.
