---
phase: 117-vertical-spine
plan: "05"
subsystem: web-joins-lab
tags: [candidate-grid, thumbnail, oxford-fork, proxy, nicegui, read-only, cnd-02]
dependency_graph:
  requires:
    - 117-01  # WebSearchExecutor (produces Candidate objects)
    - 117-02  # safe_storage schema (joins_lab namespace)
    - 117-03  # anchor viewer extraction (browse infrastructure)
    - 117-04  # joins_lab page route (consumes candidate_grid)
  provides:
    - web/components/candidate_grid.py  # create_candidate_grid, build_thumbnail_url, build_browse_url
    - tests/test_candidate_grid.py
  affects:
    - web/components/__init__.py  # not updated yet; component importable standalone
tech_stack:
  added: []
  patterns:
    - NiceGUI ui.card / ui.grid / ui.image / ui.link read-only card grid
    - Proxy-thumbnail derivation (NLI + Oxford fork) mirroring search_results.py:645-681
    - Bodleian direct URL (documented MEDIUM-5 exception) vs /api/oxford_image fallback
    - is_synthetic_sys_id guard (placeholder only for synthetic, no proxy attempt)
    - Headless test pattern — pure helper functions tested without NiceGUI runtime
key_files:
  created:
    - web/components/candidate_grid.py
    - tests/test_candidate_grid.py
  modified: []
decisions:
  - "build_thumbnail_url imports is_oxford_manuscript + get_oxford_direct_image_url from web.services (not web.pages.browse) — lighter import, avoids pulling the heavy browse page module into a small component (Codex round-3 LOW / REVIEWS.md)"
  - "is_rtl() not used in component — RTL direction applied via CSS 'direction: rtl' inline; removed unused import (ruff F401 deviation fix)"
  - "Tests use the real web.* package (not stubs) since web.translations + web.services are thin pure-Python modules with no import-time side effects"
metrics:
  duration_minutes: 8
  completed_date: "2026-06-17"
  tasks_completed: 2
  files_created: 2
  files_modified: 0
requirements: [CND-02]
---

# Phase 117 Plan 05: Candidate Grid Component — Summary

Read-only candidate grid component (`web/components/candidate_grid.py`) implementing CND-02: renders a list of `shared.joins_lab.Candidate` objects as cards with a proxy-derived thumbnail, shelfmark, library chip, title, and "View in Browse" link.

## What Was Built

**`web/components/candidate_grid.py`** — new thin component providing:

- `create_candidate_grid(candidates, *, on_browse_click=None)` — responsive 2-column grid (single-column below 640px), section header with count, empty-state message when no candidates.
- `build_thumbnail_url(sys_id, page, shelfmark='', library_code='')` — proxy thumbnail derivation mirroring `search_results.py:645-681` in full (MEDIUM-6): NLI default proxy, Oxford fork (Bodleian direct or `/api/oxford_image`), synthetic sys_id → `None`.
- `build_browse_url(cand)` — `/browse?sys_id=...&page=N` URL builder.
- `_truncate_title(title, max_chars=80)` — title truncation with ellipsis.

**`tests/test_candidate_grid.py`** — 38 headless tests covering:
- NLI proxy URL form (page/no-page, page_idx arithmetic)
- Synthetic sys_id → `None` (placeholder path)
- Oxford fork: Bodleian direct URL or `/api/oxford_image` — NOT the NLI proxy
- No `iiif.nli.org.il` in any URL
- `build_browse_url` with/without page
- Title truncation
- Empty-state and tr() key assertions
- Library chip falsy guard
- Threat boundary static checks (no `handleImageError`, no raw storage user access)

## Task Completion

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | create_candidate_grid component | c91a15c2 | web/components/candidate_grid.py |
| 1 (ruff fix) | Remove unused is_rtl import | 76c52cbc | web/components/candidate_grid.py |
| 2 | Candidate grid render tests (headless) | 1e6b006e | tests/test_candidate_grid.py |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Unused import `is_rtl` (ruff F401)**
- **Found during:** Post-Task-1 ruff check
- **Issue:** `is_rtl` was imported from `web.translations` but never called; RTL direction is applied via `style('direction: rtl')` inline CSS, no Python branching needed.
- **Fix:** Removed from the import line. No functional change.
- **Files modified:** `web/components/candidate_grid.py`
- **Commit:** 76c52cbc

## Known Stubs

None. The "placeholder" elements in the component are intentional design fallbacks (synthetic sys_id → no proxy image, image onerror → placeholder box), not data stubs. All candidate data renders from actual Candidate field values.

## Threat Flags

None. No new network endpoints, no new auth paths, no schema changes. The component reads only from Candidate fields (engine-derived corpus data) and emits only proxy URLs — the trust boundary is unchanged from the plan's threat model.

## Verification

- `pytest tests/test_candidate_grid.py -x -q` — 38 passed
- `pytest tests/test_no_raw_storage_access.py -x -q` — 6 passed (Phase-87 CI guard stays green)
- `python -m ruff check web/components/candidate_grid.py tests/test_candidate_grid.py` — all checks passed
- `grep -nE "app\.storage\.user" web/components/candidate_grid.py` — no code usage (only docstring prohibition notes)
- `grep -nE "iiif\.nli\.org\.il|handleImageError" web/components/candidate_grid.py` — no code usage
- `grep -n "/api/nli_image_by_sysid/" web/components/candidate_grid.py` — present (line 95, the NLI proxy return)
- `grep -nE "is_oxford_manuscript|get_oxford_direct_image_url|/api/oxford_image" web/components/candidate_grid.py` — Oxford fork present (MEDIUM-6)

## Self-Check

- [x] `web/components/candidate_grid.py` exists
- [x] `tests/test_candidate_grid.py` exists
- [x] Commits c91a15c2, 1e6b006e, 76c52cbc exist in git log
- [x] All 38 tests pass
- [x] Ruff clean

## Self-Check: PASSED
