---
phase: 118-joins-entry-full-builders
plan: "02"
subsystem: web/joins-lab
tags: [anc-04, anc-05, security, multitenant, known-joins, community-joins, render-component, wave-1]
dependency_graph:
  requires:
    - plans/118-01 (RED test stubs for ANC-04/ANC-05 + schema probe)
  provides:
    - web/components/joins_panel.py: confirmed_only path (status='confirmed' + ':confirmed' cache key + community merge)
    - web/components/known_joins_group.py: source-badged known-joins group renderer
  affects:
    - plans/118-04 (anchor pane caller — drops render_known_joins_group into anchor pane)
tech_stack:
  added: []
  patterns:
    - Isolated cache key suffix (':confirmed') for multitenant cache safety
    - App-layer status='confirmed' filter as the D-17 ANC-05 mechanism (DB RLS is USING(true))
    - Community puzzle joins gated to confirmed_only (Lab) path only — browse dialog unchanged
    - Pure render component (no per-user state, no data fetching) for testability
key_files:
  created:
    - web/components/known_joins_group.py
  modified:
    - web/components/joins_panel.py
decisions:
  - "ANC-05 primary mechanism: get_fragment_joins(status='confirmed') + ':confirmed' cache key (not application-layer source filtering) — status column confirmed present in supabase_setup.sql:162 and already applied at web/supabase_client.py:1593-1594"
  - "Community merge gated to confirmed_only=True (Lab path) so browse dialog community section (create_joins_dialog :671-742) is structurally unaffected — T-118-09: only is_published=True rows, already publicly visible"
  - "known_joins_group.py is a pure renderer — caller (Plan 04 anchor pane) owns the off-loop fetch; component holds zero per-user state (T-118-02 satisfied; test_no_raw_storage_access.py allowlist stays [])"
  - "invalidate_joins_cache prefix match (doc:X:) already covers doc:X:pgp:N:confirmed — no change needed"
metrics:
  duration: "~25min"
  completed: "2026-06-18"
  tasks: 2
  files: 2
---

# Phase 118 Plan 02: confirmed-only path + known-joins renderer Summary

**One-liner:** Multitenant-safe confirmed-only joins fetch (status='confirmed' + ':confirmed' cache key + community merge) and a source-badged known-joins group render component (PGP/FJMS/user/community with re-anchor + browse-open actions).

---

## What Was Built

### Task 1: confirmed-only path in `fetch_connected_fragments` (commit `08d446d4`)

Extended `web/components/joins_panel.py:fetch_connected_fragments` with `confirmed_only: bool = False`:

**ANC-05 / T-118-01 (cross-user join leak prevention):**
- `confirmed_only=True` uses cache key `doc:{id}:pgp:{pgpid}:confirmed` (or `shelf:{sm}:...:confirmed`) — isolated from the browse-dialog full-joins cache key `doc:{id}:pgp:{pgpid}`.
- `confirmed_only=True` passes `status='confirmed'` to `get_fragment_joins(...)` — the existing supported parameter at `web/supabase_client.py:1593-1594`. RLS is `USING(true)` so all rows are publicly readable; the app-layer filter is the sole D-17 mechanism that prevents any user's `proposed` joins from appearing in another session's Lab group.
- `confirmed_only=False` (default) path is completely unchanged: no status filter, uses the existing unconfirmed cache key.
- `invalidate_joins_cache(document_id=X)` was already correct — its prefix match `doc:X:` covers `doc:X:pgp:N:confirmed`.

**ANC-04 / D-15 (community source merge):**
- Lab path (`confirmed_only=True`) fetches `published_join_fragments` + `published_joins` (same query as `create_joins_dialog` lines 671-742), extracts member shelfmarks, and either merges `'community'` into an existing formatted join's `sources` list (mirror of the FJMS dedup pattern) or appends a new join with `sources=['community']`.
- Wrapped in `try/except` with `logger.error(...)` for graceful degradation.
- Browse path (`confirmed_only=False`) is unaffected — community still rendered separately in `create_joins_dialog`.

### Task 2: `web/components/known_joins_group.py` (commit `7cfb3531`)

New pure render component with two exports:

**`badge_for_source(source: str) -> tuple[str, str]`:**
- Returns `(label, css_hex_color)` per the UI-SPEC source badge color map:
  - `'PGP'` → `('#1d4ed8'` blue-700, 7.0:1 contrast)
  - `'FJMS'` → `'#7e22ce'` (purple-700, 7.2:1 contrast)
  - `'user'` → `'#047857'` (emerald-700, 5.4:1 contrast)
  - `'community'` → `'#525252'` (neutral-600, sufficient contrast)
- Desktop `join_workbench.py:166-179` parity confirmed.

**`render_known_joins_group(data, current_shelfmark, current_sys_id, on_reanchor, on_open_browse)`:**
- Renders into the current NiceGUI parent slot (no page-level container).
- `ui.expansion` header: `tr('Known Joins') + f' ({count})'`, expanded when count > 0.
- 36px compact member rows: source badge(s) + shelfmark (13px/700, RTL-aware) + `push_pin` re-anchor icon + `open_in_new` browse icon.
- Empty state: `tr('No known joins')` + muted `tr('Only confirmed public joins are shown')` (ANC-05 disclosure).
- All strings via `tr()` — 5 `tr()` calls (meets ≥5 requirement).
- Zero `app.storage.user` access (Phase 87 invariant preserved).

---

## Test Results

| Suite | Before | After | Status |
|-------|--------|-------|--------|
| test_joins_anc05_rls.py (5 tests) | 2 GREEN / 3 RED | 5/5 GREEN | PASS |
| test_known_joins_group.py (5 tests) | 4 GREEN / 1 RED | 5/5 GREEN | PASS |
| test_no_raw_storage_access.py (6 tests) | 6 GREEN | 6/6 GREEN | PASS |
| test_joins_lab.py + test_joins_lab_page.py (106 tests) | 106 GREEN | 106/106 GREEN | PASS |
| **Total** | **118 tests** | **122/122 GREEN** | **PASS** |

---

## Deviations from Plan

None — plan executed exactly as written.

The `invalidate_joins_cache` prefix match was verified correct (already covers `:confirmed` keys via `doc:{id}:` prefix) — no change needed, as the plan directed ("verify … if not, extend it").

---

## Known Stubs

None. Both modified/created files are fully functional:
- `joins_panel.py` confirmed_only path fetches live Supabase data.
- `known_joins_group.py` is a pure renderer that consumes data from the caller — no stubs.

---

## Threat Surface Scan

No new network endpoints, auth paths, or schema changes introduced.

The community merge in `fetch_connected_fragments` reads from `published_join_fragments` and `published_joins` — the same tables already read by `create_joins_dialog`. Both queries are read-only and gate on `is_published=True` (T-118-09: accepted). No new trust boundary exposure.

`known_joins_group.py` introduces no network surface and holds no per-user state (T-118-02).

---

## Self-Check: PASSED

Files verified to exist:
- web/components/joins_panel.py — FOUND (confirmed_only param + :confirmed cache key + community merge)
- web/components/known_joins_group.py — FOUND

Commits verified:
- 08d446d4: feat(118-02): confirmed_only path + community merge in fetch_connected_fragments (ANC-04/ANC-05)
- 7cfb3531: feat(118-02): build known_joins_group render component (ANC-04, D-15/D-16)
