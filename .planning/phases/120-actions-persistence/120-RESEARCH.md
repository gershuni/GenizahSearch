# Phase 120: Actions & Persistence - Research

**Researched:** 2026-06-20
**Domain:** NiceGUI web app — Supabase community writes, multi-fragment puzzle staging, safe_storage persistence, fire-and-forget async lifecycle hardening
**Confidence:** HIGH — all critical unknowns resolved against live code; no training-data-only claims in load-bearing sections

> **⚠ D-02 USER OVERRIDE (2026-06-20) — READ FIRST. This research recommends inserting
> `status='confirmed'`; the user SUPERSEDED that.** ACT-01 must keep the inserted status as the
> `'proposed'` DB default (do NOT pass `status='confirmed'` — a user-added join is an unmoderated
> claim). Instead, make the **Lab known-joins group SHOW proposed joins** by relaxing/removing the
> `confirmed_only=True` filter (`joins_lab.py:1150` → `confirmed_only=False`, or equivalent), matching
> `/browse`, plus `force_refresh` the cache. See `120-CONTEXT.md` D-02 (authoritative). Every
> "insert `status='confirmed'`" instruction below is OBSOLETE; read it as "leave status `'proposed'`
> and show proposed in the Lab."

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Add-as-Join writes via existing `create_fragment_join` (login-gated); does NOT write to `joins.db`.
- **D-02:** Match the LIVE behavior — new join must appear in known-joins group right after adding.
  **[USER OVERRIDE 2026-06-20]** Keep inserted status `'proposed'` (do NOT mark `'confirmed'`); instead
  show proposed joins in the Lab known-joins group (relax `confirmed_only`) + `force_refresh`. See the
  ⚠ banner above and CONTEXT D-02. (The "insert `status='confirmed'`" text below is superseded.)
- **D-03:** Add self-service "remove my join" on logged-in user's own joins; wire existing `delete_fragment_join` + RLS. Own joins only.
- **D-04:** Bulk handoff: anchor + multi-selected candidates via NEW multi-fragment staging payload/API; anchor always included.
- **D-05:** Add-to-List login-gated; operates on multi-selected candidates via `add_list_item`.
- **D-06:** Export = flat single-table CSV + XLSX; columns include triage verdict + one text page (matched page for text hits, first text page for VS-only). Off-loop batched text fetch; ~4000-char cap. Anonymous-OK.
- **D-07:** Make-an-anchor — promote any candidate/fragment to anchor; triage resets on re-anchor.
- **D-08:** Browse-in-Compare — open candidate (or anchor) in `/browse` new tab; reuse `build_browse_url`.
- **D-09:** Compare info buttons — FJMS catalog + PGP/bib metadata per pane; reuse existing dialogs.
- **D-10:** Compare image prefetch — silently preload adjacent candidates' images off-loop; bounded 5-slot pool; SEED-008 guard applies.
- **D-11:** Stop-search-with-partial-results — visible Stop button that APPLIES partials on explicit user stop; existing `_should_apply_results` guard keeps discarding partials from superseded runs.
- **D-12:** Hide VS toggle when anchor has zero VS data (off-loop probe via `_check_vs_service_available` / `get_suggestions`).
- **D-13:** Persist inputs only (anchor, builder lines + modifiers + gap + Text Position + mode, global toggles, other-side builder + combine mode, sys_id-keyed triage, active filter, view mode). NEVER persist candidate lists / `full_text` / image blobs.
- **D-14:** Restore = auto re-run + "restoring…" indicator; no opt-in prompt.
- **D-15:** After re-run, persisted triage / filter / view re-attach by `sys_id`.
- **D-16:** Clear/Reset wipes ALL Joins Lab working state; test confirms keys empty after reset.
- **D-17:** Wire "Choose anchor from my lists" authenticated picker (flat recent-items or list→fragments drill-down).
- **D-18:** Fix sign-in route bug — `joins_lab.py:1573` currently does `ui.navigate.to('/settings')`; route to `create_login_dialog()` from `web/auth_state.py:358`.
- **D-19:** Add "Open in Joins Lab" button on `/lists` — `link` icon between Browse (`menu_book`, `:694`) and Add-to-Puzzle (`extension`, `:701`); navigates to `/joins-lab?sys_id={sys_id}`.
- **D-20:** SEED-008 — wrap post-await UI mutations in `except RuntimeError: return` in `_load_known_joins` and `_do_vs_fetch_and_update`, plus audit all other fire-and-forget tasks including new D-10 prefetch.
- **D-21:** Run deferred Phase-119 verification (119-DEFERRED-VERIFICATION.md items 1–4) during 120 close-out.

### Claude's Discretion
- ACT-02 bulk staging mechanism (server-side staging key vs batched query param).
- Export per-cell text cap value + exact column ordering + CSV/XLSX UI affordance.
- "Choose from my lists" picker layout (flat recent-items vs list→fragments drill-down).
- Stop button placement/styling and "restoring…" indicator presentation.
- `link` glyph + tooltip for `/lists` "Open in Joins Lab" button.
- Compare info-button placement + whether prefetch also warms transcription text.
- Exact `joins_lab` schema additions under `schema_version` (extend v1; bump only on remove/retype).

### Deferred Ideas (OUT OF SCOPE)
- Cloud sync across devices → PST-F1 (future).
- Admin/cross-user join moderation + delete.
- Show anchor's saved `joins.db` join-documents (SEED-007 #3, declined).
- True engine-streaming partial results.
- Full i18n / RTL / Hebrew-leak audit → Phase 121.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| ACT-01 | Add candidate as join via pairwise-join path (login-gated) | D-02 [USER OVERRIDE]: keep inserted status `'proposed'` (no `status` kwarg); make the Lab show it via `confirmed_only=False` + `force_refresh`; `create_fragment_join` wired; delete via D-03 |
| ACT-02 | Add anchor + selected candidates to Fragment Puzzle via NEW bulk staging | Staging via `safe_storage` key (`puzzle_staging`) before navigation; `create_puzzle_page` reads it on load |
| ACT-03 | Add candidates to saved list and/or export the candidate set | `add_list_item` wired for Add-to-List; flat CSV/XLSX export with off-loop text fetch |
| PST-01 | Persist builder inputs + triage + filter + view; restore = auto re-run | `joins_lab_storage.py` extended under `schema_version: 1`; restore calls `execute_joins_search` |
| PST-02 | Persistence uses server-side per-browser-session `safe_user_*` (no blobs, versioned) | `web/safe_storage.py` chokepoint; `_JOINS_LAB_KEY = 'joins_lab'`; cap discipline enforced |
| PST-03 | User can clear/reset all Joins Lab working state | `clear_joins_lab_state()` already exists; extend to wipe all new keys; test asserts storage empty |
</phase_requirements>

---

## Summary

Phase 120 extends the already-shipped Joins Lab (Phases 117–119) with research-output actions and durable working-state persistence. All six major research questions have been resolved by reading the live code. The most critical finding is the **D-02 status discrepancy**: `create_fragment_join` does NOT set `status` on insert, so new joins land with `status='proposed'` (schema default); the Joins Lab known-joins group filters `confirmed_only=True` via `status='confirmed'`; therefore new joins from ACT-01 silently do NOT appear in the Lab's known-joins group, even though they DO appear immediately in `/browse` (whose path does NOT use `confirmed_only=True`). **[USER OVERRIDE 2026-06-20]** The fix is NOT to insert `status='confirmed'` (that would falsely mark an unmoderated claim). Instead, keep the inserted status `'proposed'` and set the Lab known-joins fetch to `confirmed_only=False` so proposed joins show in the Lab too — parity with `/browse` — plus `force_refresh` the cache after insert.

The persistence extension is straightforward: `web/joins_lab_storage.py` was written in Phase 117 with a forward-compatibility note explicitly inviting Phase 120 to add keys under `schema_version: 1`. The restore path wires into the existing `_bootstrap_anchor` / `execute_joins_search` pipeline already in place.

ACT-02 (bulk puzzle staging) requires a new `safe_storage` key written by the Joins Lab and read by `create_puzzle_page` on load — a clean, multitenant-safe staging mechanism consistent with Phase-87 invariants.

The SEED-007/008 + Lists integration items are small and well-understood. Every new async task (including D-10 prefetch) must carry the established `except RuntimeError: return` client-deleted guard.

**Primary recommendation:** Work in waves — SEED-008 hardening + D-11 Stop + D-18 sign-in fix first (small, unblock others), then ACT-01/D-03 + D-12 VS hide, then PST (storage extension + restore), then ACT-02/03 (bulk puzzle staging + export), then D-07..D-10 SEED-007 items + D-17 list picker, then D-19 /lists button + D-21 close-out verification.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Add-as-Join community write (ACT-01) | API / Backend (Supabase) | Frontend (login gate UI) | Scholarly community record; RLS enforces ownership |
| Remove my join (D-03) | API / Backend (Supabase) | Frontend (UI affordance) | RLS `USING (auth.uid() = user_id)` enforces self-scope |
| Bulk puzzle staging (ACT-02) | Frontend Server (SSR safe_storage) | — | Server-side per-session key; navigated page reads it on load |
| Add-to-List (ACT-03) | API / Backend (Supabase) | Frontend (login gate + list picker) | `list_items` table; user-scoped via RLS |
| Export CSV/XLSX (ACT-03) | Frontend Server (SSR) | — | Local download; off-loop text fetch; no cloud write |
| Persistence read/write (PST-01/02) | Frontend Server (safe_storage user) | — | `app.storage.user` per NiceGUI session cookie |
| Stop-with-partials (D-11) | Frontend Server (async task management) | — | Generation counter already server-side |
| Compare image prefetch (D-10) | Frontend Server (off-loop run.io_bound) | CDN/proxy | Images via per-provider proxy + Phase-98 breaker |
| VS toggle hide (D-12) | Frontend Server (off-loop probe) | — | Local SQLite read; no network |
| Lists → Joins Lab button (D-19) | Frontend Server (SSR render) | — | Static link render in `/lists` page |
| Sign-in flow fix (D-18) | Frontend Server (NiceGUI dialog) | — | `create_login_dialog()` is an in-process dialog |

---

## Standard Stack

### Core (no new packages — all existing)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| NiceGUI | existing | UI + safe_storage | Phase 117–119 baseline |
| `web/supabase_client.py` | existing | `create_fragment_join`, `delete_fragment_join`, `add_list_item` | Existing wrappers; correct auth client selection |
| `web/safe_storage.py` | existing | Per-session persistence chokepoint | Phase-87 CI guard invariant |
| `web/joins_lab_storage.py` | existing | `joins_lab` versioned schema | Phase-117 forward-compat stub |
| `openpyxl` | existing | XLSX export | Already used by `shared/export_dossier.py` |
| `asyncio` + `nicegui.run` | existing | Off-loop dispatch | CI-guarded by `test_joins_lab_off_loop.py` |

### Supporting (no new installs)
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `shared/export_dossier.py` | existing | Column/header helpers | Reuse for flat-sheet export column naming convention |
| `shared/visual_similarity_service.py` | existing | VS-availability probe | D-12 `_check_vs_service_available` already module-level |
| `web/components/candidate_grid.py` `build_browse_url` | existing | D-08 Browse-in-Compare URL | Already exported from candidate_grid |
| `web/auth_state.py` `create_login_dialog` | existing | D-18 login dialog | The canonical in-app login dialog |

**No new packages to install.** Phase 120 is purely wiring and extension of existing code.

---

## Package Legitimacy Audit

No new packages are installed in Phase 120. All work rides existing dependencies.

**Packages removed due to slopcheck [SLOP] verdict:** none  
**Packages flagged as suspicious [SUS]:** none

---

## Architecture Patterns

### System Architecture Diagram

```
User clicks "Add as Join"
    └─► login gate (GlobalAuthState.is_logged_in())
        ├─► anon: create_login_dialog().open()   [D-18 fix: was /settings navigate]
        └─► authed: create_fragment_join(...)  [D-02 OVERRIDE: NO status kwarg → stays 'proposed']
                └─► Supabase fragment_joins table
                    └─► _load_known_joins(force_refresh=True) with confirmed_only=False → re-render

User clicks "Add to Puzzle"
    └─► safe_user_set('puzzle_staging', {anchor_sys_id, candidate_sys_ids[]})
        └─► ui.navigate.to('/puzzle')
            └─► puzzle_page_route(add=None)
                └─► create_puzzle_page reads safe_user_get('puzzle_staging')
                    └─► clears key, loops _add_fragment_by_sys_id for each

User clicks Stop (D-11)
    └─► sets _stop_requested['value'] = True   [new flag]
        └─► _make_progress_cb: raises InterruptedError
            └─► execute_search returns partial results
                └─► _should_apply_results: True (gen still current)
                    └─► render partials

Page refresh / navigate-back → PST restore
    └─► _bootstrap_anchor reads read_joins_lab_state()
        └─► finds persisted inputs → shows "restoring…" indicator
            └─► load_anchor(sys_id) + execute_joins_search()
                └─► after results: re-attach triage/filter/view by sys_id
```

### Recommended Project Structure (extension only)
```
web/
├── joins_lab_storage.py     # extend: add write_full_state(), read_full_state()
├── pages/
│   ├── joins_lab.py         # extend: Stop button, PST restore, actions, SEED-008/007
│   └── lists.py             # extend: "Open in Joins Lab" button (D-19)
├── components/
│   ├── known_joins_group.py # extend: "remove my join" affordance (D-03)
│   └── compare_modal.py     # extend: Browse-in-Compare (D-08), info buttons (D-09), prefetch (D-10)
```

### Pattern 1: Insert as 'proposed' + show proposed in the Lab (D-02) [USER OVERRIDE 2026-06-20]
**What:** Do NOT pass `status` — let the insert keep the `'proposed'` DB default. Make ACT-01 joins
appear in the Lab's known-joins group by flipping that fetch to `confirmed_only=False` (parity with
`/browse`), then `force_refresh` the cache.
**When to use:** Every `create_fragment_join` call from ACT-01, plus the Lab known-joins fetch.
**Example:**
```python
# Source: web/supabase_client.py:1639-1651 (data dict — NO status key)
data = {
    'user_id': user_id,
    'fragment_a_sys_id': anchor_sys_id,
    'fragment_a_shelfmark': anchor_shelfmark,
    'fragment_b_sys_id': candidate_sys_id,
    'fragment_b_shelfmark': candidate_shelfmark,
    'join_type': 'uncertain',  # DB default; can expose in UI later
    'confidence': 'possible',
    # NO 'status' key → stays 'proposed' (DB default). Do NOT set 'confirmed'.
}
# Then: Lab known-joins fetch uses confirmed_only=False so this proposed join shows.
```

### Pattern 2: SEED-008 fire-and-forget guard
**What:** Wrap ALL post-await UI mutations in `except RuntimeError: return`.
**When to use:** Every `asyncio.ensure_future(coroutine)` that does `.clear()` / `render` after any `await`.
**Example:**
```python
# Source: web/pages/joins_lab.py:2122, web/components/joins_panel.py:512 (precedents)
try:
    known_joins_container.clear()
    with known_joins_container:
        render_known_joins_group(...)
except RuntimeError:
    return  # client/tab deleted mid-fetch
```

### Pattern 3: Bulk staging via safe_storage (ACT-02)
**What:** Write a staging key before navigating to `/puzzle`; puzzle page reads + clears it on load.
**When to use:** ACT-02 bulk Add-to-Puzzle.
**Example:**
```python
# Joins Lab side (before navigate)
safe_user_set('puzzle_staging', {
    'anchor_sys_id': anchor_sys_id,
    'candidate_sys_ids': [c.sys_id for c in selected_candidates],  # max 20
})
ui.navigate.to('/puzzle')

# Puzzle page side (create_puzzle_page, after existing initial_add check)
bulk = safe_user_get('puzzle_staging', default=None)
if bulk:
    safe_user_pop('puzzle_staging', None)  # clear immediately (one-shot)
    # loop _add_fragment_by_sys_id for anchor + candidates
```

### Pattern 4: Stop-with-partials (D-11)
**What:** Distinguish user-clicked Stop (apply partials) from superseded run (discard).
**When to use:** The new Stop button in Joins Lab.
**Example:**
```python
# New flag alongside _search_generation and _is_running:
_stop_requested: dict = {'value': False}

# Stop button handler:
def _on_stop_click():
    _stop_requested['value'] = True
    _cancel_current_search()  # bumps generation + cancels task

# In _make_progress_cb — check BEFORE the generation check:
def progress_cb(arg1, arg2=None):
    if _stop_requested['value']:
        # User explicitly clicked Stop — raise InterruptedError; caller will
        # apply partials because _should_apply_results still True at this moment
        # (generation bump happens AFTER the raise from _on_stop_click).
        raise InterruptedError('joins-lab search stopped by user')
    if my_gen != gen_ref['value']:
        raise InterruptedError('joins-lab search superseded')
    ...
```

**Load-bearing nuance:** The generation counter must NOT be bumped by the Stop button before `InterruptedError` is raised by the progress_cb, because `_should_apply_results` checks the generation. The correct sequence is:
1. User clicks Stop → `_stop_requested['value'] = True`
2. Progress_cb fires on next chunk → raises `InterruptedError`
3. Core returns partial results normally
4. `_should_apply_results` returns True (generation still matches)
5. Partials applied to UI
6. THEN bump generation (or reset `_stop_requested`) so the next search starts clean

### Pattern 5: PST schema extension (D-13/D-16)
**What:** Extend the existing `joins_lab` dict in `joins_lab_storage.py` with new keys under `schema_version: 1`.
**When to use:** PST-01/02/03.
**New keys to add:**
```python
{
    'schema_version': 1,        # unchanged — no keys removed/retyped
    'anchor_sys_id': str,       # existing
    'anchor_fl_id': str,        # existing
    'anchor_volume_ie': str,    # existing
    # --- Phase 120 additions ---
    'builder_rows': list,       # [{text, modifiers_json}, ...] max 10 rows
    'builder_gap': int,         # 0-5
    'builder_text_position': str,  # 'anywhere'/'start'/'end'/'line_start'/'line_end'
    'builder_mode': str,        # 'exact'/'variants'/'fuzzy'
    'global_flex_spacing': bool,
    'global_bidirectional': bool,
    'other_side_enabled': bool,
    'other_side_rows': list,    # same shape as builder_rows
    'other_side_combine': str,  # 'narrow'/'widen'
    'triage': dict,             # {sys_id: 'yes'/'maybe'/'no'} — keyed by sys_id
    'filter_state': dict,       # compact filter snapshot (no full_text)
    'view_mode': str,           # 'grid'/'table'
}
```
**Size discipline:** `triage` values are single-char strings; `builder_rows` text fields are capped at 200 chars each; `filter_state` stores only the active filter discriminants (no result data). Total payload remains well under 50 KB.

### Anti-Patterns to Avoid
- **Filtering proposed joins out of the Lab known-joins group [USER OVERRIDE 2026-06-20]:** new joins
  land `'proposed'` (DB default) and the Lab's `confirmed_only=True` path filters them OUT. Do NOT
  "fix" this by inserting `status='confirmed'` (the join is an unmoderated claim). Instead set the Lab
  fetch to `confirmed_only=False` so proposed joins show — matching `/browse` — and keep status
  `'proposed'`. [VERIFIED: supabase_setup.sql:162, web/supabase_client.py:1639, joins_lab.py:1150]
- **Using `ui.navigate.to('/settings')` for sign-in:** The current bug at `joins_lab.py:1573` — navigates away and loses all Joins Lab state. Use `create_login_dialog().open()` instead. [VERIFIED: web/auth_state.py:358]
- **Persisting result blobs:** Never write `full_text`, image bytes, or candidate lists to `safe_storage`. The 778 MB `search_history.json` incident (CHANGELOG v7.16) is the canonical warning. [VERIFIED: CHANGELOG]
- **Bumping generation before partials are returned:** See Pattern 4 above — Stop must NOT supersede the generation before `_should_apply_results` runs.
- **Calling `safe_user_get`/`safe_user_set` from inside `run.io_bound` worker:** `safe_storage` requires a NiceGUI UI context; it will log at debug and return defaults inside worker threads (by design). All persistence reads/writes must happen on the event loop, not inside `run.io_bound`. [VERIFIED: web/safe_storage.py:63-66]
- **Using `e.stop_propagation()` in Python handlers:** Forbidden by `tests/test_no_server_side_stop_propagation.py`. Use `js_handler='(e) => e.stopPropagation()'` instead. [VERIFIED: test file]

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Community join write | Custom Supabase query | `web/supabase_client.create_fragment_join` | RLS + auth client selection already correct |
| Self-delete join | Custom DELETE | `web/supabase_client.delete_fragment_join` | RLS `USING (auth.uid() = user_id)` enforces self-scope |
| In-app login dialog | Custom sign-in form | `web/auth_state.create_login_dialog()` | Handles email/password + Google OAuth + remember-me |
| List write | Direct Supabase insert | `web/supabase_client.add_list_item` | Correct authenticated client, error handling |
| XLSX export | Manual openpyxl code | Reuse `shared/export_dossier.py` column/header patterns | Already bilingual, correctly structured |
| Off-loop text fetch | Async for loop on event loop | `run.io_bound(batch_fetch_fn)` | CI guard would fail; event loop blocked |
| Browse URL for candidate | String format manually | `web/components/candidate_grid.build_browse_url(cand)` | Already handles optional page param |
| VS availability probe | New SQLite query | `_check_vs_service_available()` already in `joins_lab.py:372` | Module-level, already off-loop compatible |

---

## Verification Results (Critical Unknowns Resolved)

### V1: D-02 — fragment_joins.status discrepancy (HIGHEST PRIORITY)

**Finding: CONFIRMED DISCREPANCY + CLEAR FIX.**

Schema (`supabase_setup.sql:162`): [VERIFIED: supabase_setup.sql:162]
```sql
status TEXT DEFAULT 'proposed' CHECK (status IN ('proposed', 'confirmed', 'rejected'))
```

Code (`web/supabase_client.py:1639-1651`): [VERIFIED: supabase_client.py:1650]
The `create_fragment_join` `data` dict does NOT include a `status` key. New inserts therefore land with `status='proposed'` (DB default).

Known-joins group fetch path in `web/components/joins_panel.py:113-114`: [VERIFIED: joins_panel.py:113]
```python
if confirmed_only:
    joins = get_fragment_joins(fragment_sys_id=document_id, status='confirmed')
```

The Joins Lab passes `confirmed_only=True` at `joins_lab.py:1150`. [VERIFIED: joins_lab.py:1150]

The `/browse` dialog (the path the user tested live) calls `fetch_connected_fragments(confirmed_only=False)`, which passes NO `status` filter to `get_fragment_joins`, so ALL rows are returned — explaining why the user saw new joins appear immediately in `/browse`.

**RLS** (`supabase_setup.sql:338`): `USING (true)` — all rows publicly readable. No server-side filtering by status. [VERIFIED: supabase_setup.sql:338]

**Resolution for ACT-01 — [USER OVERRIDE 2026-06-20; supersedes the original recommendation]:**
Do **NOT** pass `status='confirmed'`. Keep the inserted status as the `'proposed'` default — a
user-added join is an *unmoderated* scholarly claim, and marking it `'confirmed'` would falsely assert
moderation. To make the new join appear in the Joins Lab's own known-joins group (SC#1), change the
Lab's fetch to `confirmed_only=False` (`joins_lab.py:1150`) so the group shows proposed + confirmed
alike — exactly as `/browse` already does. The known-joins cache key must therefore NOT be
`:confirmed`-scoped (otherwise proposed rows are cached out); `force_refresh` after insert (below).
This REVERSES Phase-118 D-17's confirmed-only filter for the Lab known-joins group.

> ~~Original (OBSOLETE) recommendation: Pass `status='confirmed'` explicitly in the
> `create_fragment_join` data dict so the join shows in the confirmed-only Lab path.~~ — replaced by
> the user override above (keep `'proposed'`, show proposed in the Lab).

**After insert, force-refresh the known-joins:** Call `_load_known_joins(sys_id, shelfmark, force_refresh=True)` (or pass `force_refresh=True` to `fetch_connected_fragments`) so the 30-second cache is bypassed and the new join appears immediately.

### V2: D-18 — the real sign-in invocation

**Finding: Use `create_login_dialog()` from `web/auth_state.py:358`.** [VERIFIED: web/auth_state.py:358]

The `complete_login` function at `web/main.py:2227` is the OAuth callback handler — it is called by the `/auth/callback` route after Google OAuth completes, not something the Joins Lab invokes directly.

The canonical in-app login dialog is `create_login_dialog()` from `web/auth_state.py`. It handles:
- Email/password login + "Remember me"
- Google OAuth (redirects, returns to current page via callback)
- Registration
- Returns `dialog` object; call `.open()` to show

**Fix for `joins_lab.py:1573`:** Replace:
```python
on_click=lambda: ui.navigate.to('/settings')
```
with:
```python
on_click=lambda: create_login_dialog().open()
```
Import: `from web.auth_state import GlobalAuthState, create_login_dialog`

**State preservation:** `create_login_dialog()` opens an overlay dialog over the current page — it does NOT navigate away. After email/password login completes (via `ui.navigate.reload()` inside the handler) or after Google OAuth returns to `/auth/callback` (which stores auth state and redirects), the Joins Lab state is preserved via the PST persistence (D-13/D-14). This is the correct behavior.

### V3: D-04 — multi-fragment Add-to-Puzzle staging

**Finding: Use a `safe_storage` staging key — cleanest multitenant-safe approach.** [VERIFIED: web/main.py:1902, web/pages/puzzle.py:2202]

`create_puzzle_page` signature: `def create_puzzle_page(initial_add: str = None, initial_doc: str = None)`.
The route handler at `web/main.py:1902`: `def puzzle_page_route(add: str = None, doc: str = None)` reads `add` from the query string and passes it as `initial_add`.

`initial_add` is a single `'sys_id'` or `'sys_id,fl_id'` string. [VERIFIED: puzzle.py:2218-2220]

**Staging mechanism (Claude's Discretion, recommendation):** Write a `safe_storage` key (`'puzzle_staging'`) containing `{anchor_sys_id, candidate_sys_ids: [...]}`  before navigating to `/puzzle`. `create_puzzle_page` reads and clears this key at the top of its initialization (after the existing `initial_add` / `initial_doc` checks). Max 20 sys_ids (anchor + 19 candidates; bounded to avoid unbounded page-load time).

**Multitenant safety:** `safe_user_get`/`safe_user_set` is per-session (Phase-87 invariant). The key is read and immediately cleared (one-shot) to prevent stale data on subsequent puzzle visits. [VERIFIED: web/safe_storage.py]

**Integration point in `create_puzzle_page`:** Read the key immediately after the existing `WEB_PUZZLE_ENABLED` guard and before the Fabric.js canvas setup. Call `_add_fragment_by_sys_id` for each sys_id in sequence (anchor first, then candidates). This function is already async-capable and handles the off-loop IIIF resolution. [VERIFIED: puzzle.py:2110]

**No allowlist entry needed [L2 CORRECTED 2026-06-20]:** `puzzle_staging` does NOT go into `.planning/phase87_storage_allowlist.yaml`. The `test_no_raw_storage_access.py` AST guard scans ONLY raw `app.storage.user` access (it is blind to `safe_user_*` keys), so any key written through the `safe_user_*` chokepoint requires NO allowlist exemption. The allowlist stays `allowed_raw_access: []`. The only requirement is that `puzzle_staging` is read/written via `safe_user_get`/`safe_user_set`/`safe_user_pop` — never raw `app.storage.user`. [VERIFIED: tests/test_no_raw_storage_access.py:8,88-89; .planning/phase87_storage_allowlist.yaml `[]`]

### V4: D-11 — stop-with-partials parity

**Finding: Feasible; the engine already returns partials; the guard currently discards them.** [VERIFIED: web/joins_executor.py:54-59, web/pages/joins_lab.py:234-250]

`execute_search` catches `InterruptedError` internally at `genizah_core.py:9000` and returns partial deduped results. The `WebSearchExecutor` wraps this with `except Exception: return []` — but `InterruptedError` is a subclass of `BaseException`, not `Exception`, so it propagates through the executor... wait, let me re-verify.

Re-reading `web/joins_executor.py:60-74`: the `except Exception` clause catches all exceptions, including any re-raise. But `genizah_core` catches `InterruptedError` INTERNALLY and returns normally — so `execute_search` returns a list (not raises), and the executor's `except Exception` path doesn't trigger. The executor's docstring confirms: "Plan 04 relies on its stale-generation guard to DISCARD those partial results — not on this adapter re-raising." [VERIFIED: joins_executor.py docstring]

**Current discard mechanism (`_should_apply_results`):** Returns `True` iff `my_gen == gen_ref['value']`. When `_cancel_current_search()` bumps the generation, the check returns `False` → partials discarded. [VERIFIED: joins_lab.py:234-250]

**Fix for D-11:** Introduce a separate `_stop_requested` flag. The Stop button sets this flag WITHOUT bumping the generation. The progress_cb checks it FIRST — raises `InterruptedError` immediately (aborting the scan loop early). The core returns partials. `_should_apply_results` still returns `True` (generation unchanged). Partials are applied to the UI. Only then is `_stop_requested` reset to `False` and the loading affordance cleared.

**`/search` Stop button precedent at `web/pages/search.py:2354-2358`:** Sets `search_state.is_cancelled = True`; the `progress_cb` at `:4055-4058` raises `InterruptedError` when this is True. [VERIFIED: search.py:2354, 4055]

**Distinguishing Stop vs. superseded:**
- Stop: `_stop_requested = True` + generation NOT bumped → `_should_apply_results` returns True → apply partials
- Superseded (new search / re-anchor): generation bumped → `_should_apply_results` returns False → discard
- The two flags are orthogonal. Reset `_stop_requested` to False at the START of every new search.

### V5: D-06 — export

**Reusable bits in `shared/export_dossier.py`:** Column naming convention (bilingual headers), `pgp_subset_for_sys_id`, `catalog_summary_for_sys_id`, `build_image_url_for_row`. These can be called from the flat export to populate the image URL and PGP columns without re-implementing metadata fetching. [VERIFIED: shared/export_dossier.py:1-80]

**Text fetch:** `executor.get_browse_page(sys_id, p_num=candidate.page)` returns a narrow dict with `'text'` key. For text-matched candidates, `candidate.page` is the matched page. For VS-only candidates (`candidate.page is None`), call `executor.get_browse_page(sys_id)` (first text page, the default). Cap at 4000 chars (matching `SEARCH_API_BROWSE_TEXT_CAP` convention). [VERIFIED: joins_executor.py:80-95]

**Off-loop batching:** The text fetch loop must run inside a `run.io_bound` closure (CI guard). The entire batch runs in one `run.io_bound` call with a progress callback to update a "Preparing export..." indicator.

**XLSX generation:** Use `openpyxl` (already imported by `web/export_service.py`); single sheet; no 4-sheet dossier structure (flat).

**CSV generation:** `io.StringIO` + `csv.writer`; served via `ui.download` with `content=...`.

**Anonymous-OK:** No login check needed for export.

### V6: D-10 / D-12 — prefetch + VS probe

**Desktop `_pump_images` bounded 5-slot model (`desktop/join_workbench.py:5061`):** Uses a list of `(label, url, target, callback)` tuples; drains up to `_MAX_CONCURRENT_IMG=5` threads at a time; each thread's `finished` signal re-calls `_pump_images` to drain more. [VERIFIED: desktop/join_workbench.py:5061-5101]

**Web equivalent pattern:** Maintain a list of pending `sys_id`s to prefetch; as each `run.io_bound` image-URL-resolution completes, start the next, bounded at 5 concurrent. Use a generation token per anchor so stale prefetch results for a prior anchor are discarded. Guard every post-await mutation with `except RuntimeError: return` (SEED-008 pattern).

**Image URLs:** Images load via AnchorViewer's `update_content()` which resolves the per-provider proxy internally. For prefetch, the relevant hook is the proxy URL construction already used in `_add_fragment_by_sys_id` (puzzle.py:2159-2171) — the same `fl_id`-based or `external_provider`-based proxy URL pattern. However, the web Compare modal already uses AnchorViewer instances which self-resolve images — prefetch means pre-instantiating (or pre-resolving) the image URL off-loop so the AnchorViewer's content is ready when the user flips. The simplest model: maintain a cache `{sys_id: resolved_url}` populated by a bounded off-loop worker.

**VS-availability probe (`_check_vs_service_available`):** Already module-level in `joins_lab.py:372`; dispatched via `run.io_bound` in `_do_vs_fetch_and_update`. D-12 reuses the same probe. When it returns `False` (no VS data for this anchor — determined by `get_suggestions(anchor_sid)` returning empty), the VS toggle is hidden (not just disabled). [VERIFIED: joins_lab.py:372-382, 1368-1380]

**CI guard:** The new D-10 prefetch closure (whatever its name) must be a sync function passed as first arg to `run.io_bound`. The `tests/test_joins_lab_off_loop.py` scanner checks for `execute_search` specifically — prefetch doesn't call execute_search, so it won't be auto-caught by the existing scanner. However, the scanner pattern (named sync closure + `run.io_bound`) should be followed anyway.

### V7: D-13/D-16 — persistence

**`web/joins_lab_storage.py` current state:** [VERIFIED: web/joins_lab_storage.py]
- Key: `_JOINS_LAB_KEY = 'joins_lab'`
- Version: `_SCHEMA_VERSION = 1`
- Current payload: `{schema_version, anchor_sys_id, anchor_fl_id, anchor_volume_ie}`
- Forward-compat note explicitly states: "Phase 120 will extend this dict with builder rows, triage verdicts, and filter state. Those keys can be added under the same `schema_version: 1` — only bump the version when a key is removed or its type changes."
- `clear_joins_lab_state()` already implemented via `safe_user_pop(_JOINS_LAB_KEY, None)`

**Extension approach:** Add `write_full_state()` and `read_full_state()` functions that operate on the same `_JOINS_LAB_KEY` dict. The `write_anchor()` function should be extended (or left as a convenience alias) — calling `write_full_state()` includes the anchor identity fields.

**Restore flow:** In `_bootstrap_anchor()`, after `read_anchor()` returns a stored state:
1. Read the full state (builder rows, triage, etc.)
2. Call `load_anchor()` (which re-fires known-joins, etc.)
3. Restore builder UI from stored rows
4. Show "restoring your search…" indicator
5. Call `execute_joins_search()` to re-run
6. After results land: apply stored triage by sys_id, restore filter, restore view mode

**Size caps:** Builder row text: max 200 chars each. Triage dict: max 200 entries (one per sys_id; values are 3-char strings). Filter state: compact discriminant dict only. Total estimated max payload: ~20 KB (far below any storage limit).

### V8: D-20 — SEED-008 fire-and-forget hardening

**Confirmed dispatch sites:** [VERIFIED: joins_lab.py grep results]
1. `_load_known_joins` dispatched at `joins_lab.py:1309` (`asyncio.ensure_future(...)`)
2. `_do_vs_fetch_and_update` dispatched at `joins_lab.py:1257` (re-anchor VS invalidate)
3. `_do_vs_fetch_and_update` dispatched at `joins_lab.py:1515` (toggle ON)
4. `_do_vs_fetch_and_update` dispatched at `joins_lab.py:2050` (end of search, VS on)
5. `_do_enrich_and_update` dispatched at `joins_lab.py:1742` and `joins_lab.py:2046`
6. Bootstrap `_runner` dispatched at `joins_lab.py:2127` (already has `except RuntimeError` at :2122-2123)

**Precedent guard pattern:** [VERIFIED: joins_lab.py:2122, joins_panel.py:512]
```python
try:
    with _page_client:
        await something()
except RuntimeError:
    pass  # or return
```

**New Phase-120 dispatch sites that must also carry the guard:**
- D-10 prefetch tasks
- D-12 VS-hide probe callback (if it mutates UI)
- ACT-01 known-joins re-render after insert

### V9: Invariant guards

**`tests/test_no_raw_storage_access.py`:** [VERIFIED: tests/test_no_raw_storage_access.py]
- Scans all `web/` `.py` files for direct `app.storage.user` access
- Allowlist at `.planning/phase87_storage_allowlist.yaml`; currently `[]` (empty)
- New `puzzle_staging` key in `create_puzzle_page` (which lives in `web/pages/puzzle.py`) must use `safe_user_get`/`safe_user_set`/`safe_user_pop` (not raw access). The scan covers `web/pages/puzzle.py`. NOTE [L2]: `safe_user_*` keys need NO allowlist entry — the guard scans only raw `app.storage.user`; the allowlist stays `[]`.
- All new `joins_lab_storage.py` writes use `safe_user_set` → guard satisfied

**`tests/test_joins_lab_off_loop.py`:** [VERIFIED: tests/test_joins_lab_off_loop.py]
- Scans `web/pages/joins_lab.py` for `execute_search` calls inside async functions (forbidden) and verifies each sync closure named as first arg to `run.io_bound`
- D-10 prefetch + D-12 VS probe: these don't call `execute_search`, so the existing scanner won't catch them. But the CI guard principle still applies — they must not block the event loop. No new scanner rules needed (the existing `run_vs_core` + `run_search_core` naming convention should be followed for any new closures).

**`tests/test_no_server_side_stop_propagation.py`:** [VERIFIED: tests/test_no_server_side_stop_propagation.py]
- Scans all `web/` for `.stop_propagation()` Python calls. No new `stop_propagation` calls may be introduced. Use `js_handler='(e) => e.stopPropagation()'` for Compare info buttons and any new clickable overlays.

---

## Common Pitfalls

### Pitfall 1: New join invisible in the Lab known-joins group (D-02) [USER OVERRIDE 2026-06-20]
**What goes wrong:** New joins land with `status='proposed'` (DB default); the Joins Lab known-joins group filters `status='confirmed'`; the new join never appears in the Lab even though it's visible in `/browse`. The user sees a "success" toast but the known-joins group doesn't update.
**Why it happens:** the Lab fetch passes `confirmed_only=True` (`joins_lab.py:1150`), filtering out proposed joins.
**How to avoid (user-chosen):** set the Lab fetch to `confirmed_only=False` so proposed joins show (parity with `/browse`), keep the inserted status `'proposed'` (do NOT mark `'confirmed'`), and ensure the known-joins cache key is not `:confirmed`-scoped, then `force_refresh` after insert. ~~(Do NOT "fix" by inserting `status='confirmed'`.)~~
**Warning signs:** Join created successfully (Supabase returns row) but known-joins group doesn't refresh even after force-refresh → check the `confirmed_only` flag and the cache key scope.

### Pitfall 2: Navigating to `/settings` for sign-in (D-18 bug)
**What goes wrong:** User is navigated away from Joins Lab; all in-memory state (candidates, triage, open dialogs) is lost; user must rebuild the search from scratch.
**How to avoid:** `create_login_dialog().open()` opens an overlay; the page stays mounted.

### Pitfall 3: Persisting result blobs (D-13)
**What goes wrong:** Storage payload grows unbounded. At 100-500 candidates with `full_text` (each potentially thousands of chars), a single session could write 1-5 MB to `app.storage.user`. Multiplied across sessions, this mirrors the 778 MB `search_history.json` incident.
**How to avoid:** Persist ONLY the inputs (builder row text, not results). See size caps in Pattern 5.

### Pitfall 4: Reading safe_storage inside run.io_bound
**What goes wrong:** `safe_user_get`/`safe_user_set` silently returns defaults and logs at debug when called outside a NiceGUI UI context (from a worker thread). Persistence writes inside `run.io_bound` are silently ignored.
**How to avoid:** All storage reads/writes happen synchronously on the event loop (before or after awaiting `run.io_bound`), never inside the sync closure.

### Pitfall 5: Stop button bumps generation before partials are applied
**What goes wrong:** If `_cancel_current_search()` (which bumps the generation) is called synchronously by the Stop button handler, `_should_apply_results` returns `False` and partials are discarded — identical to a superseded run.
**How to avoid:** See Pattern 4. Stop only sets `_stop_requested = True`; generation is NOT bumped until after the partial results are safely applied.

### Pitfall 6: puzzle_staging key not cleared (stale staging)
**What goes wrong:** User adds to puzzle from Joins Lab, puzzle opens and loads fragments. User then navigates directly to `/puzzle` later — the stale key still triggers bulk-add of the old fragments.
**How to avoid:** `safe_user_pop('puzzle_staging', None)` immediately after reading in `create_puzzle_page`. The key is one-shot.

### Pitfall 7: force_refresh=False on known-joins after ACT-01 insert
**What goes wrong:** The in-memory cache (30-second TTL) returns stale data; the new join doesn't appear for up to 30 seconds.
**How to avoid:** Call `_load_known_joins(..., force_refresh=True)` (pass `force_refresh=True` to `fetch_connected_fragments`) after a successful `create_fragment_join`.

---

## Code Examples

### Verified patterns from official sources

#### Check if user is logged in (for ACT-01 login gate)
```python
# Source: web/auth_state.py GlobalAuthState pattern (used throughout web/)
from web.auth_state import GlobalAuthState, create_login_dialog
if GlobalAuthState.is_logged_in():
    user = GlobalAuthState.get_user()
    user_id = user['id']  # Supabase UUID
    # proceed with create_fragment_join(user_id, ...)
else:
    create_login_dialog().open()
```

#### Delete own join (D-03)
```python
# Source: web/supabase_client.py:1658-1665
from web.supabase_client import delete_fragment_join
result = delete_fragment_join(join_id)  # join_id from the fragment_joins row
# RLS: DELETE policy USING (auth.uid() = user_id) enforces self-scope
```

#### Add item to list (ACT-03)
```python
# Source: web/supabase_client.py:1147-1166
from web.supabase_client import add_list_item
result = add_list_item(
    list_id=selected_list_id,
    sys_id=candidate.sys_id,
    shelfmark=candidate.shelfmark,
    title=candidate.title,
)
```

#### Get user's lists (for D-17 picker)
```python
# Source: web/supabase_client.py (get_lists pattern — similar to existing list page)
# For authenticated picker: call get_lists(user_id) to get the user's lists,
# then get_list_items(list_id) for items. Both require get_user_client().
```

#### Read current known-joins (for "remove my join" D-03)
```python
# Source: web/components/joins_panel.py:113-114
# Known joins are returned by fetch_connected_fragments(confirmed_only=True)
# The 'formatted_joins' in the result dict have 'id' (join_id) and 'created_by_username'
# Compare created_by_username to GlobalAuthState.get_username() to identify own joins
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| No join delete UI anywhere | Add self-delete on own joins (D-03) | Phase 120 | Easier to undo accidental joins |
| `/settings` navigate for sign-in | `create_login_dialog().open()` overlay | Phase 120 | State preserved |
| Single-fragment puzzle add | Bulk staging via safe_storage | Phase 120 | Anchor + candidates in one puzzle session |
| Partials always discarded on stop | Stop button applies partials | Phase 120 | Parity with `/search` |
| No working-state persistence | Full PST via safe_storage | Phase 120 | Session survives refresh |

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `get_browse_page(sys_id, p_num=None)` returns the first available text page (not a random page) | V5/D-06 export text fetch | Export shows wrong page text for VS-only candidates |
| A2 | `app.storage.user` (via safe_user_*) survives a page REFRESH for anonymous users in the current NiceGUI version | PST-02 | Persistence silently fails for anonymous users on refresh |
| A3 | The `puzzle_staging` key doesn't conflict with any existing `safe_storage` key in `create_puzzle_page` | ACT-02 | Key collision clobbers existing state |

**A1 risk is LOW:** `get_browse_page` is used by the existing AnchorViewer on every anchor load and defaults to the first page. Verified indirectly by the VS-only transcription prefix fetch at `joins_lab.py:1423`.

**A2 risk is MEDIUM:** NiceGUI's `app.storage.user` is keyed by a session cookie. On refresh the cookie is preserved and the storage is re-associated. This is the explicit design of Phase-87 PST-02, verified by the Codex critique finding "survives refresh for anonymous users." [CITED: .planning/v8.2.0-REQ-CODEX-CRITIQUE.md BLOCKER 2]

**A3 risk is LOW:** The puzzle page currently only reads `safe_storage` for `'current_page'` (set at route handler level). No other puzzle-related safe_storage keys found in `web/main.py` or `web/pages/puzzle.py`.

---

## Open Questions

1. **D-17 picker layout — flat list vs drill-down**
   - What we know: The button at `joins_lab.py:855` is "Choose from my lists"; the logged-in path currently shows a "Go to Lists" placeholder. `get_lists` and `get_list_items` exist.
   - What's unclear: How many lists does a typical user have? How many items per list?
   - Recommendation: Start with a flat recent-items list (last 20 items across all lists, showing shelfmark + list name). A two-level drill-down (list → items) is cleaner for power users but adds a navigation step. Claude's Discretion.

2. **D-03 "remove my join" UI placement**
   - What we know: `render_known_joins_group` renders source-attributed join rows; `created_by_username` is in the data.
   - What's unclear: The current `render_known_joins_group` doesn't receive the current user's identity (it's stateless per T-118-02).
   - Recommendation: Pass `current_user_id` (or `current_username`) as an optional parameter to `render_known_joins_group`; render a delete button only for own joins. This keeps the component pure-render (no auth calls inside it).

3. **puzzle_staging size cap**
   - Recommendation: Cap at anchor + 19 candidates (20 total). Each sys_id is a ~18-char string; 20 of them is ~400 bytes. Well within safe_storage limits.

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Supabase (fragment_joins table) | ACT-01/D-01/D-03 | ✓ | live | Degrade gracefully (notify user) |
| `web/safe_storage.py` | PST-01/02/03, ACT-02 | ✓ | Phase 87 | N/A (invariant) |
| `shared/visual_similarity_service.py` | D-12 | ✓ | existing | VS toggle hidden if DB absent |
| `openpyxl` | ACT-03 export | ✓ | existing | Fall back to CSV-only |
| `shared/export_dossier.py` | ACT-03 export helpers | ✓ | Phase 94 | Roll flat column names manually |

---

## Validation Architecture

Nyquist validation is ENABLED (`config.json workflow.nyquist_validation: true`).

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest |
| Config file | none — uses `PYTHONUTF8=1 GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen` env vars |
| Quick run command | `PYTHONUTF8=1 GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_joins_lab.py tests/test_joins_lab_storage.py tests/test_no_raw_storage_access.py -q --tb=short` |
| Full suite command | `PYTHONUTF8=1 GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/test_joins_lab_render_smoke.py tests/test_joins_lab.py tests/test_joins_lab_off_loop.py tests/test_joins_lab_page.py tests/test_joins_lab_storage.py tests/test_candidate_grid.py tests/test_candidate_surface.py tests/test_candidate_triage.py tests/test_candidate_filters.py tests/test_compare_modal.py tests/test_anchor_viewer.py tests/test_no_raw_storage_access.py tests/test_no_server_side_stop_propagation.py -q --tb=short` |

**Project testing reality (from CLAUDE.md / memory):** Full `pytest tests/` aborts on Windows — Qt headless segfault + Tantivy per-worker OOM with `-n auto`. Run only targeted files. New dialog/GUI tests must be added to `_GUI_TEST_FILES` in conftest for the CI marker-based split.

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | Notes |
|--------|----------|-----------|-------------------|-------|
| ACT-01 | `create_fragment_join` called with NO `status` kwarg (stays `'proposed'`); Lab fetch uses `confirmed_only=False` | unit | `pytest tests/test_joins_lab.py -k "test_add_join"` | New test; assert no status kwarg + confirmed_only=False |
| ACT-01 | Login gate shows dialog for anonymous user | render-smoke | `pytest tests/render_smoke/test_joins_lab_render_smoke.py -k "test_anon_add_join_gate"` | New render-smoke; headless mock |
| ACT-01 | New join appears in known-joins after force-refresh | unit | `pytest tests/test_joins_lab.py -k "test_add_join_visible"` | Mock fetch_connected_fragments |
| ACT-01 (D-03) | "Remove my join" deletes own join; not others | unit | `pytest tests/test_joins_lab.py -k "test_remove_own_join"` | Mock delete_fragment_join + RLS |
| ACT-02 | `puzzle_staging` key written + cleared (one-shot) | unit | `pytest tests/test_joins_lab.py -k "test_bulk_puzzle_staging"` | Mock safe_user_get/set |
| ACT-02 | Anchor always included regardless of selection | unit | `pytest tests/test_joins_lab.py -k "test_bulk_anchor_always_included"` | Pure logic test |
| ACT-03 | Export CSV contains triage verdict + text column | unit | `pytest tests/test_joins_lab.py -k "test_export_csv_columns"` | Mock executor.get_browse_page |
| ACT-03 | Export text: matched page for text hits; first page for VS-only | unit | `pytest tests/test_joins_lab.py -k "test_export_text_page_selection"` | |
| ACT-03 | Add-to-List login gate | render-smoke | `pytest tests/render_smoke/test_joins_lab_render_smoke.py -k "test_anon_add_list_gate"` | |
| PST-01 | Builder inputs serializable (no blobs in payload) | unit | `pytest tests/test_joins_lab_storage.py -k "test_write_full_state_no_blobs"` | Assert no `full_text` key |
| PST-01 | Restore calls execute_joins_search from persisted inputs | unit | `pytest tests/test_joins_lab_page.py -k "test_restore_reruns_search"` | Mock execute_joins_search |
| PST-02 | All storage writes go through safe_user_* (CI guard) | static AST | `pytest tests/test_no_raw_storage_access.py` | Existing guard; stays green |
| PST-03 | clear_joins_lab_state() leaves keys empty | unit | `pytest tests/test_joins_lab_storage.py -k "test_clear_leaves_empty"` | Assert safe_user_get returns None |
| D-11 | Stop button applies partials; superseded run discards | unit | `pytest tests/test_joins_lab.py -k "test_stop_applies_partials"` | Mock _make_progress_cb |
| D-18 | Sign-in button opens dialog (not navigate) | static/unit | `pytest tests/test_joins_lab.py -k "test_signin_opens_dialog"` | AST or mock-assert |
| D-20 | Fire-and-forget guards catch RuntimeError | render-smoke | `pytest tests/render_smoke/test_joins_lab_render_smoke.py -k "test_client_deleted_guard"` | Simulate client disconnect |
| SEED-008 | `_load_known_joins` doesn't propagate RuntimeError | unit | `pytest tests/test_joins_lab.py -k "test_load_known_joins_client_deleted"` | |

### Sampling Rate
- **Per task commit:** `pytest tests/test_joins_lab.py tests/test_joins_lab_storage.py tests/test_no_raw_storage_access.py -q --tb=short`
- **Per wave merge:** Full suite command above
- **Phase gate:** Full suite green before `/gsd-verify-work`; plus deferred-119 verification command (item 1 from `119-DEFERRED-VERIFICATION.md`)

### Wave 0 Gaps
- [ ] `tests/test_joins_lab_storage.py` — covers PST write/read/clear (extend existing file if it exists, or create)
- [ ] `tests/render_smoke/test_joins_lab_render_smoke.py` — covers SEED-008 client-deleted guard + login gates (new tests in existing render-smoke file)
- [ ] Check: `tests/test_joins_lab_storage.py` exists? If not, Wave 0 must create it.

---

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | yes | `GlobalAuthState.is_logged_in()` gate + `create_login_dialog()` |
| V3 Session Management | yes | `safe_user_*` (per NiceGUI session cookie, Phase 87) |
| V4 Access Control | yes | RLS on `fragment_joins` (`USING (auth.uid() = user_id)` for DELETE); self-scope only |
| V5 Input Validation | yes | Builder row text capped at 200 chars before storage; triage keys are `sys_id` strings (validated format); `puzzle_staging` list capped at 20 |
| V6 Cryptography | no | No new crypto; HMAC tokens for puzzle upload already in place |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Cross-user join leakage via shared cache | Information Disclosure | `confirmed_only=True` + `:confirmed` cache key already in place (T-118-01); ACT-01 uses `get_user_client()` (authenticated RLS) |
| Stale `puzzle_staging` key from prior session | Tampering | Clear key immediately on read (one-shot pattern) |
| Blob persistence in `joins_lab` storage | Denial of Service | Size cap enforced; NEVER write `full_text`/images |
| Server-side event propagation | Tampering | `js_handler='(e) => e.stopPropagation()'` for nested clickables; Python-side guard CI test |
| SSRF via image prefetch | SSRF | Images load exclusively via per-provider proxy + Phase-98 NLI breaker (T-119-09); no direct IIIF URLs |

---

## Sources

### Primary (HIGH confidence)
- `web/supabase_client.py:1574-1665` — verified `create_fragment_join`, `delete_fragment_join`, `get_fragment_joins` live code
- `supabase_setup.sql:151-166, :338-341` — verified `fragment_joins` schema, RLS policies
- `web/components/joins_panel.py:70-125` — verified `fetch_connected_fragments` confirmed_only path
- `web/pages/joins_lab.py:1-2130` — verified `_should_apply_results`, `_load_known_joins`, `_do_vs_fetch_and_update`, `_make_progress_cb`, bootstrap, storage use, sign-in bug
- `web/joins_lab_storage.py` — verified schema, `clear_joins_lab_state`, forward-compat note
- `web/safe_storage.py` — verified per-session model, UI context restriction
- `web/auth_state.py:358-531` — verified `create_login_dialog()` canonical sign-in
- `web/main.py:1902-1941` — verified `/puzzle` route handler signature
- `web/pages/puzzle.py:2202-2258` — verified `create_puzzle_page(initial_add)` single-fragment design
- `web/joins_executor.py` — verified `execute_search` partial-return behavior
- `web/pages/search.py:2354-2358, :4055-4058` — verified Stop button + progress_cb pattern
- `web/components/candidate_grid.py:442-450` — verified `build_browse_url`
- `shared/visual_similarity_service.py:38-50` — verified `VisualSimilarityService`
- `web/pages/joins_lab.py:372-408` — verified `_check_vs_service_available` + `_fetch_vs_candidates`
- `desktop/join_workbench.py:5061-5101` — verified `_pump_images` 5-slot model
- `.planning/seeds/SEED-008-joins-lab-client-deleted-crash.md` — exact fix pattern + dispatch sites
- `tests/test_no_raw_storage_access.py`, `tests/test_joins_lab_off_loop.py`, `tests/test_no_server_side_stop_propagation.py` — verified CI guard scope

### Secondary (MEDIUM confidence)
- `docs/guides/SUPABASE_GUIDE.md:518-536` — RLS policy cross-reference
- `.planning/v8.2.0-REQ-CODEX-CRITIQUE.md` — PST-02 `safe_storage` per-session behavior

### Tertiary (LOW confidence)
- None — all claims verified against live code.

---

## Metadata

**Confidence breakdown:**
- D-02 status fix: HIGH — read schema + code + RLS + fetch path directly
- ACT-02 staging mechanism: HIGH — verified single-fragment design; staging approach is Claude's Discretion but technically validated
- PST schema extension: HIGH — `joins_lab_storage.py` explicitly invites this extension
- D-11 Stop + partials: HIGH — verified progress_cb + `InterruptedError` + `_should_apply_results` chain
- D-18 sign-in fix: HIGH — verified `create_login_dialog()` existence and behavior
- SEED-008 dispatch sites: HIGH — grep-verified exact line numbers

**Research date:** 2026-06-20
**Valid until:** 2026-07-20 (stable stack; only risk is if supabase_setup.sql RLS or `joins_lab_storage.py` schema changes)
