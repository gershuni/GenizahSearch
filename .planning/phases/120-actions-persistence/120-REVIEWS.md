# Phase 120 — Cross-AI Plan Review (Codex pre-flight)

**Reviewer:** Codex (codex-cli 0.139.0, read-only sandbox) · **Date:** 2026-06-20
**Scope:** plan ↔ LIVE-code drift on the 8 Phase-120 plans · **Verdict:** CHANGES-REQUIRED
**Brief:** `_tmp/codex-120-plan-review-brief.md` · **Raw output:** `_tmp/codex-120-plan-review-output.md`

> **Confirmed CLEAN (do not regress):** No plan task bumps `_SCHEMA_VERSION` to 2; no ACT-01 plan path
> inserts `status='confirmed'`. Both load-bearing overrides are correctly encoded.

All findings below are **plan↔live-code drift** — the plans assume APIs/signatures/behaviors that the
current code does not provide. Re-verify each against the cited file:line, then make the targeted fix.

---

## BLOCKERS

### B1 — Builder persistence has no live API (Plan 120-03, lines ~91-99, 127-129; PST-01/D-13)
**Evidence:** `create_joins_builder` keeps rows/mode/query state **closure-local**
(`web/components/joins_builder.py:366-371`); the returned handle (`:940-951`) exposes **no**
snapshot/restore/change hook. Plan 03's "restore builder from persisted inputs" cannot work.
**Fix:** Add `get_state()` / `set_state(state)` / `on_change(cb)` (or `initial_state=`) support to
`web/components/joins_builder.py`, and ADD that file to Plan 03's `files_modified` with a task to
implement + test the snapshot/restore round-trip. The other-side builder (cross-side combine) needs
the same.

### B2 — Puzzle bulk-staging action is unexecutable as written (Plan 120-05, lines ~134-139; ACT-02/D-04)
**Evidence:** the plan `await`s `_add_fragment_by_sys_id(...)` **inside** `create_puzzle_page`, but
`create_puzzle_page` is **synchronous** (`web/pages/puzzle.py:2202`) and `_add_fragment_by_sys_id` is
**async** (`:2110`). Existing single-fragment auto-add is deferred via
`asyncio.ensure_future(_after_delay(...))` (`web/pages/puzzle.py:3779-3924`).
**Fix:** Pop + validate the `puzzle_staging` key synchronously on the event loop at page build, then
schedule an async `auto_add_bulk()` (mirroring the `:3779-3924` deferred pattern) that runs AFTER
canvas init and `await`s the fragments sequentially. Re-anchor must still come first (anchor index 0).

---

## HIGH

### H1 — `_load_known_joins(force_refresh=True)` does not exist (Plan 120-04, lines ~144, 158; D-02)
**Evidence:** live signature is `_load_known_joins(sys_id, shelfmark, pgpid=None, anchor_gen=0)` and it
**hardcodes** `force_refresh=False` into `fetch_connected_fragments` (`web/pages/joins_lab.py:1122-1152`).
**Fix:** Add `force_refresh: bool = False` to `_load_known_joins`, thread it into
`fetch_connected_fragments`, and update all call sites. This is the D-02 cache-bypass mechanism — the
plan depends on it. Also confirm the `confirmed_only=False` flip changes the cache key off `:confirmed`.

### H2 — Candidate selection/action substrate is mis-targeted (Plans 120-04:152, 120-05:96-103, 120-06:93-101; ACT-01/02/03)
**Evidence:** ONLY the **table** view owns multi-select (local `selected_sys_ids`,
`web/components/candidate_grid.py:890-897,926,971-983`); the **grid** view has **no** selection API
(`:992-1007`). `joins_lab.py` switches between them (`:734-756`). The plans (and UI-SPEC bulk action
bar) assume a shared multi-select across both views.
**Fix:** EITHER add a shared, page-visible selection/action API spanning grid+table, OR explicitly
scope the bulk actions (Add-to-Puzzle/List/Export) to table view and add equivalent per-row action
affordances for grid mode. Pick one and reflect it in the affected plans (and note the UI-SPEC
implication — the bulk action bar's "appears when ≥1 candidate checked" assumes selection exists in
the active view).

### H3 — Compare info buttons assume metadata Compare does not have (Plan 120-07, lines ~111-116; D-09)
**Evidence:** `create_compare_modal` only receives **measurement** enrichment
(`web/components/compare_modal.py:193-201,385-388`). The existing Browse metadata dialogs require
`BrowseState`/page data (`web/pages/browse_enrichment.py:524`) or already-fetched inputs
(`catalog_dialog.py:20`, `bibliography_dialog.py:22,242`).
**Fix:** Add an **off-loop metadata fetch layer** per Compare pane that produces the inputs the
existing `catalog_dialog`/`bibliography_dialog` need, OR pass richer browse-enrichment payloads into
Compare. Do NOT re-implement metadata fetching — feed the existing dialogs.

### H4 — Anonymous Add-as-Join auto-refire conflicts with login reload (Plan 120-04, line ~152; ACT-01/D-01)
**Evidence:** `create_login_dialog()` **reloads the page** after successful login
(`web/auth_state.py:424-427`) and has **no** success-callback hook. The UI-SPEC's "store pending
action in memory, invoke on auth complete" loses the pending add on reload.
**Fix:** Persist the pending add via `safe_user_*` and replay it after the post-login restore, OR
extend `create_login_dialog(on_success=..., reload=False)`. Apply the same reasoning to any other
login-gated action that expects an in-memory continuation (Add-to-List, choose-from-list picker).

---

## MEDIUM

### M1 — List item counts are not returned by `get_user_lists` (Plans 120-06:99, 120-08:97; D-05/D-17)
**Evidence:** `get_user_lists` only selects lists (`web/supabase_client.py:974`); counts come from a
separate `get_list_item_counts()` (`:928`).
**Fix:** Fetch counts off-loop alongside the lists, OR drop the item-count from the picker contract.

### M2 — Export image-helper call has wrong signature (PATTERNS 120-PATTERNS.md:783; D-06)
**Evidence:** pattern uses `build_image_url_for_row(sys_id=..., page=..., library=...)`; live signature
is `(sys_id, library_code, img_page, base_url=...)` (`shared/export_dossier.py:265-270`).
**Fix:** Call as `build_image_url_for_row(cand.sys_id, library_code=cand.library_code, img_page=cand.page)`.
Update PATTERNS.md and the Plan 06 export task accordingly.

### M3 — Image prefetch names the wrong resolver shape (PATTERNS 120-PATTERNS.md:538; D-10)
**Evidence:** pattern says `executor.get_browse_page(sys_id)` returns image metadata; the live
`WebSearchExecutor.get_browse_page` returns a **narrow text dict** and explicitly says NOT to enrich
images (`web/joins_executor.py:76-94`).
**Fix:** Use the same rich image-resolver path as `AnchorViewer`/Compare, off-loop, proxy URLs only.
Update PATTERNS.md and the Plan 07 prefetch task.

### M4 — SEED-008 guard scope misses pre-await UI mutation (Plan 120-02, lines ~85-90; D-20)
**Evidence:** `_load_known_joins` clears/renders the spinner **before** its first `await`
(`web/pages/joins_lab.py:1136-1142`) and is fire-and-forget. Guarding only the post-await block leaves
the pre-await UI mutation unprotected.
**Fix:** Wrap the WHOLE coroutine body that touches UI in `try/except RuntimeError: return`, not only
the post-await section. Apply to every dispatched coroutine, not just the awaited tail.

---

## LOW

### L1 — VS probe example names a non-existent free function (Plan 120-07, line ~175; D-12)
**Evidence:** `get_suggestions` is a **method** on `get_vs_service()`
(`shared/visual_similarity_service.py:97`), not an imported module function; `joins_lab.py` imports
only `get_vs_service` (`web/pages/joins_lab.py:69`).
**Fix:** Probe via `svc = get_vs_service(); svc.get_suggestions(anchor_sid, 1)` inside `run.io_bound`.

### L2 — Stale safe-storage allowlist guidance (120-RESEARCH.md:412, 120-PATTERNS.md:664, 929)
**Evidence:** the raw-storage guard scans direct `app.storage.user` only; `safe_user_*` keys do NOT
need an allowlist entry. Live allowlist is `[]` and stays `[]`.
**Fix:** DELETE the "add `puzzle_staging` to `phase87_storage_allowlist.yaml`" instructions from
RESEARCH.md and PATTERNS.md (they describe the wrong mechanism).

---

## NIT

### N1 — D-02 comment/docstring cleanup incomplete (Plan 120-04, lines ~108-114)
**Evidence:** live comments/docstrings still say confirmed-only prevents proposed cross-user leakage
(`web/components/joins_panel.py:108-111`, `web/components/known_joins_group.py:7,69`,
`web/pages/joins_lab.py:1125-1129`).
**Fix:** Update those comments to the locked "all community joins, proposed + confirmed shown in the
Lab" policy when ACT-01 / the `confirmed_only=False` flip lands.

---

## Disposition
- B1, B2, H1–H4, M1, M4, L1, N1 → **plan-logic revisions** (planner reviews-mode, re-verified vs live code).
- M2, M3, L2 → **PATTERNS.md / RESEARCH.md reference-doc fixes** (signatures + stale guidance).
- The two overrides (status `'proposed'`, schema_version 1) are correct — preserve them.
