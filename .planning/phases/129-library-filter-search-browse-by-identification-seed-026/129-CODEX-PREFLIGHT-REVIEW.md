# Phase 129 — Codex Pre-flight Plan Review (plan↔code drift, before execution)

**Date:** 2026-06-28
**Reviewer:** Codex (codex-cli 0.139.0), `codex exec` reading the 4 PLAN files against live source
**Scope:** plan↔code drift only (the internal gsd-plan-checker already cleared plan-internal consistency)
**Verdict:** **BLOCK** → resolved via targeted planner revision (this file is the revision input)

> This is the `feedback_codex_preflight_before_plan_complete` gate: the internal checker validates the plans against themselves; Codex validates them against the LIVE code. It caught a real integration gap the internal checker structurally cannot see.

---

## BLOCKER — Plan 02 wires the library filter into ONLY ONE render path

**Finding (confirmed against live `web/pages/search.py`):** `/search` applies post-search filters through a *cascade with routing predicates*, not a single helper. The existing printed/PGP filter is reached via predicates like
`elif (search_state.printed_filter != 'all' and search_state.printed_ids) or search_state.pgp_filter != 'all':`
at lines **3418** and **3446**, with parallel paths in `_apply_domain_exclusions` (3841), `_apply_word_search_exclusions_and_render` (3794), and the immediate / enrichment / history rerenders. When the **library filter is the only active filter**, every such predicate is **false**, so control falls through to a bare `render_results(...)` / measurement-only branch (e.g. 3421-3423, 3449-3451) that **never applies the library filter** → the filter silently does nothing in the common "library-only" case. Chips also won't show when library is the only active filter unless the chip-bar `has_any` accounts for it.

**Required fix (Plan 02):**
1. Apply `_apply_library_filter` inside the actual filtering steps of **all** post-search render paths — at minimum `_apply_printed_filter_and_render` (after `_apply_pgp_filter` at 3390, before `_apply_measurement_post_filters` at 3392), AND the filter steps of `_apply_domain_exclusions` and `_apply_word_search_exclusions_and_render`.
2. **Widen every routing predicate** that currently gates on printed/PGP to also fire when `search_state.library_filter` is non-empty — grep ALL occurrences of the `printed_filter != 'all' ... or ... pgp_filter != 'all'` predicate (3418, 3446, and the analogues in the other paths) and add `or bool(search_state.library_filter)`. This ensures library-only routes through a filtering path instead of the bare `render_results`.
3. Ensure the measurement-only / bare-`render_results` fallback branches apply the library filter when active (the predicate widening should route them through a filtering helper).
4. Chip bar: extend `has_any` / `_update_chip_bar` (~1132) and the reset/clear paths so the library chip(s) show when library is the only active filter.
5. Cover with a test: library-only filter (no printed/pgp/exclusions active) actually narrows the rendered set.

## HIGH — Plan 03 calls the wrong refresh function

**Finding (confirmed):** `web/pages/catalog_browse.py` — `fetch_results()` (268) only RETURNS data; `refresh_results()` (363) calls `fetch_results()` then updates the table/pagination/chips/loading. **Every** existing filter toggle/chip-clear uses `await refresh_results()` (654, 821, 835, 845, 862, 869, 919, 926, 943, 957, 968, 979, 1019, 1054, 1473). Plan 03 lists `await fetch_results()` as an acceptable UI-refresh target — it would fetch but never repaint.

**Required fix (Plan 03):** library toggles AND chip clears must call `await refresh_results()`. Remove `fetch_results()` as a UI-refresh target.

## MEDIUM — Plan 01 stale-token test asserts the wrong thing

**Finding:** `test_same_size_different_selection_not_stale` asserts two same-size selections yield different **totals**. But `COUNT(DISTINCT c.AlmaId)` over two same-size sys_id sets usually yields the SAME total even when the TEMP rebuild is correct — so the test could pass while masking the bug, or be impossible to satisfy. The robust signal for stale TEMP reuse is that the returned **AlmaId sets differ**.

**Required fix (Plan 01):** assert the returned AlmaId/result-ID **sets** differ between the two same-size selections (directly detects stale-TEMP reuse), OR craft the fixture so the two selections deliberately have different cardinality. Prefer the set-difference assertion.

## MEDIUM — Plan 04 test scaffold uses an invalid worker parent

**Finding (confirmed):** `_CatalogRefreshWorker` (genizah_app.py:497) calls `QThread.__init__(parent)`; a plain fake object as `parent` raises. Existing `tests/test_catalog_availability_filter.py:88` passes `None`.

**Required fix (Plan 04):** the scaffold must pass `None` as parent PLUS the new explicit `meta_mgr=` constructor arg (added in Task 2 — also resolves RESEARCH OQ-2), or use a real `QObject`/`QWidget`. Do NOT use a plain fake parent exposing `meta_mgr`.

## LOW — Plan 02 "[:200] render cap" is the wrong anchor

**Finding (confirmed):** `/search` paginates with `PAGE_SIZE = 50` (search.py:97). The `entries[:200]` at 3756 is an unrelated display cap. The phase framing's "[:200] render cap" is inaccurate for the live render path.

**Required fix (Plan 02):** reword to "over the FULL result set BEFORE the `PAGE_SIZE` (50) render slice / pagination". Tests should use `> PAGE_SIZE` rows (e.g. >50), not specifically >200. (Intent — filter the full set before any render slice — is unchanged.)

## LOW — Plan 02 `library_code` source anchor is wrong

**Finding:** `display.library_code` IS populated for regular results, but by `MetadataManager.get_display_data()` (metadata_manager.py:1931) + the text-result call sites (search_engine.py:2105, 2130) — NOT the cited `search_engine.py:2200-2206` (the metadata-only branch).

**Required fix (Plan 02):** correct the anchor; the data is present, only the citation is wrong.

---

## Confirmed OK (do NOT change)

- **Plan 01 push-down placement** — the new `EXISTS` belongs before `where = ...` at fjms_service.py:2226, feeding both the count and paged-results queries. Correct.
- **`resolve_library_sys_ids(library_codes, meta_mgr) -> set[str]`** contract is consistent across Plans 01/03/04.
- Plans 02/03 use the safe-storage chokepoint (no raw `app.storage.user`).
- Desktop OQ-1: live filter actions use `_catalog_start_async_refresh` (not `_catalog_refresh`). Plan 04 wires the right one.

**VERDICT: BLOCK** on the Plan 02 cascade-integration BLOCKER; fold the HIGH + 2 MEDIUM + 2 LOW into the same revision before execution.
