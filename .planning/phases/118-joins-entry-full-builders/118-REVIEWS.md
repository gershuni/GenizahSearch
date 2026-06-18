---
phase: 118
reviewers: [codex]
reviewed_at: 2026-06-18T06:50:39Z
plans_reviewed: [118-01-PLAN.md, 118-02-PLAN.md, 118-03-PLAN.md, 118-04-PLAN.md, 118-05-PLAN.md]
focus: plan↔code drift (Codex has live-repo read access)
---

# Cross-AI Plan Review — Phase 118 (Joins, Entry & Full Builders)

## Codex Review

**Summary**
Phase 118 is mostly well-grounded in the live code: the cited shared core, web executor, Joins Lab page, search/browse entry points, desktop parity references, off-loop guard, and text-position/fuzzy assumptions mostly exist as described. The main risk is ANC-05/known-joins correctness: the plans still have contradictory `fragment_joins.status` assumptions, do not actually prove or implement a safe no-status filtering strategy, and miss the existing community-joins data path.

**Strengths**
- Correctly handles `SideQuery.page_position`: live code only accepts `None|'start'|'end'`, so `line_start/line_end` must bypass `SideQuery` and go to `execute_search(text_position=...)` directly. `shared/joins_lab.py:47`.
- Correctly identifies `compose()` hardcoding `ja`, `flex_spacing`, and `bidirectional` false, and ports the desktop "merge globals after compose" pattern. `shared/joins_lab.py:741` and `desktop/join_workbench.py:2475`.
- Cross-side off-loop shape is right: `tests/test_joins_lab_off_loop.py` requires named sync closures passed directly to `run.io_bound(...)`, and Plan 04 uses `run.io_bound(run_cross_side_core)`.
- `mode='fuzzy'` is valid in the engine: `genizah_core.py` maps fuzzy to `variants_maximum`. `genizah_core.py:7713`.
- Phase boundaries are mostly respected: no triage/table/Compare/VS/action persistence is pulled into the five plans.

**Concerns**
- **HIGH — ANC-05 test contradiction.** Plan 01 says the no-status branch must avoid a DB status filter, but `test_confirmed_path_passes_status_to_get_fragment_joins` is specified to assert `status is not None` (`118-01-PLAN.md:145`). That will force the exact nonexistent-column call the prompt says to avoid if the live schema has no `status`.
- **HIGH — No-status ANC-05 path is not actually confirmed-only.** Plan 02's no-status branch keeps "publicly-readable user joins" (`118-02-PLAN.md:118`). With RLS `USING(true)` (`docs/guides/SUPABASE_GUIDE.md:520`), that means all user rows are readable. A `:confirmed` cache key prevents cross-cache poisoning, but it does not filter unconfirmed user joins from the Lab group.
- **HIGH — Community joins are not included in the connected-group data path.** `fetch_connected_fragments()` emits `user`, `PGP`, and `FJMS` sources (`web/components/joins_panel.py:101/168/229`). Community puzzle joins are fetched separately inside `create_joins_dialog()` (`web/components/joins_panel.py:671`) and are not merged into `fetch_connected_fragments()`. Plan 02 adds a `community` badge but not a `community` data source.
- **MEDIUM — BLD-02 integration is under-tested.** The page contract says use `p_num`, not `internal_index`, but Plan 04 mostly tests `resolve_other_side_pages()` in isolation. `apply_cross_side()` does not call `resolve_other_side_pages()`; it uses `Candidate.page` and `executor.get_browse_page(sid, 1)` internally (`shared/joins_lab.py:344`). Add a fake-executor integration test proving the web path passes/uses `p_num`, `volume_ie`, and `total_pages=0 -> None`.
- **MEDIUM — Plan 05 expands synchronous I/O on the event loop.** It proposes `call_later` plus direct `fetch_connected_fragments()` for every result card (`118-05-PLAN.md:139`). That function hits Supabase/SQLite synchronously (`web/components/joins_panel.py:32`). Existing button code already does this, but adding it per card increases blocking risk.
- **MEDIUM — Invalid await expression in Plan 04.** `await run.io_bound(lambda: executor.get_meta_for_id(sys_id))[0]` is invalid precedence; it subscripts the coroutine before awaiting (`118-04-PLAN.md:204`).
- **LOW — Local schema artifacts conflict.** `docs/guides/SUPABASE_GUIDE.md` omits `fragment_joins.status`, but `supabase_setup.sql` defines it (`supabase_setup.sql:151`) while still using `USING(true)` (`supabase_setup.sql:338`). Could not verify the deployed schema (no Supabase env vars present; test execution/network unavailable).

**Suggestions**
- Rewrite ANC-05 tests to branch on the schema-probe result. If no `status`, assert no `status` kwarg is passed and assert user rows are excluded or filtered by a clearly defined confirmed heuristic.
- Decide the no-status public/confirmed rule explicitly: either exclude `source == 'user'` from the Lab confirmed-only group, or define a reliable application-layer "confirmed" mapping from existing fields.
- Extend `fetch_connected_fragments()` or a Lab-specific wrapper to merge published community puzzle joins into the connected group with `sources=['community']`.
- Replace Plan 05 per-card sync count loading with `run.io_bound`, batching, or lazy-on-click loading.
- Add a BLD-02 fake-executor test around `execute_joins_search`/cross-side, not only `resolve_other_side_pages()`.
- Fix the Plan 04 metadata fetch to `shelfmark, _ = await run.io_bound(lambda: executor.get_meta_for_id(sys_id))`.

**Plan↔Code Drift Findings**
1. `fragment_joins.status`: plans assume static fallback means "no status," but repo schema files conflict — guide has no status; `supabase_setup.sql` and archived migration DO have status.
2. Plan 01 ANC-05 test expects a non-`None` status kwarg even when the same plan says no-status means no DB status filter.
3. Plan 02 claims source-badged PGP/FJMS/user/community known joins, but the live reusable data path does not return community joins.
4. Plan 04 claims the web `p_num` page contract is exercised, but proposed implementation/tests do not actually prove integration with `apply_cross_side()`.
5. Plan 05 says card count loading is "without blocking," but its proposed `call_later` body performs synchronous I/O.

Verified as matching live code: `BuilderRow`, `SideQuery`, `compose`, `resolve_other_side_pages`, `cross_side_membership`, `apply_cross_side`, `WebSearchExecutor`, `get_browse_page`, `fetch_connected_fragments`, `get_fragment_joins`, search Text Position options, search result action row, browse joins-button call site, desktop `_merge_globals`, and `mode='fuzzy'`.

**Risk Assessment**
**HIGH** until ANC-05 is tightened. The builder/search spine is structurally sound, but the known-joins safety story currently has contradictory tests and an incomplete no-status filtering strategy. (Codex could not run tests locally: `pytest` not on PATH; `python -m pytest` failed with a Windows logon-session error.)

---

## Consensus Summary

Single reviewer (Codex, with live-repo access). The builder/search spine (BLD-03/BLD-04 + off-loop + Text Position + fuzzy) verified clean against the code — Codex confirmed 15 cited symbols/contracts exist as the plans claim. The actionable feedback clusters on the **known-joins safety + completeness story (ANC-04/ANC-05)**, plus two mechanical plan bugs.

### Agreed Strengths
- The builder spine assumptions are accurate against live code (`SideQuery.page_position` routing, `compose()` hardcoded falses + `_merge_globals_web` re-injection, named-closure off-loop, `mode='fuzzy'`).
- Phase boundaries respected (no 119/120/121 scope pulled forward).

### Agreed Concerns (priority order)
1. **HIGH — ANC-05 is incomplete, not just cache-isolated.** With RLS `USING(true)`, the `:confirmed` cache key stops cross-cache poisoning but does NOT exclude another user's unconfirmed `source=='user'` joins from the Lab group. A real public/confirmed rule is needed (exclude `source=='user'`, or a defined app-layer "confirmed" mapping). This is the literal ANC-05 requirement, so it must be resolved.
2. **HIGH — ANC-05 test self-contradiction.** Plan 01's test asserts `status is not None` while the plan elsewhere says no DB status filter — the test must branch on the Wave-0 schema-probe result.
3. **HIGH — Community source missing from the data path.** Plan 02 adds a `community` badge but `fetch_connected_fragments()` never returns community joins (they live only in `create_joins_dialog()`); ANC-04's "PGP+FJMS+user+community connected group" is not fully delivered. Needs a Lab-specific merge of published community puzzle joins.
4. **MEDIUM — BLD-02 integration coverage.** Test `apply_cross_side()` end-to-end with a fake executor (it uses `Candidate.page` + `get_browse_page(sid,1)`, not `resolve_other_side_pages()` directly) — prove `p_num`/`volume_ie`/`total_pages=0→None` actually flow.
5. **MEDIUM — Two plan bugs:** Plan 04's `await run.io_bound(...)[0]` precedence bug; Plan 05's per-card synchronous `fetch_connected_fragments()` blocking risk.

### Divergent Views
None (single reviewer). The three intentional divergences (JA dropped, shared toggle set, Exact/Variants/Fuzzy) were correctly NOT flagged.

### Key drift to resolve first
The `fragment_joins.status` schema is genuinely ambiguous in-repo (`supabase_setup.sql:151` defines it; `SUPABASE_GUIDE.md` omits it). The planner assumed "no status column" — that assumption is unverified. **The Wave-0 schema probe (Plan 01 Task 3) is the linchpin**, and the ANC-05 plans + tests must branch on its result rather than hardcode either assumption.
