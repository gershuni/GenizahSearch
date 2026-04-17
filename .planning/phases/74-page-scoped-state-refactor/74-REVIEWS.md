---
phase: 74
reviewers: [gemini, codex]
reviewed_at: 2026-04-17T12:15:00Z
plans_reviewed: [74-01-PLAN.md, 74-02-PLAN.md, 74-03-PLAN.md]
skipped: [claude]
skip_reason: "running inside Claude Code CLI — skipped self per workflow"
---

# Cross-AI Plan Review — Phase 74

## Gemini Review

This review analyzes implementation plans **74-01**, **74-02**, and **74-03** for the Page-Scoped State Refactor phase.

### Overall Assessment
The plans are **high quality**, technically sound, and strictly adhere to the constraints established in `74-CONTEXT.md` and `74-RESEARCH.md`. The strategy of decomposing the refactor into three serialized waves (Persistence Boundary → Bootstrap Extraction → Async Sweep) effectively manages the risk of this "most architectural" web change. The use of NiceGUI 3.8's awaitable scheduling path is correctly identified as the behavioral fix for the URL-bar staleness bug.

---

### Plan 74-01 (Wave 1 — Persistence Boundary)
**Summary:** Introduces the hydration/serialization boundary by creating `restore/persist/clear` helpers with version stamps. It retargets direct writes in `search.py` and `browse.py` while preserving existing bootstrap read paths.

*   **Strengths:**
    *   **TDD Approach:** Starts with Wave 0 test stubs (Task 1) that define the contract before implementation.
    *   **Version Hardening:** Implements `_SNAPSHOT_VERSION` to prevent schema-drift corruption, satisfying D-04.
    *   **Surgical Migration:** Correctly distinguishes between bootstrap-input keys (direct writes) and page-state snapshots (helper writes).
*   **Concerns:**
    *   **Double-Reading (LOW):** `restore_browse_snapshot` returns the position dict but hydrates the reading desk into `state`. In Plan 74-02, the resolver will still need the desk data for collision checks, potentially leading to a second `app.storage.user.get('reading_desk_state')` call in `browse.py`.
*   **Suggestions:**
    *   Modify `restore_browse_snapshot` to return a tuple `(pos_dict, desk_dict)` or a single validated snapshot object. This allows `create_browse_page` to pass the validated desk data directly to `resolve_browse_bootstrap` without re-reading storage.
*   **Risk Assessment: LOW.** The change is additive and includes round-trip tests to ensure no data loss.

---

### Plan 74-02 (Wave 2 — Browse Bootstrap Extraction)
**Summary:** Extracts the complex precedence logic from `browse.py` into a pure, unit-testable module (`web/browse_bootstrap.py`). This gives the browse page parity with the search page's deterministic startup.

*   **Strengths:**
    *   **Pure Logic Extraction:** Correctly isolates precedence (the "what") from scheduling (the "how"). Keeping `asyncio.ensure_future` in the caller preserves Cat-2 deferred init semantics.
    *   **Exhaustive Testing:** Covers all 6 precedence cases (D-19), including the critical language-switch desk collision.
*   **Concerns:**
    *   **Redundant Clear (LOW):** Plan 74-02 Task 3 Step 3 includes `app.storage.user.pop('reading_desk_state', None)`. Ensure this is updated to `clear_browse_snapshot()` for consistency with the Wave 1 boundary.
*   **Suggestions:**
    *   Update the dispatch switch in `browse.py` to use the helper's `clear_desk` boolean returned by the resolver to trigger the clear operation.
*   **Risk Assessment: LOW.** The extracted logic is a literal mirror of the current inline code, and unit tests provide immediate verification.

---

### Plan 74-03 (Wave 3 — Cat-1 asyncio.ensure_future Sweep)
**Summary:** Executes a comprehensive sweep of detached tasks. Converts "Cat-1" event handler wrappers to idiomatic NiceGUI 3.8 returns and adds justification comments to surviving "Cat-2/3" sites.

*   **Strengths:**
    *   **Mechanism Accuracy:** Leverages NiceGUI 3.8's `handle_event` awaitable path to restore `parent_slot` context, which is the verified fix for the `history.replaceState` failure mode.
    *   **Pitfall Awareness:** Correctly identifies the multi-coroutine aggregate pattern for `filter_panel.py` (Pitfall 1).
    *   **Regression Proof:** Includes the Selenium E2E test (D-20) as the definitive proof of behavior restoration.
*   **Concerns:**
    *   **Audit Reconciliation (MEDIUM):** There is a slight mismatch in the total `ensure_future` count for `browse.py` between CONTEXT and RESEARCH. Task 1 Step 1 correctly includes a reconciliation step to classify the outlier.
*   **Suggestions:**
    *   Ensure `on_click=lambda: load_page(...)` remains a bare lambda return. Do not accidentally convert it to `async def` unless the lambda needs multiple statements; the bare return is the most concise framework idiom.
*   **Risk Assessment: MEDIUM.** While behavior-restoring, this touches the most event handlers. The manual smoke check (D-22) is critical here to ensure no handlers were accidentally "silenced" by returning the wrong value.

---

### Phase 74 Conclusion
The plans satisfy **WEBM-03** and all locked decisions. The dependency ordering is correct. Performance implications are negligible as `app.storage.user` access remains synchronous. The transition from scattered storage writes to a centralized helper boundary is a significant architectural win that facilitates the future namespaced-format migration.

**Overall Risk: LOW.** The combination of TDD, Selenium E2E, and qualitative smoke checks provides a robust safety net.

---

## Codex Review

**Cross-Plan Findings**
- The Cat-1 premise is correct. In `.venv/Lib/site-packages/nicegui/events.py:449-458`, NiceGUI only routes awaitable handler results through `parent_slot` if the return value is an `Awaitable` that is **not** an `asyncio.Task`; `asyncio.ensure_future(...)` returns a `Task`, so it bypasses that path.
- The biggest design problem is in 74-01: the proposed snapshot helpers do not match the live state model in `web/pages/search_state.py` / `web/components/filter_panel.py`, and the plan contradicts itself about whether bootstrap keys belong to the snapshot.
- The biggest regression risk is in 74-02/74-03: the browse restore path in `web/pages/browse.py:4516-4530` currently restores `shelfmark_query` and validates `volume_ie`, while the plan omits both; the planned E2E selector in 74-03 also does not actually target the shelfmark buttons.

## Plan 74-01

**Summary**
74-01 has the right boundary goal, but the implementation spec is internally inconsistent and underfits the live code. It mixes bootstrap keys into the snapshot after explicitly saying they are out of scope, proposes helper defaults that do not match actual field types, and leaves restore ownership split across `search.py`, `search_state.py`, and `filter_panel.py`.

**Strengths**
- It correctly serializes the wave ordering: `browse.py` changes happen before 74-02 touches the same file.
- It identifies the real storage sprawl points in `web/pages/search.py` and `web/pages/browse.py`.
- Keeping legacy key names is pragmatic and avoids migration noise in this phase.

**Concerns**
- **HIGH:** The plan says bootstrap keys remain out of scope, but Task 2's proposed `_SEARCH_SNAPSHOT_KEYS` and `clear_search_snapshot()` still own and clear `search_query`, `search_mode`, `search_preset`, `search_max_changes`, and `search_gap`. That directly conflicts with the locked decision.
- **HIGH:** The proposed defaults are wrong for the live code. `filter_include_mode` is `bool` in `web/pages/search_state.py:60` and restored as `True/False` in `web/components/filter_panel.py:248`, not `'any'`. `filter_measurement_material` is list-backed in `search_state.py:82` and restored as `[]` in `filter_panel.py:270-271`, not `None`.
- **HIGH:** The TDD flow is contradictory. Task 1 says `tests/test_search_state.py` "MUST fail," but the same task also requires `pytest tests/` to exit 0. Both cannot be true in the same step.
- **HIGH:** `restore_search_snapshot` is not coherently integrated. Live restore still happens directly in `web/pages/search.py:123-156` and `:239-244`, while the proposed helper deliberately does not restore filter keys. That leaves double ownership instead of reducing it.
- **MEDIUM:** `persist_search_snapshot(search_state)` would write the entire capped `search_results` payload on every small state change such as domain exclusions or printed-filter toggles. Today those paths mostly write small deltas.
- **MEDIUM:** `restore_browse_snapshot` as proposed cannot correctly hydrate `reading_desk_entries`; saved `reading_desk_state` only stores minimal `{sys_id, shelfmark}` entries, while live code expects fully populated page/source structures via `_restore_reading_desk_state()`.

**Suggestions**
- Remove bootstrap keys from the search snapshot helper contract entirely. Update Task 1 tests so `clear_search_snapshot()` does not assert on `search_query` / `search_mode`.
- Decide one owner for search filters. Either:
  1. keep them owned by `filter_panel.load_filter_state` / `persist_value`, or
  2. move them into `search_state` helpers.
  The current plan tries to do both.
- Change helper defaults to match live types exactly: `filter_include_mode=True`, `filter_measurement_material=[]`.
- Do not use full-snapshot writes for tiny mutations. Keep a single helper module if you want, but allow targeted persistence inside that module.

**Risk Assessment**
**HIGH** — the plan has scope/ownership contradictions and type mismatches that would create real regressions before it meaningfully reduces storage sprawl.

## Plan 74-02

**Summary**
74-02 is the cleanest of the three plans. Extracting browse bootstrap precedence into a pure `resolve_browse_bootstrap(...)` mirroring `resolve_search_bootstrap(...)` is the right move. The problem is that the proposed dispatch logic drops live restore behavior that already exists in `browse.py`.

**Strengths**
- Pure-function extraction is appropriate here and matches the existing search bootstrap pattern.
- The precedence cases are mostly the right ones to unit-test.
- Keeping async scheduling in `browse.py` for bootstrap Cat-2 sites is the right separation.

**Concerns**
- **HIGH:** The proposed `restore_position` dispatch drops `state.shelfmark_query = saved_position.get('shelfmark', '')` and the `volume_ie` validation logic currently in `web/pages/browse.py:4516-4528`. That violates the "zero behavior change" requirement.
- **MEDIUM:** The tests prove precedence, but not the important stateful side effects: `clear_desk`, `volume_ie` fallback, `shelfmark_query` restoration, or invalid saved volume handling.
- **MEDIUM:** `initial_page` is typed as required `int` in the new helper, while `create_browse_page(...)` receives `Optional[int]`. The call site must normalize before calling the resolver.
- **LOW:** The plan tolerates leaving a direct `app.storage.user.pop('reading_desk_state', None)` in the new dispatch, which weakens the boundary 74-01 is trying to establish.

**Suggestions**
- Preserve the live `saved_position` restore behavior exactly in the caller after `action == 'restore_position'`: restore `state.shelfmark_query`, validate `volume_ie`, then call `load_page(...)`.
- Add at least two more unit cases:
  1. saved position with valid `volume_ie`,
  2. saved position with invalid `volume_ie` falling back to `None`.
- Normalize `initial_page` once at the call site before invoking the pure helper.
- If 74-01 introduces browse snapshot helpers, use them only as data sources; do not create a second restoration path.

**Risk Assessment**
**MEDIUM** — the extraction pattern is good, but the current spec would silently lose existing browse restore behavior.

## Plan 74-03

**Summary**
74-03 is directionally correct and the NiceGUI rationale is sound, but this is the highest execution-risk wave because it touches a large number of event handlers across multiple files. The URL-bar proof test, as currently written, is not reliable enough to certify the fix.

**Strengths**
- The Cat-1 / Cat-2 / Cat-3 taxonomy is technically grounded in NiceGUI 3.8 behavior.
- Requiring explicit comments on surviving detached tasks is good discipline.
- Auditing `search_results.py` in addition to the main page files is necessary; the planner caught real extra sites there.

**Concerns**
- **HIGH:** The planned E2E selector is wrong for the claimed behavior. The shelfmark buttons in `web/pages/browse.py:1628-1630` and `:1821-1823` use `skip_previous` / `skip_next` and only tooltips; they do **not** have `aria-label`s. The fallback selector would likely hit page-nav chevron buttons at `browse.py:3712` / `:3765`, not shelfmark navigation.
- **HIGH:** Because of that selector issue, `updated_url != initial_url` can pass for the wrong reason, or the test can skip too easily. It does not currently prove "shelfmark navigation updates URL."
- **MEDIUM:** The plan over-relies on exact `ensure_future` counts in acceptance criteria. After 74-01 and 74-02 line movement, "tight equality" checks are brittle and not a good proxy for correctness.
- **MEDIUM:** Some sites, especially `web/pages/search_results.py:930`, need classification by behavior, not by quota. That call is used during dialog initialization as well as user-triggered navigation; it is not as simple as the lambda sites.
- **MEDIUM:** The plan claims D-24 manual cross-tab validation, but same-version tabs still share `app.storage.user`. Version stamps do not solve same-version tab stomping.

**Suggestions**
- Add stable selectors to the shelfmark buttons first, e.g. `data-action="prev-manuscript"` / `data-action="next-manuscript"` plus `aria-label`. Then make the E2E test use those exact selectors.
- Strengthen the E2E assertion: verify that `sys_id` or shelfmark changed, not just that the URL string changed.
- Keep the Cat-1 sweep, but classify ambiguous sites from actual handler context, not from target counts.
- Where multiple async refreshes are independent, consider `await asyncio.gather(...)` instead of forcing sequential waits, so the refactor does not unnecessarily slow the UI.

**Risk Assessment**
**HIGH** — the root theory is correct, but this wave changes a lot of runtime behavior and the current proof test is not strong enough to catch the most important regressions.

## Bottom Line
- **74-01:** needs revision before execution.
- **74-02:** good structure, but must preserve `volume_ie` / `shelfmark_query` restore semantics.
- **74-03:** the Cat-1 cleanup is justified, but the E2E proof needs a stable selector and a stronger assertion before this can be trusted as the phase gate.

---

## Consensus Summary

Gemini and Codex diverge sharply on overall risk (Gemini: LOW, Codex: HIGH on 74-01 and 74-03). Gemini's review is process-oriented and trusts the plan text; Codex cross-checked the plan against live source code and found concrete mismatches. Where they agree, the signal is strong; where Codex raises points Gemini missed, those points are grounded in specific line-number citations that the planner should verify before execution.

### Agreed Strengths
- **Wave ordering** is correct — 74-01 must land before 74-02 and 74-03 because both touch `browse.py`. Both reviewers endorsed the serialization.
- **Cat-1 mechanism claim verified** — both reviewers independently confirm that `asyncio.ensure_future(...)` returns a `Task` that bypasses NiceGUI 3.8's `parent_slot`-preserving awaitable path. Codex cites exact source at `.venv/Lib/site-packages/nicegui/events.py:449-458`.
- **Pure-function bootstrap extraction** (74-02) is the right pattern, mirrors `resolve_search_bootstrap`, and benefits from unit tests.
- **TDD / test-first structure** of 74-01 Task 1 (Wave 0 stubs) is called out favorably by Gemini; Codex criticizes its self-consistency but not its intent.
- **Cat-2 comment discipline** (keeping detached tasks with explicit justification) is endorsed by both.

### Agreed Concerns
- **Dispatch boundary leak in 74-02** — both reviewers flag the direct `app.storage.user.pop('reading_desk_state', None)` surviving in the dispatch switch as a breach of the 74-01 persistence boundary. Gemini: LOW. Codex: LOW. Consensus: fix by routing through `clear_browse_snapshot()` or using the resolver's returned `clear_desk` boolean.
- **E2E selector reliability in 74-03** — Gemini warns generally about manual smoke check being "critical"; Codex escalates to HIGH with the concrete finding that the planned selector (`aria-label*="Next manuscript"`) does not match the actual shelfmark buttons (which use `skip_previous`/`skip_next` Material icons and tooltips only). Consensus: selector must be fixed before the test can prove anything.
- **Cat-1 risk profile** — both flag 74-03 as the highest-execution-risk wave because it touches the most event handlers; differ on severity (MEDIUM vs HIGH).

### Divergent Views (Codex-only findings to verify)

These are HIGH-severity items Gemini did not catch. They should be investigated against live code before the planner moves to execution:

- **74-01 scope contradiction (HIGH):** Codex claims `_SEARCH_SNAPSHOT_KEYS` in 74-01 Task 2 Step 1 includes `search_query`, `search_mode`, `search_preset`, `search_max_changes`, `search_gap` — which CONTEXT / the plan's own `must_haves.truths` explicitly exclude as bootstrap-input keys. **Verify:** re-read 74-01-PLAN.md Task 2 Step 1's `_SEARCH_SNAPSHOT_KEYS` tuple against the CONTEXT D-01/D-08 scope rules and the plan's own first truth statement.
- **74-01 type mismatches (HIGH):** Codex claims `filter_include_mode` is `bool` (not `'any'` string) in live `web/pages/search_state.py:60` and `filter_panel.py:248`; `filter_measurement_material` is list-backed (default `[]`, not `None`). **Verify:** read those files and check proposed `clear_search_snapshot` defaults in 74-01 Task 2.
- **74-01 contradictory TDD requirement (HIGH):** Codex notes Task 1 says `tests/test_search_state.py` "MUST fail" with ImportError/AttributeError AND that `pytest tests/` must exit 0 in the same step. **Verify:** read the acceptance_criteria block — Task 1 says "the new tests either skip or remain pre-helper — they are NOT expected to pass yet; however pytest must still complete cleanly" and then says "it MUST fail with ImportError." Look for the reconciliation — likely `pytest tests/` passing depends on the failing test being deselected, not on passing.
- **74-01 double ownership (HIGH):** Codex claims live restore at `search.py:123-156` + `:239-244` is left in place AND the new helper deliberately delegates to `filter_panel.load_filter_state`. So restore remains split across three modules. **Verify:** what is the source of truth for filter restore after 74-01?
- **74-01 reading_desk hydration mismatch (MEDIUM):** Codex claims saved `reading_desk_state` only stores minimal `{sys_id, shelfmark}` entries, while live `_restore_reading_desk_state()` expects fully populated structures. **Verify:** read `browse.py` `_restore_reading_desk_state` and the `persist_browse_snapshot` proposed body.
- **74-02 dropped shelfmark_query + volume_ie validation (HIGH):** Codex claims browse.py:4516-4528 currently restores `state.shelfmark_query` from saved position AND validates `volume_ie`, but 74-02's dispatch switch omits both. **Verify:** read browse.py:4516-4528 and compare to Plan 74-02 Task 3 Step 3 dispatch block.

### Top Priorities (before executing Plan 74-01)

1. **Resolve 74-01 scope contradiction** — either remove bootstrap keys from `_SEARCH_SNAPSHOT_KEYS` / `clear_search_snapshot` or update the plan's "truths" to include them (and re-evaluate against CONTEXT D-01).
2. **Fix 74-01 type defaults** — `filter_include_mode=True` (bool), `filter_measurement_material=[]` (list).
3. **Pick one restore owner for filters** — either `filter_panel.load_filter_state` keeps it, or `restore_search_snapshot` absorbs it. The current plan reads as both.
4. **Preserve 74-02 browse restore side effects** — the `restore_position` dispatch must re-assign `state.shelfmark_query` and validate `volume_ie` exactly as the live code does.
5. **Fix 74-03 E2E selector** — add a stable selector (`data-action="prev-manuscript"` / `aria-label="Previous manuscript"`) to the shelfmark buttons before the E2E test can be trusted.

### Recommended Next Step

Run `/gsd-plan-phase 74 --reviews` to feed this review document back into planning and produce revised plans. The concrete code-citation findings from Codex (particularly the scope contradiction, type defaults, and browse restore omissions) should be addressed in Plan 74-01 and 74-02 revisions before Wave 1 executes. The 74-03 E2E selector should be fixed before Wave 3 runs.
