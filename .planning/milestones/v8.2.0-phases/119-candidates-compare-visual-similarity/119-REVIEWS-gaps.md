---
phase: 119
review_type: gap-closure-preflight
reviewers: [codex]
reviewed_at: 2026-06-19
plans_reviewed: [119-05-PLAN.md, 119-06-PLAN.md, 119-07-PLAN.md, 119-08-PLAN.md]
verdict: REVISE
brief: _tmp/codex-119-gaps-preflight-brief.md
raw_output: _tmp/codex-119-gaps-preflight-output.md
---

# Phase 119 GAP-CLOSURE — Cross-AI Plan Review (Codex pre-flight)

**Reviewer:** Codex CLI (codex-cli 0.139.0), read-only sandbox, against live source
**Subject:** the 4 NEW gap-closure plans (119-05..08), distinct from the pre-execution review in `119-REVIEWS.md` (which covered the original 119-01..04).
**Verdict:** **REVISE** — 1 BLOCKER, 4 HIGH, 6 MEDIUM.
**Value:** plan↔code DRIFT against the actual current code — what the internal plan-checker (plan-internal consistency) cannot see.

> Codex could not run tests (read-only env, Python broken) — static, code-grounded review only.

## Confirmations (no change needed)
- **G2 coverage is complete** — "no additional recompute call site beyond the plan's intended set; the key places are Step 9, `_re_render_candidates_surface`, `_do_vs_fetch_and_update`, `_on_vs_toggle_change`, and the F1 pure-VS branch."
- Wave ownership mostly clean — 05/06/07 own distinct production files; 07 owns the new off-loop test.

## Findings

### F-A1 [BLOCKER] — NiceGUI `user` fixture defaults to root `main.py`, which doesn't exist
`.venv/.../nicegui/testing/general_fixtures.py:12` + `web/main.py:1802`. The `/joins-lab` route is in `web/main.py`, not a root `main.py`. Plan 08 adds no `nicegui_main_file` marker / pytest config, so `await user.open('/joins-lab')` can't reach the real render path. **Fix:** add `@pytest.mark.module_under_test("web/main.py")` (the `nicegui_main_file` marker), set it in pytest config, or build a local fixture with an explicit route root.

### F-A2 [HIGH] — render-smoke startup runs real `initialize_engine`
`web/main.py:2358` + `.venv/.../nicegui/testing/user_simulation.py:37`. If Plan 08 drives `web/main.py`, lifespan startup runs `initialize_engine` (real metadata/index setup) before the test body can monkeypatch. **Fix:** patch startup before `user_simulation` enters lifespan, OR use a local test root that registers `/joins-lab` with mocked services.

### F-A3 [MEDIUM] — skeleton assertion not observable via `User` text matching
`web/components/anchor_viewer.py:616`. "Both panes leave skeleton state" can't be asserted via `should_see` text — the skeleton is only a CSS class with no marker/content. **Fix:** add stable `.mark(...)` markers for skeleton/image panes (in plan-06's `anchor_viewer.py`), or inspect layout descendants/classes directly.

### F-A4 [MEDIUM] — Plan 08 `files_modified` omits the pytest-asyncio config file
`119-08-PLAN.md:7`. Task text adds asyncio config but `pyproject.toml` isn't in the file list. **Fix:** add `pyproject.toml` (or wherever the config goes) to `files_modified`, or drop that path.

### F-G1a [HIGH] — highlight markup drift: helper emits `<b style=...>`, not `<mark>`
`shared/joins_lab.py:705`. Plans 05/06/08 assert `<mark>`, but `htmlify`/`snippet_html` emit `<b style='color:#dc2626'>`. Tests asserting `<mark>` will fail or force an app-wide shared-helper change. **Fix:** assert the ACTUAL helper output (`<b style=...>`); do NOT change shared `htmlify` (ripples to all search results).

### F-G1b [HIGH] — `htmlify` wraps `<div dir='rtl'>`, incompatible with per-line renderer
`web/components/typography.py:131` + `shared/joins_lab.py:706`. Plan 06 passes `htmlify(page.text, pattern)` into `render_line_numbered_html(highlight_html=...)`, but `htmlify` adds an outer `<div dir='rtl'>` while the line-number renderer splits `highlight_html` per line → fragmented wrapper markup. **Fix:** add/use a line-safe highlight helper (escape + inject highlight tags, no outer wrapper) and pass that.

### F-A4-api [HIGH] — A4 mis-states the API enrichment fields
`web/api.py:2266`. Plan 07 claims the API VS path enriches `shelfmark/title/page`; it actually enriches `shelfmark` + `library_code` (+ optional domain), NOT `title`/`page`. **Fix:** source `title` from `WebSearchExecutor.get_meta_for_id`, `library_code` from `get_library_for_id`, and define the VS-only page policy explicitly.

### F-A4-scope [HIGH] — `_fetch_vs_candidates` is module-level; `executor` is page-local
`web/pages/joins_lab.py:372` + `:718`. Plan 07 enriches VS candidates inside module-level `_fetch_vs_candidates`, but the `executor` needed for metadata lives inside `create_joins_lab_page`. **Fix:** pass metadata/library resolver callables into `_fetch_vs_candidates`, or enrich inside page-local async closures via `run.io_bound`.

### F-A4-guard [MEDIUM] — new off-loop guard must name the actual blocking calls
`tests/test_joins_lab_off_loop.py:96`. The AST guard won't catch new VS-metadata lookups unless it names them. **Fix:** guard `get_meta_for_id` / `get_library_for_id` (the exact resolver methods the A4 fix introduces).

### F-G5 [MEDIUM] — latest-wins overstated for candidate-pane replacement
`web/components/compare_modal.py:300` + `web/components/anchor_viewer.py:516`. `_nav_gen` protects ONE instance, but the modal replaces candidate viewer instances; an older in-flight instance isn't superseded by a newer instance's counter. **Fix:** add a modal-level generation guard, or invalidate the old viewer before clearing/replacing it.

### F-G3c [MEDIUM] — verdict refresh vs. auto-advance ordering
`web/components/compare_modal.py:158`. Plan 06 wants the just-recorded verdict refreshed before auto-advance, but `record_verdict` writes+advances in one helper. **Fix:** split write/refresh/advance in the live modal path, or revise acceptance to require active-state only for the post-advance candidate.

### F-VSavail [MEDIUM] — VS availability probe can open SQLite on the event loop
`shared/visual_similarity_service.py:312` + `web/pages/joins_lab.py:991`. Checking availability at page build can open SQLite synchronously. **Fix:** probe through `run.io_bound`, or reuse the existing off-loop VS fetch flow before disabling/annotating the toggle.

## Consensus / action
Single reviewer (Codex). Verdict **REVISE**: the BLOCKER (F-A1) would make the entire render-smoke harness inert, and the HIGH drifts (highlight markup `<b>` vs `<mark>`, `htmlify` wrapper incompatibility, A4 API-field mis-statement + page-local executor scope) would produce failing tests or incorrect fixes. All fixes are concrete and preserve existing behavior — fold into 119-05..08 before execution.
