---
phase: 119-candidates-compare-visual-similarity
plan: "06"
subsystem: web/components/compare_modal,web/components/anchor_viewer
tags: [compare-modal, anchor-viewer, highlight, async-load, uat-closure, G5, G1-compare, G3-compare]
dependency_graph:
  requires: [119-05]
  provides: [G5-compare-panes-load, G1-compare-highlight, G3-compare-verdict-state, F-A3-pane-markers]
  affects: [web/pages/joins_lab.py, plan-08-render-smoke]
tech_stack:
  added: []
  patterns:
    - _highlight_html_line_safe() LINE-SAFE highlighter (escape → <b style='color:#dc2626'> span, no wrapper, \\n preserved)
    - dialog.on("show", async _on_show) pane loader (NiceGUI client-context safe — no ensure_future)
    - Modal-level _cand_load_gen generation guard across AnchorViewer instance replacement (F-G5)
    - _verdict_btn_refs render-local dict + _refresh_verdict_buttons for active/inactive button toggle (G3-compare)
    - AnchorViewer.mark("anchor-viewer-image-pane") / .mark("anchor-viewer-transcription-pane") for Plan-08 render-smoke (F-A3)
key_files:
  modified:
    - web/components/anchor_viewer.py
    - web/components/compare_modal.py
    - tests/test_anchor_viewer.py
    - tests/test_compare_modal.py
decisions:
  - "_highlight_html_line_safe builds LINE-SAFE highlighted HTML: MARK_A/B stripped from input (WR-01), regex-sub → html.escape → <b style='color:#dc2626'> → NO <br> conversion → NO outer <div dir='rtl'> wrapper — compatible with render_line_numbered_html per-line splitter (F-G1b)"
  - "create_compare_modal stays SYNC (returns dialog); pane loading deferred to async dialog.on('show', _on_show) handler — safe in NiceGUI client context, no naked ensure_future (T-119-09)"
  - "Modal-level _cand_load_gen counter (not AnchorViewer's per-instance _nav_gen) guards candidate loads across instance replacement — each _fill_candidate replaces the viewer object so _nav_gen of the old instance never gets bumped; only _cand_load_gen is authoritative (F-G5)"
  - "F-G3c honored: record_verdict writes+advances atomically; _refresh_verdict_buttons (called inside _fill_candidate) keys on POST-ADVANCE candidate's triage entry; no unreachable pre-advance flash"
  - "_on_show + _load_candidate_pane exposed on dialog as test seam attributes — behavioral test drives them via asyncio.run without live NiceGUI"
metrics:
  duration: "35min"
  completed: "2026-06-19"
  tasks: 3
  files: 4
---

# Phase 119 Plan 06: Compare G5/G1-compare/G3-compare Fixes Summary

One-liner: Compare modal now loads both pane images + highlighted transcriptions via async dialog.on('show') loader (G5), applies LINE-SAFE escaped highlight spans to each pane (G1-compare), and reflects the shown candidate's verdict on verdict buttons via a render-local refresh (G3-compare).

## Tasks Completed

| # | Task | Commit | Files |
|---|------|--------|-------|
| 1 | AnchorViewer highlight_pattern + LINE-SAFE helper + pane markers | `3a7ae662` | anchor_viewer.py, test_anchor_viewer.py |
| 2 | Compare dialog.on(show) loader + async step/verdict + modal-level latest-wins guard | `a27ffe1e` | compare_modal.py, test_compare_modal.py |
| 3 | Verdict buttons reflect current candidate's verdict (G3-compare, F-G3c) | `3dae1b2e` | compare_modal.py, test_compare_modal.py |
| - | Ruff cleanup (unused imports) | `d3bb702a` | test_anchor_viewer.py, test_compare_modal.py |

## What Was Built

### G5 — Both Compare panes load image+transcription (CMP-01)

`create_compare_modal` stays SYNC but stores both viewer instances and registers an async `dialog.on("show", _on_show)` handler that awaits BOTH:
- `_anchor_viewer_ref[0].update_content(p_num=anchor_cand.page)`
- `_load_candidate_pane()` (candidate-pane async loader with modal-level latest-wins guard)

Prev/Next (`_step`) and verdict (`_record_verdict`) are made `async def` and `await _load_candidate_pane()` after rebuilding the candidate viewer. The anchor pane is NEVER reloaded by step/verdict (CMP-02 per-pane independence).

**F-G5 (modal-level latest-wins):** `_cand_load_gen = {'n': 0}` is incremented each time `_fill_candidate` creates a FRESH `AnchorViewer` instance. `_load_candidate_pane` captures its generation before the await; no-ops if a newer `_fill_candidate` superseded it. This guards ACROSS viewer instance replacement — AnchorViewer's per-instance `_nav_gen` only guards the SAME object.

### G1-compare — LINE-SAFE highlighted transcription in both Compare panes (ANC-03)

New module-level `_highlight_html_line_safe(text, pattern)` in `anchor_viewer.py`:
- Strips MARK_A/MARK_B sentinels from corpus input (WR-01 anti-forgery)
- Regex-subs matches → MARK_A + match + MARK_B (re.IGNORECASE | re.MULTILINE; swallows re.error)
- `html.escape()` the whole string (corpus content fully escaped)
- Replaces sentinels → `<b style='color:#dc2626'>` (F-G1a — same span as the rest of the app)
- Returns with `\n` preserved — NO `<br>` conversion, NO outer `<div dir='rtl'>` wrapper (F-G1b)

`AnchorViewer.__init__` now accepts `highlight_pattern: Optional[str] = None` (stored as `self._highlight_pattern`). In `update_content`, when pattern is set and text is non-empty, builds `highlight_html` via `_highlight_html_line_safe` and passes it to `render_line_numbered_html(highlight_html=...)`. No-pattern path is unchanged (existing behaviour preserved). Security T-119-10: only escaped helper output reaches `ui.html(sanitize=False)`.

Both Compare pane `AnchorViewer` constructors pass `highlight_pattern=getattr(cand/anchor_cand, 'highlight_pattern', None)`.

`shared/joins_lab.htmlify` is UNCHANGED (no app-wide ripple).

### F-A3 — Queryable pane state markers for Plan-08 render-smoke

Image container: `.mark("anchor-viewer-image-pane")`
Transcription container: `.mark("anchor-viewer-transcription-pane")`

Plan-08's render-smoke can query `user.find(marker="anchor-viewer-image-pane")` to assert the skeleton is gone after the show-loader resolves.

### G3-compare — Verdict buttons reflect SHOWN candidate's verdict (D-03)

`_verdict_btn_refs: dict[str, button]` captures all 3 verdict buttons keyed by verdict in the verdict-buttons loop.

`_refresh_verdict_buttons(cand)`:
- Reads `triage.get(cand.sys_id)` (the shared sys_id-keyed dict)
- Sets matching button to `unelevated` + solid colour (active state)
- Sets other two buttons to `outline` (inactive state)
- Render-local — factory-scoped `_verdict_btn_refs` (no module globals)

Called at end of `_fill_candidate` — runs on open + every step + every verdict.

**F-G3c (Codex constraint honored):** `record_verdict` writes + advances atomically (one call). By the time `_fill_candidate(next)` runs, `_state["current_candidate"]` is already the POST-ADVANCE candidate. No unreachable pre-advance flash attempted. The buttons correctly show the next candidate's stored verdict (or none if not yet voted). The recorded verdict for the previous candidate is persisted in triage and visible on back-navigation.

## Verification

```
python -m pytest tests/test_compare_modal.py tests/test_anchor_viewer.py -x -q
81 passed in 2.38s

python -m ruff check web/components/anchor_viewer.py web/components/compare_modal.py tests/test_anchor_viewer.py tests/test_compare_modal.py
All checks passed!

python -c "import web.components.compare_modal; import web.components.anchor_viewer"
(no output — clean import)
```

Source grep checks:
- `web/components/compare_modal.py` contains `dialog.on("show"` — confirmed
- `web/components/compare_modal.py` `_step` and `_record_verdict` are `async def` — confirmed
- `web/components/compare_modal.py` `_cand_load_gen` referenced in `_fill_candidate` + `_load_candidate_pane` — confirmed
- No `ensure_future` in `compare_modal.py` — confirmed
- No `inject_viewer_assets`, no `app.storage.user`, no `p-3`/`gap-3`, no server-side `stop_propagation` — confirmed
- `web/components/anchor_viewer.py` `highlight_pattern` stored; `_highlight_html_line_safe` defined — confirmed
- `<b style='color:#dc2626'>` emitted (NOT `<mark>`) — confirmed
- `.mark("anchor-viewer-image-pane")` + `.mark("anchor-viewer-transcription-pane")` present — confirmed

## Deviations from Plan

None — plan executed exactly as written.

The behavioral show-loader test (Task 2 requirement) drives `_on_show` via `asyncio.run` with `AsyncMock` stub viewers and asserts BOTH `update_content` awaits. A secondary test drives `_load_candidate_pane` directly and asserts only the candidate stub is re-called (not the anchor — CMP-02).

### Co-ownership note (Plan-08)

Per the plan's specification, Plan-08's render-smoke harness is the co-required LIVE owner for any G5 aspect (real images leaving skeleton state in a NiceGUI client) not asserted by the behavioral stub-viewer tests. The behavioral tests prove the coroutine contract; Plan-08 proves the end-to-end render path.

## Threat Flags

None — no new network endpoints, auth paths, file access patterns, or schema changes. All threat mitigations from the plan's threat register are implemented:
- T-119-08: images load exclusively via AnchorViewer → per-provider proxy + Phase-98 breaker; no image URLs constructed in compare_modal
- T-119-10: `_highlight_html_line_safe` escape-first, MARK_A/B stripped from input; only escaped output reaches `ui.html(sanitize=False)`
- T-119-09: loaders via `dialog.on("show")` + async click handlers in client context; no naked background coroutines
- T-119-11: verdict-button state render-local (`_verdict_btn_refs` factory-scoped); zero `app.storage.user`

## Self-Check

- [x] `web/components/anchor_viewer.py` modified — FOUND
- [x] `web/components/compare_modal.py` modified — FOUND
- [x] `tests/test_anchor_viewer.py` modified — FOUND
- [x] `tests/test_compare_modal.py` modified — FOUND
- [x] Commit `3a7ae662` (Task 1) — FOUND
- [x] Commit `a27ffe1e` (Task 2) — FOUND
- [x] Commit `3dae1b2e` (Task 3) — FOUND
- [x] Commit `d3bb702a` (ruff) — FOUND
- [x] 81 tests pass — VERIFIED
- [x] ruff clean — VERIFIED
- [x] headless import clean — VERIFIED

## Self-Check: PASSED
