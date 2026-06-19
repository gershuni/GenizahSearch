---
phase: 119-candidates-compare-visual-similarity
plan: "05"
subsystem: web/components/candidate_grid
tags: [candidate-grid, snippet, highlight, triage, compare, uat-closure]
dependency_graph:
  requires: []
  provides: [G1-snippet-highlight, G3-triage-fill, G4-image-click-compare]
  affects: [web/pages/joins_lab.py]
tech_stack:
  added: []
  patterns:
    - snippet_html() via ui.html(sanitize=False) for XSS-safe corpus text render
    - render-local per-card _triage_btn_refs dict for immediate fill update (G3)
    - hoisted _make_compare_handler for shared image+button click wiring (G4)
key_files:
  modified:
    - web/components/candidate_grid.py
    - tests/test_candidate_grid.py
decisions:
  - "_make_compare_handler hoisted before the card `with` block so both img_el and the Compare button at the bottom share the same handler — avoids defining two separate closures carrying the same candidate"
  - "Triage button refs captured into _triage_btn_refs (per-card render-local dict) passed into _make_triage_handler as a default-arg closure — preserves T-119-07 no-module-global invariant (mirrors CR-04 card_refs pattern)"
  - "snippet_html()/htmlify() escape corpus text BEFORE injecting highlight spans — only their output reaches ui.html(sanitize=False); raw cand.full_text/snippet never touch ui.html"
  - "Placeholder branch (synthetic sys_id) also gets cursor:pointer + click→Compare so no-image cards still open the Compare modal (G4 covers both image and non-image paths)"
metrics:
  duration: "20min"
  completed: "2026-06-19"
  tasks: 2
  files: 2
---

# Phase 119 Plan 05: Candidate Grid G1/G3/G4 Fixes Summary

One-liner: Candidate cards now show RTL highlighted transcription snippets via snippet_html (G1), image click opens Compare for the full candidate with cursor:pointer (G4), and triage button fills update immediately on click via render-local button refs (G3).

## Tasks Completed

| # | Task | Commit | Files |
|---|------|--------|-------|
| 1 | G1 render highlighted snippet on grid cards | `0abc8269` | candidate_grid.py, test_candidate_grid.py |
| 2 | G4 image click → Compare + G3 immediate triage fill | `0abc8269` | candidate_grid.py, test_candidate_grid.py |

(Both tasks landed in a single commit since they touch the same two files and the plan specified them as a co-located pair.)

## What Was Built

### G1 — Transcription snippet + highlight (CND-03)

`_create_candidate_card` now renders a snippet of the candidate's transcription text between the title block and the 👁 badge:

- Source: `cand.full_text or cand.snippet or ""` — no per-card network fetch.
- Pattern: `getattr(cand, "highlight_pattern", None)` — already populated by the page pipeline.
- Render: `ui.html(snippet_html(source, pattern), sanitize=False)` — corpus text is escaped first, then highlight regions wrapped in `<b style='color:#dc2626'>` (NOT `<mark>` — F-G1a verified).
- Style: `direction:rtl; text-align:right; -webkit-line-clamp:3` per UI-SPEC.
- Security T-119-05: only `snippet_html()`/`htmlify()` output passes to `ui.html(sanitize=False)`; shelfmark/title remain on auto-escaped `ui.label`.

### G4 — Image click → Compare (CND-04)

`_make_compare_handler` hoisted before the card `with` block so it is available for both the thumbnail and the Compare button at the bottom:

- `img_el.on("click", _make_compare_handler())` added after the image is created.
- `cursor:pointer` appended to `img_el` style string.
- Synthetic-placeholder branch: `.on("click", _make_compare_handler())` + `cursor:pointer` added so no-image cards also open Compare.
- Security T-119-06: no server-side `stop_propagation` (no nested clickable conflict here); AST guard continues to pass.

### G3 — Immediate triage button fill (D-11)

Per-card `_triage_btn_refs: dict[str, button]` captures each of the three button elements keyed by verdict. `_make_triage_handler` receives `_btn_refs` as a default-arg closure (T-119-07: render-local, not module-global) and on click:

1. Writes the verdict to triage state (unchanged).
2. Calls `restyle_fn` for card border repaint (unchanged).
3. NEW: iterates `_btn_refs.items()` and calls `_btn.style(...)` — sets `background:_TRIAGE_COLORS[v]; color:#fff` on the clicked button and resets the other two to the bare style — immediate DOM push via NiceGUI's `.style()`.

## Verification

```
python -m pytest tests/test_candidate_grid.py tests/test_no_raw_storage_access.py tests/test_no_server_side_stop_propagation.py -x -q
71 passed in 4.85s
```

Source grep checks all pass:
- `snippet_html` imported from `shared.joins_lab` in `_create_candidate_card`
- `ui.html(..., sanitize=False)` present; no bare `ui.html(cand.full_text)`
- `direction:rtl` and `-webkit-line-clamp` in snippet block
- `img_el.on("click"` + `cursor:pointer` present
- `_triage_btn_refs` and `_btn.style(` present
- No `app.storage.user` in non-comment functional code (CI-guarded)
- No server-side `stop_propagation` (only `js_handler` form where needed)

## Deviations from Plan

None — plan executed exactly as written.

Both tasks share the same two files (`candidate_grid.py` + `tests/test_candidate_grid.py`) and shipped as a single commit (no wave conflict; both were in wave 1 of this plan).

### Render-driven assertion co-ownership (G1)

Per the plan's explicit escape hatch: mounting `_create_candidate_card` in-process requires the full NiceGUI slot stack and `app.storage.secret` setup (the `test_joins_lab_render.py` pattern). Rather than wiring a heavy render harness into the existing lightweight headless test file, this plan satisfies the structural/source assertions (snippet_html imported, ui.html(sanitize=False) present, direction:rtl, -webkit-line-clamp, `<b style='color:#dc2626'>` is the actual markup) and delegates the live render assertion — mounting the card and asserting the rendered HTML contains the highlight span — to Plan-08's render-smoke harness as the co-required live owner.

## Threat Flags

None — no new network endpoints, auth paths, file access patterns, or schema changes introduced.

## Self-Check

- [x] `web/components/candidate_grid.py` modified — FOUND
- [x] `tests/test_candidate_grid.py` modified — FOUND
- [x] Commit `0abc8269` — FOUND (verified via `git rev-parse --short HEAD`)
- [x] 71 tests pass — VERIFIED

## Self-Check: PASSED
