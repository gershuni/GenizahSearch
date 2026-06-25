---
status: deferred
phase: 119-candidates-compare-visual-similarity
created: 2026-06-19
deferred_to: 120
reason: User directed early close of the Phase-119 R2 gap closure during live UAT; final verification carried into Phase 120 close-out.
---

# Phase 119 — Deferred Verification (carried into Phase 120)

The Phase-119 round-2 gap-closure plans (119-09, 119-10, 119-11) are **code-complete and committed**.
At the user's direction (2026-06-19, live UAT) the phase was **closed early** and the final verification
steps below were **deferred to Phase 120 (Actions & Persistence)** to be handled "with all other things to
close." Phase 119 is already `[x]` complete in ROADMAP from the original (plans 01–08) verification.

## What shipped (code-complete, committed on master-main)

| Plan | R2 items closed | Commits |
|------|-----------------|---------|
| 119-09 | R2-1 (Hebrew Compare keys) + `TRIAGE_ICONS` substrate | `1db87608`, `a8e044a6`, `d6a6dfd2` |
| 119-10 | R2-4 (grid icons), R2-8 (VS-only transcription), R2-9 (icon action row), R2-10 (dark table) | `79cbf8cb`, `909434bb`, `68b54b60`, `e594b1fa` |
| 119-11 | R2-2 (LTR counter / RTL nav), R2-3 (image height), R2-4 (Compare icons), R2-5 (verdict border), R2-6 (suppress dup shelfmark), R2-7 (Esc closes) | `89c7754b`, `ecdda699`, `388058c5` |

Each plan's SUMMARY.md is `## Self-Check: PASSED`. A `py_compile` build gate over all 6 changed source
files passed. Per-plan headless suites passed inside each executor (119-09: 71 joins_lab tests; 119-10: 9
render-smoke; 119-11: 13 render-smoke + 104 unit + 9 invariant guards).

## Deferred item 1 — Automated post-merge test gate (NOT run this session)

The orchestrator-level cross-plan test gate was intentionally not run before close. Re-run in Phase 120:

```bash
PYTHONUTF8=1 GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest \
  tests/render_smoke/test_joins_lab_render_smoke.py \
  tests/test_joins_lab.py tests/test_joins_lab_off_loop.py tests/test_joins_lab_page.py \
  tests/test_candidate_grid.py tests/test_candidate_surface.py tests/test_candidate_triage.py \
  tests/test_candidate_filters.py tests/test_candidate_pagination.py tests/test_candidate_enrichment.py \
  tests/test_compare_modal.py tests/test_anchor_viewer.py tests/test_vs_adapter.py \
  tests/test_no_raw_storage_access.py tests/test_no_server_side_stop_propagation.py \
  -q --tb=short
```

(Full `pytest tests/` is avoided on this project — Qt headless segfault + Tantivy per-worker OOM; see
`feedback_full_suite_testing_windows`. CI runs the marker-split suite.)

## Deferred item 2 — gsd-verifier goal-check (NOT run this session)

The `gsd-verifier` goal-backward pass over the R2 gap closure was not run. Re-run in Phase 120 to confirm
the gap-closure requirements (CMP-01/02/03, CND-04/06/08, VSM-02) hold against the assembled code. A
`119-VERIFICATION.md` from the original (plans 01–08) pass already exists.

## Deferred item 3 — Live HUMAN-UAT re-run (render-observable, real corpus data)

Four R2 items are render-observable only in a live browser with real data and could not be pinned by the
in-process render-smoke harness (AnchorViewer.update_content is mocked). Re-test in a live browser:

- **R2-3** — In Compare, each pane shows BOTH the image AND the transcription text (image height capped).
- **R2-6** — Candidate shelfmark renders exactly ONCE in Compare (green subtitle only; inner header suppressed) with real corpus data.
- **R2-8** — VS-only / no-text-search candidate cards show the BEGINNING of the transcription (not blank) with real corpus data.
- **R2-10** — Candidate TABLE view renders a dark background (not white) under `[data-theme="dark"]`.

Full per-item detail (statuses flipped to `code-resolved`) lives in `119-HUMAN-UAT.md`.

## Deferred item 4 — SEED-008 (pre-existing crash, separate bug)

`SEED-008-joins-lab-client-deleted-crash.md` — `_load_known_joins` (+ the VS-fetch sibling) crash with
`RuntimeError: client ... has been deleted` when the tab disconnects mid-fetch. Phase 118 origin, NOT a 119
regression. Fix pattern (`except RuntimeError`) precedented in-repo. Folded into Phase 120.

## Also pending for Phase 120 (already routed)

- `SEED-007` — six new Web Joins Lab workbench actions (Add-as-Join, Make-an-anchor, etc.).
- `docs/OPEN_ISSUES.md` — refresh with R2 gap-closure status + SEED-008 when Phase 120 verification runs.
