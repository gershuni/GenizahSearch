---
status: in-progress
phase: 75-non-regression-verification
source: [75-VERIFICATION.md]
started: 2026-04-17
updated: 2026-04-17
---

## Fixed Test Manuscripts (D-08 — locked for reruns)

| Surface test | sys_id | Shelfmark | Library | Notes |
|---|---|---|---|---|
| Cambridge T-S (web browse step c) | 990051334060205171 | T-S 12.123 | CUL | CUDL image load |
| NLI-only with crossref (web browse step a) | 990001273900205171 | MS. 2065 | HUC (Hebrew Union College) | Substituted 2026-04-17 — original 990053385670205171 (Oxford arch. O.d.8/1) had no image/text/data in-app; HUC 2065 has 20 images in nli_crossref.db nli_images with no Cambridge/JTS/Manchester overlap |
| Multi-IE (web browse step d) | 990000412990205171 | Ms. Heb. 6972=8 | Allony (NLI) | IE104549337, IE19213988 — 2 IEs, 7 trans FLs |
| JTS DPUL (web browse spot check) | 990053572370205171 | ENA 1052.1 | JTS | Princeton DPUL image path — v7.2.3 regression surface |

## Current Test

[Surfaces 1–3 passed; resuming at surface 4 Desktop Browse Responsiveness]

## Tests

### 1. Web Search Responsiveness

expected:
- (a) Cold-load `/` → first paint appears without visible delay vs pre-refactor recall
- (b) Execute text query `"שלום"` → first results visible; subjective time ≤ pre-refactor recall
- (c) Expand one result accordion → opens without hitch
- (d) Click "Browse" on one result → navigates to /browse without visible stall
- (e) Click "Export" on one result → export action completes without UI block
- (f) Paginate forward twice → next-page results appear without perceptible slowdown

result: passed (user approval 2026-04-17 — back-navigation regression fixed via 75-03-PLAN.md; items (a), (b), (c), (d), (f) confirmed green; item (e) export-whole-list pre-existing bug unchanged, remains in docs/OPEN_ISSUES.md P2, outside Phase 75 scope)

### 2. Web Browse Responsiveness

expected:
- (a) Navigate `/browse?sys_id=990053385670205171` → manuscript visible; subjective load time ≤ pre-refactor recall
- (b) Enrichment panel populates (FJMS catalog, bibliography) → Phase A/B deferred load completes without visible stall
- (c) Navigate `/browse?sys_id=990051334060205171` → CUDL image loads; no new hitches
- (d) Navigate `/browse?sys_id=990000412990205171` → volume selector renders; switching between IE104549337 and IE19213988 updates image + text without stall
- (e) Folio Prev/Next updates the URL bar (Phase 74 D-20 E2E regression check — confirms Cat-1 fix holds under real use)
- (f) Spot-check: navigate `/browse?sys_id=990053572370205171` → Princeton DPUL image path still works (v7.2.3 regression surface)

result: passed (user approval 2026-04-17 — items (a)/(b) re-run against substitute sys_id 990001273900205171 HUC MS. 2065 because the original Oxford arch. O.d.8/1 had no image/text/data in-app; (c) CUDL image, (d) volume selector + IE switch, (e) folio Prev/Next URL bar update all green; (f) Princeton DPUL path flagged as pre-existing bug — no source-switch button and credit reads NLI, confirmed NOT a v7.9 decomposition regression against live website, logged to docs/OPEN_ISSUES.md P2 for future triage, outside Phase 75 scope)

### 3. Desktop Search Responsiveness

expected:
- Functional baseline: all items in `docs/desktop-smoke-checklist.md` §2 "Basic Search" pass (type Hebrew query "שלום" → results table → click result → ResultDialog opens with metadata+text+images → close cleanly)
- Responsiveness overlay (D-06):
  - Subjective load time for results table ≤ pre-refactor recall
  - No new visible hitches/stalls during ResultDialog open/close
  - Composition search (D-09): short composition query (2–3 short chunks, ≤50 chunks target) — timer starts, ETA appears within ~2s, results stream in (cancel-with-partial-results NOT exercised — feature behavior, not regression surface)

result: passed (user approval 2026-04-17 — functional baseline and D-09 composition timer/ETA both green; user flagged two pre-existing perf issues (desktop search ~30s time-to-first-results; composition UI freezes ~15s before first composition result appears) — both confirmed **pre-existing** and NOT v7.9 decomposition regressions; logged to docs/OPEN_ISSUES.md P2 for future triage, outside Phase 75 scope)

### 4. Desktop Browse Responsiveness

expected:
- Functional baseline: all items in `docs/desktop-smoke-checklist.md` §4 "Browse Navigation" pass (select manuscript by shelfmark → images load → page forward/back → extended info panel → image adjustment sliders → fullscreen viewer)
- Responsiveness overlay (D-06):
  - Subjective image load time on Browse ≤ pre-refactor recall
  - No new visible hitches/stalls during pagination
  - Folio switching updates images without perceptible lag

result: pending

### 5. pytest baseline

expected: `python -m pytest tests/` returns exactly `1071 passed, 8 skipped` (1067 prior baseline + 4 new tests from 75-03 regression coverage: back-nav restore, fresh-query-different-saved guard, empty-snapshot edge, back-nav-restores-saved-mode-Title); tee output to `.planning/phases/75-non-regression-verification/75-pytest-baseline.txt` for verifier evidence (D-08 discretion default)

result: pending

## Summary
total: 5
passed: 3
issues: 0
pending: 2
skipped: 0
blocked: 0

## Gaps

- **Surface 1 blocker — back-navigation state loss (decomposition regression)** — Observed 2026-04-17. Symptom: hitting browser Back from `/browse` to `/` causes the search page to re-run the query from scratch instead of restoring the saved result state (chips, scroll position, result set). Expected behavior (live website `genizahsearch.com`): Back restores the prior search state. User confirmed side-by-side against live. Suspected regression origin: Phase 74 page-scoped state refactor (`web/pages/search.py`, `web/pages/search_state.py`, `web/search_bootstrap.py`) — `restore_search_snapshot()` / `persist_search_snapshot()` logic introduced in Phase 74 (see `.planning/phases/74-page-scoped-state-refactor/74-CONTEXT.md` D-20 URL-bar E2E regression check and Phase 74 snapshot helpers). Likely root cause: either the snapshot key/scope changed, the snapshot is not being read on route entry, or the bootstrap path now always treats `/` as a fresh load. Next action: run `/gsd-plan-phase 75 --gaps` to generate a fix plan; re-verify surface 1 after fix; surfaces 2–4 and pytest still pending per D-18 (pytest runs LAST, only after all manual surfaces pass). **Status:** CLOSED 2026-04-17 via plan 75-03 (fix in web/search_bootstrap.py + regression tests in tests/test_search_bootstrap.py + search_query storage-write hole closed in web/pages/search.py + elif-cascade reorder in web/pages/search.py so restored results render before auto-execute fires; user re-signed surface 1 green after Gemini-guided Option B follow-up to the initial 75-03 commit). True regression origin: commit 829cd7cf (2026-03-27), NOT Phase 74 as originally suspected.

## Notes on surface 1 items that passed

- (a) cold-load `/`: passed
- (b) Hebrew query `"שלום"` → results visible: passed
- (c) result accordion expand: passed
- (d) Browse navigation from result: passed (forward direction **and** Back direction after 75-03 fix)
- (e) Export with one checkbox ticked: **pre-existing bug** (exports whole list instead of the checked item) — NOT a v7.9 decomposition regression; logged to `docs/OPEN_ISSUES.md` §1 P2 for future triage, outside Phase 75 scope
- (f) Paginate forward twice: passed

## Notes on surface 2 items that passed

- (a) NLI-only manuscript renders: passed (substitute sys_id 990001273900205171 HUC MS. 2065 used because original Oxford arch. O.d.8/1 had no in-app data; Fixed Test Manuscripts table updated)
- (b) FJMS catalog + bibliography enrichment load: passed (on HUC 2065 substitute)
- (c) Cambridge T-S 12.123 CUDL image load: passed
- (d) Multi-IE Ms. Heb. 6972=8 volume selector + IE switch: passed
- (e) Folio Prev/Next updates URL bar (Phase 74 D-20 Cat-1 regression check): passed
- (f) JTS ENA 1052.1 Princeton DPUL spot-check: **pre-existing bug** (no source-switch button visible; image credit shows NLI for a JTS manuscript that should default to DPUL) — NOT a v7.9 decomposition regression, confirmed against live website; logged to `docs/OPEN_ISSUES.md` §1 P2 for future triage, outside Phase 75 scope

## Notes on surface 3 items that passed

- Functional baseline (basic search → ResultDialog open/close): passed
- Composition search D-09 timer + ETA mechanics: passed
- **Pre-existing perf issues flagged by user (NOT v7.9 regressions):**
  - Desktop search time-to-first-results ~30s on `"שלום"` — logged to `docs/OPEN_ISSUES.md` §1 P2
  - Desktop composition UI freezes ~15s before first composition result appears — logged to `docs/OPEN_ISSUES.md` §1 P2
