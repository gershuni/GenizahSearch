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
| NLI-only with crossref (web browse step a) | 990053385670205171 | arch. O.d.8/1 | Oxford | nli_crossref.db nli_images row; no Cambridge/JTS crossref overlap |
| Multi-IE (web browse step d) | 990000412990205171 | Ms. Heb. 6972=8 | Allony (NLI) | IE104549337, IE19213988 — 2 IEs, 7 trans FLs |
| JTS DPUL (web browse spot check) | 990053572370205171 | ENA 1052.1 | JTS | Princeton DPUL image path — v7.2.3 regression surface |

## Current Test

[none — not yet started]

## Tests

### 1. Web Search Responsiveness

expected:
- (a) Cold-load `/` → first paint appears without visible delay vs pre-refactor recall
- (b) Execute text query `"שלום"` → first results visible; subjective time ≤ pre-refactor recall
- (c) Expand one result accordion → opens without hitch
- (d) Click "Browse" on one result → navigates to /browse without visible stall
- (e) Click "Export" on one result → export action completes without UI block
- (f) Paginate forward twice → next-page results appear without perceptible slowdown

result: pending

### 2. Web Browse Responsiveness

expected:
- (a) Navigate `/browse?sys_id=990053385670205171` → manuscript visible; subjective load time ≤ pre-refactor recall
- (b) Enrichment panel populates (FJMS catalog, bibliography) → Phase A/B deferred load completes without visible stall
- (c) Navigate `/browse?sys_id=990051334060205171` → CUDL image loads; no new hitches
- (d) Navigate `/browse?sys_id=990000412990205171` → volume selector renders; switching between IE104549337 and IE19213988 updates image + text without stall
- (e) Folio Prev/Next updates the URL bar (Phase 74 D-20 E2E regression check — confirms Cat-1 fix holds under real use)
- (f) Spot-check: navigate `/browse?sys_id=990053572370205171` → Princeton DPUL image path still works (v7.2.3 regression surface)

result: pending

### 3. Desktop Search Responsiveness

expected:
- Functional baseline: all items in `docs/desktop-smoke-checklist.md` §2 "Basic Search" pass (type Hebrew query "שלום" → results table → click result → ResultDialog opens with metadata+text+images → close cleanly)
- Responsiveness overlay (D-06):
  - Subjective load time for results table ≤ pre-refactor recall
  - No new visible hitches/stalls during ResultDialog open/close
  - Composition search (D-09): short composition query (2–3 short chunks, ≤50 chunks target) — timer starts, ETA appears within ~2s, results stream in (cancel-with-partial-results NOT exercised — feature behavior, not regression surface)

result: pending

### 4. Desktop Browse Responsiveness

expected:
- Functional baseline: all items in `docs/desktop-smoke-checklist.md` §4 "Browse Navigation" pass (select manuscript by shelfmark → images load → page forward/back → extended info panel → image adjustment sliders → fullscreen viewer)
- Responsiveness overlay (D-06):
  - Subjective image load time on Browse ≤ pre-refactor recall
  - No new visible hitches/stalls during pagination
  - Folio switching updates images without perceptible lag

result: pending

### 5. pytest baseline

expected: `python -m pytest tests/` returns exactly `1067 passed, 8 skipped` (no new failures, no new skips); tee output to `.planning/phases/75-non-regression-verification/75-pytest-baseline.txt` for verifier evidence (D-08 discretion default)

result: pending

## Summary
total: 5
passed: 0
issues: 0
pending: 5
skipped: 0
blocked: 0

## Gaps

[none yet — appended by walkthrough plan (75-02) if any surface fails per D-15 triage]
