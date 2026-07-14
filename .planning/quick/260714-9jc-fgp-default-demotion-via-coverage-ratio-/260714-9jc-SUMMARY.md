---
quick_id: 260714-9jc
slug: fgp-default-demotion-via-coverage-ratio
date: 2026-07-14
status: complete
seed: SEED-030
apps: both (web + desktop) + shared core
codex_review: APPROVE
---

# Quick Task 260714-9jc — SUMMARY

Implemented SEED-030: the reading-view "Manuscript Text" default now shows the V0.8/HTR (MiDRASH)
transcription instead of a **partial/selected** FGP edition (Gregor Schwarb report). Covers ALL
four version-chooser surfaces (web browse, web Advanced/search-results, desktop browse, desktop
ResultDialog). FGP stays selectable, tagged "shorter than V0.8".

## Coverage policy (`shared/fgp_service.py::choose_default_source`, pure)

Coverage = FGP edition's base-Hebrew-letter count ÷ the HTR letters of the RIGHT baseline:
- **Foliated / c-numbered** rows (per-image) → the DISPLAYED folio's HTR.
- **Whole-document** rows (`_fgp_is_whole_doc`: no folio label AND no c_number) → the
  WHOLE-manuscript HTR via a lazy `full_htr_getter` — so a comprehensive whole-doc transcription
  (≈ full MS) stays default while a selective excerpt (Firkovich, ~2.6% of the MS) is demoted.
Editions only (never translations); demote when the best edition's coverage < threshold
(`_DEFAULT_MIN_COVERAGE=0.33`, env `FGP_DEFAULT_MIN_COVERAGE`); keep when the baseline is too
short to trust (`_COVERAGE_MIN_HTR_LETTERS=40`). `fgp_needs_full_htr()` lets callers gate the
full-MS fetch (only when a whole-doc edition is present).

**Design note (user decision 2026-07-14):** a selective whole-doc row is demoted on EVERY folio,
including blank-HTR folios (a proposed "keep FGP when the folio's own HTR is blank" refinement was
explicitly rejected — keep the straight demotion). So opening a MS at a blank cover folio shows an
empty V0.8 by default; navigate to a text folio to see the MiDRASH HTR.

## Wiring (all surfaces render the shared decision)

- **web browse:** `version_selector.load_and_apply_latest` + menu "shorter than V0.8" hint;
  `browse_enrichment` fetches whole-MS HTR off the event loop (gated), threads it via
  `BrowseState.fgp_full_htr_text` → `create_version_selector(full_original_text=…)`.
- **web Advanced/search-results:** `search_results.py` routes the initial display text through the
  policy (PGP-first, then FGP-if-eligible) + passes `full_original_text`.
- **desktop browse + ResultDialog:** `_auto_select_pgp_edition`/`_populate_pgp_combo` now take the
  caller's `sys_id`/`htr_text` (never read `self.browse_*`); each caller passes its own context.
  Full-MS HTR cached single-entry per document (`_browse_full_htr_text`, `self.searcher`).

## Verification

- 26 shared unit tests (`tests/test_fgp_default_coverage.py`) + static wiring guards for all
  surfaces + the context-parametrized desktop signatures. `pytest` green (165 in the
  fgp/version-selector set).
- **Dry-run on the real reported shelfmark `990000925330205171`**: FGP = 772 letters, whole MS =
  29,780 → ratio 0.026 → **demote to V0.8** on both a blank and a full folio. ✓

## Gate: Codex code-review — APPROVE (4 rounds)

R1 CHANGES REQUESTED (HIGH recto/verso page-num bug; MEDIUM desktop best-of-multiple; LOW tests) →
fixed. R2 CHANGES REQUESTED after the whole-MS-baseline UAT revision (HIGH c-number misclassify;
HIGH web Advanced surface; MEDIUM/`ResultDialog` cross-context) → fixed. R3/R4 **APPROVE, no
findings**. See `260714-9jc-REVIEW.md`.

## Commits (on master-main)

`c56b2491` shared policy + tests · `a057366c` web browse · `93393404` desktop browse ·
`1d3ef49c` R1 fixes · `fd331e43` whole-MS baseline (UAT) · `6166fdf5` F1/F2/F3 (c-number + Advanced
view + desktop cross-context).

## Deferred / follow-ups

- Threshold `0.33` is env-tunable live; watch real usage.
- Catalog(500) hard-demote still deferred (coverage subsumes it).
- **Not released** — committed on master-main only; web deploy + desktop rebuild/bundle are a
  separate release step. Desktop must be re-run from source or rebuilt to pick up the change.
- Live web render-smoke + a desktop UAT pass on a real Firkovich shelfmark recommended pre-release.
