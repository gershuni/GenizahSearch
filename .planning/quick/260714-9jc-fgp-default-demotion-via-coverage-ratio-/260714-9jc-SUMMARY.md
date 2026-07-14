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
transcription instead of a **partial/selected** FGP edition (Gregor Schwarb report; Firkovich
median ~9% coverage). The FGP source stays selectable, tagged "shorter than V0.8".

## What changed

- **`shared/fgp_service.py`** — new pure `choose_default_source(sources, htr_text)` +
  `_heb_letter_count()` + `_min_coverage()` (const `_DEFAULT_MIN_COVERAGE=0.33`, env
  `FGP_DEFAULT_MIN_COVERAGE`, HTR floor `_COVERAGE_MIN_HTR_LETTERS=40`). Coverage = FGP base-Hebrew
  letters ÷ HTR base-Hebrew letters (reusing the module's `_HEBWORD_RE`/`_NIKUD_RE`: strips
  nikud/te'amim/punct/whitespace and `][` markers, keeps bracketed letters). Editions only (never
  translations); fails toward KEEPING FGP when the HTR baseline is too short; measures the
  displayed whole-row `content` (never a section — FGP content is not narrowed on display).
- **`web/components/version_selector.py`** — `load_and_apply_latest` routes the FGP-vs-HTR fallback
  through the policy; demoted FGP falls through to the V0.8 default. Menu render adds the bilingual
  "shorter than V0.8" hint on demoted FGP editions.
- **`genizah_app.py`** — `_auto_select_pgp_edition` parity: PGP-first unchanged; demoted FGP →
  select V0.8; eligible FGP → select the policy's chosen edition by `source_id`. `_populate_pgp_combo`
  appends the hint to demoted FGP labels.
- **`genizah_translations.py`** — HE key `"shorter than V0.8" → "קצר מ-V0.8"`.
- **`tests/test_fgp_default_coverage.py`** — 18 tests (normalization, threshold, HTR-floor,
  translation exclusion, content-vs-section, no-page-dependence regression guard, best-of-multiple,
  env override) + static wiring guards. `pytest` green (119 in the fgp/version-selector set).

## Constraints honored

No DB change, no reindex. PGP-first preserved. Pure/testable helper (dry-runnable against a
shelfmark). Web + desktop share ONE policy.

## Gate: Codex code-review — APPROVE

Two rounds. Round 1 (CHANGES REQUESTED): HIGH recto/verso page-number bug in the coverage
measurement, MEDIUM desktop first-vs-best edition, LOW shallow tests. All fixed (commit
`1d3ef49c`). Round 2: **APPROVE**, no new defects. See `260714-9jc-REVIEW.md`.

## Commits

- `c56b2491` shared policy + tests
- `a057366c` web wiring + hint + translation
- `93393404` desktop parity + wiring guards
- `1d3ef49c` Codex review fixes (measure displayed content; desktop best-of-multiple)

## Deferred / follow-ups

- Threshold `0.33` is a first cut (env-tunable live). Watch real usage; retune if needed.
- Catalog(500) hard-demote deferred (coverage already catches short catalog stubs); would need a
  proper `source_class` field, not `image_id`-prefix parsing.
- Live render-smoke on web + a desktop manual smoke on a real Firkovich shelfmark (headless GUI
  can't be driven here) — recommended before the next release ships this.
- Not yet released: this is committed on `master-main`; web deploy + desktop bundle are a separate
  release step.
