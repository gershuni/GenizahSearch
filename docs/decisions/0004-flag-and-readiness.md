# 0004 -- An asset-backed feature flag is necessary but never sufficient

- **Status:** binding
- **Date:** 2026-07 (Phase 133/134 discovery launch work); restated in CLAUDE.md "Environment Variables"
- **Source:** the predicates themselves -- `discovery_available()` in
  [web/discovery_assets.py](../../web/discovery_assets.py), `passage_available()` in
  [web/passage_assets.py](../../web/passage_assets.py), `atlas_preview_available()` in
  [web/atlas_assets.py](../../web/atlas_assets.py) -- and the flag table in CLAUDE.md.

## Decision

Every surface behind a flag whose feature depends on a **provisioned asset** -- `DISCOVERY_ENABLED`
(the discovery sidecar), `ATLAS_PREVIEW_ENABLED` (the baked atlas), `PASSAGE_PARALLELS_ENABLED` and
`PASSAGE_MULTI_WITNESS_ENABLED` (the passage index), `FGP_TRANSCRIPTIONS_ENABLED` (`fgp_data/`) --
is gated on **one** predicate that ANDs the flag with a fail-closed readiness check of that data.
Such a flag alone is never treated as proof that the feature is live; a missing or invalid asset
hides the surface cleanly. Plain toggles with no data behind them -- `WEB_PUZZLE_ENABLED`,
`IDENTIFICATION_REVIEWS_ENABLED`, `SEARCH_API_MODE` -- are read directly, and this decision does
not ask them to grow a readiness predicate.

**One deliberate exception, kept on purpose:** the browse page's connections panel. Its
*existence* is gated by the flag alone (`web/pages/browse_enrichment.py::discovery_panel_enabled`
reads `DISCOVERY_ENABLED` directly) while its *status* is gated by `discovery_available()`, so a
flag-ON/sidecar-missing window shows the panel in a "temporarily unavailable" state instead of
making it vanish. `tests/test_discovery_panel_browse_wiring.py` pins that distinction; do not
"fix" it into the blanket rule.

## Why

Deploys are not atomic (assets go by `scp`, code by `git push`), and a flag can be flipped ahead of
its data or its gate -- the discovery beta was flipped on in production on 2026-08-08 ahead of the
REL-01 gate. Fail-closed readiness turns that window into "not shown" instead of an error page.

## Consequences

- A new gated surface calls the existing predicate; it does not read the environment variable itself.
- `GENIZAH_DISCOVERY_DATA_DIR` / `GENIZAH_PASSAGE_DATA_DIR` select which directory is read (dev/CI
  only, read once at import); the loader still applies its full contract.
- Tests for a gated surface must cover the flag-ON/asset-missing case.

## Supersedes

Nothing; this records the rule so that it is not re-derived per feature.

## Enforced by

The predicates' own tests, which cover the flag-ON/asset-missing case:
[tests/test_discovery_flag.py](../../tests/test_discovery_flag.py),
[tests/test_atlas_flag_gating.py](../../tests/test_atlas_flag_gating.py) and
[tests/test_desktop_passage_gate.py](../../tests/test_desktop_passage_gate.py).
