# 0004 -- A feature flag is necessary but never sufficient

- **Status:** binding
- **Date:** 2026-07 (Phase 133/134 discovery launch work); restated in CLAUDE.md "Environment Variables"
- **Source:** the predicates themselves -- `discovery_available()` in
  [web/discovery_assets.py](../../web/discovery_assets.py), `passage_available()` in
  [web/passage_assets.py](../../web/passage_assets.py), `atlas_preview_available()` in
  [web/atlas_assets.py](../../web/atlas_assets.py) -- and the flag table in CLAUDE.md.

## Decision

Every surface behind a feature flag (`DISCOVERY_ENABLED`, `ATLAS_PREVIEW_ENABLED`,
`PASSAGE_PARALLELS_ENABLED`, `PASSAGE_MULTI_WITNESS_ENABLED`, ...) is gated on **one** predicate that
ANDs the flag with a fail-closed readiness check of the data it needs. A flag alone is never treated
as proof that a feature is live; a missing or invalid asset hides the surface cleanly.

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
