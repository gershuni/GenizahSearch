# 0009 -- Restricted-source strings never appear in tracked files or shipped surfaces

- **Status:** binding
- **Date:** Phase 133 onward (2026-07)
- **Source:** the masking scan, [scripts/check_atlas_masking.py](../../scripts/check_atlas_masking.py),
  and the `MASKING_SCAN_PATTERNS_FILE` entry in [docs/guides/ENV_VARS.md](../guides/ENV_VARS.md).

## Decision

The names of the restricted source corpora are masked codes in every tracked file, every shipped
artifact and every public surface. The pattern file that defines them is a gitignored secret; the
scan reads it from `MASKING_SCAN_PATTERNS_FILE` and **fails (exit 1) when the variable is unset**,
never silently passing.

## Consequences

- Set the variable before running the scan or any export; a red scan with no file set is the tool
  working.
- The scan also matches path strings, so it runs **after** a `git mv`, not before.
- Evidence and the per-track details live in `docs/specs/discovery-v3-masking-evidence.md`.

This page deliberately records only the rule and its enforcer; nothing here names a pattern.

## Enforced by

[scripts/check_atlas_masking.py](../../scripts/check_atlas_masking.py) (`--scan-repo`), whose
mechanism is proved unconditionally by
[tests/test_atlas_masking_scan.py](../../tests/test_atlas_masking_scan.py) with fabricated needles
and no secret. The secret-backed surface sweep runs in exactly one CI job, `render-smoke-tests`,
which writes the `MASKING_SCAN_PATTERNS` repository secret to a gitignored path for the length of
the job and goes red by design when the secret is missing (`.github/workflows/ci.yml`); a masking
assertion added to any other job runs without a pattern set.
