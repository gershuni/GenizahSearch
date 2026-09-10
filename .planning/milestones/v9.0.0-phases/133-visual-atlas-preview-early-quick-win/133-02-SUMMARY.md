---
phase: 133-visual-atlas-preview-early-quick-win
plan: 02
subsystem: infra
tags: [atlas, binary-schema, brotli, louvain, networkx, offline-bake, ci, typed-arrays]

# Dependency graph
requires:
  - phase: 133-01
    provides: "scripts/check_atlas_masking.py (D-07 masking scan) used as this plan's exit gate"
provides:
  - "docs/specs/atlas-asset-schema-v1.md -- the FROZEN, versioned binary-asset contract plan 133-04's decoder implements field-for-field"
  - "scripts/build_atlas_asset.py -- the offline bake: fork of the gitignored prototype's Louvain/force-layout/phyllotaxis pipeline, strips the discovery overlay (D-04), proves EXACT eligible==placed node-set equality (closing the ~13K node-inclusion gap), encodes to the frozen schema (typed/delta arrays + string heap), Brotli-compresses, content-hashes the filename, and writes a manifest"
  - "tests/atlas_bake/test_atlas_bake.py -- 10 tests locking the bake's masking-safe/claim-free/node-complete/byte-bounded/deterministic/content-hash-invalidating/sys_id-precision invariants, running in --smoke/--golden mode (no research DB needed)"
  - "tests/fixtures/atlas/golden-v1.bin(.br) + golden-v1-expected.json -- committed golden fixture (synthetic, masking-clean) for the Python decode test and the downstream 133-04 JS decode/DOM-XSS test"
  - "requirements-atlas-bake.txt + a pinned atlas-bake-tests CI job (with Node) -- the pinned bake-time environment 133-04's JS golden test will also run in"
affects: [133-03, 133-04, 133-05, 133-06]

# Tech tracking
tech-stack:
  added:
    - "networkx==3.6.1 (bake-time only)"
    - "python-louvain==0.16 (bake-time only)"
    - "Brotli==1.2.0 (bake-time only)"
  patterns:
    - "Frozen versioned binary schema authored BEFORE the encoder: fixed header + self-describing section table (id/dtype/elem_size/count/byte_offset/byte_length), every section 8-byte-padded regardless of its own element size"
    - "Plain-unsigned delta-encoded edges (source_delta, target_delta) with a group-reset rule that keeps both deltas non-negative -- no zigzag/signed encoding needed"
    - "Single BigUint64 sys_id representation with a hard bake-time pure-digit-<2**64 invariant and NO fallback path (fails the bake instead of demoting to a string heap)"
    - "Content-deduplicated single UTF-8 string heap with (offset,length) ref arrays, referenced by node title/shelfmark and cluster-label title/dominant-domain"
    - "Small static/dynamic lookup tables (domain_groups, bake-discovered libraries) live in manifest.json, not the binary -- keeps the typed-array payload lean"
    - "Content-hashed filename (atlas-v1-<sha256[:12]>.bin/.bin.br) so a rebake can never reuse a stale immutable-cache URL"
    - "--smoke/--golden synthetic-dataset generators that exercise the SAME clustering/layout/encode pipeline as the real DB path, guaranteeing at least one multi-node island-only component and one true singleton island-only component (the node-inclusion-gap edge cases) plus a sys_id > 2**53"
    - "pytest.importorskip + a path-based auto-marker (mirroring the render_smoke precedent) so a bake-time-only test module self-skips cleanly in the main CI job instead of erroring at collection"

key-files:
  created:
    - docs/specs/atlas-asset-schema-v1.md
    - scripts/build_atlas_asset.py
    - tests/atlas_bake/__init__.py
    - tests/atlas_bake/test_atlas_bake.py
    - tests/fixtures/atlas/golden-v1.bin
    - tests/fixtures/atlas/golden-v1.bin.br
    - tests/fixtures/atlas/golden-v1-expected.json
    - requirements-atlas-bake.txt
  modified:
    - .github/workflows/ci.yml
    - tests/conftest.py
    - pyproject.toml

key-decisions:
  - "Small fixed/dynamic lookup tables (13 FJMS domain groups' EN/HE/color, bake-discovered library codes) live in manifest.json rather than the binary string heap -- they're tiny, static per bake, and needed anyway for cross-checking; NODE_DOMAIN/NODE_LIBRARY are indices into these manifest arrays. This kept the binary schema to per-node/per-edge/per-cluster bulk data only."
  - "Edge deltas are plain unsigned Uint32 (no zigzag): sorting edges by (source asc, target asc) and defining target_delta as absolute-on-group-reset / incremental-within-group makes both deltas provably non-negative, avoiding sign-bit/zigzag complexity entirely."
  - "Island-only cluster construction reuses the SAME force-layout/dust-ring code path as continuation clusters (just with MIN_CLUSTER=1 and a graph restricted to island-only endpoint pairs) rather than a parallel/bespoke code path -- matches the plan's RESOLVED decision and avoids duplicated layout logic."
  - "encode_asset()/decode_asset() both live in scripts/build_atlas_asset.py (not a separate module) -- the plan's file list scoped this to the bake script, and co-locating the reference decoder next to the encoder that must match it field-for-field reduces schema drift risk."

requirements-completed: [ATLAS-01]

# Metrics
duration: 40min
completed: 2026-07-20
---

# Phase 133 Plan 02: Atlas Bake -- Frozen Schema, Encoder, Golden Fixture & CI Summary

**Forked the gitignored atlas prototype into a committed, schema-frozen offline bake (`scripts/build_atlas_asset.py`) that closes the ~13,000-manuscript node-inclusion gap with a proven exact-set-equality invariant, strips the discovery overlay, encodes to a versioned typed-array+Brotli binary under a hard 6 MB cap, and ships with a committed golden fixture, 10 invariant tests, and a pinned CI bake job.**

## Performance

- **Duration:** ~40 min
- **Completed:** 2026-07-20T16:30Z (commit timestamps 18:54-19:31 local)
- **Tasks:** 3/3
- **Files modified:** 12 (2 new docs/spec+script for Task 1, 1 modified script for Task 2, 9 new/modified test+CI+config files for Task 3)

## Accomplishments

- **`docs/specs/atlas-asset-schema-v1.md`** -- the complete, FROZEN, versioned binary contract authored before any encoder code: fixed 16-byte header (magic + schema_version + section_count), a 32-byte self-describing section-table entry format, all 23 sections (node positions/cluster/domain/library/prominence/sys_id/title-ref/shelfmark-ref, delta-encoded edges + a continuation/island class byte, aggregate inter-cluster flows, cluster labels, a single UTF-8 string heap), the dtype enum, the exact edge delta-decode algorithm, the sys_id BigUint64-only invariant, and a full step-by-step decode algorithm.
- **`scripts/build_atlas_asset.py`** -- forks the prototype's proven Louvain + bounded force-layout + phyllotaxis pipeline: strips the discovery overlay entirely (D-04, verified zero discovery-shaped keys anywhere in the manifest or decoded payload); closes the node-inclusion gap by building the eligible set from ALL manuscript-pair relations (continuation OR island), homing every island-only connected component (including true singletons) as its own micro-cluster through the SAME force-layout/dust-ring code path, then asserting `missing == [] and extra == []` (exact set equality, not `>=`); records seed=42 + an algo/version string; validates every sys_id as pure-digit-`<2**64` with NO fallback representation (fails the bake on any violation); encodes to the frozen schema with 8-byte-padded sections, a content-deduplicated string heap, and plain-unsigned delta-encoded edges; Brotli-compresses (quality 11); content-hashes the filename (`atlas-v1-<sha256[:12]>.bin(.br)`); asserts the 6 MB byte-budget cap; and writes a manifest with eligible/placed/missing/extra + the full section table. CLI: `db_path` is required unless `--smoke N` or `--golden PATH` drives a small deterministic synthetic (fabricated, never-real) dataset.
- **`tests/atlas_bake/test_atlas_bake.py`** -- 10 tests (the 8 required behaviors + 2 CLI-contract bonus tests) all passing in `--smoke`/golden mode with no research DB: no-discovery-fields, exact node-set equality (using a synthetic dataset engineered to include both a multi-node island-only chain AND a true singleton island-only component), byte-budget gate, sys_id BigUint64 roundtrip, sys_id-invalid-fails-the-bake, cross-run determinism (byte-identical output), content-hash invalidation on a single changed input byte, and a golden per-field Python decode (sys_id compared via `int(str)`, never float/Number, so precision loss above 2**53 can never mask a mismatch).
- **`tests/fixtures/atlas/golden-v1.bin`, `.bin.br`, `golden-v1-expected.json`** -- a tiny (30-node) deterministically-generated, fully fabricated synthetic graph: includes an island-only chain, a true island-only singleton, a sys_id > 2**53, and a deliberately fake XSS-shaped catalogue string (`<img src=x onerror=...></script>` + a bidi-control character) for the downstream 133-04 DOM-XSS decode test. Masking-scan clean.
- **CI wiring** -- `requirements-atlas-bake.txt` pins the 3 bake-time-only deps (verified installed at the exact pinned versions in this environment); `pyproject.toml` registers the `atlas_bake` marker; `tests/conftest.py` auto-applies it to `tests/atlas_bake/` (mirroring the existing `render_smoke` path-injection); `.github/workflows/ci.yml`'s default `tests` job now excludes `atlas_bake`, and a new dedicated `atlas-bake-tests` job installs the pinned deps + Node (for 133-04's companion JS golden-decode/DOM-XSS tests, which will share this same directory and marker) and runs `pytest tests/atlas_bake -m atlas_bake`.

## Task Commits

Each task was committed atomically with explicit-path staging (never `git add -A`):

1. **Task 1: Freeze the versioned binary schema + fork the bake core** -- `1836d828` (feat)
   - `docs/specs/atlas-asset-schema-v1.md`, `scripts/build_atlas_asset.py` (pipeline core + CLI arg validation; `--report` path only, no binary encoding yet)
2. **Task 2: Encode against the frozen schema (typed/delta arrays + string heap + Brotli)** -- `13cff3f0` (feat)
   - `scripts/build_atlas_asset.py` (added `encode_asset`/`decode_asset`/`build_manifest`/`assert_byte_budget`/`print_byte_breakdown`, wired `--golden`/production write paths)
3. **Task 3: Golden fixture + bake invariant tests + pinned CI bake job** -- `c18430c8` (test)
   - `tests/atlas_bake/__init__.py`, `tests/atlas_bake/test_atlas_bake.py`, `tests/fixtures/atlas/golden-v1.bin(.br)`, `tests/fixtures/atlas/golden-v1-expected.json`, `requirements-atlas-bake.txt`, `.github/workflows/ci.yml`, `tests/conftest.py`, `pyproject.toml`

No separate plan-metadata commit was needed beyond the final docs commit below (this SUMMARY + STATE/ROADMAP update).

## Files Created/Modified

- `docs/specs/atlas-asset-schema-v1.md` -- the frozen binary contract (header, section table, all 23 sections, decode algorithm)
- `scripts/build_atlas_asset.py` -- the offline bake (pipeline core + encoder + reference decoder + CLI)
- `tests/atlas_bake/__init__.py` -- package marker (mirrors the `tests/render_smoke/`/`tests/e2e/`/`tests/scripts/` convention)
- `tests/atlas_bake/test_atlas_bake.py` -- 10 bake-invariant tests
- `tests/fixtures/atlas/golden-v1.bin`, `.bin.br`, `golden-v1-expected.json` -- committed golden fixture
- `requirements-atlas-bake.txt` -- pinned bake-time-only deps
- `.github/workflows/ci.yml` -- excluded `atlas_bake` from the default `tests` job; added the `atlas-bake-tests` job
- `tests/conftest.py` -- auto-marks `tests/atlas_bake/` with `atlas_bake`
- `pyproject.toml` -- registers the `atlas_bake` marker

## Decisions Made

See `key-decisions` in the frontmatter above (lookup-table placement in manifest.json, plain-unsigned edge deltas via a group-reset rule, island-cluster code-path reuse, and co-locating the reference decoder with the encoder). All four are implementation-detail choices within the plan's RESOLVED decisions -- none required deviating from what the plan specified.

## Deviations from Plan

None -- plan executed exactly as written. The `tests/atlas_bake/__init__.py` package marker was added as necessary supporting infrastructure (not itself in the plan's `<files>` list) to match the repo's existing convention for test subdirectories that contain actual test modules (`tests/render_smoke/`, `tests/e2e/`, `tests/scripts/` all have one; `tests/fixtures/`, which holds only fixture data, does not). This is infrastructure, not a functional deviation.

## Issues Encountered

None. The synthetic dataset generator needed one iteration to correctly produce a TRUE singleton island-only component (a first draft accidentally chained it to an adjacent island node, so `test_exact_node_set_equality` would not have exercised that edge case) -- caught and fixed during my own pre-commit sanity checks, before any test file was written, so it never surfaced as a failing test.

## User Setup Required

None -- all bake-time dependencies (networkx/python-louvain/Brotli) are already installed in this dev environment at the exact pinned versions, and the CI job installs them fresh via `requirements-atlas-bake.txt`. No secrets or external service configuration needed.

## Next Phase Readiness

- **133-03** (the `/atlas` route + flag + static/precompressed serving) can now read `docs/specs/atlas-asset-schema-v1.md` for the exact byte layout it will serve, and can run the bake locally (`pip install -r requirements-atlas-bake.txt`, then `python scripts/build_atlas_asset.py --smoke 200 --out-dir atlas_data`) to get a placeholder asset while wiring the route/flag, before the phase-exit real bake (133-06) produces the production asset.
- **133-04** (the JS decoder) has the frozen schema doc to implement field-for-field, plus the committed golden fixture (`tests/fixtures/atlas/golden-v1.bin` + `golden-v1-expected.json`) and its embedded fabricated XSS-shaped string, ready to drive a JS golden-decode test and a DOM-XSS test in the same `tests/atlas_bake/` directory / `atlas_bake` marker / dedicated CI job (Node already provisioned there).
- **133-06** (phase-exit) will run `python scripts/build_atlas_asset.py <research-db>` against the real `fullcorpus_v2.db` and assert eligible==placed against the live ~62,645/floor-62,414 figures (this plan's tests only prove the LOGIC on synthetic data, per the plan's own module docstring/read_first note) -- and will run `python scripts/check_atlas_masking.py --scan-asset atlas_data/` against the real baked asset.
- No blockers. `atlas_data/` (this plan's local smoke-bake output, gitignored per 133-01) was left on disk from verification runs -- harmless, not committed, safe to delete or regenerate at any time.

## Self-Check: PASSED

- FOUND: `docs/specs/atlas-asset-schema-v1.md`
- FOUND: `scripts/build_atlas_asset.py`
- FOUND: `tests/atlas_bake/test_atlas_bake.py`
- FOUND: `tests/fixtures/atlas/golden-v1.bin`
- FOUND: `tests/fixtures/atlas/golden-v1.bin.br`
- FOUND: `tests/fixtures/atlas/golden-v1-expected.json`
- FOUND: `requirements-atlas-bake.txt`
- FOUND: commit `1836d828` in `git log --oneline --all`
- FOUND: commit `13cff3f0` in `git log --oneline --all`
- FOUND: commit `c18430c8` in `git log --oneline --all`

---
*Phase: 133-visual-atlas-preview-early-quick-win*
*Completed: 2026-07-20*
