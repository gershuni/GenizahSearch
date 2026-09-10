# Phase 134 Discovery Data Spine — Codex CODE-review Round 1 Rework

**Status:** REWORK COMPLETE. All 9 findings fixed, each with a regression test, atomic commit,
and green gate suite. No fabricated/invented severities or dispositions — every fix below is a
real code change, verified against the pinned golden fixture and (where noted) the real
gitignored research corpus.

**Executor:** Claude (Sonnet 5), sequential execution on `master-main` (no worktree).

**Context obeyed:** `.planning/phases/134-discovery-data-spine/134-CONTEXT.md` (C-4/C-7/R1 band
contract, C-1 Q2 counts), `docs/specs/discovery-sidecar-schema-v1.md` (§1.6 band_precision,
§4 source-of-truth counts, §6 display-precedence lattice), `docs/specs/discovery-budgets.md`
(pagination/concurrency/LRU/timeout caps).

**Frozen artifacts untouched:** `scripts/discovery_ids.py` (byte-identical), the evidence_id/
claim_id recipe and enum-vocab sections of the schema doc (byte-identical), and
`tests/fixtures/discovery/discovery-v1-fixture.db` + its `manifest.json`/`expected.json` (the
pinned 134-03 golden fixture) — confirmed byte-identical: `verify_discovery_sidecar.py` against
it stays clean, and `populate_synthetic`'s own placeholder band_precision rows
(`_band_precision_rows`, tier_a=0.90) were deliberately preserved as the SYNTHETIC-mode-only
path so the fixture's bytes never move.

---

## Findings and Dispositions

### H1 — `get_work_witnesses` truncated before projecting (silently dropped units)

**Disposition: FIXED.** Commit `e7d09f57`.

The DATA-10 unit×work projection (grouping raw witness claims into physical-MS
`witness_units`, selecting each unit's highest-ranked band, anchor-exclusion,
enabled-band filtering, deterministic ordering) now runs entirely IN SQL, via a
`ROW_NUMBER() OVER (PARTITION BY unit_key ORDER BY band_rank ASC, sys_id ASC)`
window query over a shared CTE (`_WORK_WITNESSES_RANKED_CTE_SQL`, keyed by a
frozen band-rank `CASE` expression built from `_BAND_RANK_ORDER`). `LIMIT`/`OFFSET`
now paginate over UNITS post-grouping, never over a truncated pre-grouping raw-claim
scan. The old `_MAX_RAW_CLAIMS_PER_WORK=5000` cap and the whole-`witness_unit_members`
table load (`_MAX_UNIT_MEMBER_ROWS=200_000`) are both removed. A small follow-up query
(bounded to the current page's unit count) fetches each returned unit's member sys_ids
for on-demand expansion.

`_project_work_witnesses` (the pure, DB-free Python reference implementation) is kept
completely unchanged — every existing DATA-10 unit test (`test_data10_*`) that calls it
directly, plus every fixture-integration test that calls `get_work_witnesses` against the
committed golden fixture, stays green.

**New regression test:** `test_get_work_witnesses_no_truncation_beyond_old_5000_claim_cap`
(`tests/test_discovery_service.py`) builds a synthetic work with 5,200 unmerged singleton
witness claims (> the old 5,000 cap) and proves all 5,200 units are reachable across pages
with none dropped.

**Real-corpus validation:** see the "Real-corpus smoke validation" section below.

---

### H2 — missing Q2/E1 input silently ingested as empty (tier-A-only sidecar passes every gate)

**Disposition: FIXED.** Commit `6681068d`.

Added `_assert_release_inputs_complete`, wired into `finalize_build` behind a new
`release: bool = False` parameter. When `release=True`, every frozen release input —
`e1_ra_confirmed` (1,570), `e1_adjudicated_a` (174), `e1_rb_screening` (7,498),
`e1_r3_frame` (9,996), `q2_witness_collection` (4,367), `q2_shared_text` (60,156),
`q2_collection_tafsir_targum` (106), `q2_collection_with_arabic` (108), and
`track1_matches WHERE shadowed_by IS NULL` (275,894, counted directly from the
research DB) — is REQUIRED present at its EXACT frozen expected row count, checked
BEFORE any ingest into claims/evidence. Any absent/short/long input raises
`ReleaseInputsIncompleteError` and aborts, before any output file is even opened.

`allow_partial_sources: bool = False` is the ONLY sanctioned escape hatch, and is
explicitly rejected in combination with `release=True` (raises `ValueError`). The CLI
(`--release` / `--allow-partial-sources`, mutually exclusive) requires an EXPLICIT
choice for real-mode distillation — there is no silent default; omitting both is a
`parser.error`.

The verifier's own release-contract count check (`check_release_contract_counts`,
already existing) is explicitly NOT relied on as the primary fix — that check only
re-validates build-written `meta` counts against the finished `.db`'s own actual row
counts, which is self-consistent-but-wrong for a partial build (a tier-A-only sidecar
would still pass it). `_assert_release_inputs_complete` is the primary, input-side fix.

**New regression tests** (`tests/test_discovery_build.py`): direct unit tests of
`_assert_release_inputs_complete` for a missing collection, a short collection, a
tier_a count mismatch, the `release`+`allow_partial_sources` combination raising, a
fully-conforming pass case, and a non-release no-op case; plus one end-to-end
`finalize_build(release=True, frozen_precision_defaults=True)` test (no collection
paths supplied) proving `ReleaseInputsIncompleteError` is actually wired in and the
output `.db` is not left on disk.

**Real-corpus validation:** see below — the real research corpus's 8 collection files
plus the `track1_matches` count match the frozen contract EXACTLY (verified before
running the real smoke build), confirming the hardcoded expected-count constants are
correct against ground truth, not just self-consistent with the docs.

---

### H3 — real-mode build wrote a manufactured `tier_a=0.90` precision row

**Disposition: FIXED.** Commit `6681068d` (combined with H2 — both are real-mode
`finalize_build` safety fixes touching the same function).

`_band_precision_rows()` (the function real-mode previously defaulted to) is now
documented and used EXCLUSIVELY by `populate_synthetic` — i.e. the SYNTHETIC-mode
fixture path — and its placeholder `tier_a=0.90` row is explicitly called out as
fabricated-and-synthetic-only in its docstring. A new `_frozen_real_band_precision_rows()`
carries the FROZEN real-mode default spec: `tier_a` precision is `None` (there is no
measured tier_a interval in the frozen contract); the three MEASURED track1_direct
bands (`expert_verified`=0.889, `screening_rb`=0.859, `screening_canon`=0.647) and the
collection-level 0.926 [0.875,0.968] are the documented frozen-contract values.

`_resolve_band_precision_spec` (extracted as its own directly-testable function)
implements the precedence: an explicit `--precision-spec <json>` (owner-supplied at
134-07) always wins; otherwise an explicit `--frozen-precision-defaults`
acknowledgement uses the frozen defaults; a `release=True` build with NEITHER
supplied raises `ValueError` outright — a real/release payload must never silently
fabricate a number. Non-release calls (unit tests, `--allow-partial-sources` smoke
builds) default to the SAME frozen-contract rows (tier_a NULL) rather than the old
`_band_precision_rows()` fallback, so NO code path in real mode can ever write the
0.90 placeholder anymore.

**New regression tests** (`tests/test_discovery_build.py`): direct unit tests of
`_resolve_band_precision_spec` (explicit spec wins, frozen defaults yield tier_a=NULL
with the three measured bands present, `release=True` with neither raises, non-release
defaults to the frozen rows); plus an end-to-end `finalize_build` test confirming the
written `band_precision` table's `tier_a` row is NULL, not 0.90.

**Real-corpus validation:** see below — a real `--release --frozen-precision-defaults`
build was run against the actual research corpus; `tier_a` precision in the resulting
`band_precision` table is confirmed NULL.

---

### M1 — `assign_opaque_work_ids` trusted every persisted crosswalk value

**Disposition: FIXED.** Commit `3e57a3e2`.

Added `_validate_crosswalk` (format: matches `scripts/discovery_ids.py::mint_work_id`'s
frozen shape `^w\d{6}$`, mirrored locally via a regex — never imports/edits the frozen
module; uniqueness: 1:1, no two raw work_ids sharing one opaque id), called on the
persisted crosswalk immediately after load AND again immediately before persisting/
returning. Any malformed or duplicated value raises `CrosswalkValidationError`
BEFORE any candidate/work_id is assigned, any review artifact is emitted, or any
sidecar is built.

**New regression tests** (`tests/test_discovery_build.py`): a malformed persisted value
(a raw-shaped identifier), a duplicated opaque value shared by two raw work_ids, and a
positive round-trip case (a well-formed persisted crosswalk still works exactly as
before).

---

### M2 — `finalize_build`'s masking gate not fully fail-closed

**Disposition: FIXED.** Commit `10a61867`.

Pattern-loading (`_require_patterns`) and the scan itself (`scan_sqlite`) are now
wrapped in the SAME `try/except Exception` block as the hit check. On ANY exception
from this block — not just an actual masking hit — the output `.db` is deleted before
the exception propagates. Previously, an empty pattern set or a scan-time failure
(a `ScanError`) left the fully-written, never-successfully-scanned `.db` on disk
untouched — a half-finalized, unproven-clean artifact. The existing Windows
cursor-close-before-connection-close ordering fix (134-04) is preserved unchanged.

**New regression tests** (`tests/test_discovery_build.py`): `scan_sqlite` monkeypatched
to raise a `ScanError` confirms the output `.db` is deleted; the existing empty-pattern-set
test (`test_finalize_build_requires_nonempty_masking_patterns`) gained an assertion that
the `.db` is also removed on that path.

---

### M3 — `DiscoveryService` env-derived limits could be raised/crash/unbound

**Disposition: FIXED.** Commit `a10394fb`.

- `DISCOVERY_PAGE_SIZE_MAX` can no longer raise the frozen 200-row absolute ceiling
  (`docs/specs/discovery-budgets.md` §3: "never overridable above this"); it can only
  TIGHTEN it. A non-positive or out-of-range env value falls back to the ceiling.
- `DISCOVERY_MAX_CONCURRENT_QUERIES` set to `<= 0` previously either crashed
  `asyncio.Semaphore()` construction (negative) or permanently over-locked it (zero);
  now coerces to `>= 1` with a safe fallback via `_get_positive_int_env`.
- `DISCOVERY_QUERY_TIMEOUT_BROWSE`/`_WORK` set to `<= 0` made `asyncio.wait(...)`
  return instantly on every call (permanent overload); now coerces to `> 0` via
  `_get_positive_float_env`.
- `DISCOVERY_BROWSE_LRU_MAX_ENTRIES` set to `<= 0` previously disabled eviction
  entirely (the `while max_entries > 0` guard never fired) — unbounded growth. Now a
  non-positive size disables AND clears the cache (bounded to zero), never unbounded.

**New regression tests** (`tests/test_discovery_service.py`): page-size-max cannot
exceed 200 even with an absurd env value; a non-positive page-size-max falls back to
the ceiling; negative/zero concurrency doesn't crash construction; non-positive
timeouts fall back to a positive default; a non-positive LRU size disables AND clears
the cache (proven by asserting the dict is empty after the flip, and stays empty on a
subsequent call).

---

### M4 — `check_band_precision` too lax for a release build

**Disposition: FIXED.** Commit `8c4e2098`.

`check_band_precision` now also takes `meta` and, gated on
`meta['sidecar_version'] == build_discovery_sidecar.REAL_SIDECAR_VERSION` (so it NEVER
fires against the synthetic-fixture sidecar_version, keeping the pinned 134-03 golden
fixture on its existing looser, `expected.json`-driven checks), runs a new
`_check_band_precision_release_strict` validation: exactly one `scope='collection'`
row with `collection_id='propagated_witness_collection_v1'`, precision≈0.926 with
non-null numerator/denominator/ci/method; BOTH propagated bands (`corroborated`,
`weak`) present with NULL precision/ci; the THREE measured track1_direct bands
present at their exact frozen values (0.889/0.859/0.647); `tier_a` present with NULL
precision; and any OTHER band carrying a non-null precision is rejected.

**New regression tests** (`tests/test_discovery_release_contract.py`): a
frozen-real-rows db passes the strict check cleanly; a db carrying the OLD fabricated
`tier_a=0.90` shape is rejected specifically on `tier_a`; a db missing a measured band
(`screening_canon`) is rejected; the same fabricated-tier_a rows do NOT trigger any
`(M4)` violation when the sidecar_version is the synthetic-fixture constant (proving
the gate never fires on synthetic data); and the committed golden fixture itself is
confirmed to trigger zero `(M4)` violations end-to-end.

---

### L1 — default reads exposed review-only rows

**Disposition: FIXED.** Commit `3b9619b3`.

`get_claims_for_page` and `get_pages_related_to_page` now default to
`routing_status='shipped'` only; a new `include_review: bool = False` opt-in parameter
reveals `review_only` rows (e.g. the family-router `tafsir_targum`/`with_arabic`
co-citation collections) on explicit request. The async wrappers pass the opt-in
through via a DISTINCT LRU `cache_name` (rather than appending a 4th positional arg to
the sync call), so the default call shape — and the existing monkeypatch-based tests
that replace `get_claims_for_page`/`get_pages_related_to_page` with a 3-positional-arg
fake — are completely unaffected.

**New regression tests** (`tests/test_discovery_service.py`): the fixture's `p010`
family-router (review_only) claim/evidence is hidden via both methods by default and
revealed only with `include_review=True`.

---

### L2 — evidence_id equal-priority dedup was order-dependent

**Disposition: FIXED.** Commit `b84b11e0`.

The shipped-over-review_only preference (134-04) is unchanged. For an EQUAL-priority
collision (both shipped, or both review_only), the two rows' full persisted content
(`_evidence_content_key`, every column that ends up in the `discovery_evidence` INSERT)
is now compared. Identical content (a true duplicate, e.g. a repeated JSONL line)
dedupes silently exactly as before — harmless, order-independent since either row is
byte-identical. Divergent content now raises `EvidenceIdCollisionError` fail-closed
instead of silently picking whichever row happened to be first in
`evidence_specs` iteration order.

**New regression tests** (`tests/test_discovery_build.py`): an identical-content
equal-priority collision dedupes without raising; a content-divergent equal-priority
collision (same evidence_id-key fields, different `tier`/`aligned_len`) raises
`EvidenceIdCollisionError`.

---

## Real-corpus smoke validation

The gitignored research corpus (`same_work_spike/probe/data/fullcorpus_v2.db` plus the
Q2/E1 collections, `libraries.csv`, `fist_data/fjms_enrichment.db`) IS present on this
machine, so — per the task's "STRONGLY ENCOURAGED" guidance — a real open-corpus-only
smoke build was run to validate H1/H2/H3 against real data (not a CI gate; no artifacts
committed; everything stayed in gitignored `discovery_data/`).

**Frozen-count ground-truth check (before running any build):** every one of the 8
frozen Q2/E1 collection files' actual line counts, plus the actual
`track1_matches WHERE shadowed_by IS NULL` count from `fullcorpus_v2.db`, were
compared directly against the hardcoded `_EXPECTED_*` constants added for H2 — **all 9
matched EXACTLY** (1,570 / 174 / 7,498 / 9,996 / 4,367 / 60,156 / 106 / 108 / 275,894).
This confirms the H2 constants are correct against ground truth, not merely
self-consistent with the docs.

**Real `--release --frozen-precision-defaults` build:** reusing the prior session's
`discovery_data/crosswalk.json` (persisted raw→opaque work_id crosswalk) and
`discovery_data/discovery-review-approved-smoke.csv` (open-corpus-only approved works —
no owner M-source review has happened, so this is not a ship-worthy build content-wise,
only a pipeline/gate exercise), run via:

```
python scripts/build_discovery_sidecar.py same_work_spike/probe/data/fullcorpus_v2.db \
  --from-approved discovery_data/discovery-review-approved-smoke.csv \
  --crosswalk discovery_data/crosswalk.json \
  --research-data-dir same_work_spike/probe/data \
  --libraries-csv libraries.csv \
  --fjms-db fist_data/fjms_enrichment.db \
  --out discovery_data/discovery-v1-real-smoke-rework.db \
  --review-artifact discovery_data/discovery-review-candidates-rework.csv \
  --release --frozen-precision-defaults
```

**Result: `real build OK: {'works': 625, 'discovery_claim': 231604, 'discovery_evidence':
251976, 'witness_units': 5547}`** — identical row counts to the prior 134-04 smoke
build (as expected, same corpus/inputs), `evidence_id_collisions=115`.
`content_hash=60425ad5f3ec1f5101b9bd9cf7e9fb51aef0673cd5b450f947a9f0466075abd8`,
`frame_content_hash=738c33980a01084048da47f3f91ae999cf8cc4e3be4f3bcd2064fc1c82e9cf9c`.

- `python scripts/verify_discovery_sidecar.py discovery_data/discovery-v1-real-smoke-rework.db --expected-frame-hash 738c33980a...` → **clean, all invariants pass** (H2 gate did not block this build precisely BECAUSE every frozen input was present at its exact count — confirming the gate is not overly strict on a genuinely-complete input set).
- `MASKING_SCAN_PATTERNS_FILE=.masking_patterns python scripts/check_atlas_masking.py --scan-sqlite discovery_data/discovery-v1-real-smoke-rework.db` → **clean, no matches** (masking boundary holds on the real release-mode build).
- **H3 confirmed against real data:** the written `band_precision` table shows `tier_a` (`track1_direct`/`tier_a`) precision **NULL**; the three measured track1_direct bands carry their frozen values (`expert_verified`=0.889, `screening_rb`=0.859, `screening_canon`=0.647); the collection row carries 0.926; both propagated bands (`corroborated`, `weak`) carry NULL — exactly the frozen contract shape, with NO fabricated tier_a number anywhere.
- **H1 confirmed against the EXACT real-world case cited in the original finding:** `work_id='w000112'` carries **18,943** raw witness-claim rows (matching the finding's cited number exactly) which project to **5,684** ground-truth distinct `witness_units`/singleton units (matching the finding's cited "true 5,684" exactly). Calling the FIXED `DiscoveryService.get_work_witnesses('w000112', ...)` across all pages (page_size=200) now returns **all 5,684** units — a complete match with zero dropped, where the pre-fix code returned only 531 (per the original finding). This is the strongest possible confirmation that H1 is fully resolved.

All real-build artifacts (`discovery-v1-real-smoke-rework.db`, `discovery-review-candidates-rework.csv`, reused `crosswalk.json`/`discovery-review-approved-smoke.csv`) stay in gitignored `discovery_data/` — confirmed via `git check-ignore -v` — nothing was committed.

**H2 negative case, also confirmed against real data:** calling `finalize_build(..., release=True, frozen_precision_defaults=True)` directly with every REAL collection path except `q2_shared_text_path` (deliberately set to `None`, simulating a missing input) raised `ReleaseInputsIncompleteError` immediately with `"q2_shared_text: expected 60156, got 0"` — the abort happened BEFORE any output `.db` file was created (`out_db_path` never materialized) and well before the expensive `build_claims_and_evidence` ingest step, confirming H2 refuses a partial-source release build against real data, fast and fail-closed.

---

## Gates

- `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_service.py tests/test_no_back_edges_discovery.py tests/test_discovery_composition.py tests/test_discovery_build.py tests/test_discovery_loader.py tests/test_discovery_flag.py tests/test_discovery_ids.py tests/test_masking_sqlite.py tests/test_discovery_schema.py tests/test_discovery_bands.py tests/test_discovery_frame.py tests/test_discovery_units.py tests/test_discovery_release_contract.py -q` → **213 passed** (up from the pre-rework count; every new regression test included).
- `python scripts/verify_discovery_sidecar.py tests/fixtures/discovery/discovery-v1-fixture.db` → **clean** (pinned fixture untouched).
- `python -m ruff check .` → **all checks passed**.
- `MASKING_SCAN_PATTERNS_FILE=.masking_patterns python scripts/check_atlas_masking.py --scan-repo` → **clean, exit 0**.

## Deviations from the task instructions

None material. Where the task's paraphrased frozen counts could differ from the
schema doc, the schema doc's own §4 source-of-truth table was read directly and used
verbatim for every `_EXPECTED_*` constant (H2) and every measured band-precision value
(H3/M4) — all subsequently confirmed against the real research corpus (see above).

---

*Phase: 134-Discovery Data Spine — Codex CODE-review Round 1 rework*
*Completed: 2026-07-22*
