# Phase 134 Discovery Data Spine — Codex CODE-review Round 2 Rework

**Status:** REWORK COMPLETE. All 5 residual findings fixed (VERDICT was REWORK, converging --
all Round 1 fixes confirmed resolved; these are refinements, not new blockers), each with a
regression test and atomic commit. No fabricated/invented severities or dispositions.

**Executor:** Claude (Sonnet 5), sequential execution on `master-main` (no worktree).

**Context obeyed:** `docs/specs/discovery-sidecar-schema-v1.md` SS1.6 (band_precision frozen
row-set, display-precedence lattice), `scripts/discovery_ids.py` (FROZEN `mint_work_id` recipe
— confirmed the exact output alphabet is `w` + 6 ASCII digits via `format(int, "06d")`, so the
validation regex is `^w[0-9]{6}$`, matching the task's CONFIRM instruction).

**Frozen artifacts untouched:** `scripts/discovery_ids.py` (byte-identical, never edited), the
evidence_id/claim_id recipe + enum-vocab sections of the schema doc (byte-identical), and
`tests/fixtures/discovery/discovery-v1-fixture.db` + its manifest/expected (the pinned 134-03
golden fixture) — confirmed byte-identical: SHA-256
`e71de1f179eae22ea901901c5a5837c274f3716140c9fdb189c5279ffd7077d1` unchanged,
`verify_discovery_sidecar.py` against it stays clean.

---

## Findings and Dispositions

### HIGH — `_resolve_band_precision_spec` never validated an explicit `--precision-spec`

**Disposition: FIXED.** Commit `610135b0`.

An explicit `--precision-spec` was returned verbatim and inserted into a real/release DB
without any validation — a spec with a fabricated `tier_a` precision, a missing frozen row, or
an extra/duplicate measured band could have reached a finalized release `.db` + manifest
BEFORE the separate `verify_discovery_sidecar.py` process (run only after `finalize_build`
exits) ever got a chance to catch it.

Added `InvalidPrecisionSpecError` + `_validate_precision_spec`, which cross-checks an explicit
spec against `_frozen_real_band_precision_rows()` (the ONE source of truth for the frozen
row-set already defined in `scripts/build_discovery_sidecar.py` — no second hardcoded copy of
the contract that could drift). Requires EXACTLY: the collection row
(`propagated_witness_collection_v1`, precision ≈ 0.926, non-null numerator/denominator/
ci_low/ci_high/method); both propagated bands (`corroborated`, `weak`) with NULL precision; the
three measured `track1_direct` bands (`expert_verified`=0.889, `screening_rb`=0.859,
`screening_canon`=0.647) at their frozen values; `tier_a` NULL. Any missing/duplicate/extra row
or an out-of-tolerance value raises `InvalidPrecisionSpecError`, wired into
`_resolve_band_precision_spec` BEFORE the spec is returned/used.

**New regression tests** (`tests/test_discovery_build.py`): a spec with a fabricated `tier_a`
precision is rejected; a spec missing `screening_canon` is rejected; a spec with an extra
unexpected band row is rejected; the EXACT minimal fabricated spec cited in the finding
(`[{"scope": "collection", "collection_id": "x"}]`) is rejected; the unmodified frozen rows
pass (positive case). The pre-existing "explicit spec wins" test was updated to use a
legitimately-shaped owner-override spec instead of the old unvalidated placeholder (which is
now, correctly, one of the rejection cases).

---

### MED — `get_work_witnesses`' `ROW_NUMBER()` `ORDER BY` was not a total order

**Disposition: FIXED.** Commit `bba9f51d`.

`ROW_NUMBER() OVER (PARTITION BY unit_key ORDER BY band_rank ASC, sys_id ASC)` did not
discriminate between two claims sharing the same unit/sys_id at the same band (2,829 tied units
observed in the cited real-corpus large work `w000112`) — the chosen representative depended on
unspecified SQLite scan/insertion order.

Appended `page_id ASC, claim_id ASC` as stable secondary tie-breakers to BOTH the window
PARTITION's `ORDER BY` and the outer pagination `ORDER BY` in
`_WORK_WITNESSES_RANKED_CTE_SQL`'s consumer, and mirrored the IDENTICAL tie-break key order (not
just the same fields, the same priority) in the pure, DB-free `_project_work_witnesses`
Python reference implementation's `best_row` selection (and its outer `items.sort`), so SQL and
Python can never disagree on which row wins a tie.

**New regression tests** (`tests/test_discovery_service.py`): a synthetic fixture with two
witness claims sharing one sys_id (unmerged singleton unit) and the same band, differing only
by `page_id`/`claim_id`, returns the SAME representative `page_id` regardless of INSERT order
(two databases built with reversed row order) and across repeated calls; a pure-Python mirror
test uses `claim_id` values that would pick the OPPOSITE winner if `claim_id` were compared
before `page_id`, proving the two implementations use the SAME tie-break priority.

**Real-corpus validation:** work `w000112`'s 18,943 raw witness claims / 5,684 distinct units
(matching the original H1 finding's cited numbers exactly) now return **zero** unstable
representatives across repeated `get_work_witnesses` calls against the real distilled sidecar.

---

### MED — `finalize_build`'s H2/H3 gates ran AFTER output/crosswalk mutation

**Disposition: FIXED.** Commit `762915fc`.

The H2 (`_assert_release_inputs_complete`) and H3 (`_resolve_band_precision_spec`) validation
previously ran AFTER the existing output `.db` was deleted, the crosswalk was persisted (inside
`assign_opaque_work_ids`), and the review artifact may have been written — so the "no output
file created before the gate" guarantee was false: a failed release build had already mutated
all three prior artifacts.

Reordered `finalize_build` so H3 (a pure argument-validation gate with zero file I/O) runs
FIRST, before even opening the read-only research connection; then the E1/Q2 collections are
loaded and the H2 completeness gate runs; only after BOTH gates pass does the function delete
any prior output `.db`, create the output directory, mint/persist opaque work_ids (crosswalk
write), and emit the review artifact.

**New regression tests** (`tests/test_discovery_build.py`): a release=True call that fails H2
(no collections supplied) leaves the prior output `.db`, crosswalk file, and review artifact
byte-identical to their pre-call state; the same for a call that fails H3 (no precision-spec
choice supplied, regardless of collection completeness).

**Real-corpus validation:** a forced-partial `finalize_build(release=True, ...)` call against
the real research corpus with `q2_shared_text_path` deliberately omitted raised
`ReleaseInputsIncompleteError` immediately (`"q2_shared_text: expected 60156, got 0"`) with the
prior real output `.db`, crosswalk, and review-artifact files all confirmed byte-identical
(SHA-256 compared) before and after the failed call.

---

### MED — `_check_band_precision_release_strict` dict-collapse over `(source, band)`

**Disposition: FIXED.** Commit `b2b279df`.

Band rows were collapsed into a dict keyed on `(evidence_source, confidence_band)` via plain
assignment — an extra/duplicate measured row for an already-satisfied key could silently
overwrite a valid one (last-value-wins) and slip through undetected, contrary to the
complete-row-set requirement. The old "reject non-null precision on an unexpected band" check
also missed an entirely bogus/extra band row whose own precision happened to be NULL.

Rewrote `_check_band_precision_release_strict` to group band rows by the FULL
`(collection_id, evidence_source, confidence_band)` key into a LIST of matches per key, require
EXACTLY ONE row for every one of the 6 frozen expected keys (0 = missing, >1 = duplicate)
BEFORE any value is inspected, and separately reject any row keyed entirely outside the frozen
expected set regardless of its own precision value.

**New regression tests** (`tests/test_discovery_release_contract.py`): a duplicate row for an
already-satisfied expected band (the wrong/extra duplicate inserted BEFORE the valid row, so a
naive last-value-wins dict would have missed it) is rejected; an entirely extra/unexpected band
row with NULL precision is rejected too. All pre-existing M4 tests (frozen-rows pass,
fabricated tier_a, missing measured band, synthetic-fixture gating, committed golden fixture)
remain green.

---

### MED (masking) — `_validate_crosswalk` echoed raw values + `\d` matched non-ASCII digits

**Disposition: FIXED.** Commit `5c2c88a8`.

Two issues in `scripts/build_discovery_sidecar.py::_validate_crosswalk`:

1. Both exception-message branches interpolated RAW crosswalk data directly into the message —
   `malformed[:5]` (a list of raw_id KEYS) and `f"{opaque} <- {prior_raw!r} AND {raw_id!r}"` (raw
   VALUES and KEYS) — a masking-hard-constraint leak, since a malformed opaque VALUE could
   itself be, or embed, a restricted raw M-source identifier, surfacing via a CLI invocation or
   an uncaught traceback.
2. The regex used bare `\d`, which (under Python's default UNICODE flag for str patterns)
   matches ANY Unicode decimal-digit codepoint (Nd category) — e.g. fullwidth digits
   U+FF10–FF19 or Arabic-Indic digits — not just ASCII `0`-`9`. Confirmed against
   `scripts/discovery_ids.py::mint_work_id` (`f"w{int(counter):06d}"` — Python's `06d` format
   spec always emits ASCII `0`-`9` only, verified empirically) that the frozen output alphabet
   is `w` + 6 ASCII digits exactly, so the CONFIRM instruction's suggested `^w[0-9]{6}$` is
   correct and was applied verbatim.

Rewrote both error paths (malformed-format and non-1:1-duplicate) to report ONLY counts and
positional indices (stable dict-iteration-order position within the crosswalk) — never a raw
key or value — and tightened `_OPAQUE_WORK_ID_PATTERN` to `^w[0-9]{6}$`.

**New regression tests** (`tests/test_discovery_build.py`): a malformed value carrying an
embedded raw-shaped secret identifier (`"M:SECRET-RAW-RESEARCH-IDENTIFIER-XYZ"`) raises WITHOUT
that value (or the `raw_id` key) appearing anywhere in the exception message; the duplicate-value
branch gets the same guarantee; a fullwidth-digit opaque value (which the OLD `\d` pattern would
have wrongly accepted — confirmed via a direct regex probe before writing the fix) is rejected
by the fixed `[0-9]`-only pattern; every genuine `mint_work_id(...)` output for a range of
counters still matches.

---

## Real-corpus smoke validation

The gitignored research corpus (`same_work_spike/probe/data/fullcorpus_v2.db` plus the Q2/E1
collections, `libraries.csv`, `fist_data/fjms_enrichment.db`) IS present on this machine, so a
real `--release --frozen-precision-defaults` build was re-run against it with the R2 fixes
applied (not a CI gate; no artifacts committed; everything stayed in gitignored
`discovery_data/`):

```
python scripts/build_discovery_sidecar.py same_work_spike/probe/data/fullcorpus_v2.db \
  --from-approved discovery_data/discovery-review-approved-smoke.csv \
  --crosswalk discovery_data/crosswalk.json \
  --research-data-dir same_work_spike/probe/data \
  --libraries-csv libraries.csv \
  --fjms-db fist_data/fjms_enrichment.db \
  --out discovery_data/discovery-v1-real-smoke-r2rework.db \
  --review-artifact discovery_data/discovery-review-candidates-r2rework.csv \
  --release --frozen-precision-defaults
```

**Result:** `real build OK: {'works': 625, 'discovery_claim': 231604, 'discovery_evidence':
251976, 'witness_units': 5547}` — identical row counts and `frame_content_hash`
(`738c33980a01084048da47f3f91ae999cf8cc4e3be4f3bcd2064fc1c82e9cf9c`) to the prior 134-04/R1
smoke builds (as expected, same corpus/inputs), `evidence_id_collisions=115`.

- `python scripts/verify_discovery_sidecar.py discovery_data/discovery-v1-real-smoke-r2rework.db --expected-frame-hash 738c33980a...` → **clean, all invariants pass**.
- `MASKING_SCAN_PATTERNS_FILE=.masking_patterns python scripts/check_atlas_masking.py --scan-sqlite discovery_data/discovery-v1-real-smoke-r2rework.db` → **clean, no matches**.
- **Gate-ordering fix confirmed against real data:** a `finalize_build(release=True, frozen_precision_defaults=True, ...)` call with `q2_shared_text_path` deliberately set to `None` (simulating a missing input) raised `ReleaseInputsIncompleteError` immediately — the prior real output `.db`, crosswalk, and review-artifact files were confirmed BYTE-IDENTICAL (SHA-256 compared) before and after the failed call.
- **Tie-break fix confirmed against real data:** work `w000112` (18,943 raw witness claims / 5,684 distinct units, matching the original H1 finding's cited numbers exactly) returned **zero** unstable representatives across repeated `get_work_witnesses` calls against the real distilled sidecar.

All real-build artifacts stay in gitignored `discovery_data/` (confirmed via `git check-ignore -v`) — nothing was committed.

---

## Gates

- `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_service.py tests/test_discovery_build.py tests/test_discovery_ids.py tests/test_masking_sqlite.py tests/test_discovery_schema.py tests/test_discovery_bands.py tests/test_discovery_frame.py tests/test_discovery_units.py tests/test_discovery_release_contract.py tests/test_discovery_loader.py tests/test_discovery_flag.py tests/test_no_back_edges_discovery.py tests/test_discovery_composition.py -q` → **228 passed** (up from the pre-R2-rework 213/-ish count; every new regression test included).
- `python scripts/verify_discovery_sidecar.py tests/fixtures/discovery/discovery-v1-fixture.db` → **clean** (pinned fixture byte-identical, SHA-256 confirmed).
- `python -m ruff check .` → **all checks passed**.
- `MASKING_SCAN_PATTERNS_FILE=.masking_patterns python scripts/check_atlas_masking.py --scan-repo` → **clean, exit 0, no matches**.

## Deviations from the task instructions

None material. The task's suggested validation regex (`^w[0-9]{6}$`) was CONFIRMED (not just
assumed) against `scripts/discovery_ids.py::mint_work_id`'s actual implementation
(`f"w{int(counter):06d}"`) and against Python's own `format()` semantics (the `d` presentation
type always emits ASCII `0`-`9`, verified empirically) before being applied verbatim.

---

*Phase: 134-Discovery Data Spine — Codex CODE-review Round 2 rework*
*Completed: 2026-07-22*
