# Phase 134 — Codex CODE-review Round 3 (fixes applied inline by orchestrator)

Codex R3 verdict: **REWORK**, but strongly converged — it confirmed all 5 R2 fixes
genuinely resolved (identical `(band_rank, sys_id, page_id, claim_id)` SQL+Python
tie-break ordering; H2/H3 run before any output mutation; strict release verifier
rejects duplicates/extras; crosswalk messages expose only counts/positions; golden
fixture byte-identical at SHA-256 `e71de1f1…`; recipe/loader/async/web untouched).
Two narrow exactness defects remained; both fixed here.

## Findings + dispositions

### [HIGH] `scripts/build_discovery_sidecar.py::_validate_precision_spec` — key-set not exact
The per-row value checks keyed collection rows by `collection_id` and band rows by
`(evidence_source, confidence_band)` ONLY. So (a) an EXTRA `scope='collection'` row
with a different `collection_id` was ignored, and (b) a band with a valid
`(evidence_source, confidence_band)` pair but the WRONG `collection_id` matched the
frozen key and passed — either could finalize an invalid DB/manifest before the
separate verifier ran.
**Fix:** prepend an exact `(scope, collection_id, evidence_source, confidence_band)`
key-MULTISET comparison (via `collections.Counter`) against
`_frozen_real_band_precision_rows()`, plus non-dict-row / unknown-scope rejection,
BEFORE any value validation. Mismatch keys are contract enum/collection-id values
(never restricted content), so naming them in the message is masking-safe.
**Tests:** `test_resolve_band_precision_spec_rejects_extra_collection_row`,
`test_resolve_band_precision_spec_rejects_band_on_wrong_collection_id`
(all prior precision-spec tests remain green — the old per-key messages still fire).

### [MED] `scripts/build_discovery_sidecar.py::_validate_crosswalk` — `$` terminal-newline hole
`_OPAQUE_WORK_ID_PATTERN = re.compile(r"^w[0-9]{6}$")` used with `.match()` accepted
`"w000001\n"` — Python's `$` matches just before a terminal newline — which
`format(int, "06d")` (the frozen `mint_work_id` recipe) can never emit, so a
non-frozen opaque id could reach the crosswalk/review artifact.
**Fix:** pattern → `re.compile(r"w[0-9]{6}")` applied via `.fullmatch()` (whole-string;
rejects any trailing newline). `discovery_ids.py` recipe untouched.
**Test:** `test_validate_crosswalk_rejects_terminal_newline_opaque_value`
(asserts the OLD `$`-anchored `.match` would have accepted it, the fix rejects it,
and the raw value never appears in the raised message).

## Gates
- 231 discovery tests pass (was 228; +3 regression tests).
- `verify_discovery_sidecar.py` clean on the pinned golden fixture (byte-identical,
  SHA-256 `e71de1f179eae22ea901901c5a5837c274f3716140c9fdb189c5279ffd7077d1`).
- `ruff check` clean; `check_atlas_masking.py --scan-asset` clean on both changed files.

Applied inline (not via a subagent) — two surgical edits in one file + one test file.
Next: Codex CODE review Round 4.
