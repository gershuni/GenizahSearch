# Phase 134 — Codex CODE-review Round 5 (final holistic pass; fixes applied inline)

Codex R5 verdict: **REWORK**, two MED — both surfaced by the deep holistic pass (not
regressions of prior fixes). All prior findings confirmed resolved; fixture
byte-identical at SHA-256 `e71de1f1…`; Codex independently verified the router
ground-truth matches 106·18 / 108·57 (so the defect below is the absent *assertion*,
not the data).

## Findings + dispositions

### [MED, masking] `_validate_precision_spec` value-check messages echoed the supplied precision
The R4 fix protected the structural KEY-field messages, but the collection- and
band-precision *value* mismatch messages still rendered the SUPPLIED `precision`
(`{c.get('precision')!r}` / `{actual_precision!r}`). If a spec supplies a *string*
precision, restricted text could leak into a CLI error/log.
**Fix:** both value messages now name only the frozen-safe key/scope + the frozen
EXPECTED value ("precision mismatch (expected frozen …)") — never the supplied value.
Also removed the now-dead-but-leaky `extra_keys` band diagnostic (the structural
key-multiset check fails fast on any extra/wrong-collection band, so it was
unreachable-with-content and rendered supplied-derived keys).
**Test:** `test_validate_precision_spec_value_message_never_echoes_supplied_precision`
(collection + band precision each set to a sentinel string; sentinel never appears).

### [MED] `_assert_release_inputs_complete` pinned router TOTALS but not the frozen shape
The H2 gate asserted the router totals (106, 108) but not the frozen two-seed
(trials>=2) subset counts (18, 57) or per-row bucket identity — a shape-drifted input
with the same total would pass, contrary to the frozen `106·18 / 108·57` contract.
**Fix:** new `_EXPECTED_TAFSIR_TARGUM_TWO_SEED=18` / `_EXPECTED_WITH_ARABIC_TWO_SEED=57`
constants; the gate now asserts each router collection's two-seed subset count AND
that every row's `_bucket` matches the expected bucket identity, before any output
mutation. Messages name only the param, the frozen-safe expected bucket, and counts.
**Tests:** `test_assert_release_inputs_complete_router_two_seed_subset_mismatch_raises`,
`..._router_wrong_bucket_identity_raises`; the `_h2_complete_kwargs` helper now builds
conforming router rows (via a new `_router_rows` helper) with the frozen shape.

## Gates
- 235 discovery tests pass (was 232; +3 regression tests).
- `verify_discovery_sidecar.py` clean on the pinned golden fixture (byte-identical,
  SHA-256 `e71de1f179eae22ea901901c5a5837c274f3716140c9fdb189c5279ffd7077d1`).
- `ruff check` clean; `check_atlas_masking.py --scan-asset` clean on both changed files.

Applied inline (one source file + one test file). Next: Codex CODE review Round 6.
