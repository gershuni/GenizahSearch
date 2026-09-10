# Phase 134 — Codex CODE-review Round 4 (fix applied inline by orchestrator)

Codex R4 verdict: **REWORK**, one MED — a masking regression introduced by the R3
HIGH fix itself. Both R3 findings were confirmed fully resolved (multiset rejects
extra collection rows / wrong collection_id / duplicates / non-dict / unknown-scope;
`fullmatch` rejects terminal newlines; fixture byte-identical at SHA-256 `e71de1f1…`;
recipe/loader/service/async/router untouched).

## Finding + disposition

### [MED, masking] `scripts/build_discovery_sidecar.py::_validate_precision_spec`
The R3 multiset diagnostics interpolated the *supplied* `scope` and the full
unexpected-key tuples into `InvalidPrecisionSpecError`. The frozen values are safe,
but a malformed/hand-authored `--precision-spec` could embed restricted text in a key
field (e.g. `collection_id`), which would then render into a CLI error/log —
contradicting the masking guarantee (the same discipline `_validate_crosswalk`
already applies). Codex also asked for fail-fast (raise the structural check before
the value block).
**Fix:** the structural check now (a) reports unexpected/duplicate rows by POSITION
only — never rendering a supplied value; (b) names only MISSING keys, which come from
the FROZEN row-set (never the supplied spec) and are therefore masking-safe; (c)
drops the supplied `scope` from the unknown-scope message (position only); and (d)
raises IMMEDIATELY, before any value check runs — so the downstream value-check
messages only ever render frozen-safe keys.
**Test:** `test_validate_precision_spec_message_never_echoes_supplied_key_values`
(an extra band row carrying a sentinel in every key field is rejected, and the
sentinel never appears in the raised message).

## Gates
- 232 discovery tests pass (was 231; +1 masking regression test).
- `verify_discovery_sidecar.py` clean on the pinned golden fixture (byte-identical,
  SHA-256 `e71de1f179eae22ea901901c5a5837c274f3716140c9fdb189c5279ffd7077d1`).
- `ruff check` clean; `check_atlas_masking.py --scan-asset` clean on both changed files.

Applied inline (one file + one test). Next: Codex CODE review Round 5.
