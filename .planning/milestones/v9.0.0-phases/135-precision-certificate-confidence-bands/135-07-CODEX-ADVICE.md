# 135-07 pre-bake Codex advice (gpt-5.6-sol, 2026-07-24) — v2 band-vocabulary rename hardening

Grounded external review sought (owner-directed) before running the v2 production bake, after pre-flight found a cascade of gaps in the deferred `expert_verified -> high_confidence_algorithmic` rename. Codex ran read-only over the tracked scripts + specs (masking-clean; no restricted data read). Full log is gitignored scratch.

**VERDICT: PROCEED-WITH-CHANGES.** The semantic fix is correct — complete the rename v2-conditionally, keep `band_precision` inside `check_no_mixed_enum_state`, keep the 0.889/0.859/0.647 values (rename the KEY only). But land it as ONE coherent, version-threaded, end-to-end amendment (not more piecemeal patches). Do NOT re-plan Phase 135. Keep the production bake blocked until the amendment passes. Root cause of the 3-gap cascade: the rename never had an end-to-end vertical-slice test.

## Required changes (beyond the naive "flip band_precision + verifier expectations")

1. **Explicit version, threaded.** Derive one `band_vocab_version` ("v1"/"v2") ONCE at the START of `finalize_build`, thread it through every writer + validator. `canonical_merges present => v2` is tolerable for THIS bake only, but is not durable (a future v2 rebuild without the census silently becomes v1; a v1 op with merges silently becomes v2). **Ordering bug:** `band_precision` is resolved before the v2 signal is computed (~line 3714) — derive the version FIRST. Add a **release preflight**: v2 requires all three input paths AND their pins; v1 rejects v2-only inputs; a SHA without its path (or vice versa) is a hard error.

2. **Rename at EVERY site** (audit list): all `_frozen_real_band_precision_rows` callers (normal defaults, non-release defaults, and the CERT-01 reband branch — it grabs frozen rows BEFORE invalidating tier_a/screening_rb, so it keeps the old top-tier key unless version-threaded); `_validate_precision_spec` + `_resolve_band_precision_spec` (a v2 external spec must require the v2 key and reject v1, inverse for v1); the measured-fail path (reject duplicate fail rows, conflicting outcomes, unknown rows, any legacy top-tier key — without echoing supplied values); `VALID_EVIDENCE_COMBOS` (dual-key is fine for runtime read-compat but is NOT a substitute for asset-version purity); the DDL allowlist, evidence ranking SQL, display selection, labels, and precision-lookup consumers named by the lockstep spec. `confidence_band` participates in `evidence_id`, so v2 ids + `display_evidence_id` must be RECOMPUTED, not updated in place.

3. **Verifier: don't trust the asset's self-report alone.** Keep `check_no_mixed_enum_state`. Add version validation: marker present => require v2 key, forbid v1 key, require the exact v2 `band_precision` keyset; marker absent => inverse; reject both-keys and reject neither-top-tier-key. Validate the marker as a REAL SHA (not mere key presence). Compute version-specific expected sets from ONE helper (no drifting globals). **False-green to close:** an intended-v2 bake that accidentally produces a valid v1 asset (no marker) is indistinguishable from an intentional v1 build by meta inspection alone — so add an EXTERNAL verifier arg `--require-v2` / `--expected-band-vocabulary v2` (ideally with expected input hashes). The asset must prove it matches OPERATOR INTENT; it must not choose its own contract. Defense-in-depth: scan every SQLite TEXT column for the old literal, not just the two band columns (an external precision spec can persist free-form fields).

4. **Hash caveat.** `frame_content_hash` includes evidence bands + ids (so the evidence rename changes it) but NOT `band_precision` — a correct frame hash cannot prove `band_precision` was renamed. File `content_hash` changes.

5. **Preserve v1 fixtures** deliberately using the v1 key — byte identity is the guard against accidental v1-output migration.

## Systemic guard (before the real bake)
One hermetic release-contract MUTATION suite with masking-safe synthetic inputs + the EXACT production flags: a valid v2 build verifies rc=0, then each INDEPENDENT mutation returns NONZERO — (a) remove the v2 marker, (b) change only the evidence key, (c) change only the precision key, (d) produce a pure-v1 asset while invoking `--require-v2`. Tests the gate against false-NEGATIVES, not merely that builder + verifier agree on the same mistake.

## Production 135-07 invocation change
Run the verifier with `--require-v2` (+ expected input hashes) so the bake proves v2 intent, not just internal consistency.
