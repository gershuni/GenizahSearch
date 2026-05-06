---
phase: 84
round: 2
reviewers: [gemini, codex]
reviewed_at: 2026-05-06T11:06:41Z
plans_reviewed:
  - 84-01-PLAN.md
  - 84-02-PLAN.md
  - 84-03-PLAN.md
  - 84-04-PLAN.md
  - 84-05-PLAN.md
self_skipped: claude (running inside Claude Code)
unavailable: [coderabbit]
---

# Cross-AI Plan Review — Phase 84: CUDL Shelfmark Normalization (Round 2, post-revision)

This is the second review round, evaluating plans **after** they were revised to incorporate Round 1 feedback (3 HIGH, 4 MEDIUM, 5 LOW items). The previous Round 1 REVIEWS.md content was overwritten by this revision pass — the consolidated record of Round 1 lives in git history (commit prior to `2a8dff2a`).

## Gemini Review

# Cross-AI Plan Review — Phase 84: CUDL Shelfmark Normalization (Round 2)

## Summary
The revised implementation plans for Phase 84 are exceptional. They comprehensively address all 12 items (3 HIGH, 4 MEDIUM, 5 LOW) from the prior review round while maintaining the core "layered fallback" architecture. The transition from a strict orphan-set subset assertion to a "previously-resolved labels still resolve" invariant (Codex HIGH #1) is a significant improvement in test rigor. The inclusion of a strict ambiguity-exclusion policy with diagnostic reporting (Codex HIGH #2) and a conservative forward-allowlist (Codex HIGH #3) effectively mitigates the risk of silent mis-routing. The plans are technically mature, safe, and ready for execution.

## Strengths
*   **Rigorous Data Integrity:** The "Audit-first / Fail-loud" strategy for leading-zero collisions (Plan 01) combined with the new strict ambiguity-exclusion policy (Plan 02) ensures that no CUDL classmark is ever mapped to the wrong manuscript row.
*   **Defensive Forward Mapping:** The conservative pattern-aware allowlist in `shelfmark_to_cudl_label` (Plan 03) prevents the application from generating speculative or broken CUDL URLs for uncertain manuscript forms.
*   **High-Fidelity Validation:** Plan 05's four-layer guard (validated golden fixture, baseline-resolution invariant, canonical source-hash, and literal output checks) provides a world-class regression safety net for a project of this scale.
*   **Decoupled Unit Testing:** The addition of deterministic unit tests that do not depend on `MetadataManager` or `libraries.csv` (Plan 02/05) ensures the normalization logic can be verified in isolation across different environments.
*   **Unconditional Integration:** The requirement to migrate all runtime NLI manifest call paths (Plan 04) ensures the bridge is actually exercised where it provides value, fulfilling the design intent of D-08.

## Concerns
*   **Late-Import Warning Implementation (LOW):** Plan 04 Task 2 uses `getattr(shelfmark_to_cudl_label, '_warned', False)` to track warnings. While functionally correct, attaching state to a function object is less idiomatic than a module-level flag (as used in `genizah_core.py`).
*   **Startup Latency (LOW):** Building the alias index (Plan 02) involves walking ~140K rows. Although O(1) lookups are efficient, the initial build time should be monitored. (Note: This was a Gemini LOW in the prior round and remains a minor observation).
*   **Baseline Sample Size (LOW):** Plan 05 Task 3 samples only 500 rows for the baseline-resolution test. While reasonable for CI speed, a one-time full-baseline check during local execution would be even safer.

## Suggestions
*   **Idiomatic Logging:** In `web/pages/browse.py`, prefer a module-level `_BRIDGE_IMPORT_WARNED` flag over function-attribute storage for consistency with `genizah_core.py`.
*   **Pre-populate Safety Net:** In Plan 01, if any leading-zero collisions are already known from the Phase 84 investigation, hardcode them into `_BUILTIN_COLLISION_KEYS` immediately to provide immediate safety before the first audit run.
*   **Full Baseline Run:** During the final verification of Plan 05, consider running the `TestScanDiffBaselineStillResolves` without the `[:500]` slice at least once locally to confirm 100% parity across the ~141K manifests.

## Risk Assessment: LOW
The risk is low. The architecture is strictly additive (fallback-only), the most sensitive rules are protected by mandatory audits and exclusion lists, and the validation suite is exhaustive. The revisions have successfully closed the "silent mis-routing" and "test identity" gaps identified in Round 1.

**Approval:** Approved for execution.

---

## Codex Review

## Summary

The revised plans close several prior concerns in intent, especially alias ambiguity handling and delta-only leading-zero auditing, but they still have execution-level gaps. The highest-risk residual issues are that Mosseri slug generation is wrong in the provided `_index_key_for_label()` sample, the NLI runtime migration misses the actual `genizah_core.py` call path, and the new scan-diff baseline test does not actually model "previously resolved still resolves." I would not execute these plans unchanged.

## Strengths

- The alias-index ambiguity policy is materially improved: collect all claims, exclude multi-`sys_id` keys, and report `reports/cudl_alias_collisions.csv`.
- Leading-zero collision auditing is now properly scoped to the zero-collapse delta, with a separate full-normalization transparency report.
- Numeric-collapse is now gated to Or.-like keys instead of all CUL variants.
- The forward CUDL-link function is now conservative rather than applying `cudl_normalize()` to arbitrary shelfmarks.
- Deterministic in-memory unit tests were added alongside the heavier `MetadataManager()` integration tests.
- Canonical normalizer protection is stronger with source hash plus literal-output snapshot tests.

## Concerns

- **HIGH: Mosseri slug generation sample is wrong.**
  Plan 02 says `_index_key_for_label()` strips `MS-`, but the provided code does not. `MS-MOSSERI-III-00027-O` would normalize to `msmosseriiii27o`, not `mosseriiii27o`. This breaks NORM-01 and also Plan 03's `shelfmark_to_cudl_label('Moss. III,27O')`.

- **HIGH: Or. numeric forward links are still wrong.**
  `shelfmark_to_cudl_label('Or. 1080.1.1')` would return `or1080.1.1`, but the plan says CUDL uses `or1080.11`. Lookup handles collapse, but browse URL generation does not.

- **HIGH: NLI runtime migration misses the actual call path.**
  The existing manifest lookup happens in `genizah_core.py` around the external-IIIF logic via `crossref_svc.get_cambridge_manifest(...)` and `get_cambridge_manifest_by_label(...)`. Plan 04's grep/migration scope omits `genizah_core.py`, so the wrapper can be added but the real runtime path remains unchanged.

- **HIGH: The browse.py import-failure snippet can raise `NameError`.**
  In the `except ImportError` block, it references `shelfmark_to_cudl_label` even though the import failed. It also never sets a warned flag. Use a module-level `_BRIDGE_IMPORT_WARNED` helper instead.

- **HIGH: Plan 05's scan-diff baseline does not prove the revised invariant.**
  `build_cudl_baseline_resolved.py` dumps every `cambridge_manifests` row with a URL, not the labels that resolved pre-phase through the runtime or orphan scanner. The later test only samples 500 rows and passes `label` into a shelfmark-oriented wrapper. This does not close the prior HIGH scan-diff concern.

- **HIGH: Fixture generation reads the wrong orphan-report column.**
  Existing `reports/cudl_orphans_all.csv` columns are `cudl_label,manifest_url,normalized_shelfmark`. Plan 05 Source B uses `row[0]` as the classmark, but the classmark is `row[2]`.

- **MEDIUM: `get_cambridge_manifest_with_bridge()` has a bad label fallback.**
  It calls `get_cambridge_manifest_by_label(slug.upper())`; for Mosseri, the label is `MS-MOSSERI-III-00027-O`, not `MOSSERIIII27O`. The wrapper should try the original label if given, and for Mosseri should call `construct_mosseri_cudl_label()`.

- **MEDIUM: Plan 01 contradicts itself on `_BUILTIN_COLLISION_KEYS`.**
  `must_haves` says it is non-empty, while the interface says start with `set()` if none known. Either require known audited keys or drop "non-empty."

- **MEDIUM: Golden fixture may miss the critical Or numeric collapsed input.**
  The generator mostly emits `cudl_normalize(variant)`, so it may test `or1080.1.1` but not `or1080.11`.

- **MEDIUM: Plan 05 uses `git checkout` to restore reports.**
  That is destructive and can discard unrelated user edits. Add explicit `--output`/`--suffix` support to `scan_cudl_orphans.py` instead.

- **LOW: Writing reports on every alias-index build may be noisy or fail in packaged/read-only runs.**
  The failure is caught, but normal app startup should probably not emit report files unless in dev/audit mode.

## Suggestions

1. Fix `_index_key_for_label()` to explicitly drop the leading `MS` segment before normalization.
2. Apply `_collapse_numeric_runs()` inside `shelfmark_to_cudl_label()` for Or. shelfmarks.
3. Replace the two existing `genizah_core.py` Cambridge lookup branches with one call to `get_cambridge_manifest_with_bridge(shelfmark)` or explicitly migrate both existing calls.
4. Rewrite the browse.py logging as a module-level helper; do not reference a failed import symbol.
5. Redesign the baseline test: capture pre-phase matched `normalized_shelfmark` values using the old scanner logic, then assert those same classmarks still match after bridge wiring.
6. In fixture generation, use `csv.DictReader` and `row["normalized_shelfmark"]` for orphan classmarks.
7. Add explicit fixture rows for `or1080.11`, `MS-MOSSERI-III-00027-O`, and `mosseriiii27o`.
8. Add `--out-all` / `--out-neighbor` or `--suffix` to `scan_cudl_orphans.py`; do not use `git checkout` in the plan.

## Risk Assessment

**MEDIUM-HIGH.** The architecture is still sound, and several prior review items are genuinely improved. But the revised plans contain concrete implementation snippets that would fail key acceptance paths or leave the real runtime path unchanged. The main risk is not broad regression in canonical normalization; it is false confidence from tests and wrappers that do not exercise the actual CUDL lookup and browse-image paths.

---

## Consensus Summary

Reviewers diverge sharply: **Gemini approves for execution at LOW risk; Codex blocks at MEDIUM-HIGH** with 6 HIGH-severity execution-level findings. Both agree the architectural decisions in the revisions (ambiguity policy, delta-only audit, conservative forward allowlist, deterministic unit fixtures, source-hash guard) are correct in design. They disagree on whether the *concrete code snippets and fixture/baseline mechanics* in the revised plans will actually deliver those designs.

### Agreed Strengths
- Alias-index ambiguity policy (collect-all + exclude-multi + collision report).
- Leading-zero audit isolated to the zero-collapse delta.
- Conservative forward allowlist in `shelfmark_to_cudl_label`.
- Or.-only numeric-collapse gating.
- Deterministic unit tests alongside MetadataManager-integration tests.
- Source-hash + literal-output guard for `normalize_shelfmark`.

### Agreed Concerns
- **Late-import logging hygiene** in `web/pages/browse.py` should use a module-level `_BRIDGE_IMPORT_WARNED` flag, matching the genizah_core.py pattern. Gemini flags as LOW; Codex calls out the same site under HIGH (#4) because the snippet can raise `NameError` after a failed import.

### Codex-only HIGH-severity items (must address before execution)
1. **`_index_key_for_label()` Mosseri stripping is wrong.** Sample code does not strip the leading `MS-`, so `MS-MOSSERI-III-00027-O` → `msmosseriiii27o` instead of `mosseriiii27o`. Fix Plan 02's helper to drop the leading `MS` segment explicitly.
2. **Or. numeric forward URL is still wrong.** `shelfmark_to_cudl_label('Or. 1080.1.1')` would emit `or1080.1.1` not `or1080.11`. Apply `_collapse_numeric_runs()` inside the Or. branch of `shelfmark_to_cudl_label`. Lookup handles collapse already; browse URL generation does not.
3. **NLI runtime migration omits `genizah_core.py`.** The actual `crossref_svc.get_cambridge_manifest(...)` and `get_cambridge_manifest_by_label(...)` call sites live in `genizah_core.py`. Plan 04's grep scope must explicitly include `genizah_core.py` and migrate those two branches; otherwise the wrapper exists but the runtime path is unchanged.
4. **`browse.py` ImportError snippet references undefined symbol.** Inside `except ImportError`, the code refers to `shelfmark_to_cudl_label` (which failed to import). Replace with a module-level `_BRIDGE_IMPORT_WARNED` + `_warn_bridge_import_failed()` helper, mirroring genizah_core.py.
5. **Scan-diff baseline does not prove the revised invariant.** `build_cudl_baseline_resolved.py` currently dumps every `cambridge_manifests` row with a URL, not the labels/classmarks that resolved pre-phase through the runtime/scanner. Capture pre-phase matched `normalized_shelfmark` values via the OLD scanner logic, then assert those same classmarks still match after bridge wiring. Drop the `[:500]` sample for at least one local full-baseline run.
6. **Fixture generator reads wrong orphan-CSV column.** `reports/cudl_orphans_all.csv` columns are `cudl_label,manifest_url,normalized_shelfmark` — the classmark is `row[2]`, not `row[0]`. Switch to `csv.DictReader` and `row["normalized_shelfmark"]`.

### Codex-only MEDIUM items (should address)
- `get_cambridge_manifest_with_bridge()` falls back to `get_cambridge_manifest_by_label(slug.upper())`, which produces `MOSSERIIII27O` not the actual CUDL label `MS-MOSSERI-III-00027-O`. For Mosseri, route through `construct_mosseri_cudl_label()` instead.
- Plan 01 internally inconsistent: `must_haves` requires `_BUILTIN_COLLISION_KEYS` non-empty, but interface says start with `set()` if none known. Either commit a known set or drop "non-empty."
- Golden fixture may miss the critical `or1080.11` collapsed-input case. Add explicit rows for `or1080.11`, `MS-MOSSERI-III-00027-O`, `mosseriiii27o`.
- Plan 05's `git checkout` to restore pre-phase reports is destructive. Add `--output`/`--suffix` to `scan_cudl_orphans.py` instead and emit post-phase reports to dedicated paths from the start.

### Codex-only LOW items
- Alias-index report writes on every build may be noisy/fail in packaged/read-only runs. Gate report emission behind a dev/audit flag.

### Gemini-only LOW items (re-emphasized)
- Startup latency budget for the ~140K-row alias index walk should be measured/asserted.
- One-time local full-baseline run (no slice) before merge.

### Divergent Views
- **Risk level:** Gemini → LOW (Approved). Codex → MEDIUM-HIGH (would not execute unchanged).
- The disagreement turns on whether the Round 2 revisions' *snippets and fixture mechanics* actually implement the stated designs. Codex's six HIGH items, if confirmed, would close that gap and bring both reviewers into alignment.

### Recommended Next Step

The Codex HIGH items are concrete, falsifiable claims about specific code/CSV columns/call sites — not stylistic preferences. They should be verified against the live codebase (especially items 3, 5, 6 which name specific files and column orders) before either accepting or rejecting them. If verified true, run `/gsd-plan-phase 84 --reviews` again to fold them in. If any are stale (e.g., genizah_core.py call sites have already moved), correct the plans manually. Gemini's LOW items can ride along in the same revision.
