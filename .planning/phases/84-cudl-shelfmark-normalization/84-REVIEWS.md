---
phase: 84
round: 3
reviewers: [gemini, codex]
reviewed_at: 2026-05-06T11:28:08Z
plans_reviewed:
  - 84-01-PLAN.md
  - 84-02-PLAN.md
  - 84-03-PLAN.md
  - 84-04-PLAN.md
  - 84-05-PLAN.md
self_skipped: claude (running inside Claude Code)
unavailable: [coderabbit]
---

# Cross-AI Plan Review — Phase 84: CUDL Shelfmark Normalization (Round 3, post-round-2-fixes)

This is the third review round, evaluating plans **after** the Round 2 revision pass that fixed all 6 verified Codex HIGH execution-level findings. The Round 2 REVIEWS.md content is preserved in git history (commit `979a5468`).

## Gemini Review

# Cross-AI Plan Review — Phase 84: CUDL Shelfmark Normalization (Round 3)

## Summary
The Round 3 implementation plans for Phase 84 are exceptionally rigorous and represent a "gold standard" for safe, data-driven system migration. All 6 HIGH-severity execution findings from the prior round have been meticulously addressed. The architecture successfully balances the need for increased coverage (resolving Mosseri and Or. classmarks) with an absolute "zero-regression" mandate for the existing ~217,000 records. The inclusion of a multi-layered verification suite—combining source-hash integrity, literal-output snapshots, a validated golden fixture, and a full-baseline scan-diff—provides a bulletproof safety net. These plans are technically mature, defensively designed, and ready for immediate execution.

## Strengths
- **Exhaustive Regression Guard:** Plan 05's combination of SHA256 source hashing for the canonical normalizer and a literal-output snapshot is an elite defensive pattern that makes accidental regressions impossible to miss.
- **Data-Driven Fixtures:** The transition from hand-authored test cases to a reproducible, validated fixture-generation script (`scripts/build_cudl_fixture.py`) ensures that tests reflect the reality of the ~140K CUL records.
- **Strict Ambiguity Handling:** The "collect-all / exclude-multi" policy for the alias index (Plan 02) and the "delta-introduced" collision isolation (Plan 01) are precisely tuned to allow normalization improvements while explicitly rejecting any change that would cause a mis-routing.
- **Unconditional Runtime Migration:** Plan 04 correctly identifies and migrates the actual runtime call paths in `genizah_core.py` (enrichment logic) and `nli_crossref_service.py`, fulfilling the architectural intent of the bridge.
- **Defensive Error Handling:** The "warning-once" module-level flags and graceful fallback logic ensure the application remains functional even if the bridge module or audit reports are missing.

## Concerns
- **None (HIGH/MEDIUM).** All prior execution-level risks have been mitigated.
- **Redundant Pattern Logic (LOW):** Plan 03's `shelfmark_to_cudl_label` implements the Mosseri zfill-stripping logic internally, which is identical to the logic in Plan 02's `_index_key_for_label`. While safe, this is a minor DRY violation.
- **Startup Latency Visibility (LOW):** Building the index involves walking ~140K rows. While efficient (O(1) lookups), adding a `DEBUG` level log with the elapsed time in milliseconds for `build_alias_index` would be beneficial for monitoring performance on lower-end desktop environments.

## Suggestions
- **Consolidate Mosseri Logic:** In `shared/shelfmark_bridge.py`, consider having `shelfmark_to_cudl_label` call `_index_key_for_label(construct_mosseri_cudl_label(shelfmark))` to ensure there is exactly one implementation of the Mosseri-label-to-slug transform.
- **Elapsed Time Logging:** Add a simple `start_time = time.time()` / `logger.info("... took %.2f seconds", ...)` block to `build_alias_index` to provide visibility into the ~140K-row walk cost during application startup.
- **Fixture Variety Assert:** In `scripts/build_cudl_fixture.py`, consider adding a small print or assert ensuring at least one row with a `DOTMARKER` (e.g., `1080.1.1`) is actually included in the final CSV to exercise the Or. numeric-collapse logic.

## Risk Assessment: LOW
The risk is low. The architecture is strictly additive (fallback-only), the most sensitive rules are protected by mandatory audits and exclusion sets, and the verification rigor is significantly higher than a standard feature phase. The plans have successfully solved the "false confidence" problem identified in prior rounds.

**Approval:** Approved for execution.

---

## Codex Review

## Summary

The Round 2 fixes close several prior issues in intent, but not fully in execution. The plans are stronger than Round 2, especially around NLI call-site migration and orphan CSV column handling, but I would not execute unchanged. New regressions were introduced in Plan 03/05, and two prior HIGH Mosseri concerns are effectively reopened by later snippets.

## Strengths

- Plan 04 now explicitly includes the real `genizah_core.py` Cambridge manifest call sites at ~3965 and ~3981.
- `reports/cudl_orphans_all.csv` column usage is now correct: `normalized_shelfmark` is the classmark, not `cudl_label`.
- Or. numeric forward URL handling is correctly called out: `Or. 1080.1.1` must become `or1080.11`.
- The ambiguity-exclusion design remains sound: collect claims first, exclude multi-`sys_id` keys, write a diagnostic report.
- The scan-diff invariant is directionally better than strict orphan-set subset testing.

## Concerns

- **HIGH: Plan 03 reintroduces the Mosseri `MS-` bug in `shelfmark_to_cudl_label()`.**
  The Plan 02 `_index_key_for_label()` fix strips `MS`, but Plan 03's Mosseri forward path duplicates the logic and does not strip `MS`. `shelfmark_to_cudl_label("Moss. III,27O")` would produce `msmosseriiii27o`, not `mosseriiii27o`. This breaks browse CUDL URLs.

- **HIGH: Plan 03's `lookup_cudl()` replacement drops the Plan 02 forward-label fallback.**
  The Plan 02 lookup handles `MS-MOSSERI-III-00027-O` via `_index_key_for_label()`. The Plan 03 snippet replaces `lookup_cudl()` with only `cudl_normalize()` plus Or.-collapse. That breaks the explicit Round 2 critical case.

- **HIGH: Plan 05 assumes `MetadataManager()` loads `libraries.csv`, but live code does not.**
  In the current code, `MetadataManager.__init__()` only loads small caches. `_load_csv_bank()` runs later via background loading or explicit call. Therefore `scripts/build_cudl_fixture.py`, `scripts/build_cudl_baseline_resolved.py`, and the `alias_index_built` pytest fixture will see an empty `csv_bank` unless they call `mm._load_csv_bank()` directly.

- **HIGH: Plan 04 may regress Mosseri manifest lookup by deleting the all-variant loop.**
  The old runtime path tries all `call_numbers_raw` variants. The new wrapper accepts only one `shelfmark`. If the primary shelfmark is not constructible but an alternate variant is, deleting the loop loses coverage. Keep the variant loop or make the wrapper accept variants/sys_id context.

- **HIGH: The baseline-still-resolves test does not catch wrong manifest routing.**
  Plan 05 records `manifest_url` but the test only asserts `get_cambridge_manifest_with_bridge(original_shelfmark)` returns non-None. A wrong CUDL manifest URL would pass. This weakens the core "no silent misrouting" guarantee.

- **MEDIUM: Generated CUL fixture rows use `cudl_normalize()` instead of the forward URL function.**
  Source A in `build_cudl_fixture.py` uses `cudl_normalize(variant)` for CUL rows, which can emit `or1080.1.1` rather than the viewer slug `or1080.11`. That under-tests the browse URL path.

- **MEDIUM: Unit tests mutate the real `reports/cudl_alias_collisions.csv`.**
  Synthetic `build_alias_index()` tests write fake collision keys into the real report path. This creates dirty working-tree noise and can overwrite the real diagnostic artifact.

- **MEDIUM: Import-failure fallback in `get_cambridge_manifest_with_bridge()` is not pre-phase equivalent.**
  If `shelfmark_bridge` import fails, the snippet returns `self.get_cambridge_manifest(shelfmark)` instead of canonical `normalize_shelfmark(shelfmark)` lookup. That contradicts the "degrade to v7.10 behavior" claim.

- **MEDIUM: Verification for NLI migration should also forbid surviving `get_cambridge_manifest_by_label()` calls in `genizah_core.py`.**
  The automated check shown only rejects `crossref_svc.get_cambridge_manifest(...)`, not the old label call.

- **LOW: There is still no explicit end-to-end shelfmark-search test.**
  Golden tests focus on `lookup_cudl()`. Add a test for `MetadataManager.search_by_meta("mosseriiii27o", "shelfmark")`.

## Suggestions

- Reuse `_index_key_for_label()` inside `shelfmark_to_cudl_label()` for Mosseri labels. Do not duplicate MS/zfill stripping logic.
- Merge Plan 03's `lookup_cudl()` changes into the Plan 02 implementation instead of replacing it. Preserve the `_index_key_for_label()` fallback, then add the Or.-collapse fallback.
- In every Plan 05 script/test that uses `MetadataManager`, call `mm._load_csv_bank()` explicitly before reading `mm.csv_bank` or relying on the alias index.
- Do not delete the Mosseri variant loop unless `get_cambridge_manifest_with_bridge()` accepts and iterates variants. The simpler safe change is to keep the loop and call the wrapper per variant.
- In the baseline regression test, assert the returned URL equals `row["manifest_url"]`, not merely non-None.
- Add `report_path=None` or `write_report=False` to `build_alias_index()` so unit tests can avoid writing real `reports/cudl_alias_collisions.csv`.
- Generate CUL fixture classmarks via `shelfmark_to_cudl_label()` where supported, and separately include lookup-only normalized forms if desired.
- Add a grep/test that `genizah_core.py` contains no surviving `get_cambridge_manifest_by_label(` after migration unless deliberately inside a preserved fallback block.

## Risk Assessment

**MEDIUM-HIGH.** The architecture is still good, and several prior HIGH items were addressed in the right direction. But later plan snippets reopen Mosseri slug handling, break forward-label lookup, and build a regression suite that may not load the data it thinks it is testing. The biggest residual risk is false confidence: tests could pass while browse URLs are wrong, baseline manifests are misrouted, or fixture generation silently exercises an empty `csv_bank`.

---

## Consensus Summary

**Verdict split persists for the third round.** Gemini sees Round 2 as having closed every meaningful risk and approves at LOW risk. Codex finds 5 NEW HIGH-severity issues — some of them reopen prior HIGH concerns through different code paths (Mosseri MS- handling, forward-label fallback) and others are genuinely new (MetadataManager not loading csv_bank in __init__, variant-loop deletion, baseline assertion strength).

The pattern across three rounds is consistent: Codex applies adversarial code-level scrutiny and finds concrete falsifiable claims about specific snippets; Gemini reviews architecture and high-level coherence. Both are useful but they are not the same review.

### Agreed Strengths
- NLI migration scope now includes the real `genizah_core.py` runtime call sites.
- Orphan-CSV column handling is correct (`normalized_shelfmark` at index 2).
- Or. numeric forward URL is correctly identified as needing collapse.
- Ambiguity-exclusion policy and scan-diff invariant designs are sound.

### Codex-only HIGH items (worth verifying against the live codebase)

These are concrete, falsifiable claims that the orchestrator should verify before deciding whether to do a Round 3 replan, accept-as-is, or hand-edit:

1. **Plan 03 duplicates Mosseri MS-stripping logic — and gets it wrong.** Plan 02 fixed `_index_key_for_label` to strip `MS`. Plan 03's `shelfmark_to_cudl_label` Mosseri branch reimplements the zfill-stripping inline (lines ~233-238 of 84-03-PLAN.md from Round 2) WITHOUT the MS-strip. Result: `shelfmark_to_cudl_label('Moss. III,27O')` → `msmosseriiii27o` instead of `mosseriiii27o`. **Suggested fix:** have Plan 03's Mosseri branch call `_index_key_for_label(mosseri_label)` instead of duplicating logic.

2. **Plan 03's `lookup_cudl()` replacement drops the forward-label fallback added in Plan 02.** Plan 02 added a path so `lookup_cudl('MS-MOSSERI-III-00027-O')` resolves via `_index_key_for_label`. If Plan 03's snippet for `lookup_cudl` only retries with `_collapse_numeric_runs`, that Round 2 critical case regresses. **Suggested fix:** Plan 03 should ADD the Or.-collapse retry to the existing Plan 02 cascade, not replace it.

3. **Plan 05 scripts/tests assume `MetadataManager()` constructor loads `libraries.csv`.** Live code: `MetadataManager.__init__` only initializes small caches; `_load_csv_bank()` is invoked later via background loader or an explicit call. Therefore `build_cudl_fixture.py`, `build_cudl_baseline_resolved.py`, and the `alias_index_built` pytest fixture will see an empty `csv_bank` unless they call `mm._load_csv_bank()` explicitly. **Suggested fix:** every script/fixture that uses `MetadataManager` must call `mm._load_csv_bank()` after construction.

4. **Plan 04 deletes the per-variant Cambridge manifest loop in `genizah_core.py:~3981`.** That loop iterates every `call_numbers_raw` variant. The new wrapper takes a single `shelfmark`. If a non-primary variant is the one that would have resolved, coverage regresses. **Suggested fix:** keep the variant loop and call `get_cambridge_manifest_with_bridge` per variant, OR extend the wrapper to accept a variants list / row context.

5. **Plan 05 baseline test only asserts non-None URL, not URL equality.** A bridge regression that routes a baseline shelfmark to a *different* (wrong) CUDL manifest passes the current assertion. The "no silent misrouting" guarantee depends on equality. **Suggested fix:** assert `result_url == row['manifest_url']`, not `result_url is not None`.

### Codex-only MEDIUM items
- CUL fixture rows generated via `cudl_normalize(variant)` rather than `shelfmark_to_cudl_label`, under-testing the browse URL path (`or1080.1.1` enters the fixture instead of `or1080.11`).
- Unit tests write to the real `reports/cudl_alias_collisions.csv` — should support an injected/temp report path.
- Wrapper's import-failure fallback returns `get_cambridge_manifest(shelfmark)` (raw shelfmark) instead of `get_cambridge_manifest(normalize_shelfmark(shelfmark))` (canonical), which is what "degrade to v7.10" actually means.
- NLI migration grep should also reject surviving `get_cambridge_manifest_by_label(` calls in `genizah_core.py`, not just `get_cambridge_manifest(`.

### Codex-only LOW
- Add an end-to-end `MetadataManager.search_by_meta('mosseriiii27o', 'shelfmark')` test to exercise the runtime search path, not just `lookup_cudl()`.

### Gemini-only LOW
- Plan 03 duplicates Plan 02's MS/zfill stripping (same observation Codex flags as HIGH #1, but Gemini frames it as DRY rather than correctness).
- Add elapsed-time logging to `build_alias_index` for visibility during ~140K-row walk.
- `build_cudl_fixture.py` should assert at least one Or. dotted-numeric row is included.

### Divergent Views
- **Risk level:** Gemini → LOW (Approved). Codex → MEDIUM-HIGH (would not execute unchanged). Identical pattern to Round 2.
- The crux is again: Codex's claims name specific lines/snippets that, if true, mean the fixes are "correct in intent but wrong in execution." Items #1 and #2 are particularly worrying because they reopen prior HIGH findings through different code paths — a sign that the fixes were applied locally without cross-checking that the same logic appears elsewhere.

### Recommended Next Step

Verify Codex HIGH items #1, #2, #3, #5 against the actual revised plan text (and the `MetadataManager` source for #3). If 2+ are confirmed, run `/gsd-plan-phase 84 --reviews` for a Round 3 replan. If only #5 (URL equality) is confirmed, that single fix can be hand-edited in Plan 05 without a full replan cycle.

If three review rounds is the budget and the user wants to ship, the safer move is: hand-fix the verified items in the plans, then proceed to `/gsd-execute-phase 84`. The execution phase has its own gates and the threat is a wrong CUDL URL for a small fragment of users — not data corruption.
