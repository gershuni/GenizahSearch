---
phase: 86
pass: 3
reviewers: [gemini, codex]
gemini_model: gemini-3-pro-preview
codex_model: default
reviewed_at: 2026-05-11T13:22Z
plans_reviewed:
  - 86-01-PLAN.md
  - 86-02-PLAN.md
  - 86-03-PLAN.md
  - 86-04-PLAN.md
  - 86-05-PLAN.md
prior_review: see git history (commits be1aa9ca pass 2, 89e354e1 pass 1)
note: gemini default (gemini-3-flash-preview) returned 429 quota — retried with gemini-3-pro-preview which produced a substantive review
---

# Cross-AI Plan Review — Phase 86 (Pass 3)

This is the third review pass on Phase 86. The plans have been revised twice
to fold in prior gemini+codex feedback (replan commits f7bb5658 and 70237a0e).
Pass 3 evaluates whether prior concerns were correctly addressed and surfaces
any residual or newly-introduced issues.

---

## Gemini Review (gemini-3-pro-preview)

### 1. Summary

This is an exceptionally mature set of plans that systematically and comprehensively addresses all feedback from the Pass 2 reviews. The architectural pivot to a CUDL-walked generation strategy, combined with the new Python-based PowerShell-safe orchestrator (`scripts/phase86_apply.py`) and the 3-table production-correct SQL join schema, transforms this from a high-risk data mutation into a controlled, safe, and verifiable deployment. The level of detail and defensive engineering (such as preflight assertions, rollback validation, and explicit `is_synthetic` routing) is outstanding. The plans are essentially ready for execution, with only minor test-integration and script-hygiene adjustments required.

### 2. Strengths

- **Robust Orchestration (Plan 86-04):** Consolidating the operational sequence into a single, cross-platform Python orchestrator (`scripts/phase86_apply.py`) eliminates brittle shell idioms and enforces strict preflight, backup, and rollback validation before any data is mutated.
- **Honest Audit Reporting (Plan 86-04):** Renaming the alias-only tier to `phase86_existing_alma_candidate` and adding explicit framing in `cudl_coverage.md` prevents overstating "coverage achieved," directly addressing the Codex HIGH concern.
- **Status-Aware Bridge API (Plan 86-01):** The `explain_fist_by_cudl` API elegantly distinguishes between `multi_inventory_ambiguous` and `not_found`, ensuring the residue reports accurately reflect the nature of the ambiguity.
- **Production-Correct SQL (Plan 86-01 & 86-02):** Routing the title metadata extraction through the `dbo_Signature` table explicitly matches the `export_fist_enrichment.py` join schema, preventing the silent title-loss bug identified in Pass 2.
- **Concrete Adjudication Rules (Plan 86-03):** Forcing proposed rules in the `86-RESIDUE-PATTERNS.md` artifact to be explicit FIST→CUDL transformations removes ambiguity and streamlines the transition from human judgment to code.

### 3. Concerns

- **MEDIUM (Plan 86-02): Idempotency Test Mocks Out the Fix.** The `test_idempotent_when_synthetic_block_present_in_csv_bank` test uses the `empty_phase84_index` fixture, which unconditionally mocks `lookup_cudl` to return `None`. This bypasses the integration of the idempotency fix (`_build_real_only_csv_bank`) with `build_alias_index`, meaning the test only unit-tests the dict-stripping function but fails to prove that the actual Phase 84 alias index ignores synthetic rows.
- **LOW (Plan 86-04): Empty `inv_id` on Synthetic Hits.** In `scan_cudl_coverage_phase86.py`, when a synthetic row is resolved via `lookup_cudl` (the `phase86_synthetic` tier), the `inv_id` variable is left as an empty string. The markdown report authoring correctly falls back to inferring the collection from the classmark prefix, but extracting the true `inv_id` from the synthetic `sys_id` would be cleaner.
- **LOW (Plan 86-04): SQLite Connection Context Manager.** In `step_0_5_preflight()`, the syntax `with sqlite3.connect(...) as fist:` creates a transaction context, not a connection-closing context. While acceptable in a short-lived script that exits immediately, it leaves file descriptors open slightly longer than necessary.

### 4. Suggestions

- **Refactor the Idempotency Test:** In `tests/test_synthetic_generation_phase86.py`, remove the `empty_phase84_index` fixture from `test_idempotent_when_synthetic_block_present_in_csv_bank`. Instead, explicitly call `build_alias_index(_build_real_only_csv_bank(csv_bank))` and assert that `lookup_cudl('tsns329.96') is None` before invoking `_build_qualifying_inventories`. This provides true integration coverage of the HIGH-1 fix.
- **Decode Synthetic `inv_id`:** In `scan_cudl_coverage_phase86.py`, when `is_synthetic_sys_id(sys_id)` evaluates to True, use `decode_inventory_id(sys_id)` to populate the `fist_inventory_id` output column instead of returning an empty string.
- **Use `contextlib.closing`:** In `scripts/phase86_apply.py`, wrap the sqlite3 connections in `contextlib.closing(sqlite3.connect(...))` to ensure deterministic file descriptor release.

### 5. Risk Assessment

**Overall Risk: LOW.**

The structural and architectural risks of Phase 85 and the early Phase 86 drafts have been thoroughly engineered out. The CUDL-walk inversion guarantees the image-bearing invariant, and the PowerShell-safe Python orchestrator drastically reduces the operational risk of the multi-step deployment. With the minor testing adjustment suggested above, this plan set is safe, verifiable, and ready for execution.

---

## Codex Review

### Summary

Pass 3 plans are much stronger than pass 2: the major prior issues were mostly addressed, especially idempotent generation, correct FIST title join path, audit-tier naming, PowerShell-safe orchestration, and FIST→CUDL rule direction. I would still not execute unchanged. The remaining risks are concentrated in 86-04 audit classification and operational verification, plus a few tests that assert the right idea but do not actually exercise the failure mode they are meant to guard.

### Strengths

- 86-01 correctly keeps `shared/shelfmark_bridge.py` byte-stable and moves FIST↔CUDL logic into `shared/fist_cudl_bridge.py`.
- Pass-2 SQL join feedback was folded into 86-01/86-02: title metadata routes through `dbo_InventorySignature -> dbo_Signature -> dbo_UnitCatalogRec`.
- 86-02 now explicitly strips synthetic sys_ids before building the Phase 84 alias index, which is the right fix for the re-apply block-wipe risk.
- 86-03 has a clear human checkpoint and stop rule. Accepted rules are now specified as FIST→CUDL transformations, which matches `fist_to_cudl_keys`.
- 86-04's `phase86_existing_alma_candidate` rename is the right framing; it avoids overstating runtime coverage.
- The preflight in 86-04 is much safer than pass 2: tighter count bounds, T-S NS 329.96 positive assertions, rollback validation, and FJMS smoke checks.
- 86-05 correctly recommends deferring version metadata for a data-only web deploy.

### Concerns

- **HIGH — 86-04 scanner can still overcount `phase86_synthetic`.**
  In `scripts/scan_cudl_coverage_phase86.py::classify_classmark`, the fallback branch classifies `status == "single" and rec.has_alma is False` as `phase86_synthetic`. That ignores Plan 02 exclusion paths: D-06 parent-shadow, CSV-injection rejection, or any future "no emit" condition. Post-apply, the scanner should classify a row as `phase86_synthetic` only if the sys_id resolves from `libraries.csv` or the inventory id appears in `fist_data/synthetic_manifest.json`.

- **HIGH — 86-04 report template contradicts its own acceptance criteria.**
  The `reports/cudl_coverage.md` template uses the term `truly_orphan` several times, including "Legacy scanner baseline" and "<200 truly-orphan target," but acceptance requires `grep -c "truly_orphan" reports/cudl_coverage.md` to return 0. This will fail exactly as written.

- **MEDIUM — 86-02 idempotency test does not really test the idempotency failure.**
  `test_idempotent_when_synthetic_block_present_in_csv_bank` monkeypatches `scripts.generate_synthetic_rows.lookup_cudl` to always return `None`, so it cannot catch the bug where a synthetic alias-index hit incorrectly skips the row. It tests `_build_real_only_csv_bank`, but not that `build_alias_index(real_only)` changes `lookup_cudl` behavior versus raw `csv_bank`.

- **MEDIUM — 86-01 title tie-break SQL is not actually deterministic.**
  The proposed subquery selects `cat.Title`, `cat.GenizahTitleText`, and `MIN(cat.UnitCatalogRecId)` while grouping only by `isig.InventoryId`. SQLite permits this, but the non-aggregated title fields are not guaranteed to come from the row with the minimum catalog id. Use a window function or a join against a `MIN(UnitCatalogRecId)` subquery.

- **MEDIUM — 86-03 nearest-neighbor generation may be slower and less complete than assumed.**
  `WHERE LOWER(inv.Shelfmark) LIKE ? LIMIT 2000` likely prevents index use and may scan large parts of `dbo_Inventory` per residue row. It also misses noisy-prefix records such as `AIU: CUL: Or...` because they do not start with the mapped family prefix. For ~1,599 residue rows, this can be both slow and incomplete.

- **MEDIUM — 86-04 FJMS smoke check uses overly broad synthetic detection.**
  `CAST(AlmaId AS TEXT) LIKE '99%'` can flag any real AlmaId starting with 99. The synthetic predicate should match the real Option-2 contract, ideally via `is_synthetic_sys_id`, not a prefix-only SQL check.

- **MEDIUM — 86-04 skips missing FJMS tables.**
  The smoke check prints `SKIPPED` on `sqlite3.OperationalError`. For the 12 "verbatim required" tables, missing table should fail, not skip, otherwise a broken export can pass.

- **MEDIUM — 86-04 does not fully close the "both apps build" roadmap criterion.**
  It runs pytest and web UAT, but desktop is explicitly deferred unless running from source. If success criterion 4 still literally means both apps build green, the plan needs a desktop-from-source smoke/build check or an explicit roadmap waiver.

- **LOW — 86-04 orchestration should use `sys.executable`.**
  The orchestrator uses `["python", ...]` and `["pytest", ...]`. On Windows/PowerShell and venvs, `sys.executable -m pytest` is safer and more reproducible.

- **LOW — 86-05 "full-release" option is ambiguous.**
  The checkpoint presents full release as an option, but later tasks forbid creating the tag/release in scope. That is okay if intentional, but the option should be renamed "prepare full release metadata" or the forbidden actions should move outside the plan explicitly.

### Suggestions

- In `scan_cudl_coverage_phase86.py`, load `fist_data/synthetic_manifest.json` and classify no-Alma bridge hits as:
  - `phase86_synthetic` only if `inventory_id in manifest`
  - `phase86_excluded_parent_shadow`, `phase86_excluded_csv_injection`, or `phase86_residue` otherwise
- Replace all `truly_orphan` prose in `reports/cudl_coverage.md` with "legacy orphan" or "legacy scanner orphan" so the Pass 2 HIGH-3 terminology remains consistent.
- Strengthen the idempotency test by building the real `shelfmark_bridge` alias index twice: once with raw csv_bank containing a synthetic row and once with `_build_real_only_csv_bank(csv_bank)`, then assert only the raw version returns a synthetic hit.
- Fix the 86-01 title SQL with deterministic selection, for example `ROW_NUMBER() OVER (PARTITION BY InventoryId ORDER BY UnitCatalogRecId)` if SQLite version supports it, or a `MIN(UnitCatalogRecId)` CTE joined back to `dbo_UnitCatalogRec`.
- For 86-03, preload FIST candidates into normalized family buckets once, including post-colon tail forms, instead of running `LOWER(Shelfmark) LIKE ? LIMIT 2000` per residue.
- Change FJMS synthetic collision checks from `LIKE '99%'` to exact helper-equivalent checks: 18 digits, prefix `99`, suffix `000000`, decodable InventoryId.
- Make missing FJMS tables fail the orchestrator unless there is a documented migration reason.
- Add one desktop-from-source smoke command or explicitly amend the roadmap criterion to "web deploy now; desktop data bundled next release."
- In `scripts/phase86_apply.py`, use `sys.executable` for Python subprocesses and `sys.executable, "-m", "pytest"` for tests.

### Risk Assessment

**Overall risk: MEDIUM-HIGH until the 86-04 scanner classification is fixed; MEDIUM after that.** The architecture is now sound and the prior pass-2 feedback was mostly incorporated. The remaining high-risk issue is audit truthfulness: the bridge-aware scanner can still report excluded no-Alma candidates as synthetic coverage. Since Phase 86 is primarily an audit/data activation phase, inaccurate tiering would undermine AUDIT-01/02 even if generation itself works. The rest of the concerns are fixable implementation hardening items, not conceptual blockers.

---

## Consensus Summary

### Agreed Strengths

Both reviewers agree on the architectural quality of the pass-3 revision:

- **CUDL-walked generation strategy** — the inversion to walk CUDL classmarks (rather than FJMS inventories) guarantees the image-bearing invariant.
- **PowerShell-safe Python orchestrator** (`scripts/phase86_apply.py`) — cross-platform, with preflight, backup, and rollback validation.
- **3-table production-correct SQL** — title metadata via `dbo_InventorySignature → dbo_Signature → dbo_UnitCatalogRec` matches the `export_fist_enrichment.py` join schema and fixes the Pass 2 silent-title-loss bug.
- **Honest audit tier naming** — `phase86_existing_alma_candidate` replaces the misleading prior label; Codex explicitly credits this as resolving its prior HIGH concern.
- **Status-aware bridge API** — `explain_fist_by_cudl` distinguishing `multi_inventory_ambiguous` vs `not_found`.
- **86-02 strips synthetic sys_ids before building Phase 84 alias index** — correct re-apply idempotency fix in principle.

### Agreed Concerns (Highest Priority — Raised by Both Reviewers)

- **86-02 idempotency test does not exercise the integration path it claims to test.** Both reviewers (Codex MEDIUM, Gemini MEDIUM) flag that `test_idempotent_when_synthetic_block_present_in_csv_bank` mocks `lookup_cudl` to return `None` (via the `empty_phase84_index` fixture), so it never proves that `build_alias_index(_build_real_only_csv_bank(csv_bank))` produces a synthetic-free alias index. The test asserts the right idea but bypasses the fix. **Required action:** call the real `build_alias_index` on both raw and real-only csv_banks and assert `lookup_cudl('tsns329.96')` returns `None` only for the real-only variant.

### Codex-Only HIGH Concerns (Gemini Did Not Flag — Worth Investigating)

These are the two HIGH findings only Codex raised, both in 86-04:

- **HIGH-1: Scanner overcount of `phase86_synthetic`.** `scan_cudl_coverage_phase86.py::classify_classmark` classifies any `status == "single" and rec.has_alma is False` as `phase86_synthetic`, but D-06 parent-shadow exclusion and CSV-injection rejection can leave bridge-hit InventoryIds that were NOT emitted to `synthetic_manifest.json`. The scanner would report excluded no-Alma candidates as synthetic coverage, undermining AUDIT-01/02 truthfulness.
  **Recommended fix:** load `fist_data/synthetic_manifest.json` and classify no-Alma bridge hits as `phase86_synthetic` only when `inventory_id in manifest`; otherwise route to `phase86_excluded_parent_shadow`, `phase86_excluded_csv_injection`, or `phase86_residue`.

- **HIGH-2: `cudl_coverage.md` template contradicts its own acceptance check.** The template uses `truly_orphan` in body text ("Legacy scanner baseline", "<200 truly-orphan target"), but acceptance requires `grep -c "truly_orphan" reports/cudl_coverage.md` to return 0. Plan will fail acceptance exactly as written.
  **Recommended fix:** rename all occurrences of `truly_orphan` to `legacy_orphan` or `legacy_scanner_orphan` to match Pass 2 HIGH-3 terminology fix.

### Divergent Views

- **Overall risk level: Gemini LOW vs Codex MEDIUM-HIGH→MEDIUM.** Gemini reads the orchestrator and CUDL-walk inversion as having engineered out the structural risk; Codex agrees on the architecture but holds the line on audit-classification truthfulness as a HIGH issue. The divergence is real and worth resolving — if the HIGH-1 scanner classification is fixed before execution, both reviewers converge on LOW-MEDIUM.

- **Codex-only MEDIUM findings to weigh:**
  - 86-01 title tie-break SQL non-determinism (`GROUP BY isig.InventoryId` does not guarantee `cat.Title` comes from the `MIN(UnitCatalogRecId)` row in SQLite).
  - 86-03 nearest-neighbor generation may be slow + incomplete (`LOWER(inv.Shelfmark) LIKE ?` per residue × ~1,599 rows; missing post-colon noisy prefixes).
  - 86-04 FJMS smoke check `CAST(AlmaId AS TEXT) LIKE '99%'` is broader than the real synthetic predicate (`is_synthetic_sys_id` would be exact).
  - 86-04 FJMS missing-table policy: `SKIPPED` on `sqlite3.OperationalError` can mask a broken export.
  - 86-04 vs roadmap criterion 4 ("both apps build"): web-only deploy in 86-05 means desktop is not exercised in this phase; either add a desktop-from-source smoke or get an explicit roadmap waiver.
  - 86-05 "full-release" checkpoint option is ambiguous since later tasks forbid the tag/release.

- **Gemini-only LOW findings to weigh:**
  - 86-04 synthetic `inv_id` left empty in scanner output; should `decode_inventory_id(sys_id)` to populate.
  - 86-04 `with sqlite3.connect(...) as fist:` is a transaction context, not a closing context; use `contextlib.closing(...)` for deterministic fd release.

### Recommended Next Action

Pass 3 is materially better than pass 2 but **should not execute unchanged**. Before execution:

1. **Fix the two Codex HIGHs in 86-04** (scanner classification using `synthetic_manifest.json`; `truly_orphan` → `legacy_orphan` rename throughout `cudl_coverage.md`).
2. **Fix the shared MEDIUM in 86-02** (replace `empty_phase84_index` fixture with real `build_alias_index` integration so the idempotency test actually exercises the fix).
3. **Fold the remaining Codex MEDIUMs** as plan-task amendments: deterministic title SQL (86-01), residue candidate prefetch (86-03), exact synthetic predicate in FJMS smoke (86-04), required-table failure mode (86-04), explicit roadmap waiver or desktop smoke (86-04), unambiguous full-release option (86-05).
4. **Fold the Gemini LOWs** as script-hygiene items in 86-04 (`decode_inventory_id` + `contextlib.closing`).

To incorporate into planning:
  `/gsd-plan-phase 86 --reviews`
