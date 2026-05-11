---
phase: 86
reviewers: [gemini, codex]
reviewed_at: 2026-05-11
plans_reviewed:
  - 86-01-PLAN.md
  - 86-02-PLAN.md
  - 86-03-PLAN.md
  - 86-04-PLAN.md
  - 86-05-PLAN.md
overall_risk: HIGH (both reviewers concur — do not execute unchanged)
---

# Cross-AI Plan Review — Phase 86

> Two independent senior-engineer reviews from gemini and codex CLIs.
> Claude CLI skipped (we are Claude — running this review session inside Claude Code).
> Both reviewers were given the full CONTEXT.md, RESEARCH.md, and all 5 PLAN.md files.
> Convergence is striking: both flag HIGH-severity execution blockers, with substantial overlap.

---

## Gemini Review

### 1. Summary

The plans successfully architect the pivot from a FIST-walked generation strategy to a CUDL-walked strategy. This inversion is highly effective at bypassing the multi-signature fan-out ambiguities that plagued Phase 85. The operational sequencing (generate → export → scan) correctly resolves the stale `fjms_enrichment.db` blocker, and the release strategy meticulously honors the web-only deployment policy. However, the implementation drops critical FJMS metadata queries during the rewrite and fails to apply the explicit Mosseri sub-fragment concatenation rules, which will result in data regressions (missing titles) and under-recovery of Mosseri manuscripts.

### 2. Strengths

* **CUDL-Walk Inversion (Plan 02):** Changing the anchor to `cambridge_manifests` elegantly eliminates the `multi_signature` fan-out problem by making the CUDL classmark the primary key.
* **Sibling Module Pattern (Plan 01):** Creating `shared/fist_cudl_bridge.py` instead of mutating `shared/shelfmark_bridge.py` guarantees the `NORM-04` contract is preserved for existing `libraries.csv ↔ CUDL` lookups.
* **Operational Sequencing (Plan 04):** Enforcing the `generate_synthetic_rows.py --apply` → `export_fist_enrichment.py` → `scan_cudl_orphans.py` pipeline perfectly addresses the stale sidecar gap identified in the Phase 85 verification report.
* **Regression Guarding (Plan 04):** The hybrid approach to `AUDIT-03` (20-row golden fixture + regex scan-sweep) is an excellent, robust safety net for the v7.9.4 NLI fix.
* **Release Restraint (Plan 05):** Providing a formal `CHECKPOINT` to defer the desktop installer rebuild aligns perfectly with the `feedback_no_github_release_for_web_only.md` constraint.

### 3. Concerns

* **HIGH: Complete Loss of FJMS Titles in Synthetic Rows (Plan 02)**
  * *Evidence:* Plan 02's rewrite of `_build_qualifying_inventories` drops the `dbo_UnitCatalogRec` query entirely. It relies on `lookup_fist_by_cudl`, which only returns `InventoryRecord(inventory_id, fist_shelfmark, has_alma)`. The generation dictionary hardcodes `"title_heb": None, "genizah_title": None`.
  * *Impact:* The `_resolve_title` function will fall back to repeating the shelfmark. Synthetic rows written to `libraries.csv` will have no actual titles, discarding valuable FJMS catalog data.
* **HIGH: Mosseri Sub-Fragment Concatenation Missing (Plan 01)**
  * *Evidence:* Pitfall 1 explicitly warns that CUDL stores both `mosseriiii27.1` and `mosseriiii271`. The `fist_to_cudl_keys` implementation in Plan 01 only emits the with-dot form (`keys.add(f"mosseri{roman}{rest_norm}")`).
  * *Impact:* Fails to emit the concatenated variant. You will under-recover the ~3,086 Mosseri sub-fragments that rely on this.
* **HIGH: Flawed Nearest-Neighbour Prefix Matching (Plan 03)**
  * *Evidence:* `build_residue_patterns_artifact.py` extracts the first 5 characters of the CUDL classmark (`prefix = classmark[:5]`) and runs a `LIKE '%prefix%'` query against a whitespace/hyphen-stripped FIST shelfmark.
  * *Impact:* For a Mosseri classmark like `mosseriiii117.1a`, the prefix `mosse` is checked against the stripped FIST shelfmark `moss.iii,117.1a`. This will never match. The artifact will fail to surface FIST candidates for the Mosseri and T-S families.
* **MEDIUM: Vestigial "Lowest SignatureId" Tests (Plan 01/02)**
  * *Evidence:* The test `test_multi_signature_within_one_inventory_picks_lowest` asserts that the lowest SignatureId is picked. However, the new CUDL-walked implementation queries `dbo_Inventory` directly and never actually fetches `SignatureId`.
  * *Impact:* The tests are conceptually misleading. The ambiguity is resolved by the architecture itself (one row per `InventoryId` in `dbo_Inventory`), not by a tie-breaker.

### 4. Suggestions

* **Plan 01 & 02 (Title Restoration):** Extend `InventoryRecord` to include `title_heb` and `genizah_title`. Modify `build_fist_alias_index` to `LEFT JOIN dbo_UnitCatalogRec` so that the alias index caches the titles, allowing `lookup_fist_by_cudl` to return them for Plan 02 to inject into `libraries.csv`.
* **Plan 01 (Mosseri Fix):** Update the Mosseri block in `fist_to_cudl_keys` to emit both forms:
  ```python
  keys.add(f"mosseri{roman}{rest_norm}")
  keys.add(f"mosseri{roman}{rest_norm.replace('.', '')}")
  ```
* **Plan 03 (Prefix Fix):** Adjust the nearest-neighbour prefix logic to map known CUDL prefixes to their FIST equivalents before querying (e.g., if classmark starts with `mosseri`, use `moss.` as the SQL `LIKE` prefix; if `tsns`, use `t-s ns`).
* **Plan 02 (Injection Guard Validation):** Ensure `_has_csv_injection_leader` is checking the newly restored title variables, not just the shelfmark string.

### 5. Risk Assessment

**Overall Risk Level: HIGH**

The architectural pivots are excellent, but the data-flow regressions in Plan 01 and Plan 02 are critical. By accidentally severing the query to `dbo_UnitCatalogRec`, the plan neuters the synthetic rows of their titles. Furthermore, the explicit omission of the Mosseri concat-form directly violates a known pitfall from the research phase. These plans cannot be executed safely until the metadata extraction and Mosseri generation rules are patched.

---

## Codex Review

### Summary

The phase direction is right: CUDL-walked, image-bearing-only, no FJMS-only synthetic rows, and no runtime branching. That directly addresses the Phase 85 failure mode in principle. But the plans as written have several execution-level blockers and a deeper audit math problem: adding only no-Alma synthetic rows will not by itself drop `scan_cudl_orphans.py` from ~6K to ~1.6K unless the scan is upgraded to count Phase 86 FIST-bridge matches. The current plans also rely on `--dry-run` writing residue artifacts, but the existing generator returns before writing those files. I would not execute these plans unchanged.

### Strengths

- 86-01 correctly chooses a sibling `shared/fist_cudl_bridge.py` instead of mutating `shared/shelfmark_bridge.py`; that preserves the Phase 84 NORM-04 boundary.
- 86-02 is directionally correct: it removes the Phase 85 FIST-walked, FJMS-metadata inclusion predicate and makes CUDL manifest presence the source of truth.
- 86-02 Task 2 includes the right core fixtures: T-S NS 329.96, has-Alma skip, image-bearing invariant, parent-shadow filter, and residue pattern tagging.
- 86-03 correctly recognizes the 1,599 residue as human-adjudication territory, not something to normalize blindly.
- 86-04 includes the right operational dependency order: generate synthetic block before exporting `fjms_enrichment.db`, then scan/report/UAT.
- AUDIT-03's scan-sweep idea is good: `library_code='Oxford'` plus NLI call-number regex is the right regression signature.

### Concerns

- **HIGH — 86-02 / 86-04 audit math is inconsistent.** The research says 3,455 CUDL orphans resolve to existing-Alma FIST inventories and only ~108 resolve to no-Alma synthetic candidates. If 86-02 skips `rec.has_alma=True`, `libraries.csv` only gains the no-Alma rows. A plain `scan_cudl_orphans.py` re-run will not drop from ~6,053 to ~1,599 just from ~108 new rows. The scan/report must classify Phase 86 bridge matches directly, or the expected AUDIT-01 result is wrong.
- **HIGH — 86-04 assumes `scan_cudl_orphans.py` is suitable unchanged.** Current scanner builds normalized variants from `libraries.csv` rows with `row[3] == "CUL"` only and does not call `lookup_cudl` or the new FIST bridge. It will miss Mosseri synthetic rows and many Phase 84/86 alias forms. This undermines AUDIT-01 and `reports/cudl_coverage.md`.
- **HIGH — 86-03 depends on `--dry-run` writing residue CSV, but current generator does not.** Existing `main()` returns immediately on `args.dry_run` before `_write_manifest`, `_write_residue`, and `_write_coverage`. 86-03 Task 1's first step will likely read stale or absent `reports/synthetic_ambiguity_residue.csv`.
- **HIGH — 86-01 misses the Mosseri concat form called out in RESEARCH.** The research explicitly says Mosseri Roman must emit both `mosseriiii27.1` and concat-style `mosseriiii271`. 86-01 only requires `mosseriiii27.1`, so it can under-recover the bulk Mosseri class.
- **HIGH — 86-02 loses multi-inventory observability.** `lookup_fist_by_cudl()` returns `None` for both "not found" and "multi_inventory ambiguous," so generation logs both as `ambiguity_kind='no_fist_match'`. But 86-04 requires a "Multi-inventory ambiguous" count. That count becomes unrecoverable unless the bridge returns a status object.
- **HIGH — 86-02's has-Alma skip may hide a product gap.** If a CUDL classmark maps to an existing Alma row under a different shelfmark form, skipping it avoids duplicate synthetic rows, but the app may still not resolve that CUDL classmark in shelfmark search unless Phase 84 aliasing or the audit/search path is extended. The plan should state whether these are "covered for audit only" or actually searchable.
- **HIGH — 86-03 nearest-neighbor artifact is too weak for human adjudication.** The proposed SQL `LIKE` on raw `Shelfmark` after limited replacements will miss obvious forms like `T-S F1(1).100`, `Or.1080 11.1`, and `Moss. III,117.1a`. The artifact also lacks manifest URLs, FIST InventoryId/SignatureId, catalog title/description snippets, and image links, which are needed for granularity decisions.
- **MEDIUM — 86-01 D-04 test is misleading.** The bridge SQL does not read `dbo_InventorySignature` or `dbo_Signature`, so a "12 signatures" fixture does not actually exercise multi-signature relaxation. It only proves one inventory shelfmark resolves.
- **MEDIUM — 86-01 `(N)` strip is under-gated.** `_SERIES_N_RE` strips any parenthetical number in any shelfmark. That can create false aliases outside T-S F/T-S Ar unless tests add negative fixtures or the rule is prefix-gated.
- **MEDIUM — 86-02 `_classify_library_code` is too narrow.** It returns `Mosseri` only when shelfmark starts with `moss.`. FIST noise examples include `Mosseri: Moss. IV,27.1`, which would be misclassified as `CUL`.
- **MEDIUM — 86-02 tests will likely not run as written.** Existing `_make_nli_seed` creates `cambridge_manifests(normalized_shelfmark)` only, but the new implementation selects `label, manifest_url, normalized_shelfmark`. Also, monkeypatching `shared.shelfmark_bridge.lookup_cudl` will not affect a directly imported `lookup_cudl` in `scripts.generate_synthetic_rows`.
- **MEDIUM — 86-04 rollback is incomplete.** It relies on a gz backup of `fjms_enrichment.db` but does not require creating/verifying one before export. It should also back up `synthetic_manifest.json`.
- **MEDIUM — 86-04 FJMS smoke check is too strict.** Requiring synthetic AlmaId count in `catalog` to match manifest count can fail for valid CUDL-only rows with no FJMS catalog record. Validate no collisions and expected table counts, not one catalog row per synthetic.
- **MEDIUM — AUDIT-03 20-row fixture is partial.** The scan-sweep catches Oxford re-flips, but a future regression to another wrong code would only be caught for 20 sampled rows. All 461 sys_ids are cheap to test and should be used.
- **LOW — 86-05 web-only version bump is questionable.** Bumping `version.py`, Inno metadata, and README while intentionally not publishing a desktop release creates version ambiguity. Since 86-05 is optional, defaulting to defer is cleaner unless the web app needs a displayed version bump.

### Suggestions

- Update 86-04 scope to include a real Phase 86 coverage scanner, not just `scan_cudl_orphans.py` unchanged. It should output categories: Phase 84 hit, Phase 86 FIST bridge hit with Alma, Phase 86 synthetic/no-Alma, multi-inventory ambiguous, unresolved residue.
- Change 86-01 `lookup_fist_by_cudl` to return a structured result with `status`, `entries`, and `InventoryRecord`, or provide a separate `explain_fist_by_cudl()` for generation/audit.
- Add Mosseri tests for both `Moss. III,27.1 -> mosseriiii27.1` and `mosseriiii271`.
- Gate `(N)` stripping to the intended families or add negative tests proving it does not create false matches for unrelated shelfmarks.
- Fix 86-02 dry-run behavior: either `--dry-run` writes residue/manifest to explicit dry-run paths, or 86-03 must run a dedicated residue-builder command.
- Preserve lightweight FIST metadata in 86-02: populate `title_heb`, `genizah_title`, and `has_fjms_metadata` when present, but do not use those fields as qualification criteria.
- Replace 86-03's nearest-neighbor script with one built on `cudl_normalize`, `fist_to_cudl_keys`, numeric-token overlap, and emitted InventoryId/SignatureId/title/manifest URL fixtures.
- In 86-03, do not ask the user to write regex code. Present concrete proposed rules with examples and false-positive risks; ask Accept/Reject/More samples.
- Add a mandatory 86-04 preflight: run generation in dry-run/count-only mode, assert expected bounds, verify T-S NS 329.96, verify zero parent shadows, then `--apply`.
- Use all 461 v7.9.4 sys_ids in `tests/fixtures/nli_oxford_flipped_sysids.txt`; it is small and materially stronger.
- Before `export_fist_enrichment.py`, create timestamped backups of `fist_data/fjms_enrichment.db` and `fist_data/synthetic_manifest.json`.

### Risk Assessment

Overall risk: **HIGH if executed unchanged; MEDIUM after the above fixes**. The strategic design avoids the Phase 85 bib-only trap, but the current plans have enough integration and audit-count defects that UAT could again expose a mismatch between claimed coverage and actual app behavior. The biggest risk is not synthetic row content this time; it is proving coverage with the wrong scanner and losing ambiguity/category information needed to explain the result.

---

## Consensus Summary

Both reviewers concur: **HIGH overall risk if executed unchanged.** The architectural inversion is sound — both call it correct strategy. The blockers are concrete and fixable, but they must be patched before `/gsd-execute-phase 86`.

### Agreed Strengths (both reviewers cite)

- **Sibling-module bridge pattern (86-01)** — both praise the decision to create `shared/fist_cudl_bridge.py` rather than mutate Phase 84's `shared/shelfmark_bridge.py`. Preserves NORM-04 frozen contract.
- **CUDL-walk inversion (86-02)** — both agree this is the right strategic pivot. Image-bearing-only invariant + CUDL as primary key dodges Phase 85's multi_signature fan-out.
- **Operational sequencing (86-04)** — both agree the generate → export_fist_enrichment → scan order is correct.
- **AUDIT-03 hybrid (86-04)** — golden-row fixture + regex scan-sweep is a sound regression-guard pattern.
- **Plan 03 framing (86-03)** — both agree the 1,599 residue requires human adjudication, not blind normalization.

### Agreed Concerns (both reviewers cite — HIGHEST PRIORITY for revision)

1. **HIGH — Mosseri concat-form missing in 86-01.** Both reviewers cite this directly. Pitfall 1 in RESEARCH.md / CONTEXT.md says Mosseri Roman must emit BOTH `mosseriiii27.1` AND `mosseriiii271`. Plan 01's `fist_to_cudl_keys` only emits the with-dot form. Will under-recover ~3K Mosseri sub-fragments — the bulk of the win.

2. **HIGH — Plan 03 nearest-neighbor artifact too weak.** Gemini: the 5-char prefix LIKE won't match across Mosseri/T-S. Codex: the artifact is missing manifest URLs, InventoryId/SignatureId, catalog title/description, image links — all needed for the user to actually adjudicate. Both reviewers agree the proposed script will produce a useless artifact.

### Codex-Unique Concerns (HIGH severity — must address)

3. **HIGH — AUDIT-01 audit math is inconsistent.** The expected drop from 6,053 → ~1,599 orphans comes from ~3,455 alias-only fixes (CUDL classmarks resolving to existing Alma rows under different shelfmark forms) PLUS ~108 no-Alma synthetic rows. Plan 02 only emits the 108. Without a scanner that classifies Phase 86 FIST-bridge hits separately, `scan_cudl_orphans.py` unchanged will NOT show the expected coverage delta. **AUDIT-01 cannot deliver criterion 1 with the current scope.**

4. **HIGH — `scan_cudl_orphans.py` is not bridge-aware.** Current scanner uses `row[3] == "CUL"` only and does not call `lookup_cudl` or the new FIST bridge. It will miss Mosseri synthetics and many Phase 84/86 alias forms. Plan 04 cannot rely on it unchanged.

5. **HIGH — Plan 03 depends on `--dry-run` writing residue CSV but the existing generator does not.** Current `main()` returns before `_write_manifest` / `_write_residue` / `_write_coverage` in dry-run. Plan 03 Task 1's first step (reading `reports/synthetic_ambiguity_residue.csv`) will read stale or absent data.

6. **HIGH — Plan 02 collapses two ambiguity kinds into one.** `lookup_fist_by_cudl()` returns `None` for both "not found" AND "multi_inventory ambiguous". Plan 04's per-category breakdown becomes unrecoverable.

7. **HIGH — Plan 02 has-Alma skip may hide a product gap.** CUDL classmarks resolving to alias-only existing Alma rows — are they actually searchable in the apps, or only "covered for audit"? Phase 84 alias index may or may not include the new FIST↔CUDL keys. Needs explicit framing.

### Gemini-Unique Concerns (HIGH severity — must address)

8. **HIGH — Plan 02 drops `dbo_UnitCatalogRec` query — synthetic rows lose FJMS titles.** `InventoryRecord` doesn't carry `title_heb`/`genizah_title`; generation hardcodes None. The synthetic rows will render with shelfmark-only titles, discarding all available scholarly metadata.

### Agreed MEDIUM Concerns

- Plan 01 `(N)` series-strip is under-gated (Codex; Gemini implicit) — would create false aliases outside T-S F/T-S Ar without prefix-gating or negative tests.
- Plan 02 tests will fail as written — `_make_nli_seed` schema mismatch + monkeypatch on wrong module path (Codex specific, but Gemini's vestigial-test concern overlaps).
- AUDIT-03 fixture scope (20 vs 461) — Codex says use all 461. The 461-row fixture is cheap; the 20-row fixture is materially weaker.
- Plan 02 `_classify_library_code` too narrow — `Mosseri:` prefix forms classified as CUL.
- Plan 04 rollback incomplete — `synthetic_manifest.json` needs backup too; gz backup of `fjms_enrichment.db` not explicitly created/verified pre-export.

### Divergent Views

- **Title preservation framing:** Gemini frames missing FJMS titles as the primary HIGH-severity defect (data regression — synthetic rows are useless without titles). Codex doesn't flag this directly. *Resolution: Gemini is right — review the data flow and patch.*
- **Audit-math framing:** Codex frames the scanner-not-bridge-aware as the primary HIGH-severity defect (coverage claims can't be proven). Gemini doesn't flag this. *Resolution: Codex is right — review whether `scan_cudl_orphans.py` needs upgrading or whether a new Phase-86-aware scanner is in scope.*
- **Plan 05 release strategy:** Gemini praises Plan 05's release restraint (defer desktop). Codex questions the web-only version bump. *Resolution: planner's CHECKPOINT already covers both paths; user picks.*

### Recommendation

**Do NOT run `/gsd-execute-phase 86` against the current plans.** Run `/gsd-plan-phase 86 --reviews` to fold these findings into a revised plan set. Priority order for the revision:

1. **Patch 86-01 Mosseri concat-form** (both reviewers, easy fix — emit both `{rest_norm}` and `{rest_norm.replace('.', '')}` variants).
2. **Decide AUDIT-01 scope** (Codex) — either upgrade `scan_cudl_orphans.py` to be Phase-86-bridge-aware, OR add a new Phase-86 coverage scanner that classifies hits by bridge tier. The current plans cannot deliver criterion 1 otherwise.
3. **Restructure `lookup_fist_by_cudl` return type** (Codex) — make it a structured result with `status` ∈ {not_found, single, multi_inventory_ambiguous} + entries list. Plan 02 uses status to populate the residue CSV's `pattern_guess` / `ambiguity_kind` columns properly.
4. **Add `title_heb`/`genizah_title` to `InventoryRecord`** (Gemini) — LEFT JOIN `dbo_UnitCatalogRec` in `build_fist_alias_index`. Plan 02 reads these and injects into synthetic rows.
5. **Rebuild 86-03 nearest-neighbor artifact script** (both) — use `cudl_normalize` + `fist_to_cudl_keys` + numeric-token overlap; include manifest URLs + InventoryId/SignatureId + title snippets + image URLs. Don't ask the user to write regex — present concrete rule proposals with Accept/Reject/More-samples.
6. **Fix Plan 02 dry-run residue writing** (Codex) — either `--dry-run` writes to dry-run paths, or 86-03 has a dedicated residue-builder command.
7. **Gate `(N)` strip to T-S F/T-S Ar prefixes** (Codex) or add negative-fixture tests.
8. **Widen `_classify_library_code` Mosseri detection** to handle `Mosseri:` prefix forms (Codex).
9. **Repair 86-02 tests** — fix `_make_nli_seed` schema + monkeypatch path (Codex).
10. **Expand AUDIT-03 fixture to all 461 sys_ids** (Codex) — cheap, materially stronger.
11. **Add 86-04 preflight** (Codex) — dry-run/count-only mode assertion before `--apply`, plus timestamped backups of `fjms_enrichment.db` and `synthetic_manifest.json`.
12. **Confirm has-Alma skip product framing** (Codex) — are alias-only Alma rows searchable, or audit-only?

After revision, both reviewers indicated risk drops from HIGH to MEDIUM. The strategic design (CUDL-walked inversion, sibling bridge, image-bearing-only, operational sequencing, hybrid AUDIT-03) is correct and stays.
