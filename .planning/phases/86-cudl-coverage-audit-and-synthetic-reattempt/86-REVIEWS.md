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
overall_risk: MEDIUM-to-HIGH (reviewers diverge — see Divergent Views below)
prior_revision: superseded by this run; first review at f7bb5658 folded 8 HIGH+MEDIUM concerns; this is the second pass on the replanned set.
---

# Cross-AI Plan Review — Phase 86 (Pass 2)

> Two independent senior-engineer reviews from `gemini` and `codex` CLIs.
> Claude CLI skipped (we are Claude — running this review session inside Claude Code).
> Both reviewers were given the full PROJECT.md (first 80 lines), ROADMAP phase 86 section,
> REQUIREMENTS.md, 86-CONTEXT.md, 86-RESEARCH.md, and all 5 PLAN.md files (384 KB prompt total).
> This is the second cross-AI pass after commit `f7bb5658` folded the first round's feedback.

---

## Gemini Review

### 1. Summary

Phase 86 is an exceptionally well-designed recovery phase. By switching the anchor of the generation process to `cambridge_manifests` and implementing a bidirectional bridge, it fundamentally solves the `multi_signature` fan-out and "useless bibliography-only rows" issues that led to the Phase 85 data revert. The plans exhibit high technical maturity, particularly in how they address previous AI review concerns regarding metadata preservation (titles), audit math (bridge-aware scanner), and operational safety (backups/preflights).

### 2. Strengths

- **Architectural Inversion (Plan 02):** Using CUDL manifests as the primary key for generation ensures that every synthetic row is image-bearing by construction, directly satisfying the user's "research-actionable" requirement.
- **Audit Integrity (Plan 04):** The creation of a **new bridge-aware scanner** (`scripts/scan_cudl_coverage_phase86.py`) is a vital addition. It prevents the "audit math inconsistency" that would have occurred if the legacy scanner were used to prove the success of a bridge-aware generation.
- **Sibling Module Pattern (Plan 01):** Creating `shared/fist_cudl_bridge.py` instead of mutating the Phase 84 bridge is an excellent application of the "Open-Closed Principle," preserving the NORM-04 frozen contract for existing resolution paths.
- **Robust Operational Sequencing (Plan 04):** The pipeline (Backup → Preflight → Apply → Export → Double Scan) ensures that the `fjms_enrichment.db` sidecar is correctly backfilled with synthetic AlmaIds before UAT begins, closing a major operational gap from the previous phase.
- **Hybrid Regression Guard (Plan 04):** The `AUDIT-03` implementation (461-row golden fixture + regex scan-sweep) provides a permanent, CI-integrated safety net for the v7.9.4 NLI fix.

### 3. Concerns

- **MEDIUM (Plan 86-03): Nearest-Neighbor Quality.** The `build_residue_patterns_artifact.py` script uses a bounded `LIKE` query and numeric-token overlap ranking. While much improved, if a CUDL classmark has very common numeric tokens (e.g., `or1080.1`), the 500-candidate limit might still miss the "true" partner if it ranks low on overlap. However, given the prefix-family gating, this risk is significantly mitigated.
- **LOW (Plan 86-01): (N) Strip Gating.** The `_SERIES_N_FAMILY_PREFIXES` are hardcoded to `t-s f` and `t-s ar`. If other CUDL series (like `Add.`) start using parenthetical series digits in the future, the bridge will miss them. This is acceptable for the current corpus but creates a small maintenance tail.
- **LOW (Plan 86-05): Version Mismatch.** The "web-only" version bump creates a state where the source code claims `v7.11.0` but the latest desktop installer is `v7.10.x`. The plan correctly identifies this and recommends `defer` as the default, which is the correct engineering trade-off.

### 4. Suggestions

- **Plan 03 (Human Adjudication):** To further improve the adjudication experience, consider having `build_residue_patterns_artifact.py` generate a direct link to the CUDL viewer (e.g., `https://cudl.lib.cam.ac.uk/view/{cudl_label}/1`) in the markdown table, as manifest URLs are often raw JSON that requires a viewer to be useful.
- **Plan 04 (Preflight):** During the preflight assertion, consider verifying not just the *count* of qualifying rows, but also that at least one "Tier 1" row (CUDL + FJMS metadata) is present, to ensure the title propagation logic is working on real data before the final `--apply`.

### 5. Risk Assessment

**Overall Risk: LOW/MEDIUM**

The primary technical risks (ambiguity and bib-only data) have been successfully engineered out through the CUDL-walk inversion and the image-bearing-only invariant. The residual risk is primarily operational (executing the 10+ step sequence in Plan 04) and qualitative (human adjudication of the 1,599 residue). The inclusion of explicit rollback paths and a non-decreasing row-count smoke check for FJMS data provides a strong safety net. **The plans are ready for execution.**

---

## Codex Review

### Summary

The phase is well-scoped around the right correction: Phase 86 reverses the walk to CUDL-first, requires image-bearing CUDL manifests, preserves the Phase 85 infrastructure, and adds audit artifacts instead of trying to hide the remaining residue. The plan set is much stronger than Phase 85's failed inclusive predicate. That said, **I would not execute it unchanged**: there are several high-risk implementation details that could misclassify audit results, break idempotent regeneration, or mask production SQL bugs with overly simplified tests.

### Strengths

- CUDL-walked generation directly addresses Phase 85's bib-only failure mode.
- Image-bearing-only inclusion is clear and testable.
- Parent-shadow filtering is retained as cheap insurance.
- `explain_fist_by_cudl` is the right API shape; separating `not_found` from `multi_inventory_ambiguous` is important for honest audit reporting.
- T-S NS 329.96 is explicitly fixture-tested end-to-end.
- Plan 03 correctly treats residue normalization as human adjudication, not blind fuzzy matching.
- AUDIT-03 adds both an operational scan and a permanent CI fixture.
- Plan 05's default recommendation to defer version/release metadata is correct for a web-only data refresh.

### Concerns

- **HIGH — 86-02 / 86-04: idempotency risk from `lookup_cudl` after synthetics exist.** If Phase 84's alias index is built from `libraries.csv` including the synthetic block, rerunning `--apply` can cause existing synthetics to be seen as "already covered," then omitted from `qualifying`, potentially wiping the block. The bridge-aware scanner can also misclassify synthetic rows as `phase84_hit`.

- **HIGH — 86-01: proposed FIST title SQL likely uses the wrong join path.** Existing export code joins `dbo_InventorySignature -> dbo_Signature -> dbo_UnitCatalogRec`. Plan 86-01's SQL joins `dbo_UnitCatalogRec.SignatureId = dbo_InventorySignature.SetSignatureId`, which the current codebase suggests is wrong. The in-memory tests mirror the wrong shortcut, so they would pass while production metadata fails.

- **HIGH — 86-04: audit tier semantics overstate "coverage" for alias-only Alma hits.** If a CUDL classmark only resolves through the new FIST bridge but not through the runtime Phase 84 alias/search path, counting it as "coverage achieved" is misleading. It proves a real Alma row exists, but not that a user searching the CUDL form lands on it.

- **HIGH — 86-04: preflight guard is too loose and does not prove T-S NS 329.96 qualifies.** The bound `100 <= qualifying <= 5000` is too close to the reverted 5,035-row failure mode, and the proposed T-S NS check only verifies that 65549106 is not in residue. It does not assert it is in `qualifying` or in the synthetic manifest.

- **HIGH — 86-03: accepted-rule implementation direction is underspecified.** The proposed rules are often expressed as CUDL regexes, but `fist_to_cudl_keys()` emits CUDL keys from FIST shelfmarks. Each accepted rule needs an explicit FIST→CUDL implementation and tests against real FIST-form inputs.

- **MEDIUM — 86-03: nearest-neighbour script claims bridge-aware matching but the draft mostly uses prefix + numeric-token overlap.** It imports `fist_to_cudl_keys` / `cudl_normalize` but does not meaningfully use them in the candidate ranking shown. The `LIMIT 500` before ranking may miss relevant candidates in large prefix families.

- **MEDIUM — 86-04: FJMS smoke-check table list is inaccurate.** The repo's existing 12 AlmaId-keyed tables are `domains`, `joins`, `catalog`, `catalog_running_titles`, `catalog_sizes`, `catalog_fields`, `catalog_free_desc`, `catalog_full_texts`, `catalog_textual_frames`, `catalog_mentions`, `bibliography`, `catalog_refs`. The plan lists several stale/nonexistent names.

- **MEDIUM — 86-02: `_write_residue` signature is inconsistent.** Existing code is path-first. The plan describes changing to optional path but later calls `_write_residue(residue, path=...)`. Pick one signature and update all callers/tests.

- **MEDIUM — 86-04: 461-row NLI fixture generation is not guaranteed to be the actual flipped set.** Extracting "current NLI rows matching NLI_RE" and taking the first 461 can include unrelated rows or omit true flipped rows. Use a frozen fixture derived from the original v7.9.4 diff/fix output, or rename the test to reflect what it actually proves.

- **MEDIUM — 86-04: shell commands are not Windows/PowerShell-safe.** The plans use `cp`, `gzip`, `tee`, `wc`. This project context is PowerShell on Windows. Use Python scripts or PowerShell-native commands for reproducibility.

- **MEDIUM — 86-04: rollback is defined but not tested.** Backups are good, but there should be at least a validation that the gz backup decompresses and the manifest backup parses. Prefer storing large backups outside git-tracked paths unless intentionally ignored.

- **LOW — 86-01 / 86-02: several acceptance criteria are brittle grep/line-count checks.** They are useful smoke checks, but they should not substitute for behavior tests, especially around SQL schema and idempotency.

### Suggestions

- Build the Phase 84 `lookup_cudl` index from real rows only: strip the synthetic block before building, or have the alias builder skip `is_synthetic_sys_id(sys_id)`.
- Add an idempotency test: run generation once, simulate a populated synthetic block, run again, and assert the same synthetic rows remain.
- Fix `build_fist_alias_index` SQL to join through `dbo_Signature`, and update tests to use the production-like schema.
- Make `--dry-run` optionally write a small qualifying manifest/CSV, then assert `65549106` is present and `990065549106000000` would be emitted.
- Tighten preflight bounds to the expected synthetic count, not total FIST bridge recoveries. A range like `50..1000` is safer unless Plan 03 accepts many additional no-Alma rules.
- In `scan_cudl_coverage_phase86.py`, classify synthetic `lookup_cudl` hits as `phase86_synthetic` using `is_synthetic_sys_id`, not `phase84_hit`.
- Rename alias-only Alma tier to something like `phase86_existing_alma_candidate`, unless runtime search/browse resolution is actually implemented.
- For Plan 03, define a stop rule: one artifact generation + one user adjudication pass; `Spot-check more` becomes deferred unless the user explicitly requests another iteration.
- Use the exact 12 AlmaId-keyed table list from `scripts/export_fist_enrichment.py` / `tests/test_export_fist_synthetic.py`.
- Keep large rollback backups out of committed artifacts; document paths and verify restorability instead.
- Derive AUDIT-03's 461 sys_ids from a stable historical source, not from current mutable `libraries.csv`.

### Risk Assessment

**Overall risk: MEDIUM-HIGH as written.** The conceptual design is sound and directly addresses Phase 85's root causes, but the execution plans contain a few data-plane hazards: idempotency after synthetic rows exist, audit misclassification, a likely production SQL join bug, and loose preflight bounds. After fixing those, the phase drops to **MEDIUM** risk, mainly because it mutates large data artifacts and depends on human adjudication for optional residue recovery. The `<200 truly-orphan` target is not realistic as a hard gate; the plans correctly reframe it as conditional, but `cudl_coverage.md` must be explicit that the real deliverable is categorized, defensible residue, not forced compliance with an obsolete numeric target.

---

## Consensus Summary

### Agreed Strengths (both reviewers)

- **CUDL-walked inversion** is the right architectural pivot — directly resolves Phase 85's bib-only failure mode without re-litigating the multi_signature ambiguity.
- **Image-bearing-only invariant** is clear, testable, and non-negotiable. Every emitted row has a CUDL manifest by construction.
- **Parent-shadow filter (D-06)** kept as cheap insurance against the Phase 85 `T-S NS 161` shadow case.
- **AUDIT-03 hybrid approach** (operational scan-script + permanent 461-row golden fixture) gives both phase-time evidence and durable CI protection.
- **T-S NS 329.96 closure** is explicitly fixture-tested end-to-end, holding the originating user case as acceptance.
- **Plan 05 defer-version default** correctly honors `feedback_no_github_release_for_web_only.md`.

### Agreed Concerns (raised by 2+ reviewers — highest priority)

1. **Residue-pattern script quality (Plan 86-03).** Both flag that the nearest-neighbour ranker is prefix + numeric-token overlap with a `LIMIT 500` cutoff — Gemini calls this MEDIUM-mitigated by prefix gating; Codex calls it MEDIUM with the additional note that the script *imports* `fist_to_cudl_keys` / `cudl_normalize` but does not meaningfully use them in candidate ranking, despite being framed as bridge-aware.
2. **Plan 04 preflight is too loose.** Gemini wants Tier-1-row presence verification beyond the row-count check; Codex wants tighter bounds (50..1000 vs 100..5000) AND a positive assertion that 65549106 is in `qualifying`, not just "not in residue."

### Divergent Views (worth investigating before execution)

| Topic | Gemini | Codex |
|-------|--------|-------|
| **Overall risk** | LOW/MEDIUM — "plans are ready for execution" | MEDIUM-HIGH — "would not execute unchanged" |
| **Idempotency on `--apply` re-run** | Not raised | **HIGH** — synthetic block in `libraries.csv` could be seen as "already covered" by Phase 84's alias index, then wiped on rerun. Recommends stripping synthetics before alias-index build, or skipping `is_synthetic_sys_id(sys_id)` in the builder. |
| **FIST title-extraction SQL** | Not raised | **HIGH** — proposed join `dbo_UnitCatalogRec.SignatureId = dbo_InventorySignature.SetSignatureId` likely wrong; production code joins through `dbo_Signature`. In-memory test schema mirrors the wrong shortcut, so tests would pass while real data fails. |
| **Audit tier semantics** | Audit math now consistent thanks to new scanner | **HIGH** — counting alias-only Alma hits as "coverage achieved" overstates reality. Recommends renaming the tier to `phase86_existing_alma_candidate` unless runtime resolution actually catches them, and ensuring synthetic `lookup_cudl` hits are classified as `phase86_synthetic`, not `phase84_hit`. |
| **Accepted-rule integration direction (Plan 86-03)** | Not raised | **HIGH** — accepted rules are expressed as CUDL regexes but `fist_to_cudl_keys()` operates FIST→CUDL. Each rule needs explicit FIST→CUDL implementation and tests against real FIST inputs. |
| **FJMS smoke-check table list** | Not raised | **MEDIUM** — names listed in Plan 04 don't match the repo's actual 12 AlmaId-keyed tables (`domains`, `joins`, `catalog`, `catalog_running_titles`, `catalog_sizes`, `catalog_fields`, `catalog_free_desc`, `catalog_full_texts`, `catalog_textual_frames`, `catalog_mentions`, `bibliography`, `catalog_refs`). Will silently no-op or fail at runtime. |
| **Cross-platform shell commands** | Not raised | **MEDIUM** — `cp`/`gzip`/`tee`/`wc` won't run under Windows PowerShell. Prefer Python scripts or PowerShell-native equivalents. |
| **AUDIT-03 fixture derivation** | Endorsed as hybrid regression guard | **MEDIUM** — "first 461 NLI-matching rows from current libraries.csv" is not guaranteed to be the actual v7.9.4-flipped set; mutable source. Use a frozen historical fixture or rename the test to match what it actually proves. |
| **Rollback validation** | Endorsed (backups present) | **MEDIUM** — rollback path defined but never exercised. Recommends at minimum verifying gz decompresses and manifest backup parses. |

### Why the Divergence?

Codex's stderr shows it spent the review session actively exploring the live codebase (`dbo_*` schema, existing 12-table list, `is_synthetic_sys_id` callers, etc.); Gemini's review reads as a higher-level structural assessment. Both reads are legitimate but they're answering slightly different questions:

- **Gemini:** "Does this plan set address Phase 85's root causes?" → Yes, with low residual risk.
- **Codex:** "Is this plan set executable as written?" → Not safely; several integration-level details would break on first contact with the real codebase.

### Recommended Next Action

The structural design is sound — both reviewers agree the CUDL-walk inversion and image-bearing-only constraint were the right calls. But Codex's 5 HIGH-severity execution concerns deserve resolution before Wave 0 of execute-phase, especially:

1. **Idempotency on re-apply** (block-wipe risk if alias index includes synthetics).
2. **FIST title SQL join path** (would silently produce empty titles for all synthetics).
3. **Audit tier naming** (avoids "coverage achieved" overstatement).
4. **Plan 03 rule direction** (FIST→CUDL, not CUDL regex).
5. **Plan 04 preflight tightening** (positive assertion on 65549106, tighter bounds, table-list accuracy, PowerShell-safe commands).

Suggested workflow:

```
/gsd-plan-phase 86 --reviews     # fold the 5 HIGH + 6 MEDIUM concerns into a v3 plan set
```

After replan, the phase should drop to LOW-MEDIUM risk and be safe for `/gsd-execute-phase 86`.
