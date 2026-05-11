# CUDL Coverage Report (Phase 86)

**Generated:** 2026-05-11
**Source data:** nli_crossref.db.cambridge_manifests (141,368 CUDL classmarks),
FIST.db.dbo_Inventory (~279K inventories), libraries.csv (real + Phase 86 synthetic).
**Primary scanner:** `scripts/scan_cudl_coverage_phase86.py` (bridge-aware, Pass 2 HIGH-1 + HIGH-3 renamed tiers).
**Legacy baseline:** `scripts/scan_cudl_orphans.py --out-suffix _post_phase86` (byte-stable; reported as legacy-orphan baseline only — Pass 3 HIGH-2 rename).
**Orchestrator:** `scripts/phase86_apply.py --apply` (Pass 2 MEDIUM-5 single PowerShell-safe entry point).

Phase 86 inverts the bridge direction relative to Phase 85: walks the CUDL
classmark universe, filters out classmarks already resolved by Phase 84's
bridge against REAL libraries.csv rows (Pass 2 HIGH-1 idempotency), resolves
the remainder via the new `shared/fist_cudl_bridge.py` sibling module, and
emits synthetic libraries.csv rows ONLY for image-bearing-by-construction
CUDL classmarks (D-01a invariant).

## Summary

Counts derived from `reports/cudl_coverage_post_phase86.csv` (5-tier bridge-aware scan).

| Tier | Count | % of CUDL total | Semantics |
| ---- | ----- | --------------- | --------- |
| `phase84_hit` | 136,038 | 96.23% | Classmark resolved via libraries.csv alias index to a REAL Alma row (Phase 84 NORM-01..04) |
| `phase86_synthetic` | 108 | 0.08% | NEW synthetic libraries.csv row added by Phase 86 for image-bearing CUDL classmark with no Alma row in FIST (sys_id passes `is_synthetic_sys_id`) |
| `phase86_existing_alma_candidate` | 3,375 | 2.39% | **Documented candidate — NOT counted as resolution.** FIST bridge resolves this CUDL classmark to an existing-Alma libraries.csv row, BUT the user-typed CUDL form does NOT reach that row through the app's shelfmark search at runtime (depends on Phase 84 alias coverage — a separate concern, NOT extended in Phase 86). (Pass 2 HIGH-3 renamed tier — was conflated with "coverage achieved".) |
| `multi_inventory_ambiguous` | 248 | 0.18% | Excluded — D-04a; same CUDL key resolves to 2+ distinct FIST InventoryIds |
| `phase86_residue` | 1,599 | 1.13% | Residue — no FIST candidate via Phase 86 bridge (D-02c human-adjudication candidates). (Pass 2 HIGH-3 + Pass 3 HIGH-2 renamed from the legacy scanner's tier-equivalent; this report's prose uses `legacy_orphan` for the legacy-scanner number while the legacy CSV column names stay byte-stable.) |

**Legacy scanner baseline** (`scripts/scan_cudl_orphans.py --out-suffix _post_phase86`):
5,957 classmarks classified as legacy-orphan by the byte-stable scanner
(no Phase 86 bridge awareness — uses `row[3]='CUL'` filter on libraries.csv only). This
number is INTENTIONALLY higher than the bridge-aware `phase86_residue` count above,
because the legacy scanner does not credit Phase-86 alias-only-Alma candidates OR
Phase-86 synthetic rows OR Phase-84 alias hits.

(Pass 3 HIGH-2 rename note: this report uses `legacy_orphan` for the legacy
scanner's tier-equivalent number to avoid the connotation that the
classmarks are irrecoverable. The legacy scanner CSVs themselves keep their
original byte-stable column names per Pass 1 HIGH #4.)

Baseline (post-Phase-84): 6,053 legacy orphans. Post-Phase-86 (bridge-aware `phase86_residue`):
1,599. The 96-row delta between post-Phase-84 legacy and post-Phase-86 legacy
(6,053 → 5,957) reflects the legacy scanner picking up some of the 108 newly
synthesized CUL rows but not all of them, because the legacy scanner walks
`cudl_normalize(call_numbers)` and a few synthetic shelfmark variants don't
round-trip through `cudl_normalize` to the exact CUDL form.

## Per-Collection Breakdown

Per-classmark tier classification grouped by FIST library_code or by classmark prefix
when `fist_inventory_id` is empty.

| Collection | `phase84_hit` | `phase86_synthetic` | `phase86_existing_alma_candidate` | `multi_inventory_ambiguous` | `phase86_residue` |
| ---------- | ------------- | ------------------- | --------------------------------- | --------------------------- | ----------------- |
| T-S (other) | 81,562 | 67 | 2 | 1 | 8 |
| T-S NS | 39,975 | 27 | 11 | 1 | 179 |
| T-S Ar | 7,406 | 1 | 7 | 98 | 303 |
| T-S Misc | 4,424 | 1 | 5 | 0 | 98 |
| T-S F | 1,232 | 2 | 95 | 49 | 392 |
| Mosseri | 778 | 7 | 2,957 | 93 | 48 |
| Or. | 548 | 3 | 298 | 6 | 571 |
| Add. | 113 | 0 | 0 | 0 | 0 |
| **TOTAL** | **136,038** | **108** | **3,375** | **248** | **1,599** |

Numbers derived from `reports/cudl_coverage_post_phase86.csv` by grouping
classmark prefix → library_code (for `phase86_residue` rows where
`fist_inventory_id` is empty) or by FIST library lookup (for the other tiers).
The `Mosseri` row's large `phase86_existing_alma_candidate` count (2,957) is
the dominant signal that Phase 84's alias index does not currently include the
forward-label `MS-MOSSERI-*` form used by CUDL — those Mosseri rows DO exist
in libraries.csv (as Alma rows), but `lookup_cudl(MS-MOSSERI-*)` fails to
resolve them at runtime through the user-typed CUDL form. Extending Phase 84's
alias index to cover these is a future Phase 87 concern (see "Key carry-forward
finding" in `86-03-SUMMARY.md`).

## Residue Pattern Adjudication (D-02c outcomes)

Per Plan 03's `86-RESIDUE-PATTERNS.md` adjudication artifact, **all 6 pattern
families were REJECTED** in the 2026-05-11 user-adjudication checkpoint.
Total accepted rules: **0**.

| Pattern Family | Decision | Rule Citation / Rationale |
| -------------- | -------- | ------------------------- |
| T-S F flattened-series | **Rejected** | Proposed CUDL keys (`tsf1.1100`, `tsf2.250`, etc.) don't appear in residue; score-102 matches in artifact's sample table come from EXISTING D-02a Pattern 3 (N)-strip rule, not from the proposed flattened-series rule. Residue `tsf1.11` rows are `multi_inventory_ambiguous` because the existing (N)-strip rule maps both `T-S F1(1).11` and `T-S F1(2).11` to `tsf1.11`. |
| T-S Ar flattened-series | **Rejected** | Same shape as T-S F: proposed keys (`tsar18.234`) don't appear in residue; score-102 matches are from existing (N)-strip rule. |
| T-S NS minute-fragments + letter-suffix | **Rejected** | `cudl_normalize('T-S NS X.minute fragments')` already produces `tsnsXminutefragments` — rule is redundant with existing behavior. Residue suggests a separate alias-index investigation worth carry-forward, not a new normalizer rule. |
| Or. single-segment ambiguity | **Rejected** | HIGH RISK confirmed via FIST.db probing: `Or.1080 11.45`, `Or.1080 5.17`, `Or.1080 6.11`, `Or.1080 B14.1` show sub-fragment digits are REAL physical divisions. Collapsing `Or.1080 11.1` → CUDL `or1080.11` would conflate distinct manuscripts. |
| Mosseri exotic letter suffixes | **Rejected** | FIST has ZERO `Moss.{ROMAN},{N}{lowercase letter}` shelfmarks. All Mosseri letter-suffixes use UPPERCASE A: `Moss. I,53A`, `Moss. III,133A`, `Moss. I,118.1A`. Proposed regex would never match real FIST data. |
| T-S Misc multi-segment patterns | **Rejected** | FIST uses `T-S Misc.X.Y(Z)` (parens for sub-fragment) for residue cases, not `T-S Misc X.Y.Z`. Proposed regex matches existing canonical 3-segment forms that already normalize correctly — so the rule is either redundant or wrong-direction. |

**Key carry-forward finding (cited from 86-03-SUMMARY.md):** The 1,599 residue
is dominated by EXISTING-rule over-aggressiveness, not missing normalizer
rules. Three patterns documented for a future "Phase 87 — Bridge rule
disambiguation" plan:

1. **D-02a Pattern 3 ((N)-strip) conflations** — `T-S F1(1).N` and `T-S F1(2).N`
   both produce `tsf1.N`, causing `multi_inventory_ambiguous` for CUDL `tsf1.N`.
   Disambiguation idea: preserve (N) suffix for T-S F / T-S Ar families.
2. **Mosseri concat-form spurious collisions** — `Moss. I,5.1` produces
   concat-form alias `mosserii51` which collides with canonical `Moss. I,51`.
   Disambiguation idea: gate concat-form alias to fragments without internal
   sub-segments.
3. **AIU-preliminary-handlist duplicates** — `AIU: Mosseri: Moss. I,26.1` and
   canonical `Moss. I,26.1` produce identical aliases → `multi_inventory_ambiguous`.
   Disambiguation idea: prefer AlmaId-bearing inventory or strip AIU-prefix
   preliminary-handlist entries.

See `.planning/phases/86-cudl-coverage-audit-and-synthetic-reattempt/86-RESIDUE-PATTERNS.md`
for full fixture details, supporting/refuting FIST.Shelfmark examples (Pass 2 HIGH-5
FIST→CUDL direction), false-positive risk notes, and the adjudication audit trail.

## Re-run Instructions

To re-run the Phase 86 coverage audit after future data refreshes, use the
PowerShell-safe Python orchestrator (Pass 2 MEDIUM-5):

```bash
python scripts/phase86_apply.py --apply
```

This orchestrates: Step 0 backups (_tmp/phase86_backups/, Pass 2 MEDIUM-6) →
0.5 preflight (qualifying ∈ [50, 2000], 65549106 not in residue, Tier-1 present;
Pass 2 HIGH-4) → 0.6 rollback validation (gz magic + JSON parse; Pass 2 MEDIUM-6)
→ 1 `--apply` → 1.5 post-apply 65549106 + 990065549106000000 assertion (Pass 2
HIGH-4) → 2 `export_fist_enrichment.py` → 3 legacy scan (`scan_cudl_orphans.py
--out-suffix _post_phase86`) → 4 bridge-aware scan (`scan_cudl_coverage_phase86.py`)
→ 5 CRLF check → 6 FJMS 12-table smoke (Pass 2 MEDIUM-2) → 7 audit_nli_attribution.py
→ 8 pytest tests/test_nli_oxford_attribution.py.

Dry-run-only (preflight without mutation):

```bash
python scripts/phase86_apply.py --dry-run
```

After the orchestrator completes, update this report manually with the new
counts (no auto-generator).

## See Also

- `reports/synthetic_coverage.md` — Phase 85 tier breakdown (cross-link; historical audit)
- `.planning/phases/86-cudl-coverage-audit-and-synthetic-reattempt/86-RESIDUE-PATTERNS.md` — D-02c human-in-loop adjudication artifact (Pass 2 HIGH-5 FIST→CUDL direction)
- `.planning/phases/86-cudl-coverage-audit-and-synthetic-reattempt/86-RESEARCH.md` — full methodology
- `.planning/phases/85-synthetic-fjms-inventory-rows/85-VERIFICATION.md` — Phase 85 revert rationale
- `reports/cudl_coverage_post_phase86.csv` — per-classmark 5-tier classification

## Roadmap Criterion 4 Waiver

ROADMAP success criterion 4 reads "Both apps build and pass test suite
green" — Phase 86 is a data-only refresh (`libraries.csv`,
`fist_data/synthetic_manifest.json`, `fist_data/fjms_enrichment.db`) with
zero desktop code changes. Per the project rule documented in
`feedback_no_github_release_for_web_only.md`, rebuilding the desktop
installer for a pure data refresh is avoided because every desktop user
polls `/releases/latest` (see `gui_threads.py:459`) and would be prompted
to download a no-installer page. The interpretation adopted for Phase 86
is therefore **web deploy now; desktop data bundled** into the next
desktop-code release — `libraries.csv` and `fjms_enrichment.db` flow
through the desktop installer's data-file pipeline automatically the next
time it is rebuilt for any reason, so Phase 86's data ships to desktop
users at that point without a Phase-86-only installer build. Wording is
kept verbatim with the Task-4 86-04-SUMMARY.md waiver section so both
templates carry the same canonical text (T-86-04-17).

Cross-references for audit:

- `.planning/phases/86-cudl-coverage-audit-and-synthetic-reattempt/86-04-SUMMARY.md` — Task 4 carries the equivalent waiver section
- `feedback_no_github_release_for_web_only.md` — controlling project rule (cited verbatim)
- `.planning/ROADMAP.md` — Phase 86 success criterion 4 (the criterion this waiver interprets)

## AUDIT-01 Note on the "<200 legacy-orphan" Target

The ROADMAP success criterion 1 set "<200 legacy-orphan" when only the post-Phase-84
6,053 legacy-orphan count was known. Empirical research (`86-RESEARCH.md`) confirmed
the residue is structurally ~1,599 across 5 known pattern families. Phase 86 closes
the gap to the extent D-02c adjudication accepts pattern rules; the actual <200
legacy-orphan target is conditional on those adjudication outcomes (Residue
Pattern Adjudication section above). **All 6 families were REJECTED in the
2026-05-11 adjudication**, so the 1,599 `phase86_residue` count remains as the
structural floor. This report documents the true post-Phase-86 count and the
categorization rationale. The "<200 legacy-orphan" target is therefore NOT
achieved in Phase 86 and requires a future Phase 87 (bridge rule disambiguation
+ Phase 84 alias-index extension) to reach.

(Pass 3 HIGH-2 rename — the ROADMAP original prose used a deprecated
hyphenated token; this report adopts "legacy-orphan" as the canonical
naming. The semantic target is unchanged.)

## AUDIT-01 Note on the Bridge-Aware vs Legacy Scanner Discrepancy (Pass 2 HIGH-1 + HIGH-3)

The bridge-aware `scripts/scan_cudl_coverage_phase86.py` reports a 5-tier
classification:

- `phase84_hit` and `phase86_synthetic` are TRUE COVERAGE — the libraries.csv
  row exists and resolves at runtime via Phase 84's alias index (synthetic rows
  added to libraries.csv automatically participate in the alias index after
  a refresh).
- `phase86_existing_alma_candidate` is a DOCUMENTED CANDIDATE but NOT counted
  as resolution: the FIST bridge identifies that a real Alma row exists for
  the CUDL classmark's underlying inventory, but the user-typed CUDL form
  may not actually reach that row via the app's shelfmark search at runtime
  (depends on Phase 84 alias coverage; not extended in Phase 86). This tier
  is renamed per Pass 2 HIGH-3 to avoid the previous "coverage achieved"
  overstatement.
- `multi_inventory_ambiguous` is excluded by D-04a.
- `phase86_residue` (renamed from the legacy scanner's tier-equivalent per
  Pass 2 HIGH-3; this report uses `legacy_orphan` for the legacy-scanner
  number per Pass 3 HIGH-2) is the adjudication target for future iteration.

The legacy `scripts/scan_cudl_orphans.py` does NOT credit these tiers because
it filters on `row[3] == 'CUL'` directly. This is by design (Pass 1 HIGH #4):
the legacy scanner stays byte-stable as a baseline measurement.

For user-facing app implication: `phase86_existing_alma_candidate` coverage
means the libraries.csv row EXISTS (so `/browse?sys_id=...` works for that
real Alma sys_id), but whether shelfmark-search resolves the CUDL classmark
to that row depends on Phase 84's alias index. Extending Phase 84's alias
index to include Phase 86's new FIST↔CUDL keys is a separate phase concern,
not delivered in Phase 86.
