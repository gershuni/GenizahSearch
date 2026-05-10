# Phase 86: CUDL Coverage Audit + Synthetic Re-attempt - Context

**Gathered:** 2026-05-10
**Status:** Ready for research/planning

<domain>
## Phase Boundary

Two intertwined deliverables:

1. **Synthetic re-attempt with maximum-feasible CUDL coverage.** Walk the CUDL
   classmark universe (~141K), filter to those NOT already resolvable to
   `libraries.csv` via the Phase 84 bridge (~5,325), resolve each to a FIST
   InventoryId via a new **bidirectional FIST↔CUDL normalizer**, and emit a
   synthetic libraries.csv row using Phase 85's locked Option-2 sys_id format.
   Image-bearing only — every synthetic row has a CUDL manifest by
   construction (we walk the manifest set). FJMS enrichment is automatic via
   the existing Phase 85 UNION-ALL pattern when the FIST inventory has
   metadata.

2. **CUDL coverage audit.** `scripts/scan_cudl_orphans.py` re-run after this
   phase + `reports/cudl_coverage.md` durable artifact (per-collection
   breakdown + residue pattern analysis) + v7.9.4 NLI-Oxford regression
   verification.

In scope: AUDIT-01, AUDIT-02, AUDIT-03 from `.planning/REQUIREMENTS.md`,
plus the synthetic re-attempt scoped by ROADMAP §"Phase 86" success
criterion 5.

Inverted walk vs Phase 85: Phase 85 walked FIST.dbo_Inventory and used
CUDL as a filter. **Phase 86 walks CUDL and uses FIST for InventoryId
resolution.** This dodges the multi_signature ambiguity that hung up
Phase 85's Plan 02 — we're keyed by CUDL classmark, not by FIST SignatureId.

Out of scope (carried from Phase 85 + REQUIREMENTS Out of Scope):
- New `library_code` values — synthetic rows reuse `CUL` for T-S/Or, `Mosseri` for Mosseri.
- Changing the `99 + InventoryId-padded-10 + 000000` sys_id contract.
- Tantivy stub-rows for synthetic IDs (SYNTH-03 narrowing accepted: Title+Shelfmark only).
- Reverse audit (NLI Alma in libraries.csv but absent from CUDL/FJMS).
- Synthetic rows for non-CUL/Mosseri collections (AIU/Halper FJMS-only
  without CUDL manifests).
- A separate "CUDL-only no-FIST" sys_id allocation strategy (the 1,599
  residue stays a documented iteration target, not a Phase 86 deliverable).
- Migrating libraries.csv to SQLite, server-side IIIF cache, NLI Alma
  metadata fabrication.

</domain>

<decisions>
## Implementation Decisions

### Generation Strategy
- **D-01: CUDL-walked, not FIST-walked.** `_build_qualifying_inventories`
  in `scripts/generate_synthetic_rows.py` is rewritten to walk
  `nli_crossref.db.cambridge_manifests`. For each classmark:
  1. Skip if Phase 84 `lookup_cudl(classmark)` resolves it (already in libraries.csv).
  2. Resolve via the new bidirectional FIST↔CUDL normalizer to a FIST InventoryId.
  3. If resolved → emit synthetic row (with FJMS enrichment when present).
  4. If unresolved → log to residue with the closest pattern guess for
     iteration analysis.
- **D-01a (image-bearing-only):** Every emitted synthetic row HAS a CUDL
  manifest by construction. Phase 85 Plan 02's "FJMS metadata-only" inclusion
  branch is dropped — those bib-only rows were the failed stance that got
  reverted on 2026-05-09. The originating user case (`T-S NS 329.96`) closes
  here because it has both a CUDL manifest AND a FIST inventory.

### Bidirectional FIST↔CUDL Normalizer (NEW)
- **D-02: Extend `shared/shelfmark_bridge.py`** (or sibling module
  `shared/fist_cudl_bridge.py` — planner's call) with reverse-direction
  normalization that maps FIST.Shelfmark forms to CUDL classmark keys and
  vice versa. Phase 84's `lookup_cudl` covers libraries.csv ↔ CUDL; this is
  the parallel work for FIST ↔ CUDL.

- **D-02a: Confirmed patterns (locked, recovers ~70% / 3,726 of 5,325):**
  - **Mosseri Roman prefix:** `mosseri{roman}` (CUDL) ↔ `moss{roman}` (FIST).
    Bidirectional. Recovers ~3,057 Mosseri (the bulk of the win).
  - **FIST data-noise prefix-strip:** when FIST.Shelfmark contains `:`,
    also try the substring after the last `:`. Covers `Mosseri: Moss. IV,27.1`,
    `Library Shelmarks: Or. 1081/73b`, `AIU: CUL: Or.1081 1.68`,
    `T-S Ar.: T-S Ar 18.34`.
  - **`(N)` series-suffix strip:** `T-S F1(1).11` (FIST) ↔ `tsf1.11` (CUDL).
    Strip the parenthetical series indicator. Note: this collapses ALL
    `(N)` occurrences, so `T-S F1(1).11` AND `T-S F1(2).11` both map to
    `tsf1.11`. That's a multi_signature situation handled by D-04 below.
  - **Or. multi-segment dot-fix:** `or1080.X.Y` (CUDL) ↔ `or1080X.Y` (FIST norm).
    Bidirectional. Insert/remove the dot after `1080` or `1081`. Single-segment
    variant (`or1080.X` ↔ `or1080X`) included.

- **D-02b: Investigation patterns for the 1,599 residue (HUMAN-IN-THE-LOOP).**
  The user adjudicates ambiguous patterns during research/planning before
  the rules are locked. Each pattern requires a small fixture of CUDL
  classmark + FIST candidate pairs to confirm or reject the mapping rule:
  - **T-S F (392) / T-S Ar (303): "flattened-series" hypothesis.** CUDL
    `tsf1.1100` may correspond to `T-S F1(1).100` or `T-S F1(2).100` (CUDL
    appears to encode the FIST `(N)` series digit as a leading digit in the
    suffix). User reviews 5–10 fixtures with adjacent FIST entries to
    confirm or reject. Stretch target: ~600 additional recoveries if rule
    holds.
  - **T-S NS (179): "minute fragments" + letter suffixes.** FIST has
    `T-S NS 192.minute fragments` form — a phrase-suffix normalizer
    converts CUDL `tsns192minutefragments` to match. Letter-suffix patterns
    (`tsns135.1aa`, `tsns135.1ab`) need separate adjudication — FIST may
    write them as `T-S NS 135.1.AA` or as a different segmentation.
  - **Or. (571): single-segment ambiguity.** CUDL `or1080.11` vs FIST
    `Or.1080 11.1` (sub-fragment level) — these may be genuinely different
    granularities. User reviews IIIF manifest content vs FIST inventory
    description to decide whether to map (and if so, which FIST entry wins).
  - **Mosseri (48): exotic letter suffixes.** `mosseriii117.1a`,
    `mosseriv270b` — FIST may have these under variants we haven't probed
    yet. User reviews fixtures.
  - **T-S Misc (98): multi-segment patterns.** `tsmisc1.131.1`,
    `tsmisc24.137.21` — likely a multi-segment normalizer rule similar
    to Or., but warrants spot-checks before rolling out.

- **D-02c: Iteration venue and protocol.** The planner's research step
  produces a `86-RESIDUE-PATTERNS.md` artifact listing the 5 pattern
  candidates above with sample fixtures (CUDL classmark + nearest-neighbour
  FIST inventories). User adjudicates each pattern: accept (with rule),
  reject (truly-different-fragments — leave in residue), or "spot-check
  more before deciding". Each accepted pattern becomes a normalizer rule
  with test fixtures. Rejected patterns are documented in
  `reports/cudl_coverage.md` so future maintainers know they were
  evaluated and excluded by design, not by oversight.

### Multi_signature Relax (Phase 85 D-05a Carry-Forward)
- **D-04: Relax D-05a STRICT for unambiguous multi_signature.** When all FIST
  SignatureIds for a normalized key resolve to the same canonical_shelfmark
  AND the same library_code, pick the lowest SignatureId per the existing
  Phase 85 tie-break logic. The CUDL-walk inversion makes this naturally
  rare (we're keyed by CUDL classmark, so multi_inventory ambiguity drops
  to ~zero observed in the 158 with-FIST cases), but it's the rule that
  closes T-S NS 329.96 (currently in `reports/synthetic_ambiguity_residue.csv`
  with 13 distinct SignatureIds, all under the same shelfmark).
- **D-04a: multi_inventory stays excluded.** If the same CUDL classmark
  resolves to multiple distinct FIST InventoryIds (would yield different
  sys_ids), exclude — that's a real ambiguity, not a notation duplicate.
  Log to residue.

### Parent-Shadow Filter (Cheap Insurance)
- **D-06: Apply `reports/synthetic_parent_shelfmarks.csv` filter.** Even
  though CUDL-walk inversion likely makes this moot (CUDL classmarks are
  leaf-level), the filter is cheap and prevents a regression of the
  Phase 85 175-row shadow case (synthetic `T-S NS 161` shadowed 1,009 real
  `T-S NS 161.x`). User punted "should we keep the filter?" to seeing data —
  default is keep the filter; if no rows are filtered, document in
  `cudl_coverage.md` that the filter found nothing this round.

### FJMS Enrichment
- **D-07: Automatic via Phase 85 UNION-ALL.** `scripts/export_fist_enrichment.py`
  already wires synthetic AlmaId injection across 12 enrichment tables
  (catalog, bibliography, measurements, etc.). After the new synthetic
  block lands in `libraries.csv` and `fist_data/synthetic_manifest.json`,
  run the export script to regenerate `fist_data/fjms_enrichment.db` —
  same operational step Phase 85 deferred. No code changes needed in the
  export script; the new synthetic rows flow through the existing UNION-ALL
  pattern.
- **D-07a: Web deploy + desktop installer rebuild required.** Both apps
  bundle / load the regenerated `fjms_enrichment.db`. Per
  `feedback_no_github_release_for_web_only.md` — desktop installer goes
  out only if there are desktop-side code changes; for a pure data refresh,
  bundle in next desktop release rather than triggering a release-prompt
  to all desktop users. Planner decides desktop release strategy.

### Audit Deliverables
- **D-08: AUDIT-01 (`scripts/scan_cudl_orphans.py` re-run).** Re-run
  `python scripts/scan_cudl_orphans.py --out-suffix _post_phase86`
  against the regenerated `libraries.csv` (with synthetic block in place).
  Expected: orphan count drops from 6,053 (post-Phase-84) to ~1,599
  (residue not yet covered by Phase 86's normalizer). The roadmap target of
  "<200 truly-orphan" was set before we knew the residue was 1,599 mostly-
  recoverable patterns; the actual <200 target is conditional on how
  much of the 1,599 user-adjudicated investigation closes. Document the
  delta and reasoning in `cudl_coverage.md`.
- **D-09: AUDIT-02 (`reports/cudl_coverage.md`).** Single durable artifact
  capturing post-milestone state. Sections:
  - Methodology (CUDL-walk inversion, bidirectional bridge)
  - Per-collection breakdown (T-S, Mosseri, Or., Add., etc.) with
    matched-via-Phase-84-bridge / matched-via-Phase-86-FIST-bridge /
    truly-residual counts
  - Residue pattern analysis (the 1,599 with the 5 pattern hypotheses
    from D-02b, marked accepted/rejected/deferred per user adjudication)
  - Re-run instructions (which scripts in what order)
  - Cross-link to `synthetic_coverage.md` (Phase 85 artifact)
- **D-10: AUDIT-03 (v7.9.4 NLI-Oxford regression check).** Two-pronged:
  1. **Scan-based check:** SQL query / script asserting all 461 NLI-flipped
     rows from v7.9.4 still have `library_code='NLI'` post-Phase-86.
     Lives in `scripts/audit_nli_attribution.py` or similar.
  2. **Permanent regression test:** add `tests/test_nli_oxford_attribution.py`
     with golden fixture (sample of the 461 rows) so this regression
     bucket is checked on every CI run, not just Phase 86. Planner picks
     test scope (golden 20 rows vs all 461). The script-level scan is
     mandatory; the persistent test is strongly recommended.

### Phase 85 Infrastructure Activation
- **D-11: All Phase 85 infrastructure stays as-is and activates with the
  new data.** No retroactive changes to:
  - `shared/synthetic_sys_id.py` (helper module)
  - browse hide-NLI gates in 12 source files
  - `web/api.py` /api/fl_ids JSON empty + /api/nli_image_by_sysid 204
  - `shared/search_serializer.py` `is_synthetic` field
  - `corrections_client.py` + `supabase_corrections_client.py` rejection
  - `web/api_hardening.py` PostHog `is_synthetic` property
  - `scripts/export_fist_enrichment.py` UNION-ALL synthetic injection
  The infrastructure was load-bearing-correct on Phase 85 verification;
  the only thing that was wrong was the qualification rule in
  `_build_qualifying_inventories`. That's the single function Phase 86
  rewrites.

### Verification & Rollback
- **D-12: HUMAN-UAT plan replaces Phase 85's superseded UAT.** The 6 items
  in `85-HUMAN-UAT.md` were marked SUPERSEDED on 2026-05-09 because the
  data was reverted. Phase 86 ships fresh UAT covering:
  1. Browse `/browse?sys_id=99...` for 5–10 representative synthetic
     rows (cover T-S NS 329.96 + one Mosseri + one Or. + one T-S F if
     covered + one expected-to-be-bib-only edge case)
  2. Search T-S NS 329.96 in Shelfmark mode → result row → browse opens
  3. List round-trip with synthetic sys_id
  4. Correction button hidden on web + desktop btn_b_edit hidden + Ctrl+Shift+S
     gives QMessageBox without crash
  5. Open desktop app post-build, repeat browse + list flows
  6. PostHog: confirm `is_synthetic: true` events fire on synthetic browse
- **D-12a: Rollback path.** Same lever as Phase 85: empty
  `libraries.csv` synthetic block (markers preserved), set
  `fist_data/synthetic_manifest.json` to `[]`, restore
  `fjms_enrichment.db` from gz backup. Infrastructure stays dormant. The
  generation script's `--apply` mode produces a `.bak` of `libraries.csv`
  on every run for safe rollback.
- **D-12b: No env-var feature flag.** Synthetic rows are data, not a code
  toggle. The lever is "regenerate the data without the synthetic block"
  not "flip a flag." Avoids the dormant-flag-gathering-dust anti-pattern.

### Claude's Discretion
- Choice of normalizer module location (`shared/shelfmark_bridge.py`
  extension vs new `shared/fist_cudl_bridge.py` sibling).
- Test scope for D-10 v7.9.4 regression (20 golden rows vs all 461).
- Exact format of `86-RESIDUE-PATTERNS.md` research artifact (markdown
  tables vs CSV vs both).
- Whether to keep `reports/synthetic_ambiguity_residue.csv` populated
  with Phase 85's 10,689 entries or rebuild fresh from Phase 86's
  CUDL-walk path. Note: keeping it preserves audit trail; rebuilding
  it makes it consistent with the new walk. Planner picks; Phase 85
  D-05a residue stays in git history regardless.
- Desktop installer release strategy (rebuild this round or bundle
  with next desktop-code release).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & Roadmap
- `.planning/REQUIREMENTS.md` §"Coverage Audit" (AUDIT-01, AUDIT-02, AUDIT-03)
- `.planning/REQUIREMENTS.md` §"Synthetic Inventories" — SYNTH-01..06 still
  apply (Phase 85 infrastructure activates with Phase 86 data).
- `.planning/REQUIREMENTS.md` §"Out of Scope" — sys_id contract, library_code
  reuse, deferred items.
- `.planning/ROADMAP.md` §"Phase 86 -- CUDL Coverage Audit + Synthetic
  Re-attempt" — 6 success criteria.

### Phase 84 Foundation
- `.planning/phases/84-cudl-shelfmark-normalization/84-CONTEXT.md` —
  D-08 opaque-string sys_id contract, alias index pattern.
- `shared/shelfmark_bridge.py` — Phase 84 bridge module:
  `cudl_normalize`, `lookup_cudl`, `build_alias_index`,
  `_index_key_for_label`, `_collapse_numeric_runs`,
  `shelfmark_to_cudl_label`. Phase 86 extends this with the FIST↔CUDL
  reverse direction.

### Phase 85 Foundation (Infrastructure Already Shipped, Dormant)
- `.planning/phases/85-synthetic-fjms-inventory-rows/85-CONTEXT.md` —
  D-01..D-15 lock the format, layered pattern, helper contracts.
- `.planning/phases/85-synthetic-fjms-inventory-rows/85-VERIFICATION.md` —
  Phase 85 outcome incl. UAT supersession after data revert.
- `.planning/phases/85-synthetic-fjms-inventory-rows/85-04-AUDIT.md` —
  authoritative enumeration of every NLI/KTIV/PNX call site (Phase 86
  AUDIT-03 input).
- `shared/synthetic_sys_id.py` — `is_synthetic_sys_id` /
  `encode_inventory_sys_id` / `decode_inventory_id`. Locked.
- `scripts/generate_synthetic_rows.py` — `_build_qualifying_inventories`
  is the function Phase 86 rewrites. Marker-block + manifest output
  contract preserved.
- `scripts/export_fist_enrichment.py` — UNION-ALL synthetic AlmaId
  injection across 12 enrichment tables. No code change in Phase 86;
  runs unchanged after new synthetic block lands.

### CUDL & FIST Data Sources
- `nli_data/nli_crossref.db.cambridge_manifests` — 141,368 CUDL classmarks
  with manifest URLs. Phase 86 generation walks this.
- `fist_data/FIST.db` — `dbo_Inventory` (InventoryId + Shelfmark),
  `dbo_InventorySignature`, `dbo_Signature`, `dbo_InventoryAlma`,
  `dbo_UnitCatalogRec`, `dbo_UnitBibliographyReference`,
  `dbo_UnitFreeDescription`, `dbo_UnitFullText`, `dbo_CatalogMultiSize`.
  FIST↔CUDL bridge keys off `dbo_Inventory.Shelfmark`; metadata for
  enrichment from the linked tables.

### Reports (Inputs)
- `reports/cudl_orphans_post_phase84.csv` — 6,053 baseline orphans (input
  to AUDIT-01 delta).
- `reports/synthetic_parent_shelfmarks.csv` — 175 parent-shadow filter
  (Phase 85 audit input). D-06 reads this.
- `reports/synthetic_ambiguity_residue.csv` — 10,689 Phase 85 D-05a
  STRICT residue (95 of which are the multi_signature group D-04 relaxes).
- `reports/synthetic_coverage.md` — Phase 85 coverage artifact (cross-link
  in Phase 86 cudl_coverage.md, do not rewrite).
- `reports/cudl_unresolved_no_fist_sample.csv` — generated 2026-05-10
  during this discussion (5,167 entries; superseded once Phase 86 walks
  the data with the new bridge).

### Reports (Phase 86 Outputs — to be created)
- `reports/cudl_coverage.md` — AUDIT-02 deliverable.
- `reports/scan_cudl_orphans_post_phase86.txt` — AUDIT-01 deliverable.
- `reports/cudl_orphans_all_post_phase86.csv` — full residue listing.
- `reports/cudl_orphans_with_neighbor_post_phase86.csv` — neighbour-aware
  subset.
- `.planning/phases/86-cudl-coverage-audit-and-synthetic-reattempt/86-RESIDUE-PATTERNS.md` —
  research artifact for human-in-the-loop pattern adjudication (D-02c).

### libraries.csv & Sidecar Targets
- `libraries.csv` — synthetic block target (between marker comments).
- `fist_data/synthetic_manifest.json` — authoritative qualifying-set,
  rebuilt every `--apply` run.
- `fist_data/fjms_enrichment.db` — must be regenerated via
  `scripts/export_fist_enrichment.py` after `libraries.csv` synthetic
  block lands. Operational step Phase 85 deferred; Phase 86 closes it.

### v7.9.4 NLI-Oxford Regression Reference
- `scripts/fix_nli_oxford_mislabel.py` — original v7.9.4 fix (461 rows
  flipped Oxford → NLI in libraries.csv). Phase 86 D-10 builds the
  regression check around this fixture set.

### CHANGELOG
- `CHANGELOG.md` §"[Unreleased] v7.11" — Phase 86 release notes go here.
  Phase 85 outcome (infrastructure-without-data) already documented;
  Phase 86 amends with the actual synthetic-row population landing.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **Phase 84 bridge** (`shared/shelfmark_bridge.py`) — `cudl_normalize`,
  `lookup_cudl`, `_index_key_for_label`, `_collapse_numeric_runs` provide
  the libraries.csv ↔ CUDL direction. Phase 86 extends with the FIST ↔ CUDL
  direction (mirror module or extension).
- **Phase 85 helper module** (`shared/synthetic_sys_id.py`) — locked
  contract. Phase 86 imports unchanged.
- **Phase 85 generation script** (`scripts/generate_synthetic_rows.py`) —
  marker-block rewrite + manifest output + ambiguity residue CSV +
  CSV-injection fail-loud + D-01a collision check + idempotent
  regeneration. Only `_build_qualifying_inventories` and the residue
  schema (now with new `pattern_guess` column for D-02c iteration)
  change in Phase 86.
- **Phase 85 export-script UNION-ALL** (`scripts/export_fist_enrichment.py`)
  — 12 enrichment tables with synthetic AlmaId injection. Runs unchanged
  on Phase 86 data.
- **`scripts/scan_cudl_orphans.py`** — already imports from
  `shared.shelfmark_bridge`. AUDIT-01 reuses with `--out-suffix _post_phase86`.
- **Phase 85 browse hide-NLI gates, public API `is_synthetic` field,
  corrections-write rejection** — wired across 12+ source files. All
  remain load-bearing-correct; Phase 86 does not touch them.

### Established Patterns
- **Sidecars are read-only at runtime; mutation happens at export time.**
  Phase 86 follows: bridge work in `scripts/generate_synthetic_rows.py`,
  enrichment regeneration via `scripts/export_fist_enrichment.py`,
  zero runtime branches added.
- **Bridge as ONE source of truth.** Phase 84 D-08 site #4 — all 4 D-08
  call sites went through the bridge. Phase 86's FIST↔CUDL bridge follows
  the same pattern: any code that needs FIST↔CUDL resolution imports the
  bridge.
- **Marker-block idempotency** in `libraries.csv` — `# BEGIN SYNTHETIC` /
  `# END SYNTHETIC`. Rerunning generation rewrites the block; never
  duplicates rows.
- **Helper-as-public-contract** (Phase 85 D-01) — Phase 86's
  bidirectional bridge follows: publish small named functions
  (`fist_to_cudl_keys(shelfmark) -> set[str]`,
  `lookup_fist_by_cudl(classmark) -> Optional[InventoryRecord]`) rather
  than ad-hoc string slicing.
- **No int conversion of sys_ids anywhere** (Phase 85 D-01b) — the
  TestNoIntCoercion lint at `tests/test_synthetic_sys_id.py` keeps
  enforcing this. Phase 86's new code must not introduce violations.

### Integration Points
- **`scripts/generate_synthetic_rows.py::_build_qualifying_inventories`**
  — single function rewrite. New flow: walk CUDL, for each filter via
  `lookup_cudl` (Phase 84), resolve via `lookup_fist_by_cudl` (Phase 86 new),
  emit qualifying record with `cudl_label` + `inventory_id` +
  metadata-when-present.
- **Browse runtime** — Phase 85 D-08 already wires CUDL-default image
  source for synthetic sys_ids with `has_cudl_manifest`. Phase 86 data
  flows through that branch unchanged.
- **`shared/fjms_service.py`** — D-01 layered pattern preserved. The
  ~30 `WHERE AlmaId = ?` queries work transparently because the synthetic
  AlmaIds get pre-populated into the AlmaId column at FJMS export time.

### Constraints
- **Both apps must be maintained** (web + desktop parity). Phase 86 has
  almost zero code-side branching beyond the generation script — the
  data activates already-written paths. Web deploy + desktop installer
  rebuild (or bundle-with-next-release) per `feedback_no_github_release_for_web_only.md`.
- **Runtime data refresh required.** `fjms_enrichment.db` must be
  regenerated. Phase 85 deferred this; Phase 86 closes it. The web server
  needs the new sidecar before /browse on synthetic IDs renders FJMS
  enrichment correctly.
- **No new library_code values.** Synthetic rows reuse `CUL` for T-S/Or
  classmarks, `Mosseri` for Mosseri classmarks. D-15 carry-forward.
- **No int(sys_id) anywhere.** Phase 85 lint preserved.

</code_context>

<specifics>
## Specific Ideas

- **The user's reframe was the unblock**: "We have a goal — there are CUDL
  items that NLI does not have. We have to find them and create items for
  them, so we'll show the images and CUDL metadata, with synthetic sys_ID."
  This shifted Phase 86 from FIST-walked (Phase 85's failed approach)
  to CUDL-walked. The empirical investigation confirmed the inversion is
  cleaner: 96.2% of CUDL classmarks resolve via Phase 84, 5,325 are
  truly orphan, and 70%+ of those resolve to a FIST inventory under a
  different shelfmark form (not a real absence — an encoding gap).

- **The user's intuition was empirically correct**: "I guess that most
  (if not ALL!) of no-FIST id actually do have FIST id, it's just different
  exact shelfmark." Validated:
  - Naive normalize: 158 / 5,325 (3%)
  - + Mosseri Roman + FIST data-noise prefix-strip: 3,219 (60%)
  - + `(N)` series-suffix strip + Or. dot-fix: 3,726 (70%)
  - Residue 1,599: 5 known pattern families, all tractable with human
    adjudication. Probably ANOTHER 600–900 recoverable in this phase
    if the user accepts the rule proposals.

- **Iteration posture: maximize-now, not ship-and-iterate-later.** User
  explicitly chose: "I'll want to investigate to maximize the mapping —
  investigation can use me as human in the loop." Phase 86 includes a
  research/investigation step that surfaces the 5 residue patterns to the
  user with sample fixtures, captures their adjudication, and rolls
  accepted rules into the bridge before generation runs. Goal:
  ship Phase 86 at the highest feasible coverage, not at "70% baseline +
  Phase 87 for the rest."

- **Phase 85's revert is the negative example.** Plan 02's "inclusive
  coverage stance" produced 5,035 bibliography-pointer-only synthetic
  rows that were "useless for actual research without the underlying
  manuscript." Phase 86 image-bearing-only is non-negotiable. Every
  synthetic row HAS a CUDL manifest by construction.

- **T-S NS 329.96 is the user's originating case.** Currently in
  Phase 85's `synthetic_ambiguity_residue.csv` as `multi_signature` with
  13 distinct SignatureIds, all under canonical_shelfmark `T-S NS 329.96`,
  library_code `CUL`. Phase 86 D-04 multi_signature relax closes it.
  Acceptance test for Phase 86: this shelfmark must search-resolve, browse
  successfully, and show CUDL images.

- **The 1,599 residue is the start of a future investigation, not a
  documented blocker.** `cudl_coverage.md` Phase 86 output should make it
  clear: these are pattern hypotheses where rule confidence wasn't
  high enough, OR cases where human adjudication concluded they map to
  FIST entries at different fragment granularity (legitimate residue,
  not encoding gap).

</specifics>

<deferred>
## Deferred Ideas

- **CUDL-only no-FIST sys_id allocation** — for any CUDL classmarks that
  remain truly without FIST after Phase 86's bridge work (likely most
  of the 5,167 originally identified, minus what gets recovered),
  encoding via a hash-derived InventoryId or a separate ID space would
  expand coverage but changes the format contract. Document as a
  candidate phase if user wants exhaustive CUDL coverage post-86.

- **Reverse audit (NLI Alma in libraries.csv but absent from CUDL/FJMS)**
  — Phase 85 deferred; same scope rationale (different data sources).

- **Synthetic rows for non-CUL/Mosseri collections** (AIU/Halper FJMS-only
  inventories without CUDL manifests) — Phase 85 deferred; image-bearing-only
  D-01a forecloses unless they get CUDL manifests later.

- **Periodic NLI gap-file refresh** — Phase 85 deferred; no known NLI
  changelog feed. FIST.db is the refreshable source.

- **Tantivy stub-rows for synthetic IDs (full-text/Responsa search modes)**
  — Phase 85 SYNTH-03 narrowing to Title+Shelfmark only is accepted.
  Synthetic rows have no transcription text, so they don't appear in
  text/Responsa search. If user later wants this, separate infrastructure phase.

- **Mosseri "2nd series" patterns** (`Ms. L 241`, `Ms. MOSS NS`) — carried
  from Phase 84/85 deferred. If they surface in Phase 86 residue analysis,
  promote to follow-up phase.

- **Server-side IIIF image cache** (SEED-001 from project-level seeds)
  — separate trigger conditions, not coupled to Phase 86.

- **Migrating libraries.csv to SQLite** — project-level deferred. Phase 86
  continues to use the marker-block CSV mechanism.

- **Convention-aware normalization for T-S F/Ar "flattened-series"** —
  if user's adjudication during D-02c rejects the flattening hypothesis
  (e.g., if `tsf1.1100` ≠ `T-S F1(1).100` ≠ `T-S F1(2).100` and no rule
  emerges), those ~700 entries stay in residue. Document the rejection
  reasoning so future maintainers don't re-litigate.

</deferred>

---

*Phase: 86-cudl-coverage-audit-and-synthetic-reattempt*
*Context gathered: 2026-05-10*
