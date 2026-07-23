# Discovery v2 Bake Plan (data-quality re-distill)

**Status:** DRAFT — Codex review REQUIRED before any code change (phase-134 discipline: `build_discovery_sidecar.py` is Codex-reviewed 6 rounds).
**Owner-ratified decisions:** 2026-07-23 (see `.planning/STATE.md` "V2 BAKE SEQUENCING LOCKED").
**Blocking input:** the COMPLETE twin census from the SEED-029/R-source track (see §2). Draft may proceed; **execution may not** until the census lands.

---

## 1. Purpose & scope

Re-distill the discovery sidecar **v1 → v2** to fix the three data-quality defects the owner found in the v1 build (2026-07-23):

1. **Cross-corpus duplicate works** — the same work appears once per source corpus (M-source + Sefaria), `canonical_work_id` unpopulated.
2. **Band-label overclaim** — `expert_verified` conflated top-algorithmic-score with human approval (only 121 rows are `human_confirmed`; 1,067 of the 1,188 `expert_verified` are `unreviewed`).
3. **Anthology/quotation false positives** — a folio that *quotes* a work is claimed as a *witness* to it.

**In scope for v2:** canonical merge (soft), the RCh-Shabbat drop, a `work_relations` table, Lever-1 coverage routing, and the (B) band-label enum rename (already contracted, must be emitted).

**NOT in scope (explicitly deferred):**
- **(C) direction-aware shadow router = v2.1.** It reclassifies ~6% of `direct_witness → quotes_this_work` and needs a full re-instrumented Track-1 re-run + a producer-side direction router. The current shadow is **density-only** (mis-routes ~26% of anthology co-claims), so it is NOT wired into v2. Defect #3 is only *partially* addressed in v2 (coverage routing catches low-coverage quotations; the high-coverage quoted-works residual waits for v2.1).
- **Production deploy (134-08 Task 3)** — deploy the FINAL v2 sidecar ONCE, as a Phase 135 prerequisite. Do not deploy v1 then re-deploy v2.

**The spine is unchanged.** v2 runs through the SAME pipeline, schema, loader, service, and masking guard as v1. This is a data refresh, not an architecture change — which is why it does not re-open the Phase 134 spine deliverables (see §9).

---

## 2. The one open input — the twin census

The canonical merge (§4.1) is driven by a twin seed. The authoritative source is the SEED-029 track's cross-source census (`rsource/results/mask2_v2_cross_census.md` — v2-internal Sefaria/M/JA twins, masking-clean; **NOT** `gen2_workid_registry.json`, which is R-only and for a later gen).

**Required format** (per resolved pair): `raw_work_id_a ↔ raw_work_id_b`, both resolvable to `w000xxx` via `discovery_data/crosswalk.json`, plus the intended canonical representative. Sefaria side resolves with the `REF2:` prefix; RCh numerics are `M:Ytext405xxx`.

**Pitfall #7 (self-erasure):** an INCOMPLETE census silently merges only the pairs it lists and leaves the rest duplicated — or worse, a partial group collapse mislabels. The census must be the COMPLETE list, and the SEED-029 track must confirm no listed work chains to an unrelated work in the full conflict graph before we collapse it.

**Resolved so far** (owner-ratified 2026-07-23) — these are locked; the census must be a superset:

| work | M-source | Sefaria | v2 action | canonical rep |
|---|---|---|---|---|
| Tur Orach Chaim | w000190 | w001382 | MERGE | w001382 (Sef) |
| Ramban Genesis | w000192 | w001269 | MERGE | w001269 (Sef) |
| Ramban Deuteronomy | w000193 | w001267 | MERGE | w001267 (Sef) |
| Sefer haChinukh | w000191 | w001337 | MERGE | w001337 (Sef) |
| RCh Sanhedrin | w000465 | w001238 | MERGE | w001238 (Sef) |
| RCh Taanit | w000459 | w001242 | MERGE | w001242 (Sef) |
| RCh Bava Metzia | w000464 | w001226 | MERGE | w001226 (Sef) |
| **RCh Shabbat** | w000452 | w001239 | **DROP w001239; keep w000452 standalone** | w000452 (M) |
| **Hai Gaon on Shabbat** | w000451 | — | **standalone (no fold)** | w000451 (self) |

Canonical = the Sefaria representative for merges (public/citable identity; masking-aligned — the displayed work is never the restricted-source copy). RCh Shabbat is the exception because its Sefaria copy is dropped.

**Cross-check before merging (from the 2026-07-23 in-DB study):** Sefer haChinukh (28% ms overlap) and RCh Taanit (50% ms / 27% folio) are the weakest-overlapping twins. Low co-occurrence does not disprove same-work (the two reference copies are differently covered), but confirm both against the census token-jaccard (~1.0) before collapse.

---

## 3. Contested case — RESOLVED by drop (no schema needed)

Owner adjudication 2026-07-23 ("drop it and we'll be ok"): the Hai-Gaon / RCh-Shabbat contested-author group is resolved by **deleting the Sefaria RCh Shabbat copy (w001239)**, NOT by a contested-author merge.

In-DB evidence (`tmp/discovery-contested-study*.txt`): Hai Gaon (w000451) co-occurred ONLY with w001239 (11 shared direct-witness folios, byte-identical `matched_letters`), and ZERO with the M-source RCh Shabbat (w000452). After the drop, all 11 shared folios KEEP a `tier_a` Hai Gaon identification (0 loss on contested folios). Hai Gaon then has zero RCh overlap → the contested question vanishes.

**Consequence:** the planned v2 `works` **disputed-author schema (primary + alt + disputed flag) is DROPPED** — there is no case left to model. Reinstate ONLY if the complete census surfaces OTHER ≥2-author groups (the SEED-029 track must flag them).

**Cost accepted by owner:** RCh Shabbat coverage 65 → 31 direct-witness folios; of 34 Sefaria-only folios, 12 are rescued by another work and ~22 lose their (unreviewed, Sefaria-reference-only) RCh ID (~10 lose any claim). Integrity: w001239 is canonical for no other work and `witness_unit_members` has no `work_id` FK, so it is a clean bake-time exclusion (129 claims / 144 evidence rows).

**Do NOT generalize the drop.** Every other twin MERGES — there is no contested-author reason to delete, and dropping always forfeits the dropped copy's unique folios.

---

## 4. Build changes to `build_discovery_sidecar.py`

Four changes. Each must land behind the strict masking gate and the all-invariant verifier (§7).

### 4.1 Populate `canonical_work_id` from the twin seed (soft merge)
- Read the census; for each resolved pair, set `works.canonical_work_id = <rep>` on BOTH members. Self-canonical for unmerged works (already the default).
- **Soft merge only** — do NOT rewrite `discovery_claim.work_id`. Provenance (which source copy) is preserved in `work_id`; display collapses via `canonical_work_id`.
- **Consumer contract (document, enforce downstream in 135/136):** all display/aggregation groups by `canonical_work_id`, and de-dups `(page_id, canonical_work_id)` so a folio witnessed under both copies shows ONCE. The spine keeps both rows (PK `(page_id, work_id)`); the collapse is a projection.
- Transitivity guard: a work may appear in at most one merge group; reject a census that chains a work into two groups.

### 4.2 DROP list — exclude w001239 entirely at bake
- A hard exclusion set (start: `{w001239}`). Excluded works emit NO `works`, `discovery_claim`, or `discovery_evidence` rows.
- Orphan check: after exclusion, assert no surviving claim/evidence/unit-member references a dropped work_id (should be structurally impossible via the two-table build, but assert it — a HARD FAIL).

### 4.3 NEW `work_relations` table (directional + subset only)
- Schema: `(work_id_a TEXT, work_id_b TEXT, relation_type TEXT, direction TEXT NULL, note TEXT NULL)`; `relation_type ∈ {embeds, abridges, base_text}` (frozen enum, extend only via the frame doc).
- Relations are NOT merges — the two works stay distinct; the table records that one contains/abridges/is-the-base-of the other, for honest display and to suppress false "same-work" collapse.
- Seed rows (owner-ratified; w-ids for the b-side of Rif↔Bavli and Mishnah-Avot come from the census):
  - MT Sefer Zmanim `w000177` → Haggadah `w001159` — `embeds` (directional: Zmanim embeds the Haggadah text).
  - Rif ↔ Bavli, per tractate — `abridges` (jac 0.42–0.62; w-ids TBD from census).
  - Mishnah Avot ↔ David haNagid `w001135` — `base_text` (Avot is the base text David haNagid comments on; Avot w-id TBD from census).

### 4.4 Lever-1 coverage routing (in claim-gen)
- Coverage = `matched_letters / len(norm_stream(page_text))`, computed at bake. `matched_letters` is populated on 254,729 evidence rows; the denominator is the normalized source page text; verify the computed value against the stored `density` column.
- Routing: `cov ≥ 0.45 → routing_status='shipped'`; `cov < 0.45 → routing_status='review_only'` (recoverable — the claim is retained, just not surfaced). Cliff is at 0.45 (validated: high 94.0% / med 91.7% / low 37.5% at the page-level unit; 0.50 point 94.3%, one-sided 95% LB 90.1% — SEED-029 page-level catalogue-blind deck, `track1_pagelevel_manifest.json`).
- **INVARIANT (never route on catalogue):** coverage routing uses ONLY the coverage metric. Catalogue mismatch (52%, coverage-confounded) NEVER demotes a claim.
- **INVARIANT (review_only never dominates shipped):** if a page's display claim would be `review_only` while a shipped claim exists for the same `(page_id, canonical_work_id)`, the shipped claim wins the `display_evidence_id`. A `review_only` row must never orphan a shipped base.

---

## 5. Band-label honesty (B) — already contracted, must be emitted

The (B) contract (`docs/specs/discovery-band-labels-v1.md`) LANDED. v2 must emit its enum rename in lockstep across the 7 files listed in §5 of that doc:
- `expert_verified → high_confidence_algorithmic` (Track-1 top tier is an ALGORITHMIC score, not human approval).
- "verified/confirmed/certified" reserved for `adjudication_status='human_confirmed'` ONLY (121 rows corpus-wide).
- Bilingual EN/HE band labels per §2; estimated band precision `[CI]` presentation per §3, never per-item.

v2 verify (§7) must assert the v1 band enum names are ABSENT from the v2 asset (grep the shipped DB for `expert_verified`).

---

## 6. Order of operations (Codex-blessed; demotion BEFORE any finalize)

1. canonical / vgroup resolution (§4.1) + drop list (§4.2)
2. span-paired claim generation
3. distinctive / shared routing
4. relation table population (§4.3)
5. Lever-1 coverage routing (§4.4)  ← demotion happens here, before any shadow finalize
6. tier-A assignment
7. bake + verify + masking + manifest

Shadowing (v2.1) is non-monotone under row addition → a full recompute every bake; not in v2.

---

## 7. Gates (all must pass before ship)

1. **Codex review of the build-script diff** — REQUIRED, precedes code merge (phase-134 discipline).
2. **All-invariant verifier** (standalone, from 134-03) — two-table integrity, offsets-only evidence, per-source bands, frozen enums, release-contract counts, `PRAGMA integrity_check`, schema_version.
3. **Strict masking gate** — `MASKING_SCAN_PATTERNS_FILE=.masking_patterns python scripts/check_atlas_masking.py --scan-repo --scan-sqlite --scan-asset <v2.db> --strict` → exit 0. Independent re-scan of every hand-edited/AI-generated artifact.
4. **Band-enum absence** — v1 names (`expert_verified` etc.) absent from the v2 DB.
5. **Coverage sanity** — recomputed coverage matches stored `density` within tolerance; the 0.45 routing split reproduces the expected shipped/review_only counts.
6. **Frame doc** — write `docs/specs/discovery-frames-v2.md` with corrected per-band / per-evidence_source counts, merge/drop/relation summary, and the new `frame_content_hash` + DB `content_hash`; update `discovery_data/manifest.json`.

---

## 8. Pitfalls (carry forward)

- **#7 self-erasure** — incomplete census → silent partial merge. Census must be complete + chain-checked.
- **Soft-merge display gap** — if a consumer forgets to group by `canonical_work_id`, the duplicate reappears. Enforce in the 135/136 display layer.
- **review_only orphaning** — a demoted row must never be the sole `display_evidence_id` for a page that also has a shipped claim.
- **Coverage denominator** — `len(norm_stream(page_text))` must use the SAME normalization as the aligner, or coverage is wrong. Verify vs stored `density`.

---

## 9. Relationship to Phase 134 closure

This plan is a **data refresh carried forward**, not a re-opening of the Phase 134 spine. All three Phase 134 success criteria (two-table schema + contract; permanent masking guard; async DiscoveryService with budgets) are MET by the v1 build and its code, and none of the three data-quality defects violate them. The v2 re-distill runs through the unchanged pipeline and MUST complete before Phase 135 grading and Phase 136 read surfaces — it is a Phase 135 prerequisite, tracked here, gated on the census.
