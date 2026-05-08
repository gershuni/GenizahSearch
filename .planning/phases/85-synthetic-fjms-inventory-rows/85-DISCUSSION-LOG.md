# Phase 85: Synthetic FJMS Inventory Rows - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-08
**Phase:** 85-synthetic-fjms-inventory-rows
**Areas discussed:** FJMS lookup architecture, Synthetic-row scope, Persistence & generation source, Browse UX for absent NLI, Closeout (idempotency, helper contract, call_numbers shape)
**External review:** Two Codex CLI consultations — (1) FJMS lookup decision, (2) full review of locked decisions

---

## Initial Area Selection

| Option | Description | Selected |
|--------|-------------|----------|
| FJMS lookup architecture | How catalog/bib/measurement/free-desc dialogs resolve a synthetic sys_id given that all FJMS tables are AlmaId-keyed with no InventoryId column anywhere | ✓ |
| Scope of synthetic rows | Strictly the ~93 T-S NS no-match set, or also Phase 84 T-S/Or residue (~2,169 unresolved orphans), or the full FJMS-only CUL set | ✓ |
| Persistence & generation source | Where rows live (libraries.csv vs separate file vs runtime) AND data source (FIST.db vs gap file vs hybrid) | ✓ |
| Browse UX for absent NLI | How browse signals 'no NLI metadata' on synthetic rows | ✓ |

**User's choice:** All four areas selected.

---

## FJMS Lookup Architecture

| Option | Description | Selected |
|--------|-------------|----------|
| Pre-populate AlmaId column | At FJMS sidecar export time, write the synthetic sys_id INTO the AlmaId column. fjms_service.py stays unchanged. | |
| InventoryId mapping table | Add `synthetic_alma_to_inventory` table; fallback queries via mapping. But no FJMS table has InventoryId column. | |
| Add InventoryId column to all 11 FJMS tables | Schema migration; all fjms_service methods grow UNION/COALESCE on AlmaId OR InventoryId. | |
| Hybrid: synthetic in AlmaId + decode helpers | Pre-populate AlmaId AND publish `is_synthetic_sys_id`, `encode_inventory_sys_id`, `decode_inventory_id` helpers. | ✓ |

**User's choice:** Option 4 (Hybrid). User explicitly requested Codex consultation before locking.

**Codex first review:** Confirmed Option 4. "It is basically Option 1 plus a public contract. Without helpers, people will eventually hand-roll string slicing in link builders, analytics, tests, or import scripts." Flagged five guardrails: (1) export-time collision check, (2) Alma-API call-site branching, (3) other-sidecar tolerance, (4) PostHog `is_synthetic` property, (5) sys_ids stay strings, never int. SYNTH-01's encode/decode helper requirement effectively forces Option 4 by structural argument.

**Notes:** Decision captured as D-01 with Codex guardrails as D-01a (collision check) and D-01b (string discipline).

---

## Synthetic-Row Scope (primary)

| Option | Description | Selected |
|--------|-------------|----------|
| FJMS-only no-Alma | All FJMS inventories with no Alma link AND with FJMS metadata. Limits scope to truly missing manuscripts. | |
| Phase 84 orphan-scanner residue | All ~2,169 unresolved CUDL classmarks regardless of FJMS data. Aggressive; closes Phase 86 target by force. | |
| Strictly the user-reported ~93 T-S NS | Only the original T-S NS 329.x set. Conservative; fails Phase 86 <200 target. | |
| Hybrid: tier by data availability | Tier A (has FJMS), Tier B (CUDL only, lighter mechanism). | |

**User's choice (free-text):** "If it has CUDL image we want it (and we should research wether there is also info)"

**Notes:** Maximum-inclusion stance. Drove follow-up sub-questions on no-image and no-FJMS edge cases.

---

## Synthetic-Row Scope (boundary case 1: no CUDL image)

| Option | Description | Selected |
|--------|-------------|----------|
| Skip them | Image-bearing only. Marginal scholarly value without an image. | |
| Include with FJMS-only badge | Generate synthetic row, populate FJMS metadata, browse shows "No images available" + FJMS dialogs. | ✓ |
| Include only if FJMS has substantial metadata | Require minimum threshold (catalog title OR description OR measurement). | |

**User's choice:** Include with FJMS-only badge.

**Notes:** Later contradicted by D-06 (no badge); resolved as D-07 — the "include" intent stands, the "badge" interpretation is overridden. Flagged by Codex full review.

---

## Synthetic-Row Scope (boundary case 2: no FJMS data)

| Option | Description | Selected |
|--------|-------------|----------|
| Yes, image is enough | If CUDL has the image, generate synthetic row with shelfmark + manifest URL only. | ✓ |
| No, require some metadata | Only generate when at least FJMS title or bibliography exists. | |
| Include but flag separately for audit | Generate row, mark in tracking table for Phase 86 audit. | |

**User's choice:** Yes, image is enough.

**Notes:** Cemented inclusive scope. Combined with the previous boundary case = synthetic rows are generated whenever EITHER condition holds.

---

## Persistence

| Option | Description | Selected |
|--------|-------------|----------|
| Append to libraries.csv | Add ~2K synthetic rows directly. Simplest. | ✓ |
| Separate libraries_synthetic.csv | Cleaner diff/audit but two-file deploy. | |
| Runtime synthesis from sidecars | Most flexible, significant startup hit. | |
| Append + commit script | Like option 1 but regeneration script committed as build step. | |

**User's choice:** Append to libraries.csv.

**Notes:** The "Append + commit script" option's intent (committed regeneration script as source-of-truth) was folded into the captured decision D-04. Codex full review later promoted idempotency to a hard constraint as D-04a.

---

## Generation Source

| Option | Description | Selected |
|--------|-------------|----------|
| Hybrid: CUDL list × FIST.db | Cross-product of nli_crossref.db.cambridge_manifests × FIST.db. | ✓ |
| FIST.db only | Walk dbo_Signature for inventories without Alma link. Doesn't gate on CUDL presence. | |
| NLI gap file Excel | Frozen Feb 2026 snapshot. Mostly out-of-scope libraries. | |

**User's choice:** Hybrid.

**Notes:** Codex full review later refined this with D-05a — "matching, not literal cross-product" — to avoid silent fan-out on ambiguous classmark→signature mappings.

---

## Browse UX for Absent NLI

| Option | Description | Selected |
|--------|-------------|----------|
| Quiet degradation | Hide NLI-only UI; no banner, no badge. | ✓ |
| Explicit FJMS-only / CUDL-only badge | Small badge near shelfmark. | |
| Disclosure banner at top of browse | Most informative; risks UI clutter. | |
| Console/log only | No UI signaling at all. | |

**User's choice:** Quiet degradation.

**Notes:** Contradicted the earlier "Include with FJMS-only badge" answer; resolved at closeout as D-07.

---

## Closeout — Codex Full Review Triggered

**User's choice on "remaining items":** Free-text — "Ask Codex about the made decisions and about further gray areas".

**Codex full review surfaced:**
- Badge contradiction (D-02 vs D-06) — must resolve.
- CUDL-only synthetic rows imply Cambridge becomes default image source AND drives page count, not just fallback.
- Idempotent regeneration: append without ownership markers → duplicates on rerun.
- D-05 should be normalized-key matching, not literal cross-product (ambiguity exclusion).
- Quiet degradation hide-list incomplete: also `/api/fl_ids`, `/api/nli_image_by_sysid`, KTIV link, NLI bibliography chips, puzzle folio resolution, public `/api/browse`.
- Promote-to-must-lock: title precedence, call_numbers shape, no-image/no-text browse behavior (corrections availability).
- Risk: FJMS export touches many tables, not just catalog — needs shared mapping/CTE.
- Risk: Public API serializers emit weak/empty image data for metadata-only rows.
- Risk: `is_synthetic_sys_id` must run after digit-normalization (codebase normalizes at many call sites).

---

## Resolution: Badge Contradiction

| Option | Description | Selected |
|--------|-------------|----------|
| No badge — quiet degradation | Stick with D-06; override the earlier answer. | ✓ |
| Small badge near shelfmark | Restore badge per the earlier answer. | |

**User's choice:** No badge — quiet degradation.

---

## Resolution: CUDL-Only Synthetic Browse Semantics

| Option | Description | Selected |
|--------|-------------|----------|
| CUDL is THE default + drives page count | Cambridge IIIF is active source by default; total_pages = canvas count; navigate canvases. | ✓ |
| Pages still p_num=0, image is incidental | Treat like Phase 53 metadata-only; single image, no page nav. | |

**User's choice:** CUDL is THE default + drives page count.

---

## Resolution: Title Precedence

| Option | Description | Selected |
|--------|-------------|----------|
| TitleHeb → Title → GenizahTitle → shelfmark | Hebrew preferred (primary user base). | ✓ |
| Title → TitleHeb → GenizahTitle → shelfmark | English preferred. | |
| Combined "TitleHeb / Title" bilingual | Concatenate when both present. | |
| Empty (shelfmark-only display) | No title invention. | |

**User's choice:** TitleHeb → Title → GenizahTitle → shelfmark.

---

## Resolution: Community Writes on Synthetic Rows

| Option | Description | Selected |
|--------|-------------|----------|
| Lists + comments yes, corrections no | Hide corrections UI when no uid available. | |
| All community writes allowed | Permit uniformly; risk orphaned correction rows. | |
| All community writes blocked | Synthetic rows are read-only. | |

**User's choice (free-text):** "Basically corrections should be allowed, but this can be noted and deferred if not easily done"

**Notes:** Captured as D-10 — corrections allowed in principle, deferrable if uid/p_num plumbing proves hard. Plan-phase research must surface the complexity for the planner.

---

## Closeout (final)

| Option | Description | Selected |
|--------|-------------|----------|
| Lock idempotent-regeneration constraint | Codex's D-04a guardrail. | ✓ |
| Lock call_numbers shape | Codex pushed for explicit decision. | ✓ |
| Lock 'is_synthetic_sys_id must run after digit-normalization' | Codex flagged digit-only normalization at many call sites. | ✓ |
| Done — write CONTEXT.md | Defer remaining items to planner. | |

**User's choice:** All three constraints locked. (Did not select "Done" alone.)

---

## Claude's Discretion

- Internal organization of the helper module: `shared/synthetic_sys_id.py` standalone vs extending `shared/shelfmark_bridge.py`.
- Exact mechanism for D-04a idempotency (marker block in CSV vs separate manifest).
- Variant generation strategy for D-12 call_numbers (which subset of normalized forms to emit).
- Whether to add a `synthetic_manifest.json` audit file alongside the regenerated CSV block.

## Deferred Ideas

(See CONTEXT.md `<deferred>` section for full list.)

- NLI-publishes-real-Alma migration path (future phase, triggered by NLI publication event).
- Reverse audit (NLI Alma in libraries.csv but absent from CUDL/FJMS).
- Synthetic rows for non-CUL collections (AIU/Halper FJMS-only).
- Periodic NLI gap-file refresh.
- Tantivy incremental rebuild for synthetic-row updates.
- Mosseri "2nd series" patterns.
- `is_synthetic: true` PostHog property surface — flagged D-14 plan-time research item; may slip if complex.
