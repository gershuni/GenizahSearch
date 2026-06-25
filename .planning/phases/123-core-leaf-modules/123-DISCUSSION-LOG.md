# Phase 123: Core Leaf Modules - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-25
**Phase:** 123-core-leaf-modules
**Areas discussed:** (none deep-dived — user opted to lock recommended defaults and proceed to planning)

---

## Course of discussion

Claude loaded prior context (PROJECT.md, SEED-020 §7, ROADMAP Phase-123 detail, REQUIREMENTS
CORE-01..07 + SC#1–5, the Phase 122 CONTEXT) and scouted the live tree (symbol locations, the
exact shared→core back-edge set, manager↔leaf-util coupling, the `CodicologicalManager.load(csv_bank)`
no-module-level-MetadataManager-import fact). It found the phase already tightly bounded and
presented three remaining judgment calls for optional discussion:

1. **Back-edge retarget scope**
2. **Plan & commit shape**
3. **New-module test coverage**

The user asked whether any of these actually required their decision or whether to go straight to
plan. Claude's honest assessment: none required a human decision; sensible defaults consistent with
the Phase 122 precedent existed for each. The user replied **"Go"**, approving the defaults.

---

## Back-edge retarget scope

| Option | Description | Selected |
|--------|-------------|----------|
| Retarget all now-unblocked shared importers | Matches Phase 122 D-02; removes the last module-level `normalize_shelfmark` back-edge; lazy ones cheap | ✓ |
| Module-level only | Retarget `exclusion_service` only; leave lazy ones on facade | |
| Required only | Only the CORE-07 `local_indexer` text-normalize retargets | |

**User's choice:** Retarget all (recommended default, approved via "Go") → **D-01**.
**Notes:** None of these consumers is in the GUARD-01 registry, so this is hygiene/consistency, not a guard requirement.

---

## Plan & commit shape

| Option | Description | Selected |
|--------|-------------|----------|
| Single 7-wave plan, leaf-utils first | One atomic commit per cluster; browse_map_utils + text_normalize land first | ✓ |
| Themed sub-plans | Split into utils → managers → responsa-trio plans | (planner may still do this) |

**User's choice:** Single plan, one atomic commit per cluster, leaf-first ordering (approved via "Go") → **D-02**.
**Notes:** Leaf-first is partly forced — codicological/joins import browse-map utils and must import them from `shared.browse_map_utils` (importing via `genizah_core` would trip GUARD-01).

---

## New-module test coverage

| Option | Description | Selected |
|--------|-------------|----------|
| Identity + smoke tests for all 7 modules | Extends SC#1 beyond the required three | ✓ |
| Only the SC#1-required three | responsa / variants / codicological only | |

**User's choice:** All seven (recommended default, approved via "Go") → **D-03**.
**Notes:** Cheap; directly exercises GUARD-01's per-module intent.

---

## Claude's Discretion

- Exact function membership of `shared/responsa.py` (e.g. whether `build_tantivy_query` belongs with
  responsa or stays engine-side) — a research mapping task, not a user decision.
- Shim wording, import placement, `EXTRACTED_MODULES` AST mechanics, test-file location,
  `_load_ie_volume_map` JSON-path resolution after the move.

## Deferred Ideas

None — discussion stayed within phase scope. (8 weak todo matches reviewed, none folded — see CONTEXT.md.)
