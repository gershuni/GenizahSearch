# Phase 123: Core Leaf Modules - Context

**Gathered:** 2026-06-25
**Status:** Ready for planning

<domain>
## Phase Boundary

Extract **seven** low-risk, well-tested core clusters from `genizah_core.py` to `shared/`,
each behind a permanent **same-object** `# noqa: F401` re-export shim so every existing
`from genizah_core import …` caller keeps working unchanged:

| # | Symbol(s) | New module | Req |
|---|---|---|---|
| 1 | `VariantManager` | `shared/variants.py` | CORE-02 |
| 2 | `CodicologicalManager` | `shared/codicological.py` | CORE-03 |
| 3 | responsa parsing/expansion (`parse_responsa_query`, `expand_grammatical_prefixes/suffixes`, `expand_judeo_arabic`, `expand_plene_defective`, `_expand_inline_alternation`, the `ResponsaComponent` model, etc.) | `shared/responsa.py` | CORE-01 |
| 4 | `JoinsManager` | `shared/joins_manager.py` | CORE-04 |
| 5 | `ListsManager` | `shared/lists_manager.py` | CORE-05 |
| 6 | `normalize_shelfmark`, `natural_sort_key`, `dedupe_browse_map`, `get_library_display`, `_load_ie_volume_map` (+ IE-volume helpers) | `shared/browse_map_utils.py` | CORE-06 |
| 7 | `strip_nikud`, `strip_search_diacritics` + their normalization constants (`NIKUD_PATTERN`, etc.) | `shared/text_normalize.py` | CORE-07 |

This is **Phase 1** ("core leaf de-risk spine") of the v8.3.0 God-File Decomposition milestone —
it proves the shim+GUARD-01 pipeline on **core** before the engines (Phase 125) are touched.
Builds directly on the Phase 122 `Config` enabler (the cycle pivot is already moved).

**Pure refactor — zero behavior change (GUARD-02).** No user-facing change, no GitHub Release.

**In scope:** the 7 cluster moves + same-object re-export facades; retargeting the lazy
`shared/local_indexer.py` back-edges (text-normalize helpers, CORE-07) **and** the now-unblocked
`browse_map_utils` consumers (see D-01); growing the GUARD-01 `EXTRACTED_MODULES` registry by 7;
retargeting the 5 source-scanning/AST tests to both locations (GUARD-03); per-module identity +
standalone-import smoke tests (D-03).
**Out of scope:** `MetadataManager`/`Indexer` (Phase 124); engines + SEED-011 (Phase 125); any
desktop split (126–127); deleting any original implementation or any shim (deletion pass is
Phase 127 for desktop; `genizah_core.py` core facades are **permanent**, GUARD-04); any behavior change.

</domain>

<decisions>
## Implementation Decisions

These three were the only open judgment calls; all locked to the Phase 122 / SEED-020 §7 precedent.
Everything else is dictated by REQUIREMENTS CORE-01..07, SC#1–5, and the carried-forward invariants
below — do NOT re-litigate.

### Back-edge retarget scope (D-01)
- **D-01:** Retarget **all** now-unblocked `shared/` importers of moved symbols to their new homes
  this phase (matches Phase 122 **D-02** cleanliness — retarget rather than hide a back-edge behind
  the facade). Concretely, beyond the REQUIRED text-normalize retargets:
  - `shared/local_indexer.py:3154` (`strip_nikud, strip_search_diacritics`) → `shared.text_normalize` — **REQUIRED** (CORE-07, SC#3, lazy).
  - `shared/local_indexer.py:3826` (`strip_search_diacritics`) → `shared.text_normalize` — **REQUIRED** (CORE-07, SC#3, lazy).
  - `shared/exclusion_service.py:17` (`normalize_shelfmark`, **MODULE-LEVEL**) → `shared.browse_map_utils` — retarget now (this is the back-edge Phase 122 D-01 explicitly deferred *"until `normalize_shelfmark` moves to `shared/browse_map_utils.py` in Phase 123"*; that move happens here, so retarget it). Removes the last module-level `shared→core` back-edge for `normalize_shelfmark`.
  - `shared/nli_crossref_service.py:365, 376` (`normalize_shelfmark`, lazy) → `shared.browse_map_utils` — retarget (cheap).
  - `shared/search_serializer.py:248` (`get_library_display`, lazy) → `shared.browse_map_utils` — retarget (cheap).
  - **Leave on the `genizah_core` facade:** `shared/nli_crossref_service.py:396` + `shared/shelfmark_bridge.py:266,436` import `construct_mosseri_cudl_label`, which is **NOT** one of the seven moved symbols — it stays in `genizah_core`, so these are not back-edges-to-moved-symbols and are untouched this phase.
- **Note:** None of these consumers is in the GUARD-01 `EXTRACTED_MODULES` registry, so none would
  *trip* the guard either way — D-01 is a hygiene/consistency choice, not a guard requirement. The
  extracted modules themselves (variants/codicological/joins/lists/etc.) MUST import leaf utilities
  directly from `shared.browse_map_utils` / `shared.text_normalize` / `shared.config`, never via
  `genizah_core` (that WOULD trip GUARD-01).

### Plan & commit shape (D-02)
- **D-02:** Single plan, **one atomic commit per cluster** (7 commits), with a **forced ordering**:
  the leaf utilities `shared/browse_map_utils.py` and `shared/text_normalize.py` land **first**,
  before any cluster that imports them. `CodicologicalManager` (`natural_sort_key`) and `JoinsManager`
  (`normalize_shelfmark` via its `_normalize_shelfmark` wrapper) reference browse-map utils — once
  extracted they must import them directly from `shared.browse_map_utils`, so that module must exist
  at their commit boundary. `variants` and the `responsa`-trio member `responsa.py` are independent
  (no leaf-util coupling) and may land in any order after the utils. Suggested wave order:
  `browse_map_utils` → `text_normalize` → `variants` → `responsa` → `codicological` → `joins_manager`
  → `lists_manager`. (Planner may merge into themed sub-plans, but the per-cluster atomic-commit
  discipline and leaf-first ordering are fixed.)
- The full existing pytest suite must be green at **every** cluster commit boundary (SC#5).

### New-module test coverage (D-03)
- **D-03:** Extend the SC#1-required same-object identity assertion to **all seven** modules (not
  just the responsa/variants/codicological three SC#1 names). For each module add:
  (a) a standalone-import smoke assertion (`from shared.X import Y` resolves) and
  (b) a same-object identity assertion (`shared.X.Y is genizah_core.Y`) for the public symbol(s).
  Cheap, and it directly exercises GUARD-01's intent per module (the module imports without pulling
  `genizah_core`). Co-locate near / mirror the Phase 122 CONFIG-01 identity test.

### Claude's Discretion (researcher/planner's call)
- **Exact function membership of `shared/responsa.py`** — which module-level functions/constants
  constitute the responsa cluster vs. stay engine-side. Open question to settle in research: does
  `build_tantivy_query` (query-builder, touched by the SEED-006 `content_search` compat work and
  used by `SearchEngine`) belong in `responsa.py` or stay with `genizah_core`/the engine (Phase 125)?
  Map the cluster's call graph and dependencies; pick the boundary that keeps `responsa.py` free of
  engine coupling. The 6 `tests/test_responsa_*.py` files (~3,271 lines) are the regression anchor.
- Exact shim comment wording, import-line placement, AST mechanics of growing `EXTRACTED_MODULES`,
  and which test file holds the per-module identity/smoke tests.
- Resolution mechanics for `_load_ie_volume_map`'s JSON path after the move (it may import from
  `shared.config` — allowed, not a back-edge).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Decomposition strategy (authoritative)
- `.planning/seeds/SEED-020-decomposition-map.md` — full strategy & module-layout map.
  **§7 "Codex review corrections" is authoritative over §0–§6 where they conflict.** Most relevant
  here: §7 **C-2** (responsa cluster = THREE modules — variants/codicological/responsa — because
  `CodicologicalManager.load()` takes `csv_bank` *from MetadataManager*), §7 **C-3** (text-normalize
  helpers + the `shared/local_indexer.py` lazy back-edges at `:3154`/`:3826`), §2 Phase-A table
  (A1–A4 leaf wins + the "A4 utils first" note), §3 conflict/risk map, §6 condensed phase grouping.
- `.planning/ROADMAP.md` → "Phase Details → Phase 123: Core Leaf Modules" — Goal + the **5 success
  criteria** (SC#1 same-object identity for responsa/variants/codicological; SC#2 responsa suite;
  SC#3 local_indexer retarget + GUARD-01 green; SC#4 the 5 source-scanning tests retargeted to both
  locations; SC#5 per-file ruff + full suite at every commit boundary).
- `.planning/REQUIREMENTS.md` — **CORE-01..07** (lines 24–30) + **GUARD-01..04** (lines 13–16) +
  traceability (lines 84–90). CORE-07 explicitly *"closes SEED-020 §7 C-3."*

### Prior-phase context (carry-forward — read for the locked recipe & invariants)
- `.planning/phases/122-config-enabler/122-CONTEXT.md` — D-01 (strict extracted-only parametrized
  GUARD-01), D-02 (retarget-don't-hide back-edges, the precedent for this phase's D-01), D-04 (permanent
  same-object facade), D-06 (verification gate = full suite + per-file ruff, never repo-wide `--fix`).
- `.planning/phases/122-config-enabler/122-CODEX-CRITIQUE.md` — BLOCKER #1 (`__file__`-depth path fix)
  is a precedent for watching path/`__file__`-relative resolution when a symbol changes file depth
  (relevant to `_load_ie_volume_map` in CORE-06).

### Codebase maps (read for patterns/conventions)
- `.planning/codebase/STRUCTURE.md`, `.planning/codebase/CONVENTIONS.md`, `.planning/codebase/CONCERNS.md`.

### Proven recipe precedent (v7.9 + Phase 122)
- The v7.9-extracted modules (`desktop/puzzle.py`, `desktop/viewers.py`, `desktop/dialogs_scholarly.py`,
  `desktop/dialogs_filter.py`, `desktop/vs_cache.py`) and `shared/config.py` (Phase 122) +
  `tests/test_no_back_edges_core.py` (the GUARD-01 guard installed in 122) are the reference
  implementation of the copy-not-move → retarget → `# noqa: F401` same-object re-export shim recipe.

</canonical_refs>

<code_context>
## Existing Code Insights

### Symbol locations in `genizah_core.py` (verified 2026-06-25; line numbers drift — grep, don't trust)
- `NIKUD_PATTERN` :161 · `strip_nikud` :203 · `strip_search_diacritics` :6493 → `text_normalize.py`
- `normalize_shelfmark` :213 · `natural_sort_key` :540 · `get_library_display` :2213 ·
  `_load_ie_volume_map` :2296 · `dedupe_browse_map` :2479 → `browse_map_utils.py`
- `class VariantManager` :2790 · `class CodicologicalManager` :3191 · `class JoinsManager` :10669 ·
  `class ListsManager` :11201 (and `class MetadataManager` :3706 / `class SearchEngine` :7054 are
  **out of scope** — Phases 124/125).
- responsa: `parse_responsa_query` :5889 · `expand_grammatical_prefixes` :6419 · `expand_judeo_arabic`
  :6441 · `expand_grammatical_suffixes` :6661 · `expand_plene_defective` :6689 · `_expand_inline_alternation` :7005.

### Intra-shared dependencies after the split (drive the forced ordering, D-02)
- `CodicologicalManager` calls `natural_sort_key` (:3389, :3545) → `codicological.py` imports from
  `shared.browse_map_utils`.
- `JoinsManager._normalize_shelfmark` wraps module-level `normalize_shelfmark` (:10704–10706, called
  at :10746/10770/10786/10802/10830/10846/10942/10947) → `joins_manager.py` imports from
  `shared.browse_map_utils`.
- `VariantManager` and the responsa functions show **no** leaf-util coupling in their ranges — clean,
  order-independent after the utils.
- **`CodicologicalManager.load(csv_bank=None)` is clean to extract:** `csv_bank` is a method argument
  (default `None`), NOT a module-level `MetadataManager` import (SEED-020 §7 C-2 confirmed) — no
  forward dependency on Phase 124.

### Module-level back-edges into core today (the D-01 retarget set)
- `shared/exclusion_service.py:17` → `from genizah_core import normalize_shelfmark` (**MODULE-LEVEL** —
  retarget to `shared.browse_map_utils`, D-01).
- Lazy (inside functions): `shared/local_indexer.py:3154`/`:3826` (text-normalize, REQUIRED),
  `shared/nli_crossref_service.py:365`/`:376` (`normalize_shelfmark`), `shared/search_serializer.py:248`
  (`get_library_display`) → all retargeted, D-01.
- Lazy, **left on facade** (target symbol not moved this phase): `construct_mosseri_cudl_label` at
  `shared/nli_crossref_service.py:396`, `shared/shelfmark_bridge.py:266`/`:436`.

### Established patterns / constraints
- **copy-not-move → retarget importers → `# noqa: F401` same-object re-export shim**, one atomic
  commit per cluster; `genizah_core.py` keeps the core shims **permanently** (GUARD-04).
- **GUARD-01** (`tests/test_no_back_edges_core.py`, installed Phase 122): strict extracted-only,
  parametrized over `EXTRACTED_MODULES` — add all 7 new modules; each must NOT `import genizah_core`
  at module level.
- **Per-file ruff review only** — repo-wide `ruff --fix` strips the `# noqa: F401` shims.
- **GUARD-03 source-scanning tests** to retarget to both locations before any deletion:
  `test_desktop_folio_navigation.py`, `test_wr01_open_local_browse_page_ast.py`,
  `test_tabular_builder_rtl.py`, `test_view_all_cap.py`, `test_shelfmark_bridge.py` (the last hashes
  `normalize_shelfmark` source — directly affected by CORE-06).

### Test anchors
- responsa: `tests/test_responsa_*.py` (6 files, ~3,271 lines) — green via shim (SC#2).
- joins/lists: `tests/test_*_joins_*.py`, `test_user_lists_cache_isolation.py`,
  `test_recently_viewed_bugs.py`; browse-map via existing browse tests.
- New: per-module identity + standalone-import smoke tests (D-03), mirroring the Phase 122 CONFIG-01 test.

</code_context>

<specifics>
## Specific Ideas

User declined a per-area deep-dive — explicitly confirmed (2026-06-25) that no item here needs a
human decision and asked to lock sensible defaults and proceed straight to planning. The three
decisions above (D-01 retarget-all, D-02 leaf-first/one-commit-per-cluster, D-03 identity tests for
all seven) are the recommended defaults the user approved by saying "Go." This is a mechanical
extraction bounded by SEED-020 §7 + REQUIREMENTS CORE-01..07 + SC#1–5 — no "I want it like X" constraints.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed strictly within phase scope (no scope creep).

### Reviewed Todos (not folded)
The phase-match scan surfaced 8 keyword-only matches, all unrelated **feature** work rather than
mechanical core-leaf extraction — reviewed and **not** folded:
- `2026-02-11-migrate-desktop-corrections-fetch-to-shared-corrections-service.md` (score 0.9) — a
  behavioral migration of a *different* service; not in the 7-cluster set.
- `2026-04-16-reading-desk-ux-fixes.md` (0.9) — desktop UX feature, unrelated.
- `2026-03-07-server-side-search-...`, `2026-03-09-unified-metadata-text-search-...`,
  `2026-03-18-fill-missing-genizah-manuscripts-from-fist.md`, `2026-06-01-one-click-scholarly-citations.md`,
  `2026-06-18-joins-lab-search-results-survive-navigation.md` (0.6) — feature/data work, matched only
  on shared keywords ("genizah"/"core"/"shared"/"search").

</deferred>

---

*Phase: 123-core-leaf-modules*
*Context gathered: 2026-06-25*
