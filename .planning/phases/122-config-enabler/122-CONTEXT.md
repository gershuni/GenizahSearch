# Phase 122: Config Enabler - Context

**Gathered:** 2026-06-25
**Status:** Ready for planning

<domain>
## Phase Boundary

Extract the `Config` class from `genizah_core.py` to a new `shared/config.py`, leaving
`genizah_core.Config` as a permanent re-export of the **same class object**, and install
the permanent GUARD-01 AST back-edge guard (`tests/test_no_back_edges_core.py`).

This is the "Phase 0" cycle-pivot enabler of the v8.3.0 God-File Decomposition milestone:
`Config` must move first because the leaf modules extracted in Phase 123+ bind `Config.*`
at class-definition time, and a module-level `from genizah_core import Config` inside a new
core-logic module that `genizah_core` re-imports would form an import cycle.

**Pure refactor — zero behavior change (GUARD-02).** No user-facing change, no GitHub
Release (label-only version bump for the milestone as a whole).

**In scope:** the `Config` class move + same-object re-export facade + session_persistence
retarget + the GUARD-01 guard test (installed, green, parametrized).
**Out of scope:** moving any other core symbol (`normalize_shelfmark`, managers, engines —
those are Phases 123–125); the desktop split (126–127); any behavior change.

</domain>

<decisions>
## Implementation Decisions

### GUARD-01 enforcement (the permanent back-edge guard)
- **D-01:** GUARD-01 uses the **strict, extracted-only** assertion (per ROADMAP
  success-criterion #3, which is **authoritative over** the looser REQUIREMENTS GUARD-01
  "no cycle" wording where they conflict): **a `shared/` module extracted *this milestone*
  may not `import genizah_core` at module level at all** — not merely "no cycle." The test is
  **parametrized over a registry of milestone-extracted `shared/` modules that grows each
  phase** (adding a new extracted module automatically enters the scan).
  - In Phase 122 the registry = **`{shared/config.py}`** only. `shared/config.py` is
    stdlib-only (no `genizah_core` import), so the guard passes trivially at install time —
    the test is installed green now to lock the invariant for 123–127.
  - **Pre-existing** back-edges are out of scope until their target moves: specifically
    `shared/exclusion_service.py:17 → from genizah_core import normalize_shelfmark` is NOT a
    member of the extracted-this-milestone registry, so it is **not** scanned in Phase 122;
    it is resolved when `normalize_shelfmark` moves to `shared/browse_map_utils.py` in
    Phase 123. Do **not** add `exclusion_service` to the registry in 122.

### Back-edge retargeting
- **D-02:** Retarget `shared/session_persistence.py:32` from `from genizah_core import Config`
  to **`from shared.config import Config`** in Phase 122 (its target has moved; removes one
  real back-edge immediately; every other caller keeps using the `genizah_core.Config`
  facade unchanged). This retarget is for cleanliness and is independent of the guard —
  `session_persistence` is a pre-existing consumer, not in the extracted-module registry.

### Config move (locked upstream — carried forward, do NOT re-litigate)
- **D-03:** **Full move** of the entire `Config` class to `shared/config.py` (not a
  partial "constants only" move) — SEED-020 §7 Q1.
- **D-04:** `genizah_core.Config` is a **re-export of the same class object** — a test
  asserts `shared.config.Config is genizah_core.Config` (identity, not a copy) — CONFIG-01.
  `genizah_core.py` remains a **permanent** compatibility facade (GUARD-04, §7 Q5); the
  `# noqa: F401` re-export shim stays.
- **D-05:** Only the self-contained `Config` class + stdlib imports (`os`, `sys`, `ctypes`)
  travel to `shared/config.py`. The class's helpers (`_pick_writable_dir`,
  `_get_documents_dir`) live **inside** the class body and have no external module-level
  dependencies (verified). **Preserve the load-time side effect** at `genizah_core.py:2372`
  (`os.makedirs(INDEX_DIR, exist_ok=True)` in the class body) — it must run identically on
  `import shared.config`; this is part of "zero behavior change."
  **Refinement (Codex review, 2026-06-25 — see `122-CODEX-CRITIQUE.md` BLOCKER #1):** the copy is
  verbatim EXCEPT the non-frozen `BASE_DIR = os.path.dirname(os.path.abspath(__file__))` line
  (`genizah_core.py:2347`), which becomes `os.path.dirname(os.path.dirname(...))` in
  `shared/config.py` because `__file__` is one dir deeper there — without this, `BASE_DIR` and the
  derived `FILE_V8`/`FILE_V7`/`LIBRARIES_CSV`/`OXFORD_DB`/`HELP_FILE` would point at `…/shared`,
  breaking real callers. This SERVES D-05's zero-behavior-change intent (the literal "verbatim" was
  the trap). The frozen branch (`sys.executable`) is unaffected.
- **D-06:** Verification gate = the **full existing pytest suite** green at the phase
  boundary (GUARD-02). Ruff is **per-file review only** on the extraction commit — never
  repo-wide `ruff --fix` (it would strip the `# noqa: F401` re-export shim) — ROADMAP SC#4.

### Claude's Discretion
- Internal mechanics of `tests/test_no_back_edges_core.py`: AST-walk implementation, how the
  extracted-module registry is represented (explicit list constant vs. a small marker), and
  the exact parametrization shape — planner/researcher's call, as long as D-01 holds.
- Exact shim comment wording and import-line placement in `genizah_core.py`.
- Whether the CONFIG-01 identity assertion lives in a new test file or an existing core test.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Decomposition strategy (authoritative)
- `.planning/seeds/SEED-020-decomposition-map.md` — full strategy & module-layout map.
  **§7 "Codex review corrections" is authoritative over §0–§6 where they conflict.** §7 C-1
  (Config is Phase 0; same-object re-export; the 6-file shared→core importer topology) and
  the §7 adjudicated answers to the 6 open questions are the binding decisions.
- `.planning/ROADMAP.md` → "Phase Details → Phase 122: Config Enabler" — the 4 success
  criteria (SC#3 is the authoritative GUARD-01 wording per D-01).
- `.planning/REQUIREMENTS.md` — CONFIG-01 (line 20) and GUARD-01..04 (lines 13–16) +
  traceability table (lines 79–83). Note: where REQUIREMENTS GUARD-01 ("no cycle") and
  ROADMAP SC#3 ("no module-level import") conflict, **SC#3 / D-01 wins**.

### Codebase maps (read for patterns/conventions)
- `.planning/codebase/STRUCTURE.md`, `.planning/codebase/CONVENTIONS.md`,
  `.planning/codebase/CONCERNS.md` — module layout, extraction conventions, god-file concerns.

### Proven recipe precedent (v7.9)
- The v7.9-extracted modules already in the tree — `desktop/puzzle.py`, `desktop/viewers.py`,
  `desktop/dialogs_scholarly.py`, `desktop/dialogs_filter.py`, `desktop/vs_cache.py` — are the
  reference implementation of the copy-not-move → retarget → `# noqa: F401` re-export shim
  recipe SEED-020 prescribes.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **`Config` class — `genizah_core.py:2295`.** Self-contained: helpers
  `_pick_writable_dir` (:2298) and `_get_documents_dir` (:2318) are defined *inside* the class
  body; all path attributes (`BASE_DIR`, `INDEX_DIR`, `REPORTS_DIR`, `LAB_*`, `LOCAL_*`, etc.)
  resolve in the class body using only stdlib. Clean to lift wholesale into `shared/config.py`.
- **v7.9 extraction precedent** (see canonical refs) — the exact shim+guard pipeline this
  phase bootstraps for core.

### Established Patterns
- **copy-not-move → retarget importers → `# noqa: F401` re-export shim** (one atomic commit
  per cluster); delete-original + AST-guard happen in a *later* phase. Phase 122 keeps the
  `genizah_core.Config` shim **permanently** (it's the facade, not a transitional shim).
- **Per-file ruff review only** — repo-wide `ruff --fix` would gut the `# noqa: F401` shim.

### Integration Points
- **Module-level back-edges into core today (exactly two):**
  - `shared/session_persistence.py:32` → `from genizah_core import Config` → **retarget to
    `shared.config` in this phase (D-02).**
  - `shared/exclusion_service.py:17` → `from genizah_core import normalize_shelfmark` →
    **leave untouched; out of GUARD-01 scope until Phase 123 (D-01).**
- **`Config.*` consumers that stay on the facade in 122** (relevant for *later* phases, not
  to be changed now): `VariantManager` → `Config.VARIANT_GEN_LIMIT`
  (`genizah_core.py:3175, 3240`); `CodicologicalManager` → `Config.OXFORD_DB` (`:3360`);
  responsa explosion guard → `Config.MAX_EXPANDED_TERMS` (`:6965`); `JoinsManager`/`ListsManager`
  bind `Config.INDEX_DIR` at class-definition time (`:10812, :11345`). They keep importing
  `Config` via `genizah_core` and work unchanged through the re-export facade.
- **No back-edge guard exists yet** (`tests/test_no_back_edges_core.py` is new this phase);
  there are 42 `shared/*.py` modules total — but GUARD-01's registry contains only the
  milestone-extracted subset (just `shared/config.py` in 122).
- **Test anchors:** a CONFIG-01 identity test (`shared.config.Config is genizah_core.Config`);
  the new GUARD-01 test (green at install); the full existing suite, including the
  `session_persistence` import path, must pass.

</code_context>

<specifics>
## Specific Ideas

User confirmed the two recommended postures verbatim during discussion:
1. GUARD-01 = strict + extracted-only + parametrized registry (D-01).
2. Retarget `session_persistence` now rather than hiding the back-edge behind the facade (D-02).

No other "I want it like X" constraints — this is a mechanical extraction bounded tightly by
SEED-020 §7 and the ROADMAP success criteria.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed strictly within phase scope (no scope creep; this is the
narrowest, first phase of the decomposition milestone).

### Reviewed Todos (not folded)
The phase-match scan surfaced three weak (score 0.6) keyword-only matches that are unrelated
feature work, not config-extraction refactor — reviewed and **not** folded:
- `2026-02-11-migrate-desktop-corrections-fetch-to-shared-corrections-service.md` (matched on
  "shared"/"genizah").
- `2026-03-07-server-side-search-with-email-notification-of-results.md` (matched on
  "genizah"/"core").
- `2026-03-09-unified-metadata-text-search-with-translations.md` (matched on "shared"/"genizah").

</deferred>

---

*Phase: 122-config-enabler*
*Context gathered: 2026-06-25*
