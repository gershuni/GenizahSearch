# Phase 122: Config Enabler - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-25
**Phase:** 122-config-enabler
**Areas discussed:** GUARD-01 enforcement scope, session_persistence back-edge retarget

> **Note:** This phase is heavily pre-decided. SEED-020 §7 (Codex-reviewed, authoritative)
> already adjudicated all 6 of its open questions, and the ROADMAP success criteria +
> REQUIREMENTS lock the rest. Only two genuinely-open implementation choices remained; both
> were resolved below. (A pre-step also fixed a roadmap-structure bug — duplicate
> `## Phase Details` headers — that prevented the GSD SDK from resolving phases 122–127;
> committed separately as `fbe750bf`.)

---

## GUARD-01 enforcement scope

| Option | Description | Selected |
|--------|-------------|----------|
| Strict, extracted-only | Per ROADMAP SC#3: an extracted-this-milestone `shared/` module may not `import genizah_core` at module level at all (not just cycles); parametrized over a growing registry; pre-existing back-edges out of scope until their target moves. | ✓ |
| Cycle-only | Per REQUIREMENTS GUARD-01 literal wording: only flag a true import cycle. Looser. | |
| Strict over ALL shared/, with allowlist | Scan all 42 `shared/` modules now with a temporary allowlist for `exclusion_service→normalize_shelfmark` that must empty by Phase 123. More upfront rigor. | |

**User's choice:** Strict, extracted-only (recommended).
**Notes:** Resolves the wording discrepancy between REQUIREMENTS GUARD-01 ("no cycle") and
ROADMAP SC#3 ("no module-level import") in favor of SC#3. In Phase 122 the registry is just
`{shared/config.py}` (stdlib-only → guard passes trivially at install). `exclusion_service`
stays out of scope until Phase 123 moves `normalize_shelfmark`.

---

## session_persistence back-edge retarget

| Option | Description | Selected |
|--------|-------------|----------|
| Retarget now | Change `shared/session_persistence.py:32` to `from shared.config import Config` in Phase 122. Removes one real back-edge immediately. | ✓ |
| Leave on facade | Keep `from genizah_core import Config`; works forever via the permanent re-export facade but leaves a hidden back-edge. | |

**User's choice:** Retarget now (recommended).
**Notes:** Independent of the guard (session_persistence isn't in the extracted-module
registry); chosen for cleanliness — don't let the facade hide a back-edge whose target has moved.

---

## Claude's Discretion

- Internal mechanics of `tests/test_no_back_edges_core.py` (AST-walk impl, registry
  representation, parametrization shape).
- Shim comment wording / import-line placement in `genizah_core.py`.
- Whether the CONFIG-01 identity assertion lands in a new or existing test file.

## Deferred Ideas

None — discussion stayed within phase scope. Three weak keyword-only todo matches were
reviewed and not folded (unrelated feature work); recorded in CONTEXT.md `<deferred>`.
