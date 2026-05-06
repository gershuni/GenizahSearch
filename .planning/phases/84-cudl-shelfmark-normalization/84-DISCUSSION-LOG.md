# Phase 84: CUDL Shelfmark Normalization - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in `84-CONTEXT.md` — this log preserves the alternatives considered.

**Date:** 2026-05-06
**Phase:** 84-cudl-shelfmark-normalization
**Areas discussed:** Normalizer architecture, Mosseri reverse mapping, Or. + dot/zero/comma rules, Wiring + tests

---

## Normalizer Architecture

**Q1:** Where should the new CUDL-bridge normalization rules live?

| Option | Description | Selected |
|--------|-------------|----------|
| Layered: bridge module | New `shared/shelfmark_bridge.py`; canonical untouched. | ✓ |
| Extend canonical | Absorb new rules into `genizah_core.normalize_shelfmark()`. | |
| Replace ad-hoc copy | Delete bespoke normalize() in scan_cudl_orphans.py and import from bridge. | |

**User's choice:** Layered: bridge module
**Notes:** Lowest regression risk for the 25 existing canonical-normalizer callers.

**Q2:** How should the canonical (`genizah_core.normalize_shelfmark`) function evolve?

| Option | Description | Selected |
|--------|-------------|----------|
| Untouched | No changes this phase. | ✓ |
| Add aliases only | Keep rules as-is; add new prefix aliases. | |
| You decide | Defer to planning. | |

**User's choice:** Untouched
**Notes:** Canonical normalizer is frozen. Bridge is the only place new rules live.

---

## Mosseri Reverse Mapping

**Q1:** How should `mosseriiii27o` resolve back to `Moss. III,27O`?

| Option | Description | Selected |
|--------|-------------|----------|
| Pre-built alias index | Forward-parse Mosseri rows at startup; build dict. | ✓ |
| Reverse parser | Write parse_cudl_mosseri_label(). | |
| Both | Index plus reverse parser as fallback. | |

**User's choice:** Pre-built alias index
**Notes:** Reuses existing forward `construct_mosseri_cudl_label()`. ~3.9K entries.

**Q2:** Where should the Mosseri alias resolve to (search/browse target)?

| Option | Description | Selected |
|--------|-------------|----------|
| sys_id direct | Bridge returns sys_id. | |
| Canonical shelfmark string | Bridge returns `Moss. III,27O`. | |
| You decide | Claude picks during planning. | ✓ |

**User's choice:** You decide
**Notes:** Recorded as Claude's discretion (D-04). Decide per call-site fit during planning.

---

## Or. + Dot/Zero/Comma Rules

**Q1:** How aggressive should NORM-02 Or. coverage be?

| Option | Description | Selected |
|--------|-------------|----------|
| Both patterns, full coverage | Letter-suffix AND numeric-collapse, push toward Mosseri parity. | ✓ |
| Letter-suffix only | Defer numeric-collapse. | |
| Best-effort + audit | Iterate based on Phase 86 residue. | |

**User's choice:** Both patterns, full coverage
**Notes:** Documented residue goes to Phase 86's reports/cudl_coverage.md, not this phase.

**Q2:** How should NORM-03 leading-zero collisions be handled?

| Option | Description | Selected |
|--------|-------------|----------|
| Audit-first, fail-loud | One-shot audit before enabling; exclude collision keys, log them. | ✓ |
| Strip universally | Trust shelfmark structure. | |
| Strip only inside numeric segments | Match orphan-scanner's current rule. | |

**User's choice:** Audit-first, fail-loud
**Notes:** Never silently merge two distinct fragments.

---

## Wiring + Tests

**Q1 (initial):** Which call sites should the bridge plug into?

User selected "Not sure" → Claude expanded each site with user-impact framing.

**Q1 (re-asked):** Now that the four sites are clearer, which should the bridge plug into?

| Option | Description | Selected |
|--------|-------------|----------|
| All four (recommended) | Search + external link + image panel + orphan-scanner. | ✓ |
| User-facing three (1+2+3) | Skip orphan-scanner unification. | |
| Search only | Defer link + image panel. | |
| You decide | Defer to planning. | |

**User's choice:** All four
**Notes:** Sites 1–3 are the actual NORM-01/02 user value; site 4 keeps audit consistent with runtime.

**Q2:** What's the regression-guard strategy for NORM-04?

| Option | Description | Selected |
|--------|-------------|----------|
| Both: golden + scan diff | Golden fixture (~50 classmarks) + before/after scan + canonical-untouched assertion. | ✓ |
| Golden CSV only | Lighter, faster CI. | |
| Scan-diff only | Captures 140K guard but doesn't pin specific classmarks. | |

**User's choice:** Both: golden + scan diff
**Notes:** Adds canonical-untouched pytest assertion as third layer (covers NORM-04 for non-CUL rows too).

---

## Claude's Discretion

- D-04: Whether the bridge returns sys_id directly or canonical shelfmark string for the alias-resolution path.
- Internal organization of `shared/shelfmark_bridge.py` (class vs module-level dict, cache invalidation strategy).
- Exact set of Or. classmark patterns beyond letter-suffix and numeric-collapse if the orphan-with-neighbor scan reveals additional regular structures.

## Deferred Ideas

- Synthetic libraries.csv rows for FJMS-only inventories → Phase 85.
- `reports/cudl_coverage.md` and AUDIT-03 regression pass → Phase 86.
- Extending canonical `normalize_shelfmark()` to absorb bridge rules — rejected for this phase, may revisit post-milestone.
- Mosseri "2nd series" (`Ms. L 241`) and `Ms. MOSS NS` patterns — handle in a follow-up if they surface in Phase 86 residue.
