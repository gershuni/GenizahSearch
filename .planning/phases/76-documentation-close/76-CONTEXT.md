---
phase: 76-documentation-close
milestone: v7.9
created: 2026-04-17
status: complete
---

# Phase 76 — Documentation Close

## Goal

Ensure project documentation accurately reflects the v7.9 decomposed codebase so future contributors (human + AI) can navigate the new module boundaries. Milestone-level gate: `scripts/check_docs.py` passes green.

## Rationale for a lightweight phase

Phase 76 is pure doc hygiene. All decomposition work landed in Phases 67–74; Phase 75 verified no regressions. Phase 76 has no code changes, no runtime risk, no user-visible impact. The GSD discuss→plan→execute ceremony is overkill here — a single consolidated plan + summary captures the work.

## Success criteria (from ROADMAP §Phase 76)

1. `docs/CODE_INDEX.md` lists all new `desktop/` modules and updated `web/pages/` module structure with accurate descriptions
2. `docs/OPEN_ISSUES.md` includes any decomposition findings, deferred cleanup items, or import-cycle concerns
3. Docs referencing specific file paths / line numbers in `genizah_app.py`, `web/pages/search.py`, or `web/pages/browse.py` are updated to reflect new locations
4. `scripts/check_docs.py` green
5. CI green (inherited milestone gate)

## Approach

- Write `scripts/gen_code_index_section.py` — a small AST-based tool that emits CODE_INDEX-compatible markdown sections for any Python module. Future decomp phases can append module sections in one command.
- Run the generator for the 15 new v7.9 modules and append the output as a "v7.9 Decomposed Modules" section to `docs/CODE_INDEX.md`. Preserve the existing curated sections untouched.
- Update the `CODE_INDEX.md` header: bump `Last updated` date, document the generator, list v7.9 additions, and note that older sections may have shifted line numbers post-decomposition.
- Audit `docs/OPEN_ISSUES.md` for stale file:line refs in currently-open issues. Historical "Fixed" rows intentionally preserve their pre-decomposition file:line anchors (historical record, not navigation aid).
- `scripts/check_docs.py` run green before and after.

## Out of scope

- Regenerating or cleaning up the pre-v7.9 `## genizah_app.py` / `## web/pages/search.py` / `## web/pages/browse.py` sections. Line numbers there have shifted post-decomposition; the header now warns readers to treat them as intent guides rather than current anchors. A full re-index is future work (would need the generator extended to handle the 22K-line `genizah_app.py` cleanly).
- OPEN_ISSUES.md Phase 70 partial-extraction follow-up (class duplication between `genizah_app.py` and `desktop/puzzle.py`): already tracked in OPEN_ISSUES.md P1 row (fixed 2026-04-16 with a note that final cleanup is follow-up work). Not a Phase 76 gap.

## Artifacts

- `scripts/gen_code_index_section.py` (new, ~80 lines)
- `docs/CODE_INDEX.md` (header rewrite + appended "v7.9 Decomposed Modules" section, +~450 lines)
- `76-01-SUMMARY.md` (this phase's self-check)
