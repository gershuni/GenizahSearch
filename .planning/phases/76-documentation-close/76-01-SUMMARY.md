---
plan: 76-01
phase: 76-documentation-close
status: complete
outcome: passed
date: 2026-04-17
milestone: v7.9
---

# Plan 76-01 Summary — Documentation Close

## Self-Check: PASSED

All four success criteria from ROADMAP §Phase 76 satisfied.

| # | Criterion | Evidence |
|---|-----------|----------|
| 1 | `CODE_INDEX.md` lists all new `desktop/` + updated `web/pages/` modules | `docs/CODE_INDEX.md` has new "v7.9 Decomposed Modules" section listing all 15 new modules with AST-derived class/function indexes |
| 2 | `OPEN_ISSUES.md` covers decomposition findings | 5 decomp-related entries already present: Phase 70 partial puzzle extraction (P1, fixed), browse rotation TypeError (P2, fixed), browse title toggle RTL (P2, fixed), web search Export checkbox (P2, open, logged by 75-02), JTS DPUL switch (P2, open, logged by 75-02). Additionally 2 desktop perf issues surfaced by 75-02 surface 3 are logged as pre-existing P2. No import-cycle concerns surfaced; no new decomp findings require additional entries. |
| 3 | Stale file:line refs updated | `grep` of currently-open issues returned zero stale refs to `genizah_app.py` / `web/pages/search.py` / `web/pages/browse.py`. Stale refs exist only in historical "✅ Fixed" rows, which correctly preserve the file:line at time of fix (historical record, not navigation aid). `CODE_INDEX.md` header now warns readers that pre-v7.9 sections may have shifted line numbers. |
| 4 | `scripts/check_docs.py` green | Verified before and after (`All checks passed! Documentation is healthy.`). Critical documents exist, no outdated terms, all docs fresh within 90 days, all internal links valid. |

## Files modified

- `scripts/gen_code_index_section.py` (new, ~80 lines) — AST-based CODE_INDEX section generator; usage `python scripts/gen_code_index_section.py <file.py> ...`. Handles top-level functions, classes with methods/properties, docstring first-line snippets.
- `docs/CODE_INDEX.md` (+~470 lines) — header rewritten to document the generator, list v7.9 additions, and flag the pre-v7.9 sections as possibly line-shifted. New "v7.9 Decomposed Modules" section appended with indexes for: `desktop/__init__.py`, `desktop/widgets.py`, `desktop/title_helpers.py`, `desktop/image_loader.py`, `desktop/result_dialog.py`, `desktop/dialogs_filter.py`, `desktop/dialogs_scholarly.py`, `desktop/viewers.py`, `desktop/puzzle.py`, `desktop/vs_cache.py`, `web/pages/search_state.py`, `web/pages/search_results.py`, `web/pages/browse_state.py`, `web/pages/browse_enrichment.py`, `web/search_bootstrap.py`.

## Approach

Rather than manually writing 15 module sections, wrote a small AST-based generator (`scripts/gen_code_index_section.py`). This makes future decomposition phases cheaper: `python scripts/gen_code_index_section.py path/to/new_module.py >> docs/CODE_INDEX.md` and the new module is indexed. The generator preserves the existing CODE_INDEX style (bullets, `(Line N)` suffix, docstring first-line `— ...` snippets, `Method` vs `Property` discrimination).

The pre-v7.9 sections (`## genizah_app.py`, `## web/pages/search.py`, `## web/pages/browse.py`) were left untouched. Their line numbers have drifted post-decomposition, but regenerating them would require extending the generator to handle the 22K-line `genizah_app.py` which still contains significant inline code (PuzzleCanvasWindow, GenizahGUI, etc.). Out of Phase 76 scope. The CODE_INDEX header now flags this explicitly for readers.

## Commits

- (this commit) `docs(76): close v7.9 milestone — CODE_INDEX.md generator + v7.9 module sections + mark Phase 76 complete`

## Next step

Milestone v7.9 is now complete. Per user request ("release after we close the milestone"), the release handoff follows: kick off the `/release` skill to version-bump, draft What's New, build, deploy, and cut a GitHub release. Release bundles the v7.9 decomposition work plus the 75-03 back-nav fix (user-visible bugfix closing a regression that had shipped to production in commit 829cd7cf on 2026-03-27).
