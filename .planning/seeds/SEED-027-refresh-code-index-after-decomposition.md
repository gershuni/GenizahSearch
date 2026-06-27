---
id: SEED-027
status: dormant
planted: 2026-06-26
planted_during: User flagged (Hillel, 2026-06-26) mid-Phase-126 that docs/CODE_INDEX.md is ~2 months stale and the v8.3.0 decomposition is invalidating it. Parked rather than done inline ([[feedback_seed_midphase_fixes_to_cloud]]) — the structure isn't final until Phase 127.
trigger_when: **At v8.3.0 milestone close — AFTER Phase 127** (shims removed, genizah_app.py → desktop/ done, genizah_core.py final facade). Doing it before 127 means redoing it. Natural fit = fold into the Phase 127 closeout, or a standalone /gsd-quick docs pass right after the milestone ships.
scope: medium (regenerate/refresh a 1,958-line file-by-file index for the new shared/ + desktop/ layout; mechanical but large)
---

# SEED-027: Refresh docs/CODE_INDEX.md for the v8.3.0 shared/ + desktop/ layout

> User intent (Hillel, 2026-06-26): "docs/CODE_INDEX.md has not been touched 2 months" — and the
> v8.3.0 God-File Decomposition is reshaping exactly what it indexes.

## Why it's needed
`docs/CODE_INDEX.md` (1,958 lines) is a file-by-file codebase index. Last touched **2026-04-28**
(commit `015b17d5`, Phase 77-05). The v8.3.0 decomposition has already invalidated it and Phase
126/127 will invalidate more:
- **`genizah_core.py` section (~lines 965–1237):** documents the OLD ~12.5K-line god file. It is now a
  **755-line permanent re-export facade**; all the real content moved into ~12 NEW `shared/` modules
  the index does NOT list: `config`, `responsa` (+`variants`,`codicological`), `joins_manager`,
  `lists_manager`, `browse_map_utils`, `metadata_manager`, `indexer`, `lab_settings`, `lab_engine`,
  `search_engine` (+ the pre-existing `thread_local_db`, `search_serializer`). Only 4 stale `shared/`
  mentions exist today.
- **`genizah_app.py` section (~lines 30–964, ~935 lines!):** documents the OLD ~28K-line god file.
  Phase 126 extracts 7 UI panel clusters to `desktop/` (settings_dialogs, ui_widgets, catalog_browse,
  search_results_panel, browse_panel, reading_desk_panel, lists_tab) and Phase 127 removes the shims +
  shrinks `genizah_app.py` to a thin facade (target ≥70% reduction). This whole section will be wrong.
- The **"v7.9 Decomposed Modules" section (~line 1534)** should be renamed/extended to cover the
  v8.3.0 shared/ + desktop/ modules.

CLAUDE.md's "Documentation Maintenance" table already says web pages/components changes → update
`docs/CODE_INDEX.md`; a structural decomposition is the biggest such change in the project's history.

## What to do (at trigger time)
1. **Rewrite the `genizah_core.py` entry** to describe it as a permanent facade and ADD an entry per
   new `shared/` module (one short section each: purpose + key classes/functions). Source of truth =
   the actual `shared/*.py` files + the genizah_core facade shim block.
2. **Rewrite the `genizah_app.py` entry** to describe the post-127 thin facade and ADD an entry per
   new `desktop/` panel module (settings_dialogs, ui_widgets, catalog_browse, search_results_panel,
   browse_panel, reading_desk_panel, lists_tab, update_ui).
3. **Update the "Decomposed Modules" section** header/intro to v8.3.0 (it currently says v7.9).
4. Keep the existing web/ + other entries; verify they're still accurate (low churn there).
5. Update the index's own "last updated" / intro line; run `python scripts/check_docs.py`
   (needs `PYTHONUTF8=1` on Windows — see [[reference_check_docs_utf8]]).

## How (don't hand-maintain 2K lines by eye)
Consider regenerating the file/module skeleton programmatically (walk `shared/`, `desktop/`, `web/`,
top-level `*.py`; emit class/def signatures + first docstring line per file), then hand-curate the
prose. A `gsd-codebase-mapper` / `gsd-doc-writer` agent pass is a good fit. Cross-check against the
v8.3.0 SUMMARY files (`.planning/phases/12{2..7}-*/`) for the authoritative module list.

## Done when
`docs/CODE_INDEX.md` reflects the final v8.3.0 layout: genizah_core.py + genizah_app.py described as
facades, every `shared/` and `desktop/` module indexed, the "Decomposed Modules" section current,
`scripts/check_docs.py` clean, and CLAUDE.md's doc-maintenance expectation satisfied.

## NOT in scope
Touching CODE_INDEX.md mid-decomposition (structure not final until 127); rewriting other docs
(OPEN_ISSUES.md / DOCUMENTATION_INDEX.md are maintained separately); any code change.
