# Phase 68: Desktop Dialog Extractions - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in `68-CONTEXT.md` — this log preserves the alternatives considered.

**Date:** 2026-04-15
**Phase:** 68-desktop-dialog-extractions
**Areas discussed:** FilterCountWorker home, ResultDialog lazy imports, TabularQueryBuilderDialog scope, Plan split strategy

---

## Discussion Style

User preference (carried from Phase 67): questions in plain English with pros/cons, plus a self-contained prompt for external AI (Codex / Gemini CLI) review. Decisions finalized after external AI review. Below, "Selected" means the final locked decision (after external AI review), "Claude's recommendation" means what was offered to the user before external AI weighed in.

---

## Gray Area 1: Where should `FilterCountWorker` live?

| Option | Description | Selected |
|--------|-------------|----------|
| A. desktop/dialogs_filter.py | Keep next to PreSearchFilterDialog (primary user). Related code stays together but module name mismatches content. | |
| B. gui_threads.py | Alongside SearchThread and other QThread classes. Matches established convention. | ✓ |
| C. desktop/widgets.py | Neutral shared desktop module. Mixes QThread with UI helpers. | |

**Claude's recommendation:** B (gui_threads.py).
**External AI (Codex) verdict:** B. Reasoning: `FilterCountWorker` is not private to the dialog — called from 3 module-level sites in genizah_app.py plus 2 bogus self-imports. Putting a QThread in `desktop/widgets.py` is junk-drawer; parking it next to the dialog would make genizah_app import a shared worker from a dialog module. Shared QThreads → `gui_threads.py`. Codex also corrected an assumption: I wrote "re-imported by web/*" in the prompt, but in fact `web/`, `tests/`, `shared/`, `scripts/` have **zero** imports of FilterCountWorker. Verified post-hoc with grep.

**Final decision:** B. Move `FilterCountWorker` to `gui_threads.py`. Delete the two same-module self-imports at `genizah_app.py:28658` and `genizah_app.py:28695` (they self-import from the same file the class is defined in — cargo-cult). Keep a `# noqa: F401` re-export in genizah_app.py defensively, even though no external consumers exist today.

---

## Gray Area 2: Update `desktop/result_dialog.py`'s lazy imports now?

| Option | Description | Selected |
|--------|-------------|----------|
| A. Update now | Retarget 4 lazy imports from `genizah_app` to `desktop.dialogs_scholarly`. Removes back-edge immediately. | ✓ |
| B. Leave as-is | genizah_app.py re-exports make it work; less diff this phase. | |

**Claude's recommendation:** A.
**External AI (Codex) verdict:** A. Keep imports function-local (don't hoist to module top — minimal blast radius), only change target module. Those 4 lazy imports are an avoidable back-edge from `desktop/` into `genizah_app`; internal desktop code should import the real home, and genizah_app should only re-export for compatibility with outside consumers.

**Final decision:** A. Retarget the 4 function-local imports to `desktop.dialogs_scholarly`. Keep them function-local.

---

## Gray Area 3: Is `TabularQueryBuilderDialog` in scope?

| Option | Description | Selected |
|--------|-------------|----------|
| A. Out of scope | Leave in genizah_app.py. Matches exact roadmap wording. | ✓ |
| B. Fold into dialogs_filter.py | Gets it out of monolith now; stretches "filter" semantics. | |
| C. Third module dialogs_search.py | Cleanest grouping; overkill for one dialog. | |

**Claude's recommendation:** A.
**External AI (Codex) verdict:** A. It's search-builder UI, not filter UI and not scholarly UI, with a single call site. Pulling it in now muddies the module boundary for no architectural win. If a natural `desktop/dialogs_search.py` emerges in Phase 72 (search-page-split), revisit then.

**Final decision:** A. Out of scope for Phase 68. Deferred consideration noted in `68-CONTEXT.md`.

---

## Gray Area 4: Plan split strategy

| Option | Description | Selected |
|--------|-------------|----------|
| A. One combined plan | All 7 dialogs at once. Big diff, harder to bisect. | |
| B. Two plans: filter first, scholarly second | Each plan commits green. | |
| B-reversed. Two plans: scholarly first, filter second | Lower-risk slice first, immediate back-edge removal. | ✓ |
| C. Three plans: filter, scholarly, smoke | Smoke as its own artifact. | |

**Claude's recommendation:** B (two plans, filter first then scholarly). Option C (three plans with smoke as its own plan) was presented but not recommended — Claude listed it for completeness, not as the top choice.
**External AI (Codex) verdict:** Reverse the order — scholarly first, filter second. Reasoning:
- Scholarly slice is cleaner: 4 dialogs, 2 call sites each, no QThread, no self-import cleanup.
- Eliminates the `desktop.result_dialog → genizah_app` back-edge immediately (Phase 67's acknowledged debt).
- Filter slice is messier: `FilterCountWorker` relocation + self-import deletion + restore-from-session path exercise.
- Don't make smoke its own plan — make it an explicit gate/checklist at the end of each slice.

**Final decision:** B-reversed. Two plans. Plan 1 = scholarly. Plan 2 = filter + FilterCountWorker + self-import cleanup. Expanded smoke is a gate inside each plan (not a separate plan).

---

## What External AI Surfaced That We Missed

User explicitly asked the external AI prompt to flag "things we're not considering." Codex contributed:

1. **Old smoke test is insufficient.** Phase 67's "search → open result → navigate → close" path never touches the moved Phase 68 code. Expanded smoke required: open PreSearchFilterDialog, open DomainFilterDialog, open ExcludeDialog, open one scholarly dialog from ResultDialog.
2. **Bogus self-imports.** `genizah_app.py:28658` and `:28695` literally `from genizah_app import FilterCountWorker` inside `genizah_app.py`. Delete, don't paper over.
3. **Re-export lint traps.** Ruff's F401 (active in project scoped ruleset) fires on unused-imports-for-re-export. Need `# noqa: F401` or `__all__`.
4. **Windows hides case mistakes.** Local Windows dev will import `desktop.Dialogs_Filter` happily; Ubuntu CI rejects. Be exact.
5. **Don't barrel `desktop/__init__.py`.** Re-exporting everything through the package init reintroduces cycle risk.
6. **No Qt meta-object relocation hazard** (confirmed no pickling, no monkey-patching tests, no meta-object tricks).
7. **"Leaf" here means import-leaf, not pure.** ExcludeDialog uses `parent.meta_mgr`; PreSearchFilterDialog calls FJMS directly. Don't oversell these as stateless presentational components.
8. **Bigger future refactor** (deferred): the real fix for cross-app filter-count logic is extracting a pure computation service into `shared/`, with `FilterCountWorker` as a thin QThread wrapper. Not a Phase 68 task.

---

## Claude's Discretion

- Commit granularity within each plan (executor picks).
- Module docstring wording for the two new `desktop/dialogs_*.py` modules.
- Whether genizah_app.py re-exports use `# noqa: F401` or `__all__`.
- Ordering of the 4 lazy-import retargets inside `desktop/result_dialog.py`.

## Deferred Ideas

- Extract a pure filter-count computation service to `shared/` (not Phase 68 — no current web consumer).
- `TabularQueryBuilderDialog` extraction — revisit during Phase 72 (search-page-split).
- Protocol/ABC narrowing of parent surfaces — Phase 71 (GenizahGUI consolidation).
- Clean up `genizah_app.py` re-exports once external consumers migrate — Phase 71.
- `CODE_INDEX.md` updates for new module paths — Phase 76 (documentation close).
