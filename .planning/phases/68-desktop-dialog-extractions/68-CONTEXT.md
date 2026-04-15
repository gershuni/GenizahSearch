# Phase 68: Desktop Dialog Extractions - Context

**Gathered:** 2026-04-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Move 7 leaf `QDialog` classes out of `genizah_app.py` into two new modules under the existing `desktop/` package (created in Phase 67). Zero user-visible behavior change. This is the second extraction in the v7.9 Decomposition milestone and reuses the pattern established by Phase 67.

In scope:
- **`desktop/dialogs_scholarly.py`** (DESK-05): `FjmsBibliographyDialog`, `FjmsCatalogDialog`, `FjmsMeasurementsDialog`, `NliBibliographyDialog`
- **`desktop/dialogs_filter.py`** (DESK-04): `ExcludeDialog`, `DomainFilterDialog`, `PreSearchFilterDialog`
- **`gui_threads.py`**: receive `FilterCountWorker` (QThread currently sharing a module with all three filter dialogs)
- **`desktop/result_dialog.py`**: retarget 4 lazy imports from `genizah_app` to `desktop.dialogs_scholarly`
- **`genizah_app.py`**: replace inline class defs with re-exports for back-compat, delete two same-module self-imports of `FilterCountWorker`
- Expanded smoke tests per slice (see D-10, D-11)

Out of scope:
- Any behavior change, styling tweak, or feature addition
- `TabularQueryBuilderDialog` extraction (different domain — search-builder UI, not filter/scholarly; defer to later phase if a natural home emerges)
- `HelpDialog`, `SearchSettingsDialog`, `SettingsDialog`, `LabScoringDialog`, `WhatsNewDialog`, `UpdateProgressDialog` (application-level, not in DESK-04/DESK-05)
- Extracting a pure filter-count computation service so `web/*` could consume it without pulling PyQt (deferred — see Deferred Ideas)
- ResultDialog Protocol/ABC narrowing (deferred to Phase 71 per Phase 67 D-02)

</domain>

<decisions>
## Implementation Decisions

### Module Placement (Gray Area 1 — `FilterCountWorker` home)
- **D-01:** `FilterCountWorker` (currently `genizah_app.py:6131`, ~60 lines, `QThread` subclass) moves to **`gui_threads.py`** alongside `SearchThread` and the other existing QThread classes. This matches the established convention ("QThread classes live in `gui_threads.py`") and avoids the junk-drawer and back-edge pitfalls of putting it in `desktop/widgets.py` or `desktop/dialogs_filter.py`.
- **D-02:** All three live call sites in `genizah_app.py` (lines 6968 inside `PreSearchFilterDialog`, 21867, 29166) switch to a top-of-file `from gui_threads import FilterCountWorker`. The two same-module self-import lines (`genizah_app.py:28658`, `genizah_app.py:28695` — both literally `from genizah_app import FilterCountWorker` inside genizah_app.py itself) are **deleted outright**; they were working around nothing.
- **D-03:** `genizah_app.py` re-exports `FilterCountWorker` at the module top (`from gui_threads import FilterCountWorker  # noqa: F401` OR via `__all__`) so any external consumer that does `from genizah_app import FilterCountWorker` keeps working. Verified: no `web/`, `tests/`, `shared/`, `scripts/` code currently imports `FilterCountWorker`, so this re-export is purely defensive (and trivially deletable in a future cleanup phase).

### ResultDialog Lazy Imports (Gray Area 2)
- **D-04:** The four lazy (function-local) imports inside `desktop/result_dialog.py` are retargeted to the new scholarly module:
  - line 2394: `from genizah_app import FjmsBibliographyDialog` → `from desktop.dialogs_scholarly import FjmsBibliographyDialog`
  - line 2408: `from genizah_app import NliBibliographyDialog` → `from desktop.dialogs_scholarly import NliBibliographyDialog`
  - line 2432: `from genizah_app import FjmsCatalogDialog` → `from desktop.dialogs_scholarly import FjmsCatalogDialog`
  - line 2455: `from genizah_app import FjmsMeasurementsDialog` → `from desktop.dialogs_scholarly import FjmsMeasurementsDialog`
- **D-05:** The imports stay **function-local** (not hoisted to module top). This keeps blast radius minimal and matches the existing pattern inside `ResultDialog`. The only change is the source module.
- **D-06:** After D-04 + D-05, `desktop/result_dialog.py` no longer imports any of these 4 dialogs from `genizah_app`, eliminating that back-edge. `genizah_app.py` still re-exports them for external callers (see D-07).

### Re-export Strategy in `genizah_app.py` (new — from external AI review)
- **D-07:** `genizah_app.py` replaces each moved class's inline definition with a top-of-file re-export: `from desktop.dialogs_scholarly import FjmsBibliographyDialog, FjmsCatalogDialog, FjmsMeasurementsDialog, NliBibliographyDialog` and `from desktop.dialogs_filter import ExcludeDialog, DomainFilterDialog, PreSearchFilterDialog`. Add `# noqa: F401` OR declare them in a module `__all__` to prevent ruff E/F-family errors (project uses ruff scoped to E9/F401/F811/F821 — F401 is active).
- **D-08:** `desktop/__init__.py` stays a minimal docstring-only package marker. It **does not** become a barrel of re-exports — that reintroduces cycle risk. All imports into `desktop/*` submodules use the full module path (`from desktop.dialogs_filter import …`).

### Scope Boundary (Gray Area 3)
- **D-09:** `TabularQueryBuilderDialog` (`genizah_app.py:8332`, 614 lines, 1 call site at `genizah_app.py:22126`) stays in `genizah_app.py`. It is a search-query builder (Responsa tabular search), not a filter and not scholarly — pulling it into either new module would muddy the boundary. If a natural `desktop/dialogs_search.py` emerges during Phase 72 (search-page-split) or later, revisit then.

### Plan Split (Gray Area 4 — scholarly first, per external AI recommendation)
- **D-10:** **Plan 1: Scholarly slice first.** Extract the 4 scholarly dialogs to `desktop/dialogs_scholarly.py`, retarget the 4 lazy imports in `desktop/result_dialog.py`, and re-export from `genizah_app.py`. This slice is cleaner (no QThread coupling, no self-import cleanup) and immediately kills the `desktop.result_dialog → genizah_app` back-edge. Commits pytest-green.
- **D-11:** **Plan 2: Filter slice.** Move `FilterCountWorker` to `gui_threads.py`, move the 3 filter dialogs to `desktop/dialogs_filter.py`, replace the three legit call sites in `genizah_app.py` with a module-top `from gui_threads import FilterCountWorker`, and **delete** the two bogus self-imports at `genizah_app.py:28658` and `28695`. Re-export the 3 dialogs (and `FilterCountWorker`) from `genizah_app.py`. Commits pytest-green.
- **D-12:** No separate smoke-test plan. Smoke is a **gate inside each plan**, not its own artifact. Executor runs the expanded smoke (D-13, D-14) at the end of each plan and records the result in that plan's SUMMARY.md.

### Verification (Gray Area 4 — expanded per external AI concern)
- **D-13:** **Scholarly slice smoke (end of Plan 1):** launch desktop app → run one basic search → open a result in ResultDialog → from ResultDialog open **one** scholarly dialog (FJMS catalog preferred, whichever is reachable) → close dialog → close ResultDialog → close app. No crash, no visible regression. (The prior Phase 67 smoke of "search → open result → navigate → close" is **insufficient** for Phase 68 — it never touches the moved code paths.)
- **D-14:** **Filter slice smoke (end of Plan 2):** launch desktop app → open `PreSearchFilterDialog`, apply a filter, close → open `DomainFilterDialog` from the search UI, pick a domain, close → open `ExcludeDialog`, add an item, close → re-open the app to verify session-restore path (exercises `FilterCountWorker` at `genizah_app.py:28658` / `28695`-equivalent after cleanup) → close. No crash, no visible regression.
- **D-15:** pytest baseline (1067 passed, 9 skipped as of 2026-04-14, post-Phase 67 confirmed at same counts) must remain green after each plan — no change to counts.
- **D-16:** Import smoke executed after each plan:
  - After Plan 1: `python -c "from desktop.dialogs_scholarly import FjmsBibliographyDialog, FjmsCatalogDialog, FjmsMeasurementsDialog, NliBibliographyDialog; from desktop.result_dialog import ResultDialog; from genizah_app import FjmsBibliographyDialog, FjmsCatalogDialog, FjmsMeasurementsDialog, NliBibliographyDialog, GenizahGUI"` — all succeed.
  - After Plan 2: `python -c "from desktop.dialogs_filter import ExcludeDialog, DomainFilterDialog, PreSearchFilterDialog; from gui_threads import FilterCountWorker; from genizah_app import ExcludeDialog, DomainFilterDialog, PreSearchFilterDialog, FilterCountWorker, GenizahGUI"` — all succeed.
- **D-17:** CI green (Ubuntu + Windows matrix via v7.8 safety net) is the authoritative gate — per-push CI must pass before each plan closes. Windows-case-sensitivity mistakes on `desktop.dialogs_filter` vs `desktop.Dialogs_Filter` etc. will pass locally on Windows but fail on Ubuntu — planner/executor must be exact.

### Parent Coupling — **explicitly unchanged**
- **D-18:** Unlike Phase 67, these 7 dialogs do NOT use `self.parent()`. Verified by grep: zero occurrences across all 7 classes. The Phase 67 `self._app = parent` rename is **not applied** here — no need. The dialogs still couple to their parent via parameters passed to `__init__` (e.g., `ExcludeDialog` reads `parent.meta_mgr`, `PreSearchFilterDialog` calls FJMS services directly) — those couplings stay intact verbatim. "Leaf" here means import-leaf, not pure — do not oversell them as dumb presentational components.

### Claude's Discretion
- Commit granularity within each plan: executor may split into fine-grained commits (e.g., Plan 1 = "create dialogs_scholarly.py skeleton" → "move 4 classes" → "retarget 4 lazy imports" → "add re-exports to genizah_app.py") or bundle, as long as each commit is pytest-green.
- Exact wording of module docstrings for `desktop/dialogs_scholarly.py` and `desktop/dialogs_filter.py`.
- Whether re-exports in `genizah_app.py` use `# noqa: F401` or an explicit `__all__` list — whichever is less noisy for ruff.
- Ordering of the 4 lazy-import retargets inside `desktop/result_dialog.py` — executor picks whatever git-diff reads cleanest.

### Folded Todos
None — the 5 pending todos that matched Phase 68 by keyword all resolve to orthogonal feature work (corrections service migration, NLI MARC crawl, unified metadata search, FIST gap-fill, server-side email search). None overlap with dialog extraction. See `<deferred>` section.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Roadmap & Requirements
- `.planning/ROADMAP.md` — Phase 68 entry; v7.9 milestone boundaries; Phases 69-71 successor context
- `.planning/REQUIREMENTS.md` — DESK-04 (line 14), DESK-05 (line 15), DESK-06, DESK-07 (lines 18-19)
- `.planning/PROJECT.md` — v7.9 Active milestone; dual-app constraint; `desktop/` package established in Phase 67

### Source — Subject of the Phase
Filter dialogs (target module: `desktop/dialogs_filter.py`):
- `genizah_app.py:2674-3183` — `ExcludeDialog` (510 lines, 1 call site at `genizah_app.py:25357`)
- `genizah_app.py:5848-6130` — `DomainFilterDialog` (283 lines, 2 call sites at `genizah_app.py:21529`, `22067`)
- `genizah_app.py:6193-7033` — `PreSearchFilterDialog` (841 lines, 1 call site at `genizah_app.py:21707`)

QThread to relocate (target module: `gui_threads.py`):
- `genizah_app.py:6131-6192` — `FilterCountWorker` (~62 lines, QThread; callers: `6968`, `21867`, `29166`; delete self-imports at `28658`, `28695`)

Scholarly dialogs (target module: `desktop/dialogs_scholarly.py`):
- `genizah_app.py:7034-7233` — `FjmsBibliographyDialog` (200 lines, call sites: `genizah_app.py:14786`, `desktop/result_dialog.py:2395`)
- `genizah_app.py:7234-7921` — `FjmsCatalogDialog` (688 lines, call sites: `genizah_app.py:14826`, `desktop/result_dialog.py:2433`)
- `genizah_app.py:7922-8166` — `FjmsMeasurementsDialog` (245 lines, call sites: `genizah_app.py:14848`, `desktop/result_dialog.py:2456`)
- `genizah_app.py:8167-8331` — `NliBibliographyDialog` (165 lines, call sites: `genizah_app.py:14799`, `desktop/result_dialog.py:2409`)

### Downstream Consumers (must still work after extraction)
- `desktop/result_dialog.py:2394, 2408, 2432, 2455` — four function-local imports that get retargeted in D-04

### Prior Phase Context
- `.planning/phases/67-resultdialog-extraction/67-CONTEXT.md` — established pattern (one-directional imports, module docstrings, Qt lifecycle guards, Windows as deploy target, no new deps)
- `.planning/phases/67-resultdialog-extraction/67-01-SUMMARY.md` through `67-03-SUMMARY.md` — execution evidence and commit chain for the reference pattern

### Existing Siblings (for convention matching)
- `gui_threads.py` — destination for `FilterCountWorker`; study existing `SearchThread` etc. for style
- `desktop/__init__.py` — stays minimal (do NOT turn into re-export barrel)
- `desktop/widgets.py` — do NOT add `FilterCountWorker` here (junk-drawer anti-pattern)
- `desktop/result_dialog.py` — Phase 67 output; consumer of D-04 retargets

### CI & Verification Context
- `.github/workflows/ci.yml` (v7.8 safety net — Ubuntu + Windows matrix; Ubuntu will catch Windows-case-insensitivity mistakes that local Windows hides)
- `tests/` (1067 passing tests, 9 skipped as of Phase 67 close; same counts required at Phase 68 close)
- `docs/OPEN_ISSUES.md` — any new decomposition findings logged here at Phase 76 close

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **`gui_threads.py`** pattern (`SearchThread` and siblings): destination for `FilterCountWorker`. Study the existing module header, class ordering, and `pyqtSignal` placement for consistency.
- **`desktop/widgets.py`** (from Phase 67): stays the home for shared UI helpers only — NOT for threads. Do not extend its purpose.
- **`shared/*`** one-directional import discipline: same rule applies to `desktop/*` — no reverse imports from `genizah_app`.

### Established Patterns
- **One-directional imports**: `desktop/*` does not import from `genizah_app`. D-04 enforces this for the 4 lazy imports inside `desktop/result_dialog.py`.
- **Function-local imports inside Qt dialogs**: the existing pattern in `desktop/result_dialog.py` is function-local lazy imports for optional heavyweight dialogs. D-05 preserves this — only the source module changes.
- **Re-export with `# noqa: F401`**: if any `shared/*` or `web/*` module currently re-exports for back-compat, follow the same suppression style. Otherwise ruff's F401 will fire on the re-exports in `genizah_app.py`.
- **Qt lifecycle guards (`sip.isdeleted()`)** — v6.0.0 added these at Qt lifecycle sites. None of the 7 dialogs currently use `self.parent()` so no rename is needed (D-18), but any existing `sip.isdeleted()` guards must be preserved verbatim during the move.

### Integration Points
- **`genizah_app.py` top imports**: a new block of re-export imports from `desktop.dialogs_scholarly`, `desktop.dialogs_filter`, and `gui_threads` lands at the top of `genizah_app.py`, replacing the moved class definitions.
- **`desktop/result_dialog.py`**: 4 function-local import lines change source module (D-04); no structural change otherwise.
- **`genizah_app.py:28658` and `:28695`**: these two lines get **deleted**. They were `from genizah_app import FilterCountWorker` inside `genizah_app.py` itself — cargo-cult self-imports that only worked because Python tolerates them. The new `from gui_threads import FilterCountWorker` at module top covers these callers.

### Constraints from this Codebase
- **Windows is the deploy target** — CI matrix includes Windows. **Additional risk for Phase 68**: Windows filesystem is case-insensitive by default, so `from desktop.dialogs_filter import …` and `from desktop.Dialogs_Filter import …` both "work" locally but Ubuntu CI will reject the latter. Planner and executor must be exact on module-name casing.
- **No new dependencies** — v7.9 is structural only.
- **ruff scoped ruleset (E9/F401/F811/F821)** — F401 is active. Re-exports in `genizah_app.py` need `# noqa: F401` or `__all__` treatment.
- **No Qt meta-object relocation hazards** — external AI review confirmed no pickling, no monkey-patching tests, no `QMetaObject` tricks. Risk is import-graph and runtime coverage only.

### Known Parent-Surface Coupling (for researcher context; NO rename in this phase)
Grep evidence (from class-body scans) — these 7 dialogs reach their parent through `__init__` parameters, not via `self.parent()`:
- `ExcludeDialog` reaches `parent.meta_mgr`, `parent.lists_mgr`, `parent._normalize_shelfmark`, and others via the `meta_mgr` / `lists_mgr` it receives.
- `PreSearchFilterDialog` reaches FJMS services directly (`from shared.fjms_service import FjmsService`) — not a parent coupling, but a service coupling that moves with the class.
- `DomainFilterDialog` reaches `parent._result_domain_map` and similar via parameters.
- Scholarly dialogs receive FJMS/NLI records as constructor arguments — minimal parent surface.

D-18 leaves all of this verbatim. A Protocol/ABC narrowing can happen in Phase 71.

</code_context>

<specifics>
## Specific Ideas

From external AI (Codex) review — folded into decisions above:
- **Scholarly-slice-first** ordering (D-10/D-11) chosen over "filter first" because scholarly is lower-risk: no QThread, no self-import cleanup, immediate kill of the `desktop.result_dialog → genizah_app` back-edge.
- **Expanded smoke** (D-13/D-14) because the Phase 67 smoke path doesn't touch the moved code.
- **Delete the self-imports** at `genizah_app.py:28658/28695` (D-02) — don't paper over with "moved to gui_threads", actually excise the anti-pattern.
- **"Leaf ≠ pure"** (D-18) — don't oversell these dialogs as stateless presentational components; they still reach into their parent and into FJMS/NLI services.
- **Deferred consideration**: if `web/*` ever wants live filter counts, the real fix is extracting a pure filter-count computation service (not a QThread) into `shared/`. Not a Phase 68 task.

User preference (carried from Phase 67): ask questions in plain English + provide detailed prompts for external AI review; CONTEXT.md stays technical for downstream agents.

</specifics>

<deferred>
## Deferred Ideas

### For Phase 71 (GenizahGUI Consolidation & Smoke Tests)
- **Clean up the `genizah_app.py` re-exports** from D-03, D-07. Once external consumers (if any ever emerge — currently zero for `FilterCountWorker`) migrate, delete the `# noqa: F401` re-exports.
- **Protocol/ABC narrowing of parent surfaces** — same deferral note as Phase 67 D-02. `ExcludeDialog` and `DomainFilterDialog` both reach into `parent.meta_mgr` and similar without any typed contract. Phase 71's job.

### Potential Future Phase (not currently scheduled)
- **Extract a pure filter-count service.** `FilterCountWorker` currently embeds business logic directly in a QThread. If `web/*` ever needs the same "count manuscripts matching filters" behavior (it doesn't today), the right refactor is: pure computation → `shared/filter_count_service.py`; `FilterCountWorker` in `gui_threads.py` becomes a thin QThread wrapper. External AI flagged this; deliberately out of Phase 68 scope since there is no current cross-app consumer.
- **`TabularQueryBuilderDialog` extraction** (D-09). If Phase 72 (search-page-split) surfaces more search-builder UI, a `desktop/dialogs_search.py` module becomes a natural home.

### For Phase 76 (Documentation Close)
- Record `desktop/dialogs_filter.py`, `desktop/dialogs_scholarly.py`, and the `FilterCountWorker` relocation in `docs/CODE_INDEX.md`.
- Update any path references in docs pointing at `genizah_app.py:2674` (ExcludeDialog), `:5848` (DomainFilterDialog), `:6193` (PreSearchFilterDialog), `:7034` (FjmsBib), `:7234` (FjmsCatalog), `:7922` (FjmsMeasurements), `:8167` (NliBib) to the new locations.

### Reviewed Todos (not folded)
- `2026-02-11-migrate-desktop-corrections-fetch-to-shared-corrections-service.md` — matched on keyword "desktop" but is a service-layer refactor (move corrections fetch call from desktop to shared/corrections_service), not a dialog extraction. Orthogonal.
- `2026-03-08-nli-marc-crawl-and-translate.md` — matched on "nli" but is a data pipeline task. Unrelated.
- `2026-03-09-unified-metadata-text-search-with-translations.md` — matched on "fjms, catalog, filter" but is a feature, not an extraction.
- `2026-03-18-fill-missing-genizah-manuscripts-from-fist.md` — matched on "genizah, fjms" but is a data-fill task. Unrelated.
- `2026-03-07-server-side-search-with-email-notification-of-results.md` — matched on "genizah, must" (weak). Unrelated.

</deferred>

---

*Phase: 68-desktop-dialog-extractions*
*Context gathered: 2026-04-15*
