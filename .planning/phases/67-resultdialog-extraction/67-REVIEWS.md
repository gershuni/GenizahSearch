---
phase: 67
reviewers: [gemini, codex]
reviewed_at: 2026-04-15
rounds: 2
plans_reviewed:
  - 67-01-PLAN.md
  - 67-02-PLAN.md
  - 67-03-PLAN.md
notes: |
  Round 1 (2026-04-15): gemini + codex, plans v1 (pre-revision).
  Round 2 (2026-04-15): codex only — gemini rate-limited (429 capacity exhausted on gemini-3.1-pro-preview; stable 2.5-pro retry not completed); claude CLI skipped for independence (running inside Claude Code).
---

# Cross-AI Plan Review — Phase 67: ResultDialog Extraction

---

## Round 1 — Gemini + Codex review of initial plans

### Gemini Round-1 Review

This review evaluates the **Phase 67** implementation plans for extracting `ResultDialog` from the `genizah_app.py` monolith.

#### Summary
The plans are well-structured and adhere to the "one-directional import" architecture required to decompose a 33k-line file. By creating `desktop/widgets.py` as a neutral landing zone for shared UI components, the strategy successfully avoids circular dependencies between the main window (`GenizahGUI`) and its child dialogs. The mechanical refactor of `self.parent()` to `self._app` is a pragmatic middle ground that improves grep-ability without the immediate overhead of defining a full `Protocol` or `ABC`.

#### Strengths
*   **Neutral Dependency Strategy**: Moving `ActionsHoverWidget` and list formatting helpers to `desktop/widgets.py` is the correct way to break the circular dependency between `ResultDialog` and `GenizahGUI`.
*   **Mechanical Consistency**: Using `self._app = parent` and a global find-and-replace for `self.parent()` is safer and more explicit than relying on PyQt's runtime object tree.
*   **Safety Gates**: The 3-line import smoke test and the insistence on keeping the 1067-test baseline green provide high confidence.
*   **One-Way Flow**: Strict prohibition of `from genizah_app import ...` inside the `desktop/` package prevents the "spaghetti" from reforming in a new folder.

#### Concerns
*   **Missing Helper: `ListsTreeWidget` (HIGH)** — Plan 67-01 keeps `ListsTreeWidget` in `genizah_app.py`, but `ResultDialog` (which handles "Add to List") almost certainly uses it. [Note: Codex investigated the actual code and exonerated this concern — ResultDialog does not use ListsTreeWidget. Gemini's concern was a prediction, not evidence.]
*   **Incomplete Import List in `result_dialog.py` (MEDIUM)** — ResultDialog is ~2,800 lines; likely depends on more than the listed modules. Missing candidates: `unified_variants.py`, `sefaria_utils.py`, `shared.image_utils`, `shared.*` services.
*   **Exclusive Helper Discovery (MEDIUM)** — In a 33k-line file, small helpers defined just above/below ResultDialog are likely used only by it. If left behind, `result_dialog.py` breaks.
*   **`self.parent()` → `self._app` Regex (LOW)** — Global find-and-replace might catch `self.parent().parent().` or lambda/inner-function rebinds.
*   **Pickling and Object Identity (LOW)** — Module change from `genizah_app` to `desktop.result_dialog` could break `isinstance` or pickle checks.

#### Suggestions
1. Audit `ListsTreeWidget` usage before Wave 1.
2. Grep the cut block for `(from|import)` and capitalized words to ensure the import block is exhaustive.
3. Refine the self.parent() regex to avoid unrelated `parent` locals.
4. Add `from __future__ import annotations` to both new files.
5. Smoke-test the "Add to List" flow specifically.

#### Risk Assessment
**MEDIUM** — Logic is sound, but scale + interconnectivity make wave 1 likely to hit F821 errors.

---

### Codex Round-1 Review

#### Summary
The plans are directionally right but not executable as written. The phase boundary is good, and the intent to keep `genizah_app.py` as the coordinator is consistent with the milestone, but the actual dependency surface of `ResultDialog` is materially larger than these plans assume. As drafted, wave 2 will either violate D-06, fail `ruff`/pytest, or pass only weak smoke gates while hiding runtime breakage until the manual check.

#### Strengths
- Scope is disciplined at the feature level: no user-visible behavior changes, unchanged call sites, explicit success criteria.
- Creating `desktop/` first and moving neutral helpers before the dialog is sensible.
- The `_app` rename is a pragmatic first-step decoupling choice.
- Keeping `ListsTreeWidget` in `genizah_app.py` looks correct — only GenizahGUI uses it, not ResultDialog.
- The plan correctly tells the executor to search for `class ResultDialog(QDialog):` instead of trusting stale line numbers.

#### Concerns
- **HIGH**: The plan undercounts nontrivial symbols that ResultDialog resolves from `genizah_app.py`, so D-06 is not satisfiable as written. Class directly uses `apply_find_highlight`, `ManuscriptViewerWidget`, `DesktopVSCache`, `ImageLoaderThread`, and title/translation helpers (`_get_title_svc`, `_is_hebrew_text`, `_translate_hebrew_date`, `_resolve_display_title`, `_set_label_with_tooltip`).
- **HIGH**: D-06's allowed-import set conflicts with real code. ResultDialog instantiates `CommentDialog`, `CorrectionsViewerDialog`, `CommentsViewerDialog`, `JoinsDialog` from `corrections_ui`, not listed.
- **HIGH**: Proposed `desktop/result_dialog.py` import header already wrong — omits `threading` (used inside class), includes likely-unused `MetadataManager`, `JoinsManager`, `QThread`, `time` (F401 violations). `desktop/widgets.py` imports unused `QPushButton`.
- **HIGH**: Existing pytest baseline likely breaks — source-based tests in `tests/test_desktop_pending_corrections.py:125` expect `_rd_refresh_versions` and `_rd_load_version_content` to live in `genizah_app.py`.
- **MEDIUM/HIGH**: Wave 2 not a real green checkpoint — 3-line import smoke only proves modules import, not runtime name resolution.
- **MEDIUM**: Import smoke order backwards from real startup path.
- **MEDIUM**: `grep -c` not Windows-safe — conflicts with CLAUDE.md.
- **MEDIUM**: `_format_list_star` is scope creep in wave 1 — not in locked decisions.
- **LOW/MEDIUM**: Browse-only image code inside ResultDialog (`current_browse_sid`, `cancel_browse_image_thread`, `on_browse_img_failed`) suggests deeper entanglement.
- **LOW**: No repo-wide `isinstance(..., ResultDialog)` or pickle usage found.

#### Suggestions
- Add hard prerequisite: inventory every ResultDialog symbol resolving from `genizah_app.py`.
- Amend D-06 to say "never import from `genizah_app.py`" (deny-rule), not a narrow allow-list.
- Update source-based tests as part of the phase.
- Replace `grep -c` with `rg -c` or PowerShell.
- Strengthen wave-2 smoke: two import smokes (one starting with `import genizah_app`) + scripted dialog instantiation.
- Rework wave boundaries: (1) neutral support extraction, (2) ResultDialog move + `_app` rename, (3) manual smoke.
- Drop `_format_list_star` from wave 1.
- Derive import header from post-cut module via ruff, don't hand-author it.

#### Risk Assessment
**HIGH** — First desktop extraction; current plans teach the wrong lesson. Most likely outcome: long debugging session with false sense of safety from passing import smoke, followed by failures in ruff, source-based tests, or manual desktop use.

---

### Round 1 Consensus

Both reviewers reached the same overall conclusion: **the plans are directionally right but materially incomplete**. Codex (deeper code investigation) said HIGH; Gemini said MEDIUM; same root cause.

**Agreed concerns (high priority):** incomplete dependency inventory; hand-authored import header wrong; D-06 allow-list too narrow; source-grep tests will break; wave-2 smoke too weak; `grep -c` not Windows-safe.

**Divergent views settled in favor of Codex's code-based investigation:**
- `ListsTreeWidget` — Gemini predicted HIGH; Codex exonerated (ResultDialog does not use it).
- Pickle/isinstance breakage — Gemini flagged LOW; Codex confirmed no such usage.

---

## Round 2 — Codex review of revised plans

(Gemini unavailable this round: 429 capacity exhausted on gemini-3.1-pro-preview. Claude CLI skipped for independence.)

### Codex Round-2 Findings

Codex investigated the actual code (not just the plan text) to verify Round-1 fixes and surface new issues.

1. **HIGH — Manifest correctness is still the biggest weak point.** The revised cat-(e) list still omits same-file dependencies actually used by ResultDialog:
   - `_get_folio_number_from_shelfmark` (genizah_app.py:181) — used at genizah_app.py:8514, 8686
   - `_get_folio_image_index` (genizah_app.py:208) — used at genizah_app.py:8516, 8692
   - `FjmsBibliographyDialog` (genizah_app.py:10269) — instantiated at genizah_app.py:8400
   - `FjmsCatalogDialog` (genizah_app.py:10469) — instantiated at genizah_app.py:8436
   - `FjmsMeasurementsDialog` (genizah_app.py:11157) — instantiated at genizah_app.py:8458
   - `NliBibliographyDialog` (genizah_app.py:11402) — instantiated at genizah_app.py:8413
   - `_truncate_title` (genizah_app.py:8916) — called inside ResultDialog, companion helper to the moved title cluster
   - `_title_svc_singleton` (genizah_app.py:8925) — module global behind `_get_title_svc`

   **Fix:** make 67-MANIFEST.md AST-derived and exhaustive over `ast.Name` loads in ResultDialog, with explicit dispositions for every unresolved same-module symbol. The current "Python sanity-check one-liner" at the end of Plan 67-01 Task 2 is not sufficient.

2. **HIGH — A clean ruff pass still would not prove this extraction is safe.** ResultDialog's `start_browse_download()` (genizah_app.py:8772) references `self.current_browse_sid`, `self.cancel_browse_image_thread()`, `self.on_browse_img_failed()`, `self.on_browse_img_loaded()` — but those methods live on **GenizahGUI** (around genizah_app.py:31349), NOT on ResultDialog. Ruff does not flag missing runtime attributes on `self`. This is browse-residue bleed: code inside the ResultDialog class that was only ever going to work because `ResultDialog` happened to live in `genizah_app.py` where the browse code also lives and somehow the `self.` lookup finds GenizahGUI methods (this is likely a bug in the current code that just never manifests because of how parent callbacks fire, or a different resolution path).

   **Fix:** explicitly delete or isolate browse-only residue before extraction. Add at least one constructor/method smoke beyond import-only checks — e.g., instantiate ResultDialog with a mock parent in a test, or use `QTest` to drive the browse-download path.

3. **MEDIUM — Lazy `from genizah_app import ManuscriptViewerWidget/DesktopVSCache` exception is workable but incomplete.**
   - `ManuscriptViewerWidget()` is needed during dialog init at genizah_app.py:6497, and `DesktopVSCache()` at genizah_app.py:6652 — the lazy-import trick only works if these instantiations happen inside a method body, not at construction time.
   - The same problem exists for the FJMS/NLI dialog classes and folio helpers, which the plan gives no legal import path.

   **Fix:** either extract those dependencies first into peer modules, or centralize all temporary legacy imports in one documented bridge module (e.g., `desktop/_legacy_bridge.py`) instead of ad-hoc exceptions sprinkled through ResultDialog.

4. **MEDIUM — `desktop/widgets.py` is becoming a junk-drawer module.** Proposed contents mix UI widgets, a network/cache QThread, title/translation logic, and label helpers. `_resolve_display_title` and `ImageLoaderThread` are used well outside ResultDialog and well outside "widgets".

   **Fix:** split into `desktop/title_helpers.py` and `desktop/image_loader.py` at minimum; keep `desktop/widgets.py` for actual widget classes and small UI helpers.

5. **LOW — `ImageLoaderThread` specifically does not belong in a module named `widgets`.** It's a reusable downloader/cacher used by `ManuscriptViewerWidget`, browse, and list-preview code, not a UI widget.

   **Fix:** move to `desktop/image_loader.py`, or rename the destination module to something neutral like `desktop/ui_support.py`.

6. **LOW — Test-file change is directionally right but still brittle.** Tests 6-8 in `tests/test_desktop_pending_corrections.py:126` should switch to the extracted module because `_rd_refresh_versions` / `_rd_load_version_content` are inside ResultDialog; but the file still uses whole-file substring checks for browse behavior.

   **Fix:** keep the source-fixture change, but narrow browse assertions to `_check_document_community_status()` / `_browse_load_version()` instead of full-file `in` checks.

7. **LOW — `self.parent().parent()` edge case is not a real current risk.** There is no `self.parent().parent()` inside the ResultDialog block; the cited uses are elsewhere in `genizah_app.py`.

   **Fix:** drop that special-case note or replace with "verify actual matches before rewrite".

8. **LOW — Revised plan's narrow test update is probably sufficient for today's source.** Browse-side occurrences of `include_drafts=True`, `Pending`, and `corrected_text` still exist in browse code at genizah_app.py:14097 and 14155, so tests 1-5 should still pass after moving ResultDialog.

   **Fix:** state that audit explicitly in the plan, or better, make tests 1-5 method-scoped too.

9. **LOW — Revised plan is correct about `_rd_refresh_versions` / `_rd_load_version_content`.** Both methods are inside ResultDialog at genizah_app.py:6977 and genizah_app.py:7190, not on GenizahGUI.

   **Fix:** update the prose so it explicitly says the earlier contradictory review note was wrong.

10. **MEDIUM — Commit-granularity discipline is still too loose for Wave 2.** Helper moves, the 2.8k-line class cut, and test rewrites are bundled into one wave with pytest only at task boundaries. If the cut breaks imports mid-task, there is no explicit recovery checkpoint.

    **Fix:** split Wave 2 into additive copy, import switch-over, old-class removal, and test-adjustment commits, each gated by ruff + pytest.

### Round 1 Regression Check

| # | Round-1 Concern | Status | Justification |
|---|-----------------|--------|---------------|
| 1 | Dependency manifest missing | ⚠ Partial | Manifest task exists, but planned inventory is still not exhaustive (FJMS/NLI dialogs, folio helpers, `_truncate_title`, `_title_svc_singleton` omitted). |
| 2 | Hand-authored import header wrong | ✅ Resolved | Plan now derives imports via ruff instead of hand-authoring. |
| 3 | D-06 allow-list too narrow | ✅ Resolved | Restated as deny-rule; explicitly allows peer imports like `corrections_ui`. |
| 4 | Specific uninventoried symbols | ⚠ Partial | Named Round-1 symbols are included, but companion deps still missing. |
| 5 | Source-grep tests will break | ⚠ Partial | Reading-desk tests redirected correctly, but remaining browse checks still brittle. |
| 6 | `grep -c` not Windows-safe | ✅ Resolved | Plan uses `rg -c`. |
| 7 | Wave-2 smoke too weak | ⚠ Partial | Better than before, but still import-only and blind to runtime attribute-path failures. |
| 8 | Import-smoke ordering wrong | ✅ Resolved | `import genizah_app` is now first. |
| 9 | `_format_list_star` scope creep | ✅ Resolved | Helper now stays in genizah_app.py. |
| 10 | Browse-only methods inside ResultDialog | ❌ Not Resolved | Plan still copies the class verbatim even though browse residue is still inside it. |

**Score:** 5 ✅ / 4 ⚠ / 1 ❌

### Net New Concerns (introduced by the revision)

- Concrete same-module dependencies newly exposed by the revision are still unplanned: `_get_folio_number_from_shelfmark`, `_get_folio_image_index`, `FjmsBibliographyDialog`, `FjmsCatalogDialog`, `FjmsMeasurementsDialog`, `NliBibliographyDialog`, `_truncate_title`, `_title_svc_singleton`.
- Moving `_resolve_display_title` and `ImageLoaderThread` broadens the blast radius far beyond Phase 67's stated scope.
- `desktop/widgets.py` is being used as an interim dumping ground, which works tactically but weakens the whole v7.9 decomposition goal.

### Round 2 Risk Assessment

**HIGH**

The revision is materially better than Round 1 — the import-order smoke, Windows-safe acceptance commands, `_format_list_star` scope, and reading-desk test ownership are improved. But the plan still has concrete high-severity gaps around dependency completeness and runtime-only breakage paths, so Codex would not call it ready to execute unchanged.

---

## Combined Recommendation

The most important issues that must be resolved before execution:

1. **Make the manifest AST-exhaustive, not shortlist-based.** Replace the sanity-check one-liner in Plan 67-01 Task 2 with an AST walker that enumerates every `ast.Name` load inside the ResultDialog class body and classifies it. Without this, the revision's "derive-then-ruff-verify" loop still starts from a manifest that misses symbols.

2. **Investigate and explicitly handle browse residue.** `self.current_browse_sid`, `self.cancel_browse_image_thread()`, `self.on_browse_img_failed()`, `self.on_browse_img_loaded()` — inside ResultDialog but resolving to GenizahGUI. Either:
   - (a) confirm these are genuinely never-called dead paths inside ResultDialog and delete them before the cut, OR
   - (b) route them through `self._app` (the pattern the rest of the class will use after Plan 67-03), OR
   - (c) discover that they're callbacks wired through Qt signal/slot machinery and add them to the disposition table.

3. **Expand the co-resident move set or grant explicit lazy-import paths.** `FjmsBibliographyDialog`, `FjmsCatalogDialog`, `FjmsMeasurementsDialog`, `NliBibliographyDialog`, `_get_folio_number_from_shelfmark`, `_get_folio_image_index`, `_truncate_title`, `_title_svc_singleton` all need a destination. Either extract them in this phase (expands scope significantly — 4 dialog classes are ~3k lines combined) or document them as Phase 68+ scope with lazy-inline `from genizah_app import X` inside the methods that use them.

4. **Split `desktop/widgets.py`.** `ImageLoaderThread` → `desktop/image_loader.py`. Title/translation helpers → `desktop/title_helpers.py`. Keep `desktop/widgets.py` for widget classes only (`ActionsHoverWidget`, `apply_find_highlight`, `_format_add_to_list_label`).

5. **Finer commit granularity in Wave 2.** Split into 4 atomic commits: (a) additive copy of ResultDialog into `desktop/result_dialog.py` (no delete yet); (b) genizah_app.py import switch-over; (c) old-class removal; (d) test fixture update. Each gated by ruff + pytest.

### Recommended Next Step

Before executing, run one more planner pass to address items 1-5:

```
/gsd-plan-phase 67 --reviews
```

Alternatively, if the scope expansion to move 4 FJMS/NLI dialogs is too big a reach for Phase 67, **consider narrowing Phase 67's scope** to only the package skeleton + dependency manifest + shared helpers (skip the ResultDialog class move to a Phase 67.5 once the dependency graph is fully understood). That would let us ship Wave 1's low-risk changes behind CI and return to the monolith cut with full ammunition.
