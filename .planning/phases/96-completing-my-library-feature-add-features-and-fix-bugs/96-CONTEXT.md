# Phase 96: Completing My Library feature: add features and fix bugs - Context

**Gathered:** 2026-05-24
**Status:** Ready for planning

<domain>
## Phase Boundary

Take Phase 95 (My Library) from "shipped MVP" (v7.14.0 public release, 2026-05-24) to "feature-complete" by closing the P1 highlight regression, the confirmed PDF-extraction bug, two new UX gaps surfaced post-release, and adding the per-file opt-in/out drill-down feature.

**In scope:**
- D-F5 — Search-term highlighting works for LOCAL hits (P1 regression)
- D-F4 — PDF extraction no longer produces one-word-per-line output for affected PDFs
- D-F1 — Per-file checkbox opt-in/out drill-down in My Library tab
- NEW-1 — Remove the redundant `צפה בדפדוף` button from ResultDialog for LOCAL hits
- NEW-2 — Next/prev navigation + "View All" (הכל) for LOCAL in ResultDialog + Browse panel
- NEW-3 — Freestyle / Claude's-discretion bucket for on-the-fly bugs surfaced during the phase (capped, not a blank check)

**Out of scope (defer to v7.15+ via existing OPEN_ISSUES entries):**
- D-F2 — PDF OCR (Tesseract / cloud) for scanned image-only PDFs
- D-F3 — Side-by-side PDF page rendering in Browse / ResultDialog

**Carrying forward from Phase 95 (locked invariants — DO NOT break):**
- LOCAL side-index is **separate** from Genizah corpus; merged into main search via RRF k=60 **POST**-`_deduplicate()` (Codex D-08 P0)
- Three cloud-write gates pinned at **TOP** of `shared/search_serializer.py`, `corrections_client.py`, `lists_sync.sync_item_to_cloud/sync_list_to_cloud` (Codex D-30 P0)
- Desktop-only feature surface; web LIBRARY_CODES invariant pinned by `tests/test_web_library_options_no_local.py` static AST guard (allowlist `[]`)
- LOCAL row shape: `Library = parent/folder`, `Shelfmark = filename`
- v7.12 multitenant invariants carry forward (zero raw `app.storage.user` under `web/`)
- `shared/export_dossier.py` `skip_local` kwarg — web excludes LOCAL, desktop includes LOCAL

</domain>

<decisions>
## Implementation Decisions

### Scope Selection
- **D-01:** Phase 96 ships D-F5 (P1) + D-F4 + D-F1 + NEW-1 + NEW-2. D-F2 (OCR) and D-F3 (side-by-side PDF rendering) are explicitly deferred to v7.15+ — keep them as OPEN_ISSUES entries.
- **D-02:** A freestyle / "fixed-as-encountered" bucket is allowed inside the phase for small bugs the user surfaces during smoke testing. The bucket is capped at "small fixes only" — anything that materially expands scope must become a new phase or `/gsd-plant-seed` entry. Planner should leave room for this in the wave structure (e.g., a trailing polish wave).

### D-F5 — LOCAL Highlighting (P1)
- **D-03:** Approach is **investigate-first**. The planner/researcher MUST scout the highlight pipeline (both the search table and `ResultDialog`) to identify where Genizah-corpus hits get highlighted but LOCAL hits don't. Likely candidate: highlighter keys on a field that exists in V0.8/V0.7 hit dicts but not in `_build_local_result_dict` output. Choose between "normalize LOCAL hit dict shape" vs. "per-source branch in highlight pipeline" AFTER the scout — record the choice in the plan, do not pre-commit now.
- **D-04:** Highlighting MUST be **regex-aware** for LOCAL — same two-phase (Tantivy candidates → regex filter+highlight) model the Genizah corpus uses. No substring-only shortcut. Consistency over ease.

### D-F4 — PDF Extraction Quality
- **D-05:** Fix the one-word-per-line bug using a **detect-then-fallback** strategy: keep `get_text("blocks")` as primary, detect pathological output (e.g., >80% of lines have ≤1 word), fall back to `get_text("text")` (or other PyMuPDF mode chosen during planning). Preserves currently-working PDFs.
- **D-06:** Validate the fix against a small **representative sample of PDFs** — at minimum the existing `tests/fixtures/local_indexer/single_word_per_line.pdf` regression fixture PLUS a handful of user-supplied PDFs that currently extract cleanly (regression coverage in both directions: bad → good AND good → still good). Not a full audit-first sweep.

### D-F1 — Folder Drill-down (Per-file Opt-in/Out)
- **D-07:** Reuse the **existing vertical split panel** in `MyLibraryTab` (top = folder list as today). The **bottom panel becomes a new horizontal split**:
  - **Left** — a tree widget showing the selected folder's subfolders (if any) and files, each with a checkbox. Folder-level checkbox is tri-state (all / some / none).
  - **Right** — the file-status output that already lives in the bottom panel during scans (move it, don't duplicate).
- **D-08 (REVISED 2026-05-24):** Per-file opt-out state persists via **session JSON** (`shared/session_persistence.py` — the existing pattern). Rationale: user explicitly noted "User may want each search to select another file" — session JSON survives across app restarts so selections aren't lost AND the UI toggle is fast enough that users can flip files between searches without friction. Stored alongside the other ~20 LOCAL filter keys (`local_filter`, `local_filter_composition`, `local_filter_parallels`, `domain_exclusions`, etc.) under a new key (e.g. `local_file_optouts`) holding a list of canonical file paths.
  - **Original D-08:** "QSettings". Revised after researcher surfaced that ALL other LOCAL filter persistence already lives in session JSON — QSettings would create two persistence stores for the same feature family. Same persistence/restart properties; one store instead of two. Not SQLite-cache-coupled — keeping persistence out of the indexer cache lifecycle.
- **D-09:** When the indexer rescans a folder, opt-out state for files that still exist MUST be preserved. Removed files drop their state.
- **D-10:** Opt-out filtering is applied at **query time**, not index-build time. Files stay indexed; the filter excludes them from search results. This matches the existing three-state LOCAL filter pattern (Phase 95 D-39).

### NEW-1 — Remove Redundant `צפה בדפדוף` Button
- **D-11:** The `צפה בדפדוף` button (wrong translation + redundant with the existing `עיין` Browse button) is **only present on LOCAL hits** today, not Genizah hits. Remove it for LOCAL hits. No project-wide audit needed — Genizah-hit UI stays untouched.

### NEW-2 — Next/Prev Navigation + "View All" (הכל) for LOCAL
- **D-12:** Next/prev navigation in LOCAL is **format-aware**:
  - **PDF** → next/prev **page** (one page = one navigable unit)
  - **txt / docx** → next/prev **chunk** (the same chunk unit the indexer uses)
  - When a file has only one page/chunk, the buttons are disabled (no wrap).
- **D-13:** Navigation appears in **two places**: `ResultDialog` (mirroring how Genizah ResultDialog navigates fl_ids) AND the **Browse panel** (mirroring how Genizah Browse navigates folios). NOT in the search results table row (that would be a new pattern Genizah hits don't have either).
- **D-14:** "View All" (הכל) in Browse for LOCAL hits = **full file text, all chunks concatenated** in one continuous scrollable view. Analog: viewing all folios of a manuscript at once. Page/chunk boundaries should remain visible (e.g., a thin separator labeled `— page 2 —` or `— chunk 2 —`) so users still know where the boundaries are.

### NEW-3 — Freestyle Bug-Fix Bucket
- **D-15:** Small bugs surfaced during smoke testing of this phase can be fixed inline without re-planning. Anything large or scope-expanding must become a new phase. Planner should anticipate this and leave a trailing polish wave.

### Claude's Discretion
- D-F5 normalize-vs-branch choice (after the scout — see D-03)
- D-F4 exact PyMuPDF fallback mode (`get_text("text")` is the first attempt — see D-05)
- Tree widget exact PyQt6 class (`QTreeWidget` vs `QTreeView+model`)
- Tri-state checkbox styling and label conventions
- Page/chunk separator visual style for "View All"

### Folded Todos
*(None — todos matched by keyword from `gsd-sdk query todo.match-phase` were unrelated to My Library; nothing folded.)*

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 95 (My Library) — Locked decisions and invariants this phase must NOT break
- `.planning/phases/95-my-library/95-CONTEXT.md` — 46 locked decisions (D-01..D-46); especially D-08 (RRF POST-dedup), D-13 (parser generalization), D-30 (gate at TOP of cloud-write functions), D-46 (web LIBRARY_CODES AST guard)
- `.planning/phases/95-my-library/95-SPEC.md` — 10 requirements + 22 acceptance criteria
- `.planning/phases/95-my-library/95-VERIFICATION.md` — what was actually verified at Phase 95 close
- `.planning/phases/95-my-library/95-PATTERNS.md` — patterns Phase 95 established that Phase 96 should follow

### Open Issues — source of D-F1..D-F5 scope
- `docs/OPEN_ISSUES.md` §"Deferred to v7.15+ (Phase 95 follow-up backlog)" — D-F1..D-F5 entries (lines 458-469). Mark D-F5, D-F4, D-F1 as ✅ Fixed when Phase 96 closes; leave D-F2 + D-F3 as still-deferred.

### Code surfaces touched by Phase 96
- `desktop/my_library_tab.py` — MyLibraryTab vertical split panel; bottom panel restructure for D-F1
- `shared/local_indexer.py` — PyMuPDF text extraction (D-F4 fix lives here); SQLite cache schema (any opt-out persistence boundary)
- `genizah_app.py` — `ResultDialog` (NEW-1 button removal, D-F5 highlight, NEW-2 next/prev), Browse panel (NEW-2 next/prev + View All)
- `gui_threads.py` — SearchThread + LocalIndexerWorker (relevant if highlight pipeline runs there)
- `genizah_core.py` — main search merger (RRF k=60); highlight pipeline call sites
- `shared/search_serializer.py`, `corrections_client.py`, `lists_sync.py` — cloud-write gates (DO NOT touch the gates; verify they're still TOP-of-function after any refactor)
- `tests/test_web_library_options_no_local.py` — web LIBRARY_CODES AST guard (must stay green)
- `tests/fixtures/local_indexer/single_word_per_line.pdf` — D-F4 regression fixture
- `tests/test_no_raw_storage_access.py` — Phase 87 multitenant guard (allowlist `[]`); must stay green

### Project-level docs
- `CLAUDE.md` — project conventions, environment variables, "Recently Changed" section to update at close
- `CHANGELOG.md` — release-history file; add v7.15.0 (or whichever release ships Phase 96) section at close
- `docs/OPEN_ISSUES.md` — close out fixed items; leave deferred items

### External docs
- PyMuPDF text-extraction modes — needed for D-F4 fix. Researcher should fetch the PyMuPDF docs page for `Page.get_text()` to confirm behavior of `"text"` vs `"blocks"` vs `"dict"` vs `"words"` and any pinned-version-specific quirks (pymupdf==1.27.2.3 per `requirements-lock.txt`).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `desktop/my_library_tab.py` `MyLibraryTab` — already a 7th tab with vertical split. Reuse, don't rebuild.
- `genizah_app.py` `ResultDialog` — already has folio prev/next for Genizah hits; the LOCAL chunk/page navigation should mirror its pattern (state held on the dialog, buttons enabled/disabled based on bounds).
- `tests/fixtures/local_indexer/single_word_per_line.pdf` — D-F4 regression fixture already exists; no fixture work needed.
- Phase 95 three-state LOCAL filter (D-39) — established the pattern of query-time filtering on LOCAL hits. D-F1 opt-out follows the same shape.
- Phase 95 RRF merger (D-08) at POST-`_deduplicate()` — already integrates LOCAL into main search. D-F5 highlighting work fixes the LAST gap between Genizah and LOCAL parity.

### Established Patterns
- **State separation by deletion** (v7.12 Path B / multitenant) — web side; not directly touched by Phase 96 (desktop-only feature) but the cloud-write gates remain pinned at TOP of `shared/search_serializer.py` / `corrections_client.py` / `lists_sync.py`.
- **Two-phase search** (Tantivy candidates → regex filter+highlight) — Genizah and LOCAL should both use this. D-04 makes the parity explicit.
- **Three cloud-write gates at TOP of functions** — DO NOT lift or move these during the freestyle bucket. Any refactor touching the gated files must preserve gate position.
- **QThread + QMutex serialization (Phase 95 D-25)** — if D-F1 opt-out triggers re-query, ensure it doesn't race with an in-flight `LocalIndexerWorker`.

### Integration Points
- D-F1 tree widget integrates with `MyLibraryTab` bottom panel — replaces (or wraps) the existing scan-status display.
- D-F1 opt-out filter integrates at query time alongside the Phase 95 three-state LOCAL filter — they must compose cleanly.
- D-F5 highlight integration lives wherever the V0.8/V0.7 hit-dict highlighter is called — search table render AND `ResultDialog` render. Both must be fixed.
- NEW-1 button removal is a localized edit in `ResultDialog` LOCAL-hit branch only.
- NEW-2 next/prev requires the LOCAL hit dict to carry enough info (file path + current page/chunk index) for the dialog/browse to compute neighbors. Researcher should check whether `_build_local_result_dict` already exposes this or needs extension.

</code_context>

<specifics>
## Specific Ideas

- The `צפה בדפדוף` button label is a **wrong translation** — user noted this explicitly. Confirms it's been bothering them; removing rather than renaming is the right call.
- The new D-F1 bottom-panel layout is **prescribed**: vertical-split kept; bottom panel becomes a horizontal split with tree-on-left, scan-status-on-right. Don't second-guess the layout during planning.
- The PyMuPDF fix path is **detect-then-fallback** specifically because Hillel doesn't want to risk regressions on currently-working PDFs — keep that motivation visible during planning.

</specifics>

<deferred>
## Deferred Ideas

These are deliberately OUT of Phase 96. Keep as OPEN_ISSUES entries; let v7.15 or later phases revisit.

- **D-F2 — PDF OCR (Tesseract or cloud)** for scanned image-only PDFs. Has a known weak spot for Hebrew/Aramaic OCR quality. Belongs in its own phase with the OCR-engine choice as a primary discussion.
- **D-F3 — Side-by-side PDF page rendering** (PDF page image next to extracted text in Browse + ResultDialog). P3, polish, can wait for a phase dedicated to LOCAL viewer parity with manuscript split view.

### Reviewed Todos (not folded)
*(`gsd-sdk query todo.match-phase 96` returned keyword matches — desktop corrections migration, FIST missing manuscripts, Reading Desk UX, server-side search with email, NLI MARC crawl — but none are about My Library. Not folded into Phase 96 scope.)*

</deferred>

---

*Phase: 96-completing-my-library-feature-add-features-and-fix-bugs*
*Context gathered: 2026-05-24*
