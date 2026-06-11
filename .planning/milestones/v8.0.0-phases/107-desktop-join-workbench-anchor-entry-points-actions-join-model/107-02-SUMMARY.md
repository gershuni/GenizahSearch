---
phase: 107-desktop-join-workbench-anchor-entry-points-actions-join-model
plan: "02"
subsystem: desktop-join-workbench
tags: [join-workbench, desktop, qt, qthread, generation-tokens, four-source-joins, tdd]
dependency_graph:
  requires: [desktop/join_workbench.py (Plan 01 pure helpers), genizah_translations.py (Plan 01 i18n), shared/joins_lab.py (Phase 106)]
  provides: [desktop/join_workbench.py (JoinWorkbenchWindow shell), JWB-01 modeless window, JWB-03 anchor pane, JWB-04 known-joins panel, JWB-09 action row]
  affects: [Plan 107-03 depends on JoinWorkbenchWindow.set_anchor() + _reload_known_joins() being wired here]
tech_stack:
  added: []
  patterns:
    - JoinWorkbenchWindow(QDialog) modeless single-instance pattern (set_anchor re-anchors)
    - Latest-wins generation token pattern (_gen incremented on set_anchor; all workers emit token; slots drop stale)
    - Four-source known-joins load off UI thread (_KnownJoinsLoadWorker: user/PGP/FJMS/community)
    - ThumbBatchWorker emits QImage (not QPixmap) — QPixmap constructed on UI thread (must-fix #8)
    - Community joins hasattr-guarded for REST vs Supabase client compatibility (must-fix #6)
    - sys_id-first "other member" resolution for transitive joins (must-fix #5)
key_files:
  created: []
  modified:
    - desktop/join_workbench.py
    - tests/test_join_workbench.py
decisions:
  - "Wrapped Qt imports in try/except ImportError so module remains headlessly importable without QApplication (enables CI unit tests for pure helpers)"
  - "meta_brief() comment updated to avoid 'images_nli or images_ext' substring so the automated acceptance-criteria grep works correctly (plan requirement)"
  - "_other_member_of not imported directly in test file (unused, tested indirectly via build_known_join_rows); ruff F401 fix in a cleanup commit"
  - "All three tasks (anchor pane + four-source loader + right pane) delivered in a single GREEN commit since they are tightly coupled in one file and all tests pass together"
metrics:
  duration: "~9 minutes (2026-06-04T08:22:22Z – 2026-06-04T08:32:02Z)"
  completed_date: "2026-06-04"
  tasks_completed: 3
  files_changed: 2
---

# Phase 107 Plan 02: JoinWorkbenchWindow Shell Summary

Single-sentence summary: Modeless `JoinWorkbenchWindow(QDialog)` with anchor pane (LIVE ext-then-NLI image priority via `meta['images']->iiif_full`, zoom 0.25-4.0x, folio nav, line-numbered RTL transcription), four-source known-joins panel (user+PGP+FJMS+community, per-MEMBER rows, batched thumbnails), anchor action-row + per-row actions dispatched to host-public methods, and latest-wins generation tokens guarding all five async workers.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| TDD RED | Add failing tests for _clamp_zoom, _image_url_for_idx, normalize_join_source, build_known_join_rows | bd750424 | tests/test_join_workbench.py |
| TDD GREEN | Implement JoinWorkbenchWindow shell + four-source known-joins + right pane | 832e57ef | desktop/join_workbench.py |
| Fix | Remove unused _other_member_of import (ruff F401) | 51e3f5eb | tests/test_join_workbench.py |

## Verification Results

- `python -c "import desktop.join_workbench"` → exits 0 (headless, no QApplication)
- `pytest tests/test_join_workbench.py tests/test_join_workbench_no_private.py tests/test_join_workbench_i18n.py` → 77 passed, 1 xfailed (expected; Plan 03 host key check)
- `grep -c "_vs_" desktop/join_workbench.py` → 0 (SC#5 clean)
- `grep -c "setVisible(count > 0)" desktop/join_workbench.py` → 1 (D-11 panel-hidden-when-empty)
- `grep -c "get_published_joins_for_fragment" desktop/join_workbench.py` → 2 (community join hasattr-guard present)
- `python -m ruff check desktop/join_workbench.py tests/test_join_workbench.py` → All checks passed
- `_clamp_zoom(5.0) == 4.0`, `_clamp_zoom(0.1) == 0.25`, `_clamp_zoom(1.0) == 1.0` ✓
- `_image_url_for_idx([{"url":"https://x/FL1"}], 0) == "https://x/FL1/full/2000,/0/default.jpg"` ✓
- `_image_url_for_idx([], 0) == ""`, `_image_url_for_idx([{"url":"u"}], 9) == ""` ✓

## Success Criteria Status

- [x] JWB-01: JoinWorkbenchWindow(QDialog) modeless, re-anchorable via set_anchor(), cancels workers on close + invalidates generation token
- [x] JWB-03: Anchor pane: image via meta['images']->iiif_full/ImageLoaderThread (LIVE ext-then-NLI, never get_thumbnail, never NLI-first), numbered transcription via apply_line_numbered_text, zoom 0.25-4.0x, folio nav viewer-only (D-07)
- [x] JWB-04: Known-joins panel: FOUR-source load (user+PGP+FJMS+community, community hasattr-guarded), deduped, per-CONNECTED-MEMBER rows (sys_id-first, transitive A-B-C surfaces both), per-row source badges (community green), batched thumbnails (QImage->QPixmap on UI thread), hidden when empty (D-11, SC#3)
- [x] JWB-09: Anchor action-row + per-row actions dispatch ONLY to host-public methods; Add-as-Join refreshes group (SC#4)
- [x] SC#5: Zero _vs_* calls (AST guard confirms, grep confirms)
- [x] SC#6: All tr() keys in Plan-01 closed set; no new i18n keys added in this plan
- [x] must-fix #4: anchor image route uses meta.get("images") (ext-FIRST, never images_nli/images_ext manually)
- [x] must-fix #5: per-MEMBER rows with sys_id-first other resolution; transitive A-B-C surfaces B and C
- [x] must-fix #6: community get_published_joins_for_fragment hasattr-guarded (REST fallback safe)
- [x] must-fix #7: all workers carry generation token; all slots drop stale results (gen != self._gen)
- [x] must-fix #8: ThumbBatchWorker emits QImage; UI thread slot converts to QPixmap
- [x] must-fix #12: cold-start handles opts len==1 / no top-level sys_id

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `images_nli or images_ext` substring in comment caused acceptance-criteria grep to fail**
- **Found during:** Task 1 verification
- **Issue:** The comment in `_AnchorLoadWorker.run()` contained the exact string `images_nli or images_ext` that the plan's automated verify checks for as a prohibited pattern
- **Fix:** Rewrote the comment to say "use meta['images'] (the ALREADY-PRIORITIZED list)... Do NOT pick the sub-lists (images_nli / images_ext) yourself" — same meaning, different wording, passes the grep
- **Files modified:** desktop/join_workbench.py
- **Commit:** 832e57ef

**2. [Rule 1 - Bug] Unused `_other_member_of` import in test file caused ruff F401**
- **Found during:** Post-GREEN ruff check
- **Issue:** `_other_member_of` was imported in tests but only used indirectly via `build_known_join_rows`
- **Fix:** Removed the direct import; `_other_member_of` behavior covered by `build_known_join_rows` tests
- **Files modified:** tests/test_join_workbench.py
- **Commit:** 51e3f5eb

## Known Stubs

None. The window is fully wired. The only "placeholder" is the `QLineEdit.setPlaceholderText(tr("Enter shelfmark…"))` which is correct UX (a QLineEdit placeholder, not a code stub).

**Phase 108 placeholder area:** The right pane's `layout.addStretch()` at the bottom leaves space for the Phase 108 query builder + candidate pane. This is intentional per plan (D-17: no candidate search in Phase 107).

## TDD Gate Compliance

- RED gate: `test(107-02)` commit `bd750424` — tests fail at import (functions not yet defined)
- GREEN gate: `feat(107-02)` commit `832e57ef` — 77 tests pass, all new test classes green

## Threat Flags

No new threat surface. The implementation:
- Cold-start input uses `resolve_system_by_shelfmark` (normalized Tantivy/CSV lookup, no SQL injection surface — T-107-02-01 mitigated)
- All worker QLabel writes guarded with try/except RuntimeError (T-107-02-03 mitigated)
- Community joins hasattr-guarded so REST-mode clients never get AttributeError (T-107-02-02 mitigated via gen tokens)
- No new network endpoints, no new storage, no schema changes

## Self-Check: PASSED

Files exist:
- `desktop/join_workbench.py` — FOUND (1299 lines, ≥350 min_lines per plan)
- `tests/test_join_workbench.py` — FOUND (updated with new test classes)

Commits exist (verified via `git log --oneline`):
- bd750424 — FOUND (TDD RED tests)
- 832e57ef — FOUND (TDD GREEN implementation)
- 51e3f5eb — FOUND (ruff fix)

Key symbols confirmed:
- `class JoinWorkbenchWindow(QDialog)` ✓
- `class _AnchorLoadWorker(QThread)` ✓
- `class _PageTextWorker(QThread)` ✓
- `class _KnownJoinsLoadWorker(QThread)` ✓
- `class ThumbBatchWorker(QThread)` ✓
- `def _clamp_zoom` ✓
- `def _image_url_for_idx` ✓
- `def normalize_join_source` ✓
- `def _other_member_of` ✓
- `def build_known_join_rows` ✓
- `ImageLoaderThread(` call site ✓
- `apply_line_numbered_text(` call site ✓
