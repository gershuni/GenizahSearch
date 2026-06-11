---
phase: 109-visual-similarity-merge-soft-retire
reviewed: 2026-06-08T11:43:12Z
depth: standard
files_reviewed: 5
files_reviewed_list:
  - corrections_ui.py
  - desktop/join_workbench.py
  - desktop/result_dialog.py
  - genizah_app.py
  - genizah_translations.py
findings:
  critical: 0
  warning: 1
  info: 3
  total: 4
status: issues_found
---

# Phase 109: Code Review Report

**Reviewed:** 2026-06-08T11:43:12Z
**Depth:** standard
**Files Reviewed:** 5
**Status:** issues_found

## Summary

Reviewed the phase-109 diff (base `3aed78dd`) implementing the Visual-Similarity → Join-Workbench
merge + soft-retire (JWB-12) and round-4 UAT fixes. The change set is well-engineered and
defensive: the crash-safe `_EnrichWorker` teardown (`_retire_enrich_worker` / `_reap_enrich_worker`
/ `_retired_workers`) is correct and idempotent — it cancels, disconnects the stale result,
retains a still-running QThread until its `finished()` signal, handles the isRunning()↔connect race,
and tolerates double-reap via the guarded `list.remove`. The single-reusable-instance change to
`open_join_workbench` / `open_joins_workbench` is sound (the window is hidden, not deleted — no
`WA_DeleteOnClose` — so the Python reference and in-memory state stay valid for re-show). The
`_save_session` carry-forward of prior `join_lab` when `jw is None` is safe (read-before-write on
the same file, guarded). The CompareDialog `_mark` simplification is correct: `paint()` re-reads
the actual post-toggle triage and restyles the border, so the removed `_restyle_compare(val)` would
have fought the toggle. The shared `_candidate_shelf_badge` helper matches `CandidateCard`'s inline
precedence. Translations, glyph swaps (✓/?/✗, 👁), and forced-LTR nav buttons are consistent.
All five files byte-compile and pass `ruff check`. No live-code references to the removed
`btn_b_visual_sim` / `btn_rd_visual_sim` widgets remain in the main checkout. The intentionally-retained
`_show_vs_dialog` / `_on_vs_fetch_complete` / `_enrich_vs_suggestions` cluster and the pick-callback
machinery are documented D-11 one-cycle soft-retires and are correctly NOT flagged for deletion here.

One genuine warning: phase 109 widened the in-flight window for the un-reaped `_PageTextWorker`
QThreads in `CompareDialog` — the exact lifetime hazard the phase fixed for the enrich worker, now
applicable to a broader surface. Three info-level items round out the report.

## Warnings

### WR-01: CompareDialog `_PageTextWorker` threads are never reaped — same 0xC0000409 vector the phase just fixed for the enrich worker, now widened

**File:** `desktop/join_workbench.py:3960-3973` (`_load_pane_page_text`), with consumers at
`:4106-4108` (`paint` → `_fill_anchor`/`_fill_candidate`), `:4029` (`_fill_candidate`),
`:4163-4172` (`_pane_folio_step`)

**Issue:** `_load_pane_page_text` spawns a `_PageTextWorker` QThread, appends it to
`self._pane_text_workers`, and never removes it on `finished()`. `_PageTextWorker.run()` calls
`self.wb.searcher.get_browse_page(...)` (potentially-slow DB/IIIF work), so a worker can be
in-flight for a meaningful interval. The CompareDialog is modeless and is held only by
`JoinCandidatePane._compare`; the next `open_compare()` reassigns `self._compare`, making the
previous dialog (and its `_pane_text_workers` list) eligible for garbage collection. If a
`_PageTextWorker` is still running at that moment, CPython refcounting can destroy the running
QThread → Qt's "QThread: Destroyed while thread is still running" abort, surfacing on Windows as
exit code **0xC0000409** — the identical crash class the phase just hardened against for
`_EnrichWorker` via `_retire_enrich_worker`. The pattern pre-existed for the candidate pane, but
phase 109 (a) added anchor-pane page-text fetching and (b) routed every initial `_fill_anchor` /
`_fill_candidate` (not just folio nav) through the worker, so the probability a worker is mid-flight
at dialog-replacement / app-close is materially higher. The stale "CompareDialog is short-lived"
comment that justified the omission no longer holds — the dialog is modeless and long-lived.

**Fix:** Apply the same retention-until-finished discipline the phase already wrote for the enrich
worker. Reap on `finished()` and keep a strong ref until then:
```python
worker = _PageTextWorker(self.wb, gen, sid, page)
worker.done.connect(_on_text)
if not hasattr(self, "_pane_text_workers"):
    self._pane_text_workers = []
self._pane_text_workers.append(worker)
worker.finished.connect(lambda w=worker: self._reap_pane_text_worker(w))
worker.start()
# ...
def _reap_pane_text_worker(self, w):
    try:
        self._pane_text_workers.remove(w)
    except (ValueError, RuntimeError):
        pass
```
Additionally add a `closeEvent` on `CompareDialog` that bumps a local gen and lets running workers
drain (or `wait()`s briefly) rather than allowing GC to destroy them mid-run — mirroring
`JoinWorkbenchWindow.closeEvent` at `:4902`.

## Info

### IN-01: Pane image loads have no generation/staleness guard — rapid folio nav can render an out-of-order image

**File:** `desktop/join_workbench.py:3927-3946` (`_set_pane_pix`), `:4012-4021` (`_load_pane_image`)

**Issue:** `_load_pane_image` enqueues an image whose completion calls `_set_pane_pix(pane, pix)`
with no check that the load still matches `pane["page"]`/`pane["sys_id"]`. Two quick folio-next
clicks enqueue two loads; whichever completes last wins, so a zoomed/displayed image can briefly
disagree with the page label. The page-text path is gen-guarded (`wgen != self.wb._gen`), but the
image path is not. This mirrors the pre-existing image enqueue behavior (no gen guard existed
before either), so it is not a regression — noted for awareness.

**Fix:** Capture `gen = self.wb._gen` (and/or the intended `(sys_id, page)`) in `_load_pane_image`
and have the `on_pixmap` callback no-op when `pane["sys_id"]`/`pane["page"]` or `self.wb._gen` has
since changed, e.g. `on_pixmap=lambda p, pd=pane, sid=sys_id, pg=page: pd.get("sys_id")==sid and pd.get("page")==pg and self._set_pane_pix(pd, p)`.

### IN-02: `_save_session` silently drops the `join_lab` key when a live window's `to_state()` raises (pre-existing asymmetry)

**File:** `genizah_app.py:25114-25129`

**Issue:** The new `else` branch (when `jw is None`) carefully carries the prior `join_lab` forward.
But the `if jw is not None` branch, on a `to_state()` exception, only logs and omits the key — so a
transient serialization error on a live, populated window wipes the persisted Join Lab state on the
next save rather than preserving the last-good snapshot. This is a pre-existing asymmetry the phase
did not introduce, but the new carry-forward logic makes the inconsistency more visible.

**Fix (optional, low priority):** In the `to_state()` `except`, fall back to the same
load-prior-and-carry-forward used in the `else` branch so a one-off serialization hiccup does not
erase remembered state.

### IN-03: `_pane_text_workers` list grows unbounded across a long compare session (memory, not correctness)

**File:** `desktop/join_workbench.py:3971-3973`

**Issue:** Independent of WR-01's crash vector, every `_fill_anchor` / `_fill_candidate` /
`_pane_folio_step` appends a finished worker that is never pruned, so a scholar navigating dozens of
candidates/folios accumulates dozens of dead `_PageTextWorker` Python objects on the dialog. The OS
threads do terminate, but the objects linger until the dialog is GC'd. The reaping fix proposed in
WR-01 resolves this as a side effect.

**Fix:** Same as WR-01 — reap on `finished()`.

---

_Reviewed: 2026-06-08T11:43:12Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
