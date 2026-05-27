---
phase: "100"
plan: "03"
subsystem: desktop
tags: [pdf, rendering, browse, pyqt6, desktop-only, controller]
dependency_graph:
  requires:
    - "100-01 (PdfImageController with per-scope state, debounce, watchdog)"
  provides:
    - "Browse panel reveals image pane + renders LOCAL PDF page via scope='browse'"
    - "Non-PDF LOCAL files cancel 'browse' scope + keep pane hidden"
    - "LOCAL-PDF to Genizah transition cancels 'browse' scope in _start_browse_enrichment"
    - "prev/next PAGE navigation re-renders via existing re-entry into _open_local_browse_page"
  affects:
    - "genizah_app.py (_open_local_browse_page + _start_browse_enrichment)"
tech_stack:
  added: []
  patterns:
    - "is_pdf gate on Browse pane visibility (extension-gated auto-show D-08)"
    - "scope='browse' fixed key for lifetime-of-app Browse scope"
    - "sid+page-guarded lambdas mirroring _on_volume_manifest_loaded defensive recheck"
    - "getattr(self, '_pdf_image_controller', None) guard for pre-100-01 safety"
    - "cancel('browse', silent=True) in _start_browse_enrichment for R2-4 cleanup"
key_files:
  created: []
  modified:
    - genizah_app.py
decisions:
  - "Browse uses permanent scope key 'browse' with cancel() not discard_scope() — one Browse panel exists for the lifetime of the app, no timer accumulation concern (REVIEWS-R2-3)"
  - "controller cancel placed at TOP of _start_browse_enrichment (before generation bump) on the clean semantic anchor — the single Genizah-browse funnel; preferred over documenting accepted residual (REVIEWS-R2-4)"
  - "sid+page both checked in lambdas (not just sid) to guard against fast prev/next page stale results landing after user navigated to a different page of the same document (REVIEWS MEDIUM-5)"
  - "filepath already held as local in _open_local_browse_page from line 19229; reused directly — no second lookup"
metrics:
  duration: "4 minutes"
  completed_date: "2026-05-27"
  tasks_completed: 1
  files_created: 0
  files_modified: 1
---

# Phase 100 Plan 03: Browse PDF image wiring — scope='browse' controller integration

Browse panel now reveals the image pane and renders the current page for LOCAL PDF hits via `PdfImageController.request("browse", ...)`, with sid+page-guarded callbacks and prompt scope release on Genizah-browse transitions.

## What Was Built

### Task 1: Gate Browse image pane on is_pdf + wire controller (genizah_app.py)

Two edits to `genizah_app.py` replacing the unconditional pane-hide in `_open_local_browse_page` and adding a cancel at the top of `_start_browse_enrichment`.

**(A) `_open_local_browse_page` — extension-gated pane reveal + render / cancel**

The previously unconditional `self._set_browse_image_pane_visible(False)` (after `current_browse_sid`/`current_browse_p` are set) is replaced with:

```python
controller = getattr(self, '_pdf_image_controller', None)
if is_pdf and controller is not None and bool(filepath):
    self._set_browse_image_pane_visible(True)
    page_num = self.current_browse_p or 1
    controller.request(
        "browse", sys_id, page_num, filepath,
        on_image=lambda img, _sid=sys_id, _pnum=page_num: (
            self.browse_viewer.display_image(img)
            if (getattr(self, 'current_browse_sid', None) == _sid
                and getattr(self, 'current_browse_p', None) == _pnum) else None
        ),
        on_placeholder=lambda text, _sid=sys_id, _pnum=page_num: (
            self.browse_viewer.scroll_area.set_status_message(text)
            if (getattr(self, 'current_browse_sid', None) == _sid
                and getattr(self, 'current_browse_p', None) == _pnum) else None
        ),
    )
else:
    if controller is not None:
        controller.cancel("browse", silent=True)
    self._set_browse_image_pane_visible(False)
```

Key design notes:
- `is_pdf` already computed at method top via `fp = self._lookup_local_filepath(sys_id) or ""; is_pdf = fp.lower().endswith('.pdf')` — reused as-is (REVIEWS MEDIUM-6: `or ""` makes it None-safe already)
- `filepath` from line 19229 `self._lookup_local_filepath(sys_id)` — reused directly
- `current_browse_sid`/`current_browse_p` already set just above this block — `page_num = self.current_browse_p or 1` picks up the physical page
- Both `_sid` AND `_pnum` are captured in the lambda closure for the sid+page double guard (REVIEWS MEDIUM-5) — mirrors `_on_volume_manifest_loaded` precedent
- `getattr(..., None)` guard means this is safe even before 100-01 sets `_pdf_image_controller`
- The controller's own `is_pdf` gate is a second layer; `bool(filepath)` is belt-and-suspenders

**(B) `_start_browse_enrichment` — cancel 'browse' on Genizah-browse transitions (REVIEWS-R2-4)**

At the top of `_start_browse_enrichment(self, sid, is_part=False)` (the single centralized funnel for all Genizah-browse manuscript loads, called from 4 sites, NOT called from `_open_local_browse_page`), before `_browse_enrich_gen += 1`:

```python
controller = getattr(self, '_pdf_image_controller', None)
if controller is not None:
    controller.cancel("browse", silent=True)
```

This releases any retained `"browse"` callbacks promptly when the user switches from a LOCAL PDF to a Genizah manuscript, closing the brief callback-retention window that REVIEWS-R2-4 identified. The sid+page guards already prevented a stale DISPLAY — this fix ensures the controller also stops holding the callbacks.

**prev/next PAGE coverage (D-06):**
`_on_local_browse_nav` re-enters `_open_local_browse_page` on every prev/next click, so the (A) edit covers page-navigation re-renders automatically. No separate render call needed in the nav handler.

## Deviations from Plan

None — plan executed exactly as written.

## Known Stubs

None. The Browse image pane is now fully wired for LOCAL PDF. `self.browse_viewer.display_image(QImage)` and `self.browse_viewer.scroll_area.set_status_message(str)` are real ManuscriptViewerWidget methods (Phase 99 D-02 confirmed).

## Threat Surface Scan

No new network endpoints, auth paths, or file access patterns introduced. The filepath used in `controller.request()` is resolved from the already-indexed `local_files` table via `_lookup_local_filepath` — same path used for the existing text render. T-100-11 through T-100-21 all mitigated as specified in the plan's threat register.

## Self-Check: PASSED

- genizah_app.py: FOUND (modified)
- Commit 53c8ffba (Task 1 — Browse PDF wiring): FOUND
- `getattr(self, '_pdf_image_controller', None)` present in genizah_app.py: FOUND
- `controller.cancel("browse", silent=True)` appears >= 2 times: FOUND (count=2)
- `self._set_browse_image_pane_visible(True)` present: FOUND
- `self.browse_viewer.display_image(img)` present: FOUND
- `self.browse_viewer.scroll_area.set_status_message(text)` present: FOUND
- `getattr(self, 'current_browse_p', None) == _pnum` present: FOUND
- `python -c "import ast; ast.parse(...)"` exits 0: PASSED
- `python -m ruff check genizah_app.py`: All checks passed
