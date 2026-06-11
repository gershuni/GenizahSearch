---
phase: 107-desktop-join-workbench-anchor-entry-points-actions-join-model
reviewed: 2026-06-04T00:00:00Z
depth: standard
files_reviewed: 7
files_reviewed_list:
  - desktop/join_workbench.py
  - desktop/result_dialog.py
  - genizah_app.py
  - genizah_translations.py
  - tests/test_join_workbench.py
  - tests/test_join_workbench_i18n.py
  - tests/test_join_workbench_no_private.py
findings:
  critical: 0
  warning: 3
  info: 5
  total: 8
status: issues_found
---

# Phase 107: Code Review Report

**Reviewed:** 2026-06-04
**Depth:** standard
**Files Reviewed:** 7
**Status:** issues_found

## Summary

Reviewed the Phase-107 Join Workbench additions: the new `desktop/join_workbench.py`
module (1299 lines — pure helpers + JoinWorkbenchWindow QDialog shell + five QThread
workers), the host wiring in `genizah_app.py` (singleton + `open_joins_workbench` /
`open_anchor_in_puzzle` / `open_anchor_as_join` / `_browse_open_join_workbench`), the
ResultDialog entry point (`_open_join_workbench`), the i18n keys in
`genizah_translations.py`, and the three test files.

Overall the code is careful and well-documented. The "must-fix" remediations from the
earlier Codex cross-review (gen-token latest-wins, QImage-on-worker / QPixmap-on-UI,
call-time `tr()` for badges, hasattr-guarded community fetch, public host wrappers) are
all correctly implemented. I verified the host method names and instance attributes the
workbench depends on (`open_result_in_browse_from_table`, `show_add_to_list_menu`,
`_vs_add_to_puzzle`, `current_browse_sid`/`current_browse_p`, `browse_original_text`,
`resolve_system_by_shelfmark`, `get_thumbnail`, `get_browse_page` returning `total_pages`,
`get_connected_fragments_by_id` returning a dict) — all exist with the expected shapes and
return types. The i18n AST guard, the no-private-call guard, and the pure-helper unit tests
are sound and match established codebase patterns.

No critical (security / data-loss / crash) issues found. The findings below are correctness
edge-cases (warnings) and quality/robustness suggestions (info). Most are low-likelihood and
several are pre-existing codebase conventions reused here rather than new defects.

## Warnings

### WR-01: `_other_member_of` transitive-edge fallback can drop a member depending on a/b storage order

**File:** `desktop/join_workbench.py:241-244` (logic), surfaced via `build_known_join_rows:264-283`
**Issue:** For a transitive edge that does NOT touch the anchor (e.g. anchor=A, edge=B↔C),
`_other_member_of` falls through both the sys_id-match and shelfmark-match branches and
unconditionally returns `(b_sid, fb)` — i.e. whichever member happens to be stored as
`fragment_b`. If the same physical pair is stored as C↔B (C in `fragment_a`), the function
returns B, which is already `seen` (it appears via the A↔B edge), so the row is skipped and
**C never surfaces**. The favorable ordering is covered by
`test_transitive_abc_surfaces_both_members` (which stores the edge as B→C, the lucky case),
so the test passes while the reverse-storage case silently drops a connected member.
**Fix:** Surface BOTH members of a non-anchor edge instead of guessing one. For example,
have `build_known_join_rows` emit a member row for each endpoint of an edge whose neither side
is the anchor, and let the existing `seen_members` dedup collapse the duplicate:
```python
# in build_known_join_rows, when neither side is the anchor:
if a_sid_not_anchor and b_sid_not_anchor:
    candidates = [(a_sid, fa), (b_sid, fb)]
else:
    candidates = [_other_member_of(j, anchor_sid, anchor_shelf)]
for other_sid, other_shelf in candidates:
    ...  # existing per-member dedup + row build
```
Or, simpler: keep `_other_member_of` for anchor-incident edges and, for non-incident edges,
add both endpoints. Add a regression test that stores the transitive edge in the reverse
(C→B) order and asserts C still appears.

### WR-02: `ThumbBatchWorker.run` issues network requests with `verify=False` and no shared NLI timeout/circuit-breaker

**File:** `desktop/join_workbench.py:519-545`
**Issue:** The batched thumbnail worker calls `requests.get(url, ..., timeout=5, verify=False)`
in a per-row loop. Two concerns: (1) `verify=False` disables TLS certificate verification
(this matches `ImageLoaderThread._download_bytes` convention, so it is consistent rather than
new — noting for completeness), and (2) the fetch is a raw synchronous `requests.get` that
does NOT go through the Phase-98 shared NLI circuit breaker (`shared/nli_circuit_breaker.py`).
When NLI/IIIF is slow, a known-join group with N members issues N serial 5s-timeout requests
on the worker thread; in the worst case that is up to `N×5s` of work that the breaker would
otherwise short-circuit. The gen-token guard means the UI never blocks, but the thread can
stay busy long after the anchor changed (cancel is cooperative and only checked between rows).
**Fix:** Route the thumbnail bytes fetch through the existing breaker the way the Phase-98
sites do (record success/failure, skip when the breaker is open), e.g.:
```python
from shared.nli_circuit_breaker import get_breaker  # or the module's record/allow API
breaker = get_breaker()
if not breaker.allow():
    self.resolved.emit(self._gen, i, None); continue
try:
    resp = requests.get(url, headers=..., timeout=5, verify=False)
    breaker.record_success()
except Exception:
    breaker.record_failure(); qimg = None
```
At minimum, check `self._cancel` more frequently or honor a tighter per-batch budget.

### WR-03: ImageLoaderThread / worker threads are reassigned without `wait()`, risking "QThread destroyed while running"

**File:** `desktop/join_workbench.py:864-881` (`_load_current_image`), and the
`quit()`-then-reassign pattern in `_reload_known_joins:1021-1035` and `_on_known_joins_loaded:1070-1080`
**Issue:** When a new image/known-joins/thumb load starts, the previous worker is `cancel()`/`quit()`'d
and then the attribute is reassigned to a fresh thread. `quit()` only asks a thread's event loop
to stop — it does not interrupt a blocking `run()` (these workers do blocking network/IO in
`run()`, not an event loop). The old `QThread` Python object can be dropped while its OS thread
is still executing, which Qt warns about ("QThread: Destroyed while thread is still running")
and can in rare cases crash on shutdown. NOTE: this mirrors the established codebase convention
(`browse_img_thread`, `lists_preview_img_thread` in `genizah_app.py` are reassigned the same way
without `wait()`), so it is a reused pattern, not a regression. The gen-token correctly prevents
*stale results* from being applied; this finding is about *thread lifetime*, not result correctness.
**Fix (optional, defensive):** Keep finished workers alive until they actually finish, e.g. connect
`worker.finished.connect(worker.deleteLater)` and hold a transient reference, or call
`w.wait(50)` in `_cancel_workers`/`closeEvent` before dropping the reference. Given it matches
existing convention, this can be deferred — but `closeEvent` in particular (line 1000-1004) is the
right place to add a short bounded `wait()` so threads don't outlive the dialog.

## Info

### IN-01: `meta_brief` counts `images_nli` OR `images_ext`, but the worker stores the merged `images` list — count can disagree with folio total

**File:** `desktop/join_workbench.py:99-105` vs `desktop/join_workbench.py:347-348`
**Issue:** `meta_brief` computes `n_img = len(meta.get("images_nli") or meta.get("images_ext") or [])`,
i.e. it counts only ONE sub-list. But `_AnchorLoadWorker` stores the already-prioritized merged
`meta.get("images")` as `self._anchor_images`, and the folio counter (`_update_folio_controls`)
uses `len(self._anchor_images)`. If `enrich_metadata` merges NLI + ext into `images`, the
"N img" shown in the meta-brief line and the "k/total" folio counter can show different totals,
which is confusing. **Fix:** make `meta_brief` prefer `meta.get("images")` (the same list the
folio nav uses) before falling back to the sub-lists, so the two displays agree.

### IN-02: `_build_join_row` uses `row["fragment_b"]` as the per-member "title", which is a shelfmark, not a title

**File:** `desktop/join_workbench.py:1147-1153`
**Issue:** The secondary (gray, small) label under each known-join row's shelfmark is built from
`row.get("fragment_b")` truncated to 60 chars. `fragment_b` is a shelfmark string, and the row
already displays `other_shelf` as the primary label — so for many rows the small label just
repeats (a truncated form of) the shelfmark rather than showing a work title. `build_known_join_rows`
does not populate a title field. **Fix:** either drop the redundant second label, or resolve a
real title via `meta_mgr.get_meta_for_id(other_sid)` (already used elsewhere in the workbench)
and store it as `other_title` in the row dict, then render that.

### IN-03: Cold-start picker uses `tr("Enter shelfmark…")` as the QInputDialog prompt label

**File:** `desktop/join_workbench.py:1274-1276`
**Issue:** When multiple shelfmark matches exist, the disambiguation picker reuses the
`"Enter shelfmark…"` placeholder string as its prompt label. That string reads as a
data-entry hint ("Enter shelfmark…"), not as a "choose one of these matches" instruction, so
the picker prompt is slightly off semantically. **Fix:** add a dedicated key such as
`tr("Select a manuscript:")` to the Phase-107 TRANSLATIONS block and use it for the picker
prompt (keep `"Enter shelfmark…"` for the line-edit placeholder only).

### IN-04: The `"Could not load joins. Click to retry."` translation key is defined but never used in the module

**File:** `genizah_translations.py:3685` (key defined); not referenced in `desktop/join_workbench.py`
**Issue:** The comment annotates this key as "known-joins retry affordance (Plan 02 Task 3, REC-2)",
but the known-joins error path (`_KnownJoinsLoadWorker.run` swallows exceptions per-source and
`_on_known_joins_loaded` simply hides the panel when rows are empty) never surfaces a retry
affordance using this string. The key is dead but harmless. **Fix:** either wire a clickable
"retry" row/label when all four source fetches raise (so a transient network failure is
distinguishable from "genuinely no joins"), or drop the unused key to avoid drift between the
i18n table and the code.

### IN-05: `_AnchorLoadWorker.cancel()` / `_PageTextWorker.cancel()` / `_KnownJoinsLoadWorker.cancel()` are no-op stubs

**File:** `desktop/join_workbench.py:330-332, 375-377, 406-408`
**Issue:** Three of the five workers define `cancel()` as `pass` (documented as "best-effort;
gen token is the real correctness guard"). This is intentional and correct for *result*
correctness, but it means `_cancel_workers()` cannot actually stop in-flight network work for
these three — only `ThumbBatchWorker.cancel()` sets a real flag. Combined with WR-02/WR-03,
a rapidly re-anchoring user can accumulate several still-running blocking network threads.
**Fix:** if/when WR-02 routes through the breaker, also have these workers check a `self._cancel`
flag at their natural yield points (between the metadata fetch and the text fetch in
`_AnchorLoadWorker`; before each source block in `_KnownJoinsLoadWorker`) so cancellation can
short-circuit the remaining work instead of only discarding the result.

---

_Reviewed: 2026-06-04_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
