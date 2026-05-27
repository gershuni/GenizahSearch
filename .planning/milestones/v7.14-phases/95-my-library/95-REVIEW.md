---
phase: 95-my-library
reviewed: 2026-05-21T00:00:00Z
depth: standard
files_reviewed: 21
files_reviewed_list:
  - CHANGELOG.md
  - CLAUDE.md
  - GenizahSearchPro.spec
  - Help.html
  - corrections_client.py
  - desktop/my_library_tab.py
  - desktop/result_dialog.py
  - docs/OPEN_ISSUES.md
  - genizah_app.py
  - genizah_core.py
  - genizah_translations.py
  - gui_threads.py
  - lists_sync.py
  - pyproject.toml
  - requirements.txt
  - shared/export_dossier.py
  - shared/local_indexer.py
  - shared/local_sys_id.py
  - shared/search_serializer.py
  - web/export_service.py
  - web/pages/about.py
  - web/pages/help.py
findings:
  critical: 2
  warning: 8
  info: 7
  total: 17
status: fixed
fixed_at: 2026-05-21T00:00:00Z
fixed_items:
  - CR-01
  - CR-02
  - WR-01
  - WR-05
  - WR-08
deferred_items:
  - WR-02
  - WR-03
  - WR-04
  - WR-06
  - WR-07
  - IN-01
  - IN-02
  - IN-03
  - IN-04
  - IN-05
  - IN-06
  - IN-07
---

# Phase 95: Code Review Report

**Reviewed:** 2026-05-21
**Depth:** standard
**Files Reviewed:** 21
**Status:** issues_found

## Summary

Phase 95 introduces a substantial new desktop-only feature (My Library local document indexing) with strong defense-in-depth around cloud-write boundaries. The three primary invariants requested by the orchestrator are well-protected:

1. **LOCAL sys_id cloud-write gates**: All three gates are correctly placed at the TOP of their respective functions BEFORE any cloud I/O. `lists_sync.sync_item_to_cloud` and `sync_list_to_cloud` derive `sys_id` from in-memory state and short-circuit before `_get_client()`. `corrections_client.create_correction` rejects at function entry. `shared/search_serializer.serialize_search_payload` filters LOCAL items before serialization. All three gates have dedicated regression tests.

2. **Per-thread SQLite**: `LocalIndexer` correctly uses `threading.local()` (NOT `check_same_thread=False`).

3. **RRF merge after `_deduplicate()`**: Verified at `genizah_core.py:8239-8255` — main search dedup runs first, then RRF merges LOCAL hits.

4. **D-37 corrupt-index fallback**: `_open_local_searcher` catches exceptions and continues with Genizah-only search.

5. **Export `skip_local`**: `build_manuscript_row` and `build_bibliography_rows` accept `skip_local=True`; web export passes True, desktop passes False (per D-45).

6. **Web LIBRARY_CODES guard**: `tests/test_web_library_options_no_local.py` AST guard installed.

However, the review uncovered **two CRITICAL bugs** that will cause runtime failures, plus several warnings and code-quality issues that should be addressed before shipping v7.14.

## Critical Issues

### CR-01: `SearchEngine._check_local_lab_freshness` AttributeError — crashes `search_composition_logic`

**File:** `genizah_core.py:6659-6669` (def site), `genizah_core.py:8496` (call site)
**Issue:**
`SearchEngine._current_lab_weights_hash()` references `self.dynamic_rank_map` (line 6664) and `self.settings` (line 6665) but **these attributes are NOT defined on SearchEngine** — they belong to `LabEngine` (assigned in `LabEngine.__init__` at lines 686, 690). SearchEngine has no inheritance from LabEngine (verified — `class SearchEngine:` at line 6565 has no base classes).

When a user runs standard-mode Composition Search:
1. `CompositionThread → SearchEngine.search_composition_logic` invokes the LOCAL LAB hook at line 8496:
   ```python
   if not was_cancelled and self._check_local_lab_freshness():
   ```
2. `_check_local_lab_freshness()` returns False immediately when `local_lab_searcher is None` (safe). But once the LOCAL LAB index exists and `reload_local_lab_index()` populates `self.local_lab_searcher` AND `self._lab_local_meta`, line 6684 executes:
   ```python
   current_hash = self._current_lab_weights_hash()
   ```
3. Inside `_current_lab_weights_hash` line 6664: `self.dynamic_rank_map if self.dynamic_rank_map else None` → **`AttributeError: 'SearchEngine' object has no attribute 'dynamic_rank_map'`**.
4. The exception is NOT caught at the call site (line 8496 is outside the try block at line 8497). The exception propagates up through `search_composition_logic` → `CompositionThread.run` → `error_signal.emit(str(e))` — the user sees a generic error and composition search is broken.

The existing test `tests/test_local_lab_invalidation.py::TestLabCompositionSearchLocalLab` masks this bug because it constructs SearchEngine via `object.__new__(SearchEngine)` and manually attaches `engine.settings = StubSettings(); engine.dynamic_rank_map = None`. Real construction via `SearchEngine(meta_mgr, var_mgr)` does NOT set these attributes.

**Fix:** Either (a) duck-type the attributes safely, or (b) wire SearchEngine to a LabEngine reference. Minimal fix:
```python
def _current_lab_weights_hash(self) -> str:
    """Compute hash of current LAB weights for D-38 staleness check."""
    import hashlib as _hashlib
    import json as _json
    # SearchEngine has no dynamic_rank_map / settings — these live on LabEngine.
    # Use getattr with defaults so the freshness check works in both contexts.
    dyn_map = getattr(self, 'dynamic_rank_map', None)
    settings = getattr(self, 'settings', None)
    weights_dict = {
        "dynamic_rank_map": dyn_map if dyn_map else None,
        "use_dynamic_weights": getattr(settings, "use_dynamic_weights", False) if settings else False,
    }
    return _hashlib.sha256(
        _json.dumps(weights_dict, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
```
Also wrap the call site at line 8496 in a try/except to satisfy D-37 fallback semantics:
```python
try:
    _lab_fresh = self._check_local_lab_freshness()
except Exception as _e:
    LOGGER.warning("_check_local_lab_freshness raised: %r — skipping LOCAL LAB", _e)
    _lab_fresh = False
if not was_cancelled and _lab_fresh:
    ...
```
Add a regression test that constructs a real `SearchEngine(meta_mgr, var_mgr)` (not `object.__new__`) with a populated LOCAL LAB meta and confirms `_check_local_lab_freshness()` returns a boolean without raising.

---

### CR-02: REQ-6 LAB Composition Search surface — LOCAL hits never appear

**File:** `genizah_core.py:1491-1599` (LabEngine.lab_composition_search LOCAL LAB hook)
**Issue:**
The Wave 6 SUMMARY claims: *"When called from SearchEngine (which inherits both), the full freshness check runs."* This is **factually incorrect** — `SearchEngine` does NOT inherit from `LabEngine` (both are independent classes at lines 6565 and 678 respectively).

`LabCompositionThread.run()` (gui_threads.py:228) calls `self.lab_engine.lab_composition_search(...)` where `lab_engine` is a plain `LabEngine` instance. Inside that method, lines 1495-1499:
```python
_freshness_fn = getattr(self, "_check_local_lab_freshness", None)
if not was_interrupted and callable(_freshness_fn) and _freshness_fn():
    try:
        local_lab_index = getattr(self, "_local_lab_index", None)
        local_lab_searcher = self.local_lab_searcher  # AttributeError on LabEngine
```
- `_check_local_lab_freshness` does not exist on `LabEngine` → `getattr` returns None → `callable(None)` is False → entire LOCAL LAB hook is skipped silently.
- Even if the guard passed, `self.local_lab_searcher` (line 1499) would raise `AttributeError`, but it's inside the try/except Exception block at line 1592 — caught and logged.

**Net effect:** LOCAL LAB Composition Search results NEVER reach the user via LAB mode. REQ-6 (three-surface coverage: Search, Composition Search, Parallels) is only partially satisfied — standard-mode Composition Search hits work (modulo CR-01), LAB-mode does not. Since LAB mode is the more sophisticated path users may prefer for scholarly composition matching, this is a significant feature gap.

**Fix options:**
- **Option A (recommended):** Add `_check_local_lab_freshness`, `local_lab_searcher`, `_local_lab_index`, `_lab_local_meta` as initialized attributes on `LabEngine`. Mirror the population logic from `SearchEngine._open_local_searcher` / `reload_local_lab_index` in `LabEngine.__init__` and `LabEngine._reload_lab_index()`. Wire `MyLibraryTab._on_worker_finished` to also call `self.lab_engine.reload_local_indexes()`.
- **Option B:** Pass a `SearchEngine` reference into `LabEngine.lab_composition_search` (large refactor).
- **Option C (smallest, lossy):** Document the limitation. LAB Composition does not support LOCAL — Help text should clarify. This violates REQ-6 as written.

Add a regression test in `tests/test_local_lab_invalidation.py` that constructs a real `LabEngine`, sets up a stub LOCAL LAB index, and asserts a LOCAL doc surfaces in `lab_composition_search` results.

## Warnings

### WR-01: `_write_page_doc` always uses `file_id=0` for newly-indexed files

**File:** `shared/local_indexer.py:1040-1052` / `_index_one_file` flow
**Issue:** Inside `_index_one_file` the sequence is:
1. `INSERT OR REPLACE INTO processed_files` (line 945)
2. `_extract_and_write_pdf/_docx/_txt` → calls `_write_page_doc` per page
3. `_write_page_doc` (line 1041) queries `local_files` for `file_id` — but `local_files` row is not inserted until step 4
4. `_finish_file` (line 1010) inserts the `local_files` row

So every newly-indexed file's `full_header` is `{sys_id}_LOCAL_P{n}_F0000` (the fallback at line 1044). The real `file_id` is only used on subsequent re-indexes (where `local_files` row exists from the previous scan).

The `parse_full_id_components` regex `_F(\d{3,5})` matches `F0000` fine, so this is not a crash. But D-34's intent of unique `F\d{4}` suffixes per file is not satisfied for first-index files, defeating part of the design.

**Fix:** Reorder `_index_one_file` so `local_files` is INSERTed early (with `extraction_status='pending'`) and then UPDATEd at the end. Or, predetermine `file_id` via a separate `INSERT ... RETURNING file_id` before extraction begins. Add `tests/test_local_indexer.py::test_file_id_populated_on_first_index` asserting the F-suffix is non-zero for newly-indexed files.

---

### WR-02: `_iterate_supported_files` walks ALL files but ceiling check counts only supported

**File:** `shared/local_indexer.py:867-898` and `prescan_count` at line 748-768
**Issue:** `_iterate_supported_files` yields every file (so unsupported ones get `extraction_status='unsupported'`), while `prescan_count` filters by `_SUPPORTED_EXTENSIONS` before counting. A user with 50,000 `.jpg` files (or other junk) in a folder will pass the ceiling check (zero supported files) but the scan will still process every file, write SQLite rows, and emit per-file Qt signals for all of them — defeating the 5,000-file ceiling and degrading UX.

**Fix:** Either (a) make `_iterate_supported_files` truly filter to supported extensions (then unsupported files won't be tracked, but the user won't see them as "ignored"), or (b) make `prescan_count` count ALL files like the iterator does. The decision matters because the spec says "5,000 files" — clarify whether that's supported files only or total files.

---

### WR-03: `os.startfile()` on canonical filepath — defense-in-depth check missing

**File:** `desktop/result_dialog.py:1891-1895`, `genizah_app.py:18521-18525`
**Issue:** Both call sites have `if filepath and os.path.exists(filepath): os.startfile(filepath)`. The `filepath` originates from `LocalIndexer.get_filepath(sys_id)` which returns the canonical filepath from `local_files`. Canonicalization happens via `_canonical_filepath` which uses `Path.resolve(strict=False)` — this resolves symlinks/junctions.

A concern: if a user adds folder `C:\Trusted\` and the SQLite later contains a path that was resolved through a junction to `C:\Untrusted\evil.exe.lnk`, then `os.startfile()` would execute the target. However, the only way to add files is via `os.walk` over a user-chosen folder, and `os.walk(followlinks=False)` is correctly set (line 879). So this is a theoretical concern — the path can't escape the user's chosen folder via walks.

Still, as defense-in-depth:
- Verify the `filepath` is still under one of the registered `folders` paths before invoking `os.startfile`.
- Avoid launching anything with extension outside `_SUPPORTED_EXTENSIONS` (a registered `.pdf` couldn't be renamed to `.exe`, but a hardened check is cheap).

**Fix (defense-in-depth):**
```python
def _on_browse_open_file_clicked(self):
    filepath = getattr(self, '_current_local_filepath', None)
    if not filepath or not os.path.exists(filepath):
        return
    ext = os.path.splitext(filepath)[1].lower()
    if ext not in {'.docx', '.pdf', '.txt'}:
        logger.warning("Refusing to open file with disallowed extension: %s", filepath)
        return
    os.startfile(filepath)
```

---

### WR-04: `datetime.utcnow()` deprecated

**File:** `shared/local_indexer.py:1330`
**Issue:** `datetime.datetime.utcnow().isoformat() + "Z"` — `utcnow()` was deprecated in Python 3.12 and emits `DeprecationWarning`.

**Fix:**
```python
from datetime import datetime, timezone
"last_built_at": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
```

---

### WR-05: Test fixture `hebrew_sample.pdf` (6.3 MB) shipped in production installer

**File:** `GenizahSearchPro.spec:4`
**Issue:** The PyInstaller datas tuple includes `('tests\\fixtures\\local_indexer\\hebrew_sample.pdf', 'tests/fixtures/local_indexer')`. This bundles a 6.3 MB Hebrew PDF fixture into every production installer build for end users. The fixture is only needed by `tests/test_local_pyinstaller_smoke.py` which is `@pytest.mark.packaging` — release CI only.

**Fix:** Remove `hebrew_sample.pdf` from `datas`. The packaging smoke test can be run against a separate, on-the-fly built PyInstaller bundle with the fixture added only for that build. Alternatively, gate inclusion via a build-time flag.

---

### WR-06: `_iterate_lab_source_rows` opens a second handle on the LOCAL Tantivy index

**File:** `shared/local_indexer.py:1359-1382`
**Issue:** `LocalIndexer` already holds `self._index` (the LOCAL Tantivy Index with a writer). `_iterate_lab_source_rows` opens a SECOND `tantivy.Index(main_schema, path=self._index_dir)` against the same directory (line 1368). If `self._writer` has uncommitted writes when this runs, the second handle will not see them. The function does NOT explicitly call `self._writer.commit()` before opening the second handle.

This is currently dead code (`build_lab_side_index` is reached only through `SearchEngine.rebuild_local_lab_index`, which is defined but never invoked anywhere — see WR-08), so the bug doesn't fire today. But if/when LAB rebuild is wired in, it could produce empty LAB indexes when called mid-batch.

**Fix:** Before opening the second handle, call `self._writer.commit()` (or `self._commit_batch()` to use the two-phase protocol). Add a docstring contract note.

---

### WR-07: `lists_sync.sync_item_to_cloud` LOCAL gate is fragile if item_id is composite

**File:** `lists_sync.py:762-766`
**Issue:** The LOCAL gate at the top of `sync_item_to_cloud` falls back to `item_id` when `item_data` is None:
```python
sys_id = item_data.get('sys_id', item_id) if item_data else item_id
if is_local_sys_id(sys_id):
```
If a LOCAL item was stored under a composite item_id like `970012345601234567::img::xyz` (per the `_dedupe_and_index_local_items` comment about `{sys_id}::img::{img}` keys), and `item_data` is missing, `sys_id` becomes the composite string. `is_local_sys_id` rejects it (not 18 digits, contains non-digits) — the gate does NOT fire, and the cloud client gets touched.

Today this is unlikely because LOCAL items don't have `img` URLs. But the gate is one composite-key away from being bypassed. Robust gates should normalize before checking.

**Fix:** Extract the prefix-digit portion before checking:
```python
def _maybe_local(s: str) -> bool:
    if not s:
        return False
    # Composite keys like "970012345601234567::img::xyz" — strip the suffix.
    head = s.split('::', 1)[0]
    return is_local_sys_id(head)
```
Use `_maybe_local(sys_id)` in the gate. Add `tests/test_local_namespace_no_lists_leak.py::test_composite_item_id_with_local_prefix_still_blocked`.

---

### WR-08: Dead code — `rebuild_local_lab_index` is never called

**File:** `genizah_core.py:6694-6716`
**Issue:** `SearchEngine.rebuild_local_lab_index(local_indexer)` is defined but never invoked anywhere in the codebase (no callers in `genizah_app.py`, `desktop/`, or `tests/`). The Wave 6 plan claims it's called from MyLibraryTab Refresh and Tools→Rebuild LAB; neither call site exists in the diff.

D-38's invalidation triggers ("rebuild on Refresh", "rebuild on stored hash mismatch", etc.) are therefore unimplemented end-to-end — the LOCAL LAB index is built only on initial scan via `LocalIndexer.build_lab_side_index` invocation, which is also unwired. Searching for `build_lab_side_index` shows no callers either.

**Fix:** Either (a) wire `MyLibraryTab._on_worker_finished` to call `self.search_engine.rebuild_local_lab_index(self._indexer)` after `reload_local_indexes`, OR (b) remove the dead code and explicitly defer LAB rebuild to a follow-up phase, documenting the gap in the SUMMARY.

## Info

### IN-01: SearchEngine duplicates LabEngine state model without coupling

**File:** `genizah_core.py:6659-6716`
**Issue:** `_current_lab_weights_hash`, `_check_local_lab_freshness`, `_compute_fingerprint_dyn`, `_compute_fingerprint_static`, `_normalize_text`, `rebuild_local_lab_index` are all defined on SearchEngine but logically belong with LabEngine state. The duplication creates the maintenance burden that surfaced CR-01 and CR-02.

Consider refactoring to either (a) move these methods to LabEngine and have SearchEngine hold a `lab_engine` reference, or (b) extract a shared mixin/helper class.

---

### IN-02: `_iterate_supported_files` misleading docstring

**File:** `shared/local_indexer.py:867-877`
**Issue:** Function is named `_iterate_supported_files` but yields ALL files including unsupported. The docstring acknowledges this but the name is misleading. Suggest renaming to `_iterate_all_files_in_folder` and adding `_iterate_supported_files` that filters.

---

### IN-03: `D-22 two-stage UX` comment lies about implementation

**File:** `desktop/my_library_tab.py:579-588`
**Issue:** Comment says "D-22: initial status is 'Indexing...' which transitions to 'OK' once the batch commits. We use a simplified model where the worker emits the final status directly". Per D-22, the two-stage UX was the contract. The implementation simplified it away. Either restore the two-stage behavior or update the SPEC/CONTEXT to reflect the simpler design.

---

### IN-04: Help docs Hebrew arrow direction inconsistency

**File:** `Help.html:548` and `web/pages/help.py:670`
**Issue:** The Hebrew Help text uses `&larr;` (left arrow) for cycling: "הכל ← רק מקומי ← ללא מקומי ← הכל". In RTL Hebrew context, the visual direction is correct (reads right-to-left). But the actual cycling code in `genizah_app.py` cycles forward (`(cur + 1) % 3`), which English text shows with `→`. The Hebrew text uses `←` so it visually flows right-to-left, but a user might misread "the first state is 'All', then 'Only Local'" vs "the first state is 'Only Local'". This is a documentation polish issue, not a functional bug.

---

### IN-05: `_lookup_local_filepath` not guarded by `is_local_sys_id` check

**File:** `genizah_app.py:18486-18499`
**Issue:** The helper accepts any sys_id and calls `indexer.get_filepath(sys_id)`. The SQLite query naturally returns None for non-LOCAL sys_ids (they won't be in `local_files`), so no security/correctness issue, but adding an `is_local_sys_id(sys_id)` guard at the top would document intent and avoid a SQLite query for every non-LOCAL hit.

---

### IN-06: Filter state persistence uses session save/load, not QSettings as D-39 specified

**File:** `genizah_app.py:23424,23446,23447,23575,23646`
**Issue:** D-39 specifies QSettings keys (`myLibrary/search_local_filter`, etc.) for the three filter states. The implementation uses session save/load instead. Functionally equivalent for portable installations but deviates from the spec. Either update D-39 to reflect the actual choice or move the keys into QSettings.

---

### IN-07: `EncodingError` swallowed without surfacing in per-file status row

**File:** `shared/local_indexer.py:979-986`
**Issue:** When TXT extraction raises `EncodingError`, `_index_one_file` sets `extraction_status='encoding_error'` and `error_msg=str(exc)`, then `_finish_file` records it in `local_files.error_msg`. But the per-file status callback `file_finished_cb` (line 733-736) is invoked with `err=""` (empty string) — the actual error message is dropped. The user sees "Encoding error" in the per-file table but no detail about which encoding was tried.

**Fix:** Pass `error_msg` through `_index_one_file → file_finished_cb`. Update the callback signature consumer in `LocalIndexerWorker._on_file_done` to forward the message. The Status column tooltip can show the detail.

---

_Reviewed: 2026-05-21_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_

---

## Fix Summary (2026-05-21)

This review was processed via a post-review fix loop. The two critical
findings and three best-effort warnings called out by the orchestrator
were all addressed; the user-reported Tantivy IOError and the
user-requested LOCAL Browse feature were folded in.  Each fix is an
atomic commit prefixed `fix(95):` or `feat(95):`.

### Critical findings

**CR-01 — `SearchEngine._current_lab_weights_hash` AttributeError** —
FIXED.  `dynamic_rank_map` and `settings` now read via `getattr` with
safe defaults, so the bare-`SearchEngine` path returns a deterministic
hash without raising.  Call site in `search_composition_logic` wrapped
in try/except for D-37 fallback semantics. Two regression tests added in
`tests/test_local_lab_invalidation.py::TestCR01CurrentLabWeightsHashNoCrash`
that construct `object.__new__(SearchEngine)` WITHOUT hand-attaching
attributes (the masking pattern the previous tests used).

**CR-02 — LAB Composition Search LOCAL hook is dead code** — FIXED.
Wired `LabEngine.__init__` to initialize `local_lab_searcher`,
`_local_lab_index`, `_lab_local_meta`, `local_lab_searcher_stale`; added
real `LabEngine._current_lab_weights_hash`, `_check_local_lab_freshness`,
and `reload_local_lab_index` methods.  `MyLibraryTab` now exposes a
`lab_engine` property and a `_reload_all_local_indexes()` helper that
reloads BOTH engines; all four HIGH-1 reload sites route through it.
Regression tests in `TestCR02LabEngineHasLocalLabHook` (4 assertions).

### Warnings fixed

**WR-01 — `file_id=0` on first index** — FIXED.  `_index_one_file` now
does `INSERT OR IGNORE` on `local_files` with `status='pending'` BEFORE
extraction, so AUTOINCREMENT assigns a real `file_id` that
`_write_page_doc` reads.  `_finish_file` switched from `INSERT OR
REPLACE` (which would reassign `file_id`, breaking already-written
`full_header` values) to `UPDATE` the existing row, with an `INSERT`
fallback.  Regression test
`tests/test_local_indexer.py::test_file_id_populated_on_first_index`
parses the `F`-suffix out of the Tantivy stored `full_header` and
asserts it is non-zero and equals `local_files.file_id`.

**WR-05 — 6.3 MB test fixture in production installer** — FIXED.
Removed `tests/fixtures/local_indexer/hebrew_sample.pdf` from
`GenizahSearchPro.spec`'s `datas` tuple.  The packaging smoke test
(`tests/test_local_pyinstaller_smoke.py`) reads the fixture from the
source tree, not the bundled EXE, so removing it from `datas` has no
effect on the test path.

**WR-08 — `rebuild_local_lab_index` dead code (D-38 invalidation never
fires)** — FIXED.  Added `MyLibraryTab._maybe_rebuild_lab_if_stale()`
that consults `LabEngine._check_local_lab_freshness` and, if stale,
calls `SearchEngine.rebuild_local_lab_index` with the Option C
callbacks. Wired into `_on_worker_finished` (Refresh path) and
`_on_startup_recovery_completed` (startup path).  Defensive — wrapped in
try/except so a LAB rebuild failure never blocks normal flow.
Regression tests in `TestWR08RebuildLabWiring` (3 AST assertions).

### Additional fixes folded in

**Category 2 — User-reported `os error 5` BLOCKER during indexing** —
FIXED.  Added `LocalIndexer._commit_writer_with_retry` that wraps
`writer.commit()` in 3 retries with exponential backoff (250 ms / 1 s /
2 s).  Only retries on the Windows access-denied pattern detected by a
static helper `_is_windows_access_denied()`; all other exceptions
propagate immediately.  On final exhaustion the helper raises a
detailed `ValueError` that names the index directory, retry count,
pending-file count, underlying cause, and remediation guidance.
`_commit_batch` routes through the retry helper.  Regression tests in
`tests/test_local_commit_retry.py` (8 assertions covering detection,
retry success, retry exhaustion, non-AD propagation, `_commit_batch`
integration, and timing).

**Category 3 — User-requested LOCAL Browse render** — FIXED.
`_open_local_browse` reimplemented as a direct-render path that:
(1) resolves filepath via the indexer, (2) sources text from the
search-hit `full_text` field or aggregates pages from the LOCAL
side-index via a new `_get_local_full_text_for_sys_id(sys_id)` helper,
(3) renders into the Browse text widget via `apply_line_numbered_text`,
(4) hides the image pane (D-27), (5) shows the existing "Open file"
button. Bypasses `browse_load` entirely (Genizah-only path).  Entry
point: `ResultDialog` gained a "View in Browse" button (visible for
LOCAL hits only). WR-03 defense-in-depth bonus: both file-launch paths
now reject extensions outside `{.docx, .pdf, .txt}` before
`os.startfile`. Smoke-level tests in `tests/test_local_browse_panel.py`
(10 AST assertions).

### Deferred — not in scope for this fix loop

Reviewed and tracked for a future polish pass:

- **WR-02** — `_iterate_supported_files` vs `prescan_count` ceiling
  semantics (decision needed on whether "5,000 files" means supported
  or total).
- **WR-03** — `os.startfile` defense-in-depth (partially addressed:
  extension guard is now in place for both browse and ResultDialog
  launch paths; the "verify path still under registered folder"
  remediation is still TODO).
- **WR-04** — `datetime.utcnow()` deprecation warning at
  `shared/local_indexer.py:1330`.
- **WR-06** — `_iterate_lab_source_rows` opens a second handle (dead
  code today; needs commit-before-open before LAB rebuild wiring goes
  hot).
- **WR-07** — composite-key normalization in `lists_sync.sync_item_to_cloud`.
- **IN-01** through **IN-07** — code-quality cleanups (LAB/Search
  duplication, misleading function names, comment drift, Help arrow
  direction, etc.).

### Test results

After all fixes, the full local-indexer suite passes:

```
$ pytest tests/test_local_indexer.py tests/test_my_library_tab.py \
    tests/test_corpus_scope_routing.py tests/test_local_filter_*.py \
    tests/test_local_post_dedup_merge.py tests/test_local_lab_invalidation.py \
    tests/test_side_index_merge.py tests/test_local_commit_retry.py \
    tests/test_local_browse_panel.py -q
... 103 passed in ~13s
```

Wider local-indexer suite (incremental, mutex, two-phase, delete-by-uid,
reload, fallback, namespace leak gates, schema evolution, sys_id):

```
... 88 passed, 2 xfailed in ~23s
```

_Fixed: 2026-05-21_
_Fixer: Claude (post-review fix loop)_
