# HANDOFF — LOCAL "My Library" search UI freeze (v7.16) + v7.16 release state

**Date:** 2026-05-31
**Author:** prior session (Claude) — handing off

---

## ✅ RESOLVED (2026-05-31, Codex-assisted) — root cause = bloated search_history.json

**The freeze was NOT in the search path, NOT background-indexing contention, and NOT the LAB churn.** It was **search history persistence**:

- `on_search_finished` calls `_add_regular_search_to_history()` at `genizah_app.py:~17104` — **after the last profiler checkpoint** (`schedule_session_save`), which is exactly why every profiled span looked fast. That method stored `results[:5000]` (full result dicts) into each history entry, and `add_history_entry` → `_load_history_file()` + `_save_history_file()` **loaded and rewrote the ENTIRE `search_history.json` on the UI thread on every search.**
- On this machine that file had grown to **777.93 MB** (40 entries × up to 5000 full result dicts). `json.load` alone = 5.5s; the `indent=2` rewrite is slower; plus GC + UI-thread contention → the constant ~20–30s freeze, **independent of result count and scope**, and at startup-restore (the history menu refresh loads the same file). "Showed results THEN froze" = the history write runs right after render.
- Codex (gpt-5.5) ranked this unprofiled post-checkpoint history write as its #1 suspect; confirmed instantly with `(Get-Item search_history.json).Length` → 777.93 MB.

**Fix shipped (this session):**
1. `shared/session_persistence.py` — history never persists result snapshots: centralized strip in `add_history_entry` (defensive against any caller) + `_load_history_file` self-heals legacy bloated files (new `_strip_history_result_snapshots`).
2. `genizah_app.py` — `_add_regular_search_to_history` / `_add_comp_search_to_history` no longer store `results`/`filtered_results`; clicking a history entry now **re-runs** the search (`_restore_regular_search_from_state` / `_restore_comp_search_from_state`), deferring to `_on_restore_filter_finished` when pre-search filters are recomputing. Legacy entries that still carry a snapshot restore instantly (back-compat).
3. One-time migration (`_tmp/migrate_history.py`) shrank the live file **778 MB → 0.08 MB** (backup at `search_history.json.bak`).
4. Tests: `tests/test_history_no_result_snapshots.py` (4 cases). Ruff clean.

### Second freeze (startup-restore ×3) — ALSO FIXED (2026-06-01)

After the history fix, search no longer froze, but the user still saw the app **freeze 3× at startup-restore**. Root cause (found by inspection): `_restore_session` → `_replay_for_restore()` calls `shared/refinement.py::replay_chain` **synchronously on the UI thread**, and `replay_chain` runs a full `searcher.execute_search()` **per refinement-chain step**. A restored 3-step "search within" chain = 3 full searches on the UI thread = 3 freezes (the "sub-second per step" comment was wrong for genizah-scope steps — that's the deferred ~8s D-F12 cost). Matches "showed results THEN froze 3 times": `on_search_finished` rendered the restored results first (line 24740), then the per-step replay blocked.

**Fix:** new `gui_threads.RefinementReplayThread` runs `replay_chain` off the UI thread; `_replay_for_restore` starts it and applies the rebuilt restrict set in `_on_replay_for_restore_finished` (updates the refinement strip + search-within button). The window stays responsive with the restored results visible. The chain's `RefinementStep` objects are mutated only on the worker and read on the UI thread after the finished signal (no concurrent access). 40 refinement tests still pass; ruff clean. (The console's repeated `LOCAL side-index opened` / `HIGH-1 reload` / LAB-abort lines are the §2c log noise, NOT this freeze.)

### Third freeze (THE real startup freeze) — FIXED (2026-06-01, Codex-confirmed)

The refinement-replay fix above did NOT fix the user's startup freeze (they had no refinement chain). The real cause, found by forensics + a headless timing probe + an independent Codex pass (we converged exactly):

- The frozen library was a **16,462-file HTML folder** (16.8K files). A **28-file PDF folder did NOT freeze** → the freeze scales with **file count, not format**.
- `_UnifiedFileTreeWidget._on_tree_batch` (fires once per `FolderWalkWorker` batch, `BATCH_SIZE=100` → ~165 batches for 16.8K files) called `self._tab._sync_master_optout_checkbox()` **at the end of every batch**. That → `_refresh_folder_checkbox_states` → for **every registered folder** (7 of them) → `_folder_optout_aggregate` → `get_folder_filepaths` (a **full `local_files` table scan** — no index on `folder_id`) + a full membership scan. So one tree population = `O(batches × folders × files)`.
- **Measured on the real quarantined DB:** 14.96s per tree population; the LAB-stale reload churn (§2c — the `lab_index_normalize` AttributeError) repopulates the tree ~3× → **44.9s** = the "~20-30s, 3 times" freeze + "flicker" (tree cleared+rebuilt 3×). The per-folder checkbox aggregate does NOT depend on tree contents, so the per-batch call was pure waste.
- **DISPROVED by headless PyQt timing:** the `ItemIsAutoTristate` per-leaf `setCheckState` O(n²) hypothesis — it's linear (~0.5s for 16.8K). `update_file_status` is O(1). HTML/lxml/GIL not the cause (cache-hit startup doesn't parse).

**Fix:** moved `_sync_master_optout_checkbox()` out of `_on_tree_batch` → call it **once** in `_on_tree_finished` (token-guarded). Measured **14.96s → 0.10s** per population (×3 = 0.31s), which also makes the LAB ×3 churn harmless. Regression guard: `tests/test_my_library_tree_batch_no_per_batch_refresh.py` (AST: `_on_tree_batch` must not call the sync; `_on_tree_finished` must). Codex's other suggestions (index `local_files(folder_id)`; skip reload/refresh on no-op scans; fix the LAB AttributeError; batch UI inserts) are valid follow-ups but unnecessary now that population is 0.10s.

**Related but separate (still open):** `_save_session` also caps `last_results[:5000]` into `session.json` (currently 0.03 MB, harmless now, but a 27k-result search could grow it). The live `_replay_refinement_chain` (D-13 "re-evaluate", user-initiated, not startup) still replays synchronously — lower priority. D-F12 (~8s genizah-scope wall-clock) is unrelated. The LAB-rebuild AttributeError churn (§2c) is now FIXED too (2026-06-01): `SearchEngine._normalize_text` delegated to a missing `self.lab_index_normalize` (it's a `@staticmethod` on `LabEngine`) → every rebuild aborted at the pre-flight probe → `.meta.json` never written → perpetual stale → doomed rebuild re-attempted each startup (the ×3 reload amplifier + console noise). Fix: `_normalize_text` → `LabEngine.lab_index_normalize`; AND `_maybe_rebuild_lab_if_stale` now runs the rebuild on a background `LabRebuildWorker` (single-flight) instead of synchronously on the UI thread (the rebuild iterates all LOCAL pages, ~10s on a large library — would otherwise be a new freeze). Test gap closed (`tests/test_lab_normalize_callback.py` — the existing `build_lab_side_index` tests used a stub normalize_fn and never hit the real `SearchEngine` wiring). Remaining optional: index `local_files(folder_id)`; skip reload/refresh on no-op scans.

Everything below is the **original handoff** (pre-resolution), kept for the investigation record.

---

**Status (original):** v7.16.0 release HALTED for UAT fixes. 6 UAT bugs fixed + tested. **One blocker remains: a ~20–30s UI freeze on (apparently) every LOCAL search that I could NOT reproduce or pin remotely.** This doc captures the problem, everything ruled out, the fixes shipped, and concrete next steps.

---

## 0. TL;DR for the next investigator

- The **search code path is fast** — proven by the in-app profiler: `execute_search` = 0.01s, full `on_search_finished` ≈ 0.35s, LOCAL index reopen = 0.00s. **Yet the user reports the window freezes ~20–30s on every search.**
- **Therefore the freeze is OUTSIDE the instrumented span** — it fires *after* the last profiler checkpoint, or asynchronously (a worker-finished callback, a debounced timer, Qt paint/layout), or it's **background-indexing GIL/IO contention** running in parallel with searches.
- **The single highest-value next step:** add a **Qt event-loop stall watchdog** that logs a Python stack trace whenever the GUI event loop is blocked > ~2s. That will catch the freeze wherever it actually is, instead of guessing. (Sketch in §6.)
- **Second:** confirm whether a background `LocalIndexerWorker` / auto-rescan / "Re-index All" is **running during** the freeze. The freeze correlates with `Tantivy writer.commit() ... Access is denied (os error 5)` lines in the console (reader-blocks-writer on Windows during re-index).

---

## 1. The blocking bug — symptoms (verbatim from user)

> "the window freezes for ~10s when searching LOCAL" (initial report)
> "It freezes every search."
> "Every corpus, even when 1 result comes back (or 27022). It froze also in the start, when I chose to restore the search. It showed the results and THEN froze, 3 times. The freezes are ~20-30s actually."

Key properties:
- **Constant** ~20–30s, **independent of result count** (1 result *and* 27022 both freeze) → not an O(results) cost.
- **Every corpus scope** (Genizah / Local / ALL) → not specific to the LOCAL query.
- Happens **at startup-restore too** (3× — "showed results THEN froze").
- "Showed the results and **THEN** froze" → strongly implies the freeze is **after** render, not during the query.

Environment: Windows 11, desktop app run from source (`python genizah_app.py`), ~12K-PDF LOCAL library at `C:\Users\gersh\Genizah_Tantivy_Index\LocalIndex`. The library needed/needs a **"Re-index All"** for the v7.16 extractor improvements (`extraction_format_version` 2→3).

---

## 2. Evidence gathered

### 2a. In-app profiler (gated by `GENIZAH_PROFILE_SEARCH=1`)
Added this session in three places (all `print(... flush=True)`, off unless the env var is set):
- `gui_threads.py::SearchThread.run` — times `execute_search`.
- `genizah_app.py::on_search_finished` — phase checkpoints `prime_filepath_cache / pre_render / load_next_batch / resize_columns / launch_enrichment / schedule_session_save`.
- `genizah_core.py::_open_local_searcher` — times the LOCAL index reopen.

**User's captured output (a LOCAL search, "literal" mode, 57 hits):**
```
[PROFILE] _open_local_searcher (LOCAL index reopen) took 0.01s   (startup)
[PROFILE] _open_local_searcher (LOCAL index reopen) took 0.00s   (startup x2 — LAB churn)
[PROFILE] execute_search(scope=local, mode=literal) -> 57 hits in 0.01s
[PROFILE] on_search_finished prime_filepath_cache: +0.00s (n=57)
[PROFILE] on_search_finished pre_render: +0.00s (n=57)
[PROFILE] on_search_finished load_next_batch: +0.33s (n=57)
[PROFILE] on_search_finished resize_columns: +0.01s (n=57)
[PROFILE] on_search_finished launch_enrichment: +0.00s (n=57)
[PROFILE] on_search_finished schedule_session_save: +0.00s (n=57)
```
**Total measured ≈ 0.35s. No freeze inside the measured span.** The user nonetheless reports the freeze on every search.

⚠️ **Caveat to verify:** the profiled search was `scope=local, mode=literal, 57 hits`. It is POSSIBLE the user pasted a search that did NOT freeze, while other searches (different scope/mode/size) do. **Next investigator: capture `[PROFILE]` output from a search that DID visibly freeze, and note the wall-clock gap between the last `[PROFILE]` line and when the window becomes responsive.** That gap localizes the freeze to "after on_search_finished".

### 2b. Console-log correlation (the strongest lead)
**Freeze session #1 console** contained FOUR lines like:
```
Tantivy writer.commit() hit Windows access-denied (attempt 1/4, dir=...\LocalIndex):
  An IO error occurred: 'Access is denied. (os error 5)'
```
plus `WARNING: LOCAL index query failed: ValueError("Syntax Error: אמ'")`.

**Fast (non-freeze) profiled session** had **ZERO** access-denied lines.

`os error 5` on `writer.commit()` = a **writer (background re-index) trying to commit to the LocalIndex while the live `SearchEngine.local_searcher` holds the directory open** — classic Windows reader-blocks-writer. The retry helper (`shared/local_indexer.py::_commit_writer_with_retry`, ~line 3320) backs off 0.25s → 1s → 2s per file (≈3.25s × N files). This — and/or the CPU-heavy PDF re-extraction running on the `LocalIndexerWorker` thread holding the GIL — is the most plausible source of a UI freeze that is *independent of the search itself*.

### 2c. LAB rebuild churn (real bug, but NOT the freeze)
Console shows on every startup/reload:
```
[LAB] LOCAL LAB has no .meta.json — stale
build_lab_side_index: pre-flight callback probe failed
  (AttributeError("'SearchEngine' object has no attribute 'lab_index_normalize'"))
  — aborting LAB rebuild ...
```
- `lab_index_normalize` is defined on **`LabEngine`** (`genizah_core.py:849`) but the SearchEngine rebuild callback `_normalize_text` (`genizah_core.py:~7073`) calls `self.lab_index_normalize` → **AttributeError on a SearchEngine instance**.
- Result: the LOCAL LAB side-index rebuild **aborts every time** → `.meta.json` never written → permanently "stale" → `_maybe_rebuild_lab_if_stale` (`desktop/my_library_tab.py:1096`) retriggers it on every startup/mutation → repeated `reload_local_indexes()` → repeated index reopens.
- **BUT** the profiler shows each reopen is **0.00–0.01s**, so this churn is **log noise, not the freeze.** Still worth fixing (see §5).

---

## 3. Hypotheses RULED OUT (with measured evidence)

| Hypothesis | Verdict | Evidence |
|---|---|---|
| Per-hit `get_filepath()` SQLite on UI thread (in `load_next_batch` + `_apply_local_optout_filter`) | ❌ not it | 4000-doc query = 0.10s; profiler `prime_filepath_cache +0.00s` |
| LOCAL highlight loop over up to 50K candidates | ❌ not it | 4000-doc exact query (incl. highlight) = 0.10s |
| FJMS `get_measurement_summaries_batch` over LOCAL ids (UI thread) | ❌ not it | 30K ids = 0.03s |
| LOCAL Tantivy index reopen (`_open_local_searcher`) | ❌ not it | profiler = 0.00–0.01s |
| LAB rebuild churn / reopen | ❌ not it (log noise) | reopens 0.00s; rebuild aborts fast on AttributeError |
| The search query itself (`execute_search`) | ❌ not it (for local/literal) | 0.01s. **UNTESTED for scope=genizah/all** — verify. |

---

## 4. Hypotheses REMAINING (ranked) — what to test next

**H1 (top) — The freeze is after the last checkpoint / asynchronous.** The profiler's last checkpoint is `schedule_session_save`; everything after it in `on_search_finished` (the `_notify_search_complete`, `reset_ui`, statusbar message) is uninstrumented, AND several things fire LATER on the event loop:
  - **Enrichment worker FINISH callbacks** on the UI thread: `_on_domain_enrichment_loaded`, `_on_pgp_badges_loaded`, `_on_printed_badges_loaded` (connected in `_launch_enrichment_workers`, `genizah_app.py:~17380`). These run a few seconds after results render → matches "showed results THEN froze". NOTE: this session added a LOCAL-skip so these are NOT launched for an all-LOCAL result set — so **if a pure `scope=local` search still freezes, enrichment callbacks are NOT the cause.** Test both scopes.
  - **Debounced `_save_session`** fires on a timer AFTER the search (`_schedule_session_save` → `genizah_app.py:~24515`; `_save_session` → `:24446`). It serializes `last_results[:5000]` to JSON on the UI thread. Capped at 5000 and "even 1 result freezes" argues against it, but confirm.
  - **Qt deferred layout/paint** of `results_table` (e.g. `resizeColumnToContents`, row rendering). Python checkpoints do not capture Qt's event-loop paint pass.

**H2 — Background re-index commit contention (reader-blocks-writer).** Correlates with the `os error 5` console lines (present in freeze sessions, absent in the fast one). If a `LocalIndexerWorker` / auto-rescan / "Re-index All" is active, (a) its `writer.commit()` retries 0.25+1+2s per file against the live reader, and (b) its CPU-heavy PDF re-extraction (PyMuPDF + `shared/local_indexer_rtl` de-space) holds the GIL → starves the UI thread → freeze, *independent of the search*. **This best explains "every search, every corpus, even 1 result, also at startup."** The user's library had pending re-extraction (`extraction_format_version` 2→3).
  - **Test:** add a log line whenever `LocalIndexerWorker` starts/stops and whenever `_commit_writer_with_retry` is entered; correlate with the freeze. Check `MyLibraryTab._worker` / scan-run state during a freeze.

**H3 — D-F12 (known ~8s regular-search wall-clock).** Contradicted by the profiler for `scope=local` (0.01s), but the profiled search was LOCAL-only. **Profile a `scope=genizah` search** — if `execute_search` there is ~8–10s, the "every corpus" freeze is partly the known deferred D-F12 (the two-phase Tantivy→regex over up to 50K candidates), which is GIL-bound and would freeze the UI.

---

## 5. Fixes SHIPPED this session (uncommitted in working tree) — all tested

All of the following are in the working tree on top of commit `32a31f62` (tag `v7.16.0`), NOT yet committed. 67 LOCAL/extraction + 22 tab + cascade/batch tests pass; ruff clean; `check_docs` healthy.

### v7.16 UAT bug fixes (the 6 the user reported earlier — confirmed working)
1. **HTML `&nbsp` literal** — `shared/local_indexer.py::_clean_html_text` (html.unescape + NBSP→space), applied in `extract_html_pages`. ✅ user-confirmed.
2. **XLSX invisible to search** — `extract_xlsx_pages`: guard all-empty rows (`any(cs.strip() ...)`), and `data_only=False` fallback when the value pass yields nothing (uncached formulas). ✅
3. **CSV UTF-16** — `extract_csv_pages`: detect UTF-16 BOM (+ NUL-ratio guard) before cp1255. ✅
4. **Folder opt-out didn't filter** — `_UnifiedFileTreeWidget._on_item_changed` cascades a folder's definite state to leaves (`_set_descendant_leaves_check_state`). ✅
5. **Per-folder opt-out checkboxes** in the folders list (`[x]`/`[ ]` per row) — `LocalIndexer.get_folder_filepaths`, `MyLibraryTab._on_folder_checkbox_changed`, `_refresh_folder_checkbox_states`, `_folder_optout_aggregate`. ✅ user-confirmed. (Replaced an earlier single "Search all files" master checkbox the user rejected.)
6. **`אמ'` parse crash** — `_query_local_index` sanitizes Tantivy query metacharacters on parse failure (`genizah_core.py:~7114`). ✅ user-confirmed crash gone. (Full apostrophe-word *matching* still limited — see §5 note + `apostrophe-geresh-search-discrepancy.md`.)

### Freeze-related changes (kept, but did NOT fix the freeze)
- **Batched filepath lookup** — `LocalIndexer.get_filepaths(sys_ids)` + `genizah_app._prime_local_filepath_cache` / `_local_filepath_cache` (primed in `on_search_finished` + comp finish; consulted by `_lookup_local_filepath`). Removes N per-row SQLite round-trips. Correct improvement; not the freeze.
- **LOCAL-skip enrichment** — `_launch_enrichment_workers` filters LOCAL 97-prefix ids out of domain/PGP/printed/measurement enrichment (pointless for LOCAL), and still runs `_apply_results_table_filters()` for all-LOCAL sets.
- **Gated profiler** — `GENIZAH_PROFILE_SEARCH=1` (see §2a, §6).

### Known-limitation note (apostrophe, secondary)
`build_regex_pattern(["אמ'"])` returns empty `()`, and the LOCAL content tokenizer indexes `אמ'` (U+0027) such that the bare token `אמ` does not match (U+05F3 geresh `אמ׳` DOES work). Full apostrophe/gershayim LOCAL search requires aligning the LOCAL query-builder with the content tokenizer — part of the documented **MEDIUM-1** LOCAL query-builder divergence. Pre-existing; see `.planning/debug/apostrophe-geresh-search-discrepancy.md`.

---

## 6. Repro & diagnostics

### Run with profiler (no rebuild needed — app runs from source)
```powershell
$env:GENIZAH_PROFILE_SEARCH=1 ; python genizah_app.py
```
Do a search **that freezes**; paste all `[PROFILE]` lines + note the wall-clock gap to responsiveness. Profile BOTH `scope=local` and `scope=genizah`.

### Recommended next instrument — Qt event-loop stall watchdog
Drop this in `GenizahGUI.__init__` (after the event loop exists). It logs a stack of the MAIN thread whenever the event loop is blocked > 2s — i.e., it will print exactly where the freeze is, regardless of which callback/timer/paint causes it:
```python
import faulthandler, threading, time
class _UiStallWatchdog:
    def __init__(self, gui, threshold=2.0):
        self._last = time.monotonic()
        self._main = threading.get_ident()
        # heartbeat on the UI thread
        from PyQt6.QtCore import QTimer
        self._t = QTimer(gui); self._t.timeout.connect(self._beat); self._t.start(200)
        threading.Thread(target=self._watch, args=(threshold,), daemon=True).start()
    def _beat(self): self._last = time.monotonic()
    def _watch(self, threshold):
        while True:
            time.sleep(0.5)
            if time.monotonic() - self._last > threshold:
                import sys
                faulthandler.dump_traceback(file=sys.stderr)   # dumps ALL threads incl. blocked UI
                self._last = time.monotonic()
```
The dumped traceback of the main (UI) thread during the stall is the answer. If instead the main thread looks idle but a `LocalIndexerWorker` thread is deep in PDF re-extraction / `writer.commit()`, that confirms H2 (background-indexing GIL/IO contention).

### Also check during a freeze
- Is `MyLibraryTab._worker` running? Is `start_recovery_probe()` returning running scan-runs? Is an auto-rescan / Re-index All in progress?
- Grep the console for `Tantivy writer.commit() ... Access is denied` during the freeze window.

---

## 7. Things to consider / candidate fixes

- **H2 fix (if confirmed):** the live `SearchEngine.local_searcher` holds the LocalIndex open, blocking the indexer's `writer.commit()` on Windows. The existing `_commit_writer_with_retry` is a band-aid (it IS the 0.25+1+2s delay the user feels). Real fix: **release/close the live reader (`local_index`/`local_searcher` → None) around the indexer commit, then `reload_local_indexes()` after** — coordinate via the `MyLibraryTab` ↔ `SearchEngine` handoff (`reload_local_indexes` already exists for the reopen half). Alternatively, ensure re-indexing never runs concurrently with interactive search, or run the indexer in a separate process.
- **LAB churn fix:** add `lab_index_normalize` to `SearchEngine` (or make `_normalize_text` not depend on it) so the rebuild can succeed once → writes `.meta.json` → fresh → stops re-triggering. BUT note the full rebuild is ~10s ("1.9M-row" warning), so it must run on the background worker, never the UI thread, and ideally only when the user actually uses LAB-mode composition. Simplest interim: stop `_maybe_rebuild_lab_if_stale` from re-triggering a rebuild that will abort (cache the "callback missing" state), to kill the log churn.
- **D-F12:** if the genizah-scope profile shows ~8s, decide whether to tackle the two-phase search cost (cap candidates, move regex off the GIL / to a process, or stream results) — larger architectural work, previously deferred.
- **Apostrophe (MEDIUM-1):** align LOCAL query-builder + tokenizer for geresh/gershayim; or route LOCAL through the same candidate mechanism as the Genizah search instead of `parse_query`.

---

## 8. Release state (v7.16.0 "Hebrew PDF Text Quality", desktop-only)

- **HEAD = `32a31f62` "release: v7.16.0 — Hebrew PDF Text Quality"**, **tag `v7.16.0`** — both **UNPUSHED**. This commit contains the Phase 102 extractor bundle + the file-actions feature, but with a **broken `reveal_local_file`** (combined `/select,<path>` form — opens "My Documents" for paths with spaces).
- **Working tree (uncommitted):** the `reveal_local_file` revert (separate-argv form + spaces regression test) AND all the §5 UAT fixes + profiler + handoff.
- `version.py` / `version_info.txt` / `CompileScriptGenizah.iss` / `README.md` / `tests/test_release_artifacts.py::_TARGET_VERSION` already at `7.16.0`.
- `GenizahSearchPro.spec` was restored after a build overwrote its curated `collect_all('pymupdf')` — keep it out of the build's auto-regeneration / re-restore before committing.
- **Before pushing/tagging:** decide whether the freeze blocks v7.16 or ships as a documented known issue (`docs/OPEN_ISSUES.md` D-F23). Then: rebuild installer (the prior installer has the broken reveal), user re-test, amend/commit, move tag `v7.16.0`, push, watch CI (installs from `requirements-lock.txt`), create GitHub Release with installer (desktop release → required so desktop users get the update).
- Deferred follow-up logged: **D-F17** (export module not adapted to LOCAL → `/gsd-quick`).

### Build commands that worked this session
- PyInstaller: `cmd /c 'cd /d C:\Genizahsearch & .\build_app.bat'` (PowerShell tool, background, 600000ms).
- Inno Setup: `& 'C:\Program Files (x86)\Inno Setup 6\ISCC.exe' 'C:\Genizahsearch\CompileScriptGenizah.iss'`.

---

## 9. Key file:line index
- `gui_threads.py:66` SearchThread.run (search off UI thread + profiler)
- `genizah_app.py:~16920` on_search_finished (+ profiler checkpoints); `:~16693` load_next_batch; `:~17380` _launch_enrichment_workers (+ LOCAL skip); `:~17496` _apply_local_optout_filter; `:~18740` _lookup_local_filepath; `:~24446` _save_session; `:~24515` _schedule_session_save; `:~2873` _local_filepath_cache init
- `genizah_core.py:8280` execute_search; `:7078` _query_local_index (+ apostrophe sanitize); `:~6833` _open_local_searcher (+ profiler); `:6919` reload_local_indexes; `:6997` _check_local_lab_freshness; `:849` lab_index_normalize (LabEngine); `:~7073` _normalize_text (broken callback); `:~1937` get_filepaths
- `shared/local_indexer.py:3320` _commit_writer_with_retry; `:4279` build_lab_side_index (pre-flight probe); `:~1937` get_filepaths; `:~2015` get_folder_filepaths; `:1299` extract_html_pages/_clean_html_text; `:1407` extract_xlsx_pages; `:1505` extract_csv_pages
- `desktop/my_library_tab.py:1096` _maybe_rebuild_lab_if_stale; `:1071` _reload_all_local_indexes; `:~548` _on_item_changed (cascade); `:~2569` _refresh_folder_list_ui (per-folder checkboxes); `_on_folder_checkbox_changed` / `_refresh_folder_checkbox_states` / `_folder_optout_aggregate`
