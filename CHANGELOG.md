# Changelog

All notable changes to Genizah Search Pro will be documented in this file.

---

## [7.16.0] - 2026-05-31 — Hebrew PDF Text Quality

A major overhaul of how LOCAL Hebrew PDFs are read into the My Library index, dramatically improving search of typeset Hebrew scholarly books, plus new file-management actions for LOCAL hits. Desktop only.

### New Features

- **Open file location (desktop)** — LOCAL "My Library" hits now have an **Open file location** action that opens the containing folder in the OS file manager with the file selected, alongside the existing **Open file**. Available in the ResultDialog, the Browse panel, and the right-click menu on search results.
- **File-aware right-click menu for LOCAL hits (desktop)** — Right-clicking a LOCAL search result now shows file actions — **Open file**, **Open file location**, **Copy file location**, **Copy filename** — instead of the Genizah cloud-community actions (View Document, corrections, comments, discoveries) that don't apply to your own documents.

### Improvements

- **Hebrew PDF text quality — letter-spacing & word-boundary rewrite (desktop)** — A large rewrite of the LOCAL PDF Hebrew text extractor, dramatically improving search of typeset Hebrew scholarly books. Many books shattered emphasised/justified Hebrew into single letters (e.g. `אוצר הגאונים` indexed ~74% one-letter "words", so `פירוש המשנה` became `פירו ש ה מ ש נה` and was unsearchable), while tightly-set books fused whole phrases into one token. Word boundaries are now detected per line from the actual inter-letter spacing (an adaptive valley between within-word and between-word gaps), plus the embedded space glyph where a heading or citation is set with no visible gap. Measured on identical pages: `אוצר הגאונים` one-letter tokens 74% → ~5%, tightly-set books' word-fusion 16% → ~0%.
  - **Vowel/cantillation handling** — Hebrew combining marks (nikud and te'amim) are now classified by Unicode category, so the maqaf (`־`), sof-pasuq and similar punctuation are kept as real characters instead of being stripped as vowels (which had corrupted ranges like `סב־סג` and joined `דו־שיח`).
  - **Numbers read correctly** — Years and page numbers embedded in right-to-left text are no longer reversed (`1977` is `1977`, not `7791`; ranges like `194-256` stay intact).
  - **Garbled text layers flagged** — PDFs whose embedded text layer is unrecoverable garbage are now detected and marked in the My Library tree, so you know which files would need OCR rather than silently indexing noise.
  - **To benefit, run "Re-index All" (אנדקס מחדש הכל)** in the My Library tab — these improvements apply to newly-extracted text, so existing libraries must be re-indexed once (`extraction_format_version` 2 → 3).

### Bug Fixes

- **`.html` / `.xlsx` / `.csv` LOCAL files can now be opened (desktop)** — The "Open file" button refused to open these three formats even though My Library indexes them; the extension gate now uses the same supported-set as the indexer (centralized in `desktop/file_actions.py`, no longer a duplicated literal).
- **App launch no longer freezes after an interrupted "Re-index All" (desktop)** — If a bulk re-index was interrupted, the next launch could hang before the window appeared because every pending file was re-extracted synchronously on the UI thread. Recovery now defers that work to the background worker, so the app opens immediately and re-extracts in the background.
- **Four silent / crash-class bugs** from a code audit are fixed (error-handling and edge-case hardening across the indexer and a shared service path).

### Known Limitations

- Some PDFs encode a maqaf or word-space as a drawn graphic or omit it entirely from the text layer (e.g. certain abbreviation-table cells where `כתבי־יד` was typed as `כתבייד`). Such cases cannot be recovered from text extraction — they would require OCR — and are extracted faithfully to the PDF's actual content.
- The xlsx/Word/JSON **export** is still tuned for Genizah corpus results and is not yet adapted to LOCAL hits (deferred to a follow-up; LOCAL search shipped in v7.14 and export was never adapted).

### Internal

- Phase 102: `extract_pdf_pages` rewritten onto a rawdict-primary path with pure RTL glyph-trace reconstruction helpers (`shared/local_indexer_rtl.py`), Unicode-`Mn` nikud classification, per-line 1-D Otsu word-gap valley, and a `_ltr_damage_guard` RTL-trust fix. Nikud is stripped once in `_write_page_doc` for all formats (searchable). `corrupt_encoding` status + SQLite migration 2→3. 4-wave execution, 5 plans, ~150 new/updated tests.
- New `desktop/file_actions.py` centralizes LOCAL open/reveal/copy-path actions (gated on the shared `_SUPPORTED_EXTENSIONS`); `reveal_local_file` uses the absolute Explorer path + Microsoft's documented `/select,<path>` form. Unit-tested in `tests/test_file_actions.py`.

---

## [7.15.0] - 2026-05-28

### New Features

- **PDF page image in My Library (desktop)** — When viewing a LOCAL PDF search result, the actual scanned/typeset PDF page is now shown alongside the extracted text in both the ResultDialog and the Browse panel. Navigation (prev/next result, prev/next page) keeps the image in sync with the text. Non-PDF LOCAL files (`.docx`/`.html`/`.xlsx`/`.csv`/`.txt`) remain text-only by design.
- **Re-index All button (desktop)** — New button in the My Library tab that forces re-extraction of every committed file via the background worker. Use it to pick up extractor improvements (such as the RTL and reflow fixes in this release) without losing your library or opt-out preferences.

### Scalability & Indexing Reliability

- **Indexes large folders without UI freezes (desktop)** — Folder enumeration runs on a background worker (responsive within 100ms), tree population is async, and the progress bar shows indeterminate "Discovering files…" status while enumerating, then switches to determinate progress during indexing. Previously, large folder selections could freeze the UI for minutes.
- **Resume interrupted indexing (desktop)** — If indexing is interrupted (app crash, hard kill, system reboot, power loss), the next launch detects the orphaned scan and offers three choices: **Resume**, **Restart**, or **Skip**. Resume picks up where it left off via the new `scan_runs` lifecycle table — no more starting over from zero.
- **Reset My Library button (desktop)** — Two-step typed-confirm destructive action (accepts `RESET` or `אפס`) that performs a 7-step atomic teardown of the LOCAL index. Use when the index is in an unrecoverable state and a clean start is the fastest path forward.

### Improvements

- **Hebrew PDF text quality (desktop)** — Two extractor fixes dramatically improve LOCAL search of Hebrew scholarly books:
  - **Word-order RTL fix** — On PDFs whose content stream emits words in left-to-right visual order, Hebrew sentences were previously indexed last-word-first. Word tokens are now reversed via directional-run analysis; embedded Latin shelfmarks like `T-S 12.123` stay adjacent.
  - **Intra-block reflow** — PyMuPDF's bidi engine sometimes splits Hebrew paragraphs into one-fragment-per-line output (characters, commas, even quotation marks on their own line). LOCAL PDF extraction now collapses these intra-paragraph line breaks into continuous prose. Paragraph boundaries from PyMuPDF are still preserved.
- **PDF page rendering architecture (desktop)** — Page images are rendered lazily and on-demand via a shared `PdfRenderWorker` background thread, backed by a bounded LRU of open document handles. No on-disk image cache; only currently-viewed pages live in memory, so the corpus is never bulk-rendered.

### Bug Fixes

- **LOCAL/Genizah header field leak in ResultDialog (desktop)** — In ALL search mode, navigating prev/next between LOCAL and Genizah results would leak the prior result's library/shelfmark/title into the new one. LOCAL hits now populate the same `lbl_shelf` + `lbl_title` widgets Genizah hits use, so navigation cannot carry stale values.
- **Graceful PDF render failures (desktop)** — Missing files, corrupt/encrypted PDFs, out-of-range pages, and render exceptions now show a placeholder with a logged error instead of hanging the UI.

### Internal

- New `desktop/pdf_image_controller.py` (`PdfImageController` + token + latest-wins + 150ms debounce + ~8s watchdog) coordinates render requests from both UI surfaces.
- New `shared/local_indexer.py::_collapse_intra_block_newlines` + `_fix_sort_true_rtl_line` / `_fix_sort_true_rtl_page` helpers for the Hebrew extraction fixes.
- New `LocalIndexer.mark_all_pending_for_reindex()` helper backs the Re-index All button by flipping `processed_files.status='committed'` → `'pending'`.
- v7.15 milestone closed: 3 phases (99, 100, 101), 7 plans, 6/6 PDFIMG-* requirements satisfied.

---

## [Unreleased]

### Phase 97.3 — My Library UAT Stability (2026-05-26 — internal hotfix, no version bump)

Closes the six post-Phase-97.2 UAT defects reported 2026-05-26 against the desktop My Library tab. Internal hotfix on the v7.14 chain (97.1 → 97.2 → 97.3); no public release, no GitHub tag.

**R97.3-A — UI-thread freeze on folder selection (Bug A).** Replaced the synchronous recursive walk in `_UnifiedFileTreeWidget._populate_node` with an async tree-worker design. `FolderWalkWorker` (Phase 97 U-03, previously unwired) now powers the tree fill: it walks with `os.walk(folder, followlinks=False)`, pre-filters by `_SUPPORTED_EXTENSIONS` imported from `shared/local_indexer.py` (single source of truth — also closes R97.3-N), runs `_canonical_filepath` inside the worker thread, and emits 4-tuples `(filepath, canonical, mtime_ns, size)` plus a monotonic generation token. All three worker signals (`batch_emitted`, `finished_signal`, `error_signal`) carry the token; stale payloads from a cancelled or superseded worker are dropped at the UI slot. `_UnifiedFileTreeWidget` gained `_tree_token`, `_tree_worker`, `_cancel_existing_tree_worker`, `_ensure_dir_node`, `_on_tree_batch`/`_on_tree_finished`/`_on_tree_error` slots. Tree now starts collapsed (no `expandAll()` — D-04). Cancel mid-populate clears the tree entirely (D-05).

**R97.3-A — `prior_status` cache (Codex Critique #2 v7.14 blocker).** Added `MyLibraryTab._prior_status_cache` populated at `_init_indexer` and `_invalidate_prior_status_cache()` called BEFORE `_refresh_folder_list_ui` in `_on_worker_finished`, `_on_worker_error`, `_perform_reset`, folder-add, and folder-remove (D-12 ordering invariant — late clearing would leave the post-scan tree showing pre-scan status because `_refresh_folder_list_ui` calls `populate_for_folder` at line 1995 which reads the cache). The click path now never issues a `local_files` DB query.

**R97.3-B — Reset button accessible after a crash (Bug B).** Simplified `_update_reset_button_state` to a single condition: enabled when `self._worker is None or not self._worker.isRunning()`. The Phase-97.2 `start_recovery_probe()` check is gone — orphan `scan_runs.status='running'` rows are exactly what Reset is supposed to clean up. `LocalIndexer.reset_my_library()`'s own 7-step protocol (path-safety pre-check + handle-close + retry-rename + LAB-rollback + fail-loud + deferred-GC) remains the load-bearing safety; the UI guard does not duplicate it. Phase 97.2 `test_reset_my_library_full_cycle` and `test_reset_my_library_lab_rename_failure_rolls_back_local` still GREEN.

**R97.3-C — MuPDF stderr noise silenced (Bug C).** `shared/local_indexer.py` calls `fitz.TOOLS.mupdf_display_warnings(False)` at module import, wrapped in `try/except Exception` with `logger.debug` fallback (broad exception per Codex Critique #2 — a future PyMuPDF API change must not crash module import). User's smoke folder went from 624× "MuPDF error: ... unknown keyword: 'TF'" stderr lines to zero.

**R97.3-D — Recovery-Skip suppresses same-launch auto-rescan (Bug D).** New one-shot `_skip_startup_rescan_once: bool` flag on `MyLibraryTab`. Initialised `False` before the recovery-modal call path. Set `True` in the Skip branch of `_show_recovery_modal`. Read-and-cleared at the entry of `_auto_rescan_on_startup`. Resume and Restart branches do NOT set the flag (both intentionally trigger a fresh scan). D-25 silent rescan on the no-modal path is unchanged. Bilingual status-bar message ("Recovery skipped. Use Refresh to rescan. / ההתאוששות דולגה. לחץ Refresh לסריקה מחדש.") auto-fades after 5 seconds.

**R97.3-E — "Discovering files…" status during scan enumeration (Bug E).** `LocalIndexerWorker` emits new `status_updated(str)` signal with bilingual "Discovering files… / מאתר קבצים…" before `scan_all` enters its enumeration loop. `_start_worker` puts `QProgressBar` in indeterminate (busy) mode via `setRange(0, 0)`. First `progress_updated` signal flips back to determinate `setRange(0, 100)` and clears the status message. Finish/cancel/error all reset the range to `(0, 100)` so a future scan does not inherit busy state (D-21).

**R97.3-N — UI tree shows the full supported-extensions set (Bug N3).** The UI-side `SUPPORTED = {'.pdf', '.docx', '.txt'}` literal at `desktop/my_library_tab.py:227` is DELETED. `FolderWalkWorker` imports `_SUPPORTED_EXTENSIONS = {".docx", ".pdf", ".txt", ".html", ".xlsx", ".csv"}` from `shared/local_indexer.py:81` (single source of truth). `.html`/`.xlsx`/`.csv` now appear in the opt-out tree; mixed-case (`.PDF`, `.Pdf`) is normalized via `.lower()`.

**Tests added.** Seven new test files covering D-13 (token guard), D-14 (no canonicalize of unsupported), D-15 (no junction recurse via `mklink /J`), D-19 (cache invalidation ordering), D-20 (tri-state preservation across async populate), D-21 (progress range round-trips), D-22 (100ms responsiveness via `time.perf_counter()` + `QTimer.singleShot(0, marker)` + `QApplication.processEvents()`). Existing `tests/test_folder_walk_worker.py` extended for the 4-tuple + token signal shape.

**Codex review trail.** `97.3-CODEX-CRITIQUE.md` (Area 1 sub-decisions — revised Option B + worker pre-filter; flagged the `prior_status` preload risk) and `97.3-CODEX-CRITIQUE-2.md` (full decision-set — surfaced D-11 broad-exception, D-12 cache ordering, D-16..D-22 + the inverted wave order for risk locality) both addressed before plan execution.

**No version bump.** `version.py`, `version_info.txt`, `CompileScriptGenizah.iss`, `README.md`, `tests/test_release_artifacts.py` UNTOUCHED. Phase 97.3 rides the v7.14 internal-hotfix chain (97.1 → 97.2 → 97.3); the next user-facing release that bundles all three is a separate decision.

### Phase 97.2 — LOCAL Recovery Cascade Fix + Reset My Library (internal; 2026-05-26)

Internal closeout — not yet a user-facing release. Bundles the 97.1 MAX_PATH +
non-blocking cancel hotfix (commit `2e1b846e`, 2026-05-25) with the 97.2 8-bug
recovery cascade fix and the new "Reset My Library" / "אפס ספריה שלי" toolbar
action in the desktop My Library tab.

**Trigger:** 2026-05-26 cascade on the first post-97.1 run. User stopped a scan
of a 100K-file Dropbox folder mid-run, restarted the app, clicked "Remove
folder" — console emitted `Schema error -> LockBusy x3 -> 'Field scan_run_id is
not defined' -> 'NoneType has no delete_documents'`. Codex critique identified
the missing `.schema_version` marker check (Bug 6 / R97.2-F) as the actual root
cause: Phase 95 installs have `meta.json` but no `.schema_version`, so the
existing `actual_marker is not None and actual_marker != expected_marker` guard
failed to trip the rebuild path on upgrade.

**8 fixes landed in `shared/local_indexer.py` + `genizah_core.py`:**
- R97.2-F (Bug 6) — schema-marker absence triggers rebuild in BOTH files
- R97.2-A (Bug A) — redundant `tantivy.Index(...)` reopen deleted at
  `local_indexer.py:1147`; temp-indexer in `genizah_core.py:6878-6896` now
  explicitly closes via `_close_internal_writer_index()` in `try/finally`
- R97.2-B (Bug B) — explicit `fresh_writer = None; fresh_index = None;
  gc.collect()` at `:2745-2748` replaces `del` (Windows: `del` is weak,
  Rust drop is delayed, `os.rename` then races a live lock file)
- R97.2-C (Bug C) — `discard_run` step 2 introspects schema for
  `scan_run_id` field; on absence (Phase 95 schema) falls back to per-uid
  `delete_documents("unique_id", uid)` loop joined from
  `local_pages × processed_files WHERE scan_run_id=?`
- R97.2-G (Bug C2) — explicit `_del_writer = None; gc.collect()` in
  `discard_run` step 2 `finally:` before step 5 reopens `self._writer`
- R97.2-H (Bug C3) — `discard_run` raises `LocalIndexerError` BEFORE the
  SQLite delete transaction when Tantivy delete fails; prevents
  orphaned-docs state (SQLite empty, Tantivy still has the rows)
- R97.2-D (Bug D) — new `LocalIndexerError(RuntimeError)` exception class
  + new `_ensure_writer()` helper (fail-loud: raises on schema mismatch
  or LockBusy, NO silent retry past `__init__`'s 3-attempt loop); wired
  at `_delete_file`, `remove_folder`, `_recover_pending_deletes` call
  sites
- R97.2-E (Reset My Library) — new
  `LocalIndexer.reset_my_library(close_searcher_cb, reload_searcher_cb)`
  7-step protocol (close handles -> 7 path-safety pre-checks (basenames
  must equal `LocalIndex`/`LocalLabIndex`, parents match, not root, etc.;
  raise `LocalIndexerError` before any filesystem mutation) -> rename-aside
  LOCAL to `.reset-quarantine-<ts>` -> rename-aside LAB with **rollback of
  LOCAL on failure** -> recreate empty dirs -> schedule deferred cleanup
  via `pending_dir_cleanup` (Phase 97 R-02 infrastructure; falls back to
  best-effort `shutil.rmtree(ignore_errors=True)` only if the SQLite INSERT
  fails) -> `__init__` reinit triggers migration ladder -> reload searcher).
  New toolbar button in `desktop/my_library_tab.py` with destructive red
  styling, bilingual EN/HE strings, proactive active-scan guard
  (`_update_reset_button_state()` toggles `setEnabled` + tooltip on worker
  lifecycle signals), and a custom `QDialog` two-step typed confirm
  (`RESET` / `אפס` both accepted regardless of `CURRENT_LANG`). Enabled-state
  tooltip explicitly reassures that source files and the Genizah corpus
  are preserved.

**97.1 work bundled in this entry:**
- MAX_PATH long-path prefix (commit `2e1b846e`) — Windows-only handling
  of paths > 260 chars via the `\\\\?\\` prefix
- Non-blocking cancel + per-file cancel check — addressed UI freeze and
  `WinError 3` storm during cancel on large folders

**Reset scope:** LOCAL_INDEX_DIR + LOCAL_LAB_INDEX_DIR only. pgp.db,
fjms_enrichment.db, nli_crossref.db, libraries.csv, Genizah_Index/ are NEVER
touched by Reset. Source files (user's own .txt/.docx/.pdf) are NEVER touched.

**5 RED tests (each landed RED before its fix):**
- `tests/test_phase_97_2_schema_marker_absence.py` (R97.2-F)
- `tests/test_phase_97_2_writer_handle_leak.py` (R97.2-A + R97.2-B)
- `tests/test_phase_97_2_discard_writer_lifecycle.py` (R97.2-C + R97.2-G)
- `tests/test_phase_97_2_sqlite_vs_tantivy_consistency.py` (R97.2-H)
- `tests/test_phase_97_2_reset_my_library_full_cycle.py` (R97.2-E)

**Origin trace:**
`.planning/phases/97.2-recovery-cascade-lockbusy/97.2-CODEX-CRITIQUE.md` +
`97.2-CODEX-BRIEF.md`. CONTEXT D-02 adopted Codex's expanded 5->8 bug list.

**Deferred (out of scope):**
- Centralized `try_open_or_rebuild()` helper (Codex recommended; deferred
  to a future refactor phase per CONTEXT D-01).
- Defensive stale-lockfile cleanup (Codex recommended conservatively;
  deferred per CONTEXT D-06 — Bug A fix removes the actual lock-leak
  vector).

**Verification:**
- `pytest tests -k phase_97_2 -x` — all 5 new tests pass
- `pytest tests/test_phase_97_invariants.py tests/test_scan_run_id.py -x` — no regression
- `pytest tests/test_phase_87_no_raw_storage_access.py -x` — web multitenant invariant unaffected
- `python -m ruff check .` — clean

(desktop — LOCAL is desktop-only per Phase 95 invariant; web LIBRARY_CODES `[]` unaffected)

### Phase 98 — NLI Resilience (internal; 2026-05-25)

Internal closeout — not yet a user-facing release.

**Resilience hardening for NLI/IIIF code paths.** All 10 NLI fetch sites
guarded by a new shared circuit breaker (`shared/nli_circuit_breaker.py`)
that trips after 3 consecutive failures and short-circuits further calls
for 60s. Per-call timeouts dropped from 15-30s to 3-5s via 6 new env knobs
(`NLI_CIRCUIT_THRESHOLD=3`, `NLI_CIRCUIT_WINDOW=60`, `NLI_CONNECT_TIMEOUT=3`,
`NLI_IIIF_READ_TIMEOUT=5`, `NLI_MARC_READ_TIMEOUT=3`,
`NLI_IMAGE_READ_TIMEOUT=5`). `NLI_SEMAPHORE_TIMEOUT` default dropped 20→1.
PostHog telemetry on breaker open/close via factored
`shared/posthog_server.py`.

Worst-case per-request blocking budget: 45s → ~9s. After 3 consecutive
failures the breaker stays open for 60s and subsequent NLI fetches return
empty in microseconds (negative-cache short-circuit). The Nyquist test in
`tests/test_nli_circuit_breaker.py::TestNliCircuitBreakerConcurrency`
proves 20 saturating threads complete in <10s wall time.

Wired into all 10 NLI fetch sites: 4 in `web/api.py`
(`fetch_fl_ids_from_nli`, `nli_image`, `_fetch_nli_image_bytes`,
`proxy_image` — host-conditional for non-NLI), 3 in puzzle
(`PuzzleImageService._fetch_iiif_image`, `_fetch_direct_url`
host-conditional, `web/pages/puzzle.py::_resolve_folios`), 4 in
`genizah_core.py` (`fetch_iiif_manifest`, `fetch_marc_data` migrated +
new wirings at `_fetch_single_worker`, `_fetch_fl_ids`); legacy
class-attribute breaker REMOVED (RESEARCH Pitfall 5).

**Origin:** 2026-05-25 production hang — see
`docs/INCIDENT-2026-05-25-nli-iiif-hang.md` +
`docs/INCIDENT-2026-05-25-CODEX-CRITIQUE.md`. Closes the Minimum Ship
Patch from the Codex critique.

**Deferred (out of scope):** Async refactor to `httpx.AsyncClient`,
event-loop watchdog, multi-worker uvicorn (CONTEXT D-05).

**Production canary verification:**
`curl -w "%{time_total}\n" https://genizahsearch.com/api/fl_ids/990001458630205171`
10× in sequence. Expected: first 1-3 calls slow (1-5s), remaining < 0.1s.
Journal pattern `Failed to fetch FL IDs` should appear at most 3 times
per 60s window per sys_id.

134/134 Phase 98 tests pass across 6 test files
(`test_posthog_server.py`, `test_nli_circuit_breaker.py`,
`test_api_nli_breaker_integration.py`,
`test_puzzle_nli_breaker_integration.py`,
`test_genizah_core_nli_breaker_migration.py`,
`test_nli_breaker_cross_module_invariants.py`).

(both web + desktop — desktop releases next milestone)

---

## [vNEXT] - Phase 97 Wave F Gap Closure - 2026-05-25

### Phase 97 Wave F — My Library Gap Closure (desktop)

- **D-NEW-2 Network drive semantics**: `_check_folder_reachable` with
  errno-discriminated retry — `ETIMEDOUT`/`EAGAIN` → 3× retry with 2s backoff
  → `status='timeout'`; `ENOENT`/`EACCES` → `status='unreachable'`. All
  auto-rescan skip-sets updated to `('unavailable', 'unreachable', 'timeout')`.
- **D-NEW-3 TOCTOU detection**: Pre+post `os.stat` bracket around
  `_index_one_file`. On mtime_ns or size mismatch → `status='changed_during_index'`,
  re-queued for next scan (max 3 retries per scan_run).
- **D-NEW-4 Extension gate**: `_SUPPORTED_EXTENSIONS` check at INSERT site
  prevents new unsupported-format rows from entering the index.
- **D-NEW-5 chunk_locator per format**: PDF pages now carry `'p. N'` locator;
  DOCX chunks carry `'paragraphs N-M'`. Result dialog LOCAL hit header shows
  `folder/file.pdf — p. 3` format.
- **D-NEW-6 Privacy disclosure**: Bilingual EN+HE disclosure for the zstd
  cleartext cache added to Help page and About dialog.
- **D-NEW-7 AST CI guards**: 4 fail-fast tests covering cloud-write gates,
  web `LIBRARY_CODES` allowlist, `is_local_sys_id`, and RRF merge order.

---

## [vNEXT] - My Library Phase 96 Polish - 2026-05-24

### v7.14.x / v7.15.0 — My Library Phase 96 Polish (Phase 96)

Desktop My Library improvements shipped after v7.14.0 UAT rounds.

### Bug Fixes (desktop)

- **UX redesign**: Replaced QSplitter + separate opt-out tree + status
  table with a single unified `_UnifiedFileTreeWidget` (3 columns:
  Filename checkbox / Pages / Status). Per-file opt-out checkboxes and
  indexing status now live in one coherent view.
- **P2 persistence race**: Opt-outs now survive close+reopen — added
  `flush_pending()` call in `closeEvent` before `_save_session()`,
  flushing the 150 ms debounce timer synchronously.
- **P1 Enter focus**: `ResultDialog` nav buttons now have
  `autoDefault=False`; `spin_page` receives focus after a LOCAL result
  loads, so Enter jumps pages instead of firing Prev Result.
- **P1 Browse at page 1**: `_open_local_browse()` now coerces `p_num`
  to `int` (was stored as string from header parsing), so a hit on page
  7 opens Browse at page 7.
- **P2 LOCAL widget leak**: LOCAL Browse nav widgets are now hidden
  when loading a non-LOCAL (Genizah) manuscript in Browse.
- **BLOCKER-5**: Converted 10 stale `pytest.skip("Phase 96 … not yet
  implemented")` markers to positive assertions across 4 test files
  (`test_local_filter_cascade`, `test_local_hit_highlighting`,
  `test_local_nav_page_chunk`, `test_local_optout_persistence`).

### Previously deferred items now closed

D-F1 folder drill-down with file checkboxes ✅ (plan 96-04/05/06) ·
D-F4 PDF one-word-per-line extraction ✅ (plan 96-02) ·
D-F5 LOCAL search-term highlighting ✅ (plan 96-03)

D-F2 PDF OCR · D-F3 side-by-side PDF page rendering remain open
for v7.15+. (desktop)

---

## [7.14.0] - My Library: Local Document Search - 2026-05-24

### v7.14.0 — My Library (Phase 95)

Desktop adds **My Library** — a 7th tab that indexes your own
`.docx` / `.pdf` / `.txt` folders into a separate Tantivy
side-index merged inline into Search, Composition Search, and
Parallels with a blue `LOCAL` badge. Personal corpora never leave
the device: three regression tests pin the cloud-write boundaries
(API search, lists sync, corrections submit — gates at the TOP of
each function per Codex D-30 P0).

### New Features (desktop)

- **My Library tab** with multi-folder management, per-file status,
  mid-scan cancel, and a 5,000-file / 2 GB pre-scan ceiling
- **Pre-search corpus dropdown** (`Genizah` / `Local` / `ALL`,
  default `Genizah`) next to the Search button
- **Post-search 3-state LOCAL filter** on Search, Composition, and
  Parallels surfaces
- **Double-click LOCAL hit** → `ResultDialog` with text + blue
  **Open file** button (`os.startfile`); Browse tab also supports
  LOCAL text via "View in Browse"
- LOCAL `Library` column shows `parent/folder`; `Shelfmark` column
  shows the filename

### Improvements

- RRF k=60 merge *after* `_deduplicate()` (Codex D-08 P0) — LOCAL is
  never silently dropped
- Per-thread SQLite via `threading.local()` (no
  `check_same_thread=False`)
- Tantivy writer retry with exponential backoff on Windows `os error 5`
- D-37 corrupt-index fallback (search continues Genizah-only on LOCAL
  index open failure)
- 28 new Hebrew translations for MyLibraryTab UI strings

### Documentation & Packaging

- Help (web + desktop) + About (both) gain bilingual My Library
  section with D-33 cleartext-on-disk disclosure
- About credits Yehuda Seewald's `GenizahLocal` prototype in the
  contributors paragraph (EN + HE; HE spelling: יהודה זייבלד)
- PyMuPDF (`fitz`) added as desktop dep (~25 MB installer growth);
  `collect_all('pymupdf')` in `.spec` + `--self-test-pymupdf` smoke
- `shared/export_dossier.py` `skip_local` kwarg: desktop xlsx
  includes LOCAL, web xlsx excludes
- Static AST guard `tests/test_web_library_options_no_local.py`
  pins web LIBRARY_CODES invariant

### Deferred to v7.15+ (`docs/OPEN_ISSUES.md`)

D-F1 folder drill-down with file checkboxes · D-F2 PDF OCR ·
D-F3 side-by-side PDF page rendering · D-F4 PDF extraction quality
audit (one-word-per-line manifestation) · D-F5 LOCAL search-term
highlighting

Inspired by Yehuda Seewald's external GenizahLocal prototype. (both)

---

## [7.13.0] - 2026-05-21

Bundles v7.13 milestone work (Research-Grade Downloads & PGP Filter), the
homepage About + FAQ additions, and a desktop sync-merge bug fix.

### Added — Phase 94: Research-Grade Export Metadata (web + desktop)

- **`shared/export_dossier.py` module** (94-01) — single source of truth for
  citation-grade xlsx export data shaping. 4 lookup helpers
  (`pgp_subset_for_sys_id` / `nli_subset_for_sys_id` /
  `catalog_summary_for_sys_id` / `bibliography_for_sys_id`), 2 row emitters
  (`build_manuscript_row` / `build_bibliography_rows`), 2 header constants
  (`MANUSCRIPT_HEADERS` 14-col / `BIBLIOGRAPHY_HEADERS` 8-col), and the
  `build_rich_snippet_cell` helper extracted from desktop's prior inner closure
  for the `*...*` red-bold snippet rendering. Codex MUST-FIX 1-4 folded in
  (real FJMS field names, `NLI Catalog Entry` naming, narrow `get_catalog_records`
  query — never the `get_catalog_detail` variant which loads `full_texts`,
  bibliography off the `build_manuscript_row` path). (both)
- **Bilingual headers + source-language metadata** (94-04 smoke round-1,
  D-04 REVERSED 2026-05-20). 4 new bilingual helpers in `shared/export_dossier.py`
  (`main_header_row(lang)` / `manuscript_header_row(lang)` /
  `bibliography_header_row(lang)` / `sheet_titles(lang)`). All 3 dossier
  helpers thread `lang` and prefer Hebrew when `lang='he'` (with English
  fallback per field); English when `lang='en'` (with Hebrew fallback).
  `pgp_translations` table consulted via
  `TranslationService.get_pgp_translations_by_sys_ids` only on `lang='he'`.
  (both)
- **4-sheet workbook** (94-03 web + 94-04 desktop + smoke round-2). Web and
  desktop xlsx exports now produce 4 sheets:
  1. **Search Results** (renamed from `Genizah Results` in smoke round-2;
     bilingual via `sheet_titles(lang)`) — 12-col main sheet with per-row
     metadata: System ID / Library / Shelfmark / Title / Image/Page / Source
     / Snippet / Full Text / Has PGP / Is Printed / Domains / Image URL.
     Snippet column emits rich-text `*...*` matches in red+bold. Image URL
     (originally locked as the empty `IIIF Manifest` column per D-13) was
     renamed and populated in Phase 94.1 (2026-05-21) with per-folio
     `https://genizahsearch.com/api/.../?page=N` proxy URLs — clickable
     openpyxl hyperlinks; synthetic sys_ids emit empty cell.
  2. **Manuscripts** — one row per unique sys_id (first-occurrence dedupe).
     14 columns including PGP URL / Library Viewer URL / GenizahSearch URL
     (all clickable hyperlinks with blue-underline styling after smoke round-4).
  3. **Bibliography** — one row per FJMS bib entry, joinable to Manuscripts
     by System ID. 8 columns.
  4. **Credits and Info** (NEW in smoke round-2) — search metadata
     (Query / Mode / Gap / generated_at / result count) + clickable
     GenizahSearch.com hyperlink + Creator credit. Parity across web and
     desktop.
  Conditional RTL view-direction on all 4 sheets per UI language. (both)
- **Web JSON additive per-item flags** (94-02). The `/api/search` JSON
  envelope's per-item dict gains 3 keys: `has_pgp` (bool), `is_printed` (bool),
  `domains` (list). Opt-in semantics (MUST-FIX 94-02-B): keys are omitted
  on the public `/api/search` path (preserves D-11 stable response shape)
  and emitted on the JSON export path. Envelope `schema_version` stays `1`
  (Phase 83 additive-change commitment). Parallels JSON envelope unchanged
  (D-10 negative invariant pinned by `tests/test_parallels_envelope_no_pgp_keys.py`).
  (web)
- **Web state plumbing** (94-02). `web/export_state.set_search_export(...)`
  extended with 3 enrichment kwargs (`transcription_sys_ids`, `printed_ids`,
  `result_domains`); new sibling helper `update_search_export_enrichment(...)`
  with independent-field patch semantics. 5 call sites in `web/pages/search.py`.
  History-restore branch flags restored snapshots via the
  `metadata_incomplete_restored_from_history` warning marker. (web)
- **Desktop xlsx parity** (94-04, EXPORT-META-09). New module-level pure-function
  helper `_build_search_results_xlsx_bytes(...)` at `genizah_app.py:2473`
  (Qt-free, fully unit-testable offline) consumed by the rewritten xlsx branch
  of `export_results('xlsx')`. Cross-app parity invariant pinned by
  `tests/test_export_xlsx_cross_parity.py` (identical sheet names + headers
  across web and desktop on identical input at `lang='en'`). Desktop CSV / TXT
  / DOCX branches at `genizah_app.py:18294+` are byte-identical to pre-Phase-94
  (xlsx-only scope). (desktop)
- **Web Credits and Info sheet metadata** (smoke round-3): web `web/api.py`
  Credits-and-Info labels route through `tr()` for the same Hebrew/English UX
  as desktop. Web search metadata cells (Query / Mode / Gap / generated_at /
  result count) match desktop's Credits-and-Info structure exactly. (web)

### Added — Phase 93: PGP Filter on `/search` (web only, shipped 2026-05-19)

- Post-search 3-state filter button in the search results toolbar (`Filter PGP`
  default → `Has PGP` only-PGP → `No PGP` hide-PGP → back to `Filter PGP`).
  Same `outline dense no-caps` styling as the existing `printed_filter` button.
  Hidden until the current result set contains at least one PGP-tagged hit.
  Stacks AFTER `printed_filter` in the render cascade (verified by
  `tests/test_pgp_filter_cascade.py` static AST guard). Choice persisted via
  `persist_value('search_pgp_filter', ...)` routed through the
  `web/safe_storage.py` chokepoint (Phase 87 invariant preserved). Hebrew
  translations: `סנן PGP` / `PGP בלבד` / `ללא PGP` / `סנן לפי כיסוי PGP`.
  PGP-FILTER-03 (active-filter chip) was originally planned but superseded
  by user smoke direction 2026-05-19 — the colored button label already
  conveys filter state, so the chip was redundant. (web)

### Added — Homepage About + FAQ (web)

- **Bilingual About + FAQ sections on the homepage** — new to the site
  (added during SEO work; not previously published). Links to MiDRASH,
  FGP, PGP, NLI, CUDL, the Responsa Project, and Manchester. The visible
  FAQ accordion also provides FAQPage JSON-LD.
- **Manuscript JSON-LD** on manuscript pages.
- **Title/meta length cleanup, image alt text, and llms.txt.**
- **Favicon shrunk 401 KB → 9 KB** (44× reduction).

### Added — Help page (web)

- Help page documents the new 4-sheet xlsx export and the JSON export
  format, and adds a new **Public API & AI Tools** section linking to
  the Search API endpoints and the `cairo-genizah-research` Claude skill.

### Fixed

- **Desktop list-merge no longer duplicates items** — when the app
  prompts to merge differences between the web list and the desktop
  list, items previously appeared twice in the desktop list while
  staying single in the web list. Pre-existing duplicates from past
  syncs now clean up automatically on the next merge. (desktop)
- **"Important" translation-disclaimer callout is readable in dark
  mode** — light text on a light fallback background. Now themed
  properly. (web)

### Internal

- **Cross-app parity test** (`tests/test_export_xlsx_cross_parity.py`):
  pins identical sheet names + identical 12-col main / 14-col Manuscripts
  / 8-col Bibliography headers across web and desktop on identical input at
  `lang='en'`. Survives the 2026-05-20 D-04 reversal (which made headers
  + sheet titles lang-dependent — identical when both apps pick the same lang).
- **6 rounds of smoke-verification regression tests** added across
  `tests/test_export_bilingual.py` / `test_credits_sheet.py` /
  `test_smoke_round2_gaps_a_d.py` / `test_smoke_round3_label_realignment.py`
  / `test_manuscripts_urls_clickable.py` / `test_domains_dedupe.py` /
  `test_image_page_int_coercion.py`. Each round shipped as an atomic commit
  pair (production fix + matching regression test).
- Final test count after Phase 94 closure: **2316 passed / 20 skipped /
  2 xfailed** across the full test suite. Ruff clean across all touched
  production + test files.
- **Phase 87 multitenant invariant unaffected:** zero raw `app.storage.user.*`
  accesses introduced under `web/` across all 4 Phase 94 plans + 6 smoke
  rounds. Allowlist remains `[]`. `tests/test_no_raw_storage_access.py`
  green throughout.

---

## [7.12.0] - Multitenant Safety and Line Numbering - 2026-05-18

Bundles the v7.12 Path B multitenant architecture refactor with two
user-visible features (line numbering, web folio chip).

The architectural work is invisible to individual users but
load-bearing for concurrent use: the web app can no longer leak one
user's data (search results, exports, lists, auth state, etc.) into
another user's browser session. The cross-user xlsx export filename
leak fixed in v7.11.1 was one instance of this bug class; v7.12
makes the whole class structurally impossible.

### New Features

- **Line numbering on transcription text** — a right-side (RTL
  leading-edge) line-number gutter appears next to every transcription,
  on both web and desktop. Numbers correspond to source-text lines
  (matching the existing `L<N>:word` Responsa search syntax). Lines
  restart at 1 for each folio/page. Copy-paste from the body never
  picks up the numbers — the gutter is structurally separate from the
  text. Toggle via the `format_list_numbered` button in the
  transcription header (web) or the `# Lines` button in the find row
  (desktop); default ON; persisted per user. Surfaces covered: web
  Browse single-page view, web Quick View dialog, web Full Manuscript
  View, desktop Browse tab, desktop ResultDialog. (both)
- **Folio chip on web search result cards** — each web search result
  card now shows the page/image number inline after the shelfmark (the
  same field desktop's `COL_IMG` shows). You no longer need to open
  Quick View to know which folio a hit came from. Theme-aware chip;
  Hebrew/English tooltip "Image number" / "מספר תמונה". (web)

### Improvements

- **NLI image cache resilience** — the persistent image-FL-ID cache
  no longer fails with `[WinError 5]` on Windows when two requests
  finish writing simultaneously. Added a process-wide lock and
  retry-with-backoff on `os.replace`. (web)

### Bug Fixes

- **Memory leak in search export payload** — long-running web server
  processes were accumulating RSS at ~300 MB/hour because
  `export_search_payload` retained the full unbounded result list
  per user. Now capped at 5,000 results with `truncated` metadata so
  downstream UX can advise "showing first 5K of N" if needed. RSS
  dropped from 7.5 GB → 1.78 GB after deploy. (web)
- **4 P2 UI/UX bugs surfaced during line-numbering smoke checks** —
  chip overflow on long shelfmarks, Distance field overflow on small
  viewports, redundant Distance fields appearing twice, bright-mode
  ResultDialog white-on-white text, bright-mode dark toolbars. (both)

### Internal — Multitenant Architecture (Path B)

A web-only architectural refactor across 6 core phases (87-92) plus
two inserted sub-phases (92.1, 92.2). 49/49 requirements satisfied
across 28 plans. Zero net user-visible behavior change beyond the
two features listed under New Features above, plus the cross-user
leak class closure.

- **`web/safe_storage.py` is now the chokepoint for all per-user
  state.** 131 raw `app.storage.user` accesses across 14 files
  migrated through it. The allowlist of permitted raw accesses is
  empty (`allowed_raw_access: []`); a permanent CI guard
  (`tests/test_no_raw_storage_access.py`) rejects any new raw
  access at lint time.
- **State separation by deletion, not migration.** 10 per-user mirror
  fields on `AppState` physically deleted; `web/export_state.py` is
  the only path for per-user export state. The `_TEST_BACKEND` shim
  is gone (tests use `SimpleNamespace` fixture injection through
  `web.safe_storage.app` monkeypatching).
- **Lists cache rewritten per-request.** `UserListsManager` singleton
  attribute + 10-second TTL plumbing all deleted; per-request
  instantiation. Cross-user list cache leak structurally impossible.
- **Auth caching rewritten without process-wide cache.** Request-scoped
  auth via local header mutation. NO `auth.set_session()` mid-flight
  (Codex finding: `gotrue_client.py:713` `set_session()` is networked,
  not local). Refresh-only locking keyed by stable `_session_uuid`
  (token-keyed locks rotate when tokens rotate).
- **Server-side `sign_out` revocation.** `throwaway.auth.admin.sign_out(jwt, "global")`
  actually revokes the user's token at Supabase before local keys are
  popped. The anonymous singleton could not have done this.
- **Atomic auth state writes.** `set_auth` returns `bool` with symmetric
  2-key rollback on partial-write failure; `do_login` and OAuth callback
  apply defensive 3-key caller-level cleanup. `set_auth(profile=None)`
  clears stale `auth_profile` (closes a role-confusion security leak
  caught by Codex).
- **Reader-client RLS retrofit.** 12 reader functions in
  `web/supabase_client.py` migrated from anonymous singleton to
  authenticated `get_user_client()` so `TO authenticated` SELECT
  policies return rows for logged-in users.
- **Architecture reference:** `docs/guides/MULTITENANT.md` (~2150
  words, 8 sections) is the canonical doc for future contributors
  touching per-user state.

---

## [7.11.2] - Composition Search Bug Fixes - 2026-05-15

Desktop-only patch addressing two user-reported bugs in composition
search (find Genizah manuscripts that match a long source text).
Reported by a power user after the v7.11.1 desktop release. No new
features; pure correctness fixes.

### Bug Fixes

- **`Min chunks ≥ N` filter inflated when source repeats a phrase** — if
  the source text contained the same phrase multiple times (e.g.
  "ברוך אתה יי" recurring through benedictions and prayers, or two
  versions of the same text pasted together), the system counted each
  repetition as a separate chunk match. A manuscript that actually
  matched the phrase only once could pass `Min chunks = 2`. Internal
  Tantivy segment duplication on the same manuscript inflated the count
  too. The chunk counter is now derived post-hoc from unique chunk
  *contents*, not raw Tantivy hits, so the filter does what users
  expect. Affects both the standard composition search and the Lab
  composition search; the Lab path's full-mode filter was also latently
  broken (always rejected everything when min ≥ 1 — the item dict never
  surfaced the counter it was filtering on). (desktop; shared code
  also benefits web)
- **Expanded result view didn't scroll to the highlighted match** —
  opening a composition result by double-click loaded the source and
  manuscript text panes from the top, with the highlight somewhere far
  below. For 70-page source texts (common with prayer-book collections
  or Responsa volumes) the view was effectively unusable. Both panes
  now scroll to the first match automatically, and the manuscript pane
  re-anchors when navigating between pages. (desktop)

### Internal

- **v7.12 Path B foundations bundled** — internal multitenant-state
  refactor (Phases 87, 88, 89: session UUID + safe_storage chokepoint,
  AppState export-field deletion, per-request UserListsManager). Zero
  user-visible change. Web deploy still gated behind the rest of the
  Path B milestone (Phases 90-92); this desktop release is independent.

---

## [7.11.1] - Desktop Catch-up Release - 2026-05-13

A small release that brings the desktop installer up to par with what
shipped to web. The web app has been running v7.11.0 (CUDL Coverage)
plus six post-release hotfixes since 2026-05-12; this release packages
all of that into a desktop installer.

The 5 commits past `242664d3` on master-main (cross-user lists cache,
safe-storage wrapper, auth resurrection guard, persist_value, more
safety reads) are intentionally NOT in this release — they are partial
fixes superseded by the v7.12 Multitenant Architecture (Path B)
milestone now in flight.

### New on Desktop (from v7.11.0, never previously distributed to desktop users)

- **108 CUDL-only manuscripts** — Cambridge CUDL classmarks that have no
  NLI Alma record at all (including the originating user-reported case
  `T-S NS 329.96`, plus ~100 Mosseri and CUL entries) now appear in
  search results, the catalog browser, and the metadata viewer. Images
  load from CUDL canvases; catalog, source, and bibliography panels
  populate from FJMS data. No transcription text — these are
  image+metadata-only records.
- **Cambridge shelfmark normalization bridge** — alternative shelfmark
  forms (`Moss. III,27O` ↔ `mosseriiii27o`, Cambridge `Or.` numeric
  variants, leading-zero collisions, slash/comma/dot handling) now
  resolve to the same record. Shelfmark search recovers thousands of
  CUDL classmarks that previously appeared "missing" because of form
  differences.
- **"View on CUDL" link fixed** — works for previously-orphan Mosseri
  and CUL shelfmarks that used to fall through to a slug-fallback that
  404'd.
- **Browse pagination for synthetic manuscripts** — Next/Prev navigation
  on CUDL-only manuscripts (which have image pages but no transcription)
  now works correctly.

### Bug Fixes

- **Desktop comments save** — 4 of 5 comment-type dropdown values
  (`Question`, `Scholarly Note`, `Suggestion`, `Issue`) silently failed
  to save with a `comments_scope_check` database error; only `General`
  worked. Fixed by deriving `scope` from `page_number` presence
  (matching the web client's behavior). (desktop)
- **`/help` 500 error** — chained `set_visibility` calls on `ui.card()`
  were breaking the `with` statement because `set_visibility` returns
  `None`. (web)
- **Cross-user export filename leak** — User A's saved search query
  name appeared as the suggested `.xlsx` filename in User B's export
  dialog. Search/parallels export endpoints were reading from a
  process-wide singleton; routed all 5 `/api/export/*` handlers through
  per-session app storage. (web)
- **`/browse` 500 on pruned-session race** — stopping a search and then
  opening a result on `/browse` raised an `AssertionError` when the
  session_id had been evicted from NiceGUI's `_users` cache by the 10s
  prune scheduler. Snapshot read/write helpers now degrade to defaults
  instead of raising. (web)
- **Browse Phase B enrichment silently failing on production** —
  expanded manuscript panel was missing PGP description, FJMS catalog
  records, bibliography, and measurements. A JS dispatch during the
  detached `load_page` task was bubbling up a slot-lifecycle exception
  that the enrichment gate was treating as an error. Wrapped the
  dispatcher and extended the slot-race exception filter. (web)
- **Lists "Sync Now" UX clarification** — renamed misleadingly-named
  button to "Move to account" (it only migrated browser-localStorage
  anonymous lists into the cloud account, NOT cloud→view), added a new
  "Refresh from Cloud" button on the lists header for logged-in users,
  reworded surrounding copy as a one-way browser→account move. (web)

---

## [7.11.0] - CUDL Coverage & Synthetic Inventories - 2026-05-12

A 3-phase milestone (Phases 84, 85, 86) closing the gap between CUDL's ~141K
classmark catalogue and GenizahSearch's libraries.csv. The originating
user-reported case was `T-S NS 329.96` — present in CUDL with 2 image canvases,
missing from libraries.csv because its FJMS inventory has no NLI Alma record at
all. The milestone splits into normalization (recovering thousands of CUDL
classmarks already represented in libraries.csv under different forms) and
synthetic rows (a new 18-digit `99…000000` sys_id format for the small residue
of truly-orphan FJMS inventories with no NLI counterpart).

**Milestone outcome:**

- **Phase 84** (CUDL Shelfmark Normalization) — `shared/fist_cudl_bridge.py`
  bridge module with normalizers for Mosseri label form (`Moss. III,27O` ↔
  `mosseriiii27o`), Cambridge Or. numeric collapse, leading-zero collision
  audit, slash/comma/dot bug fixes. Wires into `get_image_sources`, browse
  enrichment, shelfmark search fallback, and CUDL link builder.

- **Phase 85** (Synthetic Infrastructure) — helper module
  `shared/synthetic_sys_id.py`, generation script, FJMS sidecar UNION-ALL
  pattern in export, browse hide-NLI gates, `is_synthetic` field on /api
  responses + PostHog events, corrections-write reject. The initial Phase 85
  POPULATION (5,035 bibliography-only rows) was reverted by `3c75a9bc` during
  UAT — the infrastructure stays active but produces zero rows until Phase 86
  re-attempts with image-bearing criteria.

- **Phase 86** (CUDL Coverage Audit + Synthetic Re-attempt) — **108 image-bearing
  synthetic manuscripts** including the originating case `T-S NS 329.96`. The
  CUDL-walked qualifier requires (a) a CUDL manifest in `cambridge_manifests`,
  (b) no real-Alma children, (c) unambiguous bridge resolution. AUDIT-02 5-tier
  coverage report shows 96.23% phase84_hit + 2.39% phase86_existing_alma_candidate
  + 0.08% phase86_synthetic + 1.13% phase86_residue. The 1,599-row residue
  (29.99% of original 5,325 truly-orphan CUDL classmarks) is documented in
  `.planning/phases/86-cudl-coverage-audit-and-synthetic-reattempt/86-RESIDUE-PATTERNS.md`
  with 6 pattern families analyzed and REJECTED — they need human-in-loop
  adjudication, not new auto-rules.

**Phase 85 background (kept for context):** The original Phase 85 attempt
shipped INFRASTRUCTURE but reverted POPULATION during 2026-05-09 UAT. Plan
02's "inclusive coverage stance" qualified InventoryIds on ANY FJMS signal,
but the resulting 5,035 rows had only bibliography pointers (no text, no
image, no catalog description) — not actionable for research without the
underlying manuscript. Additionally, 175 of those rows shadowed real-Alma
series-children (e.g. synthetic `T-S NS 161` shadowed 1,009 real
`T-S NS 161.x` rows). User decision: revert the data, keep the
infrastructure dormant, re-attempt in Phase 86 with image-bearing-only
criteria. **Phase 86 delivered on that decision.**

### Infrastructure landed (active in code, no production data)

- `shared/synthetic_sys_id.py` helper module (Phase 85 SYNTH-01): pure functions
  `is_synthetic_sys_id`, `encode_inventory_sys_id`, `decode_inventory_id`. Single
  source of truth — repo-grep lint enforces no hand-rolled string slicing.
- `scripts/generate_synthetic_rows.py` (Phase 85 SYNTH-02): regeneration script
  with marker-fenced libraries.csv block, authoritative manifest, idempotent
  `--dry-run` / `--apply` modes. Currently produces an empty manifest because
  `libraries.csv` synthetic block was emptied. Phase 86 will tighten qualification.
- `scripts/export_fist_enrichment.py` UNION-ALL pattern (Phase 85 SYNTH-05):
  synthetic-AlmaId rows merged with real-Alma rows in 12 enrichment tables at
  export time. Currently a no-op because manifest is empty.
- Browse page hides KTIV link, NLI source toggle, NLI bibliography chips, and NLI
  image source when `is_synthetic_sys_id(sys_id)` is true (Phase 85 SYNTH-04).
  CUDL becomes the default image source when a Cambridge IIIF manifest is
  available. Web + desktop parity. No-op in production until Phase 86 re-attempts.
- `is_synthetic: bool` field on `/api/search`, `/api/browse`, and `/api/parallels`
  response items (top-level, NOT nested under `locator`). Additive change —
  `schema_version` stays `1` per the Phase 83 stability commitment. Always `false`
  in production until Phase 86 re-attempts.
- `is_synthetic` PostHog event property on `/api/search` and `/api/browse` events
  (intentionally OMITTED from `/api/parallels` events — parallels takes `text`,
  not `sys_id`, so there is no canonical seed sys_id to tag).
- Corrections-write rejects synthetic sys_ids at the client-side write entry points
  (`CorrectionsClient.create_correction` and
  `SupabaseCorrectionsClient.create_correction`) with the message
  `synthetic_corrections_disabled: ...`. Web and desktop "Edit" buttons are also
  hidden as defense-in-depth. (REVIEWS-MODE iteration 1 B1+B2: there is no
  `POST /api/corrections` HTTP route in this codebase — gating happens at the
  client-class level.) No-op in production until Phase 86 re-attempts.

### Deferred to Phase 86 (CUDL Coverage Audit + Synthetic Re-attempt)

- **Synthetic re-attempt with image-bearing-only criteria.** Plan 02's "any FJMS
  signal" qualification produced 5,035 bibliography-pointer-only rows that didn't
  help researchers. Phase 86 will tighten `_build_qualifying_inventories` to
  require: (a) a CUDL manifest in `nli_crossref.db.cambridge_manifests`, (b) no
  real-Alma children of the synthetic's leaf shelfmark in libraries.csv (filter
  from `reports/synthetic_parent_shelfmarks.csv`), and optionally (c) relax D-05a
  STRICT for unambiguous multi_signature cases (so the originating user case
  T-S NS 329.96 — 12 SignatureIds for one shelfmark — can be synthesized when
  all 12 resolve to the same canonical_shelfmark + library_code). Expected
  output: ~100-500 image-bearing synthetic rows (instead of 5,035 bib-only).
- **Originating user case (T-S NS 329.96) and ~10,689 multi_signature peers.**
  Logged to `reports/synthetic_ambiguity_residue.csv`. Phase 86 will triage and
  decide which to admit under the relaxed D-05a.
- **175 parent-shelfmark false synthetics shadowing 10,949 real-Alma children.**
  Logged to `reports/synthetic_parent_shelfmarks.csv` (e.g. `T-S NS 161` had
  1,009 real-Alma `161.x` children but was synthesized as a series-container).
  Phase 86's "no real-Alma children" filter handles this.
- **Corrections-write on synthetic rows.** `page_number` semantics undefined for
  image-less synthetic inventories. Client-side write entries reject with
  explicit error code; UI buttons hidden. A future plan will define proper
  `page_number` semantics if image-bearing synthetics admit corrections.
- **AUDIT-01, AUDIT-02, AUDIT-03** — Phase 86 re-runs `scripts/scan_cudl_orphans.py`
  and produces `reports/cudl_coverage.md` confirming closure of the CUDL coverage
  gap from Phase 84 normalization. Phase 85 carries forward the residue artifacts
  (`reports/synthetic_ambiguity_residue.csv`,
  `reports/synthetic_parent_shelfmarks.csv`, `fist_data/synthetic_manifest.json`)
  for Phase 86 to consume.

### Phase 84 — CUDL Shelfmark Normalization (active)

- **`shared/fist_cudl_bridge.py`** — bridge module with FIST↔CUDL normalizers.
  `lookup_fist_by_cudl`, `explain_fist_by_cudl`, `build_fist_alias_index`,
  Mosseri label normalizer (`Moss. III,27O` ↔ `mosseriiii27o`), Cambridge Or.
  numeric collapse, ambiguity-policy guard against leading-zero collisions.
- **`shared/shelfmark_bridge.py`** — `build_alias_index()` + `lookup_cudl()`
  wired into `genizah_core.resolve_system_by_shelfmark` as fallback for shelfmark
  search misses, and into `get_image_sources()` so CUDL manifest URLs surface in
  browse for previously-orphan shelfmarks.
- **6 bridge wiring call sites** across `genizah_core.py`, `web/services.py`,
  `web/pages/browse_enrichment.py:208`, image-source resolution, CUDL link
  builder, and orphan-scanner unification. Web browse `View on CUDL` button
  resolves correctly for Mosseri + CUL-CUDL shelfmarks that previously fell
  through to lossy slug-fallback 404s.
- **3-layer regression guard:** `cudl_must_resolve` fixture (positive tests),
  `cudl_baseline_resolved` snapshot (regression detection), unit tests on
  ambiguity policy. `scripts/build_cudl_fixture.py` regenerates from real data.

### Phase 86 — CUDL Coverage Audit + Synthetic Re-attempt (active)

- **108 image-bearing synthetic manuscripts** in `libraries.csv`, including
  the originating user case `T-S NS 329.96` (sys_id `990065549106000000`). All
  108 have CUDL canvas images accessible via the FIST↔CUDL bridge. Distribution:
  101 CUL + 7 Mosseri. The synthetic block is marker-fenced in libraries.csv
  for idempotent regeneration.
- **Surgical DB injection** (`scripts/phase86_inject_synthetic_to_main_db.py`):
  3,264 catalog rows + 103 FTS5 docs across 11 base tables in
  `fjms_enrichment.db`, leaving the 7 supplemental tables (translations,
  measurements, blank_images, extra_info, computed_measurements, import_meta,
  fjms_translations) untouched. `catalog_sizes` skipped per Codex review
  (schema drift — main has v7.3.0 measurements columns, worktree had v6.5 form).
  Backup via `.backup()` API + full gzip CRC kept at
  `_tmp/phase86_backups/fjms_enrichment.db.pre-inject.*.bak.gz`.
- **Browse pagination for synthetic sys_ids** — synthetic rows have CUDL
  canvases but no Tantivy transcription pages, so `total_pages=0` from the core
  search engine. Fix derives `total_pages` from `cambridge_images` (web) and
  enriched image lists (desktop) so Next/Prev work and the page combo populates.
  Bypasses Tantivy for synthetic via `is_synthetic_sys_id` short-circuit,
  preserves requested `p_num` through metadata-only fallback, tolerates NiceGUI
  slot-lifecycle race during Phase A re-render.
- **AUDIT-02 5-tier coverage report** (`reports/cudl_coverage.md`):
  phase84_hit 96.23%, phase86_existing_alma_candidate 2.39%,
  phase86_synthetic 0.08%, phase86_residue 1.13%, multi_inventory_ambiguous 0.18%.
- **Residue patterns adjudication** — 6 pattern families analyzed in
  `.planning/phases/86-cudl-coverage-audit-and-synthetic-reattempt/86-RESIDUE-PATTERNS.md`.
  All marked REJECTED with carry-forward note: the residue (1,599 rows) reflects
  EXISTING-rule over-aggressiveness, not missing rules. Human-in-loop
  adjudication required, not new auto-rules.
- **NLI attribution regression guard** — `scripts/audit_nli_attribution.py` +
  `tests/fixtures/v7_9_4_nli_flipped_sys_ids.txt` (461-row golden fixture from
  `git show 29fd3044`) confirm Phase 86 work did not regress the v7.9.4
  Oxford→NLI library_code flip.

### Browse Bug Fixes (Web + Desktop)

- **Synthetic-row pagination** — `_get_metadata_only_browse_page` accepts
  `p_num` / `absolute_index` / `next_prev` and produces a moving target page
  clamped to ≥ 1. `browse_render_page` derives `total` from the largest of
  `_browse_folio_images`, `images_ext`, or `images_nli` when `metadata_only`
  and `total_pages==0`. Combo, page-count label, and viewer.set_page all sync
  to the image-driven page count.

### Other Fixes

- **fix(search):** remove duplicate top-toolbar "Exclude manuscripts" button
  (the filter panel already has one — the results bar entry was a stray
  carry-over from an earlier UX iteration). Reduces visual noise.
- **fix(search):** reset Text Position dropdown (Anywhere / Start / End /
  Line starts / Line ends) on New Search + show active-state chip when not
  set to default. Previously persisted across sessions and caused stealth
  "why aren't my results showing up" confusion.
- **fix(ci):** remove unused `pytest` imports flagged by ruff F401 across
  Phase 86 test files.

### Database Refresh

- **`fjms_enrichment.db`** rebuilt with 3,264 synthetic rows + 103 FTS5 docs
  added via surgical INSERT-only migration. All 7 supplemental tables
  preserved. Backup retained on server as
  `fjms_enrichment.db.pre-phase86-20260512`.

### Migration Notes

- **Deploy posture established:** scp DBs FIRST, then push code. The
  2026-05-11 incident (deployed code without DB sync → catalog/PGP/bib data
  loss → reverted to `6ce42522`) is now codified as the standard runbook.
- **Web auto-deployed** 2026-05-12 via `deploy.sh` after scp of
  `fjms_enrichment.db` + atomic systemd swap. Old DB preserved on server.
- **Desktop installer** rebuilt and bundled with updated `libraries.csv`
  (108 synthetic rows) + `fjms_enrichment.db` (3,264 synthetic rows). No
  user-data migration required.

---

## [7.10.0] - Search API Public Release - 2026-05-05

The v7.10 milestone (Phases 77–83 — 8 phase entries: 77, 78, 79, 80, 81A, 81B, 82, 83 — spanning serializer foundation, search/browse/parallels endpoints, hardening, contract expansion, reference Claude skill, internal docs, and public release) ships a public HTTP/JSON search-helper API over the existing Genizah corpus. Three endpoints — `POST /api/search`, `GET /api/browse`, `POST /api/parallels` — together let a research consumer execute keyword/Responsa search, drill down to a single manuscript page with PGP/FJMS/NLI enrichment, and run composition-parallels detection over arbitrary input text. The API is documented, OpenAPI-spec'd at `/api/openapi.json` with interactive Swagger UI at `/api/docs`, and publicly accessible. A reference Claude skill (`cairo-genizah-research`) demonstrates the full workflow.

### New Features

- **`POST /api/search`** — Keyword, variant, Responsa, Title, and Shelfmark search modes. Returns ranked manuscript results with locators for drill-down. Phase 78 / 81A.
- **`GET /api/browse`** — Stateless manuscript drill-down from a search locator. Returns PGP transcription (when available), FJMS/NLI metadata, and image URLs. Phase 79.
- **`POST /api/parallels`** — Composition-parallels detection using sliding-window chunk matching. Returns ranked parallel-witness groups with per-chunk match evidence. Phase 80.
- **OpenAPI spec at `/api/openapi.json`** — Auto-generated from Pydantic models with explicit `openapi_extra` metadata; scoped to the 3 search-helper endpoints (legacy `/api/*` proxies excluded). Phase 83.
- **Swagger UI at `/api/docs`** — Interactive endpoint explorer with try-it-now and full request/response schema documentation. Phase 83.
- **JSON export toolbar buttons** — `/search` and `/parallels` pages have a download button for the current result set in the Claude-friendly JSON format. Phase 77.

### Security & Hardening (web)

- Per-IP rate limiting (30 req/min default, independent bucket per endpoint, configurable via `SEARCH_API_RATE_LIMIT`). Phase 78.
- `SEARCH_API_MODE` env-var gate (`open` / `localhost-only` / `disabled`), flippable without restart. Phase 78.
- Uniform error envelope `{"error": {"code": ..., "message": ...}}` across all three endpoints. Phase 78.
- XFF spoofing protection via trusted-proxy allowlist and rightmost-non-trusted resolution. Phase 78.
- Fail-closed filter validation — unknown filter values rejected at 400, not silently dropped. Phase 78.
- Responsa expansion cap (`MAX_EXPANDED_TERMS=500`) guards against adversarial query expansion. Phase 78.
- HMAC-hashed IP telemetry in PostHog with `POSTHOG_IP_SALT` persistence. Phase 78.
- Phase 83 security audit (`83-SECURITY.md`): all Phase 78–81B mitigations re-verified load-bearing pre-deploy; Post-Deploy Verification checklist (7 items) re-run against production.

### Documentation (web)

- `docs/SEARCH_API.md` — Reframed from internal-only to a public API reference: request/response shapes, error codes, rate-limit architecture, env vars, curl examples with JSON response excerpts, Stability statement, Quick Start, Attribution, Changelog. Phase 82/83.
- Stability statement: additive changes any time; breaking changes only on major-version releases announced in `CHANGELOG.md`. Phase 83.
- `README.md` "## API" section added linking to the public docs. Phase 83.

### Internal

- `shared/search_serializer.py` — Single source of truth for JSON envelope shape (search, browse, parallels). Phase 77.
- `skills/cairo-genizah-research/` — Reference Claude skill consuming all 3 endpoints with file-locked token-bucket throttling, tiered ranking, and browse-honesty annotations. Phase 81B.
- Phase 81A: `search_mode` enum (`exact` / `variants` / `responsa` / `title` / `shelfmark`) + `responsa_options` flag bag + `request` echo block on all responses. Breaking change from Phase 78 `mode` field (announced at Phase 81A deploy).

### Release Mechanics

- Web-only release. NO desktop installer rebuilt or distributed (`bump_version.py` updates `CompileScriptGenizah.iss` as housekeeping only).
- NO GitHub Release object created (desktop polls `releases/latest` for update prompts; a no-installer release would prompt every desktop user).
- NO `v7.10.0` git tag (consistent with prior web-only release pattern in `CLAUDE.md` "Recently Changed" history).
- Release identity lives in `version.py` + this CHANGELOG entry + the master-main commit SHA.
- Rollback: `SEARCH_API_MODE=disabled` env-var flip on production kills the public surface in seconds (zero-downtime).

---

## [7.9.4] - NLI Library Code Fix - 2026-05-04

A tiny data-only patch correcting library attribution for 461 National Library of Israel manuscripts that were rendering as Oxford in browse.

### Bug Fixes

- **NLI manuscripts shown as Oxford**: 461 rows in `libraries.csv` had `library_code=Oxford` despite the call_numbers containing only NLI shelfmarks (`The National Library of Israel Ms. Heb. ...` or `JER NLI Heb`). Browse routing keys off `library_code`, so external links and source toggles rendered as Oxford. Bad data has been present since `libraries.csv` was first introduced (commit `68dc0e99`); just surfaced now via a user report on `sys_id=990025143260205171`. Fixed via `scripts/fix_nli_oxford_mislabel.py` flipping the 461 unambiguous rows to `library_code=NLI`. (both apps)

### Internal

- New script `scripts/fix_nli_oxford_mislabel.py` retained for future audits — preserves CRLF line endings and only flips rows where call_numbers contain an NLI signal but no Oxford signal (Allony/Harkavy/HAS cross-listings deliberately untouched).

---

## [7.9.3] - Visual Similarity Dialog Fixes - 2026-04-24

A small web-only patch fixing three usability bugs in the Visual Similarity dialog, all surfaced by the same user report.

### Bug Fixes

- **Firefox: `Show more` unreachable past 20 results**: the right-pane Quasar `scroll_area` did not scroll reliably in Firefox, so the pagination control below the initial 20 suggestions could not be reached (Chrome was unaffected). Replaced the wrapper with a plain `div` using `overflow-y: auto; height: 100%`. (web)
- **Ctrl/Cmd-click and middle-click opened in the same tab**: shelfmarks and the `open_in_new` icon were `ui.button`s navigating programmatically via `ui.navigate.to()`, so browser-native modifier-click never fired. Converted shelfmarks to real `ui.link('/browse?sys_id=...')` anchors and wrapped the `open_in_new` icon in `ui.link(..., new_tab=True)`. (web)
- **Shelfmarks missing from copy-paste**: Quasar's `q-btn` sets `user-select: none`, so manually selecting the suggestion list excluded the shelfmark column — the most important field. `ui.link` uses a plain anchor with `user-select: text`, so shelfmarks now copy with the rest of the row. (web)

### Internal

- Remaining per-row action buttons (Add to Puzzle, Add to List, Add as Join) moved to `click.stop` handlers so they don't also trigger the row-expansion toggle. (web)

---

## [7.9.2] - PGP Data Refresh - 2026-04-22

Refreshes our bundled Princeton Geniza Project metadata (last imported February). Plus two small post-7.9.1 fixes.

### Data

- **PGP metadata refresh**: re-imported from [princetongenizalab/pgp-metadata](https://github.com/princetongenizalab/pgp-metadata). +147 documents, +159 source editions/translations, +211 footnotes, +345 fragment links. `pgp.db` rebuilt (148.6 MB).

### Bug Fixes

- **Web browse source buttons**: Oxford and NLI source-toggle buttons on `/browse` restored (regressed post-7.9.1). (web)
- **Desktop Cambridge nav**: fixed undefined `page_idx` in the Cambridge nav helper (ruff F821). (desktop)

---

## [7.9.1] - Catalog Attribution & Reading Desk Polish - 2026-04-22

Data-quality fixes across FJMS source attribution, JTS and Cambridge image alignment, plus Reading Desk UX polish on the desktop. Also ships Phase 64/65 code-review follow-ups, security hardening, and web log hygiene.

### Bug Fixes

- **FJMS empty catalog dialogs**: ~30K manuscripts that previously rendered empty `Catalog Information` dialogs (source label was `Instatution` — an export typo suppressed by 5 of 6 `GENERIC_SOURCE_NAMES` consumers) now show real institutional attributions. Local `CODE_Institution` join rewrote 267,104 `catalog` rows and 47,800 `catalog_free_desc` rows across 8 SubIds — top by volume: GRU – Cambridge (161K), Schocken-Zulay (51K), Fleischer Piyut Project (30K), Yad Harav Herzog (15K), Uri Ehrlich (8K). 100% resolution; 4 new regression tests. (both apps)
- **JTS browse source-switch button**: ENA manuscripts like `ENA 1052.1` (sys_id 990053572370205171) couldn't toggle to Princeton DPUL images. Stack of 4 bugs: (1) MARC 942$z parser now prefers first non-numeric value, avoiding FGP photo-ID clobber of the real shelfmark; (2) new `_jts_shelfmark_variants` helper tolerates `Ms.`/`MS.` prefix mismatch; (3) new `get_jts_urls_for_sys_id(sys_id)` resolves via `nli_images.Shelfmark ↔ jts_dpul.shelfmark` JOIN in one query, replacing up to 16 variant lookups; (4) NLI IIIF/MARC/Figgy timeouts tightened 10s→5s + per-sys_id negative cache + class-level circuit breaker (3 consecutive NLI failures → 60s skip), cutting JTS navigation lag ~25s → ~5s per hop. (both apps)
- **CUL CUDL/NLI image alignment**: Bifolio, binding-canvas, and count-matched-but-order-mismatched CUDL manifests (e.g., T-S NS 158.112 with 14 transcription pages vs 12 CUDL canvases; Or.2245 with same-count but index-2 divergence) could display a wrong-leaf image. New `classify_cambridge_alignment()` helper returns `aligned | misaligned | unknown` + reason; `enrich_metadata` computes the verdict once and 5 previously-duplicated decision sites consume it, defaulting the whole manuscript to NLI when misaligned. CUL-only scope preserves non-CUL Cambridge (Mosseri, Gaster, private CUDL). 7 new regression tests. (both apps)
- **CUL paired-leaf folio labels**: `_FOLIO_PATTERN` now accepts `L{first}_{second}F...S{side}` bifolio notation — bifolio CUL manuscripts now show folio labels (1r, 1v, 2r, 2v) in the picker instead of flat page numbers. (both apps)
- **Desktop past-CUDL auto-fallback**: Viewer auto-flips to NLI images when navigating past the last CUDL page, auto-restores CUDL on return. Also disambiguates `folio_num` vs 1-indexed `page_num` across 6 image-index call sites. 18 regression tests. (desktop)
- **Reading Desk "No images available"**: Fragments added from a list, the top shelfmark field, or the green-bar input that hadn't been browsed earlier in the session showed no images, because the viewer reads from `meta_mgr.nli_cache[sid]` which is only populated by metadata enrichment. New `_browse_rd_enrich_entry(sys_id, volume_ie=None)` helper launches enrichment on add and re-renders on completion. Threads deduped in a sys_id-keyed dict with `wait(1500)`-then-terminate on exit and window close. Volume-aware: when the added fragment matches the currently-browsed manuscript, its `volume_ie` is carried through so multi-IE fragments backfill the correct volume. (desktop)
- **Reading Desk green toolbar too tall**: Green bar used to stretch vertically because its `QWidget` used the default size policy. Now `QSizePolicy(Preferred, Fixed)` pins it to its inner scroll-row height; margins/padding also slimmed. (desktop)
- **Reading Desk "Add to View" ignored typed input**: Clicking Add to View with a new shelfmark typed in the top bar silently re-added the currently-loaded manuscript. Now resolves typed shelfmark/sys_id via `resolve_system_by_shelfmark` (with multi-match dialog + not-found warning) and adds that target. (desktop)
- **Reading Desk field empty on entry**: Green toolbar's shelfmark input was empty when the desk opened. Now pre-populates with the last-added entry so the user can tweak and press Enter to add a variant. (desktop)
- **What's New dialog RTL alignment**: Hebrew bullet paragraphs were left-edge aligned. Now the wrapping `<div>` carries `dir='rtl' align='right'` and the `QLabel` is set to `AlignRight | AlignTop` in Hebrew. (desktop)
- **Banner auto-dismiss ordering**: Site-wide "What's New" banner and `/home` OCR disclaimer previously persisted the dismissed flag before deleting the banner, so navigating away within 10s permanently hid the banner on next reload without user interaction. Now the flag is persisted only on successful `.delete()`. (web)

### Improvements

- **FJMS catalog within-source dedup**: Duplicate rows from the same SourceName no longer appear as separate entries. (desktop)
- **Web `/_nicegui/` framework assets marked noindex**: `X-Robots-Tag: noindex` header so internal ESM/module URLs don't surface as Search Console "pages". Still crawlable for rendering. (web)
- **Journalctl log hygiene**: 3 `ui.timer()` callsites in ephemeral containers (What's New banner, OCR disclaimer, `/home` carousel) converted to `asyncio.call_later` / `ensure_future`, eliminating recurring `RuntimeError: The parent slot of the element has been deleted` spam. (web)

### Security

- **PostgREST `.or_()` ilike sanitization**: Added `_sanitize_ilike_pattern(value)` helper that strips PostgREST filter separators (`,` `(` `)` `*`), LIKE wildcards (`%` `_`), backslashes, and newlines. Applied at 4 `supabase_corrections_client.py` callsites: `list_corrections`, `get_connected_fragments`, `list_all_fragment_joins`, `list_user_fragment_joins`. Closes Phase 64 CR-02. (desktop)
- **Supabase config unified via shared provider**: `supabase_corrections_client.py` now imports `SUPABASE_URL` / `SUPABASE_ANON_KEY` from `shared/supabase_provider.py`. Provider opportunistically calls `load_dotenv()` at import (non-fatal if missing), so desktop entry points pick up `.env` without their own call. Closes Phase 64 CR-01. (desktop)

### Data

- **`fjms_enrichment.db` upload** (1.6 GB, 2026-04-21 build): bundles the Instatution migration above plus Shivtiel transliteration fix (ת→ט across 14,608 rows), Sussmann Supplement translation (111 rows), and 36 empty "צוות 500" placeholder records removed.

### Internal

- **`/_internal/memstat`** diagnostic endpoint (gated by `MEMSTAT_SECRET` header) added for leak investigation post-v7.9.0. (web)
- **Phase 65 code-review cleanup**: WR-01 `import re as _re` moved to top of `web/main.py`; WR-02 misleading exception comment in `puzzle_tokens.py:verify_upload_token` corrected; IN-01 redundant `except (AssertionError, Exception)` simplified in `auth_state.py`; IN-02 NiceGUI upgrade threshold extracted to `_PATCH_AUDIT_THRESHOLD` constant with module-load WARNING on exceed.

---

## [7.9.0] - Structural Foundation + Decomposition - 2026-04-19

Bundles the two internal GSD milestones `v7.8` (Structural Foundation, 2026-04-15) and `v7.9` (Decomposition, 2026-04-17) into a shippable release, plus a back-navigation bugfix caught during Phase 75 verification and a CUL paired-leaf folio-label fix. Zero user-visible behavior changes except the two bug fixes below.

### Bug Fixes
- **Back-navigation state loss**: browser Back from `/browse` to `/search` now restores the saved snapshot; regression introduced 2026-03-27 (commit `829cd7cf`) and fixed 2026-04-17 (commit `8f9c5ef3`). Caught during Phase 75 non-regression verification.
- **CUL paired-leaf folio labels**: `parse_folio_label` now handles paired-leaf bifolio `ImageName` patterns (e.g., T-S NS 158.112), so image-text alignment is correct for these manuscripts.

### Internal: v7.9 Decomposition (milestone complete 2026-04-17)
10 phases, 23 plans. Zero user-visible behavior changes.
- **Desktop split**: `ResultDialog`, filter/scholarly dialogs, image viewers (`ManuscriptViewerWidget`, `FullscreenImageWindow`), puzzle canvas, VS cache, and shared widgets extracted into a new `desktop/` package — `genizah_app.py` slimmer (though remaining core still ~22.5K lines per external review)
- **Web split**: `web/pages/search.py` decomposed into `search_state.py` + `search_results.py`; `web/pages/browse.py` decomposed into `browse_state.py` + `browse_enrichment.py`
- **Page-scoped state refactor**: reduced `app.storage.user` sprawl and detached `asyncio.ensure_future` usage in search and browse pages
- **Documentation**: `docs/CODE_INDEX.md` v7.9 section regenerated via new `scripts/gen_code_index_section.py` AST generator; `check_docs` green

### Internal: v7.8 Structural Foundation (milestone complete 2026-04-15)
4 phases, 9 plans, 64 commits, 173 files changed (+6,269/-828 lines). Zero user-visible behavior changes. 12/12 requirements satisfied.
- **CI safety net**: GitHub Actions with Ubuntu + Windows matrix running `ruff` + `check_docs` + `pytest` (`.github/workflows/ci.yml`)
- **Dependency pinning**: `requirements.txt` (14 direct) + `requirements-lock.txt` (115 transitive) for reproducible builds
- **Supabase auth migration**: deprecated `gotrue` error surface replaced with `supabase_auth`; PKCE-only OAuth flow
- **Silent-exception audit**: 205+ `except: pass` handlers reviewed across 76 first-party files
- **Framework-patch isolation**: NiceGUI monkey-patches moved into `web/framework_patches.py` with version guards and logging
- **Repo hygiene**: `.gitignore` 50 → 126 lines; untracked root files 67 → 1; CODE_INDEX / OPEN_ISSUES / DEVELOPER_GUIDE docs refreshed

### Build / Release Tooling
- **`scripts/bump_version.py`**: added README installer-filename regex so `GenizahSearchPro_V<X.Y.Z>_Setup.exe` stays in sync on future bumps. Closes long-standing drift (filename had been stale since v6.1.1, eight releases).

---

## [7.7.2] - PageSpeed Quick Wins (A11y + Perf) - 2026-04-13

### Accessibility
- **Valid html lang attribute**: Fixed Lighthouse "Document does not have a valid `lang` attribute" by passing full Quasar lang pack (with `isoName`) to `Quasar.lang.set()` instead of partial `{rtl: false}` object, which caused `<html lang="undefined">`. Added JS belt-and-braces guard + NiceGUI template patch at startup (web)
- **Aria-labels**: Added descriptive `aria-label` to 10 icon-only buttons (help, dismiss, theme toggles, citation copy/close, OCR banner dismiss, hero search) (web)
- **Color contrast (WCAG AA)**: Light-theme `--text-muted` `#94a3b8` → `#64748b` (2.34:1 → 4.63:1); global link color `--primary-700` replaces Quasar default `#5898d4` (3.06:1 → 5.44:1); dark-theme overrides for `--text-muted`, `--primary-600/700`, and Quasar primary/secondary/accent tokens so inline links and badges meet AA on dark backgrounds (web)
- **Heading hierarchy**: Homepage "What is the Cairo Genizah?" promoted from `h3` to `h2` (web)

### Performance
- **font-display: swap**: Starlette middleware injects `font-display: swap` into NiceGUI's `fonts.css` response, preventing ~1200ms of invisible text on slow connections (web)
- **Conditional iiif preconnect**: `<link rel="preconnect" href="https://iiif.nli.org.il">` now only emitted on routes that actually load manuscript images (`/search`, `/browse`, `/puzzle`) instead of every page (web)

### Results (Lighthouse desktop, homepage)
- Accessibility: 85 → 96 (target ≥95)
- Performance: 90 → 98 (target ≥93)
- SEO: 100 (unchanged)
- Remaining: 13 parchment-theme color-contrast warnings (same root cause as dark-theme fix, deferred — a11y still above target)

---

## [7.7.1] - SEO Round 2 - 2026-04-13

### Improvements
- **Bilingual meta tags**: Default title now bilingual (English brand + Hebrew search phrase + Hebrew brand) so the site can rank for Hebrew queries like "חיפוש בגניזה הקהירית" while preserving English brand identity. Description, keywords, and JSON-LD alternateName also include both languages (web)
- **Per-page titles**: Indexable pages (`/browse`, `/catalog-browse`, `/about`) use English-leading bilingual format; manuscript pages stay shelfmark-first; low-intent pages (`/help`, `/download`, `/accessibility`) restored to original concise English (web)
- **Homepage h1**: Updated to "אתר הגניזה של דיקטה — חיפוש בגניזה הקהירית" (in Hebrew UI mode) so target search phrases appear in visible above-the-fold content for crawlers (web)
- **Structured data**: Added Organization JSON-LD on homepage and BreadcrumbList JSON-LD on browse manuscript pages. Also added Sitelinks Search Box (SearchAction) markup — note: Google deprecated this surface in November 2024, but markup is harmless and may be used elsewhere (web)
- **Performance**: PostHog analytics deferred past first paint via requestIdleCallback; dns-prefetch hints added for analytics CDNs (web)
- **Title consistency fix**: Client-side intra-app navigation on `/browse` no longer overrides server-rendered title to a different format (web)

### Known Limitations
- Real performance measurement (Lighthouse / PageSpeed Insights / Search Console) was NOT performed in this release — placeholder analysis only. Schedule a follow-up with real Search Console URL Inspection and PSI runs after deploy.
- Site is single-URL bilingual (no `/he/` vs `/en/` routes, no hreflang). Google sees Hebrew as the default rendered language because UI defaults to Hebrew. Future phase: per-language URLs with hreflang.
- Browse and catalog page bodies are mostly empty until WebSocket hydration — limits crawlable text content. Tracked in `docs/OPEN_ISSUES.md`.

---

## [7.7.0] - Volume-Aware Browse - 2026-04-01

### New Features
- **Volume-aware browsing**: 3,193 multi-IE manuscripts now show a volume selector, allowing users to switch between different microfilm scans (IEs) with correct text and images per volume (both apps)
- **Volume-specific community data**: Corrections and comments are tagged with the active IE, so notes on Volume 2 only appear when browsing Volume 2 (legacy data with no IE tag still shows everywhere)
- **Auto-default to external image sources**: Manchester LUNA, Cambridge IIIF, and JTS/Princeton DPUL images now auto-load when available, instead of waiting for manual source switching (web)

### Improvements
- **Volume-correct external images**: Manchester and Oxford images properly offset by volume — Volume 2 shows the correct folio, not Volume 1's images (both apps)
- **Desktop external image filtering**: Manchester/Cambridge/JTS canvases filtered to active volume's pages instead of showing all volumes' images (desktop)
- **Volume page counts**: Volume selector shows actual transcription page count per volume instead of total IIIF manifest pages (both apps)
- **Thread-safe browse_map loading**: `threading.Lock` replaces boolean flag to prevent concurrent pickle read/write race conditions
- **Browse_map IE repair**: Automatically restores pages from non-primary IEs that were lost by pre-v7.7 deduplication, with UID format correction for Tantivy index compatibility

### Bug Fixes
- **Multi-IE image/text mismatch**: Browse page showed images from Volume 1 regardless of which volume's text was displayed — now each volume shows its own matching images
- **Browse_map pickle corruption**: Multiple SearchEngine instances loading simultaneously no longer causes "pickle data was truncated" errors
- **Notes/comments ie_id gap**: `create_notes_panel` and `create_notes_button` now pass volume IE to comment queries

---

## [7.6.0] - Visual Similarity Suggestions - 2026-03-31

### New Features
- **Visual Similarity browse dialog**: New "Visual Similarity" button in the browse page opens a side-by-side workbench showing ranked image-similarity partners from FJMS SVM analysis (~15.5M pairs). Each suggestion shows a thumbnail, shelfmark, domain, library, and score — with Browse, Puzzle, and "Add as Join" action buttons (both apps)
- **Search in visual suggestions**: From the VS dialog, restrict a text search to the suggestion partner pool. Supports union (any manuscript's partners) and intersection (shared partners only) modes for multi-manuscript selection (both apps)

### Improvements
- **VS text snippet preloading**: First 20 suggestions preload text snippets in the background for instant preview
- **VS performance**: 4 regressions fixed from Codex audit — batch enrichment, lazy loading, query optimization
- **Exclusion dialog "Active exclusions" section**: Clicking the "Exclude Manuscripts" button now shows currently active sources with per-source remove buttons and "Clear all"
- **New Search clears exclusions**: The reset button now properly clears exclusion sources and their persisted storage

### Bug Fixes
- **Exclusion chip persisted after New Search**: "Exclude Manuscripts (N)" button remained visible after clicking "New Search" — now properly cleared
- **VS dialog scrolling**: Fixed web VS dialog right pane not scrolling to show all suggestions
- **VS dark mode**: Fixed background colors and search URL parameters in dark mode
- **Desktop VS puzzle integration**: Fixed add_to_puzzle method call from VS dialog

---

## [7.5.0] - Exclude Known Manuscripts - 2026-03-29

### New Features
- **Exclude known manuscripts**: Hide already-reviewed manuscripts from search results using saved lists, imported shelfmark files (TXT/CSV), or pasted shelfmarks. Multi-source tracking with per-source clear, collapsible excluded section showing what was hidden and why, resolution report table with per-row status (found/not found/duplicate). Available in both web and desktop apps
- **Web exclusion dialog**: Tabbed picker with "Paste Shelfmarks" (default), "From List" (expandable with per-manuscript checkboxes), and "From File" (upload with resolution preview). Three entry points: results header button, post-search filter panel, and "Search only in..." pre-search panel
- **Desktop exclusion dialog**: Enhanced ExcludeDialog with QTabWidget — "From File / Manual" tab with resolution report table and "From List" tab with "Load to Editor" workflow for review before applying

### Improvements
- **Desktop exclusion by row hiding**: Uses QTableWidget `setRowHidden()` to instantly toggle visibility without re-rendering — preserves enrichment state (domain badges, printed indicators, scroll position)
- **Export respects exclusions**: Both desktop and web exports (Excel/CSV/Word) now only include visible (non-excluded) manuscripts (WYSIWYG export)
- **Session persistence**: Exclusion sources survive page navigation and session restore, with backward compatibility for legacy exclusion state
- **38 Hebrew translations**: Full bilingual coverage for all exclusion UI strings

### Bug Fixes
- **Bracket-aware search**: Searching `נשתנה` now finds `]נשתנה` (bracket-transparent matching)

---

## [7.4.0] - Search Within Results - 2026-03-29

### New Features
- **Search within results**: Progressive refinement — run a second query restricted to manuscripts from your current result set. Breadcrumb chip chain shows refinement history with per-chip removal, cross-mode support (text, Responsa, Title, Shelfmark), "Only results with all terms" page-level filter, and chain-aware snippet highlighting
- **Lightweight browse first-render**: Browse page first paint now uses only Tantivy + csv_bank (zero SQLite calls). Crossref, Oxford, Cambridge, and attribution data load asynchronously in Phase B enrichment — faster first content for users and 255K sitemap URLs for crawlers

### Improvements
- **Thread-safe SQLite services**: All shared services (FJMS, NLI, PGP, Translation) now use per-thread connections for safer concurrent access
- **NiceGUI ESM handler hardening**: Patched ESM static file handler to reject directory path traversal

### Bug Fixes
- **Bracket-aware search**: Searching `נשתנה` now finds `]נשתנה` (bracket-transparent matching). Searching `]נשתנה` still matches only the bracketed form (literal match). Two-layer fix: Tantivy OR-expansion with bracket variants for candidate recall, plus conditional bracket stripping in regex phase. Correctly handles Responsa `[N]`/`[|N]` gap operators, position filters (start/end/line), and composition search
- **csv_bank race condition**: Fixed `dictionary changed size during iteration` error in metadata search under concurrent access
- **Browse stale enrichment guard**: Generation counter re-checked after deferred Oxford translation fetch to prevent stale state on rapid navigation
- **Metadata-only page state**: Records without transcription text now correctly show page 1 with folio label after enrichment (was stuck at page 0)

---

## [7.3.1] - SEO Foundation & Shareable Browse URLs - 2026-03-27

### New Features
- **Shareable browse URLs**: Browser URL bar now updates to reflect the current manuscript and page (e.g., `/browse?sys_id=...&page=2`) as you navigate, so links can be copied and shared. Preserves highlight terms within the same manuscript, clears them on manuscript change
- **Share button**: New share icon in the manuscript header toolbar copies the current URL to clipboard with visual toast feedback

### Improvements
- **Per-page metadata**: Every web route now has a unique title, description, canonical URL, and OG/Twitter tags (replaced global META_TAGS that pointed every page's canonical at the homepage)
- **Manuscript-specific metadata**: Browse pages with sys_id resolve the real shelfmark for title/description/canonical (e.g., "T-S 12.123 — Manuscript | Dicta Genizah Search")
- **Manuscript sitemap**: Sitemap index with ~255K manuscript URLs in 40K-per-file chunks, replacing the previous 11-URL static sitemap
- **Indexability policy**: Search, parallels, lists, settings, corrections, admin, and profile pages marked `noindex, follow`; robots.txt aligned
- **Homepage structured data**: WebSite JSON-LD with publisher, bilingual name, and languages
- **Performance hints**: Preconnect for NLI IIIF server, DNS prefetch for Cambridge CUDL

---

## [7.3.0] - Manuscript Measurements, Bibliography Cleanup & Desktop Stability - 2026-03-26

### New Features
- **Manuscript Measurements dialog**: New "Measurements" button in browse (web + desktop) opens a detailed dialog showing physical dimensions, margins, line counts, text density, material, and DPI quality — per-image data from 434K computed measurements and 179K catalog size records across 231K manuscripts

### Improvements
- **Bibliography dedup**: Removed ~401K duplicate bibliography entries (828K → 427K, 48.4% reduction) via exact and near-dupe merge passes
- **55K new Hebrew translations**: English free descriptions (FreeDesc) now available in Hebrew via Dicta Translation, with hallucination filtering
- **Persistent NLI FL-ID cache**: Cache survives service restarts, reducing repeated IIIF manifest lookups
- **NLI concurrent fetches**: Default bumped from 4 to 8 for faster image loading

### Bug Fixes
- **Desktop browse tab crash**: Fixed crash when rapidly navigating between manuscripts — added 150ms navigation debounce, generation guard, and proper QThread lifecycle for image loaders
- **Desktop image thread lifecycle**: Threads now properly terminate on teardown instead of being dropped, preventing orphaned workers
- **ResultDialog image threads**: Wait-or-terminate pattern prevents thread leaks when closing dialogs
- **Profiles FK join**: Reverted broken foreign key join in community comments/responses, using batch lookup instead

---

## [7.2.4] - JTS Image Upgrade + Shelfmark Search Fixes - 2026-03-25

### New Features
- **Princeton DPUL as primary JTS image source**: JTS manuscripts now auto-default to Princeton Digital PUL images (36,283 items via full DPUL catalog import v2), replacing unreliable manifest-based loading
- **Desktop "Printed" badge**: ResultDialog info row and browse tab now show "Printed"/"דפוס" badge for printed materials, with per-session FJMS cache
- **Blue mat auto-detection**: Background removal now auto-detects blue conservation mats across all libraries instead of hardcoding CUL only

### Improvements
- **Enhanced shelfmark lookup**: Strips full library names ("Cambridge University Library", "British Library") not just codes; ENA-MS/ENA MS normalized to ENA for JTS variant matching
- **FJMS bibliography enrichment**: 8 new FIST fields (JournalVolumeTxt, Hebrew title support), fixed volume source attribution
- **Deduplicated catalog descriptions**: FJMS catalog free descriptions no longer show duplicates in get_catalog_detail()
- **Search performance**: Removed duplicate search enrichment pass and redundant background FJMS prewarm
- **Shared JS extraction**: Filter panel and manuscript viewer JavaScript extracted into reusable modules (~1,050 lines deduplicated)

### Bug Fixes
- **Browse shelfmark search**: Fixed slot context, stale content, and async caller issues
- **PostHog UX & auth**: Rageclick prevention (immediate button disable), OAuth implicit flow fix, login tracking enrichment, login dialog for anonymous write actions
- **JTS external link**: Now points to DPUL catalog page instead of raw manifest URL
- **Puzzle from ResultDialog**: Fixed add-to-puzzle using correct viewer image list, proper shelfmark/fl_id, and auto-close dialog
- **Puzzle external fragments**: Fixed session restore, metadata persistence, NLI timeout skip for non-NLI libraries, NiceGUI context loss on add-from-browse
- **Puzzle extension banner**: Fixed English text showing in Hebrew UI
- **Image viewer**: Guarded viewer.init() call in handleImageError for viewers without init

### Tests
- Fixed puzzle model tests (join_type default changed to 'physical')
- Fixed responsa explosion guard tests (Hebrew tr() warning assertions)

---

## [7.2.3] - Chrome Extension Live + Puzzle Enabled - 2026-03-20

### New Features
- **Chrome Web Store install link**: Puzzle extension install banner now includes a clickable "Install Extension" / "התקינו את התוסף" button linking to the live Chrome Web Store listing
- **Puzzle enabled by default**: `WEB_PUZZLE_ENABLED` now defaults to `True` — the Fragment Puzzle page is available on all deployments without setting an environment variable

### Improvements
- **Banner text escaping**: Switched from manual `.replace()` to `json.dumps()` for safer JS string injection in puzzle banner texts

---

## [7.2.2] - Desktop Browse Tab Polish - 2026-03-20

### Improvements
- **Browse button icons**: All browse tab buttons now have emoji icons matching ResultDialog style (Puzzle, Parallels, List, Info, View on Ktiv, View Corrections, Add to View)
- **Reorganized action row**: Action buttons (Puzzle, Parallels, List) moved from crowded top bar to dedicated ext_info_row alongside Info, Bibliography, and Catalog buttons
- **External library links**: New button shows Cambridge/Oxford/Manchester/Princeton link dynamically based on manuscript source, with globe icon
- **Compact translations toggle**: Translations ON/OFF replaced with a colored icon button (green when ON, grey when OFF) with tooltip
- **Cross-shelfmark page navigation**: Prev/Next page buttons no longer disable at manuscript boundaries — navigating past last page wraps to first page of next shelfmark, and vice versa (matching ResultDialog behavior)
- **Extended info state preserved**: Extended info panel open/close state remembered when navigating between shelfmarks
- **ResultDialog image toggle**: Hide/show image state preserved when navigating between search results
- **Fullscreen image viewer**: New fullscreen mode for manuscript images (both Browse and ResultDialog) — zoom/pan, rotation, brightness/contrast/gamma/invert, page navigation with arrow keys or buttons, Escape to close

### Bug Fixes
- **Enrichment race condition**: Fixed stale Oxford metadata appearing on RNL manuscripts — `browse_load()` did not clear `current_browse_part_id` when loading non-Part manuscripts, causing Oxford Part context to leak into subsequent enrichment callbacks
- **Centralized enrichment launch**: All 4 browse enrichment paths consolidated into `_start_browse_enrichment()` with generation counter to reject stale queued cross-thread signals
- **Lambda disconnect fix**: Stored lambda slot reference for proper `disconnect()` instead of trying to disconnect bare method (no-op)
- **Ext-info restore flag lifecycle**: Flag survives from enrichment to PGP callback for PGP-only manuscripts; cleared unconditionally at final async step to prevent forward leak

---

## [7.2.1] - Search UX Overhaul - 2026-03-19

### New Features
- **Hero search bar**: Prominent search input on home page below "What is the Cairo Genizah?" card, with Hebrew translation ("חיפוש בכתבי יד...")
- **Inline accordion results**: Replaced the 35/65 splitter layout with full-width results. Clicking a result expands an inline accordion showing manuscript thumbnail image + highlighted full text with original line breaks
- **Result card action buttons**: Browse, Quick View (renamed from Advanced View / מבט מהיר), Add to List, and Catalog Records buttons directly on each result card
- **Citation footer auto-collapse**: Full citation shows for 10 seconds, then collapses to a compact single line with copy button
- **Thumbnail images**: New `width` parameter on `/api/nli_image_by_sysid/` endpoint — accordion requests 300px thumbnails instead of 2000px full images
- **Lazy text loading**: On session restore, full text loads on first accordion expand via `get_browse_page()`
- **Clickable thumbnails**: Clicking the accordion image opens Quick View dialog
- **Progressive image loading**: All web image viewers (browse standard/fullscreen, Quick View normal/fullscreen) now show a spinner → 400px thumbnail → 2000px full resolution. CSS `.img-loading-container` spinner + JS `progressiveLoad()` upgrade chain. Desktop already had this pattern.

### Bug Fixes
- **Homepage search stuck**: `_after_delay()` deferred tasks ran in a separate asyncio task without NiceGUI slot context, causing silent `RuntimeError`. Fixed by capturing `ui.context.client` at page creation
- **Status duplication**: Merged "Search completed in X — N Results" into results header ("588 Results · 0:18"), eliminated duplicate timer-based status messages
- **Browse rotation slider**: Removed JS string handler passed to `.on('update:model-value')` where NiceGUI expects a Python callable — Python `on_change` already handles rotation

### Technical
- Web: `progressiveLoad()` JS function in VIEWER_STYLES — loads `?width=400` thumbnail, preloads full via hidden `Image()`, swaps `src` on completion
- CSS: `.img-loading-container` animated spinner, `.img-loaded` hides it
- NiceGUI: `_page_client = ui.context.client` captured at page creation, entered with `with _page_client:` in deferred async tasks
- API: `/api/nli_image_by_sysid/` accepts `width` param (100-2000, default 2000), IIIF requests use `full/{width},/0/default.jpg`

---

## [7.2.0] - Image Adjustment Controls - 2026-03-19

### New Features
- **Image adjustment controls**: Brightness, contrast, gamma sliders and invert toggle on all image viewers — web browse (standard, fullscreen, reading desk), web search advanced view, and desktop ManuscriptViewerWidget
- **Desktop export with adjustments**: Right-click Copy Image and Save Image As export the adjusted (filtered) image, not the raw original
- **Icon-based toolbar**: Compact icon labels (sun, half-circle, curve, ±, reset arrow) with translated tooltips replacing text labels

### Bug Fixes
- **Browse page crash**: Fixed `RuntimeError: slot stack empty` — async `load_page()` called `ui.run_javascript()` outside NiceGUI slot context, killing the entire page render
- **Desktop image race condition**: Stale debounce timer from previous image could fire after new image loaded, reverting the display. Fixed by cancelling timer in `set_image()`
- **Desktop thumbnail race**: Stale thumbnail callback overwrote new image when switching manuscripts with same page index. Fixed with `_load_generation` counter

### Technical
- Web: CSS `filter` property for brightness/contrast/invert (native) + SVG `feComponentTransfer` for gamma. Per-viewer SVG filter IDs for reading desk multi-image isolation
- Desktop: QImage LUT-based pixel processing with 80ms QTimer debounce. 256-entry lookup table applied via `bits()` direct byte access
- Hebrew translations: בהירות, ניגודיות, גמא, הפוך צבעים, איפוס תמונה

---

## [7.1.0] - FIST Gap Fill & Expanded Catalog - 2026-03-19

### New Features
- **38,673 new manuscript records**: FIST.db gap records merged into libraries.csv (216,942 → 255,615 records, +17.8%). Manuscripts from 52 libraries now browsable, searchable by shelfmark/title, and enrichable via FJMS catalog data and NLI images
- **7 new library codes**: Solomon Halberstam, Reinach, Vatican, Central Archives, JC Mainz, Corwin, Mehlman — with Hebrew translations
- **Metadata-only search**: Title and shelfmark search now returns records without transcription text. Results carry a `metadata_only` flag; page navigation hidden for text-less records
- **Metadata-only browse**: Browse page shows metadata, NLI images, FJMS enrichment, and external links for records without Tantivy text (instead of "No text available" error)
- **Shelfmark normalization**: Yevr→EVR (Russian National Library) and Halper→Genizah (CAJS) aliases with Halpern guard

### Bug Fixes
- **Mosseri CUDL images**: Mosseri collection images now load via Cambridge Digital Library fallback path (both apps)
- **Puzzle auto-fit**: Canvas auto-fits view only on document load, not when adding individual fragments
- **Server heartbeat**: Status heartbeat survives transient failures; removed JS ping in favor of server-state-only checks
- **Server init ordering**: Engine marked ready before FJMS pre-warm to prevent premature "loading" state
- **JWT auto-retry**: Browse guards against deleted Supabase client; automatic retry on JWT expiry

### Data
- Generation script: `scripts/generate_fist_gap_csv.py` (repeatable with `--dry-run` and `--validate-only`)
- Stats report: `docs/FIST_GAP_FILL_STATS.md`
- Gap manifest: `fist_gap_manifest.txt` (38,673 AlmaIds for validation)

---

## [7.0.1] - Web Puzzle Browser Extension - 2026-03-18

### New Features
- **GenizahSearch Image Helper extension**: Chrome/Firefox extension fetches NLI manuscript images via user's browser, bypassing datacenter IP blocks. Submitted to Chrome Web Store and Firefox AMO
- **Server derivative cache**: Processed images cached on server disk; once cached, available to all users without extension
- **Unified image loader**: Single `_loadImageWithFallbacks()` function replaces 4 separate fallback chains (add/reload/folio/restore)
- **HMAC upload tokens**: Secure cache writes — server issues signed tokens on cache miss, uploads require valid token
- **Extension install banner**: Bilingual dismissible banner when extension not detected; green "Extension active" indicator when present
- **Cache key versioning**: `PROCESSING_VERSION` in cache keys for automatic invalidation when bg removal algorithm changes
- **Privacy policy page**: `/privacy-extension` route for Chrome Web Store listing requirement

### Security
- `POST /api/puzzle_process` hardened with token verification, 10MB size limit, content-type validation, rate limiting (60/min/IP)
- New `POST /api/puzzle_upload_derivative` endpoint with same protections
- Extension validates URL origin (only `iiif.nli.org.il`) and message origin (only `genizahsearch.com`)

### Bug Fixes
- **Manchester LUNA recto/verso**: Both recto and verso showed the same (recto) image because each Manchester page has its own luna_id but only the first was fetched. New `get_manchester_canvases()` resolves ALL crossref images directly to individual IIIF canvas entries, bypassing the single-manifest approach (both apps)
- **Library attribution credit lines**: All non-Oxford manuscripts showed NLI default attribution. Now each library gets proper credit: Manchester (CC BY-NC-SA 4.0), Oxford (CC BY-NC 4.0), Cambridge/JTS (from IIIF manifest), and NLI-digitized collections (BL, RNL, AIU, Gaster, Mosseri, etc.) acknowledge both holding institution and NLI digitization. Web credit footer links to correct library website (both apps)

### Infrastructure
- Nginx: removed stale `location /api/` block that proxied to dead port 8000 (old FastAPI)
- `WEB_PUZZLE_ENABLED=true` set on production via `.env` for staged rollout

---

## [7.0.0] - Fragment Puzzle & Community Publishing - 2026-03-17

### New Features — Fragment Puzzle (Phases 47-51)

- **Fragment Puzzle canvas**: Visual workspace for arranging manuscript fragments side-by-side to reconstruct physical joins. HSV-based automatic background removal, zoom/rotate/crop controls, folio navigation (prev/next page), multiple background modes (dark gray/black/white/checkerboard/light table/grid) (both apps)
- **Save/Load puzzle arrangements**: Persistent "join documents" in local `joins.db` SQLite sidecar. Documents include title, notes, and all fragment positions/transforms. Auto-save after canvas changes (both apps)
- **Composite PNG export**: Full-resolution RGBA PNG with transparent background and metadata banner. Desktop offers draft (1000px) / standard (2000px) / full (3000px) resolution choices with progress dialog. Web downloads directly
- **Recto/Verso**: Automatic verso view generation from recto arrangement with correct verso images (both apps)
- **Bring Forward / Send Backward**: Layer ordering controls for overlapping fragments — toolbar buttons and desktop context menu (both apps)
- **Fragment selector combobox**: Dropdown showing all fragments on canvas, syncs with canvas selection. Browse button opens selected fragment in browse view (both apps)
- **Add from Lists / Known Joins**: Quick-add fragments from personal lists or from known FJMS/PGP joins for the current manuscript (both apps)
- **Saved Joins panel** (desktop): Side panel with thumbnails, details editing, delete, rename
- **Saved Joins dialog** (web): Modal dialog with load/delete/metadata editing

### New Features — Community Publishing (Phase 52)

- **Publish puzzle joins**: Share fragment arrangements with the research community. Publish button turns green when published, share dialog with copyable deep link (`/puzzle?doc={id}`) (both apps)
- **Discoveries Center integration**: Published puzzle joins appear in the community feed with composite thumbnails, author names, and shelfmark badges. "Published Puzzles" stat card in stats row
- **Fork & Open**: "Open in Puzzle" creates a local copy of any published join and opens it in the puzzle canvas (both apps)
- **Community Puzzle Joins panel**: When browsing a manuscript, see all published puzzle joins containing that fragment (both apps)
- **All Puzzles / My Puzzles tabs**: Browse and manage published puzzle joins in the community Joins section (both apps)
- **Clickable shelfmark badges**: Shelfmark badges on published joins navigate to the browse page for that manuscript
- **Admin soft-delete**: Admins can hide published puzzle joins from the community feed
- **Auto-unpublish on delete**: Deleting a local join automatically removes it from the community

### Improvements

- **Desktop toolbar compacted**: Text buttons replaced with emoji icon buttons (28px) with translated tooltips
- **Save dialog with notes**: Title and notes fields (was title-only)
- **Theme-aware web dialogs**: CSS variables for light/dark mode compatibility
- **Stats cards layout**: 7 stat cards fit in one responsive row on Discoveries page
- **Saved joins list dedup**: Hides shelfmarks line when it duplicates the title (handles reversed order, fork prefixes)
- **Help Center updated**: New Fragment Puzzle and Community Publishing sections in both web and desktop help (bilingual)
- **Hebrew translations**: 50+ new strings for all puzzle and community features

### Bug Fixes

- **Web puzzle page crash**: `ui.left_drawer` replaced with `ui.dialog` modal
- **Export position accuracy**: Fixed per-fragment `coord_scale` drift — uses single global scale factor
- **Export bg-removal fidelity**: Reuses same 800px processed image shown on canvas
- **Desktop export UI freeze**: Export moved to background thread with progress dialog
- **CUL blue conservation mat**: Two-pass background detection for colored conservation mats
- **Web publish button invisible**: Removed `flat` prop when published so green background shows
- **Fork button RuntimeWarning**: Async fork coroutine was not being awaited
- **Desktop discovery stats all zeros**: `get_discovery_stats()` now queries all relevant tables
- **BrowseState.meta_mgr AttributeError**: Guarded with `getattr` in joined view Oxford detection
- **Reading Desk from joined view**: Added `meta_mgr.resolve_system_by_shelfmark()` as fallback for fragment resolution
- **Desktop corrections showing anonymous**: Added profile batch-fetch to `get_my_corrections` and `get_all_corrections`
- **Index rebuild fails on Windows (WinError 5)**: Tantivy memory-mapped files were locked by the live searcher during rebuild. Now releases index before `shutil.rmtree` and reopens after
- **Publish broken joins**: `publish_join()` now fails fast if composite image generation returns None, with storage rollback on partial upload failure
- **Stale fragment selector after folio nav**: Fragment combobox now refreshes on folio prev/next and meta updates (both apps)
- **Web puzzle reload loses document identity**: `current_doc_id` now persisted to session storage and restored on page reload

---

## [6.5.4] - 2026-03-16

### Performance

- **Staged search enrichment**: Search results now appear immediately after core search completes, before metadata (domains, transcription badges, catalog counts, printed flags, translations) finishes loading. Enrichment runs in three progressive stages: title translations first (~1ms), then visible page (50 IDs), then remaining results in background chunks (200 IDs each)
- **FJMS build-time indexes**: 6 database indexes previously attempted at runtime (silently failing on read-only connection) are now pre-built in `fjms_enrichment.db` during export. Requires sidecar rebuild
- **Search generation guard**: New searches immediately cancel stale background enrichment from previous searches, preventing data overwrites
- **Performance instrumentation**: Timing spans added to search logger (`first_render_ms`, `visible_enrichment_ms`, `background_enrichment_ms`) for regression tracking

---

## [6.5.3] - 2026-03-15

### Improvements

- **Desktop image viewer — right-click menu**: Added context menu to the manuscript image viewer (both ResultDialog and Browse by Shelfmark) with "Copy Image" and "Save Image As..." options. Supports PNG, JPEG, and BMP export. Rotation is preserved in both copy and save

---

## [6.5.2] - 2026-03-15

### Improvements

- **Desktop ResultDialog — icon+text buttons**: Converted cluttered text-only buttons to compact icon+short text format across action row (📖 Browse, 🔍 Parallels, ⭐ List, ℹ️ Info, 📚 Bib, 📋 Catalog, 🌐 Trans), community row (📝 Corrections), and image toolbar (↩️ Reset, 🔗 External/Ktiv). All buttons include full-text tooltips
- **Web language toggle**: Moved language switch button from sidebar footer to header bar for better visibility

---

## [6.5.1] - 2026-03-14

### Improvements

- **Desktop session persistence — browse tabs**: Browse by Shelfmark restores the last viewed manuscript (text + images) on restart. Browse by Identification restores domain tree selection, date range, text filter chips, and undated checkbox
- **Desktop session persistence — composition search**: Composition search restores results (flat view), summary bar, sort mode, and appendix threshold
- **Desktop session persistence — active tab**: The last active tab is restored on restart (previously always returned to Search tab)

### Bug Fixes

- **Desktop composition search — ResultDialog navigation**: Fixed missing next/prev navigation when opening filtered (high-frequency) results. The tree traversal now recursively descends through all levels including filtered reason sub-groups and lazy-loaded appendix groups
- **Desktop composition search — lazy appendix ordering**: Lazy appendix groups are now sorted before being added to the ResultDialog navigation list, matching the order shown when groups are expanded in the tree
- **Web parallels page — parent_slot crash**: Fixed "The parent slot of the element has been deleted" RuntimeError by replacing all `ui.timer()` calls with `asyncio` patterns that don't attach to NiceGUI parent slots. The repeating progress timer and one-shot init timers no longer crash when users navigate away from the page
- **Web Hebrew UI — first-load drawer bootstrap**: Fixed the cold-start race where the drawer could paint on the wrong side on the first load and only settle after navigation/reload. The web bootstrap now resolves the persisted UI language before layout creation and retries Quasar RTL activation until the framework is ready

---

## [6.5.0] - 2026-03-13

### Milestone: Search UX & Filtered Search

Focused search by manuscript properties, ~924K catalog & metadata translations, line-boundary search for join detection, and cumulative improvements from 6.2.1–6.2.4.

#### Focused Search — Pre-Search Filtering (Phase 45)
- **Focused search panel**: Filter manuscripts by domain, author, work, date range, and material type before searching — narrows the corpus to a specific subset (both apps)
- **Removable chip bar**: Active filters shown as color-coded removable chips above results (purple=domain, blue=author, teal=work, orange=date, red=material)
- **Real-time manuscript count**: Filter panel updates matching manuscript count as filters are selected
- **All search modes**: Filters apply across Exact, Variants, Responsa, and Parallels search modes
- **Per-result word search exclusion**: Individually exclude manuscripts from word search results
- **Parallels filter parity**: Full filter panel on Parallels page with auto-exclude source manuscript, per-manuscript exclude buttons, and import exclusions from word search
- **Browse-to-search navigation**: Domain and author labels on Browse page link directly to a focused search (both apps)
- **Filter-aware search history**: Filters saved and restored with search history entries
- **Session persistence**: Filter state preserved across restarts

#### Dicta Translation — Multilingual Catalog Data (Phase 46)
- **~924K machine translations**: All catalog data, titles, and scholarly descriptions translated Hebrew↔English via Dicta Translation API with scholarly few-shot templates across 3 rounds
  - Libraries: 184,514 title translations (bilingual extraction + Dicta HE→EN)
  - PGP: 34,954 document description translations (EN→HE)
  - FJMS catalog fields: 3,830 translations across 6 categories (titles, authors, persons, genizah_titles)
  - FJMS free descriptions: 254,835 scholarly description translations (HE→EN)
  - FJMS running titles: ~134K translations (EN→HE)
  - FJMS full texts: ~71K scholarly description translations (EN→HE)
  - FJMS textual frames: ~84K translations (HE→EN)
  - Round 3 gap-closing: 206K additional translations for previously untranslated fields
- **Translation toggle**: Show Translations sidebar toggle enables translated text in search results, browse views, and catalog dialogs (both apps)
- **Translated/Original badge**: Clickable badge on each translated text to toggle between translated and original
- **Subtitle display**: When Hebrew title is short (<15 chars), English subtitle shown alongside (desktop)
- **Per-record RunningTitle translation**: Web catalog dialog uses per-record lookup matching desktop behavior
- **Translation QA**: Heuristic quality checks (length ratio, script mismatch, number drift, truncation), stratified audit sampling, user-facing "Report translation issue" dialog
- **Data quality fixes**: 12,827 translation rows fixed (stuttering, hallucinations, collapsed text), 34 gibberish rows deleted
- **Extraction fix**: MARC semicolon split improved — 87K records fixed, 58K Hebrew values improved
- **Dicta-powered translate buttons**: Individual translate buttons now use Dicta API instead of MyMemory

#### Source Attribution (Phase 46)
- **FJMS site user attribution**: 6,655 catalog records attributed to 168 named users via FJMS API bridge
- **Source name cleanup**: "Site User" → "FJMS Site User", Hebrew source labels, Fleischer Piyut Project (1,716 rows)
- **Handlist source fix**: 43,233 NULL SourceName records fixed with proper handlist/team labels

#### Citation Reminder
- **One-time citation popup**: Reminds users to cite MiDRASH when publishing material from the site (web + desktop, bilingual)

#### Line-Boundary Search (6.2.3)
- **Text position dropdown**: Search for words at Start of text, End of text, Line starts, or Line ends — useful for join detection between fragments
- **Per-word line constraints**: In Responsa mode, `|word` (start of line) and `word|` (end of line) with tabular builder checkboxes
- **Line-break syntax**: `word1 | word2` for cross-line search, `[|N]` for line gap notation
- **Snippet indicator**: `‖` (U+2016) shows line breaks in search snippets (both apps)

#### Cumulative Fixes (6.2.1–6.2.4)
- Search progress bar fixes (desktop): stuck "Restoring", elapsed timer, processing phase
- Pre-search domain filter parity: language-conditional display, "Other" disambiguation, sub-sub-domains
- Parallels search critical fix: stale branch + min-chunks filter bug
- Small-screen layout fix: browse button visibility, result card browse buttons
- Data quality: 1,144 shelfmark-SysID mismatches fixed in libraries.csv
- 30+ new Hebrew translation keys

---

## [6.2.4] - 2026-03-10

### Data Quality Fix: Shelfmark-SysID Mismatches
- **Fixed 1,144 records** in libraries.csv where a single NLI system number was incorrectly mapped to multiple different shelfmarks from the same series
- Primarily affects RNL (1,012), CUL (91), JTS (18), Oxford (10), BL (6)
- Added 36 new records for orphaned shelfmarks with their own correct sys_ids
- Added `scripts/fix_shelfmark_sysid_mismatch.py` for reproducible correction using NLI crossref as authoritative source
- Reported by Gregor Schwarb

---

## [6.2.3] - 2026-03-06

### Line-Break Search & Search Progress UX

#### Line-Break Search (| syntax)
- **Consecutive-line search** in Responsa mode: `|word` (line starts with), `word|` (line ends with), `word1 | word2` (cross-line)
- **Line gap notation** `[|N]` for skipping N lines between groups
- **Tabular builder** "Lines" scope with start/end-of-line modifier checkboxes
- **Multiline regex** matching via `_build_line_break_regex()` for accurate filtering and highlighting

#### Snippet Line-Break Indicator
- **`‖` (U+2016)** replaces invisible newline flattening in all search snippets (desktop + web)
- Styled gray/bold — visually distinct from query `|` and parallels segment breaks
- Dark theme support in web CSS

#### Search Progress Bar Fixes (Desktop)
- **Stuck "Restoring" message** — progress bar format now resets when a new search starts
- **Elapsed timer** updates every 1 second via independent QTimer (no longer freezes between progress callbacks)
- **"Processing" phase** — after Tantivy loop completes, progress bar shows `מעבד תוצאות...` with running clock during result rendering
- **Accurate "Search completed in"** — total time now includes post-processing and row rendering

#### Multiline Regex Re-highlighting
- ResultDialog and web expanded view now add `re.MULTILINE` flag when the highlight pattern contains `^` or `\n` anchors
- Fixes broken highlighting when opening line-break search results in detail view

#### Desktop Snippet Column
- Column is now **resizable** (Interactive mode, 600px default) — was previously locked to Stretch

#### Hebrew Translations
- Position dropdown: Text Position, Anywhere, Start/End of text, Line starts/ends
- Processing indicator, constraint tooltip, Position label (10 new keys)

---

## [6.2.2] - 2026-03-05

### Bug Fixes: Parallels Search & Small-Screen Layout

#### Parallels Search Fix
- **Critical: parallels search returning "No results"** — Two bugs combined to break all parallels searches:
  1. Server was on stale branch missing `restrict_sys_ids` parameter, causing silent `TypeError` on every search
  2. "Min. chunk matches" filter was incorrectly filtering on paragraph boundary crossings (always 0 for most input text), discarding all results even when matches existed
- **Missing `web/analytics.py`** — PostHog analytics module was never committed, causing server startup failure after deployment

#### Small-Screen Layout Fix
- **Browse button visibility** — ~40% of users (viewport height <700px) couldn't see the "Browse Full Manuscript" button, which was buried below tabs at the bottom of the right panel
- **Result card browse button** — Added green browse (📖) button directly on each search result card for one-click manuscript access
- **Right panel header actions** — Moved Browse, Find Parallels, and Advanced View buttons to the header row (always visible), removed old bottom action section
- **PGP tag viewer** — Same fix applied to PGP tag result viewer

---

## [6.2.1] - 2026-03-03

### Bug Fix: Pre-Search Domain Filter Parity

- **Language-conditional display**: Pre-search domain dropdown now shows only the current UI language (Hebrew or English), matching post-search filter behavior (web + desktop)
- **"Other" disambiguation**: Ambiguous child domains like "Other" now display with parent prefix (e.g., "Other (Bible)" / "אחר (מקרא)") in both dropdown and chip bar
- **Sub-sub-domain support**: 3rd-level domains now appear in pre-search filter tree/dropdown (previously only 2 levels shown)
- **Recursive checkbox propagation**: Desktop domain tree now propagates check/uncheck to all descendants (grandchildren), matching post-search filter
- **Chip bar display fix**: Web chip bar strips only trailing count `(N,NNN)` instead of all parenthesized text, preserving qualified domain names
- **Chip bar refresh**: Web chip bar re-renders after deferred filter init completes, showing proper display names
- **Qualified-name SQL filtering**: `get_filter_sys_ids()` now handles qualified domain names like "Other (Bible)" correctly, generating parent-scoped SQL queries

---

## [6.2.0] - 2026-03-02

### Milestone: Power-User UX — Search Workflow, Session & Notifications

Major UX overhaul driven by power-user feedback: composition search workflow improvements, session persistence, search history, desktop notifications, and Hebrew library names.

#### Composition Search UX (Phase 42)
- **Elapsed timer**: Real-time search duration display (both apps)
- **Chunk count display**: Shows number of chunks processed during composition search
- **Summary line**: Persistent search stats after completion (duration, matches, exclusions)
- **Min-chunks filter**: Filter regular search results by minimum chunk match count
- **Cancel with partial results**: Cancelling mid-search preserves results found so far, displayed in a collapsible "excluded" section with reason sub-headers
- **Printed badge**: Manuscripts identified as printed editions marked with badge in search results, composition tree, and catalog browse (both apps)
- **3-state printed filter**: All / Manuscripts only / Printed only toggle in both desktop and web, including composition tree
- **Responsive cancel**: Progress callback checked every chunk for immediate cancel response
- **Excluded items clickable**: Click excluded items in web to navigate to manuscript detail
- **Full Hebrew translations**: All Phase 42 UI strings translated

#### Session Persistence & Search History (Phase 43)
- **Session persistence service**: New `shared/session_persistence.py` module saves and restores full search state (query, mode, results, exclusions) across app restarts
- **Desktop session restore**: Automatic save on exit with restore prompt on startup; configurable via settings (Ask / Always / Never)
- **Web session persistence**: Search and parallels state preserved in browser sessionStorage
- **Search history dropdowns**: Dropdown arrow (▼) inside search bar with last 20 searches, showing mode indicator and result count (both apps, both search and composition)
- **Keyboard navigation**: Down arrow opens history from search bar, arrow keys navigate, Enter selects, Delete removes entries

#### Notification, Copy & Hebrew Names (Phase 44)
- **Desktop search notifications**: Taskbar flash when search completes while app is in background
- **Sleep prevention**: Prevents Windows sleep during long-running searches
- **Copy context menu**: Right-click to copy cell text from desktop search results table
- **Hebrew library names**: Full Hebrew names for all 81 library codes displayed when UI language is Hebrew (both apps)

#### Web & Performance
- **Home page redesign**: Compact notices section, hero banner, and 5 action cards for quick navigation
- **Sidebar RTL fix**: Correct positioning in RTL mode, improved Core Web Vitals
- **Lazy imports**: Faster desktop startup via deferred module loading
- **PostHog EU endpoint**: Fixed analytics endpoint to match account region; logged-in user identification
- **Language toggle fix**: Resolved bug when switching UI language

---

## [6.1.1] - 2026-03-01

### Performance: Catalog Browse & Domain Queries

Massive performance optimization for catalog browse domain filtering and async desktop UI.

#### Query Optimization (35s -> 0.8s)
- **100x faster domain-filtered queries**: Replaced JOIN+OR domain filter with IN(UNION) subquery for proper SQLite index utilization
- **Pre-dedup CTE pattern**: Deduplicate catalog rows in CTE then COUNT(*) instead of expensive COUNT(DISTINCT) on 685K-row table with 3x duplicates
- **Benchmarks** (Halakhic Literature, 20,951 manuscripts): Authors 30s->0.27s, Works 4.4s->0.29s

#### Async Desktop Catalog Browse
- **Non-blocking UI**: All catalog browse operations (domain/author/work select, pagination, text/date filters) now run in background QThread
- **Module-level QThread class**: Fixes PyQt6 signal delivery for locally-defined thread classes
- **Thread-safe FjmsService**: Default `thread_safe=True` for read-only sidecar connections

#### Domain Hierarchy Enhancements
- **3-level domain nesting**: Sub-sub-domains shown in web search filter and catalog browse (both apps)
- **Canonical FJMS ordering**: Domain tree sorted by Friedberg classification system order
- **Browse cache v2**: Versioned disk cache invalidates stale pre-nesting caches

---

## [6.1.0] - 2026-02-27

### Milestone: Catalog Browse & Navigation (Phase 41)

Added faceted catalog browsing by domain, author, and work in both apps, with free-text filtering, FIST v5.0.0 enrichment, and cross-links between browse pages.

#### Catalog Browse Pages (Plans 41-01 through 41-04)
- **Web catalog browse page**: New `/catalog-browse` page with domain hierarchy tree, author/work search dropdowns, combined filtering, pagination, and deep linking via URL params
- **Desktop catalog browse tab**: New "Browse by Identification" tab with matching domain tree, author/work filtering, and result navigation
- **Cross-links**: Domain and author labels on manuscript browse pages are clickable links to catalog browse filtered by that value (both apps)
- **Sidebar/tab navigation**: "Browse by Shelfmark" and "Browse by Identification" entries in both apps

#### Free Text Filter
- **FTS5-based catalog search**: ALL/ANY/NOT modes for filtering catalog browse results by text across titles, descriptions, and identifications
- **Hybrid FTS5 + domain LIKE**: Domain name searches (e.g., "פילוסופיה") return results via UNION query combining FTS5 catalog fields with domain table LIKE search
- **Filter chips**: Color-coded removable chips (blue=ALL, green=ANY, red=NOT) with button-style removal to avoid NiceGUI slot issues
- **sessionStorage persistence**: Text filter state preserved across page navigation

#### FIST v5.0.0 Enrichment
- **3 new tables**: genizah_persons (2,286 historical people), genizah_titles (775 works), code_values (3,440 decoded field values)
- **20 new catalog columns**: GenizahTitleId, Author, CopyToDate, CreationTypeCode, Comment, Colophon, CopyName, and more
- **Structured author/work browsing**: 801 authors (was 204) and 663 works via FK path through genizah_persons/genizah_titles
- **Graceful fallback**: `_has_persons_titles` flag enables v4 sidecar compatibility

#### Translations & Tests
- **15 new Hebrew translations** for catalog browse UI strings
- **Test updates**: Browse author/work tests updated for new dict key format; 72 FJMS tests passing

---

## [6.0.0] - 2026-02-22

### Milestone: Local Data Architecture

Migrated all PGP reference data from Supabase to a local SQLite sidecar, added FJMS catalog descriptions as a scholarly resource, stabilized the app with crash fixes and pagination, and optimized performance across both apps.

#### PGP Sidecar Migration (Phase 35-36)
- **pgp.db sidecar**: All PGP data (35,839 documents, 9,364 sources, 22,757 footnotes, 36,155 fragments) exported to local SQLite (147MB)
- **PgpService rewrite**: `document_service.py` reads from SQLite instead of Supabase -- sub-millisecond local queries replace 50-200ms API calls
- **JSON preservation**: Tags and sections stored as TEXT JSON, queried with `json_each()` for full parity with Supabase GIN queries
- **Both apps updated**: Web shim and desktop imports all point to local pgp.db
- **Zero Supabase dependency**: All PGP reference data served locally; Supabase retained only for community features (auth, corrections, lists)

#### FJMS Catalog Descriptions (Phase 37)
- **Enriched export**: fjms_enrichment.db extended to v3.0.0 with 4 new tables (running_titles, size_field, catalog_free_desc, genizah_titles) adding ~1.7M rows
- **Catalog dialog**: Dedicated 5-section scholarly layout (content identification, physical metadata, running titles, free descriptions, genizah titles) in both apps
- **Source attribution**: Each catalog entry shows which scholarly catalog or scholar produced the description
- **Batch catalog counts**: Search results show catalog source count on button labels for quick reference

#### Distribution & Offline (Phase 38)
- **Desktop bundling**: pgp.db included in installer via `build_app.bat` -- no separate download needed
- **LOCALAPPDATA resolution**: User-updated sidecars stored in AppData, separate from bundled install directory
- **Sidecar update mechanism**: SidecarUpdateThread checks for newer sidecar versions at startup with sequential download queue
- **About screen**: Data Sources section showing versions of all 3 sidecars (pgp.db, fjms_enrichment.db, nli_crossref.db)
- **Offline verification**: 12 tests confirming zero network dependency for all 3 sidecar services
- **Desktop offline PGP browsing**: Full metadata, transcriptions, footnotes, and fragment navigation without internet (images excluded)

#### Bug Fixing & Cleanup (Phase 39)
- **Desktop crash fixes**: `sip.isdeleted()` guards on all Qt lifecycle crash sites (set_status_message, update_text_pos) -- eliminates all known crash-on-navigate bugs
- **Paginated search results**: PAGE_SIZE=50 replaces the 200-result hard cap; prev/next navigation with scroll-to-top; storage persistence cap raised to 1000
- **PostHog analytics**: Integrated alongside Google Analytics (env-var gated via `POSTHOG_API_KEY`); maskAllInputs + identified_only for privacy
- **Domain filter performance**: Cached domain hierarchy eliminates ~5s lag when opening domain filter dialog (double-checked locking for thread safety)
- **E2E test infrastructure**: Custom NiceGUI Screen fixture, app-level E2E via runpy, selenium as dev dependency with skip logic for CI
- **CSS extraction**: Inline styles moved to static CSS file for maintainability
- **Lazy login dialog**: Login dialog created on-demand instead of at page load, improving navigation speed
- **Parallel page queries**: asyncio.gather + run.io_bound for search enrichment, batch FJMS for browse, async discoveries

#### Performance Optimization (Phase 40)
- **Parallel NLI fetch**: ThreadPoolExecutor for concurrent MARC + IIIF manifest calls, halving browse metadata load time
- **Desktop async domain enrichment**: DomainEnrichmentWorker thread loads domain data after results display (~200ms); catalog detail fetched lazily on click
- **Browse crossref parallelization**: 3 independent crossref queries via ThreadPoolExecutor (catalog entry, collection/storage, physical metadata)
- **FL ID O(1) index**: Dictionary-based lookup for browse-by-FL-ID replacing linear scan (with fallback during startup window)
- **Variant cache unification**: Pre-compute variants at REGEX_VARIANTS_LIMIT (8000) before per-term loops; Tantivy slices from superset cache

#### Pre-Ship Cleanup
- **IsNotGenizah badge removed**: Orange "Not Genizah" badge removed from both apps' browse pages and Reading Desk (data preserved in nli_crossref.db)

---

## [5.9.0] - 2026-02-16

### Milestone: Multi-Source Image & Metadata Integration

Import of NLI crossreference data (815K image-level records) and Cambridge IIIF manifests (141K URLs) into a second SQLite sidecar, plus Manchester LUNA and JTS/Princeton Figgy integration, enabling direct image access across 75+ libraries, physical metadata, scholarly bibliography, and library-specific viewer links in both apps.

#### Data Infrastructure (Phase 29)
- **NLI crossref sidecar** (`nli_crossref.db`): 815K image-level records from NLI crossreference CSV with 253K distinct AlmaIds, plus 141K Cambridge IIIF manifest URLs
- **Shared NliCrossrefService**: 16 query methods (images, folio labels, physical metadata, relationships, library URLs, Manchester/JTS lookups)
- **Thread-safe SQLite**: Read-only URI mode with thread safety for NiceGUI concurrent requests
- **Graceful degradation**: All methods return empty results when sidecar is missing

#### Direct Image Access (Phase 30)
- **Cambridge local resolution**: Cambridge manuscripts load images via pre-stored CUDL IIIF manifest URLs, bypassing NLI entirely (141K records)
- **Fallback chain preserved**: Memory cache -> sidecar -> network for all image resolution

#### Image Navigation & Indicators (Phase 31)
- **Folio navigation**: Page-level navigation using scholarly notation (1r, 1v, 2r, etc.) in both apps
- **Source availability indicators**: Colored chips showing which digital image sources exist (NLI, Cambridge, Manchester, JTS)
- **Source switching**: Toggle between NLI and external image sources in the browse viewer
- **Cambridge IIIF proxy**: Server-side proxy endpoint for Cambridge image serving

#### Metadata Display (Phase 32)
- **Physical metadata**: Material type (paper/parchment) and folio count on browse page (both apps)
- **NLI catalog link (KTIV)**: Clickable link to NLI KTIV viewer for manuscripts
- **Library collection links**: Clickable links to holding library digital collections (CUDL, Manchester LUNA, JTS DPUL, BL, Oxford)
- **Hebrew translations**: Material types and metadata labels translated for Hebrew UI

#### Metadata Enrichment (Phase 33)
- **FIST bibliography**: 542K denormalized bibliography references with scholar attribution, mention type badges, and transcription/translation availability (both apps)
- **Catalog cross-references**: 64K entries across 80 scholarly catalogs displayed as structured references (both apps)
- **Neubauer-Cowley catalog numbers**: 27K Oxford entries displayed alongside shelfmark
- **IsNotGenizah badge**: Orange visual badge for 304K flagged items in corpus
- **Collection & storage**: NLI collection names and physical storage references (box/volume/folio)
- **Scholarly source names**: FJMS source attributions with generic name filtering
- **FJMS sidecar extended**: fjms_enrichment.db upgraded to v2.0.0 with bibliography, catalog_refs, and reference tables

#### Library IIIF Integration (Phase 34)
- **Manchester LUNA**: 27,940 LUNA IDs pre-imported via API pagination; detail page links (not search); IIIF manifests as image source with pink source chip
- **JTS/Princeton Figgy**: 453 validated ARK IDs + Figgy manifest URLs via DPUL catalog search; catalog page links; IIIF manifests as image source with orange source chip
- **BL deferred**: British Library links use searcharchives.bl.uk (BL IIIF API still down from cyber attack)

---

## [5.8.0] - 2026-02-15

### Milestone: FJMS Integration

Integration of scholarly metadata from the Fragment of the Jewish Manuscript Studies (FJMS) database into both web and desktop apps via a SQLite sidecar database. Adds subject-based filtering, scientific join groups, and catalog enrichment for manuscripts.

#### Data Infrastructure (Phase 25)
- **SQLite sidecar database** (`fjms_enrichment.db`): 762K rows exported from 13GB FIST.db with domains, joins, catalog tables, and FTS5 full-text index
- **Shared FjmsService**: 8 query methods accessible from both web and desktop apps
- **Thread-safe SQLite**: Read-only URI mode with thread safety for NiceGUI concurrent requests
- **Graceful degradation**: All methods return empty results when sidecar is missing

#### Scientific Joins (Phase 26)
- **FJMS join groups** in Related Fragments panel: scholarly join identification with scholar name and join type (Physical Join, Codex Join, etc.)
- **Three-source merge**: FJMS joins merged as third source after user and PGP joins with full deduplication
- **Purple badge** for FJMS source visual distinction (user=none, PGP=blue, FJMS=purple)
- **Navigation**: Click join group members to navigate to that fragment in both apps

#### Domain Classifications (Phase 27)
- **Domain badges** on browse page: clickable subject classification links (e.g., Piyyut, Bible, Letters)
- **Domain search filtering**: hierarchical multi-select with type-ahead, OR logic for multi-domain queries
- **Standalone domain browsing**: browse manuscripts by domain without text query (capped at 500)
- **Post-search dynamic filtering**: Domains button with checkbox tree dialog for excluding domains from results
- **Domain indicators** on result cards: primary domain + "+N more" pattern with tooltip

#### Catalog Enrichment (Phase 28)
- **FJMS catalog titles**: Hebrew and English titles with language-aware display
- **Author information**: Scholar/author attribution from FJMS catalog records
- **Copy date and place**: Manuscript dating and origin information with sentinel value filtering
- **Content identifications**: Parsed TextualFrame entries with category and source attribution
- **FJMS description alongside PGP**: Separate sections, not replacing existing PGP metadata
- **Cross-app parity**: All catalog fields display in both web and desktop (Browse tab + ResultDialog)

---

## [5.7.2] - 2026-02-11

### Cleanup & Polish

- Removed deprecated AI Search feature code (AIManager, AIDialog, AIWorkerThread, Settings panel, button, help references)
- Removed `google-genai` dependency

### Search Normalization

- Combining diacritical marks (U+0300-U+036F) stripped from search queries at query time
- Hebrew geresh (U+05F3) and gershayim (U+05F4) stripped from search queries
- ASCII apostrophe and curly quote variants normalized in search
- Mark-tolerant search highlighting (matches through interleaved combining marks in source text)
- All existing search modes unaffected (normalization globally safe)
- Regex mode exempt from normalization (users control their own patterns)

### Test Suite

- Fixed 17 pre-existing test failures (export filenames, boundary search, responsa integration, shelfmark normalization)
- Deleted 3 obsolete backend test files (test_api_flow.py, test_corrections_api.py, test_corrections_integration.py)
- Full green suite: 447 tests passing, 0 failures

### PGP Transcription Sections

- Structural HTML section parser for PGP transcriptions from pgp-text repository
- Canvas-based parsing (h3 inside data-canvas divs) replaces fragile regex-only approach
- New `sections` JSONB column on document_sources with `source_language` and `source_direction`
- Recto/verso/margin sections correctly display alongside manuscript images in both apps
- Language-based translation ordering (Hebrew first, English second) consistent across both apps
- Import script: clones pgp-text repo, parses HTML, populates structured section data

## [5.7.0] - 2026-02-10

### Milestone: Responsa Search

Advanced search capabilities inspired by the Responsa Project, available in both web and desktop apps. Researchers can now use Responsa-Project style syntax, grammatical expansion, Judeo-Arabic support, and a visual query builder to search the Genizah corpus with fine-grained control.

#### Responsa Core Engine (Phase 14)
- **Responsa syntax**: `#word` (prefix expansion), `word#` (suffix expansion), `#word#` (both), `*word`/`word*` (wildcards), `%word` (plene/defective variants), `(a/b)` (OR alternatives), `[N]` (gap notation)
- **Hebrew grammatical expansion**: 24 prefix forms (single + compound: ו,ה,ב,כ,ל,מ,ש + combinations) and 25 suffix forms per word
- **Judeo-Arabic article expansion**: 8 forms per word using simplified al- model (no sun letter assimilation)
- **Plene/defective variants**: Bidirectional ו/י insertion/removal for spelling variations
- **Sofit letter conversion**: Final forms (ם,ן,ץ,ף,ך) normalized before suffix expansion
- **Combinatorial explosion guard**: MAX_EXPANDED_TERMS=500 with 6-step downgrade cascade (variants basic -> off -> JA off -> plene off -> suffixes off -> prefixes off -> error)
- All Responsa logic in shared `genizah_core.py` -- no search logic in UI code

#### Search UI (Phase 15)
- **Responsa as dropdown mode**: "Responsa (R)" appears as a first-class option in the Mode dropdown/combo in both apps
- **Sub-option checkboxes**: Variants, Judeo-Arabic, Flexible Spacing, Bidirectional Gap -- visible only when Responsa mode is selected
- **Syntax legend**: Quick reference for Responsa operators shown below the search field
- **Keyboard shortcut**: Type `R ` (R+Space) to switch to Responsa mode
- **URL state persistence**: Web URLs include `?mode=responsa&variants=1&ja=1&flex_spaces=1&bidirectional=1`
- **PGP Tags interaction**: Responsa sub-options hidden when PGP Tags mode is active
- **Desktop defaults**: Checkboxes reset to defaults on each app startup

#### Tabular Query Builder (Phase 16)
- **Visual query construction**: Dialog with 2-4 component columns for building complex Responsa queries without memorizing syntax
- **Per-word modifiers**: Checkboxes for prefix (#), suffix (#), wildcard (*), plene (%), and negation per word
- **Distance control**: Per-pair gap spinners with [N] notation between components
- **Live preview**: Real-time syntax preview updates as you modify the query
- **One-way sync**: "Apply" inserts generated syntax into the search field and triggers search
- **Web**: Dialog opened via "Query Builder" button in Responsa sub-row
- **Desktop**: QDialog opened via "Query Builder" button with full RTL layout

#### Integration Testing & Polish (Phase 17)
- **221 automated Responsa tests**: 68 core engine + 31 parity + 20 edge cases + 30 regression + 5 performance + 36 additional
- **Cross-app parity**: All 16 checkbox combinations verified to produce identical results
- **Non-Responsa regression**: 30 tests confirming all existing search modes (Exact, Variants, Fuzzy, Regex, Shelfmark, Title, PGP Tags) work unchanged
- **Bug fixes**: R+Space shortcut sub-options visibility, WebSocket crash on large results (200 cap), sofit-aware wildcard regex, explosion guard cascade expanded from 3 to 6 steps, ValueError surfaced to user via toast notification, desktop tabular builder unconditional RTL

---

## [5.6.1] - 2026-02-10

### Bug Fixes — User Authentication & Corrections

#### Web: Singleton Supabase Client Fix
- **Critical fix**: Web app used a shared singleton Supabase client for all users. When multiple users were logged in, the auth session belonged to whoever signed in last — causing RLS policy failures for all other users' write operations (corrections, comments, discoveries, lists, etc.)
- Added `get_user_client()` — creates a per-user Supabase client from session tokens stored in NiceGUI's per-user storage
- All 28+ write functions now use the per-user client; read-only functions remain on the efficient singleton
- Session tokens are stored during email login and Google OAuth, and refreshed automatically when expired

#### Web: Admin Panel Corrections
- Fixed admin panel not showing pending corrections — the PostgREST join between `corrections` and `profiles` failed silently because there is no direct FK between the tables (both reference `auth.users` independently). Replaced with separate queries.
- Fixed admin unable to approve/reject corrections — added RLS policies allowing admins to update/delete corrections, comments, discoveries, and fragment joins from any user
- Admin write operations now use per-user client instead of singleton

#### Web: Correction Submission UX
- Fixed "parent element deleted" error after submitting a correction — the async handler's UI slot was destroyed by `update_content()` during the submit flow. Removed all `update_content()` calls from the async handler; all feedback now uses slot-independent `ui.notify()`
- Added success notification when correction is submitted
- RLS errors (42501) now show "Session expired — please log out and log back in" instead of raw Supabase error

#### Web: Profile Password Change
- Fixed password change using singleton client — could silently fail or change wrong user's password. Now uses per-user client.

#### Desktop: Login Error Messages
- Improved error messages for common login failures:
  - "Invalid email or password" for wrong credentials
  - "Email not confirmed" for unverified accounts
  - "No account found" for non-existent emails
  - Network-specific errors for connection issues

---

## [5.6.0] - 2026-02-09

### Milestone: Desktop Parity & PGP Integration

Full integration of Princeton Geniza Project (PGP) data across both web and desktop apps.

#### PGP Data (Phases 8-9)
- Imported 35,839 PGP documents with full metadata, 9,364 sources, 22,757 footnotes, 36,155 fragment links
- Shared document_service.py for Supabase access from both apps

#### Desktop PGP Core (Phase 10)
- PGP transcriptions and metadata in desktop Browse and Result dialogs
- Per-source directionality (editions RTL, English translations LTR)

#### Virtual Reading Desk (Phase 11)
- Multi-manuscript synchronized viewer in both web and desktop apps
- Stacked images + stacked texts with fragment-level sync scrolling
- Per-fragment version selector, zoom/rotate controls, lazy loading

#### Desktop PGP Discovery (Phase 12)
- PGP badges and tag display in search results
- PGP column sorting (click to show PGP-linked manuscripts first)
- PGP joins visible in desktop JoinsDialog
- Tag-based search as a search mode in both apps

#### PGP Tag Search UX
- "PGP Tags" as a search mode in the Mode dropdown (both apps)
- Desktop: hides query row, shows tag combo in Mode row
- Web: tag select replaces query input when PGP Tags mode selected
- Tag click navigation from result dialogs and browse pages
- 251 PGP tags with curated Hebrew translations and category grouping
- 16 categories: Document Types, Law & Society, Medicine, Trade, India Book, etc.
- Language-aware display: Hebrew UI shows "עברית (English)", English UI shows English only
- Category headers as visual separators in tag dropdowns

#### Phase 13 Deferred
- Transcription Search (full-text search in PGP transcriptions) was implemented but reverted
- Reason: Tantivy index build too slow for desktop distribution
- Will revisit with server-side index architecture in a future milestone
- Full documentation preserved in docs/archive/PHASE_13_TRANSCRIPTION_SEARCH_DEFERRED.md

---

## [5.5.0] - 2026-02-04

### New Feature: In-App Software Updates

The desktop application can now download and install updates without leaving the app.

#### How It Works
1. When a new version is available, a notification bar appears at the top
2. Click "Update Now" to start the update process
3. A progress dialog shows download progress
4. After download, the installer runs automatically in silent mode
5. The app restarts with the new version

#### Technical Details
- Downloads the official installer from GitHub Releases
- Uses Inno Setup's silent mode (`/VERYSILENT /RESTARTAPPLICATIONS`)
- Installer automatically closes the running app, updates files, and restarts
- UAC prompt will appear (same as manual install) since app is in Program Files
- Falls back to opening browser if installer not found in release

#### Files Changed
- `gui_threads.py` - New `UpdateDownloaderThread` class for downloading with progress
- `genizah_app.py` - New `UpdateProgressDialog` for update UI
- `CompileScriptGenizah.iss` - Added `CloseApplications` and `RestartApplications` settings

---

## [5.4.1] - 2026-02-03

### Enhancement: "Remember Me" Login Feature

Both the desktop and web applications now support saving login credentials.

#### Desktop Application
- **"Remember me" checkbox**: New checkbox in the login dialog to opt-in to credential saving
- **Secure storage**: Password stored in Windows Credential Manager (via `keyring` library) - not in plain text files
- **Persistent across updates**: Credentials survive software updates since they're stored in user profile, not application folder
- **Easy to disable**: Uncheck "Remember me" to clear saved credentials

#### Web Application
- **"Remember me" checkbox**: New checkbox in the login dialog
- **Email remembered**: Email address saved in browser localStorage for convenience
- **Session persistence**: Login session already persists via Supabase cookies

---

## [5.4.0] - 2026-02-03

### New Feature: Library/Holding Institution Display

Every manuscript record now shows which library or collection holds the original document.

- **Coverage:** 99.99% of ~217,000 records have library codes assigned (only 14 records with missing source data)
- **Libraries identified:** 70+ institutions including Cambridge (CUL), JTS, National Library of Russia, Bodleian (Oxford), Manchester, British Library, Alliance Israélite, Library of Geneva, Senckenberg (Frankfurt), Schocken Institute, and many more

#### Web Application
- Library badge with code (e.g., "CUL") displayed in search results with full name tooltip
- Library field in Advanced View metadata cards
- Library field in browse page metadata panel
- Library column in all Excel exports (Search, Lists, Parallels)

#### Desktop Application
- New "Library" column in search results table
- Filterable/sortable like other columns
- Library column in Excel/Word exports

#### Technical Details
- New `library_code` column in `libraries.csv`
- New functions: `LIBRARY_CODES` constant, `get_library_display()`, `get_library_for_id()`
- Backward compatible with old CSV files (gracefully handles missing column)

### Enhancement: Nikud (Vowel Mark) Removal in Parallels Search

Parallels search now automatically strips Hebrew vowel marks (nikud) and cantillation marks from text before matching. This ensures consistent results whether the input text contains nikud or not.

- Affects both Lab Mode and Standard parallels search
- Also strips nikud from filter/exclude text for consistent filtering
- New function: `strip_nikud()` in `genizah_core.py`

### Enhancement: Advanced View Dialog Improvements

The Advanced View dialog (opened from search results) has been significantly enhanced:

#### Navigation & Viewing
- **Fixed navigation bug**: Results now navigate in-place without closing/reopening the dialog
- **Page navigation**: Browse pages within a manuscript using prev/next buttons
- **IIIF image viewer**: Side-by-side image panel with zoom, rotate, and pan controls
- **Fullscreen mode**: Distraction-free view with compact navigation bar
- **Image toggle**: Show/hide image panel as needed

#### Inline Editing
- Edit text directly in the Advanced View (same as Browse page)
- Save drafts, submit for review, or publish immediately (for editors/admins)
- Visual feedback: orange border for unsaved changes, green for saved
- Notes field for correction comments

#### Bug Fixes
- Fixed "Unknown" author display in version selector (now joins profiles table)
- Fixed script tag error in edit dialog (NiceGUI compatibility)

### Files Changed
- `genizah_core.py` - Core library functions, CSV loading, nikud removal
- `genizah_app.py` - Desktop table columns
- `web/services.py` - Data classes and page retrieval
- `web/pages/search.py` - Library badge display, Advanced View dialog enhancements
- `web/pages/browse.py` - Metadata panel
- `web/components/text_editor.py` - Fixed script tag in HTML
- `web/supabase_client.py` - Added profiles join to get_corrections()
- `web/export_service.py` - Export functions
- `libraries.csv` - Added library_code column

---

## [5.3.1] - 2026-02-03

### Bug Fixes

- **RTL navigation arrows:** Fixed all directional icons (arrows, chevrons, skip buttons) that were reversed in Hebrew UI mode. Icons now correctly flip direction based on language setting.
- **Removed directional icons from action buttons:** Removed `send` arrow icons from Submit/Share/Reply buttons and `arrow_forward` from Go button, as these looked incorrect in RTL mode.
- **Missing title metadata in search results:** Fixed bug where title and other metadata wasn't displayed in search results. The `get_display_data()` method now uses proper fallback logic (CSV bank → NLI cache) matching the browse page behavior.
- **Search panel auto-collapse:** Fixed scroll-based auto-collapse that wasn't working. Added proper class targeting for the results scroll area and improved JavaScript detection.
- **Search panel collapse/expand visibility:** Fixed panels not showing/hiding properly by using explicit styles with `!important` flags.
- **Advanced Options inside search panel:** Moved the Advanced Options expansion inside the collapsible search panel so it hides when the search bar collapses.
- **Search results layout overflow:** Fixed text getting cut off when zooming or resizing window. Removed `max-width` restrictions and added proper flex wrapping and word-wrap styles.
- **Removed Edit/Comment buttons from result cards:** Cleaned up search result cards by removing Edit and Send Comment buttons (still available in the detailed viewer).

### Enhancements

- **Full Text pane highlighting:** Added search term highlighting to the Full Text tab in search results, matching the highlighting in the Match pane.

### Files Changed

- `genizah_core.py` - Fixed `get_display_data()` metadata fallback
- `web/pages/search.py` - Search panel collapse, Advanced Options placement, result card layout, Full Text highlighting
- `browse.py` - Page/shelfmark navigation, Go button, Back buttons, Submit buttons
- `document.py` - Back button, page navigation
- `home.py` - Start Search, Find Parallels, Browse, View All buttons
- `discoveries.py` - Back buttons, Reply/Share buttons
- `comment_dialog.py` - Back button, Submit button
- `joins_panel.py` - Navigation indicator, Back button
- `text_editor.py` - Submit Correction button

---

## [5.3.0] - 2026-02-02

### New Feature: Cross-Paragraph Search

A new parallel search mode that finds manuscripts with text spanning paragraph boundaries, now available on **both Web and Desktop**.

- **Why it's useful:** Text within paragraphs often contains citations (Mishnah, Talmud, known phrases). Text that crosses paragraph boundaries is unlikely to be a citation, effectively filtering out noise.

- **Three search modes:**
  - **Full search** - All results (default)
  - **Cross-paragraph only** - Only matches that span paragraph breaks
  - **Combined** - All results, with boundary-crossing matches boosted

- **Customizable delimiters:** Line break, blank line (paragraph), period, colon

- **Visual indicators:**
  - Web: Amber "Cross-paragraph" badge; red `|` at boundary points in matched text
  - Desktop: 🔗 emoji prefix on scores; tooltips showing match count

- **Advanced settings:** Configurable boost factor (1.0-3.0), minimum boundary matches filter, minimum delimiter distance

- **Real-time feedback:** Desktop shows boundary count and crossing chunks before search

### Bug Fixes

- **Duplicate results fix:** Fixed bug where same manuscript appeared multiple times in Standard search when found by overlapping chunks routed to different filter maps
- **Boundary detection:** Improved to require words on BOTH sides of the boundary (not just touching)
- **Desktop boundary stats:** Fixed silent exception handling, now logs errors properly
- **Desktop translation:** Fixed fragmented translation string for cross-paragraph tooltips
- **Anonymous display bug:** Fixed discoveries showing as "Anonymous" even when user didn't check anonymous - now fetches profile data properly
- **Dialog Esc key:** Fixed Share Discovery dialog flickering when pressing Esc (removed 'persistent' prop)
- **Simplified Share Discovery:** Removed superfluous "Related manuscripts" section from dialog
- **Database constraint:** Updated discoveries type constraint to include 'identification' and 'note' types

### Technical Changes

- `CompositionThread` and `LabCompositionThread` now accept boundary parameters
- `LabSettings` stores boundary preferences (mode, delimiter, boost, min matches, min distance)
- Added temporary storage fallback for settings when `lab_engine` not initialized

### Documentation

- Updated help page with cross-paragraph search documentation (English and Hebrew)
- Updated BOUNDARY_SEARCH_SPEC.md with completed desktop implementation details

---

## [5.2.0] - 2026-02-01

### Documentation

- **Help Center rewrite:** Comprehensive bilingual help page covering Search, Parallels, Browse, Lists, and Export features
- **File index:** New `docs/FILE_INDEX.md` with comprehensive listing of all project files

### Codebase Cleanup

- **Root directory cleanup:** Removed unused directories (`backend/`, `backend_legacy/`, `frontend_web/`, `build/`, `Reports/`, `Results/`)
- **Scripts organization:** Moved utility scripts to `scripts/` folder (cleanup, verify, debug scripts)
- **Branch cleanup:** Deleted 25 stale/merged git branches

### UX Improvements

- **Search spinners:** More prominent animated spinners (bars instead of dots, larger size, pulsing text)
- **Parallels search feedback:** Spinner and status now visible in control panel without scrolling
- **Stop button:** Added to regular search (swaps with search button during search), shows partial results when stopped
- **Filter sources badge:** Shows count of enabled filter sources on the expansion header
- **Filter tooltip:** Explains filter feature in both English and Hebrew

### Header Branding

- **Dicta branding:** Header now shows "Dicta Genizah Search" with Hebrew subtitle "אתר הגניזה מבית דיקטה"
- **Mobile optimization:** Header hides on scroll down, reveals on scroll up (mobile only)
- **Responsive logo:** Text hidden on small screens, only icon shows

### Backend Migration: Supabase

- **Complete Supabase migration:** Replaced FastAPI backend with direct Supabase integration
- All authentication now handled by Supabase Auth
- User lists, corrections, and comments stored in Supabase
- Built-in rate limiting and security features

### Authentication Fixes

- **OAuth flow:** Fixed Google OAuth to use Supabase's `sign_in_with_oauth` method with proper state parameter
- **Session handling:** Implicit flow tokens properly extracted from URL hash on callback
- **Forgot password (desktop):** Added password reset link to desktop app login dialog for OAuth users
- **OAuth user guidance:** Web Google signup now shows note about setting password for desktop app login

### Row Level Security (RLS) Fixes

- **RLS policies:** Fixed all INSERT/UPDATE/DELETE policies to use `authenticated` role instead of `public`
- **Column naming:** Updated queries to use correct column names (`author_id` for comments/corrections, `user_id` for others)
- **Profile joins removed:** Removed `profiles` table joins from queries that failed without FK relationships
- **SQL script:** Added `scripts/fix_rls_policies.sql` for bulk RLS policy updates

### Community Feed & Comments

- **Feed loading:** Fixed `get_feed_items` to properly load discoveries, corrections, comments, and joins
- **Comments display:** Fixed comments to appear on browse pages (removed failing profiles join)
- **Profile page:** Fixed to load data from profile storage instead of auth user

### Lists & Projects Management

- **Management mode toggle:** New "Manage lists" button reveals edit controls
- **Icon-based actions:** Replaced dropdown menus with direct action buttons (rename, move to project, delete)
- **Improved UI:** Cleaner interface with actions hidden by default
- **Auto-sync:** Lists automatically sync between devices for logged-in users
- **Soft delete support:** Lists can be recovered after deletion

### Bug Fixes

- **Register button:** Fixed bug where clicking "Register" opened login dialog instead of register
- **Dependencies:** Added missing `gotrue` and `python-dotenv` to requirements.txt

### Documentation

- **English translation:** PRE_LAUNCH_CHECKLIST.md translated from Hebrew to English
- **Documentation reorganization:** New `docs/` structure with guides, plans, and specs

---

## [5.1.0] - 2026-01-27

### Web Platform: Dicta Genizah Search (אתר הגניזה של דיקטה)

The web platform has been rebranded to **"Dicta Genizah Search"** (אתר הגניזה של דיקטה), reflecting our partnership with DICTA. The desktop application remains "Genizah Search Pro".

### Accessibility Compliance (WCAG 2.0 / IS 5568)

- Full compliance with Israeli Standard 5568 and WCAG 2.0 AA accessibility guidelines
- Improved Hebrew RTL layout and text alignment
- Semantic headings with proper sizing
- Enhanced keyboard navigation support

### New Features

- **Automatic Text Source Filtering:** Intelligent filtering based on Sefaria text database
- **Enhanced Variant Search:** Improved letter variation handling (multi-character, 2-to-1 letters)
- **Fullscreen Edit Mode:** Image controls with splitter for side-by-side viewing
- **Fragment Joins System:** Connect related fragments via Discovery Center
- **Exclude Words UI:** Exclude specific words from search results
- **Citation Footer:** Dismissible footer with publishing guidelines

### Browse & Viewer Improvements

- Side-by-side layout for browse page
- Image drag, rotate, and wheel zoom in manuscript viewer
- Image credits/attribution for NLI and Oxford sources
- Title truncation with tooltips for long titles

### UI/UX Improvements

- Desktop app download page with website integration
- SEO metadata for social sharing
- Dark mode fixes across multiple pages
- Dismissible transcription disclaimer banner
- Creator credit in sidebar footer

### Technical Improvements

- Migrated to google-genai SDK (gemini-3-flash-preview)
- SSL certificate verification for all HTTPS requests
- Build optimizations for faster packaging
- Antivirus false positive documentation
- Server-side index building support

### Bug Fixes

- Fixed NLI and Oxford image loading issues
- Fixed version comparison for different component lengths
- Fixed RTL layout overlap and alignment issues
- Fixed theme toggle functionality
- Fixed fullscreen edit image loading

---

## [5.0.0] - 2026-01-19

### Major Release: Web Platform & Community Features

Version 5.0 marks the launch of the **Genizah Search Pro Web Platform** and introduces comprehensive **Community Features**, transforming the software into a collaborative research environment.

---

### Web Platform

- **Public Web Application:** Full-featured web interface accessible from any browser at [genizahsearch.com](https://genizahsearch.com)
- **Mobile Responsive Design:** Optimized experience for tablets and phones with adaptive layouts
- **User Authentication:** Registration, login, and profile management
- **Offline Mode:** Community features work offline and sync when reconnected

### Community Features

- **Discovery Center:** Share and explore research discoveries with the community
  - Voting system for discoveries
  - Pin important discoveries
  - Mark discoveries as answered/resolved
  - Multiple shelfmark references per discovery
  - Document lookup from discovery dialog
- **Comments System:** Add comments to manuscripts with page-specific references
  - Public and private comments
  - Draft support for work-in-progress notes
- **Corrections & Contributions:** Submit corrections to transcriptions
  - Review workflow for submitted corrections
  - Track your contributions in "My Edits & Comments" page

### Admin Features

- User management panel with role assignment
- Corrections review system for approving/rejecting submissions
- Profile editing capabilities for users

### Desktop App Integration

- New dialogs for comments and text editing
- Improved synchronization between desktop and web data
- Consistent page number handling across platforms
- Full offline mode for community tab

### Stability & Performance

- Fixed infinite timer issues causing connection problems
- Improved CSS performance for faster page loads
- Better error handling for offline scenarios
- Disabled reload mode for improved stability

---

## [4.1.1] - 2026-01-12

### Fixes
- Corrected star icon alignment in search results
- Fixed list preview image loading

---

## [4.1.0] - 2026-01

### Personal Lists Management
- New tab for creating and organizing personal manuscript lists
- Browse by list: side panel in Browse tab for navigating custom lists
- List filtering: filter search results based on personal lists

### Interface Refinements
- Compact context view at the bottom of the interface
- Reports saved to user's Documents directory
- Resolved duplicate search results issue

---

## [4.0.0] - 2025

### Major Update: From Search Engine to Research Suite

### Integrated Visual Analysis (IIIF)
- In-app viewer for high-resolution manuscript images
- Direct integration with National Library of Israel and Cambridge University Library
- Sequential page and manuscript navigation
- Built-in zoom and rotation controls

### Oxford Bodleian Integration
- Full support for Oxford Bodleian Library manuscripts
- Neubauer catalog integration
- Part-based and folio-based navigation

### Lab Mode (Experimental)
- Parallel detection algorithm based on Shmidman, Koppel, and Porat (2016)
- Rare letter encoding for spelling variation tolerance

### Additional Features
- Cross-page search
- Enhanced export (Excel, CSV, DOCX)
- Find in text with highlighting
- Composition search for parallel detection

---

## [3.6.0] and earlier

See previous release notes for historical changes.
