---
phase: 97
reviewers: [codex]
reviewed_at: 2026-05-25T07:05:04Z
plans_reviewed: [97-01-PLAN.md, 97-02-PLAN.md, 97-03-PLAN.md, 97-04-PLAN.md, 97-05-PLAN.md, 97-06-PLAN.md]
codex_model: default
codex_version: codex-cli 0.130.0
verified_against_codebase: true
---

# Cross-AI Plan Review — Phase 97

## Codex Review

**Summary**

The wave order is broadly right: recovery before capacity lift, safety before new formats, and invariant guards at the end. But the execution plans have several high-risk plan↔code drifts. The largest gaps are in Wave A: migration assumes `user_version=1` when current DBs are `0`, cached text helpers are added but never written during indexing, atomic rebuild is not actually wired into startup, and the swap plan closes SearchEngine readers but leaves `LocalIndexer`'s own writer/index handles open. **I would not execute these plans as-is.**

**Strengths**

- `97-01`, objective: correct sequencing. Recovery foundation lands before `97-04` lifts the 5K/2GB ceiling.
- `97-02`, must-haves: correctly encodes RESEARCH Issue #1: byte/count/time commit triggers only, no `writer.get_memory_usage()`.
- `97-03`, must-haves: correctly encodes RESEARCH Issue #2 and F-06: `lxml.html`, no BeautifulSoup, no `_fix_rtl_*` calls for HTML/XLSX/CSV.
- `97-05`, must-haves: correctly requires `scan_run_id` with `tokenizer_name="raw"`.
- `97-06`, D-NEW-7: good choice to duplicate invariant guards for cloud gates, web LOCAL exclusion, sys_id namespace, and post-dedup LOCAL merge.

**Concerns**

- **HIGH — `97-01` Task 2 migration fails on real current DBs.**
  Current `init_sqlite()` never sets `PRAGMA user_version`; fresh and deployed Phase 95 DBs are likely `0`, but the plan only registers `_MIGRATIONS = {1: _migrate_1_to_2}`. `test_empty_db_migrates_to_v2` contradicts the implementation template.
  *Verified: `shared/local_indexer.py:257` shows only `PRAGMA journal_mode=WAL`, no `user_version` PRAGMA anywhere in the file.*

- **HIGH — `97-01` does not actually write `cached_text`.**
  Task 2 adds `compress_cached_text()` / `decompress_cached_text()`, but no step updates `_write_page_doc()` or extractor loops to populate `local_pages.cached_text`. The R-03 must-have is therefore untested and unmet.
  *Verified: `_write_page_doc` exists at `shared/local_indexer.py:1223`; no plan task references it.*

- **HIGH — `97-01` R-02 rebuild is a method, not startup recovery.**
  Task 3 adds `rebuild_main_index_atomic()`, but no plan step changes `SearchEngine._open_local_searcher()` or `LocalIndexer.__init__()` to call it on corruption/schema mismatch. Current `LocalIndexer.__init__()` catches index-open failure and creates a fresh index, which can bypass recovery.

- **HIGH — `97-01` atomic rename will still fail on Windows.**
  Task 3 closes SearchEngine readers but not `LocalIndexer._writer` / `LocalIndexer._index`, which also hold handles on `LOCAL_INDEX_DIR`. Also the LAB close hook names `local_lab_index`, but the real attribute is `_local_lab_index`.

- **HIGH — `97-01` atomic rebuild uses the wrong Tantivy schema fields.**
  The plan's rebuild document shape mentions `sys_id`, `page_num`, and `title`; current `build_local_schema()` has `unique_id`, `content`, `content_head`, `content_tail`, `source`, `full_header`, `shelfmark`, `scope`, `boundaries`. Rebuild must reconstruct the existing stored-field shape, including `full_header` via `local_files.file_id`.
  *Verified: `shared/local_indexer.py:193-219` confirms schema field list.*

- **HIGH — `97-01` `_commit_batch` FULL bracket lacks rollback.**
  Task 4 uses `BEGIN IMMEDIATE` and `COMMIT` inside `try/finally`, but on UPDATE/COMMIT failure it does not `ROLLBACK` before restoring `PRAGMA synchronous=NORMAL`. That can leave the connection in a transaction and the pragma restore may fail.

- **HIGH — `97-01` recovery sentinel always triggers.**
  Task 4 inserts `_pending_cleanup(kind='unclean_shutdown')` during `_init_indexer`, then immediately checks for that same row. This makes every launch look interrupted. Also `desktop/my_library_tab.py` has no `closeEvent`; clean-shutdown cleanup belongs in the existing main-window close path or worker lifecycle.

- **HIGH — `97-05` Discard leaves sidecar rows and may leave uncommitted docs.**
  `discard_run()` deletes Tantivy docs and `processed_files` only. It must also delete `local_pages` and `local_files` for sys_ids in that run. It also needs to handle uncommitted current-writer docs; `delete_documents("scan_run_id", run_id)` may not remove docs added but not yet committed in the same writer session.

- **HIGH — later schema changes are not folded back into rebuild.**
  `97-05` adds `scan_run_id`; `97-06` adds `chunk_locator`. The Wave A rebuild method and tests do not get updated to emit those fields, and current `LocalIndexer.__init__()` uses `Index.open(index_dir)`, which can open an old schema without forcing rebuild.

- **HIGH — planned files do not exist.**
  Every plan lists `requirements-desktop.txt`, but this repo has only `requirements.txt` and `requirements-lock.txt`. Several tasks also mention `pytest.ini`; pytest config is in `pyproject.toml`.
  *Verified: `ls requirements*.txt pytest.ini pyproject.toml` confirms only `requirements.txt`, `requirements-lock.txt`, `pyproject.toml` exist — no `requirements-desktop.txt`, no `pytest.ini`.*

- **MEDIUM — `97-02` zip-bomb fixture likely does not work.**
  Setting `ZipInfo.file_size = 600MB` before `writestr()` will be overwritten by Python's zip writer with the real byte length. The test can pass falsely only if monkeypatched; as written it probably won't exercise `_check_zip_bomb()`.

- **MEDIUM — `97-02` oversized / zip statuses miss `local_files`.**
  The plan writes only `processed_files` for `oversized` and `zip_bomb_suspected`, but the UI status tree currently reads from `local_files`. Those rows may not display or count correctly.

- **MEDIUM — `97-04` folder counters are based on the wrong source.**
  `_refresh_folder_counters_for()` counts `processed_files.status`, but extraction errors are stored in `local_files.extraction_status`; `_commit_batch()` changes `processed_files.status` to `committed` even for files with extraction errors. The LIKE prefix query also risks `C:\foo` matching `C:\foobar`.

- **MEDIUM — `97-04` depends on statuses introduced in parallel `97-03`.**
  The counter query includes `encoding_error`, but `97-04` runs parallel to `97-03` and only depends on `97-01`. This is harmless as a string but weak as a testable contract.

- **MEDIUM — `97-05` View All plan uses non-existent names.**
  The actual code uses `self.browse_text` and `apply_line_numbered_text()`, not `browse_text_edit` or `_build_pages_html()`. Incremental append must preserve the line-gutter page-state logic, not just insert HTML fragments.

- **MEDIUM — `97-05` ETA plan contradicts its own test.**
  Task 1 says assert `compose_overall_eta() != naive sum`; Task 3 says the chosen composition is `sum`. Pick one before execution.

- **MEDIUM — `97-06` network statuses need full UI/query propagation.**
  Existing code skips `status == "unavailable"` in places. The plan adds `unreachable` and `timeout` but does not explicitly update all existing `unavailable` checks, folder-list color logic, and aggregate prescan filters.

- **LOW — dependency lock/update path is incomplete.**
  Plans add `zstandard` / `defusedxml` to `requirements.txt`, but not `requirements-lock.txt`. Given the repo's reproducible-build posture, the plan should state whether and how the lock file is regenerated.

**Suggestions**

- In `97-01` Task 2, add a `0 -> 1` bootstrap/no-op migration or set `PRAGMA user_version=1` inside `init_sqlite()` for freshly created baseline tables before running `1 -> 2`.
- In `97-01` Task 2, explicitly update `_write_page_doc()` to compress and insert `cached_text`, `cached_text_uncompressed_len`, `cached_text_codec`, and later-compatible `chunk_locator`.
- In `97-01` Task 3, add the actual startup hook: on `SearchEngine._open_local_searcher()` failure or schema mismatch, construct/use `LocalIndexer` and call atomic rebuild before falling back to `None`.
- In `97-01` Task 3, close and reopen `LocalIndexer._writer` / `_index` around directory swap, and fix LAB close to clear `_local_lab_index`.
- In `97-01` Task 4, wrap the SQLite FULL bracket with `except: ROLLBACK; raise` before restoring `NORMAL`.
- Replace the `unclean_shutdown` marker with a scan-run lifecycle marker written when a scan starts and cleared when that scan finishes/cancels cleanly.
- In `97-05`, redefine `discard_run()` as: rollback uncommitted writer state, delete committed Tantivy docs by raw `scan_run_id`, then delete `local_pages`, `local_files`, and `processed_files` for that run in one SQLite transaction.
- Remove `requirements-desktop.txt` from all plan frontmatter/tasks, and add explicit `requirements-lock.txt` handling.
- Change folder counter logic to aggregate from `local_files.folder_id` / `local_files.extraction_status`, not `processed_files.status` plus path `LIKE`.
- Update View All instructions to target the actual `browse_text` / `apply_line_numbered_text()` path and preserve page block marking.

**Risk Assessment**

**HIGH.** The context-level design is sound, but the plans have execution-level holes in migration, cached-text durability, atomic swap handle closure, recovery gating, and cancel discard semantics that could produce data loss or unrecoverable LOCAL index drift if implemented literally.

---

## Consensus Summary

Single-reviewer pass (Codex only — invoked via `/gsd-review --codex`). No consensus to triangulate, but Codex's claims were spot-checked against the codebase and the most actionable findings verified:

- ✅ `requirements-desktop.txt` does NOT exist (only `requirements.txt`, `requirements-lock.txt`)
- ✅ `pytest.ini` does NOT exist (pytest config is in `pyproject.toml`)
- ✅ `PRAGMA user_version` is never set in `shared/local_indexer.py` — current DBs are user_version=0, not 1
- ✅ `build_local_schema()` field list at `shared/local_indexer.py:193-219` differs from what 97-01 plan claims the rebuild emits
- ✅ `_write_page_doc()` exists at `shared/local_indexer.py:1223`; no Phase 97 plan task updates it to write `cached_text`

### Highest-priority issues (must fix before execution)

1. **Migration from user_version=0** — `97-01` Task 2 needs `0→1` bootstrap or `init_sqlite()` must stamp `user_version=1` on Phase 95 baseline tables
2. **cached_text write path** — `97-01` Task 2 must update `_write_page_doc()` to populate the new column (otherwise R-03 ships dead)
3. **Atomic rebuild not wired into startup** — `97-01` Task 3 missing the hook on `SearchEngine._open_local_searcher()` failure
4. **LocalIndexer writer/index handles not closed before rename** — Windows `os error 5` will persist despite the SearchEngine close
5. **Rebuild emits wrong schema fields** — `97-01` Task 3 lists `sys_id`/`page_num`/`title` which don't exist in `build_local_schema()`
6. **Discard incomplete** — `97-05` `discard_run()` must also delete `local_pages` + `local_files` + handle uncommitted writer docs
7. **Recovery sentinel always triggers** — `97-01` Task 4 logic insert-then-check makes every launch look interrupted
8. **`requirements-desktop.txt` referenced 6× across plans but does not exist** — replace with `requirements.txt` everywhere; add explicit `requirements-lock.txt` regen step
9. **Folder counters wrong source table** — `97-04` should aggregate from `local_files.extraction_status`, not `processed_files.status` + path LIKE
10. **View All field names don't exist** — `97-05` references `browse_text_edit` / `_build_pages_html()` instead of actual `self.browse_text` / `apply_line_numbered_text()`

### Lower-priority polish

- ETA contradiction in `97-05` (Task 1 asserts ≠ naive sum; Task 3 says composition IS sum) — pick one
- Zip-bomb fixture in `97-02` won't trigger `_check_zip_bomb()` as written (ZipInfo.file_size gets overwritten by writestr)
- `97-06` network status migration incomplete — `unavailable` callsites need explicit update for new `unreachable`/`timeout` codes

### Next steps

Replan with Codex feedback incorporated:

```
/gsd-plan-phase 97 --reviews
```

The planner will re-read 97-REVIEWS.md and produce targeted plan edits without regenerating from scratch.
