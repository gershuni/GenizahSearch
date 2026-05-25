I read the Phase 97 plan docs, the review files, and the relevant LOCAL/My Library code. My strongest guidance to the GSD planning agents is: do not make this a light “respond to review comments” pass. The plans need a storage/lifecycle reset around the real current code, then the feature waves can sit on top of it.

**Guidance For GSD Planning Agents**

Treat Phase 97 as blocked until the plan defines one canonical final LOCAL storage contract. The current plans drift across waves. Start by writing the target schema once, then make every wave reference it.

Key repo facts to anchor on:

- Tantivy schema is defined in [shared/local_indexer.py](C:/Users/gersh/.codex/worktrees/537d/GenizahSearch/shared/local_indexer.py:193), and it does not have `sys_id`, `page_num`, or `title`.
- SQLite is initialized in [shared/local_indexer.py](C:/Users/gersh/.codex/worktrees/537d/GenizahSearch/shared/local_indexer.py:247), and fresh/Phase 95 DBs remain at `PRAGMA user_version=0`.
- LOCAL page writes happen through `_write_page_doc()` in [shared/local_indexer.py](C:/Users/gersh/.codex/worktrees/537d/GenizahSearch/shared/local_indexer.py:1223); cached text must be written there, not only via helper functions.
- LOCAL searcher startup is in [genizah_core.py](C:/Users/gersh/.codex/worktrees/537d/GenizahSearch/genizah_core.py:6686); atomic rebuild must be wired there or into an explicitly earlier My Library recovery path.
- View All uses `self.browse_text` plus `apply_line_numbered_text()` in [genizah_app.py](C:/Users/gersh/.codex/worktrees/537d/GenizahSearch/genizah_app.py:18765), not `browse_text_edit` or `_build_pages_html`.
- Pytest config is in [pyproject.toml](C:/Users/gersh/.codex/worktrees/537d/GenizahSearch/pyproject.toml), not `pytest.ini`.
- There is no `requirements-desktop.txt`; dependencies live in [requirements.txt](C:/Users/gersh/.codex/worktrees/537d/GenizahSearch/requirements.txt) and lock handling must be explicit.

**Recommended Plan Shape**

1. **Wave A: storage contract first**
   Define the final SQLite and Tantivy schema up front, including later-wave fields like `scan_run_id` and `chunk_locator` if they are part of the final design. Do not let Wave A rebuild emit a schema that Wave E/F will immediately obsolete.

2. **Migration must start from `user_version=0`**
   Add a real migration ladder from `0 -> target`, and stamp fresh DBs with the target version. Tests must cover fresh DB, Phase 95-style DB at version 0, partially migrated DB, and rerun/idempotence. Use transaction + rollback on migration failure.

3. **Cached text must be in the write path**
   Compression helpers are not enough. `_write_page_doc()` should persist cached text/page metadata whenever it writes a Tantivy document. Include codec/version/length fields and a legacy fallback for rows with null cache.

4. **Atomic rebuild must be wired into startup**
   The plan must prevent `LocalIndexer.__init__()` from silently creating a fresh empty index after open failure. Startup recovery should rebuild from SQLite cached text, validate, close all LOCAL handles, swap with Windows-safe retry, then reload searchers.

5. **Close all Windows handles before swap/delete**
   Include `SearchEngine.local_searcher/local_index`, `local_lab_searcher/_local_lab_index`, LabEngine mirrors, and `LocalIndexer._writer/_index`. The existing plan only closes part of the handle graph.

6. **Commit/cancel semantics need a real run model**
   `_commit_batch()` needs rollback on failure. The recovery sentinel design should be replaced with scan-run lifecycle state, not “insert a row and then detect it.” Discard must remove Tantivy docs and all related SQLite rows: `processed_files`, `local_files`, and `local_pages`.

7. **Folder counters should use `local_files`**
   Do not derive counters from `processed_files` plus path `LIKE`. Use `local_files.folder_id` and `local_files.extraction_status`, and propagate new statuses everywhere that currently checks `unavailable`.

8. **View All must target the actual UI path**
   Any incremental loading plan has to preserve the line-number/page-state behavior around `apply_line_numbered_text()`. Reject pseudocode that names nonexistent widgets or helpers.

9. **Dependency/config references must be cleaned before review**
   Remove every mention of `requirements-desktop.txt` and `pytest.ini`. Add markers to `pyproject.toml`. Add package changes to `requirements.txt`, and define whether/how `requirements-lock.txt` is regenerated. For HTML, keep the plan aligned around `lxml.html`, not BeautifulSoup.

10. **Add a pre-review self-audit**
   Before launching Codex review, grep the plans for forbidden or suspicious tokens: `requirements-desktop.txt`, `pytest.ini`, `browse_text_edit`, `_build_pages_html`, `_pending_cleanup`, “insert then check same sentinel,” and any Tantivy rebuild fields not present in the real schema.

The planning agents should make each task cite the exact target function/file it modifies. The main failure pattern in the current Phase 97 material is not missing ambition; it is invented integration points. Tighten those, and the next review should become a real design review instead of a structural rejection.