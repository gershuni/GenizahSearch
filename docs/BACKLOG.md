# GenizahSearch - Backlog

> **Last Updated:** 2026-10-25 (created at the tracker reconstruction from its non-defect sections and rows). 109 items.

What is not an open defect: deferred defects, code debt, test gaps, ideas and extensions,
and server or owner tasks. Same row form and the same ID space as
[`OPEN_ISSUES.md`](OPEN_ISSUES.md); the full text of each row is in
[`ISSUE_DETAILS.md`](ISSUE_DETAILS.md) under its ID. Read only when planning; grep otherwise.
Close a row as in the tracker (`✅ ...`, then `python scripts/archive_closed_issues.py --apply`);
move one to the tracker by moving its row and changing its section to a priority.

## Summary

| Area | Deferred defects | Code debt | Test gaps | Ideas and extensions | Server and owner tasks | Total |
|------|------|------|------|------|------|------|
| Search | 0 | 5 | 0 | 6 | 0 | 11 |
| Browse and viewer | 0 | 0 | 0 | 0 | 1 | 1 |
| Desktop app | 2 | 12 | 1 | 8 | 0 | 23 |
| Website | 0 | 7 | 0 | 3 | 2 | 12 |
| Discovery | 0 | 3 | 0 | 2 | 2 | 7 |
| Accounts and community | 0 | 7 | 3 | 5 | 3 | 18 |
| Data and sidecars | 2 | 0 | 0 | 0 | 2 | 4 |
| Build, CI and tests | 0 | 6 | 24 | 0 | 0 | 30 |
| Server and operations | 0 | 0 | 0 | 0 | 3 | 3 |
| **Total** | **4** | **40** | **28** | **24** | **13** | **109** |

## Deferred defects

### Desktop app

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-159](ISSUE_DETAILS.md#gs-159) | Desktop field translation has never worked in a packaged build (owner: not worth fixing, 2026-09-20) | `desktop/gui_threads.py::few_shot_data_dir`, `GenizahSearchPro.spec` datas | 2026-09-20 | ⏸ Deferred |
| [GS-160](ISSUE_DETAILS.md#gs-160) | Results context menu detects LOCAL only from top-level res["source"], so a Lab LOCAL row with empty display.id gets the Genizah menu | `genizah_app.py:5645` (_show_results_context_menu), lines ~5681-5683 | 2026-05-31 | ⏸ Deferred |

### Data and sidecars

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-161](ISSUE_DETAILS.md#gs-161) | PGP Hebrew translations are materially wrong in ~28% of rows | `pgp_data/pgp.db` (pgp_translations), `shared/dicta_client.py`, `shared/translation_qc.py` | 2026-09-21 | ⏸ Deferred |
| [GS-162](ISSUE_DETAILS.md#gs-162) | 28 unresolved `sys_id` | `fgp_data/fgp_transcriptions.db` (gitignored); `fgp_data/README.md:106-120` | — | ⏸ Deferred |

## Code debt

### Search

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-163](ISSUE_DETAILS.md#gs-163) | Pattern language: the mark-tolerant inserter and the word separator share characters, so a multi-word pattern reads two ways at a boundary | `shared/search_engine.py` (`MARK_TOLERANT_INSERTER`, `_SEPARATOR_CLASS`, `_shared_chars_re`) | 2026-10-12 | ❌ Open |
| [GS-164](ISSUE_DETAILS.md#gs-164) | Windows: Extended/Maximum patterns match about 60 times slower with the `regex` module than with `re` | `shared/search_regex.py::Pattern._call`, `shared/research_worker.py::protect_parent_death` | 2026-10-11 | ❌ Open |
| [GS-165](ISSUE_DETAILS.md#gs-165) | Regex patterns that do not start at a word edge cost a pass over the whole term dictionary | `shared/regex_prefilter.py` (a run not anchored at a token start becomes `.*X.*`) | 2026-10-06 | ❌ Open |
| [GS-166](ISSUE_DETAILS.md#gs-166) | Engines infer cancellation from `cancel_flag` instead of returning an outcome | `shared/search_engine.py`, `shared/lab_engine.py`, `gui_threads.py` | 2026-08-19 | ❌ Open |
| [GS-167](ISSUE_DETAILS.md#gs-167) | Small per-query costs | `shared/thread_local_db.py::_get_conn`, `shared/passage_search.py::_verify_and_merge` | — | ❌ Open |

### Desktop app

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-168](ISSUE_DETAILS.md#gs-168) | Desktop viewer: the MULTILINE test fires on every word separator and DOTALL is a no-op, so a Maximum pattern is recompiled on the UI thread | `desktop/widgets/__init__.py` (mark_pattern_hits), `desktop/result_dialog.py` (6 sites), `genizah_app.py` | 2026-10-12 | ❌ Open |
| [GS-169](ISSUE_DETAILS.md#gs-169) | Dead code, swallowed errors and redundant catches | `genizah_app.py`, `shared/local_indexer.py`, `desktop/gui_threads.py`, `web/pages/parallels.py` | 2026-10-06 | ❌ Open |
| [GS-170](ISSUE_DETAILS.md#gs-170) | Desktop Visual Similarity download code is dead: its server route was removed | `desktop/vs_cache.py::VSDownloadThread`; `desktop/settings_dialogs.py::_start_vs_download` | 2026-09-25 | ❌ Open |
| [GS-171](ISSUE_DETAILS.md#gs-171) | Desktop export keeps parallel row builders in two long methods | `genizah_app.py::export_results`, `export_comp_report` | 2026-09-25 | ❌ Open |
| [GS-172](ISSUE_DETAILS.md#gs-172) | Dead branch passes kwargs `ColumnFilterDialog` does not accept | `genizah_app.py` `open_filter_dialog` (the `isinstance(col_idx, int)` branch) | 2026-09-19 | ❌ Open |
| [GS-173](ISSUE_DETAILS.md#gs-173) | `GroupingThread` emits no terminal signal when cancelled | `gui_threads.py:353-354, 367-368` | 2026-08-19 | ❌ Open |
| [GS-174](ISSUE_DETAILS.md#gs-174) | Dead signal connection to a method that does not exist | `genizah_app.py:21852` | 2026-08-19 | ❌ Open |
| [GS-175](ISSUE_DETAILS.md#gs-175) | Phase 111 telemetry — Codex review F3: `identify()` passes `user_id` to PostHog unvalidated | `desktop/telemetry.py::identify` (~line 582) | 2026-06-14 | ❌ Open |
| [GS-176](ISSUE_DETAILS.md#gs-176) | Phase 111 telemetry — Codex review F6: default distinct_id substitution conflates NLI-breaker operational events with user identity | `shared/posthog_server.py:184-187` (`enqueue_event` `distinct_id=='system'` substitution) | 2026-06-14 | ❌ Open |
| [GS-177](ISSUE_DETAILS.md#gs-177) | View All page-block matching falls back to substring matching in _mark_blocks_for_pages (fragile; deferred refactor) | `desktop/widgets/line_number_text_edit.py:283-323` (_mark_blocks_for_pages) | 2026-05-24 | ⏸ Deferred |
| [GS-178](ISSUE_DETAILS.md#gs-178) | View All helpers _aggregate_local_pages_with_separators / _get_local_full_text_for_sys_id are vestigial; renderer hand-builds <p> blocks | `genizah_app.py:280`, :27201 | 2026-05-24 | ⏸ Deferred |
| [GS-179](ISSUE_DETAILS.md#gs-179) | `images_cache/` grows without bound | `desktop/image_loader.py` | — | ❌ Open |

### Website

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-180](ISSUE_DETAILS.md#gs-180) | The NiceGUI 3.8 rollback code is dead on 3.18, and 63 fire-and-forget tasks remain | `web/framework_patches.py` (_patch_nicegui_esm_handler, _patch_binding_remove); `web/` tasks | 2026-10-20 | ❌ Open |
| [GS-181](ISSUE_DETAILS.md#gs-181) | The web app listens on `0.0.0.0:8081` | `web/main.py` (`ui.run` without `host`) | 2026-10-07 | ❌ Open |
| [GS-182](ISSUE_DETAILS.md#gs-182) | Web puzzle PNG export bytes stay in the client's message history | `web/pages/puzzle.py::export_png` | 2026-09-29 | ❌ Open |
| [GS-183](ISSUE_DETAILS.md#gs-183) | Two helpers compute the signed-out browser key | `web/saved_joins.py`, `web/puzzle_image_access.py` | 2026-09-29 | ❌ Open |
| [GS-184](ISSUE_DETAILS.md#gs-184) | Web startup still loads the retired anonymous `ListsManager`, only as a readiness signal | `web/main.py` (startup load into `state.lists_mgr`); about 15 `if not state.lists_mgr` sites | 2026-09-25 | ❌ Open |
| [GS-185](ISSUE_DETAILS.md#gs-185) | Browse map cold load costs 18 s per fresh web process (133 MB pickle + `dedupe_browse_map` repair scan over 232,450 sys_ids) | `shared/search_engine.py::_load_browse_map`, `shared/browse_service.py::_fetch_core` | 2026-09-02 | ❌ Open |
| [GS-186](ISSUE_DETAILS.md#gs-186) | CSRF protection missing | API endpoints | — | ❌ Open |

### Discovery

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-187](ISSUE_DETAILS.md#gs-187) | The bake stores ONE work-side span per claim, so scattered multi-range citations cannot be produced (WP4 deferred) | `scripts/build_discovery_sidecar.py::project_ref_span` (4433-4487), `shared/discovery_locus.py` | 2026-08-10 | ❌ Open |
| [GS-188](ISSUE_DETAILS.md#gs-188) | _band_measurements keys its cache on self._last_path before _get_conn() refreshes it, so a stale path can persist | `shared/discovery_service.py::_band_measurements` (2817-2855), `::_get_conn` (2368) | 2026-08-04 | ❌ Open |
| [GS-189](ISSUE_DETAILS.md#gs-189) | _browse_cached_call's cache key carries no artifact path, so any enveloped browse read could serve a previous artifact's rows | `shared/discovery_service.py::_browse_cached_call` (4867-4896) | 2026-08-04 | ❌ Open |

### Accounts and community

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-190](ISSUE_DETAILS.md#gs-190) | Admin approve/reject drops `reviewed_by` / `reviewed_at` from its payload (it runs in a worker thread) | `web/pages/admin.py::update_correction_status` (221-262), `approve/reject` (788-806) | 2026-10-20 | ❌ Open |
| [GS-191](ISSUE_DETAILS.md#gs-191) | A removal here never deletes a row this computer did not record | `shared/lists_manager.py::_queue_record` | 2026-09-27 | ❌ Open |
| [GS-192](ISSUE_DETAILS.md#gs-192) | While lists.pkl cannot be saved, the website-removal prompt waits | `genizah_app.py::_offer_web_removals` | 2026-09-27 | ❌ Open |
| [GS-193](ISSUE_DETAILS.md#gs-193) | A list-sync worker can print `could not acquire lock for <stderr>` at exit | `desktop/lists_sync_runner.py` (daemon worker) | 2026-09-27 | ❌ Open |
| [GS-194](ISSUE_DETAILS.md#gs-194) | The server-side end of a desktop session is best effort | `desktop/supabase_corrections_client.py::logout` (`revoke='background'`) | 2026-09-27 | ❌ Open |
| [GS-195](ISSUE_DETAILS.md#gs-195) | `sync_list_to_cloud` always creates a new cloud list for a list without a cloud id | `shared/lists_sync.py::sync_list_to_cloud` | 2026-09-27 | ❌ Open |
| [GS-196](ISSUE_DETAILS.md#gs-196) | List sync makes requests it could skip | `shared/lists_sync.py` (`get_cloud_lists_preview`, `_push_projects_and_lists`) | 2026-09-27 | ❌ Open |

### Build, CI and tests

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-197](ISSUE_DETAILS.md#gs-197) | `requirements-lock.txt` is a Windows resolution, so Linux-only packages still float on deploy | `requirements-lock.txt`; `deploy.sh` (`-c requirements-lock.txt` since 2026-10-21) | 2026-10-21 | ❌ Open |
| [GS-198](ISSUE_DETAILS.md#gs-198) | `scripts/verify_export.py` relies on the removed export fallback | `scripts/verify_export.py` | 2026-09-25 | ❌ Open |
| [GS-199](ISSUE_DETAILS.md#gs-199) | `tests/test_translations_no_duplicate_keys.py` is committed with `\r\r\n` on every line | `tests/test_translations_no_duplicate_keys.py` | 2026-09-21 | ❌ Open |
| [GS-200](ISSUE_DETAILS.md#gs-200) | `bodleian_master_index.csv` has no code reader but ships in every installer | `GenizahSearchPro.spec` datas | — | ❌ Open |
| [GS-201](ISSUE_DETAILS.md#gs-201) | fgp_transcriptions.db is in the installer spec with its producer scripts untracked and FGP absent from the updater map | `GenizahSearchPro.spec`, `fgp_data/fgp_*.py` (untracked), `gui_threads.py` sidecar-updater map | — | ❌ Partly fixed |
| [GS-202](ISSUE_DETAILS.md#gs-202) | Small hygiene gaps found by the inventory | .gitignore, `desktop/settings_dialogs.py:339`, `genizah_app.py:36939`, `scripts/verify_passage_index.py` | — | ❌ Open |

## Test gaps

### Desktop app

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-203](ISSUE_DETAILS.md#gs-203) | MARC field translations (Date/Subjects/People) | `desktop/result_dialog.py:2246`, :3798; `desktop/title_helpers.py::_translate_hebrew_date` | — | ⏳ Needs a check |

### Accounts and community

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-204](ISSUE_DETAILS.md#gs-204) | Desktop list sync by hand: the progress dialog and a pulled-cable log-out | `desktop/lists_sync_runner.py` and the sync progress dialog | 2026-09-27 | ⏳ Needs a check |
| [GS-205](ISSUE_DETAILS.md#gs-205) | End-to-End Integration | `tests/e2e/` `(test_browse_flow.py`, `test_search_flow.py`, `test_performance.py` only) | — | ❌ Open |
| [GS-206](ISSUE_DETAILS.md#gs-206) | Concurrency | `web/supabase_client.py::update_correction`; `tests/test_corrections_*.py` | — | ❌ Open |

### Build, CI and tests

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-207](ISSUE_DETAILS.md#gs-207) | A stale "interrupted" search cut-off leaks into `test_worker_result_carries_the_engines_cut_off` | `tests/test_web_search_within_cutoff.py`; `shared/research_worker.py::run_query`; _LAST_SEARCH_CUTOFF | 2026-10-25 | ❌ Open |
| [GS-208](ISSUE_DETAILS.md#gs-208) | Responsa real-data gate: two mutations cannot fail on the real index | `scripts/verify_responsa_wildcards.py::PHASE3` | 2026-10-23 | ❌ Open |
| [GS-209](ISSUE_DETAILS.md#gs-209) | Windows CI: a test fixture's Tantivy `commit()` can fail with "Access is denied (os error 5)" | `tests/test_whole_word_fuzzy.py` fixtures (w.commit()); LocalIndexer._commit_writer_with_retry | 2026-10-22 | ❌ Open |
| [GS-210](ISSUE_DETAILS.md#gs-210) | Test: the Joins Lab refused-search test times out in a long single-process run | `tests/test_responsa_refusal_surfaces.py::test_a_refused_web_search_does_not_bring...` | 2026-10-22 | ⏳ Needs a check |
| [GS-211](ISSUE_DETAILS.md#gs-211) | Tests that build a /browse page make real anonymous Supabase reads against the production URL | `tests/conftest.py`; `tests/test_browse_recents_off_loop.py::supabase_offline` | 2026-10-20 | ❌ Open |
| [GS-212](ISSUE_DETAILS.md#gs-212) | `test_filter_unavailable_desktop_window.py` fails locally when the saved UI language is Hebrew | `tests/test_filter_unavailable_desktop_window.py` (line 378, English status-bar comparison) | 2026-10-14 | ❌ Open |
| [GS-213](ISSUE_DETAILS.md#gs-213) | `test_search_regex.py` `\B` boundary tests time out after other search tests in one process | `test_search_regex.py::test_unicode_boundaries_and_search_bounds`; `search_regex.py::_match_timeout` | 2026-10-11 | ❌ Open |
| [GS-214](ISSUE_DETAILS.md#gs-214) | Test-isolation flake: the time-limit lower-bound API test gets 429 rate_limited in a multi-file batch | `tests/test_search_api.py:874`, the module-level API rate limiter | 2026-10-09 | ❌ Open |
| [GS-215](ISSUE_DETAILS.md#gs-215) | `test_search_api_library_mode.py` fails (500 `internal_error`) after two other test files in one process | `web/research_jobs.py::shutdown_research`, wait_executor; `tests/test_search_api_library_mode.py` | 2026-10-07 | ❌ Open |
| [GS-216](ISSUE_DETAILS.md#gs-216) | Two groups of tests never run in CI | `tests/conftest.py` (collect_ignore_glob); `tests/e2e/conftest.py`; `.github/workflows/ci.yml` | 2026-10-06 | ❌ Open |
| [GS-217](ISSUE_DETAILS.md#gs-217) | Test and gate tooling gaps | `scripts/run_local_tests.py`, `schedule_nightly_artifact_verify.ps1`, `init.ps1`, `export_access_index.py` | 2026-10-06 | ❌ Open |
| [GS-218](ISSUE_DETAILS.md#gs-218) | `tests/test_findings_page.py` fails locally when `web.main` is imported before it | `tests/test_findings_page.py`, `tests/render_smoke/conftest.py`, `web/main.py` (load_dotenv at import) | 2026-10-05 | ❌ Open |
| [GS-219](ISSUE_DETAILS.md#gs-219) | `test_findings_page.py` hangs a runner chunk now and then | `tests/test_findings_page.py` (24 asyncio.run sites), `web/bounded_io.py`, `web/discovery.py` | 2026-10-04 | ❌ Open |
| [GS-220](ISSUE_DETAILS.md#gs-220) | GUI test files crash when certain pairs share a process (0xC0000005) | test_my_library_tab+test_crash_hooks; test_local_ceiling_enforcement+test_comp_tree_batch_load | 2026-09-30 | ❌ Open |
| [GS-221](ISSUE_DETAILS.md#gs-221) | Local Windows test runs use the native Qt platform, so two GUI tests fail locally that pass in CI | `tests/conftest.py:29-30`; `scripts/run_local_tests.py` | 2026-09-30 | ❌ Open |
| [GS-222](ISSUE_DETAILS.md#gs-222) | `test_flush_before_exit_total_wall_time_bounded` depends on test order | `tests/test_telemetry_posthog_server_ext.py::TestFlushBeforeExit` | 2026-09-30 | ❌ Open |
| [GS-223](ISSUE_DETAILS.md#gs-223) | Tests still touch the developer's data folder at import, and two personal files are not isolated | `genizah_core.py` (logger handlers, `CURRENT_LANG = load_language()`), `tests/conftest.py` | 2026-09-25 | ❌ Open |
| [GS-224](ISSUE_DETAILS.md#gs-224) | The Stable-Citations gate cannot see bare shorthand line references | `scripts/check_docs.py` (`LINE_CITATION_RE`), `.planning/milestones/v9.0.0-MILESTONE-AUDIT.md` | 2026-09-20 | ❌ Open |
| [GS-225](ISSUE_DETAILS.md#gs-225) | Repo-wide `TestNoIntCoercion` scans walk `.worktrees/` and fail locally on stale worktree copies | `tests/test_local_sys_id_namespace.py`, `tests/test_synthetic_sys_id.py` (the `os.walk` prune list) | 2026-09-17 | ❌ Open |
| [GS-226](ISSUE_DETAILS.md#gs-226) | Three discovery dispatch tests went red after the panel render-smoke file (leaked heavy slot); no longer reproduces locally | `tests/test_findings_page.py`, `tests/test_discovery_panel_browse_wiring.py`, `shared/discovery_service.py::_run_off_loop` | 2026-08-09 | ⏳ Needs a check |
| [GS-227](ISSUE_DETAILS.md#gs-227) | The FL-ID index build is an unjoined daemon thread and seven test files still start it against a MagicMock | `shared/search_engine.py::start_fl_id_index_build`; 7 test files that build SearchEngine(MagicMock) | 2026-05-29 | ❌ Open |
| [GS-228](ISSUE_DETAILS.md#gs-228) | Test-isolation flake: `test_discard_run_short_circuits_sqlite_on_tantivy_failure` fails in batch, passes in isolation | `tests/test_phase_97_2_sqlite_vs_tantivy_consistency.py`, `shared/local_indexer.py::discard_run` | 2026-05-29 | ❌ Open |
| [GS-229](ISSUE_DETAILS.md#gs-229) | FolderWalkWorker teardown race: test_unified_tree_uncheck_close_reopen_roundtrip (and siblings) not collected on CI | `tests/conftest.py:74-88` (collect_ignore_glob), `tests/test_local_optout_persistence.py:182` | 2026-05-28 | ⏸ Deferred |
| [GS-230](ISSUE_DETAILS.md#gs-230) | Phase 87 scanner edge case: bare chained attribute double-report | `tests/test_no_raw_storage_access.py` (`_StorageAccessVisitor.visit_Attribute`) | 2026-05-13 | ❌ Open |

## Ideas and extensions

### Search

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-231](ISSUE_DETAILS.md#gs-231) | Composition Search conditions (must / must not appear): built and released in v9.7.0, still awaiting the user's G8 cases and scope answer | `shared/compound_query.py`; `shared/passage_parallels.py::search_composition_logic`; `genizah_app.py` | 2026-10-14 | ❌ Partly fixed |
| [GS-232](ISSUE_DETAILS.md#gs-232) | Variants verification could check words against the spelling set instead of one long regex | `shared/search_engine.py` (variant regex 2763-2791, _variant_alternation 796) | 2026-10-07 | ❌ Open |
| [GS-233](ISSUE_DETAILS.md#gs-233) | Lab Mode results in a search-within chain | `desktop/gui_threads.py::LabSearchThread`, `genizah_app.py::_enter_refine_mode` | 2026-10-04 | ❌ Open |
| [GS-234](ISSUE_DETAILS.md#gs-234) | Desktop search: common words are still cut at 50,000 rows, and some routes show no early rows | `shared/config.py:138`, `shared/search_engine.py:4333-4338`, `desktop/gui_threads.py:310` | 2026-04-17 | ❌ Open |
| [GS-235](ISSUE_DETAILS.md#gs-235) | Desktop composition search in chunk mode shows nothing until the whole scan ends (28-43 s) | `desktop/gui_threads.py:499` (CompositionThread), `genizah_app.py` (run_composition) | 2026-04-17 | ❌ Open |
| [GS-236](ISSUE_DETAILS.md#gs-236) | Search/indexing of FGP text | `shared/search_engine.py` (no FGP reference); `shared/fgp_service.py` | — | ❌ Open |

### Desktop app

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-237](ISSUE_DETAILS.md#gs-237) | Starting offline with a saved log-in shows you logged out | `desktop/supabase_corrections_client.py::_load_credentials` | 2026-09-27 | ❌ Open |
| [GS-238](ISSUE_DETAILS.md#gs-238) | Right-click exclusions have no list and no undo short of New | `genizah_app.py::_exclude_word_search_result` | 2026-09-25 | ❌ Open |
| [GS-239](ISSUE_DETAILS.md#gs-239) | Re-running a composition search from History uses the current Exclude Manuscripts list, not the one it ran with | `genizah_app.py::_add_comp_search_to_history`, `_restore_comp_search_from_state` | 2026-09-25 | ❌ Open |
| [GS-240](ISSUE_DETAILS.md#gs-240) | Feature requests from the 2026-09-25 sweep | `genizah_app.py`, `web/pages/lists.py` | 2026-09-25 | ❌ Open |
| [GS-241](ISSUE_DETAILS.md#gs-241) | A paused search does not survive closing the program | `shared/pause_gate.py`, `gui_threads.py::PausableSearchMixin` | 2026-08-20 | ❌ Open |
| [GS-242](ISSUE_DETAILS.md#gs-242) | Optional PDF OCR extension for image-only scanned PDFs in My Library (deferred, SEED-003) | `.planning/seeds/SEED-003-optional-ocr-extension.md`, `shared/local_indexer.py` (PDF extraction) | 2026-05-29 | ⏸ Deferred |
| [GS-243](ISSUE_DETAILS.md#gs-243) | SEED-004 (deferred): show nikud for non-PDF LOCAL formats via a stored content_display field | `shared/local_indexer.py` (_write_page_doc), My Library display | 2026-05-29 | ⏸ Deferred |
| [GS-244](ISSUE_DETAILS.md#gs-244) | Session restore is not pixel-perfect | genizah_app.py | — | ❌ Open |

### Website

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-245](ISSUE_DETAILS.md#gs-245) | Catalog browse table scrolls sideways on a phone | `web/pages/catalog_browse.py` | 2026-10-22 | ❌ Open |
| [GS-246](ISSUE_DETAILS.md#gs-246) | perf_watch cannot see a loop saturated by many sub-300 ms slices | `web/perf_watch.py` | 2026-10-20 | ❌ Open |
| [GS-247](ISSUE_DETAILS.md#gs-247) | Puzzle BG removal: brown backing page not removed on glued manuscripts | `shared/background_removal.py`, `web/pages/puzzle.py` | — | ❌ Open |

### Discovery

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-248](ISSUE_DETAILS.md#gs-248) | The validated CanonRarity distinctiveness score is used by no production code, only by offline grading tools | `same_work_spike/probe/scripts/canon_rarity.py` | 2026-08-09 | ❌ Open |
| [GS-249](ISSUE_DETAILS.md#gs-249) | Pointer to the discovery forward ledger: probe-validated ideas with no current owner | `docs/specs/discovery-forward-ledger.md` | — | ❌ Open |

### Accounts and community

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-250](ISSUE_DETAILS.md#gs-250) | While lists.pkl cannot be saved, the log-out notice does not name what did not upload | `genizah_app.py::_logout_notice_paragraphs` | 2026-09-27 | ❌ Open |
| [GS-251](ISSUE_DETAILS.md#gs-251) | A removal left unsent at log-out gets the general notice, not the list's name | `genizah_app.py::_logout_notice_paragraphs` | 2026-09-27 | ❌ Open |
| [GS-252](ISSUE_DETAILS.md#gs-252) | Feature request: comment category and anonymous comments (both apps) | comments table; `web/components/comment_dialog.py`; `desktop/corrections_ui.py::CommentDialog` | 2026-09-25 | ❌ Open |
| [GS-253](ISSUE_DETAILS.md#gs-253) | Web Joins Lab — no confirm-join / delete-join moderation UI (only self-delete D-03) | `web/pages/joins_lab.py`, `web/components/candidate_grid.py`, future admin page | 2026-06-21 | ❌ Open |
| [GS-254](ISSUE_DETAILS.md#gs-254) | Phase 87 follow-up: enable transcriptions / joins / comments on the 108 image-bearing synthetic manuscripts | `desktop/corrections_client.py:663`; `desktop/supabase_corrections_client.py:851`; `web/pages/browse.py` | 2026-05-12 | ❌ Open |

## Server and owner tasks

### Browse and viewer

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-255](ISSUE_DETAILS.md#gs-255) | Oxford images: no machine-fetchable image source (the Bodleian host is behind an Anubis bot-challenge; NLI refuses Bodleian-held ids) | `desktop/image_loader.py:188`, `desktop/viewers.py:1423-1450`, `web/api.py::oxford_image` | 2026-09-02 | ❌ Open |

### Website

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-256](ISSUE_DETAILS.md#gs-256) | Browse/catalog raw HTML is mostly an app shell, with little indexable content before hydration | `web/pages/browse.py`, `web/pages/catalog_browse.py` | — | ⏳ Needs a check |
| [GS-257](ISSUE_DETAILS.md#gs-257) | SEO follow-up: run Search Console URL Inspection and PageSpeed Insights on production (homepage and a /browse page) | production (Search Console, PSI) | — | ⏳ Needs a check |

### Discovery

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-258](ISSUE_DETAILS.md#gs-258) | The local discovery artifact is V4; production serves V4.2 | `discovery_data/live/` (staged by the local `.env` for every test run) | 2026-10-20 | ❌ Open |
| [GS-259](ISSUE_DETAILS.md#gs-259) | Provenance of the 121 human_confirmed rows is unestablished, so no surface shows a human-review badge | `docs/specs/discovery-band-labels-v1.md` (Note 2), `shared/discovery_band_labels.py::review_overlay` | 2026-07-30 | ❌ Open |

### Accounts and community

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-260](ISSUE_DETAILS.md#gs-260) | Two sign-ups at the same moment can take usernames that differ only by case | handle_new_user `(migrations/profiles_no_email_username.sql)`; `supabase_setup.sql` | 2026-10-21 | ❌ Open |
| [GS-261](ISSUE_DETAILS.md#gs-261) | Supabase DDL gaps | `supabase_setup.sql`, `migrations/` | 2026-10-21 | ❌ Open |
| [GS-262](ISSUE_DETAILS.md#gs-262) | Supabase hardening follow-ups: anon default grants, SECURITY DEFINER search_path, client-side counters, public votes, review rate-limit key | `supabase_setup.sql`; `scripts/create_identification_reviews_beta.sql`; `migrations/*guards.sql` | 2026-10-20 | ❌ Partly fixed |

### Data and sidecars

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-263](ISSUE_DETAILS.md#gs-263) | Line Height returns only with a catalog data release | `fist_data/fjms_enrichment.db`, `scripts/import_measurements.py::migrate_add_line_height` (670) | 2026-10-06 | ❌ Open |
| [GS-264](ISSUE_DETAILS.md#gs-264) | FGP shipping: three loose ends | `desktop/gui_threads.py:1960` service_map; `DATA_LIFECYCLE.md:119`; `DEPLOYMENT_TECHNICAL.md` | 2026-10-05 | ❌ Open |

### Server and operations

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-265](ISSUE_DETAILS.md#gs-265) | nginx logs and forwards Cloudflare edge addresses until the realip snippet is installed | `scripts/genizah_cloudflare_realip.conf`, `refresh_cloudflare_realip.py`, server nginx site file | 2026-10-07 | ⏳ Needs a check |
| [GS-266](ISSUE_DETAILS.md#gs-266) | Slow-request log exists app-side; nginx still lacks $request_time/$upstream_*/CF-Ray in its log format | `web/perf_watch.py` (new), `web/main.py`, nginx config (NOT yet changed) | 2026-07-30 | ❌ Partly fixed |
| [GS-267](ISSUE_DETAILS.md#gs-267) | Neither PostHog drop counter is exposed by an HTTP route, so monitoring them at deploy needs a prod REPL | `web/api_hardening.py:531`, `shared/posthog_server.py:54` | — | ❌ Open |
