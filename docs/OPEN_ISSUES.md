# GenizahSearch - Open Issues

> **Last Updated:** 2026-10-27 (folded 1 fragment(s) from `docs/open_issues.d/`: 2026-10-27-fgp-pdf-final-letters-control-chars.md). 164 open: P1 0, P2 42, P3 122.

One line per open defect, by priority and area. The full text of every row is in
[`ISSUE_DETAILS.md`](ISSUE_DETAILS.md) under its ID: **grep `GS-<n>` there, never read it
whole.** What is not a defect -- deferred defects, code debt, test gaps, ideas, server and
owner tasks -- is in [`BACKLOG.md`](BACKLOG.md). Closed history:
[`archive/OPEN_ISSUES_ARCHIVE.md`](archive/OPEN_ISSUES_ARCHIVE.md) (grep only).

- **New defect:** a fragment in [`open_issues.d/`](open_issues.d/README.md), never a row here;
  `python scripts/collect_open_issues.py --apply` gives it the next ID and folds it in.
- **Fixed:** set its status to `✅ Fixed (YYYY-MM-DD, <sha>)`, then
  `python scripts/archive_closed_issues.py --apply` moves the row and its details to the archive.
- **Rewrite:** edit the row here and its section in `ISSUE_DETAILS.md`.
- **Cite** an issue by its ID (`GS-12`), never by line number.
- **Status:** ❌ Open (or partly fixed) · ⏳ needs a check (live server, real data, the owner) ·
  ⏸ deferred. The Summary is generated; CI fails when it, an order or a heading drifts
  (`python scripts/collect_open_issues.py --check`; `--apply` repairs it).

## Summary

| Area | P1 | P2 | P3 | Total |
|------|------|------|------|------|
| Search | 0 | 4 | 21 | 25 |
| Browse and viewer | 0 | 4 | 5 | 9 |
| Desktop app | 0 | 9 | 25 | 34 |
| Website | 0 | 6 | 22 | 28 |
| API and AI | 0 | 2 | 1 | 3 |
| Discovery | 0 | 2 | 11 | 13 |
| Accounts and community | 0 | 8 | 27 | 35 |
| Data and sidecars | 0 | 5 | 5 | 10 |
| Build, CI and tests | 0 | 0 | 1 | 1 |
| Documentation | 0 | 0 | 3 | 3 |
| Server and operations | 0 | 2 | 1 | 3 |
| **Total** | **0** | **42** | **122** | **164** |

## P1 - Critical

_None open._

## P2 - Medium

### Search

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-1](ISSUE_DETAILS.md#gs-1) | Responsa: on a format-1 index the page-break step still reads every whole-manuscript document (recall of `*word` is fixed) | `shared/responsa_match.py::star_route/star_queries`; `shared/search_engine.py::execute_search` | 2026-10-21 | ❌ Partly fixed |
| [GS-2](ISSUE_DETAILS.md#gs-2) | My Library is not restricted by search-within | `shared/search_engine.py::_query_local_index` | 2026-10-04 | ❌ Open |
| [GS-3](ISSUE_DETAILS.md#gs-3) | A cut-off line-break step stays cut off in combinations | `shared/search_engine.py::_execute_line_break_search` | 2026-10-04 | ❌ Open |
| [GS-4](ISSUE_DETAILS.md#gs-4) | Bare-word search misses a word with a bracket INSIDE it (`ש[לום`) | `shared/search_engine.py::_add_bracket_variants`, the `orig_match_missing` drop in `execute_search` | 2026-09-30 | ❌ Open |

### Browse and viewer

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-5](ISSUE_DETAILS.md#gs-5) | A correction draft follows the reader to the next page: after a page turn in edit mode it is filed under the new page with the old text | `web/pages/browse.py::load_page`, _save_draft_now | 2026-10-21 | ❌ Open |
| [GS-6](ISSUE_DETAILS.md#gs-6) | The reading view can render a V0.7 transcription while labelling it V0.8 — and can pick V0.7 over a fuller V0.8 for the same folio | `web/pages/browse.py:2091,4278`; `shared/search_engine.py:5687,5743` | 2026-08-14 | ❌ Open |
| [GS-7](ISSUE_DETAILS.md#gs-7) | Image failure UX: an NLI metadata fetch failure still shows 'No images available' as if none existed | `desktop/viewers.py:1296-1298`, `genizah_app.py:10819`, `web/static/manuscript_viewer.js:199` | 2026-04-26 | ❌ Open |
| [GS-8](ISSUE_DETAILS.md#gs-8) | H2: Cached image reload only works for Oxford manuscripts when offline; NLI/CUL/JTS/Manchester cached images do not appear from disk cache | `desktop/image_loader.py` (cache key), `genizah_app.py` (browse load path), `desktop/viewers.py` | 2026-04-26 | ❌ Open |

### Desktop app

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-9](ISSUE_DETAILS.md#gs-9) | Desktop Search export, filters and counts see only the loaded rows | `genizah_app.py::export_results`, _collect_sorted_results, load_next_batch | 2026-09-25 | ❌ Open |
| [GS-10](ISSUE_DETAILS.md#gs-10) | The `*` search-hit marker is in-band, so a literal asterisk in source text is still ambiguous | `desktop/widgets/__init__.py::mark_pattern_hits`, `desktop/result_dialog.py::_htmlify` | 2026-09-03 | ❌ Open |
| [GS-11](ISSUE_DETAILS.md#gs-11) | Desktop: a stale `PGPBadgeWorker` can blank the PGP column of a table it never searched | `genizah_app.py::_on_pgp_badges_loaded`, _on_tag_search_results; PGPBadgeWorker | 2026-09-03 | ❌ Open |
| [GS-12](ISSUE_DETAILS.md#gs-12) | Desktop Lab Mode composition silently ignores Focus Search | `genizah_app.py::run_composition` (Lab branch), `desktop/gui_threads.py::LabCompositionThread` | 2026-09-03 | ❌ Open |
| [GS-13](ISSUE_DETAILS.md#gs-13) | Desktop: Focus Search is still ONE scope shared by the Search tab and the Composition tab | `genizah_app.py::_open_pre_search_filter_dialog`, pre_search_restrict_sys_ids | 2026-09-03 | ❌ Open |
| [GS-14](ISSUE_DETAILS.md#gs-14) | Stop blocks the UI thread and can still hard-kill a worker | `genizah_app.py::stop_search` (+5 sibling stop paths) | 2026-08-19 | ❌ Open |
| [GS-15](ISSUE_DETAILS.md#gs-15) | `btn_comp_run` renames itself after the first run | `genizah_app.py:5399` vs `:21532`, `:22373` | 2026-08-19 | ❌ Open |
| [GS-16](ISSUE_DETAILS.md#gs-16) | A disabled button with an inline `background-color` does not look disabled | `genizah_app.py:4598-4599` (`btn_search`), repo-wide pattern | 2026-08-19 | ❌ Open |
| [GS-17](ISSUE_DETAILS.md#gs-17) | Desktop Browse tab: three toolbars reported dark in bright mode; the named cause is no longer found in the source | `genizah_app.py` (Browse-tab toolbar construction) | 2026-05-18 | ⏳ Needs a check |

### Website

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-18](ISSUE_DETAILS.md#gs-18) | Visual Similarity / Joins Lab thumbnails of manuscripts nobody opened come up blank, and different ones on each redraw | `web/api.py::fetch_fl_ids_from_nli`, `web/nli_image_store.py` | 2026-10-21 | ❌ Partly fixed |
| [GS-19](ISSUE_DETAILS.md#gs-19) | /browse page change still rebuilds the whole content area in Phase B to fill its placeholders | `web/pages/browse_enrichment.py::load_enrichment`, `web/pages/browse.py::update_content` | 2026-10-21 | ❌ Partly fixed |
| [GS-20](ISSUE_DETAILS.md#gs-20) | Website restart can set off reload loops from open pages; backoff shipped 2026-10-21, saturation case and live check remain | `web/static/js/reconnect_backoff.js`, `web/reconnect_backoff.py`, `web/framework_patches.py:540-625` | 2026-10-21 | ❌ Partly fixed |
| [GS-21](ISSUE_DETAILS.md#gs-21) | Parallels Word export still strips highlight markers while xlsx now renders them | `web/export_service.py` (`export_parallels_word`) | 2026-08-23 | ❌ Open |
| [GS-22](ISSUE_DETAILS.md#gs-22) | Cloudflare caches no page/API response: HTML, robots, sitemap, manifest still get a re-signed Set-Cookie; assets and image routes fixed | `web/framework_patches.py:96-124,166-189` | 2026-07-30 | ❌ Partly fixed |
| [GS-23](ISSUE_DETAILS.md#gs-23) | Web Joins Lab grid thumbnails for CUL/Manchester/JTS go to the NLI proxy until the breaker opens (3 failures); proxy has no provider fallbac | `web/components/candidate_grid.py:484-547`, `web/api.py:1126` | 2026-06-21 | ❌ Open |

### API and AI

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-24](ISSUE_DETAILS.md#gs-24) | method='passage' single-text parallels exceeds its 30 s ceiling on 51-100 char inputs and returns no hits through the public endpoint | `shared/passage_search.py`, `shared/passage_policy.py`, `web/search_api.py` | 2026-09-08 | ⏳ Needs a check |
| [GS-25](ISSUE_DETAILS.md#gs-25) | Multi-witness passage parallels is non-deterministic in latency: the identical payload returned 198 results once, then nothing within 120 s | `shared/passage_parallels.py::PassageSearcher`, `web/search_api.py` witnesses path | 2026-09-08 | ⏳ Needs a check |

### Discovery

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-26](ISSUE_DETAILS.md#gs-26) | The discovery matcher never searched the HTR text of the 18,982 corpus pages whose search text was swapped for an FGP/PGP transcription | `same_work_spike/.../mapv2_stage0.py`, `scripts/attach_htr_realignment.py` | 2026-09-01 | ❌ Open |
| [GS-27](ISSUE_DETAILS.md#gs-27) | `/computed-identifications` costs ~2 s and only ~0.9 s is accounted for | `web/pages/findings.py:1172` (`_fetch_approved_review_map`), `:1312`, `:2246-2283` (facet cascade) | 2026-08-19 | ❌ Open |

### Accounts and community

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-28](ISSUE_DETAILS.md#gs-28) | List sync: an edit made during an upload can settle differently from the same edit made just after it, losing a website entry | `shared/lists_sync.py`; `tests/list_sync_scenarios.py` (invariant 14) | 2026-10-25 | ❌ Open |
| [GS-29](ISSUE_DETAILS.md#gs-29) | Web: replies to community discoveries fail, because `discovery_responses` does not exist in production | `web/supabase_client.py::get_discovery_responses/create_discovery_response`, `web/pages/discoveries.py` | 2026-10-21 | ❌ Open |
| [GS-30](ISSUE_DETAILS.md#gs-30) | Hardening: mask the author of anonymous discoveries and responses in the data, not only on screen | `supabase_setup.sql` (discoveries), `web/supabase_client.py::get_discovery_responses` | 2026-10-20 | ❌ Open |
| [GS-31](ISSUE_DETAILS.md#gs-31) | Hardening: give the documented `published_joins` admin UPDATE policy a real `WITH CHECK` | `docs/guides/SUPABASE_GUIDE.md` (the published_joins policies; no tracked DDL) | 2026-10-20 | ❌ Open |
| [GS-32](ISSUE_DETAILS.md#gs-32) | Desktop discovery responses are stubs | `desktop/supabase_corrections_client.py::add_discovery_response/get_discovery_responses` | 2026-09-25 | ❌ Open |
| [GS-33](ISSUE_DETAILS.md#gs-33) | Admins cannot see or un-hide another user's hidden discovery, and both apps report success | `supabase_setup.sql` discoveries SELECT policy; desktop unhide_discovery | 2026-09-25 | ❌ Open |
| [GS-34](ISSUE_DETAILS.md#gs-34) | The `/corrections` Review tab only works for admins: reviewers and editors cannot approve or reject someone else's correction | `web/pages/corrections.py` (Review tab), `supabase_setup.sql` corrections UPDATE policy | 2026-09-25 | ❌ Open |
| [GS-35](ISSUE_DETAILS.md#gs-35) | A resubmitted identification review replaces the published one: it disappears until approved again, and the earlier text is lost | `scripts/create_identification_reviews_beta.sql:201-217` | 2026-09-25 | ❌ Open |

### Data and sidecars

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-276](ISSUE_DETAILS.md#gs-276) | 1,810 FGP transcriptions show final letters as control characters | `fgp_data/inventory/fgp_transcriptions.candidate.db` (rows with `text_source` `pdf_rtl` / `original`); FGP text pipeline, gate 3 | 2026-10-27 | ❌ Open |
| [GS-268](ISSUE_DETAILS.md#gs-268) | FGP text drops the part of a line before a gershayim | `fgp_data/fgp_transcriptions.db` (`text_source='pdf_rtl'`), `shared/local_indexer.py::extract_pdf_pages`, `scripts/fgp_rebuild_text_and_credit.py` | 2026-10-25 | ❌ Open |
| [GS-269](ISSUE_DETAILS.md#gs-269) | FGP visual XML is applied by image number, not by source record | `scripts/fgp_rebuild_text_and_credit.py::build_xml_index`, `fgp_data/fgp_transcriptions.db` (`text_source='xml'`) | 2026-10-25 | ❌ Open |
| [GS-270](ISSUE_DETAILS.md#gs-270) | FGP credit heuristic disagrees with FIST on 1,551 / 6,151 files | `scripts/fgp_fill_credits_bilingual.py` | 2026-10-25 | ❌ Open |
| [GS-36](ISSUE_DETAILS.md#gs-36) | V0.8 corpus: cross-manuscript misattribution upstream; ~6% V0.7-only manuscripts absent from production (by owner decision) | `Transcriptions.txt` ingest (upstream); `data/v08_repair_manifest.json` | 2026-08-16 | ❌ Partly fixed |

### Server and operations

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-37](ISSUE_DETAILS.md#gs-37) | deploy.sh runs git reset --hard before pip, so a failed pip leaves new code on disk under the old process | `deploy.sh` | 2026-10-20 | ❌ Partly fixed |
| [GS-38](ISSUE_DETAILS.md#gs-38) | A tracked planning deploy log quotes the backup password of the retired local Postgres, in a public repo | a `.planning` deploy log (the path is in the 2026-10-20 review notes, not repeated here) | 2026-10-20 | ❌ Open |

## P3 - Low Priority

### Search

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-39](ISSUE_DETAILS.md#gs-39) | Responsa: a star word holding Hebrew punctuation is not refused | `shared/responsa_match.py::_TOKEN_RE`, `refuse_unsupported_stars` | 2026-10-23 | ❌ Open |
| [GS-40](ISSUE_DETAILS.md#gs-40) | Responsa: a bracket group holding only a quote between two words blocks the sequence | `shared/responsa_match.py::_separator` | 2026-10-23 | ❌ Open |
| [GS-41](ISSUE_DETAILS.md#gs-41) | Responsa: a My Library-only search's downgrade carries no effective options | `shared/search_engine.py` | 2026-10-22 | ❌ Open |
| [GS-42](ISSUE_DETAILS.md#gs-42) | Regex mode `מאמ\w* קור\w*` returned 0 rows on the real index | `shared/regex_prefilter.py::build_candidate_query`; `shared/search_engine.py::_regex_candidate_query` | 2026-10-22 | ⏳ Needs a check |
| [GS-43](ISSUE_DETAILS.md#gs-43) | A page filed under several manuscripts is shown under whichever copy the index returns first; a rebuild changes manuscript counts | `shared/search_engine.py::_deduplicate` | 2026-10-22 | ❌ Open |
| [GS-44](ISSUE_DETAILS.md#gs-44) | Exact/Variants/Fuzzy over whole manuscripts: a crossing from an earlier page can hide the next page's own | `shared/search_engine.py::_cross_page_spans` (the break loop, without `page_start`) | 2026-10-22 | ❌ Open |
| [GS-45](ISSUE_DETAILS.md#gs-45) | Exact (whole words) misses a phrase with four or more stray tokens between its words | `shared/search_engine.py::build_phrase_candidate_query` (`_PHRASE_EXTRA_SLOP = 3`) | 2026-10-21 | ❌ Open |
| [GS-46](ISSUE_DETAILS.md#gs-46) | Exact (`literal`) never finds a whole-word token outside its bracket variants (`אב[[`, `א'ב'`) | `shared/search_engine.py::_add_bracket_variants` | 2026-10-20 | ❌ Open |
| [GS-47](ISSUE_DETAILS.md#gs-47) | Shelfmark resolver: `normalize_shelfmark` glues numbers across `=`, so `… Ms. Heb. 577.10.6=04` resolves to `577.10.60=4` | `shared/browse_map_utils.py::normalize_shelfmark`, `metadata_manager.py::resolve_system_by_shelfmark` | 2026-10-18 | ❌ Open |
| [GS-48](ISSUE_DETAILS.md#gs-48) | Shelfmark resolver: a named library with nothing matching keeps today's answer, which can be another library's record | `shared/catalog_shelfmark.py::named_library_exact`, resolve_many | 2026-10-18 | ❌ Open |
| [GS-49](ISSUE_DETAILS.md#gs-49) | Shelfmark resolver: T1's dotless rule and number gluing keep other shelfmarks inside a named library | `shared/catalog_shelfmark.py::named_library_exact`, `metadata_manager.py::resolve_system_by_shelfmark` | 2026-10-18 | ❌ Open |
| [GS-50](ISSUE_DETAILS.md#gs-50) | Shelfmark resolver: `Gaster Ms. 1201.17` does not offer the stub that stores `GASTER 1201.17` | `shared/catalog_shelfmark.py::named_library_exact` (whole-input key) | 2026-10-18 | ❌ Open |
| [GS-51](ISSUE_DETAILS.md#gs-51) | Catalogue-form fallback (T2): the numeric check of its older keys sees only the variant the index kept | `shared/catalog_shelfmark.py::resolve_many` (`fits`, keys `a`/`b`/`r`/`c`/`d0`) | 2026-10-18 | ❌ Open |
| [GS-52](ISSUE_DETAILS.md#gs-52) | The shelfmark index is never rebuilt when NLI-cache shelfmarks arrive after its first build | `shared/metadata_manager.py::_get_shelfmark_index` | 2026-10-18 | ❌ Open |
| [GS-53](ISSUE_DETAILS.md#gs-53) | A per-call `regex` timeout aborts the whole desktop search instead of cutting that document off | `shared/search_engine.py::execute_search` per-hit handlers, `desktop/gui_threads.py::SearchThread.run` | 2026-10-12 | ❌ Open |
| [GS-54](ISSUE_DETAILS.md#gs-54) | A letter-level search the website stopped is shown badly on /parallels | `web/pages/parallels.py` (passage dispatch); `shared/passage_parallels.py::search_composition_logic` | 2026-10-07 | ❌ Open |
| [GS-55](ISSUE_DETAILS.md#gs-55) | Numbered-line syntax (`L1:word`) misses near spellings in Fuzzy and Variants | `shared/search_engine.py::execute_search` (L{n}: block), `shared/indexer.py::_validate_position_match` | 2026-10-04 | ❌ Open |
| [GS-56](ISSUE_DETAILS.md#gs-56) | A failed measurement lookup after a search still hides every row | `shared/fjms_service.py::get_measurement_summaries_batch`; `web/pages/search.py:5222`; `genizah_app.py` | 2026-09-28 | ❌ Open |
| [GS-57](ISSUE_DETAILS.md#gs-57) | One Regex pattern can match differently on the desktop and on the website | `shared/search_regex.py` (in-process `regex` module; website workers use stdlib `re`) | 2026-09-28 | ❌ Open |
| [GS-58](ISSUE_DETAILS.md#gs-58) | Lab Mode ignores Regex mode | `shared/lab_engine.py::lab_search`; `web/pages/search.py`; `desktop/gui_threads.py::LabSearchThread` | 2026-09-28 | ❌ Open |
| [GS-59](ISSUE_DETAILS.md#gs-59) | A query starting with `R` is run as a Responsa search in the other modes | `shared/search_engine.py::parse_query_syntax`; `genizah_app.py:23088`; `web/pages/search.py:2571` | 2026-09-25 | ❌ Open |

### Browse and viewer

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-61](ISSUE_DETAILS.md#gs-61) | Browse: a paged deep-link URL still carries `highlight`, so a reload lets the NEW folio claim the search phrase | `web/pages/browse.py::_update_browser_url` (877-906), _search_scope_phrase (835-859) | 2026-09-03 | ❌ Open |
| [GS-62](ISSUE_DETAILS.md#gs-62) | `switchImageCredit()` updates the FIRST `data-role="image-credit"` node in the document — a hidden earlier Advanced View, not the open one | `web/static/manuscript_viewer.js::switchImageCredit`; `search_results.py::open_advanced_dialog` | 2026-09-03 | ❌ Open |
| [GS-63](ISSUE_DETAILS.md#gs-63) | FGP version menu still lists whole-document rows unrelated to the displayed folio (demoted from the default, not hidden) | `shared/fgp_service.py::choose_default_source`, _select_fgp_editions_by_similarity | 2026-09-02 | ❌ Open |
| [GS-64](ISSUE_DETAILS.md#gs-64) | FGP alignment can land on an adjacent folio for Toronto `990001996710205171` | `shared/fgp_service.py::_select_fgp_editions_by_similarity`, _content_similarity | 2026-06-22 | ❌ Open |
| [GS-65](ISSUE_DETAILS.md#gs-65) | The displayed image does not match the V0.8 / FGP text on some manuscripts (NLI `990035100760205171`, Columbia `990001372620205171`) | `web/api.py::fetch_fl_ids_from_nli`; desktop fl_ids by position | 2026-06-22 | ❌ Open |

### Desktop app

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-66](ISSUE_DETAILS.md#gs-66) | Desktop image cache: a 400 px thumbnail and the 2000 px viewer image of the same page share one file | `desktop/image_loader.py::ImageLoaderThread.run` (line 68); metadata_manager._resolve_thumbnail | 2026-10-21 | ❌ Open |
| [GS-67](ISSUE_DETAILS.md#gs-67) | Quit still waits, at interpreter exit, for an NLI request in flight on the shared metadata executor | `shared/metadata_manager.py` (nli_executor, line 289); `genizah_app.py::_stop_meta_loader_for_close` | 2026-10-20 | ❌ Open |
| [GS-68](ISSUE_DETAILS.md#gs-68) | Desktop Excel export: a search-within whose completion step Stop interrupted exports the plain count | `genizah_app.py::export_results` (`_run_left_matches_out()`), the refinement chain | 2026-10-20 | ❌ Open |
| [GS-69](ISSUE_DETAILS.md#gs-69) | The Quoted texts dialog (old Filter Text) is wider than a small screen at 300% scaling | `desktop/filter_text_dialog.py::FilterTextDialog` (the Sefaria buttons row and the intro label) | 2026-10-14 | ❌ Open |
| [GS-70](ISSUE_DETAILS.md#gs-70) | Desktop viewer runs a scholar's own Regex highlight pattern through plain `re` on the UI thread with no bound | `desktop/widgets/__init__.py::mark_pattern_hits`, `desktop/result_dialog.py`, `genizah_app.py` | 2026-10-12 | ❌ Open |
| [GS-71](ISSUE_DETAILS.md#gs-71) | Desktop Joins Lab searches at the main window's pair count | `desktop/variant_run_settings.py::basic_changes_searcher`, `desktop/join_workbench.py` | 2026-10-07 | ❌ Open |
| [GS-72](ISSUE_DETAILS.md#gs-72) | Desktop small gaps: no copy confirmation, lists/manuscript saves ignore last folder, a few strings lack Hebrew, no viewer paging keys | `genizah_app.py`, `desktop/result_dialog.py`, `shared/genizah_translations.py` | 2026-10-06 | ❌ Partly fixed |
| [GS-73](ISSUE_DETAILS.md#gs-73) | My Library: a cut-off search with no matches offers the Genizah corpus as if My Library had none | `genizah_app.py::on_search_finished` (24157), `::_update_local_scope_strip` (22964) | 2026-10-06 | ❌ Open |
| [GS-74](ISSUE_DETAILS.md#gs-74) | Desktop: starting a shelfmark fetch while one runs blocks the window until the running request ends | `genizah_app.py::start_metadata_loading`, `shared/metadata_manager.py::batch_fetch_shelfmarks` | 2026-10-04 | ❌ Open |
| [GS-75](ISSUE_DETAILS.md#gs-75) | Fragment Puzzle follow-ups after the 2026-09-27 work-loss fix | `desktop/puzzle.py`, `desktop/corrections_ui.py`, `web/pages/puzzle.py` | 2026-09-27 | ❌ Open |
| [GS-76](ISSUE_DETAILS.md#gs-76) | Joins Lab and main-window threads not covered by the 2026-09-27 lifetime fix | `genizah_app.py`, `desktop/join_workbench.py`, `desktop/image_loader.py`, `desktop/viewers.py` | 2026-09-27 | ❌ Open |
| [GS-77](ISSUE_DETAILS.md#gs-77) | `is_logged_in()` is a network request on the UI thread | `desktop/supabase_corrections_client.py::is_logged_in`; `genizah_app.py::_update_corner_login_state` | 2026-09-27 | ❌ Open |
| [GS-78](ISSUE_DETAILS.md#gs-78) | Community-panel refreshes still make network calls on the UI thread | `genizah_app.py::_refresh_community_panels` | 2026-09-27 | ❌ Open |
| [GS-79](ISSUE_DETAILS.md#gs-79) | After a log-out a panel with nothing cached says "Offline - no cached data available" | `genizah_app.py::_refresh_community_panels` (`cached_only`) | 2026-09-27 | ❌ Open |
| [GS-80](ISSUE_DETAILS.md#gs-80) | The "already in sync" check at log-in ignores whose cloud ids the lists hold | `genizah_app.py::_on_lists_preview` | 2026-09-27 | ❌ Open |
| [GS-81](ISSUE_DETAILS.md#gs-81) | "Notes that differ" also counts entries whose tags differ | `genizah_app.py::_sync_note_lines`, `shared/lists_sync.py::count_differing_notes` | 2026-09-27 | ❌ Open |
| [GS-82](ISSUE_DETAILS.md#gs-82) | The desktop note field is one line, so a note holding both versions shows as one | `genizah_app.py` (`lists_detail_note`, a `QLineEdit`) | 2026-09-27 | ❌ Open |
| [GS-83](ISSUE_DETAILS.md#gs-83) | `joins_cache.pkl` and `lab/lab_config.json` are written in place | `shared/joins_manager.py`, `shared/lab_settings.py` | 2026-09-25 | ❌ Open |
| [GS-84](ISSUE_DETAILS.md#gs-84) | After the all-terms rerender, scrolling adds rows the all-terms filter excluded | `genizah_app.py::_apply_all_terms_filter_and_rerender` | 2026-09-25 | ❌ Open |
| [GS-85](ISSUE_DETAILS.md#gs-85) | Search-tab `excluded_shelfmarks` are never applied | `genizah_app.py` (Search Exclude Manuscripts) | 2026-09-25 | ❌ Open |
| [GS-86](ISSUE_DETAILS.md#gs-86) | New pressed during the startup restore is undone by the restore | `genizah_app.py::_restore_session` (`processEvents` calls) | 2026-09-25 | ❌ Open |
| [GS-87](ISSUE_DETAILS.md#gs-87) | The installer's language choice is ignored: a fresh install opens in English | `CompileScriptGenizah.iss` ([Languages], [Run]), `genizah_core.py::load_language` | 2026-09-25 | ❌ Open |
| [GS-88](ISSUE_DETAILS.md#gs-88) | Update download: Cancel can wait up to 600 s, closing leaves the download running, and a cancel is reported as a failure | `desktop/gui_threads.py::UpdateDownloaderThread`; `desktop/update_ui.py` (closeEvent, execute_update) | 2026-09-25 | ❌ Open |
| [GS-89](ISSUE_DETAILS.md#gs-89) | "Update Research Data" leaves enrichment on the old databases until restart | `shared/metadata_manager.py` (_fjms_svc, _nli_crossref_svc), `genizah_app.py:33905` | 2026-09-25 | ❌ Open |
| [GS-90](ISSUE_DETAILS.md#gs-90) | Manuscript Viewer compact mode has no image or text toggle | `desktop/result_dialog.py` (`compact_actions`, `btn_toggle_image`, `btn_toggle_text`) | 2026-09-24 | ❌ Open |

### Website

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-273](ISSUE_DETAILS.md#gs-273) | The reading desk saves a source choice it never applies, keyed by list position | `web/pages/browse.py` (reading-desk version `ui.select`, `state.reading_desk_selected_sources`), `web/pages/browse_state.py` | 2026-10-26 | ❌ Open |
| [GS-91](ISSUE_DETAILS.md#gs-91) | Website: Back to a search-within or visual-similarity search whose run is no longer kept runs it over the whole corpus | `web/pages/search.py::_search_url` (5248), page start elif initial_query (6547); `web/search_runs.py` | 2026-10-22 | ❌ Open |
| [GS-92](ISSUE_DETAILS.md#gs-92) | Website: a search still preparing can be evicted from the run registry before finished ones | `web/search_runs.py` (`_prune`, `_oldest_first_removable`) | 2026-10-22 | ❌ Open |
| [GS-93](ISSUE_DETAILS.md#gs-93) | Website: Back to a search shows its rows without the Filter Results panel's constraints | `web/pages/search.py` (Filter Results inputs); `web/pages/search_state.py::_post_filters` | 2026-10-22 | ❌ Open |
| [GS-94](ISSUE_DETAILS.md#gs-94) | Supabase calls still on the event loop: /corrections save/approve/reject, add-all list create, comment picker, edit-mode read | `web/pages/corrections.py`; `search.py:3414`; `comment_dialog.py:102`; `browse.py:2007` | 2026-10-21 | ❌ Open |
| [GS-95](ISSUE_DETAILS.md#gs-95) | Responsa downgrade warnings follow the server process's language, not the visitor's | `shared/search_engine.py::_tr`; `web/translations.py::tr_engine_message`; `web/pages/search.py:2848` | 2026-10-20 | ❌ Partly fixed |
| [GS-96](ISSUE_DETAILS.md#gs-96) | A 404 under the running `/_nicegui/<version>/` prefix is sent `Cache-Control: public, max-age=31536000, immutable` | NiceGUI's `SetCacheControlMiddleware` | 2026-10-20 | ❌ Open |
| [GS-97](ISSUE_DETAILS.md#gs-97) | Hebrew pages shift after first paint (CLS p50 ~0.84 on / and /search); the server HTML always says `lang="he"` | `web/main.py::apply_theme_immediately`, `web/framework_patches.py::_patch_html_lang_attribute` | 2026-10-20 | ❌ Open |
| [GS-98](ISSUE_DETAILS.md#gs-98) | Web Fragment Puzzle: two tabs on one draft -- the last auto-save wins | `web/pages/puzzle.py::_auto_save_now` | 2026-10-20 | ❌ Open |
| [GS-99](ISSUE_DETAILS.md#gs-99) | Dead lookup: `state.meta_mgr._shelf_to_sys` in the web Puzzle's join loader | `web/pages/puzzle.py` (fragments fallback in the joins loader) | 2026-10-18 | ❌ Open |
| [GS-100](ISSUE_DETAILS.md#gs-100) | Visual Similarity window: each look-alike keeps only its first domain | `web/components/visual_similarity_dialog.py::show_visual_similarity_dialog` (`_enrich`) | 2026-10-18 | ❌ Open |
| [GS-101](ISSUE_DETAILS.md#gs-101) | Website full-text highlighting compiles the pattern on the event loop under a 0.25 s budget a Maximum-size pattern cannot meet | `web/pages/search_results.py::highlight_full_text_html/_render_full_text`, `search_regex.py::compile` | 2026-10-12 | ❌ Open |
| [GS-102](ISSUE_DETAILS.md#gs-102) | Website: Num Changes shows the saved mode's value after a link to another mode | `web/pages/search.py` (Num Changes initial value, `resolve_search_bootstrap`) | 2026-10-07 | ❌ Open |
| [GS-103](ISSUE_DETAILS.md#gs-103) | Website: a completed search-within step is saved without re-checking that its chain is still the page's | `web/pages/search.py::_complete_chain`, _enter_refine_mode | 2026-10-07 | ❌ Open |
| [GS-104](ISSUE_DETAILS.md#gs-104) | Web small gaps: 0-result 'Ready to search' and stale zero-result flag, 700px Sefaria dialog, sidebar saved closed, My Edits vote stub | `web/pages/search_results.py:358`; `search.py::_clear_refinement_chain`; `parallels.py:4161`; `main.py:2020` | 2026-10-06 | ❌ Partly fixed |
| [GS-105](ISSUE_DETAILS.md#gs-105) | Web accessibility: icon-only buttons without a name, and an Atlas without keyboard access | `web/` (icon buttons), `web/static/js/atlas_decode.js` | 2026-10-06 | ❌ Open |
| [GS-106](ISSUE_DETAILS.md#gs-106) | Search workers' free-memory reading counts page cache as used under a cgroup memory limit | `shared/research_limits.py::_cgroup_available_memory`, `web/research_jobs.py` | 2026-10-05 | ❌ Open |
| [GS-107](ISSUE_DETAILS.md#gs-107) | Puzzle: Manchester fragments often show without background removal | `web/api.py:1368` (manchester_image), :1964 (_fetch_provider_image), :1982 (puzzle_ext_image) | 2026-09-29 | ❌ Open |
| [GS-108](ISSUE_DETAILS.md#gs-108) | Web `/lists`: unpaged list reads, and "Add page to list" leaves `page` empty | `web/supabase_client.py::get_list_items`, `web/pages/browse.py` (Add page to list) | 2026-09-27 | ❌ Open |
| [GS-109](ISSUE_DETAILS.md#gs-109) | Copy may be refused on Safari/iOS: the clipboard write runs after a server round trip, outside the click's user activation | `web/clipboard.py::copy_text_to_clipboard` and its callers | 2026-09-25 | ⏳ Needs a check |
| [GS-110](ISSUE_DETAILS.md#gs-110) | Bulk "Copy Selected Text" fetches each result's full text synchronously on the event loop | `web/pages/search.py::bulk_copy_text` (`state.searcher.get_full_text_by_id`) | 2026-09-25 | ❌ Open |
| [GS-111](ISSUE_DETAILS.md#gs-111) | Non-NLI image/manifest fetches (CUDL, Manchester, JTS, external IIIF) have no circuit breaker and keep long timeouts | `web/api.py:1191/1259/1316`, `genizah_core.py:4626` | 2026-05-25 | ❌ Open |

### API and AI

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-112](ISSUE_DETAILS.md#gs-112) | Per-IP limits will key IPv6 visitors per address (/128) once the realip snippet is installed | `web/api_hardening.py::_resolve_rate_limit_key`, the puzzle limiter in `web/api.py` | 2026-10-07 | ❌ Open |

### Discovery

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-113](ISSUE_DETAILS.md#gs-113) | A span lying wholly inside a work's preamble is addressed as unit 0 with zero or negative overlap | `shared/discovery_locus.py::units_for_span`, `::_floor_index` | 2026-08-12 | ❌ Open |
| [GS-114](ISSUE_DETAILS.md#gs-114) | Judeo-Arabic locus labels run to 100 characters; the only safe fix (work-uniform colon-trim) is not built | locus_unit.label_he (live artifact); `shared/discovery_locus.py`, `scripts/build_discovery_sidecar.py` | 2026-08-10 | ❌ Open |
| [GS-115](ISSUE_DETAILS.md#gs-115) | Multi-span rows (7.0% of addressable rows) are baked with one window, so citations can be incomplete (card 76); WP4 still deferred | `scripts/build_discovery_sidecar.py::project_ref_span`; `shared/discovery_locus.py` | 2026-08-10 | ❌ Open |
| [GS-116](ISSUE_DETAILS.md#gs-116) | Judeo-Arabic supplement headings show visually-ordered (reversed) shelfmark sigla as addresses; owner decision pending | locus_unit.label_he for w000071 units 367-370 (live artifact) | 2026-08-10 | ❌ Open |
| [GS-117](ISSUE_DETAILS.md#gs-117) | Public projection re-materializes identifications from public rows only, so pruning a tie competitor can promote rows into the main pool | `scripts/project_discovery_public.py::_materialize_public_identification` (723-766) | 2026-08-10 | ❌ Open |
| [GS-118](ISSUE_DETAILS.md#gs-118) | The canonical-text mask file predates the corpus it covers, so 247+ Sefaria (REF2:sef_*) works have no masking | `same_work_spike/probe/data/ref_canon_masks_v2.json`; `scripts/discovery_v4_extend_masks.py` | 2026-08-09 | ❌ Open |
| [GS-119](ISSUE_DETAILS.md#gs-119) | 725 'confirms' identifications rest on a per-work attestation about OTHER manuscripts, attached even when this one is not a witness | `scripts/discovery_novelty_probe.py:376-386`; `scripts/emit_work_witnesses.py:129-132` | 2026-08-09 | ❌ Open |
| [GS-120](ISSUE_DETAILS.md#gs-120) | Help methods page reads a non-existent draw_size column, so every graded band shows 'not yet measured' sample figures | `web/pages/help.py:508-510` (_render_one_band); band_precision DDL | 2026-08-09 | ❌ Open |
| [GS-121](ISSUE_DETAILS.md#gs-121) | Two JA works (w001151, w000065) still carry Latin-script author strings in the public sidecar and split the author facet | works.author in the public artifact; `scripts/attach_author_authority.py` OWNER_AUTHOR_RULINGS | 2026-08-04 | ❌ Open |
| [GS-122](ISSUE_DETAILS.md#gs-122) | w000160 (Arugat HaBosem) carries claims for two different works under one title/author; domain ruling does not settle which | `discovery-v5-REVIEW.db` review_row (w000160); `scripts/curate_work_domains.py:1711` | 2026-08-03 | ❌ Open |
| [GS-123](ISSUE_DETAILS.md#gs-123) | w000154, a 19th-c. maskilic memoir, sits in the discovery corpus; corpus membership is unresolved | `discovery-v5-REVIEW.db` review_row (w000154); `scripts/curate_work_domains.py:1431,1721` | 2026-08-03 | ❌ Open |

### Accounts and community

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-124](ISSUE_DETAILS.md#gs-124) | Web: no way to ask for a new confirmation email | `web/auth_state.py` (`create_login_dialog`; the "not confirmed yet" sign-in error) | 2026-10-21 | ❌ Open |
| [GS-125](ISSUE_DETAILS.md#gs-125) | Hardening follow-up: `profiles` columns no page needs are readable anonymously, and whether a Google account's own name is shown | `supabase_setup.sql` (profiles SELECT grants); every profiles select('*') | 2026-10-21 | ❌ Open |
| [GS-126](ISSUE_DETAILS.md#gs-126) | Closing while an upload request is unanswered can bring a removed or moved entry back later | `desktop/lists_sync_runner.py::shutdown`, `shared/lists_manager.py::abandon_upload` | 2026-09-27 | ❌ Open |
| [GS-127](ISSUE_DETAILS.md#gs-127) | An entry removed at the moment the upload sends it is still inserted or moved | `shared/lists_manager.py` (`withdrawn_now`, `finish_upload`) | 2026-09-27 | ❌ Open |
| [GS-128](ISSUE_DETAILS.md#gs-128) | A killed process loses what the running upload established | `shared/lists_manager.py` (`begin_upload` to `finish_upload`) | 2026-09-27 | ❌ Open |
| [GS-129](ISSUE_DETAILS.md#gs-129) | An account switch drops the other account's pending moves | `shared/lists_sync.py::apply_account_guard` | 2026-09-27 | ❌ Open |
| [GS-130](ISSUE_DETAILS.md#gs-130) | While lists.pkl cannot be saved, a removal can reach the website but not the file | `shared/lists_manager.py::save`, `desktop/lists_sync_runner.py` | 2026-09-27 | ❌ Open |
| [GS-131](ISSUE_DETAILS.md#gs-131) | One hung request holds the list-sync queue for up to 120 s | `desktop/lists_sync_runner.py` | 2026-09-27 | ❌ Open |
| [GS-132](ISSUE_DETAILS.md#gs-132) | After Logout the account stays logged in for up to 10.5 s | `genizah_app.py::_do_logout` (`LOGOUT_SYNC_BUDGET_S`) | 2026-09-27 | ❌ Open |
| [GS-133](ISSUE_DETAILS.md#gs-133) | A moved entry's old row that the website edited waits for a Download or Merge | `shared/lists_sync.py::_settle_moved_row` | 2026-09-27 | ❌ Open |
| [GS-134](ISSUE_DETAILS.md#gs-134) | A move into a list in the Trash waits until Restore | `shared/lists_sync.py::_settle_moved_row` (`deferred`) | 2026-09-27 | ❌ Open |
| [GS-135](ISSUE_DETAILS.md#gs-135) | A list deleted for good here stays on the website, empty | `shared/lists_sync.py` (list-level sync) | 2026-09-27 | ❌ Open |
| [GS-136](ISSUE_DETAILS.md#gs-136) | A Download can put back an entry waiting for its delete one sync early | `shared/lists_sync.py::_suppressed` | 2026-09-27 | ❌ Open |
| [GS-137](ISSUE_DETAILS.md#gs-137) | Putting an entry back while its removal is being sent still deletes the website copy | `shared/lists_manager.py::_restore_cloud_row` | 2026-09-27 | ❌ Open |
| [GS-138](ISSUE_DETAILS.md#gs-138) | A list Move becomes a copy in two cases | `shared/lists_sync.py` | 2026-09-27 | ❌ Open |
| [GS-139](ISSUE_DETAILS.md#gs-139) | Duplicate list names stay duplicated on the website | `shared/lists_manager.py::merge_duplicate_group`, `shared/lists_sync.py::_map_lists` | 2026-09-27 | ❌ Open |
| [GS-140](ISSUE_DETAILS.md#gs-140) | Rows the desktop never records come back, marked, after their text is deleted here | `shared/lists_sync.py::_reconcile` | 2026-09-27 | ❌ Open |
| [GS-141](ISSUE_DETAILS.md#gs-141) | A cloud note or tag set too long for the URL limit cannot be changed from the desktop | `shared/lists_sync.py::_patch` | 2026-09-27 | ❌ Open |
| [GS-142](ISSUE_DETAILS.md#gs-142) | Page entries without a folio id uploaded before 2b-1 appear twice after the first sync | `shared/lists_sync.py` (matching), `migrations/add_list_item_page_column.sql` | 2026-09-27 | ❌ Open |
| [GS-143](ISSUE_DETAILS.md#gs-143) | A repeated Merge can still change a row's note or an entry's record (two rare seeds) | `shared/lists_sync.py`, `tests/list_sync_scenarios.py` | 2026-09-27 | ⏳ Needs a check |
| [GS-144](ISSUE_DETAILS.md#gs-144) | Lists renamed before 2b-1 lose or keep the wrong name | `shared/lists_manager.py::update_list`, `shared/lists_sync.py::_map_lists` | 2026-09-27 | ❌ Open |
| [GS-145](ISSUE_DETAILS.md#gs-145) | Desktop discovery authors cannot edit or delete their own discovery | `desktop/supabase_corrections_client.py:1392` (_parse_discovery), :779; `desktop/corrections_ui.py:1606` | 2026-09-25 | ❌ Open |
| [GS-146](ISSUE_DETAILS.md#gs-146) | Web /corrections shows the raw English scope badge | `web/pages/corrections.py` (comment card badge) | 2026-09-25 | ❌ Open |
| [GS-147](ISSUE_DETAILS.md#gs-147) | get_user_client() silently returns the anonymous client when a token refresh fails, so user-scoped reads can look empty | `web/supabase_client.py::get_user_client` (lines 466-475), empty_trash | 2026-09-25 | ❌ Open |
| [GS-148](ISSUE_DETAILS.md#gs-148) | Star icons show as outline for everyone, signed-in users included | `web/user_lists.py:752-765`; `web/components/add_to_list_dialog.py::get_star_icon` | 2026-09-25 | ❌ Open |
| [GS-149](ISSUE_DETAILS.md#gs-149) | Write helpers that still report success when RLS changed 0 rows | `web/supabase_client.py::delete_discovery`; `web/discovery_suppression.py::unsuppress`; `discoveries.py` | 2026-09-25 | ❌ Open |
| [GS-150](ISSUE_DETAILS.md#gs-150) | Desktop Supabase refresh token stored as plaintext JSON with default permissions | `supabase_corrections_client.py` (`~/.genizah_corrections/supabase_credentials.json`) | — | ❌ Open |

### Data and sidecars

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-272](ISSUE_DETAILS.md#gs-272) | FIST's RecordStatus 1 / 5 and PermissionCommentCode 5309 have no ruling before FGP new rows ship | `fist_data/FIST.db` `dbo_Signature.RecordStatus`, `PermissionCommentCode`; `scripts/fgp_gate2_rules.py`, `scripts/fgp_gate4_chooser_validation.py` | 2026-10-26 | ❌ Partly fixed |
| [GS-151](ISSUE_DETAILS.md#gs-151) | A failed NLI metadata fetch is cached as "Unknown" (and saved, on the desktop) | `shared/metadata_manager.py::fetch_nli_data`, `batch_fetch_shelfmarks` (+ `save_caches`) | 2026-09-25 | ❌ Open |
| [GS-152](ISSUE_DETAILS.md#gs-152) | PGP import can still produce a sidecar that is wrong but passes every check | `scripts/import_pgp_full.py`, `import_pgp_sections.py`, `pgp_transcriptions_export.py` | 2026-09-22 | ❌ Open |
| [GS-153](ISSUE_DETAILS.md#gs-153) | Four sibling translate scripts accept a blank API answer as a successful translation | `scripts/translate_*.py` (four) translate_with_retry, lines `101/164/146/127` | 2026-09-21 | ❌ Open |
| [GS-154](ISSUE_DETAILS.md#gs-154) | Oxford licence attribution is incomplete: no Schaaf number, no photographer credit, no per-object link | `web/services.py::ATTRIBUTION_BY_LIBRARY`; `shared/metadata_manager.py:36`; `oxford_full_db.json` | 2026-09-02 | ❌ Open |

### Build, CI and tests

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-271](ISSUE_DETAILS.md#gs-271) | The tracker scripts' atomic write does not keep a POSIX file's group | `scripts/open_issues_model.py::write_text` | 2026-10-25 | ❌ Open |

### Documentation

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-155](ISSUE_DETAILS.md#gs-155) | `docs/SEARCH_API.md` still calls async jobs future work | `docs/SEARCH_API.md` (What This API Is NOT, Deferred Follow-Ups, Edge-Timeout Ceiling) | 2026-10-05 | ❌ Open |
| [GS-156](ISSUE_DETAILS.md#gs-156) | `NICEGUI_RELOAD` defaults to `true` in code but is documented as `false` | `web/main.py:3560-3561`; `docs/guides/DEPLOYMENT_TECHNICAL.md:1427`; `docs/guides/ENV_VARS.md` | 2026-09-25 | ❌ Open |
| [GS-157](ISSUE_DETAILS.md#gs-157) | Stale operational facts in the guides and CI comments | `docs/guides/DEPLOYMENT_TECHNICAL.md`, `docs/guides/ENV_VARS.md`, `.github/workflows/ci.yml` | — | ❌ Open |

### Server and operations

| ID | Issue | Where | Opened | Status |
|----|-------|-------|--------|--------|
| [GS-158](ISSUE_DETAILS.md#gs-158) | `scripts/cleanup_duplicate_lists.py` would lose entries | `scripts/cleanup_duplicate_lists.py` | 2026-09-27 | ❌ Open |
