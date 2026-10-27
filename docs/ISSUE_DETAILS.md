# Issue details

The full text behind every row of [`OPEN_ISSUES.md`](OPEN_ISSUES.md) and
[`BACKLOG.md`](BACKLOG.md), one section per ID. **Grep `GS-<n>`; never read this file whole.**
A section starts with its row's headline, place, area, status and date; rows moved at the
2026-10-25 reconstruction then quote the old entry verbatim and the triage note that
re-checked it against the code that day.

## GS-1

**Responsa: on a format-1 index the page-break step still reads every whole-manuscript document (recall of `*word` is fixed)** · P2 · Search · ❌ Partly fixed · opened 2026-10-21

Where: `shared/responsa_match.py::star_route/star_queries`; `shared/search_engine.py::execute_search`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Responsa `*word` misses pages, and reads every whole-manuscript doc**
- **File:** `shared/search_engine.py::build_tantivy_query` (prefix wildcard = the 24 `expand_grammatical_prefixes` forms + bracket forms), `execute_search` (Responsa keeps the single mixed page+aggregate query)
- **Status:** ❌ Open (2026-10-21; ground truth by scanning `Transcriptions.txt`)
- **Notes:** `*כהנים *גדולים`: 369 V0.8 pages match the engine's own regex; the desktop finds 361, the website 360. Missed: 7 Judeo-Arabic forms (`אלכהנים`/`ואלכהנים`/`באלכהנים` … `אלגדולים`) and one `גדולים'`. 15 pages are found only because their whole manuscript also holds a listed form elsewhere (EVR ARAB I 118 p.168 is one; the website misses it). The 3,723 candidates include 1,829 whole-manuscript docs, 619M chars: 31 s with the `regex` module (desktop), 9.8 s with stdlib `re` (production workers). Candidate retrieval from real index terms ending in the word would fix recall and let Responsa read pages first. Owner decision. Measured: `tantivy.Query.regex_query` (tantivy 0.25) `.*כהנים` AND `.*גדולים` on page docs = 1,918 pages in 7 s.

*Triage 2026-10-25:* partly fixed. v9.9.0 true wildcards (commits d4bd357d, 46281409, PRs #442-#443): a leading-star word is now a regex_query over the index's terms (shared/responsa_match.py:1362-1400: 'reversed' route or 'scan'), so any word ending in the letters matches, and the 'Responsa reads pages first' branch (shared/search_engine.py:4196-4210) reads scope:page docs with RESPONSA_PAGE_LIMIT and whole-manuscript docs only for crossings, replacing the single mixed query. CHANGELOG 9.9.0 says '*word now finds every word that ends with the letters'. Real-index gate scripts/verify_responsa_wildcards.py --phase 3 was not run by me. Re-check: re-read at ca809c56: recall is fixed on every index format -- shared/responsa_match.py::star_queries runs a regex_query over the term dictionary ('scan' route without the reversed field), so *כהנים finds אלכהנים; pages are read first (shared/search_engine.py:4197-4206). The cost half is not: on a format-1 index the crossing step still reads whole-manuscript/part documents (route 'aggregates', with their own limit). **Still open:** On a format-1 index (production until the owner's format-2 rebuild and swap; desktop copies not rebuilt) the crossing step still reads whole-manuscript documents.

## GS-2

**My Library is not restricted by search-within** · P2 · Search · ❌ Open · opened 2026-10-04

Where: `shared/search_engine.py::_query_local_index`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **My Library is not restricted by search-within**
- **File:** `shared/search_engine.py::_query_local_index`
- **Status:** ❌ Open (found 2026-10-04)
- **Notes:** It takes no `restrict_sys_ids`, so LOCAL hits merged into an `all`-scope search-within come from every local document. Needs its own restriction on the LOCAL index. Desktop.

*Triage 2026-10-25:* still open. _query_local_index has no restrict_sys_ids parameter (:1718-1721) and neither the LOCAL-only call (:3935) nor the merged call (:4653) passes a restriction; docs/plans/SEARCH_UNCAPPED_STREAMING_PLAN.md:360 still lists 'the LOCAL index (never restricted)' as a tracker item.

## GS-3

**A cut-off line-break step stays cut off in combinations** · P2 · Search · ❌ Open · opened 2026-10-04

Where: `shared/search_engine.py::_execute_line_break_search`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **A cut-off line-break step stays cut off in combinations**
- **File:** `shared/search_engine.py::_execute_line_break_search`
- **Status:** ❌ Open (found 2026-10-04)
- **Notes:** That search has no ids-only path, so search-within and the all-terms filter cannot complete it: the chain keeps "N+" and searches within the manuscripts of its first 50,000 candidates. D8 Phase 2.

*Triage 2026-10-25:* still open. _execute_line_break_search (search_engine.py:3464) has no ids_only parameter; shared/refinement.py:237-245 still marks a Responsa line-break step as one that cannot be completed ('that search has no ids-only path').

## GS-4

**Bare-word search misses a word with a bracket INSIDE it (`ש[לום`)** · P2 · Search · ❌ Open · opened 2026-09-30

Where: `shared/search_engine.py::_add_bracket_variants`, the `orig_match_missing` drop in `execute_search`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Bare-word search misses a word with a bracket INSIDE it (`ש[לום`)**
- **File:** `shared/search_engine.py::_add_bracket_variants`, the `orig_match_missing` drop in `execute_search`
- **Status:** ❌ Open (found 2026-09-30)
- **Notes:** The hebword tokenizer keeps bracketed words as single tokens and the query only carries edge forms. **`]w[` (a word between two lacunae) fixed 2026-09-30** on branch `ccr-b3b8c7ac-jyxlw3` (10 V0.8 pages for שלום; `tests/test_bracket_lacuna_retrieval.py`, mutation-checked). Still open: brackets inside the word — a Tantivy term-regex finds 2 more V0.8 pages for שלום (`ש[לום`, `של[ום`), 3 for ברוך, 3 for אלהים; even if retrieved, the verifier drops them because the original-text re-search fails. Needs offset mapping from bracket-stripped to original text (plan stage 0a). Found by Codex (plan review round 2).

*Triage 2026-10-25:* still open. _add_bracket_variants (search_engine.py:552-580) expands only edge forms and ]term[; its docstring still says brackets inside a word are not expanded (stage 0b), and tests/test_bracket_lacuna_retrieval.py:9,34 pins the 'inside' page as not returned.

## GS-5

**A correction draft follows the reader to the next page: after a page turn in edit mode it is filed under the new page with the old text** · P2 · Browse and viewer · ❌ Open · opened 2026-10-21

Where: `web/pages/browse.py::load_page`, _save_draft_now

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **A correction draft follows the reader to the next page: after a page turn in edit mode, a new draft is filed under the new page with the old page's text**
- **File:** `web/pages/browse.py::load_page` (never resets `edit_mode`, `edit_text`, `original_edit_text`, `draft_id`), `_save_draft_now`
- **Status:** ❌ Open (2026-10-21, code reading and a review probe; pre-existing)
- **Notes:** `load_page` keeps the editor's state across a page turn, so the textarea keeps page 1's text over page 2's image. Save Draft then writes page 1's text into page 1's draft (`draft_id` kept), or, with no draft yet, into a NEW correction filed under page 2's `sys_id`/`p_num` with page 1's `original_text`. Fix: on a page change leave edit mode (asking first if the text changed) or reload the editor for the new page. Found by the 2026-10-21 review of `ccr-cb9f4b76-qh2uvw`, which did not change it.

*Triage 2026-10-25:* still open. load_page (web/pages/browse.py:1072-1830) contains no assignment to edit_mode, edit_text, original_edit_text or draft_id; the prev/next buttons (4430-4490) are disabled only by page index, not by edit mode; _save_draft_now (1909-1969) creates from state.current_page.sys_id/p_num with state.original_edit_text. The only related guard is load_gen (1923, 1956), which stops a draft id being adopted by a page the reader left while a write ran, not this case.

## GS-6

**The reading view can render a V0.7 transcription while labelling it V0.8 — and can pick V0.7 over a fuller V0.8 for the same folio** · P2 · Browse and viewer · ❌ Open · opened 2026-08-14

Where: `web/pages/browse.py:2091,4278`; `shared/search_engine.py:5687,5743`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **The reading view can render a V0.7 transcription while labelling it V0.8 — and can pick V0.7 over a fuller V0.8 for the same folio**
- **File:** `web/pages/browse.py` (`get_source_badge_class`), `shared/search_engine.py` (`get_full_text_by_id`, `get_full_manuscript`)
- **Status:** ❌ Open (found 2026-08-14)
- **Notes:** (1) `get_source_badge_class` looks for `'V0.8'`/`'V0.7'` in `full_header`, which no real header contains, so every text is badged V0.8. (2) A folio's V0.7 and V0.8 records share one `unique_id`, and `get_full_text_by_id` fetches with `limit=1` and no source filter, so hit order decides; measured returning a 10-character V0.7 over the 491-character V0.8. `get_full_manuscript` inherits this. Fix: choose by the Tantivy `source` field (as `web/components/version_selector.py` does) for the fetch and the badge. Full text archived 2026-10-06.

*Triage 2026-10-25:* still open. get_source_badge_class (browse.py:2091) still tests for 'V0.8'/'V0.7' in full_header and defaults to v08; get_full_text_by_id (search_engine.py:5687) still parses unique_id:"uid" and takes hit 0 (searcher.search(q, 1)) with no source filter, and get_full_manuscript (:5743) inherits it. Neither changed since 2026-08-14 (get_source_badge_class last touched by fff6c780). A V0.8-only server index would hide it; the desktop index holds both.

## GS-7

**Image failure UX: an NLI metadata fetch failure still shows 'No images available' as if none existed** · P2 · Browse and viewer · ❌ Open · opened 2026-04-26

Where: `desktop/viewers.py:1296-1298`, `genizah_app.py:10819`, `web/static/manuscript_viewer.js:199`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **H1: Image-failure UX does not distinguish "metadata fetch failed" from "no images configured" — most common failure mode shows misleading "No images available"**
- **File:** `desktop/viewers.py` (`set_page`), `desktop/result_dialog.py` (`external_pane.setVisible(has_images)`), `genizah_app.py` (Reading Desk), `web/pages/browse.py`
- **Status:** ❌ Open
- **Notes:** Found 2026-04-26 (the v7.9.4 smoke test; that release was rolled back). When NLI metadata cannot be fetched (offline, NLI down), `images_nli` stays empty and the viewers say "No images available", so an outside outage looks like missing data. Fix: set a fetch-failed flag when `enrich_metadata` fails and show "Image temporarily unavailable from the source library" there; keep "No images available" for manuscripts with no images. Best fixed with H2. Saved attempt: `_tmp/saved-260426-hqi/`. Full text archived 2026-10-06.

*Triage 2026-10-25:* still open. viewers.set_page still says tr('No images available') for any empty active_list (viewers.py:1296-1298) and genizah_app.py:10819 does the same; enrich_metadata (shared/metadata_manager.py:1253-1262) leaves images_nli empty on a failed manifest with no flag; the web viewer shows a generic 'Image not available' (manuscript_viewer.js:199). Only the Oxford path got specific failure messages (viewers.py:1423-1450).

## GS-8

**H2: Cached image reload only works for Oxford manuscripts when offline; NLI/CUL/JTS/Manchester cached images do not appear from disk cache** · P2 · Browse and viewer · ❌ Open · opened 2026-04-26

Where: `desktop/image_loader.py` (cache key), `genizah_app.py` (browse load path), `desktop/viewers.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **H2: Cached image reload only works for Oxford manuscripts when offline; NLI/CUL/JTS/Manchester cached images do not appear from disk cache**
- **File:** `desktop/image_loader.py` (cache key), `genizah_app.py` (browse load path), `desktop/viewers.py`
- **Status:** ❌ Open
- **Notes:** Found 2026-04-26. Offline, only Oxford images reload from the disk cache: the other libraries' cache keys need FL ids from the live IIIF manifest, which cannot be fetched, so the lookup never runs. Fix: resolve FL ids from `nli_crossref.db` (or a cached per-sys_id FL list) before the live fetch; this also fixes most of H1. Full text archived 2026-10-06.

*Triage 2026-10-25:* still open. image_loader.run keys the disk cache by FL id parsed from the URL (image_loader.py:64-72); images_nli is built only from the live nli_iiif_data canvas_map (metadata_manager.py:1256-1262), and the comment at :1236 says crossref is NOT used for image URLs. No offline FL-id resolution was added.

## GS-9

**Desktop Search export, filters and counts see only the loaded rows** · P2 · Desktop app · ❌ Open · opened 2026-09-25

Where: `genizah_app.py::export_results`, _collect_sorted_results, load_next_batch

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Desktop Search export, filters and counts see only the loaded rows**
- **File:** `genizah_app.py::export_results`, `_collect_sorted_results`, `_apply_results_table_filters`, `load_next_batch` (500 rows per batch)
- **Status:** ❌ Open (2026-09-25)
- **Notes:** A 1,200-hit search that was not scrolled exports 500 rows, and filters and "Showing X of N" apply to loaded rows only. Needs filtering and export over the result list, not the table.

*Triage 2026-10-25:* still open. genizah_app.py:26743-26750 _collect_sorted_results iterates results_table.rowCount(); export_results (27890+) takes selected or visible table rows and never reads last_results or loads the remaining batches (no results_loaded / load_next_batch / last_results reference inside it); load_next_batch (23886) still adds BATCH_SIZE rows at a time.

## GS-10

**The `*` search-hit marker is in-band, so a literal asterisk in source text is still ambiguous** · P2 · Desktop app · ❌ Open · opened 2026-09-03

Where: `desktop/widgets/__init__.py::mark_pattern_hits`, `desktop/result_dialog.py::_htmlify`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **The `*` search-hit marker is in-band, so a literal asterisk in source text is still ambiguous**
- **File:** `desktop/widgets/__init__.py` (`mark_pattern_hits`, `markers_to_bold_html`), `genizah_app.py::_browse_markers_are_ours`, `desktop/result_dialog.py::_htmlify`
- **Status:** ❌ Open (logged 2026-09-03 from the PR #334 Codex review, rounds 4-5)
- **Notes:** Highlights travel as `*…*` in the text, so an inserted marker cannot be told from a page's own asterisk (17 of 7,112 PGP transcriptions contain one). Fixed: literal asterisks are neutralized before marking, and markers render only in a search context. Left: (a) inside a search, a page's own `*` pair with no hit is still bolded; (b) `ResultDialog._htmlify` converts `*…*` unconditionally on the main pane. Fix: a sentinel marker that cannot occur in source text, a contract change shared with `shared/search_engine.py` and the exporters. Full text archived 2026-10-06.

*Triage 2026-10-25:* still open. desktop/widgets/__init__.py:50-80 still inserts '*...*' into the text (after replacing source asterisks with spaces); desktop/result_dialog.py:2731 _htmlify converts '*(.*?)*' to red bold unconditionally; no sentinel marker exists.

## GS-11

**Desktop: a stale `PGPBadgeWorker` can blank the PGP column of a table it never searched** · P2 · Desktop app · ❌ Open · opened 2026-09-03

Where: `genizah_app.py::_on_pgp_badges_loaded`, _on_tag_search_results; PGPBadgeWorker

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Desktop: a stale `PGPBadgeWorker` can blank the PGP column of a table it never searched**
- **File:** `genizah_app.py::_on_pgp_badges_loaded` (the slot), `_reset_search`, `_on_tag_search_results` (the PGP-tag path), `gui_threads.py::PGPBadgeWorker`
- **Status:** ❌ Open (found 2026-09-03 by an audit of the PGP-link commit; **pre-dates it**)
- **Notes:** The slot carries no run token, and only regular-search → regular-search waits for the worker. A large search followed by a PGP-tag search lets the late reply overwrite the tag search's PGP set and repaint, blanking badges and links on rows that have PGP data; `_reset_search` neither clears `_pgp_transcription_sys_ids` nor drains the worker. Fix: stamp each badge worker with a search generation and drop stale replies. Full text archived 2026-10-06.

*Triage 2026-10-25:* still open. genizah_app.py:25687-25695 _on_pgp_badges_loaded takes no run token and overwrites _pgp_transcription_sys_ids; the only waiter for the worker is the regular-search launch (24950-24954, the sole _pgp_badge_worker use besides init at 1643); the PGP-tag path sets the set directly at 25921 without draining or cancelling the worker.

## GS-12

**Desktop Lab Mode composition silently ignores Focus Search** · P2 · Desktop app · ❌ Open · opened 2026-09-03

Where: `genizah_app.py::run_composition` (Lab branch), `desktop/gui_threads.py::LabCompositionThread`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Desktop Lab Mode composition silently ignores Focus Search**
- **File:** `genizah_app.py::run_composition` (Lab branch, ~:25158-25174), `gui_threads.py::LabCompositionThread.__init__` (:653-656)
- **Status:** ❌ Open (found 2026-09-03 by a cross-method filter audit)
- **Notes:** `LabCompositionThread` takes no `restrict_sys_ids` and the Lab branch of `run_composition` passes none (the standard branch does), so with Lab Mode on the composition Focus Search button is enabled and reports a scope that the Lab engine never receives. Fix: thread `restrict_sys_ids` into the Lab engine, or disable the button while Lab Mode is on; do not leave it enabled and ignored. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. desktop/gui_threads.py:1213-1216 LabCompositionThread.__init__ has no restrict_sys_ids parameter; the Lab branch of run_composition (genizah_app.py:30523-30543) passes none; btn_comp_pre_search_filters (genizah_app.py:6721-6731) is never disabled for Lab Mode.

## GS-13

**Desktop: Focus Search is still ONE scope shared by the Search tab and the Composition tab** · P2 · Desktop app · ❌ Open · opened 2026-09-03

Where: `genizah_app.py::_open_pre_search_filter_dialog`, pre_search_restrict_sys_ids

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Desktop: Focus Search is still ONE scope shared by the Search tab and the Composition tab**
- **File:** `genizah_app.py::_open_pre_search_filter_dialog` (:18555), wired from `btn_pre_search_filters` (:5135) and `btn_comp_pre_search_filters` (:5508)
- **Status:** ❌ Open (found 2026-09-03 alongside the exclusion split)
- **Notes:** Both buttons call one handler and write one `self.pre_search_restrict_sys_ids`, which has 17 write sites; most visibly, `_reset_search` and `_reset_composition` each clear the other tab's scope, so "New" on either tab drops the other's Focus Search. Fix: the exclusion-split pattern (un-prefixed names for Search, a `comp_` twin, accessors). Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. Both buttons connect to _open_pre_search_filter_dialog (genizah_app.py:6287, 6724), which writes the single self.pre_search_restrict_sys_ids (22308); that attribute has 17 plain-assignment write sites including the resets at 22561 and 30235; no comp_ twin exists.

## GS-14

**Stop blocks the UI thread and can still hard-kill a worker** · P2 · Desktop app · ❌ Open · opened 2026-08-19

Where: `genizah_app.py::stop_search` (+5 sibling stop paths)

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Stop blocks the UI thread and can still hard-kill a worker**
- **File:** `genizah_app.py::stop_search` (+5 sibling stop paths)
- **Status:** ❌ Open (found 2026-08-19, during the Pause/Resume work)
- **Notes:** `stop_search` waits 5 s on the UI thread, then `QThread.terminate()`s a worker that may hold a Tantivy searcher or a SQLite handle (one between checkpoints; a paused worker is never in that state). A non-blocking Stop needs discard-vs-partial intent, per-operation terminal ownership, and an `event.ignore()`-then-drain `closeEvent`. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. genizah_app.py:23557-23563 stop_search does search_thread.wait(5000) on the UI thread and then QThread.terminate(); the same terminate pattern is still at 23646 and 36355.

## GS-15

**`btn_comp_run` renames itself after the first run** · P2 · Desktop app · ❌ Open · opened 2026-08-19

Where: `genizah_app.py:5399` vs `:21532`, `:22373`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **`btn_comp_run` renames itself after the first run**
- **File:** `genizah_app.py:5399` vs `:21532`, `:22373`
- **Status:** ❌ Open (found 2026-08-19)
- **Notes:** Created as `tr("Analyze")`, restored as `tr("Analyze Composition")`, so the button's label silently changes once a composition search completes. Cosmetic, but it makes the control harder to describe in docs and support.

*Triage 2026-10-25:* still open. Button is built with tr("Analyze") at genizah_app.py:7090; reset_comp_ui (:30015) and the completion path (:31524) set tr("Analyze Composition"). Both translations still exist (shared/genizah_translations.py:210-211). Line numbers in the row have shifted (file is now ~34K lines).

## GS-16

**A disabled button with an inline `background-color` does not look disabled** · P2 · Desktop app · ❌ Open · opened 2026-08-19

Where: `genizah_app.py:4598-4599` (`btn_search`), repo-wide pattern

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **A disabled button with an inline `background-color` does not look disabled**
- **File:** `genizah_app.py:4598-4599` (`btn_search`), repo-wide pattern
- **Status:** ❌ Open (found 2026-08-19)
- **Notes:** Qt's disabled palette is overridden by a literal `background-color` in a stylesheet, so `btn_search` renders full-strength green while it is disabled during startup and looks clickable. The Pause/Resume styles work around this with an explicit `QPushButton:disabled` rule; every other button in the file has the latent bug.

*Triage 2026-10-25:* still open. btn_search is created disabled (:5937) with a literal background-color and no QPushButton:disabled rule; only the Pause/Resume styles (:23429, :23443) carry one. No app-level stylesheet exists (git grep), and _set_search_available (:34077) now disables btn_search/btn_comp_run during an index build with the same latent look.

## GS-17

**Desktop Browse tab: three toolbars reported dark in bright mode; the named cause is no longer found in the source** · P2 · Desktop app · ⏳ Needs a check · opened 2026-05-18

Where: `genizah_app.py` (Browse-tab toolbar construction)

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Desktop Browse tab: navigation toolbar, version-and-comments toolbar, and image-controls toolbar all render with dark backgrounds in BRIGHT mode**
- **File:** `genizah_app.py` (Browse-tab toolbar construction)
- **Status:** ❌ Open · ✋ *By hand:* look at the Browse tab in Windows light mode with the current build.
- **Notes:** Seen 2026-05-18: the three toolbars keep dark backgrounds in bright mode (the buttons work; the app looks half dark). Likely a hardcoded dark `background` in a `setStyleSheet`. Fix: palette roles or a theme-aware helper, and audit every literal dark hex in `genizah_app.py`. Same family as the ResultDialog bright-mode bug. Full text archived 2026-10-06.

*Triage 2026-10-25:* needs a check. The navigation toolbar was rewritten as an OverflowRow (genizah_app.py:7570, 2026-09-24) with no stylesheet; the Version/comments bar (:7640-7720) is a plain QHBoxLayout inside _make_scrollable_row (viewers.py:31-45, only a scrollbar rule) with no dark background. The only dark literal is browse_edit_bar #2c3e50, hidden by default. So no hard-coded dark toolbar background remains, but the symptom is visual and the row itself says by-hand. **Would settle it:** Open the Browse tab in Windows light mode with the current build and look at the three toolbars.

## GS-18

**Visual Similarity / Joins Lab thumbnails of manuscripts nobody opened come up blank, and different ones on each redraw** · P2 · Website · ❌ Partly fixed · opened 2026-10-21

Where: `web/api.py::fetch_fl_ids_from_nli`, `web/nli_image_store.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Visual Similarity / Joins Lab grids: thumbnails of manuscripts nobody opened come up blank, and different ones on each redraw**
- **File:** `web/api.py::fetch_fl_ids_from_nli` (`NLI_MAX_CONCURRENT_FETCHES` 8, `NLI_SEMAPHORE_TIMEOUT` 1 s), `web/nli_image_store.py`
- **Status:** ⚠️ Partly fixed (2026-10-21, `claude/happy-maxwell-2hdkns`, deployed 2026-10-22, `6a0e228c`)
- **Notes:** Owner report 2026-10-21. Each card whose manuscript the server has not resolved needs an NLI manifest; 8 run at once and the others wait 1 s, then the route answers 404; a failure is `no-store`, so each redraw (the size buttons) re-asks exactly the failed cards. Reproduced: 12 of 20 lost at a 1.5 s manifest; production cold thumbnails took 2.3-3.5 s. Fixed on the branch: the FL-id store keeps 30 days (the "30-day" JSON held 5 minutes; archived) and thumbnails are kept on disk, so a manuscript resolved in the last 30 days, or a thumbnail fetched once, no longer waits for a slot. Still open: the first view of a manuscript nobody opened in 30 days. Fix: [docs/plans/NLI_THUMBNAIL_PREFETCH_PLAN.md](plans/NLI_THUMBNAIL_PREFETCH_PLAN.md) (prefetch; four owner decisions), or a one-time browser retry. 🔑 Server check after deploy: `/_internal/memstat` → `nli_image_store` counters, and `journalctl -u genizah-web` for "NLI semaphore timeout".

*Triage 2026-10-25:* partly fixed. Disk store of thumbnails and the 30-day FL-id store exist (commits bac67736, d67984ad, 60a81755; web/nli_image_store.py:17-24). The cold-manifest path is unchanged: web/api.py:57,61 still NLI_MAX_CONCURRENT_FETCHES 8 and NLI_SEMAPHORE_TIMEOUT 1 s, and docs/plans/NLI_THUMBNAIL_PREFETCH_PLAN.md says 'Nothing in it is built' (no prefetch script in scripts/). **Still open:** First view of a manuscript not resolved in the last 30 days still waits 1 s for one of 8 slots and 404s; needs the prefetch plan (four owner decisions) or a browser retry.

## GS-19

**/browse page change still rebuilds the whole content area in Phase B to fill its placeholders** · P2 · Website · ❌ Partly fixed · opened 2026-10-21

Where: `web/pages/browse_enrichment.py::load_enrichment`, `web/pages/browse.py::update_content`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **A /browse page change still rebuilds the whole content area twice: Phase B redraws the page to fill its placeholders**
- **File:** `web/pages/browse_enrichment.py::load_enrichment` (`refs.update_content()`), `web/pages/browse.py::update_content`
- **Status:** ⚠️ Partly fixed (2026-10-21, `ccr-cb9f4b76-qh2uvw`, deployed 2026-10-22, `6a0e228c`; opened 2026-10-20)
- **Notes:** Was 2-3 rebuilds per page change (94-300 ms of loop CPU, measured locally), on a NiceGUI 3.18 that builds elements ~55-60% slower than 3.8. **Fixed 2026-10-21:** the spinner no longer clears the page (the old content stays under it, dimmed and `inert`); Phase B fills its placeholders once, not twice; the manuscript's letters are counted once; `_patch_expects_arguments_memo` makes element builds ~36-39% faster. Stub page: opening 4 -> 2 clears, a Next click 3 -> 2. **Still open:** Phase B rebuilds the whole page (~1,200 elements on a real one) to fill five placeholders plus the Phase-B header badge, source chips and folio dropdown; refillable containers end it. `update_enrichment_sections` has no runtime caller (two source-text tests guard it). Measure with `ux_timing` and perf_watch at `GENIZAH_LOOP_LAG_MS=50`.

*Triage 2026-10-25:* partly fixed. The dimmed-spinner, single fill and _patch_expects_arguments_memo parts landed 2026-10-21 (per row); web/pages/browse_enrichment.py:750-753 still ends Phase B with refs.update_content(), a full rebuild, and update_enrichment_sections (756+) is documented as having no runtime caller. No commit touches the file after 2026-10-22. **Still open:** Phase B still rebuilds the whole page (~1,200 elements) to fill five placeholders plus badge, chips and folio dropdown; refillable containers would end it.

## GS-20

**Website restart can set off reload loops from open pages; backoff shipped 2026-10-21, saturation case and live check remain** · P2 · Website · ❌ Partly fixed · opened 2026-10-21

Where: `web/static/js/reconnect_backoff.js`, `web/reconnect_backoff.py`, `web/framework_patches.py:540-625`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **A website restart can set off reload loops from open pages, and the site then stays saturated**
- **File:** `web/static/js/reconnect_backoff.js`, `web/reconnect_backoff.py`, `web/framework_patches.py::_patch_handshake_counter`
- **Status:** ⚠️ Partly fixed (2026-10-21, `ccr-cb9f4b76-qh2uvw`, deployed 2026-10-22, `6a0e228c`; opened 2026-10-07)
- **Notes:** After the v9.6.0 restart one reader's open tabs reloaded in a loop (~35 page loads a minute) and `/search` and `/browse` took ~36 s until another restart. **2026-10-21:** a refused handshake or a second timeout reloads after a jittered 1.5 s x 2^n (cap 60 s), hidden tabs wait to be viewed, NiceGUI's first timeout (its polling fallback) passes at once, a reconnect cancels a pending timeout reload; `/_internal/memstat` counts `handshakes`. Still possible under sustained saturation: a client not connected 60 s after creation is pruned. 🔑 At the next restart: `unknown_client` should spike once, not keep coming; `/browse` latency. Full text archived 2026-10-21.

*Triage 2026-10-25:* partly fixed. web/static/js/reconnect_backoff.js and web/reconnect_backoff.py exist (commits f15f0bc6, 3ce1b27b, 569bb423, all 2026-10-21) and framework_patches.py counts handshakes (_record_handshake :540, _patch_handshake_counter :574). Not verifiable from code: behavior at an actual restart. **Still open:** A client not connected 60 s after creation is still pruned under sustained saturation; confirm at the next restart that unknown_client spikes once and /browse latency stays normal.

## GS-21

**Parallels Word export still strips highlight markers while xlsx now renders them** · P2 · Website · ❌ Open · opened 2026-08-23

Where: `web/export_service.py` (`export_parallels_word`)

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Parallels Word export still strips highlight markers while xlsx now renders them**
- **File:** `web/export_service.py` (`export_parallels_word`)
- **Status:** ❌ Open (found 2026-08-23 by the PR #325 review workflow)
- **Notes:** `export_parallels_excel` renders `*span*` as red+bold runs; `export_parallels_word` still calls `remove_highlight_markers`, so the same search exported to the two formats disagrees about what matched. Low severity (no data loss, no wrong data) but a visible inconsistency now that xlsx shows the spans. python-docx supports bold runs, so the fix mirrors the xlsx one.

*Triage 2026-10-25:* still open. web/export_service.py:1335 and 1340 still wrap the source context and the manuscript match in remove_highlight_markers(...) and add them as plain paragraphs (no bold runs).

## GS-22

**Cloudflare caches no page/API response: HTML, robots, sitemap, manifest still get a re-signed Set-Cookie; assets and image routes fixed** · P2 · Website · ❌ Partly fixed · opened 2026-07-30

Where: `web/framework_patches.py:96-124,166-189`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Cloudflare caches no page or API response — `Set-Cookie` on every non-asset response (site-wide slowness, cause 2 of 2)**
- **File:** `web/framework_patches.py` (path-aware `SessionMiddleware` / `RequestTrackingMiddleware` subclasses), `web/main.py` (`ui.run(storage_secret=...)`)
- **Status:** ❌ Open (diagnosed 2026-07-30; assets fixed 2026-08-13; image routes 2026-10-19; image Cache Rule live 2026-10-21)
- **Notes:** Since `ab064ff9` the favicon, `/static/*`, versioned `/_nicegui` assets and the atlas `.bin` carry no `Set-Cookie` (tested; live 2026-10-05). Since PR #409 the six library image routes (`_is_public_image_route`) set no cookie either and keep image bytes `max-age=604800`; Cloudflare caches them only under a Cache Rule for those paths (no file extension). 2026-10-21 (owner, after the v9.8.1 deploy): the Cache Rule "Library image proxies" is live -- host `genizahsearch.com` and the six route prefixes with trailing slashes, eligible for cache, edge TTL from the origin's `Cache-Control` -- with Smart Tiered Cache on. Checked from outside: an image goes MISS -> HIT with a positive `Age` and no `Set-Cookie`; 404/400 answers (`no-store`) are BYPASS; `/api/capabilities` stays DYNAMIC. The synthetic-sys_id 204 (no origin header) is kept at the edge about 4 h, which is harmless: a synthetic id never has an image. Page HTML, `/robots.txt`, `/sitemap.xml`, the atlas manifest and 404s still get a re-signed cookie, so Cloudflare bypasses them. The data API stays uncached by owner decision 2026-10-20 ([decision 0010](decisions/0010-no-cdn-cache-for-the-api.md)): search/parallels are POST, MCP calls the API in process, and browse alone is not worth it. Never add a cache rule for responses that still set the cookie (session fixation). Ship criteria: no `Set-Cookie`, MISS->HIT with a positive `Age`, no cookie shared from cache. Full text archived 2026-10-19. 🔑 Server check after deploy: `curl -sS -D - -o /dev/null` one `/api/nli_image_by_sysid/...` URL twice (GET; the routes answer HEAD with 405): no `Set-Cookie`, `cf-cache-status` MISS then HIT, positive `Age`. 2026-10-20: on Starlette 1.9 (NiceGUI 3.18) a returning visitor would get no `Set-Cookie`, but `_validate_session_id` marks every session modified to keep the 14-day sliding expiry, so robots/sitemap/404/manifest still carry it, now with `Vary: Cookie`. Re-issuing only a cookie older than N hours would remove it (owner decision).

*Triage 2026-10-25:* partly fixed. Fixed part: _skips_session (framework_patches.py:122) exempts static assets (:73), _SESSION_FREE_PATHS and the six public image routes (_PUBLIC_IMAGE_ROUTE_RE, :117). Still open: _validate_session_id (:166-189) still calls mark_modified() on every request, and /robots.txt, /sitemap.xml and page HTML are not session-free, so they carry Set-Cookie; the API stays uncached by owner decision 0010. Last change to the file was 2026-10-21. **Still open:** Page HTML, robots.txt, sitemap.xml, the atlas manifest and 404s still re-issue the session cookie (owner decision: re-issue only a cookie older than N hours).

## GS-23

**Web Joins Lab grid thumbnails for CUL/Manchester/JTS go to the NLI proxy until the breaker opens (3 failures); proxy has no provider fallbac** · P2 · Website · ❌ Open · opened 2026-06-21

Where: `web/components/candidate_grid.py:484-547`, `web/api.py:1126`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Web Joins Lab grid thumbnails for CUL / Manchester / JTS still go to the NLI proxy while NLI is down but its breaker is closed (SEED-010 remainder)**
- **File:** `web/components/candidate_grid.py::build_thumbnail_url`, `web/components/image_resolution.py`, `shared/nli_circuit_breaker.py`
- **Status:** ❌ Open (logged 2026-06-21; narrowed 2026-10-05)
- **Notes:** The grid picks a thumbnail route once per card from the breaker's state, and the breaker opens only after 3 failures, so a fresh process, the gaps after its 60 s window, or an image-server-only outage send CUL/Manchester/JTS thumbnails to `/api/nli_image_by_sysid`, which has no provider fallback; the placeholder is permanent, and cards with an empty `library_code` always go there. Fixes: `resolve_image_url(surface='thumbnail')`, a server-side fallback, or one onerror retry. The live NLI up/down check is still not recorded. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. build_thumbnail_url (candidate_grid.py:536-547) still chooses the provider proxy only when nli_circuit_breaker.is_open() and library_code is non-empty, else /api/nli_image_by_sysid; nli_image_by_sysid (api.py:1126) shows no provider fallback. Later commits to this file were toolbar/filter only.

## GS-24

**method='passage' single-text parallels exceeds its 30 s ceiling on 51-100 char inputs and returns no hits through the public endpoint** · P2 · API and AI · ⏳ Needs a check · opened 2026-09-08

Where: `shared/passage_search.py`, `shared/passage_policy.py`, `web/search_api.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **`method='passage'` single-text parallels exceeds its own 30 s ceiling on short inputs (51-100 chars), so the documented single-text passage path returns no hits through the public endpoint**
- **File:** `shared/passage_search.py` / `shared/passage_policy.py` (`posting_budget`, policy `widest-40`), `SEARCH_API_PASSAGE_TIMEOUT` (30)
- **Status:** ❌ Open (found 2026-09-08)
- **Notes:** A 100-char single `text` returned 504 at 34 s (twice) and a 51-char one nothing by 100 s; 28 chars is below `min_span: 40`. So no single-text passage request is known to return hits from outside; multi-witness does. Same root as the posting-budget starvation on long queries. The 504 arrived as plain text although our JSON error bodies otherwise pass the proxy. Unproven hypothesis: uncancelled timed-out executor threads (up to `SEARCH_API_PASSAGE_CONCURRENCY`=4) starve the loop that has to send it. Next: measure (py-spy, or time the raise against the socket send); interim: lower the concurrency. Full text archived 2026-10-06.

*Triage 2026-10-25:* needs a check. web/search_api.py:402 still DEFAULT_PASSAGE_TIMEOUT = 30.0 and shared/passage_policy.py:85 posting_budget 500_000. Later commits (fff6c780 #339 isolated research workers, 61261a7d partial results at the time limit) change how long searches and timeouts behave but nothing in the diff addresses or disproves the 100-char 504 or the thread-starvation hypothesis. **Would settle it:** Time a 100-char and a 51-char single-text method='passage' POST /api/parallels against the real passage index (sync and as a background job), with py-spy or timing of the 504 send; needs the server or the local passage data.

## GS-25

**Multi-witness passage parallels is non-deterministic in latency: the identical payload returned 198 results once, then nothing within 120 s** · P2 · API and AI · ⏳ Needs a check · opened 2026-09-08

Where: `shared/passage_parallels.py::PassageSearcher`, `web/search_api.py` witnesses path

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Multi-witness passage parallels is non-deterministic in latency: the identical payload returned 198 results once, then nothing within 120 s**
- **File:** `shared/passage_parallels.py::PassageSearcher`, the `witnesses` path of `/api/parallels`, `SEARCH_API_PASSAGE_TIMEOUT` (30) / `SEARCH_API_HEAVY_CONCURRENCY` (2)
- **Status:** ❌ Open (found 2026-09-08)
- **Notes:** Two Birkat Hamazon witnesses sent with `method='passage'` returned 200 with 198 results once, then nothing within 120 s on an identical repeat. A saturated heavy budget would answer 503 `heavy_search_busy` at once, so queuing does not explain it. Understand it before recommending the feature to external clients. Full text archived 2026-10-19.

*Triage 2026-10-25:* needs a check. The row is an unexplained timing observation (no root cause recorded). web/search_api.py:2327-2337 now caps projected witness cost against the ceiling and a research-job path with no ceiling exists (#339), but no commit identifies or removes the cause of the stall. **Would settle it:** Replay the two-witness Birkat Hamazon payload several times against the real server (or local passage index) and watch the heavy/passage budget and executor threads (py-spy).

## GS-26

**The discovery matcher never searched the HTR text of the 18,982 corpus pages whose search text was swapped for an FGP/PGP transcription** · P2 · Discovery · ❌ Open · opened 2026-09-01

Where: `same_work_spike/.../mapv2_stage0.py`, `scripts/attach_htr_realignment.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **The discovery matcher never searched the HTR text of the 18,982 corpus pages whose search text was swapped for an FGP/PGP transcription (recall gap)**
- **File:** `same_work_spike/probe/scripts/mapv2_stage0.py` (the substitution gate), `scripts/attach_htr_realignment.py`
- **Status:** ❌ Open (found 2026-09-01 while the owner reviewed the v5 handoff text)
- **Notes:** Stage-0 (2026-07-10) replaced the HTR text with an FGP/PGP transcription on 18,982 of 667,411 pages before both discovery runs; the HTR text is still in `Transcriptions.txt`. (a) Addressing: closed 2026-09-01 (`attach_htr_realignment.py`, `htr_page`). (b) **Recall, open:** a match present only in the HTR text was never searched for; 209 pages keep only 50-70% of their HTR content and 595 are flagged risky (`results/mapv2_substitution_risk.md`). Fix: re-run the matcher on those pages' HTR text as a third satellite (`scripts/merge_v5_review_db.py` pattern), dedup, same gates; about a day. Price it with a 200-page `--cost-ceiling` sample, never a `--limit` smoke. Full text archived 2026-10-06.

*Triage 2026-10-25:* still open. docs/v5-review-pipeline.md:150-166 still describes the 18,982 substitutions and 595 risky pages; git log of scripts/attach_htr_realignment.py and merge_v5_review_db.py since 2026-09-01 shows only 3a447a10 (the addressing half the row already marks closed) and no HTR-text third-satellite re-run or merge exists.

## GS-27

**`/computed-identifications` costs ~2 s and only ~0.9 s is accounted for** · P2 · Discovery · ❌ Open · opened 2026-08-19

Where: `web/pages/findings.py:1172` (`_fetch_approved_review_map`), `:1312`, `:2246-2283` (facet cascade)

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **`/computed-identifications` costs ~2 s and only ~0.9 s is accounted for**
- **File:** `web/pages/findings.py:1172` (`_fetch_approved_review_map`), `:1312`, `:2246-2283` (facet cascade)
- **Status:** ❌ Open (found 2026-08-19) · 🔑 *Server check:* time the two Supabase round trips on the server and take a browser trace in production.
- **Notes:** The loop is not blocked; the work is serial on the request's await chain (rows 187 ms + facet cascade 348 ms + elements 200-350 ms). Prime suspects for the rest: two uncached Supabase round trips, one the identification-reviews RPC on every render. Measure them and take a browser trace first; do not `asyncio.gather` the facets (it exhausts the heavy budget). Detail: [`docs/specs/discovery-performance-situation-2026-08-20.md`](specs/discovery-performance-situation-2026-08-20.md). Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. _fetch_approved_review_map (findings.py:1232) still issues one uncached Supabase RPC per refresh via published_reviews_by_identification_async (web/identification_reviews.py:239); no caching or timing was added since 2026-08-19 (only a54f0f82 and 4f6e31f4 touched the page). The row's line numbers are stale. The cost itself needs a server trace, but nothing in code removes the suspect.

## GS-28

**List sync: an edit made during an upload can settle differently from the same edit made just after it, losing a website entry** · P2 · Accounts and community · ❌ Open · opened 2026-10-25

Where: `shared/lists_sync.py`; `tests/list_sync_scenarios.py` (invariant 14)

Found by the 2026-10-25 tracker triage, reproduced the same day at `ca809c56`: `python tests/list_sync_scenarios.py --seeds 11362,11917 --steps 200` fails invariant 14 ('not-serial') on both seeds. Seed 11362, step 198: the website's settled state lacks `('L3', '990002', None, None, ('n20',), {'"g7"'})` when the desktop's edit lands during the upload, and has it when the same edit lands right after the upload is installed; seed 11917, step 171: the same with `('L1', '990001', 'FLb', '2', ('n18',), {'"g17"'})`. The documented gate sweeps (`tests/test_list_sync_scenarios.py` docstring) run 200-step scenarios only for seeds 0-1999, so these were never in it. Next: trace one seed with `--trace`, find which upload step drops the concurrent edit, and widen the 200-step sweep range in the documented gate.

## GS-29

**Web: replies to community discoveries fail, because `discovery_responses` does not exist in production** · P2 · Accounts and community · ❌ Open · opened 2026-10-21

Where: `web/supabase_client.py::get_discovery_responses/create_discovery_response`, `web/pages/discoveries.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Web: replies to community discoveries fail, because `discovery_responses` does not exist in production**
- **File:** `web/supabase_client.py::get_discovery_responses` / `create_discovery_response`, `web/pages/discoveries.py` (the reply box)
- **Status:** ❌ Open (2026-10-21; table absence live-confirmed, the page behaviour from code reading)
- **Notes:** Reading logs an error and returns no replies, so every discovery looks unanswered; posting shows the raw database error instead of "Reply posted". Owner decision: create the table (DDL in `migrations/` with RLS, grants and the anonymous author kept out of the data from the start), or hide the reply box until then. The desktop's replies are stubs (P2 row "Desktop discovery responses are stubs").

*Triage 2026-10-25:* still open. web/supabase_client.py:2036 and 2057 still query/insert table 'discovery_responses', read errors are logged and return [] (2041-2043), create returns the raw error (2061-2062); no migrations/*.sql or supabase_setup.sql creates the table (only scripts/supabase_live_check.sql:48 asks about it). Whether the owner has since created it in Supabase by hand is not visible from the repo.

## GS-30

**Hardening: mask the author of anonymous discoveries and responses in the data, not only on screen** · P2 · Accounts and community · ❌ Open · opened 2026-10-20

Where: `supabase_setup.sql` (discoveries), `web/supabase_client.py::get_discovery_responses`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Hardening: mask the author of anonymous discoveries and responses in the data, not only on screen**
- **File:** `supabase_setup.sql` (discoveries SELECT has no column restriction; anon SELECT), `web/supabase_client.py::get_discovery_responses` (`select('*')`)
- **Status:** ❌ Open (2026-10-20, repo SQL; live-checked 2026-10-21: nothing exposed today)
- **Notes:** Live 2026-10-21 (Q13): no discovery is marked anonymous, and `discovery_responses` does not exist, so no author is exposed today; the first anonymous discovery would be. Fix before that feature is used. Options (owner): a SECURITY DEFINER read that returns no `user_id` for an anonymous row, or call it "name hidden on the page" instead of "anonymous".

*Triage 2026-10-25:* still open. supabase_setup.sql:333-334 'Anyone can view public discoveries' is a whole-row SELECT (is_hidden = false OR own), table has is_anonymous (line 140) and user_id; web/supabase_client.py:2036 still select('*') on discovery_responses; no later migration adds a masking view or SECURITY DEFINER read.

## GS-31

**Hardening: give the documented `published_joins` admin UPDATE policy a real `WITH CHECK`** · P2 · Accounts and community · ❌ Open · opened 2026-10-20

Where: `docs/guides/SUPABASE_GUIDE.md` (the published_joins policies; no tracked DDL)

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Hardening: give the documented `published_joins` admin UPDATE policy a real `WITH CHECK`**
- **File:** `docs/guides/SUPABASE_GUIDE.md` (the published_joins policies; no tracked DDL)
- **Status:** ❌ Open (2026-10-20, docs; production unverified) · 🔑 Q4, flag (b), of `scripts/supabase_live_check.sql`
- **Notes:** Its `WITH CHECK` is `true`; permissive policies' checks combine with OR, so it widens every other UPDATE policy on the table. Fix: `WITH CHECK (EXISTS (SELECT 1 FROM public.profiles WHERE id = auth.uid() AND role = 'admin'))`. See also "Supabase DDL gaps".

*Triage 2026-10-25:* still open. docs/guides/SUPABASE_GUIDE.md:971-974 still reads 'Admins can update any published join ... WITH CHECK (true)'; no tracked SQL contains a replacement (git grep published_joins over *.sql finds only scripts/supabase_live_check.sql). Production state unverified (Q4 of the live check).

## GS-32

**Desktop discovery responses are stubs** · P2 · Accounts and community · ❌ Open · opened 2026-09-25

Where: `desktop/supabase_corrections_client.py::add_discovery_response/get_discovery_responses`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Desktop discovery responses are stubs**
- **File:** `desktop/supabase_corrections_client.py::add_discovery_response`/`get_discovery_responses`, `desktop/corrections_ui.py::DiscoveryDetailDialog`
- **Status:** ❌ Open (2026-09-25)
- **Notes:** Posting a response always fails with an untranslated English error; the header always says "Responses (0)" and "No responses yet" shows even when web users replied (the web reads and writes `discovery_responses`, `web/supabase_client.py`). Reachable from the discoveries list (double-click, context-menu Edit) and the feed dialog. Fix: mirror the web client after checking the live table's columns and RLS (no DDL in the repo).

*Triage 2026-10-25:* still open. desktop/supabase_corrections_client.py:1981-1995: add_discovery_response returns (None, 'Discovery responses not implemented in Supabase schema') and get_discovery_responses returns [] unconditionally.

## GS-33

**Admins cannot see or un-hide another user's hidden discovery, and both apps report success** · P2 · Accounts and community · ❌ Open · opened 2026-09-25

Where: `supabase_setup.sql` discoveries SELECT policy; desktop unhide_discovery

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Admins cannot see or un-hide another user's hidden discovery, and both apps report success**
- **File:** `web/supabase_client.py::toggle_discovery_hidden`, `desktop/supabase_corrections_client.py::unhide_discovery`, `web/pages/discoveries.py` (`include_hidden=is_admin`), `discoveries` SELECT policy
- **Status:** ❌ Open (2026-09-25)
- **Notes:** The live `discoveries` SELECT policy is `is_hidden = false OR own`, with no admin case, so a hidden row is invisible to every admin: `include_hidden=True` returns nothing extra, and an admin un-hide changes 0 rows. The desktop `unhide_discovery` returns "Discovery unhidden" without checking rows. Found while checking the live policies on 2026-09-25. Fix: an admin SELECT policy on `discoveries`; then `get_feed_items` must move to the user client (its comment in `web/supabase_client.py` says so), and the desktop helper should check the returned rows.

*Triage 2026-10-25:* still open. supabase_setup.sql:333-334 SELECT is 'is_hidden = false OR auth.uid() = user_id' with no admin case, and no migration adds one; desktop/supabase_corrections_client.py:2189-2190 still returns (True, 'Discovery unhidden') without checking rows. Correction to the row: web toggle_discovery_hidden (web/supabase_client.py:2138-2140) does return an error when no row changes, so only the desktop reports false success.

## GS-34

**The `/corrections` Review tab only works for admins: reviewers and editors cannot approve or reject someone else's correction** · P2 · Accounts and community · ❌ Open · opened 2026-09-25

Where: `web/pages/corrections.py` (Review tab), `supabase_setup.sql` corrections UPDATE policy

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **The `/corrections` Review tab only works for admins: reviewers and editors cannot approve or reject someone else's correction**
- **File:** `web/pages/corrections.py` (Review tab, shown to reviewer/editor/admin); `desktop/supabase_corrections_client.py` (review path)
- **Status:** ❌ Open (2026-09-25)
- **Notes:** The live `corrections` UPDATE policies cover only the row's author and admins, so a reviewer's or editor's approve/reject on another user's row changes 0 rows (since #364 the web says so instead of reporting success). Found while checking the live policies after the #364 deploy. Decide which roles may review, then add a matching UPDATE policy or route reviews through a server-side function.

*Triage 2026-10-25:* still open. supabase_setup.sql:317 is still the only corrections UPDATE policy ('Users can update own corrections' USING author_id); migrations/add_privileged_column_guards.sql (~160-190) allows reviewer/editor status changes in the trigger but says it is 'reachable only once an RLS policy lets them update it', and no such policy exists in the repo; web/pages/corrections.py:264,278 still shows the tab to reviewer/editor.

## GS-35

**A resubmitted identification review replaces the published one: it disappears until approved again, and the earlier text is lost** · P2 · Accounts and community · ❌ Open · opened 2026-09-25

Where: `scripts/create_identification_reviews_beta.sql:201-217`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **A resubmitted identification review replaces the published one: it disappears until approved again, and the earlier text is lost**
- **File:** `scripts/create_identification_reviews_beta.sql` (the submit function's `INSERT ... ON CONFLICT (identification_id, reviewer_key) DO UPDATE`)
- **Status:** ❌ Open (sweep 2026-09-25 #20; the live function not checked)
- **Notes:** Each resubmission overwrites the verdict and comment and resets `status='pending'`, `publish_comment`, `moderation_note` and `reviewed_at`/`reviewed_by`; no history table keeps the published version. Decision needed: keep the published review until the new one is approved (with a history row), or accept the overwrite in writing.

*Triage 2026-10-25:* still open. submit_identification_review_beta still does INSERT ... ON CONFLICT (identification_id, reviewer_key) DO UPDATE SET ... status = 'pending', publish_comment = FALSE, moderation_note = NULL, reviewed_at = NULL, reviewed_by = NULL (lines 201-217); no history table exists and no later migration redefines the function (git grep finds it only in this file). Whether the live function matches was not checked.

## GS-36

**V0.8 corpus: cross-manuscript misattribution upstream; ~6% V0.7-only manuscripts absent from production (by owner decision)** · P2 · Data and sidecars · ❌ Partly fixed · opened 2026-08-16

Where: `Transcriptions.txt` ingest (upstream); `data/v08_repair_manifest.json`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **V0.8 corpus: one manuscript's folio text filed under a different manuscript, and ~6% of manuscripts have no V0.8 at all (invisible in production)**
- **File:** `Transcriptions.txt` / V0.8 ingest pipeline (upstream data — NOT app code); index `C:\Users\gersh\Genizah_Tantivy_Index\tantivy_db`
- **Status:** ⚠️ Upstream open; **production repaired + reindexed 2026-08-16** (found 2026-08-12; **data defect**, see SEED-033 §"Findings that are NOT this seed")
- **Notes:** Two upstream findings. (1) **Cross-manuscript misattribution:** the V0.8 ingest appends a neighbouring item's folio text (often twice) to another record, mostly in multi-IE sys_ids; reported case: the text of `p. Heid. Hebr. 19` (`990043940120205171`) inside Hebr. 18's record. A corpus-wide scan (2026-08-14; `.planning/debug/v08-cross-manuscript-misattribution.md`, `_tmp/v08_*`) found 13 back-to-back repeats and 5 confirmed host→victim pairs, a lower bound. **Deployed 2026-08-16:** the hash-guarded `data/v08_repair_manifest.json` (3 re-file, 4 strip, 1 dedupe) was applied on the server and the index rebuilt (1,161,479 → 1,161,484 docs); the reported phrase now finds Hebr. 19. Three weak cases were left unrepaired on purpose (AIU III A 22→III.A.23, Halpern 41\|42\|43→44, Halpern 27→29). Still open: the ingest bug itself (with the MiDRASH team), and the Lab index serves pre-repair text until it is rebuilt. Rebuild in the quiet hour: page-cache eviction degraded the site on the first attempt. (2) **15,539 V0.7-only manuscripts (6.68%) are absent from production by owner decision** (2026-08-14: production is V0.8-only; V0.7 serves only as a map for re-filing); about 1,700 of them carry substantive text. Not a bug. Full text archived 2026-10-06.

*Triage 2026-10-25:* partly fixed. The hash-guarded repair manifest exists (data/v08_repair_manifest.json, commit 318cf117, applied by scripts/apply_v08_repair.py per DATA_LIFECYCLE.md:101); the 2026-08-16 server apply and reindex cannot be seen from the repo. The second half (V0.7-only absent) is an owner decision, not a bug. **Still open:** The ingest bug itself (MiDRASH team, upstream); three weak cases left unrepaired on purpose; the Lab index serves pre-repair text until rebuilt (server).

## GS-37

**deploy.sh runs git reset --hard before pip, so a failed pip leaves new code on disk under the old process** · P2 · Server and operations · ❌ Partly fixed · opened 2026-10-20

Where: `deploy.sh`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **`deploy.sh` still lets anyio, python-multipart, aiohttp, multidict and Pygments float, and runs `git reset --hard` before `pip`**
- **File:** `deploy.sh`
- **Status:** ⏸ Owner decision (2026-10-20)
- **Notes:** `requirements.txt` pins Starlette / python-socketio / python-engineio to the lock since 2026-10-20, but NiceGUI 3.18's floors still raise these five to whatever is newest at deploy time, which CI never ran. Recommended: `pip install -q -r requirements.txt -c requirements-lock.txt` (constraints only: no lock-only dev package is installed). A failed `pip` after the reset leaves new code on disk under the old process.

*Triage 2026-10-25:* partly fixed. Fixed part: deploy.sh:21 now runs 'pip install -q -r requirements.txt -c requirements-lock.txt' (commit 906e310b, v9.8.1), so anyio, python-multipart, aiohttp, multidict and Pygments no longer float. Still open: deploy.sh:13 'git reset --hard origin/$BRANCH' precedes pip at line 21; with set -e a pip failure aborts before the restart at line 24, leaving new code on disk while the old process keeps serving. **Still open:** Move the checkout after a successful install (or install from a staging checkout) so a failed pip does not leave new code under the old process.

## GS-38

**A tracked planning deploy log quotes the backup password of the retired local Postgres, in a public repo** · P2 · Server and operations · ❌ Open · opened 2026-10-20

Where: a `.planning` deploy log (the path is in the 2026-10-20 review notes, not repeated here)

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **A tracked planning deploy log quotes the backup password of the retired local Postgres, in a public repo**
- **File:** a `.planning` deploy log (the path is in the 2026-10-20 review notes, not repeated here)
- **Status:** ❌ Open (2026-10-20) · 🔑 *Server check*
- **Notes:** The database is retired, but the 2026-06-11 notes show postgresql still installed and running. Rotate the password, or drop the database and disable postgresql; remove `backup.sh` and its cron; confirm port 5432 is closed; then redact the line (it stays in history and tags).

*Triage 2026-10-25:* still open. The tracked file still contains the cleartext PGPASSWORD of the retired genizah_db on lines 247-248 (not redacted; value not repeated here). Server-side rotation / postgresql shutdown cannot be checked from the repo.

## GS-39

**Responsa: a star word holding Hebrew punctuation is not refused** · P3 · Search · ❌ Open · opened 2026-10-23

Where: `shared/responsa_match.py::_TOKEN_RE`, `refuse_unsupported_stars`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Responsa: a star word holding Hebrew punctuation is not refused**
- **File:** `shared/responsa_match.py::_TOKEN_RE`, `refuse_unsupported_stars`
- **Status:** ❌ Open (2026-10-23)
- **Notes:** Codex on PR #442 (1468ca4, P2): the run check of a star word accepts the whole Hebrew block, so a maqaf, sof pasuq or Hebrew geresh inside the word (`א־*`) passes where the contract says a star never reaches past punctuation and `docs/SEARCH_API.md` promises `responsa_unsupported_star`; the search runs and a match may span the maqaf. Everywhere else a maqaf ends a word. No real search goes wrong, so the owner merged without it (2026-10-23). Fix: judge a run's characters by Unicode category (letters plus the looked-past marks, quotes and brackets) rather than the block range; settle together with the bracketed-quote-between-words row, both are punctuation between letters.

*Triage 2026-10-25:* still open. _TOKEN_RE still accepts the whole Hebrew block U+0590-05FF; run in this session, strip_search_diacritics then _TOKEN_RE.fullmatch on alef+maqaf (05BE) and alef+sof pasuq (05C3) both return True, so the run passes (refuse_unsupported_stars, line 130). The geresh (05F3) case in the row is already harmless: strip_search_diacritics removes it before the check. No commit has touched this since PR #442.

## GS-40

**Responsa: a bracket group holding only a quote between two words blocks the sequence** · P3 · Search · ❌ Open · opened 2026-10-23

Where: `shared/responsa_match.py::_separator`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Responsa: a bracket group holding only a quote between two words blocks the sequence**
- **File:** `shared/responsa_match.py::_separator`
- **Status:** ❌ Open (2026-10-23)
- **Notes:** Found by the Phase 3 real-data gates on the owner's laptop (PRs #442/#443): for `*נים *לים` the brute force and the oracle list page `IE156716378_P000002_FL156716384` (sys_id 990053175920205171) and the engine does not, on both index formats. The page reads `בעצנים ⟦'⟧` then a line break then `הגוצלים`. The separator between two query words is a class of everything but word characters, the Hebrew block and the apostrophe: the apostrophe is an in-word quote, so `⟦'⟧` counts as a word standing between the pair and a gap-0 sequence fails. The same for the finite pair `בעצנים הגוצלים`, on both regex runtimes; the line break alone is fine (checked with the compiled check on the page's text). The oracle and `measure_responsa_wildcards.py` read a bracket group of quotes and marks as transparent. One page in 3,872 for this query; older than Phase 3. Either the separator admits a bracket group whose content is only quotes or marks, or the oracle lists the kind as it lists maqaf; the owner's call.

*Triage 2026-10-25:* still open. _separator is still the class of everything but word characters, the Hebrew block and the apostrophe (no allowance for a bracket group of quotes/marks); grep of responsa_match.py finds no handling of such a group. Real-data page needs the owner's laptop, but the separator code is exactly as described.

## GS-41

**Responsa: a My Library-only search's downgrade carries no effective options** · P3 · Search · ❌ Open · opened 2026-10-22

Where: `shared/search_engine.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Responsa: a My Library-only search's downgrade carries no effective options**
- **File:** `shared/search_engine.py`
- **Status:** ❌ Open (2026-10-22)
- **Notes:** Codex round 8 on PR #432; confirmed by reading, not reproduced. When the guard turns variants or Judeo-Arabic off for a LOCAL-only Responsa query (`corpus_scope='local'`), that branch of `execute_search` records the text warning (`_set_last_responsa_downgrade`) but not `_set_last_responsa_downgrade_meta`, which the main and line-break paths record. A caller that builds `responsa_options_effective` from the meta (`web/search_api.py`) falls back to the requested options and reports the disabled ones as applied. Not checked whether any API call reaches a LOCAL-only search; the desktop shows only the text. Introduced in 6e853f3f. Fix: record the meta from `_local_compiled.options` next to the warning, as the other two paths do, with a test that reads `_consume_last_responsa_downgrade_meta()` after a LOCAL-only downgrade.

*Triage 2026-10-25:* still open. The LOCAL-only branch calls _set_last_responsa_downgrade(_local_compiled.warning) at shared/search_engine.py:3918 with no _set_last_responsa_downgrade_meta, while the other two paths set both (lines 3726-3727, 4710-4718). Latent: web/search_api.py never passes corpus_scope, so the API cannot reach a LOCAL-only search; only the desktop (text only) does.

## GS-42

**Regex mode `מאמ\w* קור\w*` returned 0 rows on the real index** · P3 · Search · ⏳ Needs a check · opened 2026-10-22

Where: `shared/regex_prefilter.py::build_candidate_query`; `shared/search_engine.py::_regex_candidate_query`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Regex mode `מאמ\w* קור\w*` returned 0 rows on the real index**
- **File:** `shared/regex_prefilter.py::build_candidate_query`, `shared/search_engine.py::_regex_candidate_query`
- **Status:** ❌ Open (2026-10-22)
- **Notes:** Reported by the session measuring Responsa `מאמ* קור*` on the owner's laptop (full V0.8 + V0.7 index, stdlib `re`): Regex mode `מאמ\w* קור\w*` gave 0 rows in 0.2 s, while Responsa `מאמתי קורין` gives 48-50 rows on the same index. Not reproduced on a one-page fixture built by the real Indexer (PR #442's session): there the same pattern gives its row, and the prefilter asks for a token that begins with קור (`Tok(start=True)`). To check on the real index: the candidate query's formula and hit count for this pattern, and whether the 0 comes from the prefilter, the check, or the caller (desktop or website).

*Triage 2026-10-25:* needs a check. Ran extract_formula on the pattern in this session: the candidate formula is a single start-anchored token for the second word (a superset), so the prefilter looks sound; shared/regex_prefilter.py has not changed since 2026-10-12. One unexamined cause: the pattern has a literal space, while the Responsa hits may be separated by line breaks. **Would settle it:** On the real index: print the candidate query's formula and hit count for this pattern, then rerun with the space replaced by a whitespace class; if the second form finds rows the 0 is the literal space (not a bug), otherwise the prefilter or caller is at fault.

## GS-43

**A page filed under several manuscripts is shown under whichever copy the index returns first; a rebuild changes manuscript counts** · P3 · Search · ❌ Open · opened 2026-10-22

Where: `shared/search_engine.py::_deduplicate`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A page filed under several manuscripts is shown under whichever copy the index returns first; a rebuild changes manuscript counts**
- **File:** `shared/search_engine.py::_deduplicate`
- **Status:** ❌ Open (2026-10-22)
- **Notes:** One page image (one uid) can be catalogued under many `sys_id`s (IE50232361_P000117 sits under 40+ records, in both sources). `_deduplicate` keeps one row per uid, the first one by score and then by document order, so which manuscript the row names follows the index's segment layout. Measured with the same code on three builds of the same corpus (Responsa plan Phase 2, 2026-10-22): an uncapped Exact `ישראל` reaches 44,055 manuscripts on today's local index, 44,056 and 44,061 on two rebuilds (`scripts/verify_search_combinations.py`); the pages are the same, their manuscript differs. Any rebuild (format 1 or 2) can shift it. A fix would pick the copy deterministically (lowest sys_id, or the copy inside a restriction) or show the page once with all its manuscripts.

*Triage 2026-10-25:* still open. _deduplicate keeps the first (first_wins) or last row per uid in result order, with no deterministic tie-break such as lowest sys_id (lines 4747-4777). No change since the row.

## GS-44

**Exact/Variants/Fuzzy over whole manuscripts: a crossing from an earlier page can hide the next page's own** · P3 · Search · ❌ Open · opened 2026-10-22

Where: `shared/search_engine.py::_cross_page_spans` (the break loop, without `page_start`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Exact/Variants/Fuzzy over whole manuscripts: a crossing from an earlier page can hide the next page's own**
- **File:** `shared/search_engine.py::_cross_page_spans` (the break loop, without `page_start`)
- **Status:** ❌ Open (2026-10-22)
- **Notes:** At each page break of a whole-manuscript document, `_cross_page_spans` takes the first match that crosses the break. A match that starts on an earlier page and runs over a whole page crosses the next break too, so it is taken there again, the duplicate is dropped, and the next page's own crossing of that break is lost. Example: pages `כהנים` \| `גדולים כהנים` \| `גדולים כהנים גדולים`, Exact `כהנים גדולים כהנים גדולים`, gives p1 only; p2's crossing into p3 is missing. It needs a match that spans a whole page, so a page of one or two words. PR #442 fixed this for Responsa on every route, and PR #443 fixed it for E/V/F read from a seam (`page_start`, Codex round 3 on #443). It stays on the routes that read whole manuscripts: format 1 today, and on format 2 when S > K. The fix is the same rule: at each break, count only a match that starts on the page just before it. That changes today's E/V/F rows, so it needs `scripts/verify_search_combinations.py` on the real index before it ships.

*Triage 2026-10-25:* still open. The whole-manuscript call at shared/search_engine.py:4453-4455 passes no page_start, so the break loop (lines 3036-3046) still takes the first crossing match, including one starting on an earlier page; page_start is only passed on the seam call at line 4371-4374 (PR #443).

## GS-45

**Exact (whole words) misses a phrase with four or more stray tokens between its words** · P3 · Search · ❌ Open · opened 2026-10-21

Where: `shared/search_engine.py::build_phrase_candidate_query` (`_PHRASE_EXTRA_SLOP = 3`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Exact (whole words) misses a phrase with four or more stray tokens between its words**
- **File:** `shared/search_engine.py::build_phrase_candidate_query` (`_PHRASE_EXTRA_SLOP = 3`)
- **Status:** ❌ Open (2026-10-21, Codex review round 4; pre-existing on the website and desktop, the API since its Exact became whole-word)
- **Notes:** The phrase prefilter retrieves a page only when each adjacent pair of words is within slop gap + 3; the hebword tokenizer keeps lone lacuna brackets and marks as tokens, so `אהרן [ ] [ ] כהן` (four) is never a candidate, and no "N+" or cut-off warning says so. Reproduced by Codex in the engine; frequency on the real index unmeasured (2026-09-30: +2 missed nothing over six phrases). Fix: measure how many pages have 4+ stray tokens inside a phrase; raise the slop or count only word tokens in it, keeping the regex as the judge; add the case to `scripts/verify_search_combinations.py`.

*Triage 2026-10-25:* still open. shared/search_engine.py:2480 `_PHRASE_EXTRA_SLOP = 3` and :2502 `slop = max_gap + self._PHRASE_EXTRA_SLOP` are unchanged: every adjacent pair must be within gap+3 tokens, so 4 lone bracket tokens drop the page before the regex judges it.

## GS-46

**Exact (`literal`) never finds a whole-word token outside its bracket variants (`אב[[`, `א'ב'`)** · P3 · Search · ❌ Open · opened 2026-10-20

Where: `shared/search_engine.py::_add_bracket_variants`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Exact (`literal`) never finds a whole-word token outside its bracket variants (`אב[[`, `א'ב'`)**
- **File:** `shared/search_engine.py::_add_bracket_variants`
- **Status:** ❌ Open (2026-10-20, real index)
- **Notes:** 2 of 72,535 rows for אב, found only by the engine's `exact` through a whole-manuscript document. Add doubled-bracket forms; whether an internal-geresh abbreviation should match is a wording question.

*Triage 2026-10-25:* still open. _add_bracket_variants still produces only the six single-bracket forms ([t, t], [t], ]t, t[, ]t[) at lines 577-579; no doubled-bracket form was added. The docstring itself says brackets inside a word are not expanded.

## GS-47

**Shelfmark resolver: `normalize_shelfmark` glues numbers across `=`, so `… Ms. Heb. 577.10.6=04` resolves to `577.10.60=4`** · P3 · Search · ❌ Open · opened 2026-10-18

Where: `shared/browse_map_utils.py::normalize_shelfmark`, `metadata_manager.py::resolve_system_by_shelfmark`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Shelfmark resolver: `normalize_shelfmark` glues numbers across `=`, so `… Ms. Heb. 577.10.6=04` resolves to `577.10.60=4`**
- **File:** `shared/browse_map_utils.py::normalize_shelfmark` (SHA-pinned), `shared/metadata_manager.py::resolve_system_by_shelfmark`
- **Status:** ❌ Open (found 2026-10-18)
- **Notes:** `6=04` becomes `604`, the key of `577.10.60=4`. Today's exact step, so untouched by the catalogue-form work (whose own keys strip zeros first and check the numbers). Any fix changes the pinned normalizer or adds a numeric check to the exact step; both need a baseline run.

*Triage 2026-10-25:* still open. browse_map_utils.py:228-231 keeps dots between digits but the non-word removal drops '=', so 6=04 becomes 604; the file has no commit after 2026-10-18 (git log). tests/test_catalog_shelfmark.py:91-95 shows only the catalogue-form zkey keeps the numbers apart.

## GS-48

**Shelfmark resolver: a named library with nothing matching keeps today's answer, which can be another library's record** · P3 · Search · ❌ Open · opened 2026-10-18

Where: `shared/catalog_shelfmark.py::named_library_exact`, resolve_many

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Shelfmark resolver: a named library with nothing matching keeps today's answer, which can be another library's record**
- **File:** `shared/catalog_shelfmark.py::named_library_exact` (R3 of `docs/plans/SHELFMARK_PREFIX_SCOPE_PLAN.md`)
- **Status:** ❌ Open (found 2026-10-18, Codex round 7 of the prefix-rule review)
- **Notes:** Inherited, not introduced: the library-prefix rule falls back to T1's old answer when no record of the named library and none storing the input matches. `Halper 441` -> HAS `990001035960205171` stored `441` (no Halper 441 exists); `adler MS 1512` -> a list with Mosseri `151/2` records and `990001035710205171`, whose Adler number is 1873; `BL T-S 12.123` -> the Cambridge record. Today's answer keeps the right record in 89,297 measured collection-alias cases, so dropping the fallback is wrong; the plan's owner question 2 offers: offer it as a choice (never opened directly) for an institution conflict, or not found.

*Triage 2026-10-25:* still open. catalog_shelfmark.py:609-611 ends with 'named_library_exact(...) or hits', so an empty scoped answer falls back to every old hit. tests/test_shelfmark_prefix_scope.py:125-128 still pins 'Halper 441' -> HAS441 and 'adler MS 1512' -> JTS1512 + JTS1873; no resolver commit after 2026-10-18 except the freeze work.

## GS-49

**Shelfmark resolver: T1's dotless rule and number gluing keep other shelfmarks inside a named library** · P3 · Search · ❌ Open · opened 2026-10-18

Where: `shared/catalog_shelfmark.py::named_library_exact`, `metadata_manager.py::resolve_system_by_shelfmark`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Shelfmark resolver: T1's dotless rule and number gluing keep other shelfmarks inside a named library**
- **File:** `shared/metadata_manager.py::resolve_system_by_shelfmark` (dotless exact match), `shared/browse_map_utils.py::normalize_shelfmark` (SHA-pinned)
- **Status:** ❌ Open (found 2026-10-18, Codex round 7)
- **Notes:** Inherited: the prefix rule removes other libraries' records only, so `Westminster College Ms. L-G Glass 12` still lists the Westminster record stored `L-G Glass.1.2` beside the CUL Glass 12 record, HAS `Ms. 36` lists the record stored `Ms. 3, 6`, and `JTS Ms. 2162` lists `Ms. 216.2`. Same family as the `=` gluing entry above. Fix sketch: a numeric check on T1's exact candidates (as T2 has), which changes exact answers and needs its own baseline run and an owner ruling.

*Triage 2026-10-25:* still open. named_library_exact still appends in-library key hits that do not carry the typed numbers whenever another hit carries them (catalog_shelfmark.py:274-284). tests/test_shelfmark_prefix_scope.py:118 and :121 pin 'Westminster College Ms. L-G Glass 12' -> CULGLASS + WESTG12 and 'JTS Ms. 2162' -> JTS2162 + JTS216_2.

## GS-50

**Shelfmark resolver: `Gaster Ms. 1201.17` does not offer the stub that stores `GASTER 1201.17`** · P3 · Search · ❌ Open · opened 2026-10-18

Where: `shared/catalog_shelfmark.py::named_library_exact` (whole-input key)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Shelfmark resolver: `Gaster Ms. 1201.17` does not offer the stub that stores `GASTER 1201.17`**
- **File:** `shared/catalog_shelfmark.py::named_library_exact` (whole-input key)
- **Status:** ❌ Open (found 2026-10-18, Codex round 7)
- **Notes:** Unchanged from today, incomplete: the full BL record is offered (provenance `Gaster, Moses Collection Ms. 1201.17`), but the whole-input key `gasterms1201.17` keeps the `Ms.`, so the stub keyed `gaster1201.17` is not. Fix sketch: also look up the whole input with a `Ms.` token after the prefix removed (number-checked), then re-measure the populations.

*Triage 2026-10-25:* still open. catalog_shelfmark.py:264 builds the whole key with normalize_shelfmark(whole_query), which keeps the Ms token after the prefix. tests/test_shelfmark_prefix_scope.py:128 pins 'Gaster Ms. 1201.17' -> ['BLFULL'] while line 114 'GASTER 1201.17' -> BLFULL + BLSTUB.

## GS-51

**Catalogue-form fallback (T2): the numeric check of its older keys sees only the variant the index kept** · P3 · Search · ❌ Open · opened 2026-10-18

Where: `shared/catalog_shelfmark.py::resolve_many` (`fits`, keys `a`/`b`/`r`/`c`/`d0`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Catalogue-form fallback (T2): the numeric check of its older keys sees only the variant the index kept**
- **File:** `shared/catalog_shelfmark.py::resolve_many` (`fits`, keys `a`/`b`/`r`/`c`/`d0`)
- **Status:** ❌ Open (found 2026-10-18, PR #402 Codex review, pre-existing in #400)
- **Notes:** The index keeps one variant per (record, key), and T2 compares the typed numbers with that one only, so a record that stores `GASTER 12 3` before `GASTER 123` can be rejected for an input whose numbers are (123,). PR #402 fixed this for the whole-input key `w` it adds (`_carries(..., zeros=True)`; no answer changed on today's corpus); the keys #400 added still check the kept variant. Not measured on today's corpus. Fix sketch: use the same record-wide check for every T2 key, then re-measure the populations.

*Triage 2026-10-25:* still open. catalog_shelfmark.py:628-637: shown() returns e[1] only if its numbers match, and for any tier other than 'w' otherwise returns None; only the whole-input 'w' key goes on to _carrying_variant over every stored variant (commit 41a212da, 2026-10-18).

## GS-52

**The shelfmark index is never rebuilt when NLI-cache shelfmarks arrive after its first build** · P3 · Search · ❌ Open · opened 2026-10-18

Where: `shared/metadata_manager.py::_get_shelfmark_index`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **The shelfmark index is never rebuilt when NLI-cache shelfmarks arrive after its first build**
- **File:** `shared/metadata_manager.py::_get_shelfmark_index` (a non-empty index is reused), cache writes in `fetch_nli_data`/`enrich_metadata`
- **Status:** ❌ Open (found 2026-10-18, Codex round 2 of the catalogue-form plan)
- **Notes:** A record known only from an NLI-cache shelfmark fetched later is invisible to shelfmark lookups until restart. The catalogue-form tables follow the index object, so they neither fix nor worsen it. Rebuilding on every cache write would cost seconds each time; a generation counter bumped only when a cached shelfmark is new would not.

*Triage 2026-10-25:* still open. metadata_manager.py:1943-1948 returns any non-empty cached index; _shelfmark_index is reset only at 288 (init), 417 (CSV load) and 1949 (empty index), never on a cache write, and there is no generation counter.

## GS-53

**A per-call `regex` timeout aborts the whole desktop search instead of cutting that document off** · P3 · Search · ❌ Open · opened 2026-10-12

Where: `shared/search_engine.py::execute_search` per-hit handlers, `desktop/gui_threads.py::SearchThread.run`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A per-call `regex` timeout aborts the whole desktop search instead of cutting that document off**
- **File:** `shared/search_engine.py::execute_search` (the per-hit handlers: `except (SearchBudgetExceeded, InterruptedError): raise`), `desktop/gui_threads.py::SearchThread.run`
- **Status:** ❌ Open (found 2026-10-12; pre-existing; follow-up of the native matching plan §8)
- **Notes:** A `regex` call past `GENIZAH_REGEX_TIMEOUT_SECONDS` (10 s) raises `SearchBudgetExceeded`; the per-hit handlers re-raise it and `SearchThread.run` sends it to `error_signal`, dropping the rows already found (Stop keeps them). On the `regex` route a per-document timeout could skip that document and mark the list cut off ("N+"); a total budget (an API deadline) must still abort. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. search_engine.py:4610 (and 5085, 5276) still re-raise (SearchBudgetExceeded, InterruptedError) from the per-hit handler; SearchThread.run (gui_threads.py:368-372) has no SearchBudgetExceeded case, so the TimeoutError subclass lands in 'except Exception' and goes to error_signal with the rows dropped. search_regex.py:441-442 turns the per-call timeout into SearchBudgetExceeded.

## GS-54

**A letter-level search the website stopped is shown badly on /parallels** · P3 · Search · ❌ Open · opened 2026-10-07

Where: `web/pages/parallels.py` (passage dispatch); `shared/passage_parallels.py::search_composition_logic`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A letter-level search the website stopped is shown badly on /parallels**
- **File:** `web/pages/parallels.py` (the passage dispatch, its witness panel), `shared/passage_parallels.py` (`witness_report`)
- **Status:** ❌ Open (2026-10-07)
- **Notes:** Since 2026-10-07 a letter-level search stops at the 3-minute limit and returns what it verified, marked partial. On /parallels a search the web side had to kill (60 s after the limit) is caught by a generic `except Exception` and shows nothing, with no message; the witness panel (one job per witness) marks a witness the limit cut as "searched" without saying it was partial; and in a stopped multi-witness result the witnesses that never ran still show `resolved: true` (only the `searched` count is right). The API is right (`results_cut_off`). Behind `PASSAGE_PARALLELS_ENABLED`.

*Triage 2026-10-25:* still open. The passage dispatch still catches the worker's SearchBudgetExceeded in 'except Exception' -> result_data=None with no notify (parallels.py:5046-5048); on a stop only witness_report['searched'] is rewritten (passage_parallels.py:751-752) while each entry keeps 'resolved': True (1069); parallels.py has no use of results_cut_off.

## GS-55

**Numbered-line syntax (`L1:word`) misses near spellings in Fuzzy and Variants** · P3 · Search · ❌ Open · opened 2026-10-04

Where: `shared/search_engine.py::execute_search` (L{n}: block), `shared/indexer.py::_validate_position_match`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Numbered-line syntax (`L1:word`) misses near spellings in Fuzzy and Variants**
- **File:** `shared/search_engine.py::execute_search` (the per-line `L{n}:` block), `shared/indexer.py::Indexer._validate_position_match` (`line_constraints`)
- **Status:** ❌ Open (found 2026-10-04, Codex review of PR #375)
- **Notes:** The `L{n}:` term is retrieved as the exact token and the line word is compared literally, so Fuzzy `L1:שלום` misses a line that starts with שלם while the same Fuzzy line-start search without `L1:` finds it (PR #375 added `_fuzzy_position_query` for that case only). Variants has the same gap for its own forms (שלוס). Both predate PR #375 (reproduced on v9.4.0); neither UI advertises the syntax. Fix: per-line near-spelling token sets as in `_fuzzy_position_query`, and `line_constraints` accepting a set of forms; a plain term in the same query also stays exact.

*Triage 2026-10-25:* still open. search_engine.py:4019-4037 still retrieves the L{n}: token as is and passes {line: word}; indexer.py:186-194 compares the line's first/last token with != word literally (no set of forms).

## GS-56

**A failed measurement lookup after a search still hides every row** · P3 · Search · ❌ Open · opened 2026-09-28

Where: `shared/fjms_service.py::get_measurement_summaries_batch`; `web/pages/search.py:5222`; `genizah_app.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A failed measurement lookup after a search still hides every row**
- **File:** `shared/fjms_service.py::get_measurement_summaries_batch` (returns `{}` or a partial dict on error), the post-search measurement filters (`web/pages/search.py`, `genizah_app.py`)
- **Status:** ❌ Open (2026-09-28, logged 2026-10-06)
- **Notes:** A row without measurement data is excluded by a post-search measurement filter, so a batch error looks like "no rows match" -- the same false empty #17 fixed for the pre-search lookup (design C-10).

*Triage 2026-10-25:* still open. get_measurement_summaries_batch still returns {} on a PRAGMA error (3578-3580) or a partial dict after logging the exception (3622-3624), with no failure signal; both callers use the result as the filter's data (search.py:5222, genizah_app.py:24968-24975).

## GS-57

**One Regex pattern can match differently on the desktop and on the website** · P3 · Search · ❌ Open · opened 2026-09-28

Where: `shared/search_regex.py` (in-process `regex` module; website workers use stdlib `re`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **One Regex pattern can match differently on the desktop and on the website**
- **File:** `shared/search_regex.py` (in-process `regex` module; website workers use stdlib `re`)
- **Status:** ❌ Open (2026-09-28, logged 2026-10-06)
- **Notes:** `x{e<=1}` is a fuzzy constraint in `regex` and literal text in `re`; `[[:alpha:]]` is a POSIX class in one and a nested set in the other. Such patterns get no prefilter (no match is lost), but the two apps can return different pages.

*Triage 2026-10-25:* still open. compile() validates with stdlib re.compile then compiles the same text with the regex module (search_regex.py:515-527); _call routes isolated native workers to the stdlib object (451-452) and everything else to regex, so fuzzy {e<=1} and POSIX classes keep two meanings.

## GS-58

**Lab Mode ignores Regex mode** · P3 · Search · ❌ Open · opened 2026-09-28

Where: `shared/lab_engine.py::lab_search`; `web/pages/search.py`; `desktop/gui_threads.py::LabSearchThread`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Lab Mode ignores Regex mode**
- **File:** `shared/lab_engine.py::lab_search` (never reads `mode`), web `/search` Lab switch, desktop `LabSearchThread`
- **Status:** ❌ Open (2026-09-28, logged 2026-10-06)
- **Notes:** A Regex pattern in Lab Mode is searched as words. Owner 2026-09-28: Lab Mode is minor.

*Triage 2026-10-25:* still open. shared/lab_engine.py:683-780: lab_search takes `mode` but never reads it (only the signature and docstring mention it; the body goes straight to text_to_fingerprint). Both callers still pass a Regex mode in (web/pages/search.py:5658-5663, desktop/gui_threads.py:468-471); no guard disables Lab Mode for Regex.

## GS-59

**A query starting with `R` is run as a Responsa search in the other modes** · P3 · Search · ❌ Open · opened 2026-09-25

Where: `shared/search_engine.py::parse_query_syntax`; `genizah_app.py:23088`; `web/pages/search.py:2571`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A query starting with `R` is run as a Responsa search in the other modes**
- **File:** `shared/search_engine.py::parse_query_syntax` (`prefix_map` ends with `('R', 'responsa')`), callers `genizah_app.py::start_search`, `web/pages/search.py`
- **Status:** ❌ Open (sweep 2026-09-25 #28; rechecked 2026-10-06)
- **Notes:** Outside Responsa mode a leading `R` still switches modes, so the Shelfmark search `RAR MS 85.126.35` runs as a Responsa search for `AR MS 85.126.35`. Restrict the bare `R` to the default word mode (or require a separator after it), never in Shelfmark/Title.

*Triage 2026-10-25:* still open. prefix_map still ends with ('R','responsa') and parse_query_syntax only bypasses it when responsa_mode is True (lines 3389-3432); both callers pass only is_responsa. tests/test_responsa_regression.py:365 even pins 'R test' -> responsa.

## GS-60

**FGP stored HTML entities (`&deg;` in ~520 rows) render differently per surface** · P3 · Browse and viewer · ❌ Open · opened 2026-09-25

Where: `web/pages/search_results.py::_fgp_text_for_display`; `web/pages/browse.py`; `desktop/result_dialog.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **FGP stored HTML entities (`&deg;` in ~520 rows) render differently per surface**
- **File:** `web/pages/search_results.py::_fgp_text_for_display` (Quick View decodes), Browse (shows `&deg;` literally), desktop `desktop/result_dialog.py::_rd_display_pgp_text` and `genizah_app.py` Browse (render edition text as unescaped Qt HTML, so tag-like notation is dropped there)
- **Status:** ❌ Open (2026-09-25)
- **Notes:** Do not decode in `shared/fgp_service.py`: the desktop relies on the entities staying encoded, and decoding there would double-decode. Fix per surface: Browse web-side decode like Quick View; desktop escape-then-mark.

*Triage 2026-10-25:* still open. Only web/pages/search_results.py:173-181 (Quick View) unescapes; web/pages/browse.py:3838 shows the FGP section_text via ui.label with no unescape, and desktop/result_dialog.py:2028-2054 renders edition text as unescaped Qt HTML. No html.unescape exists in browse.py, shared/fgp_service.py or desktop/ (grep).

## GS-61

**Browse: a paged deep-link URL still carries `highlight`, so a reload lets the NEW folio claim the search phrase** · P3 · Browse and viewer · ❌ Open · opened 2026-09-03

Where: `web/pages/browse.py::_update_browser_url` (877-906), _search_scope_phrase (835-859)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Browse: a paged deep-link URL still carries `highlight`, so a reload lets the NEW folio claim the search phrase**
- **File:** `web/pages/browse.py::_update_browser_url` (writes `_url_state['highlight']` on every same-manuscript page), `_search_scope_phrase`
- **Status:** ❌ Open (found 2026-09-03, Codex P2, PR #333 round 16 — logged under the review budget, not fixed in the PR)
- **Notes:** `_search_scope_phrase` returns None once the reader pages past the deep-linked folio, but the URL keeps `?highlight=`, so a reload or share builds a state with `highlight_scope=None` and the new folio claims the phrase (the SEED-033 `must_contain` override can then pick an unrelated FGP edition). Only on a structurally unalignable manuscript and a reloaded paged URL. Fix: drop `highlight` from the URL when the scope differs, or encode the owning folio. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. _update_browser_url only clears _url_state['highlight'] when the sys_id changes (888-889) and otherwise writes params['highlight'] on every page change (898-899), while _search_scope_phrase returns None past the owning folio, so the URL and the in-memory scope disagree exactly as described.

## GS-62

**`switchImageCredit()` updates the FIRST `data-role="image-credit"` node in the document — a hidden earlier Advanced View, not the open one** · P3 · Browse and viewer · ❌ Open · opened 2026-09-03

Where: `web/static/manuscript_viewer.js::switchImageCredit`; `search_results.py::open_advanced_dialog`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **`switchImageCredit()` updates the FIRST `data-role="image-credit"` node in the document — a hidden earlier Advanced View, not the open one**
- **File:** `web/static/manuscript_viewer.js::switchImageCredit` (`document.querySelector`), `web/pages/search_results.py` (`ui.dialog()` per open, never deleted)
- **Status:** ❌ Open (found 2026-09-03, Codex P2, PR #333 round 16 — logged under the review budget, not fixed in the PR)
- **Notes:** Each Advanced View open creates a new maximized `ui.dialog()`; closing hides it, nothing deletes it, so after the second open two credit nodes exist and the Oxford→NLI fallback rewrites the hidden one — the visible dialog keeps the Bodleian credit under an NLI image. Fix: resolve the label/link relative to the failing `<img>` (closest viewer root) or pass a viewer-specific root into the helper; alternatively delete the dialog on hide.

*Triage 2026-10-25:* still open. manuscript_viewer.js:109-110 still uses document.querySelector('[data-role="image-credit"]'); open_advanced_dialog builds a new ui.dialog() on each open (search_results.py:1205) and never deletes it (no .delete()/hide handler, ends with dialog.open() at :2634), so earlier dialogs' credit nodes stay in the DOM.

## GS-63

**FGP version menu still lists whole-document rows unrelated to the displayed folio (demoted from the default, not hidden)** · P3 · Browse and viewer · ❌ Open · opened 2026-09-02

Where: `shared/fgp_service.py::choose_default_source`, _select_fgp_editions_by_similarity

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **FGP version menu still lists whole-document rows unrelated to the displayed folio (demoted from the default, not hidden)**
- **File:** `shared/fgp_service.py::choose_default_source` / `_select_fgp_editions_by_similarity`; upstream FGP export
- **Status:** ❌ Open (2026-09-02, policy)
- **Notes:** Owner ruling 2026-09-02 = text-match DEMOTION + incipit labels (done). Hiding rows that fail the word-overlap floor is a separate policy call. Data fact: the FGP export duplicates codex-level catalogue excerpts into every part folder — MS heb. g.2: 10 identical rows × 82 parts (829 rows); 23 of 144 multi-part Oxford codices are fully duplicated; all 6,733 Oxford FGP rows have `image_side=NULL`. Upstream data, not app code.

*Triage 2026-10-25:* still open. shared/fgp_service.py:640-850 only demotes the default (reason 'demote_no_text_match'); _select_fgp_editions_by_similarity (869-900) falls back to folio_eds or all sources when nothing clears _SIM_FLOOR, so unrelated whole-doc rows still show. Last change to the file 2026-10-21 (4cb7273b, unrelated); the owner's hide-or-not policy call is still pending.

## GS-64

**FGP alignment can land on an adjacent folio for Toronto `990001996710205171`** · P3 · Browse and viewer · ❌ Open · opened 2026-06-22

Where: `shared/fgp_service.py::_select_fgp_editions_by_similarity`, _content_similarity

Moved 2026-10-25 from `4.5 FGP Transcriptions — data ready, integration pending (2026-06-18)`, where it read:

- **Item:** **FGP alignment can land on an adjacent folio for Toronto `990001996710205171`**
- **Status:** ❌ Open (2026-06-22; the per-folio alignment itself shipped 2026-09-02, `c710c6f2`)
- **Notes:** One continuous work (44 folios, garbled microfilm HTR), so adjacent folios are textually close (inter-folio similarity up to 0.39 against ~0.15 elsewhere) and `shared/fgp_service.py::_select_fgp_editions_by_similarity` occasionally picks a neighbour. Fix: a more discriminative metric (rare-word / TF-IDF weighting, or word-sequence overlap). FGP translations cannot be similarity-aligned (different language) and stay folio-matched.

*Triage 2026-10-25:* still open. shared/fgp_service.py:848-857 _content_similarity is still plain token overlap / min(|a|,|b|) and :869-905 picks the argmax edition; no rare-word / TF-IDF weighting was added (commits since 2026-06-22 on this file are the chooser/credit work, none touch the metric).

## GS-65

**The displayed image does not match the V0.8 / FGP text on some manuscripts (NLI `990035100760205171`, Columbia `990001372620205171`)** · P3 · Browse and viewer · ❌ Open · opened 2026-06-22

Where: `web/api.py::fetch_fl_ids_from_nli`; desktop fl_ids by position

Moved 2026-10-25 from `4.5 FGP Transcriptions — data ready, integration pending (2026-06-18)`, where it read:

- **Item:** **The displayed image does not match the V0.8 / FGP text on some manuscripts (NLI `990035100760205171`, Columbia `990001372620205171`)**
- **Status:** ❌ Open (pre-existing, noted 2026-06-22; cause found 2026-10-06)
- **Notes:** Separate from the FGP chooser; it affects V0.8 too. Reproduced 2026-10-06: page N shows canvas N of NLI's CURRENT manifest (`web/api.py::fetch_fl_ids_from_nli`; the desktop also takes `fl_ids` by position), and NLI has re-photographed both records, so the transcription-time FL ids return 500 and canvas N is no longer page N. Fix: keep the transcription-time FL per page and match it (or the folio label) to the live manifest. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. web/api.py:621-700 still returns the live NLI manifest's FL ids in canvas order (cache keys system_id/suffix only) and page N uses canvas N; no transcription-time FL per page is stored or matched anywhere (no commit since 2026-10-06 touched this logic: only the image-store/caching commits bac67736, d67984ad, 79d56ab8).

## GS-66

**Desktop image cache: a 400 px thumbnail and the 2000 px viewer image of the same page share one file** · P3 · Desktop app · ❌ Open · opened 2026-10-21

Where: `desktop/image_loader.py::ImageLoaderThread.run` (line 68); metadata_manager._resolve_thumbnail

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Desktop image cache: a 400 px thumbnail and the 2000 px viewer image of the same page share one file**
- **File:** `desktop/image_loader.py::ImageLoaderThread.run` (cache name `FL<id>_v2.jpg` from the FL id alone); thumbnails from `shared/metadata_manager.py::_resolve_thumbnail` (`/full/400,`)
- **Status:** ❌ Open (found 2026-10-21 by reading the code; not reproduced)
- **Notes:** Every `ImageLoaderThread` names its cache file by FL id only, whatever the requested width. A thumbnail loaded first (Joins Lab grid, result-dialog thumbnail, Lists preview, Browse thumbnails) is saved as the file the manuscript viewer then reads for its 2000 px image, so page 1 can show enlarged and blurry. Fix: key the file by FL id and width (or by URL). Fold into the desktop phase of [docs/plans/NLI_THUMBNAIL_PREFETCH_PLAN.md](plans/NLI_THUMBNAIL_PREFETCH_PLAN.md) section 7. ✋ *By hand:* open a manuscript's thumbnail in the Joins Lab, then the same manuscript in the viewer, with an empty `images_cache/`.

*Triage 2026-10-25:* still open. desktop/image_loader.py:66-68 names the cache file FL<id>_v2.jpg from the FL id alone, whatever the URL's width; thumbnails still come from /full/400, (metadata_manager.py:1631) and are loaded by ImageLoaderThread (result_dialog.py:4222, genizah_app.py:4902, join_workbench.py). No commit since 2026-10-21.

## GS-67

**Quit still waits, at interpreter exit, for an NLI request in flight on the shared metadata executor** · P3 · Desktop app · ❌ Open · opened 2026-10-20

Where: `shared/metadata_manager.py` (nli_executor, line 289); `genizah_app.py::_stop_meta_loader_for_close`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Quit still waits, at interpreter exit, for an NLI request in flight on the shared metadata executor**
- **File:** `shared/metadata_manager.py` (`nli_executor`, a `ThreadPoolExecutor`), `genizah_app.py::_stop_meta_loader_for_close`
- **Status:** ❌ Open (found 2026-10-20 while fixing the unbounded quit wait)
- **Notes:** The quit no longer waits on the loader without limit, but Python joins a `ThreadPoolExecutor`'s worker threads at exit, so the process (and `app.lock`) lives until the request already running returns. That is bounded by `NLI_CONNECT_TIMEOUT`/`NLI_MARC_READ_TIMEOUT` (3 s each, two attempts) except where they do not apply: a DNS stall, or a response that keeps trickling in. Reasoned from the code, not reproduced. Fix if it is ever seen: an executor whose threads are not joined at exit, or an exit that does not wait on that one.

*Triage 2026-10-25:* still open. nli_executor is still a plain ThreadPoolExecutor(max_workers=2) (shared/metadata_manager.py:289), which Python joins at exit; nothing in genizah_app.py or shared/metadata_manager.py calls os._exit or avoids the join (grep), and the close path only bounds the loader wait to 2 s (genizah_app.py:359, 36349).

## GS-68

**Desktop Excel export: a search-within whose completion step Stop interrupted exports the plain count** · P3 · Desktop app · ❌ Open · opened 2026-10-20

Where: `genizah_app.py::export_results` (`_run_left_matches_out()`), the refinement chain

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Desktop Excel export: a search-within whose completion step Stop interrupted exports the plain count**
- **File:** `genizah_app.py::export_results` (`_run_left_matches_out()`), the refinement chain
- **Status:** ❌ Open (found 2026-10-20 while fixing the Excel cut-off count)
- **Notes:** The website also writes "N+" for a search run within an incomplete set (`_restricted_to_incomplete`). On the desktop that happens only when Stop interrupts the completion step before a search-within: the new step is marked capped, but its own run was complete, so the export (and the status line) show the plain number. The chain's last step does not always match the rows on screen, so the export does not read it yet.

*Triage 2026-10-25:* still open. genizah_app.py:28201 passes result_count_capped=_run_left_matches_out(), which reads only this run's _search_cutoff (25324-25327); the step's own flag from _new_step_marks (25329-25336, inherits parent.result_count_capped) is never read by the export or by _shown_results_capped (25319-25322). tests/test_desktop_export_cut_off_count.py covers only _search_cutoff.

## GS-69

**The Quoted texts dialog (old Filter Text) is wider than a small screen at 300% scaling** · P3 · Desktop app · ❌ Open · opened 2026-10-14

Where: `desktop/filter_text_dialog.py::FilterTextDialog` (the Sefaria buttons row and the intro label)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **The Quoted texts dialog (old Filter Text) is wider than a small screen at 300% scaling**
- **File:** `desktop/filter_text_dialog.py::FilterTextDialog` (the Sefaria buttons row and the intro label)
- **Status:** ❌ Open (found 2026-10-14 by Codex, pre-existing)
- **Notes:** Codex measured the dialog needing more than the width of a 1366-pixel screen at 300% scaling, so its edge is cut off. The conditions strip rows had the same problem; they now elide (`31d646bb`, merged in #399). Fix: wrap the button row in a FlowWidget and wrap the intro label.

*Triage 2026-10-25:* still open. desktop/filter_text_dialog.py:559 still adds the intro QLabel without word wrap and :565-592 puts five buttons in one QHBoxLayout (no FlowWidget/OverflowRow); the only later commit (8ebb6089, 2026-10-18) concerns the Sefaria fetch thread.

## GS-70

**Desktop viewer runs a scholar's own Regex highlight pattern through plain `re` on the UI thread with no bound** · P3 · Desktop app · ❌ Open · opened 2026-10-12

Where: `desktop/widgets/__init__.py::mark_pattern_hits`, `desktop/result_dialog.py`, `genizah_app.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Desktop viewer runs a scholar's own Regex highlight pattern through plain `re` on the UI thread with no bound**
- **File:** `desktop/widgets/__init__.py::mark_pattern_hits`, `desktop/result_dialog.py` (`load_page`, `_apply_source_highlights`, `load_local_page`), `genizah_app.py::_browse_mark_search_hits`; the pattern comes from `shared/search_engine.py::build_regex_pattern` (Regex branch) and `execute_search` (`'highlight_pattern': pattern_str`)
- **Status:** ❌ Open (found 2026-10-12; pre-existing)
- **Notes:** In Regex mode `highlight_pattern` is the scholar's own text, and the viewer compiles and substitutes it with stdlib `re` on the Qt thread with no timeout (the search itself ran under `regex` with one), also for pages opened from Browse. No freezing pattern has been found; nothing bounds it. Fix: compile through `shared.search_regex` under `search_budget`, or run it off the UI thread. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. desktop/widgets/__init__.py:79-80 does re.compile(pattern_str, flags).subn(...) on the caller's (Qt) thread; desktop/ imports shared.search_regex only for cancel_check (gui_threads.py:17), so no search_budget or timeout wraps any viewer compile.

## GS-71

**Desktop Joins Lab searches at the main window's pair count** · P3 · Desktop app · ❌ Open · opened 2026-10-07

Where: `desktop/variant_run_settings.py::basic_changes_searcher`, `desktop/join_workbench.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Desktop Joins Lab searches at the main window's pair count**
- **File:** `desktop/variant_run_settings.py::basic_changes_searcher`, `desktop/join_workbench.py`
- **Status:** ❌ Open (2026-10-07)
- **Notes:** Since 2026-10-07 Joins Lab runs at Basic's Num Changes (owner ruling), but its Variants and Responsa-variants searches still use the pair count the main window last set (e.g. 150 after a Maximum search). The website's Joins Lab uses Basic's 30 pairs. Bind the pair count too (`engine_with_variant_settings` already takes it).

*Triage 2026-10-25:* still open. basic_changes_searcher's bind() returns only {'variant_max_changes': ...['basic']} (variant_run_settings.py:152) and never variant_pairs_count; join_workbench.py:5337 wraps app.searcher with it. File unchanged since 2026-10-07.

## GS-72

**Desktop small gaps: no copy confirmation, lists/manuscript saves ignore last folder, a few strings lack Hebrew, no viewer paging keys** · P3 · Desktop app · ❌ Partly fixed · opened 2026-10-06

Where: `genizah_app.py`, `desktop/result_dialog.py`, `shared/genizah_translations.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Desktop small gaps from the 2026-09-25 sweep**
- **File:** `genizah_app.py`, `desktop/result_dialog.py`, `shared/genizah_translations.py`
- **Status:** ❌ Open (sweep L2, L6, L7, #39; rechecked 2026-10-06)
- **Notes:** (a) Copy file name / Copy file location show no confirmation (`act_copy_name`, `act_copy_loc`). (b) Personal Lists export/import and Browse Save Manuscript always open in the Reports folder (`_get_default_save_folder`/`_save_last_folder` are not used there). (c) About 11 `tr()` strings have no Hebrew (e.g. 'Filter Local', 'Only Local', 'No Local', and 'Identification Reviews' in `web/pages/admin.py`); the guard `tests/test_desktop_passage_gate.py::test_every_tr_string_has_a_hebrew_entry` covers 3 files and no other web page. (d) No keyboard shortcuts to page through images or results in the viewer, or to focus the search box.

*Triage 2026-10-25:* partly fixed. Fixed: (d) 'focus the search box' - Ctrl+F -> _focus_section_field (genizah_app.py:2380, 16708; commits caecd7e0, d52619b9, 2026-10-24/25); also the three 'Filter/Only/No Local' labels pick their Hebrew text by CURRENT_LANG (25218-25234), so they are not shown untranslated. **Still open:** (a) Copy filename/file location still show no confirmation (genizah_app.py:5698, 5709); (b) Export List/Import List/Save Manuscript still start in Config.REPORTS_DIR (11508, 14938, 14982, 15033, 15046, 14653); (c) tr('Identification Reviews') in web/pages/admin.py:392 has no TRANSLATIONS entry; (d) no keyboard paging of images/results in the manuscript viewer (only the fullscreen window has arrow keys, viewers.py:610).

## GS-73

**My Library: a cut-off search with no matches offers the Genizah corpus as if My Library had none** · P3 · Desktop app · ❌ Open · opened 2026-10-06

Where: `genizah_app.py::on_search_finished` (24157), `::_update_local_scope_strip` (22964)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **My Library: a cut-off search with no matches offers the Genizah corpus as if My Library had none**
- **File:** `genizah_app.py::on_search_finished` (zero-result branch: `_update_local_scope_strip(0, cancelled=...)`)
- **Status:** ❌ Open (2026-10-06)
- **Notes:** Since 2026-10-06 the status line says "(Partial results)" when a cut-off search found nothing, but the My Library hint below it still reads "No results in My Library" and offers the Genizah corpus. Only a My Library search that reached the 50,000-candidate limit without a match (a Regex pattern with no prefilter) gets there. `tests/test_local_scope_zero_result_hint.py` pins the current source line.

*Triage 2026-10-25:* still open. on_search_finished calls _update_local_scope_strip(0, cancelled=was_cancelled) (24157) ahead of the _run_left_matches_out() check that follows, so a cut-off run that was not user-cancelled still shows the hint; tests/test_local_scope_zero_result_hint.py:194 pins that exact call.

## GS-74

**Desktop: starting a shelfmark fetch while one runs blocks the window until the running request ends** · P3 · Desktop app · ❌ Open · opened 2026-10-04

Where: `genizah_app.py::start_metadata_loading`, `shared/metadata_manager.py::batch_fetch_shelfmarks`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Desktop: starting a shelfmark fetch while one runs blocks the window until the running request ends**
- **File:** `genizah_app.py::start_metadata_loading` (`request_cancel()` then `wait()`, no timeout), `shared/metadata_manager.py::batch_fetch_shelfmarks` (`as_completed` checks cancel only between requests)
- **Status:** ❌ Open (found 2026-10-04)
- **Notes:** `start_metadata_loading` cancels and then `wait()`s with no timeout, and `batch_fetch_shelfmarks` checks cancel only between requests, so fast scrolling, "Load more" or the Composition tab can block the window for up to one NLI request (3 + 3 s, two attempts). Rare for search results (only ids missing from `libraries.csv`). Fix: cancel without waiting, keeping the old loader referenced until it ends. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. genizah_app.py:26417-26419 still does request_cancel() then wait() with no timeout on the UI thread; metadata_manager.py:1740-1741 checks check_cancel only as each future completes, not inside a request.

## GS-75

**Fragment Puzzle follow-ups after the 2026-09-27 work-loss fix** · P3 · Desktop app · ❌ Open · opened 2026-09-27

Where: `desktop/puzzle.py`, `desktop/corrections_ui.py`, `web/pages/puzzle.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Fragment Puzzle follow-ups after the 2026-09-27 work-loss fix**
- **File:** `desktop/puzzle.py`, `desktop/corrections_ui.py`, `web/pages/puzzle.py`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** Twelve items, (a)-(l): a fragment whose image failed is kept but invisible and unremovable; a failed folio or threshold reload keeps the old image; some prompts still show English buttons in Hebrew; `generate_thumbnail` runs on the UI thread; the close waits 2 s per loader in turn; z-order is not stored; 'Open in Puzzle' from Discoveries or Joins closes its dialog after Cancel; a Windows logoff is blocked by the prompt; the web autosave may ignore a failed save; two deferred-close edge cases; and an in-flight `_start_meta_resolve` lookup is not counted as pending work. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. desktop/puzzle.py has no commit since 09-27 except da620f50 (shelfmarks). Still present: (c) static QMessageBox.question at puzzle.py:2843/2856/2968; (d) generate_thumbnail in _thumbnail_for_save (2571) from _write_open_join; (e) `t.wait(2000)` per loader in closeEvent (3238); (f) PuzzleFragment (shared/puzzle_model.py:17-40) has no z field; (g) corrections_ui.py:1529/3988/5430 accept() whatever the answer; (l) _pending_additions (2541) ignores _meta_threads. (i) web _auto_save_now only logs a warning on a failed save (puzzle.py:2537-2539). (a)(b)(h)(j)(k) not individually re-checked.

## GS-76

**Joins Lab and main-window threads not covered by the 2026-09-27 lifetime fix** · P3 · Desktop app · ❌ Open · opened 2026-09-27

Where: `genizah_app.py`, `desktop/join_workbench.py`, `desktop/image_loader.py`, `desktop/viewers.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Joins Lab and main-window threads not covered by the 2026-09-27 lifetime fix**
- **File:** `genizah_app.py`, `desktop/join_workbench.py`, `desktop/image_loader.py`, `desktop/viewers.py`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** (a) seven main-window QThread holders are still replaced without being kept alive; (b) retention callbacks outside the Lab keep workers alive until the cycle collector runs; (c) CompareDialog folio stepping and card flips have the late-result race (display only); (d) `ImageLoaderThread` writes the disk cache in place, so a truncated JPEG can be accepted; (e) closing the Lab does not stop its workers; (f) quit time is unbounded overall. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. (a) genizah_app.py:10140/10453/11116 replace ReadingDeskWorker / VolumeManifestThread holders without _retire_worker or _keep_until_finished (only meta_loader at 26437 is retired); (d) ImageLoaderThread.run still does img.save(local_path, 'JPEG', 85) in place (image_loader.py:133); (e) Lab closeEvent (join_workbench.py:6120) cancels only the per-anchor workers, not search/enrich workers. (b)(c)(f) not re-checked; no later commit touches these.

## GS-77

**`is_logged_in()` is a network request on the UI thread** · P3 · Desktop app · ❌ Open · opened 2026-09-27

Where: `desktop/supabase_corrections_client.py::is_logged_in`; `genizah_app.py::_update_corner_login_state`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **`is_logged_in()` is a network request on the UI thread**
- **File:** `desktop/supabase_corrections_client.py::is_logged_in`; `genizah_app.py` (`_update_corner_login_state`, `_corner_login_clicked`)
- **Status:** ❌ Open (2026-09-27)
- **Notes:** On every corner-button update while logged in (none after a log-out), and on a Logout click, before the log-out starts. Pre-existing.

*Triage 2026-10-25:* still open. supabase_corrections_client.py:586-595 calls client.auth.get_user() (an HTTP call); genizah_app.py:2687 (_update_corner_login_state) and 2779 (_corner_login_clicked, now the account menu) call it directly on the UI thread. No change since e051d29e.

## GS-78

**Community-panel refreshes still make network calls on the UI thread** · P3 · Desktop app · ❌ Open · opened 2026-09-27

Where: `genizah_app.py::_refresh_community_panels`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Community-panel refreshes still make network calls on the UI thread**
- **File:** `genizah_app.py::_refresh_community_panels`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** Every refresh but the one after a log-out runs `is_server_available()` (a host lookup its 0.5 s timeout does not cover) and up to five API calls. Pre-existing.

*Triage 2026-10-25:* still open. genizah_app.py:15386-15410: every refresh except cached_only calls corrections_client.is_server_available() (socket.connect_ex, supabase_corrections_client.py:550-577, whose 0.5 s timeout does not cover the DNS lookup) and then the five panel refreshes make API calls inline.

## GS-79

**After a log-out a panel with nothing cached says "Offline - no cached data available"** · P3 · Desktop app · ❌ Open · opened 2026-09-27

Where: `genizah_app.py::_refresh_community_panels` (`cached_only`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **After a log-out a panel with nothing cached says "Offline - no cached data available"**
- **File:** `genizah_app.py::_refresh_community_panels` (`cached_only`)
- **Status:** ❌ Open (2026-09-27)
- **Notes:** Although the computer may be online; the next ordinary refresh replaces it.

*Triage 2026-10-25:* still open. genizah_app.py:2941 (_finish_logout) calls _refresh_community_panels(cached_only=True) -> skip_api_calls=True; each panel then shows the 'Offline - no cached data available' item when it has no cache (15466, 15614, 15735, 16030 and the else branches).

## GS-80

**The "already in sync" check at log-in ignores whose cloud ids the lists hold** · P3 · Desktop app · ❌ Open · opened 2026-09-27

Where: `genizah_app.py::_on_lists_preview`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **The "already in sync" check at log-in ignores whose cloud ids the lists hold**
- **File:** `genizah_app.py::_on_lists_preview`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** Same list names plus any cloud id skip the sync choice, even when the ids are another account's. Pre-existing; Sync lists now opens the choice.

*Triage 2026-10-25:* still open. genizah_app.py:3079-3125: skips the sync choice when local_names == cloud_names and `local_with_cloud_ids > 0` (any list with a cloud_id); it never consults store['cloud_account']. bbd6673a (account guard) acts later, at the first sync, not in this check.

## GS-81

**"Notes that differ" also counts entries whose tags differ** · P3 · Desktop app · ❌ Open · opened 2026-09-27

Where: `genizah_app.py::_sync_note_lines`, `shared/lists_sync.py::count_differing_notes`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **"Notes that differ" also counts entries whose tags differ**
- **File:** `genizah_app.py::_sync_note_lines`, `shared/lists_sync.py::count_differing_notes`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** `notes_differing` counts entries whose record has `differs`, set for a note or a tag set; the Download lines already count tags apart.

*Triage 2026-10-25:* still open. count_differing_notes (lists_sync.py:255-281) counts any record with `differs` (set for a note or a tag set); genizah_app.py::_sync_note_lines (3220+) shows it as 'Notes that differ ... {}' with no separate tag count for uploads.

## GS-82

**The desktop note field is one line, so a note holding both versions shows as one** · P3 · Desktop app · ❌ Open · opened 2026-09-27

Where: `genizah_app.py` (`lists_detail_note`, a `QLineEdit`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **The desktop note field is one line, so a note holding both versions shows as one**
- **File:** `genizah_app.py` (`lists_detail_note`, a `QLineEdit`)
- **Status:** ❌ Open (2026-09-27)
- **Notes:** Download/Merge join differing notes with newlines and a marker line; the field keeps them through edits (checked offscreen) but shows one line.

*Triage 2026-10-25:* still open. genizah_app.py:13081 self.lists_detail_note = QLineEdit(); setText(item.get('note')) at 13850 and text() at 14002. Not changed since creation (82c85fca, 2026-01-10).

## GS-83

**`joins_cache.pkl` and `lab/lab_config.json` are written in place** · P3 · Desktop app · ❌ Open · opened 2026-09-25

Where: `shared/joins_manager.py`, `shared/lab_settings.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **`joins_cache.pkl` and `lab/lab_config.json` are written in place**
- **File:** `shared/joins_manager.py`, `shared/lab_settings.py`
- **Status:** ❌ Open (2026-09-25)
- **Notes:** A torn `joins_cache.pkl` silently drops the offline queue of pending join creates/deletes; `lab_config.json` is written by the desktop (the website stopped writing it 2026-10-06). Use `shared/atomic_io.write_bytes_atomic`.

*Triage 2026-10-25:* still open. joins_manager.save still does open(JOINS_FILE,'wb') + pickle.dump; lab_settings.save still does open(LAB_CONFIG_FILE,'w') + json.dump; `git log -S write_bytes_atomic` finds no commit for either file.

## GS-84

**After the all-terms rerender, scrolling adds rows the all-terms filter excluded** · P3 · Desktop app · ❌ Open · opened 2026-09-25

Where: `genizah_app.py::_apply_all_terms_filter_and_rerender`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **After the all-terms rerender, scrolling adds rows the all-terms filter excluded**
- **File:** `genizah_app.py::_apply_all_terms_filter_and_rerender`
- **Status:** ❌ Open (2026-09-25)
- **Notes:** The rerender restores the unfiltered `last_results` after its first batch, so later batches come from the unfiltered list ("Load more results" is hidden in that view for this reason).

*Triage 2026-10-25:* still open. The rerender still restores the unfiltered last_results after the first batch (24868-24870). Only the Load-more BUTTON is guarded (_all_terms_view_active, 23850); check_scroll_load (23775-23778) calls load_next_batch unguarded, and load_next_batch (23886) reads last_results[results_loaded:].

## GS-85

**Search-tab `excluded_shelfmarks` are never applied** · P3 · Desktop app · ❌ Open · opened 2026-09-25

Where: `genizah_app.py` (Search Exclude Manuscripts)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Search-tab `excluded_shelfmarks` are never applied**
- **File:** `genizah_app.py` (Search Exclude Manuscripts)
- **Status:** ❌ Open (2026-09-25)
- **Notes:** Only `excluded_sys_ids` hides rows; entries kept as shelfmarks do nothing on Search.

*Triage 2026-10-25:* still open. _row_is_excluded (25290-25300) tests only word_excluded_sys_ids and excluded_sys_ids; self.excluded_shelfmarks is read only to save/restore (35089, 35290, 35303), whereas Composition does use its shelfmarks (29918-29924).

## GS-86

**New pressed during the startup restore is undone by the restore** · P3 · Desktop app · ❌ Open · opened 2026-09-25

Where: `genizah_app.py::_restore_session` (`processEvents` calls)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **New pressed during the startup restore is undone by the restore**
- **File:** `genizah_app.py::_restore_session` (`processEvents` calls)
- **Status:** ❌ Open (2026-09-25)
- **Notes:** A queued click on New runs mid-restore; the restore then re-applies the saved state. Before 2026-09-25 it deleted the session file instead.

*Triage 2026-10-25:* still open. _restore_session still pumps the event loop (QApplication.processEvents at genizah_app.py:35594, 35637, 35747) and _reset_search (genizah_app.py:23626) has no _restoring_session guard, nor is btn_reset_search (6036) disabled during the restore; no commit since 2026-09-25 adds one.

## GS-87

**The installer's language choice is ignored: a fresh install opens in English** · P3 · Desktop app · ❌ Open · opened 2026-09-25

Where: `CompileScriptGenizah.iss` ([Languages], [Run]), `genizah_core.py::load_language`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **The installer's language choice is ignored: a fresh install opens in English**
- **File:** `CompileScriptGenizah.iss` ([Languages], [Run]), `genizah_core.py::load_language`
- **Status:** ❌ Open (sweep 2026-09-25 #31; rechecked 2026-10-06)
- **Notes:** With no `lang.pkl`, `load_language()` returns `'en'`; the installer offers Hebrew but records the choice nowhere the app reads. Pass it on (an argument on the [Run] line, or a file the first launch reads); keep it desktop-only, since `genizah_core` is also imported by the web server.

*Triage 2026-10-25:* still open. load_language() still returns 'en' when Config.LANGUAGE_FILE is absent, and the [Run] entry launches the exe with no language argument or file; no other first-launch language source exists (CURRENT_LANG = load_language(), genizah_core.py:618).

## GS-88

**Update download: Cancel can wait up to 600 s, closing leaves the download running, and a cancel is reported as a failure** · P3 · Desktop app · ❌ Open · opened 2026-09-25

Where: `desktop/gui_threads.py::UpdateDownloaderThread`; `desktop/update_ui.py` (closeEvent, execute_update)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Update download: Cancel can wait up to 600 s, closing leaves the download running, and a cancel is reported as a failure**
- **File:** `desktop/gui_threads.py::UpdateDownloaderThread` (`timeout=600`, cancel checked between 1 MB chunks), `desktop/update_ui.py` (`closeEvent`, `on_download_finished`, `execute_update`)
- **Status:** ❌ Open (sweep 2026-09-25 #35; rechecked 2026-10-06)
- **Notes:** A stalled read blocks Cancel until the timeout; closing the dialog blocks the UI for 3 s and then closes whether or not the thread stopped; a deliberate cancel shows "Download Failed ... download manually?". The downloaded installer is run with `/VERYSILENT` without checking its size against the release asset.

*Triage 2026-10-25:* still open. requests.get(..., timeout=600) with cancel checked only between 1 MB chunks (gui_threads.py:1582-1596); closeEvent does wait(3000) then accepts (update_ui.py:586); a cancel emits (False, 'Download cancelled') which on_download_finished shows as 'Download Failed' (470-494); Popen of /VERYSILENT without a size check (545).

## GS-89

**"Update Research Data" leaves enrichment on the old databases until restart** · P3 · Desktop app · ❌ Open · opened 2026-09-25

Where: `shared/metadata_manager.py` (_fjms_svc, _nli_crossref_svc), `genizah_app.py:33905`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **"Update Research Data" leaves enrichment on the old databases until restart**
- **File:** `shared/metadata_manager.py` (`_get_fjms_service`, `_get_crossref_service`: private module-level instances), `genizah_app.py::_reset_sidecar_connections`
- **Status:** ❌ Open (sweep 2026-09-25 #42; rechecked 2026-10-06)
- **Notes:** The reset closes the shared services but not `metadata_manager`'s own copies, which `enrich_metadata` reads, so catalog and NLI cross-reference enrichment keep the old files although the app says the new data is used at once. Reachable only when the desktop data updater has a release to download.

*Triage 2026-10-25:* still open. _reset_sidecar_connections calls only reset_pgp_service/reset_fjms_service/reset_nli_crossref_service (genizah_app.py:33907-33912); metadata_manager still holds module-level _fjms_svc and _nli_crossref_svc with no reset hook (grep finds none), and enrich_metadata uses them (lines 1142, 1352).

## GS-90

**Manuscript Viewer compact mode has no image or text toggle** · P3 · Desktop app · ❌ Open · opened 2026-09-24

Where: `desktop/result_dialog.py` (`compact_actions`, `btn_toggle_image`, `btn_toggle_text`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Manuscript Viewer compact mode has no image or text toggle**
- **File:** `desktop/result_dialog.py` (`compact_actions`, `btn_toggle_image`, `btn_toggle_text`)
- **Status:** ❌ Open (2026-09-24, Codex on #362)
- **Notes:** Compact mode hides the header, where both pane toggles live, and the compact bar has twins for every other action but these two, so showing only the image or only the text first needs the ▲ toggle (the image gap predates #362). Fix: compact twins kept in step with the full buttons (blockSignals both ways), with the same visibility rules. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. The compact bar (desktop/result_dialog.py:342-476) has twins for ext-info, add-list, bib, catalog, PGP, joins, cite, web, translations but none for the image/text toggles, which are only in the full actions_row (:805-806); _toggle_compact_mode (:997-1000) hides that header.

## GS-91

**Website: Back to a search-within or visual-similarity search whose run is no longer kept runs it over the whole corpus** · P3 · Website · ❌ Open · opened 2026-10-22

Where: `web/pages/search.py::_search_url` (5248), page start elif initial_query (6547); `web/search_runs.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Website: Back to a search-within or visual-similarity search whose run is no longer kept runs it over the whole corpus**
- **File:** `web/pages/search.py` (`_search_url`, page start `elif initial_query:`), `web/search_runs.py` (eviction: 30 min, 5 per browser, 100 in all)
- **Status:** ❌ Open (2026-10-22)
- **Notes:** A scoped search's address carries its query and mode but not its scope (the refinement chain, or the visual-similarity set). Back restores it from its run (PR #436) or from the shared user-storage copy; when neither still holds it -- the run evicted, another tab's search in the shared copy -- the page searches the address's query unscoped, and the visitor sees whole-corpus results under it. Same before PR #436 (the address was then `?q=` alone). Raised by Codex on #436 (round 12) and left out of it. Fix sketch: add `scoped=1` to a scoped run's address; at page start, an unmatched scoped address fills the query box and says the search was within earlier results instead of running it.

*Triage 2026-10-25:* still open. _search_url emits only q, tag, mode, Responsa options and run=<stamp> (web/pages/search.py:5248-5264); no scoped=1 marker exists (grep of web/pages/search.py and web/search_bootstrap.py), and an unmatched address falls through to execute_search at line 6547-6552. SearchRun.scoped (web/search_runs.py:49,68) only stops a page following a scoped run; it does not mark the address.

## GS-92

**Website: a search still preparing can be evicted from the run registry before finished ones** · P3 · Website · ❌ Open · opened 2026-10-22

Where: `web/search_runs.py` (`_prune`, `_oldest_first_removable`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Website: a search still preparing can be evicted from the run registry before finished ones**
- **File:** `web/search_runs.py` (`_prune`, `_oldest_first_removable`)
- **Status:** ❌ Open (2026-10-22)
- **Notes:** Eviction keeps a run whose job is `running` last, but a run still preparing (`job is None`: the phone fold or the filter lookup, before the job starts) counts as not running, so when the same browser starts more than `PER_BROWSER` (5) newer searches during those seconds -- several tabs at once -- the preparing run can be dropped before finished snapshots. Back to its stamped address then finds nothing and searches again while the original continues. Raised by Codex on PR #436 (round 16, after the owner capped the review at information only); not changed there. Fix sketch: rank by `attachable` (unfinished and followable) instead of `running` in `_oldest_first_removable`, and exempt attachable runs from the age prune as running ones are.

*Triage 2026-10-25:* still open. _oldest_first_removable still sorts on SearchRun.running, which is False while job is None (web/search_runs.py:57-59), so a preparing run sorts with finished ones; _prune's age rule also uses 'not run.running' (line 153). attachable (line 61) exists but is not used for ranking. Last commit to the file is 6d912fd9, before the row.

## GS-93

**Website: Back to a search shows its rows without the Filter Results panel's constraints** · P3 · Website · ❌ Open · opened 2026-10-22

Where: `web/pages/search.py` (Filter Results inputs); `web/pages/search_state.py::_post_filters`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Website: Back to a search shows its rows without the Filter Results panel's constraints**
- **File:** `web/pages/search.py` (Filter Results panel: `filter_shelfmark`, `filter_title`, `filter_snippet`, the measurement `ui.number`s), `web/pages/search_state.py` (`post_filter_*`, `_post_filters`)
- **Status:** ❌ Open (2026-10-22)
- **Notes:** The panel's shelfmark/title/text inputs and the `post_filter_*` measurement constraints live only on the page: nothing writes them to a run's snapshot (`web/search_runs.py`) or to user storage. Every restore of a search's rows -- Back in any mode, a language-change rebuild -- shows the saved raw rows with those controls empty. Rows and controls agree (no hidden filter), so the visitor sees the unfiltered list, not a wrong subset. Before PR #436 a Responsa Back re-ran the search and ended the same way. Raised by Codex on #436 (round 5) and left out of it. Fix sketch: add them to `_post_filters`, call `save_post_filters_with_run` where they change, restore them in `restore_post_filters`, and set the inputs from state before the first render.

*Triage 2026-10-25:* still open. _post_filters (web/pages/search_state.py:482-490) saves only printed, domain, exclusion sources, pgp, library and word-search exclusions; the shelfmark/title/snippet inputs and post_filter_* measurements are not in it nor in restore_post_filters (493-508).

## GS-94

**Supabase calls still on the event loop: /corrections save/approve/reject, add-all list create, comment picker, edit-mode read** · P3 · Website · ❌ Open · opened 2026-10-21

Where: `web/pages/corrections.py`; `search.py:3414`; `comment_dialog.py:102`; `browse.py:2007`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Supabase calls still on the event loop: /corrections save, approve and reject; `create_list_sync` in "add all"; the comment dialog's shelfmark picker; the edit-mode corrections read**
- **File:** `web/pages/corrections.py` (save, approve, reject), `web/pages/search.py::create_and_add_all`, `web/components/comment_dialog.py` (`load_recent`, `load_lists`), `web/user_lists.py::get_items_in_list`, `web/pages/browse.py::toggle_edit_mode` (`get_corrections`)
- **Status:** ❌ Open (2026-10-21)
- **Notes:** Left by the fix that moved the list, comment and /browse correction writes off the loop (archived 2026-10-21): each is a ~150 ms Oregon->Frankfurt round trip during which no visitor is served. Same pattern: `get_user_client()` on the loop, `run.io_bound(..., client=)`. `web/pages/search.py` is in the Responsa work's path: move `create_list_sync` with it or after.

*Triage 2026-10-25:* still open. Each is still a direct synchronous call in an event handler: update_correction (corrections.py:554, 822, 836), state.lists_mgr.create_list_sync (search.py:3414), get_items_in_list_sync('recent') in load_recent (comment_dialog.py:102), get_corrections (browse.py:2007). None of web/pages/corrections.py, comment_dialog.py was changed for this since 7a43493b/4a4ecc0b.

## GS-95

**Responsa downgrade warnings follow the server process's language, not the visitor's** · P3 · Website · ❌ Partly fixed · opened 2026-10-20

Where: `shared/search_engine.py::_tr`; `web/translations.py::tr_engine_message`; `web/pages/search.py:2848`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Responsa downgrade warnings follow the server process's language, not the visitor's**
- **File:** `shared/search_engine.py::_tr`, `shared/responsa.py::_tr` (`genizah_core.CURRENT_LANG`), `web/pages/search.py` (the `responsa_warning` notify)
- **Status:** ❌ Open (2026-10-20)
- **Notes:** Built in the research worker in the process's `lang.pkl` language ('en' when absent) and shown as is; the API repeats it. Fix: pin the workers to English, then split the joined labels on '; ' and `tr()` each on the web side.

*Triage 2026-10-25:* partly fixed. e808fcbf (2026-10-23) added tr_engine_message (web/translations.py:273), which splits the engine's English notes on '; ' and translates each into the visitor's language; the results-line suffix (web/pages/search.py:2848) and the refusal notify (5765) use it. The workers are still not pinned to English: shared/search_engine.py::_tr reads genizah_core.CURRENT_LANG (from lang.pkl, 'en' when absent) and the web app never writes lang.pkl. **Still open:** Workers are not explicitly pinned to English, so the page translation only works while the server has no Hebrew lang.pkl; the API still repeats the process-language text.

## GS-96

**A 404 under the running `/_nicegui/<version>/` prefix is sent `Cache-Control: public, max-age=31536000, immutable`** · P3 · Website · ❌ Open · opened 2026-10-20

Where: NiceGUI's `SetCacheControlMiddleware`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A 404 under the running `/_nicegui/<version>/` prefix is sent `Cache-Control: public, max-age=31536000, immutable`**
- **File:** NiceGUI's `SetCacheControlMiddleware`
- **Status:** ❌ Open (2026-10-20, seen on 3.8 and 3.18)
- **Notes:** A CDN may keep such a 404 (a missing library file) for a year. Pre-existing; separate from the session-less 404 fix.

*Triage 2026-10-25:* still open. The installed NiceGUI 3.18.0 SetCacheControlMiddleware sets the cache header for every response whose path starts with /_nicegui/<version>/ regardless of status (nicegui/middlewares.py), and no repo code overrides it (grep of web/*.py for cache_control_directives and 404 cache headers is empty).

## GS-97

**Hebrew pages shift after first paint (CLS p50 ~0.84 on / and /search); the server HTML always says `lang="he"`** · P3 · Website · ❌ Open · opened 2026-10-20

Where: `web/main.py::apply_theme_immediately`, `web/framework_patches.py::_patch_html_lang_attribute`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Hebrew pages shift after first paint (CLS p50 ~0.84 on / and /search); the server HTML always says `lang="he"`**
- **File:** `web/main.py::apply_theme_immediately`, `web/framework_patches.py::_patch_html_lang_attribute`
- **Status:** ❌ Open (2026-10-20, PostHog 30 d)
- **Notes:** English `/` p50 0.003. Probable cause: `dir` is set early, but Quasar's RTL layout and `body.hebrew-mode` only once Quasar has loaded. Render dir / lang / the class on the server, per visitor (3.18's template context carries the request).

*Triage 2026-10-25:* still open. The installed nicegui/templates/index.html still emits lang="he" for every visitor, and apply_theme_immediately sets dir, lang and the hebrew-mode body class only from an inline script (web/main.py:2219-2253). The CLS figure itself is PostHog data and was not re-measured.

## GS-98

**Web Fragment Puzzle: two tabs on one draft -- the last auto-save wins** · P3 · Website · ❌ Open · opened 2026-10-20

Where: `web/pages/puzzle.py::_auto_save_now`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Web Fragment Puzzle: two tabs on one draft -- the last auto-save wins**
- **File:** `web/pages/puzzle.py::_auto_save_now`
- **Status:** ❌ Open (2026-10-20)
- **Notes:** Since auto-save writes (2026-10-20), a draft open in two tabs (or two browsers of one account) is overwritten by whichever tab saves last, with no notice; each tab reopens the draft from its saved copy on a reload, so the other tab's edits then appear only after a reload. Found while enabling auto-save, not observed. Fix if it matters: keep the `updated_at` a tab loaded and refuse (with a notice) a save over a newer one.

*Triage 2026-10-25:* still open. _auto_save_now loads the draft, replaces fragments/title/notes, stamps updated_at=now and saves (puzzle.py:2517-2538); no updated_at comparison or refusal anywhere, and save_document has none either.

## GS-99

**Dead lookup: `state.meta_mgr._shelf_to_sys` in the web Puzzle's join loader** · P3 · Website · ❌ Open · opened 2026-10-18

Where: `web/pages/puzzle.py` (fragments fallback in the joins loader)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Dead lookup: `state.meta_mgr._shelf_to_sys` in the web Puzzle's join loader**
- **File:** `web/pages/puzzle.py` (fragments fallback in the joins loader)
- **Status:** ❌ Open (found 2026-10-18)
- **Notes:** `MetadataManager` has no `_shelf_to_sys` (it is a desktop `GenizahGUI` map), so the `hasattr` guard always skips and string-only fragments of a saved join are never resolved there. Replace with `resolve_system_by_shelfmark`.

*Triage 2026-10-25:* still open. web/pages/puzzle.py:3393 still reads state.meta_mgr._shelf_to_sys behind hasattr; searching shared/ finds no _shelf_to_sys at all (only genizah_app.py and desktop/dialogs_filter.py have it), so the guard always skips.

## GS-100

**Visual Similarity window: each look-alike keeps only its first domain** · P3 · Website · ❌ Open · opened 2026-10-18

Where: `web/components/visual_similarity_dialog.py::show_visual_similarity_dialog` (`_enrich`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Visual Similarity window: each look-alike keeps only its first domain**
- **File:** `web/components/visual_similarity_dialog.py::show_visual_similarity_dialog` (`_enrich`)
- **Status:** ❌ Open (2026-10-18, pre-existing)
- **Notes:** The Domain filter, its per-domain counts, the sort and the labels use `domains[0]`. A look-alike filed under two domains is shown, counted and filtered under its first only, so choosing the second drops it. Not checked against real data (`fjms_enrichment.db` is not in the repo): how many look-alikes have more than one domain, or a parent and a child row (the browse page skips a parent when its child is shown). At phone width the fixed 260px Original pane also leaves the list and the thumbnail grid little room. The English-names half was fixed 2026-10-19 (archived).

*Triage 2026-10-25:* still open. visual_similarity_dialog.py:289-291 still sets s['domain'] = domains[0]['domain'] and the filter at :465-466 matches only that one; :48 `_ORIGINAL_PANE = {False: ('260px','200px'), ...}` still fixed-width in the normal state. Last touches (01612063, 657f244e, 0c845b6e) did not change either.

## GS-101

**Website full-text highlighting compiles the pattern on the event loop under a 0.25 s budget a Maximum-size pattern cannot meet** · P3 · Website · ❌ Open · opened 2026-10-12

Where: `web/pages/search_results.py::highlight_full_text_html/_render_full_text`, `search_regex.py::compile`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Website: full-text highlighting compiles `f'({pattern})'` inside a 0.25 s budget on the event loop, which a Maximum-size pattern cannot meet on a cold cache**
- **File:** `web/pages/search_results.py` (`_render_full_text`, called directly and after the page fetch in `_lazy_load_text`), `shared/search_regex.py::compile`
- **Status:** ❌ Open (found 2026-10-12)
- **Notes:** `highlight_pattern` is wrapped in a new string and compiled on the event loop under a 0.25 s budget that `compile()` checks only before and after its uninterruptible native compile, so a Maximum pattern (about 4.5 s with `regex`, the plan's figure) holds the loop for seconds and then shows "Highlighting stopped because it exceeded the time limit". Fix: highlight in the worker, or refuse a pattern past a size before compiling it. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. search_results.py:74-76 compiles inside search_budget(seconds=0.25) and _render_full_text (1085) is called directly on the event loop (1101, and after the page fetch in _lazy_load_text at 1132); search_regex.py:515-527 compile() checks the deadline only before and after the native _regex.compile. The f'({pattern})' wrapper the row names is gone (the pattern is compiled as is, since d686fa39) but the defect stands.

## GS-102

**Website: Num Changes shows the saved mode's value after a link to another mode** · P3 · Website · ❌ Open · opened 2026-10-07

Where: `web/pages/search.py` (Num Changes initial value, `resolve_search_bootstrap`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Website: Num Changes shows the saved mode's value after a link to another mode**
- **File:** `web/pages/search.py` (Num Changes initial value, `resolve_search_bootstrap`)
- **Status:** ❌ Open (2026-10-07)
- **Notes:** The Num Changes control is filled from the saved mode before the page resolves the mode a link asks for (e.g. `/search?mode=variants` after a Maximum search): it shows Maximum's x while the search runs with Basic's, until the level is changed. Display only. Codex review round 5 on #386 (rated P2). Fix: fill it from the resolved mode.

*Triage 2026-10-25:* still open. saved_max_changes is still computed from raw_saved_mode (search.py:192-195), before resolve_search_bootstrap picks the mode a link asks for (224); the select is built with it (773/983) and nothing calls _show_level_changes() at build time.

## GS-103

**Website: a completed search-within step is saved without re-checking that its chain is still the page's** · P3 · Website · ❌ Open · opened 2026-10-07

Where: `web/pages/search.py::_complete_chain`, _enter_refine_mode

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Website: a completed search-within step is saved without re-checking that its chain is still the page's**
- **File:** `web/pages/search.py` (`_complete_chain`, its callers after "Back to previous step" and in "Search within")
- **Status:** ❌ Open (2026-10-07)
- **Notes:** `_complete_chain` runs a separate background search and then commits its result for the chain it started with. If the visitor clears or changes the chain meanwhile ("Clear all", a new search), the old chain's result can still be saved. The replay paths and "Back to previous step" now check `_still_the_chain` (2026-10-07); the "Search within" completion predates that and was left unchanged. Fix: the same ownership check before committing.

*Triage 2026-10-25:* still open. web/pages/search.py:2741-2802 (_complete_chain) never calls _still_the_chain (defined at :1755, used only at :1782, :1787, :1819, :2892-2908); _enter_refine_mode awaits it at :2644 and then writes search_state.refinement_restrict_sys_ids from the result (:2656-2662) with no ownership check.

## GS-104

**Web small gaps: 0-result 'Ready to search' and stale zero-result flag, 700px Sefaria dialog, sidebar saved closed, My Edits vote stub** · P3 · Website · ❌ Partly fixed · opened 2026-10-06

Where: `web/pages/search_results.py:358`; `search.py::_clear_refinement_chain`; `parallels.py:4161`; `main.py:2020`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Web small gaps from the 2026-09-25 sweep**
- **File:** `web/pages/search_results.py`, `web/pages/search.py`, `web/pages/parallels.py`, `web/main.py`, `web/pages/corrections.py`
- **Status:** ❌ Open (sweep #33, #34, L23, L3; rechecked 2026-10-06)
- **Notes:** (a) A search with 0 results shows "Ready to search.", and `_clear_refinement_chain`/`_exit_refine_mode` do not clear `_zero_result_refine`, so "0 results within current scope" can come back on a later 0-hit search. (b) The Sefaria "More Sources" dialog has `min-width: 700px` (`show_all_sources_dialog`), wider than a phone. (c) Opening the sidebar on a narrow screen saves it closed (`toggle_drawer` negates the stored value), so it stays hidden later at desktop width. (d) The thumbs up/down on My Edits only say "Voting feature coming soon".

*Triage 2026-10-25:* partly fixed. Fixed in part: (b) at <=480 px, web/static/common.css:1121-1131 forces .q-dialog .q-card min-width:0 (commit a5808ce4, 2026-10-22), so the 700px card no longer overflows a portrait phone. **Still open:** (a) a 0-hit search still shows 'Ready to search.' (search_results.py:358) and _clear_refinement_chain / _exit_refine_mode (search.py:2683-2836) still do not clear _zero_result_refine; (b) 700px min-width still applies at 481-700 px (parallels.py:4161); (c) toggle_drawer still saves not(stored value) (main.py:2020-2023); (d) corrections.py:450 still only notifies 'Voting feature coming soon'.

## GS-105

**Web accessibility: icon-only buttons without a name, and an Atlas without keyboard access** · P3 · Website · ❌ Open · opened 2026-10-06

Where: `web/` (icon buttons), `web/static/js/atlas_decode.js`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Web accessibility: icon-only buttons without a name, and an Atlas without keyboard access**
- **File:** `web/` (icon buttons), `web/static/js/atlas_decode.js`
- **Status:** ❌ Open (sweep #40, #41; rechecked 2026-10-06)
- **Notes:** About 158 icon-only `ui.button(icon=...)` calls have no accessible name (an AST count; the guard in `tests/test_audit_2026_06_23_a11y_rtl.py` covers `web/pages/search_results.py` only). The Atlas handles only Escape from the keyboard and has no `tabindex`/`role`: entering a cluster, opening a manuscript and using search results need a pointer (v9.0.0 audit H19).

*Triage 2026-10-25:* still open. An AST scan of web/**/*.py finds 243 icon-only ui.button(icon=...) statements, 152 with no aria-label (31 in browse.py, 23 in discoveries.py); the a11y guard still covers only search_results.py (tests/test_audit_2026_06_23_a11y_rtl.py:104). atlas_decode.js has a single keydown handler for Escape (1582) and no tabindex/role.

## GS-106

**Search workers' free-memory reading counts page cache as used under a cgroup memory limit** · P3 · Website · ❌ Open · opened 2026-10-05

Where: `shared/research_limits.py::_cgroup_available_memory`, `web/research_jobs.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Search workers' free-memory reading counts page cache as used under a cgroup memory limit**
- **File:** `shared/research_limits.py::_cgroup_available_memory` (`memory.max - memory.current`), `web/research_jobs.py` (admission, warm-worker start, the in-run reserve check)
- **Status:** ❌ Open (found 2026-10-05) · 🔑 *Server check:* does production run under a cgroup memory limit (MemoryMax)?
- **Notes:** `memory.current` includes reclaimable file cache. In the dev container: 1,318 MB "available" with 15.3 GB free (12.5 GB cache); during a 4-lane test run it read 0, so a search waited for memory and `test_research_jobs.py::test_executes_in_another_process_and_preserves_results` failed on the commit before this branch too. Production is unaffected only while the service has no `MemoryMax` (then `memory.max` reads `max` and is skipped) -- unverified. Fix: subtract `inactive_file` from `memory.stat` (the usual working-set figure).

*Triage 2026-10-25:* still open. research_limits.py:40-45 still computes limit minus memory.current (or usage_in_bytes); there is no inactive_file / memory.stat anywhere in the file. Whether production runs under MemoryMax is an owner/server check.

## GS-107

**Puzzle: Manchester fragments often show without background removal** · P3 · Website · ❌ Open · opened 2026-09-29

Where: `web/api.py:1368` (manchester_image), :1964 (_fetch_provider_image), :1982 (puzzle_ext_image)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Puzzle: Manchester fragments often show without background removal**
- **File:** `web/api.py` (`manchester_image`, `_fetch_provider_image`, `puzzle_ext_image`), `web/pages/puzzle.py::_loadImageWithFallbacks`
- **Status:** ❌ Open (2026-09-29)
- **Notes:** Manchester renders derivatives on demand and often takes over 30 s; `_fetch_provider_image` always asks for 2000 px with a 30 s timeout, so `/api/puzzle_ext_image` answers 500 and the page shows the raw image without background removal. Options (owner; a smaller fetch lowers quality): pass the canvas size through, a longer Manchester timeout, one retry. Also: the `except` branch caches an unprocessed image under the processed name, and cache files are never replaced. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. manchester_image still clamps width to 2000 and calls get_with_checked_redirects(..., timeout=30) (api.py:1410); _fetch_provider_image (1964-1979) passes no width/timeout; puzzle_ext_image's `except Exception: result_bytes = raw_bytes` (api.py:2036) is still cached via _write_new_file (create-only) under the processed name.

## GS-108

**Web `/lists`: unpaged list reads, and "Add page to list" leaves `page` empty** · P3 · Website · ❌ Open · opened 2026-09-27

Where: `web/supabase_client.py::get_list_items`, `web/pages/browse.py` (Add page to list)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Web `/lists`: unpaged list reads, and "Add page to list" leaves `page` empty**
- **File:** `web/supabase_client.py::get_list_items`, `web/pages/browse.py` (Add page to list)
- **Status:** ❌ Open (2026-09-27)
- **Notes:** A list over one response (1,000 rows by default) shows only its first rows. Add page to list puts "Page N" in the shelfmark, so a page without an FL id reaches the desktop as the whole manuscript; it could send `page`.

*Triage 2026-10-25:* still open. supabase_client.py:1291 select('*').eq('list_id').order(...).execute() with no range/paging; browse.py:1695-1716 passes shelfmark f"... - Page N" and fl_id only; show_add_to_list_dialog (web/components/add_to_list_dialog.py:130) has no page parameter and add_list_item (1299) never writes page.

## GS-109

**Copy may be refused on Safari/iOS: the clipboard write runs after a server round trip, outside the click's user activation** · P3 · Website · ⏳ Needs a check · opened 2026-09-25

Where: `web/clipboard.py::copy_text_to_clipboard` and its callers

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Copy may be refused on Safari/iOS: the clipboard write runs after a server round trip, outside the click's user activation**
- **File:** `web/clipboard.py::copy_text_to_clipboard` and its callers
- **Status:** ❌ Open (2026-09-25, not tested on Safari) · ✋ *By hand:* try Copy on Safari and on iOS.
- **Notes:** Since 2026-09-25 a refusal shows "Copy failed" instead of a false "copied". Making Safari copy needs the text embedded in the page and written from a client-side click handler (pattern: `web/components/anchor_viewer.py` js_handler).

*Triage 2026-10-25:* needs a check. Design unchanged: web/clipboard.py:57-83 awaits client.run_javascript (server round trip) before navigator.clipboard.writeText, and still reports 'Copy failed' on refusal; no client-side click handler was added. Whether Safari/iOS actually refuses is not decidable from the code. **Would settle it:** On Safari desktop and iOS Safari, press a Copy button (e.g. bulk Copy Selected Text, citation copy) on genizahsearch.com and see whether the text lands in the clipboard or 'Copy failed' shows.

## GS-110

**Bulk "Copy Selected Text" fetches each result's full text synchronously on the event loop** · P3 · Website · ❌ Open · opened 2026-09-25

Where: `web/pages/search.py::bulk_copy_text` (`state.searcher.get_full_text_by_id`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Bulk "Copy Selected Text" fetches each result's full text synchronously on the event loop**
- **File:** `web/pages/search.py::bulk_copy_text` (`state.searcher.get_full_text_by_id`)
- **Status:** ❌ Open (2026-09-25)
- **Notes:** Same class as the event-loop fixes of 2026-09-25; move the compile loop into `run.io_bound`.

*Triage 2026-10-25:* still open. web/pages/search.py:3478-3482 calls state.searcher.get_full_text_by_id(...) inside the loop of an `async def`, with no run.io_bound; only the final clipboard write is awaited.

## GS-111

**Non-NLI image/manifest fetches (CUDL, Manchester, JTS, external IIIF) have no circuit breaker and keep long timeouts** · P3 · Website · ❌ Open · opened 2026-05-25

Where: `web/api.py:1191/1259/1316`, `genizah_core.py:4626`

Moved 2026-10-25 from `2026-05-29 Audit Follow-up (fan-out codebase audit + Codex review) / Open (logged 2026-05-29, deferred)`, where it read:

- **Issue:** Non-NLI image/manifest fetches lack a circuit breaker — `cambridge_image`/`manchester_image`/`jts_image` use `timeout=30`, `fetch_external_iiif_data` uses single-float `timeout=5` (~10s) — reproducing the 2026-05-25 NLI threadpool-saturation class against CUDL/Figgy if those hosts degrade
- **File:** `web/api.py:1191/1259/1316`, `genizah_core.py:4626`
- **Severity:** P2 Med
- **Notes:** ❌ Open. Deliberate prior scoping (D-18/D-20). **Design decision before fixing:** the breaker is a single global key, so tripping it on a CUDL failure would also blank NLI for 60s (cross-host coupling); a per-host breaker needs the singleton refactored to keyed state; shortening the 30s timeout risks breaking legitimately-slow large IIIF tiles.

*Triage 2026-10-25:* still open. web/api.py:1341, 1410, 1467, 1586 still call get_with_checked_redirects(..., timeout=30) with no breaker check; shared/metadata_manager.py:242,1422 external IIIF timeout is still 5 s; shared/nli_circuit_breaker.py is still the single global NLI key (D-01) and web/api.py uses it only on NLI paths.

## GS-112

**Per-IP limits will key IPv6 visitors per address (/128) once the realip snippet is installed** · P3 · API and AI · ❌ Open · opened 2026-10-07

Where: `web/api_hardening.py::_resolve_rate_limit_key`, the puzzle limiter in `web/api.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Per-IP limits will key IPv6 visitors per address (/128) once the realip snippet is installed**
- **File:** `web/api_hardening.py::_resolve_rate_limit_key`, the puzzle limiter in `web/api.py`
- **Status:** ❌ Open (2026-10-07)
- **Notes:** A client holding a /64 could rotate addresses for a fresh allowance. Bucket IPv6 keys by /64 (with a test). The origin listens on IPv4 only, so this arises only through Cloudflare.

*Triage 2026-10-25:* still open. web/api_hardening.py:84-132: the helper returns the raw peer/XFF entry string; no ipaddress / IPv6 / /64 handling anywhere in web/api_hardening.py or web/api.py (grep for ipv6, /64, ip_network, ipaddress finds nothing). No commit since 2026-10-07 touched it for this.

## GS-113

**A span lying wholly inside a work's preamble is addressed as unit 0 with zero or negative overlap** · P3 · Discovery · ❌ Open · opened 2026-08-12

Where: `shared/discovery_locus.py::units_for_span`, `::_floor_index`

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

❌ **Open (2026-08-12, split out 2026-08-19 from the `daf_rif` label bullet, whose label half is now fixed) — a span lying entirely inside a work's preamble resolves to unit 0 with zero or negative overlap.** 121 of 824 works have a unit table that starts after offset 0 (largest preamble 34,212 letters), and a span lying entirely in that preamble resolves to unit 0 with zero or negative overlap. All 541 are single-unit rows, so the minimum-overlap rule cannot and must not touch them — advancing `lo` would cite a unit further away still. Its own decision: suppress the address, or address the preamble.

*Triage 2026-10-25:* still open. shared/discovery_locus.py:444-451 _floor_index returns 0 when the offset precedes every unit start, and units_for_span (390-441) has no preamble branch; grep 'preamble' finds nothing in the locus module or builder (no commit mentions it). The live artifact still has 85 of 628 works whose first unit starts after offset 0.

## GS-114

**Judeo-Arabic locus labels run to 100 characters; the only safe fix (work-uniform colon-trim) is not built** · P3 · Discovery · ❌ Open · opened 2026-08-10

Where: locus_unit.label_he (live artifact); `shared/discovery_locus.py`, `scripts/build_discovery_sidecar.py`

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

❌ **Open (2026-08-10) — the Judeo-Arabic label LENGTH problem is real and mostly unfixable, and the reason is measured.** Re-measured 2026-10-06: labels run to 100 characters (median 25, p95 about 50). Shedding an ancestor per section or a uniform depth cap makes some rendered ranges longer or misleading, and deleting EVERY ancestor still leaves 318 labels over 40, because the length is in the innermost element. The only safe rule is a work-uniform colon-trim (see the decision below). Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. Read-only query of the deployed artifact 528f6d36 (discovery_data/live): 54,134 locus_unit labels, max 100 chars, 2,391 over 40, 124 over 60 (matches the 2026-10-06 re-measure). grep for colon/label-length rules in shared/discovery_locus.py and the builder finds no trim or cap.

## GS-115

**Multi-span rows (7.0% of addressable rows) are baked with one window, so citations can be incomplete (card 76); WP4 still deferred** · P3 · Discovery · ❌ Open · opened 2026-08-10

Where: `scripts/build_discovery_sidecar.py::project_ref_span`; `shared/discovery_locus.py`

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

❌ **Open (2026-08-10) — card 76 is direct evidence that the one-alignment bake is a correctness bug, not a refinement.** Every multi-span row (17,803 of 254,612 addressable rows, 7.0%) is baked with ONE window, so citations can be incomplete; `render_locus` and `citation_runs` are ready (`f9ffde3d`) and only the bake (WP4, deferred) is missing. Card 76 (`תנ"ך תהלים`, `פרק קכט–קל`): baking the second alignment adds `פרק קלא–קלב`; the Psalm 126 the owner named is in no alignment on that page, a matcher-recall gap that no bake can supply. Full text archived 2026-10-06.

*Triage 2026-10-25:* still open. Same unchanged one-alignment selection as T267 (build_discovery_sidecar.py:4485); f9ffde3d (2026-08-10) only refactored parsing into shared/discovery_locus.py and the archived note says WP4 stays deferred. The locus piece table (build_discovery_sidecar.py:2475-2500) is built from per-evidence w_start/w_end, so a second alignment within one evidence row is still never baked.

## GS-116

**Judeo-Arabic supplement headings show visually-ordered (reversed) shelfmark sigla as addresses; owner decision pending** · P3 · Discovery · ❌ Open · opened 2026-08-10

Where: locus_unit.label_he for w000071 units 367-370 (live artifact)

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

❌ **Open (2026-08-10) — the one remaining locus complaint from the audit is a Judeo-Arabic supplement heading, and the mangling is the SOURCE's.** Checked 2026-10-06: in the deployed artifact (`w000071`, locus units 367-370). The siglum list in the innermost heading is stored in VISUAL order in the source file, so its Latin reads reversed inside RTL text; reconstructing the shelfmarks risks a plausible wrong one, which is worse than a visibly mangled one. Owner decision needed: should siglum-bearing supplement headings be published as addresses at all? Blanket-stripping is refused (49 works legitimately quote a shelfmark). Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. Deployed artifact 528f6d36 still carries label_he for w000071 units 367-370 with 'T- S Ar. Box 43' and 'Ms. heb. e100' inside the address; 35 labels in 2 works contain Latin runs. No siglum-handling rule exists in shared/discovery_locus.py or the builder, and no owner ruling is recorded.

## GS-117

**Public projection re-materializes identifications from public rows only, so pruning a tie competitor can promote rows into the main pool** · P3 · Discovery · ❌ Open · opened 2026-08-10

Where: `scripts/project_discovery_public.py::_materialize_public_identification` (723-766)

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

❌ **Open (2026-08-10, session 4; narrowed 2026-10-05) — the public projection promotes identifications whose only tie competitor it pruned.** Checked 2026-10-06: the deployed artifact was built under `public-projection-exclusions-v1`, and `scripts/project_discovery_public.py::_materialize_public_identification` is unchanged. Re-materializing from public rows only drops a pruned competitor's `overlapping_tie`, which can move a row into the main pool (280 when logged); the Yalkut exclusion prunes more the same way. No guard compares the public main pool with the private one. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. The function still runs builder.populate_discovery_identification over public rows only; the only public-vs-private guard is check_identification_key_subset (773-805), a key-set check that cannot see main_pool drift. No commit to project_discovery_public.py, shared/discovery_main_pool.py or verify_discovery_sidecar.py since 2026-10-05 touches this, and verify_discovery_sidecar.py has no public-vs-private main-pool comparison.

## GS-118

**The canonical-text mask file predates the corpus it covers, so 247+ Sefaria (REF2:sef_*) works have no masking** · P3 · Discovery · ❌ Open · opened 2026-08-09

Where: `same_work_spike/probe/data/ref_canon_masks_v2.json`; `scripts/discovery_v4_extend_masks.py`

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

❌ **Open (2026-08-09) — the canonical-text MASK FILE predates the reference corpus it covers by five days, so 247+ works have no masking at all.** Found by owner report: a 103-letter match was presented as a direct witness to `ארבעה טורים, אורח חיים`, and its matched text is **Isaiah 35:10** (`ופדויי יי ישובון ובאו ציון ברנה…`) — a verse both texts merely quote.

*Triage 2026-10-25:* still open. Gitignored data checked read-only: ref_canon_masks_v2.json (2026-07-10) and its successors v4 (555 keys) and v5 (564 keys) in discovery_builds contain zero 'sef_' keys while ref2_manifest.json lists the sef_ works (REF2:sef_tur_orach_chaim among them). discovery_v4_extend_masks.py masks only the references appended by V4, so the original 247 were never re-masked.

## GS-119

**725 'confirms' identifications rest on a per-work attestation about OTHER manuscripts, attached even when this one is not a witness** · P3 · Discovery · ❌ Open · opened 2026-08-09

Where: `scripts/discovery_novelty_probe.py:376-386`; `scripts/emit_work_witnesses.py:129-132`

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

❌ **Open (2026-08-09) — 725 `confirms` identifications rest on a statement about OTHER manuscripts.** `scripts/emit_work_witnesses.py` builds a per-work, count-only attestation and `scripts/discovery_novelty_probe.py` attaches it to a pair even when this manuscript is not among the witnesses, so the prompt reads a fact about other witnesses as evidence about this one. 725 identifications (1,709 review rows, 3.2% of `confirms`) have no other evidence. The error is conservative: it understates the not-in-catalogue count. Fix: drop or retag the attestation when `known_witness_confidence IS NULL`, then re-run novelty for those pairs. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. discovery_novelty_probe.py:383-386 still sets attestation = wit_entry['attestation'] whenever the work has witness data, regardless of whether hit (known_witness_confidence) is None; emit_work_witnesses.py still builds the count-only string. Neither file changed since 2026-08-08/18 (3d0c75bf, b5ac780d); no novelty re-run is recorded.

## GS-120

**Help methods page reads a non-existent draw_size column, so every graded band shows 'not yet measured' sample figures** · P3 · Discovery · ❌ Open · opened 2026-08-09

Where: `web/pages/help.py:508-510` (_render_one_band); band_precision DDL

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

❌ **Open (2026-08-09; narrowed 2026-10-05) — the Help page's per-band sample line reads a column that does not exist, so every graded band says "not yet measured".** Checked 2026-10-06 on the deployed artifact (`528f6d36`): `web/pages/help.py` reads `row.get('draw_size')`, which `band_precision` does not have, and the real-mode `scope='band'` rows have NULL numerator/denominator, so the 4 graded bands print "not yet measured" for all three figures, in both languages. No test reaches the graded branch (`tests/render_smoke/test_help_methods_render_smoke.py` covers only the ungraded path). Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. web/pages/help.py:508 still calls row.get('draw_size'); the band_precision DDL (scripts/build_discovery_sidecar.py:484-510) has no such column, and in the deployed artifact all 6 scope='band' rows have NULL numerator/denominator (only tier_a is measured_pass). tests/render_smoke/test_help_methods_render_smoke.py has only the ungraded-tier test.

## GS-121

**Two JA works (w001151, w000065) still carry Latin-script author strings in the public sidecar and split the author facet** · P3 · Discovery · ❌ Open · opened 2026-08-04

Where: works.author in the public artifact; `scripts/attach_author_authority.py` OWNER_AUTHOR_RULINGS

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

❌ **Open (2026-08-04; narrowed 2026-10-05) — two JA-corpus works still carry Latin-script author strings in the public sidecar, and they split the author facet.** **Checked 2026-10-06:** the deployed artifact has exactly these two Latin-script authors. w001151 ('Moses b. Maimon, Rambam'): the owner ruled on 2026-09-01 that the author is יהושע הנגיד (`scripts/attach_author_authority.py` `OWNER_AUTHOR_RULINGS`, `99767425`), but the ruling reaches only the private v5 review db; the public build copies `works.author` verbatim from the approved CSV. w000065 ('Judah ha-Kohen'): no ruling and no Hebrew form. The findings author facet still groups on raw `works.author`. Original entry archived 2026-10-05.

*Triage 2026-10-25:* still open. Query of the deployed artifact 528f6d36 returns exactly these two Latin authors ('Judah ha-Kohen', 'Moses b. Maimon, Rambam'). OWNER_AUTHOR_RULINGS (attach_author_authority.py:114) is still applied only to the private review db, scripts/project_discovery_public.py has no author handling, and none of those scripts changed since 2026-10-05.

## GS-122

**w000160 (Arugat HaBosem) carries claims for two different works under one title/author; domain ruling does not settle which** · P3 · Discovery · ❌ Open · opened 2026-08-03

Where: `discovery-v5-REVIEW.db` review_row (w000160); `scripts/curate_work_domains.py:1711`

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

❌ **Open (2026-08-03) — `ערוגת הבושם` (`w000160`): title/author collision, 14 shipped claims.** Two distinct works share this title — Archivolti's 16th-c. rhetoric (matching the RECORDED author) and Abraham b. Azriel's 13th-c. piyyut commentary. Phase 136 ruling Q assigned `Philology / Grammar` on the recorded author plus a weak catalogue signal (42%, n=12), but **the ruling settles the domain, NOT which work the claims are actually about.** A 16th-century Italian printed rhetoric is an odd fit for a Genizah corpus; the piyyut commentary is likelier. Needs a look at the underlying claims.

*Triage 2026-10-25:* still open. The v5 private review db still holds 16 review_row rows for w000160 (3 shipped), unchanged title/author (Philology / Grammar, msource, identity_visibility private); the work is absent from the deployed public artifact 528f6d36, so no public surface shows it. No ruling on which work the claims are about exists (curate_work_domains.py:1711 only assigns the domain).

## GS-123

**w000154, a 19th-c. maskilic memoir, sits in the discovery corpus; corpus membership is unresolved** · P3 · Discovery · ❌ Open · opened 2026-08-03

Where: `discovery-v5-REVIEW.db` review_row (w000154); `scripts/curate_work_domains.py:1431,1721`

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

❌ **Open (2026-08-03) — `זיכרונות מימי נעוריי` (`w000154`): a 19th-century maskilic memoir carrying 6 shipped claims in a Genizah discovery corpus.** Flagged by the 136-09 curation pass as possibly not belonging in the corpus at all. Ruling Q assigned `Historiography` and deliberately did NOT use `Unassigned`, because that value would hide the row and the row's real problem is corpus membership, not domain. Check whether the work — or its claims — should be in the reference corpus.

*Triage 2026-10-25:* still open. The v5 private review db still holds 5 review_row rows for w000154 (all review_only, 0 shipped), identity_visibility private, and the work is absent from the deployed public artifact 528f6d36. No ruling on corpus membership exists; curate_work_domains.py:1721 only assigns Historiography.

## GS-124

**Web: no way to ask for a new confirmation email** · P3 · Accounts and community · ❌ Open · opened 2026-10-21

Where: `web/auth_state.py` (`create_login_dialog`; the "not confirmed yet" sign-in error)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Web: no way to ask for a new confirmation email**
- **File:** `web/auth_state.py` (`create_login_dialog`; the "not confirmed yet" sign-in error)
- **Status:** ❌ Open (2026-10-21)
- **Notes:** PostHog, 180 days to 2026-10-21: 37 people were stopped by "Email not confirmed"; 13 later signed in with a password, 13 with Google (the two overlap), 13 never signed in. A visitor whose link is lost or expired has no resend. Fix: a "Send the link again" action next to that error, calling Supabase `auth.resend(type="signup")` with the same no-enumeration answer. Whether "Forgot password?" also confirms an unconfirmed address is unverified.

*Triage 2026-10-25:* still open. Grep for `resend` over web/, shared/, desktop/ *.py finds nothing; web/auth_state.py has no 'Send the link again' action (last commits d5d85975, a254e2e7 do not add one).

## GS-125

**Hardening follow-up: `profiles` columns no page needs are readable anonymously, and whether a Google account's own name is shown** · P3 · Accounts and community · ❌ Open · opened 2026-10-21

Where: `supabase_setup.sql` (profiles SELECT grants); every profiles select('*')

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Hardening follow-up: `profiles` columns no page needs are readable anonymously, and whether a Google account's own name is shown**
- **File:** `supabase_setup.sql` (profiles SELECT grants); every `profiles` `select('*')` (web, and the installed desktop)
- **Status:** ❌ Open (2026-10-21)
- **Notes:** `settings` and `last_login` are readable like the public name fields. Column-level grants need every `select('*')` on `profiles` gone first, including the installed desktop, or those reads fail. Since 2026-10-21 a Google sign-up gets an empty name (its claims are not published); showing Google's name instead is an owner decision. The username-fix record `private.profiles_username_fix_20261021` can be dropped once the undo is no longer wanted.

*Triage 2026-10-25:* still open. supabase_setup.sql:384 still `GRANT SELECT ON public.profiles ... TO anon` (table-wide) with the policy at :272 `USING (true)`; profiles select('*') remains in web/pages/admin.py:149, web/pages/corrections.py:64, web/supabase_client.py:997 and desktop/supabase_corrections_client.py:774, :2091, so column-level grants are not yet possible.

## GS-126

**Closing while an upload request is unanswered can bring a removed or moved entry back later** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `desktop/lists_sync_runner.py::shutdown`, `shared/lists_manager.py::abandon_upload`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Closing while an upload request is unanswered can bring a removed or moved entry back later**
- **File:** `desktop/lists_sync_runner.py::shutdown`, `shared/lists_manager.py::abandon_upload`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** Such a request is unknown here after the close. If its entry was removed or moved after it was sent, the website keeps that row and a later Download puts the entry back in its old list (nothing lost). A list or project it created is taken again by name (a second website list only if renamed meanwhile); a list whose colour, project or Trash request was unanswered keeps its unsent mark, so the next upload overwrites a website change to them.

*Triage 2026-10-25:* still open. No change since 09-27 (git log --since=2026-09-28 empty for lists_sync_runner/lists_manager/lists_sync). shutdown never blocks and leaves the daemon worker unread; abandon_upload installs only reports already received (`if not recorded: return False`).

## GS-127

**An entry removed at the moment the upload sends it is still inserted or moved** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_manager.py` (`withdrawn_now`, `finish_upload`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **An entry removed at the moment the upload sends it is still inserted or moved**
- **File:** `shared/lists_manager.py` (`withdrawn_now`, `finish_upload`)
- **Status:** ❌ Open (2026-09-27)
- **Notes:** Between the upload's last check and its request. Its delete is queued when the answer is read before the upload ends or the app closes; otherwise as the row above.

*Triage 2026-10-25:* still open. withdrawn_now (681-685) reads the journal of removals since begin_upload, so a removal made between the engine's last withdrawn() check and its request is not seen; code unchanged since 09-27.

## GS-128

**A killed process loses what the running upload established** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_manager.py` (`begin_upload` to `finish_upload`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A killed process loses what the running upload established**
- **File:** `shared/lists_manager.py` (`begin_upload` to `finish_upload`)
- **Status:** ❌ Open (2026-09-27)
- **Notes:** A crash, power cut or Task Manager kill (not a close) saves nothing of that upload. Entries removed or moved during it can come back in their old list at a later Download; other rows are found again by content. Lists and projects it created are taken again by name; a list whose state it sent keeps its unsent mark, so until the next upload a website change to that list's colour, project or Trash state is not imported, and that upload overwrites it.

*Triage 2026-10-25:* still open. The upload runs on a copy (begin_upload returns two pickled copies, 668-679); records reach the live store only in finish_upload/abandon_upload (_install_upload -> save at 750-751), so a kill mid-upload persists none of it; unchanged since 09-27.

## GS-129

**An account switch drops the other account's pending moves** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_sync.py::apply_account_guard`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **An account switch drops the other account's pending moves**
- **File:** `shared/lists_sync.py::apply_account_guard`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** That account's next Download puts a moved entry back in the list it left (a copy). Its pending removals are kept and sent when it logs in again.

*Triage 2026-10-25:* still open. apply_account_guard (511-535) pops cloud_rows/cloud_id of every item and cloud_id of lists/projects when cloud_account differs; only cloud_deletes (pending removals) are kept. Unchanged since 09-27.

## GS-130

**While lists.pkl cannot be saved, a removal can reach the website but not the file** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_manager.py::save`, `desktop/lists_sync_runner.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **While lists.pkl cannot be saved, a removal can reach the website but not the file**
- **File:** `shared/lists_manager.py::save`, `desktop/lists_sync_runner.py`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** If the program then closes before a save lands, the next start reads the old file: the entry is back, its website row gone, and the removal prompt offers it (nothing lost). Changes that did not upload are lost then, as the not-saved warning says.

*Triage 2026-10-25:* still open. save() (479-512) sets _saves_failing and returns False on a write error; per the row the removal is sent whatever the file save does, so the file can lag the website; I did not trace the runner path. Code unchanged since 09-27.

## GS-131

**One hung request holds the list-sync queue for up to 120 s** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `desktop/lists_sync_runner.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **One hung request holds the list-sync queue for up to 120 s**
- **File:** `desktop/lists_sync_runner.py`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** Cancel and the log-out budget stop waiting at once, but later syncs queue behind it until postgrest's default timeout.

*Triage 2026-10-25:* still open. cancel() only marks the job and 'completes when its worker returns' (209-216); _spawn starts a plain daemon thread per job with no request timeout of its own (344-356), so the next job still waits for postgrest's default.

## GS-132

**After Logout the account stays logged in for up to 10.5 s** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `genizah_app.py::_do_logout` (`LOGOUT_SYNC_BUDGET_S`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **After Logout the account stays logged in for up to 10.5 s**
- **File:** `genizah_app.py::_do_logout` (`LOGOUT_SYNC_BUDGET_S`)
- **Status:** ❌ Open (2026-09-27)
- **Notes:** While the last upload runs; the button reads "Logging out...".

*Triage 2026-10-25:* still open. genizah_app.py:1502 LOGOUT_SYNC_BUDGET_S = 10; _do_logout (2835-2885) sets a timer of budget*1000+500 ms and only _finish_logout (2896+) calls corrections_client.logout; button shows 'Logging out...'. Introduced e051d29e (2026-09-27), untouched since.

## GS-133

**A moved entry's old row that the website edited waits for a Download or Merge** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_sync.py::_settle_moved_row`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A moved entry's old row that the website edited waits for a Download or Merge**
- **File:** `shared/lists_sync.py::_settle_moved_row`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** When the entry's new list already has its own row, the old row is deleted only while this computer holds its text; otherwise it stays, counted as a note that differs, until a Download folds it in. A desktop that only uploads keeps it.

*Triage 2026-10-25:* still open. lists_sync.py:2026-2069: the redundant old row is deleted only when `held` (this computer holds its note and tags); otherwise result['notes_kept'] += 1 and rec['differs'] = True, leaving the row until a Download folds it in. File last changed 2026-09-27.

## GS-134

**A move into a list in the Trash waits until Restore** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_sync.py::_settle_moved_row` (`deferred`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A move into a list in the Trash waits until Restore**
- **File:** `shared/lists_sync.py::_settle_moved_row` (`deferred`)
- **Status:** ❌ Open (2026-09-27)
- **Notes:** Only Clean up duplicate lists does this. Meanwhile the website shows the entry in its old list; a Download never puts it back there.

*Triage 2026-10-25:* still open. lists_sync.py:2041-2047: `ready = (iid, to) in pass_.recorded`; a Trash destination is not written, so result['deferred'] += 1 and the moved record stays. The has_unsent_changes docstring (line ~303) still documents 'a move into a list in the Trash waits for Restore'.

## GS-135

**A list deleted for good here stays on the website, empty** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_sync.py` (list-level sync)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A list deleted for good here stays on the website, empty**
- **File:** `shared/lists_sync.py` (list-level sync)
- **Status:** ❌ Open (2026-09-27)
- **Notes:** Its entries' rows are deleted, but sync deletes no list: a Download re-creates it here by name, empty.

*Triage 2026-10-25:* still open. lists_manager.py:1164-1195 permanent delete only queues the entries' row deletes; ListsCloudSync.delete_list_from_cloud (lists_sync.py:2958) has no caller anywhere in shared/, desktop/ or genizah_app.py; _map_lists (2297+) creates a local list for every unowned cloud list ('Create new local list').

## GS-136

**A Download can put back an entry waiting for its delete one sync early** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_sync.py::_suppressed`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A Download can put back an entry waiting for its delete one sync early**
- **File:** `shared/lists_sync.py::_suppressed`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** It leaves such a row out only in the entry's own cloud list; if another actor moved it to a same-name list, the entry comes back one sync early (timing only).

*Triage 2026-10-25:* still open. lists_sync.py:1959-1964 builds {cloud list id: rows waiting}; _plan_list (2620-2625) skips a waiting row only when read in that same cloud list id (`waiting = pending.get(cid)`), so a row now in another same-name list is not suppressed.

## GS-137

**Putting an entry back while its removal is being sent still deletes the website copy** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_manager.py::_restore_cloud_row`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Putting an entry back while its removal is being sent still deletes the website copy**
- **File:** `shared/lists_manager.py::_restore_cloud_row`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** The delete already sent goes through, with any note added there; the entry stays in the list here and is uploaded again. Known to the owner.

*Triage 2026-10-25:* still open. lists_manager.py:132-178: _restore_cloud_row only undoes a removal still queued in store['cloud_deletes']; a DELETE already sent by the running upload is not recalled (begin_upload/finish_upload replay at 660-700 keep the delete). File unchanged since 2026-09-27; row says known to the owner.

## GS-138

**A list Move becomes a copy in two cases** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_sync.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A list Move becomes a copy in two cases**
- **File:** `shared/lists_sync.py`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** (a) A Move, or a removal from one of several lists, not uploaded before installing 2b-1 (the old per-entry `cloud_id` is not trusted): the first Download puts the entry back. (b) A Move made on another computer: this one's next upload re-inserts the entry in the old list. Nothing lost.

*Triage 2026-10-25:* still open. Per-membership records (cloud_rows) are the only thing trusted; lists_manager.py:100-110 documents that a legacy scalar cloud_id never queues anything, and no code migrates legacy ids into records. Nothing in lists_sync.py/lists_manager.py changed after 2026-09-27.

## GS-139

**Duplicate list names stay duplicated on the website** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_manager.py::merge_duplicate_group`, `shared/lists_sync.py::_map_lists`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Duplicate list names stay duplicated on the website**
- **File:** `shared/lists_manager.py` (Clean up duplicate lists), `shared/lists_sync.py` (one local owner per cloud list)
- **Status:** ❌ Open (2026-09-27)
- **Notes:** Sync deletes no list, so Clean up leaves the duplicate cloud list (moved entries leave it at the next upload). Two local lists of one name own a cloud list each; where an older upload made them share one, the second gets its own and its old rows also show in the first. Copies only.

*Triage 2026-10-25:* still open. merge_duplicate_group (lists_manager.py:1328-1383) deletes only the local duplicate and marks moves; delete_list_from_cloud has no caller, so the duplicate cloud list persists. _map_lists (2297+) gives each local list its own cloud list and folds extra same-name ones via same_name.

## GS-140

**Rows the desktop never records come back, marked, after their text is deleted here** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_sync.py::_reconcile`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Rows the desktop never records come back, marked, after their text is deleted here**
- **File:** `shared/lists_sync.py::_reconcile`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** A website's second row for one entry and rows of a second same-name cloud list are folded in on every Download with no recorded base, so text deleted on the desktop returns under the marker line.

*Triage 2026-10-25:* still open. _walk_list appends folds and same-name matches to `sources` with base None (lists_sync.py:2603-2607); _reconcile (2787-2840) only skips a source when `b is not None and same_text(c, b)` or it equals the local text, so locally deleted text comes back as a kept-both note.

## GS-141

**A cloud note or tag set too long for the URL limit cannot be changed from the desktop** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_sync.py::_patch`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A cloud note or tag set too long for the URL limit cannot be changed from the desktop**
- **File:** `shared/lists_sync.py::_patch`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** Writes filter on the full value they replace, in the URL; a 414/431 writes nothing, and each upload counts it (`notes_too_long`) and says so until the website shortens it. Limit not measured (about 1,300 Hebrew letters at 8 KB). Accepted: no hash columns.

*Triage 2026-10-25:* still open. lists_sync.py:1815-1861: the PATCH filters on the full note/tags it replaces in the URL; _is_url_too_long (550) maps 414/431 to 'too_long' and nothing is written (counted as notes_too_long). Row records this as accepted (no hash columns); unchanged.

## GS-142

**Page entries without a folio id uploaded before 2b-1 appear twice after the first sync** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_sync.py` (matching), `migrations/add_list_item_page_column.sql`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Page entries without a folio id uploaded before 2b-1 appear twice after the first sync**
- **File:** `shared/lists_sync.py` (matching), `migrations/add_list_item_page_column.sql`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** Their old rows name only the manuscript and pair with nothing: the first upload adds a page row, the first Download makes the old row a whole-manuscript entry with the same note (delete it on the website). Rows written while the column is missing never get their page.

*Triage 2026-10-25:* still open. Matching still pairs by identity incl. page (_row_identity/_same_entry, lists_sync.py:138-168) and nothing migrates old whole-manuscript rows; migrations/add_list_item_page_column.sql exists and docs/guides/SUPABASE_GUIDE.md:162 documents the page column, but old rows are not rewritten. Upgrade-time artefact.

## GS-143

**A repeated Merge can still change a row's note or an entry's record (two rare seeds)** · P3 · Accounts and community · ⏳ Needs a check · opened 2026-09-27

Where: `shared/lists_sync.py`, `tests/list_sync_scenarios.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A repeated Merge can still change a row's note or an entry's record (two rare seeds)**
- **File:** `shared/lists_sync.py`, `tests/list_sync_scenarios.py`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** Beyond the suite's gate (500 seeds x 60 steps): of seeds 10000-19999 x 200 steps, 11940 (a second identical Merge rewrites a row's note) and 13918 (an entry loses its record for one list) break invariant 5; so does the pre-change engine on both. Replay: `python tests/list_sync_scenarios.py --seeds 11940,13918 --steps 200`.

*Triage 2026-10-25:* needs a check. Replayed on the current tree: tests/list_sync_scenarios.py --seeds 11940,13918 --steps 200 -> failing=0; seeds 10000-15999 x 200 steps (6,000 seeds) show no invariant-5 failure, only two invariant-14 ones (seeds 11362 and 11917, not in the tracker). Engine and harness changed a lot after the row (shared/lists_sync.py last touched 2026-09-27 23:07, bbd6673a), so the old seeds may no longer mean the same scenario. **Would settle it:** The row's seeds no longer fail (replayed 2026-10-25: --seeds 11940,13918 --steps 200 gives failing=0), but tests/list_sync_scenarios.py grew by ~1,400 lines after the row was written, so the seed numbers may name different scenarios. Run python tests/list_sync_scenarios.py --seeds 16000-19999 --steps 200 --jobs 16 (about 5 min) to finish the 10000-19999 range; if invariant 5 stays clean the row can be closed as not reproducible.

## GS-144

**Lists renamed before 2b-1 lose or keep the wrong name** · P3 · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_manager.py::update_list`, `shared/lists_sync.py::_map_lists`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Lists renamed before 2b-1 lose or keep the wrong name**
- **File:** `shared/lists_manager.py::update_list`, `shared/lists_sync.py::_map_lists`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** (a) Only 2b-1's renames set `list_name_unsent`: an older rename never uploaded takes the website's name at the first Download. (b) Older renames left a stale `name_en` (shown in English for the default list and downloaded lists) until the next rename.

*Triage 2026-10-25:* still open. update_list (lists_manager.py:1003-1027) sets list_name_unsent and name_en only on a rename made by the current build; no code backfills the flag or name_en for older renames, and _map_lists take_name (2339-2344) takes the website's name for any list without the flag.

## GS-145

**Desktop discovery authors cannot edit or delete their own discovery** · P3 · Accounts and community · ❌ Open · opened 2026-09-25

Where: `desktop/supabase_corrections_client.py:1392` (_parse_discovery), :779; `desktop/corrections_ui.py:1606`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Desktop discovery authors cannot edit or delete their own discovery**
- **File:** `desktop/supabase_corrections_client.py::_parse_discovery`, `desktop/corrections_ui.py` (`is_author`)
- **Status:** ❌ Open (2026-09-25)
- **Notes:** `_parse_discovery` never sets `author_id`, so `is_author` is always False for non-admins; `User.id` is also `hash(uuid) % 10**9`, which is not stable across runs. Compare UUIDs.

*Triage 2026-10-25:* still open. _parse_discovery (1397-1416) still builds Discovery with no author_id (dataclass default None, line 176); User.id is still hash(user_id) % 10**9 (779, 793); corrections_ui.py:1606 still compares current_user.id == d.author_id. 624d173f (10-21) fixed Correction.author_id only, not Discovery.

## GS-146

**Web /corrections shows the raw English scope badge** · P3 · Accounts and community · ❌ Open · opened 2026-09-25

Where: `web/pages/corrections.py` (comment card badge)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Web /corrections shows the raw English scope badge**
- **File:** `web/pages/corrections.py` (comment card badge)
- **Status:** ❌ Open (2026-09-25)
- **Notes:** `comment_type.title()` shows "Page" / "Manuscript" in the Hebrew UI; the desktop now maps it by `page_number`.

*Triage 2026-10-25:* still open. Still `ui.badge(comment_type.title())` at corrections.py:676, with no tr() (same file also title()s the role at 250 and the status at 409).

## GS-147

**get_user_client() silently returns the anonymous client when a token refresh fails, so user-scoped reads can look empty** · P3 · Accounts and community · ❌ Open · opened 2026-09-25

Where: `web/supabase_client.py::get_user_client` (lines 466-475), empty_trash

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **`get_user_client()` silently returns the anonymous client when a token refresh fails terminally, so a user-scoped read can look empty (e.g. Empty Trash reports "Deleted 0 lists")**
- **File:** `web/supabase_client.py::get_user_client` (refresh branch: `return get_client()` after `_refresh_user_session` fails); `empty_trash`, the list/project readers
- **Status:** ❌ Open (2026-09-25, Codex round 7 on #364, verified by reading; logged under the review budget, not fixed)
- **Notes:** If the token expires between a page's sign-in check and the helper call and the refresh fails terminally, the helper runs anonymously: RLS turns a SELECT into `[]` (writes now report `no_rows`, so those at least fail visibly). Fix sketch: a `get_user_client(require_auth=True)` that raises instead of degrading, used by every user-scoped helper; callers map it to "Please log in". Narrow window, so P3. 2026-10-20 review: the fallback can only return less data, never more (no cross-user token mixing in 200 async / 400 thread checks).

*Triage 2026-10-25:* still open. web/supabase_client.py:466-475 still does `return get_client()` when _refresh_user_session fails or leaves no access_token; there is no require_auth parameter anywhere in the file. empty_trash (1249-1270) on an anonymous client reads [] and returns success with deleted_count 0.

## GS-148

**Star icons show as outline for everyone, signed-in users included** · P3 · Accounts and community · ❌ Open · opened 2026-09-25

Where: `web/user_lists.py:752-765`; `web/components/add_to_list_dialog.py::get_star_icon`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Star icons show as outline for everyone, signed-in users included**
- **File:** `web/user_lists.py::is_item_in_any_list` / `get_item_lists` (always False / []), `web/components/add_to_list_dialog.py::get_star_icon`
- **Status:** ❌ Open (2026-09-25, follow-up)
- **Notes:** Before the Lists containment the stars read the shared anonymous store, for signed-in users too, so they were wrong data. Restoring them needs one Supabase membership set per page for the signed-in user, not a per-row call (search renders up to 200 rows).

*Triage 2026-10-25:* still open. web/user_lists.py:752-765 still return False / [] unconditionally (docstring: per-page Supabase membership set is a follow-up), so get_star_icon (add_to_list_dialog.py:50-69) always yields 'star_border'.

## GS-149

**Write helpers that still report success when RLS changed 0 rows** · P3 · Accounts and community · ❌ Open · opened 2026-09-25

Where: `web/supabase_client.py::delete_discovery`; `web/discovery_suppression.py::unsuppress`; `discoveries.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Write helpers that still report success when RLS changed 0 rows**
- **File:** `web/supabase_client.py` (`delete_discovery`), `web/discovery_suppression.py::unsuppress`, `web/pages/discoveries.py` `do_hide_puzzle_join`
- **Status:** ❌ Open (2026-09-25)
- **Notes:** Same shape as the corrections-page fix (2026-09-25): check `response.data` and return `{'error': ..., 'no_rows': True}`. All use the user client already; only the row check is missing. `delete_list` (both variants), `delete_list_item`, `empty_trash` and `delete_project` got theirs on 2026-09-25 (Codex review of the hotfix PR).

*Triage 2026-10-25:* still open. web/supabase_client.py:1994-2001 returns {'success': True} after .delete().execute() without reading response.data; web/discovery_suppression.py:333-336 returns True the same way; web/pages/discoveries.py:1103-1106 notifies 'Puzzle join hidden' without checking the update result. None has the no_rows check.

## GS-150

**Desktop Supabase refresh token stored as plaintext JSON with default permissions** · P3 · Accounts and community · ❌ Open · opened —

Where: `supabase_corrections_client.py` (`~/.genizah_corrections/supabase_credentials.json`)

Moved 2026-10-25 from `5. Code Quality Debt / Repo structure Round 1 findings (2026-09-18)`, where it read:

- **Issue:** **Desktop Supabase refresh token stored as plaintext JSON with default permissions**
- **File:** `supabase_corrections_client.py` (`~/.genizah_corrections/supabase_credentials.json`)
- **Status:** ❌ Open
- **Notes:** The password goes to the OS keyring but the long-lived refresh token does not. Owner decision on whether to move the token to the keyring too.

*Triage 2026-10-25:* still open. desktop/supabase_corrections_client.py:393 puts session.refresh_token in the dict and :410-411 json.dump()s it to ~/.genizah_corrections/supabase_credentials.json with plain open(..., 'w'); no chmod/0o600 anywhere in the file; only the password goes to keyring (:442). File has moved from the root (path in row is stale).

## GS-151

**A failed NLI metadata fetch is cached as "Unknown" (and saved, on the desktop)** · P3 · Data and sidecars · ❌ Open · opened 2026-09-25

Where: `shared/metadata_manager.py::fetch_nli_data`, `batch_fetch_shelfmarks` (+ `save_caches`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A failed NLI metadata fetch is cached as "Unknown" (and saved, on the desktop)**
- **File:** `shared/metadata_manager.py::fetch_nli_data`, `batch_fetch_shelfmarks` (+ `save_caches`)
- **Status:** ❌ Open (sweep 2026-09-25 L20; rechecked 2026-10-06)
- **Notes:** Whatever the worker returns, including the placeholder used while NLI is down, goes into `nli_cache` with no expiry; the desktop writes it to `nli_cache.pkl`, so a manuscript missing from `libraries.csv` that was fetched during an outage shows "Unknown" across restarts until it is opened. Do not cache a failure, or cache it for 60 s.

*Triage 2026-10-25:* still open. fetch_nli_data stores whatever _fetch_single_worker returns, including its default {'shelfmark':'Unknown'} when the breaker is open (1478-1500), into nli_cache with no expiry; batch_fetch_shelfmarks does the same then save_caches() pickles it (1749-1753).

## GS-152

**PGP import can still produce a sidecar that is wrong but passes every check** · P3 · Data and sidecars · ❌ Open · opened 2026-09-22

Where: `scripts/import_pgp_full.py`, `import_pgp_sections.py`, `pgp_transcriptions_export.py`

Moved 2026-10-25 from `5. Code Quality Debt / Repo structure Round 1 findings (2026-09-18)`, where it read:

- **Issue:** **PGP import can still produce a sidecar that is wrong but passes every check**
- **File:** `scripts/import_pgp_full.py`, `scripts/import_pgp_sections.py`, `scripts/pgp_transcriptions_export.py`, `scripts/export_pgp_sidecar.py`
- **Status:** ❌ Open (2026-09-22)
- **Notes:** Deferred findings from the PR #357 review and the round-8 audit, all pre-existing. Validation proves agreement with the visible Supabase tables, never completeness against Princeton's inputs. (1) Upserts never delete (sources, footnotes and fragment links too). (2) The footnote key `(pgpid, source_slug, doc_relation)` collapses distinct records (PGPID 1346). (3) Shelfmark normalization is first-wins, so PGP data or an IIIF URL can attach to the wrong manuscript. (4) A 50-character floor in `pgp_transcriptions_export.py` drops 40 editions. (5) Translation language is guessed from Hebrew characters. (6) A partial section import passes as success. (7) The export is four unsynchronised paginated reads, not a snapshot. (8) Closed 2026-09-22: an empty or shrinking core table is fatal. (9) Provenance corroboration compares row counts only (suggested: sampled content hashes). (10) `validate_export()` is content-blind. (11) The superseded v1 importers have no provenance check (`--run-superseded`). (12) `has_transcription` can be fresh while the stored text is stale. (13) `update_doc_relation.py`'s not-found fatal has no override. (14) 79.7% of documents are never classified, and the web reads a NULL `doc_relation` as an edition. Full text archived 2026-10-06.

*Triage 2026-10-25:* still open. Only commit touching these since 2026-09-22 is 601c23e0 (stamps meta.source_revision, a different concern). Spot checks still show the defects: pgp_transcriptions_export.py:496 'len(content) < 50' floor (item 4); export_pgp_sidecar.py:708-723 validate_export compares row counts only (items 9/10). Items 1-3, 5-7, 11-14 not individually re-verified but nothing in git history addresses them.

## GS-153

**Four sibling translate scripts accept a blank API answer as a successful translation** · P3 · Data and sidecars · ❌ Open · opened 2026-09-21

Where: `scripts/translate_*.py` (four) translate_with_retry, lines `101/164/146/127`

Moved 2026-10-25 from `5. Code Quality Debt / Repo structure Round 1 findings (2026-09-18)`, where it read:

- **Issue:** **Four sibling translate scripts accept a blank API answer as a successful translation**
- **File:** `scripts/translate_catalog_freedesc_en2he.py:101`, `scripts/translate_fjms_catalog_text.py:164`, `scripts/translate_fjms_free_desc.py:146`, `scripts/translate_library_titles_en2he.py:127`
- **Status:** ❌ Open (2026-09-21)
- **Notes:** Each `translate_with_retry()` returns on `result is not None`, so Dicta's empty-string answer (seen on long Arabic-script stretches) is stored as a finished translation and looks done to every later pass. Fixed in `scripts/translate_pgp_descriptions.py` (PR #355); the same change applies to all four, after counting each corpus's blanks. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. All four translate_with_retry() still test only 'if result is not None'; scripts/translate_pgp_descriptions.py:224 has the fixed form 'result is not None and result.strip()' plus :449, so the fix was never propagated.

## GS-154

**Oxford licence attribution is incomplete: no Schaaf number, no photographer credit, no per-object link** · P3 · Data and sidecars · ❌ Open · opened 2026-09-02

Where: `web/services.py::ATTRIBUTION_BY_LIBRARY`; `shared/metadata_manager.py:36`; `oxford_full_db.json`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Oxford licence attribution is incomplete: no Schaaf number, no photographer credit, no per-object link**
- **File:** `web/services.py::ATTRIBUTION_BY_LIBRARY['Oxford']`, `shared/metadata_manager.py::OXFORD_IMAGE_CREDIT_EN`, `oxford_full_db.json`
- **Status:** ❌ Open (found 2026-09-02)
- **Notes:** The Genizah Fragments licence asks for "[title], [author/photographer], [Schaaf No], Image provided by [owner]" + a link to the site. Shipped: the "Image provided by the Bodleian Libraries, University of Oxford" form (EN/HE), the erroneous "CC BY-NC 4.0" label removed on every surface, footer links to the folio image on `hebrew.bodleian.ox.ac.uk`, credit switches to NLI when the NLI image is what is shown. Missing: Schaaf numbers and photographer names are not in our data; the per-object page URL is unknown (we link the folio image / volume page).

*Triage 2026-10-25:* still open. The credit is still only the fixed string 'Image provided by the Bodleian Libraries, University of Oxford' (shared/metadata_manager.py:36, web/services.py:37-39); 'Schaaf' appears nowhere in web/, shared/ or desktop/, so no per-object number or photographer is carried. Needs upstream data.

## GS-155

**`docs/SEARCH_API.md` still calls async jobs future work** · P3 · Documentation · ❌ Open · opened 2026-10-05

Where: `docs/SEARCH_API.md` (What This API Is NOT, Deferred Follow-Ups, Edge-Timeout Ceiling)

Moved 2026-10-25 from `2. Documentation Issues / Stale/Outdated Content`, where it read:

- **Issue:** **`docs/SEARCH_API.md` still calls async jobs future work**
- **File:** `docs/SEARCH_API.md` (the "Not a long-running job runner" limitation, the "Async job pattern" future-work item, the Edge-Timeout Ceiling section)
- **Status:** ❌ Open (2026-10-05)
- **Notes:** The background-job API shipped in #339 (`fff6c780`, 2026-09-14) and is documented near the top of the same file, but the limitations list still says a future async-job API "may ship in v7.11+", the future-work list still proposes `POST /api/search` → `202 + job_id`, and the Edge-Timeout Ceiling section does not point fuzzy and long chunk runs at the jobs endpoints.

*Triage 2026-10-25:* still open. docs/SEARCH_API.md:1334 still says 'a future async-job API may ship in v7.11+'; :1340 still lists 'Async job pattern ... 202 + job_id' as not implemented; the Edge-Timeout Ceiling section (:1280-1318) does not mention POST /api/search/jobs or /api/parallels/jobs, which the same file documents at :25.

## GS-156

**`NICEGUI_RELOAD` defaults to `true` in code but is documented as `false`** · P3 · Documentation · ❌ Open · opened 2026-09-25

Where: `web/main.py:3560-3561`; `docs/guides/DEPLOYMENT_TECHNICAL.md:1427`; `docs/guides/ENV_VARS.md`

Moved 2026-10-25 from `2. Documentation Issues / Stale/Outdated Content`, where it read:

- **Issue:** **`NICEGUI_RELOAD` defaults to `true` in code but is documented as `false`**
- **File:** `web/main.py` (`os.environ.get('NICEGUI_RELOAD', 'true')`), `docs/guides/DEPLOYMENT_TECHNICAL.md`, `docs/guides/ENV_VARS.md`
- **Status:** ❌ Open (sweep 2026-09-25 L11; rechecked 2026-10-06)
- **Notes:** Production is unaffected (`.env`, the systemd unit, `scripts/server.py` and CI set it to false). Correct the guide's default, and add `NICEGUI_RELOAD`/`NICEGUI_SHOW` to `ENV_VARS.md`.

*Triage 2026-10-25:* still open. web/main.py:3560 `os.environ.get('NICEGUI_RELOAD', 'true')` (and NICEGUI_SHOW 'true' at :3561); docs/guides/DEPLOYMENT_TECHNICAL.md:1427-1428 list the defaults as false; grep finds neither variable in docs/guides/ENV_VARS.md.

## GS-157

**Stale operational facts in the guides and CI comments** · P3 · Documentation · ❌ Open · opened —

Where: `docs/guides/DEPLOYMENT_TECHNICAL.md`, `docs/guides/ENV_VARS.md`, `.github/workflows/ci.yml`

Moved 2026-10-25 from `5. Code Quality Debt / Repo structure Round 1 findings (2026-09-18)`, where it read:

- **Issue:** **Stale operational facts in the guides and CI comments**
- **File:** `docs/guides/DEPLOYMENT_TECHNICAL.md`, `docs/guides/ENV_VARS.md`, `.github/workflows/ci.yml`, `scripts/ci_materialize_discovery_fixture.py`
- **Status:** ❌ Open
- **Notes:** `libraries.csv` "~15 MB / ~217K" (49.5 MB / 255,723), `fjms_enrichment.db` "941 MB" (1.6 GB), `Genizah_OLD.txt` (code: `AllGenizah_OLD.txt`), an `extension/store/` that does not exist; all three CI-side texts credit `tests/test_cert01_grading_validator.py` with resolving the real artifact through `discovery_data/manifest.json`, which `scripts/cert01_freeze.py` does.

*Triage 2026-10-25:* still open. DEPLOYMENT_TECHNICAL.md:126 '~217K', :469 'extension/store/' (no such tracked path; extension/ has no store dir), :1107 'Genizah_OLD.txt', :1108 '~15 MB', :1123 '~941 MB'; ci.yml:218, ENV_VARS.md:32 and ci_materialize_discovery_fixture.py:41 still credit tests/test_cert01_grading_validator.py (no manifest.json use; only scripts/cert01_freeze.py:54 reads it).

## GS-158

**`scripts/cleanup_duplicate_lists.py` would lose entries** · P3 · Server and operations · ❌ Open · opened 2026-09-27

Where: `scripts/cleanup_duplicate_lists.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **`scripts/cleanup_duplicate_lists.py` would lose entries**
- **File:** `scripts/cleanup_duplicate_lists.py`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** Admin script, dry run unless `--delete`. With it: entries matched by `sys_id` alone, inserts naming `user_id` and `notes` (not `list_items` columns, so they fail), then every row of the duplicate deleted anyway.

*Triage 2026-10-25:* still open. Script unchanged since 156742a9 (2026-02-01): inserts {'user_id','notes',...} (lines 124-131) but list_items has no such columns (docs/guides/SUPABASE_GUIDE.md:151-163), swallows the failure, then deletes all rows of the duplicate (line 137). Dry run unless --delete; hard-coded user id.

## GS-159

**Desktop field translation has never worked in a packaged build (owner: not worth fixing, 2026-09-20)** · deferred · Desktop app · ⏸ Deferred · opened 2026-09-20

Where: `desktop/gui_threads.py::few_shot_data_dir`, `GenizahSearchPro.spec` datas

Moved 2026-10-25 from `5. Code Quality Debt / Repo structure Round 1 findings (2026-09-18)`, where it read:

- **Issue:** **Desktop field translation has never worked in a packaged build (owner: not worth fixing, 2026-09-20)**
- **File:** `desktop/gui_threads.py::few_shot_data_dir`, `GenizahSearchPro.spec` (no `data` entry), `shared/dicta_client.py`, `data/few_shot_*.json`
- **Status:** ⏸ Deferred by decision
- **Notes:** Not a regression: the prompt files have never been in any installer, so in a packaged build the Translate link on the extended-info panels always shows "Translation failed" (only with show-translations on; never reported by a user). The web is unaffected. Owner decision 2026-09-20: leave it. If revisited: add `('data', 'data')` to the spec and anchor the path on `Config`. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. GenizahSearchPro.spec:29 datas list has no ('data','data') entry; desktop/gui_threads.py:1460-1471 still resolves <repo>/data and its docstring says the frozen build bundles no data/. Deferred by owner decision 2026-09-20.

## GS-160

**Results context menu detects LOCAL only from top-level res["source"], so a Lab LOCAL row with empty display.id gets the Genizah menu** · deferred · Desktop app · ⏸ Deferred · opened 2026-05-31

Where: `genizah_app.py:5645` (_show_results_context_menu), lines ~5681-5683

Moved 2026-10-25 from `Deferred to v7.15+ (Phase 95 follow-up backlog)`, where it read:

- **#:** D-F18
- **Item:** ⏸ Deferred (2026-05-31, v7.16.0 Codex review; narrowed 2026-10-05) Context-menu LOCAL detection should read `source` from `display`
- **Description:** The id half is done: since `c4ac41fc` (2026-09-25) `_show_results_context_menu` falls back to `display['id']`. The source half is not: both checks (`genizah_app.py:5365` and the `except` branch at `:5367`) read top-level `res.get('source')`, which no row the code builds carries, so a LOCAL row with an empty `display.id` (a Lab row whose `full_header` does not parse) gets the Genizah menu instead of the file menu. Read `(res.get('display') or {}).get('source') == 'LOCAL'` as well. Original entry archived 2026-10-05.
- **Priority:** P3

*Triage 2026-10-25:* still open. genizah_app.py ~5681-5683 still read only res.get('source') == 'LOCAL' (both the try and the except branch); only the id half was fixed (sys_id falls back to display['id'], commit c4ac41fc).

## GS-161

**PGP Hebrew translations are materially wrong in ~28% of rows** · deferred · Data and sidecars · ⏸ Deferred · opened 2026-09-21

Where: `pgp_data/pgp.db` (pgp_translations), `shared/dicta_client.py`, `shared/translation_qc.py`

Moved 2026-10-25 from `5. Code Quality Debt / Repo structure Round 1 findings (2026-09-18)`, where it read:

- **Issue:** **PGP Hebrew translations are materially wrong in ~28% of rows**
- **File:** `pgp_data/pgp.db` (`pgp_translations`), `shared/dicta_client.py`, `shared/translation_qc.py`, `web/pages/help.py`
- **Status:** ⏸ Deferred -- translations WITHHELD (2026-09-21, owner)
- **Notes:** A 180-row random audit graded 27.8% of the regenerated table wrong or garbage (95% CI 21.2-34.3%); the mechanical QC's "94.7% clean" is blind to meaning and must not be quoted. The endpoint serves DictaLM 2.0 few-shot, not Dicta Translate (`model_version` and the help page's credit are wrong too). **Owner ruled 2026-09-21: do not ship.** The table is withheld from the sidecar (`pgp_data/pgp_translations_withheld_2026-09-21.db`; `scripts/restore_pgp_translations.py` puts it back). Options: [docs/plans/PGP_TRANSLATION_QUALITY.md](plans/PGP_TRANSLATION_QUALITY.md). Full text archived 2026-10-06.

*Triage 2026-10-25:* still open. Local pgp_data/pgp.db has tables documents, document_sources, document_footnotes, document_fragments, meta and no pgp_translations (read-only check); pgp_data/pgp_translations_withheld_2026-09-21.db and scripts/restore_pgp_translations.py exist; docs/plans/PGP_TRANSLATION_QUALITY.md exists; no later commit reinstates or regenerates the table.

## GS-162

**28 unresolved `sys_id`** · deferred · Data and sidecars · ⏸ Deferred · opened —

Where: `fgp_data/fgp_transcriptions.db` (gitignored); `fgp_data/README.md:106-120`

Moved 2026-10-25 from `4.5 FGP Transcriptions — data ready, integration pending (2026-06-18)`, where it read:

- **Item:** **28 unresolved `sys_id`**
- **Status:** ⏸ Deferred
- **Notes:** Absent from corpus / FGP shelfmark variants — need human adjudication.

*Triage 2026-10-25:* still open. Read-only query of the local fgp_data/fgp_transcriptions.db: 45,034 rows, 28 with NULL/empty sys_id; fgp_meta says sysid_resolved 45006/45034 and sysid_unresolved 28. Deferred by design, awaiting owner adjudication.

## GS-163

**Pattern language: the mark-tolerant inserter and the word separator share characters, so a multi-word pattern reads two ways at a boundary** · debt · Search · ❌ Open · opened 2026-10-12

Where: `shared/search_engine.py` (`MARK_TOLERANT_INSERTER`, `_SEPARATOR_CLASS`, `_shared_chars_re`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Pattern language: the mark-tolerant inserter and the word separator share characters (`"`, U+2018-2019, U+0300-036F), so a multi-word pattern can be read two ways at a word boundary**
- **File:** `shared/search_engine.py` (`MARK_TOLERANT_INSERTER`, `_SEPARATOR_CLASS`, `_shared_chars_re`)
- **Status:** ❌ Open (found 2026-10-12; follow-up of the native matching plan §8; needs a scholarly ruling)
- **Notes:** The 115 characters in those ranges can be read as part of a form or as a separator, so a failing multi-word match costs up to about 2^(words-2) times more, and a generated pattern's subject cap is divided accordingly on the ~13% of pages holding one (a 9-word query falls to `regex` there). Fix: make the two classes disjoint after a scholarly ruling on which of them separate words, then a parity run on real pages (ids and spans). Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. search_engine.py:751 MARK_TOLERANT_INSERTER still lists U+0300-036F, the double and single quote and U+2018/2019, and :836 _SEPARATOR_CLASS is a negated word/Hebrew class that matches them too; _shared_chars_re (847, used at 2807) still exists to lower the native cap for pages holding them. Needs the scholarly ruling the row names.

## GS-164

**Windows: Extended/Maximum patterns match about 60 times slower with the `regex` module than with `re`** · debt · Search · ❌ Open · opened 2026-10-11

Where: `shared/search_regex.py::Pattern._call`, `shared/research_worker.py::protect_parent_death`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Windows: Extended/Maximum patterns match about 60 times slower with the `regex` module than with `re`**
- **File:** `shared/search_regex.py::Pattern._call`, `shared/research_worker.py::protect_parent_death`
- **Status:** ❌ Open (found 2026-10-11)
- **Notes:** Since v9.6.2 (PR #395, `docs/plans/DESKTOP_NATIVE_MATCHING_PLAN.md`) engine-generated gap-0 patterns match with `re` in process under a subject cap. Still on the slow `regex` route: a gap above 0, Basic's multi-form alternation, many-word queries on the ~13% of pages with a shared quote or mark between two letters, documents over a pattern's cap, the My Library composition loop (limit checked per chunk) and every Windows research worker (by design). One large document can hold a composition search about 30 s past its limit. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. search_regex.py:411-424 (_native_allowed returns False for sub, for non-native isolated workers and for subjects over the cap) and 443-456 (non-generated patterns always go through regex); no commit to search_regex.py, search_engine.py matching or research_worker.py on this route after 2026-10-12.

## GS-165

**Regex patterns that do not start at a word edge cost a pass over the whole term dictionary** · debt · Search · ❌ Open · opened 2026-10-06

Where: `shared/regex_prefilter.py` (a run not anchored at a token start becomes `.*X.*`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Regex patterns that do not start at a word edge cost a pass over the whole term dictionary**
- **File:** `shared/regex_prefilter.py` (a run not anchored at a token start becomes `.*X.*`)
- **Status:** ❌ Open (2026-10-06)
- **Notes:** 8-12 s per such pattern on 2.2M documents (measured 2026-10-06 under load; 3.4-5.0 s in the 2026-09-28 design run); a start-anchored run takes 0.01-0.04 s. A term vocabulary sidecar or fewer index segments would cut it.

*Triage 2026-10-25:* still open. A run not anchored at a token start is still emitted as '.*X.*' (regex_prefilter.py:132) and one such full pass per query is allowed (line 51); no term-vocabulary sidecar exists. Its only commits since 2026-10-06 are ae725c86 and e2a05338, neither changes this.

## GS-166

**Engines infer cancellation from `cancel_flag` instead of returning an outcome** · debt · Search · ❌ Open · opened 2026-08-19

Where: `shared/search_engine.py`, `shared/lab_engine.py`, `gui_threads.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Engines infer cancellation from `cancel_flag` instead of returning an outcome**
- **File:** `shared/search_engine.py`, `shared/lab_engine.py`, `gui_threads.py`
- **Status:** ❌ Open (found 2026-08-19)
- **Notes:** `execute_search` swallows `InterruptedError` and returns partial results normally, so the worker cannot tell a completed run from a cancelled one by the return value alone and has to consult `cancel_flag` at emit time. That leaves a narrow race: a worker can emit `perf_signal` microseconds before a late Stop click flips the flag, recording a run as completed while the UI reports it cancelled. Telemetry imprecision at a race boundary, not a user-visible defect. The durable fix is engines returning an explicit `complete \| cancelled \| failed` outcome alongside results, which touches every engine entry point.

*Triage 2026-10-25:* still open. shared/search_engine.py:4619-4622 catches InterruptedError, notes the cut-off and returns partial results as normal; desktop/gui_threads.py:360-366 still uses cancel_flag to decide whether to emit perf_signal because 'the core swallows InterruptedError'; no complete/cancelled/failed outcome type exists.

## GS-167

**Small per-query costs** · debt · Search · ❌ Open · opened —

Where: `shared/thread_local_db.py::_get_conn`, `shared/passage_search.py::_verify_and_merge`

Moved 2026-10-25 from `5. Code Quality Debt / 2026-09-25 sweep: hygiene and tooling (logged 2026-10-06)`, where it read:

- **Issue:** **Small per-query costs**
- **File:** `shared/thread_local_db.py::_get_conn`, `shared/passage_search.py::_verify_and_merge`
- **Status:** ❌ Open (sweep L19, X27; unmeasured)
- **Notes:** (a) Every query takes a lock and runs `threading.enumerate()` to prune dead threads' connections; throttle the prune. (b) Passage verification decodes a whole record once per candidate (`PassageIndex.stream`); estimated at 1-6% of search time. Do these only if profiling shows a cost.

*Triage 2026-10-25:* still open. thread_local_db.py:96-105 takes self._lock and calls _prune_dead() (threading.enumerate(), :74) on every _get_conn, new thread or not; passage_search.py:327 calls idx.stream(ri) per candidate. Row itself says do only if profiling shows a cost; no commit changes either.

## GS-168

**Desktop viewer: the MULTILINE test fires on every word separator and DOTALL is a no-op, so a Maximum pattern is recompiled on the UI thread** · debt · Desktop app · ❌ Open · opened 2026-10-12

Where: `desktop/widgets/__init__.py` (mark_pattern_hits), `desktop/result_dialog.py` (6 sites), `genizah_app.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Desktop viewer: the MULTILINE test fires on the `[^\` of every generated word separator, and `text_has_pattern_markers` adds a no-op DOTALL, so a Maximum pattern is compiled again, under two other flag sets, on the UI thread**
- **File:** `desktop/widgets/__init__.py` (`mark_pattern_hits`, `text_has_pattern_markers`), `desktop/result_dialog.py` (`load_page`, `load_local_page`, `load_result_by_index`, `_apply_source_highlights`, `_scroll_to_first_highlight`: the same test at six sites), `genizah_app.py::_comp_preview_source_text`
- **Status:** ❌ Open (found 2026-10-12; pre-existing)
- **Notes:** Six viewer sites add `re.MULTILINE` whenever the pattern holds `^\` (every generated word separator does), and `text_has_pattern_markers` adds a no-op DOTALL, so neither reuses the search's IGNORECASE compile: a 4-word Maximum pattern is compiled again on the Qt thread (the plan measured 1.7-2.0 s; not re-measured). Fix: test for `^` outside a character class (or drop the test) and compile with the search's flags. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. desktop/widgets/__init__.py:76-77 adds MULTILINE when the pattern holds a caret followed by a backslash (every generated separator does) and :98 compiles with IGNORECASE|DOTALL; the same test is at desktop/result_dialog.py:2604, 2664, 2971, 3332, 3343, 3519 and genizah_app.py:31483.

## GS-169

**Dead code, swallowed errors and redundant catches** · debt · Desktop app · ❌ Open · opened 2026-10-06

Where: `genizah_app.py`, `shared/local_indexer.py`, `desktop/gui_threads.py`, `web/pages/parallels.py`

Moved 2026-10-25 from `5. Code Quality Debt / 2026-09-25 sweep: hygiene and tooling (logged 2026-10-06)`, where it read:

- **Issue:** **Dead code, swallowed errors and redundant catches**
- **File:** `genizah_app.py`, `shared/local_indexer.py`, `desktop/gui_threads.py`, `desktop/corrections_client.py`, `web/pages/parallels.py`, `web/pages/search.py`, `web/supabase_client.py`
- **Status:** ❌ Open (sweep L9, L10, L15, L17, L18, L26; rechecked 2026-10-06)
- **Notes:** (a) the LOCAL-id filter in `start_metadata_loading` sits in `try/except Exception: pass`; (b) `LocalIndexer.close()` drops a failed final commit silently; (c) `_strip_search_prefix` is unused in practice; (d) the composition threads store `threshold` and never read it; (e) 17 `except (RuntimeError, Exception):`; (f) the desktop corrections fallback client points at the removed `/api/v1`; (g) the read-policy comments in `web/supabase_client.py` need checking against the live policy. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. (a) genizah_app.py:26410-26415 try/except Exception: pass around the LOCAL-id filter; (b) shared/local_indexer.py:4865-4868 final writer.commit() under 'except Exception: pass'; (c) genizah_app.py:28011 _strip_search_prefix; (d) desktop/gui_threads.py:522,650 store self.threshold, never read; (e) 16 in web/pages/parallels.py + 1 in search.py; (f) desktop/corrections_client.py:251-256 still /api/v1. (g) not checked (needs live policy).

## GS-170

**Desktop Visual Similarity download code is dead: its server route was removed** · debt · Desktop app · ❌ Open · opened 2026-09-25

Where: `desktop/vs_cache.py::VSDownloadThread`; `desktop/settings_dialogs.py::_start_vs_download`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Desktop Visual Similarity download code is dead: its server route was removed**
- **File:** `desktop/vs_cache.py::VSDownloadThread`, `desktop/settings_dialogs.py::_start_vs_download` and callbacks, re-export in `genizah_app.py`
- **Status:** ❌ Open (2026-09-25)
- **Notes:** The unused `/api/visual_similarity_db` route was removed from the web app on 2026-09-25 (nothing called it). The desktop bundles the DB in the installer. Remove the dead code in a desktop PR; a future on-demand download (SEED-005) needs a new, rate-limited delivery path.

*Triage 2026-10-25:* still open. desktop/vs_cache.py:128 (class VSDownloadThread, URL /api/visual_similarity_db at :142), desktop/settings_dialogs.py:62 and :1496-1510 (_start_vs_download, no caller left in the repo; a comment at :1489 says the route is gone) and the re-export at genizah_app.py:93 all still exist.

## GS-171

**Desktop export keeps parallel row builders in two long methods** · debt · Desktop app · ❌ Open · opened 2026-09-25

Where: `genizah_app.py::export_results`, `export_comp_report`

Moved 2026-10-25 from `5. Code Quality Debt / Duplication to Address`, where it read:

- **Issue:** **Desktop export keeps parallel row builders in two long methods**
- **Files:** `genizah_app.py::export_results`, `export_comp_report`
- **Status:** ❌ Open (sweep 2026-09-25 #44)
- **Notes:** Each branches inline per format (xlsx/csv/docx/txt) with no shared row model, so a column change must be made in several places; the xlsx Library/Shelfmark swap fixed in v9.3.0 was this kind of drift. A shared row builder and a cross-format column test would pin it.

*Triage 2026-10-25:* still open. export_results still branches inline per format (`if fmt == 'xlsx'` :28025, `elif fmt == 'csv'` :28213, `elif fmt == 'docx'` :28260); export_comp_report likewise (:28604, :28719). Only the LOCAL row has a shared helper (_build_export_data_row); the tests added since (tests/test_desktop_export_page_text.py) pin page text, not a cross-format column model.

## GS-172

**Dead branch passes kwargs `ColumnFilterDialog` does not accept** · debt · Desktop app · ❌ Open · opened 2026-09-19

Where: `genizah_app.py` `open_filter_dialog` (the `isinstance(col_idx, int)` branch)

Moved 2026-10-25 from `5. Code Quality Debt / Repo structure Round 1 findings (2026-09-18)`, where it read:

- **Issue:** **Dead branch passes kwargs `ColumnFilterDialog` does not accept**
- **File:** `genizah_app.py` `open_filter_dialog` (the `isinstance(col_idx, int)` branch)
- **Status:** ❌ Open
- **Notes:** `ColumnFilterDialog(self, col_index=..., current_filter=...)` vs the class's `(parent, column_label, current_text, exclude)`; the only caller is the Filter Text button (Qt passes a bool), so the branch never runs. Found during the Stage 2 trial move (2026-09-19): delete the branch or fix the call.

*Triage 2026-10-25:* still open. genizah_app.py:29475 still calls ColumnFilterDialog(self, col_index=..., current_filter=...) while desktop/column_filter_dialog.py:7 defines __init__(self, parent, column_label, current_text='', exclude=False); the only connection is genizah_app.py:19944 (triggered -> bool), so the int branch is unreachable.

## GS-173

**`GroupingThread` emits no terminal signal when cancelled** · debt · Desktop app · ❌ Open · opened 2026-08-19

Where: `gui_threads.py:353-354, 367-368`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **`GroupingThread` emits no terminal signal when cancelled**
- **File:** `gui_threads.py:353-354, 367-368`
- **Status:** ❌ Open (found 2026-08-19)
- **Notes:** `group_composition_results` returns `(None, None, None)` when its `check_cancel` fires (`shared/search_engine.py:3482`, `:3532`), and `GroupingThread.run` turns that into a bare `return` — in the **worker**, not the core — so `finished_signal.emit(...)` at `:372` is never reached on the cancel path. That is why `toggle_composition` relies on a blocking `wait()` there: there is nothing to settle the UI on. Any future non-blocking grouping cancel needs a real terminal emit first. It is also why grouping is out of scope for Pause, and why the Pause button is hidden rather than disabled during that phase.

*Triage 2026-10-25:* still open. desktop/gui_threads.py:1316-1317 and 1330-1331 still 'return # Cancelled' when group_composition_results returns None, before finished_signal.emit at 1335; the row's file name gui_threads.py is now desktop/gui_threads.py.

## GS-174

**Dead signal connection to a method that does not exist** · debt · Desktop app · ❌ Open · opened 2026-08-19

Where: `genizah_app.py:21852`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Dead signal connection to a method that does not exist**
- **File:** `genizah_app.py:21852`
- **Status:** ❌ Open (found 2026-08-19)
- **Notes:** The `else` branch connects `comp_thread.finished_signal` to `self.on_comp_search_finished`, and no such method is defined anywhere (verified: 0 definitions). Currently unreachable because both composition threads expose `scan_finished_signal`, so the `if` branch always wins — an `AttributeError` waiting on a future thread class that does not.

*Triage 2026-10-25:* still open. genizah_app.py:30687-30690 still has `if hasattr(self.comp_thread,'scan_finished_signal') ... else: ...finished_signal.connect(_comp_live(self.on_comp_search_finished))`; `git grep 'def on_comp_search_finished'` returns nothing. Unreachable while threads expose scan_finished_signal.

## GS-175

**Phase 111 telemetry — Codex review F3: `identify()` passes `user_id` to PostHog unvalidated** · debt · Desktop app · ❌ Open · opened 2026-06-14

Where: `desktop/telemetry.py::identify` (~line 582)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Phase 111 telemetry — Codex review F3: `identify()` passes `user_id` to PostHog unvalidated**
- **File:** `desktop/telemetry.py::identify` (~line 582)
- **Status:** ⏳ Deferred (logged 2026-06-14)
- **Notes:** Codex external review of Phase 111. `identify()` sends `user_id` straight to PostHog as `distinct_id` and persists it, scrubbing only the properties. By design the sole caller (desktop login) passes the Supabase `user.id` — an opaque UUID — so practical leak risk is low; deferred as hardening. **Fix when wired (Phase 114):** validate `user_id` against an opaque-ID shape, or hash/derive a pseudonymous distinct_id before enqueue + persist. Documented as accepted risk AR-111-02 in `111-SECURITY.md`.

*Triage 2026-10-25:* still open. identify() still calls enqueue_event(..., distinct_id=user_id) and save_app_config({IDENTIFIED_USER_KEY: user_id}) with no shape check or hashing; the only caller (genizah_app.py:2047) passes user._uuid. Accepted risk AR-111-02.

## GS-176

**Phase 111 telemetry — Codex review F6: default distinct_id substitution conflates NLI-breaker operational events with user identity** · debt · Desktop app · ❌ Open · opened 2026-06-14

Where: `shared/posthog_server.py:184-187` (`enqueue_event` `distinct_id=='system'` substitution)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Phase 111 telemetry — Codex review F6: default distinct_id substitution conflates NLI-breaker operational events with user identity**
- **File:** `shared/posthog_server.py:184-187` (`enqueue_event` `distinct_id=='system'` substitution)
- **Status:** ⏳ Deferred (logged 2026-06-14)
- **Notes:** Codex external review of Phase 111. The transport rewrites any `distinct_id=='system'` event to the desktop default install_id, so ungated NLI circuit-breaker operational events become user-linked. Applies only post-opt-in (consent granted), and with F1 fixed those events fire only under consent — minor data-modeling issue. **Fix:** don't substitute the default in the generic transport; have `desktop.telemetry` pass explicit distinct_ids only for sanctioned desktop events. Documented as accepted risk AR-111-03 in `111-SECURITY.md`.

*Triage 2026-10-25:* still open. enqueue_event still substitutes _default_distinct_id whenever distinct_id == 'system' (shared/posthog_server.py:202-206); not changed since the Phase 111 review. Accepted risk AR-111-03.

## GS-177

**View All page-block matching falls back to substring matching in _mark_blocks_for_pages (fragile; deferred refactor)** · debt · Desktop app · ⏸ Deferred · opened 2026-05-24

Where: `desktop/widgets/line_number_text_edit.py:283-323` (_mark_blocks_for_pages)

Moved 2026-10-25 from `Deferred to v7.15+ (Phase 95 follow-up backlog)`, where it read:

- **#:** D-F8
- **Item:** ⏸ Deferred (2026-05-24) View All page-block matching is fragile
- **Description:** Codex P3: `_mark_blocks_for_pages()` uses SUBSTRING matching — a page containing the separator text can be tagged as a separator block. Proper fix: tag blocks by index/sentinel (e.g. `userState` directly during HTML emission). Works correctly in practice today. Refactor candidate for v7.15+. See `genizah_app.py:_mark_blocks_for_pages`.
- **Priority:** P3

*Triage 2026-10-25:* still open. desktop/widgets/line_number_text_edit.py:317-321 still does 'p in block_text or block_text in p' as step 3 of the match; function moved out of genizah_app.py (row's file reference is stale).

## GS-178

**View All helpers _aggregate_local_pages_with_separators / _get_local_full_text_for_sys_id are vestigial; renderer hand-builds <p> blocks** · debt · Desktop app · ⏸ Deferred · opened 2026-05-24

Where: `genizah_app.py:280`, :27201

Moved 2026-10-25 from `Deferred to v7.15+ (Phase 95 follow-up backlog)`, where it read:

- **#:** D-F10
- **Item:** ⏸ Deferred (2026-05-24) View All renderer path consolidation
- **Description:** Codex optimality #3: `_aggregate_local_pages_with_separators()` / `_get_local_full_text_for_sys_id()` are mostly vestigial while the actual renderer hand-builds `<p>` blocks. Consolidate into one canonical path. Defer to v7.15+.
- **Priority:** P3

*Triage 2026-10-25:* still open. git grep: both names appear only at their definitions, in docstrings (genizah_app.py:27231-27232) and in tests/test_local_browse_panel.py; _open_local_browse (genizah_app.py:27225+) calls _get_local_pages_for_sys_id and builds blocks itself, never the full-text helper.

## GS-179

**`images_cache/` grows without bound** · debt · Desktop app · ❌ Open · opened —

Where: `desktop/image_loader.py`

Moved 2026-10-25 from `5. Code Quality Debt / Repo structure Round 1 findings (2026-09-18)`, where it read:

- **Issue:** **`images_cache/` grows without bound**
- **File:** `desktop/image_loader.py`
- **Status:** ❌ Open
- **Notes:** No eviction, no size cap, no clear action; the `_v2` filename bump orphaned the older 600px files instead of deleting them.

*Triage 2026-10-25:* still open. desktop/image_loader.py:68,73,128 only writes FL{id}_v2.jpg / ext_{hash}.jpg; git grep of images_cache/IMAGE_CACHE_DIR in desktop/, shared/, genizah_app.py finds no pruning, cap or clear action (docs/architecture/DATA_LIFECYCLE.md:162 says the same).

## GS-180

**The NiceGUI 3.8 rollback code is dead on 3.18, and 63 fire-and-forget tasks remain** · debt · Website · ❌ Open · opened 2026-10-20

Where: `web/framework_patches.py` (_patch_nicegui_esm_handler, _patch_binding_remove); `web/` tasks

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **The NiceGUI 3.8 rollback code is dead on 3.18, and 63 fire-and-forget tasks remain**
- **File:** `web/framework_patches.py` (`_patch_nicegui_esm_handler`, `_patch_binding_remove` stand down on 3.18; their 24 tests skip there), `web/` (63 statement-level `asyncio.ensure_future` / `create_task`)
- **Status:** ❌ Open (2026-10-20)
- **Notes:** Owner: keep the rollback code (it rots untested unless CI runs 3.8) or delete it. Tasks: 3.18's `background_tasks.create(context=)` re-enters the page and keeps a strong reference; add a count ratchet and migrate sites when touched (16 tracker rows of the lost-page class so far).

*Triage 2026-10-25:* still open. Both patches are still registered (framework_patches.py:636-638) with NiceGUI pinned at 3.18.0 (requirements.txt:10); a grep for statement-level ensure_future/create_task calls in web/ gives 64 now (63 in the row), so no ratchet holds the count. The keep-or-delete question is the owner's.

## GS-181

**The web app listens on `0.0.0.0:8081`** · debt · Website · ❌ Open · opened 2026-10-07

Where: `web/main.py` (`ui.run` without `host`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **The web app listens on `0.0.0.0:8081`**
- **File:** `web/main.py` (`ui.run` without `host`)
- **Status:** ❌ Open (2026-10-07; hardening)
- **Notes:** NiceGUI defaults to all interfaces, so a request to port 8081 would skip nginx and Cloudflare. Checked 2026-10-07: port 8081 is not reachable from outside (the security group drops it). Binding to `127.0.0.1` (nginx proxies there) would make that hold whatever the security group says; check local-dev use first.

*Triage 2026-10-25:* still open. web/main.py:3578-3586: ui.run(title, port, reload, show, storage_secret, favicon, reconnect_timeout) with no host argument; `git log -S'host='` on web/main.py finds nothing.

## GS-182

**Web puzzle PNG export bytes stay in the client's message history** · debt · Website · ❌ Open · opened 2026-09-29

Where: `web/pages/puzzle.py::export_png`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Web puzzle PNG export bytes stay in the client's message history**
- **File:** `web/pages/puzzle.py::export_png`
- **Status:** ❌ Open (2026-09-29)
- **Notes:** `ui.download(bytes)` keeps a ~9 MB 3000-px export in NiceGUI's per-client history until the client is pruned; repeated exports add up. Alternative: a private temp file served with `Cache-Control: private, no-store`.

*Triage 2026-10-25:* still open. export_png still ends in `ui.download(png_bytes, filename, 'image/png')` (puzzle.py:3056) with the comment 'nothing is written to disk or given a URL'; no temp-file or no-store route exists.

## GS-183

**Two helpers compute the signed-out browser key** · debt · Website · ❌ Open · opened 2026-09-29

Where: `web/saved_joins.py`, `web/puzzle_image_access.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Two helpers compute the signed-out browser key**
- **File:** `web/saved_joins.py`, `web/puzzle_image_access.py`
- **Status:** ❌ Open (2026-09-29)
- **Notes:** One uses user storage's `_session_uuid`, the other the NiceGUI browser id; both follow the same cookie, so they agree in practice. Merge into one helper.

*Triage 2026-10-25:* still open. saved_joins.owner_key/browser_drafts_to_move build 'b:'+get_persisted_session_uuid() (safe_storage); puzzle_image_access builds 'b:'+app.storage.browser['id'] / request.session['id']. Both still exist and are not merged.

## GS-184

**Web startup still loads the retired anonymous `ListsManager`, only as a readiness signal** · debt · Website · ❌ Open · opened 2026-09-25

Where: `web/main.py` (startup load into `state.lists_mgr`); about 15 `if not state.lists_mgr` sites

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Web startup still loads the retired anonymous `ListsManager`, only as a readiness signal**
- **File:** `web/main.py` (startup load into `state.lists_mgr`); about 15 `if not state.lists_mgr` sites
- **Status:** ❌ Open (2026-09-25)
- **Notes:** Since #364 nothing on the web reads or writes lists through it (`web/user_lists.py` is Supabase-only), and the server's `lists.pkl` was deleted after the deploy, so it now loads an in-memory default skeleton. Replace it with a plain readiness flag; if anonymous lists are wanted again, a per-browser store keyed through `web/safe_storage.py`.

*Triage 2026-10-25:* still open. web/main.py:3457 still constructs ListsManager(state.meta_mgr) in _init_sync; web/state.py:30-67 still wraps it in a lists_mgr property, and 24 `if (not) state.lists_mgr` sites remain under web/. No commit since 2026-09-25 touches ListsManager in web/main.py or web/state.py.

## GS-185

**Browse map cold load costs 18 s per fresh web process (133 MB pickle + `dedupe_browse_map` repair scan over 232,450 sys_ids)** · debt · Website · ❌ Open · opened 2026-09-02

Where: `shared/search_engine.py::_load_browse_map`, `shared/browse_service.py::_fetch_core`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Browse map cold load costs 18 s per fresh web process (133 MB pickle + `dedupe_browse_map` repair scan over 232,450 sys_ids)**
- **File:** `shared/search_engine.py::_load_browse_map`, `shared/browse_service.py::_fetch_core`
- **Status:** ❌ Open (found 2026-09-02)
- **Notes:** Symptom (bursts of `/api/browse` 504 `core_timeout` after every restart) is CLOSED by the warm-up budget (`SEARCH_API_BROWSE_CORE_WARMUP_TIMEOUT`, default 45 s, applied only while `GenizahService.is_warm()` is False; `tests/test_browse_core_warmup.py`). The load itself is the debt: persist the repaired map (skip the repair scan when a stamp says the pickle is clean) or warm it before the server accepts requests.

*Triage 2026-10-25:* still open. _load_browse_map still unpickles the map and runs dedupe_browse_map over every sys_id on each cold load (search_engine.py:2273-2276); it rewrites the pickle only when changed, with no 'already clean' stamp that skips the scan. The symptom half is closed by DEFAULT_BROWSE_CORE_WARMUP_TIMEOUT = 45 s (shared/browse_service.py:95, :293).

## GS-186

**CSRF protection missing** · debt · Website · ❌ Open · opened —

Where: API endpoints

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **CSRF protection missing**
- **File:** API endpoints
- **Status:** ❌ Deferred
- **Notes:** Low risk - NiceGUI uses WebSocket

*Triage 2026-10-25:* still open. grep for csrf/origin checks over web/ and shared/ finds none (only a samesite='lax' cookie at web/api.py:314). The state-changing POST routes are anonymous, rate-limited compute endpoints (puzzle_process needs a signed upload token), so the row's low-risk judgement still holds; the decision stays deferred.

## GS-187

**The bake stores ONE work-side span per claim, so scattered multi-range citations cannot be produced (WP4 deferred)** · debt · Discovery · ❌ Open · opened 2026-08-10

Where: `scripts/build_discovery_sidecar.py::project_ref_span` (4433-4487), `shared/discovery_locus.py`

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

❌ **Open (2026-08-10) — v3 stores ONE work-side span per claim, so the owner's "remember the exact pieces" ruling cannot be met from this bake.** 0 of 254,612 private and 0 of 221,771 public `(page_id, work_id)` pairs carry a second distinct span, against 22.1% of multi-alignment pairs in gen-2's `g_launch3.db`. So the reference-locus WP4 (`project_ref_span` -> `parse_ref_span_alignments()`, consumed by the locus build) is a precondition for scattered multi-range citations, and the scholar-audit deck must draw its scattered stratum from gen-2. Recorded in `docs/specs/discovery-v3-bake-plan.md` §3.2. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. build_discovery_sidecar.py:4485 still returns only select_primary_alignment(parse_ref_span_alignments(...)); the only callers are 4654 and 4744, and parse_ref_span_alignments has no other caller under scripts/. In the deployed artifact only 149 of 224,255 (sys_id, page, work) pairs carry a second distinct work span (0.07%, vs 22% in gen-2).

## GS-188

**_band_measurements keys its cache on self._last_path before _get_conn() refreshes it, so a stale path can persist** · debt · Discovery · ❌ Open · opened 2026-08-04

Where: `shared/discovery_service.py::_band_measurements` (2817-2855), `::_get_conn` (2368)

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

❌ **Open (2026-08-04) — `_band_measurements` reads the artifact path before it is assigned; staleness is unbounded, not one call.** `shared/discovery_service.py::_band_measurements` keys its cache on `self._last_path`, which only `_get_conn()` assigns, and a cache hit returns before `_get_conn()` runs, so a stale path can persist indefinitely (Codex). No production impact today: the artifact swaps only with a restart. A Phase 136 mutation control catches a regression; the fix belongs to a maintenance plan. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. shared/discovery_service.py:2834 still builds key = (self._last_path, version) and returns on a cache hit at 2835-2837 before _get_conn() (2840) can assign _last_path; the sibling launch-stats code (4068-4077) explicitly documents this ordering as wrong and resolves the connection first. Latent only: the artifact path changes on restart.

## GS-189

**_browse_cached_call's cache key carries no artifact path, so any enveloped browse read could serve a previous artifact's rows** · debt · Discovery · ❌ Open · opened 2026-08-04

Where: `shared/discovery_service.py::_browse_cached_call` (4867-4896)

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

❌ **Open (2026-08-04) — `_browse_cached_call`'s cache key is path-blind for every enveloped read.** The same class as the entry above, but general: the shared caching wrapper keys on call arguments without including the resolved artifact path, so *any* enveloped read can return a previous artifact's rows after a swap. Same near-zero production exposure and the same reason for deferral (file ownership during Phase 136 waves 7–8). Fix both together: derive the key from the path `_get_conn()` actually opened, and assign that path before any cache lookup rather than after.

*Triage 2026-10-25:* still open. shared/discovery_service.py:4886 builds key = (cache_name,) + tuple(args) + (version,) with no path, and web/discovery.py:554 still notes the outer LRU 'carries no path at all'. No commit since 2026-08-04 changes this; exposure is near zero because the artifact swaps only with a restart.

## GS-190

**Admin approve/reject drops `reviewed_by` / `reviewed_at` from its payload (it runs in a worker thread)** · debt · Accounts and community · ❌ Open · opened 2026-10-20

Where: `web/pages/admin.py::update_correction_status` (221-262), `approve/reject` (788-806)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Admin approve/reject drops `reviewed_by` / `reviewed_at` from its payload (it runs in a worker thread)**
- **File:** `web/pages/admin.py` (`update_correction_status` in `run.io_bound`; `GlobalAuthState.get_user_id()` is None there)
- **Status:** ❌ Open (2026-10-20, code reading)
- **Notes:** No visible effect while the 2026-09-25 guard trigger stamps both server-side (`migrations/add_privileged_column_guards.sql`). Pass the reviewer id captured on the loop.

*Triage 2026-10-25:* still open. update_correction_status still calls GlobalAuthState.get_user_id() inside the run.io_bound worker (admin.py:239) and approve/reject pass only client=get_user_client() (792-806), so the stamp is skipped; the server trigger in migrations/add_privileged_column_guards.sql:194-195 still stamps both, so there is no visible effect.

## GS-191

**A removal here never deletes a row this computer did not record** · debt · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_manager.py::_queue_record`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A removal here never deletes a row this computer did not record**
- **File:** `shared/lists_manager.py::_queue_record`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** By design. A second website row for one entry, or the row of an entry removed before its first sync with this installer, stays on the website and can bring the entry back at a Download.

*Triage 2026-10-25:* still open. _queue_record returns without queuing when the record is missing / 'gone' / has no id or list (lists_manager.py:76-77), so an unrecorded row stays on the website. Behaviour is deliberate and unchanged; the owner can close it as accepted.

## GS-192

**While lists.pkl cannot be saved, the website-removal prompt waits** · debt · Accounts and community · ❌ Open · opened 2026-09-27

Where: `genizah_app.py::_offer_web_removals`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **While lists.pkl cannot be saved, the website-removal prompt waits**
- **File:** `genizah_app.py::_offer_web_removals`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** An answer could not be kept; the prompt opens at the first sync end after a save lands.

*Triage 2026-10-25:* still open. _offer_web_removals returns and stops the offer when mgr.saves_failing() (3553-3555); the docstring (3545-3549) records it as intended. Unchanged.

## GS-193

**A list-sync worker can print `could not acquire lock for <stderr>` at exit** · debt · Accounts and community · ❌ Open · opened 2026-09-27

Where: `desktop/lists_sync_runner.py` (daemon worker)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A list-sync worker can print `could not acquire lock for <stderr>` at exit**
- **File:** `desktop/lists_sync_runner.py` (daemon worker)
- **Status:** ❌ Open (2026-09-27)
- **Notes:** Dev console only; the windowed exe has no stderr.

*Triage 2026-10-25:* still open. Workers are still daemon threads (threading.Thread(..., daemon=True), line 356) and shutdown leaves a worker inside a request running (module docstring, lines 18-20); no change to that.

## GS-194

**The server-side end of a desktop session is best effort** · debt · Accounts and community · ❌ Open · opened 2026-09-27

Where: `desktop/supabase_corrections_client.py::logout` (`revoke='background'`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **The server-side end of a desktop session is best effort**
- **File:** `desktop/supabase_corrections_client.py::logout` (`revoke='background'`)
- **Status:** ❌ Open (2026-09-27)
- **Notes:** A daemon thread, abandoned if the app exits first; the tokens are already gone from disk and memory.

*Triage 2026-10-25:* still open. supabase_corrections_client.py:698-720: revoke='background' drops the client, deletes the credentials file and starts a daemon thread 'supabase-sign-out'; docstring says the request is simply not made if the program exits first. Accepted design, unchanged.

## GS-195

**`sync_list_to_cloud` always creates a new cloud list for a list without a cloud id** · debt · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_sync.py::sync_list_to_cloud`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **`sync_list_to_cloud` always creates a new cloud list for a list without a cloud id**
- **File:** `shared/lists_sync.py::sync_list_to_cloud`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** The upload takes the lowest-id free cloud list of the same name; this helper inserts another. Its only caller, `sync_item_to_cloud`, has none.

*Triage 2026-10-25:* still open. lists_sync.py:2843-2902: no cloud_id -> client.table('user_lists').insert(...). Its only caller is sync_item_to_cloud (2935), which has no caller in production code (only in tests/*); so it is latent, not reachable from the apps.

## GS-196

**List sync makes requests it could skip** · debt · Accounts and community · ❌ Open · opened 2026-09-27

Where: `shared/lists_sync.py` (`get_cloud_lists_preview`, `_push_projects_and_lists`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **List sync makes requests it could skip**
- **File:** `shared/lists_sync.py` (`get_cloud_lists_preview`, `_push_projects_and_lists`)
- **Status:** ❌ Open (2026-09-27)
- **Notes:** The sign-in preview counts entries with one `select('id')` per list, capped at the API's row limit, so a list over it shows a capped count (display only; `migrations/add_list_item_counts_rpc.sql` has a batched RPC); every upload updates every list's cloud row, changed or not.

*Triage 2026-10-25:* still open. lists_sync.py:849 one list_items select('id') per list (len of capped data); _push_projects_and_lists (1355-1376) issues an update on every list with a cloud id regardless of change. migrations/add_list_item_counts_rpc.sql exists but is not used here.

## GS-197

**`requirements-lock.txt` is a Windows resolution, so Linux-only packages still float on deploy** · debt · Build, CI and tests · ❌ Open · opened 2026-10-21

Where: `requirements-lock.txt`; `deploy.sh` (`-c requirements-lock.txt` since 2026-10-21)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **`requirements-lock.txt` is a Windows resolution, so Linux-only packages still float on deploy**
- **File:** `requirements-lock.txt`; `deploy.sh` (`-c requirements-lock.txt` since 2026-10-21)
- **Status:** ❌ Open (2026-10-21, Codex review; pre-existing)
- **Notes:** The constraints pin every package the lock lists, but packages only Linux needs are not in it (e.g. `SecretStorage` and `jeepney`, which `keyring` requires there), so the server resolves them freely, as it did before the constraint. Fix: generate the server's closure on Linux / Python 3.11 (a second lock or a `pip freeze` from the server) and constrain with that.

*Triage 2026-10-25:* still open. deploy.sh:23 `pip install -q -r requirements.txt -c requirements-lock.txt`; the lock pins keyring==25.7.0 (line 53) but contains no SecretStorage or jeepney, and no commit since 2026-10-21 touched the lock or deploy.sh.

## GS-198

**`scripts/verify_export.py` relies on the removed export fallback** · debt · Build, CI and tests · ❌ Open · opened 2026-09-25

Where: `scripts/verify_export.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **`scripts/verify_export.py` relies on the removed export fallback**
- **File:** `scripts/verify_export.py`
- **Status:** ❌ Open (2026-09-25)
- **Notes:** It exports from an empty table and expected every result; it now gets "Nothing to export" and exits 1. Dev script, not in CI: load rows first or delete it.

*Triage 2026-10-25:* still open. The script only sets gui.last_results; genizah_app.py:26743-26757 _collect_sorted_results reads the table only ('Empty is an answer: substituting last_results here exported every result'), and export_results (27904-27907) returns with the 'Nothing to export' notice, so no file is written and the script exits 1. Not referenced by CI.

## GS-199

**`tests/test_translations_no_duplicate_keys.py` is committed with `\r\r\n` on every line** · debt · Build, CI and tests · ❌ Open · opened 2026-09-21

Where: `tests/test_translations_no_duplicate_keys.py`

Moved 2026-10-25 from `5. Code Quality Debt / Repo structure Round 1 findings (2026-09-18)`, where it read:

- **Issue:** **`tests/test_translations_no_duplicate_keys.py` is committed with `\r\r\n` on every line**
- **File:** `tests/test_translations_no_duplicate_keys.py`
- **Status:** ❌ Open (2026-09-21)
- **Notes:** The only tracked `.py` whose blob carries `\r\r\n`; Python reads each as two newlines, so interpreter, AST and traceback line numbers are about double what `git grep -n` prints. It runs correctly, and `git status` shows it clean (`core.autocrlf`). Fix: `git add --renormalize` in its own commit, with an AST comparison proving nothing else changed. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. git cat-file -p HEAD:tests/test_translations_no_duplicate_keys.py | od -c shows '\r \r \n' after the first lines (offsets 0000032, 0000115, 0000122); last touching commit is 1bfa56c0, no renormalize commit since.

## GS-200

**`bodleian_master_index.csv` has no code reader but ships in every installer** · debt · Build, CI and tests · ❌ Open · opened —

Where: `GenizahSearchPro.spec` datas

Moved 2026-10-25 from `5. Code Quality Debt / Repo structure Round 1 findings (2026-09-18)`, where it read:

- **Issue:** **`bodleian_master_index.csv` has no code reader but ships in every installer**
- **File:** `GenizahSearchPro.spec` datas
- **Status:** ❌ Open (owner call)
- **Notes:** `.gitignore` already files it under "legacy, unreferenced" (Phase 65). Dropping it from the spec removes 89 KB from each installer; no code path changes.

*Triage 2026-10-25:* still open. GenizahSearchPro.spec:29 still lists ('bodleian_master_index.csv', '.') in datas; a repo-wide grep (excluding .claude/.planning/docs) finds the name only in the spec and .gitignore, no .py reader; the file is still tracked.

## GS-201

**fgp_transcriptions.db is in the installer spec with its producer scripts untracked and FGP absent from the updater map** · debt · Build, CI and tests · ❌ Partly fixed · opened —

Where: `GenizahSearchPro.spec`, `fgp_data/fgp_*.py` (untracked), `gui_threads.py` sidecar-updater map

Moved 2026-10-25 from `5. Code Quality Debt / Repo structure Round 1 findings (2026-09-18)`, where it read:

- **Issue:** **`fgp_data/fgp_transcriptions.db` is already in the spec while §4.5 says shipping is pending; its producer scripts are gitignored**
- **File:** `GenizahSearchPro.spec`, `fgp_data/fgp_*.py` (untracked), `gui_threads.py` sidecar-updater map
- **Status:** ❌ Open
- **Notes:** 405 MB rides in every installer with the flag off; the scripts that build it and the schema README exist only on the owner's machine (`fgp_data/` is ignored whole); FGP is absent from the post-install updater map.

*Triage 2026-10-25:* partly fixed. The 'shipping pending / flag off' premise is stale: FGP shipped in v8.2.0 and FGP_TRANSCRIPTIONS_ENABLED defaults ON (web/feature_flags.py:90, shared/fgp_service.py:123-125), so the 405 MB DB is now used. Still true: `git ls-files fgp_data` is empty and .gitignore:64 ignores fgp_data/ whole (producer scripts and README exist only on the owner's machine); service_map in desktop/gui_threads.py:1960-1964 has no FGP entry. **Still open:** Producer scripts (fgp_extract.py, fgp_resolve_sysid.py, ...) and the schema README are untracked/owner-machine only; FGP is missing from the desktop updater map (same gap as T228 item 1).

## GS-202

**Small hygiene gaps found by the inventory** · debt · Build, CI and tests · ❌ Open · opened —

Where: .gitignore, `desktop/settings_dialogs.py:339`, `genizah_app.py:36939`, `scripts/verify_passage_index.py`

Moved 2026-10-25 from `5. Code Quality Debt / Repo structure Round 1 findings (2026-09-18)`, where it read:

- **Issue:** **Small hygiene gaps found by the inventory**
- **File:** `.gitignore`, `desktop/settings_dialogs.py` + `genizah_app.py`, `version.py` + `version_info.txt`, `scripts/verify_passage_index.py`, `scripts/build_cudl_fixture.py`, `scripts/build_residue_patterns_artifact.py`
- **Status:** ❌ Open
- **Notes:** `.server.pid` is not gitignored; `icon.ico` is resolved by two different rules (`Config.BASE_DIR` vs `resource_path()`); nothing checks `version.py` against `version_info.txt`; `verify_passage_index.py` is run by no test or job (audit M32); the two scripts read untracked `reports/*` inputs (`cudl_orphans_all.csv`, `synthetic_ambiguity_residue_dryrun.csv`).

*Triage 2026-10-25:* still open. .gitignore has only 'app.pid' (line 102), no .server.pid; settings_dialogs.py:339 uses Config.BASE_DIR while genizah_app.py:36939 uses resource_path(); no test mentions version_info.txt; git grep finds verify_passage_index only in its own docstring and docs; build_residue_patterns_artifact.py:3 still reads reports/synthetic_ambiguity_residue_dryrun.csv (reports/* is ignored, .gitignore:71).

## GS-203

**MARC field translations (Date/Subjects/People)** · test-gap · Desktop app · ⏳ Needs a check · opened —

Where: `desktop/result_dialog.py:2246`, :3798; `desktop/title_helpers.py::_translate_hebrew_date`

Moved 2026-10-25 from `3. Untested Areas`, where it read:

- **Area:** **MARC field translations (Date/Subjects/People)**
- **Status:** ❌ Needs Testing · ✋ *By hand:* English UI with translations on (sys_id 990001430180205171): the badges, the Dicta translations of Subjects/People, the Original/Translated toggle.
- **Notes:** Added translate badges for Date, Subjects, People in ResultDialog and Browse extended info. Hebrew dates use direct gematria converter (`_translate_hebrew_date`) to avoid Dicta errors (e.g. "מאה ט״ו" → "15th century"). Subjects/People use Dicta on-demand. Test: open records with Hebrew dates, subjects, people in EN UI with translations ON. Verify badges appear, translations are correct, toggle works. Test record: sys_id 990001430180205171.

*Triage 2026-10-25:* needs a check. The feature code is present (desktop/title_helpers.py:43, used at result_dialog.py:2246 and :3798) but no test under tests/ exercises _translate_hebrew_date or the badges (grep finds only tests/test_no_back_edges_desktop.py naming title_helpers). **Would settle it:** By hand in the desktop app: English UI with translations on, open sys_id 990001430180205171 and check the Date/Subjects/People badges, the Dicta translations and the Original/Translated toggle.

## GS-204

**Desktop list sync by hand: the progress dialog and a pulled-cable log-out** · test-gap · Accounts and community · ⏳ Needs a check · opened 2026-09-27

Where: `desktop/lists_sync_runner.py` and the sync progress dialog

Moved 2026-10-25 from `3. Untested Areas`, where it read:

- **Area:** **Desktop list sync by hand: the progress dialog and a pulled-cable log-out**
- **Status:** ❌ Open (2026-09-27) · ✋ *By hand:* Cancel and Esc on the sync progress dialog, window-modality, and a log-out with the network off.
- **Notes:** Cancel, Esc and window-modality of the real list-sync progress dialog, and a log-out with the network cable pulled, have not been run by hand. Owed before the installer ships.

*Triage 2026-10-25:* needs a check. No record in CHANGELOG.md, the tracker or the archive that the by-hand check was run; v9.10.0 shipped since 2026-09-27 and nothing says it was done. **Would settle it:** Run the installed desktop build: start a list sync, press Cancel and Esc on the progress dialog, check the dialog is window-modal, then log out with the network cable out/Wi-Fi off.

## GS-205

**End-to-End Integration** · test-gap · Accounts and community · ❌ Open · opened —

Where: `tests/e2e/` `(test_browse_flow.py`, `test_search_flow.py`, `test_performance.py` only)

Moved 2026-10-25 from `3. Untested Areas`, where it read:

- **Area:** **End-to-End Integration**
- **Status:** ❌ Not Tested
- **Notes:** Full flows: Search→View→Edit→Submit→Approve

*Triage 2026-10-25:* still open. tests/e2e/ holds only browse, search and performance flows; grep for correct/approve/submit in tests/e2e finds nothing relevant. Corrections are covered by unit tests with mocks (tests/test_corrections_service.py, test_corrections_page_write_handlers.py, test_supabase_corrections_client.py), not as a flow.

## GS-206

**Concurrency** · test-gap · Accounts and community · ❌ Open · opened —

Where: `web/supabase_client.py::update_correction`; `tests/test_corrections_*.py`

Moved 2026-10-25 from `3. Untested Areas`, where it read:

- **Area:** **Concurrency**
- **Status:** ❌ Not Tested
- **Notes:** Two users editing same correction simultaneously

*Triage 2026-10-25:* still open. web/supabase_client.py:1623-1632 update_correction is a plain `.update(data).eq('id', ...)` with no status/version guard (last write wins), and no test in tests/test_corrections_service.py, test_corrections_page_write_handlers.py or test_supabase_corrections_client.py mentions concurrent or simultaneous edits.

## GS-207

**A stale "interrupted" search cut-off leaks into `test_worker_result_carries_the_engines_cut_off`** · test-gap · Build, CI and tests · ❌ Open · opened 2026-10-25

Where: `tests/test_web_search_within_cutoff.py`; `shared/research_worker.py::run_query`; _LAST_SEARCH_CUTOFF

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A stale "interrupted" search cut-off leaks into `test_worker_result_carries_the_engines_cut_off`**
- **File:** `tests/test_web_search_within_cutoff.py`, `shared/search_engine.py` (`_LAST_SEARCH_CUTOFF`), `shared/research_worker.py`
- **Status:** ❌ Open (2026-10-25)
- **Notes:** The test passes alone (38 passed, run three times) but fails when it runs after the other files of one 29-file chunk in the same process: `result['cutoff']` is `{'capped': True, 'interrupted': True}`, not `interrupted: False`. Reproduced identically on `26ae875` (before the startup-tips change), so it is not that change's. Cause: `_note_search_cutoff` ORs into a thread-local value that only `consume_last_search_cutoff()` clears, and an earlier test on the main thread leaves `interrupted=True` behind; `research_worker.run_query` reads it without clearing it first. Fix sketch: `run_query` (or the engine at the start of a search) calls `consume_last_search_cutoff()` before running, which also guards a long-lived thread; or an autouse fixture that clears `_LAST_SEARCH_CUTOFF`. Found by the desktop branch `claude/sleepy-hawking-id0eg0`'s chunked test run.

*Triage 2026-10-25:* still open. run_query (shared/research_worker.py:215-278) still reads consume_last_search_cutoff() without draining it first, and the test's fake Engine.execute_search (tests/test_web_search_within_cutoff.py:108-112) does not drain either; no autouse fixture clears _LAST_SEARCH_CUTOFF (grep of tests/conftest.py empty). The product is safe: the real execute_search drains at its start (shared/search_engine.py:3863), so this is test isolation only.

## GS-208

**Responsa real-data gate: two mutations cannot fail on the real index** · test-gap · Build, CI and tests · ❌ Open · opened 2026-10-23

Where: `scripts/verify_responsa_wildcards.py::PHASE3`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Responsa real-data gate: two mutations cannot fail on the real index**
- **File:** `scripts/verify_responsa_wildcards.py::PHASE3`
- **Status:** ❌ Open (2026-10-23)
- **Notes:** Measured on the owner's laptop (2026-10-23, both indexes). `--mutation no-star-windows` on today's format-1 index exits 0 with #442's Phase 3 set: the only star crossing the set has on the real index (`כהנ* גדול*`, IE45532164 p69 into p70) is that manuscript's first match, which whole-first-match reading finds too. `--mutation finite-crossings-whole` on the format-2 index exits 1 with #443's set only because of the bracketed-quote page (its own fragment): the set's nine finite queries have 0 crossings on the real index. Both are proven red on the synthetic corpus of `tests/test_verify_responsa_wildcards.py`. To make them able to fail on real data: add a Responsa finite pair with real crossings (`crossing-diff` shows `אהרן הכהן` with 2 in literal mode) and a trailing-star query whose crossing follows an in-page match (`*נים *לים` does it, 4 crossings lost under the mutation, but it belongs to #443's set). Adding a query changes the oracle cache key and costs a corpus rescan (43-56 min).

*Triage 2026-10-25:* still open. PHASE3 is unchanged since d8b6f09c/46281409 (2026-10-23): it has no finite pair with real crossings (the suggested one for Aharon ha-Kohen is absent) and no added trailing-star query; the last commit to the script is d8b6f09c.

## GS-209

**Windows CI: a test fixture's Tantivy `commit()` can fail with "Access is denied (os error 5)"** · test-gap · Build, CI and tests · ❌ Open · opened 2026-10-22

Where: `tests/test_whole_word_fuzzy.py` fixtures (w.commit()); LocalIndexer._commit_writer_with_retry

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Windows CI: a test fixture's Tantivy `commit()` can fail with "Access is denied (os error 5)"**
- **File:** `tests/test_whole_word_fuzzy.py::local_cases` (and any fixture that builds an index in `tmp_path` and commits once)
- **Status:** ❌ Open (2026-10-22)
- **Notes:** Seen once, on PR #437's first CI run (windows-latest, shard 1/3): `test_my_library_fuzzy_at_a_position_finds_a_near_spelling_there[all]` errored at `w.commit()` in its fixture with `ValueError: An IO error occurred: 'Access is denied. (os error 5)'`; the same test passed on every other recent run, and the PR does not touch it. This is the transient Windows error (antivirus or a reader holding a file) that `shared/local_indexer.py::LocalIndexer._commit_writer_with_retry` already retries in the product; the test fixtures commit without that retry. Fix: a shared test helper that commits with the same retry, used by the fixtures that build an index, or rerun the job when it happens.

*Triage 2026-10-25:* still open. The fixtures still call w.commit() directly; the retry exists only in the product (_commit_writer_with_retry, tested in tests/test_local_commit_retry.py) and no shared test helper exists (grep of tests/ and shared/). No commit to the test file since the row.

## GS-210

**Test: the Joins Lab refused-search test times out in a long single-process run** · test-gap · Build, CI and tests · ⏳ Needs a check · opened 2026-10-22

Where: `tests/test_responsa_refusal_surfaces.py::test_a_refused_web_search_does_not_bring...`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Test: the Joins Lab refused-search test times out in a long single-process run**
- **File:** `tests/test_responsa_refusal_surfaces.py::test_a_refused_web_search_does_not_bring_the_previous_cards_back`
- **Status:** ❌ Open (2026-10-22)
- **Notes:** Fails every time in one long single-process run of the 68 test files that touch the search page, `web/main.py` and the import guards -- on `origin/master-main` (1 failed, 1,585 passed) as on PR #436's branch (3 runs) -- and passes alone (4 of 4, with its file and with `-k`). The driver's own `_wait_for` times out after the first search (`AssertionError('timed out ... assert <deadline> < <now>')`, `{'first': True, 'saved': 3}`), before the refusal step: an earlier test in that run leaves the loop slower or holding something the Joins Lab page waits on. It drives `/joins-lab` only. Not seen in CI's sharded runs. Fix sketch: find the earlier file that triggers it (bisect the run's file list), then make the test independent of it -- or wait on the events it already records instead of a wall-clock deadline.

*Triage 2026-10-25:* needs a check. The test still waits on wall-clock _wait_for deadlines (e.g. lines 152, 156, 163); the file's last change is d4bd357d (2026-10-22), before this row's date was settled. The failure only shows in a multi-file single-process run of 68 files, which is too heavy to repeat here. **Would settle it:** Run the 68 files that touch the search page, web/main.py and the import guards in one pytest process, then bisect the file list to find the earlier file that slows the loop.

## GS-211

**Tests that build a /browse page make real anonymous Supabase reads against the production URL** · test-gap · Build, CI and tests · ❌ Open · opened 2026-10-20

Where: `tests/conftest.py`; `tests/test_browse_recents_off_loop.py::supabase_offline`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Tests that build a /browse page make real anonymous Supabase reads against the production URL**
- **File:** the page's comments loader and joins panel (committed default URL and anon key); likely `tests/test_browse_print_control.py`, `tests/test_browse_computed_deeplink.py`, `tests/test_discovery_panel_browse_wiring.py` (not checked)
- **Status:** ❌ Open (2026-10-20)
- **Notes:** Found when a new test hit PostgREST (harmless reads, one a uuid parse error). Tests then depend on the network and touch production. `tests/test_ux_timing.py` and `tests/test_browse_recents_off_loop.py` use a `supabase_offline` fixture (the URL points at a refused local port); make it an autouse default in conftest.

*Triage 2026-10-25:* still open. supabase_offline is defined per test file (tests/test_browse_recents_off_loop.py:36, tests/test_ux_timing.py:309) and imported by one more; tests/conftest.py has no autouse Supabase-offline fixture (autouse list checked: NLI breaker, loop isolation, local indexer, Qt dialog, personal state, NLI store, GUI keeper).

## GS-212

**`test_filter_unavailable_desktop_window.py` fails locally when the saved UI language is Hebrew** · test-gap · Build, CI and tests · ❌ Open · opened 2026-10-14

Where: `tests/test_filter_unavailable_desktop_window.py` (line 378, English status-bar comparison)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **`test_filter_unavailable_desktop_window.py` fails locally when the saved UI language is Hebrew**
- **File:** `tests/test_filter_unavailable_desktop_window.py` (`statusBar().currentMessage() == 'Search cancelled'`)
- **Status:** ❌ Open (found 2026-10-14, pre-existing: fails identically on `master-main`)
- **Notes:** The window under test reads the language from the local config, so on a machine set to Hebrew the status line is "החיפוש בוטל" and the English comparison fails. Fix: compare with `tr('Search cancelled')`, or pin `CURRENT_LANG` for the test.

*Triage 2026-10-25:* still open. Line 378 still asserts statusBar().currentMessage() == 'Search cancelled'. Ran test_replacing_the_filters_cancels_a_waiting_search this session: failed with the Hebrew message. The conftest isolation (tests/conftest.py:540-547) redirects lang.pkl per test but genizah_core.CURRENT_LANG is loaded once at import (genizah_core.py:618).

## GS-213

**`test_search_regex.py` `\B` boundary tests time out after other search tests in one process** · test-gap · Build, CI and tests · ❌ Open · opened 2026-10-11

Where: `test_search_regex.py::test_unicode_boundaries_and_search_bounds`; `search_regex.py::_match_timeout`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **`test_search_regex.py` `\B` boundary tests time out after other search tests in one process**
- **File:** `tests/test_search_regex.py::test_unicode_boundaries_and_search_bounds` (the `\B` cases), `shared/search_regex.py::_match_timeout` (10 s per call)
- **Status:** ❌ Open (found 2026-10-11)
- **Notes:** Run after several search test files in one process, the three `\B` cases raise `TimeoutError`; alone each takes about 2 s, and `scripts/run_local_tests.py` chunks pass. Measured 2026-10-12: `\B` over all of Unicode is about 1.1M `regex` matches, 1.07 s alone but over 10 s with one CPU-busy Python thread running (that a thread leaked by an earlier test is it is a hypothesis). Raising the timeout would be a mitigation, not the fix. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. Test (test_search_regex.py:61-71) and _match_timeout (search_regex.py:111-126, 10 s default) are unchanged since 2026-10-12 (git log). Not reproduced here: test_native_matching.py + test_native_matching_gap.py + test_search_regex.py in one process gave 502 passed in 23 s, so the failing order is a different one; the row's hypothesis is untested.

## GS-214

**Test-isolation flake: the time-limit lower-bound API test gets 429 rate_limited in a multi-file batch** · test-gap · Build, CI and tests · ❌ Open · opened 2026-10-09

Where: `tests/test_search_api.py:874`, the module-level API rate limiter

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Test-isolation flake: `test_a_search_stopped_at_the_time_limit_says_its_total_is_a_lower_bound` gets 429 `rate_limited` in a multi-file batch**
- **File:** `tests/test_search_api.py:874`, the module-level API rate limiter
- **Status:** ❌ Open (found 2026-10-09)
- **Notes:** Passes alone (3/3) and with its own file (45 passed, 2/2). Fails in one `pytest` process over the GPT/API files (`test_api_channel_telemetry`, `test_chatgpt_*`, `test_api_hardening`, `test_search_api*`, `test_parallels_api`, `test_search_serializer`): earlier files spend the shared per-IP bucket, so the request answers 429 instead of 200. Reproduced identically on `b40aeea7`, before the MCP commit-3 changes to `web/api_hardening.py`. Not seen in `run_local_tests.py` chunks. Fix: reset or isolate the rate limiter per test (autouse fixture), or give that test its own client IP.

*Triage 2026-10-25:* still open. RateLimiter.reset_for_tests() exists (web/api_hardening.py:250) but tests/test_search_api.py never calls it (0 matches) and tests/conftest.py has no autouse reset; only tests/test_browse_api.py resets the limiters. The module-level bucket is therefore still shared across the GPT/API files.

## GS-215

**`test_search_api_library_mode.py` fails (500 `internal_error`) after two other test files in one process** · test-gap · Build, CI and tests · ❌ Open · opened 2026-10-07

Where: `web/research_jobs.py::shutdown_research`, wait_executor; `tests/test_search_api_library_mode.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **`test_search_api_library_mode.py` fails (500 `internal_error`) after two other test files in one process**
- **File:** `tests/test_search_api_v2.py` + `tests/test_web_saved_joins_isolation.py`, then `tests/test_search_api_library_mode.py`
- **Status:** ❌ Open (2026-10-07)
- **Notes:** Run in that order in one pytest process, 6 of the 9 library-mode tests get a 500 from `/api/search` and `/api/parallels`; each file passes alone, and so does either of the first two with the third. Reproduced on master-main 6f710013, so not a product change: one of the two leaves process state (the app's middleware stack or a patched module attribute) that the third reads. `run_local_tests.py` put the three in one chunk on `fix/web-search-cutoff` (2026-10-07).

*Triage 2026-10-25:* still open. Reproduced today at ca809c56: pytest -x tests/test_search_api_v2.py tests/test_web_saved_joins_isolation.py tests/test_search_api_library_mode.py -> 1 failed (test_include_mode_is_default_same_as_omitted), 154 passed; log shows RuntimeError 'cannot schedule new futures after shutdown' raised from wait_executor() in search_api.py:1815; shutdown_research() shuts _wait_executor and wait_executor() never recreates it, so an earlier file that runs the app lifespan poisons later API tests (which file not isolated). d5132deb fixed a different leak (state.searcher) in test_search_api_v2 only.

## GS-216

**Two groups of tests never run in CI** · test-gap · Build, CI and tests · ❌ Open · opened 2026-10-06

Where: `tests/conftest.py` (collect_ignore_glob); `tests/e2e/conftest.py`; `.github/workflows/ci.yml`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Two groups of tests never run in CI**
- **File:** `tests/conftest.py` (`collect_ignore_glob` when `GITHUB_ACTIONS` is set), `tests/e2e/`, `.github/workflows/ci.yml` (slow-tests comment)
- **Status:** ❌ Open (sweep #37, #38; rechecked 2026-10-06)
- **Notes:** (a) About 133 desktop tests in 10 file patterns (My Library tab, LOCAL PDF rendering, opt-out persistence, recovery-scan cleanup, Joins Lab construction, ...) are skipped on both CI systems; D-F15 covers only one of them. (b) `tests/e2e/` (17 tests) skips everywhere: selenium is in no requirements file, although the slow-tests job's comment says the e2e test runs there.

*Triage 2026-10-25:* still open. conftest.py still extends collect_ignore_glob with 10 patterns when GITHUB_ACTIONS is true; tests/e2e/conftest.py skips when selenium/pytest-selenium are missing and neither appears in requirements*.txt or the workflows (grep), while ci.yml:359 comment says e2e runs in slow-tests.

## GS-217

**Test and gate tooling gaps** · test-gap · Build, CI and tests · ❌ Open · opened 2026-10-06

Where: `scripts/run_local_tests.py`, `schedule_nightly_artifact_verify.ps1`, `init.ps1`, `export_access_index.py`

Moved 2026-10-25 from `5. Code Quality Debt / 2026-09-25 sweep: hygiene and tooling (logged 2026-10-06)`, where it read:

- **Issue:** **Test and gate tooling gaps**
- **File:** `scripts/run_local_tests.py`, `scripts/schedule_nightly_artifact_verify.ps1`, `scripts/init.ps1`, `scripts/export_access_index.py`
- **Status:** ❌ Open (sweep L14, L24, L25; rechecked 2026-10-06)
- **Notes:** (a) `--record` cannot learn a new heavy module's duration: it spreads each chunk's time by the previous weights (`DEFAULT_DURATION = 1.0`). (b) The nightly artifact verify is registered with an interactive logon and no wake, and nothing warns when a night is missed (`init.ps1` does not read `_tmp/artifact-verify-log.jsonl`; the log shows nightly runs through 2026-10-05). (c) No test covers the masking gate in `scripts/export_access_index.py` (v9.0.0 audit H14).

*Triage 2026-10-25:* still open. (a) _record_durations still spreads each chunk's time by previous weights (DEFAULT_DURATION=1.0, :81,:633); (b) schedule script comments 'Deliberately NOT WakeToRun' (:128) and init.ps1 never references artifact-verify-log; (c) git grep finds no test referencing export_access_index. No commit since 2026-10-06 touches these three concerns.

## GS-218

**`tests/test_findings_page.py` fails locally when `web.main` is imported before it** · test-gap · Build, CI and tests · ❌ Open · opened 2026-10-05

Where: `tests/test_findings_page.py`, `tests/render_smoke/conftest.py`, `web/main.py` (load_dotenv at import)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **`tests/test_findings_page.py` fails locally when `web.main` is imported before it**
- **File:** `tests/test_findings_page.py`, `tests/render_smoke/conftest.py` (imports `web.main` at load), `web/main.py` (`load_dotenv()` at import)
- **Status:** ❌ Open (found 2026-10-05) · **Rechecked 2026-10-06:** still 4 failed with the command below; all pass with `GENIZAH_DISCOVERY_DATA_DIR` set to an empty folder, which confirms the cause.
- **Notes:** `web.main` loads `.env` at import, so if it is imported before `web.discovery_assets`, the local `GENIZAH_DISCOVERY_DATA_DIR` makes four findings-page tests render the artifact staged in `discovery_data/live` instead of their stubs. Reproduce: `pytest tests/test_findings_page.py tests/render_smoke/test_start_render_smoke.py -k "headline_slot or pool_invitation or start"`. Local only (CI has no `.env`). Fix in the tests (pin the discovery state), not by import order. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. Ran the row's reproduce command this session: 4 failed (headline_slot en/he, pool_invitation main-en/main-he), 13 passed. web/main.py:21 still calls load_dotenv(); test_findings_page.py has no commit since 2026-10-05 and no GENIZAH_DISCOVERY_DATA_DIR pin.

## GS-219

**`test_findings_page.py` hangs a runner chunk now and then** · test-gap · Build, CI and tests · ❌ Open · opened 2026-10-04

Where: `tests/test_findings_page.py` (24 asyncio.run sites), `web/bounded_io.py`, `web/discovery.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **`test_findings_page.py` hangs a runner chunk now and then**
- **File:** `tests/test_findings_page.py::_render_page` (`asyncio.run` of `create_findings_page`), `web/bounded_io.py`, `web/discovery.py::_SUPPRESSION_SEMAPHORE`
- **Status:** ❌ Open (found 2026-10-04; cause found 2026-10-19)
- **Notes:** Twice in four full runs its chunk waited forever (py-spy: `test_paging_past_the_capped_end_is_reachable`, every thread idle). **Likely cause, reproduced on demand 2026-10-19:** `bounded_io_bound` releases its permit from the worker future's done-callback, which arrives through the event loop; when a test's loop closes while the hide-list read is still in flight, the callback never runs, so the process-wide `asyncio.Semaphore(1)` stays taken and every later render waits at `acquire()` (same under plain `asyncio.run`). Production is unaffected (one loop per process). The render-smoke harness now drains in-flight executor futures before closing a loop (`tests/render_smoke/_bounded_run.py::_record_executor_futures`, PR #413); this file still uses plain `asyncio.run` (24 sites). Fix: run them through `run_bounded`. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. tests/test_findings_page.py still has 24 asyncio.run( sites and no run_bounded; the only later commits are to tests/render_smoke (7a095c04, 7a941542, 2954f1df, 2026-10-19), as the row says.

## GS-220

**GUI test files crash when certain pairs share a process (0xC0000005)** · test-gap · Build, CI and tests · ❌ Open · opened 2026-09-30

Where: test_my_library_tab+test_crash_hooks; test_local_ceiling_enforcement+test_comp_tree_batch_load

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **GUI test files crash when certain pairs share a process (0xC0000005)**
- **File:** `tests/test_my_library_tab.py` then `tests/test_crash_hooks.py`; `tests/test_local_ceiling_enforcement.py` then `tests/test_comp_tree_batch_load.py`
- **Status:** ❌ Open (found 2026-09-30; widened 2026-10-06)
- **Notes:** Each file passes alone and in the reverse order; in this order every run crashes, with `QT_QPA_PLATFORM=offscreen` too. **2026-10-06 on Windows:** the first pair now crashes DURING `test_crash_hooks.py::test_qtimer_slot_raise_reaches_excepthook` (38 of 40 passed), as on Linux, not at exit; the second pair, found by `run_local_tests.py -m gui` (chunk 25), crashes in `test_comp_tree_batch_load.py::test_all_items_are_loaded_across_batches`. CI runs GUI files one per process (`scripts/run_gui_tests.py`), so only chunked local GUI runs hit this. Likely a Qt object or thread that outlives its test.

*Triage 2026-10-25:* still open. Ran test_my_library_tab.py + test_crash_hooks.py in one process this session (offscreen): 38 tests passed, then a segmentation fault (exit 139), as the row describes. None of the four test files has a commit since 2026-10-06.

## GS-221

**Local Windows test runs use the native Qt platform, so two GUI tests fail locally that pass in CI** · test-gap · Build, CI and tests · ❌ Open · opened 2026-09-30

Where: `tests/conftest.py:29-30`; `scripts/run_local_tests.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Local Windows test runs use the native Qt platform, so two GUI tests fail locally that pass in CI**
- **File:** `tests/conftest.py` (sets `QT_QPA_PLATFORM=offscreen` only on Linux without `DISPLAY`), `tests/test_column_chooser.py`, `tests/test_eliding_label.py`
- **Status:** ❌ Open (found 2026-09-30; cause found 2026-10-06)
- **Notes:** CI sets `QT_QPA_PLATFORM=offscreen`; local Windows runs use the native platform, where `test_column_chooser.py::test_all_columns_fit_without_sideways_scrolling` (after `test_lists_sync_off_ui_thread.py`) gets 32 px instead of 30, and `test_eliding_label.py`'s zero-width test fails because Windows keeps a window at least 160 px wide. Both pass offscreen. Fix: set offscreen for every local run (conftest or the runners). Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. conftest.py:29-30 still sets QT_QPA_PLATFORM=offscreen only `if sys.platform.startswith('linux') and not DISPLAY`; no 'offscreen' in scripts/run_local_tests.py, pytest.ini or pyproject; conftest.py's last commit (bac67736, 10-21) did not touch it, and the two test files have no commit since 09-30.

## GS-222

**`test_flush_before_exit_total_wall_time_bounded` depends on test order** · test-gap · Build, CI and tests · ❌ Open · opened 2026-09-30

Where: `tests/test_telemetry_posthog_server_ext.py::TestFlushBeforeExit`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **`test_flush_before_exit_total_wall_time_bounded` depends on test order**
- **File:** `tests/test_telemetry_posthog_server_ext.py::TestFlushBeforeExit`
- **Status:** ❌ Open (found 2026-09-30)
- **Notes:** Alone it fails every time ("Per-POST timeout 2.0 must be <= deadline 0.5s"); inside its class it passes; in two full runs it failed on the 1.0 s wall-time bound. Neither the test nor `shared/posthog_server.py` changed on `ccr-b3b8c7ac-jyxlw3`. Some earlier test in the class sets state this one relies on.

*Triage 2026-10-25:* still open. Ran it alone this session (pytest -p no:cacheprovider): FAILED with 'Per-POST timeout 2.0 must be <= deadline 0.5s' at test line 342. _flush_before_exit (posthog_server.py:328) does pass min(remaining, 2.0), so the 2.0 POST came from the drain thread: a test-isolation fault. Neither file changed since June.

## GS-223

**Tests still touch the developer's data folder at import, and two personal files are not isolated** · test-gap · Build, CI and tests · ❌ Open · opened 2026-09-25

Where: `genizah_core.py` (logger handlers, `CURRENT_LANG = load_language()`), `tests/conftest.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Tests still touch the developer's data folder at import, and two personal files are not isolated**
- **File:** `genizah_core.py` (logger handlers, `CURRENT_LANG = load_language()`), `tests/conftest.py`
- **Status:** ❌ Open (2026-09-25)
- **Notes:** Importing `genizah_core` during collection attaches DEBUG file handlers to the live `genizah.log` / `lab/lab_genizah.log` and reads the live `lang.pkl`, before any fixture runs. The conftest fixture isolates `session.json`, `config.pkl`, `lang.pkl` but not `ListsManager.LISTS_FILE` or `search_history.json`; tests that write them must patch them.

*Triage 2026-10-25:* still open. genizah_core still attaches SafeRotatingFileHandler(Config.LOG_FILE / LAB_LOG_FILE) and sets CURRENT_LANG = load_language() at import; the conftest fixture patches only SESSION_FILE, CONFIG_FILE, LANGUAGE_FILE, and its own comment (conftest.py:525-526) says LISTS_FILE and the search-history file are not covered.

## GS-224

**The Stable-Citations gate cannot see bare shorthand line references** · test-gap · Build, CI and tests · ❌ Open · opened 2026-09-20

Where: `scripts/check_docs.py` (`LINE_CITATION_RE`), `.planning/milestones/v9.0.0-MILESTONE-AUDIT.md`

Moved 2026-10-25 from `5. Code Quality Debt / Repo structure Round 1 findings (2026-09-18)`, where it read:

- **Issue:** **The Stable-Citations gate cannot see bare shorthand line references**
- **File:** `scripts/check_docs.py` (`LINE_CITATION_RE`), `.planning/milestones/v9.0.0-MILESTONE-AUDIT.md`
- **Status:** ❌ Open
- **Notes:** The regex needs the filename immediately before the number, so `the tracker (:211)` and `§5 (:100)` -- the same stale-pointer disease, written without the filename -- pass the gate. Both known instances were repaired by hand on 2026-09-20 (Codex, PR #350). A rule for the bare form needs care: `(:\d+)` also matches legitimate prose, so it would have to be scoped (e.g. only in files that cite a moving document elsewhere) or the audit format changed to forbid the shorthand outright.

*Triage 2026-10-25:* still open. scripts/check_docs.py:485 LINE_CITATION_RE is still the filename-anchored form used at :539; git log on check_docs.py since the row (c18411bd, 5315980a, ebc2fada) only added fence tracking and exemptions, no bare-form rule.

## GS-225

**Repo-wide `TestNoIntCoercion` scans walk `.worktrees/` and fail locally on stale worktree copies** · test-gap · Build, CI and tests · ❌ Open · opened 2026-09-17

Where: `tests/test_local_sys_id_namespace.py`, `tests/test_synthetic_sys_id.py` (the `os.walk` prune list)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Repo-wide `TestNoIntCoercion` scans walk `.worktrees/` and fail locally on stale worktree copies**
- **File:** `tests/test_local_sys_id_namespace.py`, `tests/test_synthetic_sys_id.py` (the `os.walk` prune list)
- **Status:** Open (2026-09-17); symptom cleared 2026-09-18
- **Notes:** The `os.walk` prune list skips .git, .venv, dist and Genizah_Index but not `.worktrees`, so each worktree's copy of `test_synthetic_sys_id.py` is reported as a violation (local only). The 2026-09-18 tidy-up removed the stale worktrees (backups under `refs/tidy-backup/*`), but the next worktree under the repo root re-triggers it. Fix: add `.worktrees` and `_tmp` to the prune list. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. SKIP_DIRS still lacks '.worktrees' (it has '_tmp' and '.claude' since the tests were written, so worktrees under .claude/worktrees, where they live now, ARE skipped; no .worktrees directory exists and nothing in the repo creates one). The defect only bites a worktree created elsewhere under the repo root.

## GS-226

**Three discovery dispatch tests went red after the panel render-smoke file (leaked heavy slot); no longer reproduces locally** · test-gap · Build, CI and tests · ⏳ Needs a check · opened 2026-08-09

Where: `tests/test_findings_page.py`, `tests/test_discovery_panel_browse_wiring.py`, `shared/discovery_service.py::_run_off_loop`

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

❌ **Open (2026-08-09) — the ZERO-dispatch failure is BACK, with a different trigger: a LEAKED heavy-concurrency slot.** Rechecked 2026-10-06: still red locally (3 of 3) after `tests/render_smoke/test_panel_render_smoke.py`, and only with the artifact the local `.env` stages; with `GENIZAH_DISCOVERY_DATA_DIR` at an empty folder all three pass, and CI is unaffected. The three: two `test_findings_page.py` dispatch-count tests and `test_discovery_panel_browse_wiring.py::test_busy_reaches_the_panel_THROUGH_THE_LIVE_GATE`; each file passes alone. Full text archived 2026-10-19.

*Triage 2026-10-25:* needs a check. Reran in this session (python 3.11, NiceGUI 3.18, local .env staging discovery_data/live): tests/render_smoke/test_panel_render_smoke.py followed by the three named tests in ONE process gave 216 passed, 0 skipped, and only the unrelated local masking-scan false-red failing (untracked pgp_data/documents.csv). No commit explains the change (harness 7d326b2a and conftest e8d5efcb are the only nearby test changes), and the _run_off_loop comment still says the failure is open. **Would settle it:** Run the real CI-shaped chunk (python scripts/run_local_tests.py) on the owner's machine with discovery_data/live staged; if the three pass in their normal chunk order, close it, otherwise find the leaked slot (service._heavy_sem) after test_panel_render_smoke.

## GS-227

**The FL-ID index build is an unjoined daemon thread and seven test files still start it against a MagicMock** · test-gap · Build, CI and tests · ❌ Open · opened 2026-05-29

Where: `shared/search_engine.py::start_fl_id_index_build`; 7 test files that build SearchEngine(MagicMock)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **The FL-ID index build is an unjoined daemon thread, and seven test files still start it against a `MagicMock` (crashed a full-suite run on Windows in 2026-05)**
- **File:** `shared/search_engine.py` (`SearchEngine.__init__` → `start_fl_id_index_build` → `_build_fl_id_index`)
- **Status:** ❌ Open (2026-05-29; narrowed 2026-10-05; no crash on 2026-10-06)
- **Notes:** The thread starts unless `worker_mode`, is never joined or cancelled, and calls `meta_mgr.parse_full_id_components` on whatever `meta_mgr` is; orphaned threads on a `MagicMock` access-violated a 2026-05 full-suite run. Seven files still start it: `test_corpus_scope_routing`, `test_local_hit_highlighting`, `test_local_nav_codex_fix4`, `test_local_nav_page_chunk`, `test_recovery_gate`, `test_side_index_merge`, `test_seed011_composition_dedup`. 2026-10-06: no crash in the full chunked run (13,860 passed) or with the seven in one process (37 passed). Fix: an autouse conftest fixture patching `start_fl_id_index_build`, or gate the thread on a real `meta_mgr`. Full text archived 2026-10-06.

*Triage 2026-10-25:* still open. SearchEngine.__init__ still calls start_fl_id_index_build() unless worker_mode (search_engine.py:1363-1364), which starts a daemon thread never joined (2048-2056); the seven named test files still build SearchEngine(MagicMock(), MagicMock()) (e.g. tests/test_corpus_scope_routing.py:20-26) and tests/conftest.py has no patch for it. No crash has been seen since 2026-10-06.

## GS-228

**Test-isolation flake: `test_discard_run_short_circuits_sqlite_on_tantivy_failure` fails in batch, passes in isolation** · test-gap · Build, CI and tests · ❌ Open · opened 2026-05-29

Where: `tests/test_phase_97_2_sqlite_vs_tantivy_consistency.py`, `shared/local_indexer.py::discard_run`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Test-isolation flake: `test_discard_run_short_circuits_sqlite_on_tantivy_failure` fails in batch, passes in isolation**
- **File:** `tests/test_phase_97_2_sqlite_vs_tantivy_consistency.py`, `shared/local_indexer.py::discard_run`
- **Status:** ❌ Open
- **Notes:** Surfaced 2026-05-29 (Phase 102 regression gate). Passes when run alone; fails when run in a large batch (shared Tantivy writer/temp-index state leaking across tests). `discard_run` untouched by Phase 102 — pre-existing isolation fragility. Fix path: ensure each test gets an isolated index dir + closes writer handles in teardown.

*Triage 2026-10-25:* still open. The test file is unchanged since 2026-05-26 (b5a44b94) and has no teardown beyond gc.collect(); I ran it alone this session: 1 passed in 1.86 s, matching the row. The batch-only failure itself was not reproduced here (no large batch run), but nothing in the code addresses it.

## GS-229

**FolderWalkWorker teardown race: test_unified_tree_uncheck_close_reopen_roundtrip (and siblings) not collected on CI** · test-gap · Build, CI and tests · ⏸ Deferred · opened 2026-05-28

Where: `tests/conftest.py:74-88` (collect_ignore_glob), `tests/test_local_optout_persistence.py:182`

Moved 2026-10-25 from `Deferred to v7.15+ (Phase 95 follow-up backlog)`, where it read:

- **#:** D-F15
- **Item:** ⏸ Deferred (2026-05-28, v7.15.0 release CI — v7.16) `test_unified_tree_uncheck_close_reopen_roundtrip` SIGSEGV on headless Ubuntu CI
- **Description:** `FolderWalkWorker` cleanup races offscreen-Qt teardown under pytest on Ubuntu CI (passes alone on Windows; not a production bug), so the test is skipped on Linux. Fix: make the worker's quit + wait finish before the parent widget is destroyed (`_cancel_existing_tree_worker`, `_on_tree_finished`), or a deterministic `worker.wait()` in `_wait_for_async_populate`. Full text archived 2026-10-19.
- **Priority:** P3

*Triage 2026-10-25:* still open. The per-test Linux skipif was replaced by commit 30421ccf with collect_ignore_glob under GITHUB_ACTIONS=true for test_local_optout_persistence.py and ~8 sibling Qt files (conftest.py:76-88), so the gate is wider than the row says; the conftest comment still defers the real worker-cleanup fix as D-F15.

## GS-230

**Phase 87 scanner edge case: bare chained attribute double-report** · test-gap · Build, CI and tests · ❌ Open · opened 2026-05-13

Where: `tests/test_no_raw_storage_access.py` (`_StorageAccessVisitor.visit_Attribute`)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Phase 87 scanner edge case: bare chained attribute double-report**
- **File:** `tests/test_no_raw_storage_access.py` (`_StorageAccessVisitor.visit_Attribute`)
- **Status:** ❌ Open (logged 2026-05-13, deferred — LOW)
- **Notes:** Bare chained attributes (`x = app.storage.user.foo`) would be reported twice by `_StorageAccessVisitor.visit_Attribute`; no production code uses the pattern. Fix when needed: mark inner Attribute nodes as seen, with a regression test expecting exactly 1 violation. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. visit_Attribute (lines 137-155) still marks nothing as seen for a bare chain; running _scan_file on 'x = app.storage.user.foo' returns two violations ((2,'app.storage.user.foo'), (2,'app.storage.user')). File untouched since af28cc8a.

## GS-231

**Composition Search conditions (must / must not appear): built and released in v9.7.0, still awaiting the user's G8 cases and scope answer** · idea · Search · ❌ Partly fixed · opened 2026-10-14

Where: `shared/compound_query.py`; `shared/passage_parallels.py::search_composition_logic`; `genizah_app.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Feature request: Composition Search conditions -- "must appear" / "must not appear" on the same image, an adjacent image, or the same manuscript (desktop letter-level)**
- **File:** `shared/compound_query.py` (new), `shared/passage_parallels.py::search_composition_logic`, `desktop/gui_threads.py`, `genizah_app.py` (Composition tab), `desktop/comp_view_filter.py`, `desktop/result_dialog.py`
- **Status:** ❌ Open (2026-10-14; built 2026-10-14, Codex-reviewed (3 rounds, every finding fixed) and MERGED to master-main as PR #399 on 2026-10-18, RELEASED in v9.7.0 (2026-10-19) -- Hebrew approved by the owner 2026-10-18, hand-tested by the owner in the installed exe; still awaiting the user's two G8 cases and the power user's answer on "same image" vs "whole manuscript"; plan §12, checklist in the #399 description)
- **Notes:** A heavy user's Tishrei 5787 letter, items D1 (exclude the quoting works, e.g. Halakhot Gedolot and the Rif when searching the Talmud), D2 (all of these texts, with versions) and D3 (two sides of a leaf, first as adjacent images, literal P±1 in the same sys_id and IE). Owner 2026-10-14: all three in one desktop release, conditions under the main text, removed pages in a collapsed group with the reason, a separate group for pages that could not be fully checked. Plan `docs/plans/COMPOSITION_CONDITIONS_PLAN.md`. Strict leaf sides, the website and Parts are out of scope (plan §9).

*Triage 2026-10-25:* partly fixed. The feature is in the code: shared/compound_query.py exists, genizah_app.py wires comp_conditions (:1599) and compound_query.validate/fingerprint (:19656-19662), plan docs/plans/COMPOSITION_CONDITIONS_PLAN.md; commits 3d423585, 958a2ac4, b1297333 (2026-10-14/15); released in v9.7.0. **Still open:** The owner still waits for the heavy user's two G8 cases (siddur example texts; a Talmud passage with a known Halakhot Gedolot/Rif parallel) and his answer on 'same image' vs 'whole manuscript'; nothing in the code is left to build.

## GS-232

**Variants verification could check words against the spelling set instead of one long regex** · idea · Search · ❌ Open · opened 2026-10-07

Where: `shared/search_engine.py` (variant regex 2763-2791, _variant_alternation 796)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Variants verification could check words against the spelling set instead of one long regex**
- **File:** `shared/search_engine.py` (the variant regex built from up to 8,000 spellings and run over every candidate page)
- **Status:** ❌ Open (2026-10-07, enhancement)
- **Notes:** Matching each page word against the permitted spelling set (Codex prototype: 0.376 s -> 0.004 s on 200 synthetic pages) could replace the long variant regex, and would also serve composition chunks; a `_FuzzyMatcher`-style letter-run walk needs no compile. Measure on the real index (ids, spans, highlighting) before adopting; keep the regex for custom multi-letter forms. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. Variants still verify with the compiled alternation of up to 8,000 spellings (now as prefix trees, 2791); no spelling-set matcher exists beside _FuzzyMatcher (1077), which is Fuzzy-only.

## GS-233

**Lab Mode results in a search-within chain** · idea · Search · ❌ Open · opened 2026-10-04

Where: `desktop/gui_threads.py::LabSearchThread`, `genizah_app.py::_enter_refine_mode`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Lab Mode results in a search-within chain**
- **File:** `desktop/gui_threads.py::LabSearchThread`, `genizah_app.py::_enter_refine_mode`
- **Status:** ❌ Open (found 2026-10-04)
- **Notes:** A Lab run reports no cut-off, so a stopped Lab run used for search-within is not marked "+"; and a step built from Lab results replays (restore, chip removal, completion) as a regular search. Owner 2026-10-04: not now (Lab is not a main method).

*Triage 2026-10-25:* still open. cutoff_signal and _last_search_cutoff exist only on SearchThread (gui_threads.py:272-370); LabSearchThread (:425) has none, so _run_left_matches_out cannot see a Lab cut. Owner ruling 2026-10-04 is to leave it.

## GS-234

**Desktop search: common words are still cut at 50,000 rows, and some routes show no early rows** · idea · Search · ❌ Open · opened 2026-04-17

Where: `shared/config.py:138`, `shared/search_engine.py:4333-4338`, `desktop/gui_threads.py:310`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Desktop search: common words are still cut at 50,000 rows, and some routes show no early rows**
- **File:** `shared/config.py` (`SEARCH_LIMIT`), `shared/search_engine.py::execute_search`, `desktop/gui_threads.py::SearchThread`
- **Status:** ❌ Open (2026-04-17; narrowed 2026-10-05; timed 2026-10-06)
- **Notes:** Since v9.5.0 the desktop paints rows while the search runs (`SearchThread.preview_signal`): first row 43-337 ms for שלום (2026-10-06). Still open: (1) the 50,000 `SEARCH_LIMIT` cuts frequent words (V0.8 page hits: של 59,090, על 364,259), shown as "N+" and completed only for search-within, the all-terms filter and replay; full coverage (Stage 2, D8 Phase 2) is unshipped by owner choice. (2) No early rows for scope 'all', exclusion words or Responsa (3.5-4.9 s blank, then all at once); 'local' has no preview path but finishes in under 0.25 s. (3) The first search after launch takes 14-20 s against 4-5 s later (the FL-ID build and browse_map repair run alongside). Plan: `docs/plans/SEARCH_UNCAPPED_STREAMING_PLAN.md`. Full text archived 2026-10-06.

*Triage 2026-10-25:* still open. Config.SEARCH_LIMIT is still 50000 (config.py:138). The preview is gated by corpus_scope == 'genizah' and no exclude_words and not Responsa (search_engine.py:4333-4337), so the other routes still paint at the end. Full coverage (Stage 2 / D8 Phase 2) is unshipped by owner choice; the Responsa limit is separate (RESPONSA_PAGE_LIMIT 150,000).

## GS-235

**Desktop composition search in chunk mode shows nothing until the whole scan ends (28-43 s)** · idea · Search · ❌ Open · opened 2026-04-17

Where: `desktop/gui_threads.py:499` (CompositionThread), `genizah_app.py` (run_composition)

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Desktop composition search in chunk mode shows nothing until the whole scan ends (28-43 s)**
- **File:** `desktop/gui_threads.py::CompositionThread` (a progress count and one final signal), `genizah_app.py::run_composition`
- **Status:** ❌ Open (2026-04-17; narrowed 2026-10-06)
- **Notes:** **Timed 2026-10-06** on the real index (the real window offscreen, three 150-200-word excerpts of real transcriptions, three independent runs): the window does NOT freeze -- the longest pause of the UI thread was 40-209 ms -- but chunk mode shows no result until the scan ends, 28-43 s. Letter-level, the default once a passage index exists, finished in under 2 s. Reported 2026-04-17 (Phase 75) as a ~15 s freeze before the first result; original text archived 2026-10-06. Fix: stream rows as chunks complete, as `SearchThread.preview_signal` does for Search.

*Triage 2026-10-25:* still open. CompositionThread (gui_threads.py:499-509) still declares only progress_signal, status_signal, perf_signal, pause_ack_signal and the single scan_finished_signal; there is no preview/row signal like SearchThread.preview_signal (:310). No later commit to gui_threads.py adds one.

## GS-236

**Search/indexing of FGP text** · idea · Search · ❌ Open · opened —

Where: `shared/search_engine.py` (no FGP reference); `shared/fgp_service.py`

Moved 2026-10-25 from `4.5 FGP Transcriptions — data ready, integration pending (2026-06-18)`, where it read:

- **Item:** **Search/indexing of FGP text**
- **Status:** ❌ Pending
- **Notes:** Make FGP transcriptions searchable (Tantivy side-index merge, mirror LOCAL RRF). Deferred (FGP-12 guardrail — not touched by the chooser work). See plan.

*Triage 2026-10-25:* still open. grep -i fgp over shared/search_engine.py finds nothing; no FGP index builder or searcher exists in shared/ (only fgp_service.py, which reads fgp_transcriptions.db for the version chooser); docs/plans/FGP_TRANSCRIPTIONS_INTEGRATION_PLAN.md:117 still lists Phase 1 Search/Indexing as not done.

## GS-237

**Starting offline with a saved log-in shows you logged out** · idea · Desktop app · ❌ Open · opened 2026-09-27

Where: `desktop/supabase_corrections_client.py::_load_credentials`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Starting offline with a saved log-in shows you logged out**
- **File:** `desktop/supabase_corrections_client.py::_load_credentials`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** The saved log-in is kept and restored at the next start online. Owner: keep as is; staying logged in offline is a later improvement.

*Triage 2026-10-25:* still open. supabase_corrections_client.py:344-380: current_user is assigned only after client.auth.set_session(...) succeeds; any exception (set_session needs a token refresh when the access token is expired) is swallowed with logger.debug, leaving current_user None. Owner decision recorded in the row: keep as is.

## GS-238

**Right-click exclusions have no list and no undo short of New** · idea · Desktop app · ❌ Open · opened 2026-09-25

Where: `genizah_app.py::_exclude_word_search_result`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Right-click exclusions have no list and no undo short of New**
- **File:** `genizah_app.py::_exclude_word_search_result`
- **Status:** ❌ Open (2026-09-25)
- **Notes:** Since 2026-09-25 they last until New and across restarts; only the "(N excluded)" note shows them.

*Triage 2026-10-25:* still open. _exclude_word_search_result only adds to word_excluded_sys_ids and shows a count; the set is reset only in New (23699); no list or un-exclude UI exists.

## GS-239

**Re-running a composition search from History uses the current Exclude Manuscripts list, not the one it ran with** · idea · Desktop app · ❌ Open · opened 2026-09-25

Where: `genizah_app.py::_add_comp_search_to_history`, `_restore_comp_search_from_state`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Re-running a composition search from History uses the current Exclude Manuscripts list, not the one it ran with**
- **File:** `genizah_app.py::_add_comp_search_to_history`, `_restore_comp_search_from_state`
- **Status:** ❌ Open (sweep 2026-09-25 #30; rechecked 2026-10-06)
- **Notes:** The history entry stores only the text, the domain exclusions and the printed filter; the excluded manuscripts and shelfmarks are not saved, so a re-run applies whatever is excluded now. Regular-search history has the same gap.

*Triage 2026-10-25:* still open. The comp history entry state holds only source_text, domain_exclusions and printed_filter (genizah_app.py:35170-35174); the restore sets only those two (35021-35022). For Search, history now stores the excluded lists but a click deliberately keeps the current list (a337589b, 2026-09-25, comment at 34925-34927), so the behaviour is by design there and unchanged for composition; owner may close as by-design.

## GS-240

**Feature requests from the 2026-09-25 sweep** · idea · Desktop app · ❌ Open · opened 2026-09-25

Where: `genizah_app.py`, `web/pages/lists.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Feature requests from the 2026-09-25 sweep**
- **File:** `genizah_app.py`, `web/pages/lists.py`
- **Status:** ❌ Open (sweep L27, L28)
- **Notes:** (a) The desktop has no print or Save as PDF sheet (Browse Save Manuscript writes a .txt); the website has one. (b) The web Lists page cannot merge lists, clean up duplicates or import a list; the desktop can, and its sync reaches the same account.

*Triage 2026-10-25:* still open. No QPrinter/printToPdf/PDF export exists in genizah_app.py or desktop/ (Browse Save Manuscript writes .txt only, line 11508-11510); web/pages/lists.py and web/components/project_tree.py contain no merge, dedupe or import action, while the desktop has Merge Lists (genizah_app.py:13260) and lists_import_list (14648).

## GS-241

**A paused search does not survive closing the program** · idea · Desktop app · ❌ Open · opened 2026-08-20

Where: `shared/pause_gate.py`, `gui_threads.py::PausableSearchMixin`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A paused search does not survive closing the program**
- **File:** `shared/pause_gate.py`, `gui_threads.py::PausableSearchMixin`
- **Status:** ❌ Open by design (recorded 2026-08-20)
- **Notes:** Pause parks the worker thread between checkpoints; there is no serialised scan cursor, so closing the app ends the run and the next launch starts from the beginning. Reported during v8.6.0 UAT because "Pause/Resume" can reasonably be read as durable. The What's New text and Help.html both say so explicitly. A durable resume needs a stable scan order across calls plus a serialisable position — the engines have neither today, so it is a design phase, not a fix.

*Triage 2026-10-25:* still open. shared/pause_gate.py has no cursor/serialisation and no commit has touched it since v8.6.0 (2026-08-20); PausableSearchMixin still parks the worker thread in memory only. Open by design, as the row says.

## GS-242

**Optional PDF OCR extension for image-only scanned PDFs in My Library (deferred, SEED-003)** · idea · Desktop app · ⏸ Deferred · opened 2026-05-29

Where: `.planning/seeds/SEED-003-optional-ocr-extension.md`, `shared/local_indexer.py` (PDF extraction)

Moved 2026-10-25 from `Deferred to v7.15+ (Phase 95 follow-up backlog)`, where it read:

- **#:** D-F2
- **Item:** ⏸ Deferred — optional extension (seeded 2026-05-29, `SEED-003`) PDF OCR
- **Description:** Scanned-PDF support via Tesseract or similar. Currently PyMuPDF text extraction skips image-only PDFs (no text layer). **Spike 001 corpus profiling found a LARGE share of the real library is image-only scans** (siddurim, Albeck midrash, ערוך, פילון, dictionaries, etc.) → currently unsearchable. BUT per Hillel: OCR is heavy + most users won't need it + off-the-shelf pre-OCR exists, so it must be an **opt-in optional extension**, NOT a core dependency. Deferred to `SEED-003-optional-ocr-extension.md`; surfaces on user demand / OCR milestone.
- **Priority:** P2

*Triage 2026-10-25:* still open. git grep for tesseract/pytesseract in *.py (excluding tests) finds nothing; the seed file is still in .planning/seeds. Deferred by owner choice, no OCR code exists.

## GS-243

**SEED-004 (deferred): show nikud for non-PDF LOCAL formats via a stored content_display field** · idea · Desktop app · ⏸ Deferred · opened 2026-05-29

Where: `shared/local_indexer.py` (_write_page_doc), My Library display

Moved 2026-10-25 from `Deferred to v7.15+ (Phase 95 follow-up backlog)`, where it read:

- **#:** SEED-004
- **Item:** ⏸ Deferred (2026-05-29, Phase 102) Nikud display for non-PDF LOCAL formats
- **Description:** Phase 102 strips nikud from the LOCAL index AND display for ALL formats so un-vocalized search works (D-06 FINAL — strip once at `_write_page_doc`); PDF hides this (its page image shows vowels), but DOCX/TXT/HTML now show consonantal extracted text. If users find consonantal non-PDF display unhelpful, restore nikud display via a stored-only Tantivy `content_display` (nikud) field alongside the stripped `content` (search), read at the two display sites (`genizah_core.py:7166`, `:9631`); cost = larger LOCAL index. Start simple now; revisit only if shown useful. Do NOT mark SEED-004 fixed — it is deferred/future.
- **Priority:** P3

*Triage 2026-10-25:* still open. git grep content_display in shared/local_indexer.py, desktop/ and genizah_app.py returns nothing, so no nikud-bearing display field exists; the row itself says do not mark fixed, it is future work.

## GS-244

**Session restore is not pixel-perfect** · idea · Desktop app · ❌ Open · opened —

Where: genizah_app.py

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Session restore is not pixel-perfect**
- **File:** genizah_app.py
- **Status:** ❌ Open
- **Notes:** v6.5.1 added restore for browse tabs, catalog filters, composition results, and active tab. But composition restores flat (grouping/appendix lost), catalog sidebar doesn't highlight the selected author/work in the list widget, and browse-by-shelfmark skips full resolution (loads directly by sys_id). Could be improved to persist grouping state or re-run grouping more reliably.

*Triage 2026-10-25:* still open. genizah_app.py:35733 still says 'Restore composition results -- flat display (grouping is lost on save)'; the browse restore still loads directly by sys_id, skipping browse_load() resolution (genizah_app.py:35767). Catalog: only the domain tree selection is re-applied and the author/work show as chips (comment at ~35824); no list-widget highlight of the selected author/work (not exhaustively checked).

## GS-245

**Catalog browse table scrolls sideways on a phone** · idea · Website · ❌ Open · opened 2026-10-22

Where: `web/pages/catalog_browse.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Catalog browse table scrolls sideways on a phone**
- **File:** `web/pages/catalog_browse.py`
- **Status:** ❌ Open (2026-10-22)
- **Notes:** Under 768px the five-column table keeps a 640px minimum width and scrolls horizontally (common.css `.catalog-browse-table`); a card list per row would read better. The sidebar already stacks behind a Filters button.

*Triage 2026-10-25:* still open. web/static/common.css:2529-2530 still has overflow-x: auto and table min-width: 640px under the 768px breakpoint; the table is still the five-column q-table.

## GS-246

**perf_watch cannot see a loop saturated by many sub-300 ms slices** · idea · Website · ❌ Open · opened 2026-10-20

Where: `web/perf_watch.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **perf_watch cannot see a loop saturated by many sub-300 ms slices**
- **File:** `web/perf_watch.py`
- **Status:** ❌ Open (2026-10-20)
- **Notes:** A /browse page change is 63-95 ms slices; none trips the threshold. Add a busy fraction (lag over 20 ms / wall time) and a p99 lag to the 300 s summary.

*Triage 2026-10-25:* still open. grep of web/perf_watch.py finds no busy-fraction or p99/percentile code; the 300 s summary (line 376-380) is still get_stats_snapshot(). No commit to the file since 2026-10-20.

## GS-247

**Puzzle BG removal: brown backing page not removed on glued manuscripts** · idea · Website · ❌ Open · opened —

Where: `shared/background_removal.py`, `web/pages/puzzle.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Puzzle BG removal: brown backing page not removed on glued manuscripts**
- **File:** `shared/background_removal.py`, `web/pages/puzzle.py`
- **Status:** ⏳ Deferred
- **Notes:** BL manuscripts have brown backing over blue mat; Oxford has full brown background. Color segmentation alone can't distinguish brown backing from parchment. Planned solution: interactive click-to-remove eraser tool (user clicks background areas, BFS flood fill removes connected region, additive with per-step undo). Requires persistence plumbing (save/load/export eraser steps), auth on endpoint, proper canvas coordinate transforms, Fabric event model integration. Full design: `docs/plans/INTERACTIVE_BG_REMOVAL_DESIGN.md`. Recommend implementing as a full GSD phase.

*Triage 2026-10-25:* still open. No eraser / flood-fill / click-to-remove code exists in shared/, web/ or desktop/ (grep); shared/background_removal.py is still the colour-segmentation module (last meaningful change 2026-03/04). The row's design-doc path is stale: the doc moved to docs/archive/plans/ on 2026-03-26 (e8a452de).

## GS-248

**The validated CanonRarity distinctiveness score is used by no production code, only by offline grading tools** · idea · Discovery · ❌ Open · opened 2026-08-09

Where: `same_work_spike/probe/scripts/canon_rarity.py`

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

❌ **Open (2026-08-09) — a validated distinctiveness score exists and production references it NOWHERE.** `same_work_spike/probe/scripts/canon_rarity.py` (`CanonRarity`, an IDF-over-canonical-works score, `SHARED_TH = 1.5`, validated on a 132-item gold set) is imported only by offline grading tools; `shared/`, `web/` and `scripts/` never reference it. It is the deferred "is this text distinctive?" lever from `HANDOFF-TO-135.md`; `gen2_router.py`'s `span_class='distinctive'` is a different, competition test. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. grep for canon_rarity|CanonRarity across shared/, web/, scripts/, desktop/ and tests/ returns no hits; the only importers are build_validation_cards.py, discovery_score.py and grader.py inside the gitignored spike. No commit has touched same_work_spike since 2026-08-09.

## GS-249

**Pointer to the discovery forward ledger: probe-validated ideas with no current owner** · idea · Discovery · ❌ Open · opened —

Where: `docs/specs/discovery-forward-ledger.md`

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

**Forward ledger: [`docs/specs/discovery-forward-ledger.md`](specs/discovery-forward-ledger.md)** — 10 probe-validated ideas with no current owner (page-chain evidence, residue mining, fragmentary-tail auto-validation, conformal+FDR, motif/units product layer, interleaved refs, reversed-direction finds, confusion-matrix engine assets, per-work quotation profiles, MAPV2-8/9 debts). Review at each 136–139 phase boundary.

*Triage 2026-10-25:* still open. The ledger file exists and still lists the ideas as UNCLAIMED (now 11 entries, the 11th added in c6c77fe8, 2026-07-24); it is a list of ideas, not a defect. Note the bullet's relative link (specs/...) is broken from docs/archive/.

## GS-250

**While lists.pkl cannot be saved, the log-out notice does not name what did not upload** · idea · Accounts and community · ❌ Open · opened 2026-09-27

Where: `genizah_app.py::_logout_notice_paragraphs`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **While lists.pkl cannot be saved, the log-out notice does not name what did not upload**
- **File:** `genizah_app.py::_logout_notice_paragraphs`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** It says only that list changes not in the account are lost if the program closes first, naming neither the lists nor the notes that differ.

*Triage 2026-10-25:* still open. When mgr.saves_failing() the notice is the single generic paragraph 'List changes that did not reach your account ... are lost if you close the program' (2985-2989); the `names` branch is skipped.

## GS-251

**A removal left unsent at log-out gets the general notice, not the list's name** · idea · Accounts and community · ❌ Open · opened 2026-09-27

Where: `genizah_app.py::_logout_notice_paragraphs`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **A removal left unsent at log-out gets the general notice, not the list's name**
- **File:** `genizah_app.py::_logout_notice_paragraphs`
- **Status:** ❌ Open (2026-09-27)
- **Notes:** The row's list on the website is not always the list the entry left, so the notice says "Your latest list changes were not uploaded".

*Triage 2026-10-25:* still open. The named-lists paragraph needs `names and not upload.get('removals_failed', 0)` (2975); when a removal failed the code falls to the generic 'Your latest list changes were not uploaded' text (2980-2982).

## GS-252

**Feature request: comment category and anonymous comments (both apps)** · idea · Accounts and community · ❌ Open · opened 2026-09-25

Where: comments table; `web/components/comment_dialog.py`; `desktop/corrections_ui.py::CommentDialog`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Feature request: comment category and anonymous comments (both apps)**
- **File:** `comments` table (Supabase), `web/components/comment_dialog.py`, `desktop/corrections_ui.py::CommentDialog`, `supabase_corrections_client.py::create_comment`/`_parse_comment`
- **Status:** ❌ Open (rewritten 2026-09-25)
- **Notes:** Since 2026-09-25 the desktop no longer offers the comment Type dropdown or "Post anonymously": neither was stored, and the website always showed the author. Offer them again only with `category` and `is_anonymous` columns (explicit GRANTs, RLS audit) and web-side author masking, in both apps, and split the misnamed `Comment.comment_type` into `scope` and `category`. The 2026-05-12 plan (Phase 87) is in the full text. Full text archived 2026-10-06.

*Triage 2026-10-25:* still open. Neither category nor is_anonymous is stored: desktop/supabase_corrections_client.py:1143-1151 inserts only author_id/sys_id/content/scope/page_number/is_public/parent_id, and _parse_comment (1213-1227) reads scope into comment_type; web/components/comment_dialog.py has no category or anonymous control, and the desktop CommentDialog (corrections_ui.py:2479) has neither the Type dropdown nor Post anonymously.

## GS-253

**Web Joins Lab — no confirm-join / delete-join moderation UI (only self-delete D-03)** · idea · Accounts and community · ❌ Open · opened 2026-06-21

Where: `web/pages/joins_lab.py`, `web/components/candidate_grid.py`, future admin page

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Web Joins Lab — no confirm-join / delete-join moderation UI (only self-delete D-03)**
- **File:** `web/pages/joins_lab.py`, `web/components/candidate_grid.py`, future admin page
- **Status:** ❌ Open (logged 2026-06-21)
- **Notes:** The Joins Lab currently allows authenticated users to `add_as_join` (insert a `fragment_joins` row) and `delete_fragment_join` (delete their own join via D-03 self-delete RLS). There is no UI for: (a) a join requester to propose a join for review before it becomes publicly visible; (b) a moderator to confirm/reject pending joins; (c) any user to flag/report an incorrect join. All joins are immediately public on insert. This is acceptable for the current user base (researchers who self-moderate via the existing corrections/discovery feed), but should be revisited before broader public access. Deferred to a future moderation phase.

*Triage 2026-10-25:* still open. No confirm/reject/flag code exists in web/ or desktop/ (git grep for confirmed_by finds only the status='confirmed' read filter in web/components/joins_panel.py:239). Part of the row is stale: the DB trigger guard_fragment_join_privileges now forces inserts to status 'proposed' and only an admin can confirm, and the Lab's known-joins group reads confirmed-only; but proposed joins still show in the community list (joins_lab.py:2528) and there is still no moderation UI.

## GS-254

**Phase 87 follow-up: enable transcriptions / joins / comments on the 108 image-bearing synthetic manuscripts** · idea · Accounts and community · ❌ Open · opened 2026-05-12

Where: `desktop/corrections_client.py:663`; `desktop/supabase_corrections_client.py:851`; `web/pages/browse.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Phase 87 follow-up: enable transcriptions / joins / comments on the 108 image-bearing synthetic manuscripts**
- **File:** `corrections_client.py`, `supabase_corrections_client.py`, `genizah_app.py`, `web/pages/browse.py` (`is_synthetic_sys_id` gates)
- **Status:** ❌ Open (logged 2026-05-12)
- **Notes:** The 108 CUDL-walked synthetic manuscripts now have a defined `page_number` (the CUDL canvas index), so the Phase 85 block on corrections, joins and comments no longer applies to them. Needed: lift the write rejects and the Edit-button hides for image-bearing synthetic rows only (keep them for FJMS-only rows without images, e.g. via a `has_image_canvases(sys_id)` check), and verify `page_number` round-trips across web, desktop and Supabase. Full text archived 2026-10-06.

*Triage 2026-10-25:* still open. Both desktop write entry points still return 'synthetic_corrections_disabled' for any synthetic sys_id (desktop/corrections_client.py:663-667, desktop/supabase_corrections_client.py:851-855), and there is no has_image_canvases() helper anywhere in the repo; web/pages/browse.py still hides Ktiv/credit links on synthetic ids. (The row's file names are stale: those clients now live in desktop/.)

## GS-255

**Oxford images: no machine-fetchable image source (the Bodleian host is behind an Anubis bot-challenge; NLI refuses Bodleian-held ids)** · ops · Browse and viewer · ❌ Open · opened 2026-09-02

Where: `desktop/image_loader.py:188`, `desktop/viewers.py:1423-1450`, `web/api.py::oxford_image`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Oxford images: no machine-fetchable image source (the Bodleian host is behind an Anubis bot-challenge; NLI refuses Bodleian-held ids)**
- **File:** `desktop/image_loader.py`, `desktop/viewers.py`, `web/api.py::oxford_image`, `web/static/manuscript_viewer.js`, `oxford_full_db.json`
- **Status:** ❌ Open (found 2026-09-02; narrowed 2026-10-05)
- **Notes:** Both apps end at "No Image": `hebrew.bodleian.ox.ac.uk` sits behind an Anubis proof-of-work page (the licence rules out a bypass), NLI answers 500 for Bodleian-held FL ids, and Digital Bodleian IIIF holds 1 of the 225 volumes. Needs a Bodleian allowlist (draft `_tmp/bodleian_allowlist_request_draft.md`) or an owner ruling. Since v9.2.1 both apps link to Bodleian Libraries and the desktop shows a translated notice. Full text archived 2026-10-19.

*Triage 2026-10-25:* still open. desktop/image_loader.py:188 and tests/test_oxford_image_source_fallback.py:4,154 still handle the Anubis challenge page as a failure; viewers.py:1423-1450 only shows translated failure notices. _tmp/bodleian_allowlist_request_draft.md still exists. The block is external (allowlist or owner ruling); no code change since 2026-10-05 alters it.

## GS-256

**Browse/catalog raw HTML is mostly an app shell, with little indexable content before hydration** · ops · Website · ⏳ Needs a check · opened —

Where: `web/pages/browse.py`, `web/pages/catalog_browse.py`

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **Browse/catalog raw HTML is mostly an app shell, with little indexable content before hydration**
- **File:** `web/pages/browse.py`, `web/pages/catalog_browse.py`
- **Status:** ❌ Open · ✋ *By hand:* Search Console URL Inspection of a browse page (owner).
- **Notes:** Head tags (title, meta, canonical, OG) are now correct and manuscript-specific, but body content still loads via WebSocket after initial render. Verify via Search Console URL Inspection whether Google renders enough content before investing in prerendering.

*Triage 2026-10-25:* needs a check. Nothing in browse.py/catalog_browse.py prerenders or adds noscript/server-rendered body content; the row itself defers to a Search Console check. **Would settle it:** Owner runs Search Console URL Inspection on a /browse?sys_id=... page and checks the rendered HTML contains the transcription text.

## GS-257

**SEO follow-up: run Search Console URL Inspection and PageSpeed Insights on production (homepage and a /browse page)** · ops · Website · ⏳ Needs a check · opened —

Where: production (Search Console, PSI)

Moved 2026-10-25 from the tracker's section `7.5 SEO Follow-up (post-7.7.1 deploy)`, a bullet rather than a table row, so it was not triaged by the agents. Its text: run real Search Console URL Inspection + PSI on the production homepage and `/browse?sys_id=X` to get actual Core Web Vitals and crawlability data; per-language URL architecture was deferred. Related: the browse-pages-are-an-app-shell row in this tracker, which needs the same URL Inspection.

The bullet, verbatim:

- **SEO follow-up needed (post-7.7.1 deploy)**: Run real Search Console URL Inspection + PSI on production homepage and `/browse?sys_id=X` to get actual Core Web Vitals + crawlability data. Defer per-language URL architecture (`/he/` vs `/en/` + hreflang) and server-side rendered browse body content to future phases -- both are higher-impact than the title-language debate but require larger refactors.

## GS-258

**The local discovery artifact is V4; production serves V4.2** · ops · Discovery · ❌ Open · opened 2026-10-20

Where: `discovery_data/live/` (staged by the local `.env` for every test run)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **The local discovery artifact is V4; production serves V4.2**
- **File:** `discovery_data/live/` (staged by the local `.env` for every test run)
- **Status:** ❌ Open (found 2026-10-20)
- **Notes:** `live/` holds `528f6d36…` (V4); genizahsearch.com serves `e5bcd347…` (V4.2: its 30,528 / 28,074 counts on 2026-10-20). So local discovery tests, and at least the Help-page check of 2026-10-06 that names `528f6d36`, ran against V4. Restaging is not free: a current artifact can turn tests red that a stale one hid (see the memory note on local discovery preview); keep exactly one `discovery-v1-*.db` in `live/`.

*Triage 2026-10-25:* still open. Directory listing this session: discovery_data/live holds only discovery-v1-528f6d36...db (written 2026-08-15) and a manifest.json naming that hash; no V4.2 file is staged. Production's current hash was not checked (no server access).

## GS-259

**Provenance of the 121 human_confirmed rows is unestablished, so no surface shows a human-review badge** · ops · Discovery · ❌ Open · opened 2026-07-30

Where: `docs/specs/discovery-band-labels-v1.md` (Note 2), `shared/discovery_band_labels.py::review_overlay`

Moved 2026-10-25 from `v9.0.0 Discovery Milestone - open items (parked in docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md)`, where it read:

❌ **Open (2026-07-30) — provenance of the 121 `human_confirmed` rows is unestablished.** Phase 134's closeout left it open (*"internal deck vs owner"*). Source is `e1_adjudicated_a.jsonl` (174 individually-adjudicated cards). Until it is resolved, **no surface displays a human-review badge** (`136-CONTEXT.md` D-13f — owner decision 2026-07-30); if the owner turns out to have graded them, the badge can return with sourced wording. Requires a dated amendment to `discovery-band-labels-v1.md` §2.

*Triage 2026-10-25:* still open. docs/specs/discovery-band-labels-v1.md Note 2 (added 3b576a14, 2026-08-02, the last commit to that spec) still says the provenance is unresolved and the badge stays off; no later amendment exists. review_overlay (shared/discovery_band_labels.py:155) is computed but not rendered by any web surface. Only the owner can say who graded the 174 cards.

## GS-260

**Two sign-ups at the same moment can take usernames that differ only by case** · ops · Accounts and community · ❌ Open · opened 2026-10-21

Where: handle_new_user `(migrations/profiles_no_email_username.sql)`; `supabase_setup.sql`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Two sign-ups at the same moment can take usernames that differ only by case**
- **File:** `handle_new_user` (`migrations/profiles_no_email_username.sql`: a `lower()` existence check before the insert); `profiles.username` is `UNIQUE`, case-sensitive
- **Status:** ❌ Open (2026-10-21, Codex review on #423; needs a database change)
- **Notes:** Under READ COMMITTED both sign-ups pass the check before either commits, so `Alice` and `alice` can both be saved; the window is the few milliseconds between check and commit. Fix (owner-run, after confirming no case duplicates exist: `select lower(username), count(*) from public.profiles where username is not null group by 1 having count(*) > 1`): `create unique index concurrently profiles_username_lower_key on public.profiles (lower(username));` -- the trigger's `unique_violation` fallback then covers it. Add the index to `supabase_setup.sql` in the same change.

*Triage 2026-10-25:* still open. No `lower(username)` unique index in supabase_setup.sql or any migrations/*.sql (grep for username_lower and lower(username) finds only the existence checks inside handle_new_user, supabase_setup.sql:460, :495); the fix is an owner-run SQL change and the repo has not been updated with it. Whether the owner ran it on the live DB is not visible from the repo.

## GS-261

**Supabase DDL gaps** · ops · Accounts and community · ❌ Open · opened 2026-10-21

Where: `supabase_setup.sql`, `migrations/`

Moved 2026-10-25 from `5. Code Quality Debt / Repo structure Round 1 findings (2026-09-18)`, where it read:

- **Issue:** **Supabase DDL gaps**
- **File:** `supabase_setup.sql`, `migrations/`
- **Status:** ❌ Open
- **Notes:** `published_joins`, `published_join_fragments`, `discovery_responses` are written by code and have no DDL anywhere in the repo (`discovery_responses` does not exist in production either: 42P01, 2026-10-21; see the P2 row on discovery replies); `document_sources` is defined only in `migrations/`; no ledger records which migrations have been applied to production.

*Triage 2026-10-25:* still open. git grep over every *.sql finds no CREATE TABLE for published_joins/published_join_fragments/discovery_responses (only scripts/supabase_live_check.sql probes them); published_joins DDL exists only as prose in docs/guides/SUPABASE_GUIDE.md:902-935. document_sources is only in migrations/create_document_sources_table.sql:27; no applied-migrations ledger file exists.

## GS-262

**Supabase hardening follow-ups: anon default grants, SECURITY DEFINER search_path, client-side counters, public votes, review rate-limit key** · ops · Accounts and community · ❌ Partly fixed · opened 2026-10-20

Where: `supabase_setup.sql`; `scripts/create_identification_reviews_beta.sql`; `migrations/*guards.sql`

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Supabase hardening follow-ups (2026-10-20 review; repo SQL, production unverified)**
- **File:** `supabase_setup.sql`, `scripts/create_identification_reviews_beta.sql`, `migrations/add_privileged_column_guards.sql`
- **Status:** ❌ Open (2026-10-20) · 🔑 `scripts/supabase_live_check.sql`
- **Notes:** (a) anon probably keeps the old default INSERT/UPDATE/DELETE grants on every pre-existing table, so RLS is the only barrier: revoke after a clean live check; (b) `handle_new_user` is SECURITY DEFINER with a mutable search_path (pinned 2026-10-21 by `migrations/profiles_no_email_username.sql`, applied live); (c) vote and view counters: maintain them server-side (a SECURITY DEFINER function or a trigger), not from the client; (d) decide whether vote records and `discovery_suppressed.suppressed_by` should be public (owner); (e) key the identification-review rate limit on something the server assigns.

*Triage 2026-10-25:* partly fixed. (b) is done in the repo: migrations/profiles_no_email_username.sql:82 sets search_path = '' on handle_new_user and the migration aborts if it is unpinned (line 205); the row itself says it was applied live 2026-10-21. No commit to the other SQL files since, and the rest needs the live database (scripts/supabase_live_check.sql). **Still open:** (a) revoke anon's default write grants after a clean live check; (c) move vote/view counters server-side; (d) owner decision on public vote records and suppressed_by; (e) server-assigned rate-limit key.

## GS-263

**Line Height returns only with a catalog data release** · ops · Data and sidecars · ❌ Open · opened 2026-10-06

Where: `fist_data/fjms_enrichment.db`, `scripts/import_measurements.py::migrate_add_line_height` (670)

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **Line Height returns only with a catalog data release**
- **File:** `fist_data/fjms_enrichment.db` (`manuscript_measurements` has no `avg_line_height_mm`), `scripts/import_measurements.py::migrate_add_line_height`
- **Status:** ❌ Open (2026-10-06)
- **Notes:** Since 2026-10-06 both apps hide Line Height while the open catalog lacks the column, and show it again by themselves when it has it. To bring it back: run `migrate_add_line_height` on a copy (~157,000 manuscripts gain a value), bump the sidecar's meta version (so installed desktops are offered it), scp it to the server FIRST and restart the web process (the capability is read once), and bundle it in the next installer.

*Triage 2026-10-25:* still open. Read-only PRAGMA on fist_data/fjms_enrichment.db (meta version 5.0.0) lists manuscript_measurements columns without avg_line_height_mm; shared/fjms_service.py:3576-3582 still handles the column's absence, and CHANGELOG.md says Line Height stays hidden until the catalog carries it. Needs the owner's data release.

## GS-264

**FGP shipping: three loose ends** · ops · Data and sidecars · ❌ Open · opened 2026-10-05

Where: `desktop/gui_threads.py:1960` service_map; `DATA_LIFECYCLE.md:119`; `DEPLOYMENT_TECHNICAL.md`

Moved 2026-10-25 from `4.5 FGP Transcriptions — data ready, integration pending (2026-06-18)`, where it read:

- **Item:** **FGP shipping: three loose ends**
- **Status:** ❌ Open (narrowed 2026-10-05) · 🔑 *Server check:* is `fgp_transcriptions.db` on the server, and is the flag not overridden there?
- **Notes:** Phase C shipped in v8.2.0: the flag defaults ON in both apps (`39620cbb`), the DB is in the desktop installer, and per-transcription credits shipped in v8.2.2. Still open: (1) the desktop data updater does not handle `fgp_transcriptions.db` (its `service_map` in `desktop/gui_threads.py` lists only pgp, fjms and nli), so desktop users get new FGP data only from a new installer; (2) there is no web deploy runbook for the DB (scp + atomic rename; `docs/architecture/DATA_LIFECYCLE.md` calls the path undocumented); (3) the FGP-10 licensing/usage sign-off is not recorded. Original entry archived 2026-10-05.

*Triage 2026-10-25:* still open. (1) service_map at desktop/gui_threads.py:1960-1964 still lists only pgp.db, fjms_enrichment.db, nli_crossref.db. (2) DATA_LIFECYCLE.md:115-119 still calls the delivery of fgp_transcriptions.db to the server 'undocumented' and DEPLOYMENT_TECHNICAL.md has no fgp_transcriptions entry. (3) The server check and the FGP-10 sign-off are owner items nothing in the repo records.

## GS-265

**nginx logs and forwards Cloudflare edge addresses until the realip snippet is installed** · ops · Server and operations · ⏳ Needs a check · opened 2026-10-07

Where: `scripts/genizah_cloudflare_realip.conf`, `refresh_cloudflare_realip.py`, server nginx site file

Moved 2026-10-25 from `1. Outstanding Bugs / P3 - Low Priority`, where it read:

- **Issue:** **nginx logs and forwards Cloudflare edge addresses until the realip snippet is installed**
- **File:** `scripts/genizah_cloudflare_realip.conf`, `scripts/refresh_cloudflare_realip.py`, the server's nginx site file
- **Status:** ❌ Open (2026-10-07; owner step)
- **Notes:** Sweep item L8. The access log, `X-Real-IP` and the app's per-IP limiters see `172.70.x` edges, so a heavy client shares a bucket with everyone at its edge and can be found only in the Cloudflare dashboard. The snippet and its refresh script are in the repo; install steps: `docs/guides/DEPLOYMENT_TECHNICAL.md`, "Real visitor addresses behind Cloudflare".

*Triage 2026-10-25:* needs a check. The snippet, refresh script and install steps are in the repo (docs/guides/DEPLOYMENT_TECHNICAL.md:197-345) and web/api_hardening.py:106-117 still says 'only once nginx's realip snippet is installed'. Nothing in the repo records the install on the server. **Would settle it:** On the server: `nginx -T | grep -n real_ip` (include of genizah_cloudflare_realip.conf present) and check that the access log shows visitor addresses, not 172.70.x / 162.158.x edges.

## GS-266

**Slow-request log exists app-side; nginx still lacks $request_time/$upstream_*/CF-Ray in its log format** · ops · Server and operations · ❌ Partly fixed · opened 2026-07-30

Where: `web/perf_watch.py` (new), `web/main.py`, nginx config (NOT yet changed)

Moved 2026-10-25 from `1. Outstanding Bugs / P2 - Medium`, where it read:

- **Issue:** **No server-side latency signal existed at all — a 9-second response left no trace**
- **File:** `web/perf_watch.py` (new), `web/main.py`, nginx config (NOT yet changed)
- **Status:** ⚠️ App-side fixed (2026-07-30); nginx log format still open
- **Notes:** App side fixed 2026-07-30: `web/perf_watch.py` logs every request slower than `GENIZAH_SLOW_REQUEST_MS` (1500) and event-loop lag above `GENIZAH_LOOP_LAG_MS` (300); its attribution was corrected 2026-08-19. Still open: add `$request_time`, `$upstream_connect_time`, `$upstream_header_time`, `$upstream_response_time` and `CF-Ray` to the nginx log format (a production config edit and reload). Full text archived 2026-10-06.

*Triage 2026-10-25:* partly fixed. web/perf_watch.py is wired in web/main.py (SlowRequestTimingMiddleware, start_event_loop_lag_monitor, startup hook at :3524) with GENIZAH_SLOW_REQUEST_MS=1500 / GENIZAH_LOOP_LAG_MS=300 (perf_watch.py:85,89). No log_format/access_log exists anywhere in the repo, so the nginx change is a server-only edit. **Still open:** Add $request_time, $upstream_connect_time, $upstream_header_time, $upstream_response_time and CF-Ray to the nginx log format on the server (owner).

## GS-267

**Neither PostHog drop counter is exposed by an HTTP route, so monitoring them at deploy needs a prod REPL** · ops · Server and operations · ❌ Open · opened —

Where: `web/api_hardening.py:531`, `shared/posthog_server.py:54`

Moved 2026-10-25 from `2026-05-29 Audit Follow-up (fan-out codebase audit + Codex review) / Open (logged 2026-05-29, deferred)`, where it read:

- **Issue:** Neither PostHog drop counter (`web.api_hardening.get_dropped_event_count()` / `shared.posthog_server.get_dropped_event_count()`) is exposed by any HTTP route, so the CLAUDE.md "monitor BOTH at deploy" directive requires a prod REPL
- **File:** `web/api_hardening.py:531`, `shared/posthog_server.py:54`
- **Severity:** P2 Med
- **Notes:** ❌ Open. Surface both on a `/_internal/*` health endpoint next to `/_internal/memstat`.

*Triage 2026-10-25:* still open. git grep shows get_dropped_event_count is defined in both modules (web/api_hardening.py:572, shared/posthog_server.py:88) and read by no route; /_internal/memstat (web/main.py:273-320) returns RSS, storage and runtime-cache stats but no drop counts.

## GS-268

**FGP text drops the part of a line before a gershayim** · P2 · Data and sidecars · ❌ Open · opened 2026-10-25

Where: `fgp_data/fgp_transcriptions.db` (`text_source='pdf_rtl'`), `shared/local_indexer.py::extract_pdf_pages`, `scripts/fgp_rebuild_text_and_credit.py`

The PDF-extracted FGP text loses everything on a line before a `"`: `ועד י"ג שנים ואף על פי שלא` is stored
as `ג שנים ואף על פי שלא`, `הה"ד` as `ד`, and the missing prefix is nowhere in the row. Measured on 805
random FGP images whose own line text exists in FGP's position export
(`C:\FGPTranscriptions_WithPOSInfoAndImages\C<n>\data.js`, matched to the row through FIST
`dbo_UnitTranscription.ImageTranscriptionFolder` → `FileName`, lines aligned with `difflib`): 63 lines
in 23 documents lose a prefix; 6 catchword lines (`%אמור%`) and 2 short lines are absent; 10 long lines
are stored as two; 288 lines have mirrored lacuna brackets and 214 doubled backslashes (`{ה\/\/ב}`).
Gate 3 (`scripts/fgp_gate3_text_pipeline.py`, 2026-10-25) found more on the rows whose source is the
DOCX: final letters left as C0 control characters or symbols (`שלו\x0e ולכ\x04`, `ואיל"` for `ואילך`) or
dropped outright (`דנו` for `דנום`), vocalised letters as control characters, line numbers re-ordered by
the RTL fix (`.1` for `1.`, `-1` for `1-`), mixed-direction lines scrambled (`12.528: TS ראה`), Arabic
alef dropped, Arabic lines reduced to control characters. The same source also explains GS-60 (`&deg;`
entities). Fix sketch: rebuild the text from the structured sources (folder-B line text, else DOCX)
per `docs/plans/FGP_SOURCE_FOLDERS_ASSESSMENT.md` §6.1, gated per changed line (§8.3,
`docs/plans/FGP_TEXT_PIPELINE_RULES.md`), never from the PDF.

## GS-269

**FGP visual XML is applied by image number, not by source record** · P2 · Data and sidecars · ❌ Open · opened 2026-10-25

Where: `scripts/fgp_rebuild_text_and_credit.py::build_xml_index`, `fgp_data/fgp_transcriptions.db` (`text_source='xml'`)

The XML-primary rebuild keys the Talmud-project visual XML by C-number and writes it into every row
of that image, whatever file the row came from. FIST attributes 5 of the 6,381 `xml` rows to a
different team's edition (C192908 row 3717, whose own text reads `למשתה` / `בתר` where the XML reads
`למשתי` / `כתר`) and 3 to the wrong `_G` region of the same photograph (rows 24067, 36404, 36406
carry the G1 region's text); 3,099 team-200 rows have no FIST folder link and are unverified. Those
rows show another edition's text under their own file's credit. Fix sketch: tie XML and line text to
the source record (FIST `ImageTranscriptionFolder` ↔ `FileName`), never to the image; re-attribute the
6,381 rows before any further rebuild (assessment §2, §7).

## GS-270

**FGP credit heuristic disagrees with FIST on 1,551 / 6,151 files** · P2 · Data and sidecars · ❌ Open · opened 2026-10-25

Where: `scripts/fgp_fill_credits_bilingual.py`

Credits derive the team from `image_id[:3]` and the catalogue/book from `image_id[3:7]`, but the
14-digit id is not unique (8,131 ids carry 2–8 files) and FIST `dbo_Signature.SourceId/SubId`
disagrees on 1,551 source prefixes and 6,151 sub-ids of 69,330 files. Concrete: rows 20076
(T-S G2.98) and 22207 (T-S Misc.35.108) share `image_id` 60000010010951 and both show
"תרביץ: רבעון למדעי היהדות" from sub-id `1`, while FIST assigns book sub-ids 3308 and 6413. Fix
sketch: take team / catalogue / book from FIST (`FileName` → `SignatureId` → `SourceId/SubId`) at
build time and keep the team-matched transcriber enrichment the script already adds (assessment
§2, §6.2).

## GS-271

**The tracker scripts' atomic write does not keep a POSIX file's group** · P3 · Build, CI and tests · ❌ Open · opened 2026-10-25

Where: `scripts/open_issues_model.py::write_text`

`write_text` writes a temporary file (`tempfile.mkstemp`) and renames it over the target. Since
PR #449 (Codex round 9) it copies the target's mode, but not its group: on a POSIX checkout shared
through a group other than the invoking user's primary group, a group-owned `0664`
`OPEN_ISSUES.md`, `BACKLOG.md` or `ISSUE_DETAILS.md` comes back owned by the user's default
group after `collect_open_issues.py --apply` or `archive_closed_issues.py --apply`, and the other
members can no longer update it. No such checkout exists today: the scripts run on the owner's
Windows machine and in CI. Fix sketch: `os.chown(tmp, -1, os.stat(path).st_gid)` before the
rename, raising rather than writing when that is not permitted. Found by the Codex review of
PR #449 (round 10) and logged instead of fixed under that PR's review budget.

## GS-272

**FIST's RecordStatus 1 / 5 and PermissionCommentCode 5309 have no ruling before FGP new rows ship** · P3 · Data and sidecars · ❌ Partly fixed · opened 2026-10-26

Where: `fist_data/FIST.db` `dbo_Signature.RecordStatus`, `PermissionCommentCode`; `scripts/fgp_gate2_rules.py`, `scripts/fgp_gate4_chooser_validation.py`

The 2026-10-26 check of drafts in the FGP delta (`_tmp/fgp_delta_drafts_report.md`, section 4) found three FIST
signals the gates do not read. The first, the unit's free-text `Comment` saying `לא נבדק מול המקור` ("not checked
against the source") or `אין לפרסם עד לקריאה מול מקור` ("do not publish until read against the source"), was ruled
the same day: gate 2 holds the units whose comment is the explicit do-not-publish instruction
(`fist_comment_no_publish`, 17 of today's rows, team 130; the 150 "not checked" rows are a quality note and stay
publishable, the owner's narrowing later that day) and gate 4 applies the same rule to new rows. Two remain
without a ruling: `dbo_Signature.RecordStatus` is 0 on 40,415 of today's
40,416 units but 1 or 5 on 6,586 of the delta's units, and the FIST export carries no code table for it; and
`PermissionCommentCode` 5309 ("being worked on by a team, data temporarily inaccessible") covers 156 of today's
units (team 135, 180 rows publishable) and 8 of the delta's. Fix sketch: the owner asks FGP what `RecordStatus` 1 /
5 and code 5309 mean; a gate-2 hold for whichever means "not for publication", applied in gate 4's candidate build
before the `plain` new-rows policy ships.

Partly fixed the same evening: `RecordStatus` 5 is a superseded version (a newer status-0 signature exists for the
same unit), held by gate 4's R2f (`5d82aeb5`); `RecordStatus` 1 was ruled by the owner after checking examples on
FJMS — transcriptions ship, translations are held, R2g (`3bbebf04`). Still without a ruling: code 5309 (180 of
today's rows; no new row carries it).

## GS-273

**The reading desk saves a source choice it never applies, keyed by list position** · P3 · Website · ❌ Open · opened 2026-10-26

Where: `web/pages/browse.py` (reading-desk version `ui.select`, `state.reading_desk_selected_sources`), `web/pages/browse_state.py`

The reading desk's per-fragment version dropdown stores the chosen option under
`state.reading_desk_selected_sources[sys_id]` and the browse snapshot persists it as `selected_sources`, but
the dropdown is always built with `value=default_key` (first PGP edition, else first translation, else V0.8):
the saved value is restored into state on `enter_joined_view` and never read again, so a choice does not
survive a page rebuild or a session. The key is also the option's list position (`src_<i>`), which changes
whenever the fragment's PGP or FGP source list changes (a sidecar refresh adds or removes rows; gate 4,
`docs/plans/FGP_CHOOSER_VALIDATION_RULES.md` §5, measured 282 manuscripts whose FGP list order changes with the
larger sidecar). Found while validating the chooser against the larger FGP sidecar (gate 4 of
`docs/plans/FGP_SOURCE_FOLDERS_ASSESSMENT.md`), whose check C3 keeps `fgp:<id>` stable for exactly this reason.
Fix sketch: key the option by the source's stable id (`fgp:<id>` / `pgp:<id>`, `v08`), pass the saved key as the
select's initial value when it is still among the options, and fall back to the default otherwise.

## GS-274

**FGP's transcription marks (`?א?`, `@…@`, `¶`, …) are explained by a legend but not drawn** · idea · Browse and viewer · ❌ Open · opened 2026-10-26

Where: web reading view (`web/pages/browse.py`), desktop viewer; `shared/fgp_service.py`, `shared/fgp_marks_legend.py`

FGP's transcribers mark what they see with symbols around the text. The Genuzos team's legend (an image-only
PDF the owner holds, `Genuzos SiglaA.pdf`) reads `?..?` doubtful, `[..]` reconstructed, `(..)` deleted,
`((..))` erased, `{..}` corrected, `^..^` interlinear, `@..@` piyyut, `%..%` catchword, `*..*` heading,
`&..&` Judeo-Arabic, `##` dots above; `¶` is not on it and is unconfirmed. FJMS draws these marks; both
our apps show the text as typed. Today about 10% of FGP rows carry them; in the source-folders candidate
the corrected rows keep that rate, but of the new rows 68% carry doubtful-letter marks, 37% `@..@` and
36% `¶` (mostly the Genuzos team's convention). The owner ruled on 2026-10-26 to ship the release with the
marks as typed, and to follow up with a legend link beside the text, or by drawing the common marks.
Fix sketch: a link needs a public address for the legend (none known; the PDF is local only). Drawing
needs a rule in both readers, limited to Hebrew transcriptions (`?` is a real question mark in English
translations), and a decision on whether copied and cited text keeps the marks.

Assessed 2026-10-26 (mock: `_tmp/fgp_sigla_options.html`, untracked). The marks are the Genuzot team's: of its
11,870 new rows 84% carry `?x?`, 48% `@`, 45% `¶`; other teams carry none of these. Two team legends exist, both
local only (no public copy found; FGP's sites sit behind the FJMS sign-in): the 2024 `סיגלה אקדמית` PDF (23
marks) and a 2014 Word table, `סיגלה עיונית וסיגלה חזותית`, of the Talmud work (27 numbered marks with their
software codes, the same letters as the visual XML's `c` classes). They disagree on `[…]` (2014: between the
lines / margin; 2024: torn or blurred). Not explained by either: `@` around one to three letters inside a word
(5,078 + 933 occurrences in new rows, beside 10,893 piyyut spans), `¶` (2014 has a different glyph for a
non-standard paragraph end), `\`, `[[…]]`, `[|…|]`, `§…§` (2014: an explicitly marked variant reading; the
liturgical rows use it for cue words), runs of `??`. Every mark inside a word also defeats search-hit
highlighting in both apps (`browse.py::highlight_text`, `desktop/widgets::mark_pattern_hits`), and on the
desktop `*…*` headings collide with the hit markers. Found alongside: the desktop drops every `<…>` mark (its
own row, from `docs/open_issues.d/2026-10-26-desktop-fgp-pgp-angle-marks-swallowed.md`).

Option A built 2026-10-27 (owner: "A now and B later"): a "?" beside a Genuzot transcription opens the legend, in
both apps, from one shared table (`shared/fgp_marks_legend.py::legend_html`; web `web/components/version_selector.py`,
desktop `desktop/marks_legend.py`). It follows the 2024 list in our own words, shows the three 2014 meanings apart,
and lists the marks neither list explains; the text itself is unchanged. The wording awaits the owner's approval
(`_tmp/fgp_marks_legend_review.html`, untracked). What is left here is option B, drawing the confirmed marks, which
waits on FGP's answer for the unexplained marks (`@` around letters, `¶`, `[[…]]`, …) and on a rule for copy and
citation.

## GS-275

**FGP and PGP editions lose every `<…>` mark on the desktop** · P2 · Desktop app · ❌ Open · opened 2026-10-26

Where: `genizah_app.py::_browse_display_pgp_text`, `desktop/result_dialog.py::_rd_display_pgp_text`

Both functions show a PGP edition/translation or an FGP transcription by replacing `\n` with `<br>` and passing the
text to `setHtml` (through `apply_line_numbered_text`) without escaping it, so Qt's rich-text parser takes every
`<…>` in the text for a tag and drops it with its contents. Measured 2026-10-26 with the same calls in an offscreen
probe: `דמנח אפתורא <ד'>` shows as `דמנח אפתורא`, `<ב>`, `<(ה)>` and `<~>` vanish, and `<<רא>>` leaves a stray `>`.
In FGP's transcriptions `<…>` is the Genuzot team's line-filler / catchword mark (`<רא>`, `<ראובן>`): 9,729 of
today's FGP rows and 2,942 of the source-folders candidate's new rows carry one. In PGP, 315 of 36,642 document
transcriptions (and 382 source / 408 footnote texts) use `<…>` for an editor's added letters (`מיב<מ>נא`,
`ו<י>טול`), which the desktop silently removes from the word. Entities decode as a side effect (`&deg;` shows as
`°`, 4 FGP rows). The website escapes (`web/components/typography.py::render_line_numbered_html`) and shows them.
Fix sketch: `html.escape` the text before the `\n` → `<br>` step in both functions (the search-hit `*…*` markers
and `markers_to_bold_html` are unaffected by escaping), first checking that no PGP text relies on real markup
reaching Qt; a test that renders `<ד'>` and `מיב<מ>נא` through both paths and reads them back from the widget.
Found while assessing how to show FGP's transcription marks (GS-274).
