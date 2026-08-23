# Codebase Index

> Last updated: 2026-08-19

Auto-generated index of classes and methods. New sections for modules can be
appended via `python scripts/gen_code_index_section.py <file.py> ...` (walks
Python AST and emits markdown in the existing style).

**Phase 77 (Serializer & JSON Export) added** `shared/search_serializer.py` —
single source of truth for the Claude-friendly JSON payload shape consumed by
the new `/api/export/json` and `/api/export/parallels/json` download handlers
(and Phase 78+ `/api/search` / `/api/parallels`). See its dedicated section
below alongside the other `shared/` service modules.

**v7.9 decomposition (Phases 67–74) added the following modules** — see the
"v7.9 Decomposed Modules" section below for their class / function indexes:

- Desktop: `desktop/widgets.py`, `desktop/title_helpers.py`,
  `desktop/image_loader.py`, `desktop/result_dialog.py`,
  `desktop/dialogs_filter.py`, `desktop/dialogs_scholarly.py`,
  `desktop/viewers.py`, `desktop/puzzle.py`, `desktop/vs_cache.py`
- Web: `web/pages/search_state.py`, `web/pages/search_results.py`,
  `web/pages/browse_state.py`, `web/pages/browse_enrichment.py`,
  `web/search_bootstrap.py`

(The `## genizah_app.py` and `## genizah_core.py` sections were since refreshed by
the v8.3.0 pass below; `web/pages/search.py` / `web/pages/browse.py` are large and
not separately indexed here — grep them directly.)

**v8.3.0 God-File Decomposition (Phases 122-127) refreshed the `genizah_core.py`
and `genizah_app.py` sections below and added new `shared/*` + `desktop/*` sections.**
`genizah_core.py` is now a thin (~755-line) re-export facade — search, metadata,
variants, codicological, Responsa, joins/lists managers, browse-map utils, text
normalization, the indexer, and the SearchEngine/LabEngine/LabSettings classes now
live in `shared/*.py` (see the "v8.3.0 Decomposed Modules" section). Desktop dialog/
widget/update-UI classes moved to `desktop/settings_dialogs.py`, `desktop/ui_widgets.py`,
and `desktop/update_ui.py`. `genizah_core.X is shared.Y.X` and `genizah_app.X is
desktop.Y.X` identity holds via the facades.

## genizah_app.py

- **Function** `space_scroll_action` (Line 118) — Pure decision for desktop results-table Space-scroll.
- **Function** `paused_seconds` (Line 137) — Seconds spent parked, INCLUDING a pause that is still in progress.
- **Function** `effective_elapsed` (Line 153) — Working seconds elapsed: monotonic span minus parked time, never negative.
- **Class** `_PauseCtx` (Line 170) — Pause UI state for ONE operation (the search tab, or the composition tab).
    - Method `__init__` (Line 182)
    - Method `reset_for_run` (Line 192)
    - Method `elapsed` (Line 201) — Working-time elapsed for this operation, excluding parked time.
    - Method `accepts_ack` (Line 208) — Whether a queued pause acknowledgement belongs to the live pause.
- **Function** `_aggregate_local_pages_with_separators` (Line 229) — Phase 96 NEW-2 D-14: aggregate page texts with labeled separators.
- **Function** `_setup_crash_handler` (Line 265)
- **Class** `LabPanel` (Line 301)
    - Method `__init__` (Line 302)
    - Method `set_engine` (Line 315)
    - Method `enable_controls` (Line 322)
    - Method `init_ui` (Line 325)
    - Method `refresh_values` (Line 434)
    - Method `on_change` (Line 450)
    - Method `open_scoring` (Line 465)
    - Method `_mark_rebuild_required` (Line 470)
    - Method `run_rebuild` (Line 478)
    - Method `on_rebuild_progress` (Line 510)
    - Method `on_rebuild_error` (Line 513)
    - Method `on_rebuild_finished` (Line 518)
- **Function** `log_tls_relaxation_notice` (Line 529) — Log once that TLS verification is intentionally disabled for thumbnail fetches.
- **Function** `_get_catalog_filter_sets` (Line 547) — Return ``(pgp_link_sys_ids, edition_sys_ids)`` (both sets).
- **Function** `reset_catalog_filter_sets` (Line 573) — Invalidate the cached catalog availability sets so the next filtered query
- **Class** `_CatalogRefreshWorker` (Line 581) — Background worker for catalog browse DB queries (authors/works/results).
    - Method `__init__` (Line 590)
    - Method `run` (Line 619)
- **Class** `_CatalogFacetWorker` (Line 666) — Background worker that computes per-library facet counts for the catalog dialog.
    - Method `__init__` (Line 680)
    - Method `run` (Line 699)
- **Function** `_format_list_star` (Line 731)
- **Function** `_build_search_results_xlsx_bytes` (Line 747) — Build the 4-sheet workbook bytes for desktop xlsx search-results export.
- **Function** `_local_page_label` (Line 1163) — D-02 page label for a LOCAL result. ``chunk_locator`` is used VERBATIM
- **Function** `_build_export_data_row` (Line 1177) — Build one 7-column data_rows entry for export_results.
- **Function** `_csv_extra_cols` (Line 1215) — Return the extra (Filepath, Page) pair for LOCAL rows in a mixed CSV.
- **Function** `_local_parent_folder` (Line 1232) — Parent-folder name from a LOCAL filepath, separator-agnostic.
- **Function** `_format_txt_local_block` (Line 1246) — Return the TXT block string for a LOCAL result (Phase 103 D-09).
- **Function** `_format_txt_genizah_block` (Line 1285) — Return the TXT block string for a Genizah result.
- **Function** `_telemetry_result_bucket` (Line 1310) — Coarse result-count bucket for Phase 114 telemetry (D-07/D-08).
- **Class** `GenizahGUI` (Line 1325) — Main application window orchestrating search, browsing, and indexing.
    - Method `__init__` (Line 1329)
    - Method `start_background_init` (Line 1500)
    - Method `on_startup_finished` (Line 1509)
    - Method `_sync_telemetry_identity` (Line 1632) — Reconcile PostHog identity against the live Supabase session.
    - Method `_run_startup_telemetry_coordinator` (Line 1657) — Single boot/opt-in sequence: consent → identity-sync → session_start.
    - Method `_telemetry_ready` (Line 1716) — Producer gate: True only after the coordinator's session_start branch ran.
    - Method `_emit_feature_opened` (Line 1728) — Centralized desktop_feature_opened producer (D-03 / D-04 / REVIEWS MEDIUM-9).
    - Method `_setup_active_ping` (Line 1761) — Wire daily active-user heartbeat (D-16 / USAGE-04).
    - Method `_on_app_state_changed` (Line 1784) — Focus/resume handler — fires heartbeat on app activation.
    - Method `_maybe_emit_active_ping` (Line 1804) — Emit desktop_active_ping at most once per UTC day, active-only (USAGE-04 / D-16).
    - Method `_maybe_flush_perf_summary` (Line 1841) — Periodically flush the per-session perf accumulator (Phase 115 D-04/D-05/KQ-4).
    - Method `_check_shelfmark_completer_ready` (Line 1871)
    - Method `setup_shelfmark_completer` (Line 1879) — Initialize the shelfmark autocomplete with data from csv_bank and Parts.
    - Method `init_ui` (Line 1935)
    - Method `_update_corner_login_state` (Line 2109) — Update the corner login button based on login state.
    - Method `_set_active_tab` (Line 2120) — Set the active tab programmatically without emitting telemetry.
    - Method `_on_tab_changed` (Line 2137) — Handle tab change events.
    - Method `_corner_login_clicked` (Line 2184) — Handle corner login button click.
    - Method `_show_login_dialog` (Line 2191)
    - Method `_show_register_dialog` (Line 2204)
    - Method `_do_logout` (Line 2215)
    - Method `_enable_lists_cloud_sync` (Line 2229) — Enable cloud sync for user lists after login - shows sync dialog.
    - Method `_show_lists_sync_dialog` (Line 2308) — Show dialog to let user choose how to sync lists.
    - Method `_do_sync_action` (Line 2403) — Execute the chosen sync action.
    - Method `_disable_lists_cloud_sync` (Line 2474) — Disable cloud sync on logout.
    - Method `_show_discoveries_dialog` (Line 2498)
    - Method `_show_create_discovery_dialog` (Line 2506)
    - Method `_show_all_corrections_dialog` (Line 2531)
    - Method `_show_my_corrections_dialog` (Line 2535)
    - Method `_show_my_comments_dialog` (Line 2542)
    - Method `_browse_toggle_edit_mode` (Line 2555) — Toggle edit mode for inline corrections.
    - Method `_browse_on_text_changed` (Line 2586) — Handle text changes in edit mode.
    - Method `_browse_cancel_edit` (Line 2618) — Cancel edit mode and restore original text.
    - Method `_browse_exit_edit_mode` (Line 2637) — Exit edit mode without restoring text (after successful submit).
    - Method `_browse_save_correction` (Line 2652) — Save the inline correction.
    - Method `_browse_change_version` (Line 2795) — Change between text versions.
    - Method `_browse_load_version` (Line 2811) — Load and display a specific version.
    - Method `_browse_display_version_text` (Line 2903) — Display version text in the browse text area.
    - Method `_displayed_folio_label_for_pgp` (Line 2920) — Folio label ('1r','2v',…) of the image at the current browse page.
    - Method `_displayed_page_for_pgp` (Line 2940) — (_browse_folio_images, 1-based displayed page, total pages).
    - Method `_displayed_fgp_image_number_for_pgp` (Line 2957) — FGP image number (fgp_image_number_id) of the image at the current page.
    - Method `_populate_pgp_combo` (Line 2971) — Build combo items with PGP editions and translations grouped.
    - Method `_browse_full_htr_text` (Line 3125) — Whole-manuscript HTR (V0.8) text — all folios concatenated — for the
    - Method `_auto_select_pgp_edition` (Line 3142) — Find the first edition item and set it as current.
    - Method `_check_document_community_status` (Line 3201) — Check if document has comments and load available versions.
    - Method `_browse_add_comment` (Line 3376) — Open comment dialog for current document.
    - Method `_browse_view_corrections` (Line 3401) — View corrections for current document.
    - Method `_browse_view_comments` (Line 3424) — View comments for current document.
    - Method `_browse_view_joins` (Line 3445) — View joined fragments for current document.
    - Method `_browse_view_visual_similarity` (Line 3488) — REROUTED (Phase 109, D-10): open the Join Workbench with the Visual source auto-loaded.
    - Method `_enrich_vs_suggestions` (Line 3513) — Enrich raw VS suggestions with shelfmark, library_code, domain from csv_bank/fjms.
    - Method `_on_vs_fetch_complete` (Line 3538)
    - Method `_show_vs_dialog` (Line 3548) — Create and show the enriched Visual Similarity workbench dialog.
    - Method `_vs_navigate_to` (Line 4002) — Navigate browse to a VS partner manuscript.
    - Method `_vs_open_joins_with_partner` (Line 4022) — Open JoinsDialog with fragment A (original) and fragment B (partner) pre-filled.
    - Method `_vs_add_to_puzzle` (Line 4044) — Add a VS partner to the Fragment Puzzle.
    - Method `_vs_get_partners` (Line 4057) — Get VS partner sys_ids from local DB, cache, or server (synchronous).
    - Method `_search_in_visual_suggestions` (Line 4084) — Restrict search to visual similarity partner pool ('Search in VS' action).
    - Method `_browse_visual_suggestions` (Line 4118) — Show VS partner pool as a result set by running a wildcard search restricted to partners.
    - Method `_clear_vs_restriction` (Line 4148) — Clear the visual similarity search restriction.
    - Method `_update_vs_breadcrumb` (Line 4157) — Show or hide the VS restriction breadcrumb in the search area.
    - Method `_update_joins_dropdown` (Line 4186) — Update the joins dropdown menu with connected fragments.
    - Method `_on_joins_menu_show` (Line 4415) — Called when joins menu is about to show - trigger sync and update.
    - Method `_navigate_to_joined_fragment` (Line 4426) — Navigate to a joined fragment in browse tab.
    - Method `_show_results_context_menu` (Line 4434) — Show context menu for search results with community options.
    - Method `_context_view_document` (Line 4559) — Navigate to browse tab for this document.
    - Method `_context_submit_correction` (Line 4566) — Open correction dialog from context menu.
    - Method `_context_add_comment` (Line 4578) — Open comment dialog from context menu.
    - Method `_context_view_corrections` (Line 4590) — View corrections from context menu.
    - Method `_context_view_comments` (Line 4601) — View comments from context menu.
    - Method `_context_share_discovery` (Line 4610) — Share discovery from context menu.
    - Method `toggle_language` (Line 4625)
    - Method `create_search_tab` (Line 4657)
    - Method `set_results_loading` (Line 5297) — Toggle the search results placeholder while components initialize.
    - Method `create_composition_tab` (Line 5303)
    - Method `create_browse_tab` (Line 5715)
    - Method `browse_toggle_lists_panel` (Line 6324) — Toggle the browse lists side panel.
    - Method `browse_set_lists_panel_visible` (Line 6328) — Show or hide the browse lists side panel.
    - Method `browse_refresh_lists_panel` (Line 6363) — Refresh the lists tree and items list in the browse panel.
    - Method `browse_on_list_selected` (Line 6400) — Handle selection of a list in the browse lists panel.
    - Method `browse_on_list_item_clicked` (Line 6450) — Open a list item in the browse tab using FL/Image ID lookup.
    - Method `_browse_append_printed_badge` (Line 6495) — Append printed material badge to browse info label text if applicable.
    - Method `_start_browse_enrichment` (Line 6515) — Centralized enrichment launch — disconnects stale worker, bumps generation counter,
    - Method `_build_nli_iiif_url_for_page` (Line 6577) — Build a direct NLI IIIF URL for (sys_id, page_idx).
    - Method `_is_cambridge_display` (Line 6644) — Return True iff display_meta looks like a CUDL-Cambridge source.
    - Method `_resolve_cambridge_page_or_fallback` (Line 6666) — Compute (display_meta, idx) for a Cambridge CUDL page using
    - Method `_switch_browse_viewer_to_nli_for_page` (Line 6742) — Flip the browse viewer to NLI and jump to a positional page.
    - Method `_restore_browse_viewer_to_ext` (Line 6790) — Restore the browse viewer to CUDL after an auto-fallback.
    - Method `_resolve_cambridge_navigation_index` (Line 6831) — Side-aware index lookup for prev/next navigation on CUDL.
    - Method `on_browse_enriched_loaded` (Line 6883)
    - Method `_on_browse_pgp_loaded` (Line 7254) — Handle PGP sources loaded from background thread.
    - Method `_on_browse_pgp_error` (Line 7337) — Handle PGP source fetch error -- silently fall back to existing behavior.
    - Method `_build_pgp_extended_info_html` (Line 7341) — Build HTML for PGP metadata section in extended info panels.
    - Method `_build_fjms_domain_html` (Line 7439) — Build HTML for FJMS domain classifications in extended info.
    - Method `_build_fjms_catalog_html` (Line 7482) — Build HTML for FJMS catalog metadata in extended info.
    - Method `_build_catalog_refs_html` (Line 7568) — Build HTML for FIST catalog cross-references in extended info.
    - Method `_build_secondary_metadata_html` (Line 7589) — Build HTML for secondary metadata (source names, collection, storage).
    - Method `_build_browse_enriched_html` (Line 7629) — Build HTML for KTI/Oxford/Cambridge enrichment data in Browse extended info.
    - Method `_browse_toggle_extended_info` (Line 7851) — Toggle browse tab extended info panel visibility.
    - Method `_show_fjms_bibliography_dialog` (Line 7858) — Open the FJMS bibliography dialog.
    - Method `_show_nli_bibliography_dialog` (Line 7871) — Open the NLI bibliography dialog.
    - Method `_show_fjms_catalog_dialog` (Line 7884) — Open the FJMS catalog records dialog from Browse tab (lazy fetch).
    - Method `_show_browse_measurements_dialog` (Line 7913) — Open measurements dialog from Browse tab (lazy fetch on first click).
    - Method `_on_browse_ext_link_clicked` (Line 7936) — Handle clicks on links in browse tab extended info.
    - Method `_search_toggle_translations` (Line 7966) — Toggle show_translations from search tab toolbar button.
    - Method `_refresh_search_titles` (Line 7986) — Refresh title column in search results to reflect translation state.
    - Method `_browse_toggle_translations` (Line 7999) — Toggle show_translations from browse tab toolbar button.
    - Method `_refresh_browse_title` (Line 8022) — Refresh the browse info label to reflect current show_translations state.
    - Method `_handle_toggle_trans` (Line 8033) — Toggle translated/original text by rebuilding the PGP HTML section.
    - Method `_refresh_browse_extended_info` (Line 8053) — Refresh the browse extended info panel (after toggling show_translations).
    - Method `_start_field_translation` (Line 8097) — Start an on-demand field translation via Dicta API.
    - Method `_get_field_original_text` (Line 8144) — Extract the original English text for a field key from stored metadata.
    - Method `_on_field_translated` (Line 8188) — Handle completed field translation — refresh the relevant panel.
    - Method `_navigate_to_catalog_browse` (Line 8214) — Navigate to the catalog browse tab with the specified filter pre-set.
    - Method `_apply_pending_catalog_nav` (Line 8252) — Apply a pending domain selection after async tree load completes.
    - Method `_catalog_select_domain_in_tree` (Line 8267) — Select a domain in the catalog browse domain tree by its English key.
    - Method `_browse_display_pgp_text` (Line 8285) — Display PGP edition/translation text with proper directionality.
    - Method `_browse_refresh_pgp_for_page` (Line 8302) — Re-fetch PGP/FGP sources for current page (called on page change within same manuscript).
    - Method `_on_browse_link_clicked` (Line 8330) — Handle clicks on internal links in browse text (View All and Reading Desk modes).
    - Method `_browse_enter_reading_desk` (Line 8386) — Enter reading desk mode with the given fragments.
    - Method `_browse_rd_enrich_entry` (Line 8494) — Ensure meta_mgr.nli_cache[sys_id] has image metadata for the reading desk.
    - Method `_browse_rd_on_sources_loaded` (Line 8555) — Handle PGP sources loaded from ReadingDeskWorker.
    - Method `_browse_exit_reading_desk` (Line 8571) — Exit reading desk mode and restore normal browse view.
    - Method `_browse_add_to_view` (Line 8626) — Handle 'Add to View' button click -- enter reading desk or add a manuscript.
    - Method `_browse_add_to_puzzle` (Line 8698) — Add current browse manuscript to the puzzle canvas.
    - Method `_browse_open_join_workbench` (Line 8719) — Browse tab entry point for the Join Workbench. D-03 #2.
    - Method `_browse_rd_add_entry` (Line 8740) — Add a single manuscript entry to the reading desk (duplicate-safe).
    - Method `_browse_rd_add_by_shelfmark` (Line 8805) — Add a manuscript to the reading desk by shelfmark (toolbar input).
    - Method `_browse_rd_add_from_list` (Line 8853) — Show the browse lists panel so items can be added to reading desk.
    - Method `_browse_open_joins_in_reading_desk` (Line 8857) — Open all joined fragments in the reading desk.
    - Method `_browse_open_pgp_joins_in_reading_desk` (Line 8941) — Open PGP multi-fragment joined document in reading desk.
    - Method `_browse_rd_render` (Line 8968) — Render reading desk: stacked texts in text pane, stacked images in viewer pane.
    - Method `_browse_rd_render_images` (Line 9097) — Render stacked images in the viewer pane (right side of browse splitter).
    - Method `_browse_rd_disconnect_sync` (Line 9236) — Disconnect sync scroll handlers without affecting other signal connections.
    - Method `_browse_rd_setup_sync_scroll` (Line 9253) — Set up proportional scroll synchronization between text and image panes.
    - Method `_browse_rd_restore_normal_view` (Line 9301) — Hide reading desk image scroll and restore normal viewer.
    - Method `_browse_rd_remove_entry` (Line 9313) — Remove a fragment entry from the reading desk and re-render or exit.
    - Method `_browse_rd_show_version_dialog` (Line 9323) — Show a dialog to select PGP version source for a specific fragment.
    - Method `toggle_browse_view_all` (Line 9380)
    - Method `on_browse_page_combo_changed` (Line 9396)
    - Method `_on_browse_volume_changed` (Line 9413) — Handle volume selector change — switch to a different IE's pages and images.
    - Method `_refresh_browse_images_for_volume` (Line 9430) — Launch lightweight manifest-only worker for volume switch (no full enrichment).
    - Method `_on_volume_manifest_loaded` (Line 9456) — Handle volume manifest fetch result — update image viewer for the active volume.
    - Method `toggle_browse_image` (Line 9544)
    - Method `_set_browse_image_pane_visible` (Line 9548) — Phase 95 D-27 helper — programmatic equivalent of toggle_browse_image.
    - Method `browse_search_parallels` (Line 9560)
    - Method `browse_add_to_list` (Line 9593) — Add current manuscript to a list.
    - Method `_set_last_browse_field` (Line 9607)
    - Method `browse_load_page` (Line 9610) — Load single page text and sync viewer.
    - Method `_apply_browse_highlights` (Line 9623)
    - Method `browse_load_all` (Line 9642) — Load all pages into the text browser for continuous scrolling.
    - Method `browse_save_full` (Line 9820)
    - Method `create_catalog_browse_tab` (Line 9878) — Create the 'Browse by Identification' tab with domain tree, author/work search, and results table.
    - Method `_catalog_refresh` (Line 10235) — Main refresh: re-fetch results with current filters + pagination, update UI.
    - Method `_catalog_update_text_summary` (Line 10348) — Update the human-readable text filter summary label.
    - Method `_catalog_refresh_authors` (Line 10366) — Fetch authors scoped to current domain, update author list widget.
    - Method `_catalog_filter_authors` (Line 10375) — Filter author list widget based on current text input.
    - Method `_catalog_refresh_works` (Line 10399) — Fetch works scoped to current domain + author, update works list widget.
    - Method `_catalog_filter_works` (Line 10411) — Filter works list widget based on current text input.
    - Method `_catalog_on_domain_select` (Line 10437) — Handle domain tree item click.
    - Method `_catalog_start_async_refresh` (Line 10450) — Run catalog browse refresh in a background thread (never blocks UI).
    - Method `_catalog_on_async_refresh_done` (Line 10486) — Handle results from background refresh thread.
    - Method `_catalog_on_author_select` (Line 10568) — Handle author list item click.
    - Method `_catalog_on_work_select` (Line 10581) — Handle work list item click.
    - Method `_catalog_on_date_changed` (Line 10592) — Handle date From/To input change.
    - Method `_catalog_on_undated_changed` (Line 10607) — Handle include-undated checkbox toggle.
    - Method `_catalog_set_century` (Line 10614) — Set date range to a single century and refresh.
    - Method `_catalog_set_century_range` (Line 10621) — Set date range spanning multiple centuries and refresh.
    - Method `_catalog_clear_date` (Line 10628) — Clear date filter state and UI.
    - Method `_catalog_add_text_term` (Line 10637) — Add the current text input as a filter term with the selected mode.
    - Method `_catalog_remove_text_term` (Line 10655) — Remove a text filter term and refresh.
    - Method `_catalog_render_text_chips` (Line 10668) — Re-render the inline text filter chips below the input in the sidebar.
    - Method `_catalog_update_avail_filter_btns` (Line 10690) — Set the PGP / scholarly-transcription filter button labels + colors
    - Method `_catalog_cycle_pgp_filter` (Line 10714) — Cycle the PGP availability filter: all -> has_pgp -> no_pgp -> all.
    - Method `_catalog_cycle_editions_filter` (Line 10722) — Cycle the scholarly-transcription filter: all -> has_edition -> no_edition -> all.
    - Method `_open_catalog_library_dialog` (Line 10730) — Open LibraryFilterDialog (GAP-G) and apply the selection (dual-mode, DMF-07).
    - Method `_catalog_update_library_filter_btn` (Line 10792) — Update the library filter button label + colour to reflect the selection.
    - Method `_catalog_remove_filter` (Line 10836) — Remove a specific filter (or all) and refresh.
    - Method `_resolve_catalog_author_display` (Line 10909) — Resolve author value to display name from cached authors list.
    - Method `_resolve_catalog_work_display` (Line 10919) — Resolve work value to display name from cached works list.
    - Method `_catalog_update_chips` (Line 10929) — Update the active filter chips bar.
    - Method `_catalog_build_browse_filters` (Line 11049) — Build pre_search_filters dict from all active catalog browse filters.
    - Method `_catalog_search_in_results` (Line 11070) — Navigate to search tab with browse filters as pre-search filters.
    - Method `_catalog_parallels_in_results` (Line 11114) — Navigate to composition tab with browse filters as pre-search filters.
    - Method `_catalog_view_result` (Line 11156) — Double-click result row: open ResultDialog with prev/next navigation.
    - Method `_catalog_view_result_by_row` (Line 11160) — Open ResultDialog for the catalog browse result at the given row.
    - Method `_catalog_browse_manuscript_by_row` (Line 11188) — Navigate to Browse by Shelfmark tab for the given row.
    - Method `_catalog_on_cell_entered` (Line 11199) — Handle mouse hover on catalog results table rows for action button visibility.
    - Method `_catalog_next_page` (Line 11217) — Go to next page of results.
    - Method `_catalog_prev_page` (Line 11222) — Go to previous page of results.
    - Method `_catalog_populate_tree` (Line 11230) — Start async population of the domain tree. Never blocks main thread.
    - Method `_catalog_load_tree_from_cache` (Line 11247) — Load tree data from already-cached service (runs on main thread, instant).
    - Method `_catalog_render_tree` (Line 11263) — Render tree from pre-fetched data (runs on main thread via signal).
    - Method `create_lists_tab` (Line 11322) — Create the Personal Lists tab for managing starred manuscripts.
    - Method `lists_toggle_preview` (Line 11633) — Toggle the preview panel visibility.
    - Method `_normalize_fl_id` (Line 11637)
    - Method `_format_image_display` (Line 11641)
    - Method `_get_list_display_name` (Line 11644)
    - Method `_get_list_display_color` (Line 11652)
    - Method `lists_set_preview_visible` (Line 11662) — Show/hide preview panel with a slim collapsed bar.
    - Method `lists_refresh_all` (Line 11711) — Refresh the lists sidebar and current items view.
    - Method `_lists_auto_sync` (Line 11721) — Auto-sync to cloud after local changes (if logged in).
    - Method `lists_refresh_sidebar` (Line 11788) — Refresh the lists tree in the sidebar.
    - Method `lists_handle_tree_reorder` (Line 11876) — Apply drag-and-drop changes to list/project order and assignment.
    - Method `lists_refresh_items` (Line 11914) — Refresh the items table for the current list.
    - Method `_get_recent_items_deduped` (Line 12036) — Return Recently Viewed items in view order, with true duplicates collapsed.
    - Method `lists_on_list_selected` (Line 12070) — Handle list selection in the sidebar.
    - Method `lists_on_item_clicked` (Line 12080) — Handle item click in the table.
    - Method `lists_on_item_checkbox_changed` (Line 12093) — Handle checkbox state change.
    - Method `lists_update_selection_label` (Line 12099) — Update the selection count label.
    - Method `lists_on_select_all_toggled` (Line 12113) — Toggle all checkboxes in the list table.
    - Method `lists_sync_select_all_checkbox` (Line 12127) — Sync 'Select All' checkbox state with row selections.
    - Method `lists_get_selected_item_ids` (Line 12151) — Get list of selected item ids.
    - Method `lists_show_item_details` (Line 12162) — Show details for a specific item.
    - Method `_lists_load_preview` (Line 12238) — Load text and image preview for an item.
    - Method `_lists_load_preview_image` (Line 12272) — Load image for preview panel.
    - Method `_lists_start_preview_download` (Line 12293) — Download and display preview image for lists panel.
    - Method `_lists_on_preview_image_loaded` (Line 12313) — Handle preview image loaded for lists panel.
    - Method `_lists_on_preview_image_failed` (Line 12326) — Handle preview image load failure for lists panel.
    - Method `_lists_cancel_preview_image_thread` (Line 12333)
    - Method `lists_clear_details` (Line 12339) — Clear the details panel and preview.
    - Method `lists_save_item_details` (Line 12365) — Save changes to the current item.
    - Method `lists_create_new_list` (Line 12373) — Create a new list.
    - Method `lists_create_new_project` (Line 12382) — Create a new project.
    - Method `lists_edit_current_list` (Line 12391) — Edit the current list name/color.
    - Method `lists_delete_current_list` (Line 12406) — Delete the current list.
    - Method `lists_duplicate_selected_list` (Line 12428) — Duplicate the current list.
    - Method `lists_merge_lists` (Line 12437) — Show dialog to merge lists.
    - Method `lists_cleanup_duplicates` (Line 12472) — Clean up duplicate lists created by sync bugs.
    - Method `_show_duplicate_conflict_dialog` (Line 12523) — Show dialog for user to resolve a duplicate list conflict.
    - Method `lists_show_trash` (Line 12580) — Show dialog with deleted lists (trash).
    - Method `_trash_restore` (Line 12642) — Restore selected list from trash.
    - Method `_trash_delete_permanently` (Line 12657) — Permanently delete selected list from trash.
    - Method `_trash_empty` (Line 12678) — Empty all trash.
    - Method `lists_move_selected_items` (Line 12693) — Move selected items to another list.
    - Method `lists_add_tag_to_selected` (Line 12722) — Add a tag to selected items.
    - Method `lists_add_tag_to_item` (Line 12736) — Add a tag to a specific item.
    - Method `lists_remove_selected_items` (Line 12747) — Remove selected items from current list.
    - Method `lists_remove_item_by_id` (Line 12767) — Remove a specific item from current list.
    - Method `lists_quick_view_item` (Line 12775) — Quick view the current item.
    - Method `lists_quick_view_by_id` (Line 12780) — Open quick view dialog for an item.
    - Method `lists_browse_item` (Line 12824) — Browse the current item in the Browse tab.
    - Method `lists_browse_by_id` (Line 12829) — Open an item in the Browse tab.
    - Method `_open_document_result_dialog` (Line 12845) — Open ResultDialog for a document by shelfmark or sys_id.
    - Method `_browse_document_by_shelfmark` (Line 12899) — Browse a document by shelfmark in the Browse tab.
    - Method `lists_copy_item_info` (Line 12906) — Copy current item info to clipboard.
    - Method `_lists_add_to_puzzle` (Line 12911) — Add a list item to the puzzle canvas.
    - Method `lists_copy_info_by_id` (Line 12924) — Copy item info to clipboard with format options.
    - Method `_do_copy_info` (Line 12942) — Actually copy the info to clipboard.
    - Method `lists_export_current_list` (Line 12981) — Export the current list.
    - Method `lists_import_list` (Line 13005) — Import a list from file.
    - Method `lists_show_list_context_menu` (Line 13038) — Show context menu for list items in sidebar.
    - Method `_rename_list` (Line 13151) — Rename a specific list.
    - Method `_delete_list` (Line 13166) — Delete a specific list.
    - Method `_duplicate_list` (Line 13189) — Duplicate a specific list.
    - Method `_export_list` (Line 13198) — Export a specific list (opens format menu).
    - Method `_export_list_format` (Line 13202) — Export a specific list in the given format.
    - Method `_format_item_text` (Line 13236) — Format a single item for text export.
    - Method `_export_as_text` (Line 13263) — Export list as plain text.
    - Method `_export_as_json` (Line 13280) — Export list as JSON.
    - Method `_export_as_excel` (Line 13302) — Export list as Excel file.
    - Method `_export_as_word` (Line 13345) — Export list as Word document.
    - Method `_save_text_to_file` (Line 13396) — Save text to file.
    - Method `_copy_to_clipboard` (Line 13410) — Copy text to clipboard.
    - Method `_send_by_email` (Line 13416) — Robust Email: Copies text to clipboard and opens empty email draft.
    - Method `lists_apply_filter` (Line 13438) — Apply filter to items table.
    - Method `show_add_to_list_menu` (Line 13444) — Show menu for adding items to a list.
    - Method `create_community_tab` (Line 13511) — Create the Community tab with panels for discoveries, corrections, and comments.
    - Method `_refresh_community_panels` (Line 13736) — Refresh all community panels and update UI state.
    - Method `_update_community_header` (Line 13784) — Update the community header with user info.
    - Method `_refresh_discoveries_panel` (Line 13796) — Refresh the discoveries list panel.
    - Method `_filter_discoveries` (Line 13853) — Filter discoveries list by selected type.
    - Method `_populate_discoveries_list` (Line 13881) — Populate discoveries list from data.
    - Method `_refresh_corrections_panel` (Line 13923) — Refresh the corrections list panels.
    - Method `_populate_my_corrections_list` (Line 13970) — Populate my corrections list from data (only latest per document).
    - Method `_populate_all_corrections_list` (Line 14005) — Populate all corrections list from data (only latest per user per document).
    - Method `_refresh_comments_panel` (Line 14041) — Refresh the comments list panels (My Comments + All Comments).
    - Method `_populate_comments_list` (Line 14098) — Populate comments list from data.
    - Method `_discoveries_context_menu` (Line 14140) — Show context menu for discoveries list.
    - Method `_edit_discovery_from_list` (Line 14190) — Open edit dialog for discovery from context menu.
    - Method `_delete_discovery_from_list` (Line 14196) — Delete discovery from context menu.
    - Method `_toggle_pin_discovery` (Line 14212) — Toggle pin status from context menu.
    - Method `_toggle_hide_discovery` (Line 14220) — Toggle hide status from context menu.
    - Method `_corrections_context_menu` (Line 14231) — Show context menu for corrections list.
    - Method `_show_correction_details` (Line 14254) — Show correction details dialog.
    - Method `_comments_context_menu` (Line 14277) — Show context menu for comments list.
    - Method `_on_discovery_clicked` (Line 14295) — Handle discovery item double-click.
    - Method `_on_correction_clicked` (Line 14302) — Handle correction item double-click - open ResultDialog.
    - Method `_on_comment_clicked` (Line 14317) — Handle comment item double-click - open ResultDialog.
    - Method `_refresh_joins_panel` (Line 14329) — Refresh the joins list panels (My Joins + All Joins).
    - Method `_populate_joins_list` (Line 14418) — Populate joins list from data.
    - Method `_populate_puzzles_list` (Line 14451) — Populate puzzle list from published joins data.
    - Method `_on_puzzle_clicked` (Line 14474) — Handle double-click on a published puzzle — fork and open.
    - Method `_joins_context_menu` (Line 14488) — Show context menu for joins list.
    - Method `_on_join_clicked` (Line 14533) — Handle join item double-click - open Fragment A.
    - Method `_open_join_fragment` (Line 14541) — Open a fragment from a join - navigate to browse tab.
    - Method `_copy_join_shelfmarks` (Line 14552) — Copy join shelfmarks to clipboard.
    - Method `_delete_join_from_list` (Line 14558) — Delete a join from the community panel.
    - Method `_show_joins_feed_dialog` (Line 14577) — Show the full joins feed dialog.
    - Method `_open_puzzle_window` (Line 14590) — Open the puzzle canvas window (or bring existing one to front).
    - Method `add_to_puzzle` (Line 14600) — Add a fragment to the puzzle canvas. Opens puzzle window if needed.
    - Method `open_join_workbench` (Line 14635) — Open the Join Lab (no anchor required). Restores last session state if available.
    - Method `open_joins_workbench` (Line 14677) — Open (or re-anchor) the Join Workbench. D-01 modeless; single reusable instance.
    - Method `open_anchor_in_puzzle` (Line 14719) — Public: add a fragment to the Fragment Puzzle canvas (Join Workbench path). SC#5.
    - Method `open_anchors_in_puzzle` (Line 14723) — Public: add multiple fragments to the Fragment Puzzle canvas.
    - Method `open_anchor_as_join` (Line 14737) — Public: open JoinsDialog with anchor as Fragment A; scholar enters B freely.
    - Method `_open_settings_dialog` (Line 14779) — Open the settings dialog.
    - Method `apply_settings` (Line 14798) — OK path for the Settings dialog (DESK-01 thin named API / SP-4 boundary).
    - Method `cancel_settings` (Line 14807) — Cancel path for the Settings dialog (DESK-01 thin named API / SP-4).
    - Method `_on_language_combo_changed` (Line 14815) — Handle language combo box change — close settings dialog first to
    - Method `_create_citation_bar` (Line 14823) — Create the persistent citation bar at the bottom of the main window.
    - Method `copy_citation` (Line 14869)
    - Method `_show_citation_reminder` (Line 14874) — Show a one-time citation reminder dialog on first launch.
    - Method `_maybe_show_first_run_prompt` (Line 14907) — Gate for the one-time first-run consent dialog.
    - Method `_maybe_show_telemetry_reask` (Line 14951) — Startup gate for the non-modal telemetry re-ask bar. Never blocks.
    - Method `on_telemetry_reask_enable` (Line 14971) — User clicked Enable on the re-ask bar — the sole implicit opt-in path.
    - Method `on_telemetry_reask_learn_more` (Line 14986) — Open the full bilingual privacy disclosure (same as first-run Learn more).
    - Method `on_telemetry_reask_never` (Line 14994) — User clicked Don't ask again — persist a hard, permanent opt-out.
    - Method `on_telemetry_reask_dismissed` (Line 15003) — User dismissed the bar with ✕ — ignored this time (count already recorded).
    - Method `open_help_center` (Line 15011) — Open the bundled Help.html with optional anchor scrolling and fallback content.
    - Method `get_search_help_text` (Line 15024)
    - Method `get_comp_help_text` (Line 15028)
    - Method `get_browse_help_text` (Line 15032)
    - Method `get_settings_help_text` (Line 15036)
    - Method `_build_help_fallback_html` (Line 15040)
    - Method `_sanitize_filename` (Line 15061)
    - Method `_get_default_save_folder` (Line 15066) — Get the default folder for saving reports. Checks last used location first.
    - Method `_get_unique_filepath` (Line 15107) — If file exists, add (1), (2), etc. until we find a unique name.
    - Method `_save_last_folder` (Line 15120) — Remember the folder where user saved a file.
    - Method `_default_report_path` (Line 15126)
    - Method `_get_credit_header` (Line 15139)
    - Method `_show_export_saved_dialog` (Line 15160) — EXPUX-01: 'export complete' dialog with Open File / Open Folder.
    - Method `_get_lab_config_block` (Line 15192)
    - Method `open_search_settings` (Line 15206) — Open the Search Settings dialog for variant configuration.
    - Method `_on_search_mode_changed` (Line 15219) — Show/hide variant controls and swap query/tag input based on selected mode.
    - Method `_on_comp_mode_changed` (Line 15242) — Show/hide variant slider for composition based on selected mode.
    - Method `_on_boundary_mode_changed` (Line 15249) — Update UI based on boundary mode selection.
    - Method `_on_boundary_delimiter_changed` (Line 15274) — Save delimiter setting and update stats when delimiter changes.
    - Method `_update_boundary_stats` (Line 15286) — Update the boundary statistics label based on current text and settings.
    - Method `_open_boundary_advanced_dialog` (Line 15326) — Open dialog for advanced boundary search settings.
    - Method `_set_variant_preset` (Line 15383) — Set variant level from preset button.
    - Method `_get_current_variant_pairs_count` (Line 15407) — Get the current variant pairs count (from preset or slider).
    - Method `_sync_variant_sliders` (Line 15416) — Keep variant sliders synchronized between search and composition tabs.
    - Method `_on_query_text_changed` (Line 15442) — Handle live text changes: detect shortcut prefixes and update variant preview.
    - Method `_update_variant_count_preview` (Line 15465) — Update the variant count label based on current query and slider value.
    - Method `update_lab_ui_state` (Line 15506) — Disable standard controls when Lab Mode is active.
    - Method `on_deep_scan_toggled_search` (Line 15518)
    - Method `on_deep_scan_toggled_comp` (Line 15524)
    - Method `on_lab_mode_toggled_search` (Line 15530)
    - Method `on_lab_mode_toggled_comp` (Line 15546)
    - Method `_open_domain_filter_dialog` (Line 15566) — Open the domain filter dialog for post-search dynamic filtering.
    - Method `_update_domain_filter_label` (Line 15588) — Update the domain filter label badge to show exclusion state.
    - Method `_domain_display_name` (Line 15603) — Get display name for a domain (Hebrew if UI is Hebrew, else English).
    - Method `_apply_domain_exclusions` (Line 15613) — Apply domain exclusions by hiding/showing table rows.
    - Method `_open_measurement_filter_dialog` (Line 15656) — Open a dialog to filter results by physical measurements.
    - Method `_open_pre_search_filter_dialog` (Line 15753) — Open the pre-search filter dialog.
    - Method `_update_filter_chip_bar` (Line 15769) — Update both search and composition chip bars to reflect active filters.
    - Method `_add_filter_chip` (Line 15891) — Add a removable chip button to the filter chip bar layout.
    - Method `_remove_filter` (Line 15905) — Remove a single filter and recompute restrict_sys_ids.
    - Method `_on_filter_recompute_finished` (Line 15940) — Handle recomputed filter set after chip removal.
    - Method `_on_restore_filter_finished` (Line 15953) — Handle filter recompute after session/history restore.
    - Method `_exclude_word_search_result` (Line 15968) — Exclude a single manuscript from word search results.
    - Method `_on_domain_enrichment_loaded` (Line 15984) — Handle async domain enrichment results from DomainEnrichmentWorker.
    - Method `_navigate_to_search_with_domain` (Line 16045) — Navigate to search tab with domain context (exclusions cleared).
    - Method `_collect_comp_domain_data` (Line 16054) — Collect domain data for composition results.
    - Method `_open_comp_domain_filter_dialog` (Line 16139) — Open the domain filter dialog for composition results.
    - Method `_update_comp_domain_filter_label` (Line 16168) — Update the composition domain filter label.
    - Method `_apply_comp_domain_exclusions` (Line 16183) — Apply domain exclusions by hiding/showing composition tree items.
    - Method `_open_query_builder` (Line 16214) — Open the tabular query builder dialog.
    - Method `_on_corpus_scope_changed` (Line 16244) — Phase 95 smoke-fix (item 2): persist the corpus scope selection via session JSON.
    - Method `_on_comp_corpus_scope_changed` (Line 16250) — Phase 110 (COMP-LOC-01): persist the composition corpus scope. Mirrors
    - Method `_drain_previous_worker` (Line 16263) — Refuse to rebind a worker slot while the old worker is still alive.
    - Method `toggle_search` (Line 16292)
    - Method `start_search` (Line 16301)
    - Method `_emit_search_telemetry` (Line 16476) — Emit desktop_search_executed for a regular search run (Phase 114 USAGE-03).
    - Method `_on_perf_signal` (Line 16512) — UI-thread slot for all four search thread perf_signal emissions (Phase 115 PERF-01).
    - Method `_pause_worker_for` (Line 16543) — The live worker a pause context is tracking, or None.
    - Method `_apply_pause_state` (Line 16549) — Single owner of every Pause/Resume widget mutation.
    - Method `_pause_elapsed_str` (Line 16599)
    - Method `_paint_pause_status` (Line 16603) — Write the paused/pausing wording onto whichever surface owns this run.
    - Method `_on_pause_clicked` (Line 16620) — Pause <-> Resume. The click owns 'pausing'/'running'; only an accepted
    - Method `_on_pause_ack` (Line 16657) — Queued acknowledgement from a worker that has actually parked.
    - Method `_on_search_phase` (Line 16672) — A search entered a new phase whose progress is not comparable to the last.
    - Method `stop_search` (Line 16693)
    - Method `reset_ui` (Line 16710)
    - Method `_update_search_elapsed` (Line 16720) — Tick every 1s to keep elapsed time updating during search.
    - Method `on_error` (Line 16736)
    - Method `_reset_search` (Line 16738) — Clear all search state and start fresh.
    - Method `_on_search_progress` (Line 16851)
    - Method `render_asterisks_to_html` (Line 16862)
    - Method `check_scroll_load` (Line 16867)
    - Method `load_next_batch` (Line 16872)
    - Method `_notify_search_complete` (Line 17086) — Flash taskbar icon if app is not focused when search completes.
    - Method `on_search_finished` (Line 17123)
    - Method `_replay_refinement_chain` (Line 17316) — D-13: Re-execute chain to rebuild restrict sets. Shows 'Re-evaluating...' feedback.
    - Method `_replay_for_restore` (Line 17334) — Replay the refinement chain during session restore, OFF the UI thread.
    - Method `_on_replay_for_restore_finished` (Line 17358) — Apply the restrict set rebuilt by the off-thread chain replay.
    - Method `_on_replay_for_restore_error` (Line 17368) — Replay failed -- clear the chain rather than leave stale state.
    - Method `_enter_refine_mode` (Line 17378) — D-02, D-03: Activate refine mode on desktop search bar.
    - Method `_exit_refine_mode` (Line 17421) — D-02a: Cancel refine mode without search.
    - Method `_update_refinement_strip` (Line 17427) — D-04, D-05, D-06, D-07, D-10: Rebuild breadcrumb chip widgets.
    - Method `_remove_refinement_step` (Line 17511) — D-12: Remove chip at index and all subsequent, re-execute with feedback.
    - Method `_toggle_all_terms_filter` (Line 17519) — Toggle 'Only results with all terms' post-filter and re-render results.
    - Method `_apply_all_terms_filter_and_rerender` (Line 17525) — Re-render results table applying the all-terms filter.
    - Method `_clear_refinement_chain` (Line 17551) — D-11: Remove entire chain, return to unrestricted search.
    - Method `_update_search_within_btn` (Line 17567) — D-01: Show/hide search within button based on result availability.
    - Method `_undo_zero_result_refine` (Line 17580) — D-14a: Recover from zero-result refinement -- replay chain to restore previous results.
    - Method `_launch_enrichment_workers` (Line 17594) — Launch domain, PGP badge, printed badge, and measurement enrichment workers.
    - Method `_open_results_filter_dialog` (Line 17669)
    - Method `_update_results_filter_indicators` (Line 17714)
    - Method `_results_filter_text_for_row` (Line 17718)
    - Method `_apply_local_filter` (Line 17734) — Apply LOCAL three-state filter per D-10 / D-10 P1.
    - Method `_apply_local_optout_filter` (Line 17759) — Phase 96 D-F1: drop LOCAL hits whose canonical filepath is in
    - Method `_reapply_filters_for_optout_change` (Line 17799) — Phase 96 D-F1: re-run both cascade joinpoints after the user
    - Method `_local_filter_state_index` (Line 17824) — Return the index of ``value`` in ``states``, or 0 if unknown.
    - Method `_text_position_from_index` (Line 17837) — Map a combo index to its text-position option, or None if out of range.
    - Method `_toggle_local_filter_search` (Line 17847) — Cycle the LOCAL filter state for the Search surface (D-10 / D-39).
    - Method `_toggle_local_filter_composition` (Line 17856) — Cycle the LOCAL filter state for the Composition surface (D-10 / D-39).
    - Method `_toggle_local_filter_parallels` (Line 17865) — Cycle the LOCAL filter state for the Parallels surface (D-10 / D-39).
    - Method `_update_local_filter_btn_search` (Line 17874) — Update label on the Search surface LOCAL filter button.
    - Method `_update_local_filter_btn_composition` (Line 17886) — Update label on the Composition surface LOCAL filter button.
    - Method `_update_local_filter_btn_parallels` (Line 17898) — Update label on the Parallels surface LOCAL filter button.
    - Method `_update_local_filter_visibility_search` (Line 17910) — Show/hide the Search LOCAL filter button based on LOCAL hits presence.
    - Method `_update_local_filter_visibility_comp` (Line 17922) — Show/hide the Composition LOCAL filter button based on LOCAL hits presence.
    - Method `_update_local_filter_visibility_parallels` (Line 17930) — Show/hide the Parallels LOCAL filter button based on LOCAL hits presence.
    - Method `_show_local_filter_chip` (Line 17938) — Show or hide the no-op chip for the given surface.
    - Method `_apply_results_table_filters` (Line 17951)
    - Method `_on_pgp_badges_loaded` (Line 18094) — Handle PGP badge worker results - update the PGP + scholarly-transcription
    - Method `_on_printed_badges_loaded` (Line 18119) — Handle Printed badge worker results - update Printed column for all rows.
    - Method `_on_pgp_tags_loaded` (Line 18135) — Handle PGP tags worker results - populate tag dropdown with categorized Hebrew translations.
    - Method `_emit_pgp_tag_search_telemetry` (Line 18157) — Emit desktop_search_executed for a PGP-tags search run (Phase 114 USAGE-03).
    - Method `_execute_tag_search` (Line 18201) — Execute a search by PGP tag from the dropdown.
    - Method `_on_tag_search_results` (Line 18245) — Handle tag search results - display in results table.
    - Method `_search_by_pgp_tag` (Line 18347) — Entry point for searching by PGP tag (from browse/result dialog links).
    - Method `_open_comp_filter_dialog` (Line 18361)
    - Method `_update_comp_filter_indicators` (Line 18399)
    - Method `_apply_comp_tree_filters` (Line 18407)
    - Method `_comp_data_matches_filters` (Line 18483)
    - Method `_text_matches_filter` (Line 18525)
    - Method `start_metadata_loading` (Line 18535)
    - Method `on_meta_progress` (Line 18592)
    - Method `on_meta_finished` (Line 18632)
    - Method `_format_metadata_status` (Line 18641)
    - Method `_create_action_button` (Line 18649)
    - Method `_is_item_in_non_recent_list` (Line 18666)
    - Method `_set_add_to_list_button_label` (Line 18673)
    - Method `_update_browse_add_to_list_button` (Line 18678)
    - Method `_update_search_row_list_indicator` (Line 18688)
    - Method `_update_search_action_stars` (Line 18710)
    - Method `on_table_cell_entered` (Line 18716)
    - Method `on_lists_table_cell_entered` (Line 18733)
    - Method `eventFilter` (Line 18749)
    - Method `_collect_sorted_results` (Line 18833)
    - Method `_extract_fl_id` (Line 18848)
    - Method `show_full_text` (Line 18878)
    - Method `show_full_text_for_result` (Line 18899)
    - Method `open_result_in_browse_from_table` (Line 18928)
    - Method `on_search_select_all_toggled` (Line 18944) — Handle Select All checkbox toggle (skips hidden/excluded rows).
    - Method `on_search_result_item_changed` (Line 18957) — Handle individual checkbox changes in search results.
    - Method `_update_search_export_label` (Line 18989)
    - Method `search_add_selected_to_list` (Line 19010) — Add selected search results to a list.
    - Method `search_add_row_to_list` (Line 19037) — Add a single search result row to a list.
    - Method `_collect_selected_comp_pages` (Line 19053)
    - Method `comp_add_selected_to_list` (Line 19084) — Add selected composition results to a list.
    - Method `open_result_in_browse` (Line 19109)
    - Method `_lookup_local_filepath` (Line 19192) — Phase 95 D-28 — look up the canonical filepath for a LOCAL sys_id.
    - Method `_prime_local_filepath_cache` (Line 19217) — v7.16 BUG-6: batch-load canonical filepaths for all LOCAL hits in
    - Method `_get_local_pages_for_sys_id` (Line 19239) — Return sorted [(p_num, text), ...] for all indexed pages of a LOCAL sys_id.
    - Method `_get_local_full_text_for_sys_id` (Line 19286) — Category 3: aggregate all pages of a LOCAL sys_id into a single text.
    - Method `_open_local_browse` (Line 19311) — Phase 95 D-27 + Phase 96 NEW-2 view-mode dispatch.
    - Method `_render_view_all_batch` (Line 19517) — Phase 97 U-04 — re-render the accumulated page list via apply_line_numbered_text.
    - Method `_append_next_view_all_batch` (Line 19556) — Phase 97 U-04 — schedule the next 50-page batch via QTimer.singleShot(0, ...).
    - Method `_on_browse_open_file_clicked` (Line 19574) — Phase 95 D-28 — launch OS default app for the current LOCAL file.
    - Method `_on_browse_open_file_location_clicked` (Line 19585) — v7.16: reveal the current LOCAL file in the OS file manager.
    - Method `_is_browsing_local` (Line 19597) — Return True when the Browse panel currently shows a LOCAL file.
    - Method `_browse_prev_next` (Line 19608) — Unified prev/next handler for Browse panel.
    - Method `_show_local_browse_controls` (Line 19619) — Phase 96 NEW-2: update Browse-panel controls for LOCAL vs Genizah mode.
    - Method `_open_local_browse_page` (Line 19652) — Phase 96 NEW-2: render ONE LOCAL page at a time in the Browse panel.
    - Method `_on_local_browse_nav` (Line 19837) — Phase 96 NEW-2: prev/next click handler for LOCAL Browse nav.
    - Method `_toggle_local_browse_view_mode` (Line 19874) — Phase 96 NEW-2: flip View-All ↔ Per-Page and re-render.
    - Method `send_result_to_composition` (Line 19899)
    - Method `_sanitize_for_excel` (Line 19931) — Cleans text to prevent Excel XML corruption.
    - Method `_add_docx_highlighted_runs` (Line 19938)
    - Method `_set_paragraph_rtl` (Line 19949)
    - Method `_set_table_rtl` (Line 19962)
    - Method `_set_table_width_pct` (Line 19972)
    - Method `export_results` (Line 19983) — Export results handling specific formats directly.
    - Method `export_comp_report` (Line 20406)
    - Method `open_filter_dialog` (Line 21442)
    - Method `_get_filter_text` (Line 21481) — Get combined filter text from enabled sources.
    - Method `_update_list_filter_cache` (Line 21489) — Cache the set of system IDs for the currently selected lists to optimize filtering.
    - Method `open_list_filter_dialog` (Line 21510)
    - Method `toggle_list_filter` (Line 21528)
    - Method `load_comp_file` (Line 21547)
    - Method `open_exclude_dialog` (Line 21552)
    - Method `set_excluded_entries` (Line 21577)
    - Method `_normalize_shelfmark` (Line 21598) — Normalize shelfmarks using the canonical function from genizah_core.
    - Method `_rerender_with_exclusions` (Line 21602) — Hide/show table rows based on current exclusion state (Approach C).
    - Method `_update_exclusion_display` (Line 21630) — Update exclusion status labels with per-source breakdown (D-07).
    - Method `_remove_exclusion_source` (Line 21648) — Remove a single exclusion source by source_id (D-06 per-source clear).
    - Method `_ensure_shelf_map` (Line 21656) — Build a mapping from normalized shelfmark to sys_id for quick lookups.
    - Method `_get_meta_for_header` (Line 21678) — Return (sys_id, p_num, shelfmark, title) preferring metadata bank for shelfmarks.
    - Method `_comp_item_is_local` (Line 21696) — Phase 110 UAT (Issue 1): True iff a composition item is a LOCAL hit.
    - Method `_comp_local_display_fields` (Line 21722) — Phase 110 UAT (Issue 1): compute (shelfmark, library_display) for a
    - Method `_prime_comp_local_filepath_cache` (Line 21754) — Phase 110 UAT (Issue 1): batch-prime _local_filepath_cache for the
    - Method `_item_matches_exclusion` (Line 21791)
    - Method `_apply_manual_exclusions` (Line 21817)
    - Method `toggle_composition` (Line 21842)
    - Method `cancel_composition` (Line 21871) — Cancel composition search gracefully (called by Escape shortcut).
    - Method `reset_comp_ui` (Line 21879)
    - Method `_reset_composition` (Line 21887) — Clear all composition search state and start fresh.
    - Method `_emit_comp_search_telemetry` (Line 22003) — Emit desktop_search_executed for a composition search run (Phase 114 USAGE-03).
    - Method `run_composition` (Line 22038) — Main entry point for Composition Search.
    - Method `on_comp_display_mode_changed` (Line 22236)
    - Method `run_recursive_composition` (Line 22255)
    - Method `on_comp_status_update` (Line 22289)
    - Method `on_comp_progress` (Line 22294)
    - Method `on_comp_error` (Line 22324) — Handle errors during composition search.
    - Method `on_comp_scan_finished` (Line 22329)
    - Method `start_grouping` (Line 22420)
    - Method `on_grouping_error` (Line 22448)
    - Method `on_comp_finished` (Line 22460)
    - Method `_collect_comp_items` (Line 22501)
    - Method `on_comp_header_clicked` (Line 22512)
    - Method `_current_comp_sort_mode` (Line 22541)
    - Method `_get_comp_item_meta` (Line 22544)
    - Method `_comp_sort_key` (Line 22562)
    - Method `_sort_comp_items` (Line 22578)
    - Method `_build_comp_preview_label` (Line 22583)
    - Method `_set_comp_tree_text` (Line 22589)
    - Method `_format_score_with_boundary` (Line 22593) — Format score string with boundary indicator if applicable.
    - Method `_get_boundary_tooltip` (Line 22604) — Get tooltip text for boundary match indicator.
    - Method `_process_snippet_queue` (Line 22614)
    - Method `_update_comp_tree_tooltip` (Line 22635)
    - Method `_refresh_comp_tree_tooltips` (Line 22662)
    - Method `_apply_comp_node_previews` (Line 22674)
    - Method `_clear_comp_node_previews` (Line 22689)
    - Method `_set_comp_node_previews` (Line 22695)
    - Method `display_comp_results` (Line 22731)
    - Method `_get_filter_reason` (Line 23174) — Get human-readable filter reason for a composition result item.
    - Method `_make_node_checkable` (Line 23199) — Make a tree node checkable.
    - Method `_apply_comp_printed_badge` (Line 23204) — Set dedicated Printed column on composition tree node if manuscript is printed material.
    - Method `_add_manuscript_node` (Line 23213) — Add a manuscript/part node to the tree. Used for lazy/batched loading.
    - Method `_add_single_node_to_tree` (Line 23346) — Dedicated helper to add one row to the tree.
    - Method `_start_batched_tree_load` (Line 23406) — Start loading items into tree in batches to prevent UI freeze.
    - Method `_process_tree_batch` (Line 23415) — Process one batch of items and schedule next batch.
    - Method `_trigger_lazy_metadata_fetch` (Line 23441) — Starts background fetching for items that are currently displayed but missing data.
    - Method `on_comp_tree_item_changed` (Line 23455)
    - Method `on_comp_header_toggled` (Line 23486) — Toggle all root items in the composition tree.
    - Method `_set_check_state_recursive` (Line 23501)
    - Method `_update_comp_export_label` (Line 23507)
    - Method `_collect_checked_comp_items_struct` (Line 23516) — Collect checked items maintaining the structure (Main, Appendix, etc.)
    - Method `on_comp_tree_item_expanded` (Line 23659)
    - Method `on_comp_tree_item_collapsed` (Line 23686)
    - Method `_sync_parent_check_state` (Line 23690)
    - Method `_collect_checked_comp_page_uids` (Line 23708)
    - Method `_collect_all_comp_page_uids` (Line 23738)
    - Method `_update_recursive_button_state` (Line 23767)
    - Method `_has_comp_results` (Line 23778)
    - Method `show_comp_detail` (Line 23785)
    - Method `_refresh_comp_tree_metadata` (Line 23875)
    - Method `_fmt_item_legacy` (Line 23899)
    - Method `_format_comp_entry` (Line 23919)
    - Method `_fetch_metadata_with_dialog` (Line 23935)
    - Method `_resolve_meta_labels` (Line 23986)
    - Method `_update_part_state_for_sid` (Line 23999) — Refresh Part context (Neubauer) for the given system ID.
    - Method `browse_load` (Line 24013)
    - Method `_browse_load_part` (Line 24184) — Load a Codicological Part (Neubauer) for browsing.
    - Method `browse_navigate` (Line 24319)
    - Method `browse_render_page` (Line 24365)
    - Method `browse_open_catalog` (Line 24724)
    - Method `_browse_open_external_link` (Line 24729)
    - Method `_on_browse_thumb_resolved` (Line 24736)
    - Method `start_browse_download` (Line 24742)
    - Method `on_browse_img_loaded` (Line 24758)
    - Method `on_browse_img_failed` (Line 24768)
    - Method `cancel_browse_image_thread` (Line 24772)
    - Method `_cleanup_browse_inflight` (Line 24787) — Remove finished browse thread from in-flight list.
    - Method `fetch_browse_thumbnail` (Line 24795)
    - Method `check_updates_auto` (Line 24815) — Run update checker silently at startup.
    - Method `check_updates_manual` (Line 24826) — Run update checker with UI feedback.
    - Method `on_update_result` (Line 24836)
    - Method `on_update_error` (Line 24869)
    - Method `_on_sidecar_updates` (Line 24877) — Handle sidecar update availability notification.
    - Method `_start_sidecar_download` (Line 24901) — Download sidecar updates sequentially.
    - Method `_reset_sidecar_connections` (Line 24915) — Close all sidecar DB connections so files can be replaced.
    - Method `_download_next_sidecar` (Line 24927) — Download the next sidecar in the queue.
    - Method `_on_sidecar_download_finished` (Line 24945) — Handle completion of a single sidecar download.
    - Method `on_update_dismissed` (Line 24953) — Save dismissed version to config.
    - Method `on_whats_new_dismissed` (Line 24957) — Save that user has seen What's New for this version.
    - Method `show_whats_new_dialog` (Line 24961) — Show detailed What's New dialog.
    - Method `start_in_app_update` (Line 24968) — Start the in-app update process with progress dialog.
    - Method `run_indexing` (Line 24981)
    - Method `on_index_progress` (Line 25032)
    - Method `on_index_finished` (Line 25037)
    - Method `on_index_error` (Line 25043)
    - Method `_on_history_menu_hovered` (Line 25070) — Highlight the active QWidgetAction container on hover/keyboard navigation.
    - Method `_show_search_history_menu` (Line 25086) — Show the search history dropdown below the query input.
    - Method `_show_comp_history_menu` (Line 25094) — Show the composition history dropdown below the title input.
    - Method `_refresh_search_history` (Line 25101) — Rebuild the search history menu with per-item delete buttons.
    - Method `_refresh_comp_history` (Line 25115) — Rebuild the composition history menu with per-item delete buttons.
    - Method `_build_filter_summary` (Line 25130) — Build a compact filter summary string like [כולל: תנ״ך, תוספתא. 1000-1300].
    - Method `_add_history_menu_item` (Line 25185) — Add a single history entry to a menu with a delete button.
    - Method `_on_history_item_clicked` (Line 25245) — Restore state when a history menu item is clicked.
    - Method `_delete_history_item` (Line 25257) — Delete a single history entry and refresh the menu.
    - Method `_restore_regular_search_from_state` (Line 25272) — Apply a history entry and re-run the regular search.
    - Method `_restore_comp_search_from_state` (Line 25340) — Apply a history entry and re-run the composition search.
    - Method `_add_regular_search_to_history` (Line 25417) — Save the current regular search to history.
    - Method `_add_comp_search_to_history` (Line 25455) — Save the current composition search to history.
    - Method `_clear_search_history` (Line 25492) — Clear all entries for a search type after confirmation.
    - Method `_apply_persistent_session_preferences` (Line 25511) — Apply lightweight preferences that must survive even when full
    - Method `_save_session` (Line 25569) — Save current search state to disk for session persistence.
    - Method `_schedule_session_save` (Line 25701) — Schedule a debounced session save (500ms).
    - Method `_restore_session` (Line 25709) — Restore search state from saved session on startup.
    - Method `closeEvent` (Line 26130)
    - Method `_add_single_comp_node` (Line 26252) — Adds a node to the composition tree with parent/child logic.
    - Method `_on_comp_item_expanded` (Line 26347)
    - Method `_on_comp_item_collapsed` (Line 26352)
    - Method `on_comp_item_double_clicked` (Line 26371) — Smart navigation that restores full context (Next/Prev, Source Text).
    - Method `navigate_manuscript` (Line 26514) — Navigate to prev/next manuscript by file order, crossing Part boundaries.
    - Method `_update_part_image_for_folio` (Line 26570) — Update image viewer to show the current folio's images within a Part.
- **Function** `resource_path` (Line 26617) — Get absolute path to resource, works for dev and for PyInstaller

## genizah_core.py

- **Class** `SafeRotatingFileHandler` (Line 15) — A RotatingFileHandler that handles Windows file locking gracefully.
    - Method `doRollover` (Line 21)
- **Function** `construct_mosseri_cudl_label` (Line 143) — Convert a Mosseri shelfmark variant to a CUDL manifest label.
- **Function** `encode_word_shmidman` (Line 185) — Encode a single word by selecting its two rarest Hebrew characters.
- **Function** `text_to_fingerprint` (Line 204) — Convert free text into a fingerprint representation.
- **Function** `parse_boundaries` (Line 219) — Find word indices where boundaries occur.
- **Function** `chunk_crosses_boundary` (Line 256) — Check if a chunk spans any boundary with words on BOTH sides.
- **Function** `get_crossed_boundaries` (Line 279) — Get the set of boundary indices that a chunk crosses.
- **Function** `calculate_boundary_quality` (Line 298) — Calculate boundary match quality as average of match strengths.
- **Function** `calculate_final_score_with_boost` (Line 313) — Calculate final score with boundary boost.
- **Function** `get_boundary_stats` (Line 345) — Get pre-search statistics about boundaries.
- **Function** `get_volume_pages` (Line 410) — Filter browse_map pages to a specific IE's pages only.
- **Function** `get_volumes_for_sys_id` (Line 423) — Get volume information for a sys_id from ie_volume_map.
- **Function** `resolve_volume_suffix` (Line 434) — Map an IE identifier to its IIIF manifest suffix for a given sys_id.
- **Function** `configure_logger` (Line 467) — Configure a rotating file logger for the app (quiet for users, verbose for devs).
- **Function** `get_logger` (Line 490)
- **Function** `configure_lab_logger` (Line 498) — Configure a separate logger for Lab Mode operations.
- **Function** `load_language` (Line 539) — Load language preference. Returns 'en' or 'he'.
- **Function** `save_language` (Line 549) — Save language preference.
- **Function** `load_app_config` (Line 558) — Load general app configuration.
- **Function** `save_app_config` (Line 569) — Update general app configuration with new keys.
- **Function** `tr` (Line 583) — Translate text if current language is Hebrew.
- **Function** `calculate_smart_weights` (Line 627) — Analyzes corpus to generate HTR-aware letter frequency weights.

## supabase_corrections_client.py

- **Class** `User` (Line 87)
- **Class** `Correction` (Line 102)
- **Class** `Comment` (Line 127)
- **Class** `Discovery` (Line 147)
- **Class** `FragmentJoin` (Line 176)
- **Class** `JoinedFragmentDetail` (Line 193)
- **Class** `ConnectedFragments` (Line 204)
- **Class** `DiscoveryResponse` (Line 216)
- **Class** `FeedItem` (Line 228)
- **Function** `_map_join_type` (Line 266)
- **Class** `SupabaseCorrectionsClient` (Line 274)
    - Method `__init__` (Line 282)
    - Method `_get_client` (Line 305)
    - Method `_load_credentials` (Line 322)
    - Method `_save_credentials` (Line 359)
    - Method `save_login_credentials` (Line 398)
    - Method `get_saved_login_credentials` (Line 431)
    - Method `clear_saved_login_credentials` (Line 463)
    - Method `_load_cache` (Line 489)
    - Method `_save_cache` (Line 502)
    - Method `get_cached_data` (Line 510)
    - Method `set_cached_data` (Line 515)
    - Method `clear_cache` (Line 520)
    - Method `is_server_available` (Line 529)
    - Method `reset_offline_status` (Line 557)
    - Method `is_logged_in` (Line 564)
    - Method `login` (Line 575)
    - Method `register` (Line 615)
    - Method `logout` (Line 664)
    - Method `request_password_reset` (Line 680)
    - Method `_load_user_profile` (Line 706)
    - Method `get_current_user` (Line 741)
    - Method `create_correction` (Line 763)
    - Method `get_correction` (Line 812)
    - Method `get_corrections_for_document` (Line 828)
    - Method `get_my_corrections` (Line 873)
    - Method `get_all_corrections` (Line 923)
    - Method `vote_correction` (Line 983)
    - Method `_parse_correction` (Line 1012)
    - Method `create_comment` (Line 1033)
    - Method `get_document_comments` (Line 1070)
    - Method `get_comments_for_document` (Line 1093)
    - Method `get_my_comments` (Line 1097)
    - Method `_parse_comment` (Line 1117)
    - Method `create_discovery` (Line 1135)
    - Method `get_discovery` (Line 1179)
    - Method `get_discoveries` (Line 1205)
    - Method `vote_discovery` (Line 1261)
    - Method `_parse_discovery` (Line 1296)
    - Method `create_join` (Line 1324)
    - Method `get_connected_fragments` (Line 1365)
    - Method `get_connected_fragments_quick` (Line 1422)
    - Method `get_connected_fragments_by_id` (Line 1426)
    - Method `search_joins` (Line 1467)
    - Method `get_my_joins` (Line 1502)
    - Method `_parse_join` (Line 1536)
    - Method `_resolve_join_authors` (Line 1552)
    - Method `publish_puzzle_join` (Line 1585)
    - Method `unpublish_puzzle_join` (Line 1604)
    - Method `check_is_published` (Line 1616)
    - Method `get_published_puzzle_joins` (Line 1628)
    - Method `get_published_joins_for_fragment` (Line 1640)
    - Method `fork_puzzle_join` (Line 1652)
    - Method `get_feed` (Line 1668)
    - Method `get_correction_stats` (Line 1748)
    - Method `get_discovery_stats` (Line 1765)
    - Method `submit_correction` (Line 1794)
    - Method `get_all_comments` (Line 1809)
    - Method `update_discovery` (Line 1828)
    - Method `delete_discovery` (Line 1860)
    - Method `add_discovery_response` (Line 1872)
    - Method `get_discovery_responses` (Line 1883)
    - Method `mark_discovery_answered` (Line 1888)
    - Method `get_pending_corrections` (Line 1901)
    - Method `review_correction` (Line 1905)
    - Method `get_document_stats` (Line 1939)
    - Method `get_corrected_text` (Line 1956)
    - Method `get_page_versions` (Line 1962)
    - Method `get_version_content` (Line 1967)
    - Method `record_document_view` (Line 1971)
    - Method `get_leaderboard` (Line 1975)
    - Method `get_join_by_id` (Line 1998)
    - Method `delete_join` (Line 2014)
    - Method `update_join` (Line 2026)
    - Method `react_to_comment` (Line 2052)
    - Method `pin_discovery` (Line 2057)
    - Method `hide_discovery` (Line 2069)
    - Method `unhide_discovery` (Line 2073)
- **Function** `get_supabase_corrections_client` (Line 2093)

## gui_threads.py

- **Function** `_prevent_sleep` (Line 15) — Prevent OS sleep while search is running (Windows only).
- **Function** `_allow_sleep` (Line 26) — Re-allow OS sleep after search completes (Windows only).
- **Class** `IndexerThread` (Line 35) — Build or refresh the index without blocking the UI.
    - Method `__init__` (Line 41)
    - Method `run` (Line 45)
- **Class** `RefinementReplayThread` (Line 52) — Replay a refinement chain off the UI thread to rebuild restrict sets.
    - Method `__init__` (Line 66)
    - Method `run` (Line 72)
- **Class** `PausableSearchMixin` (Line 82) — Cancel + pause plumbing shared by the four desktop search workers.
    - Method `_init_pause_support` (Line 97)
    - Method `pause` (Line 112)
    - Method `resume` (Line 115)
    - Method `is_pause_pending` (Line 118)
    - Method `request_cancel` (Line 121) — The one stop entry point: set the flag AND un-park, atomically.
    - Method `requestInterruption` (Line 132) — Route Qt's own interruption request into request_cancel().
    - Method `_emit_pause_ack` (Line 144)
    - Method `_should_abort` (Line 150)
    - Method `_checkpoint` (Line 161) — Cooperative checkpoint: raises to cancel, blocks to pause.
- **Class** `SearchThread` (Line 173) — Execute a search query asynchronously.
    - Method `__init__` (Line 187)
    - Method `run` (Line 197)
- **Class** `LabSearchThread` (Line 241) — Execute a Lab Mode search query.
    - Method `__init__` (Line 253)
    - Method `run` (Line 270)
- **Class** `CompositionThread` (Line 315) — Scan compositions in background to keep UI responsive.
    - Method `__init__` (Line 327)
    - Method `run` (Line 352)
- **Class** `LabCompositionThread` (Line 395) — Execute Lab Composition Search (Broad-to-Narrow).
    - Method `__init__` (Line 407)
    - Method `run` (Line 431)
- **Class** `GroupingThread` (Line 483) — Group composition results while reporting progress to the UI.
    - Method `__init__` (Line 492)
    - Method `run` (Line 499)
- **Class** `ShelfmarkLoaderThread` (Line 532) — Background thread to load metadata.
    - Method `__init__` (Line 542)
    - Method `request_cancel` (Line 547)
    - Method `run` (Line 550)
- **Class** `StartupThread` (Line 574) — Initialize heavy components in the background.
    - Method `run` (Line 579)
- **Class** `EnrichMetadataThread` (Line 594) — Fetch extended metadata (IIIF/MARC) in the background.
    - Method `__init__` (Line 598)
    - Method `run` (Line 604)
- **Class** `VolumeManifestThread` (Line 614) — Lightweight manifest-only fetch for volume switches (no full enrichment).
    - Method `__init__` (Line 618)
    - Method `run` (Line 625)
- **Class** `ExternalResourceThread` (Line 633) — Fetch external IIIF resources (e.g. Cambridge) in background.
    - Method `__init__` (Line 637)
    - Method `run` (Line 642)
- **Class** `TranslateTextThread` (Line 652) — Translate a single text field via Dicta API in the background.
    - Method `__init__` (Line 657)
    - Method `run` (Line 663)
- **Class** `UpdateCheckerThread` (Line 682) — Check for updates on GitHub.
    - Method `__init__` (Line 689)
    - Method `run` (Line 694)
- **Class** `UpdateDownloaderThread` (Line 734) — Download update installer from GitHub Releases with progress reporting.
    - Method `__init__` (Line 740)
    - Method `cancel` (Line 746) — Request cancellation of the download.
    - Method `run` (Line 750)
- **Class** `DomainEnrichmentWorker` (Line 808) — Batch-fetch FJMS domain classifications for search results in background.
    - Method `__init__` (Line 816)
    - Method `run` (Line 820)
- **Class** `PGPSourceWorker` (Line 843) — Fetch PGP edition/translation sources for a fragment in the background.
    - Method `__init__` (Line 852)
    - Method `run` (Line 875)
- **Class** `PGPBadgeWorker` (Line 950) — Batch check badge sets for the results table (SEED-022).
    - Method `__init__` (Line 964)
    - Method `run` (Line 968)
- **Class** `PrintedBadgeWorker` (Line 986) — Batch check which sys_ids have FragmentMaterial=Printed for badge display.
    - Method `__init__` (Line 990)
    - Method `run` (Line 994)
- **Class** `PGPTagsWorker` (Line 1008) — Fetch all distinct PGP tags for dropdown population.
    - Method `__init__` (Line 1012)
    - Method `run` (Line 1015)
- **Class** `PGPTagSearchWorker` (Line 1025) — Search for fragments by PGP tag.
    - Method `__init__` (Line 1029)
    - Method `run` (Line 1033)
- **Class** `ReadingDeskWorker` (Line 1043) — Batch load PGP sources for multiple fragments for the reading desk.
    - Method `__init__` (Line 1052)
    - Method `run` (Line 1056)
- **Class** `SidecarUpdateThread` (Line 1079) — Check for sidecar data updates via GitHub release manifest.
    - Method `run` (Line 1091)
    - Method `_get_local_version` (Line 1141) — Get local sidecar version from the service singleton.
    - Method `_is_newer` (Line 1155) — Compare SemVer strings. Returns True if remote > local.
- **Class** `SidecarDownloadThread` (Line 1170) — Download a sidecar database update from GitHub Releases.
    - Method `__init__` (Line 1176)
    - Method `cancel` (Line 1183)
    - Method `run` (Line 1186)
- **Class** `PuzzleImageLoaderThread` (Line 1228) — Load and process a fragment image in the background via PuzzleImageService.
    - Method `__init__` (Line 1233)
    - Method `run` (Line 1243)
- **Class** `PuzzleMetaLoaderThread` (Line 1260) — Resolve images_nli (with fl_ids) for a sys_id in the background.
    - Method `__init__` (Line 1270)
    - Method `run` (Line 1276)
    - Method `_resolve_oxford_images` (Line 1330) — Try to resolve Oxford part images via shelfmark-based lookup.
- **Class** `FilterCountWorker` (Line 1382) — Background worker to compute manuscript count for pre-search filters.
    - Method `__init__` (Line 1386)
    - Method `run` (Line 1396)

## web/components/translate_button.py

- **Function** `_get_few_shot_prompt` (Line 29)
- **Function** `detect_language` (Line 71)
- **Function** `translate_text` (Line 97)
- **Function** `create_translate_button` (Line 146)
- **Function** `create_translatable_text` (Line 227)

## web/components/notes_display.py

- **Function** `render_content_with_mentions` (Line 30)
- **Function** `fetch_document_comments` (Line 170)
- **Function** `create_notes_panel` (Line 223)
- **Function** `create_comment_card` (Line 277)
- **Function** `create_reply_item` (Line 327)
- **Function** `create_notes_button` (Line 355)

## web/components/visual_similarity_dialog.py

- **Function** `_pick_preview_image_url` (Line 31) — Choose the same best-effort preview source family as the browse page.
- **Function** `_resolve_preview_image_url_sync` (Line 62) — Populate browse metadata on demand, then pick the preview image URL.
- **Function** `_fetch_original_info` (Line 90) — Fetch image URL and text snippet for the original manuscript.
- **Function** `_fetch_preview_image_url` (Line 129) — Resolve the best preview image URL without blocking the UI event loop.
- **Function** `_fetch_suggestion_text` (Line 137) — Fetch text snippet for a suggestion manuscript.
- **Function** `show_visual_similarity_dialog` (Line 155) — Show visual similarity suggestions dialog for a manuscript.
- **Function** `_render_suggestion_row` (Line 433) — Render a single suggestion row with expandable detail section.

## web/main.py

- **Function** `_inject_font_display_swap` (Line 36) — middleware injecting `font-display: swap` into NiceGUI fonts.css
- **Function** `page_meta` (Line 128) — per-route SEO metadata (title, description, canonical, OG/Twitter)
- **Function** `_resolve_ui_language` (Line 222) — detect UI language from storage/cookie
- **Function** `create_layout` (Line 240) — shared page layout (header, sidebar, footer)
- **Function** `_show_citation_reminder` (Line 635) — periodic citation reminder popup
- **Function** `apply_theme_immediately` (Line 689) — inject dark/light theme CSS
- **Function** `_safe_user_storage_get` (Line 821) — safe app.storage.user accessor
- **Function** `set_current_page` (Line 829) — track current page for analytics
- **Function** `dashboard_page` (Line 837) — homepage route
- **Function** `search_page_route` (Line 901) — search page route
- **Function** `parallels_page_route` (Line 935) — parallels page route
- **Function** `browse_page_route` (Line 954) — manuscript browse route
- **Function** `catalog_browse_page_route` (Line 1026) — FJMS catalog browse route
- **Function** `lists_page_route` (Line 1070) — user lists route
- **Function** `puzzle_page_route` (Line 1084) — fragment puzzle route
- **Function** `atlas_page_route` — Phase 133 Visual Atlas Preview beta `/atlas` route; gated on `atlas_preview_available()`, clean-hides + `noindex` when unavailable
- **Function** `_negotiate_encoding` — Accept-Encoding q-value negotiation (br/identity/`*`, honoring `q=0`) for the atlas data route; returns `'br' | 'identity' | None` (None → 406)
- **Function** `_register_atlas_data_routes` — registers `/atlas-data/manifest.json` (no-cache + ETag + 304) and `/atlas-data/{asset_name}` (content-hashed, immutable, Brotli) onto the app; both gated on `atlas_preview_available()`
- **Function** `auth_callback_route` (Line 1317) — OAuth callback handler
- **Function** `initialize_engine` (Line 1404) — async startup: load search engine, metadata, variants
- **Function** `_find_free_port` (Line 1465) — dev-mode port auto-discovery

## web/atlas_assets.py

Phase 133 (ATLAS-01) — the SINGLE authoritative asset-state source for the Visual Atlas Preview. Loads the baked binary once at startup from repo-root `atlas_data/` (deliberately OUTSIDE `web/static/`, HIGH-1) and exposes the shared availability predicate + byte accessors the `/atlas` page, nav link, and `/atlas-data/*` routes all consume.

- **Constant** `ATLAS_DATA_DIR` — repo-root `atlas_data/` (outside `STATIC_DIR`)
- **Function** `load_atlas_state` — read manifest + plain `.bin` (required) + `.bin.br` (optional) once at startup; fail-closed (`ready=False`) on any error
- **Function** `atlas_preview_available` — `ATLAS_PREVIEW_ENABLED and state.ready`; the one predicate gating page/nav/data routes (and the 133-05 teaser)
- **Functions** `atlas_bin_name` / `atlas_plain_bytes` / `atlas_br_bytes` / `atlas_manifest_bytes` / `atlas_manifest_etag` — byte + ETag accessors for the data routes

## web/pages/atlas.py

Phase 133 (ATLAS-01) — the Visual Atlas Preview page chrome (shared-shell embedded, Pattern 1).

- **Function** `create_atlas_page` — Beta badge + honesty banner + one-line intro (EN/HE + RTL via `tr`/`is_rtl`), a CLS-reserved fixed-dimension `<canvas>` container, and a documented JS injection point the 133-04 Canvas 2D renderer fills in

## web/pages/findings.py

Phase 136 — the corpus-wide "Computed Identifications" page (`/computed-identifications`).
Partial entry: the filter-state → read paths only, not the whole surface.

- **Filter state lives in per-user `safe_storage`, not the URL** — `_KEY_LOCUS_FROM` /
  `_KEY_LOCUS_TO` and siblings. Consequence: no page state can be reproduced from a link, so
  the citation-range filter cannot be exercised by `curl` and a readiness probe must call the
  service instead
- **Function** `fetch_findings` — the parent read. Passes every filter axis the builder
  accepts, including `locus_from` / `locus_to`, with `divergence=SHOWN`
- **Function** `_child_state` — the child's filter state: the parent's state with `unit`
  pinned to the leaf grain and the group key replaced. Copies *every* axis by contract
- **Function** `_fetch_children` — one grouped row's children, through the same shipped read.
  **The parent/child one-predicate invariant**: whatever axis `fetch_findings` passes, this
  must pass too, or a parent's count and its own child list describe different populations. A
  reader cannot see that kind of wrongness, which is what makes it worse than an error they
  can. `locus_from` / `locus_to` were missing here until 2026-08-20 and are covered by tests
  in `tests/test_findings_page.py`
- **Function** `_facet_request` / `fetch_facets` — the domain → author → work cascade. Never
  passes `work_id` or the locus bounds, so the citation-range predicate is structurally
  inactive in facet counts

## web/framework_patches.py

- **Function** `_patch_nicegui_esm_handler` (Line 20) — add is_file() guard to ESM route handler (prevents RuntimeError on directory URLs)
- **Function** `_patch_html_lang_attribute` (Line 63) — patch NiceGUI index.html to add `lang="he"` for Lighthouse a11y
- **Function** `apply_all_patches` (Line 92) — apply all NiceGUI monkey-patches; call once before ui.run()

## web/auth_state.py

- **Class** `GlobalAuthState` (Line 28) — singleton auth state using NiceGUI app.storage.user
    - Method `get_user` (Line 39)
    - Method `get_profile` (Line 46)
    - Method `is_logged_in` (Line 55)
    - Method `get_user_id` (Line 60)
    - Method `get_role` (Line 66)
    - Method `is_admin` (Line 72)
    - Method `is_editor` (Line 77)
    - Method `can_edit` (Line 83)
    - Method `can_comment` (Line 88)
    - Method `set_auth` (Line 93)
    - Method `_posthog_identify` (Line 102)
    - Method `update_profile_cache` (Line 115)
    - Method `clear_auth` (Line 120)
    - Method `get_username` (Line 137)
    - Method `get_headers` (Line 148)
- **Function** `do_login` (Line 154) — perform login and update global auth state
- **Function** `do_register` (Line 199) — register and auto-login
- **Function** `do_logout` (Line 240) — clear auth state
- **Function** `create_login_dialog` (Line 245) — build login/register dialog
- **Function** `create_auth_buttons` (Line 421) — header auth buttons (login/register or user menu)
- **Function** `get_api_base` (Line 482) — legacy compatibility stub
- **Function** `api_call` (Line 487) — legacy API call redirector

## shared/thread_local_db.py

- **Class** `ThreadLocalConnection` (Line 29) — thread-safe SQLite connection pool (one connection per thread)
    - Method `__init__` (Line 45)
    - Method `_prune_dead` (Line 69) — close connections from dead threads
    - Method `_get_conn` (Line 83) — get or create per-thread connection
    - Method `execute` (Line 110) — execute SQL on current thread's connection
    - Property `row_factory` (Line 114)
    - Method `close` (Line 126) — close all connections and reset state
    - Method `__bool__` (Line 138) — truthy for availability checks

## shared/search_serializer.py

Phase 77 single-source-of-truth serializer for the "Claude-friendly JSON" payload shape. One module powers both download handlers (`/api/export/json`, `/api/export/parallels/json`) and Phase 78+ API responses (`/api/search`, `/api/parallels`); modifying `_serialize_item()` updates download AND API in lockstep per EXPORT-03.

**Module-level constants:**
- `SCHEMA_VERSION = 1` (Line 52) — bump on incompatible envelope/item shape changes
- `NLI_RESOLVABLE_LIBRARY_CODES` (Line 61) — frozenset whitelist of providers with NLI IIIF coverage (CUL/JTS/BL/Manchester/RNL/AIU/Mosseri/Gaster/Halper); Oxford and other providers get `image_url=null` per HIGH-07
- `_filename_counter` (Line 72) — module-level `itertools.count()` for filename uniqueness without `time.sleep` per HIGH-06

**Public API (5 exports):**
- **Function** `serialize_search_payload` — emit envelope `{schema_version, source: 'search', query, mode, gap, filters, count, total, warnings, generated_at, results: [...]}` for /search results
- **Function** `serialize_parallels_payload` — emit envelope with separate top-level `results[]` and `filtered[]` arrays per D-11; one result per manuscript with `matches: [{chunk_index, source_chunk_text, manuscript_snippet, score}, ...]` per D-13 Path A; aggregate_score is SUM across uids
- **Function** `build_search_filename` — `genizah-search-{ISO timestamp with ms}_{counter}.json`
- **Function** `build_parallels_filename` — `genizah-parallels-{ISO timestamp with ms}_{counter}.json`

**Private helpers (single source of truth):**
- **Function** `_serialize_item` — THE per-item shape; emits `{uid, locator: {sys_id, volume_ie, p_num}, score, shelfmark, title, library: {code, name}, domains: [...], dating, snippet, excerpt, match_terms: [...], image_url}`. Both top-level functions reach into this — `serialize_parallels_payload` via `_to_parallels_envelope_item` which adds `matches: [...]` on top. `tests/test_search_serializer.py::test_serializers_share_serialize_item` enforces structurally via `dir()` introspection (no `_serialize_search_item` / `_serialize_parallels_item` shadows allowed)
- **Function** `_extract_match_terms` — D-03 dedup-in-order from `*term*` markers
- **Function** `_build_image_url` — server-relative `/api/nli_image_by_sysid/{sys_id}?page={p_num-1}` or null when library_code not in whitelist (HIGH-07)
- **Function** `_safe_library_name` — graceful-degrade lookup via `genizah_core.get_library_display`
- **Function** `_safe_fjms_lookups` — graceful FJMS singleton consumer; **does NOT call .close()** (HIGH-05; close is reserved for `reset_fjms_service()`)
- **Function** `_group_parallels_by_sys_id`, `_to_parallels_envelope_item` — D-13 Path A grouping; per Plan 05 smoke-check fixes (commits `c24fcc48`, `327aea31`): rep-field mapping uses `synth['snippet']` / `synth['full_text']` so parallels items get populated `snippet`/`excerpt`/`match_terms`, AND group-level dedup keyed on `(chunk_index, manuscript_snippet)` collapses cross-uid duplicates that NLI's multi-uid cataloging emits (e.g. Karaite prayer books with multiple Alma uids on one sys_id) — highest-scoring entry wins, matches[] then sorted by `chunk_index` ascending for stable output. The `for ch_idx, src_text, score, ms_snippet in item.get('chunk_hits', [])` loop is wrapped in `isinstance(chunk_hits, list)` guard (commit `baf481fb`) so future shape regressions fall back to Path B (single degenerate match) instead of crashing
- **Function** `_filename_timestamp_with_ms`, `_utc_iso_now` — separate concerns: filename ms+counter vs envelope second-resolution

**Companion shape contract in `genizah_core.py` (Plan 02 + Plan 05 smoke-check):**
- Both `lab_composition_search` (lab path, Plan 02 commit `6ebefb71`) AND `search_composition_logic` (standard path, Plan 05 commit `c24fcc48`) now populate per-uid `chunk_hits` as a list-of-tuples `[(chunk_index_0_based, source_chunk_text, match_score, manuscript_snippet), ...]` consumed by `_to_parallels_envelope_item`. The pre-existing int counter on the standard path was renamed `chunk_count` to free the field name (avoid future collisions). Both paths apply per-uid `_chunk_hit_keys` dedup (commit `2e2d2b75`) so the same Tantivy uid does not emit duplicate (chunk_index, ms_snippet) entries when returned from multiple segments. Search results in `search_text_tantivy` (genizah_core.py:7542 + :7559) now record the `score` variable into the result dict so JSON exports carry the Tantivy relevance score (was always 0.0 prior to commit `2e2d2b75`).

Imported by `web/api.py` handlers `GET /api/export/json` (Line ~1920) and `GET /api/export/parallels/json` (Line ~1957) — both upgraded to `logger.exception` (was `logger.error`) per Plan 05 smoke-check commit `baf481fb` so future serializer crashes surface stack traces in production logs. Future Phase 78 `POST /api/search` and Phase 80 `POST /api/parallels` inherit the same envelope/item shape via the same imports.


---

# v7.9 Decomposed Modules

## desktop/__init__.py


## desktop/widgets.py

- **Class** `ActionsHoverWidget` (Line 12)
    - Method `__init__` (Line 13)
    - Method `add_btn` (Line 22)
    - Method `set_buttons_visible` (Line 31)
- **Function** `_format_add_to_list_label` (Line 39)
- **Function** `apply_find_highlight` (Line 44)
- **Function** `_get_folio_number_from_shelfmark` (Line 66) — Extract folio number from Oxford-style shelfmarks only.
- **Function** `_get_folio_image_index` (Line 94)
- **Function** `_get_initial_image_index` (Line 122)
- **Class** `ShelfmarkCompleter` (Line 155) — Custom Completer that normalizes input before matching.
    - Method `__init__` (Line 160)
    - Method `normalize` (Line 165)
    - Method `splitPath` (Line 169)
    - Method `pathFromIndex` (Line 172)
    - Method `complete` (Line 176)

## desktop/title_helpers.py

- **Function** `_get_title_svc` (Line 11) — Get or create a cached TranslationService for title lookups.
- **Function** `_truncate_title` (Line 22) — Truncate long title text with ellipsis. Returns (truncated_text, tooltip_or_None).
- **Function** `_is_hebrew_text` (Line 30) — Check if text is purely/nearly Hebrew with negligible English.
- **Function** `_translate_hebrew_date` (Line 43) — Translate Hebrew-numeral dates like 'מאה ט״ו' → '15th century'.
- **Function** `_resolve_display_title` (Line 95) — Resolve the display title using libraries_translations.db if available.
- **Function** `_set_label_with_tooltip` (Line 164) — Set label text with truncation and tooltip for full text.

## desktop/image_loader.py

- **Class** `ImageLoaderThread` (Line 15) — Smart Image Loader:
    - Method `__init__` (Line 26)
    - Method `cancel` (Line 42)
    - Method `run` (Line 45)
    - Method `_download_bytes` (Line 123) — Helper to download bytes safely.

## desktop/result_dialog.py

- **Class** `ResultDialog` (Line 36) — Allow browsing a single search result and its surrounding pages.
    - Method `__init__` (Line 42)
    - Method `init_ui` (Line 73)
    - Method `_toggle_compact_mode` (Line 507) — Toggle between compact and full header mode.
    - Method `navigate_results` (Line 529)
    - Method `open_full_transcription` (Line 535)
    - Method `search_for_parallels` (Line 546)
    - Method `add_current_to_list` (Line 564) — Add the current manuscript to a list.
    - Method `_add_to_puzzle` (Line 588) — Add current result to puzzle canvas (mirrors _browse_add_to_puzzle logic).
    - Method `_rd_search_visual_similarity` (Line 618) — D-10: Show visual similarity dialog from ResultDialog context.
    - Method `_update_add_to_list_button` (Line 667)
    - Method `add_comment` (Line 683) — Open comment dialog for current document.
    - Method `view_corrections` (Line 699) — View corrections for current document.
    - Method `view_comments` (Line 713) — View comments for current document.
    - Method `_rd_view_joins` (Line 725) — View joined fragments for current document.
    - Method `_rd_update_joins_menu` (Line 754) — Update the joins dropdown menu with connected fragments.
    - Method `_rd_on_joins_menu_show` (Line 914) — Called when joins menu is about to show - trigger sync and update.
    - Method `_rd_navigate_to_joined_fragment` (Line 926) — Navigate to a joined fragment within the same results dialog.
    - Method `_rd_load_versions` (Line 931) — Load versions for current document page.
    - Method `_rd_change_version` (Line 965) — Handle version change in ResultDialog.
    - Method `_rd_refresh_versions` (Line 971) — Refresh version list. If select_latest=True, select and load the latest version.
    - Method `_rd_load_version_content` (Line 1184) — Load and display version content.
    - Method `_rd_display_text` (Line 1256) — Display text in the manuscript viewer.
    - Method `_rd_display_pgp_text` (Line 1261) — Display PGP edition/translation text with proper directionality.
    - Method `_on_rd_pgp_loaded` (Line 1272) — Handle PGP sources loaded from background thread.
    - Method `_on_rd_pgp_error` (Line 1337) — Handle PGP source fetch error -- silently fall back to existing behavior.
    - Method `_rd_update_extended_info_with_pgp` (Line 1341) — Rebuild extended info HTML after PGP data arrives late.
    - Method `_rd_build_extended_html` (Line 1360) — Build the full extended info HTML for ResultDialog.
    - Method `_rd_refresh_extended_info` (Line 1565) — Rebuild ResultDialog extended info with current toggle state.
    - Method `_rd_toggle_edit_mode` (Line 1581) — Toggle edit mode in ResultDialog.
    - Method `_rd_exit_edit_mode` (Line 1619) — Exit edit mode.
    - Method `_rd_on_text_changed` (Line 1638) — Handle text changes in edit mode.
    - Method `_rd_cancel_edit` (Line 1667) — Cancel edit mode and restore original text.
    - Method `_rd_save_correction` (Line 1673) — Save correction from ResultDialog.
    - Method `_refresh_find_highlights` (Line 1781)
    - Method `_apply_source_highlights` (Line 1784)
    - Method `open_external_link` (Line 1798)
    - Method `_htmlify` (Line 1806)
    - Method `_apply_manual_highlights_to_text` (Line 1812)
    - Method `load_result_by_index` (Line 1834)
    - Method `_preload_next_result` (Line 1924)
    - Method `load_by_shelfmark` (Line 1940) — Load a document by shelfmark within the same dialog.
    - Method `load_page` (Line 1999)
    - Method `_update_rd_domain_label` (Line 2135) — Update domain info label and printed badge for the current result in ResultDialog.
    - Method `apply_metadata` (Line 2212)
    - Method `toggle_extended_info` (Line 2232)
    - Method `_on_rd_ext_link_clicked` (Line 2243) — Handle clicks on links in ResultDialog extended info.
    - Method `_rd_toggle_translations` (Line 2279) — Toggle show_translations from ResultDialog toolbar button.
    - Method `_rd_refresh_title` (Line 2316) — Refresh the ResultDialog title label based on current translation toggle.
    - Method `_rd_auto_translate_all` (Line 2328) — Auto-fire translations for all translatable fields that aren't cached yet.
    - Method `_show_rd_fjms_bib` (Line 2389) — Open FJMS bibliography dialog from ResultDialog.
    - Method `_show_rd_nli_bib` (Line 2403) — Open NLI bibliography dialog from ResultDialog.
    - Method `_show_rd_catalog` (Line 2417) — Open FJMS catalog records dialog from reading desk (lazy fetch).
    - Method `_show_rd_measurements` (Line 2441) — Open measurements dialog from reading desk (lazy fetch on first click).
    - Method `toggle_external_viewer` (Line 2465)
    - Method `on_enriched_data_loaded` (Line 2470)
    - Method `sync_external_view` (Line 2679)
    - Method `on_metadata_loaded` (Line 2697)
    - Method `_wait_or_terminate_thread` (Line 2703) — Wait for a QThread to finish; terminate as last resort.
    - Method `cancel_image_thread` (Line 2711)
    - Method `fetch_image` (Line 2719)
    - Method `_on_thumb_resolved` (Line 2746)
    - Method `start_download` (Line 2754)
    - Method `on_img_loaded` (Line 2770)
    - Method `on_img_failed` (Line 2776)
    - Method `closeEvent` (Line 2780)
    - Method `open_catalog` (Line 2808)
    - Method `open_viewer` (Line 2811)

## desktop/dialogs_filter.py

- **Class** `ExcludeDialog` (Line 21) — Collect system IDs or shelfmarks that should be excluded from searches.
    - Method `__init__` (Line 23)
    - Method `_clear_all` (Line 175) — Clear all entries from the editor and accept (removes all exclusions).
    - Method `_load_list_to_editor` (Line 191) — Load selected list items into the editor tab (sys_ids + shelfmarks).
    - Method `_resolve_and_show_report` (Line 225) — Resolve shelfmarks from the text areas and show resolution report table.
    - Method `get_exclusion_sources` (Line 261) — Return ExclusionSource objects from the active tab.
    - Method `eventFilter` (Line 327)
    - Method `_split_existing_entries` (Line 346)
    - Method `_on_sys_text_changed` (Line 360)
    - Method `_on_shelf_text_changed` (Line 365)
    - Method `_sync_from_sys` (Line 370)
    - Method `_sync_from_shelf` (Line 379)
    - Method `resizeEvent` (Line 388)
    - Method `_get_lines` (Line 393)
    - Method `_set_titles` (Line 396)
    - Method `_refresh_title_display` (Line 400)
    - Method `_resolve_shelves_from_sys` (Line 409)
    - Method `_resolve_titles_from_sys` (Line 423)
    - Method `_ensure_shelf_map` (Line 437)
    - Method `_add_shelf_map` (Line 448)
    - Method `_resolve_sys_from_shelves` (Line 453)
    - Method `_normalize_shelfmark` (Line 461)
    - Method `load_file` (Line 470)
    - Method `get_entries_text` (Line 507)
- **Class** `DomainFilterDialog` (Line 533) — Hierarchical domain filter dialog with checkboxes and type-ahead search.
    - Method `__init__` (Line 540)
    - Method `_populate_tree` (Line 593) — Populate tree with domains from current search results only.
    - Method `_filter_tree` (Line 671) — Filter tree items by search text.
    - Method `_handle_item_changed` (Line 701) — Handle checkbox state changes with parent-child propagation.
    - Method `_check_all` (Line 722) — Check all items (no filtering).
    - Method `_uncheck_all` (Line 737) — Uncheck all items (exclude all domains).
    - Method `_restore_exclusions` (Line 752) — Restore previously excluded domains by unchecking them.
    - Method `get_excluded_domains` (Line 779) — Return set of excluded (unchecked) domain names.
    - Method `_update_summary` (Line 803) — Update exclusion summary label.
- **Class** `PreSearchFilterDialog` (Line 818) — Pre-search filter dialog with multi-select domain, author, work, date range,
    - Method `__init__` (Line 826)
    - Method `_check_list_item` (Line 1117) — Check/uncheck a QListWidget item by its data value.
    - Method `_check_tree_item` (Line 1126) — Check/uncheck a QTreeWidget item by its data value (searches all levels).
    - Method `_get_checked_items` (Line 1142) — Return list of data values for all checked items.
    - Method `_get_checked_tree_items` (Line 1154) — Return list of data values for all checked leaf/child items in a QTreeWidget (up to 3 levels).
    - Method `_populate_domains` (Line 1184) — Populate domain tree with hierarchy from FJMS.
    - Method `_populate_authors` (Line 1241) — Populate author dropdown, optionally filtered by first selected domain.
    - Method `_populate_works` (Line 1271) — Populate work dropdown, optionally filtered by domain and author.
    - Method `_filter_domain_list` (Line 1302) — Filter domain tree items by search text.
    - Method `_on_domain_tree_changed` (Line 1318) — Handle domain tree checkbox with parent-child propagation.
    - Method `_on_domain_changed` (Line 1335) — When domain selection changes, re-populate authors and works.
    - Method `_on_author_selected` (Line 1342) — Handle author dropdown selection — add to selected list.
    - Method `_on_work_selected` (Line 1352) — Handle work dropdown selection — add to selected list.
    - Method `_on_author_changed` (Line 1362) — When author selection changes, re-populate works.
    - Method `_on_filter_changed` (Line 1368) — Any filter changed -- update count and chip bar.
    - Method `_add_text_term` (Line 1373) — Add a text filter term from the input.
    - Method `_remove_text_term` (Line 1385) — Remove a text filter term.
    - Method `_make_chip` (Line 1392) — Create a removable chip button (dark-mode aware).
    - Method `_rebuild_dialog_chips` (Line 1420) — Rebuild unified chip bar showing all active filters.
    - Method `_get_current_filter_dict` (Line 1516) — Build filter dict from current dialog state.
    - Method `_get_measurement_filters` (Line 1550) — Extract measurement filter values from dialog spin boxes and checkboxes.
    - Method `_get_display_name` (Line 1578) — Get display name (without count) from data map.
    - Method `_update_count` (Line 1583) — Recompute manuscript count in background thread.
    - Method `_on_count_finished` (Line 1597) — Handle count worker result.
    - Method `_clear_all` (Line 1610) — Reset all filter controls to default.
    - Method `get_filters` (Line 1650) — Return the current filter state dict.
    - Method `get_restrict_sys_ids` (Line 1654) — Return the computed restrict_sys_ids set (or None).

## desktop/dialogs_scholarly.py

- **Class** `FjmsBibliographyDialog` (Line 13) — FJMS bibliography dialog with structured table.
    - Method `__init__` (Line 16)
    - Method `_filter_rows` (Line 125)
    - Method `_safe` (Line 162) — Return stripped string or empty string for None/placeholder values.
    - Method `_on_row_selected` (Line 167)
- **Class** `FjmsCatalogDialog` (Line 213) — Dialog showing FJMS catalog records with multi-team scholarly descriptions.
    - Method `__init__` (Line 222)
    - Method `_on_anchor_clicked` (Line 259) — Handle anchor clicks: toggle translation or open external links.
    - Method `_build_html` (Line 272) — Build HTML table mirroring FIST Cataloging Data Details view.
    - Method `_section_row` (Line 836) — Build a section header row.
    - Method `_field_row` (Line 844) — Build a field row: label + value columns. RTL: values first, label last.
    - Method `_field_category_row` (Line 862) — Build a row for a specific FieldCategory from catalog_fields.
    - Method `_fmt_num` (Line 881) — Format a numeric value for size display, removing trailing .0.
    - Method `_fmt_int` (Line 891) — Format a numeric value as integer (2.0 → '2').
- **Class** `FjmsMeasurementsDialog` (Line 901) — Dialog showing physical measurements for a manuscript.
    - Method `__init__` (Line 908)
    - Method `_build_html` (Line 942) — Build HTML content for the measurements dialog.
- **Class** `NliBibliographyDialog` (Line 1146) — NLI bibliography dialog with MARC 581 reference strings.
    - Method `__init__` (Line 1149)
    - Method `_filter_rows` (Line 1257)
    - Method `_on_row_selected` (Line 1288)

## desktop/viewers.py

- **Function** `_make_scrollable_row` (Line 28) — Wrap a QHBoxLayout in a horizontal QScrollArea so it can shrink freely in a splitter.
- **Function** `_generate_oxford_dynamic_url` (Line 45) — Generate dynamic Oxford image URL for a folio not in the database.
- **Class** `ZoomableScrollArea` (Line 70) — A GraphicsView that supports hand-panning and wheel-zooming.
    - Method `__init__` (Line 72)
    - Method `_show_image_context_menu` (Line 113) — Show context menu with Copy/Save options for the displayed image.
    - Method `_copy_image` (Line 126) — Copy current image (with rotation) to clipboard.
    - Method `_save_image` (Line 132) — Save current image (with rotation) to file.
    - Method `_get_rotated_pixmap` (Line 143) — Return the current pixmap with adjustments and rotation applied (for export).
    - Method `set_image` (Line 155)
    - Method `set_status_message` (Line 198)
    - Method `_update_text_pos` (Line 206)
    - Method `set_rotation` (Line 217) — Set absolute rotation (degrees clockwise) and update view.
    - Method `rotate_view` (Line 222) — Add degrees to current rotation and update.
    - Method `wheelEvent` (Line 227)
    - Method `zoom_in` (Line 239)
    - Method `zoom_out` (Line 243)
    - Method `_apply_zoom` (Line 247)
    - Method `resizeEvent` (Line 259)
    - Method `_apply_fit_to_viewport` (Line 266)
    - Method `set_adjustments` (Line 275) — Update image adjustment values and schedule a filter update.
    - Method `_schedule_filter_update` (Line 283) — Debounce filter updates to 100ms for performance on large images.
    - Method `_build_lut` (Line 295) — Build a 256-entry lookup table for brightness/contrast/gamma/invert.
    - Method `_apply_display_filters` (Line 314) — Apply brightness/contrast/gamma/invert to display via LUT on pixels.
    - Method `_apply_adjustments_to_pixmap` (Line 351) — Apply current adjustments to a pixmap and return the result. Used for export.
    - Method `reset_adjustments` (Line 378) — Reset all image adjustments to defaults.
- **Class** `FullscreenImageWindow` (Line 390) — Borderless fullscreen window for manuscript image viewing.
    - Method `__init__` (Line 402)
    - Method `_adjust_rotation` (Line 581)
    - Method `_update_page_label` (Line 585)
    - Method `set_image` (Line 594) — Update the displayed image (called when page changes).
    - Method `keyPressEvent` (Line 607)
    - Method `showFullScreen` (Line 618)
- **Class** `ManuscriptViewerWidget` (Line 625) — Reusable widget for displaying manuscript images with navigation.
    - Method `__init__` (Line 629)
    - Method `init_ui` (Line 649)
    - Method `_detect_external_provider` (Line 815)
    - Method `set_image_by_fl_id` (Line 842)
    - Method `load_images` (Line 859)
    - Method `_on_source_changed` (Line 979)
    - Method `_resolve_url` (Line 994)
    - Method `_retire_thread` (Line 999) — Move a canceled QThread to the in-flight list so it stays alive until finished.
    - Method `_cleanup_inflight` (Line 1015) — Remove a finished thread from the in-flight list and schedule deletion.
    - Method `_preload` (Line 1023)
    - Method `_wait_or_terminate` (Line 1036) — Wait for a QThread to finish; terminate as last resort to prevent destroyed-while-running.
    - Method `stop_threads` (Line 1044) — Stop all running image loading threads. Call before destroying widget.
    - Method `_on_thumbnail_ready` (Line 1062) — Handle thumbnail loaded signal - only display if still on same page and same load generation.
    - Method `_load_thumbnail_async` (Line 1070) — Load thumbnail asynchronously for quick display while full image loads.
    - Method `set_page` (Line 1100)
    - Method `_execute_set_page` (Line 1120) — Actually load the image after debounce settles.
    - Method `display_image` (Line 1167)
    - Method `open_external` (Line 1180)
    - Method `_open_ktiv_viewer` (Line 1188) — Open the NLI KTIV manuscript viewer at the current page.
    - Method `_open_fullscreen` (Line 1201) — Open current image in fullscreen window.
    - Method `_on_fullscreen_page_change` (Line 1211) — Handle page navigation from fullscreen window.
    - Method `_sync_fullscreen_image` (Line 1218) — Push current image to the fullscreen window if open.
    - Method `adjust_rotation` (Line 1225) — Adjust rotation via slider to keep controls in sync.

## desktop/puzzle.py

- **Class** `PuzzleFragmentItem` (Line 29) — A positioned fragment image on the puzzle canvas.
    - Method `__init__` (Line 45)
    - Method `_pixmap_rect` (Line 73) — The actual pixmap bounding rect (without handle margin).
    - Method `_handle_points` (Line 77) — Return dict of handle_id -> QPointF center positions.
    - Method `_hit_handle` (Line 88) — Return handle id under pos using wide border zones.
    - Method `_apply_flip` (Line 133) — Apply horizontal/vertical flip via QTransform.
    - Method `flip_horizontal` (Line 145)
    - Method `flip_vertical` (Line 149)
    - Method `_is_crop_mode` (Line 155) — Check if crop mode is active (set by PuzzleCanvasWindow).
    - Method `mousePressEvent` (Line 159)
    - Method `mouseMoveEvent` (Line 199)
    - Method `mouseReleaseEvent` (Line 292)
    - Method `hoverMoveEvent` (Line 335)
    - Method `hoverLeaveEvent` (Line 351)
    - Method `adjust_scale_from_wheel` (Line 357) — Resize fragment from a wheel delta (called by PuzzleCanvasView).
    - Method `wheelEvent` (Line 365)
    - Method `boundingRect` (Line 372) — Always include handle margin so Qt repaints handle areas on move.
    - Method `paint` (Line 378)
    - Method `update_pixmap` (Line 431) — Replace displayed image (e.g. folio nav or threshold change).
    - Method `shape` (Line 458)
- **Class** `PuzzleCanvasView` (Line 464) — A QGraphicsView hosting PuzzleFragmentItem instances.
    - Method `__init__` (Line 470)
    - Method `cycle_background` (Line 506) — Cycle to the next background mode.
    - Method `set_checkerboard` (Line 512) — Legacy toggle -- switches between dark gray and checkerboard.
    - Method `drawBackground` (Line 517)
    - Method `mousePressEvent` (Line 553)
    - Method `mouseMoveEvent` (Line 568)
    - Method `mouseReleaseEvent` (Line 582)
    - Method `wheelEvent` (Line 592)
    - Method `get_fragment_items` (Line 615) — Return all PuzzleFragmentItem instances on the scene.
    - Method `get_selected_fragments` (Line 619) — Return selected PuzzleFragmentItem instances.
- **Class** `PuzzleExportThread` (Line 624) — Compose and save a puzzle PNG without blocking the desktop UI.
    - Method `__init__` (Line 632)
    - Method `run` (Line 639)
- **Class** `PuzzlePublishThread` (Line 674) — Worker thread for publish/unpublish operations.
    - Method `__init__` (Line 678)
    - Method `run` (Line 685)
- **Class** `PuzzleCanvasWindow` (Line 696) — Standalone puzzle workspace for assembling fragment images.
    - Method `__init__` (Line 704)
    - Method `add_fragment` (Line 1045) — Add a fragment to the puzzle canvas. Starts async image load.
    - Method `_on_add_shelfmark` (Line 1115) — Handle shelfmark entry: resolve sys_id, then async fl_id resolution.
    - Method `_show_add_from_list` (Line 1152) — Show picker to add fragments from a personal list.
    - Method `_show_add_from_joins` (Line 1215) — Show connected fragments for the selected fragment and add them.
    - Method `_on_meta_resolved` (Line 1305) — Callback from PuzzleMetaLoaderThread -- cache folio list and add first folio.
    - Method `_on_meta_failed` (Line 1319) — Callback from PuzzleMetaLoaderThread -- show error.
    - Method `_on_image_loaded` (Line 1329) — Called when PuzzleImageLoaderThread finishes -- create or update item.
    - Method `_fit_all_fragments` (Line 1401) — Fit view to show all fragments with some padding.
    - Method `_on_image_failed` (Line 1415) — Called when PuzzleImageLoaderThread fails.
    - Method `_on_selection_changed` (Line 1439) — Update toolbar to reflect current selection.
    - Method `_flip_selected_h` (Line 1480)
    - Method `_flip_selected_v` (Line 1484)
    - Method `_cycle_bg` (Line 1488) — Cycle to next background mode and show name in status bar.
    - Method `_rotate_selected` (Line 1501) — Rotate selected fragments by given degrees.
    - Method `_has_blue_mat` (Line 1509) — Check if a PuzzleFragment is likely from a library with blue conservation mat.
    - Method `_flip_recto_verso` (Line 1523) — Flip selected fragment(s) to show recto/verso -- navigates to next/prev folio.
    - Method `_flip_entire_puzzle` (Line 1576) — Flip ALL fragments -- shows the other side of the joined page.
    - Method `_toggle_crop_mode` (Line 1656) — Enter/exit crop mode. In crop mode, drag edges of selected fragment to trim.
    - Method `_crop_edge` (Line 1679) — Crop a specific edge from selected fragment.
    - Method `_revert_crop` (Line 1712) — Revert selected fragments to original uncropped image.
    - Method `_nudge_threshold` (Line 1735) — Increment/decrement threshold by delta, then apply.
    - Method `_on_threshold_changed` (Line 1741) — Re-fetch images with new threshold for selected fragments.
    - Method `_nudge_scale` (Line 1759) — Increment/decrement scale by delta percent.
    - Method `_on_scale_changed` (Line 1764) — Update scale for selected fragments proportionally.
    - Method `_navigate_folio` (Line 1795) — Navigate folio prev/next for selected fragments.
    - Method `_change_z_order` (Line 1849) — Move selected fragment one layer up (+1) or down (-1).
    - Method `_delete_selected` (Line 1859) — Remove selected fragments from the canvas.
    - Method `_refresh_fragment_combo` (Line 1872) — Rebuild the fragment dropdown from current items.
    - Method `_browse_selected_fragment` (Line 1883) — Open the selected fragment in the browse tab.
    - Method `_on_fragment_combo_changed` (Line 1900) — Select the fragment chosen in the dropdown.
    - Method `_on_canvas_context_menu` (Line 1912) — Show right-click context menu on fragment items.
    - Method `_refresh_docs_list` (Line 1976) — Refresh the saved documents list in the side panel.
    - Method `_on_doc_list_clicked` (Line 2010) — Load a document when clicked in the side panel.
    - Method `_load_document` (Line 2027) — Load a PuzzleDocument onto the canvas, replacing current content.
    - Method `_spawn_meta_loader` (Line 2083) — Spawn a PuzzleMetaLoaderThread to fetch folio lists for a sys_id.
    - Method `_on_meta_ready_for_load` (Line 2091) — Handle meta_ready from folio list rebuild during document load.
    - Method `_on_save_join` (Line 2097) — Save current puzzle as a join document (new or update).
    - Method `_build_fragments_list` (Line 2169) — Build list of PuzzleFragment from current canvas items.
    - Method `_on_new_puzzle` (Line 2192) — Clear canvas to a fresh scratch pad.
    - Method `_clear_canvas` (Line 2213) — Remove all fragments from canvas.
    - Method `_on_export_png` (Line 2229) — Export composite PNG in a background thread.
    - Method `_cancel_export_thread` (Line 2297) — Request cancellation of the active export thread.
    - Method `_on_export_progress` (Line 2304) — Update the desktop export progress dialog.
    - Method `_clear_export_ui` (Line 2313) — Close and release the active export UI objects.
    - Method `_on_export_finished` (Line 2325) — Handle successful export completion.
    - Method `_on_export_cancelled` (Line 2330) — Handle user-cancelled export.
    - Method `_on_export_error` (Line 2335) — Handle export failure.
    - Method `_on_publish` (Line 2342) — Toggle publish/unpublish for current puzzle join.
    - Method `_run_publish_worker` (Line 2378) — Run publish/unpublish on a worker thread to avoid freezing UI.
    - Method `_on_publish_finished` (Line 2421) — Handle publish/unpublish completion on main thread.
    - Method `_check_publish_state` (Line 2448) — Check if current doc is published and update button state.
    - Method `_on_doc_context_menu` (Line 2464) — Show context menu on right-click in document list.
    - Method `_delete_document` (Line 2479) — Delete a saved join document with confirmation.
    - Method `_rename_document` (Line 2508) — Rename a saved join document.
    - Method `_on_title_changed` (Line 2530) — Handle title edit finished -- auto-save if editing a saved document.
    - Method `_on_notes_changed` (Line 2536) — Handle notes text changed -- auto-save if editing a saved document.
    - Method `_on_scene_changed` (Line 2542) — Handle scene.changed signal -- debounce and trigger auto-save for saved documents.
    - Method `_schedule_auto_save` (Line 2547) — Schedule a debounced auto-save (1.5s).
    - Method `_auto_save` (Line 2554) — Perform auto-save for the current document.
    - Method `_update_fragments_label` (Line 2583) — Update the fragments read-only label in the details panel.
    - Method `keyPressEvent` (Line 2595) — Keyboard shortcuts for puzzle canvas.
    - Method `closeEvent` (Line 2660) — Wait for active loader threads before closing.

## desktop/vs_cache.py

- **Class** `DesktopVSCache` (Line 11) — Local SQLite cache for visual similarity suggestions fetched from server.
    - Method `__init__` (Line 15)
    - Method `get_server_version` (Line 38)
    - Method `set_server_version` (Line 42)
    - Method `check_and_update_version` (Line 50) — Check server version and invalidate cache if stale. Called on app startup.
    - Method `get_cached` (Line 63)
    - Method `store` (Line 74)
    - Method `has_cached` (Line 90)
    - Method `get_cached_partners` (Line 93)
- **Class** `VSFetchThread` (Line 107) — Fetch visual similarity suggestions from server for a single manuscript.
    - Method `__init__` (Line 112)
    - Method `run` (Line 117)
- **Class** `VSDownloadThread` (Line 128) — Download full visual_similarity.db with checksum, disk-space, and corruption checks.
    - Method `__init__` (Line 134)
    - Method `run` (Line 139)

## web/pages/search_state.py

- **Class** `SearchUIState` (Line 27)
    - Method `__init__` (Line 28)
- **Class** `AdvancedViewState` (Line 129) — State holder for the Advanced View dialog to enable in-place updates.
    - Method `__init__` (Line 131)
- **Class** `SearchPageRefs` (Line 175) — UI element references and callbacks needed by extracted search_results functions.
- **Function** `restore_search_snapshot` (Line 241) — Hydrate page-scoped state from app.storage.user snapshot.
- **Function** `persist_search_snapshot` (Line 293) — Serialize restorable fields of SearchUIState to app.storage.user.
- **Function** `clear_search_snapshot` (Line 333) — Wipe all search snapshot keys from app.storage.user.
- **Function** `clear_search_filters` (Line 397) — Reset only pre-search filter storage keys (Advanced 'Clear All' filters).
- **Function** `get_search_history` (Line 430) — Get search history from storage.
- **Function** `add_to_search_history` (Line 435) — Add or update a search history entry. Deduplicates by query+mode.
- **Function** `delete_search_history_entry` (Line 469) — Delete a specific history entry by index.
- **Function** `clear_search_history` (Line 477) — Clear all search history.
- **Function** `domain_display_name` (Line 486) — Get display name for a domain (Hebrew if UI is Hebrew, else English).

## web/pages/search_results.py

- **Function** `copy_result_text` (Line 47) — Copy text to clipboard.
- **Function** `show_add_to_list_dialog` (Line 61)
- **Function** `toggle_expansion` (Line 86) — Toggle inline accordion expansion for a result card.
- **Function** `render_results` (Line 115)
- **Function** `create_result_card` (Line 345)
- **Function** `open_advanced_dialog` (Line 752) — Open an enhanced Advanced View dialog with in-place navigation and IIIF image viewer.

## web/pages/browse_state.py

- **Class** `BrowseState` (Line 23) — Holds the state for the browse page.
    - Method `__init__` (Line 26)
- **Function** `restore_browse_snapshot` (Line 95) — Hydrate browse snapshot fields; return raw (position, reading_desk) dicts.
- **Function** `persist_browse_snapshot` (Line 154) — Serialize browse position and reading desk state to app.storage.user.
- **Function** `clear_browse_snapshot` (Line 196) — Wipe browse snapshot keys.

## web/pages/browse_enrichment.py

- **Class** `BrowsePageRefs` (Line 33) — UI element references and callbacks needed by extracted browse_enrichment functions.
- **Function** `load_enrichment` (Line 66) — Phase B: Load PGP + FJMS enrichment data in background.
- **Function** `update_enrichment_sections` (Line 404) — Update enrichment placeholder containers after Phase B completes.
- **Function** `populate_bib_catalog_buttons` (Line 464) — Populate bibliography and catalog buttons in the page navigation pane.

## web/search_bootstrap.py

- **Function** `resolve_search_bootstrap` (Line 22) — Resolve whether persisted search UI state should be reused for this request.

## v8.3.0 Decomposed Modules

Modules created by the v8.3.0 god-file decomposition (Phases 122-127), holding code
moved out of `genizah_core.py` (→ `shared/*`) and `genizah_app.py` (→ `desktop/*`).

## shared/config.py

- **Class** `Config` (Line 15) — Static paths and limits used by the application and by bundled binaries.
    - Method `_pick_writable_dir` (Line 18) — Prefer primary; if we cannot create/write there, use fallback.
    - Method `_get_documents_dir` (Line 38) — Best-effort Documents directory (Windows-aware), falling back to home.
    - Method `resource_path` (Line 143) — Return absolute path to bundled resources.

## shared/variants.py

- **Class** `VariantManager` (Line 21) — Generate spelling variants for Hebrew search terms using unified frequency-based pairs.
    - Method `make_multimap` (Line 50) — Create bidirectional mapping from character pairs.
    - Method `__init__` (Line 58)
    - Method `_get_custom_pairs` (Line 69) — Parse custom variants from settings.
    - Method `_get_pairs_count` (Line 101) — Get the number of variant pairs to use.
    - Method `_get_unified_pairs` (Line 117) — Get top N pairs from unified variant list, split into single-char and multi-char.
    - Method `_get_multichar_pairs_for_mode` (Line 138) — Get multi-character pairs based on search mode and settings.
    - Method `_generate_multichar_variants` (Line 154) — Generate variants using multi-character substitution pairs.
    - Method `_rebuild_maps` (Line 183) — Build variant maps from unified frequency-sorted pairs list.
    - Method `set_settings` (Line 207) — Update settings reference, rebuild maps, and clear cache.
    - Method `set_variant_level` (Line 213) — Update variant pairs count (slider value) and rebuild slider map.
    - Method `get_variant_level` (Line 229) — Get current variant pairs count.
    - Method `get_max_variant_pairs` (Line 233) — Get total number of available variant pairs.
    - Method `_get_max_changes_for_length` (Line 237) — Dynamic max_changes based on term length to prevent combinatorial explosion.
    - Method `hamming_distance` (Line 261) — Calculate character difference count between term and variant.
    - Method `generate_variants` (Line 267) — Generate variants with early termination and smart position filtering.
    - Method `get_variants` (Line 317) — Generate spelling variants for Hebrew search terms.
    - Method `clear_cache` (Line 415) — Clear the variant cache.

## shared/codicological.py

- **Class** `CodicologicalManager` (Line 20) — Manages codicological units (Parts) for Oxford manuscripts.
    - Method `__init__` (Line 29)
    - Method `load` (Line 46) — Load Oxford database and build all mappings.
    - Method `_build_part_mappings` (Line 78) — Build part_metadata and volume structure from JSON.
    - Method `_build_folio_mappings` (Line 106) — Build folio→part mappings using CSV data.
    - Method `_resolve_part_by_folio_range` (Line 141) — Resolve a shelfmark to its Part using folio_range from JSON.
    - Method `_get_folio_number` (Line 182) — Extract folio number from shelfmark for sorting.
    - Method `_build_autocomplete_list` (Line 189) — Build autocomplete entries for Parts with 'part X' suffix.
    - Method `get_part_for_folio` (Line 222) — Get the Part ID for a given system ID (folio).
    - Method `get_folios_for_part` (Line 226) — Get all system IDs (folios) belonging to a Part, in order.
    - Method `get_part_metadata` (Line 230) — Get full metadata for a Part.
    - Method `get_part_images` (Line 234) — Get all images for a Part.
    - Method `get_part_display_name` (Line 239) — Get display name for a Part (with 'part X' suffix).
    - Method `get_part_label` (Line 251) — Return a short Part label suitable for shelfmark suffixes (e.g., "part 23").
    - Method `is_part_id` (Line 262) — Check if an identifier is a Part ID (vs a regular shelfmark).
    - Method `parse_part_identifier` (Line 274) — Parse an identifier that might be a Part.
    - Method `get_image_for_folio` (Line 316) — Get the specific image URL for a folio within its Part.
    - Method `get_all_images_for_part` (Line 353) — Get all images for a Part with their labels.
    - Method `get_adjacent_part` (Line 357) — Get the next or previous Part in the same volume.

## shared/responsa.py

- **Function** `_tr` (Line 19) — Translate text if current language is Hebrew.
- **Class** `ResponsaComponent` (Line 72) — Structured representation of a single token in a parsed Responsa query.
- **Function** `parse_responsa_query` (Line 91) — Parse a Responsa-Project style query string into a list of ResponsaComponent objects.
- **Function** `_has_line_break_syntax` (Line 144) — Check if a query string contains line-break syntax (| characters).
- **Class** `LineGroup` (Line 177) — A constraint on one line of text in a line-break search.
- **Function** `_parse_line_break_query` (Line 192) — Parse a Responsa query with line-break syntax into line groups.
- **Function** `extract_per_pair_gaps` (Line 322) — Extract per-pair gap values from [N] tokens in a Responsa query.
- **Function** `generate_tabular_syntax` (Line 364) — Generate Responsa syntax string from tabular builder state.
- **Function** `_tokenize_responsa_query` (Line 462) — Split a Responsa query into tokens, respecting parentheses as grouping.
- **Function** `_parse_single_token` (Line 492) — Parse a single Responsa query token into a ResponsaComponent.
- **Function** `expand_grammatical_prefixes` (Line 621) — Expand a Hebrew word with all grammatical prefix combinations.
- **Function** `expand_judeo_arabic` (Line 643) — Expand a Judeo-Arabic word with definite article and preposition forms.
- **Function** `expand_grammatical_suffixes` (Line 702) — Expand a Hebrew word with all grammatical suffix combinations.
- **Function** `expand_plene_defective` (Line 730) — Generate plene/defective spelling variants of a Hebrew word.
- **Function** `_count_expanded_terms` (Line 777) — Estimate the total number of expanded terms for a set of Responsa components.
- **Function** `_apply_explosion_guard` (Line 835) — Apply the explosion guard to prevent combinatorial blowup in Responsa queries.
- **Function** `_expand_inline_alternation` (Line 977) — Expand an inline alternation pattern into a regex.

## shared/joins_manager.py

- **Class** `JoinsManager` (Line 25) — Manages fragment joins with offline-first caching.
    - Method `__init__` (Line 39) — Initialize the joins manager.
    - Method `_get_default_data` (Line 49) — Return the default data structure.
    - Method `_normalize_shelfmark` (Line 60) — Normalize shelfmarks using the canonical module-level function.
    - Method `load` (Line 64) — Load joins from local cache file.
    - Method `save` (Line 83) — Save joins to local cache file.
    - Method `_index_join` (Line 92) — Add a join to the normalized index (by shelfmark and document_id).
    - Method `_unindex_join` (Line 117) — Remove a join from the normalized index.
    - Method `get_joins_for_shelfmark` (Line 137) — Get all joins involving a shelfmark from local cache.
    - Method `get_connected_fragments` (Line 153) — Get all fragments connected to this shelfmark (BFS through joins).
    - Method `get_connected_fragments_by_id` (Line 216) — Get all fragments connected to this document_id (sys_id).
    - Method `has_joins_by_id` (Line 290) — Quick check if a document_id has any joins.
    - Method `has_joins` (Line 296) — Quick check if a shelfmark has any joins.
    - Method `get_join_count` (Line 301) — Get count of joins for a shelfmark.
    - Method `start_background_sync` (Line 308) — Start background sync thread.
    - Method `stop_background_sync` (Line 318) — Stop background sync thread.
    - Method `_sync_loop` (Line 324) — Background sync — runs once at startup only.
    - Method `sync_with_server` (Line 334) — Sync joins with server. Called in background.
    - Method `_fetch_all_joins` (Line 380) — Fetch all joins from server with pagination.
    - Method `_process_pending_operations` (Line 420) — Process any pending create/delete operations.
    - Method `create_join_local` (Line 469) — Create a join locally and queue for server sync.
    - Method `delete_join_local` (Line 511) — Delete a join locally and queue for server sync.
    - Method `get_all_shelfmarks_with_joins` (Line 542) — Get list of all shelfmarks that have joins (for autocomplete).

## shared/lists_manager.py

- **Function** `_tr` (Line 25) — Translate text if current language is Hebrew.
- **Class** `ListsManager` (Line 39) — Manages personal lists (starred/saved manuscripts) with tags and notes.
    - Method `__init__` (Line 68) — Initialize the lists manager.
    - Method `_get_default_data` (Line 74) — Return the default data structure.
    - Method `load` (Line 105) — Load lists from file.
    - Method `save` (Line 148) — Save lists to file.
    - Method `clear_all` (Line 169) — Clear all lists and reset to default state. Used after migration.
    - Method `enable_cloud_sync` (Line 176) — Enable cloud sync for the given user (call after login).
    - Method `disable_cloud_sync` (Line 195) — Disable cloud sync (call on logout).
    - Method `sync_from_cloud` (Line 204) — Pull lists from cloud and merge with local data.
    - Method `is_sync_available` (Line 215) — Check if cloud sync is available (user logged in, network ok).
    - Property `_last_sync` (Line 227) — Get timestamp of last sync (for debouncing).
    - Method `sync_to_cloud` (Line 236) — Push local lists to cloud.
    - Method `get_cloud_lists_preview` (Line 247) — Get preview of cloud lists without syncing (for dialog display).
    - Method `get_local_lists_summary` (Line 258) — Get summary of local lists for dialog display.
    - Method `get_all_lists` (Line 274) — Get all lists sorted alphabetically (system lists have special handling).
    - Method `get_deleted_lists` (Line 303) — Get soft-deleted lists (trash view).
    - Method `_get_list_item_count` (Line 317) — Get the number of items in a list.
    - Method `create_list` (Line 328) — Create a new list. Returns the list ID.
    - Method `update_list` (Line 355) — Update list properties.
    - Method `update_list_project` (Line 372) — Assign a list to a project (or clear project).
    - Method `create_project` (Line 388) — Create a new project. Returns the project ID.
    - Method `get_projects` (Line 415) — Get projects sorted by name.
    - Method `update_project` (Line 429) — Update a project's properties.
    - Method `delete_project` (Line 439) — Delete a project and optionally its lists.
    - Method `_get_next_project_color` (Line 462)
    - Method `apply_list_layout` (Line 474) — Apply list ordering and project assignments in one save.
    - Method `delete_list` (Line 498) — Soft-delete a list (move to trash).
    - Method `restore_list` (Line 533) — Restore a soft-deleted list from trash.
    - Method `permanently_delete_list` (Line 546) — Permanently delete a list (no recovery).
    - Method `empty_trash` (Line 550) — Permanently delete all soft-deleted lists.
    - Method `duplicate_list` (Line 559) — Duplicate a list with all its items.
    - Method `merge_lists` (Line 581) — Merge source list into target list.
    - Method `find_duplicate_lists` (Line 603) — Find all duplicate lists (same name) and return info for resolution.
    - Method `merge_duplicate_group` (Line 656) — Merge a group of duplicate lists into one.
    - Method `auto_merge_duplicate_group` (Line 707) — Automatically merge a duplicate group using heuristics.
    - Method `restore_project_hierarchy` (Line 729) — Restore project hierarchy for orphaned lists by color matching.
    - Method `_build_item_id` (Line 758)
    - Method `add_item` (Line 765) — Add an item to a list. Returns True if added, False if already exists.
    - Method `add_items_bulk` (Line 809) — Add multiple items to a list at once.
    - Method `update_item` (Line 862) — Update an item's properties.
    - Method `remove_item_from_list` (Line 890) — Remove an item from a specific list.
    - Method `move_items_to_list` (Line 908) — Move items from one list to another.
    - Method `get_items_in_list` (Line 923) — Get all items in a list with their metadata.
    - Method `get_item` (Line 948) — Get a single item's data.
    - Method `is_item_in_any_list` (Line 958) — Check if an item is in any list (excluding recent).
    - Method `get_item_lists` (Line 962) — Get list of lists an item belongs to.
    - Method `add_to_recent` (Line 970) — Add an item to the recently viewed list.
    - Method `get_all_tags` (Line 1014) — Get all tags for autocomplete.
    - Method `add_tag_to_items` (Line 1018) — Add a tag to multiple items.
    - Method `export_list` (Line 1038) — Export a list to a dictionary suitable for JSON serialization.
    - Method `import_list` (Line 1076) — Import a list from exported data. Returns (list_id, imported_count, unidentified_count).
    - Method `shelfmark_sort_key` (Line 1124) — Sort key for shelfmarks that handles dots correctly.
    - Method `get_items_sorted` (Line 1142) — Get items in a list, sorted by the specified field.
    - Method `get_item_copy_text` (Line 1167) — Generate text for copying item info.

## shared/browse_map_utils.py

- **Function** `normalize_shelfmark` (Line 123) — Normalize shelfmarks for consistent matching across the codebase.
- **Function** `natural_sort_key` (Line 174) — Sort strings containing numbers naturally (e.g. 'Item 2' < 'Item 10').
- **Function** `get_library_display` (Line 180) — Return library name for display.
- **Function** `_get_library_prefix_aliases` (Line 211) — Build sorted list of (lowercase_prefix, ) for library name stripping.
- **Function** `_strip_library_prefix` (Line 243) — Strip a leading library name/code prefix from a shelfmark query.
- **Function** `_load_ie_volume_map` (Line 265) — Load IE volume map from JSON file (cached after first call).
- **Function** `_extract_ie_from_header` (Line 310) — Extract IE identifier from a browse_map entry's full_header.
- **Function** `_repair_missing_ie_pages` (Line 316) — Repair browse_map for multi-IE manuscripts where pages from non-primary IEs
- **Function** `dedupe_browse_map` (Line 448) — Deduplicate browse_map pages and tag each page with its IE.

## shared/text_normalize.py

- **Function** `strip_nikud` (Line 17) — Remove Hebrew vowel marks (nikud) and cantillation marks from text.
- **Function** `strip_search_diacritics` (Line 44) — Strip combining diacritical marks, apostrophe variants, and geresh/gershayim from search text.

## shared/metadata_manager.py

- **Function** `_warn_bridge_import_failed` (Line 37) — Log shelfmark_bridge import failure at WARNING once per process (Gemini LOW).
- **Function** `_parse_cudl_label` (Line 48) — Parse a CUDL canvas label → (folio_num:int|None, folio_side:'r'|'v'|None).
- **Function** `_get_crossref_service` (Line 85) — Lazy accessor for the NLI crossref sidecar service (desktop use).
- **Function** `_get_fjms_service` (Line 102) — Lazy accessor for the FJMS enrichment sidecar service.
- **Class** `_BoundedLRUCache` (Line 137) — Thread-safe, bounded, dict-like LRU.
    - Method `__init__` (Line 148)
    - Method `_evict_locked` (Line 158)
    - Method `__contains__` (Line 164)
    - Method `__getitem__` (Line 168)
    - Method `__setitem__` (Line 174)
    - Method `get` (Line 180)
    - Method `items` (Line 187)
    - Method `keys` (Line 191)
    - Method `values` (Line 195)
    - Method `__len__` (Line 199)
    - Method `__iter__` (Line 203)
    - Method `__reduce__` (Line 207)
    - Property `maxsize` (Line 214)
- **Class** `MetadataManager` (Line 228)
    - Method `_make_session` (Line 229)
    - Method `__init__` (Line 233)
    - Method `start_background_loading` (Line 255) — Start loading heavy metadata resources (CSV, Maps) in background.
    - Method `_load_small_caches` (Line 260)
    - Method `_load_heavy_caches_bg` (Line 279)
    - Method `_load_csv_bank` (Line 284) — Load the massive CSV file into memory for instant lookup.
    - Method `get_meta_for_id` (Line 377)
    - Method `get_library_for_id` (Line 413) — Get library code for a system ID.
    - Method `get_shelfmark_from_header` (Line 429)
    - Method `save_caches` (Line 442)
    - Method `get_part_for_folio` (Line 452) — Get the Part ID for a given system ID.
    - Method `get_folios_for_part` (Line 456) — Get all system IDs (folios) belonging to a Part.
    - Method `get_part_metadata` (Line 460) — Get full metadata for a Part (Oxford Neubauer).
    - Method `get_part_images` (Line 464) — Get all images for a Part.
    - Method `is_part_id` (Line 468) — Check if an identifier is a Part ID.
    - Method `parse_part_identifier` (Line 472) — Parse an identifier that might be a Part. Returns (part_id, is_part).
    - Method `get_part_autocomplete_list` (Line 476) — Get list of Parts for autocomplete.
    - Method `get_meta_with_part` (Line 480) — Get shelfmark, title, and Part info for a system ID.
    - Method `_build_file_map_background` (Line 506)
    - Method `extract_unique_id` (Line 523) — Robust extraction of Unique ID.
    - Method `parse_header_smart` (Line 546)
    - Method `parse_full_id_components` (Line 559) — Parse header into components regardless of order or separators.
    - Method `fetch_nli_data` (Line 596)
    - Method `_bounded_cache_get` (Line 639)
    - Method `_bounded_cache_set` (Line 647)
    - Method `_timestamp_cache_recent` (Line 659)
    - Method `_timestamp_cache_set` (Line 670)
    - Method `get_runtime_cache_stats` (Line 679) — Return lightweight cache sizes for the memstat diagnostic endpoint.
    - Method `fetch_iiif_manifest` (Line 690) — Fetch and parse IIIF manifest for physical description, attribution, and image labels.
    - Method `fetch_marc_data` (Line 803) — Fetch and parse MARC XML for bibliography, notes, and extended metadata.
    - Method `enrich_metadata` (Line 979) — Fetch extended metadata (IIIF/MARC), build Image List, and merge into cache.
    - Method `fetch_volume_manifest` (Line 1267) — Lightweight manifest-only fetch for volume switches (no MARC, no FJMS, no crossref).
    - Method `fetch_external_iiif_data` (Line 1282) — Generic handler to fetch external IIIF data.
    - Method `_fetch_single_worker` (Line 1362)
    - Method `_extract_fl_ids` (Line 1492)
    - Method `_resolve_thumbnail` (Line 1501)
    - Method `get_rosetta_fallback_url` (Line 1523) — Construct a fallback URL for Rosetta stream if IIIF fails.
    - Method `_fetch_fl_ids` (Line 1532)
    - Method `get_thumbnail` (Line 1576)
    - Method `batch_fetch_shelfmarks` (Line 1597) — Populate metadata cache.
    - Method `search_by_meta` (Line 1636) — Search for system IDs where the specified field matches the query.
    - Method `_normalize_shelfmark` (Line 1767) — Normalize shelfmarks using the canonical module-level function.
    - Method `_iter_shelfmark_sources` (Line 1771) — Yield shelfmark candidates from CSV bank and cached metadata.
    - Method `_get_shelfmark_index` (Line 1789) — Build or return cached pre-normalized shelfmark index.
    - Method `resolve_system_by_shelfmark` (Line 1822) — Resolve a system ID by shelfmark, ignoring dots/slashes/spaces.
    - Method `get_display_data` (Line 1924)

## shared/indexer.py

- **Function** `_tr` (Line 38) — Translate text if current language is Hebrew.
- **Function** `_strip_brackets` (Line 51) — Remove all square brackets from *text*. Mirrors genizah_core._strip_brackets.
- **Class** `Indexer` (Line 56) — Create or update the Tantivy index and keep browse maps in sync.
    - Method `__init__` (Line 58)
    - Method `_extract_position_fields` (Line 62) — Extract position-search fields from content text.
    - Method `_validate_position_match` (Line 90) — Post-filter: validate that a regex match occurs at the expected text position.
    - Method `_validate_line_break_match` (Line 142) — Post-filter: validate that a document satisfies line-break constraints.
    - Method `create_index` (Line 229)
    - Method `_add_continuous_document` (Line 426) — Add an aggregated document (system/part) with boundary metadata.
    - Method `_add_chunked_continuous_documents` (Line 470) — Split a large aggregated document into multiple chunks to avoid massive allocations.

## shared/search_engine.py

- **Function** `_tr` (Line 49) — Translate text if current language is Hebrew.
- **Function** `_set_last_responsa_downgrade` (Line 80) — Record a Responsa downgrade signal on the current thread.
- **Function** `_consume_last_responsa_downgrade` (Line 89) — Read-and-clear the per-thread downgrade signal. Returns None if unset.
- **Function** `_set_last_responsa_downgrade_meta` (Line 106) — Phase 81A — record a structured per-flag cascade outcome.
- **Function** `_consume_last_responsa_downgrade_meta` (Line 117) — Phase 81A — read-and-clear the structured cascade outcome.
- **Function** `_count_unique_chunks` (Line 133) — Count distinct source-chunk contents from a chunk_hits list.
- **Class** `_ChunkPlan` (Line 152) — Per-chunk precomputed plan for search_composition_logic (SEED-011 dedup).
- **Class** `_LabChunkPlan` (Line 176) — Per-chunk fingerprint plan for lab_composition_search (SEED-011 dedup).
- **Function** `_make_flex_spacing_pattern` (Line 203) — Create a flex-spacing regex pattern for a term.
- **Function** `_build_wildcard_regex` (Line 217) — Build a regex pattern for a component with wildcard type.
- **Function** `_add_bracket_variants` (Line 268) — Return bracket-adorned variants of *term* for Tantivy OR expansion.
- **Function** `_query_has_brackets` (Line 293) — Return True if *query_str* contains literal square brackets.
- **Function** `_strip_brackets` (Line 303) — Remove all square brackets from *text*.
- **Function** `_index_has_field` (Line 308) — SEED-006 compat gate: True if *index*'s schema defines *field_name*.
- **Function** `content_search_staleness_messages` (Line 332) — SEED-019 #28: human-readable staleness diagnostics for the SEED-006
- **Function** `make_mark_tolerant_pattern` (Line 378) — Insert optional combining mark matchers between characters of an escaped regex term.
- **Class** `SearchEngine` (Line 404) — Run searches, build queries, and provide browsing utilities.
    - Method `__init__` (Line 406)
    - Method `attach_my_library_tab` (Line 441) — Phase 97 R-01: attach a weakref to the MyLibraryTab for is_searchable gate.
    - Method `close_local_searcher` (Line 450) — Phase 97 R-02 LD-5: close BOTH main + LAB searcher + index handles.
    - Method `close_local_lab_searcher` (Line 464) — Phase 97 R-02 LD-5 (LAB-only variant used by rebuild_lab_index_atomic).
    - Method `_open_local_searcher` (Line 469) — Phase 97 R-02: open LOCAL side-index with atomic-rebuild recovery.
    - Method `reload_local_indexes` (Line 565) — HIGH-1 review fix: reopen LOCAL Tantivy searchers (main + LAB) so newly
    - Method `reload_local_lab_index` (Line 579) — HIGH-1 review fix (LAB-only narrow reload). Reopens self.local_lab_searcher
    - Method `_current_lab_weights_hash` (Line 628) — Compute hash of current LAB weights for D-38 staleness check.
    - Method `_check_local_lab_freshness` (Line 670) — Return True if LOCAL LAB index is fresh; False if stale or missing.
    - Method `rebuild_local_lab_index` (Line 693) — Trigger LOCAL LAB rebuild via LocalIndexer, passing fingerprint helpers
    - Method `_compute_fingerprint_dyn` (Line 736) — Compute fingerprint_dyn for a content string using the given rank map.
    - Method `_compute_fingerprint_static` (Line 742) — Compute static fingerprint for a content string using HEBREW_FREQ.
    - Method `_normalize_text` (Line 748) — Normalize content for the text_normalized field (W5 Option C callback).
    - Method `_query_local_index` (Line 761) — Query the LOCAL side-index. Returns [] if local_searcher is None (D-37).
    - Method `_build_local_result_dict` (Line 858) — Construct a result row from a LOCAL Tantivy doc per D-34 shape.
    - Method `_rrf_merge` (Line 950) — Reciprocal Rank Fusion merger (D-08 Codex P0). BM25 scores from two
    - Method `_build_fl_id_index` (Line 983) — Build FL ID -> (sys_id, page_idx) index from browse_map. Called in background thread.
    - Method `start_fl_id_index_build` (Line 1003) — Start building FL ID index in background. Non-blocking.
    - Method `_build_fl_id_index_thread` (Line 1011)
    - Method `format_snippet` (Line 1020) — Format snippet with highlighted matches, safely escaping HTML.
    - Method `close_index` (Line 1042) — Release Tantivy index and searcher to unlock files (required before rebuild on Windows).
    - Method `reload_index` (Line 1049)
    - Method `index_staleness_report` (Line 1074) — SEED-019 #28: queryable verdict on the SEED-006 ``content_search`` compat
    - Method `_warn_if_local_index_stale` (Line 1105) — SEED-019 #28: log a remediation warning when a freshly-opened LOCAL
    - Method `_load_browse_map` (Line 1120) — Load the browse map, deduplicate it, and persist corrections if needed.
    - Method `_get_or_compute_variants` (Line 1155) — Pre-compute variants at the larger limit for each search term.
    - Method `_build_local_responsa_query_and_regex` (Line 1175) — Build a Tantivy query string + filter regex for a Responsa query, for
    - Method `build_tantivy_query` (Line 1301)
    - Method `build_regex_pattern` (Line 1463)
    - Method `highlight` (Line 1591)
    - Method `_highlight_by_span` (Line 1618) — Return a highlighted snippet around a specific span.
    - Method `_parse_boundaries` (Line 1637)
    - Method `_map_span_to_pages` (Line 1652) — Return page overlaps and primary page for a match span.
    - Method `_get_field` (Line 1684)
    - Method `_get_best_text_for_id` (Line 1690) — Find the first page with meaningful text for a given System ID.
    - Method `parse_query_syntax` (Line 1739) — Parses search syntax prefix from query string.
    - Method `_expand_responsa_component` (Line 1784) — Expand a single ResponsaComponent through the full Responsa expansion pipeline.
    - Method `_build_line_break_regex` (Line 1831) — Build a regex pattern for line-break search.
    - Method `_execute_line_break_search` (Line 1928) — Execute a line-break search using | syntax.
    - Method `_execute_metadata_search` (Line 2157) — Search by title or shelfmark via csv_bank. Returns results even for metadata-only records.
    - Method `execute_search` (Line 2234)
    - Method `_deduplicate` (Line 2756)
    - Method `search_composition_logic` (Line 2763) — Scans composition chunks against the index.
    - Method `group_pages_by_manuscript` (Line 3337) — Aggregate individual page results into manuscript-level items.
    - Method `group_composition_results` (Line 3450)
    - Method `get_full_text_by_id` (Line 3554)
    - Method `get_full_manuscript` (Line 3563) — Fetch ALL pages for a system ID, sorted by page number.
    - Method `_get_metadata_only_browse_page` (Line 3585) — Build a minimal browse result from csv_bank for records with no Tantivy text.
    - Method `get_browse_page` (Line 3632)
    - Method `get_local_browse_page` (Line 3730) — Phase 96 NEW-2: LOCAL analog to `get_browse_page` (folio nav for LOCAL files).
    - Method `get_browse_page_by_fl` (Line 3862)
    - Method `get_adjacent_sys_id_by_file_order` (Line 3933) — Returns the next/prev system ID based on the order in Transcriptions.txt.

## shared/lab_engine.py

- **Class** `LabEngine` (Line 39)
    - Method `__init__` (Line 44)
    - Method `_close_index` (Line 81)
    - Method `_ensure_lab_tokenizers` (Line 87) — Register analyzers safely.
    - Method `_reload_lab_index` (Line 98) — Loads index with heavy debug logging.
    - Method `reload_local_lab_index` (Line 124) — Reopen the LOCAL LAB side-index against the current Config.LOCAL_LAB_INDEX_DIR.
    - Method `_current_lab_weights_hash` (Line 171) — Compute hash of current LAB weights for D-38 staleness check.
    - Method `_check_local_lab_freshness` (Line 187) — Return True if LOCAL LAB index is fresh; False if stale or missing.
    - Method `lab_index_normalize` (Line 212)
    - Method `rebuild_lab_index` (Line 215)
    - Method `_create_lab_query` (Line 341) — Helper to construct the Tantivy query object based on settings.
    - Method `_execute_batched_search` (Line 376) — Executes a Tantivy search in memory-safe batches.
    - Method `_get_term_weight` (Line 429) — Calculates importance using User Configurable Stop-Word scores.
    - Method `_calculate_match_metrics` (Line 455) — Calculates score with STRICT FREQUENCY CAP & SEQUENTIAL ORDER.
    - Method `_generate_highlighted_snippet` (Line 597) — Generates a snippet with asterisk markers (*text*) for highlighting.
    - Method `lab_search` (Line 656) — Lab Mode (fingerprint) word search.
    - Method `lab_composition_search` (Line 855) — Scans a composition using Lab Mode.
    - Method `_is_word_too_common` (Line 1474) — Check existing index stats to see if a word is essentially a stop-word.
    - Method `_is_phrase_statistically_weak` (Line 1495) — Returns True if the phrase consists ONLY of extremely common words.

## shared/lab_settings.py

- **Class** `LabSettings` (Line 18) — Manages configuration for the Lab Mode, including scoring weights.
    - Method `__init__` (Line 20)
    - Method `load` (Line 67)
    - Method `save` (Line 112)

## desktop/settings_dialogs.py

- **Class** `LabScoringDialog` (Line 67) — Configuration for Lab Mode Scoring (Advanced).
    - Method `__init__` (Line 69)
    - Method `save_and_close` (Line 152)
- **Class** `SearchSettingsDialog` (Line 168) — Settings for Standard Search - Variant configuration and custom pairs.
    - Method `__init__` (Line 170)
    - Method `save_and_close` (Line 294)
- **Class** `HelpDialog` (Line 324) — Display HTML help content from the bundled Help.html file with graceful fallback.
    - Method `__init__` (Line 326)
    - Method `_load_content` (Line 343)
- **Class** `TabularQueryBuilderDialog` (Line 388) — Tabular Query Builder for Responsa syntax composition.
    - Method `__init__` (Line 395)
    - Method `_setup_ui` (Line 423) — Build the complete dialog UI.
    - Method `_initialize_components` (Line 567) — Create the initial 2 components with distance spinner between them.
    - Method `_create_component` (Line 575) — Create a component card (QFrame with word inputs).
    - Method `_create_distance_spinner` (Line 675) — Create a distance spinner between components pair_index and pair_index+1.
    - Method `_update_add_word_visibility` (Line 718) — Show/hide the + button based on how many word slots are visible.
    - Method `_update_add_component_visibility` (Line 726) — Show/hide the + Component button based on current count.
    - Method `_show_next_word` (Line 731) — Reveal the next hidden word input in the given component.
    - Method `_add_component` (Line 742) — Add a new component (up to max 4).
    - Method `_remove_component` (Line 753) — Remove a component (cannot go below 2).
    - Method `_on_scope_changed` (Line 801) — Toggle distance spinner/modifier visibility based on scope.
    - Method `_on_word_focus` (Line 821) — Handle focus on a word input -- update modifier checkboxes.
    - Method `_update_mod_indicator` (Line 845) — Update the modifier indicator label for a specific word.
    - Method `_on_modifier_changed` (Line 855) — Save modifier state to the active word's data.
    - Method `_on_word_text_changed` (Line 876) — Sync QLineEdit text back to component data and update preview.
    - Method `_update_preview` (Line 888) — Regenerate syntax from current state and update preview label.
    - Method `_clear_all` (Line 924) — Reset all inputs, modifiers, spinners, and components to initial state.
    - Method `_apply` (Line 975) — Generate final syntax and accept the dialog.
    - Method `get_syntax` (Line 980) — Return the generated Responsa syntax string.
    - Method `get_negated_words` (Line 985) — Return list of words marked for exclusion.
    - Method `eventFilter` (Line 990) — Catch focus events on word inputs to update modifier checkboxes.
- **Class** `SettingsDialog` (Line 1001) — Modal settings dialog with General and About tabs.
    - Method `__init__` (Line 1013)
    - Method `_on_cancel` (Line 1082) — Restore config snapshot and close.
    - Method `_build_general_tab` (Line 1088)
    - Method `_start_vs_download` (Line 1377) — Start downloading the full visual_similarity.db with robustness checks.
    - Method `_on_vs_download_progress` (Line 1397)
    - Method `_on_vs_download_complete` (Line 1405)
    - Method `_on_vs_download_error` (Line 1415)
    - Method `_build_about_tab` (Line 1423)
    - Method `_section_label` (Line 1556)

## desktop/ui_widgets.py

- **Class** `ShelfmarkTableWidgetItem` (Line 42) — Custom item for sorting shelfmarks by ignoring 'Ms.' prefix and case.
    - Method `__lt__` (Line 44)
- **Class** `CheckBoxHeader` (Line 49) — Custom HeaderView that draws a checkbox in the first section.
    - Method `__init__` (Line 53)
    - Method `get_checkbox_rect` (Line 66)
    - Method `paintSection` (Line 78)
    - Method `mousePressEvent` (Line 105)
    - Method `setChecked` (Line 152)
    - Method `_get_icon_rect` (Line 157)
    - Method `_draw_filter_icon` (Line 173)
    - Method `_draw_star_icon` (Line 202)
    - Method `set_filter_active` (Line 247)
    - Method `set_star_active` (Line 254)
    - Method `event` (Line 261)
- **Class** `HiddenScrollArea` (Line 286)
    - Method `__init__` (Line 287)
    - Method `_update_content` (Line 315)
    - Method `_center_on_match` (Line 332)
    - Method `wheelEvent` (Line 347)
    - Method `resizeEvent` (Line 357)
- **Class** `ListsTreeWidget` (Line 363)
    - Method `__init__` (Line 364)
    - Method `dropEvent` (Line 374)

## desktop/update_ui.py

- **Class** `UpdateNotificationBar` (Line 38) — A narrow notification bar at the top of the screen.
    - Method `__init__` (Line 44)
    - Method `show_update` (Line 82)
    - Method `on_download` (Line 89)
    - Method `on_dismiss` (Line 92)
- **Class** `WhatsNewBar` (Line 97) — A notification bar showing new features after a version update.
    - Method `__init__` (Line 103)
    - Method `show_whats_new` (Line 137)
    - Method `on_learn_more` (Line 141)
    - Method `on_dismiss` (Line 144)
- **Class** `WhatsNewDialog` (Line 149) — Dialog showing detailed What's New information.
    - Method `__init__` (Line 152)
- **Class** `UpdateProgressDialog` (Line 218) — Shows download progress and handles update installation.
    - Method `__init__` (Line 221)
    - Method `start_download` (Line 280) — Start the download process.
    - Method `on_progress` (Line 308) — Update progress bar with download progress.
    - Method `on_download_finished` (Line 326) — Handle download completion.
    - Method `execute_update` (Line 352) — Run the installer in silent mode (Windows only).
    - Method `on_cancel` (Line 417) — Handle cancel button click.
    - Method `closeEvent` (Line 432) — Handle dialog close event.

## Phase 145 — Passage-Matching Parallels Search (web beta)

`shared/passage_parallels.py` and `web/passage_assets.py`, added for the
`method='passage'` option on `POST /api/parallels` (see CLAUDE.md's Key
Files list and docs/SEARCH_API.md for the feature). `web/pages/parallels.py`
gained a method selector beside the pre-existing Lab Mode toggle
(`passage_mode` checkbox, `on_passage_mode_change`) but is not separately
indexed here (too large; grep it directly, per this file's existing
convention for `web/pages/search.py` / `web/pages/browse.py`).

## shared/passage_parallels.py

- **Function** `_derive_uid` (Line 145)
- **Function** `_extract_sys_id` (Line 150)
- **Class** `_RegexSysIdParser` (Line 155) — Minimal `UidComponentParser` (shared/parallels_service.py's Protocol)
    - Method `parse_full_id_components` (Line 166)
- **Function** `_highlight_span` (Line 170) — Build a `*match*`-marked snippet around normalized-stream span
- **Class** `PageTextFetcher` (Line 202) — Structural type for the injected text source (SEED-016 #3 style).
    - Method `get_full_text_by_header` (Line 214)
- **Class** `PassageSearcher` (Line 219) — A `CompositionSearcher` (shared/parallels_service.py) backed by the
    - Method `__post_init__` (Line 235)
    - Method `search_composition_logic` (Line 241) — Same parameter names/order as
    - Method `_is_source_text_filtered` (Line 357) — True when the query-side text of ANY span matched on `hit` also
    - Method `_render_highlights` (Line 378) — Fill in `text` / `source_ctx` / `chunk_hits` for ONE row (either a

## web/passage_assets.py

- **Class** `_PassageState` (Line 71)
- **Function** `load_passage_state` (Line 80) — Open + validate the passage index ONCE at startup.
- **Function** `passage_available` (Line 116) — The ONE predicate any future passage-matching surface must gate on.
- **Function** `get_passage_searcher` (Line 127) — A fresh ``PassageSearcher``, or ``None`` when unavailable.
