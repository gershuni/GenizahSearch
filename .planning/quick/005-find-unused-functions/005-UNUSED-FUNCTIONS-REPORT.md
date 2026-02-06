# Unused Functions Report

Generated: 2026-02-06
Tool: vulture 2.14 (--min-confidence 60) + manual grep cross-reference
Scope: All Python source files (excluding dist/, venv/, __pycache__/, .git/)

## Summary

- **Definitely unused:** 34 functions/methods/classes across 14 files
- **Possibly unused:** 18 functions/methods across 8 files
- **Unclear / False positives filtered:** ~201 findings (UI attributes, variables, framework callbacks)
- **Total lines of dead code (estimated):** ~1,800 lines
- **Entire files that may be unused:** 2 files (~498 lines)

---

## Definitely Unused (Safe to Remove)

These have zero references anywhere in the codebase outside their own definition. Not framework callbacks, not dynamic dispatch.

### corrections_client.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `get_rest_api_client()` | 1611 | Zero references in entire codebase. Old REST API factory function (FastAPI backend removed Jan 2026). |

### corrections_ui.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `import QStatusBar` | 9 | Imported but never used in file. |
| `import QAction` | 19 | Imported but never used in file. |
| `TextEditorDialog` (class) | 2885 | Defined here (~218 lines). Imported in genizah_app.py line 71 but never instantiated or referenced beyond the import. |
| `CommunityHubWidget` (class) | 3103 | Zero references outside definition (~131 lines). Never imported or instantiated anywhere. |

### genizah_app.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `import QSize` | 33 | Imported but never used. |
| `import QTextDocument` | 34 | Imported but never used. |
| `import QTransform` | 34 | Imported but never used. |
| `import ExternalResourceThread` | 57 | Imported but never used (class defined in gui_threads.py line 328, also unused there). |
| `import TextEditorDialog` | 65 | Imported but never used in this file. |
| `rotate_view()` | 1352 | Zero references. Only the definition exists. |
| `_show_my_corrections_dialog()` | 4835 | Zero references outside definition. |
| `_on_browse_link_clicked()` | 6810 | Zero references outside definition. |
| `lists_quick_view_item()` | 8576 | Zero references outside definition. |
| `lists_browse_item()` | 8625 | Zero references outside definition. |
| `lists_copy_item_info()` | 8709 | Zero references outside definition. |
| `_build_comp_preview_label()` | 13595 | Zero references outside definition. |
| `_add_single_node_to_tree()` | 14180 | Zero references outside definition. |
| `_trigger_lazy_metadata_fetch()` | 14274 | Zero references outside definition. |
| `show_comp_detail()` | 14594 | Zero references outside definition. |
| `_refresh_comp_tree_metadata()` | 14684 | Zero references outside definition. |
| `_format_comp_entry()` | 14728 | Zero references outside definition. |
| `_add_single_comp_node()` | 15541 | Zero references outside definition. |

### genizah_core.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `import get_top_pairs` | 49 | Imported from unified_variants but never called (also has fallback at line 53). |
| `get_top_pairs()` (fallback) | 53 | Fallback function, never called. |
| `get_variant_level()` | 2251 | Zero references outside definition. |
| `get_max_variant_pairs()` | 2255 | Zero references outside definition. |
| `get_image_for_folio()` | 2731 | Zero references outside definition. |
| `get_all_images_for_part()` | 2768 | Zero references outside definition. |
| `get_meta_with_part()` | 3007 | Zero references outside definition. |
| Unreachable `return False` | 3759 | Dead code after if/elif/else block where all branches already return. |
| `get_joins_for_shelfmark()` | 5462 | Zero references outside definition. |
| `has_joins_by_id()` | 5615 | Zero references outside definition. |
| `has_joins()` | 5621 | Zero references outside definition. |
| `get_join_count()` | 5626 | Zero references outside definition. |
| `stop_background_sync()` | 5643 | Zero references outside definition. |
| `get_all_shelfmarks_with_joins()` | 5883 | Zero references outside definition. |

### gui_threads.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `ExternalResourceThread` (class) | 328 | Zero references outside definition. Imported in genizah_app.py but that import is also unused. |

### lists_sync.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `sync_item_to_cloud()` | 688 | Zero references outside definition. |
| `delete_list_from_cloud()` | 746 | Zero references outside definition. |
| `delete_item_from_cloud()` | 770 | Zero references outside definition. |

### sefaria_utils.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `get_categories()` | 114 | Zero references outside definition. |

### unified_variants.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `get_top_pairs()` | 25822 | Zero references (genizah_core imports it but never calls it). |
| `get_pairs_above_frequency()` | 25826 | Zero references in entire codebase. |

### web/auth_state.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `get_api_base()` | 423 | Zero references. Likely leftover from old FastAPI backend. |
| `api_call()` | 428 | Zero references. Likely leftover from old FastAPI backend. |
| `get_headers()` | 119 | Zero references outside definition. |

### web/components/typography.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `h5()` | 55 | Zero imports. All files import only h1, h2, h3, h4. |
| `h6()` | 59 | Zero imports. All files import only h1, h2, h3, h4. |

### web/pages/browse.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `fit_width()` | 1140 | Defined but never called or assigned as callback. |
| `fit_height()` | 1145 | Defined but never called or assigned as callback. |
| `rotate_reset()` | 1164 | Defined but never called or assigned as callback. |

### web/pages/corrections.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `vote_val` variable | 242 | Assigned but never read (100% confidence). |

### web/pages/discoveries.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `highlight_diff()` | 101 | Defined but never called anywhere. |

### web/services.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `SearchResult` (class) | 33 | Zero references outside definition. |
| `ImageInfo` (class) | 97 | Zero references outside definition. |
| `build_iiif_image_url()` | 125 | Zero references outside definition. |
| `init_service()` | 494 | Zero references outside definition. |

### web/state.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `import Union` | 1 | Imported but never used. |

### web/supabase_client.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `import urlencode` | 16 | Imported but never used. |
| `reset_client()` | 46 | Zero references outside definition. |

---

## Possibly Unused (Review Before Removing)

These have limited references (only tests, only same class, or appear to be leftovers) but might still be needed.

### corrections_client.py / supabase_corrections_client.py (Interface Methods)

These methods exist in both the old REST client and the Supabase client. They define a consistent interface, but none are currently called from the application code. Some have test references.

| Function/Method | Lines (corrections_client / supabase) | Evidence |
|-----------------|---------------------------------------|----------|
| `clear_cache()` | 322 / 507 | Only defined, never called. Also in genizah_core.py:2428. |
| `reset_offline_status()` | 374 / 544 | Only defined, never called. |
| `get_correction_stats()` | 739 / 1496 | Only defined, never called from app code. |
| `react_to_comment()` | 832 / 1787 | Only referenced in tests/test_corrections_api.py. |
| `review_correction()` | 1227 / 1642 | Only defined, never called. |
| `get_document_stats()` | 1245 / 1676 | Only referenced in tests/test_corrections_api.py. |
| `get_corrected_text()` | 1253 / 1693 | Only defined, never called. |
| `record_document_view()` | 1319 / 1708 | Only defined, never called. |
| `get_leaderboard()` | 1328 / 1712 | Only referenced in tests/test_corrections_api.py. |
| `get_connected_fragments_quick()` | 1428 / 1320 | Only defined, never called. |
| `get_join_by_id()` | 1472 / 1735 | Only defined, never called. |
| `update_join()` | 1489 / 1761 | Only defined, never called from app (app uses `_update_joins_dropdown` instead). |

**Note:** These may represent planned/future features (leaderboard, community reactions, corrections review). Removing them would break the interface contract if these features are later implemented.

### web/document_service.py (Service Layer - Future API)

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `get_fragments_for_document()` | 87 | Only used in tests. Part of documented service API (STATE.md). May be needed in Phase 6+. |
| `get_transcription_for_document()` | 116 | Only used in tests. Part of documented service API. |
| `get_document_metadata()` | 148 | Only used in tests. Part of documented service API. |
| `get_editions_for_document()` | 367 | Only defined. Convenience wrapper for `get_sources_for_document`. |
| `get_translations_for_document()` | 396 | Only defined. Convenience wrapper for `get_sources_for_document`. |

**Note:** These are part of the documented service layer in STATE.md. Phase 6 (Metadata Display) and beyond will likely use them. Keep until after Phase 6.

### web/supabase_client.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `get_recent_items()` | 517 | Defined here, imported in web/user_lists.py line 26, but that import is unused (also flagged). |

### web/user_lists.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `import get_recent_items` | 26 | Imported but never used in this file. |
| `move_list_to_project()` | 705 | Only defined. May be needed for future project management feature. |
| `get_lists_manager()` | 888 | Only self-referenced in module docstring test block (lines 11-13). |

### web/pages/parallels.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `load_all_sources_refs()` | 1051 | Only defined. May have been replaced by a different loading pattern. |
| `show_add_to_list_dialog_parallel()` | 1703 | Only defined. Likely replaced by a shared dialog component. |

### web/pages/search.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `prepend_to_query()` | 715 | Only defined. Was likely used for filter chip click handlers. |

### web/components/version_selector.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `has_editions_for_page()` | 122 | Only defined. Helper function that may have been inlined. |

### genizah_app.py

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `open_viewer()` | 4040 | Only defined here. web_pilot.py has its own `open_viewer` (different function). |
| `_export_list()` | 8986 | Defined and calls `_export_list_format()`, but nothing calls `_export_list()` itself (all callers go directly to `_export_list_format()`). |

---

## Unclear (Needs Human Decision)

These may be called dynamically or through Qt/NiceGUI framework mechanisms.

### Qt Completer Overrides (genizah_app.py, corrections_ui.py)

| Function/Method | Line | Evidence |
|-----------------|------|----------|
| `splitPath()` | genizah_app.py:1019, corrections_ui.py:3477 | Qt QCompleter override. Called by Qt framework when completer processes input. **Keep - Qt framework calls these.** |
| `pathFromIndex()` | genizah_app.py:1022, corrections_ui.py:3480 | Qt QCompleter override. Called by Qt framework when completer resolves selection. **Keep - Qt framework calls these.** |

### NiceGUI Route Handlers (web/main.py)

All functions from lines 1799-1972 decorated with `@ui.page(...)` are **false positives**. NiceGUI registers them automatically. They are the URL route handlers.

| Function | Line | Evidence |
|----------|------|----------|
| `dashboard_page()` | 1799 | @ui.page('/') - Framework registered |
| `search_page_route()` | 1814 | @ui.page('/search') - Framework registered |
| `parallels_page_route()` | 1827 | @ui.page('/parallels') - Framework registered |
| `browse_page_route()` | 1840 | @ui.page('/browse') - Framework registered |
| `lists_page_route()` | 1853 | @ui.page('/lists') - Framework registered |
| `settings_page_route()` | 1866 | @ui.page('/settings') - Framework registered |
| `help_page_route()` | 1879 | @ui.page('/help') - Framework registered |
| `corrections_page_route()` | 1892 | @ui.page('/corrections') - Framework registered |
| `discoveries_page_route()` | 1905 | @ui.page('/discoveries') - Framework registered |
| `admin_page_route()` | 1918 | @ui.page('/admin') - Framework registered |
| `profile_page_route()` | 1931 | @ui.page('/profile') - Framework registered |
| `accessibility_page_route()` | 1944 | @ui.page('/accessibility') - Framework registered |
| `download_page_route()` | 1958 | @ui.page('/download') - Framework registered |
| `auth_callback_route()` | 1972 | @ui.page('/auth/callback') - Framework registered |

**Verdict: All false positives. Keep all of these.**

### NiceGUI/FastAPI Route Handlers (web/api.py)

Functions decorated with `@app.get(...)` or `@app.post(...)` are registered as HTTP endpoints.

| Function | Line | Status |
|----------|------|--------|
| `nli_image()` | 98 | **USED** - Called from JS via `/api/nli_image/{fl_id}` |
| `nli_image_by_sysid()` | 146 | **USED** - Called from JS in browse.py and search.py |
| `oxford_image()` | 221 | **USED** - Called from JS in browse.py and search.py |
| `oxford_image_url()` | 331 | Possibly unused - no frontend references found. Keep for now (debug utility). |
| `oxford_images_list()` | 386 | References `/api/oxford_image/` URLs internally - keep. |
| `oxford_debug()` | 420 | **Debug endpoint only.** No frontend references. Safe to remove. |
| `browse_debug()` | 444 | **Debug endpoint only.** No frontend references. Safe to remove. |
| `proxy_image()` | 487 | **No references** in frontend code. Possibly unused. |
| `export_excel()` | 532 | **Route handler** - Called via `/api/export/excel` from JS. Keep. |
| `export_word()` | 555 | **Route handler** - Called via `/api/export/word` from JS. Keep. |
| `oauth_callback()` | 690 | **Auth callback** - Called by OAuth flow redirect. Keep. |

### UI Attributes and Variables

The remaining ~150 vulture findings for `unused attribute` and `unused variable` are predominantly **false positives** from NiceGUI and PyQt6 patterns:

- **NiceGUI variables like `tab_general`, `tab_variants`, `tab_snippet`** - These are NiceGUI `ui.tab()` objects that must exist for `ui.tab_panels()` to work, even if the Python variable is never read again.
- **PyQt attributes like `rightToLeft`, `fill`, `summaryBelow`, `autofit`, `orientation`** - These are QFont/QBrush/QBoxLayout properties set on Qt objects.
- **Instance attributes like `browse_thumb_url`, `comp_summary`, `ext_canvases`** - These are state attributes set in `__init__` and read elsewhere (vulture can miss `self.x` reads from different methods).
- **Loop variables like `book_key`, `loaded_count`, `attempt`** - Tuple unpacking variables that are intentionally unused.

**Verdict: Skip these. They are framework patterns.**

---

## Entire Files That May Be Unused

### web/pages/viewer.py (222 lines)

- **Never imported** by any file in the codebase
- Contains `get_image_urls()`, `get_full_image_url()`, `format_text_html()`, `load_result()` - all only used within the file itself
- Appears to be an **old standalone viewer component** superseded by `browse.py`
- **Safe to remove entirely**

### web/pages/document.py (276 lines)

- **Never imported** by any file in the codebase
- Contains `create_document_page()` - never called
- No `@ui.page()` decorator - not even registered as a route
- Appears to be an **old document page** superseded by `browse.py`
- **Safe to remove entirely**

### Potentially Unused: corrections_client.py (1,611 lines)

- The old REST API client from before the FastAPI backend was removed (Jan 2026)
- Now serves as a **wrapper** that dynamically imports `supabase_corrections_client.py`
- Still imported by `genizah_app.py` and `corrections_ui.py`
- The actual implementation lives in `supabase_corrections_client.py`
- **Not safe to remove** without refactoring imports, but the REST-specific code within it is dead

---

## Recommendations

### Priority 1: Remove Dead Files (~500 LOC saved)

Remove these two files that are entirely unused:
- `web/pages/viewer.py` (222 lines)
- `web/pages/document.py` (276 lines)

### Priority 2: Remove Definitely Unused Functions (~800 LOC saved)

Focus on the largest clusters:
1. **genizah_app.py**: Remove ~10 unused methods (composition tree helpers, unused list methods)
2. **genizah_core.py**: Remove ~8 unused methods (joins helpers, image helpers, variant pair functions)
3. **corrections_ui.py**: Remove `TextEditorDialog` (218 lines) and `CommunityHubWidget` (131 lines)
4. **web/auth_state.py**: Remove `get_api_base()` and `api_call()` (FastAPI leftovers)
5. **web/services.py**: Remove `SearchResult`, `ImageInfo`, `build_iiif_image_url`, `init_service`

### Priority 3: Clean Up Unused Imports (~20 lines saved, but improves clarity)

- genizah_app.py: Remove imports for `QSize`, `QTextDocument`, `QTransform`, `ExternalResourceThread`, `TextEditorDialog`
- corrections_ui.py: Remove imports for `QStatusBar`, `QAction`
- web/state.py: Remove `Union` import
- web/supabase_client.py: Remove `urlencode` import
- web/user_lists.py: Remove `get_recent_items` import

### Priority 4: Review Before Removing

- **corrections_client.py / supabase_corrections_client.py interface methods**: 12 methods that define the API contract. If these features (leaderboard, reactions, review) are not planned, remove them. If planned for future, keep.
- **web/document_service.py functions**: 5 functions used only in tests. Keep if Phase 6+ will use them. Remove after Phase 6 if still unused.
- **Debug API endpoints** (`oxford_debug`, `browse_debug`, `proxy_image`): Safe to remove if not needed for development.

### Estimated Total Savings

| Category | Lines |
|----------|-------|
| Dead files (viewer.py, document.py) | ~498 |
| Unused classes (TextEditorDialog, CommunityHubWidget, etc.) | ~400 |
| Unused methods in genizah_app.py | ~300 |
| Unused methods in genizah_core.py | ~250 |
| Other unused functions | ~200 |
| Unused imports | ~20 |
| **Total estimated** | **~1,668** |

If corrections client interface methods are also removed: add ~600 lines across both files.
