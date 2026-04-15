# Phase 67: ResultDialog Dependency Manifest

**Derived by:** `build_manifest.py` (AST walker over `genizah_app.py`)
**Raw JSON:** `67-MANIFEST-raw.json` (389 total external names, 188 self.X attribute refs)
**Date:** 2026-04-15

This is the authoritative input for Plan 67-02. Every external name that ResultDialog
references from within `genizah_app.py` is classified below with an explicit disposition.

---

## Category (a): stdlib / Qt / third-party

These stay as top-level or inline imports in `desktop/result_dialog.py`. No movement needed.

| Sub-cat | Symbols |
|---------|---------|
| Qt | QColor, QComboBox, QDesktopServices, QDialog, QFont, QHBoxLayout, QInputDialog, QLabel, QLineEdit, QMenu, QMessageBox, QPalette, QPixmap, QPushButton, QSpinBox, QSplitter, QStyle, QTextBrowser, QTimer, QToolButton, QUrl, QVBoxLayout, QWidget, pyqtSignal, Qt |
| stdlib | html, json, re, threading, traceback, urllib |

## Category (b): genizah_core / gui_threads / shared.*

Top-level imports in `desktop/result_dialog.py` from their existing modules.

| Source module | Symbols |
|---------------|---------|
| genizah_core | CURRENT_LANG, `_lac`, get_library_display, get_volumes_for_sys_id, load_app_config, save_app_config, tr |
| gui_threads | EnrichMetadataThread, PGPSourceWorker, `_field_translation_cache` |
| shared.* (inline) | get_document_for_fragment, get_fjms_service, get_fragments_for_document, get_vs_service |

## Category (c): corrections_ui (permitted peer module)

Top-level or inline imports from `corrections_ui`. Permitted per D-06 deny-rule (peer modules allowed; only `from genizah_app` is denied at column 0).

| Symbols |
|---------|
| CommentDialog, CommentsViewerDialog, CorrectionsViewerDialog, JoinsDialog |

## Category (d): GenizahGUI members (accessed via self._app)

These stay in `genizah_app.py`. ResultDialog accesses them through `self._app.X` (after Plan 67-03 rename). No import needed -- they are attributes/methods on the parent GenizahGUI instance.

~33 members listed in 67-CONTEXT.md canonical_refs section. Sample: `_VS_SERVER_URL`, `_auto_select_pgp_edition`, `_browse_document_by_shelfmark`, `_build_fjms_catalog_html`, `_build_pgp_extended_info_html`, `corrections_client`, `joins_mgr`, `lists_mgr`, `meta_mgr`, `open_result_in_browse`, `send_result_to_composition`, `show_add_to_list_menu`, etc.

## Category (e): Co-resident symbols -- DISPOSITION TABLE

These 19 symbols currently resolve inside `genizah_app.py` and need an explicit destination.

| Symbol | Destination | Disposition | Rationale |
|--------|-------------|-------------|-----------|
| `ActionsHoverWidget` | `desktop/widgets.py` | **MOVE (Plan 67-01)** | UI widget used by both ResultDialog and GenizahGUI |
| `_format_add_to_list_label` | `desktop/widgets.py` | **MOVE (Plan 67-01)** | Small shared formatting helper |
| `apply_find_highlight` | `desktop/widgets.py` | **MOVE (Plan 67-02 step 2b)** | Small UI helper function (~20 lines) |
| `_get_folio_number_from_shelfmark` | `desktop/widgets.py` | **MOVE (Plan 67-02 step 2b)** | Small folio-parsing helper, used by both ResultDialog and GenizahGUI |
| `_get_folio_image_index` | `desktop/widgets.py` | **MOVE (Plan 67-02 step 2b)** | Small folio-index helper, used by both |
| `ImageLoaderThread` | `desktop/image_loader.py` | **MOVE (Plan 67-02 step 2b)** | NOT a widget -- separate module per Codex concern #4 cohesion |
| `_get_title_svc` | `desktop/title_helpers.py` | **MOVE (Plan 67-02 step 2b)** | Title-lookup helper |
| `_title_svc_singleton` | `desktop/title_helpers.py` | **MOVE (Plan 67-02 step 2b)** | Module-global backing `_get_title_svc` -- must move with it |
| `_truncate_title` | `desktop/title_helpers.py` | **MOVE (Plan 67-02 step 2b)** | Companion of `_resolve_display_title`, used by ResultDialog AND GenizahGUI |
| `_is_hebrew_text` | `desktop/title_helpers.py` | **MOVE (Plan 67-02 step 2b)** | Hebrew-detection helper |
| `_translate_hebrew_date` | `desktop/title_helpers.py` | **MOVE (Plan 67-02 step 2b)** | Hebrew-date translation helper |
| `_resolve_display_title` | `desktop/title_helpers.py` | **MOVE (Plan 67-02 step 2b)** | Title-resolution helper |
| `_set_label_with_tooltip` | `desktop/title_helpers.py` | **MOVE (Plan 67-02 step 2b)** | Title/label rendering helper |
| `logger` | own per-module | **NEW** | `desktop/result_dialog.py` creates `logger = get_logger(__name__)` at module top |
| `ManuscriptViewerWidget` | **lazy inline** `from genizah_app import ManuscriptViewerWidget` inside `ResultDialog.__init__` | **DEFER to Phase 69** | Large class (~700 lines); import safe at init-time (genizah_app fully loaded) |
| `DesktopVSCache` | **lazy inline** `from genizah_app import DesktopVSCache` inside `_rd_search_visual_similarity` | **DEFER to later phase** | Large class; lazy inline import trivially safe |
| `FjmsBibliographyDialog` | **lazy inline** inside `_show_rd_fjms_bib` | **DEFER to Phase 68** | Phase 68 will move FJMS dialogs |
| `FjmsCatalogDialog` | **lazy inline** inside `_show_rd_catalog` | **DEFER to Phase 68** | Phase 68 will move FJMS dialogs |
| `FjmsMeasurementsDialog` | **lazy inline** inside method that invokes it | **DEFER to Phase 68** | Phase 68 will move FJMS dialogs |
| `NliBibliographyDialog` | **lazy inline** inside `_show_rd_marc_bib` | **DEFER to Phase 68** | Phase 68 will move NLI dialogs |

### Notes

- **Lazy inline imports** are the ONLY permitted `from genizah_app import X` lines in `desktop/result_dialog.py`. They MUST be indented (inside method bodies), never at column 0. This is the D-06 deny-rule exception. Phases 68-69 will resolve them by moving the target classes to their own modules.
- **`_truncate_title`** and **`_title_svc_singleton`** were not in the original Codex Round-1 list but MUST move with `_get_title_svc` because they are lexically tied (singleton + truncate are companions of the title-service API).
- The **unclassified** names in `67-MANIFEST-raw.json` are local variables, method parameters, and loop variables -- they require no import and are not part of the move set.

## Dead code to delete before extraction

Two blocks of browse residue inside ResultDialog are dead code (Finding A from orchestrator investigation):

1. **`start_browse_download(self, sid, thumb_url)`** at ~line 8772-8789 -- references `self.current_browse_sid`, `self.cancel_browse_image_thread()`, etc. which are GenizahGUI methods never available on ResultDialog. Never invoked (only caller is GenizahGUI's own version).

2. **Browse-thread cleanup in `closeEvent`** at ~line 8836-8842 -- guarded by `getattr(self, 'browse_img_thread', None)` which always returns None since `browse_img_thread` is never set on ResultDialog.

Both must be deleted before the class cut (Plan 67-02 step 2a).
