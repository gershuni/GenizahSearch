"""ResultDialog -- manuscript result viewer dialog (extracted from genizah_app.py, v7.9)."""

import json
import re
import threading

from PyQt6.QtWidgets import (
    QComboBox, QDialog, QHBoxLayout, QInputDialog, QLabel, QLineEdit,
    QMenu, QMessageBox, QPushButton, QSpinBox, QSplitter, QStyle,
    QTextBrowser, QToolButton, QVBoxLayout, QWidget,
)
from PyQt6.QtCore import Qt, QTimer, QUrl, pyqtSignal
from PyQt6.QtGui import QColor, QDesktopServices, QFont, QPalette, QPixmap

from genizah_core import (
    CURRENT_LANG, get_library_display, get_logger,
    load_app_config, save_app_config, tr,
)
from gui_threads import EnrichMetadataThread, PGPSourceWorker
from corrections_ui import (
    CommentDialog, CommentsViewerDialog, CorrectionsViewerDialog, JoinsDialog,
)
from desktop.widgets import (
    _format_add_to_list_label,
    apply_find_highlight, _get_folio_number_from_shelfmark,
    _get_folio_image_index,
)
from desktop.widgets.line_number_text_edit import (
    apply_line_numbered_text,
    is_line_numbers_enabled,
    set_line_numbers_enabled,
    refresh_visibility as refresh_line_number_visibility,
)
from desktop.title_helpers import (
    _get_title_svc, _is_hebrew_text, _translate_hebrew_date,
    _resolve_display_title, _set_label_with_tooltip,
)
from desktop.image_loader import ImageLoaderThread
from shared.synthetic_sys_id import is_synthetic_sys_id

logger = get_logger(__name__)

class ResultDialog(QDialog):
    """Allow browsing a single search result and its surrounding pages."""

    metadata_loaded = pyqtSignal(int, dict)
    thumb_resolved = pyqtSignal(str, object)

    def __init__(self, parent, all_results, current_index, meta_mgr, searcher):
        super().__init__(parent)
        self._app = parent

        # Phase 100 (REVIEWS HIGH-1): per-dialog controller scope so this dialog's
        # PDF render state is isolated from Browse's on the shared PdfImageController.
        self._pdf_scope = id(self)
        # Phase 100 (REVIEWS-R2-2): guarantee scope teardown on EVERY dialog-finish
        # path (accept/reject/done/Esc) — closeEvent alone misses reject/accept/done.
        self.finished.connect(self._on_pdf_dialog_finished)

        self.all_results = all_results
        self.current_result_idx = current_index
        self.meta_mgr = meta_mgr
        self.searcher = searcher
        self.thumb_resolved.connect(self._on_thumb_resolved)
        
        # State for internal browsing
        self.current_sys_id = None
        self.current_p_num = None
        self.current_fl_id = None
        self.current_page_text = None
        self.current_page_uid = None
        self.current_internal_idx = None
        self.current_volume_ie = None  # Active IE for multi-volume manuscripts

        self.current_meta_request = 0
        self.extended_info_visible = False
        self.external_url = None

        # External Viewer State
        self.ext_data = None
        self.ext_canvases = []

        self.init_ui()
        self.metadata_loaded.connect(self.on_metadata_loaded)
        self.load_result_by_index(self.current_result_idx)

    def init_ui(self):
        self.setWindowTitle(tr("Manuscript Viewer"))
        self.resize(1300, 850) # Wider for split view
        
        main_layout = QVBoxLayout()
        
        # --- Top Bar (Result Nav) ---
        top_bar = QHBoxLayout()
        self.btn_res_prev = QPushButton(tr("◀ Prev Result")); self.btn_res_prev.clicked.connect(lambda: self.navigate_results(-1))
        # Phase 96 bug #3 fix: prevent btn_res_prev from capturing Enter keypress.
        # QPushButton in a QDialog defaults to autoDefault=True, which means pressing
        # Enter activates the focused button. When the user types a page number into
        # spin_page and presses Enter, the focused button (Prev Result) fires instead
        # of spin_page.editingFinished. Setting autoDefault=False on both nav buttons
        # ensures Enter inside spin_page only triggers the spinbox's own signal.
        self.btn_res_prev.setAutoDefault(False)
        self.lbl_res_count = QLabel(); self.lbl_res_count.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.btn_compact_toggle = QPushButton("⏶")
        self.btn_compact_toggle.setToolTip(tr("Compact"))
        self.btn_compact_toggle.setCheckable(True)
        self.btn_compact_toggle.setChecked(False)
        self.btn_compact_toggle.setFixedWidth(36)
        self.btn_compact_toggle.clicked.connect(lambda checked: self._toggle_compact_mode(checked))
        self.btn_res_next = QPushButton(tr("Next Result ▶")); self.btn_res_next.clicked.connect(lambda: self.navigate_results(1))
        self.btn_res_next.setAutoDefault(False)
        top_bar.addWidget(self.btn_res_prev); top_bar.addWidget(self.lbl_res_count, 1); top_bar.addWidget(self.btn_compact_toggle); top_bar.addWidget(self.btn_res_next)
        main_layout.addLayout(top_bar)
        main_layout.addWidget(QSplitter(Qt.Orientation.Horizontal))

        # --- Compact Bar (initially hidden, shown in compact mode) ---
        self.compact_bar = QWidget()
        self.compact_bar.setVisible(False)
        compact_layout = QHBoxLayout(self.compact_bar)
        compact_layout.setContentsMargins(4, 2, 4, 2)
        compact_layout.setSpacing(6)

        # Shelfmark (compact)
        self.lbl_compact_shelf = QLabel()
        self.lbl_compact_shelf.setFont(QFont("Arial", 13, QFont.Weight.Bold))
        self.lbl_compact_shelf.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        compact_layout.addWidget(self.lbl_compact_shelf)

        compact_layout.addWidget(QLabel(" | "))

        # Image navigation (compact)
        compact_layout.addWidget(QLabel(tr("Image:")))
        self.btn_compact_pg_prev = QPushButton("<")
        self.btn_compact_pg_prev.setFixedWidth(25)
        self.btn_compact_pg_prev.setAutoDefault(False)  # Phase 96 fix-3: prevent Enter interception
        self.btn_compact_pg_prev.clicked.connect(lambda: self.load_page(offset=-1))
        compact_layout.addWidget(self.btn_compact_pg_prev)

        self.lbl_compact_page = QLabel("1 / ?")
        self.lbl_compact_page.setMinimumWidth(50)
        self.lbl_compact_page.setAlignment(Qt.AlignmentFlag.AlignCenter)
        compact_layout.addWidget(self.lbl_compact_page)

        self.btn_compact_pg_next = QPushButton(">")
        self.btn_compact_pg_next.setFixedWidth(25)
        self.btn_compact_pg_next.setAutoDefault(False)  # Phase 96 fix-3: prevent Enter interception
        self.btn_compact_pg_next.clicked.connect(lambda: self.load_page(offset=1))
        compact_layout.addWidget(self.btn_compact_pg_next)

        compact_layout.addWidget(QLabel(" | "))

        # Add to List (compact)
        self.btn_compact_add_list = QPushButton(_format_add_to_list_label(False))
        self.btn_compact_add_list.setToolTip(tr("Add to List"))
        self.btn_compact_add_list.clicked.connect(self.add_current_to_list)
        compact_layout.addWidget(self.btn_compact_add_list)

        # Extended Info (compact)
        self.btn_compact_ext_info = QPushButton(f"ℹ️ {tr('Info')}")
        self.btn_compact_ext_info.setToolTip(tr("Show Extended Info"))
        self.btn_compact_ext_info.setCheckable(True)
        self.btn_compact_ext_info.setVisible(False)  # shown when extended info available
        self.btn_compact_ext_info.toggled.connect(self.toggle_extended_info)
        compact_layout.addWidget(self.btn_compact_ext_info)

        # Bib buttons (compact)
        self.btn_compact_bib_fjms = QPushButton()
        self.btn_compact_bib_fjms.setVisible(False)
        self.btn_compact_bib_fjms.clicked.connect(self._show_rd_fjms_bib)
        compact_layout.addWidget(self.btn_compact_bib_fjms)
        self.btn_compact_bib_nli = QPushButton()
        self.btn_compact_bib_nli.setVisible(False)
        self.btn_compact_bib_nli.clicked.connect(self._show_rd_nli_bib)
        compact_layout.addWidget(self.btn_compact_bib_nli)

        # Catalog Records (compact)
        self.btn_compact_catalog = QPushButton()
        self.btn_compact_catalog.setVisible(False)
        self.btn_compact_catalog.clicked.connect(self._show_rd_catalog)
        compact_layout.addWidget(self.btn_compact_catalog)

        # Measurements (compact)
        self.btn_compact_measurements = QPushButton()
        self.btn_compact_measurements.setVisible(False)
        self.btn_compact_measurements.clicked.connect(self._show_rd_measurements)
        compact_layout.addWidget(self.btn_compact_measurements)

        # Joins (compact) - chain icon like normal mode
        self.btn_compact_joins = QToolButton()
        self.btn_compact_joins.setText("🔗")
        self.btn_compact_joins.setToolTip(tr("View joined fragments"))
        self.btn_compact_joins.setFixedSize(40, 32)
        self.btn_compact_joins.setStyleSheet("background-color: #95a5a6; color: white; border-radius: 4px;")
        self.btn_compact_joins.setPopupMode(QToolButton.ToolButtonPopupMode.MenuButtonPopup)
        self.btn_compact_joins.clicked.connect(self._rd_view_joins)
        compact_layout.addWidget(self.btn_compact_joins)

        # Translation toggle (compact)
        self.btn_compact_translations = QPushButton()
        self.btn_compact_translations.setCheckable(True)
        _trans_on_c = load_app_config().get('show_translations', False)
        self.btn_compact_translations.setChecked(_trans_on_c)
        self.btn_compact_translations.setText(f"🌐 {tr('Trans ON')}" if _trans_on_c else f"🌐 {tr('Trans OFF')}")
        self.btn_compact_translations.setStyleSheet(
            "QPushButton { background-color: #0369a1; color: white; border-radius: 4px; padding: 2px 8px; font-size: 11px; }"
            "QPushButton:checked { background-color: #059669; }"
        )
        self.btn_compact_translations.toggled.connect(self._rd_toggle_translations)
        compact_layout.addWidget(self.btn_compact_translations)

        compact_layout.addStretch()

        main_layout.addWidget(self.compact_bar)

        # --- Header ---
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget); header_layout.setContentsMargins(0, 5, 0, 10)
        
        # Left: Meta + Controls
        meta_col = QVBoxLayout(); meta_col.setAlignment(Qt.AlignmentFlag.AlignTop); meta_col.setSpacing(4)
        
        self.lbl_shelf = QLabel(); self.lbl_shelf.setFont(QFont("Arial", 16, QFont.Weight.Bold)); self.lbl_shelf.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.lbl_title = QLabel(); self.lbl_title.setFont(QFont("Arial", 14)); self.lbl_title.setAlignment(Qt.AlignmentFlag.AlignLeft); self.lbl_title.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)

        # Controls Row
        info_row = QHBoxLayout()
        self.btn_img = QPushButton(tr("Go to Ktiv")); self.btn_img.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DialogHelpButton)); self.btn_img.clicked.connect(self.open_catalog); self.btn_img.setFixedWidth(100)
        self.btn_external_link = QPushButton(tr("External Website"))
        self.btn_external_link.setVisible(False)
        self.btn_external_link.clicked.connect(self.open_external_link)
        self.lbl_info = QLabel(); self.lbl_info.setStyleSheet("font-size: 11px; color: palette(text);"); self.lbl_info.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.lbl_meta_loading = QLabel(tr("Loading...")); self.lbl_meta_loading.setStyleSheet("color: orange; font-size: 11px;"); self.lbl_meta_loading.setVisible(False)

        # Domain info (inlined on info_row)
        self.lbl_rd_domains = QLabel("")
        self.lbl_rd_domains.setStyleSheet("color: #8e44ad; font-size: 11px;")
        self.lbl_rd_domains.setVisible(False)

        # Printed material badge (inlined on info_row)
        self.lbl_rd_printed = QLabel("")
        self.lbl_rd_printed.setStyleSheet("color: #dc2626; font-weight: bold; font-size: 11px;")
        self.lbl_rd_printed.setVisible(False)

        info_row.addWidget(self.btn_img); info_row.addWidget(self.btn_external_link); info_row.addWidget(self.lbl_info); info_row.addWidget(self.lbl_rd_domains); info_row.addWidget(self.lbl_rd_printed); info_row.addWidget(self.lbl_meta_loading); info_row.addStretch()

        # Nav Row (Inside Header)
        nav_row = QHBoxLayout()

        # Arrows logic (Standard: Prev <, Next > regardless of RTL)
        prev_arrow = "<"
        next_arrow = ">"

        # Item 6: full-size nav buttons are instance attributes so load_local_page
        # can enable/disable them (previously only compact buttons were updated).
        self.btn_pg_prev = QPushButton(prev_arrow); self.btn_pg_prev.setFixedWidth(30); self.btn_pg_prev.setAutoDefault(False); self.btn_pg_prev.clicked.connect(lambda: self.load_page(offset=-1))
        # Item 1: replace editingFinished (fires on focus-loss too) with Enter-only
        # commit via returnPressed. setKeyboardTracking(False) prevents mid-edit
        # intermediate signals from valueChanged; returnPressed fires only on Enter.
        self.spin_page = QSpinBox(); self.spin_page.setRange(1, 9999); self.spin_page.setFixedWidth(80); self.spin_page.setKeyboardTracking(False)
        self.spin_page.lineEdit().returnPressed.connect(self._commit_spin_page_jump)
        self.btn_pg_next = QPushButton(next_arrow); self.btn_pg_next.setFixedWidth(30); self.btn_pg_next.setAutoDefault(False); self.btn_pg_next.clicked.connect(lambda: self.load_page(offset=1))
        self.lbl_total = QLabel("/ ?")

        self.lbl_img_label = QLabel("")
        self.lbl_img_label.setStyleSheet("color: #2980b9; font-weight: bold; margin-left: 10px;")

        nav_row.addWidget(QLabel(tr("Image:"))); nav_row.addWidget(self.btn_pg_prev); nav_row.addWidget(self.spin_page);
        nav_row.addWidget(self.lbl_total); nav_row.addWidget(self.btn_pg_next); nav_row.addWidget(self.lbl_img_label); nav_row.addStretch()

        action_row = QHBoxLayout()
        self.btn_view_transcription = QPushButton(f"📖 {tr('Browse')}")
        self.btn_view_transcription.setToolTip(tr("Browse manuscript"))
        self.btn_view_transcription.clicked.connect(self.open_full_transcription)
        self.btn_search_parallels = QPushButton(f"🔍 {tr('Parallels')}")
        self.btn_search_parallels.setToolTip(tr("Search for parallels"))
        self.btn_search_parallels.clicked.connect(self.search_for_parallels)

        # Add to List button
        self.btn_add_to_list = QPushButton(_format_add_to_list_label(False))
        self.btn_add_to_list.setToolTip(tr("Add to List"))
        self.btn_add_to_list.clicked.connect(self.add_current_to_list)

        # Add to Puzzle button
        self.btn_add_to_puzzle = QPushButton(f"\U0001f9e9 {tr('Puzzle')}")
        self.btn_add_to_puzzle.setToolTip(tr("Add to Fragment Puzzle"))
        self.btn_add_to_puzzle.clicked.connect(self._add_to_puzzle)

        self.btn_ext_info = QPushButton(f"ℹ️ {tr('Info')}")
        self.btn_ext_info.setToolTip(tr("Show Extended Info"))
        self.btn_ext_info.setCheckable(True)
        self.btn_ext_info.toggled.connect(self.toggle_extended_info)
        self.btn_ext_info.setVisible(False)

        # Toggle Image Button
        self.btn_toggle_image = QPushButton("🖼️")
        self.btn_toggle_image.setCheckable(True)
        self.btn_toggle_image.setChecked(True) # Default open
        self.btn_toggle_image.clicked.connect(self.toggle_external_viewer)
        self.btn_toggle_image.setVisible(False) # Hidden until images avail

        # Deprecated: btn_external_view replaced/merged logic
        self.btn_external_view = self.btn_toggle_image

        self.btn_rd_bib_fjms = QPushButton()
        self.btn_rd_bib_fjms.setVisible(False)
        self.btn_rd_bib_fjms.clicked.connect(self._show_rd_fjms_bib)
        self.btn_rd_bib_nli = QPushButton()
        self.btn_rd_bib_nli.setVisible(False)
        self.btn_rd_bib_nli.clicked.connect(self._show_rd_nli_bib)
        self.btn_rd_catalog = QPushButton(f"📋 {tr('Catalog')} (0)")
        self.btn_rd_catalog.setToolTip(tr("Catalog Records"))
        self.btn_rd_catalog.setEnabled(False)
        self.btn_rd_catalog.setVisible(False)
        self.btn_rd_catalog.clicked.connect(self._show_rd_catalog)
        self.btn_rd_measurements = QPushButton(f"\U0001f4cf {tr('Measurements')}")
        self.btn_rd_measurements.setToolTip(tr("Physical Measurements"))
        self.btn_rd_measurements.setEnabled(False)
        self.btn_rd_measurements.setVisible(False)
        self.btn_rd_measurements.clicked.connect(self._show_rd_measurements)
        self._rd_measurements_data = None
        self._rd_fjms_bib = []
        self._rd_marc_bib = []
        self._rd_catalog_detail = None

        # Translation toggle button
        self.btn_rd_translations = QPushButton()
        self.btn_rd_translations.setCheckable(True)
        _trans_on = load_app_config().get('show_translations', False)
        self.btn_rd_translations.setChecked(_trans_on)
        self.btn_rd_translations.setText(f"🌐 {tr('Trans ON')}" if _trans_on else f"🌐 {tr('Trans OFF')}")
        self.btn_rd_translations.setToolTip(tr("Toggle translations"))
        self.btn_rd_translations.setStyleSheet(
            "QPushButton { background-color: #0369a1; color: white; border-radius: 4px; padding: 2px 8px; }"
            "QPushButton:checked { background-color: #059669; }"
        )
        self.btn_rd_translations.toggled.connect(self._rd_toggle_translations)

        action_row.addWidget(self.btn_view_transcription)
        action_row.addWidget(self.btn_search_parallels)
        action_row.addWidget(self.btn_add_to_list)
        action_row.addWidget(self.btn_add_to_puzzle)
        action_row.addWidget(self.btn_ext_info)
        action_row.addWidget(self.btn_rd_bib_fjms)
        action_row.addWidget(self.btn_rd_bib_nli)
        action_row.addWidget(self.btn_rd_catalog)
        action_row.addWidget(self.btn_rd_measurements)
        action_row.addWidget(self.btn_toggle_image)
        action_row.addWidget(self.btn_rd_translations)

        # Phase 95 smoke-fix (E): "Open file" button for LOCAL hits.
        # Visible only when current result is a LOCAL file. Calls os.startfile().
        self.btn_rd_open_file = QPushButton(tr("Open file"))
        self.btn_rd_open_file.setToolTip(tr("Open the source file in the default OS application"))
        self.btn_rd_open_file.setStyleSheet(
            "QPushButton { background-color: #2980b9; color: white; border-radius: 4px; padding: 2px 8px; }"
        )
        self.btn_rd_open_file.setVisible(False)  # Hidden until a LOCAL result is shown
        self.btn_rd_open_file.clicked.connect(self._rd_open_local_file)
        self._rd_local_filepath = None  # filepath for the current LOCAL result
        action_row.addWidget(self.btn_rd_open_file)

        action_row.addStretch()

        # --- Second row: Community features (Edit, Version, Comment) ---
        community_row = QHBoxLayout()

        # Version selector
        community_row.addWidget(QLabel(tr("Version:")))
        self.rd_version_combo = QComboBox()
        self.rd_version_combo.addItem("V0.8", {"source": "original"})
        self.rd_version_combo.setFixedWidth(240)  # Wider for PGP scholar names
        self.rd_version_combo.setEnabled(False)
        self.rd_version_combo.currentIndexChanged.connect(self._rd_change_version)
        community_row.addWidget(self.rd_version_combo)
        self._rd_versions_cache = {}

        community_row.addWidget(QLabel(" | "))

        # Edit button
        self.btn_rd_edit = QPushButton(tr("✏️ Edit"))
        self.btn_rd_edit.setToolTip(tr("Enable edit mode to make corrections"))
        self.btn_rd_edit.clicked.connect(self._rd_toggle_edit_mode)
        community_row.addWidget(self.btn_rd_edit)

        # Edit action buttons (hidden by default, shown in edit mode)
        self.btn_rd_save_draft = QPushButton(f"💾 {tr('Save')}")
        self.btn_rd_save_draft.clicked.connect(lambda: self._rd_save_correction(submit=False))
        self.btn_rd_save_draft.setEnabled(False)
        self.btn_rd_save_draft.setVisible(False)
        community_row.addWidget(self.btn_rd_save_draft)

        self.btn_rd_submit = QPushButton(f"📤 {tr('Submit')}")
        self.btn_rd_submit.clicked.connect(lambda: self._rd_save_correction(submit=True))
        self.btn_rd_submit.setEnabled(False)
        self.btn_rd_submit.setVisible(False)
        community_row.addWidget(self.btn_rd_submit)

        self.btn_rd_cancel_edit = QPushButton(tr("Cancel"))
        self.btn_rd_cancel_edit.clicked.connect(self._rd_cancel_edit)
        self.btn_rd_cancel_edit.setVisible(False)
        community_row.addWidget(self.btn_rd_cancel_edit)

        # Edit status label (hidden by default)
        self.rd_edit_status = QLabel("")
        self.rd_edit_status.setVisible(False)
        community_row.addWidget(self.rd_edit_status)

        community_row.addWidget(QLabel(" | "))

        # Comment button
        self.btn_comment = QPushButton(tr("💬 Comment"))
        self.btn_comment.clicked.connect(self.add_comment)
        community_row.addWidget(self.btn_comment)

        # View Corrections button
        self.btn_view_corrections = QPushButton(f"📝 {tr('Corrections')}")
        self.btn_view_corrections.setToolTip(tr("View Corrections"))
        self.btn_view_corrections.clicked.connect(self.view_corrections)
        community_row.addWidget(self.btn_view_corrections)

        # View Comments button (icon, visible when comments exist)
        self.btn_view_comments = QPushButton("💬")
        self.btn_view_comments.setToolTip(tr("View Comments"))
        self.btn_view_comments.setFixedSize(32, 32)
        self.btn_view_comments.setVisible(False)
        self.btn_view_comments.clicked.connect(self.view_comments)
        community_row.addWidget(self.btn_view_comments)

        # Joins button with dropdown
        self.btn_joins = QToolButton()
        self.btn_joins.setText("🔗")
        self.btn_joins.setToolTip(tr("View joined fragments"))
        self.btn_joins.setFixedSize(40, 32)
        self.btn_joins.setStyleSheet("background-color: #95a5a6; color: white; border-radius: 4px;")
        self.btn_joins.setPopupMode(QToolButton.ToolButtonPopupMode.MenuButtonPopup)
        self.btn_joins.clicked.connect(self._rd_view_joins)
        self.rd_joins_menu = QMenu(self)
        self.rd_joins_menu.aboutToShow.connect(self._rd_on_joins_menu_show)
        self.btn_joins.setMenu(self.rd_joins_menu)
        community_row.addWidget(self.btn_joins)

        # Visual Similarity button — gentle orange, next to joins
        self.btn_rd_visual_sim = QPushButton("🔬")
        self.btn_rd_visual_sim.setToolTip(tr("Visual Similarity"))
        self.btn_rd_visual_sim.setFixedSize(40, 32)
        self.btn_rd_visual_sim.setStyleSheet("")
        self.btn_rd_visual_sim.setVisible(False)
        self.btn_rd_visual_sim.clicked.connect(self._rd_search_visual_similarity)
        community_row.addWidget(self.btn_rd_visual_sim)

        community_row.addStretch()

        self.txt_extended_info = QTextBrowser()
        self.txt_extended_info.setVisible(False)
        self.txt_extended_info.setMaximumHeight(200)
        # Use standard palette (transparent background allowed) to support dark mode
        self.txt_extended_info.setStyleSheet("border: 1px solid #ccc; padding: 5px;")
        self.txt_extended_info.setOpenLinks(False)
        self.txt_extended_info.anchorClicked.connect(self._on_rd_ext_link_clicked)

        meta_col.addWidget(self.lbl_shelf); meta_col.addWidget(self.lbl_title); meta_col.addLayout(info_row); meta_col.addLayout(nav_row); meta_col.addLayout(action_row); meta_col.addLayout(community_row)

        # Thumbnail (kept as hidden dummy for compatibility with existing methods)
        self.lbl_thumb = QLabel()
        self.lbl_thumb.setVisible(False)

        header_layout.addLayout(meta_col, 1)
        self.header_widget = header_widget
        main_layout.addWidget(header_widget)

        # Extended info (moved outside header to remain visible in compact mode)
        main_layout.addWidget(self.txt_extended_info)

        # Set compact joins button menu (now that rd_joins_menu is created)
        self.btn_compact_joins.setMenu(self.rd_joins_menu)
        
        # --- SPLIT VIEW (Manuscript | Source | External) ---
        self.main_splitter = QSplitter(Qt.Orientation.Horizontal)

        # 1. Manuscript View (Left)
        ms_widget = QWidget()
        ms_layout = QVBoxLayout(ms_widget); ms_layout.setContentsMargins(0,0,0,0)
        ms_text_widget = QWidget()
        ms_text_layout = QVBoxLayout(ms_text_widget); ms_text_layout.setContentsMargins(0,0,0,0)
        ms_text_layout.addWidget(QLabel("<b>" + tr("Manuscript Text") + "</b>"))
        ms_find_row = QHBoxLayout()
        ms_find_row.addWidget(QLabel(tr("Find:")))
        self.find_ms_input = QLineEdit()
        self.find_ms_input.setPlaceholderText(tr("Find in text..."))
        self.find_ms_input.textChanged.connect(lambda text: apply_find_highlight(self.text_ms, text.strip()))
        ms_find_row.addWidget(self.find_ms_input)

        # Phase 999.4 — Line-number gutter toggle (shared config key with Browse tab)
        self.btn_rd_line_numbers = QPushButton(tr("# Lines"))
        self.btn_rd_line_numbers.setCheckable(True)
        self.btn_rd_line_numbers.setChecked(is_line_numbers_enabled())
        self.btn_rd_line_numbers.setToolTip(tr("Toggle line numbers"))

        def _toggle_rd_line_numbers():
            new_state = self.btn_rd_line_numbers.isChecked()
            set_line_numbers_enabled(new_state)
            refresh_line_number_visibility(self.text_ms)

        self.btn_rd_line_numbers.clicked.connect(_toggle_rd_line_numbers)
        # In Hebrew (RTL) UI the gutter sits on the visual right of the text
        # pane; placing the toggle at index 0 of the QHBoxLayout makes RTL
        # mirroring render it on the rightmost edge, directly above the
        # gutter. LTR UI keeps the toggle appended on the right of the find
        # row (preserving the existing English-UI layout).
        from genizah_core import CURRENT_LANG as _cur_lang_for_lines_btn
        if _cur_lang_for_lines_btn == 'he':
            ms_find_row.insertWidget(0, self.btn_rd_line_numbers)
        else:
            ms_find_row.addWidget(self.btn_rd_line_numbers)

        ms_text_layout.addLayout(ms_find_row)
        self.text_ms = QTextBrowser(); self.text_ms.setFont(QFont("SBL Hebrew", 16)); self.text_ms.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        ms_text_layout.addWidget(self.text_ms)
        # Phase 999.4: attach line-number gutter as sibling widget (D-04 selection-safe).
        # First real render at _rd_display_text / browse_render_page will populate.
        apply_line_numbered_text(self.text_ms, "", source_text="", is_html=True)

        # 2. Source Context (Below Manuscript Text)
        self.src_widget = QWidget() # Container to hide/show easily
        src_layout = QVBoxLayout(self.src_widget); src_layout.setContentsMargins(0,0,0,0)
        src_layout.addWidget(QLabel("<b>" + tr("Match Context (Source)") + "</b>"))
        self.text_src = QTextBrowser()
        self.text_src.setFont(QFont("SBL Hebrew", 16))
        self.text_src.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        line_height = self.text_src.fontMetrics().lineSpacing()
        self.text_src.setMinimumHeight(line_height * 3 + 12)
        src_layout.addWidget(self.text_src)

        self.ms_text_splitter = QSplitter(Qt.Orientation.Vertical)
        self.ms_text_splitter.addWidget(ms_text_widget)
        self.ms_text_splitter.addWidget(self.src_widget)
        self.ms_text_splitter.setStretchFactor(0, 5)
        self.ms_text_splitter.setStretchFactor(1, 1)
        self.ms_text_splitter.setSizes([600, line_height * 3 + 12])
        ms_layout.addWidget(self.ms_text_splitter)

        self.main_splitter.addWidget(ms_widget)

        # 3. External Viewer Pane (Initially Hidden)
        self.external_pane = QWidget()
        self.external_pane.setVisible(False)
        ext_layout = QVBoxLayout(self.external_pane); ext_layout.setContentsMargins(0,0,0,0)

        self.lbl_ext_attr = QLabel(tr("External Viewer"))
        # Phase 100 UAT: palette-aware header so the bar adapts to dark mode
        # (previously hardcoded light #ecf0f1, which read as a white bar in dark theme).
        _ext_dark = self.palette().color(QPalette.ColorRole.Window).lightness() < 128
        if _ext_dark:
            self.lbl_ext_attr.setStyleSheet("font-weight: bold; padding: 5px; background: #3a3f44; color: #ecf0f1;")
        else:
            self.lbl_ext_attr.setStyleSheet("font-weight: bold; padding: 5px; background: #ecf0f1; color: #2c3e50;")
        self.lbl_ext_attr.setWordWrap(True)

        self.txt_ext_meta = QTextBrowser()
        self.txt_ext_meta.setMaximumHeight(100)
        self.txt_ext_meta.setStyleSheet("font-size: 11px;")

        # New: Reusable Viewer Widget
        from desktop.viewers import ManuscriptViewerWidget
        self.ms_viewer = ManuscriptViewerWidget()

        ext_layout.addWidget(self.lbl_ext_attr)
        ext_layout.addWidget(self.txt_ext_meta)
        ext_layout.addWidget(self.ms_viewer, 1)

        self.main_splitter.addWidget(self.external_pane)
        self.main_splitter.setStretchFactor(0, 1)
        self.main_splitter.setStretchFactor(1, 1)
        self.main_splitter.setSizes([650, 650])

        main_layout.addWidget(self.main_splitter, 1)

        # Footer
        btn_close = QPushButton("Close"); btn_close.clicked.connect(self.close); main_layout.addWidget(btn_close)

        # Item 3 (Codex): fully suppress all dialog-default button behavior.
        # setAutoDefault(False) prevents Qt from auto-promoting any button to
        # "default" when the current default is hidden; setDefault(False) clears
        # any explicitly-set default. Both are required together — either alone
        # can still allow Enter propagation to a button in edge cases.
        # Note: spin_page.lineEdit().returnPressed (Item 1) is the sole Enter
        # handler intended for page navigation; buttons must NOT intercept it.
        self.setLayout(main_layout)  # must be set before findChildren works
        for _btn in self.findChildren(QPushButton):
            _btn.setAutoDefault(False)
            _btn.setDefault(False)

    def _commit_spin_page_jump(self):
        """Item 1 (Codex): Enter-only spin_page commit handler.

        Connected to spin_page.lineEdit().returnPressed so that ONLY an explicit
        Enter key press triggers a page jump — focus-loss (editingFinished) no
        longer causes passive jumps when the user clicks elsewhere in the dialog.
        Reads spin_page.value() and dispatches to load_page(target=...) which
        routes to load_local_page for LOCAL sys_ids and to get_browse_page for
        Genizah hits.
        """
        self.load_page(target=self.spin_page.value())

    def _toggle_compact_mode(self, compact):
        """Toggle between compact and full header mode."""
        self.compact_bar.setVisible(compact)
        self.header_widget.setVisible(not compact)
        self.btn_compact_toggle.setChecked(compact)
        self.btn_compact_toggle.setText("⏷" if compact else "⏶")
        self.btn_compact_toggle.setToolTip(tr("Expand") if compact else tr("Compact"))

        if compact:
            # Sync compact bar state from full header
            self.lbl_compact_shelf.setText(self.lbl_shelf.text())
            page_num = self.spin_page.value()
            total_text = self.lbl_total.text()  # "/ N"
            self.lbl_compact_page.setText(f"{page_num} {total_text}")

            # Sync extended info button state
            self.btn_compact_ext_info.setVisible(self.btn_ext_info.isVisible())
            self.btn_compact_ext_info.blockSignals(True)
            self.btn_compact_ext_info.setChecked(self.btn_ext_info.isChecked())
            self.btn_compact_ext_info.blockSignals(False)
            self.btn_compact_ext_info.setText(self.btn_ext_info.text())

    def navigate_results(self, direction):
        new_idx = self.current_result_idx + direction
        if 0 <= new_idx < len(self.all_results):
            self.current_result_idx = new_idx
            self.load_result_by_index(new_idx)

    def open_full_transcription(self):
        parent = self._app
        if parent and hasattr(parent, "open_result_in_browse"):
            parent.open_result_in_browse(
                self.data,
                shelfmark=self.lbl_shelf.text(),
                title=self.lbl_title.text(),
                fl_id=self.current_fl_id,
            )
            self.close()

    def search_for_parallels(self):
        parent = self._app
        if parent and hasattr(parent, "send_result_to_composition"):
            # Trim title to first 6 words and append ... if longer
            full_title = self.lbl_title.text() or ""
            words = full_title.split()
            if len(words) > 6:
                short_title = " ".join(words[:6]) + "..."
            else:
                short_title = full_title

            parent.send_result_to_composition(
                self.data,
                source_text=self.current_page_text,
                title=short_title,
            )
            self.close()

    def add_current_to_list(self):
        """Add the current manuscript to a list."""
        parent = self._app
        if not parent or not hasattr(parent, 'lists_mgr') or not parent.lists_mgr:
            return

        # Get system ID from current result
        sys_id = None
        if self.data:
            display = self.data.get('display', {})
            sys_id = display.get('id')

        if sys_id:
            fl_id = parent._normalize_fl_id(self.current_fl_id)
            img = self.current_p_num
            parent.show_add_to_list_menu(
                [{'sys_id': sys_id, 'fl_id': fl_id, 'img': img}],
                source=tr("from browse"),
                anchor_widget=self.btn_add_to_list
            )
            # Also add to recently viewed
            parent.lists_mgr.add_to_recent(sys_id, fl_id=fl_id, img=img)
            self._update_add_to_list_button()

    def _add_to_puzzle(self):
        """Add current result to puzzle canvas (mirrors _browse_add_to_puzzle logic)."""
        parent = self._app
        if not parent or not hasattr(parent, 'add_to_puzzle'):
            return
        sys_id = self.current_sys_id
        if not sys_id:
            return
        # Get shelfmark from display metadata
        shelfmark = str(sys_id)
        if self.all_results and 0 <= self.current_result_idx < len(self.all_results):
            result = self.all_results[self.current_result_idx]
            shelfmark = (result.get('display', {}).get('shelfmark')
                         or result.get('shelfmark')
                         or result.get('call_number')
                         or str(sys_id))
        # Get fl_id from the viewer's image list (same approach as _browse_add_to_puzzle).
        # Only use fl_id when on NLI images — external sources (Cambridge, Oxford, etc.)
        # need the async PuzzleMetaLoaderThread path which carries image_url.
        fl_id = None
        folio_label = '1r'
        if hasattr(self, 'ms_viewer') and self.ms_viewer:
            if self.ms_viewer.active_list is self.ms_viewer.images_nli:
                if self.ms_viewer.current_idx < len(self.ms_viewer.active_list):
                    current_img = self.ms_viewer.active_list[self.ms_viewer.current_idx]
                    fl_id = current_img.get('fl_id', '')
                    folio_label = current_img.get('label', '1r')
        parent.add_to_puzzle(sys_id, shelfmark, folio_label, fl_id)
        self.close()

    def _rd_search_visual_similarity(self):
        """D-10: Show visual similarity dialog from ResultDialog context."""
        parent = self._app
        if not parent or not hasattr(parent, '_show_vs_dialog'):
            return
        sys_id = self.current_sys_id
        if not sys_id:
            return
        shelfmark = str(sys_id)
        if self.all_results and 0 <= self.current_result_idx < len(self.all_results):
            result = self.all_results[self.current_result_idx]
            shelfmark = (result.get('display', {}).get('shelfmark')
                         or result.get('shelfmark')
                         or str(sys_id))
        # Fetch and show VS dialog — local DB first, then cache, then server
        try:
            from shared.visual_similarity_service import get_vs_service
            vs_svc = get_vs_service(thread_safe=False)
            if vs_svc.is_available() and vs_svc.has_suggestions(sys_id):
                data = vs_svc.get_suggestions(sys_id, 200)
                parent._enrich_vs_suggestions(data)
                parent._show_vs_dialog(sys_id, shelfmark, data, parent_dialog=self)
                return
        except Exception:
            pass  # Cache operation failed; continue without cached data
        # Try cache
        if not hasattr(parent, '_vs_cache'):
            from desktop.vs_cache import DesktopVSCache
            parent._vs_cache = DesktopVSCache()
        cached = parent._vs_cache.get_cached(sys_id)
        if cached is not None:
            parent._enrich_vs_suggestions(cached)
            parent._show_vs_dialog(sys_id, shelfmark, cached, parent_dialog=self)
            return
        # Fetch from server
        try:
            import urllib.request
            url = f'{parent._VS_SERVER_URL}/api/visual_suggestions/{sys_id}?limit=200'
            with urllib.request.urlopen(url, timeout=15) as resp:
                data = json.loads(resp.read().decode())
            if data:
                parent._vs_cache.store(sys_id, data)
                parent._enrich_vs_suggestions(data)
                parent._show_vs_dialog(sys_id, shelfmark, data, parent_dialog=self)
                return
        except Exception:
            pass  # Cache operation failed; continue without cached data
        QMessageBox.information(self, tr("Visual Similarity"), tr("No visual similarity suggestions"))

    def _update_add_to_list_button(self):
        parent = self._app
        if not parent or not hasattr(parent, 'lists_mgr') or not parent.lists_mgr:
            return
        if not self.current_sys_id:
            return
        in_list = parent._is_item_in_non_recent_list(
            self.current_sys_id,
            img=self.current_p_num,
            fl_id=parent._normalize_fl_id(self.current_fl_id),
        )
        label = _format_add_to_list_label(in_list)
        self.btn_add_to_list.setText(label)
        if hasattr(self, 'btn_compact_add_list'):
            self.btn_compact_add_list.setText(label)

    def add_comment(self):
        """Open comment dialog for current document."""
        parent = self._app
        if not parent or not hasattr(parent, 'corrections_client'):
            return
        if not parent.corrections_client.is_logged_in():
            QMessageBox.warning(self, tr("Login Required"), tr("Please login to add a comment."))
            return
        dialog = CommentDialog(
            self, parent.corrections_client,
            document_id=self.current_sys_id,
            shelfmark=self.lbl_shelf.text(),
            page_number=self.current_p_num
        )
        dialog.exec()

    def view_corrections(self):
        """View corrections for current document."""
        parent = self._app
        if not parent or not hasattr(parent, 'corrections_client'):
            return
        dialog = CorrectionsViewerDialog(
            self, parent.corrections_client,
            document_id=self.current_sys_id,
            shelfmark=self.lbl_shelf.text(),
            on_view_result=lambda s: parent._open_document_result_dialog(shelfmark=s) if hasattr(parent, '_open_document_result_dialog') else None,
            on_browse=lambda s: parent._browse_document_by_shelfmark(s) if hasattr(parent, '_browse_document_by_shelfmark') else None
        )
        dialog.exec()

    def view_comments(self):
        """View comments for current document."""
        parent = self._app
        if not parent or not hasattr(parent, 'corrections_client'):
            return
        dialog = CommentsViewerDialog(
            self, parent.corrections_client,
            document_id=self.current_sys_id,
            shelfmark=self.lbl_shelf.text()
        )
        dialog.exec()

    def _rd_view_joins(self):
        """View joined fragments for current document."""
        parent = self._app
        if not parent or not hasattr(parent, 'corrections_client'):
            return

        shelfmark = self.lbl_shelf.text()
        if not shelfmark:
            return

        def navigate_to_shelfmark(target_shelfmark):
            """Navigate to a shelfmark within the same results dialog."""
            # Note: JoinsDialog already closes itself before calling this callback
            # Load the document in the same ResultDialog
            self.load_by_shelfmark(target_shelfmark)

        dialog = JoinsDialog(
            self, parent.corrections_client,
            document_id=self.current_sys_id,
            shelfmark=shelfmark,
            on_browse=navigate_to_shelfmark,
            shelf_model=getattr(parent, 'shelf_model', None),
            joins_mgr=getattr(parent, 'joins_mgr', None),
            shelf_completer=getattr(parent, 'shelf_completer', None),
            lists_mgr=getattr(parent, 'lists_mgr', None),
            meta_mgr=getattr(parent, 'meta_mgr', None)
        )
        dialog.exec()

    def _rd_update_joins_menu(self):
        """Update the joins dropdown menu with connected fragments."""
        self.rd_joins_menu.clear()
        parent = self._app

        # Use document_id (sys_id) for lookup - this is the reliable key
        document_id = self.current_sys_id
        display_shelfmark = self.lbl_shelf.text()  # For display purposes only

        if not document_id:
            action = self.rd_joins_menu.addAction(tr("No document ID"))
            action.setEnabled(False)
            return

        # Get joins from JoinsManager using document_id (offline-first)
        connected = None
        plain_shelfmark = display_shelfmark.split(' | ')[-1] if ' | ' in display_shelfmark else display_shelfmark

        if parent and hasattr(parent, 'joins_mgr') and parent.joins_mgr:
            # Debug: show what's in the indexes
            joins_mgr = parent.joins_mgr
            by_doc_id = joins_mgr.data.get('by_document_id', {})
            by_normalized = joins_mgr.data.get('by_normalized', {})
            total_joins = len(joins_mgr.data.get('joins', {}))
            logger.debug("ResultDialog joins: total_joins=%s, by_document_id=%s, by_normalized=%s", total_joins, len(by_doc_id), len(by_normalized))
            logger.debug("Looking for doc_id='%s', plain_shelfmark='%s'", document_id, plain_shelfmark)

            # First try document_id lookup
            if document_id in by_doc_id:
                logger.debug("Found in by_document_id with join_ids: %s", by_doc_id[document_id])
            connected = joins_mgr.get_connected_fragments_by_id(document_id)

            # If no results by document_id, try shelfmark
            if not connected or connected.get('total_fragments', 0) <= 1:
                normalized = joins_mgr._normalize_shelfmark(plain_shelfmark)
                logger.debug("Not found by doc_id, trying normalized shelfmark: '%s'", normalized)
                if normalized in by_normalized:
                    logger.debug("Found in by_normalized with join_ids: %s", by_normalized[normalized])
                connected = joins_mgr.get_connected_fragments(plain_shelfmark)

            logger.debug("Final connected result: fragments=%s", connected.get('fragments', []) if connected else 'None')

        if not connected or connected.get('total_fragments', 0) <= 1:
            # Check PGP multi-fragment joins as fallback
            try:
                from shared.document_service import get_document_for_fragment, get_fragments_for_document
                pgp_doc = get_document_for_fragment(self.current_sys_id)
                if pgp_doc:
                    pgp_frags = get_fragments_for_document(pgp_doc.get('pgpid'))
                    if pgp_frags and len(pgp_frags) > 1:
                        self.btn_joins.setStyleSheet("background-color: #27ae60; color: white; border-radius: 4px;")
                        if hasattr(self, 'btn_compact_joins'):
                            self.btn_compact_joins.setStyleSheet("background-color: #27ae60; color: white; border-radius: 4px;")
                        header_action = self.rd_joins_menu.addAction(
                            tr("{} connected fragments").format(len(pgp_frags)) + " [PGP]"
                        )
                        header_action.setEnabled(False)
                        self.rd_joins_menu.addSeparator()
                        for frag in pgp_frags:
                            frag_sid = frag.get('sys_id', '')
                            frag_shelf = frag.get('shelfmark', frag_sid)
                            if frag_sid == self.current_sys_id:
                                continue
                            action = self.rd_joins_menu.addAction(f"[PGP] {frag_shelf}")
                            action.triggered.connect(lambda checked, sh=frag_shelf: self._rd_navigate_to_joined_fragment(sh))
                        return
            except Exception as e:
                logger.debug("PGP joins RD dropdown fallback error: %s", e)

            action = self.rd_joins_menu.addAction(tr("No joined fragments"))
            action.setEnabled(False)
            self.btn_joins.setStyleSheet("background-color: #95a5a6; color: white; border-radius: 4px;")
            if hasattr(self, 'btn_compact_joins'):
                self.btn_compact_joins.setStyleSheet("background-color: #95a5a6; color: white; border-radius: 4px;")
            return

        # Has joins - update button style
        self.btn_joins.setStyleSheet("background-color: #27ae60; color: white; border-radius: 4px;")
        if hasattr(self, 'btn_compact_joins'):
            self.btn_compact_joins.setStyleSheet("background-color: #27ae60; color: white; border-radius: 4px;")

        header_action = self.rd_joins_menu.addAction(
            tr("{} connected fragments").format(connected.get('total_fragments', 0))
        )
        header_action.setEnabled(False)
        self.rd_joins_menu.addSeparator()

        fragments_list = connected.get('fragments', []) if connected else []
        joins_list = connected.get('joins', []) if connected else []
        fragment_details = connected.get('fragment_details', []) if connected else []

        # Extract plain shelfmark for comparison
        plain_shelfmark = display_shelfmark.split(' | ')[-1] if ' | ' in display_shelfmark else display_shelfmark

        # Build set of directly connected fragments
        direct_fragments = set()
        for join in joins_list:
            frag_a = join.get('fragment_a', '') if isinstance(join, dict) else getattr(join, 'fragment_a', '')
            frag_b = join.get('fragment_b', '') if isinstance(join, dict) else getattr(join, 'fragment_b', '')
            if frag_a.upper() == plain_shelfmark.upper():
                direct_fragments.add(frag_b.upper())
            elif frag_b.upper() == plain_shelfmark.upper():
                direct_fragments.add(frag_a.upper())

        # Build map of shelfmark -> document_id from fragment_details for title lookup
        shelfmark_to_docid = {}
        for fd in fragment_details:
            shelf = fd.get('shelfmark', '') if isinstance(fd, dict) else getattr(fd, 'shelfmark', '')
            doc_id = fd.get('document_id') if isinstance(fd, dict) else getattr(fd, 'document_id', None)
            if shelf and doc_id:
                shelfmark_to_docid[shelf.upper()] = doc_id

        logger.debug("_rd_update_joins_menu: doc_id='%s', plain_shelfmark='%s', direct=%s", document_id, plain_shelfmark, direct_fragments)
        for frag in fragments_list:
            # Compare with plain shelfmark (joins store plain shelfmarks)
            is_current = frag.upper() == plain_shelfmark.upper()
            is_direct = frag.upper() in direct_fragments

            # Get title for display
            title_preview = ""
            frag_doc_id = shelfmark_to_docid.get(frag.upper())

            # Fallback: use parent's _shelf_to_sys map from csv_bank
            if not frag_doc_id and parent and hasattr(parent, '_shelf_to_sys') and parent._shelf_to_sys:
                norm = parent._normalize_shelfmark(frag) if hasattr(parent, '_normalize_shelfmark') else None
                if norm:
                    frag_doc_id = parent._shelf_to_sys.get(norm)

            if frag_doc_id and parent and hasattr(parent, 'meta_mgr') and parent.meta_mgr:
                try:
                    _, title = parent.meta_mgr.get_meta_for_id(frag_doc_id)
                    if title:
                        words = title.split()[:4]
                        title_preview = ' '.join(words)
                        if len(title.split()) > 4:
                            title_preview += "..."
                except (KeyError, AttributeError, IndexError):
                    pass

            if is_current:
                label = f"• {frag}"
                if title_preview:
                    label += f" - {title_preview}"
                label += f" ({tr('current')})"
                action = self.rd_joins_menu.addAction(label)
                action.setEnabled(False)
            else:
                label = f"→ {frag}"
                if title_preview:
                    label += f" - {title_preview}"
                if is_direct:
                    label += f" ({tr('direct')})"
                action = self.rd_joins_menu.addAction(label)
                action.setData(frag)
                action.triggered.connect(lambda checked, f=frag: self._rd_navigate_to_joined_fragment(f))

        self.rd_joins_menu.addSeparator()
        view_all = self.rd_joins_menu.addAction(tr("View all joins..."))
        view_all.triggered.connect(self._rd_view_joins)

    def _rd_on_joins_menu_show(self):
        """Called when joins menu is about to show - trigger sync and update."""
        parent = self._app
        # Trigger a background sync to get latest joins from server
        if parent and hasattr(parent, 'joins_mgr') and parent.joins_mgr:
            import threading
            def sync_and_update():
                parent.joins_mgr.sync_with_server()
            threading.Thread(target=sync_and_update, daemon=True).start()
        # Update menu with current data
        self._rd_update_joins_menu()

    def _rd_navigate_to_joined_fragment(self, shelfmark: str):
        """Navigate to a joined fragment within the same results dialog."""
        # Load the document in the same ResultDialog instead of switching to browse tab
        self.load_by_shelfmark(shelfmark)

    def _rd_load_versions(self):
        """Load versions for current document page."""
        parent = self._app
        if not parent or not hasattr(parent, 'corrections_client'):
            return

        doc_id = self.current_sys_id
        page_num = self.current_p_num or 1
        client = parent.corrections_client

        # Store original text
        original_text = self.text_ms.toPlainText()
        self._rd_original_text = original_text
        self._rd_versions_cache = {'original': original_text}

        # Force fresh server availability check (500ms timeout) to prevent UI freeze
        if not client.is_server_available(force_check=True):
            # Server is down - skip API calls, hide version-related UI
            self.btn_view_comments.setVisible(False)
            return

        # Check for comments
        try:
            comments = client.get_comments_for_document(doc_id, page_size=1, ie_id=getattr(self, 'current_volume_ie', None))
            if comments and len(comments) > 0:
                self.btn_view_comments.setVisible(True)
            else:
                self.btn_view_comments.setVisible(False)
        except Exception:
            self.btn_view_comments.setVisible(False)  # Feature check failed; hide button

        # Fetch versions and corrections using shared method
        self._rd_refresh_versions(select_latest=True)

    def _rd_change_version(self, index):
        """Handle version change in ResultDialog."""
        version_data = self.rd_version_combo.currentData()
        if version_data:
            self._rd_load_version_content(version_data)

    def _rd_refresh_versions(self, select_latest=False):
        """Refresh version list. If select_latest=True, select and load the latest version."""
        parent = self._app
        if not parent or not hasattr(parent, 'corrections_client'):
            return

        doc_id = self.current_sys_id
        page_num = self.current_p_num or 1
        client = parent.corrections_client

        # Quick server availability check (500ms timeout) to prevent UI freeze
        if not client.is_server_available():
            # Server is down - skip API calls
            return

        # Remember current selection
        current_data = self.rd_version_combo.currentData()

        # Reset version combo
        self.rd_version_combo.blockSignals(True)
        self.rd_version_combo.clear()
        self.rd_version_combo.addItem("V0.8", {"source": "original"})

        new_user_idx = -1  # Track user's own correction/version
        users_with_versions = set()  # Track users who have versions (to avoid duplicate corrections)

        try:
            versions_data = client.get_page_versions(doc_id, page_num)
            all_versions = versions_data.get('all_versions', [])
            logger.debug("_rd_refresh_versions: doc_id=%s, page=%s, versions=%s", doc_id, page_num, len(all_versions))
            for v in all_versions:
                logger.debug("version: source=%s, user=%s, id=%s", v.get('source'), v.get('user_name'), v.get('id'))

            # Filter to only latest version per user (use user_name as key for consistent deduplication)
            user_versions = [v for v in all_versions if v.get('source') == 'user']
            latest_by_user = {}
            for ver in user_versions:
                # Use user_name as key for deduplication (more reliable than user_id which may vary)
                user_key = ver.get('user_name', 'unknown')
                if user_key not in latest_by_user:
                    latest_by_user[user_key] = ver
                else:
                    existing = latest_by_user[user_key]
                    # Keep the one with the later created_at date
                    if ver.get('created_at', '') > existing.get('created_at', ''):
                        latest_by_user[user_key] = ver

            logger.debug("After dedup: %s unique users from %s versions", len(latest_by_user), len(user_versions))

            # Add V0.7 if available
            for ver in all_versions:
                if ver.get('source') == 'V0.7':
                    ver_id = ver.get('id')
                    is_default = ver.get('is_current_default', False)
                    label = 'V0.7'
                    if is_default:
                        label += f" ({tr('Default')})"
                    self.rd_version_combo.addItem(label, {
                        "source": "V0.7", "version_id": ver_id, "is_default": is_default
                    })

            # Add unique user versions (latest per user)
            for ver in latest_by_user.values():
                ver_id = ver.get('id')
                user_name = ver.get('user_name') or 'User'
                created_at = ver.get('created_at', '')[:10] if ver.get('created_at') else ''
                is_default = ver.get('is_current_default', False)

                label = f"{tr('by')} {user_name}"
                if created_at:
                    label += f" ({created_at})"
                if is_default:
                    label += " ✓"

                self.rd_version_combo.addItem(label, {
                    "source": "user", "version_id": ver_id, "user_name": user_name, "is_default": is_default
                })

                # Check if this is the current user's version (by username or full name)
                is_current_user = False
                if client.current_user:
                    if user_name == client.current_user.username:
                        is_current_user = True
                    elif client.current_user.full_name and user_name == client.current_user.full_name:
                        is_current_user = True

                if is_current_user:
                    new_user_idx = self.rd_version_combo.count() - 1
                    # Also add the username to tracked set (for matching with corrections)
                    users_with_versions.add(client.current_user.username)

                # Track users who already have versions (to avoid duplicates with corrections)
                users_with_versions.add(user_name)

        except Exception as e:
            logger.debug("Error refreshing versions: %s", e)

        # Also fetch corrections from corrections API (separate from versions)
        # Only add corrections for users who don't already have a version entry
        try:
            corrections = client.get_corrections_for_document(doc_id, include_drafts=True, ie_id=getattr(self, 'current_volume_ie', None))
            # Filter corrections by page number
            page_corrections = [c for c in corrections if c.page_number == page_num or c.page_number is None]
            logger.debug("_rd_refresh_versions: corrections=%s, page_corrections=%s", len(corrections), len(page_corrections))
            for c in corrections:
                logger.debug("corr id=%s, status=%s, author=%s, page=%s", c.id, c.status, c.author_username, c.page_number)

            # Group by user, keep latest per user
            corrections_by_user = {}
            for corr in page_corrections:
                user_key = corr.author_username or f"user_{corr.author_id}"
                if user_key not in corrections_by_user:
                    corrections_by_user[user_key] = corr
                else:
                    existing = corrections_by_user[user_key]
                    if (corr.created_at or '') > (existing.created_at or ''):
                        corrections_by_user[user_key] = corr

            # Determine user permissions for viewing corrections
            current_username = client.current_user.username if client.current_user else None
            is_reviewer_or_admin = client.current_user and client.current_user.role in ('reviewer', 'editor', 'admin')

            for corr in corrections_by_user.values():
                user_name = corr.author_username or 'User'
                status = corr.status

                # Skip if user already has a version entry (avoid duplicates)
                if user_name in users_with_versions:
                    logger.debug("correction: user=%s SKIPPED (has version)", user_name)
                    continue

                # Filter based on status and user permissions:
                # - Authors can see their own corrections (any status)
                # - Reviewers/admins can see all corrections
                # - Regular users can only see approved corrections from others
                is_own_correction = current_username and user_name == current_username

                if status == 'rejected':
                    # Rejected corrections: only visible to author or admin
                    if not is_own_correction and not is_reviewer_or_admin:
                        logger.debug("correction: user=%s SKIPPED (rejected, not authorized)", user_name)
                        continue
                elif status in ('draft', 'pending'):
                    # Draft/Pending: only visible to author or reviewer/admin
                    if not is_own_correction and not is_reviewer_or_admin:
                        logger.debug("correction: user=%s SKIPPED (%s, not authorized)", user_name, status)
                        continue

                created_at = corr.created_at[:10] if corr.created_at else ''

                # Status indicators and label
                if status == 'draft':
                    # For drafts, just show "📝 Draft" without username
                    label = f"📝 {tr('Draft')}"
                elif status == 'pending':
                    label = f"⏳ {tr('Pending')} - {user_name}"
                elif status == 'approved':
                    label = f"✅ {tr('by')} {user_name}"
                    if created_at:
                        label += f" ({created_at})"
                elif status == 'rejected':
                    label = f"❌ {tr('Rejected')} - {user_name}"
                else:
                    label = f"{tr('by')} {user_name}"
                    if created_at:
                        label += f" ({created_at})"

                logger.debug("correction: user=%s, status=%s, id=%s", user_name, status, corr.id)

                self.rd_version_combo.addItem(label, {
                    "source": "correction",
                    "correction_id": corr.id,
                    "user_name": user_name,
                    "status": status,
                    "corrected_text": corr.corrected_text
                })

                # Check if this is the current user's correction
                if client.current_user and user_name == client.current_user.username:
                    new_user_idx = self.rd_version_combo.count() - 1

        except Exception as e:
            logger.debug("Error fetching corrections: %s", e)

        # Cache corrections/versions for re-appending after PGP combo rebuild
        self._rd_cached_corrections = []
        for i in range(self.rd_version_combo.count()):
            item_data = self.rd_version_combo.itemData(i)
            if item_data and item_data.get('source') not in ('original', 'header', None):
                self._rd_cached_corrections.append(
                    (self.rd_version_combo.itemText(i), item_data))

        # Enable combo if we have versions/corrections
        if self.rd_version_combo.count() > 1:
            self.rd_version_combo.setEnabled(True)

            if select_latest:
                # Select and load the latest (last) version as default
                latest_idx = self.rd_version_combo.count() - 1
                self.rd_version_combo.setCurrentIndex(latest_idx)
                self.rd_version_combo.blockSignals(False)
                data = self.rd_version_combo.itemData(latest_idx)
                if data and data.get('source') != 'original':
                    self._rd_load_version_content(data)
                return
            elif new_user_idx >= 0:
                # Select user's own version/correction if just saved
                self.rd_version_combo.setCurrentIndex(new_user_idx)
        else:
            self.rd_version_combo.setEnabled(False)

        self.rd_version_combo.blockSignals(False)

    def _rd_load_version_content(self, version_data):
        """Load and display version content."""
        source = version_data.get('source')
        version_id = version_data.get('version_id')
        correction_id = version_data.get('correction_id')
        source_id = version_data.get('source_id')

        # Build cache key
        if source in ('pgp_edition', 'pgp_translation'):
            cache_key = f"pgp_{source_id}" if source_id else source
        else:
            cache_key = f"{source}_{version_id or correction_id}" if (version_id or correction_id) else source

        if cache_key in self._rd_versions_cache:
            content = self._rd_versions_cache[cache_key]
            if source == 'pgp_translation':
                language = version_data.get('language', '')
                is_rtl = language != 'English'
                self._rd_display_pgp_text(content, is_rtl=is_rtl)
            elif source == 'pgp_edition':
                self._rd_display_pgp_text(content, is_rtl=True)
            else:
                self._rd_display_text(content)
            return

        if source == "original":
            # Restore RTL direction for V0.8 text
            self.text_ms.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
            if hasattr(self, '_rd_original_text'):
                self._rd_display_text(self._rd_original_text)
        elif source == "pgp_edition":
            # PGP edition content is stored directly in version_data
            content = version_data.get('content', '')
            if content:
                if source_id:
                    self._rd_versions_cache[f"pgp_{source_id}"] = content
                self._rd_display_pgp_text(content, is_rtl=True)
        elif source == "pgp_translation":
            # PGP translation content is stored directly in version_data
            content = version_data.get('content', '')
            language = version_data.get('language', '')
            if content:
                if source_id:
                    self._rd_versions_cache[f"pgp_{source_id}"] = content
                # English translations are LTR, everything else RTL
                is_rtl = language != 'English'
                self._rd_display_pgp_text(content, is_rtl=is_rtl)
        elif source == "correction":
            # Correction text is included directly in version_data
            # Restore RTL for corrections (Hebrew text)
            self.text_ms.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
            content = version_data.get('corrected_text', '')
            if content:
                self._rd_versions_cache[cache_key] = content
                self._rd_display_text(content)
        elif version_id:
            parent = self._app
            if parent and hasattr(parent, 'corrections_client'):
                # Restore RTL for user versions (Hebrew text)
                self.text_ms.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
                # Quick server availability check (500ms timeout) to prevent UI freeze
                if not parent.corrections_client.is_server_available():
                    return
                try:
                    ver_data = parent.corrections_client.get_version_content(version_id)
                    content = ver_data.get('content', '')
                    if content:
                        self._rd_versions_cache[cache_key] = content
                        self._rd_display_text(content)
                except Exception as e:
                    logger.debug("Error loading version: %s", e)

    def _rd_display_text(self, text):
        """Display text in the manuscript viewer."""
        if text:
            # Phase 999.4: route through gutter helper (source_text = raw `text`)
            apply_line_numbered_text(
                self.text_ms, self._htmlify(text), source_text=text, is_html=True,
            )

    def _rd_display_pgp_text(self, text, is_rtl=True):
        """Display PGP edition/translation text with proper directionality."""
        if not text:
            return
        direction = 'rtl' if is_rtl else 'ltr'
        layout_dir = Qt.LayoutDirection.RightToLeft if is_rtl else Qt.LayoutDirection.LeftToRight
        self.text_ms.setLayoutDirection(layout_dir)
        html_text = text.replace('\n', '<br>')
        # Phase 999.4: route through gutter helper (source_text = raw `text`)
        apply_line_numbered_text(
            self.text_ms,
            f"<div dir='{direction}'>{html_text}</div>",
            source_text=text,
            is_html=True,
        )
        self._refresh_find_highlights()

    def _on_rd_pgp_loaded(self, sys_id, sources, pgp_doc):
        """Handle PGP sources loaded from background thread."""
        # Stale-request guard: user may have navigated to a different result
        if sys_id != self.current_sys_id:
            return

        # Store PGP data
        self._rd_pgp_sources = sources
        self._rd_pgp_doc = pgp_doc

        # Handle PGP extended info display:
        # Case 1: Enriched data already built HTML -> append PGP section
        # Case 2: Enriched data ran but had nothing (early return) -> build PGP-only
        # Case 3: Enriched data hasn't arrived yet -> PGP included when it runs
        if pgp_doc:
            if getattr(self, '_rd_enriched_data_loaded', False):
                self._rd_update_extended_info_with_pgp()
            elif not self.btn_ext_info.isVisible():
                parent_win = self._app
                if parent_win and hasattr(parent_win, '_build_pgp_extended_info_html'):
                    pal = self.txt_extended_info.palette()
                    tc = pal.color(QPalette.ColorRole.Text).name()
                    bc = pal.color(QPalette.ColorRole.Base).name()
                    ph = parent_win._build_pgp_extended_info_html(pgp_doc, palette=pal, sys_id=getattr(self, 'current_sys_id', None))
                    if ph:
                        h = f"<div style='font-family:Arial; color:{tc}; background-color:{bc};'>{ph}</div>"
                        self.txt_extended_info.setHtml(h)
                        self.btn_ext_info.setVisible(True)
                        if hasattr(self, 'btn_compact_ext_info'):
                            self.btn_compact_ext_info.setVisible(True)

        if not sources:
            return

        parent = self._app
        if not parent:
            return

        # Populate combo with PGP items (clears and rebuilds: PGP Editions > Translations > V0.8)
        has_pgp = parent._populate_pgp_combo(self.rd_version_combo, sources, pgp_doc)

        if has_pgp:
            # Re-add cached corrections/versions after V0.8
            cached = getattr(self, '_rd_cached_corrections', [])
            if cached:
                self.rd_version_combo.blockSignals(True)
                self.rd_version_combo.insertSeparator(self.rd_version_combo.count())
                for label, data in cached:
                    self.rd_version_combo.addItem(label, data)
                self.rd_version_combo.blockSignals(False)

            # Store original V0.8 text (always refresh from current display)
            current_text = self.text_ms.toPlainText()
            if current_text:
                self._rd_original_text = current_text

            # Auto-select first PGP edition and display it
            edition_data = parent._auto_select_pgp_edition(self.rd_version_combo)
            if edition_data:
                content = edition_data.get('content', '')
                if content:
                    self._rd_display_pgp_text(content, is_rtl=True)

            self.rd_version_combo.setEnabled(True)

    def _on_rd_pgp_error(self, sys_id, error_msg):
        """Handle PGP source fetch error -- silently fall back to existing behavior."""
        logger.debug("PGP fetch error for %s: %s", sys_id, error_msg)

    def _rd_update_extended_info_with_pgp(self):
        """Rebuild extended info HTML after PGP data arrives late.

        Called when PGP data arrives after on_enriched_data_loaded() already built
        the extended info. Now simply rebuilds via _rd_build_extended_html which
        includes the PGP section since _rd_pgp_doc is set.
        """
        rd_meta = getattr(self, '_rd_enrichment_meta', None)
        if not rd_meta:
            return
        oxford_part_id = rd_meta.get('_part_id')
        part_meta = rd_meta.get('_part_meta')
        html = self._rd_build_extended_html(rd_meta, oxford_part_id, part_meta)
        if html:
            self.txt_extended_info.setHtml(html)
            self.btn_ext_info.setVisible(True)
            if hasattr(self, 'btn_compact_ext_info'):
                self.btn_compact_ext_info.setVisible(True)

    def _rd_build_extended_html(self, meta, oxford_part_id=None, part_meta=None):
        """Build the full extended info HTML for ResultDialog.

        Returns the complete HTML string, or None if there's nothing to display.
        Called by on_enriched_data_loaded() and _rd_refresh_extended_info().
        """
        marc = meta.get('marc', {})
        external_meta = meta.get('external_meta', {})
        has_pgp = bool(getattr(self, '_rd_pgp_doc', None))
        if not marc and not meta.get('physical_desc') and not part_meta and not external_meta and not has_pgp:
            return None

        palette = self.txt_extended_info.palette()
        text_color = palette.color(QPalette.ColorRole.Text).name()
        base_color = palette.color(QPalette.ColorRole.Base).name()
        part_bg = QColor(base_color).lighter(115).name()

        # Check show_translations for on-demand translate badges
        from genizah_core import load_app_config as _lac
        _show_trans = _lac().get('show_translations', False)
        _ft_cache = {}
        if _show_trans:
            from gui_threads import _field_translation_cache
            _ft_cache = _field_translation_cache
            # Pre-populate cache with Oxford pre-computed translations for part fields
            if part_meta:
                try:
                    _ox_svc = _get_title_svc()
                    if _ox_svc and _ox_svc.oxford_available():
                        _ox_texts = [part_meta.get(f, '').strip() for f in ('title', 'contents') if part_meta.get(f, '').strip()]
                        if _ox_texts:
                            _ox_batch = _ox_svc.get_oxford_translations_batch(_ox_texts)
                            for _ox_eng, _ox_heb in _ox_batch.items():
                                if _ox_eng == part_meta.get('title', '').strip():
                                    _ft_cache.setdefault('rd_part_title', _ox_heb)
                                if _ox_eng == part_meta.get('contents', '').strip():
                                    _ft_cache.setdefault('rd_part_contents', _ox_heb)
                except Exception:
                    pass  # Translation lookup failed; continue without translation
        import html as _html_mod
        _tbadge = 'color: #0369a1; font-size: 11px; text-decoration: none; background: #e0f2fe; padding: 1px 4px; border-radius: 3px;'

        _ui_lang = CURRENT_LANG  # 'he' or 'en'

        def _trans_or_badge(field_key, text, label, min_len=10):
            """Return text with translate badge or cached translation."""
            if not _show_trans or not text or len(text.strip()) < min_len:
                return text
            is_he_orig = _is_hebrew_text(text)
            # Skip if text is already in the UI language (nothing to translate)
            if is_he_orig and _ui_lang == 'he':
                return text
            if not is_he_orig and _ui_lang == 'en':
                return text
            cached = _ft_cache.get(field_key)
            if cached == '__translating__':
                _loading_style = 'color: #6b7280; font-size: 11px; font-style: italic;'
                return (
                    f"{_html_mod.escape(text)} "
                    f"<span style='{_loading_style}'>⏳ {tr('Translating...')}</span>"
                )
            if cached:
                parent = self._app
                _toggle = getattr(parent, '_trans_toggle_state', {}) if parent else {}
                showing_orig = _toggle.get(field_key, False)
                show_text = text if showing_orig else cached
                badge_label = tr('Original') if not showing_orig else tr('Translated')
                if is_he_orig:
                    _dir = 'rtl' if showing_orig else 'ltr'
                else:
                    _dir = 'ltr' if showing_orig else 'rtl'
                return (
                    f"<span dir='{_dir}'>{_html_mod.escape(show_text)}</span> "
                    f"<a href='toggle-trans:{field_key}:{_html_mod.escape(text)}' style='{_tbadge}'>{badge_label}</a>"
                )
            return (
                f"{_html_mod.escape(text)} "
                f"<a href='translate-field:{field_key}' style='{_tbadge}'>{tr('Translate')}</a>"
            )

        kti_html = ""
        date_val = marc.get('date')
        if date_val:
            # Pre-populate cache with direct Hebrew date conversion (avoids Dicta errors)
            if _show_trans and 'rd_date' not in _ft_cache and _is_hebrew_text(date_val):
                _direct = _translate_hebrew_date(date_val)
                if _direct:
                    _ft_cache['rd_date'] = _direct
            kti_html += f"<p><b>{tr('Date')}:</b> {_trans_or_badge('rd_date', date_val, tr('Date'), min_len=3)}</p>"

        dims = marc.get('dimensions'); phys = meta.get('physical_desc')
        if dims or phys:
            phys_text = f"{phys or ''} {dims or ''}".strip()
            kti_html += f"<p><b>{tr('Physical Description')}:</b> {_trans_or_badge('rd_phys_desc', phys_text, tr('Physical Description'))}</p>"

        eng_title = marc.get('english_title')
        if eng_title:
            kti_html += f"<p><b>{tr('English Title')}:</b> {_trans_or_badge('rd_eng_title', eng_title, tr('English Title'))}</p>"

        subjects = marc.get('subjects', [])
        if subjects:
            subjects_text = '; '.join(subjects)
            kti_html += f"<p><b>{tr('Subjects')}:</b> {_trans_or_badge('rd_subjects', subjects_text, tr('Subjects'))}</p>"

        notes = marc.get('notes', [])
        if notes:
            notes_combined = '\n'.join(notes)
            notes_result = _trans_or_badge('rd_notes', notes_combined, tr('Notes'))
            # If badge was applied to combined text, render as single block with line breaks
            if notes_result != notes_combined:
                kti_html += f"<p><b>{tr('Notes')}:</b></p>{notes_result.replace(chr(10), '<br/>')}"
            else:
                kti_html += f"<p><b>{tr('Notes')}:</b><ul>"
                for n in notes:
                    kti_html += f"<li>{_html_mod.escape(n)}</li>"
                kti_html += "</ul></p>"

        people = marc.get('people', [])
        if people:
            people_text = '; '.join(people)
            kti_html += f"<p><b>{tr('People')}:</b> {_trans_or_badge('rd_people', people_text, tr('People'))}</p>"

        external_html = ""
        if part_meta:
            part_display = self.meta_mgr.codico_mgr.get_part_display_name(oxford_part_id)
            external_html += (
                f"<div style='background-color: {part_bg}; color:{text_color}; padding: 10px; "
                "margin-bottom: 10px; border-left: 3px solid #3498db; text-align: left;' dir='ltr'>"
            )
            external_html += f"<p><b>📖 {tr('Codicological Part')}:</b> {part_display}</p>"

            folio_range = part_meta.get('folio_range', [])
            if len(folio_range) == 2:
                if folio_range[0] == folio_range[1]:
                    external_html += f"<p><b>{tr('Folio')}:</b> {folio_range[0]}</p>"
                else:
                    external_html += f"<p><b>{tr('Folio Range')}:</b> {folio_range[0]} - {folio_range[1]}</p>"

            part_title = part_meta.get('title', '')
            if part_title:
                external_html += f"<p><b>{tr('Oxford Title')}:</b> {_trans_or_badge('rd_part_title', part_title, tr('Oxford Title'))}</p>"

            part_contents = part_meta.get('contents', '')
            if part_contents:
                external_html += f"<p><b>{tr('Contents')}:</b> {_trans_or_badge('rd_part_contents', part_contents, tr('Contents'))}</p>"

            external_html += "</div>"

        if external_meta:
            external_html += f"<div style='margin-bottom: 10px; text-align: left;' dir='ltr'><ul>"
            for k, v in external_meta.items():
                ext_key = f"rd_ext_{k}"
                external_html += f"<li><b>{tr(k)}:</b> {_trans_or_badge(ext_key, v, k)}</li>"
            external_html += "</ul></div>"

        is_rtl = self.layoutDirection() == Qt.LayoutDirection.RightToLeft
        dir_attr = "rtl" if is_rtl else "ltr"
        header_align = "right" if is_rtl else "left"
        kti_header = tr("Ktiv Info")
        if oxford_part_id:
            external_header = tr("Oxford Info")
        else:
            external_header = tr("Cambridge Info")

        html = f"<div style='font-family:Arial; color:{text_color}; background-color:{base_color};'>"
        if external_html:
            if is_rtl:
                first_title, first_html = kti_header, kti_html
                second_title, second_html = external_header, external_html
            else:
                first_title, first_html = external_header, external_html
                second_title, second_html = kti_header, kti_html

            html += (
                f"<table style='width:100%; border-collapse:collapse;' dir='{dir_attr}'>"
                f"<tr>"
                f"<th style='text-align:{header_align}; padding:4px; border-bottom:1px solid #ccc;'>{first_title}</th>"
                f"<th style='text-align:{header_align}; padding:4px; border-bottom:1px solid #ccc;'>{second_title}</th>"
                f"</tr>"
                f"<tr>"
                f"<td style='vertical-align:top; padding:6px;'>{first_html}</td>"
                f"<td style='vertical-align:top; padding:6px;'>{second_html}</td>"
                f"</tr></table>"
            )
        else:
            html += kti_html

        # Append FJMS catalog section
        parent = self._app
        if parent and hasattr(parent, '_build_fjms_catalog_html'):
            fjms_catalog = parent._build_fjms_catalog_html(self.current_sys_id, text_color)
            if fjms_catalog:
                html += fjms_catalog

        # Append PGP metadata section if available
        pgp_doc = getattr(self, '_rd_pgp_doc', None)
        if pgp_doc:
            if parent and hasattr(parent, '_build_pgp_extended_info_html'):
                pgp_html = parent._build_pgp_extended_info_html(pgp_doc, palette=palette, sys_id=getattr(self, 'current_sys_id', None))
                if pgp_html:
                    html += pgp_html

        html += "</div>"
        return html

    def _rd_refresh_extended_info(self):
        """Rebuild ResultDialog extended info with current toggle state."""
        rd_meta = getattr(self, '_rd_enrichment_meta', None)
        if not rd_meta:
            return
        scrollbar = self.txt_extended_info.verticalScrollBar()
        scroll_pos = scrollbar.value() if scrollbar else 0

        oxford_part_id = rd_meta.get('_part_id')
        part_meta = rd_meta.get('_part_meta')
        html = self._rd_build_extended_html(rd_meta, oxford_part_id, part_meta)
        if html:
            self.txt_extended_info.setHtml(html)
        if scrollbar:
            scrollbar.setValue(scroll_pos)

    def _rd_toggle_edit_mode(self):
        """Toggle edit mode in ResultDialog."""
        parent = self._app
        if not parent or not hasattr(parent, 'corrections_client'):
            return
        if not parent.corrections_client.is_logged_in():
            QMessageBox.warning(self, tr("Login Required"), tr("Please login to edit."))
            return

        if not hasattr(self, '_rd_edit_mode'):
            self._rd_edit_mode = False

        self._rd_edit_mode = not self._rd_edit_mode

        if self._rd_edit_mode:
            # Enter edit mode - reset draft tracking
            self._rd_draft_correction_id = None
            self._rd_original_edit_text = self.text_ms.toPlainText()
            self.text_ms.setReadOnly(False)
            # Use palette-aware colors for dark mode support
            palette = self.palette()
            is_dark = palette.color(QPalette.ColorRole.Window).lightness() < 128
            if is_dark:
                edit_bg = "#3d3522"  # Dark yellowish for dark mode
            else:
                edit_bg = "#fffacd"  # Light lemon for light mode
            self.text_ms.setStyleSheet(f"background-color: {edit_bg}; border: 2px solid #f39c12;")
            # Show edit action buttons
            self.btn_rd_save_draft.setVisible(True)
            self.btn_rd_submit.setVisible(True)
            self.btn_rd_cancel_edit.setVisible(True)
            self.rd_edit_status.setVisible(True)
            self.btn_rd_edit.setText(tr("✏️ Editing..."))
            self.btn_rd_edit.setStyleSheet("background-color: #f39c12; color: white;")
            self.text_ms.textChanged.connect(self._rd_on_text_changed)
        else:
            self._rd_exit_edit_mode()

    def _rd_exit_edit_mode(self):
        """Exit edit mode."""
        self._rd_edit_mode = False
        self._rd_draft_correction_id = None
        try:
            self.text_ms.textChanged.disconnect(self._rd_on_text_changed)
        except (RuntimeError, AttributeError):
            pass
        self.text_ms.setReadOnly(True)
        self.text_ms.setStyleSheet("")
        # Hide edit action buttons
        self.btn_rd_save_draft.setVisible(False)
        self.btn_rd_submit.setVisible(False)
        self.btn_rd_cancel_edit.setVisible(False)
        self.rd_edit_status.setVisible(False)
        self.rd_edit_status.setText("")
        self.btn_rd_edit.setText(tr("✏️ Edit"))
        self.btn_rd_edit.setStyleSheet("")

    def _rd_on_text_changed(self):
        """Handle text changes in edit mode."""
        current = self.text_ms.toPlainText()
        has_changes = current != getattr(self, '_rd_original_edit_text', '')
        draft_id = getattr(self, '_rd_draft_correction_id', None)
        self.btn_rd_save_draft.setEnabled(has_changes)
        # Enable submit if has changes OR has saved draft
        self.btn_rd_submit.setEnabled(has_changes or draft_id is not None)

        # Get palette-aware background color
        palette = self.palette()
        is_dark = palette.color(QPalette.ColorRole.Window).lightness() < 128
        edit_bg = "#3d3522" if is_dark else "#fffacd"

        if has_changes:
            self.rd_edit_status.setText(tr("Modified"))
            self.rd_edit_status.setStyleSheet("color: #e67e22;")
            # Orange border for unsaved changes
            self.text_ms.setStyleSheet(f"background-color: {edit_bg}; border: 2px solid #f39c12;")
        elif draft_id:
            self.rd_edit_status.setText(f"✓ {tr('Saved')}")
            self.rd_edit_status.setStyleSheet("color: #27ae60; font-weight: bold;")
            # Green border for saved draft
            self.text_ms.setStyleSheet(f"background-color: {edit_bg}; border: 2px solid #27ae60;")
        else:
            self.rd_edit_status.setText("")
            # Orange border (default edit mode)
            self.text_ms.setStyleSheet(f"background-color: {edit_bg}; border: 2px solid #f39c12;")

    def _rd_cancel_edit(self):
        """Cancel edit mode and restore original text."""
        if hasattr(self, '_rd_original_edit_text'):
            self._rd_display_text(self._rd_original_edit_text)
        self._rd_exit_edit_mode()

    def _rd_save_correction(self, submit=False):
        """Save correction from ResultDialog."""
        parent = self._app
        if not parent or not hasattr(parent, 'corrections_client'):
            return

        new_text = self.text_ms.toPlainText()
        original = getattr(self, '_rd_original_edit_text', new_text)

        # Check if there are changes OR if we have a saved draft to submit
        draft_correction_id = getattr(self, '_rd_draft_correction_id', None)
        has_changes = new_text != original

        if not has_changes and not draft_correction_id:
            if submit:
                QMessageBox.information(self, tr("No Changes"), tr("No changes were made to the text."))
            return

        notes = None
        if submit:
            notes, ok = QInputDialog.getMultiLineText(
                self, tr("Correction Notes"),
                tr("Please provide a brief explanation for your correction (optional):"), ""
            )
            if not ok:
                return
            notes = notes if notes else None

        try:
            # If submitting an existing draft, try submit_correction API first
            if submit and draft_correction_id:
                success, message = parent.corrections_client.submit_correction(draft_correction_id, notes)
                if success or 'approved' in message.lower():
                    # Success, or already approved (which means it succeeded earlier)
                    QMessageBox.information(self, tr("Correction Submitted"),
                        tr("Your correction has been submitted for review. Thank you for your contribution!"))
                    self._rd_exit_edit_mode()
                    self._rd_original_edit_text = new_text
                    self._rd_draft_correction_id = None
                    # Refresh versions to show the submitted correction
                    self._rd_refresh_versions()
                else:
                    # Submit failed, try creating a new pending correction instead
                    correction, create_msg = parent.corrections_client.create_correction(
                        document_id=self.current_sys_id,
                        original_text=original if original else new_text,
                        corrected_text=new_text,
                        correction_type="text_correction",
                        page_number=self.current_p_num,
                        notes=notes,
                        shelfmark=self.lbl_shelf.text(),
                        system_id=self.current_sys_id,
                        status='pending',
                        ie_id=getattr(self, 'current_volume_ie', None)
                    )
                    if correction:
                        QMessageBox.information(self, tr("Correction Submitted"),
                            tr("Your correction has been submitted for review. Thank you for your contribution!"))
                        self._rd_exit_edit_mode()
                        self._rd_original_edit_text = new_text
                        self._rd_draft_correction_id = None
                        self._rd_refresh_versions()
                    else:
                        QMessageBox.warning(self, tr("Error"), f"{tr('Failed to submit correction')}: {create_msg}")
            else:
                # Create new correction (draft or direct submit)
                correction, message = parent.corrections_client.create_correction(
                    document_id=self.current_sys_id,
                    original_text=original if original else new_text,
                    corrected_text=new_text,
                    correction_type="text_correction",
                    page_number=self.current_p_num,
                    notes=notes,
                    shelfmark=self.lbl_shelf.text(),
                    system_id=self.current_sys_id,
                    status='pending' if submit else 'draft',
                    save_as_draft=not submit,  # Don't auto-submit when saving as draft
                    ie_id=getattr(self, 'current_volume_ie', None)
                )
                if correction:
                    if submit:
                        QMessageBox.information(self, tr("Correction Submitted"),
                            tr("Your correction has been submitted for review. Thank you for your contribution!"))
                        self._rd_exit_edit_mode()
                        self._rd_original_edit_text = new_text
                        self._rd_draft_correction_id = None
                        # Refresh versions to show the submitted correction
                        self._rd_refresh_versions()
                    else:
                        self.rd_edit_status.setText(f"✓ {tr('Saved')}")
                        self.rd_edit_status.setStyleSheet("color: #27ae60; font-weight: bold;")
                        self._rd_draft_correction_id = correction.id  # Store draft ID for later submit
                        self._rd_original_edit_text = new_text  # Update original to mark as saved
                        # Update border to green (saved)
                        palette = self.palette()
                        is_dark = palette.color(QPalette.ColorRole.Window).lightness() < 128
                        edit_bg = "#3d3522" if is_dark else "#fffacd"
                        self.text_ms.setStyleSheet(f"background-color: {edit_bg}; border: 2px solid #27ae60;")
                        # Keep submit button enabled after saving draft
                        self.btn_rd_submit.setEnabled(True)
                        self.btn_rd_save_draft.setEnabled(False)  # Disable save since no changes
                        # Refresh versions to show the draft
                        self._rd_refresh_versions()
                else:
                    QMessageBox.warning(self, tr("Error"), f"{tr('Failed to save correction')}: {message}")
        except Exception as e:
            QMessageBox.warning(self, tr("Error"), f"{tr('Failed to save correction')}: {str(e)}")

    def _refresh_find_highlights(self):
        apply_find_highlight(self.text_ms, self.find_ms_input.text().strip())

    def _scroll_to_first_highlight(self, text_browser, pattern_str):
        """Scroll text_browser viewport to the first regex match of pattern_str.

        Deferred via QTimer.singleShot(0) so it runs after Qt layout — the
        dialog renders before .exec(), and before the widget is mapped
        ensureCursorVisible() has no viewport geometry to scroll within.

        Uses toPlainText() instead of regex-on-HTML because setHtml() wraps
        matches in <b style='color:red;'>...</b> and the original
        highlight_pattern won't match cleanly across tags.
        """
        if not pattern_str or text_browser is None:
            return
        def _do_scroll():
            try:
                flags = re.IGNORECASE
                if '\\n' in pattern_str or pattern_str.startswith('^') or '^\\' in pattern_str:
                    flags |= re.MULTILINE
                regex = re.compile(pattern_str, flags)
            except re.error:
                return
            plain = text_browser.toPlainText()
            m = regex.search(plain)
            if not m:
                return
            cursor = text_browser.textCursor()
            cursor.setPosition(m.start())
            text_browser.setTextCursor(cursor)
            text_browser.ensureCursorVisible()
        QTimer.singleShot(0, _do_scroll)

    def _apply_source_highlights(self, text, pattern_str):
        if not text:
            return ""
        if pattern_str and '*' not in text:
            try:
                flags = re.IGNORECASE
                if '\\n' in pattern_str or pattern_str.startswith('^') or '^\\' in pattern_str:
                    flags |= re.MULTILINE
                regex = re.compile(pattern_str, flags)
                text = regex.sub(r'*\g<0>*', text)
            except re.error:
                pass
        return text

    def open_external_link(self):
        if self.external_url:
            url = self.external_url
            # Transform CUDL IIIF manifest URL to viewer URL
            if "cudl.lib.cam.ac.uk/iiif/" in url:
                url = url.replace("/iiif/", "/view/")
            QDesktopServices.openUrl(QUrl(url))

    def _rd_open_local_file(self):
        """Phase 95 smoke-fix (E): launch the LOCAL source file in the OS default app.

        WR-03 defense-in-depth: refuse to launch a file whose extension is
        not in the LOCAL supported set.
        """
        import os
        filepath = getattr(self, '_rd_local_filepath', None)
        if not filepath or not os.path.exists(filepath):
            return
        ext = os.path.splitext(filepath)[1].lower()
        if ext not in {'.docx', '.pdf', '.txt'}:
            return
        os.startfile(filepath)  # Windows-native

    def _htmlify(self, text):
        """Convert plain LOCAL file text to safe HTML for QTextBrowser.

        Phase 96 fix-7 (Codex P1.3): HTML-escape raw file content BEFORE
        applying newline→<br> and *...*→<b> substitutions, matching the
        Browse panel pattern in _open_local_browse_page.  Raw `<`, `>`, `&`
        in user files can no longer inject markup into ResultDialog.
        """
        if not text: return ""
        import html as _html_mod
        # 1. Escape raw file content so `<`, `>`, `&` become entities.
        t = _html_mod.escape(text)
        # 2. Convert newlines to <br> (safe — content is already escaped).
        t = t.replace("\n", "<br>")
        # 3. Apply *...*  → <b> highlighting (markers placed by regex before
        #    this call; they are unaffected by step 1 because `*` has no HTML
        #    special meaning and escape() leaves it unchanged).
        t = re.sub(r'\*(.*?)\*', r"<b style='color:red;'>\1</b>", t)
        return f"<div dir='rtl'>{t}</div>"

    def _apply_manual_highlights_to_text(self, text, uid):
        if not text or not uid:
            return text
        spans = []
        for ph in self.data.get('page_highlights', []) if self.data else []:
            if ph.get('uid') == uid:
                span = ph.get('span')
                if span and len(span) == 2:
                    spans.append(span)
        if not spans:
            return text
        # Apply in reverse order to keep indices stable
        spans.sort(key=lambda s: s[0], reverse=True)
        for s, e in spans:
            if s is None or e is None:
                continue
            if s < 0 or e > len(text) or s >= e:
                continue
            text = text[:e] + "*" + text[e:]
            text = text[:s] + "*" + text[s:]
        return text

    def load_result_by_index(self, idx):
        data = self.all_results[idx]
        if not data.get('full_text'):
            uid = data.get('uid')
            if uid:
                data['full_text'] = self.searcher.get_full_text_by_id(uid) or data.get('text', '')
            else:
                # Tag search results: get full text by sys_id from display dict
                sid = data.get('display', {}).get('id', '')
                if sid and self.searcher:
                    pages = self.searcher.get_full_manuscript(sid)
                    data['full_text'] = '\n'.join(p['text'] for p in pages if p.get('text')) if pages else data.get('text', '')
                else:
                    data['full_text'] = data.get('text', '')
        self.data = data

        # Phase 95 smoke-fix (E): show "Open file" button for LOCAL hits only.
        try:
            from shared.local_sys_id import is_local_sys_id as _is_local
            _src_id = (data.get('display', {}) or {}).get('id', '')
            _is_local_hit = bool(_src_id and _is_local(_src_id) and self._app)
            if _is_local_hit:
                # Look up filepath from the indexer via parent app helper
                _fp = None
                if hasattr(self._app, '_lookup_local_filepath'):
                    _fp = self._app._lookup_local_filepath(_src_id)
                self._rd_local_filepath = _fp
                self.btn_rd_open_file.setVisible(bool(_fp))
            else:
                self._rd_local_filepath = None
                self.btn_rd_open_file.setVisible(False)
        except Exception:
            self._rd_local_filepath = None
            _is_local_hit = False
            self.btn_rd_open_file.setVisible(False)

        # Phase 100 (REVIEWS HIGH-2/HIGH-3): if the newly-shown result is NOT a LOCAL PDF
        # (non-LOCAL, or LOCAL non-PDF), cancel any in-flight render for this dialog's scope
        # and hide the pane. LOCAL PDF results are rendered by load_local_page's trigger; we
        # do NOT request here (avoids duplicate-on-open). cancel (not discard_scope) — the
        # dialog scope is still live and may be re-requested when navigating back to a LOCAL PDF.
        _ctrl = self._pdf_controller()
        _is_local_pdf = bool(_is_local_hit) and _ctrl is not None and _ctrl.is_pdf(getattr(self, '_rd_local_filepath', None))
        if not _is_local_pdf:
            self._cancel_local_pdf_image()

        # v7.15.0 bug fix: LOCAL hits populate the SAME lbl_shelf + lbl_title that
        # Genizah hits use (via apply_metadata), so navigating LOCAL ↔ Genizah cannot
        # leak stale values from the prior result type. Filename → lbl_shelf; the
        # parent/folder path → lbl_title; chunk_locator (e.g. "p. 3") appended to
        # title for page context.
        try:
            if _is_local_hit:
                _display = data.get('display', {}) or {}
                _local_path = (
                    _display.get('shelfmark') or
                    getattr(self, '_rd_local_filepath', None) or
                    ''
                )
                if _local_path:
                    import os as _os
                    _fname = _os.path.basename(_local_path)
                    _dir = _os.path.dirname(_local_path)
                    _folder = _os.path.basename(_dir)
                    _parent = _os.path.basename(_os.path.dirname(_dir))
                    if _parent:
                        _local_title = f"{_parent}/{_folder}"
                    else:
                        _local_title = _folder or ''
                    _chunk_loc = data.get("chunk_locator", "") or ""
                    if _chunk_loc:
                        _local_title = f"{_local_title} — {_chunk_loc}" if _local_title else _chunk_loc
                    self.lbl_shelf.setText(_fname)
                    self.lbl_shelf.setToolTip(_local_path)
                    _set_label_with_tooltip(self.lbl_title, _local_title)
                    if hasattr(self, 'lbl_compact_shelf'):
                        self.lbl_compact_shelf.setText(_fname)
                else:
                    self.lbl_shelf.setText('')
                    self.lbl_title.setText('')
                    if hasattr(self, 'lbl_compact_shelf'):
                        self.lbl_compact_shelf.setText('')
        except Exception:
            pass

        # Phase 96 fix-8 (Issue 3): disable Genizah-only community buttons for
        # LOCAL hits.  These actions have no meaning for user-owned LOCAL files
        # (no Friedberg/NLI catalog entries, no community corrections/comments).
        # They are re-enabled when a non-LOCAL result is shown (see else branch).
        try:
            _genizah_only_btns = [
                'btn_img',           # "Go to Ktiv"
                'btn_add_to_puzzle', # Fragment Puzzle
                'btn_rd_edit',       # Edit (corrections)
                'btn_comment',       # Comment
                'btn_view_corrections',  # View Corrections
                'btn_joins',         # Joins
            ]
            if _is_local_hit:
                for _bn in _genizah_only_btns:
                    _btn = getattr(self, _bn, None)
                    if _btn is not None:
                        _btn.setEnabled(False)
                if hasattr(self, 'rd_version_combo'):
                    self.rd_version_combo.setEnabled(False)
            else:
                for _bn in _genizah_only_btns:
                    _btn = getattr(self, _bn, None)
                    if _btn is not None:
                        _btn.setEnabled(True)
                # rd_version_combo re-enable is handled by enrichment callback
        except Exception:
            pass

        # Nav UI Updates
        self.lbl_res_count.setText(tr("Result {} of {}").format(idx + 1, len(self.all_results)))
        self.btn_res_prev.setEnabled(idx > 0)
        self.btn_res_next.setEnabled(idx < len(self.all_results) - 1)

        # Parse Meta
        ids = self.meta_mgr.parse_full_id_components(data.get('raw_header', ''))
        prev_sys_id = self.current_sys_id
        self.current_sys_id = ids['sys_id']
        if not self.current_sys_id:
            # Fallback for tag search results: get sys_id from display dict
            self.current_sys_id = data.get('display', {}).get('id', '')
        self.current_fl_id = ids.get('fl_id')
        try: p = int(ids['p_num'])
        except (ValueError, TypeError, KeyError): p = 1
        # Phase 96 fix-4: for LOCAL hits, raw_header is empty so ids['p_num']
        # is always None → p stays 1. Use 'img' or 'p_num' from the result
        # dict directly (both hold the page/chunk number for LOCAL hits).
        try:
            from shared.local_sys_id import is_local_sys_id as _is_local
            if _is_local(self.current_sys_id):
                local_p = data.get('img') or data.get('p_num')
                if local_p is not None:
                    p_try = int(local_p)
                    if p_try >= 1:
                        p = p_try
        except Exception:
            pass

        # Extract volume_ie from result header for multi-IE manuscripts (Phase 60)
        ie_from_header = ids.get('ie_id')
        if self.current_sys_id != prev_sys_id:
            # New manuscript — reset and re-extract
            self.current_volume_ie = None
        if ie_from_header and self.current_sys_id:
            from genizah_core import get_volumes_for_sys_id
            volumes = get_volumes_for_sys_id(self.current_sys_id)
            if len(volumes) > 1:
                self.current_volume_ie = ie_from_header

        # Add to Recently Viewed
        parent = self._app
        if parent and hasattr(parent, 'lists_mgr') and parent.lists_mgr and self.current_sys_id:
            fl_id = parent._normalize_fl_id(ids.get('fl_id'))
            parent.lists_mgr.add_to_recent(self.current_sys_id, fl_id=fl_id, img=ids.get('p_num'))

        # --- Prepare Text Content ---
        # 1. Manuscript Text (Apply Pattern!)
        ms_raw = data.get('full_text', '') or data.get('text', '')
        pattern_str = data.get('highlight_pattern') # Get regex pattern
        
        if pattern_str:
            try:
                # Apply Regex to clean full-text to verify highlighting on load
                flags = re.IGNORECASE
                if '\\n' in pattern_str or pattern_str.startswith('^') or '^\\' in pattern_str:
                    flags |= re.MULTILINE
                regex = re.compile(pattern_str, flags)
                ms_raw = regex.sub(r'*\g<0>*', ms_raw)
            except re.error:
                pass

        # Phase 999.4: route through gutter helper (source_text = raw `ms_raw`)
        apply_line_numbered_text(
            self.text_ms, self._htmlify(ms_raw), source_text=ms_raw, is_html=True,
        )
        self._refresh_find_highlights()
        
        # 2. Source Context
        source_text = ""
        if 'source_ctx' in data:
            parent = self._app
            if parent and hasattr(parent, "comp_text_area"):
                source_text = parent.comp_text_area.toPlainText().strip()
            if not source_text:
                source_text = data.get('source_ctx', '')
        source_text = self._apply_source_highlights(source_text, pattern_str)
        if source_text:
            self.src_widget.setVisible(True)
            self.text_src.setHtml(self._htmlify(source_text))
            self._scroll_to_first_highlight(self.text_src, pattern_str)
        else:
            self.src_widget.setVisible(False)
            self.text_src.clear()
        
        # Load Page & Metadata
        self.load_page(target=p)

        # Preload next result
        self._preload_next_result(idx + 1)

    def _preload_next_result(self, next_idx):
        if next_idx >= len(self.all_results): return
        res = self.all_results[next_idx]

        # Extract SID logic from load_result
        meta = res.get('display', {})
        parsed = self.meta_mgr.parse_full_id_components(res.get('raw_header', ''))
        sid = parsed['sys_id'] or meta.get('id')

        if not sid: return

        # Trigger Enrich Fetch (caches metadata)
        # We don't connect signals, just run it
        self.preload_meta_worker = EnrichMetadataThread(self.meta_mgr, sid)
        self.preload_meta_worker.start()

    def load_by_shelfmark(self, shelfmark: str, page_num: int = 1):
        """Load a document by shelfmark within the same dialog."""
        try:
            parent = self._app
            if not parent:
                return False

            # Look up sys_id from shelfmark
            if hasattr(parent, '_ensure_shelf_map'):
                parent._ensure_shelf_map()
            if hasattr(parent, '_normalize_shelfmark') and hasattr(parent, '_shelf_to_sys'):
                norm = parent._normalize_shelfmark(shelfmark)
                sys_id = parent._shelf_to_sys.get(norm) if norm else None
            else:
                return False

            if not sys_id:
                QMessageBox.warning(self, tr("Error"), tr("Document not found: {}").format(shelfmark))
                return False

            # Get page data (volume_ie reset since navigating by shelfmark)
            self.current_volume_ie = None
            page_data = self.searcher.get_browse_page(sys_id, p_num=page_num)
            if not page_data:
                QMessageBox.warning(self, tr("View Error"), tr("Could not load manuscript data."))
                return False

            try:
                shelfmark_display, title = self.meta_mgr.get_meta_for_id(sys_id)
            except (KeyError, AttributeError, IndexError):
                shelfmark_display = shelfmark
                title = ""

            # Create result dict
            result = {
                'uid': page_data.get('uid', ''),
                'raw_header': page_data.get('full_header', ''),
                'full_header': page_data.get('full_header', ''),
                'text': page_data.get('text', ''),
                'full_text': page_data.get('text', ''),
                'display': {
                    'id': sys_id,
                    'shelfmark': shelfmark_display,
                    'title': title,
                    'img': str(page_num),
                    'source': ''
                }
            }

            # Add to results and navigate
            self.all_results.append(result)
            new_idx = len(self.all_results) - 1
            self.current_result_idx = new_idx
            self.load_result_by_index(new_idx)
            return True
        except Exception as e:
            logger.exception("load_by_shelfmark failed: %s", e)
            return False

    def load_page(self, offset=0, target=None):
        if not self.current_sys_id: return
        # Phase 96 NEW-2 dispatch: LOCAL hits use a separate primitive
        # (get_local_browse_page vs get_browse_page). Branch BEFORE
        # cancel_image_thread() because LOCAL has no IIIF image to cancel.
        try:
            from shared.local_sys_id import is_local_sys_id as _is_local
            if _is_local(self.current_sys_id):
                return self.load_local_page(offset=offset, target=target)
        except ImportError:
            pass  # Fall through to Genizah path on import failure (defensive)
        self.cancel_image_thread()
        
        # Determine strict navigation source
        # If target (Spinbox jump) is set -> Use p_num logic (target)
        # If offset (Next/Prev) is set -> Use internal_index logic (prevents loops)
        
        page_data = None
        
        if target is not None:
            # Jump by number (user typed in box)
            try: p = int(target)
            except (ValueError, TypeError): p = 1
            page_data = self.searcher.get_browse_page(self.current_sys_id, p_num=p, next_prev=0, allow_cross=True, volume_ie=self.current_volume_ie)
        else:
            # Relative Navigation (Next/Prev)
            # Use internal index if we have it, otherwise rely on p_num
            idx_arg = self.current_internal_idx
            p_arg = int(self.current_p_num) if self.current_p_num is not None else None

            page_data = self.searcher.get_browse_page(
                self.current_sys_id,
                p_num=p_arg,
                next_prev=offset,
                absolute_index=idx_arg, # <--- THIS FIXES THE BUG
                allow_cross=True,
                volume_ie=self.current_volume_ie
            )
            
        if not page_data: return

        # --- UPDATE STATE ---
        new_sys = page_data.get('sys_id', self.current_sys_id)
        if new_sys and new_sys != self.current_sys_id:
            self.current_volume_ie = None  # Reset volume on cross-manuscript nav
            self.current_sys_id = new_sys

        self.current_p_num = page_data['p_num']
        self.current_internal_idx = page_data['internal_index'] # <--- SAVE IT
        
        parsed_new = self.meta_mgr.parse_full_id_components(page_data['full_header'])
        self.current_fl_id = parsed_new['fl_id']
        self.current_full_header = page_data.get('full_header', '')
        self.current_page_text = page_data.get('text', '')
        self.current_page_uid = page_data.get('uid')
        self._update_add_to_list_button()

        # Keep the dialog's data object aligned with the currently displayed folio
        if self.data is not None:
            self.data['raw_header'] = page_data.get('full_header', self.data.get('raw_header', ''))
            self.data['uid'] = page_data.get('uid', self.data.get('uid'))
            self.data['full_text'] = page_data.get('text', self.data.get('full_text', ''))
            display_block = self.data.get('display', {})
            display_block['id'] = self.current_sys_id
            self.data['display'] = display_block

        # Update Info Label
        info_html = f"<b>{tr('Sys')}:</b> {self.current_sys_id} | <b>{tr('FL')}:</b> {self.current_fl_id or '?'}"
        self.lbl_info.setText(info_html)

        # Update Page Controls
        self.spin_page.blockSignals(True); self.spin_page.setValue(self.current_p_num); self.spin_page.blockSignals(False)
        self.lbl_total.setText(f"/ {page_data['total_pages']}")

        # Sync compact bar page label
        if hasattr(self, 'lbl_compact_page') and self.compact_bar.isVisible():
            self.lbl_compact_page.setText(f"{self.current_p_num} {self.lbl_total.text()}")

        # 2. Sync Image (Non-Blocking)
        if self.btn_external_view.isChecked():
            QTimer.singleShot(0, self.sync_external_view)

        # --- Render Text ---
        raw_text = page_data['text']
        raw_text = self._apply_manual_highlights_to_text(raw_text, self.current_page_uid)
        pattern_str = self.data.get('highlight_pattern')
        
        if pattern_str:
            try:
                flags = re.IGNORECASE
                if '\\n' in pattern_str or pattern_str.startswith('^') or '^\\' in pattern_str:
                    flags |= re.MULTILINE
                regex = re.compile(pattern_str, flags)
                highlighted_text = regex.sub(r'*\g<0>*', raw_text)
                raw_text = highlighted_text
            except re.error: pass

        # Phase 999.4: route through gutter helper (source_text = raw `raw_text`)
        apply_line_numbered_text(
            self.text_ms, self._htmlify(raw_text), source_text=raw_text, is_html=True,
        )
        self._refresh_find_highlights()
        self._scroll_to_first_highlight(self.text_ms, pattern_str)

        # Load versions for this page
        self._rd_load_versions()

        # Start PGP source fetch for this page (runs in background)
        # Reset PGP and enriched data flags for new result
        self._rd_pgp_doc = None
        self._rd_pgp_sources = []
        self._rd_enriched_data_loaded = False
        self._rd_fjms_bib = []
        self._rd_marc_bib = []
        self._rd_catalog_detail = None
        self.btn_rd_bib_fjms.setVisible(False)
        self.btn_rd_bib_nli.setVisible(False)
        self.btn_rd_catalog.setVisible(False)
        self.btn_rd_catalog.setEnabled(False)
        if hasattr(self, 'btn_compact_bib_fjms'):
            self.btn_compact_bib_fjms.setVisible(False)
            self.btn_compact_bib_nli.setVisible(False)
        if hasattr(self, 'btn_compact_catalog'):
            self.btn_compact_catalog.setVisible(False)
        self._rd_measurements_data = None
        self.btn_rd_measurements.setVisible(False)
        self.btn_rd_measurements.setEnabled(False)
        if hasattr(self, 'btn_compact_measurements'):
            self.btn_compact_measurements.setVisible(False)
        parent = self._app
        if parent:
            # Disconnect old worker signals first to prevent stale results
            if hasattr(self, '_rd_pgp_worker') and self._rd_pgp_worker is not None:
                try:
                    self._rd_pgp_worker.finished_signal.disconnect(self._on_rd_pgp_loaded)
                    self._rd_pgp_worker.error_signal.disconnect(self._on_rd_pgp_error)
                except (TypeError, RuntimeError):
                    pass
            self._rd_pgp_worker = PGPSourceWorker(self.current_sys_id, self.current_p_num or 1)
            self._rd_pgp_worker.finished_signal.connect(self._on_rd_pgp_loaded)
            self._rd_pgp_worker.error_signal.connect(self._on_rd_pgp_error)
            self._rd_pgp_worker.start()

        # Update joins menu
        self._rd_update_joins_menu()

        # Update Domain info + start enrichment (AFTER reset so buttons aren't wiped)
        self._update_rd_domain_label()

    def load_local_page(self, offset=0, target=None):
        """Phase 96 NEW-2: LOCAL analog to load_page.

        Page identity contract (Codex Item 4):
          - p_num   : physical page number in the source file (sparse for PDFs —
                      blank pages are skipped by the indexer; PDFs can have
                      p_num=1552 with only 1529 indexed pages before it).
          - current_idx : 0-based ordinal in the SORTED indexed page list.
                      Dense. Used only for prev/next enabled-state arithmetic.

        The spinbox always shows and accepts p_num (physical page number).
        current_idx is kept only to evaluate whether prev/next are possible.

        Calls SearchEngine.get_local_browse_page (plan 96-03) and applies the
        same state-update code Genizah hits use. The engine primitive returns
        None at file boundaries — we disable prev/next accordingly (D-12: no
        wrap). Returns None for a p_num not present in the index (Item 5:
        fall-through on missing page — spinner stays on last valid value).

        PINNED widget identifiers (BLOCKER 2):
          - self.text_ms, self.btn_compact_pg_{prev,next}, self.btn_pg_{prev,next},
            self.spin_page, self.lbl_total.
        Render via apply_line_numbered_text + self._htmlify to preserve v7.12.0
        gutter AND HTML-escape file content (W11 + Codex HIGH #4).
        Skip cancel_image_thread() — Genizah-image-specific (W9).
        No setFocus() — Item 2: focus hack removed; Enter-only commit is now
        handled by _commit_spin_page_jump via returnPressed (Item 1).
        """
        if not self.current_sys_id:
            return

        # Fetch the page dict from the engine primitive (plan 96-03).
        if target is not None:
            try:
                p = int(target)
            except (ValueError, TypeError):
                p = 1
            page_data = self.searcher.get_local_browse_page(
                self.current_sys_id, p_num=p, next_prev=0
            )
        else:
            p_arg = int(self.current_p_num) if self.current_p_num is not None else None
            page_data = self.searcher.get_local_browse_page(
                self.current_sys_id, p_num=p_arg, next_prev=offset
            )

        if not page_data:
            # D-12: no wrap at boundaries; also None for unknown p_num (Item 5).
            # Disable the offending direction button. PINNED identifiers.
            if offset > 0:
                self._set_local_page_nav_enabled(prev=None, nxt=False)
            elif offset < 0:
                self._set_local_page_nav_enabled(prev=False, nxt=None)
            # target jump to unknown p_num: leave spinner at previous valid value.
            return

        # State updates (mirror Genizah load_page state update block).
        self.current_p_num = page_data.get('p_num')
        self.current_internal_idx = page_data.get('internal_index')
        total = page_data.get('total_pages', 0)
        # current_idx is 1-based ordinal in the sorted page list (dense).
        # Used ONLY for prev/next enabled state — NOT for spinbox display.
        cur_idx = page_data.get('current_idx', 1)
        # max_p_num is the highest physical page number in the index — used to
        # set the spinbox upper bound so the user sees a meaningful range.
        max_p_num = page_data.get('max_p_num') or max(self.current_p_num or 1, total)

        # Prepare text + apply highlight markers (mirror Genizah render path).
        text = page_data.get('text', '') or ''
        pattern_str = page_data.get('highlight_pattern', '')
        if not pattern_str and self.data:
            pattern_str = self.data.get('highlight_pattern', '')
        if pattern_str:
            try:
                import re as _re
                flags = _re.IGNORECASE
                if '\\n' in pattern_str or pattern_str.startswith('^') or '^\\' in pattern_str:
                    flags |= _re.MULTILINE
                regex = _re.compile(pattern_str, flags)
                text = regex.sub(r'*\g<0>*', text)
            except Exception:
                pass

        # Render via apply_line_numbered_text — preserves v7.12.0 gutter (W11).
        # self._htmlify HTML-escapes the text (Codex HIGH #4 closure — same
        # escape path the Genizah render uses; raw `<`, `&` in file content
        # cannot inject markup).
        # PINNED: self.text_ms is the text widget.
        # DO NOT use setHtml — bypasses the gutter helper.
        apply_line_numbered_text(
            self.text_ms,
            self._htmlify(text),
            source_text=text,
            is_html=True,
        )
        self._refresh_find_highlights()
        self._scroll_to_first_highlight(self.text_ms, pattern_str)

        # Item 4: spinbox displays p_num (physical page), NOT cur_idx (ordinal).
        # Spinbox max is set from max_p_num so the user sees a sensible upper
        # bound even when pages are sparse (e.g., PDF p_num=1552, total=1529).
        self.spin_page.blockSignals(True)
        self.spin_page.setValue(self.current_p_num)
        self.spin_page.setMaximum(max(max_p_num, self.current_p_num or 1))
        self.spin_page.blockSignals(False)
        self.lbl_total.setText(f"/ {total}")

        # Item 4+6: update BOTH full-size and compact prev/next buttons.
        # cur_idx is the 1-based position in the sorted indexed page list; it is
        # correct for boundary detection regardless of p_num sparseness.
        self._set_local_page_nav_enabled(prev=(cur_idx > 1), nxt=(cur_idx < total))

        # Sync compact bar page label if present (mirrors Genizah path).
        if hasattr(self, 'lbl_compact_page') and self.compact_bar.isVisible():
            self.lbl_compact_page.setText(f"{self.current_p_num} {self.lbl_total.text()}")

        # Phase 100 (PDFIMG-03 / D-06): render the LOCAL page image for the now-shown page.
        # This is the SINGLE render trigger (REVIEWS HIGH-3): both initial open and
        # prev/next RESULT reach here via load_result_by_index -> load_page -> load_local_page,
        # and within-document prev/next PAGE also lands here. Placed on the success path so the
        # early-return at boundary/unknown pages does NOT request a render.
        self._render_local_pdf_image()

    def _set_local_page_nav_enabled(self, prev=None, nxt=None):
        """Item 6 (Codex): shared update path for BOTH full-size and compact nav buttons.

        Calling convention: pass True/False to set, None to leave unchanged.
        Updates self.btn_pg_prev / self.btn_pg_next (full-size header nav, Item 6)
        AND self.btn_compact_pg_prev / self.btn_compact_pg_next (compact bar).
        Having a single helper prevents the compact-only bug where the full-size
        buttons were never updated in LOCAL state.
        """
        if prev is not None:
            self.btn_pg_prev.setEnabled(prev)
            self.btn_compact_pg_prev.setEnabled(prev)
        if nxt is not None:
            self.btn_pg_next.setEnabled(nxt)
            self.btn_compact_pg_next.setEnabled(nxt)

    def _update_rd_domain_label(self):
        """Update domain info label and printed badge for the current result in ResultDialog."""
        parent = self._app
        if not parent or not hasattr(parent, '_result_domain_map'):
            self.lbl_rd_domains.setVisible(False)
            self.lbl_rd_printed.setVisible(False)
            return

        domain_names = parent._result_domain_map.get(self.current_sys_id, [])
        if domain_names:
            display_names = [parent._domain_display_name(d) for d in domain_names] if hasattr(parent, '_domain_display_name') else domain_names
            self.lbl_rd_domains.setText(" | " + tr("Domain") + ": " + ", ".join(display_names))
            self.lbl_rd_domains.setVisible(True)
        else:
            self.lbl_rd_domains.setVisible(False)

        # Printed material badge — check search tab cache first, then FJMS direct lookup
        printed_ids = getattr(parent, '_printed_sys_ids', None) or getattr(parent, '_comp_printed_sys_ids', None) or set()
        is_printed = self.current_sys_id and self.current_sys_id in printed_ids
        if not is_printed and self.current_sys_id and not printed_ids:
            try:
                from shared.fjms_service import get_fjms_service
                fjms_svc = get_fjms_service()
                if fjms_svc.is_available():
                    is_printed = bool(fjms_svc.get_printed_sys_ids([self.current_sys_id]))
            except Exception:
                pass  # Cache operation failed; continue without cached data
        if is_printed:
            _printed_tag = '\u05d3\u05e4\u05d5\u05e1' if CURRENT_LANG == 'he' else 'Printed'
            self.lbl_rd_printed.setText(f" | 🖨 {_printed_tag}")
            self.lbl_rd_printed.setToolTip(tr("Printed material (not handwritten manuscript)"))
            self.lbl_rd_printed.setVisible(True)
        else:
            self.lbl_rd_printed.setVisible(False)

        self.lbl_meta_loading.setVisible(False)
        self.lbl_title.setText('')
        self.lbl_img_label.setText("")

        # Clear field translation cache when navigating to a new manuscript
        from gui_threads import _field_translation_cache
        _field_translation_cache.clear()
        parent = self._app
        if parent:
            parent._trans_toggle_state = {}

        if self.ext_data and self.current_sys_id not in self.meta_mgr.nli_cache:
             self.ext_data = None
             self.ext_canvases = []
             self.btn_external_view.setVisible(False)
             self.external_pane.setVisible(False)

        cached_meta = self.meta_mgr.nli_cache.get(self.current_sys_id)
        if cached_meta:
            self.apply_metadata(cached_meta)
        else:
            self.lbl_meta_loading.setVisible(True)
            self.current_meta_request += 1
            request_id = self.current_meta_request
            def worker():
                meta = self.meta_mgr.fetch_nli_data(self.current_sys_id)
                self.metadata_loaded.emit(request_id, meta or {})
            threading.Thread(target=worker, daemon=True).start()

        if not cached_meta or 'marc' not in cached_meta:
            # Disconnect old enrich worker to prevent stale signals and GC crash
            if hasattr(self, 'enrich_worker') and self.enrich_worker is not None:
                try:
                    self.enrich_worker.finished_signal.disconnect(self.on_enriched_data_loaded)
                except (TypeError, RuntimeError):
                    pass
            self.enrich_worker = EnrichMetadataThread(self.meta_mgr, self.current_sys_id)
            self.enrich_worker.finished_signal.connect(self.on_enriched_data_loaded)
            self.enrich_worker.start()
        else:
            self.on_enriched_data_loaded(self.current_sys_id, cached_meta)

    def apply_metadata(self, meta):
        # 1. Update Text Labels
        shelf = self.meta_mgr.get_shelfmark_from_header(self.current_full_header) or meta.get('shelfmark', 'Unknown Shelf')
        # Add library name prefix
        library_code = self.meta_mgr.get_library_for_id(self.current_sys_id)
        if library_code:
            library = get_library_display(library_code, short=False)
            shelf = f"{library} | {shelf}"
        self.lbl_shelf.setText(shelf)
        if hasattr(self, 'lbl_compact_shelf'):
            self.lbl_compact_shelf.setText(shelf)
        _set_label_with_tooltip(self.lbl_title, _resolve_display_title(
            self.current_sys_id, meta.get('title', '')
        ))
        self.lbl_meta_loading.setVisible(False)

        # 2. Trigger Image Fetch using the FRESH metadata
        # (This meta object now contains 'thumb_url' from the XML 907 $d field)
        self.fetch_image(self.current_sys_id, meta)

    def toggle_extended_info(self, checked):
        self.extended_info_visible = checked
        self.txt_extended_info.setVisible(checked)
        label = f"ℹ️ {tr('Hide Info')}" if checked else f"ℹ️ {tr('Info')}"
        self.btn_ext_info.setText(label)
        if hasattr(self, 'btn_compact_ext_info'):
            self.btn_compact_ext_info.blockSignals(True)
            self.btn_compact_ext_info.setChecked(checked)
            self.btn_compact_ext_info.setText(label)
            self.btn_compact_ext_info.blockSignals(False)

    def _on_rd_ext_link_clicked(self, url):
        """Handle clicks on links in ResultDialog extended info."""
        url_str = url.toString()
        if url_str.startswith('tag:'):
            tag = url_str[4:]
            parent = self._app
            if parent and hasattr(parent, '_search_by_pgp_tag'):
                self.close()
                parent._search_by_pgp_tag(tag)
        elif url_str.startswith('toggle-trans:'):
            parent = self._app
            if parent:
                parts = url_str[len('toggle-trans:'):].split(':', 1)
                if len(parts) == 2:
                    field = parts[0]
                    if not hasattr(parent, '_trans_toggle_state'):
                        parent._trans_toggle_state = {}
                    parent._trans_toggle_state[field] = not parent._trans_toggle_state.get(field, False)
                    # Rebuild this dialog's extended info
                    self._rd_refresh_extended_info()
        elif url_str.startswith('translate-field:'):
            field_key = url_str[len('translate-field:'):]
            parent = self._app
            if parent:
                parent._start_field_translation(field_key, 'rd', self)
        elif url_str.startswith('toggle-always:'):
            action = url_str[len('toggle-always:'):]
            new_val = action == 'on'
            save_app_config({'show_translations': new_val})
            parent = self._app
            if parent:
                parent._trans_toggle_state = {}
            self._rd_refresh_extended_info()
        elif url_str.startswith('http'):
            QDesktopServices.openUrl(url)

    def _rd_toggle_translations(self, checked):
        """Toggle show_translations from ResultDialog toolbar button."""
        if getattr(self, '_rd_trans_syncing', False):
            return
        self._rd_trans_syncing = True
        try:
            save_app_config({'show_translations': checked})
            _rd_label = f"🌐 {tr('Trans ON')}" if checked else f"🌐 {tr('Trans OFF')}"
            _label = tr('Translations ON') if checked else tr('Translations OFF')
            self.btn_rd_translations.setChecked(checked)
            self.btn_rd_translations.setText(_rd_label)
            if hasattr(self, 'btn_compact_translations'):
                self.btn_compact_translations.setChecked(checked)
                self.btn_compact_translations.setText(_rd_label)
            # Sync Settings checkbox
            parent = self._app
            if parent:
                if hasattr(parent, 'chk_show_translations'):
                    parent.chk_show_translations.setChecked(checked)
                # Sync browse tab button if it exists
                if hasattr(parent, 'btn_b_translations'):
                    parent.btn_b_translations.setChecked(checked)
                    parent.btn_b_translations.setText(_label)
                # Sync search tab button
                if hasattr(parent, 'btn_search_translations'):
                    parent.btn_search_translations.setChecked(checked)
                    parent.btn_search_translations.setText(_label)
                parent._trans_toggle_state = {}
        finally:
            self._rd_trans_syncing = False
        self._rd_refresh_extended_info()
        # Refresh title (may add/remove English complement)
        self._rd_refresh_title()
        # Auto-translate all pending fields when toggled ON
        if checked:
            self._rd_auto_translate_all()

    def _rd_refresh_title(self):
        """Refresh the ResultDialog title label based on current translation toggle."""
        rd_meta = getattr(self, '_rd_enrichment_meta', None)
        if not rd_meta:
            return
        marc = rd_meta.get('marc', {})
        _display_title = _resolve_display_title(
            self.current_sys_id, rd_meta.get('title', '') or '',
            marc.get('english_title', '')
        )
        _set_label_with_tooltip(self.lbl_title, _display_title)

    def _rd_auto_translate_all(self):
        """Auto-fire translations for all translatable fields that aren't cached yet."""
        from gui_threads import _field_translation_cache
        rd_meta = getattr(self, '_rd_enrichment_meta', None)
        if not rd_meta:
            return
        parent = self._app
        if not parent:
            return
        marc = rd_meta.get('marc', {})
        part_meta = rd_meta.get('_part_meta') or {}

        # Collect all translatable field keys
        _ui_lang = CURRENT_LANG
        field_keys = []
        # Date — short Hebrew dates like "מאה ט״ו": try direct conversion first, Dicta fallback
        date_val = marc.get('date', '')
        if date_val and len(date_val.strip()) >= 3:
            is_he = _is_hebrew_text(date_val)
            if (is_he and _ui_lang != 'he') or (not is_he and _ui_lang != 'en'):
                if 'rd_date' not in _field_translation_cache and is_he:
                    _direct = _translate_hebrew_date(date_val)
                    if _direct:
                        _field_translation_cache['rd_date'] = _direct
                if 'rd_date' not in _field_translation_cache:
                    field_keys.append('rd_date')
        if marc.get('english_title') and len(marc['english_title'].strip()) >= 10:
            field_keys.append('rd_eng_title')
        phys = rd_meta.get('physical_desc', '')
        dims = marc.get('dimensions', '')
        phys_text = f"{phys or ''} {dims or ''}".strip()
        if phys_text and len(phys_text) >= 10:
            field_keys.append('rd_phys_desc')
        subjects = marc.get('subjects', [])
        if subjects:
            subjects_text = '; '.join(subjects)
            if len(subjects_text.strip()) >= 10:
                field_keys.append('rd_subjects')
        notes = marc.get('notes', [])
        if notes:
            combined = '\n'.join(notes)
            if len(combined.strip()) >= 10:
                field_keys.append('rd_notes')
        people = marc.get('people', [])
        if people:
            people_text = '; '.join(people)
            if len(people_text.strip()) >= 10:
                field_keys.append('rd_people')
        if part_meta.get('title') and len(part_meta['title'].strip()) >= 10:
            field_keys.append('rd_part_title')
        if part_meta.get('contents') and len(part_meta['contents'].strip()) >= 10:
            field_keys.append('rd_part_contents')
        for k, v in rd_meta.get('external_meta', {}).items():
            if v and len(str(v).strip()) >= 10:
                field_keys.append(f'rd_ext_{k}')

        # Fire translation for any field not already cached
        for fk in field_keys:
            if fk not in _field_translation_cache:
                parent._start_field_translation(fk, 'rd', self)

    def _show_rd_fjms_bib(self):
        """Open FJMS bibliography dialog from ResultDialog."""
        if not self._rd_fjms_bib:
            return
        shelf = self.meta_mgr.get_meta_for_id(self.current_sys_id)[0] if self.current_sys_id else ''
        from desktop.dialogs_scholarly import FjmsBibliographyDialog
        dlg = FjmsBibliographyDialog(
            self._rd_fjms_bib,
            sys_id=self.current_sys_id or '',
            shelfmark=shelf,
            parent=self,
        )
        dlg.exec()

    def _show_rd_nli_bib(self):
        """Open NLI bibliography dialog from ResultDialog."""
        if not self._rd_marc_bib:
            return
        shelf = self.meta_mgr.get_meta_for_id(self.current_sys_id)[0] if self.current_sys_id else ''
        from desktop.dialogs_scholarly import NliBibliographyDialog
        dlg = NliBibliographyDialog(
            self._rd_marc_bib,
            sys_id=self.current_sys_id or '',
            shelfmark=shelf,
            parent=self,
        )
        dlg.exec()

    def _show_rd_catalog(self):
        """Open FJMS catalog records dialog from reading desk (lazy fetch)."""
        # Lazy fetch: load catalog detail on first click if not yet loaded
        if self._rd_catalog_detail is None and self.current_sys_id:
            try:
                from shared.fjms_service import get_fjms_service
                fjms_svc = get_fjms_service()
                if fjms_svc.is_available():
                    self._rd_catalog_detail = fjms_svc.get_catalog_detail(self.current_sys_id)
            except Exception:
                pass  # Shelfmark lookup failed; use fallback identifier

        if not self._rd_catalog_detail:
            return
        shelf = self.meta_mgr.get_meta_for_id(self.current_sys_id)[0] if self.current_sys_id else ''
        from desktop.dialogs_scholarly import FjmsCatalogDialog
        dlg = FjmsCatalogDialog(
            self._rd_catalog_detail,
            sys_id=self.current_sys_id or '',
            shelfmark=shelf,
            parent=self,
        )
        dlg.exec()

    def _show_rd_measurements(self):
        """Open measurements dialog from reading desk (lazy fetch on first click)."""
        if self._rd_measurements_data is None and self.current_sys_id:
            try:
                from shared.fjms_service import get_fjms_service
                fjms_svc = get_fjms_service()
                if fjms_svc.is_available():
                    self._rd_measurements_data = fjms_svc.get_measurements(self.current_sys_id)
            except Exception:
                pass  # Measurement query failed; try next source

        if self._rd_measurements_data:
            shelf = self.meta_mgr.get_meta_for_id(self.current_sys_id)[0] if self.current_sys_id else ''
            _side = 'recto' if (self.current_p_num or 1) == 1 else 'verso'
            from desktop.dialogs_scholarly import FjmsMeasurementsDialog
            dlg = FjmsMeasurementsDialog(
                self._rd_measurements_data,
                sys_id=self.current_sys_id or '',
                shelfmark=shelf,
                parent=self,
                image_side=_side,
            )
            dlg.exec()

    def toggle_external_viewer(self, checked):
        self.external_pane.setVisible(checked)
        if checked:
            QTimer.singleShot(0, self.sync_external_view)

    def on_enriched_data_loaded(self, sid, meta):
        if not meta: return
        # Verify this data is for the currently displayed result to prevent race conditions
        if sid != self.current_sys_id:
            return
        if self.current_sys_id not in self.meta_mgr.nli_cache: return

        # 1. Update Image Label
        fl_digits = re.sub(r"\D", "", str(self.current_fl_id or ""))
        canvas_map = meta.get('canvas_map', {})
        label = canvas_map.get(fl_digits)
        self.lbl_img_label.setText(f"({label})" if label else "")

        # Check for Oxford Part metadata
        oxford_part_id = meta.get('oxford_part_id')
        part_meta = None
        if oxford_part_id:
            part_meta = self.meta_mgr.get_part_metadata(oxford_part_id)
        elif self.current_sys_id:
            # Check if this folio belongs to a Part
            part_id = self.meta_mgr.get_part_for_folio(self.current_sys_id)
            if part_id:
                oxford_part_id = part_id
                part_meta = self.meta_mgr.get_part_metadata(part_id)

        # 2. Populate External / Image Viewer
        has_images = bool(meta.get('images_nli') or meta.get('images_ext'))
        _prev_img_visible = self.btn_toggle_image.isChecked()

        self.btn_toggle_image.setVisible(has_images)

        if has_images:
            # Show viewer by default
            self.external_pane.setVisible(True)
            self.btn_toggle_image.setChecked(True)

            self.lbl_ext_attr.setVisible(False)
            self.txt_ext_meta.setHtml("")
            self.txt_ext_meta.setVisible(False)

            # Load images into widget
            shelfmark = meta.get('shelfmark') or self.meta_mgr.get_meta_for_id(self.current_sys_id)[0]
            folio_num = _get_folio_number_from_shelfmark(shelfmark)
            side_offset = 1 if (self.current_internal_idx or 0) % 2 == 1 else 0
            initial_idx = _get_folio_image_index(
                meta,
                folio_num if folio_num is not None else self.current_p_num,
                side_offset=side_offset
            )
            self.ms_viewer.load_images(meta, initial_idx, target_folio=folio_num)

            # Preserve user's image toggle preference across navigation
            if not _prev_img_visible:
                self.btn_toggle_image.setChecked(False)
                self.external_pane.setVisible(False)
        else:
            self.external_pane.setVisible(False)
            self.btn_toggle_image.setChecked(False)

        self.external_url = meta.get('external_url') or meta.get('marc', {}).get('external_iiif_link')
        # Prefer library_viewer_url (catalog page) over manifest URL for JTS/Manchester
        lib_viewer = meta.get('library_viewer_url')
        if lib_viewer and lib_viewer.get('url'):
            provider_check = meta.get('external_provider', '')
            if provider_check in ('manchester', 'jts'):
                self.external_url = lib_viewer['url']
        if self.external_url:
            provider = meta.get('external_provider', '')
            if oxford_part_id or provider == 'oxford':
                btn_label = tr("Oxford")
            elif provider == 'cambridge' or "cudl.lib.cam.ac.uk" in (self.external_url or "").lower():
                btn_label = tr("Cambridge")
            elif provider == 'manchester':
                btn_label = "Manchester LUNA"
            elif provider == 'jts':
                btn_label = "Princeton Digital Library"
            else:
                btn_label = tr("External Website")
            self.btn_external_link.setText(btn_label)
            self.btn_external_link.setVisible(True)
        else:
            self.btn_external_link.setVisible(False)

        # 3. Populate bibliography buttons (before early-return guard)
        marc = meta.get('marc', {})
        fjms_bib = meta.get('bibliography', [])
        marc_bib = marc.get('bibliography', [])
        if fjms_bib:
            self._rd_fjms_bib = fjms_bib
            lbl = f"📚 {tr('Bib FJMS')} ({len(fjms_bib)})"
            self.btn_rd_bib_fjms.setText(lbl)
            self.btn_rd_bib_fjms.setToolTip(tr("Bibliography FJMS"))
            self.btn_rd_bib_fjms.setVisible(True)
            if hasattr(self, 'btn_compact_bib_fjms'):
                self.btn_compact_bib_fjms.setText(lbl)
                self.btn_compact_bib_fjms.setToolTip(tr("Bibliography FJMS"))
                self.btn_compact_bib_fjms.setVisible(True)
        if marc_bib:
            self._rd_marc_bib = marc_bib
            lbl = f"📚 {tr('Bib Ktiv')} ({len(marc_bib)})"
            self.btn_rd_bib_nli.setText(lbl)
            self.btn_rd_bib_nli.setToolTip(tr("Bibliography Ktiv"))
            self.btn_rd_bib_nli.setVisible(True)
            if hasattr(self, 'btn_compact_bib_nli'):
                self.btn_compact_bib_nli.setText(lbl)
                self.btn_compact_bib_nli.setToolTip(tr("Bibliography Ktiv"))
                self.btn_compact_bib_nli.setVisible(True)

        # Catalog Records button
        # Detail is fetched lazily on button click, not during page load
        self._rd_catalog_detail = None
        try:
            from shared.fjms_service import get_fjms_service
            fjms_svc = get_fjms_service()
            if fjms_svc.is_available():
                source_names = fjms_svc.get_source_names(self.current_sys_id)
                catalog_count = len(source_names)
                self.btn_rd_catalog.setText(f"📋 {tr('Catalog')} ({catalog_count})")
                self.btn_rd_catalog.setEnabled(catalog_count > 0)
                self.btn_rd_catalog.setVisible(True)
                if hasattr(self, 'btn_compact_catalog'):
                    self.btn_compact_catalog.setText(f"📋 {tr('Catalog')} ({catalog_count})")
                    self.btn_compact_catalog.setToolTip(tr("Catalog Records"))
                    self.btn_compact_catalog.setEnabled(catalog_count > 0)
                    self.btn_compact_catalog.setVisible(True)
        except Exception:
            self.btn_rd_catalog.setVisible(False)  # Feature check failed; hide button
            if hasattr(self, 'btn_compact_catalog'):
                self.btn_compact_catalog.setVisible(False)

        # Measurements button (lazy check via has_measurements)
        self._rd_measurements_data = None
        try:
            from shared.fjms_service import get_fjms_service
            fjms_svc_m = get_fjms_service()
            has_m = fjms_svc_m.is_available() and fjms_svc_m.has_measurements(self.current_sys_id)
            if has_m:
                self.btn_rd_measurements.setVisible(True)
                self.btn_rd_measurements.setEnabled(True)
                self.btn_rd_measurements.setText(f"\U0001f4cf {tr('Measurements')}")
                if hasattr(self, 'btn_compact_measurements'):
                    self.btn_compact_measurements.setVisible(True)
                    self.btn_compact_measurements.setText(f"\U0001f4cf {tr('Meas.')}")
            else:
                self.btn_rd_measurements.setVisible(False)
                if hasattr(self, 'btn_compact_measurements'):
                    self.btn_compact_measurements.setVisible(False)
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.btn_rd_measurements.setVisible(False)
            if hasattr(self, 'btn_compact_measurements'):
                self.btn_compact_measurements.setVisible(False)

        # 3b. Visual Similarity button (D-10: ResultDialog context)
        _parent = self._app
        _vs_has_rd = bool(_parent and hasattr(_parent, 'meta_mgr') and _parent.meta_mgr
                          and _parent.meta_mgr.csv_bank.get(sid, {}).get('has_vs'))
        self.btn_rd_visual_sim.setVisible(_vs_has_rd)

        # 4. Build Extended Info HTML (Text)
        # Store enrichment meta for translate badge rebuild
        self._rd_enrichment_meta = {**meta, '_part_id': oxford_part_id, '_part_meta': part_meta}

        html = self._rd_build_extended_html(meta, oxford_part_id, part_meta)
        if html is None:
            self.btn_ext_info.setVisible(False)
            if hasattr(self, 'btn_compact_ext_info'):
                self.btn_compact_ext_info.setVisible(False)
            return
        self.txt_extended_info.setHtml(html)
        self.btn_ext_info.setVisible(True)
        if hasattr(self, 'btn_compact_ext_info'):
            self.btn_compact_ext_info.setVisible(True)
        # Store flag so PGP late-arrival handler knows enriched data was processed
        self._rd_enriched_data_loaded = True

        # Title display: use libraries_translations.db for clean split title
        _display_title = _resolve_display_title(
            self.current_sys_id, meta.get('title', ''), marc.get('english_title', '')
        )
        _set_label_with_tooltip(self.lbl_title, _display_title)
        shelf = meta.get('shelfmark')
        if shelf and shelf != "Unknown":
            # Try CSV library_code first, then MARC as fallback
            library_code = self.meta_mgr.get_library_for_id(self.current_sys_id)
            if library_code:
                library = get_library_display(library_code, short=False)
            else:
                library = marc.get('current_owner', '')
            if library:
                shelf = f"{library} | {shelf}"
            # Add Part info to shelfmark if available
            if oxford_part_id:
                part_label = self.meta_mgr.codico_mgr.get_part_label(oxford_part_id)
                if part_label:
                    shelf = f"{shelf} [{part_label}]"
            self.lbl_shelf.setText(shelf)
            if hasattr(self, 'lbl_compact_shelf'):
                self.lbl_compact_shelf.setText(shelf)

        thumb_url = meta.get('thumb_url')
        if thumb_url and thumb_url != getattr(self, 'current_thumb_url', None):
            self.fetch_image(self.current_sys_id, meta)

        # Auto-translate fields on initial load if translations are ON
        if load_app_config().get('show_translations', False):
            self._rd_auto_translate_all()

    def sync_external_view(self):
        meta = self.meta_mgr.nli_cache.get(self.current_sys_id, {})
        if not meta:
            return
        shelfmark = meta.get('shelfmark') or self.meta_mgr.get_meta_for_id(self.current_sys_id)[0]
        folio_num = _get_folio_number_from_shelfmark(shelfmark)
        side_offset = 1 if (self.current_internal_idx or 0) % 2 == 1 else 0

        # Use viewer's images (may include dynamic images added by load_images)
        viewer_images = getattr(self.ms_viewer, 'images_ext', None)
        if viewer_images:
            idx = _get_folio_image_index(
                {'images_ext': viewer_images},
                folio_num if folio_num is not None else self.current_p_num,
                side_offset=side_offset
            )
            self.ms_viewer.set_page(idx)

    def on_metadata_loaded(self, request_id, meta):
        if request_id != self.current_meta_request:
            return
        self.apply_metadata(meta or {})

    @staticmethod
    def _wait_or_terminate_thread(thread, timeout_ms=2000):
        """Wait for a QThread to finish; terminate as last resort."""
        thread.cancel()
        if not thread.wait(timeout_ms):
            logger.warning("Image thread did not finish in %dms, terminating", timeout_ms)
            thread.terminate()
            thread.wait()

    def cancel_image_thread(self):
        img_thread = getattr(self, 'img_thread', None)
        if img_thread and img_thread.isRunning():
            self._wait_or_terminate_thread(img_thread)

        if getattr(self, 'ext_img_thread', None) and self.ext_img_thread.isRunning():
            self._wait_or_terminate_thread(self.ext_img_thread)

    def fetch_image(self, sys_id, meta=None):
        self.cancel_image_thread()
        self.lbl_thumb.setText(tr("Loading..."))
        self.lbl_thumb.setPixmap(QPixmap())

        # Ensure we look at the global cache which acts as the "Source of Truth"
        if not meta:
            meta = self.meta_mgr.nli_cache.get(sys_id)

        # Retrieve the URL that MetadataManager logic (XML 907 $d) has determined
        thumb_url = meta.get('thumb_url') if meta else None

        if thumb_url:
            self.start_download(sys_id, thumb_url)
        else:
            # If meta exists but no thumb_url, it means no representative image found
            if meta:
                self.lbl_thumb.setText(tr("No Preview"))
            else:
                self.lbl_thumb.setText(tr("Waiting..."))

        def worker(target_sid=sys_id):
            url = self.meta_mgr.get_thumbnail(target_sid)
            self.thumb_resolved.emit(target_sid, url)

        threading.Thread(target=worker, daemon=True).start()

    def _on_thumb_resolved(self, sid, thumb_url):
        if sid != self.current_sys_id:
            return
        if thumb_url:
            self.start_download(sid, thumb_url)
        else:
            self.on_img_failed()

    def start_download(self, sid, thumb_url):
        if sid != self.current_sys_id:
            return

        self.current_thumb_url = thumb_url
        self.cancel_image_thread()

        if not thumb_url:
            self.on_img_failed()
            return

        self.img_thread = ImageLoaderThread(thumb_url)
        self.img_thread.image_loaded.connect(self.on_img_loaded)
        self.img_thread.load_failed.connect(self.on_img_failed)
        self.img_thread.start()

    def on_img_loaded(self, image):
        pix = QPixmap.fromImage(image)
        scaled = pix.scaled(self.lbl_thumb.size(), Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
        self.lbl_thumb.setPixmap(scaled)
        self.lbl_thumb.setText("")

    def on_img_failed(self):
        self.lbl_thumb.setPixmap(QPixmap())
        self.lbl_thumb.setText(tr("No Preview"))

    def closeEvent(self, event):
        try:
            if hasattr(self, 'meta_mgr'):
                self.meta_mgr.save_caches()
                logger.info("Metadata caches flushed to disk on exit.")
        except Exception as e:
            logger.error("Failed to save metadata caches on exit: %s", e)

        # 2. Stop ResultDialog-owned worker threads
        try:
            for attr in ('enrich_worker', '_rd_pgp_worker', 'preload_meta_worker'):
                worker = getattr(self, attr, None)
                if worker and worker.isRunning():
                    worker.requestInterruption()
                    if not worker.wait(2000):
                        worker.terminate()
                        worker.wait()

            # Stop dialog's own thumbnail image loaders (img_thread, ext_img_thread)
            self.cancel_image_thread()

            # Stop manuscript viewer image threads
            if getattr(self, 'ms_viewer', None):
                self.ms_viewer.stop_threads()

            # Phase 100 (REVIEWS HIGH-2 + R2-2 + R2-3): fully discard this dialog's transient
            # render scope so a late worker result cannot write into the closed dialog's ms_viewer,
            # the retained callbacks (closing over this dialog + viewer) are released, AND the
            # scope's debounce/watchdog QTimer dict entries are removed (not just stopped — they
            # would otherwise accumulate one pair per opened PDF dialog for the app session).
            # Idempotent with the finished-signal handler (_on_pdf_dialog_finished).
            try:
                ctrl = self._pdf_controller()
                if ctrl is not None:
                    ctrl.discard_scope(self._pdf_scope)
            except Exception:  # noqa: BLE001
                pass

        finally:
            super().closeEvent(event)

    # ------------------------------------------------------------------
    # Phase 100 (PDFIMG-03/05): LOCAL PDF image rendering helpers
    # ------------------------------------------------------------------

    def _pdf_controller(self):
        """Return the shared PdfImageController from the parent app, or None."""
        app = getattr(self, '_app', None)
        return getattr(app, '_pdf_image_controller', None) if app else None

    def _is_current_hit_local(self) -> bool:
        """Return True iff the current hit is a LOCAL indexed result."""
        try:
            from shared.local_sys_id import is_local_sys_id as _is_local
            return bool(
                self.current_sys_id
                and _is_local(self.current_sys_id)
                and getattr(self, '_app', None)
            )
        except Exception:  # noqa: BLE001
            return False

    def _render_local_pdf_image(self):
        """Phase 100 (PDFIMG-03/05): if the current hit is a LOCAL PDF, reveal the
        external pane and request a render of the current page.

        Called from load_local_page's SUCCESS path only (single source of truth
        for the shown LOCAL page — REVIEWS HIGH-3 de-dup). Does NOT cancel;
        non-PDF/non-LOCAL cancellation is handled by load_result_by_index +
        dialog teardown.
        """
        controller = self._pdf_controller()
        fp = getattr(self, '_rd_local_filepath', None)
        if controller is None or not self._is_current_hit_local() or not controller.is_pdf(fp):
            return  # non-LOCAL or non-PDF LOCAL: nothing to render (cancel handled elsewhere)
        sys_id = self.current_sys_id
        page_num = self.current_p_num or 1
        # Reveal the pane (D-08) and keep the toggle usable for PDFs.
        self.external_pane.setVisible(True)
        self.btn_toggle_image.setVisible(True)
        self.btn_toggle_image.setChecked(True)
        # Phase 100 UAT: a LOCAL PDF has no external metadata, so collapse the
        # empty meta box (it otherwise leaves a blank gap between the header and
        # the rendered page). Mirrors the Genizah image path which hides it too.
        # Header stays visible (now dark-mode aware) to label the pane.
        self.lbl_ext_attr.setVisible(True)
        self.txt_ext_meta.setHtml("")
        self.txt_ext_meta.setVisible(False)
        controller.request(
            self._pdf_scope,
            sys_id,
            page_num,
            fp,
            on_image=lambda img: self.ms_viewer.display_image(img),
            on_placeholder=lambda text: self.ms_viewer.scroll_area.set_status_message(text),
        )

    def _cancel_local_pdf_image(self):
        """Phase 100 (REVIEWS HIGH-2): invalidate any in-flight render for THIS dialog
        so a late success cannot write a stale image into ms_viewer, and hide the pane.

        Uses cancel (not discard_scope) because the dialog scope is still live and
        may be re-requested when the user navigates back to a LOCAL PDF result.
        """
        controller = self._pdf_controller()
        if controller is not None:
            controller.cancel(self._pdf_scope, silent=True)
        try:
            self.external_pane.setVisible(False)
            self.btn_toggle_image.setVisible(False)
        except Exception:  # noqa: BLE001
            pass

    def _on_pdf_dialog_finished(self, _result):
        """Phase 100 (REVIEWS-R2-2/R2-3): on any dialog termination (accept/reject/done/Esc),
        fully discard this dialog's transient render scope so a late worker result cannot
        write into the closed dialog's ms_viewer AND the scope's debounce/watchdog QTimer
        dict entries are removed (not just stopped). Idempotent with the closeEvent discard.
        """
        controller = self._pdf_controller()
        if controller is not None:
            try:
                controller.discard_scope(self._pdf_scope)
            except Exception:  # noqa: BLE001
                pass

    def open_catalog(self):
        # Phase 85 D-06: synthetic sys_ids skip the NLI catalog page (no Alma record)
        if self.current_sys_id and not is_synthetic_sys_id(self.current_sys_id):
            QDesktopServices.openUrl(QUrl(f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{self.current_sys_id}"))

    def open_viewer(self):
        # Phase 85 D-06: synthetic sys_ids skip the NLI viewer (no Alma record)
        if self.current_sys_id and not is_synthetic_sys_id(self.current_sys_id):
            # Use docid query param (not hash fragment) — hash-based URLs fail on direct navigation
            docid = f"PNX_MANUSCRIPTS{self.current_sys_id}-1"
            if self.current_fl_id:
                docid += f",FL{self.current_fl_id}"
            QDesktopServices.openUrl(QUrl(f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/viewerpage?vid=MANUSCRIPTS&docid={docid}"))

