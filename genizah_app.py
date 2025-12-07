# genizah_app.py
import sys
import os
import re
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QLabel, QLineEdit, QPushButton, QTabWidget, QTableWidget, 
                             QTableWidgetItem, QHeaderView, QComboBox, QCheckBox, 
                             QTextEdit, QMessageBox, QProgressBar, QSplitter, QDialog, 
                             QTextBrowser, QFileDialog, QMenu, QGroupBox, QSpinBox, 
                             QTreeWidget, QTreeWidgetItem, QPlainTextEdit)
from PyQt6.QtCore import Qt, QTimer, QUrl, QSize
from PyQt6.QtGui import QFont, QIcon, QDesktopServices, QGuiApplication, QAction

from genizah_core import Config, MetadataManager, VariantManager, SearchEngine, Indexer, AIManager
from gui_threads import SearchThread, IndexerThread, ShelfmarkLoaderThread, CompositionThread, AIWorkerThread

# ==============================================================================
#  HELP DIALOG (NEW)
# ==============================================================================
class HelpDialog(QDialog):
    def __init__(self, parent, title, content):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setWindowIcon(QIcon(os.path.join(Config.BASE_DIR, "icon.ico")))
        self.resize(500, 400)
        layout = QVBoxLayout()
        text = QTextBrowser()
        text.setHtml(content)
        text.setOpenExternalLinks(True)
        layout.addWidget(text)
        btn = QPushButton("Close")
        btn.clicked.connect(self.close)
        layout.addWidget(btn)
        self.setLayout(layout)

# ==============================================================================
#  AI DIALOG
# ==============================================================================
class AIDialog(QDialog):
    def __init__(self, parent, ai_mgr):
        super().__init__(parent)
        self.setWindowTitle("AI Regex Assistant (Gemini)")
        self.resize(600, 500)
        self.ai_mgr = ai_mgr
        self.generated_regex = ""
        
        layout = QVBoxLayout()
        self.chat_display = QTextBrowser()
        self.chat_display.setOpenExternalLinks(True)
        layout.addWidget(self.chat_display)
        
        input_layout = QHBoxLayout()
        self.prompt_input = QLineEdit()
        self.prompt_input.setPlaceholderText("Describe pattern (e.g. 'Word starting with Aleph')...")
        self.prompt_input.returnPressed.connect(self.send_request)
        self.btn_send = QPushButton("Send")
        self.btn_send.clicked.connect(self.send_request)
        input_layout.addWidget(self.prompt_input)
        input_layout.addWidget(self.btn_send)
        layout.addLayout(input_layout)
        
        self.lbl_preview = QLabel("Generated Regex will appear here.")
        self.lbl_preview.setStyleSheet("font-weight: bold; color: #2980b9; padding: 10px; background: #ecf0f1;")
        self.lbl_preview.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        layout.addWidget(self.lbl_preview)
        
        self.btn_use = QPushButton("Use this Regex")
        self.btn_use.clicked.connect(self.accept)
        self.btn_use.setEnabled(False)
        self.btn_use.setStyleSheet("background-color: #27ae60; color: white; font-weight: bold;")
        layout.addWidget(self.btn_use)
        self.setLayout(layout)
        self.append_chat("System", "Hello! I can help you build Regex for Hebrew manuscripts.")

    def append_chat(self, sender, text):
        color = "blue" if sender == "System" else "green" if sender == "You" else "black"
        self.chat_display.append(f"<b style='color:{color}'>{sender}:</b> {text}<br>")

    def send_request(self):
        text = self.prompt_input.text().strip()
        if not text: return
        self.append_chat("You", text)
        self.prompt_input.clear(); self.prompt_input.setEnabled(False); self.btn_send.setEnabled(False)
        self.lbl_preview.setText("Thinking...")
        self.worker = AIWorkerThread(self.ai_mgr, text)
        self.worker.finished_signal.connect(self.on_response)
        self.worker.start()

    def on_response(self, data, err):
        self.prompt_input.setEnabled(True); self.btn_send.setEnabled(True); self.prompt_input.setFocus()
        if err:
            self.append_chat("Error", err); self.lbl_preview.setText("Error.")
            return
        regex = data.get("regex", "")
        self.append_chat("Gemini", f"{data.get('explanation', '')}<br><code>{regex}</code>")
        self.lbl_preview.setText(regex)
        self.generated_regex = regex
        self.btn_use.setEnabled(True)

# ==============================================================================
#  RESULT DIALOG
# ==============================================================================
class ResultDialog(QDialog):
    # Updated Init Signature
    def __init__(self, parent, all_results, current_index, meta_mgr, searcher):
        super().__init__(parent)
        
        self.all_results = all_results
        self.current_result_idx = current_index
        self.meta_mgr = meta_mgr
        self.searcher = searcher
        
        # State for internal browsing
        self.current_sys_id = None
        self.current_p_num = None
        self.current_fl_id = None
        
        self.init_ui()
        self.load_result_by_index(self.current_result_idx)

    def init_ui(self):
        self.setWindowTitle(f"Manuscript Viewer")
        self.resize(1200, 850)
        
        main_layout = QVBoxLayout()
        
        # --- Top Bar: Result Navigation ---
        top_bar = QHBoxLayout()
        self.btn_res_prev = QPushButton("◀ Previous Result")
        self.btn_res_prev.clicked.connect(lambda: self.navigate_results(-1))
        
        self.lbl_res_count = QLabel()
        self.lbl_res_count.setStyleSheet("font-weight: bold; color: #555;")
        self.lbl_res_count.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self.btn_res_next = QPushButton("Next Result ▶")
        self.btn_res_next.clicked.connect(lambda: self.navigate_results(1))
        
        top_bar.addWidget(self.btn_res_prev)
        top_bar.addWidget(self.lbl_res_count, 1) # Expand middle
        top_bar.addWidget(self.btn_res_next)
        
        main_layout.addLayout(top_bar)
        
        # --- Separator ---
        line = QSplitter(); line.setFrameShape(QSplitter.Shape.HLine); main_layout.addWidget(line)
        
        # --- Header Info (Shelf/Title) ---
        header_layout = QHBoxLayout()
        title_box = QWidget()
        tb_layout = QVBoxLayout()
        tb_layout.setContentsMargins(0,0,0,0)
        
        self.lbl_shelf = QLabel()
        self.lbl_shelf.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        self.lbl_shelf.setStyleSheet("color: #2c3e50;")
        self.lbl_shelf.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        
        self.lbl_title = QLabel()
        self.lbl_title.setFont(QFont("Arial", 12))
        self.lbl_title.setWordWrap(True)
        self.lbl_title.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        
        tb_layout.addWidget(self.lbl_shelf)
        tb_layout.addWidget(self.lbl_title)
        title_box.setLayout(tb_layout)
        header_layout.addWidget(title_box, 2)
        
        self.lbl_info = QLabel()
        self.lbl_info.setTextFormat(Qt.TextFormat.RichText)
        self.lbl_info.setStyleSheet("background-color: #ecf0f1; color: #2c3e50; border-radius: 6px; padding: 8px;")
        self.lbl_info.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        header_layout.addWidget(self.lbl_info, 1)
        
        # --- Page Controls ---
        ctrl_layout = QVBoxLayout()
        nav_layout = QHBoxLayout()
        
        btn_pg_prev = QPushButton("Pg <"); btn_pg_prev.setFixedWidth(40)
        btn_pg_prev.clicked.connect(lambda: self.load_page(offset=-1))
        
        self.spin_page = QSpinBox(); self.spin_page.setPrefix("Img: "); self.spin_page.setRange(1, 9999); self.spin_page.setFixedWidth(90)
        self.spin_page.editingFinished.connect(lambda: self.load_page(target=self.spin_page.value()))
        
        btn_pg_next = QPushButton("> Pg"); btn_pg_next.setFixedWidth(40)
        btn_pg_next.clicked.connect(lambda: self.load_page(offset=1))
        
        self.lbl_total = QLabel("/ ?")
        
        nav_layout.addWidget(btn_pg_prev); nav_layout.addWidget(self.spin_page); nav_layout.addWidget(self.lbl_total); nav_layout.addWidget(btn_pg_next)
        ctrl_layout.addLayout(nav_layout)
        
        self.btn_cat = QPushButton("📄 Catalog"); self.btn_cat.clicked.connect(self.open_catalog)
        self.btn_img = QPushButton("🖼️ Ktiv Viewer"); self.btn_img.clicked.connect(self.open_viewer)
        ctrl_layout.addWidget(self.btn_cat); ctrl_layout.addWidget(self.btn_img)
        header_layout.addLayout(ctrl_layout, 1)
        
        main_layout.addLayout(header_layout)
        
        # --- Text Area ---
        self.text_browser = QTextBrowser()
        self.text_browser.setReadOnly(True)
        self.text_browser.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self.text_browser.setFont(QFont("SBL Hebrew", 16))
        main_layout.addWidget(self.text_browser)
        
        # Footer
        btn_close = QPushButton("Close")
        btn_close.clicked.connect(self.close)
        main_layout.addWidget(btn_close)
        
        self.setLayout(main_layout)

    def navigate_results(self, direction):
        new_idx = self.current_result_idx + direction
        if 0 <= new_idx < len(self.all_results):
            self.current_result_idx = new_idx
            self.load_result_by_index(new_idx)

    def load_result_by_index(self, idx):
        data = self.all_results[idx]
        if not data.get('full_text'):
            data['full_text'] = self.searcher.get_full_text_by_id(data['uid']) or data.get('text', '')
        self.data = data # Current data object
        
        # Update Result Navigation UI
        self.lbl_res_count.setText(f"Result {idx + 1} of {len(self.all_results)}")
        self.btn_res_prev.setEnabled(idx > 0)
        self.btn_res_next.setEnabled(idx < len(self.all_results) - 1)
        
        # Parse IDs
        ids = self.meta_mgr.parse_full_id_components(data['raw_header'])
        self.current_sys_id = ids['sys_id']
        try: p = int(ids['p_num']) 
        except: p = 1
        
        # Store context
        self.initial_context = data.get('source_ctx', '')
        # Fallback text (snippet or full)
        self.initial_text = data.get('full_text', '') or data.get('text', '')
        
        # Load Page Content (Logic reused from previous version)
        self.load_page(target=p)
        
        # Inject Context/Highlighting specific to this result
        if 'source_ctx' in data and data['source_ctx']:
             def htmlify_stars(text): return re.sub(r'\*(.*?)\*', r"<b style='color:red;'>\1</b>", text)
             ctx = htmlify_stars(data['source_ctx'].replace("\n", "<br>"))
             full_html = f"<div style='background-color:#e8f8f5; color:black; padding:10px; border-bottom:2px solid green;'><b>Match Context:</b><br>{ctx}</div><br><hr><br>"
             
             raw = self.initial_text.replace("\n", "<br>")
             if "*" in raw: raw = htmlify_stars(raw)
             full_html += raw
             self.text_browser.setHtml(f"<div dir='rtl'>{full_html}</div>")

    def load_page(self, offset=0, target=None):
        if not self.current_sys_id: return
        if target is not None:
            p = target
            page_data = self.searcher.get_browse_page(self.current_sys_id, p_num=p, next_prev=0)
        else:
            page_data = self.searcher.get_browse_page(self.current_sys_id, p_num=self.current_p_num, next_prev=offset)
        if not page_data: return

        self.current_p_num = page_data['p_num']
        parsed_new = self.meta_mgr.parse_full_id_components(page_data['full_header'])
        self.current_fl_id = parsed_new['fl_id']
        
        info_html = f"<b>System ID:</b> {self.current_sys_id}<br><b>File ID (FL):</b> {self.current_fl_id or 'N/A'}"
        self.lbl_info.setText(info_html)
        
        self.spin_page.blockSignals(True); self.spin_page.setValue(self.current_p_num); self.spin_page.blockSignals(False)
        self.lbl_total.setText(f"/ {page_data['total_pages']}")
        
        html = page_data['text'].replace("\n", "<br>")
        self.text_browser.setHtml(f"<div dir='rtl'>{html}</div>")
        
        meta = self.meta_mgr.fetch_nli_data(self.current_sys_id)
        self.lbl_shelf.setText(meta.get('shelfmark', 'Unknown Shelf'))
        self.lbl_title.setText(meta.get('title', ''))

    def open_catalog(self):
        if self.current_sys_id: QDesktopServices.openUrl(QUrl(f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{self.current_sys_id}"))

    def open_viewer(self):
        if self.current_sys_id and self.current_fl_id: QDesktopServices.openUrl(QUrl(f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/viewerpage?vid=MANUSCRIPT&docId=PNX_MANUSCRIPTS{self.current_sys_id}#d=[[PNX_MANUSCRIPTS{self.current_sys_id}-1,FL{self.current_fl_id}]]"))

# ==============================================================================
#  MAIN WINDOW
# ==============================================================================
class GenizahGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Genizah Search Pro V2.0")
        self.resize(1300, 850)
        
        # שלב 1: הצג חלון "ריק" עם הודעת טעינה
        lbl_loading = QLabel("Loading components... Please wait.", self)
        lbl_loading.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.setCentralWidget(lbl_loading)
        
        # שלב 2: תזמון טעינת המנוע לעוד 100ms (אחרי שהחלון עולה)
        QTimer.singleShot(100, self.delayed_init)

    def delayed_init(self):
        try:
            # כאן מתבצעת הטעינה הכבדה
            self.meta_mgr = MetadataManager()
            self.var_mgr = VariantManager()
            self.searcher = SearchEngine(self.meta_mgr, self.var_mgr)
            self.indexer = Indexer(self.meta_mgr)
            self.ai_mgr = AIManager()
            
            # אתחול הממשק המלא
            self.last_results = []
            self.comp_main = []
            self.comp_appendix = {}
            self.comp_summary = {}
            self.is_searching = False
            self.is_comp_running = False
            self.current_browse_sid = None
            self.current_browse_p = None
            
            self.init_ui() # בונה את self.tabs
            
            # בדיקת אינדקס והתראה
            # אנחנו בודקים אם התיקייה קיימת
            db_path = os.path.join(Config.INDEX_DIR, "tantivy_db")
            index_exists = os.path.exists(db_path) and os.listdir(db_path)
            
            if not index_exists:
                msg = "Index not found.\nWould you like to build it now?\n(Requires 'Transcriptions.txt' next to this app)"
                reply = QMessageBox.question(self, "Index Missing", msg, 
                                             QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
                if reply == QMessageBox.StandardButton.Yes:
                    self.tabs.setCurrentIndex(3) # מעבר לטאב הגדרות (כעת עובד כי השתמשנו ב-self.tabs)
                    self.run_indexing()
                
        except Exception as e:
            QMessageBox.critical(self, "Fatal Error", f"Failed to initialize:\n{e}")
            
    def init_ui(self):
        self.tabs = QTabWidget()
        self.tabs.addTab(self.create_search_tab(), "Search")
        self.tabs.addTab(self.create_composition_tab(), "Composition Search")
        self.tabs.addTab(self.create_browse_tab(), "Browse Manuscript")
        self.tabs.addTab(self.create_settings_tab(), "Settings & About")
        self.setCentralWidget(self.tabs)

    def create_search_tab(self):
        panel = QWidget(); layout = QVBoxLayout()
        top = QHBoxLayout()
        self.query_input = QLineEdit(); self.query_input.setPlaceholderText("Search terms...")
        self.query_input.returnPressed.connect(self.toggle_search)
        
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["Exact", "Variants (?)", "Extended (??)", "Maximum (???)", "Fuzzy (~)", "Regex"])
        # Tooltips
        self.mode_combo.setItemData(0, "Exact match")
        self.mode_combo.setItemData(1, "Basic variants: ד/ר, ה/ח, ו/י/ן etc.")
        self.mode_combo.setItemData(2, "Extended variants: Adds phonetical swaps (א/ע, ק/כ)")
        self.mode_combo.setItemData(3, "Maximum variants: Very broad search")
        self.mode_combo.setItemData(4, "Fuzzy search: Levenshtein distance")
        self.mode_combo.setItemData(5, "Regex: Use AI Assistant for complex patterns")
        
        self.gap_input = QLineEdit(); self.gap_input.setPlaceholderText("Gap"); self.gap_input.setFixedWidth(50)
        self.gap_input.setToolTip("Maximum word distance (0 = Exact phrase)")
        
        self.btn_search = QPushButton("Search"); self.btn_search.clicked.connect(self.toggle_search)
        self.btn_search.setStyleSheet("background-color: #27ae60; color: white; font-weight: bold; min-width: 80px;")
        
        btn_ai = QPushButton("🤖 AI Assistant"); btn_ai.setStyleSheet("background-color: #8e44ad; color: white;")
        btn_ai.setToolTip("Generate Regex with Gemini AI")
        btn_ai.clicked.connect(self.open_ai)

        # Help Button
        btn_help = QPushButton("?")
        btn_help.setFixedWidth(30)
        btn_help.setStyleSheet("background-color: #f39c12; color: white; font-weight: bold; border-radius: 15px;")
        btn_help.clicked.connect(lambda: HelpDialog(self, "Search Help", self.get_search_help_text()).exec())

        top.addWidget(QLabel("Query:")); top.addWidget(self.query_input, 2)
        top.addWidget(QLabel("Mode:")); top.addWidget(self.mode_combo)
        top.addWidget(QLabel("Gap:")); top.addWidget(self.gap_input)
        top.addWidget(self.btn_search); top.addWidget(btn_ai); top.addWidget(btn_help)
        layout.addLayout(top)
        
        self.search_progress = QProgressBar(); self.search_progress.setVisible(False)
        layout.addWidget(self.search_progress)
        
        self.results_table = QTableWidget(); self.results_table.setColumnCount(6)
        self.results_table.setHorizontalHeaderLabels(["System ID", "Shelfmark", "Title", "Snippet", "Img", "Src"])
        self.results_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self.results_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.results_table.doubleClicked.connect(self.show_full_text)
        layout.addWidget(self.results_table)
        
        bot = QHBoxLayout()
        self.status_label = QLabel("Ready.")
        self.btn_export = QPushButton("Export Results"); self.btn_export.clicked.connect(self.export_results); self.btn_export.setEnabled(False)
        bot.addWidget(self.status_label, 1); bot.addWidget(self.btn_export)
        layout.addLayout(bot)
        panel.setLayout(layout)
        return panel

    def create_composition_tab(self):
        panel = QWidget(); layout = QVBoxLayout(); splitter = QSplitter(Qt.Orientation.Vertical)
        
        inp_w = QWidget(); in_l = QVBoxLayout()
        tr = QHBoxLayout()
        self.comp_title_input = QLineEdit(); self.comp_title_input.setPlaceholderText("Composition Title")
        tr.addWidget(QLabel("Title:")); tr.addWidget(self.comp_title_input)
        
        # Help Button
        btn_help = QPushButton("?")
        btn_help.setFixedWidth(30)
        btn_help.setStyleSheet("background-color: #f39c12; color: white; font-weight: bold; border-radius: 15px;")
        btn_help.clicked.connect(lambda: HelpDialog(self, "Composition Help", self.get_comp_help_text()).exec())
        tr.addWidget(btn_help)
        
        in_l.addLayout(tr)
        self.comp_text_area = QPlainTextEdit(); self.comp_text_area.setPlaceholderText("Paste source text...")
        in_l.addWidget(self.comp_text_area)
        
        cr = QHBoxLayout()
        btn_load = QPushButton("Load Text File"); btn_load.clicked.connect(self.load_comp_file)
        
        self.spin_chunk = QSpinBox(); self.spin_chunk.setValue(5); self.spin_chunk.setPrefix("Chunk: ")
        self.spin_chunk.setToolTip("Words per search block (Rec: 5-7)")
        
        self.spin_freq = QSpinBox(); self.spin_freq.setValue(10); self.spin_freq.setRange(1,1000); self.spin_freq.setPrefix("Max Freq: ")
        self.spin_freq.setToolTip("Ignore phrases appearing > X times (filters common phrases)")
        
        self.comp_mode_combo = QComboBox(); self.comp_mode_combo.addItems(["Exact", "Variants", "Extended", "Maximum", "Fuzzy"])
        self.comp_mode_combo.setItemData(0, "Exact match")
        self.comp_mode_combo.setItemData(1, "Basic variants")
        self.comp_mode_combo.setItemData(2, "Extended variants")
        self.comp_mode_combo.setItemData(3, "Maximum variants")
        self.comp_mode_combo.setItemData(4, "Fuzzy search")

        self.spin_filter = QSpinBox(); self.spin_filter.setValue(5); self.spin_filter.setPrefix("Filter > ")
        self.spin_filter.setToolTip("Move titles appearing > X times to Appendix")
        
        self.btn_comp_run = QPushButton("Analyze Composition"); self.btn_comp_run.clicked.connect(self.toggle_composition)
        self.btn_comp_run.setStyleSheet("background-color: #2980b9; color: white; font-weight: bold;")
        
        cr.addWidget(btn_load); cr.addWidget(self.spin_chunk); cr.addWidget(self.spin_freq)
        cr.addWidget(self.comp_mode_combo); cr.addWidget(self.spin_filter); cr.addWidget(self.btn_comp_run)
        in_l.addLayout(cr)
        self.comp_progress = QProgressBar(); self.comp_progress.setVisible(False)
        in_l.addWidget(self.comp_progress)
        inp_w.setLayout(in_l); splitter.addWidget(inp_w)
        
        res_w = QWidget(); rl = QVBoxLayout()
        self.comp_tree = QTreeWidget(); self.comp_tree.setHeaderLabels(["Score", "Shelfmark", "Title", "System ID", "Context"])
        self.comp_tree.header().setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        self.comp_tree.itemDoubleClicked.connect(self.show_comp_detail)
        rl.addWidget(self.comp_tree)
        self.btn_comp_export = QPushButton("Save Report"); self.btn_comp_export.clicked.connect(self.export_comp_report); self.btn_comp_export.setEnabled(False)
        rl.addWidget(self.btn_comp_export)
        res_w.setLayout(rl); splitter.addWidget(res_w)
        
        layout.addWidget(splitter); panel.setLayout(layout)
        return panel

    def create_browse_tab(self):
        panel = QWidget(); layout = QVBoxLayout()
        top = QHBoxLayout()
        self.browse_sys_input = QLineEdit(); self.browse_sys_input.setPlaceholderText("Enter System ID...")
        btn_go = QPushButton("Go"); btn_go.clicked.connect(self.browse_load)
        top.addWidget(QLabel("System ID:")); top.addWidget(self.browse_sys_input); top.addWidget(btn_go)
        layout.addLayout(top)
        
        self.browse_info_lbl = QLabel("Enter ID to browse.")
        self.browse_info_lbl.setStyleSheet("font-size: 14px; font-weight: bold; color: #2c3e50;")
        layout.addWidget(self.browse_info_lbl)
        self.browse_text = QTextBrowser(); self.browse_text.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self.browse_text.setFont(QFont("SBL Hebrew", 16))
        layout.addWidget(self.browse_text)
        
        nav = QHBoxLayout()
        self.btn_b_prev = QPushButton("<< Previous Page"); self.btn_b_prev.clicked.connect(lambda: self.browse_navigate(-1))
        self.btn_b_next = QPushButton("Next Page >>"); self.btn_b_next.clicked.connect(lambda: self.browse_navigate(1))
        self.btn_b_prev.setEnabled(False); self.btn_b_next.setEnabled(False)
        self.lbl_page_count = QLabel("Page 0/0")
        nav.addWidget(self.btn_b_prev); nav.addStretch(); nav.addWidget(self.lbl_page_count); nav.addStretch(); nav.addWidget(self.btn_b_next)
        layout.addLayout(nav); panel.setLayout(layout)
        return panel

    def create_settings_tab(self):
        panel = QWidget(); layout = QVBoxLayout()
        
        gb_data = QGroupBox("Data & Index")
        dl = QVBoxLayout()
        btn_dl = QPushButton("Download Transcriptions (Zenodo)"); btn_dl.clicked.connect(lambda: QDesktopServices.openUrl(QUrl("https://doi.org/10.5281/zenodo.17734473")))
        dl.addWidget(btn_dl)
        btn_idx = QPushButton("Build / Rebuild Index"); btn_idx.clicked.connect(self.run_indexing)
        dl.addWidget(btn_idx)
        self.index_progress = QProgressBar(); dl.addWidget(self.index_progress)
        gb_data.setLayout(dl); layout.addWidget(gb_data)
        
        gb_ai = QGroupBox("AI Configuration")
        al = QHBoxLayout()
        self.txt_api_key = QLineEdit(); self.txt_api_key.setText(self.ai_mgr.api_key); self.txt_api_key.setEchoMode(QLineEdit.EchoMode.Password)
        btn_save = QPushButton("Save Key"); btn_save.clicked.connect(lambda: (self.ai_mgr.save_key(self.txt_api_key.text()), QMessageBox.information(self, "Saved", "API Key saved.")))
        al.addWidget(QLabel("API Key:")); al.addWidget(self.txt_api_key); al.addWidget(btn_save)
        gb_ai.setLayout(al); layout.addWidget(gb_ai)
        
        gb_about = QGroupBox("About")
        abl = QVBoxLayout()
        about_txt = """<div style='text-align:center;'><h2>Genizah Search Pro 2.0</h2><p>Developed by Hillel Gershuni (with Gemini AI), gershuni@gmail.com</p><hr><p><b>Data Source:</b> Stoekl Ben Ezra et al. (2025). <i>MiDRASH Automatic Transcriptions</i>. Zenodo. https://doi.org/10.5281/zenodo.17734473</p></div>"""
        lbl_about = QLabel(about_txt); lbl_about.setOpenExternalLinks(True); lbl_about.setAlignment(Qt.AlignmentFlag.AlignCenter)
        abl.addWidget(lbl_about); gb_about.setLayout(abl); layout.addWidget(gb_about)
        
        layout.addStretch(); panel.setLayout(layout)
        return panel

    # --- HELP TEXTS ---
    def get_search_help_text(self):
        return """<h3>Search Modes</h3><ul><li><b>Exact:</b> Only finds exact matches.</li><li><b>Variants (?):</b> Basic OCR errors.</li><li><b>Extended (??):</b> More variants.</li><li><b>Maximum (???):</b> Aggressive swapping (Use caution).</li><li><b>Fuzzy (~):</b> Levenshtein distance (1-2 typos).</li><li><b>Regex:</b> Advanced patterns (Use AI mode for help, or consult your preferable AI engine).</li></ul><hr><b>Gap:</b> Max distance between words."""

    def get_comp_help_text(self):
        return """<h3>Composition Search</h3><p>Finds parallels between a source text and the Genizah.</p><ul><li><b>Chunk:</b> Words per search block (5-7 recommended).</li><li><b>Max Freq:</b> Filter out common phrases appearing > X times.</li><li><b>Filter >:</b> Group results if a title appears frequently (move to Appendix).</li></ul>"""

    # --- LOGIC ---
    def open_ai(self):
        if not self.ai_mgr.api_key:
            QMessageBox.warning(self, "Missing Key", "Please set your Gemini API Key in Settings."); return
        d = AIDialog(self, self.ai_mgr)
        if d.exec(): self.query_input.setText(d.generated_regex); self.mode_combo.setCurrentIndex(5)

    def toggle_search(self):
        if self.is_searching: self.stop_search()
        else: self.start_search()

    def start_search(self):
        query = self.query_input.text().strip()
        if not query: return
        mode = ['literal', 'variants', 'variants_extended', 'variants_maximum', 'fuzzy', 'Regex'][self.mode_combo.currentIndex()]
        gap = int(self.gap_input.text()) if self.gap_input.text().isdigit() else 0
        
        self.is_searching = True; self.btn_search.setText("Stop"); self.btn_search.setStyleSheet("background-color: #c0392b; color: white;")
        self.search_progress.setRange(0, 100); self.search_progress.setValue(0); self.search_progress.setVisible(True)
        self.results_table.setRowCount(0); self.btn_export.setEnabled(False)
        
        self.search_thread = SearchThread(self.searcher, query, mode, gap)
        self.search_thread.results_signal.connect(self.on_search_finished)
        self.search_thread.progress_signal.connect(lambda c, t: (self.search_progress.setMaximum(t), self.search_progress.setValue(c)))
        self.search_thread.error_signal.connect(self.on_error)
        self.search_thread.start()

    def stop_search(self):
        if self.search_thread.isRunning(): self.search_thread.terminate(); self.search_thread.wait()
        self.reset_ui()

    def reset_ui(self):
        self.is_searching = False; self.btn_search.setText("Search"); self.btn_search.setStyleSheet("background-color: #27ae60; color: white;")
        self.search_progress.setVisible(False)

    def on_error(self, err): self.reset_ui(); QMessageBox.critical(self, "Error", str(err))

    def on_search_finished(self, results):
        self.reset_ui()
        self.status_label.setText(f"Found {len(results)}. Loading metadata...")
        self.last_results = results; self.btn_export.setEnabled(True)
        self.results_table.setRowCount(len(results))
        
        ids = []
        for i, res in enumerate(results):
            meta = res['display']; ids.append(meta['id'])
            parsed = self.meta_mgr.parse_full_id_components(res['raw_header'])
            sid = parsed['sys_id'] or meta['id']
            self.results_table.setItem(i, 0, QTableWidgetItem(sid))
            self.results_table.setItem(i, 1, QTableWidgetItem("Loading..."))
            self.results_table.setItem(i, 2, QTableWidgetItem("Loading..."))
            lbl = QLabel(f"<div dir='rtl'>{res['snippet']}</div>"); lbl.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
            self.results_table.setCellWidget(i, 3, lbl)
            self.results_table.setItem(i, 4, QTableWidgetItem(meta['img']))
            self.results_table.setItem(i, 5, QTableWidgetItem(meta['source']))

        self.meta_loader = ShelfmarkLoaderThread(self.meta_mgr, ids)
        self.meta_loader.progress_signal.connect(self.on_meta_progress)
        self.meta_loader.finished_signal.connect(lambda: self.status_label.setText(f"Loaded {len(results)} items."))
        self.meta_loader.start()

    def on_meta_progress(self, curr, total, sid):
        self.status_label.setText(f"Metadata {curr}/{total}")
        meta = self.meta_mgr.nli_cache.get(sid, {})
        for r in range(self.results_table.rowCount()):
            if self.results_table.item(r, 0).text() == sid:
                self.results_table.setItem(r, 1, QTableWidgetItem(meta.get('shelfmark', '')))
                self.results_table.setItem(r, 2, QTableWidgetItem(meta.get('title', '')))
                self.last_results[r]['display']['shelfmark'] = meta.get('shelfmark', '')
                self.last_results[r]['display']['title'] = meta.get('title', '')

    def show_full_text(self):
        row = self.results_table.currentRow()
        if row >= 0: ResultDialog(self, self.last_results, row, self.meta_mgr, self.searcher).exec()

    def export_results(self):
        path, _ = QFileDialog.getSaveFileName(self, "Export", "", "Text (*.txt)")
        if path:
            with open(path, 'w', encoding='utf-8') as f:
                for r in self.last_results:
                    f.write(f"=== {r['display']['shelfmark']} | {r['display']['title']} ===\n{r.get('raw_file_hl','')}\n\n")
            QMessageBox.information(self, "Saved", f"Saved to {path}")

    # Composition & Browse
    def load_comp_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Load", "", "Text (*.txt)")
        if path:
            with open(path, 'r', encoding='utf-8') as f: self.comp_text_area.setPlainText(f.read())

    def toggle_composition(self):
        if self.is_comp_running: self.comp_thread.terminate(); self.is_comp_running = False; self.reset_comp_ui()
        else: self.run_composition()
        
    def reset_comp_ui(self):
        self.is_comp_running = False; self.btn_comp_run.setText("Analyze Composition"); self.comp_progress.setVisible(False)

    def run_composition(self):
        txt = self.comp_text_area.toPlainText().strip(); 
        if not txt: return
        self.is_comp_running = True; self.btn_comp_run.setText("Stop"); self.btn_comp_run.setStyleSheet("background-color: #c0392b; color: white;")
        self.comp_progress.setVisible(True); self.comp_tree.clear()
        mode = ['literal', 'variants', 'variants_extended', 'variants_maximum', 'fuzzy'][self.comp_mode_combo.currentIndex()]
        
        self.comp_thread = CompositionThread(
            self.searcher, txt, self.spin_chunk.value(), self.spin_freq.value(), mode, self.spin_filter.value()
        )
        self.comp_thread.progress_signal.connect(self.comp_progress.setValue)
        self.comp_thread.status_signal.connect(lambda s: self.comp_progress.setFormat(s))
        self.comp_thread.finished_signal.connect(self.on_comp_finished)
        self.comp_thread.error_signal.connect(lambda e: QMessageBox.critical(self, "Error", e))
        self.comp_thread.start()

    def on_comp_finished(self, main, appx, summ):
        self.is_comp_running = False; self.btn_comp_run.setText("Analyze Composition"); self.btn_comp_run.setStyleSheet("background-color: #2980b9; color: white;")
        self.comp_progress.setVisible(False); self.btn_comp_export.setEnabled(True)
        self.comp_main = main; self.comp_appendix = appx; self.comp_summary = summ
        
        root = QTreeWidgetItem(self.comp_tree, [f"Main ({len(main)})"]); root.setExpanded(True)
        for i in main:
            sid, _ = self.meta_mgr.parse_header_smart(i['raw_header'])
            meta = self.meta_mgr.nli_cache.get(sid, {})
            node = QTreeWidgetItem(root)
            node.setText(0, str(i['score'])); node.setText(1, meta.get('shelfmark','')); node.setText(2, meta.get('title','')); node.setText(3, sid)
            node.setText(4, i['text'].split('\n')[0])
            node.setData(0, Qt.ItemDataRole.UserRole, i)
            
        if appx:
            root_a = QTreeWidgetItem(self.comp_tree, [f"Appendix ({len(appx)})"])
            for g, items in sorted(appx.items(), key=lambda x: len(x[1]), reverse=True):
                gn = QTreeWidgetItem(root_a, [f"{g} ({len(items)})"])
                for i in items:
                    sid, _ = self.meta_mgr.parse_header_smart(i['raw_header'])
                    meta = self.meta_mgr.nli_cache.get(sid, {})
                    ch = QTreeWidgetItem(gn)
                    ch.setText(0, str(i['score'])); ch.setText(1, meta.get('shelfmark','')); ch.setText(2, meta.get('title','')); ch.setText(3, sid)
                    ch.setData(0, Qt.ItemDataRole.UserRole, i)

    def show_comp_detail(self, item, col):
        # 1. Validate Click
        data = item.data(0, Qt.ItemDataRole.UserRole)
        if not data: return # It's a folder, ignore
        
        # 2. Flatten the Tree to create a navigation list
        flat_list = []
        clicked_index = -1
        
        # Helper to process a node
        def process_node(node):
            node_data = node.data(0, Qt.ItemDataRole.UserRole)
            if node_data: # It's a leaf item
                sid, p = self.meta_mgr.parse_header_smart(node_data['raw_header'])
                meta = self.meta_mgr.nli_cache.get(sid, {})
                
                ready_data = {
                    'uid': node_data['uid'],
                    'raw_header': node_data['raw_header'],
                    'text': node_data['text'], # Snippet
                    'full_text': None, # Will be fetched by Dialog on load
                    'source_ctx': node_data.get('source_ctx', ''),
                    'display': {
                        'shelfmark': meta.get('shelfmark', ''),
                        'title': meta.get('title', ''),
                        'img': p,
                        'source': node_data['src_lbl']
                    }
                }
                flat_list.append(ready_data)
                
                if node is item:
                    nonlocal clicked_index
                    clicked_index = len(flat_list) - 1

        # Traverse Top Level Items
        root = self.comp_tree.invisibleRootItem()
        for i in range(root.childCount()):
            group = root.child(i) # "Main" or "Appendix"
            # Traverse children of group
            for j in range(group.childCount()):
                sub = group.child(j)
                # Check if sub is a folder (Group in Appendix) or Item (in Main)
                if sub.childCount() > 0:
                    for k in range(sub.childCount()):
                        process_node(sub.child(k))
                else:
                    process_node(sub)

        if clicked_index == -1: return

        # 3. Open Dialog with List
        current_data = flat_list[clicked_index]
        current_data['full_text'] = self.searcher.get_full_text_by_id(current_data['uid']) or current_data['text']
        
        ResultDialog(self, flat_list, clicked_index, self.meta_mgr, self.searcher).exec()

    def export_comp_report(self):
        if not self.comp_main:
            QMessageBox.warning(self, "Save", "No composition data to export.")
            return

        missing_ids = []
        for item in self.comp_main:
            sys_id, p_num = self.meta_mgr.parse_header_smart(item['raw_header'])
            meta = self.meta_mgr.nli_cache.get(sys_id, {}) if sys_id else {}
            shelf = meta.get('shelfmark', '')
            title = meta.get('title', '')
            if not shelf or shelf == 'Unknown' or not title or not p_num or p_num == 'Unknown':
                if sys_id:
                    missing_ids.append(sys_id)

        if missing_ids:
            prompt = (
                "Shelfmark/Title/Page info missing for some items.\n"
                "Continue using system IDs? Choose No to load metadata first."
            )
            choice = QMessageBox.question(
                self, "Metadata Missing", prompt,
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            if choice == QMessageBox.StandardButton.No:
                self.meta_mgr.batch_fetch_shelfmarks(list(set(missing_ids)))
                self._refresh_comp_tree_metadata()

        path, _ = QFileDialog.getSaveFileName(self, "Report", "", "Text (*.txt)")
        if path:
            sep = "=" * 80
            title = self.comp_title_input.text().strip() or "Untitled Composition"
            appendix_count = sum(len(v) for v in self.comp_appendix.values())

            def _fmt_item(item):
                sid, p_num = self.meta_mgr.parse_header_smart(item.get('raw_header', ''))
                meta = self.meta_mgr.nli_cache.get(sid, {}) if sid else {}
                shelfmark = meta.get('shelfmark') or sid or "Unknown"
                title_txt = meta.get('title', '') or "Untitled"
                version = item.get('src_lbl', '') or "Unknown"
                page = p_num or "?"
                uid = item.get('uid', sid) or sid or "Unknown"

                lines = [
                    sep,
                    f"{shelfmark} | {title_txt} | Img: {page} | Version: {version} | ID: {uid} (Score: {item.get('score', 0)})",
                    "Source Context:",
                    (item.get('source_ctx', '') or "[No source context available]").strip(),
                    "",
                    "Manuscript:",
                    (item.get('text', '') or "[No manuscript text available]").strip(),
                    "",
                ]
                return lines

            lines = [
                sep,
                f"Composition Search: {title}",
                sep,
                f"Total Main Manuscripts: {len(self.comp_main)} (Appendix: {appendix_count})",
                sep,
                "FILTERED SUMMARY",
                sep,
            ]

            if self.comp_appendix:
                for sig, items in sorted(self.comp_appendix.items(), key=lambda x: len(x[1]), reverse=True):
                    fallback_summary = []
                    summary_entries = self.comp_summary.get(sig, [])
                    for idx, itm in enumerate(items):
                        shelf_val = summary_entries[idx] if idx < len(summary_entries) else ""
                        if not shelf_val or shelf_val.lower() == 'unknown':
                            sid, _ = self.meta_mgr.parse_header_smart(itm.get('raw_header', ''))
                            meta = self.meta_mgr.nli_cache.get(sid, {}) if sid else {}
                            shelf_val = meta.get('shelfmark') or sid or "Unknown"
                        fallback_summary.append(shelf_val)
                    lines.append(f"{sig} ({len(items)} items): {', '.join(fallback_summary)}")
            else:
                lines.append("No filtered compositions moved to Appendix.")

            lines.extend([
                sep,
                "MAIN MANUSCRIPTS",
                sep,
            ])

            for item in self.comp_main:
                lines.extend(_fmt_item(item))

            if self.comp_appendix:
                lines.extend([
                    sep,
                    "APPENDIX (Filtered Groups)",
                    sep,
                ])
                for sig, items in sorted(self.comp_appendix.items(), key=lambda x: len(x[1]), reverse=True):
                    lines.append(f"{sig} ({len(items)} items)")
                    for item in items:
                        lines.extend(_fmt_item(item))

            with open(path, 'w', encoding='utf-8') as f:
                report_lines = [
                    "Composition Report",
                    f"Main ({len(self.comp_main)})",
                    ""
                ]

                for item in self.comp_main:
                    report_lines.append(self._format_comp_entry(item))
                    report_lines.append("")

                if self.comp_appendix:
                    report_lines.append(f"Appendix ({sum(len(v) for v in self.comp_appendix.values())})")
                    for group, items in sorted(self.comp_appendix.items(), key=lambda x: len(x[1]), reverse=True):
                        report_lines.append(f"-- {group} ({len(items)}) --")
                        for item in items:
                            report_lines.append(self._format_comp_entry(item))
                            report_lines.append("")

                f.write("\n".join(report_lines).strip() + "\n")
            QMessageBox.information(self, "Saved", f"Saved to {path}")

    def _format_comp_entry(self, item):
        sys_id, page, shelfmark, title = self._resolve_meta_labels(item['raw_header'])

        header = f"{shelfmark} | {title} (System ID: {sys_id}, Img: {page or 'N/A'})"
        source_ctx = (item.get('source_ctx', '') or '').strip() or "[No source excerpt available]"
        ms_ctx = (item.get('text', '') or '').strip() or "[No manuscript excerpt available]"
        src_label = item.get('src_lbl', 'Source')

        return "\n".join([
            header,
            f"Source [{sys_id} | {src_label}]:",
            source_ctx,
            f"MS [{sys_id} | Img {page or 'N/A'}]:",
            ms_ctx
        ])

    def _resolve_meta_labels(self, raw_header):
        sid, page = self.meta_mgr.parse_header_smart(raw_header)
        sys_id = sid or "Unknown System ID"

        meta = self.meta_mgr.fetch_nli_data(sid) if sid else {}
        shelf = meta.get('shelfmark') if meta else None
        title = meta.get('title') if meta else None

        shelf_lbl = shelf or f"[Shelfmark missing for {sys_id}]"
        title_lbl = title or "[Title missing]"

        return sys_id, page, shelf_lbl, title_lbl

    def browse_load(self):
        sid = self.browse_sys_input.text().strip()
        if not sid: return
        self.current_browse_sid = sid; self.current_browse_p = None
        self.browse_update_view(0)

    def browse_navigate(self, d): self.browse_update_view(d)

    def browse_update_view(self, d):
        pd = self.searcher.get_browse_page(self.current_browse_sid, self.current_browse_p, d)
        if not pd: QMessageBox.warning(self, "Nav", "Not found or end."); return
        self.current_browse_p = pd['p_num']
        self.browse_text.setHtml(f"<div dir='rtl'>{pd['text'].replace(chr(10), '<br>')}</div>")
        meta = self.meta_mgr.fetch_nli_data(self.current_browse_sid)
        self.browse_info_lbl.setText(f"{meta.get('shelfmark','')} | Img: {pd['p_num']}")
        self.lbl_page_count.setText(f"{pd['current_idx']}/{pd['total_pages']}")
        self.btn_b_prev.setEnabled(pd['current_idx']>1); self.btn_b_next.setEnabled(pd['current_idx']<pd['total_pages'])

    def run_indexing(self):
        if QMessageBox.question(self, "Index", "Start indexing?", QMessageBox.StandardButton.Yes|QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes:
            self.ithread = IndexerThread(self.meta_mgr)
            self.ithread.progress_signal.connect(self.index_progress.setValue)
            self.ithread.progress_signal.connect(lambda c,t: self.index_progress.setMaximum(t))
            self.ithread.finished_signal.connect(lambda: QMessageBox.information(self, "Done", "Complete"))
            self.ithread.start()

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_path, relative_path)

if __name__ == "__main__":
    try:
        import ctypes
        myappid = 'genizah.search.pro.2.0' 
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)
    except ImportError:
        pass

    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
    icon_path = resource_path("icon.ico")
    
    if os.path.exists(icon_path):
        app_icon = QIcon(icon_path)
        app.setWindowIcon(app_icon)
    
    window = GenizahGUI()
    window.show()
    sys.exit(app.exec())