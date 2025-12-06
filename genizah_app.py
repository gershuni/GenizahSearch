# genizah_app.py
import sys
import os
import re
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QLabel, QLineEdit, QPushButton, QTabWidget, QTableWidget, 
                             QTableWidgetItem, QHeaderView, QComboBox, QCheckBox, 
                             QTextEdit, QMessageBox, QProgressBar, QSplitter, QDialog, 
                             QTextBrowser, QFileDialog, QMenu, QGroupBox, QSpinBox, 
                             QTreeWidget, QTreeWidgetItem, QTreeWidgetItemIterator, QPlainTextEdit)
from PyQt6.QtCore import Qt, QTimer, QUrl, QSize
from PyQt6.QtGui import QFont, QIcon, QDesktopServices, QGuiApplication, QAction

from genizah_core import Config, MetadataManager, VariantManager, SearchEngine, Indexer, AIManager
from gui_threads import SearchThread, IndexerThread, ShelfmarkLoaderThread, CompositionThread, AIWorkerThread

# ==============================================================================
#  HELPERS & DIALOGS
# ==============================================================================

class HelpDialog(QDialog):
    def __init__(self, parent, title, content):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(500, 400)
        l = QVBoxLayout()
        t = QTextBrowser(); t.setHtml(content); t.setOpenExternalLinks(True)
        l.addWidget(t)
        b = QPushButton("Close"); b.clicked.connect(self.close)
        l.addWidget(b); self.setLayout(l)

class AIDialog(QDialog):
    def __init__(self, parent, ai_mgr):
        super().__init__(parent)
        self.setWindowTitle("AI Assistant")
        self.resize(600, 500); self.ai = ai_mgr; self.gen_regex = ""
        l = QVBoxLayout()
        self.chat = QTextBrowser(); self.chat.setOpenExternalLinks(True); l.addWidget(self.chat)
        
        il = QHBoxLayout()
        self.inp = QLineEdit(); self.inp.setPlaceholderText("Describe pattern...")
        self.inp.returnPressed.connect(self.send)
        btn = QPushButton("Send"); btn.clicked.connect(self.send)
        il.addWidget(self.inp); il.addWidget(btn); l.addLayout(il)
        
        self.lbl = QLabel("Regex preview"); self.lbl.setStyleSheet("color:blue; background:#eee; padding:5px;")
        l.addWidget(self.lbl)
        self.use_btn = QPushButton("Use Regex"); self.use_btn.setEnabled(False); self.use_btn.clicked.connect(self.accept)
        l.addWidget(self.use_btn); self.setLayout(l)
        self.chat.append("<b>System:</b> Hello! Describe what you want to find.")

    def send(self):
        t = self.inp.text().strip(); 
        if not t: return
        self.chat.append(f"<b>You:</b> {t}"); self.inp.clear(); self.lbl.setText("Thinking...")
        self.worker = AIWorkerThread(self.ai, t)
        self.worker.finished_signal.connect(self.on_resp); self.worker.start()

    def on_resp(self, data, err):
        if err: self.chat.append(f"<b>Error:</b> {err}"); return
        rx = data.get("regex", "")
        self.chat.append(f"<b>AI:</b> {data.get('explanation','')}<br><code>{rx}</code>")
        self.lbl.setText(rx); self.gen_regex = rx; self.use_btn.setEnabled(True)

class ResultDialog(QDialog):
    def __init__(self, parent, all_results, current_idx, meta_mgr, searcher):
        super().__init__(parent)
        self.setWindowTitle("Manuscript Viewer")
        self.resize(1100, 800)
        self.meta_mgr = meta_mgr; self.searcher = searcher
        self.all_res = all_results; self.curr_idx = current_idx
        self.curr_sid = None; self.curr_p = None; self.curr_fl = None
        
        main = QVBoxLayout()
        
        # Result Navigation
        nav_res = QHBoxLayout()
        self.btn_r_prev = QPushButton("◀ Prev Result"); self.btn_r_prev.clicked.connect(lambda: self.nav_res(-1))
        self.lbl_r_idx = QLabel(); self.lbl_r_idx.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.btn_r_next = QPushButton("Next Result ▶"); self.btn_r_next.clicked.connect(lambda: self.nav_res(1))
        nav_res.addWidget(self.btn_r_prev); nav_res.addWidget(self.lbl_r_idx, 1); nav_res.addWidget(self.btn_r_next)
        main.addLayout(nav_res); main.addWidget(QSplitter(Qt.Orientation.Horizontal))
        
        # Header Info
        head = QHBoxLayout()
        titles = QVBoxLayout()
        self.l_shelf = QLabel(); self.l_shelf.setFont(QFont("Arial", 16, QFont.Weight.Bold)); self.l_shelf.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.l_title = QLabel(); self.l_title.setFont(QFont("Arial", 12)); self.l_title.setWordWrap(True); self.l_title.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        titles.addWidget(self.l_shelf); titles.addWidget(self.l_title); head.addLayout(titles, 2)
        
        self.l_info = QLabel(); self.l_info.setStyleSheet("background:#eee; padding:5px; border-radius:5px; color:#222;")
        self.l_info.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse); head.addWidget(self.l_info, 1)
        
        # Page Controls
        ctrl = QVBoxLayout()
        pg = QHBoxLayout()
        btn_pp = QPushButton("Pg <"); btn_pp.setFixedWidth(40); btn_pp.clicked.connect(lambda: self.load_page(off=-1))
        self.sp_pg = QSpinBox(); self.sp_pg.setPrefix("Img: "); self.sp_pg.setRange(1, 9999); self.sp_pg.setFixedWidth(80)
        self.sp_pg.editingFinished.connect(lambda: self.load_page(target=self.sp_pg.value()))
        btn_pn = QPushButton("> Pg"); btn_pn.setFixedWidth(40); btn_pn.clicked.connect(lambda: self.load_page(off=1))
        self.l_total = QLabel("/ ?")
        pg.addWidget(btn_pp); pg.addWidget(self.sp_pg); pg.addWidget(self.l_total); pg.addWidget(btn_pn); ctrl.addLayout(pg)
        
        btn_cat = QPushButton("📄 Catalog"); btn_cat.clicked.connect(self.open_cat)
        btn_img = QPushButton("🖼️ Viewer"); btn_img.clicked.connect(self.open_view)
        ctrl.addWidget(btn_cat); ctrl.addWidget(btn_img); head.addLayout(ctrl)
        main.addLayout(head)
        
        # Text
        self.txt = QTextBrowser(); self.txt.setReadOnly(True); self.txt.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self.txt.setFont(QFont("SBL Hebrew", 16)); main.addWidget(self.txt)
        
        self.setLayout(main)
        self.load_res(current_idx)

    def nav_res(self, d):
        ni = self.curr_idx + d
        if 0 <= ni < len(self.all_res): self.load_res(ni)

    def load_res(self, idx):
        self.curr_idx = idx; data = self.all_res[idx]; self.data = data
        self.btn_r_prev.setEnabled(idx > 0); self.btn_r_next.setEnabled(idx < len(self.all_res)-1)
        self.lbl_r_idx.setText(f"Result {idx+1} / {len(self.all_res)}")
        
        parsed = self.meta_mgr.parse_full_id_components(data['raw_header'])
        self.curr_sid = parsed['sys_id']; 
        try: p = int(parsed['p_num']) 
        except: p = 1
        
        # Initial text (might be snippet or full)
        # If full text missing, fetch it now
        if not data.get('full_text'):
             data['full_text'] = self.searcher.get_full_text_by_id(data['uid']) or data.get('text', '')

        # Load page content (from Index or Browse Map)
        self.load_page(target=p)
        
        # Inject Context & Highlight (Only on initial page load)
        full_html = ""
        if 'source_ctx' in data and data['source_ctx']:
             ctx = re.sub(r'\*(.*?)\*', r"<b style='color:red;'>\1</b>", data['source_ctx'].replace("\n", "<br>"))
             full_html += f"<div style='background:#e8f8f5; color:black; padding:10px; border-bottom:2px solid green;'><b>Context:</b><br>{ctx}</div><br><hr><br>"
        
        # Highlight main text
        raw = data.get('full_text', '')
        hl_html = self.get_hl_html(raw)
        full_html += hl_html
        self.txt.setHtml(f"<div dir='rtl'>{full_html}</div>")

    def get_hl_html(self, raw):
        pat = self.data.get('highlight_pattern')
        txt = raw.replace("\n", "<br>")
        if not pat: return txt
        try:
            rx = re.compile(pat, re.IGNORECASE)
            return rx.sub(lambda m: f"<span style='color:red; font-weight:bold;'>{m.group(0)}</span>", txt)
        except: return txt

    def load_page(self, off=0, target=None):
        if not self.curr_sid: return
        pd = self.searcher.get_browse_page(self.curr_sid, target if target else self.curr_p, 0 if target else off)
        if not pd: return
        
        self.curr_p = pd['p_num']
        self.curr_fl = self.meta_mgr.parse_full_id_components(pd['full_header'])['fl_id']
        
        self.l_info.setText(f"<b>ID:</b> {self.curr_sid}<br><b>FL:</b> {self.curr_fl or 'N/A'}<br><b>Src:</b> {self.data['display']['source']}")
        self.sp_pg.blockSignals(True); self.sp_pg.setValue(self.curr_p); self.sp_pg.blockSignals(False)
        self.l_total.setText(f"/ {pd['total_pages']}")
        
        # If we navigated away from initial result, just show text (maybe highlight if pattern fits?)
        if target is None: # Navigation
             html = self.get_hl_html(pd['text'])
             self.txt.setHtml(f"<div dir='rtl'>{html}</div>")
             
        meta = self.meta_mgr.fetch_nli_data(self.curr_sid)
        self.l_shelf.setText(meta.get('shelfmark','')); self.l_title.setText(meta.get('title',''))

    def open_cat(self): QDesktopServices.openUrl(QUrl(f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{self.curr_sid}"))
    def open_view(self): QDesktopServices.openUrl(QUrl(f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/viewerpage?vid=MANUSCRIPT&docId=PNX_MANUSCRIPTS{self.curr_sid}#d=[[PNX_MANUSCRIPTS{self.curr_sid}-1,FL{self.curr_fl}]]"))

# ==============================================================================
#  MAIN APP
# ==============================================================================
class GenizahGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Genizah Search Pro 2.1")
        self.resize(1300, 850)
        
        self.is_searching = False
        self.is_comp_running = False
        self.last_results = []
        self.comp_main = []
        self.comp_appendix = {}
        self.comp_summary = {}
        
        # Show Loading Screen
        lbl = QLabel("Loading components... Please wait.", self)
        lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.setCentralWidget(lbl)
        
        # Trigger heavy loading
        QTimer.singleShot(100, self.delayed_init)

    def delayed_init(self):
        try:
            # Heavy objects
            self.meta_mgr = MetadataManager()
            self.var_mgr = VariantManager()
            self.searcher = SearchEngine(self.meta_mgr, self.var_mgr)
            self.indexer = Indexer(self.meta_mgr)
            self.ai_mgr = AIManager()
            
            # Additional state
            self.current_browse_sid = None
            self.current_browse_p = None
            
            # Build UI
            self.init_ui()
            
            # Check Index
            idx_path = os.path.join(Config.INDEX_DIR, "tantivy_db")
            if not os.path.exists(idx_path) or not os.listdir(idx_path):
                if QMessageBox.question(self, "Index Missing", "Index not found. Build now? (Required)", QMessageBox.StandardButton.Yes|QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes:
                    self.tabs.setCurrentIndex(3)
                    self.run_indexing()
                    
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))
            
    def init_ui(self):
        self.tabs = QTabWidget()
        self.tabs.addTab(self.tab_search(), "Search")
        self.tabs.addTab(self.tab_comp(), "Composition Search")
        self.tabs.addTab(self.tab_browse(), "Browse")
        self.tabs.addTab(self.tab_settings(), "Settings")
        self.setCentralWidget(self.tabs)

    def closeEvent(self, event):
        # Stop background threads if running
        if hasattr(self, 'meta_loader') and self.meta_loader.isRunning():
            self.meta_loader.terminate()
        
        # Shutdown thread pool in core
        if hasattr(self, 'meta_mgr'):
            self.meta_mgr.nli_executor.shutdown(wait=False)
            
        # Force exit to kill lingering threads
        event.accept()
        QApplication.quit()

    # --- TABS ---
    def tab_search(self):
        p = QWidget(); l = QVBoxLayout()
        top = QHBoxLayout()
        self.q_in = QLineEdit(); self.q_in.setPlaceholderText("Search..."); self.q_in.returnPressed.connect(self.toggle_search)
        
        self.cmb_mode = QComboBox(); self.cmb_mode.addItems(["Exact", "Variants (?)", "Extended (??)", "Maximum (???)", "Fuzzy (~)", "Regex"])
        self.cmb_mode.setToolTip("Search Mode:\n?: Basic OCR\n??: Phonetic\n???: Aggressive\n~: Fuzzy\nRegex: Advanced")
        
        self.gap_in = QLineEdit(); self.gap_in.setPlaceholderText("Gap"); self.gap_in.setFixedWidth(50)
        self.gap_in.setToolTip("Max word distance")
        
        self.btn_s = QPushButton("Search"); self.btn_s.clicked.connect(self.toggle_search)
        self.btn_s.setStyleSheet("background-color: #27ae60; color: white; font-weight: bold;")
        
        btn_ai = QPushButton("🤖 AI"); btn_ai.clicked.connect(self.open_ai); btn_ai.setToolTip("Generate Regex with AI")
        btn_help = QPushButton("?"); btn_help.setFixedWidth(30); btn_help.clicked.connect(lambda: HelpDialog(self, "Help", self.get_help_txt()).exec())
        
        top.addWidget(QLabel("Query:")); top.addWidget(self.q_in, 2)
        top.addWidget(QLabel("Mode:")); top.addWidget(self.cmb_mode)
        top.addWidget(QLabel("Gap:")); top.addWidget(self.gap_in)
        top.addWidget(self.btn_s); top.addWidget(btn_ai); top.addWidget(btn_help)
        l.addLayout(top)
        
        self.prog_s = QProgressBar(); self.prog_s.setVisible(False); l.addWidget(self.prog_s)
        
        self.tbl_res = QTableWidget(0, 6)
        self.tbl_res.setHorizontalHeaderLabels(["System ID", "Shelfmark", "Title", "Snippet", "Img", "Src"])
        self.tbl_res.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self.tbl_res.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.tbl_res.doubleClicked.connect(self.show_full)
        l.addWidget(self.tbl_res)
        
        bot = QHBoxLayout()
        self.lbl_stat = QLabel("Ready."); self.btn_exp = QPushButton("Export"); self.btn_exp.clicked.connect(self.export_res); self.btn_exp.setEnabled(False)
        bot.addWidget(self.lbl_stat, 1); bot.addWidget(self.btn_exp)
        l.addLayout(bot)
        p.setLayout(l); return p

    def tab_comp(self):
        p = QWidget(); l = QVBoxLayout(); spl = QSplitter(Qt.Orientation.Vertical)
        
        # Input
        w1 = QWidget(); l1 = QVBoxLayout()
        r1 = QHBoxLayout(); self.c_tit = QLineEdit(); self.c_tit.setPlaceholderText("Title"); r1.addWidget(QLabel("Title:")); r1.addWidget(self.c_tit)
        btn_h = QPushButton("?"); btn_h.setFixedWidth(30); btn_h.clicked.connect(lambda: HelpDialog(self, "Comp Help", self.get_comp_help()).exec()); r1.addWidget(btn_h)
        l1.addLayout(r1)
        self.c_txt = QPlainTextEdit(); self.c_txt.setPlaceholderText("Paste text..."); l1.addWidget(self.c_txt)
        
        r2 = QHBoxLayout()
        b_load = QPushButton("Load File"); b_load.clicked.connect(self.load_c_file)
        self.sp_chk = QSpinBox(); self.sp_chk.setValue(5); self.sp_chk.setPrefix("Chunk: ")
        self.sp_frq = QSpinBox(); self.sp_frq.setValue(10); self.sp_frq.setRange(1,999); self.sp_frq.setPrefix("Freq: ")
        self.c_mode = QComboBox(); self.c_mode.addItems(["Exact", "Variants", "Extended", "Max", "Fuzzy"])
        self.sp_flt = QSpinBox(); self.sp_flt.setValue(5); self.sp_flt.setPrefix("Filter > ")
        
        self.btn_c_run = QPushButton("Analyze"); self.btn_c_run.clicked.connect(self.toggle_comp)
        self.btn_c_run.setStyleSheet("background-color: #2980b9; color: white; font-weight: bold;")
        self.btn_grp = QPushButton("Group"); self.btn_grp.clicked.connect(self.apply_group); self.btn_grp.setEnabled(False)
        self.btn_ref = QPushButton("🔄"); self.btn_ref.clicked.connect(self.refresh_c_meta)
        
        r2.addWidget(b_load); r2.addWidget(self.sp_chk); r2.addWidget(self.sp_frq); r2.addWidget(self.c_mode)
        r2.addWidget(self.sp_flt); r2.addWidget(self.btn_c_run); r2.addWidget(self.btn_grp); r2.addWidget(self.btn_ref)
        l1.addLayout(r2)
        self.prog_c = QProgressBar(); self.prog_c.setVisible(False); l1.addWidget(self.prog_c)
        w1.setLayout(l1); spl.addWidget(w1)
        
        # Results
        w2 = QWidget(); l2 = QVBoxLayout()
        self.tree = QTreeWidget(); self.tree.setHeaderLabels(["Score", "Shelfmark", "Title", "System ID", "Preview"])
        self.tree.header().setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        self.tree.itemDoubleClicked.connect(self.show_c_detail)
        l2.addWidget(self.tree)
        self.btn_c_exp = QPushButton("Save Report"); self.btn_c_exp.clicked.connect(self.export_c); self.btn_c_exp.setEnabled(False)
        l2.addWidget(self.btn_c_exp)
        w2.setLayout(l2); spl.addWidget(w2)
        
        l.addWidget(spl); p.setLayout(l); return p

    def tab_browse(self):
        p = QWidget(); l = QVBoxLayout()
        top = QHBoxLayout()
        self.b_sid = QLineEdit(); self.b_sid.setPlaceholderText("System ID")
        b_go = QPushButton("Go"); b_go.clicked.connect(self.browse_go)
        top.addWidget(QLabel("Sys ID:")); top.addWidget(self.b_sid); top.addWidget(b_go); l.addLayout(top)
        
        self.b_inf = QLabel("Enter ID"); self.b_inf.setStyleSheet("font-weight:bold; color:#333;")
        l.addWidget(self.b_inf)
        self.b_txt = QTextBrowser(); self.b_txt.setFont(QFont("SBL Hebrew", 16)); self.b_txt.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        l.addWidget(self.b_txt)
        
        nav = QHBoxLayout()
        bp = QPushButton("<"); bp.clicked.connect(lambda: self.browse_nav(-1)); self.bp=bp
        bn = QPushButton(">"); bn.clicked.connect(lambda: self.browse_nav(1)); self.bn=bn
        self.b_lbl = QLabel("0/0")
        nav.addWidget(bp); nav.addStretch(); nav.addWidget(self.b_lbl); nav.addStretch(); nav.addWidget(bn)
        l.addLayout(nav); p.setLayout(l); return p

    def tab_settings(self):
        p = QWidget(); l = QVBoxLayout()
        
        g1 = QGroupBox("Data"); gl1 = QVBoxLayout()
        b1 = QPushButton("Download Data"); b1.clicked.connect(lambda: QDesktopServices.openUrl(QUrl("https://doi.org/10.5281/zenodo.17734473")))
        b2 = QPushButton("Rebuild Index"); b2.clicked.connect(self.run_indexing)
        self.prog_i = QProgressBar(); gl1.addWidget(b1); gl1.addWidget(b2); gl1.addWidget(self.prog_i); g1.setLayout(gl1); l.addWidget(g1)
        
        g2 = QGroupBox("AI"); gl2 = QHBoxLayout()
        self.t_api = QLineEdit(); self.t_api.setEchoMode(QLineEdit.EchoMode.Password); self.t_api.setText(self.ai_mgr.api_key)
        b3 = QPushButton("Save"); b3.clicked.connect(lambda: (self.ai_mgr.save_key(self.t_api.text()), QMessageBox.information(self,"OK","Saved")))
        gl2.addWidget(QLabel("Key:")); gl2.addWidget(self.t_api); gl2.addWidget(b3); g2.setLayout(gl2); l.addWidget(g2)
        
        l.addStretch()
        l.addWidget(QLabel("<center>Genizah Search Pro 2.1<br>Hillel Gershuni (using Gemini Pro AI), gershuni@gmail.com<br>Data Source: Stoekl et.al, MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments [Data set]. Zenodo. https://doi.org/10.5281/zenodo.17734473</center>. Catalog information: National Library of Israel."))
        p.setLayout(l); return p

    # --- LOGIC HANDLERS ---
    
    def open_ai(self):
        if not self.ai_mgr.api_key: QMessageBox.warning(self, "No Key", "Set API Key in Settings."); return
        d = AIDialog(self, self.ai_mgr)
        if d.exec(): self.q_in.setText(d.gen_regex); self.cmb_mode.setCurrentIndex(5)

    def get_help_txt(self): return "<h3>Search Modes</h3><ul><li><b>Exact:</b> Sequence match.</li><li><b>Variants (?):</b> Basic variants, advanced or max.</li><li><b>Fuzzy:</b> Levenshtein distance.</li></ul>"
    def get_comp_help(self): return "<h3>Composition</h3><p>Paste text to find parallels. The program cuts the text to blocks of 5 (or other number) of words and searches in the transcriptions file. Click on reload button to fetch information about the manuscripts, and then group them.</p>"

    # Search
    def toggle_search(self):
        if self.is_searching: 
            if self.s_thread.isRunning(): self.s_thread.terminate()
            self.reset_s()
        else:
            q = self.q_in.text().strip(); 
            if not q: return
            self.is_searching = True; self.btn_s.setText("Stop"); self.btn_s.setStyleSheet("background-color:#c0392b; color:white;")
            self.prog_s.setVisible(True); self.prog_s.setValue(0); self.tbl_res.setRowCount(0)
            
            m = ['literal', 'variants', 'variants_extended', 'variants_maximum', 'fuzzy', 'Regex'][self.cmb_mode.currentIndex()]
            g = int(self.gap_in.text()) if self.gap_in.text().isdigit() else 0
            
            self.s_thread = SearchThread(self.searcher, q, m, g)
            self.s_thread.results_signal.connect(self.on_s_res)
            self.s_thread.progress_signal.connect(lambda c,t: (self.prog_s.setMaximum(t), self.prog_s.setValue(c)))
            self.s_thread.error_signal.connect(lambda e: (self.reset_s(), QMessageBox.critical(self,"Err",e)))
            self.s_thread.start()

    def reset_s(self):
        self.is_searching = False; self.btn_s.setText("Search"); self.btn_s.setStyleSheet("background-color:#27ae60; color:white;")
        self.prog_s.setVisible(False)

    def on_s_res(self, res):
        self.reset_s(); self.lbl_stat.setText(f"Found {len(res)}."); self.last_res = res
        self.tbl_res.setRowCount(len(res)); self.btn_exp.setEnabled(True)
        
        ids = []
        for i, r in enumerate(res):
            m = r['display']; ids.append(m['id']); pid = self.meta_mgr.parse_full_id_components(r['raw_header'])['sys_id'] or m['id']
            self.tbl_res.setItem(i, 0, QTableWidgetItem(pid))
            self.tbl_res.setItem(i, 1, QTableWidgetItem("..."))
            self.tbl_res.setItem(i, 2, QTableWidgetItem("..."))
            l = QLabel(f"<div dir='rtl'>{r['snippet']}</div>"); l.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
            self.tbl_res.setCellWidget(i, 3, l)
            self.tbl_res.setItem(i, 4, QTableWidgetItem(m['img']))
            self.tbl_res.setItem(i, 5, QTableWidgetItem(m['source']))
        
        self.ml = ShelfmarkLoaderThread(self.meta_mgr, ids)
        self.ml.progress_signal.connect(self.on_meta)
        self.ml.start()

    def on_meta(self, c, t, sid):
        self.lbl_stat.setText(f"Meta {c}/{t}")
        m = self.meta_mgr.nli_cache.get(sid, {})
        for r in range(self.tbl_res.rowCount()):
            if self.tbl_res.item(r, 0).text() == sid:
                self.tbl_res.setItem(r, 1, QTableWidgetItem(m.get('shelfmark','')))
                self.tbl_res.setItem(r, 2, QTableWidgetItem(m.get('title','')))
                self.last_res[r]['display']['shelfmark'] = m.get('shelfmark','')
                self.last_res[r]['display']['title'] = m.get('title','')

    def show_full(self):
        r = self.tbl_res.currentRow()
        if r>=0: ResultDialog(self, self.last_res, r, self.meta_mgr, self.searcher).exec()

    def export_res(self):
        path, _ = QFileDialog.getSaveFileName(self, "Export", "", "Text (*.txt)")
        if path:
            with open(path, 'w', encoding='utf-8') as f:
                for r in self.last_res: f.write(f"=== {r['display']['shelfmark']} ===\n{r.get('raw_file_hl','')}\n\n")
            QMessageBox.information(self, "OK", "Saved")

    # Composition
    def load_c_file(self):
        p, _ = QFileDialog.getOpenFileName(self, "Load", "", "Text (*.txt)")
        if p: 
            with open(p, 'r', encoding='utf-8') as f: self.c_txt.setPlainText(f.read())

    def toggle_comp(self):
        if self.is_comp_running: 
            if self.c_thread.isRunning(): self.c_thread.terminate()
            self.is_comp_running = False; self.btn_c_run.setText("Analyze"); self.prog_c.setVisible(False)
        else:
            t = self.c_txt.toPlainText().strip()
            if not t: return
            self.is_comp_running = True; self.btn_c_run.setText("Stop"); self.btn_c_run.setStyleSheet("background-color:#c0392b; color:white;")
            self.prog_c.setVisible(True); self.tree.clear(); self.btn_grp.setEnabled(False)
            m = ['literal', 'variants', 'variants_extended', 'variants_maximum', 'fuzzy'][self.c_mode.currentIndex()]
            
            self.c_thread = CompositionThread(self.searcher, t, self.sp_chk.value(), self.sp_frq.value(), m)
            self.c_thread.progress_signal.connect(self.prog_c.setValue)
            self.c_thread.status_signal.connect(lambda s: self.prog_c.setFormat(s))
            self.c_thread.finished_signal.connect(self.on_c_fin)
            self.c_thread.start()

    def on_c_fin(self, res):
        self.is_comp_running = False; self.btn_c_run.setText("Analyze"); self.btn_c_run.setStyleSheet("background-color:#2980b9; color:white;")
        self.prog_c.setVisible(False); self.btn_grp.setEnabled(True); self.btn_c_exp.setEnabled(True)
        self.raw_c = res; self.c_main = res; self.c_app = {}; self.c_sum = {}
        
        self.populate_tree(res)
        
        ids = [self.meta_mgr.parse_full_id_components(i['raw_header'])['sys_id'] for i in res]
        self.ml_c = ShelfmarkLoaderThread(self.meta_mgr, [x for x in ids if x])
        self.ml_c.progress_signal.connect(self.on_c_meta)
        self.ml_c.start()

    def populate_tree(self, items):
        self.tree.clear()
        r = QTreeWidgetItem(self.tree, [f"Results ({len(items)})"]); r.setExpanded(True)
        for i in items:
            sid = self.meta_mgr.parse_full_id_components(i['raw_header'])['sys_id'] or i['uid']
            m = self.meta_mgr.nli_cache.get(sid, {})
            n = QTreeWidgetItem(r)
            n.setText(0, str(i['score'])); n.setText(1, m.get('shelfmark','')); n.setText(2, m.get('title','')); n.setText(3, sid); n.setText(4, i['text'].split('\n')[0])
            n.setData(0, Qt.ItemDataRole.UserRole, i)

    def on_c_meta(self, c, t, sid):
        # Update tree items with this SID
        m = self.meta_mgr.nli_cache.get(sid, {})
        it = QTreeWidgetItemIterator(self.tree)
        while it.value():
            item = it.value()
            if item.text(3) == sid:
                item.setText(1, m.get('shelfmark','')); item.setText(2, m.get('title',''))
            it += 1

    def refresh_c_meta(self):
        if hasattr(self, 'raw_c'): 
            ids = [self.meta_mgr.parse_full_id_components(i['raw_header'])['sys_id'] for i in self.raw_c]
            self.ml_c = ShelfmarkLoaderThread(self.meta_mgr, [x for x in ids if x])
            self.ml_c.progress_signal.connect(self.on_c_meta)
            self.ml_c.start()

    def apply_group(self):
        if not hasattr(self, 'raw_c'): return
        main, app, sm = self.searcher.group_composition_results(self.raw_c, self.sp_flt.value())
        self.c_main = main; self.c_app = app; self.c_sum = sm
        
        self.tree.clear()
        r1 = QTreeWidgetItem(self.tree, [f"Main ({len(main)})"]); r1.setExpanded(True)
        self.fill_node(r1, main)
        
        if app:
            r2 = QTreeWidgetItem(self.tree, [f"Appendix ({len(app)})"])
            for g, lst in sorted(app.items(), key=lambda x: len(x[1]), reverse=True):
                gn = QTreeWidgetItem(r2, [f"{g} ({len(lst)})"])
                self.fill_node(gn, lst)

    def fill_node(self, parent, items):
        for i in items:
            sid = self.meta_mgr.parse_full_id_components(i['raw_header'])['sys_id'] or i['uid']
            m = self.meta_mgr.nli_cache.get(sid, {})
            n = QTreeWidgetItem(parent)
            n.setText(0, str(i['score'])); n.setText(1, m.get('shelfmark','')); n.setText(2, m.get('title','')); n.setText(3, sid); n.setText(4, i['text'].split('\n')[0])
            n.setData(0, Qt.ItemDataRole.UserRole, i)

    def show_c_detail(self, item, col):
        d = item.data(0, Qt.ItemDataRole.UserRole)
        if not d: return
        
        # Flatten current view for navigation
        flat = []
        curr_idx = 0
        it = QTreeWidgetItemIterator(self.tree)
        while it.value():
            node = it.value()
            nd = node.data(0, Qt.ItemDataRole.UserRole)
            if nd:
                # Reconstruct full object needed for Dialog
                ft = self.searcher.get_full_text_by_id(nd['uid']) or nd['text']
                sid, p = self.meta_mgr.parse_header_smart(nd['raw_header'])
                m = self.meta_mgr.nli_cache.get(sid, {})
                obj = {
                    'uid': nd['uid'], 'raw_header': nd['raw_header'], 
                    'full_text': ft, 'text': nd['text'],
                    'source_ctx': nd.get('source_ctx',''),
                    'highlight_pattern': nd.get('highlight_pattern',''),
                    'display': {'shelfmark': m.get('shelfmark',''), 'title': m.get('title',''), 'img': p, 'source': nd['src_lbl']}
                }
                flat.append(obj)
                if node is item: curr_idx = len(flat)-1
            it += 1
            
        ResultDialog(self, flat, curr_idx, self.meta_mgr, self.searcher).exec()

    def export_c(self):
        path, _ = QFileDialog.getSaveFileName(self, "Export", "", "Text (*.txt)")
        if path:
            with open(path, 'w', encoding='utf-8') as f:
                f.write(f"Composition Report\nMain: {len(self.c_main)}\n\n")
                if self.c_sum:
                    f.write("=== SUMMARY ===\n")
                    for k, v in self.c_sum.items(): f.write(f"* {k}: {len(v)} items\n")
                    f.write("\n")
                for i in self.c_main:
                    f.write(f"=== {i['raw_header']} ===\n{i['text']}\n\n")
            QMessageBox.information(self, "OK", "Saved")

    # Browse
    def browse_go(self):
        sid = self.b_sid.text().strip()
        if sid: self.cur_b_sid = sid; self.cur_b_p = None; self.browse_upd(0)

    def browse_nav(self, d): self.browse_upd(d)

    def browse_upd(self, d):
        if not hasattr(self, 'cur_b_sid'): return
        pd = self.searcher.get_browse_page(self.cur_b_sid, self.cur_b_p, d)
        if not pd: QMessageBox.warning(self, "Nav", "End/Error"); return
        self.cur_b_p = pd['p_num']
        self.b_txt.setHtml(f"<div dir='rtl'>{pd['text'].replace(chr(10), '<br>')}</div>")
        m = self.meta_mgr.fetch_nli_data(self.cur_b_sid)
        self.b_inf.setText(f"{m.get('shelfmark','')} | {m.get('title','')} | Img: {pd['p_num']}")
        self.b_lbl.setText(f"{pd['current_idx']}/{pd['total_pages']}")
        self.bp.setEnabled(pd['current_idx']>1); self.bn.setEnabled(pd['current_idx']<pd['total_pages'])

    # Index
    def run_indexing(self):
        if QMessageBox.question(self, "Index", "Rebuild?", QMessageBox.StandardButton.Yes|QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes:
            self.ithread = IndexerThread(self.meta_mgr)
            self.ithread.progress_signal.connect(self.prog_i.setValue)
            self.ithread.progress_signal.connect(lambda c,t: self.prog_i.setMaximum(t))
            self.ithread.finished_signal.connect(lambda: QMessageBox.information(self, "Done", "Complete"))
            self.ithread.start()

def resource_path(relative_path):
    try: base_path = sys._MEIPASS
    except: base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_path, relative_path)

if __name__ == "__main__":
    try:
        import ctypes
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID('genizah.pro.2.1')
    except: pass

    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
    icon = resource_path("icon.ico")
    if os.path.exists(icon): app.setWindowIcon(QIcon(icon))
    
    w = GenizahGUI()
    w.show()
    sys.exit(app.exec())