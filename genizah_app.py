"""PyQt6 GUI for Genizah search, browsing, and AI assistance."""

# genizah_app.py
import sys
import os
import re
import time
import threading
import json
import requests
import urllib3
import csv
import openpyxl
from openpyxl.styles import Font, PatternFill
from openpyxl.cell.rich_text import TextBlock, CellRichText
from openpyxl.cell.text import InlineFont

from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                             QLabel, QLineEdit, QPushButton, QTabWidget, QTableWidget,
                             QTableWidgetItem, QHeaderView, QComboBox, QCheckBox,
                             QTextEdit, QMessageBox, QProgressBar, QSplitter, QDialog,
                             QTextBrowser, QFileDialog, QMenu, QGroupBox, QSpinBox,
                             QDoubleSpinBox, QTreeWidget, QTreeWidgetItem, QPlainTextEdit, QStyle,
                             QGridLayout, QToolTip, QProgressDialog, QStackedLayout,
                             QScrollArea, QFrame) 
from PyQt6.QtCore import Qt, QTimer, QUrl, QSize, pyqtSignal, QThread, QEventLoop, QEvent 
from PyQt6.QtGui import QFont, QIcon, QDesktopServices, QPixmap, QImage, QFontMetrics, QTextDocument

from version import APP_VERSION

_CORE_IMPORT_ERROR = None
try:
    from genizah_core import Config, MetadataManager, VariantManager, SearchEngine, LabEngine, Indexer, AIManager, tr, save_language, CURRENT_LANG, check_external_services, get_logger
except ImportError as import_error:
    _CORE_IMPORT_ERROR = import_error

if _CORE_IMPORT_ERROR:
    def _show_core_import_error(err):
        app = QApplication.instance() or QApplication(sys.argv)
        QMessageBox.critical(None, "Missing dependency", str(err))

    if __name__ == "__main__":
        _show_core_import_error(_CORE_IMPORT_ERROR)
        sys.exit(1)
    else:
        raise _CORE_IMPORT_ERROR
from gui_threads import SearchThread, LabSearchThread, IndexerThread, ShelfmarkLoaderThread, CompositionThread, LabCompositionThread, GroupingThread, AIWorkerThread, StartupThread, ConnectivityThread
from filter_text_dialog import FilterTextDialog

logger = get_logger(__name__)

class LabSettingsDialog(QDialog):
    """Configuration for Lab Mode (Two-Stage Retrieval & Advanced Indexing)."""
    def __init__(self, parent, lab_engine):
        super().__init__(parent)
        self.setWindowTitle(tr("Lab Mode Settings"))
        self.resize(600, 600)
        self.lab_engine = lab_engine
        self.settings = lab_engine.settings
        if CURRENT_LANG == 'he':
            self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        layout = QVBoxLayout()

        # --- Parameters ---
        param_group = QGroupBox(tr("Search Parameters"))
        pg_layout = QGridLayout()

        self.spin_budget = QSpinBox()
        self.spin_budget.setRange(100, 20000); self.spin_budget.setSingleStep(100)
        self.spin_budget.setValue(self.settings.expansion_budget)
        self.spin_budget.setToolTip(tr("Max number of variants to search per term."))

        self.spin_slop = QSpinBox()
        self.spin_slop.setRange(1, 100)
        self.spin_slop.setValue(self.settings.slop_window)
        self.spin_slop.setToolTip(tr("Size of the window to check for matches."))

        self.spin_rare_bonus = QDoubleSpinBox()
        self.spin_rare_bonus.setRange(0.0, 5.0); self.spin_rare_bonus.setSingleStep(0.1)
        self.spin_rare_bonus.setValue(self.settings.rare_word_bonus)

        self.spin_candidate_limit = QSpinBox()
        self.spin_candidate_limit.setRange(500, 50000)
        self.spin_candidate_limit.setSingleStep(500)
        self.spin_candidate_limit.setValue(self.settings.candidate_limit)
        self.spin_candidate_limit.setToolTip(tr("Max Stage 1 candidates (default 2000, max 50000)."))

        self.spin_max_changes = QSpinBox()
        self.spin_max_changes.setRange(1, 3)
        self.spin_max_changes.setValue(self.settings.max_char_changes)
        self.spin_max_changes.setToolTip(tr("How many letters in a single word can be swapped at once. 1 = High precision/speed, 2-3 = Maximum Recall but very slow."))

        self.spin_min_match = QSpinBox()
        self.spin_min_match.setRange(1, 100)
        self.spin_min_match.setValue(self.settings.minimum_match_pct)
        self.spin_min_match.setSuffix("%")
        self.spin_min_match.setToolTip(tr("Minimum percent of query terms required for Stage 1 candidates."))

        self.chk_use_custom_variants = QCheckBox(tr("Use Custom Variants"))
        self.chk_use_custom_variants.setChecked(self.settings.use_custom_variants)
        self.chk_use_custom_variants.setToolTip(tr("Use your custom char=char replacements."))

        self.chk_use_double_scan = QCheckBox(tr("Use Double Scan (Stage 2)"))
        self.chk_use_double_scan.setChecked(self.settings.use_double_scan)
        self.chk_use_double_scan.setToolTip(tr("Run Stage 2 re-ranking (slower but more precise)."))

        self.chk_normalize = QCheckBox(tr("Normalize Abbreviations (Stage 2)"))
        self.chk_normalize.setChecked(self.settings.normalize_abbreviations)

        # New Options
        self.chk_use_slop = QCheckBox(tr("Use Slop Window"))
        self.chk_use_slop.setChecked(self.settings.use_slop_window)
        self.chk_use_slop.setToolTip(tr("Enable sliding window density check."))

        self.chk_use_rare = QCheckBox(tr("Use Rare Word Filtering"))
        self.chk_use_rare.setChecked(self.settings.use_rare_words)
        self.chk_use_rare.setToolTip(tr("Boost score for rare words found in document."))

        self.chk_prefix = QCheckBox(tr("Prefix Mode (Begins with...)"))
        self.chk_prefix.setChecked(self.settings.prefix_mode)
        self.chk_prefix.setToolTip(tr("Search for words starting with the query terms (e.g. 'lam' matches 'lama')."))

        self.spin_prefix_chars = QSpinBox()
        self.spin_prefix_chars.setRange(1, 10)
        self.spin_prefix_chars.setValue(self.settings.prefix_chars)
        self.spin_prefix_chars.setToolTip(tr("How many letters from the start of each word to match in Prefix Mode."))

        # Order Tolerance Group
        self.grp_order = QGroupBox(tr("Order Tolerance"))
        self.grp_order.setCheckable(True)
        self.grp_order.setChecked(self.settings.use_order_tolerance)
        self.grp_order.setToolTip(tr("Require N words to appear in the same order within a window of N+M words."))

        o_layout = QHBoxLayout()
        o_layout.addWidget(QLabel(tr("Find")))
        self.spin_order_n = QSpinBox(); self.spin_order_n.setRange(1, 20); self.spin_order_n.setValue(self.settings.order_n)
        o_layout.addWidget(self.spin_order_n)
        o_layout.addWidget(QLabel(tr("words in order out of")))
        self.spin_order_m = QSpinBox(); self.spin_order_m.setRange(1, 20); self.spin_order_m.setValue(self.settings.order_m)
        o_layout.addWidget(self.spin_order_m)
        o_layout.addWidget(QLabel(tr("extra words (Window N+M)")))
        self.grp_order.setLayout(o_layout)

        pg_layout.addWidget(QLabel(tr("Expansion Budget:")), 0, 0)
        pg_layout.addWidget(self.spin_budget, 0, 1)
        pg_layout.addWidget(QLabel(tr("Max Changes per Word:")), 1, 0)
        pg_layout.addWidget(self.spin_max_changes, 1, 1)
        pg_layout.addWidget(self.chk_use_custom_variants, 2, 0, 1, 2)

        pg_layout.addWidget(QLabel(tr("Candidate Limit:")), 3, 0)
        pg_layout.addWidget(self.spin_candidate_limit, 3, 1)
        pg_layout.addWidget(QLabel(tr("Minimum Match %:")), 4, 0)
        pg_layout.addWidget(self.spin_min_match, 4, 1)
        pg_layout.addWidget(self.chk_use_double_scan, 5, 0, 1, 2)
        pg_layout.addWidget(self.chk_prefix, 6, 0, 1, 2)
        pg_layout.addWidget(QLabel(tr("Prefix Length:")), 7, 0)
        pg_layout.addWidget(self.spin_prefix_chars, 7, 1)

        pg_layout.addWidget(QLabel(tr("Slop Window:")), 8, 0)
        pg_layout.addWidget(self.spin_slop, 8, 1)
        pg_layout.addWidget(self.chk_use_slop, 9, 0, 1, 2)
        pg_layout.addWidget(self.chk_use_rare, 10, 0, 1, 2)
        pg_layout.addWidget(QLabel(tr("Rare Word Bonus:")), 11, 0)
        pg_layout.addWidget(self.spin_rare_bonus, 11, 1)
        pg_layout.addWidget(self.chk_normalize, 12, 0, 1, 2)
        pg_layout.addWidget(self.grp_order, 13, 0, 1, 2)

        param_group.setLayout(pg_layout)
        layout.addWidget(param_group)

        # --- Custom Variants ---
        var_group = QGroupBox(tr("Custom Variants (char=char, word=word)"))
        vg_layout = QVBoxLayout()
        self.txt_variants = QPlainTextEdit()
        self.txt_variants.setPlaceholderText("א=ע\nנו=מ\n...")
        self.txt_variants.setPlainText(self.settings.get_variants_text())
        vg_layout.addWidget(self.txt_variants)
        var_group.setLayout(vg_layout)
        layout.addWidget(var_group)

        # --- Indexing ---
        idx_group = QGroupBox(tr("Lab Index"))
        ig_layout = QHBoxLayout()
        self.btn_rebuild = QPushButton(tr("Rebuild Lab Index"))
        self.btn_rebuild.setStyleSheet("background-color: #e67e22; color: white; font-weight: bold;")
        self.btn_rebuild.clicked.connect(self.run_rebuild)
        self.lbl_idx_status = QLabel("")
        ig_layout.addWidget(self.btn_rebuild)
        ig_layout.addWidget(self.lbl_idx_status)
        idx_group.setLayout(ig_layout)
        layout.addWidget(idx_group)

        # --- Footer ---
        btn_box = QHBoxLayout()
        self.btn_save = QPushButton(tr("Save & Close"))
        self.btn_save.clicked.connect(self.save_and_close)
        self.btn_cancel = QPushButton(tr("Cancel"))
        self.btn_cancel.clicked.connect(self.reject)

        btn_copy = QPushButton(tr("Copy Config JSON"))
        btn_copy.clicked.connect(self.copy_json)

        btn_box.addWidget(btn_copy)
        btn_box.addStretch()
        btn_box.addWidget(self.btn_cancel)
        btn_box.addWidget(self.btn_save)
        layout.addLayout(btn_box)

        self.setLayout(layout)

    def save_and_close(self):
        self.settings.expansion_budget = self.spin_budget.value()
        self.settings.slop_window = self.spin_slop.value()
        self.settings.rare_word_bonus = self.spin_rare_bonus.value()
        self.settings.candidate_limit = self.spin_candidate_limit.value()
        self.settings.minimum_match_pct = self.spin_min_match.value()
        self.settings.max_char_changes = self.spin_max_changes.value()
        self.settings.use_custom_variants = self.chk_use_custom_variants.isChecked()
        self.settings.use_standard_variants = True
        self.settings.use_double_scan = self.chk_use_double_scan.isChecked()
        self.settings.normalize_abbreviations = self.chk_normalize.isChecked()
        self.settings.use_slop_window = self.chk_use_slop.isChecked()
        self.settings.use_rare_words = self.chk_use_rare.isChecked()
        self.settings.prefix_mode = self.chk_prefix.isChecked()
        self.settings.prefix_chars = self.spin_prefix_chars.value()
        self.settings.use_order_tolerance = self.grp_order.isChecked()
        self.settings.order_n = self.spin_order_n.value()
        self.settings.order_m = self.spin_order_m.value()
        self.settings.parse_variants_text(self.txt_variants.toPlainText())
        self.settings.save()
        self.accept()

    def copy_json(self):
        cfg = {
            'expansion_budget': self.spin_budget.value(),
            'slop_window': self.spin_slop.value(),
            'custom_variants': self.txt_variants.toPlainText(),
            'rare_word_bonus': self.spin_rare_bonus.value(),
            'candidate_limit': self.spin_candidate_limit.value(),
            'minimum_match_pct': self.spin_min_match.value(),
            'max_char_changes': self.spin_max_changes.value(),
            'prefix_chars': self.spin_prefix_chars.value(),
            'use_custom_variants': self.chk_use_custom_variants.isChecked(),
            'use_double_scan': self.chk_use_double_scan.isChecked()
        }
        QApplication.clipboard().setText(json.dumps(cfg, indent=2))
        QMessageBox.information(self, tr("Copied"), tr("Configuration JSON copied to clipboard."))

    def run_rebuild(self):
        self.btn_rebuild.setEnabled(False)
        self.lbl_idx_status.setText(tr("Starting..."))
        QApplication.processEvents()

        class RebuildThread(QThread):
            finished_sig = pyqtSignal(int)
            progress_sig = pyqtSignal(int, int)
            error_sig = pyqtSignal(str)

            def __init__(self, engine):
                super().__init__()
                self.engine = engine
            def run(self):
                try:
                    # Explicitly ensure directories exist
                    if not os.path.exists(Config.LAB_DIR):
                        os.makedirs(Config.LAB_DIR)
                    if not os.path.exists(Config.LAB_INDEX_DIR):
                        os.makedirs(Config.LAB_INDEX_DIR)

                    def cb(curr, total):
                        self.progress_sig.emit(curr, total)

                    count = self.engine.rebuild_lab_index(progress_callback=cb)
                    self.finished_sig.emit(count)
                except Exception as e:
                    self.error_sig.emit(str(e))

        self.worker = RebuildThread(self.lab_engine)
        self.worker.progress_sig.connect(self.on_rebuild_progress)
        self.worker.finished_sig.connect(self.on_rebuild_finished)
        self.worker.error_sig.connect(self.on_rebuild_error)
        self.worker.start()

    def on_rebuild_progress(self, current, total):
        pct = 0
        if total > 0:
            pct = int((current / total) * 100)
        self.lbl_idx_status.setText(tr("Indexing: {}%").format(pct))

    def on_rebuild_error(self, err):
        self.btn_rebuild.setEnabled(True)
        self.lbl_idx_status.setText(tr("Error"))
        QMessageBox.critical(self, tr("Error"), str(err))

    def on_rebuild_finished(self, count):
        self.lbl_idx_status.setText(tr("Done. {} docs.").format(count))
        self.btn_rebuild.setEnabled(True)
        QMessageBox.information(self, tr("Success"), tr("Lab Index rebuilt successfully."))
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_TLS_NOTICE_LOGGED = False


def log_tls_relaxation_notice():
    """Log once that TLS verification is intentionally disabled for thumbnail fetches."""
    global _TLS_NOTICE_LOGGED
    if not _TLS_NOTICE_LOGGED:
        logger.info(
            "TLS verification is disabled for thumbnail downloads to accommodate legacy IIIF endpoints "
            "with outdated certificates; certificate validation is skipped for these image requests."
        )
        _TLS_NOTICE_LOGGED = True


class ShelfmarkTableWidgetItem(QTableWidgetItem):
    """Custom item for sorting shelfmarks by ignoring 'Ms.' prefix and case."""
    def __lt__(self, other):
        text1 = self.text()
        text2 = other.text()

        # Normalize: Remove 'Ms.'/'Ms' prefix (case insensitive) and lower case
        # We strip leading whitespace, then optional 'ms', optional '.', then whitespace
        norm1 = re.sub(r'^\s*ms\.?\s*', '', text1, flags=re.IGNORECASE)
        norm2 = re.sub(r'^\s*ms\.?\s*', '', text2, flags=re.IGNORECASE)

        def natural_keys(text):
            return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', text)]

        return natural_keys(norm1) < natural_keys(norm2)

class HiddenScrollArea(QScrollArea):
    def __init__(self, text_with_markers="", anchor_text=None, parent=None):
        super().__init__(parent)
        self._raw_text = text_with_markers
        self._anchor_text = anchor_text
        
        # Hide scrollbars
        self.setWidgetResizable(True)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setFrameShape(QFrame.Shape.NoFrame)
        
        # Add horizontal margins to create a gap between columns (5px on each side = 10px total gap)
        # We also ensure the background is transparent to show the row selection color
        self.setStyleSheet("QScrollArea { background: transparent; margin-left: 5px; margin-right: 5px; }")
        
        # Keep height strictly slim
        self.setFixedHeight(self.fontMetrics().lineSpacing() + 4)
        
        self.label = QLabel()
        self.label.setTextFormat(Qt.TextFormat.RichText)
        self.label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        # Ensure label background is transparent
        self.label.setStyleSheet("background: transparent;")
        self.setWidget(self.label)
        
        self._update_content()

    def _update_content(self):
        if not self._raw_text:
            self.label.setText(""); return

        # Apply coloring to markers
        processed = re.sub(r'\*(.*?)\*', r"<b style='color:#c0392b;'>\1</b>", self._raw_text)
        processed = re.sub(r'\*([^*]+)$', r"<b style='color:#c0392b;'>\1</b>", processed)
        processed = re.sub(r'^([^*]+)\*', r"<b style='color:#c0392b;'>\1</b>", processed)
        final_html = processed.replace("*", "")
        
        # Enforce non-breaking text
        self.label.setText(f"<div dir='rtl' style='white-space:nowrap; padding: 0 5px;'>{final_html}</div>")
        self.setToolTip(self._raw_text.replace("*", ""))
        
        # Position highlight in view initially
        QTimer.singleShot(10, self._center_on_match)

    def _center_on_match(self):
        target_pos = -1
        if self._anchor_text:
            target_pos = self._raw_text.find(f"*{self._anchor_text}*")
        if target_pos == -1:
            target_pos = self._raw_text.find('*')
            
        if target_pos != -1:
            bar = self.horizontalScrollBar()
            max_val = bar.maximum()
            if max_val > 0:
                # Calculate center ratio for RTL scrollbar
                ratio = (len(self._raw_text) - target_pos) / len(self._raw_text)
                bar.setValue(int(max_val * ratio))

    def wheelEvent(self, event):
        # Convert vertical wheel movement to horizontal scroll
        if event.angleDelta().y() != 0:
            bar = self.horizontalScrollBar()
            # Sensitivity adjustment
            bar.setValue(bar.value() - event.angleDelta().y())
            event.accept()
        else:
            super().wheelEvent(event)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        # Maintain highlight focus when column width changes
        QTimer.singleShot(10, self._center_on_match)
        
class ImageLoaderThread(QThread):
    """
    Smart Image Loader:
    1. Checks Local Disk Cache first.
    2. If missing, Downloads from IIIF (with Rosetta fallback).
    3. Saves successful downloads to Disk Cache.
    """

    image_loaded = pyqtSignal(QImage)
    load_failed = pyqtSignal()

    def __init__(self, url):
        super().__init__()
        self.url = url
        self._cancelled = False
        
        # Ensure cache directory exists
        if not os.path.exists(Config.IMAGE_CACHE_DIR):
            try:
                os.makedirs(Config.IMAGE_CACHE_DIR)
            except Exception as e:
                logger.warning(
                    "Could not create image cache directory at %s: %s; image caching disabled for this session.",
                    Config.IMAGE_CACHE_DIR,
                    e,
                )

    def cancel(self):
        self._cancelled = True

    def run(self):
        if not self.url:
            self.load_failed.emit()
            return

        # 1. Try to identify the FL ID to use as a filename
        fl_match = re.search(r'FL(\d+)', self.url)
        local_path = None
        
        if fl_match:
            fl_id = fl_match.group(1)
            local_path = os.path.join(Config.IMAGE_CACHE_DIR, f"FL{fl_id}.jpg")
            
            # --- CHECK LOCAL CACHE ---
            if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                img = QImage(local_path)
                if not img.isNull():
                    self.image_loaded.emit(img)
                    return
                else:
                    # Corrupt file? Delete it so we re-download
                    try:
                        os.remove(local_path)
                    except Exception as e:
                        logger.warning("Failed to remove corrupt cache file %s: %s", local_path, e)

        # 2. Download from Network (if not in cache)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://www.nli.org.il/"
        }

        data = None
        
        # Attempt A: Original URL
        data = self._download_bytes(self.url, headers)
        
        # Attempt B: Fallback to Rosetta if Attempt A failed and we have an FL ID
        if data is None and fl_match and not self._cancelled:
            fl_digits = fl_match.group(1)
            logger.info("Cache miss & IIIF failed. Trying Rosetta fallback for FL%s...", fl_digits)
            fallback_url = MetadataManager.get_rosetta_fallback_url(fl_digits)
            if fallback_url:
                data = self._download_bytes(fallback_url, headers)

        # 3. Process Result
        if data:
            img = QImage.fromData(data)
            if not img.isNull():
                self.image_loaded.emit(img)
                
                # --- SAVE TO LOCAL CACHE ---
                if local_path and not self._cancelled:
                    try:
                        with open(local_path, 'wb') as f:
                            f.write(data)
                        logger.debug("Saved thumbnail cache to %s", local_path)
                    except Exception as e:
                        logger.warning(
                            "Failed to write thumbnail cache for %s: %s; future loads will re-download.",
                            local_path,
                            e,
                        )
            else:
                self.load_failed.emit()
        else:
            self.load_failed.emit()

    def _download_bytes(self, target_url, headers):
        """Helper to download bytes safely."""
        try:
            resp = requests.get(target_url, headers=headers, timeout=25, stream=True, verify=False)
            if self._cancelled: return None
            if resp.status_code == 200:
                return resp.content
            return None
        except Exception as e:
            logger.warning("Image download failed for %s: %s", target_url, e)
            return None
                
class HelpDialog(QDialog):
    """Display HTML help content from the bundled Help.html file with graceful fallback."""
    def __init__(self, parent, title, source_path=None, anchor=None, fallback_html="", lang="en"):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setWindowIcon(QIcon(os.path.join(Config.BASE_DIR, "icon.ico")))
        self.resize(900, 700)
        layout = QVBoxLayout()
        self.text = QTextBrowser()
        self.text.setOpenExternalLinks(True)
        layout.addWidget(self.text)

        self._load_content(source_path, anchor, fallback_html, lang)

        btn = QPushButton(tr("Close"))
        btn.clicked.connect(self.close)
        layout.addWidget(btn)
        self.setLayout(layout)

    def _load_content(self, source_path, anchor, fallback_html, lang):
        if source_path and os.path.exists(source_path):
            try:
                with open(source_path, "r", encoding="utf-8") as f:
                    content = f.read()
                # Inject language attribute to control visibility of bilingual sections
                if "<body" in content:
                    content = content.replace("<body", f"<body data-lang='{lang}'", 1)
                base_url = QUrl.fromLocalFile(source_path)
                self.text.setHtml(content, base_url)
                if anchor:
                    QTimer.singleShot(0, lambda: self.text.scrollToAnchor(anchor))
                return
            except Exception as e:
                logger.warning("Failed to load help file %s: %s", source_path, e)
        # Fallback: prefer clean content without warning if we have a fallback HTML snippet
        if fallback_html:
            self.text.setHtml(fallback_html)
        else:
            notice = "<p style='color:#c0392b;'><b>Help file is missing or could not be loaded.</b></p>"
            self.text.setHtml(notice)
        if anchor:
            QTimer.singleShot(0, lambda: self.text.scrollToAnchor(anchor))

class AIDialog(QDialog):
    """Chat interface for requesting regex suggestions from the AI manager."""
    def __init__(self, parent, ai_mgr):
        super().__init__(parent)
        self.setWindowTitle(tr("AI Regex Assistant ({})").format(ai_mgr.provider))
        self.resize(600, 500)
        self.ai_mgr = ai_mgr
        self.generated_regex = ""
        
        layout = QVBoxLayout()
        self.chat_display = QTextBrowser()
        self.chat_display.setOpenExternalLinks(True)
        layout.addWidget(self.chat_display)
        
        input_layout = QHBoxLayout()
        self.prompt_input = QLineEdit()
        self.prompt_input.setPlaceholderText(tr("Describe pattern (e.g. 'Word starting with Aleph')..."))
        self.prompt_input.returnPressed.connect(self.send_request)
        self.btn_send = QPushButton(tr("Send"))
        self.btn_send.clicked.connect(self.send_request)
        input_layout.addWidget(self.prompt_input)
        input_layout.addWidget(self.btn_send)
        layout.addLayout(input_layout)
        
        self.lbl_preview = QLabel(tr("Generated Regex will appear here."))
        self.lbl_preview.setStyleSheet("font-weight: bold; color: #2980b9; padding: 10px; background: #ecf0f1;")
        self.lbl_preview.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        layout.addWidget(self.lbl_preview)
        
        self.btn_use = QPushButton(tr("Use this Regex"))
        self.btn_use.clicked.connect(self.accept)
        self.btn_use.setEnabled(False)
        self.btn_use.setStyleSheet("background-color: #27ae60; color: white; font-weight: bold;")
        layout.addWidget(self.btn_use)
        self.setLayout(layout)
        self.append_chat("System", tr("Hello! I can help you build Regex for Hebrew manuscripts."))

    def append_chat(self, sender, text):
        sender_tr = tr(sender)
        color = "blue" if sender == "System" else "green" if sender == "You" else "black"
        self.chat_display.append(f"<b style='color:{color}'>{sender_tr}:</b> {text}<br>")

    def send_request(self):
        text = self.prompt_input.text().strip()
        if not text: return
        self.append_chat("You", text)
        self.prompt_input.clear(); self.prompt_input.setEnabled(False); self.btn_send.setEnabled(False)
        self.lbl_preview.setText(tr("Thinking..."))
        self.worker = AIWorkerThread(self.ai_mgr, text)
        self.worker.finished_signal.connect(self.on_response)
        self.worker.start()

    def on_response(self, data, err):
        self.prompt_input.setEnabled(True); self.btn_send.setEnabled(True); self.prompt_input.setFocus()
        if err:
            self.append_chat("Error", err); self.lbl_preview.setText(tr("Error."))
            return
        regex = data.get("regex", "")
        self.append_chat("Gemini", f"{data.get('explanation', '')}<br><code>{regex}</code>")
        self.lbl_preview.setText(regex)
        self.generated_regex = regex
        self.btn_use.setEnabled(True)

class ExcludeDialog(QDialog):
    """Collect system IDs or shelfmarks that should be excluded from searches."""
    def __init__(self, parent, existing_entries=None):
        super().__init__(parent)
        self.setWindowTitle(tr("Exclude Manuscripts"))
        self.resize(500, 400)
        layout = QVBoxLayout()

        help_lbl = QLabel(tr("Enter system IDs or shelfmarks to exclude (one per line). Matching values are filled automatically."))
        help_lbl.setWordWrap(True)
        layout.addWidget(help_lbl)

        self._syncing = False
        self._shelf_to_sys = None
        self._last_edited = None
        self._full_titles = []
        self._display_titles = []
        self.meta_mgr = getattr(parent, "meta_mgr", None)

        grid = QGridLayout()
        grid.addWidget(QLabel(tr("System IDs")), 0, 0)
        grid.addWidget(QLabel(tr("Shelfmarks")), 0, 1)
        grid.addWidget(QLabel(tr("Title")), 0, 2)

        self.sys_text_area = QPlainTextEdit()
        self.sys_text_area.setPlaceholderText("990051564290205171\n990053963680205171")
        self.sys_text_area.textChanged.connect(self._on_sys_text_changed)

        self.shelf_text_area = QPlainTextEdit()
        self.shelf_text_area.setPlaceholderText("T-S NS 192.21\nMS heb. e.34/30\nMs. EVR II B 1011\nMs. Kaufmann GEN 227/A")
        self.shelf_text_area.textChanged.connect(self._on_shelf_text_changed)

        self.title_text_area = QPlainTextEdit()
        self.title_text_area.setPlaceholderText(tr("Title"))
        self.title_text_area.setReadOnly(True)
        self.title_text_area.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)

        self.sys_text_area.installEventFilter(self)
        self.shelf_text_area.installEventFilter(self)
        self.title_text_area.installEventFilter(self)

        grid.addWidget(self.sys_text_area, 1, 0)
        grid.addWidget(self.shelf_text_area, 1, 1)
        grid.addWidget(self.title_text_area, 1, 2)
        layout.addLayout(grid)

        if existing_entries:
            sys_entries, shelf_entries = self._split_existing_entries(existing_entries)
            if sys_entries:
                self.sys_text_area.setPlainText("\n".join(sys_entries))
            if shelf_entries:
                self.shelf_text_area.setPlainText("\n".join(shelf_entries))
            if sys_entries and not shelf_entries:
                self._last_edited = "sys"
                self._sync_from_sys()
            elif shelf_entries and not sys_entries:
                self._last_edited = "shelf"
                self._sync_from_shelf()
            elif sys_entries:
                self._set_titles(self._resolve_titles_from_sys(sys_entries))

        btn_row = QHBoxLayout()
        self.btn_load = QPushButton(tr("Load from File"))
        self.btn_load.clicked.connect(self.load_file)
        btn_row.addWidget(self.btn_load)

        btn_row.addStretch()
        btn_apply = QPushButton(tr("Apply"))
        btn_apply.clicked.connect(self.accept)
        btn_cancel = QPushButton(tr("Cancel"))
        btn_cancel.clicked.connect(self.reject)
        btn_row.addWidget(btn_cancel)
        btn_row.addWidget(btn_apply)
        layout.addLayout(btn_row)

        self.setLayout(layout)

    def eventFilter(self, obj, event):
        if event.type() == QEvent.Type.FocusIn:
            if obj is self.sys_text_area:
                self._last_edited = "sys"
            elif obj is self.shelf_text_area:
                self._last_edited = "shelf"
        if obj is self.title_text_area and event.type() == QEvent.Type.ToolTip:
            cursor = self.title_text_area.cursorForPosition(event.pos())
            line_idx = cursor.blockNumber()
            if 0 <= line_idx < len(self._full_titles):
                full_title = self._full_titles[line_idx]
                display_title = self._display_titles[line_idx]
                if full_title and full_title != display_title:
                    QToolTip.showText(event.globalPos(), full_title, self.title_text_area)
                    return True
            QToolTip.hideText()
            return True
        return super().eventFilter(obj, event)

    def _split_existing_entries(self, entries):
        sys_entries = []
        shelf_entries = []
        for entry in entries:
            cleaned = re.sub(r"\s+", "", entry or "")
            digits_only = re.sub(r"\D", "", cleaned)
            if digits_only and digits_only == cleaned:
                sys_entries.append(cleaned)
            else:
                stripped = (entry or "").strip()
                if stripped:
                    shelf_entries.append(stripped)
        return sys_entries, shelf_entries

    def _on_sys_text_changed(self):
        if self._syncing or self._last_edited != "sys":
            return
        self._sync_from_sys()

    def _on_shelf_text_changed(self):
        if self._syncing or self._last_edited != "shelf":
            return
        self._sync_from_shelf()

    def _sync_from_sys(self):
        self._syncing = True
        sys_lines = self._get_lines(self.sys_text_area.toPlainText())
        shelves = self._resolve_shelves_from_sys(sys_lines)
        titles = self._resolve_titles_from_sys(sys_lines)
        self.shelf_text_area.setPlainText("\n".join(shelves))
        self._set_titles(titles)
        self._syncing = False

    def _sync_from_shelf(self):
        self._syncing = True
        shelf_lines = self._get_lines(self.shelf_text_area.toPlainText())
        sys_ids = self._resolve_sys_from_shelves(shelf_lines)
        self.sys_text_area.setPlainText("\n".join(sys_ids))
        titles = self._resolve_titles_from_sys(sys_ids)
        self._set_titles(titles)
        self._syncing = False

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self._full_titles:
            self._refresh_title_display()

    def _get_lines(self, text):
        return text.splitlines()

    def _set_titles(self, titles):
        self._full_titles = titles
        self._refresh_title_display()

    def _refresh_title_display(self):
        metrics = QFontMetrics(self.title_text_area.font())
        width = max(self.title_text_area.viewport().width() - 6, 20)
        self._display_titles = [
            metrics.elidedText(title, Qt.TextElideMode.ElideRight, width) if title else ""
            for title in self._full_titles
        ]
        self.title_text_area.setPlainText("\n".join(self._display_titles))

    def _resolve_shelves_from_sys(self, sys_lines):
        shelves = []
        for line in sys_lines:
            cleaned = re.sub(r"\D", "", line or "")
            if not cleaned or not self.meta_mgr:
                shelves.append("")
                continue
            shelf, _ = self.meta_mgr.get_meta_for_id(cleaned)
            if shelf == "Unknown" and cleaned not in self.meta_mgr.nli_cache:
                self.meta_mgr.fetch_nli_data(cleaned)
                shelf, _ = self.meta_mgr.get_meta_for_id(cleaned)
            shelves.append("" if shelf == "Unknown" else shelf)
        return shelves

    def _resolve_titles_from_sys(self, sys_lines):
        titles = []
        for line in sys_lines:
            cleaned = re.sub(r"\D", "", line or "")
            if not cleaned or not self.meta_mgr:
                titles.append("")
                continue
            _, title = self.meta_mgr.get_meta_for_id(cleaned)
            if not title and cleaned not in self.meta_mgr.nli_cache:
                self.meta_mgr.fetch_nli_data(cleaned)
                _, title = self.meta_mgr.get_meta_for_id(cleaned)
            titles.append(title or "")
        return titles

    def _ensure_shelf_map(self):
        if self._shelf_to_sys is not None:
            return
        self._shelf_to_sys = {}
        if not self.meta_mgr:
            return
        for sys_id, meta in self.meta_mgr.csv_bank.items():
            self._add_shelf_map(meta.get("shelfmark"), sys_id)
        for sys_id, meta in self.meta_mgr.nli_cache.items():
            self._add_shelf_map(meta.get("shelfmark"), sys_id)

    def _add_shelf_map(self, shelf, sys_id):
        norm = self._normalize_shelfmark(shelf)
        if norm and norm not in self._shelf_to_sys:
            self._shelf_to_sys[norm] = sys_id

    def _resolve_sys_from_shelves(self, shelf_lines):
        self._ensure_shelf_map()
        sys_ids = []
        for line in shelf_lines:
            norm = self._normalize_shelfmark(line)
            sys_ids.append(self._shelf_to_sys.get(norm, "") if norm else "")
        return sys_ids

    def _normalize_shelfmark(self, shelf):
        if not shelf:
            return ""
        without_prefix = re.sub(r"^\s*m[\.\s]*s[\.\s]*\.?\s*", "", shelf, flags=re.IGNORECASE)
        cleaned = re.sub(r"[^\w]", "", without_prefix).lower()
        if cleaned.startswith("ms"):
            cleaned = cleaned[2:]
        return cleaned

    def load_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Load", "", "Text (*.txt)")
        if path:
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
            entries = [line for line in content.splitlines() if line.strip()]
            sys_entries, shelf_entries = self._split_existing_entries(entries)
            self._syncing = True
            self.sys_text_area.setPlainText("\n".join(sys_entries))
            self.shelf_text_area.setPlainText("\n".join(shelf_entries))
            self._syncing = False
            if sys_entries and not shelf_entries:
                self._last_edited = "sys"
                self._sync_from_sys()
            elif shelf_entries and not sys_entries:
                self._last_edited = "shelf"
                self._sync_from_shelf()
            elif sys_entries:
                self._set_titles(self._resolve_titles_from_sys(sys_entries))

    def get_entries_text(self):
        entries = []
        seen = set()

        sys_lines = self._get_lines(self.sys_text_area.toPlainText())
        for line in sys_lines:
            cleaned = re.sub(r"\D", "", line or "")
            if cleaned and cleaned not in seen:
                entries.append(cleaned)
                seen.add(cleaned)

        shelf_lines = self._get_lines(self.shelf_text_area.toPlainText())
        for line in shelf_lines:
            stripped = (line or "").strip()
            if stripped and stripped not in seen:
                entries.append(stripped)
                seen.add(stripped)

        return "\n".join(entries)

class ResultDialog(QDialog):
    """Allow browsing a single search result and its surrounding pages."""

    metadata_loaded = pyqtSignal(int, dict)
    thumb_resolved = pyqtSignal(str, object)

    def __init__(self, parent, all_results, current_index, meta_mgr, searcher):
        super().__init__(parent)
        
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
        
        self.current_meta_request = 0

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
        self.lbl_res_count = QLabel(); self.lbl_res_count.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.btn_res_next = QPushButton(tr("Next Result ▶")); self.btn_res_next.clicked.connect(lambda: self.navigate_results(1))
        top_bar.addWidget(self.btn_res_prev); top_bar.addWidget(self.lbl_res_count, 1); top_bar.addWidget(self.btn_res_next)
        main_layout.addLayout(top_bar)
        main_layout.addWidget(QSplitter(Qt.Orientation.Horizontal))
        
        # --- Header ---
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget); header_layout.setContentsMargins(0, 5, 0, 10)
        
        # Left: Meta + Controls
        meta_col = QVBoxLayout(); meta_col.setAlignment(Qt.AlignmentFlag.AlignTop); meta_col.setSpacing(4)
        
        self.lbl_shelf = QLabel(); self.lbl_shelf.setFont(QFont("Arial", 16, QFont.Weight.Bold)); self.lbl_shelf.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.lbl_title = QLabel(); self.lbl_title.setFont(QFont("Arial", 14)); self.lbl_title.setAlignment(Qt.AlignmentFlag.AlignLeft); self.lbl_title.setWordWrap(True); self.lbl_title.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)

        # Controls Row
        info_row = QHBoxLayout()
        self.btn_img = QPushButton(tr("Go to Ktiv")); self.btn_img.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DialogHelpButton)); self.btn_img.clicked.connect(self.open_catalog); self.btn_img.setFixedWidth(100)
        self.lbl_info = QLabel(); self.lbl_info.setStyleSheet("font-size: 11px;"); self.lbl_info.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.lbl_meta_loading = QLabel(tr("Loading...")); self.lbl_meta_loading.setStyleSheet("color: orange; font-size: 11px;"); self.lbl_meta_loading.setVisible(False)
        
        info_row.addWidget(self.btn_img); info_row.addWidget(self.lbl_info); info_row.addWidget(self.lbl_meta_loading); info_row.addStretch()

        # Nav Row (Inside Header)
        nav_row = QHBoxLayout()

        # Arrows logic (Standard: Prev <, Next > regardless of RTL)
        prev_arrow = "<"
        next_arrow = ">"

        btn_pg_prev = QPushButton(prev_arrow); btn_pg_prev.setFixedWidth(30); btn_pg_prev.clicked.connect(lambda: self.load_page(offset=-1))
        self.spin_page = QSpinBox(); self.spin_page.setRange(1, 9999); self.spin_page.setFixedWidth(80); self.spin_page.editingFinished.connect(lambda: self.load_page(target=self.spin_page.value()))
        btn_pg_next = QPushButton(next_arrow); btn_pg_next.setFixedWidth(30); btn_pg_next.clicked.connect(lambda: self.load_page(offset=1))
        self.lbl_total = QLabel("/ ?")
        nav_row.addWidget(QLabel(tr("Image:"))); nav_row.addWidget(btn_pg_prev); nav_row.addWidget(self.spin_page); nav_row.addWidget(self.lbl_total); nav_row.addWidget(btn_pg_next); nav_row.addStretch()

        action_row = QHBoxLayout()
        self.btn_view_transcription = QPushButton(tr("View full transcription"))
        self.btn_view_transcription.clicked.connect(self.open_full_transcription)
        self.btn_search_parallels = QPushButton(tr("Search for parallels"))
        self.btn_search_parallels.clicked.connect(self.search_for_parallels)
        action_row.addWidget(self.btn_view_transcription)
        action_row.addWidget(self.btn_search_parallels)
        action_row.addStretch()

        meta_col.addWidget(self.lbl_shelf); meta_col.addWidget(self.lbl_title); meta_col.addLayout(info_row); meta_col.addLayout(nav_row); meta_col.addLayout(action_row)
        
        # Right: Thumbnail
        self.lbl_thumb = QLabel(tr("No Preview")); self.lbl_thumb.setFixedSize(120, 120); self.lbl_thumb.setAlignment(Qt.AlignmentFlag.AlignCenter); self.lbl_thumb.setStyleSheet("border: 1px solid #7f8c8d;"); self.lbl_thumb.setScaledContents(True)
        
        header_layout.addLayout(meta_col, 1); header_layout.addWidget(self.lbl_thumb)
        main_layout.addWidget(header_widget)
        
        # --- SPLIT VIEW (Manuscript | Source) ---
        self.text_splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # 1. Manuscript View (Left)
        ms_widget = QWidget()
        ms_layout = QVBoxLayout(ms_widget); ms_layout.setContentsMargins(0,0,0,0)
        ms_layout.addWidget(QLabel("<b>" + tr("Manuscript Text") + "</b>"))
        self.text_ms = QTextBrowser(); self.text_ms.setFont(QFont("SBL Hebrew", 16)); self.text_ms.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        ms_layout.addWidget(self.text_ms)
        
        # 2. Source Context View (Right)
        self.src_widget = QWidget() # Container to hide/show easily
        src_layout = QVBoxLayout(self.src_widget); src_layout.setContentsMargins(0,0,0,0)
        src_layout.addWidget(QLabel("<b>" + tr("Match Context (Source)") + "</b>"))
        self.text_src = QTextBrowser(); self.text_src.setFont(QFont("SBL Hebrew", 16)); self.text_src.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        src_layout.addWidget(self.text_src)

        self.text_splitter.addWidget(ms_widget)
        self.text_splitter.addWidget(self.src_widget)
        self.text_splitter.setStretchFactor(0, 2) # Manuscript takes more space by default
        self.text_splitter.setStretchFactor(1, 1)
        
        main_layout.addWidget(self.text_splitter, 1)
        
        # Footer
        btn_close = QPushButton("Close"); btn_close.clicked.connect(self.close); main_layout.addWidget(btn_close)
        self.setLayout(main_layout)
        
    def navigate_results(self, direction):
        new_idx = self.current_result_idx + direction
        if 0 <= new_idx < len(self.all_results):
            self.current_result_idx = new_idx
            self.load_result_by_index(new_idx)

    def open_full_transcription(self):
        parent = self.parent()
        if parent and hasattr(parent, "open_result_in_browse"):
            parent.open_result_in_browse(
                self.data,
                shelfmark=self.lbl_shelf.text(),
                title=self.lbl_title.text(),
                fl_id=self.current_fl_id,
            )
            self.close()

    def search_for_parallels(self):
        parent = self.parent()
        if parent and hasattr(parent, "send_result_to_composition"):
            parent.send_result_to_composition(
                self.data,
                source_text=self.current_page_text,
                title=self.lbl_title.text(),
            )
            self.close()

    def _htmlify(self, text):
        if not text: return ""
        t = text.replace("\n", "<br>")
        t = re.sub(r'\*(.*?)\*', r"<b style='color:red;'>\1</b>", t)
        return f"<div dir='rtl'>{t}</div>"

    def load_result_by_index(self, idx):
        data = self.all_results[idx]
        if not data.get('full_text'):
            data['full_text'] = self.searcher.get_full_text_by_id(data['uid']) or data.get('text', '')
        self.data = data
        
        # Nav UI Updates
        self.lbl_res_count.setText(tr("Result {} of {}").format(idx + 1, len(self.all_results)))
        self.btn_res_prev.setEnabled(idx > 0)
        self.btn_res_next.setEnabled(idx < len(self.all_results) - 1)
        
        # Parse Meta
        ids = self.meta_mgr.parse_full_id_components(data['raw_header'])
        self.current_sys_id = ids['sys_id']
        try: p = int(ids['p_num']) 
        except: p = 1
        
        # --- Prepare Text Content ---
        # 1. Manuscript Text (Apply Pattern!)
        ms_raw = data.get('full_text', '') or data.get('text', '')
        pattern_str = data.get('highlight_pattern') # Get regex pattern
        
        if pattern_str:
            try:
                # Apply Regex to clean full-text to verify highlighting on load
                regex = re.compile(pattern_str, re.IGNORECASE)
                ms_raw = regex.sub(r'*\g<0>*', ms_raw)
            except:
                pass

        self.text_ms.setHtml(self._htmlify(ms_raw))
        
        # 2. Source Context
        src_raw = data.get('source_ctx', '')
        if src_raw:
            self.src_widget.setVisible(True)
            self.text_src.setHtml(self._htmlify(src_raw))
        else:
            self.src_widget.setVisible(False)
        
        # Load Page & Metadata
        self.load_page(target=p)

    def load_page(self, offset=0, target=None):
        if not self.current_sys_id: return
        self.cancel_image_thread()
        
        if target is not None:
            p = target
            page_data = self.searcher.get_browse_page(self.current_sys_id, p_num=p, next_prev=0)
        else:
            page_data = self.searcher.get_browse_page(self.current_sys_id, p_num=self.current_p_num, next_prev=offset)
            
        if not page_data: return

        self.current_p_num = page_data['p_num']
        parsed_new = self.meta_mgr.parse_full_id_components(page_data['full_header'])
        self.current_fl_id = parsed_new['fl_id']
        self.current_full_header = page_data.get('full_header', '')
        self.current_page_text = page_data.get('text', '')
        self.current_page_uid = page_data.get('uid')

        # Update Info Label
        info_html = f"<b>{tr('Sys')}:</b> {self.current_sys_id} | <b>{tr('FL')}:</b> {self.current_fl_id or '?'}"
        self.lbl_info.setText(info_html)
        
        # Update Page Controls
        self.spin_page.blockSignals(True); self.spin_page.setValue(self.current_p_num); self.spin_page.blockSignals(False)
        self.lbl_total.setText(f"/ {page_data['total_pages']}")

        # --- Render Text with Highlights ---
        raw_text = page_data['text']
        
        # Try to re-apply highlighting if we have a regex pattern stored in data
        pattern_str = self.data.get('highlight_pattern')
        
        if pattern_str:
            try:
                # Compile regex again
                regex = re.compile(pattern_str, re.IGNORECASE)
                # Replace matches with *match* notation so htmlify can color it
                # We use a lambda to wrap the found group with stars
                highlighted_text = regex.sub(r'*\g<0>*', raw_text)
                raw_text = highlighted_text
            except:
                pass # If regex fails, just show plain text
        
        self.text_ms.setHtml(self._htmlify(raw_text))

        # Handle Metadata & Image
        self.lbl_meta_loading.setVisible(False)
        self.lbl_title.setText('')
        
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

    def apply_metadata(self, meta):
        # 1. Update Text Labels
        shelf = self.meta_mgr.get_shelfmark_from_header(self.current_full_header) or meta.get('shelfmark', 'Unknown Shelf')
        self.lbl_shelf.setText(shelf)
        self.lbl_title.setText(meta.get('title', ''))
        self.lbl_meta_loading.setVisible(False)

        # 2. Trigger Image Fetch using the FRESH metadata
        # (This meta object now contains 'thumb_url' from the XML 907 $d field)
        self.fetch_image(self.current_sys_id, meta)

    def on_metadata_loaded(self, request_id, meta):
        if request_id != self.current_meta_request:
            return
        self.apply_metadata(meta or {})

    def cancel_image_thread(self):
        img_thread = getattr(self, 'img_thread', None)
        if img_thread and img_thread.isRunning():
            img_thread.cancel()
            img_thread.wait()

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
        
    def start_browse_download(self, sid, thumb_url):
        if sid != self.current_browse_sid:
            return

        logger.debug("Starting browse image download for SID=%s, URL=%s", sid, thumb_url)

        self.browse_thumb_url = thumb_url
        self.cancel_browse_image_thread()

        if not thumb_url:
            self.on_browse_img_failed()
            return

        # Create and start thread
        self.browse_img_thread = ImageLoaderThread(thumb_url)
        self.browse_img_thread.image_loaded.connect(self.on_browse_img_loaded)
        self.browse_img_thread.load_failed.connect(self.on_browse_img_failed)
        self.browse_img_thread.start()

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

        # 2. Stop worker threads safely
        try:
            if getattr(self, 'meta_loader', None) and self.meta_loader.isRunning():
                self.meta_loader.request_cancel()
                self.meta_loader.wait()

            if getattr(self, 'search_thread', None) and self.search_thread.isRunning():
                self.search_thread.requestInterruption()
                self.search_thread.wait(2000)
                if self.search_thread.isRunning():
                    self.search_thread.terminate()
                    self.search_thread.wait()

            if getattr(self, 'comp_thread', None) and self.comp_thread.isRunning():
                self.comp_thread.requestInterruption()
                self.comp_thread.wait(2000)
                if self.comp_thread.isRunning():
                    self.comp_thread.terminate()
                    self.comp_thread.wait()

            if getattr(self, 'group_thread', None) and self.group_thread.isRunning():
                self.group_thread.requestInterruption()
                self.group_thread.wait(2000)
                if self.group_thread.isRunning():
                    self.group_thread.terminate()
                    self.group_thread.wait()
                    
            if getattr(self, 'browse_img_thread', None) and self.browse_img_thread.isRunning():
                self.browse_img_thread.cancel()
                self.browse_img_thread.wait()
                
        finally:
            super().closeEvent(event)

    def open_catalog(self):
        if self.current_sys_id: QDesktopServices.openUrl(QUrl(f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{self.current_sys_id}"))

    def open_viewer(self):
        if self.current_sys_id and self.current_fl_id: QDesktopServices.openUrl(QUrl(f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/viewerpage?vid=MANUSCRIPT&docId=PNX_MANUSCRIPTS{self.current_sys_id}#d=[[PNX_MANUSCRIPTS{self.current_sys_id}-1,FL{self.current_fl_id}]]"))

class GenizahGUI(QMainWindow):
    """Main application window orchestrating search, browsing, and indexing."""
    browse_thumb_resolved = pyqtSignal(str, object)
    
    def __init__(self):
        super().__init__()
        self.comp_col_context = 4
        self.comp_col_ms_context = 5
        self.setWindowTitle(tr(f"Genizah Search Pro V{APP_VERSION}"))
        self.resize(1300, 850)
        log_tls_relaxation_notice()

        self.meta_mgr = None
        self.var_mgr = None
        self.searcher = None
        self.indexer = None
        self.ai_mgr = None
        self.lab_engine = None

        self.last_results = []
        self.last_search_query = ""
        self.result_row_by_sys_id = {}
        self.comp_main = []
        self.comp_appendix = {}
        self.comp_summary = {}
        self.comp_filtered_main = []
        self.comp_filtered_appendix = {}
        self.comp_filtered_summary = {}
        self.comp_raw_items = []
        self.comp_raw_filtered = []
        self.comp_grouped_main = []
        self.comp_grouped_appendix = {}
        self.comp_grouped_summary = {}
        self.comp_sort_mode = "score"
        self.comp_sort_reverse = True
        self.comp_grouped_filtered_main = []
        self.comp_grouped_filtered_appendix = {}
        self.comp_grouped_filtered_summary = {}
        self.comp_has_grouped_results = False
        self.comp_known = []
        self.pending_recursive_search = False
        self.excluded_raw_entries = []
        self.excluded_sys_ids = set()
        self.excluded_shelfmarks = set()
        self.filter_text_content = ""
        self.group_thread = None
        self.is_searching = False
        self.is_comp_running = False
        self.current_browse_sid = None
        self.current_browse_p = None
        self.meta_loader = None
        self.meta_cached_count = 0
        self.meta_to_fetch_count = 0
        self.meta_progress_current = 0
        self.browse_thumb_url = None
        self.browse_img_thread = None
        self.shelfmark_items_by_sid = {}
        self.title_items_by_sid = {}
        self._connectivity_thread = None
        self._connectivity_start_time = 0
        self._last_connectivity_state = None
        self._last_connectivity_ui_state = {"status": "degraded", "details": [tr("Checking connectivity...")]}


        self.init_ui()
        self.init_connectivity_monitor()

        # Step 2: Start heavy initialization in background
        self.status_label.setText(tr("Initializing components... Please wait."))
        self.set_results_loading(True)
        QTimer.singleShot(100, self.start_background_init)

    def start_background_init(self):
        try:
            self.startup_thread = StartupThread()
            self.startup_thread.finished_signal.connect(self.on_startup_finished)
            self.startup_thread.error_signal.connect(lambda e: QMessageBox.critical(self, tr("Fatal Error"), tr("Failed to initialize:\n{}").format(e)))
            self.startup_thread.start()
        except Exception as e:
            QMessageBox.critical(self, tr("Fatal Error"), tr("Failed to start initialization:\n{}").format(e))

    def init_connectivity_monitor(self):
        self.connectivity_timer = QTimer(self)
        self.connectivity_timer.setInterval(60_000)
        self.connectivity_timer.timeout.connect(self.refresh_connectivity_status)
        self.refresh_connectivity_status()
        self.connectivity_timer.start()

    def refresh_connectivity_status(self):
        # Manage the worker thread
        if self._connectivity_thread and self._connectivity_thread.isRunning():
            # If stuck for > 30 seconds, kill and restart
            if time.time() - self._connectivity_start_time > 30:
                logger.warning("Connectivity thread stuck (>30s). Terminating.")
                self._connectivity_thread.terminate()
                self._connectivity_thread.wait()
            else:
                # Still running normally, skip this check cycle
                return

        self._connectivity_start_time = time.time()
        self._connectivity_thread = ConnectivityThread(self.ai_mgr)
        self._connectivity_thread.finished_signal.connect(self._on_connectivity_finished)
        self._connectivity_thread.start()

    def _on_connectivity_finished(self, statuses):
        if "error" in statuses:
            logger.error("Connectivity check error: %s", statuses["error"])
            state = {"status": "degraded", "details": [tr("Check failed")]}
        else:
            logger.debug("Connectivity raw statuses: %r", statuses)
            state = self._summarize_connectivity(statuses)

        self._last_connectivity_ui_state = state
        state_key = (state['status'], tuple(state['details']))

        if state_key != self._last_connectivity_state:
            self._last_connectivity_state = state_key
            readable_details = "; ".join(state['details']) if state['details'] else "All services healthy"
            logger.info("Connectivity state changed to %s (%s)", state['status'], readable_details)

        self._update_connectivity_ui(state)

    def _summarize_connectivity(self, statuses):
        def is_reachable(obj, default=False):
            if isinstance(obj, dict):
                return bool(obj.get("reachable", default))
            return bool(obj) if obj is not None else default

        offline = not is_reachable(statuses.get("network"), default=False)

        degraded = []
        if not is_reachable(statuses.get("nli"), default=True):
            degraded.append(tr("NLI service unavailable"))
        if "ai_provider" in statuses and not is_reachable(statuses.get("ai_provider"), default=True):
            degraded.append(tr("AI provider unavailable"))

        if offline:
            return {"status": "offline", "details": [tr("No internet connection")] + degraded}
        if degraded:
            return {"status": "degraded", "details": degraded}
        return {"status": "online", "details": []}


 
    def _update_connectivity_ui(self, state):
        status = state['status']
        if status == "offline":
            text = tr("Offline")
            color = "#c0392b"
        elif status == "degraded":
            text = tr("Degraded")
            color = "#f39c12"
        else:
            text = tr("Online")
            color = "#27ae60"

        tooltip = "\n".join(state['details']) if state['details'] else tr("All external services responding.")
        self.connectivity_label.setText(text)
        self.connectivity_label.setStyleSheet(f"padding:6px; border-radius:6px; color: white; background-color: {color};")
        self.connectivity_label.setToolTip(tooltip)

    def on_startup_finished(self, meta_mgr, var_mgr, searcher, indexer, ai_mgr):
        try:
            self.meta_mgr = meta_mgr
            self.var_mgr = var_mgr
            self.searcher = searcher
            self.indexer = indexer
            self.ai_mgr = ai_mgr

            # Init Lab Engine (lightweight init)
            self.lab_engine = LabEngine(self.meta_mgr, self.var_mgr)

            os.makedirs(Config.REPORTS_DIR, exist_ok=True)
            self.browse_thumb_resolved.connect(self._on_browse_thumb_resolved)

            # Update Settings Tab with loaded AI config
            if self.ai_mgr:
                self.combo_provider.setCurrentText(self.ai_mgr.provider)
                self.txt_model.setText(self.ai_mgr.model_name)
                self.txt_api_key.setText(self.ai_mgr.api_key)
                self.refresh_connectivity_status()

            # Enable UI interactions
            self.btn_search.setEnabled(True)
            self.btn_ai.setEnabled(True)
            self.btn_comp_run.setEnabled(True)
            self.btn_browse_go.setEnabled(True)
            self.btn_save_ai.setEnabled(True)
            self.btn_build_index.setEnabled(True)
            
            self.status_label.setText(tr("Components loaded. Ready."))
            self.set_results_loading(False)

            db_path = os.path.join(Config.INDEX_DIR, "tantivy_db")
            index_exists = os.path.exists(db_path) and os.listdir(db_path)
            
            if not index_exists:
                msg = tr("Index not found.\nWould you like to build it now?\n(Requires 'Transcriptions.txt' next to this app)")
                reply = QMessageBox.question(self, tr("Index Missing"), msg,
                                             QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
                if reply == QMessageBox.StandardButton.Yes:
                    self.tabs.setCurrentIndex(3) 
                    self.run_indexing()

        except Exception as e:
            QMessageBox.critical(self, tr("Fatal Error"), tr("Failed to finalize initialization:\n{}").format(e))
             
    def init_ui(self):
        if CURRENT_LANG == 'he':
            QApplication.instance().setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        self.tabs = QTabWidget()
        self.search_tab = self.create_search_tab()
        self.composition_tab = self.create_composition_tab()
        self.browse_tab = self.create_browse_tab()
        self.settings_tab = self.create_settings_tab()
        self.tabs.addTab(self.search_tab, tr("Search"))
        self.tabs.addTab(self.composition_tab, tr("Composition Search"))
        self.tabs.addTab(self.browse_tab, tr("Browse Manuscript"))
        self.tabs.addTab(self.settings_tab, tr("Settings & About"))

        # Language Toggle
        lang_btn = QPushButton("English" if CURRENT_LANG == 'he' else "עברית")
        lang_btn.setFlat(True)
        lang_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        lang_btn.clicked.connect(self.toggle_language)
        self.tabs.setCornerWidget(lang_btn, Qt.Corner.TopRightCorner if CURRENT_LANG == 'en' else Qt.Corner.TopLeftCorner)

        self.setCentralWidget(self.tabs)

    def toggle_language(self):
        new_lang = 'en' if CURRENT_LANG == 'he' else 'he'
        save_language(new_lang)
        QMessageBox.information(self, tr("Restart Required"), tr("Please restart the application for the language change to take effect."))

    def create_search_tab(self):
        panel = QWidget(); layout = QVBoxLayout()
        top = QHBoxLayout()
        self.query_input = QLineEdit(); self.query_input.setPlaceholderText(tr("Search terms, title or shelfmark..."))
        self.query_input.returnPressed.connect(self.toggle_search)
        
        self.mode_combo = QComboBox()
        self.mode_combo.addItems([tr("Exact"), tr("Variants (?)"), tr("Extended (??)"), tr("Maximum (???)"), tr("Fuzzy (~)"), tr("Regex"), tr("Title"), tr("Shelfmark")])
        # Tooltips
        self.mode_combo.setItemData(0, tr("Exact match"))
        self.mode_combo.setItemData(1, tr("Basic variants: ד/ר, ה/ח, ו/י/ן etc."))
        self.mode_combo.setItemData(2, tr("Extended variants: Adds more swaps (א/ע, ק/כ etc.)"))
        self.mode_combo.setItemData(3, tr("Maximum variants: Very broad search"))
        self.mode_combo.setItemData(4, tr("Fuzzy search: Levenshtein distance"))
        self.mode_combo.setItemData(5, tr("Regex: Use AI Assistant for complex patterns"))
        self.mode_combo.setItemData(6, tr("Search in Title metadata"))
        self.mode_combo.setItemData(7, tr("Search in Shelfmark metadata"))
        
        self.gap_input = QLineEdit(); self.gap_input.setPlaceholderText(tr("Gap")); self.gap_input.setFixedWidth(50)
        self.gap_input.setToolTip(tr("Maximum word distance (0 = Exact phrase)"))
        
        self.btn_search = QPushButton(tr("Search")); self.btn_search.clicked.connect(self.toggle_search)
        self.btn_search.setStyleSheet("background-color: #27ae60; color: white; font-weight: bold; min-width: 80px;")
        self.btn_search.setEnabled(False)
        
        self.btn_ai = QPushButton(tr("🤖 AI Assistant")); self.btn_ai.setStyleSheet("background-color: #8e44ad; color: white;")
        self.btn_ai.setToolTip(tr("Generate Regex with Gemini AI"))
        self.btn_ai.clicked.connect(self.open_ai)
        self.btn_ai.setEnabled(False)

        # Lab Mode Controls
        self.chk_lab_mode = QCheckBox(tr("Lab Mode"))
        self.chk_lab_mode.toggled.connect(self.toggle_lab_mode_sync)

        self.btn_lab_settings = QPushButton()
        self.btn_lab_settings.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_FileDialogDetailedView))
        self.btn_lab_settings.setToolTip(tr("Lab Settings"))
        self.btn_lab_settings.clicked.connect(self.open_lab_settings)
        self.btn_lab_settings.setFixedWidth(30)

        # Help Button
        btn_help = QPushButton("?")
        btn_help.setFixedWidth(30)
        btn_help.setStyleSheet("background-color: #f39c12; color: white; font-weight: bold; border-radius: 15px;")
        btn_help.clicked.connect(lambda: self.open_help_center(anchor="search"))

        top.addWidget(QLabel(tr("Query:"))); top.addWidget(self.query_input, 2)
        top.addWidget(QLabel(tr("Mode:"))); top.addWidget(self.mode_combo)
        top.addWidget(QLabel(tr("Gap:"))); top.addWidget(self.gap_input)
        top.addWidget(self.btn_search); top.addWidget(self.btn_ai)
        top.addWidget(self.chk_lab_mode); top.addWidget(self.btn_lab_settings)
        top.addWidget(btn_help)
        layout.addLayout(top)
        
        self.search_progress = QProgressBar(); self.search_progress.setVisible(False)
        layout.addWidget(self.search_progress)
        
        self.results_table = QTableWidget(); self.results_table.setColumnCount(6)
        self.results_table.setHorizontalHeaderLabels([tr("System ID"), tr("Shelfmark"), tr("Title"), tr("Snippet"), tr("Img"), tr("Src")])
        self.results_table.setColumnWidth(0, 135) 
        self.results_table.setColumnWidth(1, 175)
        self.results_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self.results_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.results_table.setSortingEnabled(True) # Enable sorting
        self.results_table.doubleClicked.connect(self.show_full_text)
        
        self.results_placeholder = QLabel(tr("Please wait while components load..."))
        self.results_placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.results_placeholder.setWordWrap(True)
        self.results_placeholder.setStyleSheet("font-size: 16px; font-weight: bold; color: #c0392b;")

        self.results_stack = QStackedLayout()
        self.results_stack.addWidget(self.results_placeholder)
        self.results_stack.addWidget(self.results_table)

        results_container = QWidget()
        results_container.setLayout(self.results_stack)
        layout.addWidget(results_container)

        bot = QHBoxLayout()
        self.connectivity_label = QLabel(tr("Checking connectivity..."))
        self.connectivity_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.connectivity_label.setMinimumWidth(150)
        self.connectivity_label.setStyleSheet("padding:6px; border-radius:6px; color: white; background-color: #f39c12;")
        QTimer.singleShot(0, lambda: self._update_connectivity_ui(getattr(self, "_last_connectivity_ui_state", {"status": "online", "details": []})))

        self.status_label = QLabel(tr("Ready."))
        lbl_export = QLabel(tr("Export Results") + ":")
        
        # Separate export buttons
        self.btn_exp_xlsx = QPushButton("XLSX")
        self.btn_exp_xlsx.clicked.connect(lambda: self.export_results('xlsx'))
        self.btn_exp_xlsx.setFixedWidth(50)
        
        self.btn_exp_csv = QPushButton("CSV")
        self.btn_exp_csv.clicked.connect(lambda: self.export_results('csv'))
        self.btn_exp_csv.setFixedWidth(50)
        
        self.btn_exp_txt = QPushButton("TXT")
        self.btn_exp_txt.clicked.connect(lambda: self.export_results('txt'))
        self.btn_exp_txt.setFixedWidth(50)
        
        # Track export buttons for bulk enable/disable
        self.export_buttons = [self.btn_exp_xlsx, self.btn_exp_csv, self.btn_exp_txt]
        for b in self.export_buttons: b.setEnabled(False)

        self.btn_reload_meta = QPushButton()
        self.btn_reload_meta.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_BrowserReload))
        self.btn_reload_meta.setToolTip("Reload shelfmark/title metadata")
        self.btn_reload_meta.clicked.connect(self.reload_metadata)

        self.btn_stop_meta = QPushButton()
        self.btn_stop_meta.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_BrowserStop))
        self.btn_stop_meta.setToolTip("Stop metadata loading")
        self.btn_stop_meta.clicked.connect(self.stop_metadata_loading)
        self.btn_stop_meta.setEnabled(False)

        # Add controls to status row
        bot.addWidget(self.connectivity_label)
        bot.addWidget(self.status_label, 1)
        bot.addWidget(self.btn_reload_meta)
        bot.addWidget(self.btn_stop_meta)

        # Append export controls to the right
        bot.addWidget(QLabel("|"))
        bot.addWidget(lbl_export)
        bot.addWidget(self.btn_exp_xlsx)
        bot.addWidget(self.btn_exp_csv)
        bot.addWidget(self.btn_exp_txt)
        
        layout.addLayout(bot)
        panel.setLayout(layout)
        return panel

    def set_results_loading(self, is_loading: bool):
        """Toggle the search results placeholder while components initialize."""
        if hasattr(self, "results_stack") and hasattr(self, "results_placeholder") and hasattr(self, "results_table"):
            target = self.results_placeholder if is_loading else self.results_table
            self.results_stack.setCurrentWidget(target)

    def create_composition_tab(self):
        panel = QWidget(); layout = QVBoxLayout(); splitter = QSplitter(Qt.Orientation.Vertical)
        
        inp_w = QWidget(); in_l = QVBoxLayout()
        top_row = QHBoxLayout()
        self.comp_title_input = QLineEdit(); self.comp_title_input.setPlaceholderText(tr("Composition Title"))
        top_row.addWidget(QLabel(tr("Title:"))); top_row.addWidget(self.comp_title_input)
        
        # Help Button
        btn_help = QPushButton("?")
        btn_help.setFixedWidth(30)
        btn_help.setStyleSheet("background-color: #f39c12; color: white; font-weight: bold; border-radius: 15px;")
        btn_help.clicked.connect(lambda: self.open_help_center(anchor="composition"))
        top_row.addWidget(btn_help)
        
        in_l.addLayout(top_row)
        self.comp_text_area = QPlainTextEdit(); self.comp_text_area.setPlaceholderText(tr("Paste source text..."))
        if CURRENT_LANG == 'he': self.comp_text_area.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        in_l.addWidget(self.comp_text_area)

        cr = QHBoxLayout()
        btn_load = QPushButton(tr("Load Text File")); btn_load.clicked.connect(self.load_comp_file)

        btn_exclude = QPushButton(tr("Exclude Manuscripts"))
        btn_exclude.clicked.connect(self.open_exclude_dialog)

        btn_filter_text = QPushButton(tr("Filter Text"))
        btn_filter_text.clicked.connect(self.open_filter_dialog)

        self.lbl_exclude_status = QLabel(tr("Excluded: {}").format(0))
        self.lbl_exclude_status.setStyleSheet("color: #8e44ad; font-weight: bold;")

        self.spin_chunk = QSpinBox(); self.spin_chunk.setValue(5); self.spin_chunk.setPrefix(tr("Chunk: "))
        self.spin_chunk.setToolTip(tr("Words per search block (Rec: 5-7)"))
        
        self.spin_freq = QSpinBox(); self.spin_freq.setValue(10); self.spin_freq.setRange(1,1000); self.spin_freq.setPrefix(tr("Max Freq: "))
        self.spin_freq.setToolTip(tr("Ignore phrases appearing > X times (filters common phrases)"))
        
        self.comp_mode_combo = QComboBox(); self.comp_mode_combo.addItems([tr("Exact"), tr("Variants"), tr("Extended"), tr("Maximum"), tr("Fuzzy")])
        self.comp_mode_combo.setItemData(0, tr("Exact match"))
        self.comp_mode_combo.setItemData(1, tr("Basic variants"))
        self.comp_mode_combo.setItemData(2, tr("Extended variants"))
        self.comp_mode_combo.setItemData(3, tr("Maximum variants"))
        self.comp_mode_combo.setItemData(4, tr("Fuzzy search"))

        self.spin_filter = QSpinBox(); self.spin_filter.setValue(5); self.spin_filter.setPrefix(tr("Filter > "))
        self.spin_filter.setToolTip(tr("Move titles appearing > X times to Appendix"))

        self.chk_comp_flat = QCheckBox(tr("Sort by System ID/Shelfmark only"))
        self.chk_comp_flat.setToolTip(tr("Disable Main/Appendix grouping"))
        self.chk_comp_flat.toggled.connect(self.on_comp_display_mode_changed)

        # Lab Mode Controls (Comp Tab)
        self.chk_lab_mode_comp = QCheckBox(tr("Lab Mode"))
        self.chk_lab_mode_comp.toggled.connect(self.toggle_lab_mode_sync)

        self.btn_lab_settings_comp = QPushButton()
        self.btn_lab_settings_comp.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_FileDialogDetailedView))
        self.btn_lab_settings_comp.setToolTip(tr("Lab Settings"))
        self.btn_lab_settings_comp.clicked.connect(self.open_lab_settings)
        self.btn_lab_settings_comp.setFixedWidth(30)


        self.btn_comp_run = QPushButton(tr("Analyze Composition")); self.btn_comp_run.clicked.connect(self.toggle_composition)
        self.btn_comp_run.setStyleSheet("background-color: #2980b9; color: white; font-weight: bold;")
        self.btn_comp_run.setEnabled(False)
        self.btn_comp_recursive = QPushButton(tr("Full Recursive Search")); self.btn_comp_recursive.clicked.connect(self.run_recursive_composition)
        self.btn_comp_recursive.setStyleSheet("background-color: #27ae60; color: white; font-weight: bold;")
        self.btn_comp_recursive.setEnabled(True)

        cr.addWidget(btn_load); cr.addWidget(btn_exclude); cr.addWidget(btn_filter_text)
        cr.addWidget(self.lbl_exclude_status)
        cr.addWidget(self.spin_chunk); cr.addWidget(self.spin_freq)
        cr.addWidget(self.comp_mode_combo); cr.addWidget(self.spin_filter); cr.addWidget(self.chk_comp_flat)
        cr.addWidget(self.chk_lab_mode_comp); cr.addWidget(self.btn_lab_settings_comp)
        cr.addWidget(self.btn_comp_run); cr.addWidget(self.btn_comp_recursive)
        in_l.addLayout(cr)
        self.comp_progress = QProgressBar(); self.comp_progress.setVisible(False)
        in_l.addWidget(self.comp_progress)
        inp_w.setLayout(in_l); splitter.addWidget(inp_w)
        
        res_w = QWidget(); rl = QVBoxLayout()
        self.comp_tree = QTreeWidget(); self.comp_tree.setHeaderLabels([tr("Score"), tr("Shelfmark"), tr("Title"), tr("System ID"), tr("Context"), tr("MS Context")])
        self.comp_tree.itemChanged.connect(self.on_comp_tree_item_changed)
        self.comp_tree.itemExpanded.connect(self.on_comp_tree_item_expanded)
        self.comp_tree.itemCollapsed.connect(self.on_comp_tree_item_collapsed)
        self.comp_tree_updating = False
        self.comp_tree.setStyleSheet(
            "QTreeWidget::indicator { width: 16px; height: 16px; }"
            "QTreeWidget::indicator:unchecked { border: 1px solid #9b9b9b; background: transparent; }"
            "QTreeWidget::indicator:checked { border: 1px solid #9b9b9b; background: rgba(255, 255, 255, 0.35); }"
            "QTreeWidget::indicator:indeterminate { border: 1px solid #9b9b9b; background: rgba(255, 255, 255, 0.18); }"
        )

        # Configure columns width
        header = self.comp_tree.header()
        header.setSectionsClickable(True)
        header.sectionClicked.connect(self.on_comp_header_clicked)
        header.sectionResized.connect(self._refresh_comp_tree_tooltips)
        header.setSortIndicatorShown(True)
        header.setSortIndicator(0, Qt.SortOrder.DescendingOrder)
        header.setSectionResizeMode(self.comp_col_context, QHeaderView.ResizeMode.Interactive)
        header.setSectionResizeMode(self.comp_col_ms_context, QHeaderView.ResizeMode.Interactive)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Interactive) # Shelfmark
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents) # System ID

        self.comp_tree.setColumnWidth(0, 160) # Score - widened

        # Title column (~25 chars)
        title_width = self.comp_tree.fontMetrics().averageCharWidth() * 25
        self.comp_tree.setColumnWidth(2, int(title_width))
        context_width = self.comp_tree.fontMetrics().averageCharWidth() * 35
        self.comp_tree.setColumnWidth(self.comp_col_context, int(context_width))
        self.comp_tree.setColumnWidth(self.comp_col_ms_context, int(context_width))

        self.comp_tree.itemDoubleClicked.connect(self.show_comp_detail)
        rl.addWidget(self.comp_tree)
        
        exp_layout = QHBoxLayout()
        exp_layout.addWidget(QLabel(tr("Save Report")))
        
        self.btn_comp_xlsx = QPushButton("XLSX")
        self.btn_comp_xlsx.clicked.connect(lambda: self.export_comp_report('xlsx'))
        
        self.btn_comp_csv = QPushButton("CSV")
        self.btn_comp_csv.clicked.connect(lambda: self.export_comp_report('csv'))
        
        self.btn_comp_txt = QPushButton("TXT")
        self.btn_comp_txt.clicked.connect(lambda: self.export_comp_report('txt'))
        
        self.comp_export_buttons = [self.btn_comp_xlsx, self.btn_comp_csv, self.btn_comp_txt]
        for b in self.comp_export_buttons:
            b.setEnabled(False) 
            
        exp_layout.addWidget(self.btn_comp_xlsx)
        exp_layout.addWidget(self.btn_comp_csv)
        exp_layout.addWidget(self.btn_comp_txt)
        
        rl.addLayout(exp_layout)
        
        res_w.setLayout(rl); splitter.addWidget(res_w)
        
        layout.addWidget(splitter); panel.setLayout(layout)
        return panel

    def create_browse_tab(self):
        panel = QWidget(); layout = QVBoxLayout()
        
        # --- Top Area ---
        top_container = QWidget(); top_container.setFixedHeight(120)
        top_layout = QHBoxLayout(top_container); top_layout.setContentsMargins(0, 0, 0, 0)
        
        # Left Side
        left_col = QVBoxLayout(); left_col.setSpacing(5); left_col.setAlignment(Qt.AlignmentFlag.AlignTop)
        
        # Row 1: Search
        search_row = QHBoxLayout()
        self.browse_sys_input = QLineEdit(); self.browse_sys_input.setPlaceholderText(tr("Enter System ID..."))
        self.browse_fl_input = QLineEdit(); self.browse_fl_input.setPlaceholderText(tr("Enter FL ID..."))
        self.browse_fl_input.setFixedWidth(140)
        self.btn_browse_go = QPushButton(tr("Go")); self.btn_browse_go.setFixedWidth(50); self.btn_browse_go.clicked.connect(self.browse_load)
        self.btn_browse_go.setEnabled(False)
        self.browse_sys_input.returnPressed.connect(self.browse_load)
        self.browse_fl_input.returnPressed.connect(self.browse_load)
        search_row.addWidget(QLabel(tr("System ID:"))); search_row.addWidget(self.browse_sys_input)
        search_row.addWidget(QLabel(tr("FL:"))); search_row.addWidget(self.browse_fl_input)
        search_row.addWidget(self.btn_browse_go)
        
        # Row 2: Metadata
        self.browse_info_lbl = QLabel(tr("Enter ID to browse."))
        self.browse_info_lbl.setStyleSheet("font-size: 13px;") 
        self.browse_info_lbl.setWordWrap(True)
        self.browse_info_lbl.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        
        # Row 3: Buttons (Catalog | Continuous | Save)
        btn_row = QHBoxLayout()
        
        self.btn_b_catalog = QPushButton(tr("Ktiv"))
        self.btn_b_catalog.setToolTip(tr("Open in Ktiv Website"))
        self.btn_b_catalog.clicked.connect(self.browse_open_catalog)
        self.btn_b_catalog.setEnabled(False)
        
        self.btn_b_all = QPushButton(tr("View All"))
        self.btn_b_all.setToolTip(tr("Show full text continuously (Infinite Scroll)"))
        self.btn_b_all.clicked.connect(self.browse_load_all)
        self.btn_b_all.setEnabled(False)
        self.btn_b_all.setStyleSheet("font-weight: bold; color: #2980b9;")

        self.btn_b_save = QPushButton(tr("Save"))
        self.btn_b_save.setToolTip(tr("Save full manuscript to file"))
        self.btn_b_save.clicked.connect(self.browse_save_full)
        self.btn_b_save.setEnabled(False)

        btn_row.addWidget(self.btn_b_catalog)
        btn_row.addWidget(self.btn_b_all)
        btn_row.addWidget(self.btn_b_save)
        btn_row.addStretch()

        btn_browse_help = QPushButton("?")
        btn_browse_help.setFixedWidth(30)
        btn_browse_help.setStyleSheet("background-color: #f39c12; color: white; font-weight: bold; border-radius: 15px;")
        btn_browse_help.clicked.connect(lambda: self.open_help_center(anchor="browse"))
        btn_row.addWidget(btn_browse_help)

        left_col.addLayout(search_row)
        left_col.addWidget(self.browse_info_lbl)
        left_col.addLayout(btn_row)
        
        # Right Side: Thumbnail
        self.browse_thumb = QLabel(tr("No Preview"))
        self.browse_thumb.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.browse_thumb.setFixedSize(110, 110)
        self.browse_thumb.setStyleSheet("border: 1px solid #bdc3c7; background: #ecf0f1; font-size: 9px;")
        self.browse_thumb.setScaledContents(True)

        top_layout.addLayout(left_col, 1)
        top_layout.addWidget(self.browse_thumb)
        layout.addWidget(top_container)
        
        self.browse_title_lbl = QLabel(); self.browse_title_lbl.setVisible(False)

        # Main Text Browser
        self.browse_text = QTextBrowser(); self.browse_text.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self.browse_text.setFont(QFont("SBL Hebrew", 16))
        layout.addWidget(self.browse_text)
        
        # Navigation Footer
        nav = QHBoxLayout()
        self.btn_b_prev = QPushButton(tr("<< Prev")); self.btn_b_prev.clicked.connect(lambda: self.browse_navigate(-1))
        self.btn_b_next = QPushButton(tr("Next >>")); self.btn_b_next.clicked.connect(lambda: self.browse_navigate(1))
        self.btn_b_prev.setEnabled(False); self.btn_b_next.setEnabled(False)
        self.lbl_page_count = QLabel("0/0")
        nav.addWidget(self.btn_b_prev); nav.addStretch(); nav.addWidget(self.lbl_page_count); nav.addStretch(); nav.addWidget(self.btn_b_next)
        layout.addLayout(nav); panel.setLayout(layout)
        return panel
    
    def browse_load_all(self):
        """Load all pages into the text browser for continuous scrolling."""
        if not self.current_browse_sid: return
        
        self.browse_text.setText(tr("Loading full manuscript..."))
        QApplication.processEvents() # Refresh UI
        
        pages = self.searcher.get_full_manuscript(self.current_browse_sid)
        if not pages:
            QMessageBox.warning(self, tr("Error"), tr("Could not load full text."))
            return

        html_content = []
        for p in pages:
            # Anchor for scrolling
            anchor = f'<a name="page_{p["p_num"]}"></a>'
            
            # Visual Separator
            img_lbl = tr("Image")
            fl_id = p.get('fl_id')
            fl_suffix = f" ({tr('FL')}: {fl_id})" if fl_id else ""
            separator = f"""
            <div style='background-color: #f0f0f0; color: #555; padding: 5px; margin-top: 20px; border-bottom: 2px solid #ccc;'>
                <b>{img_lbl}: {p['p_num']}{fl_suffix}</b>
            </div>
            """
            
            # Content with line breaks preserved
            content = p['text'].replace("\n", "<br>")
            
            html_content.append(anchor + separator + f"<div dir='rtl'>{content}</div>")
        
        full_html = "".join(html_content)
        self.browse_text.setHtml(full_html)
        
        # Disable paging buttons since we are showing everything
        self.btn_b_prev.setEnabled(False)
        self.btn_b_next.setEnabled(False)
        self.lbl_page_count.setText(tr("Continuous View"))
        
        # Scroll to the page we were looking at
        if self.current_browse_p:
            self.browse_text.scrollToAnchor(f"page_{self.current_browse_p}")

    def browse_save_full(self):
        if not self.current_browse_sid: return
        
        # Determine default filename from shelfmark if available
        meta = self.meta_mgr.nli_cache.get(self.current_browse_sid, {})
        shelfmark = meta.get('shelfmark')

        if shelfmark and shelfmark != "Unknown":
            # Logic: Remove "Ms."/"Ms" prefix unless followed only by a number
            # 1. Match Ms prefix
            ms_match = re.match(r'^\s*ms\.?\s*(.*)', shelfmark, re.IGNORECASE)
            if ms_match:
                remainder = ms_match.group(1)
                # If remainder is NOT just digits (e.g. "T-S ...", "Or. ..."), use remainder
                if not re.fullmatch(r'\d+', remainder.strip()):
                    shelfmark = remainder.strip()

            # Sanitize filename: remove illegal chars, preserve dots, convert spaces to underscores
            safe_shelf = re.sub(r'[<>:"/\\|?*]', '', shelfmark)
            safe_shelf = re.sub(r'\s+', '_', safe_shelf).strip('_')
            default_name = f"{safe_shelf}.txt"
        else:
            default_name = f"Manuscript_{self.current_browse_sid}.txt"

        path, _ = QFileDialog.getSaveFileName(self, tr("Save Manuscript"),
                                            os.path.join(Config.REPORTS_DIR, default_name), 
                                            "Text (*.txt)")
        if not path: return
        
        pages = self.searcher.get_full_manuscript(self.current_browse_sid)
        if not pages: return
        
        with open(path, 'w', encoding='utf-8') as f:
            # Header
            f.write(self._get_credit_header())
            f.write(f"System ID: {self.current_browse_sid}\n")
            f.write(f"Shelfmark: {meta.get('shelfmark', 'Unknown')}\n")
            f.write(f"Title: {meta.get('title', 'Unknown')}\n")
            f.write("="*50 + "\n\n")
            
            for p in pages:
                f.write(f"--- Page {p['p_num']} ---\n")
                f.write(p['text'])
                f.write("\n\n")
            lab_config = self._get_lab_config_block()
            if lab_config:
                f.write(lab_config)
        
        QMessageBox.information(self, tr("Saved"), tr("Manuscript saved to:\n{}").format(path))
    
    def create_settings_tab(self):
        panel = QWidget(); layout = QVBoxLayout()

        settings_header = QHBoxLayout()
        settings_header.addStretch()
        btn_help = QPushButton("?")
        btn_help.setFixedWidth(30)
        btn_help.setStyleSheet("background-color: #f39c12; color: white; font-weight: bold; border-radius: 15px;")
        btn_help.clicked.connect(lambda: self.open_help_center(anchor="settings"))
        settings_header.addWidget(btn_help)
        layout.addLayout(settings_header)
        
        gb_data = QGroupBox(tr("Data & Index"))
        dl = QVBoxLayout()
        btn_dl = QPushButton(tr("Download Transcriptions (Zenodo)")); btn_dl.clicked.connect(lambda: QDesktopServices.openUrl(QUrl("https://doi.org/10.5281/zenodo.17734473")))
        dl.addWidget(btn_dl)
        self.btn_build_index = QPushButton(tr("Build / Rebuild Index")); self.btn_build_index.clicked.connect(self.run_indexing)
        self.btn_build_index.setEnabled(False)
        dl.addWidget(self.btn_build_index)
        self.index_progress = QProgressBar(); dl.addWidget(self.index_progress)
        gb_data.setLayout(dl); layout.addWidget(gb_data)
        
        gb_ai = QGroupBox(tr("AI Configuration"))
        al = QVBoxLayout()

        row1 = QHBoxLayout()
        self.combo_provider = QComboBox()
        self.combo_provider.addItems(["Google Gemini", "OpenAI", "Anthropic Claude"])
        self.combo_provider.setCurrentText(self.ai_mgr.provider if self.ai_mgr else "Google Gemini")
        self.combo_provider.currentTextChanged.connect(self._on_provider_changed)

        self.txt_model = QLineEdit(); self.txt_model.setText(self.ai_mgr.model_name if self.ai_mgr else "gemini-1.5-flash")
        self.txt_model.setPlaceholderText(tr("Model:") + " (e.g. gemini-1.5-flash)")

        row1.addWidget(QLabel(tr("Provider:"))); row1.addWidget(self.combo_provider)
        row1.addWidget(QLabel(tr("Model:"))); row1.addWidget(self.txt_model)

        row2 = QHBoxLayout()
        self.txt_api_key = QLineEdit(); self.txt_api_key.setText(self.ai_mgr.api_key if self.ai_mgr else ""); self.txt_api_key.setEchoMode(QLineEdit.EchoMode.Password)
        self.txt_api_key.setPlaceholderText(tr("API Key:"))

        self.btn_save_ai = QPushButton(tr("Save Settings"))
        self.btn_save_ai.clicked.connect(self.save_ai_settings)
        self.btn_save_ai.setEnabled(False)

        row2.addWidget(QLabel(tr("API Key:"))); row2.addWidget(self.txt_api_key)
        row2.addWidget(self.btn_save_ai)

        al.addLayout(row1); al.addLayout(row2)
        gb_ai.setLayout(al); layout.addWidget(gb_ai)
        
        gb_about = QGroupBox(tr("About"))
        abl = QVBoxLayout()
        about_html_en = """
        <style>
            h3 { margin-bottom: 0px; margin-top: 10px; }
            p { margin-top: 5px; margin-bottom: 5px; line-height: 1.4; }
            a { color: #2980b9; text-decoration: none; }
        </style>
        <div style='font-family: Arial; font-size: 13px;'>
            <div style='text-align:center;'>
                <h2 style='margin-bottom:5px;'>Genizah Search Pro {APP_VERSION}</h2>
                <p style='color: #7f8c8d;'>Developed by Hillel Gershuni (<a href='mailto:gershuni@gmail.com'>gershuni@gmail.com</a>)</p>
            </div>
            <hr>

            <h3>Dedicated to the memory of our beloved teacher, Prof. Menachem Kahana z"l</h3>
            
            <h3>Credits</h3>
            <p>This tool was developed with the coding assistance of <b>Gemini 3.0</b> and <b>GPT 5.1</b>. My thanks to Avi Shmidman, Elisha Rosenzweig, Ephraim Meiri, Elazar Gershuni, Itai Kagan and Elnatan Chen for their advice and support.</p>

            <h3>Data Source & Acknowledgments</h3>
            <p>This software is built on the transcription dataset produced by the <b>MiDRASH Project</b>. I am grateful to the project leaders – Daniel Stoekl Ben Ezra, Marina Rustow, Nachum Dershowitz, Avi Shmidman, and Judith Olszowy-Schlanger – and to Tsafra Siew and Yitzchak Gila from the National Library of Israel. Many thanks also to the rest of the project team: Luigi Bambaci, Benjamin Kiessling, Hayim Lapin, Nurit Ezer, Elena Lolli, Berat Kurar Barakat, Sharva Gogawale, Moshe Lavee, Vered Raziel Kretzmer, and Daria Vasyutinsky Shapira.</p>
            <p>Making such a complex and valuable dataset freely available to the public is a significant step for Open Science, and I deeply appreciate their generosity in allowing everyone to access these texts.</p>
            <h3>License</h3> 
            
            <p>The underlying dataset is licensed under the Creative Commons Attribution 4.0 International (<a href='https://creativecommons.org/licenses/by/4.0/'>CC BY 4.0</a>) license</p>

            <h3>Citation</h3>
            <p>If you use these results in your research, please cite the creators of the dataset: Stoekl Ben Ezra, Daniel, Luigi Bambaci, Benjamin Kiessling, Hayim Lapin, Nurit Ezer, Elena Lolli, Marina Rustow, et al. MiDRASH Automatic Transcriptions. Data set. Zenodo, 2025. <a href='https://doi.org/10.5281/zenodo.17734473'>https://doi.org/10.5281/zenodo.17734473</a>. You can also mention you used this program: Genizah Search Pro by Hillel Gershuni.</p>
        </div>
        """
        
        about_txt = tr("ABOUT_HTML") if CURRENT_LANG == 'he' else about_html_en.replace("{APP_VERSION}", APP_VERSION)
        txt_about = QTextBrowser()
        txt_about.setHtml(about_txt)
        txt_about.setOpenExternalLinks(True)
        abl.addWidget(txt_about)

        # Citation Row
        cit_row = QHBoxLayout()
        cit_row.addWidget(QLabel(tr("Citation:")))

        citation_str = "Stoekl Ben Ezra, Daniel, Luigi Bambaci, Benjamin Kiessling, Hayim Lapin, Nurit Ezer, Elena Lolli, Marina Rustow, et al. MiDRASH Automatic Transcriptions. Data set. Zenodo, 2025. https://doi.org/10.5281/zenodo.17734473."

        self.txt_citation = QLineEdit(citation_str)
        self.txt_citation.setReadOnly(True)
        self.txt_citation.setCursorPosition(0)
        cit_row.addWidget(self.txt_citation)

        btn_copy = QPushButton(tr("Copy"))
        btn_copy.setToolTip(tr("Copy Citation"))
        btn_copy.setFixedSize(60, 24) # Small
        btn_copy.clicked.connect(self.copy_citation)
        cit_row.addWidget(btn_copy)

        abl.addLayout(cit_row)

        gb_about.setLayout(abl); layout.addWidget(gb_about)

        panel.setLayout(layout)
        return panel

    def _on_provider_changed(self, text):
        if text == "Google Gemini":
            self.txt_model.setText("gemini-2.0-flash")
        elif text == "OpenAI":
            self.txt_model.setText("gpt-4o")
        elif text == "Anthropic Claude":
            self.txt_model.setText("claude-3-5-sonnet-20240620")

    def save_ai_settings(self):
        if not self.ai_mgr: return
        provider = self.combo_provider.currentText()
        model = self.txt_model.text().strip()
        key = self.txt_api_key.text().strip()
        if not key:
            QMessageBox.warning(self, tr("Missing Key"), tr("Please configure your AI Provider & Key in Settings."))
            return
        self.ai_mgr.save_config(provider, model, key)
        QMessageBox.information(self, tr("Saved"), tr("Saved to {}").format(provider))

    def copy_citation(self):
        citation = "Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). MiDRASH Automatic Transcriptions. Zenodo. https://doi.org/10.5281/zenodo.17734473"
        QApplication.clipboard().setText(citation)
        QMessageBox.information(self, tr("Copied"), tr("Citation copied to clipboard!"))

    # --- HELP TEXTS ---
    def open_help_center(self, anchor=None):
        """Open the bundled Help.html with optional anchor scrolling and fallback content."""
        help_path = Config.HELP_FILE
        dlg = HelpDialog(
            self,
            tr("Genizah Help"),
            source_path=help_path,
            anchor=anchor,
            fallback_html=self._build_help_fallback_html(),
            lang=CURRENT_LANG,
        )
        dlg.exec()

    def get_search_help_text(self):
        if CURRENT_LANG == 'he': return tr("SEARCH_HELP_HTML")
        return """<h3>Search Modes</h3><ul><li><b>Exact:</b> Only finds exact matches.</li><li><b>Variants (?):</b> Basic OCR errors.</li><li><b>Extended (??):</b> More variants.</li><li><b>Maximum (???):</b> Aggressive swapping (Use caution).</li><li><b>Fuzzy (~):</b> Levenshtein distance (1-2 typos).</li><li><b>Regex:</b> Advanced patterns (Use AI mode for help, or consult your preferable AI engine).</li><li><b>Title:</b> Search in composition titles (metadata).</li><li><b>Shelfmark:</b> Search for shelfmarks (metadata).</li></ul><hr><b>Gap:</b> Max distance between words (irrelevant for Title/Shelfmark)."""

    def get_comp_help_text(self):
        if CURRENT_LANG == 'he': return tr("COMP_HELP_HTML")
        return """<h3>Composition Search</h3><p>Finds parallels between a source text and the Genizah.</p><ul><li><b>Chunk:</b> Words per search block (5-7 recommended).</li><li><b>Max Freq:</b> Filter out common phrases.</li><li><b>Filter >:</b> Group results if a title appears frequently (move to Appendix).</li></ul>"""

    def get_browse_help_text(self):
        if CURRENT_LANG == 'he': return tr("BROWSE_HELP_HTML")
        return """<h3>Browse Manuscripts</h3><ul><li><b>System ID:</b> Enter an ID to load a manuscript.</li><li><b>View All:</b> Switch to continuous view of the full text.</li><li><b>Save:</b> Export the manuscript text to a file.</li></ul>"""

    def get_settings_help_text(self):
        if CURRENT_LANG == 'he': return tr("SETTINGS_HELP_HTML")
        return """<h3>Settings & Index</h3><ul><li><b>Build/Rebuild Index:</b> Required on first run or after corpus updates.</li><li><b>AI Settings:</b> Configure provider, model, and key for regex assistance.</li><li><b>About:</b> View version, credits, and citation details.</li></ul>"""

    def _build_help_fallback_html(self):
        sections = [
            ("search", tr("Search Help")),
            self.get_search_help_text(),
            ("composition", tr("Composition Help")),
            self.get_comp_help_text(),
            ("browse", tr("Browse Help")),
            self.get_browse_help_text(),
            ("settings", tr("Settings Help")),
            self.get_settings_help_text(),
        ]
        html_parts = ["<div style='font-family: Arial; font-size: 13px;'>"]
        for i in range(0, len(sections), 2):
            anchor, heading = sections[i]
            body = sections[i + 1]
            html_parts.append(f"<h2 id='{anchor}'>{heading}</h2>")
            html_parts.append(body)
            html_parts.append("<hr>")
        html_parts.append("</div>")
        return "".join(html_parts)

    def _sanitize_filename(self, text, fallback):
        clean = re.sub(r"[^\w\u0590-\u05FF\s-]", "", text or "")
        clean = re.sub(r"\s+", "_", clean).strip("_")
        return clean or fallback

    def _default_report_path(self, hint, fallback):
        filename = self._sanitize_filename(hint, fallback)

        # Lab Mode: save to Lab Dir
        base_dir = Config.REPORTS_DIR
        if getattr(self, 'chk_lab_mode', None) and self.chk_lab_mode.isChecked():
            base_dir = os.path.join(Config.BASE_DIR, "Reports")

        os.makedirs(base_dir, exist_ok=True)
        return os.path.join(base_dir, f"{filename}.txt")

    def _get_credit_header(self):
        english_text = (
            "Generated by Genizah Search Pro\n"
            "Data Source: MiDRASH Automatic Transcriptions (Stoekl Ben Ezra et al., 2025)\n"
            "Dataset available at: https://doi.org/10.5281/zenodo.17734473\n"
            "================================================================================\n"
        )

        final_text = english_text
        if CURRENT_LANG == 'he':
            final_text = tr("REPORT_CREDIT_TXT")
            
        return final_text + "\n"

    def _get_lab_config_block(self):
        if getattr(self, 'chk_lab_mode', None) and self.chk_lab_mode.isChecked() and self.lab_engine:
            settings_dump = json.dumps({
                'custom_variants': self.lab_engine.settings.custom_variants,
                'expansion_budget': self.lab_engine.settings.expansion_budget,
                'slop_window': self.lab_engine.settings.slop_window,
                'rare_word_bonus': self.lab_engine.settings.rare_word_bonus,
                'candidate_limit': self.lab_engine.settings.candidate_limit,
                'minimum_match_pct': self.lab_engine.settings.minimum_match_pct,
                'prefix_mode': self.lab_engine.settings.prefix_mode,
                'prefix_chars': self.lab_engine.settings.prefix_chars,
                'use_standard_variants': self.lab_engine.settings.use_standard_variants,
                'use_custom_variants': self.lab_engine.settings.use_custom_variants,
                'use_double_scan': self.lab_engine.settings.use_double_scan,
            }, indent=2, ensure_ascii=False)
            return f"\n[LAB MODE CONFIGURATION]\n{settings_dump}\n================================================================================\n"
        return ""
    
    # --- LOGIC ---
    def open_ai(self):
        if not self.ai_mgr: return
        if not self.ai_mgr.api_key:
            QMessageBox.warning(self, tr("Missing Key"), tr("Please configure your AI Provider & Key in Settings.")); return
        d = AIDialog(self, self.ai_mgr)
        if d.exec(): self.query_input.setText(d.generated_regex); self.mode_combo.setCurrentIndex(5)

    def toggle_lab_mode_sync(self, checked):
        # Sync the two checkboxes
        self.chk_lab_mode.blockSignals(True)
        self.chk_lab_mode_comp.blockSignals(True)
        self.chk_lab_mode.setChecked(checked)
        self.chk_lab_mode_comp.setChecked(checked)
        self.chk_lab_mode.blockSignals(False)
        self.chk_lab_mode_comp.blockSignals(False)

    def open_lab_settings(self):
        if not self.lab_engine:
            QMessageBox.warning(self, tr("Error"), tr("Lab Engine not initialized."))
            return
        d = LabSettingsDialog(self, self.lab_engine)
        d.exec()

    def toggle_search(self):
        if not self.searcher: return
        if self.is_searching: self.stop_search()
        else: self.start_search()

    def start_search(self):
        query = self.query_input.text().strip()
        if not query: return
        mode_idx = self.mode_combo.currentIndex()
        modes = ['literal', 'variants', 'variants_extended', 'variants_maximum', 'fuzzy', 'Regex', 'Title', 'Shelfmark']
        mode = modes[mode_idx]
        gap = int(self.gap_input.text()) if self.gap_input.text().isdigit() else 0

        self.last_search_query = query

        self.is_searching = True; self.btn_search.setText(tr("Stop")); self.btn_search.setStyleSheet("background-color: #c0392b; color: white;")
        self.search_progress.setRange(0, 100); self.search_progress.setValue(0); self.search_progress.setVisible(True)

        # Stop any previous metadata loading to prevent race conditions
        if self.meta_loader and self.meta_loader.isRunning():
            self.meta_loader.request_cancel()
            self.meta_loader.wait()

        # Clear item references BEFORE clearing the table to avoid accessing deleted items
        self.shelfmark_items_by_sid = {}
        self.title_items_by_sid = {}

        self.results_table.setRowCount(0) 
        for b in self.export_buttons: b.setEnabled(False)
        self.result_row_by_sys_id = {}

        if self.chk_lab_mode.isChecked():
            if not self.lab_engine:
                QMessageBox.warning(self, tr("Error"), tr("Lab Engine not initialized."))
                self.reset_ui()
                return
            self.search_thread = LabSearchThread(self.lab_engine, query, mode, gap)
        else:
            self.search_thread = SearchThread(self.searcher, query, mode, gap)

        self.search_thread.results_signal.connect(self.on_search_finished)
        self.search_thread.progress_signal.connect(lambda c, t: (self.search_progress.setMaximum(t), self.search_progress.setValue(c)))
        self.search_thread.error_signal.connect(self.on_error)
        self.search_thread.start()

    def stop_search(self):
        if self.search_thread.isRunning(): self.search_thread.terminate(); self.search_thread.wait()
        self.reset_ui()

    def reset_ui(self):
        self.is_searching = False; self.btn_search.setText(tr("Search")); self.btn_search.setStyleSheet("background-color: #27ae60; color: white;")
        self.search_progress.setVisible(False)

    def on_error(self, err): self.reset_ui(); QMessageBox.critical(self, tr("Error"), str(err))

    def on_search_finished(self, results):
        self.reset_ui()
        if not results:
            self.status_label.setText(tr("No results found."))
            self.last_results = []
            for b in self.export_buttons: b.setEnabled(False)
            self.results_table.setRowCount(0)
            self.result_row_by_sys_id = {}
            self.shelfmark_items_by_sid = {}
            self.title_items_by_sid = {}
            self.btn_stop_meta.setEnabled(False)
            return

        self.status_label.setText(tr("Found {}. Loading metadata...").format(len(results)))
        self.last_results = results 
        for b in self.export_buttons: b.setEnabled(True)
        self.results_table.setSortingEnabled(False) # Disable sorting during population
        self.results_table.setRowCount(len(results))
        self.result_row_by_sys_id = {}
        self.shelfmark_items_by_sid = {}
        self.title_items_by_sid = {}
        self._res_map_by_sid = {r['display']['id']: r for r in results} # New: map for metadata updates

        ids = []
        for i, res in enumerate(results):
            meta = res['display']
            parsed = self.meta_mgr.parse_full_id_components(res['raw_header'])
            sid = parsed['sys_id'] or meta.get('id')
            # Col 0: System ID (Store full result data here for retrieval after sort)
            item_sid = QTableWidgetItem(sid)
            item_sid.setData(Qt.ItemDataRole.UserRole, res)
            self.results_table.setItem(i, 0, item_sid)

            # Pull immediate metadata from CSV/cache
            shelf, title = self.meta_mgr.get_meta_for_id(sid)

            # Fallback decision: only queue background fetch if CSV/cache didn't provide useful data
            needs_fetch = (shelf == "Unknown" and (not title))

            if needs_fetch:
                ids.append(sid)  
                item_shelf = ShelfmarkTableWidgetItem(tr("Loading..."))
                item_title = QTableWidgetItem(tr("Loading..."))
            else:
                item_shelf = ShelfmarkTableWidgetItem(shelf if shelf else tr("Unknown"))
                item_title = QTableWidgetItem(title if title else "")

            # Col 1: Shelfmark
            self.results_table.setItem(i, 1, item_shelf)
            self.shelfmark_items_by_sid[sid] = item_shelf

            # Col 2: Title
            self.results_table.setItem(i, 2, item_title)
            self.title_items_by_sid[sid] = item_title


            # Col 3: Snippet (Widget)
            lbl = QLabel(f"<div dir='rtl'>{res['snippet']}</div>"); lbl.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
            self.results_table.setCellWidget(i, 3, lbl)

            # Col 4: Img
            self.results_table.setItem(i, 4, QTableWidgetItem(meta['img']))

            # Col 5: Source
            self.results_table.setItem(i, 5, QTableWidgetItem(meta['source']))

            self.result_row_by_sys_id[sid] = i

        self.results_table.setSortingEnabled(True) # Re-enable sorting
        self.start_metadata_loading(ids)

    def start_metadata_loading(self, ids):
        if not ids:
            return
        logger.debug("start_metadata_loading: %d ids, sample=%s", len(ids), ids[:10])

        if self.meta_loader and self.meta_loader.isRunning():
            self.meta_loader.request_cancel()
            self.meta_loader.wait()

        self.meta_cached_count = len([sid for sid in ids if sid and sid in self.meta_mgr.nli_cache])
        self.meta_to_fetch_count = len([sid for sid in ids if sid and sid not in self.meta_mgr.nli_cache])
        self.meta_progress_current = 0

        # Update initial metadata from cache for all rows
        for res in self.last_results:
            sid = res['display']['id']
            shelf = res['display'].get('shelfmark', '')
            title = res['display'].get('title', '')

            # Prefer fresh cache if available
            _, _, cached_shelf, cached_title = self._get_meta_for_header(res.get('raw_header', ''))
            shelf = cached_shelf or shelf
            title = cached_title or title

            if sid in self.shelfmark_items_by_sid and shelf:
                self.shelfmark_items_by_sid[sid].setText(shelf)
                res['display']['shelfmark'] = shelf

            if sid in self.title_items_by_sid and title:
                self.title_items_by_sid[sid].setText(title)
                res['display']['title'] = title

        if self.meta_to_fetch_count == 0:
            self.status_label.setText(tr("Metadata already loaded for {} items.").format(self.meta_cached_count))
            self.btn_stop_meta.setEnabled(False)
            return

        self.meta_loader = ShelfmarkLoaderThread(self.meta_mgr, ids)
        self.meta_loader.progress_signal.connect(self.on_meta_progress)
        self.meta_loader.finished_signal.connect(self.on_meta_finished)
        self.meta_loader.error_signal.connect(lambda err: QMessageBox.critical(self, tr("Metadata Error"), err))
        self.btn_stop_meta.setEnabled(True)
        self.status_label.setText(self._format_metadata_status())
        self.meta_loader.start()

    def on_meta_progress(self, curr, total, sid):
        self.meta_progress_current = curr
        self.status_label.setText(self._format_metadata_status())

        meta = self.meta_mgr.nli_cache.get(sid, {})
        shelf = meta.get('shelfmark', 'Unknown')
        title = meta.get('title', '')

        if sid in self.shelfmark_items_by_sid:
            if sid not in self.shelfmark_items_by_sid or sid not in self.title_items_by_sid:
                logger.debug(
                    "Meta progress sid not in table maps: sid=%s in_shelf=%s in_title=%s",
                    sid,
                    sid in self.shelfmark_items_by_sid,
                    sid in self.title_items_by_sid,
                )

            try:
                self.shelfmark_items_by_sid[sid].setText(shelf)
            except RuntimeError:
                pass # Item deleted

        if sid in self.title_items_by_sid:
            try:
                self.title_items_by_sid[sid].setText(title)
            except RuntimeError:
                pass # Item deleted

        if not hasattr(self, '_res_map_by_sid'):
            self._res_map_by_sid = {r['display']['id']: r for r in self.last_results} # Lazy build or build in search_finished

        if sid in self._res_map_by_sid:
            r = self._res_map_by_sid[sid]
            r['display']['shelfmark'] = shelf
            r['display']['title'] = title
        else:
            # Fallback
            for r in self.last_results:
                 if r['display']['id'] == sid:
                     r['display']['shelfmark'] = shelf
                     r['display']['title'] = title
                     break

    def on_meta_finished(self, cancelled):
        total_loaded = self.meta_cached_count + self.meta_progress_current
        total_expected = self.meta_cached_count + self.meta_to_fetch_count
        if cancelled:
            self.status_label.setText(tr("Metadata load cancelled. Loaded {}/{}.").format(total_loaded, total_expected))
        else:
            self.status_label.setText(tr("Loaded {} items.").format(total_expected))
        self.btn_stop_meta.setEnabled(False)
        self.meta_loader = None

    def reload_metadata(self):
        if not self.last_results:
            return
        ids = [res['display'].get('id', '') for res in self.last_results]
        self.start_metadata_loading(ids)

    def stop_metadata_loading(self):
        if self.meta_loader and self.meta_loader.isRunning():
            self.meta_loader.request_cancel()
            self.status_label.setText(tr("Stopping metadata load..."))
            self.btn_stop_meta.setEnabled(False)

    def _format_metadata_status(self):
        total_expected = self.meta_cached_count + self.meta_to_fetch_count
        total_loaded = self.meta_cached_count + self.meta_progress_current
        progress_part = ""
        if self.meta_to_fetch_count:
            progress_part = f" ({self.meta_progress_current}/{self.meta_to_fetch_count})"
        return tr("Metadata loaded: {}/{}").format(total_loaded, total_expected) + progress_part

    def show_full_text(self):
        row = self.results_table.currentRow()
        if row < 0: return

        # Reconstruct list of results in current visual order
        sorted_results = []
        rows = self.results_table.rowCount()
        for i in range(rows):
            item = self.results_table.item(i, 0)
            if item:
                res = item.data(Qt.ItemDataRole.UserRole)
                if res:
                    sorted_results.append(res)

        # If something went wrong, fall back to last_results but that might be disordered relative to view
        if not sorted_results:
            sorted_results = self.last_results

        ResultDialog(self, sorted_results, row, self.meta_mgr, self.searcher).exec()

    def open_result_in_browse(self, res, shelfmark=None, title=None, fl_id=None):
        sid = None
        if isinstance(res, dict):
            display = res.get('display')
            if isinstance(display, dict):
                sid = display.get('id')
            if not sid:
                sid = res.get('sys_id')
            if not sid:
                raw_header = res.get('raw_header') or res.get('full_header')
                if raw_header:
                    sid, _ = self.meta_mgr.parse_header_smart(raw_header)
        if not sid:
            QMessageBox.warning(self, tr("Error"), tr("No System ID found for this result."))
            return
        if shelfmark:
            info_text = f"<b>{shelfmark}</b>"
            if title:
                info_text += f"<br>{title}"
            self.browse_info_lbl.setText(info_text)
        if fl_id:
            self.browse_fl_input.setText(fl_id)
        else:
            self.browse_fl_input.setText("")
        self.browse_sys_input.setText(sid)
        self.tabs.setCurrentWidget(self.browse_tab)
        self.browse_load()

    def send_result_to_composition(self, res, source_text=None, title=None):
        if not source_text:
            if not res.get('full_text'):
                res['full_text'] = self.searcher.get_full_text_by_id(res['uid']) or res.get('text', '')
            source_text = res.get('full_text') or res.get('text', '')
        self.comp_text_area.setPlainText(source_text)
        if title:
            self.comp_title_input.setText(title)
        sys_id = res['display'].get('id')
        if sys_id:
            entries = list(self.excluded_raw_entries)
            if sys_id not in entries:
                entries.append(sys_id)
                self.set_excluded_entries("\n".join(entries))
        self.tabs.setCurrentWidget(self.composition_tab)
        self.comp_text_area.setFocus()

    def export_results(self, fmt='xlsx'):
        """
        Export results handling specific formats directly.
        fmt: 'xlsx', 'csv', or 'txt'
        """
        def clean_for_excel(text):
            t = str(text).strip()
            if t.startswith(('=', '+', '-', '@')):
                return "'" + t
            return t

        def clean_for_export_text(text):
            t = str(text)
            t = t.replace("<br>", " ").replace("\n", " ")
            t = re.sub(r'<[^>]+>', '', t)
            return t

        base_path = self._default_report_path(self.last_search_query, tr("Search_Results"))
        default_path = os.path.splitext(base_path)[0] + f".{fmt}"
        
        filters = {'xlsx': "Excel (*.xlsx)", 'csv': "CSV (*.csv)", 'txt': "Text (*.txt)"}
        selected_filter = filters.get(fmt, "All Files (*.*)")

        path, _ = QFileDialog.getSaveFileName(self, tr("Export Results"), default_path, selected_filter)
        if not path: return

        # Prepare tabular data
        headers = ["System ID", "Shelfmark", "Title", "Image/Page", "Source", "Snippet"]
        data_rows = []
        for r in self.last_results:
            d = r['display']
            # Use raw_file_hl so highlight markers remain intact
            data_rows.append([
                d.get('id', ''),
                d.get('shelfmark', ''),
                d.get('title', ''),
                str(d.get('img', '')),
                d.get('source', ''),
                r.get('raw_file_hl', '').strip()
            ])

        credit_text = self._get_credit_header()
        lab_config = self._get_lab_config_block()
        lab_config = self._get_lab_config_block()

        # --- XLSX with inline highlighting ---
        if fmt == 'xlsx':
            try:
                wb = openpyxl.Workbook()
                ws = wb.active
                ws.title = "Genizah Results"
                ws.sheet_view.rightToLeft = True

                # Fonts used for rich text snippets
                font_red = InlineFont(color='FF0000', b=True)
                font_normal = InlineFont(color='000000')

                # Helper to write rich text cells
                def write_rich_cell(row, col, text):
                    clean_text = clean_for_export_text(text)
                    if '*' not in clean_text:
                        ws.cell(row=row, column=col, value=clean_for_excel(clean_text))
                        return

                    # Split by asterisk markers
                    if clean_text.startswith(('=', '+', '-', '@')):
                        clean_text = "'" + clean_text
                    parts = clean_text.split('*')
                    rich_string = CellRichText()

                    for i, part in enumerate(parts):
                        if not part: continue
                        # Odd indices represent highlighted text
                        if i % 2 == 1:
                            rich_string.append(TextBlock(font_red, part))
                        else:
                            # Even indices are plain text
                            rich_string.append(TextBlock(font_normal, part))

                    if rich_string:
                        ws.cell(row=row, column=col, value=rich_string)
                    else:
                        ws.cell(row=row, column=col, value="")

                # Credit header
                current_row = 1
                for line in credit_text.split('\n'):
                    if not line.strip(): continue
                    cell = ws.cell(row=current_row, column=1, value=clean_for_excel(line))
                    cell.font = Font(bold=True, color="555555")
                    current_row += 1
                current_row += 1

                # Table headers
                for col_idx, header in enumerate(headers, 1):
                    cell = ws.cell(row=current_row, column=col_idx, value=header)
                    cell.font = Font(bold=True, color="FFFFFF")
                    cell.fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
                current_row += 1

                # Data rows
                for row_data in data_rows:
                    for col_idx, val in enumerate(row_data, 1):
                        val_str = str(val)

                        # Column 6 holds the snippet
                        if col_idx == 6:
                            write_rich_cell(current_row, col_idx, val_str)
                        else:
                            # Strip HTML tags in other columns
                            clean_val = clean_for_export_text(val_str)
                            ws.cell(row=current_row, column=col_idx, value=clean_for_excel(clean_val))

                    current_row += 1

                # Column widths
                ws.column_dimensions['A'].width = 15
                ws.column_dimensions['B'].width = 20
                ws.column_dimensions['C'].width = 40
                ws.column_dimensions['F'].width = 80  # Wider snippet column

                if lab_config:
                    current_row += 1
                    for line in lab_config.split('\n'):
                        if not line.strip():
                            continue
                        ws.cell(row=current_row, column=1, value=clean_for_excel(line))
                        current_row += 1

                wb.save(path)
                QMessageBox.information(self, tr("Saved"), tr("Saved to {}").format(path))

            except Exception as e:
                QMessageBox.critical(self, tr("Error"), f"Failed to save XLSX:\n{str(e)}")

        # --- CSV ---
        elif fmt == 'csv':
            try:
                with open(path, 'w', encoding='utf-8-sig', newline='') as f:
                    f.write(credit_text)
                    writer = csv.writer(f)
                    writer.writerow([])
                    writer.writerow(headers)
                    for row in data_rows:
                        # Strip HTML but keep highlight markers
                        clean_row = [clean_for_export_text(val) for val in row]
                        writer.writerow(clean_row)
                    if lab_config:
                        f.write("\n")
                        f.write(lab_config)
                QMessageBox.information(self, tr("Saved"), tr("Saved to {}").format(path))
            except Exception as e:
                QMessageBox.critical(self, tr("Error"), f"Failed to save CSV:\n{str(e)}")

        # --- TXT ---
        else:
            try:
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(credit_text)
                    for r in self.last_results:
                        f.write(f"=== {r['display']['shelfmark']} | {r['display']['title']} ===\n{r.get('raw_file_hl','')}\n\n")
                    if lab_config:
                        f.write(lab_config)
                QMessageBox.information(self, tr("Saved"), tr("Saved to {}").format(path))
            except Exception as e:
                QMessageBox.critical(self, tr("Error"), f"Failed to save TXT:\n{str(e)}")

    # Composition & Browse
    def open_filter_dialog(self):
        dlg = FilterTextDialog(self, current_text=self.filter_text_content)
        if dlg.exec():
            self.filter_text_content = dlg.get_text()

    def load_comp_file(self):
        path, _ = QFileDialog.getOpenFileName(self, tr("Load"), "", "Text (*.txt)")
        if path:
            with open(path, 'r', encoding='utf-8') as f: self.comp_text_area.setPlainText(f.read())

    def open_exclude_dialog(self):
        dlg = ExcludeDialog(self, existing_entries=self.excluded_raw_entries)
        if dlg.exec():
            self.set_excluded_entries(dlg.get_entries_text())

    def set_excluded_entries(self, entries_text: str):
        entries = [e.strip() for e in entries_text.splitlines() if e.strip()]
        self.excluded_raw_entries = entries

        sys_ids = set()
        shelves = set()
        for e in entries:
            cleaned = re.sub(r"\s+", "", e)
            digits_only = re.sub(r"\D", "", cleaned)
            if digits_only and digits_only == cleaned:
                sys_ids.add(cleaned)
            else:
                norm = self.normalize_shelfmark(e)
                if norm:
                    shelves.add(norm)

        self.excluded_sys_ids = sys_ids
        self.excluded_shelfmarks = shelves
        self.lbl_exclude_status.setText(tr("Excluded: {}").format(len(entries)))

    def normalize_shelfmark(self, shelf: str):
        if not shelf:
            return ""
        without_prefix = re.sub(r"^\s*m[\.\s]*s[\.\s]*\.?\s*", "", shelf, flags=re.IGNORECASE)
        cleaned = re.sub(r"[^\w]", "", without_prefix).lower()
        # Treat optional "ms" prefix as non-significant for comparisons
        if cleaned.startswith("ms"):
            cleaned = cleaned[2:]
        return cleaned

    def _get_meta_for_header(self, raw_header):
        """Return (sys_id, p_num, shelfmark, title) preferring metadata bank for shelfmarks."""
        sys_id, p_num = self.meta_mgr.parse_header_smart(raw_header)

        shelf = "Unknown"
        title = ""

        if sys_id:
            # Use the new unified lookup
            shelf, title = self.meta_mgr.get_meta_for_id(sys_id)

        # Fallback to header parsing if CSV/Cache failed
        if not shelf or shelf == "Unknown":
            shelf = self.meta_mgr.get_shelfmark_from_header(raw_header) or "Unknown"

        return sys_id, p_num, shelf, title

    def _item_matches_exclusion(self, item):
        sys_id, _ = self.meta_mgr.parse_header_smart(item.get('raw_header', ''))
        if sys_id and sys_id in self.excluded_sys_ids:
            return True

        if sys_id and sys_id not in self.meta_mgr.nli_cache:
            self.meta_mgr.fetch_nli_data(sys_id)

        _, _, shelf, _ = self._get_meta_for_header(item.get('raw_header', ''))
        norm_shelf = self.normalize_shelfmark(shelf)
        if norm_shelf and norm_shelf in self.excluded_shelfmarks:
            return True
        return False

    def _apply_manual_exclusions(self, main, appx):
        if not (self.excluded_sys_ids or self.excluded_shelfmarks):
            return main, appx, []

        known = []
        filtered_main = []
        for item in main:
            if self._item_matches_exclusion(item):
                known.append(item)
            else:
                filtered_main.append(item)

        filtered_appx = {}
        for key, items in appx.items():
            kept = []
            for item in items:
                if self._item_matches_exclusion(item):
                    known.append(item)
                else:
                    kept.append(item)
            if kept:
                filtered_appx[key] = kept

        return filtered_main, filtered_appx, known

    def toggle_composition(self):
        if self.is_comp_running:
            if getattr(self, 'group_thread', None) and self.group_thread.isRunning():
                # Disconnect signals to prevent race conditions during stop
                try: self.group_thread.finished_signal.disconnect()
                except: pass
                try: self.group_thread.error_signal.disconnect()
                except: pass

                self.group_thread.requestInterruption()
                self.group_thread.wait()

                QMessageBox.information(self, tr("Stopped"), tr("Grouping stopped. Showing ungrouped results."))
                # Pass explicit empty dicts for other arguments to avoid crashes
                self.display_comp_results(self.comp_raw_items or [], {}, {}, self.comp_raw_filtered or [], {}, {})
            elif getattr(self, 'comp_thread', None) and self.comp_thread.isRunning():
                self.comp_thread.terminate()
                self.comp_thread.wait()
            self.is_comp_running = False
            self.reset_comp_ui()
        else:
            self.run_composition()
        
    def reset_comp_ui(self):
        self.is_comp_running = False; self.btn_comp_run.setText(tr("Analyze Composition"))
        self.btn_comp_run.setStyleSheet("background-color: #2980b9; color: white;")
        self.comp_progress.setVisible(False)

    def run_composition(self, custom_text=None):
        txt = (custom_text if custom_text is not None else self.comp_text_area.toPlainText()).strip()
        if not txt: return
        self.is_comp_running = True; self.btn_comp_run.setText(tr("Stop")); self.btn_comp_run.setStyleSheet("background-color: #c0392b; color: white;")
        self.btn_comp_recursive.setEnabled(False)
        self.comp_progress.setVisible(True); self.comp_progress.setRange(0, 0); self.comp_progress.setValue(0); self.comp_tree.clear()
        self.comp_progress.setFormat(tr("Scanning chunks..."))
        self.comp_raw_items = []
        self.comp_filtered = []
        self.comp_known = []
        for b in self.comp_export_buttons: b.setEnabled(False)
        mode = ['literal', 'variants', 'variants_extended', 'variants_maximum', 'fuzzy'][self.comp_mode_combo.currentIndex()]

        if self.chk_lab_mode_comp.isChecked():
            if not self.lab_engine:
                QMessageBox.warning(self, tr("Error"), tr("Lab Engine not initialized."))
                self.reset_comp_ui()
                return
            self.comp_thread = LabCompositionThread(self.lab_engine, txt, mode, self.spin_chunk.value())
        else:
            self.comp_thread = CompositionThread(
                self.searcher, txt, self.spin_chunk.value(), self.spin_freq.value(), mode,
                filter_text=self.filter_text_content, threshold=self.spin_filter.value()
            )

        self.comp_thread.progress_signal.connect(self.on_comp_progress)
        self.comp_thread.status_signal.connect(lambda s: self.comp_progress.setFormat(s))
        self.comp_thread.scan_finished_signal.connect(self.on_comp_scan_finished)
        self.comp_thread.error_signal.connect(lambda e: QMessageBox.critical(self, tr("Error"), e))
        self.comp_thread.start()

    def on_comp_display_mode_changed(self, _checked):
        if self.is_comp_running:
            return
        if not self._has_comp_results():
            return
        if not self.chk_comp_flat.isChecked() and not self.comp_has_grouped_results:
            if self.comp_raw_items or self.comp_raw_filtered:
                self.start_grouping(self.comp_raw_items or [], self.comp_raw_filtered or [])
                return
        if self.comp_grouped_main or self.comp_grouped_filtered_main:
            self.display_comp_results(
                self.comp_grouped_main,
                self.comp_grouped_appendix,
                self.comp_grouped_summary,
                self.comp_grouped_filtered_main,
                self.comp_grouped_filtered_appendix,
                self.comp_grouped_filtered_summary,
            )

    def run_recursive_composition(self):
        if self.is_comp_running:
            return
        base_text = self.comp_text_area.toPlainText().strip()
        if not base_text:
            return

        if not self._has_comp_results():
            self.pending_recursive_search = True
            self.run_composition()
            return

        selected_uids = self._collect_checked_comp_page_uids()
        if selected_uids:
            source_uids = selected_uids
        else:
            source_uids = self._collect_all_comp_page_uids()
            if not source_uids:
                QMessageBox.information(self, tr("No Results"), tr("No composition matches found."))
                return

        extra_texts = []
        for uid in source_uids:
            full_text = self.searcher.get_full_text_by_id(uid)
            if full_text:
                extra_texts.append(full_text)

        if not extra_texts:
            QMessageBox.information(self, tr("No Data"), tr("Could not load full text for selected results."))
            return

        combined_text = base_text + "\n\n" + "\n\n".join(extra_texts)
        self.run_composition(custom_text=combined_text)

    def on_comp_progress(self, curr, total):
        if total:
            self.comp_progress.setRange(0, total)
        else:
            self.comp_progress.setRange(0, 0)
        self.comp_progress.setValue(curr)

    def on_comp_scan_finished(self, result_obj):
        self.is_comp_running = False
        self.reset_comp_ui()

        if isinstance(result_obj, dict):
            items = result_obj.get('main', [])
            filtered_items = result_obj.get('filtered', [])
        else:
            items = result_obj or []
            filtered_items = []

        # Immediate Grouping by Manuscript
        manuscripts = self.searcher.group_pages_by_manuscript(items)
        filtered_manuscripts = self.searcher.group_pages_by_manuscript(filtered_items)

        self.comp_raw_items = manuscripts
        self.comp_raw_filtered = filtered_manuscripts

        if not manuscripts and not filtered_manuscripts:
            QMessageBox.information(self, tr("No Results"), tr("No composition matches found."))
            self.pending_recursive_search = False
            return

        if self.chk_comp_flat.isChecked():
            self.comp_has_grouped_results = False
            self.comp_grouped_main = manuscripts
            self.comp_grouped_appendix = {}
            self.comp_grouped_summary = {}
            self.comp_grouped_filtered_main = filtered_manuscripts
            self.comp_grouped_filtered_appendix = {}
            self.comp_grouped_filtered_summary = {}
            self.display_comp_results(manuscripts, {}, {}, filtered_manuscripts, {}, {})
            return

        self.start_grouping(manuscripts, filtered_manuscripts)

    def start_grouping(self, items, filtered_items=None):
        self.is_comp_running = True
        self.comp_has_grouped_results = False
        self.btn_comp_run.setText(tr("Stop"))
        self.btn_comp_run.setStyleSheet("background-color: #c0392b; color: white;")
        self.comp_progress.setVisible(True)
        total_items = (len(items) if items else 0) + (len(filtered_items) if filtered_items else 0)
        self.comp_progress.setRange(0, total_items)
        self.comp_progress.setValue(0)
        self.comp_progress.setFormat(tr("Grouping compositions..."))

        self.group_thread = GroupingThread(
            self.searcher, items, self.spin_filter.value(), filtered_items=filtered_items
        )
        self.group_thread.progress_signal.connect(self.on_comp_progress)
        self.group_thread.status_signal.connect(lambda s: self.comp_progress.setFormat(s))
        self.group_thread.finished_signal.connect(self.on_comp_finished)
        self.group_thread.error_signal.connect(self.on_grouping_error)
        self.group_thread.start()

    def on_grouping_error(self, err):
        QMessageBox.critical(self, tr("Grouping Error"), err)
        # Fallback to ungrouped display
        self.comp_has_grouped_results = False
        self.comp_grouped_main = self.comp_raw_items or []
        self.comp_grouped_appendix = {}
        self.comp_grouped_summary = {}
        self.comp_grouped_filtered_main = self.comp_raw_filtered or []
        self.comp_grouped_filtered_appendix = {}
        self.comp_grouped_filtered_summary = {}
        self.display_comp_results(self.comp_raw_items or [], {}, {}, self.comp_raw_filtered or [], {}, {})

    def on_comp_finished(self, main_res, main_appx, main_summ, filt_res, filt_appx, filt_summ):
        self.comp_has_grouped_results = True
        self.comp_grouped_main = main_res or []
        self.comp_grouped_appendix = main_appx or {}
        self.comp_grouped_summary = main_summ or {}
        self.comp_grouped_filtered_main = filt_res or []
        self.comp_grouped_filtered_appendix = filt_appx or {}
        self.comp_grouped_filtered_summary = filt_summ or {}
        self.display_comp_results(main_res, main_appx, main_summ, filt_res, filt_appx, filt_summ)

    def _collect_comp_items(self, main_res, main_appx, filt_res, filt_appx, known):
        all_items = []
        all_items.extend(main_res or [])
        for group_items in (main_appx or {}).values():
            all_items.extend(group_items)
        all_items.extend(filt_res or [])
        for group_items in (filt_appx or {}).values():
            all_items.extend(group_items)
        all_items.extend(known or [])
        return all_items

    def on_comp_header_clicked(self, section):
        if section not in (0, 1, 2, 3):
            return
        mode_map = {0: "score", 1: "shelfmark", 2: "title", 3: "system_id"}
        new_mode = mode_map.get(section, "score")
        if new_mode == self.comp_sort_mode:
            self.comp_sort_reverse = not self.comp_sort_reverse
        else:
            self.comp_sort_mode = new_mode
            self.comp_sort_reverse = new_mode == "score"
        order = Qt.SortOrder.DescendingOrder if self.comp_sort_reverse else Qt.SortOrder.AscendingOrder
        header = self.comp_tree.header()
        header.setSortIndicator(section, order)
        if self.is_comp_running or not self._has_comp_results():
            return
        if not self.chk_comp_flat.isChecked() and not self.comp_has_grouped_results:
            if self.comp_raw_items or self.comp_raw_filtered:
                self.start_grouping(self.comp_raw_items or [], self.comp_raw_filtered or [])
                return
        if self.comp_grouped_main or self.comp_grouped_filtered_main:
            self.display_comp_results(
                self.comp_grouped_main,
                self.comp_grouped_appendix,
                self.comp_grouped_summary,
                self.comp_grouped_filtered_main,
                self.comp_grouped_filtered_appendix,
                self.comp_grouped_filtered_summary,
            )

    def _current_comp_sort_mode(self):
        return self.comp_sort_mode or "score"

    def _get_comp_item_meta(self, item):
        sid = None
        shelf = None
        title = None
        if item.get('type') == 'manuscript' and item.get('sys_id'):
            sid = item['sys_id']
            shelf, title = self.meta_mgr.get_meta_for_id(sid)
            if not shelf or shelf == "Unknown":
                shelf = self.meta_mgr.get_shelfmark_from_header(item.get('raw_header', ''))
        else:
            sid, _, shelf, title = self._get_meta_for_header(item.get('raw_header', ''))
        return sid or "", shelf or "", title or ""

    def _natural_sort_key(self, text):
        normalized = re.sub(r'^\s*ms\.?\s*', '', text or "", flags=re.IGNORECASE)
        return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', normalized)]

    def _comp_sort_key(self, item, mode=None):
        sort_mode = mode or self._current_comp_sort_mode()
        if sort_mode == "score":
            return item.get('score', 0)

        sid, shelf, title = self._get_comp_item_meta(item)

        if sort_mode == "title":
            return title.casefold()
        if sort_mode == "system_id":
            return sid.casefold()

        shelf_key = self._natural_sort_key(shelf or sid)
        sid_key = self._natural_sort_key(sid)
        return (shelf_key, sid_key)

    def _sort_comp_items(self, items, mode=None):
        sort_mode = mode or self._current_comp_sort_mode()
        reverse = self.comp_sort_reverse if sort_mode == self.comp_sort_mode else sort_mode == "score"
        return sorted(items, key=lambda item: self._comp_sort_key(item, sort_mode), reverse=reverse)

    def _build_comp_preview_label(self, text_content):
        if not text_content:
            return QLabel("")
        flat = text_content.replace("\n", " ").replace("\r", " ").strip()
        return HiddenScrollArea(flat)

    def _set_comp_tree_text(self, node, column, text):
        node.setText(column, text)
        self._update_comp_tree_tooltip(node, column)

    def _update_comp_tree_tooltip(self, node, column):
        text = node.text(column)
        if not text:
            node.setToolTip(column, "")
            return

        # Use the tree's font metrics for accuracy
        fm = self.comp_tree.fontMetrics()
        width = self.comp_tree.columnWidth(column)

        # For Column 0, we must subtract space for checkboxes and indentation
        if column == 0:
            # Approx 20px for checkbox + tree indentation level
            level = 0
            temp = node
            while temp.parent():
                level += 1
                temp = temp.parent()
            
            # Subtract indentation (default is 20 per level) and checkbox width
            width -= (self.comp_tree.indentation() * level) + 30

        elided = fm.elidedText(text, Qt.TextElideMode.ElideRight, width - 8)
        
        # If the text is elided (contains '...'), show the full text in tooltip
        node.setToolTip(column, text if elided != text else "")

    def _refresh_comp_tree_tooltips(self):
        root = self.comp_tree.invisibleRootItem()

        def visit(node):
            for col in (0, 1, 2, 3):
                self._update_comp_tree_tooltip(node, col)
            for i in range(node.childCount()):
                visit(node.child(i))

        for i in range(root.childCount()):
            visit(root.child(i))

    def _apply_comp_node_previews(self, node):
        data = node.data(0, Qt.ItemDataRole.UserRole + 1)
        if not data:
            return
            
        src_txt = data.get("source_ctx", "")
        ms_txt = data.get("ms_ctx", "")
        anchor = data.get("anchor")

        src_widget = HiddenScrollArea(src_txt.replace("\n", " "))
        self.comp_tree.setItemWidget(node, self.comp_col_context, src_widget)
        
        ms_widget = HiddenScrollArea(ms_txt.replace("\n", " "), anchor_text=anchor)
        self.comp_tree.setItemWidget(node, self.comp_col_ms_context, ms_widget)

    def _clear_comp_node_previews(self, node):
        if not node.data(0, Qt.ItemDataRole.UserRole + 1):
            return
        self.comp_tree.setItemWidget(node, self.comp_col_context, QLabel(""))
        self.comp_tree.setItemWidget(node, self.comp_col_ms_context, QLabel(""))

    def _set_comp_node_previews(self, node, source_text, ms_text):
        match = re.search(r'\*(.*?)\*', source_text or "")
        anchor = match.group(1) if match else None
        
        node.setData(
            0,
            Qt.ItemDataRole.UserRole + 1,
            {
                "source_ctx": source_text or "",
                "ms_ctx": ms_text or "",
                "anchor": anchor
            },
        )
        self._apply_comp_node_previews(node)

    def display_comp_results(self, main_res, main_appx, main_summ, filt_res, filt_appx, filt_summ):
        self.is_comp_running = False
        self.btn_comp_run.setText(tr("Analyze Composition"))
        self.btn_comp_run.setStyleSheet("background-color: #2980b9; color: white;")
        self.comp_progress.setVisible(False)
        for b in self.comp_export_buttons: b.setEnabled(True)

        # Ensure thread is finished before releasing the object to prevent QThread Destroyed error
        if self.group_thread:
            self.group_thread.wait()
        self.group_thread = None

        self.comp_raw_items = main_res
        self.comp_raw_filtered = filt_res

        # 1. Apply Exclusions to Main
        clean_main, clean_appx, known_main = self._apply_manual_exclusions(main_res, main_appx)

        # 2. Apply Exclusions to Filtered (treating it as its own result set)
        clean_filt, clean_filt_appx, known_filt = self._apply_manual_exclusions(filt_res, filt_appx)

        known = known_main + known_filt

        self.comp_main = clean_main
        self.comp_appendix = clean_appx
        self.comp_summary = main_summ

        self.comp_filtered_main = clean_filt
        self.comp_filtered_appendix = clean_filt_appx
        self.comp_filtered_summary = filt_summ

        self.comp_known = known

        # Ensure metadata is loaded
        all_ids = []
        def collect_ids(item_list):
            for item in item_list:
                # If item is manuscript, we have direct sys_id
                if item.get('type') == 'manuscript' and item.get('sys_id'):
                    all_ids.append(item['sys_id'])
                else:
                    sid, _ = self.meta_mgr.parse_header_smart(item['raw_header'])
                    if sid: all_ids.append(sid)

        collect_ids(clean_main)
        for group_items in clean_appx.values():
            collect_ids(group_items)

        collect_ids(clean_filt)
        for group_items in clean_filt_appx.values():
            collect_ids(group_items)

        collect_ids(known)

        if all_ids:
            self._fetch_metadata_with_dialog(list(set(all_ids)), title="Loading shelfmarks for report...")

        self.comp_tree_updating = True
        self.comp_tree.setUpdatesEnabled(False)
        self.comp_tree.clear()
        
        def make_checkable(node):
            node.setFlags(node.flags() | Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
            node.setCheckState(0, Qt.CheckState.Unchecked)

        def add_manuscript_node(parent, ms_item):
            # Parse meta using the representative header OR just use sys_id
            if ms_item.get('type') == 'manuscript':
                sid = ms_item['sys_id']

                # Use unified metadata retrieval (CSV > Cache)
                shelf, t = self.meta_mgr.get_meta_for_id(sid)

                # Fallback to header parsing if still unknown
                if not shelf or shelf == "Unknown":
                    header_shelf = self.meta_mgr.get_shelfmark_from_header(ms_item.get('raw_header', ''))
                    if header_shelf: shelf = header_shelf

                # Manuscript Node
                ms_node = QTreeWidgetItem(parent)
                self._set_comp_tree_text(ms_node, 0, str(ms_item.get('score', 0)))
                self._set_comp_tree_text(ms_node, 1, shelf or tr("Unknown Shelfmark"))
                self._set_comp_tree_text(ms_node, 2, t or "")
                self._set_comp_tree_text(ms_node, 3, sid)
                make_checkable(ms_node)

                # Store full MS item in UserRole
                ms_node.setData(0, Qt.ItemDataRole.UserRole, ms_item)

                pages = ms_item.get('pages', [])

                # Case A: Single Page -> Display inline
                if len(pages) == 1:
                    p_item = pages[0]
                    _, p_num, _, _ = self._get_meta_for_header(p_item['raw_header'])

                    # Update Shelfmark to include Image info
                    self._set_comp_tree_text(ms_node, 1, f"{shelf or tr('Unknown Shelfmark')} ({tr('Image')} {p_num})")

                    self._set_comp_node_previews(ms_node, p_item.get('source_ctx', ''), p_item.get('text', ''))

                # Case B: Multiple Pages -> Add children
                else:
                    # Update parent with first page image info and snippet
                    if pages:
                        p0 = pages[0]
                        _, p0_num, _, _ = self._get_meta_for_header(p0['raw_header'])
                        self._set_comp_tree_text(ms_node, 1, f"{shelf or tr('Unknown Shelfmark')} ({tr('Image')} {p0_num}...)")
                        self._set_comp_node_previews(ms_node, p0.get('source_ctx', ''), p0.get('text', ''))

                    for p_item in pages:
                        _, p_num, _, _ = self._get_meta_for_header(p_item['raw_header'])

                        page_node = QTreeWidgetItem(ms_node)
                        self._set_comp_tree_text(page_node, 0, str(p_item.get('score', '')))
                        self._set_comp_tree_text(page_node, 1, f"{tr('Image')} {p_num}")
                        self._set_comp_tree_text(page_node, 2, "") # No Title needed for page
                        self._set_comp_tree_text(page_node, 3, "") # No SysID needed for page
                        make_checkable(page_node)

                        page_node.setData(0, Qt.ItemDataRole.UserRole, p_item)

                        self._set_comp_node_previews(page_node, p_item.get('source_ctx', ''), p_item.get('text', ''))

            else:
                # Fallback for raw items (should not happen with new logic, but safe to keep)
                sid, _, shelf, title = self._get_meta_for_header(ms_item.get('raw_header', ''))
                node = QTreeWidgetItem(parent)
                self._set_comp_tree_text(node, 0, str(ms_item.get('score', '')))
                self._set_comp_tree_text(node, 1, shelf)
                self._set_comp_tree_text(node, 2, title)
                self._set_comp_tree_text(node, 3, sid)
                make_checkable(node)
                node.setData(0, Qt.ItemDataRole.UserRole, ms_item)
                self._set_comp_node_previews(node, ms_item.get('source_ctx', ''), ms_item.get('text', ''))

        # ----------------------------------------

        if self.chk_comp_flat.isChecked():
            all_items = self._collect_comp_items(clean_main, clean_appx, clean_filt, clean_filt_appx, known)
            sorted_items = self._sort_comp_items(all_items)
            root = QTreeWidgetItem(self.comp_tree, [tr("All Results ({})").format(len(sorted_items))])
            root.setExpanded(True)
            make_checkable(root)
            for item in sorted_items:
                add_manuscript_node(root, item)
        else:
            # 1. Main Results
            root = QTreeWidgetItem(self.comp_tree, [tr("Main ({})").format(len(clean_main))]); root.setExpanded(True)
            make_checkable(root)
            for item in self._sort_comp_items(clean_main):
                add_manuscript_node(root, item)

            # 2. Appendix Results
            if clean_appx:
                root_a = QTreeWidgetItem(self.comp_tree, [tr("Appendix ({})").format(len(clean_appx))])
                make_checkable(root_a)
                for g, items in sorted(clean_appx.items(), key=lambda x: len(x[1]), reverse=True):
                    gn = QTreeWidgetItem(root_a, [f"{g} ({len(items)})"])
                    make_checkable(gn)
                    for item in self._sort_comp_items(items):
                        add_manuscript_node(gn, item)

            # 3. Filtered by Text (New Category with Sub-Grouping)
            total_filtered = len(clean_filt) + sum(len(v) for v in clean_filt_appx.values())
            if total_filtered > 0:
                root_f = QTreeWidgetItem(self.comp_tree, [tr("Filtered by Text ({})").format(total_filtered)])
                make_checkable(root_f)

                # 3a. Filtered Main
                if clean_filt:
                    f_main_node = QTreeWidgetItem(root_f, [tr("Filtered Main ({})").format(len(clean_filt))])
                    f_main_node.setExpanded(True)
                    make_checkable(f_main_node)
                    for item in self._sort_comp_items(clean_filt):
                        add_manuscript_node(f_main_node, item)

                # 3b. Filtered Appendix
                if clean_filt_appx:
                    f_appx_node = QTreeWidgetItem(root_f, [tr("Filtered Appendix ({})").format(sum(len(v) for v in clean_filt_appx.values()))])
                    make_checkable(f_appx_node)
                    for g, items in sorted(clean_filt_appx.items(), key=lambda x: len(x[1]), reverse=True):
                        gn = QTreeWidgetItem(f_appx_node, [f"{g} ({len(items)})"])
                        make_checkable(gn)
                        for item in self._sort_comp_items(items):
                            add_manuscript_node(gn, item)

            # 4. Known / Excluded Results
            if known:
                root_k = QTreeWidgetItem(self.comp_tree, [tr("Known Manuscripts ({})").format(len(known))])
                make_checkable(root_k)
                for item in self._sort_comp_items(known):
                    add_manuscript_node(root_k, item)

        self.comp_tree.setUpdatesEnabled(True)
        self.comp_tree_updating = False
        self._update_recursive_button_state()
        if self.pending_recursive_search:
            self.pending_recursive_search = False
            self.run_recursive_composition()

    def on_comp_tree_item_changed(self, item, column):
        if self.comp_tree_updating or column != 0:
            return

        self.comp_tree_updating = True
        state = item.checkState(0)
        if item.childCount() > 0 and state in (Qt.CheckState.Checked, Qt.CheckState.Unchecked):
            for i in range(item.childCount()):
                item.child(i).setCheckState(0, state)

        self._sync_parent_check_state(item)
        self.comp_tree_updating = False
        self._update_recursive_button_state()

    def on_comp_tree_item_expanded(self, item):
        if item.childCount() > 0:
            self._clear_comp_node_previews(item)

    def on_comp_tree_item_collapsed(self, item):
        if item.childCount() > 0:
            self._apply_comp_node_previews(item)

    def _sync_parent_check_state(self, item):
        parent = item.parent()
        if not parent:
            return

        states = []
        for i in range(parent.childCount()):
            states.append(parent.child(i).checkState(0))

        if all(s == Qt.CheckState.Checked for s in states):
            parent.setCheckState(0, Qt.CheckState.Checked)
        elif all(s == Qt.CheckState.Unchecked for s in states):
            parent.setCheckState(0, Qt.CheckState.Unchecked)
        else:
            parent.setCheckState(0, Qt.CheckState.PartiallyChecked)

        self._sync_parent_check_state(parent)

    def _collect_checked_comp_page_uids(self):
        uids = set()

        def visit(node):
            data = node.data(0, Qt.ItemDataRole.UserRole)
            if data:
                if data.get('type') == 'manuscript':
                    if node.childCount() > 0:
                        for i in range(node.childCount()):
                            visit(node.child(i))
                    else:
                        if node.checkState(0) == Qt.CheckState.Checked:
                            pages = data.get('pages', [])
                            if pages and pages[0].get('uid'):
                                uids.add(pages[0]['uid'])
                else:
                    if node.checkState(0) == Qt.CheckState.Checked:
                        uid = data.get('uid')
                        if uid:
                            uids.add(uid)
            else:
                for i in range(node.childCount()):
                    visit(node.child(i))

        root = self.comp_tree.invisibleRootItem()
        for i in range(root.childCount()):
            visit(root.child(i))

        return sorted(uids)

    def _collect_all_comp_page_uids(self):
        uids = set()

        def add_from_item(item):
            if item.get('type') == 'manuscript':
                for page in item.get('pages', []):
                    uid = page.get('uid')
                    if uid:
                        uids.add(uid)
            else:
                uid = item.get('uid')
                if uid:
                    uids.add(uid)

        for item in self.comp_main:
            add_from_item(item)
        for group_items in self.comp_appendix.values():
            for item in group_items:
                add_from_item(item)
        for item in self.comp_filtered_main:
            add_from_item(item)
        for group_items in self.comp_filtered_appendix.values():
            for item in group_items:
                add_from_item(item)
        for item in self.comp_known:
            add_from_item(item)

        return sorted(uids)

    def _update_recursive_button_state(self):
        if self.is_comp_running:
            self.btn_comp_recursive.setEnabled(False)
            return
        checked = self._collect_checked_comp_page_uids()
        if checked:
            self.btn_comp_recursive.setText(tr("Recursive Search in Results"))
        else:
            self.btn_comp_recursive.setText(tr("Full Recursive Search"))
        self.btn_comp_recursive.setEnabled(True)

    def _has_comp_results(self):
        if self.comp_main or self.comp_appendix or self.comp_known:
            return True
        if self.comp_filtered_main or self.comp_filtered_appendix:
            return True
        return False

    def show_comp_detail(self, item, col):
        # 1. Validate Click
        data = item.data(0, Qt.ItemDataRole.UserRole)
        if not data: return # It's a structural node, ignore
        
        # 2. Flatten the Tree to create a navigation list (PAGES ONLY)
        flat_list = []
        clicked_index = -1
        
        # If user clicked a Manuscript Node (top level), check if it's single page or multi
        target_item = item
        if data.get('type') == 'manuscript':
            if item.childCount() > 0:
                # Multi-page: Auto-select first child
                target_item = item.child(0)
            else:
                # Single-page: The manuscript node IS the target
                pass

        # Helper to process a page node or a single-page manuscript
        def process_page_data(node_data, node_ref):
            # If it's a manuscript node (single page), extract the single page data
            if node_data.get('type') == 'manuscript':
                pages = node_data.get('pages', [])
                if len(pages) == 1:
                    node_data = pages[0]
                else:
                    return # Should not happen for leaf traversal

            sid, p, shelf, title = self._get_meta_for_header(node_data['raw_header'])

            ready_data = {
                'uid': node_data['uid'],
                'raw_header': node_data['raw_header'],
                'text': node_data['text'], # Snippet
                'full_text': None, # Will be fetched by Dialog on load
                'source_ctx': node_data.get('source_ctx', ''),
                'highlight_pattern': node_data.get('highlight_pattern'),
                'display': {
                    'shelfmark': shelf,
                    'title': title,
                    'img': p,
                    'source': node_data.get('src_lbl', 'Source')
                }
            }
            flat_list.append(ready_data)

            if node_ref is target_item:
                nonlocal clicked_index
                clicked_index = len(flat_list) - 1

        # Traverse Tree Logic for Manuscript Grouping
        root = self.comp_tree.invisibleRootItem()
        for i in range(root.childCount()):
            category_node = root.child(i) # "Main", "Appendix", etc.

            # Recurse into category
            for j in range(category_node.childCount()):
                sub_node = category_node.child(j)

                # sub_node is either a Manuscript (Main) or a Group (Appendix)
                # Check based on child count or data
                # Appendix groups act as folders containing Manuscripts
                if sub_node.childCount() > 0:
                    # Could be Appendix Group OR Manuscript
                    # Check if children are Manuscripts or Pages?
                    # Manuscript nodes hold "type": "manuscript"
                    d = sub_node.data(0, Qt.ItemDataRole.UserRole)
                    if d and d.get('type') == 'manuscript':
                        # It is a Manuscript with multiple pages
                        for k in range(sub_node.childCount()):
                            page_node = sub_node.child(k)
                            process_page_data(page_node.data(0, Qt.ItemDataRole.UserRole), page_node)
                    else:
                        # It is an Appendix Group
                        for k in range(sub_node.childCount()):
                            ms_node = sub_node.child(k)
                            # Check if multi-page or single-page
                            if ms_node.childCount() > 0:
                                for m in range(ms_node.childCount()):
                                    page_node = ms_node.child(m)
                                    process_page_data(page_node.data(0, Qt.ItemDataRole.UserRole), page_node)
                            else:
                                # Single page manuscript in Appendix
                                process_page_data(ms_node.data(0, Qt.ItemDataRole.UserRole), ms_node)
                else:
                    # Leaf Manuscript (Single Page) in Main
                    d = sub_node.data(0, Qt.ItemDataRole.UserRole)
                    if d and d.get('type') == 'manuscript':
                        process_page_data(d, sub_node)

        if clicked_index == -1: return

        # 3. Open Dialog with List
        ResultDialog(self, flat_list, clicked_index, self.meta_mgr, self.searcher).exec()

    def _refresh_comp_tree_metadata(self):

        def update_node(node):
            node_data = node.data(0, Qt.ItemDataRole.UserRole)
            if not node_data:
                return

            sys_id, _, shelf, title = self._get_meta_for_header(node_data.get('raw_header', ''))

            node.setText(1, shelf)
            node.setText(2, title)
            node.setText(3, sys_id or '')

        root = self.comp_tree.invisibleRootItem()
        for i in range(root.childCount()):
            group = root.child(i)
            for j in range(group.childCount()):
                child = group.child(j)
                if child.childCount() > 0:
                    for k in range(child.childCount()):
                        update_node(child.child(k))
                else:
                    update_node(child)

    def export_comp_report(self, fmt='xlsx'):
        # 1. Collect composition results
        all_filtered = self.comp_filtered_main[:]
        for v in self.comp_filtered_appendix.values():
            all_filtered.extend(v)

        if not (self.comp_main or self.comp_appendix or self.comp_known or all_filtered):
            QMessageBox.warning(self, tr("Save"), tr("No composition data to export."))
            return

        # 2. Load any missing metadata
        all_ids = []
        def collect_ids(item_list):
            for item in item_list:
                if item.get('type') == 'manuscript' and item.get('sys_id'):
                    all_ids.append(item['sys_id'])
                else:
                    sid, _ = self.meta_mgr.parse_header_smart(item['raw_header'])
                    if sid: all_ids.append(sid)

        collect_ids(self.comp_main)
        for group_items in self.comp_appendix.values(): collect_ids(group_items)
        collect_ids(self.comp_known)
        collect_ids(all_filtered)

        cancelled = self._fetch_metadata_with_dialog(list(set(all_ids)), title=tr("Fetching metadata before export..."))
        if cancelled: return

        # 3. Choose export path
        comp_title = self.comp_title_input.text().strip() or tr("Untitled Composition")
        base_path = self._default_report_path(comp_title, tr("Composition_Report"))
        default_path = os.path.splitext(base_path)[0] + f".{fmt}"
        
        filters = {'xlsx': "Excel (*.xlsx)", 'csv': "CSV (*.csv)", 'txt': "Text (*.txt)"}
        selected_filter = filters.get(fmt, "All Files (*.*)")

        path, _ = QFileDialog.getSaveFileName(self, tr("Save Report"), default_path, selected_filter)
        if not path: return

        credit_text = self._get_credit_header()

        def clean_for_excel(text):
            t = str(text).strip()
            if t.startswith(('=', '+', '-', '@')): return "'" + t
            return t

        def clean_for_export_text(text):
            t = str(text)
            t = t.replace("<br>", " ").replace("\n", " ")
            t = re.sub(r'<[^>]+>', '', t)
            return t

        # ==========================================
        #  XLSX & CSV Logic
        # ==========================================
        if fmt in ['xlsx', 'csv']:
            table_rows = []
            def add_rows(items, category, group_name=""):
                # We iterate MANUSCRIPTS here, but the rows should be PAGES
                for ms_item in items:
                    # Resolve MS Metadata
                    if ms_item.get('type') == 'manuscript':
                        sid = ms_item['sys_id']

                        # Use unified retrieval
                        shelf, title = self.meta_mgr.get_meta_for_id(sid)
                        if not shelf or shelf == "Unknown":
                             shelf = self.meta_mgr.get_shelfmark_from_header(ms_item.get('raw_header', ''))

                        ms_score = ms_item.get('score', 0)

                        # Iterate Pages
                        for page in ms_item.get('pages', []):
                             _, p_num, _, _ = self._get_meta_for_header(page['raw_header'])
                             table_rows.append([
                                category,
                                group_name,
                                sid or "",
                                shelf or "",
                                title or "",
                                str(p_num or ""),
                                f"{ms_score} (P:{page.get('score',0)})", # Show MS Score (Page Score)
                                clean_for_export_text(page.get('source_ctx', '') or '').strip(),
                                clean_for_export_text(page.get('text', '') or '').strip()
                             ])
                    else:
                        # Fallback for pages
                        sid, p_num, shelf, title = self._get_meta_for_header(ms_item.get('raw_header', ''))
                        table_rows.append([
                            category,
                            group_name,
                            sid or "",
                            shelf or "",
                            title or "",
                            str(p_num or ""),
                            str(ms_item.get('score', 0)),
                            clean_for_export_text(ms_item.get('source_ctx', '') or '').strip(),
                            clean_for_export_text(ms_item.get('text', '') or '').strip()
                        ])

            if self.chk_comp_flat.isChecked():
                flat_items = self._sort_comp_items(
                    self._collect_comp_items(
                        self.comp_main,
                        self.comp_appendix,
                        self.comp_filtered_main,
                        self.comp_filtered_appendix,
                        self.comp_known,
                    )
                )
                add_rows(flat_items, tr("All Results"))
            else:
                add_rows(self.comp_main, "Main Manuscripts")
                for sig, items in sorted(self.comp_appendix.items(), key=lambda x: len(x[1]), reverse=True):
                    add_rows(items, "Appendix", sig)
                add_rows(self.comp_filtered_main, "Filtered Main")
                for sig, items in sorted(self.comp_filtered_appendix.items(), key=lambda x: len(x[1]), reverse=True):
                    add_rows(items, "Filtered Appendix", sig)
                add_rows(self.comp_known, "Known Manuscripts")

            # --- XLSX (Rich Text) ---
            if fmt == 'xlsx':
                try:
                    wb = openpyxl.Workbook()
                    ws = wb.active
                    ws.title = "Composition Report"
                    ws.sheet_view.rightToLeft = True

                    # Fonts for highlighted snippets
                    font_red = InlineFont(color='FF0000', b=True)
                    font_normal = InlineFont(color='000000')

                    def write_rich_cell(row, col, text):
                        clean_text = clean_for_export_text(text)
                        if '*' not in clean_text:
                            ws.cell(row=row, column=col, value=clean_for_excel(clean_text))
                            return
                        if clean_text.startswith(('=', '+', '-', '@')):
                            clean_text = "'" + clean_text
                        parts = clean_text.split('*')
                        rich_string = CellRichText()
                        for i, part in enumerate(parts):
                            if not part: continue
                            if i % 2 == 1:
                                rich_string.append(TextBlock(font_red, part))
                            else:
                                rich_string.append(TextBlock(font_normal, part))
                        if rich_string:
                            ws.cell(row=row, column=col, value=rich_string)
                        else:
                            ws.cell(row=row, column=col, value="")

                    # Credit block
                    curr_row = 1
                    for line in credit_text.split('\n'):
                        if not line.strip(): continue
                        c = ws.cell(row=curr_row, column=1, value=clean_for_excel(line))
                        c.font = Font(bold=True, color="555555")
                        curr_row += 1
                    curr_row += 1

                    # Table headers
                    headers = ["Category", "Group", "System ID", "Shelfmark", "Title", "Image", "Score (MS/Page)", "Source Context", "Manuscript Text"]
                    for idx, h in enumerate(headers, 1):
                        c = ws.cell(row=curr_row, column=idx, value=h)
                        c.font = Font(bold=True, color="FFFFFF")
                        c.fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
                    curr_row += 1

                    # Data rows
                    for row_data in table_rows:
                        for idx, val in enumerate(row_data, 1):
                            val_str = str(val)

                            # Columns 8 and 9 hold highlighted text
                            if idx in [8, 9]:
                                write_rich_cell(curr_row, idx, val_str)
                            else:
                                clean_val = re.sub(r'<[^>]+>', '', val_str)
                                safe_val = clean_for_excel(clean_val)
                                ws.cell(row=curr_row, column=idx, value=safe_val)
                        curr_row += 1

                    # Column sizing
                    ws.column_dimensions['A'].width = 20
                    ws.column_dimensions['D'].width = 20
                    ws.column_dimensions['E'].width = 30
                    ws.column_dimensions['H'].width = 50
                    ws.column_dimensions['I'].width = 60

                    if lab_config:
                        curr_row += 1
                        for line in lab_config.split('\n'):
                            if not line.strip():
                                continue
                            ws.cell(row=curr_row, column=1, value=clean_for_excel(line))
                            curr_row += 1

                    wb.save(path)
                    QMessageBox.information(self, tr("Saved"), tr("Saved to {}").format(path))
                except Exception as e:
                    QMessageBox.critical(self, tr("Error"), f"Failed to save XLSX:\n{e}")

            # --- CSV ---
            elif fmt == 'csv':
                try:
                    headers = ["Category", "Group", "System ID", "Shelfmark", "Title", "Image", "Score", "Source Context", "Manuscript Text"]
                    with open(path, 'w', encoding='utf-8-sig', newline='') as f:
                        f.write(credit_text)
                        writer = csv.writer(f)
                        writer.writerow([])
                        writer.writerow(headers)
                        for row in table_rows:
                            clean_row = [clean_for_export_text(val) for val in row]
                            writer.writerow(clean_row)
                        if lab_config:
                            f.write("\n")
                            f.write(lab_config)
                    QMessageBox.information(self, tr("Saved"), tr("Saved to {}").format(path))
                except Exception as e:
                    QMessageBox.critical(self, tr("Error"), f"Failed to save CSV:\n{e}")

        # --- TXT ---
        else:
            try:
                sep = "=" * 80

                # Count Manuscripts
                appendix_count = sum(len(v) for v in self.comp_appendix.values())
                filtered_total = len(self.comp_filtered_main) + sum(len(v) for v in self.comp_filtered_appendix.values())
                known_count = len(self.comp_known)
                total_count = len(self.comp_main) + appendix_count + known_count + filtered_total

                def _fmt_ms_entry(ms_item):
                    # MS Header
                    if ms_item.get('type') == 'manuscript':
                        sid = ms_item['sys_id']

                        shelf, title = self.meta_mgr.get_meta_for_id(sid)
                        if not shelf or shelf == "Unknown":
                            shelf = self.meta_mgr.get_shelfmark_from_header(ms_item.get('raw_header', ''))

                        ms_block = [sep, f"MANUSCRIPT: {shelf} | {title} (ID: {sid}) | Total Score: {ms_item.get('score', 0)}", sep]

                        # Iterate Pages
                        for page in ms_item.get('pages', []):
                             _, p_num, _, _ = self._get_meta_for_header(page['raw_header'])
                             ms_block.append(f"\n--- Page {p_num} (Score: {page.get('score',0)}) ---")
                             ms_block.append(tr("Source Context") + ":\n" + (page.get('source_ctx', '') or "").strip())
                             ms_block.append(tr("Manuscript") + ":\n" + (page.get('text', '') or "").strip())

                        return ms_block
                    else:
                        return self._fmt_item_legacy(ms_item) # Fallback

                def _append_group_summ(target, appx_data, summary_data, label):
                    target.extend([sep, label, sep])
                    if appx_data:
                        for sig, items in sorted(appx_data.items(), key=lambda x: len(x[1]), reverse=True):
                            # items are now Manuscripts
                            # We can list shelfmarks
                            shelfmarks = []
                            for ms in items:
                                if ms.get('type') == 'manuscript':
                                    s, _ = self.meta_mgr.get_meta_for_id(ms['sys_id'])
                                    shelfmarks.append(s if s and s != "Unknown" else ms['sys_id'])
                                else:
                                    shelfmarks.append("Unknown")

                            target.append(f"{sig} ({len(items)}): {', '.join(shelfmarks)}")
                    else:
                        target.append(tr("No items."))

                if self.chk_comp_flat.isChecked():
                    summary_lines = [
                        sep, tr("COMPOSITION REPORT SUMMARY"), sep,
                        f"Title: {comp_title}",
                        f"{tr('Total Manuscripts Found')}: {total_count}"
                    ]

                    detail_lines = [sep, tr("ALL RESULTS"), sep]
                    flat_items = self._sort_comp_items(
                        self._collect_comp_items(
                            self.comp_main,
                            self.comp_appendix,
                            self.comp_filtered_main,
                            self.comp_filtered_appendix,
                            self.comp_known,
                        )
                    )
                    for item in flat_items:
                        detail_lines.extend(_fmt_ms_entry(item))
                else:
                    summary_lines = [
                        sep, tr("COMPOSITION REPORT SUMMARY"), sep,
                        f"Title: {comp_title}",
                        f"{tr('Total Manuscripts Found')}: {total_count}",
                        f"{tr('Main Manuscripts')}: {len(self.comp_main)}",
                        f"{tr('Main Appendix (Groups)')}: {len(self.comp_appendix)}",
                        f"{tr('Filtered by Text (Manuscripts)')}: {filtered_total}",
                        f"{tr('Known/Excluded Manuscripts')}: {known_count}"
                    ]
                    _append_group_summ(summary_lines, self.comp_appendix, self.comp_summary, tr("MAIN APPENDIX SUMMARY"))
                    
                    summary_lines.extend([sep, tr("KNOWN MANUSCRIPTS SUMMARY"), sep])
                    if self.comp_known:
                        for item in self.comp_known:
                            if item.get('type') == 'manuscript':
                                s, _ = self.meta_mgr.get_meta_for_id(item['sys_id'])
                                summary_lines.append(f"- {s or 'Unknown'}")
                            else:
                                summary_lines.append("- Unknown")
                    else:
                        summary_lines.append(tr("No known manuscripts were excluded."))

                    detail_lines = [sep, tr("MAIN MANUSCRIPTS"), sep]
                    for item in self.comp_main: detail_lines.extend(_fmt_ms_entry(item))

                    if self.comp_filtered_main:
                        detail_lines.extend([sep, tr("FILTERED BY TEXT") + " (Main)", sep])
                        for item in self.comp_filtered_main: detail_lines.extend(_fmt_ms_entry(item))

                    if self.comp_known:
                        detail_lines.extend([sep, tr("KNOWN MANUSCRIPTS"), sep])
                        for item in self.comp_known: detail_lines.extend(_fmt_ms_entry(item))

                    if self.comp_appendix:
                        detail_lines.extend([sep, tr("MAIN APPENDIX") + " (Grouped)", sep])
                        for sig, items in sorted(self.comp_appendix.items(), key=lambda x: len(x[1]), reverse=True):
                            detail_lines.append(f"=== GROUP: {sig} ({len(items)} items) ===")
                            for item in items: detail_lines.extend(_fmt_ms_entry(item))

                    if self.comp_filtered_appendix:
                        detail_lines.extend([sep, tr("FILTERED APPENDIX") + " (Grouped)", sep])
                        for sig, items in sorted(self.comp_filtered_appendix.items(), key=lambda x: len(x[1]), reverse=True):
                            detail_lines.append(f"=== GROUP: {sig} ({len(items)} items) ===")
                            for item in items: detail_lines.extend(_fmt_ms_entry(item))

                with open(path, 'w', encoding='utf-8') as f:
                    f.write(credit_text)
                    all_lines = summary_lines + detail_lines
                    f.write("\n".join(all_lines).strip() + "\n")
                    if lab_config:
                        f.write(lab_config)
                
                QMessageBox.information(self, tr("Saved"), tr("Saved to {}").format(path))

            except Exception as e:
                QMessageBox.critical(self, tr("Error"), f"Failed to save TXT:\n{e}")

    def _fmt_item_legacy(self, item):
        # Fallback for old page style if needed
        sid, p_num, shelf, title = self._get_meta_for_header(item.get('raw_header', ''))
        return [
            "=" * 80,
            f"{shelf or sid} | {title or 'Untitled'} | Img: {p_num} | Version: {item.get('src_lbl','')} | ID: {item.get('uid', sid)} (Score: {item.get('score', 0)})",
            tr("Source Context") + ":", (item.get('source_ctx', '') or "").strip(), "",
            tr("Manuscript") + ":", (item.get('text', '') or "").strip(), ""
        ]

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

    def _fetch_metadata_with_dialog(self, system_ids, title="Loading metadata..."):

        to_fetch = [sid for sid in system_ids if sid and sid not in self.meta_mgr.nli_cache]
        if not to_fetch:
            return False

        dialog = QProgressDialog(tr("Loading shelfmarks and titles..."), tr("Cancel"), 0, len(to_fetch), self)
        dialog.setWindowTitle(title) 
        dialog.setWindowModality(Qt.WindowModality.ApplicationModal)
        dialog.setAutoClose(False)
        dialog.setAutoReset(False)
        dialog.setMinimumDuration(0)

        loop = QEventLoop(self)
        cancelled = False

        worker = ShelfmarkLoaderThread(self.meta_mgr, to_fetch)

        def on_progress(curr, total, sid):
            dialog.setMaximum(total)
            dialog.setValue(curr)
            dialog.setLabelText(f"Loaded {curr}/{total} (ID: {sid})")

        def on_finished(was_cancelled):
            nonlocal cancelled
            cancelled = was_cancelled
            dialog.reset()
            loop.quit()
            if was_cancelled:
                QMessageBox.information(self, "Metadata", tr("Loading metadata was cancelled."))

        def on_error(err):
            QMessageBox.critical(self, tr("Metadata Error"), err)
            dialog.reset()
            loop.quit()

        def handle_cancel():
            worker.request_cancel()

        dialog.canceled.connect(handle_cancel)
        worker.progress_signal.connect(on_progress)
        worker.finished_signal.connect(on_finished)
        worker.error_signal.connect(on_error)

        worker.start()
        dialog.show()
        loop.exec()
        worker.wait()

        return cancelled

    def _resolve_meta_labels(self, raw_header):
        sid, page, shelf, title = self._get_meta_for_header(raw_header)
        sys_id = sid or "Unknown System ID"

        if sid and sid not in self.meta_mgr.nli_cache:
            meta = self.meta_mgr.fetch_nli_data(sid)
            title = title or (meta.get('title') if meta else None)

        shelf_lbl = shelf or f"[Shelfmark missing for {sys_id}]"
        title_lbl = title or "[Title missing]"

        return sys_id, page, shelf_lbl, title_lbl

    def browse_load(self):
        if not self.searcher: return
        sid = self.browse_sys_input.text().strip()
        fl_id = self.browse_fl_input.text().strip()
        if not sid and not fl_id: return

        page_data = None
        if fl_id:
            page_data = self.searcher.get_browse_page_by_fl(fl_id, sid or None)
            if not page_data and not sid:
                QMessageBox.warning(self, tr("Error"), tr("FL not found."))
                return
            if page_data:
                sid = page_data.get('sys_id', sid)
                self.browse_sys_input.setText(sid or "")
                self.browse_fl_input.setText(page_data.get('fl_id', fl_id))

        if not sid:
            QMessageBox.warning(self, tr("Error"), tr("FL not found."))
            return

        self.current_browse_sid = sid
        self.current_browse_p = page_data['p_num'] if page_data else None
        self.btn_b_catalog.setEnabled(True)
        self.btn_b_all.setEnabled(True)   # Enable
        self.btn_b_save.setEnabled(True)  # Enable
        self.browse_update_view(0)

    def browse_navigate(self, d): self.browse_update_view(d)

    def browse_update_view(self, d):
        pd = self.searcher.get_browse_page(self.current_browse_sid, self.current_browse_p, d)
        if not pd: QMessageBox.warning(self, tr("Nav"), tr("Not found or end.")); return

        self.current_browse_p = pd['p_num']
        # Preprocess the text outside the f-string to avoid backslash parsing issues
        browse_html_text = pd['text'].replace('\n', '<br>')
        self.browse_text.setHtml(f"<div dir='rtl'>{browse_html_text}</div>")
        
        full_header = pd.get('full_header', '')
        _, _, shelf, title = self._get_meta_for_header(full_header)

        # --- UPDATE: Combined Label Text ---
        info_text = f"<b>{shelf}</b><br>{title or ''}"
        self.browse_info_lbl.setText(info_text)
        # -----------------------------------

        self.lbl_page_count.setText(f"{pd['current_idx']}/{pd['total_pages']}")
        self.btn_b_prev.setEnabled(pd['current_idx']>1); self.btn_b_next.setEnabled(pd['current_idx']<pd['total_pages'])

        if self.current_browse_sid in self.meta_mgr.nli_cache:
            self.fetch_browse_thumbnail(self.current_browse_sid)
        else:
            self.browse_thumb.setText("Loading Meta...")
            def worker():
                self.meta_mgr.fetch_nli_data(self.current_browse_sid)
                self.browse_thumb_resolved.emit(self.current_browse_sid, "") 
            threading.Thread(target=worker, daemon=True).start()
        
    def browse_open_catalog(self):
        if self.current_browse_sid:
            QDesktopServices.openUrl(QUrl(f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{self.current_browse_sid}"))

    def _on_browse_thumb_resolved(self, sid, _unused_url):
        if sid != self.current_browse_sid:
            return
            
        self.fetch_browse_thumbnail(sid)

    def start_browse_download(self, sid, thumb_url):
        if sid != self.current_browse_sid:
            return

        self.browse_thumb_url = thumb_url
        self.cancel_browse_image_thread()

        if not thumb_url:
            self.on_browse_img_failed()
            return

        self.browse_img_thread = ImageLoaderThread(thumb_url)
        self.browse_img_thread.image_loaded.connect(self.on_browse_img_loaded)
        self.browse_img_thread.load_failed.connect(self.on_browse_img_failed)
        self.browse_img_thread.start()

    def on_browse_img_loaded(self, image):
        pix = QPixmap.fromImage(image)
        # Scale carefully to avoid distortion
        if not pix.isNull():
            scaled = pix.scaled(self.browse_thumb.size(), Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
            self.browse_thumb.setPixmap(scaled)
            self.browse_thumb.setText("")
        else:
            self.on_browse_img_failed()

    def on_browse_img_failed(self):
        self.browse_thumb.setPixmap(QPixmap())
        self.browse_thumb.setText("No Preview")

    def cancel_browse_image_thread(self):
        if getattr(self, 'browse_img_thread', None) and self.browse_img_thread.isRunning():
            self.browse_img_thread.cancel()
            self.browse_img_thread.wait()

    def fetch_browse_thumbnail(self, sys_id, meta=None):
        self.cancel_browse_image_thread()
        self.browse_thumb.setText("Loading...")
        self.browse_thumb.setPixmap(QPixmap())

        # Load from cache if not provided
        meta = meta or self.meta_mgr.nli_cache.get(sys_id)
        
        # In genizah_core, we now guarantee that 'thumb_url' comes from 907 $d if available
        thumb_url = meta.get('thumb_url') if meta else None

        if thumb_url:
            self.start_browse_download(sys_id, thumb_url)
        else:
            # If metadata exists but no thumb_url, it means no image at all
            if meta:
                self.browse_thumb.setText(tr("No Image"))
            else:
                self.browse_thumb.setText(tr("Waiting..."))
    
    def run_indexing(self):
        # 1. Pre-check: Does the input file exist?
        if not os.path.exists(Config.FILE_V8):
            msg = tr("The transcriptions file ('Transcriptions.txt') was not found in the application folder.") + "\n\n" + \
                  tr("Would you like to locate it manually?")
            
            reply = QMessageBox.question(
                self, 
                tr("File Not Found"), 
                msg, 
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )

            if reply == QMessageBox.StandardButton.Yes:
                # Open File Dialog to let user pick the file
                path, _ = QFileDialog.getOpenFileName(
                    self, 
                    tr("Select Transcriptions File"), 
                    "", 
                    "Text Files (*.txt);;All Files (*)"
                )
                
                if path:
                    # Update Config dynamically for this session
                    Config.FILE_V8 = path
                else:
                    # User cancelled the file dialog
                    return
            else:
                # User clicked No
                return

        # 2. Standard Indexing Confirmation
        if not self.indexer: return
        
        if QMessageBox.question(self, tr("Index"), tr("Start indexing?"), QMessageBox.StandardButton.Yes|QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes:
            self.index_progress.setRange(0, 1)
            self.index_progress.setValue(0)
            self.index_progress.setFormat(tr("Indexing... %p%"))
            
            self.ithread = IndexerThread(self.meta_mgr)
            self.ithread.progress_signal.connect(self.on_index_progress)
            self.ithread.finished_signal.connect(self.on_index_finished)
            self.ithread.error_signal.connect(self.on_index_error)
            self.ithread.start()

    def on_index_progress(self, current, total):
        self.index_progress.setRange(0, max(total, 1))
        self.index_progress.setValue(current)
        self.index_progress.setFormat(f"{current}/{total} lines")

    def on_index_finished(self, total_docs):
        self.index_progress.setValue(self.index_progress.maximum())
        self.index_progress.setFormat(tr("Indexing complete"))
        self.searcher.reload_index()
        QMessageBox.information(self, tr("Done"), tr("Indexing complete. Documents indexed: {}").format(total_docs))

    def on_index_error(self, err):
        self.index_progress.setFormat(tr("Indexing failed"))
        QMessageBox.critical(self, tr("Indexing Error"), str(err))

    def closeEvent(self, event):
        # Ensure worker threads are stopped before the window is destroyed
        try:
            if getattr(self, 'meta_loader', None) and self.meta_loader.isRunning():
                self.meta_loader.request_cancel()
                self.meta_loader.wait()

            if getattr(self, 'search_thread', None) and self.search_thread.isRunning():
                self.search_thread.requestInterruption()
                self.search_thread.wait(2000)
                if self.search_thread.isRunning():
                    self.search_thread.terminate()
                    self.search_thread.wait()

            if getattr(self, 'comp_thread', None) and self.comp_thread.isRunning():
                self.comp_thread.requestInterruption()
                self.comp_thread.wait(2000)
                if self.comp_thread.isRunning():
                    self.comp_thread.terminate()
                    self.comp_thread.wait()

            if getattr(self, 'group_thread', None) and self.group_thread.isRunning():
                self.group_thread.requestInterruption()
                self.group_thread.wait(2000)
                if self.group_thread.isRunning():
                    self.group_thread.terminate()
                    self.group_thread.wait()
        finally:
            super().closeEvent(event)

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
        if hasattr(ctypes, 'windll'):
            myappid = f'genizah.search.pro.{APP_VERSION}'
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)
    except (ImportError, AttributeError):
        pass

    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
    icon_path = resource_path("icon.ico")
    
    if os.path.exists(icon_path):
        app_icon = QIcon(icon_path)
        app.setWindowIcon(app_icon)
    
    window = GenizahGUI()
    window.showMaximized()
    sys.exit(app.exec())
