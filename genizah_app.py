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
                             QTextBrowser, QFileDialog, QMenu, QGroupBox, QSpinBox, QDoubleSpinBox,
                             QTreeWidget, QTreeWidgetItem, QPlainTextEdit, QStyle,
                             QGridLayout, QToolTip, QProgressDialog, QStackedLayout,
                             QScrollArea, QFrame, QSlider, QStyleOptionButton, QSizePolicy, QInputDialog,
                             QToolButton)
from PyQt6.QtCore import Qt, QTimer, QUrl, QSize, pyqtSignal, QThread, QEventLoop, QEvent, QRect
from PyQt6.QtGui import QFont, QIcon, QDesktopServices, QPixmap, QImage, QFontMetrics, QTextDocument, QTransform

from version import APP_VERSION

from collections import defaultdict

_CORE_IMPORT_ERROR = None
try:
    from genizah_core import Config, MetadataManager, VariantManager, SearchEngine, LabEngine, Indexer, AIManager, tr, save_language, CURRENT_LANG, get_logger, natural_sort_key
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
from gui_threads import SearchThread, LabSearchThread, IndexerThread, ShelfmarkLoaderThread, CompositionThread, LabCompositionThread, GroupingThread, AIWorkerThread, StartupThread, EnrichMetadataThread, ExternalResourceThread
from filter_text_dialog import FilterTextDialog

logger = get_logger(__name__)

class LabScoringDialog(QDialog):
    """Configuration for Lab Mode Scoring (Advanced)."""
    def __init__(self, parent, lab_engine):
        super().__init__(parent)
        self.setWindowTitle(tr("Advanced Scoring"))
        self.resize(500, 500)
        self.lab_engine = lab_engine
        self.settings = lab_engine.settings
        if CURRENT_LANG == 'he':
            self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        layout = QVBoxLayout()
        layout.addWidget(QLabel(tr("Adjust how the algorithm prioritizes results.")))
        
        grid = QGridLayout()
        
        # Order Bonus
        self.spin_order_bonus = QDoubleSpinBox(); self.spin_order_bonus.setRange(0.0, 100.0); self.spin_order_bonus.setSingleStep(1.0); self.spin_order_bonus.setValue(getattr(self.settings, 'order_bonus', 10.0))
        lbl_order = QLabel(tr("Sequential Order Bonus:")); lbl_order.setStyleSheet("color: #2980b9; font-weight: bold;")
        grid.addWidget(lbl_order, 0, 0); grid.addWidget(self.spin_order_bonus, 0, 1)

        # Coverage
        self.spin_coverage_power = QDoubleSpinBox(); self.spin_coverage_power.setRange(1.0, 10.0); self.spin_coverage_power.setValue(self.settings.coverage_power)
        grid.addWidget(QLabel(tr("Coverage Penalty Power:")), 1, 0); grid.addWidget(self.spin_coverage_power, 1, 1)

        # Noise Suppression
        lbl_noise = QLabel(tr("Stop-Word Suppression:")); lbl_noise.setStyleSheet("font-weight: bold; margin-top: 10px;")
        grid.addWidget(lbl_noise, 2, 0, 1, 2)

        # Short Word Score
        self.spin_stop_score = QDoubleSpinBox(); self.spin_stop_score.setRange(0.0, 50.0); self.spin_stop_score.setSingleStep(0.5); self.spin_stop_score.setValue(getattr(self.settings, 'stop_word_score', 1.0))
        self.spin_stop_score.setToolTip(tr("Points given for very short words (<3 letters). Keep low to reduce noise."))
        grid.addWidget(QLabel(tr("Score for Short Words (<3):")), 3, 0); grid.addWidget(self.spin_stop_score, 3, 1)

        # Common 3-Char Score
        self.spin_common3_score = QDoubleSpinBox(); self.spin_common3_score.setRange(0.0, 50.0); self.spin_common3_score.setSingleStep(0.5); self.spin_common3_score.setValue(getattr(self.settings, 'common_3char_score', 2.0))
        self.spin_common3_score.setToolTip(tr("Points for common 3-letter words (e.g. 'ליה', 'הכי')."))
        grid.addWidget(QLabel(tr("Score for Common 3-Letter:")), 4, 0); grid.addWidget(self.spin_common3_score, 4, 1)

        # Other Weights
        lbl_other = QLabel(tr("Standard Weights:")); lbl_other.setStyleSheet("font-weight: bold; margin-top: 10px;")
        grid.addWidget(lbl_other, 5, 0, 1, 2)

        self.spin_len_bonus = QDoubleSpinBox(); self.spin_len_bonus.setRange(1.0, 10.0); self.spin_len_bonus.setValue(self.settings.length_bonus_factor)
        grid.addWidget(QLabel(tr("Long Word Bonus:")), 6, 0); grid.addWidget(self.spin_len_bonus, 6, 1)

        self.spin_unique_base = QSpinBox(); self.spin_unique_base.setRange(10, 1000); self.spin_unique_base.setValue(self.settings.unique_bonus_base)
        grid.addWidget(QLabel(tr("Unique Match Base Score:")), 7, 0); grid.addWidget(self.spin_unique_base, 7, 1)

        self.spin_density = QDoubleSpinBox(); self.spin_density.setRange(0.0, 5.0); self.spin_density.setValue(self.settings.density_penalty)
        grid.addWidget(QLabel(tr("Distance Penalty:")), 8, 0); grid.addWidget(self.spin_density, 8, 1)

        self.spin_common_factor = QDoubleSpinBox(); self.spin_common_factor.setRange(0.0, 1.0); self.spin_common_factor.setValue(self.settings.common_penalty_factor)
        grid.addWidget(QLabel(tr("Repeated Word Factor:")), 9, 0); grid.addWidget(self.spin_common_factor, 9, 1)

        # Display Limit
        self.spin_display_limit = QSpinBox(); self.spin_display_limit.setRange(50, 1000); self.spin_display_limit.setValue(getattr(self.settings, 'lab_display_limit', 500))
        self.spin_display_limit.setToolTip(tr("Lower values prevent the app from freezing. All results are still exported."))
        grid.addWidget(QLabel(tr("Max Results to Display:")), 10, 0); grid.addWidget(self.spin_display_limit, 10, 1)

        layout.addLayout(grid)
        layout.addStretch()
        
        btn_box = QHBoxLayout()
        self.btn_save = QPushButton(tr("Save & Close")); self.btn_save.clicked.connect(self.save_and_close)
        self.btn_cancel = QPushButton(tr("Cancel")); self.btn_cancel.clicked.connect(self.reject)
        btn_box.addStretch(); btn_box.addWidget(self.btn_cancel); btn_box.addWidget(self.btn_save)
        layout.addLayout(btn_box)
        self.setLayout(layout)

    def save_and_close(self):
        self.settings.coverage_power = self.spin_coverage_power.value()
        self.settings.length_bonus_factor = self.spin_len_bonus.value()
        self.settings.common_penalty_factor = self.spin_common_factor.value()
        self.settings.density_penalty = self.spin_density.value()
        self.settings.unique_bonus_base = self.spin_unique_base.value()
        if hasattr(self.settings, 'order_bonus'): self.settings.order_bonus = self.spin_order_bonus.value()
        if hasattr(self.settings, 'stop_word_score'):
            self.settings.stop_word_score = self.spin_stop_score.value()
            self.settings.common_3char_score = self.spin_common3_score.value()
        
        self.settings.lab_display_limit = self.spin_display_limit.value()
        self.settings.save()
        self.accept()

class LabPanel(QFrame):
    def __init__(self, parent, mode):
        super().__init__(parent)
        self.mode = mode
        self.lab_engine = None
        self.settings = None
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setFrameShadow(QFrame.Shadow.Raised)
        # Removed hardcoded background color to support both light and dark themes
        self.setStyleSheet("border-radius: 5px; margin-bottom: 5px;")

        self.init_ui()
        self.setVisible(False)

    def set_engine(self, engine):
        self.lab_engine = engine
        self.settings = engine.settings
        self.refresh_values()
        self.enable_controls(True)
        self._mark_rebuild_required()

    def enable_controls(self, enabled):
        self.setEnabled(enabled)

    def init_ui(self):
        self.layout = QHBoxLayout(self)
        self.layout.setContentsMargins(10, 5, 10, 5)

        # Shared: Rebuild
        self.btn_rebuild = QPushButton(tr("Rebuild Lab Index"))
        self.btn_rebuild.setStyleSheet("background-color: #d35400; color: white; font-weight: bold; border-radius: 4px; padding: 4px;")
        self.btn_rebuild.clicked.connect(self.run_rebuild)
        self.layout.addWidget(self.btn_rebuild)

        self.lbl_idx_status = QLabel("")
        self.layout.addWidget(self.lbl_idx_status)

        # Spacer
        self.layout.addSpacing(20)
        self.layout.addWidget(QLabel("|"))
        self.layout.addSpacing(20)

        if self.mode == 'search':
            # Min Match
            self.layout.addWidget(QLabel(tr("Minimum Match %:")))
            self.spin_min = QSpinBox()
            self.spin_min.setToolTip(tr("Minimum percentage of query terms required to consider a result relevant."))
            self.spin_min.setRange(10, 100)
            self.spin_min.setSuffix("%")
            self.spin_min.valueChanged.connect(self.on_change)
            self.layout.addWidget(self.spin_min)

            # Candidate Limit
            self.layout.addWidget(QLabel(tr("Max Results to Process:")))
            self.spin_limit = QSpinBox()
            self.spin_limit.setToolTip(tr("Maximum number of raw candidates to fetch from the index before detailed scoring."))
            self.spin_limit.setRange(500, 1000000)
            self.spin_limit.setSingleStep(500)
            self.spin_limit.valueChanged.connect(self.on_change)
            self.layout.addWidget(self.spin_limit)

            # Deep Scan Limit
            self.layout.addWidget(QLabel(tr("Deep Limit:")))
            self.spin_scan_limit = QSpinBox()
            self.spin_scan_limit.setToolTip(tr("Maximum number of documents to scan in Deep Scan mode."))
            self.spin_scan_limit.setRange(10000, 1000000)
            self.spin_scan_limit.setSingleStep(10000)
            self.spin_scan_limit.valueChanged.connect(self.on_change)
            self.layout.addWidget(self.spin_scan_limit)

            self.layout.addStretch()

            # Advanced Scoring
            self.btn_scoring = QPushButton(tr("Advanced Scoring..."))
            self.btn_scoring.setStyleSheet("background-color: #7f8c8d; color: white; font-weight: bold; border-radius: 4px; padding: 4px;")
            self.btn_scoring.clicked.connect(self.open_scoring)
            self.layout.addWidget(self.btn_scoring)

        elif self.mode == 'comp':
            # Chunk Limit
            self.layout.addWidget(QLabel(tr("Max Candidates per Chunk:")))
            self.spin_chunk_limit = QSpinBox()
            self.spin_chunk_limit.setToolTip(tr("Maximum number of index hits to process per text chunk."))
            self.spin_chunk_limit.setRange(50, 5000)
            self.spin_chunk_limit.setSingleStep(50)
            self.spin_chunk_limit.valueChanged.connect(self.on_change)
            self.layout.addWidget(self.spin_chunk_limit)

            # Min Score
            self.layout.addWidget(QLabel(tr("Min Chunk Score:")))
            self.spin_min_score = QSpinBox()
            self.spin_min_score.setToolTip(tr("Minimum score required for a chunk to be considered a match."))
            self.spin_min_score.setRange(10, 500)
            self.spin_min_score.valueChanged.connect(self.on_change)
            self.layout.addWidget(self.spin_min_score)

            # Max Final
            self.layout.addWidget(QLabel(tr("Max Final Results:")))
            self.spin_max_final = QSpinBox()
            self.spin_max_final.setToolTip(tr("Maximum number of results to display in the tree (prevents freezing). All results are exported."))
            self.spin_max_final.setRange(10, 250)
            self.spin_max_final.setValue(200)
            self.spin_max_final.valueChanged.connect(self.on_change)
            self.layout.addWidget(self.spin_max_final)

            self.layout.addStretch()

            # Advanced Scoring
            self.btn_scoring = QPushButton(tr("Advanced Scoring..."))
            self.btn_scoring.setStyleSheet("background-color: #7f8c8d; color: white; font-weight: bold; border-radius: 4px; padding: 4px;")
            self.btn_scoring.clicked.connect(self.open_scoring)
            self.layout.addWidget(self.btn_scoring)

    def refresh_values(self):
        if not self.settings: return
        self.blockSignals(True)
        if self.mode == 'search':
            self.spin_min.setValue(self.settings.min_should_match)
            self.spin_limit.setValue(self.settings.candidate_limit)
            if hasattr(self, 'spin_scan_limit'):
                self.spin_scan_limit.setValue(getattr(self.settings, 'lab_scan_limit', 50000))
        elif self.mode == 'comp':
            self.spin_chunk_limit.setValue(self.settings.comp_chunk_limit)
            self.spin_min_score.setValue(self.settings.comp_min_score)
            self.spin_max_final.setValue(self.settings.comp_max_final_results)
        self.blockSignals(False)

    def on_change(self):
        if not self.settings: return
        if self.mode == 'search':
            self.settings.min_should_match = self.spin_min.value()
            self.settings.candidate_limit = self.spin_limit.value()
            if hasattr(self, 'spin_scan_limit'):
                self.settings.lab_scan_limit = self.spin_scan_limit.value()
        elif self.mode == 'comp':
            self.settings.comp_chunk_limit = self.spin_chunk_limit.value()
            self.settings.comp_min_score = self.spin_min_score.value()
            self.settings.comp_max_final_results = self.spin_max_final.value()
        self.settings.save()

    def open_scoring(self):
         if not self.lab_engine: return
         d = LabScoringDialog(self, self.lab_engine)
         d.exec()

    def _mark_rebuild_required(self):
        if self.lab_engine.lab_index_needs_rebuild:
            self.lbl_idx_status.setText(tr("Index missing or outdated."))
            self.lbl_idx_status.setStyleSheet("color: #c0392b;")
        else:
            self.lbl_idx_status.setText(tr("Index is ready."))
            self.lbl_idx_status.setStyleSheet("color: #27ae60;")

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
                    if not os.path.exists(Config.LAB_DIR):
                        os.makedirs(Config.LAB_DIR)
                    
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
        self.lbl_idx_status.setText(tr("Processing docs: {}").format(current))

    def on_rebuild_error(self, err):
        self.btn_rebuild.setEnabled(True)
        self.lbl_idx_status.setText(tr("Error"))
        QMessageBox.critical(self, tr("Error"), str(err))

    def on_rebuild_finished(self, count):
        self.lbl_idx_status.setText(tr("Done. {} docs.").format(count))
        self.lbl_idx_status.setStyleSheet("color: #27ae60; font-weight: bold;")
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
        return natural_sort_key(text1) < natural_sort_key(text2)

class CheckBoxHeader(QHeaderView):
    """Custom HeaderView that draws a checkbox in the first section."""
    toggled = pyqtSignal(bool)

    def __init__(self, parent=None):
        super().__init__(Qt.Orientation.Horizontal, parent)
        self.isChecked = False
        self.setSectionsClickable(True)

    def get_checkbox_rect(self, rect):
        box_size = 20
        padding = 4
        y = rect.top() + (rect.height() - box_size) // 2

        if self.layoutDirection() == Qt.LayoutDirection.RightToLeft:
            x = rect.right() - box_size - padding
        else:
            x = rect.left() + padding

        return QRect(x, y, box_size, box_size)

    def paintSection(self, painter, rect, logicalIndex):
        painter.save()
        super().paintSection(painter, rect, logicalIndex)
        painter.restore()

        if logicalIndex == 0:
            option = QStyleOptionButton()
            option.rect = self.get_checkbox_rect(rect)
            option.state = QStyle.StateFlag.State_Enabled | QStyle.StateFlag.State_Active
            if self.isChecked:
                option.state |= QStyle.StateFlag.State_On
            else:
                option.state |= QStyle.StateFlag.State_Off

            self.style().drawControl(QStyle.ControlElement.CE_CheckBox, option, painter)

    def mousePressEvent(self, event):
        if self.logicalIndexAt(event.pos()) == 0:

            sec_pos = self.sectionViewportPosition(0)
            sec_width = self.sectionSize(0)
            sec_rect = QRect(sec_pos, 0, sec_width, self.height())

            chk_rect = self.get_checkbox_rect(sec_rect)

            if chk_rect.contains(event.pos()):
                self.isChecked = not self.isChecked
                self.viewport().update()
                self.toggled.emit(self.isChecked)
                return # Consume event

        super().mousePressEvent(event)

    def setChecked(self, checked):
        if self.isChecked != checked:
            self.isChecked = checked
            self.viewport().update()

class ZoomableScrollArea(QScrollArea):
    """A ScrollArea that supports hand-panning and wheel-zooming."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWidgetResizable(False)
        self.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.setStyleSheet("background: #222; border: none;")

        # Hide scrollbars but keep functionality
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        self.lbl_img = QLabel()
        self.lbl_img.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_img.setScaledContents(False) 
        self.lbl_img.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Ignored)
        self.setWidget(self.lbl_img)

        self._pixmap = None
        self._zoom_factor = 1.0
        self._drag_start_pos = None
        self._rotation = 0
        self._auto_fit_enabled = False

        self.setCursor(Qt.CursorShape.OpenHandCursor)

    def set_image(self, pixmap):
        self._pixmap = pixmap
        self._rotation = 0
        self._auto_fit_enabled = bool(pixmap)
        if not pixmap:
            self._zoom_factor = 1.0
            self._update_view()
            return

        if not self._apply_fit_to_viewport():
            self._zoom_factor = 1.0
            self._update_view()

    def set_rotation(self, angle: float):
        """Set absolute rotation (degrees clockwise) and update view."""
        self._rotation = angle % 360 if angle is not None else 0
        self._update_view()

    def rotate_view(self, degrees):
        """Add degrees to current rotation and update."""
        self._rotation = (self._rotation + degrees) % 360
        self._update_view()

    def _update_view(self):
        if not self._pixmap or self._pixmap.isNull():
            self.lbl_img.setText(tr("No Image"))
            return

        transform = QTransform().rotate(self._rotation)
        source_pix = self._pixmap.transformed(transform, Qt.TransformationMode.SmoothTransformation)

        scaled_w = int(source_pix.width() * self._zoom_factor)
        scaled_h = int(source_pix.height() * self._zoom_factor)

        # Keep aspect ratio
        scaled_pix = source_pix.scaled(
            scaled_w, scaled_h,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation
        )
        self.lbl_img.setPixmap(scaled_pix)
        self.lbl_img.resize(scaled_pix.size())

    def wheelEvent(self, event):
        # Zoom logic
        self._auto_fit_enabled = False
        delta = event.angleDelta().y()
        if delta > 0:
            self._zoom_factor *= 1.1
        else:
            self._zoom_factor *= 0.9

        # Clamp zoom
        self._zoom_factor = max(0.1, min(self._zoom_factor, 5.0))
        self._update_view()
        event.accept()

    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.setCursor(Qt.CursorShape.ClosedHandCursor)
            self._drag_start_pos = event.pos()
        super().mousePressEvent(event)

    def mouseReleaseEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.setCursor(Qt.CursorShape.OpenHandCursor)
            self._drag_start_pos = None
        super().mouseReleaseEvent(event)

    def mouseMoveEvent(self, event):
        if self._drag_start_pos:
            delta = event.pos() - self._drag_start_pos
            # Adjust horizontal drag direction for RTL vs LTR layouts
            rtl = False
            app = QApplication.instance()
            if app and app.layoutDirection() == Qt.LayoutDirection.RightToLeft:
                rtl = True
            elif self.layoutDirection() == Qt.LayoutDirection.RightToLeft:
                rtl = True

            horiz_delta = -delta.x() if not rtl else delta.x()
            self.horizontalScrollBar().setValue(self.horizontalScrollBar().value() + horiz_delta)
            self.verticalScrollBar().setValue(self.verticalScrollBar().value() - delta.y())
            self._drag_start_pos = event.pos()
        super().mouseMoveEvent(event)

    def zoom_in(self):
        self._auto_fit_enabled = False
        self._zoom_factor *= 1.2
        self._update_view()

    def zoom_out(self):
        self._auto_fit_enabled = False
        self._zoom_factor *= 0.8
        self._update_view()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self._auto_fit_enabled:
            self._apply_fit_to_viewport()

    def _apply_fit_to_viewport(self):
        fit_factor = self._compute_fit_factor()
        if fit_factor is None:
            return False
        self._zoom_factor = fit_factor
        self._update_view()
        return True

    def _compute_fit_factor(self):
        if not self._pixmap or self._pixmap.isNull():
            return None
        viewport_size = self.viewport().size()
        if viewport_size.width() <= 0 or viewport_size.height() <= 0:
            return None

        max_w = viewport_size.width() * 0.7
        max_h = viewport_size.height() * 0.7
        factor_w = max_w / self._pixmap.width()
        factor_h = max_h / self._pixmap.height()
        fit_factor = min(factor_w, factor_h)
        return max(0.1, min(fit_factor, 5.0))

class ManuscriptViewerWidget(QWidget):
    """Reusable widget for displaying manuscript images with navigation."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.images_nli = []
        self.images_ext = []
        self.active_list = []
        self.current_idx = 0
        self.loader_thread = None
        self.external_provider = None
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # Top Bar (Source + Zoom)
        top_bar = QHBoxLayout()
        top_bar.setContentsMargins(5, 5, 5, 5)

        self.combo_source = QComboBox()
        self.combo_source.addItem("NLI")
        self.combo_source.addItem("External (Cambridge/Other)")
        self.combo_source.setVisible(False)
        self.combo_source.currentIndexChanged.connect(self._on_source_changed)

        self.combo_img_selector = QComboBox()
        self.combo_img_selector.setVisible(False)
        self.combo_img_selector.currentIndexChanged.connect(self._on_label_selected)

        btn_zoom_out = QPushButton("-")
        btn_zoom_out.setFixedWidth(30)
        btn_zoom_out.clicked.connect(lambda: self.scroll_area.zoom_out())

        btn_zoom_in = QPushButton("+")
        btn_zoom_in.setFixedWidth(30)
        btn_zoom_in.clicked.connect(lambda: self.scroll_area.zoom_in())

        # Rotation controls
        self.slider_rotation = QSlider(Qt.Orientation.Horizontal)
        self.slider_rotation.setRange(0, 360)
        self.slider_rotation.setValue(0)
        self.slider_rotation.setFixedWidth(160)
        self.slider_rotation.setToolTip(tr("Rotate image (0-360°)"))
        self.slider_rotation.valueChanged.connect(lambda val: self.scroll_area.set_rotation(val))

        btn_rot_left = QPushButton("↺")
        btn_rot_left.setToolTip(tr("Rotate Left 90°"))
        btn_rot_left.setFixedWidth(30)
        btn_rot_left.clicked.connect(lambda: self.adjust_rotation(-90))

        btn_rot_right = QPushButton("↻")
        btn_rot_right.setToolTip(tr("Rotate Right 90°"))
        btn_rot_right.setFixedWidth(30)
        btn_rot_right.clicked.connect(lambda: self.adjust_rotation(90))

        btn_rot_reset = QPushButton(tr("Reset"))
        btn_rot_reset.setToolTip(tr("Reset rotation"))
        btn_rot_reset.setFixedWidth(50)
        btn_rot_reset.clicked.connect(lambda: self.slider_rotation.setValue(0))

        self.btn_external = QPushButton(tr("External Website"))
        self.btn_external.setVisible(False)
        self.btn_external.clicked.connect(self.open_external)

        top_bar.addWidget(self.combo_source)
        top_bar.addWidget(self.combo_img_selector)
        top_bar.addStretch()
        top_bar.addWidget(self.btn_external)
        top_bar.addWidget(btn_rot_left)
        top_bar.addWidget(self.slider_rotation)
        top_bar.addWidget(btn_rot_right)
        top_bar.addWidget(btn_rot_reset)
        top_bar.addSpacing(10)
        top_bar.addWidget(btn_zoom_out)
        top_bar.addWidget(btn_zoom_in)

        layout.addLayout(top_bar)

        # Attribution
        self.lbl_attribution = QLabel("")
        self.lbl_attribution.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_attribution.setStyleSheet("font-size: 10px; color: #7f8c8d; background: transparent; margin: 0px;")
        self.lbl_attribution.setVisible(False)
        layout.addWidget(self.lbl_attribution)

        # Image Area
        self.scroll_area = ZoomableScrollArea()
        layout.addWidget(self.scroll_area, 1)

    def _detect_external_provider(self, meta):
        marc = meta.get('marc', {}) if meta else {}
        url = (marc.get('external_iiif_link') or "").lower()
        if "cudl.lib.cam.ac.uk" in url:
            return "cambridge"

        for img in meta.get('images_ext', []) or []:
            if "cudl.lib.cam.ac.uk" in (img.get('url', '').lower()):
                return "cambridge"

        return None

    def set_image_by_fl_id(self, fl_id):
        digits = re.sub(r"\D", "", str(fl_id or ""))
        if not digits:
            return False

        fallback_url = f"{Config.NLI_IIIF_BASE}/FL{digits}/full/600,/0/default.jpg"
        self.images_nli = [{'label': f"FL{digits}", 'url': fallback_url, 'fl_id': digits}]
        self.images_ext = []
        self.active_list = self.images_nli
        self.current_source = "nli"
        self.combo_source.clear()
        self.combo_source.addItem(f"NLI (1)", "nli")
        self.combo_source.setVisible(False)
        return True

    def load_images(self, meta, initial_idx=0):
        self.external_provider = self._detect_external_provider(meta)

        # Attribution
        attr = meta.get('attribution')
        if attr:
            self.lbl_attribution.setText(attr)
            self.lbl_attribution.setVisible(True)
        else:
            self.lbl_attribution.setVisible(False)

        # meta contains 'images_nli' and 'images_ext'
        self.images_nli = meta.get('images_nli', [])
        self.images_ext = meta.get('images_ext', [])

        # Determine default source
        self.combo_source.blockSignals(True)
        self.combo_source.clear()

        if self.images_ext:
            ext_label = "Cambridge" if self.external_provider == "cambridge" else "External"
            self.combo_source.addItem(f"{ext_label} ({len(self.images_ext)})", "ext")
            if self.images_nli:
                self.combo_source.addItem(f"NLI ({len(self.images_nli)})", "nli")
            self.active_list = self.images_ext
            self.current_source = "ext"
        elif self.images_nli:
            self.combo_source.addItem(f"NLI ({len(self.images_nli)})", "nli")
            self.active_list = self.images_nli
            self.current_source = "nli"
        else:
            self.active_list = []
            self.current_source = None

        self.combo_source.setVisible(len(self.images_nli) > 0 and len(self.images_ext) > 0)
        self.combo_source.blockSignals(False)

        if not self.active_list:
            fl_ids = meta.get('fl_ids') if meta else []
            if isinstance(fl_ids, str):
                fl_ids = [fl_ids]
            for fl in fl_ids or []:
                if self.set_image_by_fl_id(fl):
                    break

        # External Link
        marc = meta.get('marc', {})
        self.external_url = marc.get('external_iiif_link')
        if self.external_url:
            btn_label = tr("Cambridge Website") if self.external_provider == "cambridge" else tr("External Website")
            self.btn_external.setText(btn_label)
        self.btn_external.setVisible(bool(self.external_url))

        self._populate_label_selector()

        # Set Page
        self.set_page(initial_idx)

    def _on_source_changed(self):
        data = self.combo_source.currentData()
        if data == "nli":
            self.active_list = self.images_nli
            self.current_source = "nli"
        else:
            self.active_list = self.images_ext
            self.current_source = "ext"

        # Try to keep index within bounds
        if self.current_idx >= len(self.active_list):
            self.current_idx = 0

        self._populate_label_selector()
        self.set_page(self.current_idx)

    def _resolve_url(self, base_url):
        if not base_url: return None
        if base_url.endswith('.jpg'): return base_url
        return f"{base_url}/full/600,/0/default.jpg"

    def _preload(self, index):
        if index < 0 or index >= len(self.active_list): return
        url = self.active_list[index]['url']
        final = self._resolve_url(url)

        # Spawn thread without connecting signals (just for cache)
        # Store ref to prevent GC
        self.preload_worker = ImageLoaderThread(final)
        self.preload_worker.start()

    def set_page(self, index):
        if not self.active_list:
            self.scroll_area.set_image(None)
            self.scroll_area.lbl_img.setText(tr("No images available"))
            return

        # Bounds check
        if index < 0: index = 0
        if index >= len(self.active_list): index = len(self.active_list) - 1

        self.current_idx = index
        img_data = self.active_list[index]
        base_url = img_data['url']

        self.scroll_area.lbl_img.setText(tr("Loading..."))

        if self.loader_thread and self.loader_thread.isRunning():
            self.loader_thread.cancel()
            self.loader_thread.wait()

        final_url = self._resolve_url(base_url)

        self.loader_thread = ImageLoaderThread(final_url)
        self.loader_thread.image_loaded.connect(self.display_image)
        self.loader_thread.load_failed.connect(lambda: self.scroll_area.lbl_img.setText(tr("No Image")))
        self.loader_thread.start()

        self._sync_label_selector()

        # Preload next image
        self._preload(index + 1)

    def display_image(self, image):
        pix = QPixmap.fromImage(image)
        self.scroll_area.set_image(pix)
        self.slider_rotation.setValue(0)

    def _populate_label_selector(self):
        # Image selector disabled per requirements
        self.combo_img_selector.blockSignals(True)
        self.combo_img_selector.clear()
        self.combo_img_selector.setVisible(False)
        self.combo_img_selector.blockSignals(False)

    def _sync_label_selector(self):
        self.combo_img_selector.setVisible(False)

    def _on_label_selected(self, combo_idx):
        # Selector disabled
        return

    def open_external(self):
        if self.external_url:
            QDesktopServices.openUrl(QUrl(self.external_url))

    def adjust_rotation(self, delta):
        """Adjust rotation via slider to keep controls in sync."""
        new_val = (self.slider_rotation.value() + delta) % 360
        self.slider_rotation.setValue(int(new_val))

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
        headers = dict(Config.HTTP_HEADERS)
        headers["Referer"] = "https://www.nli.org.il/"

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
        self.current_internal_idx = None
        
        self.current_meta_request = 0
        self.extended_info_visible = False

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

        # Image Label Dropdown
        self.combo_img_labels = QComboBox()
        self.combo_img_labels.setFixedWidth(120)
        self.combo_img_labels.setVisible(False)
        self.combo_img_labels.currentIndexChanged.connect(self._on_img_label_selected)

        self.lbl_img_label = QLabel("")
        self.lbl_img_label.setStyleSheet("color: #2980b9; font-weight: bold; margin-left: 10px;")

        nav_row.addWidget(QLabel(tr("Image:"))); nav_row.addWidget(btn_pg_prev); nav_row.addWidget(self.spin_page);
        nav_row.addWidget(self.combo_img_labels) # Added dropdown
        nav_row.addWidget(self.lbl_total); nav_row.addWidget(btn_pg_next); nav_row.addWidget(self.lbl_img_label); nav_row.addStretch()

        action_row = QHBoxLayout()
        self.btn_view_transcription = QPushButton(tr("Browse manuscript")) # Renamed
        self.btn_view_transcription.clicked.connect(self.open_full_transcription)
        self.btn_search_parallels = QPushButton(tr("Search for parallels"))
        self.btn_search_parallels.clicked.connect(self.search_for_parallels)

        self.btn_ext_info = QPushButton(tr("Show Extended Info"))
        self.btn_ext_info.setCheckable(True)
        self.btn_ext_info.toggled.connect(self.toggle_extended_info)
        self.btn_ext_info.setVisible(False)

        # Toggle Image Button
        self.btn_toggle_image = QPushButton(tr("Image"))
        self.btn_toggle_image.setCheckable(True)
        self.btn_toggle_image.setChecked(True) # Default open
        self.btn_toggle_image.clicked.connect(self.toggle_external_viewer)
        self.btn_toggle_image.setVisible(False) # Hidden until images avail

        # Deprecated: btn_external_view replaced/merged logic
        self.btn_external_view = self.btn_toggle_image

        action_row.addWidget(self.btn_view_transcription)
        action_row.addWidget(self.btn_search_parallels)
        action_row.addWidget(self.btn_ext_info)
        action_row.addWidget(self.btn_toggle_image)
        action_row.addStretch()

        self.txt_extended_info = QTextBrowser()
        self.txt_extended_info.setVisible(False)
        self.txt_extended_info.setMaximumHeight(200)
        # Use standard palette (transparent background allowed) to support dark mode
        self.txt_extended_info.setStyleSheet("border: 1px solid #ccc; padding: 5px;")

        meta_col.addWidget(self.lbl_shelf); meta_col.addWidget(self.lbl_title); meta_col.addLayout(info_row); meta_col.addLayout(nav_row); meta_col.addLayout(action_row); meta_col.addWidget(self.txt_extended_info)
        
        # Right: Thumbnail
        self.lbl_thumb = QLabel(tr("No Preview")); self.lbl_thumb.setFixedSize(120, 120); self.lbl_thumb.setAlignment(Qt.AlignmentFlag.AlignCenter); self.lbl_thumb.setStyleSheet("border: 1px solid #7f8c8d;"); self.lbl_thumb.setScaledContents(True)
        
        header_layout.addLayout(meta_col, 1); header_layout.addWidget(self.lbl_thumb)
        main_layout.addWidget(header_widget)
        
        # --- SPLIT VIEW (Manuscript | Source | External) ---
        self.main_splitter = QSplitter(Qt.Orientation.Horizontal)

        # Inner Splitter for Text (Manuscript | Source)
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
        self.text_splitter.setStretchFactor(0, 2)
        self.text_splitter.setStretchFactor(1, 1)
        
        self.main_splitter.addWidget(self.text_splitter)

        # 3. External Viewer Pane (Initially Hidden)
        self.external_pane = QWidget()
        self.external_pane.setVisible(False)
        ext_layout = QVBoxLayout(self.external_pane); ext_layout.setContentsMargins(0,0,0,0)

        self.lbl_ext_attr = QLabel(tr("External Viewer"))
        self.lbl_ext_attr.setStyleSheet("font-weight: bold; padding: 5px; background: #ecf0f1;")
        self.lbl_ext_attr.setWordWrap(True)

        self.txt_ext_meta = QTextBrowser()
        self.txt_ext_meta.setMaximumHeight(100)
        self.txt_ext_meta.setStyleSheet("font-size: 11px;")

        # New: Reusable Viewer Widget
        self.ms_viewer = ManuscriptViewerWidget()

        ext_layout.addWidget(self.lbl_ext_attr)
        ext_layout.addWidget(self.txt_ext_meta)
        ext_layout.addWidget(self.ms_viewer, 1)

        self.main_splitter.addWidget(self.external_pane)
        self.main_splitter.setStretchFactor(0, 3)
        self.main_splitter.setStretchFactor(1, 7)

        main_layout.addWidget(self.main_splitter, 1)
        
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

    def load_page(self, offset=0, target=None):
        if not self.current_sys_id: return
        self.cancel_image_thread()
        
        # Determine strict navigation source
        # If target (Spinbox jump) is set -> Use p_num logic (target)
        # If offset (Next/Prev) is set -> Use internal_index logic (prevents loops)
        
        page_data = None
        
        if target is not None:
            # Jump by number (user typed in box)
            try: p = int(target)
            except: p = 1
            page_data = self.searcher.get_browse_page(self.current_sys_id, p_num=p, next_prev=0)
        else:
            # Relative Navigation (Next/Prev)
            # Use internal index if we have it, otherwise rely on p_num
            idx_arg = self.current_internal_idx 
            p_arg = int(self.current_p_num) if self.current_p_num is not None else None
            
            page_data = self.searcher.get_browse_page(
                self.current_sys_id, 
                p_num=p_arg, 
                next_prev=offset,
                absolute_index=idx_arg # <--- THIS FIXES THE BUG
            )
            
        if not page_data: return

        # --- UPDATE STATE ---
        self.current_p_num = page_data['p_num']
        self.current_internal_idx = page_data['internal_index'] # <--- SAVE IT
        
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

        # 2. Sync Image (Non-Blocking)
        if self.btn_external_view.isChecked():
            QTimer.singleShot(0, self.sync_external_view)

        # --- Render Text ---
        raw_text = page_data['text']
        pattern_str = self.data.get('highlight_pattern')
        
        if pattern_str:
            try:
                regex = re.compile(pattern_str, re.IGNORECASE)
                highlighted_text = regex.sub(r'*\g<0>*', raw_text)
                raw_text = highlighted_text
            except: pass
        
        self.text_ms.setHtml(self._htmlify(raw_text))

        self.lbl_meta_loading.setVisible(False)
        self.lbl_title.setText('')
        self.lbl_img_label.setText("")
        
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
            self.enrich_worker = EnrichMetadataThread(self.meta_mgr, self.current_sys_id)
            self.enrich_worker.finished_signal.connect(self.on_enriched_data_loaded)
            self.enrich_worker.start()
        else:
            self.on_enriched_data_loaded(cached_meta)

    def apply_metadata(self, meta):
        # 1. Update Text Labels
        shelf = self.meta_mgr.get_shelfmark_from_header(self.current_full_header) or meta.get('shelfmark', 'Unknown Shelf')
        self.lbl_shelf.setText(shelf)
        self.lbl_title.setText(meta.get('title', ''))
        self.lbl_meta_loading.setVisible(False)

        # 2. Trigger Image Fetch using the FRESH metadata
        # (This meta object now contains 'thumb_url' from the XML 907 $d field)
        self.fetch_image(self.current_sys_id, meta)

    def toggle_extended_info(self, checked):
        self.extended_info_visible = checked
        self.txt_extended_info.setVisible(checked)
        self.btn_ext_info.setText(tr("Hide Extended Info") if checked else tr("Show Extended Info"))

    def toggle_external_viewer(self, checked):
        self.external_pane.setVisible(checked)
        if checked:
            QTimer.singleShot(0, self.sync_external_view)

    def _on_img_label_selected(self):
        # Handle jump from dropdown
        fl_val = self.combo_img_labels.currentData()
        if fl_val == -1: return

        try:
            page_data = self.searcher.get_browse_page_by_fl(str(fl_val), self.current_sys_id)
            if page_data:
                target_p = page_data['p_num']
                self.load_page(target=target_p)
        except Exception:
            pass

    def on_enriched_data_loaded(self, meta):
        if not meta: return
        if self.current_sys_id not in self.meta_mgr.nli_cache: return

        # 1. Update Image Labels & Dropdown
        fl_digits = re.sub(r"\D", "", str(self.current_fl_id or ""))
        canvas_map = meta.get('canvas_map', {})
        label = canvas_map.get(fl_digits)
        self.lbl_img_label.setText(f"({label})" if label else "")

        # Populate combo box with sorted labels
        self.combo_img_labels.blockSignals(True)
        self.combo_img_labels.clear()

        has_labels = False
        if canvas_map:
            self.combo_img_labels.addItem(tr("Select Image"), -1)
            # Sort by FL for approximate order
            for fl, lbl in sorted(canvas_map.items()):
                self.combo_img_labels.addItem(lbl, fl)
            has_labels = True

        self.combo_img_labels.setVisible(has_labels)
        self.combo_img_labels.blockSignals(False)

        # 2. Populate External / Image Viewer
        has_images = bool(meta.get('images_nli') or meta.get('images_ext'))

        self.btn_toggle_image.setVisible(has_images)

        if has_images:
            # Show viewer by default
            self.external_pane.setVisible(True)
            self.btn_toggle_image.setChecked(True)

            # Metadata for side pane
            ext_meta = meta.get('external_meta', {})

            self.lbl_ext_attr.setVisible(False)

            meta_html = ""
            for k, v in ext_meta.items():
                meta_html += f"<b>{k}:</b> {v}<br>"
            self.txt_ext_meta.setHtml(meta_html)
            self.txt_ext_meta.setVisible(bool(meta_html))

            # Load images into widget
            try: initial_idx = int(self.current_p_num) - 1
            except: initial_idx = 0

            self.ms_viewer.load_images(meta, initial_idx)
        else:
            self.external_pane.setVisible(False)
            self.btn_toggle_image.setChecked(False)

        # 3. Build Extended Info HTML (Text)
        marc = meta.get('marc', {})
        if not marc and not meta.get('physical_desc'):
            self.btn_ext_info.setVisible(False)
            return

        html = "<div style='font-family:Arial;'>"
        date_val = marc.get('date');
        if date_val: html += f"<p><b>{tr('Date')}:</b> {date_val}</p>"

        dims = marc.get('dimensions'); phys = meta.get('physical_desc')
        if dims or phys: html += f"<p><b>{tr('Physical Description')}:</b> {phys or ''} {dims or ''}</p>"

        eng_title = marc.get('english_title')
        if eng_title: html += f"<p><b>{tr('English Title')}:</b> {eng_title}</p>"

        subjects = marc.get('subjects', [])
        if subjects: html += f"<p><b>{tr('Subjects')}:</b> {'; '.join(subjects)}</p>"

        notes = marc.get('notes', [])
        if notes:
            html += f"<p><b>{tr('Notes')}:</b><ul>"
            for n in notes: html += f"<li>{n}</li>"
            html += "</ul></p>"

        people = marc.get('people', [])
        if people: html += f"<p><b>{tr('People')}:</b> {'; '.join(people)}</p>"

        bib = marc.get('bibliography', [])
        if bib:
            html += f"<p><b>{tr('Bibliography')}:</b><ul>"
            for b in bib: html += f"<li>{b}</li>"
            html += "</ul></p>"

        html += "</div>"
        self.txt_extended_info.setHtml(html)
        self.btn_ext_info.setVisible(True)

        self.lbl_title.setText(meta.get('title', ''))
        shelf = meta.get('shelfmark')
        if shelf and shelf != "Unknown":
            library = marc.get('current_owner')
            if library: shelf = f"{library} | {shelf}"
            self.lbl_shelf.setText(shelf)

    def sync_external_view(self):
        # Determine index
        try: idx = int(self.current_p_num) - 1
        except: idx = 0
        self.ms_viewer.set_page(idx)

    def on_metadata_loaded(self, request_id, meta):
        if request_id != self.current_meta_request:
            return
        self.apply_metadata(meta or {})

    def cancel_image_thread(self):
        img_thread = getattr(self, 'img_thread', None)
        if img_thread and img_thread.isRunning():
            img_thread.cancel()
            img_thread.wait()

        if getattr(self, 'ext_img_thread', None) and self.ext_img_thread.isRunning():
            self.ext_img_thread.cancel()
            self.ext_img_thread.wait()

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
        self.last_browse_field = None
        self.current_browse_sid = None
        self.current_browse_p = None
        self.current_browse_internal_idx = None
        self.meta_loader = None
        self.meta_cached_count = 0
        self.meta_to_fetch_count = 0
        self.meta_progress_current = 0
        self.browse_thumb_url = None
        self.browse_img_thread = None
        self.shelfmark_items_by_sid = {}
        self.title_items_by_sid = {}
        self.comp_thread = None 
        self.comp_worker = None


        self.init_ui()

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

    def on_startup_finished(self, meta_mgr, var_mgr, searcher, indexer, ai_mgr):
        try:
            self.meta_mgr = meta_mgr
            self.var_mgr = var_mgr
            self.searcher = searcher
            self.indexer = indexer
            self.ai_mgr = ai_mgr

            # Init Lab Engine (lightweight init)
            self.lab_engine = LabEngine(self.meta_mgr, self.var_mgr)

            # Setup Panels (guaranteed to exist as init_ui runs before startup thread)
            if hasattr(self, 'lab_panel_search'):
                self.lab_panel_search.set_engine(self.lab_engine)
            else:
                logger.warning("lab_panel_search not found during startup finish")

            if hasattr(self, 'lab_panel_comp'):
                self.lab_panel_comp.set_engine(self.lab_engine)
            else:
                logger.warning("lab_panel_comp not found during startup finish")

            os.makedirs(Config.REPORTS_DIR, exist_ok=True)
            self.browse_thumb_resolved.connect(self._on_browse_thumb_resolved)

            # Update Settings Tab with loaded AI config
            if self.ai_mgr:
                self.combo_provider.setCurrentText(self.ai_mgr.provider)
                self.txt_model.setText(self.ai_mgr.model_name)
                self.txt_api_key.setText(self.ai_mgr.api_key)

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
        QMessageBox.information(self, tr("נדרש אתחול מחדש"), tr("אנא הפעילו מחדש את התוכנה כדי שהשינוי בשפה ייכנס לתוקף."))

    def create_search_tab(self):
        panel = QWidget(); layout = QVBoxLayout()

        # Top Container with Multi-Row Layout
        top_container = QWidget()
        top_layout = QVBoxLayout(top_container)
        top_layout.setContentsMargins(0, 0, 0, 0)

        # Row 1: Query & Search Buttons
        row1 = QHBoxLayout()
        self.query_input = QLineEdit(); self.query_input.setPlaceholderText(tr("Search terms, title or shelfmark..."))
        self.query_input.returnPressed.connect(self.toggle_search)
        
        self.btn_search = QPushButton(tr("Search")); self.btn_search.clicked.connect(self.toggle_search)
        self.btn_search.setStyleSheet("background-color: #27ae60; color: white; font-weight: bold; min-width: 80px;")
        self.btn_search.setEnabled(False)

        self.btn_ai = QPushButton(tr("🤖 AI Assistant")); self.btn_ai.setStyleSheet("background-color: #8e44ad; color: white;")
        self.btn_ai.setToolTip(tr("Generate Regex with Gemini AI"))
        self.btn_ai.clicked.connect(self.open_ai)
        self.btn_ai.setEnabled(False)

        row1.addWidget(QLabel(tr("Query:")))
        row1.addWidget(self.query_input)
        row1.addWidget(self.btn_search)
        row1.addWidget(self.btn_ai)

        # Row 2: Search Parameters & Lab Mode
        row2 = QHBoxLayout()

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
        
        self.btn_lab_mode_toggle = QPushButton(tr("Lab Mode"))
        self.btn_lab_mode_toggle.setCheckable(True)
        self.btn_lab_mode_toggle.setToolTip(tr("Experimental search mode using advanced proximity scoring. WARNING: Can freeze the program. Use with caution."))
        self.btn_lab_mode_toggle.toggled.connect(self.on_lab_mode_toggled_search)

        # Deep Scan Checkbox
        self.chk_lab_deep = QCheckBox(tr("Deep Scan"))
        self.chk_lab_deep.setToolTip(tr("Slower but checks deeper. Use for common phrases/quotes"))
        self.chk_lab_deep.setEnabled(False) # Enabled only in Lab Mode
        self.chk_lab_deep.toggled.connect(self.on_deep_scan_toggled_search)

        # Help Button
        btn_help = QPushButton("?")
        btn_help.setFixedWidth(30)
        btn_help.setStyleSheet("background-color: #f39c12; color: white; font-weight: bold; border-radius: 15px;")
        btn_help.clicked.connect(lambda: self.open_help_center(anchor="search"))

        row2.addWidget(QLabel(tr("Mode:")))
        row2.addWidget(self.mode_combo)
        row2.addWidget(QLabel(tr("Gap:")))
        row2.addWidget(self.gap_input)
        row2.addWidget(self.btn_lab_mode_toggle)
        row2.addWidget(self.chk_lab_deep)
        row2.addStretch()
        row2.addWidget(btn_help)

        top_layout.addLayout(row1)
        top_layout.addLayout(row2)
        layout.addWidget(top_container)

        self.lab_panel_search = LabPanel(self, 'search')
        layout.addWidget(self.lab_panel_search)
        
        self.search_progress = QProgressBar(); self.search_progress.setVisible(False)
        layout.addWidget(self.search_progress)
        
        # Results Table Setup
        self.COL_CHECKBOX = 0
        self.COL_ACTIONS = 1
        self.COL_SYS_ID = 2
        self.COL_SHELF = 3
        self.COL_TITLE = 4
        self.COL_SNIPPET = 5
        self.COL_IMG = 6
        self.COL_SRC = 7

        self.results_table = QTableWidget(); self.results_table.setColumnCount(8)
        self.results_table.setHorizontalHeaderLabels(["", tr("Actions"), tr("System ID"), tr("Shelfmark"), tr("Title"), tr("Snippet"), tr("Img"), tr("Src")])

        # Custom Header
        self.chk_search_header = CheckBoxHeader(self.results_table)
        self.chk_search_header.toggled.connect(self.on_search_select_all_toggled)
        self.results_table.setHorizontalHeader(self.chk_search_header)

        self.results_table.setColumnWidth(self.COL_CHECKBOX, 30) # Checkbox column
        self.results_table.setColumnWidth(self.COL_ACTIONS, 70)
        self.results_table.setColumnWidth(self.COL_SYS_ID, 135)
        self.results_table.setColumnWidth(self.COL_SHELF, 175)
        self.results_table.horizontalHeader().setSectionResizeMode(self.COL_SNIPPET, QHeaderView.ResizeMode.Stretch)
        # Ensure column 0 is not sortable to avoid confusion with check action
        self.results_table.horizontalHeader().setSectionResizeMode(self.COL_CHECKBOX, QHeaderView.ResizeMode.Fixed)
        self.results_table.horizontalHeader().setSectionResizeMode(self.COL_ACTIONS, QHeaderView.ResizeMode.Fixed)

        self.results_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.results_table.setSortingEnabled(True) # Enable sorting
        self.results_table.doubleClicked.connect(self.show_full_text)
        self.results_table.itemChanged.connect(self.on_search_result_item_changed)
        
        self.results_placeholder = QLabel(tr("Please wait while components load..."))
        self.results_placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.results_placeholder.setWordWrap(True)
        self.results_placeholder.setStyleSheet("font-size: 16px; font-weight: bold; color: #c0392b;")

        # Container for table
        self.table_container = QWidget()
        table_layout = QVBoxLayout(self.table_container)
        table_layout.setContentsMargins(0, 0, 0, 0)

        table_layout.addWidget(self.results_table)

        self.results_stack = QStackedLayout()
        self.results_stack.addWidget(self.results_placeholder)
        self.results_stack.addWidget(self.table_container)

        results_container = QWidget()
        results_container.setLayout(self.results_stack)
        layout.addWidget(results_container)

        bot = QHBoxLayout()

        self.status_label = QLabel(tr("Ready."))
        self.lbl_search_export = QLabel(tr("Export Results") + ":")
        
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

        # Add controls to status row
        bot.addWidget(self.status_label, 1)

        # Append export controls to the right
        bot.addWidget(QLabel("|"))
        bot.addWidget(self.lbl_search_export)
        bot.addWidget(self.btn_exp_xlsx)
        bot.addWidget(self.btn_exp_csv)
        bot.addWidget(self.btn_exp_txt)
        
        layout.addLayout(bot)
        panel.setLayout(layout)
        return panel

    def set_results_loading(self, is_loading: bool):
        """Toggle the search results placeholder while components initialize."""
        if hasattr(self, "results_stack") and hasattr(self, "results_placeholder") and hasattr(self, "table_container"):
            target = self.results_placeholder if is_loading else self.table_container
            self.results_stack.setCurrentWidget(target)

    def create_composition_tab(self):
        panel = QWidget(); layout = QVBoxLayout(); splitter = QSplitter(Qt.Orientation.Vertical)
        
        inp_w = QWidget(); in_l = QVBoxLayout()
        top_row = QHBoxLayout()
        self.comp_title_input = QLineEdit(); self.comp_title_input.setPlaceholderText(tr("Composition Title"))
        top_row.addWidget(QLabel(tr("Title:"))); top_row.addWidget(self.comp_title_input)
        
        # Load Button moved to top row
        btn_load = QPushButton(tr("Load Text File")); btn_load.clicked.connect(self.load_comp_file)
        top_row.addWidget(btn_load)

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

        # Single Row for Controls
        cr = QHBoxLayout()

        # 1. Exclude & Filter
        btn_exclude = QPushButton(tr("Exclude Manuscripts")); btn_exclude.clicked.connect(self.open_exclude_dialog)
        btn_filter_text = QPushButton(tr("Filter Text")); btn_filter_text.clicked.connect(self.open_filter_dialog)
        self.lbl_exclude_status = QLabel(tr("Excluded: {}").format(0))
        self.lbl_exclude_status.setStyleSheet("color: #8e44ad; font-weight: bold;")
        self.lbl_comp_status = QLabel("")

        cr.addWidget(btn_exclude); cr.addWidget(btn_filter_text)
        cr.addWidget(self.lbl_exclude_status)
        cr.addWidget(self.lbl_comp_status)

        # 2. Parameters
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

        # Shortened Text
        self.chk_comp_flat = QCheckBox(tr("Sort by shelfmark only"))
        self.chk_comp_flat.setToolTip(tr("Disable Main/Appendix grouping"))
        self.chk_comp_flat.toggled.connect(self.on_comp_display_mode_changed)

        cr.addWidget(self.spin_chunk); cr.addWidget(self.spin_freq)
        cr.addWidget(self.comp_mode_combo); cr.addWidget(self.spin_filter); cr.addWidget(self.chk_comp_flat)

        # 3. Lab & Action
        self.btn_lab_mode_toggle_comp = QPushButton(tr("Lab Mode"))
        self.btn_lab_mode_toggle_comp.setCheckable(True)
        self.btn_lab_mode_toggle_comp.setToolTip(tr("Experimental search mode using advanced proximity scoring. WARNING: Can freeze the program. Use with caution."))
        self.btn_lab_mode_toggle_comp.toggled.connect(self.on_lab_mode_toggled_comp)

        self.chk_lab_deep_comp = QCheckBox(tr("Deep Scan"))
        self.chk_lab_deep_comp.setToolTip(tr("Slower but checks deeper. Use for common phrases/quotes"))
        self.chk_lab_deep_comp.setEnabled(False)
        self.chk_lab_deep_comp.toggled.connect(self.on_deep_scan_toggled_comp)

        # Shortened Text
        self.btn_comp_run = QPushButton(tr("Analyze")); self.btn_comp_run.clicked.connect(self.toggle_composition)
        self.btn_comp_run.setStyleSheet("background-color: #2980b9; color: white; font-weight: bold;")
        self.btn_comp_run.setEnabled(False)
        self.btn_comp_recursive = QPushButton(tr("Full Recursive Search")); self.btn_comp_recursive.clicked.connect(self.run_recursive_composition)
        self.btn_comp_recursive.setStyleSheet("background-color: #27ae60; color: white; font-weight: bold;")
        self.btn_comp_recursive.setEnabled(True)

        cr.addWidget(self.btn_lab_mode_toggle_comp)
        cr.addWidget(self.chk_lab_deep_comp)
        cr.addWidget(self.btn_comp_run)
        cr.addWidget(self.btn_comp_recursive)

        in_l.addLayout(cr)

        self.lab_panel_comp = LabPanel(self, 'comp')
        in_l.addWidget(self.lab_panel_comp)

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
        header.setSectionResizeMode(self.comp_col_ms_context, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Interactive) # Shelfmark
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents) # System ID

        self.comp_tree.setColumnWidth(0, 160) # Score - widened

        # Title column (~25 chars)
        title_width = self.comp_tree.fontMetrics().averageCharWidth() * 25
        self.comp_tree.setColumnWidth(2, int(title_width))
        context_width = self.comp_tree.fontMetrics().averageCharWidth() * 35
        self.comp_tree.setColumnWidth(self.comp_col_context, int(context_width))
        self.comp_tree.setColumnWidth(self.comp_col_ms_context, int(context_width))
        header.setStretchLastSection(True)

        self.comp_tree.itemDoubleClicked.connect(self.on_comp_item_double_clicked)
        self.comp_tree.itemExpanded.connect(self._on_comp_item_expanded)
        self.comp_tree.itemCollapsed.connect(self._on_comp_item_collapsed)

        # Use CheckBoxHeader for tree
        self.chk_comp_header = CheckBoxHeader(self.comp_tree)
        self.chk_comp_header.toggled.connect(self.on_comp_header_toggled)
        self.comp_tree.setHeader(self.chk_comp_header)
        comp_header = self.comp_tree.header()
        comp_header.setSectionResizeMode(self.comp_col_context, QHeaderView.ResizeMode.Interactive)
        comp_header.setSectionResizeMode(self.comp_col_ms_context, QHeaderView.ResizeMode.Stretch)
        comp_header.setSectionResizeMode(1, QHeaderView.ResizeMode.Interactive) # Shelfmark
        comp_header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents) # System ID
        comp_header.setStretchLastSection(True)

        rl.addWidget(self.comp_tree)
        
        exp_layout = QHBoxLayout()
        self.lbl_comp_export = QLabel(tr("Save Report"))
        exp_layout.addWidget(self.lbl_comp_export)
        
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
        panel = QWidget(); layout = QVBoxLayout(panel)
        
        # --- Top Area: Metadata (Gray Bar) ---
        top_container = QFrame();
        top_container.setFrameShape(QFrame.Shape.StyledPanel)
        
        top_layout = QVBoxLayout(top_container)
        top_layout.setContentsMargins(10, 5, 10, 5)

        # Row 1: Search Inputs
        row1 = QHBoxLayout()
        self.btn_prev_ms = QPushButton(tr("◀"))
        self.btn_prev_ms.setToolTip(tr("Previous Manuscript (File Order)"))
        self.btn_prev_ms.setFixedWidth(25)
        self.btn_prev_ms.clicked.connect(lambda: self.navigate_manuscript(-1))

        self.btn_next_ms = QPushButton(tr("▶"))
        self.btn_next_ms.setToolTip(tr("Next Manuscript (File Order)"))
        self.btn_next_ms.setFixedWidth(25)
        self.btn_next_ms.clicked.connect(lambda: self.navigate_manuscript(1))
        
        self.browse_sys_input = QLineEdit(); self.browse_sys_input.setPlaceholderText(tr("Enter System ID..."))
        self.browse_shelf_input = QLineEdit(); self.browse_shelf_input.setPlaceholderText(tr("Enter shelfmark..."))
        self.browse_fl_input = QLineEdit(); self.browse_fl_input.setPlaceholderText(tr("Enter FL ID..."))
        self.browse_fl_input.setFixedWidth(140)

        self.btn_browse_go = QPushButton(tr("Go")); self.btn_browse_go.setFixedWidth(50)
        self.btn_browse_go.clicked.connect(self.browse_load)
        self.btn_browse_go.setEnabled(False)
        
        self.browse_sys_input.returnPressed.connect(self.browse_load)
        self.browse_shelf_input.returnPressed.connect(self.browse_load)
        self.browse_fl_input.returnPressed.connect(self.browse_load)
        self.browse_sys_input.textEdited.connect(lambda _t: self._set_last_browse_field("sys"))
        self.browse_shelf_input.textEdited.connect(lambda _t: self._set_last_browse_field("shelf"))
        self.browse_fl_input.textEdited.connect(lambda _t: self._set_last_browse_field("fl"))
        
        self.btn_b_catalog = QPushButton(tr("Ktiv")); self.btn_b_catalog.setToolTip(tr("Open in Ktiv Website"))
        self.btn_b_catalog.clicked.connect(self.browse_open_catalog); self.btn_b_catalog.setEnabled(False)
        
        self.btn_b_save = QPushButton(tr("Save")); self.btn_b_save.setToolTip(tr("Save full manuscript to file"))
        self.btn_b_save.clicked.connect(self.browse_save_full); self.btn_b_save.setEnabled(False)
        
        # View All Button
        self.btn_b_all = QPushButton(tr("View All"))
        self.btn_b_all.setCheckable(True)
        self.btn_b_all.clicked.connect(self.toggle_browse_view_all)
        self.btn_b_all.setEnabled(False)
        
        row1.addWidget(self.btn_prev_ms)    
        row1.addWidget(QLabel(tr("System ID:")))        
        row1.addWidget(self.browse_sys_input)    
        row1.addWidget(QLabel(tr("Shelfmark:"))); row1.addWidget(self.browse_shelf_input)
        row1.addWidget(self.btn_next_ms)
        row1.addSpacing(10)
        row1.addWidget(QLabel(tr("FL:"))); row1.addWidget(self.browse_fl_input)
        row1.addWidget(self.btn_browse_go)
        row1.addSpacing(20)
        row1.addWidget(self.btn_b_all)
        row1.addWidget(self.btn_b_catalog)
        row1.addWidget(self.btn_b_save)
        row1.addStretch()

        # Help
        btn_browse_help = QPushButton("?")
        btn_browse_help.setFixedWidth(30)
        btn_browse_help.setStyleSheet("background-color: #f39c12; color: white; font-weight: bold; border-radius: 15px;")
        btn_browse_help.clicked.connect(lambda: self.open_help_center(anchor="browse"))
        row1.addWidget(btn_browse_help)

        top_layout.addLayout(row1)

        # Row 2: Metadata Display (Compact)
        self.browse_info_lbl = QLabel(tr("Enter ID to browse."))
        self.browse_info_lbl.setWordWrap(True)
        self.browse_info_lbl.setStyleSheet("font-size: 12px; color: #2c3e50;")
        self.browse_info_lbl.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        top_layout.addWidget(self.browse_info_lbl)

        layout.addWidget(top_container)

        # --- Main Splitter (Left: Text, Right: Images) ---
        self.browse_splitter = QSplitter(Qt.Orientation.Horizontal)

        # Left: Text Browser
        text_widget = QWidget(); text_layout = QVBoxLayout(text_widget); text_layout.setContentsMargins(0,0,0,0)

        # Navigation Bar (Above Text)
        nav_bar = QHBoxLayout()
        self.btn_b_prev = QPushButton(tr("<< Prev Page")); self.btn_b_prev.clicked.connect(lambda: self.browse_navigate(-1))
        self.btn_b_next = QPushButton(tr("Next Page >>")); self.btn_b_next.clicked.connect(lambda: self.browse_navigate(1))
        self.lbl_page_count = QLabel("0/0")

        self.btn_b_prev.setEnabled(False); self.btn_b_next.setEnabled(False)

        nav_bar.addWidget(self.btn_b_prev); nav_bar.addStretch(); nav_bar.addWidget(self.lbl_page_count); nav_bar.addStretch(); nav_bar.addWidget(self.btn_b_next)
        text_layout.addLayout(nav_bar)

        self.browse_text = QTextBrowser()
        self.browse_text.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self.browse_text.setFont(QFont("SBL Hebrew", 16))
        text_layout.addWidget(self.browse_text)
        
        # Right: Image Viewer
        self.browse_viewer = ManuscriptViewerWidget()

        self.browse_splitter.addWidget(text_widget)
        self.browse_splitter.addWidget(self.browse_viewer)
        self.browse_splitter.setStretchFactor(0, 1)
        self.browse_splitter.setStretchFactor(1, 1)

        layout.addWidget(self.browse_splitter, 1)

        # Dummy placeholders
        self.browse_thumb = QLabel()
        self.btn_b_ext = QPushButton()
        self.browse_side_panel = QTextBrowser()

        return panel

    def on_browse_enriched_loaded(self, meta):
        if not meta: return
        if self.current_browse_sid not in self.meta_mgr.nli_cache: return

        # 1. Update Info Label (Top Bar)
        marc = meta.get('marc', {})
        shelf = meta.get('shelfmark')
        title = meta.get('title')
        if shelf and shelf != "Unknown":
            library = marc.get('current_owner')
            if library: shelf = f"{library} | {shelf}"

        label_text = f"<b>{shelf or ''}</b>"
        if title: label_text += f" | {title}"
        if meta.get('physical_desc'): label_text += f" | {meta['physical_desc']}"
        self.browse_info_lbl.setText(label_text)

        # 2. Populate Image Viewer (using new logic)
        try: idx = int(self.current_browse_p) - 1
        except: idx = 0
        self.browse_viewer.load_images(meta, idx)

        # 3. Enable buttons
        self.btn_b_catalog.setEnabled(True)
        self.btn_b_save.setEnabled(True)
        self.btn_b_all.setEnabled(True)

        # 4. Trigger Page Load to show text (IMPORTANT)
        self.browse_load_page()

    def toggle_browse_view_all(self, checked):
        if checked:
            self.browse_viewer.setVisible(False)
            self.browse_load_all()
        else:
            self.browse_viewer.setVisible(True)
            self.browse_load_page()

    def _set_last_browse_field(self, field):
        self.last_browse_field = field

    def browse_load_page(self):
        """Load single page text and sync viewer."""
        if not self.current_browse_sid: return

        p = self.current_browse_p or 1
        page_data = self.searcher.get_browse_page(self.current_browse_sid, p_num=p)

        if not page_data:
            self.browse_text.setText(tr("Page not found."))
            return

        self.current_browse_p = page_data['p_num']

        # Update Nav
        self.btn_b_prev.setEnabled(page_data['current_idx'] > 1)
        self.btn_b_next.setEnabled(page_data['current_idx'] < page_data['total_pages'])
        self.lbl_page_count.setText(f"{page_data['current_idx']} / {page_data['total_pages']}")

        # Update Text
        raw_text = page_data['text']
        html = raw_text.replace("\n", "<br>")
        self.browse_text.setHtml(f"<div dir='rtl'>{html}</div>")

        # Sync Image Viewer
        try: idx = int(self.current_browse_p) - 1
        except: idx = 0
        self.browse_viewer.set_page(idx)

    def browse_load_all(self):
        """Load all pages into the text browser for continuous scrolling."""
        if not self.current_browse_sid: return
        
        self.browse_text.setText(tr("Loading full manuscript..."))
        QApplication.processEvents() # Refresh UI
        
        pages = self.searcher.get_full_manuscript(self.current_browse_sid)
        if not pages:
            QMessageBox.warning(self, tr("Error"), tr("Could not load full text."))
            return

        # Get enriched map if available
        meta = self.meta_mgr.nli_cache.get(self.current_browse_sid, {})
        canvas_map = meta.get('canvas_map', {})

        html_content = []
        for p in pages:
            # Anchor for scrolling
            anchor = f'<a name="page_{p["p_num"]}"></a>'
            
            # Visual Separator
            img_lbl = tr("Image")
            fl_id = p.get('fl_id')

            # Resolve Label
            fl_digits = re.sub(r"\D", "", str(fl_id or ""))
            label = canvas_map.get(fl_digits, "")

            fl_suffix = f" ({tr('FL')}: {fl_id})" if fl_id else ""
            label_suffix = f" - {label}" if label else ""

            separator = f"""
            <div style='background-color: #f0f0f0; color: #555; padding: 5px; margin-top: 20px; border-bottom: 2px solid #ccc;'>
                <b>{img_lbl}: {p['p_num']}{label_suffix}</b> <span style='font-size:0.8em'>{fl_suffix}</span>
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
            <p>This tool was developed with the coding assistance of <b>Gemini 3.0</b> and <b>GPT 5.1</b>. My thanks to Avi Shmidman, Elisha Rosenzweig, Ephraim Meiri, Elazar Gershuni, Itai Kagan, Elnatan Chen and Adiel Breuer for their advice and support.</p>

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
        if getattr(self, 'btn_lab_mode_toggle', None) and self.btn_lab_mode_toggle.isChecked():
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
        if getattr(self, 'btn_lab_mode_toggle', None) and self.btn_lab_mode_toggle.isChecked() and self.lab_engine:
            settings_dump = json.dumps({
                'custom_variants': self.lab_engine.settings.custom_variants,
                'candidate_limit': self.lab_engine.settings.candidate_limit,
                'min_should_match': self.lab_engine.settings.min_should_match,
                'gap_penalty': self.lab_engine.settings.gap_penalty,
                'ignore_matres': self.lab_engine.settings.ignore_matres,
                'phonetic_expansion': self.lab_engine.settings.phonetic_expansion,
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

    def update_lab_ui_state(self, checked):
        """Disable standard controls when Lab Mode is active."""
        # Search Tab
        if hasattr(self, 'mode_combo'): self.mode_combo.setEnabled(not checked)
        if hasattr(self, 'gap_input'): self.gap_input.setEnabled(not checked)
        if hasattr(self, 'btn_ai'): self.btn_ai.setEnabled(not checked)
        if hasattr(self, 'chk_lab_deep'): self.chk_lab_deep.setEnabled(checked)

        # Composition Tab
        if hasattr(self, 'comp_mode_combo'): self.comp_mode_combo.setEnabled(not checked)
        if hasattr(self, 'spin_freq'): self.spin_freq.setEnabled(not checked)
        if hasattr(self, 'chk_lab_deep_comp'): self.chk_lab_deep_comp.setEnabled(checked)

    def on_deep_scan_toggled_search(self, checked):
        if hasattr(self, 'chk_lab_deep_comp'):
            self.chk_lab_deep_comp.blockSignals(True)
            self.chk_lab_deep_comp.setChecked(checked)
            self.chk_lab_deep_comp.blockSignals(False)

    def on_deep_scan_toggled_comp(self, checked):
        if hasattr(self, 'chk_lab_deep'):
            self.chk_lab_deep.blockSignals(True)
            self.chk_lab_deep.setChecked(checked)
            self.chk_lab_deep.blockSignals(False)

    def on_lab_mode_toggled_search(self, checked):
        # Show/Hide Panel
        if hasattr(self, 'lab_panel_search'):
            self.lab_panel_search.setVisible(checked)

        # Sync Comp Button
        if hasattr(self, 'btn_lab_mode_toggle_comp'):
            self.btn_lab_mode_toggle_comp.blockSignals(True)
            self.btn_lab_mode_toggle_comp.setChecked(checked)
            self.btn_lab_mode_toggle_comp.blockSignals(False)
            # Ensure comp panel visibility matches too
            if hasattr(self, 'lab_panel_comp'):
                self.lab_panel_comp.setVisible(checked)

        self.update_lab_ui_state(checked)

    def on_lab_mode_toggled_comp(self, checked):
        # Show/Hide Panel
        if hasattr(self, 'lab_panel_comp'):
            self.lab_panel_comp.setVisible(checked)

        # Sync Search Button
        if hasattr(self, 'btn_lab_mode_toggle'):
            self.btn_lab_mode_toggle.blockSignals(True)
            self.btn_lab_mode_toggle.setChecked(checked)
            self.btn_lab_mode_toggle.blockSignals(False)
            # Ensure search panel visibility matches too
            if hasattr(self, 'lab_panel_search'):
                self.lab_panel_search.setVisible(checked)

        self.update_lab_ui_state(checked)

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

        if self.btn_lab_mode_toggle.isChecked():
            if not self.lab_engine:
                QMessageBox.warning(self, tr("Error"), tr("Lab Engine not initialized."))
                self.reset_ui()
                return

            deep = self.chk_lab_deep.isChecked()
            limit = self.lab_engine.settings.lab_scan_limit

            self.search_thread = LabSearchThread(self.lab_engine, query, mode, gap, deep_scan=deep, scan_limit=limit)
        else:
            self.search_thread = SearchThread(self.searcher, query, mode, gap)

        self.search_thread.results_signal.connect(self.on_search_finished)
        self.search_thread.progress_signal.connect(lambda c, t: (self.search_progress.setMaximum(t), self.search_progress.setValue(c)))

        if hasattr(self.search_thread, 'status_signal'):
             self.search_thread.status_signal.connect(self.status_label.setText)

        self.search_thread.error_signal.connect(self.on_error)
        self.search_thread.start()

    def stop_search(self):
        if self.search_thread.isRunning(): self.search_thread.terminate(); self.search_thread.wait()
        self.reset_ui()

    def reset_ui(self):
        self.is_searching = False; self.btn_search.setText(tr("Search")); self.btn_search.setStyleSheet("background-color: #27ae60; color: white;")
        self.search_progress.setVisible(False)

    def on_error(self, err): self.reset_ui(); QMessageBox.critical(self, tr("Error"), str(err))

    def render_asterisks_to_html(self, text):
        if not text: return ""
        # Escape HTML chars
        t = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        # Convert *word* to highlighted span
        t = re.sub(r'\*(.*?)\*', r'<span style="color:#ff0000; font-weight:bold;">\1</span>', t)
        return f"<div dir='rtl'>{t}</div>"

    def on_search_finished(self, results):
        self.reset_ui()
        # Reset Select All Checkbox
        self.chk_search_header.blockSignals(True)
        self.chk_search_header.setChecked(False)
        self.chk_search_header.blockSignals(False)
        self.lbl_search_export.setText(tr("Export Results") + ":")

        if not results:
            self.status_label.setText(tr("No results found."))
            self.last_results = []
            for b in self.export_buttons: b.setEnabled(False)
            self.results_table.setRowCount(0)
            self.result_row_by_sys_id = {}
            self.shelfmark_items_by_sid = {}
            self.title_items_by_sid = {}
            return

        self.last_results = results 

        # Display Limit Logic (Lab Mode)
        display_limit = len(results)
        if self.btn_lab_mode_toggle.isChecked() and self.lab_engine:
            display_limit = getattr(self.lab_engine.settings, 'lab_display_limit', 500)

        visible_count = min(len(results), display_limit)

        if visible_count < len(results):
            self.status_label.setText(tr("Showing top {} of {} results. (Export for full list)").format(visible_count, len(results)))
            self.status_label.setStyleSheet("color: #e67e22; font-weight: bold;")
        else:
            self.status_label.setText(tr("Found {} results. Loading metadata...").format(len(results)))
            self.status_label.setStyleSheet("color: black;")

        for b in self.export_buttons: b.setEnabled(True)
        self.results_table.setSortingEnabled(False) # Disable sorting during population
        self.results_table.setRowCount(visible_count)

        self.result_row_by_sys_id = {}
        self.shelfmark_items_by_sid = {}
        self.title_items_by_sid = {}
        self._res_map_by_sid = {r['display']['id']: r for r in results} # New: map for metadata updates

        ids = []
        for i, res in enumerate(results):
            meta = res['display']
            parsed = self.meta_mgr.parse_full_id_components(res['raw_header'])
            sid = parsed['sys_id'] or meta.get('id')

            # Metadata Collection (Always collect all IDs for export readiness)
            # Pull immediate metadata from CSV/cache
            shelf, title = self.meta_mgr.get_meta_for_id(sid)
            needs_fetch = (shelf == "Unknown" and (not title))
            if needs_fetch: ids.append(sid)

            # Table Population (Respect Display Limit)
            if i < visible_count:
                # Checkbox column
                item_chk = QTableWidgetItem()
                item_chk.setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
                item_chk.setCheckState(Qt.CheckState.Unchecked)
                # Store full result data here for retrieval after sort
                item_chk.setData(Qt.ItemDataRole.UserRole, res)
                self.results_table.setItem(i, self.COL_CHECKBOX, item_chk)

                # Actions column (ghost buttons container)
                actions_widget = QWidget()
                actions_layout = QHBoxLayout(actions_widget)
                actions_layout.setContentsMargins(2, 2, 2, 2)
                actions_layout.setSpacing(4)
                actions_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

                view_btn = self._create_action_button(
                    self.style().standardIcon(QStyle.StandardPixmap.SP_FileDialogDetailedView),
                    tr("View result"),
                    lambda _, r=res: self.show_full_text_for_result(r),
                )
                browse_btn = self._create_action_button(
                    self.style().standardIcon(QStyle.StandardPixmap.SP_DirOpenIcon),
                    tr("Browse manuscript"),
                    lambda _, r=res: self.open_result_in_browse_from_table(r),
                )

                actions_layout.addWidget(view_btn)
                actions_layout.addWidget(browse_btn)
                self.results_table.setCellWidget(i, self.COL_ACTIONS, actions_widget)

                # System ID column
                item_sid = QTableWidgetItem(sid)
                item_sid.setData(Qt.ItemDataRole.UserRole, res)
                self.results_table.setItem(i, self.COL_SYS_ID, item_sid)

                if needs_fetch:
                    item_shelf = ShelfmarkTableWidgetItem(tr("Loading..."))
                    item_title = QTableWidgetItem(tr("Loading..."))
                else:
                    item_shelf = ShelfmarkTableWidgetItem(shelf if shelf else tr("Unknown"))
                    item_title = QTableWidgetItem(title if title else "")

                # Shelfmark column
                self.results_table.setItem(i, self.COL_SHELF, item_shelf)
                self.shelfmark_items_by_sid[sid] = item_shelf

                # Title column
                self.results_table.setItem(i, self.COL_TITLE, item_title)
                self.title_items_by_sid[sid] = item_title

                # Snippet column (Widget)
                # Render asterisks to HTML for display
                html_snippet = self.render_asterisks_to_html(res['snippet'])
                lbl = QLabel(html_snippet); lbl.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
                self.results_table.setCellWidget(i, self.COL_SNIPPET, lbl)

                # Col 5: Img
                self.results_table.setItem(i, self.COL_IMG, QTableWidgetItem(meta['img']))

                # Col 6: Source
                self.results_table.setItem(i, self.COL_SRC, QTableWidgetItem(meta['source']))

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
            return

        self.meta_loader = ShelfmarkLoaderThread(self.meta_mgr, ids)
        self.meta_loader.progress_signal.connect(self.on_meta_progress)
        self.meta_loader.finished_signal.connect(self.on_meta_finished)
        self.meta_loader.error_signal.connect(lambda err: QMessageBox.critical(self, tr("Metadata Error"), err))
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
        self.meta_loader = None

    def _format_metadata_status(self):
        total_expected = self.meta_cached_count + self.meta_to_fetch_count
        total_loaded = self.meta_cached_count + self.meta_progress_current
        progress_part = ""
        if self.meta_to_fetch_count:
            progress_part = f" ({self.meta_progress_current}/{self.meta_to_fetch_count})"
        return tr("Metadata loaded: {}/{}").format(total_loaded, total_expected) + progress_part

    def _create_action_button(self, icon, tooltip, callback):
        btn = QToolButton(self.results_table)
        btn.setIcon(icon)
        btn.setToolTip(tooltip)
        btn.setAutoRaise(True)
        btn.setCursor(Qt.CursorShape.PointingHandCursor)
        btn.setFixedSize(28, 24)
        btn.clicked.connect(callback)
        return btn

    def _collect_sorted_results(self):
        sorted_results = []
        rows = self.results_table.rowCount()
        for i in range(rows):
            item = self.results_table.item(i, self.COL_SYS_ID)
            if item:
                res = item.data(Qt.ItemDataRole.UserRole)
                if res:
                    sorted_results.append(res)
        if not sorted_results:
            sorted_results = self.last_results
        return sorted_results

    def _extract_fl_id(self, res):
        if not isinstance(res, dict):
            return None
        display = res.get('display', {}) or {}
        raw_header = res.get('raw_header') or res.get('full_header', '')
        fl_id = None

        if raw_header and self.meta_mgr:
            parsed = self.meta_mgr.parse_full_id_components(raw_header)
            fl_id = parsed.get('fl_id') or fl_id

        if not fl_id:
            fl_id = display.get('fl_id')

        if not fl_id:
            img_field = display.get('img')
            if img_field:
                m = re.search(r'FL\\s*-?(\\d+)', str(img_field))
                if m:
                    fl_id = m.group(1)

        if not fl_id:
            uid_field = res.get('uid')
            if uid_field:
                m = re.search(r'FL\\s*-?(\\d+)', str(uid_field))
                if m:
                    fl_id = m.group(1)

        return fl_id

    def show_full_text(self):
        row = self.results_table.currentRow()
        if row < 0: return

        sorted_results = self._collect_sorted_results()
        if not sorted_results:
            return
        if row >= len(sorted_results):
            row = 0
        ResultDialog(self, sorted_results, row, self.meta_mgr, self.searcher).exec()

    def show_full_text_for_result(self, res):
        if not res:
            return
        sorted_results = self._collect_sorted_results()
        if not sorted_results:
            sorted_results = [res]
        target_index = 0
        for idx, candidate in enumerate(sorted_results):
            if candidate is res:
                target_index = idx
                break
            cand_id = candidate.get('display', {}).get('id')
            if cand_id and cand_id == res.get('display', {}).get('id'):
                target_index = idx
        ResultDialog(self, sorted_results, target_index, self.meta_mgr, self.searcher).exec()

    def open_result_in_browse_from_table(self, res):
        if not res:
            return
        display = res.get('display', {})
        sid = display.get('id')
        shelf = ""
        title = ""
        if sid:
            shelf, title = self.meta_mgr.get_meta_for_id(sid)
        if not shelf:
            shelf = display.get('shelfmark', '')
        if not title:
            title = display.get('title', '')
        fl_id = self._extract_fl_id(res)
        self.open_result_in_browse(res, shelfmark=shelf, title=title, fl_id=fl_id)

    def on_search_select_all_toggled(self, checked):
        """Handle Select All checkbox toggle."""
        state = Qt.CheckState.Checked if checked else Qt.CheckState.Unchecked
        self.results_table.blockSignals(True)
        for i in range(self.results_table.rowCount()):
            item = self.results_table.item(i, self.COL_CHECKBOX)
            if item:
                item.setCheckState(state)
        self.results_table.blockSignals(False)
        self._update_search_export_label()

    def on_search_result_item_changed(self, item):
        """Handle individual checkbox changes in search results."""
        if item.column() != self.COL_CHECKBOX:
            return

        # Check if all items are checked to sync "Select All"
        all_checked = True
        has_selection = False

        # Avoid full iteration if possible, but we need to check "Select All" status
        rows = self.results_table.rowCount()
        # To optimize, we just iterate. Rows are usually < 500.
        for i in range(rows):
            it = self.results_table.item(i, self.COL_CHECKBOX)
            if it:
                if it.checkState() == Qt.CheckState.Unchecked:
                    all_checked = False
                else:
                    has_selection = True

        # If no items, all_checked is trivially true but we don't want to check the box
        if rows == 0:
            all_checked = False

        self.chk_search_header.blockSignals(True)
        self.chk_search_header.setChecked(all_checked)
        self.chk_search_header.blockSignals(False)

        self._update_search_export_label(has_selection)

    def _update_search_export_label(self, has_selection=None):
        if has_selection is None:
            # Check if any selected
            has_selection = False
            for i in range(self.results_table.rowCount()):
                it = self.results_table.item(i, self.COL_CHECKBOX)
                if it and it.checkState() == Qt.CheckState.Checked:
                    has_selection = True
                    break

        if has_selection:
            self.lbl_search_export.setText(tr("Export selected results") + ":")
        else:
            self.lbl_search_export.setText(tr("Export Results") + ":")

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

        derived_fl_id = fl_id or self._extract_fl_id(res)
        if shelfmark:
            info_text = f"<b>{shelfmark}</b>"
            if title:
                info_text += f"<br>{title}"
            self.browse_info_lbl.setText(info_text)
        if derived_fl_id:
            fl_digits = re.sub(r"\\D", "", str(derived_fl_id))
            self.browse_fl_input.setText(f"FL{fl_digits}" if fl_digits else str(derived_fl_id))
            self._set_last_browse_field("fl")
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
            
        sys_id = None
        raw_header = res.get('raw_header') or res.get('full_header', '')
        
        # 1. Try to parse strictly 99... from header
        if raw_header and self.meta_mgr:
            parsed_sid, _ = self.meta_mgr.parse_header_smart(raw_header)
            sys_id = parsed_sid
            
        # 2. Fallback to existing display ID if parsing failed
        if not sys_id:
            sys_id = res['display'].get('id')
        # ------------------------------------------------------

        if sys_id:
            entries = list(self.excluded_raw_entries)
            # Add only if not already present
            if sys_id not in entries:
                entries.append(sys_id)
                self.set_excluded_entries("\n".join(entries))
                
        self.tabs.setCurrentWidget(self.composition_tab)
        self.comp_text_area.setFocus()

    def _sanitize_for_excel(self, text):
        """Cleans text to prevent Excel XML corruption."""
        if text is None: return ""
        t = str(text)
        # Remove illegal characters
        t = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', '', t)

        # Handle malicious formulas
        t = t.strip()
        if t.startswith(('=', '+', '-', '@')):
            t = "'" + t

        # Excel cell limit
        if len(t) > 32700:
            t = t[:32700] + "..."
        return t

    def export_results(self, fmt='xlsx'):
        """
        Export results handling specific formats directly.
        fmt: 'xlsx', 'csv', or 'txt'
        """
        base_path = self._default_report_path(self.last_search_query, tr("Search_Results"))
        default_path = os.path.splitext(base_path)[0] + f".{fmt}"

        filters = {'xlsx': "Excel (*.xlsx)", 'csv': "CSV (*.csv)", 'txt': "Text (*.txt)"}
        selected_filter = filters.get(fmt, "All Files (*.*)")

        path, _ = QFileDialog.getSaveFileName(self, tr("Export Results"), default_path, selected_filter)
        if not path: return

        # Prepare tabular data
        headers = ["System ID", "Shelfmark", "Title", "Image/Page", "Source", "Snippet"]
        data_rows = []

        # Collect results to export (Selected or All)
        results_to_export = []

        # Check if any are selected in the table
        has_selection = False
        selected_rows_data = []

        # Iterate table to respect user selection and visual order
        rows = self.results_table.rowCount()
        for i in range(rows):
            chk_item = self.results_table.item(i, self.COL_CHECKBOX)
            if chk_item and chk_item.checkState() == Qt.CheckState.Checked:
                has_selection = True
                res = chk_item.data(Qt.ItemDataRole.UserRole)
                if res:
                    selected_rows_data.append(res)

        if has_selection:
            results_to_export = selected_rows_data
        else:
            # Fallback to last_results (original order) if nothing selected
            results_to_export = self.last_results

        for r in results_to_export:
            d = r['display']
            sid = d.get('id', '')

            # Fetch fresh metadata (Important for Lab Mode)
            shelf, title = self.meta_mgr.get_meta_for_id(sid)
            if not shelf or shelf == "Unknown":
                shelf = d.get('shelfmark', '')
            if not title:
                title = d.get('title', '')

            # Use raw_file_hl so highlight markers remain intact
            # Clean snippet: remove newlines (input is now clean text with asterisks)
            raw_hl = r.get('raw_file_hl', '')
            snippet = str(raw_hl).strip().replace('\n', ' ').replace('\r', ' ')
            snippet = re.sub(r'\s+', ' ', snippet)

            data_rows.append([
                sid,
                shelf,
                title,
                str(d.get('img', '')),
                d.get('source', ''),
                snippet
            ])

        credit_text = self._get_credit_header()

        # --- XLSX with inline highlighting ---
        if fmt == 'xlsx':
            try:
                wb = openpyxl.Workbook()
                ws = wb.active
                ws.title = "Genizah Results"
                ws.sheet_view.rightToLeft = True

                # Fonts used for rich text snippets
                font_red = InlineFont(color='FF0000', b=True)
                font_normal = InlineFont(color='000000', b=False)

                # Helper to write rich text cells
                def write_rich_cell(row, col, text):
                    safe_text = self._sanitize_for_excel(text)

                    if '*' not in safe_text:
                        ws.cell(row=row, column=col, value=safe_text)
                        return

                    # Split by asterisk markers
                    parts = safe_text.split('*')
                    rich_string = CellRichText()

                    for i, part in enumerate(parts):
                        if not part:
                            continue
                        # Odd indices represent highlighted text
                        if i % 2 == 1:
                            rich_string.append(TextBlock(font_red, part))
                        else:
                            # Even indices are plain text
                            rich_string.append(TextBlock(font_normal, part))

                    ws.cell(row=row, column=col, value=rich_string)

                # Credit header
                current_row = 1
                for line in credit_text.split('\n'):
                    if not line.strip(): continue
                    cell = ws.cell(row=current_row, column=1, value=self._sanitize_for_excel(line))
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
                            # Strip markers/HTML in other columns
                            clean_val = val_str.replace('*', '')
                            ws.cell(row=current_row, column=col_idx, value=self._sanitize_for_excel(clean_val))

                    current_row += 1

                # Column widths
                ws.column_dimensions['A'].width = 15
                ws.column_dimensions['B'].width = 20
                ws.column_dimensions['C'].width = 40
                ws.column_dimensions['F'].width = 80  # Wider snippet column

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
                        # Strip highlight markers for CSV
                        clean_row = [str(val).replace('*', '') for val in row]
                        writer.writerow(clean_row)
                QMessageBox.information(self, tr("Saved"), tr("Saved to {}").format(path))
            except Exception as e:
                QMessageBox.critical(self, tr("Error"), f"Failed to save CSV:\n{str(e)}")

        # --- TXT ---
        else:
            try:
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(credit_text)
                    for r in results_to_export:
                        # Clean snippet: remove newlines for single-line export
                        snippet = r.get('raw_file_hl', '').strip().replace('\n', ' ').replace('\r', '')
                        f.write(f"=== {r['display']['shelfmark']} | {r['display']['title']} ===\n{snippet}\n\n")
                QMessageBox.information(self, tr("Saved"), tr("Saved to {}").format(path))
            except Exception as e:
                QMessageBox.critical(self, tr("Error"), f"Failed to save TXT:\n{str(e)}")

    def export_comp_report(self, fmt='xlsx'):
        # 1. איסוף נתונים (לוגיקה יציבה)

        # Check for Selection
        has_selection = bool(self._collect_checked_comp_page_uids())

        if has_selection:
            # Reconstruct lists from tree selection
            c_main, c_appx, c_filt, c_filt_appx, c_known = self._collect_checked_comp_items_struct()
        else:
            # Use all data
            c_main = self.comp_main
            c_appx = self.comp_appendix
            c_filt = self.comp_filtered_main
            c_filt_appx = self.comp_filtered_appendix
            c_known = self.comp_known

        all_filtered = c_filt[:]
        for v in c_filt_appx.values():
            all_filtered.extend(v)

        if not (c_main or c_appx or c_known or all_filtered):
            QMessageBox.warning(self, tr("Save"), tr("No composition data to export."))
            return

        # 2. טעינת מטא-דאטה
        all_ids = []
        def collect_ids(item_list):
            for item in item_list:
                if item.get('type') == 'manuscript' and item.get('sys_id'):
                    all_ids.append(item['sys_id'])
                else:
                    sid, _ = self.meta_mgr.parse_header_smart(item.get('raw_header', ''))
                    if sid: all_ids.append(sid)

        collect_ids(c_main)
        for group_items in c_appx.values(): collect_ids(group_items)
        collect_ids(c_known)
        collect_ids(all_filtered)

        unique_ids = list(set(all_ids))
        if unique_ids:
            missing = [uid for uid in unique_ids if uid not in self.meta_mgr.nli_cache]
            if missing:
                self._fetch_metadata_with_dialog(missing, title=tr("Fetching metadata before export..."))

        # 3. שמירה
        comp_title = self.comp_title_input.text().strip() or tr("Untitled Composition")
        base_path = self._default_report_path(comp_title, tr("Composition_Report"))
        default_path = os.path.splitext(base_path)[0] + f".{fmt}"
        
        filters = {'xlsx': "Excel (*.xlsx)", 'csv': "CSV (*.csv)", 'txt': "Text (*.txt)"}
        selected_filter = filters.get(fmt, "All Files (*.*)")

        path, _ = QFileDialog.getSaveFileName(self, tr("Save Report"), default_path, selected_filter)
        if not path: return

        credit_text = self._get_credit_header()

        illegal_chars_re = re.compile(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]')

        def sanitize_for_excel(text):
            """Cleans text to prevent Excel XML corruption."""
            if text is None: return ""
            t = str(text)
            
            t = illegal_chars_re.sub('', t)
            
            t = t.strip()
            if t.startswith(('=', '+', '-', '@')): 
                t = "'" + t
            
            # Excel cell limit
            if len(t) > 32700:
                t = t[:32700] + "..."
            
            return t

        def _clean_and_marker(text):
            """Prepares HTML for export: converts spans to *, removes other tags."""
            t = str(text or "")
            # 1. Convert Spans to Markers (Handle content inside)
            t = re.sub(r"<span[^>]*>(.*?)</span>", r"*\1*", t, flags=re.DOTALL)

            # 2. Remove newlines and BR
            t = t.replace("<br>", " ").replace("<br/>", " ").replace("\n", " ").replace("\r", " ")

            # 3. Remove any remaining HTML tags (div, etc)
            t = re.sub(r'<[^>]+>', '', t)

            # 4. Collapse multiple spaces
            t = re.sub(r'\s+', ' ', t)

            # 5. Merge adjacent asterisks
            t = re.sub(r'\*(\s+)\*', r'\1', t)

            return t.strip()

        # ==========================================
        #  XLSX & CSV Logic
        # ==========================================
        if fmt in ['xlsx', 'csv']:
            table_rows = []
            
            def add_rows(items, category, group_name=""):
                for ms_item in items:
                    if ms_item.get('type') == 'manuscript':
                        sid = ms_item['sys_id']
                        shelf, title = self.meta_mgr.get_meta_for_id(sid)
                        if not shelf or shelf == "Unknown":
                             shelf = self.meta_mgr.get_shelfmark_from_header(ms_item.get('raw_header', ''))
                        
                        ms_score = ms_item.get('score', 0)

                        for page in ms_item.get('pages', []):
                             _, p_num, _, _ = self._get_meta_for_header(page['raw_header'])
                             
                             src_clean = _clean_and_marker(page.get('source_ctx', ''))
                             ms_clean = _clean_and_marker(page.get('text', ''))
                             
                             table_rows.append([
                                category,
                                group_name,
                                sid or "",
                                shelf or "",
                                title or "",
                                str(p_num or ""),
                                f"{ms_score} (P:{page.get('score',0)})", 
                                src_clean,
                                ms_clean
                             ])
                    else:
                        # Fallback
                        sid, p_num, shelf, title = self._get_meta_for_header(ms_item.get('raw_header', ''))
                        src_clean = _clean_and_marker(ms_item.get('source_ctx', ''))
                        ms_clean = _clean_and_marker(ms_item.get('text', ''))
                        
                        table_rows.append([
                            category,
                            group_name,
                            sid or "",
                            shelf or "",
                            title or "",
                            str(p_num or ""),
                            str(ms_item.get('score', 0)),
                            src_clean,
                            ms_clean
                        ])

            if self.chk_comp_flat.isChecked():
                all_items = self._collect_comp_items(
                    c_main, c_appx,
                    c_filt, c_filt_appx,
                    c_known
                )
                flat_items = self._sort_comp_items(all_items)
                add_rows(flat_items, tr("All Results"))
            else:
                add_rows(c_main, "Main Manuscripts")
                for sig, items in sorted(c_appx.items(), key=lambda x: len(x[1]), reverse=True):
                    add_rows(items, "Appendix", sig)
                add_rows(c_filt, "Filtered Main")
                for sig, items in sorted(c_filt_appx.items(), key=lambda x: len(x[1]), reverse=True):
                    add_rows(items, "Filtered Appendix", sig)
                add_rows(c_known, "Excluded Manuscripts")

            # --- XLSX ---
            if fmt == 'xlsx':
                try:
                    wb = openpyxl.Workbook()
                    ws = wb.active
                    ws.title = "Composition Report"
                    ws.sheet_view.rightToLeft = True

                    font_red = InlineFont(color='FF0000', b=True)
                    font_normal = InlineFont(color='000000', b=False)

                    def write_rich_cell(row, col, text):
                        safe_text = sanitize_for_excel(text)
                        
                        if '*' not in safe_text:
                            ws.cell(row=row, column=col, value=safe_text)
                            return
                        
                        parts = safe_text.split('*')
                        rich_string = CellRichText()
                        
                        for i, part in enumerate(parts):
                            if not part:
                                continue
                            if i % 2 == 1:
                                rich_string.append(TextBlock(font_red, part))
                            else:
                                rich_string.append(TextBlock(font_normal, part))
                                
                        ws.cell(row=row, column=col, value=rich_string)

                    curr_row = 1
                    for line in credit_text.split('\n'):
                        clean_line = line.strip()
                        if not clean_line or "====" in clean_line: continue
                        
                        c = ws.cell(row=curr_row, column=1, value=sanitize_for_excel(line))
                        c.font = Font(bold=True, color="555555")
                        curr_row += 1
                    curr_row += 1

                    headers = ["Category", "Group", "System ID", "Shelfmark", "Title", "Image", "Score", "Source Context", "Manuscript Text"]
                    for idx, h in enumerate(headers, 1):
                        c = ws.cell(row=curr_row, column=idx, value=h)
                        c.font = Font(bold=True, color="FFFFFF")
                        c.fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
                    curr_row += 1

                    for row_data in table_rows:
                        for idx, val in enumerate(row_data, 1):
                            val_str = str(val)
                            # Apply rich text to Source Context (8) and Manuscript Text (9)
                            if idx in (8, 9):
                                write_rich_cell(curr_row, idx, val_str)
                            else: 
                                ws.cell(row=curr_row, column=idx, value=sanitize_for_excel(val_str))
                        curr_row += 1

                    dims = {'D': 20, 'E': 30, 'H': 50, 'I': 60}
                    for col, width in dims.items():
                        ws.column_dimensions[col].width = width

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
                            clean_row = [str(val).replace('*', '') for val in row]
                            writer.writerow(clean_row)
                    QMessageBox.information(self, tr("Saved"), tr("Saved to {}").format(path))
                except Exception as e:
                    QMessageBox.critical(self, tr("Error"), f"Failed to save CSV:\n{e}")

        # --- TXT ---
        else:
            try:
                sep = "=" * 80
                appendix_count = sum(len(v) for v in c_appx.values())
                filtered_total = len(c_filt) + sum(len(v) for v in c_filt_appx.values())
                known_count = len(c_known)
                total_count = len(c_main) + appendix_count + known_count + filtered_total

                def _fmt_ms_entry(ms_item):
                    if ms_item.get('type') == 'manuscript':
                        sid = ms_item['sys_id']
                        shelf, title = self.meta_mgr.get_meta_for_id(sid)
                        if not shelf or shelf == "Unknown":
                            shelf = self.meta_mgr.get_shelfmark_from_header(ms_item.get('raw_header', ''))

                        ms_block = [sep, f"MANUSCRIPT: {shelf} | {title} (ID: {sid}) | Total Score: {ms_item.get('score', 0)}", sep]

                        for page in ms_item.get('pages', []):
                             _, p_num, _, _ = self._get_meta_for_header(page['raw_header'])
                             src_clean = _clean_and_marker(page.get('source_ctx', ''))
                             ms_clean = _clean_and_marker(page.get('text', ''))
                             ms_block.append(f"\n--- Page {p_num} (Score: {page.get('score',0)}) ---")
                             ms_block.append(tr("Source Context") + ":\n" + src_clean)
                             ms_block.append(tr("Manuscript") + ":\n" + ms_clean)
                        return ms_block
                    else:
                        return self._fmt_item_legacy(ms_item)

                summary_lines = [
                    sep, tr("COMPOSITION REPORT SUMMARY"), sep,
                    f"Title: {comp_title}",
                    f"{tr('Total Manuscripts Found')}: {total_count}"
                ]
                detail_lines = [sep, tr("ALL RESULTS"), sep]

                if self.chk_comp_flat.isChecked():
                    flat_items = self._sort_comp_items(
                        self._collect_comp_items(c_main, c_appx, c_filt, c_filt_appx, c_known)
                    )
                    for item in flat_items: detail_lines.extend(_fmt_ms_entry(item))
                else:
                    summary_lines.extend([
                        f"{tr('Main Manuscripts')}: {len(c_main)}",
                        f"{tr('Main Appendix (Groups)')}: {len(c_appx)}",
                        f"{tr('Filtered by Text (Manuscripts)')}: {filtered_total}",
                        f"{tr('Excluded Manuscripts')}: {known_count}"
                    ])
                    detail_lines = [sep, tr("MAIN MANUSCRIPTS"), sep]
                    for item in c_main: detail_lines.extend(_fmt_ms_entry(item))
                    if c_appx:
                        detail_lines.extend([sep, tr("MAIN APPENDIX") + " (Grouped)", sep])
                        for sig, items in sorted(c_appx.items(), key=lambda x: len(x[1]), reverse=True):
                            detail_lines.append(f"=== GROUP: {sig} ({len(items)} items) ===")
                            for item in items: detail_lines.extend(_fmt_ms_entry(item))
                    if c_filt:
                        detail_lines.extend([sep, tr("FILTERED"), sep])
                        for item in c_filt: detail_lines.extend(_fmt_ms_entry(item))
                    if c_filt_appx:
                        detail_lines.extend([sep, tr("FILTERED APPENDIX") + " (Grouped)", sep])
                        for sig, items in sorted(c_filt_appx.items(), key=lambda x: len(x[1]), reverse=True):
                            detail_lines.append(f"=== GROUP: {sig} ({len(items)} items) ===")
                            for item in items: detail_lines.extend(_fmt_ms_entry(item))
                    if c_known:
                        detail_lines.extend([sep, tr("EXCLUDED MANUSCRIPTS"), sep])
                        for item in c_known: detail_lines.extend(_fmt_ms_entry(item))

                with open(path, 'w', encoding='utf-8') as f:
                    f.write(credit_text)
                    all_lines = summary_lines + detail_lines
                    f.write("\n".join(all_lines).strip() + "\n")
                QMessageBox.information(self, tr("Saved"), tr("Saved to {}").format(path))

            except Exception as e:
                QMessageBox.critical(self, tr("Error"), f"Failed to save TXT:\n{e}")
                
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

    def _normalize_shelfmark(self, shelfmark: str) -> str:
        """Normalize shelfmarks: remove ALL non-alphanumeric chars (spaces, dots, etc)."""
        if not shelfmark:
            return ""
        
        cleaned = re.sub(r'\W+', '', shelfmark).casefold()
        
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
        """
        Main entry point for Composition Search.
        FINAL VERIFIED VERSION.
        """
        txt = (custom_text if custom_text is not None else self.comp_text_area.toPlainText()).strip()
        if not txt:
            QMessageBox.warning(self, tr("Error"), tr("Please enter text to search."))
            return

        self.is_comp_running = True
        self.btn_comp_run.setText(tr("Stop"))
        self.btn_comp_run.setStyleSheet("background-color: #c0392b; color: white;")
        
        if hasattr(self, 'btn_comp_recursive'):
            self.btn_comp_recursive.setEnabled(False)
            
        self.comp_progress.setVisible(True)
        self.comp_progress.setRange(0, 0)
        self.comp_progress.setValue(0)
        self.comp_tree.clear()
        self.comp_progress.setFormat(tr("Scanning chunks..."))
        
        # איפוס נתונים
        self.comp_raw_items = []
        self.comp_filtered = []
        self.comp_known = []
        
        for b in self.comp_export_buttons: b.setEnabled(False)

        chunk_size = self.spin_chunk.value()
        
        # מיפוי מצב חיפוש
        available_modes = ['literal', 'variants', 'variants_extended', 'variants_maximum', 'fuzzy']
        idx = self.comp_mode_combo.currentIndex()
        if 0 <= idx < len(available_modes):
            mode = available_modes[idx]
        else:
            mode = 'variants'

        excluded_ids = self.excluded_raw_entries

        # 1. נתיב מעבדה (LAB MODE)
        if self.btn_lab_mode_toggle_comp.isChecked():
            if not self.lab_engine:
                QMessageBox.warning(self, tr("Error"), tr("Lab Engine not initialized."))
                self.reset_comp_ui()
                return

            # Robustness: Pass resolved System IDs if available, to catch items excluded by shelfmark
            # where the user didn't explicitly type the ID.
            final_excluded_ids = excluded_ids
            if self.excluded_sys_ids:
                final_excluded_ids = list(self.excluded_sys_ids)

            deep = self.chk_lab_deep_comp.isChecked()
            limit = self.lab_engine.settings.lab_scan_limit

            self.comp_thread = LabCompositionThread(
                self.lab_engine, 
                txt, 
                mode, 
                chunk_size=chunk_size,
                excluded_ids=final_excluded_ids,
                filter_text=self.filter_text_content,
                deep_scan=deep,
                scan_limit=limit
            )
            self.comp_thread.scan_finished_signal.connect(self.on_comp_scan_finished)

        # 2. נתיב רגיל (STANDARD MODE)
        else:
            if not self.searcher:
                QMessageBox.warning(self, tr("Error"), tr("Search engine not loaded."))
                self.reset_comp_ui()
                return

            # --- התיקון הקריטי כאן: הסרת progress_callback ---
            self.comp_thread = CompositionThread(
                self.searcher, 
                txt, 
                chunk=chunk_size,
                freq=self.spin_freq.value(),
                mode=mode,
                filter_text=self.filter_text_content,
                threshold=self.spin_filter.value()
            )
            if hasattr(self.comp_thread, 'scan_finished_signal'):
                 self.comp_thread.scan_finished_signal.connect(self.on_comp_scan_finished)
            else:
                 self.comp_thread.finished_signal.connect(self.on_comp_search_finished)

        self.comp_thread.progress_signal.connect(self.on_comp_progress)
        
        if hasattr(self.comp_thread, 'status_signal'):
             self.comp_thread.status_signal.connect(self.on_comp_status_update)
             
        self.comp_thread.error_signal.connect(self.on_comp_error)
        
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

    def on_comp_status_update(self, status):
        self.comp_progress.setFormat(status)
        if status.startswith("Scanning batch") or status.startswith("Scanning items"):
            self.comp_progress.setRange(0, 0)

    def on_comp_progress(self, curr, total):
        if total:
            self.comp_progress.setRange(0, total)
        else:
            self.comp_progress.setRange(0, 0)
        self.comp_progress.setValue(curr)

    def on_comp_error(self, err):
        """Handle errors during composition search."""
        self.reset_comp_ui()
        QMessageBox.critical(self, tr("Error"), str(err))
        
    def on_comp_scan_finished(self, result_obj):
        self.is_comp_running = False
        self.reset_comp_ui()

        known_raw = []
        if isinstance(result_obj, dict):
            items = result_obj.get('main', [])
            filtered_items = result_obj.get('filtered', [])
            known_raw = result_obj.get('known', []) 
        else:
            items = result_obj or []
            filtered_items = []

        manuscripts = self.searcher.group_pages_by_manuscript(items)
        filtered_manuscripts = self.searcher.group_pages_by_manuscript(filtered_items)
        known_manuscripts = self.searcher.group_pages_by_manuscript(known_raw)

        self.comp_raw_items = manuscripts
        self.comp_raw_filtered = filtered_manuscripts
        
        self.comp_known = known_manuscripts 

        if not manuscripts and not filtered_manuscripts and not known_manuscripts:
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
        
        final_main, final_appx, manual_known = self._apply_manual_exclusions(main_res, main_appx)
        
        self.comp_grouped_main = final_main or []
        self.comp_grouped_appendix = final_appx or {}
        self.comp_grouped_summary = main_summ or {} 
        
        if manual_known:
            if not self.comp_known:
                self.comp_known = []
            self.comp_known.extend(manual_known)
            
        self.comp_grouped_filtered_main = filt_res or []
        self.comp_grouped_filtered_appendix = filt_appx or {}
        self.comp_grouped_filtered_summary = filt_summ or {}
        
        self.display_comp_results(
            self.comp_grouped_main, 
            self.comp_grouped_appendix, 
            self.comp_grouped_summary, 
            filt_res, 
            filt_appx, 
            filt_summ
        )

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

    def _comp_sort_key(self, item, mode=None):
        sort_mode = mode or self._current_comp_sort_mode()
        if sort_mode == "score":
            return item.get('score', 0)

        sid, shelf, title = self._get_comp_item_meta(item)

        if sort_mode == "title":
            return title.casefold()
        if sort_mode == "system_id":
            return sid.casefold()

        shelf_key = natural_sort_key(shelf or sid)
        sid_key = natural_sort_key(sid)
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

    def _set_comp_node_previews(self, node, source_text, ms_text, highlight_pattern=None):
        if highlight_pattern and source_text:
            try:
                # Apply highlighting to Source Text if pattern exists
                regex = re.compile(highlight_pattern, re.IGNORECASE)
                # Only apply if not already highlighted (simple check)
                if '*' not in source_text:
                    source_text = regex.sub(r'*\g<0>*', source_text)
            except Exception:
                pass

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
        # 1. איפוס וניקוי
        self.is_comp_running = False
        self.btn_comp_run.setText(tr("Analyze Composition"))
        self.btn_comp_run.setStyleSheet("background-color: #2980b9; color: white;")
        self.comp_progress.setVisible(False)
        for b in self.comp_export_buttons: b.setEnabled(True)

        # Reset Select All Checkbox
        self.chk_comp_header.blockSignals(True)
        self.chk_comp_header.setChecked(False)
        self.chk_comp_header.blockSignals(False)
        self._update_comp_export_label()

        if getattr(self, 'group_thread', None):
            self.group_thread.wait()
        self.group_thread = None

        self.comp_raw_items = main_res
        self.comp_raw_filtered = filt_res

        clean_main, clean_appx, known_main = self._apply_manual_exclusions(main_res, main_appx)
        clean_filt, clean_filt_appx, known_filt = self._apply_manual_exclusions(filt_res, filt_appx)
        
        if not hasattr(self, 'comp_known'): self.comp_known = []
        self.comp_known.extend(known_main + known_filt)

        self.comp_main = clean_main
        self.comp_appendix = clean_appx
        self.comp_summary = main_summ
        self.comp_filtered_main = clean_filt
        self.comp_filtered_appendix = clean_filt_appx
        self.comp_filtered_summary = filt_summ
        
        self.comp_grouped_main = clean_main
        self.comp_grouped_appendix = clean_appx
        self.comp_grouped_summary = main_summ
        self.comp_grouped_filtered_main = clean_filt
        self.comp_grouped_filtered_appendix = clean_filt_appx
        self.comp_grouped_filtered_summary = filt_summ

        # Display Limit Logic
        full_main_count = len(clean_main)
        visible_main = clean_main

        msg_color = "black"
        if len(visible_main) < full_main_count:
            status_msg = tr("Showing top {} of {} results. (Export for full list)").format(len(visible_main), full_main_count)
            msg_color = "#e67e22" # Orange
        else:
            status_msg = tr("Found {} results.").format(full_main_count)

        if hasattr(self, 'lbl_comp_status'):
            self.lbl_comp_status.setText(status_msg)
            self.lbl_comp_status.setStyleSheet(f"color: {msg_color}; font-weight: bold;")

        ids_to_fetch = set()
        def _collect_id(item):
            sid = item.get('sys_id')
            if not sid:
                sid, _ = self.meta_mgr.parse_header_smart(item.get('raw_header', ''))
            if sid and sid not in self.meta_mgr.nli_cache:
                 ids_to_fetch.add(sid)

        self.comp_tree.setUpdatesEnabled(False)
        self.comp_tree.clear()

        def make_checkable(node):
            node.setFlags(node.flags() | Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
            node.setCheckState(0, Qt.CheckState.Unchecked)

        def add_manuscript_node(parent, ms_item):
            if ms_item.get('type') == 'manuscript':
                sid = ms_item['sys_id']
                shelf, t = self.meta_mgr.get_meta_for_id(sid)
                if not shelf or shelf == "Unknown":
                    header_shelf = self.meta_mgr.get_shelfmark_from_header(ms_item.get('raw_header', ''))
                    if header_shelf: shelf = header_shelf

                ms_node = QTreeWidgetItem(parent)
                self._set_comp_tree_text(ms_node, 0, str(int(ms_item.get('score', 0))))
                self._set_comp_tree_text(ms_node, 1, shelf or tr("Unknown Shelfmark"))
                self._set_comp_tree_text(ms_node, 2, t or "")
                self._set_comp_tree_text(ms_node, 3, sid)
                make_checkable(ms_node)
                ms_node.setData(0, Qt.ItemDataRole.UserRole, ms_item)

                pages = ms_item.get('pages', [])
                if len(pages) == 1:
                    p_item = pages[0]
                    _, p_num, _, _ = self._get_meta_for_header(p_item['raw_header'])
                    self._set_comp_tree_text(ms_node, 1, f"{shelf or tr('Unknown Shelfmark')} ({tr('Image')} {p_num})")
                    self._set_comp_node_previews(ms_node, p_item.get('source_ctx', ''), p_item.get('text', ''), p_item.get('highlight_pattern'))
                else:
                    if pages:
                        p0 = pages[0]
                        _, p0_num, _, _ = self._get_meta_for_header(p0['raw_header'])
                        self._set_comp_tree_text(ms_node, 1, f"{shelf or tr('Unknown Shelfmark')} ({tr('Image')} {p0_num}...)")
                        self._set_comp_node_previews(ms_node, p0.get('source_ctx', ''), p0.get('text', ''), p0.get('highlight_pattern'))

                    for p_item in pages:
                        _, p_num, _, _ = self._get_meta_for_header(p_item['raw_header'])
                        page_node = QTreeWidgetItem(ms_node)
                        self._set_comp_tree_text(page_node, 0, str(int(p_item.get('score', 0))))
                        self._set_comp_tree_text(page_node, 1, f"{tr('Image')} {p_num}")
                        self._set_comp_tree_text(page_node, 2, "")
                        self._set_comp_tree_text(page_node, 3, "")
                        make_checkable(page_node)
                        page_node.setData(0, Qt.ItemDataRole.UserRole, p_item)
                        self._set_comp_node_previews(page_node, p_item.get('source_ctx', ''), p_item.get('text', ''), p_item.get('highlight_pattern'))
            else:
                # Fallback
                sid, _, shelf, title = self._get_meta_for_header(ms_item.get('raw_header', ''))
                node = QTreeWidgetItem(parent)
                self._set_comp_tree_text(node, 0, str(int(ms_item.get('score', 0))))
                self._set_comp_tree_text(node, 1, shelf)
                self._set_comp_tree_text(node, 2, title)
                self._set_comp_tree_text(node, 3, sid)
                make_checkable(node)
                node.setData(0, Qt.ItemDataRole.UserRole, ms_item)
                self._set_comp_node_previews(node, ms_item.get('source_ctx', ''), ms_item.get('text', ''), ms_item.get('highlight_pattern'))
            
            _collect_id(ms_item)


        if self.chk_comp_flat.isChecked():
            all_flat = self._collect_comp_items(
                clean_main, clean_appx, clean_filt, clean_filt_appx, self.comp_known
            )
            sorted_flat = self._sort_comp_items(all_flat)
            visible_flat = sorted_flat
            
            root = QTreeWidgetItem(self.comp_tree, [tr("All Results ({})").format(len(visible_flat))])
            root.setExpanded(True)
            make_checkable(root)
            
            for item in visible_flat:
                add_manuscript_node(root, item)

        else:
            # 1. Main Results (Sliced)
            sorted_main = self._sort_comp_items(clean_main)
            visible_sorted_main = sorted_main

            if visible_sorted_main:
                root_main = QTreeWidgetItem(self.comp_tree, [tr("Main Results ({})").format(len(visible_sorted_main))])
                root_main.setData(0, Qt.ItemDataRole.UserRole + 100, "ROOT_MAIN")
                root_main.setExpanded(True)
                make_checkable(root_main)
                for item in visible_sorted_main:
                    add_manuscript_node(root_main, item)

            # 2. Appendix
            if clean_appx:
                total_appx = sum(len(v) for v in clean_appx.values())
                root_appx = QTreeWidgetItem(self.comp_tree, [tr("Appendix - Grouped ({})").format(total_appx)])
                root_appx.setData(0, Qt.ItemDataRole.UserRole + 100, "ROOT_APPX")
                root_appx.setExpanded(False)
                make_checkable(root_appx)
                
                sorted_groups = sorted(clean_appx.items(), key=lambda x: len(x[1]), reverse=True)
                for sig, items in sorted_groups:
                    group_node = QTreeWidgetItem(root_appx, ["", "", f"{sig} ({len(items)})", ""])
                    make_checkable(group_node)
                    
                    for item in self._sort_comp_items(items):
                        add_manuscript_node(group_node, item)

            # 3. Filtered 
            total_filt = len(clean_filt) + sum(len(v) for v in clean_filt_appx.values())
            if total_filt > 0:
                root_filt = QTreeWidgetItem(self.comp_tree, [tr("Filtered ({})").format(total_filt)])
                root_filt.setData(0, Qt.ItemDataRole.UserRole + 100, "ROOT_FILT")
                root_filt.setForeground(0, Qt.GlobalColor.gray)
                make_checkable(root_filt)
                
                # Filtered Main
                for item in self._sort_comp_items(clean_filt):
                    add_manuscript_node(root_filt, item)
                
                # Filtered Appendix
                for sig, items in sorted(clean_filt_appx.items(), key=lambda x: len(x[1]), reverse=True):
                    g_node = QTreeWidgetItem(root_filt, ["", "", f"{sig} ({len(items)})", ""])
                    make_checkable(g_node)
                    for item in self._sort_comp_items(items):
                        add_manuscript_node(g_node, item)

            # 4. Excluded
            if self.comp_known:
                root_known = QTreeWidgetItem(self.comp_tree, [tr("Excluded ({})").format(len(self.comp_known))])
                root_known.setData(0, Qt.ItemDataRole.UserRole + 100, "ROOT_KNOWN")
                root_known.setForeground(0, Qt.GlobalColor.darkGray)
                make_checkable(root_known)
                
                for item in self._sort_comp_items(self.comp_known):
                    add_manuscript_node(root_known, item)

        self.comp_tree.setUpdatesEnabled(True)
        self._update_recursive_button_state()
        
        if ids_to_fetch:
            self.start_metadata_loading(list(ids_to_fetch))
    
    def _add_single_node_to_tree(self, parent, ms_item):
        """Dedicated helper to add one row to the tree."""
        sid = ms_item.get('sys_id')
        if not sid:
            sid, _ = self.meta_mgr.parse_header_smart(ms_item.get('raw_header', ''))
        
        shelf, t = self.meta_mgr.get_meta_for_id(sid)
        display_shelf = shelf if shelf and shelf != "Unknown" else (sid if sid else "Loading...")
        
        node = QTreeWidgetItem(parent)
        self._set_comp_tree_text(node, 0, str(int(ms_item.get('score', 0)))) # עיגול הציון
        self._set_comp_tree_text(node, 1, display_shelf)
        self._set_comp_tree_text(node, 2, t or "")
        self._set_comp_tree_text(node, 3, sid)
        
        node.setFlags(node.flags() | Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
        node.setCheckState(0, Qt.CheckState.Unchecked)
        node.setData(0, Qt.ItemDataRole.UserRole, ms_item)

        pages = ms_item.get('pages', [])
        
        if len(pages) == 1:
            p_item = pages[0]
            p_num = "Img"
            if 'raw_header' in p_item:
                _, p_num_extracted, _, _ = self._get_meta_for_header(p_item['raw_header'])
                if p_num_extracted: p_num = p_num_extracted
            
            self._set_comp_tree_text(node, 1, f"{display_shelf} (Img {p_num})")
            self._set_comp_node_previews(node, p_item.get('source_ctx', ''), p_item.get('text', ''), p_item.get('highlight_pattern'))
        
        elif len(pages) > 1:
             self._set_comp_tree_text(node, 1, f"{display_shelf} ({len(pages)} matches)")
             
             if pages:
                 first_p = pages[0]
                 self._set_comp_node_previews(node, first_p.get('source_ctx', ''), first_p.get('text', ''), first_p.get('highlight_pattern'))
             # ---------------------------------------------------------

             for p_item in pages:
                child = QTreeWidgetItem(node)
                self._set_comp_tree_text(child, 0, str(int(p_item.get('score', 0))))
                
                p_num_child = "?"
                if 'raw_header' in p_item:
                    _, p_val, _, _ = self._get_meta_for_header(p_item['raw_header'])
                    if p_val: p_num_child = p_val
                
                child.setText(1, f"Img {p_num_child}") 

                child.setData(0, Qt.ItemDataRole.UserRole, p_item)
                self._set_comp_node_previews(child, p_item.get('source_ctx', ''), p_item.get('text', ''), p_item.get('highlight_pattern'))
    
    def _trigger_lazy_metadata_fetch(self):
        """Starts background fetching for items that are currently displayed but missing data."""
        missing_ids = set()
        for item in self.batch_items:
            sid = item.get('sys_id')
            if not sid:
                sid, _ = self.meta_mgr.parse_header_smart(item.get('raw_header'))
            
            if sid and sid not in self.meta_mgr.nli_cache:
                 missing_ids.add(sid)
        
        if missing_ids:
            self.start_metadata_loading(list(missing_ids))
    
    def on_comp_tree_item_changed(self, item, column):
        if self.comp_tree_updating or column != 0:
            return

        self.comp_tree_updating = True
        state = item.checkState(0)
        if item.childCount() > 0 and state in (Qt.CheckState.Checked, Qt.CheckState.Unchecked):
            for i in range(item.childCount()):
                item.child(i).setCheckState(0, state)

        self._sync_parent_check_state(item)

        # Sync "Select All" checkbox state
        all_checked = True
        root = self.comp_tree.invisibleRootItem()
        if root.childCount() == 0:
            all_checked = False
        else:
            for i in range(root.childCount()):
                if root.child(i).checkState(0) == Qt.CheckState.Unchecked:
                    all_checked = False
                    break

        self.chk_comp_header.blockSignals(True)
        self.chk_comp_header.setChecked(all_checked)
        self.chk_comp_header.blockSignals(False)

        self.comp_tree_updating = False
        self._update_recursive_button_state()
        self._update_comp_export_label()

    def on_comp_header_toggled(self, checked):
        """Toggle all root items in the composition tree."""
        state = Qt.CheckState.Checked if checked else Qt.CheckState.Unchecked

        self.comp_tree.blockSignals(True)
        root = self.comp_tree.invisibleRootItem()
        for i in range(root.childCount()):
            item = root.child(i)
            item.setCheckState(0, state)
            self._set_check_state_recursive(item, state)
        self.comp_tree.blockSignals(False)

        self._update_comp_export_label()
        self._update_recursive_button_state()

    def _set_check_state_recursive(self, item, state):
        for i in range(item.childCount()):
            child = item.child(i)
            child.setCheckState(0, state)
            self._set_check_state_recursive(child, state)

    def _update_comp_export_label(self):
        has_selection = bool(self._collect_checked_comp_page_uids())
        if has_selection:
            self.lbl_comp_export.setText(tr("Export selected results"))
        else:
            self.lbl_comp_export.setText(tr("Save Report"))

    def _collect_checked_comp_items_struct(self):
        """
        Collect checked items maintaining the structure (Main, Appendix, etc.)
        Returns: (main, appendix, filtered_main, filtered_appendix, known)
        Only items that are CHECKED (or have checked descendants) are returned.
        """
        sel_main = []
        sel_appx = {}
        sel_filt = []
        sel_filt_appx = {}
        sel_known = []

        # Helper to collect checked children from a node
        def collect_from_node(node):
            collected = []
            # If node is a leaf (Manuscript or Page)
            data = node.data(0, Qt.ItemDataRole.UserRole)
            if data:
                if node.checkState(0) == Qt.CheckState.Checked:
                    collected.append(data)
                return collected

            # If node is a group container (no data)
            for k in range(node.childCount()):
                child = node.child(k)
                child_data = child.data(0, Qt.ItemDataRole.UserRole)
                if not child_data:
                    # Recursive group (e.g. Appendix group)
                    res = collect_from_node(child)
                    collected.extend(res)
                    continue

                # It is a manuscript item
                if child.checkState(0) == Qt.CheckState.Unchecked:
                    continue

                # If fully checked or partially checked
                if child.checkState(0) == Qt.CheckState.Checked:
                    # Full manuscript selected
                    collected.append(child_data)
                elif child.checkState(0) == Qt.CheckState.PartiallyChecked:
                    # Some pages selected
                    # Clone item
                    import copy
                    new_item = copy.copy(child_data)
                    new_item['pages'] = []

                    # Find checked pages
                    for p_idx in range(child.childCount()):
                        p_node = child.child(p_idx)
                        if p_node.checkState(0) == Qt.CheckState.Checked:
                            p_data = p_node.data(0, Qt.ItemDataRole.UserRole)
                            new_item['pages'].append(p_data)

                    if new_item['pages']:
                            collected.append(new_item)
            return collected

        root = self.comp_tree.invisibleRootItem()

        # If Flat Mode:
        if self.chk_comp_flat.isChecked():
            # Root 0 is "All Results"
            if root.childCount() > 0:
                sel_main = collect_from_node(root.child(0))
            return sel_main, {}, [], {}, []

        # Use node data (UserRole+100) to identify categories
        for i in range(root.childCount()):
            node = root.child(i)
            node_type = node.data(0, Qt.ItemDataRole.UserRole + 100)

            items = collect_from_node(node)
            if not items: continue

            if node_type == "ROOT_MAIN":
                 sel_main.extend(items)
            elif node_type == "ROOT_APPX":
                 # Custom traversal for Appendix to preserve grouping structure
                 for k in range(node.childCount()):
                     group_node = node.child(k)
                     group_sig_full = group_node.text(2)
                     group_sig = group_sig_full.rpartition(' (')[0] # Remove count

                     group_items = collect_from_node(group_node)
                     if group_items:
                         sel_appx[group_sig] = group_items

            elif node_type == "ROOT_FILT":
                 # Iterate children to separate Main/Appendix in filtered
                 for k in range(node.childCount()):
                     child = node.child(k)
                     c_data = child.data(0, Qt.ItemDataRole.UserRole)
                     if c_data:
                         # Direct item -> Filtered Main
                         if child.checkState(0) == Qt.CheckState.Checked:
                             sel_filt.append(c_data)
                         elif child.checkState(0) == Qt.CheckState.PartiallyChecked:
                             # Reuse logic from helper
                             import copy
                             new_item = copy.copy(c_data)
                             new_item['pages'] = []
                             for p_idx in range(child.childCount()):
                                 p_node = child.child(p_idx)
                                 if p_node.checkState(0) == Qt.CheckState.Checked:
                                     new_item['pages'].append(p_node.data(0, Qt.ItemDataRole.UserRole))
                             if new_item['pages']: sel_filt.append(new_item)
                     else:
                         # Group node -> Filtered Appendix
                         g_sig = child.text(2).rpartition(' (')[0]
                         g_items = collect_from_node(child)
                         if g_items:
                             sel_filt_appx[g_sig] = g_items

            elif node_type == "ROOT_KNOWN":
                 sel_known.extend(items)

        return sel_main, sel_appx, sel_filt, sel_filt_appx, sel_known

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

                if sub_node.childCount() > 0:
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

    def _fmt_item_legacy(self, item):
        # Fallback for old page style if needed
        sid, p_num, shelf, title = self._get_meta_for_header(item.get('raw_header', ''))

        def clean(t):
            t = str(t or "")
            t = re.sub(r'<span[^>]*>', '*', t).replace('</span>', '*')
            t = t.replace("<br>", " ").replace("\n", " ").replace("\r", "")
            t = re.sub(r'<[^>]+>', '', t)
            t = re.sub(r'\s+', ' ', t)
            t = re.sub(r'\*(\s+)\*', r'\1', t)
            return t.strip()

        return [
            "=" * 80,
            f"{shelf or sid} | {title or 'Untitled'} | Img: {p_num} | Version: {item.get('src_lbl','')} | ID: {item.get('uid', sid)} (Score: {item.get('score', 0)})",
            tr("Source Context") + ":", clean(item.get('source_ctx', '')), "",
            tr("Manuscript") + ":", clean(item.get('text', '')), ""
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
        shelf_query = self.browse_shelf_input.text().strip()
        fl_id = self.browse_fl_input.text().strip()
        if not sid and not fl_id and not shelf_query: return

        # Reset UI
        self.browse_text.setText(tr("Loading metadata..."))
        self.browse_viewer.load_images({}) # Clear viewer

        page_data = None

        # Determine priority based on last edited field (default: shelfmark > system ID > FL)
        priority = []
        if self.last_browse_field == "fl" and fl_id:
            priority.append("fl")
        elif self.last_browse_field == "shelf" and shelf_query:
            priority.append("shelf")
        elif self.last_browse_field == "sys" and sid:
            priority.append("sys")

        if shelf_query and "shelf" not in priority:
            priority.append("shelf")
        if sid and "sys" not in priority:
            priority.append("sys")
        if fl_id and "fl" not in priority:
            priority.append("fl")

        def format_option(opt, idx):
            base = opt['shelfmark']
            title = (opt.get('title') or "").strip()
            if title:
                base = f"{base} | {title}"
            label = f"{idx + 1}. {base}"
            if len(label) > 60:
                label = label[:57] + "..."
            return label

        for field in priority:
            if field == "fl":
                if not fl_id:
                    continue
                pd = self.searcher.get_browse_page_by_fl(fl_id, sid or None)
                if pd:
                    page_data = pd
                    sid = pd.get('sys_id', sid)
                    self.browse_sys_input.setText(sid or "")
                    self.browse_fl_input.setText(pd.get('fl_id', fl_id))
                    break
                # If no other identifiers exist, stop and warn
                if not sid and not shelf_query:
                    QMessageBox.warning(self, tr("Error"), tr("FL not found."))
                    return
                continue

            if field == "shelf":
                if not shelf_query:
                    continue
                shelf_res = self.meta_mgr.resolve_system_by_shelfmark(shelf_query)
                if shelf_res['sys_id']:
                    sid = shelf_res['sys_id']
                    if shelf_res['selected_shelfmark']:
                        self.browse_shelf_input.setText(shelf_res['selected_shelfmark'])
                    self.browse_sys_input.setText(sid or "")
                    break
                elif shelf_res['options']:
                    options = shelf_res['options']
                    if len(options) == 1:
                        opt = options[0]
                        sid = opt['sys_id']
                        self.browse_shelf_input.setText(opt['shelfmark'])
                        self.browse_sys_input.setText(sid or "")
                        break
                    display_options = [format_option(opt, idx) for idx, opt in enumerate(options)]
                    choice, ok = QInputDialog.getItem(
                        self, tr("Shelfmark"), tr("Multiple shelfmarks found. Select one:"), display_options, 0, False
                    )
                    if not ok:
                        return
                    if choice in display_options:
                        idx = display_options.index(choice)
                        opt = options[idx]
                        sid = opt['sys_id']
                        self.browse_shelf_input.setText(opt['shelfmark'])
                        self.browse_sys_input.setText(sid or "")
                        break
                # Shelfmark was the chosen path; stop if not resolved
                QMessageBox.warning(self, tr("Error"), tr("Shelfmark not found."))
                return

            if field == "sys":
                if sid:
                    break

        if not sid:
            msg = tr("FL not found.")
            if shelf_query:
                msg = tr("Shelfmark not found.")
            QMessageBox.warning(self, tr("Error"), msg)
            return

        self.current_browse_sid = sid
        self.current_browse_p = page_data['p_num'] if page_data else None
        self.current_browse_internal_idx = None
        
        # Disable controls until loaded
        self.btn_b_catalog.setEnabled(False)
        self.btn_b_save.setEnabled(False)

        # Trigger Unified Enrichment (Meta + Images)
        self.enrich_browse_worker = EnrichMetadataThread(self.meta_mgr, sid)
        self.enrich_browse_worker.finished_signal.connect(self.on_browse_enriched_loaded)
        self.enrich_browse_worker.start()

    def browse_navigate(self, d):
        if not self.current_browse_sid: return

        idx_arg = self.current_browse_internal_idx
        
        p_arg = None
        if self.current_browse_p is not None:
            try: p_arg = int(self.current_browse_p)
            except: p_arg = 0

        page_data = self.searcher.get_browse_page(
            self.current_browse_sid, 
            p_num=p_arg, 
            next_prev=d,
            absolute_index=idx_arg
        )

        if page_data:
            self.browse_render_page(page_data)
        else:
            QMessageBox.warning(self, tr("Nav"), tr("Not found or end."))
    
    def browse_render_page(self, pd):
        self.current_browse_p = pd['p_num']
        
        if 'internal_index' in pd:
            self.current_browse_internal_idx = pd['internal_index']
        else:
            self.current_browse_internal_idx = pd.get('current_idx', 1) - 1

        browse_html_text = pd['text'].replace('\n', '<br>')
        self.browse_text.setHtml(f"<div dir='rtl'>{browse_html_text}</div>")
        
        full_header = pd.get('full_header', '')
        _, _, shelf, title = self._get_meta_for_header(full_header)
        info_text = f"<b>{shelf}</b><br>{title or ''}"
        self.browse_info_lbl.setText(info_text)
        if shelf:
            self.browse_shelf_input.setText(shelf)

        self.lbl_page_count.setText(f"{pd['current_idx']}/{pd['total_pages']}")
        self.btn_b_prev.setEnabled(pd['current_idx'] > 1)
        self.btn_b_next.setEnabled(pd['current_idx'] < pd['total_pages'])

        parsed = self.meta_mgr.parse_full_id_components(full_header)
        if parsed.get('fl_id'):
            self.browse_fl_input.setText(f"FL{parsed['fl_id']}")
        else:
            self.browse_fl_input.setText("")
            
        # This tells the large image viewer on the right to jump to the correct index
        if hasattr(self, 'browse_viewer') and self.browse_viewer.isVisible():
            try: 
                # p_num is 1-based, array index is 0-based
                idx = int(self.current_browse_p) - 1
            except: 
                idx = 0
            self.browse_viewer.set_page(idx)
        # -------------------------------

        if self.current_browse_sid in self.meta_mgr.nli_cache:
            self.fetch_browse_thumbnail(self.current_browse_sid)
        else:
            self.browse_thumb.setText(tr("Loading Meta..."))
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
    
    def _add_single_comp_node(self, parent, ms_item):
        """Adds a node to the composition tree with parent/child logic."""
        sid = ms_item.get('sys_id')
        if not sid:
            sid, _ = self.meta_mgr.parse_header_smart(ms_item.get('raw_header', ''))
        
        shelf, t = self.meta_mgr.get_meta_for_id(sid)
        display_shelf = shelf if shelf and shelf != "Unknown" else (sid if sid else "Loading...")
        
        pages = ms_item.get('pages', [])
        best_snippet = ""
        best_ctx = ""
        if pages:
            best_snippet = pages[0].get('text', '') 
            best_ctx = pages[0].get('source_ctx', '')

        node = QTreeWidgetItem(parent)
        self._set_comp_tree_text(node, 0, str(int(ms_item.get('score', 0))))
        self._set_comp_tree_text(node, 1, display_shelf)
        self._set_comp_tree_text(node, 2, t or "")
        self._set_comp_tree_text(node, 3, sid)
        
        node.setFlags(node.flags() | Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
        node.setCheckState(0, Qt.CheckState.Unchecked)
        node.setData(0, Qt.ItemDataRole.UserRole, ms_item)

        node.setData(1, Qt.ItemDataRole.UserRole, (best_ctx, best_snippet))

        pattern = pages[0].get('highlight_pattern') if pages else None
        self._set_comp_node_previews(node, best_ctx, best_snippet, pattern)

        if len(pages) > 1:
            node.setText(1, f"{display_shelf} ({len(pages)} matches)")
            for p_item in pages:
                p_num_str = "Page Match"
                raw_h = p_item.get('raw_header', '')
                match = re.search(r'(?i)Img\s*(\d+)', raw_h)
                if match:
                    p_num_str = f"Image {match.group(1)}"
                else:
                    _, p_num_ex, _, _ = self._get_meta_for_header(raw_h)
                    if p_num_ex: p_num_str = f"Image {p_num_ex}"
                
                child = QTreeWidgetItem(node)
                self._set_comp_tree_text(child, 0, str(int(p_item.get('score', 0))))
                child.setText(1, p_num_str)
                child.setData(0, Qt.ItemDataRole.UserRole, p_item)
                self._set_comp_node_previews(child, p_item.get('source_ctx', ''), p_item.get('text', ''), p_item.get('highlight_pattern'))
        
        elif len(pages) == 1:
            p_item = pages[0]
            p_suffix = ""
            match = re.search(r'(?i)Img\s*(\d+)', p_item.get('raw_header', ''))
            if match: p_suffix = f" (Img {match.group(1)})"
            node.setText(1, f"{display_shelf}{p_suffix}")

    def _on_comp_item_expanded(self, item):
        if item.childCount() > 0:
            self.comp_tree.setItemWidget(item, self.comp_col_context, None) 
            self.comp_tree.setItemWidget(item, self.comp_col_ms_context, None) 

    def _on_comp_item_collapsed(self, item):
        if item.childCount() > 0:
            stored_data = item.data(1, Qt.ItemDataRole.UserRole)
            if stored_data:
                ctx, snippet = stored_data

                # Try to retrieve highlight pattern from the main item data (Role 0)
                item_data = item.data(0, Qt.ItemDataRole.UserRole)
                pattern = None
                if item_data:
                    if item_data.get('type') == 'manuscript':
                        pages = item_data.get('pages', [])
                        if pages:
                            pattern = pages[0].get('highlight_pattern')
                    else:
                        pattern = item_data.get('highlight_pattern')

                self._set_comp_node_previews(item, ctx, snippet, pattern)

    def on_comp_item_double_clicked(self, item, column):
        """
        Smart navigation that restores full context (Next/Prev, Source Text).
        It rebuilds the full list of results from the tree but jumps to the specific clicked item.
        """
        data = item.data(0, Qt.ItemDataRole.UserRole)
        if not data: return 

        flat_list = []
        target_index = -1
        
        clicked_node = item
        
        if data.get('type') == 'manuscript' and item.childCount() > 0:
            clicked_node = item.child(0)

        def collect_node_data(node):
            node_data = node.data(0, Qt.ItemDataRole.UserRole)
            if not node_data: return

            if node_data.get('type') == 'manuscript' and node.childCount() > 0:
                for i in range(node.childCount()):
                    collect_node_data(node.child(i))
                return

            raw_h = node_data.get('raw_header', '')
            sid, p_num, shelf, title = self._get_meta_for_header(raw_h)
            
            hl_pattern = node_data.get('highlight_pattern')
            
            ready_item = {
                'uid': node_data.get('uid', sid),
                'raw_header': raw_h,
                'text': node_data.get('text', ''),
                'full_text': None, 
                'source_ctx': node_data.get('source_ctx', ''),
                'highlight_pattern': hl_pattern,
                'display': {
                    'id': sid,
                    'shelfmark': shelf,
                    'title': title,
                    'img': p_num,
                    'source': node_data.get('src_lbl', 'Genizah Lab')
                }
            }
            
            flat_list.append(ready_item)
            
            if node is clicked_node:
                nonlocal target_index
                target_index = len(flat_list) - 1

        root = self.comp_tree.invisibleRootItem()
        for i in range(root.childCount()):
            category = root.child(i)
            if category.data(0, Qt.ItemDataRole.UserRole):
                 collect_node_data(category)
            
            for j in range(category.childCount()):
                sub = category.child(j)
                collect_node_data(sub)

        if not flat_list: return
        
        if target_index == -1: target_index = 0

        try:
            dlg = ResultDialog(self, flat_list, target_index, self.meta_mgr, self.searcher)
            dlg.exec()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to open viewer: {e}")

    def open_manuscript_viewer_by_id(self, sys_id, highlight_regex=None, target_page=0):
        """
        פותח את חלון הצפייה עבור מזהה ספציפי, ומנווט לעמוד המבוקש.
        """
        meta = self.meta_mgr.fetch_nli_data(sys_id)
        
        item_data = {
            'display': {
                'id': sys_id,
                'shelfmark': meta.get('shelfmark', sys_id),
                'title': meta.get('title', ''),
                'source': 'Genizah Lab',
                'img': meta.get('thumb_url', '')
            },
            'snippet': '',  
            'full_text': '', 
            'uid': sys_id,
            'highlight_pattern': highlight_regex,
            'raw_header': str(sys_id) 
        }

        try:
            dlg = ResultDialog(self, [item_data], 0, self.meta_mgr, self.searcher)
            
            if target_page > 0:
                dlg.load_page(target=target_page)

            dlg.exec()
        except Exception as e:
            print(f"Error opening viewer: {e}")
            QMessageBox.warning(self, "Error", f"Could not open viewer: {e}")
            
    def navigate_manuscript(self, direction):
        """Move to prev/next manuscript based on FILE ORDER."""
        current = self.current_browse_sid
        
        new_sid = self.searcher.get_adjacent_sys_id_by_file_order(current, direction)
        
        if new_sid:
            self.browse_sys_input.setText(new_sid)
            
            shelf, _ = self.meta_mgr.get_meta_for_id(new_sid)
            if shelf and shelf != "Unknown":
                self.browse_shelf_input.setText(shelf)

            # נותן עדיפות ל-System ID וטוען
            self._set_last_browse_field("sys")
            self.browse_load()
        else:
            QMessageBox.information(self, tr("Nav"), tr("End of file list."))
    
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
