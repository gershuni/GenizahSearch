"""Image viewer widgets extracted from genizah_app.py (v7.9 decomposition)."""

import re
import threading

from PyQt6.QtWidgets import (
    QApplication, QComboBox, QFileDialog, QFrame,
    QGraphicsPixmapItem, QGraphicsScene, QGraphicsSimpleTextItem,
    QGraphicsView, QHBoxLayout, QLabel, QMainWindow, QMenu,
    QPushButton, QScrollArea, QSlider, QVBoxLayout, QWidget,
)
from PyQt6.QtCore import Qt, QRectF, QTimer, QUrl, pyqtSignal
from PyQt6.QtGui import (
    QColor, QDesktopServices, QFont, QImage, QPainter, QPixmap, QTransform,
)
from PyQt6 import sip

from genizah_core import Config, get_logger, tr
from desktop.image_loader import ImageLoaderThread

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helper functions used by viewer classes
# ---------------------------------------------------------------------------

def _make_scrollable_row(layout: QHBoxLayout) -> QScrollArea:
    """Wrap a QHBoxLayout in a horizontal QScrollArea so it can shrink freely in a splitter."""
    container = QWidget()
    container.setLayout(layout)
    scroll = QScrollArea()
    scroll.setWidget(container)
    scroll.setWidgetResizable(True)
    scroll.setFrameShape(QFrame.Shape.NoFrame)
    scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
    scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
    # Derive height from content sizeHint + room for thin scrollbar, DPI-safe
    hint_h = container.sizeHint().height()
    scroll.setFixedHeight(hint_h + 8)  # 8px for the thin scrollbar when visible
    scroll.setStyleSheet("QScrollBar:horizontal { height: 6px; }")
    return scroll


def _generate_oxford_dynamic_url(oxford_part_id, folio_num, side='a'):
    """Generate dynamic Oxford image URL for a folio not in the database.

    Args:
        oxford_part_id: Part ID like "MS. Heb. f. 21/1"
        folio_num: Folio number (e.g., 21)
        side: 'a' for recto, 'b' for verso

    Returns URL string or None if generation not possible.
    """
    if not oxford_part_id:
        return None

    match = re.match(r'^MS\.?\s*Heb\.?\s*([a-z])\.?\s*(\d+)', oxford_part_id, re.IGNORECASE)
    if not match:
        return None

    letter, volume = match.groups()
    return f"https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_{letter}_{volume}_{folio_num}{side}.jpg"


# ---------------------------------------------------------------------------
# Viewer classes (ordered by dependency: base -> composite)
# ---------------------------------------------------------------------------

class ZoomableScrollArea(QGraphicsView):
    """A GraphicsView that supports hand-panning and wheel-zooming."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.scene = QGraphicsScene(self)
        self.setScene(self.scene)

        self.setRenderHint(QPainter.RenderHint.Antialiasing)
        self.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)
        self.setDragMode(QGraphicsView.DragMode.ScrollHandDrag)
        self.setTransformationAnchor(QGraphicsView.ViewportAnchor.AnchorUnderMouse)
        self.setResizeAnchor(QGraphicsView.ViewportAnchor.AnchorUnderMouse)
        self.setStyleSheet("background: #222; border: none;")

        # Hide scrollbars but keep functionality
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        self._pixmap_item = QGraphicsPixmapItem()
        self._pixmap_item.setTransformationMode(Qt.TransformationMode.SmoothTransformation)
        self.scene.addItem(self._pixmap_item)

        self._msg_item = QGraphicsSimpleTextItem()
        self._msg_item.setBrush(QColor("white"))
        font = QFont("Arial", 16)
        self._msg_item.setFont(font)
        self.scene.addItem(self._msg_item)

        self._pixmap = None
        self._rotation = 0
        self._auto_fit_enabled = False

        # Image adjustment state
        self._brightness = 0      # -100..+100
        self._contrast = 0        # -100..+100
        self._gamma = 1.0         # 0.2..3.0
        self._invert = False
        self._adj_timer = None     # debounce timer for filter updates

        # Right-click context menu for copy/save
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.customContextMenuRequested.connect(self._show_image_context_menu)

    def _show_image_context_menu(self, pos):
        """Show context menu with Copy/Save options for the displayed image."""
        if not self._pixmap or self._pixmap.isNull():
            return
        menu = QMenu(self)
        act_copy = menu.addAction(tr("Copy Image"))
        act_save = menu.addAction(tr("Save Image As..."))
        action = menu.exec(self.mapToGlobal(pos))
        if action == act_copy:
            self._copy_image()
        elif action == act_save:
            self._save_image()

    def _copy_image(self):
        """Copy current image (with rotation) to clipboard."""
        pix = self._get_rotated_pixmap()
        if pix:
            QApplication.clipboard().setPixmap(pix)

    def _save_image(self):
        """Save current image (with rotation) to file."""
        pix = self._get_rotated_pixmap()
        if not pix:
            return
        path, _ = QFileDialog.getSaveFileName(
            self, tr("Save Image As..."), "",
            "PNG (*.png);;JPEG (*.jpg *.jpeg);;BMP (*.bmp)")
        if path:
            pix.save(path)

    def _get_rotated_pixmap(self):
        """Return the current pixmap with adjustments and rotation applied (for export)."""
        if not self._pixmap or self._pixmap.isNull():
            return None
        # Apply image adjustments first
        adjusted = self._apply_adjustments_to_pixmap(self._pixmap)
        if self._rotation == 0:
            return adjusted
        transform = QTransform()
        transform.rotate(self._rotation)
        return adjusted.transformed(transform, Qt.TransformationMode.SmoothTransformation)

    def set_image(self, pixmap):
        self._pixmap = pixmap
        self._rotation = 0
        self._auto_fit_enabled = bool(pixmap)
        # Cancel any pending filter debounce timer from the previous image
        if self._adj_timer is not None:
            try:
                self._adj_timer.stop()
            except RuntimeError:
                pass
            self._adj_timer = None
        # Reset adjustments on new image (without triggering display update since we handle it below)
        self._brightness = 0
        self._contrast = 0
        self._gamma = 1.0
        self._invert = False

        # Guard against destroyed graphics items (async callback after widget close)
        if sip.isdeleted(self._msg_item) or sip.isdeleted(self._pixmap_item):
            return

        if not pixmap or pixmap.isNull():
            self._pixmap_item.setVisible(False)
            self.set_status_message(tr("No Image"))
            return

        self._msg_item.setVisible(False)
        self._pixmap_item.setPixmap(pixmap)
        self._pixmap_item.setVisible(True)

        # Reset transform
        self.resetTransform()

        # Center item transform origin
        rect = QRectF(pixmap.rect())
        # Let scene grow automatically to fit rotated items
        self.scene.setSceneRect(QRectF())
        self._pixmap_item.setPos(0, 0)
        self._pixmap_item.setTransformOriginPoint(rect.center())

        if not self._apply_fit_to_viewport():
             self.centerOn(self._pixmap_item)

    def set_status_message(self, text):
        if sip.isdeleted(self._msg_item) or sip.isdeleted(self._pixmap_item):
            return
        self._pixmap_item.setVisible(False)
        self._msg_item.setText(text)
        self._msg_item.setVisible(True)
        self._update_text_pos()

    def _update_text_pos(self):
        if sip.isdeleted(self._msg_item):
            return
        if not self._msg_item.isVisible(): return

        # Simple center in view
        # We need to map viewport center to scene
        center = self.mapToScene(self.viewport().rect().center())
        brect = self._msg_item.boundingRect()
        self._msg_item.setPos(center.x() - brect.width()/2, center.y() - brect.height()/2)

    def set_rotation(self, angle: float):
        """Set absolute rotation (degrees clockwise) and update view."""
        self._rotation = angle % 360 if angle is not None else 0
        self._pixmap_item.setRotation(self._rotation)

    def rotate_view(self, degrees):
        """Add degrees to current rotation and update."""
        self._rotation = (self._rotation + degrees) % 360
        self._pixmap_item.setRotation(self._rotation)

    def wheelEvent(self, event):
        if event.modifiers() & Qt.KeyboardModifier.ControlModifier:
            # Ctrl+wheel: zoom
            self._auto_fit_enabled = False
            delta = event.angleDelta().y()
            factor = 1.1 if delta > 0 else 0.9
            self._apply_zoom(factor)
            event.accept()
        else:
            # Plain wheel: propagate to parent for normal scrolling
            event.ignore()

    def zoom_in(self):
        self._auto_fit_enabled = False
        self._apply_zoom(1.2)

    def zoom_out(self):
        self._auto_fit_enabled = False
        self._apply_zoom(0.8)

    def _apply_zoom(self, factor):
        current_scale = self.transform().m11()
        new_scale = current_scale * factor

        # Clamp zoom level (0.1 to 5.0)
        if new_scale < 0.1:
            factor = 0.1 / current_scale
        elif new_scale > 5.0:
            factor = 5.0 / current_scale

        self.scale(factor, factor)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self._auto_fit_enabled:
            self._apply_fit_to_viewport()
        else:
            self._update_text_pos()

    def _apply_fit_to_viewport(self):
        if not self._pixmap or self._pixmap.isNull():
            return False

        self.fitInView(self._pixmap_item, Qt.AspectRatioMode.KeepAspectRatio)
        # Scale down a bit to have margins
        self.scale(0.95, 0.95)
        return True

    def set_adjustments(self, brightness=None, contrast=None, gamma=None, invert=None):
        """Update image adjustment values and schedule a filter update."""
        if brightness is not None: self._brightness = brightness
        if contrast is not None: self._contrast = contrast
        if gamma is not None: self._gamma = gamma
        if invert is not None: self._invert = invert
        self._schedule_filter_update()

    def _schedule_filter_update(self):
        """Debounce filter updates to 100ms for performance on large images."""
        if self._adj_timer is not None:
            try:
                self._adj_timer.stop()
            except RuntimeError:
                pass
        self._adj_timer = QTimer()
        self._adj_timer.setSingleShot(True)
        self._adj_timer.timeout.connect(self._apply_display_filters)
        self._adj_timer.start(80)

    def _build_lut(self):
        """Build a 256-entry lookup table for brightness/contrast/gamma/invert."""
        lut = bytearray(256)
        brightness_offset = self._brightness * 2.55
        contrast_factor = 1.0 + self._contrast / 100.0
        gamma_exp = 1.0 / self._gamma if self._gamma > 0 else 1.0
        for i in range(256):
            # Contrast around midpoint
            val = (i - 128) * contrast_factor + 128 + brightness_offset
            val = max(0.0, min(255.0, val))
            # Gamma
            if gamma_exp != 1.0:
                val = 255.0 * ((val / 255.0) ** gamma_exp)
            # Invert
            if self._invert:
                val = 255.0 - val
            lut[i] = int(max(0, min(255, round(val))))
        return lut

    def _apply_display_filters(self):
        """Apply brightness/contrast/gamma/invert to display via LUT on pixels."""
        if not self._pixmap or self._pixmap.isNull():
            return
        if sip.isdeleted(self._pixmap_item):
            return

        # If all defaults, show original
        if self._brightness == 0 and self._contrast == 0 and self._gamma == 1.0 and not self._invert:
            self._pixmap_item.setPixmap(self._pixmap)
            return

        lut = self._build_lut()
        img = self._pixmap.toImage().convertToFormat(QImage.Format.Format_ARGB32)
        w, h = img.width(), img.height()

        # Use bits() for fast pixel access
        ptr = img.bits()
        if ptr is None:
            return
        ptr.setsize(h * img.bytesPerLine())
        data = bytearray(ptr)

        bpl = img.bytesPerLine()
        for y in range(h):
            offset = y * bpl
            for x in range(w):
                idx = offset + x * 4
                # ARGB32: B, G, R, A (little-endian on Windows)
                data[idx] = lut[data[idx]]         # B
                data[idx + 1] = lut[data[idx + 1]] # G
                data[idx + 2] = lut[data[idx + 2]] # R
                # Alpha (idx+3) unchanged

        result_img = QImage(bytes(data), w, h, bpl, QImage.Format.Format_ARGB32).copy()
        self._pixmap_item.setPixmap(QPixmap.fromImage(result_img))

    def _apply_adjustments_to_pixmap(self, pixmap):
        """Apply current adjustments to a pixmap and return the result. Used for export."""
        if self._brightness == 0 and self._contrast == 0 and self._gamma == 1.0 and not self._invert:
            return pixmap

        lut = self._build_lut()
        img = pixmap.toImage().convertToFormat(QImage.Format.Format_ARGB32)
        w, h = img.width(), img.height()

        ptr = img.bits()
        if ptr is None:
            return pixmap
        ptr.setsize(h * img.bytesPerLine())
        data = bytearray(ptr)

        bpl = img.bytesPerLine()
        for y in range(h):
            offset = y * bpl
            for x in range(w):
                idx = offset + x * 4
                data[idx] = lut[data[idx]]
                data[idx + 1] = lut[data[idx + 1]]
                data[idx + 2] = lut[data[idx + 2]]

        result_img = QImage(bytes(data), w, h, bpl, QImage.Format.Format_ARGB32).copy()
        return QPixmap.fromImage(result_img)

    def reset_adjustments(self):
        """Reset all image adjustments to defaults."""
        self._brightness = 0
        self._contrast = 0
        self._gamma = 1.0
        self._invert = False
        if not self._pixmap or self._pixmap.isNull():
            return
        if sip.isdeleted(self._pixmap_item):
            return
        self._pixmap_item.setPixmap(self._pixmap)

class FullscreenImageWindow(QMainWindow):
    """Borderless fullscreen window for manuscript image viewing.
    Supports zoom/pan, rotation, image adjustments, page navigation, Escape to close."""

    page_changed = pyqtSignal(int)  # emitted with delta (-1 or +1) when user navigates

    _BTN_STYLE = (
        "QPushButton { color: #ccc; background: #333; border: 1px solid #555; border-radius: 3px; padding: 2px; }"
        "QPushButton:hover { background: #444; color: white; }"
        "QPushButton:checked { background: #555; color: white; }"
    )

    def __init__(self, pixmap, parent_viewer=None):
        # Parent to the viewer's top-level window so modal dialogs (ResultDialog.exec())
        # don't block input to this fullscreen window
        parent_window = parent_viewer.window() if parent_viewer else None
        super().__init__(parent_window)
        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint | Qt.WindowType.Window)
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        self.setFocusPolicy(Qt.FocusPolicy.StrongFocus)
        self._parent_viewer = parent_viewer

        self.setStyleSheet("background: #111;")

        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # === Top toolbar ===
        top_bar_widget = QWidget()
        top_bar_widget.setLayoutDirection(Qt.LayoutDirection.LeftToRight)  # arrows are spatial, not semantic
        top_bar = QHBoxLayout(top_bar_widget)
        top_bar.setContentsMargins(10, 6, 10, 6)

        # Prev page button
        self._btn_prev = QPushButton("\u25c0")
        self._btn_prev.setFixedWidth(30)
        self._btn_prev.setStyleSheet(self._BTN_STYLE)
        self._btn_prev.setToolTip(tr("Previous image"))
        self._btn_prev.clicked.connect(lambda: self.page_changed.emit(-1))
        top_bar.addWidget(self._btn_prev)

        # Page info
        self._lbl_page = QLabel("")
        self._lbl_page.setStyleSheet("color: #aaa; font-size: 13px; margin: 0 6px;")
        top_bar.addWidget(self._lbl_page)

        # Next page button
        self._btn_next = QPushButton("\u25b6")
        self._btn_next.setFixedWidth(30)
        self._btn_next.setStyleSheet(self._BTN_STYLE)
        self._btn_next.setToolTip(tr("Next image"))
        self._btn_next.clicked.connect(lambda: self.page_changed.emit(1))
        top_bar.addWidget(self._btn_next)

        top_bar.addSpacing(15)

        # Zoom controls
        btn_zoom_out = QPushButton("-")
        btn_zoom_out.setFixedWidth(28)
        btn_zoom_out.setStyleSheet(self._BTN_STYLE)
        btn_zoom_out.setToolTip(tr("Zoom Out"))
        top_bar.addWidget(btn_zoom_out)
        btn_zoom_in = QPushButton("+")
        btn_zoom_in.setFixedWidth(28)
        btn_zoom_in.setStyleSheet(self._BTN_STYLE)
        btn_zoom_in.setToolTip(tr("Zoom In"))
        top_bar.addWidget(btn_zoom_in)

        top_bar.addSpacing(10)

        # Rotation controls
        btn_rot_left = QPushButton("\u21ba")
        btn_rot_left.setFixedWidth(28)
        btn_rot_left.setStyleSheet(self._BTN_STYLE)
        btn_rot_left.setToolTip(tr("Rotate Left 90\u00b0"))
        top_bar.addWidget(btn_rot_left)

        self._slider_rotation = QSlider(Qt.Orientation.Horizontal)
        self._slider_rotation.setRange(0, 360)
        self._slider_rotation.setValue(0)
        self._slider_rotation.setFixedWidth(120)
        self._slider_rotation.setToolTip(tr("Rotate image (0-360\u00b0)"))
        top_bar.addWidget(self._slider_rotation)

        btn_rot_right = QPushButton("\u21bb")
        btn_rot_right.setFixedWidth(28)
        btn_rot_right.setStyleSheet(self._BTN_STYLE)
        btn_rot_right.setToolTip(tr("Rotate Right 90\u00b0"))
        top_bar.addWidget(btn_rot_right)

        top_bar.addStretch()

        # Image adjustments
        for icon, tip, slider_cfg in [
            ("\u2600", tr("Brightness"), {"range": (-100, 100), "val": 0, "w": 80}),
            ("\u25d0", tr("Contrast"), {"range": (-100, 100), "val": 0, "w": 80}),
            ("\u03b3", tr("Gamma"), {"range": (20, 300), "val": 100, "w": 80}),
        ]:
            lbl = QLabel(icon)
            lbl.setStyleSheet("color: #888; font-size: 14px;")
            lbl.setToolTip(tip)
            top_bar.addWidget(lbl)
            sl = QSlider(Qt.Orientation.Horizontal)
            sl.setRange(*slider_cfg["range"])
            sl.setValue(slider_cfg["val"])
            sl.setFixedWidth(slider_cfg["w"])
            top_bar.addWidget(sl)

        # Extract slider references (added in order: brightness, contrast, gamma)
        sliders = [w for w in [top_bar.itemAt(i).widget() for i in range(top_bar.count())]
                    if isinstance(w, QSlider) and w is not self._slider_rotation]
        self._sl_brightness = sliders[0]
        self._sl_contrast = sliders[1]
        self._sl_gamma = sliders[2]

        self._btn_invert = QPushButton("\u25d1")
        self._btn_invert.setCheckable(True)
        self._btn_invert.setFixedWidth(28)
        self._btn_invert.setToolTip(tr("Invert Colors"))
        self._btn_invert.setStyleSheet(self._BTN_STYLE)
        top_bar.addWidget(self._btn_invert)

        btn_reset = QPushButton("\u21ba")
        btn_reset.setFixedWidth(28)
        btn_reset.setToolTip(tr("Reset adjustments"))
        btn_reset.setStyleSheet(self._BTN_STYLE)
        top_bar.addWidget(btn_reset)

        top_bar.addSpacing(15)

        # Close button -- subtle, matching toolbar theme
        btn_close = QPushButton("\u2715")
        btn_close.setFixedSize(28, 28)
        btn_close.setStyleSheet(self._BTN_STYLE)
        btn_close.setToolTip(tr("Close fullscreen (Esc)"))
        btn_close.clicked.connect(self.close)
        top_bar.addWidget(btn_close)

        layout.addWidget(top_bar_widget)

        # === Image area ===
        self._scroll_area = ZoomableScrollArea()
        layout.addWidget(self._scroll_area, 1)

        if pixmap and not pixmap.isNull():
            self._scroll_area.set_image(pixmap)

        # Wire zoom
        btn_zoom_out.clicked.connect(lambda: self._scroll_area.zoom_out())
        btn_zoom_in.clicked.connect(lambda: self._scroll_area.zoom_in())

        # Wire rotation
        self._slider_rotation.valueChanged.connect(
            lambda val: self._scroll_area.set_rotation(val))
        btn_rot_left.clicked.connect(lambda: self._adjust_rotation(-90))
        btn_rot_right.clicked.connect(lambda: self._adjust_rotation(90))

        # Wire adjustments
        self._sl_brightness.valueChanged.connect(
            lambda v: self._scroll_area.set_adjustments(brightness=v))
        self._sl_contrast.valueChanged.connect(
            lambda v: self._scroll_area.set_adjustments(contrast=v))
        self._sl_gamma.valueChanged.connect(
            lambda v: self._scroll_area.set_adjustments(gamma=v / 100.0))
        self._btn_invert.toggled.connect(
            lambda c: self._scroll_area.set_adjustments(invert=c))

        def _reset():
            self._sl_brightness.setValue(0)
            self._sl_contrast.setValue(0)
            self._sl_gamma.setValue(100)
            self._btn_invert.setChecked(False)
            self._scroll_area.reset_adjustments()
        btn_reset.clicked.connect(_reset)

        # Sync state from parent viewer
        if parent_viewer and hasattr(parent_viewer, 'scroll_area'):
            sa = parent_viewer.scroll_area
            self._sl_brightness.setValue(int(sa._brightness))
            self._sl_contrast.setValue(int(sa._contrast))
            self._sl_gamma.setValue(int(sa._gamma * 100))
            self._btn_invert.setChecked(sa._invert)
            if sa._rotation:
                self._slider_rotation.setValue(int(sa._rotation))

        self._update_page_label()

    def _adjust_rotation(self, delta):
        new_val = (self._slider_rotation.value() + delta) % 360
        self._slider_rotation.setValue(int(new_val))

    def _update_page_label(self):
        if self._parent_viewer:
            idx = self._parent_viewer.current_idx
            total = len(self._parent_viewer.active_list)
            if total > 0:
                self._lbl_page.setText(f"{idx + 1} / {total}")
                self._btn_prev.setEnabled(idx > 0)
                self._btn_next.setEnabled(idx < total - 1)

    def set_image(self, pixmap):
        """Update the displayed image (called when page changes)."""
        if pixmap and not pixmap.isNull():
            b = self._sl_brightness.value()
            c = self._sl_contrast.value()
            g = self._sl_gamma.value()
            inv = self._btn_invert.isChecked()
            self._scroll_area.set_image(pixmap)
            if b or c or g != 100 or inv:
                self._scroll_area.set_adjustments(
                    brightness=b, contrast=c, gamma=g / 100.0, invert=inv)
        self._update_page_label()

    def keyPressEvent(self, event):
        key = event.key()
        if key == Qt.Key.Key_Escape:
            self.close()
        elif key in (Qt.Key.Key_Right, Qt.Key.Key_Down):
            self.page_changed.emit(1)
        elif key in (Qt.Key.Key_Left, Qt.Key.Key_Up):
            self.page_changed.emit(-1)
        else:
            super().keyPressEvent(event)

    def showFullScreen(self):
        super().showFullScreen()
        self.activateWindow()
        self.raise_()
        self.setFocus()


class ManuscriptViewerWidget(QWidget):
    """Reusable widget for displaying manuscript images with navigation."""
    _thumbnail_ready = pyqtSignal(QPixmap, int, int)  # pixmap, page_index, load_generation

    def __init__(self, parent=None):
        super().__init__(parent)
        self.images_nli = []
        self.images_ext = []
        self.active_list = []
        self.current_idx = 0
        self._load_generation = 0  # increments on each set_page/load_images to reject stale callbacks
        self.loader_thread = None
        self.preload_worker = None
        self._inflight_threads = []  # Canceled-but-still-running QThreads kept alive until finished
        self.external_provider = None
        self._closing = False
        self._thumb_threads = []  # Track thumbnail threads for cleanup
        self._nav_debounce_timer = QTimer(self)  # Persistent QTimer for debouncing rapid set_page calls
        self._nav_debounce_timer.setSingleShot(True)
        self._nav_debounce_timer.timeout.connect(self._execute_set_page)
        self._pending_page_idx = None    # Deferred page index
        self._thumbnail_ready.connect(self._on_thumbnail_ready)
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
        self.slider_rotation.setMinimumWidth(60)
        self.slider_rotation.setMaximumWidth(160)
        self.slider_rotation.setToolTip(tr("Rotate image (0-360\u00b0)"))
        self.slider_rotation.valueChanged.connect(lambda val: self.scroll_area.set_rotation(val))

        btn_rot_left = QPushButton("\u21ba")
        btn_rot_left.setToolTip(tr("Rotate Left 90\u00b0"))
        btn_rot_left.setFixedWidth(30)
        btn_rot_left.clicked.connect(lambda: self.adjust_rotation(-90))

        btn_rot_right = QPushButton("\u21bb")
        btn_rot_right.setToolTip(tr("Rotate Right 90\u00b0"))
        btn_rot_right.setFixedWidth(30)
        btn_rot_right.clicked.connect(lambda: self.adjust_rotation(90))

        btn_rot_reset = QPushButton(f"\u21a9\ufe0f {tr('Reset')}")
        btn_rot_reset.setToolTip(tr("Reset rotation"))
        btn_rot_reset.setMaximumWidth(60)
        btn_rot_reset.clicked.connect(lambda: self.slider_rotation.setValue(0))

        self.btn_external = QPushButton(f"\U0001f517 {tr('External')}")
        self.btn_external.setToolTip(tr("External Website"))
        self.btn_external.setVisible(False)
        self.btn_external.clicked.connect(self.open_external)

        # KTIV / NLI Viewer button (Phase 31)
        self.btn_ktiv = QPushButton(f"\U0001f517 {tr('Ktiv')}")
        self.btn_ktiv.setToolTip(tr("View on Ktiv"))
        self.btn_ktiv.setVisible(False)
        self.btn_ktiv.clicked.connect(self._open_ktiv_viewer)
        self._ktiv_sys_id = None

        top_bar.addWidget(self.combo_source)
        top_bar.addStretch()
        top_bar.addWidget(self.btn_ktiv)
        top_bar.addWidget(self.btn_external)
        top_bar.addWidget(btn_rot_left)
        top_bar.addWidget(self.slider_rotation)
        top_bar.addWidget(btn_rot_right)
        top_bar.addWidget(btn_rot_reset)
        top_bar.addSpacing(10)
        top_bar.addWidget(btn_zoom_out)
        top_bar.addWidget(btn_zoom_in)

        # Fullscreen button
        btn_fullscreen = QPushButton("\u26f6")
        btn_fullscreen.setToolTip(tr("Fullscreen"))
        btn_fullscreen.setFixedWidth(30)
        btn_fullscreen.clicked.connect(self._open_fullscreen)
        top_bar.addWidget(btn_fullscreen)
        self._fullscreen_window = None

        layout.addWidget(_make_scrollable_row(top_bar))

        # Image adjustment controls bar
        adj_bar = QHBoxLayout()
        adj_bar.setContentsMargins(5, 2, 5, 2)

        lbl_b = QLabel("\u2600")  # Sun symbol for brightness
        lbl_b.setStyleSheet("font-size: 14px; color: #888;")
        lbl_b.setToolTip(tr("Brightness"))
        adj_bar.addWidget(lbl_b)
        self.slider_brightness = QSlider(Qt.Orientation.Horizontal)
        self.slider_brightness.setRange(-100, 100)
        self.slider_brightness.setValue(0)
        self.slider_brightness.setMinimumWidth(40)
        self.slider_brightness.setMaximumWidth(100)
        self.slider_brightness.setToolTip(tr("Brightness"))
        adj_bar.addWidget(self.slider_brightness)

        lbl_c = QLabel("\u25d0")  # Half-filled circle for contrast
        lbl_c.setStyleSheet("font-size: 14px; color: #888;")
        lbl_c.setToolTip(tr("Contrast"))
        adj_bar.addWidget(lbl_c)
        self.slider_contrast = QSlider(Qt.Orientation.Horizontal)
        self.slider_contrast.setRange(-100, 100)
        self.slider_contrast.setValue(0)
        self.slider_contrast.setMinimumWidth(40)
        self.slider_contrast.setMaximumWidth(100)
        self.slider_contrast.setToolTip(tr("Contrast"))
        adj_bar.addWidget(self.slider_contrast)

        lbl_g = QLabel("\u03b3")  # Greek gamma letter
        lbl_g.setStyleSheet("font-size: 14px; color: #888; font-style: italic;")
        lbl_g.setToolTip(tr("Gamma"))
        adj_bar.addWidget(lbl_g)
        self.slider_gamma = QSlider(Qt.Orientation.Horizontal)
        self.slider_gamma.setRange(20, 300)
        self.slider_gamma.setValue(100)
        self.slider_gamma.setMinimumWidth(40)
        self.slider_gamma.setMaximumWidth(100)
        self.slider_gamma.setToolTip(tr("Gamma"))
        adj_bar.addWidget(self.slider_gamma)

        self.btn_invert = QPushButton("\u25d1")  # Circle with right half black -- invert
        self.btn_invert.setCheckable(True)
        self.btn_invert.setFixedWidth(30)
        self.btn_invert.setToolTip(tr("Invert Colors"))
        adj_bar.addWidget(self.btn_invert)

        btn_reset_adj = QPushButton("\u21ba")  # Circular arrow -- reset
        btn_reset_adj.setFixedWidth(30)
        btn_reset_adj.setToolTip(tr("Reset Image"))
        adj_bar.addWidget(btn_reset_adj)

        adj_bar.addStretch()

        # Connect adjustment controls
        self.slider_brightness.valueChanged.connect(
            lambda val: self.scroll_area.set_adjustments(brightness=val))
        self.slider_contrast.valueChanged.connect(
            lambda val: self.scroll_area.set_adjustments(contrast=val))
        self.slider_gamma.valueChanged.connect(
            lambda val: self.scroll_area.set_adjustments(gamma=val / 100.0))
        self.btn_invert.toggled.connect(
            lambda checked: self.scroll_area.set_adjustments(invert=checked))

        def _reset_adjustments():
            self.slider_brightness.setValue(0)
            self.slider_contrast.setValue(0)
            self.slider_gamma.setValue(100)
            self.btn_invert.setChecked(False)
            self.scroll_area.reset_adjustments()

        btn_reset_adj.clicked.connect(_reset_adjustments)

        layout.addWidget(_make_scrollable_row(adj_bar))

        # Attribution
        self.lbl_attribution = QLabel("")
        self.lbl_attribution.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_attribution.setWordWrap(True)
        self.lbl_attribution.setStyleSheet("font-size: 10px; color: #7f8c8d; background: transparent; margin: 0px;")
        self.lbl_attribution.setVisible(False)
        layout.addWidget(self.lbl_attribution)

        # Image Area
        self.scroll_area = ZoomableScrollArea()
        layout.addWidget(self.scroll_area, 1)

    def _detect_external_provider(self, meta):
        # Check explicit provider from enrich_metadata (set by Plan 03 for Manchester/JTS)
        if meta.get('external_provider'):
            return meta['external_provider']

        marc = meta.get('marc', {}) if meta else {}
        url = (marc.get('external_iiif_link') or "").lower()
        if "cudl.lib.cam.ac.uk" in url:
            return "cambridge"

        for img in meta.get('images_ext', []) or []:
            img_url = (img.get('url', '').lower())
            if "cudl.lib.cam.ac.uk" in img_url:
                return "cambridge"
            if "bodleian.ox.ac.uk" in img_url or "hebrew.bodleian" in img_url:
                return "oxford"
            if "luna.manchester.ac.uk" in img_url:
                return "manchester"
            if "iiif-cloud.princeton.edu" in img_url or "figgy.princeton.edu" in img_url:
                return "jts"

        # Check for Oxford Part ID
        if meta.get('oxford_part_id'):
            return "oxford"

        return None

    def set_image_by_fl_id(self, fl_id):
        digits = re.sub(r"\D", "", str(fl_id or ""))
        if not digits:
            return False

        fallback_url = f"{Config.NLI_IIIF_BASE}/FL{digits}/full/2000,/0/default.jpg"
        self.images_nli = [{'label': f"FL{digits}", 'url': fallback_url, 'fl_id': digits}]
        self.images_ext = []
        self.active_list = self.images_nli
        self.current_source = "nli"
        self.combo_source.clear()
        self.combo_source.addItem(f"NLI (1 page)", "nli")
        self.combo_source.setVisible(False)
        self.btn_ktiv.setVisible(False)
        self._ktiv_sys_id = None
        return True

    def load_images(self, meta, initial_idx=0, target_folio=None):
        self.external_provider = self._detect_external_provider(meta)
        self._current_meta = meta  # Store for dynamic image generation

        # Attribution
        attr = meta.get('attribution')
        if attr:
            self.lbl_attribution.setText(attr)
            self.lbl_attribution.setVisible(True)
        else:
            self.lbl_attribution.setVisible(False)

        # meta contains 'images_nli' and 'images_ext'
        # Make copies to avoid modifying the cached meta
        self.images_nli = list(meta.get('images_nli', []))
        self.images_ext = list(meta.get('images_ext', []))

        # For Oxford: check if target_folio is missing and add dynamic images
        if target_folio is not None and self.images_ext:
            folio_in_list = any(img.get('folio_num') == target_folio for img in self.images_ext)
            if not folio_in_list:
                oxford_part_id = meta.get('oxford_part_id', '')
                oxford_part_meta = meta.get('oxford_part_metadata', {})
                folio_range = oxford_part_meta.get('folio_range', [])
                if oxford_part_id and len(folio_range) >= 2 and folio_range[0] <= target_folio <= folio_range[1]:
                    # Generate dynamic URLs for this folio
                    for side in ['a', 'b']:
                        url = _generate_oxford_dynamic_url(oxford_part_id, target_folio, side)
                        if url:
                            self.images_ext.append({
                                'label': f'{target_folio}{side}',
                                'url': url,
                                'folio_num': target_folio,
                                '_dynamic': True
                            })
                    # Update initial_idx to point to the new recto image
                    initial_idx = len(self.images_ext) - 2  # Index of recto (a)

        # Determine default source
        self.combo_source.blockSignals(True)
        self.combo_source.clear()

        if self.images_ext:
            if self.external_provider == "cambridge":
                ext_label = "Cambridge"
            elif self.external_provider == "oxford":
                ext_label = "Oxford"
            elif self.external_provider == "manchester":
                ext_label = "Manchester"
            elif self.external_provider == "jts":
                ext_label = "JTS/Princeton"
            else:
                ext_label = "External"
            # 260419-cfx / 260421-aln: when external is misaligned with NLI
            # (count OR per-position mismatch via classify_cambridge_alignment),
            # default to NLI (transcription aligns with NLI canvases). Both
            # sources still appear in the combo so the user can manually
            # switch. Oxford is exempt — it doesn't have a parallel NLI list
            # in the same sense and its positional mapping is handled elsewhere.
            # For non-CUL external providers (Manchester, JTS, non-CUL Cambridge)
            # the alignment verdict is not computed; legacy length check is
            # kept as a defensive fallback.
            _align = (meta or {}).get('cambridge_alignment') or {}
            _verdict_misaligned = _align.get('verdict') == 'misaligned'
            _legacy_count_mismatch = (
                not _align
                and self.external_provider != "oxford"
                and self.images_nli
                and len(self.images_ext) != len(self.images_nli)
            )
            _count_mismatch = (
                self.external_provider != "oxford"
                and self.images_nli
                and (_verdict_misaligned or _legacy_count_mismatch)
            )
            if _count_mismatch:
                self.combo_source.addItem(f"NLI ({len(self.images_nli)} pages)", "nli")
                self.combo_source.addItem(f"{ext_label} ({len(self.images_ext)} pages)", "ext")
                self.active_list = self.images_nli
                self.current_source = "nli"
            else:
                self.combo_source.addItem(f"{ext_label} ({len(self.images_ext)} pages)", "ext")
                if self.images_nli:
                    self.combo_source.addItem(f"NLI ({len(self.images_nli)} pages)", "nli")
                self.active_list = self.images_ext
                self.current_source = "ext"
        elif self.images_nli:
            self.combo_source.addItem(f"NLI ({len(self.images_nli)} pages)", "nli")
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
        self.external_url = meta.get('external_url') or marc.get('external_iiif_link')

        # Prefer library_viewer_url when available (detail page > generic manifest URL)
        lib_viewer = meta.get('library_viewer_url')
        if lib_viewer and lib_viewer.get('url'):
            if self.external_provider in ('manchester', 'jts'):
                self.external_url = lib_viewer['url']

        if self.external_url:
            if self.external_provider == "cambridge":
                btn_label = f"\U0001f517 {tr('Cambridge')}"
            elif self.external_provider == "oxford":
                btn_label = f"\U0001f517 {tr('Oxford')}"
            elif self.external_provider == "manchester":
                btn_label = "\U0001f517 Manchester"
            elif self.external_provider == "jts":
                btn_label = "\U0001f517 Princeton"
            else:
                btn_label = f"\U0001f517 {tr('External')}"
            self.btn_external.setText(btn_label)
        self.btn_external.setVisible(bool(self.external_url))

        # KTIV button: show when NLI FGP images available (Phase 31)
        image_source_info = meta.get('image_source_info', {})
        if image_source_info.get('nli_fgp'):
            # Use sys_id from meta or try fl_id-based detection
            sys_id = meta.get('sys_id', '')
            if not sys_id:
                # Try to get sys_id from the fl_ids
                fl_ids = meta.get('fl_ids', [])
                if isinstance(fl_ids, str):
                    fl_ids = [fl_ids]
            self._ktiv_sys_id = sys_id
            self.btn_ktiv.setVisible(bool(sys_id))
        else:
            self._ktiv_sys_id = None
            self.btn_ktiv.setVisible(False)

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

        self.set_page(self.current_idx)

    def _resolve_url(self, base_url):
        if not base_url: return None
        if base_url.endswith('.jpg'): return base_url
        return f"{base_url}/full/2000,/0/default.jpg"

    def _retire_thread(self, thread):
        """Move a canceled QThread to the in-flight list so it stays alive until finished."""
        if thread is None:
            return
        thread.cancel()
        try:
            thread.image_loaded.disconnect()
            thread.load_failed.disconnect()
        except (TypeError, RuntimeError):
            pass
        if thread.isRunning():
            self._inflight_threads.append(thread)
            thread.finished.connect(lambda t=thread: self._cleanup_inflight(t))
        else:
            thread.deleteLater()

    def _cleanup_inflight(self, thread):
        """Remove a finished thread from the in-flight list and schedule deletion."""
        try:
            self._inflight_threads.remove(thread)
        except ValueError:
            pass
        thread.deleteLater()

    def _preload(self, index):
        if index < 0 or index >= len(self.active_list): return
        url = self.active_list[index]['url']
        final = self._resolve_url(url)

        # Retire previous preload worker safely
        if self.preload_worker:
            self._retire_thread(self.preload_worker)

        # Spawn thread without connecting signals (just for cache)
        self.preload_worker = ImageLoaderThread(final)
        self.preload_worker.start()

    def _wait_or_terminate(self, thread, timeout_ms=2000):
        """Wait for a QThread to finish; terminate as last resort to prevent destroyed-while-running."""
        thread.cancel()
        if not thread.wait(timeout_ms):
            logger.warning("Image thread did not finish in %dms, terminating", timeout_ms)
            thread.terminate()
            thread.wait()

    def stop_threads(self):
        """Stop all running image loading threads. Call before destroying widget."""
        self._closing = True
        self._nav_debounce_timer.stop()
        # Cancel active threads
        for thread in [self.loader_thread, self.preload_worker]:
            if thread and thread.isRunning():
                try:
                    thread.image_loaded.disconnect()
                    thread.load_failed.disconnect()
                except (TypeError, RuntimeError):
                    pass
                self._wait_or_terminate(thread)
        # Wait on any in-flight retired threads
        for thread in list(self._inflight_threads):
            self._wait_or_terminate(thread)
        self._inflight_threads.clear()

    def _on_thumbnail_ready(self, pix, page_idx, generation):
        """Handle thumbnail loaded signal - only display if still on same page and same load generation."""
        if self._closing:
            return
        if self._load_generation == generation and self.current_idx == page_idx and pix and not pix.isNull():
            self.scroll_area.set_image(pix)
            self._sync_fullscreen_image()

    def _load_thumbnail_async(self, thumb_url):
        """Load thumbnail asynchronously for quick display while full image loads."""
        current_idx = self.current_idx  # Capture current state
        generation = self._load_generation  # Capture generation to reject stale callbacks
        signal = self._thumbnail_ready  # Capture signal reference for thread
        closing_ref = lambda: self._closing  # Capture closing flag check

        def fetch_and_emit():
            try:
                if closing_ref():
                    return
                import urllib.request
                req = urllib.request.Request(thumb_url, headers=Config.HTTP_HEADERS)
                with urllib.request.urlopen(req, timeout=3) as response:
                    data = response.read()
                    if data and not closing_ref():
                        image = QImage()
                        if image.loadFromData(data):
                            pix = QPixmap.fromImage(image)
                            signal.emit(pix, current_idx, generation)
            except Exception:
                pass  # Thumbnail load failed, full image will replace it

        # Run in background thread to avoid blocking UI
        t = threading.Thread(target=fetch_and_emit, daemon=True)
        # Track thread references to prevent premature GC
        self._thumb_threads = [t2 for t2 in self._thumb_threads if t2.is_alive()]
        self._thumb_threads.append(t)
        t.start()

    def set_page(self, index):
        if not self.active_list:
            self.scroll_area.set_image(None)
            self.scroll_area.set_status_message(tr("No images available"))
            return

        # Bounds check
        if index < 0: index = 0
        if index >= len(self.active_list): index = len(self.active_list) - 1

        self.current_idx = index
        self._load_generation += 1  # Invalidate any in-flight callbacks immediately

        # Update status text immediately for responsiveness
        self.scroll_area.set_status_message(tr("Loading..."))

        # Store pending index and restart debounce timer (persistent, not recreated)
        self._pending_page_idx = index
        self._nav_debounce_timer.start(150)  # 150ms debounce

    def _execute_set_page(self):
        """Actually load the image after debounce settles."""
        index = self._pending_page_idx
        if index is None or self._closing:
            return

        # Re-check bounds (active_list may have changed)
        if not self.active_list:
            return
        if index < 0: index = 0
        if index >= len(self.active_list): index = len(self.active_list) - 1

        self.current_idx = index
        self._load_generation += 1  # Fresh generation for actual load
        gen = self._load_generation

        img_data = self.active_list[index]
        base_url = img_data['url']

        # Check for thumbnail URL (Oxford images have this)
        thumb_url = img_data.get('thumb_url', '')
        # For NLI IIIF images, auto-generate a fast preview URL (400px)
        if not thumb_url and 'iiif.nli.org.il' in base_url:
            thumb_url = f"{base_url}/full/400,/0/default.jpg"

        # Retire previous loader safely (stays alive in _inflight_threads until finished)
        if self.loader_thread:
            self._retire_thread(self.loader_thread)

        # Load low-res preview first for instant display, then high-res replaces it
        if thumb_url:
            self._load_thumbnail_async(thumb_url)

        final_url = self._resolve_url(base_url)

        self.loader_thread = ImageLoaderThread(final_url)
        self.loader_thread.image_loaded.connect(
            lambda img, g=gen: self.display_image(img) if g == self._load_generation and not self._closing else None
        )
        self.loader_thread.load_failed.connect(
            lambda g=gen: None if g != self._load_generation or self._closing else self.scroll_area.set_status_message(tr("No Image"))
        )
        self.loader_thread.start()

        # Preload next image
        self._preload(index + 1)

    def display_image(self, image):
        if self._closing:
            return
        pix = QPixmap.fromImage(image)
        self.scroll_area.set_image(pix)
        self._sync_fullscreen_image()
        self.slider_rotation.setValue(0)
        # Reset adjustment controls on new image
        self.slider_brightness.setValue(0)
        self.slider_contrast.setValue(0)
        self.slider_gamma.setValue(100)
        self.btn_invert.setChecked(False)

    def open_external(self):
        if self.external_url:
            url = self.external_url
            # Transform CUDL IIIF manifest URL to viewer URL
            if "cudl.lib.cam.ac.uk/iiif/" in url:
                url = url.replace("/iiif/", "/view/")
            QDesktopServices.openUrl(QUrl(url))

    def _open_ktiv_viewer(self):
        """Open the NLI KTIV manuscript viewer at the current page."""
        if self._ktiv_sys_id:
            # Use docid query param (not hash fragment) -- hash-based URLs fail on direct navigation
            docid = f"PNX_MANUSCRIPTS{self._ktiv_sys_id}-1"
            # Append FL ID to navigate to the current page
            if self.active_list and self.current_source == "nli" and 0 <= self.current_idx < len(self.active_list):
                fl_id = self.active_list[self.current_idx].get('fl_id')
                if fl_id:
                    docid += f",FL{fl_id}"
            url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/viewerpage?vid=MANUSCRIPTS&docid={docid}"
            QDesktopServices.openUrl(QUrl(url))

    def _open_fullscreen(self):
        """Open current image in fullscreen window."""
        pixmap = self.scroll_area._pixmap
        if not pixmap or pixmap.isNull():
            return
        win = FullscreenImageWindow(pixmap, parent_viewer=self)
        win.page_changed.connect(self._on_fullscreen_page_change)
        self._fullscreen_window = win
        win.showFullScreen()

    def _on_fullscreen_page_change(self, delta):
        """Handle page navigation from fullscreen window."""
        new_idx = self.current_idx + delta
        if new_idx < 0 or new_idx >= len(self.active_list):
            return
        self.set_page(new_idx)

    def _sync_fullscreen_image(self):
        """Push current image to the fullscreen window if open."""
        if self._fullscreen_window and not sip.isdeleted(self._fullscreen_window):
            pixmap = self.scroll_area._pixmap
            if pixmap and not pixmap.isNull():
                self._fullscreen_window.set_image(pixmap)

    def adjust_rotation(self, delta):
        """Adjust rotation via slider to keep controls in sync."""
        new_val = (self.slider_rotation.value() + delta) % 360
        self.slider_rotation.setValue(int(new_val))
