"""Puzzle/join canvas classes extracted from genizah_app.py (v7.9 decomposition)."""

import math
import os

from functools import partial

from PyQt6.QtWidgets import (
    QApplication, QComboBox, QCompleter, QDialog, QDockWidget,
    QFileDialog, QGraphicsItem, QGraphicsPixmapItem, QGraphicsTextItem,
    QGraphicsScene, QGraphicsView, QGroupBox, QHBoxLayout, QInputDialog,
    QLabel, QLineEdit, QListWidget, QListWidgetItem, QMainWindow, QMenu,
    QMessageBox, QPushButton, QProgressDialog, QSlider, QTextEdit,
    QVBoxLayout, QWidget,
)
from PyQt6.QtCore import Qt, QRectF, QSize, QPointF, QTimer, pyqtSignal, QThread
from PyQt6.QtGui import (
    QAction, QBrush, QColor, QCursor, QIcon, QImage,
    QPainter, QPainterPath, QPen, QPixmap, QTransform,
)
from PyQt6 import sip

from genizah_core import get_logger, normalize_shelfmark, tr
from gui_threads import PuzzleImageLoaderThread, PuzzleMetaLoaderThread

logger = get_logger(__name__)


class PuzzleFragmentItem(QGraphicsPixmapItem):
    """A positioned fragment image on the puzzle canvas.

    Supports drag, corner-handle rotation, border-handle resize (aspect-locked),
    flip H/V, wheel-resize, Shift-snap to 20px grid, multi-select with visual handles.
    """

    HANDLE_SIZE = 10  # handle visual half-size in pixels
    _BORDER_ZONE = 30  # width of the interactive border strip (pixels)
    _CORNER_ZONE = 40  # corner zone size (pixels from corner)

    # Handle identifiers
    _H_NONE = 0
    _H_TL = 1; _H_TR = 2; _H_BL = 3; _H_BR = 4  # corners = rotate
    _H_T = 5; _H_B = 6; _H_L = 7; _H_R = 8       # edges = resize (or crop)

    def __init__(self, puzzle_frag, pixmap, parent=None):
        super().__init__(pixmap, parent)
        self.puzzle_frag = puzzle_frag

        # Enable interaction flags
        self.setFlags(
            QGraphicsItem.GraphicsItemFlag.ItemIsMovable
            | QGraphicsItem.GraphicsItemFlag.ItemIsSelectable
            | QGraphicsItem.GraphicsItemFlag.ItemSendsGeometryChanges
        )
        self.setTransformationMode(Qt.TransformationMode.SmoothTransformation)
        self.setTransformOriginPoint(self._pixmap_rect().center())
        self.setAcceptHoverEvents(True)

        # Apply initial state from data model
        self.setPos(puzzle_frag.x, puzzle_frag.y)
        self.setRotation(puzzle_frag.rotation)
        self.setScale(puzzle_frag.scale)
        self._apply_flip()

        # Interaction state
        self._rotating = False
        self._rotation_start_angle = 0.0
        self._resizing = False
        self._resize_handle = self._H_NONE
        self._resize_start_pos = QPointF()
        self._resize_start_scale = 1.0

    def _pixmap_rect(self):
        """The actual pixmap bounding rect (without handle margin)."""
        return super().boundingRect()

    def _handle_points(self):
        """Return dict of handle_id -> QPointF center positions."""
        br = self._pixmap_rect()
        mx, my = br.center().x(), br.center().y()
        return {
            self._H_TL: br.topLeft(), self._H_TR: br.topRight(),
            self._H_BL: br.bottomLeft(), self._H_BR: br.bottomRight(),
            self._H_T: QPointF(mx, br.top()), self._H_B: QPointF(mx, br.bottom()),
            self._H_L: QPointF(br.left(), my), self._H_R: QPointF(br.right(), my),
        }

    def _hit_handle(self, pos):
        """Return handle id under pos using wide border zones.

        The entire border strip (_BORDER_ZONE px from edge) is interactive.
        Corners (within _CORNER_ZONE of each corner) -> rotate handles.
        Edge strips between corners -> resize/crop handles.
        """
        br = self._pixmap_rect()
        x, y = pos.x(), pos.y()
        bz = self._BORDER_ZONE
        cz = self._CORNER_ZONE

        # Check if in the border zone at all
        near_top = y < br.top() + bz
        near_bottom = y > br.bottom() - bz
        near_left = x < br.left() + bz
        near_right = x > br.right() - bz

        if not (near_top or near_bottom or near_left or near_right):
            return self._H_NONE

        # Corner zones (priority over edges)
        if near_top and near_left and (x < br.left() + cz) and (y < br.top() + cz):
            return self._H_TL
        if near_top and near_right and (x > br.right() - cz) and (y < br.top() + cz):
            return self._H_TR
        if near_bottom and near_left and (x < br.left() + cz) and (y > br.bottom() - cz):
            return self._H_BL
        if near_bottom and near_right and (x > br.right() - cz) and (y > br.bottom() - cz):
            return self._H_BR

        # Edge zones
        if near_top:
            return self._H_T
        if near_bottom:
            return self._H_B
        if near_left:
            return self._H_L
        if near_right:
            return self._H_R

        return self._H_NONE

    # -- Flip --

    def _apply_flip(self):
        """Apply horizontal/vertical flip via QTransform."""
        t = QTransform()
        pr = self._pixmap_rect()
        if self.puzzle_frag.flip_h:
            t.scale(-1, 1)
            t.translate(-pr.width(), 0)
        if self.puzzle_frag.flip_v:
            t.scale(1, -1)
            t.translate(0, -pr.height())
        self.setTransform(t)

    def flip_horizontal(self):
        self.puzzle_frag.flip_h = not self.puzzle_frag.flip_h
        self._apply_flip()

    def flip_vertical(self):
        self.puzzle_frag.flip_v = not self.puzzle_frag.flip_v
        self._apply_flip()

    # -- Mouse interaction --

    def _is_crop_mode(self):
        """Check if crop mode is active (set by PuzzleCanvasWindow)."""
        return getattr(self, '_crop_mode', False)

    def mousePressEvent(self, event):
        if self.isSelected():
            hid = self._hit_handle(event.pos())
            if self._is_crop_mode() and hid in (self._H_T, self._H_B, self._H_L, self._H_R):
                # Crop mode: edge drag crops
                self._cropping = True
                self._crop_handle = hid
                self._crop_start_pos = event.pos()
                # Save original for revert
                if not hasattr(self, '_original_pixmap') or self._original_pixmap is None:
                    self._original_pixmap = self.pixmap().copy()
                    self._crop_offsets = [0, 0, 0, 0]
                event.accept()
                return
            elif hid in (self._H_TL, self._H_TR, self._H_BL, self._H_BR):
                # Corner = rotation
                self._rotating = True
                center = self._pixmap_rect().center()
                pos = event.pos()
                self._rotation_start_angle = (
                    math.degrees(math.atan2(pos.y() - center.y(), pos.x() - center.x()))
                    - self.rotation()
                )
                event.accept()
                return
            elif hid != self._H_NONE:
                # Edge = resize (aspect-locked)
                self._resizing = True
                self._resize_handle = hid
                self._resize_start_pos = event.pos()
                self._resize_start_scale = self.scale()
                scene = self.scene()
                if scene:
                    for sel_item in scene.selectedItems():
                        if isinstance(sel_item, PuzzleFragmentItem):
                            sel_item._group_resize_base = sel_item.puzzle_frag.scale
                event.accept()
                return
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if getattr(self, '_cropping', False):
            delta = event.pos() - self._crop_start_pos
            hid = self._crop_handle
            pm = self.pixmap()
            if pm.isNull():
                event.accept()
                return
            w, h = pm.width(), pm.height()
            # Compute how many pixels to crop based on drag distance
            if hid == self._H_T:
                amount = max(0, int(delta.y()))
            elif hid == self._H_B:
                amount = max(0, int(-delta.y()))
            elif hid == self._H_L:
                amount = max(0, int(delta.x()))
            elif hid == self._H_R:
                amount = max(0, int(-delta.x()))
            else:
                amount = 0
            # Live preview: show crop overlay via update
            self._crop_preview_amount = amount
            self.update()
            event.accept()
            return

        if self._rotating:
            center = self._pixmap_rect().center()
            pos = event.pos()
            angle = math.degrees(math.atan2(pos.y() - center.y(), pos.x() - center.x()))
            new_rotation = (angle - self._rotation_start_angle) % 360
            old_rotation = self.rotation()
            delta_rot = new_rotation - old_rotation
            self.setRotation(new_rotation)
            self.puzzle_frag.rotation = new_rotation
            # Apply same rotation delta to all other selected items
            scene = self.scene()
            if scene:
                for sel_item in scene.selectedItems():
                    if isinstance(sel_item, PuzzleFragmentItem) and sel_item is not self:
                        r = (sel_item.rotation() + delta_rot) % 360
                        sel_item.setRotation(r)
                        sel_item.puzzle_frag.rotation = r
            event.accept()
            return

        if self._resizing:
            delta = event.pos() - self._resize_start_pos
            # Use the drag axis that matches the handle direction
            if self._resize_handle in (self._H_T, self._H_B):
                d = -delta.y() if self._resize_handle == self._H_T else delta.y()
            elif self._resize_handle in (self._H_L, self._H_R):
                d = -delta.x() if self._resize_handle == self._H_L else delta.x()
            else:
                d = 0
            pr = self._pixmap_rect()
            ref_size = max(pr.width(), pr.height(), 1)
            factor = 1.0 + d / ref_size
            new_scale = max(0.1, min(4.0, self._resize_start_scale * factor))
            # Apply proportional resize to all selected items
            ratio = new_scale / self._resize_start_scale if self._resize_start_scale > 0 else 1.0
            scene = self.scene()
            if scene:
                for sel_item in scene.selectedItems():
                    if isinstance(sel_item, PuzzleFragmentItem):
                        if sel_item is self:
                            sel_item.prepareGeometryChange()
                            sel_item.setScale(new_scale)
                            sel_item.puzzle_frag.scale = new_scale
                        else:
                            base = getattr(sel_item, '_group_resize_base', sel_item.puzzle_frag.scale)
                            s = max(0.1, min(4.0, base * ratio))
                            sel_item.prepareGeometryChange()
                            sel_item.setScale(s)
                            sel_item.puzzle_frag.scale = s
            else:
                self.prepareGeometryChange()
                self.setScale(new_scale)
                self.puzzle_frag.scale = new_scale
            event.accept()
            return

        super().mouseMoveEvent(event)

        # Shift-snap to 20px grid
        if event.modifiers() & Qt.KeyboardModifier.ShiftModifier:
            x, y = self.pos().x(), self.pos().y()
            self.setPos(round(x / 20) * 20, round(y / 20) * 20)

        # Sync position back to data model
        self.puzzle_frag.x = self.pos().x()
        self.puzzle_frag.y = self.pos().y()

    def mouseReleaseEvent(self, event):
        if getattr(self, '_cropping', False):
            # Apply the crop
            amount = getattr(self, '_crop_preview_amount', 0)
            if amount > 0:
                hid = self._crop_handle
                edge = {self._H_T: "top", self._H_B: "bottom",
                        self._H_L: "left", self._H_R: "right"}.get(hid)
                if edge:
                    pm = self.pixmap()
                    w, h = pm.width(), pm.height()
                    a = min(amount, w // 3 if edge in ("left", "right") else h // 3)
                    t = a if edge == "top" else 0
                    b = a if edge == "bottom" else 0
                    l = a if edge == "left" else 0
                    r = a if edge == "right" else 0
                    if w - l - r >= 50 and h - t - b >= 50:
                        cropped = pm.copy(l, t, w - l - r, h - t - b)
                        self._crop_offsets[0] += t
                        self._crop_offsets[1] += b
                        self._crop_offsets[2] += l
                        self._crop_offsets[3] += r
                        self.prepareGeometryChange()
                        self.setPixmap(cropped)
                        self.setTransformOriginPoint(self._pixmap_rect().center())
                        self.setPos(self.pos().x() + l * self.scale(),
                                    self.pos().y() + t * self.scale())
                        self.puzzle_frag.x = self.pos().x()
                        self.puzzle_frag.y = self.pos().y()
            self._cropping = False
            self._crop_handle = self._H_NONE
            self._crop_preview_amount = 0
            self.update()
            event.accept()
            return
        self._rotating = False
        self._resizing = False
        self._resize_handle = self._H_NONE
        self.setCursor(Qt.CursorShape.ArrowCursor)
        super().mouseReleaseEvent(event)

    # -- Hover cursor --

    def hoverMoveEvent(self, event):
        if self.isSelected():
            hid = self._hit_handle(event.pos())
            crop = self._is_crop_mode()
            if hid in (self._H_TL, self._H_TR, self._H_BL, self._H_BR):
                self.setCursor(Qt.CursorShape.CrossCursor)  # rotation
            elif hid in (self._H_L, self._H_R):
                self.setCursor(Qt.CursorShape.SplitHCursor if crop else Qt.CursorShape.SizeHorCursor)
            elif hid in (self._H_T, self._H_B):
                self.setCursor(Qt.CursorShape.SplitVCursor if crop else Qt.CursorShape.SizeVerCursor)
            else:
                self.setCursor(Qt.CursorShape.SizeAllCursor)
        else:
            self.setCursor(Qt.CursorShape.ArrowCursor)
        super().hoverMoveEvent(event)

    def hoverLeaveEvent(self, event):
        self.setCursor(Qt.CursorShape.ArrowCursor)
        super().hoverLeaveEvent(event)

    # -- Resize (wheel) --

    def adjust_scale_from_wheel(self, delta_y):
        """Resize fragment from a wheel delta (called by PuzzleCanvasView)."""
        factor = 1.05 if delta_y > 0 else 0.95
        new_scale = max(0.1, min(4.0, self.scale() * factor))
        self.prepareGeometryChange()
        self.setScale(new_scale)
        self.puzzle_frag.scale = new_scale

    def wheelEvent(self, event):
        delta = event.delta() if hasattr(event, 'delta') else event.angleDelta().y()
        self.adjust_scale_from_wheel(delta)
        event.accept()

    # -- Visual feedback --

    def boundingRect(self):
        """Always include handle margin so Qt repaints handle areas on move."""
        br = self._pixmap_rect()
        m = self.HANDLE_SIZE + 2
        return br.adjusted(-m, -m, m, m)

    def paint(self, painter, option, widget=None):
        # Draw the pixmap in its original rect
        painter.drawPixmap(self._pixmap_rect().topLeft().toPoint(), self.pixmap())
        if not self.isSelected():
            return
        pr = self._pixmap_rect()
        crop = self._is_crop_mode()

        # Crop preview overlay -- darken the area being cropped
        preview_amt = getattr(self, '_crop_preview_amount', 0)
        if crop and preview_amt > 0 and getattr(self, '_cropping', False):
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(QBrush(QColor(255, 0, 0, 80)))
            hid = getattr(self, '_crop_handle', self._H_NONE)
            a = preview_amt
            if hid == self._H_T:
                painter.drawRect(QRectF(pr.left(), pr.top(), pr.width(), min(a, pr.height() / 3)))
            elif hid == self._H_B:
                painter.drawRect(QRectF(pr.left(), pr.bottom() - min(a, pr.height() / 3), pr.width(), min(a, pr.height() / 3)))
            elif hid == self._H_L:
                painter.drawRect(QRectF(pr.left(), pr.top(), min(a, pr.width() / 3), pr.height()))
            elif hid == self._H_R:
                painter.drawRect(QRectF(pr.right() - min(a, pr.width() / 3), pr.top(), min(a, pr.width() / 3), pr.height()))

        # Selection border
        border_color = QColor(255, 120, 50, 200) if crop else QColor(255, 255, 255, 180)
        painter.setPen(QPen(border_color, 2 if crop else 1, Qt.PenStyle.DashLine))
        painter.setBrush(Qt.BrushStyle.NoBrush)
        painter.drawRect(pr)

        # Corner handles = circles (rotation)
        hs = self.HANDLE_SIZE
        painter.setPen(QPen(QColor(255, 255, 255), 1))
        painter.setBrush(QBrush(QColor(255, 255, 255, 128)))
        for hid in (self._H_TL, self._H_TR, self._H_BL, self._H_BR):
            pt = self._handle_points()[hid]
            painter.drawEllipse(pt, hs / 2, hs / 2)

        # Edge handles -- different visual for crop vs resize mode
        if crop:
            # Orange triangles pointing inward for crop
            painter.setPen(QPen(QColor(255, 120, 50), 1))
            painter.setBrush(QBrush(QColor(255, 120, 50, 180)))
        else:
            # Blue squares for resize
            painter.setPen(QPen(QColor(200, 200, 255), 1))
            painter.setBrush(QBrush(QColor(200, 200, 255, 160)))
        for hid in (self._H_T, self._H_B, self._H_L, self._H_R):
            pt = self._handle_points()[hid]
            painter.drawRect(QRectF(pt.x() - hs / 2, pt.y() - hs / 2, hs, hs))

    # -- Utilities --

    def update_pixmap(self, pixmap):
        """Replace displayed image (e.g. folio nav or threshold change).

        Re-applies any active crop offsets to the new image so crop
        state is preserved across folio flips and threshold changes.
        """
        self.prepareGeometryChange()
        offsets = getattr(self, '_crop_offsets', None)
        if offsets and any(o > 0 for o in offsets):
            # Re-apply crop to the new full image
            self._original_pixmap = pixmap.copy()
            t, b, l, r = offsets
            w, h = pixmap.width(), pixmap.height()
            # Scale offsets proportionally if new image is different size
            if self._original_pixmap is not None:
                cropped = pixmap.copy(
                    min(l, w // 3), min(t, h // 3),
                    max(50, w - min(l, w // 3) - min(r, w // 3)),
                    max(50, h - min(t, h // 3) - min(b, h // 3))
                )
                self.setPixmap(cropped)
            else:
                self.setPixmap(pixmap)
        else:
            self.setPixmap(pixmap)
        self.setTransformOriginPoint(self._pixmap_rect().center())

    def shape(self):
        path = QPainterPath()
        path.addRect(self._pixmap_rect())
        return path


class PuzzleCanvasView(QGraphicsView):
    """A QGraphicsView hosting PuzzleFragmentItem instances.

    Features: Ctrl+wheel zoom, hand-drag pan, dark gray / checkerboard background.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.scene = QGraphicsScene(self)
        self.setScene(self.scene)
        self.scene.setSceneRect(QRectF(-10000, -10000, 20000, 20000))

        # Render quality
        self.setRenderHint(QPainter.RenderHint.Antialiasing)
        self.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)

        # We handle pan manually (not ScrollHandDrag) so items can be dragged
        self.setDragMode(QGraphicsView.DragMode.NoDrag)
        self.setTransformationAnchor(QGraphicsView.ViewportAnchor.AnchorUnderMouse)

        # Hide scrollbars
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        # State: background mode cycles through options
        # 0=dark gray, 1=black, 2=white, 3=checkerboard, 4=light table (warm), 5=grid
        self._bg_mode = 0
        self._BG_MODES = [
            ("dark_gray", QColor(0x33, 0x33, 0x33)),
            ("black", QColor(0x00, 0x00, 0x00)),
            ("white", QColor(0xFF, 0xFF, 0xFF)),
            ("checkerboard", None),
            ("light_table", QColor(0xF5, 0xF0, 0xE0)),  # warm cream (simulates light table)
            ("grid", QColor(0x28, 0x28, 0x28)),
        ]
        self._panning = False
        self._pan_start = QPointF()

        self.setStyleSheet("border: none;")

    # -- Background --

    def cycle_background(self):
        """Cycle to the next background mode."""
        self._bg_mode = (self._bg_mode + 1) % len(self._BG_MODES)
        self.scene.invalidate()
        return self._BG_MODES[self._bg_mode][0]

    def set_checkerboard(self, enabled):
        """Legacy toggle -- switches between dark gray and checkerboard."""
        self._bg_mode = 3 if enabled else 0
        self.scene.invalidate()

    def drawBackground(self, painter, rect):
        mode_name, color = self._BG_MODES[self._bg_mode]

        if mode_name == "checkerboard":
            tile = 20
            light = QColor(0xC8, 0xC8, 0xC8)
            dark = QColor(0x96, 0x96, 0x96)
            left = int(rect.left()) - (int(rect.left()) % tile)
            top = int(rect.top()) - (int(rect.top()) % tile)
            x = left
            while x < rect.right():
                y = top
                while y < rect.bottom():
                    c = light if ((x // tile) + (y // tile)) % 2 == 0 else dark
                    painter.fillRect(QRectF(x, y, tile, tile), c)
                    y += tile
                x += tile
        elif mode_name == "grid":
            painter.fillRect(rect, color)
            painter.setPen(QPen(QColor(0x40, 0x40, 0x40), 1))
            step = 50
            left = int(rect.left()) - (int(rect.left()) % step)
            top = int(rect.top()) - (int(rect.top()) % step)
            x = left
            while x < rect.right():
                painter.drawLine(QPointF(x, rect.top()), QPointF(x, rect.bottom()))
                x += step
            y = top
            while y < rect.bottom():
                painter.drawLine(QPointF(rect.left(), y), QPointF(rect.right(), y))
                y += step
        else:
            painter.fillRect(rect, color)

    # -- Pan --

    def mousePressEvent(self, event):
        # Middle-button or left-click on empty canvas => pan
        is_middle = event.button() == Qt.MouseButton.MiddleButton
        is_left_no_item = (
            event.button() == Qt.MouseButton.LeftButton
            and not self.itemAt(event.position().toPoint())
        )
        if is_middle or is_left_no_item:
            self._panning = True
            self._pan_start = event.position()
            self.setCursor(QCursor(Qt.CursorShape.ClosedHandCursor))
            event.accept()
            return
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if self._panning:
            delta = event.position() - self._pan_start
            self._pan_start = event.position()
            self.horizontalScrollBar().setValue(
                self.horizontalScrollBar().value() - int(delta.x())
            )
            self.verticalScrollBar().setValue(
                self.verticalScrollBar().value() - int(delta.y())
            )
            event.accept()
            return
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        if self._panning:
            self._panning = False
            self.setCursor(QCursor(Qt.CursorShape.ArrowCursor))
            event.accept()
            return
        super().mouseReleaseEvent(event)

    # -- Zoom / item resize --

    def wheelEvent(self, event):
        delta = event.angleDelta().y()
        if event.modifiers() & Qt.KeyboardModifier.ControlModifier:
            # Ctrl+wheel: zoom view
            factor = 1.15 if delta > 0 else 1.0 / 1.15
            current_scale = self.transform().m11()
            new_scale = current_scale * factor
            if 0.05 <= new_scale <= 10.0:
                self.scale(factor, factor)
            event.accept()
            return

        # No Ctrl: resize item under mouse
        item = self.itemAt(event.position().toPoint())
        if isinstance(item, PuzzleFragmentItem):
            item.adjust_scale_from_wheel(delta)
            event.accept()
            return

        event.ignore()

    # -- Accessors --

    def get_fragment_items(self):
        """Return all PuzzleFragmentItem instances on the scene."""
        return [i for i in self.scene.items() if isinstance(i, PuzzleFragmentItem)]

    def get_selected_fragments(self):
        """Return selected PuzzleFragmentItem instances."""
        return [i for i in self.scene.selectedItems() if isinstance(i, PuzzleFragmentItem)]


class PuzzleExportThread(QThread):
    """Compose and save a puzzle PNG without blocking the desktop UI."""

    progress_signal = pyqtSignal(int, int, str)  # current, total, label
    finished_signal = pyqtSignal(str)            # saved path
    cancelled_signal = pyqtSignal()
    error_signal = pyqtSignal(str)

    def __init__(self, fragments, path: str, export_size: int = 2000, margin: int = 20, parent=None):
        super().__init__(parent)
        self.fragments = list(fragments)
        self.path = path
        self.export_size = export_size
        self.margin = margin

    def run(self):
        try:
            from shared.puzzle_export import compose_puzzle_export, add_metadata_banner
            from shared.puzzle_image_service import get_puzzle_image_service

            img_svc = get_puzzle_image_service()
            result = compose_puzzle_export(
                self.fragments,
                img_svc,
                export_size=self.export_size,
                margin=self.margin,
                progress_callback=lambda current, total, message: self.progress_signal.emit(current, total, message),
                check_cancel=self.isInterruptionRequested,
            )
            if self.isInterruptionRequested():
                raise InterruptedError("Puzzle export cancelled")
            if result is None:
                self.error_signal.emit("Export failed")
                return
            # Add metadata banner
            result = add_metadata_banner(result, self.fragments, app_variant='desktop')
            total = max(1, len(self.fragments))
            self.progress_signal.emit(total, total, "Saving PNG")
            if self.isInterruptionRequested():
                raise InterruptedError("Puzzle export cancelled")
            result.save(self.path, 'PNG')
            if self.isInterruptionRequested():
                raise InterruptedError("Puzzle export cancelled")
            self.finished_signal.emit(self.path)
        except InterruptedError:
            self.cancelled_signal.emit()
        except Exception as e:
            self.error_signal.emit(str(e))


class PuzzlePublishThread(QThread):
    """Worker thread for publish/unpublish operations."""
    finished = pyqtSignal(bool, str)  # success, message

    def __init__(self, client, doc=None, join_id=None, unpublish=False, parent=None):
        super().__init__(parent)
        self.client = client
        self.doc = doc
        self.join_id = join_id
        self.unpublish = unpublish

    def run(self):
        try:
            if self.unpublish:
                success, msg = self.client.unpublish_puzzle_join(self.join_id)
            else:
                success, msg = self.client.publish_puzzle_join(self.doc)
            self.finished.emit(success, msg)
        except Exception as e:
            self.finished.emit(False, str(e))


class PuzzleCanvasWindow(QMainWindow):
    """Standalone puzzle workspace for assembling fragment images.

    Provides toolbar controls (shelfmark autocomplete, flip, threshold, folio
    navigation, scale, delete, background toggle) around a PuzzleCanvasView.
    Singleton pattern: GenizahGUI.add_to_puzzle() reuses this window.
    """

    def __init__(self, app):
        super().__init__(app)
        self.app = app
        self.setWindowTitle(tr("Fragment Puzzle"))
        self.setMinimumSize(900, 600)
        if hasattr(app, 'windowIcon'):
            self.setWindowIcon(app.windowIcon())

        # Thread and item tracking
        self._loader_threads = []
        self._meta_threads = []
        self._fragment_items = {}       # (sys_id, folio_label) -> PuzzleFragmentItem
        self._pending_fragments = {}    # (sys_id, folio_label) -> PuzzleFragment
        self._folio_lists = {}          # sys_id -> list of {'fl_id': str, 'label': str, ...}
        self._placeholder_items = {}    # (sys_id, folio_label) -> QGraphicsTextItem
        self._next_x = 50.0

        # Join document state
        self._current_doc_id = None        # None = scratch pad, str = saved document
        self._has_unsaved_changes = False   # For scratch pad save prompt
        self._loading_document = False     # True while async image loads are in progress (guards auto-save)
        self._load_pending_count = 0       # Number of image loads still in flight during document load
        self._auto_save_timer = QTimer()
        self._auto_save_timer.setSingleShot(True)
        self._auto_save_timer.setInterval(1500)  # 1.5s debounce
        self._auto_save_timer.timeout.connect(self._auto_save)
        self._export_thread = None
        self._export_progress = None

        # --- Central widget layout ---
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(4, 4, 4, 4)
        main_layout.setSpacing(2)

        # --- Row 1: Shelfmark input + fragment list + selection info ---
        row1 = QHBoxLayout()
        row1.setSpacing(4)

        # Shelfmark input with autocomplete
        self.shelfmark_input = QLineEdit()
        self.shelfmark_input.setPlaceholderText(tr("Enter shelfmark..."))
        self.shelfmark_input.setMinimumWidth(180)
        self.shelfmark_input.setMaximumWidth(280)
        if hasattr(self.app, 'shelf_model') and self.app.shelf_model:
            from desktop.widgets import ShelfmarkCompleter
            completer = ShelfmarkCompleter(
                self.app.shelf_model, self,
                valid_keys=getattr(self.app, 'valid_shelf_keys', set())
            )
            completer.setCompletionRole(Qt.ItemDataRole.UserRole)
            completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
            completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
            completer.setFilterMode(Qt.MatchFlag.MatchStartsWith)
            self.shelfmark_input.setCompleter(completer)
        self.shelfmark_input.returnPressed.connect(self._on_add_shelfmark)
        row1.addWidget(self.shelfmark_input)

        self.btn_add = QPushButton("+")
        self.btn_add.setFixedWidth(28)
        self.btn_add.setToolTip(tr("Add fragment"))
        self.btn_add.clicked.connect(self._on_add_shelfmark)
        row1.addWidget(self.btn_add)

        # Add from personal list
        btn_from_list = QPushButton("\U0001f4cb")
        btn_from_list.setFixedWidth(30)
        btn_from_list.setToolTip(tr("Add from personal list"))
        btn_from_list.clicked.connect(self._show_add_from_list)
        row1.addWidget(btn_from_list)

        # Add from joins
        btn_from_joins = QPushButton("\U0001f517")
        btn_from_joins.setFixedWidth(30)
        btn_from_joins.setToolTip(tr("Add connected fragments (joins)"))
        btn_from_joins.clicked.connect(self._show_add_from_joins)
        row1.addWidget(btn_from_joins)

        row1.addWidget(QLabel("|"))

        # Fragment dropdown -- shows shelfmark for each fragment on canvas
        self.combo_fragments = QComboBox()
        self.combo_fragments.setMinimumWidth(200)
        self.combo_fragments.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)
        self.combo_fragments.setPlaceholderText(tr("Fragments on canvas"))
        self.combo_fragments.currentIndexChanged.connect(self._on_fragment_combo_changed)
        row1.addWidget(self.combo_fragments)

        # Browse button -- opens the selected fragment in the browse tab
        btn_browse_frag = QPushButton("\U0001f4d6")
        btn_browse_frag.setFixedWidth(28)
        btn_browse_frag.setToolTip(tr("Browse this fragment"))
        btn_browse_frag.clicked.connect(self._browse_selected_fragment)
        row1.addWidget(btn_browse_frag)

        row1.addWidget(QLabel("|"))

        btn_save = QPushButton("\U0001f4be")
        btn_save.setFixedWidth(28)
        btn_save.setToolTip(tr("Save Puzzle"))
        btn_save.clicked.connect(self._on_save_join)
        row1.addWidget(btn_save)

        btn_new = QPushButton("\U0001f4c4")
        btn_new.setFixedWidth(28)
        btn_new.setToolTip(tr("New Puzzle"))
        btn_new.clicked.connect(self._on_new_puzzle)
        row1.addWidget(btn_new)

        btn_export = QPushButton("\U0001f5bc")
        btn_export.setFixedWidth(28)
        btn_export.setToolTip(tr("Export PNG"))
        btn_export.clicked.connect(self._on_export_png)
        row1.addWidget(btn_export)

        self.btn_publish = QPushButton("\U0001f310")
        self.btn_publish.setFixedWidth(28)
        self.btn_publish.setToolTip(tr("Publish to Community"))
        self.btn_publish.clicked.connect(self._on_publish)
        row1.addWidget(self.btn_publish)
        self._is_published = False
        self._publish_thread = None

        row1.addStretch()

        main_layout.addLayout(row1)

        # --- Row 2: Transform + Flip + Folio ---
        row2 = QHBoxLayout()
        row2.setSpacing(4)

        btn_rotate_ccw = QPushButton("\u21ba")
        btn_rotate_ccw.setToolTip(tr("Rotate left 1\u00b0"))
        btn_rotate_ccw.setFixedWidth(30)
        btn_rotate_ccw.clicked.connect(lambda: self._rotate_selected(-1))
        row2.addWidget(btn_rotate_ccw)

        btn_rotate_cw = QPushButton("\u21bb")
        btn_rotate_cw.setToolTip(tr("Rotate right 1\u00b0"))
        btn_rotate_cw.setFixedWidth(30)
        btn_rotate_cw.clicked.connect(lambda: self._rotate_selected(1))
        row2.addWidget(btn_rotate_cw)

        row2.addWidget(QLabel("|"))

        self.btn_flip_rv = QPushButton("\u2194")
        self.btn_flip_rv.setFixedWidth(28)
        self.btn_flip_rv.setToolTip(tr("Flip selected fragment (recto/verso)"))
        self.btn_flip_rv.clicked.connect(self._flip_recto_verso)
        row2.addWidget(self.btn_flip_rv)

        self.btn_flip_puzzle = QPushButton("\u21c4")
        self.btn_flip_puzzle.setFixedWidth(28)
        self.btn_flip_puzzle.setToolTip(tr("Flip all fragments \u2014 show other side of joined page"))
        self.btn_flip_puzzle.clicked.connect(self._flip_entire_puzzle)
        row2.addWidget(self.btn_flip_puzzle)

        row2.addWidget(QLabel("|"))

        self.btn_folio_prev = QPushButton("<")
        self.btn_folio_prev.setMaximumWidth(30)
        self.btn_folio_prev.setToolTip(tr("Previous page"))
        self.btn_folio_prev.clicked.connect(lambda: self._navigate_folio(-1))
        row2.addWidget(self.btn_folio_prev)

        self.btn_folio_next = QPushButton(">")
        self.btn_folio_next.setMaximumWidth(30)
        self.btn_folio_next.setToolTip(tr("Next page"))
        self.btn_folio_next.clicked.connect(lambda: self._navigate_folio(1))
        row2.addWidget(self.btn_folio_next)

        row2.addWidget(QLabel("|"))

        # Crop mode
        self.btn_crop = QPushButton("\u2702")
        self.btn_crop.setFixedWidth(28)
        self.btn_crop.setToolTip(tr("Crop \u2014 drag edges to trim"))
        self.btn_crop.setCheckable(True)
        self.btn_crop.toggled.connect(self._toggle_crop_mode)
        row2.addWidget(self.btn_crop)

        row2.addWidget(QLabel("|"))

        self.btn_delete = QPushButton("\U0001f5d1")
        self.btn_delete.setFixedWidth(28)
        self.btn_delete.setToolTip(tr("Delete selected fragment"))
        self.btn_delete.clicked.connect(self._delete_selected)
        row2.addWidget(self.btn_delete)

        row2.addWidget(QLabel("|"))

        btn_bring_fwd = QPushButton("\u2b06")
        btn_bring_fwd.setFixedWidth(28)
        btn_bring_fwd.setToolTip(tr("Bring Forward"))
        btn_bring_fwd.clicked.connect(lambda: self._change_z_order(1))
        row2.addWidget(btn_bring_fwd)

        btn_send_bwd = QPushButton("\u2b07")
        btn_send_bwd.setFixedWidth(28)
        btn_send_bwd.setToolTip(tr("Send Backward"))
        btn_send_bwd.clicked.connect(lambda: self._change_z_order(-1))
        row2.addWidget(btn_send_bwd)

        row2.addStretch()

        self.btn_bg_toggle = QPushButton("\U0001f3a8")
        self.btn_bg_toggle.setFixedWidth(28)
        self.btn_bg_toggle.setToolTip(tr("Cycle background: dark gray / black / white / checkerboard / light table / grid"))
        self.btn_bg_toggle.clicked.connect(self._cycle_bg)
        row2.addWidget(self.btn_bg_toggle)

        main_layout.addLayout(row2)

        # --- Row 3: Sliders ---
        row3 = QHBoxLayout()
        row3.setSpacing(4)

        row3.addWidget(QLabel(tr("Threshold:")))
        btn_thr_minus = QPushButton("-")
        btn_thr_minus.setFixedWidth(24)
        btn_thr_minus.clicked.connect(lambda: self._nudge_threshold(-1))
        row3.addWidget(btn_thr_minus)
        self.slider_threshold = QSlider(Qt.Orientation.Horizontal)
        self.slider_threshold.setRange(0, 150)
        self.slider_threshold.setValue(30)
        self.slider_threshold.setMinimumWidth(140)
        self.lbl_threshold_val = QLabel("30")
        self.lbl_threshold_val.setMinimumWidth(28)
        self.slider_threshold.valueChanged.connect(
            lambda v: self.lbl_threshold_val.setText(tr("OFF") if v == 0 else str(v))
        )
        self.slider_threshold.sliderReleased.connect(self._on_threshold_changed)
        row3.addWidget(self.slider_threshold)
        row3.addWidget(self.lbl_threshold_val)
        btn_thr_plus = QPushButton("+")
        btn_thr_plus.setFixedWidth(24)
        btn_thr_plus.clicked.connect(lambda: self._nudge_threshold(1))
        row3.addWidget(btn_thr_plus)

        row3.addWidget(QLabel("  "))

        row3.addWidget(QLabel(tr("Scale:")))
        btn_scale_minus = QPushButton("-")
        btn_scale_minus.setFixedWidth(24)
        btn_scale_minus.clicked.connect(lambda: self._nudge_scale(-5))
        row3.addWidget(btn_scale_minus)
        self.slider_scale = QSlider(Qt.Orientation.Horizontal)
        self.slider_scale.setRange(10, 400)
        self.slider_scale.setValue(100)
        self.slider_scale.setMinimumWidth(140)
        self.lbl_scale_val = QLabel("100%")
        self.lbl_scale_val.setMinimumWidth(35)
        self.slider_scale.valueChanged.connect(self._on_scale_changed)
        row3.addWidget(self.slider_scale)
        row3.addWidget(self.lbl_scale_val)
        btn_scale_plus = QPushButton("+")
        btn_scale_plus.setFixedWidth(24)
        btn_scale_plus.clicked.connect(lambda: self._nudge_scale(5))
        row3.addWidget(btn_scale_plus)

        row3.addStretch()

        main_layout.addLayout(row3)

        # --- Canvas view ---
        self.canvas_view = PuzzleCanvasView(self)
        # bg toggle is now cycle-click, not toggled
        main_layout.addWidget(self.canvas_view, 1)

        # Selection tracking
        self.canvas_view.scene.selectionChanged.connect(self._on_selection_changed)

        # Context menu on canvas view
        self.canvas_view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.canvas_view.customContextMenuRequested.connect(self._on_canvas_context_menu)

        # Status bar for messages
        self.statusBar().showMessage(tr("Ready"))

        # --- Document side panel ---
        self._docs_dock = QDockWidget(tr("Saved Joins"), self)
        self._docs_dock.setAllowedAreas(Qt.DockWidgetArea.LeftDockWidgetArea | Qt.DockWidgetArea.RightDockWidgetArea)
        self._docs_dock.setFeatures(QDockWidget.DockWidgetFeature.DockWidgetMovable)

        dock_widget = QWidget()
        dock_layout = QVBoxLayout(dock_widget)
        dock_layout.setContentsMargins(4, 4, 4, 4)
        dock_layout.setSpacing(4)

        # Document list
        self._docs_list = QListWidget()
        self._docs_list.setIconSize(QSize(80, 80))
        self._docs_list.itemClicked.connect(self._on_doc_list_clicked)
        self._docs_list.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self._docs_list.customContextMenuRequested.connect(self._on_doc_context_menu)
        dock_layout.addWidget(self._docs_list, stretch=3)

        # Details section (shown when a saved doc is active)
        self._details_group = QGroupBox(tr("Details"))
        details_layout = QVBoxLayout(self._details_group)
        details_layout.setSpacing(4)

        self._title_edit = QLineEdit()
        self._title_edit.setPlaceholderText(tr("Join title"))
        self._title_edit.editingFinished.connect(self._on_title_changed)
        details_layout.addWidget(QLabel(tr("Title:")))
        details_layout.addWidget(self._title_edit)

        self._notes_edit = QTextEdit()
        self._notes_edit.setPlaceholderText(tr("Notes and observations..."))
        self._notes_edit.setMaximumHeight(100)
        self._notes_edit.textChanged.connect(self._on_notes_changed)
        details_layout.addWidget(QLabel(tr("Notes:")))
        details_layout.addWidget(self._notes_edit)

        self._fragments_label = QLabel("")
        self._fragments_label.setWordWrap(True)
        self._fragments_label.setStyleSheet("color: #888; font-size: 11px;")
        details_layout.addWidget(QLabel(tr("Fragments:")))
        details_layout.addWidget(self._fragments_label)

        self._details_group.setVisible(False)
        dock_layout.addWidget(self._details_group, stretch=1)
        self._docs_dock.setWidget(dock_widget)
        self.addDockWidget(Qt.DockWidgetArea.LeftDockWidgetArea, self._docs_dock)
        self._docs_dock.setMinimumWidth(220)
        self._docs_dock.setMaximumWidth(350)

        # Initial refresh
        self._refresh_docs_list()

        # Event-driven auto-save: scene.changed fires after any visual change (drag, rotate, scale)
        self.canvas_view.scene.changed.connect(self._on_scene_changed)
        self._scene_change_debounce = QTimer()
        self._scene_change_debounce.setSingleShot(True)
        self._scene_change_debounce.setInterval(500)  # 500ms debounce to batch rapid changes
        self._scene_change_debounce.timeout.connect(self._schedule_auto_save)

    # -- Public API --

    def add_fragment(self, sys_id, shelfmark, folio_label, fl_id,
                     image_url='', external_provider='', page_index=-1):
        """Add a fragment to the puzzle canvas. Starts async image load."""
        item_key = (sys_id, folio_label)

        # Dedup: already loaded
        if item_key in self._fragment_items:
            existing = self._fragment_items[item_key]
            existing.setSelected(True)
            return

        # Dedup: already loading
        if item_key in self._pending_fragments:
            return

        from shared.puzzle_model import PuzzleFragment
        # Per-library bg removal settings:
        # - CUL + CUDL private collections (Mosseri etc.): high threshold (150) for blue mats
        # - Oxford: skip bg removal -- dark ink on similar-colored parchment
        # - Others: default threshold (30)
        threshold = 30.0
        skip_bg = False
        lib_code = ''
        if hasattr(self.app, 'meta_mgr') and self.app.meta_mgr:
            lib_code = self.app.meta_mgr.get_library_for_id(sys_id) or ''
        if lib_code == 'CUL' or external_provider == 'cambridge':
            threshold = 150.0
        elif lib_code == 'Oxford':
            skip_bg = True
        elif shelfmark:
            s = shelfmark.upper()
            if s.startswith(('T-S', 'OR.', 'ADD.')):
                threshold = 150.0
            elif s.startswith(('MS HEB.', 'MS. HEB.')):
                skip_bg = True
        puzzle_frag = PuzzleFragment(
            sys_id=sys_id,
            folio_label=folio_label,
            fl_id=fl_id,
            shelfmark=shelfmark,
            x=self._next_x,
            y=50.0,
            bg_removal_threshold=threshold,
            processed=not skip_bg,
            image_url=image_url,
            external_provider=external_provider,
            page_index=page_index,
        )
        self._pending_fragments[item_key] = puzzle_frag

        # Placeholder on canvas
        placeholder = QGraphicsTextItem(f"{shelfmark}\n{tr('Loading...')}")
        placeholder.setDefaultTextColor(QColor(200, 200, 200))
        placeholder.setPos(self._next_x, 50.0)
        self.canvas_view.scene.addItem(placeholder)
        self._placeholder_items[item_key] = placeholder

        # Start image loader thread
        thr = puzzle_frag.bg_removal_threshold
        is_cul = (lib_code == 'CUL') or (external_provider == 'cambridge') or (shelfmark and shelfmark.upper().startswith(('T-S', 'OR.', 'ADD.')))
        do_processed = (thr > 0) and not skip_bg
        thread = PuzzleImageLoaderThread(fl_id, threshold=thr, processed=do_processed, is_cul=is_cul,
                                         image_url=image_url)
        thread.image_ready.connect(partial(self._on_image_loaded, item_key))
        thread.load_failed.connect(self._on_image_failed)
        self._loader_threads.append(thread)
        thread.start()

    # -- Shelfmark input --

    def _on_add_shelfmark(self):
        """Handle shelfmark entry: resolve sys_id, then async fl_id resolution."""
        text = self.shelfmark_input.text().strip()
        if not text:
            return

        # Ensure shelf map is populated
        if hasattr(self.app, '_ensure_shelf_map'):
            self.app._ensure_shelf_map()

        norm = normalize_shelfmark(text)
        shelf_to_sys = getattr(self.app, '_shelf_to_sys', None)
        sys_id = shelf_to_sys.get(norm) if shelf_to_sys else None
        if not sys_id:
            self.statusBar().showMessage(tr("Shelfmark not found"), 3000)
            return

        shelfmark, _ = self.app.meta_mgr.get_meta_for_id(sys_id)
        if not shelfmark or shelfmark == "Unknown":
            shelfmark = text

        # Check cached folio list
        if sys_id in self._folio_lists and self._folio_lists[sys_id]:
            images = self._folio_lists[sys_id]
            first = images[0]
            self.add_fragment(sys_id, shelfmark, first.get('label', '1r'), first.get('fl_id', ''))
        else:
            # Async fl_id resolution via PuzzleMetaLoaderThread
            self.statusBar().showMessage(tr("Resolving images..."), 5000)
            thread = PuzzleMetaLoaderThread(self.app.meta_mgr, sys_id, shelfmark)
            thread.meta_ready.connect(self._on_meta_resolved)
            thread.meta_failed.connect(self._on_meta_failed)
            self._meta_threads.append(thread)
            thread.start()

        self.shelfmark_input.clear()

    def _show_add_from_list(self):
        """Show picker to add fragments from a personal list."""
        lists_mgr = getattr(self.app, 'lists_mgr', None)
        if not lists_mgr:
            self.statusBar().showMessage(tr("Lists not available"), 2000)
            return
        all_lists = lists_mgr.get_all_lists(include_recent=False)
        if not all_lists:
            self.statusBar().showMessage(tr("No personal lists found"), 2000)
            return

        dlg = QDialog(self)
        dlg.setWindowTitle(tr("Add from Personal List"))
        dlg.setMinimumSize(400, 500)
        layout = QVBoxLayout(dlg)

        combo = QComboBox()
        for lst in all_lists:
            combo.addItem(lst.get('name', lst.get('id', '?')), lst.get('id'))
        layout.addWidget(combo)

        list_widget = QListWidget()
        list_widget.setSelectionMode(QListWidget.SelectionMode.ExtendedSelection)
        layout.addWidget(list_widget)

        def load_items():
            list_widget.clear()
            list_id = combo.currentData()
            if not list_id:
                return
            items = lists_mgr.get_items_in_list(list_id)
            for it in items:
                sid = it.get('sys_id', '')
                shelf, _ = self.app.meta_mgr.get_meta_for_id(sid) if sid else ('', '')
                display = shelf or sid
                lw_item = QListWidgetItem(display)
                lw_item.setData(Qt.ItemDataRole.UserRole, {'sys_id': sid, 'shelfmark': shelf or sid})
                list_widget.addItem(lw_item)

        combo.currentIndexChanged.connect(lambda: load_items())
        load_items()

        btn_row = QHBoxLayout()
        btn_add = QPushButton(tr("Add Selected"))
        btn_add_all = QPushButton(tr("Add All"))
        btn_close = QPushButton(tr("Close"))
        btn_row.addWidget(btn_add)
        btn_row.addWidget(btn_add_all)
        btn_row.addStretch()
        btn_row.addWidget(btn_close)
        layout.addLayout(btn_row)

        def add_items(items_to_add):
            for lw_item in items_to_add:
                data = lw_item.data(Qt.ItemDataRole.UserRole)
                if data:
                    self.app.add_to_puzzle(data['sys_id'], data['shelfmark'])

        btn_add.clicked.connect(lambda: add_items(list_widget.selectedItems()))
        btn_add_all.clicked.connect(lambda: add_items([list_widget.item(i) for i in range(list_widget.count())]))
        btn_close.clicked.connect(dlg.close)
        dlg.show()

    def _show_add_from_joins(self):
        """Show connected fragments for the selected fragment and add them."""
        selected = self.canvas_view.get_selected_fragments()
        if not selected:
            self.statusBar().showMessage(tr("Select a fragment first to find its joins"), 3000)
            return
        joins_mgr = getattr(self.app, 'joins_mgr', None)
        if not joins_mgr:
            self.statusBar().showMessage(tr("Joins data not available"), 2000)
            return

        pf = selected[0].puzzle_frag
        shelfmark = pf.shelfmark or pf.sys_id

        # Try by document_id first, then by shelfmark
        connected = joins_mgr.get_connected_fragments_by_id(pf.sys_id)
        if not connected or connected.get('total_fragments', 0) <= 1:
            connected = joins_mgr.get_connected_fragments(shelfmark)

        if not connected or connected.get('total_fragments', 0) <= 1:
            self.statusBar().showMessage(tr("No joins found for {}").format(shelfmark), 3000)
            return

        # Build a map of document_id -> shelfmark from the joins data
        frag_map = {}  # doc_id -> shelfmark
        for join in connected.get('joins', []):
            for doc_key, shelf_key in [('document_id_a', 'fragment_a'), ('document_id_b', 'fragment_b')]:
                doc_id = join.get(doc_key, '')
                frag_shelf = join.get(shelf_key, '')
                if doc_id and doc_id not in frag_map:
                    frag_map[doc_id] = frag_shelf or doc_id

        # Also from fragments list (shelfmark-only joins that lack document_id)
        for frag_shelf in connected.get('fragments', []):
            if isinstance(frag_shelf, str) and frag_shelf not in frag_map.values():
                # Try resolving to sys_id
                norm = normalize_shelfmark(frag_shelf)
                shelf_to_sys = getattr(self.app, '_shelf_to_sys', None)
                sid = shelf_to_sys.get(norm) if shelf_to_sys else None
                if sid and sid not in frag_map:
                    frag_map[sid] = frag_shelf

        if not frag_map:
            self.statusBar().showMessage(tr("No joins found for {}").format(shelfmark), 3000)
            return

        dlg = QDialog(self)
        dlg.setWindowTitle(tr("Add Joined Fragments"))
        dlg.setMinimumSize(350, 400)
        layout = QVBoxLayout(dlg)
        layout.addWidget(QLabel(tr("Joins for: {}").format(shelfmark)))

        list_widget = QListWidget()
        list_widget.setSelectionMode(QListWidget.SelectionMode.ExtendedSelection)
        for doc_id, frag_shelf in frag_map.items():
            # Skip the fragment already on canvas
            if doc_id == pf.sys_id:
                continue
            lw_item = QListWidgetItem(frag_shelf or doc_id)
            lw_item.setData(Qt.ItemDataRole.UserRole, (doc_id, frag_shelf))
            list_widget.addItem(lw_item)
        layout.addWidget(list_widget)

        if list_widget.count() == 0:
            layout.addWidget(QLabel(tr("All joined fragments are already on canvas")))

        # Select all by default
        list_widget.selectAll()

        btn_row = QHBoxLayout()
        btn_add = QPushButton(tr("Add Selected"))
        btn_close = QPushButton(tr("Close"))
        btn_row.addWidget(btn_add)
        btn_row.addStretch()
        btn_row.addWidget(btn_close)
        layout.addLayout(btn_row)

        def add_joined():
            for lw_item in list_widget.selectedItems():
                data = lw_item.data(Qt.ItemDataRole.UserRole)
                if not data:
                    continue
                doc_id, frag_shelf = data
                self.app.add_to_puzzle(doc_id, frag_shelf or doc_id)
            dlg.close()

        btn_add.clicked.connect(add_joined)
        btn_close.clicked.connect(dlg.close)
        dlg.show()

    def _on_meta_resolved(self, sys_id, shelfmark, images_nli):
        """Callback from PuzzleMetaLoaderThread -- cache folio list and add first folio."""
        if sip.isdeleted(self):
            return
        self._folio_lists[sys_id] = images_nli
        first = images_nli[0]
        self.add_fragment(sys_id, shelfmark, first.get('label', '1r'), first.get('fl_id', ''),
                          image_url=first.get('image_url', ''),
                          external_provider=first.get('external_provider', ''),
                          page_index=first.get('page_index', -1))
        self.statusBar().showMessage(
            tr("Added {} ({} folios)").format(shelfmark, len(images_nli)), 3000
        )

    def _on_meta_failed(self, sys_id, error):
        """Callback from PuzzleMetaLoaderThread -- show error."""
        if sip.isdeleted(self):
            return
        self.statusBar().showMessage(
            tr("Failed to resolve images: {}").format(error), 5000
        )

    # -- Image loading callbacks --

    def _on_image_loaded(self, item_key, fl_id, image_bytes):
        """Called when PuzzleImageLoaderThread finishes -- create or update item."""
        if sip.isdeleted(self):
            return

        # Remove placeholder
        placeholder = self._placeholder_items.pop(item_key, None)
        if placeholder and placeholder.scene():
            self.canvas_view.scene.removeItem(placeholder)

        # Check for folio/threshold update path (item already exists)
        if item_key in self._fragment_items:
            existing = self._fragment_items[item_key]
            img = QImage()
            img.loadFromData(image_bytes)
            pixmap = QPixmap.fromImage(img)
            existing.update_pixmap(pixmap)
            # Remove from pending if present
            self._pending_fragments.pop(item_key, None)
            # Decrement loading counter for update path too
            if self._loading_document:
                self._load_pending_count -= 1
                if self._load_pending_count <= 0:
                    self._loading_document = False
                    self._load_pending_count = 0
                    logger.info("Document load complete: all fragments loaded")
            return

        # New fragment path
        puzzle_frag = self._pending_fragments.pop(item_key, None)
        if puzzle_frag is None:
            return  # was deleted while loading

        img = QImage()
        img.loadFromData(image_bytes)
        pixmap = QPixmap.fromImage(img)

        item = PuzzleFragmentItem(puzzle_frag, pixmap)

        # Apply saved crop offsets if any
        ct, cb, cl, cr = puzzle_frag.crop_top, puzzle_frag.crop_bottom, puzzle_frag.crop_left, puzzle_frag.crop_right
        if ct + cb + cl + cr > 0:
            w, h = pixmap.width(), pixmap.height()
            cropped = pixmap.copy(cl, ct, max(w - cl - cr, 1), max(h - ct - cb, 1))
            item._original_pixmap = pixmap.copy()
            item._crop_offsets = [ct, cb, cl, cr]
            item.prepareGeometryChange()
            item.setPixmap(cropped)
            item.setTransformOriginPoint(item._pixmap_rect().center())

        self.canvas_view.scene.addItem(item)
        self._fragment_items[item_key] = item

        # Advance placement position based on actual pixmap width
        self._next_x = puzzle_frag.x + pixmap.width() * puzzle_frag.scale + 50

        # Update fragment dropdown
        self._refresh_fragment_combo()

        # Decrement loading counter and clear guard when all fragments are loaded
        if self._loading_document:
            self._load_pending_count -= 1
            if self._load_pending_count <= 0:
                self._loading_document = False
                self._load_pending_count = 0
                self._update_fragments_label()
                self._fit_all_fragments()
                logger.info("Document load complete: all fragments loaded")
        else:
            # Single fragment add: scroll to show it without shrinking existing fragments
            self.canvas_view.ensureVisible(item.sceneBoundingRect(), 50, 50)

    def _fit_all_fragments(self):
        """Fit view to show all fragments with some padding."""
        items = self.canvas_view.get_fragment_items()
        if not items:
            return
        # Compute bounding rect of all items in scene coords
        rect = QRectF()
        for item in items:
            rect = rect.united(item.sceneBoundingRect())
        # Add padding (10% on each side, minimum 50px)
        pad = max(50, rect.width() * 0.1, rect.height() * 0.1)
        rect.adjust(-pad, -pad, pad, pad)
        self.canvas_view.fitInView(rect, Qt.AspectRatioMode.KeepAspectRatio)

    def _on_image_failed(self, fl_id, error):
        """Called when PuzzleImageLoaderThread fails."""
        if sip.isdeleted(self):
            return
        # Find and remove placeholder for this fl_id
        for key, pf in list(self._pending_fragments.items()):
            if pf.fl_id == fl_id:
                placeholder = self._placeholder_items.pop(key, None)
                if placeholder and placeholder.scene():
                    self.canvas_view.scene.removeItem(placeholder)
                self._pending_fragments.pop(key, None)
                break
        self.statusBar().showMessage(
            tr("Failed to load image: {}").format(error), 5000
        )
        # Decrement loading counter for failed loads
        if self._loading_document:
            self._load_pending_count -= 1
            if self._load_pending_count <= 0:
                self._loading_document = False
                self._load_pending_count = 0

    # -- Selection tracking --

    def _on_selection_changed(self):
        """Update toolbar to reflect current selection."""
        selected = self.canvas_view.get_selected_fragments()
        if len(selected) == 1:
            frag = selected[0]
            pf = frag.puzzle_frag
            label = pf.shelfmark or pf.sys_id
            # Sync combo to selected fragment -- find item_key for this graphics item
            item_key = None
            for k, v in self._fragment_items.items():
                if v is frag:
                    item_key = k
                    break
            if item_key:
                for i in range(self.combo_fragments.count()):
                    if self.combo_fragments.itemData(i) == item_key:
                        self.combo_fragments.blockSignals(True)
                        self.combo_fragments.setCurrentIndex(i)
                        self.combo_fragments.blockSignals(False)
                        break

            # Sync sliders without triggering callbacks
            self.slider_threshold.blockSignals(True)
            self.slider_threshold.setValue(int(pf.bg_removal_threshold))
            self.slider_threshold.blockSignals(False)
            tv = int(pf.bg_removal_threshold)
            self.lbl_threshold_val.setText(tr("OFF") if tv == 0 else str(tv))

            self.slider_scale.blockSignals(True)
            self.slider_scale.setValue(int(pf.scale * 100))
            self.slider_scale.blockSignals(False)
            self.lbl_scale_val.setText(f"{int(pf.scale * 100)}%")
        elif len(selected) > 1:
            pass  # multiple selection -- combo stays as-is
        else:
            self.combo_fragments.blockSignals(True)
            self.combo_fragments.setCurrentIndex(-1)
            self.combo_fragments.blockSignals(False)

    # -- Toolbar actions --

    def _flip_selected_h(self):
        for item in self.canvas_view.get_selected_fragments():
            item.flip_horizontal()

    def _flip_selected_v(self):
        for item in self.canvas_view.get_selected_fragments():
            item.flip_vertical()

    def _cycle_bg(self):
        """Cycle to next background mode and show name in status bar."""
        bg_names = {
            "dark_gray": tr("Dark Gray"),
            "black": tr("Black"),
            "white": tr("White"),
            "checkerboard": tr("Checkerboard"),
            "light_table": tr("Light Table"),
            "grid": tr("Grid"),
        }
        mode = self.canvas_view.cycle_background()
        self.statusBar().showMessage(bg_names.get(mode, mode), 2000)

    def _rotate_selected(self, degrees):
        """Rotate selected fragments by given degrees."""
        for item in self.canvas_view.get_selected_fragments():
            new_rot = (item.rotation() + degrees) % 360
            item.setRotation(new_rot)
            item.puzzle_frag.rotation = new_rot

    @staticmethod
    def _has_blue_mat(pf) -> bool:
        """Check if a PuzzleFragment is likely from a library with blue conservation mat.

        Used as a cache key hint -- actual blue mat removal is also auto-detected
        by remove_background() regardless of this hint.
        """
        if getattr(pf, 'external_provider', '') == 'cambridge':
            return True
        if pf.shelfmark:
            s = pf.shelfmark.upper()
            if s.startswith(('T-S', 'OR.', 'ADD.')):
                return True
        return False

    def _flip_recto_verso(self):
        """Flip selected fragment(s) to show recto/verso -- navigates to next/prev folio."""
        selected = self.canvas_view.get_selected_fragments()
        if not selected:
            self.statusBar().showMessage(tr("No selection"), 2000)
            return
        for item in selected:
            pf = item.puzzle_frag
            folio_list = self._folio_lists.get(pf.sys_id)
            if not folio_list or len(folio_list) < 2:
                self.statusBar().showMessage(tr("No folio list available"), 2000)
                continue
            # Find current index -- match by fl_id (NLI) or by page_index/label (non-NLI)
            current_idx = 0
            for i, entry in enumerate(folio_list):
                if pf.fl_id and entry.get('fl_id') == pf.fl_id:
                    current_idx = i
                    break
                elif not pf.fl_id and pf.page_index >= 0 and entry.get('page_index') == pf.page_index:
                    current_idx = i
                    break
                elif not pf.fl_id and entry.get('label') == pf.folio_label:
                    current_idx = i
                    break
            # Recto (odd index 0,2,4..) -> verso (1,3,5..), verso -> recto
            if current_idx % 2 == 0:
                new_idx = min(current_idx + 1, len(folio_list) - 1)
            else:
                new_idx = max(current_idx - 1, 0)
            if new_idx == current_idx:
                continue
            new_entry = folio_list[new_idx]
            # Re-key and fetch new image
            old_key = (pf.sys_id, pf.folio_label)
            new_label = new_entry.get('label', pf.folio_label)
            new_key = (pf.sys_id, new_label)
            self._fragment_items[new_key] = self._fragment_items.pop(old_key, item)
            pf.fl_id = new_entry.get('fl_id', '')
            pf.image_url = new_entry.get('image_url', '')
            pf.external_provider = new_entry.get('external_provider', pf.external_provider)
            pf.page_index = new_entry.get('page_index', -1)
            pf.folio_label = new_label
            self._pending_fragments[new_key] = pf
            thr = pf.bg_removal_threshold
            thread = PuzzleImageLoaderThread(pf.fl_id, threshold=thr, processed=(thr > 0),
                                             is_cul=self._has_blue_mat(pf),
                                             image_url=pf.image_url)
            thread.image_ready.connect(partial(self._on_image_loaded, new_key))
            thread.load_failed.connect(self._on_image_failed)
            self._loader_threads.append(thread)
            thread.start()
        self._refresh_fragment_combo()

    def _flip_entire_puzzle(self):
        """Flip ALL fragments -- shows the other side of the joined page.

        Physically turning a page over means:
        1. Each fragment shows its recto/verso counterpart (other folio image)
        2. The horizontal layout is mirrored (left<->right)
        NLI verso images are already photographed from the verso side,
        so we do NOT additionally flip each image -- just load the other folio.
        """
        items = self.canvas_view.get_fragment_items()
        if not items:
            return

        # 1. Navigate each fragment to its recto/verso counterpart
        for item in items:
            pf = item.puzzle_frag
            folio_list = self._folio_lists.get(pf.sys_id)
            if not folio_list or len(folio_list) < 2:
                continue
            current_idx = 0
            for i, entry in enumerate(folio_list):
                if pf.fl_id and entry.get('fl_id') == pf.fl_id:
                    current_idx = i
                    break
                elif not pf.fl_id and pf.page_index >= 0 and entry.get('page_index') == pf.page_index:
                    current_idx = i
                    break
                elif not pf.fl_id and entry.get('label') == pf.folio_label:
                    current_idx = i
                    break
            new_idx = current_idx + 1 if current_idx % 2 == 0 else current_idx - 1
            new_idx = max(0, min(new_idx, len(folio_list) - 1))
            if new_idx == current_idx:
                continue
            new_entry = folio_list[new_idx]
            old_key = (pf.sys_id, pf.folio_label)
            new_label = new_entry.get('label', pf.folio_label)
            new_key = (pf.sys_id, new_label)
            self._fragment_items[new_key] = self._fragment_items.pop(old_key, item)
            pf.fl_id = new_entry.get('fl_id', '')
            pf.image_url = new_entry.get('image_url', '')
            pf.external_provider = new_entry.get('external_provider', pf.external_provider)
            pf.page_index = new_entry.get('page_index', -1)
            pf.folio_label = new_label
            self._pending_fragments[new_key] = pf
            thr = pf.bg_removal_threshold
            thread = PuzzleImageLoaderThread(pf.fl_id, threshold=thr, processed=(thr > 0),
                                             is_cul=self._has_blue_mat(pf),
                                             image_url=pf.image_url)
            thread.image_ready.connect(partial(self._on_image_loaded, new_key))
            thread.load_failed.connect(self._on_image_failed)
            self._loader_threads.append(thread)
            thread.start()

        # 2. Mirror layout horizontally: swap positions + negate rotations + toggle flip_h
        #    Use sceneBoundingRect for accurate bounds (accounts for rotation)
        if items:
            scene_rects = [it.sceneBoundingRect() for it in items]
            left = min(r.left() for r in scene_rects)
            right = max(r.right() for r in scene_rects)
            center_x = (left + right) / 2.0

            for item in items:
                # Mirror x-position around center
                sr = item.sceneBoundingRect()
                item_center_x = sr.center().x()
                new_center_x = 2 * center_x - item_center_x
                # Offset from item pos to scene center
                dx = new_center_x - item_center_x
                item.setPos(item.pos().x() + dx, item.pos().y())
                item.puzzle_frag.x = item.pos().x()

                # Negate rotation (clockwise <-> counter-clockwise)
                old_rot = item.rotation()
                new_rot = (360 - old_rot) % 360
                item.setRotation(new_rot)
                item.puzzle_frag.rotation = new_rot

        self._refresh_fragment_combo()

    def _toggle_crop_mode(self, checked):
        """Enter/exit crop mode. In crop mode, drag edges of selected fragment to trim."""
        if checked:
            selected = self.canvas_view.get_selected_fragments()
            if not selected:
                self.statusBar().showMessage(tr("Select a fragment first"), 2000)
                self.btn_crop.setChecked(False)
                return
            item = selected[0]
            # Save original for revert
            if not hasattr(item, '_original_pixmap') or item._original_pixmap is None:
                item._original_pixmap = item.pixmap().copy()
                item._crop_offsets = [0, 0, 0, 0]  # top, bottom, left, right
            item._crop_mode = True
            self.statusBar().showMessage(
                tr("Crop mode: drag edges with arrow keys (\u2191\u2193\u2190\u2192), Enter=OK, Esc=revert"), 0)
        else:
            # Exit crop mode
            for item in self.canvas_view.get_fragment_items():
                if hasattr(item, '_crop_mode'):
                    item._crop_mode = False
            self.statusBar().showMessage(tr("Ready"), 2000)

    def _crop_edge(self, edge, amount=None):
        """Crop a specific edge from selected fragment."""
        if amount is None:
            amount = 20  # default step for arrow key crop
        selected = self.canvas_view.get_selected_fragments()
        if not selected:
            return
        for item in selected:
            pm = item.pixmap()
            if pm.isNull():
                continue
            if not hasattr(item, '_original_pixmap') or item._original_pixmap is None:
                item._original_pixmap = pm.copy()
                item._crop_offsets = [0, 0, 0, 0]
            w, h = pm.width(), pm.height()
            t = min(amount, h // 3) if edge == "top" else 0
            b = min(amount, h // 3) if edge == "bottom" else 0
            l = min(amount, w // 3) if edge == "left" else 0
            r = min(amount, w // 3) if edge == "right" else 0
            if w - l - r < 50 or h - t - b < 50:
                continue
            cropped = pm.copy(l, t, w - l - r, h - t - b)
            item._crop_offsets[0] += t
            item._crop_offsets[1] += b
            item._crop_offsets[2] += l
            item._crop_offsets[3] += r
            item.prepareGeometryChange()
            item.setPixmap(cropped)
            item.setTransformOriginPoint(item._pixmap_rect().center())
            item.setPos(item.pos().x() + l * item.scale(), item.pos().y() + t * item.scale())
            item.puzzle_frag.x = item.pos().x()
            item.puzzle_frag.y = item.pos().y()

    def _revert_crop(self):
        """Revert selected fragments to original uncropped image."""
        selected = self.canvas_view.get_selected_fragments()
        if not selected:
            return
        for item in selected:
            if not hasattr(item, '_original_pixmap') or item._original_pixmap is None:
                continue
            offsets = item._crop_offsets
            item.prepareGeometryChange()
            item.setPixmap(item._original_pixmap)
            item.setTransformOriginPoint(item._pixmap_rect().center())
            item.setPos(item.pos().x() - offsets[2] * item.scale(),
                        item.pos().y() - offsets[0] * item.scale())
            item.puzzle_frag.x = item.pos().x()
            item.puzzle_frag.y = item.pos().y()
            item._original_pixmap = None
            item._crop_offsets = [0, 0, 0, 0]
            if hasattr(item, '_crop_mode'):
                item._crop_mode = False
        self.btn_crop.setChecked(False)
        self.statusBar().showMessage(tr("Crop reverted"), 2000)

    def _nudge_threshold(self, delta):
        """Increment/decrement threshold by delta, then apply."""
        v = max(0, min(150, self.slider_threshold.value() + delta))
        self.slider_threshold.setValue(v)
        self._on_threshold_changed()

    def _on_threshold_changed(self):
        """Re-fetch images with new threshold for selected fragments."""
        value = self.slider_threshold.value()
        processed = value > 0  # 0 = no bg removal
        for item in self.canvas_view.get_selected_fragments():
            pf = item.puzzle_frag
            pf.bg_removal_threshold = float(value)
            item_key = (pf.sys_id, pf.folio_label)
            self._pending_fragments[item_key] = pf
            thread = PuzzleImageLoaderThread(
                pf.fl_id, threshold=float(value), processed=processed,
                is_cul=self._has_blue_mat(pf)
            )
            thread.image_ready.connect(partial(self._on_image_loaded, item_key))
            thread.load_failed.connect(self._on_image_failed)
            self._loader_threads.append(thread)
            thread.start()

    def _nudge_scale(self, delta):
        """Increment/decrement scale by delta percent."""
        v = max(10, min(400, self.slider_scale.value() + delta))
        self.slider_scale.setValue(v)

    def _on_scale_changed(self, value):
        """Update scale for selected fragments proportionally.

        When multiple fragments are selected with different scales (e.g., A=125%, B=74%),
        the slider shows the first item's scale. Moving the slider applies the same
        RATIO to all selected items, preserving their relative sizes.
        """
        self.lbl_scale_val.setText(f"{value}%")
        selected = self.canvas_view.get_selected_fragments()
        if not selected:
            return
        if len(selected) == 1:
            # Single item: set absolute scale
            item = selected[0]
            item.prepareGeometryChange()
            item.setScale(value / 100.0)
            item.puzzle_frag.scale = value / 100.0
        else:
            # Multiple items: apply same ratio change to all
            # Use the reference item (first selected) to compute the ratio
            ref = selected[0]
            old_ref_scale = ref.puzzle_frag.scale
            if old_ref_scale <= 0:
                old_ref_scale = 1.0
            ratio = (value / 100.0) / old_ref_scale
            for item in selected:
                new_scale = max(0.1, min(4.0, item.puzzle_frag.scale * ratio))
                item.prepareGeometryChange()
                item.setScale(new_scale)
                item.puzzle_frag.scale = new_scale

    def _navigate_folio(self, direction):
        """Navigate folio prev/next for selected fragments."""
        for item in self.canvas_view.get_selected_fragments():
            pf = item.puzzle_frag
            folio_list = self._folio_lists.get(pf.sys_id)
            if not folio_list:
                self.statusBar().showMessage(tr("No folio list available"), 3000)
                continue

            # Find current index by fl_id
            current_idx = None
            for i, entry in enumerate(folio_list):
                if entry.get('fl_id') == pf.fl_id:
                    current_idx = i
                    break
            if current_idx is None:
                # Try matching by label
                for i, entry in enumerate(folio_list):
                    if entry.get('label') == pf.folio_label:
                        current_idx = i
                        break
            if current_idx is None:
                current_idx = 0

            new_idx = current_idx + direction
            if new_idx < 0 or new_idx >= len(folio_list):
                self.statusBar().showMessage(tr("No more folios"), 2000)
                continue

            new_entry = folio_list[new_idx]
            new_fl_id = new_entry.get('fl_id', '')
            new_label = new_entry.get('label', pf.folio_label)

            # Re-key the item
            old_key = (pf.sys_id, pf.folio_label)
            new_key = (pf.sys_id, new_label)
            self._fragment_items[new_key] = self._fragment_items.pop(old_key, item)

            # Update fragment data
            pf.fl_id = new_fl_id
            pf.folio_label = new_label

            # Store in pending for update path in _on_image_loaded
            self._pending_fragments[new_key] = pf

            # Fetch new image
            thread = PuzzleImageLoaderThread(new_fl_id, threshold=pf.bg_removal_threshold, is_cul=self._has_blue_mat(pf))
            thread.image_ready.connect(partial(self._on_image_loaded, new_key))
            thread.load_failed.connect(self._on_image_failed)
            self._loader_threads.append(thread)
            thread.start()
        self._refresh_fragment_combo()
        self._schedule_auto_save()

    def _change_z_order(self, direction):
        """Move selected fragment one layer up (+1) or down (-1)."""
        selected = self.canvas_view.get_selected_fragments()
        if not selected:
            return
        for item in selected:
            item.setZValue(item.zValue() + direction)
        self.canvas_view.scene.update()
        self._schedule_auto_save()

    def _delete_selected(self):
        """Remove selected fragments from the canvas."""
        for item in self.canvas_view.get_selected_fragments():
            pf = item.puzzle_frag
            item_key = (pf.sys_id, pf.folio_label)
            self.canvas_view.scene.removeItem(item)
            self._fragment_items.pop(item_key, None)
            self._pending_fragments.pop(item_key, None)
        self._refresh_fragment_combo()
        self._schedule_auto_save()

    # -- Fragment combo --

    def _refresh_fragment_combo(self):
        """Rebuild the fragment dropdown from current items."""
        self.combo_fragments.blockSignals(True)
        self.combo_fragments.clear()
        for item_key, item in self._fragment_items.items():
            pf = item.puzzle_frag
            label = pf.shelfmark or pf.sys_id
            display = f"{label} / {pf.folio_label}"
            self.combo_fragments.addItem(display, item_key)
        self.combo_fragments.blockSignals(False)

    def _browse_selected_fragment(self):
        """Open the selected fragment in the browse tab."""
        index = self.combo_fragments.currentIndex()
        if index < 0:
            return
        item_key = self.combo_fragments.itemData(index)
        if not item_key or item_key not in self._fragment_items:
            return
        pf = self._fragment_items[item_key].puzzle_frag
        shelfmark = pf.shelfmark
        # Walk up to GenizahGUI parent
        parent = self.parent()
        while parent and not hasattr(parent, '_browse_document_by_shelfmark'):
            parent = parent.parent()
        if parent and hasattr(parent, '_browse_document_by_shelfmark'):
            parent._browse_document_by_shelfmark(shelfmark)

    def _on_fragment_combo_changed(self, index):
        """Select the fragment chosen in the dropdown."""
        if index < 0:
            return
        item_key = self.combo_fragments.itemData(index)
        if item_key and item_key in self._fragment_items:
            # Clear current selection, select this one
            self.canvas_view.scene.clearSelection()
            self._fragment_items[item_key].setSelected(True)

    # -- Context menu --

    def _on_canvas_context_menu(self, pos):
        """Show right-click context menu on fragment items."""
        scene_pos = self.canvas_view.mapToScene(pos)
        item = self.canvas_view.scene.itemAt(scene_pos, self.canvas_view.transform())
        if not isinstance(item, PuzzleFragmentItem):
            return

        # Select the item under cursor if not already selected
        if not item.isSelected():
            self.canvas_view.scene.clearSelection()
            item.setSelected(True)

        menu = QMenu(self)

        act_rotate_ccw = QAction(tr("Rotate left 1\u00b0"), self)
        act_rotate_ccw.triggered.connect(lambda: self._rotate_selected(-1))
        menu.addAction(act_rotate_ccw)

        act_rotate_cw = QAction(tr("Rotate right 1\u00b0"), self)
        act_rotate_cw.triggered.connect(lambda: self._rotate_selected(1))
        menu.addAction(act_rotate_cw)

        act_rotate_ccw90 = QAction(tr("Rotate left 90\u00b0"), self)
        act_rotate_ccw90.triggered.connect(lambda: self._rotate_selected(-90))
        menu.addAction(act_rotate_ccw90)

        act_rotate_cw90 = QAction(tr("Rotate right 90\u00b0"), self)
        act_rotate_cw90.triggered.connect(lambda: self._rotate_selected(90))
        menu.addAction(act_rotate_cw90)

        menu.addSeparator()

        act_flip_rv = QAction(tr("Flip") + " (recto/verso)", self)
        act_flip_rv.triggered.connect(self._flip_recto_verso)
        menu.addAction(act_flip_rv)

        act_flip_h = QAction(tr("Flip Horizontal"), self)
        act_flip_h.triggered.connect(self._flip_selected_h)
        menu.addAction(act_flip_h)

        act_flip_v = QAction(tr("Flip Vertical"), self)
        act_flip_v.triggered.connect(self._flip_selected_v)
        menu.addAction(act_flip_v)

        menu.addSeparator()

        act_bring_fwd = QAction(tr("Bring Forward"), self)
        act_bring_fwd.triggered.connect(lambda: self._change_z_order(1))
        menu.addAction(act_bring_fwd)

        act_send_bwd = QAction(tr("Send Backward"), self)
        act_send_bwd.triggered.connect(lambda: self._change_z_order(-1))
        menu.addAction(act_send_bwd)

        menu.addSeparator()

        act_delete = QAction(tr("Delete Fragment"), self)
        act_delete.triggered.connect(self._delete_selected)
        menu.addAction(act_delete)

        menu.exec(self.canvas_view.mapToGlobal(pos))

    # -- Join document management --

    def _refresh_docs_list(self):
        """Refresh the saved documents list in the side panel."""
        self._docs_list.clear()
        from shared.puzzle_service import get_puzzle_service
        svc = get_puzzle_service()
        if not svc.is_available():
            return
        docs = svc.list_documents()
        for doc in docs:
            item = QListWidgetItem()
            title = doc.get('title', '') or 'Untitled'
            updated = doc.get('updated_at', '')[:10]
            shelfmarks = doc.get('shelfmarks_summary', '')
            # Only show shelfmarks if not already covered by title
            sm_parts = [s.strip() for s in shelfmarks.split('+')]
            shelfmarks_in_title = all(p in title for p in sm_parts if p)
            if shelfmarks and not shelfmarks_in_title:
                item.setText(f"{title}\n{shelfmarks}\n{updated}")
            else:
                item.setText(f"{title}\n{updated}")
            item.setData(Qt.ItemDataRole.UserRole, doc['id'])

            thumb_b64 = doc.get('thumbnail_b64', '')
            if thumb_b64:
                try:
                    import base64
                    thumb_bytes = base64.b64decode(thumb_b64)
                    pixmap = QPixmap()
                    pixmap.loadFromData(thumb_bytes)
                    item.setIcon(QIcon(pixmap))
                except Exception:
                    pass  # Thumbnail load failed; full image will replace it
            self._docs_list.addItem(item)

    def _on_doc_list_clicked(self, item):
        """Load a document when clicked in the side panel."""
        doc_id = item.data(Qt.ItemDataRole.UserRole)
        if not doc_id:
            return
        if self._current_doc_id is None and self._has_unsaved_changes and self._fragment_items:
            reply = QMessageBox.question(
                self, tr("Save current work?"),
                tr("Save current puzzle before loading?"),
                QMessageBox.StandardButton.Save | QMessageBox.StandardButton.Discard | QMessageBox.StandardButton.Cancel
            )
            if reply == QMessageBox.StandardButton.Cancel:
                return
            if reply == QMessageBox.StandardButton.Save:
                self._on_save_join()
        self._load_document(doc_id)

    def _load_document(self, doc_id):
        """Load a PuzzleDocument onto the canvas, replacing current content."""
        from shared.puzzle_service import get_puzzle_service
        svc = get_puzzle_service()
        doc = svc.load_document(doc_id)
        if doc is None:
            QMessageBox.warning(self, tr("Error"), tr("Could not load document"))
            return

        # Clear canvas
        self._clear_canvas()

        # Set state
        self._current_doc_id = doc.id
        self._has_unsaved_changes = False

        # Set loading guard to prevent auto-save from overwriting
        # a partially-loaded document
        self._loading_document = True
        self._load_pending_count = len(doc.fragments)

        # Add each fragment via the existing _pending_fragments + _on_image_loaded pipeline
        for frag in doc.fragments:
            item_key = (frag.sys_id, frag.folio_label)
            self._pending_fragments[item_key] = frag

            thread = PuzzleImageLoaderThread(
                frag.fl_id,
                threshold=frag.bg_removal_threshold,
                size=800,
                processed=frag.processed,
                is_cul=self._has_blue_mat(frag),
                image_url=getattr(frag, 'image_url', '')
            )
            thread.image_ready.connect(partial(self._on_image_loaded, item_key))
            thread.load_failed.connect(self._on_image_failed)
            self._loader_threads.append(thread)
            thread.start()

            # Rebuild folio lists for each unique sys_id
            if frag.sys_id not in self._folio_lists:
                self._spawn_meta_loader(frag.sys_id, frag.shelfmark)

        # If doc has zero fragments, clear loading guard immediately
        if not doc.fragments:
            self._loading_document = False
            self._load_pending_count = 0

        # Update details panel
        self._title_edit.setText(doc.title)
        self._notes_edit.setPlainText(doc.notes)
        self._update_fragments_label()
        self._details_group.setVisible(True)
        self.setWindowTitle(f"{tr('Fragment Puzzle')} - {doc.title}")
        self._check_publish_state()

    def _spawn_meta_loader(self, sys_id, shelfmark=''):
        """Spawn a PuzzleMetaLoaderThread to fetch folio lists for a sys_id."""
        thread = PuzzleMetaLoaderThread(self.app.meta_mgr, sys_id, shelfmark)
        thread.meta_ready.connect(self._on_meta_ready_for_load)
        thread.meta_failed.connect(lambda sid, err: logger.warning("Meta load failed for %s: %s", sid, err))
        self._meta_threads.append(thread)
        thread.start()

    def _on_meta_ready_for_load(self, sys_id, shelfmark, images_nli):
        """Handle meta_ready from folio list rebuild during document load."""
        if sip.isdeleted(self):
            return
        self._folio_lists[sys_id] = images_nli

    def _on_save_join(self):
        """Save current puzzle as a join document (new or update)."""
        if not self._fragment_items:
            QMessageBox.information(self, tr("Empty"), tr("Add fragments before saving"))
            return

        from shared.puzzle_model import PuzzleDocument
        from shared.puzzle_export import auto_suggest_title, generate_thumbnail
        from shared.puzzle_image_service import get_puzzle_image_service
        from shared.puzzle_service import get_puzzle_service

        fragments = self._build_fragments_list()

        if self._current_doc_id is None:
            suggested = auto_suggest_title(fragments)
            # Custom save dialog with title + notes
            dlg = QDialog(self)
            dlg.setWindowTitle(tr("Save Puzzle Document"))
            dlg.setMinimumWidth(350)
            layout = QVBoxLayout(dlg)
            layout.addWidget(QLabel(tr("Title:")))
            title_edit = QLineEdit(suggested)
            layout.addWidget(title_edit)
            layout.addWidget(QLabel(tr("Notes:")))
            notes_edit = QTextEdit()
            notes_edit.setMaximumHeight(80)
            notes_edit.setPlaceholderText(tr("Optional notes about this join..."))
            layout.addWidget(notes_edit)
            btn_row = QHBoxLayout()
            btn_row.addStretch()
            btn_save = QPushButton(tr("Save"))
            btn_save.setDefault(True)
            btn_save.clicked.connect(dlg.accept)
            btn_cancel = QPushButton(tr("Cancel"))
            btn_cancel.clicked.connect(dlg.reject)
            btn_row.addWidget(btn_cancel)
            btn_row.addWidget(btn_save)
            layout.addLayout(btn_row)
            if dlg.exec() != QDialog.DialogCode.Accepted:
                return
            title = title_edit.text().strip()
            if not title:
                return
            doc = PuzzleDocument(title=title, notes=notes_edit.toPlainText(), fragments=fragments)
        else:
            svc = get_puzzle_service()
            doc = svc.load_document(self._current_doc_id)
            if doc is None:
                doc = PuzzleDocument(id=self._current_doc_id, fragments=fragments)
            doc.fragments = fragments
            doc.title = self._title_edit.text() or doc.title
            doc.notes = self._notes_edit.toPlainText()
            import datetime
            doc.updated_at = datetime.datetime.now().isoformat()

        # Generate thumbnail
        img_svc = get_puzzle_image_service()
        thumb = generate_thumbnail(fragments, img_svc, thumb_size=150)

        svc = get_puzzle_service()
        doc_id = svc.save_document(doc, thumbnail_b64=thumb)
        if doc_id:
            self._current_doc_id = doc_id
            self._has_unsaved_changes = False
            self._details_group.setVisible(True)
            self._title_edit.setText(doc.title)
            self._notes_edit.setPlainText(doc.notes)
            self._update_fragments_label()
            self._refresh_docs_list()
            self.setWindowTitle(f"{tr('Fragment Puzzle')} - {doc.title}")
            self.statusBar().showMessage(tr("Saved"), 3000)

    def _build_fragments_list(self):
        """Build list of PuzzleFragment from current canvas items."""
        fragments = []
        for key, item in self._fragment_items.items():
            sys_id, folio_label = key
            pf = item.puzzle_frag
            # Sync current position to puzzle_frag
            pf.x = item.pos().x()
            pf.y = item.pos().y()
            pf.rotation = item.rotation()
            pf.scale = item.scale()
            pf.flip_h = item.transform().m11() < 0
            pf.flip_v = item.transform().m22() < 0
            # Sync crop offsets from canvas item to model
            offsets = getattr(item, '_crop_offsets', None)
            if offsets:
                pf.crop_top = offsets[0]
                pf.crop_bottom = offsets[1]
                pf.crop_left = offsets[2]
                pf.crop_right = offsets[3]
            fragments.append(pf)
        return fragments

    def _on_new_puzzle(self):
        """Clear canvas to a fresh scratch pad."""
        if self._current_doc_id is None and self._has_unsaved_changes and self._fragment_items:
            reply = QMessageBox.question(
                self, tr("Save current work?"),
                tr("Save current puzzle before starting new?"),
                QMessageBox.StandardButton.Save | QMessageBox.StandardButton.Discard | QMessageBox.StandardButton.Cancel
            )
            if reply == QMessageBox.StandardButton.Cancel:
                return
            if reply == QMessageBox.StandardButton.Save:
                self._on_save_join()
        self._clear_canvas()
        self._current_doc_id = None
        self._has_unsaved_changes = False
        self._is_published = False
        self.btn_publish.setToolTip(tr("Publish to Community"))
        self.btn_publish.setStyleSheet("")
        self._details_group.setVisible(False)
        self.setWindowTitle(tr("Fragment Puzzle"))

    def _clear_canvas(self):
        """Remove all fragments from canvas."""
        scene = self.canvas_view.scene
        for key in list(self._fragment_items.keys()):
            item = self._fragment_items.pop(key, None)
            if item and item.scene():
                scene.removeItem(item)
        for key in list(self._placeholder_items.keys()):
            item = self._placeholder_items.pop(key, None)
            if item and item.scene():
                scene.removeItem(item)
        self._pending_fragments.clear()
        self._folio_lists.clear()
        self._next_x = 50.0
        self._refresh_fragment_combo()

    def _on_export_png(self):
        """Export composite PNG in a background thread."""
        if not self._fragment_items:
            QMessageBox.information(self, tr("Empty"), tr("Add fragments before exporting"))
            return
        if self._export_thread and self._export_thread.isRunning():
            QMessageBox.information(self, tr("Export"), tr("An export is already in progress"))
            return

        from shared.puzzle_export import auto_suggest_title

        fragments = self._build_fragments_list()
        resolution_items = [
            (tr("Draft (1000 px)"), 1000),
            (tr("Standard (2000 px)"), 2000),
            (tr("Full (3000 px)"), 3000),
        ]
        labels = [label for label, _ in resolution_items]
        selected_label, ok = QInputDialog.getItem(
            self,
            tr("Export PNG"),
            tr("Select resolution:"),
            labels,
            1,
            False,
        )
        if not ok or not selected_label:
            return
        export_size = dict(resolution_items).get(selected_label, 2000)

        suggested_name = auto_suggest_title(fragments).replace(' ', '_').replace('+', '_') + '.png'

        # Default save folder: Documents\GenizahSearchPro\Puzzle Images\
        default_folder = self.app._get_default_save_folder()
        puzzle_folder = os.path.join(os.path.dirname(default_folder), "Puzzle Images")
        try:
            os.makedirs(puzzle_folder, exist_ok=True)
        except Exception:
            puzzle_folder = default_folder  # Custom folder path failed; use default
        default_path = os.path.join(puzzle_folder, suggested_name)

        path, _ = QFileDialog.getSaveFileName(
            self, tr("Export Composite Image"), default_path,
            "PNG Images (*.png)"
        )
        if not path:
            return

        progress = QProgressDialog(tr("Preparing export..."), tr("Cancel"), 0, max(1, len(fragments)), self)
        progress.setWindowTitle(tr("Export PNG"))
        progress.setWindowModality(Qt.WindowModality.WindowModal)
        progress.setAutoClose(False)
        progress.setAutoReset(False)
        progress.setMinimumDuration(0)
        progress.setValue(0)
        progress.canceled.connect(self._cancel_export_thread)
        progress.show()

        thread = PuzzleExportThread(fragments, path, export_size=export_size, margin=20, parent=self)
        thread.progress_signal.connect(self._on_export_progress)
        thread.finished_signal.connect(self._on_export_finished)
        thread.cancelled_signal.connect(self._on_export_cancelled)
        thread.error_signal.connect(self._on_export_error)

        self._export_progress = progress
        self._export_thread = thread
        thread.start()

    def _cancel_export_thread(self):
        """Request cancellation of the active export thread."""
        if self._export_thread and self._export_thread.isRunning():
            self._export_thread.requestInterruption()
        if self._export_progress:
            self._export_progress.setLabelText(tr("Cancelling export..."))

    def _on_export_progress(self, current, total, label):
        """Update the desktop export progress dialog."""
        if not self._export_progress:
            return
        self._export_progress.setMaximum(max(1, total))
        self._export_progress.setValue(max(0, min(current, max(1, total))))
        if label:
            self._export_progress.setLabelText(label)

    def _clear_export_ui(self):
        """Close and release the active export UI objects."""
        progress = self._export_progress
        thread = self._export_thread
        self._export_progress = None
        self._export_thread = None
        if progress:
            progress.close()
            progress.deleteLater()
        if thread:
            thread.deleteLater()

    def _on_export_finished(self, path):
        """Handle successful export completion."""
        self._clear_export_ui()
        self.statusBar().showMessage(tr("Exported to") + f" {path}", 5000)

    def _on_export_cancelled(self):
        """Handle user-cancelled export."""
        self._clear_export_ui()
        self.statusBar().showMessage(tr("Export cancelled"), 3000)

    def _on_export_error(self, error):
        """Handle export failure."""
        self._clear_export_ui()
        QMessageBox.warning(self, tr("Error"), error or tr("Export failed"))

    # -- Community Publish --

    def _on_publish(self):
        """Toggle publish/unpublish for current puzzle join."""
        if not hasattr(self.app, 'corrections_client') or not self.app.corrections_client:
            QMessageBox.warning(self, tr("Login Required"), tr("Please log in to publish"))
            return
        if not self.app.corrections_client.current_user:
            QMessageBox.warning(self, tr("Login Required"), tr("Please log in to publish"))
            return
        if not self._current_doc_id:
            QMessageBox.warning(self, tr("Save First"), tr("Save the puzzle before publishing"))
            return

        if self._is_published:
            # UNPUBLISH flow
            reply = QMessageBox.question(
                self, tr("Unpublish"),
                tr("Remove this join from community view?"),
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply != QMessageBox.StandardButton.Yes:
                return
            self._run_publish_worker(unpublish=True)
        else:
            # PUBLISH flow
            if not self._fragment_items:
                QMessageBox.warning(self, tr("No Fragments"), tr("Add fragments before publishing"))
                return
            reply = QMessageBox.question(
                self, tr("Publish to Community"),
                tr("This will make your puzzle join visible to all users.\n\nPublish now?"),
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply != QMessageBox.StandardButton.Yes:
                return
            self._run_publish_worker(unpublish=False)

    def _run_publish_worker(self, unpublish=False):
        """Run publish/unpublish on a worker thread to avoid freezing UI."""
        self._publish_progress = QProgressDialog(
            tr("Unpublishing...") if unpublish else tr("Publishing..."),
            None,  # No cancel button
            0, 0,  # Indeterminate
            self
        )
        self._publish_progress.setWindowModality(Qt.WindowModality.WindowModal)
        self._publish_progress.show()

        if unpublish:
            self._publish_thread = PuzzlePublishThread(
                self.app.corrections_client,
                join_id=self._current_doc_id,
                unpublish=True,
                parent=self
            )
        else:
            # Save current state first, then publish
            from shared.puzzle_service import get_puzzle_service
            svc = get_puzzle_service()
            doc = svc.load_document(self._current_doc_id)
            if not doc:
                self._publish_progress.close()
                QMessageBox.warning(self, tr("Error"), tr("Could not load document"))
                return
            doc.fragments = self._build_fragments_list()
            if hasattr(self, '_title_edit'):
                doc.title = self._title_edit.text() or doc.title
            if hasattr(self, '_notes_edit'):
                doc.notes = self._notes_edit.toPlainText() or ''

            self._publish_thread = PuzzlePublishThread(
                self.app.corrections_client,
                doc=doc,
                unpublish=False,
                parent=self
            )

        self._publish_thread.finished.connect(self._on_publish_finished)
        self._publish_thread.start()

    def _on_publish_finished(self, success: bool, message: str):
        """Handle publish/unpublish completion on main thread."""
        if hasattr(self, '_publish_progress') and self._publish_progress:
            self._publish_progress.close()
        if success:
            if self._is_published:
                # Was published, now unpublished
                self._is_published = False
                self.btn_publish.setToolTip(tr("Publish to Community"))
                self.btn_publish.setStyleSheet("")
                QMessageBox.information(self, tr("Unpublished"), tr("Your puzzle join is no longer visible to the community"))
            else:
                # Was unpublished, now published
                self._is_published = True
                self.btn_publish.setToolTip(tr("Published -- click to unpublish"))
                self.btn_publish.setStyleSheet("background-color: #4caf50; color: white; border-radius: 4px;")
                share_url = f"https://genizahsearch.com/puzzle?doc={self._current_doc_id}"
                QApplication.clipboard().setText(share_url)
                QMessageBox.information(
                    self, tr("Published"),
                    f"{tr('Your puzzle join is now visible to the community')}\n\n"
                    f"{share_url}\n\n"
                    f"{tr('Link copied to clipboard')}"
                )
        else:
            QMessageBox.warning(self, tr("Error"), message)

    def _check_publish_state(self):
        """Check if current doc is published and update button state."""
        if not self._current_doc_id or not hasattr(self.app, 'corrections_client') or not self.app.corrections_client:
            self._is_published = False
            return
        try:
            self._is_published = self.app.corrections_client.check_is_published(self._current_doc_id)
            if self._is_published:
                self.btn_publish.setToolTip(tr("Published -- click to unpublish"))
                self.btn_publish.setStyleSheet("background-color: #4caf50; color: white; border-radius: 4px;")
            else:
                self.btn_publish.setToolTip(tr("Publish to Community"))
                self.btn_publish.setStyleSheet("")
        except Exception:
            self._is_published = False  # Publish status check failed; assume unpublished

    def _on_doc_context_menu(self, pos):
        """Show context menu on right-click in document list."""
        item = self._docs_list.itemAt(pos)
        if not item:
            return
        doc_id = item.data(Qt.ItemDataRole.UserRole)
        menu = QMenu(self)
        delete_action = menu.addAction(tr("Delete"))
        rename_action = menu.addAction(tr("Rename"))
        action = menu.exec(self._docs_list.mapToGlobal(pos))
        if action == delete_action:
            self._delete_document(doc_id)
        elif action == rename_action:
            self._rename_document(doc_id)

    def _delete_document(self, doc_id):
        """Delete a saved join document with confirmation."""
        reply = QMessageBox.question(
            self, tr("Delete join?"),
            tr("Are you sure you want to delete this join document?"),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        from shared.puzzle_service import get_puzzle_service
        svc = get_puzzle_service()
        # Auto-unpublish from Supabase if published
        try:
            parent = self.parent()
            while parent and not hasattr(parent, 'corrections_client'):
                parent = parent.parent()
            if parent and parent.corrections_client and parent.corrections_client.is_logged_in():
                parent.corrections_client.unpublish_puzzle_join(doc_id)
        except Exception:
            pass  # Not published or not logged in -- fine
        svc.delete_document(doc_id)
        if self._current_doc_id == doc_id:
            self._current_doc_id = None
            self._has_unsaved_changes = False
            self._is_published = False
            self._details_group.setVisible(False)
            self.setWindowTitle(tr("Fragment Puzzle"))
        self._refresh_docs_list()

    def _rename_document(self, doc_id):
        """Rename a saved join document."""
        from shared.puzzle_service import get_puzzle_service
        svc = get_puzzle_service()
        doc = svc.load_document(doc_id)
        if not doc:
            return
        title, ok = QInputDialog.getText(
            self, tr("Rename"), tr("New title:"),
            QLineEdit.EchoMode.Normal, doc.title
        )
        if ok and title.strip():
            doc.title = title.strip()
            import datetime
            doc.updated_at = datetime.datetime.now().isoformat()
            # Use thumbnail_b64=None to preserve existing thumbnail
            svc.save_document(doc)
            self._refresh_docs_list()
            if self._current_doc_id == doc_id:
                self._title_edit.setText(doc.title)
                self.setWindowTitle(f"{tr('Fragment Puzzle')} - {doc.title}")

    def _on_title_changed(self):
        """Handle title edit finished -- auto-save if editing a saved document."""
        if self._current_doc_id is None:
            return
        self._schedule_auto_save()

    def _on_notes_changed(self):
        """Handle notes text changed -- auto-save if editing a saved document."""
        if self._current_doc_id is None:
            return
        self._schedule_auto_save()

    def _on_scene_changed(self, region_list):
        """Handle scene.changed signal -- debounce and trigger auto-save for saved documents."""
        if self._current_doc_id is not None and self._fragment_items:
            self._scene_change_debounce.start()

    def _schedule_auto_save(self):
        """Schedule a debounced auto-save (1.5s)."""
        if self._current_doc_id is None:
            self._has_unsaved_changes = True
            return
        self._auto_save_timer.start()  # restarts if already running

    def _auto_save(self):
        """Perform auto-save for the current document."""
        if self._current_doc_id is None:
            return
        # Do NOT auto-save while document is still loading
        if self._loading_document:
            return

        from shared.puzzle_service import get_puzzle_service
        from shared.puzzle_export import generate_thumbnail
        from shared.puzzle_image_service import get_puzzle_image_service

        fragments = self._build_fragments_list()
        svc = get_puzzle_service()
        doc = svc.load_document(self._current_doc_id)
        if doc is None:
            return
        doc.fragments = fragments
        doc.title = self._title_edit.text() or doc.title
        doc.notes = self._notes_edit.toPlainText()
        import datetime
        doc.updated_at = datetime.datetime.now().isoformat()
        # Regenerate thumbnail
        img_svc = get_puzzle_image_service()
        thumb = generate_thumbnail(fragments, img_svc, thumb_size=150)
        svc.save_document(doc, thumbnail_b64=thumb)
        self._refresh_docs_list()
        self.statusBar().showMessage(tr("Auto-saved"), 1500)

    def _update_fragments_label(self):
        """Update the fragments read-only label in the details panel."""
        parts = []
        for key, item in self._fragment_items.items():
            sys_id, folio_label = key
            pf = item.puzzle_frag
            sm = pf.shelfmark or sys_id
            parts.append(f"{sm} ({folio_label})")
        self._fragments_label.setText('\n'.join(parts) if parts else tr("No fragments"))

    # -- Cleanup --

    def keyPressEvent(self, event):
        """Keyboard shortcuts for puzzle canvas.

        Esc         - Exit crop mode, or close window
        Delete      - Delete selected fragments
        R / Shift+R - Rotate selected 1 deg CW / CCW
        F           - Flip recto/verso
        Ctrl+A      - Select all
        Ctrl+0      - Fit all fragments in view
        Arrow keys  - Move selected (or crop edges in crop mode)
        +/-         - Scale up/down
        """
        key = event.key()
        mod = event.modifiers()
        crop_active = self.btn_crop.isChecked()

        if key == Qt.Key.Key_Escape:
            if crop_active:
                self._revert_crop()
            else:
                self.close()
        elif key == Qt.Key.Key_Return and crop_active:
            # Confirm crop
            self.btn_crop.setChecked(False)
        elif key == Qt.Key.Key_Delete:
            self._delete_selected()
        elif key == Qt.Key.Key_R:
            if mod & Qt.KeyboardModifier.ShiftModifier:
                self._rotate_selected(-1)
            else:
                self._rotate_selected(1)
        elif key == Qt.Key.Key_F and not (mod & Qt.KeyboardModifier.ControlModifier):
            self._flip_recto_verso()
        elif key == Qt.Key.Key_A and mod & Qt.KeyboardModifier.ControlModifier:
            for item in self.canvas_view.get_fragment_items():
                item.setSelected(True)
        elif key == Qt.Key.Key_0 and mod & Qt.KeyboardModifier.ControlModifier:
            self._fit_all_fragments()
        elif key in (Qt.Key.Key_Plus, Qt.Key.Key_Equal):
            self._nudge_scale(5)
        elif key == Qt.Key.Key_Minus:
            self._nudge_scale(-5)
        elif key in (Qt.Key.Key_Up, Qt.Key.Key_Down, Qt.Key.Key_Left, Qt.Key.Key_Right):
            if crop_active:
                edge_map = {Qt.Key.Key_Up: "top", Qt.Key.Key_Down: "bottom",
                            Qt.Key.Key_Left: "left", Qt.Key.Key_Right: "right"}
                self._crop_edge(edge_map[key])
            else:
                # Convert view-space direction to scene-space delta
                # This handles any view transform (zoom, fitInView flips)
                step = 5
                view_dx = {Qt.Key.Key_Left: -step, Qt.Key.Key_Right: step}.get(key, 0)
                view_dy = {Qt.Key.Key_Up: -step, Qt.Key.Key_Down: step}.get(key, 0)
                # Map a view-space vector to scene-space
                origin = self.canvas_view.mapToScene(0, 0)
                target = self.canvas_view.mapToScene(int(view_dx), int(view_dy))
                dx = target.x() - origin.x()
                dy = target.y() - origin.y()
                for it in self.canvas_view.get_selected_fragments():
                    it.setPos(it.pos().x() + dx, it.pos().y() + dy)
                    it.puzzle_frag.x = it.pos().x()
                    it.puzzle_frag.y = it.pos().y()
        else:
            super().keyPressEvent(event)

    def closeEvent(self, event):
        """Wait for active loader threads before closing."""
        if self._export_thread and self._export_thread.isRunning():
            self._export_thread.requestInterruption()
            self._export_thread.wait(3000)
        for t in self._loader_threads + self._meta_threads:
            if t.isRunning():
                t.wait(2000)
        super().closeEvent(event)
