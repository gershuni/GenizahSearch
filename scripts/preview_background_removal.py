# -*- coding: utf-8 -*-
"""
Interactive Background Removal Preview Tool for Fragment Puzzle.

Loads real IIIF manuscript images through the shared image resolver/cache,
applies background removal, and shows original vs stripped side-by-side
with an adjustable threshold slider.

Usage:
    python scripts/preview_background_removal.py

Requires: PyQt6, Pillow, numpy, requests
"""

import io
import os
import sys
import time

# Add project root so shared imports work
_script_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_script_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import numpy as np
from PIL import Image
import requests

from PyQt6.QtCore import Qt, QByteArray, QBuffer
from PyQt6.QtGui import QImage, QPixmap, QPainter, QColor, QBrush
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QHBoxLayout, QVBoxLayout,
    QLabel, QPushButton, QSlider, QComboBox, QLineEdit, QGroupBox,
    QSplitter, QStatusBar, QScrollArea
)

from shared.puzzle_image_service import PuzzleImageService
from shared.background_removal import (
    remove_background, detect_background_color,
    DEFAULT_THRESHOLD, LOW_SATURATION_THRESHOLD
)


# ── Sample images from different libraries ──

SAMPLE_IMAGES = [
    # (Label, type, id_or_url)
    # NLI-hosted images -- use real NLI FL IDs (9+ digits from IIIF manifests)
    ("NLI: T-S 12.1 recto (CUL)", "nli", "166909775"),
    ("NLI: T-S 12.1 verso (CUL)", "nli", "166909776"),
    # AIU (Paris)
    ("NLI: H 147 A recto (AIU)", "nli", "47443607"),
    ("NLI: H 147 A verso (AIU)", "nli", "47443612"),
    # Manchester
    ("NLI: A 1 recto (Manchester)", "nli", "168498808"),
    ("NLI: A 1 verso (Manchester)", "nli", "168498811"),
    # Cambridge (direct IIIF, not NLI-hosted)
    ("Cambridge: MS-ADD-00863-00002 p1", "cambridge", "MS-ADD-00863-00002"),
    ("Cambridge: MS-TS-00012-00001 p1", "cambridge", "MS-TS-00012-00001"),
]

CAMBRIDGE_IIIF_BASE = "https://images.lib.cam.ac.uk/iiif"


def _create_checkerboard(width: int, height: int, tile_size: int = 16) -> QPixmap:
    """Create a checkerboard pattern pixmap for transparency visualization."""
    img = QImage(width, height, QImage.Format.Format_RGB32)
    light = QColor(220, 220, 220)
    dark = QColor(180, 180, 180)
    for y in range(0, height, tile_size):
        for x in range(0, width, tile_size):
            col = light if ((x // tile_size) + (y // tile_size)) % 2 == 0 else dark
            for dy in range(min(tile_size, height - y)):
                for dx in range(min(tile_size, width - x)):
                    img.setPixelColor(x + dx, y + dy, col)
    return QPixmap.fromImage(img)


def _rgba_bytes_to_pixmap(rgba_bytes: bytes, with_checkerboard: bool = True) -> QPixmap:
    """Convert RGBA PNG bytes to QPixmap, optionally composited over checkerboard."""
    img = Image.open(io.BytesIO(rgba_bytes)).convert('RGBA')
    w, h = img.size

    if with_checkerboard:
        # Composite over checkerboard
        checker = Image.new('RGBA', (w, h), (220, 220, 220, 255))
        tile = 16
        dark = (180, 180, 180, 255)
        pixels = checker.load()
        for y in range(h):
            for x in range(w):
                if ((x // tile) + (y // tile)) % 2 != 0:
                    pixels[x, y] = dark
        result = Image.alpha_composite(checker, img)
        result = result.convert('RGB')
        data = result.tobytes('raw', 'RGB')
        qimg = QImage(data, w, h, w * 3, QImage.Format.Format_RGB888)
    else:
        data = img.tobytes('raw', 'RGBA')
        qimg = QImage(data, w, h, w * 4, QImage.Format.Format_RGBA8888)

    # QImage references external data, must copy
    return QPixmap.fromImage(qimg.copy())


def _jpeg_bytes_to_pixmap(jpeg_bytes: bytes) -> QPixmap:
    """Convert JPEG/PNG bytes to QPixmap."""
    pixmap = QPixmap()
    pixmap.loadFromData(QByteArray(jpeg_bytes))
    return pixmap


class BackgroundRemovalPreview(QMainWindow):
    """Interactive preview window for background removal tuning."""

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Background Removal Preview - Fragment Puzzle")
        self.resize(1400, 800)

        self._service = PuzzleImageService()
        self._original_bytes: bytes | None = None
        self._processed_bytes: bytes | None = None
        self._show_stripped = True  # Toggle state

        self._build_ui()
        self.statusBar().showMessage("Select an image or enter an FL ID to begin.")

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)

        # ── Top controls ──
        controls = QGroupBox("Controls")
        controls_layout = QHBoxLayout(controls)

        # Sample dropdown
        controls_layout.addWidget(QLabel("Sample:"))
        self._combo = QComboBox()
        self._combo.addItem("-- Select sample --")
        for label, _type, _id in SAMPLE_IMAGES:
            self._combo.addItem(label)
        self._combo.currentIndexChanged.connect(self._on_sample_selected)
        controls_layout.addWidget(self._combo)

        controls_layout.addWidget(QLabel("  FL ID:"))
        self._fl_input = QLineEdit()
        self._fl_input.setPlaceholderText("e.g. 322229")
        self._fl_input.setMaximumWidth(300)
        controls_layout.addWidget(self._fl_input)

        self._load_btn = QPushButton("Load")
        self._load_btn.clicked.connect(self._on_load_clicked)
        controls_layout.addWidget(self._load_btn)

        controls_layout.addStretch()

        # Threshold slider
        controls_layout.addWidget(QLabel("Threshold:"))
        self._slider = QSlider(Qt.Orientation.Horizontal)
        self._slider.setRange(5, 150)
        self._slider.setValue(int(DEFAULT_THRESHOLD))
        self._slider.setMaximumWidth(200)
        self._slider.valueChanged.connect(self._on_threshold_changed)
        controls_layout.addWidget(self._slider)

        self._threshold_label = QLabel(str(int(DEFAULT_THRESHOLD)))
        self._threshold_label.setMinimumWidth(30)
        controls_layout.addWidget(self._threshold_label)

        # Toggle button
        self._toggle_btn = QPushButton("Show Original")
        self._toggle_btn.setCheckable(True)
        self._toggle_btn.clicked.connect(self._on_toggle)
        controls_layout.addWidget(self._toggle_btn)

        main_layout.addWidget(controls)

        # ── Image panels ──
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # Left: Original
        left_group = QGroupBox("Original")
        left_layout = QVBoxLayout(left_group)
        self._left_scroll = QScrollArea()
        self._left_scroll.setWidgetResizable(True)
        self._left_label = QLabel("No image loaded")
        self._left_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._left_scroll.setWidget(self._left_label)
        left_layout.addWidget(self._left_scroll)
        splitter.addWidget(left_group)

        # Right: Processed
        right_group = QGroupBox("Background Removed")
        right_layout = QVBoxLayout(right_group)
        self._right_scroll = QScrollArea()
        self._right_scroll.setWidgetResizable(True)
        self._right_label = QLabel("No image loaded")
        self._right_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._right_scroll.setWidget(self._right_label)
        right_layout.addWidget(self._right_scroll)
        splitter.addWidget(right_group)

        main_layout.addWidget(splitter, stretch=1)

        # ── Info panel ──
        info_group = QGroupBox("Detection Info")
        info_layout = QHBoxLayout(info_group)
        self._info_label = QLabel("Load an image to see detection info.")
        self._info_label.setWordWrap(True)
        info_layout.addWidget(self._info_label)
        main_layout.addWidget(info_group)

    def _on_sample_selected(self, index: int):
        """Handle dropdown selection."""
        if index <= 0:
            return
        _label, img_type, img_id = SAMPLE_IMAGES[index - 1]
        if img_type == "nli":
            self._fl_input.setText(img_id)
            self._load_nli_image(img_id)
        elif img_type == "cambridge":
            self._fl_input.setText(f"cambridge:{img_id}")
            self._load_cambridge_image(img_id)

    def _on_load_clicked(self):
        """Handle Load button click."""
        text = self._fl_input.text().strip()
        if not text:
            self.statusBar().showMessage("Enter an FL ID or select a sample.")
            return
        if text.startswith("cambridge:"):
            self._load_cambridge_image(text[len("cambridge:"):])
        else:
            self._load_nli_image(text)

    def _load_nli_image(self, fl_id: str):
        """Load image via shared PuzzleImageService (NLI IIIF)."""
        self.statusBar().showMessage(f"Fetching NLI image FL{fl_id}...")
        QApplication.processEvents()

        # Fetch original (unprocessed)
        original = self._service.resolve_fragment_image(
            fl_id, size=1200, processed=False
        )
        if original is None:
            self.statusBar().showMessage(f"Failed to fetch image for FL ID: {fl_id}")
            return

        self._original_bytes = original
        self._apply_background_removal()

    def _load_cambridge_image(self, classmark: str):
        """Load Cambridge image directly via IIIF (not NLI-hosted)."""
        self.statusBar().showMessage(f"Fetching Cambridge image {classmark}...")
        QApplication.processEvents()

        # Cambridge IIIF: first canvas image
        url = f"{CAMBRIDGE_IIIF_BASE}/{classmark}-000-00001.jp2/full/1200,/0/default.jpg"
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            self._original_bytes = resp.content
        except Exception as e:
            self.statusBar().showMessage(f"Cambridge fetch failed: {e}")
            return

        self._apply_background_removal()

    def _apply_background_removal(self):
        """Run background removal on cached original bytes and update display."""
        if self._original_bytes is None:
            return

        threshold = float(self._slider.value())

        # Show original on left
        orig_pixmap = _jpeg_bytes_to_pixmap(self._original_bytes)
        self._left_label.setPixmap(orig_pixmap)

        # Run background removal with timing
        t0 = time.perf_counter()
        try:
            self._processed_bytes = remove_background(
                self._original_bytes, threshold=threshold
            )
        except Exception as e:
            self.statusBar().showMessage(f"Background removal failed: {e}")
            return
        elapsed_ms = (time.perf_counter() - t0) * 1000

        # Show processed on right
        self._update_right_panel()

        # Compute and display detection info
        self._update_info(elapsed_ms)

        self.statusBar().showMessage(
            f"Done. Threshold={threshold:.0f}, Processing={elapsed_ms:.0f}ms"
        )

    def _update_right_panel(self):
        """Update the right panel based on toggle state."""
        if self._show_stripped and self._processed_bytes:
            pixmap = _rgba_bytes_to_pixmap(self._processed_bytes, with_checkerboard=True)
            self._right_label.setPixmap(pixmap)
        elif self._original_bytes:
            pixmap = _jpeg_bytes_to_pixmap(self._original_bytes)
            self._right_label.setPixmap(pixmap)

    def _update_info(self, elapsed_ms: float):
        """Update the info panel with detection details."""
        if self._original_bytes is None:
            return

        try:
            img = Image.open(io.BytesIO(self._original_bytes)).convert('RGB')
            hsv_img = img.convert('HSV')
            hsv_array = np.array(hsv_img)
            bg_color = detect_background_color(hsv_array)

            bg_h, bg_s, bg_v = bg_color
            bg_saturation = bg_s
            low_sat = bg_saturation < LOW_SATURATION_THRESHOLD

            # Compute foreground ratio from processed image
            fg_ratio = "N/A"
            if self._processed_bytes:
                proc_img = Image.open(io.BytesIO(self._processed_bytes)).convert('RGBA')
                alpha = np.array(proc_img)[:, :, 3]
                fg_pixels = np.count_nonzero(alpha)
                total_pixels = alpha.size
                fg_ratio = f"{fg_pixels / total_pixels * 100:.1f}%"

            info_parts = [
                f"<b>Background HSV:</b> H={bg_h:.0f}, S={bg_s:.0f}, V={bg_v:.0f} (0-255 scale)",
                f"<b>Low-saturation fallback:</b> {'YES (S < {})'.format(LOW_SATURATION_THRESHOLD) if low_sat else 'No (S >= {})'.format(LOW_SATURATION_THRESHOLD)}",
                f"<b>Foreground ratio:</b> {fg_ratio}",
                f"<b>Processing time:</b> {elapsed_ms:.0f} ms",
                f"<b>Image size:</b> {img.size[0]}x{img.size[1]}",
            ]
            self._info_label.setText("  |  ".join(info_parts))
        except Exception as e:
            self._info_label.setText(f"Info error: {e}")

    def _on_threshold_changed(self, value: int):
        """Re-process with new threshold (no re-fetch)."""
        self._threshold_label.setText(str(value))
        if self._original_bytes is not None:
            self._apply_background_removal()

    def _on_toggle(self, checked: bool):
        """Toggle between stripped and original view on right panel."""
        self._show_stripped = not checked
        if checked:
            self._toggle_btn.setText("Show Stripped")
            self.findChild(QGroupBox, "")  # no-op, just update label
        else:
            self._toggle_btn.setText("Show Original")

        # Update the right panel group box title
        right_group = self.centralWidget().findChild(QSplitter).widget(1)
        if isinstance(right_group, QGroupBox):
            right_group.setTitle("Original" if checked else "Background Removed")

        self._update_right_panel()


def main():
    app = QApplication(sys.argv)
    window = BackgroundRemovalPreview()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
