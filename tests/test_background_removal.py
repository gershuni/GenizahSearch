# -*- coding: utf-8 -*-
"""Tests for the background removal engine (shared/background_removal.py).

All tests use synthetic images created with Pillow -- no external test fixtures needed.
"""

import io
import numpy as np
from PIL import Image

from shared.background_removal import (
    remove_background,
    detect_background_color,
    detect_blue_mat,
    create_blue_mat_mask,
    create_cul_blue_mask,  # legacy alias
    BLUE_MAT_DETECT_THRESHOLD,
)


def make_test_image(bg_color, fg_color, size=200, fg_size=50) -> bytes:
    """Create a test image with solid background and centered foreground square.

    Returns PNG bytes.
    """
    img = Image.new('RGB', (size, size), bg_color)
    # Draw centered foreground square
    x0 = (size - fg_size) // 2
    y0 = (size - fg_size) // 2
    for x in range(x0, x0 + fg_size):
        for y in range(y0, y0 + fg_size):
            img.putpixel((x, y), fg_color)
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    return buf.getvalue()


def open_result(result_bytes: bytes) -> Image.Image:
    """Open result bytes as PIL Image."""
    return Image.open(io.BytesIO(result_bytes))


class TestSolidBackgroundRemoval:
    """Tests for removing solid-color backgrounds."""

    def test_solid_blue_background_removed(self):
        """Blue (0,0,255) background should become transparent, white center stays opaque."""
        img_bytes = make_test_image((0, 0, 255), (255, 255, 255))
        result = remove_background(img_bytes)
        result_img = open_result(result)

        assert result_img.mode == 'RGBA'
        # Corner should be transparent
        assert result_img.getpixel((5, 5))[3] == 0
        # Center should be opaque
        assert result_img.getpixel((100, 100))[3] == 255

    def test_solid_green_background_removed(self):
        """Green (0,128,0) background should become transparent, white center stays opaque."""
        img_bytes = make_test_image((0, 128, 0), (255, 255, 255))
        result = remove_background(img_bytes)
        result_img = open_result(result)

        assert result_img.mode == 'RGBA'
        assert result_img.getpixel((5, 5))[3] == 0
        assert result_img.getpixel((100, 100))[3] == 255

    def test_solid_gray_background_removed(self):
        """Gray (128,128,128) is low-saturation -- value-only fallback should handle it."""
        img_bytes = make_test_image((128, 128, 128), (255, 255, 255))
        result = remove_background(img_bytes)
        result_img = open_result(result)

        assert result_img.mode == 'RGBA'
        # Gray corners should be transparent
        assert result_img.getpixel((5, 5))[3] == 0
        # White center should be opaque
        assert result_img.getpixel((100, 100))[3] == 255

    def test_low_saturation_cream_background(self):
        """Cream (235,225,200) background with brown (120,80,50) foreground.

        Cream has low saturation, so value-only distance fallback activates.
        """
        img_bytes = make_test_image((235, 225, 200), (120, 80, 50))
        result = remove_background(img_bytes)
        result_img = open_result(result)

        assert result_img.mode == 'RGBA'
        # Cream corner should be transparent
        assert result_img.getpixel((5, 5))[3] == 0
        # Brown center should be opaque
        assert result_img.getpixel((100, 100))[3] == 255


class TestThresholdAndSafety:
    """Tests for threshold control and safety fallback."""

    def test_threshold_affects_mask(self):
        """Lower threshold = tighter removal, higher = more aggressive.

        Uses a gray background with a wide gradient ring so threshold
        difference is clearly visible after morphological cleanup.
        """
        # Gray background (low saturation — uses value-channel distance)
        img = Image.new('RGB', (300, 300), (80, 80, 80))
        # Center white square
        for x in range(120, 180):
            for y in range(120, 180):
                img.putpixel((x, y), (255, 255, 255))
        # Wide gradient ring — values from 100 to 220 (between bg=80 and fg=255)
        for x in range(60, 240):
            for y in range(60, 240):
                if not (120 <= x < 180 and 120 <= y < 180):
                    t = min(abs(x - 150), abs(y - 150)) / 90.0
                    v = int(80 + t * 175)  # 80 (bg-like) to 255 (fg-like)
                    img.putpixel((x, y), (v, v, v))
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        img_bytes = buf.getvalue()

        result_tight = remove_background(img_bytes, threshold=5.0)
        result_aggressive = remove_background(img_bytes, threshold=80.0)

        tight_img = open_result(result_tight)
        aggressive_img = open_result(result_aggressive)

        # Count opaque pixels
        tight_opaque = sum(1 for x in range(300) for y in range(300)
                          if tight_img.getpixel((x, y))[3] > 0)
        aggressive_opaque = sum(1 for x in range(300) for y in range(300)
                                if aggressive_img.getpixel((x, y))[3] > 0)

        # Tight threshold should keep more pixels opaque (gradient pixels survive)
        assert tight_opaque > aggressive_opaque

    def test_safety_check_preserves_content(self):
        """Uniform-color image: removal would eliminate >95%, so safety fallback keeps all opaque."""
        # All one color -- everything would be "background"
        img = Image.new('RGB', (100, 100), (0, 0, 255))
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        img_bytes = buf.getvalue()

        result = remove_background(img_bytes)
        result_img = open_result(result)

        assert result_img.mode == 'RGBA'
        # All pixels should be opaque (safety fallback)
        for x in [5, 50, 95]:
            for y in [5, 50, 95]:
                assert result_img.getpixel((x, y))[3] == 255

    def test_small_fragment_large_margin(self):
        """Small foreground on large background -- use min_foreground_ratio=0.005 to allow removal."""
        img_bytes = make_test_image((0, 0, 255), (255, 255, 255), size=400, fg_size=30)
        # Foreground ratio ~0.56%, below default 5%, so pass explicit low threshold
        result = remove_background(img_bytes, min_foreground_ratio=0.005)
        result_img = open_result(result)

        assert result_img.mode == 'RGBA'
        # Corner should be transparent (removal happened)
        assert result_img.getpixel((5, 5))[3] == 0
        # Center should be opaque
        assert result_img.getpixel((200, 200))[3] == 255

    def test_min_foreground_ratio_parameter(self):
        """remove_background() accepts and respects min_foreground_ratio parameter."""
        img_bytes = make_test_image((0, 0, 255), (255, 255, 255), size=400, fg_size=30)

        # With very low ratio, removal proceeds
        result_low = remove_background(img_bytes, min_foreground_ratio=0.001)
        result_low_img = open_result(result_low)
        assert result_low_img.getpixel((5, 5))[3] == 0  # transparent corner

        # With high ratio (0.99), safety fallback triggers (foreground < 99%)
        result_high = remove_background(img_bytes, min_foreground_ratio=0.99)
        result_high_img = open_result(result_high)
        assert result_high_img.getpixel((5, 5))[3] == 255  # opaque (safety)


class TestOutputFormat:
    """Tests for output format and data integrity."""

    def test_original_preserved(self):
        """Original bytes are not mutated. Both are valid images. Output is RGBA."""
        img_bytes = make_test_image((0, 0, 255), (255, 255, 255))
        original_copy = img_bytes[:]

        result = remove_background(img_bytes)

        # Original not mutated
        assert img_bytes == original_copy
        # Both are valid images
        assert Image.open(io.BytesIO(img_bytes)).size == (200, 200)
        result_img = open_result(result)
        assert result_img.size == (200, 200)
        assert result_img.mode == 'RGBA'

    def test_output_is_rgba_png(self):
        """Output starts with PNG signature and is RGBA mode."""
        img_bytes = make_test_image((0, 0, 255), (255, 255, 255))
        result = remove_background(img_bytes)

        # PNG signature
        assert result[:4] == b'\x89PNG'
        # RGBA mode
        result_img = open_result(result)
        assert result_img.mode == 'RGBA'


class TestDetectBackgroundColor:
    """Tests for detect_background_color()."""

    def test_detect_background_color_from_corners(self):
        """Detect background color from corners of an image with blue background."""
        img = Image.new('RGB', (200, 200), (0, 0, 255))
        # Put different color in center
        for x in range(80, 120):
            for y in range(80, 120):
                img.putpixel((x, y), (255, 255, 255))

        hsv_img = img.convert('HSV')
        hsv_array = np.array(hsv_img)
        bg_color = detect_background_color(hsv_array)

        # Should detect blue-ish HSV from corners (not white from center)
        # Blue in Pillow HSV: H~170 (0-255 scale maps 240deg), S~255, V~255
        assert bg_color.shape == (3,)
        # Saturation should be high (it's a pure blue)
        assert bg_color[1] > 200


def make_two_layer_image(outer_color, inner_color, fg_color,
                         size=300, inner_margin=40, fg_size=60) -> bytes:
    """Create test image with outer border, inner mat, and centered foreground.

    Layout: outer_color fills entire image, inner_color fills a rectangle
    inset by inner_margin, fg_color is a centered square of fg_size.
    This simulates CUL images: gray border → blue mat → parchment.
    """
    img = Image.new('RGB', (size, size), outer_color)
    # Draw inner mat
    for x in range(inner_margin, size - inner_margin):
        for y in range(inner_margin, size - inner_margin):
            img.putpixel((x, y), inner_color)
    # Draw centered foreground
    x0 = (size - fg_size) // 2
    y0 = (size - fg_size) // 2
    for x in range(x0, x0 + fg_size):
        for y in range(y0, y0 + fg_size):
            img.putpixel((x, y), fg_color)
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    return buf.getvalue()


class TestCulBlueMatRemoval:
    """Tests for CUL blue conservation mat removal (is_cul=True)."""

    def test_blue_mat_removed_border_kept(self):
        """is_cul removes blue mat only; border frame is kept (cream borders match parchment)."""
        img_bytes = make_two_layer_image(
            outer_color=(180, 180, 180),  # gray border
            inner_color=(0, 0, 255),       # blue mat
            fg_color=(220, 200, 170),      # parchment-like
        )
        result = remove_background(img_bytes, is_cul=True)
        result_img = open_result(result)

        assert result_img.mode == 'RGBA'
        # Corner (gray border) stays opaque — CUL mode only targets blue
        assert result_img.getpixel((5, 5))[3] == 255
        # Inner mat area (blue) should be transparent
        assert result_img.getpixel((50, 150))[3] == 0
        # Center (parchment) should be opaque
        assert result_img.getpixel((150, 150))[3] == 255

    def test_darker_blue_mat_removed(self):
        """CUL blue mat with more realistic darker blue shade."""
        img_bytes = make_two_layer_image(
            outer_color=(200, 200, 200),   # light gray border
            inner_color=(30, 50, 180),      # darker CUL blue
            fg_color=(210, 190, 160),       # parchment
        )
        result = remove_background(img_bytes, is_cul=True)
        result_img = open_result(result)

        # Border kept (CUL only targets blue), blue removed, parchment kept
        assert result_img.getpixel((5, 5))[3] == 255     # border kept
        assert result_img.getpixel((50, 150))[3] == 0     # blue mat removed
        assert result_img.getpixel((150, 150))[3] == 255  # parchment kept

    def test_blue_only_no_border_cul(self):
        """CUL image with only blue background (no gray border)."""
        img_bytes = make_test_image((0, 0, 255), (220, 200, 170))
        result = remove_background(img_bytes, is_cul=True)
        result_img = open_result(result)

        # Blue removed, parchment kept
        assert result_img.getpixel((5, 5))[3] == 0
        assert result_img.getpixel((100, 100))[3] == 255

    def test_is_cul_false_auto_detects_blue_mat(self):
        """Without is_cul=True, blue mat IS now auto-detected and removed."""
        img_bytes = make_two_layer_image(
            outer_color=(180, 180, 180),  # gray border (corners detect this)
            inner_color=(0, 0, 255),       # blue mat
            fg_color=(220, 200, 170),      # parchment
        )
        # Without is_cul, auto-detection triggers (blue mat >2% of pixels)
        result = remove_background(img_bytes, is_cul=False)
        result_img = open_result(result)

        # Blue mat IS removed (auto-detected)
        assert result_img.getpixel((50, 150))[3] == 0
        # Gray border KEPT (auto-detect uses blue-only mask for safety)
        assert result_img.getpixel((5, 5))[3] == 255
        # Parchment kept
        assert result_img.getpixel((150, 150))[3] == 255

    def test_cul_blue_mask_direct(self):
        """create_cul_blue_mask correctly identifies blue pixels by hue range."""
        img = Image.new('RGB', (200, 200), (30, 60, 180))  # blue
        # Parchment center
        for x in range(80, 120):
            for y in range(80, 120):
                img.putpixel((x, y), (220, 200, 170))

        hsv_array = np.array(img.convert('HSV'))
        mask = create_cul_blue_mask(hsv_array)
        mask_arr = np.array(mask)

        # Blue corners should be 0 (background)
        assert mask_arr[5, 5] == 0
        # Parchment center should be 255 (foreground)
        assert mask_arr[100, 100] == 255

    def test_is_cul_only_targets_blue(self):
        """is_cul=True only removes blue pixels, not gray/white backgrounds."""
        img_bytes = make_test_image((128, 128, 128), (255, 255, 255))

        result_normal = remove_background(img_bytes, is_cul=False)
        result_cul = remove_background(img_bytes, is_cul=True)

        normal_img = open_result(result_normal)
        cul_img = open_result(result_cul)

        # Without is_cul: gray removed normally
        assert normal_img.getpixel((5, 5))[3] == 0
        assert normal_img.getpixel((100, 100))[3] == 255
        # With is_cul: gray NOT removed (only blue is targeted)
        assert cul_img.getpixel((5, 5))[3] == 255
        assert cul_img.getpixel((100, 100))[3] == 255


class TestBlueMatAutoDetection:
    """Tests for automatic blue mat detection (no is_cul hint needed)."""

    def test_detect_blue_mat_returns_high_for_blue_image(self):
        """detect_blue_mat() returns high fraction for image with blue background."""
        img = Image.new('RGB', (200, 200), (0, 0, 255))  # blue
        # Small parchment center
        for x in range(80, 120):
            for y in range(80, 120):
                img.putpixel((x, y), (220, 200, 170))
        hsv_array = np.array(img.convert('HSV'))
        frac = detect_blue_mat(hsv_array)
        # ~96% blue (200x200 - 40x40 center = 38400/40000)
        assert frac > 0.5

    def test_detect_blue_mat_returns_low_for_gray_image(self):
        """detect_blue_mat() returns ~0 for gray background image."""
        img = Image.new('RGB', (200, 200), (128, 128, 128))
        for x in range(80, 120):
            for y in range(80, 120):
                img.putpixel((x, y), (255, 255, 255))
        hsv_array = np.array(img.convert('HSV'))
        frac = detect_blue_mat(hsv_array)
        assert frac < BLUE_MAT_DETECT_THRESHOLD

    def test_auto_detect_triggers_on_blue_background(self):
        """Blue background auto-detected and removed even without is_cul=True."""
        # Blue background with white foreground — enough blue to trigger (>2%)
        img_bytes = make_test_image((0, 0, 255), (255, 255, 255))
        result = remove_background(img_bytes, is_cul=False)
        result_img = open_result(result)

        # Blue corners should be transparent (auto-detected and removed)
        assert result_img.getpixel((5, 5))[3] == 0
        # White center should be opaque
        assert result_img.getpixel((100, 100))[3] == 255

    def test_auto_detect_does_not_trigger_on_gray(self):
        """Gray background does not trigger blue auto-detection."""
        img_bytes = make_test_image((128, 128, 128), (255, 255, 255))
        result = remove_background(img_bytes, is_cul=False)
        result_img = open_result(result)

        # Gray corners removed by normal mask, not blue mask
        assert result_img.getpixel((5, 5))[3] == 0
        assert result_img.getpixel((100, 100))[3] == 255

    def test_auto_detect_removes_blue_keeps_gray_border(self):
        """Auto-detected blue: removes blue mat but keeps gray border (blue-only mask).

        Auto-detect uses blue-only mask (same as explicit is_cul) to avoid
        corner-sampling damage when fragments touch the image edges.
        """
        img_bytes = make_two_layer_image(
            outer_color=(180, 180, 180),  # gray border
            inner_color=(0, 0, 255),       # blue mat
            fg_color=(220, 200, 170),      # parchment
        )
        # Without is_cul, but blue mat is >2% of pixels, so auto-detect triggers
        result = remove_background(img_bytes, is_cul=False)
        result_img = open_result(result)

        # Gray border KEPT (blue-only mask, same as explicit is_cul)
        assert result_img.getpixel((5, 5))[3] == 255
        # Blue mat should be transparent
        assert result_img.getpixel((50, 150))[3] == 0
        # Parchment center should be opaque
        assert result_img.getpixel((150, 150))[3] == 255

    def test_auto_detect_same_as_explicit_is_cul(self):
        """Auto-detect produces same result as explicit is_cul=True.

        Both use blue-only mask. Auto-detect is conservative: it avoids
        corner-sampled normal mask which can damage parchment at edges.
        """
        img_bytes = make_two_layer_image(
            outer_color=(180, 180, 180),  # gray border
            inner_color=(0, 0, 255),       # blue mat
            fg_color=(220, 200, 170),      # parchment
        )

        result_explicit = remove_background(img_bytes, is_cul=True)
        result_auto = remove_background(img_bytes, is_cul=False)

        explicit_img = open_result(result_explicit)
        auto_img = open_result(result_auto)

        # Both keep gray border (blue-only mask)
        assert explicit_img.getpixel((5, 5))[3] == 255
        assert auto_img.getpixel((5, 5))[3] == 255
        # Both remove blue mat
        assert explicit_img.getpixel((50, 150))[3] == 0
        assert auto_img.getpixel((50, 150))[3] == 0

    def test_small_blue_below_threshold_not_detected(self):
        """Tiny blue area (<2%) does not trigger auto-detection."""
        # 200x200 = 40000 pixels. 2% = 800 pixels. Make blue area ~400 (1%)
        img = Image.new('RGB', (200, 200), (128, 128, 128))  # gray bg
        for x in range(90, 110):
            for y in range(90, 110):
                img.putpixel((x, y), (0, 0, 255))  # 20x20=400 blue pixels
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        img_bytes = buf.getvalue()

        hsv_array = np.array(img.convert('HSV'))
        frac = detect_blue_mat(hsv_array)
        assert frac < BLUE_MAT_DETECT_THRESHOLD  # <2%, not triggered

    def test_legacy_alias_create_cul_blue_mask(self):
        """Legacy alias create_cul_blue_mask still works."""
        assert create_cul_blue_mask is create_blue_mat_mask
