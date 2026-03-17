# -*- coding: utf-8 -*-
"""Tests for the background removal engine (shared/background_removal.py).

All tests use synthetic images created with Pillow -- no external test fixtures needed.
"""

import io
import pytest
import numpy as np
from PIL import Image

from shared.background_removal import (
    remove_background,
    detect_background_color,
    detect_edge_midpoint_color,
    create_mask,
    DEFAULT_THRESHOLD,
    MIN_FOREGROUND_RATIO,
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

        Uses an image with gradient border pixels so threshold difference is visible.
        """
        # Create image with gradient transition between bg and fg
        img = Image.new('RGB', (200, 200), (0, 0, 255))
        # Center white square
        for x in range(75, 125):
            for y in range(75, 125):
                img.putpixel((x, y), (255, 255, 255))
        # Add gradient border around the square (colors between blue and white)
        for x in range(60, 140):
            for y in range(60, 140):
                if not (75 <= x < 125 and 75 <= y < 125):
                    # Intermediate color -- closer to blue but not exact
                    img.putpixel((x, y), (100, 100, 255))
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        img_bytes = buf.getvalue()

        result_tight = remove_background(img_bytes, threshold=5.0)
        result_aggressive = remove_background(img_bytes, threshold=100.0)

        tight_img = open_result(result_tight)
        aggressive_img = open_result(result_aggressive)

        # Count opaque pixels
        tight_opaque = sum(1 for x in range(200) for y in range(200)
                          if tight_img.getpixel((x, y))[3] > 0)
        aggressive_opaque = sum(1 for x in range(200) for y in range(200)
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


class TestTwoLayerBackground:
    """Tests for two-pass background removal (border frame + colored mat)."""

    def test_gray_border_blue_mat_both_removed(self):
        """Gray border + blue mat: both should become transparent, parchment stays opaque."""
        img_bytes = make_two_layer_image(
            outer_color=(180, 180, 180),  # gray border
            inner_color=(0, 0, 255),       # blue mat
            fg_color=(220, 200, 170),      # parchment-like
        )
        result = remove_background(img_bytes)
        result_img = open_result(result)

        assert result_img.mode == 'RGBA'
        # Corner (gray border) should be transparent
        assert result_img.getpixel((5, 5))[3] == 0
        # Inner mat area (blue) should also be transparent
        assert result_img.getpixel((50, 150))[3] == 0
        # Center (parchment) should be opaque
        assert result_img.getpixel((150, 150))[3] == 255

    def test_white_border_green_mat_both_removed(self):
        """White border + green mat: both removed, foreground kept."""
        img_bytes = make_two_layer_image(
            outer_color=(240, 240, 240),  # white border
            inner_color=(0, 128, 0),       # green mat
            fg_color=(200, 180, 150),      # parchment-like
        )
        result = remove_background(img_bytes)
        result_img = open_result(result)

        # Corner (white) should be transparent
        assert result_img.getpixel((5, 5))[3] == 0
        # Green mat should be transparent
        assert result_img.getpixel((50, 150))[3] == 0
        # Center (parchment) should be opaque
        assert result_img.getpixel((150, 150))[3] == 255

    def test_single_layer_blue_unchanged(self):
        """Single blue background (no border): behavior unchanged from before."""
        img_bytes = make_test_image((0, 0, 255), (255, 255, 255))
        result = remove_background(img_bytes)
        result_img = open_result(result)

        # Blue corners transparent, white center opaque (same as before)
        assert result_img.getpixel((5, 5))[3] == 0
        assert result_img.getpixel((100, 100))[3] == 255

    def test_single_layer_gray_unchanged(self):
        """Single gray background (no border): behavior unchanged from before."""
        img_bytes = make_test_image((128, 128, 128), (255, 255, 255))
        result = remove_background(img_bytes)
        result_img = open_result(result)

        assert result_img.getpixel((5, 5))[3] == 0
        assert result_img.getpixel((100, 100))[3] == 255

    def test_detect_edge_midpoint_color_two_layer(self):
        """Edge midpoints should detect inner mat color, not outer border."""
        img = Image.new('RGB', (300, 300), (180, 180, 180))  # gray border
        # Blue inner mat
        for x in range(40, 260):
            for y in range(40, 260):
                img.putpixel((x, y), (0, 0, 255))
        # Small white center
        for x in range(120, 180):
            for y in range(120, 180):
                img.putpixel((x, y), (255, 255, 255))

        hsv_array = np.array(img.convert('HSV'))
        edge_color = detect_edge_midpoint_color(hsv_array)

        # Edge midpoints should detect the blue mat (high saturation)
        assert edge_color.shape == (3,)
        assert edge_color[1] > 200  # high saturation = blue

    def test_same_color_edges_no_second_pass(self):
        """When edge midpoints match corners (same single bg), no second pass triggered."""
        # Uniform blue background with white center — edges and corners both blue
        img_bytes = make_test_image((0, 0, 255), (255, 255, 255), size=200, fg_size=80)
        result = remove_background(img_bytes)
        result_img = open_result(result)

        # Should work exactly as single-pass: blue removed, white kept
        assert result_img.getpixel((5, 5))[3] == 0
        assert result_img.getpixel((100, 100))[3] == 255
