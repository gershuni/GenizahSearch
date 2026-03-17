# -*- coding: utf-8 -*-
"""
Background Removal Engine for Fragment Puzzle.

HSV-based color segmentation that strips solid-color library scanning
backgrounds from IIIF manuscript images. Uses Pillow + NumPy only (no OpenCV).

Used by both web (via API endpoint) and desktop (direct call). Same code, same results.

Key design decisions:
- Pillow HSV scale is 0-255 for ALL channels (not 0-360/0-100)
- Low-saturation backgrounds (gray/cream/white where S < 30 on 0-255 scale):
  use Value-channel-only distance instead of full HSV Euclidean, because
  hue is circular and noisy when saturation is near zero (Finding 5)
- MIN_FOREGROUND_RATIO defaults to 0.05 (5%) not 0.10 (10%) to handle
  small fragments with large scanning margins (Finding 6)
"""

import io
import numpy as np
from PIL import Image, ImageFilter
from typing import Tuple, Optional

DEFAULT_THRESHOLD = 30.0
CORNER_SAMPLE_SIZE = 20
MIN_FOREGROUND_RATIO = 0.05  # 5% -- small fragments on large backgrounds are valid
LOW_SATURATION_THRESHOLD = 30  # S < 30 (on 0-255 scale) = low saturation


def detect_background_color(hsv_array: np.ndarray) -> np.ndarray:
    """Sample corners of HSV image array to detect dominant background color.

    Returns median HSV values from all four corners as numpy array of shape (3,).
    All values in Pillow's 0-255 scale.
    """
    h, w = hsv_array.shape[:2]
    s = min(CORNER_SAMPLE_SIZE, h // 4, w // 4)  # safety for small images
    corners = [
        hsv_array[:s, :s],
        hsv_array[:s, w-s:],
        hsv_array[h-s:, :s],
        hsv_array[h-s:, w-s:],
    ]
    all_pixels = np.concatenate([c.reshape(-1, 3) for c in corners], axis=0)
    return np.median(all_pixels, axis=0)


HIGH_SATURATION_THRESHOLD = 100  # S > 100 = colored background (blue, green, etc.)

# CUL blue conservation mat HSV range (Pillow 0-255 scale)
# CUL images consistently use a blue mat; the exact shade varies but hue
# is always in the blue range with clearly saturated color.
CUL_BLUE_HUE_MIN = 135   # ~190 degrees
CUL_BLUE_HUE_MAX = 185   # ~261 degrees
CUL_BLUE_SAT_MIN = 60    # must be clearly colored (not gray/brown)


def _circular_hue_distance(h_array, h_bg):
    """Circular distance on PIL's 0-255 hue wheel (period=256)."""
    raw = np.abs(h_array.astype(float) - float(h_bg))
    return np.minimum(raw, 256.0 - raw)


def create_cul_blue_mask(hsv_array: np.ndarray) -> Image.Image:
    """Create mask targeting CUL blue conservation mat by hue range.

    Uses domain knowledge: CUL images have a distinctive blue mat.
    Any pixel with blue hue + sufficient saturation is marked as background.
    This is deterministic and doesn't depend on sampling.

    Returns: PIL Image mask (foreground=255, blue background=0).
    """
    h_chan = hsv_array[:, :, 0].astype(float)
    s_chan = hsv_array[:, :, 1].astype(float)
    is_blue = (
        (h_chan >= CUL_BLUE_HUE_MIN) &
        (h_chan <= CUL_BLUE_HUE_MAX) &
        (s_chan >= CUL_BLUE_SAT_MIN)
    )
    mask_array = np.where(is_blue, 0, 255).astype(np.uint8)
    mask_img = Image.fromarray(mask_array, mode='L')
    mask_img = mask_img.filter(ImageFilter.MinFilter(3))   # erode noise
    mask_img = mask_img.filter(ImageFilter.MaxFilter(5))   # dilate foreground
    return mask_img


def create_mask(hsv_array: np.ndarray, bg_color: np.ndarray,
                threshold: float) -> Image.Image:
    """Create binary foreground mask. Foreground=255, background=0.

    Three modes based on background saturation:

    1. Low saturation (S < 30): gray/cream/white backgrounds.
       Use Value-channel-only distance (hue is noisy at low S).

    2. High saturation (S > 100): colored backgrounds like CUL blue.
       Use hue-dominant distance: circular hue distance * 3 + S distance + V distance.
       The hue channel is the primary discriminant — parchment hue is far from blue
       but S and V values can be similar, so raw Euclidean fails.
       Threshold is interpreted as this weighted distance (typical range 30-120).

    3. Medium saturation (30-100): mixed. Use standard HSV Euclidean.

    All apply morphological cleanup: MinFilter(3) erode then MaxFilter(5) dilate.
    """
    bg_saturation = bg_color[1]  # S channel, 0-255 scale

    if bg_saturation < LOW_SATURATION_THRESHOLD:
        # Low saturation: hue is meaningless, use Value channel only
        diff = np.abs(hsv_array[:, :, 2].astype(float) - float(bg_color[2]))
    elif bg_saturation > HIGH_SATURATION_THRESHOLD:
        # High saturation (colored bg like CUL blue): hue-dominant distance
        h_dist = _circular_hue_distance(hsv_array[:, :, 0], bg_color[0])
        s_dist = np.abs(hsv_array[:, :, 1].astype(float) - float(bg_color[1]))
        v_dist = np.abs(hsv_array[:, :, 2].astype(float) - float(bg_color[2]))
        # Weight hue heavily — it's the key discriminant for colored backgrounds
        diff = h_dist * 3.0 + s_dist + v_dist
    else:
        # Medium saturation: standard HSV Euclidean distance
        diff = np.sqrt(np.sum((hsv_array.astype(float) - bg_color) ** 2, axis=2))

    mask_array = np.where(diff > threshold, 255, 0).astype(np.uint8)
    mask_img = Image.fromarray(mask_array, mode='L')
    mask_img = mask_img.filter(ImageFilter.MinFilter(3))   # erode noise
    mask_img = mask_img.filter(ImageFilter.MaxFilter(5))   # dilate foreground
    return mask_img


def remove_background(image_bytes: bytes,
                      threshold: float = DEFAULT_THRESHOLD,
                      min_foreground_ratio: float = MIN_FOREGROUND_RATIO,
                      is_cul: bool = False) -> bytes:
    """Remove solid-color background from image bytes.

    Args:
        image_bytes: Input image as bytes (JPEG, PNG, etc.)
        threshold: HSV color distance threshold (0-255 scale).
                   Higher = more aggressive removal. Default 30.0.
        min_foreground_ratio: Safety threshold -- if less than this fraction
                   of pixels are foreground, skip removal. Default 0.05 (5%).
        is_cul: If True, also remove CUL blue conservation mat by hue range.
                CUL images have a blue mat that may be surrounded by a gray
                border frame. The hue-range mask handles this reliably.

    Returns:
        RGBA PNG bytes with transparent background.
        If removal would eliminate too many pixels (foreground < min_foreground_ratio),
        returns original as RGBA PNG (safety fallback).
    """
    img = Image.open(io.BytesIO(image_bytes)).convert('RGB')
    hsv_img = img.convert('HSV')
    hsv_array = np.array(hsv_img)

    bg_color = detect_background_color(hsv_array)

    if is_cul:
        # CUL-specific: remove blue mat by hue range only.
        # Do NOT combine with corner-based border mask — CUL border frames are
        # often cream/beige colored, nearly identical to parchment in HSV space,
        # so any corner-based mask risks removing parchment. The thin border
        # frame is acceptable; the blue mat is what matters.
        mask = create_cul_blue_mask(hsv_array)
    else:
        mask = create_mask(hsv_array, bg_color, threshold)

    # Safety check
    mask_array = np.array(mask)
    foreground_ratio = np.count_nonzero(mask_array) / mask_array.size

    rgba = img.convert('RGBA')
    if foreground_ratio >= min_foreground_ratio:
        rgba.putalpha(mask)
    # else: keep full opacity (safety fallback)

    buf = io.BytesIO()
    rgba.save(buf, format='PNG', optimize=True)
    return buf.getvalue()
