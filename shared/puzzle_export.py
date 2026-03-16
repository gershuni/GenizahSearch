# -*- coding: utf-8 -*-
"""
Composite Export and Thumbnail Service for Fragment Puzzle.

Provides functions to:
- Generate auto-suggested titles from fragment shelfmarks
- Compose full-resolution RGBA composite images from positioned fragments
- Generate small base64-encoded PNG thumbnails for document listings

Coordinate system (matching both Fabric.js and PyQt):
- x/y = top-left corner of the unrotated image in 800px canvas space
- Rotation pivots around the center of the (scaled, flipped) image
- PIL.rotate is counter-clockwise, so we negate the angle (apps use clockwise-positive)
"""

import base64
import io
import logging
import math
from typing import List, Optional

from PIL import Image

from shared.puzzle_model import PuzzleFragment

logger = logging.getLogger(__name__)

# Canvas images are loaded at this width for interaction
CANVAS_IMAGE_WIDTH = 800


def auto_suggest_title(fragments: List[PuzzleFragment]) -> str:
    """Generate a title from unique fragment shelfmarks joined by ' + '.

    Args:
        fragments: List of PuzzleFragment objects.

    Returns:
        Title string like 'T-S 12.1 + T-S 13.5', or 'Untitled Join' if no shelfmarks.
    """
    seen = []
    for f in fragments:
        if f.shelfmark and f.shelfmark not in seen:
            seen.append(f.shelfmark)
    return ' + '.join(seen) if seen else 'Untitled Join'


def compose_puzzle_export(fragments: List[PuzzleFragment],
                          image_service,
                          export_size: int = 3000,
                          margin: int = 20) -> Optional[Image.Image]:
    """Compose full-resolution RGBA PNG from positioned fragments.

    Uses top-left origin with centered rotation pivot, matching both
    Fabric.js and PyQt canvas behavior.

    Args:
        fragments: List of positioned PuzzleFragment objects.
        image_service: PuzzleImageService instance with resolve_fragment_image().
        export_size: Width in pixels for fetching high-res images (default 3000).
        margin: Padding in pixels around the auto-cropped result (default 20).

    Returns:
        PIL RGBA Image with transparent background, or None if no fragments rendered.
    """
    if not fragments:
        return None

    # Phase 1: Transform each fragment and compute placement coordinates
    placed = []  # list of (paste_x, paste_y, rotated_img)

    for frag in fragments:
        # Fetch high-res image
        img_bytes = image_service.resolve_fragment_image(
            frag.fl_id, size=export_size,
            threshold=frag.bg_removal_threshold,
            processed=frag.processed
        )
        if img_bytes is None:
            logger.warning("compose_puzzle_export: no image for fl_id=%s, skipping", frag.fl_id)
            continue

        try:
            export_img = Image.open(io.BytesIO(img_bytes)).convert('RGBA')
        except Exception as e:
            logger.warning("compose_puzzle_export: failed to open image for fl_id=%s: %s", frag.fl_id, e)
            continue

        # Coordinate scale: map 800px canvas coords to export-size coords
        coord_scale = export_img.width / float(CANVAS_IMAGE_WIDTH)

        # Apply crop (crop values are in 800px canvas-pixel space)
        crop_scale = coord_scale
        ct = int(frag.crop_top * crop_scale)
        cb = int(frag.crop_bottom * crop_scale)
        cl = int(frag.crop_left * crop_scale)
        cr = int(frag.crop_right * crop_scale)
        if ct + cb + cl + cr > 0:
            new_left = cl
            new_top = ct
            new_right = max(export_img.width - cr, new_left + 1)
            new_bottom = max(export_img.height - cb, new_top + 1)
            export_img = export_img.crop((new_left, new_top, new_right, new_bottom))

        # Apply scale
        new_w = max(1, int(export_img.width * frag.scale))
        new_h = max(1, int(export_img.height * frag.scale))
        scaled_img = export_img.resize((new_w, new_h), Image.LANCZOS)

        # Apply flips
        if frag.flip_h:
            scaled_img = scaled_img.transpose(Image.FLIP_LEFT_RIGHT)
        if frag.flip_v:
            scaled_img = scaled_img.transpose(Image.FLIP_TOP_BOTTOM)

        # Compute center of the scaled/flipped (unrotated) image in export space
        tl_x = frag.x * coord_scale
        tl_y = frag.y * coord_scale
        half_w = scaled_img.width / 2.0
        half_h = scaled_img.height / 2.0
        center_x = tl_x + half_w
        center_y = tl_y + half_h

        # Apply rotation (negate angle: apps use clockwise-positive, PIL uses CCW)
        if abs(frag.rotation) > 0.01:
            rotated_img = scaled_img.rotate(
                -frag.rotation, expand=True,
                resample=Image.BICUBIC, fillcolor=(0, 0, 0, 0)
            )
        else:
            rotated_img = scaled_img

        # Place rotated image so its center aligns with computed center
        paste_x = center_x - rotated_img.width / 2.0
        paste_y = center_y - rotated_img.height / 2.0

        placed.append((paste_x, paste_y, rotated_img))

    if not placed:
        return None

    # Phase 2: Compute bounding box
    min_x = min(p[0] for p in placed)
    min_y = min(p[1] for p in placed)
    max_x = max(p[0] + p[2].width for p in placed)
    max_y = max(p[1] + p[2].height for p in placed)

    # Canvas size
    canvas_w = int(math.ceil(max_x - min_x))
    canvas_h = int(math.ceil(max_y - min_y))
    if canvas_w <= 0 or canvas_h <= 0:
        return None

    canvas = Image.new('RGBA', (canvas_w, canvas_h), (0, 0, 0, 0))

    # Phase 3: Paste fragments using alpha composite
    for paste_x, paste_y, img in placed:
        x = int(round(paste_x - min_x))
        y = int(round(paste_y - min_y))
        canvas.alpha_composite(img, dest=(x, y))

    # Phase 4: Auto-crop to content + margin
    bbox = canvas.getbbox()
    if bbox is None:
        return canvas

    left, top, right, bottom = bbox
    left = max(0, left - margin)
    top = max(0, top - margin)
    right = min(canvas_w, right + margin)
    bottom = min(canvas_h, bottom + margin)
    canvas = canvas.crop((left, top, right, bottom))

    return canvas


def generate_thumbnail(fragments: List[PuzzleFragment],
                       image_service,
                       thumb_size: int = 150) -> str:
    """Generate a small composite thumbnail as base64-encoded PNG.

    Args:
        fragments: List of positioned PuzzleFragment objects.
        image_service: PuzzleImageService instance.
        thumb_size: Maximum dimension in pixels (default 150).

    Returns:
        Base64-encoded PNG string (no data URI prefix), or empty string on failure.
    """
    composite = compose_puzzle_export(fragments, image_service, export_size=400, margin=5)
    if composite is None:
        return ''

    # Resize to fit within thumb_size x thumb_size maintaining aspect ratio
    composite.thumbnail((thumb_size, thumb_size), Image.LANCZOS)

    buf = io.BytesIO()
    composite.save(buf, format='PNG', optimize=True)
    return base64.b64encode(buf.getvalue()).decode('ascii')
