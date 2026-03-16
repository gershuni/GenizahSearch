# -*- coding: utf-8 -*-
"""
Composite Export and Thumbnail Service for Fragment Puzzle.

Provides functions to:
- Generate auto-suggested titles from fragment shelfmarks
- Compose full-resolution RGBA composite images from positioned fragments
- Generate small base64-encoded PNG thumbnails for document listings

Coordinate system (matching both Fabric.js and PyQt):
- x/y = top-left corner of the unrotated image in canvas space (pixels)
- scale = display scale factor applied to the ~800px canvas image
- Rotation pivots around the center of the (scaled, flipped) image
- PIL.rotate is counter-clockwise, so we negate the angle (apps use clockwise-positive)

Export strategy:
- A single global resolution multiplier scales ALL positions uniformly
- Each fragment image is resized to its correct visual size at that resolution
- This avoids per-fragment coord_scale drift when source images vary in size
"""

import base64
import io
import logging
import math
from typing import List, Optional

from PIL import Image

from shared.puzzle_model import PuzzleFragment

logger = logging.getLogger(__name__)

# Canvas images are loaded at approximately this width for interaction
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

    Uses a single global resolution multiplier so all fragment positions
    and sizes scale uniformly, regardless of per-image source dimensions.

    Args:
        fragments: List of positioned PuzzleFragment objects.
        image_service: PuzzleImageService instance with resolve_fragment_image().
        export_size: Width hint for fetching high-res images (default 3000).
        margin: Padding in pixels around the auto-cropped result (default 20).

    Returns:
        PIL RGBA Image with transparent background, or None if no fragments rendered.
    """
    if not fragments:
        return None

    # Global resolution multiplier: scales canvas coords to export coords
    global_scale = export_size / float(CANVAS_IMAGE_WIDTH)

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

        # Per-fragment image scale: ratio of actual loaded width to canvas width
        img_scale = export_img.width / float(CANVAS_IMAGE_WIDTH)

        # Apply crop (crop values are in ~800px canvas-pixel space)
        ct = int(frag.crop_top * img_scale)
        cb = int(frag.crop_bottom * img_scale)
        cl = int(frag.crop_left * img_scale)
        cr = int(frag.crop_right * img_scale)
        if ct + cb + cl + cr > 0:
            new_left = cl
            new_top = ct
            new_right = max(export_img.width - cr, new_left + 1)
            new_bottom = max(export_img.height - cb, new_top + 1)
            export_img = export_img.crop((new_left, new_top, new_right, new_bottom))

        # Target visual size in export space:
        # On canvas, the image (after crop) is displayed at approximately:
        #   canvas_visual_w = (cropped_canvas_w) * frag.scale
        # In export space, we want:
        #   export_visual_w = canvas_visual_w * global_scale
        # = (cropped_canvas_w) * frag.scale * global_scale
        #
        # cropped_canvas_w = export_img.width / img_scale  (map back to canvas space)
        cropped_canvas_w = export_img.width / img_scale
        cropped_canvas_h = export_img.height / img_scale
        target_w = max(1, int(cropped_canvas_w * frag.scale * global_scale))
        target_h = max(1, int(cropped_canvas_h * frag.scale * global_scale))
        scaled_img = export_img.resize((target_w, target_h), Image.LANCZOS)

        # Apply flips
        if frag.flip_h:
            scaled_img = scaled_img.transpose(Image.FLIP_LEFT_RIGHT)
        if frag.flip_v:
            scaled_img = scaled_img.transpose(Image.FLIP_TOP_BOTTOM)

        # Position: frag.x/y is the local origin (top-left of UNSCALED image) in canvas space.
        # Both PyQt and Fabric.js apply scale/rotation around the image CENTER, so the
        # center position = pos + (unscaled_size / 2), regardless of scale.
        # We compute center in export space, then place the scaled image centered on it.
        center_x = (frag.x + cropped_canvas_w / 2.0) * global_scale
        center_y = (frag.y + cropped_canvas_h / 2.0) * global_scale

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
