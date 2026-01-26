#!/usr/bin/env python3
"""Generate Open Graph preview image for Dicta Genizah Search."""

from PIL import Image, ImageDraw, ImageFont
import os

# Image dimensions (standard OG size)
WIDTH = 1200
HEIGHT = 630

# Colors
BG_COLOR = (30, 41, 59)  # Dark slate
ACCENT_COLOR = (5, 150, 105)  # Green
TEXT_COLOR = (255, 255, 255)
SUBTITLE_COLOR = (200, 200, 200)
FEATURE_COLOR = (52, 211, 153)  # Light green

def create_og_image(
    title="Dicta Genizah Search",
    subtitle="Advanced Research Platform for Cairo Genizah Manuscripts",
    features="Full-Text Search  •  Manuscript Viewer  •  Parallel Detection",
    url="genizah.dicta.org.il",
    output_path="web/static/og-image.png"
):
    # Create image
    img = Image.new('RGB', (WIDTH, HEIGHT), BG_COLOR)
    draw = ImageDraw.Draw(img)

    # Draw accent bar at top
    draw.rectangle([0, 0, WIDTH, 8], fill=ACCENT_COLOR)

    # Try to load fonts (fallback to default if not available)
    try:
        font_title = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 72)
        font_subtitle = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 32)
        font_features = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 28)
        font_url = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 28)
    except:
        font_title = ImageFont.load_default()
        font_subtitle = font_title
        font_features = font_title
        font_url = font_title

    # Draw title (centered)
    title_bbox = draw.textbbox((0, 0), title, font=font_title)
    title_width = title_bbox[2] - title_bbox[0]
    title_x = (WIDTH - title_width) // 2
    draw.text((title_x, 180), title, font=font_title, fill=TEXT_COLOR)

    # Draw subtitle (centered)
    sub_bbox = draw.textbbox((0, 0), subtitle, font=font_subtitle)
    sub_width = sub_bbox[2] - sub_bbox[0]
    sub_x = (WIDTH - sub_width) // 2
    draw.text((sub_x, 280), subtitle, font=font_subtitle, fill=SUBTITLE_COLOR)

    # Draw features (centered)
    feat_bbox = draw.textbbox((0, 0), features, font=font_features)
    feat_width = feat_bbox[2] - feat_bbox[0]
    feat_x = (WIDTH - feat_width) // 2
    draw.text((feat_x, 340), features, font=font_features, fill=FEATURE_COLOR)

    # Draw URL button at bottom
    url_bbox = draw.textbbox((0, 0), url, font=font_url)
    url_width = url_bbox[2] - url_bbox[0]
    url_height = url_bbox[3] - url_bbox[1]

    btn_padding = 20
    btn_width = url_width + btn_padding * 2
    btn_height = url_height + btn_padding * 2
    btn_x = (WIDTH - btn_width) // 2
    btn_y = 480

    # Draw rounded rectangle for button
    draw.rounded_rectangle(
        [btn_x, btn_y, btn_x + btn_width, btn_y + btn_height],
        radius=10,
        fill=ACCENT_COLOR
    )

    # Draw URL text
    url_x = btn_x + btn_padding
    url_y = btn_y + btn_padding - 5
    draw.text((url_x, url_y), url, font=font_url, fill=TEXT_COLOR)

    # Save
    script_dir = os.path.dirname(os.path.abspath(__file__))
    full_path = os.path.join(script_dir, output_path)
    img.save(full_path, 'PNG')
    print(f"Created: {full_path}")
    return full_path


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        # Custom title from command line
        create_og_image(title=sys.argv[1])
    else:
        create_og_image()
