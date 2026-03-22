# -*- coding: utf-8 -*-
"""
Test background removal on ~30 real CUL fragment images.

Fetches IIIF images, runs the algorithm, and produces a visual HTML report
showing original vs processed for each sample. Also dumps HSV statistics
to help tune the CUL blue hue range.

Usage:
    python scripts/test_bg_removal_samples.py [--fetch-only] [--report-only]
"""

import io
import json
import os
import sys
import base64
import argparse
from pathlib import Path

import numpy as np
from PIL import Image

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.background_removal import (
    remove_background, detect_background_color, create_blue_mat_mask,
    create_cul_blue_mask,  # legacy alias
    create_mask, BLUE_MAT_HUE_MIN, BLUE_MAT_HUE_MAX, BLUE_MAT_SAT_MIN,
    CUL_BLUE_HUE_MIN, CUL_BLUE_HUE_MAX, CUL_BLUE_SAT_MIN,  # legacy aliases
    DEFAULT_THRESHOLD,
)
from shared.puzzle_image_service import PuzzleImageService

# ── Sample CUL sys_ids to test ──
# Mix of T-S NS, T-S, OR., ADD. — varied shelfmarks and conditions
SAMPLES = [
    # (sys_id, shelfmark, description)
    ('990051422390205171', 'T-S NS 43.1', 'NS large fragment'),
    ('990051422300205171', 'T-S NS 43.2', 'NS large fragment'),
    ('990051422480205171', 'T-S NS 43.7', 'NS medium fragment'),
    ('990051422570205171', 'T-S NS 43.16', 'NS medium fragment'),
    ('990051422660205171', 'T-S NS 43.25', 'NS medium fragment'),
    ('990051422840205171', 'T-S NS 43.43', 'NS medium fragment'),
    ('990051423010205171', 'T-S NS 43.60', 'NS medium fragment'),
    ('990053291030205171', 'T-S NS J401n', 'NS J series'),
    ('990012254980205171', 'T-S 12.1', 'Classic T-S numbering'),
    ('990012254990205171', 'T-S 12.2', 'Classic T-S numbering'),
    ('990012255000205171', 'T-S 12.3', 'Classic T-S numbering'),
    ('990012252910205171', 'T-S 10.1', 'Classic T-S'),
    ('990012252920205171', 'T-S 10.2', 'Classic T-S'),
    ('990012252930205171', 'T-S 10.3', 'Classic T-S'),
    ('990012253920205171', 'T-S 16.1', 'Classic T-S'),
    ('990012253930205171', 'T-S 16.2', 'Classic T-S'),
    ('990012253940205171', 'T-S 16.3', 'Classic T-S'),
    ('990012254010205171', 'T-S 20.1', 'Classic T-S'),
    ('990012254020205171', 'T-S 20.2', 'Classic T-S'),
    ('990012254030205171', 'T-S 20.3', 'Classic T-S'),
    ('990051420470205171', 'T-S NS 1.1', 'NS small'),
    ('990051420480205171', 'T-S NS 1.2', 'NS small'),
    ('990051420490205171', 'T-S NS 1.3', 'NS small'),
    ('990051420590205171', 'T-S NS 2.1', 'NS 2 series'),
    ('990051420600205171', 'T-S NS 2.2', 'NS 2 series'),
    ('990027244850205171', 'T-S Ar.30.1', 'Arabic series'),
    ('990027244860205171', 'T-S Ar.30.2', 'Arabic series'),
    ('990012257240205171', 'T-S 8J1.1', 'J series'),
    ('990012257250205171', 'T-S 8J1.2', 'J series'),
    ('990012257260205171', 'T-S 8J1.3', 'J series'),
]

OUTPUT_DIR = Path(__file__).parent.parent / 'scripts' / 'bg_removal_test'


def fetch_fl_ids(sys_id: str) -> list:
    """Resolve sys_id -> FL IDs via NLI IIIF manifest.

    Uses the correct PNX_MANUSCRIPTS DOCID format, with MARC API fallback.
    """
    import re
    import requests

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }

    # Primary: IIIF manifest
    manifest_url = f"https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{sys_id}-1/manifest"
    try:
        resp = requests.get(manifest_url, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            fl_ids = []
            for seq in data.get('sequences', []):
                for canvas in seq.get('canvases', []):
                    for img in canvas.get('images', []):
                        resource = img.get('resource', {})
                        service = resource.get('service', {})
                        sid = service.get('@id', '')
                        fl_match = re.search(r'FL(\d+)', sid)
                        if fl_match:
                            fl_ids.append(fl_match.group(1))
            if fl_ids:
                return fl_ids
    except Exception as e:
        print(f"  IIIF manifest failed for {sys_id}: {e}")

    # Fallback: MARC API
    try:
        marc_url = f"https://iiif.nli.org.il/IIIFv21/marc/bib/{sys_id}"
        resp = requests.get(marc_url, headers=headers, timeout=10)
        if resp.status_code == 200:
            fl_ids = list(dict.fromkeys(re.findall(r'FL(\d+)', resp.text)))
            if fl_ids:
                return fl_ids
    except Exception as e:
        print(f"  MARC fallback failed for {sys_id}: {e}")

    return []


def fetch_image(fl_id: str, size: int = 800) -> bytes | None:
    """Fetch IIIF image."""
    import re
    import requests

    digits = re.sub(r"\D", "", str(fl_id))
    url = f"https://iiif.nli.org.il/IIIFv21/FL{digits}/full/{size},/0/default.jpg"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://www.nli.org.il/',
    }
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.content
    except Exception as e:
        print(f"  Fetch failed for FL{fl_id}: {e}")
        return None


def analyze_hsv(image_bytes: bytes) -> dict:
    """Analyze HSV distribution of an image."""
    img = Image.open(io.BytesIO(image_bytes)).convert('RGB')
    hsv = np.array(img.convert('HSV'))

    # Corner analysis
    h, w = hsv.shape[:2]
    s = min(20, h // 4, w // 4)
    corners = np.concatenate([
        hsv[:s, :s].reshape(-1, 3),
        hsv[:s, w-s:].reshape(-1, 3),
        hsv[h-s:, :s].reshape(-1, 3),
        hsv[h-s:, w-s:].reshape(-1, 3),
    ], axis=0)
    corner_median = np.median(corners, axis=0)

    # Blue pixel analysis: count pixels in CUL blue range
    h_chan = hsv[:, :, 0].astype(float)
    s_chan = hsv[:, :, 1].astype(float)
    is_blue = (h_chan >= CUL_BLUE_HUE_MIN) & (h_chan <= CUL_BLUE_HUE_MAX) & (s_chan >= CUL_BLUE_SAT_MIN)
    blue_ratio = np.sum(is_blue) / is_blue.size

    # Blue pixel HSV stats
    blue_pixels = hsv[is_blue]
    if len(blue_pixels) > 0:
        blue_h_range = (float(np.min(blue_pixels[:, 0])), float(np.max(blue_pixels[:, 0])))
        blue_s_range = (float(np.min(blue_pixels[:, 1])), float(np.max(blue_pixels[:, 1])))
        blue_v_range = (float(np.min(blue_pixels[:, 2])), float(np.max(blue_pixels[:, 2])))
        blue_h_median = float(np.median(blue_pixels[:, 0]))
        blue_s_median = float(np.median(blue_pixels[:, 1]))
    else:
        blue_h_range = blue_s_range = blue_v_range = (0, 0)
        blue_h_median = blue_s_median = 0

    # Overall hue histogram (for debugging)
    h_hist, _ = np.histogram(hsv[:, :, 0].ravel(), bins=32, range=(0, 256))

    return {
        'corner_hsv': corner_median.tolist(),
        'corner_h': float(corner_median[0]),
        'corner_s': float(corner_median[1]),
        'corner_v': float(corner_median[2]),
        'blue_ratio': float(blue_ratio),
        'blue_h_range': blue_h_range,
        'blue_s_range': blue_s_range,
        'blue_v_range': blue_v_range,
        'blue_h_median': blue_h_median,
        'blue_s_median': blue_s_median,
        'h_histogram': h_hist.tolist(),
        'image_size': (w, h),
    }


def process_sample(image_bytes: bytes, threshold: float = 30.0) -> dict:
    """Run background removal (is_cul=True) and return results."""
    # Run with is_cul=True
    result_cul = remove_background(image_bytes, threshold=threshold, is_cul=True)
    result_img = Image.open(io.BytesIO(result_cul))

    # Count transparent vs opaque
    alpha = np.array(result_img)[:, :, 3]
    transparent_ratio = np.sum(alpha == 0) / alpha.size
    opaque_ratio = np.sum(alpha == 255) / alpha.size

    # Also run without is_cul for comparison
    result_plain = remove_background(image_bytes, threshold=threshold, is_cul=False)
    plain_img = Image.open(io.BytesIO(result_plain))
    plain_alpha = np.array(plain_img)[:, :, 3]
    plain_transparent = np.sum(plain_alpha == 0) / plain_alpha.size

    return {
        'cul_result': result_cul,
        'plain_result': result_plain,
        'cul_transparent_ratio': float(transparent_ratio),
        'cul_opaque_ratio': float(opaque_ratio),
        'plain_transparent_ratio': float(plain_transparent),
    }


def img_to_data_uri(image_bytes: bytes, fmt: str = 'image/png') -> str:
    """Convert image bytes to data URI for HTML embedding."""
    b64 = base64.b64encode(image_bytes).decode()
    return f"data:{fmt};base64,{b64}"


def generate_report(results: list) -> str:
    """Generate HTML report from results."""
    html = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>CUL Background Removal Test Report</title>
<style>
body { font-family: Arial, sans-serif; background: #1a1a1a; color: #eee; margin: 20px; }
h1 { color: #4fc3f7; }
h2 { color: #81c784; margin-top: 40px; }
.sample { border: 1px solid #444; margin: 20px 0; padding: 15px; border-radius: 8px; background: #2a2a2a; }
.images { display: flex; gap: 10px; flex-wrap: wrap; }
.images img { max-width: 300px; max-height: 400px; border: 1px solid #555; background: #3a3a3a; }
.label { font-size: 12px; color: #aaa; text-align: center; }
.stats { font-size: 13px; color: #ccc; margin: 10px 0; }
.stats td { padding: 2px 10px; }
.pass { color: #81c784; font-weight: bold; }
.fail { color: #e57373; font-weight: bold; }
.warn { color: #ffb74d; font-weight: bold; }
table.summary { border-collapse: collapse; margin: 20px 0; }
table.summary th, table.summary td { border: 1px solid #555; padding: 6px 12px; text-align: left; }
table.summary th { background: #333; }
</style>
</head>
<body>
<h1>CUL Background Removal Test Report</h1>
"""

    # Summary table
    pass_count = sum(1 for r in results if r.get('verdict') == 'PASS')
    fail_count = sum(1 for r in results if r.get('verdict') == 'FAIL')
    warn_count = sum(1 for r in results if r.get('verdict') == 'WARN')
    total = len(results)

    html += f"""
<h2>Summary: {pass_count}/{total} PASS, {fail_count} FAIL, {warn_count} WARN</h2>
<table class="summary">
<tr><th>#</th><th>Shelfmark</th><th>Folio</th><th>Blue %</th><th>CUL Transparent %</th><th>Plain Transparent %</th><th>Verdict</th></tr>
"""
    for i, r in enumerate(results):
        v_class = r.get('verdict', 'WARN').lower()
        html += f"""<tr>
<td>{i+1}</td>
<td>{r['shelfmark']}</td>
<td>{r['folio']}</td>
<td>{r['hsv']['blue_ratio']*100:.1f}%</td>
<td>{r['process']['cul_transparent_ratio']*100:.1f}%</td>
<td>{r['process']['plain_transparent_ratio']*100:.1f}%</td>
<td class="{v_class}">{r['verdict']}</td>
</tr>"""
    html += "</table>"

    # Individual samples
    for i, r in enumerate(results):
        v_class = r.get('verdict', 'WARN').lower()
        html += f"""
<div class="sample">
<h2>{i+1}. {r['shelfmark']} — {r['folio']} <span class="{v_class}">[{r['verdict']}]</span></h2>
<table class="stats">
<tr><td>Corner HSV:</td><td>H={r['hsv']['corner_h']:.0f} S={r['hsv']['corner_s']:.0f} V={r['hsv']['corner_v']:.0f}</td></tr>
<tr><td>Blue pixel ratio:</td><td>{r['hsv']['blue_ratio']*100:.1f}%</td></tr>
<tr><td>Blue H range:</td><td>{r['hsv']['blue_h_range'][0]:.0f} - {r['hsv']['blue_h_range'][1]:.0f} (median {r['hsv']['blue_h_median']:.0f})</td></tr>
<tr><td>Blue S range:</td><td>{r['hsv']['blue_s_range'][0]:.0f} - {r['hsv']['blue_s_range'][1]:.0f} (median {r['hsv']['blue_s_median']:.0f})</td></tr>
<tr><td>CUL transparent:</td><td>{r['process']['cul_transparent_ratio']*100:.1f}%</td></tr>
<tr><td>Plain transparent:</td><td>{r['process']['plain_transparent_ratio']*100:.1f}%</td></tr>
</table>
<div class="images">
<div><div class="label">Original</div><img src="{r['original_uri']}"></div>
<div><div class="label">is_cul=True</div><img src="{r['cul_uri']}"></div>
<div><div class="label">is_cul=False (plain)</div><img src="{r['plain_uri']}"></div>
</div>
</div>
"""

    html += """
</body>
</html>"""
    return html


def main():
    parser = argparse.ArgumentParser(description='Test bg removal on CUL samples')
    parser.add_argument('--fetch-only', action='store_true', help='Only fetch images, skip processing')
    parser.add_argument('--report-only', action='store_true', help='Only regenerate report from cached data')
    parser.add_argument('--threshold', type=float, default=30.0, help='BG removal threshold')
    parser.add_argument('--max-folios', type=int, default=2, help='Max folios per manuscript (recto+verso)')
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    cache_dir = OUTPUT_DIR / 'cache'
    cache_dir.mkdir(exist_ok=True)
    data_file = OUTPUT_DIR / 'sample_data.json'

    if not args.report_only:
        print(f"Testing {len(SAMPLES)} CUL manuscripts (up to {args.max_folios} folios each)")
        print(f"Threshold: {args.threshold}")
        print(f"Output: {OUTPUT_DIR}")
        print()

        sample_data = []
        for sys_id, shelfmark, desc in SAMPLES:
            print(f"Processing {shelfmark} ({desc})...")

            # Check if we already have cached images
            cached_fl_file = cache_dir / f"{sys_id}_fl_ids.json"
            if cached_fl_file.exists():
                fl_ids = json.loads(cached_fl_file.read_text())
                print(f"  Using cached FL IDs: {fl_ids[:args.max_folios]}")
            else:
                fl_ids = fetch_fl_ids(sys_id)
                if fl_ids:
                    cached_fl_file.write_text(json.dumps(fl_ids))
                print(f"  Found {len(fl_ids)} FL IDs")

            if not fl_ids:
                print(f"  SKIP: No FL IDs found")
                continue

            folio_labels = ['recto', 'verso', '2r', '2v', '3r', '3v']
            for idx, fl_id in enumerate(fl_ids[:args.max_folios]):
                folio = folio_labels[idx] if idx < len(folio_labels) else f"f{idx+1}"
                print(f"  Folio {folio} (FL{fl_id})...", end=' ')

                # Fetch or use cached image
                img_cache = cache_dir / f"FL{fl_id}_800.jpg"
                if img_cache.exists():
                    image_bytes = img_cache.read_bytes()
                    print("(cached)", end=' ')
                else:
                    if args.fetch_only:
                        image_bytes = fetch_image(fl_id, size=800)
                        if image_bytes:
                            img_cache.write_bytes(image_bytes)
                            print("fetched", end=' ')
                        else:
                            print("FAILED")
                            continue
                    else:
                        image_bytes = fetch_image(fl_id, size=800)
                        if image_bytes:
                            img_cache.write_bytes(image_bytes)
                        else:
                            print("FAILED")
                            continue

                if args.fetch_only:
                    print()
                    sample_data.append({
                        'sys_id': sys_id, 'shelfmark': shelfmark, 'desc': desc,
                        'fl_id': fl_id, 'folio': folio,
                    })
                    continue

                # Analyze HSV
                hsv_stats = analyze_hsv(image_bytes)

                # Process with both modes
                proc = process_sample(image_bytes, threshold=args.threshold)

                # Determine verdict:
                # PASS = blue removed, parchment kept (cul_transparent > 20% && opaque > 10%)
                # FAIL = parchment removed or blue kept
                # WARN = marginal
                cul_trans = proc['cul_transparent_ratio']
                cul_opaq = proc['cul_opaque_ratio']
                blue_ratio = hsv_stats['blue_ratio']

                if blue_ratio < 0.05:
                    # No significant blue — verdict based on overall quality
                    verdict = 'PASS' if cul_opaq > 0.3 else 'WARN'
                elif cul_trans > 0.15 and cul_opaq > 0.10:
                    verdict = 'PASS'
                elif cul_opaq < 0.05:
                    verdict = 'FAIL'  # everything removed
                else:
                    verdict = 'WARN'

                print(f"blue={blue_ratio*100:.0f}% trans={cul_trans*100:.0f}% opaq={cul_opaq*100:.0f}% [{verdict}]")

                sample_data.append({
                    'sys_id': sys_id, 'shelfmark': shelfmark, 'desc': desc,
                    'fl_id': fl_id, 'folio': folio,
                    'hsv': hsv_stats,
                    'process': {k: v for k, v in proc.items() if k not in ('cul_result', 'plain_result')},
                    'verdict': verdict,
                    'threshold': args.threshold,
                })

                # Save processed images
                (cache_dir / f"FL{fl_id}_cul.png").write_bytes(proc['cul_result'])
                (cache_dir / f"FL{fl_id}_plain.png").write_bytes(proc['plain_result'])

        # Save data
        data_file.write_text(json.dumps(sample_data, indent=2))
        print(f"\nData saved to {data_file}")

    # Generate report
    print("Generating HTML report...")
    if data_file.exists():
        sample_data = json.loads(data_file.read_text())
    else:
        print("No data file found. Run without --report-only first.")
        return

    # Build report entries with embedded images
    report_entries = []
    for entry in sample_data:
        if 'hsv' not in entry:
            continue

        fl_id = entry['fl_id']
        img_cache = cache_dir / f"FL{fl_id}_800.jpg"
        cul_cache = cache_dir / f"FL{fl_id}_cul.png"
        plain_cache = cache_dir / f"FL{fl_id}_plain.png"

        if not all(f.exists() for f in [img_cache, cul_cache, plain_cache]):
            continue

        entry['original_uri'] = img_to_data_uri(img_cache.read_bytes(), 'image/jpeg')
        entry['cul_uri'] = img_to_data_uri(cul_cache.read_bytes(), 'image/png')
        entry['plain_uri'] = img_to_data_uri(plain_cache.read_bytes(), 'image/png')
        report_entries.append(entry)

    html = generate_report(report_entries)
    report_path = OUTPUT_DIR / 'bg_removal_report.html'
    report_path.write_text(html, encoding='utf-8')
    print(f"Report: {report_path}")

    # Print summary
    verdicts = [r['verdict'] for r in report_entries]
    print(f"\nResults: {verdicts.count('PASS')} PASS, {verdicts.count('FAIL')} FAIL, {verdicts.count('WARN')} WARN / {len(verdicts)} total")

    # Print HSV statistics for blue pixels
    print("\n-- Blue Pixel HSV Statistics --")
    all_blue_h = []
    all_blue_s = []
    for r in report_entries:
        if r['hsv']['blue_ratio'] > 0.05:
            all_blue_h.append(r['hsv']['blue_h_median'])
            all_blue_s.append(r['hsv']['blue_s_median'])
            print(f"  {r['shelfmark']:20s} {r['folio']:6s}  blue={r['hsv']['blue_ratio']*100:4.1f}%  "
                  f"H={r['hsv']['blue_h_range'][0]:.0f}-{r['hsv']['blue_h_range'][1]:.0f} (med {r['hsv']['blue_h_median']:.0f})  "
                  f"S={r['hsv']['blue_s_range'][0]:.0f}-{r['hsv']['blue_s_range'][1]:.0f} (med {r['hsv']['blue_s_median']:.0f})")

    if all_blue_h:
        print(f"\n  Overall blue H median range: {min(all_blue_h):.0f} - {max(all_blue_h):.0f}")
        print(f"  Overall blue S median range: {min(all_blue_s):.0f} - {max(all_blue_s):.0f}")
        print(f"  Current config: H={CUL_BLUE_HUE_MIN}-{CUL_BLUE_HUE_MAX}, S>={CUL_BLUE_SAT_MIN}")


if __name__ == '__main__':
    main()
