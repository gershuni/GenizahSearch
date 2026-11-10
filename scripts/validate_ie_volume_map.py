"""
Validate ie_volume_map.json against live NLI IIIF manifests.

Stratified sampling:
- All heuristic edge cases (if any flagged)
- Separate strata: 2-volume, 3-volume, 4+ volume manuscripts
- Cases with large page-count gaps between volumes

Sample size: ~200-300 manuscripts (configurable via --sample)

Output: Structured report with pass/fail per manuscript + summary

Usage:
  python scripts/validate_ie_volume_map.py                    # Full validation (~250 samples)
  python scripts/validate_ie_volume_map.py --sample 50        # Quick check
  python scripts/validate_ie_volume_map.py --output report.json  # Save JSON report
  python scripts/validate_ie_volume_map.py --delay 1.0        # Custom delay between requests
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package required. Install with: pip install requests")
    sys.exit(1)

SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
VOLUME_MAP_FILE = PROJECT_DIR / "ie_volume_map.json"

NLI_IIIF_BASE = "https://iiif.nli.org.il/IIIFv21"


def load_volume_map(path=None):
    """Load ie_volume_map.json from project root."""
    map_path = Path(path) if path else VOLUME_MAP_FILE
    if not map_path.exists():
        print(f"ERROR: Volume map not found at {map_path}")
        sys.exit(1)
    with open(map_path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_stratified_sample(volume_map, target_size=250):
    """Build a stratified sample of manuscripts for validation.

    Strata:
    - 2_volume: manuscripts with exactly 2 volumes (most common)
    - 3_volume: manuscripts with exactly 3 volumes
    - 4plus_volume: manuscripts with 4+ volumes (rare, all included)
    - large_gap: page_count ratio > 5:1 between largest/smallest volume

    Returns list of sys_ids and strata counts dict.
    """
    strata = {
        '2_volume': [],
        '3_volume': [],
        '4plus_volume': [],
        'large_gap': [],
    }

    for sys_id, entry in volume_map.items():
        vols = entry.get('volumes', [])
        n = len(vols)
        if n < 2:
            continue

        counts = [v.get('page_count', 0) or 0 for v in vols]
        min_count = max(min(counts), 1)  # avoid division by zero
        max_count = max(counts)

        # Check for large page-count gap
        if max_count / min_count > 5:
            strata['large_gap'].append(sys_id)

        # Classify by volume count
        if n == 2:
            strata['2_volume'].append(sys_id)
        elif n == 3:
            strata['3_volume'].append(sys_id)
        else:
            strata['4plus_volume'].append(sys_id)

    # Build sample: take ALL rare strata, fill from common proportionally
    sample = set()

    # Always include all 4+ volume (rare)
    sample.update(strata['4plus_volume'])

    # Always include all large_gap (interesting edge cases)
    sample.update(strata['large_gap'])

    remaining = target_size - len(sample)
    if remaining <= 0:
        return list(sample), {k: len(v) for k, v in strata.items()}

    # Distribute remaining proportionally between 2-vol and 3-vol
    total_2_3 = len(strata['2_volume']) + len(strata['3_volume'])
    if total_2_3 > 0:
        ratio_2 = len(strata['2_volume']) / total_2_3
        n_2 = min(int(remaining * ratio_2), len(strata['2_volume']))
        n_3 = min(remaining - n_2, len(strata['3_volume']))

        # If we couldn't fill n_3, give remainder back to n_2
        if n_3 < remaining - n_2:
            n_2 = min(n_2 + (remaining - n_2 - n_3), len(strata['2_volume']))

        if n_2 > 0:
            # Exclude items already in sample (from large_gap overlap)
            available_2 = [s for s in strata['2_volume'] if s not in sample]
            sample.update(random.sample(available_2, min(n_2, len(available_2))))

        if n_3 > 0:
            available_3 = [s for s in strata['3_volume'] if s not in sample]
            sample.update(random.sample(available_3, min(n_3, len(available_3))))

    strata_counts = {k: len(v) for k, v in strata.items()}
    return list(sample), strata_counts


def classify_sys_id(volume_map, sys_id):
    """Return the stratum label for a sys_id."""
    entry = volume_map.get(sys_id, {})
    vols = entry.get('volumes', [])
    n = len(vols)
    if n >= 4:
        return '4plus_volume'
    elif n == 3:
        return '3_volume'
    else:
        return '2_volume'


def validate_manuscript(sys_id, entry, session, delay=0.5):
    """Validate a single manuscript's volume mapping against live IIIF.

    Checks:
    1. Each suffix's IIIF manifest exists (HTTP 200)
    2. Canvas count in manifest matches page_count in map
    3. Suffix=1 is the primary IE
    4. No suffix gaps (e.g., 1, 3 without 2)

    Returns dict with status (pass/fail/error), details, volumes_checked.
    """
    result = {
        'sys_id': sys_id,
        'status': 'pass',
        'volumes_checked': 0,
        'details': '',
        'issues': [],
    }

    volumes = entry.get('volumes', [])
    if not volumes:
        result['status'] = 'fail'
        result['details'] = 'No volumes in map entry'
        return result

    primary_ie = entry.get('primary_ie', '')

    # Check 3: suffix=1 should be primary IE
    suffix_1_vol = next((v for v in volumes if v.get('suffix') == 1), None)
    if suffix_1_vol and suffix_1_vol.get('ie_id') != primary_ie:
        result['issues'].append(
            f"Suffix 1 IE ({suffix_1_vol['ie_id']}) != primary_ie ({primary_ie})"
        )

    # Check 4: no suffix gaps
    suffixes = sorted(v.get('suffix', 0) for v in volumes)
    expected_suffixes = list(range(1, max(suffixes) + 1)) if suffixes else []
    if suffixes != expected_suffixes:
        result['issues'].append(f"Suffix gap: have {suffixes}, expected {expected_suffixes}")

    # Check 1 & 2: verify each suffix's manifest
    for vol in volumes:
        suffix = vol.get('suffix', 1)
        ie_id = vol.get('ie_id', '?')
        expected_pages = vol.get('page_count', 0)

        manifest_url = f"{NLI_IIIF_BASE}/DOCID/PNX_MANUSCRIPTS{sys_id}-{suffix}/manifest"

        try:
            resp = session.get(manifest_url, timeout=20)
            result['volumes_checked'] += 1

            if resp.status_code != 200:
                result['issues'].append(
                    f"Suffix {suffix} ({ie_id}): HTTP {resp.status_code}"
                )
                continue

            # Parse manifest and count canvases
            try:
                manifest = resp.json()
                sequences = manifest.get('sequences', [])
                if sequences:
                    canvases = sequences[0].get('canvases', [])
                    actual_pages = len(canvases)
                else:
                    actual_pages = 0

                if expected_pages and actual_pages != expected_pages:
                    result['issues'].append(
                        f"Suffix {suffix} ({ie_id}): map says {expected_pages} pages, "
                        f"manifest has {actual_pages} canvases"
                    )
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                result['issues'].append(
                    f"Suffix {suffix} ({ie_id}): manifest parse error: {e}"
                )

        except requests.exceptions.Timeout:
            result['issues'].append(f"Suffix {suffix} ({ie_id}): timeout")
            result['volumes_checked'] += 1
        except requests.exceptions.RequestException as e:
            result['issues'].append(f"Suffix {suffix} ({ie_id}): network error: {e}")
            result['volumes_checked'] += 1

        if delay > 0:
            time.sleep(delay)

    # Determine final status
    if result['issues']:
        # Distinguish network errors from real failures
        network_issues = [i for i in result['issues'] if 'timeout' in i.lower() or 'network error' in i.lower()]
        if len(network_issues) == len(result['issues']):
            result['status'] = 'error'
        else:
            result['status'] = 'fail'
        result['details'] = '; '.join(result['issues'])
    else:
        result['details'] = f"All {len(volumes)} suffixes valid"

    return result


def validate_sample(volume_map, sample, delay=0.5):
    """Validate a list of sys_ids against live IIIF manifests.

    Returns list of result dicts.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) GenizahSearch-Validator/1.0",
    })

    results = []
    total = len(sample)

    for i, sys_id in enumerate(sample):
        entry = volume_map.get(sys_id, {})
        if not entry:
            results.append({
                'sys_id': sys_id,
                'status': 'error',
                'volumes_checked': 0,
                'details': 'sys_id not found in volume map',
            })
            continue

        if (i + 1) % 25 == 0 or i == 0:
            print(f"  Validating {i + 1}/{total}...")

        result = validate_manuscript(sys_id, entry, session, delay=delay)
        results.append(result)

    return results


def build_report(results, strata_counts, volume_map):
    """Build structured JSON report from validation results."""
    passed = sum(1 for r in results if r['status'] == 'pass')
    failed = sum(1 for r in results if r['status'] == 'fail')
    errors = sum(1 for r in results if r['status'] == 'error')
    total = len(results)

    # Count sampled strata
    sampled_strata = {'2_volume': 0, '3_volume': 0, '4plus_volume': 0, 'large_gap': 0}
    for r in results:
        stratum = classify_sys_id(volume_map, r['sys_id'])
        sampled_strata[stratum] += 1

    pass_rate = (passed / total * 100) if total > 0 else 0

    report = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'total_in_map': len(volume_map),
        'total_sampled': total,
        'passed': passed,
        'failed': failed,
        'errors': errors,
        'pass_rate_pct': round(pass_rate, 1),
        'strata_population': strata_counts,
        'strata_sampled': sampled_strata,
        'results': [
            {k: v for k, v in r.items() if k != 'issues'}
            for r in results
        ],
        'summary': (
            f"{pass_rate:.1f}% pass rate ({passed}/{total}). "
            f"{failed} failures, {errors} network errors."
        ),
    }
    return report


def print_summary(report):
    """Print human-readable summary to stdout."""
    print("\n" + "=" * 60)
    print("  IE Volume Map Validation Report")
    print("=" * 60)
    print(f"  Timestamp:      {report['timestamp']}")
    print(f"  Map entries:    {report['total_in_map']:,}")
    print(f"  Sampled:        {report['total_sampled']}")
    print(f"  Passed:         {report['passed']}")
    print(f"  Failed:         {report['failed']}")
    print(f"  Network errors: {report['errors']}")
    print(f"  Pass rate:      {report['pass_rate_pct']}%")

    print(f"\n  Strata population:")
    for k, v in report['strata_population'].items():
        print(f"    {k}: {v:,}")

    print(f"\n  Strata sampled:")
    for k, v in report['strata_sampled'].items():
        print(f"    {k}: {v}")

    # Show failures
    failures = [r for r in report['results'] if r['status'] in ('fail', 'error')]
    if failures:
        print(f"\n  Issues ({len(failures)}):")
        for r in failures[:20]:  # cap display
            print(f"    [{r['status'].upper()}] {r['sys_id']}: {r['details']}")
        if len(failures) > 20:
            print(f"    ... and {len(failures) - 20} more")

    print(f"\n  {report['summary']}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Validate ie_volume_map.json against live NLI IIIF manifests"
    )
    parser.add_argument(
        "--sample", type=int, default=250,
        help="Number of manuscripts to sample (default: 250)"
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Save JSON report to this file"
    )
    parser.add_argument(
        "--delay", type=float, default=0.5,
        help="Delay between IIIF requests in seconds (default: 0.5)"
    )
    parser.add_argument(
        "--map-file", type=str, default=None,
        help="Path to ie_volume_map.json (default: project root)"
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Random seed for reproducible sampling"
    )
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    # Load volume map
    print("Loading ie_volume_map.json...")
    volume_map = load_volume_map(args.map_file)
    print(f"  Loaded {len(volume_map):,} entries")

    # Build stratified sample
    print(f"\nBuilding stratified sample (target: {args.sample})...")
    sample, strata_counts = build_stratified_sample(volume_map, target_size=args.sample)
    print(f"  Sample size: {len(sample)}")
    print(f"  Strata population: {strata_counts}")

    # Validate
    print(f"\nValidating {len(sample)} manuscripts (delay={args.delay}s)...")
    results = validate_sample(volume_map, sample, delay=args.delay)

    # Build and display report
    report = build_report(results, strata_counts, volume_map)
    print_summary(report)

    # Save JSON report if requested
    if args.output:
        output_path = Path(args.output)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\n  Report saved to {output_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
