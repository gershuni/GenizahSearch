"""
Build IE-to-IIIF-suffix mapping for all multi-IE manuscripts.

For each multi-IE manuscript, fetches NLI MARC record and extracts ALL 907
fields in order. The i-th 907 field maps to IIIF manifest suffix -(i+1).

Output: ie_volume_map.json in the project root

Structure:
{
  "990000...": {
    "primary_ie": "IE12345",       // first 907 = primary
    "volumes": [
      {"ie_id": "IE12345", "suffix": 1, "page_count": 58},
      {"ie_id": "IE67890", "suffix": 2, "page_count": 13}
    ]
  }
}

Usage:
  python scripts/build_ie_volume_map.py --local       # From existing primary_ie_map.json (no network)
  python scripts/build_ie_volume_map.py --server       # Fetch MARC records from NLI
  python scripts/build_ie_volume_map.py --server --batch 10  # Limit fetches (testing)
"""

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
TRANSCRIPTIONS_FILE = PROJECT_DIR / "Transcriptions.txt"
OUTPUT_FILE = PROJECT_DIR / "ie_volume_map.json"
PRIMARY_IE_MAP_FILE = PROJECT_DIR / "primary_ie_map.json"


def parse_multi_ie_manuscripts(v8_path):
    """Parse Transcriptions.txt and return multi-IE manuscripts with per-IE page counts.

    Returns:
        {sys_id: {ie_id: page_count, ...}, ...}  (only multi-IE)
    """
    sys_ie_counts = defaultdict(lambda: defaultdict(int))
    header_re = re.compile(r"(\d{10,})_(IE\d+)_P(\d+)_FL(\d+)")

    print(f"  Parsing {v8_path}...")
    with open(v8_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("==>"):
                m = header_re.search(line)
                if m:
                    sys_id, ie_id = m.group(1), m.group(2)
                    sys_ie_counts[sys_id][ie_id] += 1

    multi = {
        sid: dict(ies)
        for sid, ies in sys_ie_counts.items()
        if len(ies) > 1
    }
    print(f"  Found {len(multi):,} multi-IE manuscripts")
    return multi


def fetch_marc_all_907s(sys_id, session):
    """Fetch MARC record and extract ALL 907 fields in order.

    Returns list of {ie_id, page_count} in 907 document order,
    or None on failure.
    """
    url = f"https://iiif.nli.org.il/IIIFv21/marc/bib/{sys_id}"
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            return None

        text = resp.text

        # Extract all 907 blocks in document order
        ie_blocks = re.findall(
            r'<datafield[^>]*tag="907"[^>]*>(.*?)</datafield>',
            text,
            re.DOTALL,
        )
        if not ie_blocks:
            return None

        volumes = []
        for idx, block in enumerate(ie_blocks):
            ie_match = re.search(
                r'<subfield code="c">(IE\d+)</subfield>', block
            )
            page_match = re.search(
                r'<subfield code="i">(\d+)</subfield>', block
            )
            if ie_match:
                volumes.append({
                    "ie_id": ie_match.group(1),
                    "suffix": idx + 1,
                    "page_count": int(page_match.group(1)) if page_match else None,
                })

        return volumes if volumes else None

    except Exception:
        return None


def build_map_local(multi_ie_manuscripts):
    """Build volume map from existing primary_ie_map.json (no network).

    Uses heuristic ordering: primary IE first (from primary_ie_map.json if
    available), remaining IEs sorted by page count descending.
    Suffix assignment is HEURISTIC — not validated against MARC 907 order.
    """
    # Load existing primary_ie_map if available
    primary_map = {}
    if PRIMARY_IE_MAP_FILE.exists():
        with open(PRIMARY_IE_MAP_FILE, "r", encoding="utf-8") as f:
            primary_map = json.load(f)

    result = {}
    for sys_id, ies in multi_ie_manuscripts.items():
        primary_ie = primary_map.get(sys_id, {}).get("primary_ie")

        # Order: primary IE first, then by page count descending
        ie_list = sorted(ies.keys(), key=lambda ie: (
            0 if ie == primary_ie else 1,
            -ies[ie],
        ))

        volumes = []
        for idx, ie_id in enumerate(ie_list):
            volumes.append({
                "ie_id": ie_id,
                "suffix": idx + 1,
                "page_count": ies[ie_id],
                "source": "heuristic",
            })

        result[sys_id] = {
            "primary_ie": primary_ie or ie_list[0],
            "volumes": volumes,
        }

    return result


def build_map_server(multi_ie_manuscripts, batch_limit=None):
    """Build volume map by fetching MARC records from NLI."""
    import requests

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    })

    result = {}
    marc_hits = 0
    marc_misses = 0
    fallback_used = 0

    sys_ids = sorted(multi_ie_manuscripts.keys())
    if batch_limit:
        sys_ids = sys_ids[:batch_limit]

    for i, sys_id in enumerate(sys_ids):
        if (i + 1) % 100 == 0:
            print(f"  Processing {i + 1}/{len(sys_ids)}... "
                  f"(MARC: {marc_hits}, fallback: {fallback_used})")

        ies = multi_ie_manuscripts[sys_id]
        marc_volumes = fetch_marc_all_907s(sys_id, session)

        if marc_volumes:
            # Validate: at least one MARC IE exists in our transcription data
            marc_ie_ids = {v["ie_id"] for v in marc_volumes}
            known_ies = marc_ie_ids & set(ies.keys())

            if known_ies:
                # Use MARC ordering, supplement page counts from transcription data
                volumes = []
                for v in marc_volumes:
                    vol = {
                        "ie_id": v["ie_id"],
                        "suffix": v["suffix"],
                        "page_count": v["page_count"] or ies.get(v["ie_id"], 0),
                        "source": "marc_907",
                    }
                    # Add transcription page count if different from MARC
                    if v["ie_id"] in ies:
                        vol["transcription_pages"] = ies[v["ie_id"]]
                    volumes.append(vol)

                result[sys_id] = {
                    "primary_ie": marc_volumes[0]["ie_id"],
                    "volumes": volumes,
                }
                marc_hits += 1
                continue
            else:
                marc_misses += 1

        # Fallback: heuristic ordering
        fallback_used += 1
        ie_list = sorted(ies.keys(), key=lambda ie: -ies[ie])
        volumes = []
        for idx, ie_id in enumerate(ie_list):
            volumes.append({
                "ie_id": ie_id,
                "suffix": idx + 1,
                "page_count": ies[ie_id],
                "source": "heuristic",
            })

        result[sys_id] = {
            "primary_ie": ie_list[0],
            "volumes": volumes,
        }

        # Rate limiting: 5 requests per second
        if (i + 1) % 5 == 0:
            time.sleep(1)

    print(f"\n  Results: MARC={marc_hits}, MARC-miss={marc_misses}, "
          f"fallback={fallback_used}")
    return result


def build_map_smart(multi_ie_manuscripts, batch_limit=None):
    """Smart build: use primary_ie_map.json for 2-IE, MARC only for 3+ IE.

    For 2-IE manuscripts, ordering is deterministic:
    primary IE = suffix 1, other IE = suffix 2.
    Only fetches MARC records for 3+ IE manuscripts where ordering matters.
    """
    import requests

    # Load existing primary_ie_map.json
    primary_map = {}
    if PRIMARY_IE_MAP_FILE.exists():
        with open(PRIMARY_IE_MAP_FILE, "r", encoding="utf-8") as f:
            primary_map = json.load(f)

    result = {}
    two_ie_count = 0
    marc_needed = []

    # Phase 1: Handle 2-IE manuscripts locally (deterministic ordering)
    for sys_id, ies in multi_ie_manuscripts.items():
        if len(ies) == 2:
            primary_ie = primary_map.get(sys_id, {}).get("primary_ie")
            ie_list = sorted(ies.keys(), key=lambda ie: (
                0 if ie == primary_ie else 1,
                -ies[ie],
            ))
            result[sys_id] = {
                "primary_ie": primary_ie or ie_list[0],
                "volumes": [
                    {"ie_id": ie_list[0], "suffix": 1, "page_count": ies[ie_list[0]],
                     "source": "deterministic_2ie"},
                    {"ie_id": ie_list[1], "suffix": 2, "page_count": ies[ie_list[1]],
                     "source": "deterministic_2ie"},
                ],
            }
            two_ie_count += 1
        else:
            marc_needed.append(sys_id)

    print(f"  2-IE manuscripts (local): {two_ie_count}")
    print(f"  3+ IE manuscripts (need MARC): {len(marc_needed)}")

    if not marc_needed:
        return result

    # Phase 2: Fetch MARC for 3+ IE manuscripts
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    })

    if batch_limit:
        marc_needed = marc_needed[:batch_limit]

    marc_hits = 0
    fallback_used = 0

    for i, sys_id in enumerate(marc_needed):
        if (i + 1) % 50 == 0:
            print(f"  MARC fetch {i + 1}/{len(marc_needed)}... "
                  f"(hits: {marc_hits}, fallback: {fallback_used})")

        ies = multi_ie_manuscripts[sys_id]
        marc_volumes = fetch_marc_all_907s(sys_id, session)

        if marc_volumes:
            marc_ie_ids = {v["ie_id"] for v in marc_volumes}
            known_ies = marc_ie_ids & set(ies.keys())

            if known_ies:
                volumes = []
                for v in marc_volumes:
                    vol = {
                        "ie_id": v["ie_id"],
                        "suffix": v["suffix"],
                        "page_count": v["page_count"] or ies.get(v["ie_id"], 0),
                        "source": "marc_907",
                    }
                    if v["ie_id"] in ies:
                        vol["transcription_pages"] = ies[v["ie_id"]]
                    volumes.append(vol)

                result[sys_id] = {
                    "primary_ie": marc_volumes[0]["ie_id"],
                    "volumes": volumes,
                }
                marc_hits += 1
                continue

        # Fallback: heuristic ordering
        fallback_used += 1
        primary_ie = primary_map.get(sys_id, {}).get("primary_ie")
        ie_list = sorted(ies.keys(), key=lambda ie: (
            0 if ie == primary_ie else 1, -ies[ie],
        ))
        volumes = []
        for idx, ie_id in enumerate(ie_list):
            volumes.append({
                "ie_id": ie_id, "suffix": idx + 1, "page_count": ies[ie_id],
                "source": "heuristic",
            })
        result[sys_id] = {"primary_ie": ie_list[0], "volumes": volumes}

        # Rate limiting
        if (i + 1) % 5 == 0:
            time.sleep(1)

    print(f"\n  3+ IE results: MARC={marc_hits}, fallback={fallback_used}")
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Build IE volume map for multi-IE manuscripts"
    )
    parser.add_argument("--local", action="store_true",
                        help="Heuristic only (no MARC fetch)")
    parser.add_argument("--server", action="store_true",
                        help="Fetch MARC records from NLI (all)")
    parser.add_argument("--smart", action="store_true",
                        help="Local for 2-IE, MARC only for 3+ IE")
    parser.add_argument("--batch", type=int, default=None,
                        help="Limit MARC fetches (for testing)")
    args = parser.parse_args()

    if not args.local and not args.server and not args.smart:
        print("Specify --local, --server, or --smart")
        sys.exit(1)

    multi = parse_multi_ie_manuscripts(TRANSCRIPTIONS_FILE)

    if args.local:
        print("\n  Building map with heuristic ordering...")
        result = build_map_local(multi)
    elif args.smart:
        print("\n  Building map: local for 2-IE, MARC for 3+ IE...")
        result = build_map_smart(multi, args.batch)
    else:
        print(f"\n  Building map with MARC 907 ordering + heuristic fallback...")
        result = build_map_server(multi, args.batch)

    # Save
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"\n  Saved {len(result):,} entries to {OUTPUT_FILE}")

    # Summary stats
    sources = defaultdict(int)
    total_volumes = 0
    for entry in result.values():
        for vol in entry["volumes"]:
            sources[vol.get("source", "unknown")] += 1
            total_volumes += 1
    print(f"\n  Total volumes: {total_volumes:,}")
    print("  Source distribution:")
    for src, count in sorted(sources.items()):
        print(f"    {src}: {count:,}")

    # Volume count distribution
    vol_counts = defaultdict(int)
    for entry in result.values():
        vol_counts[len(entry["volumes"])] += 1
    print("\n  Volume count distribution:")
    for n, count in sorted(vol_counts.items()):
        print(f"    {n} volumes: {count:,} manuscripts")

    # Validate known manuscripts
    for sid, expected_primary in [
        ("990000910280205171", "IE89040977"),
        ("990000571740205171", "IE48174416"),
    ]:
        if sid in result:
            entry = result[sid]
            actual = entry["primary_ie"]
            vols = entry["volumes"]
            match = "OK" if actual == expected_primary else f"MISMATCH (got {actual})"
            print(f"\n  Validate {sid}: primary={actual} {match}")
            for v in vols:
                print(f"    suffix -{v['suffix']}: {v['ie_id']} "
                      f"({v['page_count']} pages, {v.get('source', '?')})")


if __name__ == "__main__":
    main()
