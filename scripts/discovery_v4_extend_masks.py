#!/usr/bin/env python3
"""Compute canonical-text masks only for the references appended by V4."""

from __future__ import annotations

import argparse
import json
import pickle
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    from scripts.discovery_v4_common import require_hash, sha256_file, stable_json_dump
except ModuleNotFoundError:  # direct invocation
    from discovery_v4_common import require_hash, sha256_file, stable_json_dump


CANONICAL_CATEGORIES = {"Bible", "Mishnah", "Tosefta", "Bavli", "Yerushalmi"}


def run(args: argparse.Namespace) -> dict:
    probe_root = Path(args.probe_root).resolve()
    scripts_dir = probe_root / "scripts"
    if not scripts_dir.is_dir():
        raise FileNotFoundError(f"research scripts directory not found: {scripts_dir}")
    sys.path.insert(0, str(scripts_dir))
    from mask_ref_canon import mask_edited_works, mask_one_work  # noqa: PLC0415
    from track1_match import build_ref_index  # noqa: PLC0415

    base_reference = Path(args.base_reference)
    v4_reference = Path(args.v4_reference)
    base_masks_path = Path(args.base_masks)
    output_path = Path(args.output)
    require_hash(base_reference, args.base_reference_sha256, "base reference")
    require_hash(v4_reference, args.v4_reference_sha256, "V4 reference")
    require_hash(base_masks_path, args.base_masks_sha256, "base canonical masks")
    with base_reference.open("rb") as stream:
        base = pickle.load(stream)
    with v4_reference.open("rb") as stream:
        v4 = pickle.load(stream)
    if v4[: len(base)] != base:
        raise ValueError("V4 reference does not preserve the complete base-corpus prefix")
    appended = v4[len(base) :]
    if not appended or any(not work.get("id", "").startswith("REF4:") for work in appended):
        raise ValueError("V4 reference append set is missing or contains a non-REF4 id")
    canonical = [work for work in v4 if work.get("cat") in CANONICAL_CATEGORIES]
    index = build_ref_index(canonical)
    stats: Counter[str] = Counter()
    if args.workers == 1:
        new_masks = mask_edited_works(appended, *index[:-1], stats)
    else:
        seg_streams, _seg_work, _seg_off, codes_f, seg_f, pos_f = index[:-1]

        def mask_one(work):
            local: Counter[str] = Counter()
            merged = mask_one_work(
                work["stream"], seg_streams, codes_f, seg_f, pos_f, local
            )
            return work["id"], merged, local

        new_masks = {}
        completed = 0
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [executor.submit(mask_one, work) for work in appended]
            for future in as_completed(futures):
                raw_id, merged, local = future.result()
                stats.update(local)
                if merged:
                    new_masks[raw_id] = merged
                completed += 1
                print(
                    f"canonical masks: {completed}/{len(appended)} references complete",
                    flush=True,
                )
    base_masks = json.loads(base_masks_path.read_text(encoding="utf-8"))
    overlap = set(base_masks) & set(new_masks)
    if overlap:
        raise ValueError("V4 mask ids collide with the base mask set")
    merged = {**base_masks, **new_masks}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    stable_json_dump(merged, output_path)
    report = {
        "schema_version": "discovery-v4-canonical-masks-v1",
        "base_reference_sha256": sha256_file(base_reference),
        "v4_reference_sha256": sha256_file(v4_reference),
        "base_masks_sha256": sha256_file(base_masks_path),
        "output_masks_sha256": sha256_file(output_path),
        "appended_reference_count": len(appended),
        "new_masked_reference_count": len(new_masks),
        "new_masked_letters": sum(
            end - start for ranges in new_masks.values() for start, end in ranges
        ),
        "total_masked_reference_count": len(merged),
        "workers": args.workers,
        "stats": dict(stats),
    }
    stable_json_dump(report, output_path.with_suffix(".report.json"))
    print(json.dumps(report, indent=2))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--probe-root", required=True)
    parser.add_argument("--base-reference", required=True)
    parser.add_argument("--base-reference-sha256", required=True)
    parser.add_argument("--v4-reference", required=True)
    parser.add_argument("--v4-reference-sha256", required=True)
    parser.add_argument("--base-masks", required=True)
    parser.add_argument("--base-masks-sha256", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--workers", type=int, default=1)
    return parser.parse_args()


if __name__ == "__main__":
    parsed = parse_args()
    if parsed.workers <= 0:
        raise ValueError("--workers must be positive")
    run(parsed)
