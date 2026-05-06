"""Audit CUL libraries.csv variants for leading-zero collapse collisions (Phase 84 D-06).

Walks every CUL row in libraries.csv. For each shelfmark variant, computes BOTH:
  base_key = _normalize_without_zero_collapse(variant)
  full_key = cudl_normalize(variant)

The DELTA -- entries where (base_key != full_key) AND multiple distinct sys_ids
collapse onto the same full_key -- is the actual D-06 risk: collisions INTRODUCED
by the leading-zero rule. These keys MUST be excluded from the runtime alias index.

Two output files:
  reports/leading_zero_collisions.csv         <- gate file, runtime exclusion source
  reports/cudl_full_normalization_collisions.csv <- transparency dump (NOT a gate)
"""
from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))  # so we can import shared.shelfmark_bridge
LIBRARIES_CSV = ROOT / "libraries.csv"
REPORTS_DIR = ROOT / "reports"

from shared.shelfmark_bridge import cudl_normalize, _normalize_without_zero_collapse  # noqa: E402


def main() -> int:
    REPORTS_DIR.mkdir(exist_ok=True)

    # full_key -> list of (sys_id, original_variant)
    full_to_sys: dict[str, list[tuple[str, str]]] = defaultdict(list)
    # base_key -> list of (sys_id, original_variant)
    base_to_sys: dict[str, list[tuple[str, str]]] = defaultdict(list)

    cul_row_count = 0

    with LIBRARIES_CSV.open("r", encoding="utf-8", newline="") as f:
        for row in csv.reader(f):
            if len(row) < 4 or row[3] != "CUL":
                continue
            cul_row_count += 1
            sys_id = row[0]
            for variant in (row[2] or "").split("|"):
                v = variant.strip()
                if not v:
                    continue
                full_key = cudl_normalize(v)
                base_key = _normalize_without_zero_collapse(v)
                if full_key:
                    full_to_sys[full_key].append((sys_id, v))
                if base_key:
                    base_to_sys[base_key].append((sys_id, v))

    # -----------------------------------------------------------------------
    # Detect full-normalization collisions (transparency dump)
    # A "full collision" = same full_key maps to 2+ distinct sys_ids
    # -----------------------------------------------------------------------
    full_collisions: list[tuple[str, list[str], list[str]]] = []
    for full_key, entries in full_to_sys.items():
        distinct_sys = list(dict.fromkeys(sid for sid, _ in entries))  # preserve insertion order, dedup
        if len(distinct_sys) > 1:
            variants = list(dict.fromkeys(v for _, v in entries))
            full_collisions.append((full_key, distinct_sys, variants))

    full_collisions.sort(key=lambda x: x[0])

    # -----------------------------------------------------------------------
    # Detect delta-only collisions (gate file)
    # A "delta collision" = full_key collision where the EXTRA sys_ids in the
    # full_key bucket cannot be explained by the base_key already colliding.
    #
    # Algorithm: for each full_key that collides, check if ANY base_key that
    # maps INTO this full_key already has the same set of sys_ids. If the full
    # bucket's sys_id set is strictly larger than every contributing base
    # bucket's sys_id set, the extra sys_ids are introduced by zero-collapse.
    # -----------------------------------------------------------------------
    delta_collisions: list[tuple[str, list[str], list[str]]] = []
    for full_key, entries in full_to_sys.items():
        distinct_full_sys = set(sid for sid, _ in entries)
        if len(distinct_full_sys) <= 1:
            continue  # not a collision at all

        # Collect all base keys whose variants normalize to this full_key
        # (i.e. variants that changed under zero-collapse)
        contributing_base_sys: set[str] = set()
        for sys_id, variant in entries:
            bk = _normalize_without_zero_collapse(variant)
            base_entries = base_to_sys.get(bk, [])
            for base_sid, _ in base_entries:
                contributing_base_sys.add(base_sid)

        # If the base keys collectively already cover all the sys_ids that
        # appear under the full key, then the collision pre-exists zero-collapse
        # and is NOT a delta collision.
        if contributing_base_sys >= distinct_full_sys:
            continue

        # There are sys_ids in the full bucket not explained by any base bucket
        # that would naturally collide there — zero-collapse INTRODUCED this merge.
        variants = list(dict.fromkeys(v for _, v in entries))
        sys_ids = list(dict.fromkeys(sid for sid, _ in entries))
        delta_collisions.append((full_key, sys_ids, variants))

    delta_collisions.sort(key=lambda x: x[0])

    # -----------------------------------------------------------------------
    # Write gate file: reports/leading_zero_collisions.csv
    # -----------------------------------------------------------------------
    gate_path = REPORTS_DIR / "leading_zero_collisions.csv"
    with gate_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["normalized_key", "sys_ids", "variants"])
        for full_key, sys_ids, variants in delta_collisions:
            w.writerow([full_key, "|".join(sys_ids), "|".join(variants)])

    # -----------------------------------------------------------------------
    # Write transparency dump: reports/cudl_full_normalization_collisions.csv
    # -----------------------------------------------------------------------
    full_path = REPORTS_DIR / "cudl_full_normalization_collisions.csv"
    with full_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["normalized_key", "sys_ids", "variants"])
        for full_key, sys_ids, variants in full_collisions:
            w.writerow([full_key, "|".join(sys_ids), "|".join(variants)])

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    distinct_full_keys = len(full_to_sys)
    print(f"CUL rows in libraries.csv:          {cul_row_count:,}")
    print(f"Distinct full-normalized keys:       {distinct_full_keys:,}")
    print(f"Full-normalization collisions:       {len(full_collisions):,}  -> {full_path.name}")
    print(f"Leading-zero DELTA collisions (gate):{len(delta_collisions):,}  -> {gate_path.name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
