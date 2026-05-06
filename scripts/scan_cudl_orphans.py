"""Scan for CUDL classmarks absent from libraries.csv.

CUDL (Cambridge University Digital Library) has manifests for ~141K classmarks.
Some of those classmarks are missing from libraries.csv entirely — sometimes
because the corresponding NLI Alma record bundles two adjacent classmarks under
a single sys_id (e.g. the user-reported case of T-S NS 329.96 sharing a record
with T-S NS 329.97).

This scan produces two reports:

1. ``reports/cudl_orphans_all.csv`` — every CUDL classmark with no matching
   libraries.csv variant (~19K rows). Rough catch-all.
2. ``reports/cudl_orphans_with_neighbor.csv`` — the high-confidence subset:
   orphans whose immediate numeric neighbour (Δ ±1 or ±2) DOES exist in
   libraries.csv. These are the strongest candidates for "merge missing
   shelfmark alias into existing row".

Usage:
    python scripts/scan_cudl_orphans.py
"""
from __future__ import annotations

import csv
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LIBRARIES_CSV = ROOT / "libraries.csv"
NLI_CROSSREF = ROOT / "nli_data" / "nli_crossref.db"
REPORTS_DIR = ROOT / "reports"

# Phase 84: import from shared.shelfmark_bridge — one source of truth (site #4, D-08).
import sys as _sys
if str(ROOT) not in _sys.path:
    _sys.path.insert(0, str(ROOT))
from shared.shelfmark_bridge import cudl_normalize as normalize, NUM_RE  # noqa: F401


def main() -> int:
    REPORTS_DIR.mkdir(exist_ok=True)

    norm_to_sys: dict[str, list[str]] = defaultdict(list)
    sys_to_calls: dict[str, str] = {}
    with LIBRARIES_CSV.open("r", encoding="utf-8", newline="") as f:
        for row in csv.reader(f):
            if len(row) < 4 or row[3] != "CUL":
                continue
            sys_id = row[0]
            sys_to_calls[sys_id] = row[2]
            for variant in (row[2] or "").split("|"):
                v = variant.strip()
                if not v:
                    continue
                n = normalize(v)
                if n:
                    norm_to_sys[n].append(sys_id)

    print(f"CUL rows in libraries.csv: {len(sys_to_calls):,}")
    print(f"Distinct normalized variants: {len(norm_to_sys):,}")

    con = sqlite3.connect(f"file:{NLI_CROSSREF}?mode=ro", uri=True)
    cudl = list(
        con.execute(
            "SELECT label, manifest_url, normalized_shelfmark FROM cambridge_manifests"
        )
    )
    print(f"CUDL manifests: {len(cudl):,}")

    all_orphans: list[tuple[str, str, str]] = []
    with_neighbor: list[dict[str, str]] = []
    for label, manifest_url, ns in cudl:
        n = normalize(ns)
        if n in norm_to_sys:
            continue
        all_orphans.append((label, manifest_url, ns))

        m = NUM_RE.match(n)
        if not m:
            continue
        prefix, num = m.group(1), int(m.group(2))
        seen_sids: set[str] = set()
        rows: list[tuple[int, str, str]] = []
        for delta in (-1, 1, -2, 2):
            cand = f"{prefix}{num + delta}"
            for sid in norm_to_sys.get(cand, []):
                if sid in seen_sids:
                    continue
                seen_sids.add(sid)
                rows.append((delta, cand, sid))
        if rows:
            rows.sort(key=lambda r: abs(r[0]))
            best_delta, best_cand, best_sid = rows[0]
            with_neighbor.append(
                {
                    "cudl_classmark": ns,
                    "cudl_label": label,
                    "cudl_manifest_url": manifest_url,
                    "best_delta": str(best_delta),
                    "neighbor_norm": best_cand,
                    "neighbor_sys_id": best_sid,
                    "neighbor_call_numbers": sys_to_calls[best_sid],
                    "all_neighbors": "; ".join(
                        f"d{d:+d}={c}->{s}" for d, c, s in rows
                    ),
                }
            )

    all_path = REPORTS_DIR / "cudl_orphans_all.csv"
    with all_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["cudl_label", "manifest_url", "normalized_shelfmark"])
        w.writerows(all_orphans)
    print(f"Wrote {all_path}: {len(all_orphans):,} rows")

    nb_path = REPORTS_DIR / "cudl_orphans_with_neighbor.csv"
    with nb_path.open("w", encoding="utf-8", newline="") as f:
        if with_neighbor:
            w = csv.DictWriter(f, fieldnames=list(with_neighbor[0].keys()))
            w.writeheader()
            w.writerows(with_neighbor)
    print(f"Wrote {nb_path}: {len(with_neighbor):,} rows")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
