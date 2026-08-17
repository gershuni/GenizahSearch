#!/usr/bin/env python3
"""Build the canonical-mask review packet for one reference namespace.

The masker blanks the stretches of an edited work that are verbatim-or-near
quotations of the canonical corpus (Bible, Mishnah, Tosefta, Bavli,
Yerushalmi), so the matcher never credits a fragment to the quoting work when
it really matches the quoted one.  Over-masking is the failure mode this packet
exists to catch: formulaic Aramaic can read as Talmud to a density-based
matcher, and a work masked to shreds loses recall silently -- no gate fires,
the identifications simply never appear.

The packet names ONLY the works of the reviewed namespace.  Every other work in
the reference corpus is summarised in aggregate, with no ids and no titles: the
base corpus carries identifiers that must not be enumerated in a file anyone
may read, and a review packet is exactly the sort of file that gets shared.
"""

from __future__ import annotations

import argparse
import bisect
import json
import math
import pickle
import random
import statistics
from pathlib import Path

try:
    from scripts.discovery_v4_common import (
        raw_id_prefix,
        require_hash,
        sha256_file,
        stable_json_dump,
    )
except ModuleNotFoundError:  # direct invocation
    from discovery_v4_common import (
        raw_id_prefix,
        require_hash,
        sha256_file,
        stable_json_dump,
    )


SCHEMA_VERSION = "discovery-v4-mask-review-v1"


def check_ranges(raw_id: str, ranges: list) -> None:
    """Reject a mask list that is inverted, unsorted, or self-overlapping.

    Every masked-letter total downstream is a plain sum of range widths, which
    is only the masked length if the ranges are disjoint.  The masker merges
    them, so a violation here means the mask file is not what it claims to be
    -- summing anyway would report a masked fraction above the truth (and,
    above 1.0, an obvious absurdity from a subtle cause).
    """
    previous_end = 0
    for entry in ranges:
        if len(entry) != 2:
            raise ValueError(f"{raw_id}: mask range is not a [start, end] pair")
        start, end = entry
        if start > end:
            raise ValueError(f"{raw_id}: inverted mask range {start} > {end}")
        if start < previous_end:
            raise ValueError(
                f"{raw_id}: mask ranges overlap or are unsorted at {start}"
            )
        previous_end = end


def masked_letters(ranges: list) -> int:
    return sum(end - start for start, end in ranges)


def unit_index_for_offset(unit_starts: list, offset: int) -> int:
    """Index of the division containing `offset`; -1 before the first one."""
    return bisect.bisect_right(unit_starts, offset) - 1


def excerpt_text(stream: str, start: int, end: int, budget: int) -> str:
    """The masked span, elided in the middle when it exceeds the budget."""
    span = stream[start:end]
    if budget <= 0 or len(span) <= budget:
        return span
    head = budget // 2
    tail = budget - head
    return f"{span[:head]}…{span[len(span) - tail:]}"


def build_interval_rows(
    ranges: list,
    stream: str,
    units: list,
    excerpt_chars: int,
    context_chars: int,
) -> list:
    unit_starts = [unit["start_offset"] for unit in units]
    rows = []
    for start, end in ranges:
        position = unit_index_for_offset(unit_starts, start)
        unit = units[position] if position >= 0 else None
        rows.append(
            {
                "start": start,
                "end": end,
                "length": end - start,
                "unit_ordinal": position + 1 if unit else None,
                "unit_label_he": unit.get("label_he") if unit else None,
                "context_before": stream[max(0, start - context_chars) : start],
                "excerpt": excerpt_text(stream, start, end, excerpt_chars),
                "context_after": stream[end : end + context_chars],
            }
        )
    return rows


def select_longest(rows: list, count: int) -> list:
    """The `count` widest intervals, longest first, ties broken by position."""
    return sorted(rows, key=lambda row: (-row["length"], row["start"]))[:count]


def select_sample(rows: list, count: int, seed: int, exclude: set) -> list:
    """A deterministic sample of the intervals not already shown as longest."""
    pool = [row for row in rows if row["start"] not in exclude]
    if count <= 0 or not pool:
        return []
    if len(pool) <= count:
        return list(pool)
    picks = random.Random(seed).sample(range(len(pool)), count)
    return [pool[index] for index in sorted(picks)]


def percentile(values: list, share: float) -> float:
    """Nearest-rank percentile (an observed value, never an interpolation).

    The median is reported with ``statistics.median`` instead: a column headed
    "median" that is really a nearest-rank p50 would read as a true median and
    quietly be a different number.
    """
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(1, min(len(ordered), math.ceil(share * len(ordered))))
    return ordered[rank - 1]


def group_label(raw_id: str) -> str:
    """The namespace a raw id belongs to -- never the id itself."""
    return raw_id.split(":", 1)[0] if ":" in raw_id else "(unprefixed)"


def summarize_baseline(reference: list, masks: dict, prefix: str) -> list:
    """Aggregate masked-fraction stats per corpus group, naming no work."""
    groups: dict = {}
    for work in reference:
        raw_id = str(work.get("id", ""))
        if raw_id.startswith(prefix):
            continue
        length = len(work["stream"])
        ranges = masks.get(raw_id) or []
        fraction = masked_letters(ranges) / length if length else 0.0
        bucket = groups.setdefault(
            group_label(raw_id), {"fractions": [], "masked_works": 0, "letters": 0}
        )
        bucket["fractions"].append(fraction)
        bucket["letters"] += length
        if ranges:
            bucket["masked_works"] += 1
    rows = []
    for label in sorted(groups):
        bucket = groups[label]
        fractions = bucket["fractions"]
        rows.append(
            {
                "group": label,
                "work_count": len(fractions),
                "works_with_masks": bucket["masked_works"],
                "letters": bucket["letters"],
                "masked_fraction_median": round(statistics.median(fractions), 6),
                "masked_fraction_p90": round(percentile(fractions, 0.9), 6),
                "masked_fraction_max": round(max(fractions), 6),
            }
        )
    return rows


def build_packet(args: argparse.Namespace) -> dict:
    reference_path = Path(args.reference)
    masks_path = Path(args.masks)
    manifest_path = Path(args.manifest)
    # A pin is optional here -- this tool reads artifacts, it does not produce
    # one -- but an offered pin is always enforced.
    for path, expected, label in (
        (reference_path, args.reference_sha256, "reference corpus"),
        (masks_path, args.masks_sha256, "canonical masks"),
        (manifest_path, args.manifest_sha256, "reference manifest"),
    ):
        if expected:
            require_hash(path, expected, label)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entries = {entry["raw_reference_id"]: entry for entry in manifest["entries"]}
    masks = json.loads(masks_path.read_text(encoding="utf-8"))
    with reference_path.open("rb") as stream:
        reference = pickle.load(stream)

    prefix = raw_id_prefix(args.reference_namespace)
    namespace_works = [
        work for work in reference if str(work.get("id", "")).startswith(prefix)
    ]
    if not namespace_works:
        raise ValueError(
            f"reference corpus holds no {args.reference_namespace} references"
        )
    missing = sorted(
        str(work["id"]) for work in namespace_works if work["id"] not in entries
    )
    if missing:
        raise ValueError(
            f"manifest does not describe every {args.reference_namespace} "
            f"reference (first missing: {missing[0]})"
        )

    include = set(args.detail_include or [])
    works = []
    for work in namespace_works:
        raw_id = str(work["id"])
        entry = entries[raw_id]
        ranges = masks.get(raw_id) or []
        check_ranges(raw_id, ranges)
        length = len(work["stream"])
        masked = masked_letters(ranges)
        rows = build_interval_rows(
            ranges,
            work["stream"],
            entry["unit_offsets"],
            args.excerpt_chars,
            args.context_chars,
        )
        longest = select_longest(rows, args.top_intervals)
        sample = select_sample(
            rows, args.sample_per_work, args.sample_seed, {row["start"] for row in longest}
        )
        works.append(
            {
                "raw_reference_id": raw_id,
                "source_key": entry["source_key"],
                "title": entry["title"],
                "provider": entry["provider"],
                "locus_grain": entry["locus_grain"],
                "stream_len": length,
                "masked_letters": masked,
                "masked_fraction": round(masked / length, 6) if length else 0.0,
                "interval_count": len(ranges),
                "longest_interval": max((row["length"] for row in rows), default=0),
                "longest_intervals": longest,
                "sample_intervals": sample,
                "forced_detail": raw_id in include or entry["source_key"] in include,
            }
        )
    works.sort(key=lambda row: (-row["masked_fraction"], row["raw_reference_id"]))

    total_letters = sum(row["stream_len"] for row in works)
    total_masked = sum(row["masked_letters"] for row in works)
    return {
        "schema_version": SCHEMA_VERSION,
        "reference_namespace": args.reference_namespace,
        "reference_sha256": sha256_file(reference_path),
        "masks_sha256": sha256_file(masks_path),
        "manifest_sha256": sha256_file(manifest_path),
        "sample_seed": args.sample_seed,
        "namespace_totals": {
            "work_count": len(works),
            "works_with_masks": sum(1 for row in works if row["interval_count"]),
            "letters": total_letters,
            "masked_letters": total_masked,
            "masked_fraction": round(total_masked / total_letters, 6)
            if total_letters
            else 0.0,
        },
        "works": works,
        "baseline_groups": summarize_baseline(reference, masks, prefix),
    }


def _interval_block(row: dict) -> list:
    address = row["unit_label_he"] or "(before the first division)"
    lines = [
        f"- **{row['length']:,} letters** at offset {row['start']:,} "
        f"— {address}",
    ]
    if row["context_before"]:
        lines.append(f"  - before: `{row['context_before']}`")
    lines.append(f"  - **masked: `{row['excerpt']}`**")
    if row["context_after"]:
        lines.append(f"  - after: `{row['context_after']}`")
    return lines


def render_markdown(packet: dict, detail_works: int) -> str:
    namespace = packet["reference_namespace"]
    totals = packet["namespace_totals"]
    lines = [
        f"# Canonical-mask review — {namespace}",
        "",
        "The masker blanks what an edited work quotes from the canonical corpus",
        "so a fragment of the quoted text is never credited to the quoting work.",
        "**The question this packet asks is whether it blanked more than that.**",
        "A plausible-looking masked span that is not actually a canonical",
        "quotation costs recall in silence: no gate fires, the identifications",
        "just never appear.",
        "",
        "## Inputs",
        "",
        f"- masks `{packet['masks_sha256']}`",
        f"- reference corpus `{packet['reference_sha256']}`",
        f"- reference manifest `{packet['manifest_sha256']}`",
        f"- sample seed `{packet['sample_seed']}` (the sample is reproducible)",
        "",
        "## Totals",
        "",
        f"{totals['work_count']} references, {totals['letters']:,} letters, "
        f"{totals['masked_letters']:,} masked "
        f"({totals['masked_fraction']:.2%}); "
        f"{totals['works_with_masks']} carry at least one mask.",
        "",
        "## Per-work masked fraction",
        "",
        "| Work | Letters | Masked | % | Intervals | Longest |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in packet["works"]:
        lines.append(
            f"| {row['title']} | {row['stream_len']:,} | "
            f"{row['masked_letters']:,} | {row['masked_fraction']:.2%} | "
            f"{row['interval_count']:,} | {row['longest_interval']:,} |"
        )
    lines += [
        "",
        "## Baseline — the rest of the reference corpus",
        "",
        "Aggregate only, by corpus group. A group's median is what a normal",
        "masked fraction looks like there; compare the table above against it.",
        "",
        "| Group | Works | With masks | Median | p90 | Max |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in packet["baseline_groups"]:
        lines.append(
            f"| {row['group']} | {row['work_count']:,} | "
            f"{row['works_with_masks']:,} | {row['masked_fraction_median']:.2%} | "
            f"{row['masked_fraction_p90']:.2%} | {row['masked_fraction_max']:.2%} |"
        )

    ranked = [row for row in packet["works"] if row["interval_count"]]
    detailed = ranked[:detail_works] + [
        row for row in ranked[detail_works:] if row["forced_detail"]
    ]
    omitted = len(ranked) - len(detailed)
    lines += [
        "",
        "## What was masked",
        "",
        f"The {len(detailed)} most-masked references (plus any explicitly"
        f" requested), longest intervals first, then a seeded sample of the"
        f" rest. {omitted} further masked references are summarised in the"
        " table above but not detailed here.",
        "",
    ]
    for row in detailed:
        lines += [
            f"### {row['title']}",
            "",
            f"`{row['raw_reference_id']}` — {row['stream_len']:,} letters, "
            f"{row['masked_letters']:,} masked ({row['masked_fraction']:.2%}) "
            f"across {row['interval_count']:,} intervals.",
            "",
            "**Longest masked intervals**",
            "",
        ]
        for interval in row["longest_intervals"]:
            lines += _interval_block(interval)
        if row["sample_intervals"]:
            lines += ["", "**Sample of the others**", ""]
            for interval in row["sample_intervals"]:
                lines += _interval_block(interval)
        lines.append("")
    return "\n".join(lines) + "\n"


def run(args: argparse.Namespace) -> dict:
    packet = build_packet(args)
    if args.output:
        stable_json_dump(packet, args.output)
    if args.output_markdown:
        path = Path(args.output_markdown)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            render_markdown(packet, args.detail_works), encoding="utf-8", newline="\n"
        )
    totals = packet["namespace_totals"]
    print(
        json.dumps(
            {
                "reference_namespace": packet["reference_namespace"],
                "masks_sha256": packet["masks_sha256"],
                **totals,
                "most_masked": [
                    {
                        "title": row["title"],
                        "masked_fraction": row["masked_fraction"],
                        "longest_interval": row["longest_interval"],
                    }
                    for row in packet["works"][:5]
                ],
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return packet


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference", required=True)
    parser.add_argument("--reference-sha256")
    parser.add_argument("--masks", required=True)
    parser.add_argument("--masks-sha256")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--manifest-sha256")
    parser.add_argument("--reference-namespace", required=True)
    parser.add_argument("--output")
    parser.add_argument("--output-markdown")
    parser.add_argument("--top-intervals", type=int, default=5)
    parser.add_argument("--sample-per-work", type=int, default=3)
    parser.add_argument("--sample-seed", type=int, default=20260817)
    parser.add_argument("--excerpt-chars", type=int, default=220)
    parser.add_argument("--context-chars", type=int, default=40)
    parser.add_argument("--detail-works", type=int, default=12)
    parser.add_argument(
        "--detail-include",
        nargs="*",
        help="raw ids or source keys always detailed, however little they mask",
    )
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
