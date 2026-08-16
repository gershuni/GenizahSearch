#!/usr/bin/env python3
"""Audit private discovery works against pinned public catalogue snapshots.

This is the first V4 gate.  It distinguishes an external title hit from an
actual missing public identity and applies a small curated hierarchy map for
containers that do not have the same grain as the internal work catalogue.
The output contains titles because it is a local review artifact; no output of
this script is shipped in the public discovery database.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

try:
    from scripts.discovery_v4_common import (
        iter_sefaria_titles,
        load_source_config,
        normalize_title,
        require_hash,
        sha256_file,
        source_target_ids,
        stable_json_dump,
    )
except ModuleNotFoundError:  # direct ``python scripts/...py`` invocation
    from discovery_v4_common import (
        iter_sefaria_titles,
        load_source_config,
        normalize_title,
        require_hash,
        sha256_file,
        source_target_ids,
        stable_json_dump,
    )


def _load_sefaria(path: Path) -> tuple[list[dict], dict[str, list[dict]]]:
    doc = json.loads(path.read_text(encoding="utf-8"))
    rows = list(iter_sefaria_titles(doc))
    by_title: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        for title in (row.get("title"), row.get("he_title")):
            if title:
                by_title[normalize_title(title)].append(row)
    return rows, dict(by_title)


def _load_wikisource(path: Path) -> tuple[int, dict[str, list[str]]]:
    by_title: dict[str, list[str]] = defaultdict(list)
    count = 0
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        header = next(stream, "").strip()
        if header != "page_title":
            raise ValueError("unexpected Wikisource title-dump header")
        for line in stream:
            title = line.rstrip("\n").replace("_", " ")
            if not title:
                continue
            count += 1
            by_title[normalize_title(title)].append(title)
    return count, dict(by_title)


def _work_rows(conn: sqlite3.Connection) -> list[dict]:
    stats = {
        row[0]: {
            "identification_count": int(row[1]),
            "main_pool_count": int(row[2]),
            "fragment_count": int(row[3]),
        }
        for row in conn.execute(
            """
            SELECT canonical_work_id, COUNT(*), SUM(main_pool), COUNT(DISTINCT sys_id)
            FROM discovery_identification
            GROUP BY canonical_work_id
            """
        )
    }
    rows = []
    for row in conn.execute(
        """
        SELECT work_id, canonical_work_id, neutral_title, author, genre,
               source_corpus, identity_visibility
        FROM works
        ORDER BY work_id
        """
    ):
        work_id, canonical_work_id, title, author, genre, corpus, visibility = row
        rows.append(
            {
                "work_id": work_id,
                "canonical_work_id": canonical_work_id,
                "title": title,
                "author": author,
                "genre": genre,
                "source_corpus": corpus,
                "identity_visibility": visibility,
                **stats.get(
                    canonical_work_id,
                    {
                        "identification_count": 0,
                        "main_pool_count": 0,
                        "fragment_count": 0,
                    },
                ),
            }
        )
    return rows


def run(args: argparse.Namespace) -> dict:
    private_db = Path(args.private_db)
    sefaria_path = Path(args.sefaria_index)
    wiki_path = Path(args.wikisource_titles)
    source_map_path = Path(args.source_map)
    for path, label in (
        (private_db, "private discovery database"),
        (sefaria_path, "Sefaria index"),
        (wiki_path, "Wikisource title dump"),
        (source_map_path, "V4 source map"),
    ):
        if not path.is_file():
            raise FileNotFoundError(f"{label} not found: {path}")
    require_hash(sefaria_path, args.sefaria_sha256, "Sefaria index")
    require_hash(wiki_path, args.wikisource_sha256, "Wikisource title dump")

    config = load_source_config(source_map_path)
    sefaria_rows, sefaria_by_title = _load_sefaria(sefaria_path)
    wiki_count, wiki_by_title = _load_wikisource(wiki_path)

    conn = sqlite3.connect(f"file:{private_db.as_posix()}?mode=ro", uri=True)
    try:
        works = _work_rows(conn)
    finally:
        conn.close()
    works_by_id = {row["work_id"]: row for row in works}
    public_by_title: dict[str, list[dict]] = defaultdict(list)
    for row in works:
        if row["identity_visibility"] == "public":
            public_by_title[normalize_title(row["title"])].append(row)

    planned = source_target_ids(config)
    aggregate = {
        item["work_id"]: item for item in config.get("aggregate_already_covered", [])
    }
    false_hits = {
        item["work_id"]: item for item in config.get("false_title_collisions", [])
    }
    overlap = (planned & set(aggregate)) | (planned & set(false_hits)) | (
        set(aggregate) & set(false_hits)
    )
    if overlap:
        raise ValueError(f"V4 source-map dispositions overlap: {sorted(overlap)}")
    for work_id in planned | set(aggregate) | set(false_hits):
        row = works_by_id.get(work_id)
        if row is None:
            raise ValueError(f"curated work is absent from private DB: {work_id}")
        if row["identity_visibility"] != "private":
            raise ValueError(f"curated target is not private: {work_id}")
    for item in aggregate.values():
        for covered_id in item["covered_by"]:
            covered = works_by_id.get(covered_id)
            if covered is None or covered["identity_visibility"] != "public":
                raise ValueError(
                    f"aggregate coverage member is absent/non-public: {covered_id}"
                )

    source_by_work: dict[str, dict] = {}
    source_checks = []
    for source in config["sources"]:
        catalogue = sefaria_by_title if source["provider"] == "sefaria" else wiki_by_title
        present = bool(catalogue.get(normalize_title(source["source_ref"])))
        if not present:
            raise ValueError(
                f"curated {source['provider']} source is absent from pinned catalogue: "
                f"{source['source_ref']}"
            )
        source_checks.append(
            {
                "key": source["key"],
                "provider": source["provider"],
                "source_ref": source["source_ref"],
                "catalogue_present": present,
                "target_count": len(source["mappings"]),
            }
        )
        for mapping in source["mappings"]:
            source_by_work[mapping["target_work_id"]] = {
                "source_key": source["key"],
                "source_provider": source["provider"],
                "source_ref": source["source_ref"],
                "chapter_range": mapping.get("chapter_range"),
            }

    audit_rows = []
    for work in works:
        if work["identity_visibility"] != "private":
            continue
        key = normalize_title(work["title"])
        sefaria_matches = sefaria_by_title.get(key, [])
        wiki_matches = wiki_by_title.get(key, [])
        public_matches = public_by_title.get(key, [])
        work_id = work["work_id"]
        if work_id in source_by_work:
            disposition = "planned_public_source"
            note = "Curated high-confidence public source target."
        elif work_id in aggregate:
            disposition = "aggregate_already_covered"
            note = aggregate[work_id]["note"]
        elif work_id in false_hits:
            disposition = "false_title_collision"
            note = false_hits[work_id]["note"]
        elif public_matches:
            disposition = "already_public_same_title"
            note = "A public internal work already has the same normalized title."
        elif sefaria_matches or wiki_matches:
            disposition = "unreviewed_exact_external_hit"
            note = "Exact title hit requires identity and license review."
        else:
            disposition = "no_exact_external_hit"
            note = "No exact normalized title in either pinned catalogue."
        audit_rows.append(
            {
                **work,
                "normalized_title": key,
                "sefaria_exact_titles": sorted(
                    {row.get("title") or "" for row in sefaria_matches if row.get("title")}
                ),
                "wikisource_exact_titles": sorted(set(wiki_matches)),
                "public_same_title_work_ids": sorted(
                    row["work_id"] for row in public_matches
                ),
                "disposition": disposition,
                "note": note,
                **source_by_work.get(work_id, {}),
            }
        )

    audit_rows.sort(
        key=lambda row: (
            row["disposition"] != "planned_public_source",
            -row["main_pool_count"],
            -row["identification_count"],
            row["work_id"],
        )
    )
    disposition_counts = Counter(row["disposition"] for row in audit_rows)
    planned_rows = [
        row for row in audit_rows if row["disposition"] == "planned_public_source"
    ]
    report = {
        "schema_version": "discovery-v4-catalog-audit-v1",
        "inputs": {
            "private_db": str(private_db.resolve()),
            "private_db_sha256": sha256_file(private_db),
            "sefaria_index": str(sefaria_path.resolve()),
            "sefaria_index_sha256": sha256_file(sefaria_path),
            "wikisource_titles": str(wiki_path.resolve()),
            "wikisource_titles_sha256": sha256_file(wiki_path),
            "source_map": str(source_map_path.resolve()),
            "source_map_sha256": sha256_file(source_map_path),
        },
        "catalogues": {
            "sefaria_title_nodes": len(sefaria_rows),
            "wikisource_namespace0_titles": wiki_count,
        },
        "summary": {
            "private_works": len(audit_rows),
            "planned_target_work_ids": len(planned),
            "planned_external_sources": len(config["sources"]),
            "planned_identifications": sum(
                row["identification_count"] for row in planned_rows
            ),
            "planned_main_pool_identifications": sum(
                row["main_pool_count"] for row in planned_rows
            ),
            "planned_fragments": sum(row["fragment_count"] for row in planned_rows),
            "dispositions": dict(sorted(disposition_counts.items())),
        },
        "source_checks": source_checks,
        "works": audit_rows,
    }
    output_json = Path(args.output_json)
    output_csv = Path(args.output_csv)
    output_md = Path(args.output_md)
    for output in (output_json, output_csv, output_md):
        output.parent.mkdir(parents=True, exist_ok=True)
    stable_json_dump(report, output_json)
    columns = [
        "work_id",
        "title",
        "author",
        "genre",
        "identification_count",
        "main_pool_count",
        "fragment_count",
        "disposition",
        "source_key",
        "source_provider",
        "source_ref",
        "chapter_range",
        "note",
    ]
    with output_csv.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in audit_rows:
            csv_row = dict(row)
            if csv_row.get("chapter_range"):
                csv_row["chapter_range"] = "-".join(
                    str(value) for value in csv_row["chapter_range"]
                )
            writer.writerow(csv_row)
    summary = report["summary"]
    lines = [
        "# Discovery V4 public-catalogue audit",
        "",
        f"- Private works audited: {summary['private_works']:,}",
        f"- Planned target identities: {summary['planned_target_work_ids']:,}",
        f"- External source containers: {summary['planned_external_sources']:,}",
        f"- Current identifications on those targets: {summary['planned_identifications']:,}",
        f"- Current main-pool identifications: {summary['planned_main_pool_identifications']:,}",
        "",
        "## Dispositions",
        "",
    ]
    lines.extend(
        f"- `{name}`: {count:,}"
        for name, count in sorted(summary["dispositions"].items())
    )
    lines.extend(["", "## Planned targets", "", "| Work | Title | Provider | Source | IDs | Main |", "|---|---|---|---|---:|---:|"])
    for row in planned_rows:
        title = str(row["title"]).replace("|", "\\|")
        source_ref = str(row["source_ref"]).replace("|", "\\|")
        lines.append(
            f"| {row['work_id']} | {title} | {row['source_provider']} | "
            f"{source_ref} | {row['identification_count']:,} | {row['main_pool_count']:,} |"
        )
    output_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--private-db", required=True)
    parser.add_argument("--sefaria-index", required=True)
    parser.add_argument("--sefaria-sha256", required=True)
    parser.add_argument("--wikisource-titles", required=True)
    parser.add_argument("--wikisource-sha256", required=True)
    parser.add_argument(
        "--source-map",
        default=str(Path(__file__).with_name("discovery_v4_sources.json")),
    )
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--output-md", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    result = run(parse_args())
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
