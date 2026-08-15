#!/usr/bin/env python3
"""Append acquired V4 texts to the frozen V2 reference corpus.

The append is intentionally narrow: existing reference objects are copied byte
for semantic byte after deserialization, and only acquired/allowlisted V4
sources are added.  The same run extends the pinned locus-division database so
new matcher offsets and locus offsets share one exact coordinate system.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pickle
import shutil
import sqlite3
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from shared.discovery_locus import daf_label_he, heb_numeral, sefaria_daf
from scripts.build_work_divisions import WorkUnits, build_staged_ja_chapters

try:
    from scripts.discovery_v4_common import (
        compact_stream,
        load_source_config,
        normalize_title,
        require_hash,
        sha256_file,
        stable_json_dump,
    )
except ModuleNotFoundError:  # direct ``python scripts/...py`` invocation
    from discovery_v4_common import (
        compact_stream,
        load_source_config,
        normalize_title,
        require_hash,
        sha256_file,
        stable_json_dump,
    )


def raw_reference_id(source_key: str, mapping: dict, mapping_count: int) -> str:
    if mapping_count == 1:
        return f"REF4:{source_key}"
    chapter_range = mapping.get("chapter_range")
    if chapter_range:
        suffix = f"{chapter_range[0]}_{chapter_range[1]}"
    else:
        suffix = mapping["target_work_id"]
    return f"REF4:{source_key}:{suffix}"


def select_units(units: list[dict], mapping: dict) -> list[dict]:
    selected = units
    if mapping.get("chapter_range"):
        lower, upper = (int(value) for value in mapping["chapter_range"])
        if lower < 1 or upper < lower:
            raise ValueError("invalid source chapter_range")
        selected = [
            unit for unit in units if lower <= int(unit["ordinal"]) <= upper
        ]
        observed = {int(unit["ordinal"]) for unit in selected}
        expected = set(range(lower, upper + 1))
        if observed != expected:
            missing = sorted(expected - observed)
            raise ValueError(f"chapter_range has missing source units: {missing[:8]}")
    if not selected:
        raise ValueError("source mapping selected no units")
    return selected


def _load_private_works(path: Path) -> dict[str, dict]:
    conn = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    try:
        return {
            row[0]: {
                "title": row[1],
                "author": row[2] or "",
                "genre": row[3] or "",
                "identity_visibility": row[4],
            }
            for row in conn.execute(
                "SELECT work_id, neutral_title, author, genre, identity_visibility FROM works"
            )
        }
    finally:
        conn.close()


def _unit_offsets(units: list[dict]) -> tuple[str, list[tuple[dict, int]]]:
    chunks = []
    rows = []
    offset = 0
    for unit in units:
        chunk = compact_stream(unit["text"])
        if not chunk:
            continue
        rows.append((unit, offset))
        chunks.append(chunk)
        offset += len(chunk)
    stream = "".join(chunks)
    if not stream:
        raise ValueError("selected source units normalize to an empty stream")
    return stream, rows


def _legacy_reference_metadata_key(corpus: list[dict]) -> str:
    """Return the research-only field between ``genre`` and ``stream``.

    The frozen matcher indexes that field directly on every reference row, but
    its name is restricted vocabulary and must not be copied into this public
    expansion's source code. Deriving it from the pinned base corpus preserves
    the matcher-compatible mapping shape without introducing a new literal.
    """
    if not corpus:
        raise ValueError("base reference corpus is empty")
    keys = list(corpus[0])
    try:
        genre_pos = keys.index("genre")
        stream_pos = keys.index("stream")
    except ValueError as exc:
        raise ValueError("base reference corpus lacks its frozen mapping shape") from exc
    if stream_pos != genre_pos + 2:
        raise ValueError("base reference corpus metadata-field position drift")
    key = keys[genre_pos + 1]
    if not isinstance(key, str) or not key:
        raise ValueError("base reference corpus metadata key is invalid")
    if any(key not in work for work in corpus):
        raise ValueError("base reference corpus metadata key is not universal")
    # The former reviewed builder used one module-literal key object for all
    # appended mappings. Reusing the unpickled base key changes pickle memo
    # opcodes despite producing equal Python objects, so make one fresh object
    # and reuse it to preserve byte-for-byte reference-corpus reproducibility.
    return key.encode("utf-8").decode("utf-8")


def _locus_label(
    source: dict, mapping: dict, work: dict, unit: dict, grain: str
) -> tuple[str, int]:
    ordinal = int(unit["ordinal"])
    if grain == "daf_bavli":
        daf, amud = sefaria_daf(ordinal)
        return daf_label_he(daf, amud), daf * 2 + amud - 1
    locus_title = source.get("locus_title_he") or work["title"]
    if grain == "section":
        unit_label = str(unit.get("label") or "").strip()
        if normalize_title(unit_label).startswith(normalize_title(locus_title)):
            return unit_label, ordinal
        return f"{locus_title}, {unit_label}", ordinal
    numeral = heb_numeral(ordinal) if 1 <= ordinal <= 999 else str(ordinal)
    return f"{locus_title} {numeral}", ordinal


def _extend_locus(
    *,
    base_db: Path,
    base_coverage: Path,
    output_db: Path,
    output_coverage: Path,
    new_reference_hash: str,
    reference_entries: list[dict],
    supplemental_works: list[WorkUnits] | None = None,
) -> dict:
    coverage = json.loads(base_coverage.read_text(encoding="utf-8"))
    if coverage.get("invariant_problems") != []:
        raise ValueError("base locus coverage reports invariant problems")
    output_db.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(base_db, output_db)
    conn = sqlite3.connect(output_db)
    added_works = 0
    added_units = 0
    grain_counts: Counter[str] = Counter()
    whole_work_refs = 0
    supplemental_added_works = 0
    supplemental_added_units = 0
    supplemental_works = list(supplemental_works or [])
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        for work in supplemental_works:
            exists = conn.execute(
                "SELECT 1 FROM locus_work WHERE locus_ref_id=?", (work.ref_id,)
            ).fetchone()
            if exists:
                continue
            conn.execute(
                "INSERT INTO locus_work VALUES (?,?,?,?,?)",
                (work.ref_id, work.family, work.grain, work.stream_len, len(work.units)),
            )
            conn.executemany(
                "INSERT INTO locus_unit VALUES (?,?,?,?,?,?)",
                [
                    (
                        work.ref_id,
                        unit.unit_ord,
                        unit.start,
                        unit.part_key,
                        unit.label_he,
                        unit.citation_pos,
                    )
                    for unit in work.units
                ],
            )
            supplemental_added_works += 1
            supplemental_added_units += len(work.units)
        for entry in reference_entries:
            unit_rows = entry["unit_offsets"]
            if len(unit_rows) <= 1:
                whole_work_refs += 1
                continue
            grain = entry["locus_grain"]
            raw_id = entry["raw_reference_id"]
            conn.execute(
                "INSERT INTO locus_work VALUES (?,?,?,?,?)",
                (raw_id, "sefaria", grain, entry["stream_len"], len(unit_rows)),
            )
            for unit_ord, row in enumerate(unit_rows):
                conn.execute(
                    "INSERT INTO locus_unit VALUES (?,?,?,?,?,?)",
                    (
                        raw_id,
                        unit_ord,
                        row["start_offset"],
                        f"{grain}:{row['source_ordinal']}",
                        row["label_he"],
                        row["citation_pos"],
                    ),
                )
            added_works += 1
            added_units += len(unit_rows)
            grain_counts[grain] += 1
        conn.commit()
        foreign_key_problems = conn.execute("PRAGMA foreign_key_check").fetchall()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        work_count = conn.execute("SELECT COUNT(*) FROM locus_work").fetchone()[0]
        unit_count = conn.execute("SELECT COUNT(*) FROM locus_unit").fetchone()[0]
        families = dict(
            conn.execute(
                "SELECT family, COUNT(*) FROM locus_work GROUP BY family ORDER BY family"
            ).fetchall()
        )
        grains = dict(
            conn.execute(
                "SELECT grain, COUNT(*) FROM locus_work GROUP BY grain ORDER BY grain"
            ).fetchall()
        )
    finally:
        conn.close()
    problems = []
    if integrity != "ok":
        problems.append(f"integrity_check={integrity}")
    if foreign_key_problems:
        problems.append(f"foreign_key_check={len(foreign_key_problems)}")
    if work_count != (
        int(coverage["works_with_units"]) + added_works + supplemental_added_works
    ):
        problems.append("work-count drift")
    if unit_count != (
        int(coverage["units_total"]) + added_units + supplemental_added_units
    ):
        problems.append("unit-count drift")
    new_coverage = {
        **coverage,
        "reference_corpus_sha256": new_reference_hash,
        "works_with_units": work_count,
        "units_total": unit_count,
        "by_family": families,
        "by_grain": grains,
        "invariant_problems": problems,
        "v4_extension": {
            "added_works_with_units": added_works,
            "added_units": added_units,
            "whole_work_fallback_refs": whole_work_refs,
            "added_by_grain": dict(sorted(grain_counts.items())),
        },
        "supplemental_structures": {
            "added_works_with_units": supplemental_added_works,
            "added_units": supplemental_added_units,
            "reference_ids": [work.ref_id for work in supplemental_works],
        },
    }
    stable_json_dump(new_coverage, output_coverage)
    if problems:
        raise ValueError(f"extended locus invariant problems: {problems}")
    return new_coverage


def run(args: argparse.Namespace) -> dict:
    base_ref = Path(args.base_reference)
    acquisition_manifest_path = Path(args.acquisition_manifest)
    normalized_dir = Path(args.normalized_dir)
    source_map_path = Path(args.source_map)
    private_db = Path(args.private_db)
    base_locus_db = Path(args.base_locus_db)
    base_locus_coverage = Path(args.base_locus_coverage)
    refs_staging = Path(args.refs_staging)
    output_ref = Path(args.output_reference)
    output_manifest = Path(args.output_manifest)
    output_locus_db = Path(args.output_locus_db)
    output_locus_coverage = Path(args.output_locus_coverage)
    require_hash(base_ref, args.base_reference_sha256, "base reference corpus")
    require_hash(
        acquisition_manifest_path,
        args.acquisition_manifest_sha256,
        "V4 acquisition manifest",
    )
    require_hash(base_locus_db, args.base_locus_sha256, "base locus database")
    require_hash(
        base_locus_coverage, args.base_locus_coverage_sha256, "base locus coverage"
    )
    config = load_source_config(source_map_path)
    config_by_key = {source["key"]: source for source in config["sources"]}
    acquisition = json.loads(acquisition_manifest_path.read_text(encoding="utf-8"))
    if acquisition.get("schema_version") != "discovery-v4-acquisition-manifest-v1":
        raise ValueError("unsupported acquisition manifest schema")
    if acquisition.get("source_map_sha256") != sha256_file(source_map_path):
        raise ValueError("acquisition manifest source-map hash mismatch")
    base_coverage = json.loads(base_locus_coverage.read_text(encoding="utf-8"))
    if str(base_coverage.get("reference_corpus_sha256", "")).lower() != (
        args.base_reference_sha256.lower()
    ):
        raise ValueError("base locus coverage does not describe the base reference")
    private_works = _load_private_works(private_db)
    with base_ref.open("rb") as stream:
        corpus = pickle.load(stream)
    if not isinstance(corpus, list):
        raise ValueError("base reference corpus must be a list")
    if not refs_staging.is_dir():
        raise FileNotFoundError(f"refs staging directory not found: {refs_staging}")
    staging_manifest_path = refs_staging / "manifest.json"
    if not staging_manifest_path.is_file():
        raise FileNotFoundError(f"refs staging manifest not found: {staging_manifest_path}")
    staging_entries = json.loads(
        staging_manifest_path.read_text(encoding="utf-8")
    ).get("entries", [])
    guide_entry = next(
        (entry for entry in staging_entries if entry.get("key") == "ja2_rambam_moreh"),
        None,
    )
    if guide_entry is None:
        raise ValueError("staging manifest lacks the Guide for the Perplexed source")
    shipped = {
        work["id"]: work["stream"]
        for work in corpus
        if isinstance(work, dict) and work.get("id") and work.get("stream") is not None
    }
    guide_units = build_staged_ja_chapters(
        guide_entry["key"],
        str(refs_staging),
        guide_entry.get("body_file", "ja2_rambam_moreh.txt"),
        "REF2:ja2_rambam_moreh",
        shipped,
    )
    if guide_units is None:
        raise ValueError("Guide chapter structure does not match its pinned stream")
    existing_ids = {work.get("id") for work in corpus if isinstance(work, dict)}
    legacy_metadata_key = _legacy_reference_metadata_key(corpus)
    reference_entries = []
    for acquired_entry in acquisition["entries"]:
        if acquired_entry.get("status") != "acquired":
            continue
        key = acquired_entry["key"]
        source = config_by_key.get(key)
        if source is None:
            raise ValueError(f"acquired source missing from source map: {key}")
        normalized_path = normalized_dir / acquired_entry["normalized_file"]
        require_hash(
            normalized_path, acquired_entry["normalized_sha256"], f"normalized {key}"
        )
        normalized = json.loads(normalized_path.read_text(encoding="utf-8"))
        if normalized.get("mappings") != source["mappings"]:
            raise ValueError(f"normalized source mapping drift: {key}")
        for mapping in source["mappings"]:
            target_id = mapping["target_work_id"]
            work = private_works.get(target_id)
            if work is None or work["identity_visibility"] != "private":
                raise ValueError(f"V4 target absent or non-private: {target_id}")
            units = select_units(normalized["units"], mapping)
            ref_id = raw_reference_id(key, mapping, len(source["mappings"]))
            if ref_id in existing_ids:
                raise ValueError(f"new reference id collides with base corpus: {ref_id}")
            stream, offsets = _unit_offsets(units)
            grain = source.get("locus_grain") or (
                "section" if source.get("mode") == "schema_leaves" else "chapter"
            )
            locus_rows = []
            for unit, start_offset in offsets:
                label, citation_pos = _locus_label(source, mapping, work, unit, grain)
                locus_rows.append(
                    {
                        "source_ordinal": int(unit["ordinal"]),
                        "start_offset": start_offset,
                        "label_he": label,
                        "citation_pos": citation_pos,
                    }
                )
            reference_work = {
                "id": ref_id,
                "cat": "Sefaria",
                "author": work["author"],
                "title": work["title"],
                "date": "",
                "genre": work["genre"],
                legacy_metadata_key: "",
                "stream": stream,
                "title_en": "",
                "provenance": normalized["provider"],
                "source_url": normalized["source_url"],
                "license": normalized["license"],
                "ref_kind": "public_reference",
                "vgroup": None,
                "split_parent": key if len(source["mappings"]) > 1 else None,
                "split_division": mapping.get("chapter_range"),
            }
            corpus.append(reference_work)
            existing_ids.add(ref_id)
            reference_entries.append(
                {
                    "raw_reference_id": ref_id,
                    "source_key": key,
                    "target_private_work_id": target_id,
                    "title": work["title"],
                    "provider": normalized["provider"],
                    "license": normalized["license"],
                    "source_url": normalized["source_url"],
                    "source_coverage_status": normalized.get(
                        "coverage_status", "complete"
                    ),
                    "source_missing_pages": normalized.get("missing_pages", []),
                    "stream_len": len(stream),
                    "stream_sha256": hashlib.sha256(stream.encode("utf-8")).hexdigest(),
                    "locus_grain": grain,
                    "unit_offsets": locus_rows,
                }
            )
    output_ref.parent.mkdir(parents=True, exist_ok=True)
    with output_ref.open("wb") as stream:
        pickle.dump(corpus, stream, protocol=4)
    output_ref_hash = sha256_file(output_ref)
    locus_coverage = _extend_locus(
        base_db=base_locus_db,
        base_coverage=base_locus_coverage,
        output_db=output_locus_db,
        output_coverage=output_locus_coverage,
        new_reference_hash=output_ref_hash,
        reference_entries=reference_entries,
        supplemental_works=[guide_units],
    )
    report = {
        "schema_version": "discovery-v4-reference-manifest-v1",
        "base_reference": str(base_ref.resolve()),
        "base_reference_sha256": sha256_file(base_ref),
        "reference_corpus": str(output_ref.resolve()),
        "reference_corpus_sha256": output_ref_hash,
        "base_work_count": len(corpus) - len(reference_entries),
        "new_reference_count": len(reference_entries),
        "total_work_count": len(corpus),
        "acquisition_manifest": str(acquisition_manifest_path.resolve()),
        "acquisition_manifest_sha256": sha256_file(acquisition_manifest_path),
        "source_map_sha256": sha256_file(source_map_path),
        "private_db_sha256": sha256_file(private_db),
        "locus_divisions": str(output_locus_db.resolve()),
        "locus_divisions_sha256": sha256_file(output_locus_db),
        "locus_coverage": str(output_locus_coverage.resolve()),
        "locus_coverage_sha256": sha256_file(output_locus_coverage),
        "locus_counts": {
            "works_with_units": locus_coverage["works_with_units"],
            "units_total": locus_coverage["units_total"],
        },
        "locus_supplemental_sources": {
            "refs_staging_manifest_sha256": sha256_file(staging_manifest_path),
            "guide_body_sha256": sha256_file(
                refs_staging / guide_entry.get("body_file", "ja2_rambam_moreh.txt")
            ),
            "guide_units": len(guide_units.units),
        },
        "entries": reference_entries,
    }
    stable_json_dump(report, output_manifest)
    print(
        json.dumps(
            {
                "reference_corpus_sha256": output_ref_hash,
                "new_reference_count": len(reference_entries),
                "total_work_count": len(corpus),
                "locus_works": locus_coverage["works_with_units"],
                "locus_units": locus_coverage["units_total"],
            },
            indent=2,
        )
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-reference", required=True)
    parser.add_argument("--base-reference-sha256", required=True)
    parser.add_argument("--acquisition-manifest", required=True)
    parser.add_argument("--acquisition-manifest-sha256", required=True)
    parser.add_argument("--normalized-dir", required=True)
    parser.add_argument("--private-db", required=True)
    parser.add_argument(
        "--source-map",
        default=str(Path(__file__).with_name("discovery_v4_sources.json")),
    )
    parser.add_argument("--base-locus-db", required=True)
    parser.add_argument("--base-locus-sha256", required=True)
    parser.add_argument("--base-locus-coverage", required=True)
    parser.add_argument("--base-locus-coverage-sha256", required=True)
    parser.add_argument("--refs-staging", required=True)
    parser.add_argument("--output-reference", required=True)
    parser.add_argument("--output-manifest", required=True)
    parser.add_argument("--output-locus-db", required=True)
    parser.add_argument("--output-locus-coverage", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
