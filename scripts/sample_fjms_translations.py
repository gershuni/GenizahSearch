"""Draw a reproducible random sample of FJMS automatic translations for a quality audit.

Catalogue text search (docs/plans/CATALOGUE_TEXT_SEARCH_PLAN.md, decision D2) searches the
automatic translations in ``fjms_translations``, and up to ~40% of its hits rest on them. The PGP
Hebrew translations were withheld after a sampled audit found 27.8% of them wrong
(docs/plans/PGP_TRANSLATION_QUALITY.md); this draws the same kind of sample for FJMS, so the two
rates can be compared. Grading follows that plan's method: every row graded good / minor / wrong /
garbage against its original, every adverse grade re-judged by a second reader told to overturn
stylistic nitpicking.

Two populations:

- ``--sidecar catalog_data/catalog_text.db`` (use this one): exactly what search indexes. Records
  are drawn uniformly from the built sidecar's automatic FJMS records, and each is traced back to
  the ``fjms_translations`` row it came from, for the original text. A translation the build left
  out (a duplicate of an original, a placeholder, an entity that leads to no manuscript) cannot
  be drawn, and a work-title or author translation that went to many manuscripts counts once per
  manuscript, as search meets it.
- without ``--sidecar``: the ``fjms_translations`` rows themselves, uniformly by row, minus the
  person descriptions and bibliography titles (``EXCLUDED_FIELDS``, as the builder). The first
  audit (2026-11-05, seed 20261105) used this population; 146 of its 150 rows turned out to enter
  the sidecar (plan §3.2).

Usage::

    python scripts/sample_fjms_translations.py --db fist_data/fjms_enrichment.db \
        --sidecar catalog_data/catalog_text.db --out sample.jsonl [--n 150] [--seed 20261105]

Both files are opened read-only. The output is JSON Lines, one row per sampled translation.
"""
from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_catalog_text_sidecar import (  # noqa: E402  -- the builder's own rules
    EXCLUDED_TRANSLATION_FIELDS as EXCLUDED_FIELDS,
)
from scripts.build_catalog_text_sidecar import PLACEHOLDER_FIELDS, drop_placeholders  # noqa: E402

MAX_CHARS = 2000


def _ro(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)


def _row(row_id, sys_id, field, direction, model, original, translated, record_id=None) -> dict:
    out = {
        "id": row_id,
        "sys_id": sys_id,
        "field": field,
        "direction": direction,
        "model": model,
        "original": (original or "")[:MAX_CHARS],
        "translation": (translated or "")[:MAX_CHARS],
        "truncated": len(original or "") > MAX_CHARS or len(translated or "") > MAX_CHARS,
    }
    if record_id is not None:
        out["record_id"] = record_id
    return out


def draw(db_path: Path, n: int, seed: int) -> list[dict]:
    """``n`` rows of ``fjms_translations`` (the table population)."""
    conn = _ro(db_path)
    try:
        placeholders = ",".join("?" * len(EXCLUDED_FIELDS))
        ids = [
            row[0]
            for row in conn.execute(
                f"SELECT id FROM fjms_translations WHERE field_name NOT IN ({placeholders}) "
                "AND translated_text IS NOT NULL AND translated_text <> '' ORDER BY id",
                EXCLUDED_FIELDS,
            )
        ]
        if len(ids) < n:
            raise SystemExit(f"only {len(ids)} eligible rows, asked for {n}")
        out = []
        for row_id in sorted(random.Random(seed).sample(ids, n)):
            alma_id, field, direction, original, translated, model = conn.execute(
                "SELECT alma_id, field_name, direction, original_text, translated_text, "
                "model_version FROM fjms_translations WHERE id = ?",
                (row_id,),
            ).fetchone()
            out.append(_row(row_id, alma_id, field, direction, model, original, translated))
        return out
    finally:
        conn.close()


def _stored_text(field: str, translated: str) -> str:
    """The text the builder stores for a translation (``Writer.add``)."""
    text = (translated or "").strip()
    return drop_placeholders(text) if field in PLACEHOLDER_FIELDS else text


def draw_from_sidecar(db_path: Path, sidecar_path: Path, n: int, seed: int) -> list[dict]:
    """``n`` of the sidecar's automatic FJMS records, each traced to its ``fjms_translations`` row."""
    side = _ro(sidecar_path)
    conn = _ro(db_path)
    try:
        ids = [r[0] for r in side.execute(
            "SELECT id FROM records WHERE source = 'fjms' AND automatic = 1 ORDER BY id")]
        if len(ids) < n:
            raise SystemExit(f"only {len(ids)} automatic FJMS records, asked for {n}")
        out = []
        for record_id in sorted(random.Random(seed).sample(ids, n)):
            sys_id, field, text = side.execute(
                "SELECT sys_id, field, text FROM records WHERE id = ?", (record_id,)).fetchone()
            # The keys the translation may be stored under (the builder's ENTITY_KEYED_TRANSLATIONS).
            if field == "GenizahTitleEngTitle":
                keys = [str(r[0]) for r in conn.execute(
                    "SELECT DISTINCT GenizahTitleId FROM catalog WHERE AlmaId = ? AND GenizahTitleId IS NOT NULL",
                    (sys_id,))]
            elif field == "AuthorText":
                keys = [r[0] for r in conn.execute(
                    "SELECT DISTINCT AuthorText FROM catalog WHERE AlmaId = ? AND AuthorText IS NOT NULL",
                    (sys_id,))]
            else:
                keys = [sys_id]
            # Every key at once, in id order: the builder reads translations by id and keeps the
            # first that gives a manuscript a text, so the lowest matching id is the record's source
            # (whatever order the catalogue lists a manuscript's titles or authors in).
            source = None
            marks = ",".join("?" * len(keys))
            for row in conn.execute(
                "SELECT id, direction, original_text, translated_text, model_version FROM fjms_translations "
                f"WHERE field_name = ? AND alma_id IN ({marks}) ORDER BY id", (field, *keys)) if keys else ():
                if _stored_text(field, row[3]) == text:
                    source = row
                    break
            if source is None:
                raise SystemExit(f"record {record_id} ({sys_id}, {field}) has no fjms_translations row: "
                                 "the sidecar was not built from this FJMS file")
            row_id, direction, original, translated, model = source
            out.append(_row(row_id, sys_id, field, direction, model, original, translated, record_id))
        return out
    finally:
        conn.close()
        side.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", type=Path, required=True, help="the FJMS sidecar (fjms_enrichment.db)")
    parser.add_argument("--sidecar", type=Path, help="the built catalog_text.db: sample what search indexes")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--n", type=int, default=150)
    parser.add_argument("--seed", type=int, default=20261105)
    args = parser.parse_args(argv)
    for path in (args.db, args.sidecar):
        if path is not None and (not path.is_file() or path.stat().st_size == 0):
            print(f"not a sidecar file: {path}", file=sys.stderr)
            return 2
    if args.sidecar is not None:
        rows = draw_from_sidecar(args.db, args.sidecar, args.n, args.seed)
    else:
        rows = draw(args.db, args.n, args.seed)
    with args.out.open("w", encoding="utf-8", newline="\n") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    by_dir: dict[str, int] = {}
    for row in rows:
        by_dir[row["direction"] or "?"] = by_dir.get(row["direction"] or "?", 0) + 1
    population = "sidecar records" if args.sidecar is not None else "translation rows"
    print(f"{len(rows)} {population} -> {args.out} ({by_dir})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
