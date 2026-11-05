"""Draw a reproducible random sample of FJMS automatic translations for a quality audit.

Catalogue text search (docs/plans/CATALOGUE_TEXT_SEARCH_PLAN.md, decision D2) searches the
automatic translations in ``fjms_translations``, and up to ~40% of its hits rest on them. The PGP
Hebrew translations were withheld after a sampled audit found 27.8% of them wrong
(docs/plans/PGP_TRANSLATION_QUALITY.md); this draws the same kind of sample for FJMS, so the two
rates can be compared. Grading follows that plan's method: every row graded good / minor / wrong /
garbage against its original, every adverse grade re-judged by a second reader told to overturn
stylistic nitpicking.

The sample covers exactly the rows the catalogue text sidecar indexes: every translated field
except the person descriptions (``PersonEngDesc`` / ``PersonHebDesc``), which describe people,
not manuscripts. Rows are drawn uniformly by row, which is how search meets them.

Usage::

    python scripts/sample_fjms_translations.py --db fist_data/fjms_enrichment.db \
        --out sample.jsonl [--n 150] [--seed 20261105]

The sidecar is opened read-only. The output is JSON Lines, one row per sampled translation.
"""
from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
from pathlib import Path

EXCLUDED_FIELDS = ("PersonEngDesc", "PersonHebDesc")
MAX_CHARS = 2000


def draw(db_path: Path, n: int, seed: int) -> list[dict]:
    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
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
        chosen = sorted(random.Random(seed).sample(ids, n))
        out = []
        for row_id in chosen:
            alma_id, field, direction, original, translated, model = conn.execute(
                "SELECT alma_id, field_name, direction, original_text, translated_text, "
                "model_version FROM fjms_translations WHERE id = ?",
                (row_id,),
            ).fetchone()
            out.append({
                "id": row_id,
                "sys_id": alma_id,
                "field": field,
                "direction": direction,
                "model": model,
                "original": (original or "")[:MAX_CHARS],
                "translation": (translated or "")[:MAX_CHARS],
                "truncated": len(original or "") > MAX_CHARS or len(translated or "") > MAX_CHARS,
            })
        return out
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--n", type=int, default=150)
    parser.add_argument("--seed", type=int, default=20261105)
    args = parser.parse_args(argv)
    if not args.db.is_file() or args.db.stat().st_size == 0:
        print(f"not a sidecar file: {args.db}", file=sys.stderr)
        return 2
    rows = draw(args.db, args.n, args.seed)
    with args.out.open("w", encoding="utf-8", newline="\n") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    by_dir: dict[str, int] = {}
    for row in rows:
        by_dir[row["direction"]] = by_dir.get(row["direction"], 0) + 1
    print(f"{len(rows)} rows -> {args.out} ({by_dir})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
