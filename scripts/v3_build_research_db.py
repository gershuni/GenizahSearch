"""Build the SLIM research DB the discovery-v3 bake feeds to the sidecar builder.

`scripts/build_discovery_sidecar.py` reads a "research DB" through exactly two
tables -- `track1_matches` and `pages` -- via `select_shown_works`,
`_count_tier_a_rows`, `PageTextIndex` and `_compute_htr_snapshot_hash`. This
script materialises those two tables from the gen-2 artifacts, and nothing else.

WHY A SLIM DB RATHER THAN THE CORPUS FILE (this is a containment boundary, not
tidiness). The gen-2 corpus file also contains a table literally named
`track1_matches` -- the OLD-engine one -- plus `track1_matches_rs*`. The builder
resolves that name blindly, so handing it the corpus file would silently feed it
v2-era rows including **349 R-source works**, which are excluded from v3 by
decision. `select_shown_works` has no prefix rejection: a stray restricted-corpus
row would be classified through the ordinary `cat`/genre path and ship. So the
name `track1_matches` is materialised HERE from the gen-2 rows only, and the
build never opens the corpus file again.

Two derived columns, because the gen-2 match table lacks them:

* `shadowed_by` -- gen-2 records shadowing on EVIDENCE rows, not match rows. The
  producer (`gen2_shadow.py::shadow_pass`) builds its competition unit at
  `(claim_id, ref_work)` and updates all of a unit's evidence rows together, so
  the value is a property of the unit. We therefore aggregate at the producer's
  own grain and **HALT on any unit whose rows disagree** rather than silently
  reducing with ANY/ALL. Measured on `g_launch3`: zero mixed units today
  (105,447 wholly shadowed, 275,894 wholly unshadowed) -- but that is one run's
  property, not a guarantee, so the check is enforced rather than assumed.
* `sys_id` -- gen-2's `page_id` embeds it as the leading segment
  (`{sys_id}_IE…_P…_FL…`, verified on 100.00% of 198,238 distinct page_ids), so
  it is a split, not a lookup. The gen-2 table also carries `sys_id` directly;
  we prefer the stored value and assert it agrees with the embedded one.

MASKING (D-25). The gen-2 match table carries a column whose NAME is the
restricted corpus's signature vocabulary. The builder never reads it, so it is
DROPPED here, and an explicit denylist refuses to emit any column matching the
forbidden names -- a fail-closed guard rather than a comment, because the
masking scanner's pattern set is owner-held and cannot be relied on to carry
every fingerprint form (measured 2026-08-06: the bare Hebrew form is unusable as
a scan pattern, so this denylist is the operative control for that class).

Idempotent by construction: writes a NEW file at `--out` (refusing to overwrite
unless `--force`), never mutates a source. Safe to re-run after an interrupted
attempt, which is what `scripts/v3_bake_state.py` requires of a step.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

# The gen-2 match table that IS the v3 population. Named explicitly: resolving
# a bare `track1_matches` against the corpus file is the failure this guards.
GEN2_MATCH_TABLE = "track1_matches_pilot_glaunch3_live"

# Columns the sidecar builder actually reads (Codex HIGH: the full list, not a
# count). Everything else is deliberately not carried.
TRACK1_COLUMNS: Tuple[str, ...] = (
    "page_id", "sys_id", "work_id", "cat", "genre", "author", "title",
    "matched_letters", "best_density", "n_spans", "spans_json", "shadowed_by",
)
PAGES_COLUMNS: Tuple[str, ...] = (
    "page_id", "sys_id", "buckets", "n_chars", "text", "provenance",
    "fgp_id", "fgp_score", "htr_n_chars",
)

# D-25: column names that must never be emitted. Substring match, casefolded.
# `src_attr_note` is the neutral replacement used in fixtures.
FORBIDDEN_COLUMN_SUBSTRINGS: Tuple[str, ...] = ("mesir",)

# Work-id prefixes. `RS:` is R-source: excluded from v3 by decision (gen-2 never
# matched it; its rows exist only in the v2-era table).
EXCLUDED_WORK_PREFIXES: Tuple[str, ...] = ("RS:",)


class ResearchDbError(RuntimeError):
    """Fail-closed error building the slim research DB."""


def assert_no_forbidden_columns(columns: Iterable[str]) -> None:
    """D-25 compensating control (gate 16). Refuse a forbidden column NAME."""
    bad = [
        c for c in columns
        if any(token in c.casefold() for token in FORBIDDEN_COLUMN_SUBSTRINGS)
    ]
    if bad:
        raise ResearchDbError(
            f"refusing to emit {len(bad)} column(s) whose name is restricted-corpus "
            f"signature vocabulary (D-25). Rename to a neutral name such as "
            f"'src_attr_note'. Offending column count: {len(bad)}"
        )


def derive_shadowed_by(evidence_db: str) -> Dict[Tuple[str, str], str]:
    """Return `{(page_id, ref_work): shadowed_by}` for wholly-shadowed units.

    Aggregated at the PRODUCER's `(claim_id, ref_work)` grain and then keyed by
    `(page_id, ref_work)` for the match table. **HALTS on a mixed unit** (gate
    11): a unit whose evidence rows disagree has no defined match-row value, and
    reducing it with an undocumented ANY/ALL is how such a semantic silently
    drifts. Only wholly-shadowed units appear in the result; absent means NULL
    (unshadowed), which is what `WHERE shadowed_by IS NULL` selects.
    """
    conn = sqlite3.connect(f"file:{Path(evidence_db).resolve().as_uri()[8:]}?mode=ro", uri=True)
    try:
        conn.execute("PRAGMA cache_size=-300000")
        rows = conn.execute(
            """
            SELECT cl.page_id, e.ref_work,
                   SUM(CASE WHEN e.shadowed_by IS NULL THEN 1 ELSE 0 END) AS n_unshadowed,
                   COUNT(*) AS n_total,
                   MIN(e.shadowed_by) AS a_shadow
            FROM discovery_evidence e
            JOIN discovery_claim cl ON cl.claim_id = e.claim_id
            GROUP BY e.claim_id, e.ref_work
            """
        ).fetchall()
    finally:
        conn.close()

    shadowed: Dict[Tuple[str, str], str] = {}
    mixed: List[Tuple[str, str]] = []
    for page_id, ref_work, n_unshadowed, n_total, a_shadow in rows:
        if n_unshadowed == n_total:
            continue                      # wholly unshadowed -> NULL
        if n_unshadowed != 0:
            mixed.append((page_id, ref_work))
            continue
        shadowed[(page_id, ref_work)] = a_shadow
    if mixed:
        raise ResearchDbError(
            f"{len(mixed)} competition unit(s) are MIXED (some evidence rows shadowed, "
            f"some not). The producer updates a unit's rows together, so a mixed unit "
            f"means the producer changed or the grain assumption is wrong -- halting "
            f"rather than reducing with ANY/ALL. First key: {mixed[0][0][:24]}…"
        )
    return shadowed


def build(corpus_db: str, evidence_db: str, out_path: str, *, force: bool = False) -> Dict:
    out = Path(out_path)
    if out.exists() and not force:
        raise ResearchDbError(f"refusing to overwrite an existing file: {out} (use --force)")

    assert_no_forbidden_columns(TRACK1_COLUMNS + PAGES_COLUMNS)
    shadowed = derive_shadowed_by(evidence_db)

    src = sqlite3.connect(
        f"file:{Path(corpus_db).resolve().as_uri()[8:]}?mode=ro", uri=True
    )
    tmp = out.with_suffix(out.suffix + ".partial")
    if tmp.exists():
        tmp.unlink()                       # a prior interrupted attempt
    dst = sqlite3.connect(str(tmp))
    stats: Dict = {}
    try:
        src.execute("PRAGMA cache_size=-300000")
        have = {r[1] for r in src.execute(f"PRAGMA table_info({GEN2_MATCH_TABLE})")}
        assert_no_forbidden_columns(have & set(TRACK1_COLUMNS))
        missing = [c for c in TRACK1_COLUMNS if c not in have and c != "shadowed_by"]
        if missing:
            raise ResearchDbError(
                f"gen-2 match table {GEN2_MATCH_TABLE} lacks required column(s): {missing}"
            )

        dst.execute("PRAGMA journal_mode=OFF")
        dst.execute("PRAGMA synchronous=OFF")
        dst.execute(
            "CREATE TABLE track1_matches ("
            "page_id TEXT, sys_id TEXT, work_id TEXT, cat TEXT, genre TEXT, author TEXT, "
            "title TEXT, matched_letters INT, best_density REAL, n_spans INT, "
            "spans_json TEXT, shadowed_by TEXT)"
        )
        dst.execute(
            "CREATE TABLE pages (page_id TEXT PRIMARY KEY, sys_id TEXT, buckets TEXT, "
            "n_chars INTEGER, text TEXT, provenance TEXT, fgp_id INTEGER, "
            "fgp_score REAL, htr_n_chars INTEGER)"
        )

        read_cols = [c for c in TRACK1_COLUMNS if c != "shadowed_by"]
        excluded = sys_mismatch = 0
        batch: List[Tuple] = []
        n_match = 0
        for row in src.execute(
            f"SELECT {', '.join(read_cols)} FROM {GEN2_MATCH_TABLE}"
        ):
            rec = dict(zip(read_cols, row))
            work_id = rec["work_id"] or ""
            if work_id.startswith(EXCLUDED_WORK_PREFIXES):
                excluded += 1
                continue
            page_id = rec["page_id"] or ""
            embedded = page_id.split("_", 1)[0]
            if rec.get("sys_id") and embedded and rec["sys_id"] != embedded:
                sys_mismatch += 1
            batch.append(tuple(rec[c] for c in read_cols)
                         + (shadowed.get((page_id, work_id)),))
            if len(batch) >= 20000:
                dst.executemany(
                    f"INSERT INTO track1_matches ({', '.join(TRACK1_COLUMNS)}) "
                    f"VALUES ({', '.join('?' * len(TRACK1_COLUMNS))})", batch)
                n_match += len(batch)
                batch.clear()
        if batch:
            dst.executemany(
                f"INSERT INTO track1_matches ({', '.join(TRACK1_COLUMNS)}) "
                f"VALUES ({', '.join('?' * len(TRACK1_COLUMNS))})", batch)
            n_match += len(batch)
        if sys_mismatch:
            raise ResearchDbError(
                f"{sys_mismatch} match row(s) carry a sys_id disagreeing with the one "
                f"embedded in page_id -- the manuscript-axis assumption is violated"
            )

        n_pages = 0
        batch.clear()
        for row in src.execute(f"SELECT {', '.join(PAGES_COLUMNS)} FROM pages"):
            batch.append(row)
            if len(batch) >= 5000:
                dst.executemany(
                    f"INSERT OR REPLACE INTO pages ({', '.join(PAGES_COLUMNS)}) "
                    f"VALUES ({', '.join('?' * len(PAGES_COLUMNS))})", batch)
                n_pages += len(batch)
                batch.clear()
        if batch:
            dst.executemany(
                f"INSERT OR REPLACE INTO pages ({', '.join(PAGES_COLUMNS)}) "
                f"VALUES ({', '.join('?' * len(PAGES_COLUMNS))})", batch)
            n_pages += len(batch)

        dst.execute("CREATE INDEX ix_t1_work ON track1_matches(work_id, page_id)")
        dst.execute("CREATE INDEX ix_t1_shadow ON track1_matches(shadowed_by)")
        dst.commit()

        # Post-build containment assertions (gate 12): prove the emitted DB is
        # clean rather than trusting the filter that produced it.
        leaked = dst.execute(
            "SELECT COUNT(*) FROM track1_matches WHERE work_id LIKE 'RS:%'"
        ).fetchone()[0]
        if leaked:
            raise ResearchDbError(f"{leaked} R-source row(s) reached the slim DB")
        assert_no_forbidden_columns(
            {r[1] for r in dst.execute("PRAGMA table_info(track1_matches)")}
            | {r[1] for r in dst.execute("PRAGMA table_info(pages)")}
        )
        tier_a = dst.execute(
            "SELECT COUNT(*) FROM track1_matches WHERE shadowed_by IS NULL"
        ).fetchone()[0]
        stats = {
            "track1_matches": n_match,
            "pages": n_pages,
            "tier_a_unshadowed": tier_a,
            "shadowed_units": len(shadowed),
            "rsource_rows_excluded": excluded,
        }
    finally:
        src.close()
        dst.close()

    os.replace(tmp, out)                   # atomic publish
    return stats


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Build the slim v3 research DB")
    ap.add_argument("--corpus-db", required=True, help="gen-2 corpus DB (pages + match table)")
    ap.add_argument("--evidence-db", required=True, help="gen-2 evidence DB (for shadowing)")
    ap.add_argument("--out", required=True, help="destination slim DB (must not exist)")
    ap.add_argument("--force", action="store_true", help="overwrite --out if present")
    args = ap.parse_args(argv)
    try:
        stats = build(args.corpus_db, args.evidence_db, args.out, force=args.force)
    except ResearchDbError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    for key, value in stats.items():
        print(f"  {key:26s} {value}")
    print("slim research DB OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
