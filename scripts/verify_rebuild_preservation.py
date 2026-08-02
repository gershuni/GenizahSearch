#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Rebuild-preservation gate for the Phase-136 discovery sidecar rebuild (D-02b).

Proves that a rebuild of `discovery.db` cannot silently lose, add, or alter
anything OUTSIDE the explicitly authorized allowlist in
`docs/specs/discovery-sidecar-schema-v1.md`'s `## Amendment 2026-08-02
(Phase 136)` section. Two independent mechanisms, both required:

  1. An EXACT old/new allowlisted full-table diff over the six core tables
     (`works`, `discovery_claim`, `discovery_evidence`, `witness_units`,
     `witness_unit_members`, `discovery_routing_audit`): every pre-existing
     column byte-identical, ONLY the amendment's new/authorized-to-change
     columns permitted to differ. Streamed in primary-key order so peak
     memory never scales with table size (the asset carries ~268K claims /
     ~297K evidence rows).
  2. Recomputed frame/population/cluster-map hashes AND a CERT-01
     card-binding check, both compared against an EXTERNALLY pinned
     expectation file (`136-REBUILD-PRESERVATION-EXPECTED.json`, generated
     ONCE from the live asset before any rebuild exists) -- NEVER against the
     candidate build's own manifest (Codex F-04: a wrong rebuild is still
     internally self-consistent with itself).

A separate, dedicated (non-streaming -- the table is tiny) check covers
`band_precision`: every row must be byte-identical old vs new EXCEPT the one
authorized `tier_a` row, which may ONLY change `measurement_status`/`ci_low`
(D-02a) -- `precision` must stay NULL either way.

CLI:
    python scripts/verify_rebuild_preservation.py <old_db> <new_db> --expected <pinned.json>
    python scripts/verify_rebuild_preservation.py <old_db> --generate --out <pinned.json> \\
        --manifest <manifest.json> --research-db <fullcorpus_v2.db>

Masking discipline: a violation message names a table, a primary key, and a
column NAME -- NEVER a cell value (mirrors `scripts/build_discovery_sidecar.py`'s
`_validate_precision_spec` masking comments and `scripts/verify_discovery_sidecar.py`'s
violation-message convention).

Never raises for an expected data problem (a missing table, a schema drift, a
content mismatch) -- only for a genuine usage error (bad CLI args). Exit 0 on
a clean run, 1 (fail-closed) on any violation.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, FrozenSet, List, Optional, Sequence, Tuple

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPTS_DIR)
for _p in (_REPO_ROOT, _SCRIPTS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import cert01_frame as cf  # scripts/cert01_frame.py -- population_hash/cluster_map_hash/hash_file, IMPORTED
import build_discovery_sidecar as sidecar_build  # compute_frame_content_hash, the ONE canonical recipe

try:
    import bench_discovery as _bench  # scripts/bench_discovery.py -- portable current-RSS reader, reused

    def get_rss_bytes() -> int:
        return _bench.get_rss_bytes()
except Exception:  # pragma: no cover -- defensive; never block the gate on a memory-probe failure
    def get_rss_bytes() -> int:
        return -1


# ---------------------------------------------------------------------------
# The allowlist -- cites docs/specs/discovery-sidecar-schema-v1.md's
# "## Amendment 2026-08-02 (Phase 136)" section by sub-section letter.
# Nothing outside this list is authorized to differ between the pre-rebuild
# ("old") and post-rebuild ("new") asset for these six core tables.
# ---------------------------------------------------------------------------

SCHEMA_DOC_PATH = os.path.join(_REPO_ROOT, "docs", "specs", "discovery-sidecar-schema-v1.md")
SCHEMA_AMENDMENT_HEADER = "## Amendment 2026-08-02 (Phase 136)"

# (A) `discovery_evidence` additions: coverage_ppm/coverage_status (matched-
# letter coverage, direct-family only), band_rank (materialized sort key),
# novelty_status/novelty_source_label (tri-state novelty, all families), and
# assertion_visibility (VIS-01, per-evidence-row axis).
_DISCOVERY_EVIDENCE_ALLOWED = frozenset({
    "coverage_ppm", "coverage_status", "band_rank",
    "novelty_status", "novelty_source_label", "assertion_visibility",
})

# (A) identity_visibility lands on `works` (VIS-01, per-work axis) despite the
# sub-section header naming "discovery_evidence / discovery_claim additions".
# (C) `works.genre` -- an EXISTING column (no ADD COLUMN migration; see (C)'s
# own "does NOT add it" language) that goes from all-NULL to curated domain
# values in this ONE rebuild -- the single authorized VALUE-level change on an
# already-existing column.
_WORKS_ALLOWED = frozenset({"identity_visibility", "genre"})

# Despite sub-section (A)'s header naming "discovery_claim additions", reading
# the full amendment shows it defines NO new/changed discovery_claim column --
# every field it actually specifies lands on discovery_evidence or works.
_DISCOVERY_CLAIM_ALLOWED: FrozenSet[str] = frozenset()

_WITNESS_UNITS_ALLOWED: FrozenSet[str] = frozenset()
_WITNESS_UNIT_MEMBERS_ALLOWED: FrozenSet[str] = frozenset()

# (F) discovery_routing_audit -- `kept_tie` rows get `demoted_work_id`
# backfilled (previously NULL); the ONE authorized value-level change on this
# table (the audit row's PRIMARY KEY `id` and every OTHER column stay fixed).
_DISCOVERY_ROUTING_AUDIT_ALLOWED = frozenset({"demoted_work_id"})

ALLOWED_DIFFERING_COLUMNS: Dict[str, FrozenSet[str]] = {
    "works": _WORKS_ALLOWED,
    "discovery_claim": _DISCOVERY_CLAIM_ALLOWED,
    "discovery_evidence": _DISCOVERY_EVIDENCE_ALLOWED,
    "witness_units": _WITNESS_UNITS_ALLOWED,
    "witness_unit_members": _WITNESS_UNIT_MEMBERS_ALLOWED,
    "discovery_routing_audit": _DISCOVERY_ROUTING_AUDIT_ALLOWED,
}

# The six core tables this gate diffs (Task 1). `band_precision` is
# DELIBERATELY excluded from this generic per-column-allowlist mechanism --
# it carries exactly ONE authorized ROW-level exception (the tier_a
# measurement_status/ci_low pair, D-02a) rather than a column-level allowlist,
# so it gets its own small dedicated full-table check
# (`check_band_precision_authorized_change`), never the streamed mechanism
# below. `meta` is excluded on purpose (build-time bookkeeping, not asset
# content); `discovery_identification`/`manuscript_display` are BRAND NEW
# tables this rebuild adds (nothing to diff against in "old").
CORE_TABLES: Tuple[str, ...] = (
    "works",
    "discovery_claim",
    "discovery_evidence",
    "witness_units",
    "witness_unit_members",
    "discovery_routing_audit",
)

PRIMARY_KEYS: Dict[str, Tuple[str, ...]] = {
    "works": ("work_id",),
    "discovery_claim": ("page_id", "work_id"),
    "discovery_evidence": ("evidence_id",),
    "witness_units": ("unit_id",),
    # No literal PRIMARY KEY in the DDL; UNIQUE(sys_id) makes sys_id the
    # natural ordering/join key.
    "witness_unit_members": ("sys_id",),
    "discovery_routing_audit": ("id",),
}

# band_precision's natural key (never its autoincrement `id`, which a rebuild
# is free to renumber) -- (scope, collection_id, evidence_source,
# confidence_band), mirroring `scripts/verify_discovery_sidecar.py`'s own
# `_expected_band_keys()` key shape.
_BAND_PRECISION_KEY_COLS: Tuple[str, ...] = ("scope", "collection_id", "evidence_source", "confidence_band")
_BAND_PRECISION_COLUMNS: Tuple[str, ...] = (
    "scope", "collection_id", "evidence_source", "confidence_band", "numerator", "denominator",
    "precision", "ci_low", "ci_high", "method", "sampling_frame", "ins_policy", "weighting", "notes",
    "measurement_status", "measurement_date", "grader", "audit_status", "report_id",
)
_BAND_PRECISION_TIER_A_KEY: Tuple[str, str, str, str] = (
    "band", "e1_certification_registry_v1", "track1_direct", "tier_a",
)
# D-02a: the ONE authorized field-level exception on the ONE authorized row.
_BAND_PRECISION_TIER_A_ALLOWED_FIELDS = frozenset({"measurement_status", "ci_low"})


# ---------------------------------------------------------------------------
# Allowlist provenance -- read the schema doc's amendment section AT RUNTIME
# and confirm every allowlisted column name is actually cited there, so the
# allowlist can never silently drift ahead of (or behind) the frozen contract
# it claims to implement.
# ---------------------------------------------------------------------------

def _read_amendment_section_text(schema_doc_path: Optional[str] = None) -> str:
    path = schema_doc_path or SCHEMA_DOC_PATH
    text = Path(path).read_text(encoding="utf-8")
    idx = text.find(SCHEMA_AMENDMENT_HEADER)
    if idx == -1:
        raise RuntimeError(
            f"{path}: could not find {SCHEMA_AMENDMENT_HEADER!r} -- allowlist provenance check cannot run"
        )
    return text[idx:]


def check_allowlist_provenance(schema_doc_path: Optional[str] = None) -> List[str]:
    """Confirm every column this script allows to differ is named somewhere in
    the cited amendment section of the frozen schema doc. Returns a list of
    violation strings (empty = clean) -- degrades to a single reported
    violation (never raises) if the doc is unreadable, per the CLI's
    never-raise-on-an-expected-problem discipline."""
    try:
        section_text = _read_amendment_section_text(schema_doc_path)
    except (OSError, RuntimeError) as e:
        return [f"allowlist provenance: {e}"]
    violations: List[str] = []
    for table, cols in ALLOWED_DIFFERING_COLUMNS.items():
        for col in sorted(cols):
            pattern = re.compile(r"\b" + re.escape(col) + r"\b")
            if not pattern.search(section_text):
                violations.append(
                    f"allowlist provenance: column {table}.{col} is allowlisted to differ but does not "
                    f"appear in {SCHEMA_AMENDMENT_HEADER!r} of {os.path.basename(SCHEMA_DOC_PATH)}"
                )
    return violations


# ---------------------------------------------------------------------------
# Low-level DB helpers
# ---------------------------------------------------------------------------

def _connect_ro(db_path) -> sqlite3.Connection:
    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    return sqlite3.connect(uri, uri=True)


def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    return [row[1] for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


_NULL_SENTINEL = "\x00"
_FIELD_SEP = "\x1f"
_ROW_SEP = "\x1e"


def _cell_repr(v) -> str:
    return _NULL_SENTINEL if v is None else str(v)


def compute_table_hash(
    conn: sqlite3.Connection, table: str, compare_cols: Sequence[str], pk_cols: Sequence[str]
) -> Tuple[str, int]:
    """Stream every row of `table` in primary-key order, project onto
    `compare_cols`, and fold into one running SHA-256 -- never materializes
    the whole table (the sqlite3 cursor is iterated lazily, never
    `.fetchall()`'d), so peak memory stays O(1) per table regardless of row
    count."""
    col_csv = ", ".join(f'"{c}"' for c in compare_cols)
    order_csv = ", ".join(f'"{c}"' for c in pk_cols)
    cur = conn.execute(f'SELECT {col_csv} FROM "{table}" ORDER BY {order_csv}')  # noqa: S608 (PRAGMA-derived names)
    h = hashlib.sha256()
    n = 0
    for row in cur:
        h.update(_FIELD_SEP.join(_cell_repr(v) for v in row).encode("utf-8"))
        h.update(_ROW_SEP.encode("ascii"))
        n += 1
    return h.hexdigest(), n


def _compare_columns_for_table(conn_old: sqlite3.Connection, table: str) -> Tuple[List[str], List[str]]:
    """Returns (compare_cols, violations). `compare_cols` = OLD's own column
    set MINUS this table's allowlist -- so a column that is brand-new in the
    rebuild (not in OLD's schema at all) and a column that already existed but
    is authorized to change VALUE (e.g. `works.genre`) are both excluded by
    the identical mechanism."""
    allowed = ALLOWED_DIFFERING_COLUMNS.get(table, frozenset())
    old_cols = _table_columns(conn_old, table)
    pk_cols = PRIMARY_KEYS[table]
    compare_cols = [c for c in old_cols if c not in allowed]
    violations = []
    for pk in pk_cols:
        if pk not in compare_cols:
            violations.append(
                f"{table}: primary key column {pk!r} is unexpectedly allowlisted or missing -- "
                "refusing to diff this table"
            )
    return compare_cols, violations


def _first_diff(
    conn_old: sqlite3.Connection, conn_new: sqlite3.Connection, table: str,
    compare_cols: Sequence[str], pk_cols: Sequence[str],
) -> Optional[str]:
    """Locate the FIRST differing row between old and new (in PK order) via a
    streaming dual-cursor merge -- never a cell value in the returned message,
    only the table, primary key, and column NAME (or "row added"/"row
    deleted")."""
    pk_n = len(pk_cols)
    non_pk_cols = [c for c in compare_cols if c not in pk_cols]
    select_cols = list(pk_cols) + non_pk_cols
    col_csv = ", ".join(f'"{c}"' for c in select_cols)
    order_csv = ", ".join(f'"{c}"' for c in pk_cols)
    query = f'SELECT {col_csv} FROM "{table}" ORDER BY {order_csv}'  # noqa: S608

    cur_old = conn_old.execute(query)
    cur_new = conn_new.execute(query)

    def _next(cur):
        row = cur.fetchone()
        if row is None:
            return None, None
        return tuple(row[:pk_n]), tuple(row[pk_n:])

    pk_old, content_old = _next(cur_old)
    pk_new, content_new = _next(cur_new)
    while pk_old is not None or pk_new is not None:
        if pk_new is None or (pk_old is not None and pk_old < pk_new):
            return (
                f"{table}: row deleted (present in OLD, absent in NEW), "
                f"primary key {dict(zip(pk_cols, pk_old))}"
            )
        if pk_old is None or pk_new < pk_old:
            return (
                f"{table}: row added (absent in OLD, present in NEW), "
                f"primary key {dict(zip(pk_cols, pk_new))}"
            )
        for col, v_old, v_new in zip(non_pk_cols, content_old, content_new):
            if v_old != v_new:
                return (
                    f"{table}: row with primary key {dict(zip(pk_cols, pk_old))} differs in column "
                    f"{col!r} (value not shown -- masking discipline)"
                )
        pk_old, content_old = _next(cur_old)
        pk_new, content_new = _next(cur_new)
    return None


def check_table_preserved(
    conn_old: sqlite3.Connection, conn_new: sqlite3.Connection, table: str
) -> Tuple[List[str], str]:
    """Returns (violations, summary_line) for one of the six core tables."""
    allowed = ALLOWED_DIFFERING_COLUMNS.get(table, frozenset())
    if not _has_table(conn_old, table):
        return (
            [f"{table}: table missing from OLD asset"],
            f"{table}: FAIL (table missing from old asset, {len(allowed)} columns allowlisted)",
        )
    if not _has_table(conn_new, table):
        return (
            [f"{table}: table missing from NEW asset"],
            f"{table}: FAIL (table missing from new asset, {len(allowed)} columns allowlisted)",
        )

    compare_cols, violations = _compare_columns_for_table(conn_old, table)
    new_cols = set(_table_columns(conn_new, table))
    missing_in_new = [c for c in compare_cols if c not in new_cols]
    for c in missing_in_new:
        violations.append(f"{table}: pre-existing column {c!r} is missing from the new asset")
    compare_cols = [c for c in compare_cols if c not in missing_in_new]

    if violations:
        return violations, f"{table}: FAIL (schema mismatch, 0 rows compared, {len(allowed)} columns allowlisted)"

    pk_cols = PRIMARY_KEYS[table]
    old_hash, old_n = compute_table_hash(conn_old, table, compare_cols, pk_cols)
    new_hash, new_n = compute_table_hash(conn_new, table, compare_cols, pk_cols)

    if old_hash == new_hash and old_n == new_n:
        return [], f"{table}: PASS ({old_n} rows compared, {len(allowed)} columns allowlisted)"

    diff_violation = _first_diff(conn_old, conn_new, table, compare_cols, pk_cols)
    table_violations = [diff_violation] if diff_violation else [
        f"{table}: content hash mismatch but no row-level difference located "
        f"(old {old_n} rows, new {new_n} rows) -- investigate"
    ]
    if old_n != new_n:
        table_violations.append(f"{table}: row count changed old={old_n} new={new_n}")
    return (
        table_violations,
        f"{table}: FAIL ({old_n} old rows / {new_n} new rows compared, {len(allowed)} columns allowlisted)",
    )


# ---------------------------------------------------------------------------
# band_precision -- dedicated small-table check (D-02a). NOT part of
# CORE_TABLES: the table is tiny (a handful of rows) and carries exactly one
# authorized ROW-level exception rather than a column-level allowlist.
# ---------------------------------------------------------------------------

def check_band_precision_authorized_change(
    conn_old: sqlite3.Connection, conn_new: sqlite3.Connection
) -> Tuple[List[str], str]:
    if not _has_table(conn_old, "band_precision") or not _has_table(conn_new, "band_precision"):
        return [], "band_precision: SKIPPED (table absent from old or new asset)"

    def _rows(conn) -> Dict[Tuple, Dict[str, object]]:
        col_csv = ", ".join(f'"{c}"' for c in _BAND_PRECISION_COLUMNS)
        order_csv = ", ".join(f'"{c}"' for c in _BAND_PRECISION_KEY_COLS)
        out: Dict[Tuple, Dict[str, object]] = {}
        for row in conn.execute(f'SELECT {col_csv} FROM band_precision ORDER BY {order_csv}'):  # noqa: S608
            d = dict(zip(_BAND_PRECISION_COLUMNS, row))
            out[tuple(d[k] for k in _BAND_PRECISION_KEY_COLS)] = d
        return out

    old_rows = _rows(conn_old)
    new_rows = _rows(conn_new)
    violations: List[str] = []
    for key in sorted(set(old_rows) | set(new_rows)):
        o, n = old_rows.get(key), new_rows.get(key)
        if o is None:
            violations.append(f"band_precision: row added, key {dict(zip(_BAND_PRECISION_KEY_COLS, key))}")
            continue
        if n is None:
            violations.append(f"band_precision: row deleted, key {dict(zip(_BAND_PRECISION_KEY_COLS, key))}")
            continue
        is_tier_a = key == _BAND_PRECISION_TIER_A_KEY
        for col in _BAND_PRECISION_COLUMNS:
            if col in _BAND_PRECISION_KEY_COLS or o[col] == n[col]:
                continue
            if is_tier_a and col in _BAND_PRECISION_TIER_A_ALLOWED_FIELDS:
                continue
            violations.append(
                f"band_precision: row {dict(zip(_BAND_PRECISION_KEY_COLS, key))} differs in column "
                f"{col!r} (only the tier_a row's measurement_status/ci_low may change, D-02a)"
            )
        if is_tier_a and n.get("precision") is not None:
            violations.append(
                "band_precision: the tier_a row's precision is non-NULL in the new asset "
                "(D-02a forbids this under any circumstance)"
            )

    summary = (
        f"band_precision: {len(old_rows)} old / {len(new_rows)} new rows compared "
        "(tier_a measurement_status/ci_low is the ONE authorized exception, D-02a)"
    )
    return violations, summary


# ---------------------------------------------------------------------------
# CERT-01 card-binding check (Task 2). Resolves each graded card's bound
# claim_id/display_evidence_id/span_start/span_end/snapshot_hash via the SAME
# frozen ranked-display SQL `scripts.cert01_frame` already uses for the
# CERT-01 estimand -- imported, never re-derived.
# ---------------------------------------------------------------------------

def resolve_card_bindings(sidecar_db_path, cards: Sequence[dict]) -> Dict[str, Optional[dict]]:
    conn = cf._connect_ro(sidecar_db_path)
    try:
        sql = cf._RANKED_ESTIMAND_SQL_TEMPLATE.format(
            dropped=cf._dropped_sql_literal(cf.DROPPED_WORK_IDS)
        )
        cur = conn.execute(sql)
        cols = [d[0] for d in cur.description]
        rows_by_key: Dict[Tuple[str, str], dict] = {}
        for r in cur.fetchall():
            row = dict(zip(cols, r))
            rows_by_key[(row["page_id"], row["canonical_work_id"])] = row

        display_ids = {row["display_evidence_id"] for row in rows_by_key.values()}
        snapshot_by_evid: Dict[str, object] = {}
        if display_ids:
            qmarks = ",".join("?" for _ in display_ids)
            for evid, snap in conn.execute(
                f"SELECT evidence_id, snapshot_hash FROM discovery_evidence WHERE evidence_id IN ({qmarks})",  # noqa: S608
                list(display_ids),
            ).fetchall():
                snapshot_by_evid[evid] = snap
    finally:
        conn.close()

    out: Dict[str, Optional[dict]] = {}
    for card in cards:
        key = (card["page_id"], card["canonical_work_id"])
        row = rows_by_key.get(key)
        if row is None:
            out[card["uid"]] = None
            continue
        out[card["uid"]] = {
            "claim_id": row["claim_id"],
            "display_evidence_id": row["display_evidence_id"],
            "span_start": row["span_start"],
            "span_end": row["span_end"],
            "snapshot_hash": snapshot_by_evid.get(row["display_evidence_id"]),
        }
    return out


def check_card_binding(old_db_path, new_db_path, cards: Sequence[dict]) -> Tuple[List[str], str]:
    if not cards:
        return [], "card-binding: SKIPPED (no graded cards supplied)"
    old_bindings = resolve_card_bindings(old_db_path, cards)
    new_bindings = resolve_card_bindings(new_db_path, cards)
    violations: List[str] = []
    for card in cards:
        uid = card["uid"]
        old_b, new_b = old_bindings.get(uid), new_bindings.get(uid)
        if old_b is None or new_b is None:
            violations.append(
                f"card-binding: card {uid!r} could not be resolved in the "
                f"{'OLD' if old_b is None else 'NEW'} asset"
            )
            continue
        for field_name in ("claim_id", "display_evidence_id", "span_start", "span_end", "snapshot_hash"):
            if old_b[field_name] != new_b[field_name]:
                violations.append(
                    f"card-binding: graded card {uid!r} field {field_name!r} differs between old and "
                    "new asset -- the rebuild moved a graded card's evidence"
                )
    return violations, f"card-binding: {len(cards)} graded card(s) checked"


def load_graded_cards(deck_key_path, verdicts_path) -> List[dict]:
    """Loads every card from `deck_key_path` that has >=1 recorded verdict in
    `verdicts_path` -- i.e. every GRADED card. Both paths point at owner-only
    local research artifacts (gitignored, never committed); callers should
    treat a missing file as "no graded cards yet" rather than an error."""
    deck = json.loads(Path(deck_key_path).read_text(encoding="utf-8"))
    cards_by_uid = {c["uid"]: c for c in deck["cards"]}
    verdicts = json.loads(Path(verdicts_path).read_text(encoding="utf-8"))
    graded_uids = {v["uid"] for v in verdicts if v.get("verdict")}
    return [cards_by_uid[u] for u in graded_uids if u in cards_by_uid]


# ---------------------------------------------------------------------------
# Frame / population / cluster-map hash comparison -- against the PINNED
# expectation, NEVER the candidate's own manifest (Codex F-04).
# ---------------------------------------------------------------------------

def check_frame_hash(new_db_path, expected: dict) -> List[str]:
    conn = _connect_ro(new_db_path)
    try:
        recomputed = sidecar_build.compute_frame_content_hash(conn)
    finally:
        conn.close()
    if recomputed != expected.get("frame_content_hash"):
        return [
            f"frame_content_hash mismatch: recomputed {recomputed} != pinned expectation "
            f"{expected.get('frame_content_hash')} (compared against the EXTERNAL pin, never the "
            "candidate's own manifest -- Codex F-04)"
        ]
    return []


def check_population_stats(new_db_path, research_db_path, expected: dict) -> List[str]:
    rows = cf.compute_estimand_rows(new_db_path, research_db_path)
    pop_hash = cf.population_hash(rows)
    clus_hash = cf.cluster_map_hash(rows)
    counts = cf.stratum_counts(rows)
    violations = []
    if pop_hash != expected.get("population_hash"):
        violations.append(
            f"population_hash mismatch: recomputed {pop_hash} != pinned {expected.get('population_hash')}"
        )
    if clus_hash != expected.get("cluster_map_hash"):
        violations.append(
            f"cluster_map_hash mismatch: recomputed {clus_hash} != pinned {expected.get('cluster_map_hash')}"
        )
    if counts != expected.get("stratum_counts"):
        violations.append(
            f"stratum_counts mismatch: recomputed {counts} != pinned {expected.get('stratum_counts')}"
        )
    return violations


# ---------------------------------------------------------------------------
# verify() -- the single all-invariant entry point (Task 1/2 combined)
# ---------------------------------------------------------------------------

@dataclass
class PreservationResult:
    exit_code: int
    violations: List[str] = field(default_factory=list)
    summary_lines: List[str] = field(default_factory=list)
    peak_rss_bytes: int = -1


def run_verification(
    old_db_path, new_db_path, expected_path, *,
    research_db_path: Optional[str] = None,
    cards: Optional[Sequence[dict]] = None,
) -> PreservationResult:
    violations: List[str] = []
    summary_lines: List[str] = []
    peak_rss = get_rss_bytes()

    violations += check_allowlist_provenance()

    expected = json.loads(Path(expected_path).read_text(encoding="utf-8"))

    old_content_hash = cf.hash_file(old_db_path)
    if old_content_hash != expected.get("db_content_hash"):
        violations.append(
            f"old_db {os.path.basename(str(old_db_path))} content hash {old_content_hash} != the pinned "
            f"expectation's db_content_hash {expected.get('db_content_hash')} -- this is not the SAME "
            "live asset the expectation was pinned from (closes the F-04 candidate-sourced-expectation "
            "failure mode: an expectation generated from the candidate itself would carry the "
            "CANDIDATE's hash here, never the old asset's)"
        )

    conn_old = _connect_ro(old_db_path)
    conn_new = _connect_ro(new_db_path)
    try:
        for table in CORE_TABLES:
            table_violations, summary = check_table_preserved(conn_old, conn_new, table)
            violations += table_violations
            summary_lines.append(summary)
            peak_rss = max(peak_rss, get_rss_bytes())

        bp_violations, bp_summary = check_band_precision_authorized_change(conn_old, conn_new)
        violations += bp_violations
        summary_lines.append(bp_summary)
    finally:
        conn_new.close()
        conn_old.close()

    violations += check_frame_hash(new_db_path, expected)
    peak_rss = max(peak_rss, get_rss_bytes())

    if research_db_path:
        violations += check_population_stats(new_db_path, research_db_path, expected)
        summary_lines.append("population_hash/cluster_map_hash/stratum_counts: recomputed + compared")
    else:
        summary_lines.append(
            "population_hash/cluster_map_hash/stratum_counts: SKIPPED (no --research-db given)"
        )
    peak_rss = max(peak_rss, get_rss_bytes())

    if cards:
        card_violations, card_summary = check_card_binding(old_db_path, new_db_path, cards)
        violations += card_violations
        summary_lines.append(card_summary)
    else:
        summary_lines.append("card-binding: SKIPPED (no graded cards supplied)")

    peak_rss = max(peak_rss, get_rss_bytes())
    exit_code = 1 if violations else 0
    return PreservationResult(
        exit_code=exit_code, violations=violations, summary_lines=summary_lines, peak_rss_bytes=peak_rss
    )


def _mb(n_bytes: int) -> str:
    if n_bytes is None or n_bytes < 0:
        return "unavailable"
    return f"{n_bytes / (1024.0 * 1024.0):.1f} MB"


def verify(
    old_db_path, new_db_path, expected_path, *,
    research_db_path: Optional[str] = None,
    cards: Optional[Sequence[dict]] = None,
) -> int:
    """CLI-facing wrapper: runs `run_verification`, prints the per-table
    summary + any violations, and returns the exit code."""
    result = run_verification(
        old_db_path, new_db_path, expected_path,
        research_db_path=research_db_path, cards=cards,
    )
    for line in result.summary_lines:
        print(line)
    print(f"peak RSS observed during this run: {_mb(result.peak_rss_bytes)}")

    if result.violations:
        for v in result.violations:
            print(f"VIOLATION: {v}", file=sys.stderr)
        print(
            f"verify_rebuild_preservation: {len(result.violations)} violation(s) -- FAILED (fail-closed)",
            file=sys.stderr,
        )
        return 1
    print("verify_rebuild_preservation: all invariants pass -- clean.")
    return 0


# ---------------------------------------------------------------------------
# --generate mode (Task 2): pin the expectation from a SINGLE live asset,
# before any rebuild exists. Refuses to overwrite an existing file without an
# explicit flag -- regenerating AFTER the rebuild has begun defeats the whole
# purpose of an externally pinned expectation (D-02b/D-02c).
# ---------------------------------------------------------------------------

_ARTIFACT_NOTE = (
    "This artifact is the EXTERNALLY pinned pre-rebuild expectation for the Phase-136 "
    "rebuild-preservation gate (D-02b). It supersedes docs/specs/discovery-frames-v2.md as the "
    "external frame-hash pin for THIS rebuild only. The CERT-01 pre-registration "
    "(.planning/phases/135-precision-certificate-confidence-bands/cert01_prereg.json) stays "
    "IMMUTABLE -- scripts/verify_cert01_grading.py's check 10 continues to pin the CURRENT "
    "(pre-rebuild) db_content_hash, so a rebuilt byte-stream fails it BY DESIGN; that check is "
    "never weakened. The rebuilt asset instead receives a separate compatibility attestation "
    "(plan 136-13)."
)


def generate_expectation(
    live_db_path, out_path, *,
    manifest_path: Optional[str] = None,
    research_db_path: Optional[str] = None,
    allow_overwrite: bool = False,
) -> int:
    out = Path(out_path)
    if out.exists() and not allow_overwrite:
        print(
            f"REFUSED: {out} already exists. Regenerating this file after a rebuild has begun would "
            "erase the entire purpose of an EXTERNALLY pinned expectation (D-02b/D-02c) -- the "
            "expectation must be pinned from the CURRENTLY-LIVE asset BEFORE the rebuild exists, and an "
            "expectation re-derived afterward could never detect a wrong rebuild. Re-run with "
            "--regenerate-i-know-what-this-means only if you are certain this is a legitimate re-pin of "
            "a still-pre-rebuild live asset.",
            file=sys.stderr,
        )
        return 1

    conn = _connect_ro(live_db_path)
    try:
        table_hashes: Dict[str, str] = {}
        table_rowcounts: Dict[str, int] = {}
        for table in CORE_TABLES:
            allowed = ALLOWED_DIFFERING_COLUMNS.get(table, frozenset())
            cols = [c for c in _table_columns(conn, table) if c not in allowed]
            pk_cols = PRIMARY_KEYS[table]
            h, n = compute_table_hash(conn, table, cols, pk_cols)
            table_hashes[table] = h
            table_rowcounts[table] = n
        frame_hash = sidecar_build.compute_frame_content_hash(conn)
    finally:
        conn.close()

    db_content_hash = cf.hash_file(live_db_path)

    if manifest_path:
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        if manifest.get("content_hash") != db_content_hash:
            print(
                f"WARNING: manifest content_hash {manifest.get('content_hash')} != recomputed live-db "
                f"hash {db_content_hash} -- is {manifest_path} really the manifest for {live_db_path}?",
                file=sys.stderr,
            )

    payload: Dict[str, object] = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_asset": os.path.basename(str(live_db_path)),
        "db_content_hash": db_content_hash,
        "frame_content_hash": frame_hash,
        "table_hashes": table_hashes,
        "table_rowcounts": table_rowcounts,
        "allowlisted_columns": {t: sorted(c) for t, c in ALLOWED_DIFFERING_COLUMNS.items()},
        "note": _ARTIFACT_NOTE,
    }

    if research_db_path:
        rows = cf.compute_estimand_rows(live_db_path, research_db_path)
        payload["population_hash"] = cf.population_hash(rows)
        payload["cluster_map_hash"] = cf.cluster_map_hash(rows)
        payload["stratum_counts"] = cf.stratum_counts(rows)
    else:
        print(
            "WARNING: --research-db not given -- population_hash/cluster_map_hash/stratum_counts are "
            "OMITTED from the generated artifact. A committed rebuild-preservation baseline MUST include "
            "them (they are computed via scripts.cert01_frame.compute_estimand_rows over BOTH the "
            "sidecar and the research corpus DB).",
            file=sys.stderr,
        )

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {out} (db_content_hash={db_content_hash}, frame_content_hash={frame_hash})")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("old_db", nargs="?", help="Path to the pre-rebuild ('old', currently-live) discovery.db sidecar")
    parser.add_argument("new_db", nargs="?", help="Path to the rebuilt ('new', candidate) discovery.db sidecar")
    parser.add_argument(
        "--expected", metavar="PATH", default=None,
        help="Pinned rebuild-preservation expectation JSON "
             "(136-REBUILD-PRESERVATION-EXPECTED.json), generated ONCE from the live asset before the "
             "rebuild existed. Required for verify mode.",
    )
    parser.add_argument(
        "--research-db", metavar="PATH", default=None,
        help="Research corpus DB (e.g. fullcorpus_v2.db) needed to recompute "
             "population_hash/cluster_map_hash/stratum_counts via scripts.cert01_frame. In verify mode, "
             "omitting this skips that recomputation (frame_content_hash and the six/seven table checks "
             "still run). In --generate mode this is REQUIRED for a real, committable artifact.",
    )
    parser.add_argument(
        "--cert01-cards", metavar="PATH", default=None,
        help="A JSON file containing a list of {uid, page_id, canonical_work_id} graded cards for the "
             "CERT-01 card-binding check (verify mode only); omit to skip that check.",
    )
    parser.add_argument("--generate", action="store_true", help="Generate the pinned expectation file from a SINGLE live asset (old_db) instead of diffing two")
    parser.add_argument("--out", metavar="PATH", default=None, help="Output path for --generate mode")
    parser.add_argument("--manifest", metavar="PATH", default=None, help="manifest.json to cross-check db_content_hash against (--generate mode)")
    parser.add_argument(
        "--regenerate-i-know-what-this-means", action="store_true",
        help="Allow --generate to overwrite an existing --out file. Regenerating AFTER a rebuild has "
             "happened defeats the entire purpose of this artifact (D-02b) -- this flag exists so that "
             "mistake requires a deliberate, named action, never an accidental overwrite.",
    )
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.generate:
        if not args.old_db or not args.out:
            parser.error("--generate requires the old_db positional argument and --out")
        return generate_expectation(
            args.old_db, args.out,
            manifest_path=args.manifest,
            research_db_path=args.research_db,
            allow_overwrite=args.regenerate_i_know_what_this_means,
        )

    if not args.old_db or not args.new_db or not args.expected:
        parser.error("verify mode requires old_db, new_db, and --expected")

    cards = None
    if args.cert01_cards:
        cards = json.loads(Path(args.cert01_cards).read_text(encoding="utf-8"))

    return verify(
        args.old_db, args.new_db, args.expected,
        research_db_path=args.research_db, cards=cards,
    )


if __name__ == "__main__":
    sys.exit(main())
