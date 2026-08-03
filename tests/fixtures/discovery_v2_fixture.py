# -*- coding: utf-8 -*-
"""Post-rebuild ("Amendment 2026-08-02") discovery sidecar fixture builder.

Phase 136, plan 136-20. The committed golden fixture
``tests/fixtures/discovery/discovery-v1-fixture.db`` is a PRE-REBUILD asset:
it predates the Amendment 2026-08-02 contract, so it carries neither
``meta.audience`` nor the two new tables nor the new columns. That is exactly
the shape plan 136-20's readiness contract must REFUSE.

This module therefore does two jobs:

  1. ``materialize_pre_rebuild_sidecar()`` -- hand back the golden fixture
     untouched, as the rollback/pre-rebuild case.
  2. ``materialize_sidecar()`` -- copy the golden fixture and UPGRADE it in
     place to the Amendment 2026-08-02 shape (new columns, the two new tables,
     ``meta.audience`` and the two new release-contract count keys), so the
     existing ready-path tests (and the new audience tests) have a sidecar the
     post-rebuild loader actually accepts.

Every knob a defect-mode test needs is a parameter rather than a post-hoc
mutation: ``omit_tables``, ``omit_columns``, ``extra_columns``,
``meta_overrides``, ``omit_meta_keys`` and ``audience``. Building the defect
shape UP (rather than tearing a valid shape DOWN) avoids depending on SQLite's
``ALTER TABLE ... DROP COLUMN``, which is version-dependent.

Masking discipline (D-25): every value here is fabricated/synthetic. Nothing
in this module touches real research data, and no restricted corpus is named
-- restricted corpora appear only as "M-source"/"R-source" anywhere in this
repository.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Sequence, Tuple

GOLDEN_DIR = Path(__file__).resolve().parent / "discovery"
GOLDEN_DB = GOLDEN_DIR / "discovery-v1-fixture.db"
GOLDEN_MANIFEST = GOLDEN_DIR / "manifest.json"
GOLDEN_BASENAME = "discovery-v1-fixture"
GOLDEN_SIDECAR_VERSION = "discovery-v1-synthetic-fixture"

# Columns the Amendment 2026-08-02 adds to tables that ALREADY EXIST in the
# v1 asset. `works.genre` is deliberately absent here: the amendment's section
# (C) is explicit that `genre` already exists on `works` and is NOT added by a
# migration -- only populated. It is still part of the loader's required-column
# contract, which is why the v1 fixture already satisfies it.
ADDED_COLUMNS_ON_EXISTING_TABLES: Dict[str, Tuple[Tuple[str, str], ...]] = {
    "discovery_evidence": (
        ("coverage_ppm", "INTEGER"),
        ("coverage_status", "TEXT"),
        ("band_rank", "INTEGER"),
        ("novelty_status", "TEXT NOT NULL DEFAULT 'not_checked'"),
        ("novelty_source_label", "TEXT"),
        ("divergence_correctness", "TEXT"),
        ("assertion_visibility", "TEXT NOT NULL DEFAULT 'public'"),
    ),
    "works": (
        ("identity_visibility", "TEXT NOT NULL DEFAULT 'public'"),
    ),
}

# Tables the Amendment 2026-08-02 adds, plus `discovery_routing_audit` (added
# by the 2026-07-24 amendment, after the golden fixture was built -- the
# amendment's section (F) makes its `demoted_work_id` column contractual, so a
# post-rebuild-shaped fixture must carry it).
NEW_TABLE_COLUMNS: Dict[str, Tuple[Tuple[str, str], ...]] = {
    "discovery_identification": (
        ("identification_id", "TEXT PRIMARY KEY"),
        ("sys_id", "TEXT NOT NULL"),
        ("canonical_work_id", "TEXT NOT NULL"),
        ("display_work_id", "TEXT NOT NULL"),
        ("main_pool", "INTEGER NOT NULL"),
        ("main_pool_reason", "TEXT NOT NULL"),
        ("best_band_rank", "INTEGER NOT NULL"),
        ("page_count", "INTEGER NOT NULL"),
        ("max_coverage_ppm", "INTEGER"),
        ("relation_kind", "TEXT NOT NULL"),
        ("novelty_status", "TEXT NOT NULL"),
        ("divergence_correctness", "TEXT"),
        ("assertion_visibility", "TEXT NOT NULL"),
        ("identity_visibility", "TEXT NOT NULL"),
    ),
    "manuscript_display": (
        ("sys_id", "TEXT PRIMARY KEY"),
        ("library_code", "TEXT NOT NULL"),
        ("library_sort_key", "TEXT NOT NULL"),
        ("shelfmark_display", "TEXT NOT NULL"),
        ("shelfmark_sort_key", "TEXT NOT NULL"),
    ),
    "discovery_routing_audit": (
        ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
        ("page_id", "TEXT"),
        ("kept_work_id", "TEXT"),
        ("demoted_work_id", "TEXT"),
        ("kept_year", "INTEGER"),
        ("demoted_year", "INTEGER"),
        ("delta_years", "INTEGER"),
        ("decision", "TEXT"),
        ("routing_reason", "TEXT"),
    ),
}

# The two release-contract count meta keys the amendment's section (C1) adds,
# keyed by the table each one counts.
NEW_COUNT_META_KEY_BY_TABLE: Dict[str, str] = {
    "discovery_identification": "expected_rows_discovery_identification",
    "manuscript_display": "expected_rows_manuscript_display",
}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _create_table(conn: sqlite3.Connection, table: str, omit_columns: Iterable[str]) -> None:
    omit = set(omit_columns)
    cols = [
        f"{name} {decl}"
        for name, decl in NEW_TABLE_COLUMNS[table]
        if name not in omit
    ]
    conn.execute(f"CREATE TABLE {table} ({', '.join(cols)})")


def upgrade_db_to_post_rebuild(
    db_path: Path,
    *,
    audience: Optional[str] = "public",
    omit_tables: Sequence[str] = (),
    omit_columns: Sequence[Tuple[str, str]] = (),
    extra_columns: Sequence[Tuple[str, str, str]] = (),
    meta_overrides: Optional[Mapping[str, str]] = None,
    omit_meta_keys: Sequence[str] = (),
) -> None:
    """Upgrade a v1-shaped sidecar at ``db_path`` to the Amendment 2026-08-02
    shape, in place.

    ``omit_tables``      -- table names NOT to create (partial-asset cases).
    ``omit_columns``     -- ``(table, column)`` pairs NOT to create.
    ``extra_columns``    -- ``(table, column, decl)`` triples to ADD beyond the
                            contract (proves the column check is a SUBSET check).
    ``audience``         -- the ``meta.audience`` value; ``None`` writes no key
                            at all (the fail-closed default case).
    """
    omitted_cols = set(omit_columns)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys = OFF")

        existing_tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

        for table, columns in ADDED_COLUMNS_ON_EXISTING_TABLES.items():
            if table not in existing_tables:
                continue
            present = {
                row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
            }
            for name, decl in columns:
                if name in present or (table, name) in omitted_cols:
                    continue
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")

        for table in NEW_TABLE_COLUMNS:
            if table in omit_tables or table in existing_tables:
                continue
            _create_table(
                conn,
                table,
                omit_columns=[c for (t, c) in omitted_cols if t == table],
            )

        for table, name, decl in extra_columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")

        meta: Dict[str, str] = {}
        if audience is not None:
            meta["audience"] = audience
        for table, meta_key in NEW_COUNT_META_KEY_BY_TABLE.items():
            if table in omit_tables:
                # Deliberately still write the count key (as 0) when the table
                # itself is omitted, so a missing-TABLE test fails on the table
                # check rather than vacuously on the missing meta key. A test
                # that wants both missing asks for it via `omit_meta_keys`.
                meta[meta_key] = "0"
                continue
            (count,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            meta[meta_key] = str(count)
        if meta_overrides:
            meta.update({k: str(v) for k, v in meta_overrides.items()})
        for key in omit_meta_keys:
            meta.pop(key, None)

        for key, value in meta.items():
            conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
        for key in omit_meta_keys:
            conn.execute("DELETE FROM meta WHERE key = ?", (key,))
        conn.commit()
    finally:
        conn.close()


def write_manifest(
    dir_path: Path,
    db_path: Path,
    *,
    asset_basename: str = GOLDEN_BASENAME,
    schema_version: str = "discovery-v1",
) -> None:
    """Write a manifest whose ``content_hash`` is computed AFTER every
    mutation, so the loader's hash check never masks the defect under test."""
    manifest = {
        "asset_basename": asset_basename,
        "content_hash": sha256_file(db_path),
        "frame_content_hash": "b5f7970e93f4a342bc8939366b5d5a994a81c1a9f2aed48555c410102a8562b4",
        "schema_version": schema_version,
    }
    (dir_path / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def materialize_pre_rebuild_sidecar(
    dest_dir: Path, *, audience: Optional[str] = None
) -> Path:
    """The PRE-REBUILD shape -- the committed golden v1 fixture, untouched.

    This is the rollback case: no ``meta.audience``, no
    ``discovery_identification``/``manuscript_display``, none of the new
    columns. The post-rebuild readiness contract must leave it not-ready.

    ``audience`` optionally stamps ONLY the audience key onto the otherwise
    untouched v1 asset. That isolates the structural checks: with a `public`
    marker in place, the audience gate can no longer be what refuses the asset,
    so anything that still refuses it is the required-table / required-COLUMN /
    row-count contract doing the work.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    db_path = dest_dir / f"{GOLDEN_BASENAME}.db"
    shutil.copyfile(GOLDEN_DB, db_path)
    if audience is None:
        shutil.copyfile(GOLDEN_MANIFEST, dest_dir / "manifest.json")
        return db_path

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('audience', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (audience,),
        )
        conn.commit()
    finally:
        conn.close()
    write_manifest(dest_dir, db_path)
    return db_path


def materialize_sidecar(dest_dir: Path, **upgrade_kwargs) -> Path:
    """Copy the golden fixture into ``dest_dir``, upgrade it to the post-rebuild
    shape (honouring every defect knob in ``upgrade_db_to_post_rebuild``), then
    write a matching manifest. Returns the sidecar path."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    db_path = dest_dir / f"{GOLDEN_BASENAME}.db"
    shutil.copyfile(GOLDEN_DB, db_path)
    upgrade_db_to_post_rebuild(db_path, **upgrade_kwargs)
    write_manifest(dest_dir, db_path)
    return db_path
