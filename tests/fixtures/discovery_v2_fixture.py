# -*- coding: utf-8 -*-
"""Post-rebuild ("Amendment 2026-08-02") discovery sidecar fixture builder.

Phase 136, plan 136-20.

⟨AMENDED 2026-08-03, plan 136-12⟩ **This module used to rely on the committed
golden fixture BEING pre-rebuild.** That was true only because plans 136-11 and
136-12 had not yet regenerated it: the golden carried neither ``meta.audience``
nor the two new tables nor the new columns, so "the pre-rebuild shape" and "the
golden fixture" were the same file. 136-12's Task 3 refreshes the golden so it
exercises every field the rebuild adds -- which makes that coincidence false.

Relying on a fixture's STALENESS is not a contract, so this module no longer
does. It now derives its own v1-shaped base by STRIPPING the Amendment
2026-08-02 additions from the golden (``_write_v1_shaped_copy``), and both
entry points build up from that base. Every ``omit_*`` knob therefore behaves
exactly as it did before the refresh, and no consumer test needed changing.

This module does two jobs:

  1. ``materialize_pre_rebuild_sidecar()`` -- the v1-shaped (pre-Amendment)
     asset, as the rollback/pre-rebuild case.
  2. ``materialize_sidecar()`` -- the same v1-shaped base, UPGRADED in place to
     the Amendment 2026-08-02 shape (new columns, the two new tables,
     ``meta.audience`` and the two new release-contract count keys), so the
     existing ready-path tests (and the audience tests) have a sidecar the
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

# ---------------------------------------------------------------------------
# CD batch (schema Amendment 2026-08-12): the seven tables, the two
# discovery_identification columns, and the meta keys the batch adds ON TOP of
# the Amendment 2026-08-02 shape. Column lists MIRROR the amendment's DDL
# (drift against the builder is guarded by test). The v1 strip removes all of
# it (the golden fixture is regenerated post-batch), `upgrade_db_to_post_
# rebuild` deliberately does NOT re-add it (its shape stays 2026-08-02, which
# the loader accepts as a PRE-batch asset -- no marker, no requirement), and
# `upgrade_db_to_cd_batch` re-adds it with the same defect-knob pattern.
# ---------------------------------------------------------------------------
CD_BATCH_TABLE_COLUMNS: Dict[str, Tuple[Tuple[str, str], ...]] = {
    "locus_work": (
        ("work_id", "TEXT PRIMARY KEY"),
        ("family", "TEXT NOT NULL"),
        ("grain", "TEXT NOT NULL"),
        ("stream_len", "INTEGER NOT NULL"),
        ("unit_count", "INTEGER NOT NULL"),
    ),
    "locus_unit": (
        ("work_id", "TEXT NOT NULL"),
        ("unit_ord", "INTEGER NOT NULL"),
        ("start_offset", "INTEGER NOT NULL"),
        ("part_key", "TEXT NOT NULL"),
        ("label_he", "TEXT NOT NULL"),
        ("citation_pos", "INTEGER"),
    ),
    "locus_edition": (
        ("work_id", "TEXT PRIMARY KEY"),
        ("title_he", "TEXT NOT NULL"),
        ("title_original", "TEXT NOT NULL"),
        ("author_short", "TEXT NOT NULL"),
        ("author_full", "TEXT NOT NULL"),
        ("publisher", "TEXT NOT NULL"),
        ("publisher_city", "TEXT NOT NULL"),
        ("publisher_year", "TEXT NOT NULL"),
        ("editor", "TEXT NOT NULL"),
        ("edition", "TEXT NOT NULL"),
    ),
    "discovery_region_map": (
        ("region_version", "TEXT NOT NULL"),
        ("work_id", "TEXT NOT NULL"),
        ("unit_ord", "INTEGER NOT NULL"),
        ("discriminative", "INTEGER"),
        ("source", "TEXT NOT NULL"),
        ("basis", "TEXT"),
    ),
    "discovery_curated_quoter": (
        ("list_version", "TEXT NOT NULL"),
        ("canonical_work_id", "TEXT NOT NULL"),
        ("ruled_date", "TEXT NOT NULL"),
        ("note", "TEXT"),
    ),
    "discovery_stratum_membership": (
        ("frame_version", "TEXT NOT NULL"),
        ("stratum_id", "TEXT NOT NULL"),
        ("identification_id", "TEXT NOT NULL"),
    ),
    "discovery_withholding": (
        ("withhold_version", "TEXT NOT NULL"),
        ("scope_id", "TEXT NOT NULL"),
        ("predicate_json", "TEXT NOT NULL"),
        ("frame_version", "TEXT"),
        ("stratum_id", "TEXT"),
        ("reason", "TEXT NOT NULL"),
        ("created_date", "TEXT NOT NULL"),
    ),
}
CD_BATCH_ADDED_COLUMNS: Dict[str, Tuple[Tuple[str, str], ...]] = {
    "discovery_identification": (
        ("routing_reason", "TEXT NOT NULL DEFAULT 'none'"),
        ("rendered_relation", "TEXT NOT NULL DEFAULT 'uncertain'"),
    ),
}
CD_BATCH_MARKER_KEY = "locus_schema_version"
CD_BATCH_MARKER_VALUE = "locus-v1"
CD_BATCH_COUNT_META_KEY_BY_TABLE: Dict[str, str] = {
    table: f"expected_rows_{table}" for table in CD_BATCH_TABLE_COLUMNS
}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# Meta keys the Amendment 2026-08-02 build writes and a v1-shaped asset must
# NOT carry. `audience` and the two count keys are the amendment's own (C1);
# the three provenance pins are written by 136-12's curated/verdict loads.
_POST_REBUILD_META_KEYS: Tuple[str, ...] = (
    "audience",
    "expected_rows_discovery_identification",
    "expected_rows_manuscript_display",
    "work_domains_content_hash",
    "work_author_aliases_content_hash",
    "novelty_verdicts_sha256",
)


def _write_v1_shaped_copy(dest_db: Path) -> None:
    """Copy the golden fixture to ``dest_db`` and STRIP it back to the
    pre-Amendment-2026-08-02 (v1) shape, in place.

    Needed since 136-12 refreshed the golden fixture: "the pre-rebuild shape"
    used to be "the golden fixture, untouched", which was only ever true
    because the golden was stale. Deriving the v1 shape explicitly makes the
    pre-rebuild case a STATED contract rather than an accident that a
    regeneration silently deletes.

    Columns are removed by rebuilding each affected table through
    ``CREATE TABLE ... AS SELECT <v1 columns>`` rather than
    ``ALTER TABLE ... DROP COLUMN`` (which is SQLite-version dependent). Table
    constraints and indexes are NOT reproduced: every consumer of this module
    is a LOADER-READINESS test, which inspects table/column presence, meta keys
    and row counts -- never a constraint. A future consumer that needs the real
    constraints should build its own fixture rather than widening this one.
    """
    shutil.copyfile(GOLDEN_DB, dest_db)
    conn = sqlite3.connect(str(dest_db))
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        existing = {
            r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        for table, added in ADDED_COLUMNS_ON_EXISTING_TABLES.items():
            if table not in existing:
                continue
            drop = {name for name, _decl in added}
            keep = [
                r[1] for r in conn.execute(f"PRAGMA table_info({table})") if r[1] not in drop
            ]
            if len(keep) == len(list(conn.execute(f"PRAGMA table_info({table})"))):
                continue  # already v1-shaped
            col_list = ", ".join(f'"{c}"' for c in keep)
            conn.execute(f'CREATE TABLE "{table}__v1" AS SELECT {col_list} FROM "{table}"')
            conn.execute(f'DROP TABLE "{table}"')
            conn.execute(f'ALTER TABLE "{table}__v1" RENAME TO "{table}"')
        for table in NEW_TABLE_COLUMNS:
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        # CD batch (Amendment 2026-08-12): the regenerated golden also carries
        # the batch's tables, columns and meta keys -- the v1 shape has none of
        # them. (The two identification COLUMNS go with the table drop above.)
        for table in CD_BATCH_TABLE_COLUMNS:
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.executemany(
            "DELETE FROM meta WHERE key = ?",
            [(k,) for k in _POST_REBUILD_META_KEYS]
            + [(CD_BATCH_MARKER_KEY,)]
            + [(k,) for k in CD_BATCH_COUNT_META_KEY_BY_TABLE.values()],
        )
        conn.commit()
    finally:
        conn.close()


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
    """The PRE-REBUILD shape -- the golden fixture stripped back to v1.

    This is the rollback case: no ``meta.audience``, no
    ``discovery_identification``/``manuscript_display``, none of the new
    columns. The post-rebuild readiness contract must leave it not-ready.

    ⟨AMENDED 2026-08-03, plan 136-12⟩ Derived by ``_write_v1_shaped_copy``
    rather than by handing back the golden untouched -- since the golden was
    refreshed it carries the full post-rebuild shape, so "untouched" would now
    return an asset the readiness contract ACCEPTS, silently inverting every
    test that calls this.

    ``audience`` optionally stamps ONLY the audience key onto the otherwise
    v1-shaped asset. That isolates the structural checks: with a `public`
    marker in place, the audience gate can no longer be what refuses the asset,
    so anything that still refuses it is the required-table / required-COLUMN /
    row-count contract doing the work.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    db_path = dest_dir / f"{GOLDEN_BASENAME}.db"
    _write_v1_shaped_copy(db_path)
    if audience is None:
        # The manifest must match the STRIPPED file, not the golden -- otherwise
        # the loader's content-hash check refuses the asset before any
        # structural check runs, and the test would pass for the wrong reason.
        write_manifest(dest_dir, db_path)
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
    # Build UP from the v1 shape, so every `omit_tables`/`omit_columns` knob can
    # still WITHHOLD something -- a knob cannot omit what the base already has.
    _write_v1_shaped_copy(db_path)
    upgrade_db_to_post_rebuild(db_path, **upgrade_kwargs)
    write_manifest(dest_dir, db_path)
    return db_path


def upgrade_db_to_cd_batch(
    db_path: Path,
    *,
    omit_tables: Sequence[str] = (),
    omit_columns: Sequence[Tuple[str, str]] = (),
    omit_marker: bool = False,
    meta_overrides: Optional[Mapping[str, str]] = None,
    omit_meta_keys: Sequence[str] = (),
) -> None:
    """Upgrade a post-rebuild (Amendment 2026-08-02) sidecar to the CD-batch
    (Amendment 2026-08-12) shape, in place -- the seven new tables, the two
    ``discovery_identification`` columns, the ``locus_schema_version`` marker
    and the seven count keys. Same defect-knob pattern as
    ``upgrade_db_to_post_rebuild``; ``omit_marker`` builds the deliberate
    PRE-batch presentation (marker absent -> the loader must not require any
    of it)."""
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
        for table, columns in CD_BATCH_ADDED_COLUMNS.items():
            if table not in existing_tables:
                continue
            present = {
                row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
            }
            for name, decl in columns:
                if name in present or (table, name) in omitted_cols:
                    continue
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")

        for table, columns in CD_BATCH_TABLE_COLUMNS.items():
            if table in omit_tables or table in existing_tables:
                continue
            cols = [
                f"{name} {decl}"
                for name, decl in columns
                if (table, name) not in omitted_cols
            ]
            conn.execute(f"CREATE TABLE {table} ({', '.join(cols)})")

        meta: Dict[str, str] = {}
        if not omit_marker:
            meta[CD_BATCH_MARKER_KEY] = CD_BATCH_MARKER_VALUE
        for table, meta_key in CD_BATCH_COUNT_META_KEY_BY_TABLE.items():
            if table in omit_tables:
                # Same convention as upgrade_db_to_post_rebuild: still write
                # the count key (as 0) so a missing-TABLE test fails on the
                # table check, not vacuously on the missing meta key.
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


def materialize_cd_batch_sidecar(
    dest_dir: Path,
    *,
    post_rebuild_kwargs: Optional[Mapping] = None,
    **cd_batch_kwargs,
) -> Path:
    """The full CD-batch (Amendment 2026-08-12) presentation: v1 base ->
    post-rebuild upgrade -> CD-batch upgrade -> manifest written LAST, so the
    loader's content-hash check never masks the defect under test."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    db_path = dest_dir / f"{GOLDEN_BASENAME}.db"
    _write_v1_shaped_copy(db_path)
    upgrade_db_to_post_rebuild(db_path, **dict(post_rebuild_kwargs or {}))
    upgrade_db_to_cd_batch(db_path, **cd_batch_kwargs)
    write_manifest(dest_dir, db_path)
    return db_path
