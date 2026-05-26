# -*- coding: utf-8 -*-
"""Phase 97.2 R97.2-F — schema-marker absence triggers atomic rebuild.

RED gate: Phase 95 install (meta.json present, .schema_version ABSENT) must trip
rebuild at LocalIndexer.__init__, not crash with 'Schema error' or open a stale
Phase 95 index that later crashes with 'Field scan_run_id is not defined'.
"""
import gc
import os
import tantivy
from shared.local_indexer import (
    LocalIndexer,
    build_local_schema,
    _compute_schema_marker,
    _read_schema_marker,
)


def test_phase_95_install_no_schema_marker_triggers_rebuild(tmp_path):
    """RED before R97.2-F fix; GREEN after."""
    idx_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "db.sqlite3")
    os.makedirs(idx_dir)
    os.makedirs(lab_dir)

    # Simulate Phase 95 install: Tantivy index present, NO .schema_version marker.
    pre_schema = build_local_schema()
    pre_index = tantivy.Index(pre_schema, path=idx_dir)
    pre_writer = pre_index.writer(heap_size=15_000_000)
    pre_writer.commit()
    pre_writer = None
    pre_index = None
    gc.collect()

    assert os.path.isfile(os.path.join(idx_dir, "meta.json"))
    assert not os.path.isfile(os.path.join(idx_dir, ".schema_version"))

    indexer = LocalIndexer(idx_dir, lab_dir, db_path)
    try:
        # POST-FIX assertions: __init__ detected (None != expected) and rebuilt.
        assert indexer._writer is not None, "writer must be acquired after rebuild"
        assert os.path.isfile(os.path.join(idx_dir, ".schema_version")), (
            "rebuild must write .schema_version marker"
        )
    finally:
        indexer._close_internal_writer_index()
