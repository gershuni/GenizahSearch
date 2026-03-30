# -*- coding: utf-8 -*-
"""Tests for visual similarity service and import script."""

import pytest
import sqlite3
import os
import tempfile


@pytest.fixture
def tmp_vs_db(tmp_path):
    """Create a temporary visual_similarity.db with known test data."""
    db_path = str(tmp_path / 'visual_similarity.db')
    conn = sqlite3.connect(db_path)
    conn.execute('''CREATE TABLE visual_suggestions (
        alma_id_a INTEGER NOT NULL,
        alma_id_b INTEGER NOT NULL,
        svm_score REAL NOT NULL,
        PRIMARY KEY (alma_id_a, alma_id_b)
    )''')
    conn.execute('CREATE INDEX idx_vs_a ON visual_suggestions(alma_id_a)')
    conn.execute('''CREATE TABLE vs_metadata (key TEXT PRIMARY KEY, value TEXT)''')
    conn.execute("INSERT INTO vs_metadata VALUES ('version', '1.0.0')")
    conn.execute("INSERT INTO vs_metadata VALUES ('import_date', '2026-03-29')")
    conn.execute("INSERT INTO vs_metadata VALUES ('pair_count', '8')")
    conn.execute("INSERT INTO vs_metadata VALUES ('manuscript_count', '2')")
    # Insert 5 test pairs for alma_id_a=100
    test_pairs = [
        (100, 201, 15.5), (100, 202, 12.3), (100, 203, 10.1),
        (100, 204, 8.7), (100, 205, 5.2),
    ]
    conn.executemany('INSERT INTO visual_suggestions VALUES (?,?,?)', test_pairs)
    # Insert pairs for alma_id_a=200
    conn.executemany('INSERT INTO visual_suggestions VALUES (?,?,?)', [
        (200, 301, 11.0), (200, 205, 9.5), (200, 302, 7.0),
    ])
    conn.commit()
    conn.close()
    return db_path


def test_get_suggestions_returns_ranked_list(tmp_vs_db):
    raise NotImplementedError("Stub -- implement after service exists")


def test_get_suggestions_empty(tmp_vs_db):
    raise NotImplementedError("Stub")


def test_has_suggestions_true(tmp_vs_db):
    raise NotImplementedError("Stub")


def test_has_suggestions_false(tmp_vs_db):
    raise NotImplementedError("Stub")


def test_get_suggestion_partners_union(tmp_vs_db):
    raise NotImplementedError("Stub")


def test_get_suggestion_partners_intersection(tmp_vs_db):
    raise NotImplementedError("Stub")


def test_get_suggestion_partners_empty_input(tmp_vs_db):
    raise NotImplementedError("Stub")


def test_get_suggestion_count(tmp_vs_db):
    raise NotImplementedError("Stub")


def test_batch_has_suggestions(tmp_vs_db):
    raise NotImplementedError("Stub")


def test_get_db_version(tmp_vs_db):
    raise NotImplementedError("Stub")


def test_import_creates_sidecar():
    raise NotImplementedError("Stub")


def test_import_deduplicates():
    raise NotImplementedError("Stub")


def test_import_excludes_self_pairs():
    raise NotImplementedError("Stub")


def test_import_skips_zero_score_markcodes():
    raise NotImplementedError("Stub")
