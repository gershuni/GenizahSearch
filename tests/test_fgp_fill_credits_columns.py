# -*- coding: utf-8 -*-
"""FIST catalog/book credit builders degrade gracefully on lean schema variants.

Codex PR #309 (P2): ``build_catalog_map`` ran a hardcoded
``SELECT ... Publisher, YearOfPublishing FROM CODE_Catalog``. Those columns
exist in the full FIST.db but NOT in every FIST schema variant (the synthetic
test fixture omits them). Because the builder runs unconditionally at startup,
a missing column would raise ``OperationalError`` and abort the *entire* credit
fill — not just catalog rows. These tests pin the column-defensive behaviour.
"""
from __future__ import annotations

import sqlite3

from scripts.fgp_fill_credits_bilingual import build_book_map, build_catalog_map


def _lean_catalog_conn():
    """CODE_Catalog without Publisher/YearOfPublishing (synthetic-fixture shape)."""
    con = sqlite3.connect(":memory:")
    con.executescript(
        """
        CREATE TABLE CODE_Catalog (
            CatalogId INTEGER PRIMARY KEY, CatalogType TEXT, Author TEXT,
            CatAcronym TEXT, Title TEXT, Domain TEXT, Collection TEXT
        );
        INSERT INTO CODE_Catalog (CatalogId, CatAcronym) VALUES (12, 'CUDL'), (13, NULL);
        """
    )
    return con


def test_catalog_map_no_publisher_year_columns():
    con = _lean_catalog_conn()
    out = build_catalog_map(con)  # must not raise OperationalError
    assert out == {12: "CUDL Catalog"}  # acronym-only; NULL acronym skipped


def test_catalog_map_full_schema_uses_publisher_year():
    con = sqlite3.connect(":memory:")
    con.executescript(
        """
        CREATE TABLE CODE_Catalog (
            CatalogId INTEGER PRIMARY KEY, CatAcronym TEXT,
            Publisher TEXT, YearOfPublishing TEXT
        );
        INSERT INTO CODE_Catalog VALUES (12, 'CUDL', 'Cambridge UP', '1998');
        INSERT INTO CODE_Catalog VALUES (13, 'X', NULL, NULL);
        """
    )
    out = build_catalog_map(con)
    assert out[12] == "CUDL Catalog, Cambridge UP, 1998"
    assert out[13] == "X Catalog"  # optional fields gracefully omitted


def test_catalog_map_missing_table_returns_empty():
    con = sqlite3.connect(":memory:")
    assert build_catalog_map(con) == {}


def test_book_map_without_author_tables():
    con = sqlite3.connect(":memory:")
    con.executescript(
        """
        CREATE TABLE CODE_Title (
            TitleId INTEGER PRIMARY KEY, FullTitleHeb TEXT,
            RunningTitleHeb TEXT, AcronymHeb TEXT
        );
        INSERT INTO CODE_Title VALUES (5, 'full', 'running', 'acr');
        INSERT INTO CODE_Title VALUES (6, NULL, NULL, 'onlyacr');
        """
    )
    out = build_book_map(con)  # no CODE_TitleAuthor / CODE_Author present
    assert out == {5: "running", 6: "onlyacr"}  # title-only, prefers RunningTitleHeb


def test_book_map_missing_title_table_returns_empty():
    con = sqlite3.connect(":memory:")
    assert build_book_map(con) == {}
