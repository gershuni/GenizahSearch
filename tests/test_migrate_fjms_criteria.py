"""scripts/migrate_fjms_criteria.py: the 5.0.x -> 5.1.0 sidecar step of
docs/plans/CATALOGUE_CRITERIA_PLAN.md §6.5, on small real SQLite files."""
import sqlite3

import pytest

from scripts.migrate_fjms_criteria import (
    EXIT_INSTATUTION, EXIT_OK, EXIT_REFUSED, main,
)


def build(path, *, version="5.0.0", instatution=False, measurements=True):
    con = sqlite3.connect(str(path))
    con.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    con.execute("INSERT INTO meta VALUES ('version', ?)", (version,))
    con.execute("CREATE TABLE catalog (AlmaId TEXT, UnitCatalogRecId INTEGER,"
                " NumRow TEXT, NumColumn TEXT, SourceName TEXT)")
    source = "Instatution" if instatution else "GRU – Cambridge"
    con.executemany("INSERT INTO catalog VALUES (?, ?, ?, ?, ?)", [
        ("990001", 1, "19-21", "2", "NLI Aleph Catalog"),
        ("990001", 2, "0", "0", source),          # 0 = not recorded
        ("990002", 3, "22,25", "1-1", "Inventory"),
        ("990003", 4, None, "", "Inventory"),
    ])
    con.execute("CREATE TABLE catalog_free_desc (AlmaId TEXT, SignatureId INTEGER,"
                " SourceName TEXT)")
    con.execute("CREATE TABLE catalog_fields (AlmaId TEXT, UnitCatalogRecId INTEGER,"
                " FieldCategory TEXT, FieldValue TEXT, FieldValueHeb TEXT)")
    if measurements:
        con.execute("CREATE TABLE manuscript_measurements (AlmaId TEXT PRIMARY KEY,"
                    " avg_text_density REAL, material TEXT)")
        con.execute("INSERT INTO manuscript_measurements VALUES ('990001', 1.0, 'Paper')")
        con.execute("CREATE TABLE computed_measurements (AlmaId TEXT,"
                    " Avg_Line_Height_Text_mm REAL, Flag_DPI_High INTEGER,"
                    " Flag_DPI_Low INTEGER, Flag_Negative_Margin INTEGER,"
                    " Flag_BifolioLoc_Error INTEGER)")
        con.executemany("INSERT INTO computed_measurements VALUES (?, ?, 0, 0, 0, 0)",
                        [("990001", 4.0), ("990001", 6.0)])
    con.commit()
    con.close()
    return path


def read(path, sql):
    con = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_dry_run_writes_nothing(tmp_path):
    db = build(tmp_path / "s.db")
    before = db.read_bytes()
    assert main(["--db", str(db)]) == EXIT_OK
    assert db.read_bytes() == before


def test_apply_builds_the_5_1_0_file(tmp_path):
    db = build(tmp_path / "s.db")
    assert main(["--db", str(db), "--apply"]) == EXIT_OK
    assert read(db, "SELECT value FROM meta WHERE key='version'") == [("5.1.0",)]
    rows = read(db, "SELECT AlmaId, UnitCatalogRecId, key, lo, hi FROM criteria_numbers"
                    " ORDER BY AlmaId, key, lo")
    assert rows == [
        ("990001", 1, "columns", 2, 2),
        ("990001", 1, "lines", 19, 21),
        ("990002", 3, "columns", 1, 1),
        ("990002", 3, "lines", 22, 22),
        ("990002", 3, "lines", 25, 25),
    ]  # the '0' record and the empty one give nothing
    indexes = {name for (name,) in read(db, "SELECT name FROM sqlite_master WHERE type='index'")}
    assert {"idx_critnum_key_range", "idx_catfld_cat_val"} <= indexes
    assert read(db, "SELECT avg_line_height_mm FROM manuscript_measurements") == [(5.0,)]


def test_rerun_on_a_complete_5_1_0_file_is_a_no_op(tmp_path):
    db = build(tmp_path / "s.db")
    assert main(["--db", str(db), "--apply"]) == EXIT_OK
    after_first = db.read_bytes()
    assert main(["--db", str(db), "--apply"]) == EXIT_OK
    assert db.read_bytes() == after_first


def test_refuses_a_file_that_still_has_instatution_rows(tmp_path):
    db = build(tmp_path / "s.db", instatution=True)
    before = db.read_bytes()
    assert main(["--db", str(db), "--apply"]) == EXIT_INSTATUTION
    assert db.read_bytes() == before


@pytest.mark.parametrize("version", ["4.2.0", "", "6.0.0"])
def test_refuses_other_versions(tmp_path, version):
    db = build(tmp_path / "s.db", version=version)
    before = db.read_bytes()
    assert main(["--db", str(db), "--apply"]) == EXIT_REFUSED
    assert db.read_bytes() == before


def test_refuses_an_incomplete_5_1_0_file(tmp_path):
    db = build(tmp_path / "s.db", version="5.1.0")
    assert main(["--db", str(db), "--apply"]) == EXIT_REFUSED


def test_missing_file_is_refused_not_created(tmp_path):
    db = tmp_path / "absent.db"
    assert main(["--db", str(db)]) == EXIT_REFUSED
    assert not db.exists()


def test_a_file_without_measurement_tables_still_upgrades(tmp_path):
    db = build(tmp_path / "s.db", measurements=False)
    assert main(["--db", str(db), "--apply"]) == EXIT_OK
    assert read(db, "SELECT value FROM meta WHERE key='version'") == [("5.1.0",)]
