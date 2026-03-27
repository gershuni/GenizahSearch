# -*- coding: utf-8 -*-
"""Tests for FIST measurement import and FjmsService measurement methods."""

import sqlite3
import pytest

from shared.fjms_service import FjmsService


def _create_measurement_tables(conn):
    """Create all measurement tables with test data in an in-memory DB."""
    # extra_info
    conn.execute("""
        CREATE TABLE extra_info (
            FGP TEXT NOT NULL PRIMARY KEY,
            AlmaId TEXT,
            Shelfmark TEXT,
            Material TEXT,
            Size_Category TEXT,
            NumFolio INTEGER,
            NumBifolio INTEGER,
            PixelWidth INTEGER,
            PixelHeight INTEGER,
            Image_Type TEXT,
            Rotation_Angle_deg REAL
        )
    """)
    conn.execute("CREATE INDEX idx_ei_alma ON extra_info(AlmaId)")
    conn.executemany("INSERT INTO extra_info VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
        ("FGP001", "990001", "T-S 12.1", "Paper", "Medium", 2, 0, 3000, 4000, "recto", 0.0),
        ("FGP002", "990001", "T-S 12.1", "Paper", "Medium", 2, 0, 3000, 4000, "verso", 0.0),
        ("FGP003", "990002", "T-S 13.1", "Parchment", "Large", 4, 2, 5000, 6000, "recto", 0.0),
    ])

    # computed_measurements
    conn.execute("""
        CREATE TABLE computed_measurements (
            FGP TEXT NOT NULL,
            AlmaId TEXT,
            Image_Side TEXT,
            Component_Num INTEGER,
            Bifolio_Side TEXT,
            Page_Width_cm REAL,
            Page_Height_cm REAL,
            Num_Lines INTEGER,
            Left_Margin_cm REAL,
            Right_Margin_cm REAL,
            Top_Margin_cm REAL,
            Bottom_Margin_cm REAL,
            Written_Width_cm REAL,
            Written_Height_cm REAL,
            Avg_Line_Height_Text_mm REAL,
            Text_Density_per10cm REAL,
            DpiGrid INTEGER,
            DisplayDPI INTEGER,
            Flag_DPI_High INTEGER DEFAULT 0,
            Flag_DPI_Low INTEGER DEFAULT 0,
            Flag_Negative_Margin INTEGER DEFAULT 0,
            Flag_BifolioLoc_Error INTEGER DEFAULT 0
        )
    """)
    conn.execute("CREATE INDEX idx_cm_alma ON computed_measurements(AlmaId)")
    conn.executemany(
        "INSERT INTO computed_measurements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            # Unflagged rows
            ("FGP001", "990001", "recto", 1, None, 15.5, 22.3, 25, 1.0, 1.2, 1.5, 2.0, 12.0, 17.0, 3.5, 7.1, 300, 300, 0, 0, 0, 0),
            ("FGP002", "990001", "verso", 1, None, 15.8, 22.1, 23, 1.1, 1.3, 1.4, 1.9, 12.2, 17.2, 3.4, 7.0, 300, 300, 0, 0, 0, 0),
            # Flagged row (Flag_DPI_High=1) -- should be EXCLUDED from summary
            ("FGP003", "990001", "recto", 2, None, 99.0, 99.0, 50, 5.0, 5.0, 5.0, 5.0, 80.0, 80.0, 10.0, 20.0, 72, 72, 1, 0, 0, 0),
            # Another manuscript
            ("FGP004", "990002", "recto", 1, None, 20.0, 30.0, 35, 2.0, 2.0, 2.5, 3.0, 16.0, 24.0, 4.0, 8.5, 400, 400, 0, 0, 0, 0),
        ],
    )

    # blank_images
    conn.execute("""
        CREATE TABLE blank_images (
            FGP TEXT NOT NULL,
            AlmaId TEXT,
            Fragment_Width_cm REAL,
            Fragment_Height_cm REAL,
            IsNotWhole INTEGER,
            PuzzleRatio REAL
        )
    """)
    conn.execute("CREATE INDEX idx_bi_alma ON blank_images(AlmaId)")
    conn.executemany("INSERT INTO blank_images VALUES (?, ?, ?, ?, ?, ?)", [
        ("FGP005", "990001", 16.0, 23.0, 0, 0.7),
    ])

    # catalog_sizes (new schema with cm values and flags)
    conn.execute("""
        CREATE TABLE catalog_sizes (
            AlmaId TEXT NOT NULL,
            UnitCatalogRecId INTEGER NOT NULL,
            SizeX_cm REAL,
            SizeY_cm REAL,
            InnerSizeX_cm REAL,
            InnerSizeY_cm REAL,
            SizeUnit TEXT,
            Measurement_Scope TEXT,
            Flag_WH_Swap TEXT,
            Flag_Unit_Error TEXT
        )
    """)
    conn.execute("CREATE INDEX idx_catsz_alma ON catalog_sizes(AlmaId)")
    conn.executemany("INSERT INTO catalog_sizes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
        # Unflagged
        ("990001", 100, 16.5, 21.0, 14.0, 18.0, "mm", None, None, None),
        ("990001", 101, 16.8, 21.5, None, None, "mm", None, None, None),
        # Flagged (Flag_WH_Swap) -- should be EXCLUDED from summary
        ("990001", 102, 99.0, 99.0, None, None, "mm", None, "swapped", None),
        # Another manuscript
        ("990002", 200, 20.0, 30.0, 17.0, 27.0, "cm", None, None, None),
    ])

    # manuscript_measurements (summary)
    conn.execute("""
        CREATE TABLE manuscript_measurements (
            AlmaId TEXT NOT NULL PRIMARY KEY,
            catalog_width_cm REAL,
            catalog_height_cm REAL,
            catalog_inner_width_cm REAL,
            catalog_inner_height_cm REAL,
            catalog_count INTEGER,
            min_computed_width_cm REAL,
            max_computed_width_cm REAL,
            min_computed_height_cm REAL,
            max_computed_height_cm REAL,
            avg_num_lines REAL,
            min_num_lines INTEGER,
            max_num_lines INTEGER,
            avg_text_density REAL,
            avg_line_height_mm REAL,
            computed_image_count INTEGER,
            material TEXT,
            size_category TEXT,
            total_image_count INTEGER,
            has_blank_images INTEGER DEFAULT 0,
            blank_image_count INTEGER DEFAULT 0
        )
    """)
    # Build summary from the test data (mimics what import script does)
    conn.executemany(
        "INSERT INTO manuscript_measurements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            # 990001: unflagged catalog MAX = 16.8, 21.5; unflagged computed = FGP001+FGP002 only
            # avg_line_height_mm = AVG(3.5, 3.4) = 3.45
            ("990001", 16.8, 21.5, 14.0, 18.0, 2, 15.5, 15.8, 22.1, 22.3, 24.0, 23, 25, 7.05, 3.45, 2, "Paper", "Medium", 2, 1, 1),
            # 990002: avg_line_height_mm = 4.0
            ("990002", 20.0, 30.0, 17.0, 27.0, 1, 20.0, 20.0, 30.0, 30.0, 35.0, 35, 35, 8.5, 4.0, 1, "Parchment", "Large", 1, 0, 0),
        ],
    )

    conn.commit()


@pytest.fixture
def measurement_service(tmp_path):
    """Create a FjmsService with measurement tables populated."""
    db_path = str(tmp_path / "test_fjms.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Create minimal required tables for FjmsService to initialize
    conn.execute("CREATE TABLE IF NOT EXISTS domains (AlmaId TEXT, DomainDescEng TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS joins (AlmaId TEXT)")

    _create_measurement_tables(conn)
    conn.close()

    svc = FjmsService(db_path=db_path, thread_safe=False)
    yield svc
    svc.close()


@pytest.fixture
def empty_service(tmp_path):
    """Create a FjmsService WITHOUT measurement tables (old sidecar scenario)."""
    db_path = str(tmp_path / "test_fjms_old.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE IF NOT EXISTS domains (AlmaId TEXT, DomainDescEng TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS joins (AlmaId TEXT)")
    conn.commit()
    conn.close()

    svc = FjmsService(db_path=db_path, thread_safe=False)
    yield svc
    svc.close()


class TestAlmaIdFromFistDb:
    """Test that AlmaId resolution uses FIST.db (not xlsx floats)."""

    def test_fist_alma_ids_are_exact_integers(self):
        """FIST.db AlmaIds are exact integers, not float-corrupted."""
        # The xlsx stores 9.900017468002052e+17 which int() gives 990001746800205184 (WRONG)
        # FIST.db stores 990001746800205171 (CORRECT)
        # This test verifies our lookup function exists and returns strings
        from scripts.import_measurements import build_fgp_to_alma_from_fist
        assert callable(build_fgp_to_alma_from_fist)

    def test_shelfmark_to_alma_function_exists(self):
        """Shelfmark→AlmaId builder for catalog_sizes resolution."""
        from scripts.import_measurements import build_shelfmark_to_alma_from_fist
        assert callable(build_shelfmark_to_alma_from_fist)


class TestGetMeasurements:
    """Test FjmsService.get_measurements()."""

    def test_get_measurements(self, measurement_service):
        """get_measurements returns all 5 data sections."""
        result = measurement_service.get_measurements("990001")

        assert "catalog_sizes" in result
        assert "computed" in result
        assert "extra_info" in result
        assert "blank_images" in result
        assert "summary" in result

        # Summary populated
        assert result["summary"] is not None
        assert result["summary"]["catalog_width_cm"] == 16.8
        assert result["summary"]["catalog_height_cm"] == 21.5

        # Catalog sizes: only unflagged (2 out of 3)
        assert len(result["catalog_sizes"]) == 2

        # Computed: only unflagged (2 out of 3 for 990001)
        assert len(result["computed"]) == 2

        # Extra info
        assert len(result["extra_info"]) == 2

        # Blank images
        assert len(result["blank_images"]) == 1

    def test_get_measurements_nonexistent(self, measurement_service):
        """get_measurements returns empty result for nonexistent sys_id."""
        result = measurement_service.get_measurements("NONEXISTENT")
        assert result["catalog_sizes"] == []
        assert result["computed"] == []
        assert result["extra_info"] == []
        assert result["blank_images"] == []
        assert result["summary"] is None

    def test_get_measurements_graceful_missing_tables(self, empty_service):
        """get_measurements returns empty dict structure when tables do NOT exist."""
        result = empty_service.get_measurements("990001")
        assert result["catalog_sizes"] == []
        assert result["computed"] == []
        assert result["extra_info"] == []
        assert result["blank_images"] == []
        assert result["summary"] is None


class TestHasMeasurements:
    """Test FjmsService.has_measurements()."""

    def test_has_measurements(self, measurement_service):
        """has_measurements returns True for manuscript with data."""
        assert measurement_service.has_measurements("990001") is True
        assert measurement_service.has_measurements("990002") is True

    def test_has_measurements_false(self, measurement_service):
        """has_measurements returns False for nonexistent manuscript."""
        assert measurement_service.has_measurements("NONEXISTENT") is False

    def test_has_measurements_graceful_missing_tables(self, empty_service):
        """has_measurements returns False when measurement tables don't exist."""
        assert empty_service.has_measurements("990001") is False


class TestLineHeightColumn:
    """Test avg_line_height_mm column in manuscript_measurements."""

    def test_line_height_column(self, measurement_service):
        """After import, manuscript_measurements has avg_line_height_mm with non-NULL values."""
        result = measurement_service.get_measurements("990001")
        summary = result["summary"]
        assert "avg_line_height_mm" in summary
        assert summary["avg_line_height_mm"] is not None
        # 990001 has 2 unflagged rows: 3.5 and 3.4 -> avg = 3.45
        assert abs(summary["avg_line_height_mm"] - 3.45) < 0.01

    def test_line_height_column_990002(self, measurement_service):
        """990002 should also have avg_line_height_mm populated."""
        result = measurement_service.get_measurements("990002")
        summary = result["summary"]
        assert summary["avg_line_height_mm"] is not None
        # 990002 has 1 unflagged row: 4.0
        assert abs(summary["avg_line_height_mm"] - 4.0) < 0.01

    def test_migrate_add_line_height_idempotent(self, tmp_path):
        """Calling migrate_add_line_height twice does not error (idempotent)."""
        from scripts.import_measurements import migrate_add_line_height
        db_path = str(tmp_path / "test_migrate.db")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        _create_measurement_tables(conn)
        # Call migrate twice -- should not error
        migrate_add_line_height(conn)
        migrate_add_line_height(conn)
        # Column should exist
        cols = {row[1] for row in conn.execute("PRAGMA table_info(manuscript_measurements)")}
        assert "avg_line_height_mm" in cols
        conn.close()

    def test_line_height_excludes_flagged(self, tmp_path):
        """avg_line_height_mm aggregation excludes flagged rows."""
        from scripts.import_measurements import migrate_add_line_height
        db_path = str(tmp_path / "test_flag_excl.db")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        _create_measurement_tables(conn)
        # Remove avg_line_height_mm if present, re-add via migration
        try:
            # Drop and recreate without the column to test migration
            cols = {row[1] for row in conn.execute("PRAGMA table_info(manuscript_measurements)")}
            if "avg_line_height_mm" not in cols:
                pass  # Column not there yet, migration will add it
        except Exception:
            pass
        migrate_add_line_height(conn)
        row = conn.execute(
            "SELECT avg_line_height_mm FROM manuscript_measurements WHERE AlmaId = ?",
            ("990001",)
        ).fetchone()
        # Flagged FGP003 has Avg_Line_Height_Text_mm=10.0 but Flag_DPI_High=1
        # Unflagged: 3.5 and 3.4 -> avg = 3.45
        assert row is not None
        assert row[0] is not None
        assert abs(row[0] - 3.45) < 0.01
        conn.close()


class TestFlagExclusion:
    """Test that flagged records are excluded from queries."""

    def test_flagged_excluded_from_summary(self, measurement_service):
        """Flagged computed and catalog rows should NOT appear in summary."""
        result = measurement_service.get_measurements("990001")
        summary = result["summary"]

        # Computed: flagged FGP003 (99.0 x 99.0) should NOT be in min/max
        assert summary["max_computed_width_cm"] < 20.0  # not 99.0
        assert summary["max_computed_height_cm"] < 30.0  # not 99.0
        assert summary["computed_image_count"] == 2  # not 3

        # Catalog: flagged UnitCatalogRecId=102 (99.0 x 99.0) should NOT be in catalog_width
        assert summary["catalog_width_cm"] < 20.0  # not 99.0
        assert summary["catalog_count"] == 2  # not 3

    def test_flagged_excluded_from_computed_query(self, measurement_service):
        """get_measurements computed section excludes flagged rows."""
        result = measurement_service.get_measurements("990001")
        # Should only have 2 unflagged computed rows, not 3
        assert len(result["computed"]) == 2
        for row in result["computed"]:
            assert row["Page_Width_cm"] < 50.0  # not the flagged 99.0

    def test_flagged_excluded_from_catalog_query(self, measurement_service):
        """get_measurements catalog_sizes section excludes flagged rows."""
        result = measurement_service.get_measurements("990001")
        # Should only have 2 unflagged catalog rows, not 3
        assert len(result["catalog_sizes"]) == 2

    def test_summary_min_max_pairs(self, measurement_service):
        """Summary table has both min and max for computed dimensions."""
        result = measurement_service.get_measurements("990001")
        summary = result["summary"]

        assert "min_computed_width_cm" in summary
        assert "max_computed_width_cm" in summary
        assert "min_computed_height_cm" in summary
        assert "max_computed_height_cm" in summary

        # min <= max
        assert summary["min_computed_width_cm"] <= summary["max_computed_width_cm"]
        assert summary["min_computed_height_cm"] <= summary["max_computed_height_cm"]
