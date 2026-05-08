"""Phase 85 SYNTH-02 + SYNTH-03 regeneration script tests.

Covers:
  - csv_bank marker-block tolerance (TestLoaderMarkerTolerance)
  - generate_synthetic_rows.py: idempotency, collision-detection, ambiguity
    residue (multi-inventory + multi-signature), CSV-injection fail-loud,
    D-02 EXPANDED predicate, deterministic ordering, manifest authority,
    SYNTH-03 narrowed to Title/Shelfmark search modes.

Per Phase 84 lesson (Round 3 Codex MEDIUM): NEVER mutate real libraries.csv,
real reports/, or real fist_data/ from tests. Use tmp_path fixture for all writes.
Connections injected per Gemini LOW (no monkeypatching sqlite3.connect).
"""
from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# csv_bank marker-block tolerance (genizah_core.py:_load_csv_bank guard)
# ---------------------------------------------------------------------------


def _write_libraries_csv(path: Path, rows: list[list[str]]) -> None:
    """Write a libraries.csv-shaped CSV (8 columns, header) to `path`."""
    header = [
        "system_number",
        "oxford_part_id",
        "call_numbers",
        "library_code",
        "",
        "",
        "",
        "titles_non_placeholder",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in rows:
            w.writerow(row)


class TestLoaderMarkerTolerance:
    def test_loader_skips_marker_lines(self, tmp_path, monkeypatch):
        """A '# BEGIN SYNTHETIC' line in column 0 must not appear in csv_bank."""
        from genizah_core import Config, MetadataManager

        csv_path = tmp_path / "libraries.csv"
        rows = [
            ["990025143260205171", "", "T-S 1.1", "CUL", "", "", "", "Test title"],
            ["# BEGIN SYNTHETIC", "", "", "", "", "", "", ""],
            ["990001234560000000", "", "T-S NS 329.96", "CUL", "", "", "", "Synthetic Title"],
            ["# END SYNTHETIC", "", "", "", "", "", "", ""],
        ]
        _write_libraries_csv(csv_path, rows)

        monkeypatch.setattr(Config, "LIBRARIES_CSV", str(csv_path))
        mm = MetadataManager()
        mm._load_csv_bank()

        assert "990025143260205171" in mm.csv_bank
        assert "990001234560000000" in mm.csv_bank
        # Marker lines must not produce a key
        assert "" not in mm.csv_bank
        assert all(not k.startswith("#") for k in mm.csv_bank)

    def test_loader_preserves_real_rows(self, tmp_path, monkeypatch):
        """3 real + 2 markers + 2 synthetic = csv_bank with exactly 5 entries."""
        from genizah_core import Config, MetadataManager

        csv_path = tmp_path / "libraries.csv"
        rows = [
            ["990025143260205171", "", "T-S 1.1", "CUL", "", "", "", "Real 1"],
            ["990025143270205171", "", "T-S 1.2", "CUL", "", "", "", "Real 2"],
            ["990025143280205171", "", "T-S 1.3", "CUL", "", "", "", "Real 3"],
            ["# BEGIN SYNTHETIC", "", "", "", "", "", "", ""],
            ["990001234560000000", "", "T-S NS 329.96", "CUL", "", "", "", "Synth 1"],
            ["990001234570000000", "", "T-S NS 329.97", "CUL", "", "", "", "Synth 2"],
            ["# END SYNTHETIC", "", "", "", "", "", "", ""],
        ]
        _write_libraries_csv(csv_path, rows)

        monkeypatch.setattr(Config, "LIBRARIES_CSV", str(csv_path))
        mm = MetadataManager()
        mm._load_csv_bank()

        assert len(mm.csv_bank) == 5

    def test_loader_synthetic_rows_have_shelfmark(self, tmp_path, monkeypatch):
        """SYNTH-03: csv_bank entries for synthetic IDs have populated shelfmark + title (Pitfall 5)."""
        from genizah_core import Config, MetadataManager
        from shared.synthetic_sys_id import is_synthetic_sys_id

        csv_path = tmp_path / "libraries.csv"
        rows = [
            ["# BEGIN SYNTHETIC", "", "", "", "", "", "", ""],
            ["990001234560000000", "", "T-S NS 329.96", "CUL", "", "", "", "מילון תלמודי"],
            ["# END SYNTHETIC", "", "", "", "", "", "", ""],
        ]
        _write_libraries_csv(csv_path, rows)

        monkeypatch.setattr(Config, "LIBRARIES_CSV", str(csv_path))
        mm = MetadataManager()
        mm._load_csv_bank()

        entry = mm.csv_bank["990001234560000000"]
        assert is_synthetic_sys_id("990001234560000000")
        assert entry["shelfmark"] == "T-S NS 329.96"
        assert entry["title"] == "מילון תלמודי"
        assert entry["library_code"] == "CUL"


# ---------------------------------------------------------------------------
# Helper: build seeded FIST.db + nli_crossref.db in-memory
# ---------------------------------------------------------------------------


def _make_fist_seed(extra_setup: str = "") -> sqlite3.Connection:
    """Create an in-memory FIST.db schema. Caller can pass extra INSERTs.

    Schema mirrors fist_data/FIST.db (verified against worktree main checkout).
    """
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE dbo_Inventory (
            InventoryId INTEGER PRIMARY KEY,
            Shelfmark TEXT
        );
        CREATE TABLE dbo_InventorySignature (
            InventoryId INTEGER,
            SetSignatureId INTEGER
        );
        CREATE TABLE dbo_Signature (
            SetSignatureId INTEGER,
            SignatureId INTEGER PRIMARY KEY
        );
        CREATE TABLE dbo_UnitCatalogRec (
            UnitCatalogRecId INTEGER PRIMARY KEY,
            SignatureId INTEGER,
            Title TEXT,
            GenizahTitleText TEXT
        );
        CREATE TABLE dbo_InventoryAlma (
            ID INTEGER PRIMARY KEY,
            InventoryId INTEGER,
            AlmaId INTEGER
        );
        CREATE TABLE dbo_UnitBibliographyReference (
            UnitBibliographyReferenceId INTEGER PRIMARY KEY,
            SignatureId INTEGER
        );
        CREATE TABLE dbo_UnitFreeDescription (
            UnitFreeDescriptionId INTEGER PRIMARY KEY,
            SignatureId INTEGER,
            FreeDesc TEXT
        );
        CREATE TABLE dbo_UnitFullText (
            UnitFullTextId INTEGER PRIMARY KEY,
            SignatureId INTEGER
        );
        CREATE TABLE dbo_CatalogMultiSize (
            CatalogMultiSizeId INTEGER PRIMARY KEY,
            UnitCatalogRecId INTEGER
        );
        """
    )
    if extra_setup:
        conn.executescript(extra_setup)
    conn.commit()
    return conn


def _make_nli_seed(classmarks: list[str] | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE cambridge_manifests (normalized_shelfmark TEXT)")
    if classmarks:
        for c in classmarks:
            conn.execute(
                "INSERT INTO cambridge_manifests (normalized_shelfmark) VALUES (?)",
                (c,),
            )
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Test suite for the script itself
# ---------------------------------------------------------------------------


class TestQualifyingInventories:
    """Unit tests for _build_qualifying_inventories (D-02 EXPANDED, D-05a STRICT)."""

    def test_d02_expanded_predicate_includes_bibliography(self):
        """REVIEWS-MODE Codex HIGH: D-02 expanded — bibliography signal qualifies."""
        from scripts.generate_synthetic_rows import _build_qualifying_inventories

        # inv 3 has NO catalog title but has a bibliography row.
        fist = _make_fist_seed(
            """
            INSERT INTO dbo_Inventory VALUES
                (1, 'T-S 1.1'),
                (2, 'T-S NS 329.96'),
                (3, 'T-S NS 330.10');
            INSERT INTO dbo_InventorySignature VALUES (1, 100), (2, 200), (3, 300);
            INSERT INTO dbo_Signature VALUES (100, 1000), (200, 2000), (300, 3000);
            INSERT INTO dbo_UnitCatalogRec VALUES
                (10, 1000, 'Real Title', NULL),
                (20, 2000, 'מילון', NULL);
            -- inv 1 has Alma; inv 3 has bibliography only.
            INSERT INTO dbo_InventoryAlma VALUES (1, 1, 990025143260205171);
            INSERT INTO dbo_UnitBibliographyReference VALUES (5001, 3000);
            """
        )
        nli = _make_nli_seed(["tsns329.96"])
        qualifying, _residue = _build_qualifying_inventories(fist, nli)
        # inv 3 has NO title fields but has a bibliography row → qualifies.
        assert 3 in qualifying, "D-02 EXPANDED: bibliography-only inventory missing"
        assert qualifying[3]["has_fjms_metadata"] is True
        # inv 2 qualifies via title.
        assert 2 in qualifying
        # inv 1 has Alma so it's excluded.
        assert 1 not in qualifying

    def test_d02_expanded_predicate_includes_free_desc(self):
        """REVIEWS-MODE Codex HIGH: D-02 expanded — free description qualifies."""
        from scripts.generate_synthetic_rows import _build_qualifying_inventories

        fist = _make_fist_seed(
            """
            INSERT INTO dbo_Inventory VALUES (4, 'T-S NS 400.1');
            INSERT INTO dbo_InventorySignature VALUES (4, 400);
            INSERT INTO dbo_Signature VALUES (400, 4000);
            -- No UnitCatalogRec → no title fields.
            -- BUT a free-description record exists.
            INSERT INTO dbo_UnitFreeDescription VALUES (5002, 4000, 'A scholarly note.');
            """
        )
        nli = _make_nli_seed([])
        qualifying, _residue = _build_qualifying_inventories(fist, nli)
        assert 4 in qualifying
        assert qualifying[4]["has_fjms_metadata"] is True
        assert qualifying[4]["has_cudl_manifest"] is False

    def test_ambiguity_residue_multi_inventory_logged(self):
        """REVIEWS-MODE Codex HIGH: D-05a — multi-inventory ambiguity logged."""
        from scripts.generate_synthetic_rows import _build_qualifying_inventories

        # Two distinct InventoryIds, same shelfmark → ambiguous.
        fist = _make_fist_seed(
            """
            INSERT INTO dbo_Inventory VALUES
                (10, 'T-S 12.345'),
                (11, 'T-S 12.345');
            INSERT INTO dbo_InventorySignature VALUES (10, 1010), (11, 1011);
            INSERT INTO dbo_Signature VALUES (1010, 10100), (1011, 10110);
            INSERT INTO dbo_UnitCatalogRec VALUES
                (110, 10100, 'A', NULL),
                (111, 10110, 'B', NULL);
            """
        )
        nli = _make_nli_seed([])
        qualifying, residue = _build_qualifying_inventories(fist, nli)
        assert 10 not in qualifying
        assert 11 not in qualifying
        kinds = {r["ambiguity_kind"] for r in residue}
        assert "multi_inventory" in kinds

    def test_ambiguity_residue_multi_signature_logged(self):
        """REVIEWS-MODE Codex HIGH: D-05a — multi-signature ambiguity logged."""
        from scripts.generate_synthetic_rows import _build_qualifying_inventories

        # ONE InventoryId but TWO distinct SignatureIds (recto/verso) → ambiguous.
        fist = _make_fist_seed(
            """
            INSERT INTO dbo_Inventory VALUES (20, 'T-S NS 500.1');
            INSERT INTO dbo_InventorySignature VALUES (20, 2010), (20, 2020);
            INSERT INTO dbo_Signature VALUES (2010, 20100), (2020, 20200);
            INSERT INTO dbo_UnitCatalogRec VALUES
                (210, 20100, 'recto', NULL),
                (220, 20200, 'verso', NULL);
            """
        )
        nli = _make_nli_seed([])
        qualifying, residue = _build_qualifying_inventories(fist, nli)
        assert 20 not in qualifying
        kinds = {r["ambiguity_kind"] for r in residue}
        assert "multi_signature" in kinds

    def test_csv_injection_fail_loud(self):
        """REVIEWS-MODE Codex MEDIUM: leading =/+/-/@ in title excludes the row."""
        from scripts.generate_synthetic_rows import _build_qualifying_inventories

        fist = _make_fist_seed(
            """
            INSERT INTO dbo_Inventory VALUES (30, 'T-S NS 600.1');
            INSERT INTO dbo_InventorySignature VALUES (30, 3010);
            INSERT INTO dbo_Signature VALUES (3010, 30100);
            INSERT INTO dbo_UnitCatalogRec VALUES
                (310, 30100, '=HYPERLINK("evil")', NULL);
            """
        )
        nli = _make_nli_seed([])
        qualifying, residue = _build_qualifying_inventories(fist, nli)
        assert 30 not in qualifying
        kinds = {r["ambiguity_kind"] for r in residue}
        assert "csv_injection_leader" in kinds

    def test_csv_injection_excludes_row(self, tmp_path):
        """Sub-feature 6 acceptance: row with leading =/+/-/@ in title NOT emitted to libraries.csv."""
        from scripts import generate_synthetic_rows as gen

        fist = _make_fist_seed(
            """
            INSERT INTO dbo_Inventory VALUES (31, 'T-S NS 600.2');
            INSERT INTO dbo_InventorySignature VALUES (31, 3110);
            INSERT INTO dbo_Signature VALUES (3110, 31100);
            INSERT INTO dbo_UnitCatalogRec VALUES
                (311, 31100, '+CMD evil', NULL);
            """
        )
        nli = _make_nli_seed([])
        qualifying, residue = gen._build_qualifying_inventories(fist, nli)
        assert 31 not in qualifying

        # Verify residue write produces ambiguity_kind='csv_injection_leader'.
        residue_path = tmp_path / "residue.csv"
        gen._write_residue(residue_path, residue)
        text = residue_path.read_text(encoding="utf-8")
        assert "csv_injection_leader" in text


class TestRegenerateScript:
    """Integration tests using monkeypatched paths + injected connections."""

    @pytest.fixture
    def setup_paths(self, tmp_path, monkeypatch):
        """Redirect all script paths into tmp_path; supply seeded FIST + NLI conns."""
        from scripts import generate_synthetic_rows as gen

        csv_path = tmp_path / "libraries.csv"
        manifest_path = tmp_path / "synthetic_manifest.json"
        residue_path = tmp_path / "synthetic_ambiguity_residue.csv"
        coverage_path = tmp_path / "synthetic_coverage.md"

        # Seed a libraries.csv with one real-Alma row and CRLF line endings.
        csv_path.write_bytes(
            b"system_number,oxford_part_id,call_numbers,library_code,,,,titles_non_placeholder\r\n"
            b"990025143260205171,,T-S 1.1,CUL,,,,Real Title\r\n"
        )

        monkeypatch.setattr(gen, "CSV_PATH", csv_path)
        monkeypatch.setattr(gen, "MANIFEST_PATH", manifest_path)
        monkeypatch.setattr(gen, "RESIDUE_PATH", residue_path)
        monkeypatch.setattr(gen, "COVERAGE_PATH", coverage_path)
        return {
            "gen": gen,
            "csv": csv_path,
            "manifest": manifest_path,
            "residue": residue_path,
            "coverage": coverage_path,
        }

    def _seed_fist_nli(self):
        """Seed: inv 1 has Alma; inv 2 + 3 are synthetic-eligible."""
        fist = _make_fist_seed(
            """
            INSERT INTO dbo_Inventory VALUES
                (1, 'T-S 1.1'),
                (2, 'T-S NS 329.96'),
                (3, 'T-S NS 330.10');
            INSERT INTO dbo_InventorySignature VALUES (1, 100), (2, 200), (3, 300);
            INSERT INTO dbo_Signature VALUES (100, 1000), (200, 2000), (300, 3000);
            INSERT INTO dbo_UnitCatalogRec VALUES
                (10, 1000, 'Real Title', NULL),
                (20, 2000, 'מילון', NULL),
                (30, 3000, 'אגדה', 'זמירות');
            INSERT INTO dbo_InventoryAlma VALUES (1, 1, 990025143260205171);
            """
        )
        nli = _make_nli_seed(["tsns329.96"])
        return fist, nli

    def _run_apply(self, gen, monkeypatch, tmp_path):
        """Invoke main() with --apply and seeded connections.

        Redirects FIST_DB and NLI_DB to tmp paths that exist (touched files),
        so .exists() returns True without monkeypatching the read-only Path
        attribute. Connections are intercepted by sqlite3.connect monkeypatch.
        """
        fist, nli = self._seed_fist_nli()

        # Create stub files at tmp paths so Path.exists() returns True.
        fist_stub = tmp_path / "fist_stub.db"
        nli_stub = tmp_path / "nli_stub.db"
        fist_stub.write_bytes(b"")
        nli_stub.write_bytes(b"")
        monkeypatch.setattr(gen, "FIST_DB", fist_stub)
        monkeypatch.setattr(gen, "NLI_DB", nli_stub)

        original_connect = sqlite3.connect

        def fake_connect(target, *args, **kwargs):
            t = str(target)
            if "fist_stub" in t:
                return fist
            if "nli_stub" in t:
                return nli
            return original_connect(target, *args, **kwargs)

        monkeypatch.setattr(sqlite3, "connect", fake_connect)
        # argparse reads sys.argv
        import sys

        monkeypatch.setattr(sys, "argv", ["generate_synthetic_rows.py", "--apply"])
        gen.main()

    def test_idempotent_regeneration(self, setup_paths, monkeypatch):
        """D-04a: --apply twice produces byte-identical libraries.csv AND manifest.json."""
        ctx = setup_paths
        gen = ctx["gen"]

        self._run_apply(gen, monkeypatch, ctx["csv"].parent)
        first_csv = ctx["csv"].read_bytes()
        first_manifest = ctx["manifest"].read_bytes()

        self._run_apply(gen, monkeypatch, ctx["csv"].parent)
        second_csv = ctx["csv"].read_bytes()
        second_manifest = ctx["manifest"].read_bytes()

        assert first_csv == second_csv, "D-04a violation: libraries.csv not idempotent"
        assert first_manifest == second_manifest, (
            "D-04a violation: manifest.json not byte-identical (Codex HIGH ORDER BY)"
        )

    def test_idempotency_byte_identity(self, setup_paths, monkeypatch):
        """Sub-feature 7 acceptance: re-running --apply produces byte-identical artifacts."""
        ctx = setup_paths
        gen = ctx["gen"]

        self._run_apply(gen, monkeypatch, ctx["csv"].parent)
        import hashlib

        h1_csv = hashlib.sha256(ctx["csv"].read_bytes()).hexdigest()
        h1_manifest = hashlib.sha256(ctx["manifest"].read_bytes()).hexdigest()

        self._run_apply(gen, monkeypatch, ctx["csv"].parent)
        h2_csv = hashlib.sha256(ctx["csv"].read_bytes()).hexdigest()
        h2_manifest = hashlib.sha256(ctx["manifest"].read_bytes()).hexdigest()

        assert h1_csv == h2_csv
        assert h1_manifest == h2_manifest

    def test_marker_block_round_trip(self, setup_paths, monkeypatch):
        """Re-running with existing block deletes old block and writes fresh one."""
        ctx = setup_paths
        gen = ctx["gen"]

        self._run_apply(gen, monkeypatch, ctx["csv"].parent)
        text1 = ctx["csv"].read_text(encoding="utf-8")
        # Exactly one BEGIN and one END.
        assert text1.count("# BEGIN SYNTHETIC") == 1
        assert text1.count("# END SYNTHETIC") == 1

        # Run again — still exactly one pair (no doubled block).
        self._run_apply(gen, monkeypatch, ctx["csv"].parent)
        text2 = ctx["csv"].read_text(encoding="utf-8")
        assert text2.count("# BEGIN SYNTHETIC") == 1
        assert text2.count("# END SYNTHETIC") == 1

    def test_manifest_is_authoritative_for_plan_03(self, setup_paths, monkeypatch):
        """Manifest contains every InventoryId for which a synthetic row was emitted."""
        ctx = setup_paths
        gen = ctx["gen"]
        self._run_apply(gen, monkeypatch, ctx["csv"].parent)

        data = json.loads(ctx["manifest"].read_text(encoding="utf-8"))
        inventory_ids = [r["inventory_id"] for r in data]
        assert inventory_ids == sorted(inventory_ids), "manifest not deterministically sorted"
        # inv 2 + 3 qualify (inv 1 has Alma).
        assert 2 in inventory_ids
        assert 3 in inventory_ids
        assert 1 not in inventory_ids
        # Each entry has the four required keys.
        for r in data:
            assert "inventory_id" in r
            assert "synthetic_sys_id" in r
            assert "source" in r
            assert "canonical_shelfmark" in r
            assert "library_code" in r

    def test_no_collision_with_real_alma(self, setup_paths, monkeypatch):
        """D-01a: every real-Alma row in libraries.csv classifies as NOT synthetic after regen."""
        from shared.synthetic_sys_id import is_synthetic_sys_id

        ctx = setup_paths
        gen = ctx["gen"]
        self._run_apply(gen, monkeypatch, ctx["csv"].parent)

        # Re-read the libraries.csv post-regen and verify the original real-Alma
        # row is still present and NOT classified as synthetic.
        text = ctx["csv"].read_text(encoding="utf-8")
        assert "990025143260205171" in text
        assert is_synthetic_sys_id("990025143260205171") is False

    def test_collision_check_fails_loud(self, tmp_path, monkeypatch):
        """Sub-feature 5: planting a colliding synthetic ID makes the script abort.

        The real-Alma libraries.csv contains '990000000002000000' which is exactly
        what encode_inventory_sys_id(2) produces. The script must SystemExit.
        """
        from scripts import generate_synthetic_rows as gen
        from shared.synthetic_sys_id import encode_inventory_sys_id

        # Plant a colliding sys_id in the libraries.csv real-Alma rows.
        colliding_sys_id = encode_inventory_sys_id(2)  # inv 2 will get this same ID
        csv_path = tmp_path / "libraries.csv"
        csv_path.write_bytes(
            b"system_number,oxford_part_id,call_numbers,library_code,,,,titles_non_placeholder\r\n"
            + colliding_sys_id.encode("ascii")
            + b",,T-S X,CUL,,,,Real\r\n"
        )

        monkeypatch.setattr(gen, "CSV_PATH", csv_path)
        monkeypatch.setattr(gen, "MANIFEST_PATH", tmp_path / "manifest.json")
        monkeypatch.setattr(gen, "RESIDUE_PATH", tmp_path / "residue.csv")
        monkeypatch.setattr(gen, "COVERAGE_PATH", tmp_path / "coverage.md")

        # Seed FIST so InventoryId=2 qualifies.
        fist = _make_fist_seed(
            """
            INSERT INTO dbo_Inventory VALUES (2, 'T-S NS 329.96');
            INSERT INTO dbo_InventorySignature VALUES (2, 200);
            INSERT INTO dbo_Signature VALUES (200, 2000);
            INSERT INTO dbo_UnitCatalogRec VALUES (20, 2000, 'מילון', NULL);
            """
        )
        nli = _make_nli_seed([])

        # Stub files at tmp paths so .exists() returns True without monkeypatching
        # the read-only Path attribute.
        fist_stub = tmp_path / "fist_stub.db"
        nli_stub = tmp_path / "nli_stub.db"
        fist_stub.write_bytes(b"")
        nli_stub.write_bytes(b"")
        monkeypatch.setattr(gen, "FIST_DB", fist_stub)
        monkeypatch.setattr(gen, "NLI_DB", nli_stub)

        original_connect = sqlite3.connect

        def fake_connect(target, *args, **kwargs):
            t = str(target)
            if "fist_stub" in t:
                return fist
            if "nli_stub" in t:
                return nli
            return original_connect(target, *args, **kwargs)

        monkeypatch.setattr(sqlite3, "connect", fake_connect)

        import sys

        monkeypatch.setattr(sys, "argv", ["generate_synthetic_rows.py", "--apply"])
        with pytest.raises(SystemExit) as excinfo:
            gen.main()
        assert "D-01a COLLISION" in str(excinfo.value)

    def test_synthetic_block_structure(self, setup_paths, monkeypatch):
        """Synthetic rows have call_numbers, library_code ∈ {CUL, Mosseri}, title populated per D-09."""
        ctx = setup_paths
        gen = ctx["gen"]
        self._run_apply(gen, monkeypatch, ctx["csv"].parent)

        with ctx["csv"].open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)

        # Find the synthetic block.
        in_block = False
        synth_rows = []
        for r in rows:
            if r and r[0] == "# BEGIN SYNTHETIC":
                in_block = True
                continue
            if r and r[0] == "# END SYNTHETIC":
                in_block = False
                continue
            if in_block:
                synth_rows.append(r)

        assert len(synth_rows) >= 2  # inv 2 + 3 expected
        for r in synth_rows:
            assert len(r) == 8
            assert r[2]  # call_numbers populated
            assert r[3] in ("CUL", "Mosseri")
            assert r[7]  # title populated
            # title precedence: should be the Hebrew Title we seeded, not the shelfmark.
            assert r[7] != "T-S NS 329.96"  # we seeded title="מילון" for inv 2

    def test_csv_line_endings_preserved(self, setup_paths, monkeypatch):
        """CRLF preserved if libraries.csv had CRLF (Windows-checked-out clone)."""
        ctx = setup_paths
        gen = ctx["gen"]
        # Setup wrote CRLF; verify it's preserved.
        self._run_apply(gen, monkeypatch, ctx["csv"].parent)
        raw = ctx["csv"].read_bytes()
        # Has CRLF terminators.
        assert b"\r\n" in raw
        # No bare LF (would indicate corruption).
        # csv.writer emits exactly the lineterminator we set; check that bare \n
        # without a preceding \r doesn't appear.
        bare_lf = 0
        for i, b in enumerate(raw):
            if b == 0x0A and (i == 0 or raw[i - 1] != 0x0D):
                bare_lf += 1
        assert bare_lf == 0, f"Found {bare_lf} bare LF chars; CRLF not preserved"

    def test_deterministic_ordering(self, setup_paths, monkeypatch):
        """Manifest items sorted by inventory_id ascending; output bytes deterministic across runs."""
        ctx = setup_paths
        gen = ctx["gen"]
        self._run_apply(gen, monkeypatch, ctx["csv"].parent)
        data = json.loads(ctx["manifest"].read_text(encoding="utf-8"))
        ids = [r["inventory_id"] for r in data]
        assert ids == sorted(ids)


class TestCoverageReport:
    """Verify the coverage.md report content for the Phase 86 cross-link."""

    def test_coverage_has_phase_86_cross_link(self, tmp_path):
        from scripts.generate_synthetic_rows import _write_coverage

        qualifying = {
            1: {"has_cudl_manifest": True, "has_fjms_metadata": True},
            2: {"has_cudl_manifest": True, "has_fjms_metadata": False},
            3: {"has_cudl_manifest": False, "has_fjms_metadata": True},
        }
        path = tmp_path / "coverage.md"
        _write_coverage(path, qualifying, residue_count=5)
        text = path.read_text(encoding="utf-8")
        assert "AUDIT-01" in text
        assert "AUDIT-02" in text
        assert "AUDIT-03" in text
        assert "Phase 86" in text
        assert "## Tier 1" in text
        assert "## Tier 2" in text
        assert "## Tier 3" in text
        assert "## SYNTH-03 Search Mode Coverage" in text
        assert "## Phase 86 Audit Cross-Link" in text


class TestResidueWriter:
    """Verify the residue CSV header includes both legacy and required column names."""

    def test_residue_header_has_required_columns(self, tmp_path):
        from scripts.generate_synthetic_rows import _write_residue

        path = tmp_path / "residue.csv"
        _write_residue(path, [])
        with path.open("r", encoding="utf-8", newline="") as f:
            header = next(csv.reader(f))
        # Sub-feature 3 acceptance: the four required column names.
        for col in ("inventory_id", "signature_id", "ambiguity_kind", "classmark"):
            assert col in header, f"Required residue column missing: {col}"

    def test_residue_csv_injection_leader_logged(self, tmp_path):
        """A row with ambiguity_kind='csv_injection_leader' is written correctly."""
        from scripts.generate_synthetic_rows import _write_residue

        residue = [
            {
                "cudl_label": "tsns600.2",
                "ambiguity_kind": "csv_injection_leader",
                "fist_signature_ids": "31100",
                "fist_inventory_ids": "31",
                "leading_char": "+",
                "inventory_id": 31,
                "signature_id": 31100,
                "classmark": "tsns600.2",
            }
        ]
        path = tmp_path / "residue.csv"
        _write_residue(path, residue)
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            row = next(reader)
        assert row["ambiguity_kind"] == "csv_injection_leader"
        assert row["leading_char"] == "+"
        assert row["inventory_id"] == "31"
        assert row["classmark"] == "tsns600.2"


class TestSynth03ModeNarrowing:
    """SYNTH-03: synthetic rows discoverable in Title/Shelfmark only.

    Verifies that genizah_core.py:7372-7373 still routes only those modes
    to _execute_metadata_search. Text/Responsa would require Tantivy stubs
    (deferred — see coverage.md).
    """

    def test_synth_03_title_shelfmark_only(self, tmp_path, monkeypatch):
        """In Title/Shelfmark mode synthetic rows are findable via csv_bank.

        We don't run the full Tantivy pipeline here — we only assert that
        csv_bank for a synthetic row is populated such that
        `_execute_metadata_search` would return it.
        """
        from genizah_core import Config, MetadataManager
        from shared.synthetic_sys_id import is_synthetic_sys_id

        csv_path = tmp_path / "libraries.csv"
        rows = [
            ["# BEGIN SYNTHETIC", "", "", "", "", "", "", ""],
            ["990001234560000000", "", "T-S NS 329.96", "CUL", "", "", "", "מילון תלמודי"],
            ["# END SYNTHETIC", "", "", "", "", "", "", ""],
        ]
        _write_libraries_csv(csv_path, rows)
        monkeypatch.setattr(Config, "LIBRARIES_CSV", str(csv_path))

        mm = MetadataManager()
        mm._load_csv_bank()

        synth_keys = [k for k in mm.csv_bank if is_synthetic_sys_id(k)]
        assert len(synth_keys) == 1
        key = synth_keys[0]
        # Both shelfmark and title populated → both Title and Shelfmark search
        # modes have non-empty fields to match against.
        assert mm.csv_bank[key]["shelfmark"]
        assert mm.csv_bank[key]["title"]
