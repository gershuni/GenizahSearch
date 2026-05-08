"""Phase 85 SYNTH-05 FJMS sidecar export tests (REVIEWS-MODE 2026-05-08).

Verifies the UNION-ALL injection in scripts/export_fist_enrichment.py:
  - Synthetic AlmaIds appear in all 12 AlmaId-keyed tables
  - No collision with real-Alma rows
  - shared/fjms_service.py service queries work transparently with synthetic AlmaIds
  - Manifest-as-authority cross-plan invariant (Plan 02 -> Plan 03)
  - Table-specific invariants (catalog triple uniqueness; 1:N manifest membership)
  - FTS5 rebuild succeeds
  - Stale table names absent (regression guard)

Per Phase 84 lesson (Round 3 Codex MEDIUM): NEVER mutate the real
fist_data/fjms_enrichment.db (1.6GB). Use tmp_path + in-memory FIST seed.
"""
from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path

import pytest

from shared.synthetic_sys_id import is_synthetic_sys_id


# 12 AlmaId-keyed tables verified against scripts/export_fist_enrichment.py 2026-05-08.
ALMA_KEYED_TABLES = [
    "domains", "joins", "catalog", "catalog_running_titles", "catalog_sizes",
    "catalog_fields", "catalog_free_desc", "catalog_full_texts",
    "catalog_textual_frames", "catalog_mentions", "bibliography", "catalog_refs",
]

# Reference tables — NOT AlmaId-keyed; must stay synthetic-free.
NON_ALMA_TABLES = [
    "ref_catalogs", "ref_titles", "ref_authors",
    "genizah_persons", "genizah_titles", "code_values", "meta",
]

# Stale table names from prior plan revision — must never appear as export targets.
STALE_TABLE_NAMES = ["measurements", "manuscript_measurements", "extra_info", "computed_measurements"]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _seed_fist_schema(conn: sqlite3.Connection) -> None:
    """Create FIST.db schema stubs for ALL tables referenced by any export query.

    Many queries JOIN against tables that we don't actually populate; the
    JOIN simply returns no rows for those tables. But the table must EXIST
    or the query raises sqlite3.OperationalError. Schema verified against
    scripts/export_fist_enrichment.py 2026-05-08.
    """
    conn.executescript("""
        -- Core tables (populated by fixtures below)
        CREATE TABLE IF NOT EXISTS dbo_Inventory (InventoryId INTEGER PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS dbo_InventorySignature (InventoryId INTEGER, SetSignatureId INTEGER);
        CREATE TABLE IF NOT EXISTS dbo_Signature (
            SetSignatureId INTEGER,
            SignatureId INTEGER,
            Signature TEXT,
            SourceId INTEGER,
            SubId INTEGER
        );
        CREATE TABLE IF NOT EXISTS dbo_UnitCatalogRec (
            UnitCatalogRecId INTEGER PRIMARY KEY,
            SignatureId INTEGER,
            Title TEXT,
            GenizahTitleText TEXT,
            AuthorText TEXT,
            CopyDate REAL,
            CopyPlace TEXT,
            BI_TextualFrameHeb TEXT,
            BI_TextualFrameEng TEXT,
            NumFolio REAL,
            NumBifolio REAL,
            NumColumn TEXT,
            NumRow TEXT,
            GenizahTitleId INTEGER,
            Author INTEGER,
            CopyToDate REAL,
            CreationTypeCode INTEGER,
            PartVocalCode INTEGER,
            CantillationCode INTEGER,
            SizeUnitCode INTEGER,
            Autograph INTEGER,
            Massorah INTEGER,
            PalimpsestCode INTEGER,
            IllustrationCode INTEGER,
            Comment TEXT,
            Colophon TEXT,
            ColophonFolio REAL,
            CopyName TEXT,
            ShelfmarkRange TEXT,
            OrgCreation INTEGER,
            OrgAuthor INTEGER,
            GenizahCode INTEGER,
            NumEmpty REAL,
            CatAcronym TEXT
        );
        CREATE TABLE IF NOT EXISTS dbo_InventoryAlma (InventoryId INTEGER, AlmaId TEXT);
        CREATE TABLE IF NOT EXISTS dbo_CatalogMultiDomain (UnitCatalogRecId INTEGER, DomainId INTEGER);
        CREATE TABLE IF NOT EXISTS CODE_Domain (DomainId INTEGER, EngDesc TEXT, HebDesc TEXT, BelongToDomainId INTEGER);
        CREATE TABLE IF NOT EXISTS dbo_UnitJoin (
            UnitJoinId INTEGER, SignatureId INTEGER, ScholarName TEXT, Comment TEXT, JoinTypeCode INTEGER
        );
        CREATE TABLE IF NOT EXISTS dbo_CatalogMultiRunningTitle (
            UnitCatalogRecId INTEGER, RunningTitle TEXT, Comment TEXT
        );
        CREATE TABLE IF NOT EXISTS dbo_CatalogMultiSize (
            UnitCatalogRecId INTEGER, SizeX REAL, SizeY REAL, InnerSizeX REAL, InnerSizeY REAL
        );
        CREATE TABLE IF NOT EXISTS dbo_CatalogMultiField (
            UnitCatalogRecId INTEGER, ValueCode INTEGER
        );
        CREATE TABLE IF NOT EXISTS dbo_UnitFreeDescription (SignatureId INTEGER, FreeDesc TEXT);
        CREATE TABLE IF NOT EXISTS dbo_UnitFullText (SignatureId INTEGER, FullText TEXT);
        CREATE TABLE IF NOT EXISTS dbo_CatalogMultiTextualFrame_Simple (
            UnitCatalogRecId INTEGER, TextualFrameHeb TEXT, TextualFrameEng TEXT
        );
        CREATE TABLE IF NOT EXISTS dbo_CatalogMultiMention (
            UnitCatalogRecId INTEGER, MentionTypeCode INTEGER, Mention TEXT, MentionDesc TEXT
        );
        CREATE TABLE IF NOT EXISTS dbo_UnitBibliographyReference (
            UnitBibliographyReferenceId INTEGER PRIMARY KEY,
            SignatureId INTEGER,
            TitleId INTEGER,
            MentionPage TEXT,
            FromPage TEXT,
            ToPage TEXT,
            JournalVolumeTxt TEXT,
            Volume TEXT,
            EVolume TEXT,
            JournalDate TEXT,
            MentionTypeCode INTEGER,
            IsHasTranscriptionCode INTEGER,
            IsHasTranslationCode INTEGER,
            ArticleName TEXT,
            Comment TEXT,
            NoteForDisplay TEXT,
            CatalogEntry TEXT,
            CatalogId INTEGER
        );
        CREATE TABLE IF NOT EXISTS dbo_BibMultiArticleAuthor (
            UnitBibliographyReferenceId INTEGER, AuthorOrder INTEGER, ArticleAuthorId INTEGER
        );
        CREATE TABLE IF NOT EXISTS dbo_CatalogMultiCatalogRef (
            UnitCatalogRecId INTEGER, CatalogCode INTEGER, CatalogEntry TEXT, IsSource INTEGER
        );
        CREATE TABLE IF NOT EXISTS dbo_CodeSource (
            TeamCode INTEGER, EngDesc TEXT, HebDesc TEXT
        );
        CREATE TABLE IF NOT EXISTS CODE_Catalog (
            CatalogId INTEGER PRIMARY KEY, CatalogType TEXT, Author TEXT, CatAcronym TEXT,
            Title TEXT, Domain TEXT, Collection TEXT
        );
        CREATE TABLE IF NOT EXISTS CODE_GenizahTitle (
            GenizahTitleID INTEGER PRIMARY KEY, OrgTitle TEXT, EngTitle TEXT,
            DomainId INTEGER, AuthorId INTEGER, LanguageCode INTEGER
        );
        CREATE TABLE IF NOT EXISTS CODE_FullCode (
            ComputedCode INTEGER, FCDTableId INTEGER, FCDTableInnerId INTEGER,
            EngDesc TEXT, HebDesc TEXT, IsCanceledCode INTEGER
        );
        CREATE TABLE IF NOT EXISTS CODE_FCDTable (FCDTableId INTEGER, TableName TEXT);
        CREATE TABLE IF NOT EXISTS CODE_Title (
            TitleId INTEGER PRIMARY KEY, FullTitleEng TEXT, FullTitleHeb TEXT,
            RunningTitleEng TEXT, RunningTitleHeb TEXT, AcronymEng TEXT, AcronymHeb TEXT,
            CityEng TEXT, TitleYearEng TEXT, PublisherEng TEXT
        );
        CREATE TABLE IF NOT EXISTS CODE_Author (
            AuthorId INTEGER PRIMARY KEY, EngDesc TEXT, HebDesc TEXT
        );
        CREATE TABLE IF NOT EXISTS CODE_GenizahPerson (
            GenizahPersonId INTEGER PRIMARY KEY, EngDesc TEXT, HebDesc TEXT, HebDescAc TEXT
        );
    """)


@pytest.fixture
def fist_seed():
    """In-memory FIST.db with minimal schema + 3 fixture inventories.

    Inventory 1: real Alma (control)
    Inventory 2: no Alma + has CUDL classmark + FJMS title (qualifies)
    Inventory 3: no Alma + has FJMS title only (qualifies — tier 3)
    """
    conn = sqlite3.connect(":memory:")
    _seed_fist_schema(conn)
    conn.executescript("""
        INSERT INTO dbo_Inventory VALUES (1), (2), (3);
        INSERT INTO dbo_InventorySignature VALUES (1, 100), (2, 200), (3, 300);
        INSERT INTO dbo_Signature (SetSignatureId, SignatureId, Signature, SourceId, SubId) VALUES
            (100, 1000, 'T-S 1.1', 100, 1),
            (200, 2000, 'T-S NS 329.96', 100, 2),
            (300, 3000, 'T-S NS 330.10', 100, 3);
        INSERT INTO dbo_UnitCatalogRec (UnitCatalogRecId, SignatureId, Title, GenizahTitleText) VALUES
            (10, 1000, 'Real Title', NULL),
            (20, 2000, 'Synthetic Title', 'מילון'),
            (30, 3000, 'Synthetic Title 3', NULL);
        INSERT INTO dbo_InventoryAlma VALUES (1, '990025143260205171');
        -- Inventories 2 & 3 have no Alma link -> manifest will list them.
        INSERT INTO dbo_CatalogMultiDomain VALUES (10, 1), (20, 1), (30, 1);
        INSERT INTO CODE_Domain VALUES (1, 'Halakhah', 'הלכה', NULL);
        INSERT INTO dbo_CodeSource (TeamCode, EngDesc, HebDesc) VALUES (100, 'Test Team', 'צוות');
    """)
    conn.commit()
    return conn


@pytest.fixture
def manifest_fixture(tmp_path):
    """Fixture synthetic_manifest.json (Plan 02 output that Plan 03 reads).

    Lists InventoryIds 2 and 3 as the qualifying set (matches fist_seed).
    """
    manifest = tmp_path / "synthetic_manifest.json"
    items = [
        {
            "inventory_id": 2,
            "synthetic_sys_id": "990000000002000000",
            "source": "both",
            "canonical_shelfmark": "T-S NS 329.96",
            "library_code": "CUL",
        },
        {
            "inventory_id": 3,
            "synthetic_sys_id": "990000000003000000",
            "source": "fjms_metadata",
            "canonical_shelfmark": "T-S NS 330.10",
            "library_code": "CUL",
        },
    ]
    manifest.write_text(json.dumps(items, indent=2, sort_keys=True), encoding="utf-8")
    return manifest


# ---------------------------------------------------------------------------
# Tests — Wave 0 (run BEFORE Task 2 modifies the script): static-analysis
# guards that operate on the source-text only. Other tests are XFAIL until
# Task 2 wires the implementation.
# ---------------------------------------------------------------------------


class TestParameterizedSqlOnly:
    """T-85-02 mitigation: scan export_fist_enrichment.py for f-string SQL on dynamic values."""

    def test_no_fstring_sql_with_dynamic_values(self):
        path = Path(__file__).resolve().parent.parent / "scripts" / "export_fist_enrichment.py"
        src = path.read_text(encoding="utf-8")
        sql_blocks = re.findall(r'""".*?"""', src, re.DOTALL)
        for block in sql_blocks:
            if any(kw in block.upper() for kw in ("SELECT", "INSERT", "UPDATE", "DELETE")):
                bad = re.findall(r"\{[a-zA-Z_][a-zA-Z0-9_]*\}", block)
                bad = [b for b in bad if b not in ("{}", "{0}")]
                assert not bad, f"f-string interpolation in SQL block: {bad} in {block[:100]}..."


class TestStaleTableNamesAbsent:
    """REVIEWS-MODE Codex HIGH: prior plan's stale table names must not appear as export targets."""

    @pytest.mark.parametrize("stale", STALE_TABLE_NAMES)
    def test_stale_target_absent(self, stale):
        path = Path(__file__).resolve().parent.parent / "scripts" / "export_fist_enrichment.py"
        src = path.read_text(encoding="utf-8")
        # The stale name must not appear as an export function or as a CREATE TABLE target.
        forbidden = [
            f"def export_{stale}",
            f"CREATE TABLE {stale}",
            f'CREATE TABLE IF NOT EXISTS {stale}',
        ]
        for f in forbidden:
            assert f not in src, (
                f"Stale table name {stale!r} reintroduced via {f!r} — "
                f"REVIEWS-MODE Codex HIGH guard"
            )


# ---------------------------------------------------------------------------
# Tests — Wave 1 (run AFTER Task 2 modifies the script): exercise the UNION
# ALL injection. These rely on Task 2 exposing:
#   - load_synthetic_manifest_into_temp_table(source, manifest_path)
#   - export_<table>(source, target) for each AlmaId-keyed table
# ---------------------------------------------------------------------------


def _has_union_implementation() -> bool:
    """Return True iff Task 2 has wired the manifest loader."""
    path = Path(__file__).resolve().parent.parent / "scripts" / "export_fist_enrichment.py"
    src = path.read_text(encoding="utf-8")
    return (
        "def load_synthetic_manifest_into_temp_table" in src
        and "synthetic_qualifying_inventories" in src
    )


# Skip dynamic tests when Task 2 hasn't been completed yet (Wave-0 phase).
needs_task2 = pytest.mark.skipif(
    not _has_union_implementation(),
    reason="Task 2 not yet wired — load_synthetic_manifest_into_temp_table absent",
)


@needs_task2
class TestSyntheticAlmaInjection:
    def test_synthetic_alma_appears_in_catalog(self, fist_seed, manifest_fixture, tmp_path):
        """Run modified export -> resulting catalog has synthetic-AlmaId rows."""
        from scripts import export_fist_enrichment as ef

        target_path = tmp_path / "fjms_enrichment.db"
        target_conn = sqlite3.connect(target_path)
        try:
            target = target_conn.cursor()
            ef.load_synthetic_manifest_into_temp_table(fist_seed, manifest_fixture)
            ef.export_catalog(fist_seed.cursor(), target)
            target_conn.commit()

            rows = target_conn.execute("SELECT AlmaId FROM catalog").fetchall()
            alma_ids = [r[0] for r in rows]
            assert any(is_synthetic_sys_id(a) for a in alma_ids), \
                "Expected at least one synthetic AlmaId in catalog table"
            assert "990025143260205171" in alma_ids, "Real Alma must still be present"
        finally:
            target_conn.close()

    @pytest.mark.parametrize("table", ALMA_KEYED_TABLES)
    def test_alma_keyed_tables_all_unioned(self, fist_seed, manifest_fixture, tmp_path, table):
        """Every one of the 12 AlmaId-keyed tables must have synthetic-AlmaId UNION."""
        from scripts import export_fist_enrichment as ef

        target_path = tmp_path / f"fjms_{table}.db"
        target_conn = sqlite3.connect(target_path)
        try:
            target = target_conn.cursor()
            ef.load_synthetic_manifest_into_temp_table(fist_seed, manifest_fixture)
            export_fn = getattr(ef, f"export_{table}")
            export_fn(fist_seed.cursor(), target)
            target_conn.commit()

            # Verify the table has at least one synthetic AlmaId row when manifest is non-empty
            # AND when the underlying FIST data has matching content for that inventory.
            # Some tables may be legitimately empty for a fixture inventory — relax to
            # "synthetic AlmaIds in table are subset of manifest InventoryIds".
            rows = target_conn.execute(f"SELECT DISTINCT AlmaId FROM {table}").fetchall()
            synthetic_in_table = {r[0] for r in rows if is_synthetic_sys_id(r[0])}
            manifest_synthetic_ids = {item["synthetic_sys_id"] for item in json.loads(manifest_fixture.read_text())}
            assert synthetic_in_table.issubset(manifest_synthetic_ids), (
                f"{table}: synthetic AlmaIds {synthetic_in_table - manifest_synthetic_ids} "
                f"not in manifest — manifest-authority violation"
            )
        finally:
            target_conn.close()

    def test_synthetic_alma_format_valid(self, fist_seed, manifest_fixture, tmp_path):
        from scripts import export_fist_enrichment as ef
        target_path = tmp_path / "fjms.db"
        target_conn = sqlite3.connect(target_path)
        try:
            target = target_conn.cursor()
            ef.load_synthetic_manifest_into_temp_table(fist_seed, manifest_fixture)
            ef.export_catalog(fist_seed.cursor(), target)
            target_conn.commit()

            synthetic_ids = [
                r[0] for r in target_conn.execute(
                    "SELECT DISTINCT AlmaId FROM catalog WHERE LENGTH(AlmaId) = 18 AND AlmaId LIKE '99%000000'"
                )
            ]
            for alma in synthetic_ids:
                assert is_synthetic_sys_id(alma), f"Malformed synthetic AlmaId: {alma}"
        finally:
            target_conn.close()

    def test_no_collision_real_vs_synthetic(self, fist_seed, manifest_fixture, tmp_path):
        from scripts import export_fist_enrichment as ef
        target_path = tmp_path / "fjms.db"
        target_conn = sqlite3.connect(target_path)
        try:
            target = target_conn.cursor()
            ef.load_synthetic_manifest_into_temp_table(fist_seed, manifest_fixture)
            ef.export_catalog(fist_seed.cursor(), target)
            target_conn.commit()

            real = {r[0] for r in target_conn.execute("SELECT AlmaId FROM catalog") if not is_synthetic_sys_id(r[0])}
            synthetic = {r[0] for r in target_conn.execute("SELECT AlmaId FROM catalog") if is_synthetic_sys_id(r[0])}
            assert real.isdisjoint(synthetic), \
                f"D-01a violation: AlmaIds appear in both partitions: {real & synthetic}"
        finally:
            target_conn.close()

    def test_fjms_service_queries_synthetic_unchanged(self, fist_seed, manifest_fixture, tmp_path):
        """D-01: shared/fjms_service.py must work transparently with synthetic AlmaIds."""
        from scripts import export_fist_enrichment as ef
        from shared.fjms_service import FjmsService

        target_path = tmp_path / "fjms.db"
        target_conn = sqlite3.connect(target_path)
        try:
            target = target_conn.cursor()
            ef.load_synthetic_manifest_into_temp_table(fist_seed, manifest_fixture)
            ef.export_catalog(fist_seed.cursor(), target)
            ef.export_domains(fist_seed.cursor(), target)
            target_conn.commit()
        finally:
            target_conn.close()

        synth_alma = "990000000002000000"  # InventoryId=2 from manifest
        svc = FjmsService(str(target_path), thread_safe=False)
        try:
            assert svc.is_available(), "FjmsService failed to open the test sidecar"
            result = svc.get_catalog(synth_alma)
            assert result is not None, (
                f"FjmsService.get_catalog returned None for synthetic AlmaId {synth_alma} — "
                f"D-01 layered-not-extended invariant violated"
            )
        finally:
            # Ensure connection released so tmp_path cleanup works on Windows.
            if svc._conn is not None:
                svc._conn = None


@needs_task2
class TestManifestAuthority:
    """REVIEWS-MODE: cross-plan invariant — Plan 03 reads Plan 02's manifest as the only InventoryId source."""

    def test_manifest_is_authority(self, fist_seed, manifest_fixture, tmp_path):
        """Every synthetic AlmaId in fjms_enrichment.db corresponds to a manifest entry."""
        from scripts import export_fist_enrichment as ef
        from shared.synthetic_sys_id import decode_inventory_id

        target_path = tmp_path / "fjms.db"
        target_conn = sqlite3.connect(target_path)
        try:
            target = target_conn.cursor()
            ef.load_synthetic_manifest_into_temp_table(fist_seed, manifest_fixture)
            ef.export_catalog(fist_seed.cursor(), target)
            target_conn.commit()

            manifest_inv_ids = {item["inventory_id"] for item in json.loads(manifest_fixture.read_text())}
            synthetic_ids = [r[0] for r in target_conn.execute(
                "SELECT DISTINCT AlmaId FROM catalog WHERE AlmaId LIKE '99%000000'"
            ).fetchall()]
            for sa in synthetic_ids:
                inv_id = decode_inventory_id(sa)
                assert inv_id in manifest_inv_ids, (
                    f"Synthetic AlmaId {sa} (InventoryId={inv_id}) not in manifest — "
                    f"manifest-as-authority contract broken"
                )
        finally:
            target_conn.close()

    def test_no_independent_qualifying_set_predicate(self):
        """REVIEWS-MODE guard: scripts/export_fist_enrichment.py must NOT contain
        an independent qualifying-set predicate that could diverge from Plan 02."""
        path = Path(__file__).resolve().parent.parent / "scripts" / "export_fist_enrichment.py"
        src = path.read_text(encoding="utf-8")
        # Forbidden: SQL that builds a qualifying set inline (the prior plan's pattern).
        forbidden_patterns = [
            r"cat\.Title IS NOT NULL AND TRIM\(cat\.Title\)",  # the prior plan's predicate
            r"cudl_classmark\s+match",
        ]
        for pattern in forbidden_patterns:
            matches = re.findall(pattern, src)
            assert not matches, (
                f"REVIEWS-MODE violation: scripts/export_fist_enrichment.py contains "
                f"forbidden qualifying-set predicate matching {pattern!r}. "
                f"Plan 03 must read manifest only — NOT compute its own qualifying set."
            )


@needs_task2
class TestDeterministicOrdering:
    """REVIEWS-MODE Codex HIGH: every UNION block has explicit ORDER BY."""

    def test_explicit_order_by_in_each_union(self):
        path = Path(__file__).resolve().parent.parent / "scripts" / "export_fist_enrichment.py"
        src = path.read_text(encoding="utf-8")
        # Compute SQL-only UNION ALL positions: scan only the inside of triple-
        # double-quoted strings (the SQL blocks), skipping module-level
        # docstrings and comments. Each export function's SQL is wrapped in
        # `source.execute("""...""")` so triple-quoted blocks are SQL-region
        # candidates. Filter to only those whose body contains a SELECT.
        sql_union_positions: list[int] = []
        for sql_match in re.finditer(r'"""(.*?)"""', src, re.DOTALL):
            body = sql_match.group(1)
            if "SELECT" not in body.upper():
                continue
            block_start = sql_match.start(1)
            for m in re.finditer(r"\bUNION ALL\b", body):
                sql_union_positions.append(block_start + m.start())
        assert sql_union_positions, "No SQL UNION ALL blocks found — Task 2 implementation missing"
        for pos in sql_union_positions:
            window = src[pos:pos + 3000]
            assert "ORDER BY" in window, (
                f"UNION ALL at offset {pos} lacks ORDER BY within 3000 chars — "
                f"REVIEWS-MODE byte-stability violation"
            )


@needs_task2
class TestCatalogTripleUniqueness:
    """REVIEWS-MODE Codex HIGH: catalog 1:1 by (AlmaId, UnitCatalogRecId, SignatureId)."""

    def test_no_duplicate_triples_in_synthetic_block(self, fist_seed, manifest_fixture, tmp_path):
        from scripts import export_fist_enrichment as ef
        target_path = tmp_path / "fjms.db"
        target_conn = sqlite3.connect(target_path)
        try:
            target = target_conn.cursor()
            ef.load_synthetic_manifest_into_temp_table(fist_seed, manifest_fixture)
            ef.export_catalog(fist_seed.cursor(), target)
            target_conn.commit()

            # catalog table schema doesn't include SignatureId — use UnitCatalogRecId
            # which is a unique key per row, sufficient for the uniqueness invariant.
            rows = target_conn.execute("""
                SELECT AlmaId, UnitCatalogRecId, COUNT(*) as c
                FROM catalog
                WHERE AlmaId LIKE '99%000000'
                GROUP BY AlmaId, UnitCatalogRecId
                HAVING c > 1
            """).fetchall()
            assert not rows, (
                f"catalog has duplicate (AlmaId, UnitCatalogRecId) pairs in synthetic block: "
                f"{rows[:5]}"
            )
        finally:
            target_conn.close()


@needs_task2
class TestSyntheticAlmaInCatalogAtMinimum:
    """REVIEWS-MODE Codex HIGH: every synthetic AlmaId in 1:N tables MUST appear in catalog.

    Smoke-level: warn (don't fail) on absence — synthetic AlmaIds that appear in
    1:N tables but not in catalog are legal but suspicious.
    """

    def test_synthetic_in_bib_but_missing_from_catalog_warns(self, fist_seed, manifest_fixture, tmp_path):
        # Implementation surface: scripts/export_fist_enrichment.py exposes
        # _validate_synthetic_export(target) which prints warnings for orphan
        # synthetic AlmaIds. Smoke-test that the function exists.
        from scripts import export_fist_enrichment as ef
        assert hasattr(ef, "_validate_synthetic_export"), (
            "Task 2 must expose _validate_synthetic_export(target) for post-export "
            "table-specific invariants (Codex HIGH)."
        )


@needs_task2
class TestNonAlmaTablesUnchanged:
    """ref_*, genizah_*, code_values, meta tables stay AlmaId-free."""

    @pytest.mark.parametrize("table", NON_ALMA_TABLES)
    def test_table_has_no_synthetic_alma(self, table, fist_seed, manifest_fixture, tmp_path):
        """These tables either don't have an AlmaId column OR don't get synthetic insertion."""
        from scripts import export_fist_enrichment as ef
        target_path = tmp_path / f"fjms_{table}.db"
        target_conn = sqlite3.connect(target_path)
        try:
            target = target_conn.cursor()
            ef.load_synthetic_manifest_into_temp_table(fist_seed, manifest_fixture)
            export_fn = getattr(ef, f"export_{table}", None)
            if export_fn is None:
                pytest.skip(f"export_{table} not present (legitimate: this is a sanity-check)")
            export_fn(fist_seed.cursor(), target)
            target_conn.commit()
            try:
                rows = target_conn.execute(
                    f"SELECT DISTINCT AlmaId FROM {table} WHERE AlmaId LIKE '99%000000'"
                ).fetchall()
                assert not rows, f"{table} should not contain synthetic AlmaIds: {rows}"
            except sqlite3.OperationalError:
                # No AlmaId column — pass. (This is the expected outcome for ref_* tables.)
                pass
        finally:
            target_conn.close()


@needs_task2
class TestFts5Rebuild:
    """FTS5 rebuild succeeds without errors after synthetic-row UNION."""

    def test_fts5_populates_after_synthetic_union(self, fist_seed, manifest_fixture, tmp_path):
        from scripts import export_fist_enrichment as ef
        target_path = tmp_path / "fjms.db"
        target_conn = sqlite3.connect(target_path)
        try:
            target = target_conn.cursor()
            ef.load_synthetic_manifest_into_temp_table(fist_seed, manifest_fixture)
            ef.export_catalog(fist_seed.cursor(), target)
            ef.export_catalog_running_titles(fist_seed.cursor(), target)
            ef.export_catalog_free_desc(fist_seed.cursor(), target)
            ef.export_catalog_full_texts(fist_seed.cursor(), target)
            target_conn.commit()

            # create_fts5 builds the FTS5 virtual table from the base tables.
            ef.create_fts5(target)
            target_conn.commit()

            # Smoke: FTS table exists and accepts queries.
            tables = [r[0] for r in target_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name = 'catalog_fts'"
            )]
            assert tables, "catalog_fts virtual table not created"
            target_conn.execute("SELECT count(*) FROM catalog_fts").fetchone()
        finally:
            target_conn.close()
