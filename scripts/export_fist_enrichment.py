#!/usr/bin/env python3
"""
Export FIST enrichment data to a SQLite sidecar database.

Reads from FIST_DB_BACKUP/FIST.db and produces fist_data/fjms_enrichment.db
with the following tables:
  - domains:      AlmaId -> Domain classifications (~390K rows)
  - joins:        AlmaId -> Scholar join groups (~48K rows)
  - catalog:      AlmaId -> Catalog metadata (~243K rows)
  - catalog_fts:  FTS5 virtual table for full-text search on catalog
  - bibliography: AlmaId -> Denormalized bibliography references (~733K rows)
  - catalog_refs: AlmaId -> Catalog cross-references (~78K rows)
  - ref_catalogs: CODE_Catalog reference lookup (80 rows)
  - ref_titles:   CODE_Title reference lookup (~4.3K rows)
  - ref_authors:  CODE_Author reference lookup (~3K rows)
  - meta:         Version and build metadata

This is the data foundation for FJMS Integration (v5.8.0) and
Metadata Enrichment (v5.9.0).
"""

import sqlite3
import os
from datetime import datetime, timezone
from pathlib import Path

from tqdm import tqdm

VERSION = "2.0.0"
BATCH_SIZE = 10_000


def clean_copy_date(value):
    """Convert CopyDate from REAL (e.g. 1744.0) to clean TEXT string."""
    if value is None:
        return None
    s = str(value)
    if s.endswith(".0"):
        s = s[:-2]
    return s


def export_domains(source, target):
    """Export domain classifications from FIST to sidecar."""
    print("Exporting domains...")

    target.execute("DROP TABLE IF EXISTS domains")
    target.execute("""
        CREATE TABLE domains (
            AlmaId TEXT NOT NULL,
            Domain TEXT NOT NULL,
            DomainHeb TEXT,
            ParentDomain TEXT,
            ParentDomainHeb TEXT
        )
    """)

    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            d.EngDesc as Domain,
            d.HebDesc as DomainHeb,
            pd.EngDesc as ParentDomain,
            pd.HebDesc as ParentDomainHeb
        FROM dbo_InventoryAlma alma
        JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
        JOIN dbo_CatalogMultiDomain cmd ON cat.UnitCatalogRecId = cmd.UnitCatalogRecId
        JOIN CODE_Domain d ON cmd.DomainId = d.DomainId
        LEFT JOIN CODE_Domain pd ON d.BelongToDomainId = pd.DomainId
    """)

    batch = []
    total = 0
    for row in tqdm(cursor, desc="  domains", unit=" rows"):
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            target.executemany(
                "INSERT INTO domains VALUES (?, ?, ?, ?, ?)", batch
            )
            total += len(batch)
            batch = []

    if batch:
        target.executemany(
            "INSERT INTO domains VALUES (?, ?, ?, ?, ?)", batch
        )
        total += len(batch)

    target.execute("CREATE INDEX idx_domains_alma ON domains(AlmaId)")
    target.execute("CREATE INDEX idx_domains_domain ON domains(Domain)")
    target.connection.commit()

    distinct_alma = target.execute(
        "SELECT COUNT(DISTINCT AlmaId) FROM domains"
    ).fetchone()[0]
    print(f"  Exported {total:,} rows ({distinct_alma:,} distinct AlmaIds)")
    return total


def export_joins(source, target):
    """Export scholar join groups from FIST to sidecar."""
    print("Exporting joins...")

    target.execute("DROP TABLE IF EXISTS joins")
    target.execute("""
        CREATE TABLE joins (
            AlmaId TEXT NOT NULL,
            JoinGroupId INTEGER NOT NULL,
            ScholarName TEXT,
            Comment TEXT,
            JoinType TEXT
        )
    """)

    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            uj.UnitJoinId as JoinGroupId,
            uj.ScholarName,
            uj.Comment,
            fc.EngDesc as JoinType
        FROM dbo_InventoryAlma alma
        JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        JOIN dbo_UnitJoin uj ON sig.SignatureId = uj.SignatureId
        LEFT JOIN CODE_FullCode fc ON uj.JoinTypeCode = fc.ComputedCode
    """)

    batch = []
    total = 0
    for row in tqdm(cursor, desc="  joins", unit=" rows"):
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            target.executemany(
                "INSERT INTO joins VALUES (?, ?, ?, ?, ?)", batch
            )
            total += len(batch)
            batch = []

    if batch:
        target.executemany(
            "INSERT INTO joins VALUES (?, ?, ?, ?, ?)", batch
        )
        total += len(batch)

    target.execute("CREATE INDEX idx_joins_alma ON joins(AlmaId)")
    target.execute("CREATE INDEX idx_joins_group ON joins(JoinGroupId)")
    target.connection.commit()

    distinct_alma = target.execute(
        "SELECT COUNT(DISTINCT AlmaId) FROM joins"
    ).fetchone()[0]
    print(f"  Exported {total:,} rows ({distinct_alma:,} distinct AlmaIds)")
    return total


def export_catalog(source, target):
    """Export catalog metadata from FIST to sidecar."""
    print("Exporting catalog...")

    target.execute("DROP TABLE IF EXISTS catalog")
    target.execute("""
        CREATE TABLE catalog (
            AlmaId TEXT NOT NULL,
            Title TEXT,
            TitleHeb TEXT,
            AuthorText TEXT,
            CopyDate TEXT,
            CopyPlace TEXT,
            DescriptionEng TEXT,
            DescriptionHeb TEXT,
            TextualFrameHeb TEXT,
            TextualFrameEng TEXT,
            SourceName TEXT,
            SourceNameHeb TEXT
        )
    """)

    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            cat.Title,
            cat.GenizahTitleText as TitleHeb,
            cat.AuthorText,
            cat.CopyDate,
            cat.CopyPlace,
            cat.IdentificationTextEng as DescriptionEng,
            cat.IdentificationTextHeb as DescriptionHeb,
            cat.BI_TextualFrameHeb as TextualFrameHeb,
            cat.BI_TextualFrameEng as TextualFrameEng,
            cs.EngDesc as SourceName,
            cs.HebDesc as SourceNameHeb
        FROM dbo_InventoryAlma alma
        JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
        LEFT JOIN dbo_CodeSource cs ON sig.SourceId = cs.TeamCode
    """)

    batch = []
    total = 0
    for row in tqdm(cursor, desc="  catalog", unit=" rows"):
        # Clean CopyDate (index 4) from REAL to TEXT
        cleaned = (
            row[0], row[1], row[2], row[3],
            clean_copy_date(row[4]),
            row[5], row[6], row[7], row[8], row[9],
            row[10], row[11]  # SourceName, SourceNameHeb
        )
        batch.append(cleaned)
        if len(batch) >= BATCH_SIZE:
            target.executemany(
                "INSERT INTO catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch,
            )
            total += len(batch)
            batch = []

    if batch:
        target.executemany(
            "INSERT INTO catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
        total += len(batch)

    target.execute("CREATE INDEX idx_catalog_alma ON catalog(AlmaId)")
    target.connection.commit()

    distinct_alma = target.execute(
        "SELECT COUNT(DISTINCT AlmaId) FROM catalog"
    ).fetchone()[0]
    print(f"  Exported {total:,} rows ({distinct_alma:,} distinct AlmaIds)")
    return total


def export_bibliography(source, target):
    """Export bibliography references from FIST to sidecar (denormalized)."""
    print("Exporting bibliography...")

    target.execute("DROP TABLE IF EXISTS bibliography")
    target.execute("""
        CREATE TABLE bibliography (
            AlmaId TEXT NOT NULL,
            RunningTitle TEXT,
            TitleYear TEXT,
            TitleAcronym TEXT,
            MentionPage TEXT,
            FromPage TEXT,
            ToPage TEXT,
            Volume TEXT,
            MentionType TEXT,
            TranscriptionType TEXT,
            TranslationType TEXT,
            ArticleName TEXT,
            ArticleAuthorEng TEXT,
            ArticleAuthorHeb TEXT,
            CatalogAcronym TEXT
        )
    """)

    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            t.RunningTitleEng, t.TitleYearEng, t.AcronymEng,
            bib.MentionPage, bib.FromPage, bib.ToPage, bib.Volume,
            fc.EngDesc as MentionType,
            ft.EngDesc as TranscriptionType,
            fl.EngDesc as TranslationType,
            bib.ArticleName,
            a.EngDesc as ArticleAuthorEng, a.HebDesc as ArticleAuthorHeb,
            cat.CatAcronym
        FROM dbo_InventoryAlma alma
        JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        JOIN dbo_UnitBibliographyReference bib ON sig.SignatureId = bib.SignatureId
        LEFT JOIN CODE_Title t ON bib.TitleId = t.TitleId
        LEFT JOIN CODE_FullCode fc ON ABS(bib.MentionTypeCode) = fc.ComputedCode
        LEFT JOIN CODE_FullCode ft ON bib.IsHasTranscriptionCode = ft.ComputedCode
        LEFT JOIN CODE_FullCode fl ON bib.IsHasTranslationCode = fl.ComputedCode
        LEFT JOIN dbo_BibMultiArticleAuthor baa
            ON bib.UnitBibliographyReferenceId = baa.UnitBibliographyReferenceId
            AND baa.AuthorOrder = 1
        LEFT JOIN CODE_Author a ON baa.ArticleAuthorId = a.AuthorId
        LEFT JOIN CODE_Catalog cat ON bib.CatalogId = cat.CatalogId
    """)

    batch = []
    total = 0
    for row in tqdm(cursor, desc="  bibliography", unit=" rows"):
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            target.executemany(
                "INSERT INTO bibliography VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch,
            )
            total += len(batch)
            batch = []

    if batch:
        target.executemany(
            "INSERT INTO bibliography VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
        total += len(batch)

    target.execute("CREATE INDEX idx_bib_alma ON bibliography(AlmaId)")
    target.connection.commit()

    distinct_alma = target.execute(
        "SELECT COUNT(DISTINCT AlmaId) FROM bibliography"
    ).fetchone()[0]
    print(f"  Exported {total:,} rows ({distinct_alma:,} distinct AlmaIds)")
    return total


def export_catalog_refs(source, target):
    """Export catalog cross-references from FIST to sidecar."""
    print("Exporting catalog cross-references...")

    target.execute("DROP TABLE IF EXISTS catalog_refs")
    target.execute("""
        CREATE TABLE catalog_refs (
            AlmaId TEXT NOT NULL,
            CatAcronym TEXT,
            CatalogAuthor TEXT,
            CatalogTitle TEXT,
            CatalogEntry TEXT,
            IsSource INTEGER
        )
    """)

    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            cat.CatAcronym, cat.Author, cat.Title,
            ccr.CatalogEntry, ccr.IsSource
        FROM dbo_InventoryAlma alma
        JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        JOIN dbo_UnitCatalogRec ucr ON sig.SignatureId = ucr.SignatureId
        JOIN dbo_CatalogMultiCatalogRef ccr ON ucr.UnitCatalogRecId = ccr.UnitCatalogRecId
        JOIN CODE_Catalog cat ON ccr.CatalogCode = cat.CatalogId
    """)

    batch = []
    total = 0
    for row in tqdm(cursor, desc="  catalog_refs", unit=" rows"):
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            target.executemany(
                "INSERT INTO catalog_refs VALUES (?, ?, ?, ?, ?, ?)", batch
            )
            total += len(batch)
            batch = []

    if batch:
        target.executemany(
            "INSERT INTO catalog_refs VALUES (?, ?, ?, ?, ?, ?)", batch
        )
        total += len(batch)

    target.execute("CREATE INDEX idx_catrefs_alma ON catalog_refs(AlmaId)")
    target.connection.commit()

    distinct_alma = target.execute(
        "SELECT COUNT(DISTINCT AlmaId) FROM catalog_refs"
    ).fetchone()[0]
    print(f"  Exported {total:,} rows ({distinct_alma:,} distinct AlmaIds)")
    return total


def export_ref_catalogs(source, target):
    """Export CODE_Catalog reference table to sidecar."""
    print("Exporting ref_catalogs...")

    target.execute("DROP TABLE IF EXISTS ref_catalogs")
    target.execute("""
        CREATE TABLE ref_catalogs (
            CatalogId INTEGER PRIMARY KEY,
            CatalogType TEXT,
            Author TEXT,
            CatAcronym TEXT,
            Title TEXT,
            Domain TEXT,
            Collection TEXT
        )
    """)

    cursor = source.execute(
        "SELECT CatalogId, CatalogType, Author, CatAcronym, Title, "
        "Domain, Collection FROM CODE_Catalog"
    )
    rows = cursor.fetchall()
    target.executemany(
        "INSERT INTO ref_catalogs VALUES (?, ?, ?, ?, ?, ?, ?)", rows
    )
    target.connection.commit()

    total = len(rows)
    print(f"  Exported {total:,} rows")
    return total


def export_ref_titles(source, target):
    """Export CODE_Title reference table to sidecar."""
    print("Exporting ref_titles...")

    target.execute("DROP TABLE IF EXISTS ref_titles")
    target.execute("""
        CREATE TABLE ref_titles (
            TitleId INTEGER PRIMARY KEY,
            FullTitleEng TEXT,
            FullTitleHeb TEXT,
            RunningTitleEng TEXT,
            AcronymEng TEXT,
            City TEXT,
            Year TEXT,
            Publisher TEXT
        )
    """)

    cursor = source.execute(
        "SELECT TitleId, FullTitleEng, FullTitleHeb, RunningTitleEng, "
        "AcronymEng, CityEng, TitleYearEng, PublisherEng FROM CODE_Title"
    )
    rows = cursor.fetchall()
    target.executemany(
        "INSERT INTO ref_titles VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows
    )
    target.connection.commit()

    total = len(rows)
    print(f"  Exported {total:,} rows")
    return total


def export_ref_authors(source, target):
    """Export CODE_Author reference table to sidecar."""
    print("Exporting ref_authors...")

    target.execute("DROP TABLE IF EXISTS ref_authors")
    target.execute("""
        CREATE TABLE ref_authors (
            AuthorId INTEGER PRIMARY KEY,
            EngDesc TEXT,
            HebDesc TEXT
        )
    """)

    cursor = source.execute(
        "SELECT AuthorId, EngDesc, HebDesc FROM CODE_Author"
    )
    rows = cursor.fetchall()
    target.executemany(
        "INSERT INTO ref_authors VALUES (?, ?, ?)", rows
    )
    target.connection.commit()

    total = len(rows)
    print(f"  Exported {total:,} rows")
    return total


def create_fts5(target):
    """Create FTS5 virtual table for full-text search on catalog."""
    print("Creating FTS5 index...")

    target.execute("DROP TABLE IF EXISTS catalog_fts")
    target.execute("""
        CREATE VIRTUAL TABLE catalog_fts USING fts5(
            AlmaId,
            Title,
            TitleHeb,
            DescriptionEng,
            DescriptionHeb,
            TextualFrameHeb,
            TextualFrameEng,
            content='catalog',
            content_rowid='rowid'
        )
    """)

    target.execute("""
        INSERT INTO catalog_fts(rowid, AlmaId, Title, TitleHeb,
            DescriptionEng, DescriptionHeb, TextualFrameHeb, TextualFrameEng)
        SELECT rowid, AlmaId, Title, TitleHeb,
            DescriptionEng, DescriptionHeb, TextualFrameHeb, TextualFrameEng
        FROM catalog
    """)
    target.connection.commit()

    fts_count = target.execute(
        "SELECT COUNT(*) FROM catalog_fts"
    ).fetchone()[0]
    print(f"  FTS5 index created with {fts_count:,} entries")


def create_meta(target):
    """Create meta table with version and build info."""
    target.execute("DROP TABLE IF EXISTS meta")
    target.execute("""
        CREATE TABLE meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    now = datetime.now(timezone.utc).isoformat()
    target.executemany(
        "INSERT INTO meta (key, value) VALUES (?, ?)",
        [
            ("version", VERSION),
            ("created", now),
            ("source", "FIST.db"),
        ],
    )
    target.connection.commit()
    print(f"  Meta table created (version {VERSION})")


def main():
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent

    source_path = project_dir / "FIST_DB_BACKUP" / "FIST.db"
    target_dir = project_dir / "fist_data"
    target_path = target_dir / "fjms_enrichment.db"

    # Validate source exists
    if not source_path.exists():
        print(f"ERROR: Source database not found: {source_path}")
        return

    # Delete stale 0-byte sidecar copy if it exists
    stale_path = project_dir / "nli_data" / "fjms_enrichment.db"
    if stale_path.exists() and stale_path.stat().st_size == 0:
        print(f"Deleting stale {stale_path}...")
        os.remove(stale_path)

    # Create output directory if needed
    target_dir.mkdir(exist_ok=True)

    # Delete existing target for idempotent re-runs (skip if locked)
    if target_path.exists():
        try:
            print(f"Removing existing {target_path.name}...")
            os.remove(target_path)
        except PermissionError:
            print(f"  {target_path.name} is locked; will overwrite tables in-place")

    print(f"Source: {source_path}")
    print(f"Target: {target_path}")
    print()

    # Connect to source (read-only)
    source_conn = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    source = source_conn.cursor()

    # Create target database
    target_conn = sqlite3.connect(str(target_path))
    target = target_conn.cursor()
    target.execute("PRAGMA journal_mode=WAL")
    target.execute("PRAGMA synchronous=NORMAL")

    try:
        domain_count = export_domains(source, target)
        join_count = export_joins(source, target)
        catalog_count = export_catalog(source, target)
        bib_count = export_bibliography(source, target)
        catref_count = export_catalog_refs(source, target)
        refcat_count = export_ref_catalogs(source, target)
        reftitle_count = export_ref_titles(source, target)
        refauthor_count = export_ref_authors(source, target)
        create_fts5(target)
        create_meta(target)

        # Compact the database
        print("\nCompacting database...")
        target.execute("PRAGMA journal_mode=DELETE")
        target_conn.commit()
        target.execute("VACUUM")
        target_conn.commit()

        # Summary
        file_size_mb = target_path.stat().st_size / (1024 * 1024)
        print(f"\nExport complete!")
        print(f"  domains:      {domain_count:>10,} rows")
        print(f"  joins:        {join_count:>10,} rows")
        print(f"  catalog:      {catalog_count:>10,} rows")
        print(f"  bibliography: {bib_count:>10,} rows")
        print(f"  catalog_refs: {catref_count:>10,} rows")
        print(f"  ref_catalogs: {refcat_count:>10,} rows")
        print(f"  ref_titles:   {reftitle_count:>10,} rows")
        print(f"  ref_authors:  {refauthor_count:>10,} rows")
        print(f"  File size: {file_size_mb:.1f} MB")

    finally:
        source_conn.close()
        target_conn.close()


if __name__ == "__main__":
    main()
