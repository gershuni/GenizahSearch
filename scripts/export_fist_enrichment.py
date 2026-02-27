#!/usr/bin/env python3
"""
Export FIST enrichment data to a SQLite sidecar database.

Reads from FIST_DB_BACKUP/FIST.db and produces fist_data/fjms_enrichment.db
with the following tables:
  - domains:                 AlmaId -> Domain classifications (~390K rows)
  - joins:                   AlmaId -> Scholar join groups (~48K rows)
  - catalog:                 AlmaId -> Catalog metadata v2 (~500K rows)
  - catalog_running_titles:  AlmaId -> Running titles per team (~235K rows)
  - catalog_sizes:           AlmaId -> Physical sizes per record (~161K rows)
  - catalog_fields:          AlmaId -> Coded multi-fields (~1.1M rows)
  - catalog_free_desc:       AlmaId -> Scholarly free descriptions (~190K rows)
  - catalog_full_texts:      AlmaId -> Scholarly prose descriptions (~65K rows)
  - catalog_textual_frames:  AlmaId -> Detailed content identifications (~199K rows)
  - catalog_mentions:        AlmaId -> Named entity mentions (~24K rows)
  - catalog_fts:             FTS5 virtual table (catalog + running titles + free desc + full texts)
  - bibliography:            AlmaId -> Denormalized bibliography references (~733K rows)
  - catalog_refs:            AlmaId -> Catalog cross-references (~78K rows)
  - ref_catalogs:            CODE_Catalog reference lookup (80 rows)
  - ref_titles:              CODE_Title reference lookup (~4.3K rows)
  - ref_authors:             CODE_Author reference lookup (~3K rows)
  - genizah_persons:         CODE_GenizahPerson historical people (~2.3K rows)
  - genizah_titles:          CODE_GenizahTitle work/title lookup (~775 rows)
  - code_values:             CODE_FullCode decoded field values (~3K rows)
  - meta:                    Version and build metadata

This is the data foundation for FJMS Integration (v5.8.0),
Metadata Enrichment (v5.9.0), and Catalog Descriptions (Phase 37).
"""

import sqlite3
import os
from datetime import datetime, timezone
from pathlib import Path

from tqdm import tqdm

VERSION = "5.0.0"
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
    """Export catalog metadata from FIST to sidecar (v2 schema).

    Latest-version filter: only exports the most recent version of each
    signature (per SetSignatureId), avoiding duplicate team entries.

    Catalog name resolution: for SourceId=500 (Catalogs), resolves SubId
    to CODE_Catalog.CatAcronym for specific catalog names (e.g. 'Danzig Catalog')
    instead of the generic 'Catalogs' label.
    """
    print("Exporting catalog...")

    target.execute("DROP TABLE IF EXISTS catalog")
    target.execute("""
        CREATE TABLE catalog (
            AlmaId TEXT NOT NULL,
            UnitCatalogRecId INTEGER NOT NULL,
            Title TEXT,
            TitleHeb TEXT,
            AuthorText TEXT,
            CopyDate TEXT,
            CopyPlace TEXT,
            TextualFrameHeb TEXT,
            TextualFrameEng TEXT,
            SourceName TEXT,
            SourceNameHeb TEXT,
            NumFolio REAL,
            NumBifolio REAL,
            NumColumn TEXT,
            NumRow TEXT,
            GenizahTitleOrgTitle TEXT,
            GenizahTitleEngTitle TEXT,
            GenizahTitleId INTEGER,
            Author INTEGER,
            CopyToDate TEXT,
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
            NumEmpty REAL
        )
    """)

    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            cat.UnitCatalogRecId,
            cat.Title,
            cat.GenizahTitleText as TitleHeb,
            cat.AuthorText,
            cat.CopyDate,
            cat.CopyPlace,
            cat.BI_TextualFrameHeb as TextualFrameHeb,
            cat.BI_TextualFrameEng as TextualFrameEng,
            CASE
                WHEN sig.SourceId = 500
                    THEN COALESCE(catname.CatAcronym || ' Catalog', cs.EngDesc)
                ELSE cs.EngDesc
            END as SourceName,
            CASE
                WHEN sig.SourceId = 500
                    THEN COALESCE(catname.CatAcronym || ' Catalog', cs.HebDesc)
                ELSE cs.HebDesc
            END as SourceNameHeb,
            cat.NumFolio,
            cat.NumBifolio,
            cat.NumColumn,
            cat.NumRow,
            gt.OrgTitle as GenizahTitleOrgTitle,
            gt.EngTitle as GenizahTitleEngTitle,
            cat.GenizahTitleId,
            cat.Author,
            cat.CopyToDate,
            cat.CreationTypeCode,
            cat.PartVocalCode,
            cat.CantillationCode,
            cat.SizeUnitCode,
            cat.Autograph,
            cat.Massorah,
            cat.PalimpsestCode,
            cat.IllustrationCode,
            cat.Comment,
            cat.Colophon,
            cat.ColophonFolio,
            cat.CopyName,
            cat.ShelfmarkRange,
            cat.OrgCreation,
            cat.OrgAuthor,
            cat.GenizahCode,
            cat.NumEmpty
        FROM dbo_InventoryAlma alma
        JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        JOIN (
            SELECT SetSignatureId, MAX(Version) as MaxVersion
            FROM dbo_Signature GROUP BY SetSignatureId
        ) lsv ON sig.SetSignatureId = lsv.SetSignatureId
            AND sig.Version = lsv.MaxVersion
        JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
        LEFT JOIN dbo_CodeSource cs ON sig.SourceId = cs.TeamCode
        LEFT JOIN CODE_Catalog catname
            ON sig.SourceId = 500 AND sig.SubId = catname.CatalogId
        LEFT JOIN CODE_GenizahTitle gt ON cat.GenizahTitleId = gt.GenizahTitleID
    """)

    _placeholders = ", ".join(["?"] * 37)
    _insert_sql = f"INSERT INTO catalog VALUES ({_placeholders})"

    batch = []
    total = 0
    for row in tqdm(cursor, desc="  catalog", unit=" rows"):
        # Clean CopyDate (index 5) and CopyToDate (index 19) from REAL to TEXT
        cleaned = (
            row[0],   # AlmaId
            row[1],   # UnitCatalogRecId
            row[2],   # Title
            row[3],   # TitleHeb
            row[4],   # AuthorText
            clean_copy_date(row[5]),   # CopyDate
            row[6],   # CopyPlace
            row[7],   # TextualFrameHeb
            row[8],   # TextualFrameEng
            row[9],   # SourceName
            row[10],  # SourceNameHeb
            row[11],  # NumFolio
            row[12],  # NumBifolio
            row[13],  # NumColumn
            row[14],  # NumRow
            row[15],  # GenizahTitleOrgTitle
            row[16],  # GenizahTitleEngTitle
            row[17],  # GenizahTitleId
            row[18],  # Author (FK)
            clean_copy_date(row[19]),  # CopyToDate
            row[20],  # CreationTypeCode
            row[21],  # PartVocalCode
            row[22],  # CantillationCode
            row[23],  # SizeUnitCode
            row[24],  # Autograph
            row[25],  # Massorah
            row[26],  # PalimpsestCode
            row[27],  # IllustrationCode
            row[28],  # Comment
            row[29],  # Colophon
            row[30],  # ColophonFolio
            row[31],  # CopyName
            row[32],  # ShelfmarkRange
            row[33],  # OrgCreation
            row[34],  # OrgAuthor
            row[35],  # GenizahCode
            row[36],  # NumEmpty
        )
        batch.append(cleaned)
        if len(batch) >= BATCH_SIZE:
            target.executemany(_insert_sql, batch)
            total += len(batch)
            batch = []

    if batch:
        target.executemany(_insert_sql, batch)
        total += len(batch)

    target.execute("CREATE INDEX idx_catalog_alma ON catalog(AlmaId)")
    target.execute("CREATE INDEX idx_catalog_ucrid ON catalog(UnitCatalogRecId)")
    target.execute("CREATE INDEX idx_catalog_genizah_title ON catalog(GenizahTitleId)")
    target.execute("CREATE INDEX idx_catalog_author_fk ON catalog(Author)")
    target.connection.commit()

    distinct_alma = target.execute(
        "SELECT COUNT(DISTINCT AlmaId) FROM catalog"
    ).fetchone()[0]
    print(f"  Exported {total:,} rows ({distinct_alma:,} distinct AlmaIds)")
    return total


def export_catalog_running_titles(source, target):
    """Export catalog running titles from FIST to sidecar."""
    print("Exporting catalog running titles...")

    target.execute("DROP TABLE IF EXISTS catalog_running_titles")
    target.execute("""
        CREATE TABLE catalog_running_titles (
            AlmaId TEXT NOT NULL,
            UnitCatalogRecId INTEGER NOT NULL,
            RunningTitle TEXT,
            Comment TEXT
        )
    """)

    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            cat.UnitCatalogRecId,
            rt.RunningTitle,
            rt.Comment
        FROM dbo_InventoryAlma alma
        JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        JOIN (
            SELECT SetSignatureId, MAX(Version) as MaxVersion
            FROM dbo_Signature GROUP BY SetSignatureId
        ) lsv ON sig.SetSignatureId = lsv.SetSignatureId
            AND sig.Version = lsv.MaxVersion
        JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
        JOIN dbo_CatalogMultiRunningTitle rt ON cat.UnitCatalogRecId = rt.UnitCatalogRecId
    """)

    batch = []
    total = 0
    for row in tqdm(cursor, desc="  running_titles", unit=" rows"):
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            target.executemany(
                "INSERT INTO catalog_running_titles VALUES (?, ?, ?, ?)", batch
            )
            total += len(batch)
            batch = []

    if batch:
        target.executemany(
            "INSERT INTO catalog_running_titles VALUES (?, ?, ?, ?)", batch
        )
        total += len(batch)

    target.execute("CREATE INDEX idx_catrt_alma ON catalog_running_titles(AlmaId)")
    target.execute("CREATE INDEX idx_catrt_ucrid ON catalog_running_titles(UnitCatalogRecId)")
    target.connection.commit()

    distinct_alma = target.execute(
        "SELECT COUNT(DISTINCT AlmaId) FROM catalog_running_titles"
    ).fetchone()[0]
    print(f"  Exported {total:,} rows ({distinct_alma:,} distinct AlmaIds)")
    return total


def export_catalog_sizes(source, target):
    """Export catalog sizes from FIST to sidecar."""
    print("Exporting catalog sizes...")

    target.execute("DROP TABLE IF EXISTS catalog_sizes")
    target.execute("""
        CREATE TABLE catalog_sizes (
            AlmaId TEXT NOT NULL,
            UnitCatalogRecId INTEGER NOT NULL,
            SizeX REAL,
            SizeY REAL,
            InnerSizeX REAL,
            InnerSizeY REAL
        )
    """)

    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            cat.UnitCatalogRecId,
            sz.SizeX,
            sz.SizeY,
            sz.InnerSizeX,
            sz.InnerSizeY
        FROM dbo_InventoryAlma alma
        JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        JOIN (
            SELECT SetSignatureId, MAX(Version) as MaxVersion
            FROM dbo_Signature GROUP BY SetSignatureId
        ) lsv ON sig.SetSignatureId = lsv.SetSignatureId
            AND sig.Version = lsv.MaxVersion
        JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
        JOIN dbo_CatalogMultiSize sz ON cat.UnitCatalogRecId = sz.UnitCatalogRecId
    """)

    batch = []
    total = 0
    for row in tqdm(cursor, desc="  sizes", unit=" rows"):
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            target.executemany(
                "INSERT INTO catalog_sizes VALUES (?, ?, ?, ?, ?, ?)", batch
            )
            total += len(batch)
            batch = []

    if batch:
        target.executemany(
            "INSERT INTO catalog_sizes VALUES (?, ?, ?, ?, ?, ?)", batch
        )
        total += len(batch)

    target.execute("CREATE INDEX idx_catsz_alma ON catalog_sizes(AlmaId)")
    target.connection.commit()

    distinct_alma = target.execute(
        "SELECT COUNT(DISTINCT AlmaId) FROM catalog_sizes"
    ).fetchone()[0]
    print(f"  Exported {total:,} rows ({distinct_alma:,} distinct AlmaIds)")
    return total


def export_catalog_fields(source, target):
    """Export catalog multi-field values from FIST to sidecar."""
    print("Exporting catalog fields...")

    target.execute("DROP TABLE IF EXISTS catalog_fields")
    target.execute("""
        CREATE TABLE catalog_fields (
            AlmaId TEXT NOT NULL,
            UnitCatalogRecId INTEGER NOT NULL,
            FieldCategory TEXT NOT NULL,
            FieldValue TEXT,
            FieldValueHeb TEXT
        )
    """)

    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            cat.UnitCatalogRecId,
            fct.TableName as FieldCategory,
            fc.EngDesc as FieldValue,
            fc.HebDesc as FieldValueHeb
        FROM dbo_InventoryAlma alma
        JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        JOIN (
            SELECT SetSignatureId, MAX(Version) as MaxVersion
            FROM dbo_Signature GROUP BY SetSignatureId
        ) lsv ON sig.SetSignatureId = lsv.SetSignatureId
            AND sig.Version = lsv.MaxVersion
        JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
        JOIN dbo_CatalogMultiField fld ON cat.UnitCatalogRecId = fld.UnitCatalogRecId
        JOIN CODE_FullCode fc ON fld.ValueCode = fc.ComputedCode
        JOIN CODE_FCDTable fct ON fc.FCDTableId = fct.FCDTableId
    """)

    batch = []
    total = 0
    for row in tqdm(cursor, desc="  fields", unit=" rows"):
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            target.executemany(
                "INSERT INTO catalog_fields VALUES (?, ?, ?, ?, ?)", batch
            )
            total += len(batch)
            batch = []

    if batch:
        target.executemany(
            "INSERT INTO catalog_fields VALUES (?, ?, ?, ?, ?)", batch
        )
        total += len(batch)

    target.execute("CREATE INDEX idx_catfld_alma ON catalog_fields(AlmaId)")
    target.execute("CREATE INDEX idx_catfld_cat ON catalog_fields(FieldCategory)")
    target.connection.commit()

    distinct_alma = target.execute(
        "SELECT COUNT(DISTINCT AlmaId) FROM catalog_fields"
    ).fetchone()[0]
    print(f"  Exported {total:,} rows ({distinct_alma:,} distinct AlmaIds)")
    return total


def export_catalog_free_desc(source, target):
    """Export catalog free descriptions from FIST to sidecar."""
    print("Exporting catalog free descriptions...")

    target.execute("DROP TABLE IF EXISTS catalog_free_desc")
    target.execute("""
        CREATE TABLE catalog_free_desc (
            AlmaId TEXT NOT NULL,
            SignatureId INTEGER NOT NULL,
            FreeDesc TEXT,
            SourceName TEXT,
            SourceNameHeb TEXT
        )
    """)

    # NOTE: No latest-version filter here. Free descriptions are linked to
    # different signatures than catalog records within the same set. E.g. for
    # ENA 2943.21, the catalog rec is on V3 (SigId 38059814) but the free desc
    # is on V2 (SigId 37858814). Filtering to latest version would lose them.
    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            sig.SignatureId,
            fd.FreeDesc,
            CASE
                WHEN sig.SourceId = 500
                    THEN COALESCE(catname.CatAcronym || ' Catalog', cs.EngDesc)
                ELSE cs.EngDesc
            END as SourceName,
            CASE
                WHEN sig.SourceId = 500
                    THEN COALESCE(catname.CatAcronym || ' Catalog', cs.HebDesc)
                ELSE cs.HebDesc
            END as SourceNameHeb
        FROM dbo_InventoryAlma alma
        JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        JOIN dbo_UnitFreeDescription fd ON sig.SignatureId = fd.SignatureId
        LEFT JOIN dbo_CodeSource cs ON sig.SourceId = cs.TeamCode
        LEFT JOIN CODE_Catalog catname
            ON sig.SourceId = 500 AND sig.SubId = catname.CatalogId
    """)

    batch = []
    total = 0
    for row in tqdm(cursor, desc="  free_desc", unit=" rows"):
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            target.executemany(
                "INSERT INTO catalog_free_desc VALUES (?, ?, ?, ?, ?)", batch
            )
            total += len(batch)
            batch = []

    if batch:
        target.executemany(
            "INSERT INTO catalog_free_desc VALUES (?, ?, ?, ?, ?)", batch
        )
        total += len(batch)

    target.execute("CREATE INDEX idx_catfd_alma ON catalog_free_desc(AlmaId)")
    target.connection.commit()

    distinct_alma = target.execute(
        "SELECT COUNT(DISTINCT AlmaId) FROM catalog_free_desc"
    ).fetchone()[0]
    print(f"  Exported {total:,} rows ({distinct_alma:,} distinct AlmaIds)")
    return total


def export_catalog_full_texts(source, target):
    """Export catalog full texts from FIST to sidecar.

    Full texts are scholarly prose descriptions (up to 21K chars) stored in
    dbo_UnitFullText. All records have SourceId=500 (Catalogs). Linked via
    SignatureId (same pattern as catalog_free_desc — no version filter).
    """
    print("Exporting catalog full texts...")

    target.execute("DROP TABLE IF EXISTS catalog_full_texts")
    target.execute("""
        CREATE TABLE catalog_full_texts (
            AlmaId TEXT NOT NULL,
            SignatureId INTEGER NOT NULL,
            FullText TEXT
        )
    """)

    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            sig.SignatureId,
            ft.FullText
        FROM dbo_InventoryAlma alma
        JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        JOIN dbo_UnitFullText ft ON sig.SignatureId = ft.SignatureId
        WHERE ft.FullText IS NOT NULL AND TRIM(ft.FullText) != ''
    """)

    batch = []
    total = 0
    for row in tqdm(cursor, desc="  full_texts", unit=" rows"):
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            target.executemany(
                "INSERT INTO catalog_full_texts VALUES (?, ?, ?)", batch
            )
            total += len(batch)
            batch = []

    if batch:
        target.executemany(
            "INSERT INTO catalog_full_texts VALUES (?, ?, ?)", batch
        )
        total += len(batch)

    target.execute("CREATE INDEX idx_catft_alma ON catalog_full_texts(AlmaId)")
    target.connection.commit()

    distinct_alma = target.execute(
        "SELECT COUNT(DISTINCT AlmaId) FROM catalog_full_texts"
    ).fetchone()[0]
    print(f"  Exported {total:,} rows ({distinct_alma:,} distinct AlmaIds)")
    return total


def export_catalog_textual_frames(source, target):
    """Export detailed textual frames from FIST to sidecar.

    Richer than the BI_TextualFrame summary in the catalog table — includes
    individual per-verse biblical/rabbinic references with author names.
    Uses latest-version filter (same as catalog_running_titles).
    """
    print("Exporting catalog textual frames...")

    target.execute("DROP TABLE IF EXISTS catalog_textual_frames")
    target.execute("""
        CREATE TABLE catalog_textual_frames (
            AlmaId TEXT NOT NULL,
            UnitCatalogRecId INTEGER NOT NULL,
            TextualFrameHeb TEXT,
            TextualFrameEng TEXT
        )
    """)

    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            cat.UnitCatalogRecId,
            tf.TextualFrameHeb,
            tf.TextualFrameEng
        FROM dbo_InventoryAlma alma
        JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        JOIN (
            SELECT SetSignatureId, MAX(Version) as MaxVersion
            FROM dbo_Signature GROUP BY SetSignatureId
        ) lsv ON sig.SetSignatureId = lsv.SetSignatureId
            AND sig.Version = lsv.MaxVersion
        JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
        JOIN dbo_CatalogMultiTextualFrame_Simple tf
            ON cat.UnitCatalogRecId = tf.UnitCatalogRecId
    """)

    batch = []
    total = 0
    for row in tqdm(cursor, desc="  textual_frames", unit=" rows"):
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            target.executemany(
                "INSERT INTO catalog_textual_frames VALUES (?, ?, ?, ?)", batch
            )
            total += len(batch)
            batch = []

    if batch:
        target.executemany(
            "INSERT INTO catalog_textual_frames VALUES (?, ?, ?, ?)", batch
        )
        total += len(batch)

    target.execute("CREATE INDEX idx_cattf_alma ON catalog_textual_frames(AlmaId)")
    target.execute("CREATE INDEX idx_cattf_ucrid ON catalog_textual_frames(UnitCatalogRecId)")
    target.connection.commit()

    distinct_alma = target.execute(
        "SELECT COUNT(DISTINCT AlmaId) FROM catalog_textual_frames"
    ).fetchone()[0]
    print(f"  Exported {total:,} rows ({distinct_alma:,} distinct AlmaIds)")
    return total


def export_catalog_mentions(source, target):
    """Export catalog mentions (named entities) from FIST to sidecar.

    MentionType resolved via CODE_FullCode.ComputedCode:
    Personalities, Places, Creations, Dates, Groups, Other.
    Uses latest-version filter (same as catalog_fields).
    """
    print("Exporting catalog mentions...")

    target.execute("DROP TABLE IF EXISTS catalog_mentions")
    target.execute("""
        CREATE TABLE catalog_mentions (
            AlmaId TEXT NOT NULL,
            UnitCatalogRecId INTEGER NOT NULL,
            MentionType TEXT,
            Mention TEXT,
            MentionDesc TEXT
        )
    """)

    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            cat.UnitCatalogRecId,
            fc.EngDesc as MentionType,
            m.Mention,
            m.MentionDesc
        FROM dbo_InventoryAlma alma
        JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        JOIN (
            SELECT SetSignatureId, MAX(Version) as MaxVersion
            FROM dbo_Signature GROUP BY SetSignatureId
        ) lsv ON sig.SetSignatureId = lsv.SetSignatureId
            AND sig.Version = lsv.MaxVersion
        JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
        JOIN dbo_CatalogMultiMention m ON cat.UnitCatalogRecId = m.UnitCatalogRecId
        LEFT JOIN CODE_FullCode fc ON m.MentionTypeCode = fc.ComputedCode
    """)

    batch = []
    total = 0
    for row in tqdm(cursor, desc="  mentions", unit=" rows"):
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            target.executemany(
                "INSERT INTO catalog_mentions VALUES (?, ?, ?, ?, ?)", batch
            )
            total += len(batch)
            batch = []

    if batch:
        target.executemany(
            "INSERT INTO catalog_mentions VALUES (?, ?, ?, ?, ?)", batch
        )
        total += len(batch)

    target.execute("CREATE INDEX idx_catmn_alma ON catalog_mentions(AlmaId)")
    target.execute("CREATE INDEX idx_catmn_ucrid ON catalog_mentions(UnitCatalogRecId)")
    target.connection.commit()

    distinct_alma = target.execute(
        "SELECT COUNT(DISTINCT AlmaId) FROM catalog_mentions"
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


def export_genizah_persons(source, target):
    """Export CODE_GenizahPerson historical people reference table to sidecar."""
    print("Exporting genizah_persons...")

    target.execute("DROP TABLE IF EXISTS genizah_persons")
    target.execute("""
        CREATE TABLE genizah_persons (
            GenizahPersonId INTEGER PRIMARY KEY,
            EngDesc TEXT,
            HebDesc TEXT,
            HebDescAc TEXT
        )
    """)

    cursor = source.execute(
        "SELECT GenizahPersonId, EngDesc, HebDesc, HebDescAc "
        "FROM CODE_GenizahPerson"
    )
    rows = cursor.fetchall()
    target.executemany(
        "INSERT INTO genizah_persons VALUES (?, ?, ?, ?)", rows
    )
    target.connection.commit()

    total = len(rows)
    print(f"  Exported {total:,} rows")
    return total


def export_genizah_titles(source, target):
    """Export CODE_GenizahTitle work/title reference table to sidecar."""
    print("Exporting genizah_titles...")

    target.execute("DROP TABLE IF EXISTS genizah_titles")
    target.execute("""
        CREATE TABLE genizah_titles (
            GenizahTitleId INTEGER PRIMARY KEY,
            OrgTitle TEXT,
            EngTitle TEXT,
            DomainId INTEGER,
            AuthorId INTEGER,
            LanguageCode INTEGER
        )
    """)

    cursor = source.execute(
        "SELECT GenizahTitleID, OrgTitle, EngTitle, DomainId, AuthorId, "
        "LanguageCode FROM CODE_GenizahTitle"
    )
    rows = cursor.fetchall()
    target.executemany(
        "INSERT INTO genizah_titles VALUES (?, ?, ?, ?, ?, ?)", rows
    )
    target.execute("CREATE INDEX idx_gt_author ON genizah_titles(AuthorId)")
    target.connection.commit()

    total = len(rows)
    print(f"  Exported {total:,} rows")
    return total


def export_code_values(source, target):
    """Export CODE_FullCode decoded field values to sidecar.

    This single table decodes all integer code columns in the catalog
    (CreationTypeCode, PartVocalCode, etc.) via the composite key
    (FCDTableId, Code). FCDTableId maps: 22=CreationType, 26=TypeOfScript,
    27=TypeOfScriptPlace, 28=TypeOfScriptStyle, 29=TypeOfVocalization, etc.
    """
    print("Exporting code_values...")

    target.execute("DROP TABLE IF EXISTS code_values")
    target.execute("""
        CREATE TABLE code_values (
            FCDTableId INTEGER NOT NULL,
            Code INTEGER NOT NULL,
            EngDesc TEXT,
            HebDesc TEXT,
            PRIMARY KEY (FCDTableId, Code)
        )
    """)

    cursor = source.execute(
        "SELECT FCDTableId, FCDTableInnerId, EngDesc, HebDesc "
        "FROM CODE_FullCode "
        "WHERE IsCanceledCode IS NULL OR IsCanceledCode = 0"
    )
    rows = cursor.fetchall()
    target.executemany(
        "INSERT OR IGNORE INTO code_values VALUES (?, ?, ?, ?)", rows
    )
    target.connection.commit()

    total = len(rows)
    print(f"  Exported {total:,} rows")
    return total


def create_fts5(target):
    """Create contentless FTS5 index spanning catalog + running titles + free desc + full texts + detailed frames."""
    print("Creating FTS5 index...")

    target.execute("DROP TABLE IF EXISTS catalog_fts")
    target.execute("""
        CREATE VIRTUAL TABLE catalog_fts USING fts5(
            AlmaId,
            Title,
            TitleHeb,
            TextualFrameHeb,
            TextualFrameEng,
            RunningTitle,
            FreeDescription,
            FullText,
            DetailedFrames,
            content='',
            content_rowid='rowid'
        )
    """)

    # Build aggregated rows: one row per AlmaId with all searchable text
    alma_ids = target.execute("SELECT DISTINCT AlmaId FROM catalog").fetchall()

    # Check if new tables exist (forward compat)
    has_full_texts = _table_exists(target, 'catalog_full_texts')
    has_textual_frames = _table_exists(target, 'catalog_textual_frames')

    batch = []
    total = 0
    for (alma_id,) in tqdm(alma_ids, desc="  fts5", unit=" docs"):
        # Get first catalog record's text fields
        cat_row = target.execute(
            "SELECT Title, TitleHeb, TextualFrameHeb, TextualFrameEng "
            "FROM catalog WHERE AlmaId = ? LIMIT 1",
            (alma_id,),
        ).fetchone()

        # Aggregate running titles for this AlmaId
        rt_rows = target.execute(
            "SELECT GROUP_CONCAT(RunningTitle, '; ') FROM catalog_running_titles WHERE AlmaId = ?",
            (alma_id,),
        ).fetchone()
        running_titles = rt_rows[0] if rt_rows and rt_rows[0] else ''

        # Aggregate free descriptions for this AlmaId
        fd_rows = target.execute(
            "SELECT GROUP_CONCAT(FreeDesc, '; ') FROM catalog_free_desc WHERE AlmaId = ?",
            (alma_id,),
        ).fetchone()
        free_descs = fd_rows[0] if fd_rows and fd_rows[0] else ''

        # Aggregate full texts for this AlmaId
        full_texts = ''
        if has_full_texts:
            ft_rows = target.execute(
                "SELECT GROUP_CONCAT(FullText, '; ') FROM catalog_full_texts WHERE AlmaId = ?",
                (alma_id,),
            ).fetchone()
            full_texts = ft_rows[0] if ft_rows and ft_rows[0] else ''

        # Aggregate detailed textual frames for this AlmaId
        detailed_frames = ''
        if has_textual_frames:
            tf_rows = target.execute(
                "SELECT GROUP_CONCAT(COALESCE(TextualFrameEng, '') || ' ' || COALESCE(TextualFrameHeb, ''), '; ') "
                "FROM catalog_textual_frames WHERE AlmaId = ?",
                (alma_id,),
            ).fetchone()
            detailed_frames = tf_rows[0] if tf_rows and tf_rows[0] else ''

        batch.append((
            alma_id,
            cat_row[0] if cat_row else '',  # Title
            cat_row[1] if cat_row else '',  # TitleHeb
            cat_row[2] if cat_row else '',  # TextualFrameHeb
            cat_row[3] if cat_row else '',  # TextualFrameEng
            running_titles,
            free_descs,
            full_texts,
            detailed_frames,
        ))

        if len(batch) >= BATCH_SIZE:
            target.executemany(
                "INSERT INTO catalog_fts(AlmaId, Title, TitleHeb, TextualFrameHeb, "
                "TextualFrameEng, RunningTitle, FreeDescription, FullText, DetailedFrames) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch,
            )
            total += len(batch)
            batch = []

    if batch:
        target.executemany(
            "INSERT INTO catalog_fts(AlmaId, Title, TitleHeb, TextualFrameHeb, "
            "TextualFrameEng, RunningTitle, FreeDescription, FullText, DetailedFrames) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
        total += len(batch)

    target.connection.commit()
    print(f"  FTS5 index created with {total:,} entries")


def _table_exists(cursor, table_name: str) -> bool:
    """Check if a table exists in the target database."""
    result = cursor.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return result[0] > 0


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
        rt_count = export_catalog_running_titles(source, target)
        sz_count = export_catalog_sizes(source, target)
        fld_count = export_catalog_fields(source, target)
        fd_count = export_catalog_free_desc(source, target)
        ft_count = export_catalog_full_texts(source, target)
        tf_count = export_catalog_textual_frames(source, target)
        mn_count = export_catalog_mentions(source, target)
        bib_count = export_bibliography(source, target)
        catref_count = export_catalog_refs(source, target)
        refcat_count = export_ref_catalogs(source, target)
        reftitle_count = export_ref_titles(source, target)
        refauthor_count = export_ref_authors(source, target)
        gp_count = export_genizah_persons(source, target)
        gt_count = export_genizah_titles(source, target)
        cv_count = export_code_values(source, target)
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
        print(f"  domains:                   {domain_count:>10,} rows")
        print(f"  joins:                     {join_count:>10,} rows")
        print(f"  catalog:                   {catalog_count:>10,} rows")
        print(f"  catalog_running_titles:    {rt_count:>10,} rows")
        print(f"  catalog_sizes:             {sz_count:>10,} rows")
        print(f"  catalog_fields:            {fld_count:>10,} rows")
        print(f"  catalog_free_desc:         {fd_count:>10,} rows")
        print(f"  catalog_full_texts:        {ft_count:>10,} rows")
        print(f"  catalog_textual_frames:    {tf_count:>10,} rows")
        print(f"  catalog_mentions:          {mn_count:>10,} rows")
        print(f"  bibliography:              {bib_count:>10,} rows")
        print(f"  catalog_refs:              {catref_count:>10,} rows")
        print(f"  ref_catalogs:              {refcat_count:>10,} rows")
        print(f"  ref_titles:                {reftitle_count:>10,} rows")
        print(f"  ref_authors:               {refauthor_count:>10,} rows")
        print(f"  genizah_persons:           {gp_count:>10,} rows")
        print(f"  genizah_titles:            {gt_count:>10,} rows")
        print(f"  code_values:               {cv_count:>10,} rows")
        print(f"  File size: {file_size_mb:.1f} MB")

    finally:
        source_conn.close()
        target_conn.close()


if __name__ == "__main__":
    main()
