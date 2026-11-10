"""A small FJMS sidecar for the catalogue-criteria tests (PR 1a of
docs/plans/CATALOGUE_CRITERIA_PLAN.md). A helper module, like
tests/fjms_filter_sidecar.py.

``build_criteria_sidecar(path, upgrade=True)`` writes a 5.0.0 file with the
columns the criteria read, then (by default) runs the real 5.1.0 step,
scripts/migrate_fjms_criteria.py, on it.

Manuscripts (UNIVERSE = all six; D and E have no catalogue record):

  A  style Square; region Oriental + Yemenite (two records); material Paper
     + Vellum; lines "19-21"; columns "2"; copied 1050-1100; vocalization
     Full; illustration; catalogue sizes 10x15 and 13x20; pages 10.5x14 /
     20 lines / left margin 1.5, and 12x30 / 31 lines
  B  style Semi-Cursive; region Spanish; material Vellum; lines "0";
     columns "0" and "1" (two records); copied 1027-8 (= 1027); vocalization
     None; colophon; page 11.5x16 / 24 lines / left margin 3
  C  region Unknown; lines "22,25"; CopyDate -99 with CopyToDate 1200 (no
     date); vocalization Unknown; massorah; a FLAGGED size 11x16
  D  uncatalogued; page 11x15 / 22 lines; measured material Paper
  E  uncatalogued, nothing recorded
  F  catalogued, nothing else recorded
"""
import sqlite3
import sys
from pathlib import Path

A, B, C, D, E, F = (f"99000000{n}0205171" for n in "123456")
UNIVERSE = frozenset({A, B, C, D, E, F})

_CATALOG = [
    # AlmaId, rec, NumRow, NumColumn, CopyDate, CopyToDate, PartVocalCode,
    # IllustrationCode, PalimpsestCode, Colophon, CantillationCode, Massorah, Autograph
    (A, 1, "19-21", "2", "1050", "1100", 1036, 1038, None, None, None, None, 0),
    (A, 2, None, None, None, None, None, None, None, None, None, None, 0),
    (B, 3, "0", "0", "1027", "8", 2036, None, None, "Copied by ...", None, None, 0),
    (B, 4, None, "1", None, None, None, None, None, None, None, None, 0),
    (C, 5, "22,25", None, "-99", "1200", -1036, None, None, None, None, 3, 0),
    (F, 6, None, None, None, None, None, None, None, None, None, None, 0),
]
_FIELDS = [
    (A, 1, "TypeOfScriptStyle", "Square", "מרובע"),
    (A, 1, "TypeOfScriptPlace", "Oriental", "מזרחי"),
    (A, 2, "TypeOfScriptPlace", "Yemenite", "תימני"),
    (A, 1, "FragmentMaterial", "Paper", "נייר"),
    (A, 1, "GenizahLanguages", "Hebrew", "עברית"),
    (B, 3, "GenizahLanguages", "Arabic", "ערבית"),
    (A, 2, "FragmentMaterial", "Vellum", "קלף"),
    (B, 3, "TypeOfScriptStyle", "Semi-Cursive", "בינוני"),
    (B, 3, "TypeOfScriptPlace", "Spanish", "ספרדי"),
    (B, 3, "FragmentMaterial", "Vellum", "קלף"),
    (C, 5, "TypeOfScriptPlace", "Unknown", "לא ידוע"),
]
_SIZES = [
    # AlmaId, SizeX, SizeY, Flag_WH_Swap, Flag_Unit_Error
    (A, 10.0, 15.0, None, None),
    (A, 13.0, 20.0, None, None),
    (C, 11.0, 16.0, "swap", None),
]
_PAGES = [
    # FGP, AlmaId, width, height, lines, left margin
    ("P1", A, 10.5, 14.0, 20, 1.5),
    ("P2", A, 12.0, 30.0, 31, None),
    ("P3", B, 11.5, 16.0, 24, 3.0),
    ("P4", D, 11.0, 15.0, 22, None),
]
_CODES = [(36, 1, "Full", "מלא"), (36, 2, "None", "אין"), (36, 3, "Partial", "חלקי"),
          (36, 4, "Exists", "קיים"), (36, -1, "Unknown", "לא ידוע"),
          (36, -2, "Cannot be determined", "לא ניתן לדעת")]


def build_criteria_sidecar(path, *, upgrade: bool = True, fts: bool = True) -> str:
    con = sqlite3.connect(str(path))
    con.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    con.execute("INSERT INTO meta VALUES ('version', '5.0.0')")
    con.execute(
        "CREATE TABLE catalog (AlmaId TEXT, UnitCatalogRecId INTEGER, Title TEXT,"
        " TitleHeb TEXT, AuthorText TEXT, TextualFrameHeb TEXT, TextualFrameEng TEXT,"
        " GenizahTitleId INTEGER, Author INTEGER, SourceName TEXT,"
        " NumRow TEXT, NumColumn TEXT, CopyDate TEXT, CopyToDate TEXT, PartVocalCode INTEGER,"
        " IllustrationCode INTEGER, PalimpsestCode INTEGER, Colophon TEXT,"
        " CantillationCode INTEGER, Massorah INTEGER, Autograph INTEGER)")
    con.executemany(
        "INSERT INTO catalog (AlmaId, UnitCatalogRecId, NumRow, NumColumn, CopyDate,"
        " CopyToDate, PartVocalCode, IllustrationCode, PalimpsestCode, Colophon,"
        " CantillationCode, Massorah, Autograph, Title, SourceName)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'Inventory')",
        [row + (f"Title {row[0][-8:]}",) for row in _CATALOG])
    con.execute("CREATE TABLE catalog_free_desc (AlmaId TEXT, SignatureId INTEGER,"
                " SourceName TEXT)")
    con.execute("CREATE TABLE catalog_fields (AlmaId TEXT NOT NULL, UnitCatalogRecId INTEGER"
                " NOT NULL, FieldCategory TEXT NOT NULL, FieldValue TEXT, FieldValueHeb TEXT)")
    con.executemany("INSERT INTO catalog_fields VALUES (?,?,?,?,?)", _FIELDS)
    con.execute("CREATE TABLE code_values (FCDTableId INTEGER NOT NULL, Code INTEGER NOT NULL,"
                " EngDesc TEXT, HebDesc TEXT, PRIMARY KEY (FCDTableId, Code))")
    con.executemany("INSERT INTO code_values VALUES (?,?,?,?)", _CODES)
    con.execute("CREATE TABLE catalog_sizes (AlmaId TEXT NOT NULL, Shelfmark TEXT,"
                " SizeX_cm REAL, SizeY_cm REAL, InnerSizeX_cm REAL, InnerSizeY_cm REAL,"
                " SizeUnit TEXT, Measurement_Scope TEXT, Flag_WH_Swap TEXT, Flag_Unit_Error TEXT)")
    con.executemany("INSERT INTO catalog_sizes (AlmaId, SizeX_cm, SizeY_cm, Flag_WH_Swap,"
                    " Flag_Unit_Error) VALUES (?,?,?,?,?)", _SIZES)
    con.execute(
        "CREATE TABLE computed_measurements (FGP TEXT NOT NULL, AlmaId TEXT,"
        " Page_Width_cm REAL, Page_Height_cm REAL, Num_Lines INTEGER, Left_Margin_cm REAL,"
        " Right_Margin_cm REAL, Top_Margin_cm REAL, Bottom_Margin_cm REAL,"
        " Written_Width_cm REAL, Written_Height_cm REAL, Avg_Line_Height_Text_mm REAL,"
        " Text_Density_per10cm REAL, DpiGrid INTEGER, DisplayDPI INTEGER,"
        " Flag_DPI_High INTEGER DEFAULT 0, Flag_DPI_Low INTEGER DEFAULT 0,"
        " Flag_Negative_Margin INTEGER DEFAULT 0, Flag_BifolioLoc_Error INTEGER DEFAULT 0)")
    con.executemany("INSERT INTO computed_measurements (FGP, AlmaId, Page_Width_cm,"
                    " Page_Height_cm, Num_Lines, Left_Margin_cm) VALUES (?,?,?,?,?,?)", _PAGES)
    con.execute("CREATE TABLE manuscript_measurements (AlmaId TEXT NOT NULL PRIMARY KEY,"
                " catalog_width_cm REAL, max_computed_width_cm REAL, catalog_height_cm REAL,"
                " max_computed_height_cm REAL, avg_num_lines REAL, avg_text_density REAL,"
                " material TEXT)")
    con.execute("INSERT INTO manuscript_measurements (AlmaId, material) VALUES (?, 'Paper')", (D,))
    con.execute("CREATE TABLE domains (AlmaId TEXT, Domain TEXT, ParentDomain TEXT,"
                " DomainHeb TEXT, ParentDomainHeb TEXT)")
    con.execute("INSERT INTO domains VALUES (?, 'Bible', NULL, 'מקרא', NULL)", (A,))
    if fts:  # the catalogue-text filters' index (text all/any/not)
        con.execute("CREATE VIRTUAL TABLE catalog_fts USING fts5(Title)")
        con.execute("INSERT INTO catalog_fts (rowid, Title) SELECT rowid, Title FROM catalog")
    con.commit()
    con.close()
    if upgrade:
        repo = Path(__file__).resolve().parent.parent
        if str(repo) not in sys.path:
            sys.path.insert(0, str(repo))
        from scripts.migrate_fjms_criteria import main
        assert main(["--db", str(path), "--apply"]) == 0
    return str(path)
