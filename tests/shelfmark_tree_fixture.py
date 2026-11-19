# -*- coding: utf-8 -*-
"""A small FGP dump and ``libraries.csv`` for the shelf tree tests (scripts/build_shelfmark_tree.py).

Three libraries (a deep one coded ``CUL``, a single-collection one coded ``Strasbourg``, a
collector's one coded ``Harkavy``) and a cancelled one; records that FGP places and records the
matching rules must place (or refuse to place). ``SYS`` names every record a test asserts on.
"""
from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

SYS = {
    # placed by FGP
    "as2": [f"9900000{n:02d}0205171" for n in range(1, 9)],   # T-S AS 2.1 .. 2.8
    "as10": [f"9900001{n:02d}0205171" for n in range(1, 4)],  # T-S AS 10.1 .. 10.3
    "ns5": [f"9900002{n:02d}0205171" for n in range(1, 7)],   # T-S NS 5.1 .. 5.6
    "or": [f"9900003{n:02d}0205171" for n in range(1, 7)],    # Or. 1 .. 6, no volume
    "stras": [f"9900004{n:02d}0205171" for n in range(1, 7)],
    "kiev": [f"9900005{n:02d}0205171" for n in range(1, 7)],
    "inactive_volume": "990000601010205171",   # T-S AS 1.1, its volume is inactive
    "two_places": "990000602010205171",        # on T-S AS 2.3 and on T-S NS 5.3
    "shared_row": "990000603010205171",        # shares the T-S AS 2.4 row with as2[3]
    "oxford_part": "990000604010205171",       # Or. 6's second record, with a Part
    # not placed by FGP
    "exact": "990000701010205171",             # T-S AS 2.9: an unlinked row of volume T-S AS 2
    "boundary": "990000702010205171",          # T-S AS 29: must not match T-S AS 2.9
    "by_head": "990000703010205171",           # Or. 77
    "foreign": "990000704010205171",           # coded CUL, its long form names Vernadsky
    "single": "990000705010205171",            # Strasbourg, no usable head: the one collection
    "collector": "990000706010205171",         # Harkavy, no usable head: a collector's code
    "no_code": "990000707010205171",           # no code, no shelfmark
    "no_fist": "990000708010205171",           # coded Lutzki, which FGP has no library for
    "collision": "990000709010205171",         # T-S X 1 is a row in two collections
    "inactive_row": "990000710010205171",      # linked only to an inactive inventory row
}

CANARIES = {
    "collections": (("CUL", ("T-S NS", "T-S AS", "Or.")),),
    "first_leaves": (("T-S AS 2", ("T-S AS 2.1", "T-S AS 2.2", "T-S AS 2.3")),),
    "other": (SYS["collector"],),
}

_FIST_SCHEMA = """
CREATE TABLE CODE_Library (LibraryId INTEGER, LibraryNameAc TEXT, LibraryNameEng TEXT, LibraryNameHeb TEXT,
                           IsPrivate INTEGER, IsCanceledCode INTEGER);
CREATE TABLE CODE_Collection (CollectionId INTEGER, LibraryId INTEGER, CollectionName TEXT, DisplayOrder INTEGER,
                              IsCanceledCode INTEGER);
CREATE TABLE dbo_InventoryGroup (InventoryGroupId INTEGER, CollectionId INTEGER, InventoryGroupDesc TEXT,
                                 OrderBy1Num INTEGER, OrderBy2Num INTEGER, OrderBy3Text TEXT, RecordStatus INTEGER);
CREATE TABLE dbo_Inventory (InventoryId INTEGER, CollectionId INTEGER, InventoryGroupId INTEGER, Shelfmark TEXT,
    OBCollectionCodeOrder INTEGER, OBBoxNumber INTEGER, OBBox TEXT, OBVolume INTEGER, OBVolumeText TEXT,
    OBFolio REAL, OBFolioText TEXT, OBFolioMtf1 REAL, OBFolioMtf1Text TEXT, OBFolioMtf2 REAL, OBFolioMtf2Text TEXT,
    RecordStatus INTEGER, MoveToInventoryId INTEGER);
CREATE TABLE dbo_InventoryAlma (ID INTEGER PRIMARY KEY, InventoryId INTEGER, AlmaId INTEGER);
"""


def make_fist(path: Path, moved: bool = False) -> Path:
    con = sqlite3.connect(str(path))
    con.executescript(_FIST_SCHEMA)
    con.executemany("INSERT INTO CODE_Library VALUES (?,?,?,?,?,?)", [
        (1, "CUL", "Cambridge University Library", "ספריית אוניברסיטת קיימברידג'", 0, 0),
        (2, "Strasbourg", "National and University Library of Strasbourg", "שטרסבורג", 0, 0),
        (3, "Kiev", "Vernadsky National Library", "קייב", 0, 0),
        (4, "Gone", "A cancelled library", "", 0, 1),
    ])
    con.executemany("INSERT INTO CODE_Collection VALUES (?,?,?,?,?)", [
        (10, 1, "T-S AS", 2, 0), (11, 1, "T-S NS", 1, 0), (12, 1, "Or.", 3, 0), (13, 1, "Cancelled", 4, 1),
        (20, 2, "Strasbourg", 1, 0), (30, 3, "Kiev", 1, 0), (40, 4, "Gone", 1, 0),
        (99, -999, "Undefined Shelfmarks", None, 0),
    ])
    con.executemany("INSERT INTO dbo_InventoryGroup VALUES (?,?,?,?,?,?,?)", [
        (100, 10, "T-S AS 2", 2, None, None, 0), (101, 10, "T-S AS 10", 10, None, None, 0),
        (102, 10, "T-S AS 1", 1, None, None, 1), (110, 11, "T-S NS 5", 5, None, None, 0),
    ])
    inv, links = [], []

    def row(iid, cid, gid, shelf, volume=None, folio=None, status=0):
        inv.append((iid, cid, gid, shelf, None, None, None, volume, None, folio, None, None, None, None, None,
                    status, (iid if moved and iid == 1001 else None)))

    for n in range(1, 10):                       # T-S AS 2.1 .. 2.9, the 9th unlinked
        row(1000 + n, 10, 100, f"T-S AS 2.{n}", 2, float(n))
    for n in range(1, 4):
        row(1100 + n, 10, 101, f"T-S AS 10.{n}", 10, float(n))
    row(1201, 10, 102, "T-S AS 1.1", 1, 1.0)     # an inactive volume
    for n in range(1, 7):
        row(1300 + n, 11, 110, f"T-S NS 5.{n}", 5, float(n))
    for n in range(1, 7):
        row(1400 + n, 12, None, f"Or. {n}", None, float(n))
    for n in range(1, 7):
        row(2000 + n, 20, None, f"Ms. {n}", None, float(n))
    for n in range(1, 7):
        row(3000 + n, 30, None, f"Harkavy {n}", None, float(n))
    row(1501, 10, None, "T-S X 1")               # the same shelfmark in two collections
    row(1502, 11, None, "T-S X 1")
    row(1601, 10, 100, "T-S AS 2.99", 2, 99.0, status=1)   # inactive row
    row(1701, 99, None, "Undefined 1")
    con.executemany("INSERT INTO dbo_Inventory VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", inv)

    for n, sid in enumerate(SYS["as2"], 1):
        links.append((1000 + n, sid))
    for n, sid in enumerate(SYS["as10"], 1):
        links.append((1100 + n, sid))
    for n, sid in enumerate(SYS["ns5"], 1):
        links.append((1300 + n, sid))
    for n, sid in enumerate(SYS["or"], 1):
        links.append((1400 + n, sid))
    for n, sid in enumerate(SYS["stras"], 1):
        links.append((2000 + n, sid))
    for n, sid in enumerate(SYS["kiev"], 1):
        links.append((3000 + n, sid))
    links += [(1201, SYS["inactive_volume"]), (1003, SYS["two_places"]), (1303, SYS["two_places"]),
              (1004, SYS["shared_row"]), (1406, SYS["oxford_part"]), (1601, SYS["inactive_row"]),
              (1009, "990009999990205171")]   # the 2.9 row is linked to a manuscript not in libraries.csv
    con.executemany("INSERT INTO dbo_InventoryAlma (InventoryId, AlmaId) VALUES (?, ?)",
                    [(i, int(s)) for i, s in links])
    con.commit()
    con.close()
    return path


def make_libraries(path: Path) -> Path:
    rows = []
    for n, sid in enumerate(SYS["as2"], 1):
        rows.append((sid, "", f"T-S AS 2.{n}", "CUL"))
    for n, sid in enumerate(SYS["as10"], 1):
        rows.append((sid, "", f"T-S AS 10.{n}", "CUL"))
    for n, sid in enumerate(SYS["ns5"], 1):
        rows.append((sid, "", f"T-S NS 5.{n}", "CUL"))
    for n, sid in enumerate(SYS["or"], 1):
        rows.append((sid, "", f"Or. {n}|Cambridge University Library, Ms. Or. {n}", "CUL"))
    for n, sid in enumerate(SYS["stras"], 1):
        rows.append((sid, "", f"Ms. {n}", "Strasbourg"))
    for n, sid in enumerate(SYS["kiev"], 1):
        rows.append((sid, "", f"Harkavy {n}", "Harkavy"))
    rows += [
        (SYS["inactive_volume"], "", "T-S AS 1.1", "CUL"),
        (SYS["two_places"], "", "T-S NS 5.3", "CUL"),
        (SYS["shared_row"], "", "T-S AS 2.4", "CUL"),
        (SYS["oxford_part"], "MS. Heb. d. 29/2", "Or. 6", "CUL"),
        (SYS["exact"], "", "T-S AS 2.9", "CUL"),
        (SYS["boundary"], "", "T-S AS 29", "CUL"),
        (SYS["by_head"], "", "Or. 77", "CUL"),
        (SYS["foreign"], "", "Or. 78|Vernadsky National Library of Ukraine, Ms. Or. 78", "CUL"),
        (SYS["single"], "", "Q 5", "Strasbourg"),
        (SYS["collector"], "", "Q 6", "Harkavy"),
        (SYS["no_code"], "", "", ""),
        (SYS["no_fist"], "", "Lutzki 5", "Lutzki"),
        (SYS["collision"], "", "T-S X 1", "CUL"),
        (SYS["inactive_row"], "", "T-S AS 2.99", "CUL"),
    ]
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["system_number", "oxford_part_id", "call_numbers", "library_code", "", "", "", "titles"])
        for sid, part, calls, code in rows:
            w.writerow([sid, part, calls, code, "", "", "", ""])
    return path


def build_fixture(tmp_path: Path, **kw):
    """Make the inputs and build the tree; answer (db path, libraries path, meta)."""
    from scripts.build_shelfmark_tree import build

    fist = make_fist(tmp_path / "FIST.db")
    libraries = make_libraries(tmp_path / "libraries.csv")
    out = tmp_path / "shelfmark_tree.db"
    meta = build(fist, libraries, out, log=lambda *_: None, **kw)
    return out, libraries, meta
