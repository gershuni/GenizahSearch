# -*- coding: utf-8 -*-
"""Synthetic inputs for the catalogue text sidecar tests: tiny FJMS and PGP sidecars, a
libraries.csv and a libraries_translations.db, written into a temporary directory.

Never the real data: every record below is made up to exercise one rule of
scripts/build_catalog_text_sidecar.py or shared/catalog_text_service.py.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# The manuscripts, by what each one exercises.
A = "990000000000000001"  # FJMS work title כתובה (title column)
B = "990000000000000002"  # free description with הכתובה only (found through a prefix)
C = "990000000000000003"  # כתובה deep in a full catalogue text (fulltext column)
D = "990000000000000004"  # "marriage contract" only in an automatic translation
E = "990000000000000005"  # PGP description, the document lies on E and F
F = "990000000000000006"
G = "990000000000000007"  # library title with a placeholder segment
H = "990000000000000008"  # רמב"ם typed with an ASCII quote
I = "990000000000000009"  # a word with nikud
J = "990000000000000010"  # a literal * in the text
K = "990000000000000011"  # a long text with cheese in the middle; an unknown author
M = "990000000000000012"  # shares a work title (id 3000) with H: one translation, two manuscripts

LONG_TEXT = ("Letter about trade in the Mediterranean. " * 6
             + "The writer mentions a shipment of cheese from Sicily. "
             + "Further accounts of flax, silk and indigo follow at length. " * 6)


def write_fjms(path: Path, extra_translations=()) -> Path:
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE meta (key TEXT, value TEXT);
        CREATE TABLE catalog (AlmaId TEXT, GenizahTitleOrgTitle TEXT, GenizahTitleEngTitle TEXT, Title TEXT,
            TitleHeb TEXT, TextualFrameHeb TEXT, TextualFrameEng TEXT, Comment TEXT, Colophon TEXT,
            AuthorText TEXT, CopyPlace TEXT, GenizahTitleId INTEGER);
        CREATE TABLE catalog_running_titles (AlmaId TEXT, RunningTitle TEXT);
        CREATE TABLE catalog_free_desc (AlmaId TEXT, FreeDesc TEXT);
        CREATE TABLE catalog_full_texts (AlmaId TEXT, FullText TEXT);
        CREATE TABLE catalog_textual_frames (AlmaId TEXT, TextualFrameHeb TEXT, TextualFrameEng TEXT);
        CREATE TABLE catalog_mentions (AlmaId TEXT, Mention TEXT, MentionDesc TEXT);
        CREATE TABLE fjms_translations (id INTEGER PRIMARY KEY, alma_id TEXT, field_name TEXT,
            original_text TEXT, translated_text TEXT, direction TEXT, model_version TEXT);
    """)
    conn.executemany("INSERT INTO meta VALUES (?, ?)", [("version", "5.1.0"), ("created", "2026-03-11T06:25:38")])
    conn.executemany("INSERT INTO catalog VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", [
        (A, "כתובה", None, None, None, "תפילין", None, None, None, None, None, 1000),
        (A, "כתובה", None, None, None, None, None, None, None, None, None, 1000),   # second record, same title
        (C, "שטר", None, None, None, None, None, None, None, "נתן בן נהוראי", None, 2000),
        (G, "לא ידוע", None, None, None, None, None, None, None, None, None, -1),    # FJMS's unknown work
        (H, "שאלות ותשובות", None, None, None, None, None, None, None, None, None, 3000),
        (M, "שאלות ותשובות", None, None, None, None, None, None, None, None, None, 3000),
        (K, None, None, None, None, None, None, None, None, "לא מזוהה", None, None),     # FJMS's unknown author
    ])
    conn.executemany("INSERT INTO catalog_running_titles VALUES (?, ?)", [
        (H, 'תשובות רמב"ם'),
    ])
    conn.executemany("INSERT INTO catalog_free_desc VALUES (?, ?)", [
        (B, "שטר הכתובה של פלונית"),
        (D, "כתובת סת אלדאר"),
        (J, "Note * with a star, about cheese"),
        ("12345", "a row whose sys_id is not a corpus sys_id"),
    ])
    conn.executemany("INSERT INTO catalog_full_texts VALUES (?, ?)", [
        (C, "שטר מבית דין. " * 8 + "ובו נזכרת כתובה ישנה. " + "ועוד דברים. " * 20),
        (K, LONG_TEXT),
    ])
    conn.executemany("INSERT INTO catalog_textual_frames VALUES (?, ?, ?)", [
        (A, "תפילין", None),            # the same text as A's catalog frame: stored once
        (I, "סדר שָׁלוֹם", "Order of peace"),
    ])
    conn.executemany("INSERT INTO catalog_mentions VALUES (?, ?, ?)", [
        (C, "שמואל בן חפני", "גאון"),
    ])
    rows = [
        (D, "FreeDesc", "Marriage contract (ketubba) of Sitt al-Dar"),
        (A, "RunningTitle", "כתובה"),        # identical to an original of A: left out
        (A, "PersonEngDesc", "A person, not a manuscript"),
        # Keyed by the work title's id and by the author's text, not by a manuscript:
        ("3000", "GenizahTitleEngTitle", "Questions and Answers"),   # to H and M
        ("-1", "GenizahTitleEngTitle", "Unknown"),                   # the unknown work: nowhere
        ("5555", "GenizahTitleEngTitle", "Orphan title"),            # no catalogue record: nowhere
        ("נתן בן נהוראי", "AuthorText", "Nathan ben Nahorai"),       # to C
        ("לא מזוהה", "AuthorText", "Unknown"),                       # to K, but a placeholder
        *extra_translations,
    ]
    conn.executemany("INSERT INTO fjms_translations (alma_id, field_name, translated_text) VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.close()
    return path


def write_pgp(path: Path) -> Path:
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE meta (key TEXT, value TEXT);
        CREATE TABLE documents (pgpid INTEGER PRIMARY KEY, description TEXT);
        CREATE TABLE document_fragments (id INTEGER PRIMARY KEY, document_id INTEGER, sys_id TEXT);
    """)
    conn.executemany("INSERT INTO meta VALUES (?, ?)", [
        ("version", "1.1.0"), ("upstream_commit", "abc123"), ("upstream_committed", "2026-09-21T19:31:40Z"),
        ("documents_count", "2"),
    ])
    conn.executemany("INSERT INTO documents VALUES (?, ?)", [
        (1, "Marriage contract (ketubba). Fusṭāṭ, 1100."),
        (2, ""),
    ])
    conn.executemany("INSERT INTO document_fragments (document_id, sys_id) VALUES (?, ?)", [
        (1, E), (1, F), (2, A), (1, None),
    ])
    conn.commit()
    conn.close()
    return path


def write_libraries_csv(path: Path) -> Path:
    lines = [
        "﻿system_number,oxford_part_id,call_numbers,library_code,,,,titles_non_placeholder",
        f"{G},,T-S 1.1,CUL,,,,פיוט ליום כפור;לא ניתן להכרעה מתוך הקטלוג.",
        f"{A},,T-S 1.2,CUL,,,,",
        f"{H},,T-S 1.3,CUL,,,,תשובות",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def write_title_translations(path: Path) -> Path:
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE meta (key TEXT, value TEXT);
        CREATE TABLE title_translations (system_number TEXT, english_title TEXT);
    """)
    conn.executemany("INSERT INTO meta VALUES (?, ?)", [("created_at", "2026-03-10T06:15:53")])
    conn.executemany("INSERT INTO title_translations VALUES (?, ?)", [
        (G, "Piyyut for Yom Kippur ; Cannot be determined from the catalog."),
        (H, "Responsa"),
    ])
    conn.commit()
    conn.close()
    return path


def write_inputs(dir_path: Path, extra_translations=()) -> dict[str, Path]:
    dir_path.mkdir(parents=True, exist_ok=True)
    return {
        "fjms": write_fjms(dir_path / "fjms_enrichment.db", extra_translations),
        "pgp": write_pgp(dir_path / "pgp.db"),
        "libraries_csv": write_libraries_csv(dir_path / "libraries.csv"),
        "title_translations": write_title_translations(dir_path / "libraries_translations.db"),
    }


def build_args(inputs: dict[str, Path], out: Path) -> list[str]:
    return [
        "--fjms", str(inputs["fjms"]), "--pgp", str(inputs["pgp"]),
        "--libraries-csv", str(inputs["libraries_csv"]),
        "--title-translations", str(inputs["title_translations"]), "--out", str(out),
    ]


def build_sidecar(dir_path: Path) -> Path:
    """Write the inputs and build the sidecar from them; returns the sidecar's path."""
    from scripts.build_catalog_text_sidecar import main

    inputs = write_inputs(dir_path / "inputs")
    out = dir_path / "catalog_data" / "catalog_text.db"
    assert main(build_args(inputs, out)) == 0
    return out
