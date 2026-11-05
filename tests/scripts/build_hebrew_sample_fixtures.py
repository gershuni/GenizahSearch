#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Reproducible builder for the Hebrew extraction fixtures.

Generates, into tests/fixtures/local_indexer/:

  1. hebrew_sample.pdf + hebrew_sample.expected.txt
     Two pages of plain Hebrew prose, one line of the .txt per line of the PDF.
     Used by test_pymupdf_hebrew_extraction_quality (line-anchor overlap),
     the 3-tuple contract tests, the non-empty-text test and the PyInstaller
     smoke test.

  2. hebrew_rtl_fixture.pdf
     One page of Hebrew with vocalized (nikud-bearing) words. Used by
     test_sort_true_rtl_real_hebrew_fixture and
     test_nikud_bearing_yield_hebrew_fixture.

Every word of the text below was written for these fixtures; none of it is
copied from a book, an edition or a corpus, so the files can ship with the
code. (Two vocalized phrases are verses of Genesis, which are in the public
domain.) Do not replace this text with an excerpt from a real publication.

Layout follows real Hebrew PDFs: words are placed right to left, and each
word is written as its REVERSED string, so the first logical letter sits at
the highest x -- the same convention as build_phase102_fixtures.py.

Usage::

    python tests/scripts/build_hebrew_sample_fixtures.py

Idempotent: regenerates the three files in place.
"""
from __future__ import annotations

import os
import sys
import unicodedata

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import fitz  # PyMuPDF  # noqa: E402

_FIXTURES_DIR = os.path.join(_REPO_ROOT, "tests", "fixtures", "local_indexer")

_HEBREW_FONT_CANDIDATES = [
    "C:/Windows/Fonts/arial.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    "/System/Library/Fonts/Supplemental/Arial.ttf",
]

_PAGE_W, _PAGE_H = 595.0, 842.0  # A4 in points
_MARGIN = 60.0
_FONT_SIZE = 12.0
_LINE_GAP = 22.0
_LINES_PER_PAGE = 14

# Original prose written for this fixture (see the module docstring).
SAMPLE_PARAGRAPHS = [
    "גניזת קהיר היא אוסף גדול של כתבי יד וקטעי כתבי יד שנשמרו במשך מאות שנים "
    "בבית הכנסת בן עזרא שבפסטאט. הקטעים מפוזרים היום בספריות רבות בעולם, ולכן "
    "מלאכת זיהוים וצירופם זה לזה נמשכת עד היום.",
    "בין הקטעים נמצאים ספרי מקרא, פירושים, פיוטים, תשובות של גאונים, מכתבים "
    "פרטיים, שטרות וחשבונות של סוחרים. רבים מהם כתובים עברית, ואחרים בערבית "
    "יהודית או בארמית.",
    "חוקר המבקש לזהות קטע חדש משווה את לשונו לטקסטים ידועים, בודק את הכתב ואת "
    "החומר, ומחפש קטעים אחרים שנכתבו בידי אותו סופר. כאשר שני קטעים משלימים זה "
    "את זה, אפשר לצרף אותם ולשחזר דף שלם.",
    "מנוע החיפוש מאפשר לחפש מילים וצירופים בתעתיקים של הקטעים, לסנן לפי ספרייה "
    "ולפי תחום, ולשמור רשימות אישיות. החיפוש מתחשב בחילופי כתיב נפוצים, כגון "
    "כתיב מלא וחסר וחילופי אותיות דומות.",
    "שמות של ספריות מופיעים בקטלוגים בצורות שונות, ולכן כל סימן מדף נשמר בכמה "
    "גרסאות. החיפוש לפי סימן מדף מנרמל רווחים, נקודות ואותיות גדולות וקטנות.",
    "כאשר מתגלה טעות בתעתיק, המשתמשים יכולים להציע תיקון. התיקונים נבדקים לפני "
    "שהם מתפרסמים, וכך משתפר הטקסט עם הזמן ומשרת את כל הקוראים.",
    "ספרייה אישית מאפשרת לחפש גם בקבצים שבמחשב של המשתמש: מסמכים, קובצי טקסט "
    "וקובצי פי די אף. הקבצים נשארים במחשב ואינם נשלחים לשום מקום.",
    "בקובצי פי די אף רבים האותיות העבריות שמורות בסדר הפוך, מן השמאל אל הימין. "
    "לכן התוכנה מסדרת מחדש את המילים ואת האותיות לפני שהיא מוסיפה אותן לאינדקס.",
    "בעמוד השני מופיעות שורות נוספות, כדי לבדוק שהחילוץ עובר נכון בין עמודים "
    "ושומר על סדר הקריאה מימין לשמאל בכל שורה ושורה.",
    "הקובץ הזה נוצר במיוחד לבדיקות התוכנה. הטקסט נכתב לצורך הבדיקה ואינו מועתק "
    "מספר או ממאגר כלשהו, ולכן אפשר להפיץ אותו יחד עם הקוד בלי הגבלה.",
    "מי שמבקש להרחיב את הבדיקות יכול להוסיף פסקאות משלו לרשימה שבסקריפט, להריץ "
    "אותו מחדש ולבדוק שהקובץ החדש עדיין עובר את כל הבדיקות הקיימות.",
]

RTL_FIXTURE_LINES = [
    "בְּרֵאשִׁית בָּרָא אֱלֹהִים אֵת הַשָּׁמַיִם וְאֵת הָאָרֶץ",
    "וַיֹּאמֶר אֱלֹהִים יְהִי אוֹר וַיְהִי אוֹר",
    "שורה זו כתובה בלי ניקוד כדי שיהיו בעמוד גם מילים רגילות",
    "שָׁלוֹם לְכָל הַקּוֹרְאִים בַּקְּטָעִים הָאֵלֶּה",
    "סדר המילים בשורה צריך להישמר גם אחרי החילוץ מן הקובץ",
]


def _font_path() -> str:
    for path in _HEBREW_FONT_CANDIDATES:
        if os.path.exists(path):
            return path
    raise SystemExit("No Hebrew-capable font found (Arial, DejaVu Sans or FreeSans).")


def _wrap(paragraphs: list[str], font: fitz.Font) -> list[str]:
    """Greedy word wrap to the text width; one returned string per PDF line."""
    width = _PAGE_W - 2 * _MARGIN
    space = font.text_length(" ", fontsize=_FONT_SIZE)
    lines: list[str] = []
    for para in paragraphs:
        current: list[str] = []
        used = 0.0
        for word in para.split():
            w = font.text_length(word, fontsize=_FONT_SIZE)
            if current and used + space + w > width:
                lines.append(" ".join(current))
                current, used = [], 0.0
            used = w if not current else used + space + w
            current.append(word)
        if current:
            lines.append(" ".join(current))
    return lines


def _visual_order(word: str) -> str:
    """Reverse a word letter by letter, keeping each letter's points after it.

    Reversing code points would put every vowel point before its letter;
    reversing whole clusters (letter + its combining marks) keeps the
    vocalized words well formed, as in a real vocalized PDF.
    """
    clusters: list[str] = []
    for ch in word:
        if clusters and unicodedata.combining(ch):
            clusters[-1] += ch
        else:
            clusters.append(ch)
    return "".join(reversed(clusters))


def _write_rtl_line(page: fitz.Page, line: str, y: float, font: fitz.Font,
                    font_path: str) -> None:
    """Place the words of one line right to left, each as its reversed string."""
    space = font.text_length(" ", fontsize=_FONT_SIZE)
    x_right = _PAGE_W - _MARGIN
    for word in line.split():
        nfc = unicodedata.normalize("NFC", word)
        w = font.text_length(nfc, fontsize=_FONT_SIZE)
        page.insert_text(
            fitz.Point(x_right - w, y),
            _visual_order(nfc),
            fontsize=_FONT_SIZE,
            fontfile=font_path,
            fontname="heb",
            color=(0, 0, 0),
        )
        x_right -= w + space


def _write_pdf(dest: str, lines: list[str], font_path: str) -> None:
    font = fitz.Font(fontfile=font_path)
    doc = fitz.open()
    for start in range(0, len(lines), _LINES_PER_PAGE):
        page = doc.new_page(width=_PAGE_W, height=_PAGE_H)
        y = _MARGIN + _FONT_SIZE
        for line in lines[start:start + _LINES_PER_PAGE]:
            _write_rtl_line(page, line, y, font, font_path)
            y += _LINE_GAP
    doc.set_metadata({"title": "GenizahSearch Hebrew test fixture",
                      "author": "GenizahSearch tests",
                      "creator": "tests/scripts/build_hebrew_sample_fixtures.py"})
    doc.subset_fonts()  # embed only the glyphs used: keeps the fixtures small
    doc.save(dest, garbage=4, deflate=True)
    doc.close()


def main() -> None:
    os.makedirs(_FIXTURES_DIR, exist_ok=True)
    font_path = _font_path()
    font = fitz.Font(fontfile=font_path)

    sample_lines = _wrap(SAMPLE_PARAGRAPHS, font)
    sample_pdf = os.path.join(_FIXTURES_DIR, "hebrew_sample.pdf")
    sample_txt = os.path.join(_FIXTURES_DIR, "hebrew_sample.expected.txt")
    _write_pdf(sample_pdf, sample_lines, font_path)
    with open(sample_txt, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(sample_lines) + "\n")

    rtl_pdf = os.path.join(_FIXTURES_DIR, "hebrew_rtl_fixture.pdf")
    _write_pdf(rtl_pdf, RTL_FIXTURE_LINES, font_path)

    print(f"Font: {font_path}")
    for path in (sample_pdf, sample_txt, rtl_pdf):
        print(f"  {os.path.basename(path)}: {os.path.getsize(path)} bytes")
    print(f"  {len(sample_lines)} lines in hebrew_sample")


if __name__ == "__main__":
    main()
