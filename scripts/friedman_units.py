#!/usr/bin/env python3
"""Recover dictionary units from Friedman's archived PDFs (plan step 1: entries first).

Reads the PDFs bound by ``friedman_data/source_manifest.json`` glyph by glyph, rebuilds
every printed line in logical reading order from glyph positions, and cuts the body into
units by the typography the printed book uses (its legend, "רשימת קיצורים וסימנים"):

- a Hadassah-Bold head opens an ENTRY,
- the Wingdings diamond (``❖``, "ציון התחלת ערך משנה") opens a SUB-ENTRY,
- a bold Roman numeral I–XI opens a BINYAN paragraph of the preceding entry,
- the bullet (``•``, "ציון התחלת המובאות") starts the quotations inside a unit,
- any other paragraph continues the preceding unit (across page breaks too).

Paragraph boundaries come from the extractor's blocks, corrected by the book's own marks:
the extractor merges most ❖ paragraphs into the paragraph before them and splits long
paragraphs, so a ❖ at the start of a line always opens a unit, a numeral or bold head at
the start of a line opens one when the previous line ends short of the margin, and a
paragraph without an opening mark continues the current unit.

Inside a unit the fonts separate the roles: Hadassah-Regular carries the definition and the
Judeo-Arabic quotations, FrankRuehl 11.5 the translations and references, FrankRuehl 9.5
the locators, Times 7.5 the shelfmarks, Times 10–12 Arabic script and Latin text.

Everything here is a TYPOGRAPHIC CANDIDATE: boundaries, heads, roles and keys are readings
of fonts and positions, not reviewed lexicography. Bold type also marks cross-references,
a Roman numeral can occur inside a quotation, and a gloss and a quotation share one font.
The output keeps every original reading, the source position of every unit, and a flag
for each known weakness, so review can start from it; nothing here is served.

    python scripts/friedman_units.py build      # needs PyMuPDF; reads the archived PDFs
    python scripts/friedman_units.py verify     # stdlib: binding, coverage, conservation
    python scripts/friedman_units.py show 'לא אבד'

``verify`` checks that the units are bound to the archived sources, that every printed page
1..N is covered exactly once, that every non-space glyph of every body page sits in exactly
one unit, and that the keys follow the current normalizer. It does not judge the readings.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.prepare_friedman_dictionary import (  # noqa: E402
    BODY_IDS, CREDIT_HE, VIEWER_BASE, checked_sources, read_json, sha256, write_json)
from shared.blau_normalize import NORMALIZER_VERSION, normalize_ja  # noqa: E402

UNITS_SCHEMA = "friedman-units-1"
RECOVERY_VERSION = 4  # 3 (2026-11-08): marks attach under their ink; 4 (same day): heads keep their notes, keys drop placeholders
STATUS = "typographic_candidate_not_reviewed"
TOP_BAND = 0.125     # running heads end above this share of the page height (measured: <= 0.122)
HEADING_BAND = 0.16  # a letter heading sits above this share, below the running head
BOTTOM_BAND = 0.89   # page numbers start below this share (measured: 0.87 and 0.90)
SPACE_GAP_EM = 0.15  # a horizontal gap of at least this many em between glyphs is a space
NARROW_GAP_EM = 0.1   # ... or this many, when a blank glyph sits in it (the diacritic font's blanks sit in no gap)
LINE_MERGE = 0.5     # fragments overlapping vertically by this share of the smaller height share a line
SHORT_LINE = 12.0    # a line ending this far right of the body margin ends a paragraph
HIDDEN_SIZE = 2.0    # the producer left 0.8 pt glyphs that print nothing
# Where a zero-advance mark's ink sits relative to the origin the producer gave it, in em: measured
# 2026-11-08 from the embedded fonts' glyph outlines (fontTools, by glyph id). The diacritic font
# (LucidaSansUnicode: the Judeo-Arabic dot, the two dots, the bar) draws 0.31 em LEFT of its origin,
# and the producer sets that origin at the base's RIGHT edge, so it lies inside the box of the
# letter BEFORE the base; attaching by the origin put the dot on that letter in 951 of 1,815
# dotted heads. The harakat and the Hebrew points draw to the right of theirs. Keyed by the font's
# basename and the mark's block: a Latin combining mark (U+0300-036F) or a point (anything else).
MARK_INK_EM = {("LucidaSansUnicode", "combining"): -0.307, ("ArialUnicodeMS", "combining"): -0.278,
               ("ArialUnicodeMS", "point"): 0.15, ("TimesNewRomanPSMT", "point"): 0.10,
               ("TimesNewRomanPSMT", "combining"): -0.27,  # one dot in the body; by its position
               ("FrankRuehl", "point"): 0.10, ("Hadassah-Regular", "point"): 0.25}
# Three FrankRuehl points whose subset glyph carries no outline to measure: from their positions
# over the letters they sit in (the dagesh inside, the shin and sin dots above the right arm).
MARK_INK_EM_BY_CHAR = {("FrankRuehl", "ּ"): -0.18, ("FrankRuehl", "ׁ"): -0.30,
                       ("FrankRuehl", "ׂ"): -0.30}
BULLET = "•"
SUBENTRY_MARK = "❖"
# The two Wingdings glyphs the legend assigns; the PUA codes are the same glyphs as Word
# sometimes stores them.
WINGDINGS = {"y": BULLET, "": BULLET, "v": SUBENTRY_MARK, "": SUBENTRY_MARK}
BINYAN_PUNCTUATION = frozenset("/()<>?:.- ")
PAGE_NUMBER_RE = re.compile(r"^[\[\]](\d{1,4})[\[\]]$")
LETTER_HEADING_RE = re.compile(r"^[א-ת]̇?\s*\([^()]*\)$")
NUMBER_RE = re.compile(r"\d+(?:[.,]\d+)*")
# Section files are named by the letter's numerical value; "_" marks the dotted letter.
_LETTER_VALUES = dict(zip([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400],
                          "אבגדהוזחטיכלמנסעפצקרשת"))
SECTION_LETTERS = {sid: _LETTER_VALUES[int(sid.rstrip("_"))] + ("̇" if sid.endswith("_") else "")
                   for sid in BODY_IDS}
UNIT_KINDS = ("entry", "subentry", "binyan", "unclassified")
OPENING = {"head": "entry", "subentry": "subentry", "binyan": "binyan"}


def classify_char(font: str, size: float, char: str) -> str:
    """Role of one glyph from its font: the book's typography, nothing semantic."""
    if size < HIDDEN_SIZE:
        return "hidden"
    if unicodedata.category(char).startswith("M") or "ﱞ" <= char <= "ﱣ" or "ﹰ" <= char <= "ﹿ":
        return "mark"  # combining marks, and the Arabic harakat the producer stored as presentation forms
    if char.isspace():
        return "space"
    base = font.split("+")[-1]
    if "Wingdings" in base:
        glyph = WINGDINGS.get(char)
        return "bullet" if glyph == BULLET else "subentry" if glyph == SUBENTRY_MARK else "other"
    if "Hadassah-Bold" in base:
        return "head"
    if base.startswith("TimesNewRomanPS-Bold"):
        return "binyan" if char in "IVX" or char in BINYAN_PUNCTUATION else "foreign"
    if base.startswith("Hadassah"):
        return "ja"
    if base.startswith("FrankRu"):
        return "loc" if size < 11 else "he"
    if base.startswith("TimesNewRoman"):
        return "shelf" if size <= 8 else "foreign"
    return "other"


def _centre(c: dict) -> float:
    return (c["x0"] + c["x1"]) / 2


def mark_anchor(font: str, size: float, char: str, origin: float) -> float:
    """The x under a combining mark's ink: its origin moved by the font's measured offset."""
    base = font.split("+")[-1]
    block = "combining" if "̀" <= char <= "ͯ" else "point"
    return origin + size * MARK_INK_EM_BY_CHAR.get((base, char), MARK_INK_EM.get((base, block), 0.0))


def _bidi(glyph: dict) -> str:
    """L for Latin letters and digits, R for everything that keeps the line's direction."""
    if glyph["cls"] in ("bullet", "subentry") or glyph["c"] in "()[]{}":
        return "R"  # the book's marks and brackets never belong to a Latin or numeric run
    kind = unicodedata.bidirectional(glyph["c"])
    return "L" if kind in ("L", "EN", "AN") else "R" if kind in ("R", "AL") else "N"


def logical_line(chars: list[dict]) -> list[dict]:
    """Rebuild one printed line in reading order from glyph positions.

    The content stream interleaves right-to-left text, numbers and marks in no usable order.
    Base glyphs are sorted right to left; each combining mark follows the base glyph under its
    ink (``anchor``, from ``mark_anchor``: a mark's origin is not where it prints); a word space
    is a geometric gap (the diacritic font carries spaces inside words, and
    some real spaces carry no glyph); left-to-right runs are put back in reading order: a run
    with Latin letters as a whole, a numeric expression digit group by digit group, because
    the book prints a line range the way Word renders it (``46–45`` for ``45–46``).
    """
    bases = sorted((c for c in chars if c["cls"] not in ("mark", "space", "hidden")),
                   key=lambda c: (-_centre(c), -c["x0"]))
    if not bases:
        return []
    attached: dict[int, list[dict]] = {i: [] for i in range(len(bases))}
    for mark in sorted((c for c in chars if c["cls"] == "mark"), key=lambda c: -c.get("anchor", _centre(c))):
        anchor = mark.get("anchor", _centre(mark))
        host = min(range(len(bases)), key=lambda i: abs(_centre(bases[i]) - anchor))
        attached[host].append(dict(mark, cls=bases[host]["cls"]))
    blanks = [_centre(c) for c in chars if c["cls"] == "space"]
    clusters = []
    previous = None
    for i, base in enumerate(bases):
        if previous is not None:
            gap = previous["x0"] - base["x1"]
            em = min(previous["size"], base["size"])
            # A narrow gap (a justified bold head) counts when the producer did put a blank there.
            narrow = gap >= NARROW_GAP_EM * em and any(base["x1"] - 0.5 <= x <= previous["x0"] + 0.5 for x in blanks)
            if gap >= SPACE_GAP_EM * em or narrow:
                clusters.append([dict(previous, c=" ", cls="space")])
        clusters.append([base, *attached[i]])
        previous = base
    ordered = []
    i = 0
    while i < len(clusters):
        if _bidi(clusters[i][0]) != "L":
            ordered.append(clusters[i])
            i += 1
            continue
        last = i
        j = i
        while j < len(clusters) and _bidi(clusters[j][0]) != "R":
            if _bidi(clusters[j][0]) == "L":
                last = j
            j += 1
        run = clusters[i:last + 1]
        if any(unicodedata.bidirectional(cl[0]["c"]) == "L" for cl in run):
            ordered.extend(reversed(run))
        else:
            text = "".join(cl[0]["c"] for cl in run)
            pos = 0
            for match in NUMBER_RE.finditer(text):
                ordered.extend(run[pos:match.start()])
                ordered.extend(reversed(run[match.start():match.end()]))
                pos = match.end()
            ordered.extend(run[pos:])
        i = last + 1
    return [glyph for cluster in ordered for glyph in cluster]


def text_of(glyphs: list[dict]) -> str:
    return re.sub(r"\s+", " ", "".join(WINGDINGS.get(g["c"], g["c"]) if g["cls"] in ("bullet", "subentry") else g["c"]
                                       for g in glyphs)).strip()


def segments_of(glyphs: list[dict]) -> list[list[str]]:
    """Maximal runs of one role, as [role, text]; a space takes the preceding role."""
    segments: list[list[str]] = []
    for g in glyphs:
        ch = WINGDINGS.get(g["c"], g["c"]) if g["cls"] in ("bullet", "subentry") else g["c"]
        if g["cls"] == "space":
            if segments:
                segments[-1][1] += ch
            continue
        if segments and segments[-1][0] == g["cls"]:
            segments[-1][1] += ch
        else:
            segments.append([g["cls"], ch])
    return _merge_punctuation_runs([[cls, re.sub(r"\s+", " ", text)] for cls, text in segments])


def _merge_punctuation_runs(segments: list[list[str]]) -> list[list[str]]:
    """A run of punctuation set in another font between two runs of one role belongs to that
    role: the dots of ``[...]`` in the Hebrew font inside a quotation, the comma of a bold
    ``פילי, פיליא``. Without this the dots became the translation and the quotation kept ``[]``."""
    merged: list[list[str]] = []
    for cls, text in segments:
        if (len(merged) >= 2 and merged[-2][0] == cls and merged[-1][0] not in ("bullet", "subentry")
                and not any(ch.isalnum() for ch in merged[-1][1])):
            punctuation = merged.pop()[1]
            merged[-1][1] += punctuation + text
        else:
            merged.append([cls, text])
    return merged


def _first_role(glyphs: list[dict]) -> str | None:
    """The role that opens a paragraph; a brace (``{`` = an entry the book rejects) is looked past."""
    ink = [g for g in glyphs if g["cls"] != "space"]
    if len(ink) > 1 and ink[0]["c"] == "{" and ink[1]["cls"] in OPENING:
        return ink[1]["cls"]
    return ink[0]["cls"] if ink else None


def block_glyphs(block: dict) -> list[dict]:
    """One extractor block as glyphs in reading order, its lines joined by a space."""
    glyphs: list[dict] = []
    for line in page_lines([block])[0]:
        if glyphs:
            glyphs.append(dict(line["glyphs"][0], c=" ", cls="space"))
        glyphs.extend(line["glyphs"])
    return glyphs


def page_lines(blocks: list[dict]) -> tuple[list[dict], int]:
    """Every printed line of a page in reading order, with the extractor's block index.

    The extractor sometimes stores a line's Roman numeral or ❖ as a line of its own beside
    the Hebrew line; fragments that overlap vertically and not horizontally are one line.
    """
    fragments = []
    hidden = 0
    for index, block in enumerate(blocks):
        if block.get("type") != 0:
            continue
        for line in block["lines"]:
            chars = []
            for span in line["spans"]:
                font, size = span["font"], float(span["size"])
                if size < HIDDEN_SIZE:
                    hidden += len(span["chars"])
                for c in span["chars"]:
                    cls = classify_char(font, size, c["c"])
                    char = {"c": c["c"], "x0": c["bbox"][0], "x1": c["bbox"][2], "size": size, "cls": cls}
                    if cls == "mark":
                        char["anchor"] = mark_anchor(font, size, c["c"], c.get("origin", c["bbox"])[0])
                    chars.append(char)
            ink = [c for c in chars if c["cls"] not in ("space", "hidden")]
            if ink:  # the extent of the printed glyphs: a line's bbox also covers its blank glyphs
                fragments.append({"block": index, "bbox": list(line["bbox"]), "chars": chars,
                                  "ink": [min(c["x0"] for c in ink), max(c["x1"] for c in ink)]})
    fragments.sort(key=lambda f: (f["bbox"][1] + f["bbox"][3]) / 2)
    lines: list[dict] = []
    for fragment in fragments:
        last = lines[-1] if lines else None
        if last is not None:
            overlap = min(last["bbox"][3], fragment["bbox"][3]) - max(last["bbox"][1], fragment["bbox"][1])
            height = min(last["bbox"][3] - last["bbox"][1], fragment["bbox"][3] - fragment["bbox"][1])
            apart = fragment["ink"][0] >= last["ink"][1] - 1 or last["ink"][0] >= fragment["ink"][1] - 1
            if overlap >= LINE_MERGE * height and apart:
                last["chars"].extend(fragment["chars"])
                last["blocks"][fragment["block"]] += len(fragment["chars"])
                last["bbox"] = [min(last["bbox"][0], fragment["bbox"][0]), min(last["bbox"][1], fragment["bbox"][1]),
                                max(last["bbox"][2], fragment["bbox"][2]), max(last["bbox"][3], fragment["bbox"][3])]
                last["ink"] = [min(last["ink"][0], fragment["ink"][0]), max(last["ink"][1], fragment["ink"][1])]
                continue
        lines.append({"bbox": fragment["bbox"], "ink": fragment["ink"], "chars": fragment["chars"],
                      "blocks": Counter({fragment["block"]: len(fragment["chars"])})})
    result = []
    for line in lines:
        glyphs = logical_line(line["chars"])
        if text_of(glyphs):
            result.append({"bbox": [round(v, 1) for v in line["bbox"]], "glyphs": glyphs,
                           "block": line["blocks"].most_common(1)[0][0]})
    return result, hidden


def parse_page(blocks: list[dict], height: float) -> dict:
    """Split a page into running head, letter heading, page number and body paragraphs."""
    page = {"running_head": None, "letter_heading": None, "printed_page": None,
            "body_chars": 0, "page_number_chars": 0, "hidden_chars": 0, "paragraphs": []}
    lines, page["hidden_chars"] = page_lines(blocks)
    labels = []
    body = []
    for line in lines:
        text = text_of(line["glyphs"])
        if line["bbox"][3] < height * HEADING_BAND and LETTER_HEADING_RE.match(text):
            page["letter_heading"] = text  # a letter opens on a page of its own
        elif line["bbox"][3] < height * TOP_BAND:
            page["running_head"] = text if page["running_head"] is None else page["running_head"] + " " + text
        elif PAGE_NUMBER_RE.match(text.replace(" ", "")) or line["bbox"][1] > height * BOTTOM_BAND:
            number = PAGE_NUMBER_RE.match(text.replace(" ", ""))
            if number:
                labels.append(number.group(1))
                page["page_number_chars"] += sum(g["cls"] != "space" for g in line["glyphs"])
        else:
            body.append(line)
    page["printed_page"] = labels[0] if len(labels) == 1 else None
    margin = min((line["bbox"][0] for line in body), default=0.0)
    previous = None
    for line in body:
        page["body_chars"] += sum(g["cls"] != "space" for g in line["glyphs"])
        first = _first_role(line["glyphs"])
        opens = previous is None or line["block"] != previous["block"] or first == "subentry"
        flag = None
        if not opens and first in ("binyan", "head"):
            if previous["bbox"][0] > margin + SHORT_LINE:
                opens, flag = True, "split_from_block"
            else:
                flag = f"{first}_at_line_start"
        if opens:
            flags = [flag] if flag else []
            if next((g["c"] for g in line["glyphs"] if g["cls"] != "space"), "") == "{" and first in OPENING:
                flags.append("braced_head")  # "{ } = ערך הנזכר במילון אחר או בספרות המחקר אך יסודו בטעות"
            page["paragraphs"].append({"bbox": list(line["bbox"]), "glyphs": list(line["glyphs"]), "first": first,
                                       "flags": flags})
        else:
            paragraph = page["paragraphs"][-1]
            paragraph["glyphs"].append(dict(line["glyphs"][0], c=" ", cls="space"))
            paragraph["glyphs"].extend(line["glyphs"])
            paragraph["bbox"] = [min(paragraph["bbox"][0], line["bbox"][0]), paragraph["bbox"][1],
                                 max(paragraph["bbox"][2], line["bbox"][2]), line["bbox"][3]]
            if flag:
                paragraph["flags"].append(flag)
        previous = line
    return page


def strip_marks(text: str) -> str:
    """Drop combining marks and the Arabic harakat the producer stored as presentation forms."""
    return "".join(ch for ch in text if not unicodedata.category(ch).startswith("M")
                   and not ("ﱞ" <= ch <= "ﱣ" or "ﹰ" <= ch <= "ﹿ"))


def key_of(form: str) -> str:
    """The lookup key of a printed form: the shared normalizer over the mark-free form."""
    return normalize_ja(strip_marks(form))


def strip_marks_but_dots(text: str) -> str:
    """Vowels and presentation-form harakat dropped; the Judeo-Arabic dots and bars kept."""
    return "".join(ch for ch in text if ch in "̇̄̈" or not (
        unicodedata.category(ch).startswith("M") or "ﱞ" <= ch <= "ﱣ" or "ﹰ" <= ch <= "ﹿ"))


FORM_SPLIT_RE = re.compile(r"/|<|>|,|\s[רנ][׳']\s")  # alternatives (a reading and its correction: בצט<בסט), the legend's ר' (plural) and נ' (feminine)
# A note the book sets in parentheses after a form, carrying other forms of it: a variant
# spelling (<קרטאס, =קרטלה̈), the singular of a plural (ריבוי של סנג̇ה̈) or a plural (ר׳ אדבש).
NOTE_FORMS_RE = re.compile(r"^\s*(?:[=<>]|ריבוי של|ר[׳'])\s*(.+)$")
# One or two letters in parentheses glued to a word are optional letters, (א)קצר, not a note.
OPTIONAL_LETTERS_RE = re.compile(r"(?<=\S)\(([א-ת]{1,2})\)|\(([א-ת]{1,2})\)(?=\S)")
# A construction frame, not a form: the book writes the valency after the numeral or ❖ with
# pronoun suffixes (-ה, -הא), prepositions with suffixes (עליה, בהא, מנה) and placeholders
# (פעל "to do something", פלאן "so-and-so", שי "something"). Keys are never made from these.
_PRONOUNS = ("ה", "הא", "ך", "כם", "הם", "הן")  # the book writes frames in the third person (and -ך)
_PREPOSITIONS = ("ב", "ל", "מן", "עלי", "אלי", "פי", "ען", "מע", "ענד", "בין", "חתי", "כ", "עלא", "לדי")
FRAME_TOKENS = frozenset(normalize_ja(t) for t in (
    *_PRONOUNS, *_PREPOSITIONS, "ני", "נא", *(p + s for p in _PREPOSITIONS for s in _PRONOUNS),
    "פעל", "יפעל", "ליפעל", "פעלא", "פעלה", "אן", "אן יפעל", "בפעל", "פלאן", "פלאנה", "שי", "שיא", "אלשי",
    "כדא", "דלך", "ו", "אלא", "מא", "מכאן", "אלי מכאן", "בלד", "אלי בלד", "דאך", "הדא", "הדה"))


# The book's placeholders for a verb or a thing inside a multiword lemma (ען כלפה פעל, מכתוב
# כדא): never words of a quotation, so the key is made of the words around them.
PLACEHOLDERS = frozenset(normalize_ja(t) for t in ("פעל", "יפעל", "ליפעל", "פעלא", "פעלה", "בפעל", "כדא", "פלאן", "פלאנה"))


def _forms(text: str) -> list[str]:
    return [form.strip(" ()[]{}.;") for form in FORM_SPLIT_RE.split(" " + text + " ") if form.strip(" ()[]{}.;")]


def _extend(parts: list[str], text: str) -> None:
    """Add text to the last collected part, so that a bracket and its note stay one word."""
    if parts:
        parts[-1] += text
    else:
        parts.append(text)


def is_frame(form: str) -> bool:
    """True for a valency frame (``-הא``, ``עליה``, ``אן יפעל``); False for a lexical form."""
    key = key_of(form)
    return bool(form) and (form[0] in "-–" or (bool(key) and all(t in FRAME_TOKENS for t in key.split())))


def dotted_key(text: str) -> str:
    """Letters with their upper dots kept (ג̇ stays distinct from ג), everything else dropped."""
    kept = "".join(ch for ch in text if "א" <= ch <= "ת" or ch in "̇̄̈" or ch.isspace())
    return re.sub(r"\s+", " ", kept).strip()


def parse_head(segments: list[list[str]], kind: str = "entry") -> tuple[dict | None, str | None, int, list[str]]:
    """The bold opening of a unit: root, lemma, alternatives and lookup keys; its binyan.

    Bold text before the Roman numeral is the root, bold text after it the construction
    (``אבי V ען פעל/דלך``); without a numeral a colon separates root and lemma
    (``אבד: לאבדי``). The numeral group keeps the book's own spelling (``IV/I``, ``I(II?)``).
    A valency frame (``עליה``, ``אן יפעל``) is a lemma only where the book writes frames: after
    a colon or numeral, or as the head of a sub-entry; an entry ``אן`` is the particle itself.
    """
    before: list[str] = []
    after: list[str] = []
    numeral: list[str] = []
    stage = "before"
    end = len(segments)
    skip_until = 0
    for i, (cls, text) in enumerate(segments):
        if i < skip_until or cls == "subentry" or not text.strip() or (text.strip() == "{" and not before and not numeral):
            continue  # the ❖ and the brace of a rejected entry precede its head
        if cls == "binyan":
            if stage == "before" and before and all(set(part.strip()) <= BINYAN_PUNCTUATION for part in before):
                numeral, before = before, []
            numeral.append(text.strip())
            stage = "numeral"
        elif stage == "numeral" and set(text.strip()) <= BINYAN_PUNCTUATION:
            numeral.append(text.strip())  # the slash of "IV/I" is sometimes set in the text font
        elif cls == "head":
            parts = after if stage == "after" else before
            if "".join(parts).count("(") > "".join(parts).count(")"):
                _extend(parts, text)  # inside the head's own bracket the bold goes on without a space
            elif stage == "before":
                before.append(text)
            else:
                stage = "after"
                lead = text[:len(text) - len(text.lstrip(") "))]
                if lead.strip():
                    numeral.append(lead.strip())  # the closing bracket of "(IV/I) אויס"
                after.append(text[len(lead):].lstrip(": "))  # "כלו I: לא כלות" keeps its colon out of the lemma
        else:
            collected = " ".join(before + after)
            close = None
            if collected.count("(") > collected.count(")"):
                # A note the book sets in the text font inside the head's bracket, "(<קרטאס)",
                # "(ריבוי של סנג̇ה̈)", "(ולא רק פי)": the head goes on past it, so the segments up
                # to the closing bracket join the head, within a few segments and no quotation.
                close = next((j for j in range(i, min(i + 4, len(segments))) if ")" in segments[j][1]), None)
                if close is not None and (sum(len(segments[j][1]) for j in range(i, close + 1)) > 80
                                          or any(segments[j][0] in ("bullet", "subentry", "loc") for j in range(i, close))):
                    close = None
            if close is None:
                end = i
                break
            parts = after if stage == "after" else before
            for j in range(i, close):
                _extend(parts, segments[j][1])
            if segments[close][0] == "head":
                skip_until = close  # a bold closing bracket, and what follows it in bold, is the head
                continue
            inside, _, rest = segments[close][1].partition(")")
            _extend(parts, inside + ")")
            if rest.strip():
                segments[close:close + 1] = [[segments[close][0], inside + ")"], [segments[close][0], rest.lstrip()]]
                end = close + 1  # the body starts after the bracket; the unit's text keeps every glyph
                break
            skip_until = close + 1
    binyan = re.sub(r"\s+", "", "".join(numeral)) or None
    flags = []
    if sum(part.strip("()/<>?:.- ") != "" for part in numeral) > 1:
        flags.append("compound_binyan")
    head_text = re.sub(r"\s+", " ", " ".join(before + after)).strip()
    if not head_text:
        return None, binyan, end, flags
    plain = strip_marks(head_text)
    notes = [n.strip() for n in re.findall(r"\(([^()]*)\)", OPTIONAL_LETTERS_RE.sub("", plain))]

    def forms_of(optional_letters: bool) -> tuple[list[str], list[str], str | None]:
        """Root and lemma forms, with the optional letters of ``(א)קצר`` dropped or written."""
        def sub(text: str) -> str:
            text = OPTIONAL_LETTERS_RE.sub((lambda m: m.group(1) or m.group(2)) if optional_letters else "", text)
            return re.sub(r"\([^()]*\)", " ", text)
        if after:
            roots = _forms(sub(strip_marks(" ".join(before))))
            lemmas = _forms(sub(strip_marks(" ".join(after))))
            return roots, lemmas, "numeral" if before else None
        root, colon, lemma = sub(plain).partition(":")
        roots = _forms(root)
        lemmas = _forms(lemma) if colon else roots
        return roots, lemmas, "colon" if colon else "single_word" if len(" ".join(roots).split()) == 1 else "whole_head"

    def frames_of(roots: list[str], lemmas: list[str], source: str | None) -> list[str]:
        # The root (before a colon or numeral) is a word even when it looks like a frame (בני, ענד);
        # the construction after it, or a sub-entry's head, is a frame when it is made of frame tokens.
        if source in ("colon", "numeral"):
            return [form for form in lemmas if is_frame(form)] + [form for form in roots if form[0] in "-–"]
        if kind != "entry":
            return [form for form in roots if is_frame(form)]
        return [form for form in roots if form[0] in "-–"]

    root_forms, lemma_forms, root_source = forms_of(optional_letters=False)
    if after and before:
        flags.append("binyan_in_head")
    frames = frames_of(root_forms, lemma_forms, root_source)
    keyed = [form for form in [*lemma_forms, *root_forms] if form not in frames]
    if OPTIONAL_LETTERS_RE.search(plain):  # (א)קצר is keyed both ways
        roots, lemmas, source = forms_of(optional_letters=True)
        keyed += [form for form in [*lemmas, *roots] if form not in frames_of(roots, lemmas, source)]
    if keyed and frames:
        flags.append("complement_lemma")
    note_forms = [form for note in notes for match in [NOTE_FORMS_RE.match(note)] if match
                  for form in _forms(match.group(1)) if not is_frame(form)]
    keys = []
    for form in [*keyed, *note_forms]:
        key = key_of(form)
        words = key.split()
        if len(words) > 1:
            key = " ".join(w for w in words if w not in PLACEHOLDERS)  # ען כלפה פעל is keyed as ען כלפה
        if key and key not in keys:
            keys.append(key)
    if not keys:
        flags.append("no_head_key")
    # A colon-less multiword head (``בצד עדרה ענד פלאן``) may be root + construction: the
    # first word is recorded as a candidate, never as the root.
    candidate = root_forms[0].split()[0] if root_source == "whole_head" and root_forms else None
    root_text = " ".join(before) if after else head_text.partition(":")[0]
    dotted = []
    for part in FORM_SPLIT_RE.split(" " + re.sub(r"\([^()]*\)", " ", root_text) + " "):
        key = dotted_key(strip_marks_but_dots(part))
        if key and key not in dotted and strip_marks(part).strip(" ()[]{}") in root_forms:
            dotted.append(key)
    head = {"text": head_text, "plain": re.sub(r"\s+", " ", plain).strip(), "root": " / ".join(root_forms) or None,
            "root_source": root_source, "root_candidate": candidate, "root_dotted": dotted,
            "lemma": " / ".join(lemma_forms) or None, "alternatives": lemma_forms, "frames": frames, "notes": notes,
            "keys": keys}
    return head, binyan, end, flags


def _zone(segments: list[list[str]]) -> dict:
    zone = {"definition": "", "translation": "", "locator": "", "foreign": "", "xrefs": [], "other": ""}
    for cls, text in segments:
        if cls == "ja":
            zone["definition"] += text
        elif cls == "he":
            zone["translation"] += text
        elif cls in ("loc", "shelf"):
            zone["locator"] += text
        elif cls == "foreign":
            zone["foreign"] += text
        elif cls == "head":
            zone["xrefs"].append(text.strip())
        elif cls not in ("bullet", "subentry", "binyan"):
            zone["other"] += text
    return {k: (re.sub(r"\s+", " ", v).strip() if isinstance(v, str) else v) for k, v in zone.items()}


def parse_body(segments: list[list[str]]) -> tuple[dict, list[dict], list[str]]:
    """Gloss zone before the first bullet; quotations split at bullets and closed locators."""
    zones: list[list[list[str]]] = [[]]
    quoting = False
    closed = False
    for cls, text in segments:
        if cls == "bullet":
            zones.append([])
            quoting, closed = True, False
            continue
        if quoting and closed and cls == "ja" and text.strip():
            zones.append([])
            closed = False
        if cls in ("loc", "shelf") and ")" in text:
            closed = True
        zones[-1].append([cls, text])
    gloss = _zone(zones[0])
    gloss["references"] = gloss.pop("translation")
    examples = []
    for zone in zones[1:]:
        example = _zone(zone)
        example["quote"] = example.pop("definition")
        examples.append(example)
    flags = []
    if any(not ex["locator"] for ex in examples):
        flags.append("quotation_without_locator")
    if not examples:
        flags.append("no_quotations")
    if gloss["xrefs"] or any(ex["xrefs"] for ex in examples):
        flags.append("bold_inside_body")
    if gloss["foreign"] or any(ex["foreign"] for ex in examples):
        flags.append("foreign_text")
    if gloss["other"] or any(ex["other"] for ex in examples):
        flags.append("other_font")
    return gloss, examples, sorted(flags)


XREF_PHRASE_RE = re.compile(r"(ראה|השווה|לפי השורש|הערך|הערכים|ערך המשנה|ערכי המשנה|בערך|ראה גם|וכן)\s*$")


def finish_unit(unit: dict) -> dict:
    segments = segments_of(unit.pop("glyphs"))
    head, binyan, body_start, flags = (parse_head(segments, unit["kind"]) if unit["kind"] != "unclassified"
                                       else (None, None, 0, []))
    if "no_head_key" in flags and unit["kind"] != "entry":
        flags[flags.index("no_head_key")] = "complement_only"  # e.g. ❖ (מנה) under its entry
    if "head_at_line_start" in unit["flags"]:
        # Bold that follows "ראה הערך" is a cross-reference the line wrap put first; keep the
        # stronger flag only when some line-start bold has no such phrase before it.
        bolds = [i for i in range(body_start, len(segments)) if segments[i][0] == "head"]
        if bolds and all(XREF_PHRASE_RE.search("".join(t for _, t in segments[max(0, i - 3):i]).strip()) for i in bolds):
            unit["flags"] = [f if f != "head_at_line_start" else "xref_at_line_start" for f in unit["flags"]]
    gloss, examples, body_flags = parse_body(segments[body_start:])
    unit.update({"head": head, "binyan": binyan, "gloss": gloss, "examples": examples,
                 "segments": segments, "text": re.sub(r"\s+", " ", "".join(t for _, t in segments)).strip()})
    unit["flags"] = sorted(set(unit["flags"]) | set(flags) | set(body_flags))
    return unit


def _head_words(unit: dict) -> set[str]:
    """First words of a unit's root and lemma keys: what a running head can show."""
    if not unit["head"]:
        return set()
    forms = unit["head"]["keys"] + [key_of(f) for f in (unit["head"]["root"] or "").split(" / ")]
    return {form.split()[0] for form in forms if form}


def recover(directory: Path, open_pdf) -> dict:
    """Walk the body sections in letter order and cut them into units."""
    manifest = checked_sources(directory)
    sources = {row["id"]: row for row in manifest["sources"]}
    units: list[dict] = []
    pages: list[dict] = []
    seen_labels: dict[str, int] = {}
    current: dict | None = None
    last_entry: dict | None = None
    for sid in BODY_IDS:
        with open_pdf(directory / "download" / f"{sid}.pdf") as doc:
            for pdf_page, page in enumerate(doc, 1):
                parsed = parse_page(page.get_text("rawdict", sort=False)["blocks"], page.rect.height)
                record = {"source_id": sid, "pdf_page": pdf_page, "printed_page": parsed["printed_page"],
                          "url": f"{VIEWER_BASE}{sid}.pdf#page={pdf_page}", "letter": SECTION_LETTERS[sid],
                          "running_head": parsed["running_head"], "letter_heading": parsed["letter_heading"],
                          "body_chars": parsed["body_chars"], "page_number_chars": parsed["page_number_chars"],
                          "hidden_chars": parsed["hidden_chars"], "duplicate_of": None, "units": []}
                label = parsed["printed_page"]
                if label in seen_labels:
                    record["duplicate_of"] = seen_labels[label]
                    pages.append(record)
                    continue
                if label is not None:
                    seen_labels[label] = len(pages)
                for index, paragraph in enumerate(parsed["paragraphs"]):
                    kind = OPENING.get(paragraph["first"])
                    location = {"source_id": sid, "pdf_page": pdf_page, "printed_page": label, "paragraph": index,
                                "bbox": [round(v, 1) for v in paragraph["bbox"]],
                                "chars": sum(g["cls"] != "space" for g in paragraph["glyphs"])}
                    flags = list(paragraph["flags"]) + (["unlabelled_page"] if label is None else [])
                    if kind is None and current is not None:
                        current["glyphs"].append(dict(paragraph["glyphs"][0], c=" ", cls="space"))
                        current["glyphs"].extend(paragraph["glyphs"])
                        current["locations"].append(location)
                        current["flags"] += flags + ["continued_across_page" if index == 0 else "split_paragraph"]
                        if current["id"] not in record["units"]:
                            record["units"].append(current["id"])
                        continue
                    if current is not None:
                        units.append(finish_unit(current))
                    current = {"id": f"f{len(units) + 1:06d}", "kind": kind or "unclassified", "letter": SECTION_LETTERS[sid],
                               "parent": None, "glyphs": list(paragraph["glyphs"]), "locations": [location],
                               "flags": flags + ([] if kind else ["no_opening_mark"])}
                    if kind == "entry":
                        head, binyan, _, _ = parse_head(segments_of(paragraph["glyphs"]))
                        if head is None:  # the only bold glyph is a bracket: "(IV/I) ..." is a binyan paragraph
                            kind = "binyan" if binyan else "unclassified"
                            current["kind"] = kind
                            current["flags"].append("bold_without_head")
                        elif head["plain"][0] in "-–":
                            current["kind"], kind = "subentry", "subentry"  # a bold complement (-ה) under its entry
                            current["flags"].append("complement_head")
                    if kind == "entry":
                        last_entry = current
                    elif kind in ("subentry", "binyan"):
                        if last_entry is None:
                            current["flags"].append("no_parent_entry")
                        else:
                            current["parent"] = last_entry["id"]
                    record["units"].append(current["id"])
                pages.append(record)
    if current is not None:
        units.append(finish_unit(current))
    by_id = {u["id"]: u for u in units}
    matches = 0
    for page in pages:
        words: set[str] = set()
        for uid in page["units"]:
            unit = by_id[uid]
            words |= _head_words(unit)
            if unit["parent"]:
                words |= _head_words(by_id[unit["parent"]])
        head = key_of((page["running_head"] or "").split("(")[0])
        page["running_head_matches"] = bool(head) and head.split()[0] in words
        matches += page["running_head_matches"]
    labels = sorted(int(lb) for lb in seen_labels)
    result = {"schema": UNITS_SCHEMA, "recovery_version": RECOVERY_VERSION, "normalizer_version": str(NORMALIZER_VERSION),
              "status": STATUS, "credit_he": CREDIT_HE, "built_at": datetime.now(timezone.utc).isoformat(),
              "manifest_sha256": sha256((directory / "source_manifest.json").read_bytes()),
              "sources": [{"id": sid, "sha256": sources[sid]["sha256"]} for sid in BODY_IDS],
              "printed_pages": {"first": labels[0] if labels else None, "last": labels[-1] if labels else None,
                                "count": len(labels), "missing": [n for n in range(labels[0], labels[-1] + 1) if n not in set(labels)] if labels else [],
                                "duplicates": sum(p["duplicate_of"] is not None for p in pages),
                                "unlabelled": sum(p["printed_page"] is None and p["duplicate_of"] is None for p in pages)},
              "running_head_matches": matches,
              "pages_with_running_head": sum(bool(p["running_head"]) and p["duplicate_of"] is None for p in pages),
              "pages": pages, "units": units}
    return result


def summarize(result: dict) -> dict:
    units = result["units"]
    flags = Counter(flag for u in units for flag in u["flags"])
    return {"schema": result["schema"], "recovery_version": result["recovery_version"], "status": result["status"],
            "built_at": result["built_at"], "units": len(units), "by_kind": dict(Counter(u["kind"] for u in units)),
            "by_letter": dict(Counter(u["letter"] for u in units if u["kind"] == "entry")),
            "with_binyan": sum(u["binyan"] is not None for u in units),
            "quotations": sum(len(u["examples"]) for u in units), "flags": dict(sorted(flags.items())),
            "pages": len(result["pages"]), "printed_pages": result["printed_pages"],
            "running_head_matches": result["running_head_matches"],
            "pages_with_running_head": result["pages_with_running_head"]}


def build(directory: Path) -> dict:
    import fitz

    result = recover(directory, fitz.open)
    candidate = directory / "candidate"
    write_json(candidate / "units.json", result)
    report = summarize(result)
    report["units_sha256"] = sha256((candidate / "units.json").read_bytes())
    write_json(candidate / "units_report.json", report)
    return report


def verify(directory: Path) -> dict:
    """Binding, page coverage, glyph conservation and key reproducibility; no philology."""
    manifest = checked_sources(directory)
    candidate = directory / "candidate"
    report = read_json(candidate / "units_report.json")
    data = (candidate / "units.json").read_bytes()
    if report.get("units_sha256") != sha256(data):
        raise ValueError("Units checksum mismatch")
    result = json.loads(data)
    if result["schema"] != UNITS_SCHEMA or result["recovery_version"] != RECOVERY_VERSION:
        raise ValueError("Units must be rebuilt with the current recovery version")
    if result["normalizer_version"] != str(NORMALIZER_VERSION):
        raise ValueError("Units must be rebuilt with the current normalizer")
    if result["manifest_sha256"] != sha256((directory / "source_manifest.json").read_bytes()):
        raise ValueError("Units are not bound to this source archive")
    sources = {row["id"]: row["sha256"] for row in manifest["sources"]}
    if [(s["id"], s["sha256"]) for s in result["sources"]] != [(sid, sources[sid]) for sid in BODY_IDS]:
        raise ValueError("Units are not bound to the archived PDFs")
    pages = result["pages"]
    labels = [int(p["printed_page"]) for p in pages if p["duplicate_of"] is None and p["printed_page"] is not None]
    if len(labels) != len(set(labels)) or (labels and set(labels) != set(range(min(labels), max(labels) + 1))):
        raise ValueError("Printed pages are not covered exactly once")
    for page in pages:
        if page["duplicate_of"] is not None and not (0 <= page["duplicate_of"] < len(pages)
                                                     and pages[page["duplicate_of"]]["printed_page"] == page["printed_page"]):
            raise ValueError("Duplicate page does not point at its original")
    units = result["units"]
    by_id: dict[str, dict] = {}
    counted: Counter = Counter()
    for order, unit in enumerate(units):
        if unit["id"] != f"f{order + 1:06d}" or unit["kind"] not in UNIT_KINDS:
            raise ValueError(f"Unit order or kind broken at {unit['id']}")
        if unit["parent"] is not None and (unit["parent"] not in by_id or by_id[unit["parent"]]["kind"] != "entry"):
            raise ValueError(f"Unit {unit['id']} has no preceding parent entry")
        if unit["kind"] == "entry" and unit["head"] is None:
            raise ValueError(f"Entry {unit['id']} lacks its head")
        if unit["head"] is not None:
            head, _, _, _ = parse_head([[cls, text] for cls, text in unit["segments"]], unit["kind"])
            if head is None or head["keys"] != unit["head"]["keys"]:
                raise ValueError(f"Head keys of {unit['id']} do not follow the current normalizer")
        if "".join(t for _, t in unit["segments"]).split() != unit["text"].split():
            raise ValueError(f"Unit text of {unit['id']} differs from its segments")
        for loc in unit["locations"]:
            counted[(loc["source_id"], loc["pdf_page"])] += loc["chars"]
        by_id[unit["id"]] = unit
    for page in pages:
        key = (page["source_id"], page["pdf_page"])
        expected = 0 if page["duplicate_of"] is not None else page["body_chars"]
        if counted.get(key, 0) != expected:
            raise ValueError(f"Glyphs lost or duplicated on {key}: {counted.get(key, 0)} in units, {expected} on the page")
        for uid in page["units"]:
            if uid not in by_id or not any((loc["source_id"], loc["pdf_page"]) == key for loc in by_id[uid]["locations"]):
                raise ValueError(f"Page {key} lists a unit that is not on it")
    for key in counted:
        if key not in {(p["source_id"], p["pdf_page"]) for p in pages}:
            raise ValueError(f"Unit on an unknown page: {key}")
    if summarize(result) != {k: v for k, v in report.items() if k != "units_sha256"}:
        raise ValueError("Report differs from the units")
    return {"verified": True, "units": len(units), "pages": len(pages), "printed_pages": result["printed_pages"]["count"],
            "status": result["status"]}


def show(directory: Path, query: str, limit: int = 10) -> list[dict]:
    result = read_json(directory / "candidate" / "units.json")
    key = key_of(query)
    found = []
    for unit in result["units"]:
        if unit["id"] == query or (key and unit["head"] and key in unit["head"]["keys"]):
            found.append({k: unit[k] for k in ("id", "kind", "letter", "parent", "head", "binyan", "gloss", "examples",
                                               "locations", "flags")})
            if len(found) >= limit:
                break
    return found


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("build", "verify", "show"))
    parser.add_argument("query", nargs="?")
    parser.add_argument("--directory", type=Path, default=ROOT / "friedman_data")
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args(argv)
    try:
        if args.command == "build":
            report = build(args.directory)
            print(f"Units: {report['units']} ({report['by_kind']}); {report['printed_pages']['count']} printed pages.")
        elif args.command == "verify":
            print(json.dumps(verify(args.directory), ensure_ascii=False, indent=2))
        else:
            if not args.query:
                parser.error("show requires a unit id or a word")
            print(json.dumps(show(args.directory, args.query, args.limit), ensure_ascii=False, indent=2))
        return 0
    except (OSError, ValueError, KeyError) as exc:
        print(f"Friedman unit recovery failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
