#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Gate 3 of the FGP source-folders plan: the unified text pipeline on a separate output DB.

WHY THIS EXISTS (docs/plans/FGP_SOURCE_FOLDERS_ASSESSMENT.md, section 8, gate 3)
-------------------------------------------------------------------------------
Today's ``fgp_transcriptions`` text was extracted from PDFs and loses the part of a line
before a gershayim, catchword lines, final letters and vocalised letters (left as control
characters), mirrors brackets, doubles backslashes and re-wraps lines. The delivered
folders hold the same editions as structured text: folder B's line text (B1 ``N.xml``, B2
``data.js`` ``ac.rows``) and the editable DOCX originals in the FIST file store. This gate
reads every row's text from the best source *its own FIST record links to*, diffs it line
by line against today's text, classifies every changed block into declared artefact
classes, and writes the result to ``gate3.db`` -- never into the sidecar. A row's text is
replaced only when every changed block falls in an accepted class (or an owner approval
names it); everything else is held and listed for review.

THE RULES, IN SHORT
-------------------
Readers, in order: the B folder named by the unit's ``ImageTranscriptionFolder`` (B1 XML
through the same parser today's XML rows came through; B2 ``ac.rows`` text), else the
DOCX whose basename is the unit's ``FileName`` in the unit's own FIST directory, else
today's text (PDF route; the delivered PDF is hashed against today's copy). Readers never
guess: no B folder by image number, no DOCX by 14-digit id.

Diff classes (``ACCEPTED_CLASSES``), each a PDF-extraction artefact, each with a declared
test: ``whitespace``, ``spacing``, ``escape`` (doubled backslashes), ``bracket`` (mirrored
brackets, direction-insensitive), ``numbering`` (a line number re-ordered by the RTL fix:
``.1`` / ``-1``), ``diacritics``, ``control_chars`` and ``final_letters`` (a glyph the PDF
font left as a control character or symbol), ``restored_prefix`` (text before a gershayim),
``catchword`` (a ``%...%`` line), ``regrouped`` (wrapped or merged lines),
``bidi_reorder`` (a mixed-direction line scrambled by the PDF), ``footnote_apart`` and
``header_apart`` (DOCX footnotes and headers the PDF printed inline). Anything else is
``unclassified`` and holds the row.

New rows (files FIST holds that today's sidecar does not) get their text and a DOCX
structure census (tables, footnotes, superscripts, numbering, hidden runs, tracked
changes) but no comparator; under ``--new-rows-policy hold`` (the default) none is
accepted. Descriptions are never read. XML rows never regress: a replaced XML row must
keep every line of today's text.

CHECKS
------
T01 two runs identical (``--previous-summary``) · T02 every replaced row has a FIST
source link · T03 every diff block has a disposition · T04 every replaced block is in an
accepted class or approved · T05 no new row accepted without a basis · T06 no XML row
regresses · T07 no description becomes an edition · T08 kinds from FIST · E_* declared
expectations.

OUTPUTS
-------
``<out-dir>/gate3.db`` (``text_rows``, ``unit_texts``, ``line_diffs``, ``checks``,
``counts``, ``gate3_meta``), ``gate3_summary.json``, ``GATE3_REPORT.md``. Read-only on
every input.

USAGE
-----
    powershell -File scripts/run_fgp_gate3_text_pipeline.ps1
    python scripts/fgp_gate3_text_pipeline.py --allow-single-run --expect-file scripts/fgp_gate3_text_pipeline.expect.json

Exit codes: 0 every check passed, 1 a check failed (fail-closed), 2 usage.
"""
from __future__ import annotations

import argparse
import collections
import difflib
import hashlib
import html
import json
import os
import re
import sqlite3
import sys
import time
import unicodedata
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from scripts.fgp_association_inventory import (  # noqa: E402
    DEFAULT_FGP_DB, DEFAULT_NOPOS, DEFAULT_OUT, DEFAULT_WITHPOS, NAME_SUFFIX_RE, parse_expectations, ro,
)
from scripts.fgp_rebuild_text_and_credit import parse_fgp_xml  # noqa: E402  (today's XML rows came through it)

try:
    import lxml.etree as _LX

    def _xml(data: bytes):
        return _LX.fromstring(data)

    _HAS_LXML = True
except ImportError:  # pragma: no cover - lxml is in requirements
    import xml.etree.ElementTree as _ET

    def _xml(data: bytes):
        return _ET.fromstring(data)

    _HAS_LXML = False

DEFAULT_GATE2 = os.path.join(DEFAULT_OUT, "gate2.db")
DEFAULT_TODAY_FILES = os.path.join(ROOT, "fgp_data", "transcriptions")
DEFAULT_APPROVALS = os.path.join(ROOT, "scripts", "fgp_gate3_approvals.json")

W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
MC = "{http://schemas.openxmlformats.org/markup-compatibility/2006}"
READERS =("b1_xml", "b2_rows", "docx", "pdf_today", "none")
ACCEPTED_CLASSES = frozenset((
    "whitespace", "spacing", "escape", "bracket", "numbering", "diacritics", "vowel_glyphs", "control_chars",
    "final_letters", "arabic_alef", "arabic_glyphs", "restored_prefix", "catchword", "column_label", "regrouped",
    "bidi_reorder", "footnote_apart", "header_apart",
))
STATUSES = ("identical", "accepted", "verified", "approved", "held", "no_comparator", "no_source", "retired")
CHAR_LEVEL_LIMIT = 60_000  # characters; a block beyond it is held rather than compared char by char

CTRL_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")
INVISIBLE_RE = re.compile(r"[﻿​‌‍‎‏‪-‮]")
MARKS_RE = re.compile(r"[̀-֑ͯ-ׇֽֿׁׂًׅׄ-ٰٟ]")
HEB_LETTER_RE = re.compile(r"[א-ת]")
ARABIC_LETTER_RE = re.compile(r"[ؠ-يٱ-ۓ]")
LETTER_RE = re.compile(r"[א-תؠ-يٱ-ۓ]")
# what the PDF font left where it could not render a letter or a vowel point: ASCII, control
# characters, Arabic presentation forms
JUNK_RE = re.compile(r"[\x00-\x7fﭐ-﷿ﹰ-﻿]")
MARK_SENTINEL = "\x00"
ASCII_VISIBLE_RE = re.compile(r"[!-~]")
NON_WORD_RE = re.compile(r"[\W_²³¹⁰-₟ᴀ-ᶿ]")  # + superscripts, modifier letters
FINALS = frozenset("ךםןףץ")
ARABIC_ALEF = "ا"
GERSHAYIM = ('"', "״")
SYMBOLS = frozenset("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")
BRACKET_TOKENS = str.maketrans("[]()<>{}", "[[((<<{{")
CATCHWORD_RE = re.compile(r"^%([^%]+)%(@?)$")
CATCHWORD_PDF_RE = re.compile(r"^<+([^<>]+)>+$")
COLUMN_LABEL_RE = re.compile(r"^טור_[א-ת]{1,2}\d*$")
NUMBER_PREFIX_RE = re.compile(r"^([.\-])(\d+)(?=\s|$)")
WS_RE = re.compile(r"\s+")
DIGITS_RE = re.compile(r"\d+")
DIGITS_ONLY_RE = re.compile(r"^\d{1,3}$")
ASCII_ALNUM_RE = re.compile(r"[0-9A-Za-z]")
FOOTNOTE_SEPARATOR = "\n\n"
# classes that verify an XML row's text as its own record's edition without replacing it: the
# DOCX keeps the typed sigla as characters (uncertainty ``?..?``, doubled brackets, the ``~``
# filler) where today's XML parse carried them as class codes, so letters-and-digits equality
# proves the edition and nothing is replaced.
VERIFY_ONLY_CLASSES = frozenset(("sigla_rendering", "extra_line"))


def log(msg: str) -> None:
    print(msg, flush=True)


def _j(v) -> str:
    return json.dumps(v, ensure_ascii=False, sort_keys=True)


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def file_sha(path: str) -> str | None:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


# --------------------------------------------------------------------------- normalisation


def ws(s: str) -> str:
    """Entities decoded, NBSP and runs of whitespace collapsed, BOM / direction marks dropped, ends stripped."""
    return WS_RE.sub(" ", INVISIBLE_RE.sub("", html.unescape(s or "")).replace("\xa0", " ")).strip()


def nfd(s: str) -> str:
    """Canonical decomposition: the PDF route emits precomposed letters with dagesh or a vowel
    (U+FB1D-FB4E) where the sources carry the letter and the combining mark."""
    return unicodedata.normalize("NFD", s)


def canon(s: str) -> str:
    """The loosest comparison form: letters and digits only (any script), control characters,
    marks, spaces and symbols gone, final letters dropped. Used for 'is this line still present'
    and for matching footnotes and headers the PDF printed inline, never to accept a replacement."""
    s = NON_WORD_RE.sub("", nfd(ws(s)))
    return "".join("" if c in FINALS else c for c in s)


def fix_number_prefix(line: str) -> str:
    """``.1 text`` / ``-1 text`` (the RTL fix moved the mark) -> ``1. text`` / ``1- text``."""
    return NUMBER_PREFIX_RE.sub(lambda m: m.group(2) + m.group(1), line)


def catchword_form(line: str, pdf_side: bool = False) -> str | None:
    """``%(word)%`` / ``%word%@`` (the DOCX and folder-B siglum) -> ``%word%`` (+ ``@``); on the PDF
    side also ``<word>`` / ``<<word>>``, which is how the PDF route rendered it. None otherwise."""
    m = CATCHWORD_RE.match(line)
    tail = ""
    if m:
        tail = m.group(2)
    elif pdf_side:
        m = CATCHWORD_PDF_RE.match(line)
    if not m:
        return None
    inner = m.group(1).strip().strip("()").strip()
    return f"%{inner.replace(' ', '')}%{tail}" if inner else None


# --------------------------------------------------------------------------- DOCX reader


class DocxText:
    __slots__ = ("lines", "footnotes", "headers", "structure")

    def __init__(self):
        self.lines: list = []          # body lines, in document order (table cells one per line)
        self.footnotes: list = []      # (n, text) in reference order
        self.headers: list = []        # (canon_text, has_page_field) from header/footer parts
        self.structure: dict = collections.Counter()


class _State:
    __slots__ = ("st", "fn_order", "in_instr", "instr", "skip_result", "numbering", "counters", "style_num",
                 "hidden_styles")

    def __init__(self, st):
        self.st = st
        self.fn_order = {}
        self.in_instr = False
        self.instr = ""
        self.skip_result = False
        self.numbering = {}
        self.counters = {}
        self.style_num = {}
        self.hidden_styles = frozenset()


def _attr(node, name):
    return node.get(W + name)


# a paragraph with any of these takes the full recursive walk; the rest take the fast path
_SLOW_TAGS = tuple(W + t for t in (
    "del", "moveFrom", "ins", "vanish", "tab", "br", "cr", "noBreakHyphen", "footnoteReference", "endnoteReference",
    "instrText", "delText", "fldChar", "fldSimple", "drawing", "pict", "object",
))


def _collect(node, out: list, s: _State) -> None:
    tag = node.tag
    if not isinstance(tag, str):
        return
    if tag in (W + "del", W + "moveFrom"):
        s.st["tracked_changes"] += 1
        return
    if tag == W + "ins":
        s.st["tracked_changes"] += 1
    elif tag == W + "r":
        rpr = node.find(W + "rPr")
        if rpr is not None:
            v = rpr.find(W + "vanish")
            if v is not None and (_attr(v, "val") or "true") not in ("0", "false"):
                s.st["hidden_runs"] += 1
                return
            rs = rpr.find(W + "rStyle")
            if rs is not None and _attr(rs, "val") in s.hidden_styles:
                s.st["hidden_runs"] += 1
                return
            if rpr.find(W + "b") is not None:
                s.st["bold_runs"] += 1
            va = rpr.find(W + "vertAlign")
            if va is not None and _attr(va, "val") == "superscript":
                s.st["superscript_runs"] += 1
    elif tag == W + "t":
        if not s.in_instr and not s.skip_result:
            out.append(node.text or "")
        return
    elif tag == W + "tab":
        if not s.skip_result:
            out.append(" ")
        s.st["tabs"] += 1
        return
    elif tag in (W + "br", W + "cr"):
        out.append("\n")
        s.st["breaks"] += 1
        return
    elif tag == W + "noBreakHyphen":
        out.append("-")
        return
    elif tag == W + "footnoteReference":
        fid = _attr(node, "id")
        if fid not in s.fn_order:
            s.fn_order[fid] = len(s.fn_order) + 1
        out.append(str(s.fn_order[fid]))
        s.st["footnote_refs"] += 1
        return
    elif tag == W + "endnoteReference":
        s.st["endnote_refs"] += 1
        return
    elif tag in (W + "instrText", W + "delText"):
        if tag == W + "instrText":
            s.instr += node.text or ""
        return
    elif tag == W + "fldChar":
        t = _attr(node, "fldCharType")
        if t == "begin":
            s.in_instr, s.instr = True, ""
        elif t == "separate":
            s.in_instr = False
            s.skip_result = "PAGE" in s.instr.upper()
        elif t == "end":
            s.in_instr, s.skip_result = False, False
        return
    elif tag == W + "fldSimple":
        if "PAGE" in (_attr(node, "instr") or "").upper():
            return
    elif tag in (W + "drawing", W + "pict", W + "object"):
        s.st["images"] += 1
        return
    elif tag in (W + "pPr", W + "rPr", W + "tblPr", W + "trPr", W + "tcPr", W + "sectPr"):
        return
    for ch in node:
        _collect(ch, out, s)


def _load_numbering(z: zipfile.ZipFile) -> dict:
    """{numId: {ilvl: (numFmt, lvlText, start)}} from word/numbering.xml, or {}."""
    if "word/numbering.xml" not in z.namelist():
        return {}
    root = _xml(z.read("word/numbering.xml"))
    abstract = {}
    for an in root.iter(W + "abstractNum"):
        lvls = {}
        for lvl in an.findall(W + "lvl"):
            fmt = lvl.find(W + "numFmt")
            txt = lvl.find(W + "lvlText")
            start = lvl.find(W + "start")
            lvls[int(_attr(lvl, "ilvl") or 0)] = (
                _attr(fmt, "val") if fmt is not None else "decimal",
                _attr(txt, "val") if txt is not None else "%1.",
                int(_attr(start, "val") or 1) if start is not None else 1,
            )
        abstract[_attr(an, "abstractNumId")] = lvls
    nums = {}
    for num in root.iter(W + "num"):
        ref = num.find(W + "abstractNumId")
        lvls = dict(abstract.get(_attr(ref, "val") if ref is not None else None, {}))
        for ov in num.findall(W + "lvlOverride"):
            so = ov.find(W + "startOverride")
            il = int(_attr(ov, "ilvl") or 0)
            if so is not None and il in lvls:
                lvls[il] = (lvls[il][0], lvls[il][1], int(_attr(so, "val") or 1))
        nums[_attr(num, "numId")] = lvls
    return nums


def _load_styles(z: zipfile.ZipFile) -> tuple:
    """({styleId: (numId, ilvl)} for paragraph styles that carry numbering, {hidden style ids});
    one basedOn hop for both."""
    if "word/styles.xml" not in z.namelist():
        return {}, frozenset()
    root = _xml(z.read("word/styles.xml"))
    own, based, hidden = {}, {}, set()
    for st in root.iter(W + "style"):
        sid = _attr(st, "styleId")
        b = st.find(W + "basedOn")
        if b is not None:
            based[sid] = _attr(b, "val")
        rpr = st.find(W + "rPr")
        v = rpr.find(W + "vanish") if rpr is not None else None
        if v is not None and (_attr(v, "val") or "true") not in ("0", "false"):
            hidden.add(sid)
        if _attr(st, "type") != "paragraph":
            continue
        ppr = st.find(W + "pPr")
        npr = ppr.find(W + "numPr") if ppr is not None else None
        if npr is not None:
            nid, il = npr.find(W + "numId"), npr.find(W + "ilvl")
            if nid is not None:
                own[sid] = (_attr(nid, "val"), int(_attr(il, "val") or 0) if il is not None else 0)
    for sid, parent in based.items():
        if sid not in own and parent in own:
            own[sid] = own[parent]
        if parent in hidden:
            hidden.add(sid)
    return own, frozenset(hidden)


_HEB_ONES = "אבגדהוזחט"
_HEB_TENS = "יכלמנסעפצ"
_HEB_HUNDREDS = "קרשת"


def _hebrew1(n: int) -> str:
    if n <= 0:
        return str(n)
    out = ""
    while n >= 400:
        out += "ת"
        n -= 400
    if n >= 100:
        out += _HEB_HUNDREDS[n // 100 - 1]
        n %= 100
    if n in (15, 16):
        return out + ("טו" if n == 15 else "טז")
    if n >= 10:
        out += _HEB_TENS[n // 10 - 1]
        n %= 10
    if n:
        out += _HEB_ONES[n - 1]
    return out


def _roman(n: int) -> str:
    vals = ((1000, "m"), (900, "cm"), (500, "d"), (400, "cd"), (100, "c"), (90, "xc"), (50, "l"), (40, "xl"),
            (10, "x"), (9, "ix"), (5, "v"), (4, "iv"), (1, "i"))
    out = ""
    for v, s in vals:
        while n >= v:
            out += s
            n -= v
    return out


def _format_number(fmt: str, n: int, st) -> str:
    if fmt == "decimal":
        return str(n)
    if fmt == "decimalZero":
        return f"{n:02d}"
    if fmt == "lowerLetter":
        return chr(ord("a") + (n - 1) % 26) * ((n - 1) // 26 + 1)
    if fmt == "upperLetter":
        return chr(ord("A") + (n - 1) % 26) * ((n - 1) // 26 + 1)
    if fmt == "lowerRoman":
        return _roman(n)
    if fmt == "upperRoman":
        return _roman(n).upper()
    if fmt == "hebrew1":
        return _hebrew1(n)
    if fmt == "hebrew2":
        letters = _HEB_ONES + _HEB_TENS + _HEB_HUNDREDS
        return letters[(n - 1) % len(letters)] * ((n - 1) // len(letters) + 1)
    if fmt in ("bullet", "none"):
        return ""
    st["numbering_unknown_format"] += 1
    return str(n)


def _render_numbering(p, s: _State) -> str:
    ppr = p.find(W + "pPr")
    num_id, ilvl = None, 0
    if ppr is not None:
        npr = ppr.find(W + "numPr")
        if npr is not None:
            nid, il = npr.find(W + "numId"), npr.find(W + "ilvl")
            if nid is not None:
                num_id = _attr(nid, "val")
                ilvl = int(_attr(il, "val") or 0) if il is not None else 0
        if num_id is None:
            ps = ppr.find(W + "pStyle")
            if ps is not None and _attr(ps, "val") in s.style_num:
                num_id, ilvl = s.style_num[_attr(ps, "val")]
    if num_id in (None, "0"):
        return ""
    lvls = s.numbering.get(num_id)
    if not lvls or ilvl not in lvls:
        s.st["numbering_unrendered"] += 1
        return ""
    counters = s.counters.setdefault(num_id, {})
    counters[ilvl] = counters.get(ilvl, lvls[ilvl][2] - 1) + 1
    for deeper in [k for k in counters if k > ilvl]:
        del counters[deeper]
    fmt, text, _start = lvls[ilvl]
    s.st["numbered_paragraphs"] += 1

    def sub(m):
        k = int(m.group(1)) - 1
        n = counters.get(k, lvls.get(k, ("decimal", "", 1))[2])
        return _format_number(lvls.get(k, (fmt,))[0], n, s.st)

    rendered = re.sub(r"%(\d)", sub, text)
    return rendered + " " if rendered else ""


def _part_paragraph_texts(z: zipfile.ZipFile, name: str, s: _State) -> list:
    root = _xml(z.read(name))
    out = []
    for p in root.iter(W + "p"):
        parts = []
        _collect(p, parts, s)
        out.append("".join(parts))
    return out


def read_docx(path: str) -> DocxText:
    """Structure-aware text of a DOCX: paragraphs (and table cells) one per line, manual breaks as
    lines, tracked deletions and hidden runs dropped, auto-numbering rendered, footnote references
    numbered in order and the footnotes returned apart, header/footer text returned apart."""
    d = DocxText()
    s = _State(d.structure)
    with zipfile.ZipFile(path) as z:
        names = set(z.namelist())
        s.numbering = _load_numbering(z)
        s.style_num, s.hidden_styles = _load_styles(z)
        root = _xml(z.read("word/document.xml"))
        body = root.find(W + "body")
        if body is None:
            return d
        has_tables = next(body.iter(W + "tbl"), None) is not None
        d.structure["tables"] = sum(1 for _ in body.iter(W + "tbl"))
        has_alternate = _HAS_LXML and next(body.iter(MC + "AlternateContent"), None) is not None

        def inside_table(el) -> bool:  # lxml proxies have no stable identity: ask the ancestors
            return has_tables and any(a.tag == W + "tbl" for a in el.iterancestors())

        def para_text(p) -> str | None:
            ppr = p.find(W + "pPr")
            ps = ppr.find(W + "pStyle") if ppr is not None else None
            if ps is not None and _attr(ps, "val") in s.hidden_styles:
                d.structure["hidden_paragraphs"] += 1
                return None
            if has_alternate and any(a.tag == MC + "Fallback" for a in p.iterancestors()):
                return None  # the same text box twice: keep the Choice rendering only
            parts = [_render_numbering(p, s)]
            if _HAS_LXML and next(p.iter(*_SLOW_TAGS), None) is None and \
                    (not s.hidden_styles or next(p.iter(W + "rStyle"), None) is None):
                # the common paragraph: plain runs only; count the run properties, join the text
                parts.extend(t.text or "" for t in p.iter(W + "t"))
                for rpr in p.iter(W + "rPr"):
                    if rpr.find(W + "b") is not None:
                        s.st["bold_runs"] += 1
                    va = rpr.find(W + "vertAlign")
                    if va is not None and _attr(va, "val") == "superscript":
                        s.st["superscript_runs"] += 1
            else:
                _collect(p, parts, s)
            d.structure["paragraphs"] += 1
            return "".join(parts)

        def add_lines(text: str) -> None:
            for ln in text.split("\n"):
                if COLUMN_LABEL_RE.match(ws(ln)):
                    # a column label (``טור_א``) is structure, not text: a blank line separates columns
                    d.structure["column_labels"] += 1
                    d.lines.append("")
                    continue
                d.lines.append(ln)

        for el in body.iter(W + "p", W + "tbl") if _HAS_LXML else body.iter(W + "p"):
            if el.tag == W + "tbl":
                if inside_table(el):
                    continue  # a nested table is part of its cell's text
                # a table row is one line: its cells in document order, each cell's paragraphs joined
                # (the PDF printed ``01 text`` for a number | text row; the order is a bidi matter)
                for tr in el.iter(W + "tr"):
                    cells = []
                    for tc in tr.findall(W + "tc"):
                        texts = [t for t in (para_text(p) for p in tc.iter(W + "p")) if t is not None]
                        cell = " ".join(ws(t.replace("\n", " ")) for t in texts if ws(t))
                        if cell:
                            cells.append(cell)
                    d.structure["table_rows"] += 1
                    add_lines(" ".join(cells))
                continue
            if _HAS_LXML and inside_table(el):
                continue
            text = para_text(el)
            if text is not None:
                add_lines(text)
        if "word/footnotes.xml" in names:
            fx = _xml(z.read("word/footnotes.xml"))
            texts = {}
            for fn in fx.iter(W + "footnote"):
                if _attr(fn, "type") in ("separator", "continuationSeparator"):
                    continue
                paras = []
                for p in fn.iter(W + "p"):
                    parts = []
                    _collect(p, parts, s)
                    paras.append("".join(parts).replace("\n", " "))
                t = ws(" ".join(paras))
                if t:
                    texts[_attr(fn, "id")] = t
            for fid, n in sorted(s.fn_order.items(), key=lambda kv: kv[1]):
                if fid in texts:
                    d.footnotes.append((n, texts[fid]))
            d.structure["footnotes"] = len(d.footnotes)
        for name in sorted(n for n in names if re.match(r"word/(header|footer)\d*\.xml$", n)):
            hs = _State(collections.Counter())
            raw = z.read(name).decode("utf-8", "replace")
            has_page = "PAGE" in raw.upper()
            for t in _part_paragraph_texts(z, name, hs):
                c = canon(t)
                if c:
                    d.headers.append((c, has_page))
            if d.headers:
                d.structure["header_parts"] += 1
    return d


def docx_text(d: DocxText, with_footnotes: bool = True) -> str:
    """The published form: body lines (columns separated by one blank line), a blank line, then
    the footnotes as ``<n> <text>``."""
    out, blank = [], False
    for ln in d.lines:
        t = ws(ln)
        if t:
            out.append(t)
            blank = False
        elif out and not blank:
            out.append("")
            blank = True
    while out and out[-1] == "":
        out.pop()
    body = "\n".join(out)
    if d.footnotes and with_footnotes:
        body += FOOTNOTE_SEPARATOR + "\n".join(f"{n} {t}" for n, t in d.footnotes)
    return body


# --------------------------------------------------------------------------- folder-B readers


def read_b1(dir_path: str) -> tuple:
    xmls = sorted(f for f in os.listdir(dir_path) if f.lower().endswith(".xml"))
    if not xmls:
        return None, None
    path = os.path.join(dir_path, xmls[0])
    return parse_fgp_xml(path), path


def read_b2(dir_path: str) -> tuple:
    path = os.path.join(dir_path, "data.js")
    if not os.path.isfile(path):
        return None, None
    with open(path, encoding="utf-8", errors="replace") as fh:
        s = fh.read()
    eq = s.find("=")
    try:
        j = json.loads(s[eq + 1:].strip().rstrip(";"))
    except (ValueError, IndexError):
        return None, path
    cols = []
    for col in j.get("ac") or []:
        rows = col.get("rows") if isinstance(col, dict) else None
        if not rows:
            continue
        lines = []
        for k in sorted(rows, key=lambda x: int(x) if str(x).isdigit() else 0):
            line = ws(rows[k].get("txt", "") if isinstance(rows[k], dict) else "")
            if line:
                lines.append(line)
        if lines:
            cols.append("\n".join(lines))
    return "\n\n".join(cols).strip(), path


# --------------------------------------------------------------------------- diff and classes


def _lines(text: str) -> list:
    return [ws(x) for x in (text or "").replace("\r", "").split("\n") if ws(x)]


def _find_run(canon_lines: list, target: str, used: set) -> tuple | None:
    """A maximal run of consecutive lines (not yet used) whose concatenation equals target; a
    single line also matches as a character multiset (the PDF scrambles mixed-direction lines)."""
    n = len(canon_lines)
    target_sorted = None
    for i in range(n):
        if i in used or not canon_lines[i]:
            continue
        if target.startswith(canon_lines[i]):
            acc, j = "", i
            while j < n and j not in used and len(acc) < len(target):
                acc += canon_lines[j]
                j += 1
            if acc == target:
                return i, j
        if len(canon_lines[i]) == len(target):
            if target_sorted is None:
                target_sorted = sorted(target)
            if sorted(canon_lines[i]) == target_sorted:
                return i, i + 1
    return None


def strip_apart(today: list, footnotes: list, headers: list) -> tuple:
    """Remove from today's lines the DOCX footnotes and header/footer text the PDF printed inline,
    and the column labels the DOCX carries as lines. Returns (remaining lines, classes)."""
    classes = collections.Counter()
    cl = [canon(x) for x in today]
    used = set()
    for n, text in footnotes:
        for target in (canon(f"{n}{text}"), canon(text)):
            if not target:
                continue
            hit = _find_run(cl, target, used)
            if hit:
                used.update(range(hit[0], hit[1]))
                classes["footnote_apart"] += hit[1] - hit[0]
                break
    for i, c in enumerate(cl):
        if i in used:
            continue
        if COLUMN_LABEL_RE.match(today[i]):
            used.add(i)
            classes["column_label"] += 1
            continue
        if not c:
            continue
        for hc, has_page in headers:
            if c == hc or (has_page and DIGITS_RE.sub("", c) == DIGITS_RE.sub("", hc) and DIGITS_RE.sub("", c)):
                used.add(i)
                classes["header_apart"] += 1
                break
    return [x for i, x in enumerate(today) if i not in used], classes


def _trim_common(a: str, b: str) -> tuple:
    """(a_mid, b_mid, next_char_of_b) after dropping the common prefix and suffix."""
    i = 0
    n = min(len(a), len(b))
    while i < n and a[i] == b[i]:
        i += 1
    j = 0
    while j < n - i and a[-1 - j] == b[-1 - j]:
        j += 1
    return a[i:len(a) - j], b[i:len(b) - j], b[len(b) - j:len(b) - j + 1]


def _char_level(a: str, b: str) -> tuple:
    """Opcode-level tolerance between today's (a) and the new (b) string: control characters and
    symbols standing for letters, final letters and Arabic alef the PDF dropped, Arabic the PDF
    reduced to junk, a restored gershayim prefix. When b carries MARK_SENTINEL where the source
    has a vowel point (the second attempt), an ASCII glyph of today's standing in its place is a
    ``vowel_glyphs`` difference and a sentinel today lacks is ``diacritics``. Returns (classes, ok)."""
    classes = set()
    a, b, after = _trim_common(a, b)
    if not a and not b:
        return classes, True
    if len(a) + len(b) > CHAR_LEVEL_LIMIT:
        return classes, False
    sm = difflib.SequenceMatcher(a=a, b=b, autojunk=False)
    for op, i1, i2, j1, j2 in sm.get_opcodes():
        if op == "equal":
            continue
        ta, nb_raw = a[i1:i2], b[j1:j2]
        nb = nb_raw.replace(MARK_SENTINEL, "")
        had_mark = len(nb) != len(nb_raw)
        if had_mark:
            classes.add("diacritics")
        if op == "replace":
            if not nb and had_mark and ta and all(JUNK_RE.match(c) for c in ta) and len(ta) <= 2:
                classes.add("vowel_glyphs")
                continue
            if had_mark and len(nb) == 1 and HEB_LETTER_RE.match(nb) and len(ta) == 1 and JUNK_RE.match(ta):
                classes.add("vowel_glyphs")  # a vocalised letter the font rendered as a symbol (``מֵ<ה`` for ``מֵאָה``)
                continue
            if ta and all(CTRL_RE.match(c) for c in ta) and nb and all(LETTER_RE.match(c) for c in nb):
                classes.add("control_chars")
                continue
            if ta and all(CTRL_RE.match(c) or c in SYMBOLS for c in ta) and nb and all(c in FINALS for c in nb):
                classes.add("final_letters")
                continue
            if ta and all(JUNK_RE.match(c) for c in ta) and nb and all(ARABIC_LETTER_RE.match(c) for c in nb) \
                    and not ARABIC_LETTER_RE.search(a):
                classes.add("arabic_glyphs")
                continue
            return classes, False
        if op == "delete":
            if all(CTRL_RE.match(c) for c in ta):
                classes.add("control_chars")
                continue
            return classes, False
        if op == "insert":
            if not nb:
                continue  # marks the source has and today's text lacks: diacritics
            if all(c in FINALS for c in nb):
                classes.add("final_letters")
                continue
            if had_mark and len(nb) == 1 and HEB_LETTER_RE.match(nb):
                classes.add("vowel_glyphs")  # a vocalised letter the font dropped (``כּ חִים`` for ``כְּאַחִים``)
                continue
            if all(c == ARABIC_ALEF for c in nb):
                classes.add("arabic_alef")
                continue
            nxt = (b[j2:j2 + 1] or after).replace(MARK_SENTINEL, "")
            if HEB_LETTER_RE.search(nb) and (nb[-1] in GERSHAYIM or nxt in GERSHAYIM):
                classes.add("restored_prefix")
                continue
            return classes, False
    return classes, True


def _reduced(s: str) -> tuple:
    """(letters and digits with final letters dropped, symbols, finals count) for the bidi fallback."""
    s = MARKS_RE.sub("", nfd(s))
    letters = "".join(c for c in s if not NON_WORD_RE.match(c) and c not in FINALS)
    symbols = "".join(c for c in s if c in SYMBOLS)
    finals = sum(1 for c in s if c in FINALS)
    return letters, symbols, finals


def _bidi_equal(a: str, b: str) -> set | None:
    """A mixed-direction line the PDF scrambled: the same letters and digits in another order;
    symbols either the same multiset or fewer by exactly the final letters they stood for. A line
    of symbols only (a lacuna with its marks) matches as a symbol multiset."""
    if not ASCII_VISIBLE_RE.search(a) and HEB_LETTER_RE.search(a):
        return None
    la, sa, fa = _reduced(CTRL_RE.sub("", a))
    lb, sb, fb = _reduced(b)
    if sorted(la) != sorted(lb):
        return None
    if not la and not lb:
        return {"bidi_reorder"} if sa and sorted(sa) == sorted(sb) else None
    if sorted(sa) == sorted(sb):
        return {"bidi_reorder"}
    if len(sa) - len(sb) == fb - fa > 0:
        return {"bidi_reorder", "final_letters"}
    return None


def _restored_prefix(a: str, b: str) -> bool:
    """Today's string is what the new string keeps after a gershayim: the PDF route lost the part
    of the line before the mark (``ועד י"ג שנים`` -> ``ג שנים``)."""
    if not a or len(b) <= len(a) or not b.endswith(a):
        return False
    prefix = b[:len(b) - len(a)].rstrip()
    return bool(prefix) and prefix[-1] in GERSHAYIM and bool(HEB_LETTER_RE.search(prefix))


_STAGES = (
    ("spacing", lambda s: s.replace(" ", "")),
    ("escape", lambda s: s.replace("\\", "")),
    ("bracket", lambda s: s.translate(BRACKET_TOKENS)),
)


def _classify_strings(a: str, b: str, cls: set) -> tuple:
    """Today's string a against the new string b through the stages, then the character-level
    tolerance, the bidi fallback and (verify-only) letters-and-digits equality."""
    if a == b:
        return cls, True
    for name, f in _STAGES:
        a2, b2 = f(a), f(b)
        if (a2, b2) != (a, b):
            cls.add(name)
        a, b = a2, b2
        if a == b:
            return cls, True
    a_pre, b_pre = nfd(a), nfd(b)
    a, b = MARKS_RE.sub("", a_pre), MARKS_RE.sub("", b_pre)
    if (a, b) != (a_pre, b_pre):
        cls.add("diacritics")
    if a == b:
        return cls, True
    if _restored_prefix(a, b):
        return cls | {"restored_prefix"}, True
    sub, ok = _char_level(a, b)
    if ok:
        return cls | sub, True
    # second attempt: the source's vowel points as sentinels, so an ASCII glyph the PDF font left
    # in a point's place (``ש0ב`` for ``שֵב``) is seen as standing for that point
    if MARKS_RE.search(b_pre):
        sub2, ok = _char_level(a, MARKS_RE.sub(MARK_SENTINEL, b_pre))
        if ok:
            return cls | sub2, True
    bidi = _bidi_equal(a, b)
    if bidi:
        return cls | bidi, True
    la = canon(a)
    if la and la == canon(b):
        return cls | sub | {"sigla_rendering"}, True
    return cls | sub | {"unclassified"}, False


def line_key(line: str, pdf_side: bool) -> str:
    """The alignment key of a line: what is left once the cheap artefacts are normalised away
    (catchword form, line-number order, spaces, backslashes, bracket direction, marks, control
    characters, final letters). Lines with equal keys are aligned and then classified pairwise."""
    cw = catchword_form(line, pdf_side)
    if cw:
        return cw
    s = fix_number_prefix(line) if pdf_side else line
    s = nfd(s.replace(" ", "").replace("\\", "").translate(BRACKET_TOKENS))
    s = MARKS_RE.sub("", CTRL_RE.sub("", s))
    return "".join(c for c in s if c not in FINALS)


def classify_pair(t: str, n: str) -> tuple:
    """Classes of two aligned lines (same key) whose raw text differs."""
    cls = set()
    cw_t, cw_n = catchword_form(t, pdf_side=True), catchword_form(n)
    if cw_t and cw_n:
        if cw_t != t:
            cls.add("catchword")
        return cls, True
    t2 = fix_number_prefix(t)
    if t2 != t:
        cls.add("numbering")
    return _classify_strings(t2, n, cls)


def classify_block(T: list, N: list) -> tuple:
    """Classes of one changed block (today's lines T vs new lines N, ws-normalised) and whether
    every difference is an accepted artefact. Blocks with as many lines on both sides are
    classified line by line so one bad line does not hide the others' classes."""
    cls = set()
    N2, T2 = [], []
    for x in T:
        cw = catchword_form(x, pdf_side=True)
        if cw and cw != x:
            cls.add("catchword")
        T2.append(cw or x)
    for x in N:
        cw = catchword_form(x)
        if cw and cw not in T2:
            cls.add("catchword")  # a catchword line the PDF route dropped: an accepted insert
            continue
        N2.append(cw or x)
    if T2 == N2:
        return cls, True
    if not T2 and N2:
        # lines the source has and today's text lacks (a transcriber's note, a line the PDF
        # dropped): they do not disprove the edition, and they never replace a text unread
        return cls | {"extra_line"}, True
    if T2 and not N2 and all(DIGITS_ONLY_RE.match(x) for x in T2):
        # a footnote marker or line number the PDF printed on a line of its own
        return cls | {"numbering"}, True
    T3 = [fix_number_prefix(x) for x in T2]
    if T3 != T2:
        cls.add("numbering")
    if T3 == N2:
        return cls, True
    if len(T3) == len(N2):
        pair_cls, ok_all = set(), True
        for ta, nb in zip(T3, N2):
            sub, ok = _classify_strings(ta, nb, set())
            pair_cls |= sub
            ok_all = ok_all and ok
        if ok_all:
            return cls | pair_cls, True
        # a word moved across a line boundary: the lines differ, the block does not
        joined_cls, ok = _classify_strings(" ".join(T3), " ".join(N2), {"regrouped"})
        if ok:
            return cls | joined_cls, True
        return cls | pair_cls, False
    cls.add("regrouped")
    return _classify_strings(" ".join(T3), " ".join(N2), cls)


def diff_texts(today_text: str, body_text: str, full_text: str, footnotes: list, headers: list) -> dict:
    """Line diff of today's text against the new body text; the DOCX footnotes and headers the PDF
    printed inline are matched apart; 'lost' lines are checked against the full published text."""
    today = _lines(today_text)
    new = _lines(body_text)
    today, apart = strip_apart(today, footnotes, headers)
    sm = difflib.SequenceMatcher(a=[line_key(x, True) for x in today], b=[line_key(x, False) for x in new], autojunk=False)
    blocks, classes, held, verify_only, lost = [], collections.Counter(apart), 0, 0, 0
    for op, i1, i2, j1, j2 in sm.get_opcodes():
        if op == "equal":
            # aligned by key: the raw lines may still differ by the cheap artefacts
            for k in range(i2 - i1):
                t, n = today[i1 + k], new[j1 + k]
                if t == n:
                    continue
                cls, ok = classify_pair(t, n)
                if not ok:
                    held += 1
                    disp = "held"
                elif cls & VERIFY_ONLY_CLASSES:
                    disp = "verify_only"
                    verify_only += 1
                else:
                    disp = "accepted"
                for c in cls:
                    classes[c] += 1
                blocks.append({"op": "normalize", "today_from": i1 + k, "today_to": i1 + k + 1, "new_from": j1 + k,
                               "new_to": j1 + k + 1, "today_text": t, "new_text": n, "classes": sorted(cls),
                               "disposition": disp})
            continue
        T, N = today[i1:i2], new[j1:j2]
        cls, ok = classify_block(T, N)
        if not ok:
            held += 1
            disp = "held"
        elif cls & VERIFY_ONLY_CLASSES:
            disp = "verify_only"
            verify_only += 1
        else:
            disp = "accepted"
        for c in cls:
            classes[c] += 1
        # content loss: letters of today's side that the new side of the block does not have at all
        # (a dropped line); an order change, a number or a restored prefix lose nothing, and the
        # ASCII a glyph class attests to be the font's junk is not content
        ct = canon(" ".join(T))
        if cls & {"vowel_glyphs", "arabic_glyphs", "numbering"}:
            ct = ASCII_ALNUM_RE.sub("", ct)  # a line number or footnote marker is not content either
        if T and (collections.Counter(ct) - collections.Counter(canon(" ".join(N)))):
            lost += len(T)
        blocks.append({"op": op, "today_from": i1, "today_to": i2, "new_from": j1, "new_to": j2,
                       "today_text": "\n".join(T), "new_text": "\n".join(N), "classes": sorted(cls),
                       "disposition": disp})
    if not blocks and today_text.strip() != full_text.strip():
        classes["whitespace"] += 1
    return {"blocks": blocks, "classes": classes, "held_blocks": held, "verify_only_blocks": verify_only,
            "today_lines": len(today), "new_lines": len(new), "lost_today_lines": lost}


# --------------------------------------------------------------------------- loading


def load_inputs(inventory: str, gate2: str, fgp_db: str) -> dict:
    inv = ro(inventory)
    files = {}
    for b, d, hd, hp, kind, status, dnd in inv.execute(
        "SELECT basename, dir, has_docx, has_pdf, kind, status, double_not_display FROM files"
    ):
        files[b] = {"dir": d, "has_docx": hd, "has_pdf": hp, "kind": kind, "status": status, "held": dnd}
    units = {}
    for b, ut, uid, kind, itf, itfx, fp, dnd in inv.execute(
        "SELECT basename, unit_table, unit_id, kind, image_transcription_folder, itf_exists, folder_path, "
        "double_not_display FROM file_units"
    ):
        units[(ut, uid)] = {"basename": b, "table": ut, "unit_id": uid, "kind": kind, "itf": itf,
                            "itf_exists": itfx == 1, "folder": (fp or "").strip("\\/"), "held": dnd == 1}
    b_formats = {d: f for d, f in inv.execute("SELECT dir, format FROM b_dirs")}
    almas = collections.defaultdict(set)
    for b, alma in inv.execute("SELECT basename, alma_id FROM file_almas WHERE alma_id IS NOT NULL"):
        almas[b].add(str(alma))
    inv.close()
    g2 = ro(gate2)
    cols = ("id", "basename", "filename", "kind", "language", "sys_id", "c_number", "text_source", "unit_table",
            "unit_id", "unit_kind", "outcome", "publishable", "hold_reasons", "text_status", "durable_key")
    rows = [dict(zip(cols, r)) for r in g2.execute(f"SELECT {', '.join(cols)} FROM row_outcomes ORDER BY id")]
    g2.close()
    fgp = ro(fgp_db)
    today = {}
    for rid, content, rel in fgp.execute("SELECT id, content, rel_path FROM fgp_transcriptions"):
        today[rid] = (content or "", rel)
    fgp.close()
    return {"files": files, "units": units, "b_formats": b_formats, "almas": almas, "rows": rows, "today": today}


def load_approvals(path: str | None) -> dict:
    """{(key, today_hash, new_hash): entry}; key is a durable key or ``<unit_table>:<unit_id>``."""
    if not path or not os.path.isfile(path):
        return {}
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    out = {}
    for e in data.get("approvals", data if isinstance(data, list) else []):
        key = e.get("durable_key") or e.get("unit")
        if key and e.get("today_hash") and e.get("new_hash"):
            out[(key, e["today_hash"], e["new_hash"])] = e
    return out


# --------------------------------------------------------------------------- the pipeline


class UnitText:
    __slots__ = ("reader", "text", "body", "path", "source_link", "docx", "error", "file_hash")

    def __init__(self):
        self.reader, self.text, self.body, self.path, self.source_link, self.docx, self.error, self.file_hash = \
            "none", None, None, None, None, None, None, None


def read_unit(unit: dict, inp: dict, nopos: str, withpos: str) -> UnitText:
    """The text of one FIST unit from the best source its own record links to."""
    ut = UnitText()
    f = inp["files"].get(unit["basename"], {})
    if unit["itf_exists"] and unit["itf"]:
        d = os.path.join(withpos, unit["itf"])
        fmt = inp["b_formats"].get(unit["itf"])
        text, path = (read_b1(d) if fmt == "B1" else read_b2(d)) if fmt else (None, None)
        if text:
            ut.reader = "b1_xml" if fmt == "B1" else "b2_rows"
            ut.text, ut.body, ut.path, ut.source_link = text, text, os.path.relpath(path, withpos), "fist_itf"
            return ut
        ut.error = f"b_folder_unreadable:{unit['itf']}"
    if f.get("has_docx"):
        for d in dict.fromkeys(x for x in (unit["folder"], f.get("dir")) if x):
            p = os.path.join(nopos, d, "word", unit["basename"] + ".docx")
            if os.path.isfile(p):
                try:
                    dx = read_docx(p)
                except (zipfile.BadZipFile, OSError, ValueError) as ex:
                    ut.error = f"docx_unreadable:{type(ex).__name__}"
                    break
                ut.reader, ut.docx, ut.path, ut.source_link = "docx", dx, os.path.relpath(p, nopos), "fist_filename_docx"
                ut.body, ut.text = docx_text(dx, with_footnotes=False), docx_text(dx)
                return ut
    if f.get("has_pdf"):
        for d in dict.fromkeys(x for x in (unit["folder"], f.get("dir")) if x):
            p = os.path.join(nopos, d, "pdf", unit["basename"] + ".pdf")
            if os.path.isfile(p):
                ut.reader, ut.path, ut.file_hash = "pdf_today", os.path.relpath(p, nopos), file_sha(p)
                return ut
    return ut


def build(inp: dict, nopos: str, withpos: str, today_files: str, approvals: dict,
          new_rows_policy: str = "hold", limit: int | None = None) -> tuple:
    C = collections.Counter()
    unit_cache: dict = {}
    text_rows, line_diffs, unit_texts = [], [], {}
    diff_cache: dict = {}

    def unit_text(key):
        if key not in unit_cache:
            u = inp["units"].get(key)
            unit_cache[key] = read_unit(u, inp, nopos, withpos) if u else UnitText()
        return unit_cache[key]

    def row_out(**kw):
        base = {"row_key": None, "today_id": None, "unit_table": None, "unit_id": None, "basename": None,
                "kind": None, "language": None, "sys_id": None, "c_number": None, "durable_key": None,
                "reader": "none", "source_path": None, "source_link": None, "source_error": None,
                "today_text_source": None, "today_hash": None, "new_hash": None, "today_lines": None,
                "new_lines": None, "status": None, "classes": None, "blocks": 0, "held_blocks": 0,
                "lost_today_lines": None, "text_replaced": 0, "xml_verified": 0, "text_accepted": 0,
                "acceptance_basis": None, "gate2_publishable": None, "gate2_hold_reasons": None,
                "publishable_text": 0, "structure": None, "visibility_hold": 0, "hold_reason": None}
        base.update(kw)
        return base

    rows = inp["rows"][:limit] if limit else inp["rows"]
    log(f"  today's rows: {len(rows):,}")
    t0 = time.time()
    for n, r in enumerate(rows, 1):
        if n % 5000 == 0:
            log(f"    {n:,} rows ({time.time() - t0:.0f}s)")
        content, rel = inp["today"].get(r["id"], ("", None))
        key = (r["unit_table"], r["unit_id"]) if r["unit_table"] else None
        out = row_out(row_key=f"today:{r['id']}", today_id=r["id"], unit_table=r["unit_table"], unit_id=r["unit_id"],
                      basename=r["basename"], kind=r["kind"], language=r["language"], sys_id=r["sys_id"],
                      c_number=r["c_number"], durable_key=r["durable_key"], today_text_source=r["text_source"],
                      today_hash=sha(content), gate2_publishable=r["publishable"], gate2_hold_reasons=r["hold_reasons"])
        if r["outcome"] == "retired-with-alias":
            out["status"] = "retired"
            text_rows.append(out)
            C["status.retired"] += 1
            continue
        ut = unit_text(key) if key else UnitText()
        out.update({"reader": ut.reader, "source_path": ut.path, "source_link": ut.source_link, "source_error": ut.error})
        unit = inp["units"].get(key) if key else None
        if unit and unit["held"]:
            out["visibility_hold"] = 1
        if ut.reader == "none":
            out["status"], out["hold_reason"] = "no_source", ut.error or "file_absent_in_delivery"
            text_rows.append(out)
            C["status.no_source"] += 1
            continue
        if ut.reader == "pdf_today":
            today_hash = file_sha(os.path.join(today_files, rel)) if rel else None
            same = today_hash is not None and today_hash == ut.file_hash
            out["source_link"] = "pdf_hash_identical" if same else ("pdf_hash_differs" if today_hash else "pdf_today_missing")
            out.update({"new_hash": out["today_hash"], "status": "identical", "today_lines": len(_lines(content)),
                        "new_lines": len(_lines(content)), "lost_today_lines": 0, "classes": "{}"})
            unit_texts.setdefault(key, {"unit_table": key[0], "unit_id": key[1], "reader": ut.reader,
                                        "source_path": ut.path, "text": content, "text_hash": sha(content),
                                        "structure": None, "footnotes": None})
            text_rows.append(out)
            C["status.identical"] += 1
            C[f"route.{ut.reader}"] += 1
            continue
        new_text = ut.text
        new_hash = sha(new_text)
        if key not in unit_texts:
            dx = ut.docx
            unit_texts[key] = {"unit_table": key[0], "unit_id": key[1], "reader": ut.reader, "source_path": ut.path,
                               "text": new_text, "text_hash": new_hash,
                               "structure": _j(dict(dx.structure)) if dx else None,
                               "footnotes": _j(dx.footnotes) if dx and dx.footnotes else None}
        out["new_hash"] = new_hash
        dkey = (key, out["today_hash"])
        if dkey not in diff_cache:
            dx = ut.docx
            diff_cache[dkey] = diff_texts(content, ut.body, new_text, dx.footnotes if dx else [], dx.headers if dx else [])
        d = diff_cache[dkey]
        if ut.docx:
            out["structure"] = _j(dict(ut.docx.structure))
        out.update({"today_lines": d["today_lines"], "new_lines": d["new_lines"], "classes": _j(dict(d["classes"])),
                    "blocks": len(d["blocks"]), "held_blocks": d["held_blocks"], "lost_today_lines": d["lost_today_lines"]})
        # An XML row's DOCX is a verifier, never a replacement: today's XML text is the decoded
        # edition, the DOCX the typed original with font-rendered sigla. XML never regresses.
        verify_only = r["text_source"] == "xml" and ut.reader == "docx"
        approval = None
        for akey in (r["durable_key"], f"{key[0]}:{key[1]}"):
            approval = approvals.get((akey, out["today_hash"], new_hash))
            if approval:
                break
        if new_text.strip() == content.strip():
            status = "identical"
        elif d["held_blocks"] == 0 and verify_only:
            status = "verified"
        elif d["held_blocks"] == 0 and d["verify_only_blocks"] == 0:
            status = "accepted"
        elif approval:
            status = "verified" if verify_only else "approved"
            out["acceptance_basis"] = "approval:" + str(approval.get("by", ""))
        else:
            status = "held"
        out["status"] = status
        out["text_accepted"] = int(status in ("identical", "accepted", "verified", "approved"))
        out["text_replaced"] = int(status in ("accepted", "approved"))
        if status == "held":
            out["hold_reason"] = "unclassified_diff" if d["held_blocks"] else \
                "needs_reading:" + ",".join(sorted(set(d["classes"]) & VERIFY_ONLY_CLASSES))
        # gate-2 holds that are about today's XML text are lifted when the own record's text agrees
        holds = json.loads(r["hold_reasons"]) if r["hold_reasons"] else []
        xml_holds = [h for h in holds if h.startswith("xml_")]
        if r["text_source"] == "xml" and xml_holds and out["text_accepted"]:
            out["xml_verified"] = 1
        other_holds = [h for h in holds if not h.startswith("xml_")]
        out["publishable_text"] = int(out["text_accepted"] and not other_holds and (r["publishable"] == 1 or out["xml_verified"] == 1))
        if status == "held" and xml_holds and not other_holds:
            out["hold_reason"] = "xml_text_unverified"
        text_rows.append(out)
        for i, b in enumerate(d["blocks"]):
            disp = b["disposition"]
            if approval and status in ("approved", "verified") and disp in ("held", "verify_only"):
                disp = "approved"
            line_diffs.append({"row_key": out["row_key"], "block_no": i, "op": b["op"], "today_from": b["today_from"],
                               "today_to": b["today_to"], "new_from": b["new_from"], "new_to": b["new_to"],
                               "today_text": b["today_text"], "new_text": b["new_text"], "classes": _j(b["classes"]),
                               "disposition": disp})
        C[f"status.{status}"] += 1
        C[f"route.{ut.reader}"] += 1
        for c, v in d["classes"].items():
            C[f"class_blocks.{c}"] += v
            C[f"class_rows.{c}"] += 1
    log(f"  today's rows read and diffed ({time.time() - t0:.0f}s); new candidates ...")
    # ---- new rows: units of files FIST holds that today's sidecar does not serve
    new_statuses = ("candidate", "candidate_multi_alma", "hold_visibility", "unresolved_alma")
    served_units = {(r["unit_table"], r["unit_id"]) for r in rows if r["unit_table"]}
    new_units = sorted(
        (k for k, u in inp["units"].items()
         if k not in served_units and u["kind"] in ("transcription", "translation")
         and inp["files"].get(u["basename"], {}).get("status") in new_statuses),
        key=lambda k: (k[0], k[1]),
    )
    if limit:
        new_units = new_units[:limit]
    for key in new_units:
        u = inp["units"][key]
        ut = unit_text(key)
        f = inp["files"][u["basename"]]
        sys_ids = sorted(inp["almas"].get(u["basename"], set())) or [None]
        structure = _j(dict(ut.docx.structure)) if ut.docx else None
        plain = bool(ut.docx) and not any(ut.docx.structure.get(k) for k in (
            "tables", "footnotes", "superscript_runs", "hidden_runs", "tracked_changes", "images",
            "numbering_unrendered", "numbering_unknown_format"))
        if ut.reader not in ("none", "pdf_today") and key not in unit_texts:
            unit_texts[key] = {"unit_table": key[0], "unit_id": key[1], "reader": ut.reader, "source_path": ut.path,
                               "text": ut.text, "text_hash": sha(ut.text), "structure": structure,
                               "footnotes": _j(ut.docx.footnotes) if ut.docx and ut.docx.footnotes else None}
        for sid in sys_ids:
            out = row_out(row_key=f"new:{key[0]}:{key[1]}|{sid or ''}", unit_table=key[0], unit_id=key[1],
                          basename=u["basename"], kind=u["kind"], sys_id=sid, durable_key=f"{key[0]}:{key[1]}|{sid or ''}|",
                          reader=ut.reader, source_path=ut.path, source_link=ut.source_link, source_error=ut.error,
                          structure=structure, visibility_hold=int(u["held"] or f["status"] == "hold_visibility"))
            if ut.reader in ("none", "pdf_today"):
                out["status"] = "no_source"
                out["hold_reason"] = ut.error or ("pdf_only_no_text_reader" if ut.reader == "pdf_today" else "file_absent")
            else:
                out["status"] = "no_comparator"
                out["new_hash"] = sha(ut.text)
                out["new_lines"] = len(_lines(ut.text))
                accepted = new_rows_policy == "plain" and ut.reader in ("b1_xml", "b2_rows") or \
                    new_rows_policy == "plain" and plain and bool(ws(ut.text))
                if accepted:
                    out["text_accepted"], out["acceptance_basis"] = 1, ("rule:folder_b_text" if ut.reader != "docx" else "rule:plain_docx")
                else:
                    out["hold_reason"] = "needs_acceptance" if new_rows_policy == "hold" else "docx_structure"
            text_rows.append(out)
            C[f"status.{out['status']}"] += 1
            C[f"new.route.{ut.reader}"] += 1
            C["rows.new"] += 1
            if out["text_accepted"]:
                C["new.accepted"] += 1
            if structure:
                for k, v in json.loads(structure).items():
                    if v and k in ("tables", "footnotes", "superscript_runs", "hidden_runs", "tracked_changes", "images",
                                   "numbered_paragraphs", "numbering_unrendered", "header_parts"):
                        C[f"new.structure.{k}"] += 1
    # ---- counts
    C["rows"] = len(text_rows)
    C["rows.today"] = len(rows)
    C["units.read"] = len(unit_texts)
    C["replaced_rows"] = sum(o["text_replaced"] for o in text_rows)
    for o in text_rows:
        if o["text_replaced"]:
            C[f"replaced.by_reader.{o['reader']}"] += 1
        if o["today_id"] and o["status"] == "held":
            C[f"held.by_reader.{o['reader']}"] += 1
        if o["today_id"] and o["today_text_source"] == "xml":
            C[f"xml.{o['status']}"] += 1
            if o["xml_verified"]:
                C["xml.holds_lifted"] += 1
    C["publishable_text"] = sum(o["publishable_text"] for o in text_rows)
    C["gate2_publishable"] = sum(1 for o in text_rows if o["gate2_publishable"] == 1)
    C["rows_with_lost_lines"] = sum(1 for o in text_rows if (o["lost_today_lines"] or 0) > 0)
    C["replaced_with_lost_lines"] = sum(1 for o in text_rows if o["text_replaced"] and (o["lost_today_lines"] or 0) > 0)
    C["source_errors"] = sum(1 for o in text_rows if o["source_error"])
    C["line_diff_blocks"] = len(line_diffs)
    C["held_blocks"] = sum(1 for b in line_diffs if b["disposition"] == "held")
    digest = hashlib.sha256()
    for o in sorted(text_rows, key=lambda x: x["row_key"]):
        digest.update("|".join(str(o[k]) for k in ("row_key", "reader", "new_hash", "status", "classes", "text_replaced",
                                                   "publishable_text", "text_accepted")).encode("utf-8"))
        digest.update(b"\n")
    for k in sorted(unit_texts):
        digest.update(f"{k[0]}:{k[1]}:{unit_texts[k]['text_hash']}\n".encode("utf-8"))
    return text_rows, line_diffs, list(unit_texts.values()), C, digest.hexdigest()


# --------------------------------------------------------------------------- checks


def run_checks(text_rows: list, line_diffs: list, C: dict, expectations: dict, digest: str,
               previous_digest: str | None, allow_single_run: bool) -> list:
    checks = []

    def add(name, passed, expected, actual, note):
        checks.append({"name": name, "passed": int(bool(passed)), "expected": str(expected), "actual": str(actual), "note": note})

    if previous_digest is not None:
        add("T01_two_runs_identical", previous_digest == digest, previous_digest[:16], digest[:16],
            "the digest over every row's reader, text hash, status and classes equals the previous run's")
    else:
        add("T01_two_runs_identical", allow_single_run, "a previous run's digest", "not compared",
            "single run: pass --previous-summary (the wrapper does) or --allow-single-run")
    bad = [o["row_key"] for o in text_rows if o["text_replaced"] and o["source_link"] not in ("fist_itf", "fist_filename_docx")]
    add("T02_replaced_rows_have_source_link", not bad, "0 replaced rows without a FIST link", f"{len(bad)}",
        "a replaced text comes from the B folder FIST names or the DOCX of the unit's own FileName")
    bad = [b for b in line_diffs if b["disposition"] not in ("accepted", "held", "approved", "verify_only")]
    by_key = collections.defaultdict(list)
    for b in line_diffs:
        by_key[b["row_key"]].append(b)
    bad2 = [o["row_key"] for o in text_rows if o["status"] == "accepted" and any(b["disposition"] != "accepted" for b in by_key[o["row_key"]])]
    add("T03_every_diff_block_dispositioned", not bad and not bad2, "0 blocks without a disposition, 0 accepted rows with a held block",
        f"{len(bad)}, {len(bad2)}", "every changed block is accepted, approved, verify-only or held; an accepted row has only accepted blocks")
    bad = []
    for o in text_rows:
        if not o["text_replaced"]:
            continue
        for b in by_key[o["row_key"]]:
            cls = set(json.loads(b["classes"]))
            if b["disposition"] == "accepted" and not cls <= ACCEPTED_CLASSES:
                bad.append(o["row_key"])
            if b["disposition"] == "held":
                bad.append(o["row_key"])
    add("T04_replaced_blocks_in_accepted_classes", not bad, "0", f"{len(set(bad))}",
        "every block of a replaced row is in ACCEPTED_CLASSES or carries an owner approval")
    bad = [o["row_key"] for o in text_rows if not o["today_id"] and o["text_accepted"] and not o["acceptance_basis"]]
    add("T05_new_rows_need_acceptance", not bad, "0 new rows accepted without a basis", f"{len(bad)}",
        "a row without a comparator is accepted only by a declared rule or an approval")
    bad = [o["row_key"] for o in text_rows if o["today_text_source"] == "xml" and o["text_replaced"] and (o["lost_today_lines"] or 0) > 0]
    add("T06_no_xml_regression", not bad, "0 replaced XML rows losing a line", f"{len(bad)}",
        "a replaced XML row keeps every line of today's text")
    desc = [o["row_key"] for o in text_rows if o["unit_table"] == "dbo_UnitFreeDescription" or o["kind"] == "description"
            or (o["basename"] and NAME_SUFFIX_RE.search(o["basename"]) and NAME_SUFFIX_RE.search(o["basename"]).group(1) == "D")]
    add("T07_no_description_becomes_edition", not desc, "0", f"{len(desc)}",
        "no row comes from a FreeDescription unit or a _D file")
    bad = [o["row_key"] for o in text_rows if o["kind"] not in ("transcription", "translation")]
    add("T08_kinds_from_fist", not bad, "0", f"{len(bad)}", "every row is a transcription or a translation by FIST")
    for key, want in sorted(expectations.items()):
        got = C.get(key, 0)
        add(f"E_{key}", got == want, want, got, "declared expectation")
    return checks


# --------------------------------------------------------------------------- outputs


def _create(conn, table, rows, pk=None):
    if not rows:
        conn.execute(f'CREATE TABLE "{table}" (empty INTEGER)')
        return
    cols = list(rows[0].keys())
    defs = ", ".join(f'"{c}"' + (" PRIMARY KEY" if c == pk else "") for c in cols)
    conn.execute(f'CREATE TABLE "{table}" ({defs})')
    conn.executemany(f'INSERT INTO "{table}" VALUES ({",".join("?" * len(cols))})', ([r[c] for c in cols] for r in rows))


def write_outputs(text_rows, line_diffs, unit_texts, checks, C, out_dir, meta) -> str:
    os.makedirs(out_dir, exist_ok=True)
    db_path = os.path.join(out_dir, "gate3.db")
    if os.path.exists(db_path):
        try:
            old = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            ours = old.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='gate3_meta'").fetchone()[0] == 1
            old.close()
        except sqlite3.DatabaseError:
            ours = False
        if not ours:
            raise SystemExit(f"refusing to overwrite {db_path}: not a gate-3 database this script wrote")
        os.remove(db_path)
    tmp = db_path + ".tmp"
    if os.path.exists(tmp):
        os.remove(tmp)
    conn = sqlite3.connect(tmp)
    with conn:
        _create(conn, "gate3_meta", [{"key": k, "value": str(v)} for k, v in meta.items()], pk="key")
        _create(conn, "text_rows", text_rows, pk="row_key")
        _create(conn, "unit_texts", unit_texts)
        _create(conn, "line_diffs", line_diffs)
        _create(conn, "checks", checks, pk="name")
        _create(conn, "counts", [{"key": k, "value": v} for k, v in sorted(C.items())], pk="key")
        conn.execute('CREATE INDEX "ix_tr_today" ON "text_rows"("today_id")')
        conn.execute('CREATE INDEX "ix_tr_unit" ON "text_rows"("unit_table", "unit_id")')
        conn.execute('CREATE INDEX "ix_tr_status" ON "text_rows"("status")')
        if unit_texts:
            conn.execute('CREATE INDEX "ix_ut_unit" ON "unit_texts"("unit_table", "unit_id")')
        if line_diffs:
            conn.execute('CREATE INDEX "ix_ld_row" ON "line_diffs"("row_key")')
    conn.close()
    os.replace(tmp, db_path)
    with open(os.path.join(out_dir, "gate3_summary.json"), "w", encoding="utf-8") as fh:
        json.dump({"meta": meta, "counts": dict(sorted(C.items())), "checks": checks}, fh, ensure_ascii=False, indent=1)
    with open(os.path.join(out_dir, "GATE3_REPORT.md"), "w", encoding="utf-8") as fh:
        fh.write(render_report(text_rows, line_diffs, checks, C, meta))
    return db_path


def render_report(text_rows, line_diffs, checks, C, meta) -> str:
    failed = [c for c in checks if not c["passed"]]
    out = [f"# FGP unified text pipeline — gate 3 ({meta.get('run_at', '')})", "",
           f"**Result: {'FAIL' if failed else 'PASS'}** — {len(checks) - len(failed)} / {len(checks)} checks passed.", "",
           "| Check | Result | Expected | Actual | What it guards |", "|---|---|---|---|---|"]
    for c in checks:
        out.append(f"| `{c['name']}` | {'pass' if c['passed'] else '**FAIL**'} | {c['expected']} | {c['actual']} | {c['note']} |")
    out += ["", "## Today's rows by reader and status", "",
            "| Reader | identical | accepted | verified | approved | held | no_source | retired |",
            "|---|---|---|---|---|---|---|---|"]
    table = collections.defaultdict(collections.Counter)
    for o in text_rows:
        if o["today_id"]:
            table[o["reader"]][o["status"]] += 1
    for reader in READERS:
        if reader in table:
            t = table[reader]
            out.append(f"| `{reader}` | {t['identical']:,} | {t['accepted']:,} | {t['verified']:,} | {t['approved']:,} | "
                       f"{t['held']:,} | {t['no_source']:,} | {t['retired']:,} |")
    out += ["", f"Text replaced on **{C['replaced_rows']:,}** rows; publishable with text {C['publishable_text']:,} "
            f"(gate 2 publishable {C['gate2_publishable']:,}; XML holds lifted on {C.get('xml.holds_lifted', 0):,} rows). "
            f"New rows {C.get('rows.new', 0):,} (policy `{meta.get('new_rows_policy')}`, accepted {C.get('new.accepted', 0):,}).",
            "", "## Diff classes (blocks / rows)", "", "| Class | Blocks | Rows | Accepted |", "|---|---|---|---|"]
    for k in sorted(k for k in C if k.startswith("class_blocks.")):
        name = k[len("class_blocks."):]
        verdict = "yes" if name in ACCEPTED_CLASSES else ("verify only" if name in VERIFY_ONLY_CLASSES else "**no**")
        out.append(f"| `{name}` | {C[k]:,} | {C.get('class_rows.' + name, 0):,} | {verdict} |")
    out += ["", "## One accepted example per class (today → new)", ""]
    seen = set()
    for b in line_diffs:
        if b["disposition"] != "accepted":
            continue
        for c in json.loads(b["classes"]):
            if c not in seen and len(b["today_text"]) < 200:
                seen.add(c)
                out.append(f"- `{c}` (`{b['row_key']}`): `{b['today_text'][:120]!r}` → `{b['new_text'][:120]!r}`")
    out += ["", "## Held rows: first unclassified blocks (for review)", ""]
    shown = 0
    held_keys = {o["row_key"]: o for o in text_rows if o["status"] == "held"}
    for b in line_diffs:
        if b["disposition"] == "held" and b["row_key"] in held_keys and shown < 25:
            o = held_keys[b["row_key"]]
            out.append(f"- `{b['row_key']}` ({o['reader']}, {o['basename']}; classes {b['classes']})")
            out.append(f"  - today: `{b['today_text'][:160]!r}`")
            out.append(f"  - new: `{b['new_text'][:160]!r}`")
            shown += 1
    out += ["", "## All counts", "", "| Key | Value |", "|---|---|"]
    for k, v in sorted(C.items()):
        out.append(f"| `{k}` | {v:,} |" if isinstance(v, int) else f"| `{k}` | {v} |")
    out += ["", "## Inputs", ""] + [f"- `{k}`: {v}" for k, v in meta.items()] + [""]
    return "\n".join(out)


# --------------------------------------------------------------------------- main


def main(argv=None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--inventory", default=os.path.join(DEFAULT_OUT, "inventory.db"), help="gate-1 inventory.db")
    ap.add_argument("--gate2", default=DEFAULT_GATE2, help="gate-2 gate2.db")
    ap.add_argument("--fgp-db", default=DEFAULT_FGP_DB)
    ap.add_argument("--nopos", default=DEFAULT_NOPOS)
    ap.add_argument("--withpos", default=DEFAULT_WITHPOS)
    ap.add_argument("--today-files", default=DEFAULT_TODAY_FILES, help="today's copied PDFs (rel_path root)")
    ap.add_argument("--approvals", default=DEFAULT_APPROVALS, help="owner approvals JSON (absent = none)")
    ap.add_argument("--out-dir", default=DEFAULT_OUT)
    ap.add_argument("--new-rows-policy", choices=("hold", "plain"), default="hold",
                    help="hold: no new row is accepted; plain: folder-B text and plain DOCX (no tables, footnotes, "
                         "superscripts, hidden runs, tracked changes, images) are accepted by rule")
    ap.add_argument("--previous-summary", metavar="PATH", help="a previous run's gate3_summary.json (T01)")
    ap.add_argument("--allow-single-run", action="store_true", help="let T01 pass without a previous run")
    ap.add_argument("--limit", type=int, help="development: only the first N rows / new units")
    ap.add_argument("--expect", action="append", metavar="KEY=VALUE")
    ap.add_argument("--expect-file", metavar="PATH")
    args = ap.parse_args(argv)
    try:
        expectations = parse_expectations(args.expect, args.expect_file)
    except (SystemExit, OSError, ValueError) as ex:
        print(ex, file=sys.stderr)
        return 2
    previous_digest = None
    if args.previous_summary:
        try:
            with open(args.previous_summary, encoding="utf-8") as fh:
                previous_digest = json.load(fh)["meta"]["digest"]
        except (OSError, ValueError, KeyError) as ex:
            print(f"cannot read --previous-summary: {ex}", file=sys.stderr)
            return 2
    t0 = time.time()
    meta = {"run_at": time.strftime("%Y-%m-%d %H:%M:%S"), "inventory": args.inventory, "gate2": args.gate2,
            "fgp_db": args.fgp_db, "nopos": args.nopos, "withpos": args.withpos, "approvals": args.approvals,
            "new_rows_policy": args.new_rows_policy, "limit": args.limit or ""}
    log("loading the gate-1 inventory, gate-2 outcomes and today's text ...")
    inp = load_inputs(args.inventory, args.gate2, args.fgp_db)
    approvals = load_approvals(args.approvals)
    log(f"  {len(inp['units']):,} units, {len(inp['rows']):,} rows, {len(approvals):,} approvals ({time.time() - t0:.0f}s)")
    log("reading and diffing ...")
    text_rows, line_diffs, unit_texts, C, digest = build(inp, args.nopos, args.withpos, args.today_files, approvals,
                                                         new_rows_policy=args.new_rows_policy, limit=args.limit)
    meta["digest"] = digest
    checks = run_checks(text_rows, line_diffs, C, expectations, digest, previous_digest, args.allow_single_run)
    db_path = write_outputs(text_rows, line_diffs, unit_texts, checks, C, args.out_dir, meta)
    failed = [c for c in checks if not c["passed"]]
    for c in checks:
        log(f"  [{'ok  ' if c['passed'] else 'FAIL'}] {c['name']}: expected {c['expected']}, got {c['actual']}")
    log(f"wrote {db_path} ({time.time() - t0:.0f}s); digest {digest[:16]}")
    log(f"RESULT: {'FAIL' if failed else 'PASS'} ({len(checks) - len(failed)}/{len(checks)})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
