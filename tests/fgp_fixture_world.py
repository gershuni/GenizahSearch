# -*- coding: utf-8 -*-
"""One synthetic world for the FGP source-folder gates (gate 1 inventory, gate 2 rules).

A small FIST, the two delivery folders, today's sidecar, fjms and nli are built under a
root directory. The cases are chosen so every rule has at least one row to act on: a served
file, a candidate with a scan twin, a description, a multi-AlmaId inventory, a visibility
hold, duplicate unit rows, absent FIST files, an unresolved signature, unmatched files of
every reason, and sidecar rows that are duplicates, unreachable, in conflict with FIST,
registered twice, or carrying XML text from another record's folder.
"""
from __future__ import annotations

import json
import os
import sqlite3
import zipfile
from xml.sax.saxutils import escape

ALMA_A, ALMA_B, ALMA_C, ALMA_D = "990001", "990002", "990003", "990004"


def touch(path: str, content: bytes = b"x") -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(content)


# --------------------------------------------------------------------------- gate-3 text content

_W_NS = 'xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"'


def _run(text: str, hidden: bool = False, style: str | None = None) -> str:
    rpr = ""
    if hidden or style:
        rpr = "<w:rPr>" + (f'<w:rStyle w:val="{style}"/>' if style else "") + ("<w:vanish/>" if hidden else "") + "</w:rPr>"
    return f'<w:r>{rpr}<w:t xml:space="preserve">{escape(text)}</w:t></w:r>'


def _para(text: str, numbered: bool = False, hidden: bool = False) -> str:
    """One paragraph; ``<fn1>`` in the text becomes a footnote reference to footnote id 1."""
    ppr = '<w:pPr><w:numPr><w:ilvl w:val="0"/><w:numId w:val="1"/></w:numPr></w:pPr>' if numbered else ""
    runs = []
    for i, piece in enumerate(text.split("<fn1>")):
        if i:
            runs.append('<w:r><w:footnoteReference w:id="1"/></w:r>')
        if piece:
            runs.append(_run(piece, hidden=hidden))
    return f"<w:p>{ppr}{''.join(runs)}</w:p>"


def write_docx(path: str, paragraphs: list, numbered: tuple = (), footnotes: dict | None = None,
               header: str | None = None, hidden: list = (), table_rows: list = ()) -> None:
    """A minimal DOCX: paragraphs (``numbered`` indices get auto-numbering ``%1.``), hidden
    paragraphs, a table (one cell text per cell), footnotes.xml and header1.xml when given."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    body = [_para(p, numbered=i in numbered) for i, p in enumerate(paragraphs)]
    body += [_para(h, hidden=True) for h in hidden]
    if table_rows:
        rows = "".join("<w:tr>" + "".join(f"<w:tc>{_para(c)}</w:tc>" for c in r) + "</w:tr>" for r in table_rows)
        body.append(f"<w:tbl>{rows}</w:tbl>")
    doc = f'<?xml version="1.0" encoding="UTF-8"?><w:document {_W_NS}><w:body>{"".join(body)}<w:sectPr/></w:body></w:document>'
    numbering = (
        f'<?xml version="1.0" encoding="UTF-8"?><w:numbering {_W_NS}>'
        '<w:abstractNum w:abstractNumId="0"><w:lvl w:ilvl="0"><w:start w:val="1"/><w:numFmt w:val="decimal"/>'
        '<w:lvlText w:val="%1."/></w:lvl></w:abstractNum><w:num w:numId="1"><w:abstractNumId w:val="0"/></w:num>'
        "</w:numbering>"
    )
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("[Content_Types].xml", '<?xml version="1.0" encoding="UTF-8"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"/>')
        z.writestr("word/document.xml", doc)
        z.writestr("word/numbering.xml", numbering)
        if footnotes:
            fns = ('<w:footnote w:type="separator" w:id="-1"><w:p/></w:footnote>'
                   '<w:footnote w:type="continuationSeparator" w:id="0"><w:p/></w:footnote>')
            fns += "".join(f'<w:footnote w:id="{i}">{_para(t)}</w:footnote>' for i, t in footnotes.items())
            z.writestr("word/footnotes.xml", f'<?xml version="1.0" encoding="UTF-8"?><w:footnotes {_W_NS}>{fns}</w:footnotes>')
        if header:
            z.writestr("word/header1.xml", f'<?xml version="1.0" encoding="UTF-8"?><w:hdr {_W_NS}>{_para(header)}</w:hdr>')


# the B1 XML of unit 1 (its own position folder 100000) and the text today's parser makes of it
B1_XML = ('<?xml version="1.0"?><root><info><fgp>100000</fgp></info>'
          '<col n="1" t="טור_א"><row u="1" num="1"><t><w u="2">שורה </w><w u="3">ראשונה</w></t></row>'
          '<row u="4" num="2"><t><w u="5">שורה </w><w u="6">שנייה</w></t></row></col></root>')
B1_TEXT = "שורה ראשונה\nשורה שנייה"

# the B2 data.js of unit 19 (folder C400) and today's PDF text of the same edition
B2_C400_ROWS = ["@[.........]", "יום הכפורים אסור באכילה", "אוסרי{ם\\/\\/ן}&nbsp;ב'", "%ליענש%"]
B2_C400_TODAY = "].........[@\nיום הכפורים אסור באכילה\nאוסרי{ם\\\\//\\\\ן} ב'"
B2_C400_TEXT = "@[.........]\nיום הכפורים אסור באכילה\nאוסרי{ם\\/\\/ן} ב'\n%ליענש%"

# the DOCX of unit 3 (rows 5, 6 and 7) and today's PDF text of it, one artefact per line
DOCX_UNIT3_PARAS = ['ועד י"ג שנים [אב]ג', "שלום ולכן הוא דנום", "דבר ארוך מאד שנשבר לשתי שורות", "%ואמר%",
                    "הערה<fn1> בטקסט", "ראה TS 12.528:", "{ה\\/\\/ב}"]
DOCX_UNIT3_TODAY = ("T-S 1.5 Cambridge\n.1 ג שנים ]אב[ג\n.2 שלו\x0e ולכ\x04 הוא דנו\n.3 דבר ארוך מאד שנשבר\nלשתי שורות\n"
                    ".4 הערה1 בטקסט\n12.528: TS ראה\n{ה\\\\/\\\\/ב}\n1 זו הערת שוליים")
DOCX_UNIT3_TEXT = ('1. ועד י"ג שנים [אב]ג\n2. שלום ולכן הוא דנום\n3. דבר ארוך מאד שנשבר לשתי שורות\n%ואמר%\n'
                   "4. הערה1 בטקסט\nראה TS 12.528:\n{ה\\/\\/ב}\n\n1 זו הערת שוליים")


def _b2_js(number: str, rows: list) -> str:
    return "fgp=" + json.dumps({"FGP": number, "ac": [{"title": "", "rows": {str(i + 1): {"ndx": i, "txt": t} for i, t in enumerate(rows)}}],
                                "info": {"fgpNumber": number}}, ensure_ascii=False) + ";"


def write_text_content(root: str) -> None:
    """Real content for the files gate 3 reads, on top of the extended world."""
    nopos, withpos = os.path.join(root, "NoPOS"), os.path.join(root, "WithPOS")
    touch(os.path.join(withpos, "100000", "100000.xml"), B1_XML.encode("utf-8"))
    touch(os.path.join(withpos, "C200", "data.js"), _b2_js("C200", ["שורה אחת", "שורה שתיים"]).encode("utf-8"))
    touch(os.path.join(withpos, "C400", "data.js"), _b2_js("C400", B2_C400_ROWS).encode("utf-8"))
    touch(os.path.join(withpos, "C400", "C400_1.png"))
    word = os.path.join(nopos, "2000000", "word")
    write_docx(os.path.join(word, "20000000000015_CUL_T-S 1.5_Tc.docx"), DOCX_UNIT3_PARAS, numbered=(0, 1, 2, 4),
               footnotes={1: "זו הערת שוליים"}, header="T-S 1.5 Cambridge", hidden=["0001"])
    write_docx(os.path.join(word, "20000000000016_CUL_T-S 1.6_Tc.docx"), ["אבג דהו", "שורה שנייה"])
    write_docx(os.path.join(word, "20000000000031_CUL_T-S 3.1_Tc.docx"), ["כותרת"], table_rows=[["1", "תא ראשון"], ["2", "תא שני"]])
    write_docx(os.path.join(word, "20000000000032_CUL_T-S 3.2_Tc.docx"), ["טקסט פשוט", "שורה שנייה"])
    write_docx(os.path.join(nopos, "2000004", "word", "20000040000061_CUL_T-S 6.1_Tc.docx"), ["?ש?ורה ראשונה", "שורה שנייה"])
    for name in ("20000000000021_CUL_T-S 2.1_Tc", "20000000000031_CUL_T-S 3.1_Tc", "20000000000032_CUL_T-S 3.2_Tc"):
        touch(os.path.join(nopos, "2000000", "pdf", name + ".pdf"))
    # today's copied PDFs: row 2's is byte-identical to the delivered one, row 12's is not
    touch(os.path.join(root, "today_transcriptions", "CUL", "2", "20000000000011_CUL_T-S 1.1_Tl_translate_eng.pdf"))
    touch(os.path.join(root, "today_transcriptions", "CUL", "12", "10600030000021_Oxford_MS 2_Tc_trans.pdf"), b"other")


def build_fist(path: str, mixed_kind: bool = False, extended: bool = False, text: bool = False) -> None:
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE dbo_UnitTranscription (UnitTranscriptionId INTEGER, SignatureId INTEGER, FileName TEXT,
            Lang INTEGER, FolderPath TEXT, tmp_DoubleNotToDisplay INTEGER, HasFiles INTEGER,
            ImageTranscriptionFolder TEXT, HebCopyRight TEXT, EngCopyRight TEXT);
        CREATE TABLE dbo_UnitTranslation (UnitTranslationId INTEGER, SignatureId INTEGER, FileName TEXT,
            Lang INTEGER, FolderPath TEXT, tmp_DoubleNotToDisplay INTEGER, HasFiles INTEGER,
            HebCopyRight TEXT, EngCopyRight TEXT);
        CREATE TABLE dbo_UnitFreeDescription (UnitFreeDescriptionId INTEGER, SignatureId INTEGER, FileName TEXT,
            FolderPath TEXT, FreeDesc TEXT, HasFiles INTEGER);
        CREATE TABLE dbo_Signature (SignatureId INTEGER, SetSignatureId INTEGER, SourceId INTEGER, SubId INTEGER,
            DateCreated TEXT);
        CREATE TABLE dbo_InventorySignature (InventoryId INTEGER, SetSignatureId INTEGER);
        CREATE TABLE dbo_InventoryAlma (ID INTEGER, InventoryId INTEGER, AlmaId INTEGER);
        CREATE TABLE dbo_CodeSource (TeamCode INTEGER, EngDesc TEXT);
        CREATE TABLE CODE_Catalog (CatalogId INTEGER, CatAcronym TEXT, Publisher TEXT, YearOfPublishing TEXT);
        CREATE TABLE CODE_Title (TitleId INTEGER, RunningTitleHeb TEXT, FullTitleHeb TEXT, AcronymHeb TEXT);
        CREATE TABLE dbo_UnitBibliographyReference (UnitBibliographyReferenceId INTEGER, SignatureId INTEGER,
            TitleId INTEGER, JournalDate TEXT, MentionPage TEXT, FromPage TEXT, ToPage TEXT);
        CREATE TABLE dbo_BibMultiArticleAuthor (BibMultiArticleAuthorID INTEGER, UnitBibliographyReferenceId INTEGER,
            ArticleAuthorId INTEGER, ArticleAuthorDisplay TEXT, AuthorOrder INTEGER);
        CREATE TABLE CODE_Author (AuthorId INTEGER, HebDesc TEXT, EngDesc TEXT);
        """
    )
    tr = [
        # served today, one Alma, linked to an existing B1 dir (its own position folder)
        (1, 101, "20000000000011_CUL_T-S 1.1_Tc", 1, "2000000\\", None, 2, "100000", None, None),
        # candidate (not in today's DB), scan twin, linked to a B2 dir, copyright text
        (2, 102, "20000000000012_CUL_T-S 1.2", None, "2000000\\", None, 2, "C200", "ack", "ack-en"),
        # multi-Alma through one inventory; credit prefix disagrees (SourceId 132)
        (3, 103, "20000000000015_CUL_T-S 1.5_Tc", None, "2000000\\", None, 2, "9999_G1", None, None),
        # held: tmp_DoubleNotToDisplay = 1
        (4, 104, "20000000000016_CUL_T-S 1.6_Tc", None, "2000000\\", 1, 2, None, None, None),
        # duplicate unit rows for one file, same signature
        (5, 105, "20000000000017", None, "2000000\\", None, 2, None, None, None),
        (6, 105, "20000000000017", None, "2000000\\", None, 2, None, None, None),
        # absent from the export: dir present, file missing
        (7, 106, "20000000000099_CUL_T-S 9.9_Tc", None, "2000000\\", None, 2, None, None, None),
        # absent from the export: dir not exported (the 19 sidecar-only files' shape)
        (8, 107, "85000070000451_JTS_ENA NS 2.23_Tc", None, "8500000\\", None, 2, None, None, None),
        # unresolved: signature without an inventory
        (9, 108, "20000000000018_CUL_T-S 1.8_Tc", None, "2000000\\", None, 2, None, None, None),
    ]
    extended_tr = [
        # registered twice for one manuscript (two signatures, two sets, same source)
        (10, 111, "10600030000021_Oxford_MS 2_Tc", None, "1060003\\", None, 2, None, None, None),
        (11, 112, "10600030000021_Oxford_MS 2_Tc", None, "1060003\\", None, 2, None, None, None),
        # a catalogue (500) and a book (600) source; the book's SubId has a title, the other has none
        (12, 113, "50000010000031", None, "5000001\\", None, 2, None, None, None),
        (13, 114, "60000010000041", None, "6000001\\", None, 2, None, None, None),
        (14, 115, "60000010000051", None, "6000001\\", None, 2, None, None, None),
        # a Genuzos unit without a position folder whose image's B1 folder belongs to another Genuzos unit
        (15, 116, "20000040000061_CUL_T-S 6.1_Tc", None, "2000004\\", None, 2, None, None, None),
        # a bibliographic-reference unit (source 601; SubId = the reference id)
        (16, 117, "60198140000021_CUL_ Or.1080 6.17 _Tc", None, "6010000\\", None, 2, None, None, None),
        # a site-user unit (source 850; SubId = a user id read off the live site) and a personal-list unit (300)
        (17, 118, "85002210000011_CUL_ T-S 10F1_Tc", None, "8500221\\", None, 2, None, None, None),
        (18, 119, "30000530000201_CUL_ T-S AS 74.11 _Tc", None, "3000053\\", None, 2, None, None, None),
    ]
    text_tr = [
        # served today (row 21), its own B2 position folder C400
        (19, 120, "20000000000021_CUL_T-S 2.1_Tc", None, "2000000\\", None, 2, "C400", None, None),
        # candidates (not in today's DB): a DOCX with a table, and a plain DOCX
        (20, 121, "20000000000031_CUL_T-S 3.1_Tc", None, "2000000\\", None, 2, None, None, None),
        (21, 122, "20000000000032_CUL_T-S 3.2_Tc", None, "2000000\\", None, 2, None, None, None),
    ]
    con.executemany("INSERT INTO dbo_UnitTranscription VALUES (?,?,?,?,?,?,?,?,?,?)",
                    tr + (extended_tr if extended else []) + (text_tr if text else []))
    con.execute("INSERT INTO dbo_UnitTranslation VALUES (?,?,?,?,?,?,?,?,?)",
                (1, 109, "20000000000011_CUL_T-S 1.1_Tl", 2, "2000000\\", None, 2, None, None))
    con.execute("INSERT INTO dbo_UnitFreeDescription VALUES (?,?,?,?,?,?)",
                (1, 110, "20000000000013", "2000000\\", "a description", 2))
    if mixed_kind:
        con.execute("INSERT INTO dbo_UnitFreeDescription VALUES (?,?,?,?,?,?)",
                    (2, 101, "20000000000011_CUL_T-S 1.1_Tc", "2000000\\", "same file, other kind", 2))
    con.executemany("INSERT INTO dbo_Signature VALUES (?,?,?,?,?)", [
        (101, 1001, 200, 0, "2020"), (102, 1002, 200, 0, "2020"), (103, 1003, 132, 0, "2020"),
        (104, 1004, 200, 0, "2020"), (105, 1005, 200, 0, "2020"), (106, 1006, 200, 0, "2020"),
        (107, 1007, 850, 7, "2020"), (108, 1008, 200, 0, "2020"), (109, 1001, 200, 0, "2020"),
        (110, 1010, 500, 352, "2020"), (111, 1011, 106, 3, "2020"), (112, 1012, 106, 3, "2021"),
        (113, 1013, 500, 12, "2020"), (114, 1014, 600, 77, "2020"), (115, 1015, 600, 78, "2020"),
        (116, 1016, 200, 0, "2020"), (117, 1017, 601, 43279814, "2015"), (118, 1018, 850, 221, "2016"),
        (119, 1019, 300, 53, "2016"), (120, 1020, 200, 0, "2020"), (121, 1021, 200, 0, "2020"), (122, 1022, 200, 0, "2020"),
    ])
    con.executemany("INSERT INTO dbo_InventorySignature VALUES (?,?)", [
        (11, 1001), (12, 1002), (13, 1003), (14, 1004), (15, 1005), (16, 1006), (17, 1007), (20, 1010),
        (21, 1011), (21, 1012), (22, 1013), (23, 1014), (24, 1015), (25, 1016), (26, 1017), (27, 1018), (28, 1019),
        (29, 1020), (30, 1021), (31, 1022),
    ])
    con.executemany("INSERT INTO dbo_InventoryAlma VALUES (?,?,?)", [
        (1, 11, ALMA_A), (2, 12, ALMA_B), (3, 13, ALMA_C), (4, 13, ALMA_D), (5, 14, ALMA_A),
        (6, 15, ALMA_B), (7, 16, ALMA_A), (8, 17, ALMA_D), (9, 20, ALMA_C), (10, 21, ALMA_B),
        (11, 22, ALMA_A), (12, 23, ALMA_A), (13, 24, ALMA_A), (14, 25, ALMA_A), (15, 26, ALMA_A), (16, 27, ALMA_A),
        (17, 28, ALMA_A), (18, 29, ALMA_A), (19, 30, ALMA_B), (20, 31, ALMA_C),
    ])
    con.executemany("INSERT INTO dbo_CodeSource VALUES (?,?)", [(200, "Genuzos"), (132, "Goitein")])
    con.execute("INSERT INTO CODE_Catalog VALUES (12, 'CUDL', 'Cambridge', '2010')")
    con.execute("INSERT INTO CODE_Title VALUES (77, 'ספר הבהיר', NULL, NULL)")
    con.execute("INSERT INTO CODE_Title VALUES (78, NULL, NULL, NULL)")
    con.execute("INSERT INTO CODE_Title VALUES (51153, 'קובץ חצי גבורים', NULL, NULL)")
    con.execute("INSERT INTO dbo_UnitBibliographyReference VALUES (43279814, 117, 51153, 'אלול תשע\"ד', 'כו', 'כא', 'כז')")
    con.execute("INSERT INTO dbo_BibMultiArticleAuthor VALUES (1, 43279814, 4293, NULL, 2)")
    con.execute("INSERT INTO CODE_Author VALUES (4293, 'מילר, יעקב יצחק', NULL)")
    con.commit()
    con.close()


def build_folders(root: str, extended: bool = False) -> tuple:
    nopos = os.path.join(root, "NoPOS")
    pdf = os.path.join(nopos, "2000000", "pdf")
    for name in (
        "20000000000011_CUL_T-S 1.1_Tc.pdf", "20000000000011_CUL_T-S 1.1_Tl.pdf",
        "20000000000012_CUL_T-S 1.2.pdf", "20000000000012_CUL_T-S 1.2.scan.pdf",
        "20000000000013.pdf", "20000000000015_CUL_T-S 1.5_Tc.pdf", "20000000000016_CUL_T-S 1.6_Tc.pdf",
        "20000000000017.pdf", "20000000000018_CUL_T-S 1.8_Tc.pdf",
        "20000000000014_CUL_T-S 1.4_Tc.pdf",        # no FIST trace at all
        "20000000000015_CUL_T-S 1.5 renamed.pdf",   # same id as a FIST file, other name
        "20000000000011_CUL_T-S  1.1_Tc.pdf",       # whitespace drift of a FIST FileName (NTFS folds case)
        "C252953.pdf",                               # C-number named
        "Copy of 20000000000011_CUL_T-S 1.1_Tc.pdf", "~$lock.pdf", "notes.txt",
    ):
        touch(os.path.join(pdf, name))
    touch(os.path.join(nopos, "2000000", "word", "20000000000011_CUL_T-S 1.1_Tc.docx"))
    touch(os.path.join(nopos, "2000000", "word", "20000000000020_docx_only.docx"))
    touch(os.path.join(nopos, "2000000", "word_old", "20000000000011_CUL_T-S 1.1_Tc.doc"))
    if extended:
        touch(os.path.join(nopos, "1060003", "pdf", "10600030000021_Oxford_MS 2_Tc.pdf"))
        touch(os.path.join(nopos, "5000001", "pdf", "50000010000031.pdf"))
        touch(os.path.join(nopos, "6000001", "pdf", "60000010000041.pdf"))
        touch(os.path.join(nopos, "6000001", "pdf", "60000010000051.pdf"))
        touch(os.path.join(nopos, "2000004", "pdf", "20000040000061_CUL_T-S 6.1_Tc.pdf"))
        touch(os.path.join(nopos, "6010000", "pdf", "60198140000021_CUL_ Or.1080 6.17 _Tc.pdf"))
        touch(os.path.join(nopos, "8500221", "pdf", "85002210000011_CUL_ T-S 10F1_Tc.pdf"))
        touch(os.path.join(nopos, "3000053", "pdf", "30000530000201_CUL_ T-S AS 74.11 _Tc.pdf"))
    touch(os.path.join(nopos, "New Text Document.txt"))
    withpos = os.path.join(root, "WithPOS")
    for f in ("100000.xml", "100000.js", "100000.xml.js", "100000.json", "100000_1.png", "img.jpg"):
        touch(os.path.join(withpos, "100000", f))
    for f in ("data.js", "C200_1.png", "img.jpg"):
        touch(os.path.join(withpos, "C200", f))
    for f in ("300.xml", "300.js", "allb.jpg"):
        touch(os.path.join(withpos, "300 301 99944", f))
    touch(os.path.join(withpos, "400_G1", "400_G1.xml"))
    touch(os.path.join(withpos, "C200.G2", "data.js"))
    touch(os.path.join(withpos, "100000 dup", "x.xml"))
    os.makedirs(os.path.join(withpos, "V"))
    touch(os.path.join(withpos, "stray.txt"))
    return nopos, withpos


def build_sidecars(root: str, extended: bool = False, text: bool = False) -> tuple:
    fgp = os.path.join(root, "fgp_transcriptions.db")
    con = sqlite3.connect(fgp)
    con.execute(
        "CREATE TABLE fgp_transcriptions (id INTEGER, filename TEXT, collection TEXT, shelfmark TEXT, c_number TEXT, "
        "image_id TEXT, doc_relation TEXT, language TEXT, sys_id TEXT, mms_id TEXT, sysid_method TEXT, text_source TEXT, "
        "source_credit_he TEXT, source_credit_en TEXT, content TEXT, content_length INTEGER, rel_path TEXT)"
    )
    genuzos = "צוות FGP להעתקות -גנוזות"
    genuzos_en = "FGP Transcriptions Team - Genuzos"

    def row(rid, filename, sys_id, c_number="", image_id="", text_source="pdf_rtl", credit=None, content="text",
            rel_path=None, language=None, doc_relation="Digital Edition", method="fjms_cnumber", credit_en=None):
        return (rid, filename, "CUL", "T-S", c_number or None, image_id or filename[:14], doc_relation, language,
                sys_id, "m" + (sys_id or ""), method if sys_id else None, text_source, credit,
                credit_en if credit_en is not None else credit, content,
                len(content), rel_path or f"CUL/{rid}/{filename}")

    # gate 3: real text behind the rows its readers and classes act on
    c_xml = B1_TEXT if text else "text"
    c_unit3 = DOCX_UNIT3_TODAY if text else "text"
    c_held = "אבד דהו\nשורה שנייה" if text else "text"
    base_rows = [
        # 1: served, own position folder, XML text from its own folder -> kept
        row(1, "20000000000011_CUL_T-S 1.1_Tc_trans.pdf", ALMA_A, "C100000", text_source="xml", credit=genuzos, credit_en=genuzos_en,
            content=c_xml),
        # 2: its translation (English)
        row(2, "20000000000011_CUL_T-S 1.1_Tl_translate_eng.pdf", ALMA_A, "C100000", text_source="original",
            credit=genuzos, credit_en=genuzos_en, language="English", doc_relation="Digital Translation"),
        # 3: sidecar-only file (dir not exported) -> kept, file absent, credit unresolved (source 850)
        row(3, "85000070000451_JTS_ENA NS 2.23_Tc_trans.pdf", ALMA_D, image_id="85000070000451", method="fjms_shelfmark"),
        # 4: not in FIST at all and no sys_id -> unreachable (gate 2 retires it)
        row(4, "99999999999999_Nowhere_Tc_trans.pdf", None, "C300", image_id="99999999999999"),
    ]
    extended_rows = [
        # 17: not in FIST at all but reachable -> association none, kept but held
        row(17, "88888888888888_Nowhere_Tc_trans.pdf", ALMA_C, image_id="88888888888888", method="fjms_shelfmark"),
        # 18: a bibliographic reference (source 601) -> credit built from FIST's reference row
        row(18, "60198140000021_CUL_ Or.1080 6.17 _Tc_trans.pdf", ALMA_A, image_id="60198140000021"),
        # 19: a site user known by id (source 850, SubId 221); 20: a personal list known by id (source 300, SubId 53)
        row(19, "85002210000011_CUL_ T-S 10F1_Tc_trans.pdf", ALMA_A, image_id="85002210000011"),
        row(20, "30000530000201_CUL_ T-S AS 74.11 _Tc_trans.pdf", ALMA_A, image_id="30000530000201"),
        # 5/6: exact duplicates (same file, manuscript, image; two shelfmark folders) -> 6 retired, successor 5
        row(5, "20000000000015_CUL_T-S 1.5_Tc_trans.pdf", ALMA_C, credit="wrong team", rel_path="CUL/a/x.pdf", content=c_unit3),
        row(6, "20000000000015_CUL_T-S 1.5_Tc_trans.pdf", ALMA_C, credit="wrong team", rel_path="CUL/b/x.pdf", content=c_unit3),
        # 7: the same file under its second AlmaId -> kept (multi-Alma is one row per manuscript)
        row(7, "20000000000015_CUL_T-S 1.5_Tc_trans.pdf", ALMA_D, credit="wrong team", content=c_unit3),
        # 8: held unit -> visibility hold
        row(8, "20000000000016_CUL_T-S 1.6_Tc_trans.pdf", ALMA_A, credit=genuzos, credit_en=genuzos_en, content=c_held),
        # 9: sys_id the FIST chain contradicts -> retired, no successor
        row(9, "20000000000017_trans.pdf", ALMA_D, credit=genuzos, credit_en=genuzos_en, method="libraries_csv"),
        # 10: no sys_id -> unreachable, retired
        row(10, "20000000000018_CUL_T-S 1.8_Tc_trans.pdf", None, credit=genuzos, credit_en=genuzos_en),
        # 11: FIST unit has no AlmaId -> unconfirmed, kept, held
        row(11, "20000000000018_CUL_T-S 1.8_Tc_trans.pdf", ALMA_B, credit=genuzos, credit_en=genuzos_en, method="lib_shelfmark_token"),
        # 12: registered twice for the same manuscript (two signatures, same source) -> kept, lowest unit id
        row(12, "10600030000021_Oxford_MS 2_Tc_trans.pdf", ALMA_B, image_id="10600030000021", credit=None),
        # 13: catalogue source 500 -> resolved catalogue credit (today null -> changed)
        row(13, "50000010000031_trans.pdf", ALMA_A, image_id="50000010000031"),
        # 14: book with a title -> resolved; 15: book SubId without a title -> unresolved
        row(14, "60000010000041_trans.pdf", ALMA_A, image_id="60000010000041"),
        row(15, "60000010000051_trans.pdf", ALMA_A, image_id="60000010000051"),
        # 16: XML text keyed by image 100000 on a unit without a folder; the folder's owner is unit 1 (same source, same ms)
        row(16, "20000040000061_CUL_T-S 6.1_Tc_trans.pdf", ALMA_A, "C100000", image_id="20000040000061",
            text_source="xml", credit=genuzos, credit_en=genuzos_en, content=c_xml),
    ]
    text_rows = [
        # 21: served, its unit's own B2 folder C400; today's text came through the PDF route
        row(21, "20000000000021_CUL_T-S 2.1_Tc_trans.pdf", ALMA_A, "C400", image_id="20000000000021",
            credit=genuzos, credit_en=genuzos_en, content=B2_C400_TODAY),
    ]
    con.executemany("INSERT INTO fgp_transcriptions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    base_rows + (extended_rows if extended else []) + (text_rows if text else []))
    con.execute("CREATE TABLE fgp_shelfmark_meta (collection TEXT, shelfmark TEXT, mms_id TEXT, raw_json TEXT)")
    if extended:
        # FJMS DataSource for the manuscript of row 3 (source 850): one site user beside a catalogue,
        # the single matching part gate 2 credits; and for ALMA_C's manuscript two site users (ambiguous).
        con.executemany("INSERT INTO fgp_shelfmark_meta VALUES (?,?,?,?)", [
            ("JTS", "ENA NS 2.23", "m" + ALMA_D, json.dumps({"DataSource":
                "{eng:Schwab Catalog, heb:Schwab Catalog}, {eng:משתמש באתר - פלוני אלמוני, heb:משתמש באתר - פלוני אלמוני}"})),
            ("CUL", "T-S 1.5", "m" + ALMA_C, json.dumps({"DataSource":
                "{eng:משתמש באתר - ראובן, heb:משתמש באתר - ראובן}, {eng:משתמש באתר - שמעון, heb:משתמש באתר - שמעון}"})),
        ])
    con.commit()
    con.close()
    fjms = os.path.join(root, "fjms_enrichment.db")
    con = sqlite3.connect(fjms)
    con.execute("CREATE TABLE extra_info (FGP TEXT, AlmaId TEXT, PixelWidth INTEGER, PixelHeight INTEGER, Rotation_Angle_deg REAL)")
    con.executemany("INSERT INTO extra_info VALUES (?,?,?,?,?)", [
        ("C100000", ALMA_A, 2232, 2712, 1.0), ("C200", ALMA_B, 2000, 3000, 182.0),
        ("C300", ALMA_C, 2000, 3000, None), ("C301", ALMA_C, 2000, 3000, 270.0),
        ("C99944", ALMA_C, 2000, 3000, 45.0), ("C777", "x", 1, 1, None),
    ])
    con.commit()
    con.close()
    nli = os.path.join(root, "nli_crossref.db")
    con = sqlite3.connect(nli)
    # The columns the apps' NliCrossrefService.get_images reads too (gate 4 drives it over this file).
    con.execute("CREATE TABLE nli_images (FGPImageNumberId TEXT, NLI_AlmaId TEXT, ImageName TEXT, FGPNumber TEXT, "
                "ImageSourceName TEXT, Shelfmark TEXT)")
    con.executemany("INSERT INTO nli_images VALUES (?,?,?,?,?,?)", [
        ("100000", ALMA_A, "T_S_1_1__L1F0B0S1", "C100000", "T-S 1.1 recto", "T-S 1.1"),
        ("100001", ALMA_A, "T_S_1_1__L1F0B0S2", "C100001", "T-S 1.1 verso", "T-S 1.1"),
        ("200", ALMA_B, "T_S_1_2__L1F0B0S1", "C200", "T-S 1.2 recto", "T-S 1.2"),
        ("200", ALMA_C, "T_S_1_2__L1F0B0S1", "C200", "T-S 1.2 recto", "T-S 1.2"), ("300", ALMA_C, "X", "C300", "X", "X"),
    ])
    con.commit()
    con.close()
    return fgp, fjms, nli


def build_world(root: str, extended: bool = False, text: bool = False) -> dict:
    """The gate-1 world; ``extended`` adds the files, units and sidecar rows gate 2's rules act on;
    ``text`` (implies extended) adds real XML / data.js / DOCX content and the rows gate 3 reads."""
    extended = extended or text
    fist = os.path.join(root, "FIST.db")
    build_fist(fist, extended=extended, text=text)
    nopos, withpos = build_folders(root, extended=extended)
    fgp, fjms, nli = build_sidecars(root, extended=extended, text=text)
    if text:
        write_text_content(root)
    return {"root": root, "fist": fist, "nopos": nopos, "withpos": withpos, "fgp": fgp, "fjms": fjms,
            "nli": nli, "out": os.path.join(root, "inventory"), "today_files": os.path.join(root, "today_transcriptions")}


def read_docx_path(world: dict, dir_name: str, basename: str) -> str:
    return os.path.join(world["nopos"], dir_name, "word", basename + ".docx")


def rows(db: str, sql: str) -> list:
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(sql)]
    finally:
        con.close()
