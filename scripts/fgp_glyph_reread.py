# -*- coding: utf-8 -*-
"""Re-read FGP's PDF-text transcriptions through their fonts' glyph names (GS-276).

Plan: ``docs/plans/FGP_PDF_GLYPH_REREAD_PLAN.md`` (revision 3, approved 2026-10-27). FGP's PDFs embed simple fonts
whose ``/Differences`` name every glyph (``nunfinal``, ``vavwithholam``, ``alefisolated`` ...) but most carry no
``/ToUnicode``; the extractor does not know those names and falls back to the raw byte code, so letters came out as
boxes, as wrong symbols, or not at all. This module reads each glyph exactly (font resource + byte code), decodes it
through a reviewed name registry (``scripts/fgp_glyph_registry.py``) and certified glyph shapes, and re-runs the same
extraction on a repaired scratch copy of the PDF.

Nothing here writes to the candidate. Working files go to ``fgp_data/inventory/glyph_reread/`` (gitignored).

    python scripts/fgp_glyph_reread.py census      # step 1: fonts, glyph names and drawn codes of every PDF in scope
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import multiprocessing as mp
import os
import sqlite3
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

FGP = os.path.join(REPO, "fgp_data")
# gate 3 points these at its own run (use_work_dir); the workers this process starts read them from the environment
PDF_ROOT = os.environ.get("FGP_GLYPH_REREAD_PDF_ROOT") or os.path.join(FGP, "transcriptions")
TODAY_DB = os.path.join(FGP, "fgp_transcriptions.db")
INV = os.path.join(FGP, "inventory")
CAND_DB = os.path.join(INV, "fgp_transcriptions.candidate.db")
GATE3_DB = os.path.join(INV, "gate3.db")
WORK = os.environ.get("FGP_GLYPH_REREAD_WORK") or os.path.join(INV, "glyph_reread")
CENSUS_DB = os.path.join(WORK, "census.db")

PDF_SOURCES = ("pdf_rtl", "original")  # today's text_source values that came from a PDF


def ro(path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# A run this small is read in this process: no workers to start, and none that could resolve ``scripts`` to another
# package (under pytest the skill's own ``scripts`` package is on the path, and a worker that cannot load its task
# leaves the pool waiting for ever).
SERIAL_BELOW = 64


class _Serial:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def imap_unordered(self, fn, items, chunksize=1):
        return map(fn, items)


def _pool(workers: int, n: int, maxtasksperchild: int | None = None):
    return _Serial() if workers <= 1 or n < SERIAL_BELOW else mp.Pool(workers, maxtasksperchild=maxtasksperchild)


# --- the rows -----------------------------------------------------------------------------------------------------

def scope_from(text_rows, today: dict, final_texts: dict) -> list[dict]:
    """The rows in scope (plan: 'The problem, measured'): every row gate 3 keeps with today's text (not retired, not
    replaced) whose text came from a PDF. id, rel_path, text_source, S0 (today's stored text) and S (S0 with the
    vocabulary repair, what a held row keeps). ``today`` is {id: (content, rel_path)}, ``final_texts`` gate 3's
    final_letters texts by today id."""
    out = []
    for o in sorted(text_rows, key=lambda o: o["today_id"] or 0):
        tid, src = o["today_id"], o["today_text_source"] or ""
        if not tid or o["status"] == "retired" or o["text_replaced"] or src.split("+")[0] not in PDF_SOURCES:
            continue
        content, rel = today.get(tid, ("", None))
        if not rel:
            continue
        out.append({"id": tid, "rel_path": rel, "text_source": src, "s0": content or "",
                    "s": final_texts.get(tid, content or "")})
    return out


def bench_from(text_rows, today: dict, unit_texts: dict) -> list[dict]:
    """Check C3's benchmark: every today row from a PDF whose text gate 3 replaces with Word or line text.
    ``unit_texts`` is {(unit_table, unit_id): text}."""
    out = []
    for o in sorted(text_rows, key=lambda o: o["today_id"] or 0):
        tid = o["today_id"]
        if not tid or o["status"] == "retired" or not o["text_replaced"]:
            continue
        rel = today.get(tid, ("", None))[1]
        word = unit_texts.get((o["unit_table"], o["unit_id"]))
        if not rel or (o["today_text_source"] or "") not in PDF_SOURCES or not word:
            continue
        out.append({"id": tid, "rel_path": rel, "reader": o["reader"], "word_text": word})
    return out


# Gate 3 hands its own rows in (set_inputs); standalone, they are read from gate3.db and today's database.
_INPUTS: dict | None = None


def set_inputs(scope: list, bench: list, vocab_maps: dict, vocabulary_texts: list) -> None:
    global _INPUTS
    _INPUTS = {"scope": scope, "bench": bench, "vocab_maps": vocab_maps, "vocabulary_texts": vocabulary_texts}


def _gate3_text_rows():
    cols = ("today_id", "status", "text_replaced", "today_text_source", "unit_table", "unit_id", "reader")
    return [dict(zip(cols, r)) for r in ro(GATE3_DB).execute(f"SELECT {', '.join(cols)} FROM text_rows")]


def _today():
    return {i: (c or "", rel) for i, c, rel in ro(TODAY_DB).execute("SELECT id, content, rel_path FROM fgp_transcriptions")}


def scope_rows() -> list[dict]:
    if _INPUTS is not None:
        return _INPUTS["scope"]
    finals = dict(ro(GATE3_DB).execute("SELECT today_id, text FROM final_letters"))
    return scope_from(_gate3_text_rows(), _today(), finals)


def benchmark_rows() -> list[dict]:
    if _INPUTS is not None:
        return _INPUTS["bench"]
    units = {(t, u): txt for t, u, txt in ro(GATE3_DB).execute("SELECT unit_table, unit_id, text FROM unit_texts")}
    return bench_from(_gate3_text_rows(), _today(), units)


def vocab_maps() -> dict:
    """{today id: {code character: letter}}: gate 3's vocabulary repair, for check C6."""
    if _INPUTS is not None:
        return _INPUTS["vocab_maps"]
    return {tid: {chr(int(k[2:], 16)): v for k, v in json.loads(mapping).items()}
            for tid, mapping in ro(GATE3_DB).execute("SELECT today_id, mapping FROM final_letters WHERE applied > 0")}


def vocabulary_texts() -> list:
    """The clean texts gate 3's vocabulary is built from (unit texts and today's texts), for the C6 exception rule."""
    if _INPUTS is not None:
        return _INPUTS["vocabulary_texts"]
    return ([t for (t,) in ro(GATE3_DB).execute("SELECT text FROM unit_texts")]
            + [c for (c,) in ro(TODAY_DB).execute("SELECT content FROM fgp_transcriptions")])


# --- step 1: census -----------------------------------------------------------------------------------------------

def _name(x) -> str | None:
    from pdfminer.psparser import PSLiteral
    if isinstance(x, PSLiteral):
        n = x.name
        return n.decode("latin-1") if isinstance(n, bytes) else str(n)
    return None


def _font_record(spec) -> dict:
    """What the font dictionary says: subtype, base font, embedded program, existing map, encoding names."""
    from pdfminer.pdftypes import resolve1
    spec = resolve1(spec) if spec is not None else {}
    if not isinstance(spec, dict):
        return {}
    desc = resolve1(spec.get("FontDescriptor")) or {}
    if not isinstance(desc, dict):
        desc = {}
    enc = resolve1(spec.get("Encoding"))
    base_enc, diffs = None, {}
    if isinstance(enc, dict):
        base_enc = _name(resolve1(enc.get("BaseEncoding")))
        code = None
        for x in resolve1(enc.get("Differences")) or []:
            x = resolve1(x)
            if isinstance(x, int):
                code = x
            elif _name(x) is not None and code is not None:
                diffs[code] = _name(x)
                code += 1
    elif _name(enc):
        base_enc = _name(enc)
    return {"subtype": _name(resolve1(spec.get("Subtype"))), "basefont": _name(resolve1(spec.get("BaseFont"))),
            "font_file": next((k for k in ("FontFile", "FontFile2", "FontFile3") if k in desc), None),
            "flags": resolve1(desc.get("Flags")), "has_tounicode": "ToUnicode" in spec,
            "base_encoding": base_enc, "differences": diffs}


def census_one(path: str) -> dict:
    """Fonts (by PDF object number) and every drawn (font, byte code) of one PDF, read from the content streams."""
    from pdfminer.pdfdevice import PDFTextDevice
    from pdfminer.pdfdocument import PDFDocument
    from pdfminer.pdfinterp import PDFPageInterpreter, PDFResourceManager
    from pdfminer.pdfpage import PDFPage
    from pdfminer.pdfparser import PDFParser
    from pdfminer.pdftypes import resolve1

    fonts: dict = {}

    class RM(PDFResourceManager):
        def get_font(self, objid, spec):
            font = super().get_font(objid, spec)
            key = objid if objid is not None else -id(font)
            if key not in fonts:
                fonts[key] = _font_record(spec)
            font._gs_key = key
            return font

    drawn: collections.Counter = collections.Counter()
    std: dict = {}

    class Dev(PDFTextDevice):
        def render_char(self, matrix, font, fontsize, scaling, rise, cid, ncs, graphicstate):
            k = (getattr(font, "_gs_key", None), cid)
            drawn[k] += 1
            if k not in std:
                try:
                    std[k] = font.to_unichr(cid)
                except Exception:  # noqa: BLE001 - pdfminer raises several kinds for an unmapped code
                    std[k] = None
            return 0.0

    rec = {"sha256": sha256_file(path), "pages": 0, "producer": None, "error": None}
    try:
        with open(path, "rb") as fh:
            doc = PDFDocument(PDFParser(fh))
            info = doc.info[0] if doc.info else {}
            prod = resolve1(info.get("Producer")) if isinstance(info, dict) else None
            rec["producer"] = prod.decode("latin-1", "replace") if isinstance(prod, bytes) else (str(prod) if prod else None)
            rm = RM(caching=True)
            interp = PDFPageInterpreter(rm, Dev(rm))
            for page in PDFPage.create_pages(doc):
                interp.process_page(page)
                rec["pages"] += 1
    except Exception as e:  # noqa: BLE001 - a damaged PDF is recorded, not fatal
        rec["error"] = f"{type(e).__name__}: {e}"[:300]
    rec["fonts"] = fonts
    rec["drawn"] = [(k[0], k[1], n, std.get(k)) for k, n in drawn.items()]
    return rec


def _census_task(path):
    full = os.path.join(PDF_ROOT, path)
    if not os.path.isfile(full):  # recorded as unreadable: its rows have nothing to decode
        return path, {"sha256": None, "pages": 0, "producer": None, "error": "missing", "fonts": {}, "drawn": []}
    return path, census_one(full)


def run_census(workers: int = 16) -> None:
    os.makedirs(WORK, exist_ok=True)
    scope, bench = scope_rows(), benchmark_rows()
    paths = sorted({r["rel_path"] for r in scope} | {r["rel_path"] for r in bench})
    print(f"scope rows {len(scope):,}, benchmark rows {len(bench):,}, PDF paths {len(paths):,}", flush=True)
    if os.path.exists(CENSUS_DB):
        os.remove(CENSUS_DB)  # our own working file, rebuilt from the PDFs
    db = sqlite3.connect(CENSUS_DB)
    db.executescript("""
        CREATE TABLE rows (kind TEXT, id INTEGER, rel_path TEXT, PRIMARY KEY (kind, id));
        CREATE TABLE pdfs (rel_path TEXT PRIMARY KEY, sha256 TEXT, pages INTEGER, producer TEXT, error TEXT);
        CREATE TABLE fonts (sha256 TEXT, objid INTEGER, subtype TEXT, basefont TEXT, font_file TEXT, flags INTEGER,
                            has_tounicode INTEGER, base_encoding TEXT, differences TEXT, PRIMARY KEY (sha256, objid));
        CREATE TABLE drawn (sha256 TEXT, objid INTEGER, code INTEGER, n INTEGER, std TEXT,
                            PRIMARY KEY (sha256, objid, code));
    """)
    db.executemany("INSERT INTO rows VALUES ('scope', ?, ?)", [(r["id"], r["rel_path"]) for r in scope])
    db.executemany("INSERT INTO rows VALUES ('bench', ?, ?)", [(r["id"], r["rel_path"]) for r in bench])
    seen, t0 = set(), time.time()
    with _pool(workers, len(paths)) as pool:
        for k, (rel, rec) in enumerate(pool.imap_unordered(_census_task, paths, chunksize=4), 1):
            db.execute("INSERT INTO pdfs VALUES (?, ?, ?, ?, ?)",
                       (rel, rec["sha256"], rec["pages"], rec["producer"], rec["error"]))
            if rec["sha256"] not in seen:
                seen.add(rec["sha256"])
                for objid, f in rec["fonts"].items():
                    db.execute("INSERT OR IGNORE INTO fonts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               (rec["sha256"], objid, f.get("subtype"), f.get("basefont"), f.get("font_file"),
                                f.get("flags"), int(bool(f.get("has_tounicode"))), f.get("base_encoding"),
                                json.dumps(f.get("differences") or {}, ensure_ascii=False)))
                db.executemany("INSERT OR IGNORE INTO drawn VALUES (?, ?, ?, ?, ?)",
                               [(rec["sha256"], o, c, n, s) for o, c, n, s in rec["drawn"]])
            if k % 2000 == 0:
                db.commit()
                print(f"  {k:,} / {len(paths):,} PDFs, {time.time() - t0:.0f}s", flush=True)
    db.commit()
    n_err = db.execute("SELECT count(*) FROM pdfs WHERE error IS NOT NULL").fetchone()[0]
    print(f"done: {len(paths):,} paths, {len(seen):,} distinct PDFs, {n_err} unreadable, {time.time() - t0:.0f}s")


# --- step 3: glyph certification ------------------------------------------------------------------------------------

FONTS_DIR = os.path.join(os.environ.get("WINDIR", r"C:\Windows"), "Fonts")
# Reference typefaces (plan step 3), pinned by file hash in every output. Family = the typeface a match must agree with.
REFERENCE_FONTS = {
    "david.ttf": "David", "davidbd.ttf": "David", "nrkis.ttf": "Narkisim", "frank.ttf": "Frank Ruehl",
    "mriam.ttf": "Miriam", "mriamc.ttf": "Miriam Fixed", "rod.ttf": "Rod", "lvnm.ttf": "Levenim MT",
    "lvnmbd.ttf": "Levenim MT", "gisha.ttf": "Gisha", "gishabd.ttf": "Gisha",
    "times.ttf": "Times New Roman", "timesbd.ttf": "Times New Roman", "timesi.ttf": "Times New Roman",
    "timesbi.ttf": "Times New Roman", "arial.ttf": "Arial", "arialbd.ttf": "Arial", "ariali.ttf": "Arial",
    "arialbi.ttf": "Arial", "tahoma.ttf": "Tahoma", "tahomabd.ttf": "Tahoma", "cour.ttf": "Courier New",
    "courbd.ttf": "Courier New", "couri.ttf": "Courier New", "courbi.ttf": "Courier New",
}
REF_PICKLE = os.path.join(WORK, "reference_outlines.pkl")


def canon_outline(tt, gname):
    """Canonical unhinted outline of a TrueType glyph: composites decomposed, units scaled to 2048 per em, each
    contour rotated to start at its smallest point (a harmless start-point difference), contours sorted. ``None``
    when the glyph is missing, ``()`` when it is empty (a space draws nothing and never certifies anything)."""
    glyf = tt["glyf"]
    if not gname or gname not in glyf:
        return None
    coords, ends, flags = glyf[gname].getCoordinates(glyf)
    upm = tt["head"].unitsPerEm
    contours, start = [], 0
    for e in ends:
        pts = [(round(coords[i][0] * 2048 / upm), round(coords[i][1] * 2048 / upm), int(flags[i] & 1))
               for i in range(start, e + 1)]
        start = e + 1
        if pts:
            k = min(range(len(pts)), key=lambda i: pts[i])
            contours.append(tuple(pts[k:] + pts[:k]))
    return tuple(sorted(contours))


def build_reference() -> dict:
    """{'files': {file: sha256}, 'index': {outline: [(file, codepoint)]}} over every cmap-mapped glyph."""
    from fontTools.ttLib import TTFont
    files, index = {}, collections.defaultdict(list)
    for fname in sorted(REFERENCE_FONTS):
        path = os.path.join(FONTS_DIR, fname)
        if not os.path.isfile(path):
            continue
        files[fname] = sha256_file(path)
        tt = TTFont(path, fontNumber=0)
        for cp, gname in (tt.getBestCmap() or {}).items():
            c = canon_outline(tt, gname)
            if c:
                index[c].append((fname, cp))
    return {"files": files, "index": dict(index)}


def load_reference() -> dict:
    import pickle
    if os.path.exists(REF_PICKLE):
        with open(REF_PICKLE, "rb") as fh:
            ref = pickle.load(fh)
        if all(sha256_file(os.path.join(FONTS_DIR, f)) == h for f, h in ref["files"].items()):
            return ref
    ref = build_reference()
    os.makedirs(WORK, exist_ok=True)
    with open(REF_PICKLE, "wb") as fh:
        pickle.dump(ref, fh)
    return ref


def reference_version(ref: dict) -> str:
    return hashlib.sha256(json.dumps(sorted(ref["files"].items())).encode()).hexdigest()[:16]


def embedded_truetype(doc, xref):
    """(TTFont, {byte code: glyph name}) of an embedded TrueType simple font, or (None, {})."""
    import io
    from fontTools.ttLib import TTFont
    try:
        _, ext, _, buf = doc.extract_font(xref)
        if not buf or ext != "ttf":
            return None, {}
        tt = TTFont(io.BytesIO(buf), lazy=True)
        by_code = {}
        for t in tt["cmap"].tables:
            for k, g in t.cmap.items():
                code = k - 0xF000 if (t.platformID, t.platEncID) == (3, 0) and k >= 0xF000 else k
                if 0 <= code < 256:
                    by_code.setdefault(code, g)
        return tt, by_code
    except Exception:  # noqa: BLE001 - an unreadable program certifies nothing
        return None, {}


SHAPE_RANGES = ((0x0591, 0x05F4), (0x0600, 0x06FF), (0x0750, 0x077F), (0xFB1D, 0xFB4F), (0xFB50, 0xFDFF),
                (0xFE70, 0xFEFF))


def shape_may_certify(text: str) -> bool:
    """A glyph with no usable name is decoded by its shape alone only into Hebrew or Arabic letters and marks:
    a right-to-left PDF draws a bracket with its mirror image, and some marks share an outline with other
    characters (a dagesh with the dotted circle), so a shape cannot name those."""
    import unicodedata
    # A mark alone stays allowed (2026-10-27): requiring a letter cost 60 accepted rows ~9,000 real vowel points their
    # fonts carry without names; a dot drawn in a lacuna (certified as a dagesh) is held by C2's stray-character check.
    return bool(text) and all(any(lo <= ord(ch) <= hi for lo, hi in SHAPE_RANGES) and not unicodedata.mirrored(ch)
                              for ch in text)


def is_private_use(text: str) -> bool:
    return any(0xE000 <= ord(ch) <= 0xF8FF for ch in text)


def one_character_form(form: str | None) -> str | None:
    """The one character a repaired copy maps a code to for a glyph read by its name: the registry's form when it is
    one character, else the Hebrew presentation form of the pointed letter (normalised back to letter + points after
    extraction, as a shape-certified pointed letter is); ``None`` when there is no such character."""
    import unicodedata
    if not form:
        return None
    if len(form) == 1:
        return form
    nfd = unicodedata.normalize("NFD", form)
    return next((chr(cp) for cp in range(0xFB1D, 0xFB50)
                 if unicodedata.normalize("NFD", chr(cp)) == nfd and chr(cp) != nfd), None)


PLAIN_HEBREW = frozenset(map(chr, [*range(0x05D0, 0x05EB), *range(0x05B0, 0x05BE), 0x05BF, 0x05C1, 0x05C2]))
# variants that are no compatibility form of the letter or point they stand beside in a reference font
HEBREW_VARIANTS = {"ׇ": "ָ", "׆": "נ"}  # qamats qatan -> qamats, nun hafukha -> nun


def plain_of(interps) -> str | None:
    """Tie rule (owner, 2026-10-28): an outline whose readings in the font's typeface are one plain Hebrew letter or
    point and otherwise only its variant forms (a compatibility form of it, a listed variant, a private-use code)
    reads as the plain one. Two plain readings, or any other reading beside it, leave the glyph ambiguous."""
    import unicodedata
    plain = [x for x in interps if x in PLAIN_HEBREW]
    if len(plain) != 1:
        return None
    p = plain[0]
    others = [x for x in interps if x != p]
    ok = all(all(0xE000 <= ord(ch) <= 0xF8FF for ch in o) or unicodedata.normalize("NFKD", o) == p
             or HEBREW_VARIANTS.get(o) == p for o in others)
    return p if ok and others else None


def typefaces(hit_lists) -> frozenset:
    """The font's typeface: the reference family most of its glyphs match. Families that tie are all kept (a glyph
    then has to agree with any of them): choosing one would follow Python's string hashing, which changes from run
    to run (2026-10-27: 76 entries moved between runs)."""
    fam = collections.Counter(REFERENCE_FONTS[f] for hs in hit_lists for f in {f for f, _ in hs})
    top = max(fam.values(), default=0)
    return frozenset(f for f, n in fam.items() if n == top and top)


def certify_font(tt, by_code: dict, codes, names: dict, ref: dict) -> dict:
    """Step 3 for one embedded font: {code: decision}. A decision's ``text`` is set when the glyph's outline is
    certified: it equals a reference glyph in the font's own typeface (the family most of its glyphs match) and
    supports exactly the interpretation the name gives (or, without a usable name, exactly one interpretation, or
    one plain Hebrew letter or point beside only its variant forms: the tie rule). Owner's ruling of 2026-10-28 (the
    name rule): a glyph whose outline matches no reference glyph in its typeface is read by its name when the
    registry reads the name, the reading has no private-use code and fits one character, and any reference glyph the
    outline does match reads the same; every such shape was checked by eye (``_tmp/fgp_shape_review.html``), and
    ``run_decisions`` withdraws the reading where the same outline is certified elsewhere as something else."""
    from scripts.fgp_glyph_registry import form_of, text_of
    hits = {}
    for code in codes:
        gname = by_code.get(code)
        c = canon_outline(tt, gname) if (tt is not None and gname) else None
        hits[code] = (gname, c, ref["index"].get(c, []) if c else [])
    faces = typefaces(hs for _, _, hs in hits.values())
    face = " + ".join(sorted(faces)) or None
    out = {}
    for code in codes:
        gname, c, hs = hits[code]
        name = names.get(code)
        name_text = text_of(form_of(name)) if name else None
        in_face = [(f, cp) for f, cp in hs if REFERENCE_FONTS[f] in faces]
        interps = sorted({text_of(chr(cp)) for _, cp in in_face})
        d = {"name": name, "glyph": gname, "name_text": name_text, "face": face, "interps": interps,
             "refs": sorted({f for f, _ in in_face}), "status": None, "text": None, "form": None,
             "outline": hashlib.sha1(repr(c).encode()).hexdigest()[:16] if c else None}
        if c is None:
            d["status"] = "no glyph"
        elif c == ():
            d["status"] = "empty"
        elif name in (".notdef",):
            d["status"] = "notdef"
        elif not in_face:
            d["status"] = "unmatched" if not hs else "other face"
            one = one_character_form(form_of(name)) if name_text else None
            if one and not is_private_use(name_text) and (not hs or name_text in {text_of(chr(cp)) for _, cp in hs}):
                d["status"], d["text"], d["form"] = "certified by name", name_text, one
        elif name_text is not None:
            if name_text in interps:
                d["status"], d["text"] = "certified", name_text
            else:
                d["status"] = "conflict"
        elif len(interps) == 1 and shape_may_certify(interps[0]):
            d["status"], d["text"] = "certified by shape", interps[0]
        elif len(interps) == 1:
            d["status"] = "shape only, not a letter or mark"
        elif plain_of(interps):
            d["status"], d["text"] = "certified by plain form", plain_of(interps)
        else:
            d["status"] = "ambiguous"
        if d["text"] and not d["form"]:
            # what the repaired copy maps the code to: ONE character, the reference glyph's own code point (a
            # presentation form for a pointed letter or an Arabic shape), so the extractor still sees one glyph; the
            # decoded sequence is normalised to ``text`` after extraction (normalize_presentation)
            d["form"] = chr(min(cp for _, cp in in_face if text_of(chr(cp)) == d["text"]))
        out[code] = d
    return out


def is_presentation(ch: str) -> bool:
    cp = ord(ch)
    return 0xFB1D <= cp <= 0xFB4F or 0xFB50 <= cp <= 0xFDFF or 0xFE70 <= cp <= 0xFEFF


def normalize_presentation(text: str) -> str:
    """Decisions 1-2 on decoded glyphs: a Hebrew presentation form becomes its letter plus points, an Arabic one its
    plain letters. Only presentation-form characters change; every other character is kept as it is."""
    from scripts.fgp_glyph_registry import text_of
    return "".join(text_of(ch) if is_presentation(ch) else ch for ch in text)


DECISIONS_DB = os.path.join(WORK, "decisions.db")
_REF = None


def parse_tounicode(data: bytes) -> dict:
    """{byte code: text} from a ToUnicode CMap's bfchar / bfrange sections (one-byte codes, as simple fonts use)."""
    import re
    s = data.decode("latin-1", "replace")
    out = {}

    def hexstr(h):
        h = h.strip()
        return "".join(chr(int(h[i:i + 4], 16)) for i in range(0, len(h) - len(h) % 4, 4)) if len(h) >= 4 else chr(int(h, 16))

    for block in re.findall(r"beginbfchar(.*?)endbfchar", s, re.S):
        for src, dst in re.findall(r"<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]*)>", block):
            out[int(src, 16)] = hexstr(dst) if dst else ""
    for block in re.findall(r"beginbfrange(.*?)endbfrange", s, re.S):
        for lo, hi, rest in re.findall(r"<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>\s*(\[[^\]]*\]|<[0-9A-Fa-f]*>)", block):
            lo, hi = int(lo, 16), int(hi, 16)
            if rest.startswith("["):
                for i, d in enumerate(re.findall(r"<([0-9A-Fa-f]*)>", rest)):
                    out[lo + i] = hexstr(d)
            else:
                base = rest.strip("<>")
                first = int(base[-4:], 16) if len(base) >= 4 else int(base, 16)
                prefix = hexstr(base[:-4]) if len(base) > 4 else ""
                for i in range(hi - lo + 1):
                    out[lo + i] = prefix + chr(first + i)
    return out


def _decide_task(args):
    """Certify every drawn code of every TrueType simple font of one PDF; read existing ToUnicode maps."""
    sha, rel, fonts = args
    import fitz
    global _REF
    if _REF is None:
        _REF = load_reference()
    rows = []
    try:
        doc = fitz.open(os.path.join(PDF_ROOT, rel))
    except Exception as e:  # noqa: BLE001
        return sha, [], f"{type(e).__name__}: {e}"
    for objid, names, drawn, has_tu in fonts:
        existing = {}
        if has_tu:
            try:
                kind, val = doc.xref_get_key(objid, "ToUnicode")
                if kind == "xref":
                    existing = parse_tounicode(doc.xref_stream(int(val.split()[0])) or b"")
            except Exception:  # noqa: BLE001
                existing = {}
        tt, by_code = embedded_truetype(doc, objid)
        decisions = certify_font(tt, by_code, sorted(drawn), names, _REF)
        for code, d in decisions.items():
            rows.append((sha, objid, code, drawn[code][0], drawn[code][1], d["name"], d["glyph"], d["name_text"],
                         d["face"], json.dumps(d["interps"], ensure_ascii=False), ",".join(d["refs"]), d["status"],
                         d["text"], int(bool(has_tu)), existing.get(code) if has_tu else None,
                         int(code in existing) if has_tu else None, d["outline"], d["form"]))
    return sha, rows, None


def withdraw_contradicted_names(db: sqlite3.Connection) -> int:
    """The name rule's last condition (owner, 2026-10-28): no reading by name where the same outline is certified by
    its shape elsewhere in the corpus as something else. Two fonts that name one outline differently (a holam and a
    sin dot drawn alike) each keep their own name. Returns the number of readings withdrawn."""
    db.execute("CREATE INDEX IF NOT EXISTS decisions_outline ON decisions (outline)")
    return db.execute(
        "UPDATE decisions SET status = 'name contradicts a shape certified elsewhere', text = NULL, form = NULL "
        "WHERE status = 'certified by name' AND EXISTS (SELECT 1 FROM decisions e WHERE e.outline = decisions.outline "
        f"AND e.status IN ({','.join('?' * len(SHAPE_CERTIFIED))}) AND e.text != decisions.text)",
        SHAPE_CERTIFIED).rowcount


def run_decisions(workers: int = 16) -> None:
    """Step 3 over the census: one decision per (PDF, font, drawn code) of every TrueType simple font."""
    from scripts.fgp_glyph_registry import REGISTRY_VERSION
    ref = load_reference()
    cen = ro(CENSUS_DB)
    rel_of = {}
    for rel, sha in cen.execute("SELECT rel_path, sha256 FROM pdfs ORDER BY rel_path"):
        rel_of.setdefault(sha, rel)
    fonts = collections.defaultdict(dict)
    for sha, objid, diffs, tu in cen.execute(
            "SELECT sha256, objid, differences, has_tounicode FROM fonts WHERE subtype = 'TrueType'"):
        fonts[sha][objid] = ({int(k): v for k, v in json.loads(diffs).items()}, {}, tu)
    for sha, objid, code, n, std in cen.execute("SELECT sha256, objid, code, n, std FROM drawn"):
        if objid in fonts.get(sha, {}):
            fonts[sha][objid][1][code] = (n, std)
    tasks = [(sha, rel_of[sha], [(o, nm, dr, tu) for o, (nm, dr, tu) in f.items() if dr]) for sha, f in fonts.items()]
    if os.path.exists(DECISIONS_DB):
        os.remove(DECISIONS_DB)  # our own working file
    db = sqlite3.connect(DECISIONS_DB)
    db.executescript("""
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE decisions (sha256 TEXT, objid INTEGER, code INTEGER, n INTEGER, std TEXT, name TEXT, glyph TEXT,
            name_text TEXT, face TEXT, interps TEXT, refs TEXT, status TEXT, text TEXT, has_tounicode INTEGER,
            existing TEXT, in_existing INTEGER, outline TEXT, form TEXT, PRIMARY KEY (sha256, objid, code));
    """)
    db.executemany("INSERT INTO meta VALUES (?, ?)", [
        ("registry_version", REGISTRY_VERSION), ("reference_version", reference_version(ref)),
        ("reference_files", json.dumps(ref["files"])), ("created", time.strftime("%Y-%m-%d %H:%M:%S"))])
    t0, errors = time.time(), 0
    with _pool(workers, len(tasks)) as pool:
        for k, (sha, rows, err) in enumerate(pool.imap_unordered(_decide_task, tasks, chunksize=8), 1):
            errors += bool(err)
            db.executemany("INSERT INTO decisions VALUES (" + ",".join("?" * 18) + ")", rows)
            if k % 4000 == 0:
                db.commit()
                print(f"  {k:,} / {len(tasks):,} PDFs, {time.time() - t0:.0f}s", flush=True)
    db.commit()
    db.execute("INSERT INTO meta VALUES ('errors', ?)", (str(errors),))
    withdrawn = withdraw_contradicted_names(db)
    db.execute("INSERT INTO meta VALUES ('name_readings_withdrawn', ?)", (str(withdrawn),))
    db.commit()
    print(f"done: {len(tasks):,} PDFs with TrueType simple fonts, {errors} unreadable, {withdrawn} name readings "
          f"withdrawn, {time.time() - t0:.0f}s")


# --- step 2: the repaired copy ---------------------------------------------------------------------------------------

COPIES = os.path.join(WORK, "copies")
SHAPE_CERTIFIED = ("certified", "certified by shape", "certified by plain form")
CERTIFIED = SHAPE_CERTIFIED + ("certified by name",)


def font_maps(sha: str, dec: sqlite3.Connection) -> tuple[dict, list]:
    """({font objid: {code: text}}, conflicts) for one PDF. A font without /ToUnicode gets an entry for every drawn
    code whose certified text the standard decoding does not already give; a font with a map keeps every entry and
    gains only certified codes it lacks; an existing entry that disagrees with a certified glyph is a conflict."""
    from scripts.fgp_glyph_registry import text_of
    maps, conflicts = collections.defaultdict(dict), []
    for objid, code, std, status, text, has_tu, existing, in_existing, form in dec.execute(
            "SELECT objid, code, std, status, text, has_tounicode, existing, in_existing, form FROM decisions "
            "WHERE sha256 = ?", (sha,)):
        if status not in CERTIFIED or not text:
            continue
        if has_tu and in_existing:
            # the producer's own map is kept; only a glyph certified by name AND shape can contradict it
            if status == "certified" and text_of(existing) != text and MIRROR.get(text) != text_of(existing):
                conflicts.append({"objid": objid, "code": code, "existing": existing, "certified": text})
            continue
        if std is not None and text_of(std) == text:
            continue
        maps[objid][code] = form
    return dict(maps), conflicts


def tounicode_cmap(mapping: dict) -> bytes:
    lines = ["/CIDInit /ProcSet findresource begin", "12 dict begin", "begincmap",
             "/CIDSystemInfo << /Registry (Adobe) /Ordering (UCS) /Supplement 0 >> def",
             "/CMapName /Adobe-Identity-UCS def", "/CMapType 2 def",
             "1 begincodespacerange", "<00> <FF>", "endcodespacerange"]
    items = sorted(mapping.items())
    for i in range(0, len(items), 100):
        chunk = items[i:i + 100]
        lines.append(f"{len(chunk)} beginbfchar")
        lines += [f"<{code:02X}> <{''.join(f'{ord(c):04X}' for c in text)}>" for code, text in chunk]
        lines.append("endbfchar")
    lines += ["endcmap", "CMapName currentdict /CIDResource defineresource pop", "end", "end"]
    return "\n".join(lines).encode("ascii")


def write_repaired(src: str, dst: str, maps: dict) -> None:
    """Copy ``src`` to ``dst`` giving each font in ``maps`` a /ToUnicode (merged with any it already has)."""
    import fitz
    doc = fitz.open(src)
    for objid, mapping in maps.items():
        merged = {}
        kind, val = doc.xref_get_key(objid, "ToUnicode")
        if kind == "xref":
            merged = parse_tounicode(doc.xref_stream(int(val.split()[0])) or b"")
        merged.update(mapping)
        cm = doc.get_new_xref()
        doc.update_object(cm, "<<>>")
        doc.update_stream(cm, tounicode_cmap(merged))
        doc.xref_set_key(objid, "ToUnicode", f"{cm} 0 R")
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    doc.save(dst)
    doc.close()


# --- step 4: traced extraction -----------------------------------------------------------------------------------------

def extractor_version() -> str:
    import fitz
    h = hashlib.sha256(fitz.VersionBind.encode())
    for rel in ("shared/local_indexer.py", "shared/local_indexer_rtl.py", "ephraim_meiri_pdf_converter/pdf_to_docx.py",
                "scripts/fgp_rebuild_text_and_credit.py"):
        h.update(open(os.path.join(REPO, rel), "rb").read())
    return h.hexdigest()[:16]


def code_version() -> str:
    """The re-read's own code and registry (C7): any change to them changes the digest."""
    h = hashlib.sha256()
    for rel in ("scripts/fgp_glyph_reread.py", "scripts/fgp_glyph_registry.py"):
        h.update(open(os.path.join(REPO, rel), "rb").read())
    return h.hexdigest()[:16]


def libraries() -> str:
    """The libraries that read the fonts and the Unicode tables the decisions rest on (C7)."""
    import unicodedata

    import fontTools
    import pdfminer
    return json.dumps({"pdfminer": getattr(pdfminer, "__version__", "?"), "fontTools": fontTools.version,
                       "unicode": unicodedata.unidata_version}, sort_keys=True)


def _traced_page(page, page_num: int, decoded_from: dict | None = None) -> tuple[str, str, list, bool]:
    """``shared.local_indexer._extract_one_page_rawdict`` step for step, calling the same helpers, with every glyph
    tagged by its position (page, origin, font, size) so each output row can be tied to the glyphs it was made of.

    ``decoded_from`` ({glyph id: character} of the untouched reading) marks the decoded glyphs of a repaired
    reading: in a row holding one, presentation forms are normalised (decisions 1-2) after the row is built, unless
    the row's other glyphs already carry presentation forms, which the row then reports instead.
    Returns (page text as the extractor makes it, normalised page text, rows, whether the LTR guard replaced it)."""
    from shared import local_indexer as li
    page_dict = page.get_text("rawdict", flags=li._RAWDICT_FLAGS)
    seen: collections.Counter = collections.Counter()
    for blk in page_dict.get("blocks", []):
        for line in blk.get("lines", []):
            for sp in line.get("spans", []):
                for ch in sp.get("chars", []):
                    bb = ch.get("bbox", (0, 0, 0, 0))
                    key = (page_num, round(ch["origin"][0], 2), round(ch["origin"][1], 2), sp.get("font", ""),
                           round(sp.get("size", 0.0), 2), round(bb[0], 1), round(bb[1], 1), round(bb[2], 1),
                           round(bb[3], 1))
                    seen[key] += 1
                    ch["_gid"] = "|".join(map(str, key + (seen[key],)))
    try:
        from ephraim_meiri_pdf_converter.pdf_to_docx import _attach_nikud_page
        _attach_nikud_page(page_dict)
    except Exception:  # noqa: BLE001 - as the extractor does
        pass
    rows, block_texts, block_norm, span_counter = [], [], [], 0
    for bi, blk in enumerate(page_dict.get("blocks", [])):
        if blk.get("type") != 0 or not blk.get("lines", []):
            continue
        row_texts, row_norm, glyph_order = [], [], 0
        for ri, row_line_dicts in enumerate(li.group_lines_by_baseline(blk.get("lines", []))):
            glyphs = []
            for line_dict in row_line_dicts:
                for sp in line_dict.get("spans", []):
                    sp_id = span_counter
                    span_counter += 1
                    for ch in sp.get("chars", []):
                        glyphs.append({"c": ch.get("c", ""), "bbox": list(ch.get("bbox", [0.0, 0.0, 0.0, 0.0])),
                                       "font": sp.get("font", ""), "size": sp.get("size", 0.0), "span_id": sp_id,
                                       "original_order": glyph_order, "_gid": ch.get("_gid")})
                        glyph_order += 1
            if not glyphs:
                continue
            line_text_raw = "".join(g["c"] for g in glyphs)
            row_rtl = li.rtl_ratio(line_text_raw) > 0.4
            if not row_rtl:
                out = line_text_raw.strip()
            else:
                units = li.despace_line_to_word_units(glyphs)
                units = li.reorder_word_units_rtl(units, line_text_raw)
                out = li.normalize_punctuation_spacing(li.fix_visual_brackets_rtl(li.line_text_from_word_units(units)))
                out = out if out.strip() else ""
            norm, native = out, False
            if decoded_from is not None:
                dchars = "".join(g["c"] for g in glyphs if decoded_from.get(g["_gid"]) != g["c"])
                other = "".join(g["c"] for g in glyphs if decoded_from.get(g["_gid"]) == g["c"])
                if any(is_presentation(ch) for ch in dchars):
                    native = any(is_presentation(ch) for ch in other)
                    norm = out if native else normalize_presentation(out)
            if out:
                row_texts.append(out)
                row_norm.append(norm)
            rows.append({"page": page_num, "block": bi, "row": ri, "rtl": row_rtl, "text": out, "norm": norm,
                         "native_presentation": native, "glyphs": [(g["_gid"], g["c"]) for g in glyphs]})
        if row_texts:
            block_texts.append("\n".join(row_texts))
            block_norm.append("\n".join(row_norm))
    rawdict_text = "\n\n".join(t for t in block_texts if t.strip())
    final = li._ltr_damage_guard(rawdict_text, li._extract_blocks_text(page))
    guarded = final != rawdict_text.strip()
    final_norm = final if guarded else "\n\n".join(t for t in block_norm if t.strip()).strip()
    return final, final_norm, rows, guarded


def traced_extract(path: str, decoded_from: dict | None = None) -> dict:
    """The stored-text recipe (``fgp_rebuild_text_and_credit``: extract_pdf_pages, pages joined by a blank line,
    stripped, then ``_collapse_blanks``) with row tracing; also runs the real extractor and records whether the
    traced copy reproduces it exactly. ``text`` is the extractor's text; ``norm`` the same after the presentation
    forms of decoded glyphs are normalised (equal to ``text`` for an untouched reading)."""
    import fitz
    from shared import local_indexer as li
    from scripts.fgp_rebuild_text_and_credit import _collapse_blanks
    doc = fitz.open(path)
    pages, norms, rows, guard, skipped, errors = [], [], [], [], [], []
    for page_num, page in enumerate(doc, start=1):
        try:
            text, norm, prow, guarded = _traced_page(page, page_num, decoded_from)
        except Exception as e:  # noqa: BLE001 - the extractor skips a failing page too
            errors.append(f"page {page_num}: {type(e).__name__}")
            continue
        if len(text.strip()) < li._EMPTY_PAGE_CHAR_THRESHOLD:
            skipped.append(page_num)
            continue
        pages.append(text)
        norms.append((page_num, norm))
        rows += prow
        if guarded:
            guard.append(page_num)
    doc.close()
    joined = "\n\n".join(pages).strip()
    real = "\n\n".join(t for _, t, _ in li.extract_pdf_pages(path)).strip()
    return {"text": _collapse_blanks(joined), "norm": _collapse_blanks("\n\n".join(n for _, n in norms).strip()),
            "page_norms": norms, "faithful": joined == real, "rows": rows, "guard": guard, "skipped": skipped,
            "errors": errors}


def reassemble(reading: dict) -> str:
    """The normalised text rebuilt from its rows exactly as ``_traced_page`` and ``traced_extract`` join them (a
    page the LTR guard replaced keeps its own text)."""
    from scripts.fgp_rebuild_text_and_credit import _collapse_blanks
    by_page = collections.defaultdict(lambda: collections.defaultdict(list))
    for row in reading["rows"]:
        if row["text"]:
            by_page[row["page"]][row["block"]].append(row["norm"])
    pages = []
    for page_num, norm in reading["page_norms"]:
        if page_num in reading["guard"]:
            pages.append(norm)
            continue
        blocks = ["\n".join(rows) for _, rows in sorted(by_page[page_num].items())]
        pages.append("\n\n".join(t for t in blocks if t.strip()).strip())
    return _collapse_blanks("\n\n".join(pages).strip())


_RANGE_SIDE = r"[0-9A-Za-z][0-9A-Za-z./,:]*"


def repair_ranges(u: dict, r: dict) -> int:
    """Owner's request (2026-10-27): a hyphen that comes back between two numbers makes the RTL reorder reverse the
    range (the untouched reading's ``12\\x1625`` becomes ``25-12``). Where the untouched row reads ``A<code>B``
    with digits on both sides and the repaired row has ``B-A`` but not ``A-B``, the repaired row gets ``A-B``: the
    order the untouched reading already had. Changes the repaired reading in place; returns the count."""
    import re
    uc = {g: c for row in u["rows"] for g, c in row["glyphs"]}
    rc = {g: c for row in r["rows"] for g, c in row["glyphs"]}
    d = {g for g in set(uc) | set(rc) if uc.get(g) != rc.get(g)}
    key = lambda row: frozenset(g for g, _ in row["glyphs"] if g not in d)  # noqa: E731
    umap = {key(row): row for row in u["rows"]}
    fixed = 0
    for rr in r["rows"]:
        codes = {uc[g] for g, c in rr["glyphs"] if g in d and c == "-" and uc.get(g)}
        ur = umap.get(key(rr))
        if not codes or ur is None:
            continue
        cls = "[" + "".join(re.escape(c) for c in codes) + "]+"
        for m in re.finditer(f"({_RANGE_SIDE})({cls})({_RANGE_SIDE})", ur["text"]):
            a, sep, b = m.groups()
            if not (any(ch.isdigit() for ch in a) and any(ch.isdigit() for ch in b)):
                continue
            want, reversed_ = a + "-" * len(sep) + b, b + "-" * len(sep) + a
            if reversed_ in rr["text"] and want not in rr["text"]:
                rr["text"] = rr["text"].replace(reversed_, want, 1)
                rr["norm"] = rr["norm"].replace(reversed_, want, 1)
                fixed += 1
    if fixed:
        r["norm"] = reassemble(r)
    return fixed


# --- checks C1, C2 ---------------------------------------------------------------------------------------------------

MIRROR = {"(": ")", ")": "(", "[": "]", "]": "[", "{": "}", "}": "{", "<": ">", ">": "<"}


def _classes(chars: str) -> collections.Counter:
    """Character counts with whitespace dropped and each bracket pair counted as one class (the RTL bracket fix
    swaps them)."""
    out: collections.Counter = collections.Counter()
    for ch in chars:
        if ch.isspace():
            continue
        out[min(ch, MIRROR[ch]) if ch in MIRROR else ch] += 1
    return out


def _moved_kind(ur: dict, rr: dict, resid) -> str:
    """Which reconstruction step moved the other characters, for review by pattern."""
    if ur["rtl"] != rr["rtl"]:
        return "the row changed direction"
    a, b = resid(ur["text"]), resid(rr["text"])
    no_br = lambda t: "".join(ch for ch in t if ch not in MIRROR)  # noqa: E731
    if no_br(a) == no_br(b):
        return "brackets re-paired"
    rtl_only = lambda t: "".join(ch for ch in t if not (ch.isdigit() or ("a" <= ch.lower() <= "z")))  # noqa: E731
    if rtl_only(no_br(a)) == rtl_only(no_br(b)):
        return "digits or Latin reordered"
    return "Hebrew or Arabic reordered"


def c2_check(u: dict, r: dict) -> dict:
    """C2 per occurrence (plan, 'Checks'): D = glyph positions whose character differs between the untouched (U) and
    repaired (R) readings or that exist in only one. Rows without D must come out identical; in a row with D, the
    character counts must change by exactly what D's glyphs change (whitespace aside, bracket pairs as one class),
    and the other characters must keep their order once whitespace and D's character classes are set aside.
    Spaces may change only inside a row that holds a D glyph (the extractor's word spacing is decided per row).
    Returns {'d': n, 'reasons': [...], 'rows_changed': n, 'blind_spot': n}."""
    uc = {gid: c for row in u["rows"] for gid, c in row["glyphs"]}
    rc = {gid: c for row in r["rows"] for gid, c in row["glyphs"]}
    d = {g for g in set(uc) | set(rc) if uc.get(g) != rc.get(g)}
    reasons: list = []
    if u["skipped"] != r["skipped"]:
        reasons.append("pages skipped differently")
    if set(u["guard"]) | set(r["guard"]):
        guarded = set(u["guard"]) | set(r["guard"])
        if any(int(g.split("|")[0]) in guarded for g in d):
            reasons.append("a page with a decoded glyph fell back to the blocks text")

    def key(row):
        return frozenset(g for g, _ in row["glyphs"] if g not in d)

    umap = {key(row): row for row in u["rows"]}
    rmap = {key(row): row for row in r["rows"]}
    if set(umap) != set(rmap):
        reasons.append("rows grouped differently")
    if any(row.get("native_presentation") for row in r["rows"]):
        reasons.append("a row mixes decoded and native presentation forms")
    changed = blind = 0
    for k, ur in umap.items():
        rr = rmap.get(k)
        if rr is None:
            continue
        dg = [g for g, _ in ur["glyphs"] if g in d] + [g for g, _ in rr["glyphs"] if g in d and g not in uc]
        if not dg:
            if ur["text"] != rr["text"]:
                reasons.append(f"row without a decoded glyph changed (page {ur['page']})")
            continue
        changed += ur["text"] != rr["text"]
        expect = _classes("".join(rc.get(g, "") for g in set(dg)))
        expect.subtract(_classes("".join(uc.get(g, "") for g in set(dg))))
        got = _classes(rr["text"])
        got.subtract(_classes(ur["text"]))
        if {k2: v for k2, v in got.items() if v} != {k2: v for k2, v in expect.items() if v}:
            reasons.append(f"characters changed beyond the decoded glyphs (page {ur['page']})")
            continue
        kset = set()
        for g in dg:
            for ch in uc.get(g, "") + rc.get(g, ""):
                kset |= {ch, MIRROR.get(ch, ch)}
        resid = lambda t: "".join(ch for ch in t if not ch.isspace() and ch not in kset)  # noqa: E731
        blind += sum(1 for ch in ur["text"] if ch in kset)
        if resid(ur["text"]) != resid(rr["text"]):
            reasons.append(f"other characters moved: {_moved_kind(ur, rr, resid)} (page {ur['page']})")
    return {"d": len(d), "reasons": sorted(set(reasons)), "rows_changed": changed, "blind_spot": blind}


# --- check C3: against the Word or line text --------------------------------------------------------------------------

EDGE = "[](){}<>.,:;!?\"'״׳|/\\*#$%^&+=~`…"


def norm_token(t: str) -> str:
    """Declared C3 normalisation: canonical decomposition, edge punctuation and sigla brackets set aside."""
    import unicodedata
    return unicodedata.normalize("NFD", t).strip(EDGE)


def c3_compare(u_text: str, r_text: str, w_text: str) -> dict:
    """Tokens the re-read changed (U -> R), aligned to the Word/line text; weights are changed characters."""
    import difflib
    ut, rt, wt = u_text.split(), r_text.split(), w_text.split()
    changed = {}
    for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(None, ut, rt, autojunk=False).get_opcodes():
        if tag == "equal":
            continue
        for j in range(j1, j2):
            u_tok = " ".join(ut[i1:i2]) if (i2 - i1) != (j2 - j1) else ut[i1 + (j - j1)]
            sm = difflib.SequenceMatcher(None, u_tok, rt[j], autojunk=False)
            changed[j] = (u_tok, max(1, sum(j2_ - j1_ for t, _, _, j1_, j2_ in sm.get_opcodes() if t != "equal")))
    rn, wn = [norm_token(t) for t in rt], [norm_token(t) for t in wt]
    to_w = {}
    for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(None, rn, wn, autojunk=False).get_opcodes():
        if tag == "equal" or (tag == "replace" and i2 - i1 == j2 - j1):
            for k in range(i2 - i1):
                to_w[i1 + k] = j1 + k
    out = {"weight": 0, "judged": 0, "agree": 0, "decode_errors": [], "texts_differ": 0}
    for j, (u_tok, w) in changed.items():
        out["weight"] += w
        if j not in to_w:
            continue
        out["judged"] += w
        r_tok, w_tok = rt[j], wt[to_w[j]]
        if norm_token(r_tok) == norm_token(w_tok):
            out["agree"] += w
            continue
        # a decoding error: R and the Word text differ only where the re-read changed the token
        ru = {k for t, a1, a2, _, _ in difflib.SequenceMatcher(None, r_tok, u_tok, autojunk=False).get_opcodes()
              if t != "equal" for k in range(a1, max(a2, a1 + 1))}
        rw = {k for t, a1, a2, _, _ in difflib.SequenceMatcher(None, r_tok, w_tok, autojunk=False).get_opcodes()
              if t != "equal" for k in range(a1, max(a2, a1 + 1))}
        if rw and rw <= ru:
            out["decode_errors"].append((u_tok, r_tok, w_tok))
        else:
            out["texts_differ"] += w
    return out


# --- step 5: measurement ---------------------------------------------------------------------------------------------

MEASURE_DB = os.path.join(WORK, "measure.db")


def _measure_task(task):
    """Read one PDF untouched (U) and repaired (R); judge every row that uses it."""
    import tempfile
    sha, rel, maps, conflicts, scope, bench, tmpdir = task
    src = os.path.join(PDF_ROOT, rel)
    res = {"sha": sha, "rows": [], "error": None}
    try:
        u = traced_extract(src)
        uc = {g: c for row in u["rows"] for g, c in row["glyphs"]}
        fd, dst = tempfile.mkstemp(suffix=".pdf", dir=tmpdir)
        os.close(fd)
        try:
            write_repaired(src, dst, {int(k): {int(c): t for c, t in m.items()} for k, m in maps.items()})
            r = traced_extract(dst, decoded_from=uc)
        finally:
            os.remove(dst)  # our own scratch copy
    except Exception as e:  # noqa: BLE001
        res["error"] = f"{type(e).__name__}: {e}"[:300]
        return res
    reassembled_ok = reassemble(r) == r["norm"]
    ranges_fixed = repair_ranges(u, r) if reassembled_ok else 0
    c2 = c2_check(u, r)
    if not reassembled_ok:
        c2["reasons"] = sorted(set(c2["reasons"]) | {"rows do not reassemble into the reading"})
    injected = {ch for m in maps.values() for t in m.values() for ch in t}
    rc = {g: c for row in r["rows"] for g, c in row["glyphs"]}
    # a character the injected map cannot have produced; a bracket MuPDF merely mirrored (its bidi context changed
    # next to a decoded letter) is the same glyph, not a stray one
    stray = sorted({ch for g, c in rc.items() if uc.get(g) != c and MIRROR.get(uc.get(g) or "") != c
                    for ch in c if ch not in injected and not ch.isspace()})
    base = {"faithful": u["faithful"] and r["faithful"], "c2": c2, "stray": stray, "u_errors": u["errors"],
            "ranges_fixed": ranges_fixed}
    # C3 first: a decoding error found on this PDF's Word-text row holds every kept row that reads the same PDF (plan,
    # C3: "every row with an identified decoding error is held"; Codex review of step 6)
    judged_bench = [(row, c3_compare(u["text"], r["norm"], row["word"])) for row in bench]
    pdf_decode_error = any(c["decode_errors"] for _, c in judged_bench)
    for row in scope:
        rid, s0, s, vocab = row["id"], row["s0"], row["s"], row["vocab"]
        reasons = list(c2["reasons"])
        if pdf_decode_error:
            reasons.append("C3: a decoding error on this PDF's Word-text row")
        if not base["faithful"]:
            reasons.append("traced extraction differs from the extractor")
        if u["text"] != s0:
            reasons.append("C1: the extractor no longer reproduces the stored text")
        if conflicts:
            reasons.append("an existing Unicode map disagrees with a certified glyph")
        if stray:
            reasons.append("a decoded glyph produced an uncertified character")
        c6 = collections.Counter()
        for code_ch, letter in (vocab or {}).items():
            outs = collections.Counter(rc.get(g, "") for g, c in uc.items() if c == code_ch)
            if not outs:
                continue
            if all(o == letter for o in outs):
                c6["agree"] += 1
            elif any(o == code_ch for o in outs):
                c6["left undecoded"] += 1
                reasons.append("C6: the re-read leaves a glyph the vocabulary repair had filled")
            else:
                c6["names differ"] += 1
        decision = "no change" if r["norm"] == u["text"] and not reasons else ("held" if reasons else "accepted")
        res["rows"].append({"kind": "scope", "id": rid, "decision": decision, "reasons": sorted(set(reasons)),
                            "u": u["text"], "r": r["norm"], "c6": dict(c6), **base})
    for row, c3 in judged_bench:
        res["rows"].append({"kind": "bench", "id": row["id"], "c3": c3,
                            "held": bool(c2["reasons"] or stray or conflicts or not base["faithful"] or c3["decode_errors"]),
                            **base})
    return res


def run_measure(workers: int = 16, limit: int | None = None) -> None:
    import tempfile
    from scripts.fgp_glyph_registry import REGISTRY_VERSION
    t0 = time.time()
    cen, dec = ro(CENSUS_DB), ro(DECISIONS_DB)
    sha_of = dict(cen.execute("SELECT rel_path, sha256 FROM pdfs"))
    vocab = vocab_maps()
    by_sha = collections.defaultdict(lambda: {"rel": None, "scope": [], "bench": []})
    scope = scope_rows()
    held_original = [r for r in scope if r["text_source"].startswith("original")]
    for r in scope:
        if r["text_source"].startswith("original"):
            continue
        sha = sha_of[r["rel_path"]]
        by_sha[sha]["rel"] = r["rel_path"]
        by_sha[sha]["scope"].append({"id": r["id"], "s0": r["s0"], "s": r["s"], "vocab": vocab.get(r["id"])})
    for b in benchmark_rows():
        sha = sha_of[b["rel_path"]]
        by_sha[sha]["rel"] = by_sha[sha]["rel"] or b["rel_path"]
        by_sha[sha]["bench"].append({"id": b["id"], "word": b["word_text"]})
    tasks, unchanged = [], collections.Counter()
    tmpdir = tempfile.mkdtemp(prefix="fgp_reread_", dir=WORK)
    for sha, v in sorted(by_sha.items(), key=lambda kv: kv[0] or ""):
        maps, conflicts = font_maps(sha, dec) if sha else ({}, [])  # an unreadable PDF has nothing to decode
        if not maps and not conflicts:
            unchanged["scope"] += len(v["scope"])
            unchanged["bench"] += len(v["bench"])
            continue
        tasks.append((sha, v["rel"], {str(k): {str(c): t for c, t in m.items()} for k, m in maps.items()}, conflicts,
                      v["scope"], v["bench"], tmpdir))
    tasks = tasks[:limit] if limit else tasks
    print(f"PDFs to re-read {len(tasks):,}; rows with nothing to decode: {dict(unchanged)}; "
          f"original rows held (decision 4): {len(held_original)}", flush=True)
    if os.path.exists(MEASURE_DB):
        os.remove(MEASURE_DB)  # our own working file
    db = sqlite3.connect(MEASURE_DB)
    db.executescript("""
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE scope (id INTEGER PRIMARY KEY, sha256 TEXT, decision TEXT, reasons TEXT, u TEXT, r TEXT,
                            faithful INTEGER, d INTEGER, rows_changed INTEGER, blind_spot INTEGER, c6 TEXT, stray TEXT);
        CREATE TABLE bench (id INTEGER PRIMARY KEY, sha256 TEXT, held INTEGER, weight INTEGER, judged INTEGER,
                            agree INTEGER, texts_differ INTEGER, decode_errors TEXT, d INTEGER, faithful INTEGER);
        CREATE TABLE errors (sha256 TEXT, error TEXT);
    """)
    db.executemany("INSERT INTO meta VALUES (?, ?)", [
        ("registry_version", REGISTRY_VERSION), ("extractor_version", extractor_version()),
        ("reference_version", dec.execute("SELECT value FROM meta WHERE key='reference_version'").fetchone()[0]),
        ("code_version", code_version()), ("libraries", libraries()),
        ("unchanged_scope", str(unchanged["scope"])), ("unchanged_bench", str(unchanged["bench"])),
        ("held_original", json.dumps([r["id"] for r in held_original])), ("started", time.strftime("%Y-%m-%d %H:%M:%S"))])
    with _pool(workers, len(tasks), maxtasksperchild=50) as pool:
        for k, res in enumerate(pool.imap_unordered(_measure_task, tasks, chunksize=2), 1):
            if res["error"]:
                db.execute("INSERT INTO errors VALUES (?, ?)", (res["sha"], res["error"]))
            for row in res["rows"]:
                if row["kind"] == "scope":
                    db.execute("INSERT INTO scope VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                               (row["id"], res["sha"], row["decision"], json.dumps(row["reasons"]), row["u"], row["r"],
                                int(row["faithful"]), row["c2"]["d"], row["c2"]["rows_changed"],
                                row["c2"]["blind_spot"], json.dumps(row["c6"]), json.dumps(row["stray"])))
                else:
                    c3 = row["c3"]
                    db.execute("INSERT INTO bench VALUES (?,?,?,?,?,?,?,?,?,?)",
                               (row["id"], res["sha"], int(row["held"]), c3["weight"], c3["judged"], c3["agree"],
                                c3["texts_differ"], json.dumps(c3["decode_errors"][:20], ensure_ascii=False),
                                row["c2"]["d"], int(row["faithful"])))
            if k % 1000 == 0:
                db.commit()
                print(f"  {k:,} / {len(tasks):,} PDFs, {time.time() - t0:.0f}s", flush=True)
    db.commit()
    try:
        os.rmdir(tmpdir)
    except OSError:
        pass
    print(f"done in {time.time() - t0:.0f}s")


# --- summary: C3-C7 --------------------------------------------------------------------------------------------------

C3_MIN_JUDGED, C3_MIN_AGREE = 0.90, 0.995      # plan revision 3
C5_MIN_RECOVERED, C5_MAX_UNRESOLVED = 0.50, 0.25  # rules section 9, fixed before the measured run
CENSUS_DENOMINATOR = 79_030                        # Hebrew letters missing in the candidate text when declared


def c5_verdict(c: dict, denominator: int) -> dict:
    rec = c["recovered"] / denominator if denominator else 0.0
    unr = c["unresolved"] / denominator if denominator else 1.0
    return {"recovered_share": rec, "unresolved_share": unr,
            "passed": bool(c["recovered"]) and rec >= C5_MIN_RECOVERED and unr <= C5_MAX_UNRESOLVED}


def _hebrew_counts(text: str) -> collections.Counter:
    import unicodedata
    return collections.Counter(ch for ch in unicodedata.normalize("NFKD", text or "") if "א" <= ch <= "ת")


def pdf_letters(cen, dec) -> dict:
    """{sha: Counter of the Hebrew letters each PDF draws}: a certified decision's text, else the registry's text for
    the glyph's name, else the standard decoding (the census method)."""
    from scripts.fgp_glyph_registry import text_of_name
    certified = {(s, o, c): t for s, o, c, t in dec.execute(
        "SELECT sha256, objid, code, text FROM decisions WHERE text IS NOT NULL")}
    names = {}
    for s, o, diffs in cen.execute("SELECT sha256, objid, differences FROM fonts"):
        names[(s, o)] = {int(k): v for k, v in json.loads(diffs).items()}
    out = collections.defaultdict(collections.Counter)
    for s, o, c, n, std in cen.execute("SELECT sha256, objid, code, n, std FROM drawn"):
        t = certified.get((s, o, c)) or text_of_name(names.get((s, o), {}).get(c)) or std or ""
        for ch, k in _hebrew_counts(t).items():
            out[s][ch] += k * n
    return out


def _deficit(pdf: collections.Counter, text: str) -> int:
    have = _hebrew_counts(text)
    return sum(max(0, n - have.get(ch, 0)) for ch, n in pdf.items())


# --- owner-approved exceptions (rules section 9, C2) ---------------------------------------------------------------

EXCEPTIONS_JSON = os.path.join(WORK, "exceptions.json")
C6_REASON = "C6: the re-read leaves a glyph the vocabulary repair had filled"
# Owner, 2026-10-27, after the per-pattern review (_tmp/fgp_glyph_reread_patterns.html): these patterns may ship as
# recorded exceptions; digits/Latin reordered stays held.
APPROVED_PATTERNS = (
    "other characters moved: brackets re-paired",
    "rows grouped differently",
    "a row mixes decoded and native presentation forms",
    "other characters moved: Hebrew or Arabic reordered",
    # owner, 2026-10-27, after the page of all 83 rows (_tmp/fgp_glyph_reread_stray.html): "not perfect but good enough"
    "a decoded glyph produced an uncertified character",
    # owner, 2026-10-28, after the page of all 83 rows (_tmp/fgp_direction_review.html): decoded Arabic letters turn a
    # line of boxes and symbols right-to-left, which is the correct reading
    "other characters moved: the row changed direction",
    C6_REASON,
)


def vocabulary_letters_lost(r_text: str, vocab_map: dict, vocab) -> list[str]:
    """The owner's C6 exception excludes a row that loses CORRECT vocabulary letters: a word the re-read leaves with
    a box where the vocabulary repair put a final letter, and which with that letter is a word attested in the clean
    FGP texts (the vocabulary gate 3 used)."""
    from scripts import fgp_final_letters as fl
    lost = []
    for tok in fl.WORD_RE.findall(r_text or ""):
        for code, letter in vocab_map.items():
            if code in tok:
                w = tok.replace(code, letter)
                # a word, as the vocabulary repair voted: a stem of two letters or more (a lone letter is not one)
                if not fl.CODE_RE.search(w) and len(w) - 1 >= fl.MIN_STEM and w in vocab:
                    lost.append(w)
    return lost


def build_exceptions() -> dict:
    """One exception per held row whose every hold pattern is approved, tied to its untouched and re-read texts."""
    from scripts import fgp_final_letters as fl
    m = ro(MEASURE_DB)
    vmaps = vocab_maps()
    vocab = None
    rows, excluded = [], []
    for rid, reasons, u, r in m.execute("SELECT id, reasons, u, r FROM scope WHERE decision = 'held' ORDER BY id"):
        pats = {x.split(" (page")[0] for x in json.loads(reasons)}
        if not pats or not pats <= set(APPROVED_PATTERNS):
            continue
        if C6_REASON in pats:
            if vocab is None:
                vocab = fl.build_vocabulary(vocabulary_texts())
            lost = vocabulary_letters_lost(r, vmaps.get(rid, {}), vocab)
            if lost:
                excluded.append({"id": rid, "lost_words": sorted(set(lost))[:20], "n": len(lost)})
                continue
        rows.append({"id": rid, "patterns": sorted(pats), "u_sha256": hashlib.sha256(u.encode()).hexdigest(),
                     "r_sha256": hashlib.sha256(r.encode()).hexdigest()})
    doc = {"approved": "2026-10-27", "by": "owner",
           "rule": "every hold pattern of the row is approved; a C6 row is excluded when the re-read leaves a box where "
                   "the vocabulary repair made a word of three letters or more attested in the clean FGP texts",
           "patterns": list(APPROVED_PATTERNS), "rows": rows, "excluded_c6_rows": excluded}
    os.makedirs(WORK, exist_ok=True)
    with open(EXCEPTIONS_JSON, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=1)
    return doc


def load_exceptions() -> dict:
    """{row id: (u_sha256, r_sha256)} of the recorded exceptions; empty when none were recorded."""
    if not os.path.exists(EXCEPTIONS_JSON):
        return {}
    with open(EXCEPTIONS_JSON, encoding="utf-8") as fh:
        return {r["id"]: (r["u_sha256"], r["r_sha256"]) for r in json.load(fh)["rows"]}


def row_decisions() -> list[dict]:
    """Every row in scope with what it ships: an accepted row (or a recorded exception) its re-read text R, any
    other row S unchanged (plan: 'Texts kept per row')."""
    m = ro(MEASURE_DB)
    meta = dict(m.execute("SELECT key, value FROM meta"))
    measured = {i: (sha, dec_, json.loads(rs), r, json.loads(c6), u) for i, sha, dec_, rs, r, c6, u in m.execute(
        "SELECT id, sha256, decision, reasons, r, c6, u FROM scope")}
    exceptions = load_exceptions()
    held_original = set(json.loads(meta["held_original"]))
    out = []
    for row in sorted(scope_rows(), key=lambda r: r["id"]):
        rid, s = row["id"], row["s"]
        sha, c6, exception = None, {}, False
        if rid in held_original:
            dec_, rs, r = "held", ["original row (decision 4)"], None
        elif rid in measured:
            sha, dec_, rs, r, c6, u = measured[rid]
            if dec_ == "held" and exceptions.get(rid) == (hashlib.sha256(u.encode()).hexdigest(),
                                                           hashlib.sha256(r.encode()).hexdigest()):
                dec_, exception = "accepted (exception)", True
        else:
            dec_, rs, r = "nothing to decode", [], None
        out.append({"id": rid, "rel_path": row["rel_path"], "sha256": sha, "decision": dec_, "reasons": rs,
                    "exception": exception, "c6": c6, "s": s, "r": r,
                    "shipped": r if dec_.startswith("accepted") else s})
    return out


def run_summary(out_json: str | None = None, rows: list | None = None) -> dict:
    m, cen, dec = ro(MEASURE_DB), ro(CENSUS_DB), ro(DECISIONS_DB)
    meta = dict(m.execute("SELECT key, value FROM meta"))
    sha_of = dict(cen.execute("SELECT rel_path, sha256 FROM pdfs"))
    measured = {i: json.loads(c6) for i, c6 in m.execute("SELECT id, c6 FROM scope")}
    letters = pdf_letters(cen, dec)
    counts = collections.Counter()
    decisions = collections.Counter()
    reasons = collections.Counter()
    seen_texts = set()
    digest = hashlib.sha256()
    for k in ("registry_version", "reference_version", "extractor_version", "code_version", "libraries"):
        digest.update(f"{k}={meta.get(k)}\n".encode())
    # every PDF read, by its content (C7: the input, not only the decisions made on it)
    for rel, sha in sorted(sha_of.items()):
        digest.update(f"pdf:{rel}:{sha}\n".encode())
    for row in rows if rows is not None else row_decisions():
        rid, s, r, dec_, rs, shipped = row["id"], row["s"], row["r"], row["decision"], row["reasons"], row["shipped"]
        decisions[dec_] += 1
        reasons.update({x.split(" (page")[0] for x in rs})
        digest.update(json.dumps([rid, dec_, sorted(rs), hashlib.sha256(shipped.encode()).hexdigest()]).encode())
        key = (sha_of[row["rel_path"]], s)
        if key in seen_texts:  # distinct texts, as the census counted them
            continue
        seen_texts.add(key)
        pdf = letters.get(sha_of[row["rel_path"]], collections.Counter())
        before = _deficit(pdf, s)
        counts["denominator"] += before
        if dec_.startswith("accepted"):
            after = _deficit(pdf, r)
            counts["recovered"] += before - after
            counts["unresolved"] += after
        elif dec_ == "held":
            counts["held"] += before
        else:
            counts["unresolved"] += before
    c6 = collections.Counter()
    for x in measured.values():
        c6.update(x)
    b = m.execute("SELECT count(*), sum(held), sum(weight), sum(judged), sum(agree), sum(texts_differ) FROM bench").fetchone()
    errs = collections.Counter()
    for (x,) in m.execute("SELECT decode_errors FROM bench"):
        errs["decode_errors"] += len(json.loads(x))
    for i, judged, agree, de in m.execute("SELECT id, judged, agree, decode_errors FROM bench ORDER BY id"):
        digest.update(json.dumps([i, judged, agree, de]).encode())
    # the certification itself, so a run-to-run change in a decision fails C7 even where no row shows it
    for x in dec.execute("SELECT sha256, objid, code, face, status, text, form FROM decisions"
                         " ORDER BY sha256, objid, code"):
        digest.update(json.dumps(x, ensure_ascii=False).encode())
    c4 = dict(dec.execute("SELECT status, count(*) FROM decisions WHERE text IS NOT NULL GROUP BY status").fetchall())
    c3 = {"rows": b[0], "rows_held": b[1] or 0, "weight": b[2] or 0, "judged": b[3] or 0, "agree": b[4] or 0,
          "texts_differ": b[5] or 0, **errs}
    c3["judged_share"] = c3["judged"] / c3["weight"] if c3["weight"] else 0.0
    # Owner's ruling (2026-10-27): where the PDF and the Word text differ outside the decoded letters, the texts differ,
    # not the decoding; that weight is reported apart and the 99.5% bar applies to decoding errors only.
    c3["decode_error_weight"] = c3["judged"] - c3["agree"] - c3["texts_differ"]
    decodable = c3["agree"] + c3["decode_error_weight"]
    c3["agree_share"] = c3["agree"] / decodable if decodable else 0.0
    c3["agree_share_all_disagreements"] = c3["agree"] / c3["judged"] if c3["judged"] else 0.0
    c3["texts_differ_share"] = c3["texts_differ"] / c3["judged"] if c3["judged"] else 0.0
    c3["passed"] = c3["judged_share"] >= C3_MIN_JUDGED and c3["agree_share"] >= C3_MIN_AGREE
    c5 = {**counts, **c5_verdict(counts, CENSUS_DENOMINATOR), "declared_denominator": CENSUS_DENOMINATOR}
    summary = {"meta": {k: v for k, v in meta.items() if k != "held_original"}, "scope_decisions": dict(decisions),
               "hold_reasons": dict(reasons.most_common()), "c3": c3, "c4_entries_by_status": c4, "c5": c5,
               "c6": dict(c6), "errors": m.execute("SELECT count(*) FROM errors").fetchone()[0],
               "unfaithful_rows": m.execute("SELECT count(*) FROM scope WHERE faithful = 0").fetchone()[0],
               "unfaithful_bench": m.execute("SELECT count(*) FROM bench WHERE faithful = 0").fetchone()[0],
               "certify_errors": int(dict(dec.execute("SELECT key, value FROM meta")).get("errors") or 0),
               "unreadable_pdfs": cen.execute("SELECT count(*) FROM pdfs WHERE error IS NOT NULL").fetchone()[0],
               "digest": digest.hexdigest()[:16]}
    if out_json:
        with open(out_json, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, ensure_ascii=False, indent=1)
    return summary


def use_work_dir(work: str, pdf_root: str | None = None) -> None:
    """Point every working file at ``work`` (and the PDFs at ``pdf_root``), in this process and in the workers it
    starts, which read both from the environment when they import this module."""
    global WORK, CENSUS_DB, REF_PICKLE, DECISIONS_DB, COPIES, MEASURE_DB, EXCEPTIONS_JSON, PDF_ROOT, _REF
    os.environ["FGP_GLYPH_REREAD_WORK"] = WORK = work
    if pdf_root:
        os.environ["FGP_GLYPH_REREAD_PDF_ROOT"] = PDF_ROOT = pdf_root
    CENSUS_DB = os.path.join(WORK, "census.db")
    REF_PICKLE = os.path.join(WORK, "reference_outlines.pkl")
    DECISIONS_DB = os.path.join(WORK, "decisions.db")
    COPIES = os.path.join(WORK, "copies")
    MEASURE_DB = os.path.join(WORK, "measure.db")
    EXCEPTIONS_JSON = os.path.join(WORK, "exceptions.json")
    _REF = None


_DEFAULT_WORK, _DEFAULT_PDF_ROOT = WORK, PDF_ROOT
_ENV_AT_IMPORT = {k: os.environ.get(k) for k in ("FGP_GLYPH_REREAD_WORK", "FGP_GLYPH_REREAD_PDF_ROOT")}


def reset() -> None:
    """Undo ``use_work_dir`` and ``set_inputs``: the paths and environment as they were at import, in this process
    and the workers it starts later."""
    global _INPUTS
    _INPUTS = None
    use_work_dir(_DEFAULT_WORK, _DEFAULT_PDF_ROOT)
    for k, v in _ENV_AT_IMPORT.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def applied_map() -> list[dict]:
    """Every font-map entry a repaired copy injects, with its certification evidence (gate 3's ``glyph_map``)."""
    dec = ro(DECISIONS_DB)
    shas = [s for (s,) in ro(MEASURE_DB).execute("SELECT DISTINCT sha256 FROM scope UNION SELECT DISTINCT sha256 "
                                                  "FROM bench ORDER BY 1")]
    out = []
    for sha in shas:
        maps, _ = font_maps(sha, dec)
        for objid, mp in sorted(maps.items()):
            for code, form in sorted(mp.items()):
                name, glyph, face, interps, refs, status, text, outline = dec.execute(
                    "SELECT name, glyph, face, interps, refs, status, text, outline FROM decisions "
                    "WHERE sha256=? AND objid=? AND code=?", (sha, objid, code)).fetchone()
                out.append({"sha256": sha, "font_objid": objid, "code": code, "name": name, "glyph": glyph,
                            "sequence": text, "form": form, "status": status, "typeface": face, "interpretations": interps,
                            "reference_files": refs, "outline": outline})
    return out


def run_all(workers: int = 16) -> dict:
    """Steps 1-5 in one call, as gate 3 runs them: census, certification, measurement, exceptions and summary.
    Returns the summary, every row's decision and the applied map."""
    run_census(workers)
    run_decisions(workers)
    run_measure(workers)
    doc = build_exceptions()
    rows = row_decisions()
    out = {"summary": run_summary(rows=rows), "rows": rows, "exceptions": doc, "map": applied_map()}
    import gc
    gc.collect()  # release the working databases (connections left in reference cycles keep the files open)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("exceptions")
    p = sub.add_parser("summary")
    p.add_argument("--out")
    p = sub.add_parser("measure")
    p.add_argument("--workers", type=int, default=16)
    p.add_argument("--limit", type=int)
    for cmd in ("census", "certify"):
        p = sub.add_parser(cmd)
        p.add_argument("--workers", type=int, default=16)
    args = ap.parse_args(argv)
    if args.cmd == "census":
        run_census(args.workers)
    elif args.cmd == "certify":
        run_decisions(args.workers)
    elif args.cmd == "measure":
        run_measure(args.workers, args.limit)
    elif args.cmd == "exceptions":
        doc = build_exceptions()
        print(f"exceptions recorded: {len(doc['rows'])}; C6 rows excluded for losing attested words: "
              f"{[(x['id'], x['n']) for x in doc['excluded_c6_rows']]}")
    elif args.cmd == "summary":
        print(json.dumps(run_summary(args.out), ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
