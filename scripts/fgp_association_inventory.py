#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Gate 1 of the FGP source-folders plan: the association inventory.

WHY THIS EXISTS (docs/plans/FGP_SOURCE_FOLDERS_ASSESSMENT.md, section 8, gate 1)
-------------------------------------------------------------------------------
Two FGP deliveries sit beside today's ``fgp_data/fgp_transcriptions.db``: the FIST file
store (``FGPTranscriptions_NoPOSInfo``: one PDF / DOCX / DOC per FIST unit row) and the
word-position folders (``FGPTranscriptions_WithPOSInfoAndImages``: one directory per FGP
image). Before any of it is imported, every physical file has to be tied to the FIST
source record it belongs to, the manuscript(s) that record resolves to, the position
directory FIST names for it, its visibility flags and its standing against today's
sidecar -- and every count has to reconcile. A file that cannot be tied to a record, or a
multi-association without a declared rule, must stop the pipeline here, not surface as
a wrong credit or a wrong manuscript later.

WHAT IT PRODUCES
----------------
``<out-dir>/inventory.db`` (SQLite) with one row per NoPOS basename (``files``), one per
matching FIST unit row (``file_units``), one per (file, inventory, AlmaId) association
path (``file_almas``), one per WithPOS directory (``b_dirs``) and per image number
(``b_numbers``), one per row of today's sidecar (``today_rows``), one per FIST
transcription / translation / description FileName that has no file in the export
(``fist_absent``), plus ``checks`` and ``counts``. ``summary.json`` carries the counts and
checks; ``REPORT.md`` is the human-readable gate result.

WHAT MAKES IT FAIL
------------------
Each check in ``CHECKS`` names its failure condition. In short: a count that does not
partition (files / basenames / unit rows / images / manuscripts), a FIST unit row whose
signature is missing, a basename matched to more than one kind, a multi-association
without a resolution class, an unmatched basename that *is* a FIST FileName (a matching
bug), a sidecar row or an absent FIST file without a reason, a WithPOS directory that
yields no image number, an image number that fjms resolves to several manuscripts, and
any ``--expect KEY=VALUE`` that the run does not reproduce.

The resolution rules declared here (``RESOLUTION_RULES``) are proposals; gate 2 confirms
them. The inventory is read-only on every input.

USAGE
-----
    powershell -File scripts/run_fgp_association_inventory.ps1      # the declared expectations

    python scripts/fgp_association_inventory.py \\
        --nopos "C:\\FGPTranscriptions_NoPOSInfo" \\
        --withpos "C:\\FGPTranscriptions_WithPOSInfoAndImages" \\
        --out-dir fgp_data/inventory \\
        --expect-file scripts/fgp_association_inventory.expect.json \\
        --expect nopos.basenames_pdf=72098

Exit codes: 0 every check passed, 1 at least one check failed (fail-closed), 2 usage.
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_NOPOS = r"C:\FGPTranscriptions_NoPOSInfo"
DEFAULT_WITHPOS = r"C:\FGPTranscriptions_WithPOSInfoAndImages"
DEFAULT_FIST = os.path.join(ROOT, "fist_data", "FIST.db")
DEFAULT_FJMS = os.path.join(ROOT, "fist_data", "fjms_enrichment.db")
DEFAULT_NLI = os.path.join(ROOT, "nli_data", "nli_crossref.db")
DEFAULT_FGP_DB = os.path.join(ROOT, "fgp_data", "fgp_transcriptions.db")
DEFAULT_OUT = os.path.join(ROOT, "fgp_data", "inventory")

# The junk the assessment filtered before counting basenames (section 3.1).
JUNK_BASENAME_RE = re.compile(r"^(Copy of |New Microsoft|inv$|rrrr$|test$|temp8)")
LOCK_PREFIX = "~$"
KEPT_EXTS = ("pdf", "docx", "doc")
NAME_ID_RE = re.compile(r"^(C?\d+)")
NAME_SUFFIX_RE = re.compile(r"_(Tc|Tl|D)$")
CREDIT_PREFIX_RE = re.compile(r"^(\d{3})(\d{4})")
TODAY_FILENAME_RE = re.compile(r"_(trans|translate_heb|translate_eng)\.pdf$")
B_REGION_RE = re.compile(r"[._]G(\d+)")
B_TOKEN_RE = re.compile(r"C?(\d+)")

SUFFIX_KIND = {"Tc": "transcription", "Tl": "translation", "D": "description"}

UNIT_TABLES = (
    ("dbo_UnitTranscription", "UnitTranscriptionId", "transcription"),
    ("dbo_UnitTranslation", "UnitTranslationId", "translation"),
    ("dbo_UnitFreeDescription", "UnitFreeDescriptionId", "description"),
)

# Declared resolution rules for every multi-association class. Proposals for gate 2.
RESOLUTION_RULES = {
    "dup_unit_rows_same_signature": (
        "One source record registered more than once for one file: key on the lowest unit id; "
        "the other unit rows are aliases of it."
    ),
    "multi_signature_same_set": (
        "Several signatures of one signature set share the file: one record per set; the "
        "manuscript set is the union; the credit comes from the signature with the latest DateCreated."
    ),
    "multi_signature_different_sets": (
        "Distinct source records share a file name: one inventory row per signature; the text "
        "pipeline keys on (unit_table, unit_id), never on the basename."
    ),
    "inventory_multi_alma": (
        "The inventory carries several AlmaIds (a joined or multi-part shelfmark): one chooser "
        "row per AlmaId, each carrying the same unit id; the join itself goes to the joins pilot."
    ),
    "set_multi_inventory": (
        "The signature set is attached to several inventories (a join group): one chooser row "
        "per AlmaId, flagged for the joins pilot."
    ),
}

STATUS_ORDER = (
    "unmatched",
    "description",
    "hold_visibility",
    "unresolved_alma",
    "served",
    "candidate_multi_alma",
    "candidate",
)


def log(msg: str) -> None:
    print(msg, flush=True)


def ro(path: str) -> sqlite3.Connection:
    """Read-only connection; never creates a stub file for a missing sidecar."""
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        raise SystemExit(f"not a usable SQLite file: {path}")
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


# --------------------------------------------------------------------------- folder A


@dataclass
class FileRec:
    basename: str
    dirs: set = field(default_factory=set)
    pdf: int = 0
    scan_pdf: int = 0
    docx: int = 0
    doc: int = 0


@dataclass
class NoposScan:
    files: dict = field(default_factory=dict)  # basename -> FileRec
    top_dirs: set = field(default_factory=set)
    total_entries: int = 0
    lock_files: int = 0
    other_ext: int = 0
    junk: int = 0
    counted: int = 0
    by_ext: collections.Counter = field(default_factory=collections.Counter)


def scan_nopos(root: str) -> NoposScan:
    s = NoposScan()
    for top in sorted(os.listdir(root)):
        tp = os.path.join(root, top)
        if not os.path.isdir(tp):
            continue
        s.top_dirs.add(top)
        for sub in ("pdf", "word", "word_old"):
            sp = os.path.join(tp, sub)
            if not os.path.isdir(sp):
                continue
            with os.scandir(sp) as it:
                for e in it:
                    if not e.is_file():
                        continue
                    s.total_entries += 1
                    if e.name.startswith(LOCK_PREFIX):
                        s.lock_files += 1
                        continue
                    stem, dot, ext = e.name.rpartition(".")
                    ext = ext.lower() if dot else ""
                    if ext not in KEPT_EXTS:
                        s.other_ext += 1
                        continue
                    base = stem
                    is_scan = False
                    if ext == "pdf" and base.endswith(".scan"):
                        base = base[: -len(".scan")]
                        is_scan = True
                    if JUNK_BASENAME_RE.search(base):
                        s.junk += 1
                        continue
                    s.counted += 1
                    s.by_ext[ext] += 1
                    rec = s.files.get(base)
                    if rec is None:
                        rec = s.files[base] = FileRec(base)
                    rec.dirs.add(top)
                    if ext == "pdf":
                        if is_scan:
                            rec.scan_pdf += 1
                        else:
                            rec.pdf += 1
                    elif ext == "docx":
                        rec.docx += 1
                    else:
                        rec.doc += 1
    return s


def name_shape(base: str) -> tuple:
    """(shape, leading id or None, suffix or None)."""
    m = NAME_ID_RE.match(base)
    if not m:
        return "no_leading_id", None, None
    lead = m.group(1)
    if lead.startswith("C"):
        return "c_named", lead, None
    if "_" not in base:
        return "id_only", lead, None
    sm = NAME_SUFFIX_RE.search(base)
    if sm:
        return "suffixed", lead, sm.group(1)
    return "no_suffix", lead, None


# --------------------------------------------------------------------------- folder B


@dataclass
class BDir:
    name: str
    fmt: str  # B1 | B2 | other
    numbers: list
    region: str | None
    has_xml: bool = False
    has_js: bool = False
    has_json: bool = False
    has_data_js: bool = False
    has_img: bool = False
    png_count: int = 0
    file_count: int = 0


def parse_b_dirname(name: str) -> tuple:
    """(format, [numbers], region suffix or None). Numbers keep their order; 'C' stripped."""
    region = None
    rm = B_REGION_RE.search(name)
    if rm:
        region = "G" + rm.group(1)
    nums = []
    for tok in name.split():
        tok = B_REGION_RE.sub("", tok)
        m = B_TOKEN_RE.fullmatch(tok)
        if m:
            nums.append(m.group(1))
    if not nums:
        return "other", [], region
    return ("B2" if name.startswith("C") else "B1"), nums, region


def scan_withpos(root: str) -> tuple:
    dirs: list[BDir] = []
    stray_files = 0
    for top in sorted(os.listdir(root)):
        tp = os.path.join(root, top)
        if not os.path.isdir(tp):
            stray_files += 1
            continue
        fmt, nums, region = parse_b_dirname(top)
        d = BDir(top, fmt, nums, region)
        with os.scandir(tp) as it:
            for e in it:
                if not e.is_file():
                    continue
                d.file_count += 1
                n = e.name.lower()
                if n == "data.js":
                    d.has_data_js = True
                elif n.endswith(".xml"):
                    d.has_xml = True
                elif n.endswith(".xml.js"):
                    pass
                elif n.endswith(".js"):
                    d.has_js = True
                elif n.endswith(".json"):
                    d.has_json = True
                elif n == "img.jpg":
                    d.has_img = True
                elif n.endswith(".png"):
                    d.png_count += 1
        dirs.append(d)
    return dirs, stray_files


# --------------------------------------------------------------------------- FIST


@dataclass
class UnitRow:
    table: str
    kind: str
    unit_id: int
    signature_id: int
    filename: str
    folder_path: str | None
    lang: int | None
    itf: str | None
    has_files: int | None
    double_not_display: int | None
    heb_copyright: bool
    eng_copyright: bool


@dataclass
class Fist:
    units_by_name: dict = field(default_factory=lambda: collections.defaultdict(list))
    all_filenames: set = field(default_factory=set)
    norm_index: dict = field(default_factory=lambda: collections.defaultdict(set))
    id_index: dict = field(default_factory=lambda: collections.defaultdict(set))
    sig: dict = field(default_factory=dict)  # SignatureId -> (SetSignatureId, SourceId, SubId, DateCreated)
    set2inv: dict = field(default_factory=lambda: collections.defaultdict(set))
    inv2alma: dict = field(default_factory=lambda: collections.defaultdict(set))
    source_names: dict = field(default_factory=dict)
    itf_counts: collections.Counter = field(default_factory=collections.Counter)
    filename_rows: int = 0


def _norm_name(s: str) -> str:
    return re.sub(r"\s+", "", s).lower()


def load_fist(conn: sqlite3.Connection) -> Fist:
    f = Fist()
    cols_t = ("FileName, SignatureId, FolderPath, Lang, ImageTranscriptionFolder, HasFiles, "
              "tmp_DoubleNotToDisplay, HebCopyRight, EngCopyRight")
    cols_l = "FileName, SignatureId, FolderPath, Lang, NULL, HasFiles, tmp_DoubleNotToDisplay, HebCopyRight, EngCopyRight"
    cols_d = "FileName, SignatureId, FolderPath, NULL, NULL, HasFiles, NULL, NULL, NULL"
    for (table, id_col, kind), cols in zip(UNIT_TABLES, (cols_t, cols_l, cols_d)):
        q = f"SELECT {id_col}, {cols} FROM {table} WHERE FileName IS NOT NULL AND FileName <> ''"
        for uid, fn, sid, fp, lang, itf, hf, dnd, heb, eng in conn.execute(q):
            fn = str(fn)
            f.filename_rows += 1
            f.all_filenames.add(fn)
            f.norm_index[_norm_name(fn)].add(fn)
            m = NAME_ID_RE.match(fn)
            if m and not m.group(1).startswith("C"):
                f.id_index[m.group(1)].add(fn)
            f.units_by_name[fn].append(UnitRow(
                table, kind, int(uid), int(sid) if sid is not None else -1, fn,
                str(fp) if fp else None, lang, str(itf) if itf else None, hf, dnd,
                bool(heb), bool(eng),
            ))
            if itf:
                f.itf_counts[str(itf)] += 1
    for sid, setid, so, su, dc in conn.execute(
        "SELECT SignatureId, SetSignatureId, SourceId, SubId, DateCreated FROM dbo_Signature"
    ):
        f.sig[int(sid)] = (setid, so, su, dc)
    for inv, setid in conn.execute("SELECT InventoryId, SetSignatureId FROM dbo_InventorySignature"):
        if setid is not None and inv is not None:
            f.set2inv[setid].add(int(inv))
    for inv, alma in conn.execute("SELECT InventoryId, AlmaId FROM dbo_InventoryAlma WHERE AlmaId IS NOT NULL"):
        f.inv2alma[int(inv)].add(str(alma))
    try:
        for tc, en in conn.execute("SELECT TeamCode, EngDesc FROM dbo_CodeSource"):
            f.source_names[tc] = en
    except sqlite3.OperationalError:
        pass
    return f


# --------------------------------------------------------------------------- today / fjms / nli


def today_basename(filename: str) -> str:
    return TODAY_FILENAME_RE.sub("", filename or "")


def today_kind(filename: str) -> str | None:
    m = TODAY_FILENAME_RE.search(filename or "")
    if not m:
        return None
    return "transcription" if m.group(1) == "trans" else "translation"


def load_today(conn: sqlite3.Connection) -> list:
    return [
        dict(zip(("id", "filename", "c_number", "sys_id", "text_source", "image_id"), r))
        for r in conn.execute(
            "SELECT id, filename, c_number, sys_id, text_source, image_id FROM fgp_transcriptions"
        )
    ]


def load_fjms(conn: sqlite3.Connection, numbers: set) -> dict:
    out = {}
    for fgp, alma, w, h, rot in conn.execute(
        "SELECT FGP, AlmaId, PixelWidth, PixelHeight, Rotation_Angle_deg FROM extra_info WHERE FGP IS NOT NULL"
    ):
        n = str(fgp).lstrip("C")
        if n in numbers:
            rec = out.setdefault(n, {"almas": set(), "w": w, "h": h, "rot": rot})
            if alma is not None:
                rec["almas"].add(str(alma))
    return out


def load_nli(conn: sqlite3.Connection, numbers: set) -> dict:
    out = {}
    for fgpn, alma, img in conn.execute(
        "SELECT FGPImageNumberId, NLI_AlmaId, ImageName FROM nli_images WHERE FGPImageNumberId IS NOT NULL"
    ):
        n = str(fgpn)
        if n in numbers:
            rec = out.setdefault(n, {"almas": set(), "images": set()})
            if alma is not None:
                rec["almas"].add(str(alma))
            if img:
                rec["images"].add(str(img))
    return out


# --------------------------------------------------------------------------- inventory


@dataclass
class Inventory:
    files: list = field(default_factory=list)
    file_units: list = field(default_factory=list)
    file_almas: list = field(default_factory=list)
    b_dirs: list = field(default_factory=list)
    b_numbers: list = field(default_factory=list)
    today_rows: list = field(default_factory=list)
    fist_absent: list = field(default_factory=list)
    counts: dict = field(default_factory=dict)
    checks: list = field(default_factory=list)


def _j(v) -> str:
    return json.dumps(sorted(v) if isinstance(v, set) else v, ensure_ascii=False)


def classify_multi(unit_rows: list, fist: Fist) -> tuple:
    """Return (classes, signature ids, set ids, inventories, almas, unresolved reason)."""
    sigs = sorted({u.signature_id for u in unit_rows})
    missing = [s for s in sigs if s not in fist.sig]
    sets = sorted({fist.sig[s][0] for s in sigs if s in fist.sig and fist.sig[s][0] is not None})
    invs = sorted({i for st in sets for i in fist.set2inv.get(st, ())})
    almas = sorted({a for i in invs for a in fist.inv2alma.get(i, ())})
    classes = []
    if len(unit_rows) > 1 and len(sigs) == 1:
        classes.append("dup_unit_rows_same_signature")
    if len(sigs) > 1 and len(sets) == 1:
        classes.append("multi_signature_same_set")
    if len(sets) > 1:
        classes.append("multi_signature_different_sets")
    if any(len(fist.inv2alma.get(i, ())) > 1 for i in invs):
        classes.append("inventory_multi_alma")
    if any(len([i for i in fist.set2inv.get(st, ()) if fist.inv2alma.get(i)]) > 1 for st in sets):
        classes.append("set_multi_inventory")
    multi = len(unit_rows) > 1 or len(almas) > 1
    if multi and not classes:
        classes.append("unclassified")
    unresolved = None
    if not almas:
        if missing:
            unresolved = "signature_missing"
        elif not sets:
            unresolved = "no_set_signature"
        elif not invs:
            unresolved = "set_without_inventory"
        else:
            unresolved = "inventory_without_alma"
    return classes, sigs, sets, invs, almas, unresolved, missing


def unmatched_reason(base: str, rec: FileRec, fist: Fist) -> str:
    if fist.norm_index.get(_norm_name(base)):
        return "fist_match_after_normalizing"
    m = NAME_ID_RE.match(base)
    if m and not m.group(1).startswith("C") and fist.id_index.get(m.group(1)):
        return "fist_match_by_id_prefix_other_name"
    if m and m.group(1).startswith("C"):
        return "c_number_named_no_fist"
    if rec.pdf == 0 and rec.scan_pdf > 0:
        return "scan_only_no_fist"
    return "no_fist_trace"


def absence_reason(units: list, top_dirs: set) -> str:
    fps = {u.folder_path.rstrip("\\/") for u in units if u.folder_path}
    if not fps:
        return "no_folder_path"
    if fps & top_dirs:
        return "dir_present_file_missing"
    return "dir_absent_from_export"


def build_inventory(nopos: NoposScan, b_dirs: list, b_stray: int, fist: Fist, today: list,
                    fjms: dict, nli: dict) -> Inventory:
    inv = Inventory()
    C = inv.counts

    # ---- today's sidecar, by basename
    today_by_base = collections.defaultdict(list)
    today_sys = set()
    today_by_cn = collections.defaultdict(list)
    for r in today:
        today_by_base[today_basename(r["filename"])].append(r)
        if r["sys_id"]:
            today_sys.add(str(r["sys_id"]))
        if r["c_number"]:
            today_by_cn[str(r["c_number"]).lstrip("C")].append(r)

    # ---- folder B index
    b_dir_names = {d.name for d in b_dirs}
    b_fmt_by_name = {d.name: d.fmt for d in b_dirs}

    # ---- files
    kind_counts = collections.Counter()
    mixed = 0
    status_counts = collections.Counter()
    multi_classes = collections.Counter()
    unresolved_counts = collections.Counter()
    unmatched_counts = collections.Counter()
    shape_counts = collections.Counter()
    sig_missing_rows = 0
    credit_compared = credit_src_dis = credit_sub_dis = 0
    resolved = multi_alma = 0
    tr_almas_all = set()
    desc_almas = set()
    held = 0
    served_and_held = 0
    files_in_multiple_dirs = 0
    itf_linked_existing = 0
    unmatched_in_fist_exact = 0
    kind_disagree_with_today = 0
    suffix_disagree = 0
    # The assessment's section 3 counted PDF basenames only; these keep its numbers reproducible.
    pdfonly = collections.Counter()
    for base in sorted(nopos.files):
        rec = nopos.files[base]
        pdf_backed = bool(rec.pdf or rec.scan_pdf)
        shape, lead, suffix = name_shape(base)
        shape_counts[shape] += 1
        if len(rec.dirs) > 1:
            files_in_multiple_dirs += 1
        units = fist.units_by_name.get(base, [])
        units = sorted(units, key=lambda u: (u.table, u.unit_id))
        trows = today_by_base.get(base, [])
        row = {
            "basename": base, "dir": min(rec.dirs), "dirs": _j(rec.dirs), "has_pdf": int(rec.pdf > 0),
            "has_scan_pdf": int(rec.scan_pdf > 0), "has_docx": int(rec.docx > 0), "has_doc": int(rec.doc > 0),
            "pdf_files": rec.pdf + rec.scan_pdf, "name_shape": shape, "name_id": lead, "name_suffix": suffix,
            "kind": None, "unit_rows": len(units), "unit_ids": None, "signature_ids": None,
            "set_signature_ids": None, "source_ids": None, "sub_ids": None, "source_names": None,
            "inventory_ids": None, "alma_ids": None, "alma_count": 0, "multi_class": None,
            "resolution_rule": None, "unresolved_reason": None, "b_dir": None, "b_dir_exists": None,
            "b_format": None, "lang": None, "has_copyright": None, "double_not_display": None,
            "has_files": None, "credit_prefix_agrees": None, "in_today_db": int(bool(trows)),
            "today_row_ids": _j([r["id"] for r in trows]) if trows else None,
            "today_sys_ids": _j({str(r["sys_id"]) for r in trows if r["sys_id"]}) if trows else None,
            "status": None, "unmatched_reason": None,
        }
        if not units:
            if base in fist.all_filenames:
                unmatched_in_fist_exact += 1
            row["kind"] = "unmatched"
            row["unmatched_reason"] = unmatched_reason(base, rec, fist)
            unmatched_counts[row["unmatched_reason"]] += 1
            row["status"] = "unmatched"
            status_counts["unmatched"] += 1
            if pdf_backed:
                pdfonly["unmatched"] += 1
            inv.files.append(row)
            continue
        kinds = sorted({u.kind for u in units})
        if len(kinds) > 1:
            mixed += 1
        kind = kinds[0] if len(kinds) == 1 else "mixed:" + "+".join(kinds)
        row["kind"] = kind
        kind_counts[kind] += 1
        if suffix and SUFFIX_KIND.get(suffix) != kind:
            suffix_disagree += 1
        if pdf_backed:
            pdfonly["matched"] += 1
            pdfonly["kind." + kind] += 1
        if trows:
            tk = {today_kind(r["filename"]) for r in trows} - {None}
            if tk and kind not in tk:
                kind_disagree_with_today += 1
        classes, sigs, sets, invs, almas, unresolved, missing = classify_multi(units, fist)
        sig_missing_rows += len(missing)
        srcs = sorted({fist.sig[s][1] for s in sigs if s in fist.sig and fist.sig[s][1] is not None})
        subs = sorted({fist.sig[s][2] for s in sigs if s in fist.sig and fist.sig[s][2] is not None})
        row.update({
            "unit_ids": _j([[u.table, u.unit_id] for u in units]), "signature_ids": _j(sigs),
            "set_signature_ids": _j(sets), "source_ids": _j(srcs), "sub_ids": _j(subs),
            "source_names": _j([fist.source_names.get(s, "") for s in srcs]),
            "inventory_ids": _j(invs), "alma_ids": _j(almas), "alma_count": len(almas),
            "multi_class": "+".join(classes) if classes else None,
            "resolution_rule": " | ".join(RESOLUTION_RULES.get(c, "NONE DECLARED") for c in classes) or None,
            "unresolved_reason": unresolved,
            "lang": _j(sorted({u.lang for u in units if u.lang is not None})) or None,
            "has_copyright": int(any(u.heb_copyright or u.eng_copyright for u in units)),
            "double_not_display": int(any(u.double_not_display == 1 for u in units)),
            "has_files": _j(sorted({u.has_files for u in units if u.has_files is not None})) or None,
        })
        for c in classes:
            multi_classes[c] += 1
        if almas:
            resolved += 1
            if len(almas) > 1:
                multi_alma += 1
            if pdf_backed:
                pdfonly["resolved"] += 1
                pdfonly["multi_alma_files"] += int(len(almas) > 1)
            if kind == "transcription":
                tr_almas_all.update(almas)
            elif kind == "description":
                desc_almas.update(almas)
        else:
            unresolved_counts[unresolved] += 1
        itfs = sorted({u.itf for u in units if u.itf})
        if itfs:
            row["b_dir"] = _j(itfs) if len(itfs) > 1 else itfs[0]
            exists = [t for t in itfs if t in b_dir_names]
            row["b_dir_exists"] = int(bool(exists))
            row["b_format"] = "+".join(sorted({b_fmt_by_name[t] for t in exists})) or None
            if exists:
                itf_linked_existing += 1
        cm = CREDIT_PREFIX_RE.match(base)
        first_sig = units[0].signature_id
        if cm and first_sig in fist.sig and fist.sig[first_sig][1] is not None:
            credit_compared += 1
            so, su = fist.sig[first_sig][1], fist.sig[first_sig][2]
            src_ok = str(so) == cm.group(1)
            sub_ok = su is not None and int(cm.group(2)) == int(su)
            credit_src_dis += int(not src_ok)
            credit_sub_dis += int(not sub_ok)
            row["credit_prefix_agrees"] = int(src_ok and sub_ok)
        # status
        if kind == "description":
            status = "description"
        elif kind.startswith("mixed:"):
            status = "unresolved_alma"  # never publishable until the kind is settled
        elif row["double_not_display"]:
            status = "hold_visibility"
        elif not almas:
            status = "unresolved_alma"
        elif trows:
            status = "served"
        elif len(almas) > 1:
            status = "candidate_multi_alma"
        else:
            status = "candidate"
        if row["double_not_display"]:
            held += 1
            if trows:
                served_and_held += 1
        row["status"] = status
        status_counts[status] += 1
        inv.files.append(row)
        for u in units:
            s = fist.sig.get(u.signature_id)
            inv.file_units.append({
                "basename": base, "unit_table": u.table, "unit_id": u.unit_id, "kind": u.kind,
                "signature_id": u.signature_id, "set_signature_id": s[0] if s else None,
                "source_id": s[1] if s else None, "sub_id": s[2] if s else None,
                "signature_date": s[3] if s else None, "lang": u.lang, "folder_path": u.folder_path,
                "image_transcription_folder": u.itf,
                "itf_exists": int(u.itf in b_dir_names) if u.itf else None,
                "has_files": u.has_files, "double_not_display": u.double_not_display,
                "heb_copyright": int(u.heb_copyright), "eng_copyright": int(u.eng_copyright),
            })
        for s in sigs:
            st = fist.sig[s][0] if s in fist.sig else None
            for i in fist.set2inv.get(st, ()) if st is not None else ():
                for a in sorted(fist.inv2alma.get(i, ())):
                    inv.file_almas.append({
                        "basename": base, "signature_id": s, "set_signature_id": st, "inventory_id": i,
                        "alma_id": a, "inventory_alma_count": len(fist.inv2alma[i]),
                        "in_today_db": int(a in today_sys),
                    })

    # ---- absent FIST files
    absent_counts = collections.Counter()
    for fn, units in fist.units_by_name.items():
        if fn in nopos.files:
            continue
        kinds = sorted({u.kind for u in units})
        reason = absence_reason(units, nopos.top_dirs)
        for k in kinds:
            absent_counts[(k, reason)] += 1
        inv.fist_absent.append({
            "filename": fn, "kinds": "+".join(kinds), "unit_ids": _j([[u.table, u.unit_id] for u in units]),
            "folder_paths": _j(sorted({u.folder_path for u in units if u.folder_path})),
            "has_files": _j(sorted({u.has_files for u in units if u.has_files is not None})),
            "reason": reason,
        })

    # ---- today's rows
    db_only_bases = set()
    db_only_reasons = collections.Counter()
    for r in today:
        base = today_basename(r["filename"])
        in_nopos = base in nopos.files
        reason = None
        if not in_nopos:
            db_only_bases.add(base)
            units = fist.units_by_name.get(base)
            reason = "not_in_fist" if not units else "fist_" + absence_reason(units, nopos.top_dirs)
            db_only_reasons[reason] += 1
        inv.today_rows.append({
            "id": r["id"], "basename": base, "filename": r["filename"],
            "kind_from_filename": today_kind(r["filename"]), "c_number": r["c_number"],
            "sys_id": r["sys_id"], "text_source": r["text_source"], "image_id": r["image_id"],
            "in_nopos": int(in_nopos), "db_only_reason": reason,
        })

    # ---- folder B
    numbers = collections.defaultdict(lambda: {"fmts": set(), "dirs": []})
    region_dirs = multi_dirs = 0
    fmt_counts = collections.Counter()
    for d in b_dirs:
        fmt_counts[d.fmt] += 1
        if d.region:
            region_dirs += 1
        if len(d.numbers) > 1:
            multi_dirs += 1
        for n in d.numbers:
            numbers[n]["fmts"].add(d.fmt)
            numbers[n]["dirs"].append(d.name)
        inv.b_dirs.append({
            "dir": d.name, "format": d.fmt, "numbers": _j(d.numbers), "number_count": len(d.numbers),
            "is_multi_image": int(len(d.numbers) > 1), "region": d.region,
            "fist_transcription_refs": fist.itf_counts.get(d.name, 0),
            "has_xml": int(d.has_xml), "has_js": int(d.has_js), "has_json": int(d.has_json),
            "has_data_js": int(d.has_data_js), "has_img": int(d.has_img), "png_count": d.png_count,
            "file_count": d.file_count,
        })
    n_rich_only = n_c_only = n_both = 0
    fjms_resolved = fjms_multi = 0
    b_almas = set()
    nli_hit = nli_multi = 0
    rows_covered = 0
    numbers_with_rows = 0
    text_sources = collections.Counter()
    rot_bins = collections.Counter()
    for n in sorted(numbers, key=lambda x: (len(x), x)):
        fm = numbers[n]["fmts"]
        if fm == {"B1"}:
            n_rich_only += 1
        elif fm == {"B2"}:
            n_c_only += 1
        else:
            n_both += 1
        fj = fjms.get(n)
        almas = sorted(fj["almas"]) if fj else []
        if almas:
            fjms_resolved += 1
            b_almas.update(almas)
            if len(almas) > 1:
                fjms_multi += 1
        nl = nli.get(n)
        if nl:
            nli_hit += 1
            if len(nl["almas"]) > 1:
                nli_multi += 1
        trows = today_by_cn.get(n, [])
        if trows:
            numbers_with_rows += 1
            rows_covered += len(trows)
            for r in trows:
                text_sources[r["text_source"]] += 1
        rot = fj["rot"] if fj else None
        if rot is None:
            rot_bins["none"] += 1
        else:
            a = abs(float(rot)) % 360
            if a <= 10 or a >= 350:
                rot_bins["<=10"] += 1
            elif 80 <= a <= 100 or 260 <= a <= 280:
                rot_bins["~90/270"] += 1
            elif 170 <= a <= 190:
                rot_bins["~180"] += 1
            else:
                rot_bins["other"] += 1
        inv.b_numbers.append({
            "number": n, "formats": "+".join(sorted(fm)), "dirs": _j(numbers[n]["dirs"]),
            "fjms_alma_ids": _j(almas) if almas else None, "fjms_alma_count": len(almas),
            "pixel_width": fj["w"] if fj else None, "pixel_height": fj["h"] if fj else None,
            "rotation_deg": rot, "nli_alma_ids": _j(nl["almas"]) if nl else None,
            "nli_image_names": _j(nl["images"]) if nl else None,
            "today_rows": len(trows), "today_text_sources": _j(dict(collections.Counter(r["text_source"] for r in trows))) if trows else None,
            "manuscript_in_today_db": int(any(a in today_sys for a in almas)) if almas else None,
        })
    itf_distinct = set(fist.itf_counts)
    itf_exists = itf_distinct & b_dir_names

    # ---- counts
    C.update({
        "nopos.entries": nopos.total_entries, "nopos.lock_files": nopos.lock_files,
        "nopos.other_ext": nopos.other_ext, "nopos.junk": nopos.junk, "nopos.counted_files": nopos.counted,
        "nopos.files_pdf": nopos.by_ext["pdf"], "nopos.files_docx": nopos.by_ext["docx"],
        "nopos.files_doc": nopos.by_ext["doc"],
        "nopos.basenames": len(nopos.files),
        "nopos.basenames_pdf": sum(1 for r in nopos.files.values() if r.pdf or r.scan_pdf),
        "nopos.basenames_docx_only": sum(1 for r in nopos.files.values() if not (r.pdf or r.scan_pdf)),
        "nopos.basenames_with_scan_twin": sum(1 for r in nopos.files.values() if r.scan_pdf and r.pdf),
        "nopos.basenames_scan_only": sum(1 for r in nopos.files.values() if r.scan_pdf and not r.pdf),
        "nopos.basenames_in_multiple_dirs": files_in_multiple_dirs,
        "nopos.top_dirs": len(nopos.top_dirs),
        "fist.filename_rows": fist.filename_rows, "fist.filenames_distinct": len(fist.all_filenames),
        "fist.matched": sum(kind_counts.values()), "fist.unmatched": status_counts["unmatched"],
        "fist.mixed_kind": mixed, "fist.signature_missing_rows": sig_missing_rows,
        "fist.unmatched_but_exact_fist_filename": unmatched_in_fist_exact,
        "fist.kind_disagrees_with_today_filename": kind_disagree_with_today,
        "fist.name_suffix_disagrees_with_kind": suffix_disagree,
        "alma.resolved": resolved, "alma.unresolved": sum(unresolved_counts.values()),
        "alma.multi_alma_files": multi_alma,
        "alma.transcription_manuscripts": len(tr_almas_all),
        "alma.transcription_manuscripts_not_in_today": len(tr_almas_all - today_sys),
        "alma.description_only_manuscripts": len(desc_almas - tr_almas_all),
        "credit.compared": credit_compared, "credit.source_prefix_disagree": credit_src_dis,
        "credit.subid_disagree": credit_sub_dis,
        "visibility.double_not_display_files": held, "visibility.served_and_held": served_and_held,
        "today.rows": len(today), "today.sys_ids": len(today_sys),
        "today.rows_in_nopos": sum(1 for r in inv.today_rows if r["in_nopos"]),
        "today.rows_db_only": sum(1 for r in inv.today_rows if not r["in_nopos"]),
        "today.db_only_basenames": len(db_only_bases),
        "withpos.dirs": len(b_dirs), "withpos.stray_files": b_stray,
        "withpos.dirs_B1": fmt_counts["B1"], "withpos.dirs_B2": fmt_counts["B2"],
        "withpos.dirs_other": fmt_counts["other"], "withpos.region_dirs": region_dirs,
        "withpos.multi_image_dirs": multi_dirs, "withpos.numbers": len(numbers),
        "withpos.numbers_B1": n_rich_only + n_both, "withpos.numbers_B2": n_c_only + n_both,
        "withpos.numbers_both": n_both,
        "withpos.numbers_fjms_resolved": fjms_resolved, "withpos.numbers_fjms_unresolved": len(numbers) - fjms_resolved,
        "withpos.numbers_fjms_multi_alma": fjms_multi, "withpos.manuscripts": len(b_almas),
        "withpos.manuscripts_in_today_db": len(b_almas & today_sys),
        "withpos.manuscripts_not_in_today_db": len(b_almas - today_sys),
        "withpos.numbers_in_nli": nli_hit, "withpos.numbers_nli_multi_alma": nli_multi,
        "withpos.numbers_with_today_rows": numbers_with_rows, "withpos.today_rows_covered": rows_covered,
        "withpos.itf_distinct": len(itf_distinct), "withpos.itf_exists": len(itf_exists),
        "withpos.itf_missing": len(itf_distinct - b_dir_names),
        "withpos.dirs_unreferenced_by_fist": len(b_dir_names - itf_distinct),
        "withpos.nopos_files_linked_to_existing_dir": itf_linked_existing,
    })
    for k, v in sorted(kind_counts.items()):
        C[f"kind.{k}"] = v
    for k, v in sorted(pdfonly.items()):
        C[f"pdfonly.{k}"] = v
    for k, v in sorted(status_counts.items()):
        C[f"status.{k}"] = v
    for k, v in sorted(multi_classes.items()):
        C[f"multi.{k}"] = v
    for k, v in sorted(unresolved_counts.items()):
        C[f"unresolved.{k}"] = v
    for k, v in sorted(unmatched_counts.items()):
        C[f"unmatched.{k}"] = v
    for k, v in sorted(shape_counts.items()):
        C[f"shape.{k}"] = v
    for (k, r), v in sorted(absent_counts.items()):
        C[f"absent.{k}.{r}"] = v
    for k, v in sorted(db_only_reasons.items()):
        C[f"db_only.{k}"] = v
    for k, v in sorted(text_sources.items()):
        C[f"withpos.today_text_source.{k}"] = v
    for k, v in sorted(rot_bins.items()):
        C[f"withpos.rotation.{k}"] = v
    return inv


# --------------------------------------------------------------------------- checks


def _sum_prefix(C: dict, prefix: str) -> int:
    return sum(v for k, v in C.items() if k.startswith(prefix))


def run_checks(inv: Inventory, expectations: dict) -> list:
    C = inv.counts
    checks = []

    def add(name, passed, expected, actual, note):
        checks.append({"name": name, "passed": int(bool(passed)), "expected": str(expected),
                       "actual": str(actual), "note": note})

    lhs = C["nopos.lock_files"] + C["nopos.other_ext"] + C["nopos.junk"] + C["nopos.counted_files"]
    add("C01_physical_files_partition", lhs == C["nopos.entries"], C["nopos.entries"], lhs,
        "lock + other extension + junk + counted == every file under pdf/, word/, word_old/")
    lhs = C["fist.matched"] + C["fist.unmatched"]
    add("C02_basenames_partition", lhs == C["nopos.basenames"], C["nopos.basenames"], lhs,
        "matched + unmatched == basenames")
    lhs = _sum_prefix(C, "kind.")
    add("C03_kinds_partition", lhs == C["fist.matched"] and C["fist.mixed_kind"] == 0,
        f"{C['fist.matched']} and 0 mixed", f"{lhs} and {C['fist.mixed_kind']} mixed",
        "transcription + translation + description == matched; no basename of two kinds")
    add("C04_signatures_resolve", C["fist.signature_missing_rows"] == 0, 0, C["fist.signature_missing_rows"],
        "every matched unit row's SignatureId has a dbo_Signature row")
    lhs = C["alma.resolved"] + C["alma.unresolved"]
    add("C05_alma_partition", lhs == C["fist.matched"], C["fist.matched"], lhs,
        "resolved + unresolved (each with a reason) == matched")
    add("C06_multi_association_rules", C.get("multi.unclassified", 0) == 0, 0, C.get("multi.unclassified", 0),
        "every file with several unit rows or several AlmaIds has a declared resolution class")
    add("C07_unmatched_not_in_fist", C["fist.unmatched_but_exact_fist_filename"] == 0, 0,
        C["fist.unmatched_but_exact_fist_filename"], "no unmatched basename equals a FIST FileName")
    lhs = _sum_prefix(C, "unmatched.")
    add("C08_unmatched_reasons", lhs == C["fist.unmatched"], C["fist.unmatched"], lhs,
        "every unmatched basename carries a reason")
    lhs = C["today.rows_in_nopos"] + C["today.rows_db_only"]
    db_only_reasoned = _sum_prefix(C, "db_only.")
    add("C09_today_rows_partition", lhs == C["today.rows"] and db_only_reasoned == C["today.rows_db_only"],
        f"{C['today.rows']} rows, {C['today.rows_db_only']} reasoned", f"{lhs} rows, {db_only_reasoned} reasoned",
        "rows in the export + rows only in the sidecar == today's rows; every sidecar-only row has a reason")
    absent_tr = _sum_prefix(C, "absent.transcription.") + _sum_prefix(C, "absent.translation.")
    absent_rows = sum(1 for r in inv.fist_absent if r["kinds"] != "description")
    add("C10_fist_absent_reasons", absent_tr == absent_rows and all(r["reason"] for r in inv.fist_absent),
        absent_rows, absent_tr, "every FIST transcription / translation FileName without a file has a reason")
    lhs = C["withpos.dirs_B1"] + C["withpos.dirs_B2"] + C["withpos.dirs_other"]
    no_number = sum(1 for d in inv.b_dirs if d["format"] != "other" and d["number_count"] == 0)
    add("C11_withpos_dirs_partition", lhs == C["withpos.dirs"] and no_number == 0,
        f"{C['withpos.dirs']} dirs, 0 without a number", f"{lhs} dirs, {no_number} without a number",
        "B1 + B2 + other == directories; every B1 / B2 directory yields an image number")
    lhs = (C["withpos.numbers_B1"] - C["withpos.numbers_both"]) + (C["withpos.numbers_B2"] - C["withpos.numbers_both"]) + C["withpos.numbers_both"]
    add("C12_withpos_numbers_partition", lhs == C["withpos.numbers"], C["withpos.numbers"], lhs,
        "B1-only + B2-only + both == image numbers")
    multi_bad = sum(1 for d in inv.b_dirs if d["is_multi_image"] != int(d["number_count"] > 1))
    region_bad = sum(1 for d in inv.b_dirs if bool(d["region"]) != bool(B_REGION_RE.search(d["dir"])))
    add("C13_withpos_multi_and_region_flags", multi_bad == 0 and region_bad == 0, "0 and 0",
        f"{multi_bad} and {region_bad}", "multi-image and region flags agree with the directory name")
    lhs = C["withpos.itf_exists"] + C["withpos.itf_missing"]
    add("C14_fist_bridge_partition", lhs == C["withpos.itf_distinct"], C["withpos.itf_distinct"], lhs,
        "ImageTranscriptionFolder values: existing + missing == distinct")
    lhs = C["withpos.numbers_fjms_resolved"] + C["withpos.numbers_fjms_unresolved"]
    add("C15_withpos_fjms_partition", lhs == C["withpos.numbers"] and C["withpos.numbers_fjms_multi_alma"] == 0,
        f"{C['withpos.numbers']} and 0 multi", f"{lhs} and {C['withpos.numbers_fjms_multi_alma']} multi",
        "resolved + unresolved == numbers; no image number resolves to several manuscripts in fjms")
    lhs = C["withpos.manuscripts_in_today_db"] + C["withpos.manuscripts_not_in_today_db"]
    add("C16_withpos_manuscripts_partition", lhs == C["withpos.manuscripts"], C["withpos.manuscripts"], lhs,
        "manuscripts already served + new == manuscripts reached from folder B")
    desc_rows = sum(1 for f in inv.files if f["kind"] == "description")
    add("C17_descriptions_counted_once", desc_rows == C.get("kind.description", 0),
        C.get("kind.description", 0), desc_rows, "description basenames counted once, never as transcriptions")
    lhs = _sum_prefix(C, "status.")
    add("C18_status_partition", lhs == C["nopos.basenames"], C["nopos.basenames"], lhs,
        "every basename has exactly one status")
    for key, want in sorted(expectations.items()):
        have = C.get(key)
        add(f"E_{key}", have == want, want, have, "declared expectation (--expect)")
    inv.checks = checks
    return checks


# --------------------------------------------------------------------------- outputs


def _create(conn: sqlite3.Connection, table: str, rows: list, pk: str | None = None) -> None:
    if not rows:
        conn.execute(f'CREATE TABLE "{table}" (empty INTEGER)')
        return
    cols = list(rows[0].keys())
    defs = ", ".join(f'"{c}"' + (" PRIMARY KEY" if c == pk else "") for c in cols)
    conn.execute(f'CREATE TABLE "{table}" ({defs})')
    conn.executemany(
        f'INSERT INTO "{table}" VALUES ({",".join("?" * len(cols))})',
        ([r[c] for c in cols] for r in rows),
    )


def write_outputs(inv: Inventory, out_dir: str, meta: dict) -> str:
    os.makedirs(out_dir, exist_ok=True)
    db_path = os.path.join(out_dir, "inventory.db")
    if os.path.exists(db_path):
        try:
            old = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            is_ours = old.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='inventory_meta'"
            ).fetchone()[0] == 1
            old.close()
        except sqlite3.DatabaseError:
            is_ours = False
        if not is_ours:
            raise SystemExit(f"refusing to overwrite {db_path}: not an inventory this script wrote")
        os.remove(db_path)
    tmp = db_path + ".tmp"
    if os.path.exists(tmp):
        os.remove(tmp)
    conn = sqlite3.connect(tmp)
    with conn:
        _create(conn, "inventory_meta", [{"key": k, "value": str(v)} for k, v in meta.items()], pk="key")
        _create(conn, "files", inv.files, pk="basename")
        _create(conn, "file_units", inv.file_units)
        _create(conn, "file_almas", inv.file_almas)
        _create(conn, "b_dirs", inv.b_dirs, pk="dir")
        _create(conn, "b_numbers", inv.b_numbers, pk="number")
        _create(conn, "today_rows", inv.today_rows, pk="id")
        _create(conn, "fist_absent", inv.fist_absent, pk="filename")
        _create(conn, "checks", inv.checks, pk="name")
        _create(conn, "counts", [{"key": k, "value": v} for k, v in inv.counts.items()], pk="key")
        conn.execute('CREATE INDEX "ix_file_units_base" ON "file_units"("basename")')
        conn.execute('CREATE INDEX "ix_file_almas_base" ON "file_almas"("basename")')
        conn.execute('CREATE INDEX "ix_file_almas_alma" ON "file_almas"("alma_id")')
        conn.execute('CREATE INDEX "ix_today_base" ON "today_rows"("basename")')
    conn.close()
    os.replace(tmp, db_path)
    with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as fh:
        json.dump({"meta": meta, "counts": inv.counts, "checks": inv.checks}, fh, ensure_ascii=False, indent=1)
    with open(os.path.join(out_dir, "REPORT.md"), "w", encoding="utf-8") as fh:
        fh.write(render_report(inv, meta))
    return db_path


def render_report(inv: Inventory, meta: dict) -> str:
    C = inv.counts
    failed = [c for c in inv.checks if not c["passed"]]
    out = [f"# FGP association inventory — gate 1 ({meta.get('run_at', '')})", ""]
    out.append(f"**Result: {'FAIL' if failed else 'PASS'}** — {len(inv.checks) - len(failed)} / {len(inv.checks)} checks passed.")
    out.append("")
    out.append("| Check | Result | Expected | Actual | What it guards |")
    out.append("|---|---|---|---|---|")
    for c in inv.checks:
        out.append(f"| `{c['name']}` | {'pass' if c['passed'] else '**FAIL**'} | {c['expected']} | {c['actual']} | {c['note']} |")
    out.append("")
    out.append("## Headline counts")
    out.append("")
    out.append("| Key | Value |")
    out.append("|---|---|")
    for k in sorted(C):
        out.append(f"| `{k}` | {C[k]:,} |" if isinstance(C[k], int) else f"| `{k}` | {C[k]} |")
    out.append("")
    out.append("## Multi-association classes and the rule declared for each (proposals; gate 2 confirms)")
    out.append("")
    out.append("| Class | Files | Rule |")
    out.append("|---|---|---|")
    for k, rule in RESOLUTION_RULES.items():
        out.append(f"| `{k}` | {C.get('multi.' + k, 0):,} | {rule} |")
    if C.get("multi.unclassified"):
        out.append(f"| `unclassified` | {C['multi.unclassified']:,} | **NONE — gate fails** |")
    out.append("")
    out.append("## Inputs")
    out.append("")
    for k, v in meta.items():
        out.append(f"- `{k}`: {v}")
    out.append("")
    return "\n".join(out)


# --------------------------------------------------------------------------- main


def parse_expectations(items: list, path: str | None = None) -> dict:
    out = {}
    if path:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            raise SystemExit(f"--expect-file must hold a JSON object: {path}")
        for k, v in data.items():
            if k.startswith("_"):
                continue
            if not isinstance(v, int) or isinstance(v, bool):
                raise SystemExit(f"--expect-file value must be an integer: {k}={v!r}")
            out[k] = v
    for item in items or ():
        if "=" not in item:
            raise SystemExit(f"--expect needs KEY=VALUE, got {item!r}")
        k, v = item.split("=", 1)
        try:
            out[k.strip()] = int(v.strip())
        except ValueError:
            raise SystemExit(f"--expect value must be an integer: {item!r}") from None
    return out


def main(argv: list | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--nopos", default=DEFAULT_NOPOS)
    ap.add_argument("--withpos", default=DEFAULT_WITHPOS)
    ap.add_argument("--fist", default=DEFAULT_FIST)
    ap.add_argument("--fjms", default=DEFAULT_FJMS)
    ap.add_argument("--nli", default=DEFAULT_NLI)
    ap.add_argument("--fgp-db", default=DEFAULT_FGP_DB)
    ap.add_argument("--out-dir", default=DEFAULT_OUT)
    ap.add_argument("--expect", action="append", metavar="KEY=VALUE",
                    help="a count the run must reproduce; repeatable; the gate fails on any mismatch")
    ap.add_argument("--expect-file", metavar="PATH",
                    help="a JSON object of KEY: VALUE expectations (keys starting with _ are ignored)")
    args = ap.parse_args(argv)
    try:
        expectations = parse_expectations(args.expect, args.expect_file)
    except (SystemExit, OSError, ValueError) as ex:
        print(ex, file=sys.stderr)
        return 2
    for p in (args.nopos, args.withpos):
        if not os.path.isdir(p):
            print(f"not a directory: {p}", file=sys.stderr)
            return 2
    t0 = time.time()
    meta = {"run_at": time.strftime("%Y-%m-%d %H:%M:%S"), "nopos": args.nopos, "withpos": args.withpos,
            "fist": args.fist, "fjms": args.fjms, "nli": args.nli, "fgp_db": args.fgp_db}
    log("scanning folder A ...")
    nopos = scan_nopos(args.nopos)
    log(f"  {nopos.total_entries:,} entries, {len(nopos.files):,} basenames ({time.time() - t0:.0f}s)")
    log("scanning folder B ...")
    b_dirs, b_stray = scan_withpos(args.withpos)
    log(f"  {len(b_dirs):,} directories ({time.time() - t0:.0f}s)")
    log("loading FIST ...")
    with ro(args.fist) as conn:
        fist = load_fist(conn)
    log(f"  {fist.filename_rows:,} unit rows with a FileName, {len(fist.sig):,} signatures ({time.time() - t0:.0f}s)")
    log("loading today's sidecar, fjms and nli ...")
    with ro(args.fgp_db) as conn:
        today = load_today(conn)
    numbers = {n for d in b_dirs for n in d.numbers}
    with ro(args.fjms) as conn:
        fjms = load_fjms(conn, numbers)
    with ro(args.nli) as conn:
        nli = load_nli(conn, numbers)
    log(f"  {len(today):,} rows, fjms {len(fjms):,} / nli {len(nli):,} of {len(numbers):,} numbers ({time.time() - t0:.0f}s)")
    log("building the inventory ...")
    inv = build_inventory(nopos, b_dirs, b_stray, fist, today, fjms, nli)
    checks = run_checks(inv, expectations)
    db_path = write_outputs(inv, args.out_dir, meta)
    failed = [c for c in checks if not c["passed"]]
    for c in checks:
        log(f"  [{'ok  ' if c['passed'] else 'FAIL'}] {c['name']}: expected {c['expected']}, got {c['actual']}")
    log(f"wrote {db_path} ({time.time() - t0:.0f}s)")
    log(f"RESULT: {'FAIL' if failed else 'PASS'} ({len(checks) - len(failed)}/{len(checks)})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
