"""Census of the Word features that hold new FGP rows out of the candidate sidecar.

Gate 4 (``scripts/fgp_gate4_chooser_validation.py``, rule R5) holds a new DOCX row when gate 3's
structure census found tables, footnotes, hidden runs, tracked changes or auto-numbering in the
file (``docx_structure``). This script says what those features hold -- rows and units per
feature and per combination, the teams, what the hidden runs contain, the table shapes, the
numbering formats -- and how gate 3 fared on today's rows that carry the same feature. The
rules drafted from it are ``docs/plans/FGP_DOCX_STRUCTURE_RULES.md``.

Usage: python scripts/fgp_docx_structure_census.py [--out fgp_data/inventory/docx_structure_census.json]
Reads gate3.db, gate4.db, inventory.db, FIST.db and the FIST file store; writes nothing else.
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import re
import sqlite3
import sys
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lxml import etree  # noqa: E402

from scripts import fgp_gate3_text_pipeline as g3  # noqa: E402

W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
FEATS = ("hidden_runs", "numbered_paragraphs", "tables", "footnotes", "tracked_changes")
HEB = re.compile(r"[\u05d0-\u05ea\u0600-\u06ff]")
CODE12 = re.compile(r"^\d{12}$")
NUMBER = re.compile(r"^\d{1,4}\s*$")
TALMUD = re.compile(r"^\d+\s+מסכת")


def ro(path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def hidden_kind(t: str) -> str:
    """What a hidden run holds: the Nuscha project's apparatus kinds, or Hebrew text."""
    s = t.strip()
    if not s:
        return "blank"
    if CODE12.match(s):
        return "code12"
    if NUMBER.match(s):
        return "number"
    if TALMUD.match(s):
        return "talmud_ref"
    if HEB.search(s):
        return "hebrew_short" if len(HEB.findall(s)) <= 12 else "hebrew_long"
    return "latin_or_id"


def hidden_runs(root) -> list:
    out = []
    for r in root.iter(W + "r"):
        rpr = r.find(W + "rPr")
        v = rpr.find(W + "vanish") if rpr is not None else None
        if v is not None and (v.get(W + "val") or "true") not in ("0", "false"):
            out.append("".join(x.text or "" for x in r.iter(W + "t")))
    return out


def table_shapes(root) -> list:
    out = []
    for tbl in root.iter(W + "tbl"):
        rows = list(tbl.iter(W + "tr"))
        out.append((len(rows), max((len(list(tr.iter(W + "tc"))) for tr in rows), default=0)))
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--inventory-dir", default="fgp_data/inventory")
    ap.add_argument("--fist", default="fist_data/FIST.db")
    ap.add_argument("--store", default=r"C:\FGPTranscriptions_NoPOSInfo")
    ap.add_argument("--out", default="fgp_data/inventory/docx_structure_census.json")
    a = ap.parse_args(argv)

    g3db = ro(os.path.join(a.inventory_dir, "gate3.db"))
    g4db = ro(os.path.join(a.inventory_dir, "gate4.db"))
    inv = ro(os.path.join(a.inventory_dir, "inventory.db"))
    fist = ro(a.fist)
    teams = {str(r[0]): r[1] for r in fist.execute("SELECT TeamCode, EngDesc FROM dbo_CodeSource")}

    rk_unit = {r[0]: (r[1], r[2], r[3]) for r in g3db.execute(
        "SELECT row_key, unit_table, unit_id, basename FROM text_rows WHERE today_id IS NULL")}
    unit_path = {(r[0], r[1]): r[2] for r in g3db.execute(
        "SELECT unit_table, unit_id, source_path FROM unit_texts WHERE reader='docx'")}
    unit_team = {}
    for bn, ut, uid, sid in inv.execute("SELECT basename, unit_table, unit_id, source_id FROM file_units"):
        unit_team[(ut, uid)] = str(sid)

    # ---- rows and units per feature and combination
    R = {"held_rows": 0, "held_units": 0, "feature": {}, "combination": {}, "teams": {}}
    feat_rows, feat_units = collections.Counter(), collections.defaultdict(set)
    combo_rows, combo_units = collections.Counter(), collections.defaultdict(set)
    team_rows = collections.defaultdict(collections.Counter)
    all_units, held_basenames = set(), set()
    for key, detail in g4db.execute("SELECT key, detail FROM row_dispositions WHERE disposition='docx_structure'"):
        u = rk_unit.get(key)
        if not u:
            continue
        R["held_rows"] += 1
        d = json.loads(detail or "{}")
        combo = "+".join(sorted(d))
        combo_rows[combo] += 1
        combo_units[combo].add(u[:2])
        all_units.add(u[:2])
        held_basenames.add(u[2])
        team = unit_team.get(u[:2], "?")
        for f in d:
            feat_rows[f] += 1
            feat_units[f].add(u[:2])
            team_rows[f][f"{team} {teams.get(team, '')}".strip()] += 1
    R["held_units"] = len(all_units)
    for f in FEATS:
        R["feature"][f] = {"rows": feat_rows[f], "units": len(feat_units[f]),
                           "teams": dict(team_rows[f].most_common(8))}
    for c, n in combo_rows.most_common():
        R["combination"][c] = {"rows": n, "units": len(combo_units[c])}

    # ---- the PDF yardstick
    has = collections.Counter()
    for bn, hp, hs in inv.execute("SELECT basename, has_pdf, has_scan_pdf FROM files"):
        if bn in held_basenames:
            has["files"] += 1
            has["has_pdf"] += bool(hp)
            has["has_scan_pdf"] += bool(hs)
    R["pdf_beside_docx"] = dict(has)

    # ---- how gate 3 fared on today's DOCX rows with each feature
    R["today_rows_by_feature"] = {}
    today = g3db.execute("SELECT structure, status, classes FROM text_rows WHERE today_id IS NOT NULL AND reader='docx'").fetchall()
    for f in FEATS:
        st, cls, n = collections.Counter(), collections.Counter(), 0
        for structure, status, classes in today:
            if json.loads(structure or "{}").get(f):
                n += 1
                st[status] += 1
                if status == "held":
                    for k in json.loads(classes or "{}"):
                        cls[k] += 1
        R["today_rows_by_feature"][f] = {"rows": n, "status": dict(st), "held_classes": dict(cls.most_common(8))}

    # ---- what the hidden runs contain
    H = {"units": 0, "runs_by_kind": collections.Counter(), "chars_by_kind": collections.Counter(),
         "units_with_hebrew_long": 0, "hebrew_long_samples": [], "hebrew_short_texts": collections.Counter()}
    for u in sorted(feat_units["hidden_runs"]):
        try:
            with zipfile.ZipFile(os.path.join(a.store, unit_path[u])) as z:
                root = etree.fromstring(z.read("word/document.xml"))
        except (KeyError, OSError, zipfile.BadZipFile, etree.XMLSyntaxError):
            continue
        H["units"] += 1
        long_seen = False
        for t in hidden_runs(root):
            k = hidden_kind(t)
            H["runs_by_kind"][k] += 1
            H["chars_by_kind"][k] += len(t)
            if k == "hebrew_long":
                long_seen = True
                if len(H["hebrew_long_samples"]) < 30:
                    H["hebrew_long_samples"].append([os.path.basename(unit_path[u]), t.strip()[:120]])
            elif k == "hebrew_short":
                H["hebrew_short_texts"][t.strip()[:30]] += 1
        H["units_with_hebrew_long"] += long_seen
    R["hidden"] = H

    # ---- table shapes
    T = {"units": 0, "max_columns_per_unit": collections.Counter(), "tables_per_unit": collections.Counter(),
         "multi_column_units": []}
    for u in sorted(feat_units["tables"]):
        try:
            with zipfile.ZipFile(os.path.join(a.store, unit_path[u])) as z:
                root = etree.fromstring(z.read("word/document.xml"))
        except (KeyError, OSError, zipfile.BadZipFile, etree.XMLSyntaxError):
            continue
        T["units"] += 1
        shapes = table_shapes(root)
        T["tables_per_unit"][str(min(len(shapes), 6))] += 1
        mc = max((c for _, c in shapes), default=0)
        T["max_columns_per_unit"][str(mc)] += 1
        if mc >= 2:
            T["multi_column_units"].append([os.path.basename(unit_path[u]), unit_team.get(u, "?"), shapes[:6]])
    R["tables"] = T

    # ---- numbering formats and unrendered numbering
    N = {"units": 0, "level0_formats": collections.Counter(), "unrendered_units": 0, "unrendered_paragraphs": 0}
    for u in sorted(feat_units["numbered_paragraphs"]):
        p = os.path.join(a.store, unit_path[u])
        try:
            with zipfile.ZipFile(p) as z:
                numbering = g3._load_numbering(z)
            d = g3.read_docx(p)
        except (KeyError, OSError, zipfile.BadZipFile, etree.XMLSyntaxError):
            continue
        N["units"] += 1
        for lvls in numbering.values():
            for ilvl, (fmt, text, _start) in lvls.items():
                if ilvl == 0:
                    N["level0_formats"][f"{fmt}:{text}"] += 1
        unr = d.structure.get("numbering_unrendered", 0)
        if unr:
            N["unrendered_units"] += 1
            N["unrendered_paragraphs"] += unr
    R["numbering"] = N

    def plain(o):
        if isinstance(o, collections.Counter):
            return dict(o.most_common())
        if isinstance(o, dict):
            return {k: plain(v) for k, v in o.items()}
        return o

    R = plain(R)
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as fh:
        json.dump(R, fh, ensure_ascii=False, indent=1)
    print(f"held rows {R['held_rows']}, units {R['held_units']}")
    for f in FEATS:
        print(f"  {f}: {R['feature'][f]['rows']} rows / {R['feature'][f]['units']} units; teams {R['feature'][f]['teams']}")
    print("combinations:", {k: v["rows"] for k, v in R["combination"].items()})
    print("pdf beside docx:", R["pdf_beside_docx"])
    print("hidden runs by kind:", R["hidden"]["runs_by_kind"], "| units with a long Hebrew hidden run:",
          R["hidden"]["units_with_hebrew_long"])
    print("tables: max columns per unit", R["tables"]["max_columns_per_unit"])
    print("numbering: formats", R["numbering"]["level0_formats"], "| unrendered units", R["numbering"]["unrendered_units"])
    print("wrote", a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
