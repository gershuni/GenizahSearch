"""The Nuscha team's hidden layer, kept in a store of its own (the owner's ruling of 2026-10-26).

The Nuscha (FIST team 200, Talmud text) Word files carry, as hidden runs, a bookkeeping layer the
published text never shows: on each line its line number, the Talmud location of the line
(``1 מסכת ראש השנה (10,2,29)``, spans such as ``פסחים מ לב,א,33 עד לב,ב,43``) and a 12-digit
image/line code (``020160301300``); at the head of the file the record ids, shelfmarks and library
names. Gate 3's reader drops hidden runs, as FGP's own PDF does; this script keeps the layer, raw
and parsed, for a later feature ("where in the Talmud is this line", the positions work).

Usage: python scripts/fgp_nuscha_hidden_layer.py [--out fgp_data/nuscha_hidden_layer.db]
Reads inventory.db (which units are Nuscha's and where their DOCX is) and the FIST file store.
Output tables: ``units`` (one per DOCX unit: header hidden texts, line counts), ``lines`` (one per
paragraph with text or hidden runs: the parsed line number, Talmud location and code, every hidden
run raw, the visible text), ``meta``.
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
import zipfile

from lxml import etree

W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
NUSCHA_TEAM = 200
LINE_NUMBER_RE = re.compile(r"^\d{1,4}$")
CODE12_RE = re.compile(r"^\d{12}$")
TALMUD_RE = re.compile(r"מסכת|\b[א-ת]+,[אב],\d+")


def ro(path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def paragraph_runs(p) -> list:
    """[(hidden: bool, text)] of a paragraph's runs, empty texts dropped."""
    out = []
    for r in p.iter(W + "r"):
        rpr = r.find(W + "rPr")
        v = rpr.find(W + "vanish") if rpr is not None else None
        hidden = v is not None and (v.get(W + "val") or "true") not in ("0", "false")
        t = "".join(x.text or "" for x in r.iter(W + "t"))
        if t:
            out.append((hidden, t))
    return out


def parse_hidden(hidden: list) -> dict:
    """The parsed pieces of a line's hidden runs: its number, Talmud location, 12-digit code."""
    out = {"line_number": None, "talmud_ref": None, "code12": None}
    for t in hidden:
        s = t.strip()
        if not s:
            continue
        if out["line_number"] is None and LINE_NUMBER_RE.match(s):
            out["line_number"] = s
        elif out["code12"] is None and CODE12_RE.match(s):
            out["code12"] = s
        elif out["talmud_ref"] is None and TALMUD_RE.search(s):
            out["talmud_ref"] = s
    return out


def read_layer(path: str) -> tuple:
    """(header_hidden, lines) of one DOCX: the hidden runs of the paragraphs before the first visible text
    are the header; every later paragraph with text or hidden runs is a line."""
    with zipfile.ZipFile(path) as z:
        root = etree.fromstring(z.read("word/document.xml"))
    body = root.find(W + "body")
    header, lines, seen_text = [], [], False
    for p in body.iter(W + "p"):
        runs = paragraph_runs(p)
        if not runs:
            continue
        hidden = [t for h, t in runs if h]
        visible = "".join(t for h, t in runs if not h)
        if not seen_text and not visible.strip():
            header.extend(t for t in hidden if t.strip())
            continue
        seen_text = True
        lines.append({"hidden": hidden, "visible": visible, **parse_hidden(hidden)})
    return header, lines


def main(argv=None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--inventory", default="fgp_data/inventory/inventory.db")
    ap.add_argument("--store", default=r"C:\FGPTranscriptions_NoPOSInfo")
    ap.add_argument("--out", default="fgp_data/nuscha_hidden_layer.db")
    ap.add_argument("--team", type=int, default=NUSCHA_TEAM)
    ap.add_argument("--limit", type=int)
    a = ap.parse_args(argv)
    t0 = time.time()
    inv = ro(a.inventory)
    files = {b: (d, hd) for b, d, hd in inv.execute("SELECT basename, dir, has_docx FROM files")}
    units = {}
    for bn, ut, uid, sid, fp in inv.execute(
            "SELECT basename, unit_table, unit_id, source_id, folder_path FROM file_units WHERE source_id=?", (a.team,)):
        units.setdefault((ut, uid), (bn, (fp or "").strip("\\/")))
    inv.close()
    keys = sorted(units)[: a.limit] if a.limit else sorted(units)
    print(f"{len(keys):,} units of team {a.team}")
    if os.path.exists(a.out):
        os.remove(a.out)
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    con = sqlite3.connect(a.out)
    con.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    con.execute("CREATE TABLE units (unit_table TEXT, unit_id INTEGER, basename TEXT, source_path TEXT, "
                "header_hidden TEXT, lines INTEGER, lines_with_number INTEGER, lines_with_talmud_ref INTEGER, "
                "lines_with_code INTEGER, error TEXT, PRIMARY KEY (unit_table, unit_id))")
    con.execute("CREATE TABLE lines (unit_table TEXT, unit_id INTEGER, line_no INTEGER, line_number TEXT, "
                "talmud_ref TEXT, code12 TEXT, hidden_json TEXT, visible_text TEXT, "
                "PRIMARY KEY (unit_table, unit_id, line_no))")
    C = collections.Counter()
    for n, key in enumerate(keys, 1):
        bn, folder = units[key]
        d, has_docx = files.get(bn, (None, 0))
        path = None
        for cand in dict.fromkeys(x for x in (folder, d) if x):
            p = os.path.join(a.store, cand, "word", bn + ".docx")
            if os.path.isfile(p):
                path = p
                break
        if not path:
            con.execute("INSERT INTO units VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (key[0], key[1], bn, None, None, 0, 0, 0, 0, "no_docx"))
            C["no_docx"] += 1
            continue
        try:
            header, lines = read_layer(path)
        except (KeyError, OSError, zipfile.BadZipFile, etree.XMLSyntaxError) as ex:
            con.execute("INSERT INTO units VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (key[0], key[1], bn, os.path.relpath(path, a.store), None, 0, 0, 0, 0, f"unreadable:{type(ex).__name__}"))
            C["unreadable"] += 1
            continue
        con.execute("INSERT INTO units VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (key[0], key[1], bn, os.path.relpath(path, a.store), json.dumps(header, ensure_ascii=False),
                     len(lines), sum(1 for x in lines if x["line_number"]), sum(1 for x in lines if x["talmud_ref"]),
                     sum(1 for x in lines if x["code12"]), None))
        con.executemany("INSERT INTO lines VALUES (?,?,?,?,?,?,?,?)",
                        ((key[0], key[1], i, x["line_number"], x["talmud_ref"], x["code12"],
                          json.dumps(x["hidden"], ensure_ascii=False), x["visible"]) for i, x in enumerate(lines, 1)))
        C["units"] += 1
        C["lines"] += len(lines)
        C["lines_with_number"] += sum(1 for x in lines if x["line_number"])
        C["lines_with_talmud_ref"] += sum(1 for x in lines if x["talmud_ref"])
        C["lines_with_code"] += sum(1 for x in lines if x["code12"])
        if n % 2000 == 0:
            print(f"  {n:,} units ({time.time() - t0:.0f}s)")
            con.commit()
    for k, v in sorted(C.items()):
        con.execute("INSERT INTO meta VALUES (?,?)", (k, str(v)))
    con.execute("INSERT INTO meta VALUES (?,?)", ("run_at", time.strftime("%Y-%m-%d %H:%M:%S")))
    con.execute("INSERT INTO meta VALUES (?,?)", ("store", a.store))
    con.execute("INSERT INTO meta VALUES (?,?)", ("team", str(a.team)))
    con.commit()
    con.close()
    print(f"wrote {a.out} ({time.time() - t0:.0f}s): " + ", ".join(f"{k} {v:,}" for k, v in sorted(C.items())))
    return 0


if __name__ == "__main__":
    sys.exit(main())
