# -*- coding: utf-8 -*-
"""Rebuild FGP transcription text quality + populate per-source credit.

Fixes two FGP data-quality issues found in UAT (2026-06-21):

* **Text quality (C):** the original ``fgp_extract.py`` PyMuPDF extraction
  detached Hebrew abbreviation marks (gershayim/geresh) and parens onto their
  own lines and mis-ordered them (RTL). This rebuild replaces ``content`` with:
    1. the pristine source **XML** (``<col><row><t><w>``) where a row's
       ``c_number`` has one (gold path), else
    2. a **Phase-102 RTL re-extraction** of the local PDF
       (``shared.local_indexer.extract_pdf_pages``), else
    3. the original content (guard: also kept if a re-extraction regresses /
       comes back empty).
  Provenance is recorded in a new ``text_source`` column ('xml'|'pdf_rtl'|'original').

* **Credit (A):** populates a new ``source_credit`` column from
  ``fgp_shelfmark_meta.raw_json -> DataSource`` (the FGP team-leader credit,
  e.g. "יעקב זוסמן, ראש צוות FGP לספרות תלמודית"), joined by ``mms_id``.

Operates IN PLACE on the DB path given. Run on a COPY first, validate, then swap.
Idempotent: re-running recomputes from XML/PDF again (original content is read
from the row, so re-running a rebuilt DB would re-extract from source files,
which is fine — source files are the source of truth).

Usage:
    python scripts/fgp_rebuild_text_and_credit.py <db_path> [--limit N] [--report]
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

FGP_DATA = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "fgp_data")
TRANS_ROOT = os.path.join(FGP_DATA, "transcriptions")

_HEB = re.compile(r"[֐-׿]")
_SHORT_MARK = re.compile(r"^[\"'\)\(\.\,\:\;\?\!׳״\s\.]+$")

# FGP's in-house transcription team. The XML (visual) transcriptions carry no
# per-shelfmark DataSource (esp. CUL, whose MetadataOnShelfmark was never
# imported); they are this team's work (user-confirmed), so credit them to it
# when no DataSource credit applies.
XML_DEFAULT_CREDIT = "צוות FGP להעתקות -גנוזות"


def heb_ratio(text: str) -> float:
    if not text:
        return 0.0
    letters = [c for c in text if c.strip()]
    if not letters:
        return 0.0
    return sum(1 for c in letters if _HEB.match(c)) / len(letters)


def break_score(text: str) -> float:
    """Fraction of non-empty lines that are <=2 chars or pure punctuation."""
    if not text:
        return 0.0
    lines = [ln.strip() for ln in text.split("\n")]
    lines = [ln for ln in lines if ln]
    if not lines:
        return 0.0
    short = sum(1 for ln in lines if len(ln) <= 2 or _SHORT_MARK.match(ln))
    return short / len(lines)


def build_xml_index() -> dict:
    """{c_number(str 'C#####') -> xml_path}. First file wins per c_number."""
    idx = {}
    for x in glob.glob(os.path.join(TRANS_ROOT, "**", "*.xml"), recursive=True):
        m = re.search(r"C(\d+)\.", x)
        if not m:
            continue
        cn = "C" + m.group(1)
        idx.setdefault(cn, x)
    return idx


def parse_fgp_xml(path: str) -> str:
    """Clean reading-order academic text from FGP ``<root><col><row><t><w>`` XML.

    The visual XML is the structured, letter-positioned source. Reconstruction:
      * columns in document order, separated by a blank line;
      * rows within a column joined by newline (one manuscript line per text
        line, so the line-number gutter lines up);
      * words within a row concatenated — each ``<w>``'s text already carries its
        trailing space, so word boundaries are preserved EXACTLY (the PDF merges
        them, e.g. "מאי טע'" -> "מאיטע").

    The ``<w>`` content already carries FGP's self-marked academic sigla
    (reconstruction ``[..]``, deletion ``(..)``, correction ``{..}``, uncertainty
    ``?..?``, heading ``*..*``, lacuna dots ``....``), so this faithful
    concatenation reproduces the academic transcription without having to decode
    FGP's 400+ internal class codes. ``ET.fromstring`` unescapes entities (so
    ``<~>`` line-fillers render literally). Runs of whitespace are collapsed
    (manuscript visual alignment is not semantic); empty lines are dropped.
    """
    try:
        root = ET.fromstring(open(path, encoding="utf-8", errors="replace").read())
    except ET.ParseError:
        return ""
    cols_out = []
    cols = root.findall("col") or [root]  # tolerate XML with no <col> wrapper
    for col in cols:
        rows_out = []
        for row in col.iter("row"):  # document order == reading order
            words = [(w.text or "") for w in row.iter("w")]
            line = re.sub(r"[ \t]+", " ", "".join(words)).strip()
            if line:
                rows_out.append(line)
        if rows_out:
            cols_out.append("\n".join(rows_out))
    return "\n\n".join(cols_out).strip()


def _parse_datasource(ds) -> "str | None":
    """Extract a credit string from FGP ``DataSource``.

    The value is NOT real JSON — it's a custom ``{eng: X, heb: Y}`` string whose
    X/Y often contain commas (e.g. "יעקב זוסמן, ראש צוות FGP…"), so it can't be
    split naively. Prefer the Hebrew side, fall back to English / the raw value.
    Also handles a genuine dict, just in case some records differ.
    """
    if not ds:
        return None
    if isinstance(ds, dict):
        v = ds.get("heb") or ds.get("eng")
        return v.strip() if v else None
    s = str(ds).strip()

    def _kv(block: str, key: str, other: str) -> "str | None":
        # value runs from "key:" up to ", <other>:" or end — order-independent,
        # tolerant of commas/semicolons inside the value (Codex LOW).
        m = re.search(rf"{key}\s*:\s*(.*?)(?:\s*,\s*{other}\s*:|$)", block, re.DOTALL)
        return m.group(1).strip() if m else None

    vals = []
    for block in re.findall(r"\{([^{}]*)\}", s):  # each {eng:.., heb:..} pseudo-dict
        v = (_kv(block, "heb", "eng") or _kv(block, "eng", "heb") or "").strip()
        if v and v not in vals:  # multi-source lists often duplicate
            vals.append(v)
    if vals:
        return "; ".join(vals)
    return s.strip("{} ").strip() or None


def build_credit_index(conn: sqlite3.Connection) -> dict:
    """{mms_id -> credit str} from fgp_shelfmark_meta.raw_json DataSource.

    A single mms_id has MANY metadata rows with DIFFERENT credits (Codex HIGH:
    10,980 mms_ids with >1 distinct credit). Aggregate ALL of them — dedupe in
    stable order and join with "; " — instead of last-row-wins (which was both
    lossy and nondeterministic). Ordered by rowid for determinism.
    """
    acc: dict = {}
    try:
        rows = conn.execute(
            "SELECT mms_id, raw_json FROM fgp_shelfmark_meta "
            "WHERE mms_id IS NOT NULL ORDER BY mms_id, rowid"
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    for mms_id, raw in rows:
        if not raw:
            continue
        try:
            j = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        credit = _parse_datasource(j.get("DataSource"))
        if not credit:
            continue
        lst = acc.setdefault(str(mms_id), [])
        for part in credit.split("; "):  # credit may already be "; "-joined
            p = part.strip()
            if p and p not in lst:
                lst.append(p)
    return {k: "; ".join(v) for k, v in acc.items()}


def _collapse_blanks(text: str) -> str:
    """Collapse Phase-102's block-separating blank lines — FGP single-image
    transcriptions are continuous lines, so the extra blanks are spurious."""
    return re.sub(r"\n[ \t]*\n+", "\n", text).strip() if text else text


def reextract_pdf(rel_path: str) -> str:
    from shared.local_indexer import extract_pdf_pages
    fp = os.path.join(TRANS_ROOT, rel_path)
    if not os.path.isfile(fp):
        return ""
    try:
        pages = list(extract_pdf_pages(fp))
    except Exception:
        return ""
    return "\n\n".join(t for _, t, _ in pages).strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("db_path")
    ap.add_argument("--limit", type=int, default=0, help="process only N rows (sample/test)")
    ap.add_argument("--report", action="store_true", help="don't write; just print what would change")
    ap.add_argument("--credit-only", action="store_true",
                    help="only (re)populate source_credit; skip text extraction")
    ap.add_argument("--xml-only", action="store_true",
                    help="only process rows whose c_number has a visual XML "
                         "(fast, targeted pass; leaves PDF-only rows untouched)")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row

    # add columns if missing
    cols = {r[1] for r in conn.execute("PRAGMA table_info(fgp_transcriptions)")}
    if not args.report:
        if "source_credit" not in cols:
            conn.execute("ALTER TABLE fgp_transcriptions ADD COLUMN source_credit TEXT")
        if "text_source" not in cols:
            conn.execute("ALTER TABLE fgp_transcriptions ADD COLUMN text_source TEXT")
        conn.commit()

    print("Building credit + XML indexes ...")
    credit_index = build_credit_index(conn)
    xml_index = build_xml_index()
    print(f"  credit mms_ids: {len(credit_index)} | XML c_numbers: {len(xml_index)}")

    def credit_for(mms_id, c_number) -> "str | None":
        """DataSource credit if known; else the FGP in-house team for any image
        that has a visual XML (== FGP transcription-team work, user-confirmed),
        independent of which text source we end up displaying."""
        ds = credit_index.get(str(mms_id)) if mms_id is not None else None
        if ds:
            return ds
        if c_number and c_number in xml_index:
            return XML_DEFAULT_CREDIT
        return None

    if args.credit_only:
        rows = conn.execute(
            "SELECT id, mms_id, c_number FROM fgp_transcriptions"
        ).fetchall()
        n = 0
        for r in rows:
            credit = credit_for(r["mms_id"], r["c_number"])
            if credit:
                if not args.report:
                    conn.execute(
                        "UPDATE fgp_transcriptions SET source_credit=? WHERE id=?",
                        (credit, r["id"]),
                    )
                n += 1
        if args.report:
            # --report is documented as "don't write" — honor it even here.
            print(f"credit-only REPORT (no write): {n} rows would get a credit.")
        else:
            conn.commit()
            print(f"credit-only: updated {n} rows.")
        conn.close()
        return 0

    sel = "SELECT id, c_number, rel_path, mms_id, sys_id, content FROM fgp_transcriptions"
    if args.limit:
        sel += f" LIMIT {args.limit}"
    rows = conn.execute(sel).fetchall()
    print(f"Processing {len(rows)} rows ...")

    stats = {"xml": 0, "pdf_rtl": 0, "original": 0, "credit": 0, "guard_kept": 0}
    bs_before = bs_after = 0.0
    n_score = 0
    t0 = time.perf_counter()
    updates = []
    for i, r in enumerate(rows):
        orig = r["content"] or ""
        c_number = r["c_number"]
        rel_path = r["rel_path"]
        new_text = ""
        source = "original"

        # --xml-only: skip rows that have no visual XML — leave their PDF text as-is.
        if args.xml_only and not (c_number and c_number in xml_index):
            continue

        # PRIMARY: the visual XML transcription when this folio has one. It is
        # the structured, letter-positioned source — it preserves word spacing
        # and line breaks EXACTLY (the academic PDF merges words, e.g.
        # "מאי טע'" -> "מאיטע"), and its <w> content already carries the
        # self-marked academic sigla (reconstruction [..], deletion (..),
        # correction {..}, uncertainty ?..?, heading *..*, lacuna dots ....).
        # Validated cleaner than the PDF (lower single-letter-token ratio) across
        # all 3,283 XML folios. FALL BACK to the academic PDF only when the XML
        # is absent, truncated, or markedly noisier than the PDF for this folio.
        xml_text = ""
        if c_number and c_number in xml_index:
            xml_text = parse_fgp_xml(xml_index[c_number])
        pdf_text = reextract_pdf(rel_path) if rel_path else ""
        if (xml_text and len(xml_text) >= 0.5 * max(len(orig), 1)
                and (not pdf_text or break_score(xml_text) <= break_score(pdf_text) + 0.05)):
            new_text = xml_text
            source = "xml"
        elif pdf_text and break_score(pdf_text) <= break_score(orig) + 0.02 and len(pdf_text) >= 0.5 * len(orig):
            new_text = _collapse_blanks(pdf_text)
            source = "pdf_rtl"
        else:
            if xml_text or pdf_text:
                stats["guard_kept"] += 1
            new_text = orig
            source = "original"

        credit = credit_for(r["mms_id"], c_number)

        stats[source] += 1
        if credit:
            stats["credit"] += 1

        if source != "original":
            n_score += 1
            bs_before += break_score(orig)
            bs_after += break_score(new_text)

        # null sections on replaced rows so display uses the clean content
        sections_clear = source != "original"
        updates.append((new_text, len(new_text), round(heb_ratio(new_text), 4),
                        credit, source, sections_clear, r["id"]))

        if (i + 1) % 5000 == 0:
            print(f"  ... {i+1}/{len(rows)}  ({time.perf_counter()-t0:.0f}s)")

    if args.report:
        print("REPORT (no write).")
    else:
        print("Writing updates ...")
        for new_text, ln, hr, credit, source, sec_clear, rid in updates:
            if sec_clear:
                conn.execute(
                    "UPDATE fgp_transcriptions SET content=?, content_length=?, heb_ratio=?, "
                    "source_credit=?, text_source=?, sections=NULL WHERE id=?",
                    (new_text, ln, hr, credit, source, rid),
                )
            else:
                conn.execute(
                    "UPDATE fgp_transcriptions SET content=?, content_length=?, heb_ratio=?, "
                    "source_credit=?, text_source=? WHERE id=?",
                    (new_text, ln, hr, credit, source, rid),
                )
        conn.commit()

    dt = time.perf_counter() - t0
    print(f"\nDone in {dt:.0f}s. stats: {stats}")
    if n_score:
        print(f"avg break-score on replaced rows: {bs_before/n_score:.3f} -> {bs_after/n_score:.3f}")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
