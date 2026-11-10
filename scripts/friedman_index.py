#!/usr/bin/env python3
"""Rows of Friedman's index of forms ("מפתח ערכים ושורשיהם") from the recovered extraction.

The 26-page index lists a SELECTION of forms, verbs in the past form, each with the root(s)
the dictionary files it under, dotted letters marked (its own introduction says so; the
dictionary's entries are not all listed). ``prepare_friedman_dictionary.py build`` recovered
its letters in memory from the pinned PDF; this module reads that recovery
(``candidate/Value_Keys.json``, ``letter_recovery.pages``) and turns each printed line into a
row ``form -> roots``: three columns per page read right to left, the ToUnicode tilde the
recovery left for the upper dot turned back into U+0307, the mirrored parentheses of the
broken font read as the brackets they are.

These rows are form-to-root CANDIDATES for lookup, not a morphological analyzer: an ambiguous
form carries several roots (``אחד )אחד, חדד(``), a form without a root is a word the book
files by its letters, and nothing here says how a form inflects.

    python scripts/friedman_index.py build
    python scripts/friedman_index.py verify
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.friedman_units import dotted_key as _dotted_key  # noqa: E402
from scripts.prepare_friedman_dictionary import (  # noqa: E402
    CREDIT_HE, INDEX_RECOVERY_SHA256, VIEWER_BASE, read_json, sha256, write_json)
from shared.blau_normalize import NORMALIZER_VERSION, normalize_ja  # noqa: E402

INDEX_SCHEMA = "friedman-index-1"
INDEX_VERSION = 1
STATUS = "form_to_root_candidate_not_reviewed"
COLUMN_EDGES = (380.0, 260.0)  # a line ending right of the first edge is in column 0, of the second in column 1
PARAGRAPH_WIDTH = 200.0        # the introduction's lines are wider than any column
ROW_RE = re.compile(r"^(?P<form>.+?)\s*\)(?P<roots>[^()]*)\($")
PAGE_NUMBER_RE = re.compile(r"^\[(\d{1,4})\]$")
RUNNING_HEAD = "מפתח ערכים ושורשיהם"
# The recovery maps the font's glyph names, so the upper dot arrives as the "tilde" glyph and
# the bar as "macron", each followed by the font's own blank. Both are marks on the letter before.
MARK_GLYPHS = {"˜": "̇", "¯": "̄"}


def marks_restored(text: str) -> str:
    return re.sub(r"([˜¯])\s?", lambda m: MARK_GLYPHS[m.group(1)], text)


def dotted_key(text: str) -> str:
    """The units' dotted key (ג̇ distinct from ג) over the restored marks."""
    return _dotted_key(marks_restored(text))


def parse_line(text: str) -> dict | None:
    """One printed line of the index as a row, or None for anything that is not a row."""
    text = text.strip()
    if not text or text == RUNNING_HEAD or PAGE_NUMBER_RE.match(text):
        return None
    match = ROW_RE.match(text)
    if match:
        form, roots_text = match.group("form"), match.group("roots")
    elif "(" in text or ")" in text or any(unicodedata.category(ch) == "Nd" for ch in text):
        return None
    else:
        form, roots_text = text, ""
    roots = []
    for group in [g.strip() for g in roots_text.split(",") if g.strip()]:
        alternatives = [a.strip() for a in group.split("/") if a.strip()]
        for alternative in alternatives:
            restored = marks_restored(alternative)
            roots.append({"text": restored, "dotted": dotted_key(restored), "key": normalize_ja(restored),
                          "alternatives": len(alternatives)})
    form_restored = marks_restored(form)
    return {"form": form_restored, "form_key": normalize_ja(form_restored), "roots": roots, "line": text}


def page_rows(page: dict) -> tuple[list[dict], str | None, int]:
    """Rows of one recovered page in reading order: right column first, top to bottom."""
    lines = []
    label = None
    skipped = 0
    for block in page["blocks"]:
        for line in block["lines"]:
            text = "".join(span["text"] for span in line["spans"]).strip()
            x0, y0, x1, y1 = line["bbox"]
            number = PAGE_NUMBER_RE.match(text)
            if number:
                label = number.group(1)
                continue
            if not text or text == RUNNING_HEAD:
                continue
            if x1 - x0 > PARAGRAPH_WIDTH:
                skipped += 1  # the introduction on the first page
                continue
            column = 0 if x1 >= COLUMN_EDGES[0] else 1 if x1 >= COLUMN_EDGES[1] else 2
            lines.append((column, y0, text, [round(v, 1) for v in line["bbox"]]))
    lines.sort(key=lambda item: (item[0], item[1]))
    rows = []
    for column, _, text, bbox in lines:
        row = parse_line(text)
        if row is None:
            skipped += 1
            continue
        row.update({"column": column, "bbox": bbox})
        rows.append(row)
    return rows, label, skipped


def build_rows(extraction: dict) -> dict:
    recovery = extraction.get("letter_recovery") or {}
    if extraction.get("source_sha256") != INDEX_RECOVERY_SHA256 or "pages" not in recovery:
        raise ValueError("The index extraction is not the recovered, pinned source")
    rows = []
    pages = []
    skipped = 0
    for page in recovery["pages"]:
        page_rows_, label, skipped_here = page_rows(page)
        skipped += skipped_here
        for order, row in enumerate(page_rows_):
            row.update({"pdf_page": page["pdf_page"], "printed_page": label, "order": order,
                        "url": f"{VIEWER_BASE}Value_Keys.pdf#page={page['pdf_page']}"})
        rows.extend(page_rows_)
        pages.append({"pdf_page": page["pdf_page"], "printed_page": label, "rows": len(page_rows_)})
    keys = [row["form_key"] for row in rows]
    inversions = sum(a > b for a, b in zip(keys, keys[1:]) if a and b)
    return {"schema": INDEX_SCHEMA, "index_version": INDEX_VERSION, "normalizer_version": str(NORMALIZER_VERSION),
            "status": STATUS, "credit_he": CREDIT_HE, "built_at": datetime.now(timezone.utc).isoformat(),
            "source_sha256": INDEX_RECOVERY_SHA256, "pages": pages, "rows": rows,
            "summary": {"rows": len(rows), "with_roots": sum(bool(r["roots"]) for r in rows),
                        "several_roots": sum(len(r["roots"]) > 1 for r in rows),
                        "distinct_forms": len({r["form_key"] for r in rows}),
                        "distinct_roots": len({root["dotted"] for r in rows for root in r["roots"]}),
                        "dotted_roots": sum(any("̇" in root["dotted"] for root in r["roots"]) for r in rows),
                        "lines_skipped": skipped, "order_inversions": inversions}}


def build(directory: Path) -> dict:
    candidate = directory / "candidate"
    extraction = read_json(candidate / "Value_Keys.json")
    result = build_rows(extraction)
    result["extraction_sha256"] = sha256((candidate / "Value_Keys.json").read_bytes())
    write_json(candidate / "index_forms.json", result)
    return result["summary"]


def verify(directory: Path) -> dict:
    """The rows are exactly what the retained recovery yields under the current code."""
    candidate = directory / "candidate"
    stored = read_json(candidate / "index_forms.json")
    if stored.get("schema") != INDEX_SCHEMA or stored.get("index_version") != INDEX_VERSION:
        raise ValueError("Index rows must be rebuilt with the current index version")
    if stored.get("normalizer_version") != str(NORMALIZER_VERSION):
        raise ValueError("Index rows must be rebuilt with the current normalizer")
    data = (candidate / "Value_Keys.json").read_bytes()
    if stored.get("extraction_sha256") != sha256(data):
        raise ValueError("Index rows are not bound to the retained extraction")
    fresh = build_rows(json.loads(data))
    for key in ("rows", "pages", "summary", "source_sha256", "credit_he"):
        if fresh[key] != stored.get(key):
            raise ValueError(f"Index rows differ from the retained recovery: {key}")
    return {"verified": True, **stored["summary"], "status": stored["status"]}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("build", "verify"))
    parser.add_argument("--directory", type=Path, default=ROOT / "friedman_data")
    args = parser.parse_args(argv)
    try:
        result = build(args.directory) if args.command == "build" else verify(args.directory)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (OSError, ValueError, KeyError) as exc:
        print(f"Friedman index recovery failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
