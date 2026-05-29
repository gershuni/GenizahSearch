"""Spike 001 follow-up — reorder-VISIBLE CURRENT-vs-MEIRI text diff.

The metric comparison (compare_extractors.py) is blind to word reordering. This
script aligns the two extractors' output line-by-line with difflib and shows
every line where they differ as a CUR/MEI pair, so a human can judge which
reading order / spacing is correct. Equal lines are shown with '=' for context.

Usage (from project root):
    python .planning/spikes/001-meiri-glyph-reorder-vs-current/diff_batch.py

Writes a single combined report to out/DIFF_BATCH.txt and prints a per-page
summary (lines changed) to stdout.
"""
from __future__ import annotations

import difflib
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import compare_extractors as C  # noqa: E402

OUT = os.path.join(HERE, "out", "DIFF_BATCH.txt")

DB = r"C:\Users\gersh\Dropbox"
# (pdf path, [pages to diff]) — pages chosen for fragmentation/interest + the cited ones
BATCH = [
    (DB + r"\ספרים\בית שני\יוספוס\פלביוס_יוסיפוס_,_אברהם_שליט_קדמוניות_היהודים_1_1944,_Bialik_Institute.pdf",
     [73, 224]),
    (DB + r"\ספרים\ספריה\הגות\הגות יהודית\מאת הרמבם ועליו\מאת הרמבם\איגרות הרמבם - שילת.pdf",
     [242, 520]),
    (DB + r"\מאמרים והרצאות\חיפה 2023\ירחי משוח מלחמה.pdf",
     [60, 100]),
    (DB + r"\ספרים\בית שני\Qumran\Parry & Tov 2004 - Dead Sea Scrolls Reader 4.pdf",
     [132, 186]),
    (DB + r"\ספרים\מדרש\שמות רבה פרשות א-יד - מהדורת שנאן.pdf",
     [115, 161]),
]


def _lines(text: str) -> list[str]:
    return [ln.strip() for ln in text.replace("\n\n", "\n").split("\n") if ln.strip()]


def diff_page(cur: str, mei: str) -> tuple[list[str], int]:
    a, b = _lines(cur), _lines(mei)
    sm = difflib.SequenceMatcher(a=a, b=b, autojunk=False)
    out: list[str] = []
    changed = 0
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            for ln in a[i1:i2]:
                out.append(f"=   {ln}")
        else:
            changed += max(i2 - i1, j2 - j1)
            for ln in a[i1:i2]:
                out.append(f"CUR  {ln}")
            for ln in b[j1:j2]:
                out.append(f"MEI  {ln}")
            out.append("    ~~~")
    return out, changed


def main() -> int:
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    report: list[str] = [
        "CURRENT-vs-MEIRI reorder-visible diff. '=' identical; CUR/MEI pairs differ.",
        "Judge which reading order + word-spacing is correct.\n",
    ]
    print(f"{'PDF':<34} {'page':>4} {'changed lines':>13}")
    print("-" * 56)
    for pdf, pages in BATCH:
        name = os.path.basename(pdf)
        if not os.path.exists(pdf):
            print(f"!! MISSING {pdf}", file=sys.stderr)
            continue
        tgt = set(pages)
        cur = C.current_extract_sampled(pdf, tgt)
        mei = C.meiri_extract_sampled(pdf, tgt)
        report.append("\n" + "#" * 70)
        report.append(f"# {name}")
        report.append("#" * 70)
        for p in pages:
            block, changed = diff_page(cur.get(p, ""), mei.get(p, ""))
            print(f"{name[:33]:<34} {p:>4} {changed:>13}")
            report.append(f"\n===== page {p}  ({changed} changed lines) =====")
            report.extend(block)
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(report))
    print(f"\n-> wrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
