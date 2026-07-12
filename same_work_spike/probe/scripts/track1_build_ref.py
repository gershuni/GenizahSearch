# -*- coding: utf-8 -*-
"""Build the Track-1 reference corpus: Maagarim + Friedberg JA -> ref_corpus.pkl.

Maagarim (8,233 txt): filename = author--work--date--genre--Ytext<id>...
  content has ##...## headers (incl. המסירה = source manuscript!, extracted
  to metadata), >> line markers, editorial ?word? / <...> markup — all
  non-Hebrew-letter chars vanish in norm_stream anyway; ## blocks are
  stripped BEFORE normalize so header words don't pollute the stream.
JA (92 per_doc txt): '***\\n<title>\\n---\\n' header then text.
"""
import os
import pickle
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from normalize import norm_stream  # noqa: E402

MAAGARIM = r"C:\Users\gersh\Dropbox\דיקטה\מאגרים\AllTextsOnlyText"
JA_DIR = r"C:\Users\gersh\Dropbox\דיקטה\JA\ערבית יהודית מעובד\per_doc"
OUT = r"C:\Genizahsearch\same_work_spike\probe\data\ref_corpus.pkl"

# Header body may contain a lone '#' ([1050#] date brackets, &#39; entities)
# but never '##' and never a newline — see results/ref_header_bug.md (fixes
# 30,677 letters wrongly deleted from ספר הרקמה + 342 leaked location letters;
# single-# inline text markers #…# preserved byte-for-byte).
HEADER_RE = re.compile(r'##(?:[^#\n]|#(?!#))*##')
# Matches BOTH mesirah forms: plain ##המסירה:…## and section-scoped
# ##סעיף N | המסירה:…## (metadata completeness; stream unaffected).
MESIRAH_RE = re.compile(r'##[^#]*?המסירה:\s*([^#]+?)\s*##')
MIN_LETTERS = 150

CANON_CATS = [  # substring of the work/author fields -> coarse category
    ('מחבר לא ידוע--מקרא', 'Bible'),
    ('מחבר לא ידוע--משנה', 'Mishnah'),
    ('מחבר לא ידוע--תלמוד בבלי', 'Bavli'),
    ('מחבר לא ידוע--תלמוד ירושלמי', 'Yerushalmi'),
    ('מחבר לא ידוע--תוספתא', 'Tosefta'),
]


def main():
    t0 = time.time()
    works = []
    skipped = 0
    for fn in sorted(os.listdir(MAAGARIM)):
        if not fn.endswith('.txt'):
            continue
        # \\?\ prefix: some filenames push the path past Windows MAX_PATH
        raw = open('\\\\?\\' + os.path.join(MAAGARIM, fn), encoding='utf-8',
                   errors='replace').read()
        mes = MESIRAH_RE.search(raw)
        text = HEADER_RE.sub(' ', raw)
        stream, _ = norm_stream(text)
        if len(stream) < MIN_LETTERS:
            skipped += 1
            continue
        base = fn.replace('.txt-OnlyText.txt', '')
        parts = base.split('--')
        author = parts[0] if len(parts) > 0 else ''
        title = parts[1] if len(parts) > 1 else base
        date = parts[2] if len(parts) > 2 else ''
        genre = parts[3] if len(parts) > 3 else ''
        cat = 'Maagarim'
        for pat, c in CANON_CATS:
            if pat in fn:
                cat = c
                break
        works.append({
            'id': f'M:{parts[-1] if parts else fn}',
            'cat': cat, 'author': author, 'title': title,
            'date': date, 'genre': genre,
            'mesirah': mes.group(1).strip() if mes else '',
            'stream': stream,
        })
    n_maagarim = len(works)
    print(f"maagarim: {n_maagarim} works "
          f"({skipped} skipped <{MIN_LETTERS} letters), "
          f"{sum(len(w['stream']) for w in works):,} letters "
          f"({time.time() - t0:.0f}s)", flush=True)

    for fn in sorted(os.listdir(JA_DIR)):
        if not fn.endswith('.txt'):
            continue
        raw = open(os.path.join(JA_DIR, fn), encoding='utf-8',
                   errors='replace').read()
        lines = raw.split('\n')
        title = lines[1].strip() if len(lines) > 1 else fn
        stream, _ = norm_stream(raw)
        if len(stream) < MIN_LETTERS:
            continue
        works.append({
            'id': f'J:{fn[:-4]}', 'cat': 'JA', 'author': '',
            'title': title, 'date': '', 'genre': 'ערבית יהודית',
            'mesirah': '', 'stream': stream,
        })
    print(f"JA: {len(works) - n_maagarim} works, total letters "
          f"{sum(len(w['stream']) for w in works):,}", flush=True)

    from collections import Counter
    print('categories:', dict(Counter(w['cat'] for w in works)))
    print('top genres:', Counter(w['genre'] for w in works).most_common(10))
    with open(OUT, 'wb') as f:
        pickle.dump(works, f, protocol=4)
    print(f"wrote {OUT} ({os.path.getsize(OUT) // 1048576} MB) "
          f"in {time.time() - t0:.0f}s")


if __name__ == '__main__':
    main()
