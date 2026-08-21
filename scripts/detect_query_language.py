# -*- coding: utf-8 -*-
"""Label a query as Hebrew or Judeo-Arabic, and prove the label at query length.

Why. The default-flip decision requires non-inferiority in every named
protected class, and language is one of them. No evaluation stratum for
Judeo-Arabic existed: the witness oracle carries 0 rows from the Judeo-Arabic
corpus, so the stratum has to come from the query text itself.

Judeo-Arabic is written in Hebrew script, so script detection is useless. Two
orthographic signals were measured on labelled works:

  geresh-marked letters (Arabic phonemes)  WEAK -- Hebrew uses the geresh too,
      for abbreviation and numerals, so it does not separate (F1 0.776).
  Arabic definite-article prefix rate      the classifier.

Two corrections were needed before the article rate could be trusted, and both
are the reason this file reports what it reports:

1. Precision is not a property of the classifier here. The first validation
   sample was 80% Judeo-Arabic, and no query set has that balance. Precision
   moves with the class prior, so what is recorded and asserted is RECALL and
   the FALSE-POSITIVE RATE, which do not. Precision is derived from them at a
   stated prior, never reported bare.

2. The rule matched Hebrew words that merely begin with the same two letters
   -- the divine name, demonstratives, several common names. That was the whole
   false-positive channel. Excluding the head of that distribution (STOP below)
   cuts the false-positive rate by roughly 10x AT HIGHER RECALL:

       without stoplist, threshold 0.02674 : recall 0.886  FPR 0.0744
       with stoplist,    threshold 0.019   : recall 0.868  FPR 0.0076

   Measured on 28,644 Judeo-Arabic and 6,946 Hebrew 600-character windows.
   Implied precision with the stoplist: 0.987 at a 40% prior, 0.966 at 20%,
   0.927 at 10% -- robust, which the first version was not.

The threshold is fitted on windows, not whole works, because it is applied to
short query passages where the rate is noisier. `--validate` re-measures at the
query length in use: a classifier validated only at document length would be a
claim about a different input than the one it scores.

Usage:
  python scripts/detect_query_language.py --validate \
      --ja-dir <dir> --he-dir <dir> [--window 600] [--he-sample 300]
  python scripts/detect_query_language.py --label-queries Q.jsonl --out Q2.jsonl
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys

WORD = re.compile('[א-ת]+')
# Reference-corpus metadata blocks; see scripts/build_witness_query_set.py.
HEADER = re.compile(r'##(?:[^#\n]|#(?!#))*##')

ALEF, LAMED = 'א', 'ל'

# Frequent Hebrew words that begin with the same two letters as the Arabic
# article. Excluding them is what makes the false-positive rate low enough for
# the classifier's precision to survive a realistic class prior. Not
# exhaustive by design -- it removes the head of the distribution, and the
# residual rate is measured rather than assumed.
STOP = frozenset((
    'אלהים', 'אלוהים', 'אלהי', 'אלוהי', 'אלהיו', 'אלהיך', 'אלהינו',
    'אלה', 'אלו', 'אלין', 'אליו', 'אליה', 'אליהם', 'אליהן', 'אליך',
    'אלי', 'אלא', 'אלף', 'אלפים', 'אלפי', 'אליהו', 'אלישע', 'אלעזר',
    'אלכסנדר', 'אלמנה', 'אלמנות', 'אלם', 'אלול', 'אלון', 'אלגביש',
    'אלישבע', 'אלקנה', 'אליעזר', 'אלימלך', 'אלכסנדריה', 'אלנתן',
))

# Fitted on 600-character windows at the FPR <= 1% operating point; re-validate
# with --validate before trusting it at a different query length.
AL_THRESHOLD = 0.019
MIN_WORDS = 30          # below this the rate is too noisy to label at all


def al_rate(text: str) -> tuple:
    """-> (article-prefix rate per word, word count), stoplist applied."""
    t = HEADER.sub(' ', text)
    words = WORD.findall(t)
    if not words:
        return (0.0, 0)
    hits = sum(1 for w in words
               if len(w) >= 3 and w[0] == ALEF and w[1] == LAMED
               and w not in STOP)
    return (hits / len(words), len(words))


def label(text: str, threshold: float = AL_THRESHOLD) -> str:
    rate, n = al_rate(text)
    if n < MIN_WORDS:
        return 'unknown'
    return 'ja' if rate >= threshold else 'he'


def _read(path: str) -> str:
    # The long-path prefix: some of these filenames exceed Windows MAX_PATH.
    prefix = '\\\\?\\'
    with open(prefix + os.path.abspath(path), encoding='utf-8',
              errors='replace') as fh:
        return fh.read()


def _windows(text: str, size: int) -> list:
    """Non-overlapping windows of `size` characters, header-stripped first."""
    t = HEADER.sub(' ', text)
    return [t[i:i + size] for i in range(0, max(0, len(t) - size), size)]


def validate(ja_dir: str, he_dir: str, window: int, he_sample: int,
             seed: int, threshold: float) -> int:
    random.seed(seed)
    pos, neg = [], []
    for fn in sorted(os.listdir(ja_dir)):
        if fn.endswith('.txt'):
            pos.extend(_windows(_read(os.path.join(ja_dir, fn)), window))
    he = [f for f in os.listdir(he_dir) if f.endswith('.txt')]
    random.shuffle(he)
    for fn in he[:he_sample]:
        neg.extend(_windows(_read(os.path.join(he_dir, fn)), window))

    def score(rows):
        out = {'ja': 0, 'he': 0, 'unknown': 0}
        for t in rows:
            out[label(t, threshold)] += 1
        return out

    p, n = score(pos), score(neg)
    recall = p['ja'] / (p['ja'] + p['he']) if p['ja'] + p['he'] else 0.0
    fpr = n['ja'] / (n['ja'] + n['he']) if n['ja'] + n['he'] else 0.0
    print(f'window={window} chars   threshold={threshold}')
    print(f'  Judeo-Arabic windows n={len(pos):,}  '
          f'labelled ja={p["ja"]:,} he={p["he"]:,} unknown={p["unknown"]:,}')
    print(f'  Hebrew windows       n={len(neg):,}  '
          f'labelled ja={n["ja"]:,} he={n["he"]:,} unknown={n["unknown"]:,}')
    print(f'  recall {recall:.3f}   false-positive rate {fpr:.4f}')
    # Precision depends on the class prior, so it is derived at several priors
    # rather than reported once from this sample's own balance, which is not
    # the balance of any query set.
    print('  implied precision by true Judeo-Arabic share:')
    for prior in (0.5, 0.4, 0.3, 0.2, 0.1, 0.05):
        num = prior * recall
        den = num + (1 - prior) * fpr
        if den:
            print(f'    prior {prior:.0%} -> {num / den:.3f}')
    if not pos or not neg:
        print('  INSUFFICIENT LABELLED DATA -- no verdict', file=sys.stderr)
        return 1
    return 0


def label_queries(path: str, out_path: str | None, threshold: float) -> int:
    counts = {'ja': 0, 'he': 0, 'unknown': 0}
    rows = []
    with open(path, encoding='utf-8') as fh:
        for line in fh:
            d = json.loads(line)
            lang = label(d.get('text') or '', threshold)
            counts[lang] += 1
            d.setdefault('strata', {})['language'] = lang
            rows.append(d)
    total = max(1, sum(counts.values()))
    print(f'{path}: {sum(counts.values()):,} queries')
    for k in ('he', 'ja', 'unknown'):
        print(f'  {k:8s} {counts[k]:6,d}  ({counts[k] / total:.1%})')
    if out_path:
        with open(out_path, 'w', encoding='utf-8') as fh:
            for d in rows:
                fh.write(json.dumps(d, ensure_ascii=False) + '\n')
        print(f'wrote {out_path}')
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--validate', action='store_true')
    ap.add_argument('--ja-dir')
    ap.add_argument('--he-dir')
    ap.add_argument('--window', type=int, default=600)
    ap.add_argument('--he-sample', type=int, default=300)
    ap.add_argument('--seed', type=int, default=7)
    ap.add_argument('--threshold', type=float, default=AL_THRESHOLD)
    ap.add_argument('--label-queries')
    ap.add_argument('--out')
    args = ap.parse_args()

    if args.validate:
        if not (args.ja_dir and args.he_dir):
            print('--validate needs --ja-dir and --he-dir', file=sys.stderr)
            return 2
        rc = validate(args.ja_dir, args.he_dir, args.window,
                      args.he_sample, args.seed, args.threshold)
        if rc:
            return rc
    if args.label_queries:
        return label_queries(args.label_queries, args.out, args.threshold)
    if not args.validate:
        print('nothing to do: pass --validate and/or --label-queries',
              file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
