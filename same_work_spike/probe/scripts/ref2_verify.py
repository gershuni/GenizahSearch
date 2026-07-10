# -*- coding: utf-8 -*-
"""REF-2 verification: ref_corpus_v2.pkl integrity vs v1.

  1. no duplicate work ids
  2. every v1 id present in v2; 20 random v1 works spot-checked for
     byte-identical streams (and cat/title untouched)
  3. new (REF2:) works' streams are pure Hebrew base-letter streams
     (finals folded away by norm_stream); print 100-char slices of 3
  4. totals by category

Run: python -X utf8 -u ref2_verify.py
"""
import os
import pickle
import random
import re
import sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
PROBE = os.path.dirname(HERE)
V1_PKL = os.path.join(PROBE, 'data', 'ref_corpus.pkl')
V2_PKL = os.path.join(PROBE, 'data', 'ref_corpus_v2.pkl')

HEB_STREAM_RE = re.compile(r'^[א-ת]+$')
FINALS = set('ךםןףץ')


def main():
    v1 = pickle.load(open(V1_PKL, 'rb'))
    v2 = pickle.load(open(V2_PKL, 'rb'))
    print(f'v1: {len(v1)} works, v2: {len(v2)} works', flush=True)

    # 1. no duplicate ids
    ids = [w['id'] for w in v2]
    assert len(ids) == len(set(ids)), 'duplicate ids in v2'
    print('OK: no duplicate work ids', flush=True)

    # 2. every v1 id present; 20 random spot-checks of stream identity
    v2_by_id = {w['id']: w for w in v2}
    missing = [w['id'] for w in v1 if w['id'] not in v2_by_id]
    assert not missing, f'v1 ids missing from v2: {missing[:5]}'
    print(f'OK: all {len(v1)} v1 ids present in v2', flush=True)
    rng = random.Random(29)  # deterministic (SEED-029)
    for w in rng.sample(v1, 20):
        w2 = v2_by_id[w['id']]
        assert w2['stream'] == w['stream'], f'stream drift: {w["id"]}'
        assert w2.get('cat') == w.get('cat'), f'cat drift: {w["id"]}'
        assert w2.get('title') == w.get('title'), f'title drift: {w["id"]}'
    print('OK: 20 random v1 works spot-checked -- streams identical, '
          'cat/title untouched', flush=True)

    # 3. new works: Hebrew base-letter streams, finals folded
    new = [w for w in v2 if w['id'].startswith('REF2:')]
    assert new, 'no REF2 works found in v2'
    for w in new:
        s = w['stream']
        assert HEB_STREAM_RE.match(s), f'non-Hebrew chars in {w["id"]}'
        assert not (set(s) & FINALS), f'unfolded finals in {w["id"]}'
        assert len(s) >= 200, f'short stream: {w["id"]}'
        assert w['cat'] in ('Targum', 'Liturgy', 'Sefaria'), w['cat']
    print(f'OK: {len(new)} REF2 works -- pure Hebrew letter streams '
          f'(finals folded), cats valid', flush=True)

    print('\nsample 100-char stream slices:')
    for wid in ('REF2:targum_onkelos_genesis', 'REF2:liturgy_birkat_hamazon',
                'REF2:b2_keter_malkhut'):
        s = v2_by_id[wid]['stream']
        print(f'  {wid} ({len(s):,} letters):')
        print(f'    {s[1000:1100]}')

    # 4. totals
    cats = Counter(w['cat'] for w in v2)
    print(f'\ncategories: {dict(cats)}')
    n_letters_new = sum(len(w['stream']) for w in new)
    n_letters_all = sum(len(w['stream']) for w in v2)
    vgrouped = [w['id'] for w in v2 if w.get('vgroup') is not None]
    print(f'new letters: {n_letters_new:,} | corpus letters: '
          f'{n_letters_all:,}')
    print(f'works with vgroup: {len(vgrouped)} '
          f'({sorted(set(w.get("vgroup") for w in v2) - {None})} groups)')
    print('\nALL CHECKS PASSED')


if __name__ == '__main__':
    main()
