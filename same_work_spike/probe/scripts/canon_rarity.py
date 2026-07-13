# -*- coding: utf-8 -*-
"""MAPV2-15f — rarity-weighted canonical detector (ACT's cheap idea; no
aligner, no structural machinery, per Hillel).

Each canon n-gram is weighted by IDF over the canon works (rare/distinctive
-> high; ubiquitous phrase -> ~0). A span's 'canonical rarity mass per
length' is the summed IDF of its n-grams that occur in the canon, divided by
its n-gram count. High -> the distinctive content of the span IS canonical
(a shared Bible/Talmud/Mishnah quotation); low -> distinctive NON-canonical
content (discovery-eligible). This suppresses the common-Hebrew-fragment
noise that made a plain n-gram overlap over-fire, so a short (noise-tolerant)
n works.

Validated on the 132 gold: shared median 1.11 vs discovery 0.22; at
mass/len >= 1.0, 16/30 shared caught, 2/26 discovery false.

Keys are crc32 of the utf-8 n-gram (stable across processes, so the IDF
model caches to disk and reloads fast).
"""
import hashlib
import math
import os
import pickle
from collections import defaultdict

PROBE = r'C:\Genizahsearch\same_work_spike\probe'
CANON_PKL = PROBE + r'\data\canon_corpus_maagarim.pkl'
IDF_CACHE = PROBE + r'\data\canon_rarity_idf_v2.pkl'   # v2: 64-bit keys + non-neg IDF
CANON_CATS = {'Bible', 'Mishnah', 'Tosefta', 'Bavli', 'Yerushalmi'}
N = 8
SHARED_TH = 1.5     # mass/len at/above -> canonical-quotation (shared).
# Discovery-safe: on the 132 gold, mass/len>=1.5 flips 0 discoveries (vs 2 at
# 1.0) while still catching the clearest shared quotations. Protecting
# discovery recall outranks catching more shared cheaply; the AI layer takes
# the rest.


def _g(s):
    # 64-bit stable hash (crc32 collided ~thousands of times over ~7M 8-grams;
    # blake2b/8 is collision-free at this scale and stable across processes so
    # the IDF model still caches to disk).
    return int.from_bytes(
        hashlib.blake2b(s.encode('utf-8'), digest_size=8).digest(), 'big')


class CanonRarity:
    def __init__(self, rebuild=False):
        if os.path.exists(IDF_CACHE) and not rebuild:
            self.idf = pickle.load(open(IDF_CACHE, 'rb'))
            return
        works = [w for w in pickle.load(open(CANON_PKL, 'rb'))
                 if w['cat'] in CANON_CATS]
        nw = len(works)
        df = defaultdict(int)
        for w in works:
            s = w['stream']
            seen = set()
            for i in range(len(s) - N + 1):
                seen.add(_g(s[i:i + N]))
            for k in seen:
                df[k] += 1
        # non-negative IDF: log((nw+1)/(df+1)) is >= 0 for all df<=nw, so a gram
        # in every work scores ~0 ("ubiquitous -> ~0"), never negative.
        self.idf = {k: math.log((nw + 1) / (c + 1)) for k, c in df.items()}
        pickle.dump(self.idf, open(IDF_CACHE, 'wb'))

    def mass_per_len(self, span_stream):
        """Rarity-weighted canonical mass per n-gram of the span stream."""
        if not span_stream or len(span_stream) < N:
            return 0.0
        n = len(span_stream) - N + 1
        tot = 0.0
        for i in range(n):
            tot += self.idf.get(_g(span_stream[i:i + N]), 0.0)
        return tot / n

    def is_canonical(self, span_stream, th=SHARED_TH):
        return self.mass_per_len(span_stream) >= th
