"""Page identities of a letter-level (passage) index, for building record masks.

A passage record is one transcribed page; its id is
`{sys_id}_IE{n}_P{######}_FL{n}` (shared/passage_corpus.py). The composition
conditions (docs/plans/COMPOSITION_CONDITIONS_PLAN.md) need the reverse
direction -- "which records are the pages (sys_id, IE, P), or every page of
these manuscripts?" -- to build the mask a condition is searched over
(`shared.passage_search.search_passage(..., record_mask=)`).

`PageIndex` is that reverse map, built once per index BUILD and cached by the
build's identity (a hash of its manifest, which carries the corpus file's
sha256, the counts and the build timings). It holds copied identifiers and
record numbers only, never the index or its memory maps, so caching it cannot
keep a closed index alive. A mask is only ever built against the index it will
be applied to: `mask_for` refuses an index whose build identity differs from
the one the map was built from.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
from dataclasses import dataclass
from typing import Iterable, Optional

import numpy as np

from shared.passage_index import PassageIndex
from shared.sys_id_patterns import CORPUS_SYS_ID_PATTERN

# The record-id grain, anchored. The sys_id part is the corpus pattern (the
# passage index holds the Genizah corpus only; shared/passage_parallels.py).
_RECORD_RE = re.compile(CORPUS_SYS_ID_PATTERN + r'_(IE\d+)_P(\d+)_')


def build_identity(index: PassageIndex) -> str:
    """A content identity for one index build: sha256 of its manifest."""
    payload = json.dumps(index.manifest, sort_keys=True, ensure_ascii=True,
                         default=str)
    return hashlib.sha256(payload.encode('ascii')).hexdigest()


class StaleIndexError(RuntimeError):
    """A page map built for one index build was offered another build."""


@dataclass
class PageIndex:
    build: str
    n_records: int
    sys_records: dict          # sys_id -> np.ndarray of record numbers
    rec_ie: np.ndarray         # record -> code into `ie_codes` (-1: unparsed)
    rec_p: np.ndarray          # record -> page number P
    ie_codes: dict             # IE string -> code
    unparsed: int              # records whose id did not match the grain

    @classmethod
    def build_from(cls, index: PassageIndex) -> 'PageIndex':
        n = index.n_records
        rec_ie = np.full(n, -1, dtype=np.int32)
        rec_p = np.full(n, -1, dtype=np.int32)
        ie_codes: dict = {}
        by_sys: dict = {}
        unparsed = 0
        for r in range(n):
            m = _RECORD_RE.match(index.record_id(r))
            if not m:
                unparsed += 1
                continue
            sys_id, ie, p = m.group(1), m.group(2), int(m.group(3))
            code = ie_codes.get(ie)
            if code is None:
                code = ie_codes[ie] = len(ie_codes)
            rec_ie[r] = code
            rec_p[r] = p
            by_sys.setdefault(sys_id, []).append(r)
        sys_records = {s: np.asarray(rs, dtype=np.int64)
                       for s, rs in by_sys.items()}
        return cls(build=build_identity(index), n_records=n,
                   sys_records=sys_records, rec_ie=rec_ie, rec_p=rec_p,
                   ie_codes=ie_codes, unparsed=unparsed)

    def records_for_page(self, sys_id: str, ie: str, p: int) -> np.ndarray:
        recs = self.sys_records.get(sys_id)
        code = self.ie_codes.get(ie)
        if recs is None or code is None:
            return np.empty(0, dtype=np.int64)
        sel = (self.rec_ie[recs] == code) & (self.rec_p[recs] == int(p))
        return recs[sel]

    def mask_for(self, index: PassageIndex,
                 pages: Iterable[tuple] = (),
                 sys_ids: Iterable[str] = ()) -> np.ndarray:
        """A bool mask over `index`'s records: the given (sys_id, IE, P) pages
        plus every record of the given manuscripts. A page with no record (a
        page the index left out, or never had) contributes nothing.

        Raises StaleIndexError if `index` is not the build this map came from:
        record numbers mean nothing across builds."""
        if index.n_records != self.n_records or build_identity(index) != self.build:
            raise StaleIndexError(
                'page map built for a different letter-level index build')
        mask = np.zeros(self.n_records, dtype=bool)
        for sys_id in sys_ids:
            recs = self.sys_records.get(sys_id)
            if recs is not None:
                mask[recs] = True
        for sys_id, ie, p in pages:
            mask[self.records_for_page(sys_id, ie, p)] = True
        return mask


_CACHE: dict = {}
_CACHE_ORDER: list = []
_CACHE_LOCK = threading.Lock()
_CACHE_KEEP = 2


def page_index_for(index: PassageIndex) -> PageIndex:
    """The cached `PageIndex` for this index's build, built on first use
    (a scan of every record id: about a second or two on the full index)."""
    ident = build_identity(index)
    with _CACHE_LOCK:
        hit = _CACHE.get(ident)
        if hit is not None:
            return hit
    built = PageIndex.build_from(index)
    with _CACHE_LOCK:
        if ident not in _CACHE:
            _CACHE[ident] = built
            _CACHE_ORDER.append(ident)
            while len(_CACHE_ORDER) > _CACHE_KEEP:
                _CACHE.pop(_CACHE_ORDER.pop(0), None)
        return _CACHE[ident]


def clear_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()
        _CACHE_ORDER.clear()


def cached_build(index: PassageIndex) -> Optional[PageIndex]:
    """The cached map for this build, or None (never builds)."""
    with _CACHE_LOCK:
        return _CACHE.get(build_identity(index))
