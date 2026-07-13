# -*- coding: utf-8 -*-
"""MAPV2-15d — shared-source (canonical-quotation) detector, per track1 match.

This is the detector Codex asked for; the MECHANISM already exists in
classify_canonical_edges.py (edge-level canonical overlap) and in the
canon-mask pipeline. Here it is packaged as a per-(page,work) match feature
the grader can consume: the canonical intervals of a page are the Track-1
Bible/Mishnah/Talmud identifications on that page; a NON-canonical match
whose span lies mostly inside those intervals is a shared canonical
quotation (both sides quote the source) — NOT a discovery of the
non-canonical work.

Codex distinctiveness guard: we score overlap on the matched SPAN itself, so
"mostly canonical" means the distinctive matched wording is canonical, not
merely that a verse appears somewhere on the page.
"""
import json
import sqlite3
from collections import defaultdict

CANON_CATS = ('Bible', 'Mishnah', 'Tosefta', 'Bavli', 'Yerushalmi')
T_CANON, T_CLEAN = 0.70, 0.30


def _merge(iv):
    iv.sort()
    out = []
    for a, b in iv:
        if out and a <= out[-1][1]:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return out


class CanonIndex:
    """Per-page canonical character intervals (from Track-1 canon matches)."""

    def __init__(self, db):
        con = sqlite3.connect(
            'file:' + db.replace('\\', '/') + '?mode=ro', uri=True)
        self.canon = defaultdict(list)
        cats = ",".join(f"'{c}'" for c in CANON_CATS)
        for pid, sj in con.execute(
                f"SELECT page_id, spans_json FROM track1_matches "
                f"WHERE cat IN ({cats})"):
            try:
                for s in json.loads(sj):
                    self.canon[pid].append((int(s[0]), int(s[1])))
            except Exception:
                pass
        for p in list(self.canon):
            self.canon[p] = _merge(self.canon[p])
        con.close()

    def overlap(self, page_id, spans):
        """Fraction of the matched span-union inside canonical intervals."""
        iv = self.canon.get(page_id)
        if not iv or not spans:
            return 0.0
        tot = ov = 0
        for s in spans:
            a, b = int(s[0]), int(s[1])
            tot += max(1, b - a)
            for q0, q1 in iv:
                ov += max(0, min(b, q1) - max(a, q0))
        return ov / max(1, tot)

    def classify_match(self, page_id, spans, cat):
        """-> ('canonical'|'mixed'|'clean', overlap_frac). A match whose work
        is ITSELF canonical is canonical by definition."""
        f = self.overlap(page_id, spans)
        if cat in CANON_CATS:
            return 'canonical', max(f, 1.0 if f == 0 else f)
        if f >= T_CANON:
            return 'canonical', f
        if f <= T_CLEAN:
            return 'clean', f
        return 'mixed', f
