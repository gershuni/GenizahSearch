# -*- coding: utf-8 -*-
"""MAPV2-15a — metadata scope detector ("one book vs. mixed bag").

Codex final-gate condition (c)+(d): replace the binary ms-regime with an
INDEPENDENT `metadata_scope_confidence` derived from catalog metadata ONLY
(NLI title + FJMS GenizahTitleOrgTitle identifications + physical size), and
a per-page tri-state resolution flag that decides how much that ms-level
metadata is allowed to say about ONE page. Same-work matches are NOT an
input to the regime (they would be circular); `n_matched_works` is accepted
only as a weak, leave-target-out tie-breaker.

Regime (per sys_id, catalog-only):
  single_work           one specific work identification, no collection
                        marker, not multi-genre -> ms metadata strongly
                        predicts every page.
  homogeneous_anthology no specific work but a single coherent genre (a
                        "פיוטים"/"קרובות" codex) OR >=2 specific works of one
                        genre -> predicts the GENRE of a page, not the unit.
  miscellany            collection marker ("קובץ"/"שונים"/miscellaneous), or
                        >=3 distinct specific works, or many identifications
                        across genres -> ms metadata barely predicts a page.
  ambiguous             nothing to go on (generic, single genre-less title).

Per-page resolution (regime + does the catalog name THIS claim):
  page_resolved_known   catalog/bib names this claim's work for this ms
                        -> may veto to `known`.
  global_ms_likely      single_work / strong homogeneous -> discovery
                        unlikely on any page; still audited.
  ms_scope_ambiguous    miscellany / generic -> weak only; cannot demote;
                        this is where discoveries live.

CLI: python -X utf8 -u metadata_scope.py   (cross-tabs the regime against
Hillel's 132 gold grades via the enriched cards, as a sanity check).
"""
import csv
import os
import re
import sqlite3
from collections import defaultdict

from title_gate import (GENRE_OF, TitleGate, WEAK_TOKENS, is_generic_title,
                        norm_text, tokens)

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
ROOT = r"C:\Genizahsearch"
FJMS_DB = r"C:\Genizahsearch\fist_data\fjms_enrichment.db"
LIB_CSV = ROOT + r"\libraries.csv"

# explicit "mixed bag" markers (a title that announces itself a collection)
COLLECTION_MARKERS = {'קובץ', 'קבץ', 'אוסף', 'לקט', 'ליקוטים', 'ליקוט',
                      'שונים', 'שונות', 'מגוון', 'מעורב', 'מעורבים',
                      'miscellaneous', 'miscellany', 'collection', 'various',
                      'compilation', 'anthology'}


def _load_nli_titles():
    """sys_id -> NLI title (libraries.csv column 7)."""
    out = {}
    with open(LIB_CSV, encoding='utf-8-sig', newline='') as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) >= 8 and row[0]:
                t = (row[7] or '').strip()
                if t:
                    out[row[0]] = t
    return out


def _sig(title):
    """A work signature = first two significant (non-weak, len>=3) tokens in
    TITLE ORDER, for counting DISTINCT works without splitting name-variants
    apart. Order-preserving (norm_text keeps sequence; tokens() is a set)."""
    sig_tokens = [t for t in norm_text(title).split()
                  if t not in WEAK_TOKENS and len(t) >= 3]
    return ' '.join(sorted(sig_tokens[:2])) if sig_tokens else None


def _genres_of(title):
    return {GENRE_OF[t] for t in tokens(title) if t in GENRE_OF}


class ScopeGate:
    """Per-ms metadata scope, catalog-only. Reuses TitleGate for the actual
    catalog identifications (NLI + FJMS) so classification stays consistent
    with the deck's title gate."""

    def __init__(self, nli_titles=None, fjms_db=FJMS_DB, n_pages=None,
                 n_matched_works=None):
        self.nli = nli_titles if nli_titles is not None else _load_nli_titles()
        self.tg = TitleGate(self.nli, fjms_db=fjms_db)
        self.n_pages = n_pages or {}            # sys_id -> page count (weak)
        self.n_matched = n_matched_works or {}  # sys_id -> #works (weak only)
        self._cache = {}
        # physical folio count per AlmaId (weak size signal)
        self.n_folio = {}
        if fjms_db and os.path.exists(fjms_db):
            con = sqlite3.connect(
                'file:' + fjms_db.replace('\\', '/') + '?mode=ro', uri=True)
            for sid, nf in con.execute(
                    "SELECT AlmaId, MAX(CAST(NumFolio AS INTEGER)) FROM catalog"
                    " WHERE NumFolio IS NOT NULL GROUP BY AlmaId"):
                if nf:
                    self.n_folio[str(sid)] = nf
            con.close()

    def scope(self, sid):
        sid = str(sid)
        if sid in self._cache:
            return self._cache[sid]
        titles = self.tg.titles_of(sid)
        specific = [t for t in titles if not is_generic_title(t)]
        # distinct specific works (collapse name-variants by signature)
        sigs = {s for s in (_sig(t) for t in specific) if s}
        n_specific = len(sigs)
        # genres present across ALL identifications (specific + generic genre)
        genres = set()
        for t in titles:
            genres |= _genres_of(t)
        collection = any(tok in COLLECTION_MARKERS
                         for t in titles for tok in tokens(t))
        n_ids = len(titles)

        # --- regime decision (catalog-only) ---
        if collection or n_specific >= 3 or (n_ids >= 4 and n_specific >= 2) \
                or len(genres) >= 3:
            regime = 'miscellany'
            conf = 0.9 if collection or n_specific >= 3 else 0.7
        elif n_specific == 1 and n_ids <= 2 and len(genres) <= 1:
            regime = 'single_work'
            conf = 0.85
            if self.n_folio.get(sid, 0) and self.n_folio[sid] <= 4:
                conf = 0.9              # a tiny fragment of one work
            if self.n_pages.get(sid, 0) > 20:
                conf = 0.65             # large -> could still be composite
        elif n_specific == 2 and len(genres) <= 1:
            regime = 'homogeneous_anthology'
            conf = 0.65
        elif n_specific == 0 and len(genres) == 1:
            regime = 'homogeneous_anthology'   # a one-genre codex, no work id
            conf = 0.55
        elif n_specific == 0:
            regime = 'ambiguous'
            conf = 0.4
        else:
            regime = 'miscellany'
            conf = 0.55
        # weak, leave-target-out tie-break: many DISTINCT matched works on an
        # otherwise-ambiguous ms nudges toward miscellany (never toward
        # single_work; never overrides a catalog signal).
        nm = self.n_matched.get(sid, 0)
        if regime in ('ambiguous', 'homogeneous_anthology') and nm >= 5:
            regime, conf = 'miscellany', max(conf, 0.5)

        out = {'sys_id': sid, 'regime': regime, 'confidence': round(conf, 2),
               'n_specific_works': n_specific, 'n_catalog_ids': n_ids,
               'genres': sorted(genres), 'collection_marker': collection,
               'n_folio': self.n_folio.get(sid, 0),
               'n_pages': self.n_pages.get(sid, 0),
               'n_matched_works': nm}
        self._cache[sid] = out
        return out

    def resolution(self, sid, claim_name):
        """Tri-state: how much ms metadata may say about THIS page/claim."""
        sc = self.scope(sid)
        tcls, _ = self.tg.classify(sid, claim_name)
        if tcls in ('same_work', 'name_variant'):
            return 'page_resolved_known'
        if sc['regime'] == 'single_work' and sc['confidence'] >= 0.8:
            return 'global_ms_likely'
        if sc['regime'] == 'homogeneous_anthology' and tcls == 'different_specific':
            return 'global_ms_likely'
        return 'ms_scope_ambiguous'


def _validate():
    """Cross-tab regime + resolution against Hillel's 132 gold grades."""
    import json
    enr = {c['card_no']: c for c in json.load(open(os.path.join(
        PROBE, 'review', 'full_deck', 'mapv2_deck_cards_enriched.json'),
        encoding='utf-8'))}
    gold = json.load(open(os.path.join(
        PROBE, 'review', 'full_deck', 'mapv2_v13_human_grades.json'),
        encoding='utf-8'))
    # n_pages per sys_id from the enriched cards (n_pages_this_ms)
    n_pages = {c['sys_id']: c.get('n_pages_this_ms', 0) for c in enr.values()}
    sg = ScopeGate(n_pages=n_pages)
    reg = defaultdict(lambda: defaultdict(int))
    res = defaultdict(lambda: defaultdict(int))
    for g in gold:
        c = enr.get(g['card_no'])
        if not c:
            continue
        sc = sg.scope(c['sys_id'])
        rr = sg.resolution(c['sys_id'], c['work_name'])
        reg[sc['regime']][g['grade']] += 1
        res[rr][g['grade']] += 1
    grades = ['discovery', 'witness', 'citation', 'shared', 'known', 'tsarich']
    print("=== regime (rows) x Hillel grade (cols) ===")
    print(f"{'regime':22s}" + "".join(f"{x[:9]:>10s}" for x in grades))
    for r in ('single_work', 'homogeneous_anthology', 'miscellany',
              'ambiguous'):
        print(f"{r:22s}" + "".join(f"{reg[r][gd]:>10d}" for gd in grades))
    print("\n=== per-page resolution (rows) x Hillel grade (cols) ===")
    print(f"{'resolution':22s}" + "".join(f"{x[:9]:>10s}" for x in grades))
    for r in ('page_resolved_known', 'global_ms_likely', 'ms_scope_ambiguous'):
        print(f"{r:22s}" + "".join(f"{res[r][gd]:>10d}" for gd in grades))
    # the key check: where do discoveries live?
    disc_amb = res['ms_scope_ambiguous']['discovery']
    disc_all = sum(res[r]['discovery'] for r in res)
    known_res = res['page_resolved_known']['known']
    known_all = sum(res[r]['known'] for r in res)
    print(f"\ndiscoveries in ms_scope_ambiguous: {disc_amb}/{disc_all}")
    print(f"knowns in page_resolved_known:     {known_res}/{known_all}")


if __name__ == '__main__':
    _validate()
