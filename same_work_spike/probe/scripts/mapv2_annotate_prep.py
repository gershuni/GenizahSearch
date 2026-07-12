# -*- coding: utf-8 -*-
"""MAPV2-A prep — enrich the deck card dump (mapv2_deck_cards.json) with the
catalog identifications an annotator needs to judge "is this actually already
known?":

  - libraries.csv NLI title (already on the card, kept)
  - fjms_enrichment.db catalog units: GenizahTitleOrgTitle/Eng (THE work
    identification), AuthorText, SourceNameHeb (which catalog said so),
    TextualFrameHeb (genre frame) — distinct, per sys_id

Writes review/full_deck/mapv2_deck_cards_enriched.json + N chunk files
review/full_deck/annot_chunk_{i}.json for the annotation agents.

Usage: python -X utf8 -u mapv2_annotate_prep.py [--chunks 4]
"""
import argparse
import json
import os
import sqlite3

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
FJMS = r"C:\Genizahsearch\fist_data\fjms_enrichment.db"
CARDS = os.path.join(PROBE, 'review', 'full_deck', 'mapv2_deck_cards.json')
OUT = os.path.join(PROBE, 'review', 'full_deck',
                   'mapv2_deck_cards_enriched.json')


def fjms_idents(con, sid):
    rows = con.execute(
        """SELECT DISTINCT GenizahTitleOrgTitle, GenizahTitleEngTitle,
                  AuthorText, SourceNameHeb, TextualFrameHeb, Title, TitleHeb
           FROM catalog WHERE AlmaId=?""", (sid,)).fetchall()
    out = []
    seen = set()
    for org, eng, auth, src, frame, t, th in rows:
        ident = {}
        if org:
            ident['work'] = org
        if eng:
            ident['work_en'] = eng
        if auth:
            ident['author'] = auth
        if frame:
            ident['genre_frame'] = frame
        if th or t:
            ident['unit_title'] = th or t
        if not ident:
            continue
        ident['identified_by'] = src or '?'
        key = json.dumps(ident, ensure_ascii=False, sort_keys=True)
        if key not in seen:
            seen.add(key)
            out.append(ident)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--chunks', type=int, default=4)
    args = ap.parse_args()

    cards = json.load(open(CARDS, encoding='utf-8'))
    con = sqlite3.connect(FJMS)
    # MAPV2-11: Friedberg bibliography rows per manuscript — the annotators'
    # biggest blind spot in the v11 cycle (9/17 human-killed "discoveries"
    # were known there)
    from bib_gate import BibGate
    bg = BibGate()
    n_with = 0
    for c in cards:
        idents = fjms_idents(con, c['sys_id'])
        c['fjms_catalog_identifications'] = idents
        c['friedberg_bibliography'] = bg.display(c['sys_id'], k=8)
        if idents:
            n_with += 1
    con.close()
    json.dump(cards, open(OUT, 'w', encoding='utf-8'),
              ensure_ascii=False, indent=1)
    print(f"wrote {OUT}: {len(cards)} cards, "
          f"{n_with} with FJMS identifications")

    n = args.chunks
    per = (len(cards) + n - 1) // n
    # agents must classify blind to the deck's own routing (v11): strip the
    # section/title-gate fields from the chunk files (kept in enriched)
    BLIND_DROP = ('section', 'title_class', 'title_evidence')
    for i in range(n):
        chunk = [{k: v for k, v in c.items() if k not in BLIND_DROP}
                 for c in cards[i * per:(i + 1) * per]]
        p = os.path.join(PROBE, 'review', 'full_deck',
                         f'annot_chunk_{i + 1}.json')
        json.dump(chunk, open(p, 'w', encoding='utf-8'),
                  ensure_ascii=False, indent=1)
        print(f"  chunk {i + 1}: {len(chunk)} cards "
              f"(#{chunk[0]['card_no']}–#{chunk[-1]['card_no']}) -> {p}")


if __name__ == '__main__':
    main()
