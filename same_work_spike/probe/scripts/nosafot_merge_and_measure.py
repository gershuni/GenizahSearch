# -*- coding: utf-8 -*-
"""Merge the Maagarim מסירות נוספות harvest with the used-mesirah table and
measure the INCREMENTAL discovery-queue demotions the nosafot channel adds.

SEED-029 Track-1. Consumes:
  * ..\\data\\mesirah_witnesses.json  — USED source manuscripts, parsed from the
        local Maagarim ##המסירה:## headers (channel = used_mesirah).
  * ..\\data\\mesirot_nosafot.json    — the website harvest
        (maagarim_nosafot_harvest.py): per work, `matched` = additional-witness
        (מסירות נוספות) sys_id matches [channel = nosafot], and `msirot_matched`
        = the website's PRIMARY manuscript-witness list [channel = msirot_web,
        reported as a bonus — it is the API-side view of the used manuscripts
        and can be more complete than the local-header parse].
  * ..\\results\\track1_full_testimonies.csv — the discovery queue.

Produces:
  * ..\\data\\known_witnesses_all.json — unified rows
        {work_id, sys_id, channel in {used_mesirah, nosafot, msirot_web},
         library_code, confidence}  (best confidence per work_id+sys_id+channel).
  * ..\\results\\nosafot_harvest.md  — the harvest + incremental-demotion report.

The headline measurement, over the committed new?-queue
(tier=='new?' & cls=='testimony' & cat not canonical), at HIGH confidence:
  (a) used_mesirah alone   (b) nosafot alone   (c) union
  incremental = (c) - (a) = rows nosafot demotes that used-mesirah missed.

Run:  cd C:\\Genizahsearch\\same_work_spike\\probe\\scripts
      python -X utf8 -u nosafot_merge_and_measure.py
"""
import csv
import json
import os
import time
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
MES = os.path.join(HERE, '..', 'data', 'mesirah_witnesses.json')
NOS = os.path.join(HERE, '..', 'data', 'mesirot_nosafot.json')
TESTIMONIES = os.path.join(HERE, '..', 'results', 'track1_full_testimonies.csv')
OUT_JSON = os.path.join(HERE, '..', 'data', 'known_witnesses_all.json')
OUT_MD = os.path.join(HERE, '..', 'results', 'nosafot_harvest.md')

CANON = ('Bible', 'Bavli', 'Mishnah', 'Yerushalmi', 'Tosefta')
RANK = {'high': 0, 'low': 1, 'ambiguous': 2}


def better(a, b):
    """best (lowest-rank) confidence of two (either may be None)."""
    if a is None:
        return b
    if b is None:
        return a
    return a if RANK[a] <= RANK[b] else b


def main():
    t0 = time.time()

    # ---------------------------------------------------------- used_mesirah
    used_rows = json.load(open(MES, encoding='utf-8'))
    # (work_id, sys_id) -> [best_conf, lib]
    used = {}
    for r in used_rows:
        if r.get('sys_id') and r.get('confidence'):
            k = (r['work_id'], r['sys_id'])
            cur = used.get(k)
            conf = better(cur[0] if cur else None, r['confidence'])
            lib = (cur[1] if cur else None) or r.get('library_code')
            used[k] = [conf, lib]

    # ---------------------------------------------------------- nosafot + msirot_web
    nos_works = json.load(open(NOS, encoding='utf-8'))
    nosafot = {}       # (work_id, sys_id) -> [conf, lib]
    msirotw = {}       # (work_id, sys_id) -> [conf, lib]
    n_ok = n_fail = 0
    n_nos_ms_tab = 0
    tot_nos_strings = tot_ms_strings = 0
    example_pool = []          # (work_id, title, raw string, matched?)
    for w in nos_works:
        if not w.get('ok'):
            n_fail += 1
            continue
        n_ok += 1
        wid = w['work_id']
        if w.get('nosafot_header') == 'כתבי יד':
            n_nos_ms_tab += 1
        tot_nos_strings += len(w.get('nosafot', []))
        tot_ms_strings += len(w.get('msirot_web', []))
        for m in w.get('matched', []):
            k = (wid, m['sys_id'])
            cur = nosafot.get(k)
            nosafot[k] = [better(cur[0] if cur else None, m['confidence']),
                          (cur[1] if cur else None) or m.get('library_code')]
        for m in w.get('msirot_matched', []):
            k = (wid, m['sys_id'])
            cur = msirotw.get(k)
            msirotw[k] = [better(cur[0] if cur else None, m['confidence']),
                          (cur[1] if cur else None) or m.get('library_code')]
        # collect example nosafot rows (from works whose tab is manuscripts)
        if w.get('nosafot_header') == 'כתבי יד':
            for s in w.get('nosafot', [])[:3]:
                example_pool.append((wid, w.get('title', ''), s))

    # ---------------------------------------------------------- unified table
    unified = []
    for (wid, sid), (conf, lib) in sorted(used.items()):
        unified.append({'work_id': wid, 'sys_id': sid,
                        'channel': 'used_mesirah', 'library_code': lib,
                         'confidence': conf})
    for (wid, sid), (conf, lib) in sorted(nosafot.items()):
        unified.append({'work_id': wid, 'sys_id': sid, 'channel': 'nosafot',
                        'library_code': lib, 'confidence': conf})
    for (wid, sid), (conf, lib) in sorted(msirotw.items()):
        unified.append({'work_id': wid, 'sys_id': sid, 'channel': 'msirot_web',
                        'library_code': lib, 'confidence': conf})
    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(unified, f, ensure_ascii=False, indent=1)
    print(f'wrote {os.path.abspath(OUT_JSON)} ({len(unified):,} rows)')

    # ---------------------------------------------------------- measurement
    with open(TESTIMONIES, encoding='utf-8-sig', newline='') as f:
        trows = list(csv.DictReader(f))
    queue = [r for r in trows if r['tier'] == 'new?' and r['cls'] == 'testimony'
             and r['cat'] not in CANON]

    def conf_in(channel_map, wid, sid):
        v = channel_map.get((wid, sid))
        return v[0] if v else None

    # per-queue-row demotion confidence by channel and union
    def measure(channel_maps):
        """channel_maps = list of dicts; returns Counter of best conf across them."""
        b = Counter()
        hits = []
        for r in queue:
            k = (r['work_id'], r['sys_id'])
            best_c = None
            for cm in channel_maps:
                best_c = better(best_c, conf_in(cm, *k))
            b[best_c if best_c else 'none'] += 1
            if best_c:
                hits.append((r, best_c))
        return b, hits

    a_b, a_hits = measure([used])
    b_b, b_hits = measure([nosafot])
    c_b, c_hits = measure([used, nosafot])
    d_b, _ = measure([used, nosafot, msirotw])       # bonus: + website primary

    # incremental: queue rows demoted (high) by nosafot but NOT by used_mesirah
    a_high = {(r['work_id'], r['sys_id']) for r, c in a_hits if c == 'high'}
    c_high = [(r, c) for r, c in c_hits if c == 'high']
    incr_rows = [(r, c) for r, c in c_high
                 if (r['work_id'], r['sys_id']) not in a_high]
    d_high_pairs = {(r['work_id'], r['sys_id'])
                    for r, c in measure([used, nosafot, msirotw])[1] if c == 'high'}
    d_incr = len(d_high_pairs - a_high)

    print(f'queue rows: {len(queue):,}')
    print(f'(a) used_mesirah alone : {dict(a_b)}')
    print(f'(b) nosafot alone      : {dict(b_b)}')
    print(f'(c) union              : {dict(c_b)}')
    print(f'(d) + msirot_web bonus : {dict(d_b)}')
    print(f'INCREMENTAL high-conf demotions from nosafot beyond used-mesirah: '
          f'{len(incr_rows)}')
    print(f'  (+ msirot_web too: {d_incr} beyond used-mesirah)')

    # ---------------------------------------------------------- report
    md = []
    md.append('# Maagarim מסירות נוספות harvest — 4th novelty channel (SEED-029)')
    md.append('')
    md.append(f'Generated by `scripts/nosafot_merge_and_measure.py` on '
              f'{time.strftime("%Y-%m-%d %H:%M")}.')
    md.append('')
    md.append('## The working API (task-0 resolved)')
    md.append('')
    md.append('The brief anticipated finishing the `GetYzira` request body. '
              'Tracing the site `mainJs` showed `GetYzira` returns the '
              'reading-view HTML of the work, **not** the witness lists. The '
              'witness lists come from a different endpoint — '
              '`ShowEssayDetails(n,t)` builds `{misyzira:n, tabNum:t}` and POSTs '
              'it to `Mqorot.asmx/GetPirteiHibur`, whose JSON response carries '
              'the `msirot` and `nosafot` HTML fragments. So no `GetYzira` body '
              'and no page-scraping fallback were needed; the clean JSON path is:')
    md.append('')
    md.append('```')
    md.append('POST https://maagarim.hebrew-academy.org.il/Pages/ws/'
              'Mqorot.asmx/GetPirteiHibur')
    md.append('Content-Type: application/json; charset=utf-8')
    md.append('User-Agent: <browser UA>        # Cloudflare 403s without one')
    md.append('body = {"misyzira": <N>, "tabNum": 0}')
    md.append('```')
    md.append('')
    md.append('Response fields used:')
    md.append('')
    md.append('- `msirot` — manuscript witnesses **used** for the edition '
              '(tab "כתבי יד"); a superset of `GetYziraFull.mesirot` / the local '
              '`##המסירה##` headers.')
    md.append('- `nosafot` — element `liMsirotNosafot` (מסירות נוספות). Content '
              'header is one of: `כתבי יד` (**additional manuscript witnesses** — '
              'the target channel), `פרסומים בדפוס` (printed publications — no '
              'shelfmark), or `אין מסירות נוספות` (none).')
    md.append('')
    md.append('Each witness row exposes the clean location string in '
              "`<a class='openClose'>…</a>` (== the `zihuy` of its "
              '`doFeedbackForMesira` handler); those are matched with the proven '
              '`mesirah_witnesses.py` parser/index (imported, not reimplemented).')
    md.append('')
    md.append('## Harvest stats')
    md.append('')
    md.append(f'- Works in scope (tier `new?`/`new?known`, `M:Ytext` ids): '
              f'**{n_ok + n_fail:,}** attempted')
    md.append(f'- Succeeded: **{n_ok:,}**  ·  failed/skipped: **{n_fail:,}**')
    md.append(f'- Works whose `nosafot` tab = additional **manuscripts** '
              f'(`כתבי יד`): **{n_nos_ms_tab:,}**')
    md.append(f'- Total `nosafot` raw witness strings: **{tot_nos_strings:,}** '
              f'(+ {tot_ms_strings:,} `msirot` primary strings)')
    md.append(f'- `nosafot` -> Genizah sys_id matches (distinct work+sys pairs): '
              f'**{len(nosafot):,}** '
              f'(high {sum(1 for v in nosafot.values() if v[0]=="high"):,}, '
              f'low {sum(1 for v in nosafot.values() if v[0]=="low"):,}, '
              f'ambiguous {sum(1 for v in nosafot.values() if v[0]=="ambiguous"):,})')
    md.append(f'- `msirot_web` -> matches (bonus): **{len(msirotw):,}** '
              f'(high {sum(1 for v in msirotw.values() if v[0]=="high"):,})')
    md.append(f'- Unified `known_witnesses_all.json`: **{len(unified):,}** rows '
              f'(used_mesirah {len(used):,}, nosafot {len(nosafot):,}, '
              f'msirot_web {len(msirotw):,})')
    md.append('')
    md.append('## Discovery-queue demotion (the key measurement)')
    md.append('')
    md.append(f'Committed new?-queue = `track1_full_testimonies.csv` rows with '
              f"tier==`new?` & cls==`testimony` & cat non-canonical: "
              f'**{len(queue):,}** rows. Demotion counts at each confidence:')
    md.append('')
    md.append('| channel | high (auto-demote) | low | ambiguous | none |')
    md.append('|---|---|---|---|---|')
    for label, b in [('(a) used_mesirah alone', a_b),
                     ('(b) nosafot alone', b_b),
                     ('(c) union (a+b)', c_b),
                     ('(d) + msirot_web (bonus)', d_b)]:
        md.append(f"| {label} | {b['high']} | {b['low']} | "
                  f"{b['ambiguous']} | {b['none']} |")
    md.append('')
    md.append(f'**INCREMENTAL high-confidence demotions the `nosafot` channel '
              f'adds beyond used-mesirah: {len(incr_rows)}** '
              f'(used-mesirah alone = {a_b["high"]}; union = {c_b["high"]}).')
    md.append('')
    md.append(f'Bonus: adding the website `msirot_web` primary list lifts the '
              f'union to **{d_b["high"]}** high-confidence demotions '
              f'({d_incr} beyond used-mesirah).')
    md.append('')
    if incr_rows:
        md.append('### new?-queue rows demoted by nosafot but MISSED by used-mesirah')
        md.append('')
        md.append('These are Genizah fragments our queue flagged as `new?` '
                  'witnesses of a work, that the Academy already lists as an '
                  '*additional* known witness (מסירה נוספת):')
        md.append('')
        md.append('| work_id | sys_id | shelfmark | lib | conf |')
        md.append('|---|---|---|---|---|')
        for r, c in incr_rows[:60]:
            md.append(f"| {r['work_id']} | {r['sys_id']} | "
                      f"{r.get('shelfmark','')} | {r.get('lib','')} | {c} |")
        if len(incr_rows) > 60:
            md.append(f'| … | ({len(incr_rows)-60} more) | | | |')
        md.append('')
    else:
        md.append('### new?-queue rows demoted by nosafot but MISSED by used-mesirah')
        md.append('')
        md.append('_None: every high-confidence nosafot demotion was already '
                  'covered by the used-mesirah channel._')
        md.append('')

    md.append('## Example nosafot witness rows (from additional-manuscript tabs)')
    md.append('')
    seen_w = set()
    shown = 0
    for wid, title, s in example_pool:
        if wid in seen_w:
            continue
        seen_w.add(wid)
        md.append(f'- `{wid}` — {title[:40]} — `{s[:90]}`')
        shown += 1
        if shown >= 10:
            break
    md.append('')

    with open(OUT_MD, 'w', encoding='utf-8') as f:
        f.write('\n'.join(md))
    print(f'wrote {os.path.abspath(OUT_MD)}')
    print(f'done in {time.time()-t0:.1f}s')


if __name__ == '__main__':
    main()
