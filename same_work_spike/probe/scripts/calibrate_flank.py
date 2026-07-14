# -*- coding: utf-8 -*-
"""MAPV2-15m stage 3 — leakage-free calibration + held-out validation.

Derives the flank target from the 8-class grades (no human flank label exists):
  continuation (must-NOT-demote) = {discovery, witness, known}
  island (desired citation demote) = {citation, shared, formula, norel}
  abstain                          = {tsarich}
Calibrates cont/island thresholds on the 127 human-graded DEV cards ONLY
(objective: max citation recall s.t. <=1 false demotion of a must-not-demote
card), reports the rescue of the 49 cards the OLD naive island label buried,
FREEZES thresholds, then runs the 100 held-out ONCE (post-stratified). Reuses
discovery_flank.flank_signals + decide so dev == production (no drift).

Out: data/flank_thresholds.json + results/flank_calibration.md
"""
import json
import os
import pickle
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from flank_align import gram_index
from discovery_flank import (REF, _ro, decide, flank_signals, load_competitors,
                             load_target_spans)
from normalize import norm_stream

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
FULL = PROBE + r"\review\full_deck"
CONT_GRADES = {'discovery', 'witness', 'known'}
ISLE_GRADES = {'citation', 'shared', 'formula', 'norel'}
CONT_GRID = [0.40, 0.42, 0.45, 0.48]
ISLAND_GRID = [0.56, 0.58, 0.62]


def target_of(grade):
    if grade in CONT_GRADES:
        return 'continuation'
    if grade in ISLE_GRADES:
        return 'island'
    return 'abstain'                       # tsarich (+ anything else)


def demoted(verdict):
    return verdict.startswith('likely_citation')


def load_cat_title(pages):
    con = _ro()
    out = {}
    pl = list(pages)
    for tbl in ('track1_matches', 'track1_candidates'):
        for i in range(0, len(pl), 400):
            b = pl[i:i + 400]
            qm = ','.join('?' * len(b))
            for pid, wid, cat, ti in con.execute(
                    f"SELECT page_id, work_id, cat, title FROM {tbl} "
                    f"WHERE page_id IN ({qm})", b):
                out.setdefault((pid, wid), (cat, ti or ''))
    con.close()
    return out


def build_sigs(cards, ref):
    pages = {c['page_id'] for c in cards}
    con = _ro()
    ptext = {}
    pl = list(pages)
    for i in range(0, len(pl), 400):
        b = pl[i:i + 400]
        qm = ','.join('?' * len(b))
        for pid, tx in con.execute(
                f"SELECT page_id, text FROM pages WHERE page_id IN ({qm})", b):
            ptext[pid] = norm_stream(tx or '')[0]
    con.close()
    tgt = load_target_spans(pages)
    comps = load_competitors(pages)
    cattitle = load_cat_title(pages)
    gpos_cache = {}
    out = []
    for c in cards:
        wid, pid = c['work_id'], c['page_id']
        ws = ref.get(wid)
        if wid not in gpos_cache:
            gpos_cache[wid] = gram_index(ws) if ws else None
        cat, ti = cattitle.get((pid, wid), ('', ''))
        r = {'work_id': wid, 'cat': cat, 'title': ti}
        sig = flank_signals(r, ptext.get(pid, ''), tgt.get((pid, wid), []),
                            ws, gpos_cache[wid], comps.get(pid, []))
        out.append((c, sig))
    return out


def evaluate(sigs, cont, island):
    fd = strong_fd = cit_recall_num = cit_total = dem_total = dem_island = 0
    abst = 0
    for c, sig in sigs:
        tgt = c['_target']
        v, _ = decide(sig, cont, island)
        dem = demoted(v)
        if v == 'abstain':
            abst += 1
        if tgt == 'continuation' and dem:
            fd += 1                          # any demotion of a must-not-demote
            if v == 'likely_citation_strong':
                strong_fd += 1               # Codex's binding constraint (<=1)
        if tgt == 'island':
            cit_total += 1
            if dem:
                cit_recall_num += 1
        if dem:
            dem_total += 1
            if tgt == 'island':
                dem_island += 1
    return {'false_demote': fd, 'strong_false_demote': strong_fd,
            'cit_recall': cit_recall_num / max(1, cit_total),
            'cit_precision': dem_island / max(1, dem_total),
            'dem_total': dem_total, 'abstain': abst, 'cit_total': cit_total}


def main():
    ref = {w['id']: w['stream'] for w in pickle.load(open(REF, 'rb'))}
    print(f"ref streams {len(ref)}", flush=True)

    # ---- DEV: 132 enriched cards + 127 grades ----
    enr = {c['card_no']: c for c in json.load(open(
        os.path.join(FULL, 'mapv2_deck_cards_enriched.json'), encoding='utf-8'))}
    grades = {g['card_no']: g['grade'] for g in json.load(open(
        os.path.join(FULL, 'mapv2_v13_human_grades.json'), encoding='utf-8'))
        if g.get('grade')}
    dev = []
    for cn, g in grades.items():
        c = enr.get(cn)
        if not c:
            continue
        dev.append({'card_no': cn, 'page_id': c['page_id'],
                    'work_id': c['work_id'], 'sys_id': c['sys_id'],
                    'grade': g, '_target': target_of(g),
                    'old_flank': c.get('flank_class')})
    print(f"dev cards {len(dev)} "
          f"(targets {dict(Counter(c['_target'] for c in dev))})", flush=True)
    dev_sigs = build_sigs(dev, ref)
    for c, sig in dev_sigs:
        c['_sig'] = sig

    # grid search: max citation recall s.t. false_demote <= 1
    best = None                            # (cit_recall, -fd, cit_prec)
    grid_rows = []
    for cont in CONT_GRID:
        for island in ISLAND_GRID:
            if cont >= island:
                continue
            m = evaluate(dev_sigs, cont, island)
            grid_rows.append((cont, island, m))
            if m['strong_false_demote'] <= 1:   # Codex: <=1 false STRONG-demotion
                key = (m['cit_recall'], -m['strong_false_demote'], m['cit_precision'])
                if best is None or key > best[0]:
                    best = (key, cont, island, m)
    if best is None:                       # none met <=1 strong: min strong-fd
        cont_f, island_f, mdev = min(
            grid_rows, key=lambda x: (x[2]['strong_false_demote'], -x[2]['cit_recall']))
    else:
        _, cont_f, island_f, mdev = best
    print(f"\nFROZEN thresholds: cont={cont_f} island={island_f}  dev {mdev}")

    # rescue-of-49: old flank_class=='island' AND continuation-target
    buried = [c for c, _ in dev_sigs
              if c['old_flank'] == 'island' and c['_target'] == 'continuation']
    rescued = sum(1 for c in buried
                  if not demoted(decide(c['_sig'], cont_f, island_f)[0]))
    print(f"rescue-of-buried: {rescued}/{len(buried)} old-island "
          f"must-not-demote cards NOT demoted by the new detector")

    json.dump({'cont_thr': cont_f, 'island_thr': island_f,
               'dev': mdev, 'rescued': rescued, 'buried': len(buried)},
              open(PROBE + r'\data\flank_thresholds.json', 'w'), indent=1)

    # ---- HELD-OUT 100 (run once, post-stratified) ----
    man = json.load(open(PROBE + r'\data\validation_100_manifest.json',
                         encoding='utf-8'))
    cards100 = {c['no']: c for c in man['cards']}
    fcells = man['meta']['frame_cells']
    scount = man['meta']['sample_cell_counts']
    hg = {h['no']: h['grade'] for h in json.load(open(
        os.path.join(FULL, 'mapv2_validation_100_human.json'), encoding='utf-8'))
        if h.get('grade')}
    ho = []
    for no, g in hg.items():
        c = cards100.get(no)
        if not c:
            continue
        ho.append({'no': no, 'page_id': c['page_id'], 'work_id': c['work_id'],
                   'sys_id': c['sys_id'], 'grade': g, '_target': target_of(g),
                   '_cell': c['_cell']})
    ho_sigs = build_sigs(ho, ref)
    # unweighted + weighted metrics at frozen thresholds
    m_ho = evaluate([(c, s) for c, s in ho_sigs], cont_f, island_f)
    wsum = defaultdict(float)
    for c, sig in ho_sigs:
        w = fcells.get(c['_cell'], 0) / max(1, scount.get(c['_cell'], 1))
        v, _ = decide(sig, cont_f, island_f)
        key = ('demote' if demoted(v) else 'abstain' if v == 'abstain' else 'keep')
        wsum[f"{c['_target']}|{key}"] += w
    w_fd = sum(v for k, v in wsum.items() if k.startswith('continuation|demote'))
    w_cont = sum(v for k, v in wsum.items() if k.startswith('continuation|'))

    L = ["# Flank detector calibration + held-out validation (MAPV2-15m)", "",
         f"## DEV (127 graded, targets cont/island/abstain)",
         f"- FROZEN: cont_thr={cont_f}, island_thr={island_f}",
         f"- **false STRONG-demote (Codex constraint <=1): "
         f"{mdev['strong_false_demote']}**; total false-demote (incl. mild "
         f"x0.75 weak) {mdev['false_demote']}",
         f"- citation recall: {mdev['cit_recall']:.0%} of {mdev['cit_total']} "
         f"island-target cards; citation precision {mdev['cit_precision']:.0%}",
         f"- **rescue of the naive-island-buried must-not-demote cards: "
         f"{rescued}/{len(buried)}**", "",
         "### grid search (cont, island -> false_demote, cit_recall, cit_prec)", ""]
    for cont, island, m in grid_rows:
        mk = ' *' if (cont == cont_f and island == island_f) else ''
        L.append(f"- ({cont},{island}): fd={m['false_demote']} "
                 f"recall={m['cit_recall']:.0%} prec={m['cit_precision']:.0%}{mk}")
    L += ["", "## HELD-OUT 100 (run ONCE at frozen thresholds)",
          f"- unweighted: false-demote {m_ho['false_demote']}, citation recall "
          f"{m_ho['cit_recall']:.0%}/{m_ho['cit_total']}, precision "
          f"{m_ho['cit_precision']:.0%}, abstain {m_ho['abstain']}/{len(ho)}",
          f"- **post-stratified same-work false-demotion rate: "
          f"{100*w_fd/max(1e-9,w_cont):.1f}%** of must-not-demote mass",
          "", "(advisory multipliers only; every survivor is human-reviewed.)"]
    open(PROBE + r'\results\flank_calibration.md', 'w', encoding='utf-8').write(
        '\n'.join(L) + '\n')
    print('\n'.join(L))


if __name__ == '__main__':
    main()
