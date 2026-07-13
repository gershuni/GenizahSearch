# -*- coding: utf-8 -*-
"""MAPV2-15c — the two-stage grader + the first honest measurement.

Design (Codex-gated): a NARROW rule tier fires only where the label is
deterministic from this-ms metadata (known / statutory-witness); everything
else — all discovery candidates, shared/citation calls, doubt — is RESIDUAL
and defers to the AI adjudication layer. Discoveries are never auto-accepted;
they stay reviewed claims.

Two entry points:
  --measure  score the full grader (rules where they fire, else the AI layer)
             against Hillel's 132 gold, per category, with discovery
             precision/recall. Reuses the already-produced critic grades as
             the AI layer, so this costs no new model calls. We do NOT tune
             the rules to this set (it is Hillel's gold, used to report only).
  --frame    run the rule tier over the frozen 467-row audit sample; report
             the rule-label distribution, the residual (AI-needed) size, and
             the post-stratified corpus-wide estimate.

Usage: python -X utf8 -u grader.py --measure
       python -X utf8 -u grader.py --frame
"""
import argparse
import json
import os
import re
import sqlite3
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from canon_rarity import SHARED_TH, CanonRarity
from normalize import norm_stream

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
FULL = PROBE + r"\review\full_deck"
DB = PROBE + r"\data\fullcorpus_v2.db"


def _canon_scores(pairs, cr):
    """{(page_id, work_id): rarity mass/len of the largest matched span}.

    Spans live in track1_candidates (the discovery pool), NOT track1_matches
    (confirmed/canonical identifications) — the earlier lookup queried the
    wrong table, so the canonical signal never reached the grader.
    """
    con = sqlite3.connect('file:' + DB.replace('\\', '/') + '?mode=ro',
                          uri=True)
    want = set(pairs)
    pages = {p for p, w in want}
    # matched spans (candidate pool first, then confirmed)
    spans = {}
    for tbl in ('track1_candidates', 'track1_matches'):
        plist = [p for p in pages]
        for i in range(0, len(plist), 400):
            batch = plist[i:i + 400]
            qm = ','.join('?' * len(batch))
            for pid, wid, sj in con.execute(
                    f"SELECT page_id, work_id, spans_json FROM {tbl} "
                    f"WHERE page_id IN ({qm})", batch):
                if (pid, wid) in want and (pid, wid) not in spans:
                    try:
                        spans[(pid, wid)] = json.loads(sj)
                    except Exception:
                        spans[(pid, wid)] = []
    # page text -> stream, slice the largest span, score
    ptext = {}
    plist = list(pages)
    for i in range(0, len(plist), 400):
        batch = plist[i:i + 400]
        qm = ','.join('?' * len(batch))
        for pid, tx in con.execute(
                f"SELECT page_id, text FROM pages WHERE page_id IN ({qm})",
                batch):
            ptext[pid] = tx or ''
    con.close()
    out = {}
    for (pid, wid), sp in spans.items():
        if not sp:
            out[(pid, wid)] = 0.0
            continue
        st = norm_stream(ptext.get(pid, ''))[0]
        # score over ALL matched spans (matched_letters is a sum over spans, so
        # scoring only the longest mis-scored multi-span matches). Merge
        # overlapping intervals, concatenate the slices, score the union.
        iv = sorted(((int(s[0]), int(s[1])) for s in sp), key=lambda x: x[0])
        merged = []
        for a, b in iv:
            if merged and a <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], b)
            else:
                merged.append([a, b])
        seg = ''.join(st[a:b] for a, b in merged)
        out[(pid, wid)] = cr.mass_per_len(seg)
    return out

# statutory/liturgical unit heads -> witness when in a liturgical container
WITNESS_HEADS = {'אמירה', 'ברכה', 'ברכת', 'תפילה', 'פתיחה', 'בקשה', 'הרחבה',
                 'וידוי', 'קדושת', 'קדושה', 'הבדלה', 'תחנון', 'סליחה',
                 'הושענא', 'תוספת'}
LITURGY_GENRES = {'פיוט ותפילה', 'Liturgy', 'liturgy'}


def _head(title):
    m = re.match(r'[א-ת]+', (title or '').strip())
    return m.group(0) if m else ''


def rule_grade(feat):
    """Narrow, high-precision vetoes only. Returns (grade, reason) or
    (None, None) = residual (defer to the AI layer). NEVER returns
    'discovery' — discoveries are never rule-decided."""
    tc = feat.get('title_class')
    bc = feat.get('bib_class')
    # this-ms metadata NAMES the work -> known. Only the catalog TITLE naming
    # the work is a safe veto; a bibliography MENTION (known_bib) is NOT —
    # the row may describe another folio, or the matched span may be a shared
    # classical source the work merely quotes (Codex "span semantics"). So a
    # bib mention defers to the AI layer instead of auto-marking known.
    if tc in ('same_work', 'name_variant'):
        return 'known', f'title:{tc}'
    # shared canonical source: the matched span's distinctive content is
    # Bible/Mishnah/Talmud that page and reference both quote -> shared, not a
    # discovery of the non-canonical work (MAPV2-15f rarity-weighted canon).
    if feat.get('canon_mass', 0.0) >= SHARED_TH:
        return 'shared', f"canon:{feat.get('canon_mass', 0):.2f}"
    # anonymous statutory unit in a liturgical container -> witness
    title = feat.get('title') or feat.get('work_name') or ''
    author = feat.get('author')
    genre = feat.get('genre') or feat.get('cat') or ''
    head = _head(title.split('—')[-1] if '—' in title else title)
    anon = (not author) or ('לא ידוע' in str(author)) or \
        str(feat.get('work_name', '')).startswith('מחבר לא ידוע')
    if head in WITNESS_HEADS and anon and \
            (genre in LITURGY_GENRES or feat.get('scope_regime') ==
             'homogeneous_anthology'):
        return 'witness', f'statutory:{head}'
    return None, None


def full_grade(feat, ai_grade):
    g, why = rule_grade(feat)
    if g:
        return g, f'rule:{why}'
    # Missing AI adjudication is a COVERAGE FAILURE, not a legitimate 'tsarich'
    # grade — surface it explicitly so it can't masquerade as a real label.
    if not ai_grade:
        return 'missing_ai', 'ai-missing'
    return ai_grade, 'ai'


# ------------------------------------------------------------------ measure
def measure():
    enr = {c['card_no']: c for c in json.load(open(
        os.path.join(FULL, 'mapv2_deck_cards_enriched.json'), encoding='utf-8'))}
    gold = {g['card_no']: g['grade'] for g in json.load(open(
        os.path.join(FULL, 'mapv2_v13_human_grades.json'), encoding='utf-8'))}
    critic = {c['card_no']: c['grade'] for c in json.load(open(
        os.path.join(FULL, 'mapv2_v13_critic_grades.json'), encoding='utf-8'))}

    GR = ['discovery', 'witness', 'citation', 'shared', 'known', 'formula',
          'norel', 'tsarich', 'missing_ai']
    cr = CanonRarity()
    cscore = _canon_scores([(c['page_id'], c['work_id']) for c in enr.values()
                            if c['card_no'] in gold], cr)
    # scope_regime for parity with frame() (the deployed rule tier uses it)
    from metadata_scope import ScopeGate
    sg = ScopeGate(n_pages={c['sys_id']: c.get('n_pages_this_ms', 0)
                            for c in enr.values()})
    rule_fired = 0
    rule_correct = 0
    full_correct = 0
    n_missing_ai = 0
    conf = defaultdict(lambda: defaultdict(int))   # gold -> full pred
    n = 0
    for cn, g in gold.items():
        c = enr.get(cn)
        if not c:
            continue
        n += 1
        feat = {'title_class': c.get('title_class'),
                'bib_class': c.get('bib_class'),
                'work_name': c.get('work_name'), 'title': c.get('work_name'),
                'author': None, 'genre': c.get('cat'),
                'canon_mass': cscore.get((c['page_id'], c['work_id']), 0.0),
                'scope_regime': sg.scope(c['sys_id'])['regime']}
        rg, _ = rule_grade(feat)
        if rg:
            rule_fired += 1
            if rg == g:
                rule_correct += 1
        fg, _ = full_grade(feat, critic.get(cn))
        if fg == 'missing_ai':
            n_missing_ai += 1
        if fg == g:
            full_correct += 1
        conf[g][fg] += 1

    print(f"n gold = {n}")
    print(f"\nRULE tier: fires on {rule_fired}/{n} "
          f"({100*rule_fired//n}%), correct {rule_correct}/{rule_fired} "
          f"({100*rule_correct//max(1,rule_fired)}%)")
    print(f"FULL grader (rules + AI residual) agrees with Hillel: "
          f"{full_correct}/{n} ({100*full_correct//n}%)")
    print(f"  (missing AI adjudication: {n_missing_ai}; NOTE: dev-set "
          f"agreement — the AI layer saw these cards and TH was tuned here, "
          f"so this is NOT held-out validation.)")

    print("\n=== confusion: Hillel (row) x full-grader (col) ===")
    _hdr = 'gold\\pred'
    print(f"{_hdr:12s}" + "".join(f"{x[:8]:>9s}" for x in GR))
    for g in GR:
        if sum(conf[g].values()) == 0:
            continue
        print(f"{g:12s}" + "".join(f"{conf[g][p]:>9d}" for p in GR))

    # the metric that matters: discovery precision/recall
    tp = conf['discovery']['discovery']
    pred_disc = sum(conf[g]['discovery'] for g in GR)
    gold_disc = sum(conf['discovery'][p] for p in GR)
    prec = tp / pred_disc if pred_disc else 0
    rec = tp / gold_disc if gold_disc else 0
    print(f"\nDISCOVERY  precision {tp}/{pred_disc} ({prec:.0%})  "
          f"recall {tp}/{gold_disc} ({rec:.0%})")
    # where discovery leaks: what the grader called Hillel's discoveries
    miss = {p: conf['discovery'][p] for p in GR if p != 'discovery'
            and conf['discovery'][p]}
    print(f"Hillel-discoveries the grader graded otherwise: {miss}")
    falsepos = {g: conf[g]['discovery'] for g in GR if g != 'discovery'
                and conf[g]['discovery']}
    print(f"grader-discoveries Hillel graded otherwise: {falsepos}")


# -------------------------------------------------------------------- frame
def frame():
    d = json.load(open(os.path.join(PROBE, 'data', 'audit_sample_v1.json'),
                       encoding='utf-8'))
    items = d['items']
    frame_cells = d['manifest']['frame_cells']
    frame_total = d['manifest']['frame_total']

    cr = CanonRarity()
    cscore = _canon_scores([(it['page_id'], it['work_id']) for it in items], cr)
    rule_lab = Counter()
    residual = 0
    # per-item weight = frame_cell_size / sampled_in_cell (post-stratification)
    samp_cell = Counter()
    for it in items:
        samp_cell[f"{it['genre_bucket']}|{it['letters_band']}|{it['stitch_status']}"] += 1
    wsum = defaultdict(float)   # grade/residual -> summed corpus weight
    for it in items:
        feat = {'title_class': it['title_class'], 'bib_class': it['bib_class'],
                'work_name': f"{it.get('author') or ''} — {it.get('title') or ''}",
                'title': it.get('title'), 'author': it.get('author'),
                'genre': it.get('genre'), 'scope_regime': it['scope_regime'],
                'canon_mass': cscore.get((it['page_id'], it['work_id']), 0.0)}
        g, _ = rule_grade(feat)
        key = f"{it['genre_bucket']}|{it['letters_band']}|{it['stitch_status']}"
        w = frame_cells.get(key, 0) / max(1, samp_cell[key])
        if g:
            rule_lab[g] += 1
            wsum[g] += w
        else:
            residual += 1
            # provisional bucket for the residual, by scope
            prov = 'discovery-candidate' if it['resolution'] == \
                'ms_scope_ambiguous' else 'ai-shared/known?'
            wsum[prov] += w

    print(f"audit sample: {len(items)} rows; raw frame {frame_total:,}")
    print(f"\nRULE tier fires on {sum(rule_lab.values())}/{len(items)} "
          f"({100*sum(rule_lab.values())//len(items)}%); "
          f"residual (AI needed) {residual}/{len(items)} "
          f"({100*residual//len(items)}%)")
    print("rule labels (sample count):", dict(rule_lab))

    tot = sum(wsum.values())
    print("\n=== estimated CORPUS-WIDE mix (post-stratified weights) ===")
    for k, v in sorted(wsum.items(), key=lambda x: -x[1]):
        print(f"  {k:22s} {v/tot:6.1%}")
    print("\n(residual = the AI layer must decide; discovery-candidate rows "
          "always route to human review before publication.)")


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--measure', action='store_true')
    ap.add_argument('--frame', action='store_true')
    a = ap.parse_args()
    if a.measure:
        measure()
    if a.frame:
        frame()
    if not (a.measure or a.frame):
        measure()
        print("\n" + "=" * 60 + "\n")
        frame()
