# -*- coding: utf-8 -*-
"""Render the multi-manuscript Phase 136 panel mockup with every agreed rule applied."""
import html, json, os, re, unicodedata
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
d = json.load(open(os.path.join(HERE, 'multi_data.json'), encoding='utf-8'))
OUT = os.path.join(HERE, 'phase136-mockup-multi.html')

libs, texts, wc = d['libraries'], d['page_texts'], d['work_counts']
SHORT_LETTERS = 150          # D-13c threshold (provisional, set at gate 1)
HEB = re.compile(r'[֐-׿]')


def letters(s):
    return sum(1 for ch in s if HEB.match(ch) and unicodedata.category(ch) != 'Mn')


def esc(s):
    return html.escape(s or '')


def shelf(sid):
    L = libs.get(sid) or {}
    return (L.get('call_numbers') or '').split('|')[0].strip() or sid


def lib(sid):
    return (libs.get(sid) or {}).get('library_code') or '—'


def cat_title(sid):
    return (libs.get(sid) or {}).get('title') or ''


BANDS = {
    ('track1_direct', 'high_confidence_algorithmic'): ('High-confidence match', 'b-hi'),
    ('track1_direct', 'expert_verified'): ('High-confidence match', 'b-hi'),
    ('track1_direct', 'tier_a'): ('tier A', 'b-a'),
    ('track1_direct', 'screening_rb'): ('screening — rule-based', 'b-scr'),
    ('track1_direct', 'screening_canon'): ('screening — canon', 'b-scr'),
    ('propagated', 'corroborated'): ('corroborated', 'b-corr'),
    ('propagated', 'weak'): ('matching witnesses (weak)', 'b-weak'),
    ('propagated', 'not_evaluated'): ('shared text — not evaluated', 'b-ne'),
}
RANK = {'high_confidence_algorithmic': 0, 'expert_verified': 0, 'tier_a': 1, 'corroborated': 2,
        'weak': 3, 'screening_rb': 4, 'screening_canon': 5, 'not_evaluated': 6}

NOTES = {
    'clean': 'The common case — one strong identification, nothing else. 141,553 pages look like this.',
    'commentary': 'A commentary manuscript: the base text and its commentary both match, on different '
                  'passages — legitimately multi-register. Also carries a canonical duplicate.',
    'judeo-arabic': 'Judeo-Arabic multi-register: two Tafsir portions plus a commentary, all correct, '
                    'all on different passages.',
    'reviewed': 'An expert-reviewed row — the only case that earns a review badge.',
    'siddur': 'THE PROBLEM CASE you flagged: a prayer book whose verse-chain page pulls in later works '
              'that merely quote the same verses.',
    'shared-text': 'Page-relation heavy: 54 shared-text rows touch this page — the bucket that used to '
                   'be invisible.',
    'high-count': '427 identifications across 427 pages from 8 works — what "elsewhere in this '
                  'manuscript" has to survive.',
}


def bucket(m):
    """Apply the agreed rules; return (identifications, also_shares, behind_toggle, stats)."""
    anchor = m['anchor_page']
    page_len = letters(texts.get(anchor, '')) or 1
    on_page = [r for r in m['claims'] if r['page_id'] == anchor and r['routing_status'] == 'shipped']
    elsewhere = [r for r in m['claims'] if r['page_id'] != anchor and r['routing_status'] == 'shipped']
    review_only = [r for r in m['claims'] if r['routing_status'] != 'shipped']

    # D-13a canonical collapse
    by_canon = defaultdict(list)
    for r in on_page:
        by_canon[r['canonical_work_id'] or r['work_id']].append(r)
    collapsed = []
    n_collapsed = 0
    for key, rs in by_canon.items():
        rs.sort(key=lambda r: RANK.get(r['confidence_band'], 9))
        lead = dict(rs[0])
        lead['_title'] = lead.get('canon_title') or lead['neutral_title']
        lead['_alias_dropped'] = len(rs) - 1
        n_collapsed += len(rs) - 1
        collapsed.append(lead)

    # D-13b/new: same-span groups with >1 canonical work -> generic shared passage
    spans = defaultdict(list)
    for r in collapsed:
        if r['evidence_kind'] == 'witness':
            spans[(r['span_start'], r['span_end'])].append(r)
    generic = []
    ids = []
    for r in collapsed:
        grp = spans.get((r['span_start'], r['span_end']), [])
        if r['evidence_kind'] == 'witness' and len(grp) > 1:
            generic.append(r)
        else:
            ids.append(r)

    # D-13c short evidence -> behind the toggle
    def size(r):
        return r['matched_letters'] if r['matched_letters'] else (r['span_end'] - r['span_start'])
    short = [r for r in ids if size(r) < SHORT_LETTERS]
    ids = [r for r in ids if size(r) >= SHORT_LETTERS]
    ids.sort(key=lambda r: (RANK.get(r['confidence_band'], 9), -(size(r) or 0)))
    generic.sort(key=lambda r: (r['span_start'], RANK.get(r['confidence_band'], 9)))

    st_shipped = [r for r in m['shared_text'] if r['routing_status'] == 'shipped']
    rel_pages = len({r['other_page_id'] if r['a_page_id'] == anchor else r['a_page_id']
                     for r in st_shipped if r.get('other_page_id') or r.get('a_page_id')})
    return ids, generic, short, {
        'page_len': page_len, 'elsewhere': elsewhere, 'review_only': review_only,
        'rel_pages': rel_pages, 'n_collapsed': n_collapsed,
        'span_groups': len([g for g in spans.values() if len(g) > 1]),
    }


def pct_html(r, page_len):
    if r['evidence_source'] != 'track1_direct' or not r['matched_letters']:
        return '<span class="nopct">no coverage figure for this family</span>'
    p = round(100 * r['matched_letters'] / page_len)
    return f'<span class="pct">{p}% of page matched · {r["matched_letters"]} letters</span>'


def row(r, page_len, generic=False):
    label, cls = BANDS.get((r['evidence_source'], r['confidence_band']), (r['confidence_band'], 'b-a'))
    badge = ('<span class="rev">Expert-reviewed ✓ <span class="he">נבדק בידי מומחה</span></span>'
             if r['adjudication_status'] == 'human_confirmed'
             else '<span class="unrev">unreviewed · algorithmic estimate</span>')
    alias = (f'<span class="alias">canonical duplicate collapsed ×{r["_alias_dropped"]}</span>'
             if r.get('_alias_dropped') else '')
    w = wc.get(r['work_id'], {})
    others = (f'<button class="btn">Other manuscripts matching this work ▸ '
              f'{w.get("nsys", "?")}</button>')
    return f"""
      <div class="claim">
        <div class="c-main">
          <span class="c-title" dir="rtl">{esc(r.get('_title') or r['neutral_title'])}</span>
          <span class="c-en">Matches · התאמה</span>
          {pct_html(r, page_len)}
          {alias}
        </div>
        <div class="c-meta">
          <span class="band {cls}">{label}</span>{badge}
          <span class="wid">{r['work_id']}</span>
          <span dir="rtl">{esc(r.get('author') or '')}</span>
          <span class="off">offsets {r['span_start']}–{r['span_end']}</span>
        </div>
        <div class="c-act">
          <button class="btn">Evidence · ראיות</button>{'' if generic else others}
          <span class="vote">✓ · ? · ✗ <span class="ph">(placeholder)</span></span>
        </div>
      </div>"""


cards, summary = [], []
for m in d['manuscripts']:
    ids, generic, short, s = bucket(m)
    n_toggle = len(short) + len(s['review_only'])
    summary.append((m['kind'], shelf(m['sys_id']), lib(m['sys_id']), m['n_shipped'],
                    len(ids), len(generic), n_toggle, s['rel_pages'], s['n_collapsed']))
    gen_block = ''
    if generic:
        by_span = defaultdict(list)
        for r in generic:
            by_span[(r['span_start'], r['span_end'])].append(r)
        parts = []
        for (a, b), rs in sorted(by_span.items()):
            names = ' · '.join(f'<span dir="rtl">{esc(r.get("_title") or r["neutral_title"])}</span>'
                               for r in rs)
            parts.append(f'<div class="gen-row">One passage (offsets {a}–{b}, '
                         f'{rs[0]["matched_letters"] or b - a} letters) appears in '
                         f'<strong>{len(rs)} works</strong>: {names}</div>')
        gen_block = f"""
      <div class="bucket2">
        <div class="b-head">Also shares text with · <span dir="rtl">חולק טקסט גם עם</span>
          <span class="cnt">{len(generic)} works over {len(by_span)} passage(s)</span>
          <span class="ph">— collapsed by default; not presented as identifications</span></div>
        {''.join(parts)}
      </div>"""
    cards.append(f"""
    <div class="card {'problem' if m['kind'] == 'siddur' else ''}">
      <div class="ms-head">
        <span class="ms-shelf">{esc(shelf(m['sys_id']))}</span>
        <span class="ms-lib">{esc(lib(m['sys_id']))}</span>
        <span class="ms-cat" dir="rtl">{esc(cat_title(m['sys_id']))}</span>
      </div>
      <div class="ms-note">{esc(NOTES[m['kind']])}</div>

      <div class="b-head">Identifications · <span dir="rtl">זיהויים</span>
        <span class="cnt">On this page — {len(ids)} shown</span></div>
      {''.join(row(r, s['page_len']) for r in ids) or '<div class="empty">nothing survives the default rules on this page</div>'}

      {gen_block}

      <div class="toggle">Elsewhere in this manuscript ▸
        <span class="cnt">{len(s['elsewhere'])} more on {len({r['page_id'] for r in s['elsewhere']})} pages</span>
      </div>
      <div class="toggle">Pages matching this page in other manuscripts ▸
        <span class="cnt">{s['rel_pages']} pages</span>
        <span class="ph">— unevaluated candidate alignments</span></div>
      <div class="toggle">Show more possible matches ▾
        <span class="cnt">{n_toggle}</span>
        <span class="ph">— {len(short)} short-passage, {len(s['review_only'])} screening / review-only</span></div>
      <div class="disc">Not exhaustive — more identifications may exist. ·
        <span dir="rtl">אינו ממצה — ייתכנו זיהויים נוספים.</span></div>
    </div>""")

srows = ''.join(
    f'<tr class="{"hl" if k == "siddur" else ""}"><td>{esc(sh)}</td><td>{esc(lb)}</td>'
    f'<td class="num">{tot}</td><td class="num strong">{i}</td><td class="num">{g}</td>'
    f'<td class="num">{t}</td><td class="num">{rp}</td><td class="num">{nc}</td></tr>'
    for k, sh, lb, tot, i, g, t, rp, nc in summary)

HTML = f"""<title>Phase 136 — panel across seven real manuscripts</title>
<style>
  :root {{ --bg:#fbfaf8; --fg:#1c1a17; --mut:#6b645c; --line:#e2ddd5; --card:#fff; --accent:#7a4f2c;
    --warnbg:#fff6ea; --warnfg:#8a4b00; --okfg:#2f7d4f; }}
  @media (prefers-color-scheme: dark) {{ :root {{ --bg:#16151a; --fg:#ebe7e0; --mut:#a19a90;
    --line:#2f2c34; --card:#1e1d23; --accent:#d8a878; --warnbg:#3a2a12; --warnfg:#f0c48a; --okfg:#79c79b; }} }}
  :root[data-theme="dark"] {{ --bg:#16151a; --fg:#ebe7e0; --mut:#a19a90; --line:#2f2c34; --card:#1e1d23;
    --accent:#d8a878; --warnbg:#3a2a12; --warnfg:#f0c48a; --okfg:#79c79b; }}
  :root[data-theme="light"] {{ --bg:#fbfaf8; --fg:#1c1a17; --mut:#6b645c; --line:#e2ddd5; --card:#fff;
    --accent:#7a4f2c; --warnbg:#fff6ea; --warnfg:#8a4b00; --okfg:#2f7d4f; }}
  body {{ background:var(--bg); color:var(--fg); font:15px/1.55 -apple-system,Segoe UI,Roboto,sans-serif;
    margin:0; padding:0 0 4rem; }}
  .wrap {{ max-width:1020px; margin:0 auto; padding:0 1.1rem; }}
  h1 {{ font-size:1.45rem; margin:2rem 0 .3rem; }}
  h2 {{ font-size:1.1rem; margin:2.4rem 0 .4rem; padding-top:1.2rem; border-top:1px solid var(--line); }}
  .lede {{ color:var(--mut); margin:.2rem 0 1.2rem; }}
  .card {{ background:var(--card); border:1px solid var(--line); border-radius:11px; padding:1rem 1.1rem;
    margin:1.1rem 0; }}
  .card.problem {{ border-color:var(--warnfg); }}
  .ms-head {{ display:flex; gap:.6rem; align-items:baseline; flex-wrap:wrap; }}
  .ms-shelf {{ font-weight:700; font-size:1.02rem; }}
  .ms-lib {{ font-size:.75rem; border:1px solid var(--line); border-radius:20px; padding:0 .45rem;
    color:var(--mut); }}
  .ms-cat {{ color:var(--mut); font-size:.9rem; }}
  .ms-note {{ font-size:.85rem; color:var(--mut); font-style:italic; margin:.35rem 0 .8rem; }}
  .card.problem .ms-note {{ color:var(--warnfg); font-style:normal; }}
  .b-head {{ font-weight:650; font-size:.93rem; margin:.9rem 0 .1rem; }}
  .b-head .cnt {{ font-weight:400; color:var(--mut); margin-inline-start:.4rem; }}
  .claim {{ border-top:1px solid var(--line); padding:.6rem 0 .55rem; }}
  .c-main {{ display:flex; align-items:baseline; gap:.5rem; flex-wrap:wrap; }}
  .c-title {{ font-size:1.02rem; font-weight:600; }}
  .c-en {{ color:var(--mut); font-size:.78rem; }}
  .pct {{ font-size:.78rem; color:var(--mut); border:1px solid var(--line); border-radius:20px;
    padding:.05rem .5rem; }}
  .nopct {{ font-size:.75rem; color:var(--mut); font-style:italic; }}
  .alias {{ font-size:.72rem; color:var(--accent); border:1px dashed var(--accent); border-radius:20px;
    padding:0 .45rem; }}
  .c-meta {{ display:flex; align-items:center; gap:.5rem; flex-wrap:wrap; font-size:.78rem;
    color:var(--mut); margin-top:.22rem; }}
  .band {{ border:1px solid var(--line); border-radius:5px; padding:.05rem .45rem; }}
  .b-hi {{ border-color:var(--okfg); color:var(--okfg); }}
  .b-a {{ border-color:var(--accent); color:var(--accent); }}
  .b-ne, .b-weak {{ opacity:.8; }}
  .rev {{ color:var(--okfg); font-weight:600; }}
  .rev .he {{ opacity:.8; font-weight:400; }}
  .unrev {{ font-style:italic; }}
  .wid {{ font-family:ui-monospace,monospace; font-size:.7rem; opacity:.55; }}
  .off {{ font-family:ui-monospace,monospace; font-size:.7rem; opacity:.5; }}
  .c-act {{ display:flex; gap:.4rem; flex-wrap:wrap; align-items:center; margin-top:.4rem; }}
  .btn {{ background:transparent; color:var(--accent); border:1px solid var(--line); border-radius:6px;
    padding:.2rem .5rem; font-size:.78rem; cursor:pointer; }}
  .vote {{ margin-inline-start:auto; font-size:.82rem; color:var(--mut); }}
  .ph {{ font-size:.72rem; opacity:.65; }}
  .bucket2 {{ background:var(--warnbg); border-radius:8px; padding:.6rem .75rem; margin:.9rem 0 .3rem; }}
  .gen-row {{ font-size:.86rem; margin:.3rem 0; }}
  .toggle {{ border-top:1px solid var(--line); margin-top:.6rem; padding-top:.55rem; font-size:.86rem;
    color:var(--accent); }}
  .empty {{ font-size:.85rem; color:var(--mut); font-style:italic; border-top:1px solid var(--line);
    padding-top:.55rem; }}
  .disc {{ font-size:.8rem; color:var(--mut); margin-top:.65rem; font-style:italic; }}
  table {{ border-collapse:collapse; width:100%; font-size:.86rem; }}
  th, td {{ text-align:start; padding:.4rem .5rem; border-bottom:1px solid var(--line); }}
  th {{ color:var(--mut); font-size:.72rem; text-transform:uppercase; letter-spacing:.05em; }}
  td.num {{ text-align:end; font-variant-numeric:tabular-nums; }}
  td.strong {{ font-weight:700; }}
  tr.hl td {{ background:var(--warnbg); }}
  .tbl-wrap {{ overflow-x:auto; }}
  ul.tight {{ margin:.4rem 0 0; padding-inline-start:1.2rem; }}
  ul.tight li {{ margin:.25rem 0; }}
  .prov {{ font-size:.8rem; color:var(--mut); background:var(--card); border:1px solid var(--line);
    border-radius:8px; padding:.7rem .9rem; }}
</style>

<div class="wrap">
<h1>Phase 136 panel — seven real manuscripts, all rules applied</h1>
<p class="lede">Every row is real, from the deployed asset. The rules now applied: canonical duplicates
collapsed, identical-span groups pulled out of the identifications, short passages behind the toggle,
coverage shown only where it is defined, review badge only where a human actually reviewed.</p>

<div class="prov"><strong>Rules in force.</strong> Canonical collapse · identical-span groups →
“Also shares text with” (not identifications) · under {SHORT_LETTERS} matched letters → behind the
toggle · coverage figure on the direct family only · <em>mock:</em> vote controls (phase 137);
novelty is absent entirely because it is not computed yet.</div>

<h2>What the rules do, per manuscript</h2>
<div class="tbl-wrap"><table>
  <tr><th>Shelfmark</th><th>Lib</th><th>Shipped on MS</th><th>Shown as IDs</th>
    <th>Shares-text</th><th>Behind toggle</th><th>Related pages</th><th>Dupes collapsed</th></tr>
  {srows}
</table></div>

<h2>The panels</h2>
{''.join(cards)}

<h2>What to look for</h2>
<div class="card">
  <ul class="tight">
    <li><strong>The prayer book</strong> — does moving the identical-span group out of the
      identifications give you the reading you wanted? Note what is left behind: a lone Mishneh Torah
      match on its own passage, which no rule in this phase catches. That is the case we agreed to
      leave to the later evidence refresh.</li>
    <li><strong>The commentary and Judeo-Arabic manuscripts</strong> — the multi-register cases must
      survive intact. Several works on one page, different passages, all correct: if these look thinned
      out, the rules are too aggressive.</li>
    <li><strong>The clean case and the reviewed case</strong> — the reference for what a good row looks
      like, and the only place a review badge appears.</li>
    <li><strong>The shares-text bucket wording</strong> — “One passage appears in N works” is the
      honest phrasing, but is it useful to you, or just noise to collapse and forget?</li>
    <li><strong>The 427-identification manuscript</strong> — one match per page across 427 pages. Is
      “Elsewhere in this manuscript” a usable place to put that, or does it need its own view?</li>
  </ul>
</div>
</div>
"""

open(OUT, 'w', encoding='utf-8').write(HTML)
print('wrote', OUT, os.path.getsize(OUT))
for s in summary:
    print(s)
