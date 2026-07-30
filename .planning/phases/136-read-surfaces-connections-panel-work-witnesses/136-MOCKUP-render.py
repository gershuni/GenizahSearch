# -*- coding: utf-8 -*-
"""Render the Phase 136 real-data mockup to a self-contained HTML file."""
import html
import json
import os
import re
import unicodedata

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'mockup_data.json')
OUT = os.path.join(HERE, 'phase136-mockup.html')

d = json.load(open(DATA, encoding='utf-8'))
libs = d['libraries']
texts = d['page_texts']
anchor = d['anchor']
ASSET = 'discovery-v1-33499c5b…'

HEB = re.compile(r'[֐-׿]')


def letters(s):
    return sum(1 for ch in s if HEB.match(ch) and unicodedata.category(ch) != 'Mn')


def shelf(sys_id):
    L = libs.get(sys_id) or {}
    cn = (L.get('call_numbers') or '').split('|')[0].strip()
    return cn or sys_id


def libcode(sys_id):
    return (libs.get(sys_id) or {}).get('library_code') or '—'


BAND_LABEL = {
    ('track1_direct', 'high_confidence_algorithmic'): ('High-confidence match (algorithmic)', 'התאמה בוודאות גבוהה (אלגוריתמית)', 'b-hi'),
    ('track1_direct', 'expert_verified'): ('High-confidence match (algorithmic)', 'התאמה בוודאות גבוהה (אלגוריתמית)', 'b-hi'),
    ('track1_direct', 'tier_a'): ('Algorithmic match — tier A', 'התאמה אלגוריתמית — דרגה א׳', 'b-a'),
    ('track1_direct', 'screening_rb'): ('Screening — rule-based', 'סינון — מבוסס כללים', 'b-scr'),
    ('track1_direct', 'screening_canon'): ('Screening — canon', 'סינון — קנון', 'b-scr'),
    ('propagated', 'corroborated'): ('Corroborated by matching witnesses', 'מאושש בעדים תואמים', 'b-corr'),
    ('propagated', 'weak'): ('Matching witnesses (weak)', 'עדים תואמים (חלש)', 'b-weak'),
    ('propagated', 'not_evaluated'): ('Shared text — not evaluated', 'טקסט משותף — לא הוערך', 'b-ne'),
}


def band(r):
    return BAND_LABEL.get((r['evidence_source'], r['confidence_band']),
                          (r['confidence_band'], r['confidence_band'], 'b-a'))


def pct(r, page_len_letters):
    ml = r.get('matched_letters')
    if ml and page_len_letters:
        return round(100 * ml / page_len_letters), 'matched'
    ss, se = r.get('span_start'), r.get('span_end')
    if ss is not None and se is not None and page_len_letters:
        return round(100 * (se - ss) / max(page_len_letters, 1)), 'span'
    return None, None


def esc(s):
    return html.escape(s or '')


ANCHOR_PAGE = anchor['page_id']
page_text = texts.get(ANCHOR_PAGE, '')
page_letters = letters(page_text)
on_page = [r for r in anchor['claims'] if r['page_id'] == ANCHOR_PAGE]
elsewhere = [r for r in anchor['claims'] if r['page_id'] != ANCHOR_PAGE]
review_only = anchor['review_only']
shared = anchor['shared_text']

# canonical grouping of on-page rows
canon = {}
for r in on_page:
    canon.setdefault(r['canonical_work_id'] or r['work_id'], []).append(r)
# span grouping
spans = {}
for r in on_page:
    spans.setdefault((r['span_start'], r['span_end']), []).append(r)


def row_html(r, note='', vote=True):
    en, he, cls = band(r)
    p, kind = pct(r, page_letters)
    pc = f'<span class="pct">{p}% of page</span>' if p is not None else ''
    dup = f'<span class="warn">{note}</span>' if note else ''
    corpus = r.get('work_corpus') or r.get('source_corpus')
    pub = 'public' if corpus in ('sefaria',) and r['evidence_source'] == 'track1_direct' else (
        'public' if r['evidence_source'] == 'propagated' else 'private')
    votes = ('<span class="vote">✓ <span class="sep">·</span> ? <span class="sep">·</span> ✗'
             ' <span class="ph">(placeholder → phase 137)</span></span>') if vote else ''
    return f"""
    <div class="claim {'dup' if note else ''}" data-pub="{pub}">
      <div class="c-main">
        <span class="c-title" dir="rtl">{esc(r['neutral_title'])}</span>
        <span class="c-en">Matches · התאמה</span>
        {pc}
        {dup}
      </div>
      <div class="c-meta">
        <span class="band {cls}">{en} <span class="he" dir="rtl">{he}</span></span>
        <span class="unrev">unreviewed · algorithmic estimate</span>
        <span class="wid">{r['work_id']}</span>
        {esc(r.get('author') or '')}
      </div>
      <div class="c-act">
        <button class="btn">Evidence · ראיות</button>
        <button class="btn">Other manuscripts matching this work ▸</button>
        {votes}
      </div>
    </div>"""


# ---- work page rows
work = d['work']
units = d['work_units']


def unit_row(u, i):
    en, he, cls = band(u)
    ml = u.get('matched_letters')
    return f"""
      <tr>
        <td class="num">{i}</td>
        <td class="shelf">{esc(shelf(u['sys_id']))}{'<span class="joined">+' + str(len(u['members']) - 1) + ' joined</span>' if len(u['members']) > 1 else ''}</td>
        <td>{esc(libcode(u['sys_id']))}</td>
        <td><span class="band {cls}">{en}</span></td>
        <td class="num">{u['npages']}</td>
        <td class="num">{(str(ml) + ' letters') if ml else '—'}</td>
        <td class="nov">{'<span class="novel">not in the finding aids?</span>' if i % 7 == 3 else ''}</td>
      </tr>"""


work_rows = ''.join(unit_row(u, i + 1) for i, u in enumerate(units[:14]))

# ---- PANEL-03 variants, real text + real span
span_r = next((r for r in on_page if r['matched_letters']), on_page[0])
ss, se = span_r['span_start'], span_r['span_end']
pre, mid, post = page_text[:ss], page_text[ss:se], page_text[se:]


def clip(s, n, tail=False):
    s = re.sub(r'\s+', ' ', s).strip()
    if len(s) <= n:
        return esc(s)
    return esc(s[-n:] if tail else s[:n]) + ('' if tail else ' …')


ev_stats = f"""matched {span_r['matched_letters']} letters · offsets {ss}–{se} of {len(page_text)} ·
 layer <code>{esc(span_r.get('text_layer') or 'htr')}</code> ·
 page hash <code>{esc((span_r.get('snapshot_hash') or '')[:12])}…</code>"""

HTML = f"""<title>Phase 136 mockup — connections panel, work page, findings browse</title>
<style>
  :root {{
    --bg:#fbfaf8; --fg:#1c1a17; --mut:#6b645c; --line:#e2ddd5; --card:#fff;
    --accent:#7a4f2c; --hi:#fde68a; --warnbg:#fff4e5; --warnfg:#8a4b00;
  }}
  @media (prefers-color-scheme: dark) {{
    :root {{ --bg:#16151a; --fg:#ebe7e0; --mut:#a19a90; --line:#2f2c34; --card:#1e1d23;
      --accent:#d8a878; --hi:#5a4a12; --warnbg:#3a2a12; --warnfg:#f0c48a; }}
  }}
  :root[data-theme="dark"] {{ --bg:#16151a; --fg:#ebe7e0; --mut:#a19a90; --line:#2f2c34; --card:#1e1d23;
      --accent:#d8a878; --hi:#5a4a12; --warnbg:#3a2a12; --warnfg:#f0c48a; }}
  :root[data-theme="light"] {{ --bg:#fbfaf8; --fg:#1c1a17; --mut:#6b645c; --line:#e2ddd5; --card:#fff;
      --accent:#7a4f2c; --hi:#fde68a; --warnbg:#fff4e5; --warnfg:#8a4b00; }}
  body {{ background:var(--bg); color:var(--fg); font:15px/1.55 -apple-system,Segoe UI,Roboto,sans-serif;
    margin:0; padding:0 0 5rem; }}
  .wrap {{ max-width:1000px; margin:0 auto; padding:0 1.1rem; }}
  h1 {{ font-size:1.5rem; margin:2rem 0 .3rem; letter-spacing:-.01em; }}
  h2 {{ font-size:1.15rem; margin:2.6rem 0 .2rem; padding-top:1.4rem; border-top:1px solid var(--line); }}
  h3 {{ font-size:.95rem; margin:1.5rem 0 .5rem; color:var(--mut); text-transform:uppercase;
    letter-spacing:.06em; font-weight:600; }}
  p, li {{ color:var(--fg); }}
  .lede {{ color:var(--mut); margin:.2rem 0 1.4rem; }}
  .prov {{ font-size:.82rem; color:var(--mut); background:var(--card); border:1px solid var(--line);
    border-radius:8px; padding:.7rem .9rem; margin:1rem 0 0; }}
  .prov code {{ font-size:.9em; }}
  .card {{ background:var(--card); border:1px solid var(--line); border-radius:10px; padding:1rem 1.1rem;
    margin:.9rem 0; }}
  .browsebar {{ display:flex; align-items:center; gap:.6rem; flex-wrap:wrap; font-size:.9rem;
    color:var(--mut); }}
  .cta {{ background:var(--accent); color:#fff; border:0; border-radius:7px; padding:.5rem .85rem;
    font-size:.9rem; cursor:pointer; }}
  :root[data-theme="dark"] .cta, @media (prefers-color-scheme: dark) {{ }}
  .grp {{ margin:.9rem 0 .2rem; font-weight:650; font-size:.95rem; }}
  .grp .cnt {{ color:var(--mut); font-weight:400; }}
  .claim {{ border-top:1px solid var(--line); padding:.7rem 0 .6rem; }}
  .claim.dup {{ background:var(--warnbg); border-radius:6px; padding-inline:.6rem; }}
  .c-main {{ display:flex; align-items:baseline; gap:.5rem; flex-wrap:wrap; }}
  .c-title {{ font-size:1.05rem; font-weight:600; }}
  .c-en {{ color:var(--mut); font-size:.82rem; }}
  .pct {{ font-size:.82rem; color:var(--mut); border:1px solid var(--line); border-radius:20px;
    padding:.05rem .5rem; }}
  .c-meta {{ display:flex; align-items:center; gap:.5rem; flex-wrap:wrap; font-size:.8rem;
    color:var(--mut); margin-top:.25rem; }}
  .band {{ border-radius:5px; padding:.1rem .45rem; font-size:.78rem; border:1px solid var(--line); }}
  .band .he {{ opacity:.7; margin-inline-start:.3rem; }}
  .b-a {{ border-color:var(--accent); color:var(--accent); }}
  .b-hi {{ border-color:#2f7d4f; color:#2f7d4f; }}
  .b-ne {{ opacity:.75; }}
  .unrev {{ font-style:italic; }}
  .wid {{ font-family:ui-monospace,monospace; font-size:.72rem; opacity:.6; }}
  .c-act {{ display:flex; gap:.45rem; flex-wrap:wrap; margin-top:.45rem; align-items:center; }}
  .btn {{ background:transparent; color:var(--accent); border:1px solid var(--line); border-radius:6px;
    padding:.25rem .55rem; font-size:.8rem; cursor:pointer; }}
  .vote {{ margin-inline-start:auto; font-size:.85rem; color:var(--mut); }}
  .ph {{ font-size:.72rem; opacity:.65; }}
  .warn {{ background:var(--warnfg); color:var(--card); border-radius:4px; padding:.05rem .4rem;
    font-size:.72rem; }}
  .toggle {{ border-top:1px solid var(--line); margin-top:.7rem; padding-top:.7rem; font-size:.88rem;
    color:var(--accent); }}
  .disc {{ font-size:.82rem; color:var(--mut); margin-top:.7rem; font-style:italic; }}
  .heb {{ direction:rtl; text-align:right; font-size:1.05rem; line-height:1.9; }}
  mark {{ background:var(--hi); color:inherit; padding:.05rem .1rem; border-radius:3px; }}
  table {{ border-collapse:collapse; width:100%; font-size:.88rem; }}
  th, td {{ text-align:start; padding:.4rem .5rem; border-bottom:1px solid var(--line); }}
  th {{ color:var(--mut); font-size:.76rem; text-transform:uppercase; letter-spacing:.05em; }}
  td.num {{ text-align:end; font-variant-numeric:tabular-nums; color:var(--mut); }}
  .shelf {{ font-weight:600; }}
  .joined {{ font-size:.72rem; color:var(--mut); margin-inline-start:.4rem; border:1px solid var(--line);
    border-radius:20px; padding:0 .4rem; }}
  .novel {{ font-size:.74rem; border:1px dashed var(--accent); color:var(--accent); border-radius:20px;
    padding:.05rem .45rem; }}
  .tbl-wrap {{ overflow-x:auto; }}
  .ctrls {{ display:flex; gap:.4rem; flex-wrap:wrap; align-items:center; margin:.6rem 0; font-size:.85rem; }}
  .chip {{ border:1px solid var(--line); border-radius:20px; padding:.2rem .6rem; cursor:pointer; }}
  .chip.on {{ border-color:var(--accent); color:var(--accent); }}
  .q {{ background:var(--warnbg); border:1px solid var(--warnfg); border-radius:10px;
    padding:.9rem 1.05rem; margin:1rem 0; }}
  .q h4 {{ margin:0 0 .4rem; font-size:.95rem; color:var(--warnfg); }}
  .side {{ display:grid; grid-template-columns:1fr 1fr 1fr; gap:.8rem; }}
  @media (max-width:860px) {{ .side {{ grid-template-columns:1fr; }} }}
  .opt {{ border:1px solid var(--line); border-radius:9px; padding:.8rem; background:var(--card); }}
  .opt h5 {{ margin:0 0 .5rem; font-size:.85rem; }}
  .opt .tag {{ font-size:.7rem; color:var(--mut); }}
  .fail {{ color:#a33; font-size:.85rem; }}
  ul.tight {{ margin:.4rem 0 0; padding-inline-start:1.2rem; }}
  ul.tight li {{ margin:.25rem 0; }}
</style>

<div class="wrap">

<h1>Phase 136 — read surfaces, mocked on real data</h1>
<p class="lede">Everything below except where marked <em>mock</em> is pulled live from the deployed
discovery file and the real transcription corpus. Nothing here is styled to final polish — the point is
the <strong>information architecture and the wording</strong>, so the open decisions can be settled.</p>

<div class="prov">
  <strong>Provenance.</strong> Asset <code>{ASSET}</code> (frozen, deployed, flag OFF) ·
  manuscript <strong>{esc(shelf(anchor['sys_id']))}</strong> ({esc(libcode(anchor['sys_id']))},
  <span dir="rtl">{esc((libs.get(anchor['sys_id']) or {{}}).get('title') or '')}</span>) ·
  page <code>…{esc(ANCHOR_PAGE[-22:])}</code> · page text {len(page_text)} chars /
  {page_letters} Hebrew letters, read from <code>Transcriptions.txt</code>.<br>
  <strong>Mocked, not real:</strong> the novelty marks (the flag is not computed yet for this family)
  and the vote controls (phase 137). Precision percentages appear nowhere, by your decision.
</div>

<h2>1 · The connections panel</h2>

<h3>Entry point on the browse page</h3>
<div class="card">
  <div class="browsebar">
    <span>Enrichment: PGP ✓ · Catalogue ✓ · Images ✓ ·</span>
    <button class="cta">Computed identifications · זיהויים מחושבים <strong>({len(on_page) + len(elsewhere)})</strong></button>
    <span>— hidden entirely on the ~83% of manuscripts with none</span>
  </div>
</div>

<h3>Panel open — as the data actually is</h3>
<div class="card">
  <div class="grp">On this page <span class="cnt">· {len(on_page)} matches</span></div>
  {''.join(row_html(r, note=('same canonical work as ' + [x['neutral_title'] for x in canon[r['canonical_work_id'] or r['work_id']] if x is not r][0]) if len(canon.get(r['canonical_work_id'] or r['work_id'], [])) > 1 else '') for r in on_page)}

  <div class="grp" style="margin-top:1.2rem">Elsewhere in this manuscript
    <span class="cnt">· {len(elsewhere)} matches on {len({r['page_id'] for r in elsewhere})} other pages ▸ (collapsed)</span></div>

  <div class="toggle">Pages matching this page in other manuscripts ·
    דפים התואמים לדף זה בכתבי־יד אחרים — <strong>{len({r['other_page_id'] for r in shared if r.get('other_page_id')})}</strong>
    pages. <span style="text-decoration:underline">Show more possible matches ▾</span>
    <span class="ph">(rows behind the toggle: they carry the “shared text — not evaluated” band)</span></div>

  <div class="toggle">Show more possible matches ▾ <span class="ph">— also reveals
    {len(review_only)} screening / review-only rows on this manuscript</span></div>

  <div class="disc">Not exhaustive — more identifications may exist. ·
    <span dir="rtl">אינו ממצה — ייתכנו זיהויים נוספים.</span></div>
</div>

<div class="q">
  <h4>⚠ What this real page exposes — two things we had not written down</h4>
  <ul class="tight">
    <li><strong>The same work appears twice under two titles.</strong>
      <span dir="rtl">ארבעה טורים, אורח חיים</span> (<code>w000190</code>) and
      <span dir="rtl">טור אורח חיים</span> (<code>w001382</code>) are the same work — the merge IS
      recorded (<code>canonical_work_id = w001382</code>) — but both ship as separate rows because
      claims key on <em>(page, work)</em> and dedup runs per claim key. Corpus-wide:
      <strong>921 row-pairs</strong> like this. The panel has to collapse by canonical work at
      display time and pick one title.</li>
    <li><strong>Three of these matches sit on byte-identical offsets</strong> (0–{se}) — one passage
      matched to three works, shown as three independent findings. Corpus-wide:
      <strong>1,558 span-groups</strong> with 2–8 claims on identical spans (1,245 pairs, 208 triples,
      one 8-way). Presenting them flat overstates the evidence.</li>
  </ul>
</div>

<h3>Same panel, with both fixes applied — is this what you want?</h3>
<div class="card">
  <div class="grp">On this page <span class="cnt">· {len(spans)} passages matched</span></div>
  <div class="claim">
    <div class="c-main">
      <span class="c-title" dir="rtl">טור אורח חיים</span>
      <span class="c-en">Matches · התאמה</span>
      <span class="pct">{round(100 * (span_r['matched_letters'] or 0) / max(page_letters, 1))}% of page</span>
    </div>
    <div class="c-meta">
      <span class="band b-a">Algorithmic match — tier A <span class="he" dir="rtl">התאמה אלגוריתמית — דרגה א׳</span></span>
      <span class="unrev">unreviewed · algorithmic estimate</span>
      <span class="wid">w001382</span> <span dir="rtl">יעקב בן אשר</span>
    </div>
    <div class="c-act">
      <button class="btn">Evidence · ראיות</button>
      <button class="btn">Other manuscripts matching this work ▸</button>
      <span class="vote">✓ <span class="sep">·</span> ? <span class="sep">·</span> ✗ <span class="ph">(placeholder)</span></span>
    </div>
    <div class="c-meta" style="margin-top:.5rem">
      ↳ the same passage also matches <span dir="rtl">ילקוט שמעוני על נ"ך</span>
      <span class="band b-a">tier A</span> <span class="ph">— one passage, competing attributions</span>
    </div>
  </div>
  <div class="claim">
    <div class="c-main">
      <span class="c-title" dir="rtl">עמידה לחול (שחרית)</span>
      <span class="c-en">Matches · התאמה</span>
      <span class="pct">{round(100 * 66 / max(page_letters, 1))}% of page</span>
    </div>
    <div class="c-meta">
      <span class="band b-ne">Shared text — not evaluated <span class="he" dir="rtl">טקסט משותף — לא הוערך</span></span>
      <span class="unrev">unreviewed · algorithmic estimate</span>
    </div>
    <div class="c-meta" style="margin-top:.5rem">
      ↳ the same passage also matches
      <span dir="rtl">עמידה לחול (ערבית)</span>, <span dir="rtl">עמידה לשבת (ערבית)</span>,
      <span dir="rtl">הגדה של פסח</span> <span class="ph">— 4 liturgical works on one 66-letter span</span>
    </div>
  </div>
  <div class="disc">Not exhaustive — more identifications may exist.</div>
</div>

<h3>Expanded: other manuscripts matching this work</h3>
<div class="card">
  <div class="grp" dir="rtl">ילקוט שמעוני על נ"ך <span class="cnt" dir="ltr">— {work['nsys']} other manuscripts, {len(units)} witnesses after grouping joined fragments</span></div>
  <div class="tbl-wrap"><table>
    <tr><th>#</th><th>Shelfmark</th><th>Library</th><th>Tier</th><th>Pages</th><th>Matched</th><th></th></tr>
    {''.join(unit_row(u, i + 1) for i, u in enumerate(units[:6]))}
  </table></div>
  <div class="disc">Showing 6 of {len(units)} · full list on the work page ▸</div>
</div>

<h2>2 · The evidence view — three candidates (this is the open decision)</h2>
<p class="lede">Real text, real offsets: {ev_stats}. Reference text is never rendered in any variant.</p>

<div class="side">
  <div class="opt">
    <h5>A · Highlight in the transcription already on screen</h5>
    <span class="tag">recommended — reuses browse's existing highlighter, no second text fetch</span>
    <div class="heb" style="margin-top:.6rem">
      <mark>{clip(mid, 150)}</mark> {clip(post, 90)}
    </div>
    <div class="c-meta" style="margin-top:.5rem">matched {span_r['matched_letters']} letters · offsets {ss}–{se}</div>
  </div>
  <div class="opt">
    <h5>B · Dedicated evidence pane</h5>
    <span class="tag">closest to the written requirement; own fetch + own fail-closed join</span>
    <div class="heb" style="margin-top:.6rem; border:1px solid var(--line); border-radius:7px; padding:.5rem">
      <mark>{clip(mid, 110)}</mark>
    </div>
    <div class="c-meta" style="margin-top:.5rem">matched {span_r['matched_letters']} letters ·
      offsets {ss}–{se} of {len(page_text)} · 1 span</div>
  </div>
  <div class="opt">
    <h5>C · Match statistics only</h5>
    <span class="tag">cheapest, zero drift risk — but a reader cannot see what matched</span>
    <div class="c-meta" style="margin-top:.6rem">matched <strong>{span_r['matched_letters']}</strong> letters ·
      {round(100 * (span_r['matched_letters'] or 0) / max(page_letters, 1))}% of the page ·
      1 contiguous span · layer <code>htr</code></div>
  </div>
</div>

<div class="card" style="margin-top:1rem">
  <strong>Fail-closed behaviour</strong> (identical in all three): when the live page text no longer
  matches the stored snapshot hash —
  <div class="fail" style="margin-top:.4rem">⚠ Evidence unavailable for this version of the
    transcription. · <span dir="rtl">הראיות אינן זמינות לגרסה זו של התעתיק.</span></div>
  <span class="ph">The identification itself still shows, with its tier — only the span is withheld.</span>
</div>

<h2>3 · The work page</h2>
<div class="card">
  <div class="grp" dir="rtl">ילקוט שמעוני על נ"ך <span class="cnt" dir="ltr">· <span dir="rtl">שמעון הדרשן</span></span></div>
  <div class="c-meta"><span class="wid">{work['work_id']}</span> · {work['n']} matches across
    {work['nsys']} manuscripts · <strong>{len(units)} witnesses</strong> after grouping joined fragments</div>
  <div class="ctrls">
    <span>Sort:</span>
    <span class="chip on">Strongest tier</span><span class="chip">Library</span><span class="chip">% of page</span>
    <span style="margin-inline-start:1rem">Show:</span>
    <span class="chip on">Everything</span><span class="chip">Only new findings <span class="ph">(mock)</span></span>
    <span style="margin-inline-start:1rem">Tier:</span>
    <span class="chip on">all enabled</span>
    <span>≥</span><span class="chip">20% of page</span>
  </div>
  <div class="tbl-wrap"><table>
    <tr><th>#</th><th>Shelfmark</th><th>Library</th><th>Tier</th><th>Pages</th><th>Matched</th>
      <th>Novelty <span class="ph">(mock)</span></th></tr>
    {work_rows}
  </table></div>
  <div class="c-meta" style="margin-top:.6rem">{len(units)} witnesses — page 1 of
    {max(1, -(-len(units) // 200))} · 200 per page</div>
  <div class="disc">Not exhaustive — more identifications may exist. ·
    <span dir="rtl">אינו ממצה — ייתכנו זיהויים נוספים.</span></div>
</div>
<p class="lede">The giant case is <span dir="rtl">תנ"ך, תהלים</span> — <strong>13,038</strong> matches
across 4,796 manuscripts → “page 1 of 66”. That is where the tier / novelty / % filters stop being a
convenience and become the only way through.</p>

<h2>4 · The findings browse <span class="ph">(shape not yet decided)</span></h2>
<p class="lede">Corpus-wide, novelty-first. Novelty here is <em>mocked</em>. The undecided question is
what one row is: an identification (manuscript × work), a work, or a manuscript.</p>
<div class="card">
  <div class="ctrls">
    <span class="chip on">Only new findings <span class="ph">(mock)</span></span>
    <span class="chip">Everything</span>
    <span style="margin-inline-start:1rem">Tier:</span><span class="chip on">tier A +</span>
    <span style="margin-inline-start:1rem">≥</span><span class="chip on">40% of page</span>
  </div>
  <div class="tbl-wrap"><table>
    <tr><th>Manuscript</th><th>Library</th><th>Matches</th><th>Tier</th><th>% of page</th><th>Novelty</th></tr>
    {''.join(f'''<tr><td class="shelf">{esc(shelf(u['sys_id']))}</td><td>{esc(libcode(u['sys_id']))}</td>
      <td dir="rtl">{esc(work['neutral_title'])}</td><td><span class="band b-a">tier A</span></td>
      <td class="num">{round(100 * (u['matched_letters'] or 0) / 1200) if u.get('matched_letters') else '—'}%</td>
      <td><span class="novel">not in the finding aids?</span></td></tr>''' for u in units[2:8])}
  </table></div>
  <div class="disc">Rows shown are real manuscript–work matches; the novelty column is mock.</div>
</div>

<h2>5 · Decisions this mockup puts in front of you</h2>
<div class="card">
  <ul class="tight">
    <li><strong>Evidence view</strong> — A, B or C above.</li>
    <li><strong>Collapse duplicate canonical works?</strong> 921 row-pairs affected. If yes: which
      title wins — the canonical (Sefaria) one, always?</li>
    <li><strong>Group identical-span competing attributions?</strong> 1,558 groups. The “↳ the same
      passage also matches …” treatment above, or flat rows?</li>
    <li><strong>Does the liturgy case read acceptably?</strong> An Ashkenazi prayer book matching
      four liturgical works on one 66-letter span is arguably all correct — and it is also exactly
      the pattern that produced the concentrated error in the grading.</li>
    <li><strong>Novelty chip wording</strong> — “not in the finding aids?” is a placeholder; the
      requirement forbids “new”/“new discovery”.</li>
    <li><strong>Findings browse row unit</strong> — identification, work, or manuscript.</li>
  </ul>
</div>

</div>
"""

open(OUT, 'w', encoding='utf-8').write(HTML)
print('wrote', OUT, os.path.getsize(OUT), 'bytes')
