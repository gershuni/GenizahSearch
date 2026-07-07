# -*- coding: utf-8 -*-
"""Interactive text-reuse graph v2 (self-contained HTML, canvas, no CDN deps).

Usage: python build_reuse_graph.py [db_path] [tag]
Output: review/rehearsal_<tag>_graph.html

v2 (feedback: "giant blob + disconnected confetti, click on blob froze"):
- Oversized continuation components (> SPLIT_AT members) are decomposed with
  LOUVAIN community detection (recursively, max 3 levels) — the giant
  connected component (Bible + liturgy + piyyut + exegesis, bridged) becomes
  dozens of explorable circles instead of one dead blob. Its communities
  differentiate by FJMS domain (Bible/Liturgy/Piyyut/Exegesis/Halakha…), so
  labeling it "liturgical" would be factually wrong — it is simply the
  giant component, pre Track-1 canonical masking.
- The overview is a real NETWORK: inter-cluster links are drawn (green =
  same-work bridges e.g. between communities of the former giant; orange =
  quotation/island links between different works). Circle layout is
  force-directed in Python over the link structure, so connected clusters
  sit near each other.
- Drill-in edges are capped to the strongest 4,000 and the biggest clusters
  sampled to their top-300 most-connected members — no more freeze.
"""
import csv
import json
import math
import sqlite3
import sys
import time
from collections import Counter, defaultdict

import community as community_louvain
import networkx as nx
import numpy as np
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

ROOT = r"C:\Genizahsearch"
DB = sys.argv[1] if len(sys.argv) > 1 else \
    ROOT + r"\same_work_spike\probe\data\rehearsal.db"
TAG = sys.argv[2] if len(sys.argv) > 2 else "100k"
TABLE = sys.argv[3] if len(sys.argv) > 3 else "accepted_pairs"
OUT = ROOT + rf"\same_work_spike\probe\review\rehearsal_{TAG}_graph.html"

MEMBER_CAP = 300      # drill-in node sample (top-degree)
EDGE_CAP = 4000       # drill-in edges (strongest)
LINK_CAP = 4000       # overview inter-cluster links (strongest)
SPLIT_AT = 800        # Louvain-decompose components bigger than this
MIN_CLUSTER = 2


def load_lib_meta():
    meta = {}
    with open(ROOT + r"\libraries.csv", encoding='utf-8-sig', newline='') as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) >= 4 and row[0]:
                variants = [v.strip() for v in (row[2] or '').split('|')
                            if v.strip()]
                title = row[7].strip() if len(row) >= 8 else ''
                meta[row[0]] = (variants[0] if variants else row[0],
                                row[3].strip() or '?', title)
    return meta


# ---- FJMS domain layer -> ~13 coarse color groups ----
# (index, EN, HE, color) — embedded in the JS as GROUPS
DOMAIN_GROUPS = [
    ('Bible', 'מקרא', '#4ea3ff'),
    ('Exegesis & Tafsir', 'פרשנות ותפסיר', '#2ec8e6'),
    ('Piyyut', 'פיוט', '#a06bff'),
    ('Liturgy', 'תפילה וברכות', '#ffd35e'),
    ('Poetry', 'שירה', '#ff5ed0'),
    ('Talmud & Midrash', 'תלמוד ומדרש', '#c98a4b'),
    ('Halakha', 'הלכה', '#8fe65e'),
    ('Documents & Letters', 'תעודות ומכתבים', '#e8e8e8'),
    ('Thought & Kabbalah', 'הגות וקבלה', '#ff7d5e'),
    ('Sciences & Medicine', 'מדעים ורפואה', '#5effd0'),
    ('Philology', 'בלשנות ומילונאות', '#9aa7ff'),
    ('Belles Lettres', 'סיפורת', '#d4ff5e'),
    ('Other / Unidentified', 'אחר', '#77808f'),
]
_G = {name: i for i, (name, _, _) in enumerate(DOMAIN_GROUPS)}
_KEYWORD_GROUPS = [
    ('tafsir', 'Exegesis & Tafsir'), ('exegesis', 'Exegesis & Tafsir'),
    ('piyyut', 'Piyyut'), ('piyut', 'Piyyut'),
    ('secular poetry', 'Poetry'),
    ('liturgy', 'Liturgy'), ('prayer', 'Liturgy'),
    ('bible', 'Bible'), ('masorah', 'Bible'), ('massorah', 'Bible'),
    ('halakh', 'Halakha'), ('responsa', 'Halakha'),
    ('talmud', 'Talmud & Midrash'), ('mishnah', 'Talmud & Midrash'),
    ('rabbinic', 'Talmud & Midrash'), ('midrash', 'Talmud & Midrash'),
    ('derashot', 'Talmud & Midrash'),
    ('letters', 'Documents & Letters'), ('document', 'Documents & Letters'),
    ('lists', 'Documents & Letters'),
    ('philosoph', 'Thought & Kabbalah'), ('theolog', 'Thought & Kabbalah'),
    ('kabbalah', 'Thought & Kabbalah'), ('kalam', 'Thought & Kabbalah'),
    ('ethical', 'Thought & Kabbalah'), ('polemic', 'Thought & Kabbalah'),
    ('science', 'Sciences & Medicine'), ('medicine', 'Sciences & Medicine'),
    ('astronomy', 'Sciences & Medicine'), ('occult', 'Sciences & Medicine'),
    ('predicting', 'Sciences & Medicine'),
    ('philolog', 'Philology'), ('glossar', 'Philology'),
    ('stories', 'Belles Lettres'), ('belles', 'Belles Lettres'),
]


def _group_of(domain, parent):
    txt = f"{domain or ''} {parent or ''}".casefold()
    for kw, grp in _KEYWORD_GROUPS:
        if kw in txt:
            return _G[grp]
    return _G['Other / Unidentified']


def load_domains():
    """sys_id -> (Counter[group_idx], Counter[hebrew domain label])."""
    con = sqlite3.connect(ROOT + r"\fist_data\fjms_enrichment.db")
    out = {}
    for alma, dom, dom_he, par, par_he in con.execute(
            "SELECT AlmaId, Domain, DomainHeb, ParentDomain, ParentDomainHeb "
            "FROM domains"):
        rec = out.get(alma)
        if rec is None:
            rec = out[alma] = (Counter(), Counter())
        rec[0][_group_of(dom, par)] += 1
        label = dom_he or dom
        if label and label not in ('לא מזוהה', 'Unidentified'):
            rec[1][label] += 1
    con.close()
    return out


def split_recursive(members, cont_adj, depth=0):
    """Louvain-split an oversized member set; returns list of member lists."""
    if len(members) <= SPLIT_AT or depth >= 3:
        return [members]
    g = nx.Graph()
    mset = set(members)
    g.add_nodes_from(members)
    for a in members:
        for b, w in cont_adj[a]:
            if b in mset and a < b:
                g.add_edge(a, b, weight=w)
    part = community_louvain.best_partition(g, random_state=42)
    groups = defaultdict(list)
    for s, c in part.items():
        groups[c].append(s)
    if len(groups) <= 1:
        return [members]
    out = []
    for grp in groups.values():
        out.extend(split_recursive(grp, cont_adj, depth + 1))
    return out


def main():
    t0 = time.time()
    meta = load_lib_meta()
    domains = load_domains()
    print(f"domain records for {len(domains):,} sys_ids "
          f"({time.time() - t0:.0f}s)", flush=True)
    con = sqlite3.connect(DB)
    rows = con.execute(f"""
        SELECT sys_a, sys_b, aligned_len, density, flank_class
        FROM {TABLE} WHERE dup_shelf = 0 AND dup_lines < 0.6
    """).fetchall()
    con.close()

    ms_pairs = defaultdict(lambda: [0, 0, 0, 0])  # n, best_len, cont, isl
    for sa, sb, alen, dens, fc in rows:
        key = (sa, sb) if sa < sb else (sb, sa)
        r = ms_pairs[key]
        r[0] += 1
        r[1] = max(r[1], alen)
        if fc in ('continuation', 'edge'):
            r[2] += 1
        elif fc == 'island':
            r[3] += 1

    # ---- continuation components ----
    cont_keys = [k for k, r in ms_pairs.items() if r[2] >= max(1, r[3])]
    ms_ids = sorted({s for k in cont_keys for s in k})
    idx = {s: i for i, s in enumerate(ms_ids)}
    ea = np.array([idx[a] for a, b in cont_keys])
    eb = np.array([idx[b] for a, b in cont_keys])
    m = coo_matrix((np.ones(len(ea)), (ea, eb)),
                   shape=(len(ms_ids), len(ms_ids)))
    _, labels = connected_components(m, directed=False)
    comp_members = defaultdict(list)
    for s in ms_ids:
        comp_members[int(labels[idx[s]])].append(s)

    cont_adj = defaultdict(list)
    for (a, b) in cont_keys:
        w = ms_pairs[(a, b)][0]
        cont_adj[a].append((b, w))
        cont_adj[b].append((a, w))

    # ---- decompose oversized components (Louvain) ----
    clusters = []   # list of (members, from_giant)
    for members in comp_members.values():
        if len(members) < MIN_CLUSTER:
            continue
        if len(members) > SPLIT_AT:
            for grp in split_recursive(members, cont_adj):
                if len(grp) >= MIN_CLUSTER:
                    clusters.append((grp, True))
        else:
            clusters.append((members, False))
    clusters.sort(key=lambda c: -len(c[0]))
    print(f"clusters after Louvain split: {len(clusters)} "
          f"({time.time() - t0:.0f}s)", flush=True)

    deg = Counter()
    for (a, b), r in ms_pairs.items():
        deg[a] += r[0]
        deg[b] += r[0]

    # ---- cluster records + membership map (single pass for edges) ----
    cluster_of = {}
    sampled_of = {}
    comp_records = []
    other_grp = _G['Other / Unidentified']

    def ms_group(s):
        rec = domains.get(s)
        if not rec or not rec[0]:
            return other_grp
        # prefer a specific group over Other when both present
        top = rec[0].most_common(2)
        if top[0][0] == other_grp and len(top) > 1:
            return top[1][0]
        return top[0][0]

    for ci, (members, from_giant) in enumerate(clusters):
        libs = Counter(meta.get(s, ('', '?', ''))[1] for s in members)
        titles = Counter(t for s in members
                         for t in [meta.get(s, ('', '?', ''))[2]] if t)
        grp_cnt = Counter(ms_group(s) for s in members)
        dom_labels = Counter()
        for s in members:
            rec = domains.get(s)
            if rec:
                dom_labels.update(rec[1])
        full_n = len(members)
        sampled = full_n > MEMBER_CAP
        keep = (sorted(members, key=lambda s: -deg[s])[:MEMBER_CAP]
                if sampled else members)
        loc = {s: i for i, s in enumerate(keep)}
        for s in members:
            cluster_of[s] = ci
        sampled_of[ci] = loc
        nodes = [[s] + list(meta.get(s, (s, '?', ''))) + [deg[s], ms_group(s)]
                 for s in keep]
        dgrp = grp_cnt.most_common(1)[0][0]
        if dgrp == other_grp and len(grp_cnt) > 1:
            nd = [g for g, _ in grp_cnt.most_common(2) if g != other_grp]
            if nd and grp_cnt[nd[0]] >= max(2, 0.25 * full_n):
                dgrp = nd[0]
        comp_records.append({
            'id': ci, 'n': full_n, 'sampled': sampled, 'giant': from_giant,
            'libs': dict(libs.most_common(5)),
            'titles': [[t, c] for t, c in titles.most_common(4)],
            'doms': [[t, c] for t, c in dom_labels.most_common(4)],
            'dgrp': dgrp,
            'nodes': nodes, 'edges': [],
        })

    links_agg = defaultdict(lambda: [0, 0])  # (ci,cj) -> [cont_w, isl_w]
    for (a, b), r in ms_pairs.items():
        ca, cb = cluster_of.get(a), cluster_of.get(b)
        if ca is None or cb is None:
            continue
        if ca == cb:
            loc = sampled_of[ca]
            if a in loc and b in loc:
                comp_records[ca]['edges'].append(
                    [loc[a], loc[b], r[0], r[1], r[2], r[3]])
        else:
            key = (ca, cb) if ca < cb else (cb, ca)
            links_agg[key][0] += r[2]
            links_agg[key][1] += r[3]
    for rec in comp_records:
        rec['edges'].sort(key=lambda e: -e[2])
        del rec['edges'][EDGE_CAP:]
    links = sorted(([ci, cj, w[0], w[1]]
                    for (ci, cj), w in links_agg.items()),
                   key=lambda x: -(x[2] + x[3]))[:LINK_CAP]
    print(f"links: {len(links_agg)} kept {len(links)} "
          f"({time.time() - t0:.0f}s)", flush=True)

    # ---- overview layout: force-directed over cluster circles ----
    n = len(comp_records)
    R = np.array([min(6 + 3.0 * math.sqrt(c['n']), 200)
                  for c in comp_records])
    rng = np.random.default_rng(42)
    order = np.argsort(-R)
    P = np.zeros((n, 2))
    # spiral init, big first
    placed = []
    for oi in order:
        r = R[oi]
        if not placed:
            placed.append((0.0, 0.0, r))
            P[oi] = (0, 0)
            continue
        ang, rad = float(rng.uniform(0, 6.28)), placed[0][2] + r + 8
        while True:
            x, y = rad * math.cos(ang), rad * math.sin(ang)
            if all((x - px) ** 2 + (y - py) ** 2 >= (r + pr + 4) ** 2
                   for px, py, pr in placed):
                placed.append((x, y, r))
                P[oi] = (x, y)
                break
            ang += 0.37
            rad += 1.0
    la = np.array([l[0] for l in links], dtype=int)
    lb = np.array([l[1] for l in links], dtype=int)
    lw = np.log2(1 + np.array([l[2] + l[3] for l in links], float))
    for it in range(240):
        d = P[:, None, :] - P[None, :, :]
        dist = np.sqrt((d ** 2).sum(-1)) + 1e-6
        # circle-aware repulsion (stronger inside touching distance)
        touch = R[:, None] + R[None, :] + 10
        f = np.where(dist < touch * 2.2, (touch * 2.2 - dist) * 0.05, 0.0)
        np.fill_diagonal(f, 0)
        F = (d / dist[..., None] * f[..., None]).sum(1)
        # link attraction
        if len(la):
            dv = P[lb] - P[la]
            dd = np.sqrt((dv ** 2).sum(-1)) + 1e-6
            want = (R[la] + R[lb]) * 1.4 + 40
            pull = ((dd - want) * 0.004 * (0.5 + lw / 6))[:, None] * dv / \
                dd[:, None]
            np.add.at(F, la, pull)
            np.add.at(F, lb, -pull)
        # weak centering
        F -= P * 0.001
        P += np.clip(F, -18, 18)
    # final overlap resolution
    for it in range(60):
        d = P[:, None, :] - P[None, :, :]
        dist = np.sqrt((d ** 2).sum(-1)) + 1e-6
        touch = R[:, None] + R[None, :] + 6
        over = np.where(dist < touch, (touch - dist) * 0.5, 0.0)
        np.fill_diagonal(over, 0)
        P += (d / dist[..., None] * over[..., None]).sum(1) * 0.5
    for rec, (x, y), r in zip(comp_records, P, R):
        rec['x'], rec['y'], rec['r'] = round(float(x), 1), \
            round(float(y), 1), round(float(r), 1)
    print(f"layout done ({time.time() - t0:.0f}s)", flush=True)

    n_ms = sum(rec['n'] for rec in comp_records)
    data = json.dumps({'comps': comp_records, 'links': links, 'tag': TAG,
                       'n_ms': n_ms, 'n_edges': len(ms_pairs),
                       'groups': DOMAIN_GROUPS},
                      ensure_ascii=False, separators=(',', ':'))

    html_doc = """<!DOCTYPE html><html lang='he'><head><meta charset='utf-8'>
<title>Text-reuse graph — __TAG__</title>
<style>
 html,body{margin:0;height:100%;overflow:hidden;font-family:Segoe UI,Arial,sans-serif;background:#111420;color:#eee}
 #bar{position:fixed;top:0;left:0;right:0;background:#1b2030ee;padding:8px 14px;z-index:5;display:flex;gap:14px;align-items:center;border-bottom:1px solid #333}
 #bar b{color:#8fd3ff}
 #search{background:#252b3d;border:1px solid #444;color:#eee;border-radius:6px;padding:4px 10px;width:260px}
 #back{display:none;background:#2c5c8f;border:none;color:#fff;border-radius:6px;padding:4px 14px;cursor:pointer}
 #info{font-size:12.5px;color:#aab}
 #tip{position:fixed;display:none;background:#000d;border:1px solid #555;border-radius:8px;padding:8px 12px;font-size:13px;max-width:380px;pointer-events:none;z-index:9;direction:rtl;text-align:right}
 #tip .en{direction:ltr;text-align:left;color:#9ab;font-size:11.5px}
 canvas{display:block}
 #legend{position:fixed;bottom:10px;left:12px;font-size:12px;color:#99a;z-index:5;background:#1b2030cc;padding:6px 10px;border-radius:8px}
 .sw{display:inline-block;width:10px;height:10px;border-radius:5px;margin:0 3px -1px 8px}
 .lw{display:inline-block;width:16px;height:3px;border-radius:2px;margin:0 3px 2px 8px}
</style></head><body>
<div id='bar'>
 <b>Text-reuse graph</b>
 <button id='back'>&#8592; overview</button>
 <button id='colormode' style='background:#3d3f6b;border:none;color:#fff;border-radius:6px;padding:4px 14px;cursor:pointer'>color: domain</button>
 <input id='search' placeholder='filter clusters by title or domain (e.g. רמבם, פיוט)'>
 <span id='info'></span>
</div>
<div id='tip'></div>
<div id='legend'></div>
<canvas id='cv'></canvas>
<script>
const DATA = __DATA__;
const LIBCOL = {CUL:'#4ea3ff',RNL:'#ff7d5e',JTS:'#ffd35e',Oxford:'#9d7bff',BL:'#5effa3',
                Manchester:'#ff5ed0',AIU:'#5ef2ff',Mosseri:'#c2ff5e','?':'#8892a8'};
const OTHER='#8892a8';
const cv=document.getElementById('cv'),ctx=cv.getContext('2d');
const tip=document.getElementById('tip'),info=document.getElementById('info');
const backBtn=document.getElementById('back'),search=document.getElementById('search');
let W,H,DPR;
function resize(){DPR=devicePixelRatio||1;W=innerWidth;H=innerHeight;
 cv.width=W*DPR;cv.height=H*DPR;cv.style.width=W+'px';cv.style.height=H+'px';draw();}
addEventListener('resize',resize);
function libColor(l){return LIBCOL[l]||OTHER}
function domLib(libs){let b=null,bc=-1;for(const k in libs)if(libs[k]>bc){bc=libs[k];b=k}return b}
const GROUPS=DATA.groups; // [en, he, color]
let colorBy='domain';     // 'domain' | 'library'
function compColor(c){return colorBy==='domain'?GROUPS[c.dgrp][2]:libColor(domLib(c.libs))}
function nodeColor(nd){return colorBy==='domain'?GROUPS[nd[5]][2]:libColor(nd[2])}

let mode='over';
let cam={x:0,y:0,k:1};
let sim=null,hover=null,filterQ='';

function toScreen(x,y){return[(x-cam.x)*cam.k+W/2,(y-cam.y)*cam.k+H/2]}
function toWorld(sx,sy){return[(sx-W/2)/cam.k+cam.x,(sy-H/2)/cam.k+cam.y]}
function fitOverview(){
 let minx=1e9,maxx=-1e9,miny=1e9,maxy=-1e9;
 for(const c of DATA.comps){minx=Math.min(minx,c.x-c.r);maxx=Math.max(maxx,c.x+c.r);
  miny=Math.min(miny,c.y-c.r);maxy=Math.max(maxy,c.y+c.r);}
 cam.x=(minx+maxx)/2;cam.y=(miny+maxy)/2;
 cam.k=Math.min(W/(maxx-minx+100),(H-70)/(maxy-miny+100));
}

// ---------- force sim (drill) ----------
function startSim(comp){
 const n=comp.nodes.length;
 const pos=new Float32Array(2*n),vel=new Float32Array(2*n);
 for(let i=0;i<n;i++){const a=i*2.399963;const r=16*Math.sqrt(i);
  pos[2*i]=r*Math.cos(a);pos[2*i+1]=r*Math.sin(a);}
 sim={comp,pos,vel,tick:0,running:true};
 cam={x:0,y:0,k:Math.min(W,H)/(34*Math.sqrt(n)+260)};
 requestAnimationFrame(step);
}
function step(){
 if(!sim||!sim.running)return;
 const {comp,pos,vel}=sim;const n=comp.nodes.length;
 const REP=2200,SPR=0.06,DAMP=0.85,LEN=64;
 for(let it=0;it<3;it++){
  for(let i=0;i<n;i++){
   let fx=0,fy=0;
   for(let j=0;j<n;j++){if(i===j)continue;
    let dx=pos[2*i]-pos[2*j],dy=pos[2*i+1]-pos[2*j+1];
    let d2=dx*dx+dy*dy+40;const f=REP/d2;
    fx+=dx*f/Math.sqrt(d2);fy+=dy*f/Math.sqrt(d2);}
   fx-=pos[2*i]*0.004;fy-=pos[2*i+1]*0.004;
   vel[2*i]=(vel[2*i]+fx)*DAMP;vel[2*i+1]=(vel[2*i+1]+fy)*DAMP;}
  for(const e of comp.edges){
   const i=e[0],j=e[1];
   let dx=pos[2*j]-pos[2*i],dy=pos[2*j+1]-pos[2*i+1];
   const d=Math.sqrt(dx*dx+dy*dy)+1e-3;
   const w=Math.min(1,Math.log2(1+e[2])/3);
   const f=SPR*(d-LEN)*(0.4+w)/d;
   vel[2*i]+=dx*f;vel[2*i+1]+=dy*f;vel[2*j]-=dx*f;vel[2*j+1]-=dy*f;}
  for(let i=0;i<2*n;i++)pos[i]+=vel[i];
  sim.tick++;}
 draw();
 if(sim.tick<380)requestAnimationFrame(step);else sim.running=false;
}

// ---------- drawing ----------
function draw(){
 ctx.setTransform(DPR,0,0,DPR,0,0);
 ctx.clearRect(0,0,W,H);
 if(mode==='over')drawOver();else drawDrill();
}
function linkColor(cont,isl){return cont>=isl?'#39d98a':'#ff9d4d'}
function drawOver(){
 const q=filterQ.trim();
 // links first (the web)
 for(const L of DATA.links){
  const a=DATA.comps[L[0]],b=DATA.comps[L[1]];
  const[x1,y1]=toScreen(a.x,a.y),[x2,y2]=toScreen(b.x,b.y);
  if(Math.max(x1,x2)<0||Math.min(x1,x2)>W||Math.max(y1,y2)<0||Math.min(y1,y2)>H)continue;
  const w=L[2]+L[3];
  ctx.beginPath();ctx.moveTo(x1,y1);ctx.lineTo(x2,y2);
  ctx.strokeStyle=linkColor(L[2],L[3])+'40';
  ctx.lineWidth=Math.min(4,0.4+Math.log2(1+w)*0.45);
  ctx.stroke();}
 for(const c of DATA.comps){
  const[sx,sy]=toScreen(c.x,c.y);const r=Math.max(c.r*cam.k,1.4);
  if(sx<-r||sy<-r||sx>W+r||sy>H+r)continue;
  let match=true;
  if(q)match=c.titles.some(t=>t[0].includes(q))
        ||c.doms.some(t=>t[0].includes(q))
        ||GROUPS[c.dgrp][0].toLowerCase().includes(q.toLowerCase())
        ||GROUPS[c.dgrp][1].includes(q);
  ctx.beginPath();ctx.arc(sx,sy,r,0,7);
  ctx.fillStyle=compColor(c)+(match?'cc':'1e');
  ctx.fill();
  if(c.giant&&match){ctx.strokeStyle='#ffffff55';ctx.lineWidth=1;ctx.stroke();}
  if(c===hover){ctx.strokeStyle='#fff';ctx.lineWidth=2;ctx.stroke();}
  if(r>30&&match){
   ctx.fillStyle='#fff';ctx.font='bold 12px Segoe UI';ctx.textAlign='center';
   let t=(c.titles[0]&&c.titles[0][0])?c.titles[0][0].replace(/[.;].*$/,'').slice(0,22):'';
   if(!t)t=(c.doms[0]&&c.doms[0][0])?c.doms[0][0].slice(0,22):GROUPS[c.dgrp][1];
   ctx.fillText(t||c.n+' MSS',sx,sy-2);
   ctx.fillStyle='#ffffffaa';ctx.font='10.5px Segoe UI';
   ctx.fillText(c.n+' MSS',sx,sy+13);}
 }
 info.textContent=`${DATA.comps.length} same-unit clusters · ${DATA.n_ms.toLocaleString()} manuscripts · lines = shared-text links between clusters (green=same-work bridge, orange=quotation) · click a circle`;
}
function edgeColor(e){
 const cont=e[4],isl=e[5];
 if(cont>=isl*2)return'#39d98a';
 if(isl>=cont*2)return'#ff9d4d';
 return'#b9c26b';
}
function drawDrill(){
 const {comp,pos}=sim;
 const maxDraw=Math.min(comp.edges.length,4000);
 for(let k=0;k<maxDraw;k++){
  const e=comp.edges[k];
  const[x1,y1]=toScreen(pos[2*e[0]],pos[2*e[0]+1]);
  const[x2,y2]=toScreen(pos[2*e[1]],pos[2*e[1]+1]);
  ctx.beginPath();ctx.moveTo(x1,y1);ctx.lineTo(x2,y2);
  ctx.strokeStyle=edgeColor(e)+'88';
  ctx.lineWidth=Math.min(6,0.6+Math.log2(1+e[2]));
  ctx.stroke();}
 comp.nodes.forEach((nd,i)=>{
  const[sx,sy]=toScreen(pos[2*i],pos[2*i+1]);
  const r=(4+2.4*Math.sqrt(Math.min(nd[4],120)))*Math.min(cam.k,1.4);
  ctx.beginPath();ctx.arc(sx,sy,Math.max(r,2),0,7);
  ctx.fillStyle=nodeColor(nd);ctx.fill();
  if(i===hover){ctx.strokeStyle='#fff';ctx.lineWidth=2;ctx.stroke();}});
 info.textContent=`cluster of ${comp.n.toLocaleString()} MSS`+
  (comp.sampled?` — showing top-${comp.nodes.length} by connectivity`:'')+
  (comp.giant?' · Louvain subdivision of the giant connected component (pre Track-1 masking)':'')+
  ` · ${comp.edges.length.toLocaleString()} edges · click node → browse`;
}
function legend(){
 let h='';
 if(colorBy==='domain'){
  for(const g of GROUPS)
   h+=`<span class='sw' style='background:${g[2]}'></span>${g[1]}`;
 }else{
  for(const k of['CUL','RNL','JTS','Oxford','BL'])
   h+=`<span class='sw' style='background:${LIBCOL[k]}'></span>${k}`;
  h+=`<span class='sw' style='background:${OTHER}'></span>other`;
 }
 h+=`<span class='lw' style='background:#39d98a'></span>same-work`;
 h+=`<span class='lw' style='background:#ff9d4d'></span>quotation`;
 document.getElementById('legend').innerHTML=h;
}

// ---------- interaction ----------
function pick(mx,my){
 if(mode==='over'){
  for(let i=DATA.comps.length-1;i>=0;i--){const c=DATA.comps[i];
   const[sx,sy]=toScreen(c.x,c.y);const r=Math.max(c.r*cam.k,4);
   if((mx-sx)**2+(my-sy)**2<=r*r)return c;}
  return null;}
 const {comp,pos}=sim;
 for(let i=comp.nodes.length-1;i>=0;i--){
  const[sx,sy]=toScreen(pos[2*i],pos[2*i+1]);
  const r=Math.max((4+2.4*Math.sqrt(Math.min(comp.nodes[i][4],120)))*Math.min(cam.k,1.4),2)+2;
  if((mx-sx)**2+(my-sy)**2<=r*r)return i;}
 return null;
}
let dragging=false,dx0=0,dy0=0,moved=false;
cv.addEventListener('mousedown',e=>{dragging=true;moved=false;dx0=e.clientX;dy0=e.clientY;});
addEventListener('mouseup',()=>dragging=false);
cv.addEventListener('mousemove',e=>{
 if(dragging){const ddx=e.clientX-dx0,ddy=e.clientY-dy0;
  if(Math.abs(ddx)+Math.abs(ddy)>3)moved=true;
  cam.x-=ddx/cam.k;cam.y-=ddy/cam.k;dx0=e.clientX;dy0=e.clientY;draw();return;}
 const p=pick(e.clientX,e.clientY);
 if(p!==hover){hover=p;draw();}
 if(p!=null){
  tip.style.display='block';
  tip.style.left=Math.min(e.clientX+16,W-400)+'px';
  tip.style.top=(e.clientY+16)+'px';
  if(mode==='over'){
   const t=p.titles.map(x=>`${x[0]} (${x[1]})`).join(' · ')||'ללא כותרת קטלוגית';
   const d=p.doms.map(x=>`${x[0]} (${x[1]})`).join(' · ');
   tip.innerHTML=`<b>${p.n} כתבי יד · ${GROUPS[p.dgrp][1]}</b>${p.giant?' <span style="color:#f90">· תת-קבוצה מהרכיב הקשיר הגדול</span>':''}<br>${t}${d?`<br><span style='color:#8fd3ff'>${d}</span>`:''}<div class='en'>libraries: ${JSON.stringify(p.libs)}</div>`;
  }else{
   const nd=sim.comp.nodes[p];
   tip.innerHTML=`<b>${nd[1]}</b> · ${GROUPS[nd[5]][1]}<br>${nd[3]||'ללא כותרת'}<div class='en'>${nd[2]} · ${nd[4]} page-pair links · click to open</div>`;
  }
 }else tip.style.display='none';
});
cv.addEventListener('wheel',e=>{e.preventDefault();
 const f=Math.exp(-e.deltaY*0.0012);
 const[wx,wy]=toWorld(e.clientX,e.clientY);
 cam.k*=f;cam.x=wx-(e.clientX-W/2)/cam.k;cam.y=wy-(e.clientY-H/2)/cam.k;draw();},{passive:false});
cv.addEventListener('click',e=>{
 if(moved)return;
 const p=pick(e.clientX,e.clientY);
 if(p==null)return;
 if(mode==='over'){mode='drill';backBtn.style.display='inline-block';
  hover=null;startSim(p);}
 else{const nd=sim.comp.nodes[p];
  window.open('https://genizahsearch.com/browse?sys_id='+nd[0],'_blank');}
});
backBtn.onclick=()=>{mode='over';sim=null;hover=null;
 backBtn.style.display='none';fitOverview();draw();};
search.addEventListener('input',()=>{filterQ=search.value;if(mode==='over')draw();});
document.getElementById('colormode').onclick=function(){
 colorBy=colorBy==='domain'?'library':'domain';
 this.textContent='color: '+colorBy;legend();draw();};

resize();fitOverview();legend();draw();
</script></body></html>"""
    html_doc = html_doc.replace('__TAG__', TAG).replace('__DATA__', data)
    open(OUT, 'w', encoding='utf-8').write(html_doc)
    kb = len(html_doc.encode('utf-8')) // 1024
    print(f"wrote {OUT} ({kb} KB, {len(comp_records)} clusters, "
          f"{n_ms:,} MSS, {len(links)} links)")


if __name__ == '__main__':
    main()
