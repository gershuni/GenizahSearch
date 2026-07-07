# -*- coding: utf-8 -*-
"""Interactive text-reuse graph (self-contained HTML, canvas, no deps).

Usage: python build_reuse_graph.py [db_path] [tag]
Output: review/rehearsal_<tag>_graph.html

Two levels:
- OVERVIEW: every connected component (>=2 MSS) as a packed circle,
  radius ~ sqrt(#MSS), color = dominant library. Hover = titles summary,
  click = drill in. Search box filters by catalog title.
- DRILL-IN: live force-directed MS graph of the cluster. Node size ~ number
  of accepted page pairs touching the MS; edge width ~ page-pair count;
  edge color: continuation (green) / island (orange) / mixed. Click a node
  -> genizahsearch.com browse. Giant components are sampled to the
  top-degree MEMBER_CAP subgraph (noted in the header).
"""
import csv
import json
import math
import sqlite3
import sys
from collections import Counter, defaultdict

import numpy as np
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

ROOT = r"C:\Genizahsearch"
DB = sys.argv[1] if len(sys.argv) > 1 else \
    ROOT + r"\same_work_spike\probe\data\rehearsal.db"
TAG = sys.argv[2] if len(sys.argv) > 2 else "100k"
OUT = ROOT + rf"\same_work_spike\probe\review\rehearsal_{TAG}_graph.html"
MEMBER_CAP = 300


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


def main():
    meta = load_lib_meta()
    con = sqlite3.connect(DB)
    rows = con.execute("""
        SELECT sys_a, sys_b, aligned_len, density, flank_class
        FROM accepted_pairs WHERE dup_shelf = 0 AND dup_lines < 0.6
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

    # cluster on the CONTINUATION (same-unit) layer only — island edges are
    # quotation relationships and would bridge unrelated works into one blob
    # (the all-layer giant swallows e.g. the 51-MS grammar cluster).
    # Drill-in still displays island edges WITHIN a cluster (orange).
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
    comps = sorted((c for c in comp_members.values() if len(c) >= 2),
                   key=len, reverse=True)

    deg = Counter()
    for (a, b), r in ms_pairs.items():
        deg[a] += r[0]
        deg[b] += r[0]

    comp_records = []
    for ci, members in enumerate(comps):
        libs = Counter(meta.get(s, ('', '?', ''))[1] for s in members)
        titles = Counter(t for s in members
                         for t in [meta.get(s, ('', '?', ''))[2]] if t)
        full_n = len(members)
        sampled = full_n > MEMBER_CAP
        if sampled:
            members = sorted(members, key=lambda s: -deg[s])[:MEMBER_CAP]
        mset = set(members)
        loc = {s: i for i, s in enumerate(members)}
        nodes = []
        for s in members:
            sm, lib, ti = meta.get(s, (s, '?', ''))
            nodes.append([s, sm, lib, ti, deg[s]])
        edges = []
        for (a, b), r in ms_pairs.items():
            if a in mset and b in mset:
                edges.append([loc[a], loc[b], r[0], r[1], r[2], r[3]])
        comp_records.append({
            'id': ci, 'n': full_n, 'sampled': sampled,
            'libs': dict(libs.most_common(5)),
            'titles': [[t, c] for t, c in titles.most_common(4)],
            'nodes': nodes, 'edges': edges,
        })

    # ---- circle packing on a spiral (deterministic, no deps) ----
    placed = []  # (x, y, r)
    for rec in comp_records:
        r = 6 + 3.2 * math.sqrt(rec['n'])
        r = min(r, 320)
        if not placed:
            rec['x'], rec['y'], rec['r'] = 0.0, 0.0, r
            placed.append((0.0, 0.0, r))
            continue
        ang, rad = 0.0, placed[0][2] + r + 8
        while True:
            x = rad * math.cos(ang)
            y = rad * math.sin(ang)
            if all((x - px) ** 2 + (y - py) ** 2 >= (r + pr + 6) ** 2
                   for px, py, pr in placed):
                rec['x'], rec['y'], rec['r'] = round(x, 1), round(y, 1), r
                placed.append((x, y, r))
                break
            ang += 0.35
            rad += 1.1

    n_ms = sum(rec['n'] for rec in comp_records)
    data = json.dumps({'comps': comp_records, 'tag': TAG,
                       'n_ms': n_ms, 'n_edges': len(ms_pairs)},
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
</style></head><body>
<div id='bar'>
 <b>Text-reuse graph</b>
 <button id='back'>&#8592; overview</button>
 <input id='search' placeholder='filter clusters by title (e.g. רמבם, פיוט)'>
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

// ---------- view state ----------
let mode='over';           // 'over' | 'drill'
let cam={x:0,y:0,k:1};     // world->screen: s=(w-cam)*k + center
let cur=null;              // drilled comp
let sim=null;              // force sim state
let hover=null;
let filterQ='';

function toScreen(x,y){return[(x-cam.x)*cam.k+W/2,(y-cam.y)*cam.k+H/2]}
function toWorld(sx,sy){return[(sx-W/2)/cam.k+cam.x,(sy-H/2)/cam.k+cam.y]}

function fitOverview(){
 let minx=1e9,maxx=-1e9,miny=1e9,maxy=-1e9;
 for(const c of DATA.comps){minx=Math.min(minx,c.x-c.r);maxx=Math.max(maxx,c.x+c.r);
  miny=Math.min(miny,c.y-c.r);maxy=Math.max(maxy,c.y+c.r);}
 cam.x=(minx+maxx)/2;cam.y=(miny+maxy)/2;
 cam.k=Math.min(W/(maxx-minx+80),(H-60)/(maxy-miny+80));
}

// ---------- force sim (drill) ----------
function startSim(comp){
 const n=comp.nodes.length;
 const pos=new Float32Array(2*n),vel=new Float32Array(2*n);
 for(let i=0;i<n;i++){const a=i*2.399963;const r=14*Math.sqrt(i);
  pos[2*i]=r*Math.cos(a);pos[2*i+1]=r*Math.sin(a);}
 sim={comp,pos,vel,tick:0,running:true};
 cam={x:0,y:0,k:Math.min(W,H)/(60*Math.sqrt(n)+240)*2.2};
 requestAnimationFrame(step);
}
function step(){
 if(!sim||!sim.running)return;
 const {comp,pos,vel}=sim;const n=comp.nodes.length;
 const REP=1800,SPR=0.06,DAMP=0.85,LEN=60;
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
 if(sim.tick<400)requestAnimationFrame(step);else sim.running=false;
}

// ---------- drawing ----------
function draw(){
 ctx.setTransform(DPR,0,0,DPR,0,0);
 ctx.clearRect(0,0,W,H);
 if(mode==='over')drawOver();else drawDrill();
}
function drawOver(){
 const q=filterQ.trim();
 for(const c of DATA.comps){
  const[sx,sy]=toScreen(c.x,c.y);const r=c.r*cam.k;
  if(sx<-r||sy<-r||sx>W+r||sy>H+r)continue;
  let match=true;
  if(q)match=c.titles.some(t=>t[0].includes(q));
  ctx.beginPath();ctx.arc(sx,sy,Math.max(r,1.2),0,7);
  ctx.fillStyle=libColor(domLib(c.libs))+(match?'cc':'22');
  ctx.fill();
  if(c===hover){ctx.strokeStyle='#fff';ctx.lineWidth=2;ctx.stroke();}
  if(r>26){ctx.fillStyle=match?'#fff':'#667';ctx.font='12px Segoe UI';
   ctx.textAlign='center';
   ctx.fillText(c.n+' MSS',sx,sy+4);}
 }
 info.textContent=`${DATA.comps.length} same-unit clusters (continuation layer) · ${DATA.n_ms.toLocaleString()} manuscripts · ${DATA.n_edges.toLocaleString()} MS-pair edges total · click a circle to open it`;
}
function edgeColor(e){
 const cont=e[4],isl=e[5];
 if(cont>=isl*2)return'#39d98a';
 if(isl>=cont*2)return'#ff9d4d';
 return'#b9c26b';
}
function drawDrill(){
 const {comp,pos}=sim;
 for(const e of comp.edges){
  const[x1,y1]=toScreen(pos[2*e[0]],pos[2*e[0]+1]);
  const[x2,y2]=toScreen(pos[2*e[1]],pos[2*e[1]+1]);
  ctx.beginPath();ctx.moveTo(x1,y1);ctx.lineTo(x2,y2);
  ctx.strokeStyle=edgeColor(e)+'99';
  ctx.lineWidth=Math.min(7,0.7+Math.log2(1+e[2]));
  ctx.stroke();}
 comp.nodes.forEach((nd,i)=>{
  const[sx,sy]=toScreen(pos[2*i],pos[2*i+1]);
  const r=4+2.4*Math.sqrt(Math.min(nd[4],120));
  ctx.beginPath();ctx.arc(sx,sy,r*Math.min(cam.k,1.4),0,7);
  ctx.fillStyle=libColor(nd[2]);ctx.fill();
  if(i===hover){ctx.strokeStyle='#fff';ctx.lineWidth=2;ctx.stroke();}});
 info.textContent=`cluster of ${comp.n.toLocaleString()} MSS`+
  (comp.sampled?` — showing top-${comp.nodes.length} by connectivity`:'')+
  ` · ${comp.edges.length.toLocaleString()} edges · green=continuation orange=island · click node → browse`;
}
function legend(){
 let h='node = manuscript circle ~ evidence; ';
 for(const k of['CUL','RNL','JTS','Oxford','BL'])
  h+=`<span class='sw' style='background:${LIBCOL[k]}'></span>${k}`;
 h+=`<span class='sw' style='background:${OTHER}'></span>other`;
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
  const r=(4+2.4*Math.sqrt(Math.min(comp.nodes[i][4],120)))*Math.min(cam.k,1.4)+2;
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
   tip.innerHTML=`<b>${p.n} כתבי יד</b><br>${t}<div class='en'>libraries: ${JSON.stringify(p.libs)}</div>`;
  }else{
   const nd=sim.comp.nodes[p];
   tip.innerHTML=`<b>${nd[1]}</b><br>${nd[3]||'ללא כותרת'}<div class='en'>${nd[2]} · ${nd[4]} page-pair links · click to open</div>`;
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
 if(mode==='over'){mode='drill';cur=p;backBtn.style.display='inline-block';
  hover=null;startSim(p);}
 else{const nd=sim.comp.nodes[p];
  window.open('https://genizahsearch.com/browse?sys_id='+nd[0],'_blank');}
});
backBtn.onclick=()=>{mode='over';cur=null;sim=null;hover=null;
 backBtn.style.display='none';fitOverview();draw();};
search.addEventListener('input',()=>{filterQ=search.value;if(mode==='over')draw();});

resize();fitOverview();legend();draw();
</script></body></html>"""
    html_doc = html_doc.replace('__TAG__', TAG).replace('__DATA__', data)
    open(OUT, 'w', encoding='utf-8').write(html_doc)
    kb = len(html_doc.encode('utf-8')) // 1024
    print(f"wrote {OUT} ({kb} KB, {len(comp_records)} clusters, "
          f"{n_ms:,} MSS)")


if __name__ == '__main__':
    main()
