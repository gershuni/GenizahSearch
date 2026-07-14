# -*- coding: utf-8 -*-
"""MAPV2-15l — the "already-identified" gate.

Hillel: catalog-silence must consult ALL identification sources, or a fragment
that Friedberg / a printed catalog / FGP / PGP already names surfaces as a false
"discovery". This gate sweeps the discovery-scored candidates and DEMOTES any
that an existing source already names (discovery -> known). Sources only ever
demote; never the reverse.

Sources (all keyed by sys_id == AlmaId; read-only):
  bib          Friedberg bibliography      -> reuse bib_gate.BibGate
               (known_bib/published_full = NAMED; known_bib_genre/partial/
                mentions = studied-only, softer — genre editions don't demote a
                specific find, per Hillel).
  catalog_refs printed catalogs (Neubauer-Cowley, Baker-Polliack, Davis-
               Outhwaite …) -> entry token-matches the work = NAMED; else
               presence = studied.
  fgp          FGP transcriptions          -> title_he/author_he token-matches
               the work = NAMED; else a transcription exists = studied.
  pgp          Princeton Geniza Project    -> a document with a description /
               transcription = NAMED (PGP documents are identified, esp.
               documentary letters/deeds); bare fragment link = studied.
  (Oxford oxford_full_db.json is shelfmark-keyed, 225 recs; Oxford is already
   covered by catalog_refs' Neubauer-Cowley, so it is deferred here.)

NAMED -> bucket2='known', disc_score2=0. studied-only -> disc_score2 halved.

Out: data/discovery_scored_gated.jsonl + results/discovery_identified_gate_report.md
"""
import json
import os
import sqlite3
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bib_gate import BibGate, WEAK, heb_tokens, _base, _variants

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
FJMS = r"C:\Genizahsearch\fist_data\fjms_enrichment.db"
FGP = r"C:\Genizahsearch\fgp_data\fgp_transcriptions.db"
PGP = r"C:\Genizahsearch\pgp_data\pgp.db"
IN = PROBE + r"\data\discovery_scored.jsonl"
OUT = PROBE + r"\data\discovery_scored_gated.jsonl"
MD = PROBE + r"\results\discovery_identified_gate_report.md"


def _ro(p):
    return sqlite3.connect('file:' + p.replace('\\', '/') + '?mode=ro', uri=True)


def name_match(work_title, work_author, texts):
    """True if the work title/author strong-token-matches any of `texts`
    (HTR-tolerant via bib_gate variants; >=2 distinct matched tokens, >=1
    non-WEAK)."""
    claim = heb_tokens(f"{work_author or ''} {work_title or ''}")
    if not claim:
        return False
    forms = set()
    for t in texts:
        for tk in heb_tokens(t):
            forms |= _variants(tk)
    matched, strong = set(), 0
    for t in claim:
        b = _base(t)
        if _variants(t) & forms and b not in matched:
            matched.add(b)
            if b not in WEAK:
                strong += 1
    return len(matched) >= 2 and strong >= 1


def load_catalog_refs():
    con = _ro(FJMS)
    out = defaultdict(list)
    for aid, ent, tit in con.execute(
            "SELECT AlmaId, CatalogEntry, CatalogTitle FROM catalog_refs"):
        out[str(aid)].append(f"{ent or ''} {tit or ''}")
    con.close()
    return out


def load_fgp():
    con = _ro(FGP)
    out = defaultdict(list)
    for sid, th, ah, te in con.execute(
            "SELECT sys_id, title_he, author_he, title_en FROM fgp_transcriptions"):
        if sid:
            out[str(sid)].append((th or '', ah or '', te or ''))
    con.close()
    return out


def load_pgp():
    con = _ro(PGP)
    docinfo = {}
    for pid, desc, tr, ht, dt in con.execute(
            "SELECT pgpid, description, transcription, has_transcription, "
            "document_type FROM documents"):
        docinfo[pid] = (bool((desc or '').strip()),
                        bool((tr or '').strip()) or bool(ht), dt or '')
    out = {}
    for sid, did in con.execute(
            "SELECT sys_id, document_id FROM document_fragments"):
        if not sid:
            continue
        info = docinfo.get(did)
        if info:
            # keep the strongest doc for this sys_id
            prev = out.get(str(sid))
            score = (info[0] or info[1])
            if prev is None or (score and not (prev[0] or prev[1])):
                out[str(sid)] = info
    con.close()
    return out


def main():
    rows = [json.loads(l) for l in open(IN, encoding='utf-8')]
    disc = [r for r in rows if r.get('bucket') == 'discovery']
    print(f"rows {len(rows)}; discovery-bucket {len(disc)}", flush=True)

    bg = BibGate()
    crefs = load_catalog_refs()
    fgp = load_fgp()
    pgp = load_pgp()
    print(f"sources loaded (catalog_refs {len(crefs)}, fgp {len(fgp)}, "
          f"pgp {len(pgp)})", flush=True)

    named_by = Counter()
    studied_by = Counter()
    for r in rows:
        r['identified_by'] = []
        r['studied_by'] = []
        r['bucket2'] = r.get('bucket')
        r['disc_score2'] = r.get('disc_score', 0.0)
        if r.get('bucket') != 'discovery':
            continue
        sid = str(r['sys_id'])
        ti, au = r.get('title'), r.get('author')
        idb, stb = [], []
        # 1. Friedberg bib
        cls, _ev = bg.classify(sid, f"{au or ''} — {ti or ''}",
                               author=au, title=ti)
        if cls in ('known_bib', 'published_full'):
            idb.append(f"bib:{cls}")
        elif cls in ('known_bib_genre', 'bib_partial', 'bib_mentions'):
            stb.append(f"bib:{cls}")
        # 2. printed catalogs
        cr = crefs.get(sid)
        if cr:
            if name_match(ti, au, cr):
                idb.append('catalog_ref')
            else:
                stb.append('catalog_ref')
        # 3. FGP
        fg = fgp.get(sid)
        if fg:
            texts = [x for tup in fg for x in tup]
            if name_match(ti, au, texts):
                idb.append('fgp')
            else:
                stb.append('fgp')
        # 4. PGP
        pg = pgp.get(sid)
        if pg:
            has_desc, has_tr, dt = pg
            if has_desc or has_tr:
                idb.append(f"pgp:{dt or 'doc'}")
            else:
                stb.append('pgp')
        r['identified_by'], r['studied_by'] = idb, stb
        for s in idb:
            named_by[s.split(':')[0]] += 1
        for s in stb:
            studied_by[s.split(':')[0]] += 1
        if idb:
            r['bucket2'] = 'known'
            r['disc_score2'] = 0.0
        elif stb:
            r['disc_score2'] = round(r['disc_score'] * 0.5, 4)

    with open(OUT, 'w', encoding='utf-8') as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')

    # ---- report ----
    def surv(score_key, th):
        return [r for r in rows if r.get('bucket2') == 'discovery'
                and r.get(score_key, 0) >= th]
    before_hi = [r for r in disc if r['disc_score'] >= 0.3]
    before_cr = [r for r in disc if r['disc_score'] >= 0.5]
    after_hi = surv('disc_score2', 0.3)
    after_cr = surv('disc_score2', 0.5)
    n_named = sum(1 for r in disc if r['identified_by'])
    n_studied = sum(1 for r in disc if r['studied_by'] and not r['identified_by'])
    n_clean = sum(1 for r in disc if not r['identified_by'] and not r['studied_by'])

    L = ["# Already-identified gate (MAPV2-15l)", "",
         f"- discovery-bucket candidates: {len(disc)}",
         f"- NAMED by an existing source (-> known): **{n_named}** "
         f"({100*n_named//max(1,len(disc))}%)",
         f"- studied-only (score halved, kept): {n_studied}",
         f"- catalog-silent across ALL sources (genuine): **{n_clean}** "
         f"({100*n_clean//max(1,len(disc))}%)", "",
         "## NAMED by source (a candidate can hit several)", ""]
    for s, n in named_by.most_common():
        L.append(f"- {s}: {n}")
    L += ["", "## studied-only presence by source", ""]
    for s, n in studied_by.most_common():
        L.append(f"- {s}: {n}")
    L += ["", "## surviving genuine discovery — before vs after the gate", "",
          f"- score >= 0.3: {len(before_hi)} -> **{len(after_hi)}**",
          f"- score >= 0.5 (cream): {len(before_cr)} -> **{len(after_cr)}**", ""]
    after_hi.sort(key=lambda r: -r['disc_score2'])
    L += ["## top 30 surviving discoveries (score | letters | work | ms | wit)", ""]
    for r in after_hi[:30]:
        L.append(f"- `{r['disc_score2']:.2f}` {r['matched_letters']}L  "
                 f"{(r['title'] or '')[:40]} · {r['sys_id']} ({r['work_nms']} wit)")
    byw = defaultdict(list)
    for r in after_hi:
        byw[(r['work_id'], r['title'])].append(r)
    L += ["", "## surviving discovery grouped by work (top 20)", ""]
    for (wid, ti), rs in sorted(byw.items(), key=lambda x: -len(x[1]))[:20]:
        L.append(f"- **{(ti or wid)[:44]}** — {len(rs)} fragments "
                 f"(best {max(x['disc_score2'] for x in rs):.2f})")
    # notable demotions
    dem_pgp = [r for r in disc if any(s.startswith('pgp') for s in r['identified_by'])][:8]
    dem_bib = [r for r in disc if any(s.startswith('bib') for s in r['identified_by'])][:8]
    L += ["", "## notable demotions", "", "PGP (documentary):"]
    for r in dem_pgp:
        L.append(f"- {(r['title'] or '')[:44]} · {r['sys_id']}")
    L += ["", "Friedberg bib:"]
    for r in dem_bib:
        L.append(f"- {(r['title'] or '')[:44]} · {r['sys_id']}")
    open(MD, 'w', encoding='utf-8').write('\n'.join(L) + '\n')
    print('\n'.join(L[:40]))
    print(f"\nwrote {OUT} + {MD}")


if __name__ == '__main__':
    main()
