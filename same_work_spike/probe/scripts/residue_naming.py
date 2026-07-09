# -*- coding: utf-8 -*-
"""Residue naming aid (SEED-029, scholar-in-the-loop).

Hillel's instruction (2026-07-09) on the residue-unidentified deck: these
most-copied UNIDENTIFIED clusters "link together probably many witnesses of the
same work, and can be judged by the more explicit title, or should be decided
if two competing titles exist (can be they are parallel works)."

Our reference corpus didn't identify them, but the manuscript CATALOGERS often
did: fjms_enrichment.db `catalog.GenizahTitleOrgTitle` is the cataloger's
assigned genizah-work title (set only when a codicological unit was identified).
So for each residue cluster we aggregate the catalogers' titles across the
cluster's witnesses and surface the modal / competing titles for Hillel to name.

CORRECTNESS: count DISTINCT WITNESS MANUSCRIPTS per title, never raw catalog
rows — one composite MS carries many catalog rows/titles and would otherwise
dominate (same distinct-vs-raw lesson as A2's DF cap + the new_sample
bib-demotion). A witness "carries title X" iff >=1 of its catalog rows has
GenizahTitleOrgTitle == X. LIMITATION: a passage unit points to specific PAGES,
but we aggregate at MANUSCRIPT level (folio->catalog-unit mapping is out of
scope), so a composite witness contributes every work its MS was identified as.
The modal title over the cluster is a strong same-work signal; Hillel judges.

Read-only; PRAGMA busy_timeout defensively; no writes; work_query_* untouched.
Uses the SAME residue predicate as build_discovery_review.py so the 40 units
align card-for-card with the reviewer's residue deck.

Usage: python -X utf8 -u residue_naming.py
Out:   results/residue_naming.md
"""
import csv
import sqlite3
import time
import unicodedata
from collections import Counter, defaultdict

sys_path = r"C:\Genizahsearch\same_work_spike\probe\scripts"
import sys
sys.path.insert(0, sys_path)
import build_frag1_review as B          # noqa: E402  (viewer_url / pnum)
from normalize import norm_stream       # noqa: E402

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
DB = PROBE + r"\data\fullcorpus.db"
FJMS = ROOT + r"\fist_data\fjms_enrichment.db"
LIBS = ROOT + r"\libraries.csv"
OUT = PROBE + r"\results\residue_naming.md"

UNITS_TABLE = "passage_units_accepted_pairs_canonmask"
MEMBERS_TABLE = "passage_unit_members_accepted_pairs_canonmask"
CONTINUUM_UNIT = 367274
N_RESIDUE = 40
CTX = 70

# a witness is "clearly named" if a single genizah-title covers this share of
# its NAMED witnesses; competing if top-2 both meaningful and no clear modal.
MODAL_SHARE = 0.50
MIN_NAMED = 3          # need >=3 named witnesses to call a modal "clear"
COMPETE_MIN = 3        # a competing title needs >=3 witnesses to count

# cataloger placeholders that are NOT a work name (drop like empty).
PLACEHOLDERS = {'לא מקודד', 'not coded', 'לא זוהה', 'לא ידוע', 'unidentified',
                '[טקסט]', '', '?'}

# liturgy/piyyut MARC-title keywords. A cluster where >=LITURGY_SHARE of
# witnesses carry one is very likely a liturgical-poetry AGGLOMERATION (many
# different poems chained by shared liturgical formulae), NOT one unknown work —
# route to canonical/liturgy masking + splitting, not to naming or discovery.
LITURGY_KW = ('פיוט', 'פיוטים', 'דיואן', 'סליחות', 'סליחה', 'קינה', 'קינות',
              'תפיל', 'תפל', 'שירה', 'שירים', 'בקשה', 'בקשות', 'פזמון',
              'קדושתא', 'יוצר', 'אזהרות', 'מעמד', 'זמר', 'הושענ', 'קרוב',
              'piyyut', 'liturg', 'selih', 'qinah', 'diwan')
LITURGY_SHARE = 0.50


def is_placeholder(s):
    return clean(s).lower() in PLACEHOLDERS


def is_liturgy_title(s):
    low = clean(s).lower()
    return any(k in low for k in LITURGY_KW)


def load_lib_title():
    """sys_id -> (shelfmark, library_code, libraries.csv title)."""
    meta = {}
    with open(LIBS, encoding='utf-8-sig', newline='') as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) >= 4 and row[0]:
                vs = [v.strip() for v in (row[2] or '').split('|') if v.strip()]
                title = row[7].strip() if len(row) >= 8 else ''
                meta[row[0]] = (vs[0] if vs else row[0],
                                row[3].strip() or '?', title)
    return meta


def clean(s):
    return unicodedata.normalize('NFC', (s or '').strip())


def spaced_snippet(page_text, s, e, pad=CTX):
    """A short spaced (…before…SPAN…after…) snippet from a member page."""
    nfc = unicodedata.normalize('NFC', page_text or '')
    _stream, offs = norm_stream(page_text or '')
    p = B.spaced_pieces(nfc, offs, int(s), int(e), pad=pad)
    if not p or not p.get('span'):
        return ''
    return f"…{p['before']}[[{p['span']}]]{p['after']}…".replace('\n', ' ')


def witness_titles(fj, sids, lib):
    """For each title field, sys_id -> set(values). Distinct-witness safe:
    a witness contributes each distinct value once regardless of row count.
    Placeholders ('לא מקודד' etc.) are dropped like empty. `marc` is the
    per-manuscript libraries.csv title (often the cleaner name for narrative
    works where GenizahTitleOrgTitle is a composite-MS artifact)."""
    org = defaultdict(set)          # GenizahTitleOrgTitle (cataloger work-id)
    eng = defaultdict(set)          # GenizahTitleEngTitle
    frame = defaultdict(set)        # TextualFrameHeb (genre)
    marc = defaultdict(set)         # libraries.csv title (co-equal signal)
    for sid in sids:
        mt = lib.get(sid, ('', '', ''))[2]
        if mt and not is_placeholder(mt):
            marc[sid].add(clean(mt))
    if not sids:
        return org, eng, frame, marc
    ph = ','.join('?' * len(sids))
    rows = fj.execute(
        f"SELECT AlmaId, GenizahTitleOrgTitle, GenizahTitleEngTitle, "
        f"TextualFrameHeb FROM catalog WHERE AlmaId IN ({ph})",
        list(sids)).fetchall()
    for aid, o, en, fr in rows:
        if clean(o) and not is_placeholder(o):
            org[aid].add(clean(o))
        if clean(en) and not is_placeholder(en):
            eng[aid].add(clean(en))
        if clean(fr):
            frame[aid].add(clean(fr))
    return org, eng, frame, marc


def distinct_witness_counter(field_map, sids):
    """title-string -> number of witnesses (in sids) carrying it."""
    c = Counter()
    for sid in sids:
        for v in field_map.get(sid, ()):
            c[v] += 1
    return c


def analyze(counter, n_named):
    """(verdict, top-list) for one title source, on distinct-witness counts."""
    top = counter.most_common()
    if n_named == 0:
        return 'unnamed', top
    if n_named < MIN_NAMED:
        return 'thin', top[:8]
    lead = top[0][1]
    competing = [t for t, c in top if c >= COMPETE_MIN]
    if lead / n_named >= MODAL_SHARE and (len(top) == 1 or top[1][1] < lead):
        return 'clear', top[:8]
    if len(competing) >= 2:
        return 'competing', top[:8]
    if lead / n_named >= MODAL_SHARE:
        return 'clear', top[:8]
    return 'thin', top[:8]


def build():
    t0 = time.time()
    fc = sqlite3.connect(DB)
    fc.execute("PRAGMA busy_timeout=120000")
    fj = sqlite3.connect(FJMS)
    fj.execute("PRAGMA busy_timeout=120000")
    lib = load_lib_title()

    rows = fc.execute(f"""
        SELECT unit, n_pages, n_ms, med_len
        FROM {UNITS_TABLE}
        WHERE labeled = 0 AND COALESCE(t1_label, '') = ''
              AND unit != {CONTINUUM_UNIT}
        ORDER BY n_ms DESC, unit ASC
        LIMIT {N_RESIDUE}""").fetchall()
    units = [r[0] for r in rows]
    umeta = {r[0]: {'n_pages': r[1], 'n_ms': r[2], 'med_len': r[3]}
             for r in rows}
    ph = ','.join('?' * len(units))
    mem = fc.execute(
        f"SELECT unit, page_id, sys_id, start, end FROM {MEMBERS_TABLE} "
        f"WHERE unit IN ({ph})", units).fetchall()
    members = defaultdict(list)
    for u, pid, sid, s, e in mem:
        members[u].append((pid, sid, s, e))

    blocks = []
    summary = {'clear': 0, 'competing': 0, 'thin': 0, 'unnamed': 0,
               '_liturgy': 0}
    for u in units:
        ms = members[u]
        sids = sorted({m[1] for m in ms})
        org, eng, frame, marc = witness_titles(fj, sids, lib)
        n_w = len(sids)

        org_c = distinct_witness_counter(org, sids)
        eng_c = distinct_witness_counter(eng, sids)
        frame_c = distinct_witness_counter(frame, sids)
        marc_c = distinct_witness_counter(marc, sids)
        n_named_org = len({sid for sid in sids if org.get(sid)})
        n_named_marc = len({sid for sid in sids if marc.get(sid)})
        n_named_any = len({sid for sid in sids if org.get(sid) or marc.get(sid)})

        # Verdict from the PRECISE signal (genizah-title: catalogers set it only
        # on identified units, so its absence = genuine unknown/discovery). The
        # MARC title is near-universal but noisy (whole-MS descriptive title,
        # composite pollution, wording variation) -> shown as co-evidence, and
        # only used to flag a hint when genizah-ID is thin/absent but MARC has a
        # clean modal.
        verdict, top_org = analyze(org_c, n_named_org)
        v_marc, top_marc = analyze(marc_c, n_named_marc)
        marc_hint = (top_marc[0][0] if verdict in ('unnamed', 'thin')
                     and v_marc == 'clear' and top_marc else '')
        # liturgy-agglomeration flag (share of witnesses with a liturgy MARC
        # title). Overrides the naming verdict for triage purposes.
        lit_sids = {sid for sid in sids
                    if any(is_liturgy_title(t) for t in marc.get(sid, ()))}
        liturgy_share = len(lit_sids) / n_w if n_w else 0.0
        is_liturgy = liturgy_share >= LITURGY_SHARE
        summary[verdict] += 1
        if is_liturgy:
            summary['_liturgy'] += 1

        # representative passage (longest span)
        rep = max(ms, key=lambda m: m[3] - m[2])
        rr = fc.execute("SELECT text FROM pages WHERE page_id=?",
                        (rep[0],)).fetchone()
        snippet = spaced_snippet(rr[0] if rr else '', rep[2], rep[3])

        # top sample witnesses (by span), with links + libraries.csv title
        best = {}
        for pid, sid, s, e in ms:
            if sid not in best or (e - s) > (best[sid][3] - best[sid][2]):
                best[sid] = (pid, sid, s, e)
        sample = sorted(best.values(), key=lambda m: -(m[3] - m[2]))[:6]

        blocks.append({
            'unit': u, 'n_w': n_w, 'n_pages': umeta[u]['n_pages'],
            'med_len': umeta[u]['med_len'], 'verdict': verdict,
            'marc_hint': marc_hint, 'n_named_org': n_named_org,
            'n_named_marc': n_named_marc, 'is_liturgy': is_liturgy,
            'liturgy_share': round(liturgy_share, 2),
            'org': top_org[:8], 'marc': marc_c.most_common(8),
            'eng': eng_c.most_common(4), 'frame': frame_c.most_common(4),
            'snippet': snippet, 'sample': sample,
        })

    fc.close()
    fj.close()
    write_report(blocks, summary, lib, t0)


def write_report(blocks, summary, lib, t0):
    L = ["# Residue naming — cataloger-title aggregation (SEED-029, 2026-07-09)",
         "",
         "The 40 most-copied UNIDENTIFIED passage clusters (Track-1 assigned no "
         "label), each named by aggregating TWO cataloger title sources across "
         "the cluster's witness manuscripts: the genizah-work title "
         "(`catalog.GenizahTitleOrgTitle`) and the library MARC title "
         "(`libraries.csv`). Counts are **distinct witness MSS** carrying each "
         "title (not catalog rows); placeholders ('לא מקודד') dropped. The "
         "verdict uses the PRECISE genizah-title (catalogers set it only on "
         "identified units, so its absence = genuine unknown); the near-"
         "universal but noisier MARC title is shown as co-evidence, and flagged "
         "as a `📖 MARC suggests` hint when the genizah-ID is thin/absent but "
         "the MARC titles agree. A cluster is:",
         "",
         "- **clear** — one title covers ≥" f"{int(MODAL_SHARE*100)}% of "
         "named witnesses (propose it as the work name);",
         "- **competing** — ≥2 titles each on ≥" f"{COMPETE_MIN} "
         "witnesses, no dominant (Hillel decides: variants of one work, or "
         "parallel works?);",
         "- **thin** — some titles but <" f"{MIN_NAMED} named witnesses (weak "
         "hint);",
         "- **unnamed** — no cataloger ever assigned a genizah-title "
         "(genuine unknown / discovery candidate).",
         "",
         "LIMITATION: aggregation is at MANUSCRIPT level; a composite witness "
         "contributes every work its MS was identified as, so titles unrelated "
         "to this passage can appear. The modal title is the signal.",
         "",
         "## Summary",
         "",
         f"| verdict | units |",
         f"|---|---|",
         f"| clear (propose name) | {summary['clear']} |",
         f"| competing (same-work vs parallel-works?) | {summary['competing']} |",
         f"| thin (weak hint) | {summary['thin']} |",
         f"| unnamed (genuine unknown) | {summary['unnamed']} |",
         "",
         f"**🎵 {summary['_liturgy']} of the 40 clusters are "
         f"liturgy-agglomerations** (≥{int(LITURGY_SHARE*100)}% of witnesses "
         "carry a piyyut/liturgy MARC title) — these are NOT single unknown "
         "works but many poems chained by shared liturgical formulae (the "
         "canonical-liturgy trap, cf. the quarantined continuum unit "
         f"{CONTINUUM_UNIT}); they should be split/masked, not named. The "
         "non-liturgy `clear`/`competing` clusters are genuine reference-gap "
         "works (catalogued Judeo-Arabic tafsir, grammar, narrative) "
         "recoverable by ADDING their references.",
         "",
         "## Clusters",
         ""]
    V = {'clear': '✅ CLEAR', 'competing': '⚖️ COMPETING',
         'thin': '❓ THIN', 'unnamed': '⬜ UNNAMED'}
    order = {'clear': 0, 'competing': 1, 'thin': 2, 'unnamed': 3}
    for b in sorted(blocks, key=lambda x: (order[x['verdict']], -x['n_w'])):
        hint = f" · 📖 MARC suggests: **{b['marc_hint']}**" if b['marc_hint'] \
            else ""
        lit = (f" · 🎵 **LITURGY-AGGLOMERATION** ({int(b['liturgy_share']*100)}"
               "% piyyut/liturgy witnesses — likely many poems chained, "
               "route to masking not naming)") if b['is_liturgy'] else ""
        L.append(f"### unit {b['unit']} — {V[b['verdict']]}{hint}{lit} · "
                 f"{b['n_w']} witnesses · {b['n_pages']} pages · "
                 f"med {b['med_len']} letters")
        if b['org']:
            L.append("")
            L.append(f"**cataloger genizah-titles** ({b['n_named_org']} named, "
                     "distinct-witness count):  ")
            L.append("  \n".join(f"- `{c}×` {t}" for t, c in b['org']))
        if b['marc']:
            L.append("")
            L.append(f"**library (MARC) titles** ({b['n_named_marc']} named, "
                     "distinct-witness count):  ")
            L.append("  \n".join(f"- `{c}×` {t}" for t, c in b['marc']))
        if b['eng']:
            L.append("")
            L.append("_English titles:_ " +
                     "; ".join(f"{t} ({c})" for t, c in b['eng']))
        if b['frame']:
            L.append("")
            L.append("_textual frame (genre):_ " +
                     "; ".join(f"{t} ({c})" for t, c in b['frame']))
        if b['snippet']:
            L.append("")
            L.append(f"> {b['snippet']}")
        L.append("")
        L.append("witnesses:  ")
        for pid, sid, s, e in b['sample']:
            sm, lc, lt = lib.get(sid, (sid, '?', ''))
            url = B.viewer_url(sid, B.pnum(pid))
            tail = f" — _{lt[:70]}_" if lt else ""
            L.append(f"- [{sm}]({url}) ({lc}) p{B.pnum(pid)}{tail}")
        L.append("")
    L.append(f"_runtime {time.time() - t0:.0f}s · {len(blocks)} clusters_")
    open(OUT, 'w', encoding='utf-8').write('\n'.join(L))
    print(f"summary: {summary}")
    print(f"wrote {OUT} ({time.time() - t0:.0f}s)")


if __name__ == '__main__':
    build()
