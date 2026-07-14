# -*- coding: utf-8 -*-
"""Maagarim מסירות נוספות (additional known-witness manuscripts) harvest.

SEED-029 Track-1 4th novelty channel. When Track-1 flags a Genizah fragment as
a "new?" witness of a Maagarim-edited work W, the fragment may actually be a
manuscript the Academy of the Hebrew Language already knows about but did NOT
use for the printed edition. Those "additional deliveries" (מסירות נוספות) are
a website-only known-witness list that further shrinks the true "new" set.

TASK-0 RESOLVED — the working API path
--------------------------------------
The task brief anticipated finishing the `GetYzira` request body. Tracing the
site's `mainJs` showed `GetYzira` returns the *reading-view HTML* of the work,
NOT the witness lists. The witness lists come from a DIFFERENT endpoint:

    POST https://maagarim.hebrew-academy.org.il/Pages/ws/Mqorot.asmx/GetPirteiHibur
    Content-Type: application/json; charset=utf-8
    User-Agent: <browser UA>   (Cloudflare 403s without one)
    body = {"misyzira": <N>, "tabNum": 0}

(mainJs: `ShowEssayDetails(n,t)` builds `i={misyzira:n, tabNum:t}` and POSTs it
to `Mqorot.asmx/GetPirteiHibur`; the success handler fills
`liMsirot.innerHTML=i.msirot` and `liMsirotNosafot.innerHTML=i.nosafot`.)

The JSON response object carries HTML-fragment fields, of which we use:
  * `msirot`   — the manuscript witnesses USED for the edition (tab "כתבי יד").
                 A superset of GetYziraFull.mesirot / the local ##המסירה##
                 headers (it also lists extra deliveries the edition skipped).
  * `nosafot`  — element `liMsirotNosafot` (מסירות נוספות). Its content header
                 is EITHER:
                   - "כתבי יד"          -> ADDITIONAL manuscript witnesses  <-- gold
                   - "פרסומים בדפוס"    -> printed publications (won't match a shelfmark)
                   - "אין מסירות נוספות" -> none

Each witness row exposes the clean location string inside
`<a class='openClose' ...><span> </span>LOCATION</a>` (identical to the `zihuy`
in the row's `doFeedbackForMesira({...})` handler). We extract those, then run
each through the PROVEN shelfmark parser/matcher in `mesirah_witnesses.py`
(imported, not reimplemented).

Output: ..\\data\\mesirot_nosafot.json  — one object per harvested work:
  {work_id, misyzira, title, ok,
   nosafot_header, nosafot:[raw strings], matched:[{sys_id, library_code,
        classmark, confidence}],                     # nosafot-field matches
   msirot_header,  msirot_web:[raw strings], msirot_matched:[{...}]}

Politeness: 1 request/second; browser UA; on HTTP error back off 5s and retry
up to 3x, then log + skip. Progress is checkpointed to
..\\data\\mesirot_nosafot_checkpoint.json every 50 works so a crash resumes.

Scope: DISTINCT work_id from results/track1_full_testimonies.csv where
tier in ('new?','new?known') -> the M:Ytext<N> integer N (738 works). The
residue CLEAR/COMPETING clusters in results/residue_naming.md carry NO Ytext
ids (they are Track-1 UNIDENTIFIED units), so they are not resolvable and are
skipped per the brief.

Run:  cd C:\\Genizahsearch\\same_work_spike\\probe\\scripts
      python -X utf8 -u maagarim_nosafot_harvest.py
"""
import csv
import json
import os
import re
import sys
import time
import urllib.request
from html import unescape

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mesirah_witnesses as mw  # noqa: E402  (proven parser/matcher/index)
from maagarim_api_probe import UA, THROTTLE_S  # noqa: E402  (proven recipe)

HERE = os.path.dirname(os.path.abspath(__file__))
TESTIMONIES = os.path.join(HERE, '..', 'results', 'track1_full_testimonies.csv')
OUT_JSON = os.path.join(HERE, '..', 'data', 'mesirot_nosafot.json')
CKPT = os.path.join(HERE, '..', 'data', 'mesirot_nosafot_checkpoint.json')

ROOT = 'https://maagarim.hebrew-academy.org.il/Pages/ws'
RETRIES = 3
BACKOFF_S = 5.0
CKPT_EVERY = 50

RANK = {'high': 0, 'low': 1, 'ambiguous': 2}

# clean witness location = text of the openClose anchor (== doFeedbackForMesira zihuy)
OPEN_RE = re.compile(
    r"<a class='openClose'[^>]*>(?:<span>[^<]*</span>)?\s*([^<]+?)\s*</a>")
HDR_RE = re.compile(r"srEsDetOvHeader'><li><a>([^<]*)</a>")
H4_RE = re.compile(r"<h4>([^<]*)</h4>")


def get_pirtei(misyzira):
    """POST GetPirteiHibur; returns the parsed JSON dict (raises on failure)."""
    body = json.dumps({'misyzira': int(misyzira), 'tabNum': 0}).encode('utf-8')
    req = urllib.request.Request(
        f'{ROOT}/Mqorot.asmx/GetPirteiHibur', data=body,
        headers={'Content-Type': 'application/json; charset=utf-8',
                 'User-Agent': UA})
    with urllib.request.urlopen(req, timeout=30) as r:
        d = json.loads(r.read().decode('utf-8'))
    v = d.get('d', d)
    return json.loads(v) if isinstance(v, str) else v


def header_of(html_fragment):
    if not html_fragment:
        return None
    m = HDR_RE.search(html_fragment)
    if m:
        return m.group(1).strip()
    m = H4_RE.search(html_fragment)
    return m.group(1).strip() if m else None


def extract_locations(html_fragment):
    """All witness location strings from a msirot/nosafot HTML fragment."""
    if not html_fragment:
        return []
    seen, out = set(), []
    for loc in OPEN_RE.findall(html_fragment):
        s = unescape(loc).strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def match_strings(raw_strings, main_index, base_index):
    """Run raw location strings through the mesirah_witnesses pipeline.

    Returns a list of dedup'd matches [{sys_id, library_code, classmark,
    confidence}] (best confidence per sys_id)."""
    best = {}          # sys_id -> (lib, conf, classmark)
    for raw in raw_strings:
        cleaned = mw.clean_raw(raw)
        parts = [x.strip(' ,') for x in cleaned.split(';') if x.strip(' ,')]
        for part in parts or ['']:
            p = mw.parse_location(part)
            if p.reason in ('non_latin_or_printed', 'printed_edition',
                            'non_genizah_location', 'oxford_neubauer_only',
                            'bl_catalog_number_only', 'no_classmark', 'empty',
                            'unrecognized_institution'):
                continue
            cands, exact_only, low_cap, display = mw.build_candidates(p)
            if not cands:
                continue
            matched = mw.match_candidates(cands, exact_only, low_cap, p.code,
                                          main_index, base_index)
            for sid, (lib, conf) in matched.items():
                old = best.get(sid)
                if old is None or RANK[conf] < RANK[old[1]]:
                    best[sid] = (lib, conf, display)
    return [{'sys_id': sid, 'library_code': lib, 'classmark': cm,
             'confidence': conf}
            for sid, (lib, conf, cm) in sorted(best.items())]


FLANK = os.path.join(HERE, '..', 'data', 'discovery_scored_flank.jsonl')


def load_scope():
    """UNION of the original testimonies new?/new?known works AND every
    distinct discovery-candidate work (MAPV2-15n gap fix): the discovery pile's
    works were 77% uncovered, so their known witnesses leaked as false finds.
    Only Maagarim M:Ytext works have a misyzira to query (JA/Sefaria skip)."""
    ids = set()
    try:
        with open(TESTIMONIES, encoding='utf-8-sig', newline='') as f:
            for r in csv.DictReader(f):
                if r['tier'] in ('new?', 'new?known') \
                        and r['work_id'].startswith('M:Ytext'):
                    ids.add(r['work_id'])
    except FileNotFoundError:
        pass
    if os.path.exists(FLANK):
        for line in open(FLANK, encoding='utf-8'):
            r = json.loads(line)
            if r.get('bucket2') == 'discovery' \
                    and str(r.get('work_id', '')).startswith('M:Ytext'):
                ids.add(r['work_id'])
    out = []
    for w in sorted(ids):
        try:
            out.append((w, int(w.replace('M:Ytext', ''))))
        except ValueError:
            pass
    return out


def load_checkpoint():
    if os.path.exists(CKPT):
        try:
            return json.load(open(CKPT, encoding='utf-8'))
        except Exception:
            pass
    return {}


def save_checkpoint(data):
    tmp = CKPT + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, CKPT)


def rematch_existing():
    """Network-free: re-run the shelfmark matcher over the raw location strings
    ALREADY stored in mesirot_nosafot.json and rewrite the matched fields. Use
    after a mesirah_witnesses.py parser/index improvement (e.g. JTS multi-
    numbering cores) so existing harvested works pick up the better matches
    without re-fetching. Only `msirot_matched` / `matched` change; the fetched
    `msirot_web` / `nosafot` raw strings are the source of truth and untouched."""
    t0 = time.time()
    if not os.path.exists(OUT_JSON):
        print(f'no {OUT_JSON} to re-match', flush=True)
        return
    print('building shelfmark indexes from libraries.csv ...', flush=True)
    main_index, base_index, n_rows = mw.build_indexes()
    print(f'  {n_rows:,} rows -> {len(main_index):,} exact keys '
          f'({time.time()-t0:.0f}s)', flush=True)
    works = json.load(open(OUT_JSON, encoding='utf-8'))
    before = after = changed = 0
    for r in works:
        if not r.get('ok'):
            continue
        before += len(r.get('msirot_matched') or []) + len(r.get('matched') or [])
        ms_new = match_strings(r.get('msirot_web') or [], main_index, base_index)
        no_new = match_strings(r.get('nosafot') or [], main_index, base_index)
        old = {m['sys_id'] for m in (r.get('msirot_matched') or [])
               } | {m['sys_id'] for m in (r.get('matched') or [])}
        r['msirot_matched'] = ms_new
        r['matched'] = no_new
        new = {m['sys_id'] for m in ms_new} | {m['sys_id'] for m in no_new}
        after += len(ms_new) + len(no_new)
        if new != old:
            changed += 1
    with open(OUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(works, f, ensure_ascii=False, indent=1)
    # keep the checkpoint consistent so a later fetch-run doesn't revert matches
    ckpt = {r['work_id']: r for r in works if r.get('work_id')}
    save_checkpoint(ckpt)
    print(f'rematched {len(works):,} works; matched sys_ids {before:,} -> '
          f'{after:,} (+{after-before:,}); works changed: {changed:,}')
    print(f'wrote {os.path.abspath(OUT_JSON)} ({time.time()-t0:.0f}s)')


def main():
    if '--rematch' in sys.argv:
        rematch_existing()
        return
    t0 = time.time()
    print('building shelfmark indexes from libraries.csv ...', flush=True)
    main_index, base_index, n_rows = mw.build_indexes()
    print(f'  {n_rows:,} rows -> {len(main_index):,} exact keys '
          f'({time.time()-t0:.0f}s)', flush=True)

    scope = load_scope()
    print(f'scope: {len(scope):,} works (tier new?/new?known, M:Ytext ids)',
          flush=True)

    results = load_checkpoint()
    # seed from the already-written output so the original 738 harvested works
    # are NOT re-fetched when the scope is expanded (no data loss, no re-query).
    if os.path.exists(OUT_JSON):
        try:
            for r in json.load(open(OUT_JSON, encoding='utf-8')):
                if r.get('work_id'):
                    results.setdefault(r['work_id'], r)
        except Exception:  # noqa: BLE001
            pass
    done = set(results.keys())
    print(f'checkpoint+existing: {len(done):,} works already harvested; '
          f'resuming', flush=True)

    n_ok = n_fail = n_new = 0
    for i, (work_id, mid) in enumerate(scope):
        if work_id in done:
            r = results[work_id]
            if r.get('ok'):
                n_ok += 1
            else:
                n_fail += 1
            continue

        pirtei = None
        for attempt in range(1, RETRIES + 1):
            try:
                time.sleep(THROTTLE_S)
                pirtei = get_pirtei(mid)
                break
            except Exception as e:  # noqa: BLE001 (retry/skip everything)
                print(f'  ! {work_id} (misyzira={mid}) attempt {attempt}/'
                      f'{RETRIES}: {type(e).__name__}: {e}', flush=True)
                if attempt < RETRIES:
                    time.sleep(BACKOFF_S)

        if pirtei is None:
            results[work_id] = {'work_id': work_id, 'misyzira': mid,
                                'ok': False, 'error': 'fetch_failed'}
            n_fail += 1
        else:
            ms_html = pirtei.get('msirot') or ''
            no_html = pirtei.get('nosafot') or ''
            ms_locs = extract_locations(ms_html)
            no_locs = extract_locations(no_html)
            no_matched = match_strings(no_locs, main_index, base_index)
            ms_matched = match_strings(ms_locs, main_index, base_index)
            results[work_id] = {
                'work_id': work_id,
                'misyzira': mid,
                'ok': True,
                'title': (pirtei.get('title') or '').strip(),
                'nosafot_header': header_of(no_html),
                'nosafot': no_locs,
                'matched': no_matched,               # nosafot-field matches
                'msirot_header': header_of(ms_html),
                'msirot_web': ms_locs,
                'msirot_matched': ms_matched,
            }
            n_ok += 1
            n_new += 1

        if (i + 1) % CKPT_EVERY == 0 or (i + 1) == len(scope):
            save_checkpoint(results)
            print(f'  [{i+1:>4}/{len(scope)}] ok={n_ok} fail={n_fail} '
                  f'new_this_run={n_new} ({time.time()-t0:.0f}s)', flush=True)

    save_checkpoint(results)

    # ---- write final output (only the in-scope works, ordered by scope) ----
    scope_ids = [w for w, _ in scope]
    ordered = [results[w] for w in scope_ids if w in results]
    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(ordered, f, ensure_ascii=False, indent=1)

    tot_nosafot = sum(len(r.get('nosafot', [])) for r in ordered if r.get('ok'))
    tot_no_match = sum(len(r.get('matched', [])) for r in ordered if r.get('ok'))
    n_no_ms = sum(1 for r in ordered
                  if r.get('ok') and r.get('nosafot_header') == 'כתבי יד')
    print(f'\nwrote {os.path.abspath(OUT_JSON)} ({len(ordered):,} works)')
    print(f'  ok={n_ok} fail={n_fail}')
    print(f'  works whose nosafot tab = additional manuscripts: {n_no_ms:,}')
    print(f'  total nosafot raw strings: {tot_nosafot:,}')
    print(f'  total nosafot sys_id matches: {tot_no_match:,}')
    print(f'done in {time.time()-t0:.0f}s')


if __name__ == '__main__':
    main()
