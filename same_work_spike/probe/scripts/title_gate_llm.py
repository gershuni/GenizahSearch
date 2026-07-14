# -*- coding: utf-8 -*-
"""MAPV2-15o — LLM title-understanding gate (non-mechanical).

The mechanical TitleGate/identified-gate cannot connect a matched work to a
manuscript's catalogue identification when they differ by LANGUAGE
(כתאב אללמע = ריב"ג ספר הרקמה), ABBREVIATION/ALT-TITLE (כתאב אלמרשד = the short
Arabic title of בחיי חובות הלבבות), or a compound/section name the token-bag
drops (משנה תורה ספר שופטים). This gate asks an LLM (Gemini 3.5 Flash via
OpenRouter — pilot-validated: 28x faster than per-card Opus, 0 container-witness
buried, 84% agreement) to classify each distinct (work, catalogue-title) pair:

  same_work  catalogue names the SAME work (alias/translation/abbrev/section) -> known
  container  catalogue names a CONTAINER (siddur/anthology/piyyut booklet) that
             legitimately HOLDS this specific unit -> keep (real witness/find)
  different  catalogue names a genuinely different composition
  uncertain  -> keep

Robustness (from the pilot): verdicts align by POSITION not echoed uid (Gemini
corrupted 4/200 long Hebrew uids); an ALIAS_SEED pins the classic Arabic<->Hebrew
pairs Flash's memory misses; verdicts are CACHED by (work, catalogue signature)
so re-runs on future harvests are instant and only novel pairs hit the API.

Reusable: import verdicts_for_pairs() / get_verdict(); or run standalone to
populate the cache over the discovery pile's distinct pairs.

Out: data/title_pair_verdicts.json  (the cache: sig -> verdict record)
Usage: python -X utf8 -u title_gate_llm.py [--model M] [--batch N] [--limit N]
"""
import argparse
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from title_gate import norm_text  # noqa: E402

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
GATED = PROBE + r"\data\discovery_scored_gated.jsonl"
LIB = r"C:\Genizahsearch\libraries.csv"
CACHE = PROBE + r"\data\title_pair_verdicts_v2.json"   # v2 = refined 5-way taxonomy
_URL = 'https://openrouter.ai/api/v1/chat/completions'

# Classic Judeo-Arabic <-> Hebrew / abbreviation title pairs Flash's memory is
# unreliable on. Both markers must appear (catalogue side AND work side) to fire
# -> same_work (high). Precise, low false-positive; the LLM handles the tail.
# (catalogue_marker, work_marker)
ALIAS_SEED = [
    ('אללמע', 'הרקמה'),                 # ibn Janah, Kitab al-Luma
    ('אלאצול', 'השרשים'),               # ibn Janah, Kitab al-Usul
    ('אלמסתלחק', 'ההשגה'),              # ibn Janah, al-Mustalhaq
    ('אלמרשד', 'חובות הלבבות'),         # Bahya (abbrev. of al-Hidaya)
    ('אלהדאיה', 'חובות הלבבות'),        # Bahya (full)
    ('פראיץ אלקלוב', 'חובות הלבבות'),   # Bahya (full)
    ('אלמחאצרה', 'העיונים והדיונים'),   # Moses ibn Ezra, poetics
    ('אלמחאצרה', 'שירת ישראל'),
    ('דלאלה אלחאירין', 'מורה נבוכים'),  # Maimonides
    ('אלאמאנאת', 'אמונות ודעות'),       # Saadia
    ('אלאמאנאת ואלאעתקאדאת', 'אמונות ודעות'),
]

SYS_PROMPT = """You are a Cairo Genizah cataloguing expert. THE TEST: reading ONLY the manuscript's CATALOGUE title, could a cataloguer predict/expect this specific composition (work_title) here? Classify each item:

- "known": the catalogue EXPLICITLY names THIS composition — you'd know it's this exact work. Includes: direct name; a Judeo-Arabic original <-> Hebrew title of the SAME work ("כתאב אללמע"=ספר הרקמה); an ABBREVIATION/alt title ("כתאב אלמרשד"=חובות הלבבות); a section/volume name of the same work ("משנה תורה (ספר שופטים)"); an EXPLICIT author+work naming ("תפסיר רס"ג: משלי", "…לרס"ג", "סעדיה גאון"); OR a Bible-section catalogue (מקרא/כתובים/נביאים/תורה/חומש) for the ARAMAIC TARGUM of a book in that section (the Bible codex is that Targum's home, e.g. "כתובים" + תרגום רות).
- "witness": the catalogue names a full standard RITE prayer-book or a SPECIFIC ceremony/occasion whose content is predictable — you'd expect this exact unit there. E.g. "סדור מנהג אשכנז"/"מחזור [rite]" (a whole standard siddur/machzor -> its standard piyyutim), "קינות על נפטרים" (a dirge for the dead), "סדר נשואין כמנהג קראים" (a specific ceremony).
- "discovery": the catalogue is GENERIC and does NOT let you predict this specific content — identifying it is a real find. E.g. "קבץ קטעי גניזה", "קטעי תפלה : גניזה", "חוברת פיוטים", "קטעי מקרא", "פיוט", "קטעי גניזה", "מעמדיה"; a GENERIC "תרגום ערבי ל[book]"/"תרגום ופרוש ערבי לכתובים" that does NOT name the specific author/work (an Arabic translation is NOT an expected companion — finding it is Saadia's specific work is a discovery); AND a BROAD rite grab-bag like "סליחות מנהג קראים"/"תפלות מנהג קראים"/"פיוטים" that names only a genre+rite, not a specific occasion, so the exact piyyut is NOT predictable.
- "different": the catalogue names a genuinely DIFFERENT specific composition.
- "uncertain": cannot tell.

Decisive rule: a GENERIC collection title (fragments / booklet / "prayer fragments" / generic "Arabic translation of X" / broad genre+rite) is NEVER witness or known — it is "discovery". Reserve known/witness for titles that specifically name the work OR a specific predictive rite/ceremony/Bible-section. Judge only from titles/author/genre.

Input is a JSON array of items each with an integer "i". Return ONLY a JSON array with one object per item IN THE SAME ORDER, echoing the integer i: [{"i":0,"verdict":"known|witness|discovery|different|uncertain","confidence":"high|medium|low","reason":"<=12 words"}]. No prose, no markdown fences."""


# ---------------------------------------------------------------- key + client
def _load_key():
    for p in (r'C:\Genizahsearch\.env', os.path.join(os.getcwd(), '.env')):
        try:
            with open(p, encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('OPENROUTER_API_KEY='):
                        v = line.split('=', 1)[1].strip().strip('"').strip("'")
                        if v:
                            return v
        except OSError:
            pass
    return os.environ.get('OPENROUTER_API_KEY')


_KEY = _load_key()


def _chat(messages, model, temperature=0, max_tokens=8000, timeout=180):
    if not _KEY:
        raise RuntimeError('no OPENROUTER_API_KEY')
    body = json.dumps({'model': model, 'messages': messages,
                       'temperature': temperature, 'max_tokens': max_tokens}
                      ).encode('utf-8')
    req = urllib.request.Request(
        _URL, data=body,
        headers={'Authorization': f'Bearer {_KEY}',
                 'Content-Type': 'application/json',
                 'HTTP-Referer': 'https://genizahsearch.com',
                 'X-Title': 'genizah-title-gate'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        d = json.loads(r.read().decode('utf-8'))
    return d['choices'][0]['message']['content']


# ---------------------------------------------------------------- pair helpers
def pair_sig(work_title, catalog_titles):
    """Stable cache key for a (work, catalogue) pair — normalized so trivial
    punctuation/spacing variants collapse."""
    ct = ' | '.join(sorted(catalog_titles or []))
    return norm_text(work_title or '') + ' ::: ' + norm_text(ct)


def seed_verdict(work_title, catalog_titles):
    w = work_title or ''
    ctext = ' '.join(catalog_titles or [])
    for cm, wm in ALIAS_SEED:
        if cm in ctext and wm in w:
            return {'verdict': 'known', 'confidence': 'high',
                    'reason': f'alias seed: {cm} = {wm}'}
    return None


def _parse_array(txt):
    m = re.search(r'\[.*\]', txt, re.S)
    if not m:
        raise ValueError('no JSON array in response')
    return json.loads(m.group(0))


def classify_batch(items, model):
    """items: list of dicts with work_title/author/cat/genre/catalog_titles.
    Returns list of verdict dicts aligned BY POSITION (echoed i is validated)."""
    payload = [{'i': k, 'work_title': it['work_title'], 'author': it.get('author', ''),
                'cat': it.get('cat', ''), 'genre': it.get('genre', ''),
                'catalog_titles': it['catalog_titles']}
               for k, it in enumerate(items)]
    txt = _chat([{'role': 'system', 'content': SYS_PROMPT},
                 {'role': 'user', 'content': json.dumps(payload, ensure_ascii=False)}],
                model=model)
    arr = _parse_array(txt)
    if len(arr) != len(items):
        raise ValueError(f'count mismatch: got {len(arr)} want {len(items)}')
    # align by position; keep verdict fields only (ignore possibly-wrong echoed i)
    out = []
    for v in arr:
        out.append({'verdict': v.get('verdict', 'uncertain'),
                    'confidence': v.get('confidence', 'low'),
                    'reason': (v.get('reason') or '')[:120]})
    return out


# ---------------------------------------------------------------- cache
def load_cache():
    if os.path.exists(CACHE):
        try:
            return json.load(open(CACHE, encoding='utf-8'))
        except (OSError, ValueError):
            pass
    return {}


def save_cache(cache):
    tmp = CACHE + '.tmp'
    json.dump(cache, open(tmp, 'w', encoding='utf-8'), ensure_ascii=False, indent=0)
    os.replace(tmp, CACHE)


def get_verdict(work_title, catalog_titles, cache):
    """Cached lookup: seed -> cache -> None (caller batches the misses)."""
    sd = seed_verdict(work_title, catalog_titles)
    if sd:
        return sd
    return cache.get(pair_sig(work_title, catalog_titles))


# ---------------------------------------------------------------- populate
def _load_nli():
    import csv
    out = {}
    with open(LIB, encoding='utf-8-sig', newline='') as f:
        rd = csv.reader(f)
        next(rd, None)
        for row in rd:
            if len(row) >= 8 and row[0]:
                out[row[0]] = (row[7] or '').strip()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--model', default='google/gemini-3.5-flash')
    ap.add_argument('--batch', type=int, default=40)
    ap.add_argument('--limit', type=int, default=None)
    a = ap.parse_args()
    t0 = time.time()

    nli = _load_nli()
    rows = [json.loads(l) for l in open(GATED, encoding='utf-8')]
    disc = [r for r in rows if r.get('bucket2') == 'discovery']

    # distinct (work_title, catalog-sig) pairs among rows WITH any catalogue
    # title (generic titles included — the LLM decides generic->discovery, and
    # skipping them fell through to a bad mechanical fallback; validated 99%)
    seen, pairs = {}, []
    for r in disc:
        t = nli.get(str(r['sys_id']), '')
        if not t:
            continue
        cats = [t]
        item = {'work_title': r.get('title') or '', 'author': r.get('author') or '',
                'cat': r.get('cat') or '', 'genre': r.get('genre') or '',
                'catalog_titles': cats}
        sig = pair_sig(item['work_title'], cats)
        if sig not in seen:
            seen[sig] = item
            pairs.append((sig, item))
    print(f"discovery rows: {len(disc)}; distinct specific-title pairs: {len(pairs)}",
          flush=True)

    cache = load_cache()
    # seed + cache hits first; only novel non-seed pairs go to the API
    todo = []
    n_seed = n_cached = 0
    for sig, item in pairs:
        if seed_verdict(item['work_title'], item['catalog_titles']):
            n_seed += 1
            continue
        if sig in cache:
            n_cached += 1
            continue
        todo.append((sig, item))
    if a.limit:
        todo = todo[:a.limit]
    print(f"seed-covered {n_seed}, cache hits {n_cached}, to classify {len(todo)}",
          flush=True)

    done = 0
    for i in range(0, len(todo), a.batch):
        chunk = todo[i:i + a.batch]
        items = [it for _s, it in chunk]
        for attempt in range(4):
            try:
                vs = classify_batch(items, a.model)
                for (sig, _it), v in zip(chunk, vs):
                    cache[sig] = v
                break
            except (urllib.error.HTTPError, urllib.error.URLError, ValueError,
                    TimeoutError) as e:
                print(f"  batch@{i} attempt{attempt}: {type(e).__name__} "
                      f"{str(e)[:100]}", flush=True)
                time.sleep(4 * (attempt + 1))
        else:
            print(f"  batch@{i} FAILED after retries; leaving uncached", flush=True)
        done += len(chunk)
        if (i // a.batch) % 5 == 0 or done >= len(todo):
            save_cache(cache)
            print(f"  {done}/{len(todo)} ({time.time()-t0:.0f}s)", flush=True)
    save_cache(cache)

    # summary over ALL pairs (seed + cache)
    from collections import Counter
    dist = Counter()
    for sig, item in pairs:
        v = get_verdict(item['work_title'], item['catalog_titles'], cache)
        dist[v['verdict'] if v else 'unclassified'] += 1
    print(f"\nverdict distribution over {len(pairs)} distinct pairs: {dict(dist)}")
    print(f"cache size {len(cache)}; done in {time.time()-t0:.0f}s")
    print(f"wrote {CACHE}")


if __name__ == '__main__':
    main()
