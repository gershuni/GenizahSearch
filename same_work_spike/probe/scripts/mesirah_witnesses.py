# -*- coding: utf-8 -*-
"""Mesirah witness cross-check table (SEED-029 Track-1 demotion layer).

Maagarim edition files record the source manuscript ("##המסירה: ...##",
also section-scoped "##סעיף N | המסירה: ...##" — the plain-form regex in
track1_build_ref.py misses the section-scoped variant, e.g. Ytext689001's
ENA 1501, 1-7 lives ONLY in section headers). When the Track-1 discovery
queue flags a fragment as a "new?" witness of work W but the fragment's
manuscript IS W's recorded mesirah, it is the known source witness, not new.

Outputs:
  ..\\data\\mesirah_witnesses.json  — rows {work_id, raw, library_hint,
        classmark, sys_id, library_code, confidence}
  ..\\results\\mesirah_crosscheck.md — extraction/match/demotion report

Work-id convention MATCHES track1_build_ref.py exactly:
    base = fn.replace('.txt-OnlyText.txt', ''); id = 'M:' + base.split('--')[-1]

Confidence tiers (binding, precision-first):
  high      — normalized classmark matches a call-number variant EXACTLY and
              the library_code agrees with the parsed library hint. Range
              mesirot (e.g. "ENA, 1501, 1-7") are expanded leaf-by-leaf; each
              expanded leaf that hits exactly one variant is a high match.
  low       — classmark matches but no library agreement, OR only a
              base/box-level (substring/range-situation) match.
  ambiguous — the match resolves to MANY sys_ids (>3); never auto-demote.

Run:  cd C:\\Genizahsearch\\same_work_spike\\probe\\scripts
      python -X utf8 -u mesirah_witnesses.py
"""
import csv
import html
import itertools
import json
import os
import re
import sys
import time
import unicodedata
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
MAAGARIM = r"C:\Users\gersh\Dropbox\דיקטה\מאגרים\AllTextsOnlyText"
LIBCSV = r"C:\Genizahsearch\libraries.csv"
OUT_JSON = os.path.join(HERE, '..', 'data', 'mesirah_witnesses.json')
OUT_MD = os.path.join(HERE, '..', 'results', 'mesirah_crosscheck.md')
TESTIMONIES = os.path.join(HERE, '..', 'results', 'track1_full_testimonies.csv')
CARDS = os.path.join(HERE, '..', 'review', 'discovery_cards.json')

# any ##...## block that carries המסירה: (plain OR section-scoped form)
MES_RE = re.compile(r'##[^#]*?המסירה:\s*([^#]+?)\s*##')
RANGE_RE = re.compile(r'^(\d+)\s*[-–_]\s*(\d+)$')
MAX_RANGE = 40          # widest leaf-range we expand
AMBIG_N = 3             # >3 sys_ids for one classmark => 'ambiguous'

CANON_CATS = ('Bible', 'Bavli', 'Mishnah', 'Yerushalmi', 'Tosefta')

# Hillel's known leaks (work ids; exact sys_ids pulled from discovery_cards.json)
LEAK_WORKS = ['M:Ytext839000', 'M:Ytext689001', 'M:Ytext500017', 'M:Ytext610000']


# ---------------------------------------------------------------- normalize
def norm(s: str) -> str:
    """lowercase; NFKD-fold accents; keep only ascii alnum + Hebrew letters."""
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    return ''.join(ch for ch in s
                   if ch.isalnum() and (ch.isascii() or '֐' <= ch <= '׿'))


# normalized institution-name prefixes to strip off call-number variants
STRIP_PREFIXES = [
    'libraryattheherbertdkatzcenterforadvancedjudaicstudiesuniversityofpennsylvania',
    'instituteoforientalmanuscriptstherussianacademyofsciences',
    'thejewishtheologicalseminaryofamerica',
    'libraryofthehungarianacademyofsciences',
    'thebodleianlibrariesuniversityofoxford',
    'libraryoftheallianceisraeliteuniverselle',
    'theuniversityofmanchesterlibrary',
    'westminstercollegelibrary',
    'cambridgeuniversitylibrary',
    'thenationallibraryofrussia',
    'therussianstatelibrary',
    'austriannationallibrary',
    'gastermosescollection',
    'thebritishlibrary',
    'mosserijacques',
    'libraryofgeneva',
    'adlerelkannathan',
]


def variant_keys(nv: str):
    """All normalized index keys derivable from one normalized variant."""
    keys = set()

    def add(k):
        if len(k) >= 3 and not k.isdigit():
            keys.add(k)

    add(nv)
    stripped = nv
    changed = True
    while changed:
        changed = False
        for p in STRIP_PREFIXES:
            if stripped.startswith(p) and len(stripped) > len(p):
                stripped = stripped[len(p):]
                changed = True
    add(stripped)
    if stripped.startswith('ms') and not stripped.startswith('misc') \
            and not stripped.startswith('moss'):
        add(stripped[2:])
        stripped2 = stripped[2:]
    else:
        stripped2 = stripped
    # aliases
    for k in (stripped, stripped2):
        if k.startswith('cataloguehalperphiladelphia'):
            add('halper' + k[len('cataloguehalperphiladelphia'):])
        if k.startswith('evrantonin'):
            add('antonin' + k[len('evrantonin'):])
        if k.startswith('lg') and len(k) > 4:      # Lewis-Gibson 'L-G Lit. ...'
            add(k[2:])
    return keys


# ---- JTS multi-numbering cores ----------------------------------------------
# JTS catalogues the SAME manuscript under parallel numbering systems (ENA,
# ENA NS, Rab., Ms., and bare Steinschneider/Ms. catalogue numbers). Maagarim
# frequently cites one system where libraries.csv records another — e.g. mesirah
# "Rab. 2148" vs libraries.csv "Catalogue Brumer Rab. 2148 | Ms. 9926", or a
# bare "4391" vs "Ms. 4391, fol. 121" — so the leaf never resolves through the
# ordinary segment path (the "Catalogue Brumer" prefix / ", fol. N" suffix block
# the normalized key). Indexing a normalized collection+number "core" from BOTH
# the library variants AND the mesirah citation reconciles the cross-numbering.
# Scoped to Rab./Ms. (whole-manuscript identifiers); ENA has fine-grained leaf
# numbering already resolved by the ordinary path, so coring it would over-match
# sibling leaves.
_JTS_CORE_RE = re.compile(r'\b(rab|ms)\b\s*\.?\s*(\d{1,5})', re.I)
_JTS_MS_NUM_RE = re.compile(r'\bms\b\s*\.?\s*(\d{1,5})', re.I)
_JTS_ENA_NUM_RE = re.compile(r'\bena\b\s*\.?\s*(\d{1,5})', re.I)


def jts_cores(text):
    """{collection+number} NAMED cores in a JTS shelfmark string
    ('Catalogue Brumer Rab. 2148 | Ms. 9926' -> {'rab2148', 'ms9926'};
    'Ms. 4391, fol. 121' -> {'ms4391'}). Named = the collection word is present,
    so the citation is unambiguous and safe to match verbatim."""
    return {m.group(1).lower() + m.group(2)
            for m in _JTS_CORE_RE.finditer(text or '')}


def jts_mesirah_cores(raw_segs):
    """Cores for a JTS mesirah citation. `raw_segs` is the RAW (pre-clean_segs)
    classmark tail so a Hebrew catalogue annotation can't drop the number.
    A NAMED Rab./Ms. number -> its verbatim core. A BARE leading number carrying
    NO collection word (e.g. '4391 (שטיינשניידר 19: 8 א)') is emitted as a
    'jtsbare<n>' key, which build_indexes resolves ONLY for numbers whose Ms./ENA
    forms name the same manuscript — ambiguous bare numbers (the 208 disjoint
    Ms.N != ENA N cases) get no key and safely match nothing (per the
    'do NOT bare-number match / buries discoveries' constraint)."""
    joined = ' '.join(raw_segs)
    cores = jts_cores(joined)
    if not cores and not re.search(r'\b(ena|rab|ms)\b', joined, re.I):
        for s in raw_segs:
            m = re.match(r'^\s*(\d{2,5})\b', s.strip())
            if m:
                cores.add('jtsbare' + m.group(1))
                break
    return cores


def build_indexes():
    """main index: exact normalized classmark -> {(sys_id, lib)};
    base index: classmark minus trailing leaf number -> {(sys_id, lib)}."""
    main_index = defaultdict(set)
    base_index = defaultdict(set)
    base_re = re.compile(r'^(.*[A-Za-z].*?)[\s.,/]+(\d{1,4}[A-Za-z]?)$')
    jts_ms = defaultdict(set)     # bare number -> sys_ids seen as 'Ms. N'
    jts_ena = defaultdict(set)    # bare number -> sys_ids seen as 'ENA N'
    n_rows = 0
    with open(LIBCSV, encoding='utf-8-sig', newline='') as f:
        rdr = csv.reader(f)
        header = next(rdr)
        for r in rdr:
            if len(r) < 4 or not r[0].strip():
                continue
            sys_id, variants, lib = r[0].strip(), r[2], r[3].strip()
            if not variants:
                continue
            n_rows += 1
            ent = (sys_id, lib)
            for v in variants.split('|'):
                v = v.strip()
                if not v:
                    continue
                nv = norm(v)
                if not nv:
                    continue
                for k in variant_keys(nv):
                    main_index[k].add(ent)
                m = base_re.match(v)
                if m:
                    nb = norm(m.group(1))
                    if nb:
                        for k in variant_keys(nb):
                            base_index[k].add(ent)
            if lib == 'JTS':
                for c in jts_cores(variants):        # named Rab./Ms. cores
                    main_index[c].add(ent)
                # per-variant bare-number tallies (a variant with 'ENA' is an
                # ENA form even when it also spells 'Ms.', e.g. 'Ms. ENA 2757')
                for v in variants.split('|'):
                    ena = _JTS_ENA_NUM_RE.findall(v)
                    for n in ena:
                        jts_ena[n].add(sys_id)
                    if not ena:
                        for n in _JTS_MS_NUM_RE.findall(v):
                            jts_ms[n].add(sys_id)
    # bare-number resolution: only for numbers whose Ms./ENA forms co-refer (same
    # sys_id) or where one form is absent. Disjoint Ms.N != ENA N numbers are
    # ambiguous -> no key -> a bare citation abstains instead of mis-mapping.
    for n in set(jts_ms) | set(jts_ena):
        ms_s, ena_s = jts_ms.get(n, set()), jts_ena.get(n, set())
        if not ms_s or not ena_s or (ms_s & ena_s):
            for sid in ms_s | ena_s:
                main_index['jtsbare' + n].add((sid, 'JTS'))
    return main_index, base_index, n_rows


# ---------------------------------------------------------------- extraction
def extract_mesirot():
    """-> list of (work_id, filename, [unique raw mesirah strings])"""
    out = []
    n_files = 0
    for fn in sorted(os.listdir(MAAGARIM)):
        if not fn.endswith('.txt'):
            continue
        n_files += 1
        raw = open('\\\\?\\' + os.path.join(MAAGARIM, fn), encoding='utf-8',
                   errors='replace').read()
        found = MES_RE.findall(raw)
        if not found:
            continue
        base = fn.replace('.txt-OnlyText.txt', '')
        work_id = 'M:' + base.split('--')[-1]
        seen, uniq = set(), []
        for s in found:
            s = s.strip()
            if s and s not in seen:
                seen.add(s)
                uniq.append(s)
        out.append((work_id, fn, uniq, len(found)))
    return out, n_files


# ---------------------------------------------------------------- parsing
NONGEN_FIRST = (
    'sassoon', 'qumran', 'livorno', 'parma', 'vatican', 'munich', 'madrid',
    'leiden', 'rome', 'roma', 'nimes', 'copenhagen', 'berlin', 'leipzig',
    'frankfurt', 'milano', 'milan', 'firenze', 'florence', 'torino', 'turin',
    'venezia', 'venice', 'amsterdam', 'hamburg', 'dresden', 'breslau',
    'warsaw', 'unknown', 'mur', 'mas', 'nahal', '5/6hev', 'xhev', 'ein gedi',
    'jericho', 'sdeir', 'private',
)

ANNOT_WORDS = ('according', 'manuscript', 'לפי', 'edition', 'version')
HEB_RE = re.compile(r'[֐-׿]')
BIDI_RE = re.compile(r'[‎‏‪-‮﻿]')


def clean_raw(s: str) -> str:
    s = html.unescape(s)
    s = BIDI_RE.sub('', s)
    if '>' in s:                       # embedded XML garbage: "|FL..." ... >Munich, ...
        s = s.split('>')[-1]
    return s.strip().strip(';').strip()


def seg_alternatives(seg: str):
    """'181 (148)' -> ['181 (148)', '181', '148'] (as-is form FIRST — for
    shelfmarks where the parens are structural, e.g. T-S 'Ar. 18(2)');
    plain seg -> [seg]."""
    m = re.match(r'^(.*?)\s*\(([^()]*)\)\s*$', seg)
    if m and m.group(1).strip() and m.group(2).strip():
        return [seg, m.group(1).strip(), m.group(2).strip()]
    return [seg]


def clean_segs(segs):
    out = []
    for t in segs:
        t = t.strip()
        if not t:
            continue
        low = t.lower()
        if HEB_RE.search(t):
            continue
        if any(w in low for w in ANNOT_WORDS):
            continue
        out.append(t)
    return out


class Parsed:
    __slots__ = ('hint', 'code', 'prefix', 'rest', 'reason', 'cores')

    def __init__(self, hint=None, code=None, prefix='', rest=None, reason=None,
                 cores=None):
        self.hint, self.code, self.prefix = hint, code, prefix
        self.rest = rest or []
        self.reason = reason
        self.cores = cores or set()


def parse_location(s: str) -> Parsed:
    """Split a cleaned mesirah location string into (library hint, expected
    library_code, collection prefix, remaining classmark segments)."""
    if not re.search(r'[A-Za-z]', s):
        return Parsed(reason='non_latin_or_printed')
    if s.startswith('['):
        return Parsed(reason='printed_edition')
    segs = [t.strip() for t in s.split(',') if t.strip()]
    if not segs:
        return Parsed(reason='empty')
    low0 = segs[0].lower()

    def L(i):
        return segs[i].lower() if len(segs) > i else ''

    if low0.startswith(NONGEN_FIRST) or re.match(r'^\d+q', low0):
        return Parsed(hint=segs[0], reason='non_genizah_location')

    if low0.startswith('cambridge'):
        if 'lewis-gibson' in L(1) or 'westminster' in L(1):
            rest = clean_segs(segs[2:])
            pref = ''
            if rest:
                c0 = rest[0].lower().lstrip('.')
                cmap = {'talmudica': 'talm', 'liturgica': 'lit',
                        'misc': 'misc', 'glass': 'glass'}
                for k, v in cmap.items():
                    if c0.startswith(k):
                        pref, rest = v, rest[1:]
                        break
            return Parsed('Cambridge Lewis-Gibson (Westminster)',
                          ('Westminster', 'CUL'), pref, rest)
        if 'university library' in L(1):
            rest = clean_segs(segs[2:])
            pref = ''
            if rest and rest[0].lower().replace('-', '').replace(' ', '') \
                    .startswith('ts'):
                pref, rest = 'ts', rest[1:]
            return Parsed('Cambridge University Library', 'CUL', pref, rest)
        return Parsed('Cambridge University Library', 'CUL', '',
                      clean_segs(segs[1:]))

    if low0.startswith('new york'):
        if 'jewish theological' in L(1):
            # 'ENA' / 'ENA NS' / 'Rab.' stay as ordinary segments — the
            # index variants carry them too ('ENA 1501.1', 'ENA NS 19.3').
            # JTS multi-numbering: also emit collection+number cores so a
            # Rab./Ms./bare number cited by Maagarim resolves against the
            # cross-numbered libraries.csv variant (from the RAW tail so a
            # Hebrew catalogue annotation doesn't strip the number).
            return Parsed('New York JTS', 'JTS', '', clean_segs(segs[2:]),
                          cores=jts_mesirah_cores(segs[2:]))
        return Parsed('New York ' + (segs[1] if len(segs) > 1 else ''),
                      None, '', [], 'non_genizah_location')

    if low0.startswith('philadelphia'):
        rest = [t for t in segs[1:]
                if 'center for advanced' not in t.lower()
                and 'dropsie' not in t.lower()
                and 'university of pennsylvania' not in t.lower()
                and 'annenberg' not in t.lower()]
        rest = clean_segs(rest)
        pref = ''
        if rest and rest[0].lower().startswith('halper'):
            pref, rest = 'halper', rest[1:]
        return Parsed('Philadelphia Katz (Halper)', 'Katz', pref, rest)

    if low0.startswith('oxford'):
        if 'bodleian' not in L(1):
            return Parsed('Oxford ' + (segs[1] if len(segs) > 1 else ''),
                          None, '', [], 'non_genizah_location')
        rest = clean_segs(segs[2:])
        if not rest:
            return Parsed('Oxford Bodleian', 'Oxford', '', [],
                          'no_classmark')
        first = rest[0]
        # 'Heb. c. 57' / 'MS Heb. e 32'
        m = re.match(r'^(?:ms\.?\s*)?heb\.?\s*([a-g])\.?\s*(\d+[a-z]?)$',
                     first, re.I)
        if m:
            return Parsed('Oxford Bodleian', 'Oxford',
                          'heb' + m.group(1).lower() + m.group(2).lower(),
                          rest[1:])
        # 'c.28 (2876)' / 'e.37 (2710)' — letter.vol with Neubauer in parens
        m = re.match(r'^([a-g])\.?\s*(\d+[a-z]?)\s*(?:\(\d+[a-z]?\))?$',
                     first, re.I)
        if m:
            return Parsed('Oxford Bodleian', 'Oxford',
                          'heb' + m.group(1).lower() + m.group(2).lower(),
                          rest[1:])
        if re.match(r'^\d+[a-z]?$', first, re.I):
            # bare Neubauer catalogue number — not resolvable to a shelfmark
            return Parsed('Oxford Bodleian', 'Oxford', '', [],
                          'oxford_neubauer_only')
        return Parsed('Oxford Bodleian', 'Oxford', '', rest)

    if low0.startswith('london'):
        if 'british library' in L(1) or 'british museum' in L(1):
            rest = clean_segs(segs[2:])
            if rest and re.match(r'^\d+[a-z]?$', rest[0], re.I):
                # bare Margoliouth catalogue number
                return Parsed('London British Library', 'BL', '', [],
                              'bl_catalog_number_only')
            return Parsed('London British Library', 'BL', '', rest)
        return Parsed('London ' + (segs[1] if len(segs) > 1 else ''),
                      None, '', [], 'non_genizah_location')

    if low0.startswith('manchester'):
        rest = clean_segs([t for t in segs[1:]
                           if 'rylands' not in t.lower()])
        pref = ''
        if rest and rest[0].lower().startswith('gaster'):
            pref = 'gaster'
            r0 = rest[0][len('Gaster'):].strip(' .')
            rest = ([r0] if r0 else []) + rest[1:]
        return Parsed('Manchester John Rylands', 'Manchester', pref, rest)

    if low0.startswith('mosseri'):
        return Parsed('Mosseri Collection', 'Mosseri', 'moss',
                      clean_segs(segs[1:]))

    if low0.startswith('budapest'):
        rest = clean_segs([t for t in segs[1:]
                           if 'magyar' not in t.lower()
                           and 'kaufmann' not in t.lower()
                           and t.lower().strip('.') != 'geniza'])
        return Parsed('Budapest HAS (Kaufmann Geniza)', 'HAS',
                      'kaufmanngen', rest)

    if low0.startswith('st. petersburg') or low0.startswith('st petersburg') \
            or low0.startswith('saint petersburg'):
        rest_of_1 = ''
        code, hint = None, 'St. Petersburg'
        l1 = L(1)
        if 'russian national library' in l1:
            code, hint = 'RNL', 'St. Petersburg RNL'
            rest_of_1 = re.sub(r'^russian national library',
                               '', segs[1], flags=re.I).strip(' ,')
        elif 'institute of oriental' in l1:
            code, hint = 'IOM', 'St. Petersburg IOM'
        elif 'harkavy' in l1:
            return Parsed("St. Petersburg Harkavy's Ms.",
                          ('Harkavy', 'RNL'), '', clean_segs(segs[2:]))
        else:
            return Parsed('St. Petersburg ' + (segs[1] if len(segs) > 1 else ''),
                          None, '', [], 'unrecognized_institution')
        rest = clean_segs(([rest_of_1] if rest_of_1 else []) + segs[2:])
        return Parsed(hint, code, '', rest)

    if low0.startswith('moscow'):
        rest = clean_segs([t for t in segs[1:]
                           if 'russian state library' not in t.lower()])
        pref = ''
        if rest and rest[0].lower().startswith('guenzburg'):
            pref = 'guenzburg'
            r0 = rest[0][len('Guenzburg'):].strip(' .')
            rest = ([r0] if r0 else []) + rest[1:]
        return Parsed('Moscow RSL (Guenzburg)', 'RSL', pref, rest)

    if low0.startswith('paris'):
        if 'alliance' in L(1):
            return Parsed('Paris AIU', 'AIU', '', clean_segs(segs[2:]))
        return Parsed('Paris ' + (segs[1] if len(segs) > 1 else ''),
                      None, '', [], 'non_genizah_location')

    if low0.startswith('wien') or low0.startswith('vienna'):
        rest = clean_segs([t for t in segs[1:]
                           if 'nationalbibliothek' not in t.lower()
                           and 'national library' not in t.lower()])
        return Parsed('Vienna ONB (Rainer)', 'Vienna', '', rest)

    if low0.startswith('strasbourg'):
        return Parsed('Strasbourg', 'Strasbourg', '', clean_segs(segs[1:]))
    if low0.startswith('geneva') or low0.startswith('geneve'):
        return Parsed('Geneva', 'Geneva', '', clean_segs(segs[1:]))
    if low0.startswith('washington'):
        rest = clean_segs([t for t in segs[1:]
                           if 'smithsonian' not in t.lower()
                           and 'freer' not in t.lower()])
        return Parsed('Washington Freer', 'Freer', '', rest)
    if low0.startswith('cincinnati'):
        return Parsed('Cincinnati HUC', 'HUC', '',
                      clean_segs([t for t in segs[1:]
                                  if 'hebrew union college' not in t.lower()]))
    if low0.startswith('birmingham'):
        return Parsed('Birmingham (Mingana)', 'Birmingham', '',
                      clean_segs(segs[1:]))
    if low0.startswith('jerusalem'):
        if 'national library' in L(1):
            return Parsed('Jerusalem NLI', 'NLI', '', clean_segs(segs[2:]))
        return Parsed('Jerusalem ' + (segs[1] if len(segs) > 1 else ''),
                      None, '', [], 'non_genizah_location')

    return Parsed(segs[0], None, '', [], 'unrecognized_institution')


def build_candidates(p: Parsed):
    """-> (candidate keys, exact-only keys, classmark display string).
    Handles trailing leaf ranges ('1-7') by leaf-by-leaf expansion and
    parenthesized alternatives ('181 (148)'). Bare collection/volume keys
    produced as a side effect of range expansion are exact-only (a base-index
    hit on e.g. 'antoninb' or 'ts12' would fan out to a whole box).

    JTS collection+number cores (p.cores) are prepended as exact candidates so
    the cross-numbering leaks resolve — they stand alone even when the classmark
    tail was Hebrew-only (rest empty)."""
    cores = sorted(getattr(p, 'cores', None) or ())
    if not p.rest and not p.prefix:
        return cores, set(), set(), (', '.join(cores) if cores else '')
    segs = p.rest
    display = (p.prefix + ' ' + ', '.join(segs)).strip()
    rng = None
    if segs:
        m = RANGE_RE.match(segs[-1])
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            if a < b and (b - a) < MAX_RANGE:
                rng = (a, b)
                segs = segs[:-1]
    alt_lists = [seg_alternatives(t) for t in segs]
    combos = list(itertools.islice(itertools.product(*alt_lists), 8)) \
        if alt_lists else [()]
    cands, exact_only, low_cap = [], set(), set()
    for combo in combos:
        base = norm(p.prefix + ''.join(combo))
        if rng:
            for i in range(rng[0], rng[1] + 1):
                k = base + str(i)
                if len(k) >= 3:
                    cands.append(k)
            if len(base) >= 3:          # whole-volume record, exact-only
                cands.append(base)
                exact_only.add(base)
        else:
            if len(base) >= 3 and not base.isdigit():
                cands.append(base)
                # side/part letter fallback: 'ena34433a' -> 'ena34433'
                # (folio side designator). Not an exact classmark match, so
                # capped at low confidence.
                if re.search(r'\d[a-z]$', base) and len(base) > 3:
                    cands.append(base[:-1])
                    low_cap.add(base[:-1])
    # dedupe preserving order (cores first — tried as exact keys)
    seen, out = set(), []
    for c in cores + cands:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out, exact_only, low_cap, display


def match_candidates(cands, exact_only, low_cap, expected,
                     main_index, base_index):
    """-> dict sys_id -> (library_code, confidence).
    Two-phase: exact lookups over ALL candidates first; base/box-level
    lookups only if NO candidate produced an exact hit (keeps a genuine
    exact match from being drowned in box-level fan-out noise)."""
    results = {}
    RANK = {'high': 0, 'low': 1, 'ambiguous': 2}
    expected_set = ({expected} if isinstance(expected, str)
                    else set(expected or ()))

    def absorb(hits, via):
        by_sys = {}
        for sid, lib in hits:
            by_sys.setdefault(sid, lib)
        many = len(by_sys) > AMBIG_N
        for sid, lib in by_sys.items():
            if many:
                conf = 'ambiguous'
            elif via != 'exact':
                conf = 'low'
            elif lib in expected_set:
                conf = 'high'
            else:
                conf = 'low'
            old = results.get(sid)
            if old is None or RANK[conf] < RANK[old[1]]:
                results[sid] = (lib, conf)

    exact_found = False
    for c in cands:
        hits = main_index.get(c)
        if hits:
            exact_found = True
            absorb(hits, 'exact' if c not in low_cap else 'exact-lowcap')
    if not exact_found:
        for c in cands:
            if c in exact_only:
                continue
            hits = base_index.get(c)
            if hits:
                absorb(hits, 'base')
    return results


# ---------------------------------------------------------------- main
def main():
    t0 = time.time()
    print('building shelfmark indexes from libraries.csv ...', flush=True)
    main_index, base_index, n_lib_rows = build_indexes()
    print(f'  {n_lib_rows:,} rows -> {len(main_index):,} exact keys, '
          f'{len(base_index):,} base keys ({time.time()-t0:.0f}s)', flush=True)

    print('extracting mesirah headers from Maagarim ...', flush=True)
    per_work, n_files = extract_mesirot()
    total_occurrences = sum(n for _, _, _, n in per_work)
    total_uniq = sum(len(u) for _, _, u, _ in per_work)
    print(f'  {n_files:,} files scanned; {len(per_work):,} works with mesirah; '
          f'{total_occurrences:,} header occurrences; '
          f'{total_uniq:,} unique (work,string) pairs', flush=True)

    rows = []
    parse_ok = parse_known_nonmatchable = parse_fail = total_parts = 0
    unmatched_examples = defaultdict(list)
    conf_counter = Counter()
    for work_id, fn, uniq, _n in per_work:
        for raw in uniq:
            cleaned = clean_raw(raw)
            # a single header may carry several locations separated by ';'
            parts = [x.strip(' ,') for x in cleaned.split(';') if x.strip(' ,')]
            for s in parts or ['']:
                total_parts += 1
                p = parse_location(s)
                base_row = {
                    'work_id': work_id,
                    'raw': raw,
                    'library_hint': p.hint,
                }
                if p.reason in ('non_latin_or_printed', 'printed_edition',
                                'non_genizah_location', 'oxford_neubauer_only',
                                'bl_catalog_number_only', 'no_classmark',
                                'empty'):
                    parse_known_nonmatchable += 1
                    rows.append({**base_row, 'classmark': None, 'sys_id': None,
                                 'library_code': None, 'confidence': None,
                                 'reason': p.reason})
                    if len(unmatched_examples[p.reason]) < 12:
                        unmatched_examples[p.reason].append(s)
                    continue
                if p.reason == 'unrecognized_institution':
                    parse_fail += 1
                    rows.append({**base_row, 'classmark': None, 'sys_id': None,
                                 'library_code': None, 'confidence': None,
                                 'reason': p.reason})
                    if len(unmatched_examples[p.reason]) < 12:
                        unmatched_examples[p.reason].append(s)
                    continue
                parse_ok += 1
                cands, exact_only, low_cap, display = build_candidates(p)
                matched = match_candidates(cands, exact_only, low_cap, p.code,
                                           main_index, base_index) \
                    if cands else {}
                if not matched:
                    rows.append({**base_row, 'classmark': display or None,
                                 'sys_id': None, 'library_code': None,
                                 'confidence': None,
                                 'reason': 'no_index_match'})
                    if len(unmatched_examples['no_index_match']) < 12:
                        unmatched_examples['no_index_match'].append(s)
                    continue
                for sid, (lib, conf) in sorted(matched.items()):
                    conf_counter[conf] += 1
                    rows.append({**base_row, 'classmark': display,
                                 'sys_id': sid, 'library_code': lib,
                                 'confidence': conf})

    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=1)
    print(f'wrote {os.path.abspath(OUT_JSON)} ({len(rows):,} rows)', flush=True)
    print(f'  parse: ok={parse_ok:,}  known-nonmatchable='
          f'{parse_known_nonmatchable:,}  unrecognized={parse_fail:,}')
    print(f'  match confidences: {dict(conf_counter)}', flush=True)

    # ------------------------------------------------------------ validation
    pair_conf = {}          # (work_id, sys_id) -> best confidence
    RANK = {'high': 0, 'low': 1, 'ambiguous': 2}
    for r in rows:
        if r['sys_id'] and r['confidence']:
            k = (r['work_id'], r['sys_id'])
            old = pair_conf.get(k)
            if old is None or RANK[r['confidence']] < RANK[old]:
                pair_conf[k] = r['confidence']

    with open(TESTIMONIES, encoding='utf-8-sig', newline='') as f:
        trows = list(csv.DictReader(f))
    queue = [r for r in trows if r['tier'] == 'new?' and r['cls'] == 'testimony'
             and r['cat'] not in CANON_CATS]
    new_any = [r for r in trows if r['tier'] == 'new?']

    def bucket(rs):
        b = Counter()
        hits = defaultdict(list)
        for r in rs:
            c = pair_conf.get((r['work_id'], r['sys_id']))
            if c:
                b[c] += 1
                hits[c].append(r)
            else:
                b['none'] += 1
        return b, hits

    q_bucket, q_hits = bucket(queue)
    a_bucket, _ = bucket(new_any)
    print(f'new? testimony queue: {len(queue):,} rows -> {dict(q_bucket)}')
    print(f'new? all-cls set:     {len(new_any):,} rows -> {dict(a_bucket)}',
          flush=True)

    # match stats against ALL testimony rows (context)
    all_bucket, _ = bucket(trows)

    # ------------------------------------------------------------ leak check
    cards = json.load(open(CARDS, encoding='utf-8'))
    leak_pairs = []
    for c in cards:
        cid = str(c.get('id', ''))
        if not cid.startswith('new|'):
            continue
        wid = (c.get('work') or {}).get('work_id', '')
        if wid in LEAK_WORKS:
            leak_pairs.append((wid, c['sys_id'], c.get('shelf', '')))
    leak_results = []
    for wid, sid, shelf in leak_pairs:
        conf = pair_conf.get((wid, sid))
        leak_results.append((wid, sid, shelf, conf))
        print(f'  leak {wid} / {sid} ({shelf}): '
              f'{conf if conf else "MISSED"}')

    # ------------------------------------------------------------ report
    def sample_rows(conf, n=10):
        out = [r for r in rows if r['confidence'] == conf]
        # spread across distinct works for variety
        seen_w, picked = set(), []
        for r in out:
            if r['work_id'] not in seen_w:
                seen_w.add(r['work_id'])
                picked.append(r)
            if len(picked) >= n:
                break
        return picked

    md = []
    md.append('# Mesirah witness cross-check (SEED-029)')
    md.append('')
    md.append(f'Generated by `scripts/mesirah_witnesses.py` on '
              f'{time.strftime("%Y-%m-%d %H:%M")}.')
    md.append('')
    md.append('Maagarim edition files record the source manuscript of each '
              'edited work in `##המסירה: ...##` headers (also section-scoped '
              '`##סעיף N | המסירה: ...##` — the plain-form regex in '
              '`track1_build_ref.py` misses these; e.g. Ytext689001\'s '
              'ENA 1501, 1-7 exists ONLY in section-scoped headers). A '
              '"new?" discovery whose (work, manuscript) pair appears here '
              'is the *known source witness*, not a discovery.')
    md.append('')
    md.append('## Extraction stats')
    md.append('')
    md.append(f'- Maagarim files scanned: **{n_files:,}**')
    md.append(f'- Files with >=1 mesirah header: **{len(per_work):,}**')
    md.append(f'- Total mesirah header occurrences: **{total_occurrences:,}**')
    md.append(f'- Unique (work, location-string) pairs: **{total_uniq:,}** '
              f'(-> {total_parts:,} single-location parts after '
              f"';'-splitting)")
    pr = 100.0 * (parse_ok + parse_known_nonmatchable) / max(1, total_parts)
    md.append(f'- Parsed (institution recognized or classified '
              f'non-matchable): **{parse_ok + parse_known_nonmatchable:,} '
              f'({pr:.1f}%)** — of which {parse_ok:,} candidate-bearing, '
              f'{parse_known_nonmatchable:,} known non-matchable '
              f'(printed editions, European codices, Qumran, bare catalogue '
              f'numbers), {parse_fail:,} unrecognized institutions')
    md.append('')
    md.append('## Match stats')
    md.append('')
    md.append(f'- Output rows in `data/mesirah_witnesses.json`: '
              f'**{len(rows):,}**')
    n_unmatched = sum(1 for r in rows if r['sys_id'] is None)
    md.append(f'- Matched rows: high **{conf_counter["high"]:,}**, '
              f'low **{conf_counter["low"]:,}**, '
              f'ambiguous **{conf_counter["ambiguous"]:,}**; '
              f'unmatched strings **{n_unmatched:,}**')
    md.append(f'- Distinct (work_id, sys_id) pairs: **{len(pair_conf):,}** '
              f'(high '
              f'{sum(1 for v in pair_conf.values() if v == "high"):,}, low '
              f'{sum(1 for v in pair_conf.values() if v == "low"):,}, '
              f'ambiguous '
              f'{sum(1 for v in pair_conf.values() if v == "ambiguous"):,})')
    md.append('')
    md.append('Confidence tiers: **high** = normalized classmark matches a '
              'libraries.csv call-number variant exactly AND library_code '
              'agrees with the parsed library hint (ranges like "ENA, 1501, '
              '1-7" are expanded leaf-by-leaf; each leaf is an exact match). '
              '**low** = string match without library agreement, or only a '
              'base/box-level match. **ambiguous** = classmark resolves to '
              f'>{AMBIG_N} sys_ids — never auto-demote.')
    md.append('')
    md.append('## Discovery-queue demotion (validation)')
    md.append('')
    md.append(f'- Committed queue: `results/track1_full_testimonies.csv`, '
              f"tier=='new?' & cls=='testimony' & cat not canonical: "
              f'**{len(queue):,}** rows')
    md.append(f'- Would DEMOTE at high confidence: '
              f'**{q_bucket["high"]:,}**')
    md.append(f'- Flag-only at low confidence: **{q_bucket["low"]:,}**')
    md.append(f'- Flag-only at ambiguous: **{q_bucket["ambiguous"]:,}**')
    md.append(f'- No mesirah relation: **{q_bucket["none"]:,}**')
    md.append('')
    md.append(f"- Wider tier=='new?' set (incl. cls=='partial'): "
              f'{len(new_any):,} rows -> high {a_bucket["high"]:,}, '
              f'low {a_bucket["low"]:,}, ambiguous {a_bucket["ambiguous"]:,}')
    md.append(f'- All testimonies file ({len(trows):,} rows) sharing a '
              f'mesirah pair: high {all_bucket["high"]:,}, '
              f'low {all_bucket["low"]:,}, '
              f'ambiguous {all_bucket["ambiguous"]:,} '
              f'(sanity: known-source rows concentrate outside the new? tier)')
    md.append('')
    if q_bucket['high']:
        md.append('### new? queue rows demoted at high confidence')
        md.append('')
        md.append('| work_id | sys_id | shelfmark | lib |')
        md.append('|---|---|---|---|')
        for r in q_hits['high'][:40]:
            md.append(f"| {r['work_id']} | {r['sys_id']} | "
                      f"{r['shelfmark']} | {r['lib']} |")
        if len(q_hits['high']) > 40:
            md.append(f'| ... | ({len(q_hits["high"]) - 40} more) | | |')
        md.append('')
    md.append('## Known-leak verification (Hillel grading, 6/34 leaks)')
    md.append('')
    md.append('Exact (work_id, sys_id) pairs taken from '
              '`review/discovery_cards.json` cards with ids starting `new|` '
              'for the flagged works:')
    md.append('')
    md.append('| work_id | sys_id | card shelfmark | table confidence |')
    md.append('|---|---|---|---|')
    for wid, sid, shelf, conf in leak_results:
        md.append(f'| {wid} | {sid} | {shelf} | '
                  f'{conf if conf else "**MISSED**"} |')
    md.append('')
    n_caught = sum(1 for *_x, c in leak_results if c == 'high')
    md.append(f'**{n_caught}/{len(leak_results)} leak pairs caught at high '
              'confidence.**')
    md.append('')
    md.append('## Example rows per tier')
    for conf in ('high', 'low', 'ambiguous'):
        md.append('')
        md.append(f'### {conf}')
        md.append('')
        md.append('| work_id | raw (mesirah) | classmark | sys_id | lib |')
        md.append('|---|---|---|---|---|')
        for r in sample_rows(conf):
            raw_disp = r['raw'][:80].replace('|', '\\|')
            md.append(f"| {r['work_id']} | {raw_disp} | {r['classmark']} | "
                      f"{r['sys_id']} | {r['library_code']} |")
    md.append('')
    md.append('## Unmatched location examples')
    for reason in ('non_genizah_location', 'printed_edition',
                   'non_latin_or_printed', 'oxford_neubauer_only',
                   'bl_catalog_number_only', 'unrecognized_institution',
                   'no_index_match'):
        exs = unmatched_examples.get(reason)
        if not exs:
            continue
        md.append('')
        md.append(f'### {reason}')
        md.append('')
        for e in exs[:10]:
            md.append(f'- `{e[:120]}`')
    md.append('')

    os.makedirs(os.path.dirname(OUT_MD), exist_ok=True)
    with open(OUT_MD, 'w', encoding='utf-8') as f:
        f.write('\n'.join(md))
    print(f'wrote {os.path.abspath(OUT_MD)}')
    print(f'done in {time.time()-t0:.0f}s')


if __name__ == '__main__':
    main()
