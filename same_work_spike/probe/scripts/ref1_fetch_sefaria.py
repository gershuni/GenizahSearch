# -*- coding: utf-8 -*-
"""REF-1 Stage 1: targeted reference-text acquisition from the Sefaria API.

SEED-029 spike (see ../SPIKE-BRIEFS-2026-07-08.md, "REF-1" section). Fetches:
  (a) Targum Onkelos (Torah, 5 books) + Targum (Pseudo-)Jonathan (Torah + Nevi'im,
      26 books) + Aramaic Targum to Writings (Ketuvim, ~10 books)
  (b) statutory liturgy core (Weekday+Shabbat Amidah, Birkat Hamazon, Shema
      blessings, Kiddush, Hallel, Haggadah) -- tagged ref_kind=modern_rite_mask_only
  (c) verified B2 catalog-gap matches confirmed present on Sefaria (see the
      hand-curated report; this script only fetches the CONFIRMED hits)

Writes ONE plain-text body file + ONE *.versemap.json sidecar per work into
../refs_staging/, plus a single manifest.json with per-file provenance/license
metadata. Does NOT touch ref_corpus.pkl or track1_build_ref.py (Stage-2 concern).

Body-cleaning policy (per the brief): strip HTML tags; drop <small>/<sup>/
footnote-class <i> blocks entirely (Sefaria siddur editions use <small> for
publisher rubrics/kavanah instructions -- e.g. "בלחש:"/"בקול:" -- which are NOT
prayer text); then keep ONLY Hebrew base letters (א-ת, finals NOT
folded -- this is for human/downstream reading, unlike normalize.norm_stream's
matching-oriented stream) plus single spaces; maqaf (־) is mapped to a
space (readability); everything else (nikud/te'amim -- Mn category, always
outside the Hebrew-letter block -- geresh/gershayim/quotes, digits, Latin,
other punctuation) is dropped by the whitelist. No verse/chapter labels ever
enter the body; verse/leaf boundaries go into the *.versemap.json sidecar only.

Run: python -X utf8 -u ref1_fetch_sefaria.py [--only KEY] [--dry-run] [--list]
Working dir expectation: same_work_spike/probe/scripts/
"""
import argparse
import html
import json
import os
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request

SEF_BASE = 'https://www.sefaria.org'
STAGING = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'refs_staging'))
RETRIEVAL_DATE = '2026-07-09'
USER_AGENT = 'GenizahSearch-SEED029-REF1/1.0 (research use; contact gershuni@dicta.org.il)'
RATE_SLEEP = 1.0  # polite ~1 req/sec

os.makedirs(STAGING, exist_ok=True)

_last_call_ts = [0.0]


def _sleep_gate():
    now = time.time()
    wait = RATE_SLEEP - (now - _last_call_ts[0])
    if wait > 0:
        time.sleep(wait)


def http_get_json(url, tries=3):
    last_err = None
    for attempt in range(tries):
        _sleep_gate()
        req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = resp.read()
            _last_call_ts[0] = time.time()
            return json.loads(data.decode('utf-8'))
        except urllib.error.HTTPError as e:
            _last_call_ts[0] = time.time()
            body = e.read().decode('utf-8', errors='replace')
            try:
                return json.loads(body)
            except Exception:
                last_err = f'HTTP {e.code}: {body[:300]}'
        except Exception as e:
            _last_call_ts[0] = time.time()
            last_err = str(e)
        time.sleep(1.5 * (attempt + 1))
    return {'error': f'request failed after {tries} tries: {last_err}'}


def v3_fetch(ref, version=None):
    q = 'version=' + urllib.parse.quote(version) if version else 'version=source'
    url = f'{SEF_BASE}/api/v3/texts/{urllib.parse.quote(ref)}?{q}'
    return http_get_json(url)


# ------------------------- license helpers -------------------------

_LICENSE_RANK = {
    'public domain': 0,
    'cc0': 1,
    'cc-by': 2,
    'cc-by-sa': 3,
    'cc-by-nc': 4,
    'cc-by-nc-sa': 5,
    'cc-by-nc-nd': 6,
}

_LICENSE_URL = {
    'public domain': None,
    'cc0': 'https://creativecommons.org/publicdomain/zero/1.0/',
    'cc-by': 'https://creativecommons.org/licenses/by/4.0/',
    'cc-by-sa': 'https://creativecommons.org/licenses/by-sa/4.0/',
    'cc-by-nc': 'https://creativecommons.org/licenses/by-nc/4.0/',
    'cc-by-nc-sa': 'https://creativecommons.org/licenses/by-nc-sa/4.0/',
    'cc-by-nc-nd': 'https://creativecommons.org/licenses/by-nc-nd/4.0/',
}


def license_rank(lic):
    return _LICENSE_RANK.get((lic or '').strip().lower(), 99)


def reuse_ok_for(lic):
    r = license_rank(lic)
    if r <= 3:
        return 'yes'
    if r <= 6:
        return 'unclear'  # NC/ND -- commercial-use question, quarantine for human review
    return 'unclear'  # unknown/'' license -- can't clear without checking


def pick_best_hebrew_version(available_versions):
    """Return the best-license Hebrew version dict, or None."""
    he = [v for v in (available_versions or []) if v.get('language') == 'he']
    if not he:
        return None
    he.sort(key=lambda v: (license_rank(v.get('license')), not v.get('isPrimary', False)))
    return he[0]


# ------------------------- body cleaning -------------------------

HEB_MIN, HEB_MAX = 0x05D0, 0x05EA
_SMALL_RE = re.compile(r'<small[^>]*>.*?</small>', re.IGNORECASE | re.DOTALL)
_SUP_RE = re.compile(r'<sup[^>]*>.*?</sup>', re.IGNORECASE | re.DOTALL)
_FOOTNOTE_I_RE = re.compile(r'<i[^>]*footnote[^>]*>.*?</i>', re.IGNORECASE | re.DOTALL)
_TAG_RE = re.compile(r'<[^>]+>')


def clean_body(raw):
    """content-text-only cleaning: drop rubrics/footnotes/tags, nikud, geresh,
    punctuation, digits, Latin; keep Hebrew base letters + single spaces."""
    if not raw or not isinstance(raw, str):
        return ''
    t = _SMALL_RE.sub(' ', raw)
    t = _SUP_RE.sub(' ', t)
    t = _FOOTNOTE_I_RE.sub(' ', t)
    t = _TAG_RE.sub('', t)
    t = html.unescape(t)
    t = unicodedata.normalize('NFC', t)
    t = t.replace('־', ' ')  # maqaf -> space (readability; not a word char)
    out = []
    for ch in t:
        o = ord(ch)
        if HEB_MIN <= o <= HEB_MAX:
            out.append(ch)
        elif ch.isspace():
            out.append(' ')
        # else: dropped (nikud/te'amim Mn marks, geresh/gershayim/quotes,
        # digits, Latin, other punctuation -- none of these are in the
        # Hebrew-letter block so the whitelist above handles it implicitly)
    cleaned = re.sub(r' +', ' ', ''.join(out)).strip()
    return cleaned


def flatten_text(node):
    if node is None:
        return
    if isinstance(node, str):
        yield node
    elif isinstance(node, list):
        for item in node:
            yield from flatten_text(item)
    else:
        yield str(node)


# ------------------------- body/versemap builders -------------------------

def build_body_chapters(nested_text, ref_title_en):
    """Targum-style whole-book fetch: nested_text[chapter_idx][verse_idx] = str
    for textDepth==2 (Onkelos/Jonathan/Aramaic-Targum-to-Writings -- all
    verified depth 2). Some bonus/secondary works (e.g. Targum Sheni on
    Esther) are textDepth==3+ (chapter/section/verse) -- flatten anything
    below the chapter level with flatten_text() so depth doesn't matter;
    the verse number then just means "Nth leaf string in this chapter"
    for those, which is documented as a per-work transformation note.
    'Simple' verse texts -- versemap stays a flat units list (per Hillel's
    mid-flight note: no schema-node hierarchy needed here, only chapter:verse)."""
    lines = []
    units = []
    pos = 0
    for ci, chapter in enumerate(nested_text, start=1):
        verses = list(flatten_text(chapter))
        for vi, vtext in enumerate(verses, start=1):
            clean = clean_body(vtext)
            if not clean:
                continue
            start = pos
            end = pos + len(clean)
            units.append({
                'ref': f'{ref_title_en} {ci}:{vi}',
                'chapter': ci, 'verse': vi,
                'start': start, 'end': end,
            })
            lines.append(clean)
            pos = end + 1
    body = '\n'.join(lines)
    return body, {'structure': 'verse', 'units': units}


def build_body_leaves(leaf_results):
    """Liturgy/complex-text leaf concatenation.
    leaf_results: list of (leaf_label_en, leaf_label_he, leaf_ref, raw_text_node)

    Sefaria works at this granularity are CONTAINERS (e.g. the whole Siddur
    Ashkenaz) coarser than our unit inventory (e.g. individual berakhot).
    Stage 2 segments containers into unit-spans and needs the schema-node
    hierarchy to do it -- so each leaf becomes a 'section' carrying its FULL
    schema path (e.g. ["Siddur Ashkenaz","Weekday","Shacharit","Amidah",
    "Patriarchs"], derived by splitting the Sefaria ref on ', ') plus the
    section's own [start,end) span in the body, with per-paragraph offsets
    nested inside for finer granularity where useful.
    """
    lines = []
    sections = []
    pos = 0
    for label_en, label_he, leaf_ref, raw_node in leaf_results:
        schema_path = [p.strip() for p in leaf_ref.split(',')]
        section_start = pos
        paragraphs = []
        seq = 0
        for raw in flatten_text(raw_node):
            clean = clean_body(raw)
            if not clean:
                continue
            seq += 1
            start = pos
            end = pos + len(clean)
            paragraphs.append({'seq': seq, 'start': start, 'end': end})
            lines.append(clean)
            pos = end + 1
        if not paragraphs:
            continue  # this leaf contributed no text (e.g. version gap); no section emitted
        section_end = paragraphs[-1]['end']
        sections.append({
            'schema_path': schema_path,
            'ref': leaf_ref,
            'section_en': label_en, 'section_he': label_he,
            'start': section_start, 'end': section_end,
            'paragraphs': paragraphs,
        })
    body = '\n'.join(lines)
    return body, {'structure': 'hierarchical', 'sections': sections}


# ------------------------- manifest -------------------------

MANIFEST_PATH = os.path.join(STAGING, 'manifest.json')


def load_manifest():
    if os.path.exists(MANIFEST_PATH):
        with open(MANIFEST_PATH, encoding='utf-8') as f:
            return json.load(f)
    return {'retrieval_date': RETRIEVAL_DATE, 'source': 'Sefaria API (api.sefaria.org / www.sefaria.org)', 'entries': []}


def save_manifest(manifest):
    with open(MANIFEST_PATH, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=1)


def upsert_manifest_entry(manifest, entry):
    manifest['entries'] = [e for e in manifest['entries'] if e['key'] != entry['key']]
    manifest['entries'].append(entry)


def build_manifest_entry(key, title_he, title_en, sefaria_ref, version_meta, ref_kind,
                          body_len, transformation_notes, structure, quarantine_reason=None):
    lic = (version_meta or {}).get('license', 'unknown') or 'unknown'
    version_title = (version_meta or {}).get('versionTitle', '')
    version_source = (version_meta or {}).get('versionSource', '')
    attribution = (
        f"\"{title_en}\" ({title_he}), version \"{version_title}\", via Sefaria.org "
        f"(https://www.sefaria.org/{urllib.parse.quote(sefaria_ref)}). License: {lic}."
    )
    if version_source:
        attribution += f' Original source: {version_source}.'
    entry = {
        'key': key,
        'title_he': title_he,
        'title_en': title_en,
        'source_ref': sefaria_ref,
        'source_url': f'https://www.sefaria.org/{urllib.parse.quote(sefaria_ref)}',
        'version_title': version_title,
        'license': lic,
        'license_url': _LICENSE_URL.get(lic.strip().lower(), None),
        'attribution_text': attribution,
        'retrieval_date': RETRIEVAL_DATE,
        'transformation_notes': transformation_notes,
        'ref_kind': ref_kind,
        'reuse_ok': 'no' if quarantine_reason == 'no' else reuse_ok_for(lic),
        'char_count': body_len,
        'body_file': key + '.txt',
        'versemap_file': key + '.versemap.json',
        'structure': structure,
    }
    if quarantine_reason:
        entry['quarantine_note'] = quarantine_reason
    return entry


DEFAULT_TRANSFORM_NOTES = (
    'HTML tags stripped; <small>/<sup>/footnote-class <i> blocks (publisher '
    'rubrics, kavanah instructions, footnote markers) dropped entirely; '
    'nikud/te\'amim, geresh/gershayim/quotes, digits, Latin and other '
    'punctuation dropped; maqaf mapped to a space; final-letter forms '
    'preserved (not folded); verse/leaf boundaries recorded only in the '
    '*.versemap.json sidecar, never in the body text.'
)


# ------------------------- fetch orchestration -------------------------

def write_work(key, title_he, title_en, sefaria_ref, version_meta, body, versemap_payload,
               ref_kind, manifest, extra_notes='', quarantine_reason=None):
    if not body:
        print(f'  [SKIP] {key}: empty body after cleaning', flush=True)
        return False
    structure = versemap_payload.get('structure', 'flat')
    body_path = os.path.join(STAGING, key + '.txt')
    vm_path = os.path.join(STAGING, key + '.versemap.json')
    with open(body_path, 'w', encoding='utf-8', newline='\n') as f:
        f.write(body)
    sidecar = {'key': key, 'source_ref': sefaria_ref}
    sidecar.update(versemap_payload)
    with open(vm_path, 'w', encoding='utf-8') as f:
        json.dump(sidecar, f, ensure_ascii=False, indent=1)
    notes = DEFAULT_TRANSFORM_NOTES + (' ' + extra_notes if extra_notes else '')
    entry = build_manifest_entry(key, title_he, title_en, sefaria_ref, version_meta,
                                  ref_kind, len(body), notes, structure, quarantine_reason)
    upsert_manifest_entry(manifest, entry)
    print(f'  [OK] {key}: {len(body):,} chars, license={entry["license"]}, reuse_ok={entry["reuse_ok"]}, structure={structure}', flush=True)
    return True


def fetch_whole_book(target, manifest):
    key, ref = target['key'], target['ref']
    d = v3_fetch(ref, version=None)
    if 'error' in d:
        print(f'  [MISS] {key} ({ref}): {d["error"][:200]}', flush=True)
        return False
    versions = d.get('versions', [])
    available = d.get('available_versions', [])
    best = pick_best_hebrew_version(available)
    if best is None:
        print(f'  [MISS] {key} ({ref}): no Hebrew version available', flush=True)
        return False
    chosen_title = best.get('versionTitle')
    default_title = (versions[0].get('versionTitle') if versions else None)
    if chosen_title and chosen_title != default_title:
        d2 = v3_fetch(ref, version=f'hebrew|{chosen_title}')
        if 'error' in d2:
            print(f'  [WARN] {key}: re-fetch of preferred version failed, using default. {d2["error"][:150]}', flush=True)
            version_meta = versions[0] if versions else best
            text_node = version_meta.get('text')
        else:
            version_meta = d2.get('versions', [{}])[0]
            text_node = version_meta.get('text')
    else:
        version_meta = versions[0] if versions else best
        text_node = version_meta.get('text')
    if not text_node:
        print(f'  [MISS] {key} ({ref}): chosen version had no text field', flush=True)
        return False
    body, versemap = build_body_chapters(text_node, target.get('ref_title_en', ref))
    return write_work(key, target['title_he'], target['title_en'], ref, version_meta,
                       body, versemap, target['ref_kind'], manifest)


def fetch_leaf_concat(target, manifest):
    key = target['key']
    disc_ref = target['discovery_ref']
    d = v3_fetch(disc_ref, version=None)
    if 'error' in d:
        print(f'  [MISS] {key} (discovery {disc_ref}): {d["error"][:200]}', flush=True)
        return False
    available = d.get('available_versions', [])
    best = pick_best_hebrew_version(available)
    if best is None:
        print(f'  [MISS] {key}: no Hebrew version available', flush=True)
        return False
    version_param = f'hebrew|{best.get("versionTitle")}'
    leaf_results = []
    version_meta_final = None
    for label_en, label_he, leaf_ref in target['leaves']:
        d2 = v3_fetch(leaf_ref, version=version_param)
        if 'error' in d2:
            print(f'    [leaf MISS] {leaf_ref}: {d2["error"][:150]}', flush=True)
            continue
        vlist = d2.get('versions', [])
        if not vlist:
            print(f'    [leaf MISS] {leaf_ref}: no versions in response', flush=True)
            continue
        version_meta_final = vlist[0]
        leaf_results.append((label_en, label_he, leaf_ref, version_meta_final.get('text')))
    if not leaf_results:
        print(f'  [MISS] {key}: no leaves resolved', flush=True)
        return False
    body, versemap = build_body_leaves(leaf_results)
    notes = f'Concatenation of {len(leaf_results)}/{len(target["leaves"])} schema leaves under "{disc_ref}".'
    return write_work(key, target['title_he'], target['title_en'], disc_ref, version_meta_final,
                       body, versemap, target['ref_kind'], manifest, extra_notes=notes)


def fetch_single_leaf(target, manifest):
    key, ref = target['key'], target['ref']
    d = v3_fetch(ref, version=None)
    if 'error' in d:
        print(f'  [MISS] {key} ({ref}): {d["error"][:200]}', flush=True)
        return False
    available = d.get('available_versions', [])
    best = pick_best_hebrew_version(available)
    if best is None:
        print(f'  [MISS] {key}: no Hebrew version available', flush=True)
        return False
    versions = d.get('versions', [])
    default_title = versions[0].get('versionTitle') if versions else None
    if best.get('versionTitle') != default_title:
        d2 = v3_fetch(ref, version=f'hebrew|{best.get("versionTitle")}')
        version_meta = d2.get('versions', [{}])[0] if 'error' not in d2 else (versions[0] if versions else best)
    else:
        version_meta = versions[0] if versions else best
    text_node = version_meta.get('text')
    body, versemap = build_body_leaves([(target['title_en'], target['title_he'], ref, text_node)])
    return write_work(key, target['title_he'], target['title_en'], ref, version_meta,
                       body, versemap, target['ref_kind'], manifest)


# ------------------------- targets registry -------------------------

TORAH_BOOKS = [('Genesis', 'בראשית'), ('Exodus', 'שמות'), ('Leviticus', 'ויקרא'),
               ('Numbers', 'במדבר'), ('Deuteronomy', 'דברים')]

NEVIIM_BOOKS = [
    ('Joshua', 'יהושע'), ('Judges', 'שופטים'), ('I Samuel', 'שמואל א'), ('II Samuel', 'שמואל ב'),
    ('I Kings', 'מלכים א'), ('II Kings', 'מלכים ב'), ('Isaiah', 'ישעיהו'), ('Jeremiah', 'ירמיהו'),
    ('Ezekiel', 'יחזקאל'), ('Hosea', 'הושע'), ('Joel', 'יואל'), ('Amos', 'עמוס'),
    ('Obadiah', 'עובדיה'), ('Jonah', 'יונה'), ('Micah', 'מיכה'), ('Nahum', 'נחום'),
    ('Habakkuk', 'חבקוק'), ('Zephaniah', 'צפניה'), ('Haggai', 'חגי'), ('Zechariah', 'זכריה'),
    ('Malachi', 'מלאכי'),
]

TARGETS = []


def slugify(s):
    return re.sub(r'[^a-z0-9]+', '_', s.lower()).strip('_')


for book_en, book_he in TORAH_BOOKS:
    TARGETS.append({
        'key': f'targum_onkelos_{slugify(book_en)}', 'kind': 'whole_book',
        'ref': f'Onkelos {book_en}', 'ref_title_en': f'Onkelos {book_en}',
        'title_he': f'תרגום אונקלוס על {book_he}', 'title_en': f'Targum Onkelos, {book_en}',
        'ref_kind': 'edition', 'group': 'targum_torah',
    })

for book_en, book_he in TORAH_BOOKS:
    TARGETS.append({
        'key': f'targum_jonathan_{slugify(book_en)}', 'kind': 'whole_book',
        'ref': f'Targum Jonathan on {book_en}', 'ref_title_en': f'Targum Jonathan on {book_en}',
        'title_he': f'תרגום (פסאודו-)יונתן על {book_he}', 'title_en': f'Targum (Pseudo-)Jonathan, {book_en}',
        'ref_kind': 'edition', 'group': 'targum_torah',
    })

for book_en, book_he in NEVIIM_BOOKS:
    TARGETS.append({
        'key': f'targum_jonathan_{slugify(book_en)}', 'kind': 'whole_book',
        'ref': f'Targum Jonathan on {book_en}', 'ref_title_en': f'Targum Jonathan on {book_en}',
        'title_he': f'תרגום יונתן על {book_he}', 'title_en': f'Targum Jonathan, {book_en}',
        'ref_kind': 'edition', 'group': 'targum_neviim',
    })

KETUVIM_TARGUM = [
    ('psalms', 'Aramaic Targum to Psalms', 'תרגום תהלים', 'Aramaic Targum to Psalms'),
    ('proverbs', 'Aramaic Targum to Proverbs', 'תרגום משלי', 'Aramaic Targum to Proverbs'),
    ('job', 'Aramaic Targum to Job', 'תרגום איוב', 'Aramaic Targum to Job'),
    ('song_of_songs', 'Aramaic Targum to Song of Songs', 'תרגום על שיר השירים', 'Aramaic Targum to Song of Songs'),
    ('ruth', 'Aramaic Targum to Ruth', 'תרגום רות', 'Aramaic Targum to Ruth'),
    ('lamentations', 'Aramaic Targum to Lamentations', 'תרגום איכה', 'Aramaic Targum to Lamentations'),
    ('ecclesiastes', 'Aramaic Targum to Ecclesiastes', 'תרגום קהלת', 'Aramaic Targum to Ecclesiastes'),
    ('esther', 'Aramaic Targum to Esther', 'תרגום אסתר', 'Aramaic Targum to Esther (Targum Rishon)'),
    ('esther_targum_sheni', 'Targum Sheni on Esther', 'תרגום שני על אסתר', 'Targum Sheni on Esther'),
    ('chronicles_1', 'Targum of I Chronicles', 'תרגום דברי הימים א', 'Targum of I Chronicles'),
    ('chronicles_2', 'Targum of II Chronicles', 'תרגום דברי הימים ב', 'Targum of II Chronicles'),
]
for slug, ref, he, en in KETUVIM_TARGUM:
    TARGETS.append({
        'key': f'targum_ketuvim_{slug}', 'kind': 'whole_book',
        'ref': ref, 'ref_title_en': ref,
        'title_he': he, 'title_en': en,
        'ref_kind': 'edition', 'group': 'targum_ketuvim',
    })

# ---- Liturgy core (mask_only) ----

_SID = 'Siddur Ashkenaz'


def sid_leaves(path_prefix, leaf_names_en_he):
    out = []
    for en, he in leaf_names_en_he:
        full = f'{_SID}, {path_prefix}, {en}'
        out.append((en, he, full))
    return out


TARGETS.append({
    'key': 'liturgy_amidah_weekday_shacharit', 'kind': 'leaf_concat',
    'discovery_ref': f'{_SID}, Weekday, Shacharit, Amidah, Patriarchs',
    'title_he': 'עמידה לחול (שחרית)', 'title_en': 'Weekday Amidah (Shacharit)',
    'ref_kind': 'modern_rite_mask_only',
    'leaves': sid_leaves('Weekday, Shacharit, Amidah', [
        ('Patriarchs', 'אבות'), ('Divine Might', 'גבורות'), ('Holiness of God', 'קדושת השם'),
        ('Knowledge', 'דעת'), ('Repentance', 'תשובה'), ('Forgiveness', 'סליחה'),
        ('Redemption', 'גאולה'), ('Healing', 'רפואה'), ('Prosperity', 'ברכת השנים'),
        ('Gathering the Exiles', 'קבוץ גליות'), ('Justice', 'משפט'), ('Against Enemies', 'ברכת המינים'),
        ('The Righteous', 'על הצדיקים'), ('Rebuilding Jerusalem', 'בנין ירושלים'),
        ('Kingdom of David', 'מלכות בית דוד'), ('Response to Prayer', 'שומע תפילה'),
        ('Temple Service', 'עבודה'), ('Thanksgiving', 'מודים'), ('Peace', 'שים שלום'),
        ('Concluding Passage', 'אלוהי נצור'),
    ]),
})

TARGETS.append({
    'key': 'liturgy_amidah_weekday_maariv', 'kind': 'leaf_concat',
    'discovery_ref': f'{_SID}, Weekday, Maariv, Amidah, Patriarchs',
    'title_he': 'עמידה לחול (ערבית)', 'title_en': 'Weekday Amidah (Maariv)',
    'ref_kind': 'modern_rite_mask_only',
    'leaves': sid_leaves('Weekday, Maariv, Amidah', [
        ('Patriarchs', 'אבות'), ('Divine Might', 'גבורות'), ('Holiness of God', 'קדושת השם'),
        ('Knowledge', 'דעת'), ('Repentance', 'תשובה'), ('Forgiveness', 'סליחה'),
        ('Redemption', 'גאולה'), ('Healing', 'רפואה'), ('Prosperity', 'ברכת השנים'),
        ('Gathering the Exiles', 'קיבוץ גלויות'), ('Justice', 'משפט'), ('Against Enemies', 'ברכת המינים'),
        ('The Righteous', 'על הצדיקים'), ('Rebuilding Jerusalem', 'בניין ירושלים'),
        ('Kingdom of David', 'מלכות בית דוד'), ('Response to Prayer', 'שומע תפילה'),
        ('Temple Service', 'עבודה'), ('Thanksgiving', 'מודים'), ('Peace', 'שים שלום'),
        ('Concluding Passage', 'אלוהי נצור'),
    ]),
})

TARGETS.append({
    'key': 'liturgy_amidah_shabbat_maariv', 'kind': 'leaf_concat',
    'discovery_ref': f'{_SID}, Shabbat, Maariv, Amidah, Patriarchs',
    'title_he': 'עמידה לשבת (ערבית)', 'title_en': 'Shabbat Amidah (Maariv)',
    'ref_kind': 'modern_rite_mask_only',
    'leaves': sid_leaves('Shabbat, Maariv, Amidah', [
        ('Patriarchs', 'אבות'), ('Divine Might', 'גבורות'), ("Holines of God's Name", 'קדושת השם'),
        ('Sanctity of the Day', 'קדושת היום'), ('Temple Service', 'עבודה'), ('Thanksgiving', 'מודים'),
        ('Peace', 'שלום רב'), ('Concluding Passage', 'אלהי נצור'),
    ]),
})

TARGETS.append({
    'key': 'liturgy_amidah_shabbat_shacharit', 'kind': 'leaf_concat',
    'discovery_ref': f'{_SID}, Shabbat, Shacharit, Amidah, Patriarchs',
    'title_he': 'עמידה לשבת (שחרית)', 'title_en': 'Shabbat Amidah (Shacharit)',
    'ref_kind': 'modern_rite_mask_only',
    'leaves': sid_leaves('Shabbat, Shacharit, Amidah', [
        ('Patriarchs', 'אבות'), ('Divine Might', 'גבורות'), ('Kedushah', 'קדושה'),
        ('Holiness of God', 'קדושת השם'), ('Sanctity of the Day', 'קדושת היום'),
        ('Temple Service', 'עבודה'), ('Thanksgiving', 'מודים'), ('Birkat Kohanim', 'ברכת כהנים'),
        ('Peace', 'שים שלום'), ('Concluding Passage', 'אלוהי נצור'),
    ]),
})

TARGETS.append({
    'key': 'liturgy_amidah_shabbat_musaf', 'kind': 'leaf_concat',
    'discovery_ref': f'{_SID}, Shabbat, Musaf LeShabbat, Amidah, Patriarchs',
    'title_he': 'עמידה למוסף שבת', 'title_en': 'Shabbat Amidah (Musaf)',
    'ref_kind': 'modern_rite_mask_only',
    'leaves': sid_leaves('Shabbat, Musaf LeShabbat, Amidah', [
        ('Patriarchs', 'אבות'), ('Divine Might', 'גבורות'), ('Kedushah', 'קדושה'),
        ('Holiness of God', 'קדושת השם'), ('Sanctity of the Day, For Shabbat', 'קדושת היום, לשבת'),
        ('Temple Service', 'עבודה'), ('Thanksgiving', 'מודים'), ('Birkat Kohanim', 'ברכת כהנים'),
        ('Peace', 'שים שלום'), ('Concluding Passage', 'אלוהי נצור'),
    ]),
})

TARGETS.append({
    'key': 'liturgy_shema_blessings_weekday_shacharit', 'kind': 'leaf_concat',
    'discovery_ref': f'{_SID}, Weekday, Shacharit, Blessings of the Shema, Barchu',
    'title_he': 'ברכות שמע לשחרית חול', 'title_en': 'Weekday Shacharit Blessings of the Shema',
    'ref_kind': 'modern_rite_mask_only',
    'leaves': sid_leaves('Weekday, Shacharit, Blessings of the Shema', [
        ('Barchu', 'ברכו'), ('First Blessing before Shema', 'יוצר אור'),
        ('Second Blessing before Shema', 'אהבת ישראל'), ('Shema', 'שמע'),
        ('Blessing after Shema', 'גאל ישראל'),
    ]),
})

TARGETS.append({
    'key': 'liturgy_shema_blessings_weekday_maariv', 'kind': 'leaf_concat',
    'discovery_ref': f'{_SID}, Weekday, Maariv, Blessings of the Shema, Shema',
    'title_he': 'ברכות שמע לערבית חול', 'title_en': 'Weekday Maariv Blessings of the Shema',
    'ref_kind': 'modern_rite_mask_only',
    'leaves': sid_leaves('Weekday, Maariv, Blessings of the Shema', [
        ('First Blessing before Shema', 'המעריב ערבים'), ('Second Blessing before Shema', 'אהבת עולם'),
        ('Shema', 'שמע'), ('First Blessing after Shema', 'אמת ואמונה'),
        ('Second Blessing after Shema', 'השכיבנו'),
        ('Third Blessing after Shema', 'ברוך ה׳ לעולם (outside of Israel)'),
    ]),
})

TARGETS.append({
    'key': 'liturgy_shema_blessings_shabbat_maariv', 'kind': 'leaf_concat',
    'discovery_ref': f'{_SID}, Shabbat, Maariv, Blessings of the Shema, Shema',
    'title_he': 'ברכות שמע לערבית שבת', 'title_en': 'Shabbat Maariv Blessings of the Shema',
    'ref_kind': 'modern_rite_mask_only',
    'leaves': sid_leaves('Shabbat, Maariv, Blessings of the Shema', [
        ('First Blessing before Shema', 'המעריב ערבים'), ('Second Blessing before Shema', 'אהבת עולם'),
        ('Shema', 'שמע'), ('First Blessing after Shema', 'אמת ואמונה'),
        ('Second Blessing after Shema', 'השכיבינו'),
    ]),
})

TARGETS.append({
    'key': 'liturgy_kiddush_friday_night', 'kind': 'single_leaf',
    'ref': f'{_SID}, Shabbat, Shabbat Evening, Kiddush',
    'title_he': 'קידוש ליל שבת', 'title_en': 'Kiddush (Friday Night)',
    'ref_kind': 'modern_rite_mask_only',
})

TARGETS.append({
    'key': 'liturgy_kiddush_shabbat_day', 'kind': 'single_leaf',
    'ref': f'{_SID}, Shabbat, Daytime Meal, Kiddusha Rabba',
    'title_he': 'קידושא רבה (קידוש יום שבת)', 'title_en': 'Kiddush (Shabbat Day / Kiddusha Rabba)',
    'ref_kind': 'modern_rite_mask_only',
})

TARGETS.append({
    'key': 'liturgy_hallel', 'kind': 'whole_book',
    'ref': 'Hallel', 'ref_title_en': 'Hallel',
    'title_he': 'הלל', 'title_en': 'Hallel',
    'ref_kind': 'modern_rite_mask_only',
})

_BH_LEAVES = [
    ('Preliminary Psalms', 'לפני ברכת המזון'), ('Zimmun', 'זימון'),
    ('Blessing on the Food', 'ברכת הזן'), ('Blessing on the Land', 'ברכת הארץ'),
    ('Blessing on Jerusalem', 'ברכת בונה ירושלים'), ('Hatov Vehametiv', 'ברכת הטוב והמטיב'),
    ("Blessings After Other Foods, M'ainShalosh", 'ברכה מעין שלש'),
    ('Blessings After Other Foods, Bore Nefashot', 'בורא נפשות'),
    ('HaRachaman of Brit Milah', 'הרחמן לברית מילה'), ('Sheva Brachot', 'שבע ברכות'),
]
TARGETS.append({
    'key': 'liturgy_birkat_hamazon', 'kind': 'leaf_concat',
    'discovery_ref': 'Birkat Hamazon, Zimmun',
    'title_he': 'ברכת המזון', 'title_en': 'Birkat Hamazon (Grace After Meals)',
    'ref_kind': 'modern_rite_mask_only',
    'leaves': [(en, he, f'Birkat Hamazon, {en}') for en, he in _BH_LEAVES],
})

_HAG_LEAVES = [
    'Kadesh', 'Urchatz', 'Karpas', 'Yachatz',
    'Magid, Ha Lachma Anya', 'Magid, Four Questions', 'Magid, We Were Slaves in Egypt',
    'Magid, Story of the Five Rabbis', 'Magid, The Four Sons', 'Magid, Yechol Me\'rosh Chodesh',
    'Magid, In the Beginning Our Fathers Were Idol Worshipers', 'Magid, First Fruits Declaration',
    'Magid, The Ten Plagues', 'Magid, Dayenu', "Magid, Rabban Gamliel's Three Things",
    'Magid, First Half of Hallel', 'Magid, Second Cup of Wine',
    'Rachtzah', 'Motzi Matzah', 'Maror', 'Korech', 'Shulchan Orech', 'Tzafun',
    'Barech, Birkat Hamazon', 'Barech, Third Cup of Wine', 'Barech, Pour Out Thy Wrath',
    'Hallel, Second Half of Hallel', 'Hallel, Songs of Praise and Thanks', 'Hallel, Fourth Cup of Wine',
    'Nirtzah, Chasal Siddur Pesach', "Nirtzah, L'Shana HaBaa", 'Nirtzah, And It Happened at Midnight',
    'Nirtzah, Zevach Pesach', "Nirtzah, Ki Lo Na'e", 'Nirtzah, Adir Hu',
    'Nirtzah, Sefirat HaOmer', 'Nirtzah, Echad Mi Yodea', 'Nirtzah, Chad Gadya',
]
TARGETS.append({
    'key': 'liturgy_haggadah', 'kind': 'leaf_concat',
    'discovery_ref': 'Pesach Haggadah, Kadesh',
    'title_he': 'הגדה של פסח', 'title_en': 'Pesach Haggadah',
    'ref_kind': 'modern_rite_mask_only',
    'leaves': [(en, en, f'Pesach Haggadah, {en}') for en in _HAG_LEAVES],
})

# ---- B2 catalog-gap CONFIRMED hits (see ref1_acquisition_report.md for the
# full hand-checked hit-rate table over all 90 auto-labeled entries; these
# are the ones a Sefaria name/TOC lookup confirmed present) ----

TARGETS.append({
    'key': 'b2_rif_hilchot_shabbat', 'kind': 'whole_book',
    'ref': 'Rif Shabbat', 'ref_title_en': 'Rif Shabbat',
    'title_he': 'הלכות הרי"ף (שבת)', 'title_en': "Rif (Isaac Alfasi), Hilchot HaRif on Shabbat",
    'ref_kind': 'edition', 'group': 'b2_confirmed',
})

TARGETS.append({
    'key': 'b2_radak_isaiah', 'kind': 'whole_book',
    'ref': 'Radak on Isaiah', 'ref_title_en': 'Radak on Isaiah',
    'title_he': 'פירוש רד"ק לישעיה', 'title_en': 'Radak on Isaiah',
    'ref_kind': 'edition', 'group': 'b2_confirmed',
})

# b2_zohar_devarim_haazinu: DROPPED. Sefaria's "Zohar" is present but is ONE
# index addressed by traditional daf/page (e.g. "Zohar 3:279a"), not by
# parsha name -- there is no simple "Zohar, Devarim" ref. Resolving the
# B2 catalog citation (Vilna-edition pages רצב-רצט) to the matching Zohar
# daf range is a small research task in its own right; deferred to Stage 2
# (noted in the acquisition report as FOUND-BUT-NOT-STAGED).

TARGETS.append({
    'key': 'b2_keter_malkhut', 'kind': 'whole_book',
    'ref': 'Keter Malkhut', 'ref_title_en': 'Keter Malkhut',
    'title_he': 'כתר מלכות (רשב"ג/אבן גבירול)', 'title_en': "Keter Malkhut (Solomon ibn Gabirol)",
    'ref_kind': 'edition', 'group': 'b2_confirmed',
})


def dispatch(target, manifest):
    kind = target['kind']
    if kind == 'whole_book':
        return fetch_whole_book(target, manifest)
    if kind == 'leaf_concat':
        return fetch_leaf_concat(target, manifest)
    if kind == 'single_leaf':
        return fetch_single_leaf(target, manifest)
    raise ValueError(f'unknown kind {kind}')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--only', action='append', help='fetch only targets whose key contains this substring (repeatable)')
    ap.add_argument('--group', action='append', help='fetch only targets in this group (repeatable)')
    ap.add_argument('--list', action='store_true', help='list targets and exit')
    ap.add_argument('--dry-run', action='store_true', help='list what would be fetched, no network calls')
    args = ap.parse_args()

    targets = TARGETS
    if args.only:
        targets = [t for t in targets if any(o in t['key'] for o in args.only)]
    if args.group:
        targets = [t for t in targets if t.get('group') in args.group]

    if args.list or args.dry_run:
        for t in targets:
            print(t['key'], '|', t['kind'], '|', t.get('ref') or t.get('discovery_ref'))
        print(f'{len(targets)} targets', flush=True)
        return

    manifest = load_manifest()
    ok, miss = 0, 0
    t0 = time.time()
    for i, t in enumerate(targets, 1):
        print(f'[{i}/{len(targets)}] {t["key"]}', flush=True)
        try:
            success = dispatch(t, manifest)
        except Exception as e:
            print(f'  [ERROR] {t["key"]}: {e}', flush=True)
            success = False
        if success:
            ok += 1
        else:
            miss += 1
        if i % 10 == 0:
            save_manifest(manifest)
    save_manifest(manifest)
    print(f'\nDone in {time.time() - t0:.0f}s. OK={ok} MISS={miss} total={len(targets)}', flush=True)
    print(f'Manifest: {MANIFEST_PATH}', flush=True)


if __name__ == '__main__':
    main()
