"""Rebuild shared/katz_images.csv from Penn Digital Collections and the NLI crossref.

Run from the repository root with Python. --cache-dir keeps small metadata
responses for retries. The preservation HEAD request reads the original scan
filename from Penn's redirect, without downloading the TIFF or saving signed URLs.
"""

import argparse
import csv
import json
import re
import sqlite3
import tempfile
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


CATALOG = 'https://digitalcollections.library.upenn.edu'
REPOSITORY = 'https://digitalrepository.library.upenn.edu'
IMAGES = 'https://iiif-images.library.upenn.edu/iiif/3/'
UUID = r'[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}'
_local = threading.local()


def session():
    if not hasattr(_local, 'session'):
        client = requests.Session()
        client.mount('https://', HTTPAdapter(max_retries=Retry(
            total=3, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504))))
        _local.session = client
    return _local.session


def catalogue_items(query):
    items = set()
    page = 1
    while page:
        response = session().get(f'{CATALOG}/items.json', params={
            'q': query, 'per_page': 100, 'page': page,
            'sort': 'last_published_at_dtsi desc',
        }, timeout=30)
        response.raise_for_status()
        result = response.json()
        items.update(doc['id'] for doc in result['data'])
        pages = result['meta']['pages']
        page = pages['next_page']
    if len(items) != pages['total_count']:
        raise ValueError(f"Penn pagination returned {len(items)} unique items of {pages['total_count']}")
    return items


def penn_record(item_id, cache_dir):
    if not re.fullmatch(UUID, item_id):
        raise ValueError(f'Unexpected Penn item id {item_id}')
    cache = cache_dir / f'{item_id}.json'
    if cache.exists():
        return json.loads(cache.read_text(encoding='utf-8'))
    response = session().get(f'{REPOSITORY}/iiif/items/{item_id}/manifest', timeout=30)
    response.raise_for_status()
    manifest = response.json()
    fields = {m['label']['none'][0]: m['value']['none'] for m in manifest['metadata']}
    locations = [s for s in fields.get('Physical Location', []) if s.startswith('Library at the Katz Center,')]
    images = []
    if locations and any('NoC-US/' in s or 'publicdomain/' in s for s in fields.get('Rights', [])):
        for canvas in manifest['items']:
            body = canvas['items'][0]['items'][0]['body']
            service = body['service'][0]['id']
            match = re.fullmatch(re.escape(IMAGES) + f'({UUID})', service)
            if not match:
                continue
            asset_id = match[1]
            # Original filenames are also present in NLI's ImageSourceName column.
            url = f'{REPOSITORY}/v1/assets/{asset_id}/preservation'
            head = session().head(url, allow_redirects=False, timeout=30)
            head.raise_for_status()
            redirect = urlsplit(head.headers.get('Location', ''))
            disposition = parse_qs(redirect.query).get('response-content-disposition', [''])[0]
            filename = re.search(r'filename="([^"/\\]+)\.tiff?"', disposition, re.I)
            if filename:
                images.append((filename[1].casefold(), asset_id))
    record = {'id': item_id, 'locations': locations, 'images': images}
    scratch = cache.with_suffix('.tmp')
    scratch.write_text(json.dumps(record), encoding='utf-8')
    scratch.replace(cache)
    return record


def catalogue_marks(names):
    """Translate only Katz's Genizah and RAR call-number series to Penn's notation."""
    marks = set()
    for name in names.split('|'):
        name = name.strip()
        if name.startswith(('Halper ', 'RAR MS ')):
            marks.add(name)
        elif name.startswith('Ms. Genizah '):
            number = name.removeprefix('Ms. Genizah ')
            if re.fullmatch(r'\d+[a-z]?', number, re.I):
                marks.add(f'Halper {number}')
            elif re.fullmatch(r'\d+\.[\d.a-z-]+|GF\s+\d+', number, re.I):
                marks.add(f'RAR MS {number}')
    return sorted(marks)


def build_rows(records, db_path, libraries_path):
    sources = defaultdict(set)
    holdings = defaultdict(set)
    for record in records:
        for name, asset in record['images']:
            sources[name].add((record['id'], asset))
        for location in record['locations']:
            # Penn's physical holdings field may have several comma-delimited
            # institution/collection qualifiers. Only exact shelfmarks count.
            mark = location.rsplit(', ', 1)[-1].casefold().removeprefix('cajs: ').removeprefix('cajs ')
            holdings[mark].add(record['id'])
    manuscripts = defaultdict(list)
    with sqlite3.connect(f'file:{db_path.as_posix()}?mode=ro', uri=True) as db:
        for sid, shelfmark, name, image in db.execute(
                'SELECT NLI_AlmaId, Shelfmark, ImageSourceName, ImageName FROM nli_images '
                'WHERE LibraryAbbrev = ? AND NLI_AlmaId != ?', ('Penn CAJS', '')):
            manuscripts[sid].append((shelfmark, name or '', image or ''))
    with libraries_path.open(encoding='utf-8-sig', newline='') as stream:
        catalogue = {r['system_number']: catalogue_marks(r['call_numbers']) for r in csv.DictReader(stream)
                     if r['library_code'] == 'Katz'}
    rows = []
    for sid, marks in sorted(catalogue.items()):
        pictures = manuscripts[sid]
        items_by_mark = {}
        for mark in marks:
            held = holdings.get(mark.casefold(), set())
            # Two Penn items can carry the same shelfmark. Only an original
            # filename belonging to that holding can disambiguate them.
            filenames = {name.casefold() for sm, name, _ in pictures if sm.casefold() == mark.casefold()}
            items = {item for name in filenames for item, _ in sources.get(name, ()) if item in held} or held
            if len(items) != 1:
                items_by_mark.clear()
                break
            items_by_mark[mark] = next(iter(items))
        if not items_by_mark:
            continue
        items = set(items_by_mark.values())
        picture_marks = {sm.casefold() for sm, _, _ in pictures}
        same_marks = picture_marks == {m.casefold() for m in marks}
        if len(items) > 1:
            if not same_marks:  # Multiple aliases alone cannot establish separate components.
                continue
            marks_by_item = defaultdict(list)
            for mark, item in sorted(items_by_mark.items()):
                marks_by_item[item].append(mark)
            rows.extend((sid, item, '', '', ' + '.join(marks)) for item, marks in marks_by_item.items())
            continue
        item = next(iter(items))
        sides = {}
        if same_marks:
            for _, name, image in pictures:
                # Require the original file within the already corroborated holding.
                # The file h001_wk1_body0001 actually belongs to Penn's Halper 12a.
                assets = {asset for held_item, asset in sources.get(name.casefold(), ()) if held_item == item}
                folio = re.search(r'__L(\d+)F\d+B\d+S([12])$', image)
                if len(assets) != 1 or not folio:
                    break
                side = str(int(folio[1])) + ('r' if folio[2] == '1' else 'v')
                sides[side] = next(iter(assets))
        if sides and len(pictures) == len(sides) == len(set(sides.values())):
            rows.extend((sid, item, side, asset, '') for side, asset in sorted(sides.items()))
        else:
            rows.append((sid, item, '', '', ''))
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cache-dir', type=Path)
    parser.add_argument('--db', type=Path, default=Path('nli_data/nli_crossref.db'))
    parser.add_argument('--libraries', type=Path, default=Path('libraries.csv'))
    parser.add_argument('--output', type=Path, default=Path('shared/katz_images.csv'))
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        cache = args.cache_dir or Path(tmp)
        cache.mkdir(exist_ok=True, parents=True)
        ids = sorted(catalogue_items('"Cairo Genizah Collection"') | catalogue_items('"Genizah Fragments"'))
        print(f'{len(ids)} Penn catalogue records', flush=True)
        with ThreadPoolExecutor(max_workers=6) as pool:
            records = list(pool.map(lambda item: penn_record(item, cache), ids))
        rows = build_rows(records, args.db, args.libraries)
        if not rows:
            raise ValueError('No Penn records matched the NLI crossref')
        temp_output = args.output.with_suffix('.tmp')
        with temp_output.open('w', encoding='utf-8', newline='') as out:
            writer = csv.writer(out, lineterminator='\n')
            writer.writerow(('sys_id', 'penn_item', 'folio', 'penn_image', 'component'))
            writer.writerows(rows)
        temp_output.replace(args.output)
        print(f'{len({r[0] for r in rows})} NLI records, {sum(bool(r[3]) for r in rows)} sides')


if __name__ == '__main__':
    main()
