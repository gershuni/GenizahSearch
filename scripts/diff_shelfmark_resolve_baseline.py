"""Review a regenerated shelfmark baseline against the previous one, entry by entry.

    python scripts/diff_shelfmark_resolve_baseline.py OLD.json NEW.json [--expect QUERY=ID[,ID...]]...

For every entry whose response changed it prints the input, its source record and the
direction (source kept / added / lost), then fails (exit 1) on any of:

* a changed entry whose input names no library by its prefix
  (``shared.catalog_shelfmark.prefix_codes``) -- the prefix rule of
  docs/plans/SHELFMARK_PREFIX_SCOPE_PLAN.md changes nothing else;
* a lost source, or a single record other than the source;
* a changed entry with no source (the explicit inputs) whose new ids are not the ones given
  with ``--expect``.

Read-only: it reads two fixture files and imports the resolver's helpers.
"""
import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))


def _ids(response):
    return [response['sys_id']] if response.get('sys_id') else list(response.get('options') or [])


def main():
    from shared.browse_map_utils import _strip_library_prefix
    from shared.catalog_shelfmark import prefix_codes

    ap = argparse.ArgumentParser()
    ap.add_argument('old')
    ap.add_argument('new')
    ap.add_argument('--expect', action='append', default=[], help='QUERY=ID[,ID...] for a changed explicit input')
    args = ap.parse_args()
    expect = {}
    for item in args.expect:
        query, _, ids = item.rpartition('=')
        expect[query] = ids.split(',')
    old = json.loads(Path(args.old).read_text(encoding='utf-8'))['entries']
    new = json.loads(Path(args.new).read_text(encoding='utf-8'))['entries']
    key = lambda e: (e['kind'], e['query'], e['limit'], e['source'])  # noqa: E731
    old_by, new_by = {key(e): e for e in old}, {key(e): e for e in new}
    if old_by.keys() != new_by.keys():
        sys.exit('the two fixtures have different inputs')
    changed = bad = 0
    for k, a in old_by.items():
        b = new_by[k]
        if a['response'] == b['response'] and a['exact'] == b['exact']:
            continue
        changed += 1
        kind, query, limit, source = k
        before, after = _ids(a['response']), _ids(b['response'])
        text = query.strip()
        problem = ''
        if not prefix_codes(text, _strip_library_prefix(text)):
            problem = 'CHANGED WITHOUT A NAMED LIBRARY'
        elif source and source in before and source not in after:
            problem = 'LOST SOURCE'
        elif source and b['response'].get('sys_id') and b['response']['sys_id'] != source:
            problem = 'SINGLE OTHER RECORD'
        elif not source and expect.get(query) != after:
            problem = 'EXPLICIT INPUT CHANGED WITHOUT --expect'
        if problem:
            bad += 1
        direction = ('' if not source else 'source added' if source in after and source not in before
                     else 'source kept' if source in after else 'source absent')
        print(f"{problem or 'ok':40} {kind:15} limit={limit:<4} {direction:13} {query!r}")
        print(f"{'':40} before {len(before)}: {before[:5]}  shown {a['response'].get('selected_shelfmark')!r}")
        print(f"{'':40} after  {len(after)}: {after[:5]}  shown {b['response'].get('selected_shelfmark')!r}")
    print(f'{changed} of {len(old_by)} entries changed; {bad} problems')
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main())
