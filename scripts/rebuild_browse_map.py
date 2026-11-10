# -*- coding: utf-8 -*-
"""Rebuild browse_map.pkl from the corpus files, without re-indexing.

The browse map is written in exactly one place -- the tail of
`shared/indexer.py::Indexer.build_index` -- so the only supported way to
replace a damaged one is a full Tantivy rebuild of a 13 GB index. But the map
itself does not depend on Tantivy at all: every field in it is parsed out of
the corpus files' own header lines. This replays that part of the loop and
nothing else.

Faithfulness is the whole point, so this reuses the indexer's own decisions
rather than restating them:

* the same two files in the same order (V0.8 then V0.7), which is what makes
  the key order "file order" -- `get_adjacent_sys_id_by_file_order` navigates
  between manuscripts by it;
* the same separator test, and the same `if cid and ctext` guard, so a header
  with no text lines under it is skipped here exactly as it is there;
* `MetadataManager`'s own two parsers, borrowed unbound (both are pure regex
  over the header string and touch no instance state), so a change to either
  moves this script with it;
* the same `dedupe_browse_map` and the same sort.

Writes nowhere by default: it prints what it WOULD write. `--out` writes to a
named file; `--install` replaces the live map, backing up the current one
first and swapping through os.replace so a kill mid-write cannot leave a
truncated map behind.
"""
from __future__ import annotations

import argparse
import os
import pickle
import shutil
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.browse_map_utils import dedupe_browse_map      # noqa: E402
from shared.config import Config                            # noqa: E402
from shared.metadata_manager import MetadataManager         # noqa: E402


class _Parsers:
    """Borrows the two header parsers unbound. Both are pure regex over the
    header text -- constructing a real MetadataManager would load the CSV
    bank and the metadata cache for nothing."""
    extract_unique_id = MetadataManager.extract_unique_id
    parse_full_id_components = MetadataManager.parse_full_id_components


def build(verbose=True):
    p = _Parsers()
    browse_map = defaultdict(list)
    started = time.monotonic()
    lines_seen = 0

    for fpath, label in [(Config.FILE_V8, 'V0.8'), (Config.FILE_V7, 'V0.7')]:
        if not os.path.exists(fpath):
            if verbose:
                print('skipping (absent): %s' % fpath)
            continue
        if verbose:
            print('reading %s (%s, %.2f GB)'
                  % (fpath, label, os.path.getsize(fpath) / 1e9))

        def flush(cid, chead, ctext):
            # The indexer appends only when the PREVIOUS record had text, so a
            # header with nothing under it contributes no page here either.
            if not (cid and ctext):
                return
            parsed = p.parse_full_id_components(chead)
            if parsed['sys_id'] and parsed['p_num']:
                browse_map[parsed['sys_id']].append({
                    'p_num': int(parsed['p_num']),
                    'uid': cid,
                    'full_header': chead,
                })

        with open(fpath, 'r', encoding='utf-8') as f:
            cid, chead, ctext = None, None, []
            for line in f:
                lines_seen += 1
                line = line.strip()
                is_sep = ((label == 'V0.8' and line.startswith('==>'))
                          or (label == 'V0.7' and line.startswith('###')))
                if is_sep:
                    flush(cid, chead, ctext)
                    chead = (line.replace('==>', '').replace('<==', '').strip()
                             if label == 'V0.8' else line)
                    cid = p.extract_unique_id(line)
                    ctext = []
                else:
                    ctext.append(line)
                if verbose and lines_seen % 5_000_000 == 0:
                    print('  %.1fM lines, %d manuscripts so far (%.0fs)'
                          % (lines_seen / 1e6, len(browse_map),
                             time.monotonic() - started))
            flush(cid, chead, ctext)

    for sid in browse_map:
        browse_map[sid].sort(key=lambda x: x['p_num'])
    before = sum(len(v) for v in browse_map.values())
    cleaned, deduped = dedupe_browse_map(browse_map)
    after = sum(len(v) for v in cleaned.values())
    if verbose:
        print('\n%d manuscripts, %d pages (%d duplicate pages removed) in %.0fs'
              % (len(cleaned), after, before - after, time.monotonic() - started))
    return cleaned


def write_atomic(path, obj):
    tmp = '%s.rebuild.tmp' % path
    with open(tmp, 'wb') as f:
        pickle.dump(obj, f)
    os.replace(tmp, path)


def describe_live_map(path):
    """One line about the map being replaced -- never a prerequisite for
    replacing it. A truncated or unpicklable map is precisely the damage
    this tool exists to repair, so failing to read one is a thing to REPORT
    and carry on from, not a reason to refuse to rebuild."""
    if not os.path.exists(path):
        return 'live map: %s -- ABSENT' % path
    try:
        with open(path, 'rb') as f:
            current = pickle.load(f)
        return ('live map: %s -- %d manuscripts, %d bytes'
                % (path, len(current), os.path.getsize(path)))
    except Exception as exc:                                 # noqa: BLE001
        return ('live map: %s -- UNREADABLE (%s: %s), %d bytes. Rebuilding '
                'anyway; that is what this tool is for.'
                % (path, type(exc).__name__, exc, os.path.getsize(path)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', help='write the rebuilt map here')
    ap.add_argument('--install', action='store_true',
                    help='replace the LIVE browse map (backs the old one up)')
    args = ap.parse_args()

    live = Config.BROWSE_MAP
    print(describe_live_map(live))

    cleaned = build()

    if args.out:
        write_atomic(args.out, cleaned)
        print('wrote %s (%d bytes)' % (args.out, os.path.getsize(args.out)))
    if args.install:
        if os.path.exists(live):
            backup = '%s.bak-%d' % (live, int(time.time()))
            shutil.copy2(live, backup)
            print('backed up the old map to %s' % backup)
        write_atomic(live, cleaned)
        print('installed %s (%d bytes)' % (live, os.path.getsize(live)))
    if not args.out and not args.install:
        print('\nDRY RUN -- nothing written. Pass --out PATH or --install.')


if __name__ == '__main__':
    main()
