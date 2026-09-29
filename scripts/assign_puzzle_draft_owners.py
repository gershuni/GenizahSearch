# -*- coding: utf-8 -*-
"""Give saved Fragment Puzzle joins (drafts) in a joins.db an owner.

Web saved joins are kept per visitor: each row of ``join_documents`` carries
an ``owner_key`` -- ``'u:<Supabase user id>'`` for a signed-in account, or
``'b:<session uuid>'`` for a signed-out browser (see ``web/saved_joins.py``).
Rows saved before owners existed have ``owner_key = NULL``: they stay in the
file, and no web visitor sees them. This owner-run script assigns owners to
some of those rows from a mapping the owner builds himself (for example from
Supabase ``published_joins.local_doc_id`` -> ``user_id``, plus the ids he
recognises as his own).

Run it on a COPY of joins.db, check the counts, then swap the copy in during
a restart::

    python scripts/assign_puzzle_draft_owners.py joins-copy.db --mapping owners.csv
    python scripts/assign_puzzle_draft_owners.py joins-copy.db --mapping owners.csv --apply

Mapping file:

* ``.json``: an object ``{"<doc id>": "u:<user id>", ...}`` or a list of
  ``{"doc_id": ..., "owner_key": ...}`` objects;
* ``.csv``: a header row with ``doc_id`` and ``owner_key`` columns.

Rules:

* dry run by default: nothing is written, and only COUNTS are printed (never
  ids, titles, notes or owner keys);
* ``--apply`` writes in one transaction, and only rows whose owner is still
  NULL; it never replaces an owner that is already set;
* if any mapping entry is malformed, names one id twice with different
  owners, or would replace an existing owner, ``--apply`` writes nothing and
  exits 1;
* the database file must already exist (it is never created).

Exit codes: 0 ok, 1 refused (nothing written), 2 bad arguments or input.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import sys
from typing import Dict, List, Optional, Tuple

OWNER_PREFIXES = ('u:', 'b:')


def _valid_owner(value: object) -> bool:
    """'u:<id>' or 'b:<id>' with a non-empty id and no whitespace anywhere."""
    return (isinstance(value, str)
            and value.startswith(OWNER_PREFIXES)
            and len(value) > 2
            and not any(c.isspace() for c in value))


def load_mapping(path: str) -> Tuple[List[Tuple[object, object]], Optional[str]]:
    """Read (doc_id, owner_key) pairs from a .json or .csv file.

    Returns (pairs, error). Pairs keep the raw values so that malformed ones
    can be counted rather than silently dropped.
    """
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == '.json':
            with open(path, encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict):
                return list(data.items()), None
            if isinstance(data, list):
                pairs = []
                for item in data:
                    if isinstance(item, dict):
                        pairs.append((item.get('doc_id'), item.get('owner_key')))
                    else:
                        pairs.append((None, None))
                return pairs, None
            return [], 'the JSON must be an object or a list'
        if ext == '.csv':
            with open(path, encoding='utf-8-sig', newline='') as f:
                reader = csv.DictReader(f)
                fields = set(reader.fieldnames or [])
                if not {'doc_id', 'owner_key'} <= fields:
                    return [], 'the CSV needs doc_id and owner_key columns'
                return [(row.get('doc_id'), row.get('owner_key')) for row in reader], None
    except (OSError, ValueError) as e:
        return [], f'could not read the mapping file ({type(e).__name__})'
    return [], 'the mapping file must be .json or .csv'


def plan(conn: sqlite3.Connection, pairs: List[Tuple[object, object]],
         has_owner_column: bool) -> Tuple[Dict[str, int], Dict[str, str]]:
    """Work out what an apply would do. Returns (counts, assignments)."""
    counts = {
        'mapping_entries': len(pairs),
        'malformed': 0,
        'conflicting_duplicates': 0,
        'not_found': 0,
        'already_this_owner': 0,
        'owned_by_someone_else': 0,
        'to_assign': 0,
    }
    wanted: Dict[str, str] = {}
    for doc_id, owner in pairs:
        if not isinstance(doc_id, str) or not doc_id.strip() or not _valid_owner(owner):
            counts['malformed'] += 1
            continue
        doc_id = doc_id.strip()
        if doc_id in wanted and wanted[doc_id] != owner:
            counts['conflicting_duplicates'] += 1
            continue
        wanted[doc_id] = owner

    assignments: Dict[str, str] = {}
    for doc_id, owner in wanted.items():
        if has_owner_column:
            row = conn.execute('SELECT owner_key FROM join_documents WHERE id = ?', (doc_id,)).fetchone()
        else:
            row = conn.execute('SELECT NULL FROM join_documents WHERE id = ?', (doc_id,)).fetchone()
        if row is None:
            counts['not_found'] += 1
        elif row[0] is None:
            assignments[doc_id] = owner
        elif row[0] == owner:
            counts['already_this_owner'] += 1
        else:
            counts['owned_by_someone_else'] += 1
    counts['to_assign'] = len(assignments)
    return counts, assignments


def _table_counts(conn: sqlite3.Connection, has_owner_column: bool) -> Dict[str, int]:
    total = conn.execute('SELECT COUNT(*) FROM join_documents').fetchone()[0]
    if not has_owner_column:
        return {'rows_total': total, 'rows_without_owner': total}
    without = conn.execute('SELECT COUNT(*) FROM join_documents WHERE owner_key IS NULL').fetchone()[0]
    return {'rows_total': total, 'rows_without_owner': without}


def _has_owner_column(conn: sqlite3.Connection) -> bool:
    return any(r[1] == 'owner_key' for r in conn.execute('PRAGMA table_info(join_documents)'))


def _print_counts(title: str, counts: Dict[str, int]) -> None:
    print(title)
    for k, v in counts.items():
        print(f'  {k}: {v}')


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('db', help='path to a COPY of joins.db')
    ap.add_argument('--mapping', required=True, help='.json or .csv file: doc_id -> owner_key')
    ap.add_argument('--apply', action='store_true', help='write the owners (default: dry run)')
    args = ap.parse_args(argv)

    if not os.path.isfile(args.db):
        print('error: the database file does not exist', file=sys.stderr)
        return 2
    pairs, err = load_mapping(args.mapping)
    if err:
        print(f'error: {err}', file=sys.stderr)
        return 2

    if args.apply:
        conn = sqlite3.connect(args.db)
    else:
        from pathlib import Path
        conn = sqlite3.connect(Path(os.path.abspath(args.db)).as_uri() + '?mode=ro', uri=True)
    try:
        try:
            conn.execute('SELECT 1 FROM join_documents LIMIT 0')
        except sqlite3.Error:
            print('error: no join_documents table in this file', file=sys.stderr)
            return 2
        has_col = _has_owner_column(conn)
        _print_counts('Before:', _table_counts(conn, has_col))
        counts, assignments = plan(conn, pairs, has_col)
        _print_counts('Mapping:', counts)

        blocked = counts['malformed'] + counts['conflicting_duplicates'] + counts['owned_by_someone_else']
        if not args.apply:
            print('Dry run: nothing written. Re-run with --apply to write.')
            if blocked:
                print('Note: --apply would refuse this mapping (see malformed / conflicting / someone else).')
            return 0
        if blocked:
            print('Refused: nothing written (fix the mapping first).')
            return 1

        try:
            with conn:  # one transaction: all or nothing
                if not has_col:
                    conn.execute('ALTER TABLE join_documents ADD COLUMN owner_key TEXT')
                written = 0
                for doc_id, owner in assignments.items():
                    cur = conn.execute(
                        'UPDATE join_documents SET owner_key = ? WHERE id = ? AND owner_key IS NULL',
                        (owner, doc_id))
                    written += cur.rowcount
                if written != len(assignments):
                    raise RuntimeError('a row changed while the script ran')
        except (RuntimeError, sqlite3.Error) as e:
            print(f'Refused: nothing written ({e}).')
            return 1
        _print_counts('After:', _table_counts(conn, True))
        print(f'Written: {written}')
        return 0
    finally:
        conn.close()


if __name__ == '__main__':
    sys.exit(main())
