# -*- coding: utf-8 -*-
"""The main index's format marker (Responsa plan §6.1, "Index format marker").

``tantivy_db/index_format.json`` is written LAST by ``Indexer.create_index``, after the
main and seam indexes are committed, so its presence says the build finished:

    {"format": 2, "build_id": "...", "created": "...",
     "reversed_field": "content_rev" | null,
     "seams": {"dir": "seams", "words_each_side": 30, "count": N, ...} | null}

``tantivy_db/seams/seam_format.json`` repeats the build id. The engine trusts what both
say AND what the index shows (the field in the schema, the seam index opening with its
fields): a marker without the data, a seam index from another build, or no marker at all
(every index before this one) is an old index, searched by today's routes.
"""
from __future__ import annotations

import datetime
import json
import os
import uuid

from shared.index_seams import INDEX_FORMAT_VERSION, REVERSED_WORDS_FIELD, SEAMS_SUBDIR

INDEX_FORMAT_FILE = 'index_format.json'
SEAM_FORMAT_FILE = 'seam_format.json'


def _write_json(path, payload):
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
    os.replace(tmp, path)


def _read_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def write_index_format(db_path, *, reversed_field, seams):
    """Write the seam marker (when *seams*, the seam build's counts) and then the main
    marker. Returns the main marker's payload."""
    build_id = uuid.uuid4().hex
    created = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')
    if seams:
        seams = dict(seams, dir=SEAMS_SUBDIR)
        _write_json(os.path.join(db_path, SEAMS_SUBDIR, SEAM_FORMAT_FILE),
                    {'build_id': build_id, 'words_each_side': seams['words_each_side'],
                     'count': seams['count']})
    payload = {'format': INDEX_FORMAT_VERSION, 'build_id': build_id, 'created': created,
               'reversed_field': REVERSED_WORDS_FIELD if reversed_field else None,
               'seams': seams or None}
    _write_json(os.path.join(db_path, INDEX_FORMAT_FILE), payload)
    return payload


def read_index_format(db_path):
    """The main marker, or None (no marker: an index from before Phase 2, or a broken one)."""
    data = _read_json(os.path.join(db_path, INDEX_FORMAT_FILE))
    if not data or not isinstance(data.get('format'), int):
        return None
    return data


def seam_index_path(db_path, marker):
    """The seam index directory the marker names, when its own marker carries the same
    build id and a positive window; else None (no seams, or seams of another build)."""
    seams = (marker or {}).get('seams')
    if not isinstance(seams, dict) or seams.get('dir') != SEAMS_SUBDIR:
        return None
    path = os.path.join(db_path, SEAMS_SUBDIR)
    own = _read_json(os.path.join(path, SEAM_FORMAT_FILE))
    words = (own or {}).get('words_each_side')
    if not own or own.get('build_id') != marker.get('build_id') or not isinstance(words, int) or words < 1:
        return None
    return path
