# -*- coding: utf-8 -*-
"""The shared corpus parser contract for passage-index builds.

One parser, used by every builder, so that "the index and the corpus cannot
disagree" is a property of the code rather than a hope. A build records the
hash of each input file it consumed (see `source_manifest`), which is what
makes a stale index detectable instead of silently wrong.

Record grain is the transcription page:

    ==> {sys_id}_{IE...}_{P######}_{FL...} <==
    ...text lines...

Everything between two headers belongs to the first of them. This is the grain
every calibrated constant in docs/specs/passage-matching-algorithm.md was
measured at, which is why the builder must not silently index a different one
(the continuous multi-page pseudo-documents in the Tantivy index, for example).
"""
from __future__ import annotations

import hashlib
import os
import re
from typing import Iterable, Iterator

HEADER_RE = re.compile(r'^==>\s*(\S+)\s*<==\s*$')

# Read in large blocks: the corpus is ~1.5 GB and line-at-a-time Python I/O
# over 948K records is a measurable share of build time.
_READ_CHUNK = 1 << 22


def iter_records(path: str, *, encoding: str = 'utf-8') -> Iterator[tuple]:
    """Yield (record_id, text) for every record in a transcriptions file.

    A leading blob before the first header is skipped and counted by the
    caller if it cares; a trailing record is emitted at EOF.
    """
    record_id = None
    lines: list = []
    with open(path, 'r', encoding=encoding, errors='replace',
              newline='') as fh:
        for raw in fh:
            line = raw.rstrip('\r\n')
            m = HEADER_RE.match(line)
            if m:
                if record_id is not None:
                    yield record_id, '\n'.join(lines)
                record_id = m.group(1)
                lines = []
            elif record_id is not None:
                lines.append(line)
    if record_id is not None:
        yield record_id, '\n'.join(lines)


def sha256_file(path: str, *, chunk: int = _READ_CHUNK) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        while True:
            b = fh.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def source_manifest(paths: Iterable[str]) -> list:
    """Per-input provenance: path, size, sha256.

    Artifact-specific by design. A single hash of one file cannot prove that
    some OTHER index was built from the same source -- the main Tantivy
    builder writes no manifest at all and consumes more than one input -- so
    each artifact records its own input set instead of asserting agreement it
    cannot check.
    """
    out = []
    for p in paths:
        out.append({
            'path': os.path.basename(p),
            'bytes': os.path.getsize(p),
            'sha256': sha256_file(p),
        })
    return out
