# -*- coding: utf-8 -*-
"""Rank fusion for multi-witness passage search (pure, dependency-free).

One work survives in many manuscripts, and no single witness of it retrieves
every other. Measured on two independent instruments (2026-08-24, normal
depth, floor-30 index):

* Birkat Hamazon -- one witness reaches 50-69% of the reachable census; the
  same 17 witnesses searched SEPARATELY and merged reach 85%.
* Megillat Antiochus -- a seed plus three rounds of promoted witnesses took
  frontier coverage from 2 to 9 of 20 and positives from 50 to 57 of 68.

Two findings shape this module, and both are counter-intuitive enough that
they are recorded here rather than in a commit message:

1. **Never concatenate witnesses into one query.** The passage engine spends
   a per-query POSTING BUDGET (shared/passage_policy.py), so one long
   concatenated text starves: BH scored 59% concatenated vs 85% fused, and on
   Antiochus recursion EVERY concatenated round scored BELOW the seed alone
   (43-46 positives vs 50). This is a property of `method='passage'`
   specifically. It does NOT generalise: the chunk engine decomposes a query
   into independent per-chunk lookups with no shared budget, and there
   concatenation and union were measured to return the IDENTICAL manuscript
   set (392 both ways, empty difference in both directions), which is why
   desktop's `run_recursive_composition` is correct for its own engine.

2. **Fuse by RANK, not by score.** A passage score is matched QUERY letters,
   so a long witness mechanically outscores a short one for reasons that have
   nothing to do with match quality. RRF ties sum-of-scores where witness
   lengths are similar (BH) and beats it decisively where they are not
   (Antiochus, witnesses spanning 1,153-5,979 letters: 18/26 positives in the
   top 50/100 against 10/19). Length normalisation was measured WORSE than
   raw score at every cut-off on both instruments and is not offered.

This module is deliberately pure -- no NiceGUI, no engine imports, no I/O --
because it has two callers with different shapes and they must not drift:

* `shared/passage_parallels.py` runs N witnesses inside ONE stateless API
  request and fuses at the end.
* `web/pages/parallels.py` is a session: it searches each newly added witness
  ALONE and re-fuses against rows already on screen, which is what makes an
  R-round auto-expansion cost `1 + rounds x K` searches instead of
  re-running everything every time.

Same maths, one definition, two callers.
"""
from __future__ import annotations

import hashlib
import re
from typing import Iterable, Mapping, Optional, Sequence

# Mirrors shared/search_engine.py::RRF_K (60), the value that module's own
# LOCAL/Genizah fusion uses. Deliberately NOT imported: shared/passage_* never
# imports search_engine (the passage stack is a separate layer and importing
# the 12k-line engine for one integer would create the coupling on purpose).
# If one moves, the other is a judgement call, not an automatic follow.
_RRF_K = 60

# Separator for the flat `witness_ids` scalar carried on a fused row. A
# comma-joined STRING, not a list, on purpose: `web/export_state.py`'s
# per-row allowlist trusts nested values without sanitising them, so a nested
# container on a result row can bypass the snapshot's own size budget. Every
# multi-witness field this module writes onto a row is a flat scalar.
_ID_SEP = ','

# The owner's own splitter for a bulk paste: witnesses separated by a blank
# line, tolerating a lone `*` on the separator line (the shape of the
# hand-maintained Birkat Hamazon witness file that motivated this feature).
_PASTE_SPLIT_RE = re.compile(r"\n\s*(?:\*\s*)?\n")

# Same floor `web/pages/parallels.py` applies to the seed text before
# dispatching a search ("Enter at least 3 words"). A bulk paste must not be
# able to smuggle in witnesses the page would have refused if typed.
MIN_WITNESS_WORDS = 3


def witness_id_for(index: int) -> str:
    """Canonical id for the `index`-th witness (0-based).

    Short by design -- ids are repeated on every fused row via `witness_ids`,
    and the page snapshot carries those rows.
    """
    return f"w{index + 1}"


def text_digest(text: str) -> str:
    """Short, stable digest of a witness text, for the search fingerprint.

    The fingerprint must move when the SET OF TEXTS SEARCHED changes, and a
    label will not do it: labels are user-editable, so two different pastes
    can share one label and a stale result set would be recovered for the
    wrong witnesses. Truncated to 16 hex chars -- this identifies a payload
    within one session's tab set, it is not a security boundary.
    """
    return hashlib.sha256((text or '').encode('utf-8')).hexdigest()[:16]


def split_pasted(blob: str) -> tuple[list[str], int]:
    """Split a bulk paste into witness texts on blank lines.

    Returns `(texts, skipped_too_short)` -- the count is returned, never
    silently dropped, so the UI can say "N witnesses detected (M skipped: too
    short)". A split that quietly discards a third of a paste is exactly the
    silent-content-loss failure this project treats as a defect.
    """
    texts: list[str] = []
    skipped = 0
    for chunk in _PASTE_SPLIT_RE.split(blob or ''):
        chunk = chunk.strip()
        if not chunk or chunk == '*':
            continue
        if len([w for w in chunk.split() if w]) < MIN_WITNESS_WORDS:
            skipped += 1
            continue
        texts.append(chunk)
    return texts, skipped


def tag_rows(rows: Sequence[dict], witness_id: str,
             witness_label: str = '') -> list:
    """Stamp `witness_id`, `witness_label` and `witness_rank` on `rows`, in
    place, and return them.

    `witness_rank` is 1-based POSITION in `rows`, which the engine returns in
    descending score order. This is the ONLY place a rank is assigned;
    `fuse()` reads it rather than re-deriving one, so there is no second
    definition to drift.
    """
    for i, row in enumerate(rows, start=1):
        row['witness_id'] = witness_id
        row['witness_label'] = witness_label
        row['witness_rank'] = i
    return list(rows)


def _rank_of(row: dict) -> int:
    rank = row.get('witness_rank')
    if not isinstance(rank, int) or rank < 1:
        raise ValueError(
            'passage_fusion.fuse: row is missing a valid witness_rank; call '
            'tag_rows() on every witness result list before fusing '
            f'(got {rank!r} for {row.get("raw_header")!r})'
        )
    return rank


def _score_of(row: dict) -> float:
    v = row.get('final_score')
    if v is None:
        v = row.get('score')
    try:
        return float(v or 0.0)
    except (TypeError, ValueError):
        return 0.0


def fuse(rows_by_witness: "Mapping[str, Sequence[dict]] | Iterable[tuple]",
         key: str = 'raw_header') -> list[dict]:
    """Fuse per-witness result lists into ONE row per record.

    `rows_by_witness` maps witness_id -> that witness's rows (already tagged
    by `tag_rows`), in witness order; an iterable of `(witness_id, rows)`
    pairs is accepted too, so callers need not build a dict.

    The returned row is a COPY of the WINNING witness's row -- best rank,
    ties broken by score, then by witness order. The winner supplies every
    rendered field (`text`, `source_ctx`, `chunk_hits`, `chunk_count`), which
    is what keeps the highlighted evidence consistent with the witness label
    displayed beside it: a span offset is a position in ONE witness's text
    and is meaningless against another's.

    Fields written onto the fused row:

    ==================  ====================================================
    `score`             the BEST single witness's matched letters -- scale
    `final_score`       unchanged, because `create_manuscript_group` renders
                        Max:/Avg: badges straight off it and the exports
                        carry it as a column. Overwriting it with the RRF
                        sum would silently change ~214 into ~0.03.
    `fusion_score`      sum of 1/(60 + rank) over every witness that matched
    `witness_count`     how many witnesses matched this record
    `witness_id`        the WINNING witness (as does `witness_label`)
    `witness_ids`       every matching witness id, rank order, comma-joined
    ==================  ====================================================
    """
    pairs = (list(rows_by_witness.items())
             if hasattr(rows_by_witness, 'items') else list(rows_by_witness))

    # record key -> {'contributors': [(order, rank, score, row)], 'rrf': float}
    acc: dict = {}
    for order, (_wid, rows) in enumerate(pairs):
        for row in rows or []:
            rec = row.get(key)
            if not rec:
                continue
            rank = _rank_of(row)
            entry = acc.get(rec)
            if entry is None:
                entry = acc[rec] = {'contributors': [], 'rrf': 0.0}
            # Accumulate, never last-witness-wins: `hit_by_header`-shaped
            # dicts keyed by record collide across witnesses by design, and
            # overwriting is how witness_count silently becomes 1.
            entry['contributors'].append((order, rank, _score_of(row), row))
            entry['rrf'] += 1.0 / (_RRF_K + rank)

    fused: list[dict] = []
    for rec, entry in acc.items():
        contributors = entry['contributors']
        # Winner: best (lowest) rank; ties to the higher score; then to the
        # earlier witness, so the result is order-deterministic.
        order, _rank, _score, best_row = min(
            contributors, key=lambda c: (c[1], -c[2], c[0]))
        out = dict(best_row)
        out['fusion_score'] = entry['rrf']
        # Distinct WITNESSES, keyed by their position in `pairs` -- not by
        # row identity (two rows are trivially distinct objects) and not by
        # witness_id (an untagged '' would collapse a real count to 1).
        out['witness_count'] = len({c[0] for c in contributors})
        out['witness_ids'] = _ID_SEP.join(
            _wid_of(c[3]) for c in sorted(contributors, key=lambda c: (c[1], c[0]))
        )
        out['score'] = max(c[2] for c in contributors)
        out['final_score'] = out['score']
        out['chunk_count'] = best_row.get('chunk_count')
        fused.append(out)

    fused.sort(key=lambda r: (r['fusion_score'], r['score'],
                              str(r.get(key) or '')), reverse=True)
    return fused


def _wid_of(row: dict) -> str:
    return str(row.get('witness_id') or '')


def split_ids(value) -> list[str]:
    """Parse a fused row's flat `witness_ids` scalar back into a list."""
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value if v]
    return [p for p in str(value).split(_ID_SEP) if p]


def group_stats(items: Sequence[dict]) -> dict:
    """Fusion facts for ONE manuscript group, from its fused rows.

    `witness_count` is the UNION of the witnesses across the group's rows,
    never a sum: a manuscript's count means "how many distinct witnesses
    point at this manuscript", and two pages of it matched by the same
    witness are one witness, not two. This is the only genuinely new piece of
    ranking logic in the feature and it is unit-tested directly.
    """
    seen: dict = {}
    total = 0.0
    for it in items or []:
        try:
            total += float(it.get('fusion_score') or 0.0)
        except (TypeError, ValueError):
            pass
        for wid in split_ids(it.get('witness_ids')):
            seen.setdefault(wid, None)
        wid = it.get('witness_id')
        if wid:
            seen.setdefault(str(wid), None)
    return {
        'witness_count': len(seen),
        'fusion_score': total,
        'witness_ids': list(seen.keys()),
    }


def sort_key_for(sort: str, group: dict, stats: Optional[dict] = None):
    """Sort key for ONE manuscript group under a user-chosen `sort`.

    Centralised here so the page's group order and any other consumer agree.
    Every key is returned for `reverse=True` sorting, so string keys are
    negated by the caller's own branch rather than here -- see
    `web/pages/parallels.py::_group_sort_key`.
    """
    stats = stats or {}
    if sort == 'fused':
        return (float(stats.get('fusion_score') or 0.0),
                float(group.get('max_score') or 0.0))
    if sort == 'witnesses':
        return (int(stats.get('witness_count') or 0),
                float(stats.get('fusion_score') or 0.0))
    return (float(group.get('max_score') or 0.0),)
