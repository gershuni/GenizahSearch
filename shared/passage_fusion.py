# -*- coding: utf-8 -*-
"""Rank fusion for multi-witness passage search (pure, dependency-free).

One work survives in many manuscripts, and no single witness of it retrieves
every other.

Measured THROUGH THIS MODULE and `PassageSearcher` (policy `max-40+short`,
normal depth, floor30_v1 index) against the Birkat Hamazon census -- 673
entries, of which 614 have any indexed text at all, that being the only
denominator a search can be held to:

    concatenated (17 texts joined)   296 / 614   48.2%
    best SINGLE witness              348 / 614   56.7%
    17 witnesses fused               455 / 614   74.1%

Page path and API path return the same 455.

NOT COMPARABLE to the 85% in docs/specs/passage-matching-algorithm.md SS10.2a,
and the difference is the denominator, not the code: that table counts "of 442
reachable" while this counts index membership over a 673-entry census (614).
85% of 442 is 376 manuscripts, so the shipped path in fact finds MORE of the
census (455) than the design harness reported -- it just reports a smaller
fraction of a larger, more conservative denominator. Quote the COUNT and its
denominator, never the bare percentage.

On Megillat Antiochus a seed plus three rounds of promoted witnesses took
frontier coverage from 2 to 9 of 20 and positives from 50 to 57 of 68 -- that
one was measured in the DESIGN harness and has not been re-run through the
shipped path.

Two findings shape this module, and both are counter-intuitive enough that
they are recorded here rather than in a commit message:

1. **Never concatenate witnesses into one query.** The passage engine spends
   a per-query POSTING BUDGET (shared/passage_policy.py), so one long
   concatenated text starves. Joining the 17 BH witnesses gives a 33,180-char
   query carrying 21,093,233 postings, of which the budget admits 499,662 --
   **2.4%** -- and the 27,106 candidates that survive are cut to the 3,000
   verify cap. Result: **48.2%, WORSE than the best single witness's 56.7%**,
   against 74.1% fused. On Antiochus recursion every concatenated round
   likewise scored below the seed alone. This is a property of
   `method='passage'`
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
from typing import Iterable, Mapping, Sequence

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

# The per-witness length ceiling, in characters. Generous next to the
# 456-5,979-letter reality of the measured witness sets, and identical to the
# API's `COMPOSITION_LENGTH_CAP` -- it lives HERE so the page and the API
# cannot drift into disagreeing about what a witness is. The page had no cap
# at all, so an over-long paste became a witness that timed out after 30s,
# once per witness.
MAX_WITNESS_CHARS = 20000


def split_by_length(texts, cap: int = None):
    """Partition candidate witness texts into (searchable, too_long).

    Module level and pure so the RULE is tested by calling it. Inline in the
    page it was covered only by a substring assertion, and a mutation
    replacing the whole filter with `[]` -- which searches every over-long
    text -- left the suite green while the words `MAX_WITNESS_CHARS` sat two
    lines above, still satisfying the grep.

    An over-long witness is REJECTED, never truncated: half a manuscript
    searched as if it were the whole one is a worse answer than none, and an
    invisible one. The caller reports the rejects.
    """
    limit = MAX_WITNESS_CHARS if cap is None else cap
    ok, too_long = [], []
    for t in texts or []:
        (too_long if len(t or '') > limit else ok).append(t)
    return ok, too_long


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

    ==========================  ============================================
    `score`                     the WINNING witness's matched letters --
    `final_score`               scale unchanged, because
                                `create_manuscript_group` renders Max:/Avg:
                                badges straight off it and the exports carry
                                it as a column. Overwriting it with the RRF
                                sum would silently change ~214 into ~0.03.
    `fusion_score`              sum of 1/(60 + rank) over every witness that
                                matched
    `witness_count`             how many witnesses matched this record
    `witness_id`                the WINNING witness (as does `witness_label`)
    `witness_ids`               every matching witness id, rank order,
                                comma-joined
    `best_witness_score`        the highest score any contributor scored --
                                a fact about the FUSION, not about this row
    ==========================  ============================================

    `score` used to be `max()` across the contributors, which contradicted
    the winner rule above: a record found at rank 1 by a 400-letter witness
    and at rank 31 by a 900-letter one rendered the SHORT witness's label and
    highlighted span beside the number 900. A reader would take those 900
    letters to be the ones highlighted in front of them. The number and the
    evidence now come from the same row; the maximum is still reported, under
    a name that says what it is.
    """
    pairs = _as_pairs(rows_by_witness)

    # record key -> {'contributors': [(order, rank, score, row)], 'rrf': float}
    acc: dict = {}
    for order, (_wid, rows) in enumerate(pairs):
        # One contribution per witness per record. The engine keys its hits by
        # (witness, record) and so should never emit the same record twice in
        # one witness's list -- which is precisely why a duplicate would go
        # unnoticed: it would not change `witness_count` (keyed by witness
        # position), only inflate that witness's share of the RRF sum, and a
        # ranking that is quietly wrong looks exactly like one that is right.
        best_here: dict = {}
        for row in rows or []:
            rec = row.get(key)
            if not rec:
                continue
            rank = _rank_of(row)
            prev = best_here.get(rec)
            if prev is None or rank < prev[1]:
                best_here[rec] = (order, rank, _score_of(row), row)
        for rec, contribution in best_here.items():
            entry = acc.get(rec)
            if entry is None:
                entry = acc[rec] = {'contributors': [], 'rrf': 0.0}
            # Accumulate ACROSS witnesses, never last-witness-wins:
            # `hit_by_header`-shaped dicts keyed by record collide across
            # witnesses by design, and overwriting is how witness_count
            # silently becomes 1.
            entry['contributors'].append(contribution)
            entry['rrf'] += 1.0 / (_RRF_K + contribution[1])

    fused: list[dict] = []
    for rec, entry in acc.items():
        contributors = entry['contributors']
        # Winner: best (lowest) rank; ties to the higher score; then to the
        # earlier witness, so the result is order-deterministic.
        _order, _rank, win_score, best_row = min(
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
        # From the WINNER, not `max()` across contributors: this row shows
        # the winner's label and the winner's highlighted span, so a score
        # from anyone else describes text that is not on the screen.
        out['score'] = win_score
        out['final_score'] = win_score
        out['best_witness_score'] = max(c[2] for c in contributors)
        out['chunk_count'] = best_row.get('chunk_count')
        fused.append(out)

    fused.sort(key=lambda r: (r['fusion_score'], r['score'],
                              str(r.get(key) or '')), reverse=True)
    return fused


def _as_pairs(rows_by_witness):
    return (list(rows_by_witness.items())
            if hasattr(rows_by_witness, 'items') else list(rows_by_witness))


def fuse_routed(eligible_pairs, filtered_pairs, key: str = 'raw_header'):
    """Fuse two ROUTED buckets into `(main, filtered)`.

    `filter_text` routes a witness's match to `filtered` when the composition
    text it matched is itself known/printed source. That is a statement about
    a ROW. The fusion facts are statements about a RECORD, and the two were
    being conflated: fusing each bucket separately built a main record's
    `fusion_score`, `witness_count`, `witness_ids` and `best_witness_score`
    from its ELIGIBLE contributors only, then dropped its filtered twin. A
    manuscript found by two witnesses, one of them on known source text,
    therefore reported one -- contradicting `witness_count`'s own documented
    meaning ("how many distinct witnesses point at this manuscript") and
    under-ranking it against records whose contributors happened to avoid the
    filter.

    So: the rendered row still comes from an ELIGIBLE contributor -- a row
    shows one witness's highlighted span, and a filtered span is precisely the
    text the caller asked to discount -- while the fusion statistics count
    EVERY contributor. Evidence from the eligible bucket, arithmetic over all
    of it.

    Routing itself is unchanged and stays here rather than in either caller:
    a record is `filtered` only when EVERY witness that matched it filtered
    it, or the filter would grow STRICTER the more witnesses are added, the
    opposite of what the control says. That rule had been written out twice,
    in `shared/passage_parallels.py` and in `web/pages/parallels.py`, which is
    the drift this module's docstring says it exists to prevent.

    With no `filter_text` every filtered bucket is empty, the overlay is a
    no-op and the result is byte-identical to fusing the eligible bucket
    alone -- which is the common path.
    """
    e_pairs = _as_pairs(eligible_pairs)
    f_pairs = _as_pairs(filtered_pairs)

    main = fuse(e_pairs, key=key)
    filtered = fuse(f_pairs, key=key)

    # Complete contributor set, per witness, in witness order. `fuse` keeps
    # ONE contribution per witness per record, so a witness appearing in both
    # buckets for the same record cannot double-count; and the ranks were
    # stamped on that witness's FULL list before the split, so ranks from the
    # two buckets are directly comparable.
    f_by_wid = dict(f_pairs)
    combined = [(wid, list(rows) + list(f_by_wid.pop(wid, None) or []))
                for wid, rows in e_pairs]
    combined.extend((wid, list(rows)) for wid, rows in f_by_wid.items())

    complete = {row.get(key): row for row in fuse(combined, key=key)}
    for row in main:
        stats = complete.get(row.get(key))
        if not stats:
            continue
        for field in ('fusion_score', 'witness_count', 'witness_ids',
                      'best_witness_score'):
            row[field] = stats[field]
    # `score` / `final_score` are deliberately NOT overlaid: they belong to
    # the row being rendered, and the strongest match across every
    # contributor is reported as `best_witness_score` under a name that says
    # so.

    # Re-sorted because the overlay changed the key it was sorted by.
    main.sort(key=lambda r: (r['fusion_score'], r['score'],
                             str(r.get(key) or '')), reverse=True)

    in_main = {row.get(key) for row in main}
    return main, [r for r in filtered if r.get(key) not in in_main]


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
    best = 0.0
    for it in items or []:
        try:
            total += float(it.get('fusion_score') or 0.0)
        except (TypeError, ValueError):
            pass
        try:
            # Falls back to the row's OWN score, which for a row that never
            # went through `fuse()` -- a single-witness or a chunk result --
            # is exactly "the best score any witness made on it", there
            # being one witness. Without the fallback every such group
            # reported 0.0, and `best_match` sorted the whole page into one
            # tie.
            _best_of_row = it.get('best_witness_score')
            if _best_of_row is None:
                _best_of_row = it.get('score')
            best = max(best, float(_best_of_row or 0.0))
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
        # The strongest single match found on this manuscript by ANY witness.
        # Reported here rather than as a row's `score` because it may belong
        # to a witness whose evidence no row in the group renders.
        'best_witness_score': best,
    }

