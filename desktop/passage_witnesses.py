# -*- coding: utf-8 -*-
"""The desktop's multi-witness state machine -- pure, Qt-free, no I/O.

One work survives in many manuscripts and no single witness of it retrieves
every other: on the 614 Birkat Hamazon census manuscripts that have any indexed
text, the best single witness finds 348 (56.7%) and the same 17 searched
separately and rank-fused find 455 (74.1%). Concatenating them into one query
finds 296 (48.2%) -- WORSE than one witness -- because the passage engine
spends a per-query posting budget and a 33,180-character query admits 2.4% of
its own postings. So the desktop searches each witness on its own and fuses by
rank, exactly as the web does.

Everything that decides WHAT is searched lives here rather than in
`genizah_app.py`, for two reasons. The obvious one is that a 28,000-line god
file is a poor home for a state machine. The load-bearing one is that these
rules are only testable if they are reachable without a QApplication: every
rule below is a bug the web hit first, and each is pinned by a test that calls
the function directly. A rule embedded in a widget callback can only be
asserted against its own source text, and a source-text assertion cannot tell
`for header in headers` from `for header in []` -- which is how one of these
rules was once proven vacuous.

The arithmetic is NOT here. Ranking and fusion live in
`shared/passage_fusion.py`, which the web page and the public API already
share; this module is the third caller that module's own docstring
anticipated. Witness resolution (turning a promoted manuscript into text)
lives in `shared/passage_witness_source.py`. What is left here is bookkeeping:
identity, duplication, staleness, capacity, and which rows belong to whom.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from shared.passage_fusion import (
    MIN_WITNESS_WORDS, fuse_routed, rank_and_route, split_by_length,
    split_pasted, witness_text_key,
)
from shared.passage_witness_source import WITNESS_CAP, WITNESS_SEED_ID

# How many witnesses ONE desktop run may search. FLAT at every depth, by owner
# ruling 2026-08-27, and deliberately unlike the web's 25/8/4 ladder.
#
# The web scales its cap by depth because a witness there costs a slot of a
# FOUR-slot pool shared with every other visitor and must finish inside a 30 s
# ceiling; twenty-five deep witnesses would be eight minutes of one visitor
# holding a quarter of the site's passage capacity. None of that is true on a
# desktop, where the only thing spent is the user's own wall clock -- roughly
# 18 s for 25 witnesses at Normal, 3.5 minutes at Deep, 8 minutes at Deepest.
#
# A flat cap is only defensible because Stop now works BETWEEN witnesses: the
# user can end a long batch and keep what it found. Warn honestly about the
# wait; do not refuse work the machine is perfectly able to do.
DESKTOP_WITNESS_CAP = WITNESS_CAP

# Status values a witness can hold. `stale` is not an error: it means the
# witness was gathered for a DIFFERENT source text, and a witness of one work
# is noise in another.
STATUS_PENDING = 'pending'
STATUS_RUNNING = 'running'
STATUS_SEARCHED = 'searched'
STATUS_FAILED = 'failed'
STATUS_STALE = 'stale'


@dataclass
class WitnessEntry:
    """One witness. `text` is what gets searched; everything else describes
    where it came from and how the last attempt went."""

    id: str
    label: str
    kind: str = 'pasted'          # 'pasted' | 'manuscript'
    sys_id: Optional[str] = None
    # The seed this witness was gathered FOR. A later search against a
    # different source text must not quietly include it.
    seed_digest: str = ''
    # The pages a PROMOTED witness was built from. A promoted witness is not a
    # function of its sys_id -- it is the concatenation of the pages that
    # matched -- so without this a restore rebuilds a different witness under
    # the same label.
    headers: list = field(default_factory=list)
    text: str = ''
    status: str = STATUS_PENDING
    hits: int = 0
    error: str = ''


@dataclass
class WitnessSet:
    """The witness list plus the per-witness rows the fusion is rebuilt from.

    `rows` / `filtered` are keyed by witness id and hold that witness's OWN
    result lists. They are kept rather than only the fused output because a
    fusion has to be rebuildable: adding a witness re-fuses against rows
    already in hand, which is what makes an R-round expansion cost
    `1 + rounds x K` searches instead of re-running everything every time.
    """

    entries: list = field(default_factory=list)
    seq: int = 0
    rows: dict = field(default_factory=dict)
    filtered: dict = field(default_factory=dict)
    # WHAT the two caches above are answers TO: the seed text and the search
    # settings their rows were produced under. Rows are reusable only for an
    # identical key; see `invalidate_cache`.
    cache_key: object = None


# --- identity --------------------------------------------------------------

def new_id(state: WitnessSet) -> str:
    """Monotonic, and never reused after a removal.

    A recycled id lets a removed witness's stale rows be attributed to its
    replacement -- the row caches are keyed by id, so the collision is silent
    and shows up only as a wrong `witness_count`.
    """
    state.seq += 1
    return f'w{state.seq}'


def default_label(text: str, fallback: str) -> str:
    """First five words, or the caller's fallback.

    `fallback` is injected rather than translated here so this module stays
    free of the UI's string table -- the same reason
    `restore_witness_entries` takes one.
    """
    words = [w for w in (text or '').split() if w][:5]
    return ' '.join(words) or fallback


def order(state: WitnessSet) -> list:
    """Witness ids in fusion order, the seed first. The seed IS a witness."""
    return [WITNESS_SEED_ID] + [e.id for e in state.entries]


def labels(state: WitnessSet, seed_label: str) -> dict:
    return dict(
        [(WITNESS_SEED_ID, seed_label)]
        + [(e.id, e.label or '') for e in state.entries]
    )


def searched_count(state: WitnessSet) -> int:
    """How many witnesses have rows on record -- the `m` in "3 of 17".

    Counts a witness that searched and found NOTHING: it is still a witness
    that was consulted, and excluding it would make the denominator move
    depending on results.
    """
    return sum(1 for wid in order(state) if state.rows.get(wid) is not None)


def pending(state: WitnessSet) -> list:
    return [e for e in state.entries if e.status == STATUS_PENDING]


def text_keys(state: WitnessSet, seed_text: str) -> set:
    """Every witness text already represented, THE SEED INCLUDED.

    The seed is fused under `WITNESS_SEED_ID` like any other witness, so a
    paste identical to the box above counts the same text twice and
    `witness_count` then reports two witnesses where there is one.
    """
    keys = {witness_text_key(seed_text)}
    keys.update(witness_text_key(e.text) for e in state.entries
                if (e.text or '').strip())
    return keys


# --- adding ----------------------------------------------------------------

@dataclass
class AddReport:
    """What `add_texts` did, in full. Every rejection is COUNTED and returned.

    A paste that quietly loses a third of a file is the failure this project
    treats as a defect, so nothing here is dropped in silence -- the caller
    has the numbers it needs to say exactly what happened.
    """

    added: list = field(default_factory=list)
    duplicates: int = 0
    too_long: int = 0
    too_short: int = 0
    over_cap: int = 0


def split_paste(blob: str):
    """Split a bulk paste on blank lines. Delegates, so the desktop and the
    web cannot disagree about where one witness ends and the next begins."""
    return split_pasted(blob)


def add_texts(state: WitnessSet, texts, seed_text: str, fallback_label: str,
              label: str = '', kind: str = 'pasted', sys_id: str = None,
              seed_digest: str = None, headers=None,
              cap: int = DESKTOP_WITNESS_CAP) -> AddReport:
    """Add witnesses, applying every admission rule, in a fixed order.

    The order matters and is the same one both web doors use: too-short, then
    too-long, then duplicate, then capacity. Checking capacity first would
    spend the last slots on texts that are about to be rejected anyway.

    An over-long witness is REJECTED, never truncated: half a manuscript
    searched as if it were the whole one is a worse answer than none, and an
    invisible one.
    """
    report = AddReport()
    candidates = []
    for t in (texts or []):
        t = (t or '').strip()
        if len([w for w in t.split() if w]) < MIN_WITNESS_WORDS:
            report.too_short += 1
            continue
        candidates.append(t)

    fits, over = split_by_length(candidates)
    report.too_long = len(over)

    seen = text_keys(state, seed_text)
    fresh = []
    for t in fits:
        key = witness_text_key(t)
        if key in seen:
            report.duplicates += 1
            continue
        seen.add(key)
        fresh.append(t)

    room = max(0, cap - len(state.entries))
    if len(fresh) > room:
        report.over_cap = len(fresh) - room
        fresh = fresh[:room]

    for i, t in enumerate(fresh):
        entry_label = (label or '').strip()
        if entry_label and len(fresh) > 1:
            entry_label = f'{entry_label} {i + 1}'
        state.entries.append(WitnessEntry(
            id=new_id(state),
            label=entry_label or default_label(t, fallback_label),
            kind=kind,
            sys_id=sys_id,
            seed_digest=(seed_digest if seed_digest is not None
                         else witness_text_key(seed_text)),
            headers=list(headers or []),
            text=t,
        ))
        report.added.append(state.entries[-1])
    return report


# --- removing --------------------------------------------------------------

def remove(state: WitnessSet, wid: str) -> bool:
    """Drop a witness AND the rows it contributed.

    Leaving the rows would have the panel say the witness is gone while its
    results -- up to a few thousand of them, for a witness that found nothing
    useful -- stayed on screen with no way to attribute or remove them.

    Returns whether the caller can actually RE-STRIP those rows, which is only
    true if some other witness's rows survive to re-fuse from. After a session
    restore the per-witness caches are deliberately empty (per-witness ranks
    cannot be recovered from fused rows), so the rows on screen keep the
    removed witness's contributions whatever happens here. The honest move is
    to say so, not to delete a restored result set that exists nowhere else.
    """
    state.entries = [e for e in state.entries if e.id != wid]
    state.rows.pop(wid, None)
    state.filtered.pop(wid, None)
    return bool(state.rows)


def invalidate_cache(state: WitnessSet, key) -> bool:
    """Drop the row caches when they answer a DIFFERENT question. Returns
    whether anything was dropped.

    The caches exist so an auto-expand round costs one search per NEW witness
    instead of re-running the whole roster. That reuse is sound only while the
    rows keep answering the same question, and nothing about a `WitnessSet`
    notices when the question changes: `mark_stale_against` reads the seed
    digest but touches only `pending` entries, so a witness already `searched`
    keeps both its status and its rows.

    Left uninvalidated, the failure is not a stale-LOOKING result -- it is a
    silent one. A second run after an edit finds the seed's rows cached and
    every witness `searched`, dispatches NOTHING, and re-publishes the previous
    query's rows as though they were the new query's answer.

    `key` is opaque here on purpose: which inputs a row depends on is a
    property of the SEARCH, not of this list, so the surface builds it (see
    `genizah_app.py::_comp_witness_cache_key`). Only equality is used.

    Entries go back to `pending` rather than being dropped -- they are still
    witnesses of the same work, they simply have no results any more. Only
    `searched` ones are touched: a `failed` witness has no rows to invalidate,
    and silently re-queueing it would retry a known failure on every settings
    change instead of leaving the user their explicit Retry.
    """
    if state.cache_key == key:
        return False
    had = bool(state.rows or state.filtered)
    state.rows.clear()
    state.filtered.clear()
    for e in state.entries:
        if e.status == STATUS_SEARCHED:
            e.status = STATUS_PENDING
            e.hits = 0
    state.cache_key = key
    return had


# --- staleness -------------------------------------------------------------

def mark_stale_against(state: WitnessSet, seed_text: str) -> int:
    """Mark every witness gathered for a DIFFERENT source text.

    Measured against the text about to be searched, so it is read live. Only
    a witness that would otherwise be dispatched is marked -- one already
    `searched` or `failed` describes a run that really happened.
    """
    digest = witness_text_key(seed_text)
    n = 0
    for e in state.entries:
        if e.status == STATUS_PENDING and e.seed_digest != digest:
            e.status = STATUS_STALE
            n += 1
    return n


def revive_stale(state: WitnessSet, seed_text: str) -> int:
    """Adopt the stale witnesses into the CURRENT source text.

    Re-stamping the digest is the point: without it they go stale again on the
    next search and the user answers the same question twice.
    """
    digest = witness_text_key(seed_text)
    n = 0
    for e in state.entries:
        if e.status == STATUS_STALE:
            e.status = STATUS_PENDING
            e.seed_digest = digest
            n += 1
    return n


def remove_stale(state: WitnessSet) -> int:
    stale = [e.id for e in state.entries if e.status == STATUS_STALE]
    for wid in stale:
        remove(state, wid)
    return len(stale)


# --- fusion ----------------------------------------------------------------

def fuse_all(state: WitnessSet):
    """Rebuild `(main, filtered)` from the per-witness rows.

    Returns `None` when there is nothing to fuse FROM, which is NOT the same
    as "the result set is empty" -- conflating the two destroyed data on the
    web. After a restore the caches are empty while the fused rows are on
    screen; rows this function did not produce are not its to discard, so the
    caller keeps what it has and the panel says the witnesses need re-running.

    With a single searched witness the rows pass through UNTOUCHED and carry
    no fusion fields: RRF over one list is a 1/(k+rank) rescale that carries
    no information, and `score` must keep meaning matched letters for the
    common case. This mirrors the engine's own short-circuit exactly, so a
    one-witness desktop search and a one-witness API search agree.
    """
    live = [wid for wid in order(state) if state.rows.get(wid) is not None]
    if not live:
        return None
    if len(live) == 1:
        wid = live[0]
        return (list(state.rows.get(wid) or []),
                list(state.filtered.get(wid) or []))

    lbl = labels(state, '')
    main_pairs, filt_pairs = [], []
    for wid in live:
        main, filt = rank_and_route(
            list(state.rows.get(wid) or []),
            list(state.filtered.get(wid) or []),
            wid, lbl.get(wid, ''))
        main_pairs.append((wid, main))
        filt_pairs.append((wid, filt))
    return fuse_routed(main_pairs, filt_pairs)


def fusion_set(roster, rows, filtered=None) -> WitnessSet:
    """A `WitnessSet` assembled for ONE fusion, from a roster and row caches.

    `roster` is `[(witness_id, label), ...]` in FUSION ORDER, and it is the
    caller's full picture -- every witness whose rows should take part,
    including ones searched in an earlier round and not re-run now. That is
    what keeps an R-round auto-expansion linear: round three fuses rounds one
    and two from cache and searches only the K witnesses it just promoted,
    so the cost is `1 + rounds x K` searches rather than re-running everything
    every time.

    Order is not cosmetic. `fuse()` breaks rank ties by witness POSITION, so
    two rosters holding the same witnesses in different orders can rank the
    same records differently. The roster the caller supplies is authoritative
    and is reproduced exactly; the seed, which `order()` always puts first,
    is skipped here rather than duplicated.
    """
    state = WitnessSet()
    for wid, label in roster:
        if wid == WITNESS_SEED_ID:
            continue
        state.entries.append(WitnessEntry(id=wid, label=label or ''))
    state.rows = {k: list(v) for k, v in (rows or {}).items()}
    state.filtered = {k: list(v) for k, v in (filtered or {}).items()}
    return state


def fuse_and_cap(state: WitnessSet, cap: int = None):
    """`fuse_all`, then the group cap -- in that order, which is the whole
    point of the multi-witness render-cap change.

    Each witness is searched UNCAPPED (`render_cap=0`) so that its full
    result list reaches the fusion. Capping per witness first would fuse N
    already-truncated lists and silently drop every contributor that sat past
    rank 200 in its own witness -- which is exactly where a rare witness of a
    widely-copied work shows up. The cap belongs here, once, on the fused
    list.

    `order_key='fusion_score'` decides WHICH groups survive. Without it the
    cap keeps the groups with the most raw matched letters, which is the
    ranking the fusion exists to replace: a manuscript found by six witnesses
    would be discarded in favour of one long single-witness match. The
    parameter exists on `_cap_main_results_by_group` for precisely this path.

    Returns `(main, filtered, truncated)`; `truncated` says the reader is
    NOT looking at everything, which is a fact a search must never withhold.
    Returns `None` when there is nothing to fuse from, exactly as `fuse_all`
    does -- an empty result and no result are different answers.
    """
    from shared.parallels_service import (
        PARALLELS_GROUP_CAP, _cap_main_results_by_group)
    from shared.passage_parallels import _RegexSysIdParser

    fused = fuse_all(state)
    if fused is None:
        return None
    main, filtered = fused
    limit = PARALLELS_GROUP_CAP if cap is None else cap
    if not limit or limit <= 0:
        return main, filtered, False

    # Only a FUSED list carries `fusion_score`; the single-witness
    # short-circuit deliberately leaves rows untouched, and ordering that by a
    # key none of them has would collapse the page into one tie.
    order_key = 'fusion_score' if searched_count(state) > 1 else None
    parser = _RegexSysIdParser()
    capped_main, truncated = _cap_main_results_by_group(
        main, parser, cap=limit, order_key=order_key)
    # Both buckets, by the same function and the same cap, so rendered rows
    # and kept rows never disagree.
    capped_filtered, _ = _cap_main_results_by_group(
        filtered, parser, cap=limit, order_key=order_key)
    return capped_main, capped_filtered, truncated


# --- persistence -----------------------------------------------------------

def snapshot(state: WitnessSet) -> list:
    """Serialise for the session file.

    A MANUSCRIPT witness is stored WITHOUT its text: the corpus still has it,
    and copying up to 25 x 20,000 characters of corpus text into a session
    file buys nothing. It must be re-fetched before dispatch -- a witness that
    searches the empty string comes back `searched, 0 matches`, a false
    negative indistinguishable from a real one.
    """
    out = []
    for e in state.entries:
        row = {
            'id': e.id, 'label': e.label, 'kind': e.kind,
            'sys_id': e.sys_id, 'seed_digest': e.seed_digest,
            'headers': list(e.headers or []),
        }
        if not (e.kind == 'manuscript' and e.sys_id):
            row['text'] = e.text
        out.append(row)
    return out


def restore(raw, fallback_label: str,
            cap: int = DESKTOP_WITNESS_CAP) -> WitnessSet:
    """Rebuild a `WitnessSet` from a snapshot.

    Delegates the entry rules to `shared/passage_witness_source.py` so the
    desktop and the web restore the same list from the same bytes -- including
    the one that is easy to get wrong: a manuscript witness with a `sys_id`
    survives WITHOUT text (the rehydrator puts it back), while anything else
    textless is dropped, because nothing in the world can recover it.

    Every witness comes back `pending` and the row caches come back EMPTY: the
    snapshot holds fused rows, per-witness ranks cannot be recovered from
    them, and a fusion rebuilt from partial inputs would be quietly wrong
    rather than visibly absent.
    """
    from shared.passage_witness_source import restore_witness_entries

    state = WitnessSet()
    for d in restore_witness_entries(raw, fallback_label, cap=cap):
        state.entries.append(WitnessEntry(
            id=d['id'], label=d['label'], kind=d['kind'], sys_id=d['sys_id'],
            seed_digest=d['seed_digest'], headers=list(d['headers'] or []),
            text=d['text'], status=STATUS_PENDING,
        ))
    # `restore_witness_entries` renumbers survivors w1..wN, so the counter has
    # to follow or `new_id` would re-issue an id that is already in use.
    state.seq = len(state.entries)
    return state
