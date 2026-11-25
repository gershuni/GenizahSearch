# -*- coding: utf-8 -*-
"""The first rows of a search with the scholarly editions, while the main index is still read.

Editions plan (private repository) §3.3 item 7, the "stable-prefix merged preview". The two legs'
rows become one list by reciprocal rank fusion (``editions_search.group_results``) and are then
folded (``fold_same_text``), so what the first result is depends on rows found later. But not on
all of them: with the editions leg complete and the main leg's first *k* rows settled, each
result's fused score lies between two bounds, and a result whose lower bound no other result can
pass is where it will be in the finished list. ``GroupedPrefix`` gives that start of the list.

What it needs of the main leg beyond its settled rows: whether the pages the editions found (and
the manuscripts of editions whose page is not named, which ``fold_same_text`` joins by manuscript)
also match there. Those pages are few, and the engine checks them out of turn when asked
(``wanted`` / ``checked``; SearchEngine._execute_search_leg). Nothing else in the main leg can
meet a result already placed (``editions_search.counterparts`` has the argument).

No row is shown on a guess: where the bounds do not decide, the prefix stops there and grows as
more of the main leg is read.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from shared.editions_search import (RRF_K, _fold_clusters, _folded_row, _group_ranked, _hit_key, _row_sys_id,
                                    group_key)

LOGGER = logging.getLogger("genizah." + __name__)

# Results this far down a leg are not looked at one by one: the first ones (a preview shows 50)
# score more than any of them can, and what they can score is taken together.
_REACH = 400
# The editions and the main rows ranked this high whose pages are looked up in the main index
# before any is needed; one further down is looked up when the prefix meets it.
_LOOKUP_REACH = 160
# How often a source of edition rows that is not there yet is asked again.
_POLL_SECONDS = 0.1


class _Links:
    """Which groups can end up in one result: connected by a hit they share (the same match of
    one edition), or by a manuscript where an edition's page is not named (disjoint sets)."""

    def __init__(self):
        self._parent: Dict[Any, Any] = {}

    def find(self, x):
        parent = self._parent
        root = parent.setdefault(x, x)
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    def join(self, a, b):
        a, b = self.find(a), self.find(b)
        if a != b:
            self._parent[b] = a


class GroupedPrefix:
    """The certain start of a search's grouped list (module docstring).

    *edition_rows*: the editions leg's rows, all of them, in rank order. ``add_main`` takes the
    main leg's rows as they are found. ``wanted`` names pages and manuscripts whose main rows
    must be known; ``checked`` takes them, every row the main leg has there. ``rows`` is the
    prefix.

    A main row's *order* is its place among the rows of its source in the order the main leg
    reads them (the list keeps a page's first V0.8 row, then, after all of those, V0.7's:
    ``SearchEngine._deduplicate``)."""

    def __init__(self, edition_rows: Sequence[Dict[str, Any]], k: int = RRF_K):
        self._k = k
        self._groups: Dict[str, List[Tuple[int, Dict[str, Any]]]] = {}    # key -> [(rank, edition row)]
        self._order: List[str] = []                                       # the keys, best rank first
        self._unplaced: Set[str] = set()          # keys that are an edition's own uid: no page
        self._unplaced_sids: Set[str] = set()     # their manuscripts
        for rank, row in enumerate(edition_rows, start=1):
            key = group_key(row)
            if key not in self._groups:
                self._groups[key] = []
                self._order.append(key)
            self._groups[key].append((rank, row))
            if key == (row.get("edition") or {}).get("uid"):
                self._unplaced.add(key)
                sid = _row_sys_id(row)
                if sid:
                    self._unplaced_sids.add(sid)
        self._links = links = _Links()
        for key, members in self._groups.items():
            node = ("group", key)
            for _rank, row in members:
                hit = _hit_key(row)
                if hit:
                    links.join(node, ("hit", hit))
                sid = _row_sys_id(row)
                if sid in self._unplaced_sids:
                    links.join(node, ("sid", sid))
        self._component: Dict[Any, List[str]] = {}         # linked groups, by their set
        for key in self._order:
            self._component.setdefault(links.find(("group", key)), []).append(key)
        self._component_sids: Dict[Any, List[str]] = {}
        for sid in sorted(self._unplaced_sids):
            self._component_sids.setdefault(links.find(("sid", sid)), []).append(sid)
        self._main: List[Dict[str, Any]] = []        # settled: the row at index i ranks i + 1
        self._rank: Dict[str, int] = {}              # page -> its settled rank
        # A page's row known before its rank is: (order, the rank it has at most or None, row).
        self._late: Dict[str, Tuple[Any, Optional[int], Dict[str, Any]]] = {}
        self._sid_pages: Dict[str, Dict[str, None]] = {}   # unplaced manuscript -> its known main pages
        self._asked: Set[Any] = set()
        self._pending: List[Any] = []
        self._known: Set[Any] = set()                # components whose main rows are all known
        self._met: Dict[Any, None] = {}              # components the prefix stopped at, not known

    # -- the main leg -----------------------------------------------------------------------

    def add_main(self, row: Dict[str, Any], order: Any) -> None:
        """The next row the main leg found (its first for the page and source counts)."""
        uid = row["uid"]
        if uid in self._rank:
            return
        if (row.get("display") or {}).get("source") != "V0.8":
            self._add_late(row, order, None)
            return
        self._main.append(row)
        self._rank[uid] = len(self._main)
        self._late.pop(uid, None)
        self._note_sid(uid, row)

    def _add_late(self, row, order, most):
        uid = row["uid"]
        if uid in self._rank:
            return
        last = (row.get("display") or {}).get("source") != "V0.8"     # after every V0.8 row
        order, most = (last, order), None if last else most
        held = self._late.get(uid)
        if held is None or order < held[0]:
            if uid not in self._groups and _row_sys_id(row) not in self._unplaced_sids:
                return          # a result of its own, below every settled one
            self._late[uid] = (order, most, row)
            self._note_sid(uid, row)

    def _note_sid(self, uid, row):
        sid = _row_sys_id(row)
        if sid in self._unplaced_sids:
            self._sid_pages.setdefault(sid, {})[uid] = None

    @property
    def settled(self) -> int:
        return len(self._main)

    @property
    def asks(self) -> bool:
        """Did the last ``rows`` stop at something ``wanted`` will name?"""
        return any(component not in self._asked for component in self._met)

    def _component_of(self, key: str):
        if key in self._groups:
            return self._links.find(("group", key))
        rank = self._rank.get(key)
        row = self._main[rank - 1] if rank is not None else self._late[key][2]
        sid = _row_sys_id(row)
        return self._links.find(("sid", sid)) if sid in self._unplaced_sids else None

    def wanted(self) -> Tuple[Set[str], Set[str], Set[str]]:
        """``(pages, manuscripts, the pages' manuscripts)`` whose main rows are needed and were
        not asked for yet: those of the components (_Links) of the best editions, of the best
        main rows, and of the rows the prefix last stopped at."""
        need = [self._links.find(("group", key)) for key in self._order[:_LOOKUP_REACH]]
        need += [self._component_of(row["uid"]) for row in self._main[:_LOOKUP_REACH]]
        need += self._met
        self._met = {}
        pages: Set[str] = set()
        sids: Set[str] = set()
        page_sids: Set[str] = set()
        for component in dict.fromkeys(need):
            if component is None or component in self._asked:
                continue
            self._asked.add(component)
            self._pending.append(component)
            sids.update(self._component_sids.get(component, ()))
            for key in self._component.get(component, ()):
                if key not in self._unplaced:
                    pages.add(key)
                    page_sids.update(s for s in (_row_sys_id(row) for _r, row in self._groups[key]) if s)
        if not pages and not sids:
            self.checked(())
        return pages, sids, page_sids

    def checked(self, rows: Iterable[Tuple[Any, Optional[int], Dict[str, Any]]]) -> None:
        """Every main row of what ``wanted`` last named that ``add_main`` has not given:
        ``(order, most, row)``, *most* the rank it has at most in the list, when known."""
        for order, most, row in rows:
            self._add_late(row, order, most)
        self._known.update(self._pending)
        self._pending = []

    # -- the prefix -------------------------------------------------------------------------

    def _bounds(self, key: str) -> Tuple[float, float]:
        """The lowest and the highest fused score the key's result can have."""
        members = self._groups.get(key)
        e = 1.0 / (self._k + members[0][0]) if members else 0.0
        rank = self._rank.get(key)
        if rank is not None:
            exact = 1.0 / (self._k + rank) + e
            return exact, exact
        ahead = 1.0 / (self._k + self.settled + 1)
        late = self._late.get(key)
        if late is not None:
            return (1.0 / (self._k + late[1]) if late[1] else 0.0) + e, ahead + e
        if key in self._unplaced or self._links.find(("group", key)) in self._known:
            return e, e                 # its page does not match in the main index
        return e, ahead + e

    def _sort_key(self, key: str, low: float):
        """Best first; equal scores keep the order ``_group_ranked`` meets the groups in: the
        main leg's, then the editions' own."""
        rank = self._rank.get(key)
        if rank is not None:
            return (-low, 0, rank, ())
        if key in self._late:
            return (-low, 1, 0, self._late[key][0])
        return (-low, 2, self._groups[key][0][0], ())

    def rows(self, limit: int) -> List[Dict[str, Any]]:
        """The first results of the finished list, at most *limit*, as far as they are certain.
        The results of a later call begin with these."""
        pool = dict.fromkeys(row["uid"] for row in self._main[:_REACH])
        pool.update(dict.fromkeys(self._order[:_REACH]))
        bounds = {key: self._bounds(key) for key in pool}
        order = sorted(pool, key=lambda key: self._sort_key(key, bounds[key][0]))
        # What the pool leaves out: a main row below the settled or looked-at ones, an edition
        # past _REACH, or both on one page.
        rest = 1.0 / (self._k + min(self.settled, _REACH) + 1)
        if len(self._order) > _REACH:
            rest += 1.0 / (self._k + _REACH + 1)
        highest = [rest] * (len(order) + 1)
        for i in range(len(order) - 1, -1, -1):
            highest[i] = max(highest[i + 1], bounds[order[i]][1])
        placed: List[str] = []
        certain: Dict[Any, bool] = {}
        for i, key in enumerate(order):
            if len(placed) >= 2 * limit:            # folded, they are fewer results
                break
            low = bounds[key][0]
            if low < highest[i + 1]:
                break
            if low == highest[i + 1] and not (rest < low and all(
                    h < low or h == lo for lo, h in (bounds[later] for later in order[i + 1:]))):
                break       # a tie decided only where every equal score is exact (_sort_key)
            component = self._component_of(key)
            if component is not None:
                if component not in certain:
                    certain[component] = self._component_certain(component)
                if not certain[component]:
                    break
            placed.append(key)
        return self._build(placed)[:limit]

    def _component_keys(self, component) -> List[str]:
        keys = dict.fromkeys(self._component.get(component, ()))
        for sid in self._component_sids.get(component, ()):
            keys.update(self._sid_pages.get(sid, {}))
        return list(keys)

    def _component_certain(self, component) -> bool:
        """Are the component's groups all known, and their order and members the finished
        list's? Folding reads nothing else."""
        if component not in self._known:
            self._met[component] = None
            return False
        keys = self._component_keys(component)
        for key in keys:
            late = self._late.get(key)
            for rank, _row in (self._groups.get(key, ()) if late is not None else ()):
                # The page's own text stands before an edition it ranks at least as high as.
                if rank > self.settled and not (late[1] is not None and late[1] <= rank):
                    return False
        bounds = {key: self._bounds(key) for key in keys}
        keys.sort(key=lambda key: self._sort_key(key, bounds[key][0]))
        highest = 0.0
        for i in range(len(keys) - 1, -1, -1):
            low, high = bounds[keys[i]]
            if low <= highest and not all(self._before(keys[i], later, bounds) for later in keys[i + 1:]):
                return False
            highest = max(highest, high)
        return True

    def _before(self, key: str, later: str, bounds) -> bool:
        """Does *key*, sorted before *later*, stand before it in the finished list?"""
        low, high = bounds[key]
        if bounds[later][1] < low or (low == high and bounds[later][0] == bounds[later][1]):
            return True
        # Two pages of the main leg alone stand in its order.
        return (key not in self._groups and later not in self._groups
                and self._main_order(key) < self._main_order(later))

    def _main_order(self, key: str):
        rank = self._rank.get(key)
        return (0, rank, ()) if rank is not None else (1, 0, self._late[key][0])

    def _build(self, placed: Sequence[str]) -> List[Dict[str, Any]]:
        """The results whose first row is one of the groups *placed* (the start of the fused
        list): those groups and the ones linked with them, grouped and folded."""
        wanted = dict.fromkeys(placed)
        for key in placed:
            component = self._component_of(key)
            if component is not None:
                wanted.update(dict.fromkeys(self._component_keys(component)))
        start = set(placed)
        tail = sorted((key for key in wanted if key not in start),
                      key=lambda key: self._sort_key(key, self._bounds(key)[0]))
        order = [*placed, *tail]
        main = []
        for key in order:
            rank = self._rank.get(key)
            if rank is not None:
                main.append((rank, self._main[rank - 1]))
            elif key in self._late:
                _order, most, row = self._late[key]
                main.append((most or 10 ** 9, row))    # any rank its bounds allow: see above
        editions = [pair for key in order for pair in self._groups.get(key, ())]
        grouped = {row["uid"]: row for row in _group_ranked(main, editions, (), k=self._k)}
        rows = [grouped[key] for key in order]
        first = {id(row) for row in rows[:len(placed)]}
        return [_folded_row(c) for c in _fold_clusters(rows) if id(c[0]) in first]


class PreviewFeed:
    """Hands a search's grouped first rows to *callback* while its main leg runs
    (SearchEngine._execute_search_leg). *edition_rows*: the editions leg's rows, or a callable
    giving them once they are there (the website's other worker), None until then."""

    def __init__(self, callback: Callable[[List[Dict[str, Any]]], None], edition_rows):
        self._callback = callback
        self._source = edition_rows if callable(edition_rows) else (lambda: edition_rows)
        self._asked_at: Optional[float] = None
        self._waiting: List[Tuple[Dict[str, Any], Any]] = []
        self.prefix: Optional[GroupedPrefix] = None
        self.sent = 0

    def add_main(self, row: Dict[str, Any], order: Any) -> None:
        if self.prefix is not None:
            self.prefix.add_main(row, order)
        else:
            self._waiting.append((row, order))

    @property
    def settled(self) -> int:
        """Main rows found so far (those that can settle, once the editions are there)."""
        return self.prefix.settled if self.prefix is not None else len(self._waiting)

    def ready(self) -> bool:
        """Are the edition rows there?"""
        if self.prefix is not None:
            return True
        now = time.monotonic()
        if self._asked_at is not None and now - self._asked_at < _POLL_SECONDS:
            return False
        self._asked_at = now
        rows = self._source()
        if rows is None:
            return False
        self.prefix = GroupedPrefix(rows)
        for row, order in self._waiting:
            self.prefix.add_main(row, order)
        self._waiting = []
        return True

    def send(self, limit: int) -> bool:
        """Hand over the results that are certain, when they are more than before; True once
        *limit* of them have been."""
        rows = self.prefix.rows(limit)
        if len(rows) > self.sent:
            self.sent = len(rows)
            self._callback(rows)
        return self.sent >= limit


def check_preview(shown: Sequence[str], rows: Sequence[Dict[str, Any]]) -> bool:
    """Say so in the log when the early rows (*shown*: their uids) are not the start of the
    finished list: a flaw in the reasoning above, which no reader should have to find."""
    final = [row.get("uid") for row in rows[:len(shown)]]
    if list(shown) == final:
        return True
    at = next((i for i, (a, b) in enumerate(zip(shown, final)) if a != b), len(final))
    LOGGER.error("Early rows of a search with editions are not its first rows (from result %d of %d)",
                 at + 1, len(shown))
    return False
