# -*- coding: utf-8 -*-
"""How much the multiple-choice shelf picker takes, and the names its chips carry (owner, 2026-11-21).

The owner: picking thousands of shelfmarks together is not reasonable, so it is limited from the start,
when ticking, not when the host applies what was ticked; and the Reading Desk is limited by pages, not by
a number of shelfmarks (an RNL shelfmark holds hundreds of pages, which the desk handles). Both apps' pickers
ask a ``SelectionLimit`` before a tick is kept, and their hosts ask it again when they apply the choice,
since what is on the desk may have changed meanwhile.

* A limit counts the union of what is ticked and what the host already holds (``base_ids``: the desk's
  manuscripts, with the one shown that a new desk begins with; the Puzzle's canvas), each manuscript once.
* Only a change that adds is refused: unticking, and a host taking what was applied, always pass, even
  over a limit (a kept choice from before the limits, or a desk that grew meanwhile).
* A node holding more manuscripts than the limit allows is too big to tick whole (``node_too_big``):
  read from the node's stored counts, so drawing a column costs no lookups -- all of it for a node's box,
  its own shelfmarks only for "Select all" over a node that also holds volumes (``scope_for``: DIRECT;
  Codex on #80). A smaller one is measured when it is ticked; ``count_too_big`` for a count already known.
* The Reading Desk's pages are counted never below what it draws: the larger of the index's pages
  (``SearchEngine.page_count``) and what the app's desk draws for a manuscript without them -- the web the
  two images of one leaf (``web/pages/browse_desk_images.py::desk_images``), the desktop every image, so it
  passes its own count as ``fallback`` (Codex on #80: one rule for both apps undercounted the desktop).

Per purpose (``limit_for``): the Reading Desk up to 1,000 pages and 30 manuscripts; the Puzzle and a join
30 fragments; a list 2,000; an exclusion unlimited (excluding a whole collection is what it is for).

``chip_name`` is a chip's name with its library's code first ("CUL T-S 32", "RNL Yevr.-Arab. I 1700"):
the manuscript's own code when the host can say it (``code_of``, its ``libraries.csv`` ``library_code``:
a library node may hold several, Penn's "Katz" and "UPenn"), else the library node's code.

Pure: no Qt, no NiceGUI.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Optional

from shared.shelfmark_tree_selection import ALL, DIRECT, NODE, Chip
from shared.shelfmark_tree_view import display_name

READING_DESK_MAX_PAGES = 1000
READING_DESK_MAX_ITEMS = 30
PUZZLE_MAX_ITEMS = 30
JOIN_MAX_ITEMS = 30
LIST_MAX_ITEMS = 2000
#: The images the desk draws for a manuscript the index has no pages of (one leaf, both sides).
UNTRANSCRIBED_DESK_PAGES = 2

#: The limits of each purpose a host opens the picker for: (manuscripts, pages). None: no limit.
PURPOSE_LIMITS = {
    'reading_desk': (READING_DESK_MAX_ITEMS, READING_DESK_MAX_PAGES),
    'puzzle': (PUZZLE_MAX_ITEMS, None),
    'join': (JOIN_MAX_ITEMS, None),
    'lists': (LIST_MAX_ITEMS, None),
}

#: The purposes whose items are fragments, and whose limit says so: the Puzzle's canvas, a join (Codex on #80).
FRAGMENT_PURPOSES = frozenset({'puzzle', 'join'})

PagesOf = Callable[[str], int]
BaseIds = Callable[[], Iterable[str]]


@dataclass(frozen=True)
class Refusal:
    """Why a tick was not kept: the measure passed (``items``, ``fragments`` -- items that are fragments --
    or ``pages``), its limit, and the total the tick would have made."""
    unit: str
    limit: int
    total: int


class SelectionLimit:
    """See the module docstring. ``pages_of(sys_id)`` is needed with ``max_pages``; ``base_ids()`` answers
    what the host holds now (read at each question, so a desk that grew is counted). ``base_items()``,
    for a host whose items are not one per manuscript (the Puzzle: two folios of one manuscript are two
    fragments; Codex on #82), answers how many items it holds instead: each manuscript ticked is then one
    more item, whether or not it holds that manuscript already."""

    def __init__(self, max_items: Optional[int] = None, max_pages: Optional[int] = None,
                 pages_of: Optional[PagesOf] = None, base_ids: Optional[BaseIds] = None,
                 base_items: Optional[Callable[[], int]] = None, fragments: bool = False):
        if max_pages is not None and pages_of is None:
            raise ValueError("a page limit needs pages_of")
        if base_items is not None and (base_ids is not None or max_pages is not None):
            raise ValueError("base_items counts items alone: no base_ids, no page limit")
        self.max_items = max_items
        self.max_pages = max_pages
        self._pages_of = pages_of
        self._base_ids = base_ids
        self._base_items = base_items
        self.fragments = fragments                  # its items are fragments: its refusal says so

    def _held_items(self) -> int:
        try:
            return max(0, int(self._base_items())) if self._base_items else 0
        except Exception:   # noqa: BLE001 -- a host that cannot count holds nothing more
            return 0

    def base(self) -> frozenset:
        try:
            return frozenset(s for s in (self._base_ids() if self._base_ids else ()) if s)
        except Exception:   # noqa: BLE001 -- a host that cannot say what it holds holds nothing more
            return frozenset()

    def _pages(self, sys_id: str) -> int:
        try:
            return max(1, int(self._pages_of(sys_id)))
        except Exception:   # noqa: BLE001 -- an unknown count is one page, never a crash
            return 1

    def totals(self, ids: Iterable[str], base: Optional[frozenset] = None,
               items: Optional[int] = None) -> tuple[int, int]:
        """(manuscripts, pages) of ``ids`` with what the host holds, each manuscript once. Pages are
        counted only under a page limit (0 otherwise). ``base`` / ``items``: what the host holds, read
        once by the caller, so two totals compare like with like."""
        held = (self.base() if base is None else base) | frozenset(ids)
        pages = sum(self._pages(s) for s in held) if self.max_pages is not None else 0
        return len(held) + (self._held_items() if items is None else items), pages

    def refusal(self, before: Iterable[str], after: Iterable[str]) -> Optional[Refusal]:
        """None when the change from ``before`` to ``after`` (what is ticked) may be kept: within the
        limits, or adding nothing to a measure that is already over. Removals always pass: a change
        that only takes out passes before anything is counted, and what the host holds is read once
        for both totals (Codex on #82: the Puzzle's count read twice refused an untick)."""
        after = frozenset(after)
        if after <= frozenset(before):
            return None
        base, items = self.base(), self._held_items()
        b_items, b_pages = self.totals(before, base, items)
        a_items, a_pages = self.totals(after, base, items)
        if self.max_items is not None and a_items > self.max_items and a_items > b_items:
            return Refusal('fragments' if self.fragments else 'items', self.max_items, a_items)
        if self.max_pages is not None and a_pages > self.max_pages and a_pages > b_pages:
            return Refusal('pages', self.max_pages, a_pages)
        return None

    def count_too_big(self, count: int) -> bool:
        """More manuscripts than the limit allows (each at least one page)."""
        n = int(count or 0)
        return ((self.max_items is not None and n > self.max_items)
                or (self.max_pages is not None and n > self.max_pages))

    def node_too_big(self, node, scope: str = ALL, service=None) -> bool:
        """A node too big to tick whole in ``scope``: ``ALL`` (its box) by its stored count of distinct
        manuscripts, nothing read; ``DIRECT`` ("Select all" over a node that also holds volumes) by its
        own shelfmarks only -- a collection with thousands in its volumes and a few of its own is not too
        big for those few (Codex on #80). Its stored ``direct_leaves`` counts placements, which can name a
        manuscript twice, so it is only a bound: past the limit, the node's own distinct manuscripts are
        counted (``service``, one node), and without a service it is not called too big (the tick's own
        check, which counts exactly, decides)."""
        if node is None:
            return False
        if scope != DIRECT:
            return self.count_too_big(getattr(node, 'manuscripts', 0))
        if not self.count_too_big(getattr(node, 'direct_leaves', 0)):
            return False
        if service is None:
            return False
        try:
            return self.count_too_big(len(service.members_under(node.id, True)))
        except Exception:   # noqa: BLE001 -- unknown: the tick's own check decides
            return False

    def take(self, ids_in_order: Iterable[str]) -> list:
        """What a host applies: the ids, in the order chosen, while they fit with what it holds (one it
        holds already is taken at no cost). It stops at the first that does not fit, so what is applied
        is a beginning of what was chosen and the rest stays ticked in its order."""
        held = set(self.base())
        items, pages = self.totals((), frozenset(held), self._held_items())
        out = []
        for sys_id in ids_in_order:
            if sys_id in held:
                out.append(sys_id)
                continue
            more = self._pages(sys_id) if self.max_pages is not None else 0
            if self.max_items is not None and items + 1 > self.max_items:
                break
            if self.max_pages is not None and pages + more > self.max_pages:
                break
            items, pages = items + 1, pages + more
            held.add(sys_id)
            out.append(sys_id)
        return out


def limit_for(purpose: str, pages_of: Optional[PagesOf] = None, base_ids: Optional[BaseIds] = None,
              base_items: Optional[Callable[[], int]] = None) -> Optional[SelectionLimit]:
    """The limit of a purpose (``PURPOSE_LIMITS``), None for one without (an exclusion). The Puzzle
    passes ``base_items`` (its canvas's fragments), the Reading Desk ``base_ids`` and ``pages_of``."""
    if purpose not in PURPOSE_LIMITS:
        return None
    max_items, max_pages = PURPOSE_LIMITS[purpose]
    return SelectionLimit(max_items, max_pages if pages_of is not None else None, pages_of, base_ids,
                          base_items, fragments=purpose in FRAGMENT_PURPOSES)


def desk_pages_function(page_count: PagesOf, fallback: Optional[PagesOf] = None) -> PagesOf:
    """The Reading Desk's pages of a manuscript, never below what it draws: the larger of the index's
    (``page_count``) and ``fallback``, what the app's desk draws for a manuscript without pages (by
    default the web's two images of one leaf; the desktop draws every image and passes that count).
    The index may count a page whose text is empty, which the desk drops (Codex on #80): taking the
    larger keeps such a manuscript at its fallback."""
    def pages_of(sys_id: str) -> int:
        try:
            n = int(page_count(sys_id) or 0)
        except Exception:   # noqa: BLE001 -- counted as the desk would draw it without pages
            n = 0
        try:
            least = int(fallback(sys_id)) if fallback is not None else UNTRANSCRIBED_DESK_PAGES
        except Exception:   # noqa: BLE001 -- the web's rule when the host's count fails
            least = UNTRANSCRIBED_DESK_PAGES
        return max(n, least, 1)
    return pages_of


def refusal_message(refusal: Refusal, tr: Callable[[str], str]) -> str:
    """What the reader is told when a tick is not kept."""
    if refusal.unit == 'pages':
        return tr('The Reading Desk holds up to {n} pages; these would make {total}').format(
            n=f"{refusal.limit:,}", total=f"{refusal.total:,}")
    if refusal.unit == 'fragments':
        return tr('Up to {n} fragments here').format(n=f"{refusal.limit:,}")
    return tr('Up to {n} manuscripts here').format(n=f"{refusal.limit:,}")


# -- the names on the chips ----------------------------------------------------------------------

def library_code(service, library_id: str) -> str:
    """The code the apps use for a library node ("CUL", "RNL", "HAS"): its short name when that is one
    of its codes, else its code (the first A-Z when it has several: a guess, so ``chip_name`` asks the
    host for the manuscript's own code first), else its short name."""
    if not library_id:
        return ''
    if library_id.startswith('code:'):
        return library_id[len('code:'):]
    try:
        codes = service.codes_of_library(library_id)
        node = service.node(library_id)
    except Exception:   # noqa: BLE001 -- a chip is named without its code rather than not at all
        return ''
    short = (node.short if node is not None else '') or ''
    if short and short in codes:
        return short
    return codes[0] if codes else short


def _own_code(code_of: Optional[Callable[[str], str]], sys_id: Optional[str]) -> str:
    if code_of is None or not sys_id:
        return ''
    try:
        return (code_of(sys_id) or '').strip()
    except Exception:   # noqa: BLE001 -- the library node's code instead
        return ''


def chip_name(service, chip: Chip, lang: str = 'en', code_of: Optional[Callable[[str], str]] = None) -> str:
    """A chip's name as both apps show it: its library's code, then the node's name in the reader's
    language or the shelfmark ("CUL T-S 32"). The code is the manuscript's own (``code_of(sys_id)``;
    for a node, one of its manuscripts', the node's chosen set being known already) when the host gives
    it, else the library node's. A name that already begins with the code keeps it once."""
    if chip.kind == NODE:
        node = service.node(chip.ref)
        name = display_name(node, lang) if node is not None else chip.label
        ancestors = service.ancestors(chip.ref) if node is not None else ()
        library_id = ancestors[0].id if ancestors else ''
        sample = None
        if code_of is not None and node is not None:
            try:
                # one of the manuscripts the chip still holds (Codex on #80: not one unticked inside it)
                held = service.members_under(chip.ref, chip.scope == DIRECT) - set(chip.removed or ())
                sample = min(held, default=None)
            except Exception:   # noqa: BLE001 -- named by the library node instead
                sample = None
        code = _own_code(code_of, sample)
    else:
        leaf = service.leaf(chip.ref)
        name = chip.label
        library_id = leaf.library_id if leaf is not None else ''
        code = _own_code(code_of, chip.sys_id)
    code = code or library_code(service, library_id)
    if code and not (name or '').lower().startswith(code.lower()):
        return f"{code} {name}".strip()
    return name
