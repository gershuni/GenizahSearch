# -*- coding: utf-8 -*-
"""Seam documents and the reversed-words field (Responsa plan, Phase 2).

docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md §6.1 is the design; this module is its
pure half (no tantivy writer, no I/O), so the indexer, the measurement scripts and
the tests build seams with one piece of code.

A **run** is a sequence of pages that follow each other in reading order: one
source (V0.8 or V0.7), one volume (IE), each page number one more than the last.
A whole-manuscript document joins far more than that -- the V0.8 pages and then
the V0.7 copy of the same volume restarting at page 1, volume changes, missing
pages (§3.6.5: 226,926 of 1,751,787 junctions) -- so a match across one of its
"page breaks" can join a manuscript's end to its own beginning.

A **chain** is one run, or runs of consecutive folios of one Oxford part linked
end to start (the last page of folio N, the first page of folio N+1, one
source): the reading order a part document was built for.

A **seam** is the text around one junction of a chain: *words* word-holding
chunks on each side (the window ``_cross_page_spans`` cuts, with the same
helpers), crossing further page breaks of the chain when its pages are shorter.
So a crossing match whose side is at most *words* words lies inside the seam
centred on the first break it crosses (``tests/test_cross_page_spans.py``'s
three-page match included).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

# K, the words on each side of a seam's junction (§6.1). The format marker records the
# value an index was built with; the engine reads it from there, never from here.
SEAM_WORDS_EACH_SIDE = 30

# The version of the main index's layout this code writes (index_format.json).
# 1 = no marker (every index before Phase 2); 2 = seams + the reversed-words field.
INDEX_FORMAT_VERSION = 2

REVERSED_WORDS_FIELD = 'content_rev'
SEAM_SCOPE = 'seam'
SEAMS_SUBDIR = 'seams'

_IE_RE = re.compile(r'(IE\d+)')


def _ie_of(page) -> str:
    m = _IE_RE.search(page.get('uid') or '') or _IE_RE.search(page.get('full_header') or '')
    return m.group(1) if m else ''


def _p_num(page) -> int:
    try:
        return int(page.get('p_num') or 0)
    except (TypeError, ValueError):
        return 0


def follows(a, b) -> bool:
    """True when page *b* comes right after page *a* in one volume of one transcription:
    same source, same sys_id, same IE, page number + 1 (and both numbered)."""
    pa, pb = _p_num(a), _p_num(b)
    return (pa > 0 and pb == pa + 1
            and (a.get('source') or '') == (b.get('source') or '')
            and (a.get('sys_id') or '') == (b.get('sys_id') or '')
            and _ie_of(a) == _ie_of(b) != '')


def runs_of(pages: List[dict]) -> List[List[dict]]:
    """One manuscript's pages (file order) cut into runs: a new run wherever the next
    page does not follow the last one (``follows``)."""
    runs: List[List[dict]] = []
    for page in pages:
        if runs and follows(runs[-1][-1], page):
            runs[-1].append(page)
        else:
            runs.append([page])
    return runs


@dataclass
class ChainStats:
    """What chains_for found, for the build log and the measurements."""
    runs: int = 0
    run_junctions: int = 0
    bridges: int = 0
    bridges_refused: Dict[str, int] = field(default_factory=dict)

    def refuse(self, why):
        self.bridges_refused[why] = self.bridges_refused.get(why, 0) + 1


def chains_for(system_pages: Dict[str, List[dict]],
               part_to_folios: Optional[Dict[str, List[str]]] = None,
               folio_number=None, max_part_pages: int = 1000,
               stats: Optional[ChainStats] = None) -> List[List[dict]]:
    """Every chain of the corpus.

    *system_pages*: sys_id -> its pages in file order (the indexer's lists: p_num, uid,
    full_header, source, sys_id, content). *part_to_folios*: Oxford part -> its sys_ids
    in folio order; *folio_number(sys_id)* -> the folio number (0 when unknown). A
    bridge joins folio A's last run in a source to folio B's first run in it when B
    comes right after A in the part, folio(B) == folio(A) + 1, A and B each have one
    volume in that source, A's run ends on its last page and B's starts on page 1.
    Parts over *max_part_pages* pages are skipped, as the indexer skips their documents.
    """
    stats = stats if stats is not None else ChainStats()
    runs_by_sys = {sid: runs_of(pages) for sid, pages in system_pages.items() if pages}
    # The chain each run belongs to: a run starts as its own chain; a bridge appends the
    # next folio's chain to it. Keyed by id(run), since runs are lists.
    next_of: Dict[int, List[dict]] = {}
    has_prev = set()
    for runs in runs_by_sys.values():
        stats.runs += len(runs)
        stats.run_junctions += sum(len(r) - 1 for r in runs)

    if part_to_folios and folio_number is not None:
        for _part, folios in part_to_folios.items():
            if sum(len(system_pages.get(s) or ()) for s in folios) > max_part_pages:
                continue
            for a, b in zip(folios, folios[1:]):
                fa, fb = folio_number(a), folio_number(b)
                for source in ('V0.8', 'V0.7'):
                    ra = [r for r in runs_by_sys.get(a, ()) if r[0].get('source') == source]
                    rb = [r for r in runs_by_sys.get(b, ()) if r[0].get('source') == source]
                    if not ra or not rb:
                        continue
                    if not (fa > 0 and fb == fa + 1):
                        stats.refuse('folio numbers not consecutive')
                        continue
                    if len({_ie_of(r[0]) for r in ra}) != 1 or len({_ie_of(r[0]) for r in rb}) != 1:
                        stats.refuse('a folio with more than one volume')
                        continue
                    last, first = ra[-1], rb[0]
                    if _p_num(last[-1]) != max(_p_num(p) for r in ra for p in r) or _p_num(first[0]) != 1:
                        stats.refuse('a missing page at the folio edge')
                        continue
                    if id(last) in next_of or id(first) in has_prev:
                        stats.refuse('already bridged')
                        continue
                    next_of[id(last)] = first
                    has_prev.add(id(first))
                    stats.bridges += 1

    chains = []
    for runs in runs_by_sys.values():
        for run in runs:
            if id(run) in has_prev:
                continue            # it is inside the chain that starts before it
            chain = list(run)
            nxt = next_of.get(id(run))
            while nxt is not None:
                chain.extend(nxt)
                nxt = next_of.get(id(nxt))
            chains.append(chain)
    return chains


@dataclass
class Seam:
    unique_id: str
    text: str
    cut: int                       # the junction, in the seam's text
    slices: List[dict]             # provenance, one per page slice (see seams_of)
    left: dict
    right: dict


def seams_of(chain: List[dict], words: int = SEAM_WORDS_EACH_SIDE) -> Iterable[Seam]:
    """The seams of one chain, one per junction. Each slice records its page (uid,
    p_num, full_header, source, sys_id), where it stands in the seam (start, end) and
    where the slice starts in its page (``offset``), so a span maps back to page
    coordinates (``SearchEngine._map_span_to_pages``). The pages are joined by one
    newline, as in a whole-manuscript document, and the newline belongs to the page
    before it."""
    from shared.search_engine import _left_window_start, _right_window_end

    if len(chain) < 2:
        return
    parts, starts, ends = [], [], []
    cursor = 0
    for i, page in enumerate(chain):
        text = page.get('content', '') or ''
        starts.append(cursor)
        parts.append(text)
        cursor += len(text)
        if i != len(chain) - 1:
            parts.append('\n')
            cursor += 1
        ends.append(cursor)
    content = ''.join(parts)
    for j in range(1, len(chain)):
        cut = starts[j]
        lo = _left_window_start(content, cut, words)
        hi = _right_window_end(content, cut, words)
        slices = []
        for i in range(len(chain)):
            s, e = max(lo, starts[i]), min(hi, ends[i])
            if s >= e and not (i == j and s == e == cut):
                continue
            page = chain[i]
            slices.append({
                'uid': page.get('uid'), 'p_num': page.get('p_num'),
                'full_header': page.get('full_header', ''), 'source': page.get('source', ''),
                'sys_id': page.get('sys_id', ''),
                'start': s - lo, 'end': e - lo, 'offset': s - starts[i],
            })
        left, right = chain[j - 1], chain[j]
        # The sys_id too: 2,725 junctions of the corpus repeat under a second manuscript
        # that shares the volume's images, and each manuscript keeps its own seam.
        yield Seam(unique_id=f"seam:{left.get('source', '')}:{left.get('sys_id', '')}:{left.get('uid')}>{right.get('uid')}",
                   text=content[lo:hi], cut=cut - lo, slices=slices, left=left, right=right)


def reversed_words(folded_text: str, analyzer) -> str:
    """The reversed-words field's text: every token the hebword tokenizer makes of
    *folded_text* (the ``content_search`` text), written backwards, space-separated.
    *analyzer*: the hebword analyzer itself (``hebword_analyzer()``), so the tokens are
    the index's own, never a Python re-implementation of them."""
    return ' '.join(tok[::-1] for tok in analyzer.analyze(folded_text))


def reversed_ending_pattern(ending: str) -> Optional[str]:
    """The ``regex_query`` pattern on the reversed-words field for the words that end with
    *ending* (folded here as content_search is), or None for an empty ending. Brackets
    and Hebrew marks are looked past between its letters and after its last one, as the
    content_search lookups do (``_T_CONTENT_SEARCH``): ``נתקנים[`` is the token
    ``[םינקתנ``, ``[כהנים]`` is ``]םינהכ[`` (Codex review of PR #438)."""
    from shared.responsa_match import _T_CONTENT_SEARCH, _rust_escape
    from shared.text_normalize import strip_search_diacritics
    folded = strip_search_diacritics(ending or '')
    if not folded:
        return None
    return _T_CONTENT_SEARCH + _T_CONTENT_SEARCH.join(_rust_escape(c) for c in reversed(folded)) + '.*'


def hebword_analyzer():
    """The hebword tokenizer as a tantivy TextAnalyzer (``analyze(text)`` -> tokens)."""
    import tantivy
    from shared.search_tokenizer import HEBWORD_TOKENIZER_PATTERN
    return tantivy.TextAnalyzerBuilder(tantivy.Tokenizer.regex(HEBWORD_TOKENIZER_PATTERN)).build()
