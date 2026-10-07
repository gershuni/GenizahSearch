# -*- coding: utf-8 -*-
"""Where composition search places its word windows, and what it tells the
user when the text does not fit the requested settings.

Both composition engines query the index once per window of consecutive
words: the standard engine (SearchEngine.search_composition_logic) with a
stride of one word, Lab Mode (LabEngine.lab_composition_search) with a stride
of half a window. This module is the single placement rule for both:

* every word of a text that is long enough to search is inside at least one
  window -- a stride that stops short of the end gets one extra window that
  ends on the last word;
* a text shorter than the chunk size is searched as ONE window of all its
  words, and the result says so (``text_shorter_than_chunk_size``);
* a text too short to search at all returns no windows and says why
  (``text_too_short``) instead of an empty result that reads as "no
  parallels exist";
* a chunk size below the engine's minimum window is raised to it, and the
  result says so (``chunk_size_raised``). Lab Mode's minimum is four words
  (its fingerprint match is not selective below that); the standard
  engine's is two.

Two more notices come from the search itself: ``min_chunk_matches_lowered``
(the minimum-chunk-matches filter asked for more chunks than the text has, or
than Lab Mode searches; :func:`cap_min_chunk_matches`) and ``text_too_common``
(Lab Mode skipped every window as statistically weak, so nothing was searched).

Each engine returns its notices as ``composition_notices`` (a list, empty for
an ordinary run). The public API appends them to ``warnings[]``; the web page
and the desktop window show them with :func:`chunk_notice_message`.
"""
from __future__ import annotations

from typing import Callable, List, NamedTuple, Optional

STANDARD_MIN_WORDS = 2
LAB_MIN_WINDOW = 4

TEXT_SHORTER = 'text_shorter_than_chunk_size'
TEXT_TOO_SHORT = 'text_too_short'
CHUNK_SIZE_RAISED = 'chunk_size_raised'
MIN_CHUNKS_LOWERED = 'min_chunk_matches_lowered'
TEXT_TOO_COMMON = 'text_too_common'


class WindowPlan(NamedTuple):
    starts: List[int]          # 0-based word offset of each window to search
    size: Optional[int]        # words per window; None when nothing is searched
    notices: List[dict]        # empty for an ordinary run
    stride_count: int          # windows placed by the stride alone (no tail window)


def plan_windows(n_words: int, chunk_size: Optional[int], *,
                 stride: Callable[[int], float] = lambda size: 1,
                 min_window: int = STANDARD_MIN_WORDS) -> WindowPlan:
    """Place the windows for a text of ``n_words`` words.

    ``min_window`` is both the smallest window the engine will query and the
    fewest words it will search at all.
    """
    requested = int(chunk_size) if chunk_size else 0
    size = requested
    notices: List[dict] = []
    if size < min_window:
        notices.append({'code': CHUNK_SIZE_RAISED, 'chunk_size': requested,
                        'effective_chunk_size': min_window})
        size = min_window
    if n_words < min_window:
        return WindowPlan([], None,
                          [{'code': TEXT_TOO_SHORT, 'words': n_words,
                            'minimum': min_window}], 0)
    if n_words < size:
        # The raised-size notice (if any) is superseded: the window is the text.
        return WindowPlan([0], n_words,
                          [{'code': TEXT_SHORTER, 'words': n_words,
                            'chunk_size': requested,
                            'effective_chunk_size': n_words}], 1)
    step = max(1, int(stride(size)))
    last = n_words - size
    starts = list(range(0, last + 1, step))
    stride_count = len(starts)
    if starts[-1] != last:
        starts.append(last)
    return WindowPlan(starts, size, notices, stride_count)


def distinct_windows(tokens, plan: WindowPlan) -> int:
    """How many different chunk texts the plan searches. A result's chunk count
    counts distinct chunk texts (``_count_unique_chunks``), so a text that
    repeats a phrase offers fewer chunks to match than it has windows."""
    return len({' '.join(tokens[i:i + plan.size]) for i in plan.starts})


def cap_min_chunk_matches(min_chunk_matches: int, windows: int, notices: List[dict], *,
                          too_common: bool = False) -> int:
    """A document cannot match more distinct chunks than the text has
    (*windows*: ``distinct_windows``), or than Lab Mode searches (*too_common*:
    the others are made of very common words). When the filter asks for more,
    lower it to that number and say so."""
    if windows and min_chunk_matches > windows:
        notice = {'code': MIN_CHUNKS_LOWERED, 'min_chunk_matches': min_chunk_matches,
                  'windows': windows}
        if too_common:
            notice['too_common'] = True
        notices.append(notice)
        return windows
    return min_chunk_matches


def plan_standard(tokens, chunk_size, boundary_mode='full', min_chunk_matches=0):
    """The standard engine's windows for *tokens*
    (SearchEngine.search_composition_logic): ``(plan, min_chunk_matches, notices)``,
    the minimum lowered (``boundary_mode`` 'full') when the text has fewer
    distinct chunks."""
    plan = plan_windows(len(tokens), chunk_size)
    notices = list(plan.notices)
    if plan.starts and boundary_mode == 'full':
        min_chunk_matches = cap_min_chunk_matches(min_chunk_matches, distinct_windows(tokens, plan),
                                                  notices)
    return plan, min_chunk_matches, notices


def standard_notices(text, chunk_size, boundary_mode='full', min_chunk_matches=0) -> List[dict]:
    """The notices the standard engine gives *text*, without searching it: for a
    request answered before any search runs (/api/parallels with filters that
    match nothing). Words as that engine reads them, nikud stripped."""
    import re
    from shared.config import Config
    from shared.text_normalize import strip_nikud
    tokens = [strip_nikud(m.group()) for m in re.finditer(Config.WORD_TOKEN_PATTERN, text or '')]
    return plan_standard(tokens, chunk_size, boundary_mode, min_chunk_matches)[2]


# Every user-facing string, so the translation test can find them all.
_MSG_SHORTER = ('The text has {words} words, fewer than the chunk size '
                '({chunk_size}), so it was searched as one chunk of {words} words.')
_MSG_TOO_SHORT = 'The text is too short to search: enter at least {minimum} words.'
_MSG_RAISED = ('This search uses at least {effective_chunk_size} words per chunk, '
               'so the chunk size was raised from {chunk_size} to '
               '{effective_chunk_size}.')
_MSG_MIN_LOWERED = ('The text has {windows} chunk(s) in all, so the minimum chunk '
                    'matches was lowered from {min_chunk_matches} to {windows}.')
_MSG_TOO_COMMON = ('Every chunk of the text is made of very common words, so '
                   'Lab Mode did not search it. Try a longer or more distinctive text.')
_MSG_MIN_LOWERED_COMMON = ("Lab Mode searched only {windows} of the text's chunks (the others "
                           'are made of very common words), so the minimum chunk matches was '
                           'lowered from {min_chunk_matches} to {windows}.')
NOTICE_STRINGS = (_MSG_SHORTER, _MSG_TOO_SHORT, _MSG_RAISED, _MSG_MIN_LOWERED,
                  _MSG_TOO_COMMON, _MSG_MIN_LOWERED_COMMON)
_BY_CODE = {TEXT_SHORTER: _MSG_SHORTER, TEXT_TOO_SHORT: _MSG_TOO_SHORT,
            CHUNK_SIZE_RAISED: _MSG_RAISED, MIN_CHUNKS_LOWERED: _MSG_MIN_LOWERED,
            TEXT_TOO_COMMON: _MSG_TOO_COMMON}
_WARNING_CODES = frozenset({TEXT_TOO_SHORT, TEXT_TOO_COMMON})


def chunk_notice_message(notice: Optional[dict], tr: Callable[[str], str]) -> Optional[str]:
    """The translated sentence for one notice, or None for an unknown code."""
    if not notice:
        return None
    template = (_MSG_MIN_LOWERED_COMMON
                if notice.get('code') == MIN_CHUNKS_LOWERED and notice.get('too_common')
                else _BY_CODE.get(notice.get('code')))
    if template is None:
        return None
    return tr(template).format(**notice)


def is_warning(notice: Optional[dict]) -> bool:
    """True when nothing was searched (show as a warning, not as info)."""
    return bool(notice) and notice.get('code') in _WARNING_CODES
