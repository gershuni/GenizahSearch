"""CR HIGH-6 / HIGH-5: line-break (multi-line) Responsa queries.

HIGH-6: intra-line [N] word gaps used to be DROPPED in line-break mode, so
'word1 [2] word2' searched adjacent words while the UI claimed a gap. They are
now captured into LineGroup.word_gaps and honored by the line-break regex.

HIGH-5: the whole-query Text Position 'line_start'/'line_end' is applied to the
first/last group in _execute_line_break_search; the regex builder anchors those
groups to the start/end of their line.
"""

from __future__ import annotations

from genizah_core import SearchEngine, _parse_line_break_query


def _expanded(groups):
    """Build expanded_groups (exact words only, no variant expansion) for the
    pure _build_line_break_regex call."""
    return [[set(c.words) for c in g.components] for g in groups]


# ---------------------------------------------------------------------------
# HIGH-6: word gaps parsed + honored
# ---------------------------------------------------------------------------

def test_parse_captures_intra_line_word_gaps():
    groups, line_gaps = _parse_line_break_query('אמר [2] רבי [|0] עקיבא')
    assert groups is not None and len(groups) == 2
    assert groups[0].word_gaps == [2]   # gap between אמר and רבי
    assert groups[1].word_gaps == []    # single word → no intra-line gaps


def test_no_gap_token_yields_adjacent():
    groups, _ = _parse_line_break_query('אמר רבי [|0] עקיבא')
    # two words, no [N] between them → gap recorded as None (adjacent)
    assert groups[0].word_gaps == [None]


def test_word_gap_allows_intervening_words():
    groups, line_gaps = _parse_line_break_query('אמר [2] רבי [|0] עקיבא')
    rx = SearchEngine._build_line_break_regex(groups, line_gaps, _expanded(groups))
    assert rx is not None
    # 'לו' sits between אמר and רבי on line 1; עקיבא on line 2. gap=2 permits it.
    assert rx.search('אמר לו רבי\nעקיבא')
    # adjacent also still matches (0 intervening words)
    assert rx.search('אמר רבי\nעקיבא')


def test_zero_gap_rejects_intervening_words():
    groups, line_gaps = _parse_line_break_query('אמר רבי [|0] עקיבא')  # no [N] → adjacent
    rx = SearchEngine._build_line_break_regex(groups, line_gaps, _expanded(groups))
    assert rx is not None
    assert rx.search('אמר רבי\nעקיבא')          # adjacent matches
    assert not rx.search('אמר לו רבי\nעקיבא')   # intervening word must NOT match


# ---------------------------------------------------------------------------
# HIGH-5: line anchors honored in the line-break regex
# ---------------------------------------------------------------------------

def test_line_end_anchor_requires_end_of_line():
    # Mirrors what the text_position='line_end' guard does: anchor the last group.
    groups, line_gaps = _parse_line_break_query('שלום [|0] עולם')
    groups[-1].line_end = True
    rx = SearchEngine._build_line_break_regex(groups, line_gaps, _expanded(groups))
    assert rx is not None
    assert rx.search('שלום\nעולם')              # עולם ends its line
    assert not rx.search('שלום\nעולם רבה')      # trailing word → not at line end


def test_line_start_anchor_requires_start_of_line():
    groups, line_gaps = _parse_line_break_query('שלום [|0] עולם')
    groups[0].line_start = True
    rx = SearchEngine._build_line_break_regex(groups, line_gaps, _expanded(groups))
    assert rx is not None
    assert rx.search('שלום עליכם\nעולם')        # שלום starts line 1
    assert not rx.search('ויאמר שלום\nעולם')    # שלום not at line start
