# -*- coding: utf-8 -*-
"""Routing of the go-to boxes (owner, 2026-10-19; changed 2026-11-04).

``web/home_query.py::home_query_target`` decides where the homepage box and the
header box send what the visitor typed: a corpus sys_id opens that manuscript,
a shelfmark (as the /search box recognises one, ``shared/search_box_input.py``)
is looked up by ``/browse``, and everything else -- Hebrew, English words,
Hebrew typed on the English layout -- is searched. The wiring on the pages is
``tests/render_smoke/test_home_search_routing_render_smoke.py`` and
``tests/render_smoke/test_header_search_render_smoke.py``.
"""
from __future__ import annotations

from urllib.parse import parse_qs, quote, urlsplit

import pytest

from web.home_query import has_hebrew_or_arabic_letter, home_query_target


def _route(text):
    """(path, the single query value) for where ``text`` is sent."""
    target = home_query_target(text)
    parts = urlsplit(target)
    params = parse_qs(parts.query)
    assert len(params) == 1, target
    (key, values), = params.items()
    return parts.path, key, values[0]


@pytest.mark.parametrize('text', [None, '', '   ', '\t\n'])
def test_a_blank_box_goes_nowhere(text):
    assert home_query_target(text) is None


@pytest.mark.parametrize('sys_id', [
    '990053835020205171',            # NLI Alma id, libraries.csv row 1
    '  990053835020205171  ',        # pasted with spaces
    '990000012345000000',            # Phase-85 synthetic (99 + InventoryId + 000000)
])
def test_a_corpus_sys_id_opens_that_manuscript(sys_id):
    assert _route(sys_id) == ('/browse', 'sys_id', sys_id.strip())


@pytest.mark.parametrize('text', [
    '970993169503583183',   # LOCAL "My Library" namespace -- never a corpus record
    '123456789099',         # ends with 99 but is not in the 99 namespace
    '99',                   # too short to be a sys_id
    '12',                   # a bare number is not a shelfmark (15,533 catalogue rows)
    '٩٩٠٠٥٣٨٣٥٠٢٠٢٠٥١٧١',   # Arabic-Indic digits: `\d` matches them, a sys_id never has them
])
def test_other_numbers_are_searched(text):
    assert _route(text) == ('/search', 'q', text)


@pytest.mark.parametrize('text', [
    'שבת',
    'ויאמר משה',
    'בְּרֵאשִׁית',                 # pointed
    'قال',                      # Arabic script
    'T-S 12.123 שבת',           # a Hebrew letter anywhere wins
    'ש*',
])
def test_hebrew_or_arabic_text_is_searched(text):
    assert _route(text) == ('/search', 'q', text)


@pytest.mark.parametrize('text', [
    'dcuk',                     # גבול typed on the English layout
    'fufch anh',                # Hebrew typed on the English layout, two words
    'Psalm 23',                 # a word and a number
    'Maimonides',
    'letter from Fustat',
    'version 1.2',
])
def test_latin_text_that_is_not_a_shelfmark_is_searched(text):
    """It reached /browse as a shelfmark and found nothing before 2026-11-04."""
    assert _route(text) == ('/search', 'q', text)


@pytest.mark.parametrize('shelfmark', [
    'T-S 8J6.1',
    'ts 20.57',
    'add 3356',
    'MS Heb c 57',
    'ENA 2591.1',
    'JTS ENA 2591.1',           # library-prefixed
    'Moss. III,27O',
    'Halper 331',
    "T-S 10J9 (2)",
    'Cambridge University Library, Ms. T-S K 08.026',   # the NLI catalogue form
    'Moss. IIIa,20',            # libraries.csv, sys_id 990053799030205171 (Codex on #476)
])
def test_a_shelfmark_is_looked_up_and_survives_the_url(shelfmark):
    assert _route(shelfmark) == ('/browse', 'shelfmark', shelfmark)


@pytest.mark.parametrize('text', [
    'T-S AS 74.1+2',            # '+' would read as a space if not encoded
    'T-S 10J9 #5',              # '#' would start a URL fragment if not encoded
    'A&B 1',                    # '&' would split the query string if not encoded
])
def test_characters_no_shelfmark_carries_send_it_to_search_intact(text):
    """No libraries.csv shelfmark has '+', '#' or '&'-plus-this-shape, so the
    recogniser calls these text; /search must still get them unmangled."""
    assert _route(text) == ('/search', 'q', text)


def test_a_search_query_survives_the_url():
    assert _route('משה + אהרן & #') == ('/search', 'q', 'משה + אהרן & #')


@pytest.mark.parametrize('text, expected', [
    ('א', True),
    ('ﬠ', True),                # Hebrew presentation form (FB20)
    ('ب', True),
    ('ﺏ', True),                # Arabic presentation form (FE8F)
    ('ָּ', False),    # niqqud alone is not a letter
    ('١٢٣', False),             # Arabic-Indic digits are not letters
    ('T-S 8J6.1', False),
    ('Ελληνικά', False),
])
def test_only_hebrew_and_arabic_letters_count(text, expected):
    assert has_hebrew_or_arabic_letter(text) is expected


# --- the catalogue lookup (Codex on #476) ------------------------------------

class _Catalogue:
    def __init__(self, holds=()):
        self.holds, self.asked = set(holds), []

    def __call__(self, query):
        self.asked.append(query)
        return query in self.holds


@pytest.mark.parametrize('shelfmark', ['Mingana 9', 'P. Mich. Inv. 531', 'B 2093'])
def test_a_shelfmark_the_catalogue_holds_opens_browse(shelfmark):
    catalogue = _Catalogue({shelfmark})
    assert home_query_target(shelfmark, catalogue) == '/browse?shelfmark=' + quote(shelfmark, safe='')
    assert catalogue.asked == [shelfmark]


def test_without_the_catalogue_the_rules_decide():
    assert _route('Mingana 9') == ('/search', 'q', 'Mingana 9')


def test_text_the_catalogue_does_not_hold_is_searched():
    catalogue = _Catalogue({'Mingana 9'})
    assert home_query_target('Psalm 23', catalogue) == '/search?q=Psalm%2023'
    assert catalogue.asked == ['Psalm 23']


@pytest.mark.parametrize('text', ['dcuk', 'Maimonides', 'שבת 12', 'T-S 8J6.1', '990053835020205171', '  '])
def test_the_catalogue_is_asked_only_about_latin_text_with_a_digit(text):
    """Words without a digit are never a shelfmark; Hebrew is searched; what the
    rules already know is not asked again."""
    catalogue = _Catalogue({text.strip()})
    home_query_target(text, catalogue)
    assert catalogue.asked == []


def test_the_page_lookup_is_false_without_a_catalogue(monkeypatch):
    from web.home_query import catalogue_has_shelfmark
    from web.state import state
    monkeypatch.setattr(state, 'meta_mgr', None)
    assert catalogue_has_shelfmark('Mingana 9') is False


def test_the_page_lookup_searches_when_the_catalogue_fails(monkeypatch):
    from web.home_query import catalogue_has_shelfmark
    from web.state import state

    class _Broken:
        def has_shelfmark(self, query):
            raise RuntimeError('index not ready')

    monkeypatch.setattr(state, 'meta_mgr', _Broken())
    assert catalogue_has_shelfmark('Mingana 9') is False


# --- the search-mode signs (2026-11-18) ----------------------------------------
# A sign and a space open /search in its mode, the sign removed: the /search box's own
# signs (shared/search_mode_signs.py, tests/test_search_mode_signs.py).

def _search(text, has_shelfmark=None, catalog_served=False):
    """(path, {name: value}) for where ``text`` is sent."""
    target = home_query_target(text, has_shelfmark, catalog_served=catalog_served)
    parts = urlsplit(target)
    return parts.path, {k: v[0] for k, v in parse_qs(parts.query).items()}


@pytest.mark.parametrize('sign, mode', [
    ('$', 'Title'), ('#', 'Shelfmark'), ('?', 'variants'), ('??', 'variants_extended'),
    ('???', 'variants_maximum'), ('~', 'fuzzy'), ('=', 'exact'), ('/', 'Regex'),
    ('R', 'responsa'), ('%', 'Catalog'),
])
def test_a_sign_opens_search_in_its_mode_without_the_sign(sign, mode):
    assert _search(f'{sign} שלום', catalog_served=True) == ('/search', {'q': 'שלום', 'mode': mode})


def test_the_signed_url_carries_the_query_intact():
    target = home_query_target('$ משה + אהרן & #')
    assert target == '/search?q=' + quote('משה + אהרן & #', safe='') + '&mode=Title'


@pytest.mark.parametrize('text, expected', [
    ('# T-S 12.123', ('/search', {'q': 'T-S 12.123', 'mode': 'Shelfmark'})),   # a sign wins over Browse
    ('= T-S 12.123', ('/search', {'q': 'T-S 12.123', 'mode': 'exact'})),       # the rules call this a shelfmark
    ('$ Psalm 23', ('/search', {'q': 'Psalm 23', 'mode': 'Title'})),
    ('~ 990053835020205171', ('/search', {'q': '990053835020205171', 'mode': 'fuzzy'})),
    ('  $  שלום  ', ('/search', {'q': 'שלום', 'mode': 'Title'})),
])
def test_a_sign_decides_before_the_input_kind(text, expected):
    assert _search(text) == expected


@pytest.mark.parametrize('text, mode', [('$ ', 'Title'), ('R ', 'responsa')])
def test_the_sign_alone_opens_search_in_its_mode(text, mode):
    assert home_query_target(text) == f'/search?mode={mode}'


def test_percent_is_a_sign_only_where_the_catalogue_search_is_served():
    assert _search('% שלום', catalog_served=True) == ('/search', {'q': 'שלום', 'mode': 'Catalog'})
    # Not served: the box does what it did before the signs (the whole input, searched).
    assert _route('% שלום') == ('/search', 'q', '% שלום')
    assert home_query_target('% שלום', catalog_served=False) == home_query_target('% שלום')


@pytest.mark.parametrize('text', ['$שלום', 'Rashi', 'r שלום'])
def test_a_sign_without_a_space_is_no_sign_here(text):
    """As in the /search box; /search's own parsing still reads ``$שלום`` there."""
    assert _route(text) == ('/search', 'q', text)


def test_r_gives_way_to_text_of_an_r_shelfmarks_shape():
    """``R 15`` is a Mosseri shelfmark (libraries.csv, 33 shelfmarks begin with ``R ``): text of
    that shape is routed as if it had no sign; anything else after ``R`` is a Responsa query."""
    catalogue = _Catalogue({'R 15', 'R II 5'})
    assert home_query_target('R 15', catalogue) == '/browse?shelfmark=R%2015'
    assert home_query_target('  R II 5 ', catalogue) == '/browse?shelfmark=R%20II%205'
    assert _route('R 99') == ('/search', 'q', 'R 99')    # the shape, though no such shelfmark
    assert catalogue.asked == ['R 15', 'R II 5']   # the go-to routing's own lookup, not the sign's


@pytest.mark.parametrize('text, query', [
    ('R שלום', 'שלום'),
    ('R قال', 'قال'),
    ('R 99 [3] 100', '99 [3] 100'),   # digits and a gap: a Responsa query (Codex review of PR #52)
    ('R 15 שבת', '15 שבת'),
    ('R abc', 'abc'),
])
def test_any_other_text_after_r_is_a_responsa_query(text, query):
    assert _search(text, _Catalogue()) == ('/search', {'q': query, 'mode': 'responsa'})


def test_the_r_shape_covers_every_r_shelfmark_in_the_catalogue():
    """Every libraries.csv call number that begins with the Responsa sign and a space has the
    shape ``R_SHELFMARK_RE`` knows; a new form there fails here, not on a reader's search."""
    import csv
    from pathlib import Path

    from web.home_query import R_SHELFMARK_RE

    path = Path(__file__).resolve().parents[1] / 'libraries.csv'
    with path.open(encoding='utf-8', newline='') as f:
        r_marks = {cn.strip() for row in csv.reader(f) if len(row) > 2
                   for cn in row[2].split('|') if cn.strip().startswith('R ')}
    assert len(r_marks) >= 33, len(r_marks)
    assert sorted(m for m in r_marks if not R_SHELFMARK_RE.fullmatch(m)) == []


def test_r_15_does_not_depend_on_the_shelfmark_index_being_ready():
    """Until its index is built the catalogue lookup says False (MetadataManager.has_shelfmark);
    ``R 15`` must not become a Responsa search for ``15`` meanwhile (Codex review of PR #52)."""
    not_ready = _Catalogue()
    assert _route('R 15') == ('/search', 'q', 'R 15')
    assert home_query_target('R 15', not_ready) == '/search?q=R%2015'
    assert home_query_target('R 15', _Catalogue({'R 15'})) == '/browse?shelfmark=R%2015'


def test_no_sign_asks_the_catalogue():
    catalogue = _Catalogue({'# 15', 'R שלום'})
    assert _search('# 15', catalogue) == ('/search', {'q': '15', 'mode': 'Shelfmark'})
    assert _search('R שלום', catalogue) == ('/search', {'q': 'שלום', 'mode': 'responsa'})
    assert catalogue.asked == []
