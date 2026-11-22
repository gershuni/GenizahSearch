"""Help describes Pick from the shelf only where the shelf tree is served.

The shelf button exists only where ``web/shelfmark_tree_assets.py::shelf_tree_available`` is
true, so a Help card describing it elsewhere would send the reader to a button that is not there.
"""
from unittest.mock import patch

from web.pages import help as help_page

_TOC = [
    ('reading-desk', 'Reading Desk'),
    ('browse', 'Browse by Shelfmark'),
    ('catalog-browse', 'Browse by Identification'),
]


def test_the_shelf_entry_follows_browse_where_the_shelf_is_served():
    for lang in ('en', 'he'):
        with patch.object(help_page, '_shelf_served', return_value=True):
            toc = help_page._with_shelf_entry(list(_TOC), lang)
        assert [anchor for anchor, _ in toc] == ['reading-desk', 'browse', 'shelf', 'catalog-browse']
        assert toc[2][1] == help_page._SHELF_HELP[lang][0]


def test_no_shelf_entry_where_the_shelf_is_not_served():
    with patch.object(help_page, '_shelf_served', return_value=False):
        assert help_page._with_shelf_entry(list(_TOC), 'en') == _TOC


def test_no_shelf_card_where_the_shelf_is_not_served():
    with (patch.object(help_page, '_shelf_served', return_value=False),
          patch.object(help_page.ui, 'card', side_effect=AssertionError('a card was drawn'))):
        help_page._render_shelf_help('en')
        help_page._render_shelf_help('he')


def test_the_served_check_is_the_shelf_gate():
    with patch('web.shelfmark_tree_assets.shelf_tree_available', return_value=True):
        assert help_page._shelf_served() is True
    with patch('web.shelfmark_tree_assets.shelf_tree_available', return_value=False):
        assert help_page._shelf_served() is False


def test_both_languages_carry_the_approved_titles_and_limits():
    assert help_page._SHELF_HELP['en'][0] == 'Pick from the shelf'
    assert help_page._SHELF_HELP['he'][0] == 'בחירה מהמדף'
    for lang, limits in (('en', '30 manuscripts and 1,000 pages'), ('he', '30 כתבי יד ו-1,000 עמודים')):
        assert limits in help_page._SHELF_HELP[lang][1]
