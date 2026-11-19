"""Help lists the Catalog (%) search mode only where the catalogue text search is served.

The mode and its % sign exist only where ``web/catalog_assets.py::catalog_available`` is
true, so Help describing them elsewhere would send the reader to a choice that is not there.
"""
from unittest.mock import patch

from web.pages import help as help_page

_MODES = [
    ('Exact (=)', 'x'),
    ('Title ($)', 'x'),
    ('Shelfmark (#)', 'x'),
]


def test_the_catalog_line_follows_title_where_the_catalogue_is_served():
    for lang in ('en', 'he'):
        with patch.object(help_page, '_catalog_served', return_value=True):
            modes = help_page._with_catalog_mode(list(_MODES), lang)
        assert [mode for mode, _ in modes] == [
            'Exact (=)', 'Title ($)', help_page._CATALOG_MODE_HELP[lang][0], 'Shelfmark (#)']
        assert '(%)' in modes[2][0]


def test_no_catalog_line_where_the_catalogue_is_not_served():
    with patch.object(help_page, '_catalog_served', return_value=False):
        assert help_page._with_catalog_mode(list(_MODES), 'en') == _MODES


def test_the_served_check_is_the_catalogue_gate():
    with patch('web.catalog_assets.catalog_available', return_value=True):
        assert help_page._catalog_served() is True
    with patch('web.catalog_assets.catalog_available', return_value=False):
        assert help_page._catalog_served() is False
