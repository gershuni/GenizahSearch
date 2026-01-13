# -*- coding: utf-8 -*-
"""Web pages package."""

from web.pages.search import create_search_page
from web.pages.document import create_document_page
from web.pages.parallels import create_parallels_page
from web.pages.browse import create_browse_page

__all__ = [
    'create_search_page',
    'create_document_page',
    'create_parallels_page',
    'create_browse_page',
]
