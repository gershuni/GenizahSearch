# -*- coding: utf-8 -*-
"""A page without an automatic transcription gets its reading view (owner, 2026-10-27): its text is taken as empty,
so the version selector, Edit, Comment, Notes and Joins are built and an FGP or PGP edition of the page shows by the
usual default rule. A synthetic catalogue-only record still gets none of them."""
from types import SimpleNamespace

import pytest

from shared.fgp_service import choose_default_source
from web.pages.browse_state import page_has_reading_view


@pytest.mark.parametrize("text, sys_id, expected", [
    ("שורה", "990051614260205171", True),          # automatic text: as before
    ("", "990051614260205171", True),              # no automatic text: the reading view still
    (None, "990051614260205171", True),
    ("", "990001234560000000", False),             # synthetic catalogue-only record: nothing to read or correct
])
def test_reading_view_on_a_page_without_automatic_text(text, sys_id, expected):
    assert page_has_reading_view(SimpleNamespace(text=text, sys_id=sys_id)) is expected


def test_no_page_no_reading_view():
    assert page_has_reading_view(None) is False


def test_an_edition_is_the_default_where_the_automatic_text_is_empty():
    # the rule the reading view now reaches on such a page: an FGP edition with an empty automatic text
    edition = {"id": 22952, "is_fgp": True, "doc_relation": "Digital Edition", "language": None, "fgp_c_number": None,
               "content": "אֵל מְשוך בְטלה / לְנִפְקְדו בַטְלאִ ים " * 20}
    decision = choose_default_source([edition], "", full_htr_getter=lambda: "")
    assert decision["eligible"] and decision["source"]["id"] == 22952
