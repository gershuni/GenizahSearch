# -*- coding: utf-8 -*-
"""`serve_v3_review.sefaria_ref` — the citation link for canonical works.

The link is a CONVENIENCE beside the file+character address, never instead of it
(owner, 2026-09-01). So the rule that matters most here is the negative one: an
unmapped book or an unparsable address must yield NO link. A link to the wrong
passage would be worse than none, because a reader would compare our match
against a text it was never matched to.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "scripts"))
from serve_v3_review import sefaria_ref  # noqa: E402


@pytest.mark.parametrize("title,locus,url", [
    ('תנ"ך, בראשית', "פרק א", "https://www.sefaria.org/Genesis.1"),
    ('תנ"ך, תהלים', "פרק קיט", "https://www.sefaria.org/Psalms.119"),
    ('תנ"ך, שמואל א', "פרק ב", "https://www.sefaria.org/I_Samuel.2"),
    ('תנ"ך, דברי הימים ב', "פרק ל", "https://www.sefaria.org/II_Chronicles.30"),
    ('תנ"ך, שיר השירים', "פרק ד", "https://www.sefaria.org/Song_of_Songs.4"),
    ("תלמוד בבלי, ברכות", 'יז ע"א', "https://www.sefaria.org/Berakhot.17a"),
    ("תלמוד בבלי, שבת", 'לב ע"ב', "https://www.sefaria.org/Shabbat.32b"),
    ("תלמוד בבלי, בבא מציעא", 'קיט ע"א',
     "https://www.sefaria.org/Bava_Metzia.119a"),
    ("משנה, מידות", "פרק א", "https://www.sefaria.org/Mishnah_Middot.1"),
    # Avot is NOT "Mishnah_Avot" on Sefaria
    ("משנה, אבות", "פרק ה", "https://www.sefaria.org/Pirkei_Avot.5"),
])
def test_exact_refs(title, locus, url):
    assert sefaria_ref(title, locus)[0] == url


def test_a_chapter_range_links_to_its_first_chapter():
    """`פרק א–ג` is one match spanning three chapters; the link opens the
    first, and the row's own address stays the precise one."""
    assert sefaria_ref('תנ"ך, איוב', "פרק א–ג")[0] == "https://www.sefaria.org/Job.1"
    assert sefaria_ref('תנ"ך, איוב', "פרק ב-ג")[0] == "https://www.sefaria.org/Job.2"


@pytest.mark.parametrize("title,locus", [
    # numbering does not correspond 1:1 -- deliberately unlinked
    ("תלמוד ירושלמי, ברכות", "פרק א, הלכה ד"),
    # the book sits inside the locus, with an edition marker
    ("תוספתא", "סוכה (ליברמן), פרק ג, הלכה יח"),
    ("תוספתא, ברכות", "פרק א"),
    # loci are Hebrew section names, not numbers
    ("משנה תורה, ספר אהבה", "סדר התפילה, הלכה מא"),
    ("משנה תורה לרמב״ם", "סדר תפילות כל השנה, כל השנה"),
    # not a canonical work at all
    ("מגן אבות לרשב״ץ", "פרק ד, משנה ז"),
    ("משנת רבי אליעזר (ברייתא דל״ב מידות)", "פרק א"),
])
def test_no_link_where_the_mapping_is_not_exact(title, locus):
    assert sefaria_ref(title, locus) == (None, None)


@pytest.mark.parametrize("title,locus", [
    ('תנ"ך, בראשית', None),
    (None, "פרק א"),
    ('תנ"ך, בראשית', "פרק"),               # no numeral
    ('תנ"ך, בראשית', "פרק שלמה"),          # a WORD, not a numeral
    ('תנ"ך, בראשית', "עמוד א"),            # not a chapter address
    ('תנ"ך, ספר שאיננו', "פרק א"),         # unmapped book
    ("תלמוד בבלי, ברכות", "יז"),           # folio with no side
    ("תלמוד בבלי, ברכות", 'יז ע"ג'),       # no such side
    ("בראשית", "פרק א"),                   # no family prefix
])
def test_refuses_rather_than_guesses(title, locus):
    assert sefaria_ref(title, locus) == (None, None)


def test_gematria_is_not_fooled_by_words():
    """A locus is only a number when every letter is a numeral; otherwise the
    builder must decline. Reading a word as gematria would fabricate an
    address."""
    from serve_v3_review import _gematria
    assert _gematria("קיט") == 119
    assert _gematria("תתקצט") == 999       # descending, so it parses...
    assert _gematria("א׳") == 1
    # ...but a chapter that big is refused at the call site
    assert sefaria_ref('תנ"ך, תהלים', "פרק תתקצט") == (None, None)
    # a WORD made entirely of numeral letters: 300,30,40,5 -- 40 follows 30, so
    # it is not a numeral at all. Without this rule it read as 375.
    assert _gematria("שלמה") is None
    assert _gematria("") is None
    assert _gematria(None) is None
