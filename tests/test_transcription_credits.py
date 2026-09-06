# -*- coding: utf-8 -*-
"""Who the printed sheet credits, per transcription source.

The /browse print sheet shipped on 2026-09-04 crediting MiDRASH for every page.
That is wrong the moment the reader is looking at an FGP edition, a PGP edition,
a translation, or their own correction: it credits a machine for a scholar's
work, and it fails the CC-BY-4.0 attribution the /help page states.

The owner's rule: MiDRASH only for the automatic transcription; otherwise the
actual creator; always the site; bilingual throughout.

The load-bearing test here is
`test_every_source_version_selector_can_emit_is_handled`, which reads the
literals out of `web/components/version_selector.py` rather than restating them.
A hand-written list of source values would pass forever after someone adds an
eighth one -- and the defect that ships then is a mis-credit, which is exactly
the class of bug this file exists to prevent.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from shared.export_utils import GENIZAHSEARCH_URL, MIDRASH_CREDIT_LINES
from shared.transcription_credits import (
    HTR_SOURCES,
    KIND_HTR,
    resolve_transcription_credit,
)

REPO = Path(__file__).resolve().parents[1]
VERSION_SELECTOR = REPO / 'web' / 'components' / 'version_selector.py'
BROWSE = REPO / 'web' / 'pages' / 'browse.py'

LANGS = ('en', 'he')

#: A representative payload per source kind, shaped like the real
#: `version_info` dicts `create_version_selector` hands to `on_version_change`.
SAMPLES = {
    'fgp': {'source': 'fgp', 'source_credit': 'Prof. Example (FGP)'},
    'pgp': {'source': 'pgp', 'attribution': 'S. D. Goitein, unpublished editions. (T-S 8J)'},
    'translation': {'source': 'translation', 'language': 'Hebrew', 'attribution': 'M. Gil'},
    'user': {'source': 'user', 'author': 'A Reader'},
    'pending': {'source': 'pending'},
}


# ---------------------------------------------------------------------------
# The owner's rule
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('lang', LANGS)
@pytest.mark.parametrize('source', HTR_SOURCES)
def test_the_automatic_transcription_credits_midrash(source, lang):
    """Every spelling of "the HTR text is on screen" owes MiDRASH.

    `None` and `''` are in here on purpose: the print masthead renders
    synchronously, BEFORE `choose_default_source` has run, and on a manuscript
    with no alternative sources `on_version_change` is never called at all. The
    unset state is the plain HTR text, so it must credit MiDRASH rather than
    print nothing.
    """
    info = None if source is None else {'source': source}
    credit = resolve_transcription_credit(info, lang=lang)
    assert credit.kind == KIND_HTR
    assert credit.credits_midrash is True
    assert list(MIDRASH_CREDIT_LINES) == credit.citation_lines


def test_the_must_contain_path_is_not_mis_credited():
    """`'original'` and `'V0.8'` are the SAME text under two names.

    `version_selector` emits `'original'` when a search phrase forced the V0.8
    text (the path a reader arriving from a search hit takes) and `'V0.8'` when
    it is picked from the menu. The obvious `== 'V0.8'` test silently drops the
    first one, so this pins them equal rather than trusting the parametrize
    above to keep both entries.
    """
    a = resolve_transcription_credit({'source': 'original'}, lang='en')
    b = resolve_transcription_credit({'source': 'V0.8'}, lang='en')
    assert a.heading == b.heading
    assert a.citation_lines == b.citation_lines


@pytest.mark.parametrize('lang', LANGS)
@pytest.mark.parametrize('source', ['fgp', 'pgp', 'translation'])
def test_someone_elses_scholarship_does_not_credit_midrash(source, lang):
    """The whole point of the change. An FGP or PGP edition, or a translation,
    is not a machine reading, and MiDRASH must not appear on that sheet."""
    credit = resolve_transcription_credit(SAMPLES[source], lang=lang)
    assert credit.credits_midrash is False
    printed = '\n'.join(credit.all_lines())
    assert 'MiDRASH' not in printed
    assert 'zenodo' not in printed.lower()


@pytest.mark.parametrize('lang', LANGS)
@pytest.mark.parametrize('source', ['user', 'pending'])
def test_a_community_correction_credits_both_the_corrector_and_midrash(source, lang):
    """DELIBERATE, and the one place this goes past the rule as literally worded.

    A correction is an edit OF the automatic transcription -- `supabase_client`
    stores the HTR as the correction's `original_text` -- so the printed text is
    a derivative of a CC-BY-4.0 work and the attribution survives modification.
    Dropping MiDRASH here would credit a proof-reader for a machine's reading.

    If the owner rules otherwise, this test is the thing to change, and changing
    it is then a decision rather than an accident.
    """
    credit = resolve_transcription_credit(SAMPLES[source], lang=lang)
    assert credit.credits_midrash is True
    assert list(MIDRASH_CREDIT_LINES) == credit.citation_lines[1:]


def test_a_correction_names_its_author_and_nothing_more_private():
    """`author` is a display name (full_name or username) -- `get_corrections`
    selects only id/full_name/username from `profiles`, never the email column
    that exists there. Printing the same name already on screen discloses
    nothing new; printing an id or an address would."""
    credit = resolve_transcription_credit(
        {'source': 'user', 'author': 'A Reader', 'correction_id': 42}, lang='en')
    assert 'A Reader' in credit.heading
    printed = '\n'.join(credit.all_lines())
    assert '42' not in printed
    assert '@' not in printed


# ---------------------------------------------------------------------------
# The site credit
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('lang', LANGS)
@pytest.mark.parametrize('key', list(SAMPLES) + ['htr'])
def test_the_site_is_credited_on_every_sheet(key, lang):
    info = None if key == 'htr' else SAMPLES[key]
    credit = resolve_transcription_credit(info, lang=lang)
    assert credit.site_lines, 'no site credit for %r' % key
    printed = '\n'.join(credit.site_lines)
    assert GENIZAHSEARCH_URL in printed
    if lang == 'he':
        assert 'אתר הגניזה של דיקטה' in printed
    else:
        assert 'Dicta Genizah Search' in printed


def test_the_hebrew_site_line_is_not_doubled():
    """The Hebrew site name already starts with אתר, so a generic
    "label: value" join produced "נצפה באתר: אתר הגניזה..." -- "viewed on
    site: site of the genizah"."""
    line = resolve_transcription_credit(None, lang='he').site_lines[0]
    assert 'אתר: אתר' not in line
    assert line.startswith('נצפה באתר הגניזה של דיקטה')


# ---------------------------------------------------------------------------
# Bilingual, and its honest limits
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('key', list(SAMPLES) + ['htr'])
def test_the_frame_is_hebrew_in_a_hebrew_ui(key):
    info = None if key == 'htr' else SAMPLES[key]
    heading = resolve_transcription_credit(info, lang='he').heading
    assert re.search(r'[֐-׿]', heading), (
        'the %r heading has no Hebrew at all: %r' % (key, heading))


def test_the_midrash_citation_stays_english_on_a_hebrew_sheet():
    """`shared/export_utils` documents this: a published citation is cited as
    published, and the DOI, dataset URL and authors' names must survive intact.
    The rule the workbooks follow applies on paper too."""
    credit = resolve_transcription_credit(None, lang='he')
    assert list(MIDRASH_CREDIT_LINES) == credit.citation_lines


def test_a_pgp_scholar_citation_passes_through_verbatim():
    """PGP attributions are English-only free text in pgp.db with no Hebrew
    column and no translation path. A Hebrew sheet gets a Hebrew LABEL around an
    untouched English citation -- mangling it would be worse than leaving it."""
    attribution = 'S. D. Goitein, unpublished editions. (T-S 8J)'
    for lang in LANGS:
        credit = resolve_transcription_credit(
            {'source': 'pgp', 'attribution': attribution}, lang=lang)
        assert attribution in credit.citation_lines


def test_translation_language_names_are_localised():
    """`version_selector` passes the language through as a raw English label and
    never localises it -- the on-screen notification has the same gap."""
    he = resolve_transcription_credit(
        {'source': 'translation', 'language': 'Hebrew'}, lang='he').heading
    assert 'Hebrew' not in he
    assert 'עברית' in he


# ---------------------------------------------------------------------------
# Coverage of the real source vocabulary
# ---------------------------------------------------------------------------

def _source_literals_in_version_selector():
    """Every `'source': '<literal>'` in the version selector.

    Read from the file, not restated, so an eighth source value added later
    fails this suite instead of silently taking the MiDRASH fallback.
    """
    text = VERSION_SELECTOR.read_text(encoding='utf-8')
    found = set(re.findall(r"['\"]source['\"]\s*:\s*['\"]([^'\"]+)['\"]", text))
    assert found, 'found no source literals -- has the contract moved?'
    return sorted(found)


def test_every_source_version_selector_can_emit_is_handled():
    """No value reaches the unknown-source fallback by accident."""
    unclassified = []
    for source in _source_literals_in_version_selector():
        credit = resolve_transcription_credit({'source': source}, lang='en')
        if source not in HTR_SOURCES and credit.kind == KIND_HTR:
            unclassified.append(source)
    assert not unclassified, (
        'these source values fall through to the MiDRASH fallback and would be '
        'mis-credited on a printed sheet: %r' % (unclassified,))


@pytest.mark.parametrize('lang', LANGS)
def test_an_unknown_source_still_credits_somebody(lang):
    """The fallback is MiDRASH, not silence. On every code path that exists an
    unrecognised source is still showing the automatic text, and a MISSING
    attribution is a licence problem where a redundant one is not."""
    credit = resolve_transcription_credit({'source': 'something-new'}, lang=lang)
    assert credit.kind == KIND_HTR
    assert credit.citation_lines
    assert credit.site_lines


def test_no_source_value_raises():
    for source in _source_literals_in_version_selector() + ['', 'x', None]:
        info = None if source is None else {'source': source}
        resolve_transcription_credit(info, lang='he')
        resolve_transcription_credit(info, lang='en')


# ---------------------------------------------------------------------------
# The citation stays single-sourced
# ---------------------------------------------------------------------------

def test_browse_does_not_hand_copy_the_midrash_citation_again():
    """`shared/export_utils` exists because this citation had been written out
    verbatim in three places, each declaring another canonical. The print
    masthead was briefly a fourth. It must take the constant instead -- a stale
    citation credits the wrong record, which is worse than none.
    """
    text = BROWSE.read_text(encoding='utf-8')
    assert 'zenodo.17734473' not in text, (
        'browse.py hand-copies the MiDRASH DOI again; import the credit instead')
    assert 'MiDRASH Automatic Transcriptions' not in text
    assert 'resolve_transcription_credit' in text, (
        'browse.py no longer resolves the credit per source')


def test_the_credit_follows_the_displayed_version():
    """A build-time-only credit is wrong twice over: the default source is
    decided asynchronously AFTER this renders, and the reader can switch
    versions afterwards. `handle_version_change` must redraw it."""
    text = BROWSE.read_text(encoding='utf-8')
    handler = text.split('def handle_version_change', 1)
    assert len(handler) == 2, 'handle_version_change is gone'
    body = handler[1].split('# Version selector placeholder', 1)[0]
    assert '_render_print_credit(version_info)' in body, (
        'the printed credit is not redrawn when the reader switches version')
