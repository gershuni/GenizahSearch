# -*- coding: utf-8 -*-
"""The desktop citation bar must credit whoever actually wrote the text.

THE DEFECT THIS FIXES, so it is not re-introduced. Until 2026-09-04 the bar at
the bottom of every desktop tab showed ONE hardcoded MiDRASH/Zenodo citation
with no awareness of the version combo, and "Copy Citation" handed that to the
reader. `_auto_select_pgp_edition` is PGP-FIRST unconditionally, so a reader on
the Browse tab is usually looking at a Princeton or Friedberg edition -- meaning
the wrong-attribution case was the DEFAULT path, not an edge case. It is the
same defect the web printed sheet and citation footer had, and it is fixed from
the same shared decision (`shared/transcription_credits`) so the two apps cannot
drift on who is owed what.

HOW THESE TESTS RUN WITHOUT Qt
------------------------------
`_credit_version_info` is a method on the main window class, and instantiating
that needs a QApplication -- which would put these tests in the `gui` lane that
CI excludes (`-m "not gui and not render_smoke and not atlas_bake"`), i.e. the
guard on a licensing defect would never run in CI.

So the method is lifted out of `genizah_app.py` by AST and executed standalone
against a stub `self`. That is not a source-text assertion dressed up: the real
function body runs, and its output is fed to the REAL shared module, so what is
asserted is the citation a reader would actually get. The only thing stubbed is
the one attribute it touches (`_browse_pgp_url`).
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from shared.transcription_credits import (
    KIND_FGP,
    KIND_HTR,
    KIND_PENDING,
    KIND_PGP,
    KIND_TRANSLATION,
    KIND_USER,
    page_citation,
    site_citation,
)

REPO = Path(__file__).resolve().parents[1]
APP = REPO / 'genizah_app.py'


# ---------------------------------------------------------------------------
# Lifting the real function out of a 25,000-line Qt module
# ---------------------------------------------------------------------------

def _load_method(name: str, **attrs):
    """Return `genizah_app`'s method `name` as a plain callable, plus a stub self.

    Compiled from the file's own AST, so the body under test is the shipped one
    -- if it is edited, this runs the edit.
    """
    tree = ast.parse(APP.read_text(encoding='utf-8'))
    found = [n for n in ast.walk(tree)
             if isinstance(n, ast.FunctionDef) and n.name == name]
    assert len(found) == 1, (
        'expected exactly one def %s in genizah_app.py, found %d'
        % (name, len(found)))

    fn = found[0]
    fn.decorator_list = []          # @staticmethod etc. are meaningless here
    module = ast.Module(body=[fn], type_ignores=[])
    ast.fix_missing_locations(module)
    namespace: dict = {}
    exec(compile(module, str(APP), 'exec'), namespace)      # noqa: S102

    class _Stub:
        pass

    stub = _Stub()
    for key, value in attrs.items():
        setattr(stub, key, value)
    return namespace[name], stub


@pytest.fixture(scope='module')
def adapter():
    """`_credit_version_info` bound to a stub with a PGP url available."""
    fn, stub = _load_method('_credit_version_info',
                            _browse_pgp_url='https://geniza.princeton.edu/x')

    def call(version_data):
        return fn(stub, version_data)

    return call


# ---------------------------------------------------------------------------
# 1. THE DEFECT ITSELF
# ---------------------------------------------------------------------------

def test_a_princeton_edition_is_not_credited_to_midrash(adapter):
    """THE BUG, stated as a test. This is the default Browse view.

    `_auto_select_pgp_edition` prefers a PGP edition unconditionally, so this is
    what a reader normally has on screen -- and the bar used to hand them the
    MiDRASH citation for it.
    """
    info = adapter({'source': 'pgp_edition', 'scholar': 'Goitein, S. D.',
                    'content': '...'})
    citation = page_citation(info, lang='en', library='Cambridge University Library',
                             shelfmark='T-S Ar.50.74', folio='1r',
                             retrieved_on='2026-09-04')
    assert citation.kind == KIND_PGP
    assert citation.credits_midrash is False, (
        'a Princeton edition is being credited to MiDRASH -- the exact defect '
        'this bar was fixed to remove')
    assert 'MiDRASH' not in citation.text
    assert 'Goitein, S. D.' in citation.text
    assert 'T-S Ar.50.74' in citation.text


def test_the_automatic_transcription_still_credits_midrash(adapter):
    """The CONTROL for the test above.

    Without it, "no MiDRASH in the citation" would be satisfied by a bar that
    credits MiDRASH for nothing at all -- which is a licence breach in the other
    direction, and the easier mistake to make while fixing the first.
    """
    for version_data in (None, {}, {'source': 'original'}):
        info = adapter(version_data)
        assert info is None, version_data
        citation = page_citation(info, lang='en', shelfmark='T-S 12.123',
                                 retrieved_on='2026-09-04')
        assert citation.kind == KIND_HTR
        assert citation.credits_midrash is True
        assert 'MiDRASH' in citation.text


def test_the_hardcoded_citation_is_gone_from_the_app_module():
    """The bar carried the full author list as a literal -- TWICE.

    The visible label read `SettingsDialog.FULL_CITATION` and `copy_citation`
    re-typed the same 346 characters inline; they were byte-identical, so
    changing one would have silently made the bar show something other than what
    it copied. Both are replaced by the shared module.

    `desktop/settings_dialogs.py` keeps `FULL_CITATION` deliberately: the About
    tab is genuinely about MiDRASH, and citing it there is correct.

    SCOPED TO THE BAR. `_get_credit_header` -- the credit on desktop's DOCX/TXT
    exports -- still holds its own hardcoded MiDRASH text, and that copy has
    already drifted from the canonical `shared/export_utils.MIDRASH_CREDIT_LINES`
    it mirrors ("Dataset available at:" for "Dataset:", and the full author line
    dropped). That is a real, separate gap on a surface this change did not
    touch, and it is named here so a later reader does not mistake it for
    something this test failed to notice.
    """
    src = APP.read_text(encoding='utf-8')
    bar = src[src.index('def _create_citation_bar'):
              src.index('def _show_citation_reminder')]
    assert 'Stoekl Ben Ezra' not in bar, (
        'the MiDRASH author list is hardcoded in the citation bar again; it '
        'belongs to shared/export_utils, which the shared credit module reads')
    assert 'FULL_CITATION' not in bar, (
        'the bar reads the About tab\'s static citation again, which is not '
        'aware of what is on screen')
    # And the byte-identical SECOND copy that `copy_citation` re-typed is gone.
    assert 'def copy_citation(self)' not in src, (
        'the old source-blind copy_citation is back')


# ---------------------------------------------------------------------------
# 2. Every source the combo can emit, mapped
# ---------------------------------------------------------------------------

CASES = [
    ({'source': 'pgp_edition', 'scholar': 'Goitein, S. D.'}, KIND_PGP, False),
    ({'source': 'fgp_edition', 'source_credit': 'FGP team'}, KIND_FGP, False),
    ({'source': 'pgp_translation', 'scholar': 'Goitein, S. D.',
      'language': 'English'}, KIND_TRANSLATION, False),
    ({'source': 'fgp_translation', 'source_credit': 'FGP team',
      'language': 'Hebrew'}, KIND_TRANSLATION, False),
    # A community-submitted VERSION -- a whole alternative transcription by a
    # named contributor, which is a DIFFERENT combo source from a correction and
    # was missed by the first cut of the adapter (found by the completeness test
    # below). It fell through to the MiDRASH default: the same defect, on a path
    # nobody had looked at.
    ({'source': 'user', 'version_id': 7, 'user_name': 'Ada'}, KIND_USER, True),
    ({'source': 'correction', 'status': 'approved', 'user_name': 'Ada'},
     KIND_USER, True),
    ({'source': 'correction', 'status': 'pending', 'user_name': 'Ada'},
     KIND_PENDING, True),
    ({'source': 'correction', 'status': 'draft', 'user_name': 'Ada'},
     KIND_PENDING, True),
    ({'source': 'correction', 'status': 'rejected', 'user_name': 'Ada'},
     KIND_PENDING, True),
    ({'source': 'header'}, KIND_HTR, True),
    ({'source': 'original'}, KIND_HTR, True),
]


@pytest.mark.parametrize('version_data,kind,midrash', CASES)
@pytest.mark.parametrize('lang', ['en', 'he'])
def test_each_combo_source_gets_the_right_credit(adapter, version_data, kind,
                                                 midrash, lang):
    citation = page_citation(adapter(version_data), lang=lang,
                            shelfmark='T-S 12.123', retrieved_on='2026-09-04')
    assert citation.kind == kind, version_data
    assert citation.credits_midrash is midrash, version_data


def test_an_unapproved_correction_does_not_read_as_an_accepted_one(adapter):
    """`draft`, `pending` and `rejected` are all unapproved.

    Only `approved` has been accepted by anyone. Collapsing the four into one
    "community correction" heading would print an unreviewed edit as a settled
    reading -- the same class of false claim as mis-crediting a transcriber.
    """
    approved = page_citation(
        adapter({'source': 'correction', 'status': 'approved',
                 'user_name': 'Ada'}), lang='en', retrieved_on='2026-09-04')
    pending = page_citation(
        adapter({'source': 'correction', 'status': 'pending',
                 'user_name': 'Ada'}), lang='en', retrieved_on='2026-09-04')
    assert approved.text != pending.text
    assert 'Ada' in approved.text
    # Both are edits OF the automatic transcription, so both still owe MiDRASH.
    assert approved.credits_midrash and pending.credits_midrash


def test_the_adapter_covers_every_source_the_desktop_actually_emits(adapter):
    """COMPLETENESS, derived from the app rather than from this file's memory.

    Collects every `{'source': <literal>}` the desktop builds into combo item
    data and checks the adapter classifies each one deliberately. A seventh
    source added to `_populate_pgp_combo` later fails HERE rather than silently
    falling through to the MiDRASH default -- which is the failure mode that
    would quietly re-create the original defect for the new provider.
    """
    # Companion keys that only a VERSION item carries. Used to recognise a
    # combo item by its SHAPE, not by how it happens to reach the widget.
    VERSION_KEYS = {'version_id', 'source_id', 'content', 'corrected_text',
                    'correction_id', 'user_name', 'is_default', 'scholar',
                    'source_credit'}

    def _source_of(dict_node):
        source = None
        keys = set()
        for key, value in zip(dict_node.keys, dict_node.values):
            if not isinstance(key, ast.Constant):
                continue
            keys.add(key.value)
            if (key.value == 'source' and isinstance(value, ast.Constant)
                    and isinstance(value.value, str)):
                source = value.value
        return source, keys

    tree = ast.parse(APP.read_text(encoding='utf-8'))

    # The adapter's OWN output dicts are excluded: it PRODUCES the shared
    # module's vocabulary ({'source': 'fgp', 'source_credit': ...}) rather than
    # consuming desktop's, and those carry version companion keys too, so the
    # shape test above claims them as unmapped desktop sources otherwise.
    adapter_defs = [n for n in ast.walk(tree)
                    if isinstance(n, ast.FunctionDef)
                    and n.name == '_credit_version_info']
    assert len(adapter_defs) == 1
    produced = {id(n) for n in ast.walk(adapter_defs[0])}

    emitted = set()
    for node in ast.walk(tree):
        if id(node) in produced:
            continue
        # TWO ways in, deliberately, because each alone has a blind spot.
        #
        # (a) the dict handed straight to `addItem(label, {...})` -- how every
        #     combo item is written today; and
        # (b) any dict literal carrying `source` PLUS a version companion key,
        #     which still catches an item built into a variable first.
        #
        # (b) is what makes the check hold under refactoring, and it was added
        # after mutation testing: a NEW provider written as
        # `_new = {...}; combo.addItem(label, _new)` slips past (a) entirely and
        # would fall through to the MiDRASH default in silence. With (b) that
        # mutation goes red. (Moving an ALREADY-mapped source into a variable
        # stays green, correctly -- it is still mapped.)
        #
        # Walking every dict with a `source` key was the first cut and was too
        # wide the other way -- it swept in result-row `display` dicts, whose
        # unrelated `source` field carries '' and 'LOCAL'. Those have none of
        # VERSION_KEYS, so the shape test excludes them.
        if (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == 'addItem'
                and len(node.args) == 2
                and isinstance(node.args[1], ast.Dict)):
            source, _ = _source_of(node.args[1])
            if source is not None:
                emitted.add(source)
            continue
        if isinstance(node, ast.Dict):
            source, keys = _source_of(node)
            if source is not None and (keys & VERSION_KEYS):
                emitted.add(source)

    assert emitted, (
        'no version-combo item dicts were found at all, so this test is '
        'asserting nothing -- the addItem shape it looks for has changed')

    # The vocabulary the adapter is written against. Anything else is new.
    # 'V0.7' and 'V0.8' are here because they ARE combo items; the adapter sends
    # them to the HTR credit by asking the shared module's HTR_SOURCES.
    known = {'pgp_edition', 'fgp_edition', 'pgp_translation', 'fgp_translation',
             'correction', 'user', 'header', 'original', 'V0.7', 'V0.8'}
    unexpected = emitted - known
    assert not unexpected, (
        'genizah_app.py builds version dicts with source(s) %r that '
        '_credit_version_info does not classify; they would fall through to the '
        'MiDRASH default and mis-credit that provider' % sorted(unexpected))

    # And each consumed literal really is handled -- not merely listed above.
    for source in ('pgp_edition', 'fgp_edition', 'pgp_translation',
                   'fgp_translation', 'correction', 'user'):
        info = adapter({'source': source, 'status': 'approved'})
        assert info is not None, (
            '%r falls through to the HTR/MiDRASH default' % source)


def test_no_metadata_manager_yields_no_page_citation_rather_than_a_crash():
    """`self.meta_mgr` starts as None and is only assigned conditionally.

    `_browse_page_citation` reads `meta_mgr.nli_cache`, and it runs inside
    `menu.aboutToShow` -- so on a start-up where metadata never loaded, an
    unguarded read would take down the whole menu, including the site citation,
    which is always available. Withholding the page citation is the right
    failure; showing a wrong one is not.
    """
    fn, stub = _load_method('_browse_page_citation')
    stub.current_browse_sid = '99001'
    stub.meta_mgr = None
    assert fn(stub, retrieved_on='2026-09-05') is None

    # And the ordinary "nothing open" case, which is the common one.
    fn2, stub2 = _load_method('_browse_page_citation')
    stub2.current_browse_sid = None
    stub2.meta_mgr = object()
    assert fn2(stub2, retrieved_on='2026-09-05') is None


# ---------------------------------------------------------------------------
# 3. The strip, and what gets copied
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('lang', ['en', 'he'])
def test_the_strip_names_this_site_as_well_as_midrash(lang):
    """The old bar credited MiDRASH and never mentioned Dicta or the address.

    The site citation is what the strip now shows, on every tab: it is true
    everywhere, and it names both parties.
    """
    text = site_citation(lang=lang).text
    assert 'genizahsearch.com' in text
    assert 'MiDRASH' in text
    assert ('Dicta Genizah Search' in text if lang == 'en'
            else 'דיקטה' in text)


def test_the_strip_carries_no_date_but_a_copied_citation_does():
    """The bar is built once and the app may stay open for days.

    A retrieval date painted at startup would be quietly wrong by the next
    morning, so the visible strip omits it and the COPIED string is stamped when
    it is copied. This is the property that lets the strip never be repainted.
    """
    on_screen = site_citation(lang='en').text
    copied = site_citation(lang='en', retrieved_on='2026-09-04').text
    assert 'Sept. 4, 2026' in copied
    assert '2026' not in on_screen.replace('2025', ''), (
        'the strip carries a date, which goes stale in a long-running session')


def test_the_strip_elides_visibly_and_never_hands_out_the_cut_string():
    """CI-visible half of the ElidingLabel guard.

    `tests/test_eliding_label.py` exercises the widget itself, but it is
    `gui`-marked and CI runs `-m "not gui ..."`. These two properties need no Qt
    and are the ones that go wrong at the CALL site: using a plain QLabel again
    (which clips in silence), or copying `text()` -- the elided form -- instead
    of `full_text`.

    With the owner's full 17-author citation the strip is ~428 characters on one
    fixed 30px line, so it cannot all fit at any ordinary window width; the only
    question is whether the reader can SEE that.
    """
    src = APP.read_text(encoding='utf-8')
    bar = src[src.index('def _create_citation_bar'):
              src.index('def _copy_citation_text')]
    assert 'ElidingLabel(' in bar, (
        'the citation strip is a plain QLabel again, which does not elide -- it '
        'stops painting, so a cut citation looks like a short one')
    assert 'cit_lbl.setToolTip(cit_lbl.full_text)' in bar, (
        'the tooltip shows the ELIDED text, so the whole citation is nowhere '
        'reachable by reading')
    assert 'TextSelectableByMouse' not in bar, (
        'the strip is selectable again: `text()` is the elided form, so a mouse '
        'selection yields a citation that is cut but looks whole')


def test_the_bar_offers_both_citations_and_chooses_neither():
    """Two menu entries, each wired to its own copy handler.

    A single button whose meaning changes with the active tab was the other
    option and is worse: the reader cannot tell which citation they are about to
    get, which is how the original defect went unnoticed.
    """
    src = APP.read_text(encoding='utf-8')
    for handler in ('def copy_page_citation', 'def copy_site_citation'):
        assert handler in src, handler
    assert re.search(r'act_page\.triggered\.connect\(self\.copy_page_citation\)',
                     src), 'the page entry is not wired to the page handler'
    assert re.search(r'act_site\.triggered\.connect\(self\.copy_site_citation\)',
                     src), 'the site entry is not wired to the site handler'
    assert 'menu.aboutToShow.connect' in src, (
        'the page entry is not re-evaluated when the menu opens, so its enabled '
        'state can disagree with what is on screen')
